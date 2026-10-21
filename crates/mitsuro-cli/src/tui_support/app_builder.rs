//! App initialization helpers
//!
//! Breaks up the 300+ line App::new() constructor into focused helper functions.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::agent::{PackageHookConfig, UserHookManager, UserPostToolHook, UserPreToolHook};
use crate::ai::models::{create_model_registry, ModelKey, ModelMetadata, SharedModelRegistry};
use crate::ai::providers::{builtin_providers, ProviderId};
use crate::extensions::WasmHost;
use crate::paths;
use crate::plan::PlanManager;
use crate::plugins::PluginManager;
use crate::process::ProcessRegistry;
use crate::storage::{CredentialStore, Database, Preferences, SessionManager};
use crate::tools::{register_all_tools, ToolRegistry};
use crate::tui_support::utils::OAuthStatusUpdate;
use crate::tui_support::AppServices;
use mitsuro_core::mcp::{McpConnectionAuthority, McpPackageConfig};
use mitsuro_core::skills::SkillsManager;

/// Initialize core services (tools, extensions, etc.)
pub async fn init_services(
    working_dir: &Path,
) -> (
    AppServices,
    tokio::sync::mpsc::UnboundedReceiver<OAuthStatusUpdate>,
    Arc<ProcessRegistry>,
    String,
    Option<ModelKey>,
    ProviderId,
) {
    let process_registry = Arc::new(ProcessRegistry::new());

    // WASM extension host
    let extensions_dir = paths::extensions_dir();
    let http_client = reqwest::Client::new();
    let wasm_host = Some(WasmHost::new(http_client, extensions_dir.clone()));
    let (wasm_extensions, wasm_diagnostics) = if let Some(host) = &wasm_host {
        host.load_extensions_from_root(&extensions_dir).await
    } else {
        (Vec::new(), Vec::new())
    };
    tracing::info!(
        loaded = wasm_extensions.len(),
        path = %extensions_dir.display(),
        "Zed-compatible WASM extension host initialized"
    );
    for (path, error) in wasm_diagnostics {
        tracing::warn!(path = %path.display(), error = %error, "Failed to load WASM extension");
    }

    // Installable plugin manager
    let plugins_dir = paths::plugins_dir();
    let plugin_manager = Some(Arc::new(PluginManager::new(
        reqwest::Client::new(),
        plugins_dir.clone(),
    )));
    if let Some(manager) = &plugin_manager {
        if let Err(err) = manager.ensure_layout().await {
            tracing::warn!(
                "Failed to initialize installable plugin layout at {:?}: {}",
                plugins_dir,
                err
            );
        } else {
            tracing::info!(
                "Installable plugin manager initialized at {:?}",
                plugins_dir
            );
        }
    }
    let installed_plugins = if let Some(manager) = &plugin_manager {
        match manager.list_installed_plugins().await {
            Ok(plugins) => plugins
                .into_iter()
                .filter(|plugin| plugin.enabled)
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(error = %error, "Failed to resolve installed plugin contributions");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let mut executable_plugin_ids = HashSet::new();
    let mut mcp_plugin_authorities = HashMap::new();
    if let Some(manager) = &plugin_manager {
        for plugin in &installed_plugins {
            if !plugin.agent_extension_paths.is_empty() || !plugin.hook_paths.is_empty() {
                match manager
                    .ensure_installed_plugin_permission(
                        plugin,
                        mitsuro_core::plugins::PluginPermission::Process,
                    )
                    .await
                {
                    Ok(()) => {
                        executable_plugin_ids.insert(plugin.id.clone());
                    }
                    Err(error) => tracing::warn!(
                        plugin_id = %plugin.id,
                        error = %error,
                        "Executable plugin contribution remains disabled until process permission is granted"
                    ),
                }
            }
            if plugin.mcp_servers_path.is_some() {
                match manager.permission_status_for_installed(plugin).await {
                    Ok(status) if status.grant_is_current => {
                        let authority = McpConnectionAuthority::new(
                            status.granted.process,
                            status.granted.network,
                        );
                        if !authority.is_empty() {
                            mcp_plugin_authorities.insert(plugin.id.clone(), authority);
                        }
                    }
                    Ok(_) => tracing::warn!(
                        plugin_id = %plugin.id,
                        "Plugin MCP contribution remains disabled until process or network authority is granted"
                    ),
                    Err(error) => {
                        tracing::warn!(plugin_id = %plugin.id, error = %error, "Failed to resolve plugin MCP permissions")
                    }
                }
            }
        }
    }

    // Database path
    let db_path = paths::config_dir().join("mitsuro.db");

    // User hook manager
    let user_hook_manager = init_user_hooks(&db_path).await;
    let package_hook_configs = installed_plugins
        .iter()
        .filter(|plugin| executable_plugin_ids.contains(&plugin.id))
        .flat_map(|plugin| {
            plugin
                .hook_paths
                .iter()
                .map(|path| PackageHookConfig::new(&plugin.id, path, &plugin.install_path))
        })
        .collect();
    match user_hook_manager
        .write()
        .await
        .replace_package_hooks(package_hook_configs)
    {
        Ok(report) if report.hook_count > 0 => tracing::info!(
            configs = report.config_count,
            hooks = report.hook_count,
            "Loaded package command hooks"
        ),
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, "Package hooks were disabled after a load failure")
        }
    }

    // Tool registry with hooks
    let tool_registry = init_tool_registry(&user_hook_manager).await;
    let agent_extensions = mitsuro_core::extensions::AgentExtensionManager::new(working_dir);
    for plugin in &installed_plugins {
        if !executable_plugin_ids.contains(&plugin.id) {
            continue;
        }
        for path in &plugin.agent_extension_paths {
            agent_extensions
                .register_root(mitsuro_core::extensions::AgentExtensionRoot::new(
                    path,
                    mitsuro_core::extensions::AgentExtensionScope::Package,
                ))
                .await;
        }
    }
    tool_registry.set_agent_extension_manager(agent_extensions.clone());
    if let Err(error) = agent_extensions.refresh_and_register(&tool_registry).await {
        tracing::warn!(error = %error, "Failed to initialize agent extensions");
    } else {
        let loaded = agent_extensions.loaded_ids().await;
        if !loaded.is_empty() {
            tracing::info!(extensions = ?loaded, "Loaded executable agent extensions");
        }
        for diagnostic in agent_extensions.diagnostics().await {
            tracing::warn!(
                path = %diagnostic.path.display(),
                extension_id = ?diagnostic.extension_id,
                message = %diagnostic.message,
                "Agent extension diagnostic"
            );
        }
    }
    // Cache the complete catalog locally; request-time policy selects the
    // compact wire surface for each turn.
    let cached_ai_tools = tool_registry.get_ai_tools_all().await;

    // Preferences
    let preferences = init_preferences(&db_path);

    // Session manager
    let session_manager = init_session_manager(&db_path);

    // Plan manager
    let plan_manager = init_plan_manager(&db_path);

    // Credentials and active provider
    let credential_store = CredentialStore::load().unwrap_or_else(|e| {
        tracing::warn!("Failed to load credential store: {}", e);
        CredentialStore::default()
    });
    let saved_active_provider = crate::storage::credentials::ActiveProviderStore::load();

    // Model registry
    let model_registry = init_model_registry(&preferences);

    // Exact selections win over the legacy slug. Older unambiguous
    // preferences are migrated once; ambiguous bare IDs remain non-runnable
    // until the user explicitly selects a catalog row.
    let persisted_model_key = preferences
        .as_ref()
        .and_then(|preferences| preferences.get_current_model_key())
        .filter(|key| !key.model_id.trim().is_empty());
    let legacy_model = preferences
        .as_ref()
        .and_then(|preferences| preferences.get_current_model())
        .map(|model| model.trim().to_string())
        .filter(|model| !model.is_empty());
    let current_model_key = persisted_model_key.clone().or_else(|| {
        legacy_model
            .as_deref()
            .and_then(|model| model_registry.try_resolve_legacy_key(model).ok())
    });
    let current_model = current_model_key
        .as_ref()
        .map(|key| key.model_id.clone())
        .or(legacy_model)
        .unwrap_or_default();
    if persisted_model_key.is_none() {
        if let (Some(preferences), Some(key)) = (&preferences, &current_model_key) {
            if let Err(error) = preferences.set_current_model_key(key) {
                tracing::warn!(%error, "Failed to migrate current model preference to an exact key");
            }
        }
    }
    let active_provider = resolve_initial_provider(
        saved_active_provider,
        current_model_key.as_ref(),
        &current_model,
        &credential_store,
        &model_registry,
    );

    // Skills manager
    let global_skills_dir = paths::config_dir().join("skills");
    let project_skills_dir = Some(working_dir.join(".mitsuro").join("skills"));
    let mut skills = SkillsManager::new(global_skills_dir, project_skills_dir);
    for plugin in &installed_plugins {
        for path in &plugin.skill_paths {
            skills.register_package_root(&plugin.id, path.clone());
        }
    }
    skills.refresh();
    let skills_manager = Arc::new(RwLock::new(skills));

    // MCP manager and channels
    let mcp_manager = Arc::new(mitsuro_core::mcp::McpManager::new(
        working_dir.to_path_buf(),
    ));
    mcp_manager
        .set_package_configs(
            installed_plugins
                .iter()
                .filter_map(|plugin| {
                    let path = plugin.mcp_servers_path.clone()?;
                    let authority = mcp_plugin_authorities.get(&plugin.id).copied()?;
                    Some(McpPackageConfig::new(path, authority))
                })
                .collect(),
        )
        .await;
    let (oauth_status_tx, oauth_status_rx) = tokio::sync::mpsc::unbounded_channel();

    // Connect MCP servers in background
    spawn_mcp_connections(&mcp_manager, &tool_registry).await;

    let services = AppServices {
        plan_manager,
        session_manager,
        preferences,
        credential_store,
        model_registry,
        tool_registry,
        cached_ai_tools,
        user_hook_manager,
        _wasm_host: wasm_host,
        _wasm_extensions: wasm_extensions,
        plugin_manager,
        skills_manager,
        mcp_manager,
        oauth_status_tx,
    };

    (
        services,
        oauth_status_rx,
        process_registry,
        current_model,
        current_model_key,
        active_provider,
    )
}

/// Initialize user hooks from database
async fn init_user_hooks(db_path: &Path) -> Arc<RwLock<UserHookManager>> {
    let user_hook_manager = Arc::new(RwLock::new(UserHookManager::new()));
    if let Ok(db) = Database::new(db_path) {
        let hook_count = {
            let mut mgr = user_hook_manager.write().await;
            if let Err(e) = mgr.load(&db) {
                tracing::warn!("Failed to load user hooks: {}", e);
            }
            mgr.hooks().len()
        };
        if hook_count > 0 {
            tracing::info!("Loaded {} user hooks", hook_count);
        }
    }
    user_hook_manager
}

/// Initialize tool registry with safety hooks
async fn init_tool_registry(user_hook_manager: &Arc<RwLock<UserHookManager>>) -> Arc<ToolRegistry> {
    let mut tool_registry = ToolRegistry::new();
    tool_registry.add_pre_hook(Arc::new(crate::agent::SafetyHook::new()));
    tool_registry.add_pre_hook(Arc::new(crate::agent::PlanModeHook::new()));
    tool_registry.add_post_hook(Arc::new(crate::agent::LoggingHook::new()));
    tool_registry.add_pre_hook(Arc::new(UserPreToolHook::new(user_hook_manager.clone())));
    tool_registry.add_post_hook(Arc::new(UserPostToolHook::new(user_hook_manager.clone())));
    let tool_registry = Arc::new(tool_registry);
    register_all_tools(&tool_registry).await;
    tool_registry
}

/// Initialize persisted terminal preferences.
fn init_preferences(db_path: &Path) -> Option<Preferences> {
    match Database::new(db_path) {
        Ok(db) => Some(Preferences::new(db)),
        Err(e) => {
            tracing::warn!("Failed to initialize preferences: {}", e);
            None
        }
    }
}

fn resolve_initial_provider(
    saved_active_provider: Option<ProviderId>,
    current_model_key: Option<&ModelKey>,
    current_model: &str,
    credential_store: &CredentialStore,
    model_registry: &SharedModelRegistry,
) -> ProviderId {
    current_model_key
        .map(|key| key.provider)
        .or_else(|| {
            crate::tui_support::auth::infer_provider_for_model(model_registry, current_model)
        })
        .or(saved_active_provider)
        .or_else(|| credential_store.providers_with_auth().into_iter().next())
        .unwrap_or(ProviderId::MiniMax)
}

/// Initialize session manager
fn init_session_manager(db_path: &Path) -> Option<SessionManager> {
    match Database::new(db_path) {
        Ok(db) => {
            tracing::info!("Session database initialized at {:?}", db_path);
            Some(SessionManager::new(db))
        }
        Err(e) => {
            tracing::warn!("Failed to initialize session database: {}", e);
            None
        }
    }
}

/// Initialize plan manager with migration
fn init_plan_manager(db_path: &Path) -> Option<PlanManager> {
    let plan_manager = match PlanManager::new(db_path.to_path_buf()) {
        Ok(pm) => pm,
        Err(e) => {
            tracing::error!("Failed to create plan manager: {}", e);
            return None;
        }
    };

    // Migrate legacy file-based plans
    match plan_manager.migrate_legacy_plans() {
        Ok((migrated, skipped)) if migrated > 0 => {
            tracing::info!(
                "Migrated {} legacy plans to database ({} skipped)",
                migrated,
                skipped
            );
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Failed to migrate legacy plans: {}", e);
        }
    }

    Some(plan_manager)
}

/// Initialize model registry with static and cached models
fn init_model_registry(preferences: &Option<Preferences>) -> SharedModelRegistry {
    let model_registry = create_model_registry();

    // Load static models from builtin providers
    for provider in builtin_providers() {
        if provider.models.is_empty() {
            continue;
        }
        let models: Vec<ModelMetadata> = provider
            .models
            .iter()
            .map(|m| {
                let api_format = crate::ai::format_detection::detect_api_format(provider.id, &m.id);
                let mut meta = ModelMetadata::new(&m.id, &m.display_name, provider.id)
                    .with_context(m.context_window, m.max_output);
                if let Some(format) = m.reasoning {
                    meta = meta.with_thinking(format);
                }
                meta.supported_reasoning_levels = m.supported_reasoning_levels.clone();
                meta.default_reasoning_level = m.default_reasoning_level;
                meta.reasoning_is_mandatory = m.reasoning_is_mandatory;
                meta.reasoning_control = m.reasoning_control;
                meta.fast_mode = m.fast_mode;
                meta.api_format = api_format;
                meta.supports_vision = mitsuro_core::ai::models::resolve_model_metadata(
                    provider.id,
                    &m.id,
                    api_format,
                )
                .supports_vision;
                meta
            })
            .collect();
        futures::executor::block_on(model_registry.set_models(provider.id, models));
    }
    tracing::info!("Model registry initialized with static models");

    // Load cached models for all dynamic providers (OpenRouter, OpenAI, Grok, …)
    // so CLI model select matches server catalogs across restarts.
    if let Some(ref prefs) = preferences {
        for provider in mitsuro_core::ai::catalog::dynamic_model_providers() {
            if let Some(cached_models) = prefs.get_cached_models(provider) {
                futures::executor::block_on(
                    model_registry.set_models(provider, cached_models.clone()),
                );
                tracing::info!(
                    "Loaded {} cached {:?} models from preferences",
                    cached_models.len(),
                    provider
                );
            }
        }
    }

    // Re-apply persisted custom/manual model metadata after catalog loads so
    // free-form model IDs remain usable across restarts and provider refreshes.
    if let Some(ref prefs) = preferences {
        for provider in ProviderId::all() {
            let custom_models = prefs.get_custom_models(*provider);
            if custom_models.is_empty() {
                continue;
            }

            futures::executor::block_on(async {
                for metadata in custom_models {
                    model_registry.upsert_model(metadata).await;
                }
            });
        }
    }

    // Load recent models
    if let Some(ref prefs) = preferences {
        let recent_keys = prefs.get_recent_model_keys();
        if !recent_keys.is_empty() {
            let count = recent_keys.len();
            futures::executor::block_on(model_registry.set_recent_keys(recent_keys));
            tracing::info!("Loaded {} provider-aware recent models", count);
        } else {
            let recent_ids = prefs.get_recent_models();
            if !recent_ids.is_empty() {
                futures::executor::block_on(model_registry.set_recent_ids(recent_ids.clone()));
                tracing::info!("Loaded {} recent models from preferences", recent_ids.len());
            }
        }
    }

    model_registry
}

/// Spawn MCP server connections in background
async fn spawn_mcp_connections(
    mcp_manager: &Arc<mitsuro_core::mcp::McpManager>,
    tool_registry: &Arc<ToolRegistry>,
) {
    if let Err(e) = mcp_manager.load_config().await {
        tracing::warn!("Failed to load MCP config: {}", e);
        return;
    }

    if !mcp_manager.has_servers().await {
        return;
    }

    let mcp = mcp_manager.clone();
    let registry = tool_registry.clone();

    tokio::spawn(async move {
        if let Err(e) = mcp.connect_all().await {
            tracing::warn!("MCP server connection errors: {}", e);
        }
        mitsuro_core::mcp::tool::register_mcp_tools(mcp.clone(), &registry).await;

        let tool_count = mcp.get_all_tools().await.len();
        if tool_count > 0 {
            tracing::info!(tool_count, "MCP initialized");
        }
    });
}
