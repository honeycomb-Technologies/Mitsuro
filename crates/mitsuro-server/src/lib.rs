//! Mitsuro Server
//!
//! Self-hosted API server for chat, tools, sessions, and local workspace access.
//! This is a library crate — the server is started via `start_server()`.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{header, Method, Response, StatusCode, Uri},
    middleware,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use rust_embed::Embed;
use serde::Serialize;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    trace::TraceLayer,
};

use mitsuro_core::agent::{
    AgentCancellation, LoggingHook, PackageHookConfig, PlanModeHook, SafetyHook, UserHookManager,
    UserPostToolHook, UserPreToolHook,
};
use mitsuro_core::ai::client::AiClient;
use mitsuro_core::ai::models::{create_model_registry, SharedModelRegistry};
use mitsuro_core::mcp::{McpConnectionAuthority, McpManager, McpPackageConfig};
use mitsuro_core::paths;
use mitsuro_core::process::ProcessRegistry;
use mitsuro_core::skills::SkillsManager;
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::storage::Database;
use mitsuro_core::tools::{
    register_agent_tool, register_all_tools, register_hive_tools, ToolRegistry,
};

use self::ai_bootstrap::{
    create_ai_client, create_ai_client_for_key, create_ai_client_for_model, initialize_models,
    spawn_model_catalog_refresh,
};

type SessionGuard = Arc<Mutex<()>>;
const SESSION_LOCK_MAX_ENTRIES: usize = 1000;
const SESSION_LOCK_MAX_AGE: Duration = Duration::from_secs(3600);
mod acceptance;
mod agent_streams;
mod ai_bootstrap;
mod child_wake;
mod control_leases;
mod process_wake;
type SessionLockMap = HashMap<String, (SessionGuard, Instant)>;
type SessionInputMap =
    HashMap<String, tokio::sync::mpsc::UnboundedSender<mitsuro_core::agent::LoopInput>>;
type SessionPresenceMap = presence::SessionPresenceMap;
type DelegatedStateMap = HashMap<String, Vec<types::DelegatedToolStateResponse>>;
pub mod apns;
pub mod auth;
pub(crate) mod device_access;
pub mod error;
pub mod hive_execution_host;
pub mod hive_runtime;
pub(crate) mod legacy_identity;
pub mod notifications;
pub(crate) mod oauth_flow;
pub mod presence;
pub mod push;
pub mod remote_access;
pub mod routes;
pub mod types;
pub mod utils;
pub mod ws;

pub use acceptance::{start_hive_acceptance_server, HiveAcceptanceServerConfig};

/// Embedded web frontend assets.
///
/// At compile time, rust-embed includes all files from the Expo web build directory.
/// When the build directory is absent, this will be empty and the server
/// gracefully falls back to API-only mode.
#[derive(Embed)]
#[folder = "../../apps/mobile/dist"]
#[prefix = ""]
#[allow_missing = true]
struct WebAssets;

pub(crate) const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 100 * 1024 * 1024;
/// Compatibility version for the server's shared HTTP/SSE/WebSocket contract.
pub const API_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy)]
struct ServerHttpPolicy {
    allow_any_browser_origin: bool,
    max_request_body_bytes: usize,
    immutable_asset_max_age_secs: u64,
    mutable_asset_max_age_secs: u64,
}

impl Default for ServerHttpPolicy {
    fn default() -> Self {
        Self {
            allow_any_browser_origin: false,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            immutable_asset_max_age_secs: 31_536_000,
            mutable_asset_max_age_secs: 3_600,
        }
    }
}

impl ServerHttpPolicy {
    fn cors_layer(self) -> CorsLayer {
        let cors = CorsLayer::new()
            .allow_methods([
                Method::GET,
                Method::POST,
                Method::PATCH,
                Method::PUT,
                Method::DELETE,
            ])
            .allow_headers(Any);

        if self.allow_any_browser_origin {
            cors.allow_origin(Any)
        } else {
            cors.allow_origin(AllowOrigin::predicate(|origin, _request_parts| {
                origin
                    .to_str()
                    .ok()
                    .is_some_and(crate::auth::is_trusted_local_origin)
            }))
        }
    }

    fn cache_control(self, path: &str) -> String {
        if path.contains("/_app/immutable/") || path.starts_with("_expo/static/") {
            format!(
                "public, max-age={}, immutable",
                self.immutable_asset_max_age_secs
            )
        } else if path.ends_with(".html") {
            "no-cache".to_string()
        } else {
            format!("public, max-age={}", self.mutable_asset_max_age_secs)
        }
    }
}

/// Configuration for starting the server.
pub struct ServerConfig {
    /// Port to listen on (default: 3000).
    pub port: u16,
    /// Working directory for file/tools APIs.
    pub working_dir: PathBuf,
    /// Optional explicit database path for isolated previews and evaluations.
    /// Production callers normally leave this unset and use `~/.mitsuro/mitsuro.db`.
    pub database_path: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            database_path: None,
        }
    }
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub server_port: u16,
    pub db_path: Arc<PathBuf>,
    pub working_dir: Arc<PathBuf>,
    pub ai_client: Option<Arc<AiClient>>,
    pub tool_registry: Arc<ToolRegistry>,
    pub process_registry: Arc<ProcessRegistry>,
    pub model_registry: SharedModelRegistry,
    pub credential_store: Arc<RwLock<CredentialStore>>,
    pub mcp_manager: Arc<McpManager>,
    pub hook_manager: Arc<RwLock<UserHookManager>>,
    pub skills_manager: Arc<RwLock<SkillsManager>>,
    pub cancellation: AgentCancellation,
    /// Per-session locks to prevent concurrent agentic loops on the same session.
    pub session_locks: Arc<RwLock<SessionLockMap>>,
    /// Active orchestrator input channels for tool approvals / cancellation.
    pub session_inputs: Arc<RwLock<SessionInputMap>>,
    /// Presence registry for active viewers/controllers per session.
    pub session_presence: Arc<RwLock<SessionPresenceMap>>,
    /// Ephemeral, opaque mutation authority for interactive Code Agents.
    pub(crate) session_control_leases: Arc<RwLock<control_leases::SessionControlLeaseRegistry>>,
    /// Bounded canonical live/replay events for interactive Code Agent views.
    pub(crate) agent_streams: Arc<RwLock<agent_streams::AgentStreamHub>>,
    /// Active delegated tool snapshots per session for reconnect/reload parity.
    pub delegated_state: Arc<RwLock<DelegatedStateMap>>,
    /// Cached remote-access authority configuration.
    pub remote_access: Arc<RwLock<remote_access::RemoteAccessConfig>>,
    pub(crate) device_access: Arc<device_access::DeviceAccessRuntime>,
    /// Count of currently active agent SSE streams.
    pub active_agent_streams: Arc<AtomicUsize>,
    /// Peak observed RSS bytes for this server process.
    pub peak_rss_bytes: Arc<AtomicU64>,
    /// Peak observed virtual bytes for this server process.
    pub peak_virtual_bytes: Arc<AtomicU64>,
    /// Web Push notification service (None if VAPID init failed).
    pub push_service: Option<Arc<push::PushService>>,
    /// APNs (Apple Push Notification service) for iOS devices.
    pub apns_service: Option<Arc<apns::ApnsService>>,
    /// Active OAuth flows keyed by provider storage key.
    pub oauth_flows: Arc<Mutex<HashMap<String, oauth_flow::OAuthFlowState>>>,
    /// Background runtime owner for autonomous Hive sessions.
    pub hive_runtime: Arc<hive_runtime::HiveRuntimeManager>,
}

impl AppState {
    async fn session_lock(&self, session_id: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.write().await;
        if locks.len() > SESSION_LOCK_MAX_ENTRIES {
            locks.retain(|_, (lock, created_at)| {
                created_at.elapsed() < SESSION_LOCK_MAX_AGE || Arc::strong_count(lock) > 1
            });
        }
        let (lock, _) = locks
            .entry(session_id.to_string())
            .or_insert_with(|| (Arc::new(Mutex::new(())), Instant::now()));
        Arc::clone(lock)
    }

    /// Acquire the canonical per-session mutation lock without waiting.
    /// Chat, autonomous runs, and manual compaction all share this guard.
    pub(crate) async fn try_lock_session(&self, session_id: &str) -> Option<OwnedMutexGuard<()>> {
        self.session_lock(session_id).await.try_lock_owned().ok()
    }

    /// Wait for the canonical session mutation lock. Internal wake/recovery
    /// paths use this only after a live input handoff is unavailable, then
    /// re-check their durable work while holding the guard.
    pub(crate) async fn lock_session(&self, session_id: &str) -> OwnedMutexGuard<()> {
        self.session_lock(session_id).await.lock_owned().await
    }

    /// Resolve a fresh AI client using the current credential store and requested model.
    pub async fn resolve_ai_client(&self, requested_model: Option<&str>) -> Option<Arc<AiClient>> {
        self.resolve_ai_client_for_user(requested_model, None).await
    }

    pub async fn resolve_ai_client_for_user(
        &self,
        requested_model: Option<&str>,
        user_id: Option<&str>,
    ) -> Option<Arc<AiClient>> {
        let credentials = self.credential_store.read().await.clone();
        let client = create_ai_client_for_model(
            &credentials,
            &self.model_registry,
            self.db_path.as_ref().as_path(),
            requested_model,
            user_id,
        )
        .await
        .map(Arc::new)?;
        self.ensure_agent_tool_registered(client.clone()).await;
        Some(client)
    }

    /// Resolve a fresh AI client from an exact provider-aware model identity.
    ///
    /// `user_id` is accepted for signature parity with the legacy-slug path
    /// but is intentionally unused here: an exact `ModelKey` needs no
    /// per-user selection, and provider credentials are server-owned by
    /// design (one self-hosted trust domain, one credential store). True
    /// per-user credential partitioning is a product architecture decision,
    /// not a route-level change.
    pub async fn resolve_ai_client_for_key_for_user(
        &self,
        key: &mitsuro_core::ai::models::ModelKey,
        _user_id: Option<&str>,
    ) -> Option<Arc<AiClient>> {
        let credentials = self.credential_store.read().await.clone();
        let client = create_ai_client_for_key(&credentials, &self.model_registry, key)
            .await
            .map(Arc::new)?;
        self.ensure_agent_tool_registered(client.clone()).await;
        Some(client)
    }

    async fn ensure_agent_tool_registered(&self, client: Arc<AiClient>) {
        if self.tool_registry.get("agent").await.is_some() {
            return;
        }

        register_agent_tool(&self.tool_registry, client, self.cancellation.clone()).await;
        tracing::info!("Registered unified agent sub-agent tool");
    }
}

fn register_autonomous_classifier_hook(
    tool_registry_inner: &mut ToolRegistry,
    ai_client: Option<Arc<AiClient>>,
) {
    use mitsuro_core::agent::autonomy::auto_classifier::AutoClassifierHook;

    let hook = match ai_client {
        Some(client) => AutoClassifierHook::new(client),
        None => AutoClassifierHook::without_bootstrap_client(),
    };
    tool_registry_inner.add_pre_hook(Arc::new(hook));
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum HiveRuntimeMode {
    DaemonProxy,
    ExecutionHost,
    IsolatedEvaluation,
}

async fn initialize_mcp_manager(
    mode: HiveRuntimeMode,
    working_dir: &std::path::Path,
    tool_registry: &ToolRegistry,
    package_configs: Vec<McpPackageConfig>,
) -> Arc<McpManager> {
    let manager = Arc::new(McpManager::new(working_dir.to_path_buf()));
    if matches!(
        mode,
        HiveRuntimeMode::ExecutionHost | HiveRuntimeMode::IsolatedEvaluation
    ) {
        // The daemon can execute runs for many frozen project roots. Loading
        // `.mcp.json` from its launch directory would expose the wrong remote
        // tools to every project, while HTTP-process connect/trust state cannot
        // safely authorize a different process. Keep autonomous MCP fail-closed
        // until the daemon has canonical project+config trust and child/server
        // lifecycle ownership.
        tracing::info!(
            working_dir = %working_dir.display(),
            "Skipping cwd-scoped MCP bootstrap in the Hive execution host"
        );
        return manager;
    }

    manager.set_package_configs(package_configs).await;
    if let Err(error) = manager.load_config().await {
        tracing::warn!(error = %error, "Failed to load MCP config");
    } else if let Err(error) = manager.connect_all().await {
        tracing::warn!(error = %error, "Failed to connect MCP servers");
    }
    mitsuro_core::mcp::tool::register_mcp_tools(Arc::clone(&manager), tool_registry).await;
    manager
}

fn initialize_remote_access(
    mode: HiveRuntimeMode,
    db_path: &std::path::Path,
) -> anyhow::Result<remote_access::RemoteAccessConfig> {
    match mode {
        HiveRuntimeMode::DaemonProxy => remote_access::RemoteAccessConfig::load_or_create(db_path),
        HiveRuntimeMode::ExecutionHost | HiveRuntimeMode::IsolatedEvaluation => {
            Ok(remote_access::RemoteAccessConfig::disabled_ephemeral())
        }
    }
}
/// Build the shared agent/tool state used by an HTTP process or the standalone
/// Hive executor. Production HTTP discovers its daemon, isolated acceptance
/// may inject one explicit daemon client, and the executor receives an
/// embedded manager so it cannot recursively call its own socket.
pub(crate) async fn build_app_state(
    config: &ServerConfig,
    hive_mode: HiveRuntimeMode,
    database_path: Option<PathBuf>,
) -> anyhow::Result<AppState> {
    build_app_state_with_hive_runtime(config, hive_mode, database_path, None).await
}

async fn build_app_state_with_hive_runtime(
    config: &ServerConfig,
    hive_mode: HiveRuntimeMode,
    database_path: Option<PathBuf>,
    hive_runtime_override: Option<Arc<hive_runtime::HiveRuntimeManager>>,
) -> anyhow::Result<AppState> {
    let isolated_evaluation = matches!(hive_mode, HiveRuntimeMode::IsolatedEvaluation);
    let db_path = database_path.unwrap_or_else(|| paths::config_dir().join("mitsuro.db"));
    let _db = Database::new(&db_path)?;
    if matches!(hive_mode, HiveRuntimeMode::DaemonProxy) {
        reconcile_transient_agent_states(&db_path)?;
    }

    let credential_store_inner = CredentialStore::load().unwrap_or_default();
    let credential_store = Arc::new(RwLock::new(credential_store_inner.clone()));
    let model_registry = create_model_registry();
    initialize_models(&model_registry, &db_path).await;
    let ai_client = create_ai_client(&credential_store_inner, &model_registry, &db_path)
        .await
        .map(Arc::new);

    let process_registry = Arc::new(ProcessRegistry::new());
    let cancellation = AgentCancellation::new();
    // Completion wake is installed after AppState is fully built (see below).

    let plugin_manager = routes::plugins::plugin_manager();
    let installed_plugins = if isolated_evaluation {
        tracing::info!("Skipping global plugin contributions for isolated evaluation");
        Vec::new()
    } else {
        match plugin_manager.ensure_layout().await {
            Ok(()) => match plugin_manager.list_installed_plugins().await {
                Ok(plugins) => plugins
                    .into_iter()
                    .filter(|plugin| plugin.enabled)
                    .collect::<Vec<_>>(),
                Err(error) => {
                    tracing::warn!(error = %error, "Failed to resolve installed plugin contributions");
                    Vec::new()
                }
            },
            Err(error) => {
                tracing::warn!(error = %error, "Failed to initialize plugin layout");
                Vec::new()
            }
        }
    };
    let mut executable_plugin_ids = HashSet::new();
    let mut mcp_plugin_authorities = HashMap::new();
    for plugin in &installed_plugins {
        if !plugin.agent_extension_paths.is_empty() || !plugin.hook_paths.is_empty() {
            match plugin_manager
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
            match plugin_manager.permission_status_for_installed(plugin).await {
                Ok(status) if status.grant_is_current => {
                    let authority =
                        McpConnectionAuthority::new(status.granted.process, status.granted.network);
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

    // Load user hooks from database
    let mut hook_manager_inner = UserHookManager::new();
    if let Ok(db) = Database::new(&db_path) {
        if let Err(e) = hook_manager_inner.load(&db) {
            tracing::warn!("Failed to load hooks: {}", e);
        }
    }
    let package_hook_configs = installed_plugins
        .iter()
        .filter(|plugin| executable_plugin_ids.contains(&plugin.id))
        .flat_map(|plugin| {
            plugin.hook_paths.iter().map(|path| {
                PackageHookConfig::new(plugin.id.clone(), path.clone(), plugin.install_path.clone())
            })
        })
        .collect();
    match hook_manager_inner.replace_package_hooks(package_hook_configs) {
        Ok(report) if report.hook_count > 0 => tracing::info!(
            config_count = report.config_count,
            hook_count = report.hook_count,
            "Loaded declarative package hooks"
        ),
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(error = %error, "Failed to initialize declarative package hooks")
        }
    }
    let hook_manager = Arc::new(RwLock::new(hook_manager_inner));

    // Build tool registry with full hook chain (matches TUI's init_tool_registry)
    let mut tool_registry_inner = ToolRegistry::new();
    tool_registry_inner.add_pre_hook(Arc::new(SafetyHook::new()));
    tool_registry_inner.add_pre_hook(Arc::new(PlanModeHook::new()));
    // Auto-classifier for Hive autonomous sessions (no-op when permission_mode != Autonomous).
    // Register even when no bootstrap client exists so later per-user Hive clients
    // are classified via ToolContext instead of bypassing the hook chain.
    register_autonomous_classifier_hook(&mut tool_registry_inner, ai_client.clone());
    tool_registry_inner.add_post_hook(Arc::new(LoggingHook::new()));
    tool_registry_inner.add_pre_hook(Arc::new(UserPreToolHook::new(hook_manager.clone())));
    tool_registry_inner.add_post_hook(Arc::new(UserPostToolHook::new(hook_manager.clone())));
    let tool_registry = Arc::new(tool_registry_inner);
    register_all_tools(&tool_registry).await;
    register_hive_tools(&tool_registry).await;
    routes::browser::register_tool(&tool_registry).await;

    let agent_extensions =
        mitsuro_core::extensions::AgentExtensionManager::new(&config.working_dir);
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

    // Register unified agent tool (explore, plan, verify, build) if AI client is available
    if let Some(ref client) = ai_client {
        register_agent_tool(&tool_registry, client.clone(), cancellation.clone()).await;
        tracing::info!("Registered unified agent sub-agent tool");
    }

    let package_configs = installed_plugins
        .iter()
        .filter_map(|plugin| {
            let path = plugin.mcp_servers_path.clone()?;
            let authority = mcp_plugin_authorities.get(&plugin.id).copied()?;
            Some(McpPackageConfig::new(path, authority))
        })
        .collect();
    let mcp_manager = initialize_mcp_manager(
        hive_mode,
        &config.working_dir,
        &tool_registry,
        package_configs,
    )
    .await;
    let tool_count = tool_registry.get_ai_tools_all().await.len();
    tracing::info!("Tool registry initialized with {} tools", tool_count);

    let push_service = if isolated_evaluation {
        None
    } else {
        match push::PushService::init(&paths::vapid_key_path(), Arc::new(db_path.clone())) {
            Ok(svc) => {
                tracing::info!("Web Push service initialized");
                Some(Arc::new(svc))
            }
            Err(e) => {
                tracing::warn!("Push notifications unavailable: {}", e);
                None
            }
        }
    };
    let apns_service = if isolated_evaluation {
        None
    } else {
        apns::ApnsService::from_env(Arc::new(db_path.clone())).map(Arc::new)
    };
    if apns_service.is_some() {
        tracing::info!("APNs service initialized");
    }
    if let Some(service) = push_service.clone() {
        tokio::spawn(async move {
            service.recover_notification_intents().await;
        });
    }
    if let Some(service) = apns_service.clone() {
        tokio::spawn(async move {
            service.recover_notification_intents().await;
        });
    }
    let remote_access = Arc::new(RwLock::new(initialize_remote_access(hive_mode, &db_path)?));
    let hive_runtime = match (hive_runtime_override, hive_mode) {
        (Some(runtime), HiveRuntimeMode::IsolatedEvaluation) => runtime,
        (Some(_), _) => {
            anyhow::bail!("a Hive runtime override is valid only for isolated evaluation")
        }
        (None, HiveRuntimeMode::DaemonProxy) => {
            hive_runtime::HiveRuntimeManager::daemon_from_discovered()
                .await
                .context("connecting mitsuro-server to the Hive daemon")?
        }
        (None, HiveRuntimeMode::ExecutionHost | HiveRuntimeMode::IsolatedEvaluation) => {
            hive_runtime::HiveRuntimeManager::execution_host()
        }
    };

    let mut skills = if isolated_evaluation {
        SkillsManager::new(config.working_dir.join(".mitsuro/evaluation-skills"), None)
    } else {
        SkillsManager::with_defaults(&config.working_dir)
    };
    for plugin in &installed_plugins {
        for path in &plugin.skill_paths {
            skills.register_package_root(&plugin.id, path.clone());
        }
    }
    skills.refresh();

    let state = AppState {
        server_port: config.port,
        db_path: Arc::new(db_path),
        working_dir: Arc::new(config.working_dir.clone()),
        ai_client,
        tool_registry,
        process_registry: Arc::clone(&process_registry),
        model_registry,
        credential_store,
        mcp_manager,
        hook_manager,
        skills_manager: Arc::new(RwLock::new(skills)),
        cancellation,
        session_locks: Arc::new(RwLock::new(HashMap::new())),
        session_inputs: Arc::new(RwLock::new(HashMap::new())),
        session_presence: Arc::new(RwLock::new(HashMap::new())),
        session_control_leases: Arc::new(RwLock::new(
            control_leases::SessionControlLeaseRegistry::default(),
        )),
        agent_streams: Arc::new(RwLock::new(agent_streams::AgentStreamHub::default())),
        delegated_state: Arc::new(RwLock::new(HashMap::new())),
        remote_access,
        device_access: Default::default(),
        active_agent_streams: Arc::new(AtomicUsize::new(0)),
        peak_rss_bytes: Arc::new(AtomicU64::new(0)),
        peak_virtual_bytes: Arc::new(AtomicU64::new(0)),
        push_service,
        apns_service,
        oauth_flows: Arc::new(Mutex::new(HashMap::new())),
        hive_runtime,
    };

    // Detached bash jobs wake their parent session on terminal status so the
    // agent does not thrash on gh/process status polls.
    process_wake::install_process_completion_wake(process_registry, state.clone()).await;
    // Background child agents wake the parent the same way (no status thrash).
    child_wake::install_child_completion_wake(
        state.tool_registry.agent_runtime_manager(),
        state.clone(),
    )
    .await;

    spawn_model_catalog_refresh(
        state.model_registry.clone(),
        state.credential_store.clone(),
        state.db_path.clone(),
    );
    if matches!(hive_mode, HiveRuntimeMode::DaemonProxy) {
        state
            .hive_runtime
            .restore_persisted_sessions(state.clone())
            .await?;
    }

    Ok(state)
}

/// Build the Axum router with all routes and embedded web assets.
pub async fn build_router(config: &ServerConfig) -> anyhow::Result<(Router, AppState)> {
    mitsuro_core::identity::import_legacy_environment();
    if config.database_path.is_none() {
        mitsuro_core::identity::require_startup_identity()
            .context("validating Mitsuro configuration authority")?;
    }
    build_router_with_runtime_mode(config, HiveRuntimeMode::DaemonProxy).await
}

async fn build_router_with_runtime_mode(
    config: &ServerConfig,
    hive_mode: HiveRuntimeMode,
) -> anyhow::Result<(Router, AppState)> {
    let isolated_evaluation = matches!(hive_mode, HiveRuntimeMode::IsolatedEvaluation);
    let http_policy = ServerHttpPolicy::default();
    let state = build_app_state(config, hive_mode, config.database_path.clone()).await?;

    let cors = http_policy.cors_layer();

    let protected_routes = if isolated_evaluation {
        Router::new().nest("/api", routes::evaluation_api_router())
    } else {
        Router::new()
            .route("/ws/terminal", get(ws::terminal::handler))
            .nest("/api", routes::api_router())
    }
    .layer(middleware::from_fn_with_state(
        state.clone(),
        auth::auth_middleware,
    ));

    let public_routes = Router::new().route("/health", get(health));
    let public_routes = if isolated_evaluation {
        public_routes
    } else {
        public_routes
            .merge(routes::oauth::callback_router())
            .merge(routes::device_pairing::public_router())
    };

    let app = public_routes
        .merge(protected_routes)
        .fallback(serve_web_app)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        // Allow large request bodies for image uploads (up to 100MB)
        .layer(DefaultBodyLimit::max(http_policy.max_request_body_bytes))
        .with_state(state.clone());

    Ok((app, state))
}

fn reconcile_transient_agent_states(db_path: &std::path::Path) -> anyhow::Result<()> {
    let session_manager = mitsuro_core::SessionManager::new(Database::new(db_path)?);
    let repaired = session_manager.reset_transient_agent_states()?;
    let cleared_recovery = session_manager.clear_stale_transient_recovery_states()?;
    let recovered_workflows = mitsuro_core::workflow::WorkflowManager::new(db_path.to_path_buf())?
        .recover_interrupted_attempts()?;
    if repaired > 0 {
        tracing::info!(
            repaired_sessions = repaired,
            "Cleared transient agent execution states during server startup"
        );
    }
    if cleared_recovery > 0 {
        tracing::info!(
            cleared_sessions = cleared_recovery,
            "Cleared stale non-resumable recovery state during server startup"
        );
    }
    if recovered_workflows > 0 {
        tracing::info!(
            recovered_goals = recovered_workflows,
            "Paused interrupted durable Goal attempts during server startup"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProcessMemorySample {
    pub rss_bytes: Option<u64>,
    pub virtual_bytes: Option<u64>,
}

pub(crate) fn read_process_memory_status() -> ProcessMemorySample {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return ProcessMemorySample::default();
    };

    let mut rss_bytes = None;
    let mut virtual_bytes = None;

    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            rss_bytes = parse_proc_status_kib_line(line).map(|value| value * 1024);
        } else if line.starts_with("VmSize:") {
            virtual_bytes = parse_proc_status_kib_line(line).map(|value| value * 1024);
        }
    }

    ProcessMemorySample {
        rss_bytes,
        virtual_bytes,
    }
}

pub(crate) fn observe_process_memory(state: &AppState) -> ProcessMemorySample {
    let sample = read_process_memory_status();
    if let Some(rss_bytes) = sample.rss_bytes {
        state.peak_rss_bytes.fetch_max(rss_bytes, Ordering::Relaxed);
    }
    if let Some(virtual_bytes) = sample.virtual_bytes {
        state
            .peak_virtual_bytes
            .fetch_max(virtual_bytes, Ordering::Relaxed);
    }
    sample
}

fn parse_proc_status_kib_line(line: &str) -> Option<u64> {
    line.split_whitespace().nth(1)?.parse::<u64>().ok()
}

/// Start the Mitsuro server and block until shutdown.
pub async fn start_server(config: ServerConfig) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("0.0.0.0:{}", config.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    start_server_with_listener(config, listener).await
}

/// Start the Mitsuro server from a pre-bound listener.
pub async fn start_server_with_listener(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    start_server_with_listener_mode(config, listener, HiveRuntimeMode::DaemonProxy).await
}

/// Start the canonical server from a pre-bound listener and drain in-flight
/// HTTP work after the supplied process shutdown signal resolves.
pub async fn start_server_with_listener_and_shutdown<F>(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let local_addr = listener.local_addr()?;
    let (app, _state) =
        build_router_with_runtime_mode(&config, HiveRuntimeMode::DaemonProxy).await?;

    tracing::info!(
        bind_address = %local_addr,
        local_url = %format!("http://localhost:{}", local_addr.port()),
        "Mitsuro server listening"
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await?;

    Ok(())
}

/// Start a desktop-managed companion that keeps the full HTTP API but does
/// not attach to a shared Hive daemon.
pub async fn start_desktop_managed_server_with_listener_and_shutdown<F>(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let local_addr = listener.local_addr()?;
    let (app, _state) =
        build_router_with_runtime_mode(&config, HiveRuntimeMode::ExecutionHost).await?;

    tracing::info!(
        bind_address = %local_addr,
        local_url = %format!("http://localhost:{}", local_addr.port()),
        "Mitsuro server listening"
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await?;

    Ok(())
}

/// Start a server that is isolated from the shared Hive control plane.
///
/// This is intended for disposable candidate evaluation. It keeps the normal
/// HTTP agent runtime and credential catalog but disables Hive daemon
/// discovery, persistent remote-access state, and project MCP auto-connect.
pub async fn start_isolated_server_with_listener(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    start_server_with_listener_mode(config, listener, HiveRuntimeMode::IsolatedEvaluation).await
}

async fn start_server_with_listener_mode(
    config: ServerConfig,
    listener: tokio::net::TcpListener,
    hive_mode: HiveRuntimeMode,
) -> anyhow::Result<()> {
    let local_addr = listener.local_addr()?;
    let (app, _state) = build_router_with_runtime_mode(&config, hive_mode).await?;

    tracing::info!(
        bind_address = %local_addr,
        local_url = %format!("http://localhost:{}", local_addr.port()),
        "Mitsuro server listening"
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

/// Serve embedded web assets with SPA fallback.
async fn serve_web_app(uri: Uri) -> impl IntoResponse {
    let http_policy = ServerHttpPolicy::default();
    let path = uri.path().trim_start_matches('/');

    // Try exact file match first
    if let Some(file) = WebAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime.as_ref())
            .header(header::CACHE_CONTROL, cache_control(path, http_policy))
            .body(Body::from(file.data.to_vec()))
            .expect("static response builder");
    }

    // SPA fallback: serve index.html for all non-file routes
    match WebAssets::get("index.html") {
        Some(index) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from(index.data.to_vec()))
            .expect("static response builder"),
        None => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(
                "Mitsuro API server running. Web frontend not embedded in this build.",
            ))
            .expect("static response builder"),
    }
}

/// Cache-control header value based on file type.
fn cache_control(path: &str, http_policy: ServerHttpPolicy) -> String {
    // Bundled immutable assets use content-hashed filenames, so cache policy lives
    // in the shared HTTP policy rather than as route-local string literals.
    http_policy.cache_control(path)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let chat_available = state.ai_client.is_some();
    let tools_available = !state.tool_registry.get_ai_tools_all().await.is_empty();

    Json(HealthResponse {
        status: "ok".to_string(),
        identity: mitsuro_core::server_instance::HEALTH_IDENTITY.to_string(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        api_protocol: API_PROTOCOL_VERSION,
        capabilities: HashMap::from([
            ("agent_streams".to_string(), 1),
            ("browser_sessions".to_string(), 1),
            ("desktop_canvases".to_string(), 1),
            ("terminal_sessions".to_string(), 2),
        ]),
        features: HashMap::from([
            ("chat".to_string(), chat_available),
            ("tools".to_string(), tools_available),
        ]),
    })
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    identity: String,
    pid: u32,
    version: String,
    api_protocol: u16,
    capabilities: HashMap<String, u16>,
    features: HashMap<String, bool>,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use axum::async_trait;
    use serde_json::{json, Value};
    use tempfile::TempDir;

    use mitsuro_core::tools::registry::{
        PermissionMode, Tool, ToolContext, ToolRegistry, ToolResult,
    };

    use super::*;

    struct TestWriteTool;

    #[test]
    fn health_payload_carries_canonical_server_identity_and_process_pid() {
        let response = HealthResponse {
            status: "ok".to_string(),
            identity: mitsuro_core::server_instance::HEALTH_IDENTITY.to_string(),
            pid: std::process::id(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            api_protocol: API_PROTOCOL_VERSION,
            capabilities: HashMap::from([
                ("agent_streams".to_string(), 1),
                ("browser_sessions".to_string(), 1),
                ("desktop_canvases".to_string(), 1),
                ("terminal_sessions".to_string(), 2),
            ]),
            features: HashMap::new(),
        };
        let value = serde_json::to_value(response).expect("health response JSON");
        assert_eq!(
            value["identity"],
            mitsuro_core::server_instance::HEALTH_IDENTITY
        );
        assert_eq!(value["pid"], std::process::id());
        assert_eq!(value["api_protocol"], API_PROTOCOL_VERSION);
        assert_eq!(value["capabilities"]["desktop_canvases"], 1);
        assert_eq!(value["capabilities"]["terminal_sessions"], 2);
    }

    #[tokio::test]
    async fn execution_host_does_not_load_daemon_cwd_mcp_config() {
        let temp = TempDir::new().expect("temporary daemon root should exist");
        fs::write(
            temp.path().join(".mcp.json"),
            r#"{
                "mcpServers": {
                    "wrong-project": {
                        "type": "url",
                        "url": "http://127.0.0.1:9/mcp"
                    }
                }
            }"#,
        )
        .expect("daemon-root MCP config should be written");
        let registry = ToolRegistry::new();

        let manager = initialize_mcp_manager(
            HiveRuntimeMode::ExecutionHost,
            temp.path(),
            &registry,
            Vec::new(),
        )
        .await;

        assert!(manager.list_servers().await.is_empty());
        assert!(registry
            .get_ai_tools_all()
            .await
            .iter()
            .all(|tool| !tool.name.starts_with("mcp__")));
    }

    #[test]
    fn execution_host_remote_access_is_ephemeral_and_does_not_write_preferences() {
        use mitsuro_core::storage::Preferences;

        let temp = TempDir::new().expect("temporary daemon root should exist");
        let db_path = temp.path().join("mitsuro.db");
        let _db = Database::new(&db_path).expect("test database should initialize");

        let config = initialize_remote_access(HiveRuntimeMode::ExecutionHost, &db_path)
            .expect("execution host placeholder should initialize");

        assert!(!config.enabled);
        assert!(config.token.is_empty());
        let preferences =
            Preferences::new(Database::new(&db_path).expect("test database should reopen"));
        assert!(preferences.get("server_remote_access_enabled").is_none());
        assert!(preferences.get("server_remote_access_token").is_none());
    }

    #[async_trait]
    impl Tool for TestWriteTool {
        fn name(&self) -> &str {
            "write"
        }

        fn description(&self) -> &str {
            "test write tool"
        }

        fn parameters_schema(&self) -> Value {
            json!({"type": "object"})
        }

        async fn execute(&self, _params: Value, _ctx: &ToolContext) -> ToolResult {
            ToolResult::success_data(json!({"executed": true}))
        }
    }

    #[tokio::test]
    async fn no_bootstrap_tool_registry_uses_deterministic_autonomous_policy() {
        let mut registry = ToolRegistry::new();
        register_autonomous_classifier_hook(&mut registry, None);
        let registry = Arc::new(registry);
        registry.register(Arc::new(TestWriteTool)).await;
        let ctx = ToolContext {
            permission_mode: PermissionMode::Autonomous,
            ..Default::default()
        };

        let result = registry
            .execute(
                "write",
                json!({"path": "src/lib.rs", "content": "unsafe autonomous write"}),
                &ctx,
            )
            .await
            .expect("test write tool should be registered");

        assert!(
            !result.is_error,
            "workspace mutation should not require a classifier model: {}",
            result.output
        );

        let blocked = registry
            .execute(
                "write",
                json!({"path": "/etc/shadow", "content": "unsafe autonomous write"}),
                &ctx,
            )
            .await
            .expect("test write tool should be registered");

        assert!(blocked.is_error, "sensitive path must fail closed");
        assert!(
            blocked.output.contains("credential or system path"),
            "unexpected result output: {}",
            blocked.output
        );
    }
}
