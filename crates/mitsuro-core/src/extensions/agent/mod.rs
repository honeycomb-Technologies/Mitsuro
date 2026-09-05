//! Executable agent extensions.
//!
//! This host is intentionally separate from the Zed-compatible WASM extension
//! ABI. Zed extensions add editor/language functionality; agent extensions add
//! coding-agent tools, slash commands, lifecycle observers, and turn context.

mod manifest;
mod process;
mod tool;
mod trust;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock as StdRwLock};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

use crate::agent::LoopEvent;
use crate::ai::models::{ModelKey, ResolvedModelRuntime};
use crate::extensions::bun_runtime::BunRuntime;
use crate::tools::{ToolContext, ToolRegistry};

pub use manifest::{AgentExtensionManifest, AgentExtensionPermissions};
use process::{AgentExtensionProcess, RegisteredCommand, RegisteredTool};
use tool::AgentExtensionTool;
pub use trust::ProjectAgentExtensionTrustStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentExtensionScope {
    Global,
    Project,
    Package,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExtensionRoot {
    pub path: PathBuf,
    pub scope: AgentExtensionScope,
}

impl AgentExtensionRoot {
    pub fn new(path: impl Into<PathBuf>, scope: AgentExtensionScope) -> Self {
        Self {
            path: path.into(),
            scope,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentExtensionDiagnostic {
    pub path: PathBuf,
    pub extension_id: Option<String>,
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentExtensionCommand {
    pub name: String,
    pub description: String,
    pub extension_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentExtensionStatus {
    pub id: String,
    pub name: String,
    pub version: String,
    pub scope: AgentExtensionScope,
    pub path: PathBuf,
    pub tools: Vec<String>,
    pub commands: Vec<String>,
    pub events: Vec<String>,
    pub contributes_context: bool,
}

/// Result of executable extension interception before a tool call. Extensions
/// may narrow/normalize arguments or block; central tool governance and safety
/// hooks still run afterward against the effective arguments.
#[derive(Debug, Clone)]
pub struct AgentExtensionToolIntercept {
    pub params: Value,
    pub block_reason: Option<String>,
}

/// Stable, privacy-conscious context exposed to extension callbacks.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ExtensionCallContext {
    pub working_dir: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_key: Option<ModelKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_catalog_revision: Option<String>,
    pub permission_mode: String,
    pub plan_mode: bool,
}

impl ExtensionCallContext {
    pub fn from_tool_context(context: &ToolContext) -> Self {
        Self {
            working_dir: context.working_dir.clone(),
            project_dir: context.project_dir.clone(),
            session_id: context.session_id.clone(),
            model: context.current_model.clone(),
            model_key: context.current_model_key.clone(),
            model_catalog_revision: context
                .ai_client
                .as_ref()
                .and_then(|client| client.resolved_model().catalog_revision.clone()),
            permission_mode: format!("{:?}", context.permission_mode).to_ascii_lowercase(),
            plan_mode: context.plan_mode,
        }
    }

    pub fn for_turn(
        working_dir: PathBuf,
        project_dir: Option<PathBuf>,
        session_id: Option<String>,
        model: Option<String>,
        permission_mode: impl Into<String>,
        plan_mode: bool,
    ) -> Self {
        Self {
            working_dir,
            project_dir,
            session_id,
            model,
            model_key: None,
            model_catalog_revision: None,
            permission_mode: permission_mode.into(),
            plan_mode,
        }
    }

    pub fn for_resolved_turn(
        working_dir: PathBuf,
        project_dir: Option<PathBuf>,
        session_id: Option<String>,
        runtime: &ResolvedModelRuntime,
        permission_mode: impl Into<String>,
        plan_mode: bool,
    ) -> Self {
        Self {
            working_dir,
            project_dir,
            session_id,
            model: Some(runtime.wire_model_id.clone()),
            model_key: Some(runtime.key.clone()),
            model_catalog_revision: runtime.catalog_revision.clone(),
            permission_mode: permission_mode.into(),
            plan_mode,
        }
    }
}

struct LoadedAgentExtension {
    manifest: AgentExtensionManifest,
    scope: AgentExtensionScope,
    root_path: PathBuf,
    process: Arc<Mutex<AgentExtensionProcess>>,
    commands: Vec<RegisteredCommand>,
    events: BTreeSet<String>,
    context_hook: bool,
    registered_tools: Vec<String>,
}

struct PreparedAgentExtension {
    manifest: AgentExtensionManifest,
    scope: AgentExtensionScope,
    root_path: PathBuf,
    process: Arc<Mutex<AgentExtensionProcess>>,
    commands: Vec<RegisteredCommand>,
    events: BTreeSet<String>,
    context_hook: bool,
    tools: Vec<RegisteredTool>,
}

/// Shared manager for project, global, and package-provided agent extensions.
pub struct AgentExtensionManager {
    working_dir: PathBuf,
    runtime_dir: PathBuf,
    bun_runtime: BunRuntime,
    /// Serializes trust/root mutation with discovery, worker activation, and
    /// registry commits so an older refresh cannot win after revocation.
    lifecycle: Mutex<()>,
    roots: RwLock<Vec<AgentExtensionRoot>>,
    loaded: RwLock<BTreeMap<String, Arc<LoadedAgentExtension>>>,
    diagnostics: RwLock<Vec<AgentExtensionDiagnostic>>,
    observed_events: StdRwLock<BTreeSet<String>>,
    project_trust: trust::ProjectAgentExtensionTrustStore,
    #[cfg(test)]
    test_tool_interceptor: StdRwLock<Option<TestToolInterceptor>>,
}

#[cfg(test)]
type TestToolInterceptor =
    Arc<dyn Fn(&str, Value) -> AgentExtensionToolIntercept + Send + Sync + 'static>;

impl AgentExtensionManager {
    pub fn new(working_dir: impl Into<PathBuf>) -> Arc<Self> {
        let config_dir = crate::paths::config_dir();
        Self::new_with_paths(
            working_dir,
            config_dir.join("extensions").join("agent-runtime"),
            config_dir.join("extensions").join("agent"),
        )
    }

    pub fn new_with_paths(
        working_dir: impl Into<PathBuf>,
        runtime_dir: impl Into<PathBuf>,
        global_root: impl Into<PathBuf>,
    ) -> Arc<Self> {
        let working_dir = working_dir.into();
        let runtime_dir = runtime_dir.into();
        let roots = vec![
            AgentExtensionRoot::new(global_root, AgentExtensionScope::Global),
            AgentExtensionRoot::new(
                crate::identity::legacy_project_state_dir(&working_dir).join("extensions"),
                AgentExtensionScope::Project,
            ),
            AgentExtensionRoot::new(
                crate::paths::project_state_dir(&working_dir).join("extensions"),
                AgentExtensionScope::Project,
            ),
        ];
        Arc::new(Self {
            working_dir,
            project_trust: trust::ProjectAgentExtensionTrustStore::new(
                runtime_dir.join("project-trust.json"),
            ),
            runtime_dir,
            bun_runtime: BunRuntime::new(reqwest::Client::new(), crate::paths::config_dir()),
            lifecycle: Mutex::new(()),
            roots: RwLock::new(roots),
            loaded: RwLock::new(BTreeMap::new()),
            diagnostics: RwLock::new(Vec::new()),
            observed_events: StdRwLock::new(BTreeSet::new()),
            #[cfg(test)]
            test_tool_interceptor: StdRwLock::new(None),
        })
    }

    #[cfg(test)]
    pub(crate) fn set_test_tool_interceptor(
        &self,
        interceptor: impl Fn(&str, Value) -> AgentExtensionToolIntercept + Send + Sync + 'static,
    ) {
        *self
            .test_tool_interceptor
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(interceptor));
    }

    pub async fn register_root(&self, root: AgentExtensionRoot) {
        let _lifecycle = self.lifecycle.lock().await;
        self.register_root_locked(root).await;
    }

    async fn register_root_locked(&self, root: AgentExtensionRoot) {
        let mut roots = self.roots.write().await;
        if !roots.iter().any(|existing| existing.path == root.path) {
            roots.push(root);
        }
    }

    /// Replace every package-contributed root from the current enabled plugin
    /// snapshot. Global and project roots are retained.
    pub async fn set_package_roots(&self, mut paths: Vec<PathBuf>) {
        let _lifecycle = self.lifecycle.lock().await;
        self.set_package_roots_locked(&mut paths).await;
    }

    async fn set_package_roots_locked(&self, paths: &mut Vec<PathBuf>) {
        paths.sort();
        paths.dedup();
        let mut roots = self.roots.write().await;
        roots.retain(|root| root.scope != AgentExtensionScope::Package);
        roots.extend(
            paths
                .drain(..)
                .map(|path| AgentExtensionRoot::new(path, AgentExtensionScope::Package)),
        );
    }

    /// Atomically replace package roots and commit the matching worker/tool
    /// snapshot under the same lifecycle guard.
    pub async fn set_package_roots_and_refresh(
        self: &Arc<Self>,
        mut paths: Vec<PathBuf>,
        registry: &Arc<ToolRegistry>,
    ) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        self.set_package_roots_locked(&mut paths).await;
        self.refresh_and_register_locked(registry).await
    }

    pub async fn roots(&self) -> Vec<AgentExtensionRoot> {
        self.roots.read().await.clone()
    }

    pub async fn diagnostics(&self) -> Vec<AgentExtensionDiagnostic> {
        self.diagnostics.read().await.clone()
    }

    pub async fn loaded_ids(&self) -> Vec<String> {
        self.loaded.read().await.keys().cloned().collect()
    }

    /// Project extension trust is stored outside the repository. A project's
    /// own `.mitsuro/settings.json` can narrow an existing grant but can never
    /// grant code-execution authority to itself.
    pub fn project_trust_status(&self) -> Result<ProjectAgentExtensionTrustStatus> {
        self.project_trust.status(&self.working_dir)
    }

    pub async fn set_project_trusted(
        &self,
        trusted: bool,
    ) -> Result<ProjectAgentExtensionTrustStatus> {
        let _lifecycle = self.lifecycle.lock().await;
        self.project_trust.set_trusted(&self.working_dir, trusted)
    }

    /// Atomically change project trust and commit the corresponding worker/tool
    /// snapshot before returning to the caller.
    pub async fn set_project_trusted_and_refresh(
        self: &Arc<Self>,
        trusted: bool,
        registry: &Arc<ToolRegistry>,
    ) -> Result<ProjectAgentExtensionTrustStatus> {
        let _lifecycle = self.lifecycle.lock().await;
        let status = self.project_trust.set_trusted(&self.working_dir, trusted)?;
        self.refresh_and_register_locked(registry).await?;
        Ok(status)
    }

    pub async fn statuses(&self) -> Vec<AgentExtensionStatus> {
        self.loaded
            .read()
            .await
            .values()
            .map(|extension| AgentExtensionStatus {
                id: extension.manifest.id.clone(),
                name: extension.manifest.name.clone(),
                version: extension.manifest.version.clone(),
                scope: extension.scope,
                path: extension.root_path.clone(),
                tools: extension.registered_tools.clone(),
                commands: extension
                    .commands
                    .iter()
                    .map(|command| command.name.clone())
                    .collect(),
                events: extension.events.iter().cloned().collect(),
                contributes_context: extension.context_hook,
            })
            .collect()
    }

    pub async fn commands(&self) -> Vec<AgentExtensionCommand> {
        let loaded = self.loaded.read().await;
        let mut commands = loaded
            .values()
            .flat_map(|extension| {
                extension
                    .commands
                    .iter()
                    .map(|command| AgentExtensionCommand {
                        name: command.name.clone(),
                        description: command.description.clone(),
                        extension_id: extension.manifest.id.clone(),
                    })
            })
            .collect::<Vec<_>>();
        commands.sort_by(|left, right| left.name.cmp(&right.name));
        commands
    }

    /// Refresh workers while preserving the last-known-good instance whenever
    /// an edited extension fails validation or startup.
    pub async fn refresh_and_register(
        self: &Arc<Self>,
        registry: &Arc<ToolRegistry>,
    ) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        self.refresh_and_register_locked(registry).await
    }

    async fn refresh_and_register_locked(
        self: &Arc<Self>,
        registry: &Arc<ToolRegistry>,
    ) -> Result<()> {
        let previous = self.loaded.read().await.clone();
        self.diagnostics.write().await.clear();
        let roots = self.roots.read().await.clone();
        let project_trusted = match self.project_trust_status() {
            Ok(status) => status.trusted,
            Err(error) => {
                self.record_diagnostic(
                    &self.working_dir,
                    None,
                    "error",
                    format!("Failed to read project extension trust: {error}"),
                )
                .await;
                false
            }
        };
        let mut discovered = Vec::new();
        let mut failed_discovery = BTreeMap::new();
        for root in roots {
            if root.scope == AgentExtensionScope::Project && !project_trusted {
                if root.path.exists() {
                    self.record_diagnostic(
                        &root.path,
                        None,
                        "info",
                        "Project agent extensions are disabled until this project is trusted from the user-owned trust store".to_string(),
                    )
                    .await;
                }
                continue;
            }
            match discover_root(&root).await {
                Ok((mut candidates, issues)) => {
                    discovered.append(&mut candidates);
                    for issue in issues {
                        let inferred_id = issue.extension_id.or_else(|| {
                            previous
                                .iter()
                                .find(|(_, extension)| extension.root_path == issue.path)
                                .map(|(id, _)| id.clone())
                        });
                        if let Some(extension_id) = inferred_id.as_ref() {
                            failed_discovery.insert(extension_id.clone(), issue.scope);
                        }
                        self.record_diagnostic(&issue.path, inferred_id, "error", issue.message)
                            .await;
                    }
                }
                Err(error) => {
                    self.record_diagnostic(&root.path, None, "error", error.to_string())
                        .await;
                }
            }
        }
        discovered.sort_by(|left, right| {
            scope_rank(left.scope)
                .cmp(&scope_rank(right.scope))
                .then_with(|| left.manifest.id.cmp(&right.manifest.id))
        });

        // Later (project) scopes replace earlier (global/package) ids.
        let mut selected = BTreeMap::new();
        for candidate in discovered {
            selected.insert(candidate.manifest.id.clone(), candidate);
        }

        let mut preserved_after_discovery_failure = BTreeMap::new();
        for (extension_id, failed_scope) in failed_discovery {
            let Some(previous_extension) = previous.get(&extension_id) else {
                continue;
            };
            let replacement_is_higher_precedence = selected
                .get(&extension_id)
                .is_some_and(|candidate| scope_rank(candidate.scope) > scope_rank(failed_scope));
            if previous_extension.scope == failed_scope && !replacement_is_higher_precedence {
                selected.remove(&extension_id);
                preserved_after_discovery_failure.insert(extension_id, previous_extension.clone());
            }
        }

        let selected_ids = selected
            .keys()
            .chain(preserved_after_discovery_failure.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        let project_settings = crate::storage::ProjectSettings::load(&self.working_dir);
        let mut next = preserved_after_discovery_failure;
        for (extension_id, candidate) in selected {
            if !candidate.manifest.enabled
                || !project_settings.allows_agent_extension(&extension_id)
            {
                if let Some(previous) = previous.get(&extension_id) {
                    unregister_extension_tools(registry, previous).await;
                }
                if candidate.manifest.enabled {
                    self.record_diagnostic(
                        &candidate.extension_dir,
                        Some(extension_id),
                        "info",
                        "Agent extension disabled by .mitsuro/settings.json policy".to_string(),
                    )
                    .await;
                }
                continue;
            }
            match self.prepare_candidate(candidate).await {
                Ok(prepared) => {
                    if let Some(previous) = previous.get(&extension_id) {
                        unregister_extension_tools(registry, previous).await;
                    }
                    let extension = activate_extension(prepared, registry).await;
                    next.insert(extension.manifest.id.clone(), Arc::new(extension));
                }
                Err(error) => {
                    self.record_diagnostic(
                        &error.path,
                        error.extension_id,
                        "error",
                        error.source.to_string(),
                    )
                    .await;
                    if let Some(previous) = previous.get(&extension_id) {
                        next.insert(extension_id, previous.clone());
                    }
                }
            }
        }
        for (extension_id, extension) in &previous {
            if !selected_ids.contains(extension_id) {
                unregister_extension_tools(registry, extension).await;
            }
        }
        let observed_events = next
            .values()
            .flat_map(|extension| extension.events.iter().cloned())
            .collect();
        *self
            .observed_events
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = observed_events;
        *self.loaded.write().await = next;
        Ok(())
    }

    pub fn observes_loop_event(&self, event: &LoopEvent) -> bool {
        let events = self
            .observed_events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events.contains("*") || events.contains(loop_event_name(event))
    }

    /// Whether any loaded extension can rewrite or block a tool call. The
    /// agent executor uses this to run interception before authorization so
    /// the approval prompt is bound to the exact effective arguments.
    pub fn has_tool_interceptors(&self) -> bool {
        #[cfg(test)]
        if self
            .test_tool_interceptor
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
        {
            return true;
        }

        let events = self
            .observed_events
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        events.contains("tool.execute.before") || events.contains("tool_call")
    }

    async fn prepare_candidate(
        &self,
        candidate: DiscoveredExtension,
    ) -> std::result::Result<PreparedAgentExtension, CandidateLoadError> {
        let extension_id = candidate.manifest.id.clone();
        let error_path = candidate.extension_dir.clone();
        let result = async {
            let process = AgentExtensionProcess::start(
                &self.bun_runtime,
                &self.runtime_dir,
                &candidate.extension_dir,
                &candidate.entry,
                &candidate.manifest,
                &self.working_dir,
            )
            .await?;
            validate_registrations(&candidate.manifest.id, &process.ready)?;
            let commands = process.ready.commands.clone();
            let events = process.ready.events.iter().cloned().collect();
            let context_hook = process.ready.context_hook;
            let tools = process.ready.tools.clone();
            let process = Arc::new(Mutex::new(process));

            Ok::<_, anyhow::Error>(PreparedAgentExtension {
                manifest: candidate.manifest,
                scope: candidate.scope,
                root_path: candidate.extension_dir,
                process,
                commands,
                events,
                context_hook,
                tools,
            })
        }
        .await;

        result.map_err(|source| CandidateLoadError {
            path: error_path,
            extension_id: Some(extension_id),
            source,
        })
    }

    pub async fn execute_command(
        &self,
        name: &str,
        argument: &str,
        context: &ExtensionCallContext,
    ) -> Result<Value> {
        let normalized = name.trim_start_matches('/');
        let extension = {
            let loaded = self.loaded.read().await;
            loaded
                .values()
                .find(|extension| {
                    extension
                        .commands
                        .iter()
                        .any(|command| command.name == normalized)
                })
                .cloned()
        }
        .with_context(|| format!("unknown agent extension command '/{normalized}'"))?;

        let result = extension
            .process
            .lock()
            .await
            .call_command(normalized, argument, context)
            .await;
        result
    }

    pub async fn context_for_turn(&self, context: &ExtensionCallContext) -> Vec<String> {
        let extensions = self
            .loaded
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut additions = Vec::new();
        for extension in extensions {
            if !extension.context_hook {
                continue;
            }
            match extension.process.lock().await.call_context(context).await {
                Ok(value) => collect_context_strings(&value, &mut additions),
                Err(error) => {
                    tracing::warn!(
                        extension_id = %extension.manifest.id,
                        error = %error,
                        "Agent extension context hook failed"
                    );
                }
            }
        }
        bound_context_additions(additions)
    }

    pub async fn dispatch_event(&self, event: LoopEvent, context: ExtensionCallContext) {
        // `tool_result` is delivered synchronously from ToolRegistry with the
        // tool name, effective input, and result. The later presentation event
        // lacks that data and would otherwise invoke the same handler twice.
        if matches!(&event, LoopEvent::ToolResult { .. }) {
            return;
        }
        let name = loop_event_name(&event);
        let payload = match serde_json::to_value(event) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(error = %error, "Failed to serialize extension lifecycle event");
                return;
            }
        };
        let extensions = self
            .loaded
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for extension in extensions {
            if !extension.events.contains(name) && !extension.events.contains("*") {
                continue;
            }
            if let Err(error) = extension
                .process
                .lock()
                .await
                .call_event(name, payload.clone(), &context)
                .await
            {
                tracing::warn!(
                    extension_id = %extension.manifest.id,
                    event = name,
                    error = %error,
                    "Agent extension lifecycle hook failed"
                );
            }
        }
    }

    /// Run Pi-style `tool_call` and OpenCode-style `tool.execute.before`
    /// interceptors in deterministic extension order.
    pub async fn before_tool(
        &self,
        name: &str,
        mut params: Value,
        context: &ToolContext,
    ) -> AgentExtensionToolIntercept {
        #[cfg(test)]
        {
            let interceptor = self
                .test_tool_interceptor
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if let Some(interceptor) = interceptor {
                return interceptor(name, params);
            }
        }

        let extensions = self
            .loaded
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let context = ExtensionCallContext::from_tool_context(context);
        let mut block_reason = None;

        for extension in extensions {
            for event_name in ["tool.execute.before", "tool_call"] {
                if !extension.events.contains(event_name) {
                    continue;
                }
                let event = if event_name == "tool.execute.before" {
                    serde_json::json!({
                        "input": { "tool": name },
                        "output": { "args": params.clone() }
                    })
                } else {
                    serde_json::json!({ "toolName": name, "input": params.clone() })
                };
                match extension
                    .process
                    .lock()
                    .await
                    .call_event(event_name, event, &context)
                    .await
                {
                    Ok(value) => {
                        apply_tool_intercept_result(&value, &mut params, &mut block_reason);
                        if block_reason.is_some() {
                            return AgentExtensionToolIntercept {
                                params,
                                block_reason,
                            };
                        }
                    }
                    Err(error) => tracing::warn!(
                        extension_id = %extension.manifest.id,
                        event = event_name,
                        error = %error,
                        "Agent extension tool interceptor failed"
                    ),
                }
            }
        }

        AgentExtensionToolIntercept {
            params,
            block_reason,
        }
    }

    /// Notify Pi/OpenCode-compatible post-tool observers. Post handlers are
    /// observational: they cannot rewrite the canonical result retained by the
    /// agent runtime.
    pub async fn after_tool(
        &self,
        name: &str,
        params: &Value,
        result: &crate::tools::ToolResult,
        context: &ToolContext,
    ) {
        let extensions = self
            .loaded
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let context = ExtensionCallContext::from_tool_context(context);
        for extension in extensions {
            for event_name in ["tool.execute.after", "tool_result"] {
                if !extension.events.contains(event_name) {
                    continue;
                }
                let event = if event_name == "tool.execute.after" {
                    serde_json::json!({
                        "input": { "tool": name },
                        "output": {
                            "args": params,
                            "result": {
                                "output": result.output.clone(),
                                "isError": result.is_error
                            }
                        }
                    })
                } else {
                    serde_json::json!({
                        "toolName": name,
                        "input": params,
                        "result": {
                            "output": result.output.clone(),
                            "isError": result.is_error
                        }
                    })
                };
                if let Err(error) = extension
                    .process
                    .lock()
                    .await
                    .call_event(event_name, event, &context)
                    .await
                {
                    tracing::warn!(
                        extension_id = %extension.manifest.id,
                        event = event_name,
                        error = %error,
                        "Agent extension post-tool observer failed"
                    );
                }
            }
        }
    }

    async fn record_diagnostic(
        &self,
        path: &Path,
        extension_id: Option<String>,
        level: &str,
        message: String,
    ) {
        self.diagnostics
            .write()
            .await
            .push(AgentExtensionDiagnostic {
                path: path.to_path_buf(),
                extension_id,
                level: level.to_string(),
                message,
            });
    }
}

fn apply_tool_intercept_result(
    value: &Value,
    params: &mut Value,
    block_reason: &mut Option<String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                apply_tool_intercept_result(value, params, block_reason);
                if block_reason.is_some() {
                    break;
                }
            }
        }
        Value::Object(object) => {
            if object.get("block").and_then(Value::as_bool) == Some(true) {
                *block_reason = Some(
                    object
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or("Blocked by agent extension")
                        .to_string(),
                );
                return;
            }
            if let Some(effective) = object
                .get("args")
                .or_else(|| object.get("params"))
                .filter(|value| value.is_object())
            {
                *params = effective.clone();
            }
        }
        _ => {}
    }
}

async fn activate_extension(
    prepared: PreparedAgentExtension,
    registry: &Arc<ToolRegistry>,
) -> LoadedAgentExtension {
    let mut registered_tools = Vec::new();
    for definition in prepared.tools {
        let public_name =
            resolve_public_tool_name(registry, &prepared.manifest.id, &definition.name).await;
        registry
            .register(Arc::new(AgentExtensionTool::new(
                public_name.clone(),
                definition.name,
                prepared.manifest.id.clone(),
                definition.description,
                definition.parameters,
                prepared.process.clone(),
            )))
            .await;
        registered_tools.push(public_name);
    }

    LoadedAgentExtension {
        manifest: prepared.manifest,
        scope: prepared.scope,
        root_path: prepared.root_path,
        process: prepared.process,
        commands: prepared.commands,
        events: prepared.events,
        context_hook: prepared.context_hook,
        registered_tools,
    }
}

async fn unregister_extension_tools(registry: &ToolRegistry, extension: &LoadedAgentExtension) {
    for name in &extension.registered_tools {
        registry.unregister(name).await;
    }
}

struct DiscoveredExtension {
    manifest: AgentExtensionManifest,
    scope: AgentExtensionScope,
    extension_dir: PathBuf,
    entry: PathBuf,
}

struct CandidateLoadError {
    path: PathBuf,
    extension_id: Option<String>,
    source: anyhow::Error,
}

struct DiscoveryIssue {
    path: PathBuf,
    scope: AgentExtensionScope,
    extension_id: Option<String>,
    message: String,
}

async fn discover_root(
    root: &AgentExtensionRoot,
) -> Result<(Vec<DiscoveredExtension>, Vec<DiscoveryIssue>)> {
    if !root.path.exists() {
        return Ok((Vec::new(), Vec::new()));
    }
    if is_script(&root.path) {
        return match discover_path(root, &root.path).await {
            Ok(Some(candidate)) => Ok((vec![candidate], Vec::new())),
            Ok(None) => Ok((Vec::new(), Vec::new())),
            Err(error) => Ok((
                Vec::new(),
                vec![DiscoveryIssue {
                    path: root.path.clone(),
                    scope: root.scope,
                    extension_id: None,
                    message: error.to_string(),
                }],
            )),
        };
    }
    if !root.path.is_dir() {
        bail!(
            "agent extension root is not a directory: {}",
            root.path.display()
        );
    }

    if extension_manifest_path(&root.path).is_some() {
        return match discover_path(root, &root.path).await {
            Ok(Some(candidate)) => Ok((vec![candidate], Vec::new())),
            Ok(None) => Ok((Vec::new(), Vec::new())),
            Err(error) => Ok((
                Vec::new(),
                vec![DiscoveryIssue {
                    path: root.path.clone(),
                    scope: root.scope,
                    extension_id: None,
                    message: error.to_string(),
                }],
            )),
        };
    }

    let mut entries = tokio::fs::read_dir(&root.path).await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        paths.push(entry.path());
    }
    paths.sort();

    let mut discovered = Vec::new();
    let mut issues = Vec::new();
    for path in paths {
        let candidate = discover_path(root, &path).await;
        match candidate {
            Ok(Some(candidate)) => discovered.push(candidate),
            Ok(None) => {}
            Err(error) => issues.push(DiscoveryIssue {
                path: path.clone(),
                scope: root.scope,
                extension_id: None,
                message: error.to_string(),
            }),
        }
    }
    Ok((discovered, issues))
}

async fn discover_path(
    root: &AgentExtensionRoot,
    path: &Path,
) -> Result<Option<DiscoveredExtension>> {
    if path.is_dir() {
        let Some(manifest_path) = extension_manifest_path(path) else {
            return Ok(None);
        };
        let manifest = AgentExtensionManifest::from_json_file(&manifest_path).await?;
        let entry = manifest.validate_and_resolve(path)?;
        Ok(Some(DiscoveredExtension {
            manifest,
            scope: root.scope,
            extension_dir: path.to_path_buf(),
            entry,
        }))
    } else if is_script(path) {
        let manifest = AgentExtensionManifest::from_entry(path)?;
        let extension_dir = path.parent().unwrap_or(&root.path).to_path_buf();
        let entry = path.canonicalize()?;
        Ok(Some(DiscoveredExtension {
            manifest,
            scope: root.scope,
            extension_dir,
            entry,
        }))
    } else {
        Ok(None)
    }
}

fn extension_manifest_path(directory: &Path) -> Option<PathBuf> {
    let canonical = directory.join(crate::identity::AGENT_EXTENSION_MANIFEST_FILE_NAME);
    if canonical.is_file() {
        return Some(canonical);
    }
    let deprecated = directory.join(crate::identity::legacy::AGENT_EXTENSION_MANIFEST_FILE_NAME);
    deprecated.is_file().then_some(deprecated)
}

fn is_script(path: &Path) -> bool {
    path.is_file()
        && matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("js" | "mjs" | "cjs" | "ts" | "mts" | "cts")
        )
}

fn scope_rank(scope: AgentExtensionScope) -> u8 {
    match scope {
        AgentExtensionScope::Package => 0,
        AgentExtensionScope::Global => 1,
        AgentExtensionScope::Project => 2,
    }
}

fn validate_registrations(extension_id: &str, ready: &process::ReadyMessage) -> Result<()> {
    let mut names = BTreeSet::new();
    for tool in &ready.tools {
        validate_registration_name(extension_id, "tool", &tool.name)?;
        if tool.description.trim().is_empty() {
            bail!(
                "agent extension '{extension_id}' tool '{}' has no description",
                tool.name
            );
        }
        if !tool.parameters.is_object() {
            bail!(
                "agent extension '{extension_id}' tool '{}' has a non-object JSON schema",
                tool.name
            );
        }
        if !names.insert((&tool.name, "tool")) {
            bail!(
                "agent extension '{extension_id}' registered tool '{}' twice",
                tool.name
            );
        }
    }
    for command in &ready.commands {
        validate_registration_name(extension_id, "command", &command.name)?;
        if !names.insert((&command.name, "command")) {
            bail!(
                "agent extension '{extension_id}' registered command '{}' twice",
                command.name
            );
        }
    }
    Ok(())
}

fn validate_registration_name(extension_id: &str, kind: &str, name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        bail!(
            "agent extension '{extension_id}' {kind} name '{name}' must contain only letters, numbers, '_' or '-'"
        );
    }
    Ok(())
}

async fn resolve_public_tool_name(
    registry: &ToolRegistry,
    extension_id: &str,
    requested: &str,
) -> String {
    if registry.get(requested).await.is_none() {
        requested.to_string()
    } else {
        format!(
            "ext__{}__{}",
            extension_id.replace(['-', '.'], "_"),
            requested.replace('-', "_")
        )
    }
}

fn collect_context_strings(value: &Value, output: &mut Vec<String>) {
    match value {
        Value::String(text) if !text.trim().is_empty() => output.push(text.clone()),
        Value::Array(values) => {
            for value in values {
                collect_context_strings(value, output);
            }
        }
        Value::Object(map) => {
            if let Some(Value::String(text)) = map.get("text") {
                if !text.trim().is_empty() {
                    output.push(text.clone());
                }
            }
        }
        _ => {}
    }
}

fn bound_context_additions(additions: Vec<String>) -> Vec<String> {
    const MAX_ADDITIONS: usize = 16;
    const MAX_TOTAL_BYTES: usize = 32 * 1024;
    let mut remaining = MAX_TOTAL_BYTES;
    let mut bounded = Vec::new();
    for mut addition in additions.into_iter().take(MAX_ADDITIONS) {
        if remaining == 0 {
            break;
        }
        if addition.len() > remaining {
            let mut boundary = remaining;
            while boundary > 0 && !addition.is_char_boundary(boundary) {
                boundary -= 1;
            }
            addition.truncate(boundary);
        }
        remaining = remaining.saturating_sub(addition.len());
        if !addition.trim().is_empty() {
            bounded.push(addition);
        }
    }
    bounded
}

fn loop_event_name(event: &LoopEvent) -> &'static str {
    match event {
        LoopEvent::TextDelta { .. } => "text_delta",
        LoopEvent::TextDeltaWithCitations { .. } => "text_delta_with_citations",
        LoopEvent::ThinkingDelta { .. } => "thinking_delta",
        LoopEvent::ThinkingComplete { .. } => "thinking_complete",
        LoopEvent::ToolCallStart { .. } => "tool_call_start",
        LoopEvent::ToolCallPreparing { .. } => "tool_call_preparing",
        LoopEvent::ToolCallComplete { .. } => "tool_call_complete",
        LoopEvent::ToolExecuting { .. } => "tool_executing",
        LoopEvent::ToolOutputDelta { .. } => "tool_output_delta",
        LoopEvent::ToolResult { .. } => "tool_result",
        LoopEvent::AwaitingInput { .. } => "awaiting_input",
        LoopEvent::ToolApprovalRequired { .. } => "tool_approval_required",
        LoopEvent::ToolApproved { .. } => "tool_approved",
        LoopEvent::ToolDenied { .. } => "tool_denied",
        LoopEvent::SteeringInjected { .. } => "steering_injected",
        LoopEvent::ServerToolStart { .. } => "server_tool_start",
        LoopEvent::ServerToolComplete { .. } => "server_tool_complete",
        LoopEvent::WebSearchResults { .. } => "web_search_results",
        LoopEvent::WebFetchResult { .. } => "web_fetch_result",
        LoopEvent::ServerToolError { .. } => "server_tool_error",
        LoopEvent::ModeChange { .. } => "mode_change",
        LoopEvent::PlanUpdate { .. } => "plan_update",
        LoopEvent::WorkflowUpdated { .. } => "workflow_updated",
        LoopEvent::PlanComplete { .. } => "plan_complete",
        LoopEvent::AgentSleeping { .. } => "agent_sleeping",
        LoopEvent::TurnComplete { .. } => "turn_complete",
        LoopEvent::RunBudgetResolved { .. } => "run_budget_resolved",
        LoopEvent::ProviderRequestPrepared { .. } => "provider_request_prepared",
        LoopEvent::MicrocompactionApplied { .. } => "microcompaction_applied",
        LoopEvent::ProgressGuard { .. } => "progress_guard",
        LoopEvent::TickInjected { .. } => "tick_injected",
        LoopEvent::Usage { .. } => "usage",
        LoopEvent::SessionPinched { .. } => "session_pinched",
        LoopEvent::ContextCompactionStarted { .. } => "context_compaction_started",
        LoopEvent::ContextCompacted { .. } => "context_compacted",
        LoopEvent::TitleGenerated { .. } => "title_generated",
        LoopEvent::Finished { .. } => "finished",
        LoopEvent::Error { .. } => "error",
        LoopEvent::AgentBackgroundStarted { .. } => "agent_background_started",
        LoopEvent::AgentBackgroundCompleted { .. } => "agent_background_completed",
        LoopEvent::UserMessage { .. } => "user_message",
        LoopEvent::ClassifierDecision { .. } => "classifier_decision",
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn discovers_manifest_and_standalone_extensions() {
        let temp = TempDir::new().expect("temp dir");
        let root = temp.path().join("extensions");
        fs::create_dir_all(root.join("manifested")).expect("extension root");
        fs::write(root.join("manifested/index.ts"), "export default () => {}")
            .expect("manifest entry");
        fs::write(
            root.join("manifested/mitsuro-extension.json"),
            r#"{"id":"manifested","name":"Manifested","entry":"index.ts"}"#,
        )
        .expect("manifest");
        fs::write(root.join("standalone.ts"), "export default () => {}").expect("standalone");

        let (found, issues) = discover_root(&AgentExtensionRoot::new(
            &root,
            AgentExtensionScope::Project,
        ))
        .await
        .expect("discovery");
        assert!(issues.is_empty());
        let ids = found
            .into_iter()
            .map(|extension| extension.manifest.id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            ids,
            BTreeSet::from(["manifested".to_string(), "standalone".to_string()])
        );
    }

    #[tokio::test]
    async fn discovers_deprecated_agent_extension_manifest_name() {
        let temp = TempDir::new().expect("temp dir");
        let root = temp.path().join("extensions");
        fs::create_dir_all(root.join("manifested")).expect("extension root");
        fs::write(root.join("manifested/index.ts"), "export default () => {}")
            .expect("manifest entry");
        fs::write(
            root.join("manifested")
                .join(crate::identity::legacy::AGENT_EXTENSION_MANIFEST_FILE_NAME),
            r#"{"id":"deprecated-manifest","name":"Deprecated Manifest","entry":"index.ts"}"#,
        )
        .expect("manifest");

        let (found, issues) = discover_root(&AgentExtensionRoot::new(
            &root,
            AgentExtensionScope::Project,
        ))
        .await
        .expect("discovery");
        assert!(issues.is_empty());
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].manifest.id, "deprecated-manifest");
    }

    #[test]
    fn canonical_agent_extension_manifest_wins_over_deprecated_name() {
        let temp = TempDir::new().expect("temp dir");
        fs::write(
            temp.path()
                .join(crate::identity::AGENT_EXTENSION_MANIFEST_FILE_NAME),
            "{}",
        )
        .expect("canonical manifest");
        fs::write(
            temp.path()
                .join(crate::identity::legacy::AGENT_EXTENSION_MANIFEST_FILE_NAME),
            "{}",
        )
        .expect("deprecated manifest");

        let expected = temp
            .path()
            .join(crate::identity::AGENT_EXTENSION_MANIFEST_FILE_NAME);
        assert_eq!(
            extension_manifest_path(temp.path()).as_deref(),
            Some(expected.as_path())
        );
    }

    #[test]
    fn context_hook_results_are_flattened() {
        let mut output = Vec::new();
        collect_context_strings(
            &serde_json::json!(["one", {"text": "two"}, null]),
            &mut output,
        );
        assert_eq!(output, vec!["one", "two"]);
    }

    #[tokio::test]
    async fn refresh_trust_and_root_mutations_share_one_lifecycle_guard() {
        let temp = TempDir::new().expect("temp dir");
        fs::create_dir_all(temp.path().join("workspace")).expect("workspace");
        let manager = AgentExtensionManager::new_with_paths(
            temp.path().join("workspace"),
            temp.path().join("runtime"),
            temp.path().join("global"),
        );
        let registry = Arc::new(ToolRegistry::new());

        let lifecycle = manager.lifecycle.lock().await;
        let (started_tx, started_rx) = oneshot::channel();
        let mutation_manager = manager.clone();
        let package_root = temp.path().join("package");
        let mut mutation = tokio::spawn(async move {
            let _ = started_tx.send(());
            mutation_manager.set_package_roots(vec![package_root]).await;
        });
        started_rx.await.expect("root mutation should start");
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut mutation)
                .await
                .is_err(),
            "root mutation must wait for the lifecycle guard"
        );
        drop(lifecycle);
        mutation.await.expect("root mutation task should finish");
        assert!(manager
            .roots()
            .await
            .iter()
            .any(|root| root.scope == AgentExtensionScope::Package));

        manager
            .set_project_trusted(true)
            .await
            .expect("seed project trust");
        let lifecycle = manager.lifecycle.lock().await;
        let (started_tx, started_rx) = oneshot::channel();
        let trust_manager = manager.clone();
        let trust_registry = registry.clone();
        let mut revocation = tokio::spawn(async move {
            let _ = started_tx.send(());
            trust_manager
                .set_project_trusted_and_refresh(false, &trust_registry)
                .await
        });
        started_rx.await.expect("trust mutation should start");
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut revocation)
                .await
                .is_err(),
            "trust plus refresh must wait for the lifecycle guard"
        );
        drop(lifecycle);
        let status = revocation
            .await
            .expect("trust mutation task should finish")
            .expect("trust mutation and refresh should succeed");
        assert!(!status.trusted);
        assert!(
            !manager
                .project_trust_status()
                .expect("read project trust")
                .trusted
        );
    }

    #[tokio::test]
    async fn bun_extension_registers_tools_commands_events_and_context() {
        if which::which("bun").is_err() {
            return;
        }

        let temp = TempDir::new().expect("temp dir");
        let working_dir = temp.path().join("workspace");
        let extension_dir = working_dir
            .join(".mitsuro")
            .join("extensions")
            .join("smoke");
        fs::create_dir_all(&extension_dir).expect("extension directory");
        fs::write(
            extension_dir.join("mitsuro-extension.json"),
            r#"{"id":"smoke","name":"Smoke","version":"1.0.0","entry":"index.ts"}"#,
        )
        .expect("manifest");
        fs::write(
            extension_dir.join("index.ts"),
            r#"
export default (mitsuro) => {
  let events = 0;
  mitsuro.registerTool({
    name: "extension_echo",
    description: "Echo a value through the extension host",
    parameters: {
      type: "object",
      properties: { value: { type: "string" } },
      required: ["value"]
    },
    execute: ({ value }, context) => ({ value, session: context.session_id, events })
  });
  mitsuro.registerCommand("hello-extension", {
    description: "Return a command greeting",
    handler: (argument) => `hello ${argument}`
  });
  mitsuro.on("turn_complete", () => { events += 1; });
  mitsuro.on("tool.execute.before", (input, output) => {
    if (input.tool !== "extension_echo") return;
    if (output.args.value === "block") {
      return { block: true, reason: "blocked by smoke extension" };
    }
    output.args.value = `hooked:${output.args.value}`;
  });
  mitsuro.addContext(() => "Context supplied by the smoke extension.");
};
"#,
        )
        .expect("entry");

        let manager = AgentExtensionManager::new_with_paths(
            &working_dir,
            temp.path().join("runtime"),
            temp.path().join("global"),
        );
        let registry = Arc::new(ToolRegistry::new());
        registry.set_agent_extension_manager(manager.clone());
        manager
            .set_project_trusted(true)
            .await
            .expect("explicitly trust test project");
        manager
            .refresh_and_register(&registry)
            .await
            .expect("load extension");

        assert_eq!(manager.loaded_ids().await, vec!["smoke"]);
        let context = ExtensionCallContext::for_turn(
            working_dir.clone(),
            Some(working_dir.clone()),
            Some("session-1".to_string()),
            Some("test-model".to_string()),
            "supervised",
            false,
        );
        assert_eq!(
            manager.context_for_turn(&context).await,
            vec!["Context supplied by the smoke extension."]
        );
        let command = manager
            .execute_command("hello-extension", "world", &context)
            .await
            .expect("command");
        assert_eq!(command, Value::String("hello world".to_string()));

        manager
            .dispatch_event(
                LoopEvent::TurnComplete {
                    turn: 1,
                    has_more: false,
                },
                context,
            )
            .await;
        let result = registry
            .execute(
                "extension_echo",
                serde_json::json!({"value": "works"}),
                &ToolContext {
                    working_dir,
                    session_id: Some("session-1".to_string()),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("registered extension tool");
        let envelope: Value = serde_json::from_str(&result.output).expect("structured result");
        assert_eq!(envelope["data"]["value"], "hooked:works");
        assert_eq!(envelope["data"]["events"], 1);

        let blocked = registry
            .execute(
                "extension_echo",
                serde_json::json!({"value": "block"}),
                &ToolContext {
                    working_dir: temp.path().join("workspace"),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("blocked tool result");
        assert!(blocked.is_error);
        assert!(blocked.output.contains("blocked_by_extension"));

        fs::write(extension_dir.join("index.ts"), "export default (")
            .expect("break extension source");
        manager
            .refresh_and_register(&registry)
            .await
            .expect("failed refresh remains recoverable");
        assert!(!manager.diagnostics().await.is_empty());
        let retained = registry
            .execute(
                "extension_echo",
                serde_json::json!({"value": "still-live"}),
                &ToolContext {
                    working_dir: temp.path().join("workspace"),
                    session_id: Some("session-1".to_string()),
                    ..ToolContext::default()
                },
            )
            .await
            .expect("last-known-good extension tool remains registered");
        assert!(!retained.is_error, "{}", retained.output);

        fs::write(extension_dir.join("mitsuro-extension.json"), "{")
            .expect("break extension manifest");
        manager
            .refresh_and_register(&registry)
            .await
            .expect("manifest validation failure remains recoverable");
        assert_eq!(manager.loaded_ids().await, vec!["smoke"]);
        assert!(registry.get("extension_echo").await.is_some());
    }

    #[tokio::test]
    async fn repository_settings_cannot_self_authorize_project_code() {
        let temp = TempDir::new().expect("temp dir");
        let working_dir = temp.path().join("workspace");
        let extension_dir = working_dir
            .join(".mitsuro")
            .join("extensions")
            .join("untrusted");
        fs::create_dir_all(&extension_dir).expect("extension directory");
        fs::write(
            extension_dir.join("mitsuro-extension.json"),
            r#"{"id":"untrusted","name":"Untrusted","entry":"index.ts"}"#,
        )
        .expect("manifest");
        fs::write(
            extension_dir.join("index.ts"),
            "throw new Error('must not run')",
        )
        .expect("entry");
        fs::write(
            working_dir.join(".mitsuro/settings.json"),
            r#"{"agent_extensions":{"enabled":true,"allow":["*"]}}"#,
        )
        .expect("self-authorizing project settings");

        let manager = AgentExtensionManager::new_with_paths(
            &working_dir,
            temp.path().join("runtime"),
            temp.path().join("global"),
        );
        let registry = Arc::new(ToolRegistry::new());
        registry.set_agent_extension_manager(manager.clone());
        manager.refresh_and_register(&registry).await.unwrap();

        assert!(manager.loaded_ids().await.is_empty());
        assert!(!manager.project_trust_status().unwrap().trusted);
        assert!(manager.diagnostics().await.iter().any(|diagnostic| {
            diagnostic
                .message
                .contains("disabled until this project is trusted")
        }));
    }

    #[test]
    fn tool_intercept_results_chain_argument_patches_and_blocks() {
        let mut params = serde_json::json!({"value": "original"});
        let mut block = None;
        apply_tool_intercept_result(
            &serde_json::json!([
                {"args": {"value": "patched"}},
                {"block": true, "reason": "policy"}
            ]),
            &mut params,
            &mut block,
        );
        assert_eq!(params["value"], "patched");
        assert_eq!(block.as_deref(), Some("policy"));
    }
}
