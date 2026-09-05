use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

use super::types::{McpServerInfo, McpServerStatus, McpToolDef, McpToolResult};
use crate::mcp::client::{McpClient, McpClientStatus};
use crate::mcp::config::{
    McpConfig, McpConfigSource, McpConnectionAuthority, McpOAuthConfig, McpPackageConfig,
    McpServerConfig, McpToolApproval, RemoteMcpServer,
};
use crate::mcp::oauth::{McpOAuthCoordinator, McpOAuthStart, McpOAuthStatus};

/// Owns MCP configuration, connections, recovery, and capability discovery.
pub struct McpManager {
    clients: RwLock<HashMap<String, Arc<McpClient>>>,
    configs: RwLock<HashMap<String, McpServerConfig>>,
    remote_servers: RwLock<Vec<RemoteMcpServer>>,
    last_errors: RwLock<HashMap<String, String>>,
    connection_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    package_configs: RwLock<Vec<McpPackageConfig>>,
    explicit_authorities: RwLock<HashMap<String, McpConnectionAuthority>>,
    /// Connections hold a read guard for their entire startup/discovery path;
    /// config reload/revocation holds the write guard. This guarantees that a
    /// client created from an old snapshot cannot be inserted after reload.
    lifecycle_gate: RwLock<()>,
    config_generation: AtomicU64,
    oauth: McpOAuthCoordinator,
    working_dir: PathBuf,
    global_config_path: PathBuf,
}

impl McpManager {
    pub fn new(working_dir: PathBuf) -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
            remote_servers: RwLock::new(Vec::new()),
            last_errors: RwLock::new(HashMap::new()),
            connection_locks: RwLock::new(HashMap::new()),
            package_configs: RwLock::new(Vec::new()),
            explicit_authorities: RwLock::new(HashMap::new()),
            lifecycle_gate: RwLock::new(()),
            config_generation: AtomicU64::new(0),
            oauth: McpOAuthCoordinator::new(),
            working_dir,
            global_config_path: crate::paths::config_dir().join("mcp.json"),
        }
    }

    #[cfg(test)]
    fn new_with_global_config(working_dir: PathBuf, global_config_path: PathBuf) -> Self {
        let mut manager = Self::new(working_dir);
        manager.global_config_path = global_config_path;
        manager
    }

    pub async fn load_config(&self) -> Result<()> {
        let _lifecycle = self.lifecycle_gate.write().await;
        self.load_config_locked().await
    }

    async fn load_config_locked(&self) -> Result<()> {
        let package_configs = self.package_configs.read().await.clone();
        let parsed = match McpConfig::load_with_package_configs(
            &self.working_dir,
            &self.global_config_path,
            &package_configs,
        )
        .await
        {
            Ok(parsed) => parsed,
            Err(error) => {
                // Configuration is an authorization boundary. Retaining old
                // clients after a revoke/uninstall plus a malformed remaining
                // fragment would keep removed remote tools live, so parsing
                // failures deliberately invalidate the entire active snapshot.
                self.config_generation.fetch_add(1, Ordering::AcqRel);
                self.clear_active_config().await;
                return Err(error);
            }
        };
        let new_configs = parsed.servers().await;
        let remote_servers = parsed.remote_servers_for_api().await;
        let old_configs = self.configs.read().await.clone();

        let stale_connections: Vec<String> = {
            let clients = self.clients.read().await;
            clients
                .keys()
                .filter(|name| {
                    let next = new_configs.get(*name);
                    next.is_none()
                        || next.is_some_and(|config| !config.is_enabled())
                        || old_configs.get(*name) != next
                })
                .cloned()
                .collect()
        };

        for name in stale_connections {
            self.disconnect(&name).await;
        }

        let configured_names: HashSet<_> = new_configs.keys().cloned().collect();
        let oauth_resources: HashMap<_, _> = new_configs
            .iter()
            .filter_map(|(name, config)| match config {
                McpServerConfig::Remote {
                    url,
                    oauth: Some(oauth),
                    ..
                } if oauth.enabled => Some((name.clone(), url.clone())),
                _ => None,
            })
            .collect();
        if let Err(error) = self.oauth.retain_servers(&oauth_resources).await {
            // Credential cleanup is part of applying the authorization
            // snapshot. Fail closed instead of leaving removed servers usable
            // through the previous in-memory configuration.
            self.config_generation.fetch_add(1, Ordering::AcqRel);
            self.clear_active_config().await;
            return Err(error).context("failed to reconcile MCP OAuth credentials");
        }
        self.explicit_authorities.write().await.retain(|name, _| {
            old_configs.get(name) == new_configs.get(name)
                && new_configs
                    .get(name)
                    .is_some_and(|config| config.source() == McpConfigSource::Project)
        });
        self.config_generation.fetch_add(1, Ordering::AcqRel);
        *self.configs.write().await = new_configs;
        *self.remote_servers.write().await = remote_servers;
        self.last_errors
            .write()
            .await
            .retain(|name, _| configured_names.contains(name));

        let configs = self.configs.read().await;
        let local_count = configs.values().filter(|config| config.is_local()).count();
        let remote_count = configs.values().filter(|config| config.is_remote()).count();
        let disabled_count = configs
            .values()
            .filter(|config| !config.is_enabled())
            .count();
        info!(
            local_count,
            remote_count, disabled_count, "Loaded MCP config"
        );
        Ok(())
    }

    async fn clear_active_config(&self) {
        let connected = self
            .clients
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in connected {
            self.disconnect(&name).await;
        }
        self.configs.write().await.clear();
        self.remote_servers.write().await.clear();
        self.last_errors.write().await.clear();
        self.explicit_authorities.write().await.clear();
        self.oauth.clear_pending_flows().await;
    }

    pub async fn connect_all(&self) -> Result<()> {
        let (connectable, unavailable_required): (Vec<_>, Vec<_>) = {
            let configs = self.configs.read().await;
            let clients = self.clients.read().await;
            let connectable = configs
                .iter()
                .filter(|(_, config)| config.should_auto_connect())
                .map(|(name, config)| (name.clone(), config.is_required()))
                .collect();
            let unavailable_required = configs
                .iter()
                .filter(|(name, config)| {
                    config.is_required()
                        && !config.should_auto_connect()
                        && !clients.contains_key(*name)
                })
                .map(|(name, config)| {
                    if config.is_enabled() {
                        format!("{name}: explicit trust/connect is required")
                    } else {
                        format!("{name}: required server is disabled")
                    }
                })
                .collect();
            (connectable, unavailable_required)
        };

        let futures = connectable.into_iter().map(|(name, required)| async move {
            let result = self.connect(&name).await;
            (name, required, result)
        });
        let results = futures::future::join_all(futures).await;

        let mut required_failures = unavailable_required;
        for (name, required, result) in results {
            if let Err(error) = result {
                warn!(server = name, %error, "Failed to connect MCP server");
                if required {
                    required_failures.push(format!("{name}: {error}"));
                }
            }
        }

        if required_failures.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Required MCP servers are unavailable: {}",
                required_failures.join("; ")
            ))
        }
    }

    /// Register a package fragment with the exact current descriptor grant.
    pub async fn register_package_config(&self, config: McpPackageConfig) {
        let mut configs = self.package_configs.write().await;
        if !configs.contains(&config) {
            configs.push(config);
        }
    }

    /// Replace package fragments and their exact process/network grants.
    pub async fn set_package_configs(&self, configs: Vec<McpPackageConfig>) {
        *self.package_configs.write().await = configs;
    }

    /// Return secret-free OAuth state for a configured server.
    pub async fn oauth_status(&self, name: &str) -> Result<McpOAuthStatus> {
        let Some((url, oauth)) = self.oauth_server_config(name).await? else {
            return Ok(McpOAuthStatus::disabled(name));
        };
        if !oauth.enabled {
            return Ok(McpOAuthStatus::disabled(name));
        }
        self.oauth.status(name, &url, &oauth).await
    }

    /// Start an OAuth 2.1 browser flow with PKCE and CSRF protection.
    pub async fn start_oauth(&self, name: &str, redirect_uri: &str) -> Result<McpOAuthStart> {
        let _lifecycle = self.lifecycle_gate.read().await;
        let (url, oauth) = self
            .oauth_server_config(name)
            .await?
            .with_context(|| format!("MCP server '{name}' does not have OAuth configured"))?;
        if !oauth.enabled {
            anyhow::bail!("OAuth is disabled for MCP server '{name}'");
        }
        self.authorize_explicit_transport(name).await?;
        self.oauth.start(name, &url, &oauth, redirect_uri).await
    }

    /// Validate an OAuth callback, persist its tokens, and establish the MCP
    /// connection. Token exchange success remains durable even if MCP
    /// initialization subsequently reports a separate connection error.
    pub async fn complete_oauth(
        &self,
        name: &str,
        code: &str,
        state: &str,
    ) -> Result<McpOAuthStatus> {
        let (_, oauth) = self
            .oauth_server_config(name)
            .await?
            .with_context(|| format!("MCP server '{name}' does not have OAuth configured"))?;
        if !oauth.enabled {
            anyhow::bail!("OAuth is disabled for MCP server '{name}'");
        }
        self.oauth.complete(name, code, state).await?;
        self.disconnect(name).await;
        self.connect(name).await.with_context(|| {
            format!(
                "OAuth succeeded for MCP server '{name}', but the MCP connection could not be initialized"
            )
        })?;
        self.oauth_status(name).await
    }

    /// Finish a provider error callback only after matching its CSRF state to
    /// the pending browser authorization.
    pub async fn cancel_oauth_callback(&self, name: &str, state: &str) -> Result<()> {
        self.oauth.cancel_with_state(name, state).await
    }

    /// Remove saved OAuth credentials and disconnect the server.
    pub async fn logout_oauth(&self, name: &str) -> Result<McpOAuthStatus> {
        let (url, oauth) = self
            .oauth_server_config(name)
            .await?
            .with_context(|| format!("MCP server '{name}' does not have OAuth configured"))?;
        self.oauth.logout(name, &url).await?;
        self.disconnect(name).await;
        if oauth.enabled {
            self.oauth_status(name).await
        } else {
            Ok(McpOAuthStatus::disabled(name))
        }
    }

    pub async fn connect(&self, name: &str) -> Result<()> {
        self.connect_with_intent(name, false).await
    }

    /// Connect after an explicit user action. Project declarations receive
    /// authority for exactly their configured transport; package declarations
    /// still require the matching descriptor grant and cannot be elevated by
    /// a generic connect button.
    pub async fn connect_explicit(&self, name: &str) -> Result<()> {
        self.connect_with_intent(name, true).await
    }

    async fn connect_with_intent(&self, name: &str, explicit: bool) -> Result<()> {
        let _lifecycle = self.lifecycle_gate.read().await;
        let connection_lock = self.connection_lock(name).await;
        let _guard = connection_lock.lock().await;
        if explicit {
            self.authorize_explicit_transport(name).await?;
        }
        self.connect_locked(name).await
    }

    async fn connect_locked(&self, name: &str) -> Result<()> {
        let generation = self.config_generation.load(Ordering::Acquire);
        let config = self
            .configs
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown server: {name}"))?;
        if !config.is_enabled() {
            anyhow::bail!("MCP server '{name}' is disabled");
        }
        self.ensure_connection_authorized(name, &config).await?;

        self.clients.write().await.remove(name);
        let startup_timeout = Duration::from_millis(config.startup_timeout_ms());
        let request_timeout = Duration::from_millis(config.tool_timeout_ms());
        let client_result = match &config {
            McpServerConfig::Local {
                command,
                args,
                env,
                cwd,
                ..
            } => {
                let cwd = resolve_cwd(&self.working_dir, cwd.as_deref());
                McpClient::connect_local(
                    name,
                    command,
                    args,
                    env,
                    &cwd,
                    startup_timeout,
                    request_timeout,
                )
                .await
            }
            McpServerConfig::Remote {
                url,
                authorization_token,
                oauth,
                headers,
                ..
            } => {
                if authorization_token.is_some()
                    || oauth.as_ref().is_none_or(|oauth| !oauth.enabled)
                {
                    McpClient::connect_remote(
                        name,
                        url,
                        authorization_token.as_deref(),
                        headers,
                        startup_timeout,
                        request_timeout,
                    )
                    .await
                } else {
                    let authorization_manager =
                        match self.oauth.authorization_manager(name, url).await {
                            Ok(manager) => manager,
                            Err(error) => {
                                self.record_error(name, error.to_string()).await;
                                return Err(error);
                            }
                        };
                    McpClient::connect_remote_oauth(
                        name,
                        url,
                        authorization_manager,
                        headers,
                        startup_timeout,
                        request_timeout,
                    )
                    .await
                }
            }
        };

        let client = match client_result {
            Ok(client) => client,
            Err(error) => {
                self.record_error(name, error.to_string()).await;
                return Err(anyhow::anyhow!(error));
            }
        };

        if client
            .server_info()
            .is_some_and(|info| info.capabilities.tools.is_some())
        {
            if let Err(error) = client.list_tools().await {
                error!(server = name, %error, "Failed initial MCP tool discovery");
                self.record_error(name, error.to_string()).await;
                return Err(anyhow::anyhow!(error));
            }
        }

        if !self
            .connection_snapshot_is_current(name, &config, generation)
            .await
        {
            anyhow::bail!(
                "MCP configuration changed while connecting '{name}'; stale connection discarded"
            );
        }
        self.clients
            .write()
            .await
            .insert(name.to_string(), Arc::new(client));
        self.last_errors.write().await.remove(name);
        info!(server = name, "Connected to MCP server");
        Ok(())
    }

    pub async fn disconnect(&self, name: &str) {
        let connection_lock = self.connection_lock(name).await;
        let _guard = connection_lock.lock().await;
        if self.clients.write().await.remove(name).is_some() {
            info!(server = name, "Disconnected MCP server");
        }
    }

    /// Refresh tool caches invalidated by MCP `tools/list_changed`
    /// notifications. Returns true when at least one catalog changed.
    pub async fn refresh_changed_tools(&self) -> bool {
        let clients: Vec<_> = self.clients.read().await.values().cloned().collect();
        let mut changed = false;
        for client in clients {
            match client.refresh_tools_if_changed().await {
                Ok(was_changed) => changed |= was_changed,
                Err(error) => {
                    self.record_error(client.name(), error.to_string()).await;
                    warn!(server = client.name(), %error, "Failed dynamic MCP tool refresh");
                }
            }
        }
        changed
    }

    /// Force a fresh tool catalog for one connected server.
    pub async fn refresh_tools(&self, server: &str) -> Result<()> {
        self.connected_client(server)
            .await?
            .list_tools()
            .await
            .map(|_| ())
            .map_err(anyhow::Error::from)
    }

    /// Return a freshly discovered, configuration-filtered tool catalog for
    /// one server. This powers the generic dispatcher when a server changes
    /// its catalog before a surface has re-registered named wrappers.
    pub async fn list_tools(&self, server: &str) -> Result<Vec<McpToolDef>> {
        let config = self.config(server).await?;
        let client = self.connected_client(server).await?;
        let instructions = client.server_info().and_then(|info| info.instructions);
        let mut tools: Vec<_> = client
            .list_tools()
            .await
            .map_err(anyhow::Error::from)?
            .into_iter()
            .filter(|tool| config.allows_tool(tool.name.as_ref()))
            .map(|tool| {
                let approval = config.tool_approval(tool.name.as_ref());
                let mut definition = McpToolDef::from_tool(tool, approval);
                definition.server_instructions = instructions.clone();
                definition
            })
            .collect();
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(tools)
    }

    pub async fn get_all_tools(&self) -> Vec<(String, McpToolDef)> {
        self.refresh_changed_tools().await;
        let clients: Vec<_> = self
            .clients
            .read()
            .await
            .iter()
            .map(|(name, client)| (name.clone(), client.clone()))
            .collect();
        let configs = self.configs.read().await.clone();
        let mut tools = Vec::new();

        for (server_name, client) in clients {
            let Some(config) = configs.get(&server_name) else {
                continue;
            };
            let server_instructions = client.server_info().and_then(|info| info.instructions);
            for tool in client.get_cached_tools().await {
                if config.allows_tool(tool.name.as_ref()) {
                    let approval = config.tool_approval(tool.name.as_ref());
                    let mut definition = McpToolDef::from_tool(tool, approval);
                    definition.server_instructions = server_instructions.clone();
                    tools.push((server_name.clone(), definition));
                }
            }
        }
        tools.sort_by(|(left_server, left), (right_server, right)| {
            left_server
                .cmp(right_server)
                .then_with(|| left.name.cmp(&right.name))
        });
        tools
    }

    pub async fn call_tool(
        &self,
        server: &str,
        tool: &str,
        arguments: serde_json::Value,
    ) -> Result<McpToolResult> {
        let config = self.config(server).await?;
        if !config.allows_tool(tool) {
            anyhow::bail!("MCP tool '{server}/{tool}' is denied by configuration");
        }
        let client = self.connected_client(server).await?;
        match client.call_tool(tool, arguments).await {
            Ok(result) => Ok(McpToolResult::from(result)),
            Err(error) => {
                self.record_error(server, error.to_string()).await;
                let recovery = self.connect(server).await;
                let suffix = if recovery.is_ok() {
                    " The connection was restored, but the tool call was not replayed because its side effects are unknown."
                } else {
                    " Automatic reconnect also failed."
                };
                Err(anyhow::anyhow!("{error}.{suffix}"))
            }
        }
    }

    pub async fn tool_approval(&self, server: &str, tool: &str) -> Result<McpToolApproval> {
        let config = self.config(server).await?;
        if !config.allows_tool(tool) {
            anyhow::bail!("MCP tool '{server}/{tool}' is denied by configuration");
        }
        Ok(config.tool_approval(tool))
    }

    pub async fn list_resources(&self, server: &str) -> Result<Vec<rmcp::model::Resource>> {
        let client = self.connected_client(server).await?;
        match client.list_resources().await {
            Ok(value) => Ok(value),
            Err(first_error) => {
                self.reconnect_for_read(server, &first_error.to_string())
                    .await?;
                self.connected_client(server)
                    .await?
                    .list_resources()
                    .await
                    .map_err(|error| anyhow::anyhow!(error))
            }
        }
    }

    pub async fn list_resource_templates(
        &self,
        server: &str,
    ) -> Result<Vec<rmcp::model::ResourceTemplate>> {
        let client = self.connected_client(server).await?;
        match client.list_resource_templates().await {
            Ok(value) => Ok(value),
            Err(first_error) => {
                self.reconnect_for_read(server, &first_error.to_string())
                    .await?;
                self.connected_client(server)
                    .await?
                    .list_resource_templates()
                    .await
                    .map_err(|error| anyhow::anyhow!(error))
            }
        }
    }

    pub async fn read_resource(
        &self,
        server: &str,
        uri: &str,
    ) -> Result<rmcp::model::ReadResourceResult> {
        let client = self.connected_client(server).await?;
        match client.read_resource(uri).await {
            Ok(value) => Ok(value),
            Err(first_error) => {
                self.reconnect_for_read(server, &first_error.to_string())
                    .await?;
                self.connected_client(server)
                    .await?
                    .read_resource(uri)
                    .await
                    .map_err(|error| anyhow::anyhow!(error))
            }
        }
    }

    pub async fn list_prompts(&self, server: &str) -> Result<Vec<rmcp::model::Prompt>> {
        let client = self.connected_client(server).await?;
        match client.list_prompts().await {
            Ok(value) => Ok(value),
            Err(first_error) => {
                self.reconnect_for_read(server, &first_error.to_string())
                    .await?;
                self.connected_client(server)
                    .await?
                    .list_prompts()
                    .await
                    .map_err(|error| anyhow::anyhow!(error))
            }
        }
    }

    pub async fn get_prompt(
        &self,
        server: &str,
        name: &str,
        arguments: Option<serde_json::Value>,
    ) -> Result<rmcp::model::GetPromptResult> {
        let client = self.connected_client(server).await?;
        match client.get_prompt(name, arguments.clone()).await {
            Ok(value) => Ok(value),
            Err(first_error) => {
                self.reconnect_for_read(server, &first_error.to_string())
                    .await?;
                self.connected_client(server)
                    .await?
                    .get_prompt(name, arguments)
                    .await
                    .map_err(|error| anyhow::anyhow!(error))
            }
        }
    }

    pub async fn list_servers(&self) -> Vec<McpServerInfo> {
        self.refresh_changed_tools().await;
        let configs = self.configs.read().await.clone();
        let clients = self.clients.read().await.clone();
        let errors = self.last_errors.read().await.clone();
        let mut servers = Vec::new();

        for (name, config) in configs {
            let client = clients.get(&name);
            let (status, cached_tools, server_info) = if !config.is_enabled() {
                (McpServerStatus::Disconnected, Vec::new(), None)
            } else if let Some(client) = client {
                let status = match client.status().await {
                    McpClientStatus::Connected => McpServerStatus::Connected,
                    McpClientStatus::Error => McpServerStatus::Error(
                        errors
                            .get(&name)
                            .cloned()
                            .unwrap_or_else(|| "connection error".to_string()),
                    ),
                };
                (
                    status,
                    client.get_cached_tools().await,
                    client.server_info(),
                )
            } else if let Some(error) = errors.get(&name) {
                (McpServerStatus::Error(error.clone()), Vec::new(), None)
            } else {
                (McpServerStatus::Disconnected, Vec::new(), None)
            };

            let tools: Vec<_> = cached_tools
                .into_iter()
                .filter(|tool| config.allows_tool(tool.name.as_ref()))
                .map(|tool| {
                    let approval = config.tool_approval(tool.name.as_ref());
                    let mut definition = McpToolDef::from_tool(tool, approval);
                    definition.server_instructions = server_info
                        .as_ref()
                        .and_then(|server_info| server_info.instructions.clone());
                    definition
                })
                .collect();
            let instructions = server_info
                .as_ref()
                .and_then(|server_info| server_info.instructions.clone());
            let serialized_server_info = server_info
                .as_ref()
                .and_then(|server_info| serde_json::to_value(server_info).ok());

            servers.push(McpServerInfo {
                name: name.clone(),
                server_type: config.transport_type().to_string(),
                source: config.source(),
                enabled: config.is_enabled(),
                required: config.is_required(),
                status,
                tool_count: tools.len(),
                tools,
                instructions,
                server_info: serialized_server_info,
                error: errors.get(&name).cloned(),
            });
        }
        servers.sort_by(|left, right| left.name.cmp(&right.name));
        servers
    }

    pub async fn get_remote_servers(&self) -> Vec<RemoteMcpServer> {
        self.remote_servers.read().await.clone()
    }

    pub async fn has_servers(&self) -> bool {
        !self.configs.read().await.is_empty()
    }

    async fn config(&self, server: &str) -> Result<McpServerConfig> {
        let config = self
            .configs
            .read()
            .await
            .get(server)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown server: {server}"))?;
        if !config.is_enabled() {
            anyhow::bail!("MCP server '{server}' is disabled");
        }
        Ok(config)
    }

    async fn authorize_explicit_transport(&self, server: &str) -> Result<()> {
        let config = self.config(server).await?;
        match config.source() {
            McpConfigSource::Project => {
                self.explicit_authorities
                    .write()
                    .await
                    .insert(server.to_string(), config.required_authority());
                Ok(())
            }
            McpConfigSource::Package | McpConfigSource::Global => {
                self.ensure_connection_authorized(server, &config).await
            }
        }
    }

    async fn ensure_connection_authorized(
        &self,
        server: &str,
        config: &McpServerConfig,
    ) -> Result<()> {
        let declared = config.declared_authority();
        let explicitly_granted = if config.source() == McpConfigSource::Project {
            self.explicit_authorities
                .read()
                .await
                .get(server)
                .copied()
                .unwrap_or(McpConnectionAuthority::NONE)
        } else {
            McpConnectionAuthority::NONE
        };
        if config.is_authorized_by(declared) || config.is_authorized_by(explicitly_granted) {
            return Ok(());
        }

        let required = if config.is_local() {
            "process authority for stdio child execution"
        } else {
            "network authority for remote HTTP"
        };
        anyhow::bail!(
            "MCP server '{server}' from {:?} configuration requires explicit {required}",
            config.source()
        )
    }

    async fn connection_snapshot_is_current(
        &self,
        server: &str,
        config: &McpServerConfig,
        generation: u64,
    ) -> bool {
        self.config_generation.load(Ordering::Acquire) == generation
            && self.configs.read().await.get(server) == Some(config)
    }

    async fn oauth_server_config(&self, server: &str) -> Result<Option<(String, McpOAuthConfig)>> {
        let config = self
            .configs
            .read()
            .await
            .get(server)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unknown server: {server}"))?;
        Ok(match config {
            McpServerConfig::Remote {
                url,
                oauth: Some(oauth),
                ..
            } => Some((url, oauth)),
            _ => None,
        })
    }

    async fn connected_client(&self, server: &str) -> Result<Arc<McpClient>> {
        self.config(server).await?;
        self.clients
            .read()
            .await
            .get(server)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Server not connected: {server}"))
    }

    async fn reconnect_for_read(&self, server: &str, first_error: &str) -> Result<()> {
        self.record_error(server, first_error.to_string()).await;
        self.connect(server)
            .await
            .with_context(|| format!("MCP read request failed ({first_error}); reconnect failed"))
    }

    async fn record_error(&self, server: &str, error: String) {
        self.last_errors
            .write()
            .await
            .insert(server.to_string(), error);
    }

    async fn connection_lock(&self, server: &str) -> Arc<Mutex<()>> {
        if let Some(lock) = self.connection_locks.read().await.get(server).cloned() {
            return lock;
        }
        self.connection_locks
            .write()
            .await
            .entry(server.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

fn resolve_cwd(workspace: &Path, configured: Option<&Path>) -> PathBuf {
    match configured {
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => workspace.join(path),
        None => workspace.to_path_buf(),
    }
}

#[cfg(test)]
mod fail_closed_tests {
    use super::McpManager;
    use crate::mcp::{McpConnectionAuthority, McpPackageConfig};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn malformed_reload_invalidates_previous_configuration_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let project_config = temp.path().join(".mcp.json");
        tokio::fs::write(
            &project_config,
            r#"{"mcpServers":{"safe":{"command":"echo","args":["ok"]}}}"#,
        )
        .await
        .unwrap();
        let manager = McpManager::new_with_global_config(
            temp.path().to_path_buf(),
            temp.path().join("missing-global.json"),
        );
        manager.load_config().await.unwrap();
        assert!(manager.has_servers().await);

        tokio::fs::write(&project_config, "{").await.unwrap();
        assert!(manager.load_config().await.is_err());
        assert!(!manager.has_servers().await);
        assert!(manager.get_remote_servers().await.is_empty());
    }

    #[tokio::test]
    async fn project_servers_require_an_explicit_transport_specific_action() {
        let temp = tempfile::tempdir().unwrap();
        tokio::fs::write(
            temp.path().join(".mcp.json"),
            r#"{"mcpServers":{"local":{"command":"echo"},"remote":{"type":"url","url":"https://mcp.example/mcp"}}}"#,
        )
        .await
        .unwrap();
        let manager = McpManager::new_with_global_config(
            temp.path().to_path_buf(),
            temp.path().join("missing-global.json"),
        );
        manager.load_config().await.unwrap();
        let configs = manager.configs.read().await.clone();

        assert!(manager
            .ensure_connection_authorized("local", &configs["local"])
            .await
            .is_err());
        assert!(manager
            .ensure_connection_authorized("remote", &configs["remote"])
            .await
            .is_err());

        manager.authorize_explicit_transport("local").await.unwrap();
        assert!(manager
            .ensure_connection_authorized("local", &configs["local"])
            .await
            .is_ok());
        assert!(manager
            .ensure_connection_authorized("remote", &configs["remote"])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn package_network_grant_never_authorizes_stdio() {
        let temp = tempfile::tempdir().unwrap();
        let package = temp.path().join("package-mcp.json");
        tokio::fs::write(
            &package,
            r#"{"mcpServers":{"local":{"command":"echo"},"remote":{"type":"url","url":"https://mcp.example/mcp"}}}"#,
        )
        .await
        .unwrap();
        let manager = McpManager::new_with_global_config(
            temp.path().to_path_buf(),
            temp.path().join("missing-global.json"),
        );
        manager
            .set_package_configs(vec![McpPackageConfig::new(
                package,
                McpConnectionAuthority::new(false, true),
            )])
            .await;
        manager.load_config().await.unwrap();
        let configs = manager.configs.read().await.clone();

        assert!(manager
            .ensure_connection_authorized("local", &configs["local"])
            .await
            .is_err());
        assert!(manager
            .ensure_connection_authorized("remote", &configs["remote"])
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn generation_guard_rejects_a_stale_connection_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        tokio::fs::write(
            temp.path().join(".mcp.json"),
            r#"{"mcpServers":{"local":{"command":"echo"}}}"#,
        )
        .await
        .unwrap();
        let manager = McpManager::new_with_global_config(
            temp.path().to_path_buf(),
            temp.path().join("missing-global.json"),
        );
        manager.load_config().await.unwrap();
        let config = manager.configs.read().await["local"].clone();
        let generation = manager.config_generation.load(Ordering::Acquire);
        assert!(
            manager
                .connection_snapshot_is_current("local", &config, generation)
                .await
        );

        manager.config_generation.fetch_add(1, Ordering::AcqRel);
        assert!(
            !manager
                .connection_snapshot_is_current("local", &config, generation)
                .await
        );
    }
}
