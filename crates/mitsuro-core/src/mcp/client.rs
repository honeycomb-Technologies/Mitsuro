//! MCP client wrapper around the official rmcp SDK.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http::{HeaderName, HeaderValue};
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientInfo, GetPromptRequestParams,
    GetPromptResult, Implementation, LoggingMessageNotificationParam, PaginatedRequestParams,
    ReadResourceRequestParams, ReadResourceResult,
};
use rmcp::service::{NotificationContext, RoleClient, RunningService};
use rmcp::transport::auth::{AuthClient, AuthorizationManager};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransportConfig, StreamableHttpClientWorker,
};
use rmcp::ClientHandler;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::stdio_transport::BoundedStdioTransport;
use super::transport::ReqwestStreamableHttpClient;

const MAX_MCP_CATALOG_ITEMS: usize = 2_048;
const MAX_MCP_CATALOG_SERIALIZED_BYTES: usize = 8 * 1024 * 1024;
const MAX_MCP_CATALOG_PAGES: usize = 128;
const MAX_MCP_CURSOR_BYTES: usize = 16 * 1024;

#[derive(Debug, Default)]
struct ChangeFlags {
    tools: AtomicBool,
    resources: AtomicBool,
    prompts: AtomicBool,
}

#[derive(Clone)]
struct MitsuroClientHandler {
    info: ClientInfo,
    changes: Arc<ChangeFlags>,
    server_name: String,
}

impl ClientHandler for MitsuroClientHandler {
    async fn on_tool_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.changes.tools.store(true, Ordering::Release);
    }

    async fn on_resource_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.changes.resources.store(true, Ordering::Release);
    }

    async fn on_prompt_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.changes.prompts.store(true, Ordering::Release);
    }

    async fn on_logging_message(
        &self,
        params: LoggingMessageNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        debug!(
            server = self.server_name,
            level = ?params.level,
            logger = ?params.logger,
            data = ?params.data,
            "MCP server log"
        );
    }

    fn get_info(&self) -> ClientInfo {
        self.info.clone()
    }
}

/// MCP client wrapping an rmcp running service.
pub struct McpClient {
    name: String,
    service: RunningService<RoleClient, MitsuroClientHandler>,
    cached_tools: RwLock<Vec<rmcp::model::Tool>>,
    status: RwLock<McpClientStatus>,
    request_timeout: Duration,
    changes: Arc<ChangeFlags>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpClientStatus {
    Connected,
    Error,
}

#[derive(Debug, thiserror::Error)]
pub enum McpClientError {
    #[error("Failed to spawn MCP server: {0}")]
    Spawn(String),
    #[error("Failed to initialize MCP connection: {0}")]
    Init(String),
    #[error("Invalid MCP transport configuration: {0}")]
    Config(String),
    #[error("MCP {operation} timed out after {timeout_ms}ms")]
    Timeout { operation: String, timeout_ms: u64 },
    #[error("MCP request failed: {0}")]
    Request(String),
}

impl McpClient {
    pub async fn connect_local(
        name: &str,
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
        working_dir: &Path,
        startup_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, McpClientError> {
        info!(server = name, command, "Connecting to local MCP server");

        let mut command_builder = tokio::process::Command::new(command);
        configure_local_command_environment(&mut command_builder, env);
        command_builder.args(args).current_dir(working_dir);
        let process = BoundedStdioTransport::spawn(command_builder)
            .map_err(|error| McpClientError::Spawn(error.to_string()))?;

        let changes = Arc::new(ChangeFlags::default());
        let handler = client_handler(name, changes.clone());
        let service = tokio::time::timeout(startup_timeout, rmcp::serve_client(handler, process))
            .await
            .map_err(|_| McpClientError::Timeout {
                operation: "startup".to_string(),
                timeout_ms: duration_ms(startup_timeout),
            })?
            .map_err(|error| McpClientError::Init(error.to_string()))?;

        info!(server = name, "Connected to local MCP server");
        Ok(Self::new(name, service, request_timeout, changes))
    }

    pub async fn connect_remote(
        name: &str,
        url: &str,
        auth_token: Option<&str>,
        headers: &HashMap<String, String>,
        startup_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, McpClientError> {
        info!(server = name, url, "Connecting to remote MCP server");

        let mut config = remote_transport_config(url, headers)?;
        if let Some(token) = auth_token {
            config.auth_header = Some(format!("Bearer {token}"));
        }

        let worker = StreamableHttpClientWorker::new(ReqwestStreamableHttpClient::new(), config);
        let changes = Arc::new(ChangeFlags::default());
        let handler = client_handler(name, changes.clone());
        let service = tokio::time::timeout(startup_timeout, rmcp::serve_client(handler, worker))
            .await
            .map_err(|_| McpClientError::Timeout {
                operation: "startup".to_string(),
                timeout_ms: duration_ms(startup_timeout),
            })?
            .map_err(|error| McpClientError::Init(error.to_string()))?;

        info!(server = name, "Connected to remote MCP server");
        Ok(Self::new(name, service, request_timeout, changes))
    }

    /// Connect through rmcp's authorization-aware transport. The auth client
    /// obtains (and, when needed, refreshes) a bearer token for every HTTP
    /// request rather than freezing one token at connection startup.
    pub async fn connect_remote_oauth(
        name: &str,
        url: &str,
        authorization_manager: AuthorizationManager,
        headers: &HashMap<String, String>,
        startup_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, McpClientError> {
        info!(
            server = name,
            url, "Connecting to OAuth-protected remote MCP server"
        );

        let config = remote_transport_config(url, headers)?;
        let http_client =
            AuthClient::new(ReqwestStreamableHttpClient::new(), authorization_manager);
        let worker = StreamableHttpClientWorker::new(http_client, config);
        let changes = Arc::new(ChangeFlags::default());
        let handler = client_handler(name, changes.clone());
        let service = tokio::time::timeout(startup_timeout, rmcp::serve_client(handler, worker))
            .await
            .map_err(|_| McpClientError::Timeout {
                operation: "startup".to_string(),
                timeout_ms: duration_ms(startup_timeout),
            })?
            .map_err(|error| McpClientError::Init(error.to_string()))?;

        info!(
            server = name,
            "Connected to OAuth-protected remote MCP server"
        );
        Ok(Self::new(name, service, request_timeout, changes))
    }

    fn new(
        name: &str,
        service: RunningService<RoleClient, MitsuroClientHandler>,
        request_timeout: Duration,
        changes: Arc<ChangeFlags>,
    ) -> Self {
        Self {
            name: name.to_string(),
            service,
            cached_tools: RwLock::new(Vec::new()),
            status: RwLock::new(McpClientStatus::Connected),
            request_timeout,
            changes,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn list_tools(&self) -> Result<Vec<rmcp::model::Tool>, McpClientError> {
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut budget = CatalogBudget::new("tool");
        loop {
            budget.begin_page()?;
            let page = self
                .request(
                    "tools/list",
                    self.service
                        .peer()
                        .list_tools(Some(PaginatedRequestParams::default().with_cursor(cursor))),
                )
                .await?;
            budget.add_items(&page.tools)?;
            tools.extend(page.tools);
            cursor = budget.next_cursor(page.next_cursor)?;
            if cursor.is_none() {
                break;
            }
        }
        self.changes.tools.store(false, Ordering::Release);

        info!(
            server = self.name,
            count = tools.len(),
            "Refreshed MCP tools"
        );
        for tool in &tools {
            debug!(
                server = self.name,
                tool = %tool.name,
                description = tool.description.as_deref().unwrap_or("<no description>"),
                "Discovered MCP tool"
            );
        }
        *self.cached_tools.write().await = tools.clone();
        Ok(tools)
    }

    pub async fn refresh_tools_if_changed(&self) -> Result<bool, McpClientError> {
        if !self.changes.tools.swap(false, Ordering::AcqRel) {
            return Ok(false);
        }
        if let Err(error) = self.list_tools().await {
            self.changes.tools.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(true)
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: serde_json::Value,
    ) -> Result<CallToolResult, McpClientError> {
        let mut params = CallToolRequestParams::new(name.to_string());
        if let Some(arguments) = arguments.as_object() {
            params = params.with_arguments(arguments.clone());
        }
        self.request("tools/call", self.service.peer().call_tool(params))
            .await
    }

    pub async fn list_resources(&self) -> Result<Vec<rmcp::model::Resource>, McpClientError> {
        let mut resources = Vec::new();
        let mut cursor = None;
        let mut budget = CatalogBudget::new("resource");
        loop {
            budget.begin_page()?;
            let page = self
                .request(
                    "resources/list",
                    self.service.peer().list_resources(Some(
                        PaginatedRequestParams::default().with_cursor(cursor),
                    )),
                )
                .await?;
            budget.add_items(&page.resources)?;
            resources.extend(page.resources);
            cursor = budget.next_cursor(page.next_cursor)?;
            if cursor.is_none() {
                break;
            }
        }
        self.changes.resources.store(false, Ordering::Release);
        Ok(resources)
    }

    pub async fn list_resource_templates(
        &self,
    ) -> Result<Vec<rmcp::model::ResourceTemplate>, McpClientError> {
        let mut templates = Vec::new();
        let mut cursor = None;
        let mut budget = CatalogBudget::new("resource-template");
        loop {
            budget.begin_page()?;
            let page = self
                .request(
                    "resources/templates/list",
                    self.service.peer().list_resource_templates(Some(
                        PaginatedRequestParams::default().with_cursor(cursor),
                    )),
                )
                .await?;
            budget.add_items(&page.resource_templates)?;
            templates.extend(page.resource_templates);
            cursor = budget.next_cursor(page.next_cursor)?;
            if cursor.is_none() {
                break;
            }
        }
        Ok(templates)
    }

    pub async fn read_resource(&self, uri: &str) -> Result<ReadResourceResult, McpClientError> {
        self.request(
            "resources/read",
            self.service
                .peer()
                .read_resource(ReadResourceRequestParams::new(uri)),
        )
        .await
    }

    pub async fn list_prompts(&self) -> Result<Vec<rmcp::model::Prompt>, McpClientError> {
        let mut prompts = Vec::new();
        let mut cursor = None;
        let mut budget = CatalogBudget::new("prompt");
        loop {
            budget.begin_page()?;
            let page = self
                .request(
                    "prompts/list",
                    self.service
                        .peer()
                        .list_prompts(Some(PaginatedRequestParams::default().with_cursor(cursor))),
                )
                .await?;
            budget.add_items(&page.prompts)?;
            prompts.extend(page.prompts);
            cursor = budget.next_cursor(page.next_cursor)?;
            if cursor.is_none() {
                break;
            }
        }
        self.changes.prompts.store(false, Ordering::Release);
        Ok(prompts)
    }

    pub async fn get_prompt(
        &self,
        name: &str,
        arguments: Option<serde_json::Value>,
    ) -> Result<GetPromptResult, McpClientError> {
        let mut params = GetPromptRequestParams::new(name);
        if let Some(arguments) = arguments.and_then(|value| value.as_object().cloned()) {
            params = params.with_arguments(arguments);
        }
        self.request("prompts/get", self.service.peer().get_prompt(params))
            .await
    }

    pub async fn get_cached_tools(&self) -> Vec<rmcp::model::Tool> {
        self.cached_tools.read().await.clone()
    }

    pub fn server_info(&self) -> Option<rmcp::model::ServerInfo> {
        self.service.peer().peer_info().cloned()
    }

    pub async fn status(&self) -> McpClientStatus {
        *self.status.read().await
    }

    async fn request<T, E, F>(&self, operation: &str, request: F) -> Result<T, McpClientError>
    where
        F: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        match tokio::time::timeout(self.request_timeout, request).await {
            Ok(Ok(value)) => {
                *self.status.write().await = McpClientStatus::Connected;
                Ok(value)
            }
            Ok(Err(error)) => {
                *self.status.write().await = McpClientStatus::Error;
                Err(McpClientError::Request(format!(
                    "{operation} on '{}': {error}",
                    self.name
                )))
            }
            Err(_) => {
                *self.status.write().await = McpClientStatus::Error;
                Err(McpClientError::Timeout {
                    operation: operation.to_string(),
                    timeout_ms: duration_ms(self.request_timeout),
                })
            }
        }
    }
}

fn configure_local_command_environment(
    command: &mut tokio::process::Command,
    configured: &HashMap<String, String>,
) {
    // Package/project stdio servers must not inherit API keys, cloud
    // credentials, agent sockets, or other ambient secrets from Mitsuro.
    // Preserve only the minimum shell-independent process environment, then
    // apply the declaration's already-resolved explicit values.
    command.env_clear();
    for key in ["PATH", "HOME"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    command.envs(configured);
}

struct CatalogBudget {
    label: &'static str,
    item_count: usize,
    serialized_bytes: usize,
    page_count: usize,
    cursors: HashSet<String>,
}

impl CatalogBudget {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            item_count: 0,
            serialized_bytes: 0,
            page_count: 0,
            cursors: HashSet::new(),
        }
    }

    fn begin_page(&mut self) -> Result<(), McpClientError> {
        self.page_count += 1;
        if self.page_count > MAX_MCP_CATALOG_PAGES {
            return Err(self.limit_error(format!(
                "more than {MAX_MCP_CATALOG_PAGES} pagination pages"
            )));
        }
        Ok(())
    }

    fn add_items<T: Serialize>(&mut self, items: &[T]) -> Result<(), McpClientError> {
        self.item_count = self.item_count.checked_add(items.len()).ok_or_else(|| {
            self.limit_error("an overflowing number of catalog items".to_string())
        })?;
        if self.item_count > MAX_MCP_CATALOG_ITEMS {
            return Err(self.limit_error(format!(
                "{} items (limit: {MAX_MCP_CATALOG_ITEMS})",
                self.item_count
            )));
        }

        let remaining = MAX_MCP_CATALOG_SERIALIZED_BYTES
            .checked_sub(self.serialized_bytes)
            .ok_or_else(|| self.limit_error("the serialized byte limit".to_string()))?;
        let mut writer = BoundedCountingWriter::new(remaining);
        if let Err(error) = serde_json::to_writer(&mut writer, items) {
            if writer.overflowed {
                return Err(self.limit_error(format!(
                    "more than {MAX_MCP_CATALOG_SERIALIZED_BYTES} aggregate serialized bytes"
                )));
            }
            return Err(McpClientError::Request(format!(
                "failed to measure MCP {} catalog: {error}",
                self.label
            )));
        }
        self.serialized_bytes = self
            .serialized_bytes
            .checked_add(writer.written)
            .ok_or_else(|| self.limit_error("the serialized byte limit".to_string()))?;
        Ok(())
    }

    fn next_cursor(&mut self, cursor: Option<String>) -> Result<Option<String>, McpClientError> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        if cursor.len() > MAX_MCP_CURSOR_BYTES {
            return Err(self.limit_error(format!(
                "a pagination cursor larger than {MAX_MCP_CURSOR_BYTES} bytes"
            )));
        }
        if !self.cursors.insert(cursor.clone()) {
            return Err(McpClientError::Request(format!(
                "MCP {} catalog repeated a pagination cursor",
                self.label
            )));
        }
        Ok(Some(cursor))
    }

    fn limit_error(&self, detail: String) -> McpClientError {
        McpClientError::Request(format!(
            "MCP {} catalog exceeded its safety bound: {detail}",
            self.label
        ))
    }
}

struct BoundedCountingWriter {
    limit: usize,
    written: usize,
    overflowed: bool,
}

impl BoundedCountingWriter {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            written: 0,
            overflowed: false,
        }
    }
}

impl Write for BoundedCountingWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.len() > self.limit.saturating_sub(self.written) {
            self.overflowed = true;
            return Err(std::io::Error::other("MCP catalog byte limit exceeded"));
        }
        self.written += buffer.len();
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn remote_transport_config(
    url: &str,
    headers: &HashMap<String, String>,
) -> Result<StreamableHttpClientTransportConfig, McpClientError> {
    validate_remote_transport_url(url)?;

    let mut custom_headers = HashMap::new();
    for (name, value) in headers {
        let header_name = HeaderName::from_str(name).map_err(|error| {
            McpClientError::Config(format!("invalid header name '{name}': {error}"))
        })?;
        let header_value = HeaderValue::from_str(value).map_err(|error| {
            McpClientError::Config(format!("invalid value for header '{name}': {error}"))
        })?;
        custom_headers.insert(header_name, header_value);
    }

    let mut config = StreamableHttpClientTransportConfig::with_uri(url)
        .custom_headers(custom_headers)
        .reinit_on_expired_session(true);
    config.allow_stateless = true;
    Ok(config)
}

fn validate_remote_transport_url(value: &str) -> Result<(), McpClientError> {
    let url = reqwest::Url::parse(value)
        .map_err(|error| McpClientError::Config(format!("invalid remote MCP URL: {error}")))?;
    let loopback_http = url.scheme() == "http"
        && match url.host() {
            Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(address)) => address.is_loopback(),
            Some(url::Host::Ipv6(address)) => address.is_loopback(),
            None => false,
        };
    if url.scheme() != "https" && !loopback_http {
        return Err(McpClientError::Config(
            "remote MCP URL must use HTTPS (HTTP is allowed only for localhost/loopback)"
                .to_string(),
        ));
    }
    Ok(())
}

fn client_handler(name: &str, changes: Arc<ChangeFlags>) -> MitsuroClientHandler {
    MitsuroClientHandler {
        info: ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new(format!("mitsuro-mcp-{name}"), env!("CARGO_PKG_VERSION")),
        ),
        changes,
        server_name: name.to_string(),
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::manager::{McpContent, McpToolResult};

    const TEST_SERVER: &str = r#"
import json
import sys
import time

def send(payload):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method")
    request_id = request.get("id")
    if request_id is None:
        continue

    if method == "initialize":
        result = {
            "protocolVersion": request["params"]["protocolVersion"],
            "capabilities": {
                "tools": {"listChanged": True},
                "resources": {"subscribe": False, "listChanged": True},
                "prompts": {"listChanged": True}
            },
            "serverInfo": {"name": "mitsuro-test-mcp", "version": "1.0.0"},
            "instructions": "Use fixture data for tests only."
        }
    elif method == "tools/list":
        result = {"tools": [{
            "name": "fixture",
            "title": "Fixture Tool",
            "description": "Returns every supported content shape",
            "inputSchema": {"type": "object", "properties": {}},
            "outputSchema": {"type": "object", "properties": {"answer": {"type": "integer"}}},
            "annotations": {"readOnlyHint": True}
        }]}
    elif method == "tools/call":
        if request.get("params", {}).get("arguments", {}).get("slow"):
            time.sleep(0.2)
        result = {
            "content": [
                {"type": "text", "text": "fixture text"},
                {"type": "image", "data": "aW1hZ2U=", "mimeType": "image/png"},
                {"type": "audio", "data": "YXVkaW8=", "mimeType": "audio/wav"}
            ],
            "structuredContent": {"answer": 42},
            "isError": False,
            "_meta": {"fixture": True}
        }
    elif method == "resources/list":
        result = {"resources": [{
            "uri": "fixture://readme",
            "name": "readme",
            "description": "Fixture resource",
            "mimeType": "text/plain"
        }]}
    elif method == "resources/templates/list":
        result = {"resourceTemplates": [{
            "uriTemplate": "fixture://items/{id}",
            "name": "item",
            "description": "Fixture resource template"
        }]}
    elif method == "resources/read":
        result = {"contents": [{
            "uri": request["params"]["uri"],
            "mimeType": "text/plain",
            "text": "fixture resource body"
        }]}
    elif method == "prompts/list":
        result = {"prompts": [{
            "name": "fixture-prompt",
            "description": "Fixture prompt",
            "arguments": [{"name": "topic", "required": False}]
        }]}
    elif method == "prompts/get":
        result = {
            "description": "Rendered fixture prompt",
            "messages": [{
                "role": "user",
                "content": {"type": "text", "text": "fixture prompt body"}
            }]
        }
    else:
        send({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": "unknown method"}})
        continue

    send({"jsonrpc": "2.0", "id": request_id, "result": result})
    if method == "tools/call":
        send({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
"#;

    #[test]
    fn remote_transport_requires_tls_except_for_loopback() {
        assert!(remote_transport_config("https://mcp.example/mcp", &HashMap::new()).is_ok());
        assert!(remote_transport_config("http://localhost:3000/mcp", &HashMap::new()).is_ok());
        assert!(remote_transport_config("http://127.0.0.1:3000/mcp", &HashMap::new()).is_ok());
        assert!(remote_transport_config("http://[::1]:3000/mcp", &HashMap::new()).is_ok());
        assert!(matches!(
            remote_transport_config("http://mcp.example/mcp", &HashMap::new()),
            Err(McpClientError::Config(message)) if message.contains("must use HTTPS")
        ));
    }

    #[test]
    fn stdio_child_environment_is_clear_except_for_safe_and_configured_values() {
        let mut command = tokio::process::Command::new("ignored");
        configure_local_command_environment(
            &mut command,
            &HashMap::from([
                ("HOME".to_string(), "/explicit-home".to_string()),
                ("MCP_EXPLICIT".to_string(), "configured".to_string()),
            ]),
        );

        let environment = command
            .as_std()
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(
            environment.get("HOME").and_then(|value| value.as_deref()),
            Some("/explicit-home")
        );
        assert_eq!(
            environment
                .get("MCP_EXPLICIT")
                .and_then(|value| value.as_deref()),
            Some("configured")
        );
        assert!(environment
            .keys()
            .all(|key| matches!(key.as_str(), "PATH" | "HOME" | "MCP_EXPLICIT")));
        assert!(!environment.contains_key("OPENAI_API_KEY"));
        assert!(!environment.contains_key("AWS_SECRET_ACCESS_KEY"));
    }

    #[test]
    fn catalog_budget_fails_closed_on_count_bytes_and_cursor_cycles() {
        let mut count_budget = CatalogBudget::new("tool");
        count_budget.item_count = MAX_MCP_CATALOG_ITEMS;
        assert!(count_budget
            .add_items(&[serde_json::json!({"name": "overflow"})])
            .unwrap_err()
            .to_string()
            .contains("items"));

        let mut byte_budget = CatalogBudget::new("resource");
        byte_budget.serialized_bytes = MAX_MCP_CATALOG_SERIALIZED_BYTES - 1;
        assert!(byte_budget
            .add_items(&[serde_json::json!("too large")])
            .unwrap_err()
            .to_string()
            .contains("serialized bytes"));

        let mut cursor_budget = CatalogBudget::new("prompt");
        assert_eq!(
            cursor_budget.next_cursor(Some("same".to_string())).unwrap(),
            Some("same".to_string())
        );
        assert!(cursor_budget
            .next_cursor(Some("same".to_string()))
            .unwrap_err()
            .to_string()
            .contains("repeated"));
    }

    #[tokio::test]
    async fn plaintext_remote_with_bearer_or_custom_headers_is_rejected_before_network() {
        let mut headers = HashMap::new();
        headers.insert("X-Api-Key".to_string(), "custom-secret".to_string());
        assert!(matches!(
            McpClient::connect_remote(
                "plaintext-bearer",
                "http://mcp.example/mcp",
                Some("bearer-secret"),
                &HashMap::new(),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .await,
            Err(McpClientError::Config(message)) if message.contains("must use HTTPS")
        ));
        assert!(matches!(
            remote_transport_config("http://mcp.example/mcp", &headers),
            Err(McpClientError::Config(message)) if message.contains("must use HTTPS")
        ));
    }

    #[tokio::test]
    async fn child_server_exercises_tools_resources_templates_prompts_and_notifications() {
        let Ok(python) = which::which("python3") else {
            eprintln!("python3 unavailable; skipping MCP child integration test");
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("mcp_test_server.py");
        tokio::fs::write(&script, TEST_SERVER).await.unwrap();
        let args = vec!["-u".to_string(), script.display().to_string()];

        let client = McpClient::connect_local(
            "fixture",
            python.to_str().unwrap(),
            &args,
            &HashMap::new(),
            directory.path(),
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        let server_info = client.server_info().unwrap();
        assert_eq!(
            server_info.instructions.as_deref(),
            Some("Use fixture data for tests only.")
        );

        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "fixture");
        assert!(tools[0].output_schema.is_some());
        assert_eq!(
            tools[0]
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.read_only_hint),
            Some(true)
        );

        let raw_result = client
            .call_tool("fixture", serde_json::json!({}))
            .await
            .unwrap();
        let result = McpToolResult::from(raw_result);
        assert_eq!(
            result.structured_content,
            Some(serde_json::json!({"answer": 42}))
        );
        assert!(matches!(
            result.content.as_slice(),
            [
                McpContent::Text { .. },
                McpContent::Image { .. },
                McpContent::Audio { .. }
            ]
        ));

        let resources = client.list_resources().await.unwrap();
        assert_eq!(resources[0].raw.uri, "fixture://readme");
        let templates = client.list_resource_templates().await.unwrap();
        assert_eq!(templates[0].raw.uri_template, "fixture://items/{id}");
        let resource = client.read_resource("fixture://readme").await.unwrap();
        assert_eq!(resource.contents.len(), 1);
        let prompts = client.list_prompts().await.unwrap();
        assert_eq!(prompts[0].name, "fixture-prompt");
        let prompt = client
            .get_prompt("fixture-prompt", Some(serde_json::json!({"topic": "MCP"})))
            .await
            .unwrap();
        assert_eq!(prompt.messages.len(), 1);

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(client.refresh_tools_if_changed().await.unwrap());
    }

    #[tokio::test]
    async fn request_timeout_is_enforced_for_child_server() {
        let Ok(python) = which::which("python3") else {
            eprintln!("python3 unavailable; skipping MCP child timeout test");
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let script = directory.path().join("mcp_slow_test_server.py");
        tokio::fs::write(&script, TEST_SERVER).await.unwrap();
        let args = vec!["-u".to_string(), script.display().to_string()];
        let client = McpClient::connect_local(
            "slow-fixture",
            python.to_str().unwrap(),
            &args,
            &HashMap::new(),
            directory.path(),
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await
        .unwrap();

        let error = client
            .call_tool("fixture", serde_json::json!({"slow": true}))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            McpClientError::Timeout { operation, .. } if operation == "tools/call"
        ));
    }
}
