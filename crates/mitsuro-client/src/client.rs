use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context as _, Result};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE, IF_MATCH};
use reqwest::{Client, Response, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

use crate::agent_sse::{agent_stream_from_response, AgentEventStream};
use crate::canvas_sse::{canvas_stream_from_response, CanvasEventStream};
use crate::sse::{chat_stream_from_response, ChatEventStream};
use crate::{
    AgentControlState, AgentInputResponseRequest, BackgroundProcess, BrowseResponse, BrowserAction,
    BrowserActionBatchRequest, BrowserActionResponse, BrowserHeartbeatRequest,
    BrowserPresenceCapability, BrowserSession, BrowserSessionListResponse,
    CanvasCollaborationResponse, CanvasGeometryPreviewRequest, CanvasPresenceHeartbeatRequest,
    ChatRequest, CleanupCanvasResourceResponse, CreateBrowserSessionRequest, CreateCanvasRequest,
    CreateCanvasResourceRequest, CreateCanvasResourceResponse, CreateCanvasResponse,
    CreateSessionRequest, DetachCanvasResourceRequest, DetachCanvasResourceResponse,
    ExtensionOverview, FileResponse, FileTreeResponse, GitChangesResponse, GitFileDiffResponse,
    HealthResponse, HeartbeatAgentControlRequest, HiveCrewRequest, HiveCurrentResponse,
    HiveDispatchRequest, HiveDispatchResponse, HiveMessageRequest, HivePriorityRequest,
    HiveRunPriority, HiveScheduleMutationResponse, HiveScheduleSummary, HiveScheduleWriteRequest,
    HiveSessionStatus, ListCanvasResourceIntentsResponse, ListCanvasesResponse,
    ListProjectsResponse, McpServer, ModelsResponse, OAuthExchangeRequest, OAuthExchangeResponse,
    OAuthStartRequest, OAuthStartResponse, OAuthStatusResponse, PatchCanvasRequest,
    PatchCanvasResponse, ProviderStatus, ServerAccessResponse, ServerStatusResponse, SessionInfo,
    SessionStateOptions, SessionStateResponse, SessionType, SessionWithMessages,
    SetCredentialRequest, SimpleOkResponse, SkillInfo, SpatialCanvas, SpatialCanvasSurface,
    SteerRequest, SteerResponse, StopBrowserSessionRequest, TakeAgentControlRequest,
    TakeAgentControlResponse, TakeBrowserControlRequest, TakeBrowserControlResponse,
    TakeTerminalControlRequest, TakeTerminalControlResponse, TerminalDescriptor,
    TerminalListResponse, TerminalSpec, ToolApprovalRequest, UpdateServerAccessRequest,
    UpdateSessionRequest,
};

use crate::{AGENT_CONTROL_LEASE_HEADER, CANVAS_PRESENCE_TOKEN_HEADER};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const STREAM_TIMEOUT: Duration = Duration::from_secs(300);
const OBSERVER_STREAM_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_COMPATIBILITY_AGENT_LEASES: usize = 512;

#[derive(Clone)]
pub struct MitsuroClient {
    base_url: Arc<str>,
    http: Client,
    authorization: Option<HeaderValue>,
    compatibility_agent_control: Arc<tokio::sync::Mutex<CompatibilityAgentControl>>,
}

struct CompatibilityAgentControl {
    client_id: String,
    leases: HashMap<String, String>,
}

impl std::fmt::Debug for MitsuroClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MitsuroClient")
            .field("base_url", &self.base_url)
            .field("authenticated", &self.authorization.is_some())
            .finish_non_exhaustive()
    }
}

impl MitsuroClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Self::build(base_url.into(), None)
    }

    /// Construct a client for an authenticated remote Mitsuro server.
    ///
    /// The token is installed as a default header and is never exposed by this
    /// type's public API. Local loopback servers normally use [`Self::local`].
    pub fn with_bearer_token(base_url: impl Into<String>, token: impl AsRef<str>) -> Result<Self> {
        Self::build(base_url.into(), Some(token.as_ref()))
    }

    fn build(base_url: String, bearer_token: Option<&str>) -> Result<Self> {
        let base_url = normalize_base_url(base_url);
        let mut headers = HeaderMap::new();
        let authorization = if let Some(token) = bearer_token {
            let token = token.trim();
            if token.is_empty() {
                return Err(anyhow!("Mitsuro bearer token cannot be empty"));
            }
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("validating Mitsuro bearer token")?;
            headers.insert(AUTHORIZATION, value.clone());
            Some(value)
        } else {
            None
        };
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .default_headers(headers)
            .build()
            .context("building Mitsuro HTTP client")?;
        Ok(Self {
            base_url: Arc::from(base_url),
            http,
            authorization,
            compatibility_agent_control: Arc::new(tokio::sync::Mutex::new(
                CompatibilityAgentControl {
                    client_id: compatibility_agent_client_id(),
                    leases: HashMap::new(),
                },
            )),
        })
    }

    pub fn local() -> Result<Self> {
        Self::new("http://127.0.0.1:3000")
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Build a WebSocket client handshake carrying this client's private
    /// authentication header. The target must be the WebSocket equivalent of
    /// this Mitsuro server's exact origin so a bearer token can never be sent
    /// to an arbitrary host.
    pub fn websocket_request(&self, url: &str) -> Result<http::Request<()>> {
        let websocket_url = reqwest::Url::parse(url).context("parsing Mitsuro WebSocket URL")?;
        let base_url = reqwest::Url::parse(self.base_url()).context("parsing Mitsuro base URL")?;
        let expected_scheme = match base_url.scheme() {
            "http" => "ws",
            "https" => "wss",
            scheme => return Err(anyhow!("unsupported Mitsuro server URL scheme: {scheme}")),
        };
        let same_origin = websocket_url.scheme() == expected_scheme
            && websocket_url.host_str() == base_url.host_str()
            && websocket_url.port_or_known_default() == base_url.port_or_known_default();
        if !same_origin {
            return Err(anyhow!(
                "Mitsuro WebSocket URL must use the configured server origin"
            ));
        }

        let mut request = url
            .into_client_request()
            .context("building Mitsuro WebSocket handshake")?;
        if let Some(authorization) = self.authorization.as_ref() {
            request
                .headers_mut()
                .insert(AUTHORIZATION, authorization.clone());
        }
        Ok(request)
    }

    /// Build the authenticated native Browser handshake. Account and control
    /// credentials stay in headers; the URL remains safe for diagnostics.
    pub fn browser_websocket_request(
        &self,
        url: &str,
        lease_token: Option<&str>,
    ) -> Result<http::Request<()>> {
        let mut request = self.websocket_request(url)?;
        if let Some(lease_token) = lease_token {
            let lease_token = required_opaque_browser_lease(lease_token)?;
            request.headers_mut().insert(
                crate::BROWSER_CONTROL_LEASE_HEADER,
                HeaderValue::from_str(lease_token)
                    .context("validating Browser control lease header")?,
            );
        }
        Ok(request)
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        self.get_json_at(&self.url("/health")).await
    }

    pub async fn list_models(&self) -> Result<ModelsResponse> {
        self.get_json("/models").await
    }

    pub async fn list_credentials(&self) -> Result<Vec<ProviderStatus>> {
        self.get_json("/credentials").await
    }

    pub async fn credential_provider(&self, provider_id: &str) -> Result<ProviderStatus> {
        self.get_json(&format!("/credentials/{provider_id}")).await
    }

    pub async fn set_credential(
        &self,
        provider_id: &str,
        api_key: impl Into<String>,
    ) -> Result<ProviderStatus> {
        let request = SetCredentialRequest {
            api_key: api_key.into(),
        };
        self.post_json(&format!("/credentials/{provider_id}"), &request)
            .await
    }

    pub async fn delete_credential(&self, provider_id: &str) -> Result<ProviderStatus> {
        self.delete_json(&format!("/credentials/{provider_id}"))
            .await
    }

    pub async fn start_oauth(&self, request: OAuthStartRequest) -> Result<OAuthStartResponse> {
        self.post_json("/auth/oauth/start", &request).await
    }

    pub async fn oauth_status(&self, provider_id: &str) -> Result<OAuthStatusResponse> {
        self.get_json(&format!("/auth/oauth/status/{provider_id}"))
            .await
    }

    pub async fn exchange_oauth_code(
        &self,
        request: OAuthExchangeRequest,
    ) -> Result<OAuthExchangeResponse> {
        self.post_json("/auth/oauth/exchange", &request).await
    }

    pub async fn revoke_oauth(&self, provider_id: &str) -> Result<OAuthStatusResponse> {
        self.delete_json(&format!("/auth/oauth/revoke/{provider_id}"))
            .await
    }

    pub async fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        self.get_json("/sessions").await
    }

    pub async fn create_session(&self, request: CreateSessionRequest) -> Result<SessionInfo> {
        self.post_json("/sessions", &request).await
    }

    pub async fn get_session(&self, session_id: &str) -> Result<SessionWithMessages> {
        self.get_json(&format!("/sessions/{session_id}")).await
    }

    pub async fn get_agent_control(&self, session_id: &str) -> Result<AgentControlState> {
        self.get_json(&format!("/sessions/{session_id}/control"))
            .await
    }

    pub async fn take_agent_control(
        &self,
        session_id: &str,
        request: &TakeAgentControlRequest,
    ) -> Result<TakeAgentControlResponse> {
        self.post_json(&format!("/sessions/{session_id}/control"), request)
            .await
    }

    pub async fn heartbeat_agent_control(
        &self,
        session_id: &str,
        request: &HeartbeatAgentControlRequest,
    ) -> Result<AgentControlState> {
        self.put_json(&format!("/sessions/{session_id}/control"), request)
            .await
    }

    pub async fn get_session_state(&self, session_id: &str) -> Result<SessionStateResponse> {
        self.get_session_state_with_options(session_id, SessionStateOptions::default())
            .await
    }

    pub async fn get_session_state_with_options(
        &self,
        session_id: &str,
        options: SessionStateOptions,
    ) -> Result<SessionStateResponse> {
        let mut query = Vec::new();
        if options.include_delegated_history {
            query.push("include_delegated_history=true".to_string());
        }
        if let Some(cursor) = options.delegation_after_cursor {
            query.push(format!("delegation_after_cursor={}", cursor.max(0)));
        }
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!("?{}", query.join("&"))
        };
        self.get_json(&format!("/sessions/{session_id}/state{suffix}"))
            .await
    }

    /// Attach to the canonical server-owned live/replay stream for one Code
    /// Agent. The cursor is inclusive of everything the caller has applied.
    pub async fn observe_agent_stream(
        &self,
        session_id: &str,
        after_generation: u64,
        after_sequence: u64,
    ) -> Result<AgentEventStream> {
        let response = self
            .http
            .get(self.api_url(&format!(
                "/sessions/{session_id}/stream?after_generation={after_generation}&after_sequence={after_sequence}"
            )))
            .timeout(OBSERVER_STREAM_TIMEOUT)
            .header(ACCEPT, "text/event-stream")
            .send()
            .await
            .context("attaching Code Agent observer stream")?;
        Ok(agent_stream_from_response(ensure_success(response).await?))
    }

    pub async fn update_session(
        &self,
        session_id: &str,
        request: UpdateSessionRequest,
    ) -> Result<SessionInfo> {
        self.patch_json(&format!("/sessions/{session_id}"), &request)
            .await
    }

    pub async fn delete_session(&self, session_id: &str) -> Result<SimpleOkResponse> {
        let url = self.api_url(&format!("/sessions/{session_id}"));
        let response = self
            .http
            .delete(&url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        ensure_success(response).await?;
        Ok(SimpleOkResponse { ok: true })
    }

    pub async fn cancel_session(&self, session_id: &str) -> Result<SimpleOkResponse> {
        let lease = self.compatibility_agent_lease(session_id).await?;
        self.cancel_session_with_lease(session_id, lease.as_deref())
            .await
    }

    pub async fn cancel_session_with_lease(
        &self,
        session_id: &str,
        lease_token: Option<&str>,
    ) -> Result<SimpleOkResponse> {
        let url = self.api_url(&format!("/sessions/{session_id}/cancel"));
        let request = with_agent_control_lease(
            self.http
                .post(&url)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json")
                .json(&serde_json::json!({})),
            lease_token,
        )?;
        let response = request
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        decode_json_response(response, &url).await
    }

    pub async fn read_file(&self, path: &str) -> Result<FileResponse> {
        self.get_json_with_query("/files", &[("path", path)]).await
    }

    pub async fn file_tree(&self, root: &str, depth: usize) -> Result<FileTreeResponse> {
        let depth = depth.min(10).to_string();
        self.get_json_with_query("/files/tree", &[("root", root), ("depth", depth.as_str())])
            .await
    }

    /// Read a project-relative file through the exact-owner Canvas boundary.
    ///
    /// The server derives the project root from the persisted Canvas; callers
    /// cannot supply or override that root.
    pub async fn canvas_read_file(&self, canvas_id: &str, path: &str) -> Result<FileResponse> {
        let mut url = self.api_url_with_segments("/canvases", &[canvas_id, "files"])?;
        url.query_pairs_mut().append_pair("path", path);
        self.get_json_at(url.as_str()).await
    }

    /// Load the bounded project tree owned by an exact Canvas.
    pub async fn canvas_file_tree(
        &self,
        canvas_id: &str,
        depth: usize,
    ) -> Result<FileTreeResponse> {
        let mut url = self.api_url_with_segments("/canvases", &[canvas_id, "files", "tree"])?;
        let depth = depth.min(10).to_string();
        url.query_pairs_mut().append_pair("depth", depth.as_str());
        self.get_json_at(url.as_str()).await
    }

    pub async fn git_changes(&self, path: &str) -> Result<GitChangesResponse> {
        self.get_json_with_query("/git/changes", &[("path", path)])
            .await
    }

    pub async fn git_file_diff(&self, path: &str, file: &str) -> Result<GitFileDiffResponse> {
        self.get_json_with_query("/git/diff", &[("path", path), ("file", file)])
            .await
    }

    pub async fn browse_directories(&self, path: Option<&str>) -> Result<BrowseResponse> {
        match path {
            Some(path) => {
                self.get_json_with_query("/files/browse", &[("path", path)])
                    .await
            }
            None => self.get_json("/files/browse").await,
        }
    }

    pub async fn list_browser_sessions(&self) -> Result<BrowserSessionListResponse> {
        self.get_json("/browser").await
    }

    pub async fn create_browser_session(
        &self,
        request: &CreateBrowserSessionRequest,
    ) -> Result<BrowserSession> {
        self.post_json("/browser", request).await
    }

    pub async fn get_browser_session(&self, session_id: &str) -> Result<BrowserSession> {
        self.get_json(&format!("/browser/{session_id}")).await
    }

    pub async fn stop_browser_session(
        &self,
        session_id: &str,
        lease_token: &str,
    ) -> Result<BrowserSession> {
        let lease_token = required_opaque_browser_lease(lease_token)?;
        self.post_json(
            &format!("/browser/{session_id}/stop"),
            &StopBrowserSessionRequest {
                lease_token: lease_token.to_owned(),
            },
        )
        .await
    }

    /// Explicitly transfer control of a Browser session to one native client.
    /// The returned token is kept in memory and must accompany every mutation.
    pub async fn take_browser_control(
        &self,
        session_id: &str,
        request: &TakeBrowserControlRequest,
    ) -> Result<TakeBrowserControlResponse> {
        self.post_json(&format!("/browser/{session_id}/control"), request)
            .await
    }

    pub async fn heartbeat_browser_session(
        &self,
        session_id: &str,
        capability: BrowserPresenceCapability,
        client_id: impl Into<String>,
        lease_token: Option<&str>,
    ) -> Result<BrowserSession> {
        let lease_token = browser_control_token(capability, lease_token)?;
        self.post_json(
            &format!("/browser/{session_id}/heartbeat"),
            &BrowserHeartbeatRequest {
                capability,
                client_id: Some(client_id.into()),
                lease_token: lease_token.map(ToOwned::to_owned),
            },
        )
        .await
    }

    pub async fn run_browser_actions(
        &self,
        session_id: &str,
        lease_token: &str,
        actions: &[BrowserAction],
    ) -> Result<BrowserActionResponse> {
        let lease_token = required_opaque_browser_lease(lease_token)?;
        self.post_json(
            &format!("/browser/{session_id}/actions"),
            &BrowserActionBatchRequest {
                lease_token: lease_token.to_owned(),
                actions: actions.to_vec(),
            },
        )
        .await
    }

    pub fn browser_stream_url(
        &self,
        stream_path: &str,
        capability: BrowserPresenceCapability,
        client_id: &str,
        lease_token: Option<&str>,
    ) -> Result<String> {
        if !stream_path.starts_with("/api/browser/")
            || !stream_path.ends_with("/stream")
            || stream_path.contains(['?', '#'])
        {
            return Err(anyhow!("invalid Mitsuro Browser stream path"));
        }
        let client_id = client_id.trim();
        if client_id.is_empty() {
            return Err(anyhow!("Mitsuro Browser client id cannot be empty"));
        }
        let _lease_token = browser_control_token(capability, lease_token)?;
        let mut url = reqwest::Url::parse(&format!("{}{}", self.base_url(), stream_path))
            .context("building Mitsuro Browser stream URL")?;
        let websocket_scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            scheme => return Err(anyhow!("unsupported Mitsuro server URL scheme: {scheme}")),
        };
        url.set_scheme(websocket_scheme)
            .map_err(|_| anyhow!("could not set Mitsuro Browser WebSocket scheme"))?;
        url.query_pairs_mut()
            .append_pair(
                "capability",
                match capability {
                    BrowserPresenceCapability::Viewer => "viewer",
                    BrowserPresenceCapability::Controller => "controller",
                },
            )
            .append_pair("client_id", client_id);
        Ok(url.to_string())
    }

    pub async fn list_terminals(&self) -> Result<TerminalListResponse> {
        self.get_json("/terminals").await
    }

    pub async fn create_terminal(&self, request: &TerminalSpec) -> Result<TerminalDescriptor> {
        self.post_json("/terminals", request).await
    }

    pub async fn get_terminal(&self, terminal_id: &str) -> Result<TerminalDescriptor> {
        self.get_json(&format!("/terminals/{terminal_id}")).await
    }

    pub async fn take_terminal_control(
        &self,
        terminal_id: &str,
        request: &TakeTerminalControlRequest,
    ) -> Result<TakeTerminalControlResponse> {
        self.post_json(&format!("/terminals/{terminal_id}/control"), request)
            .await
    }

    pub async fn delete_terminal(
        &self,
        terminal_id: &str,
        lease_token: &str,
    ) -> Result<TerminalDescriptor> {
        let (url, request) = self.terminal_delete_request(terminal_id, lease_token)?;
        let response = self
            .http
            .execute(request)
            .await
            .with_context(|| format!("DELETE {url}"))?;
        decode_json_response(response, &url).await
    }

    fn terminal_delete_request(
        &self,
        terminal_id: &str,
        lease_token: &str,
    ) -> Result<(String, reqwest::Request)> {
        let lease_token = required_opaque_terminal_lease(lease_token)?;
        let url = self.api_url(&format!("/terminals/{terminal_id}"));
        let request = self
            .http
            .delete(&url)
            .header(crate::TERMINAL_CONTROL_LEASE_HEADER, lease_token)
            .header(ACCEPT, "application/json")
            .build()
            .with_context(|| format!("building DELETE {url}"))?;
        Ok((url, request))
    }

    /// Construct the authenticated terminal v2 WebSocket endpoint. Local
    /// clients inherit loopback authentication; remote callers must supply the
    /// same bearer credential used to construct this client during the WS
    /// handshake.
    pub fn terminal_stream_url(
        &self,
        terminal_id: &str,
        after_offset: Option<u64>,
    ) -> Result<String> {
        let mut url =
            reqwest::Url::parse(&self.api_url(&format!("/terminals/{terminal_id}/stream")))
                .context("building Mitsuro terminal stream URL")?;
        let websocket_scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            scheme => return Err(anyhow!("unsupported Mitsuro server URL scheme: {scheme}")),
        };
        url.set_scheme(websocket_scheme)
            .map_err(|_| anyhow!("could not set Mitsuro terminal WebSocket scheme"))?;
        if let Some(after_offset) = after_offset {
            url.query_pairs_mut()
                .append_pair("after_offset", &after_offset.to_string());
        }
        Ok(url.to_string())
    }

    pub async fn list_projects(
        &self,
        surface: SpatialCanvasSurface,
    ) -> Result<ListProjectsResponse> {
        self.get_json_with_query("/projects", &[("surface", surface.as_str())])
            .await
    }

    pub async fn list_canvases(
        &self,
        surface: Option<SpatialCanvasSurface>,
    ) -> Result<ListCanvasesResponse> {
        match surface {
            Some(surface) => {
                self.get_json_with_query("/canvases", &[("surface", surface.as_str())])
                    .await
            }
            None => self.get_json("/canvases").await,
        }
    }

    pub async fn create_canvas(
        &self,
        request: &CreateCanvasRequest,
    ) -> Result<CreateCanvasResponse> {
        self.post_json("/canvases", request).await
    }

    pub async fn get_canvas(&self, canvas_id: &str) -> Result<SpatialCanvas> {
        self.get_json(&format!("/canvases/{canvas_id}")).await
    }

    pub async fn patch_canvas(
        &self,
        canvas_id: &str,
        request: &PatchCanvasRequest,
    ) -> Result<PatchCanvasResponse> {
        self.patch_json(&format!("/canvases/{canvas_id}"), request)
            .await
    }

    pub async fn create_canvas_resource(
        &self,
        canvas_id: &str,
        request: &CreateCanvasResourceRequest,
    ) -> Result<CreateCanvasResourceResponse> {
        self.post_json(&format!("/canvases/{canvas_id}/resources"), request)
            .await
    }

    pub async fn list_canvas_resource_intents(
        &self,
        canvas_id: &str,
    ) -> Result<ListCanvasResourceIntentsResponse> {
        self.get_json(&format!("/canvases/{canvas_id}/resources"))
            .await
    }

    pub async fn detach_canvas_resource(
        &self,
        canvas_id: &str,
        create_op_id: &str,
        request: &DetachCanvasResourceRequest,
    ) -> Result<DetachCanvasResourceResponse> {
        self.post_json(
            &format!("/canvases/{canvas_id}/resources/{create_op_id}/detach"),
            request,
        )
        .await
    }

    pub async fn cleanup_canvas_resource(
        &self,
        canvas_id: &str,
        create_op_id: &str,
    ) -> Result<CleanupCanvasResourceResponse> {
        let url = self.api_url(&format!("/canvases/{canvas_id}/resources/{create_op_id}"));
        let response = self
            .http
            .delete(url.clone())
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        decode_json_response(response, url.as_str()).await
    }

    pub async fn canvas_stream(
        &self,
        canvas_id: &str,
        after_sequence: Option<u64>,
    ) -> Result<CanvasEventStream> {
        let url = self.api_url(&format!("/canvases/{canvas_id}/stream"));
        let mut request = self
            .http
            .get(&url)
            .timeout(STREAM_TIMEOUT)
            .header(ACCEPT, "text/event-stream");
        if let Some(after_sequence) = after_sequence {
            request = request.query(&[("after_sequence", after_sequence)]);
        }
        let response = request.send().await.with_context(|| format!("GET {url}"))?;
        let response = ensure_success(response).await?;
        Ok(canvas_stream_from_response(response))
    }

    /// Publish this client's bounded canvas presence heartbeat and optional
    /// world-space cursor. The server keeps this state ephemeral.
    pub async fn heartbeat_canvas_presence(
        &self,
        canvas_id: &str,
        request: &CanvasPresenceHeartbeatRequest,
    ) -> Result<CanvasCollaborationResponse> {
        let url = self.api_url_with_segments("/canvases", &[canvas_id, "presence"])?;
        let response = self
            .http
            .put(url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(request)
            .send()
            .await
            .with_context(|| format!("PUT {url}"))?;
        decode_json_response(response, url.as_str()).await
    }

    /// Explicitly leave a canvas, immediately releasing this client's
    /// cursors, previews, and advisory geometry locks.
    pub async fn leave_canvas_presence(
        &self,
        canvas_id: &str,
        client_id: &str,
        presence_token: &str,
    ) -> Result<CanvasCollaborationResponse> {
        let url = self.api_url_with_segments("/canvases", &[canvas_id, "presence", client_id])?;
        let response = self
            .http
            .delete(url.clone())
            .header(CANVAS_PRESENCE_TOKEN_HEADER, presence_token)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        decode_json_response(response, url.as_str()).await
    }

    /// Acquire or refresh an advisory node lock while publishing a transient
    /// geometry preview. Final geometry still uses `patch_canvas`.
    pub async fn update_canvas_geometry_preview(
        &self,
        canvas_id: &str,
        node_id: &str,
        request: &CanvasGeometryPreviewRequest,
    ) -> Result<CanvasCollaborationResponse> {
        let url =
            self.api_url_with_segments("/canvases", &[canvas_id, "geometry-previews", node_id])?;
        let response = self
            .http
            .put(url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(request)
            .send()
            .await
            .with_context(|| format!("PUT {url}"))?;
        decode_json_response(response, url.as_str()).await
    }

    /// Release an advisory node lock. `committed` describes whether a final
    /// durable geometry patch was enqueued by the caller.
    pub async fn release_canvas_geometry_preview(
        &self,
        canvas_id: &str,
        node_id: &str,
        client_id: &str,
        presence_token: &str,
        committed: bool,
    ) -> Result<CanvasCollaborationResponse> {
        let mut url =
            self.api_url_with_segments("/canvases", &[canvas_id, "geometry-previews", node_id])?;
        url.query_pairs_mut()
            .append_pair("client_id", client_id)
            .append_pair("committed", if committed { "true" } else { "false" });
        let response = self
            .http
            .delete(url.clone())
            .header(CANVAS_PRESENCE_TOKEN_HEADER, presence_token)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        decode_json_response(response, url.as_str()).await
    }

    pub async fn list_skills(&self) -> Result<Vec<SkillInfo>> {
        self.get_json("/skills").await
    }

    pub async fn list_extensions(&self) -> Result<ExtensionOverview> {
        self.get_json("/extensions").await
    }

    pub async fn list_mcp_servers(&self) -> Result<Vec<McpServer>> {
        self.get_json("/mcp").await
    }

    pub async fn list_processes(&self) -> Result<Vec<BackgroundProcess>> {
        self.get_json("/processes").await
    }

    pub async fn kill_process(&self, process_id: &str) -> Result<()> {
        let mut url = reqwest::Url::parse(&self.api_url("/processes"))
            .context("building Mitsuro process kill URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("Mitsuro process endpoint cannot be a base URL"))?
            .push(process_id)
            .push("kill");
        let url = url.to_string();
        let response = self
            .http
            .post(&url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        ensure_success(response).await?;
        Ok(())
    }

    pub async fn hive_current(&self) -> Result<HiveCurrentResponse> {
        self.get_json("/hive/current").await
    }

    pub async fn dispatch_hive(
        &self,
        request: &HiveDispatchRequest,
        idempotency_key: Option<&str>,
    ) -> Result<HiveDispatchResponse> {
        let url = self.api_url("/hive/dispatch");
        let mut builder = self
            .http
            .post(&url)
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .json(request);
        if let Some(key) = idempotency_key.filter(|key| !key.trim().is_empty()) {
            builder = builder.header("Idempotency-Key", key);
        }
        let response = builder
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        decode_json_response(response, &url).await
    }

    pub async fn hive_session_status(&self, session_id: &str) -> Result<HiveSessionStatus> {
        let url = self.hive_session_url(session_id, &["status"])?;
        self.get_json_at(&url).await
    }

    pub async fn send_hive_message(
        &self,
        session_id: &str,
        message: impl Into<String>,
        idempotency_key: Option<&str>,
    ) -> Result<SimpleOkResponse> {
        self.post_hive_session_json(
            session_id,
            "message",
            &HiveMessageRequest {
                message: message.into(),
            },
            idempotency_key,
        )
        .await
    }

    pub async fn pause_hive_session(
        &self,
        session_id: &str,
        idempotency_key: Option<&str>,
    ) -> Result<SimpleOkResponse> {
        self.post_hive_session_json(session_id, "pause", &serde_json::json!({}), idempotency_key)
            .await
    }

    pub async fn resume_hive_session(
        &self,
        session_id: &str,
        idempotency_key: Option<&str>,
    ) -> Result<SimpleOkResponse> {
        self.post_hive_session_json(
            session_id,
            "resume",
            &serde_json::json!({}),
            idempotency_key,
        )
        .await
    }

    pub async fn set_hive_priority(
        &self,
        session_id: &str,
        priority: HiveRunPriority,
        idempotency_key: Option<&str>,
    ) -> Result<SimpleOkResponse> {
        self.post_hive_session_json(
            session_id,
            "priority",
            &HivePriorityRequest { priority },
            idempotency_key,
        )
        .await
    }

    pub async fn set_hive_crew(
        &self,
        session_id: &str,
        crew_slug: Option<String>,
        idempotency_key: Option<&str>,
    ) -> Result<SimpleOkResponse> {
        self.post_hive_session_json(
            session_id,
            "crew",
            &HiveCrewRequest { crew_slug },
            idempotency_key,
        )
        .await
    }

    pub async fn cancel_hive_session(
        &self,
        session_id: &str,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        let url = self.hive_session_url(session_id, &[])?;
        let mut builder = self.http.delete(&url).header(ACCEPT, "application/json");
        if let Some(key) = idempotency_key.filter(|key| !key.trim().is_empty()) {
            builder = builder.header("Idempotency-Key", key);
        }
        let response = builder
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        ensure_success(response).await?;
        Ok(())
    }

    pub async fn list_hive_schedules(&self) -> Result<Vec<HiveScheduleSummary>> {
        self.get_json("/hive/schedules").await
    }

    pub async fn create_hive_schedule(
        &self,
        session_id: &str,
        definition: &HiveScheduleWriteRequest,
        idempotency_key: Option<&str>,
    ) -> Result<HiveScheduleMutationResponse> {
        self.write_hive_schedule(
            reqwest::Method::POST,
            session_id,
            None,
            None,
            definition,
            idempotency_key,
        )
        .await
    }

    pub async fn replace_hive_schedule(
        &self,
        session_id: &str,
        schedule_id: &str,
        revision: u64,
        definition: &HiveScheduleWriteRequest,
        idempotency_key: Option<&str>,
    ) -> Result<HiveScheduleMutationResponse> {
        self.write_hive_schedule(
            reqwest::Method::PUT,
            session_id,
            Some(schedule_id),
            Some(revision),
            definition,
            idempotency_key,
        )
        .await
    }

    pub async fn pause_hive_schedule(
        &self,
        session_id: &str,
        schedule_id: &str,
        revision: u64,
        idempotency_key: Option<&str>,
    ) -> Result<HiveScheduleMutationResponse> {
        self.mutate_hive_schedule_status(
            reqwest::Method::POST,
            session_id,
            schedule_id,
            Some("pause"),
            revision,
            idempotency_key,
        )
        .await
    }

    pub async fn resume_hive_schedule(
        &self,
        session_id: &str,
        schedule_id: &str,
        revision: u64,
        idempotency_key: Option<&str>,
    ) -> Result<HiveScheduleMutationResponse> {
        self.mutate_hive_schedule_status(
            reqwest::Method::POST,
            session_id,
            schedule_id,
            Some("resume"),
            revision,
            idempotency_key,
        )
        .await
    }

    pub async fn cancel_hive_schedule(
        &self,
        session_id: &str,
        schedule_id: &str,
        revision: u64,
        idempotency_key: Option<&str>,
    ) -> Result<HiveScheduleMutationResponse> {
        self.mutate_hive_schedule_status(
            reqwest::Method::DELETE,
            session_id,
            schedule_id,
            None,
            revision,
            idempotency_key,
        )
        .await
    }

    async fn mutate_hive_schedule_status(
        &self,
        method: reqwest::Method,
        session_id: &str,
        schedule_id: &str,
        action: Option<&str>,
        revision: u64,
        idempotency_key: Option<&str>,
    ) -> Result<HiveScheduleMutationResponse> {
        let mut url = reqwest::Url::parse(&self.api_url("/hive/sessions"))
            .context("building Mitsuro Hive schedule mutation URL")?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow!("Mitsuro Hive endpoint cannot be a base URL"))?;
            segments
                .push(session_id)
                .push("schedules")
                .push(schedule_id);
            if let Some(action) = action {
                segments.push(action);
            }
        }
        let url = url.to_string();
        let mut request = self
            .http
            .request(method, &url)
            .header(ACCEPT, "application/json")
            .header(IF_MATCH, format!("\"{revision}\""));
        if let Some(key) = idempotency_key.filter(|key| !key.trim().is_empty()) {
            request = request.header("Idempotency-Key", key);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("mutating Hive schedule at {url}"))?;
        decode_json_response(response, &url).await
    }

    async fn write_hive_schedule(
        &self,
        method: reqwest::Method,
        session_id: &str,
        schedule_id: Option<&str>,
        revision: Option<u64>,
        definition: &HiveScheduleWriteRequest,
        idempotency_key: Option<&str>,
    ) -> Result<HiveScheduleMutationResponse> {
        let mut url = reqwest::Url::parse(&self.api_url("/hive/sessions"))
            .context("building Mitsuro Hive schedule write URL")?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow!("Mitsuro Hive endpoint cannot be a base URL"))?;
            segments.push(session_id).push("schedules");
            if let Some(schedule_id) = schedule_id {
                segments.push(schedule_id);
            }
        }
        let url = url.to_string();
        let mut request = self
            .http
            .request(method, &url)
            .header(ACCEPT, "application/json")
            .json(definition);
        if let Some(revision) = revision {
            request = request.header(IF_MATCH, format!("\"{revision}\""));
        }
        if let Some(key) = idempotency_key.filter(|key| !key.trim().is_empty()) {
            request = request.header("Idempotency-Key", key);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("writing Hive schedule at {url}"))?;
        decode_json_response(response, &url).await
    }

    fn hive_session_url(&self, session_id: &str, suffix: &[&str]) -> Result<String> {
        let mut url = reqwest::Url::parse(&self.api_url("/hive/sessions"))
            .context("building Mitsuro Hive session URL")?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow!("Mitsuro Hive endpoint cannot be a base URL"))?;
            segments.push(session_id);
            for segment in suffix {
                segments.push(segment);
            }
        }
        Ok(url.to_string())
    }

    async fn post_hive_session_json<T, B>(
        &self,
        session_id: &str,
        action: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let url = self.hive_session_url(session_id, &[action])?;
        let mut builder = self
            .http
            .post(&url)
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .json(body);
        if let Some(key) = idempotency_key.filter(|key| !key.trim().is_empty()) {
            builder = builder.header("Idempotency-Key", key);
        }
        let response = builder
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        decode_json_response(response, &url).await
    }

    pub async fn server_access(&self) -> Result<ServerAccessResponse> {
        self.get_json("/server/access").await
    }

    pub async fn update_server_access(
        &self,
        request: UpdateServerAccessRequest,
    ) -> Result<ServerAccessResponse> {
        self.patch_json("/server/access", &request).await
    }

    pub async fn server_status(&self) -> Result<ServerStatusResponse> {
        self.get_json("/server/status").await
    }

    pub async fn approve_tool(
        &self,
        session_id: &str,
        tool_call_id: &str,
        approved: bool,
    ) -> Result<SimpleOkResponse> {
        let lease = self.compatibility_agent_lease(session_id).await?;
        self.approve_tool_with_lease(session_id, tool_call_id, approved, lease.as_deref())
            .await
    }

    pub async fn approve_tool_with_lease(
        &self,
        session_id: &str,
        tool_call_id: &str,
        approved: bool,
        lease_token: Option<&str>,
    ) -> Result<SimpleOkResponse> {
        let request = ToolApprovalRequest {
            session_id: session_id.to_owned(),
            tool_call_id: tool_call_id.to_owned(),
            approved,
        };
        let primary = with_agent_control_lease(
            self.http
                .post(self.api_url(&format!("/sessions/{session_id}/tool-approval")))
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json")
                .json(&serde_json::json!({
                    "tool_call_id": tool_call_id,
                    "approved": approved,
                })),
            lease_token,
        )?;
        let response = primary
            .send()
            .await
            .with_context(|| format!("submitting approval for tool call {tool_call_id}"))?;

        match ensure_success(response).await {
            Ok(response) => response
                .json::<SimpleOkResponse>()
                .await
                .or_else(|_| Ok(SimpleOkResponse { ok: true })),
            Err(primary_error) => {
                let fallback = with_agent_control_lease(
                    self.http
                        .post(self.api_url("/chat/tool-approval"))
                        .header(CONTENT_TYPE, "application/json")
                        .header(ACCEPT, "application/json")
                        .json(&request),
                    lease_token,
                )?
                .send()
                .await
                .context("submitting fallback chat tool approval")?;
                ensure_success(fallback)
                    .await
                    .map_err(|fallback_error| {
                        anyhow!("{primary_error:#}; fallback failed: {fallback_error:#}")
                    })?
                    .json::<SimpleOkResponse>()
                    .await
                    .or_else(|_| Ok(SimpleOkResponse { ok: true }))
            }
        }
    }

    pub async fn chat_stream(&self, request: ChatRequest) -> Result<ChatEventStream> {
        let lease = if request.session_type.unwrap_or(SessionType::Code) == SessionType::Code {
            match request.session_id.as_deref() {
                Some(session_id) => self.compatibility_agent_lease(session_id).await?,
                None => None,
            }
        } else {
            None
        };
        self.chat_stream_with_lease(request, lease.as_deref()).await
    }

    pub async fn chat_stream_with_lease(
        &self,
        request: ChatRequest,
        lease_token: Option<&str>,
    ) -> Result<ChatEventStream> {
        let request = with_agent_control_lease(
            self.http
                .post(self.api_url("/chat"))
                .timeout(STREAM_TIMEOUT)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "text/event-stream")
                .json(&request),
            lease_token,
        )?;
        let response = request.send().await.context("submitting chat request")?;
        let response = ensure_success(response).await?;
        Ok(chat_stream_from_response(response))
    }

    /// Resume an exact pending AskUserQuestion interaction under the current
    /// Code Agent control lease.
    pub async fn answer_agent_input_stream_with_lease(
        &self,
        request: AgentInputResponseRequest,
        lease_token: &str,
    ) -> Result<ChatEventStream> {
        let request = with_agent_control_lease(
            self.http
                .post(self.api_url("/chat/tool-result"))
                .timeout(STREAM_TIMEOUT)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "text/event-stream")
                .json(&request),
            Some(lease_token),
        )?;
        let response = request
            .send()
            .await
            .context("submitting Agent input response")?;
        let response = ensure_success(response).await?;
        Ok(chat_stream_from_response(response))
    }

    /// Inject user input into the active run for an existing session.
    pub async fn steer(&self, request: SteerRequest) -> Result<SteerResponse> {
        let lease = self.compatibility_agent_lease(&request.session_id).await?;
        self.steer_with_lease(request, lease.as_deref()).await
    }

    pub async fn steer_with_lease(
        &self,
        request: SteerRequest,
        lease_token: Option<&str>,
    ) -> Result<SteerResponse> {
        let url = self.api_url("/chat/steer");
        let request = with_agent_control_lease(
            self.http
                .post(&url)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json")
                .json(&request),
            lease_token,
        )?;
        let response = request
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        decode_json_response(response, &url).await
    }

    /// Keep older shipped Rust callers operational under strict Code Agent
    /// authority. Chat/Hive control endpoints return 409 and bypass this path;
    /// Code is claimed only when unowned or already owned by this client.
    async fn compatibility_agent_lease(&self, session_id: &str) -> Result<Option<String>> {
        let mut control = self.compatibility_agent_control.lock().await;
        let url = self.api_url(&format!("/sessions/{session_id}/control"));
        let response = self
            .http
            .get(&url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if response.status() == StatusCode::CONFLICT {
            return Ok(None);
        }
        let state = decode_json_response::<AgentControlState>(response, &url).await?;
        if let Some(controller) = state.controller.as_ref() {
            if controller.client_id != control.client_id {
                return Err(anyhow!(
                    "Code Agent is controlled by another client ({})",
                    controller.client_id
                ));
            }
            if let Some(token) = control.leases.get(session_id) {
                return Ok(Some(token.clone()));
            }
        }

        let response = self
            .take_agent_control(
                session_id,
                &TakeAgentControlRequest {
                    client_id: control.client_id.clone(),
                    force: false,
                },
            )
            .await?;
        let token = response.lease.lease_token;
        if control.leases.len() >= MAX_COMPATIBILITY_AGENT_LEASES
            && !control.leases.contains_key(session_id)
        {
            if let Some(oldest) = control.leases.keys().next().cloned() {
                control.leases.remove(&oldest);
            }
        }
        control.leases.insert(session_id.to_owned(), token.clone());
        Ok(Some(token))
    }

    async fn get_json<T>(&self, path: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.get_json_at(&self.api_url(path)).await
    }

    async fn get_json_at<T>(&self, url: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let response = self
            .http
            .get(url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        decode_json_response(response, url).await
    }

    async fn get_json_with_query<T, Q>(&self, path: &str, query: &Q) -> Result<T>
    where
        T: DeserializeOwned,
        Q: serde::Serialize + ?Sized,
    {
        let url = self.api_url(path);
        let response = self
            .http
            .get(&url)
            .query(query)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        decode_json_response(response, &url).await
    }

    async fn post_json<T, B>(&self, path: &str, body: &B) -> Result<T>
    where
        T: DeserializeOwned,
        B: serde::Serialize + ?Sized,
    {
        let url = self.api_url(path);
        let response = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        decode_json_response(response, &url).await
    }

    async fn put_json<T, B>(&self, path: &str, body: &B) -> Result<T>
    where
        T: DeserializeOwned,
        B: serde::Serialize + ?Sized,
    {
        let url = self.api_url(path);
        let response = self
            .http
            .put(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(body)
            .send()
            .await
            .with_context(|| format!("PUT {url}"))?;
        decode_json_response(response, &url).await
    }

    async fn patch_json<T, B>(&self, path: &str, body: &B) -> Result<T>
    where
        T: DeserializeOwned,
        B: serde::Serialize + ?Sized,
    {
        let url = self.api_url(path);
        let response = self
            .http
            .patch(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(body)
            .send()
            .await
            .with_context(|| format!("PATCH {url}"))?;
        decode_json_response(response, &url).await
    }

    async fn delete_json<T>(&self, path: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let url = self.api_url(path);
        let response = self
            .http
            .delete(&url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .with_context(|| format!("DELETE {url}"))?;
        decode_json_response(response, &url).await
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/api{}", self.base_url, prefixed_path(path))
    }

    fn api_url_with_segments(&self, path: &str, segments: &[&str]) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&self.api_url(path))
            .with_context(|| format!("building Mitsuro API URL for {path}"))?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("Mitsuro API URL cannot be a base URL"))?
            .extend(segments.iter().copied());
        Ok(url)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, prefixed_path(path))
    }
}

fn compatibility_agent_client_id() -> String {
    static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!(
        "mitsuro-client-{}-{now}-{}",
        std::process::id(),
        NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn with_agent_control_lease(
    request: reqwest::RequestBuilder,
    lease_token: Option<&str>,
) -> Result<reqwest::RequestBuilder> {
    let Some(lease_token) = lease_token else {
        return Ok(request);
    };
    let lease_token = lease_token.trim();
    if lease_token.is_empty() {
        return Err(anyhow!("Code Agent control lease cannot be empty"));
    }
    let value =
        HeaderValue::from_str(lease_token).context("validating Code Agent control lease")?;
    Ok(request.header(AGENT_CONTROL_LEASE_HEADER, value))
}

fn browser_control_token(
    capability: BrowserPresenceCapability,
    lease_token: Option<&str>,
) -> Result<Option<&str>> {
    match capability {
        BrowserPresenceCapability::Viewer => {
            if lease_token.is_some() {
                return Err(anyhow!(
                    "Mitsuro Browser observer streams must not carry a control lease"
                ));
            }
            Ok(None)
        }
        BrowserPresenceCapability::Controller => {
            required_opaque_browser_lease(lease_token.unwrap_or_default()).map(Some)
        }
    }
}

fn required_opaque_browser_lease(lease_token: &str) -> Result<&str> {
    let lease_token = lease_token.trim();
    if lease_token.is_empty() {
        Err(anyhow!(
            "Mitsuro Browser control lease is required for mutations"
        ))
    } else {
        Ok(lease_token)
    }
}

fn required_opaque_terminal_lease(lease_token: &str) -> Result<&str> {
    let lease_token = lease_token.trim();
    if lease_token.is_empty() {
        Err(anyhow!(
            "Mitsuro Terminal control lease is required for termination"
        ))
    } else {
        Ok(lease_token)
    }
}

fn normalize_base_url(mut base_url: String) -> String {
    if base_url.trim().is_empty() {
        base_url = "http://127.0.0.1:3000".to_owned();
    }
    base_url.trim().trim_end_matches('/').to_owned()
}

fn prefixed_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    }
}

async fn decode_json_response<T>(response: Response, url: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    ensure_success(response)
        .await?
        .json::<T>()
        .await
        .with_context(|| format!("decoding response from {url}"))
}

async fn ensure_success(response: Response) -> Result<Response> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|_| "Request failed".to_owned());
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .or_else(|| value.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or(body);
    Err(anyhow!("API {status}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delete_session_accepts_successful_empty_response() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let size = socket.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.starts_with("DELETE /api/sessions/session-1 "));
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .expect("write response");
        });

        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        let response = client
            .delete_session("session-1")
            .await
            .expect("empty successful delete response");
        assert!(response.ok);
        server.join().expect("test server join");
    }

    #[tokio::test]
    async fn kill_process_posts_to_scoped_process_endpoint_and_accepts_no_content() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 2048];
            let size = socket.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.starts_with("POST /api/processes/process-1/kill "));
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .expect("write response");
        });

        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        client
            .kill_process("process-1")
            .await
            .expect("empty successful process kill response");
        server.join().expect("test server join");
    }

    #[tokio::test]
    async fn hive_schedule_status_mutations_are_scoped_revisioned_and_idempotent() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            let expected = [
                ("POST", "pause", "paused", 8_u64),
                ("POST", "resume", "enabled", 9_u64),
                ("DELETE", "", "cancelled", 10_u64),
            ];
            for (method, action, status, revision) in expected {
                let (mut socket, _) = listener.accept().expect("accept request");
                let mut request = [0_u8; 4096];
                let size = socket.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..size]);
                let suffix = if action.is_empty() {
                    String::new()
                } else {
                    format!("/{action}")
                };
                assert!(request.starts_with(&format!(
                    "{method} /api/hive/sessions/session%20one/schedules/schedule%2Fone{suffix} "
                )));
                let headers = request.to_ascii_lowercase();
                assert!(headers.contains(&format!("if-match: \"{}\"", revision - 1)));
                assert!(headers.contains("idempotency-key: mutation-key"));
                let body = format!(
                    r#"{{"schedule_id":"schedule/one","revision":{revision},"status":"{status}"}}"#
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .expect("write response");
            }
        });

        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        let paused = client
            .pause_hive_schedule("session one", "schedule/one", 7, Some("mutation-key"))
            .await
            .expect("pause response");
        assert_eq!(paused.status, "paused");
        assert_eq!(paused.revision, 8);

        let resumed = client
            .resume_hive_schedule("session one", "schedule/one", 8, Some("mutation-key"))
            .await
            .expect("resume response");
        assert_eq!(resumed.status, "enabled");
        assert_eq!(resumed.revision, 9);

        let cancelled = client
            .cancel_hive_schedule("session one", "schedule/one", 9, Some("mutation-key"))
            .await
            .expect("cancel response");
        assert_eq!(cancelled.status, "cancelled");
        assert_eq!(cancelled.revision, 10);
        server.join().expect("test server join");
    }

    #[tokio::test]
    async fn hive_schedule_writes_preserve_full_definition_and_concurrency_headers() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            for (method, path, revision, response_revision) in [
                (
                    "POST",
                    "/api/hive/sessions/session%20one/schedules",
                    None,
                    0_u64,
                ),
                (
                    "PUT",
                    "/api/hive/sessions/session%20one/schedules/schedule%2Fone",
                    Some(0_u64),
                    1_u64,
                ),
            ] {
                let (mut socket, _) = listener.accept().expect("accept request");
                let mut request = [0_u8; 8192];
                let size = socket.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..size]);
                assert!(request.starts_with(&format!("{method} {path} ")));
                let headers = request.to_ascii_lowercase();
                assert!(headers.contains("idempotency-key: write-key"));
                if let Some(revision) = revision {
                    assert!(headers.contains(&format!("if-match: \"{revision}\"")));
                } else {
                    assert!(!headers.contains("if-match:"));
                }
                let body = request.split("\r\n\r\n").nth(1).expect("request body");
                let body: serde_json::Value = serde_json::from_str(body).expect("schedule JSON");
                assert_eq!(body["recurrence"]["kind"], "weekly");
                assert_eq!(body["recurrence"]["weekdays"][0], "monday");
                assert_eq!(body["dst_policy"]["gap"], "shift_forward");
                assert_eq!(body["misfire"]["policy"], "fire_once");
                assert_eq!(body["overlap_policy"], "queue_one");
                assert_eq!(body["retry"]["jitter"], "full");
                let response_body = format!(
                    r#"{{"schedule_id":"schedule/one","revision":{response_revision},"status":"enabled"}}"#
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{response_body}",
                            response_body.len()
                        )
                        .as_bytes(),
                    )
                    .expect("write response");
            }
        });

        let definition = HiveScheduleWriteRequest {
            title: "Weekly audit".into(),
            summary: "Inspect the workspace".into(),
            objective: "Run the full audit".into(),
            recurrence: crate::HiveScheduleRecurrence::Weekly {
                start_date: "2026-08-10".into(),
                time: "09:30:00".into(),
                weekdays: vec![crate::HiveScheduleWeekday::Monday],
            },
            timezone: "America/Los_Angeles".into(),
            dst_policy: crate::HiveDstPolicy::default(),
            priority: 2,
            project_dir: Some("/workspace".into()),
            model: Some("gpt-5.5".into()),
            model_key: None,
            crew_slug: Some("audit".into()),
            misfire: crate::HiveMisfireConfig::default(),
            overlap_policy: crate::HiveOverlapPolicy::QueueOne,
            retry: crate::HiveRetryPolicy::default(),
        };
        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        let created = client
            .create_hive_schedule("session one", &definition, Some("write-key"))
            .await
            .expect("create response");
        assert_eq!(created.revision, 0);
        let replaced = client
            .replace_hive_schedule(
                "session one",
                "schedule/one",
                created.revision,
                &definition,
                Some("write-key"),
            )
            .await
            .expect("replace response");
        assert_eq!(replaced.revision, 1);
        server.join().expect("test server join");
    }

    #[tokio::test]
    async fn hive_session_controls_are_typed_scoped_and_idempotent() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            let expected = [
                ("POST", "/api/hive/dispatch", "\"task\":\"Ship Work\""),
                ("GET", "/api/hive/sessions/session%2Fone/status", ""),
                (
                    "POST",
                    "/api/hive/sessions/session%2Fone/message",
                    "\"message\":\"Focus on tests\"",
                ),
                ("POST", "/api/hive/sessions/session%2Fone/pause", "{}"),
                ("POST", "/api/hive/sessions/session%2Fone/resume", "{}"),
                (
                    "POST",
                    "/api/hive/sessions/session%2Fone/priority",
                    "\"priority\":\"high\"",
                ),
                (
                    "POST",
                    "/api/hive/sessions/session%2Fone/crew",
                    "\"crew_slug\":\"release\"",
                ),
                ("DELETE", "/api/hive/sessions/session%2Fone", ""),
            ];
            for (index, (method, path, body_fragment)) in expected.into_iter().enumerate() {
                let (mut socket, _) = listener.accept().expect("accept request");
                let mut request = [0_u8; 8192];
                let size = socket.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..size]);
                assert!(request.starts_with(&format!("{method} {path} ")));
                if !body_fragment.is_empty() {
                    assert!(request.contains(body_fragment), "request was {request}");
                }
                if method != "GET" {
                    assert!(request
                        .to_ascii_lowercase()
                        .contains("idempotency-key: work-key"));
                }
                let (status, body) = match index {
                    0 => (
                        "201 Created",
                        r#"{"session_id":"session/one","status":"started"}"#,
                    ),
                    1 => (
                        "200 OK",
                        r#"{"session_id":"session/one","session_type":"hive","title":"Ship Work","tasks":[{"id":"task-1","session_id":"session/one","subject":"Implement","description":"Wire controls","status":"in_progress","owner":"release","blocked_by":[],"created_at":"2026-08-10T00:00:00Z","updated_at":"2026-08-10T00:01:00Z","completed_at":null,"result":null}],"agent_state":"running","runtime":{"session_id":"session/one","status":"running","next_wake_at":null,"sleep_reason":null,"last_error":null,"current_run_id":"run-1","last_wake_reason":"dispatch","crew_slug":"release","priority":"high","updated_at":"2026-08-10T00:01:00Z"},"cadence":{"tick_interval_secs":30,"max_ticks":1000}}"#,
                    ),
                    7 => ("204 No Content", ""),
                    _ => ("200 OK", r#"{"ok":true}"#),
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .expect("write response");
            }
        });

        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        let dispatch = client
            .dispatch_hive(
                &HiveDispatchRequest {
                    task: "Ship Work".into(),
                    project_dir: Some("/workspace".into()),
                    model: Some("gpt-5.5".into()),
                    model_key: Some(crate::ModelKey {
                        provider: "openai".into(),
                        model_id: "gpt-5.5".into(),
                        auth_scope: Some("chatgpt".into()),
                        api_format: "responses".into(),
                    }),
                    start_at: None,
                    priority: Some(HiveRunPriority::High),
                    crew_slug: Some("release".into()),
                },
                Some("work-key"),
            )
            .await
            .expect("dispatch");
        assert_eq!(dispatch.session_id, "session/one");

        let status = client
            .hive_session_status("session/one")
            .await
            .expect("status");
        assert_eq!(status.tasks[0].subject, "Implement");
        assert_eq!(
            status.runtime.as_ref().map(|runtime| runtime.priority),
            Some(HiveRunPriority::High)
        );

        client
            .send_hive_message("session/one", "Focus on tests", Some("work-key"))
            .await
            .expect("message");
        client
            .pause_hive_session("session/one", Some("work-key"))
            .await
            .expect("pause");
        client
            .resume_hive_session("session/one", Some("work-key"))
            .await
            .expect("resume");
        client
            .set_hive_priority("session/one", HiveRunPriority::High, Some("work-key"))
            .await
            .expect("priority");
        client
            .set_hive_crew("session/one", Some("release".into()), Some("work-key"))
            .await
            .expect("crew");
        client
            .cancel_hive_session("session/one", Some("work-key"))
            .await
            .expect("cancel");
        server.join().expect("test server join");
    }

    #[tokio::test]
    async fn steer_posts_to_the_durable_chat_endpoint() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept request");
            let mut request = [0_u8; 4096];
            let size = socket.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.starts_with("POST /api/chat/steer "));
            assert!(request
                .to_ascii_lowercase()
                .contains("x-mitsuro-control-lease: lease-secret"));
            assert!(request.contains("\"session_id\":\"session-1\""));
            assert!(request.contains("\"message\":\"focus on tests\""));
            let body = r#"{"status":"accepted","pending_id":"pending-1"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .expect("write response");
        });

        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        let response = client
            .steer_with_lease(
                SteerRequest {
                    session_id: "session-1".to_owned(),
                    message: "focus on tests".to_owned(),
                    content: Vec::new(),
                },
                Some("lease-secret"),
            )
            .await
            .expect("steer response");
        assert_eq!(response.status, "accepted");
        assert_eq!(response.pending_id, "pending-1");
        server.join().expect("test server join");
    }

    #[tokio::test]
    async fn compatibility_steer_claims_control_before_serializing_the_mutation() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            let responses = [
                r#"{"session_id":"session-1","controller":null,"ttl_seconds":15}"#,
                r#"{"lease":{"resource_id":"session-1","client_id":"compat","lease_token":"compat-secret","expires_at":"later"}}"#,
                r#"{"status":"accepted","pending_id":"pending-1"}"#,
            ];
            for (index, body) in responses.into_iter().enumerate() {
                let (mut socket, _) = listener.accept().expect("accept request");
                let mut request = [0_u8; 8192];
                let size = socket.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..size]);
                match index {
                    0 => assert!(request.starts_with("GET /api/sessions/session-1/control ")),
                    1 => {
                        assert!(request.starts_with("POST /api/sessions/session-1/control "));
                        assert!(request.contains("\"force\":false"));
                        assert!(request.contains("\"client_id\":\"mitsuro-client-"));
                    }
                    2 => {
                        assert!(request.starts_with("POST /api/chat/steer "));
                        assert!(request
                            .to_ascii_lowercase()
                            .contains("x-mitsuro-control-lease: compat-secret"));
                    }
                    _ => unreachable!(),
                }
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .expect("write response");
            }
        });

        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        let response = client
            .steer(SteerRequest {
                session_id: "session-1".to_owned(),
                message: "preserve shipped caller".to_owned(),
                content: Vec::new(),
            })
            .await
            .expect("compatibility steer");
        assert_eq!(response.pending_id, "pending-1");
        server.join().expect("test server join");
    }

    #[test]
    fn normalizes_blank_base_url_to_local_server() {
        let client = MitsuroClient::new(" ").expect("client");
        assert_eq!(client.base_url(), "http://127.0.0.1:3000");
    }

    #[test]
    fn trims_trailing_slashes() {
        let client = MitsuroClient::new("http://localhost:3000///").expect("client");
        assert_eq!(client.base_url(), "http://localhost:3000");
    }

    #[test]
    fn builds_terminal_replay_websocket_url() {
        let client = MitsuroClient::new("https://mitsuro.example").expect("client");
        assert_eq!(
            client
                .terminal_stream_url("terminal-1", Some(42))
                .expect("terminal URL"),
            "wss://mitsuro.example/api/terminals/terminal-1/stream?after_offset=42"
        );
    }

    #[test]
    fn delete_terminal_request_carries_the_opaque_control_lease() {
        let client = MitsuroClient::new("https://mitsuro.example").expect("client");
        let (_, request) = client
            .terminal_delete_request("terminal-1", "  opaque-lease  ")
            .expect("delete request");
        assert_eq!(request.method(), reqwest::Method::DELETE);
        assert_eq!(request.url().path(), "/api/terminals/terminal-1");
        assert_eq!(
            request
                .headers()
                .get(crate::TERMINAL_CONTROL_LEASE_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("opaque-lease")
        );
    }

    #[test]
    fn delete_terminal_rejects_a_blank_lease_before_network_io() {
        let client = MitsuroClient::local().expect("client");
        let error = client
            .terminal_delete_request("terminal-1", "  ")
            .expect_err("blank lease must fail");
        assert!(error.to_string().contains("control lease is required"));
    }

    #[test]
    fn canvas_collaboration_paths_encode_opaque_ids_as_segments() {
        let client = MitsuroClient::new("https://mitsuro.example").expect("client");
        let url = client
            .api_url_with_segments(
                "/canvases",
                &["canvas/with/slashes", "presence", "client?one"],
            )
            .expect("collaboration URL");
        assert_eq!(
            url.as_str(),
            "https://mitsuro.example/api/canvases/canvas%2Fwith%2Fslashes/presence/client%3Fone"
        );
    }

    #[tokio::test]
    async fn canvas_file_requests_encode_canvas_and_relative_file_paths() {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = std::thread::spawn(move || {
            let expected = [
                (
                    "/api/canvases/canvas%2Fone/files/tree?depth=6",
                    r#"{"root":".","entries":[]}"#,
                ),
                (
                    "/api/canvases/canvas%2Fone/files?path=src%2Flib.rs",
                    r#"{"path":"src/lib.rs","content":"safe","size":4}"#,
                ),
            ];
            for (path, body) in expected {
                let (mut socket, _) = listener.accept().expect("accept request");
                let mut request = [0_u8; 4096];
                let size = socket.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..size]);
                assert!(
                    request.starts_with(&format!("GET {path} ")),
                    "request was {request}"
                );
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .expect("write response");
            }
        });

        let client = MitsuroClient::new(format!("http://{address}")).expect("client");
        let tree = client
            .canvas_file_tree("canvas/one", 6)
            .await
            .expect("tree response");
        assert_eq!(tree.root, ".");
        let file = client
            .canvas_read_file("canvas/one", "src/lib.rs")
            .await
            .expect("file response");
        assert_eq!(file.content, "safe");
        server.join().expect("test server join");
    }

    #[test]
    fn browser_stream_credentials_use_headers_and_never_the_url() {
        let client =
            MitsuroClient::with_bearer_token("https://mitsuro.example", "private-auth-token")
                .expect("client");
        let url = client
            .browser_stream_url(
                "/api/browser/browser-1/stream",
                BrowserPresenceCapability::Controller,
                "canvas-a",
                Some("opaque-lease-token"),
            )
            .expect("controller stream URL");
        assert_eq!(
            url,
            "wss://mitsuro.example/api/browser/browser-1/stream?capability=controller&client_id=canvas-a"
        );
        assert!(!url.contains("private-auth-token"));
        assert!(!url.contains("opaque-lease-token"));
        let request = client
            .browser_websocket_request(&url, Some("opaque-lease-token"))
            .expect("same-origin authenticated request");
        assert_eq!(
            request
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer private-auth-token")
        );
        assert_eq!(
            request
                .headers()
                .get(crate::BROWSER_CONTROL_LEASE_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("opaque-lease-token")
        );
    }

    #[test]
    fn browser_observer_stream_has_no_control_token() {
        let client = MitsuroClient::new("http://127.0.0.1:3000").expect("client");
        assert_eq!(
            client
                .browser_stream_url(
                    "/api/browser/browser-1/stream",
                    BrowserPresenceCapability::Viewer,
                    "canvas-b",
                    None,
                )
                .expect("observer URL"),
            "ws://127.0.0.1:3000/api/browser/browser-1/stream?capability=viewer&client_id=canvas-b"
        );
        assert!(client
            .browser_stream_url(
                "/api/browser/browser-1/stream",
                BrowserPresenceCapability::Viewer,
                "canvas-b",
                Some("should-not-leak"),
            )
            .is_err());
    }

    #[test]
    fn browser_stream_rejects_noncanonical_paths_and_missing_controller_lease() {
        let client = MitsuroClient::new("https://mitsuro.example").expect("client");
        assert!(client
            .browser_stream_url(
                "https://attacker.invalid/api/browser/x/stream",
                BrowserPresenceCapability::Viewer,
                "canvas-a",
                None,
            )
            .is_err());
        assert!(client
            .browser_stream_url(
                "/api/browser/browser-1/stream?token=auth",
                BrowserPresenceCapability::Viewer,
                "canvas-a",
                None,
            )
            .is_err());
        assert!(client
            .browser_stream_url(
                "/api/browser/browser-1/stream",
                BrowserPresenceCapability::Controller,
                "canvas-a",
                None,
            )
            .is_err());
    }

    #[test]
    fn websocket_request_carries_auth_without_exposing_it_in_debug() {
        let client = MitsuroClient::with_bearer_token("https://mitsuro.example", "secret-token")
            .expect("client");
        let request = client
            .websocket_request("wss://mitsuro.example/api/terminals/terminal-1/stream")
            .expect("same-origin WebSocket request");
        assert_eq!(
            request
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer secret-token")
        );
        let debug = format!("{client:?}");
        assert!(debug.contains("authenticated: true"));
        assert!(!debug.contains("secret-token"));
    }

    #[test]
    fn websocket_request_rejects_foreign_origins_before_copying_auth() {
        let client = MitsuroClient::with_bearer_token("https://mitsuro.example", "secret-token")
            .expect("client");
        let error = client
            .websocket_request("wss://attacker.invalid/collect")
            .expect_err("foreign origin must fail");
        assert!(error.to_string().contains("configured server origin"));
    }

    #[test]
    fn rejects_empty_remote_bearer_token() {
        let error = MitsuroClient::with_bearer_token("https://mitsuro.example", "  ")
            .expect_err("blank token must fail");
        assert!(error.to_string().contains("cannot be empty"));
    }
}
