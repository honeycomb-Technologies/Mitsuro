//! Atlas: authenticated, server-owned Chromium sessions powered by agent-browser.
//!
//! The native stream and CDP endpoints stay loopback-only. Clients receive the
//! Mitsuro WebSocket proxy and a deliberately small semantic action surface.

use std::{
    collections::HashMap,
    env,
    path::{Path as FsPath, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};

use axum::{
    extract::{
        ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::{Mutex, Notify, RwLock},
    time::timeout,
};
use tokio_tungstenite::{connect_async, tungstenite::Message as UpstreamMessage};
use uuid::Uuid;

use mitsuro_core::tools::{registry::Tool, ToolContext, ToolRegistry, ToolResult};

use crate::{
    auth::{AuthenticatedUser, CurrentUser},
    error::AppError,
    utils::workspace::{allowed_root, resolve_scoped_workspace_path, workspace_base},
    AppState,
};

const MAX_SESSIONS_PER_OWNER: usize = 6;
const MAX_CANVAS_SESSIONS_PER_OWNER: usize = 4;
const MAX_ACTIONS: usize = 32;
const MAX_TEXT_BYTES: usize = 32 * 1024;
const PRESENCE_TTL: Duration = Duration::from_secs(20);
const CONTROL_LEASE_TTL: Duration = Duration::from_secs(15);
const COMMAND_TIMEOUT: Duration = Duration::from_secs(45);
const AGENT_TIMEOUT: Duration = Duration::from_secs(180);
const CONTROL_LEASE_HEADER: &str = "x-mitsuro-control-lease";

static ATLAS: OnceLock<AtlasRuntime> = OnceLock::new();

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_sessions).post(create_session))
        .route("/:id", get(get_session))
        .route("/:id/stop", post(stop_session))
        .route("/:id/control", post(take_control))
        .route("/:id/heartbeat", post(heartbeat))
        .route("/:id/actions", post(run_actions))
        .route("/:id/stream", get(stream_session))
}

pub(crate) async fn register_tool(registry: &ToolRegistry) {
    registry.register(Arc::new(AtlasBrowserTool)).await;
}

struct AtlasBrowserTool;

#[derive(Debug, Deserialize)]
struct BrowserToolRequest {
    #[serde(default)]
    list_tabs: bool,
    tab_id: Option<String>,
    /// Injected by a trusted client/runtime bridge. It is deliberately omitted
    /// from the model-facing schema so a provider is never invited to request,
    /// echo, or fabricate this bearer secret.
    lease_token: Option<String>,
    #[serde(default)]
    actions: Vec<BrowserAction>,
}

#[async_trait::async_trait]
impl Tool for AtlasBrowserTool {
    fn name(&self) -> &str {
        "browser"
    }

    fn description(&self) -> &str {
        "Inspect and control the user's visible Mitsuro Chromium tabs through semantic browser actions."
    }

    fn prompt(&self) -> Option<&str> {
        Some(
            "Use browser with list_tabs=true to discover the user's open visible tabs. Use a compact interactive snapshot before clicking or filling, then act only on references returned by that snapshot. This tool controls the same browser surface the user sees; do not start a separate browser agent chat.",
        )
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "list_tabs": {
                    "type": "boolean",
                    "description": "List the user's visible browser tabs without performing actions."
                },
                "tab_id": {
                    "type": "string",
                    "description": "Visible browser tab id. Omit to use the most recently active runnable tab."
                },
                "actions": {
                    "type": "array",
                    "maxItems": MAX_ACTIONS,
                    "description": "Ordered semantic browser actions.",
                    "items": {
                        "oneOf": [
                            {"type":"object","properties":{"type":{"const":"navigate"},"url":{"type":"string"}},"required":["type","url"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"snapshot"},"interactive":{"type":"boolean"},"compact":{"type":"boolean"},"depth":{"type":"integer","minimum":1,"maximum":20}},"required":["type"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"click"},"target":{"type":"string"}},"required":["type","target"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"enum":["fill","type"]},"target":{"type":"string"},"value":{"type":"string"}},"required":["type","target","value"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"press"},"key":{"type":"string"}},"required":["type","key"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"hover"},"target":{"type":"string"}},"required":["type","target"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"select"},"target":{"type":"string"},"values":{"type":"array","items":{"type":"string"}}},"required":["type","target","values"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"scroll"},"direction":{"enum":["up","down","left","right"]},"amount":{"type":"integer","minimum":1}},"required":["type","direction"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"enum":["back","forward","reload"]}},"required":["type"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"wait"},"ms":{"type":"integer","minimum":0,"maximum":30000}},"required":["type","ms"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"get"},"property":{"enum":["text","html","value","title","url","count"]},"target":{"type":"string"}},"required":["type","property"],"additionalProperties":false},
                            {"type":"object","properties":{"type":{"const":"attribute"},"target":{"type":"string"},"name":{"type":"string"}},"required":["type","target","name"],"additionalProperties":false}
                            ,{"type":"object","properties":{"type":{"const":"viewport"},"mode":{"enum":["mobile","desktop"]}},"required":["type","mode"],"additionalProperties":false}
                        ]
                    }
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, params: Value, ctx: &ToolContext) -> ToolResult {
        let request = match serde_json::from_value::<BrowserToolRequest>(params) {
            Ok(request) => request,
            Err(error) => return ToolResult::invalid_parameters(error),
        };
        let owner = OwnerKey::from_user_id(ctx.user_id.as_deref());
        let records: Vec<_> = runtime().sessions.read().await.values().cloned().collect();
        let mut owned = Vec::new();
        for record in records.into_iter().filter(|record| record.owner == owner) {
            let updated_at = record.state.read().await.public.updated_at.clone();
            owned.push((updated_at, record));
        }
        owned.sort_by(|left, right| right.0.cmp(&left.0));

        if request.list_tabs {
            let mut tabs = Vec::new();
            for (_, record) in owned {
                tabs.push(public_session(&record).await);
            }
            return ToolResult::success_data(json!({ "tabs": tabs }));
        }
        if request.actions.is_empty() || request.actions.len() > MAX_ACTIONS {
            return ToolResult::invalid_parameters(format!(
                "browser actions must contain between 1 and {MAX_ACTIONS} items"
            ));
        }
        let record = if let Some(tab_id) = request.tab_id.as_deref() {
            let mut found = None;
            for (_, record) in owned {
                if record.state.read().await.public.id == tab_id {
                    found = Some(record);
                    break;
                }
            }
            found
        } else {
            let mut found = None;
            for (_, record) in owned {
                if matches!(
                    record.state.read().await.public.status,
                    BrowserSessionStatus::Ready | BrowserSessionStatus::Running
                ) {
                    found = Some(record);
                    break;
                }
            }
            found
        };
        let Some(record) = record else {
            return ToolResult::error_with_code(
                "browser_tab_not_found",
                "No runnable visible browser tab was found. Ask the user to open the Browser surface.",
            );
        };
        if let Err((_, Json(body))) = ensure_runnable(&record).await {
            return ToolResult::error_with_code(
                "browser_tab_not_runnable",
                body.get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("Browser tab is not runnable"),
            );
        }
        let Some(lease_token) = request.lease_token.as_deref() else {
            return ToolResult::error_with_code(
                "browser_control_required",
                "Browser actions require an active control lease from a trusted client.",
            );
        };
        if let Err((_, Json(body))) = authorize_control(&record, lease_token, None).await {
            return ToolResult::error_with_code(
                "browser_control_rejected",
                body.get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("Browser control lease was rejected"),
            );
        }
        let commands = match action_commands(&request.actions) {
            Ok(commands) => commands,
            Err((_, Json(body))) => {
                return ToolResult::invalid_parameters(
                    body.get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("Invalid browser action"),
                )
            }
        };
        let input = match serde_json::to_vec(&commands) {
            Ok(input) => input,
            Err(error) => return ToolResult::error_with_code("browser_encode_failed", error),
        };
        {
            let mut state = record.state.write().await;
            state.public.status = BrowserSessionStatus::Running;
            state.public.updated_at = chrono::Utc::now().to_rfc3339();
        }
        match run_cli(
            &record,
            vec!["batch".into(), "--bail".into()],
            Some(input),
            AGENT_TIMEOUT,
        )
        .await
        {
            Ok(results) => {
                let mut state = record.state.write().await;
                state.public.status = BrowserSessionStatus::Ready;
                state.public.updated_at = chrono::Utc::now().to_rfc3339();
                if let Some(url) = latest_url(&results) {
                    state.public.url = Some(url);
                }
                if let Some(mode) = latest_viewport_mode(&request.actions) {
                    state.public.viewport_mode = mode;
                }
                ToolResult::success_data(json!({
                    "tab": state.public,
                    "results": results
                }))
            }
            Err(error) => {
                let mut state = record.state.write().await;
                state.public.status = BrowserSessionStatus::Ready;
                state.public.updated_at = chrono::Utc::now().to_rfc3339();
                state.public.last_error = Some(error.clone());
                ToolResult::error_with_code("browser_action_failed", error)
            }
        }
    }
}

fn runtime() -> &'static AtlasRuntime {
    ATLAS.get_or_init(AtlasRuntime::default)
}

#[derive(Default)]
struct AtlasRuntime {
    sessions: RwLock<HashMap<String, Arc<SessionRecord>>>,
}

#[cfg(test)]
pub(crate) struct CanvasBrowserEnsureTestGate {
    pub(crate) before_insert: tokio::sync::Semaphore,
    pub(crate) release_insert: tokio::sync::Semaphore,
    pub(crate) after_insert: tokio::sync::Semaphore,
    pub(crate) release_launch: tokio::sync::Semaphore,
    runtime_executable: PathBuf,
}

#[cfg(test)]
fn canvas_browser_ensure_test_gates(
) -> &'static Mutex<HashMap<String, Arc<CanvasBrowserEnsureTestGate>>> {
    static GATES: OnceLock<Mutex<HashMap<String, Arc<CanvasBrowserEnsureTestGate>>>> =
        OnceLock::new();
    GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) async fn install_canvas_browser_ensure_test_gate(
    browser_id: &str,
    runtime_executable: PathBuf,
) -> Arc<CanvasBrowserEnsureTestGate> {
    let gate = Arc::new(CanvasBrowserEnsureTestGate {
        before_insert: tokio::sync::Semaphore::new(0),
        release_insert: tokio::sync::Semaphore::new(0),
        after_insert: tokio::sync::Semaphore::new(0),
        release_launch: tokio::sync::Semaphore::new(0),
        runtime_executable,
    });
    canvas_browser_ensure_test_gates()
        .lock()
        .await
        .insert(browser_id.to_owned(), Arc::clone(&gate));
    gate
}

#[cfg(test)]
pub(crate) async fn remove_canvas_browser_ensure_test_gate(browser_id: &str) {
    canvas_browser_ensure_test_gates()
        .lock()
        .await
        .remove(browser_id);
}

#[cfg(test)]
async fn canvas_browser_ensure_test_gate(
    browser_id: &str,
) -> Option<Arc<CanvasBrowserEnsureTestGate>> {
    canvas_browser_ensure_test_gates()
        .lock()
        .await
        .get(browser_id)
        .cloned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnerKey(Option<String>);

impl OwnerKey {
    fn from_user_id(user_id: Option<&str>) -> Self {
        Self(user_id.map(ToOwned::to_owned))
    }

    fn fallback_client_id(&self) -> &str {
        self.0.as_deref().unwrap_or("local")
    }
}

struct SessionRecord {
    owner: OwnerKey,
    session_name: String,
    creation_complete: AtomicBool,
    creation_notify: Notify,
    creation_spec: BrowserCreationSpec,
    reserves_capacity: AtomicBool,
    runtime_executable_override: Option<PathBuf>,
    state: RwLock<SessionState>,
    command_lock: Mutex<()>,
}

struct SessionState {
    public: BrowserSession,
    termination: RuntimeTerminationState,
    stream_port: Option<u16>,
    viewers: HashMap<String, Instant>,
    controller: Option<ControlLeaseState>,
}

#[derive(Debug, Clone)]
struct ControlLeaseState {
    client_id: String,
    lease_token: String,
    last_seen: Instant,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserCapability {
    available: bool,
    runtime: &'static str,
    version: &'static str,
    executable: Option<String>,
    live_stream: bool,
    semantic_actions: bool,
    agent_chat: bool,
    control_leases: bool,
    control_lease_version: u16,
    control_lease_ttl_seconds: u64,
    reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserListResponse {
    sessions: Vec<BrowserSession>,
    capability: BrowserCapability,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserSession {
    id: String,
    title: String,
    project_dir: String,
    kind: BrowserSessionKind,
    status: BrowserSessionStatus,
    url: Option<String>,
    /// Intentionally always absent: raw CDP is never a client capability.
    cdp_url: Option<String>,
    debug_port: Option<u16>,
    stream_url: Option<String>,
    viewers: usize,
    controllers: usize,
    controller: Option<BrowserControlLeaseSummary>,
    last_error: Option<String>,
    created_at: String,
    updated_at: String,
    viewport_mode: ViewportMode,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct BrowserControlLeaseSummary {
    client_id: String,
    expires_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct BrowserControlLease {
    resource_id: String,
    client_id: String,
    lease_token: String,
    expires_at: String,
}

#[derive(Debug, Serialize)]
struct TakeBrowserControlResponse {
    lease: BrowserControlLease,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ViewportMode {
    Mobile,
    Desktop,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum BrowserSessionKind {
    Interactive,
    Agent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BrowserCreationSpec {
    owner: OwnerKey,
    title: String,
    project_dir: String,
    kind: BrowserSessionKind,
    initial_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeTerminationState {
    NeverStarted,
    MayBeLive,
    ConfirmedStopped,
}

enum InsertSessionOutcome {
    Inserted,
    Existing(Arc<SessionRecord>),
}

struct BrowserCreationCompletionGuard {
    record: Arc<SessionRecord>,
}

impl Drop for BrowserCreationCompletionGuard {
    fn drop(&mut self) {
        self.record.creation_complete.store(true, Ordering::Release);
        self.record.creation_notify.notify_waiters();
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum BrowserSessionStatus {
    Starting,
    Ready,
    Running,
    Stopped,
    Error,
}

#[derive(Debug, Deserialize)]
struct CreateBrowserSession {
    title: Option<String>,
    project_dir: Option<String>,
    #[serde(default = "interactive_kind")]
    kind: BrowserSessionKind,
    url: Option<String>,
    #[allow(dead_code)]
    launch_local: Option<bool>,
}

fn interactive_kind() -> BrowserSessionKind {
    BrowserSessionKind::Interactive
}

#[derive(Debug, Deserialize)]
struct HeartbeatRequest {
    #[serde(default)]
    capability: PresenceCapability,
    client_id: Option<String>,
    lease_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TakeBrowserControlRequest {
    client_id: String,
}

#[derive(Debug, Deserialize)]
struct StopBrowserSessionRequest {
    lease_token: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PresenceCapability {
    #[default]
    Viewer,
    Controller,
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    #[serde(default)]
    capability: PresenceCapability,
    client_id: Option<String>,
    lease_token: Option<String>,
    #[allow(dead_code)]
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ActionBatch {
    lease_token: Option<String>,
    actions: Vec<BrowserAction>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BrowserAction {
    Navigate {
        url: String,
    },
    Snapshot {
        #[serde(default)]
        interactive: bool,
        #[serde(default)]
        compact: bool,
        depth: Option<u16>,
    },
    Click {
        target: String,
    },
    Fill {
        target: String,
        value: String,
    },
    Type {
        target: String,
        value: String,
    },
    Press {
        key: String,
    },
    Hover {
        target: String,
    },
    Select {
        target: String,
        values: Vec<String>,
    },
    Scroll {
        direction: ScrollDirection,
        amount: Option<u32>,
    },
    Back,
    Forward,
    Reload,
    Wait {
        ms: u32,
    },
    Get {
        property: GetProperty,
        target: Option<String>,
    },
    Attribute {
        target: String,
        name: String,
    },
    Viewport {
        mode: ViewportMode,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScrollDirection {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GetProperty {
    Text,
    Html,
    Value,
    Title,
    Url,
    Count,
}

type ApiResult<T> = Result<T, (StatusCode, Json<Value>)>;

async fn list_sessions(CurrentUser(user): CurrentUser) -> Json<BrowserListResponse> {
    let owner = OwnerKey::from_user_id(user.user_id.as_deref());
    let records: Vec<_> = runtime().sessions.read().await.values().cloned().collect();
    let mut sessions = Vec::new();
    for record in records {
        if record.owner == owner {
            sessions.push(public_session(&record).await);
        }
    }
    sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(BrowserListResponse {
        sessions,
        capability: browser_capability(),
    })
}

async fn create_session(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Json(request): Json<CreateBrowserSession>,
) -> ApiResult<Json<BrowserSession>> {
    let owner = OwnerKey::from_user_id(user.user_id.as_deref());
    let url = normalize_url(request.url.as_deref().unwrap_or("about:blank"))?;
    let title = bounded_text(request.title.as_deref().unwrap_or("Browser"), 120)?;
    let project_dir = canonical_browser_project_dir(&state, &user, request.project_dir.as_deref())
        .map_err(app_error_to_api)?;

    let session = create_browser_session_with_id(
        Uuid::new_v4().to_string(),
        owner,
        title,
        project_dir,
        request.kind,
        url,
        MAX_SESSIONS_PER_OWNER,
    )
    .await?;
    Ok(Json(session))
}

async fn create_browser_session_with_id(
    id: String,
    owner: OwnerKey,
    title: String,
    project_dir: String,
    kind: BrowserSessionKind,
    url: String,
    active_limit: usize,
) -> ApiResult<BrowserSession> {
    create_browser_session_with_id_and_runtime(
        id,
        owner,
        title,
        project_dir,
        kind,
        url,
        active_limit,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_browser_session_with_id_and_runtime(
    id: String,
    owner: OwnerKey,
    title: String,
    project_dir: String,
    kind: BrowserSessionKind,
    url: String,
    active_limit: usize,
    runtime_executable_override: Option<PathBuf>,
) -> ApiResult<BrowserSession> {
    if Uuid::parse_str(&id).is_err() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "Browser resource id must be a UUID",
        ));
    }
    if let Some(existing) = runtime().sessions.read().await.get(&id).cloned() {
        return wait_and_reconcile_browser_create(&existing, &owner, &title, &project_dir).await;
    }

    #[cfg(test)]
    let mut test_gate = canvas_browser_ensure_test_gate(&id).await;
    #[cfg(test)]
    let runtime_executable_override = if let Some(gate) = test_gate.as_ref() {
        gate.before_insert.add_permits(1);
        let _permit = gate.release_insert.acquire().await.map_err(|_| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Canvas Browser insertion test gate closed",
            )
        })?;
        Some(gate.runtime_executable.clone())
    } else {
        runtime_executable_override
    };

    let runtime_available =
        runtime_executable_override.is_some() || discover_agent_browser().is_some();
    if !runtime_available {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            missing_runtime_message(),
        ));
    }

    let creation_spec = BrowserCreationSpec {
        owner: owner.clone(),
        title: title.clone(),
        project_dir: project_dir.clone(),
        kind,
        initial_url: url.clone(),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let record = Arc::new(SessionRecord {
        owner: owner.clone(),
        session_name: format!("atlas-{}", id.replace('-', "")),
        creation_complete: AtomicBool::new(false),
        creation_notify: Notify::new(),
        creation_spec: creation_spec.clone(),
        reserves_capacity: AtomicBool::new(true),
        runtime_executable_override: runtime_executable_override.clone(),
        state: RwLock::new(SessionState {
            public: BrowserSession {
                id: id.clone(),
                title,
                project_dir,
                kind,
                status: BrowserSessionStatus::Starting,
                url: Some(url.clone()),
                cdp_url: None,
                debug_port: None,
                stream_url: None,
                viewers: 0,
                controllers: 0,
                controller: None,
                last_error: None,
                created_at: now.clone(),
                updated_at: now,
                viewport_mode: ViewportMode::Mobile,
            },
            termination: RuntimeTerminationState::NeverStarted,
            stream_port: None,
            viewers: HashMap::new(),
            controller: None,
        }),
        command_lock: Mutex::new(()),
    });
    prune_finished_owned(&owner).await;
    match insert_bounded_session_with_limit(&id, Arc::clone(&record), active_limit).await? {
        InsertSessionOutcome::Inserted => {}
        InsertSessionOutcome::Existing(existing) => {
            return wait_and_reconcile_browser_create(
                &existing,
                &owner,
                &creation_spec.title,
                &creation_spec.project_dir,
            )
            .await;
        }
    }

    #[cfg(test)]
    if let Some(gate) = test_gate.take() {
        gate.after_insert.add_permits(1);
        let _permit = gate.release_launch.acquire().await.map_err(|_| {
            api_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Canvas Browser launch test gate closed",
            )
        })?;
    }

    let _completion = BrowserCreationCompletionGuard {
        record: Arc::clone(&record),
    };
    launch_browser_session(&record, &id, &url).await;
    Ok(public_session(&record).await)
}

async fn launch_browser_session(record: &Arc<SessionRecord>, id: &str, url: &str) {
    let launch_commands = match serde_json::to_vec(&vec![
        vec!["open", url],
        vec!["set", "device", "iPhone 12"],
        vec!["set", "viewport", "390", "700", "3"],
    ]) {
        Ok(commands) => commands,
        Err(error) => {
            let mut state = record.state.write().await;
            state.public.status = BrowserSessionStatus::Error;
            state.public.last_error =
                Some(format!("Could not encode Browser command batch: {error}"));
            state.public.updated_at = chrono::Utc::now().to_rfc3339();
            return;
        }
    };
    let launch_result = run_cli(
        record,
        vec!["batch".into(), "--bail".into()],
        Some(launch_commands),
        COMMAND_TIMEOUT,
    )
    .await;
    match launch_result {
        Ok(_) => {
            let stream = run_cli(
                record,
                vec!["stream".into(), "status".into()],
                None,
                COMMAND_TIMEOUT,
            )
            .await;
            let mut state = record.state.write().await;
            state.termination = RuntimeTerminationState::MayBeLive;
            state.public.status = BrowserSessionStatus::Ready;
            state.public.updated_at = chrono::Utc::now().to_rfc3339();
            match stream.and_then(|value| {
                find_stream_port(&value)
                    .ok_or_else(|| "stream port missing from runtime response".into())
            }) {
                Ok(port) => {
                    state.stream_port = Some(port);
                    state.public.stream_url = Some(format!("/api/browser/{id}/stream"));
                }
                Err(error) => {
                    state.public.last_error = Some(format!("Live stream unavailable: {error}"))
                }
            }
        }
        Err(error) => {
            let mut state = record.state.write().await;
            state.public.status = BrowserSessionStatus::Error;
            state.public.last_error = Some(error);
            state.public.updated_at = chrono::Utc::now().to_rfc3339();
        }
    }
}

async fn wait_and_reconcile_browser_create(
    record: &SessionRecord,
    owner: &OwnerKey,
    title: &str,
    project_dir: &str,
) -> ApiResult<BrowserSession> {
    if &record.owner != owner {
        return Err(api_error(StatusCode::NOT_FOUND, "Browser tab not found"));
    }
    if record.creation_spec.title != title || record.creation_spec.project_dir != project_dir {
        return Err(api_error(
            StatusCode::CONFLICT,
            "Browser create operation was reused with different input",
        ));
    }
    loop {
        if record.creation_complete.load(Ordering::Acquire) {
            break;
        }
        let notified = record.creation_notify.notified();
        if record.creation_complete.load(Ordering::Acquire) {
            break;
        }
        notified.await;
    }
    if record.state.read().await.termination == RuntimeTerminationState::NeverStarted
        && record.state.read().await.public.status == BrowserSessionStatus::Error
    {
        return Err(api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            record
                .state
                .read()
                .await
                .public
                .last_error
                .clone()
                .unwrap_or_else(missing_runtime_message),
        ));
    }
    Ok(public_session(record).await)
}

async fn get_session(
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
) -> ApiResult<Json<BrowserSession>> {
    let record = owned_session(&id, user.user_id.as_deref()).await?;
    Ok(Json(public_session(&record).await))
}

async fn stop_session(
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
    Json(request): Json<StopBrowserSessionRequest>,
) -> ApiResult<Json<BrowserSession>> {
    let record = owned_session(&id, user.user_id.as_deref()).await?;
    let cleanup_only = matches!(
        record.state.read().await.public.status,
        BrowserSessionStatus::Error | BrowserSessionStatus::Stopped
    );
    let result = if cleanup_only {
        Ok(Value::Null)
    } else {
        let lease_token = require_control_token(request.lease_token.as_deref())?;
        authorize_control(&record, lease_token, None).await?;
        run_cli(&record, vec!["close".into()], None, COMMAND_TIMEOUT).await
    };
    let mut state = record.state.write().await;
    state.public.updated_at = chrono::Utc::now().to_rfc3339();
    state.viewers.clear();
    state.controller = None;
    state.public.viewers = 0;
    state.public.controllers = 0;
    state.public.controller = None;
    state.stream_port = None;
    state.public.stream_url = None;
    match result {
        Ok(_) => {
            state.public.status = BrowserSessionStatus::Stopped;
            state.public.last_error = None;
        }
        Err(error) => {
            state.public.status = BrowserSessionStatus::Error;
            state.public.last_error = Some(error);
        }
    }
    let response = state.public.clone();
    drop(state);
    runtime().sessions.write().await.remove(&id);
    Ok(Json(response))
}

async fn heartbeat(
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
    Json(request): Json<HeartbeatRequest>,
) -> ApiResult<Json<BrowserSession>> {
    let record = owned_session(&id, user.user_id.as_deref()).await?;
    match request.capability {
        PresenceCapability::Viewer => {
            claim_viewer(
                &record,
                request.client_id.as_deref(),
                user.user_id.as_deref(),
            )
            .await?;
        }
        PresenceCapability::Controller => {
            let lease_token = require_control_token(request.lease_token.as_deref())?;
            authorize_control(&record, lease_token, request.client_id.as_deref()).await?;
        }
    }
    Ok(Json(public_session(&record).await))
}

async fn take_control(
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
    Json(request): Json<TakeBrowserControlRequest>,
) -> ApiResult<Json<TakeBrowserControlResponse>> {
    let record = owned_session(&id, user.user_id.as_deref()).await?;
    ensure_runnable(&record).await?;
    let lease = issue_control_lease(&record, &request.client_id).await?;
    Ok(Json(TakeBrowserControlResponse { lease }))
}

async fn run_actions(
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
    Json(request): Json<ActionBatch>,
) -> ApiResult<Json<Value>> {
    if request.actions.is_empty() || request.actions.len() > MAX_ACTIONS {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "actions must contain between 1 and 32 entries",
        ));
    }
    let record = owned_session(&id, user.user_id.as_deref()).await?;
    let lease_token = require_control_token(request.lease_token.as_deref())?;
    authorize_control(&record, lease_token, None).await?;
    ensure_runnable(&record).await?;
    let commands = action_commands(&request.actions)?;
    let input = serde_json::to_vec(&commands).map_err(internal_error)?;
    {
        let mut state = record.state.write().await;
        state.public.status = BrowserSessionStatus::Running;
        state.public.updated_at = chrono::Utc::now().to_rfc3339();
    }
    let output = run_cli(
        &record,
        vec!["batch".into(), "--bail".into()],
        Some(input),
        COMMAND_TIMEOUT,
    )
    .await
    .map_err(runtime_error)?;

    let mut state = record.state.write().await;
    state.public.status = BrowserSessionStatus::Ready;
    state.public.updated_at = chrono::Utc::now().to_rfc3339();
    if let Some(url) = latest_url(&output) {
        state.public.url = Some(url);
    }
    if let Some(mode) = latest_viewport_mode(&request.actions) {
        state.public.viewport_mode = mode;
    }
    Ok(Json(json!({ "ok": true, "results": output })))
}

async fn stream_session(
    CurrentUser(user): CurrentUser,
    Path(id): Path<String>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> ApiResult<impl IntoResponse> {
    let record = owned_session(&id, user.user_id.as_deref()).await?;
    let header_lease = headers
        .get(CONTROL_LEASE_HEADER)
        .map(|value| {
            value.to_str().map(str::trim).map_err(|_| {
                api_error(
                    StatusCode::BAD_REQUEST,
                    "Invalid Browser control lease header",
                )
            })
        })
        .transpose()?;
    if header_lease.is_some()
        && query.lease_token.is_some()
        && header_lease != query.lease_token.as_deref()
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "Conflicting Browser control lease credentials",
        ));
    }
    let supplied_lease = header_lease.or(query.lease_token.as_deref());
    let control_token = match query.capability {
        PresenceCapability::Viewer => {
            claim_viewer(&record, query.client_id.as_deref(), user.user_id.as_deref()).await?;
            None
        }
        PresenceCapability::Controller => {
            let lease_token = require_control_token(supplied_lease)?;
            authorize_control(&record, lease_token, query.client_id.as_deref()).await?;
            Some(lease_token.to_string())
        }
    };
    let stream_port =
        record.state.read().await.stream_port.ok_or_else(|| {
            api_error(StatusCode::CONFLICT, "Browser live stream is not available")
        })?;
    Ok(upgrade.on_upgrade(move |socket| proxy_stream(socket, stream_port, record, control_token)))
}

async fn proxy_stream(
    client: WebSocket,
    stream_port: u16,
    record: Arc<SessionRecord>,
    control_token: Option<String>,
) {
    let upstream_url = format!("ws://127.0.0.1:{stream_port}/?pacing=ack&maxFps=30");
    let Ok((upstream, _)) = connect_async(&upstream_url).await else {
        tracing::warn!(
            port = stream_port,
            "Browser stream proxy could not reach runtime"
        );
        return;
    };
    let (mut client_tx, mut client_rx) = client.split();
    let (mut upstream_tx, mut upstream_rx) = upstream.split();

    loop {
        tokio::select! {
            from_runtime = upstream_rx.next() => {
                let Some(Ok(message)) = from_runtime else { break };
                let mapped = match message {
                    UpstreamMessage::Text(value) => AxumMessage::Text(value),
                    UpstreamMessage::Binary(value) => AxumMessage::Binary(value),
                    UpstreamMessage::Ping(value) => AxumMessage::Ping(value),
                    UpstreamMessage::Pong(value) => AxumMessage::Pong(value),
                    UpstreamMessage::Close(_) => AxumMessage::Close(None),
                    UpstreamMessage::Frame(_) => continue,
                };
                if client_tx.send(mapped).await.is_err() { break; }
            }
            from_client = client_rx.next() => {
                let Some(Ok(message)) = from_client else { break };
                let mapped = match message {
                    AxumMessage::Text(value) if stream_text_input_allowed(&record, control_token.as_deref(), &value).await => UpstreamMessage::Text(value),
                    AxumMessage::Binary(value) if stream_binary_input_allowed(&record, control_token.as_deref()).await => UpstreamMessage::Binary(value),
                    AxumMessage::Ping(value) => UpstreamMessage::Ping(value),
                    AxumMessage::Pong(value) => UpstreamMessage::Pong(value),
                    AxumMessage::Close(_) => UpstreamMessage::Close(None),
                    _ => continue,
                };
                if upstream_tx.send(mapped).await.is_err() { break; }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamInputClass {
    Protocol,
    BrowserMutation,
}

fn classify_stream_text_input(text: &str) -> Option<StreamInputClass> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return None;
    };
    match value.get("type").and_then(Value::as_str) {
        Some("config" | "ack") => Some(StreamInputClass::Protocol),
        Some("input_mouse" | "input_keyboard" | "input_touch") => {
            Some(StreamInputClass::BrowserMutation)
        }
        _ => None,
    }
}

async fn stream_text_input_allowed(
    record: &SessionRecord,
    control_token: Option<&str>,
    text: &str,
) -> bool {
    match classify_stream_text_input(text) {
        Some(StreamInputClass::Protocol) => true,
        Some(StreamInputClass::BrowserMutation) => {
            let Some(control_token) = control_token else {
                return false;
            };
            authorize_control(record, control_token, None).await.is_ok()
        }
        None => false,
    }
}

async fn stream_binary_input_allowed(record: &SessionRecord, control_token: Option<&str>) -> bool {
    let Some(control_token) = control_token else {
        return false;
    };
    authorize_control(record, control_token, None).await.is_ok()
}

#[cfg(test)]
async fn insert_bounded_session(id: &str, record: Arc<SessionRecord>) -> ApiResult<()> {
    match insert_bounded_session_with_limit(id, record, MAX_SESSIONS_PER_OWNER).await? {
        InsertSessionOutcome::Inserted | InsertSessionOutcome::Existing(_) => Ok(()),
    }
}

async fn insert_bounded_session_with_limit(
    id: &str,
    record: Arc<SessionRecord>,
    active_limit: usize,
) -> ApiResult<InsertSessionOutcome> {
    let mut sessions = runtime().sessions.write().await;
    if let Some(existing) = sessions.get(id).cloned() {
        return Ok(InsertSessionOutcome::Existing(existing));
    }
    let owned_count = sessions
        .values()
        .filter(|candidate| {
            candidate.owner == record.owner && candidate.reserves_capacity.load(Ordering::Acquire)
        })
        .count();
    if owned_count >= active_limit {
        return Err(api_error(
            StatusCode::CONFLICT,
            "Browser tab limit reached; close an existing tab first",
        ));
    }
    sessions.insert(id.to_string(), record);
    Ok(InsertSessionOutcome::Inserted)
}

async fn prune_finished_owned(owner: &OwnerKey) {
    let candidates = runtime()
        .sessions
        .read()
        .await
        .iter()
        .filter(|(_, record)| &record.owner == owner)
        .map(|(id, record)| (id.clone(), Arc::clone(record)))
        .collect::<Vec<_>>();
    let mut finished = Vec::new();
    for (id, record) in candidates {
        if matches!(
            record.state.read().await.public.status,
            BrowserSessionStatus::Stopped | BrowserSessionStatus::Error
        ) {
            finished.push((id, record));
        }
    }
    let mut sessions = runtime().sessions.write().await;
    for (id, record) in finished {
        if sessions
            .get(&id)
            .is_some_and(|current| Arc::ptr_eq(current, &record))
        {
            sessions.remove(&id);
        }
    }
}

async fn owned_session(id: &str, user_id: Option<&str>) -> ApiResult<Arc<SessionRecord>> {
    let record = runtime()
        .sessions
        .read()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "Browser tab not found"))?;
    if record.owner != OwnerKey::from_user_id(user_id) {
        return Err(api_error(StatusCode::NOT_FOUND, "Browser tab not found"));
    }
    Ok(record)
}

/// Validate a canvas Browser node against the authoritative live session.
/// Wrong-owner identifiers collapse to `NotFound`, and the frozen canonical
/// project directory must exactly match the canvas project.
pub(crate) async fn validate_browser_canvas_binding(
    user: &AuthenticatedUser,
    browser_session_id: &str,
    canonical_project_dir: &FsPath,
) -> Result<(), AppError> {
    let record = runtime()
        .sessions
        .read()
        .await
        .get(browser_session_id)
        .cloned()
        .ok_or_else(browser_binding_not_found)?;
    if record.owner != OwnerKey::from_user_id(user.user_id.as_deref()) {
        return Err(browser_binding_not_found());
    }
    let project_dir = record.state.read().await.public.project_dir.clone();
    if FsPath::new(&project_dir) != canonical_project_dir {
        return Err(AppError::BadRequest(
            "Browser session belongs to a different canvas project".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn insert_canvas_test_browser(
    browser_id: &str,
    user_id: Option<&str>,
    canonical_project_dir: &FsPath,
) -> Result<(), AppError> {
    insert_canvas_test_browser_with_runtime(browser_id, user_id, canonical_project_dir, None).await
}

#[cfg(test)]
pub(crate) async fn insert_canvas_test_browser_with_runtime(
    browser_id: &str,
    user_id: Option<&str>,
    canonical_project_dir: &FsPath,
    runtime_executable_override: Option<PathBuf>,
) -> Result<(), AppError> {
    let canonical_project_dir = canonical_project_dir.canonicalize().map_err(|error| {
        AppError::NotFound(format!(
            "Canvas Browser test project is unavailable: {error}"
        ))
    })?;
    let now = chrono::Utc::now().to_rfc3339();
    let owner = OwnerKey::from_user_id(user_id);
    let project_dir = canonical_project_dir.to_string_lossy().into_owned();
    let record = Arc::new(SessionRecord {
        owner: owner.clone(),
        session_name: format!("canvas-test-{}", browser_id.replace('-', "")),
        creation_complete: AtomicBool::new(true),
        creation_notify: Notify::new(),
        creation_spec: BrowserCreationSpec {
            owner: owner.clone(),
            title: "Browser".to_string(),
            project_dir: project_dir.clone(),
            kind: BrowserSessionKind::Interactive,
            initial_url: "about:blank".to_string(),
        },
        reserves_capacity: AtomicBool::new(true),
        runtime_executable_override,
        state: RwLock::new(SessionState {
            public: BrowserSession {
                id: browser_id.to_string(),
                title: "Browser".to_string(),
                project_dir,
                kind: BrowserSessionKind::Interactive,
                status: BrowserSessionStatus::Ready,
                url: Some("about:blank".to_string()),
                cdp_url: None,
                debug_port: None,
                stream_url: None,
                viewers: 0,
                controllers: 0,
                controller: None,
                last_error: None,
                created_at: now.clone(),
                updated_at: now,
                viewport_mode: ViewportMode::Desktop,
            },
            termination: RuntimeTerminationState::MayBeLive,
            stream_port: None,
            viewers: HashMap::new(),
            controller: None,
        }),
        command_lock: Mutex::new(()),
    });
    match insert_bounded_session_with_limit(browser_id, record, MAX_CANVAS_SESSIONS_PER_OWNER)
        .await
        .map_err(api_to_app_error)?
    {
        InsertSessionOutcome::Inserted => Ok(()),
        InsertSessionOutcome::Existing(existing) => wait_and_reconcile_browser_create(
            &existing,
            &owner,
            "Browser",
            &canonical_project_dir.to_string_lossy(),
        )
        .await
        .map(|_| ())
        .map_err(api_to_app_error),
    }
}

#[cfg(test)]
pub(crate) async fn remove_canvas_test_browser(browser_id: &str, user_id: Option<&str>) {
    let owner = OwnerKey::from_user_id(user_id);
    let mut sessions = runtime().sessions.write().await;
    if sessions
        .get(browser_id)
        .is_some_and(|record| record.owner == owner)
    {
        sessions.remove(browser_id);
    }
}

#[cfg(test)]
pub(crate) async fn canvas_test_active_browser_count(user_id: Option<&str>) -> usize {
    let owner = OwnerKey::from_user_id(user_id);
    runtime()
        .sessions
        .read()
        .await
        .values()
        .filter(|record| record.owner == owner && record.reserves_capacity.load(Ordering::Acquire))
        .count()
}

#[cfg(test)]
pub(crate) async fn mark_canvas_test_browser_confirmed_stopped(
    browser_id: &str,
    user_id: Option<&str>,
) -> Result<(), AppError> {
    let record = owned_session(browser_id, user_id)
        .await
        .map_err(api_to_app_error)?;
    let mut state = record.state.write().await;
    state.termination = RuntimeTerminationState::ConfirmedStopped;
    state.public.status = BrowserSessionStatus::Stopped;
    record.reserves_capacity.store(false, Ordering::Release);
    Ok(())
}

fn browser_binding_not_found() -> AppError {
    AppError::NotFound("Browser session not found".to_string())
}

async fn public_session(record: &SessionRecord) -> BrowserSession {
    let now = Instant::now();
    let mut state = record.state.write().await;
    state
        .viewers
        .retain(|_, seen| now.duration_since(*seen) <= PRESENCE_TTL);
    prune_expired_control(&mut state);
    state.public.viewers = state.viewers.len();
    state.public.controllers = usize::from(state.controller.is_some());
    state.public.controller = state
        .controller
        .as_ref()
        .map(|control| control_summary(control, CONTROL_LEASE_TTL));
    state.public.clone()
}

async fn claim_viewer(
    record: &SessionRecord,
    client_id: Option<&str>,
    user_id: Option<&str>,
) -> ApiResult<()> {
    let fallback = OwnerKey::from_user_id(user_id);
    let client_id = bounded_text(client_id.unwrap_or(fallback.fallback_client_id()), 128)?;
    let now = Instant::now();
    let mut state = record.state.write().await;
    state
        .viewers
        .retain(|_, seen| now.duration_since(*seen) <= PRESENCE_TTL);
    state.viewers.insert(client_id, now);
    Ok(())
}

async fn issue_control_lease(
    record: &SessionRecord,
    client_id: &str,
) -> ApiResult<BrowserControlLease> {
    let client_id = bounded_text(client_id, 128)?;
    if client_id.trim().is_empty() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "Browser controller client_id must contain 1 to 128 bytes",
        ));
    }
    let mut state = record.state.write().await;
    let control = ControlLeaseState {
        client_id,
        lease_token: Uuid::new_v4().to_string(),
        last_seen: Instant::now(),
    };
    let summary = control_summary(&control, CONTROL_LEASE_TTL);
    let lease = BrowserControlLease {
        resource_id: state.public.id.clone(),
        client_id: control.client_id.clone(),
        lease_token: control.lease_token.clone(),
        expires_at: summary.expires_at.clone(),
    };
    state.controller = Some(control);
    state.public.controllers = 1;
    state.public.controller = Some(summary);
    state.public.updated_at = chrono::Utc::now().to_rfc3339();
    Ok(lease)
}

async fn authorize_control(
    record: &SessionRecord,
    lease_token: &str,
    client_id: Option<&str>,
) -> ApiResult<()> {
    let mut state = record.state.write().await;
    prune_expired_control(&mut state);
    let Some(control) = state.controller.as_mut() else {
        return Err(api_error(
            StatusCode::CONFLICT,
            "Browser has no active control lease; take control before mutating it",
        ));
    };
    if !constant_time_token_eq(&control.lease_token, lease_token) {
        return Err(api_error(
            StatusCode::CONFLICT,
            "Browser control lease is held by another client",
        ));
    }
    if let Some(client_id) = client_id {
        if control.client_id != client_id {
            return Err(api_error(
                StatusCode::CONFLICT,
                "Browser control lease client_id does not match this client",
            ));
        }
    }
    control.last_seen = Instant::now();
    let summary = control_summary(control, CONTROL_LEASE_TTL);
    state.public.controllers = 1;
    state.public.controller = Some(summary);
    state.public.updated_at = chrono::Utc::now().to_rfc3339();
    Ok(())
}

fn require_control_token(token: Option<&str>) -> ApiResult<&str> {
    token.filter(|value| !value.is_empty()).ok_or_else(|| {
        api_error(
            StatusCode::CONFLICT,
            "Browser control lease is required; take control before mutating it",
        )
    })
}

fn prune_expired_control(state: &mut SessionState) -> bool {
    let expired = state
        .controller
        .as_ref()
        .is_some_and(|control| control.last_seen.elapsed() >= CONTROL_LEASE_TTL);
    if expired {
        state.controller = None;
        state.public.controllers = 0;
        state.public.controller = None;
        state.public.updated_at = chrono::Utc::now().to_rfc3339();
    }
    expired
}

fn control_summary(control: &ControlLeaseState, ttl: Duration) -> BrowserControlLeaseSummary {
    let remaining = ttl.saturating_sub(control.last_seen.elapsed());
    let expires_at = chrono::Utc::now()
        + chrono::Duration::from_std(remaining).unwrap_or_else(|_| chrono::Duration::zero());
    BrowserControlLeaseSummary {
        client_id: control.client_id.clone(),
        expires_at: expires_at.to_rfc3339(),
    }
}

fn constant_time_token_eq(expected: &str, candidate: &str) -> bool {
    if expected.len() != candidate.len() {
        return false;
    }
    expected
        .bytes()
        .zip(candidate.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

async fn ensure_runnable(record: &SessionRecord) -> ApiResult<()> {
    let status = record.state.read().await.public.status;
    if matches!(
        status,
        BrowserSessionStatus::Stopped | BrowserSessionStatus::Error
    ) {
        Err(api_error(
            StatusCode::CONFLICT,
            "Browser tab is not running",
        ))
    } else {
        Ok(())
    }
}

async fn run_cli(
    record: &SessionRecord,
    args: Vec<String>,
    stdin: Option<Vec<u8>>,
    deadline: Duration,
) -> Result<Value, String> {
    let _guard = record.command_lock.lock().await;
    let executable = record
        .runtime_executable_override
        .clone()
        .or_else(discover_agent_browser)
        .ok_or_else(|| "agent-browser runtime not found".to_string())?;
    let mut command = Command::new(executable);
    command
        .arg("--namespace")
        .arg("mitsuro-atlas")
        .arg("--session")
        .arg(&record.session_name)
        .arg("--json")
        .arg("--content-boundaries")
        .arg("--max-output")
        .arg("200000")
        .arg("--idle-timeout")
        .arg("30m")
        .arg("--confirm-actions")
        .arg("eval,download")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("could not start Browser runtime: {error}"))?;
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .ok_or_else(|| "Browser runtime stdin unavailable".to_string())?
            .write_all(&input)
            .await
            .map_err(|error| format!("could not write Browser command batch: {error}"))?;
    }
    let output = timeout(deadline, child.wait_with_output())
        .await
        .map_err(|_| "Browser runtime command timed out".to_string())?
        .map_err(|error| format!("Browser runtime failed: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let parsed =
        serde_json::from_str::<Value>(&stdout).unwrap_or_else(|_| json!({ "text": stdout }));
    if !output.status.success() || parsed.get("success") == Some(&Value::Bool(false)) {
        let message = parsed
            .get("error")
            .and_then(Value::as_str)
            .or_else(|| (!stderr.is_empty()).then_some(stderr.as_str()))
            .unwrap_or("Browser runtime command failed");
        return Err(message.to_string());
    }
    Ok(parsed)
}

fn action_command(action: &BrowserAction) -> ApiResult<Vec<String>> {
    let command = match action {
        BrowserAction::Navigate { url } => vec!["open".into(), normalize_url(url)?],
        BrowserAction::Snapshot {
            interactive,
            compact,
            depth,
        } => {
            let mut args = vec!["snapshot".into()];
            if *interactive {
                args.push("-i".into());
            }
            if *compact {
                args.push("-c".into());
            }
            if let Some(depth) = depth {
                args.extend(["-d".into(), depth.min(&64).to_string()]);
            }
            args
        }
        BrowserAction::Click { target } => vec!["click".into(), safe_target(target)?],
        BrowserAction::Fill { target, value } => vec![
            "fill".into(),
            safe_target(target)?,
            bounded_text(value, MAX_TEXT_BYTES)?,
        ],
        BrowserAction::Type { target, value } => vec![
            "type".into(),
            safe_target(target)?,
            bounded_text(value, MAX_TEXT_BYTES)?,
        ],
        BrowserAction::Press { key } => vec!["press".into(), safe_argument(key, 100)?],
        BrowserAction::Hover { target } => vec!["hover".into(), safe_target(target)?],
        BrowserAction::Select { target, values } => {
            if values.is_empty() || values.len() > 32 {
                return Err(api_error(
                    StatusCode::BAD_REQUEST,
                    "select requires 1 to 32 values",
                ));
            }
            let mut args = vec!["select".into(), safe_target(target)?];
            for value in values {
                args.push(safe_argument(value, 1024)?);
            }
            args
        }
        BrowserAction::Scroll { direction, amount } => {
            let direction = match direction {
                ScrollDirection::Up => "up",
                ScrollDirection::Down => "down",
                ScrollDirection::Left => "left",
                ScrollDirection::Right => "right",
            };
            let mut args = vec!["scroll".into(), direction.into()];
            if let Some(amount) = amount {
                args.push(amount.min(&100_000).to_string());
            }
            args
        }
        BrowserAction::Back => vec!["back".into()],
        BrowserAction::Forward => vec!["forward".into()],
        BrowserAction::Reload => vec!["reload".into()],
        BrowserAction::Wait { ms } => vec!["wait".into(), ms.min(&30_000).to_string()],
        BrowserAction::Get { property, target } => {
            let page_property = matches!(property, GetProperty::Title | GetProperty::Url);
            if !page_property && target.is_none() {
                return Err(api_error(
                    StatusCode::BAD_REQUEST,
                    "element queries require a target",
                ));
            }
            let property = match property {
                GetProperty::Text => "text",
                GetProperty::Html => "html",
                GetProperty::Value => "value",
                GetProperty::Title => "title",
                GetProperty::Url => "url",
                GetProperty::Count => "count",
            };
            let mut args = vec!["get".into(), property.into()];
            if let Some(target) = target {
                args.push(safe_target(target)?);
            }
            args
        }
        BrowserAction::Attribute { target, name } => vec![
            "get".into(),
            "attr".into(),
            safe_target(target)?,
            safe_argument(name, 256)?,
        ],
        BrowserAction::Viewport { mode } => match mode {
            ViewportMode::Mobile => {
                vec!["set".into(), "device".into(), "iPhone 12".into()]
            }
            ViewportMode::Desktop => {
                vec!["set".into(), "viewport".into(), "1440".into(), "900".into()]
            }
        },
    };
    Ok(command)
}

fn action_commands(actions: &[BrowserAction]) -> ApiResult<Vec<Vec<String>>> {
    let mut commands = Vec::with_capacity(actions.len() + 1);
    for action in actions {
        commands.push(action_command(action)?);
        if matches!(
            action,
            BrowserAction::Viewport {
                mode: ViewportMode::Mobile
            }
        ) {
            commands.push(vec![
                "set".into(),
                "viewport".into(),
                "390".into(),
                "700".into(),
                "3".into(),
            ]);
        }
    }
    Ok(commands)
}

fn latest_viewport_mode(actions: &[BrowserAction]) -> Option<ViewportMode> {
    actions.iter().rev().find_map(|action| match action {
        BrowserAction::Viewport { mode } => Some(*mode),
        _ => None,
    })
}

fn browser_capability() -> BrowserCapability {
    let executable = discover_agent_browser();
    BrowserCapability {
        available: executable.is_some(),
        runtime: "agent-browser",
        version: "0.34.0",
        executable: executable.as_ref().map(|path| path.display().to_string()),
        live_stream: executable.is_some(),
        semantic_actions: executable.is_some(),
        agent_chat: executable.is_some(),
        control_leases: true,
        control_lease_version: 1,
        control_lease_ttl_seconds: CONTROL_LEASE_TTL.as_secs(),
        reason: executable.is_none().then(missing_runtime_message),
    }
}

fn missing_runtime_message() -> String {
    "Browser runtime is not installed. Honey: the linux archive ships agent-browser beside mitsuro; run scripts/honey-atlas-repair.sh or set MITSURO_AGENT_BROWSER_PATH. Source checkouts: sh scripts/install-atlas-runtime.sh".to_string()
}

fn discover_agent_browser() -> Option<PathBuf> {
    first_existing_file(agent_browser_candidates())
}

fn agent_browser_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path) = env::var_os("MITSURO_AGENT_BROWSER_PATH").map(PathBuf::from) {
        candidates.push(path);
    }
    if let Ok(current) = env::current_exe() {
        candidates.extend(sibling_agent_browser_paths(&current));
    }
    if let Some(home) = env::var_os("HOME").map(PathBuf::from) {
        candidates.extend(managed_install_agent_browser_paths(&home));
    }
    let workspace = FsPath::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    candidates.push(workspace.join("target/atlas").join(binary_name()));
    candidates.push(
        workspace
            .join("tools/atlas/node_modules/agent-browser/bin")
            .join(package_binary_name()),
    );
    if let Some(paths) = env::var_os("PATH") {
        candidates.extend(env::split_paths(&paths).map(|path| path.join(binary_name())));
    }
    candidates
}

fn sibling_agent_browser_paths(current_exe: &FsPath) -> Vec<PathBuf> {
    let directory = current_exe.parent().unwrap_or_else(|| FsPath::new("."));
    vec![
        directory.join(binary_name()),
        directory.join("libexec/mitsuro").join(binary_name()),
    ]
}

fn managed_install_agent_browser_paths(home: &FsPath) -> Vec<PathBuf> {
    vec![
        home.join(".local/bin/.mitsuro-current").join(binary_name()),
        home.join(".local/bin").join(binary_name()),
        home.join(".local/lib/mitsuro").join(binary_name()),
    ]
}

fn first_existing_file(paths: impl IntoIterator<Item = PathBuf>) -> Option<PathBuf> {
    paths.into_iter().find(|path| path.is_file())
}

fn binary_name() -> &'static str {
    if cfg!(windows) {
        "agent-browser.exe"
    } else {
        "agent-browser"
    }
}

fn package_binary_name() -> &'static str {
    if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        "agent-browser-linux-x64"
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        "agent-browser-linux-arm64"
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        "agent-browser-darwin-x64"
    } else if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        "agent-browser-darwin-arm64"
    } else if cfg!(windows) {
        "agent-browser-win32-x64.exe"
    } else {
        "agent-browser"
    }
}

fn normalize_url(input: &str) -> ApiResult<String> {
    let input = bounded_text(input.trim(), 8 * 1024)?;
    if input == "about:blank" || input.starts_with("http://") || input.starts_with("https://") {
        Ok(input)
    } else {
        Err(api_error(
            StatusCode::BAD_REQUEST,
            "Browser URLs must use http or https",
        ))
    }
}

fn canonical_browser_project_dir(
    state: &AppState,
    user: &AuthenticatedUser,
    requested: Option<&str>,
) -> Result<String, AppError> {
    let base_dir = workspace_base(user.home_dir.as_deref(), state.working_dir.as_ref());
    let allowed_root = allowed_root(user.home_dir.as_deref(), &base_dir);
    canonicalize_browser_project_dir(requested, &base_dir, &allowed_root)
}

fn canonicalize_browser_project_dir(
    requested: Option<&str>,
    base_dir: &FsPath,
    allowed_root: &FsPath,
) -> Result<String, AppError> {
    let resolved = resolve_scoped_workspace_path(requested, base_dir, allowed_root)?;
    let canonical = resolved.canonicalize().map_err(|_| {
        AppError::NotFound(format!(
            "Browser project directory not found: {}",
            resolved.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(AppError::BadRequest(format!(
            "Browser project path is not a directory: {}",
            canonical.display()
        )));
    }
    let canonical_root = allowed_root.canonicalize().map_err(|error| {
        AppError::Internal(format!(
            "Failed to canonicalize Browser workspace root: {error}"
        ))
    })?;
    if !canonical.starts_with(canonical_root) {
        return Err(AppError::BadRequest(
            "Browser project directory must be within the allowed workspace root".to_string(),
        ));
    }
    Ok(canonical.to_string_lossy().into_owned())
}

fn safe_target(value: &str) -> ApiResult<String> {
    safe_argument(value, 4 * 1024)
}

fn safe_argument(value: &str, max: usize) -> ApiResult<String> {
    let value = bounded_text(value, max)?;
    if value.starts_with('-') {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "Browser arguments cannot begin with a flag",
        ));
    }
    Ok(value)
}

fn bounded_text(value: &str, max: usize) -> ApiResult<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > max || value.contains('\0') {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "Browser input is empty or too large",
        ));
    }
    Ok(value.to_string())
}

fn find_stream_port(value: &Value) -> Option<u16> {
    value
        .pointer("/data/port")
        .or_else(|| value.get("port"))
        .and_then(Value::as_u64)
        .and_then(|port| u16::try_from(port).ok())
}

fn latest_url(value: &Value) -> Option<String> {
    value
        .as_array()?
        .iter()
        .rev()
        .find_map(|item| item.pointer("/data/url").and_then(Value::as_str))
        .map(str::to_string)
}

fn api_error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message.into() })))
}

fn app_error_to_api(error: AppError) -> (StatusCode, Json<Value>) {
    match error {
        AppError::NotFound(message) => api_error(StatusCode::NOT_FOUND, message),
        AppError::BadRequest(message) => api_error(StatusCode::BAD_REQUEST, message),
        AppError::Conflict(message) => api_error(StatusCode::CONFLICT, message),
        AppError::ServiceUnavailable(message) => {
            api_error(StatusCode::SERVICE_UNAVAILABLE, message)
        }
        AppError::BadGateway(message) => api_error(StatusCode::BAD_GATEWAY, message),
        AppError::Internal(message) => api_error(StatusCode::INTERNAL_SERVER_ERROR, message),
    }
}

fn runtime_error(message: String) -> (StatusCode, Json<Value>) {
    api_error(StatusCode::BAD_GATEWAY, message)
}

fn internal_error(error: serde_json::Error) -> (StatusCode, Json<Value>) {
    tracing::error!(%error, "Could not encode Browser command batch");
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Could not encode Browser command batch",
    )
}

fn api_to_app_error(error: (StatusCode, Json<Value>)) -> AppError {
    let (status, Json(value)) = error;
    let message = value
        .get("error")
        .and_then(|value| value.as_str())
        .unwrap_or("Browser request failed")
        .to_string();
    match status {
        StatusCode::NOT_FOUND => AppError::NotFound(message),
        StatusCode::BAD_REQUEST => AppError::BadRequest(message),
        StatusCode::CONFLICT => AppError::Conflict(message),
        StatusCode::SERVICE_UNAVAILABLE => AppError::ServiceUnavailable(message),
        StatusCode::BAD_GATEWAY => AppError::BadGateway(message),
        _ => AppError::Internal(message),
    }
}

pub(crate) async fn ensure_browser_for_canvas(
    user: &AuthenticatedUser,
    browser_id: &str,
    canonical_project_dir: &FsPath,
    title: &str,
    url: Option<&str>,
) -> Result<Value, AppError> {
    if Uuid::parse_str(browser_id).is_err() {
        return Err(AppError::BadRequest(
            "Browser resource id must be a UUID".to_string(),
        ));
    }
    let canonical_project_dir = canonical_project_dir.canonicalize().map_err(|_| {
        AppError::NotFound("Canvas project is unavailable for Browser creation".to_string())
    })?;
    let title = bounded_text(title, 120).map_err(api_to_app_error)?;
    let url = normalize_url(url.unwrap_or("about:blank")).map_err(api_to_app_error)?;
    let session = create_browser_session_with_id(
        browser_id.to_string(),
        OwnerKey::from_user_id(user.user_id.as_deref()),
        title,
        canonical_project_dir.to_string_lossy().into_owned(),
        BrowserSessionKind::Interactive,
        url,
        MAX_CANVAS_SESSIONS_PER_OWNER,
    )
    .await
    .map_err(api_to_app_error)?;
    serde_json::to_value(session)
        .map_err(|error| AppError::Internal(format!("Could not encode Browser session: {error}")))
}

pub(crate) async fn cleanup_browser_for_canvas(
    user: &AuthenticatedUser,
    browser_id: &str,
    canonical_project_dir: &FsPath,
    _title: &str,
    _url: Option<&str>,
    absence_is_authoritative: bool,
) -> Result<(), AppError> {
    match owned_session(browser_id, user.user_id.as_deref()).await {
        Ok(record) => {
            let project_dir = record.state.read().await.public.project_dir.clone();
            if FsPath::new(&project_dir) != canonical_project_dir {
                return Err(AppError::Conflict(
                    "Browser cleanup request does not match the exact Canvas resource spec"
                        .to_string(),
                ));
            }
            if record.state.read().await.termination == RuntimeTerminationState::ConfirmedStopped
            {
                record.reserves_capacity.store(false, Ordering::Release);
                runtime().sessions.write().await.remove(browser_id);
                return Ok(());
            }
            match stop_owned_record(&record).await {
                Ok(()) => {
                    let mut state = record.state.write().await;
                    state.termination = RuntimeTerminationState::ConfirmedStopped;
                    record.reserves_capacity.store(false, Ordering::Release);
                    drop(state);
                    runtime().sessions.write().await.remove(browser_id);
                    Ok(())
                }
                Err(error) => {
                    let mut state = record.state.write().await;
                    state.termination = RuntimeTerminationState::MayBeLive;
                    state.public.status = BrowserSessionStatus::Error;
                    state.public.last_error = Some(
                        error
                            .1
                            .0
                            .get("error")
                            .and_then(|value| value.as_str())
                            .unwrap_or("Browser termination cannot be confirmed")
                            .to_string(),
                    );
                    Err(AppError::Conflict(
                        "Browser termination cannot be confirmed after the prior server lifecycle; the cleanup intent remains ambiguous"
                            .to_string(),
                    ))
                }
            }
        }
        Err(_) if absence_is_authoritative => Ok(()),
        Err(_) => Err(AppError::Conflict(
            "Browser termination cannot be confirmed after the prior server lifecycle; the cleanup intent remains ambiguous"
                .to_string(),
        )),
    }
}

pub(crate) async fn classify_browser_cleanup_for_canvas(
    user: &AuthenticatedUser,
    browser_id: &str,
    canonical_project_dir: &FsPath,
    _title: &str,
    _url: Option<&str>,
    _lease_token: Option<&str>,
    absence_is_authoritative: bool,
) -> Result<super::canvas_resources::BoundCleanupProof, AppError> {
    match owned_session(browser_id, user.user_id.as_deref()).await {
        Ok(record) => {
            let project_dir = record.state.read().await.public.project_dir.clone();
            if FsPath::new(&project_dir) != canonical_project_dir {
                return Err(AppError::Conflict(
                    "Browser cleanup request does not match the exact Canvas resource spec"
                        .to_string(),
                ));
            }
            Ok(super::canvas_resources::BoundCleanupProof::LiveLease)
        }
        Err(_) if absence_is_authoritative => {
            Ok(super::canvas_resources::BoundCleanupProof::AuthoritativelyEnded)
        }
        Err(error) => Err(api_to_app_error(error)),
    }
}

async fn stop_owned_record(record: &Arc<SessionRecord>) -> ApiResult<()> {
    let confirmed_stopped =
        record.state.read().await.termination == RuntimeTerminationState::ConfirmedStopped;
    if !confirmed_stopped {
        run_cli(record, vec!["close".into()], None, COMMAND_TIMEOUT)
            .await
            .map_err(|error| api_error(StatusCode::CONFLICT, error))?;
    }
    let mut state = record.state.write().await;
    state.public.status = BrowserSessionStatus::Stopped;
    state.public.updated_at = chrono::Utc::now().to_rfc3339();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_browser_record(
        id: &str,
        owner: OwnerKey,
        canonical_project_dir: &FsPath,
    ) -> Arc<SessionRecord> {
        let now = chrono::Utc::now().to_rfc3339();
        Arc::new(SessionRecord {
            owner: owner.clone(),
            session_name: format!("test-{id}"),
            creation_complete: AtomicBool::new(true),
            creation_notify: Notify::new(),
            creation_spec: BrowserCreationSpec {
                owner,
                title: "Browser".to_string(),
                project_dir: canonical_project_dir.to_string_lossy().into_owned(),
                kind: BrowserSessionKind::Interactive,
                initial_url: "about:blank".to_string(),
            },
            reserves_capacity: AtomicBool::new(true),
            runtime_executable_override: None,
            state: RwLock::new(SessionState {
                public: BrowserSession {
                    id: id.to_string(),
                    title: "Browser".to_string(),
                    project_dir: canonical_project_dir.to_string_lossy().into_owned(),
                    kind: BrowserSessionKind::Interactive,
                    status: BrowserSessionStatus::Ready,
                    url: Some("about:blank".to_string()),
                    cdp_url: None,
                    debug_port: None,
                    stream_url: None,
                    viewers: 0,
                    controllers: 0,
                    controller: None,
                    last_error: None,
                    created_at: now.clone(),
                    updated_at: now,
                    viewport_mode: ViewportMode::Desktop,
                },
                termination: RuntimeTerminationState::MayBeLive,
                stream_port: None,
                viewers: HashMap::new(),
                controller: None,
            }),
            command_lock: Mutex::new(()),
        })
    }

    #[test]
    fn browser_owner_key_keeps_local_null_distinct_from_literal_local_user() {
        assert_ne!(
            OwnerKey::from_user_id(None),
            OwnerKey::from_user_id(Some("local"))
        );
    }

    #[test]
    fn browser_project_scope_rejects_symlink_escape() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), root.path().join("escape"))
                .expect("symlink");
            assert!(matches!(
                canonicalize_browser_project_dir(Some("escape"), root.path(), root.path()),
                Err(AppError::BadRequest(_))
            ));
        }
    }

    #[tokio::test]
    async fn canvas_browser_binding_requires_exact_owner_and_project() {
        let root = tempfile::tempdir().expect("root");
        let project = root.path().join("project");
        let other = root.path().join("other");
        std::fs::create_dir_all(&project).expect("project");
        std::fs::create_dir_all(&other).expect("other");
        let project = project.canonicalize().expect("canonical project");
        let other = other.canonicalize().expect("canonical other");
        let id = format!("canvas-browser-{}", Uuid::new_v4());
        runtime().sessions.write().await.insert(
            id.clone(),
            test_browser_record(&id, OwnerKey::from_user_id(None), &project),
        );

        let local = AuthenticatedUser::local();
        validate_browser_canvas_binding(&local, &id, &project)
            .await
            .expect("exact local owner and project");
        assert!(matches!(
            validate_browser_canvas_binding(
                &AuthenticatedUser {
                    user_id: Some("local".to_string()),
                    home_dir: None,
                },
                &id,
                &project,
            )
            .await,
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            validate_browser_canvas_binding(&local, &id, &other).await,
            Err(AppError::BadRequest(_))
        ));

        runtime().sessions.write().await.remove(&id);
    }

    #[test]
    fn rejects_flag_shaped_targets() {
        let action = BrowserAction::Click {
            target: "--eval".into(),
        };
        assert!(action_command(&action).is_err());
    }

    #[test]
    fn semantic_actions_are_argument_arrays() {
        let action = BrowserAction::Fill {
            target: "@e2".into(),
            value: "hello world".into(),
        };
        assert_eq!(
            action_command(&action).unwrap(),
            vec!["fill", "@e2", "hello world"]
        );
    }

    #[test]
    fn viewport_actions_use_real_browser_dimensions() {
        assert_eq!(
            action_command(&BrowserAction::Viewport {
                mode: ViewportMode::Mobile,
            })
            .unwrap(),
            vec!["set", "device", "iPhone 12"]
        );
        assert_eq!(
            action_command(&BrowserAction::Viewport {
                mode: ViewportMode::Desktop,
            })
            .unwrap(),
            vec!["set", "viewport", "1440", "900"]
        );
        assert_eq!(
            action_commands(&[BrowserAction::Viewport {
                mode: ViewportMode::Mobile,
            }])
            .unwrap(),
            vec![
                vec!["set", "device", "iPhone 12"],
                vec!["set", "viewport", "390", "700", "3"],
            ]
        );
    }

    #[test]
    fn navigation_accepts_local_and_public_web_pages() {
        for url in ["http://127.0.0.1:5173", "https://example.com/docs"] {
            let action = BrowserAction::Navigate { url: url.into() };
            assert_eq!(action_command(&action).unwrap(), vec!["open", url]);
        }

        let action = BrowserAction::Navigate {
            url: "file:///etc/passwd".into(),
        };
        assert!(action_command(&action).is_err());
    }

    #[tokio::test]
    async fn observer_actions_are_rejected_and_take_control_transfers_authority() {
        let root = tempfile::tempdir().expect("project");
        let record =
            test_browser_record("lease-transfer", OwnerKey::from_user_id(None), root.path());

        assert_eq!(
            require_control_token(None).unwrap_err().0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            authorize_control(&record, "observer-token", None)
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );

        let first = issue_control_lease(&record, "first-client")
            .await
            .expect("first lease");
        assert_eq!(
            authorize_control(&record, "wrong-token", None)
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
        authorize_control(&record, &first.lease_token, Some("first-client"))
            .await
            .expect("first controller authorizes");

        let second = issue_control_lease(&record, "second-client")
            .await
            .expect("explicit transfer");
        assert_eq!(
            authorize_control(&record, &first.lease_token, Some("first-client"))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
        authorize_control(&record, &second.lease_token, Some("second-client"))
            .await
            .expect("new controller authorizes");
    }

    #[tokio::test]
    async fn observer_cannot_stop_a_browser_session() {
        let root = tempfile::tempdir().expect("project");
        let id = format!("observer-stop-{}", Uuid::new_v4());
        runtime().sessions.write().await.insert(
            id.clone(),
            test_browser_record(&id, OwnerKey::from_user_id(None), root.path()),
        );

        let error = stop_session(
            CurrentUser(AuthenticatedUser::local()),
            Path(id.clone()),
            Json(StopBrowserSessionRequest { lease_token: None }),
        )
        .await
        .expect_err("observer stop must be rejected");
        assert_eq!(error.0, StatusCode::CONFLICT);
        assert!(runtime().sessions.read().await.contains_key(&id));

        runtime().sessions.write().await.remove(&id);
    }

    #[tokio::test]
    async fn exact_owner_can_remove_failed_session_without_a_control_lease() {
        let root = tempfile::tempdir().expect("project");
        let owner_id = format!("failed-owner-{}", Uuid::new_v4());
        let owner = OwnerKey::from_user_id(Some(&owner_id));
        let id = format!("failed-browser-{}", Uuid::new_v4());
        let record = test_browser_record(&id, owner, root.path());
        {
            let mut state = record.state.write().await;
            state.public.status = BrowserSessionStatus::Error;
            state.public.last_error = Some("launch failed".to_string());
        }
        runtime().sessions.write().await.insert(id.clone(), record);

        let response = stop_session(
            CurrentUser(AuthenticatedUser {
                user_id: Some(owner_id),
                home_dir: None,
            }),
            Path(id.clone()),
            Json(StopBrowserSessionRequest { lease_token: None }),
        )
        .await
        .expect("exact owner may clean a failed session without impossible control");
        assert_eq!(response.0.status, BrowserSessionStatus::Stopped);
        assert!(!runtime().sessions.read().await.contains_key(&id));
    }

    #[tokio::test]
    async fn concurrent_browser_reservations_never_exceed_owner_cap() {
        let root = tempfile::tempdir().expect("project");
        let owner_id = format!("capacity-owner-{}", Uuid::new_v4());
        let owner = OwnerKey::from_user_id(Some(&owner_id));
        let candidates = (0..(MAX_SESSIONS_PER_OWNER + 3))
            .map(|index| {
                let id = format!("capacity-{owner_id}-{index}");
                let record = test_browser_record(&id, owner.clone(), root.path());
                async move { (id.clone(), insert_bounded_session(&id, record).await) }
            })
            .collect::<Vec<_>>();
        let results = futures::future::join_all(candidates).await;
        assert_eq!(
            results.iter().filter(|(_, result)| result.is_ok()).count(),
            MAX_SESSIONS_PER_OWNER
        );
        assert_eq!(
            runtime()
                .sessions
                .read()
                .await
                .values()
                .filter(|record| record.owner == owner)
                .count(),
            MAX_SESSIONS_PER_OWNER
        );
        runtime()
            .sessions
            .write()
            .await
            .retain(|_, record| record.owner != owner);
    }

    #[tokio::test]
    async fn expired_control_lease_is_released() {
        assert_eq!(CONTROL_LEASE_TTL, Duration::from_secs(15));
        let root = tempfile::tempdir().expect("project");
        let record = test_browser_record("lease-expiry", OwnerKey::from_user_id(None), root.path());
        let lease = issue_control_lease(&record, "detached-client")
            .await
            .expect("lease");
        {
            let mut state = record.state.write().await;
            state.controller.as_mut().expect("control").last_seen = Instant::now()
                .checked_sub(CONTROL_LEASE_TTL + Duration::from_secs(1))
                .expect("past instant");
        }

        assert_eq!(
            authorize_control(&record, &lease.lease_token, None)
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
        let public = public_session(&record).await;
        assert_eq!(public.controllers, 0);
        assert!(public.controller.is_none());
    }

    #[tokio::test]
    async fn wrong_owner_cannot_resolve_browser_for_control() {
        let root = tempfile::tempdir().expect("project");
        let id = format!("browser-owner-{}", Uuid::new_v4());
        runtime().sessions.write().await.insert(
            id.clone(),
            test_browser_record(&id, OwnerKey::from_user_id(Some("owner-a")), root.path()),
        );

        assert_eq!(
            owned_session(&id, Some("owner-b"))
                .await
                .err()
                .expect("wrong owner must be hidden")
                .0,
            StatusCode::NOT_FOUND
        );
        runtime().sessions.write().await.remove(&id);
    }

    #[tokio::test]
    async fn stream_proxy_rechecks_control_token_for_each_input() {
        let root = tempfile::tempdir().expect("project");
        let record =
            test_browser_record("stream-control", OwnerKey::from_user_id(None), root.path());
        let input = r#"{"type":"input_mouse"}"#;

        assert!(stream_text_input_allowed(&record, None, r#"{"type":"ack","seq":1}"#).await);
        assert!(!stream_text_input_allowed(&record, None, input).await);
        assert!(!stream_binary_input_allowed(&record, None).await);

        let first = issue_control_lease(&record, "first-client")
            .await
            .expect("first lease");
        assert!(stream_text_input_allowed(&record, Some(&first.lease_token), input).await);
        assert!(stream_binary_input_allowed(&record, Some(&first.lease_token)).await);
        assert!(!stream_text_input_allowed(&record, Some("wrong-token"), input).await);
        assert!(
            !stream_text_input_allowed(&record, Some(&first.lease_token), r#"{"type":"eval"}"#)
                .await
        );

        let second = issue_control_lease(&record, "second-client")
            .await
            .expect("transfer");
        assert!(!stream_text_input_allowed(&record, Some(&first.lease_token), input).await);
        assert!(stream_text_input_allowed(&record, Some(&second.lease_token), input).await);
    }

    #[test]
    fn finds_nested_stream_port() {
        assert_eq!(
            find_stream_port(&json!({"data":{"port":43123}})),
            Some(43123)
        );
    }

    #[test]
    fn first_existing_file_skips_missing_candidates() {
        let dir = std::env::temp_dir().join(format!(
            "mitsuro-atlas-discover-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("missing");
        let present = dir.join("agent-browser");
        std::fs::write(&present, b"ok").unwrap();
        assert_eq!(
            first_existing_file([missing, present.clone()]),
            Some(present)
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn managed_install_paths_include_current_release_sidecar() {
        let home = FsPath::new("/home/honey");
        let paths = managed_install_agent_browser_paths(home);
        assert!(paths
            .iter()
            .any(|path| { path.ends_with(".local/bin/.mitsuro-current/agent-browser") }));
        assert!(paths
            .iter()
            .any(|path| path.ends_with(".local/lib/mitsuro/agent-browser")));
    }

    #[test]
    fn sibling_discovery_looks_beside_the_running_binary() {
        let paths = sibling_agent_browser_paths(FsPath::new(
            "/home/honey/.local/bin/.mitsuro-releases/v0.9.23/mitsuro",
        ));
        assert_eq!(
            paths[0],
            PathBuf::from("/home/honey/.local/bin/.mitsuro-releases/v0.9.23/agent-browser")
        );
    }
}
