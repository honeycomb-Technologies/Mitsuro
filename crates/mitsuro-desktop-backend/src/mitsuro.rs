//! Mitsuro HTTP/SSE backend adapter.

mod source_references;

use source_references::SourceReferences;

use async_trait::async_trait;
use base64::Engine as _;
use futures::StreamExt as _;
use mitsuro_client::{
    ChatRequest, ChatStreamEvent, ContentBlock, CreateSessionRequest, ImageSource, MessageResponse,
    MitsuroClient, PermissionMode, SessionType, SteerRequest, UpdateSessionRequest, WorkMode,
};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::account::{
    CancelLoginAccountParams, CancelLoginAccountResponse, ConsumeAccountRateLimitResetCreditParams,
    ConsumeAccountRateLimitResetCreditResponse, GetAccountParams, GetAccountRateLimitsResponse,
    GetAccountResponse, GetAccountTokenUsageResponse, GetWorkspaceMessagesResponse,
    LoginAccountParams, LoginAccountResponse, LogoutAccountResponse,
    SendAddCreditsNudgeEmailParams, SendAddCreditsNudgeEmailResponse,
};
use crate::approvals::{ApprovalChoice, ApprovalKind, PendingApproval};
use crate::backend::AgentBackend;
use crate::environment::{
    CollaborationModeListParams, CollaborationModeListResponse, EnvironmentAddParams,
    EnvironmentAddResponse, EnvironmentInfoParams, EnvironmentInfoResponse,
    EnvironmentStatusParams, EnvironmentStatusResponse,
};
use crate::extensions::{
    ListMcpServerStatusParams, ListMcpServerStatusResponse, McpAuthStatus, McpServerInfo,
    McpServerStatus, McpServerToolCallParams, McpServerToolCallResponse, PluginAuthPolicy,
    PluginAvailability, PluginInstallPolicy, PluginInstalledParams, PluginInstalledResponse,
    PluginInterface, PluginListParams, PluginListResponse, PluginMarketplaceEntry,
    PluginReadParams, PluginReadResponse, PluginSource, PluginSummary,
};
use crate::fs::{
    fuzzy_score_name, FsGetMetadataParams, FsGetMetadataResponse, FsReadDirectoryEntry,
    FsReadDirectoryParams, FsReadDirectoryResponse, FsReadFileParams, FsReadFileResponse,
    FuzzyFileSearchMatchType, FuzzyFileSearchParams, FuzzyFileSearchResponse,
    FuzzyFileSearchResult, FuzzyFileSearchSessionStartParams, FuzzyFileSearchSessionStartResponse,
    FuzzyFileSearchSessionStopParams, FuzzyFileSearchSessionStopResponse,
    FuzzyFileSearchSessionUpdateParams, FuzzyFileSearchSessionUpdateResponse,
};
use crate::live_turn::{LiveApprovalBridge, LiveTurnOutcome};
use crate::process::{
    ProcessKillParams, ProcessKillResponse, ProcessResizePtyParams, ProcessResizePtyResponse,
    ProcessSpawnParams, ProcessSpawnResponse, ProcessWriteStdinParams, ProcessWriteStdinResponse,
};
use crate::protocol::{
    ConfigReadParams, ConfigReadResponse, InitializeResponse, ModelInfo, ModelListParams,
    ModelListResponse, ModelServiceTier, ReasoningEffortOption, SkillMetadata, SkillsListEntry,
    SkillsListParams, SkillsListResponse, ThreadArchiveParams, ThreadArchiveResponse,
    ThreadDeleteParams, ThreadDeleteResponse, ThreadForkParams, ThreadForkResponse,
    ThreadGoalClearParams, ThreadGoalClearResponse, ThreadGoalGetParams, ThreadGoalGetResponse,
    ThreadGoalSetParams, ThreadGoalSetResponse, ThreadListParams, ThreadListResponse,
    ThreadReadParams, ThreadReadResponse, ThreadResumeParams, ThreadResumeResponse,
    ThreadSearchParams, ThreadSearchResponse, ThreadSetNameParams, ThreadSetNameResponse,
    ThreadStartParams, ThreadStartResponse, ThreadUnarchiveParams, ThreadUnarchiveResponse,
    TurnInterruptParams, TurnInterruptResponse, TurnStartParams, TurnStartResponse,
    TurnSteerParams, TurnSteerResponse,
};
use crate::thread_history::{
    list_turns_in_thread, search_occurrences_in_thread, ThreadRollbackParams,
    ThreadRollbackResponse, ThreadSearchOccurrencesParams, ThreadSearchOccurrencesResponse,
    ThreadTurnsListParams, ThreadTurnsListResponse,
};
use crate::types::{
    AgentError, ConnectionStatus, DelegatedProgressProjection, DelegationExecution,
    DelegationGroupProjection, DelegationGroupStatus, DelegationKind,
    DelegationParentContinuationStatus, DelegationRole, DelegationRunStage,
    DelegationTaskProjection, DelegationTaskStatus, DurableDelegationEvent,
    DurableDelegationEventKind, ItemKind, Result, SessionDelegationProjection, TurnStreamEvent,
};

/// Adapter for a local or authenticated remote Mitsuro server.
#[derive(Debug)]
pub struct MitsuroServerBackend {
    client: MitsuroClient,
    status: RwLock<ConnectionStatus>,
    next_turn_id: AtomicU64,
    server_version: RwLock<Option<String>>,
}

impl MitsuroServerBackend {
    pub fn new() -> Self {
        Self::from_url("http://127.0.0.1:3000", None)
            .expect("the built-in Mitsuro loopback URL is valid")
    }

    pub fn from_url(base_url: impl Into<String>, bearer_token: Option<&str>) -> Result<Self> {
        let base_url = base_url.into();
        let client = match bearer_token {
            Some(token) => MitsuroClient::with_bearer_token(base_url, token),
            None => MitsuroClient::new(base_url),
        }
        .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(Self {
            client,
            status: RwLock::new(ConnectionStatus::Disconnected),
            next_turn_id: AtomicU64::new(1),
            server_version: RwLock::new(None),
        })
    }

    pub fn from_env() -> Result<Self> {
        let url = std::env::var("MITSURO_SERVER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned());
        let token = std::env::var("MITSURO_SERVER_TOKEN").ok();
        Self::from_url(url, token.as_deref())
    }

    pub fn client(&self) -> &MitsuroClient {
        &self.client
    }

    pub fn server_version(&self) -> Option<String> {
        self.server_version
            .read()
            .ok()
            .and_then(|version| version.clone())
    }

    fn set_status(&self, status: ConnectionStatus) {
        if let Ok(mut current) = self.status.write() {
            *current = status;
        }
    }

    /// Hydrate the canonical delegation projection used by desktop surfaces on
    /// initial session load and reconnect.
    pub async fn session_delegation_projection(
        &self,
        session_id: &str,
    ) -> Result<SessionDelegationProjection> {
        self.session_delegation_projection_after(session_id, None)
            .await
    }

    /// Hydrate a reconnect delta after a durable event cursor. Passing `None`
    /// requests the server's initial bounded projection.
    pub async fn session_delegation_projection_after(
        &self,
        session_id: &str,
        event_cursor: Option<i64>,
    ) -> Result<SessionDelegationProjection> {
        self.client
            .get_session_state_with_options(
                session_id,
                mitsuro_client::SessionStateOptions {
                    delegation_after_cursor: event_cursor,
                    ..Default::default()
                },
            )
            .await
            .map(session_delegation_projection)
            .map_err(|error| AgentError::Other(error.to_string()))
    }
}

impl Default for MitsuroServerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MitsuroServerBackend {
    pub async fn run_turn_streaming(
        &self,
        params: TurnStartParams,
        event_tx: std::sync::mpsc::Sender<TurnStreamEvent>,
        bridge: Arc<LiveApprovalBridge>,
        overall_timeout: Duration,
    ) -> Result<LiveTurnOutcome> {
        let thread_id = params.thread_id;
        let (text, content) = turn_input_content(&params.input).await?;
        let turn_id = format!(
            "mitsuro-turn-{}",
            self.next_turn_id.fetch_add(1, Ordering::Relaxed)
        );
        let assistant_item_id = format!("{turn_id}-assistant");
        let reasoning_item_id = format!("{turn_id}-reasoning");
        let request = ChatRequest {
            session_id: Some(thread_id.clone()),
            message: text,
            content,
            project_dir: None,
            working_dir: params.cwd,
            workspace_mode: None,
            target_branch: None,
            session_type: Some(SessionType::Code),
            model: params.model,
            model_key: None,
            thinking_enabled: params.effort,
            fast_mode: params.mitsuro_fast_mode,
            permission_mode: mitsuro_permission_mode(params.mitsuro_permission_mode.as_deref())?,
            mode: mitsuro_work_mode(params.mitsuro_work_mode.as_deref())?,
            research_enabled: None,
        };

        let mut stream = self
            .client
            .chat_stream(request)
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        let _ = event_tx.send(TurnStreamEvent::TurnStarted {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            turn: None,
        });

        let deadline = tokio::time::Instant::now() + overall_timeout;
        let mut event_count = 1usize;
        let mut approvals_answered = 0usize;
        let mut completed = false;
        let mut source_references = SourceReferences::default();
        while let Ok(Some(next)) = tokio::time::timeout_at(deadline, stream.next()).await {
            let event = match next {
                Ok(event) => event,
                Err(error) => {
                    flush_source_references(
                        &event_tx,
                        &thread_id,
                        &turn_id,
                        &assistant_item_id,
                        &mut source_references,
                    );
                    return Err(AgentError::Other(error.to_string()));
                }
            };
            if !matches!(
                &event,
                ChatStreamEvent::TextDelta { .. }
                    | ChatStreamEvent::TextDeltaWithCitations { .. }
                    | ChatStreamEvent::Usage { .. }
            ) {
                event_count += flush_source_references(
                    &event_tx,
                    &thread_id,
                    &turn_id,
                    &assistant_item_id,
                    &mut source_references,
                );
            }
            if matches!(&event, ChatStreamEvent::TurnComplete { .. }) {
                source_references.end_response();
            }
            if let ChatStreamEvent::TextDeltaWithCitations { citations, .. } = &event {
                source_references.extend(citations);
            }
            match event {
                ChatStreamEvent::TextDelta { delta }
                | ChatStreamEvent::TextDeltaWithCitations { delta, .. } => {
                    let _ = event_tx.send(TurnStreamEvent::AgentMessageDelta {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        item_id: assistant_item_id.clone(),
                        delta,
                    });
                    event_count += 1;
                }
                ChatStreamEvent::ThinkingDelta { thinking } => {
                    let _ = event_tx.send(TurnStreamEvent::ReasoningTextDelta {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        item_id: reasoning_item_id.clone(),
                        content_index: None,
                        delta: thinking,
                    });
                    event_count += 1;
                }
                ChatStreamEvent::ToolCallStart { id, name }
                | ChatStreamEvent::ToolExecuting { id, name } => {
                    let _ = event_tx.send(TurnStreamEvent::ItemStarted {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        item_id: id,
                        kind: ItemKind::CommandExecution,
                        item: Some(serde_json::json!({"name": name})),
                    });
                    event_count += 1;
                }
                ChatStreamEvent::ToolOutputDelta { id, delta } => {
                    let _ = event_tx.send(TurnStreamEvent::CommandExecutionOutputDelta {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        item_id: id,
                        delta,
                    });
                    event_count += 1;
                }
                ChatStreamEvent::ToolResult {
                    id,
                    output,
                    is_error,
                } => {
                    let _ = event_tx.send(TurnStreamEvent::ItemCompleted {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        item_id: id,
                        kind: ItemKind::CommandExecution,
                        text: Some(output),
                        item: Some(serde_json::json!({"isError": is_error})),
                    });
                    event_count += 1;
                }
                ChatStreamEvent::PlanUpdate { items } => {
                    let text = items
                        .into_iter()
                        .map(|item| {
                            format!(
                                "- [{}] {}",
                                if item.completed { "x" } else { " " },
                                item.content
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n");
                    let _ = event_tx.send(TurnStreamEvent::PlanDelta {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        item_id: format!("{turn_id}-plan"),
                        delta: text,
                    });
                    event_count += 1;
                }
                ChatStreamEvent::DelegatedProgress { payload } => {
                    let _ = event_tx.send(TurnStreamEvent::DelegatedProgress {
                        thread_id: thread_id.clone(),
                        progress: delegated_progress_projection(payload),
                    });
                    event_count += 1;
                }
                ChatStreamEvent::DelegationEvent { event } => {
                    let _ = event_tx.send(TurnStreamEvent::DelegationEvent {
                        thread_id: thread_id.clone(),
                        event: durable_delegation_event(event),
                    });
                    event_count += 1;
                }
                ChatStreamEvent::ToolApprovalRequired {
                    id,
                    name,
                    arguments,
                } => {
                    let pending = PendingApproval {
                        request_id: crate::protocol::JsonRpcId::String(id.clone()),
                        method: "mitsuro/tool-approval".to_owned(),
                        kind: ApprovalKind::CommandExecution,
                        title: "Approve tool".to_owned(),
                        summary: name,
                        detail: arguments.to_string(),
                        thread_id: Some(thread_id.clone()),
                        turn_id: Some(turn_id.clone()),
                        raw_params: arguments,
                    };
                    let _ = event_tx.send(TurnStreamEvent::ApprovalRequested(pending));
                    event_count += 1;
                    let choice = tokio::task::spawn_blocking({
                        let bridge = Arc::clone(&bridge);
                        move || bridge.wait()
                    })
                    .await
                    .unwrap_or(ApprovalChoice::Reject);
                    self.client
                        .approve_tool(&thread_id, &id, matches!(choice, ApprovalChoice::Approve))
                        .await
                        .map_err(|error| AgentError::Other(error.to_string()))?;
                    approvals_answered += 1;
                }
                ChatStreamEvent::Finish { stop_reason, .. } => {
                    let _ = event_tx.send(TurnStreamEvent::TurnCompleted {
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        status: Some(stop_reason),
                        turn: None,
                    });
                    event_count += 1;
                    completed = true;
                    break;
                }
                ChatStreamEvent::Error { error } => return Err(AgentError::Other(error)),
                ChatStreamEvent::Other {
                    event_type,
                    payload,
                } => {
                    let _ = event_tx.send(unknown_mitsuro_stream_event(event_type, payload));
                    event_count += 1;
                }
                other => {
                    let _ = event_tx.send(TurnStreamEvent::Other {
                        method: format!("mitsuro/{other:?}"),
                        params: None,
                    });
                    event_count += 1;
                }
            }
        }

        event_count += flush_source_references(
            &event_tx,
            &thread_id,
            &turn_id,
            &assistant_item_id,
            &mut source_references,
        );

        Ok(LiveTurnOutcome {
            event_count,
            approvals_answered,
            completed,
        })
    }
}

fn flush_source_references(
    event_tx: &std::sync::mpsc::Sender<TurnStreamEvent>,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    references: &mut SourceReferences,
) -> usize {
    let Some(delta) = references.take_text() else {
        return 0;
    };
    let _ = event_tx.send(TurnStreamEvent::AgentMessageDelta {
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        item_id: item_id.to_owned(),
        delta,
    });
    1
}

fn unknown_mitsuro_stream_event(event_type: String, payload: Value) -> TurnStreamEvent {
    TurnStreamEvent::Other {
        method: format!("mitsuro/{event_type}"),
        params: Some(payload),
    }
}

fn delegation_group_status(value: mitsuro_client::DelegationGroupState) -> DelegationGroupStatus {
    match value {
        mitsuro_client::DelegationGroupState::Created => DelegationGroupStatus::Created,
        mitsuro_client::DelegationGroupState::Queued => DelegationGroupStatus::Queued,
        mitsuro_client::DelegationGroupState::Running => DelegationGroupStatus::Running,
        mitsuro_client::DelegationGroupState::ReadyForParent => {
            DelegationGroupStatus::ReadyForParent
        }
        mitsuro_client::DelegationGroupState::Synthesizing => DelegationGroupStatus::Synthesizing,
        mitsuro_client::DelegationGroupState::Complete => DelegationGroupStatus::Complete,
        mitsuro_client::DelegationGroupState::Degraded => DelegationGroupStatus::Degraded,
        mitsuro_client::DelegationGroupState::Failed => DelegationGroupStatus::Failed,
        mitsuro_client::DelegationGroupState::Cancelled => DelegationGroupStatus::Cancelled,
    }
}

fn delegation_task_status(value: mitsuro_client::DelegationTaskState) -> DelegationTaskStatus {
    match value {
        mitsuro_client::DelegationTaskState::Created => DelegationTaskStatus::Created,
        mitsuro_client::DelegationTaskState::Queued => DelegationTaskStatus::Queued,
        mitsuro_client::DelegationTaskState::Leased => DelegationTaskStatus::Leased,
        mitsuro_client::DelegationTaskState::Running => DelegationTaskStatus::Running,
        mitsuro_client::DelegationTaskState::Retrying => DelegationTaskStatus::Retrying,
        mitsuro_client::DelegationTaskState::Complete => DelegationTaskStatus::Complete,
        mitsuro_client::DelegationTaskState::Degraded => DelegationTaskStatus::Degraded,
        mitsuro_client::DelegationTaskState::Failed => DelegationTaskStatus::Failed,
        mitsuro_client::DelegationTaskState::Cancelled => DelegationTaskStatus::Cancelled,
    }
}

fn delegated_progress_status(
    value: mitsuro_client::DelegatedProgressStatus,
) -> DelegationTaskStatus {
    match value {
        mitsuro_client::DelegatedProgressStatus::Created => DelegationTaskStatus::Created,
        mitsuro_client::DelegatedProgressStatus::Queued => DelegationTaskStatus::Queued,
        mitsuro_client::DelegatedProgressStatus::Leased => DelegationTaskStatus::Leased,
        mitsuro_client::DelegatedProgressStatus::Running => DelegationTaskStatus::Running,
        mitsuro_client::DelegatedProgressStatus::Retrying => DelegationTaskStatus::Retrying,
        mitsuro_client::DelegatedProgressStatus::Complete => DelegationTaskStatus::Complete,
        mitsuro_client::DelegatedProgressStatus::Degraded => DelegationTaskStatus::Degraded,
        mitsuro_client::DelegatedProgressStatus::Cancelled => DelegationTaskStatus::Cancelled,
        mitsuro_client::DelegatedProgressStatus::Failed => DelegationTaskStatus::Failed,
    }
}

fn delegation_role(value: mitsuro_client::DelegatedRunRole) -> DelegationRole {
    match value {
        mitsuro_client::DelegatedRunRole::Explore => DelegationRole::Explore,
        mitsuro_client::DelegatedRunRole::Build => DelegationRole::Build,
        mitsuro_client::DelegatedRunRole::Planner => DelegationRole::Planner,
        mitsuro_client::DelegatedRunRole::Verifier => DelegationRole::Verifier,
    }
}

fn delegation_kind(value: mitsuro_client::DelegatedToolKind) -> DelegationKind {
    match value {
        mitsuro_client::DelegatedToolKind::Explore => DelegationKind::Explore,
        mitsuro_client::DelegatedToolKind::Plan => DelegationKind::Plan,
        mitsuro_client::DelegatedToolKind::Verify => DelegationKind::Verify,
        mitsuro_client::DelegatedToolKind::Build => DelegationKind::Build,
    }
}

fn delegation_run_stage(value: mitsuro_client::DelegatedRunStage) -> DelegationRunStage {
    match value {
        mitsuro_client::DelegatedRunStage::Created => DelegationRunStage::Created,
        mitsuro_client::DelegatedRunStage::Running => DelegationRunStage::Running,
        mitsuro_client::DelegatedRunStage::Synthesizing => DelegationRunStage::Synthesizing,
        mitsuro_client::DelegatedRunStage::Complete => DelegationRunStage::Complete,
        mitsuro_client::DelegatedRunStage::Degraded => DelegationRunStage::Degraded,
        mitsuro_client::DelegatedRunStage::Failed => DelegationRunStage::Failed,
        mitsuro_client::DelegatedRunStage::Cancelled => DelegationRunStage::Cancelled,
    }
}

fn delegated_progress_projection(
    value: mitsuro_client::DelegatedProgressEvent,
) -> DelegatedProgressProjection {
    DelegatedProgressProjection {
        delegated_run_id: value.delegated_run_id,
        tool_call_id: value.tool_call_id,
        kind: delegation_kind(value.kind),
        stage: delegation_run_stage(value.stage),
        parent_session_id: value.parent_session_id,
        task_id: value.task_id,
        agent_name: value.agent_name,
        status: delegated_progress_status(value.status),
        tool_count: value.tool_count,
        tokens: value.tokens,
        current_action: value.current_action,
        completion_summary: value.completion_summary,
        lines_added: value.lines_added,
        lines_removed: value.lines_removed,
        completed_plan_task: value.completed_plan_task,
    }
}

fn durable_event_kind(value: mitsuro_client::DelegationEventKind) -> DurableDelegationEventKind {
    match value {
        mitsuro_client::DelegationEventKind::GroupCreated => {
            DurableDelegationEventKind::GroupCreated
        }
        mitsuro_client::DelegationEventKind::GroupQueued => DurableDelegationEventKind::GroupQueued,
        mitsuro_client::DelegationEventKind::GroupStateChanged => {
            DurableDelegationEventKind::GroupStateChanged
        }
        mitsuro_client::DelegationEventKind::TaskClaimed => DurableDelegationEventKind::TaskClaimed,
        mitsuro_client::DelegationEventKind::TaskRunning => DurableDelegationEventKind::TaskRunning,
        mitsuro_client::DelegationEventKind::TaskStateChanged => {
            DurableDelegationEventKind::TaskStateChanged
        }
        mitsuro_client::DelegationEventKind::ParentContinuationQueued => {
            DurableDelegationEventKind::ParentContinuationQueued
        }
        mitsuro_client::DelegationEventKind::ParentContinuationPromoted => {
            DurableDelegationEventKind::ParentContinuationPromoted
        }
        mitsuro_client::DelegationEventKind::Other(value) => {
            DurableDelegationEventKind::Other(value)
        }
    }
}

fn durable_delegation_event(
    value: mitsuro_client::DelegationEventResponse,
) -> DurableDelegationEvent {
    DurableDelegationEvent {
        id: value.event_id,
        parent_session_id: value.parent_session_id,
        group_id: value.delegation_group_id,
        task_id: value.delegation_task_id,
        kind: durable_event_kind(value.event_type),
        payload: value.payload,
        created_at: value.created_at,
    }
}

fn session_delegation_projection(
    value: mitsuro_client::SessionStateResponse,
) -> SessionDelegationProjection {
    SessionDelegationProjection {
        groups: value
            .delegation_groups
            .into_iter()
            .map(|group| DelegationGroupProjection {
                id: group.delegation_group_id,
                parent_tool_call_id: group.parent_tool_call_id,
                status: delegation_group_status(group.state),
                execution: match group.execution_mode {
                    mitsuro_client::DelegationExecutionMode::Foreground => {
                        DelegationExecution::Foreground
                    }
                    mitsuro_client::DelegationExecutionMode::Detached => {
                        DelegationExecution::Detached
                    }
                },
                parent_continuation: match group.parent_continuation_state {
                    mitsuro_client::DelegationParentContinuationState::NotRequested => {
                        DelegationParentContinuationStatus::NotRequested
                    }
                    mitsuro_client::DelegationParentContinuationState::Pending => {
                        DelegationParentContinuationStatus::Pending
                    }
                    mitsuro_client::DelegationParentContinuationState::Queued => {
                        DelegationParentContinuationStatus::Queued
                    }
                    mitsuro_client::DelegationParentContinuationState::Promoted => {
                        DelegationParentContinuationStatus::Promoted
                    }
                },
                tasks: group
                    .tasks
                    .into_iter()
                    .map(|task| DelegationTaskProjection {
                        id: task.delegation_task_id,
                        key: task.task_key,
                        role: delegation_role(task.role),
                        status: delegation_task_status(task.state),
                        attempt_count: task.attempt_count,
                        updated_at: task.updated_at,
                    })
                    .collect(),
                updated_at: group.updated_at,
            })
            .collect(),
        events: value
            .delegation_events
            .into_iter()
            .map(durable_delegation_event)
            .collect(),
        event_cursor: value.delegation_event_cursor,
    }
}

fn turn_input_text(input: &[Value]) -> String {
    input
        .iter()
        .filter_map(|value| value.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn turn_input_content(input: &[Value]) -> Result<(String, Vec<ContentBlock>)> {
    const MAX_LOCAL_IMAGE_BYTES: usize = 20 * 1024 * 1024;

    let text = turn_input_text(input);
    let mut content = Vec::new();
    for value in input {
        match value.get("type").and_then(Value::as_str) {
            Some("image") => {
                let url = value
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or_else(|| AgentError::Protocol("image input is missing url".to_owned()))?;
                content.push(ContentBlock::Image {
                    source: ImageSource::Url {
                        url: url.to_owned(),
                    },
                });
            }
            Some("localImage") => {
                let raw_path = value.get("path").and_then(Value::as_str).ok_or_else(|| {
                    AgentError::Protocol("localImage input is missing path".to_owned())
                })?;
                let path = Path::new(raw_path);
                let media_type = local_image_media_type(path).ok_or_else(|| {
                    AgentError::Protocol(format!(
                        "unsupported local image type: {}",
                        path.display()
                    ))
                })?;
                let bytes = tokio::fs::read(path).await.map_err(|error| {
                    AgentError::Other(format!("read local image {}: {error}", path.display()))
                })?;
                if bytes.len() > MAX_LOCAL_IMAGE_BYTES {
                    return Err(AgentError::Other(format!(
                        "local image exceeds 20 MiB: {}",
                        path.display()
                    )));
                }
                content.push(ContentBlock::Image {
                    source: ImageSource::Base64 {
                        media_type: media_type.to_owned(),
                        data: base64::engine::general_purpose::STANDARD.encode(bytes),
                    },
                });
            }
            Some("audio" | "localAudio" | "skill" | "mention") => {
                return Err(AgentError::NotImplemented(
                    "Mitsuro HTTP does not accept this Codex-only input type".to_owned(),
                ));
            }
            _ => {}
        }
    }
    Ok((text, content))
}

fn local_image_media_type(path: &Path) -> Option<&'static str> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())?
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "webp" => Some("image/webp"),
        "gif" => Some("image/gif"),
        _ => None,
    }
}

fn message_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .map(message_text)
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("content"))
            .map(message_text)
            .unwrap_or_else(|| value.to_string()),
        Value::Null => String::new(),
        _ => value.to_string(),
    }
}

fn mitsuro_user_content(value: &Value) -> Vec<Value> {
    let parts = value.as_array().map(Vec::as_slice).unwrap_or(&[]);
    if parts.is_empty() {
        let text = message_text(value);
        return if text.is_empty() {
            Vec::new()
        } else {
            vec![serde_json::json!({"type": "text", "text": text})]
        };
    }

    parts
        .iter()
        .filter_map(|part| match part.get("type").and_then(Value::as_str) {
            Some("text") => part
                .get("text")
                .and_then(Value::as_str)
                .map(|text| serde_json::json!({"type": "text", "text": text})),
            Some("image") => {
                let source = part.get("source")?;
                match source.get("type").and_then(Value::as_str) {
                    Some("url") => source
                        .get("url")
                        .and_then(Value::as_str)
                        .map(|url| serde_json::json!({"type": "image", "url": url})),
                    Some("base64") => {
                        let media_type = source.get("media_type").and_then(Value::as_str)?;
                        let data = source.get("data").and_then(Value::as_str)?;
                        Some(serde_json::json!({
                            "type": "image",
                            "url": format!("data:{media_type};base64,{data}")
                        }))
                    }
                    _ => None,
                }
            }
            _ => None,
        })
        .collect()
}

/// Project Mitsuro's persisted model-message blocks into the same typed thread-item
/// vocabulary used by Codex. Stored tool results use the `user` model role because
/// that is how providers consume them; they are not authored user chat messages and
/// must never be rendered as raw JSON bubbles.
fn mitsuro_history_items(messages: &[MessageResponse]) -> Vec<Value> {
    let mut items = Vec::new();
    let mut tool_items = HashMap::<String, usize>::new();

    for (message_index, message) in messages.iter().enumerate() {
        let parts = message
            .content
            .as_array()
            .cloned()
            .unwrap_or_else(|| vec![message.content.clone()]);
        let mut authored_user_parts = Vec::new();
        let mut source_references = SourceReferences::default();

        for (part_index, part) in parts.into_iter().enumerate() {
            let item_id = format!("mitsuro-message-{message_index}-{part_index}");
            let block_type = part.get("type").and_then(Value::as_str);

            if message.role == "user" {
                if block_type == Some("tool_result") {
                    apply_mitsuro_tool_result(&mut items, &tool_items, &part, item_id);
                } else {
                    authored_user_parts.push(part);
                }
                continue;
            }

            if message.role == "assistant" {
                match block_type {
                    Some("thinking") => {
                        if let Some(thinking) = part.get("thinking").and_then(Value::as_str) {
                            if !thinking.is_empty() {
                                items.push(serde_json::json!({
                                    "id": item_id,
                                    "type": "reasoning",
                                    "summary": [thinking],
                                    "content": [],
                                }));
                            }
                        }
                    }
                    Some("tool_use") => {
                        let tool_id = part
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .map(str::to_owned)
                            .unwrap_or_else(|| item_id.clone());
                        let item = mitsuro_tool_use_item(&part, &tool_id);
                        tool_items.insert(tool_id, items.len());
                        items.push(item);
                    }
                    Some("text") => {
                        if let Some(text) = part.get("text").and_then(Value::as_str) {
                            let citations =
                                mitsuro_client::citations_from_value(part.get("citations"));
                            source_references.extend(&citations);
                            let mut text = text.to_owned();
                            if let Some(sources) = source_references.take_text() {
                                text.push_str(&sources);
                            }
                            if !text.is_empty() || !citations.is_empty() {
                                let mut item = serde_json::json!({
                                    "id": item_id,
                                    "type": "agentMessage",
                                    "text": text,
                                });
                                if !citations.is_empty() {
                                    item["citations"] = serde_json::json!(citations);
                                }
                                items.push(item);
                            }
                        }
                    }
                    _ => {
                        let text = message_text(&part);
                        if !text.is_empty() && !part.is_object() {
                            items.push(serde_json::json!({
                                "id": item_id,
                                "type": "agentMessage",
                                "text": text,
                            }));
                        } else if !part.is_null() {
                            items.push(serde_json::json!({
                                "id": item_id,
                                "type": "mitsuroActivity",
                                "status": "completed",
                                "sourceType": block_type.unwrap_or("unknown"),
                                "detail": part,
                            }));
                        }
                    }
                }
                continue;
            }

            if !part.is_null() {
                items.push(serde_json::json!({
                    "id": item_id,
                    "type": "mitsuroActivity",
                    "status": "completed",
                    "sourceRole": message.role,
                    "detail": part,
                }));
            }
        }

        if !authored_user_parts.is_empty() {
            let content = mitsuro_user_content(&Value::Array(authored_user_parts));
            if !content.is_empty() {
                items.push(serde_json::json!({
                    "id": format!("mitsuro-message-{message_index}"),
                    "type": "userMessage",
                    "content": content,
                }));
            }
        }
    }

    items
}

fn mitsuro_tool_use_item(part: &Value, tool_id: &str) -> Value {
    let name = part.get("name").and_then(Value::as_str).unwrap_or("tool");
    let input = part.get("input").cloned().unwrap_or(Value::Null);
    match name {
        "bash" => serde_json::json!({
            "id": tool_id,
            "type": "commandExecution",
            "command": input.get("command").and_then(Value::as_str).unwrap_or("bash"),
            "cwd": input.get("cwd").and_then(Value::as_str).unwrap_or(""),
            "status": "inProgress",
            "aggregatedOutput": "",
        }),
        "agent" => serde_json::json!({
            "id": tool_id,
            "type": "collabAgentToolCall",
            "tool": name,
            "prompt": input.get("prompt")
                .or_else(|| input.get("description"))
                .and_then(Value::as_str)
                .unwrap_or(""),
            "status": "inProgress",
        }),
        "webfetch" => serde_json::json!({
            "id": tool_id,
            "type": "webSearch",
            "query": input.get("url").and_then(Value::as_str).unwrap_or(""),
            "status": "inProgress",
        }),
        _ => serde_json::json!({
            "id": tool_id,
            "type": "dynamicToolCall",
            "namespace": "mitsuro",
            "tool": name,
            "status": "inProgress",
            "arguments": input,
        }),
    }
}

fn apply_mitsuro_tool_result(
    items: &mut Vec<Value>,
    tool_items: &HashMap<String, usize>,
    part: &Value,
    fallback_id: String,
) {
    let tool_id = part
        .get("tool_use_id")
        .and_then(Value::as_str)
        .unwrap_or("");
    let output = part.get("output").unwrap_or(&Value::Null);
    let is_error = part
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || output
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let summary = mitsuro_tool_result_summary(output);
    let status = if is_error { "failed" } else { "completed" };

    if let Some(item) = tool_items
        .get(tool_id)
        .and_then(|index| items.get_mut(*index))
        .and_then(Value::as_object_mut)
    {
        item.insert("status".to_owned(), Value::String(status.to_owned()));
        item.insert("summary".to_owned(), Value::String(summary.clone()));
        if item.get("type").and_then(Value::as_str) == Some("commandExecution") {
            item.insert("aggregatedOutput".to_owned(), Value::String(summary));
            if let Some(exit_code) = output
                .get("result")
                .and_then(|result| result.get("exit_code"))
                .and_then(Value::as_i64)
            {
                item.insert("exitCode".to_owned(), Value::from(exit_code));
            }
        }
        return;
    }

    let tool = output.get("tool").and_then(Value::as_str).unwrap_or("tool");
    items.push(serde_json::json!({
        "id": if tool_id.is_empty() { fallback_id } else { tool_id.to_owned() },
        "type": "dynamicToolCall",
        "namespace": "mitsuro",
        "tool": tool,
        "status": status,
        "summary": summary,
    }));
}

fn mitsuro_tool_result_summary(output: &Value) -> String {
    output
        .get("summary")
        .and_then(Value::as_str)
        .or_else(|| {
            output
                .get("result")
                .and_then(|result| result.get("output_preview"))
                .and_then(Value::as_str)
        })
        .or_else(|| output.as_str())
        .unwrap_or_else(|| {
            if output.is_null() {
                "Tool completed"
            } else {
                "Tool result available"
            }
        })
        .to_owned()
}

fn mitsuro_reasoning_effort_name(effort: mitsuro_client::ReasoningEffort) -> Option<&'static str> {
    use mitsuro_client::ReasoningEffort;
    match effort {
        ReasoningEffort::None => Some("none"),
        ReasoningEffort::Minimal => Some("minimal"),
        ReasoningEffort::Low => Some("low"),
        ReasoningEffort::Medium => Some("medium"),
        ReasoningEffort::High => Some("high"),
        ReasoningEffort::XHigh => Some("xhigh"),
        ReasoningEffort::Max => Some("max"),
        ReasoningEffort::Ultra => Some("ultra"),
        ReasoningEffort::Unknown => None,
    }
}

fn session_json(session: &mitsuro_client::SessionInfo) -> Value {
    serde_json::json!({
        "id": session.id,
        "name": session.title,
        "preview": session.title,
        "cwd": session.working_dir,
        "updatedAt": parse_session_timestamp(&session.updated_at),
        "modelProvider": session.model_key.as_ref().map(|key| key.provider.clone()),
        "ephemeral": false,
        "archived": false,
    })
}

fn parse_session_timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value.trim())
        .ok()
        .map(|timestamp| timestamp.timestamp())
}

fn collect_fuzzy_matches(
    root: &str,
    query: &str,
    entries: Vec<mitsuro_client::FileTreeEntry>,
    matches: &mut Vec<FuzzyFileSearchResult>,
) {
    for entry in entries {
        if let Some((score, indices)) = fuzzy_score_name(query, &entry.name) {
            matches.push(FuzzyFileSearchResult {
                root: root.to_owned(),
                path: entry.path.clone(),
                match_type: if entry.is_dir {
                    FuzzyFileSearchMatchType::Directory
                } else {
                    FuzzyFileSearchMatchType::File
                },
                file_name: entry.name,
                score,
                indices: Some(indices),
            });
        }
        if let Some(children) = entry.children {
            collect_fuzzy_matches(root, query, children, matches);
        }
    }
}

#[async_trait]
impl AgentBackend for MitsuroServerBackend {
    fn name(&self) -> &'static str {
        "mitsuro"
    }

    fn status(&self) -> ConnectionStatus {
        self.status
            .read()
            .map(|s| s.clone())
            .unwrap_or(ConnectionStatus::Disconnected)
    }

    fn supports_method(&self, method: &str) -> bool {
        matches!(
            method,
            "initialize"
                | "thread/list"
                | "thread/start"
                | "thread/read"
                | "thread/name/set"
                | "thread/delete"
                | "turn/start"
                | "turn/steer"
                | "turn/interrupt"
                | "model/list"
                | "skills/list"
                | "fs/readDirectory"
                | "fs/readFile"
                | "fuzzyFileSearch"
                | "mcpServerStatus/list"
                | "plugin/list"
        )
    }

    async fn call_raw(&self, method: &str, _params: Value) -> Result<Value> {
        Err(AgentError::NotImplemented(format!(
            "MitsuroServerBackend::call_raw({method}) — not implemented"
        )))
    }

    async fn connect(&self) -> Result<InitializeResponse> {
        self.set_status(ConnectionStatus::Connecting);
        match self.client.health().await {
            Ok(health) => {
                if let Ok(mut version) = self.server_version.write() {
                    *version = Some(health.version.clone());
                }
                self.set_status(ConnectionStatus::Ready);
                Ok(InitializeResponse {
                    codex_home: String::new(),
                    platform_family: "mitsuro-http".to_owned(),
                    platform_os: std::env::consts::OS.to_owned(),
                    user_agent: format!("mitsuro-server/{}", health.version),
                })
            }
            Err(error) => {
                self.set_status(ConnectionStatus::Error(error.to_string()));
                Err(AgentError::Other(error.to_string()))
            }
        }
    }

    async fn thread_list(&self, params: ThreadListParams) -> Result<ThreadListResponse> {
        let mut sessions = self
            .client
            .list_sessions()
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        if let Some(limit) = params.limit {
            sessions.truncate(limit as usize);
        }
        Ok(ThreadListResponse {
            data: sessions.iter().map(session_json).collect(),
            next_cursor: None,
            backwards_cursor: None,
        })
    }

    async fn thread_start(&self, params: ThreadStartParams) -> Result<ThreadStartResponse> {
        let session = self
            .client
            .create_session(CreateSessionRequest {
                title: None,
                model: params.model.clone(),
                model_key: None,
                project_dir: None,
                working_dir: params.cwd.clone(),
                workspace_mode: None,
                target_branch: None,
                session_type: Some(SessionType::Code),
                permission_mode: mitsuro_permission_mode(
                    params.mitsuro_permission_mode.as_deref(),
                )?,
            })
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(ThreadStartResponse {
            thread: session_json(&session),
            model: session.model,
            model_provider: session.model_key.map(|key| key.provider),
            cwd: session.working_dir,
        })
    }

    async fn thread_read(&self, params: ThreadReadParams) -> Result<ThreadReadResponse> {
        let transcript = self
            .client
            .get_session(&params.thread_id)
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        let items = mitsuro_history_items(&transcript.messages);
        let mut thread = session_json(&transcript.session);
        if let Some(object) = thread.as_object_mut() {
            object.insert(
                "turns".to_owned(),
                serde_json::json!([{"id": "mitsuro-history", "items": items}]),
            );
        }
        Ok(ThreadReadResponse { thread })
    }

    async fn model_list(&self, _params: ModelListParams) -> Result<ModelListResponse> {
        let response = self
            .client
            .list_models()
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        let default = response.default_model;
        let data = response
            .models
            .into_iter()
            .map(|model| {
                let mut input_modalities = vec!["text".to_owned()];
                if model.supports_vision {
                    input_modalities.push("image".to_owned());
                }
                let default_reasoning_effort = model
                    .default_reasoning_level
                    .and_then(mitsuro_reasoning_effort_name)
                    .unwrap_or_default()
                    .to_owned();
                let supported_reasoning_efforts = model
                    .supported_reasoning_levels
                    .into_iter()
                    .filter_map(|effort| {
                        mitsuro_reasoning_effort_name(effort).map(|name| ReasoningEffortOption {
                            reasoning_effort: name.to_owned(),
                            description: format!("Mitsuro {name} reasoning"),
                        })
                    })
                    .collect();
                let service_tiers = if model.supports_fast_mode {
                    vec![ModelServiceTier {
                        id: "priority".to_owned(),
                        name: "Fast".to_owned(),
                        description: "Faster responses with increased Mitsuro usage".to_owned(),
                    }]
                } else {
                    Vec::new()
                };
                ModelInfo {
                    id: model.id.clone(),
                    model: model.id.clone(),
                    display_name: model.display_name,
                    description: format!("{} provider", model.provider),
                    hidden: false,
                    is_default: default.as_deref() == Some(model.id.as_str()),
                    default_reasoning_effort,
                    supported_reasoning_efforts,
                    service_tiers,
                    default_service_tier: None,
                    input_modalities,
                    upgrade: None,
                }
            })
            .collect();
        Ok(ModelListResponse {
            data,
            next_cursor: None,
        })
    }

    async fn config_read(&self, _params: ConfigReadParams) -> Result<ConfigReadResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::config_read — not implemented".into(),
        ))
    }

    async fn thread_search(&self, _params: ThreadSearchParams) -> Result<ThreadSearchResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_search — not implemented".into(),
        ))
    }

    async fn thread_search_occurrences(
        &self,
        params: ThreadSearchOccurrencesParams,
    ) -> Result<ThreadSearchOccurrencesResponse> {
        let response = self
            .thread_read(ThreadReadParams {
                thread_id: params.thread_id.clone(),
                include_turns: Some(true),
            })
            .await?;
        Ok(search_occurrences_in_thread(&response.thread, &params))
    }

    async fn thread_turns_list(
        &self,
        params: ThreadTurnsListParams,
    ) -> Result<ThreadTurnsListResponse> {
        let response = self
            .thread_read(ThreadReadParams {
                thread_id: params.thread_id.clone(),
                include_turns: Some(true),
            })
            .await?;
        Ok(list_turns_in_thread(&response.thread, &params))
    }

    async fn thread_rollback(
        &self,
        _params: ThreadRollbackParams,
    ) -> Result<ThreadRollbackResponse> {
        Err(AgentError::NotImplemented(
            "Mitsuro HTTP does not expose destructive turn rollback".to_owned(),
        ))
    }

    async fn thread_name_set(&self, params: ThreadSetNameParams) -> Result<ThreadSetNameResponse> {
        self.client
            .update_session(
                &params.thread_id,
                UpdateSessionRequest {
                    title: Some(params.name),
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(ThreadSetNameResponse::default())
    }

    async fn thread_archive(&self, _params: ThreadArchiveParams) -> Result<ThreadArchiveResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_archive — not implemented".into(),
        ))
    }

    async fn thread_unarchive(
        &self,
        _params: ThreadUnarchiveParams,
    ) -> Result<ThreadUnarchiveResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_unarchive — not implemented".into(),
        ))
    }

    async fn thread_delete(&self, params: ThreadDeleteParams) -> Result<ThreadDeleteResponse> {
        self.client
            .delete_session(&params.thread_id)
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(ThreadDeleteResponse::default())
    }

    async fn thread_fork(&self, _params: ThreadForkParams) -> Result<ThreadForkResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_fork — not implemented".into(),
        ))
    }

    async fn thread_resume(&self, _params: ThreadResumeParams) -> Result<ThreadResumeResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_resume — not implemented".into(),
        ))
    }

    async fn thread_goal_get(&self, _params: ThreadGoalGetParams) -> Result<ThreadGoalGetResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_goal_get — not implemented".into(),
        ))
    }

    async fn thread_goal_set(&self, _params: ThreadGoalSetParams) -> Result<ThreadGoalSetResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_goal_set — not implemented".into(),
        ))
    }

    async fn thread_goal_clear(
        &self,
        _params: ThreadGoalClearParams,
    ) -> Result<ThreadGoalClearResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::thread_goal_clear — not implemented".into(),
        ))
    }

    async fn skills_list(&self, _params: SkillsListParams) -> Result<SkillsListResponse> {
        let skills = self
            .client
            .list_skills()
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?
            .into_iter()
            .map(|skill| SkillMetadata {
                name: skill.name,
                description: skill.description,
                enabled: skill.enabled,
                path: skill.path,
                scope: skill.source,
                short_description: None,
            })
            .collect();
        Ok(SkillsListResponse {
            data: vec![SkillsListEntry {
                cwd: String::new(),
                skills,
                errors: Vec::new(),
            }],
        })
    }

    async fn turn_start(&self, _params: TurnStartParams) -> Result<TurnStartResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::turn_start — not implemented".into(),
        ))
    }

    async fn turn_steer(&self, params: TurnSteerParams) -> Result<TurnSteerResponse> {
        let (message, content) = turn_input_content(&params.input).await?;
        self.client
            .steer(SteerRequest {
                session_id: params.thread_id,
                message,
                content,
            })
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(TurnSteerResponse {
            turn_id: params.expected_turn_id,
        })
    }

    async fn turn_interrupt(&self, params: TurnInterruptParams) -> Result<TurnInterruptResponse> {
        self.client
            .cancel_session(&params.thread_id)
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(TurnInterruptResponse::default())
    }

    async fn process_spawn(&self, _params: ProcessSpawnParams) -> Result<ProcessSpawnResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::process_spawn — not implemented".into(),
        ))
    }

    async fn process_write_stdin(
        &self,
        _params: ProcessWriteStdinParams,
    ) -> Result<ProcessWriteStdinResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::process_write_stdin — not implemented".into(),
        ))
    }

    async fn process_resize_pty(
        &self,
        _params: ProcessResizePtyParams,
    ) -> Result<ProcessResizePtyResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::process_resize_pty — not implemented".into(),
        ))
    }

    async fn process_kill(&self, _params: ProcessKillParams) -> Result<ProcessKillResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::process_kill — not implemented".into(),
        ))
    }

    async fn fs_read_directory(
        &self,
        params: FsReadDirectoryParams,
    ) -> Result<FsReadDirectoryResponse> {
        let tree = self
            .client
            .file_tree(&params.path, 1)
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(FsReadDirectoryResponse {
            entries: tree
                .entries
                .into_iter()
                .map(|entry| {
                    if entry.is_dir {
                        FsReadDirectoryEntry::directory(entry.name)
                    } else {
                        FsReadDirectoryEntry::file(entry.name)
                    }
                })
                .collect(),
        })
    }

    async fn fs_read_file(&self, params: FsReadFileParams) -> Result<FsReadFileResponse> {
        let file = self
            .client
            .read_file(&params.path)
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        Ok(FsReadFileResponse::from_text(&file.content))
    }

    async fn fs_get_metadata(&self, _params: FsGetMetadataParams) -> Result<FsGetMetadataResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::fs_get_metadata — not implemented".into(),
        ))
    }

    async fn fuzzy_file_search(
        &self,
        params: FuzzyFileSearchParams,
    ) -> Result<FuzzyFileSearchResponse> {
        let mut files = Vec::new();
        for root in &params.roots {
            let tree = self
                .client
                .file_tree(root, 10)
                .await
                .map_err(|error| AgentError::Other(error.to_string()))?;
            collect_fuzzy_matches(root, &params.query, tree.entries, &mut files);
        }
        files.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.path.cmp(&right.path))
        });
        files.truncate(200);
        Ok(FuzzyFileSearchResponse { files })
    }

    async fn fuzzy_file_search_session_start(
        &self,
        _params: FuzzyFileSearchSessionStartParams,
    ) -> Result<FuzzyFileSearchSessionStartResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::fuzzy_file_search_session_start — not implemented".into(),
        ))
    }

    async fn fuzzy_file_search_session_update(
        &self,
        _params: FuzzyFileSearchSessionUpdateParams,
    ) -> Result<FuzzyFileSearchSessionUpdateResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::fuzzy_file_search_session_update — not implemented".into(),
        ))
    }

    async fn fuzzy_file_search_session_stop(
        &self,
        _params: FuzzyFileSearchSessionStopParams,
    ) -> Result<FuzzyFileSearchSessionStopResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::fuzzy_file_search_session_stop — not implemented".into(),
        ))
    }

    async fn mcp_server_status_list(
        &self,
        _params: ListMcpServerStatusParams,
    ) -> Result<ListMcpServerStatusResponse> {
        let data = self
            .client
            .list_mcp_servers()
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?
            .into_iter()
            .map(|server| McpServerStatus {
                name: server.name.clone(),
                server_info: Some(McpServerInfo {
                    name: server.name,
                    version: String::new(),
                    title: None,
                    description: Some(server.status),
                    website_url: None,
                }),
                tools: server
                    .tools
                    .into_iter()
                    .filter_map(|tool| {
                        let name = tool.get("name")?.as_str()?.to_owned();
                        Some((name, tool))
                    })
                    .collect(),
                resources: Vec::new(),
                resource_templates: Vec::new(),
                auth_status: if server.connected {
                    McpAuthStatus::Unsupported
                } else {
                    McpAuthStatus::NotLoggedIn
                },
            })
            .collect();
        Ok(ListMcpServerStatusResponse {
            data,
            next_cursor: None,
        })
    }

    async fn mcp_server_tool_call(
        &self,
        _params: McpServerToolCallParams,
    ) -> Result<McpServerToolCallResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::mcp_server_tool_call — not implemented".into(),
        ))
    }

    async fn plugin_list(&self, _params: PluginListParams) -> Result<PluginListResponse> {
        let overview = self
            .client
            .list_extensions()
            .await
            .map_err(|error| AgentError::Other(error.to_string()))?;
        let plugins = overview
            .extensions
            .into_iter()
            .map(|extension| PluginSummary {
                id: extension.id,
                name: extension.name.clone(),
                source: PluginSource::Local {
                    path: extension.path,
                },
                installed: true,
                enabled: true,
                install_policy: PluginInstallPolicy::NotAvailable,
                auth_policy: PluginAuthPolicy::OnUse,
                availability: PluginAvailability::Available,
                version: Some(extension.version.clone()),
                local_version: Some(extension.version),
                remote_plugin_id: None,
                interface: Some(PluginInterface {
                    display_name: Some(extension.name),
                    short_description: Some(format!(
                        "{} tool(s) · {} command(s)",
                        extension.tools.len(),
                        extension.commands.len()
                    )),
                    category: Some("agent extension".to_owned()),
                    capabilities: extension
                        .tools
                        .into_iter()
                        .chain(extension.commands)
                        .collect(),
                    ..Default::default()
                }),
                keywords: vec!["mitsuro".to_owned(), "extension".to_owned()],
                extra: Default::default(),
            })
            .collect();
        Ok(PluginListResponse {
            marketplaces: vec![PluginMarketplaceEntry {
                name: "Mitsuro agent extensions".to_owned(),
                path: None,
                plugins,
                interface: None,
            }],
            marketplace_load_errors: overview.diagnostics,
            featured_plugin_ids: Vec::new(),
        })
    }

    async fn plugin_read(&self, _params: PluginReadParams) -> Result<PluginReadResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::plugin_read — not implemented".into(),
        ))
    }

    async fn plugin_installed(
        &self,
        _params: PluginInstalledParams,
    ) -> Result<PluginInstalledResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::plugin_installed — not implemented".into(),
        ))
    }

    async fn environment_info(
        &self,
        _params: EnvironmentInfoParams,
    ) -> Result<EnvironmentInfoResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::environment_info — not implemented".into(),
        ))
    }

    async fn environment_status(
        &self,
        _params: EnvironmentStatusParams,
    ) -> Result<EnvironmentStatusResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::environment_status — not implemented".into(),
        ))
    }

    async fn environment_add(
        &self,
        _params: EnvironmentAddParams,
    ) -> Result<EnvironmentAddResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::environment_add — not implemented".into(),
        ))
    }

    async fn collaboration_mode_list(
        &self,
        _params: CollaborationModeListParams,
    ) -> Result<CollaborationModeListResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::collaboration_mode_list — not implemented".into(),
        ))
    }

    async fn account_read(&self, _params: GetAccountParams) -> Result<GetAccountResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::account_read — not implemented".into(),
        ))
    }

    async fn account_login_start(
        &self,
        _params: LoginAccountParams,
    ) -> Result<LoginAccountResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::account_login_start — not implemented".into(),
        ))
    }

    async fn account_login_cancel(
        &self,
        _params: CancelLoginAccountParams,
    ) -> Result<CancelLoginAccountResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::account_login_cancel — not implemented".into(),
        ))
    }

    async fn account_logout(&self) -> Result<LogoutAccountResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::account_logout — not implemented".into(),
        ))
    }

    async fn account_usage_read(&self) -> Result<GetAccountTokenUsageResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::account_usage_read — not implemented".into(),
        ))
    }

    async fn account_rate_limits_read(&self) -> Result<GetAccountRateLimitsResponse> {
        Err(AgentError::NotImplemented(
            "MitsuroServerBackend::account_rate_limits_read — not implemented".into(),
        ))
    }

    async fn account_workspace_messages_read(&self) -> Result<GetWorkspaceMessagesResponse> {
        Err(AgentError::NotImplemented(
            "Mitsuro HTTP does not expose Codex workspace account messages".into(),
        ))
    }

    async fn account_rate_limit_reset_credit_consume(
        &self,
        _params: ConsumeAccountRateLimitResetCreditParams,
    ) -> Result<ConsumeAccountRateLimitResetCreditResponse> {
        Err(AgentError::NotImplemented(
            "Mitsuro HTTP does not expose Codex rate-limit reset credits".into(),
        ))
    }

    async fn account_send_add_credits_nudge_email(
        &self,
        _params: SendAddCreditsNudgeEmailParams,
    ) -> Result<SendAddCreditsNudgeEmailResponse> {
        Err(AgentError::NotImplemented(
            "Mitsuro HTTP does not expose Codex workspace credit email actions".into(),
        ))
    }

    async fn disconnect(&self) -> Result<()> {
        if let Ok(mut s) = self.status.write() {
            *s = ConnectionStatus::Disconnected;
        }
        Ok(())
    }
}

fn mitsuro_permission_mode(value: Option<&str>) -> Result<Option<PermissionMode>> {
    match value {
        None => Ok(None),
        Some("supervised") => Ok(Some(PermissionMode::Supervised)),
        Some("autonomous") => Ok(Some(PermissionMode::Autonomous)),
        Some(other) => Err(AgentError::NotImplemented(format!(
            "Mitsuro HTTP does not accept permission mode {other}"
        ))),
    }
}

fn mitsuro_work_mode(value: Option<&str>) -> Result<Option<WorkMode>> {
    match value {
        None => Ok(None),
        Some("build") => Ok(Some(WorkMode::Build)),
        Some("plan") => Ok(Some(WorkMode::Plan)),
        Some(other) => Err(AgentError::NotImplemented(format!(
            "Mitsuro HTTP does not accept work mode {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_citations_keep_metadata_and_readable_sources_on_the_answer() {
        let citation = serde_json::json!({"url":"https://example.com/docs","title":"Docs","cited_text":"Answer"});
        let messages: Vec<MessageResponse> = serde_json::from_value(serde_json::json!([{
            "role":"assistant", "content":[
                {"type":"text","text":"Answer", "citations":[citation.clone()]},
                {"type":"text","text":"More", "citations":[citation.clone()]}
            ]
        }]))
        .unwrap();
        let items = mitsuro_history_items(&messages);
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0]["text"],
            "Answer\n\nSources:\n- [Docs](<https://example.com/docs>)\n"
        );
        assert_eq!(items[0]["citations"], serde_json::json!([citation]));
        assert_eq!(items[1]["text"], "More");
    }

    #[tokio::test]
    async fn cited_stream_sources_survive_finish_cancel_error_disconnect_and_timeout() {
        use std::io::{Read as _, Write as _};

        for terminal in [
            "finish",
            "cancel",
            "error",
            "disconnect",
            "timeout",
            "malformed",
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let frame = serde_json::json!({"type":"text_delta_with_citations","delta":"Answer","citations":[{"url":"https://example.com/docs","title":"Docs","cited_text":"Answer"}]}).to_string();
            let annotations = serde_json::json!({"type":"text_delta_with_citations","delta":"","citations":[{"url":"https://example.com/docs","title":"Docs","cited_text":"Answer"}]}).to_string();
            let ending = match terminal {
                "finish" => "data: {\"type\":\"finish\",\"session_id\":\"session-1\",\"stop_reason\":\"completed\"}\n\n",
                "cancel" => "data: {\"type\":\"finish\",\"session_id\":\"session-1\",\"stop_reason\":\"user_abort\"}\n\n",
                "error" => "data: {\"type\":\"error\",\"error\":\"fixture failure\"}\n\n",
                "malformed" => "data: {invalid JSON}\n\n",
                _ => "",
            };
            let body = format!("data: {frame}\n\ndata: {annotations}\n\n{ending}");
            let server = std::thread::spawn(move || {
                for request_index in 0..2 {
                    let (mut socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0_u8; 2048];
                    loop {
                        let count = socket.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        request.extend_from_slice(&buffer[..count]);
                        let Some(header_end) =
                            request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                        else {
                            continue;
                        };
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if request.len() >= header_end + 4 + length {
                            break;
                        }
                    }
                    if request_index == 0 {
                        assert!(request.starts_with(b"GET /api/sessions/session-1/control "));
                        socket.write_all(b"HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").unwrap();
                        continue;
                    }
                    assert!(request.starts_with(b"POST /api/chat "));
                    let length = body.len() + usize::from(terminal == "timeout");
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}").as_bytes()).unwrap();
                    socket.flush().unwrap();
                    if terminal == "timeout" {
                        std::thread::sleep(Duration::from_millis(150));
                    }
                }
            });
            let backend =
                MitsuroServerBackend::from_url(format!("http://{address}"), None).unwrap();
            let (events, received) = std::sync::mpsc::channel();
            let timeout = if terminal == "timeout" {
                Duration::from_millis(40)
            } else {
                Duration::from_secs(3)
            };
            let result = backend
                .run_turn_streaming(
                    TurnStartParams::text("session-1", "fixture"),
                    events,
                    Arc::new(LiveApprovalBridge::new()),
                    timeout,
                )
                .await;
            server.join().unwrap();
            assert_eq!(
                result.is_err(),
                matches!(terminal, "error" | "malformed"),
                "{terminal}"
            );
            if let Ok(outcome) = result {
                assert_eq!(outcome.completed, matches!(terminal, "finish" | "cancel"));
            }
            let events = received.try_iter().collect::<Vec<_>>();
            let text = events
                .iter()
                .filter_map(|event| match event {
                    TurnStreamEvent::AgentMessageDelta { delta, .. } => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<String>();
            assert_eq!(
                text, "Answer\n\nSources:\n- [Docs](<https://example.com/docs>)\n",
                "{terminal}"
            );
            if let Some(completed) = events
                .iter()
                .position(|event| matches!(event, TurnStreamEvent::TurnCompleted { .. }))
            {
                assert!(
                    matches!(&events[completed - 1], TurnStreamEvent::AgentMessageDelta { delta, .. } if delta.contains("Sources:"))
                );
            }
        }
    }

    #[test]
    fn session_json_preserves_authoritative_rfc3339_update_time() {
        let session: mitsuro_client::SessionInfo = serde_json::from_value(serde_json::json!({
            "id": "session-1",
            "title": "Real session",
            "updated_at": "2026-08-11T03:42:58.324417490+00:00"
        }))
        .expect("typed session");

        let summary: crate::protocol::ThreadSummary =
            serde_json::from_value(session_json(&session)).expect("thread summary");

        assert_eq!(summary.updated_at, Some(1_786_419_778));
    }

    #[test]
    fn invalid_session_update_time_remains_unknown() {
        assert_eq!(parse_session_timestamp(""), None);
        assert_eq!(parse_session_timestamp("not-a-timestamp"), None);
    }

    #[test]
    fn persisted_tool_blocks_project_to_typed_activity_not_user_json() {
        let messages: Vec<MessageResponse> = serde_json::from_value(serde_json::json!([
            {
                "role": "user",
                "content": [{"type": "text", "text": "Inspect the workspace"}]
            },
            {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "Checking files", "signature": ""},
                    {
                        "type": "tool_use",
                        "id": "call-bash",
                        "name": "bash",
                        "input": {"command": "pwd", "cwd": "/workspace"}
                    },
                    {
                        "type": "tool_use",
                        "id": "call-read",
                        "name": "read",
                        "input": {"file_path": "/workspace/README.md"}
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-bash",
                        "output": {
                            "is_error": false,
                            "summary": "bash completed successfully",
                            "result": {"exit_code": 0, "output_preview": "/workspace"},
                            "tool": "bash"
                        }
                    },
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-read",
                        "output": {
                            "is_error": false,
                            "summary": "read returned 3 lines",
                            "tool": "read"
                        }
                    }
                ]
            },
            {
                "role": "assistant",
                "content": [{"type": "text", "text": "The workspace is ready."}]
            }
        ]))
        .expect("typed Mitsuro messages");

        let items = mitsuro_history_items(&messages);
        let kinds = items
            .iter()
            .filter_map(|item| item.get("type").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            vec![
                "userMessage",
                "reasoning",
                "commandExecution",
                "dynamicToolCall",
                "agentMessage"
            ]
        );
        assert_eq!(items[2]["status"], "completed");
        assert_eq!(items[2]["aggregatedOutput"], "bash completed successfully");
        assert_eq!(items[2]["exitCode"], 0);
        assert_eq!(items[3]["summary"], "read returned 3 lines");

        let thread = ThreadReadResponse {
            thread: serde_json::json!({"turns": [{"items": items}]}),
        };
        let transcript = thread.transcript_messages();
        assert_eq!(
            transcript
                .iter()
                .filter(|message| message.role == crate::protocol::TranscriptRole::User)
                .count(),
            1
        );
        assert!(transcript.iter().any(|message| {
            message.role == crate::protocol::TranscriptRole::CommandExecution
                && message.body.contains("bash completed successfully")
        }));
        assert!(transcript
            .iter()
            .all(|message| !message.body.contains("tool_use_id")));
    }

    #[test]
    fn unmatched_persisted_tool_result_is_bounded_activity_not_authored_chat() {
        let messages: Vec<MessageResponse> = serde_json::from_value(serde_json::json!([{
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "missing-call",
                "output": {
                    "is_error": true,
                    "summary": "Tool failed before its call was persisted",
                    "tool": "grep"
                }
            }]
        }]))
        .expect("typed Mitsuro messages");

        let items = mitsuro_history_items(&messages);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["type"], "dynamicToolCall");
        assert_eq!(items[0]["status"], "failed");
        assert_eq!(
            items[0]["summary"],
            "Tool failed before its call was persisted"
        );
    }

    #[test]
    fn product_permission_modes_map_to_exact_mitsuro_contract() {
        assert_eq!(
            mitsuro_permission_mode(Some("supervised")).unwrap(),
            Some(PermissionMode::Supervised)
        );
        assert_eq!(
            mitsuro_permission_mode(Some("autonomous")).unwrap(),
            Some(PermissionMode::Autonomous)
        );
        assert_eq!(mitsuro_permission_mode(None).unwrap(), None);
        assert!(mitsuro_permission_mode(Some("danger-full-access")).is_err());
    }

    #[test]
    fn product_work_modes_map_to_exact_mitsuro_contract() {
        assert_eq!(
            mitsuro_work_mode(Some("build")).unwrap(),
            Some(WorkMode::Build)
        );
        assert_eq!(
            mitsuro_work_mode(Some("plan")).unwrap(),
            Some(WorkMode::Plan)
        );
        assert_eq!(mitsuro_work_mode(None).unwrap(), None);
    }

    #[test]
    fn session_projection_keeps_exact_task_state_cursor_and_unknown_event() {
        let state: mitsuro_client::SessionStateResponse =
            serde_json::from_value(serde_json::json!({
                "id": "session-1",
                "agent_state": "working",
                "delegation_groups": [{
                    "delegation_group_id": "group-1",
                    "parent_tool_call_id": "tool-1",
                    "state": "running",
                    "execution_mode": "detached",
                    "parent_continuation_state": "pending",
                    "tasks": [{
                        "delegation_task_id": "task-1",
                        "task_key": "explore-api",
                        "role": "explore",
                        "state": "retrying",
                        "attempt_count": 2,
                        "updated_at": "2026-08-08T00:00:01Z"
                    }],
                    "updated_at": "2026-08-08T00:00:01Z"
                }],
                "delegation_events": [{
                    "event_id": 44,
                    "parent_session_id": "session-1",
                    "delegation_group_id": "group-1",
                    "delegation_task_id": "task-1",
                    "event_type": "future_scheduler_event",
                    "payload": {"lease_epoch": 9},
                    "created_at": "2026-08-08T00:00:01Z"
                }],
                "delegation_event_cursor": 44
            }))
            .expect("typed session state");

        let projection = session_delegation_projection(state);
        assert_eq!(projection.event_cursor, Some(44));
        assert_eq!(projection.groups[0].status, DelegationGroupStatus::Running);
        assert_eq!(
            projection.groups[0].execution,
            DelegationExecution::Detached
        );
        assert_eq!(
            projection.groups[0].parent_continuation,
            DelegationParentContinuationStatus::Pending
        );
        assert_eq!(
            projection.groups[0].tasks[0].status,
            DelegationTaskStatus::Retrying
        );
        assert_eq!(
            projection.events[0].kind,
            DurableDelegationEventKind::Other("future_scheduler_event".to_owned())
        );
        assert_eq!(projection.events[0].payload["lease_epoch"], 9);
    }

    #[test]
    fn live_progress_maps_to_backend_neutral_exact_lifecycle() {
        let payload: mitsuro_client::DelegatedProgressEvent =
            serde_json::from_value(serde_json::json!({
                "delegated_run_id": "run-1",
                "tool_call_id": "tool-1",
                "kind": "verify",
                "stage": "running",
                "parent_session_id": "session-1",
                "task_id": "task-1",
                "agent_name": "Verifier",
                "status": "leased",
                "tool_count": 3,
                "tokens": 144,
                "current_action": "checking tests",
                "completion_summary": null,
                "lines_added": 0,
                "lines_removed": 0,
                "completed_plan_task": null
            }))
            .expect("typed delegated progress");

        let progress = delegated_progress_projection(payload);
        assert_eq!(progress.kind, DelegationKind::Verify);
        assert_eq!(progress.stage, DelegationRunStage::Running);
        assert_eq!(progress.status, DelegationTaskStatus::Leased);
        assert_eq!(progress.current_action.as_deref(), Some("checking tests"));
    }

    #[test]
    fn unknown_stream_event_retains_original_payload() {
        let payload = serde_json::json!({"future": {"sequence": 7}});
        let event = unknown_mitsuro_stream_event("future_event".to_owned(), payload.clone());
        assert_eq!(event.method_name(), "mitsuro/future_event");
        match event {
            TurnStreamEvent::Other { params, .. } => assert_eq!(params, Some(payload)),
            other => panic!("expected forward-compatible event, got {other:?}"),
        }
    }

    #[test]
    fn extracts_codex_text_input_for_mitsuro_chat() {
        let input = vec![
            serde_json::json!({"type": "text", "text": "hello"}),
            serde_json::json!({"type": "text", "text": "world"}),
        ];
        assert_eq!(turn_input_text(&input), "hello\nworld");
    }

    #[tokio::test]
    async fn maps_local_image_input_to_real_mitsuro_content_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capture.png");
        std::fs::write(&path, b"png-bytes").unwrap();
        let input = vec![
            serde_json::json!({"type": "text", "text": "inspect"}),
            serde_json::json!({"type": "localImage", "path": path}),
        ];

        let (text, content) = turn_input_content(&input).await.unwrap();
        assert_eq!(text, "inspect");
        assert_eq!(content.len(), 1);
        assert_eq!(
            content[0],
            ContentBlock::Image {
                source: ImageSource::Base64 {
                    media_type: "image/png".to_owned(),
                    data: base64::engine::general_purpose::STANDARD.encode(b"png-bytes"),
                }
            }
        );
    }

    #[tokio::test]
    async fn rejects_codex_only_input_in_the_low_level_mitsuro_adapter() {
        for input in [
            serde_json::json!({"type": "localAudio", "path": "/tmp/recording.wav"}),
            serde_json::json!({
                "type": "skill",
                "name": "release",
                "path": "/skills/release/SKILL.md"
            }),
            serde_json::json!({
                "type": "mention",
                "name": "Cargo.toml",
                "path": "/workspace/Cargo.toml"
            }),
        ] {
            let error = turn_input_content(&[input])
                .await
                .expect_err("Mitsuro content has no matching input block");
            assert!(error
                .to_string()
                .contains("Mitsuro HTTP does not accept this Codex-only input type"));
        }
    }

    #[test]
    fn preserves_persisted_mitsuro_image_blocks_for_thread_hydration() {
        let content = serde_json::json!([
            {"type": "text", "text": "look"},
            {"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "cG5n"}}
        ]);
        assert_eq!(
            mitsuro_user_content(&content),
            vec![
                serde_json::json!({"type": "text", "text": "look"}),
                serde_json::json!({"type": "image", "url": "https://example.com/a.png"}),
                serde_json::json!({"type": "image", "url": "data:image/png;base64,cG5n"})
            ]
        );
    }

    #[test]
    fn maps_mitsuro_reasoning_efforts_without_inventing_unknown_values() {
        assert_eq!(
            mitsuro_reasoning_effort_name(mitsuro_client::ReasoningEffort::Minimal),
            Some("minimal")
        );
        assert_eq!(
            mitsuro_reasoning_effort_name(mitsuro_client::ReasoningEffort::XHigh),
            Some("xhigh")
        );
        assert_eq!(
            mitsuro_reasoning_effort_name(mitsuro_client::ReasoningEffort::Unknown),
            None
        );
    }

    #[tokio::test]
    async fn live_server_read_only_contract() {
        if std::env::var_os("MITSURO_RUN_SERVER_IT").is_none() {
            eprintln!("skip: set MITSURO_RUN_SERVER_IT=1 for local read-only contract check");
            return;
        }
        let backend = MitsuroServerBackend::from_env().expect("backend configuration");
        let init = backend.connect().await.expect("health/connect");
        assert_eq!(init.platform_family, "mitsuro-http");
        let sessions = backend
            .thread_list(ThreadListParams {
                limit: Some(3),
                ..Default::default()
            })
            .await
            .expect("session list");
        assert!(sessions.data.len() <= 3);
        let models = backend
            .model_list(ModelListParams {
                limit: Some(100),
                ..Default::default()
            })
            .await
            .expect("model list");
        assert!(!models.data.is_empty());
        let workspace = std::env::current_dir()
            .expect("current directory")
            .display()
            .to_string();
        let files = backend
            .fs_read_directory(FsReadDirectoryParams::new(workspace))
            .await
            .expect("file directory");
        assert!(!files.entries.is_empty());
        let skills = backend
            .skills_list(SkillsListParams::default())
            .await
            .expect("skills list");
        assert!(skills.skill_count() > 0);
        backend
            .mcp_server_status_list(ListMcpServerStatusParams::default())
            .await
            .expect("MCP list");
        backend
            .plugin_list(PluginListParams::default())
            .await
            .expect("extension list");
        backend
            .client()
            .list_processes()
            .await
            .expect("process list");
        let hive = backend.client().hive_current().await.expect("Hive current");
        if let Some(run) = hive.runs.first() {
            let detail = backend
                .client()
                .hive_session_status(&run.session_id)
                .await
                .expect("Hive session detail");
            assert_eq!(detail.session_id, run.session_id);
            assert_eq!(detail.session_type, "hive");
        }
        backend
            .client()
            .list_hive_schedules()
            .await
            .expect("Hive schedules");
        backend.disconnect().await.expect("disconnect");
    }

    /// Strict paid acceptance: a real Mitsuro SSE turn must stream text and complete.
    #[tokio::test]
    async fn live_server_streaming_turn() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        if std::env::var_os("MITSURO_RUN_LIVE_ACCEPTANCE").is_none() {
            eprintln!(
                "skip: set MITSURO_RUN_LIVE_ACCEPTANCE=1 to require a completed live Mitsuro turn"
            );
            return;
        }

        let backend = MitsuroServerBackend::from_env().expect("backend configuration");
        backend.connect().await.expect("health/connect");
        let started = backend
            .thread_start(ThreadStartParams {
                cwd: Some(
                    std::env::current_dir()
                        .expect("current directory")
                        .display()
                        .to_string(),
                ),
                ephemeral: Some(false),
                ..Default::default()
            })
            .await
            .expect("session create");
        let thread_id = started.summary().id;
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let bridge = Arc::new(LiveApprovalBridge::new());
        let responder_bridge = Arc::clone(&bridge);
        let responder_stop = Arc::new(AtomicBool::new(false));
        let responder_stop_thread = Arc::clone(&responder_stop);
        let responder = std::thread::spawn(move || {
            while !responder_stop_thread.load(AtomicOrdering::Acquire) {
                if responder_bridge.is_waiting() {
                    responder_bridge.submit(ApprovalChoice::Reject);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });

        let mut turn = TurnStartParams::text(
            thread_id.clone(),
            "Reply with exactly MITSURO_DESKTOP_ACCEPTANCE_OK. Do not use tools.",
        );
        turn.model = std::env::var("MITSURO_LIVE_ACCEPTANCE_MODEL")
            .ok()
            .filter(|model| !model.trim().is_empty());
        let outcome = backend
            .run_turn_streaming(turn, event_tx, bridge, Duration::from_secs(120))
            .await;
        responder_stop.store(true, AtomicOrdering::Release);
        responder.join().expect("approval responder join");
        let cleanup = backend
            .thread_delete(ThreadDeleteParams::new(thread_id))
            .await;
        backend.disconnect().await.expect("disconnect");

        let outcome = outcome.expect("completed Mitsuro streaming turn");
        cleanup.expect("delete acceptance session");
        assert!(
            outcome.completed,
            "Mitsuro turn did not emit a finish event"
        );
        assert!(
            event_rx.try_iter().any(|event| matches!(
                event,
                TurnStreamEvent::AgentMessageDelta { delta, .. } if !delta.is_empty()
            )),
            "Mitsuro turn emitted no assistant text delta"
        );
    }
}
