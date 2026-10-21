use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::{
    Client as AcpClient, ContentBlock as AcpContent, ContentChunk, CurrentModeUpdate,
    PermissionOption, PermissionOptionId, PermissionOptionKind, Plan, PlanEntry, PlanEntryPriority,
    PlanEntryStatus, RequestPermissionOutcome, RequestPermissionRequest, SessionNotification,
    SessionUpdate, StopReason, TextContent, ToolCall, ToolCallId, ToolCallStatus, ToolCallUpdate,
    ToolCallUpdateFields,
};

use crate::agent::loop_events::{LoopEvent, LoopStopReason};
use crate::agent::{LoopInput, OrchestratorServices, RunBudget, RunProvenance, RunSpecBuilder};
use crate::ai::client::CallOptions;
use crate::ai::types::{Content, Role};
use crate::skills::SkillsManager;
use crate::storage::SessionType;
use crate::storage::{Database, DelegationStore};

use super::content::convert_acp_content;
use super::PromptProcessor;
use crate::acp::error::AcpError;
use crate::acp::session::SessionState;
use crate::acp::source_references::{cited_text_content, SourceReferences};
use crate::acp::tools::{
    create_tool_call_complete, create_tool_call_failed, create_tool_call_start,
    text_to_tool_content, tool_name_to_kind,
};

impl PromptProcessor {
    /// Project canonical durable delegation state/events through standard ACP
    /// tool calls. The session-scoped cursor fences reconnect and polling
    /// duplicates while the storage query enforces parent-session ownership.
    pub(crate) async fn replay_delegation_state<C: AcpClient>(
        &self,
        session: &SessionState,
        connection: &C,
    ) -> Result<(), AcpError> {
        let store = DelegationStore::new(
            Database::new(&self.db_path)
                .map_err(|error| AcpError::InternalError(error.to_string()))?,
        );
        let (initialized, cursor) = {
            let projection = session.delegation_projection.lock().map_err(|_| {
                AcpError::InternalError("ACP delegation projection poisoned".into())
            })?;
            (projection.is_initialized(), projection.cursor())
        };

        let updates = if !initialized {
            // Fence the snapshot before reading group state. Events committed
            // after this cursor remain eligible for the next replay instead of
            // being skipped behind a newer cursor paired with stale groups.
            let latest_cursor = store
                .list_latest_session_events(&session.id.to_string(), 1)
                .map_err(|error| AcpError::InternalError(error.to_string()))?
                .last()
                .map(|event| event.event_id)
                .unwrap_or(0);
            let groups = store
                .list_groups_for_session(&session.id.to_string(), 1000)
                .map_err(|error| AcpError::InternalError(error.to_string()))?;
            session
                .delegation_projection
                .lock()
                .map_err(|_| AcpError::InternalError("ACP delegation projection poisoned".into()))?
                .hydrate(&groups, latest_cursor)
        } else {
            let events = store
                .list_session_events_after(&session.id.to_string(), cursor, 1000)
                .map_err(|error| AcpError::InternalError(error.to_string()))?;
            let mut projection = session.delegation_projection.lock().map_err(|_| {
                AcpError::InternalError("ACP delegation projection poisoned".into())
            })?;
            let mut updates = Vec::new();
            for event in events {
                // Resolve only through the event's session-filtered group id;
                // projection never accepts an event queried for another owner.
                let group = store
                    .get_group(&event.delegation_group_id)
                    .map_err(|error| AcpError::InternalError(error.to_string()))?;
                if group
                    .as_ref()
                    .is_some_and(|group| group.parent_session_id != session.id.to_string())
                {
                    continue;
                }
                updates.extend(projection.apply_event(&event, group.as_ref()));
            }
            updates
        };

        for update in updates {
            send_update(session, connection, update).await?;
        }
        Ok(())
    }

    /// Process an ACP prompt through Mitsuro's canonical agentic orchestrator.
    pub async fn process_prompt<C: AcpClient>(
        &self,
        session: &SessionState,
        prompt: Vec<AcpContent>,
        connection: &C,
    ) -> Result<StopReason, AcpError> {
        let ai_client = session
            .ai_client()
            .await
            .or_else(|| self.default_ai_client())
            .ok_or_else(|| {
                AcpError::NotAuthenticated(
                    "AI client not initialized - select a model first".to_string(),
                )
            })?;

        let initial_content: Vec<Content> =
            prompt.into_iter().filter_map(convert_acp_content).collect();
        if initial_content.is_empty() {
            return Err(AcpError::InvalidRequest(
                "prompt contained no supported content".to_string(),
            ));
        }
        session
            .add_user_message_content(initial_content.clone())
            .await;

        let mut conversation = session.history().await;
        if let Some(recovery_notice) = session.take_recovery_notice().await {
            let insert_at = conversation
                .iter()
                .rposition(|message| message.role == Role::User)
                .unwrap_or(conversation.len());
            conversation.insert(insert_at, recovery_notice);
        }

        let workspace_root = canonical_acp_workspace_root(session)?;
        let tool_defs = self.tools.get_ai_tools().await;
        let options = CallOptions {
            tools: (!tool_defs.is_empty()).then_some(tool_defs),
            enable_caching: true,
            session_id: Some(session.id.to_string()),
            codex_parallel_tool_calls: true,
            ..Default::default()
        };
        let mode_aware_code_tools = options.tools.is_some();
        let services = OrchestratorServices {
            ai_client,
            tool_registry: Arc::clone(&self.tools),
            process_registry: Arc::clone(&self.process_registry),
            db_path: self.db_path.clone(),
            skills_manager: Arc::new(tokio::sync::RwLock::new(SkillsManager::with_defaults(
                &workspace_root,
            ))),
        };
        let run_spec = RunSpecBuilder::new(
            RunProvenance::Acp,
            session.id.to_string(),
            workspace_root.clone(),
            SessionType::Code,
        )
        .project_dir(Some(workspace_root))
        .permission_mode(session.permission_mode().await)
        .run_budget(
            self.agent_config
                .acp_max_turns()
                .map(RunBudget::with_max_turns),
        )
        .stream_idle_timeout(self.agent_config.stream_idle_timeout())
        .initial_work_mode(session.work_mode().await)
        .mode_aware_code_tools(mode_aware_code_tools)
        .generate_title(false)
        .call_options(options)
        .build(services.ai_client.as_ref())
        .map_err(|error| AcpError::InvalidRequest(error.to_string()))?;

        let (mut event_rx, input_tx) = run_spec.start(services, conversation);
        session.set_active_input(input_tx.clone());

        let result = self
            .bridge_loop_events(session, connection, &mut event_rx, &input_tx)
            .await;
        if result.is_err() {
            // If the ACP connection disappears or rejects an update, do not leave
            // the canonical loop working invisibly in the background.
            let _ = input_tx.send(LoopInput::Cancel);
        }
        session.clear_active_input();

        if let Some(storage_session_id) = session.get_storage_session_id().await {
            if let Err(error) = session.load_from_storage(&storage_session_id).await {
                tracing::warn!(
                    session_id = %session.id,
                    "Failed to refresh ACP history after canonical run: {}",
                    error
                );
            }
        }

        result
    }

    async fn bridge_loop_events<C: AcpClient>(
        &self,
        session: &SessionState,
        connection: &C,
        event_rx: &mut tokio::sync::mpsc::UnboundedReceiver<LoopEvent>,
        input_tx: &tokio::sync::mpsc::UnboundedSender<LoopInput>,
    ) -> Result<StopReason, AcpError> {
        let mut tool_output = HashMap::<String, String>::new();
        let mut delegated_tools = AcpDelegatedToolProjection::default();
        let mut last_error = None::<String>;
        let mut source_references = SourceReferences::default();

        self.replay_delegation_state(session, connection).await?;
        // ACP has no delegation push extension. A 400ms storage cadence keeps
        // live lifecycle updates responsive without opening/querying SQLite at
        // token-stream frequency; tool-result and finish boundaries replay
        // immediately below.
        let mut delegation_tick = tokio::time::interval(std::time::Duration::from_millis(400));
        delegation_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let event = tokio::select! {
                event = event_rx.recv() => event,
                _ = delegation_tick.tick() => {
                    if let Err(error) = self.replay_delegation_state(session, connection).await {
                        flush_source_references(session, connection, &mut source_references).await?;
                        return Err(error);
                    }
                    continue;
                }
            };
            let Some(event) = event else {
                break;
            };
            if !matches!(
                &event,
                LoopEvent::TextDelta { .. }
                    | LoopEvent::TextDeltaWithCitations { .. }
                    | LoopEvent::Usage { .. }
            ) {
                flush_source_references(session, connection, &mut source_references).await?;
            }
            if matches!(&event, LoopEvent::TurnComplete { .. }) {
                source_references.end_response();
            }
            match event {
                LoopEvent::TextDelta { delta } => {
                    send_text_update(session, connection, false, delta).await?;
                }
                LoopEvent::TextDeltaWithCitations { delta, citations } => {
                    source_references.extend(&citations);
                    let content = cited_text_content(delta, &citations);
                    send_update(
                        session,
                        connection,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(AcpContent::Text(
                            content,
                        ))),
                    )
                    .await?;
                }
                LoopEvent::ThinkingDelta { thinking } => {
                    send_text_update(session, connection, true, thinking).await?;
                }
                LoopEvent::ToolCallStart { id, name } => {
                    let tool_call =
                        ToolCall::new(ToolCallId::from(id), format!("Running {}", name))
                            .kind(tool_name_to_kind(&name));
                    send_update(session, connection, SessionUpdate::ToolCall(tool_call)).await?;
                }
                LoopEvent::ToolCallComplete {
                    id,
                    name,
                    arguments,
                } => {
                    let update = create_tool_call_start(&id, &name, arguments);
                    send_update(session, connection, SessionUpdate::ToolCallUpdate(update)).await?;
                }
                LoopEvent::ToolExecuting { id, .. } => {
                    let fields = ToolCallUpdateFields::new().status(ToolCallStatus::InProgress);
                    send_update(
                        session,
                        connection,
                        SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                            ToolCallId::from(id),
                            fields,
                        )),
                    )
                    .await?;
                }
                LoopEvent::ToolOutputDelta { id, delta } => {
                    let output = tool_output.entry(id.clone()).or_default();
                    output.push_str(&delta);
                    let fields =
                        ToolCallUpdateFields::new().content(vec![text_to_tool_content(output)]);
                    send_update(
                        session,
                        connection,
                        SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                            ToolCallId::from(id),
                            fields,
                        )),
                    )
                    .await?;
                }
                LoopEvent::ToolResult {
                    id,
                    output,
                    is_error,
                } => {
                    tool_output.remove(&id);
                    let update = if is_error {
                        create_tool_call_failed(&id, &output)
                    } else {
                        create_tool_call_complete(&id, vec![text_to_tool_content(&output)])
                    };
                    send_update(session, connection, SessionUpdate::ToolCallUpdate(update)).await?;
                    self.replay_delegation_state(session, connection).await?;
                }
                LoopEvent::ToolApprovalRequired {
                    id,
                    name,
                    arguments,
                } => {
                    let decision =
                        request_tool_permission(session, connection, &id, &name, arguments).await?;
                    let input = match decision {
                        ToolPermissionDecision::Approved => LoopInput::ToolApproval {
                            tool_call_id: id,
                            approved: true,
                        },
                        ToolPermissionDecision::Denied => LoopInput::ToolApproval {
                            tool_call_id: id,
                            approved: false,
                        },
                        ToolPermissionDecision::Cancelled => LoopInput::Cancel,
                    };
                    input_tx.send(input).map_err(|_| {
                        AcpError::InternalError(
                            "canonical agent loop closed during approval".to_string(),
                        )
                    })?;
                }
                LoopEvent::ToolApproved { id } => {
                    let fields = ToolCallUpdateFields::new().status(ToolCallStatus::InProgress);
                    send_update(
                        session,
                        connection,
                        SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                            ToolCallId::from(id),
                            fields,
                        )),
                    )
                    .await?;
                }
                LoopEvent::ToolDenied { id } => {
                    let update = create_tool_call_failed(&id, "Tool execution denied by user");
                    send_update(session, connection, SessionUpdate::ToolCallUpdate(update)).await?;
                }
                LoopEvent::PlanUpdate { tasks } => {
                    let entries = tasks
                        .into_iter()
                        .map(|task| {
                            PlanEntry::new(
                                task.description,
                                PlanEntryPriority::Medium,
                                if task.completed {
                                    PlanEntryStatus::Completed
                                } else {
                                    PlanEntryStatus::Pending
                                },
                            )
                        })
                        .collect();
                    send_update(session, connection, SessionUpdate::Plan(Plan::new(entries)))
                        .await?;
                }
                LoopEvent::WorkflowUpdated { .. } => {
                    // ACP receives the projected PlanUpdate event alongside
                    // this revision signal; native clients refresh via API.
                }
                LoopEvent::ModeChange { mode, .. } => {
                    let acp_mode = canonical_mode_to_acp(&mode);
                    session.set_mode(Some(acp_mode.to_string())).await;
                    send_update(
                        session,
                        connection,
                        SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(acp_mode)),
                    )
                    .await?;
                }
                LoopEvent::ServerToolStart { id, name } => {
                    let call = ToolCall::new(ToolCallId::from(id), format!("Running {}", name))
                        .kind(tool_name_to_kind(&name));
                    send_update(session, connection, SessionUpdate::ToolCall(call)).await?;
                }
                LoopEvent::ServerToolComplete { id, .. } => {
                    let update = create_tool_call_complete(&id, Vec::new());
                    send_update(session, connection, SessionUpdate::ToolCallUpdate(update)).await?;
                }
                LoopEvent::ServerToolError {
                    tool_use_id,
                    error_code,
                } => {
                    let update = create_tool_call_failed(&tool_use_id, &error_code);
                    send_update(session, connection, SessionUpdate::ToolCallUpdate(update)).await?;
                }
                LoopEvent::UserMessage { message, .. } => {
                    send_text_update(session, connection, false, message).await?;
                }
                LoopEvent::Error { error } => {
                    last_error = Some(error);
                }
                LoopEvent::Finished { stop_reason, .. } => {
                    self.replay_delegation_state(session, connection).await?;
                    return convert_loop_stop_reason(stop_reason, last_error);
                }
                LoopEvent::AgentBackgroundStarted {
                    delegated_run_id,
                    agent_type,
                    description,
                } => {
                    self.replay_delegation_state(session, connection).await?;
                    if !canonical_delegation_group_projected(session, &delegated_run_id) {
                        if let Some(update) =
                            delegated_tools.started(delegated_run_id, agent_type, description)
                        {
                            send_update(session, connection, update).await?;
                        }
                    }
                }
                LoopEvent::AgentBackgroundCompleted {
                    delegated_run_id,
                    agent_type,
                    success,
                    summary,
                } => {
                    self.replay_delegation_state(session, connection).await?;
                    if !canonical_delegation_group_projected(session, &delegated_run_id) {
                        if let Some(update) = delegated_tools.completed(
                            delegated_run_id,
                            agent_type,
                            success,
                            summary,
                        ) {
                            send_update(session, connection, update).await?;
                        }
                    }
                }
                LoopEvent::ToolCallPreparing { .. }
                | LoopEvent::AwaitingInput { .. }
                | LoopEvent::SteeringInjected { .. }
                | LoopEvent::PlanComplete { .. }
                | LoopEvent::Usage { .. }
                | LoopEvent::TurnComplete { .. }
                | LoopEvent::RunBudgetResolved { .. }
                | LoopEvent::ProviderRequestPrepared { .. }
                | LoopEvent::MicrocompactionApplied { .. }
                | LoopEvent::ProgressGuard { .. }
                | LoopEvent::TickInjected { .. }
                | LoopEvent::AgentSleeping { .. }
                | LoopEvent::SessionPinched { .. }
                | LoopEvent::ContextCompactionStarted { .. }
                | LoopEvent::ContextCompacted { .. }
                | LoopEvent::ThinkingComplete { .. }
                | LoopEvent::TitleGenerated { .. }
                | LoopEvent::WebSearchResults { .. }
                | LoopEvent::WebFetchResult { .. }
                | LoopEvent::ClassifierDecision { .. } => {}
            }
        }

        flush_source_references(session, connection, &mut source_references).await?;
        Err(AcpError::InternalError(
            "canonical agent loop ended without a terminal event".to_string(),
        ))
    }
}

async fn flush_source_references<C: AcpClient>(
    session: &SessionState,
    connection: &C,
    references: &mut SourceReferences,
) -> Result<(), AcpError> {
    if let Some(text) = references.take_text() {
        send_text_update(session, connection, false, text).await?;
    }
    Ok(())
}

fn canonical_delegation_group_projected(session: &SessionState, group_id: &str) -> bool {
    session
        .delegation_projection
        .lock()
        .map(|projection| projection.contains_group(group_id))
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcpDelegatedToolState {
    Running,
    Terminal,
}

/// ACP projection of the canonical aggregate background-agent lifecycle.
///
/// The core loop remains the event authority. This adapter only translates its
/// existing start/completion pair into standard ACP tool-call notifications and
/// fences duplicate or reordered delivery so each aggregate has one terminal
/// presentation update.
#[derive(Default)]
struct AcpDelegatedToolProjection {
    runs: HashMap<String, AcpDelegatedToolState>,
}

impl AcpDelegatedToolProjection {
    fn started(
        &mut self,
        delegated_run_id: String,
        agent_type: String,
        description: String,
    ) -> Option<SessionUpdate> {
        match self.runs.entry(delegated_run_id.clone()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(entry) => {
                entry.insert(AcpDelegatedToolState::Running);
                Some(SessionUpdate::ToolCall(
                    ToolCall::new(
                        delegated_tool_call_id(&delegated_run_id),
                        delegated_tool_title(&agent_type),
                    )
                    .kind(tool_name_to_kind("agent"))
                    .status(ToolCallStatus::InProgress)
                    .content(vec![text_to_tool_content(&format!(
                        "Running in background: {description}"
                    ))]),
                ))
            }
        }
    }

    fn completed(
        &mut self,
        delegated_run_id: String,
        agent_type: String,
        success: bool,
        summary: String,
    ) -> Option<SessionUpdate> {
        let tool_call_id = delegated_tool_call_id(&delegated_run_id);
        match self.runs.entry(delegated_run_id) {
            Entry::Occupied(mut entry) => {
                if *entry.get() == AcpDelegatedToolState::Terminal {
                    return None;
                }
                entry.insert(AcpDelegatedToolState::Terminal);
                Some(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
                    tool_call_id,
                    delegated_terminal_fields(&agent_type, success, summary),
                )))
            }
            Entry::Vacant(entry) => {
                // A reconnect or bounded transport may observe completion after
                // losing the start. A terminal ToolCall is valid ACP and avoids
                // sending a synthetic start followed by a second terminal event.
                entry.insert(AcpDelegatedToolState::Terminal);
                Some(SessionUpdate::ToolCall(
                    ToolCall::new(tool_call_id, delegated_terminal_title(&agent_type, success))
                        .kind(tool_name_to_kind("agent"))
                        .status(if success {
                            ToolCallStatus::Completed
                        } else {
                            ToolCallStatus::Failed
                        })
                        .content(vec![text_to_tool_content(&summary)]),
                ))
            }
        }
    }
}

fn delegated_tool_call_id(delegated_run_id: &str) -> ToolCallId {
    ToolCallId::from(format!("delegated:{delegated_run_id}"))
}

fn delegated_tool_title(agent_type: &str) -> String {
    format!("Agent {agent_type}")
}

fn delegated_terminal_title(agent_type: &str, success: bool) -> String {
    format!(
        "Agent {agent_type} {}",
        if success { "completed" } else { "failed" }
    )
}

fn delegated_terminal_fields(
    agent_type: &str,
    success: bool,
    summary: String,
) -> ToolCallUpdateFields {
    ToolCallUpdateFields::new()
        .title(delegated_terminal_title(agent_type, success))
        .status(if success {
            ToolCallStatus::Completed
        } else {
            ToolCallStatus::Failed
        })
        .content(vec![text_to_tool_content(&summary)])
}

async fn send_update<C: AcpClient>(
    session: &SessionState,
    connection: &C,
    update: SessionUpdate,
) -> Result<(), AcpError> {
    connection
        .session_notification(SessionNotification::new(session.id.clone(), update))
        .await
        .map_err(|error| AcpError::ProtocolError(error.to_string()))
}

async fn send_text_update<C: AcpClient>(
    session: &SessionState,
    connection: &C,
    thinking: bool,
    text: String,
) -> Result<(), AcpError> {
    let chunk = ContentChunk::new(AcpContent::Text(TextContent::new(text)));
    let update = if thinking {
        SessionUpdate::AgentThoughtChunk(chunk)
    } else {
        SessionUpdate::AgentMessageChunk(chunk)
    };
    send_update(session, connection, update).await
}

async fn request_tool_permission<C: AcpClient>(
    session: &SessionState,
    connection: &C,
    id: &str,
    name: &str,
    arguments: serde_json::Value,
) -> Result<ToolPermissionDecision, AcpError> {
    let request = RequestPermissionRequest::new(
        session.id.clone(),
        ToolCallUpdate::new(
            ToolCallId::from(id.to_string()),
            ToolCallUpdateFields::new()
                .title(format!("Run {}", name))
                .kind(tool_name_to_kind(name))
                .raw_input(arguments),
        ),
        vec![
            PermissionOption::new(
                PermissionOptionId::new("allow-once"),
                "Allow once",
                PermissionOptionKind::AllowOnce,
            ),
            PermissionOption::new(
                PermissionOptionId::new("reject-once"),
                "Reject",
                PermissionOptionKind::RejectOnce,
            ),
        ],
    );

    let response = connection
        .request_permission(request)
        .await
        .map_err(|error| AcpError::ProtocolError(error.to_string()))?;
    Ok(match response.outcome {
        RequestPermissionOutcome::Selected(selected)
            if selected.option_id.0.as_ref().starts_with("allow") =>
        {
            ToolPermissionDecision::Approved
        }
        RequestPermissionOutcome::Selected(_) => ToolPermissionDecision::Denied,
        RequestPermissionOutcome::Cancelled => ToolPermissionDecision::Cancelled,
        _ => ToolPermissionDecision::Denied,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolPermissionDecision {
    Approved,
    Denied,
    Cancelled,
}

pub(super) fn canonical_acp_workspace_root(session: &SessionState) -> Result<PathBuf, AcpError> {
    session.cwd.canonicalize().map_err(|error| {
        AcpError::InvalidRequest(format!(
            "session working directory '{}' is not accessible: {}",
            session.cwd.display(),
            error
        ))
    })
}

fn canonical_mode_to_acp(mode: &str) -> &'static str {
    match mode {
        "plan" => "plan",
        _ => "code",
    }
}

pub(super) fn convert_loop_stop_reason(
    reason: LoopStopReason,
    last_error: Option<String>,
) -> Result<StopReason, AcpError> {
    match reason {
        LoopStopReason::Completed
        | LoopStopReason::AwaitingInput
        | LoopStopReason::Sleeping
        | LoopStopReason::Pinched => Ok(StopReason::EndTurn),
        LoopStopReason::BudgetExhausted | LoopStopReason::LoopGuardTriggered => {
            Ok(StopReason::MaxTurnRequests)
        }
        LoopStopReason::UserAbort => Ok(StopReason::Cancelled),
        LoopStopReason::ProviderError
        | LoopStopReason::StreamIdleTimeout
        | LoopStopReason::PinchFailed => {
            Err(AcpError::AiClientError(last_error.unwrap_or_else(|| {
                format!("canonical agent loop stopped: {:?}", reason)
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cited_sources_survive_tool_finish_cancel_error_and_closed_stream_boundaries() {
        use crate::acp::{create_notification_channel, AcpOutbound};
        use crate::ai::types::Citation;

        for terminal in [
            Some(LoopStopReason::Completed),
            Some(LoopStopReason::UserAbort),
            Some(LoopStopReason::ProviderError),
            None,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let db_path = directory.path().join("acp.db");
            Database::new(&db_path).unwrap();
            let processor =
                PromptProcessor::with_db_path(Arc::new(crate::tools::ToolRegistry::new()), db_path);
            let session = SessionState::new(
                agent_client_protocol::SessionId::from("citation-test"),
                Some(directory.path().to_path_buf()),
                None,
            );
            let (connection, mut notifications) = create_notification_channel();
            let (events, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
            let (inputs, _input_rx) = tokio::sync::mpsc::unbounded_channel();
            let source = Citation {
                url: "https://example.com/first".into(),
                title: "First source".into(),
                cited_text: "Answer".into(),
            };
            events
                .send(LoopEvent::TextDeltaWithCitations {
                    delta: "Answer".into(),
                    citations: vec![source.clone()],
                })
                .unwrap();
            events
                .send(LoopEvent::TextDeltaWithCitations {
                    delta: String::new(),
                    citations: vec![source],
                })
                .unwrap();
            events
                .send(LoopEvent::ToolCallStart {
                    id: "read-1".into(),
                    name: "read".into(),
                })
                .unwrap();
            events
                .send(LoopEvent::TextDeltaWithCitations {
                    delta: "More evidence".into(),
                    citations: vec![Citation {
                        url: "https://example.com/second".into(),
                        title: "Second source".into(),
                        cited_text: "More evidence".into(),
                    }],
                })
                .unwrap();
            if let Some(stop_reason) = terminal.clone() {
                events
                    .send(LoopEvent::Finished {
                        session_id: "citation-test".into(),
                        stop_reason,
                    })
                    .unwrap();
            }
            drop(events);

            let result = processor
                .bridge_loop_events(&session, &connection, &mut event_rx, &inputs)
                .await;
            assert_eq!(
                result.is_ok(),
                matches!(
                    terminal,
                    Some(LoopStopReason::Completed | LoopStopReason::UserAbort)
                )
            );
            let mut text = String::new();
            let mut order = Vec::new();
            let mut annotated_chunks = 0;
            while let Ok(AcpOutbound::Notification(notification)) = notifications.try_recv() {
                match notification.update {
                    SessionUpdate::AgentMessageChunk(ContentChunk {
                        content: AcpContent::Text(content),
                        ..
                    }) => {
                        if content.meta.is_some() {
                            annotated_chunks += 1;
                        }
                        order.push(if content.text.contains("Sources:") {
                            "sources"
                        } else {
                            "text"
                        });
                        text.push_str(&content.text);
                    }
                    SessionUpdate::ToolCall(_) => order.push("tool"),
                    _ => {}
                }
            }
            assert_eq!(annotated_chunks, 3);
            assert_eq!(text.matches("https://example.com/first").count(), 1);
            assert_eq!(text.matches("https://example.com/second").count(), 1);
            assert_eq!(
                order,
                ["text", "text", "sources", "tool", "text", "sources"]
            );
        }
    }

    fn expect_tool_call(update: SessionUpdate) -> ToolCall {
        match update {
            SessionUpdate::ToolCall(tool_call) => tool_call,
            other => panic!("expected tool call, got {other:?}"),
        }
    }

    fn expect_tool_call_update(update: SessionUpdate) -> ToolCallUpdate {
        match update {
            SessionUpdate::ToolCallUpdate(tool_call) => tool_call,
            other => panic!("expected tool call update, got {other:?}"),
        }
    }

    #[test]
    fn canonical_stop_reasons_preserve_acp_semantics() {
        assert_eq!(
            convert_loop_stop_reason(LoopStopReason::Completed, None).unwrap(),
            StopReason::EndTurn
        );
        assert_eq!(
            convert_loop_stop_reason(LoopStopReason::BudgetExhausted, None).unwrap(),
            StopReason::MaxTurnRequests
        );
        assert_eq!(
            convert_loop_stop_reason(LoopStopReason::UserAbort, None).unwrap(),
            StopReason::Cancelled
        );
        assert!(convert_loop_stop_reason(
            LoopStopReason::ProviderError,
            Some("provider failed".to_string())
        )
        .is_err());
    }

    #[test]
    fn background_delegation_projects_one_running_call_and_one_terminal_update() {
        let mut projection = AcpDelegatedToolProjection::default();

        let started = expect_tool_call(
            projection
                .started(
                    "run-1".to_string(),
                    "build".to_string(),
                    "Repair API and UI in parallel".to_string(),
                )
                .expect("first start should be visible"),
        );
        assert_eq!(started.tool_call_id.0.as_ref(), "delegated:run-1");
        assert_eq!(started.title, "Agent build");
        assert_eq!(started.status, ToolCallStatus::InProgress);
        assert_eq!(started.content.len(), 1);
        assert!(projection
            .started(
                "run-1".to_string(),
                "build".to_string(),
                "duplicate".to_string(),
            )
            .is_none());

        let completed = expect_tool_call_update(
            projection
                .completed(
                    "run-1".to_string(),
                    "build".to_string(),
                    true,
                    "Both components passed validation".to_string(),
                )
                .expect("first completion should be visible"),
        );
        assert_eq!(completed.tool_call_id.0.as_ref(), "delegated:run-1");
        assert_eq!(completed.fields.status, Some(ToolCallStatus::Completed));
        assert_eq!(
            completed.fields.title.as_deref(),
            Some("Agent build completed")
        );
        assert!(projection
            .completed(
                "run-1".to_string(),
                "build".to_string(),
                true,
                "duplicate".to_string(),
            )
            .is_none());
    }

    #[test]
    fn orphan_background_completion_is_one_terminal_tool_call() {
        let mut projection = AcpDelegatedToolProjection::default();

        let failed = expect_tool_call(
            projection
                .completed(
                    "run-late".to_string(),
                    "verify".to_string(),
                    false,
                    "Validation failed".to_string(),
                )
                .expect("orphan completion should remain visible"),
        );
        assert_eq!(failed.tool_call_id.0.as_ref(), "delegated:run-late");
        assert_eq!(failed.title, "Agent verify failed");
        assert_eq!(failed.status, ToolCallStatus::Failed);
        assert_eq!(failed.content.len(), 1);
        assert!(projection
            .completed(
                "run-late".to_string(),
                "verify".to_string(),
                false,
                "duplicate".to_string(),
            )
            .is_none());
        assert!(projection
            .started(
                "run-late".to_string(),
                "verify".to_string(),
                "late start".to_string(),
            )
            .is_none());
    }
}
