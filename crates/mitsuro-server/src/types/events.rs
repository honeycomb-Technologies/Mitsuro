use mitsuro_core::agent::subagent::AgentProgressStatus;
use mitsuro_core::agent::{
    DelegatedProgressEvent, DelegatedRunStage as CoreDelegatedRunStage,
    DelegatedToolKind as CoreDelegatedToolKind, ProgressGuardTelemetry, ProviderRequestSnapshot,
    RunBudgetSource,
};
use mitsuro_core::ai::types::{Citation, WebFetchContent, WebSearchResult};
use mitsuro_core::storage::{DelegationEventRecord, RuntimeTraceEvent};
use serde::{Deserialize, Serialize};

// ============================================================================
// Plan Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanItem {
    pub content: String,
    pub completed: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedToolKind {
    Explore,
    Plan,
    Verify,
    Build,
}

impl From<CoreDelegatedToolKind> for DelegatedToolKind {
    fn from(value: CoreDelegatedToolKind) -> Self {
        match value {
            CoreDelegatedToolKind::Explore => Self::Explore,
            CoreDelegatedToolKind::Plan => Self::Plan,
            CoreDelegatedToolKind::Verify => Self::Verify,
            CoreDelegatedToolKind::Build => Self::Build,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedProgressStatus {
    Created,
    Queued,
    Leased,
    Running,
    Retrying,
    Complete,
    Degraded,
    Cancelled,
    Failed,
}

impl From<&AgentProgressStatus> for DelegatedProgressStatus {
    fn from(value: &AgentProgressStatus) -> Self {
        match value {
            AgentProgressStatus::Created => Self::Created,
            AgentProgressStatus::Queued => Self::Queued,
            AgentProgressStatus::Leased => Self::Leased,
            AgentProgressStatus::Running => Self::Running,
            AgentProgressStatus::Retrying => Self::Retrying,
            AgentProgressStatus::Complete => Self::Complete,
            AgentProgressStatus::Degraded => Self::Degraded,
            AgentProgressStatus::Failed => Self::Failed,
            AgentProgressStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl DelegatedProgressStatus {
    pub(crate) fn from_progress(
        status: &AgentProgressStatus,
        stage: CoreDelegatedRunStage,
    ) -> Self {
        match (status, stage) {
            (AgentProgressStatus::Failed, CoreDelegatedRunStage::Degraded) => Self::Degraded,
            (AgentProgressStatus::Failed, CoreDelegatedRunStage::Cancelled) => Self::Cancelled,
            _ => Self::from(status),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedRunStage {
    Created,
    Running,
    Synthesizing,
    Complete,
    Degraded,
    Failed,
    Cancelled,
}

impl From<CoreDelegatedRunStage> for DelegatedRunStage {
    fn from(value: CoreDelegatedRunStage) -> Self {
        match value {
            CoreDelegatedRunStage::Created => Self::Created,
            CoreDelegatedRunStage::Running => Self::Running,
            CoreDelegatedRunStage::Synthesizing => Self::Synthesizing,
            CoreDelegatedRunStage::Complete => Self::Complete,
            CoreDelegatedRunStage::Degraded => Self::Degraded,
            CoreDelegatedRunStage::Failed => Self::Failed,
            CoreDelegatedRunStage::Cancelled => Self::Cancelled,
        }
    }
}

// ============================================================================
// Agentic SSE Events
// ============================================================================

/// Events sent to the client during agentic chat loop
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgenticEvent {
    /// Text content delta from AI
    TextDelta { delta: String },
    /// Text content delta with citations
    TextDeltaWithCitations {
        delta: String,
        citations: Vec<Citation>,
    },
    /// Extended thinking delta
    ThinkingDelta { thinking: String },
    /// Extended thinking block completed
    ThinkingComplete { thinking: String, signature: String },
    /// AI is starting a tool call
    ToolCallStart { id: String, name: String },
    /// AI is still streaming a large tool input. Arguments remain redacted.
    ToolCallPreparing {
        id: String,
        name: String,
        received_bytes: usize,
    },
    /// Tool call complete with arguments
    ToolCallComplete {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },
    /// Server is executing a tool
    ToolExecuting { id: String, name: String },
    /// Streaming output delta from a tool (e.g., bash)
    ToolOutputDelta { id: String, delta: String },
    /// Live delegated progress from explore/build sub-agents.
    DelegatedProgress {
        delegated_run_id: String,
        tool_call_id: String,
        kind: DelegatedToolKind,
        stage: DelegatedRunStage,
        parent_session_id: String,
        task_id: String,
        agent_name: String,
        status: DelegatedProgressStatus,
        tool_count: usize,
        tokens: usize,
        current_action: Option<String>,
        completion_summary: Option<String>,
        lines_added: usize,
        lines_removed: usize,
        completed_plan_task: Option<String>,
    },
    /// One append-only, durable delegation lifecycle event. This is an
    /// immediate delivery optimization; reconnecting clients replay the same
    /// event IDs from the session-state endpoint.
    DelegationEvent { event: DelegationEventRecord },
    /// Tool execution result
    ToolResult {
        id: String,
        output: String,
        is_error: bool,
    },
    /// Server-side tool started
    ServerToolStart { id: String, name: String },
    /// Server-side tool completed
    ServerToolComplete { id: String, name: String },
    /// Server-side web search results
    WebSearchResults {
        tool_use_id: String,
        results: Vec<WebSearchResult>,
    },
    /// Server-side web fetch result
    WebFetchResult {
        tool_use_id: String,
        content: WebFetchContent,
    },
    /// Server-side tool error
    ServerToolError {
        tool_use_id: String,
        error_code: String,
    },
    /// Waiting for user input (AskUserQuestion)
    AwaitingInput {
        tool_call_id: String,
        tool_name: String,
    },
    /// Mode change (set_work_mode / enter_plan_mode tools)
    ModeChange {
        mode: String,
        reason: Option<String>,
    },
    /// Plan tasks update - sent when plan is detected
    PlanUpdate { items: Vec<PlanItem> },
    /// Canonical durable Goal/plan state changed.
    WorkflowUpdated {
        goal_id: String,
        aggregate_revision: u64,
        operation_id: String,
    },
    /// Plan detected in AI response - awaiting confirmation
    PlanComplete {
        tool_call_id: String,
        title: String,
        task_count: usize,
    },
    /// The autonomous agent is sleeping between ticks.
    AgentSleeping { duration_secs: u64, reason: String },
    /// An agentic turn completed
    TurnComplete { turn: usize, has_more: bool },
    /// Effective parent-run resource budget and provenance.
    RunBudgetResolved {
        max_turns: Option<usize>,
        source: RunBudgetSource,
    },
    /// Redacted provider request contract, emitted immediately before a turn.
    ProviderRequestPrepared {
        turn: usize,
        diagnostics: Box<ProviderRequestSnapshot>,
    },
    /// A local history rewrite changed the provider-visible cache prefix.
    MicrocompactionApplied {
        turn: usize,
        generation: usize,
        message_count: usize,
        history_rewritten: bool,
        tool_inputs_rewritten: bool,
    },
    /// Semantic no-progress warning or terminal guard telemetry.
    ProgressGuard { telemetry: ProgressGuardTelemetry },
    /// The server injected a synthetic tick to continue autonomous work.
    TickInjected { tick_number: usize },
    /// Token usage information
    Usage {
        prompt_tokens: usize,
        input_tokens: usize,
        completion_tokens: usize,
        reasoning_tokens: usize,
        cache_creation_input_tokens: usize,
        cache_read_input_tokens: usize,
        total_tokens: usize,
    },
    /// Active work was handed off into a linked continuation session.
    SessionPinched {
        reason: String,
        source_session_id: String,
        new_session_id: String,
        estimated_tokens_before: usize,
    },
    /// In-place compaction started.
    ContextCompactionStarted { reason: String },
    /// Conversation was compacted in place.
    ContextCompacted {
        reason: String,
        estimated_tokens_before: usize,
        estimated_tokens_after: usize,
        replaced_messages: usize,
        checkpoint_id: String,
        compaction_count: u32,
    },
    /// Some non-terminal stream events were dropped because the client fell behind.
    Lagged { skipped: usize },
    /// A durable Hive daemon event that does not have a legacy chat-event mapping.
    ///
    /// Keeping the daemon sequence and raw payload makes controller lifecycle
    /// events replayable without forcing older clients to understand them.
    HiveControllerEvent {
        session_id: Option<String>,
        run_id: Option<String>,
        sequence: Option<i64>,
        emitted_at_unix_ms: i64,
        event_type: String,
        payload: serde_json::Value,
    },
    /// Agentic loop finished
    Finish {
        session_id: String,
        stop_reason: String,
    },
    /// Session title updated (from Haiku)
    TitleUpdate { title: String },
    /// Tool requires user approval (supervised mode)
    ToolApprovalRequired {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },
    /// Tool was approved by user
    ToolApproved { id: String },
    /// Tool was denied by user
    ToolDenied { id: String },
    /// A live user follow-up entered canonical history.
    SteeringInjected {
        pending_id: Option<String>,
        message: String,
    },
    /// A Worker DM input was durably staged behind an exact active run. The
    /// same event is emitted again with `successor_run_id` once the durable
    /// one-at-a-time materializer assigns its response run.
    WorkerInputStaged {
        worker_id: String,
        session_id: String,
        active_run_id: String,
        staged_input_id: String,
        successor_run_id: Option<String>,
    },
    /// A neutral Worker response has started streaming, but none of its
    /// assistant text is canonical until the matching committed event.
    WorkerResponsePending {
        worker_id: String,
        session_id: String,
        run_id: String,
    },
    /// The exact neutral Worker response passed its durable response writer
    /// and provider-accounting fence. Clients may finalize the matching draft
    /// only when this is followed by a completed terminal event.
    WorkerResponseCommitted {
        worker_id: String,
        session_id: String,
        run_id: String,
    },
    /// Error occurred
    Error { error: String },
    /// A background agent was started
    AgentBackgroundStarted {
        delegated_run_id: String,
        agent_type: String,
        description: String,
    },
    /// A background agent completed
    AgentBackgroundCompleted {
        delegated_run_id: String,
        agent_type: String,
        success: bool,
        summary: String,
    },
    // ── Hive autonomous agent events ─────────────────────────────────
    /// A user-visible message emitted by the SendUserMessage tool
    UserMessage {
        title: Option<String>,
        message: String,
        level: String,
    },
    /// Auto-classifier evaluated a tool call
    ClassifierDecision {
        tool_name: String,
        decision: String,
        reason: String,
        stage: u8,
    },
}

impl AgenticEvent {
    pub fn delegated_progress(event: DelegatedProgressEvent) -> Self {
        let stage = event.stage;
        let progress = event.progress;
        Self::DelegatedProgress {
            delegated_run_id: event.delegated_run_id,
            tool_call_id: event.tool_call_id,
            kind: DelegatedToolKind::from(event.kind),
            stage: DelegatedRunStage::from(stage),
            parent_session_id: event.parent_session_id,
            task_id: progress.task_id,
            agent_name: progress.name,
            status: DelegatedProgressStatus::from_progress(&progress.status, stage),
            tool_count: progress.tool_count,
            tokens: progress.tokens,
            current_action: progress.current_action,
            completion_summary: progress.completion_summary,
            lines_added: progress.lines_added,
            lines_removed: progress.lines_removed,
            completed_plan_task: progress.completed_plan_task,
        }
    }

    pub fn from_runtime_trace(event: RuntimeTraceEvent) -> Option<Self> {
        let payload = event.payload;
        match event.event_type.as_str() {
            "provider_request_prepared" => Some(Self::ProviderRequestPrepared {
                turn: payload.get("turn")?.as_u64()?.try_into().ok()?,
                diagnostics: Box::new(
                    serde_json::from_value(payload.get("diagnostics")?.clone()).ok()?,
                ),
            }),
            "microcompaction_applied" => Some(Self::MicrocompactionApplied {
                turn: payload.get("turn")?.as_u64()?.try_into().ok()?,
                generation: payload.get("generation")?.as_u64()?.try_into().ok()?,
                message_count: payload.get("message_count")?.as_u64()?.try_into().ok()?,
                history_rewritten: payload.get("history_rewritten")?.as_bool()?,
                tool_inputs_rewritten: payload.get("tool_inputs_rewritten")?.as_bool()?,
            }),
            "agent_sleeping" => Some(Self::AgentSleeping {
                duration_secs: payload.get("duration_secs")?.as_u64()?,
                reason: payload.get("reason")?.as_str()?.to_string(),
            }),
            "finished" => Some(Self::Finish {
                session_id: payload.get("session_id")?.as_str()?.to_string(),
                stop_reason: payload.get("stop_reason")?.as_str()?.to_string(),
            }),
            "session_pinched" => Some(Self::SessionPinched {
                reason: payload.get("reason")?.as_str()?.to_string(),
                source_session_id: payload.get("source_session_id")?.as_str()?.to_string(),
                new_session_id: payload.get("new_session_id")?.as_str()?.to_string(),
                estimated_tokens_before: payload.get("estimated_tokens_before")?.as_u64()? as usize,
            }),
            "context_compaction_started" => Some(Self::ContextCompactionStarted {
                reason: payload.get("reason")?.as_str()?.to_string(),
            }),
            "context_compacted" => Some(Self::ContextCompacted {
                reason: payload.get("reason")?.as_str()?.to_string(),
                estimated_tokens_before: payload.get("estimated_tokens_before")?.as_u64()? as usize,
                estimated_tokens_after: payload.get("estimated_tokens_after")?.as_u64()? as usize,
                replaced_messages: payload.get("replaced_messages")?.as_u64()? as usize,
                checkpoint_id: payload.get("checkpoint_id")?.as_str()?.to_string(),
                compaction_count: payload.get("compaction_count")?.as_u64()? as u32,
            }),
            "error" => Some(Self::Error {
                error: payload.get("error")?.as_str()?.to_string(),
            }),
            "user_message" => Some(Self::UserMessage {
                title: payload
                    .get("title")
                    .and_then(|value| value.as_str().map(ToString::to_string)),
                message: payload.get("message")?.as_str()?.to_string(),
                level: payload
                    .get("level")
                    .and_then(|value| value.as_str())
                    .unwrap_or("info")
                    .to_string(),
            }),
            "classifier_decision" => Some(Self::ClassifierDecision {
                tool_name: payload.get("tool_name")?.as_str()?.to_string(),
                decision: payload.get("decision")?.as_str()?.to_string(),
                reason: payload.get("reason")?.as_str()?.to_string(),
                stage: payload.get("stage")?.as_u64()? as u8,
            }),
            "title_generated" => Some(Self::TitleUpdate {
                title: payload.get("title")?.as_str()?.to_string(),
            }),
            "agent_background_started" => Some(Self::AgentBackgroundStarted {
                delegated_run_id: payload.get("delegated_run_id")?.as_str()?.to_string(),
                agent_type: payload.get("agent_type")?.as_str()?.to_string(),
                description: payload.get("description")?.as_str()?.to_string(),
            }),
            "agent_background_completed" => Some(Self::AgentBackgroundCompleted {
                delegated_run_id: payload.get("delegated_run_id")?.as_str()?.to_string(),
                agent_type: payload.get("agent_type")?.as_str()?.to_string(),
                success: payload.get("success")?.as_bool()?,
                summary: payload.get("summary")?.as_str()?.to_string(),
            }),
            _ => None,
        }
    }
}

impl From<mitsuro_core::agent::LoopEvent> for AgenticEvent {
    fn from(event: mitsuro_core::agent::LoopEvent) -> Self {
        use mitsuro_core::agent::LoopEvent;
        match event {
            LoopEvent::TextDelta { delta } => Self::TextDelta { delta },
            LoopEvent::TextDeltaWithCitations { delta, citations } => {
                Self::TextDeltaWithCitations { delta, citations }
            }
            LoopEvent::ThinkingDelta { thinking } => Self::ThinkingDelta { thinking },
            LoopEvent::ThinkingComplete {
                thinking,
                signature,
            } => Self::ThinkingComplete {
                thinking,
                signature,
            },
            LoopEvent::ToolCallStart { id, name } => Self::ToolCallStart { id, name },
            LoopEvent::ToolCallPreparing {
                id,
                name,
                received_bytes,
            } => Self::ToolCallPreparing {
                id,
                name,
                received_bytes,
            },
            LoopEvent::ToolCallComplete {
                id,
                name,
                arguments,
            } => Self::ToolCallComplete {
                id,
                name,
                arguments,
            },
            LoopEvent::ToolExecuting { id, name } => Self::ToolExecuting { id, name },
            LoopEvent::ToolOutputDelta { id, delta } => Self::ToolOutputDelta { id, delta },
            LoopEvent::ToolResult {
                id,
                output,
                is_error,
            } => Self::ToolResult {
                id,
                output,
                is_error,
            },
            LoopEvent::AwaitingInput {
                tool_call_id,
                tool_name,
            } => Self::AwaitingInput {
                tool_call_id,
                tool_name,
            },
            LoopEvent::ToolApprovalRequired {
                id,
                name,
                arguments,
            } => Self::ToolApprovalRequired {
                id,
                name,
                arguments,
            },
            LoopEvent::ToolApproved { id } => Self::ToolApproved { id },
            LoopEvent::ToolDenied { id } => Self::ToolDenied { id },
            LoopEvent::SteeringInjected {
                pending_id,
                message,
            } => Self::SteeringInjected {
                pending_id,
                message,
            },
            LoopEvent::ServerToolStart { id, name } => Self::ServerToolStart { id, name },
            LoopEvent::ServerToolComplete { id, name } => Self::ServerToolComplete { id, name },
            LoopEvent::WebSearchResults {
                tool_use_id,
                results,
            } => Self::WebSearchResults {
                tool_use_id,
                results,
            },
            LoopEvent::WebFetchResult {
                tool_use_id,
                content,
            } => Self::WebFetchResult {
                tool_use_id,
                content,
            },
            LoopEvent::ServerToolError {
                tool_use_id,
                error_code,
            } => Self::ServerToolError {
                tool_use_id,
                error_code,
            },
            LoopEvent::ModeChange { mode, reason } => Self::ModeChange { mode, reason },
            LoopEvent::PlanUpdate { tasks } => Self::PlanUpdate {
                items: tasks
                    .into_iter()
                    .map(|t| PlanItem {
                        content: t.description,
                        completed: t.completed,
                    })
                    .collect(),
            },
            LoopEvent::WorkflowUpdated {
                goal_id,
                aggregate_revision,
                operation_id,
            } => Self::WorkflowUpdated {
                goal_id,
                aggregate_revision,
                operation_id,
            },
            LoopEvent::PlanComplete {
                tool_call_id,
                title,
                task_count,
            } => Self::PlanComplete {
                tool_call_id,
                title,
                task_count,
            },
            LoopEvent::AgentSleeping {
                duration_secs,
                reason,
            } => Self::AgentSleeping {
                duration_secs,
                reason,
            },
            LoopEvent::TurnComplete { turn, has_more } => Self::TurnComplete { turn, has_more },
            LoopEvent::RunBudgetResolved { max_turns, source } => {
                Self::RunBudgetResolved { max_turns, source }
            }
            LoopEvent::ProviderRequestPrepared { turn, diagnostics } => {
                Self::ProviderRequestPrepared { turn, diagnostics }
            }
            LoopEvent::MicrocompactionApplied {
                turn,
                generation,
                message_count,
                history_rewritten,
                tool_inputs_rewritten,
            } => Self::MicrocompactionApplied {
                turn,
                generation,
                message_count,
                history_rewritten,
                tool_inputs_rewritten,
            },
            LoopEvent::ProgressGuard { telemetry } => Self::ProgressGuard { telemetry },
            LoopEvent::TickInjected { tick_number } => Self::TickInjected { tick_number },
            LoopEvent::Usage {
                prompt_tokens,
                input_tokens,
                completion_tokens,
                reasoning_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                total_tokens,
            } => Self::Usage {
                prompt_tokens,
                input_tokens,
                completion_tokens,
                reasoning_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                total_tokens,
            },
            LoopEvent::SessionPinched {
                reason,
                source_session_id,
                new_session_id,
                estimated_tokens_before,
            } => Self::SessionPinched {
                reason,
                source_session_id,
                new_session_id,
                estimated_tokens_before,
            },
            LoopEvent::ContextCompactionStarted { reason } => {
                Self::ContextCompactionStarted { reason }
            }
            LoopEvent::ContextCompacted {
                reason,
                estimated_tokens_before,
                estimated_tokens_after,
                replaced_messages,
                checkpoint_id,
                compaction_count,
            } => Self::ContextCompacted {
                reason,
                estimated_tokens_before,
                estimated_tokens_after,
                replaced_messages,
                checkpoint_id,
                compaction_count,
            },
            LoopEvent::TitleGenerated { title } => Self::TitleUpdate { title },
            LoopEvent::Finished {
                session_id,
                stop_reason,
            } => Self::Finish {
                session_id,
                stop_reason: serde_json::to_value(stop_reason)
                    .ok()
                    .and_then(|v| v.as_str().map(ToString::to_string))
                    .unwrap_or_else(|| "completed".to_string()),
            },
            LoopEvent::Error { error } => Self::Error { error },
            LoopEvent::AgentBackgroundStarted {
                delegated_run_id,
                agent_type,
                description,
            } => Self::AgentBackgroundStarted {
                delegated_run_id,
                agent_type,
                description,
            },
            LoopEvent::AgentBackgroundCompleted {
                delegated_run_id,
                agent_type,
                success,
                summary,
            } => Self::AgentBackgroundCompleted {
                delegated_run_id,
                agent_type,
                success,
                summary,
            },
            LoopEvent::UserMessage {
                title,
                message,
                level,
            } => Self::UserMessage {
                title,
                message,
                level,
            },
            LoopEvent::ClassifierDecision {
                tool_name,
                decision,
                reason,
                stage,
            } => Self::ClassifierDecision {
                tool_name,
                decision,
                reason,
                stage,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use mitsuro_core::agent::LoopEvent;
    use mitsuro_core::ai::client::{AiClient, AiClientConfig, CallOptions};
    use mitsuro_core::ai::types::{Content, ModelMessage, Role};
    use mitsuro_core::storage::RuntimeTraceEvent;

    use super::AgenticEvent;

    #[test]
    fn provider_request_diagnostics_survive_sse_and_trace_replay_without_contents() {
        const SYSTEM_SECRET: &str = "sse-system-secret";
        const USER_SECRET: &str = "sse-user-secret";
        const CREDENTIAL_SECRET: &str = "sse-credential-secret";

        let client = AiClient::new(
            AiClientConfig::for_grok("grok-4.5"),
            CREDENTIAL_SECRET.to_string(),
        );
        let messages = vec![
            ModelMessage {
                role: Role::System,
                content: vec![Content::text(SYSTEM_SECRET.to_string())],
            },
            ModelMessage {
                role: Role::User,
                content: vec![Content::text(USER_SECRET.to_string())],
            },
        ];
        let diagnostics = client.request_diagnostics(
            &messages,
            &CallOptions {
                system_prompt: Some(SYSTEM_SECRET.to_string()),
                ..CallOptions::default()
            },
        );
        let loop_event = LoopEvent::ProviderRequestPrepared {
            turn: 3,
            diagnostics: Box::new(diagnostics.into()),
        };

        let sse_event = AgenticEvent::from(loop_event.clone());
        let sse_json = serde_json::to_string(&sse_event).expect("SSE event should serialize");
        assert!(sse_json.contains("provider_request_prepared"));
        assert!(sse_json.contains("grok-4.5"));

        let trace = RuntimeTraceEvent::from_loop_event("run-sse", 1, 3, &loop_event);
        let replayed = AgenticEvent::from_runtime_trace(trace).expect("trace should replay");
        let replay_json = serde_json::to_string(&replayed).expect("replay should serialize");
        assert!(replay_json.contains("provider_request_prepared"));
        assert!(replay_json.contains("grok-4.5"));

        for secret in [SYSTEM_SECRET, USER_SECRET, CREDENTIAL_SECRET] {
            assert!(!sse_json.contains(secret));
            assert!(!replay_json.contains(secret));
        }
    }
}
