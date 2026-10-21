//! Canonical event protocol for the agentic loop.
//!
//! `LoopEvent` is the single source of truth for everything the orchestrator
//! emits. Transport layers (TUI, HTTP/SSE server) consume these events and
//! map them to their own presentation format.
//!
//! `LoopInput` represents external inputs that the platform provides back to
//! the running orchestrator (tool approvals, user responses, cancellation).

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::ai::client::config::EffectiveRequestSettings;
use crate::ai::client::PreparedRequestDiagnostics;
use crate::ai::models::{ModelCatalogSource, ModelKey};
use crate::ai::types::{Citation, Content, WebFetchContent, WebSearchResult};

use super::progress::ProgressGuardTelemetry;
use super::state::RunBudgetSource;

/// Stable, minimal projection of `PreparedRequestDiagnostics` that is safe to
/// cross persistence, extension, ACP, TUI, and HTTP/SSE boundaries.
///
/// Keeping the projection separate means a future field added to the internal
/// prepared request cannot accidentally expose request contents to observers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderRequestSnapshot {
    pub model_key: ModelKey,
    pub catalog_source: ModelCatalogSource,
    pub catalog_revision: Option<String>,
    pub effective_request: EffectiveRequestSettings,
    #[serde(default)]
    pub tool_names: Vec<String>,
    pub prompt_manifest: serde_json::Value,
    #[serde(default)]
    pub stable_request_fingerprint: String,
    #[serde(default)]
    pub stable_instruction_bytes: usize,
    #[serde(default)]
    pub volatile_session_bytes: usize,
    #[serde(default)]
    pub tool_schema_bytes: usize,
    #[serde(default)]
    pub history_bytes: usize,
    #[serde(default)]
    pub cache_key_present: bool,
    #[serde(default)]
    pub cache_mode: String,
    #[serde(default)]
    pub continuation_mode: Option<String>,
    pub message_count: usize,
    pub system_message_count: usize,
    pub user_message_count: usize,
    pub assistant_message_count: usize,
}

impl From<PreparedRequestDiagnostics> for ProviderRequestSnapshot {
    fn from(diagnostics: PreparedRequestDiagnostics) -> Self {
        Self {
            model_key: diagnostics.model.key,
            catalog_source: diagnostics.model.catalog_source,
            catalog_revision: diagnostics.model.catalog_revision,
            effective_request: diagnostics.effective_request,
            tool_names: diagnostics.tool_names,
            prompt_manifest: diagnostics.prompt_manifest,
            stable_request_fingerprint: diagnostics.stable_request_fingerprint,
            stable_instruction_bytes: diagnostics.stable_instruction_bytes,
            volatile_session_bytes: diagnostics.volatile_session_bytes,
            tool_schema_bytes: diagnostics.tool_schema_bytes,
            history_bytes: diagnostics.history_bytes,
            cache_key_present: diagnostics.cache_key_present,
            cache_mode: diagnostics.cache_mode,
            continuation_mode: diagnostics.continuation_mode,
            message_count: diagnostics.message_count,
            system_message_count: diagnostics.system_message_count,
            user_message_count: diagnostics.user_message_count,
            assistant_message_count: diagnostics.assistant_message_count,
        }
    }
}

/// Structured terminal reason for an agentic loop.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LoopStopReason {
    Completed,
    AwaitingInput,
    Sleeping,
    BudgetExhausted,
    ProviderError,
    LoopGuardTriggered,
    StreamIdleTimeout,
    UserAbort,
    Pinched,
    PinchFailed,
}

/// Events emitted by the agentic orchestrator.
///
/// Each variant represents a discrete state change in the agentic loop.
/// Consumers (TUI, server) map these to their own presentation format.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LoopEvent {
    // ── Streaming ──────────────────────────────────────────────────────
    /// Text content delta from AI response.
    TextDelta { delta: String },

    /// Text content delta with web citations.
    TextDeltaWithCitations {
        delta: String,
        citations: Vec<Citation>,
    },

    /// Extended thinking delta.
    ThinkingDelta { thinking: String },

    /// Extended thinking block completed.
    ThinkingComplete { thinking: String, signature: String },

    // ── Tool lifecycle ─────────────────────────────────────────────────
    /// AI is starting to stream a tool call (arguments not yet complete).
    ToolCallStart { id: String, name: String },

    /// Bounded progress while the provider is still composing a large tool
    /// argument payload. Raw arguments remain private until completion.
    ToolCallPreparing {
        id: String,
        name: String,
        received_bytes: usize,
    },

    /// Tool call arguments fully received from AI.
    ToolCallComplete {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },

    /// Tool is being executed.
    ToolExecuting { id: String, name: String },

    /// Streaming output delta from a running tool (e.g. bash output).
    ToolOutputDelta { id: String, delta: String },

    /// Tool execution completed with result.
    ToolResult {
        id: String,
        output: String,
        is_error: bool,
    },

    // ── Interaction ────────────────────────────────────────────────────
    /// Orchestrator is waiting for user input (AskUser or PlanConfirm).
    AwaitingInput {
        tool_call_id: String,
        tool_name: String,
    },

    /// Tool requires user approval before execution (supervised mode).
    ToolApprovalRequired {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },

    /// Tool was approved by user.
    ToolApproved { id: String },

    /// Tool was denied by user.
    ToolDenied { id: String },

    /// A live user follow-up was durably inserted at a safe model boundary.
    /// This is distinct from `UserMessage`, which is output produced by the
    /// autonomous SendUserMessage tool.
    SteeringInjected {
        pending_id: Option<String>,
        message: String,
    },

    // ── Server-side tools (web search/fetch) ──────────────────────────
    /// Server-side tool started (web_search, web_fetch).
    ServerToolStart { id: String, name: String },

    /// Server-side tool completed.
    ServerToolComplete { id: String, name: String },

    /// Web search results received.
    WebSearchResults {
        tool_use_id: String,
        results: Vec<WebSearchResult>,
    },

    /// Web fetch result received.
    WebFetchResult {
        tool_use_id: String,
        content: WebFetchContent,
    },

    /// Server-side tool error.
    ServerToolError {
        tool_use_id: String,
        error_code: String,
    },

    // ── Mode + Plan ────────────────────────────────────────────────────
    /// Work mode changed (build ↔ plan).
    ModeChange {
        mode: String,
        reason: Option<String>,
    },

    /// Plan tasks detected/updated.
    PlanUpdate { tasks: Vec<PlanTaskInfo> },

    /// Canonical Goal/plan aggregate changed. Consumers should refresh the
    /// session workflow snapshot when this revision is newer than local state.
    WorkflowUpdated {
        goal_id: String,
        aggregate_revision: u64,
        operation_id: String,
    },

    /// Plan detected in AI response, awaiting user confirmation.
    PlanComplete {
        tool_call_id: String,
        title: String,
        task_count: usize,
    },

    /// The autonomous agent is intentionally sleeping between ticks.
    AgentSleeping { duration_secs: u64, reason: String },

    // ── Turn lifecycle ─────────────────────────────────────────────────
    /// An agentic turn completed.
    TurnComplete { turn: usize, has_more: bool },

    /// Effective parent-run budget resolved once at the core boundary.
    RunBudgetResolved {
        max_turns: Option<usize>,
        source: RunBudgetSource,
    },

    /// Redacted request contract frozen immediately before a provider turn.
    /// Prompt/user contents, tool schemas, and credentials are intentionally
    /// absent; consumers may safely persist or expose this diagnostic event.
    ProviderRequestPrepared {
        turn: usize,
        diagnostics: Box<ProviderRequestSnapshot>,
    },

    /// A local history rewrite changed the provider-visible prefix. This is a
    /// first-class cache boundary so misses can be attributed rather than
    /// appearing as unexplained provider behavior.
    MicrocompactionApplied {
        turn: usize,
        generation: usize,
        message_count: usize,
        history_rewritten: bool,
        tool_inputs_rewritten: bool,
    },

    /// Semantic no-progress telemetry. `triggered=false` is an early warning;
    /// `triggered=true` is immediately followed by the typed terminal event.
    ProgressGuard { telemetry: ProgressGuardTelemetry },

    /// Tick engine injected a synthetic tick message to continue autonomous work.
    TickInjected { tick_number: usize },

    /// Token usage for this turn.
    Usage {
        /// Uncached input tokens billed at the normal input rate.
        prompt_tokens: usize,
        /// Logical input tokens: uncached + cache write + cache read.
        input_tokens: usize,
        /// Generated output, including reasoning tokens.
        completion_tokens: usize,
        /// Reasoning tokens contained within `completion_tokens`.
        reasoning_tokens: usize,
        /// Input tokens written to a provider prompt cache.
        cache_creation_input_tokens: usize,
        /// Input tokens served from a provider prompt cache.
        cache_read_input_tokens: usize,
        /// Total input and output tokens represented by this turn snapshot;
        /// reasoning is not added again.
        total_tokens: usize,
    },

    /// Active work was handed off into a linked continuation session.
    SessionPinched {
        reason: String,
        source_session_id: String,
        new_session_id: String,
        estimated_tokens_before: usize,
    },

    /// In-place compaction started (manual, auto, overflow, or reactive).
    ContextCompactionStarted { reason: String },

    /// Conversation was compacted in place to relieve context pressure.
    ContextCompacted {
        reason: String,
        estimated_tokens_before: usize,
        estimated_tokens_after: usize,
        replaced_messages: usize,
        checkpoint_id: String,
        compaction_count: u32,
    },

    /// Session title generated.
    TitleGenerated { title: String },

    /// Agentic loop finished.
    Finished {
        session_id: String,
        stop_reason: LoopStopReason,
    },

    /// Error occurred.
    Error { error: String },

    // ── Background agent lifecycle ────────────────────────────────────
    /// A background agent was started. Parent can continue immediately.
    AgentBackgroundStarted {
        delegated_run_id: String,
        agent_type: String,
        description: String,
    },

    /// A background agent completed. Result is in the delegated run store.
    AgentBackgroundCompleted {
        delegated_run_id: String,
        agent_type: String,
        success: bool,
        summary: String,
    },

    // ── Hive autonomous agent events ─────────────────────────────────
    /// Explicit user-facing message from the autonomous agent.
    UserMessage {
        title: Option<String>,
        message: String,
        level: String,
    },

    /// Auto-classifier evaluated a tool call.
    ClassifierDecision {
        tool_name: String,
        decision: String,
        reason: String,
        stage: u8,
    },
}

/// Simple plan task info for event transport.
#[derive(Debug, Clone, Serialize)]
pub struct PlanTaskInfo {
    pub description: String,
    pub completed: bool,
}

/// External inputs the platform provides back to the orchestrator.
#[derive(Debug, Clone)]
pub enum LoopInput {
    /// User approved or denied a tool execution.
    ToolApproval {
        tool_call_id: String,
        approved: bool,
    },

    /// User responded to an AskUser or PlanConfirm prompt.
    UserResponse {
        tool_call_id: String,
        response: String,
    },

    /// A follow-up from the user while the current run is active. The
    /// orchestrator queues this until the next model boundary so an in-flight
    /// provider stream or tool lifecycle is never spliced mid-operation.
    Steer {
        /// Durable pending-message identifier when the transport persisted the
        /// input before enqueueing it. `None` is reserved for in-process
        /// surfaces whose lifecycle is already owned by the caller.
        pending_id: Option<String>,
        content: Vec<Content>,
    },

    /// A canonical user message that was committed by an external durable
    /// controller before delivery to the live loop. It follows the same safe
    /// model-boundary injection path as steering, but must never be written to
    /// conversation history a second time.
    PersistedUserMessage { content: Vec<Content> },

    /// A durable workflow command changed the active Goal while this loop was
    /// running. This is a control-plane wake, not a synthetic user message.
    WorkflowUpdated {
        goal_id: String,
        aggregate_revision: u64,
        operation_id: String,
    },

    /// User requested cancellation.
    Cancel,
}

/// Canonical input inbox for a running agent loop.
///
/// Steering is captured out-of-band while control inputs retain their FIFO
/// order. Consumers can therefore continue waiting for cancellation or an
/// approval without accidentally discarding a concurrent user follow-up.
pub(crate) struct LoopInputInbox {
    receiver: mpsc::UnboundedReceiver<LoopInput>,
    controls: VecDeque<LoopInput>,
    steering: VecDeque<PendingSteering>,
    workflow_updates: VecDeque<(String, u64, String)>,
}

#[derive(Debug)]
pub(crate) struct PendingSteering {
    pub(crate) pending_id: Option<String>,
    pub(crate) content: Vec<Content>,
    pub(crate) already_persisted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ToolApprovalInput {
    Decision(bool),
    Cancelled,
    Closed,
}

impl LoopInputInbox {
    pub(crate) fn new(receiver: mpsc::UnboundedReceiver<LoopInput>) -> Self {
        Self {
            receiver,
            controls: VecDeque::new(),
            steering: VecDeque::new(),
            workflow_updates: VecDeque::new(),
        }
    }

    /// Receive the next control input, retaining any steering encountered
    /// while waiting for the caller to inject at a safe model boundary.
    #[cfg(test)]
    pub(crate) async fn recv_control(&mut self) -> Option<LoopInput> {
        loop {
            if let Some(input) = self.controls.pop_front() {
                return Some(input);
            }

            match self.receiver.recv().await {
                Some(LoopInput::Steer {
                    pending_id,
                    content,
                }) => self.steering.push_back(PendingSteering {
                    pending_id,
                    content,
                    already_persisted: false,
                }),
                Some(LoopInput::PersistedUserMessage { content }) => {
                    self.steering.push_back(PendingSteering {
                        pending_id: None,
                        content,
                        already_persisted: true,
                    })
                }
                Some(LoopInput::WorkflowUpdated {
                    goal_id,
                    aggregate_revision,
                    operation_id,
                }) => self
                    .workflow_updates
                    .push_back((goal_id, aggregate_revision, operation_id)),
                input => return input,
            }
        }
    }

    /// Wait for cancellation while retaining every out-of-phase control for
    /// its actual lifecycle owner. This is used around provider streams and
    /// already-authorized tool execution, where an approval must not be eaten.
    pub(crate) async fn recv_cancel(&mut self) -> Option<()> {
        if self.take_cancel() {
            return Some(());
        }

        loop {
            match self.receiver.recv().await {
                Some(LoopInput::Cancel) => return Some(()),
                Some(LoopInput::Steer {
                    pending_id,
                    content,
                }) => self.steering.push_back(PendingSteering {
                    pending_id,
                    content,
                    already_persisted: false,
                }),
                Some(LoopInput::PersistedUserMessage { content }) => {
                    self.steering.push_back(PendingSteering {
                        pending_id: None,
                        content,
                        already_persisted: true,
                    })
                }
                Some(LoopInput::WorkflowUpdated {
                    goal_id,
                    aggregate_revision,
                    operation_id,
                }) => self
                    .workflow_updates
                    .push_back((goal_id, aggregate_revision, operation_id)),
                Some(input) => self.controls.push_back(input),
                None => return None,
            }
        }
    }

    /// Wait for one tool's decision without consuming approvals or responses
    /// owned by another interaction.
    pub(crate) async fn recv_tool_approval(
        &mut self,
        expected_tool_call_id: &str,
    ) -> ToolApprovalInput {
        loop {
            if let Some(position) = self.controls.iter().position(|input| {
                matches!(input, LoopInput::Cancel)
                    || matches!(
                        input,
                        LoopInput::ToolApproval { tool_call_id, .. }
                            if tool_call_id == expected_tool_call_id
                    )
            }) {
                match self.controls.remove(position) {
                    Some(LoopInput::Cancel) => return ToolApprovalInput::Cancelled,
                    Some(LoopInput::ToolApproval { approved, .. }) => {
                        return ToolApprovalInput::Decision(approved);
                    }
                    _ => unreachable!("selective approval predicate returned another input"),
                }
            }

            match self.receiver.recv().await {
                Some(LoopInput::Cancel) => return ToolApprovalInput::Cancelled,
                Some(LoopInput::ToolApproval {
                    tool_call_id,
                    approved,
                }) if tool_call_id == expected_tool_call_id => {
                    return ToolApprovalInput::Decision(approved);
                }
                Some(LoopInput::Steer {
                    pending_id,
                    content,
                }) => self.steering.push_back(PendingSteering {
                    pending_id,
                    content,
                    already_persisted: false,
                }),
                Some(LoopInput::PersistedUserMessage { content }) => {
                    self.steering.push_back(PendingSteering {
                        pending_id: None,
                        content,
                        already_persisted: true,
                    })
                }
                Some(LoopInput::WorkflowUpdated {
                    goal_id,
                    aggregate_revision,
                    operation_id,
                }) => self
                    .workflow_updates
                    .push_back((goal_id, aggregate_revision, operation_id)),
                Some(input) => self.controls.push_back(input),
                None => return ToolApprovalInput::Closed,
            }
        }
    }

    /// Capture all inputs already queued without blocking. Non-steering
    /// controls are preserved for the lifecycle component that owns them.
    pub(crate) fn collect_ready(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(LoopInput::Steer {
                    pending_id,
                    content,
                }) => self.steering.push_back(PendingSteering {
                    pending_id,
                    content,
                    already_persisted: false,
                }),
                Ok(LoopInput::PersistedUserMessage { content }) => {
                    self.steering.push_back(PendingSteering {
                        pending_id: None,
                        content,
                        already_persisted: true,
                    })
                }
                Ok(LoopInput::WorkflowUpdated {
                    goal_id,
                    aggregate_revision,
                    operation_id,
                }) => self
                    .workflow_updates
                    .push_back((goal_id, aggregate_revision, operation_id)),
                Ok(input) => self.controls.push_back(input),
                Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                    break;
                }
            }
        }
    }

    /// Cancellation has priority at model boundaries, but does not reorder or
    /// consume any other control input.
    pub(crate) fn take_cancel(&mut self) -> bool {
        let Some(position) = self
            .controls
            .iter()
            .position(|input| matches!(input, LoopInput::Cancel))
        else {
            return false;
        };
        self.controls.remove(position);
        true
    }

    pub(crate) fn take_steering(&mut self) -> Vec<PendingSteering> {
        self.steering.drain(..).collect()
    }

    pub(crate) fn take_workflow_updates(&mut self) -> Vec<(String, u64, String)> {
        self.workflow_updates.drain(..).collect()
    }

    #[cfg(test)]
    fn pending_control_count(&self) -> usize {
        self.controls.len()
    }
}

#[cfg(test)]
mod input_tests {
    use super::*;

    fn text(value: &str) -> Vec<Content> {
        vec![Content::text(value.to_string())]
    }

    #[tokio::test]
    async fn inbox_queues_concurrent_steering_without_hiding_cancellation() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(LoopInput::Steer {
            pending_id: None,
            content: text("first"),
        })
        .expect("first steering should enqueue");
        tx.send(LoopInput::Cancel)
            .expect("cancellation should enqueue");
        tx.send(LoopInput::Steer {
            pending_id: None,
            content: text("second"),
        })
        .expect("second steering should enqueue");

        let mut inbox = LoopInputInbox::new(rx);
        inbox.collect_ready();

        assert!(inbox.take_cancel());
        assert_eq!(inbox.pending_control_count(), 0);
        let steering = inbox.take_steering();
        assert_eq!(steering.len(), 2);
        assert!(matches!(
            &steering[0].content[0],
            Content::Text { text, .. } if text == "first"
        ));
        assert!(matches!(
            &steering[1].content[0],
            Content::Text { text, .. } if text == "second"
        ));
    }

    #[test]
    fn workflow_updates_are_control_plane_wakes_not_user_steering() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(LoopInput::WorkflowUpdated {
            goal_id: "goal-1".into(),
            aggregate_revision: 7,
            operation_id: "activate-1".into(),
        })
        .expect("workflow update should enqueue");

        let mut inbox = LoopInputInbox::new(rx);
        inbox.collect_ready();

        assert!(inbox.take_steering().is_empty());
        assert_eq!(
            inbox.take_workflow_updates(),
            vec![("goal-1".into(), 7, "activate-1".into())]
        );
    }

    #[tokio::test]
    async fn recv_control_retains_steering_while_waiting_for_approval() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(LoopInput::Steer {
            pending_id: None,
            content: text("change direction"),
        })
        .expect("steering should enqueue");
        tx.send(LoopInput::ToolApproval {
            tool_call_id: "tool-1".into(),
            approved: true,
        })
        .expect("approval should enqueue");

        let mut inbox = LoopInputInbox::new(rx);
        assert!(matches!(
            inbox.recv_control().await,
            Some(LoopInput::ToolApproval {
                tool_call_id,
                approved: true,
            }) if tool_call_id == "tool-1"
        ));
        assert_eq!(inbox.take_steering().len(), 1);
    }

    #[tokio::test]
    async fn cancellation_wait_preserves_late_approval_for_its_owner() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(LoopInput::ToolApproval {
            tool_call_id: "tool-late".into(),
            approved: true,
        })
        .expect("approval should enqueue");
        tx.send(LoopInput::Cancel)
            .expect("cancellation should enqueue");

        let mut inbox = LoopInputInbox::new(rx);
        assert_eq!(inbox.recv_cancel().await, Some(()));
        assert!(matches!(
            inbox.recv_control().await,
            Some(LoopInput::ToolApproval {
                tool_call_id,
                approved: true,
            }) if tool_call_id == "tool-late"
        ));
    }

    #[tokio::test]
    async fn selective_approval_wait_preserves_unrelated_controls() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(LoopInput::ToolApproval {
            tool_call_id: "other-tool".into(),
            approved: false,
        })
        .expect("unrelated approval should enqueue");
        tx.send(LoopInput::UserResponse {
            tool_call_id: "question-1".into(),
            response: "answer".into(),
        })
        .expect("unrelated response should enqueue");
        tx.send(LoopInput::ToolApproval {
            tool_call_id: "expected-tool".into(),
            approved: true,
        })
        .expect("matching approval should enqueue");

        let mut inbox = LoopInputInbox::new(rx);
        assert_eq!(
            inbox.recv_tool_approval("expected-tool").await,
            ToolApprovalInput::Decision(true)
        );
        assert!(matches!(
            inbox.recv_control().await,
            Some(LoopInput::ToolApproval {
                tool_call_id,
                approved: false,
            }) if tool_call_id == "other-tool"
        ));
        assert!(matches!(
            inbox.recv_control().await,
            Some(LoopInput::UserResponse {
                tool_call_id,
                response,
            }) if tool_call_id == "question-1" && response == "answer"
        ));
    }
}
