//! Agentic orchestrator — the single canonical agentic loop.
//!
//! `AgenticOrchestrator` encapsulates the complete AI agent loop:
//! streaming, tool execution, context injection, plan management,
//! failure detection, and title generation.
//!
//! Both the TUI and HTTP server are thin presentation layers that:
//! - Create an orchestrator from their own state
//! - Call `run()` to get an event stream and input channel
//! - Map `LoopEvent` to their display format
//! - Send `LoopInput` for user interactions
//!
//! ```text
//!  ┌─────────────┐        LoopEvent         ┌─────────────┐
//!  │ Orchestrator │ ─────────────────────►   │  Consumer   │
//!  │   (core)     │                          │ (TUI/Server)│
//!  │              │ ◄─────────────────────   │             │
//!  └─────────────┘        LoopInput          └─────────────┘
//! ```

mod message_builder;
mod persistence;
mod recovery;
#[cfg(test)]
mod runtime_tests;
mod title;
mod tool_surface;
mod workflow_runtime;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use tokio::sync::{mpsc, RwLock};

use crate::ai::client::{AiClient, CallOptions, RemoteAttemptPolicy};
use crate::ai::transport_policy::StreamTransportPolicy;
use crate::ai::types::{AiToolCall, Content, ModelMessage, Role};
use crate::constants;
use crate::process::ProcessRegistry;
use crate::skills::SkillsManager;
use crate::storage::{
    Database, DelegatedRunRecord, DelegatedRunStore, HiveGroupRunContext, HiveProfileSnapshot,
    PartialAssistantState, PendingInteractionSnapshot, ProjectSettings, RecoveryStatus,
    SessionManager, SessionType, WorkMode, WorkerConversationResponseCommitError,
};
use crate::tools::registry::{
    agent_call_action, agent_call_is_research, agent_call_requests_write, effective_tool_call,
};
use crate::tools::registry::{
    trusted_changed, FileObservationTracker, PermissionMode, ToolRegistry,
};
use crate::workflow::{AttemptStatus, GoalStatus, WorkflowManager, WorkflowStepStatus};

use super::compaction::{
    effective_context_window_for_runtime, is_context_overflow_error,
    microcompact::{microcompact_messages_cache_aware, should_rewrite_microcompact_history},
    run_compaction_pipeline_observed, CompactionManager, CompactionRequest,
    CompactionRequestBudget, CompactionResult, CompactionTrigger,
};
use super::context;
use super::context_ledger::ContextLedger;
use super::executor;
use super::failure;
use super::loop_events::{LoopEvent, LoopInput, LoopInputInbox, LoopStopReason};
use super::progress::{DelegationNudgeTracker, LoopGuard};
use super::run_spec::RunContextMode;
use super::state::{RunBudget, RunBudgetResolution};
use super::stream;
use super::subagent::AgentCapability;
use super::{
    bounded_reservation, DelegatedProgressEvent, WorkerConversationResponseCommitInput,
    WorkerGoalAttemptOutcome, WorkerGoalEffectSummary, WorkerGoalEvidence, WorkerGoalEvidenceKind,
    WorkerGoalOutcomeCommitError, WorkerGoalOutcomeCommitInput, WorkerGoalOutcomeCommitter,
    WorkerGoalOutcomeCounters, WorkerGoalOutcomeInputError, WorkerProviderAdmission,
    WorkerProviderCallKind, WorkerProviderCallSlot, WorkerProviderCompletion,
    WorkerProviderTerminalOutcome, MAX_WORKER_GOAL_EVIDENCE_ITEMS,
    MAX_WORKER_GOAL_PROVIDER_CALL_IDS,
};

use self::message_builder::{build_assistant_message, finalize_explore_only_turn};
use self::persistence::{
    clear_recovery_state, persist_context_state, save_message, set_agent_state, update_token_count,
};
use self::recovery::{
    build_awaiting_input_recovery_state, build_partial_assistant_state, build_recovery_state,
    continuation_recovery_message,
};
use self::title::maybe_generate_title;
use self::tool_surface::{advertised_names, ModeAwareToolSurface};
use self::workflow_runtime::{
    active_goal_for_run, awaiting_background_delegation, block_active_goal_for_stop,
    ensure_active_goal_attempt_for_run, finish_active_attempt_for_stop, goal_final_disposition,
    pause_active_goal_for_stop, record_active_attempt_progress, record_active_goal_tokens,
    AttemptProgressTracker, GoalFinalDisposition, GOAL_FINAL_CONTINUATION,
};
use crate::plan::has_active_workflow_or_plan;

const EMPTY_COMPLETION_RECOVERY_INSTRUCTION: &str = "[EMPTY RESPONSE RECOVERY]\nThe previous model completion contained no user-visible text or tool call. Continue the same turn from the existing conversation and provide the response requested by the user now, or make a necessary new tool call. Do not repeat a completed tool call merely because the prior completion was empty, and do not mention this recovery instruction.";
const AWAITING_INPUT_PERSISTENCE_ERROR: &str =
    "Unable to safely pause for user input because the continuation policy could not be persisted.";
const EMPTY_COMPLETION_ERROR: &str = "The AI provider completed twice without producing user-visible text or a tool call. Try again or choose another model.";
const EMPTY_COMPLETION_AFTER_SERVER_TOOL_ERROR: &str = "The AI provider completed after hosted tool activity without producing a user-visible response. The hosted tool was not replayed; try again or choose another model.";
const LOOP_GUARD_LANDING_FALLBACK: &str = super::loop_kernel::LANDING_FALLBACK_USER;

/// Canonical Chat/Code/Hive-session persistence is not the durable record for
/// a WorkerGoal run. Its trigger, assistant stream, and tool protocol belong
/// to the fenced Hive run/outcome transaction, so this boundary drops every
/// generic session context, recovery, transcript, and token-accounting write
/// while leaving provider governance, LoopEvents, traces, cancellation, and
/// tool side effects intact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalSessionPersistenceBoundary {
    enabled: bool,
}

impl CanonicalSessionPersistenceBoundary {
    fn for_context_mode(context_mode: &RunContextMode) -> Self {
        Self {
            enabled: !context_mode.is_worker_goal(),
        }
    }

    fn persist_context_state(self, db_path: &Path, session_id: &str, ledger: &ContextLedger) {
        if self.enabled {
            persistence::persist_context_state(db_path, session_id, ledger);
        }
    }

    fn persist_recovery_state(
        self,
        db_path: &Path,
        session_id: &str,
        recovery: &crate::storage::SessionRecoveryState,
    ) {
        if self.enabled {
            persistence::persist_recovery_state(db_path, session_id, recovery);
        }
    }

    fn persist_required_recovery_state(
        self,
        db_path: &Path,
        session_id: &str,
        recovery: &crate::storage::SessionRecoveryState,
    ) -> anyhow::Result<()> {
        if self.enabled {
            persistence::persist_required_recovery_state(db_path, session_id, recovery)
        } else {
            Ok(())
        }
    }

    fn clear_recovery_state(self, db_path: &Path, session_id: &str) {
        if self.enabled {
            persistence::clear_recovery_state(db_path, session_id);
        }
    }

    fn save_message(self, db_path: &Path, session_id: &str, message: &ModelMessage) {
        if self.enabled {
            persistence::save_message(db_path, session_id, message);
        }
    }

    fn update_token_count(self, db_path: &Path, session_id: &str, count: usize) {
        if self.enabled {
            persistence::update_token_count(db_path, session_id, count);
        }
    }
}

#[derive(Debug, Default)]
struct WorkerGoalOutcomeJournal {
    provider_call_ids: Vec<String>,
    evidence: Vec<WorkerGoalEvidence>,
    counters: WorkerGoalOutcomeCounters,
    workspace_mutated: bool,
    overflowed: bool,
}

impl WorkerGoalOutcomeJournal {
    fn record_provider_call(&mut self, provider_call_id: String) {
        self.counters.provider_calls = self.counters.provider_calls.saturating_add(1);
        self.counters.turns = self.counters.turns.saturating_add(1);
        if self.provider_call_ids.len() >= MAX_WORKER_GOAL_PROVIDER_CALL_IDS {
            self.overflowed = true;
            return;
        }
        self.provider_call_ids.push(provider_call_id);
    }

    fn record_tool_results(&mut self, calls: &[AiToolCall], results: &[Content]) {
        for result in results {
            let Content::ToolResult {
                tool_use_id,
                output,
                is_error,
            } = result
            else {
                continue;
            };
            self.counters.tool_calls = self.counters.tool_calls.saturating_add(1);
            let failed = is_error.unwrap_or(false)
                || output
                    .get("is_error")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
            if failed {
                self.counters.failed_tool_calls = self.counters.failed_tool_calls.saturating_add(1);
            } else {
                self.counters.successful_tool_calls =
                    self.counters.successful_tool_calls.saturating_add(1);
            }

            let mut matching_calls = calls.iter().filter(|call| call.id == *tool_use_id);
            let matching_call = matching_calls.next();
            if matching_call.is_none() || matching_calls.next().is_some() {
                // Tool-use ids are model supplied and therefore only an
                // internal correlation hint. Ambiguous correlation makes the
                // journal ineligible for canonical progress.
                self.overflowed = true;
            }
            let tool = matching_call
                .map(|call| effective_tool_call(&call.name, &call.arguments).0)
                .unwrap_or("workspace_tool");
            let changed = output.get("changed").and_then(serde_json::Value::as_bool);
            let mutation_tool = matches!(tool, "apply_patch" | "edit" | "multiedit" | "write");
            if !failed && mutation_tool && changed == Some(true) {
                self.workspace_mutated = true;
            }
            let kind = if failed {
                WorkerGoalEvidenceKind::ToolFailure
            } else if mutation_tool && changed == Some(true) {
                WorkerGoalEvidenceKind::WorkspaceMutation
            } else if (mutation_tool && changed == Some(false))
                || matches!(tool, "read" | "grep" | "glob" | "list")
            {
                WorkerGoalEvidenceKind::WorkspaceObservation
            } else {
                WorkerGoalEvidenceKind::Runtime
            };
            let summary = if failed {
                format!("Governed workspace tool {tool} returned an error")
            } else if mutation_tool && changed == Some(true) {
                format!("Governed workspace tool {tool} reported a workspace change")
            } else if mutation_tool && changed == Some(false) {
                format!("Governed workspace tool {tool} completed without a workspace change")
            } else if mutation_tool {
                format!("Governed workspace tool {tool} completed without a trusted change status")
            } else {
                format!("Governed workspace tool {tool} completed successfully")
            };
            if self.evidence.len() < MAX_WORKER_GOAL_EVIDENCE_ITEMS {
                if let Ok(evidence) = WorkerGoalEvidence::new(kind, summary) {
                    self.evidence.push(evidence);
                } else {
                    self.overflowed = true;
                }
            }
        }
    }

    fn can_commit_outcome(&self) -> bool {
        !self.overflowed
            && self.counters.failed_tool_calls == 0
            && self.counters.successful_tool_calls > 0
            && !self.evidence.is_empty()
    }

    fn record_research_actions(&mut self, count: usize) {
        let Ok(count) = u32::try_from(count) else {
            self.overflowed = true;
            return;
        };
        let Some(total) = self.counters.research_actions.checked_add(count) else {
            self.overflowed = true;
            return;
        };
        self.counters.research_actions = total;
    }

    fn attempt_outcome(&self) -> WorkerGoalAttemptOutcome {
        // This loop can prove governed effects, not acceptance. A separate
        // typed verifier may promote the exact frozen step later.
        WorkerGoalAttemptOutcome::Progressed
    }

    fn effect_summary(&self) -> Result<WorkerGoalEffectSummary, WorkerGoalOutcomeInputError> {
        WorkerGoalEffectSummary::new(
            format!(
                "Observed {} successful governed workspace tool calls, {} failed calls, and {} research actions; explicit workspace mutation reported: {}. Acceptance was not evaluated by this runner.",
                self.counters.successful_tool_calls,
                self.counters.failed_tool_calls,
                self.counters.research_actions,
                self.workspace_mutated
            ),
            self.workspace_mutated,
        )
    }
}

enum WorkerGoalOutcomeFinalize {
    Committed,
    ProvenStale,
    Ambiguous(WorkerGoalOutcomeCommitError),
    ProviderAccountingUncertain {
        outcome_committed: bool,
        error: anyhow::Error,
    },
}

/// Persist/adopt the canonical Goal outcome before terminalizing the final
/// no-tool provider permit. Conflict or transaction uncertainty deliberately
/// leave the provider call Started for fenced crash recovery.
fn commit_worker_goal_outcome_before_provider_completion<F>(
    committer: &dyn WorkerGoalOutcomeCommitter,
    input: &WorkerGoalOutcomeCommitInput,
    complete_provider: F,
) -> WorkerGoalOutcomeFinalize
where
    F: FnOnce(WorkerProviderTerminalOutcome) -> anyhow::Result<()>,
{
    match committer.commit_outcome(input) {
        Ok(_) => match complete_provider(WorkerProviderTerminalOutcome::Completed) {
            Ok(()) => WorkerGoalOutcomeFinalize::Committed,
            Err(error) => WorkerGoalOutcomeFinalize::ProviderAccountingUncertain {
                outcome_committed: true,
                error,
            },
        },
        Err(error) if error.is_proven_stale() => {
            match complete_provider(WorkerProviderTerminalOutcome::CanonicalCommitStale) {
                Ok(()) => WorkerGoalOutcomeFinalize::ProvenStale,
                Err(error) => WorkerGoalOutcomeFinalize::ProviderAccountingUncertain {
                    outcome_committed: false,
                    error,
                },
            }
        }
        Err(error) => WorkerGoalOutcomeFinalize::Ambiguous(error),
    }
}

#[derive(Debug, Clone)]
struct LoopGuardLanding {
    diagnostic: String,
    block_goal: bool,
}

fn provider_options_for_turn(options: &CallOptions, landing: bool) -> CallOptions {
    let mut request_options = options.clone();
    if landing {
        // A loop-guard landing is exactly one synthesis request. Disable both
        // advertised function tools and provider-hosted tools so it cannot
        // extend the loop through another observation or side effect.
        request_options.tools = None;
        request_options.web_search = None;
        request_options.web_fetch = None;
        request_options.codex_parallel_tool_calls = false;
        request_options.parallel_tool_calls = Some(false);
    }
    request_options
}

fn loop_guard_landing_instruction(landing: &LoopGuardLanding) -> String {
    super::loop_kernel::loop_guard_landing_instruction(
        &landing.diagnostic,
        super::loop_kernel::LandingAudience::User,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmptyCompletionAction {
    None,
    Retry,
    Fail,
}

fn empty_completion_action(
    text: &str,
    tool_calls: &[AiToolCall],
    has_citations: bool,
    retry_attempted: bool,
    had_server_tool_activity: bool,
) -> EmptyCompletionAction {
    if !text.trim().is_empty() || !tool_calls.is_empty() || has_citations {
        EmptyCompletionAction::None
    } else if retry_attempted || had_server_tool_activity {
        EmptyCompletionAction::Fail
    } else {
        EmptyCompletionAction::Retry
    }
}

/// Only an explicit, pre-write stale-fence rejection proves that no canonical
/// response can have committed. Conflicts and commit-uncertain failures keep
/// the append-only provider call Started for exact takeover/adoption.
fn worker_response_commit_terminal_outcome(
    error: &WorkerConversationResponseCommitError,
) -> Option<WorkerProviderTerminalOutcome> {
    match error {
        WorkerConversationResponseCommitError::StaleRejected(_) => {
            Some(WorkerProviderTerminalOutcome::CanonicalCommitStale)
        }
        WorkerConversationResponseCommitError::ConflictOrCorrupt(_)
        | WorkerConversationResponseCommitError::CommitUncertain(_) => None,
    }
}

fn split_single_pending_ask_user_call<'a>(
    calls: &'a [&'a AiToolCall],
) -> Option<(&'a AiToolCall, &'a [&'a AiToolCall])> {
    calls
        .split_first()
        .map(|(primary, rejected)| (*primary, rejected))
}

fn terminal_agent_state_after_interruption(stop_reason: &LoopStopReason) -> &'static str {
    match stop_reason {
        LoopStopReason::StreamIdleTimeout | LoopStopReason::UserAbort => "idle",
        LoopStopReason::ProviderError | LoopStopReason::PinchFailed => "error",
        _ => "idle",
    }
}

fn finish_worker_governor_gate(
    db_path: &Path,
    session_id: &str,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    decision: &crate::storage::WorkerGovernorDecision,
) {
    let reason = serde_json::to_string(decision)
        .map(|decision| format!("Hive Worker provider call gated: {decision}"))
        .unwrap_or_else(|_| "Hive Worker provider call gated by durable policy".to_string());
    if let Some(next_eligible_at) = decision
        .next_eligible_at
        .as_deref()
        .and_then(|value| crate::hive::parse_utc_timestamp(value).ok())
    {
        let duration_secs = next_eligible_at
            .signed_duration_since(Utc::now())
            .num_seconds()
            .max(1) as u64;
        set_agent_state(db_path, session_id, "sleeping");
        let _ = event_tx.send(LoopEvent::AgentSleeping {
            duration_secs,
            reason,
        });
        let _ = event_tx.send(LoopEvent::Finished {
            session_id: session_id.to_string(),
            stop_reason: LoopStopReason::Sleeping,
        });
    } else {
        set_agent_state(db_path, session_id, "awaiting_input");
        let _ = event_tx.send(LoopEvent::Error { error: reason });
        let _ = event_tx.send(LoopEvent::Finished {
            session_id: session_id.to_string(),
            stop_reason: LoopStopReason::AwaitingInput,
        });
    }
}

fn finish_worker_provider_attention(
    db_path: &Path,
    session_id: &str,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    error: String,
) {
    set_agent_state(db_path, session_id, "awaiting_input");
    let _ = event_tx.send(LoopEvent::Error { error });
    let _ = event_tx.send(LoopEvent::Finished {
        session_id: session_id.to_string(),
        stop_reason: LoopStopReason::AwaitingInput,
    });
}

fn is_research_action(
    call: &AiToolCall,
    tool_results: &[Content],
    delegated_store: Option<&DelegatedRunStore>,
    session_id: &str,
) -> bool {
    if matches!(
        call.name.as_str(),
        "read" | "glob" | "grep" | "list" | "web_search" | "web_fetch"
    ) {
        return true;
    }
    if call.name != "agent" {
        return false;
    }

    let action = agent_call_action(&call.arguments);
    let source_run_id = call
        .arguments
        .get("delegated_run_id")
        .and_then(serde_json::Value::as_str);
    let returned_run_id = successful_agent_result_run_id(&call.id, tool_results);
    let returned_run = returned_run_id
        .as_deref()
        .and_then(|run_id| load_owned_delegated_run(delegated_store, run_id, session_id))
        .filter(|record| {
            record
                .parent_tool_call_id
                .as_deref()
                .is_none_or(|parent_tool_call_id| parent_tool_call_id == call.id)
        });

    match action {
        // Prefer the durable child contract over request labels. Static request
        // classification remains the fallback for a failed spawn that never
        // produced a durable run.
        "spawn" => returned_run
            .as_ref()
            .map(delegated_run_is_research)
            .unwrap_or_else(|| agent_call_is_research(&call.arguments)),
        // Resume restores its capability ceiling from storage, so a bare resume
        // cannot be classified from the top-level arguments. Prefer the newly
        // created continuation, then the source contract if the attempt failed
        // before creating one.
        "resume" => returned_run
            .as_ref()
            .map(delegated_run_is_research)
            .or_else(|| {
                source_run_id
                    .and_then(|run_id| {
                        load_owned_delegated_run(delegated_store, run_id, session_id)
                    })
                    .as_ref()
                    .map(delegated_run_is_research)
            })
            .unwrap_or(false),
        // A live followup only writes to an existing mailbox and is not a new
        // research action. A terminal followup returns a different run id after
        // it has been converted into a real Spawn; classify that durable child.
        "followup" if returned_run_id.as_deref() != source_run_id => returned_run
            .as_ref()
            .map(delegated_run_is_research)
            .unwrap_or(false),
        _ => false,
    }
}

fn successful_agent_result_run_id(tool_call_id: &str, tool_results: &[Content]) -> Option<String> {
    tool_results.iter().find_map(|result| {
        let Content::ToolResult {
            tool_use_id,
            output,
            is_error,
        } = result
        else {
            return None;
        };
        if tool_use_id != tool_call_id
            || is_error.unwrap_or(false)
            || output
                .get("is_error")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        {
            return None;
        }
        let result = output.get("result").unwrap_or(output);
        let payload = result.get("data").unwrap_or(result);
        payload
            .get("delegated_run_id")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string)
    })
}

fn load_owned_delegated_run(
    delegated_store: Option<&DelegatedRunStore>,
    delegated_run_id: &str,
    session_id: &str,
) -> Option<DelegatedRunRecord> {
    delegated_store
        .and_then(|store| store.get_run(delegated_run_id).ok().flatten())
        .filter(|record| record.parent_session_id == session_id)
}

fn delegated_run_is_research(record: &DelegatedRunRecord) -> bool {
    let capabilities = record.effective_capabilities();
    capabilities.contains(&AgentCapability::Read) && !capabilities.contains(&AgentCapability::Write)
}

fn tool_batch_made_material_progress(tool_results: &[Content]) -> bool {
    tool_results.iter().any(|result| {
        matches!(
            result,
            Content::ToolResult {
                output,
                is_error,
                ..
            } if !is_error.unwrap_or(false) && trusted_changed(output) == Some(true)
        )
    })
}

fn fail_required_recovery_persistence(
    workflow_goal_id: Option<&str>,
    db_path: &Path,
    session_id: &str,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    error: &anyhow::Error,
) {
    tracing::error!(
        session_id = %session_id,
        %error,
        "Failed to persist required awaiting-input continuation policy"
    );
    set_agent_state(db_path, session_id, "error");
    let _ = event_tx.send(LoopEvent::Error {
        error: AWAITING_INPUT_PERSISTENCE_ERROR.to_string(),
    });
    finish_run_preserving_recovery(
        workflow_goal_id,
        db_path,
        session_id,
        event_tx,
        "error",
        LoopStopReason::ProviderError,
    );
}

fn should_retry_empty_stream_interruption(
    stop_reason: Option<&LoopStopReason>,
    last_failure: Option<&stream::StreamFailure>,
    produced_output: bool,
    retry_attempted: bool,
) -> bool {
    if retry_attempted || produced_output {
        return false;
    }

    match stop_reason {
        Some(LoopStopReason::StreamIdleTimeout) => true,
        Some(LoopStopReason::ProviderError) => {
            matches!(
                last_failure,
                Some(stream::StreamFailure::RetryableTransport(_))
            )
        }
        _ => false,
    }
}

/// Close every unexecuted provider call at a control boundary. Cancelled tool
/// batches may contain partial results; preserve those and fill only missing IDs.
fn close_unexecuted_tool_calls(
    calls: &[AiToolCall],
    results: &mut Vec<Content>,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    reason: &str,
) {
    for call in calls {
        if results.iter().any(|result| matches!(result, Content::ToolResult { tool_use_id, .. } if tool_use_id == &call.id)) {
            continue;
        }
        let output = serde_json::json!({"ok":false,"error":{"code":"execution_interrupted","message":reason}});
        results.push(Content::ToolResult {
            tool_use_id: call.id.clone(),
            output: output.clone(),
            is_error: Some(true),
        });
        let _ = event_tx.send(LoopEvent::ToolResult {
            id: call.id.clone(),
            output: output.to_string(),
            is_error: true,
        });
    }
}

/// Preserve completed siblings, close every remaining call, and settle usage
/// before a cancelled batch exits. AskUser batches share this same boundary.
#[allow(clippy::too_many_arguments)]
fn finish_cancelled_tool_batch(
    db_path: &Path,
    session_id: &str,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    active_goal: bool,
    tracker: &mut AttemptProgressTracker,
    calls: &[AiToolCall],
    mut results: Vec<Content>,
    run_counts: (usize, usize, usize),
    current_research_actions: usize,
) {
    close_unexecuted_tool_calls(
        calls,
        &mut results,
        event_tx,
        "Cancelled before this tool could execute",
    );
    if active_goal {
        if let Err(error) = record_active_attempt_progress(
            db_path,
            session_id,
            tracker,
            run_counts.0,
            run_counts.1,
            run_counts.2,
            calls.len(),
            current_research_actions,
            tool_batch_made_material_progress(&results),
            None,
        ) {
            let _ = event_tx.send(LoopEvent::Error {
                error: format!("Goal cancellation accounting could not be persisted: {error}"),
            });
        }
    }
    save_message(
        db_path,
        session_id,
        &ModelMessage {
            role: Role::User,
            content: results,
        },
    );
    finish_run(
        tracker.goal_id(),
        db_path,
        session_id,
        event_tx,
        "idle",
        LoopStopReason::UserAbort,
    );
}

fn publish_workflow_transition(
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    result: Result<Option<crate::workflow::WorkflowMutation>, crate::workflow::WorkflowError>,
) -> bool {
    match result {
        Ok(Some(mutation)) => {
            let _ = event_tx.send(LoopEvent::WorkflowUpdated {
                goal_id: mutation.snapshot.goal.id,
                aggregate_revision: mutation.snapshot.aggregate_revision,
                operation_id: mutation.operation_id,
            });
            true
        }
        Ok(None) => true,
        Err(error) => {
            let _ = event_tx.send(LoopEvent::Error {
                error: format!("Unable to persist the Goal transition: {error}"),
            });
            false
        }
    }
}

/// Terminal loop exit shared by every stop path that clears recovery state:
/// clear, record the terminal agent state, and emit the single canonical
/// `Finished` event. Sites whose recovery state is already clear (for example
/// after tool-result persistence) treat the clear as a no-op normalization.
fn finish_run(
    workflow_goal_id: Option<&str>,
    db_path: &Path,
    session_id: &str,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    terminal_state: &str,
    stop_reason: LoopStopReason,
) {
    clear_recovery_state(db_path, session_id);
    finish_run_preserving_recovery(
        workflow_goal_id,
        db_path,
        session_id,
        event_tx,
        terminal_state,
        stop_reason,
    );
}

fn finish_run_preserving_recovery(
    workflow_goal_id: Option<&str>,
    db_path: &Path,
    session_id: &str,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    terminal_state: &str,
    stop_reason: LoopStopReason,
) {
    let stop_reason = if let Some(goal_id) = workflow_goal_id {
        match finish_active_attempt_for_stop(
            db_path,
            session_id,
            goal_id,
            &format!("run_{stop_reason:?}"),
        ) {
            Ok(Some(mutation)) => {
                let _ = event_tx.send(LoopEvent::WorkflowUpdated {
                    goal_id: mutation.snapshot.goal.id,
                    aggregate_revision: mutation.snapshot.aggregate_revision,
                    operation_id: mutation.operation_id,
                });
                stop_reason
            }
            Ok(None) => stop_reason,
            Err(error) => {
                let _ = event_tx.send(LoopEvent::Error {
                    error: format!("Unable to checkpoint the Goal attempt at run exit: {error}"),
                });
                LoopStopReason::ProviderError
            }
        }
    } else {
        stop_reason
    };
    set_agent_state(db_path, session_id, terminal_state);
    let _ = event_tx.send(LoopEvent::Finished {
        session_id: session_id.to_string(),
        stop_reason,
    });
}

/// Continue the loop after accepted steering: recovery cleared, streaming
/// state recorded, and the turn boundary emitted before the steering events.
fn emit_steering_turn_boundary(
    db_path: &Path,
    session_id: &str,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    iteration: usize,
    injected_steering: Vec<InjectedSteering>,
) {
    clear_recovery_state(db_path, session_id);
    set_agent_state(db_path, session_id, "streaming");
    let _ = event_tx.send(LoopEvent::TurnComplete {
        turn: iteration,
        has_more: true,
    });
    emit_steering_events(event_tx, injected_steering);
}

/// Fresh per-turn recovery state at a steering boundary: a new user directive
/// re-arms every one-shot retry budget and clears any pending landing turn.
#[allow(clippy::too_many_arguments)]
fn reset_recovery_flags(
    loop_guard: &mut LoopGuard,
    active_goal: bool,
    loop_guard_landing: &mut Option<LoopGuardLanding>,
    delegation_nudge_tracker: &mut DelegationNudgeTracker,
    empty_stream_retry_attempted: &mut bool,
    empty_completion_retry_attempted: &mut bool,
    empty_completion_recovery_pending: &mut bool,
    provider_tool_activity_seen: &mut bool,
    overflow_compact_retry_attempted: &mut bool,
    goal_final_continuation_attempted: &mut bool,
) {
    *goal_final_continuation_attempted = false;
    *loop_guard_landing = None;
    delegation_nudge_tracker.reset_for_steering();
    *empty_stream_retry_attempted = false;
    *empty_completion_retry_attempted = false;
    *empty_completion_recovery_pending = false;
    *provider_tool_activity_seen = false;
    *overflow_compact_retry_attempted = false;
    if !active_goal {
        loop_guard.reset_for_steering();
    }
}

/// Compact once in place and adopt the result for an immediate retry.
/// Returns the compaction error when compaction failed; the caller owns the
/// terminal path.
#[allow(clippy::too_many_arguments)]
async fn compact_and_retry_once(
    db_path: &Path,
    session_id: &str,
    conversation: &mut Vec<ModelMessage>,
    context_ledger: &mut ContextLedger,
    working_dir: &Path,
    project_dir: Option<&str>,
    user_id: Option<&str>,
    ai_client: &AiClient,
    compaction_manager: &CompactionManager,
    trigger: CompactionTrigger,
    last_usage_prompt_tokens: &mut Option<usize>,
    messages_at_last_usage: &mut usize,
    last_token_count: &mut usize,
    request_budget: Option<CompactionRequestBudget>,
    provider_call_trace: &super::ProviderCallTraceContext,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
) -> Result<(), anyhow::Error> {
    let messages_after_usage = conversation.len().saturating_sub(*messages_at_last_usage);
    let result = apply_in_place_compaction(
        db_path,
        session_id,
        conversation,
        context_ledger,
        working_dir,
        project_dir,
        user_id,
        ai_client,
        compaction_manager,
        trigger,
        *last_usage_prompt_tokens,
        messages_after_usage,
        request_budget,
        provider_call_trace,
        event_tx,
    )
    .await?;
    *last_usage_prompt_tokens = None;
    *messages_at_last_usage = conversation.len();
    *last_token_count = result.estimated_tokens_after;
    update_token_count(db_path, session_id, result.estimated_tokens_after);
    clear_recovery_state(db_path, session_id);
    set_agent_state(db_path, session_id, "streaming");
    Ok(())
}

/// Configuration for an orchestrator run.
pub(crate) struct OrchestratorConfig {
    pub(crate) session_id: String,
    pub(crate) working_dir: PathBuf,
    pub(crate) project_dir: Option<PathBuf>,
    pub(crate) hive_crew_slug: Option<String>,
    /// Group linkage when this run is one member of a Hive group turn. It
    /// scopes the [GROUP ROOM] context block and the post_to_group tool.
    pub(crate) hive_group_run: Option<HiveGroupRunContext>,
    /// Database-owned Mako identity frozen once at run start.
    pub(crate) hive_profile: Option<Arc<HiveProfileSnapshot>>,
    /// Typed prompt/capability boundary. The default retains ordinary
    /// workspace-aware orchestration; neutral Worker conversations use only
    /// their exact durable persona and conversation continuity.
    pub(crate) context_mode: RunContextMode,
    pub(crate) session_type: SessionType,
    pub(crate) permission_mode: PermissionMode,
    /// Optional explicit per-turn execution capability. `None` preserves the
    /// normal governed deferred-tool surface; `Some`, including an empty set,
    /// also constrains effective wrapper targets such as `tool_search`.
    pub(crate) execution_tool_allowlist: Option<HashSet<String>>,
    /// Rebuild the governed Code tool schemas whenever effective work mode
    /// changes. Disabled for intentionally tool-free turns and non-Code runs.
    pub(crate) refresh_code_tools_on_mode_change: bool,
    /// Typed parent-run budget. `Some(RunBudget::unlimited())` explicitly
    /// overrides repository limits; `None` allows project/default resolution.
    pub(crate) run_budget: Option<RunBudget>,
    pub(crate) stream_idle_timeout: std::time::Duration,
    pub(crate) user_id: Option<String>,
    pub(crate) initial_work_mode: WorkMode,
    /// Whether to generate a title on first AI response.
    /// Set to true for new sessions, false for resumed conversations.
    pub(crate) generate_title: bool,
    /// Optional explore delegated progress channel for external surfaces.
    pub(crate) delegated_progress_tx: Option<mpsc::UnboundedSender<DelegatedProgressEvent>>,
    /// Exact claimed Worker/run provider capability. This lives on RunSpec's
    /// immutable configuration rather than global services so non-Worker
    /// callers and existing service construction remain unchanged.
    pub(crate) provider_governor: Option<Arc<super::WorkerProviderCallGovernor>>,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            working_dir: PathBuf::new(),
            project_dir: None,
            hive_crew_slug: None,
            hive_group_run: None,
            hive_profile: None,
            context_mode: RunContextMode::Standard,
            session_type: SessionType::Code,
            permission_mode: PermissionMode::default(),
            execution_tool_allowlist: None,
            refresh_code_tools_on_mode_change: false,
            run_budget: None,
            stream_idle_timeout: constants::http::STREAM_TIMEOUT,
            user_id: None,
            initial_work_mode: WorkMode::default(),
            generate_title: false,
            delegated_progress_tx: None,
            provider_governor: None,
        }
    }
}

/// Shared services the orchestrator needs.
#[derive(Clone)]
pub struct OrchestratorServices {
    pub ai_client: Arc<AiClient>,
    pub tool_registry: Arc<ToolRegistry>,
    pub process_registry: Arc<ProcessRegistry>,
    pub db_path: PathBuf,
    pub skills_manager: Arc<RwLock<SkillsManager>>,
}

fn resolve_project_permission_mode(
    requested_permission_mode: PermissionMode,
    project_settings: &ProjectSettings,
) -> PermissionMode {
    let Some(ref mode_str) = project_settings.permission_mode else {
        return requested_permission_mode;
    };

    match mode_str.as_str() {
        "supervised" => {
            tracing::info!("Project settings override: permission_mode = supervised");
            PermissionMode::Supervised
        }
        "autonomous" if requested_permission_mode == PermissionMode::Autonomous => {
            tracing::info!("Project settings confirm: permission_mode = autonomous");
            PermissionMode::Autonomous
        }
        "autonomous" => {
            tracing::warn!(
                "Ignoring project settings permission_mode = autonomous because the request is supervised"
            );
            requested_permission_mode
        }
        other => {
            tracing::warn!(
                "Unknown permission_mode in project settings: {:?}, keeping requested mode",
                other
            );
            requested_permission_mode
        }
    }
}

/// The agentic orchestrator — runs the complete AI agent loop.
pub(crate) struct AgenticOrchestrator {
    services: OrchestratorServices,
    config: OrchestratorConfig,
}

fn session_type_name(session_type: SessionType) -> &'static str {
    match session_type {
        SessionType::Chat => "chat",
        SessionType::Code => "code",
        SessionType::Hive => "hive",
    }
}

#[allow(clippy::too_many_arguments)]
fn inject_runtime_context(
    conversation: &[ModelMessage],
    db_path: &Path,
    session_id: &str,
    working_dir: &Path,
    project_dir: Option<&Path>,
    hive_crew_slug: Option<&str>,
    hive_group_run: Option<&HiveGroupRunContext>,
    hive_profile: Option<&HiveProfileSnapshot>,
    work_mode: WorkMode,
    skills_manager: &RwLock<SkillsManager>,
    model: Option<&str>,
    session_type: SessionType,
    user_id: Option<&str>,
) -> Vec<ModelMessage> {
    context::inject_context_with_hive_profile_and_group(
        conversation,
        db_path,
        session_id,
        working_dir,
        project_dir,
        work_mode,
        skills_manager,
        model,
        Some(session_type_name(session_type)),
        hive_crew_slug,
        user_id,
        hive_profile,
        hive_group_run,
    )
}

impl AgenticOrchestrator {
    pub(crate) fn new(services: OrchestratorServices, config: OrchestratorConfig) -> Self {
        Self { services, config }
    }

    /// Start the agentic loop.
    ///
    /// Returns `(event_receiver, input_sender)`. The loop runs as a spawned
    /// tokio task. It emits `LoopEvent`s for every state change. The caller
    /// sends `LoopInput`s for user interactions (approvals, AskUser responses,
    /// cancellation).
    pub(crate) fn run(
        self,
        conversation: Vec<ModelMessage>,
        options: CallOptions,
    ) -> (
        mpsc::UnboundedReceiver<LoopEvent>,
        mpsc::UnboundedSender<LoopInput>,
    ) {
        let (trace_tx, trace_rx) = mpsc::unbounded_channel();
        let (provider_call_tx, provider_call_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (input_tx, input_rx) = mpsc::unbounded_channel();
        let trace_db_path = self.services.db_path.clone();
        let trace_session_id = self.config.session_id.clone();
        let trace_run_id = super::observability::new_runtime_trace_run_id();
        let extension_dispatch = if self.config.context_mode.is_isolated_worker() {
            None
        } else {
            self.services
                .tool_registry
                .agent_extension_manager()
                .map(|manager| {
                    let context = crate::extensions::ExtensionCallContext::for_resolved_turn(
                        self.config.working_dir.clone(),
                        self.config.project_dir.clone(),
                        Some(self.config.session_id.clone()),
                        self.services.ai_client.resolved_model(),
                        format!("{:?}", self.config.permission_mode).to_ascii_lowercase(),
                        matches!(self.config.initial_work_mode, WorkMode::Plan),
                    );
                    (manager, context)
                })
        };

        tokio::spawn(async move {
            super::observability::forward_runtime_traces(
                trace_db_path,
                trace_session_id,
                trace_run_id,
                trace_rx,
                provider_call_rx,
                event_tx,
                extension_dispatch,
            )
            .await;
        });

        tokio::spawn(async move {
            self.run_inner(conversation, options, trace_tx, provider_call_tx, input_rx)
                .await;
        });

        (event_rx, input_tx)
    }

    async fn run_inner(
        self,
        mut conversation: Vec<ModelMessage>,
        options: CallOptions,
        event_tx: mpsc::UnboundedSender<LoopEvent>,
        provider_call_tx: mpsc::UnboundedSender<super::observability::ProviderCallTrace>,
        input_rx: mpsc::UnboundedReceiver<LoopInput>,
    ) {
        let mut input_inbox = LoopInputInbox::new(input_rx);
        let OrchestratorServices {
            ai_client,
            tool_registry,
            process_registry,
            db_path,
            skills_manager,
        } = self.services;

        let OrchestratorConfig {
            session_id,
            working_dir,
            project_dir,
            hive_crew_slug,
            hive_group_run,
            hive_profile,
            context_mode,
            session_type,
            permission_mode,
            execution_tool_allowlist,
            refresh_code_tools_on_mode_change,
            run_budget,
            stream_idle_timeout,
            user_id,
            initial_work_mode,
            generate_title,
            delegated_progress_tx,
            provider_governor,
        } = self.config;

        let canonical_session_persistence =
            CanonicalSessionPersistenceBoundary::for_context_mode(&context_mode);
        // Keep the existing call sites structurally identical while routing
        // every generic session write through the WorkerGoal isolation gate.
        let persist_context_state = |db_path: &Path, session_id: &str, ledger: &ContextLedger| {
            canonical_session_persistence.persist_context_state(db_path, session_id, ledger);
        };
        let persist_recovery_state =
            |db_path: &Path, session_id: &str, recovery: &crate::storage::SessionRecoveryState| {
                canonical_session_persistence.persist_recovery_state(db_path, session_id, recovery);
            };
        let persist_required_recovery_state =
            |db_path: &Path, session_id: &str, recovery: &crate::storage::SessionRecoveryState| {
                canonical_session_persistence
                    .persist_required_recovery_state(db_path, session_id, recovery)
            };
        let clear_recovery_state = |db_path: &Path, session_id: &str| {
            canonical_session_persistence.clear_recovery_state(db_path, session_id);
        };
        let save_message = |db_path: &Path, session_id: &str, message: &ModelMessage| {
            canonical_session_persistence.save_message(db_path, session_id, message);
        };
        let update_token_count = |db_path: &Path, session_id: &str, count: usize| {
            canonical_session_persistence.update_token_count(db_path, session_id, count);
        };

        // Load per-project settings from .krusty/settings.json
        let project_settings = if context_mode.is_isolated_worker() {
            ProjectSettings::default()
        } else {
            ProjectSettings::load(project_dir.as_deref().unwrap_or(&working_dir))
        };

        // WorkerGoal carries a frozen Workflow attempt, but the ordinary
        // session orchestrator is not its lifecycle authority. The Hive run
        // host must settle that exact binding through its typed outcome and
        // evidence transaction rather than letting generic session helpers
        // select or mutate live Goal rows by session id.
        let manages_workflow_lifecycle = matches!(&context_mode, RunContextMode::Standard);
        let mut active_goal =
            manages_workflow_lifecycle && active_goal_for_run(&db_path, &session_id);
        let run_budget = if active_goal {
            RunBudgetResolution::resolve_goal_attempt(run_budget, project_settings.run_limits)
        } else {
            RunBudgetResolution::resolve(run_budget, project_settings.run_limits)
        };

        let permission_mode = resolve_project_permission_mode(permission_mode, &project_settings);
        // Resolve provider/model reasoning and tool capabilities once at the
        // run boundary. The same canonical effort then governs parent calls,
        // delegated children, durable replay, and observability.
        let mut options = options.canonicalized_for_runtime(ai_client.resolved_model());
        let mode_tool_surface = ModeAwareToolSurface::capture(
            refresh_code_tools_on_mode_change,
            &options,
            tool_registry.as_ref(),
        )
        .await;
        let mut work_mode = initial_work_mode;
        // Freeze the complete registry catalog once, then derive the exact
        // provider-facing surface for the effective initial mode. The same
        // helper runs at every later transition, so request schemas and
        // execution authorization cannot drift apart.
        let mut advertised_tool_names = advertised_names(&options);
        mode_tool_surface.refresh(
            &mut options,
            &mut advertised_tool_names,
            ai_client.as_ref(),
            permission_mode,
            work_mode,
            !context_mode.is_isolated_worker()
                && has_active_workflow_or_plan(&db_path, &session_id),
            project_settings
                .disabled_tools
                .as_deref()
                .unwrap_or_default(),
            execution_tool_allowlist.as_ref(),
        );
        let mut last_token_count = 0usize;
        let mut last_usage_prompt_tokens = None::<usize>;
        let mut messages_at_last_usage = 0usize;
        let mut loop_guard = LoopGuard::new();
        let mut delegation_nudge_tracker = DelegationNudgeTracker::new();
        let mut title_generated = !generate_title;
        let mut iteration = 0usize;
        let mut goal_tool_call_count = 0usize;
        let mut goal_research_action_count = 0usize;
        let mut attempt_progress_tracker = AttemptProgressTracker::default();
        let mut goal_final_continuation_attempted = false;
        let mut worker_goal_outcome_journal = WorkerGoalOutcomeJournal::default();
        let mut loop_guard_landing = None::<LoopGuardLanding>;
        let model_context_window = effective_context_window_for_runtime(
            ai_client.config().uses_chatgpt_codex_format(),
            ai_client.resolved_model().capabilities.context_window,
        );
        let compaction_manager = CompactionManager::for_model(
            ai_client.provider_id(),
            ai_client.config().api_format,
            &ai_client.config().model,
            model_context_window,
        );
        let mut context_ledger = ContextLedger::from_conversation(&conversation);
        let mut empty_stream_retry_attempted = false;
        let mut empty_completion_retry_attempted = false;
        let mut empty_completion_recovery_pending = false;
        let mut provider_tool_activity_seen = false;
        let mut overflow_compact_retry_attempted = false;
        let mut stale_compaction_reload_attempted = false;
        let mut last_microcompact_history_message_count = conversation.len();
        let mut microcompaction_generation = 0usize;
        let project_dir_key = project_dir
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned());
        let project_dir_for_compaction = project_dir_key.as_deref();
        let user_id_for_compaction = user_id.as_deref();
        let file_observations = Arc::new(FileObservationTracker::default());
        persist_context_state(&db_path, &session_id, &context_ledger);
        clear_recovery_state(&db_path, &session_id);

        let transport_policy =
            StreamTransportPolicy::resolve(ai_client.provider_id(), ai_client.config().api_format);
        let effective_stream_idle_timeout = stream_idle_timeout.max(transport_policy.idle_timeout);

        set_agent_state(&db_path, &session_id, "streaming");
        tracing::info!(
            max_turns = ?run_budget.budget.max_turns,
            source = ?run_budget.source,
            "Resolved parent agent run budget"
        );
        let _ = event_tx.send(LoopEvent::RunBudgetResolved {
            max_turns: run_budget.budget.max_turns,
            source: run_budget.source,
        });

        loop {
            input_inbox.collect_ready();
            if context_mode.is_isolated_worker() {
                // Worker conversation inputs are accepted and sequenced by
                // the durable Hive input table. Live steering/workflow wakes
                // must not splice a second user boundary into this claimed
                // one-response run. Dropping delivery here leaves any staged
                // durable input unpromoted for the next run.
                let _ = input_inbox.take_steering();
                let _ = input_inbox.take_workflow_updates();
            } else {
                emit_workflow_update_inputs(&event_tx, input_inbox.take_workflow_updates());
            }
            if input_inbox.take_cancel() {
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    LoopStopReason::UserAbort,
                );
                return;
            }
            if manages_workflow_lifecycle {
                let binding = (|| -> anyhow::Result<_> {
                    let manager = WorkflowManager::new(db_path.clone())?;
                    if let Some(mutation) =
                        ensure_active_goal_attempt_for_run(&db_path, &session_id, permission_mode)?
                    {
                        let _ = event_tx.send(LoopEvent::WorkflowUpdated {
                            goal_id: mutation.snapshot.goal.id,
                            aggregate_revision: mutation.snapshot.aggregate_revision,
                            operation_id: mutation.operation_id,
                        });
                    }
                    manager.get_snapshot(&session_id).map_err(Into::into)
                })();
                let snapshot = match binding {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        let _ = event_tx.send(LoopEvent::Error {
                            error: format!("Unable to bind Goal execution: {error}"),
                        });
                        finish_run(
                            attempt_progress_tracker.goal_id(),
                            &db_path,
                            &session_id,
                            &event_tx,
                            "error",
                            LoopStopReason::ProviderError,
                        );
                        return;
                    }
                };
                // Durable controls still stop this run when their live
                // notification raced with input-channel registration. A
                // completed Goal may receive its final synthesis below.
                if active_goal
                    && snapshot.as_ref().is_some_and(|snapshot| {
                        matches!(
                            snapshot.goal.status,
                            GoalStatus::Paused | GoalStatus::Blocked | GoalStatus::Cancelled
                        )
                    })
                {
                    finish_run(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        "idle",
                        LoopStopReason::AwaitingInput,
                    );
                    return;
                }
                active_goal = snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.goal.status == GoalStatus::Active);
                if active_goal {
                    work_mode = WorkMode::Build;
                    if !snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.latest_attempt.as_ref())
                        .is_some_and(|attempt| attempt.status == AttemptStatus::Running)
                    {
                        let _ = event_tx.send(LoopEvent::Error { error: "The active Goal has no runnable attempt. Its workflow changed or its remaining steps need attention; no provider work was admitted.".into() });
                        finish_run(
                            attempt_progress_tracker.goal_id(),
                            &db_path,
                            &session_id,
                            &event_tx,
                            "awaiting_input",
                            LoopStopReason::AwaitingInput,
                        );
                        return;
                    }
                }
                if let Some(attempt) = snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.latest_attempt.as_ref())
                    .filter(|attempt| attempt.status == AttemptStatus::Running)
                {
                    attempt_progress_tracker.bind(
                        attempt,
                        iteration,
                        goal_tool_call_count,
                        goal_research_action_count,
                    );
                }
                mode_tool_surface.refresh(
                    &mut options,
                    &mut advertised_tool_names,
                    ai_client.as_ref(),
                    permission_mode,
                    work_mode,
                    has_active_workflow_or_plan(&db_path, &session_id),
                    project_settings
                        .disabled_tools
                        .as_deref()
                        .unwrap_or_default(),
                    execution_tool_allowlist.as_ref(),
                );
            }
            let injected_steering = if context_mode.is_isolated_worker() {
                Vec::new()
            } else {
                inject_pending_steering(
                    &mut input_inbox,
                    &mut conversation,
                    &mut context_ledger,
                    &db_path,
                    &session_id,
                )
            };
            if !injected_steering.is_empty() {
                emit_steering_events(&event_tx, injected_steering);
                reset_recovery_flags(
                    &mut loop_guard,
                    active_goal,
                    &mut loop_guard_landing,
                    &mut delegation_nudge_tracker,
                    &mut empty_stream_retry_attempted,
                    &mut empty_completion_retry_attempted,
                    &mut empty_completion_recovery_pending,
                    &mut provider_tool_activity_seen,
                    &mut overflow_compact_retry_attempted,
                    &mut goal_final_continuation_attempted,
                );
            }

            if loop_guard_landing.is_none() && run_budget.budget.is_exhausted(iteration) {
                let message = run_budget
                    .budget
                    .max_turns
                    .map(|max| format!("Agent turn budget exhausted after {} turns", max))
                    .unwrap_or_else(|| "Agent turn budget exhausted".to_string());
                let _ = event_tx.send(LoopEvent::Error { error: message });
                if last_token_count > 0 {
                    update_token_count(&db_path, &session_id, last_token_count);
                }
                if active_goal {
                    publish_workflow_transition(
                        &event_tx,
                        pause_active_goal_for_stop(&db_path, &session_id, "turn_budget_exhausted"),
                    );
                }
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    LoopStopReason::BudgetExhausted,
                );
                return;
            }
            // A finite interactive Code run spends its last available response
            // reporting evidence, not starting another tool batch. This stays
            // inside the configured provider budget; durable Goals and workers
            // retain their own attempt/outcome rules. A one-turn run still gets
            // its initial tool-capable response.
            if loop_guard_landing.is_none()
                && !active_goal
                && !context_mode.is_isolated_worker()
                && session_type == SessionType::Code
                && iteration > 0
                && run_budget.budget.has_one_turn_remaining(iteration)
            {
                let landing = LoopGuardLanding {
                    diagnostic: "This is the last response available within this run's configured turn budget. Report what the existing evidence proves, any unfinished work, and any failed or unrun required checks. Reaching the limit does not prove task completion; do not invent verification or promise another action in this run.".into(),
                    block_goal: false,
                };
                conversation.push(ModelMessage {
                    role: Role::System,
                    content: vec![Content::text(loop_guard_landing_instruction(&landing))],
                });
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
                loop_guard_landing = Some(landing);
            }
            // Soft pressure for unlimited interactive runs only. Goal/project
            // budgets already hard-stop; this keeps long thrash visible without
            // killing legitimate multi-turn builds.
            if matches!(
                run_budget.source,
                super::state::RunBudgetSource::UnlimitedDefault
            ) && run_budget.budget.max_turns.is_none()
            {
                use super::state::{INTERACTIVE_SOFT_TURN_REPLAN, INTERACTIVE_SOFT_TURN_WARN};
                let soft_message = if iteration + 1 == INTERACTIVE_SOFT_TURN_REPLAN {
                    Some(format!(
                        "[SOFT TURN BUDGET]\nYou are on turn {}. Long interactive runs should synthesize, act, or ask the user — not re-poll the same status. Prefer background jobs with completion wake over repeated status checks.",
                        iteration + 1
                    ))
                } else if iteration + 1 == INTERACTIVE_SOFT_TURN_WARN {
                    Some(format!(
                        "[SOFT TURN BUDGET]\nTurn {} of an unlimited interactive run. If you are waiting on CI/builds, detach and continue; do not burn turns on identical status polls.",
                        iteration + 1
                    ))
                } else {
                    None
                };
                if let Some(text) = soft_message {
                    conversation.push(ModelMessage {
                        role: Role::System,
                        content: vec![Content::text(text)],
                    });
                }
            }
            iteration += 1;
            let provider_call_trace = super::observability::ProviderCallTraceContext::for_run(
                provider_call_tx.clone(),
                iteration,
            )
            .with_provider_governor(provider_governor.clone());

            if conversation.len() < last_microcompact_history_message_count {
                last_microcompact_history_message_count = conversation.len();
            }
            let rewrite_history = should_rewrite_microcompact_history(
                conversation.len(),
                last_microcompact_history_message_count,
            );
            let message_count = conversation.len();
            let micro = microcompact_messages_cache_aware(&conversation, rewrite_history);
            if rewrite_history {
                last_microcompact_history_message_count = message_count;
            }
            if micro.changed {
                microcompaction_generation = microcompaction_generation.saturating_add(1);
                let _ = event_tx.send(LoopEvent::MicrocompactionApplied {
                    turn: iteration,
                    generation: microcompaction_generation,
                    message_count,
                    history_rewritten: micro.history_rewritten,
                    tool_inputs_rewritten: micro.tool_inputs_rewritten,
                });
                conversation = micro.messages;
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
            }

            // Keep assistant findings and decisions alongside tool calls;
            // text has no reliable status-only classification.
            let model_history = &conversation;
            let mut conversation_with_context = match &context_mode {
                RunContextMode::WorkerConversation { worker_id, .. } => {
                    match context::inject_worker_conversation_context(
                        model_history,
                        &db_path,
                        &session_id,
                        worker_id,
                        user_id.as_deref(),
                        hive_group_run.as_ref(),
                    ) {
                        Ok(conversation) => conversation,
                        Err(error) => {
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "Hive Worker conversation context failed closed before provider access: {error}"
                                ),
                            );
                            return;
                        }
                    }
                }
                RunContextMode::WorkerGoal {
                    context: goal_context,
                    ..
                } => {
                    let Some(allowlist) = execution_tool_allowlist.as_ref() else {
                        finish_worker_provider_attention(
                            &db_path,
                            &session_id,
                            &event_tx,
                            "Hive Worker Goal tool capability is unavailable".to_string(),
                        );
                        return;
                    };
                    match context::inject_worker_goal_context(
                        model_history,
                        &db_path,
                        &session_id,
                        user_id.as_deref(),
                        goal_context,
                        allowlist,
                    ) {
                        Ok(conversation) => conversation,
                        Err(error) => {
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "Hive Worker Goal context failed closed before provider access: {error}"
                                ),
                            );
                            return;
                        }
                    }
                }
                RunContextMode::Standard => inject_runtime_context(
                    model_history,
                    &db_path,
                    &session_id,
                    &working_dir,
                    project_dir.as_deref(),
                    hive_crew_slug.as_deref(),
                    hive_group_run.as_ref(),
                    hive_profile.as_deref(),
                    work_mode,
                    &skills_manager,
                    Some(ai_client.config().model.as_str()),
                    session_type,
                    user_id.as_deref(),
                ),
            };
            if !context_mode.is_isolated_worker() {
                if let Some(extension_manager) = tool_registry.agent_extension_manager() {
                    let extension_context =
                        crate::extensions::ExtensionCallContext::for_resolved_turn(
                            working_dir.clone(),
                            project_dir.clone(),
                            Some(session_id.clone()),
                            ai_client.resolved_model(),
                            format!("{:?}", permission_mode).to_ascii_lowercase(),
                            matches!(work_mode, WorkMode::Plan),
                        );
                    let extension_context_additions =
                        extension_manager.context_for_turn(&extension_context).await;
                    if !extension_context_additions.is_empty() {
                        conversation_with_context.push(ModelMessage {
                            role: Role::System,
                            content: vec![Content::text(format!(
                                "[AGENT EXTENSION CONTEXT]\n\n{}\n\n[END AGENT EXTENSION CONTEXT]",
                                extension_context_additions.join("\n\n")
                            ))],
                        });
                    }
                }
            }
            if empty_completion_recovery_pending {
                conversation_with_context.push(ModelMessage {
                    role: Role::System,
                    content: vec![Content::text(
                        EMPTY_COMPLETION_RECOVERY_INSTRUCTION.to_string(),
                    )],
                });
            }
            let request_options = provider_options_for_turn(&options, loop_guard_landing.is_some());
            let request_estimate = super::estimate_rendered_request_tokens(
                ai_client.as_ref(),
                &conversation_with_context,
                &request_options,
            );
            let usage_calibrated_estimate = super::estimate_tokens_with_usage(
                &conversation,
                last_usage_prompt_tokens,
                conversation.len().saturating_sub(messages_at_last_usage),
            );
            let estimated_tokens_before =
                request_estimate.total_tokens.max(usage_calibrated_estimate);
            let compaction_request_budget =
                request_estimate.compaction_budget(estimated_tokens_before);
            tracing::debug!(
                session_id = %session_id,
                base_prompt_tokens = request_estimate.base_prompt_tokens,
                project_context_tokens = request_estimate.project_context_tokens,
                session_context_tokens = request_estimate.session_context_tokens,
                message_tokens = request_estimate.message_tokens,
                tool_tokens = request_estimate.tool_tokens,
                usage_calibrated_tokens = usage_calibrated_estimate,
                total_tokens = request_estimate.total_tokens,
                compaction_pressure_tokens = estimated_tokens_before,
                "Preflight rendered-request token estimate"
            );

            if compaction_manager.should_compact(estimated_tokens_before) {
                if context_mode.is_isolated_worker() {
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        "Hive Worker run exceeded its bounded context window; ambient session compaction was not invoked"
                            .to_string(),
                    );
                    return;
                }
                if let Err(error) = compact_and_retry_once(
                    &db_path,
                    &session_id,
                    &mut conversation,
                    &mut context_ledger,
                    &working_dir,
                    project_dir_for_compaction,
                    user_id_for_compaction,
                    ai_client.as_ref(),
                    &compaction_manager,
                    CompactionTrigger::Auto,
                    &mut last_usage_prompt_tokens,
                    &mut messages_at_last_usage,
                    &mut last_token_count,
                    Some(compaction_request_budget),
                    &provider_call_trace,
                    &event_tx,
                )
                .await
                {
                    if !stale_compaction_reload_attempted
                        && is_stale_compaction_snapshot_error(&error)
                    {
                        match reload_persisted_conversation(&db_path, &session_id) {
                            Ok(reloaded) if !reloaded.is_empty() => {
                                tracing::warn!(
                                    session_id = %session_id,
                                    message_count = reloaded.len(),
                                    %error,
                                    "Reloaded canonical transcript after compaction snapshot race"
                                );
                                conversation = reloaded;
                                context_ledger.update_from_conversation(&conversation);
                                persist_context_state(&db_path, &session_id, &context_ledger);
                                last_usage_prompt_tokens = None;
                                messages_at_last_usage = conversation.len();
                                last_microcompact_history_message_count = conversation.len();
                                stale_compaction_reload_attempted = true;
                                continue;
                            }
                            Ok(_) => {}
                            Err(reload_error) => {
                                tracing::warn!(
                                    session_id = %session_id,
                                    %reload_error,
                                    "Failed to reload canonical transcript after compaction snapshot race"
                                );
                            }
                        }
                    }
                    let _ = event_tx.send(LoopEvent::Error {
                        error: format!("Automatic compaction failed: {}", error),
                    });
                    if last_token_count > 0 {
                        update_token_count(&db_path, &session_id, last_token_count);
                    }
                    if active_goal {
                        publish_workflow_transition(
                            &event_tx,
                            pause_active_goal_for_stop(&db_path, &session_id, "pinch_failed"),
                        );
                    }
                    finish_run(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        "error",
                        LoopStopReason::PinchFailed,
                    );
                    return;
                }
            }

            let provider_call_slot = WorkerProviderCallSlot::new(
                WorkerProviderCallKind::AgentTurn,
                u32::try_from(iteration).unwrap_or(u32::MAX),
                0,
            );
            let reserved_tokens = bounded_reservation(
                request_estimate.total_tokens,
                request_options
                    .max_tokens
                    .unwrap_or(ai_client.config().max_tokens),
            );
            let (provider_call_id, provider_call_permit) = if let Some(governor) =
                provider_governor.as_ref()
            {
                match governor.admit(provider_call_slot, reserved_tokens) {
                    Ok(WorkerProviderAdmission::Allowed(permit)) => {
                        let provider_call_id = permit.provider_call_id().to_string();
                        if context_mode.is_worker_goal() {
                            worker_goal_outcome_journal
                                .record_provider_call(provider_call_id.clone());
                        }
                        (provider_call_id, Some(permit))
                    }
                    Ok(WorkerProviderAdmission::Gated(decision)) => {
                        finish_worker_governor_gate(&db_path, &session_id, &event_tx, &decision);
                        return;
                    }
                    Ok(WorkerProviderAdmission::AlreadyStarted(call)) => {
                        finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "Hive Worker provider call {} may already have been accepted; it was not replayed",
                                    call.provider_call_id
                                ),
                            );
                        return;
                    }
                    Err(error) => {
                        finish_worker_provider_attention(
                            &db_path,
                            &session_id,
                            &event_tx,
                            format!("Hive Worker provider-call admission failed closed: {error:#}"),
                        );
                        return;
                    }
                }
            } else {
                (uuid::Uuid::new_v4().to_string(), None)
            };

            // Stream AI response only after durable Worker admission.
            persist_recovery_state(
                &db_path,
                &session_id,
                &build_recovery_state(
                    &context_ledger,
                    RecoveryStatus::Streaming,
                    None,
                    None,
                    PartialAssistantState::default(),
                ),
            );
            let provider_call_started = Instant::now();
            let request_diagnostics =
                ai_client.request_diagnostics(&conversation_with_context, &request_options);
            let _ = event_tx.send(LoopEvent::ProviderRequestPrepared {
                turn: iteration,
                diagnostics: Box::new(request_diagnostics.into()),
            });
            // Keep the request future in a nested scope so its immutable
            // borrow of `options` ends as soon as setup resolves. Later mode
            // transitions must be able to replace the governed schemas.
            let setup_result = {
                let attempt_policy = if provider_call_permit.is_some() {
                    RemoteAttemptPolicy::GovernedSingleAttempt
                } else {
                    RemoteAttemptPolicy::ConfiguredRetries
                };
                let streaming_setup = ai_client.call_streaming_with_attempt_policy(
                    conversation_with_context,
                    &request_options,
                    attempt_policy,
                );
                tokio::pin!(streaming_setup);
                let mut setup_input_closed = false;
                loop {
                    tokio::select! {
                        result = &mut streaming_setup => break Some(result),
                        cancelled = input_inbox.recv_cancel(), if !setup_input_closed => {
                            match cancelled {
                                Some(()) => break None,
                                None => setup_input_closed = true,
                            }
                        }
                    }
                }
            };

            let Some(setup_result) = setup_result else {
                let _ = provider_call_tx.send(super::observability::ProviderCallTrace::agent_loop(
                    provider_call_id.clone(),
                    iteration,
                    ai_client.provider_id(),
                    &ai_client.config().model,
                    options.reasoning_effort,
                    "cancelled_during_setup",
                    None,
                    provider_call_started.elapsed(),
                ));
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    LoopStopReason::UserAbort,
                );
                return;
            };

            let api_rx = match setup_result {
                Ok(rx) => rx,
                Err(e) => {
                    if let Some(transport_error) = e.downcast_ref::<reqwest::Error>() {
                        tracing::error!(
                            session_id = %session_id,
                            provider = %ai_client.provider_id(),
                            model = %ai_client.config().model,
                            is_builder = transport_error.is_builder(),
                            is_connect = transport_error.is_connect(),
                            is_request = transport_error.is_request(),
                            is_timeout = transport_error.is_timeout(),
                            is_body = transport_error.is_body(),
                            is_decode = transport_error.is_decode(),
                            status = transport_error.status().map(|status| status.as_u16()),
                            error_chain = %format!("{e:#}"),
                            "Provider streaming setup failed after retries"
                        );
                    } else {
                        tracing::error!(
                            session_id = %session_id,
                            provider = %ai_client.provider_id(),
                            model = %ai_client.config().model,
                            error_chain = %format!("{e:#}"),
                            "Provider streaming setup failed after retries"
                        );
                    }
                    let _ =
                        provider_call_tx.send(super::observability::ProviderCallTrace::agent_loop(
                            provider_call_id.clone(),
                            iteration,
                            ai_client.provider_id(),
                            &ai_client.config().model,
                            options.reasoning_effort,
                            "setup_error",
                            None,
                            provider_call_started.elapsed(),
                        ));
                    let error = format!("AI error: {e:#}");
                    if provider_call_permit.is_some() {
                        // The transport does not currently prove whether a
                        // setup error happened before or after remote
                        // acceptance. Keep Started unresolved and stop this
                        // run without entering the ordinary retry path.
                        persist_recovery_state(
                            &db_path,
                            &session_id,
                            &build_recovery_state(
                                &context_ledger,
                                RecoveryStatus::Interrupted,
                                Some(LoopStopReason::AwaitingInput),
                                Some(error.clone()),
                                PartialAssistantState::default(),
                            ),
                        );
                        finish_worker_provider_attention(
                            &db_path,
                            &session_id,
                            &event_tx,
                            format!(
                                "Hive Worker provider acceptance is uncertain; the call was not replayed: {error}"
                            ),
                        );
                        return;
                    }
                    if !overflow_compact_retry_attempted && is_context_overflow_error(&error) {
                        overflow_compact_retry_attempted = true;
                        tracing::warn!(
                            session_id = %session_id,
                            "Provider rejected request for context overflow; compacting and retrying once"
                        );
                        match compact_and_retry_once(
                            &db_path,
                            &session_id,
                            &mut conversation,
                            &mut context_ledger,
                            &working_dir,
                            project_dir_for_compaction,
                            user_id_for_compaction,
                            ai_client.as_ref(),
                            &compaction_manager,
                            CompactionTrigger::Overflow,
                            &mut last_usage_prompt_tokens,
                            &mut messages_at_last_usage,
                            &mut last_token_count,
                            Some(compaction_request_budget),
                            &provider_call_trace,
                            &event_tx,
                        )
                        .await
                        {
                            Ok(()) => continue,
                            Err(compaction_error) => {
                                tracing::error!(
                                    session_id = %session_id,
                                    error = %compaction_error,
                                    "Reactive overflow compaction failed"
                                );
                            }
                        }
                    }
                    persist_recovery_state(
                        &db_path,
                        &session_id,
                        &build_recovery_state(
                            &context_ledger,
                            RecoveryStatus::Interrupted,
                            Some(LoopStopReason::ProviderError),
                            Some(error.clone()),
                            PartialAssistantState::default(),
                        ),
                    );
                    let _ = event_tx.send(LoopEvent::Error { error });
                    if last_token_count > 0 {
                        update_token_count(&db_path, &session_id, last_token_count);
                    }
                    finish_run_preserving_recovery(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        "error",
                        LoopStopReason::ProviderError,
                    );
                    return;
                }
            };

            let result = {
                let stream_processing = stream::process_stream(
                    api_rx,
                    &event_tx,
                    effective_stream_idle_timeout,
                    |checkpoint| {
                        persist_recovery_state(
                            &db_path,
                            &session_id,
                            &build_recovery_state(
                                &context_ledger,
                                RecoveryStatus::Streaming,
                                None,
                                None,
                                build_partial_assistant_state(checkpoint),
                            ),
                        );
                    },
                );
                tokio::pin!(stream_processing);
                let mut input_closed = false;
                loop {
                    tokio::select! {
                        result = &mut stream_processing => break Some(result),
                        cancelled = input_inbox.recv_cancel(), if !input_closed => {
                            match cancelled {
                                Some(()) => break None,
                                None => input_closed = true,
                            }
                        }
                    }
                }
            };

            let Some(result) = result else {
                if active_goal {
                    if let Err(error) = record_active_attempt_progress(
                        &db_path,
                        &session_id,
                        &mut attempt_progress_tracker,
                        iteration,
                        goal_tool_call_count,
                        goal_research_action_count,
                        0,
                        0,
                        false,
                        None,
                    ) {
                        let _ = event_tx.send(LoopEvent::Error {
                            error: format!(
                                "Goal cancellation usage could not be persisted: {error}"
                            ),
                        });
                    }
                }
                if let Some(permit) = provider_call_permit.as_ref() {
                    if let Err(error) = permit.complete(WorkerProviderCompletion::acknowledged(
                        WorkerProviderTerminalOutcome::CancelledAfterAcceptance,
                        None,
                    )) {
                        tracing::error!(
                            session_id = %session_id,
                            %error,
                            "Failed to terminalize cancelled Hive Worker provider call"
                        );
                    }
                }
                let _ = provider_call_tx.send(super::observability::ProviderCallTrace::agent_loop(
                    provider_call_id.clone(),
                    iteration,
                    ai_client.provider_id(),
                    &ai_client.config().model,
                    options.reasoning_effort,
                    "cancelled_during_stream",
                    None,
                    provider_call_started.elapsed(),
                ));
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    LoopStopReason::UserAbort,
                );
                return;
            };

            let neutral_invalid_outcome =
                if context_mode.is_worker_conversation() && result.stop_reason.is_none() {
                    if !result.tool_calls.is_empty() {
                        Some(WorkerProviderTerminalOutcome::UnsafeOutput)
                    } else if result.text.trim().is_empty() {
                        Some(WorkerProviderTerminalOutcome::SemanticInvalid)
                    } else {
                        None
                    }
                } else {
                    None
                };
            let worker_goal_server_tool_rejected = context_mode.is_worker_goal()
                && result.stop_reason.is_none()
                && result.had_server_tool_activity;
            let worker_goal_commit_pending = context_mode.is_worker_goal()
                && !worker_goal_server_tool_rejected
                && result.stop_reason.is_none()
                && result.tool_calls.is_empty()
                && loop_guard_landing.is_none();
            let worker_goal_requested_research_actions = result
                .tool_calls
                .iter()
                .filter(|call| matches!(call.name.as_str(), "read" | "glob" | "grep" | "list"))
                .count();
            let worker_goal_budget_rejected =
                context_mode
                    .worker_goal_context()
                    .is_some_and(|goal_context| {
                        !goal_context.permits_additional_attempt_work(
                            worker_goal_outcome_journal.counters.tool_calls,
                            result.tool_calls.len(),
                            worker_goal_outcome_journal.counters.research_actions,
                            worker_goal_requested_research_actions,
                        )
                    });
            let worker_goal_landing = context_mode.is_worker_goal()
                && result.stop_reason.is_none()
                && result.tool_calls.is_empty()
                && loop_guard_landing.is_some();
            let provider_call_outcome = if worker_goal_commit_pending {
                "awaiting_worker_goal_outcome_commit"
            } else if worker_goal_server_tool_rejected {
                "worker_goal_forbidden_server_tool_activity"
            } else if worker_goal_budget_rejected {
                "worker_goal_attempt_budget_rejected"
            } else if worker_goal_landing {
                "worker_goal_landing_rejected"
            } else {
                match (
                    result.stop_reason.as_ref(),
                    neutral_invalid_outcome,
                    context_mode.is_worker_conversation(),
                ) {
                    (None, Some(WorkerProviderTerminalOutcome::UnsafeOutput), _) => {
                        "unsafe_tool_output"
                    }
                    (None, Some(WorkerProviderTerminalOutcome::SemanticInvalid), _) => {
                        "semantic_invalid"
                    }
                    (None, None, true) => "awaiting_canonical_commit",
                    (None, None, false) => "completed",
                    (Some(LoopStopReason::ProviderError), _, _) => "provider_error",
                    (Some(LoopStopReason::StreamIdleTimeout), _, _) => "stream_idle_timeout",
                    (Some(LoopStopReason::UserAbort), _, _) => "user_abort",
                    (Some(_), _, _) => "interrupted",
                    (None, Some(_), _) => "semantic_invalid",
                }
            };
            if let Some(permit) = provider_call_permit.as_ref() {
                let terminal_outcome = match (result.stop_reason.as_ref(), neutral_invalid_outcome)
                {
                    (None, Some(outcome)) => Some(outcome),
                    (None, None) if context_mode.is_worker_conversation() => None,
                    (None, None) if worker_goal_commit_pending => None,
                    (None, None) if worker_goal_server_tool_rejected => {
                        Some(WorkerProviderTerminalOutcome::UnsafeOutput)
                    }
                    (None, None) if worker_goal_budget_rejected => {
                        Some(WorkerProviderTerminalOutcome::SemanticInvalid)
                    }
                    (None, None) if worker_goal_landing => {
                        Some(WorkerProviderTerminalOutcome::SemanticInvalid)
                    }
                    (None, None) => Some(WorkerProviderTerminalOutcome::Completed),
                    (Some(LoopStopReason::StreamIdleTimeout), _) => {
                        Some(WorkerProviderTerminalOutcome::StreamIdleTimeout)
                    }
                    (Some(LoopStopReason::ProviderError), _) => {
                        Some(WorkerProviderTerminalOutcome::StreamError)
                    }
                    (Some(LoopStopReason::UserAbort), _) => {
                        Some(WorkerProviderTerminalOutcome::CancelledAfterAcceptance)
                    }
                    (Some(_), _) => Some(WorkerProviderTerminalOutcome::StreamError),
                };
                if let Some(terminal_outcome) = terminal_outcome {
                    if let Err(error) = permit.complete(WorkerProviderCompletion::acknowledged(
                        terminal_outcome,
                        result.usage_available.then_some(result.usage.clone()),
                    )) {
                        finish_worker_provider_attention(
                            &db_path,
                            &session_id,
                            &event_tx,
                            format!(
                                "Hive Worker provider call completed remotely but accounting failed closed: {error:#}"
                            ),
                        );
                        return;
                    }
                }
            }
            let neutral_commit_pending = context_mode.is_worker_conversation()
                && result.stop_reason.is_none()
                && neutral_invalid_outcome.is_none();
            if !neutral_commit_pending && !worker_goal_commit_pending {
                let _ = provider_call_tx.send(super::observability::ProviderCallTrace::agent_loop(
                    provider_call_id.clone(),
                    iteration,
                    ai_client.provider_id(),
                    &ai_client.config().model,
                    options.reasoning_effort,
                    provider_call_outcome,
                    result.usage_available.then_some(result.usage.clone()),
                    provider_call_started.elapsed(),
                ));
            }

            if result.total_tokens > 0 {
                last_token_count = result.total_tokens;
            }
            if result.prompt_tokens > 0 {
                last_usage_prompt_tokens = Some(result.prompt_tokens);
                messages_at_last_usage = conversation.len();
            }
            provider_tool_activity_seen |= result.had_server_tool_activity;
            let goal_token_stop = if active_goal {
                match record_active_goal_tokens(
                    &db_path,
                    &session_id,
                    attempt_progress_tracker.goal_id(),
                    result.total_tokens,
                ) {
                    Ok(stop) => stop,
                    Err(error) => {
                        let _ = event_tx.send(LoopEvent::Error {
                            error: format!("Goal token accounting could not be persisted: {error}"),
                        });
                        finish_run_preserving_recovery(
                            attempt_progress_tracker.goal_id(),
                            &db_path,
                            &session_id,
                            &event_tx,
                            "error",
                            LoopStopReason::ProviderError,
                        );
                        return;
                    }
                }
            } else {
                None
            };

            if !context_mode.is_worker_conversation()
                && loop_guard_landing.is_none()
                && goal_token_stop.is_none()
                && should_retry_empty_stream_interruption(
                    result.stop_reason.as_ref(),
                    result.last_error.as_ref(),
                    result.produced_output,
                    empty_stream_retry_attempted,
                )
            {
                empty_stream_retry_attempted = true;
                tracing::warn!(
                    session_id = %session_id,
                    "Provider stream ended before text or tool calls; retrying once"
                );
                clear_recovery_state(&db_path, &session_id);
                set_agent_state(&db_path, &session_id, "streaming");
                continue;
            }

            if active_goal && (result.tool_calls.is_empty() || result.stop_reason.is_some()) {
                if let Err(error) = record_active_attempt_progress(
                    &db_path,
                    &session_id,
                    &mut attempt_progress_tracker,
                    iteration,
                    goal_tool_call_count,
                    goal_research_action_count,
                    0,
                    0,
                    false,
                    None,
                ) {
                    let _ = event_tx.send(LoopEvent::Error {
                        error: format!("Goal response accounting could not be persisted: {error}"),
                    });
                    finish_run_preserving_recovery(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        "error",
                        LoopStopReason::ProviderError,
                    );
                    return;
                }
            }

            let effective_stop_reason = if loop_guard_landing.is_none() {
                result.stop_reason.clone()
            } else {
                None
            };
            if let Some(stop_reason) = effective_stop_reason {
                if !context_mode.is_isolated_worker()
                    && !overflow_compact_retry_attempted
                    && stop_reason == LoopStopReason::ProviderError
                    && result.last_error.as_ref().is_some_and(|failure| {
                        matches!(failure, stream::StreamFailure::ContextOverflow(_))
                    })
                {
                    overflow_compact_retry_attempted = true;
                    tracing::warn!(
                        session_id = %session_id,
                        "Provider stream reported context overflow; compacting and retrying once"
                    );
                    match compact_and_retry_once(
                        &db_path,
                        &session_id,
                        &mut conversation,
                        &mut context_ledger,
                        &working_dir,
                        project_dir_for_compaction,
                        user_id_for_compaction,
                        ai_client.as_ref(),
                        &compaction_manager,
                        CompactionTrigger::Overflow,
                        &mut last_usage_prompt_tokens,
                        &mut messages_at_last_usage,
                        &mut last_token_count,
                        Some(compaction_request_budget),
                        &provider_call_trace,
                        &event_tx,
                    )
                    .await
                    {
                        Ok(()) => continue,
                        Err(compaction_error) => {
                            tracing::error!(
                                session_id = %session_id,
                                error = %compaction_error,
                                "Reactive overflow compaction failed"
                            );
                        }
                    }
                }
                persist_recovery_state(
                    &db_path,
                    &session_id,
                    &build_recovery_state(
                        &context_ledger,
                        RecoveryStatus::Interrupted,
                        Some(stop_reason.clone()),
                        result
                            .last_error
                            .as_ref()
                            .map(|failure| failure.message().to_string()),
                        build_partial_assistant_state(&result.recovery_checkpoint),
                    ),
                );
                persist_context_state(&db_path, &session_id, &context_ledger);
                let terminal_error = result
                    .last_error
                    .as_ref()
                    .map(|failure| failure.message().to_string())
                    .unwrap_or_else(|| continuation_recovery_message(&context_ledger));
                let _ = event_tx.send(LoopEvent::Error {
                    error: terminal_error,
                });
                if last_token_count > 0 {
                    update_token_count(&db_path, &session_id, last_token_count);
                }
                finish_run_preserving_recovery(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    terminal_agent_state_after_interruption(&stop_reason),
                    stop_reason,
                );
                return;
            }

            if context_mode.is_worker_conversation() {
                if let Some(invalid_outcome) = neutral_invalid_outcome {
                    let reason = match invalid_outcome {
                        WorkerProviderTerminalOutcome::UnsafeOutput => {
                            "Hive Worker conversation returned an unadvertised tool call; no tool ran and no response was committed"
                        }
                        WorkerProviderTerminalOutcome::SemanticInvalid => {
                            "Hive Worker conversation returned no user-visible text; no response was committed"
                        }
                        _ => "Hive Worker conversation response was rejected before commit",
                    };
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        reason.to_string(),
                    );
                    return;
                }

                let Some(committer) = context_mode.response_committer() else {
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        "Hive Worker canonical response committer is unavailable".to_string(),
                    );
                    return;
                };
                let Some(governor) = provider_governor.as_ref() else {
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        "Hive Worker provider governor is unavailable during response commit"
                            .to_string(),
                    );
                    return;
                };
                let Some(permit) = provider_call_permit.as_ref() else {
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        "Hive Worker provider permit is unavailable during response commit"
                            .to_string(),
                    );
                    return;
                };
                let binding = governor.binding();
                let commit_input = WorkerConversationResponseCommitInput {
                    worker_id: binding.worker_id.clone(),
                    worker_revision: binding.worker_revision,
                    owner_user_id: binding.owner_user_id.clone(),
                    session_id: binding.session_id.clone(),
                    lane: binding.conversation_lane.clone(),
                    run_id: binding.run_id.clone(),
                    run_lease_token: binding.run_lease_token.clone(),
                    run_lease_epoch: binding.run_lease_epoch,
                    provider_call_id: permit.provider_call_id().to_string(),
                    response_text: result.text.clone(),
                };
                if let Err(error) = committer.commit_response(&commit_input) {
                    if let Some(outcome) = worker_response_commit_terminal_outcome(&error) {
                        if let Err(accounting_error) =
                            permit.complete(WorkerProviderCompletion::acknowledged(
                                outcome,
                                result.usage_available.then_some(result.usage.clone()),
                            ))
                        {
                            let _ = provider_call_tx.send(
                                super::observability::ProviderCallTrace::agent_loop(
                                    provider_call_id.clone(),
                                    iteration,
                                    ai_client.provider_id(),
                                    &ai_client.config().model,
                                    options.reasoning_effort,
                                    "canonical_commit_stale_accounting_uncertain",
                                    result.usage_available.then_some(result.usage.clone()),
                                    provider_call_started.elapsed(),
                                ),
                            );
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "Hive Worker response was rejected by a stale execution fence, but provider accounting requires fenced recovery: {accounting_error:#}"
                                ),
                            );
                            return;
                        }
                        let _ = provider_call_tx.send(
                            super::observability::ProviderCallTrace::agent_loop(
                                provider_call_id.clone(),
                                iteration,
                                ai_client.provider_id(),
                                &ai_client.config().model,
                                options.reasoning_effort,
                                "canonical_commit_stale",
                                result.usage_available.then_some(result.usage.clone()),
                                provider_call_started.elapsed(),
                            ),
                        );
                        finish_worker_provider_attention(
                            &db_path,
                            &session_id,
                            &event_tx,
                            format!(
                                "Hive Worker response was rejected by a stale execution fence and was not published: {error}"
                            ),
                        );
                        return;
                    }

                    let trace_outcome = match &error {
                        WorkerConversationResponseCommitError::ConflictOrCorrupt(_) => {
                            "canonical_commit_conflict_or_corrupt"
                        }
                        WorkerConversationResponseCommitError::CommitUncertain(_) => {
                            "canonical_commit_uncertain"
                        }
                        WorkerConversationResponseCommitError::StaleRejected(_) => {
                            unreachable!("stale response commit rejection was handled as terminal")
                        }
                    };
                    let _ =
                        provider_call_tx.send(super::observability::ProviderCallTrace::agent_loop(
                            provider_call_id.clone(),
                            iteration,
                            ai_client.provider_id(),
                            &ai_client.config().model,
                            options.reasoning_effort,
                            trace_outcome,
                            result.usage_available.then_some(result.usage.clone()),
                            provider_call_started.elapsed(),
                        ));
                    // A conflicting durable row may be an adoption candidate,
                    // and commit failure can be transaction-uncertain. Keep
                    // Started so takeover can inspect the exact canonical row.
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        format!("Hive Worker response commit requires fenced recovery: {error}"),
                    );
                    return;
                }
                if let Err(error) = permit.complete(WorkerProviderCompletion::acknowledged(
                    WorkerProviderTerminalOutcome::Completed,
                    result.usage_available.then_some(result.usage.clone()),
                )) {
                    let _ =
                        provider_call_tx.send(super::observability::ProviderCallTrace::agent_loop(
                            provider_call_id.clone(),
                            iteration,
                            ai_client.provider_id(),
                            &ai_client.config().model,
                            options.reasoning_effort,
                            "canonical_committed_accounting_uncertain",
                            result.usage_available.then_some(result.usage.clone()),
                            provider_call_started.elapsed(),
                        ));
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        format!(
                            "Hive Worker response committed, but provider accounting requires fenced adoption: {error:#}"
                        ),
                    );
                    return;
                }
                let _ = provider_call_tx.send(super::observability::ProviderCallTrace::agent_loop(
                    provider_call_id,
                    iteration,
                    ai_client.provider_id(),
                    &ai_client.config().model,
                    options.reasoning_effort,
                    "completed",
                    result.usage_available.then_some(result.usage.clone()),
                    provider_call_started.elapsed(),
                ));
                if last_token_count > 0 {
                    update_token_count(&db_path, &session_id, last_token_count);
                }
                clear_recovery_state(&db_path, &session_id);
                set_agent_state(&db_path, &session_id, "idle");
                let _ = event_tx.send(LoopEvent::TurnComplete {
                    turn: iteration,
                    has_more: false,
                });
                let _ = event_tx.send(LoopEvent::Finished {
                    session_id: session_id.clone(),
                    stop_reason: LoopStopReason::Completed,
                });
                return;
            }

            if worker_goal_server_tool_rejected {
                finish_worker_provider_attention(
                    &db_path,
                    &session_id,
                    &event_tx,
                    "Hive Worker Goal provider reported hosted tool activity on a surface with no hosted tools; no result or Workflow progress was accepted"
                        .to_string(),
                );
                return;
            }

            if worker_goal_budget_rejected {
                finish_worker_provider_attention(
                    &db_path,
                    &session_id,
                    &event_tx,
                    "Hive Worker Goal requested a tool batch beyond the frozen attempt budget; no tool ran and no Workflow progress was published"
                        .to_string(),
                );
                return;
            }

            // A provider can successfully finish after emitting only internal
            // reasoning. That is a valid transport response but not a usable
            // assistant turn. Recover semantically once without replaying any
            // completed tool call or polluting canonical conversation history.
            empty_completion_recovery_pending = false;
            let empty_completion =
                if loop_guard_landing.is_some() || session_type == SessionType::Hive {
                    EmptyCompletionAction::None
                } else {
                    empty_completion_action(
                        &result.text,
                        &result.tool_calls,
                        !result.citations.is_empty(),
                        empty_completion_retry_attempted,
                        provider_tool_activity_seen,
                    )
                };
            match empty_completion {
                EmptyCompletionAction::Retry => {
                    empty_completion_retry_attempted = true;
                    empty_completion_recovery_pending = true;
                    tracing::warn!(
                        session_id = %session_id,
                        "Provider completed without user-visible text or a tool call; requesting one semantic continuation"
                    );
                    clear_recovery_state(&db_path, &session_id);
                    set_agent_state(&db_path, &session_id, "streaming");
                    continue;
                }
                EmptyCompletionAction::Fail => {
                    let error = if provider_tool_activity_seen {
                        EMPTY_COMPLETION_AFTER_SERVER_TOOL_ERROR
                    } else {
                        EMPTY_COMPLETION_ERROR
                    };
                    tracing::error!(
                        session_id = %session_id,
                        had_server_tool_activity = provider_tool_activity_seen,
                        "Provider produced an unrecoverable empty semantic completion"
                    );
                    persist_recovery_state(
                        &db_path,
                        &session_id,
                        &build_recovery_state(
                            &context_ledger,
                            RecoveryStatus::Interrupted,
                            Some(LoopStopReason::ProviderError),
                            Some(error.to_string()),
                            build_partial_assistant_state(&result.recovery_checkpoint),
                        ),
                    );
                    persist_context_state(&db_path, &session_id, &context_ledger);
                    let _ = event_tx.send(LoopEvent::Error {
                        error: error.to_string(),
                    });
                    if last_token_count > 0 {
                        update_token_count(&db_path, &session_id, last_token_count);
                    }
                    finish_run_preserving_recovery(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        "error",
                        LoopStopReason::ProviderError,
                    );
                    return;
                }
                EmptyCompletionAction::None => {}
            }

            // The provider has no tool schemas during a loop-guard landing.
            // If a transport nevertheless reports a tool call, discard it
            // rather than persisting an unfulfilled tool-use block.
            let assistant_tool_calls = if loop_guard_landing.is_some() {
                &[][..]
            } else {
                result.tool_calls.as_slice()
            };
            let assistant_msg = build_assistant_message(
                &result.text,
                &result.thinking_blocks,
                assistant_tool_calls,
                &result.citations,
            );
            if !assistant_msg.content.is_empty() {
                conversation.push(assistant_msg.clone());
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
                save_message(&db_path, &session_id, &assistant_msg);
            }

            input_inbox.collect_ready();
            let workflow_updated = if context_mode.is_isolated_worker() {
                let _ = input_inbox.take_workflow_updates();
                false
            } else {
                emit_workflow_update_inputs(&event_tx, input_inbox.take_workflow_updates())
            };
            let cancelled = input_inbox.take_cancel();
            let workflow_control_stop = if goal_token_stop.is_some()
                && !result.tool_calls.is_empty()
            {
                let _ = event_tx.send(LoopEvent::Error {
                    error: "The Goal token budget was exhausted; pending tools were not executed."
                        .into(),
                });
                Some(LoopStopReason::BudgetExhausted)
            } else if manages_workflow_lifecycle && (workflow_updated || active_goal) {
                match WorkflowManager::new(db_path.clone())
                    .and_then(|manager| manager.get_snapshot(&session_id))
                {
                    Ok(snapshot)
                        if active_goal
                            && !snapshot.as_ref().is_some_and(|snapshot| {
                                snapshot.goal.status == GoalStatus::Active
                                    && attempt_progress_tracker.goal_id()
                                        == Some(snapshot.goal.id.as_str())
                            }) =>
                    {
                        Some(LoopStopReason::AwaitingInput)
                    }
                    Ok(_) => None,
                    Err(error) => {
                        let _ = event_tx.send(LoopEvent::Error {
                            error: format!("Unable to refresh changed Goal execution: {error}"),
                        });
                        Some(LoopStopReason::ProviderError)
                    }
                }
            } else {
                None
            };
            if cancelled
                || workflow_control_stop.is_some()
                || (workflow_updated && !result.tool_calls.is_empty())
            {
                if active_goal {
                    if let Err(error) = record_active_attempt_progress(
                        &db_path,
                        &session_id,
                        &mut attempt_progress_tracker,
                        iteration,
                        goal_tool_call_count,
                        goal_research_action_count,
                        0,
                        0,
                        false,
                        None,
                    ) {
                        let _ = event_tx.send(LoopEvent::Error {
                            error: format!(
                                "Interrupted Goal response usage could not be persisted: {error}"
                            ),
                        });
                    }
                }
                let mut results = Vec::new();
                close_unexecuted_tool_calls(
                    &result.tool_calls,
                    &mut results,
                    &event_tx,
                    if cancelled {
                        "Cancelled before tool execution"
                    } else {
                        "Workflow changed before tool execution; reload the current execution contract"
                    },
                );
                if !results.is_empty() {
                    let tool_msg = ModelMessage {
                        role: Role::User,
                        content: results,
                    };
                    conversation.push(tool_msg.clone());
                    context_ledger.update_from_conversation(&conversation);
                    persist_context_state(&db_path, &session_id, &context_ledger);
                    save_message(&db_path, &session_id, &tool_msg);
                }
                if cancelled || workflow_control_stop.is_some() {
                    finish_run(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        "idle",
                        if cancelled {
                            LoopStopReason::UserAbort
                        } else {
                            workflow_control_stop.expect("workflow control stop checked")
                        },
                    );
                    return;
                }
                let _ = event_tx.send(LoopEvent::TurnComplete {
                    turn: iteration,
                    has_more: true,
                });
                continue;
            }

            // Title generation on first response
            if !title_generated && !result.text.is_empty() {
                title_generated = true;
                maybe_generate_title(&conversation, &event_tx, &session_id, &db_path);
            }

            if loop_guard_landing.is_some() {
                let injected_steering = if context_mode.is_isolated_worker() {
                    let _ = input_inbox.take_steering();
                    Vec::new()
                } else {
                    inject_pending_steering(
                        &mut input_inbox,
                        &mut conversation,
                        &mut context_ledger,
                        &db_path,
                        &session_id,
                    )
                };
                if !injected_steering.is_empty() || workflow_updated {
                    reset_recovery_flags(
                        &mut loop_guard,
                        active_goal,
                        &mut loop_guard_landing,
                        &mut delegation_nudge_tracker,
                        &mut empty_stream_retry_attempted,
                        &mut empty_completion_retry_attempted,
                        &mut empty_completion_recovery_pending,
                        &mut provider_tool_activity_seen,
                        &mut overflow_compact_retry_attempted,
                        &mut goal_final_continuation_attempted,
                    );
                    emit_steering_turn_boundary(
                        &db_path,
                        &session_id,
                        &event_tx,
                        iteration,
                        injected_steering,
                    );
                    continue;
                }

                let landing = loop_guard_landing
                    .take()
                    .expect("landing state checked immediately before completion");
                if context_mode.is_worker_goal() {
                    finish_worker_provider_attention(
                        &db_path,
                        &session_id,
                        &event_tx,
                        format!(
                            "Hive Worker Goal stopped at the bounded loop guard without a successful outcome commit; no Workflow progress was published: {}",
                            landing.diagnostic
                        ),
                    );
                    return;
                }
                if result.text.trim().is_empty() {
                    let fallback = LOOP_GUARD_LANDING_FALLBACK.to_string();
                    let _ = event_tx.send(LoopEvent::TextDelta {
                        delta: fallback.clone(),
                    });
                    let fallback_msg = ModelMessage {
                        role: Role::Assistant,
                        content: vec![Content::text(fallback)],
                    };
                    conversation.push(fallback_msg.clone());
                    context_ledger.update_from_conversation(&conversation);
                    persist_context_state(&db_path, &session_id, &context_ledger);
                    save_message(&db_path, &session_id, &fallback_msg);
                    if !title_generated {
                        maybe_generate_title(&conversation, &event_tx, &session_id, &db_path);
                    }
                }
                if last_token_count > 0 {
                    update_token_count(&db_path, &session_id, last_token_count);
                }
                if active_goal {
                    let transition = if landing.block_goal {
                        block_active_goal_for_stop(&db_path, &session_id, "semantic_no_progress")
                    } else {
                        pause_active_goal_for_stop(
                            &db_path,
                            &session_id,
                            "validation_converged_before_goal_verification",
                        )
                    };
                    if !publish_workflow_transition(&event_tx, transition) {
                        finish_run(
                            attempt_progress_tracker.goal_id(),
                            &db_path,
                            &session_id,
                            &event_tx,
                            "error",
                            LoopStopReason::ProviderError,
                        );
                        return;
                    }
                }
                let _ = event_tx.send(LoopEvent::TurnComplete {
                    turn: iteration,
                    has_more: false,
                });
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    if landing.block_goal {
                        LoopStopReason::LoopGuardTriggered
                    } else if active_goal {
                        LoopStopReason::AwaitingInput
                    } else {
                        LoopStopReason::Completed
                    },
                );
                return;
            }

            // A tool-free completion is also a safe boundary for live steering.
            if result.tool_calls.is_empty() {
                let injected_steering = if context_mode.is_isolated_worker() {
                    let _ = input_inbox.take_steering();
                    Vec::new()
                } else {
                    inject_pending_steering(
                        &mut input_inbox,
                        &mut conversation,
                        &mut context_ledger,
                        &db_path,
                        &session_id,
                    )
                };
                if no_tool_completion_should_continue(&injected_steering) || workflow_updated {
                    reset_recovery_flags(
                        &mut loop_guard,
                        active_goal,
                        &mut loop_guard_landing,
                        &mut delegation_nudge_tracker,
                        &mut empty_stream_retry_attempted,
                        &mut empty_completion_retry_attempted,
                        &mut empty_completion_recovery_pending,
                        &mut provider_tool_activity_seen,
                        &mut overflow_compact_retry_attempted,
                        &mut goal_final_continuation_attempted,
                    );
                    emit_steering_turn_boundary(
                        &db_path,
                        &session_id,
                        &event_tx,
                        iteration,
                        injected_steering,
                    );
                    continue;
                }
            }

            // No tool calls means this turn is complete. Assistant prose cannot
            // create, approve, or activate workflow state.
            if result.tool_calls.is_empty() {
                if context_mode.is_worker_goal() {
                    let Some(permit) = provider_call_permit.as_ref() else {
                        finish_worker_provider_attention(
                            &db_path,
                            &session_id,
                            &event_tx,
                            "Hive Worker Goal final provider permit is unavailable".to_string(),
                        );
                        return;
                    };
                    let successful_terminal = !result.text.trim().is_empty()
                        && worker_goal_outcome_journal.can_commit_outcome();
                    if !successful_terminal {
                        let reason = if result.text.trim().is_empty() {
                            "Hive Worker Goal ended without a usable final outcome; no Workflow progress was committed"
                        } else {
                            "Hive Worker Goal lacks concrete successful governed-tool evidence, observed a tool failure, or exceeded its bounded outcome journal; no Workflow progress was committed"
                        };
                        if let Err(error) = permit.complete(WorkerProviderCompletion::acknowledged(
                            WorkerProviderTerminalOutcome::SemanticInvalid,
                            result.usage_available.then_some(result.usage.clone()),
                        )) {
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "{reason}; final provider accounting requires fenced recovery: {error:#}"
                                ),
                            );
                            return;
                        }
                        let _ = provider_call_tx.send(
                            super::observability::ProviderCallTrace::agent_loop(
                                provider_call_id.clone(),
                                iteration,
                                ai_client.provider_id(),
                                &ai_client.config().model,
                                options.reasoning_effort,
                                "worker_goal_outcome_rejected",
                                result.usage_available.then_some(result.usage.clone()),
                                provider_call_started.elapsed(),
                            ),
                        );
                        finish_worker_provider_attention(
                            &db_path,
                            &session_id,
                            &event_tx,
                            reason.to_string(),
                        );
                        return;
                    }

                    let Some(goal_context) = context_mode.worker_goal_context() else {
                        unreachable!("Worker Goal mode checked immediately before context access")
                    };
                    let Some(committer) = context_mode.worker_goal_outcome_committer() else {
                        unreachable!("Worker Goal mode structurally requires an outcome committer")
                    };
                    let outcome_input =
                        worker_goal_outcome_journal
                            .effect_summary()
                            .and_then(|effect| {
                                goal_context.outcome_commit_input(
                                    worker_goal_outcome_journal.provider_call_ids.clone(),
                                    worker_goal_outcome_journal.attempt_outcome(),
                                    worker_goal_outcome_journal.evidence.clone(),
                                    effect,
                                    worker_goal_outcome_journal.counters,
                                )
                            });
                    let outcome_input = match outcome_input {
                        Ok(input)
                            if input.final_provider_call_id() == provider_call_id.as_str() =>
                        {
                            input
                        }
                        Ok(_) => {
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                "Hive Worker Goal final provider identity drifted before outcome commit; the provider call remains unresolved for recovery"
                                    .to_string(),
                            );
                            return;
                        }
                        Err(error) => {
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "Hive Worker Goal outcome input failed closed; the final provider call remains unresolved for recovery: {error}"
                                ),
                            );
                            return;
                        }
                    };
                    let finalize = commit_worker_goal_outcome_before_provider_completion(
                        committer.as_ref(),
                        &outcome_input,
                        |terminal_outcome| {
                            permit.complete(WorkerProviderCompletion::acknowledged(
                                terminal_outcome,
                                result.usage_available.then_some(result.usage.clone()),
                            ))?;
                            Ok(())
                        },
                    );
                    match finalize {
                        WorkerGoalOutcomeFinalize::Committed => {
                            let _ = provider_call_tx.send(
                                super::observability::ProviderCallTrace::agent_loop(
                                    provider_call_id.clone(),
                                    iteration,
                                    ai_client.provider_id(),
                                    &ai_client.config().model,
                                    options.reasoning_effort,
                                    "worker_goal_outcome_committed",
                                    result.usage_available.then_some(result.usage.clone()),
                                    provider_call_started.elapsed(),
                                ),
                            );
                        }
                        WorkerGoalOutcomeFinalize::ProvenStale => {
                            let _ = provider_call_tx.send(
                                super::observability::ProviderCallTrace::agent_loop(
                                    provider_call_id.clone(),
                                    iteration,
                                    ai_client.provider_id(),
                                    &ai_client.config().model,
                                    options.reasoning_effort,
                                    "worker_goal_outcome_stale",
                                    result.usage_available.then_some(result.usage.clone()),
                                    provider_call_started.elapsed(),
                                ),
                            );
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                "Hive Worker Goal outcome was rejected by a proven stale run fence; no stale progress was published"
                                    .to_string(),
                            );
                            return;
                        }
                        WorkerGoalOutcomeFinalize::Ambiguous(error) => {
                            let trace_outcome = match &error {
                                WorkerGoalOutcomeCommitError::ConflictOrCorrupt(_) => {
                                    "worker_goal_outcome_conflict_or_corrupt"
                                }
                                WorkerGoalOutcomeCommitError::CommitUncertain(_) => {
                                    "worker_goal_outcome_commit_uncertain"
                                }
                                WorkerGoalOutcomeCommitError::StaleRejected(_) => {
                                    unreachable!("proven stale handled by the finalize helper")
                                }
                            };
                            let _ = provider_call_tx.send(
                                super::observability::ProviderCallTrace::agent_loop(
                                    provider_call_id.clone(),
                                    iteration,
                                    ai_client.provider_id(),
                                    &ai_client.config().model,
                                    options.reasoning_effort,
                                    trace_outcome,
                                    result.usage_available.then_some(result.usage.clone()),
                                    provider_call_started.elapsed(),
                                ),
                            );
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "Hive Worker Goal outcome requires fenced recovery; the final provider call was deliberately left unresolved: {error}"
                                ),
                            );
                            return;
                        }
                        WorkerGoalOutcomeFinalize::ProviderAccountingUncertain {
                            outcome_committed,
                            error,
                        } => {
                            let trace_outcome = if outcome_committed {
                                "worker_goal_outcome_committed_accounting_uncertain"
                            } else {
                                "worker_goal_outcome_stale_accounting_uncertain"
                            };
                            let _ = provider_call_tx.send(
                                super::observability::ProviderCallTrace::agent_loop(
                                    provider_call_id.clone(),
                                    iteration,
                                    ai_client.provider_id(),
                                    &ai_client.config().model,
                                    options.reasoning_effort,
                                    trace_outcome,
                                    result.usage_available.then_some(result.usage.clone()),
                                    provider_call_started.elapsed(),
                                ),
                            );
                            finish_worker_provider_attention(
                                &db_path,
                                &session_id,
                                &event_tx,
                                format!(
                                    "Hive Worker Goal outcome boundary resolved, but final provider accounting requires fenced recovery: {error:#}"
                                ),
                            );
                            return;
                        }
                    }
                }
                if manages_workflow_lifecycle && goal_token_stop.is_none() {
                    let disposition = (|| -> anyhow::Result<_> {
                        let snapshot =
                            WorkflowManager::new(db_path.clone())?.get_snapshot(&session_id)?;
                        let awaiting_delegation =
                            awaiting_background_delegation(&db_path, &session_id)?;
                        Ok(goal_final_disposition(
                            snapshot.as_ref(),
                            awaiting_delegation,
                            goal_final_continuation_attempted,
                        ))
                    })();
                    match disposition {
                        Ok(GoalFinalDisposition::Continue) => {
                            goal_final_continuation_attempted = true;
                            conversation.push(ModelMessage {
                                role: Role::System,
                                content: vec![Content::text(GOAL_FINAL_CONTINUATION.into())],
                            });
                            context_ledger.update_from_conversation(&conversation);
                            persist_context_state(&db_path, &session_id, &context_ledger);
                            let _ = event_tx.send(LoopEvent::TurnComplete {
                                turn: iteration,
                                has_more: true,
                            });
                            continue;
                        }
                        Ok(GoalFinalDisposition::Pause) => {
                            if !publish_workflow_transition(
                                &event_tx,
                                pause_active_goal_for_stop(
                                    &db_path,
                                    &session_id,
                                    "model_completion_without_goal_verification",
                                ),
                            ) {
                                finish_run(
                                    attempt_progress_tracker.goal_id(),
                                    &db_path,
                                    &session_id,
                                    &event_tx,
                                    "error",
                                    LoopStopReason::ProviderError,
                                );
                                return;
                            }
                            let _ = event_tx.send(LoopEvent::Error { error: "The Agent ended twice while the Goal was still active. The Goal is paused with its work preserved; completion was not verified.".into() });
                            finish_run(
                                attempt_progress_tracker.goal_id(),
                                &db_path,
                                &session_id,
                                &event_tx,
                                "awaiting_input",
                                LoopStopReason::AwaitingInput,
                            );
                            return;
                        }
                        Err(error) => {
                            let _ = event_tx.send(LoopEvent::Error {
                                error: format!("Unable to settle Goal response: {error}"),
                            });
                            finish_run(
                                attempt_progress_tracker.goal_id(),
                                &db_path,
                                &session_id,
                                &event_tx,
                                "error",
                                LoopStopReason::ProviderError,
                            );
                            return;
                        }
                        Ok(
                            GoalFinalDisposition::Complete | GoalFinalDisposition::AwaitDelegation,
                        ) => {}
                    }
                }
                let _ = event_tx.send(LoopEvent::TurnComplete {
                    turn: iteration,
                    has_more: false,
                });
                if last_token_count > 0 {
                    update_token_count(&db_path, &session_id, last_token_count);
                }
                if let Some((_, reason)) = &goal_token_stop {
                    let _ = event_tx.send(LoopEvent::Error {
                        error: format!(
                            "Goal attempt stopped: {}",
                            reason
                                .clone()
                                .unwrap_or_else(|| "token_budget_exhausted".to_string())
                        ),
                    });
                }
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    if goal_token_stop.is_some() {
                        LoopStopReason::BudgetExhausted
                    } else {
                        LoopStopReason::Completed
                    },
                );
                return;
            }

            // AskUser partition
            let (ask_user_calls, non_ask_user_calls): (Vec<_>, Vec<_>) =
                result.tool_calls.iter().partition::<Vec<_>, _>(|t| {
                    t.name == "AskUserQuestion"
                        && advertised_tool_names.contains(&t.name)
                        && execution_tool_allowlist
                            .as_ref()
                            .is_none_or(|allowlist| allowlist.contains(&t.name))
                });

            if !ask_user_calls.is_empty() {
                let ask_user_partial_assistant =
                    build_partial_assistant_state(&result.recovery_checkpoint);
                // One tool call may contain multiple questions, but one model
                // turn must never create multiple independently resumable
                // AskUser calls. A continuation resumes the model after one
                // response, so persisting several calls would silently discard
                // the unanswered remainder. Keep the first as the sole durable
                // interaction and return explicit tool errors for the extras.
                let (pending_ask_user_call, rejected_ask_user_calls) =
                    split_single_pending_ask_user_call(&ask_user_calls)
                        .expect("non-empty AskUser partition must have a primary call");
                let ask_user_pending_interactions =
                    vec![PendingInteractionSnapshot::ask_user_from_call(
                        &pending_ask_user_call.id,
                        &pending_ask_user_call.arguments,
                    )];
                let mut all_results: Vec<Content> = Vec::new();
                let mut cancelled = false;

                // Execute non-AskUser tools first
                if !non_ask_user_calls.is_empty() {
                    let other_calls: Vec<_> = non_ask_user_calls.into_iter().cloned().collect();
                    set_agent_state(&db_path, &session_id, "tool_executing");
                    let other_batch = executor::execute_tools(
                        &other_calls,
                        &tool_registry,
                        &ai_client,
                        &working_dir,
                        project_dir.as_deref(),
                        &process_registry,
                        &skills_manager,
                        &session_id,
                        &db_path,
                        user_id.as_deref(),
                        permission_mode,
                        work_mode,
                        (!context_mode.is_worker_goal()).then_some(&ask_user_partial_assistant),
                        delegated_progress_tx.as_ref(),
                        &event_tx,
                        Some(&provider_call_trace),
                        &mut input_inbox,
                        project_settings.subagent_max_turns,
                        options.reasoning_effort,
                        &advertised_tool_names,
                        execution_tool_allowlist.as_ref(),
                        project_settings.disabled_tools.as_deref(),
                        hive_group_run.as_ref(),
                        Arc::clone(&file_observations),
                        if context_mode.is_worker_goal() {
                            executor::ExtensionExecutionPolicy::DisabledWorkerGoal
                        } else if context_mode.is_isolated_worker() {
                            executor::ExtensionExecutionPolicy::Disabled
                        } else {
                            executor::ExtensionExecutionPolicy::Enabled
                        },
                    )
                    .await;
                    all_results.extend(other_batch.results);
                    cancelled = other_batch.cancelled;
                }
                input_inbox.collect_ready();
                cancelled |= input_inbox.take_cancel();
                if cancelled {
                    let delegated_store = Database::new(&db_path).ok().map(DelegatedRunStore::new);
                    let research_actions = result
                        .tool_calls
                        .iter()
                        .filter(|call| {
                            is_research_action(
                                call,
                                &all_results,
                                delegated_store.as_ref(),
                                &session_id,
                            )
                        })
                        .count();
                    finish_cancelled_tool_batch(
                        &db_path,
                        &session_id,
                        &event_tx,
                        active_goal,
                        &mut attempt_progress_tracker,
                        &result.tool_calls,
                        all_results,
                        (
                            iteration,
                            goal_tool_call_count.saturating_add(result.tool_calls.len()),
                            goal_research_action_count.saturating_add(research_actions),
                        ),
                        research_actions,
                    );
                    return;
                }

                // Add one resumable placeholder and terminal errors for any
                // extra calls so every provider tool call still has a result.
                all_results.push(Content::ToolResult {
                    tool_use_id: pending_ask_user_call.id.clone(),
                    output: serde_json::Value::String("Awaiting user response...".to_string()),
                    is_error: None,
                });
                for call in rejected_ask_user_calls {
                    all_results.push(Content::ToolResult {
                        tool_use_id: call.id.clone(),
                        output: serde_json::Value::String(
                            "Only one AskUserQuestion tool call is allowed per model turn. Combine all questions into the first call's questions array."
                                .to_string(),
                        ),
                        is_error: Some(true),
                    });
                }

                let tool_msg = ModelMessage {
                    role: Role::User,
                    content: all_results,
                };
                conversation.push(tool_msg.clone());
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
                save_message(&db_path, &session_id, &tool_msg);

                let injected_steering = if context_mode.is_isolated_worker() {
                    let _ = input_inbox.take_steering();
                    Vec::new()
                } else {
                    inject_pending_steering(
                        &mut input_inbox,
                        &mut conversation,
                        &mut context_ledger,
                        &db_path,
                        &session_id,
                    )
                };
                emit_steering_events(&event_tx, injected_steering);

                if last_token_count > 0 {
                    update_token_count(&db_path, &session_id, last_token_count);
                }
                if active_goal {
                    let delegated_store = Database::new(&db_path).ok().map(DelegatedRunStore::new);
                    let research_actions = result
                        .tool_calls
                        .iter()
                        .filter(|call| {
                            is_research_action(
                                call,
                                &tool_msg.content,
                                delegated_store.as_ref(),
                                &session_id,
                            )
                        })
                        .count();
                    if let Err(error) = record_active_attempt_progress(
                        &db_path,
                        &session_id,
                        &mut attempt_progress_tracker,
                        iteration,
                        goal_tool_call_count.saturating_add(result.tool_calls.len()),
                        goal_research_action_count.saturating_add(research_actions),
                        result.tool_calls.len(),
                        research_actions,
                        tool_batch_made_material_progress(&tool_msg.content),
                        None,
                    ) {
                        let _ = event_tx.send(LoopEvent::Error {
                            error: format!(
                                "Goal input-wait accounting could not be persisted: {error}"
                            ),
                        });
                        finish_run(
                            attempt_progress_tracker.goal_id(),
                            &db_path,
                            &session_id,
                            &event_tx,
                            "error",
                            LoopStopReason::ProviderError,
                        );
                        return;
                    }
                }
                let recovery = build_awaiting_input_recovery_state(
                    ask_user_partial_assistant,
                    ask_user_pending_interactions,
                    permission_mode,
                    execution_tool_allowlist.as_ref(),
                );
                if let Err(error) =
                    persist_required_recovery_state(&db_path, &session_id, &recovery)
                {
                    fail_required_recovery_persistence(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        &error,
                    );
                    return;
                }

                let _ = event_tx.send(LoopEvent::AwaitingInput {
                    tool_call_id: pending_ask_user_call.id.clone(),
                    tool_name: pending_ask_user_call.name.clone(),
                });

                finish_run_preserving_recovery(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "awaiting_input",
                    LoopStopReason::AwaitingInput,
                );
                return;
            }

            // Execute tools
            let tool_execution_partial_assistant =
                build_partial_assistant_state(&result.recovery_checkpoint);
            persist_recovery_state(
                &db_path,
                &session_id,
                &build_recovery_state(
                    &context_ledger,
                    RecoveryStatus::ToolExecuting,
                    None,
                    None,
                    tool_execution_partial_assistant.clone(),
                ),
            );
            set_agent_state(&db_path, &session_id, "tool_executing");
            let tool_batch = executor::execute_tools(
                &result.tool_calls,
                &tool_registry,
                &ai_client,
                &working_dir,
                project_dir.as_deref(),
                &process_registry,
                &skills_manager,
                &session_id,
                &db_path,
                user_id.as_deref(),
                permission_mode,
                work_mode,
                (!context_mode.is_worker_goal()).then_some(&tool_execution_partial_assistant),
                delegated_progress_tx.as_ref(),
                &event_tx,
                Some(&provider_call_trace),
                &mut input_inbox,
                project_settings.subagent_max_turns,
                options.reasoning_effort,
                &advertised_tool_names,
                execution_tool_allowlist.as_ref(),
                project_settings.disabled_tools.as_deref(),
                hive_group_run.as_ref(),
                Arc::clone(&file_observations),
                if context_mode.is_worker_goal() {
                    executor::ExtensionExecutionPolicy::DisabledWorkerGoal
                } else if context_mode.is_isolated_worker() {
                    executor::ExtensionExecutionPolicy::Disabled
                } else {
                    executor::ExtensionExecutionPolicy::Enabled
                },
            )
            .await;
            work_mode = tool_batch.next_work_mode;
            let background_agent_started = tool_batch.background_agent_started;
            // A tool batch can change either work mode (`set_work_mode`) or
            // plan lifecycle state (for example, completing the final task).
            // Refresh both dimensions before the next provider request.
            mode_tool_surface.refresh(
                &mut options,
                &mut advertised_tool_names,
                ai_client.as_ref(),
                permission_mode,
                work_mode,
                !context_mode.is_isolated_worker()
                    && has_active_workflow_or_plan(&db_path, &session_id),
                project_settings
                    .disabled_tools
                    .as_deref()
                    .unwrap_or_default(),
                execution_tool_allowlist.as_ref(),
            );
            let tool_results = tool_batch.results;
            if context_mode.is_worker_goal() {
                worker_goal_outcome_journal.record_tool_results(&result.tool_calls, &tool_results);
            }
            let delegated_store = Database::new(&db_path).ok().map(DelegatedRunStore::new);
            let current_turn_tool_calls = result.tool_calls.len();
            let current_turn_research_actions = result
                .tool_calls
                .iter()
                .filter(|call| {
                    is_research_action(call, &tool_results, delegated_store.as_ref(), &session_id)
                })
                .count();
            if context_mode.is_worker_goal() {
                worker_goal_outcome_journal.record_research_actions(current_turn_research_actions);
            }
            goal_tool_call_count = goal_tool_call_count.saturating_add(current_turn_tool_calls);
            goal_research_action_count =
                goal_research_action_count.saturating_add(current_turn_research_actions);

            if tool_batch.cancelled {
                finish_cancelled_tool_batch(
                    &db_path,
                    &session_id,
                    &event_tx,
                    active_goal,
                    &mut attempt_progress_tracker,
                    &result.tool_calls,
                    tool_results,
                    (iteration, goal_tool_call_count, goal_research_action_count),
                    current_turn_research_actions,
                );
                return;
            }

            // Canonical failure and semantic-progress evaluation shared with children.
            let guard = loop_guard.evaluate(&result.tool_calls, &tool_results);
            let fail_diagnostic = guard.repeated_failure;
            let explore_diagnostic =
                failure::detect_terminal_explore_failure(&result.tool_calls, &tool_results);
            let read_only_loop_diagnostic = guard.repeated_read_only;
            let post_explore_diagnostic =
                failure::detect_post_explore_manual_fallback(&conversation, &result.tool_calls);
            let validation_diagnostic = guard.repeated_validation;
            let progress_telemetry = guard.progress;
            let progress_diagnostic = progress_telemetry
                .as_ref()
                .and_then(|telemetry| telemetry.diagnostic());
            let progress_replan_instruction = progress_telemetry
                .as_ref()
                .and_then(|telemetry| telemetry.replan_instruction())
                .map(ToString::to_string);
            let guard_loop_landing = fail_diagnostic
                .as_ref()
                .or(explore_diagnostic.as_ref())
                .or(read_only_loop_diagnostic.as_ref())
                .or(post_explore_diagnostic.as_ref())
                .or(progress_diagnostic.as_ref())
                .map(|diagnostic| LoopGuardLanding {
                    diagnostic: diagnostic.clone(),
                    block_goal: true,
                })
                .or_else(|| {
                    validation_diagnostic
                        .as_ref()
                        .map(|diagnostic| LoopGuardLanding {
                            diagnostic: diagnostic.clone(),
                            block_goal: false,
                        })
                });
            let delegation_nudge_instruction = (!context_mode.is_isolated_worker()
                && guard_loop_landing.is_none())
            .then(|| {
                delegation_nudge_tracker.record_turn(
                    &result.tool_calls,
                    &tool_results,
                    project_settings.delegation_mode.unwrap_or_default(),
                    advertised_tool_names.contains("agent"),
                )
            })
            .flatten();
            let terminal_loop_landing = guard_loop_landing;
            let blocker_fingerprint = terminal_loop_landing
                .as_ref()
                .filter(|landing| landing.block_goal)
                .map(|landing| landing.diagnostic.clone());
            let token_budget_stopped = goal_token_stop.is_some();
            let attempt_progress_result = if active_goal {
                record_active_attempt_progress(
                    &db_path,
                    &session_id,
                    &mut attempt_progress_tracker,
                    iteration,
                    goal_tool_call_count,
                    goal_research_action_count,
                    current_turn_tool_calls,
                    current_turn_research_actions,
                    tool_batch_made_material_progress(&tool_results),
                    blocker_fingerprint,
                )
            } else {
                Ok(None)
            };
            let attempt_accounting_error = attempt_progress_result
                .as_ref()
                .err()
                .map(ToString::to_string);
            let goal_runtime_stop =
                goal_token_stop.or_else(|| attempt_progress_result.ok().flatten());
            if let Some(telemetry) = progress_telemetry {
                if telemetry.triggered {
                    tracing::warn!(
                        iteration,
                        session_id = %session_id,
                        no_progress_turns = telemetry.no_progress_turns,
                        threshold = telemetry.threshold,
                        action = ?telemetry.action,
                        evidence_signature = %telemetry.evidence_signature,
                        "Semantic no-progress guard triggered"
                    );
                } else {
                    tracing::info!(
                        iteration,
                        session_id = %session_id,
                        no_progress_turns = telemetry.no_progress_turns,
                        threshold = telemetry.threshold,
                        action = ?telemetry.action,
                        evidence_signature = %telemetry.evidence_signature,
                        "Semantic no-progress guard warning"
                    );
                }
                let _ = event_tx.send(LoopEvent::ProgressGuard { telemetry });
            }

            // Save tool results
            let tool_msg = ModelMessage {
                role: Role::User,
                content: tool_results,
            };
            conversation.push(tool_msg.clone());
            if let Some(instruction) = progress_replan_instruction {
                conversation.push(ModelMessage {
                    role: Role::System,
                    content: vec![Content::text(instruction)],
                });
            }
            if let Some(instruction) = delegation_nudge_instruction {
                conversation.push(ModelMessage {
                    role: Role::System,
                    content: vec![Content::text(instruction)],
                });
            }
            refresh_validation_reminder(
                &mut conversation,
                session_type,
                &result.tool_calls,
                &tool_msg.content,
            );
            context_ledger.update_from_conversation(&conversation);
            persist_context_state(&db_path, &session_id, &context_ledger);
            save_message(&db_path, &session_id, &tool_msg);
            clear_recovery_state(&db_path, &session_id);

            if let Some(error) = attempt_accounting_error {
                let _ = event_tx.send(LoopEvent::Error {
                    error: format!("Goal accounting could not be persisted: {error}"),
                });
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "error",
                    LoopStopReason::ProviderError,
                );
                return;
            }
            goal_final_continuation_attempted = false;

            if background_agent_started {
                if context_mode.is_worker_goal() {
                    finish_worker_provider_attention(&db_path, &session_id, &event_tx,
                        "Hive Worker Goal reached a forbidden background-agent start; no Workflow progress was published".into());
                    return;
                }
                conversation.push(ModelMessage {
                    role: Role::System,
                    content: vec![Content::text("A background Agent is working and owns a durable completion wake. Continue useful independent work now. Do not poll or wait repeatedly. When only child-dependent work remains, end the foreground response with an honest progress update and await the completion wake.".into())],
                });
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
            }

            if active_goal
                && successful_task_completion_needs_goal_followthrough(
                    &db_path,
                    &session_id,
                    &result.tool_calls,
                    &tool_msg.content,
                )
            {
                let instruction = "All approved plan steps are now terminal, but the Goal is still active. Continue in this same run: call `workflow_update` with `verify_criterion` and concrete evidence for every pending required criterion, then call `workflow_update` with `complete_goal`. Do not stop or claim completion in prose while the Goal remains active.";
                conversation.push(ModelMessage {
                    role: Role::System,
                    content: vec![Content::text(instruction.to_string())],
                });
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
            }

            let loop_guard_owns_blocked_stop = !token_budget_stopped
                && terminal_loop_landing
                    .as_ref()
                    .is_some_and(|landing| landing.block_goal)
                && goal_runtime_stop
                    .as_ref()
                    .is_some_and(|(status, _)| *status == GoalStatus::Blocked);
            if !loop_guard_owns_blocked_stop {
                if let Some((status, reason)) = goal_runtime_stop {
                    let reason = reason.unwrap_or_else(|| "goal_attempt_stopped".to_string());
                    let _ = event_tx.send(LoopEvent::Error {
                        error: format!("Goal attempt stopped: {reason}"),
                    });
                    let _ = event_tx.send(LoopEvent::TurnComplete {
                        turn: iteration,
                        has_more: false,
                    });
                    finish_run(
                        attempt_progress_tracker.goal_id(),
                        &db_path,
                        &session_id,
                        &event_tx,
                        "idle",
                        if status == GoalStatus::Blocked {
                            LoopStopReason::LoopGuardTriggered
                        } else {
                            LoopStopReason::BudgetExhausted
                        },
                    );
                    return;
                }
            }

            input_inbox.collect_ready();
            if input_inbox.take_cancel() {
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    LoopStopReason::UserAbort,
                );
                return;
            }
            let injected_steering = if context_mode.is_isolated_worker() {
                let _ = input_inbox.take_steering();
                Vec::new()
            } else {
                inject_pending_steering(
                    &mut input_inbox,
                    &mut conversation,
                    &mut context_ledger,
                    &db_path,
                    &session_id,
                )
            };
            if !injected_steering.is_empty() {
                reset_recovery_flags(
                    &mut loop_guard,
                    active_goal,
                    &mut loop_guard_landing,
                    &mut delegation_nudge_tracker,
                    &mut empty_stream_retry_attempted,
                    &mut empty_completion_retry_attempted,
                    &mut empty_completion_recovery_pending,
                    &mut provider_tool_activity_seen,
                    &mut overflow_compact_retry_attempted,
                    &mut goal_final_continuation_attempted,
                );
                emit_steering_turn_boundary(
                    &db_path,
                    &session_id,
                    &event_tx,
                    iteration,
                    injected_steering,
                );
                continue;
            }

            if let Some(landing) = terminal_loop_landing {
                tracing::warn!(
                    iteration,
                    session_id = %session_id,
                    diagnostic = %landing.diagnostic,
                    blocks_goal = landing.block_goal,
                    "Loop guard entered one bounded synthesis landing turn"
                );
                let instruction = loop_guard_landing_instruction(&landing);
                loop_guard_landing = Some(landing);
                conversation.push(ModelMessage {
                    role: Role::System,
                    content: vec![Content::text(instruction)],
                });
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
                clear_recovery_state(&db_path, &session_id);
                set_agent_state(&db_path, &session_id, "streaming");
                let _ = event_tx.send(LoopEvent::TurnComplete {
                    turn: iteration,
                    has_more: true,
                });
                continue;
            }

            if let Some(explore_summary) = (!context_mode.is_worker_goal() && !active_goal)
                .then(|| finalize_explore_only_turn(&result.tool_calls, &tool_msg.content))
                .flatten()
            {
                let _ = event_tx.send(LoopEvent::TextDelta {
                    delta: explore_summary.clone(),
                });

                let assistant_msg = ModelMessage {
                    role: Role::Assistant,
                    content: vec![Content::text(explore_summary)],
                };
                conversation.push(assistant_msg.clone());
                context_ledger.update_from_conversation(&conversation);
                persist_context_state(&db_path, &session_id, &context_ledger);
                save_message(&db_path, &session_id, &assistant_msg);

                if !title_generated {
                    maybe_generate_title(&conversation, &event_tx, &session_id, &db_path);
                }

                if last_token_count > 0 {
                    update_token_count(&db_path, &session_id, last_token_count);
                }
                let _ = event_tx.send(LoopEvent::TurnComplete {
                    turn: iteration,
                    has_more: false,
                });
                finish_run(
                    attempt_progress_tracker.goal_id(),
                    &db_path,
                    &session_id,
                    &event_tx,
                    "idle",
                    LoopStopReason::Completed,
                );
                return;
            }

            set_agent_state(&db_path, &session_id, "streaming");
            let _ = event_tx.send(LoopEvent::TurnComplete {
                turn: iteration,
                has_more: true,
            });
        }
    }
}

/// Promote each queued user follow-up at a model boundary and append it to the
/// in-memory conversation exactly once. Server-originated inputs are first
/// staged under a non-canonical role; promotion deletes that staging row and
/// appends the canonical user message in one SQLite transaction.
struct InjectedSteering {
    pending_id: Option<String>,
    message: String,
}

fn no_tool_completion_should_continue(steering: &[InjectedSteering]) -> bool {
    !steering.is_empty()
}

fn emit_steering_events(
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    steering: Vec<InjectedSteering>,
) {
    for input in steering {
        let _ = event_tx.send(LoopEvent::SteeringInjected {
            pending_id: input.pending_id,
            message: input.message,
        });
    }
}

fn emit_workflow_update_inputs(
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    updates: Vec<(String, u64, String)>,
) -> bool {
    let changed = !updates.is_empty();
    for (goal_id, aggregate_revision, operation_id) in updates {
        let _ = event_tx.send(LoopEvent::WorkflowUpdated {
            goal_id,
            aggregate_revision,
            operation_id,
        });
    }
    changed
}

fn steering_display_message(content: &[Content]) -> String {
    let text = content
        .iter()
        .filter_map(|item| match item {
            Content::Text { text, .. } if !text.trim().is_empty() => Some(text.trim()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    if text.is_empty() {
        "[Attached content]".to_string()
    } else {
        text
    }
}

fn inject_pending_steering(
    input_inbox: &mut LoopInputInbox,
    conversation: &mut Vec<ModelMessage>,
    context_ledger: &mut ContextLedger,
    db_path: &Path,
    session_id: &str,
) -> Vec<InjectedSteering> {
    let pending = input_inbox.take_steering();
    if pending.is_empty() {
        return Vec::new();
    }

    let session_manager = Database::new(db_path).ok().map(SessionManager::new);
    let mut injected = Vec::new();

    for steering in pending {
        if steering.content.is_empty() {
            continue;
        }

        if let Some(pending_id) = steering.pending_id.as_deref() {
            let promoted = session_manager
                .as_ref()
                .and_then(|manager| {
                    manager
                        .promote_pending_steering(session_id, pending_id)
                        .map_err(|error| {
                            tracing::warn!(
                                session_id,
                                pending_id,
                                %error,
                                "Failed to promote durable live steering input"
                            );
                            error
                        })
                        .ok()
                })
                .flatten();
            if promoted.is_none() {
                // Missing staging rows are duplicate deliveries; failed
                // promotions remain durable for restart recovery.
                continue;
            }
        }

        let display_message = steering_display_message(&steering.content);
        let message = ModelMessage {
            role: Role::User,
            content: steering.content,
        };
        conversation.push(message.clone());
        if steering.pending_id.is_none() && !steering.already_persisted {
            save_message(db_path, session_id, &message);
        }
        injected.push(InjectedSteering {
            pending_id: steering.pending_id,
            message: display_message,
        });
    }

    if !injected.is_empty() {
        context_ledger.update_from_conversation(conversation);
        persist_context_state(db_path, session_id, context_ledger);
    }
    injected
}

fn is_stale_compaction_snapshot_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .starts_with("Session transcript changed before compaction started")
}

fn reload_persisted_conversation(
    db_path: &Path,
    session_id: &str,
) -> Result<Vec<ModelMessage>, String> {
    let manager = Database::new(db_path)
        .map(SessionManager::new)
        .map_err(|error| format!("open transcript database: {error}"))?;
    manager
        .load_session_messages(session_id)
        .map_err(|error| format!("load transcript: {error}"))?
        .into_iter()
        .map(|(role, content_json)| {
            let role = match role.as_str() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                value => return Err(format!("unsupported persisted role {value:?}")),
            };
            let content = serde_json::from_str(&content_json)
                .map_err(|error| format!("parse persisted {role:?} message: {error}"))?;
            Ok(ModelMessage { role, content })
        })
        .collect()
}

/// A post-mutation cue lasts for one provider response. It is an advisory,
/// not a ledger of required checks: consuming it must not depend on recognizing
/// the next command, or imply that user/Goal verification requirements passed.
fn refresh_validation_reminder(
    conversation: &mut Vec<ModelMessage>,
    session_type: SessionType,
    tool_calls: &[crate::ai::types::AiToolCall],
    tool_results: &[Content],
) {
    remove_validation_reminders(conversation);
    if session_type != SessionType::Code {
        return;
    }
    let changed_ids = tool_results
        .iter()
        .filter_map(|result| match result {
            Content::ToolResult {
                tool_use_id,
                output,
                is_error,
            } if !is_error.unwrap_or(false) && trusted_changed(output) == Some(true) => {
                Some(tool_use_id.as_str())
            }
            _ => None,
        })
        .collect::<HashSet<_>>();
    if tool_calls
        .iter()
        .any(|call| changed_ids.contains(call.id.as_str()) && is_mutation_call(call))
    {
        conversation.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(VALIDATION_REMINDER.into())],
        });
    }
}

fn successful_task_completion_needs_goal_followthrough(
    db_path: &Path,
    session_id: &str,
    tool_calls: &[AiToolCall],
    tool_results: &[Content],
) -> bool {
    let successful_ids = tool_results
        .iter()
        .filter_map(|result| match result {
            Content::ToolResult {
                tool_use_id,
                is_error,
                ..
            } if !is_error.unwrap_or(false) => Some(tool_use_id.as_str()),
            _ => None,
        })
        .collect::<HashSet<_>>();
    if !tool_calls
        .iter()
        .any(|call| call.name == "task_complete" && successful_ids.contains(call.id.as_str()))
    {
        return false;
    }

    let snapshot = WorkflowManager::new(db_path.to_path_buf())
        .and_then(|manager| manager.get_snapshot(session_id))
        .ok()
        .flatten();
    snapshot.is_some_and(|snapshot| {
        snapshot.goal.status == GoalStatus::Active
            && snapshot.plan_revision.is_some()
            && !snapshot.steps.is_empty()
            && snapshot.steps.iter().all(|step| {
                matches!(
                    step.status,
                    WorkflowStepStatus::Completed | WorkflowStepStatus::Skipped
                )
            })
    })
}

const VALIDATION_REMINDER: &str = "Files changed successfully. Verify with the narrowest relevant check available through your authorized tools. Respect user-assigned external validation: perform requested inspection, then report external checks as pending. If a required check is unavailable, say so; do not substitute repeated reads or invent test results. Explicit required checks and Goal verification criteria remain requirements. Once available verification is complete, report the change, evidence, and any remaining verification gaps.";

fn remove_validation_reminders(conversation: &mut Vec<ModelMessage>) {
    conversation.retain(|message| {
        !(message.role == Role::System
            && message.content.iter().any(
                |content| matches!(content, Content::Text { text, .. } if text == VALIDATION_REMINDER),
            ))
    });
}

fn is_mutation_call(call: &crate::ai::types::AiToolCall) -> bool {
    let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
    match name {
        "edit" | "write" | "multiedit" | "apply_patch" => true,
        "agent" => agent_call_requests_write(arguments),
        "bash" | "shell" | "execute" => arguments
            .get("command")
            .and_then(|value| value.as_str())
            .is_some_and(|command| {
                super::hooks::shell_policy::classify_bash_command(command)
                    .modifies_filesystem_or_process
            }),
        _ => false,
    }
}

async fn apply_in_place_compaction(
    db_path: &Path,
    session_id: &str,
    conversation: &mut Vec<ModelMessage>,
    context_ledger: &mut ContextLedger,
    working_dir: &Path,
    project_dir: Option<&str>,
    user_id: Option<&str>,
    ai_client: &AiClient,
    compaction_manager: &CompactionManager,
    trigger: CompactionTrigger,
    last_usage_prompt_tokens: Option<usize>,
    messages_after_usage: usize,
    request_budget: Option<CompactionRequestBudget>,
    provider_call_trace: &super::ProviderCallTraceContext,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
) -> Result<CompactionResult, anyhow::Error> {
    let reason = match &trigger {
        CompactionTrigger::Auto => "auto",
        CompactionTrigger::Manual { .. } => "manual",
        CompactionTrigger::Reactive => "reactive",
        CompactionTrigger::Overflow => "overflow",
    }
    .to_string();

    let _ = event_tx.send(LoopEvent::ContextCompactionStarted {
        reason: reason.clone(),
    });

    let compaction_result = run_compaction_pipeline_observed(
        CompactionRequest {
            db_path,
            session_id,
            conversation,
            working_dir,
            ai_client: Some(ai_client),
            model: Some(ai_client.config().model.as_str()),
            trigger,
            compaction_manager: *compaction_manager,
            request_budget,
            last_usage_prompt_tokens,
            messages_after_usage,
            summary_override: None,
            project_dir,
            user_id,
        },
        provider_call_trace,
    )
    .await?;

    *conversation = compaction_result.compacted_conversation.clone();
    context_ledger.update_from_conversation(conversation);
    persist_context_state(db_path, session_id, context_ledger);
    let _ = event_tx.send(LoopEvent::ContextCompacted {
        reason: reason.clone(),
        estimated_tokens_before: compaction_result.estimated_tokens_before,
        estimated_tokens_after: compaction_result.estimated_tokens_after,
        replaced_messages: compaction_result.replaced_messages,
        checkpoint_id: compaction_result.checkpoint_id.clone(),
        compaction_count: compaction_result.compaction_count,
    });
    tracing::info!(
        session_id = %session_id,
        reason = %reason,
        tokens_before = compaction_result.estimated_tokens_before,
        tokens_after = compaction_result.estimated_tokens_after,
        replaced = compaction_result.replaced_messages,
        "In-place compaction completed"
    );

    Ok(compaction_result)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::sync::{Arc, Mutex};

    use super::effective_context_window_for_runtime;
    use super::empty_completion_action;
    use super::ensure_active_goal_attempt_for_run;
    use super::inject_pending_steering;
    use super::inject_runtime_context;
    use super::is_research_action;
    use super::message_builder::finalize_explore_only_turn;
    use super::no_tool_completion_should_continue;
    use super::provider_options_for_turn;
    use super::refresh_validation_reminder;
    use super::remove_validation_reminders;
    use super::resolve_project_permission_mode;
    use super::should_retry_empty_stream_interruption;
    use super::split_single_pending_ask_user_call;
    use super::successful_task_completion_needs_goal_followthrough;
    use super::terminal_agent_state_after_interruption;
    use super::worker_response_commit_terminal_outcome;
    use super::AttemptProgressTracker;
    use super::CanonicalSessionPersistenceBoundary;
    use super::EmptyCompletionAction;
    use super::LoopGuardLanding;
    use super::WorkerGoalOutcomeFinalize;
    use super::WorkerGoalOutcomeJournal;
    use super::VALIDATION_REMINDER;
    use super::{
        is_stale_compaction_snapshot_error, mpsc, reload_persisted_conversation, ContextLedger,
    };
    use crate::agent::loop_events::{LoopInput, LoopInputInbox, LoopStopReason};
    use crate::agent::subagent::AgentCapability;
    use crate::agent::{
        DelegatedRunStage, WorkerGoalAttemptOutcome, WorkerGoalEffectSummary,
        WorkerGoalOutcomeCommit, WorkerGoalOutcomeCommitDisposition, WorkerGoalOutcomeCommitError,
        WorkerGoalOutcomeCommitInput, WorkerGoalOutcomeCommitter, WorkerGoalOutcomeCounters,
        WorkerProviderTerminalOutcome,
    };
    use crate::ai::client::CallOptions;
    use crate::ai::types::{AiTool, AiToolCall, Content, ModelMessage, Role};
    use crate::skills::SkillsManager;
    use crate::storage::{
        Database, DelegatedRunRole, DelegatedRunScope, DelegatedRunStartInput, DelegatedRunStore,
        ProjectSettings, SessionManager, SessionType, WorkMode,
        WorkerConversationResponseCommitError, WorkerRunOrigin,
    };
    use crate::tools::registry::PermissionMode;
    use crate::workflow::{
        AttemptStatus, CompleteStepInput, CreateGoalInput, CriterionInput, GoalStatus,
        PlanProposalInput, StepProposalInput, WorkflowManager, WorkflowStepStatus,
        DEFAULT_GOAL_ATTEMPT_MAX_RESEARCH_ACTIONS, DEFAULT_GOAL_ATTEMPT_MAX_TURNS,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    #[derive(Clone, Copy)]
    enum GoalCommitBehavior {
        Insert,
        Stale,
        Conflict,
    }

    struct OrderedGoalCommitter {
        order: Arc<Mutex<Vec<String>>>,
        behavior: GoalCommitBehavior,
    }

    impl WorkerGoalOutcomeCommitter for OrderedGoalCommitter {
        fn commit_outcome(
            &self,
            _input: &WorkerGoalOutcomeCommitInput,
        ) -> Result<WorkerGoalOutcomeCommit, WorkerGoalOutcomeCommitError> {
            self.order.lock().unwrap().push("outcome".into());
            match self.behavior {
                GoalCommitBehavior::Insert => Ok(WorkerGoalOutcomeCommit {
                    disposition: WorkerGoalOutcomeCommitDisposition::Inserted,
                }),
                GoalCommitBehavior::Stale => {
                    Err(WorkerGoalOutcomeCommitError::StaleRejected("stale".into()))
                }
                GoalCommitBehavior::Conflict => Err(
                    WorkerGoalOutcomeCommitError::ConflictOrCorrupt("conflict".into()),
                ),
            }
        }
    }

    fn worker_goal_outcome_input() -> WorkerGoalOutcomeCommitInput {
        WorkerGoalOutcomeCommitInput::from_validated_run(
            "worker-1".into(),
            1,
            Some("user-1".into()),
            "worker-session".into(),
            "run-1".into(),
            "lease-1".into(),
            1,
            WorkerRunOrigin::UserWorkflowActivation,
            "goal-1".into(),
            1,
            1,
            "attempt-1".into(),
            "plan-1".into(),
            1,
            "step-1".into(),
            1,
            std::path::PathBuf::from("/tmp/worker-goal-workspace"),
            vec!["provider-call-1".into()],
            WorkerGoalAttemptOutcome::Succeeded,
            Vec::new(),
            WorkerGoalEffectSummary::new("No workspace mutation was observed.", false).unwrap(),
            WorkerGoalOutcomeCounters {
                provider_calls: 1,
                turns: 1,
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn worker_goal_outcome_commit_precedes_final_provider_completion() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let committer = OrderedGoalCommitter {
            order: Arc::clone(&order),
            behavior: GoalCommitBehavior::Insert,
        };
        let provider_order = Arc::clone(&order);
        let result = super::commit_worker_goal_outcome_before_provider_completion(
            &committer,
            &worker_goal_outcome_input(),
            move |outcome| {
                provider_order
                    .lock()
                    .unwrap()
                    .push(format!("provider:{outcome:?}"));
                Ok(())
            },
        );
        assert!(matches!(result, WorkerGoalOutcomeFinalize::Committed));
        assert_eq!(
            order.lock().unwrap().clone(),
            vec!["outcome".to_string(), "provider:Completed".to_string()]
        );
    }

    #[test]
    fn ambiguous_worker_goal_commit_leaves_final_provider_call_unresolved() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let committer = OrderedGoalCommitter {
            order: Arc::clone(&order),
            behavior: GoalCommitBehavior::Conflict,
        };
        let provider_order = Arc::clone(&order);
        let result = super::commit_worker_goal_outcome_before_provider_completion(
            &committer,
            &worker_goal_outcome_input(),
            move |outcome| {
                provider_order
                    .lock()
                    .unwrap()
                    .push(format!("provider:{outcome:?}"));
                Ok(())
            },
        );
        assert!(matches!(result, WorkerGoalOutcomeFinalize::Ambiguous(_)));
        assert_eq!(order.lock().unwrap().clone(), vec!["outcome".to_string()]);
    }

    #[test]
    fn proven_stale_worker_goal_commit_terminalizes_with_stale_outcome_only() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let committer = OrderedGoalCommitter {
            order: Arc::clone(&order),
            behavior: GoalCommitBehavior::Stale,
        };
        let provider_order = Arc::clone(&order);
        let result = super::commit_worker_goal_outcome_before_provider_completion(
            &committer,
            &worker_goal_outcome_input(),
            move |outcome| {
                provider_order
                    .lock()
                    .unwrap()
                    .push(format!("provider:{outcome:?}"));
                Ok(())
            },
        );
        assert!(matches!(result, WorkerGoalOutcomeFinalize::ProvenStale));
        assert_eq!(
            order.lock().unwrap().clone(),
            vec![
                "outcome".to_string(),
                "provider:CanonicalCommitStale".to_string()
            ]
        );
    }

    #[test]
    fn worker_goal_journal_keeps_bounded_typed_effects_not_raw_tool_output() {
        let mut journal = WorkerGoalOutcomeJournal::default();
        journal.record_provider_call("provider-call-1".into());
        let read_call = AiToolCall {
            id: "MODEL-SELECTED-TOOL-ID-CANARY".into(),
            name: "read".into(),
            arguments: json!({"file_path": "src/lib.rs"}),
        };
        journal.record_tool_results(
            std::slice::from_ref(&read_call),
            &[Content::ToolResult {
                tool_use_id: read_call.id.clone(),
                output: json!({
                    "tool": "MODEL-SPOOFED-TOOL-NAME-CANARY",
                    "summary": "RAW-TOOL-OUTPUT-SECRET-CANARY",
                    "is_error": false
                }),
                is_error: None,
            }],
        );
        journal.record_research_actions(1);

        assert_eq!(journal.counters.provider_calls, 1);
        assert_eq!(journal.counters.tool_calls, 1);
        assert_eq!(journal.counters.successful_tool_calls, 1);
        assert_eq!(journal.counters.research_actions, 1);
        assert!(journal.can_commit_outcome());
        assert_eq!(
            journal.attempt_outcome(),
            WorkerGoalAttemptOutcome::Progressed
        );
        let evidence = journal
            .evidence
            .first()
            .expect("one typed observation should be retained")
            .summary();
        assert!(evidence.contains("read"));
        assert!(!evidence.contains("RAW-TOOL-OUTPUT-SECRET-CANARY"));
        assert!(!evidence.contains("MODEL-SELECTED-TOOL-ID-CANARY"));
        assert!(!evidence.contains("MODEL-SPOOFED-TOOL-NAME-CANARY"));
    }

    #[test]
    fn worker_goal_mutation_evidence_requires_explicit_changed_true() {
        let journal = |name: &str, output: serde_json::Value| {
            let call = AiToolCall {
                id: format!("{name}-call"),
                name: name.into(),
                arguments: json!({}),
            };
            let mut journal = WorkerGoalOutcomeJournal::default();
            journal.record_tool_results(
                std::slice::from_ref(&call),
                &[Content::ToolResult {
                    tool_use_id: call.id.clone(),
                    output,
                    is_error: None,
                }],
            );
            journal
        };

        let unchanged = journal(
            "write",
            json!({"tool": "write", "is_error": false, "changed": false}),
        );
        assert!(!unchanged.workspace_mutated);
        assert_eq!(
            unchanged.evidence[0].kind(),
            crate::agent::WorkerGoalEvidenceKind::WorkspaceObservation
        );
        assert!(!unchanged.effect_summary().unwrap().workspace_mutated());

        let missing = journal("edit", json!({"tool": "edit", "is_error": false}));
        assert!(!missing.workspace_mutated);
        assert_eq!(
            missing.evidence[0].kind(),
            crate::agent::WorkerGoalEvidenceKind::Runtime
        );

        let invalid = journal(
            "edit",
            json!({"tool": "edit", "is_error": false, "changed": "yes"}),
        );
        assert!(!invalid.workspace_mutated);
        assert_eq!(
            invalid.evidence[0].kind(),
            crate::agent::WorkerGoalEvidenceKind::Runtime
        );

        let opaque = journal(
            "bash",
            json!({"tool": "bash", "is_error": false, "changed": true}),
        );
        assert!(!opaque.workspace_mutated);
        assert_eq!(
            opaque.evidence[0].kind(),
            crate::agent::WorkerGoalEvidenceKind::Runtime
        );

        let changed = journal(
            "write",
            json!({"tool": "write", "is_error": false, "changed": true}),
        );
        assert!(changed.workspace_mutated);
        assert_eq!(
            changed.evidence[0].kind(),
            crate::agent::WorkerGoalEvidenceKind::WorkspaceMutation
        );
        assert!(changed.effect_summary().unwrap().workspace_mutated());
    }

    #[test]
    fn worker_goal_nonempty_prose_without_tool_evidence_cannot_commit_progress() {
        let final_text = "I cannot do this, but I am finished.";
        let mut journal = WorkerGoalOutcomeJournal::default();
        journal.record_provider_call("provider-call-1".into());

        assert!(!final_text.trim().is_empty());
        assert_eq!(journal.counters.successful_tool_calls, 0);
        assert!(journal.evidence.is_empty());
        assert!(!journal.can_commit_outcome());
    }

    #[test]
    fn worker_goal_generic_command_heuristics_never_claim_verified_completion() {
        let mut journal = WorkerGoalOutcomeJournal::default();
        journal.record_provider_call("provider-call-1".into());
        let validation_call = AiToolCall {
            id: "validation-call".into(),
            name: "bash".into(),
            arguments: json!({"command": "cargo test -p focused-package"}),
        };
        journal.record_tool_results(
            std::slice::from_ref(&validation_call),
            &[Content::ToolResult {
                tool_use_id: validation_call.id.clone(),
                output: json!({"tool": "bash", "is_error": false}),
                is_error: None,
            }],
        );

        assert!(journal.can_commit_outcome());
        assert_eq!(
            journal.attempt_outcome(),
            WorkerGoalAttemptOutcome::Progressed
        );
        assert!(journal
            .evidence
            .iter()
            .all(|item| item.kind() != crate::agent::WorkerGoalEvidenceKind::Verification));
    }

    #[test]
    fn worker_goal_persistence_boundary_writes_no_trigger_tool_message_or_episode() {
        let temp = TempDir::new().expect("temporary directory should be created");
        let db_path = temp.path().join("worker-goal-persistence.db");
        let sessions = SessionManager::new(Database::new(&db_path).expect("database should open"));
        let session_id = sessions
            .create_session("Worker Goal DM", None, None)
            .expect("session should be created");
        let boundary = CanonicalSessionPersistenceBoundary { enabled: false };

        let trigger = ModelMessage {
            role: Role::User,
            content: vec![Content::text(
                "[WORKER GOAL TRIGGER v1] EPHEMERAL-TRIGGER-LEAK-CANARY".into(),
            )],
        };
        let tool_result = ModelMessage {
            role: Role::User,
            content: vec![Content::ToolResult {
                tool_use_id: "worker-goal-read-1".into(),
                output: json!("TOOL-RESULT-LEAK-CANARY"),
                is_error: None,
            }],
        };
        boundary.save_message(&db_path, &session_id, &trigger);
        boundary.save_message(&db_path, &session_id, &tool_result);
        boundary.persist_context_state(
            &db_path,
            &session_id,
            &ContextLedger::from_conversation(&[trigger]),
        );
        boundary.update_token_count(&db_path, &session_id, 999);

        let db = Database::new(&db_path).expect("database should reopen");
        let message_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .expect("message count should load");
        let episode_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM conversation_episodes WHERE session_id = ?1",
                [&session_id],
                |row| row.get(0),
            )
            .expect("episode count should load");
        let (ledger, continuation, recovery, token_count): (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
        ) = db
            .conn()
            .query_row(
                "SELECT context_ledger_json, continuation_json, recovery_json, token_count
                 FROM sessions WHERE id = ?1",
                [&session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("session persistence columns should load");

        assert_eq!(message_count, 0);
        assert_eq!(episode_count, 0);
        assert!(ledger.is_none());
        assert!(continuation.is_none());
        assert!(recovery.is_none());
        assert!(token_count.is_none_or(|count| count == 0));
    }

    #[test]
    fn worker_response_commit_failure_only_terminalizes_proven_stale_rejection() {
        assert_eq!(
            worker_response_commit_terminal_outcome(
                &WorkerConversationResponseCommitError::StaleRejected("paused".into()),
            ),
            Some(WorkerProviderTerminalOutcome::CanonicalCommitStale),
        );
        assert_eq!(
            worker_response_commit_terminal_outcome(
                &WorkerConversationResponseCommitError::ConflictOrCorrupt(
                    "different canonical content".into(),
                ),
            ),
            None,
            "a conflict can require canonical-row adoption, so Started must remain unresolved",
        );
        assert_eq!(
            worker_response_commit_terminal_outcome(
                &WorkerConversationResponseCommitError::CommitUncertain(
                    "commit acknowledgement lost".into(),
                ),
            ),
            None,
            "an uncertain transaction must remain Started for fenced recovery",
        );
    }

    #[test]
    fn workflow_attempt_progress_uses_attempt_local_counters() {
        let mut tracker = AttemptProgressTracker::default();

        assert_eq!(tracker.local_counts("attempt-a", 1, 3, 2, 3, 2), (1, 3, 2));
        assert_eq!(tracker.local_counts("attempt-a", 4, 8, 5, 2, 1), (4, 8, 5));
        assert_eq!(
            tracker.local_counts("attempt-b", 5, 10, 6, 2, 1),
            (1, 2, 1),
            "a new workflow attempt must not inherit prior step counters"
        );
        assert_eq!(tracker.local_counts("attempt-b", 7, 13, 7, 1, 0), (3, 5, 2));
    }

    fn seed_research_accounting_run(
        store: &DelegatedRunStore,
        session_id: &str,
        delegated_run_id: &str,
        parent_tool_call_id: &str,
        role: DelegatedRunRole,
        stage: DelegatedRunStage,
        capabilities: BTreeSet<AgentCapability>,
    ) -> anyhow::Result<()> {
        store.create_run_with_child_contract(
            &DelegatedRunStartInput {
                delegated_run_id: delegated_run_id.to_string(),
                parent_session_id: session_id.to_string(),
                parent_tool_call_id: Some(parent_tool_call_id.to_string()),
                role,
                stage,
                provider: None,
                model: None,
                resumable: true,
                resumed_from_run_id: None,
                target_scope: vec![DelegatedRunScope {
                    label: "workspace".to_string(),
                    path: ".".to_string(),
                    kind: "workspace".to_string(),
                }],
            },
            Some(delegated_run_id),
            &capabilities,
        )?;
        Ok(())
    }

    fn successful_agent_result(
        tool_call_id: &str,
        delegated_run_id: &str,
        status: &str,
    ) -> Content {
        Content::ToolResult {
            tool_use_id: tool_call_id.to_string(),
            output: json!({
                "tool": "agent",
                "is_error": false,
                "result": {
                    "status": status,
                    "delegated_run_id": delegated_run_id,
                }
            }),
            is_error: None,
        }
    }

    #[test]
    fn research_accounting_uses_durable_resume_and_followup_contracts() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("delegated-research-accounting.db");
        let sessions = SessionManager::new(Database::new(&db_path)?);
        let session_id =
            sessions.create_session("research accounting", Some("test-model"), None)?;
        drop(sessions);
        let store = DelegatedRunStore::new(Database::new(&db_path)?);

        seed_research_accounting_run(
            &store,
            &session_id,
            "read-source",
            "original-read-call",
            DelegatedRunRole::Explore,
            DelegatedRunStage::Complete,
            BTreeSet::from([AgentCapability::Read]),
        )?;
        seed_research_accounting_run(
            &store,
            &session_id,
            "write-source",
            "original-write-call",
            DelegatedRunRole::Build,
            DelegatedRunStage::Complete,
            BTreeSet::from([AgentCapability::Read, AgentCapability::Write]),
        )?;
        seed_research_accounting_run(
            &store,
            &session_id,
            "read-continuation",
            "terminal-followup-read",
            DelegatedRunRole::Explore,
            DelegatedRunStage::Created,
            BTreeSet::from([AgentCapability::Read]),
        )?;
        seed_research_accounting_run(
            &store,
            &session_id,
            "write-continuation",
            "terminal-followup-write",
            DelegatedRunRole::Build,
            DelegatedRunStage::Created,
            BTreeSet::from([AgentCapability::Read, AgentCapability::Write]),
        )?;

        let bare_read_resume = AiToolCall {
            id: "resume-read".to_string(),
            name: "agent".to_string(),
            arguments: json!({"action": "resume", "delegated_run_id": "read-source"}),
        };
        assert!(is_research_action(
            &bare_read_resume,
            &[],
            Some(&store),
            &session_id
        ));

        let bare_write_resume = AiToolCall {
            id: "resume-write".to_string(),
            name: "agent".to_string(),
            arguments: json!({"action": "resume", "delegated_run_id": "write-source"}),
        };
        assert!(!is_research_action(
            &bare_write_resume,
            &[],
            Some(&store),
            &session_id
        ));

        let live_followup = AiToolCall {
            id: "live-followup".to_string(),
            name: "agent".to_string(),
            arguments: json!({"action": "followup", "delegated_run_id": "read-source"}),
        };
        assert!(!is_research_action(
            &live_followup,
            &[successful_agent_result(
                "live-followup",
                "read-source",
                "queued"
            )],
            Some(&store),
            &session_id
        ));

        let terminal_read_followup = AiToolCall {
            id: "terminal-followup-read".to_string(),
            name: "agent".to_string(),
            arguments: json!({"action": "followup", "delegated_run_id": "read-source"}),
        };
        assert!(is_research_action(
            &terminal_read_followup,
            &[successful_agent_result(
                "terminal-followup-read",
                "read-continuation",
                "background_started"
            )],
            Some(&store),
            &session_id
        ));

        let terminal_write_followup = AiToolCall {
            id: "terminal-followup-write".to_string(),
            name: "agent".to_string(),
            arguments: json!({"action": "followup", "delegated_run_id": "write-source"}),
        };
        assert!(!is_research_action(
            &terminal_write_followup,
            &[successful_agent_result(
                "terminal-followup-write",
                "write-continuation",
                "background_started"
            )],
            Some(&store),
            &session_id
        ));

        Ok(())
    }

    #[test]
    fn stale_compaction_snapshot_can_reload_canonical_transcript() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("stale-compaction-reload.db");
        let sessions = SessionManager::new(Database::new(&db_path)?);
        let session_id = sessions.create_session("reload", Some("test-model"), None)?;
        sessions.save_message(
            &session_id,
            "user",
            r#"[{"type":"text","text":"latest durable direction"}]"#,
        )?;
        sessions.save_message(
            &session_id,
            "assistant",
            r#"[{"type":"text","text":"durable response"}]"#,
        )?;

        let reloaded =
            reload_persisted_conversation(&db_path, &session_id).map_err(anyhow::Error::msg)?;
        assert_eq!(reloaded.len(), 2);
        assert_eq!(reloaded[0].role, Role::User);
        assert_eq!(reloaded[1].role, Role::Assistant);
        assert!(is_stale_compaction_snapshot_error(&anyhow::anyhow!(
            "Session transcript changed before compaction started; refusing a stale in-memory snapshot"
        )));
        assert!(!is_stale_compaction_snapshot_error(&anyhow::anyhow!(
            "provider request failed"
        )));
        Ok(())
    }

    #[test]
    fn active_goal_run_automatically_claims_next_ready_step() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("auto-attempt.db");
        let sessions = SessionManager::new(Database::new(&db_path)?);
        let session_id = sessions.create_session("auto attempt", Some("test-model"), None)?;
        let manager = WorkflowManager::new(db_path.clone())?;
        let created = manager.create_goal(
            &session_id,
            CreateGoalInput {
                title: "Execute reliably".into(),
                objective: "Claim the next step without prompt compliance".into(),
                constraints: vec![],
                criteria: vec![CriterionInput {
                    description: "The ready step is claimed".into(),
                    required: true,
                }],
                token_budget: None,
            },
            "create-auto-goal",
            "user",
        )?;
        let goal_id = created.snapshot.goal.id.clone();
        let proposed = manager.propose_plan(
            &session_id,
            &goal_id,
            created.snapshot.aggregate_revision,
            PlanProposalInput {
                title: "Automatic execution".into(),
                rationale: None,
                source_message_id: None,
                predecessor_id: None,
                legacy_markdown: None,
                steps: vec![StepProposalInput {
                    display_key: "1.1".into(),
                    description: "Implement the ready step".into(),
                    context: None,
                    parent_display_key: None,
                    dependencies: vec![],
                    acceptance_criteria: vec![],
                    required: true,
                }],
            },
            "propose-auto-plan",
            "agent",
        )?;
        let plan_id = proposed
            .snapshot
            .plan_revision
            .as_ref()
            .expect("proposed plan")
            .id
            .clone();
        let approved = manager.approve_plan(
            &session_id,
            &goal_id,
            &plan_id,
            proposed.snapshot.aggregate_revision,
            "approve-auto-plan",
            "user",
        )?;
        let active = manager.activate_goal(
            &session_id,
            &goal_id,
            approved.snapshot.aggregate_revision,
            "activate-auto-goal",
            "user",
        )?;
        assert_eq!(active.snapshot.goal.status, GoalStatus::Active);

        let mutation =
            ensure_active_goal_attempt_for_run(&db_path, &session_id, PermissionMode::Autonomous)?
                .expect("active run should create an attempt");
        assert_eq!(
            mutation.snapshot.steps[0].status,
            WorkflowStepStatus::InProgress
        );
        let attempt = mutation
            .snapshot
            .latest_attempt
            .as_ref()
            .expect("running attempt");
        assert_eq!(attempt.status, AttemptStatus::Running);
        assert_eq!(attempt.permission_mode, "autonomous");
        assert_eq!(attempt.max_turns, DEFAULT_GOAL_ATTEMPT_MAX_TURNS);
        assert_eq!(
            attempt.max_research_actions,
            DEFAULT_GOAL_ATTEMPT_MAX_RESEARCH_ACTIONS
        );
        let attempt_id = attempt.id.clone();
        let step_id = mutation.snapshot.steps[0].id.clone();
        let revision = mutation.snapshot.aggregate_revision;

        assert!(
            ensure_active_goal_attempt_for_run(&db_path, &session_id, PermissionMode::Autonomous,)?
                .is_none(),
            "a running attempt must not be duplicated"
        );

        let completed = manager.complete_step(
            &session_id,
            &goal_id,
            &step_id,
            revision,
            CompleteStepInput {
                attempt_id,
                outcome: "ready step completed".into(),
                evidence: vec!["focused validation passed".into()],
            },
            "complete-auto-step",
            "agent",
        )?;
        let tool_calls = vec![AiToolCall {
            id: "complete-call".into(),
            name: "task_complete".into(),
            arguments: json!({
                "task_id": step_id,
                "result": "ready step completed",
            }),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "complete-call".into(),
            output: json!({"ok": true}),
            is_error: Some(false),
        }];
        assert!(
            successful_task_completion_needs_goal_followthrough(
                &db_path,
                &session_id,
                &tool_calls,
                &tool_results,
            ),
            "the final task completion must keep the run alive for Goal follow-through"
        );

        let criterion_id = completed.snapshot.criteria[0].id.clone();
        manager.set_criterion(
            &session_id,
            &goal_id,
            &criterion_id,
            completed.snapshot.aggregate_revision,
            crate::workflow::SetCriterionInput {
                status: crate::workflow::CriterionStatus::Passed,
                evidence: vec!["focused validation passed".into()],
                verifier: "agent".into(),
            },
            "verify-auto-goal",
            "agent",
        )?;
        assert!(
            successful_task_completion_needs_goal_followthrough(
                &db_path,
                &session_id,
                &tool_calls,
                &tool_results,
            ),
            "a fully verified Goal still needs follow-through so the agent completes it explicitly"
        );
        Ok(())
    }

    #[test]
    fn durable_steering_promotes_once_after_the_completed_assistant_message() -> anyhow::Result<()>
    {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("krusty.db");
        let manager = SessionManager::new(Database::new(&db_path)?);
        let session_id = manager.create_session("steering", Some("test-model"), None)?;
        let initial_user = ModelMessage {
            role: Role::User,
            content: vec![Content::text("start".into())],
        };
        let assistant = ModelMessage {
            role: Role::Assistant,
            content: vec![Content::text("first response".into())],
        };
        let steering_content = vec![Content::text("change direction".into())];
        let steering_json = serde_json::to_string(&steering_content)?;
        manager.save_message(
            &session_id,
            "user",
            &serde_json::to_string(&initial_user.content)?,
        )?;
        manager.queue_pending_steering(&session_id, "steer-1", &steering_json)?;
        manager.save_message(
            &session_id,
            "assistant",
            &serde_json::to_string(&assistant.content)?,
        )?;

        let (input_tx, input_rx) = mpsc::unbounded_channel();
        for _ in 0..2 {
            input_tx.send(LoopInput::Steer {
                pending_id: Some("steer-1".into()),
                content: steering_content.clone(),
            })?;
        }
        let mut inbox = LoopInputInbox::new(input_rx);
        inbox.collect_ready();
        let mut conversation = vec![initial_user, assistant];
        let mut ledger = ContextLedger::from_conversation(&conversation);

        let injected = inject_pending_steering(
            &mut inbox,
            &mut conversation,
            &mut ledger,
            &db_path,
            &session_id,
        );

        assert_eq!(
            injected.len(),
            1,
            "duplicate channel delivery must be idempotent"
        );
        assert!(no_tool_completion_should_continue(&injected));
        assert_eq!(conversation.len(), 3);
        let stored = manager.load_session_messages(&session_id)?;
        assert_eq!(
            stored
                .iter()
                .map(|(role, _)| role.as_str())
                .collect::<Vec<_>>(),
            vec!["user", "assistant", "user"],
            "promotion must place steering after the completed assistant response"
        );
        assert_eq!(
            stored
                .iter()
                .filter(|(_, content)| content.contains("change direction"))
                .count(),
            1,
            "steering must persist exactly once"
        );
        Ok(())
    }

    #[test]
    fn externally_persisted_user_message_is_injected_without_duplicate_history(
    ) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let db_path = temp.path().join("krusty.db");
        let manager = SessionManager::new(Database::new(&db_path)?);
        let session_id = manager.create_session("persisted input", Some("test-model"), None)?;
        let initial_user = ModelMessage {
            role: Role::User,
            content: vec![Content::text("start".into())],
        };
        let follow_up = vec![Content::text("already committed by daemon".into())];
        manager.save_message(
            &session_id,
            "user",
            &serde_json::to_string(&initial_user.content)?,
        )?;
        manager.save_message(&session_id, "user", &serde_json::to_string(&follow_up)?)?;

        let (input_tx, input_rx) = mpsc::unbounded_channel();
        input_tx.send(LoopInput::PersistedUserMessage {
            content: follow_up.clone(),
        })?;
        let mut inbox = LoopInputInbox::new(input_rx);
        inbox.collect_ready();
        let mut conversation = vec![initial_user];
        let mut ledger = ContextLedger::from_conversation(&conversation);

        let injected = inject_pending_steering(
            &mut inbox,
            &mut conversation,
            &mut ledger,
            &db_path,
            &session_id,
        );

        assert_eq!(injected.len(), 1);
        assert_eq!(conversation.len(), 2);
        assert_eq!(
            serde_json::to_value(&conversation[1].content)?,
            serde_json::to_value(&follow_up)?
        );
        let stored = manager.load_session_messages(&session_id)?;
        assert_eq!(stored.len(), 2, "live injection must not save a third row");
        assert_eq!(
            stored
                .iter()
                .filter(|(_, content)| content.contains("already committed by daemon"))
                .count(),
            1,
            "the daemon-owned canonical message must remain exactly once"
        );
        Ok(())
    }

    #[test]
    fn no_tool_completion_finishes_only_without_queued_steering() {
        assert!(!no_tool_completion_should_continue(&[]));
        assert!(no_tool_completion_should_continue(&[
            super::InjectedSteering {
                pending_id: Some("steer-1".into()),
                message: "keep going".into(),
            }
        ]));
    }

    #[test]
    fn validation_cue_expires_without_claiming_required_checks_passed() {
        let mutation = AiToolCall {
            id: "edit".into(),
            name: "edit".into(),
            arguments: json!({"file_path": "app.py"}),
        };
        let changed = Content::ToolResult {
            tool_use_id: "edit".into(),
            output: json!({"ok": true, "changed": true}),
            is_error: None,
        };
        let obligation = ModelMessage {
            role: Role::User,
            content: vec![Content::text(
                "Run the required tests; native acceptance remains pending.".into(),
            )],
        };
        let mut conversation = vec![obligation.clone()];
        refresh_validation_reminder(
            &mut conversation,
            SessionType::Code,
            std::slice::from_ref(&mutation),
            std::slice::from_ref(&changed),
        );
        assert_eq!(conversation.len(), 2);

        // A custom inspection need not match a test-command keyword. Its result
        // consumes the cue, but neither alters the obligation nor claims success.
        let inspection = AiToolCall {
            id: "inspect".into(),
            name: "bash".into(),
            arguments: json!({"command": "python3 -c 'print(open(\"app.py\", \"rb\").read())'"}),
        };
        let failed = Content::ToolResult {
            tool_use_id: "inspect".into(),
            output: json!({"error": "inspection failed"}),
            is_error: Some(true),
        };
        conversation.push(ModelMessage {
            role: Role::User,
            content: vec![failed.clone()],
        });
        refresh_validation_reminder(
            &mut conversation,
            SessionType::Code,
            &[inspection],
            &[failed],
        );
        assert_eq!(conversation.len(), 2);
        assert_eq!(
            serde_json::to_value(&conversation[0]).unwrap(),
            serde_json::to_value(obligation).unwrap()
        );
        assert!(matches!(
            &conversation[1].content[0],
            Content::ToolResult {
                is_error: Some(true),
                ..
            }
        ));

        // A subsequent real change independently earns another one-response cue.
        refresh_validation_reminder(
            &mut conversation,
            SessionType::Code,
            &[mutation],
            &[changed],
        );
        assert_eq!(conversation.len(), 3);
    }

    #[test]
    fn validation_cue_requires_a_successful_proven_mutation() {
        let call = AiToolCall {
            id: "edit".into(),
            name: "edit".into(),
            arguments: json!({"file_path": "app.py"}),
        };
        for (output, is_error) in [
            (json!({"ok": true}), None),
            (json!({"changed": false}), None),
            (json!({"changed": true}), Some(true)),
        ] {
            let mut conversation = vec![];
            refresh_validation_reminder(
                &mut conversation,
                SessionType::Code,
                std::slice::from_ref(&call),
                &[Content::ToolResult {
                    tool_use_id: call.id.clone(),
                    output,
                    is_error,
                }],
            );
            assert!(conversation.is_empty());
        }
    }

    #[test]
    fn successful_validation_removes_only_transient_validation_reminders() {
        let mut conversation = vec![
            ModelMessage {
                role: Role::System,
                content: vec![Content::text("durable instruction".into())],
            },
            ModelMessage {
                role: Role::System,
                content: vec![Content::text(VALIDATION_REMINDER.into())],
            },
        ];

        remove_validation_reminders(&mut conversation);

        assert_eq!(conversation.len(), 1);
        assert!(matches!(
            &conversation[0].content[0],
            Content::Text { text, .. } if text == "durable instruction"
        ));
    }

    #[test]
    fn chatgpt_codex_runtime_uses_conservative_context_window() {
        assert_eq!(effective_context_window_for_runtime(true, 400_000), 256_000);
        assert_eq!(effective_context_window_for_runtime(true, 128_000), 128_000);
        assert_eq!(
            effective_context_window_for_runtime(false, 400_000),
            400_000
        );
    }

    #[test]
    fn empty_stream_idle_retries_once_before_recovery() {
        assert!(should_retry_empty_stream_interruption(
            Some(&LoopStopReason::StreamIdleTimeout),
            None,
            false,
            false
        ));
        assert!(!should_retry_empty_stream_interruption(
            Some(&LoopStopReason::StreamIdleTimeout),
            None,
            true,
            false
        ));
        assert!(!should_retry_empty_stream_interruption(
            Some(&LoopStopReason::StreamIdleTimeout),
            None,
            false,
            true
        ));
        assert!(should_retry_empty_stream_interruption(
            Some(&LoopStopReason::ProviderError),
            Some(&super::stream::StreamFailure::RetryableTransport(
                "API error: 429 Too Many Requests - capacity".to_string()
            )),
            false,
            false
        ));
        assert!(should_retry_empty_stream_interruption(
            Some(&LoopStopReason::ProviderError),
            Some(&super::stream::StreamFailure::RetryableTransport(
                "AI stream ended without a finish signal".to_string()
            )),
            false,
            false
        ));
        assert!(!should_retry_empty_stream_interruption(
            Some(&LoopStopReason::ProviderError),
            Some(&super::stream::StreamFailure::Terminal(
                "API error: 402 Payment Required - limit reached".to_string()
            )),
            false,
            false
        ));
        assert!(!should_retry_empty_stream_interruption(
            Some(&LoopStopReason::ProviderError),
            Some(&super::stream::StreamFailure::RetryableTransport(
                "API error: 503 Service Unavailable".to_string()
            )),
            true,
            false
        ));
    }

    #[test]
    fn semantic_empty_completion_retries_once_then_fails_visibly() {
        assert_eq!(
            empty_completion_action("", &[], false, false, false),
            EmptyCompletionAction::Retry
        );
        assert_eq!(
            empty_completion_action("  \n", &[], false, true, false),
            EmptyCompletionAction::Fail
        );
        assert_eq!(
            empty_completion_action("", &[], false, false, true),
            EmptyCompletionAction::Fail
        );
    }

    #[test]
    fn semantic_completion_with_visible_text_or_tool_call_needs_no_recovery() {
        let tool_call = AiToolCall {
            id: "call-1".into(),
            name: "bash".into(),
            arguments: json!({"command": "true"}),
        };

        assert_eq!(
            empty_completion_action("done", &[], false, false, false),
            EmptyCompletionAction::None
        );
        assert_eq!(
            empty_completion_action("", &[tool_call], false, false, false),
            EmptyCompletionAction::None
        );
    }

    #[test]
    fn loop_guard_landing_request_has_no_tool_surface() {
        let options = CallOptions {
            tools: Some(vec![AiTool {
                name: "read".to_string(),
                description: "read a file".to_string(),
                input_schema: json!({"type": "object"}),
                prompt: None,
            }]),
            codex_parallel_tool_calls: true,
            ..CallOptions::default()
        };

        let normal = provider_options_for_turn(&options, false);
        assert_eq!(normal.tools.as_ref().map(Vec::len), Some(1));
        assert!(normal.codex_parallel_tool_calls);

        let landing = provider_options_for_turn(&options, true);
        assert!(landing.tools.is_none());
        assert!(landing.web_search.is_none());
        assert!(landing.web_fetch.is_none());
        assert!(!landing.codex_parallel_tool_calls);
        assert_eq!(landing.parallel_tool_calls, Some(false));
        assert_eq!(options.tools.as_ref().map(Vec::len), Some(1));

        let instruction = super::loop_guard_landing_instruction(&LoopGuardLanding {
            diagnostic: "same observation repeated".to_string(),
            block_goal: true,
        });
        assert!(instruction.contains("one bounded synthesis turn"));
        assert!(instruction.contains("No tools are available"));
    }

    #[test]
    fn multiple_ask_user_calls_create_one_pending_call_and_reject_the_rest() {
        let first = AiToolCall {
            id: "ask-1".into(),
            name: "AskUserQuestion".into(),
            arguments: json!({"questions": [{"question": "First?"}]}),
        };
        let second = AiToolCall {
            id: "ask-2".into(),
            name: "AskUserQuestion".into(),
            arguments: json!({"questions": [{"question": "Second?"}]}),
        };
        let calls = vec![&first, &second];

        let (pending, rejected) = split_single_pending_ask_user_call(&calls)
            .expect("two AskUser calls should still select one primary");

        assert_eq!(pending.id, "ask-1");
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].id, "ask-2");
    }

    #[test]
    fn stream_idle_timeout_is_terminal_but_not_active_processing() {
        assert_eq!(
            terminal_agent_state_after_interruption(&LoopStopReason::StreamIdleTimeout),
            "idle"
        );
        assert_eq!(
            terminal_agent_state_after_interruption(&LoopStopReason::ProviderError),
            "error"
        );
    }

    #[test]
    fn project_settings_cannot_elevate_supervised_permission_mode() {
        let settings = ProjectSettings {
            permission_mode: Some("autonomous".to_string()),
            ..Default::default()
        };

        let resolved = resolve_project_permission_mode(PermissionMode::Supervised, &settings);

        assert_eq!(resolved, PermissionMode::Supervised);
    }

    #[test]
    fn project_settings_can_restrict_autonomous_permission_mode() {
        let settings = ProjectSettings {
            permission_mode: Some("supervised".to_string()),
            ..Default::default()
        };

        let resolved = resolve_project_permission_mode(PermissionMode::Autonomous, &settings);

        assert_eq!(resolved, PermissionMode::Supervised);
    }

    #[test]
    fn project_settings_preserve_requested_autonomous_permission_mode() {
        let settings = ProjectSettings {
            permission_mode: Some("autonomous".to_string()),
            ..Default::default()
        };

        let resolved = resolve_project_permission_mode(PermissionMode::Autonomous, &settings);

        assert_eq!(resolved, PermissionMode::Autonomous);
    }

    #[test]
    fn finalize_explore_only_turn_returns_summary_for_successful_explore() -> anyhow::Result<()> {
        let tool_calls = vec![AiToolCall {
            id: "call-1".to_string(),
            name: "explore".to_string(),
            arguments: json!({}),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "call-1".to_string(),
            output: json!({
                "tool": "explore",
                "result": {
                    "outcome": "success",
                    "usable_agents": 2,
                    "message": "Explore completed: 2 agents",
                    "human_review": "Architecture review completed across 2 targets.\n\nTarget reviews:\n- agent: Owns orchestration.\n- ai: Owns providers.",
                    "investigation_summary": "Delegated exploration gathered usable evidence for dir-0, dir-1.",
                    "confidence": "high",
                    "paths_examined_count": 15
                }
            }),
            is_error: None,
        }];

        let summary = finalize_explore_only_turn(&tool_calls, &tool_results)
            .ok_or_else(|| anyhow::anyhow!("explore should finalize"))?;

        assert!(summary.contains("Architecture review completed across 2 targets."));
        assert!(summary.contains("agent: Owns orchestration."));
        assert!(!summary.contains("Evidence examined: 15 tracked paths/files."));
        Ok(())
    }

    #[test]
    fn finalize_explore_only_turn_skips_failed_or_non_explore_results() {
        let tool_calls = vec![AiToolCall {
            id: "call-1".to_string(),
            name: "explore".to_string(),
            arguments: json!({}),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "call-1".to_string(),
            output: json!({
                "tool": "explore",
                "result": {
                    "outcome": "failed",
                    "usable_agents": 0
                }
            }),
            is_error: None,
        }];

        assert!(finalize_explore_only_turn(&tool_calls, &tool_results).is_none());
    }

    #[test]
    fn inject_runtime_context_applies_hive_session_identity() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let repo = temp.path();
        fs::create_dir_all(repo.join(".git"))?;
        fs::write(repo.join("AGENTS.md"), "repo instructions")?;
        fs::write(repo.join("HIVE.md"), "Always Swimming.")?;

        let skills = RwLock::new(SkillsManager::with_defaults(repo));
        let conversation = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::text("hello".to_string())],
        }];
        let db_path = repo.join("krusty.db");

        let hive_injected = inject_runtime_context(
            &conversation,
            &db_path,
            "session-id",
            repo,
            Some(repo),
            None,
            None,
            None,
            WorkMode::Build,
            &skills,
            None,
            SessionType::Hive,
            None,
        );
        let code_injected = inject_runtime_context(
            &conversation,
            &db_path,
            "session-id",
            repo,
            Some(repo),
            None,
            None,
            None,
            WorkMode::Build,
            &skills,
            None,
            SessionType::Code,
            None,
        );

        assert!(hive_injected.iter().any(|message| {
            matches!(
                &message.content[0],
                Content::Text { text, .. }
                    if text.contains("[HIVE PROJECT OVERLAY - HIVE.md]") && text.contains("Always Swimming.")
            )
        }));
        assert!(!code_injected.iter().any(|message| {
            matches!(
                &message.content[0],
                Content::Text { text, .. } if text.contains("[HIVE PROJECT OVERLAY - HIVE.md]")
            )
        }));
        Ok(())
    }
    #[test]
    fn sources_only_completion_is_meaningful_output() {
        assert_eq!(
            empty_completion_action("", &[], true, false, false),
            EmptyCompletionAction::None
        );
    }
}
