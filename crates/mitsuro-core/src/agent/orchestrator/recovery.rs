use std::collections::HashSet;

use crate::agent::context_ledger::{ContextLedger, ContinuationDecision, NonResumableReason};
use crate::agent::stream;
use crate::storage::{
    PartialAssistantState, PendingInteractionSnapshot, RecoveryDecision,
    RecoveryNonResumableReason, RecoveryStatus, RecoveryToolCall, SessionRecoveryState,
};
use crate::tools::registry::PermissionMode;

use super::super::loop_events::LoopStopReason;

pub(super) fn continuation_recovery_message(ledger: &ContextLedger) -> String {
    match ledger.continuation_decision() {
        ContinuationDecision::Resumable {
            latest_user_objective,
        } => format!(
            "Continuation contract: resumable. Preserved objective: {}",
            latest_user_objective
        ),
        ContinuationDecision::NonResumable { reason } => format!(
            "Continuation contract: non-resumable ({:?}). Start a fresh turn with explicit user intent.",
            reason
        ),
    }
}

pub(super) fn build_partial_assistant_state(
    checkpoint: &stream::StreamCheckpoint,
) -> PartialAssistantState {
    PartialAssistantState {
        text: checkpoint.text.clone(),
        citations: checkpoint.citations.clone(),
        thinking: checkpoint.thinking.clone(),
        tool_calls: checkpoint
            .tool_calls
            .iter()
            .map(|tool| RecoveryToolCall::from_call_parts(&tool.id, &tool.name, &tool.arguments))
            .collect(),
    }
}

pub(super) fn build_recovery_state(
    ledger: &ContextLedger,
    status: RecoveryStatus,
    stop_reason: Option<LoopStopReason>,
    last_error: Option<String>,
    partial_assistant: PartialAssistantState,
) -> SessionRecoveryState {
    SessionRecoveryState::new(
        status.clone(),
        stop_reason,
        last_error,
        partial_assistant.clone(),
        recovery_decision(ledger, &status, &partial_assistant),
    )
}

pub(super) fn build_awaiting_input_recovery_state(
    partial_assistant: PartialAssistantState,
    pending_interactions: Vec<PendingInteractionSnapshot>,
    permission_mode: PermissionMode,
    execution_tool_allowlist: Option<&HashSet<String>>,
) -> SessionRecoveryState {
    SessionRecoveryState::new_with_pending_interactions(
        RecoveryStatus::AwaitingInput,
        Some(LoopStopReason::AwaitingInput),
        None,
        partial_assistant,
        pending_interactions,
        RecoveryDecision::NonResumable {
            reason: RecoveryNonResumableReason::AwaitingHumanInput,
        },
    )
    .with_permission_mode(permission_mode)
    .with_execution_tool_allowlist(execution_tool_allowlist)
}

fn recovery_decision(
    ledger: &ContextLedger,
    status: &RecoveryStatus,
    partial_assistant: &PartialAssistantState,
) -> RecoveryDecision {
    let override_reason = match status {
        RecoveryStatus::ToolExecuting => Some(RecoveryNonResumableReason::ToolExecutionInProgress),
        RecoveryStatus::AwaitingInput => Some(RecoveryNonResumableReason::AwaitingHumanInput),
        RecoveryStatus::Streaming | RecoveryStatus::Interrupted
            if !partial_assistant.tool_calls.is_empty() =>
        {
            Some(RecoveryNonResumableReason::PendingToolCall)
        }
        _ => None,
    };

    if let Some(reason) = override_reason {
        return RecoveryDecision::NonResumable { reason };
    }

    match ledger.continuation_decision() {
        ContinuationDecision::Resumable {
            latest_user_objective,
        } => RecoveryDecision::Resumable {
            latest_user_objective,
        },
        ContinuationDecision::NonResumable { reason } => RecoveryDecision::NonResumable {
            reason: match reason {
                NonResumableReason::MissingUserObjective => {
                    RecoveryNonResumableReason::MissingUserObjective
                }
                NonResumableReason::EmptyConversation => {
                    RecoveryNonResumableReason::EmptyConversation
                }
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::types::{Content, ModelMessage, Role};
    use serde_json::json;

    fn ledger_with_objective(objective: &str) -> ContextLedger {
        ContextLedger::from_conversation(&[ModelMessage {
            role: Role::User,
            content: vec![Content::text(objective.to_string())],
        }])
    }

    #[test]
    fn orchestrator_awaiting_ask_user_persists_reconstructable_pending_input() {
        let arguments = json!({
            "questions": [{
                "header": "Scope",
                "question": "Should I update storage only or include server wiring?",
                "options": [
                    {"label": "Storage only", "description": "Keep this card focused"},
                    {"label": "Include server", "description": "Broader follow-up work"}
                ],
                "multiSelect": false
            }]
        });
        let partial = PartialAssistantState {
            citations: Vec::new(),
            text: "I need one decision.".to_string(),
            thinking: String::new(),
            tool_calls: vec![RecoveryToolCall::from_call_parts(
                "ask-1",
                "AskUserQuestion",
                &arguments,
            )],
        };

        let state = build_awaiting_input_recovery_state(
            partial,
            vec![PendingInteractionSnapshot::ask_user_from_call(
                "ask-1", &arguments,
            )],
            PermissionMode::Supervised,
            Some(&HashSet::from(["AskUserQuestion".to_string()])),
        );

        assert_eq!(state.status, RecoveryStatus::AwaitingInput);
        assert_eq!(
            state.decision,
            RecoveryDecision::NonResumable {
                reason: RecoveryNonResumableReason::AwaitingHumanInput
            }
        );
        assert_eq!(state.permission_mode, Some(PermissionMode::Supervised));
        assert_eq!(
            state.execution_tool_allowlist,
            Some(vec!["AskUserQuestion".to_string()])
        );
        assert_eq!(state.pending_interactions.len(), 1);
        match &state.pending_interactions[0] {
            PendingInteractionSnapshot::AskUserQuestion {
                tool_call_id,
                questions,
            } => {
                assert_eq!(tool_call_id, "ask-1");
                assert_eq!(questions[0].header, "Scope");
                assert_eq!(
                    questions[0].question,
                    "Should I update storage only or include server wiring?"
                );
                assert_eq!(questions[0].options[0].label, "Storage only");
            }
            other => panic!("unexpected pending interaction: {other:?}"),
        }
    }

    #[test]
    fn get_build_recovery_state_marks_tool_executing_and_partial_tool_calls_non_resumable() {
        let ledger = ledger_with_objective("finish durable recovery");
        let tool_executing = build_recovery_state(
            &ledger,
            RecoveryStatus::ToolExecuting,
            None,
            None,
            PartialAssistantState::default(),
        );
        assert_eq!(
            tool_executing.decision,
            RecoveryDecision::NonResumable {
                reason: RecoveryNonResumableReason::ToolExecutionInProgress
            }
        );

        let partial_tool_call = build_recovery_state(
            &ledger,
            RecoveryStatus::Streaming,
            None,
            None,
            PartialAssistantState {
                citations: Vec::new(),
                text: String::new(),
                thinking: String::new(),
                tool_calls: vec![RecoveryToolCall::summary("partial-1", "bash")],
            },
        );
        assert_eq!(
            partial_tool_call.decision,
            RecoveryDecision::NonResumable {
                reason: RecoveryNonResumableReason::PendingToolCall
            }
        );
    }
}
