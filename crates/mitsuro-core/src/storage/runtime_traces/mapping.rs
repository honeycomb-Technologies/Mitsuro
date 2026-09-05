use serde_json::{json, Value};

use super::model::TraceFailureCategory;
use crate::agent::loop_events::{LoopEvent, LoopStopReason};

pub(super) fn loop_event_type(event: &LoopEvent) -> &'static str {
    match event {
        LoopEvent::TextDelta { .. } => "text_delta",
        LoopEvent::TextDeltaWithCitations { .. } => "text_delta_with_citations",
        LoopEvent::ThinkingDelta { .. } => "thinking_delta",
        LoopEvent::ThinkingComplete { .. } => "thinking_complete",
        LoopEvent::ToolCallStart { .. } => "tool_call_start",
        LoopEvent::ToolCallPreparing { .. } => "tool_call_preparing",
        LoopEvent::ToolCallComplete { .. } => "tool_call_complete",
        LoopEvent::ToolExecuting { .. } => "tool_executing",
        LoopEvent::ToolOutputDelta { .. } => "tool_output_delta",
        LoopEvent::ToolResult { .. } => "tool_result",
        LoopEvent::AwaitingInput { .. } => "awaiting_input",
        LoopEvent::ToolApprovalRequired { .. } => "tool_approval_required",
        LoopEvent::ToolApproved { .. } => "tool_approved",
        LoopEvent::ToolDenied { .. } => "tool_denied",
        LoopEvent::SteeringInjected { .. } => "steering_injected",
        LoopEvent::ServerToolStart { .. } => "server_tool_start",
        LoopEvent::ServerToolComplete { .. } => "server_tool_complete",
        LoopEvent::WebSearchResults { .. } => "web_search_results",
        LoopEvent::WebFetchResult { .. } => "web_fetch_result",
        LoopEvent::ServerToolError { .. } => "server_tool_error",
        LoopEvent::ModeChange { .. } => "mode_change",
        LoopEvent::PlanUpdate { .. } => "plan_update",
        LoopEvent::WorkflowUpdated { .. } => "workflow_updated",
        LoopEvent::PlanComplete { .. } => "plan_complete",
        LoopEvent::AgentSleeping { .. } => "agent_sleeping",
        LoopEvent::TurnComplete { .. } => "turn_complete",
        LoopEvent::RunBudgetResolved { .. } => "run_budget_resolved",
        LoopEvent::ProviderRequestPrepared { .. } => "provider_request_prepared",
        LoopEvent::MicrocompactionApplied { .. } => "microcompaction_applied",
        LoopEvent::ProgressGuard { .. } => "progress_guard",
        LoopEvent::TickInjected { .. } => "tick_injected",
        LoopEvent::Usage { .. } => "usage",
        LoopEvent::SessionPinched { .. } => "session_pinched",
        LoopEvent::ContextCompactionStarted { .. } => "context_compaction_started",
        LoopEvent::ContextCompacted { .. } => "context_compacted",
        LoopEvent::TitleGenerated { .. } => "title_generated",
        LoopEvent::Finished { .. } => "finished",
        LoopEvent::Error { .. } => "error",
        LoopEvent::AgentBackgroundStarted { .. } => "agent_background_started",
        LoopEvent::AgentBackgroundCompleted { .. } => "agent_background_completed",
        LoopEvent::UserMessage { .. } => "user_message",
        LoopEvent::ClassifierDecision { .. } => "classifier_decision",
    }
}

pub(super) fn summarize_loop_event(event: &LoopEvent) -> Value {
    match event {
        LoopEvent::TextDelta { delta } => json!({ "chars": delta.chars().count() }),
        LoopEvent::TextDeltaWithCitations { delta, citations } => {
            json!({ "chars": delta.chars().count(), "citations": citations.len() })
        }
        LoopEvent::ThinkingDelta { thinking } => json!({ "chars": thinking.chars().count() }),
        LoopEvent::ThinkingComplete { thinking, .. } => {
            json!({ "chars": thinking.chars().count() })
        }
        LoopEvent::ToolCallStart { id, name } => json!({ "id": id, "name": name }),
        LoopEvent::ToolCallPreparing {
            id,
            name,
            received_bytes,
        } => json!({ "id": id, "name": name, "received_bytes": received_bytes }),
        LoopEvent::ToolCallComplete {
            id,
            name,
            arguments,
        } => json!({
            "id": id,
            "name": name,
            "arguments": summarize_json_shape(arguments),
        }),
        LoopEvent::ToolExecuting { id, name } => json!({ "id": id, "name": name }),
        LoopEvent::ToolOutputDelta { id, delta } => {
            json!({ "id": id, "chars": delta.chars().count() })
        }
        LoopEvent::ToolResult {
            id,
            output,
            is_error,
        } => json!({
            "id": id,
            "is_error": is_error,
            "output_chars": output.chars().count(),
        }),
        LoopEvent::AwaitingInput {
            tool_call_id,
            tool_name,
        } => json!({ "tool_call_id": tool_call_id, "tool_name": tool_name }),
        LoopEvent::ToolApprovalRequired {
            id,
            name,
            arguments,
        } => json!({
            "id": id,
            "name": name,
            "arguments": summarize_json_shape(arguments),
        }),
        LoopEvent::ToolApproved { id } => json!({ "id": id }),
        LoopEvent::ToolDenied { id } => json!({ "id": id }),
        LoopEvent::ServerToolStart { id, name } => json!({ "id": id, "name": name }),
        LoopEvent::ServerToolComplete { id, name } => json!({ "id": id, "name": name }),
        LoopEvent::WebSearchResults {
            tool_use_id,
            results,
        } => {
            json!({ "tool_use_id": tool_use_id, "result_count": results.len() })
        }
        LoopEvent::WebFetchResult {
            tool_use_id,
            content,
        } => json!({
            "tool_use_id": tool_use_id,
            "url": content.url,
            "media_type": content.media_type,
            "content_chars": content.content.chars().count(),
        }),
        LoopEvent::ServerToolError {
            tool_use_id,
            error_code,
        } => json!({ "tool_use_id": tool_use_id, "error_code": error_code }),
        LoopEvent::ModeChange { mode, reason } => json!({ "mode": mode, "reason": reason }),
        LoopEvent::PlanUpdate { tasks } => json!({
            "task_count": tasks.len(),
            "completed_count": tasks.iter().filter(|task| task.completed).count(),
        }),
        LoopEvent::WorkflowUpdated {
            goal_id,
            aggregate_revision,
            operation_id,
        } => json!({
            "goal_id": goal_id,
            "aggregate_revision": aggregate_revision,
            "operation_id": operation_id,
        }),
        LoopEvent::PlanComplete {
            tool_call_id,
            title,
            task_count,
        } => json!({
            "tool_call_id": tool_call_id,
            "title": title,
            "task_count": task_count,
        }),
        LoopEvent::AgentSleeping {
            duration_secs,
            reason,
        } => json!({
            "duration_secs": duration_secs,
            "reason": reason,
        }),
        LoopEvent::TurnComplete { turn, has_more } => {
            json!({ "turn": turn, "has_more": has_more })
        }
        LoopEvent::RunBudgetResolved { max_turns, source } => json!({
            "max_turns": max_turns,
            "source": source,
        }),
        LoopEvent::ProviderRequestPrepared { turn, diagnostics } => json!({
            "turn": turn,
            "diagnostics": diagnostics,
        }),
        LoopEvent::MicrocompactionApplied {
            turn,
            generation,
            message_count,
            history_rewritten,
            tool_inputs_rewritten,
        } => json!({
            "turn": turn,
            "generation": generation,
            "message_count": message_count,
            "history_rewritten": history_rewritten,
            "tool_inputs_rewritten": tool_inputs_rewritten,
        }),
        LoopEvent::ProgressGuard { telemetry } => json!({
            "guard": telemetry.guard,
            "action": telemetry.action,
            "no_progress_turns": telemetry.no_progress_turns,
            "threshold": telemetry.threshold,
            "mutation_epoch": telemetry.mutation_epoch,
            "action_classes": telemetry.action_classes,
            "evidence_signature": telemetry.evidence_signature,
            "triggered": telemetry.triggered,
        }),
        LoopEvent::TickInjected { tick_number } => json!({ "tick_number": tick_number }),
        LoopEvent::Usage {
            prompt_tokens,
            input_tokens,
            completion_tokens,
            reasoning_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            total_tokens,
        } => json!({
            "prompt_tokens": prompt_tokens,
            "input_tokens": input_tokens,
            "completion_tokens": completion_tokens,
            "reasoning_tokens": reasoning_tokens,
            "cache_creation_input_tokens": cache_creation_input_tokens,
            "cache_read_input_tokens": cache_read_input_tokens,
            "total_tokens": total_tokens,
        }),
        LoopEvent::SessionPinched {
            reason,
            source_session_id,
            new_session_id,
            estimated_tokens_before,
        } => json!({
            "reason": reason,
            "source_session_id": source_session_id,
            "new_session_id": new_session_id,
            "estimated_tokens_before": estimated_tokens_before,
        }),
        LoopEvent::ContextCompactionStarted { reason } => json!({ "reason": reason }),
        LoopEvent::ContextCompacted {
            reason,
            estimated_tokens_before,
            estimated_tokens_after,
            replaced_messages,
            checkpoint_id,
            compaction_count,
        } => json!({
            "reason": reason,
            "estimated_tokens_before": estimated_tokens_before,
            "estimated_tokens_after": estimated_tokens_after,
            "replaced_messages": replaced_messages,
            "checkpoint_id": checkpoint_id,
            "compaction_count": compaction_count,
        }),
        LoopEvent::TitleGenerated { title } => json!({ "title": title }),
        LoopEvent::Finished {
            session_id,
            stop_reason,
        } => json!({ "session_id": session_id, "stop_reason": stop_reason }),
        LoopEvent::Error { error } => json!({ "error": error }),
        LoopEvent::AgentBackgroundStarted {
            delegated_run_id,
            agent_type,
            description,
        } => json!({
            "delegated_run_id": delegated_run_id,
            "agent_type": agent_type,
            "description": description,
        }),
        LoopEvent::AgentBackgroundCompleted {
            delegated_run_id,
            agent_type,
            success,
            summary,
        } => json!({
            "delegated_run_id": delegated_run_id,
            "agent_type": agent_type,
            "success": success,
            "summary": summary,
        }),
        LoopEvent::UserMessage {
            title,
            message,
            level,
        } => json!({ "title": title, "message": message, "level": level }),
        LoopEvent::SteeringInjected {
            pending_id,
            message,
        } => json!({ "pending_id": pending_id, "chars": message.chars().count() }),
        LoopEvent::ClassifierDecision {
            tool_name,
            decision,
            reason,
            stage,
        } => {
            json!({ "tool_name": tool_name, "decision": decision, "reason": reason, "stage": stage })
        }
    }
}

fn summarize_json_shape(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            json!({ "type": "object", "keys": keys })
        }
        Value::Array(items) => json!({ "type": "array", "len": items.len() }),
        Value::String(_) => json!({ "type": "string" }),
        Value::Number(_) => json!({ "type": "number" }),
        Value::Bool(_) => json!({ "type": "bool" }),
        Value::Null => json!({ "type": "null" }),
    }
}

pub(super) fn failure_category_for_event(event: &LoopEvent) -> Option<TraceFailureCategory> {
    match event {
        LoopEvent::ToolResult { is_error: true, .. } => {
            Some(TraceFailureCategory::ToolExecutionError)
        }
        LoopEvent::ToolDenied { .. } => Some(TraceFailureCategory::ToolDenied),
        LoopEvent::ServerToolError { .. } => Some(TraceFailureCategory::ServerToolError),
        LoopEvent::Finished { stop_reason, .. } => match stop_reason {
            LoopStopReason::Completed
            | LoopStopReason::AwaitingInput
            | LoopStopReason::Sleeping => None,
            LoopStopReason::ProviderError => Some(TraceFailureCategory::ProviderError),
            LoopStopReason::BudgetExhausted => Some(TraceFailureCategory::BudgetExhausted),
            LoopStopReason::LoopGuardTriggered => Some(TraceFailureCategory::LoopGuardTriggered),
            LoopStopReason::StreamIdleTimeout => Some(TraceFailureCategory::StreamIdleTimeout),
            LoopStopReason::UserAbort => Some(TraceFailureCategory::UserAbort),
            LoopStopReason::Pinched => None,
            LoopStopReason::PinchFailed => Some(TraceFailureCategory::PinchFailed),
        },
        LoopEvent::Error { .. } => Some(TraceFailureCategory::AgentError),
        _ => None,
    }
}

pub(super) fn stop_reason_for_event(event: &LoopEvent) -> Option<LoopStopReason> {
    match event {
        LoopEvent::Finished { stop_reason, .. } => Some(stop_reason.clone()),
        _ => None,
    }
}
