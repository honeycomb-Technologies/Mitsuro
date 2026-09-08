use std::collections::HashSet;
use std::time::Instant;

use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::agent::compaction::is_context_overflow_error;
use crate::agent::constants::subagent;
use crate::agent::history_policy::build_history_tool_result;
use crate::agent::progress::LoopGuard;
use crate::agent::RunProvenance;
use crate::ai::client::AiClient;
use crate::ai::types::{AiTool, AiToolCall, Content, ModelMessage, Role};
use crate::tools::registry::{effective_tool_call, tool_policy_for_call, ToolCategory};
use crate::tools::ToolResult;

use super::super::lifecycle::AgentMailboxFinish;
use super::super::types::{
    missing_required_browser_acceptance_proofs, parse_delegated_handoff, parse_explore_report,
    summary_looks_non_substantive, synthesize_explore_report, AgentConversationEvent,
    AgentConversationToolCall, AgentProgress, AgentProgressStatus, DelegatedEvidenceKind,
    DelegatedEvidenceSummary, DelegatedProcessArtifact, SubAgentResult, SubAgentTask,
    SubAgentTermination, TaskObjectiveStatus,
};
use super::api::{call_subagent_api, parse_response, parse_response_usage};
use super::config::AgentConfig;
use super::explorer::{
    collect_paths_from_tool_result, completion_summary_preview, normalize_explorer_result,
    relative_or_display, synthesized_explorer_output, text_claims_tool_empty,
    timeout_partial_output, tool_result_has_positive_evidence,
};
use super::governance::{build_subagent_tool_context, delegated_is_explore, delegated_turn_budget};

const MAX_DELEGATED_POLICY_VIOLATIONS: usize = 3;
const EXPLORER_STALE_SEQUENCE_THRESHOLD: usize = 3;
const EXPLORER_SYNTHESIS_FILE_THRESHOLD: usize = 8;
const TURN_BUDGET_LANDING_INSTRUCTION: &str = "[TURN BUDGET LANDING]\nThis is the final provider turn reserved inside the delegated turn budget. No tools are available. Using only canonical evidence already gathered, give the parent a concise, truthful handoff now. State completed work, verification actually performed, and any unresolved gap. Do not request or describe another tool call, and do not claim unverified work succeeded.";
const LOOP_GUARD_LANDING_FALLBACK: &str = crate::agent::loop_kernel::LANDING_FALLBACK_PARENT_AGENT;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvidenceCompletionDecision {
    Ready,
    RequestEvidence,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaxTokensLandingDecision {
    NotApplicable,
    RetryConciseLanding,
    TerminalIncomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForcedSummaryToolDecision {
    NotApplicable,
    AllowGapClosingCycle,
    ForceSummary,
}

fn forced_summary_tool_decision(
    summary_requested: bool,
    gap_closing_cycle_used: bool,
    all_read_only_tools: bool,
) -> ForcedSummaryToolDecision {
    if !summary_requested || !all_read_only_tools {
        ForcedSummaryToolDecision::NotApplicable
    } else if !gap_closing_cycle_used {
        ForcedSummaryToolDecision::AllowGapClosingCycle
    } else {
        ForcedSummaryToolDecision::ForceSummary
    }
}

fn child_conversation_display_text(output: &str) -> String {
    let mut visible = output.trim_end();
    if visible.ends_with("</delegated_handoff>") {
        if let Some(start) = visible.rfind("<delegated_handoff>") {
            visible = visible[..start].trim_end();
        }
    }
    if visible.ends_with("</explore_report>") {
        if let Some(start) = visible.rfind("<explore_report>") {
            visible = visible[..start].trim_end();
        }
    }
    if !visible.trim().is_empty() {
        return visible.to_string();
    }
    parse_delegated_handoff(output)
        .map(|handoff| handoff.summary.trim().to_string())
        .unwrap_or_default()
}

fn terminal_progress_status(result: &SubAgentResult) -> AgentProgressStatus {
    match result.objective_status() {
        TaskObjectiveStatus::Complete => AgentProgressStatus::Complete,
        TaskObjectiveStatus::Degraded | TaskObjectiveStatus::Blocked => {
            AgentProgressStatus::Degraded
        }
        TaskObjectiveStatus::Failed => AgentProgressStatus::Failed,
    }
}

fn response_requires_tool_execution(
    tool_calls: &[super::super::types::ToolCall],
    _stop_reason: &str,
) -> bool {
    // Structured calls are executable intent. Provider stop reasons are only
    // meaningful when no call is present; contradictory `end_turn` metadata
    // must never discard a call that already crossed the normalized boundary.
    !tool_calls.is_empty()
}

fn max_tokens_landing_decision(
    tool_calls: &[super::super::types::ToolCall],
    stop_reason: &str,
    retry_attempted: bool,
    turn_available: bool,
) -> MaxTokensLandingDecision {
    if !tool_calls.is_empty() || stop_reason != "max_tokens" {
        MaxTokensLandingDecision::NotApplicable
    } else if !retry_attempted && turn_available {
        MaxTokensLandingDecision::RetryConciseLanding
    } else {
        MaxTokensLandingDecision::TerminalIncomplete
    }
}

fn turn_budget_landing_due(
    completed_turns: usize,
    max_turns: Option<usize>,
    has_canonical_evidence: bool,
    another_landing_active: bool,
) -> bool {
    !another_landing_active
        && has_canonical_evidence
        && max_turns.is_some_and(|max_turns| {
            max_turns > 1 && completed_turns.saturating_add(1) == max_turns
        })
}

fn completed_report_at_turn_budget(
    task: &SubAgentTask,
    evidence: &DelegatedEvidenceSummary,
    output: &str,
    paths_examined: &[String],
) -> bool {
    delegated_is_explore(task)
        && evidence.has_canonical_evidence()
        && (parse_explore_report(output).is_some()
            || (!summary_looks_non_substantive(output)
                && synthesize_explore_report(output, paths_examined).is_some()))
}

fn evidence_completion_decision(
    evidence: &DelegatedEvidenceSummary,
    correction_requested: bool,
    has_effective_tools: bool,
) -> EvidenceCompletionDecision {
    if evidence.has_canonical_evidence() {
        EvidenceCompletionDecision::Ready
    } else if correction_requested || !has_effective_tools {
        EvidenceCompletionDecision::Reject
    } else {
        EvidenceCompletionDecision::RequestEvidence
    }
}

fn enforce_canonical_evidence(mut result: SubAgentResult, task: &SubAgentTask) -> SubAgentResult {
    result.enforce_acceptance_contract(&task.prompt);
    if result.success && !result.evidence.has_canonical_evidence() {
        result.success = false;
        result.termination = SubAgentTermination::Failed;
        result.error =
            Some("Delegated child completed without canonical tool evidence".to_string());
    }
    result
}

fn loop_guard_landing_instruction(diagnostic: &str) -> String {
    crate::agent::loop_kernel::loop_guard_landing_instruction(
        diagnostic,
        crate::agent::loop_kernel::LandingAudience::ParentAgent,
    )
}

fn loop_guard_landing_output(
    provider_output: &str,
    diagnostic: &str,
    files_examined: &[String],
) -> String {
    let mut output = if provider_output.trim().is_empty() {
        LOOP_GUARD_LANDING_FALLBACK.to_string()
    } else {
        provider_output.trim().to_string()
    };
    if !files_examined.is_empty() && provider_output.trim().is_empty() {
        output.push_str("\n\nEvidence paths retained: ");
        output.push_str(
            &files_examined
                .iter()
                .take(12)
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    output.push_str("\n\n[DELEGATED LOOP GUARD]\n");
    output.push_str(diagnostic.trim());
    output
}

fn tool_surface_for_turn(
    tools: &[AiTool],
    loop_guard_landing: bool,
    max_tokens_landing: bool,
    turn_budget_landing: bool,
    handoff_repair: bool,
) -> &[AiTool] {
    if loop_guard_landing || max_tokens_landing || turn_budget_landing || handoff_repair {
        &[]
    } else {
        tools
    }
}

fn delegated_evidence_kind(
    tool_name: &str,
    input: &serde_json::Value,
) -> Option<DelegatedEvidenceKind> {
    let (effective_name, effective_input) = effective_tool_call(tool_name, input);
    if effective_name == "tool_search" {
        return None;
    }
    if effective_name == "bash" {
        return Some(DelegatedEvidenceKind::Execution);
    }

    match tool_policy_for_call(effective_name, effective_input).category {
        ToolCategory::ReadOnly => Some(DelegatedEvidenceKind::Observation),
        ToolCategory::Write => Some(DelegatedEvidenceKind::Mutation),
        ToolCategory::Interactive => None,
    }
}

fn tool_call_requires_completion_shield(tool_name: &str, input: &serde_json::Value) -> bool {
    crate::agent::loop_kernel::tool_call_requires_completion_shield(tool_name, input)
}

fn append_parent_messages(messages: &mut Vec<ModelMessage>, parent_messages: Vec<String>) {
    for message in parent_messages {
        messages.push(ModelMessage {
            role: Role::User,
            content: vec![Content::text(format!(
                "[PARENT MESSAGE]\n{message}\n[/PARENT MESSAGE]"
            ))],
        });
    }
}

fn append_late_mailbox_continuation(
    messages: &mut Vec<ModelMessage>,
    assistant_text: &[String],
    parent_messages: Vec<String>,
) {
    if !assistant_text.is_empty() {
        messages.push(ModelMessage {
            role: Role::Assistant,
            content: assistant_text
                .iter()
                .map(|text| Content::text(text.clone()))
                .collect(),
        });
    }
    append_parent_messages(messages, parent_messages);
}

fn retain_unconsumed_parent_messages(output: &mut String, parent_messages: Vec<String>) {
    if parent_messages.is_empty() {
        return;
    }

    if !output.trim().is_empty() {
        output.push_str("\n\n");
    }
    output.push_str("[UNCONSUMED PARENT STEERING]\n");
    for message in parent_messages {
        output.push_str("- ");
        output.push_str(message.trim());
        output.push('\n');
    }
    output.push_str(
        "The child reached a non-continuable terminal boundary before applying this steering. Preserve it when resuming or synthesizing.\n[/UNCONSUMED PARENT STEERING]",
    );
}

fn hash_cache_scope_component(hasher: &mut Sha256, label: &str, value: &[u8]) {
    hasher.update(label.len().to_be_bytes());
    hasher.update(label.as_bytes());
    hasher.update(value.len().to_be_bytes());
    hasher.update(value);
}

/// Build a parent-scoped routing key for immutable delegated prompt prefixes.
///
/// The task objective is intentionally excluded because it is the volatile
/// user tail. Conversation and websocket identity remain task-scoped. Any
/// difference in provider, model, owner, workspace, system prompt, tools, or
/// inherited governance produces a different cache scope.
fn delegated_prompt_cache_scope(
    task: &SubAgentTask,
    provider: &str,
    model: &str,
    system_prompt: &str,
    tools: &[AiTool],
) -> Option<String> {
    let parent_scope = task
        .parent_session_id
        .as_deref()
        .or(task.delegated_run_id.as_deref())?;
    let policy = task.delegation_policy.as_ref()?;
    let policy = serde_json::to_vec(policy).ok()?;
    let mut canonical_tools = tools.iter().collect::<Vec<_>>();
    canonical_tools.sort_by(|left, right| left.name.cmp(&right.name));
    let canonical_tools = serde_json::to_vec(&canonical_tools).ok()?;

    let mut hasher = Sha256::new();
    hash_cache_scope_component(&mut hasher, "contract", b"delegated-prefix-v1");
    hash_cache_scope_component(&mut hasher, "parent", parent_scope.as_bytes());
    hash_cache_scope_component(
        &mut hasher,
        "owner",
        task.process_owner_id.as_deref().unwrap_or("").as_bytes(),
    );
    hash_cache_scope_component(
        &mut hasher,
        "workspace",
        task.working_dir.to_string_lossy().as_bytes(),
    );
    hash_cache_scope_component(&mut hasher, "provider", provider.as_bytes());
    hash_cache_scope_component(&mut hasher, "model", model.as_bytes());
    hash_cache_scope_component(
        &mut hasher,
        "reasoning_effort",
        serde_json::to_string(&task.reasoning_effort)
            .unwrap_or_else(|_| "null".to_string())
            .as_bytes(),
    );
    hash_cache_scope_component(&mut hasher, "system", system_prompt.as_bytes());
    hash_cache_scope_component(&mut hasher, "tools", &canonical_tools);
    hash_cache_scope_component(&mut hasher, "governance", &policy);
    Some(format!("{:x}", hasher.finalize()))
}

fn delegated_process_artifact(
    tool_name: &str,
    input: &serde_json::Value,
    result: &ToolResult,
    working_dir: &std::path::Path,
    process_owner_id: Option<&str>,
) -> Option<DelegatedProcessArtifact> {
    if tool_name != "bash" || result.is_error {
        return None;
    }

    let parsed = serde_json::from_str::<serde_json::Value>(&result.output).ok()?;
    let payload = parsed.get("data").unwrap_or(&parsed);
    let process_id = payload.get("process_id")?.as_str()?.trim();
    if process_id.is_empty() {
        return None;
    }
    let status = payload
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let command = input
        .get("command")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let endpoint_hints = payload
        .get("endpoint_hints")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some(DelegatedProcessArtifact {
        process_id: process_id.to_string(),
        owner_id: process_owner_id.unwrap_or_default().to_string(),
        status,
        command,
        working_dir: working_dir.display().to_string(),
        endpoint_hints,
        reused_existing: payload
            .get("reused_existing")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    })
}

fn record_delegated_process(
    processes: &mut Vec<DelegatedProcessArtifact>,
    process: DelegatedProcessArtifact,
) {
    if let Some(existing) = processes
        .iter_mut()
        .find(|existing| existing.process_id == process.process_id)
    {
        *existing = process;
    } else {
        processes.push(process);
    }
}

fn compact_delegated_history(
    messages: &mut Vec<ModelMessage>,
    files_examined: &[String],
    final_output: &str,
    trigger: &str,
) -> bool {
    if messages.len() <= 3 {
        return false;
    }

    // Retain a bounded complete tail beginning at an assistant turn so tool
    // result messages never survive without their corresponding tool calls.
    let mut tail_start = messages.len().saturating_sub(16).max(1);
    while tail_start < messages.len() && !matches!(messages[tail_start].role, Role::Assistant) {
        tail_start += 1;
    }
    if tail_start <= 1 {
        return false;
    }

    let removed = messages.drain(1..tail_start).count();
    let paths = files_examined
        .iter()
        .take(32)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let last_output = final_output.chars().take(2_000).collect::<String>();
    messages.insert(
        1,
        ModelMessage {
            role: Role::User,
            content: vec![Content::text(format!(
                    "[DELEGATED COMPACTION CHECKPOINT]\nTrigger: {trigger}\nCompacted messages: {removed}\nEvidence paths: {paths}\nLatest synthesis: {last_output}\nContinue the original objective from this checkpoint; do not repeat completed work.\n[/DELEGATED COMPACTION CHECKPOINT]"
                ))],
        },
    );
    true
}

/// Delegated agents use a specialized non-streaming transport, while sharing
/// canonical governance, semantic progress, failure, compaction, and history
/// policies with the parent loop.
pub(crate) async fn execute_agent_loop<C: AgentConfig>(
    client: &AiClient,
    task: &SubAgentTask,
    model: &str,
    cancellation: CancellationToken,
    config: &C,
    progress_tx: Option<mpsc::UnboundedSender<AgentProgress>>,
) -> SubAgentResult {
    let mut result =
        execute_agent_loop_inner(client, task, model, cancellation, config, progress_tx).await;

    release_delegated_task_processes(task, &mut result).await;
    result
}

async fn release_delegated_task_processes(task: &SubAgentTask, result: &mut SubAgentResult) {
    let process_owner_id = task.delegated_process_owner_id();
    if let Some(registry) = task.process_registry.as_ref() {
        let owner_id = process_owner_id;
        let failures = registry.kill_all_for_user(&owner_id).await;
        for process in &mut result.background_processes {
            let artifact_owner = if process.owner_id.is_empty() {
                owner_id.as_str()
            } else {
                process.owner_id.as_str()
            };
            process.status = registry
                .get_for_user(artifact_owner, &process.process_id)
                .await
                .map(|info| info.display_status().to_string())
                .unwrap_or_else(|| "missing".to_string());
        }
        for (process_id, error) in failures {
            tracing::warn!(
                task_id = %task.id,
                %process_id,
                %error,
                "Failed to release delegated task background process"
            );
        }
    }
}

async fn execute_agent_loop_inner<C: AgentConfig>(
    client: &AiClient,
    task: &SubAgentTask,
    model: &str,
    cancellation: CancellationToken,
    config: &C,
    progress_tx: Option<mpsc::UnboundedSender<AgentProgress>>,
) -> SubAgentResult {
    let provenance = RunProvenance::Delegated;
    info!(
        surface = provenance.as_str(),
        kernel = provenance.kernel().as_str(),
        task_id = %task.id,
        delegated_run_id = ?task.delegated_run_id,
        "Starting delegated agent kernel"
    );
    let start = Instant::now();
    let task_id = task.id.clone();
    let progress_task_id = task
        .delegation_task_id
        .clone()
        .unwrap_or_else(|| task_id.clone());
    let task_name = task.display_name();
    let plan_task_id = task.plan_task_id.clone();
    let transport_session_id = task
        .delegated_run_id
        .as_deref()
        .map(|run_id| format!("{run_id}:{task_id}"))
        .unwrap_or_else(|| task_id.clone());

    let ai_tools = config.get_ai_tools();
    let ctx = build_subagent_tool_context(task, config.timeout_secs());

    let mut messages: Vec<ModelMessage> = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(task.prompt.clone())],
    }];

    let mut files_examined: Vec<String> = vec![];
    let mut turns = 0;
    let mut total_tool_calls = 0;
    let mut estimated_tokens: usize = 0;
    let mut final_output = String::new();
    let mut last_action = "starting...".to_string();
    let mut policy_violations: Vec<String> = Vec::new();
    let mut unique_files_examined: HashSet<String> = HashSet::new();
    let mut stale_readonly_cycles = 0usize;
    let mut forced_summary_requested = false;
    let mut forced_summary_gap_closing_cycle_used = false;
    let mut last_cycle_positive_evidence = false;
    let mut tool_truth_corrections = 0usize;
    let mut forced_read_before_completion = false;
    let mut structured_report_repair_requested = false;
    let mut structured_handoff_repair_requested = false;
    let mut structured_handoff_repair_pending = false;
    let mut background_processes = Vec::new();
    let mut evidence = DelegatedEvidenceSummary::default();
    let mut canonical_evidence_correction_requested = false;
    let mut max_tokens_landing_retry_attempted = false;
    let mut max_tokens_landing_pending = false;
    let mut turn_budget_landing_pending = false;
    let mut loop_guard = LoopGuard::new();
    let mut loop_guard_landing: Option<String> = None;
    let mut overflow_compact_retry_attempted = false;
    let mut timeout_retry_attempted = false;
    let mut last_dynamic_context: Option<String> = None;
    let mut browser_acceptance_correction_requested = false;

    let send_progress = |status: AgentProgressStatus,
                         action: &str,
                         tool_count: usize,
                         tokens: usize,
                         completion_summary: Option<String>,
                         config: &C| {
        if let Some(ref tx) = progress_tx {
            let is_complete = status == AgentProgressStatus::Complete;
            let mut progress = AgentProgress {
                delegated_run_id: task.delegated_run_id.clone(),
                task_id: progress_task_id.clone(),
                name: task_name.clone(),
                identity: task.identity.clone(),
                status,
                tool_count,
                tokens,
                current_action: Some(action.to_string()),
                completion_summary,
                completed_plan_task: if is_complete {
                    plan_task_id.clone()
                } else {
                    None
                },
                ..Default::default()
            };
            config.update_progress(&mut progress);
            let _ = tx.send(progress);
        }
    };

    let send_conversation = |event: AgentConversationEvent,
                             action: &str,
                             tool_count: usize,
                             tokens: usize,
                             config: &C| {
        if let Some(ref tx) = progress_tx {
            let mut progress = AgentProgress {
                delegated_run_id: task.delegated_run_id.clone(),
                task_id: progress_task_id.clone(),
                name: task_name.clone(),
                identity: task.identity.clone(),
                status: AgentProgressStatus::Running,
                tool_count,
                tokens,
                current_action: Some(action.to_string()),
                conversation_event: Some(event),
                ..AgentProgress::default()
            };
            config.update_progress(&mut progress);
            let _ = tx.send(progress);
        }
    };

    send_progress(
        AgentProgressStatus::Running,
        &last_action,
        0,
        0,
        None,
        config,
    );

    macro_rules! seal_terminal_mailbox {
        () => {{
            if let Some(mailbox) = task.mailbox.as_ref() {
                let _ = mailbox.seal_for_terminal();
            }
        }};
    }

    macro_rules! preserve_terminal_mailbox {
        () => {{
            if let Some(mailbox) = task.mailbox.as_ref() {
                let parent_messages = mailbox.seal_for_terminal();
                retain_unconsumed_parent_messages(&mut final_output, parent_messages);
            }
        }};
    }

    macro_rules! return_cancelled {
        () => {{
            seal_terminal_mailbox!();
            info!(task_id = %task_id, "Agent cancelled");
            send_progress(
                AgentProgressStatus::Cancelled,
                "cancelled",
                total_tool_calls,
                estimated_tokens,
                None,
                config,
            );
            config.cleanup();
            return SubAgentResult {
                task_id,
                agent_name: task_name.clone(),
                delegated_run_id: task.delegated_run_id.clone(),
                success: false,
                output: final_output.clone(),
                files_examined: files_examined.clone(),
                duration_ms: start.elapsed().as_millis() as u64,
                turns_used: turns,
                error: Some("Cancelled".to_string()),
                termination: SubAgentTermination::Cancelled,
                policy_violations: policy_violations.clone(),
                evidence: evidence.clone(),
                background_processes: background_processes.clone(),
            };
        }};
    }

    macro_rules! return_governor_blocked {
        ($reason:expr) => {{
            let reason = $reason;
            preserve_terminal_mailbox!();
            send_progress(
                AgentProgressStatus::Failed,
                "provider call blocked",
                total_tool_calls,
                estimated_tokens,
                None,
                config,
            );
            config.cleanup();
            return SubAgentResult {
                task_id,
                agent_name: task_name.clone(),
                delegated_run_id: task.delegated_run_id.clone(),
                success: false,
                output: final_output.clone(),
                files_examined: files_examined.clone(),
                duration_ms: start.elapsed().as_millis() as u64,
                turns_used: turns,
                error: Some(reason),
                termination: SubAgentTermination::Failed,
                policy_violations: policy_violations.clone(),
                evidence: evidence.clone(),
                background_processes: background_processes.clone(),
            };
        }};
    }

    macro_rules! seal_terminal_or_continue {
        ($assistant_text:expr) => {{
            if let Some(mailbox) = task.mailbox.as_ref() {
                match mailbox.drain_or_seal_for_finish() {
                    AgentMailboxFinish::Continue(parent_messages) => {
                        append_late_mailbox_continuation(
                            &mut messages,
                            $assistant_text,
                            parent_messages,
                        );
                        loop_guard.reset_for_steering();
                        continue;
                    }
                    AgentMailboxFinish::Cancelled => return_cancelled!(),
                    AgentMailboxFinish::WorkerFinished | AgentMailboxFinish::LastWorkerSealed => {}
                }
            }
        }};
    }

    loop {
        if cancellation.is_cancelled() {
            return_cancelled!();
        }

        let max_turns_budget = delegated_turn_budget(task);
        if let Some(mailbox) = task.mailbox.as_ref() {
            let parent_messages = mailbox.drain();
            if !parent_messages.is_empty() {
                loop_guard.reset_for_steering();
                loop_guard_landing = None;
                max_tokens_landing_pending = false;
            }
            append_parent_messages(&mut messages, parent_messages);
        }

        if loop_guard_landing.is_none()
            && !max_tokens_landing_pending
            && !structured_handoff_repair_pending
        {
            if let Some(max_turns) = max_turns_budget {
                if turns >= max_turns {
                    if completed_report_at_turn_budget(
                        task,
                        &evidence,
                        &final_output,
                        &files_examined,
                    ) {
                        preserve_terminal_mailbox!();
                        let result = enforce_canonical_evidence(
                            normalize_explorer_result(
                                SubAgentResult {
                                    task_id: task_id.clone(),
                                    agent_name: task_name.clone(),
                                    delegated_run_id: task.delegated_run_id.clone(),
                                    success: true,
                                    output: final_output,
                                    files_examined: files_examined.clone(),
                                    duration_ms: start.elapsed().as_millis() as u64,
                                    turns_used: turns,
                                    error: None,
                                    termination: SubAgentTermination::Completed,
                                    policy_violations: policy_violations.clone(),
                                    evidence: evidence.clone(),
                                    background_processes: background_processes.clone(),
                                },
                                task,
                            ),
                            task,
                        );
                        info!(
                            task_id = %result.task_id,
                            turns,
                            max_turns,
                            "Accepted canonical delegated report at exact turn boundary"
                        );
                        send_progress(
                            AgentProgressStatus::Complete,
                            "complete at turn boundary",
                            total_tool_calls,
                            estimated_tokens,
                            completion_summary_preview(&result.output),
                            config,
                        );
                        config.cleanup();
                        return result;
                    }
                    preserve_terminal_mailbox!();
                    warn!(
                        task_id = %task_id,
                        turns = turns,
                        max_turns = max_turns,
                        "Sub-agent exceeded max turns"
                    );
                    send_progress(
                        AgentProgressStatus::Failed,
                        "max turns reached",
                        total_tool_calls,
                        estimated_tokens,
                        None,
                        config,
                    );
                    config.cleanup();
                    return SubAgentResult {
                        task_id,
                        agent_name: task_name.clone(),
                        delegated_run_id: task.delegated_run_id.clone(),
                        success: false,
                        output: final_output,
                        files_examined: files_examined.clone(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        turns_used: turns,
                        error: Some(format!(
                            "Sub-agent exceeded configured turn budget ({})",
                            max_turns
                        )),
                        termination: SubAgentTermination::Failed,
                        policy_violations: policy_violations.clone(),
                        evidence: evidence.clone(),
                        background_processes: background_processes.clone(),
                    };
                }
            }
        }

        if turn_budget_landing_due(
            turns,
            max_turns_budget,
            evidence.has_canonical_evidence(),
            loop_guard_landing.is_some() || max_tokens_landing_pending,
        ) {
            messages.push(ModelMessage {
                role: Role::System,
                content: vec![Content::text(TURN_BUDGET_LANDING_INSTRUCTION.to_string())],
            });
            turn_budget_landing_pending = true;
            send_progress(
                AgentProgressStatus::Running,
                "preparing final handoff",
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&final_output),
                config,
            );
        }
        turns += 1;

        if let Some(context) = config
            .dynamic_context()
            .filter(|context| !context.trim().is_empty())
        {
            if last_dynamic_context.as_deref() != Some(context.as_str()) {
                messages.push(ModelMessage {
                    role: Role::User,
                    content: vec![Content::text(context.clone())],
                });
                last_dynamic_context = Some(context);
            }
        }

        let system_prompt = config.system_prompt(turns);
        let tools_for_turn = tool_surface_for_turn(
            &ai_tools,
            loop_guard_landing.is_some(),
            max_tokens_landing_pending,
            turn_budget_landing_pending,
            structured_handoff_repair_pending,
        );
        let prompt_cache_scope = delegated_prompt_cache_scope(
            task,
            client.provider_id().storage_key(),
            model,
            &system_prompt,
            tools_for_turn,
        );

        let thinking_action = if total_tool_calls > 0 {
            format!("{}...", last_action)
        } else {
            "thinking...".to_string()
        };
        send_progress(
            AgentProgressStatus::Running,
            &thinking_action,
            total_tool_calls,
            estimated_tokens,
            None,
            config,
        );

        let delegated_permit = if let Some(governor) = task
            .provider_call_trace
            .as_ref()
            .and_then(|trace| trace.provider_governor())
        {
            let trace = task
                .provider_call_trace
                .as_ref()
                .expect("provider trace was present");
            let child_scope = format!(
                "{}:{}",
                task.delegated_run_id.as_deref().unwrap_or("delegated"),
                task_id
            );
            let slot = match crate::agent::WorkerProviderCallSlot::child(
                crate::agent::WorkerProviderCallKind::DelegatedAgentTurn,
                u32::try_from(trace.turn()).unwrap_or(u32::MAX),
                u32::try_from(turns).unwrap_or(u32::MAX),
                child_scope,
            ) {
                Ok(slot) => slot,
                Err(error) => return_governor_blocked!(format!(
                    "Hive Worker delegated provider slot failed closed: {error:#}"
                )),
            };
            let request_bytes = serde_json::to_vec(&(&messages, tools_for_turn))
                .map(|encoded| encoded.len())
                .unwrap_or_default()
                .saturating_add(system_prompt.len());
            let reservation = crate::agent::bounded_reservation(
                request_bytes
                    .saturating_add(2)
                    .saturating_div(3)
                    .saturating_add(128),
                config.max_tokens(),
            );
            match governor.admit(slot, reservation) {
                Ok(crate::agent::WorkerProviderAdmission::Allowed(permit)) => Some(permit),
                Ok(crate::agent::WorkerProviderAdmission::Gated(decision)) => {
                    return_governor_blocked!(format!(
                        "Hive Worker delegated provider call gated: {}",
                        serde_json::to_string(&decision)
                            .unwrap_or_else(|_| "durable policy".into())
                    ))
                }
                Ok(crate::agent::WorkerProviderAdmission::AlreadyStarted(call)) => {
                    return_governor_blocked!(format!(
                        "Hive Worker delegated provider call {} was already Started and was not replayed",
                        call.provider_call_id
                    ))
                }
                Err(error) => return_governor_blocked!(format!(
                    "Hive Worker delegated provider admission failed closed: {error:#}"
                )),
            }
        } else {
            None
        };
        let delegated_provider_call_id = delegated_permit
            .as_ref()
            .map(|permit| permit.provider_call_id().to_string());
        let provider_call_started = Instant::now();
        let attempt_policy = if delegated_permit.is_some() {
            crate::ai::client::RemoteAttemptPolicy::GovernedSingleAttempt
        } else {
            crate::ai::client::RemoteAttemptPolicy::ConfiguredRetries
        };
        let api_future = call_subagent_api(
            client,
            model,
            &system_prompt,
            &messages,
            tools_for_turn,
            config.max_tokens(),
            task.reasoning_effort,
            &transport_session_id,
            prompt_cache_scope.as_deref(),
            attempt_policy,
        );

        let api_result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            result = tokio::time::timeout(config.api_call_timeout(), api_future) => Some(result),
        };
        let (provider_outcome, provider_usage) = match &api_result {
            Some(Ok(Ok(response))) => ("completed", parse_response_usage(response)),
            Some(Ok(Err(_))) => ("error", None),
            Some(Err(_)) => ("timeout", None),
            None => ("cancelled", None),
        };
        if let Some(trace) = task.provider_call_trace.as_ref() {
            if let Some(provider_call_id) = delegated_provider_call_id.clone() {
                trace
                    .record_delegated_call_with_id(
                        provider_call_id,
                        "delegated_agent_turn",
                        client.provider_id(),
                        model,
                        task.reasoning_effort,
                        task.delegated_run_id.as_deref(),
                        &task_id,
                        turns,
                        provider_call_started,
                        provider_outcome,
                        provider_usage.clone(),
                    )
                    .await;
            } else {
                trace
                    .record_delegated_call(
                        "delegated_agent_turn",
                        client.provider_id(),
                        model,
                        task.reasoning_effort,
                        task.delegated_run_id.as_deref(),
                        &task_id,
                        turns,
                        provider_call_started,
                        provider_outcome,
                        provider_usage.clone(),
                    )
                    .await;
            }
        }

        if let (Some(permit), Some(Ok(Ok(_)))) = (delegated_permit.as_ref(), api_result.as_ref()) {
            if let Err(error) =
                permit.complete(crate::agent::WorkerProviderCompletion::acknowledged(
                    crate::agent::WorkerProviderTerminalOutcome::Completed,
                    provider_usage.clone(),
                ))
            {
                return_governor_blocked!(format!(
                    "Hive Worker delegated provider accounting failed closed: {error:#}"
                ));
            }
        } else if delegated_permit.is_some() && api_result.is_some() {
            return_governor_blocked!(
                "Hive Worker delegated provider acceptance is uncertain; the call was not retried"
                    .to_string()
            );
        }

        let Some(api_result) = api_result else {
            return_cancelled!();
        };
        if cancellation.is_cancelled() {
            return_cancelled!();
        }

        let response = match api_result {
            Ok(Ok(r)) => r,
            Ok(Err(e))
                if loop_guard_landing.is_none()
                    && !max_tokens_landing_pending
                    && !overflow_compact_retry_attempted
                    && is_context_overflow_error(&e.to_string())
                    && compact_delegated_history(
                        &mut messages,
                        &files_examined,
                        &final_output,
                        "provider_overflow",
                    ) =>
            {
                overflow_compact_retry_attempted = true;
                turns = turns.saturating_sub(1);
                warn!(
                    task_id = %task_id,
                    "Provider rejected delegated context; compacted in place and retrying once"
                );
                send_progress(
                    AgentProgressStatus::Running,
                    "compacted context; retrying",
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                continue;
            }
            Ok(Err(e)) => {
                if let Some(diagnostic) = loop_guard_landing.take() {
                    preserve_terminal_mailbox!();
                    let output =
                        loop_guard_landing_output(&final_output, &diagnostic, &files_examined);
                    send_progress(
                        AgentProgressStatus::Failed,
                        if evidence.has_canonical_evidence() {
                            "degraded loop guard landing"
                        } else {
                            "loop guard landing failed"
                        },
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&output),
                        config,
                    );
                    config.cleanup();
                    return SubAgentResult {
                        task_id,
                        agent_name: task_name.clone(),
                        delegated_run_id: task.delegated_run_id.clone(),
                        success: false,
                        output,
                        files_examined,
                        duration_ms: start.elapsed().as_millis() as u64,
                        turns_used: turns,
                        error: Some(format!(
                            "{diagnostic} The bounded synthesis landing failed: {e}"
                        )),
                        termination: SubAgentTermination::LoopGuard,
                        policy_violations,
                        evidence: evidence.clone(),
                        background_processes: background_processes.clone(),
                    };
                }
                preserve_terminal_mailbox!();
                send_progress(
                    AgentProgressStatus::Failed,
                    "error",
                    total_tool_calls,
                    estimated_tokens,
                    None,
                    config,
                );
                config.cleanup();
                return SubAgentResult {
                    task_id,
                    agent_name: task_name.clone(),
                    delegated_run_id: task.delegated_run_id.clone(),
                    success: false,
                    output: final_output,
                    files_examined,
                    duration_ms: start.elapsed().as_millis() as u64,
                    turns_used: turns,
                    error: Some(e.to_string()),
                    termination: SubAgentTermination::Failed,
                    policy_violations,
                    evidence: evidence.clone(),
                    background_processes: background_processes.clone(),
                };
            }
            Err(_) => {
                if let Some(diagnostic) = loop_guard_landing.take() {
                    preserve_terminal_mailbox!();
                    let output =
                        loop_guard_landing_output(&final_output, &diagnostic, &files_examined);
                    send_progress(
                        AgentProgressStatus::Failed,
                        if evidence.has_canonical_evidence() {
                            "degraded loop guard landing"
                        } else {
                            "loop guard landing failed"
                        },
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&output),
                        config,
                    );
                    config.cleanup();
                    return SubAgentResult {
                        task_id,
                        agent_name: task_name.clone(),
                        delegated_run_id: task.delegated_run_id.clone(),
                        success: false,
                        output,
                        files_examined,
                        duration_ms: start.elapsed().as_millis() as u64,
                        turns_used: turns,
                        error: Some(format!(
                            "{diagnostic} The bounded synthesis landing timed out."
                        )),
                        termination: SubAgentTermination::LoopGuard,
                        policy_violations,
                        evidence: evidence.clone(),
                        background_processes: background_processes.clone(),
                    };
                }
                if claim_single_timeout_retry(&mut timeout_retry_attempted) {
                    turns = turns.saturating_sub(1);
                    warn!(
                        task_id = %task_id,
                        turn = turns,
                        timeout_secs = config.api_call_timeout().as_secs(),
                        files_examined = files_examined.len(),
                        "Sub-agent API call timed out; retrying once"
                    );
                    send_progress(
                        AgentProgressStatus::Running,
                        "timed out; retrying",
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&final_output),
                        config,
                    );
                    continue;
                }
                warn!(
                    task_id = %task_id,
                    turn = turns,
                    timeout_secs = config.api_call_timeout().as_secs(),
                    files_examined = files_examined.len(),
                    "Sub-agent API call timed out"
                );
                let output = timeout_partial_output(&final_output, &files_examined);
                seal_terminal_or_continue!(&[] as &[String]);
                send_progress(
                    AgentProgressStatus::Failed,
                    if evidence.has_canonical_evidence() {
                        "timeout (partial evidence retained)"
                    } else {
                        "timeout"
                    },
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&output),
                    config,
                );
                config.cleanup();
                return SubAgentResult {
                    task_id,
                    agent_name: task_name.clone(),
                    delegated_run_id: task.delegated_run_id.clone(),
                    success: false,
                    output,
                    files_examined,
                    duration_ms: start.elapsed().as_millis() as u64,
                    turns_used: turns,
                    error: Some(format!(
                        "API call timed out after {}s on turn {}",
                        config.api_call_timeout().as_secs(),
                        turns
                    )),
                    termination: SubAgentTermination::ProviderTimeout,
                    policy_violations,
                    evidence: evidence.clone(),
                    background_processes: background_processes.clone(),
                };
            }
        };

        if let Some(usage) = provider_usage {
            estimated_tokens = estimated_tokens.saturating_add(usage.logical_total_tokens());
        }

        let (text_parts, tool_calls, stop_reason) = parse_response(&response);

        if cancellation.is_cancelled() {
            return_cancelled!();
        }

        if !text_parts.is_empty() {
            final_output = text_parts.join("\n");
        }

        let conversation_message_id = format!("{task_id}:turn:{turns}");
        let conversation_content = child_conversation_display_text(&text_parts.join("\n"));
        if !text_parts.is_empty() || !tool_calls.is_empty() {
            send_conversation(
                AgentConversationEvent::AssistantTurn {
                    message_id: conversation_message_id.clone(),
                    turn: turns,
                    content: conversation_content,
                    tool_calls: tool_calls
                        .iter()
                        .map(|call| AgentConversationToolCall {
                            id: call.id.clone(),
                            name: call.name.clone(),
                            arguments: call.input.clone(),
                        })
                        .collect(),
                },
                &thinking_action,
                total_tool_calls,
                estimated_tokens,
                config,
            );
        }

        // A semantic guard gets exactly one tool-free landing call. Never
        // execute a hallucinated call from that response, and never silently
        // publish a clean completion: canonical evidence is a degraded result;
        // no evidence is a truthful failure.
        if let Some(diagnostic) = loop_guard_landing.take() {
            final_output = loop_guard_landing_output(&final_output, &diagnostic, &files_examined);
            seal_terminal_or_continue!(&text_parts);
            let has_evidence = evidence.has_canonical_evidence();
            send_progress(
                AgentProgressStatus::Failed,
                if has_evidence {
                    "degraded loop guard landing"
                } else {
                    "loop guard landing failed"
                },
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&final_output),
                config,
            );
            config.cleanup();
            return SubAgentResult {
                task_id,
                agent_name: task_name.clone(),
                delegated_run_id: task.delegated_run_id.clone(),
                success: false,
                output: final_output,
                files_examined,
                duration_ms: start.elapsed().as_millis() as u64,
                turns_used: turns,
                error: Some(diagnostic),
                termination: SubAgentTermination::LoopGuard,
                policy_violations,
                evidence: evidence.clone(),
                background_processes: background_processes.clone(),
            };
        }

        // The concise max-token retry is also a bounded, tool-free landing.
        // A provider that still emits a structured call cannot reopen work
        // after the landing boundary; retain the partial response instead.
        if max_tokens_landing_pending {
            max_tokens_landing_pending = false;
            if stop_reason == "max_tokens" || !tool_calls.is_empty() {
                seal_terminal_or_continue!(&text_parts);
                let has_evidence = evidence.has_canonical_evidence();
                send_progress(
                    AgentProgressStatus::Failed,
                    if has_evidence {
                        "output limit reached (partial evidence retained)"
                    } else {
                        "output limit reached"
                    },
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                config.cleanup();
                return SubAgentResult {
                    task_id,
                    agent_name: task_name.clone(),
                    delegated_run_id: task.delegated_run_id.clone(),
                    success: false,
                    output: final_output,
                    files_examined,
                    duration_ms: start.elapsed().as_millis() as u64,
                    turns_used: turns,
                    error: Some(
                        "Provider output remained incomplete after one bounded tool-free landing"
                            .to_string(),
                    ),
                    termination: SubAgentTermination::ProviderMaxTokens,
                    policy_violations,
                    evidence: evidence.clone(),
                    background_processes: background_processes.clone(),
                };
            }
        }

        let landing_turn_available = max_turns_budget
            .map(|max_turns| turns < max_turns)
            .unwrap_or(true);
        match max_tokens_landing_decision(
            &tool_calls,
            &stop_reason,
            max_tokens_landing_retry_attempted,
            landing_turn_available,
        ) {
            MaxTokensLandingDecision::NotApplicable => {}
            MaxTokensLandingDecision::RetryConciseLanding => {
                max_tokens_landing_retry_attempted = true;
                max_tokens_landing_pending = true;
                if !text_parts.is_empty() {
                    messages.push(ModelMessage {
                        role: Role::Assistant,
                        content: text_parts
                            .iter()
                            .map(|text| Content::text(text.clone()))
                            .collect(),
                    });
                }
                messages.push(ModelMessage {
                    role: Role::User,
                    content: vec![Content::text("Your previous response hit the provider output limit and is incomplete. Using only evidence already gathered, return one concise final answer now. Do not call more tools. State what is established and any remaining gap; omit repeated detail."
                            .to_string())],
                });
                send_progress(
                    AgentProgressStatus::Running,
                    "landing truncated response concisely",
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                continue;
            }
            MaxTokensLandingDecision::TerminalIncomplete => {
                seal_terminal_or_continue!(&text_parts);
                let has_evidence = evidence.has_canonical_evidence();
                send_progress(
                    AgentProgressStatus::Failed,
                    if has_evidence {
                        "output limit reached (partial evidence retained)"
                    } else {
                        "output limit reached"
                    },
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                config.cleanup();
                return SubAgentResult {
                    task_id,
                    agent_name: task_name.clone(),
                    delegated_run_id: task.delegated_run_id.clone(),
                    success: false,
                    output: final_output,
                    files_examined,
                    duration_ms: start.elapsed().as_millis() as u64,
                    turns_used: turns,
                    error: Some(
                        "Provider output reached the token limit before a complete final answer"
                            .to_string(),
                    ),
                    termination: SubAgentTermination::ProviderMaxTokens,
                    policy_violations,
                    evidence: evidence.clone(),
                    background_processes: background_processes.clone(),
                };
            }
        }

        if structured_handoff_repair_pending {
            structured_handoff_repair_pending = false;
        }

        if turn_budget_landing_pending {
            turn_budget_landing_pending = false;
            if !tool_calls.is_empty() {
                preserve_terminal_mailbox!();
                send_progress(
                    AgentProgressStatus::Failed,
                    "invalid tool call during final handoff",
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                config.cleanup();
                return SubAgentResult {
                    task_id,
                    agent_name: task_name.clone(),
                    delegated_run_id: task.delegated_run_id.clone(),
                    success: false,
                    output: final_output,
                    files_examined,
                    duration_ms: start.elapsed().as_millis() as u64,
                    turns_used: turns,
                    error: Some(
                        "Provider emitted a tool call during the reserved tool-free final handoff"
                            .to_string(),
                    ),
                    termination: SubAgentTermination::Failed,
                    policy_violations,
                    evidence: evidence.clone(),
                    background_processes: background_processes.clone(),
                };
            }
        }

        if config.use_explorer_heuristics()
            && delegated_is_explore(task)
            && last_cycle_positive_evidence
            && text_claims_tool_empty(&final_output)
        {
            if tool_truth_corrections >= 1 {
                preserve_terminal_mailbox!();
                send_progress(
                    AgentProgressStatus::Failed,
                    "misread tool output",
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                config.cleanup();
                return SubAgentResult {
                    task_id,
                    agent_name: task_name.clone(),
                    delegated_run_id: task.delegated_run_id.clone(),
                    success: false,
                    output: final_output,
                    files_examined,
                    duration_ms: start.elapsed().as_millis() as u64,
                    turns_used: turns,
                    error: Some(
                        "Misread successful tool output after correction; delegated exploration is no longer trustworthy"
                            .to_string(),
                    ),
                    termination: SubAgentTermination::Failed,
                    policy_violations,
                    evidence: evidence.clone(),
                    background_processes: background_processes.clone(),
                };
            }

            tool_truth_corrections += 1;
            messages.push(ModelMessage {
                role: Role::User,
                content: vec![Content::text("Correction: the previous tool calls returned real data. You must treat successful tool output as ground truth and use it directly. Do not claim the results were empty or that nothing worked. Summarize the evidence from the returned paths/content instead of rechecking blindly.".to_string())],
            });
            send_progress(
                AgentProgressStatus::Running,
                "correcting tool interpretation",
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&final_output),
                config,
            );
            continue;
        }

        if !response_requires_tool_execution(&tool_calls, &stop_reason) {
            match evidence_completion_decision(
                &evidence,
                canonical_evidence_correction_requested,
                !ai_tools.is_empty(),
            ) {
                EvidenceCompletionDecision::Ready => {}
                EvidenceCompletionDecision::RequestEvidence => {
                    canonical_evidence_correction_requested = true;
                    messages.push(ModelMessage {
                        role: Role::User,
                        content: vec![Content::text("You have not gathered canonical evidence from the governed tool surface yet. Before finishing, use at least one available tool that directly supports the assigned objective, then report only what that tool result establishes. Do not claim completion from prose alone."
                                .to_string())],
                    });
                    send_progress(
                        AgentProgressStatus::Running,
                        "requiring canonical tool evidence",
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&final_output),
                        config,
                    );
                    continue;
                }
                EvidenceCompletionDecision::Reject => {
                    seal_terminal_or_continue!(&text_parts);
                    send_progress(
                        AgentProgressStatus::Failed,
                        "no canonical tool evidence",
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&final_output),
                        config,
                    );
                    config.cleanup();
                    return SubAgentResult {
                        task_id,
                        agent_name: task_name.clone(),
                        delegated_run_id: task.delegated_run_id.clone(),
                        success: false,
                        output: final_output,
                        files_examined: files_examined.clone(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        turns_used: turns,
                        error: Some(
                            "Delegated child completed without canonical tool evidence".to_string(),
                        ),
                        termination: SubAgentTermination::Failed,
                        policy_violations: policy_violations.clone(),
                        evidence: evidence.clone(),
                        background_processes: background_processes.clone(),
                    };
                }
            }

            if config.use_explorer_heuristics() && delegated_is_explore(task) {
                let missing_report = parse_explore_report(&final_output).is_none();

                if files_examined.is_empty() && !forced_read_before_completion {
                    forced_read_before_completion = true;
                    messages.push(ModelMessage {
                        role: Role::User,
                        content: vec![Content::text("You have not gathered any real path evidence yet. Before you finish, use the current working directory to inspect the most relevant files or directories, then produce the required <explore_report> with those paths in `paths_examined` and any concrete reads in `files_examined`. Do not stop yet.".to_string())],
                    });
                    send_progress(
                        AgentProgressStatus::Running,
                        "requiring path evidence",
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&final_output),
                        config,
                    );
                    continue;
                }

                if missing_report && !structured_report_repair_requested {
                    structured_report_repair_requested = true;
                    messages.push(ModelMessage {
                        role: Role::User,
                        content: vec![Content::text("Your previous response did not include the required <explore_report> JSON block. Using only the evidence you already gathered, reply now with exactly one valid <explore_report> block followed by exactly one valid <delegated_handoff> block as the final content. Include all supporting paths in `paths_examined`, concrete reads in `files_examined`, and a truthful objective status. Do not call more tools unless a single critical read is required.".to_string())],
                    });
                    send_progress(
                        AgentProgressStatus::Running,
                        "repairing structured report",
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&final_output),
                        config,
                    );
                    continue;
                }
            }

            if parse_delegated_handoff(&final_output).is_none()
                && !structured_handoff_repair_requested
            {
                structured_handoff_repair_requested = true;
                structured_handoff_repair_pending = true;
                let repair_instruction = if config.use_explorer_heuristics()
                    && delegated_is_explore(task)
                {
                    "Your previous response omitted or malformed the required <delegated_handoff> JSON block. Using only canonical evidence already gathered, re-emit exactly one valid <explore_report> block followed by exactly one valid <delegated_handoff> block as the final content. No tools are available; do not claim checks that did not run."
                } else {
                    "Your previous response omitted or malformed the required <delegated_handoff> JSON block. Using only canonical evidence already gathered, reply with a concise human summary followed by exactly one valid <delegated_handoff> block as the final content. No tools are available; do not claim checks that did not run."
                };
                messages.push(ModelMessage {
                    role: Role::User,
                    content: vec![Content::text(repair_instruction.to_string())],
                });
                send_progress(
                    AgentProgressStatus::Running,
                    "repairing delegated handoff",
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                continue;
            }

            let missing_browser_proofs =
                missing_required_browser_acceptance_proofs(&task.prompt, &evidence);
            if !missing_browser_proofs.is_empty()
                && ai_tools.iter().any(|tool| tool.name == "browser_check")
                && !browser_acceptance_correction_requested
            {
                browser_acceptance_correction_requested = true;
                messages.push(ModelMessage {
                    role: Role::User,
                    content: vec![Content::text(format!(
                            "Your handoff is missing required canonical browser evidence: {}. The governed browser_check tool is present and authorized in your current tool list. Use it now against the tracked loopback preview; start or repair that preview with bash first if needed. Do not report browser_check as unavailable. A build, curl, or HTTP 200 is not a substitute. If the check fails, fix an in-scope product issue and retry once before returning a truthful handoff.",
                            missing_browser_proofs.join(", ")
                        ))],
                });
                send_progress(
                    AgentProgressStatus::Running,
                    "requiring governed browser acceptance",
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&final_output),
                    config,
                );
                continue;
            }

            let raw_result = SubAgentResult {
                task_id: task_id.clone(),
                agent_name: task_name.clone(),
                delegated_run_id: task.delegated_run_id.clone(),
                success: true,
                output: final_output.clone(),
                files_examined: files_examined.clone(),
                duration_ms: start.elapsed().as_millis() as u64,
                turns_used: turns,
                error: None,
                termination: SubAgentTermination::Completed,
                policy_violations: policy_violations.clone(),
                evidence: evidence.clone(),
                background_processes: background_processes.clone(),
            };
            let result = if config.use_explorer_heuristics() {
                normalize_explorer_result(raw_result, task)
            } else {
                raw_result
            };
            let result = enforce_canonical_evidence(result, task);
            seal_terminal_or_continue!(&text_parts);
            info!(
                task_id = %result.task_id,
                turns = turns,
                output_len = result.output.len(),
                success = result.success,
                "Agent completed"
            );
            send_progress(
                terminal_progress_status(&result),
                if result.success {
                    "complete"
                } else {
                    "degraded completion"
                },
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&result.output),
                config,
            );
            config.cleanup();
            return result;
        }

        let is_explore_delegation = delegated_is_explore(task);
        let all_read_only_tools = is_explore_delegation
            && tool_calls
                .iter()
                .all(|call| matches!(call.name.as_str(), "read" | "glob" | "grep" | "list"));

        let forced_summary_decision = forced_summary_tool_decision(
            forced_summary_requested,
            forced_summary_gap_closing_cycle_used,
            all_read_only_tools,
        );
        if forced_summary_decision == ForcedSummaryToolDecision::AllowGapClosingCycle {
            forced_summary_gap_closing_cycle_used = true;
        }

        if config.use_explorer_heuristics()
            && is_explore_delegation
            && forced_summary_decision == ForcedSummaryToolDecision::ForceSummary
        {
            let synthesized =
                synthesized_explorer_output(&task.name, &final_output, &files_examined);
            let result = enforce_canonical_evidence(
                normalize_explorer_result(
                    SubAgentResult {
                        task_id: task_id.clone(),
                        agent_name: task_name.clone(),
                        delegated_run_id: task.delegated_run_id.clone(),
                        success: true,
                        output: synthesized,
                        files_examined: files_examined.clone(),
                        duration_ms: start.elapsed().as_millis() as u64,
                        turns_used: turns,
                        error: None,
                        termination: SubAgentTermination::Completed,
                        policy_violations: policy_violations.clone(),
                        evidence: evidence.clone(),
                        background_processes: background_processes.clone(),
                    },
                    task,
                ),
                task,
            );
            seal_terminal_or_continue!(&text_parts);
            send_progress(
                terminal_progress_status(&result),
                if result.success {
                    "forced summary"
                } else {
                    "forced summary degraded"
                },
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&result.output),
                config,
            );
            config.cleanup();
            return result;
        }

        let mut assistant_content: Vec<Content> = text_parts
            .iter()
            .map(|t| Content::text(t.clone()))
            .collect();

        let mut cycle_new_files = 0usize;

        for tc in &tool_calls {
            assistant_content.push(Content::ToolUse {
                id: tc.id.clone(),
                name: tc.name.clone(),
                input: tc.input.clone(),
            });
        }

        messages.push(ModelMessage {
            role: Role::Assistant,
            content: assistant_content,
        });

        let mut tool_results: Vec<Content> = vec![];
        let mut cycle_positive_evidence = false;

        for tc in &tool_calls {
            if cancellation.is_cancelled() {
                return_cancelled!();
            }
            total_tool_calls += 1;
            evidence.record_attempt();
            if let Some(policy) = task.delegation_policy.as_ref() {
                if let Err(reason) = policy.authorize_tool_call(&tc.name, &tc.input, ctx.plan_mode)
                {
                    let violation = format!("{}: {}", tc.name, reason);
                    policy_violations.push(violation.clone());
                    let display_output = build_history_tool_result(
                        &tc.name,
                        &crate::tools::registry::ToolResult::error_with_details(
                            "delegated_policy_block",
                            reason,
                            None,
                            Some(policy.audit_json()),
                        )
                        .output,
                        true,
                    );
                    send_conversation(
                        AgentConversationEvent::ToolResult {
                            message_id: conversation_message_id.clone(),
                            tool_call_id: tc.id.clone(),
                            name: tc.name.clone(),
                            output: display_output.to_string(),
                            is_error: true,
                        },
                        &last_action,
                        total_tool_calls,
                        estimated_tokens,
                        config,
                    );
                    tool_results.push(Content::ToolResult {
                        tool_use_id: tc.id.clone(),
                        output: display_output,
                        is_error: Some(true),
                    });

                    if policy_violations.len() >= MAX_DELEGATED_POLICY_VIOLATIONS {
                        preserve_terminal_mailbox!();
                        send_progress(
                            AgentProgressStatus::Failed,
                            "delegated policy blocked repeated tool calls",
                            total_tool_calls,
                            estimated_tokens,
                            None,
                            config,
                        );
                        config.cleanup();
                        return SubAgentResult {
                            task_id,
                            agent_name: task_name.clone(),
                            delegated_run_id: task.delegated_run_id.clone(),
                            success: false,
                            output: final_output,
                            files_examined,
                            duration_ms: start.elapsed().as_millis() as u64,
                            turns_used: turns,
                            error: Some(
                                "Delegated policy containment triggered after repeated blocked tool attempts"
                                    .to_string(),
                            ),
                            termination: SubAgentTermination::Failed,
                            policy_violations,
                            evidence: evidence.clone(),
                            background_processes: background_processes.clone(),
                        };
                    }
                    continue;
                }
            }

            if cancellation.is_cancelled() {
                return_cancelled!();
            }

            last_action = config.format_action(&tc.name, &tc.input);
            send_progress(
                AgentProgressStatus::Running,
                &last_action,
                total_tool_calls,
                estimated_tokens,
                None,
                config,
            );

            // Once a filesystem mutation is dispatched, dropping its future
            // cannot prove that no mutation occurred: a write or multi-file
            // patch may have committed before its result is assembled. Bash is
            // the deliberate exception: its process-group drop guard is the
            // bounded quiescence mechanism, so interruption must drop that
            // future instead of waiting for the command timeout. Read-only work
            // remains promptly cancellable as well.
            let result = if tool_call_requires_completion_shield(&tc.name, &tc.input) {
                config.execute_tool(&tc.name, tc.input.clone(), &ctx).await
            } else {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return_cancelled!(),
                    result = config.execute_tool(&tc.name, tc.input.clone(), &ctx) => result,
                }
            };

            let (output, is_error) = match result {
                Some(r) => {
                    if let Some(process) = delegated_process_artifact(
                        &tc.name,
                        &tc.input,
                        &r,
                        &task.working_dir,
                        ctx.effective_process_owner_id(),
                    ) {
                        record_delegated_process(&mut background_processes, process);
                    }
                    (r.output, r.is_error)
                }
                None => (format!("Unknown tool: {}", tc.name), true),
            };

            if !is_error {
                if let Some(kind) = delegated_evidence_kind(&tc.name, &tc.input) {
                    evidence.record_success(kind);
                }
                let (effective_name, effective_input) = effective_tool_call(&tc.name, &tc.input);
                if effective_name == "browser_check" {
                    evidence.record_acceptance_proof("browser_runtime");
                    let actions = effective_input
                        .get("actions")
                        .and_then(serde_json::Value::as_array);
                    if actions.is_some_and(|actions| {
                        actions.iter().any(|action| {
                            action.get("action").and_then(serde_json::Value::as_str) == Some("key")
                        })
                    }) {
                        evidence.record_acceptance_proof("browser_keyboard");
                    }
                    let has_mobile_viewport = effective_input
                        .get("viewports")
                        .and_then(serde_json::Value::as_array)
                        .is_none_or(|viewports| {
                            viewports.iter().any(|viewport| {
                                viewport.get("mobile").and_then(serde_json::Value::as_bool)
                                    == Some(true)
                            })
                        });
                    if has_mobile_viewport
                        && actions.is_some_and(|actions| {
                            actions.iter().any(|action| {
                                action.get("action").and_then(serde_json::Value::as_str)
                                    == Some("click")
                            })
                        })
                    {
                        evidence.record_acceptance_proof("browser_touch");
                    }
                }
            }

            if tool_result_has_positive_evidence(&tc.name, &output, is_error) {
                cycle_positive_evidence = true;
            }

            if config.is_read_tool(&tc.name) {
                if let Some(path) = tc.input.get("file_path").and_then(|value| value.as_str()) {
                    let normalized = relative_or_display(path, &task.working_dir);
                    if unique_files_examined.insert(normalized.clone()) {
                        cycle_new_files += 1;
                        files_examined.push(normalized);
                    }
                }
            }

            for path in collect_paths_from_tool_result(&tc.name, &output, &task.working_dir) {
                if unique_files_examined.insert(path.clone()) {
                    cycle_new_files += 1;
                    files_examined.push(path);
                }
            }

            let display_output = build_history_tool_result(&tc.name, &output, is_error);
            send_conversation(
                AgentConversationEvent::ToolResult {
                    message_id: conversation_message_id.clone(),
                    tool_call_id: tc.id.clone(),
                    name: tc.name.clone(),
                    output: display_output.to_string(),
                    is_error,
                },
                &last_action,
                total_tool_calls,
                estimated_tokens,
                config,
            );
            tool_results.push(Content::ToolResult {
                tool_use_id: tc.id.clone(),
                output: display_output,
                is_error: Some(is_error),
            });
        }

        if cancellation.is_cancelled() {
            return_cancelled!();
        }

        let progress_calls = tool_calls
            .iter()
            .map(|call| AiToolCall {
                id: call.id.clone(),
                name: call.name.clone(),
                arguments: call.input.clone(),
            })
            .collect::<Vec<_>>();
        let guard = loop_guard.evaluate(&progress_calls, &tool_results);
        let progress_telemetry = guard.progress;
        let validation_completion = guard.repeated_validation;
        let post_explore_diagnostic =
            crate::agent::failure::detect_post_explore_manual_fallback(&messages, &progress_calls);
        let guard_diagnostic = guard
            .repeated_failure
            .or(guard.repeated_read_only)
            .or(post_explore_diagnostic)
            .or_else(|| {
                progress_telemetry
                    .as_ref()
                    .and_then(|telemetry| telemetry.diagnostic())
            });

        messages.push(ModelMessage {
            role: Role::User,
            content: tool_results,
        });
        if config.use_explorer_heuristics()
            && is_explore_delegation
            && forced_summary_decision == ForcedSummaryToolDecision::AllowGapClosingCycle
        {
            structured_handoff_repair_pending = true;
            messages.push(ModelMessage {
                role: Role::User,
                content: vec![Content::text("The one bounded critical-gap tool cycle is complete. No more tools are available. Using the evidence now gathered, return the concise final report and the exact required structured handoff with every acceptance-check id. Do not describe another intended tool call."
                        .to_string())],
            });
            send_progress(
                AgentProgressStatus::Running,
                "closing critical gap and synthesizing",
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&final_output),
                config,
            );
            continue;
        }
        crate::agent::progress::refresh_progress_feedback(
            &mut messages,
            progress_telemetry.as_ref(),
        );
        if let Some(completion) = validation_completion {
            info!(
                task_id = %task_id,
                turns,
                completion = %completion,
                "Delegated run completed after repeated successful validation"
            );
            let output = if final_output.trim().is_empty() {
                completion
            } else {
                format!("{}\n\n{}", final_output.trim(), completion)
            };
            seal_terminal_or_continue!(&[] as &[String]);
            let result = enforce_canonical_evidence(
                SubAgentResult {
                    task_id: task_id.clone(),
                    agent_name: task_name.clone(),
                    delegated_run_id: task.delegated_run_id.clone(),
                    success: true,
                    output,
                    files_examined,
                    duration_ms: start.elapsed().as_millis() as u64,
                    turns_used: turns,
                    error: None,
                    termination: SubAgentTermination::Completed,
                    policy_violations,
                    evidence: evidence.clone(),
                    background_processes: background_processes.clone(),
                },
                task,
            );
            send_progress(
                terminal_progress_status(&result),
                if result.success {
                    "validated"
                } else {
                    "no canonical tool evidence"
                },
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&result.output),
                config,
            );
            config.cleanup();
            return result;
        }
        if let Some(diagnostic) = guard_diagnostic {
            warn!(
                task_id = %task_id,
                turns,
                diagnostic = %diagnostic,
                "Delegated semantic progress guard entered one bounded synthesis landing"
            );
            messages.push(ModelMessage {
                role: Role::System,
                content: vec![Content::text(loop_guard_landing_instruction(&diagnostic))],
            });
            loop_guard_landing = Some(diagnostic);
            send_progress(
                AgentProgressStatus::Running,
                "synthesizing guarded evidence",
                total_tool_calls,
                estimated_tokens,
                completion_summary_preview(&final_output),
                config,
            );
            continue;
        }
        last_cycle_positive_evidence = cycle_positive_evidence;

        if config.use_explorer_heuristics() && is_explore_delegation && all_read_only_tools {
            let produced_new_files = cycle_new_files > 0;
            let has_sufficient_evidence = !final_output.trim().is_empty()
                && unique_files_examined.len() >= EXPLORER_SYNTHESIS_FILE_THRESHOLD;

            if has_sufficient_evidence || !produced_new_files {
                stale_readonly_cycles += 1;
            } else {
                stale_readonly_cycles = 0;
            }

            if stale_readonly_cycles >= EXPLORER_STALE_SEQUENCE_THRESHOLD {
                if !forced_summary_requested {
                    forced_summary_requested = true;
                    stale_readonly_cycles = 0;
                    messages.push(ModelMessage {
                        role: Role::User,
                        content: vec![Content::text("You now have enough evidence to answer the investigation. Stop exploring. Do not call more tools unless a critical gap prevents answering. Provide a concise summary covering architecture, key modules, design patterns, main concerns, and the most important files examined.".to_string())],
                    });
                    send_progress(
                        AgentProgressStatus::Running,
                        "synthesizing findings",
                        total_tool_calls,
                        estimated_tokens,
                        completion_summary_preview(&final_output),
                        config,
                    );
                    continue;
                }

                let synthesized =
                    synthesized_explorer_output(&task.name, &final_output, &files_examined);
                let result = enforce_canonical_evidence(
                    normalize_explorer_result(
                        SubAgentResult {
                            task_id: task_id.clone(),
                            agent_name: task_name.clone(),
                            delegated_run_id: task.delegated_run_id.clone(),
                            success: true,
                            output: synthesized,
                            files_examined: files_examined.clone(),
                            duration_ms: start.elapsed().as_millis() as u64,
                            turns_used: turns,
                            error: None,
                            termination: SubAgentTermination::Completed,
                            policy_violations: policy_violations.clone(),
                            evidence: evidence.clone(),
                            background_processes: background_processes.clone(),
                        },
                        task,
                    ),
                    task,
                );
                seal_terminal_or_continue!(&[] as &[String]);
                send_progress(
                    terminal_progress_status(&result),
                    if result.success {
                        "converged"
                    } else {
                        "converged degraded"
                    },
                    total_tool_calls,
                    estimated_tokens,
                    completion_summary_preview(&result.output),
                    config,
                );
                config.cleanup();
                return result;
            }
        } else {
            stale_readonly_cycles = 0;
        }

        if messages.len() > subagent::MAX_MESSAGES
            && compact_delegated_history(
                &mut messages,
                &files_examined,
                &final_output,
                "history_budget",
            )
        {
            tracing::debug!(
                task_id = %task_id,
                remaining = messages.len(),
                "Compacted delegated history in place"
            );
        }
    }
}

fn claim_single_timeout_retry(retry_attempted: &mut bool) -> bool {
    if *retry_attempted {
        return false;
    }
    *retry_attempted = true;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::providers::ReasoningEffort;
    use crate::tools::registry::{DelegationPolicy, PermissionMode};
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn child_conversation_hides_internal_handoff_after_human_summary() {
        let output = concat!(
            "Reviewed both files; the implementation is consistent.\n\n",
            "<delegated_handoff>{\"status\":\"complete\",\"summary\":\"done\",",
            "\"acceptance_checks\":[],\"remaining_work\":[],\"blockers\":[]}",
            "</delegated_handoff>"
        );

        assert_eq!(
            child_conversation_display_text(output),
            "Reviewed both files; the implementation is consistent."
        );
    }

    #[test]
    fn child_conversation_uses_handoff_summary_when_no_human_text_exists() {
        let output = concat!(
            "<delegated_handoff>{\"status\":\"complete\",",
            "\"summary\":\"The requested audit is complete.\",",
            "\"acceptance_checks\":[],\"remaining_work\":[],\"blockers\":[]}",
            "</delegated_handoff>"
        );

        assert_eq!(
            child_conversation_display_text(output),
            "The requested audit is complete."
        );
    }

    #[test]
    fn child_conversation_hides_explore_report_and_handoff_envelopes() {
        let output = concat!(
            "Found the relevant ownership boundary.\n",
            "<explore_report>{\"objective_status\":\"complete\",",
            "\"summary\":\"found\",\"paths_examined\":[],\"files_examined\":[]}",
            "</explore_report>",
            "<delegated_handoff>{\"status\":\"complete\",\"summary\":\"done\",",
            "\"acceptance_checks\":[],\"remaining_work\":[],\"blockers\":[]}",
            "</delegated_handoff>"
        );

        assert_eq!(
            child_conversation_display_text(output),
            "Found the relevant ownership boundary."
        );
    }

    fn cache_scope_task(id: &str, prompt: &str, parent: &str) -> SubAgentTask {
        SubAgentTask::new(id, prompt)
            .with_working_dir(std::path::PathBuf::from("/workspace"))
            .with_delegation_policy(DelegationPolicy::for_subagent_build(
                PermissionMode::Autonomous,
                Some(6),
            ))
            .with_process_context(None, Some("owner-a".to_string()), Some(parent.to_string()))
    }

    fn cache_scope_tools() -> Vec<AiTool> {
        vec![AiTool {
            name: "read".to_string(),
            description: "Read a file".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"path": {"type": "string"}}
            }),
            prompt: None,
        }]
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delegated_terminal_cleanup_stops_only_the_exact_task_process_owner() {
        let registry = Arc::new(crate::process::ProcessRegistry::new());
        let directory = tempfile::tempdir().expect("tempdir");
        let task = SubAgentTask::new("ui-proof", "validate preview")
            .with_working_dir(directory.path().to_path_buf())
            .with_delegated_run_id("group-a")
            .with_process_context(
                Some(Arc::clone(&registry)),
                Some("tenant-a".to_string()),
                Some("session-a".to_string()),
            );
        let task_owner = task.delegated_process_owner_id();
        let task_process = registry
            .spawn_for_user(
                &task_owner,
                "sleep 30".to_string(),
                directory.path().to_path_buf(),
                None,
                None,
            )
            .await
            .expect("task process");
        let parent_process = registry
            .spawn_for_user(
                "tenant-a",
                "sleep 30".to_string(),
                directory.path().to_path_buf(),
                None,
                Some("session-a".to_string()),
            )
            .await
            .expect("parent process");
        let mut result = SubAgentResult {
            task_id: task.id.clone(),
            agent_name: task.name.clone(),
            delegated_run_id: task.delegated_run_id.clone(),
            success: true,
            output: "validated".to_string(),
            files_examined: Vec::new(),
            duration_ms: 1,
            turns_used: 1,
            error: None,
            termination: SubAgentTermination::Completed,
            policy_violations: Vec::new(),
            evidence: DelegatedEvidenceSummary::default(),
            background_processes: vec![DelegatedProcessArtifact {
                process_id: task_process.clone(),
                owner_id: task_owner.clone(),
                status: "running".to_string(),
                command: "sleep 30".to_string(),
                working_dir: directory.path().display().to_string(),
                endpoint_hints: Vec::new(),
                reused_existing: false,
            }],
        };

        release_delegated_task_processes(&task, &mut result).await;

        assert_eq!(result.background_processes[0].status, "killed");
        assert!(!registry
            .get_for_user(&task_owner, &task_process)
            .await
            .expect("task process entry")
            .is_active());
        assert!(registry
            .get_for_user("tenant-a", &parent_process)
            .await
            .expect("parent process entry")
            .is_active());
        registry
            .kill_for_user("tenant-a", &parent_process)
            .await
            .expect("parent cleanup");
    }

    #[test]
    fn nonempty_tool_calls_outrank_a_contradictory_end_turn_reason() {
        let calls = vec![super::super::super::types::ToolCall {
            id: "call-1".to_string(),
            name: "read".to_string(),
            input: json!({"file_path": "src/lib.rs"}),
        }];

        assert!(response_requires_tool_execution(&calls, "end_turn"));
        assert!(!response_requires_tool_execution(&[], "end_turn"));
    }

    #[test]
    fn no_call_max_tokens_gets_exactly_one_available_landing_retry() {
        assert_eq!(
            max_tokens_landing_decision(&[], "max_tokens", false, true),
            MaxTokensLandingDecision::RetryConciseLanding
        );
        assert_eq!(
            max_tokens_landing_decision(&[], "max_tokens", true, true),
            MaxTokensLandingDecision::TerminalIncomplete
        );
        assert_eq!(
            max_tokens_landing_decision(&[], "max_tokens", false, false),
            MaxTokensLandingDecision::TerminalIncomplete
        );
    }

    #[test]
    fn forced_explorer_summary_allows_one_critical_gap_read_cycle() {
        assert_eq!(
            forced_summary_tool_decision(true, false, true),
            ForcedSummaryToolDecision::AllowGapClosingCycle
        );
        assert_eq!(
            forced_summary_tool_decision(true, true, true),
            ForcedSummaryToolDecision::ForceSummary
        );
        assert_eq!(
            forced_summary_tool_decision(false, false, true),
            ForcedSummaryToolDecision::NotApplicable
        );
        assert_eq!(
            forced_summary_tool_decision(true, false, false),
            ForcedSummaryToolDecision::NotApplicable
        );
    }

    #[test]
    fn delegated_provider_timeout_gets_exactly_one_retry() {
        let mut attempted = false;
        assert!(claim_single_timeout_retry(&mut attempted));
        assert!(attempted);
        assert!(!claim_single_timeout_retry(&mut attempted));
    }

    #[test]
    fn every_bounded_landing_removes_the_tool_surface() {
        let tools = cache_scope_tools();

        assert_eq!(
            tool_surface_for_turn(&tools, false, false, false, false).len(),
            1
        );
        assert!(tool_surface_for_turn(&tools, true, false, false, false).is_empty());
        assert!(tool_surface_for_turn(&tools, false, true, false, false).is_empty());
        assert!(tool_surface_for_turn(&tools, false, false, true, false).is_empty());
        assert!(tool_surface_for_turn(&tools, false, false, false, true).is_empty());
    }

    #[test]
    fn bounded_delegation_reserves_its_last_turn_only_after_real_evidence() {
        assert!(!turn_budget_landing_due(0, Some(1), true, false));
        assert!(!turn_budget_landing_due(4, Some(6), false, false));
        assert!(turn_budget_landing_due(5, Some(6), true, false));
        assert!(!turn_budget_landing_due(5, Some(6), true, true));
        assert!(!turn_budget_landing_due(6, Some(6), true, false));
        assert!(!turn_budget_landing_due(99, None, true, false));
    }

    #[test]
    fn exact_turn_boundary_accepts_a_canonical_read_only_report() {
        let policy = DelegationPolicy::for_subagent_verify(PermissionMode::Supervised, Some(4));
        let task = SubAgentTask::new("report", "inspect").with_delegation_policy(policy);
        let mut evidence = DelegatedEvidenceSummary::default();
        evidence.record_attempt();
        evidence.record_success(DelegatedEvidenceKind::Observation);
        let output = r#"<explore_report>{"summary":"verified","paths_examined":["."],"files_examined":[],"evidence":"directory inspected","changes":"none"}</explore_report>"#;

        assert!(completed_report_at_turn_budget(
            &task,
            &evidence,
            output,
            &[".".to_string()]
        ));
        assert!(!completed_report_at_turn_budget(
            &task,
            &evidence,
            "unstructured partial prose",
            &[]
        ));
        assert!(!completed_report_at_turn_budget(
            &task,
            &evidence,
            "Let me inspect the workspace before reporting.",
            &[".".to_string()]
        ));
    }

    #[test]
    fn exact_turn_boundary_accepts_substantive_negative_evidence_for_a_search_path() {
        let policy = DelegationPolicy::for_subagent_verify(PermissionMode::Supervised, Some(4));
        let task = SubAgentTask::new("metadata", "inspect").with_delegation_policy(policy);
        let mut evidence = DelegatedEvidenceSummary::default();
        evidence.record_attempt();
        evidence.record_success(DelegatedEvidenceKind::Observation);

        assert!(completed_report_at_turn_budget(
            &task,
            &evidence,
            "The workspace search completed and found no matching files.",
            &[".".to_string()]
        ));
    }

    #[test]
    fn max_tokens_never_suppresses_structured_tool_calls() {
        let calls = vec![super::super::super::types::ToolCall {
            id: "call-1".to_string(),
            name: "read".to_string(),
            input: json!({"file_path": "src/lib.rs"}),
        }];

        assert_eq!(
            max_tokens_landing_decision(&calls, "max_tokens", false, true),
            MaxTokensLandingDecision::NotApplicable
        );
        assert!(response_requires_tool_execution(&calls, "max_tokens"));
    }

    #[test]
    fn writes_are_shielded_but_bash_uses_its_process_group_drop_guard() {
        assert!(tool_call_requires_completion_shield(
            "write",
            &json!({"file_path": "src/lib.rs", "content": "changed"})
        ));
        assert!(tool_call_requires_completion_shield(
            "apply_patch",
            &json!({"patch": "*** Begin Patch"})
        ));
        assert!(!tool_call_requires_completion_shield(
            "bash",
            &json!({"command": "cargo test"})
        ));
        assert!(tool_call_requires_completion_shield(
            "tool_search",
            &json!({
                "action": "execute",
                "tool": "write",
                "arguments": {"file_path": "src/lib.rs", "content": "changed"}
            })
        ));
        assert!(!tool_call_requires_completion_shield(
            "read",
            &json!({"file_path": "src/lib.rs"})
        ));
    }

    #[test]
    fn loop_guard_landing_is_tool_free_and_retains_truthful_diagnostic() {
        let output = loop_guard_landing_output(
            "Found the durable race and isolated it to the lease handoff.",
            "Stopping exploration loop after repeated evidence.",
            &["src/lease.rs".to_string()],
        );
        let instruction =
            loop_guard_landing_instruction("Stopping exploration loop after repeated evidence.");

        assert!(instruction.contains("one bounded synthesis turn"));
        assert!(instruction.contains("No tools are available"));
        assert!(output.contains("Found the durable race"));
        assert!(output.contains("[DELEGATED LOOP GUARD]"));
        assert!(output.contains("Stopping exploration loop"));
    }

    #[test]
    fn noncontinuable_terminal_retains_accepted_parent_steering_for_resume() {
        let mut output = "evidence gathered".to_string();
        retain_unconsumed_parent_messages(
            &mut output,
            vec!["verify the migration edge case".to_string()],
        );

        assert!(output.contains("[UNCONSUMED PARENT STEERING]"));
        assert!(output.contains("verify the migration edge case"));
        assert!(output.contains("Preserve it when resuming or synthesizing"));
    }

    #[test]
    fn zero_evidence_gets_one_bounded_correction_then_rejects() {
        let evidence = DelegatedEvidenceSummary::default();

        assert_eq!(
            evidence_completion_decision(&evidence, false, true),
            EvidenceCompletionDecision::RequestEvidence
        );
        assert_eq!(
            evidence_completion_decision(&evidence, true, true),
            EvidenceCompletionDecision::Reject
        );
        assert_eq!(
            evidence_completion_decision(&evidence, false, false),
            EvidenceCompletionDecision::Reject
        );
    }

    #[test]
    fn successful_authorized_capability_categories_satisfy_evidence_gate() {
        for (tool, input, expected) in [
            (
                "read",
                json!({"file_path": "src/lib.rs"}),
                DelegatedEvidenceKind::Observation,
            ),
            (
                "apply_patch",
                json!({"patch": "*** Begin Patch"}),
                DelegatedEvidenceKind::Mutation,
            ),
            (
                "bash",
                json!({"command": "cargo test"}),
                DelegatedEvidenceKind::Execution,
            ),
        ] {
            assert_eq!(delegated_evidence_kind(tool, &input), Some(expected));
        }
        assert_eq!(
            delegated_evidence_kind("tool_search", &json!({"action": "search"})),
            None
        );
        assert_eq!(
            delegated_evidence_kind(
                "tool_search",
                &json!({
                    "action": "execute",
                    "tool": "bash",
                    "arguments": {"command": "cargo test"}
                })
            ),
            Some(DelegatedEvidenceKind::Execution)
        );
        assert_eq!(
            delegated_evidence_kind("agent", &json!({"action": "spawn", "agent_type": "build"})),
            Some(DelegatedEvidenceKind::Mutation)
        );

        let mut evidence = DelegatedEvidenceSummary::default();
        evidence.record_attempt();
        evidence.record_success(DelegatedEvidenceKind::Observation);
        assert_eq!(
            evidence_completion_decision(&evidence, false, true),
            EvidenceCompletionDecision::Ready
        );
    }

    #[test]
    fn delegated_cache_scope_is_shared_by_compatible_siblings() {
        let first = cache_scope_task("builder-a", "implement metrics", "parent-a");
        let second = cache_scope_task(
            "builder-b",
            "implement rendering\n\n[DIRECT DEPENDENCY EVIDENCE]\nvolatile handoff tail",
            "parent-a",
        );
        let tools = cache_scope_tools();

        let first_scope = delegated_prompt_cache_scope(
            &first,
            "openai",
            "gpt-5.6-terra",
            "stable builder prompt",
            &tools,
        );
        let second_scope = delegated_prompt_cache_scope(
            &second,
            "openai",
            "gpt-5.6-terra",
            "stable builder prompt",
            &tools,
        );

        assert_eq!(first_scope, second_scope);
        assert_eq!(first_scope.unwrap().len(), 64);
    }

    #[test]
    fn delegated_cache_scope_separates_reasoning_effort() {
        let medium = cache_scope_task("builder-a", "implement metrics", "parent-a")
            .with_reasoning_effort(Some(ReasoningEffort::Medium));
        let xhigh = cache_scope_task("builder-b", "implement metrics", "parent-a")
            .with_reasoning_effort(Some(ReasoningEffort::XHigh));
        let tools = cache_scope_tools();

        assert_ne!(
            delegated_prompt_cache_scope(
                &medium,
                "openai",
                "gpt-5.6-sol",
                "stable builder prompt",
                &tools,
            ),
            delegated_prompt_cache_scope(
                &xhigh,
                "openai",
                "gpt-5.6-sol",
                "stable builder prompt",
                &tools,
            )
        );
    }

    #[test]
    fn delegated_cache_scope_invalidates_on_safety_or_prefix_changes() {
        let base = cache_scope_task("builder-a", "implement metrics", "parent-a");
        let different_parent = cache_scope_task("builder-b", "implement metrics", "parent-b");
        let tools = cache_scope_tools();
        let scope = |task: &SubAgentTask, model: &str, system: &str, tools: &[AiTool]| {
            delegated_prompt_cache_scope(task, "openai", model, system, tools).unwrap()
        };

        assert_ne!(
            scope(&base, "gpt-5.6-terra", "stable", &tools),
            scope(&different_parent, "gpt-5.6-terra", "stable", &tools)
        );
        assert_ne!(
            scope(&base, "gpt-5.6-terra", "stable", &tools),
            scope(&base, "gpt-5.6-terra", "changed", &tools)
        );
        assert_ne!(
            scope(&base, "gpt-5.6-terra", "stable", &tools),
            scope(&base, "gpt-5.6", "stable", &tools)
        );
        assert_ne!(
            scope(&base, "gpt-5.6-terra", "stable", &tools),
            scope(&base, "gpt-5.6-terra", "stable", &[])
        );
    }

    #[test]
    fn delegated_compaction_keeps_objective_checkpoint_and_complete_tail() {
        let mut messages = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::text("original objective".to_string())],
        }];
        for index in 0..20 {
            messages.push(ModelMessage {
                role: if index % 2 == 0 {
                    Role::Assistant
                } else {
                    Role::User
                },
                content: vec![Content::text(format!("turn {index}"))],
            });
        }
        let original_len = messages.len();

        assert!(compact_delegated_history(
            &mut messages,
            &["src/lib.rs".to_string()],
            "latest finding",
            "test",
        ));
        assert!(messages.len() < original_len);
        assert!(matches!(messages[0].role, Role::User));
        assert!(matches!(messages[1].role, Role::User));
        assert!(matches!(messages[2].role, Role::Assistant));
        let checkpoint = match &messages[1].content[0] {
            Content::Text { text, .. } => text,
            _ => panic!("checkpoint must be text"),
        };
        assert!(checkpoint.contains("DELEGATED COMPACTION CHECKPOINT"));
        assert!(checkpoint.contains("src/lib.rs"));
        assert!(checkpoint.contains("latest finding"));
    }
}
