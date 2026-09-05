use std::path::{Path, PathBuf};

use anyhow::{ensure, Context};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::agent::subagent::{AgentProgress, AgentProgressStatus, SubAgentResult};
use crate::agent::DelegatedRunStage;
use crate::ai::types::{Content, ModelMessage, Role};
use crate::storage::{
    Database, DelegatedRunLease, DelegatedRunRecord, DelegatedRunScope, DelegatedRunStore,
    DelegationStore, SessionManager,
};
use crate::tools::registry::DelegationPolicy;
use crate::tools::{ToolContext, ToolResult};

/// Build the immediate response for a background agent launch.
pub(super) fn background_started_result(
    delegated_run_id: &str,
    agent_type: &str,
    name: Option<&str>,
) -> ToolResult {
    let display = name.unwrap_or(agent_type);
    let mut result = json!({
        "status": "background_started",
        "delegated_run_id": delegated_run_id,
        "name": display,
        "agent_type": agent_type,
        "message": format!(
            "Child agent '{}' started in background. Continue other work; you will be notified when it completes. Do not thrash-poll status. delegated_run_id: '{}'.",
            display, delegated_run_id
        ),
    });
    if let Some(name) = name {
        result["name"] = json!(name);
    }
    ToolResult::success_data(result)
}

/// Queue durable completion steering and notify the server completion bus so
/// the parent can wake like a background process completion.
pub(super) fn notify_child_completion(
    runtime: &crate::agent::subagent::AgentRuntimeManager,
    db_path: Option<&Path>,
    session_id: Option<&str>,
    user_id: Option<&str>,
    workspace_root: Option<&Path>,
    delegated_run_id: &str,
    _name: &str,
    success: bool,
    summary: &str,
) -> anyhow::Result<bool> {
    let Some(db_path) = db_path else {
        anyhow::bail!("background child completion has no database path");
    };
    let Some(session_id) = session_id else {
        anyhow::bail!("background child completion has no parent session");
    };
    let delegated = DelegatedRunStore::new(Database::new(db_path)?)
        .get_run(delegated_run_id)?
        .with_context(|| {
            format!("background child completion references unknown run '{delegated_run_id}'")
        })?;
    ensure!(
        delegated.parent_session_id == session_id,
        "background child completion run belongs to a different parent session"
    );
    ensure!(
        delegated.should_wake_parent(),
        "background child completion cannot publish authoritative stage {:?}",
        delegated.stage
    );
    let durable_workspaces = delegated
        .target_scope
        .iter()
        .filter(|scope| scope.kind == "workspace")
        .collect::<Vec<_>>();
    let [durable_workspace] = durable_workspaces.as_slice() else {
        anyhow::bail!("background child completion has no unique durable launch workspace");
    };
    let durable_workspace_root = PathBuf::from(&durable_workspace.path)
        .canonicalize()
        .context("canonicalizing durable child launch workspace")?;
    let supplied_workspace_root = workspace_root
        .context("background child completion has no captured workspace authority")?
        .canonicalize()
        .context("canonicalizing captured child workspace authority")?;
    ensure!(
        durable_workspace_root.starts_with(&supplied_workspace_root),
        "background child completion launch workspace escapes captured workspace authority"
    );
    let event = crate::agent::subagent::ChildCompletionEvent::from_durable_run(
        &delegated,
        user_id.map(ToOwned::to_owned),
    )?;
    let authoritative_success = event.success;
    ensure!(
        success == authoritative_success,
        "background child completion outcome disagrees with durable stage {:?}",
        delegated.stage
    );
    let authoritative_summary = delegated
        .human_review
        .as_deref()
        .context("background child completion has no durable review summary")?;
    ensure!(
        summary == authoritative_summary,
        "background child completion summary disagrees with its durable result"
    );

    let pending_id = event.pending_id.clone();
    let group_store = DelegationStore::new(Database::new(db_path)?);
    if group_store.get_group(delegated_run_id)?.is_some() {
        ensure!(
            group_store.authorize_parent_continuation(delegated_run_id, &pending_id)?,
            "background child completion group does not authorize this parent continuation"
        );
    }
    let content_json = serde_json::to_string(&event.content)?;
    let session_manager = SessionManager::new(Database::new(db_path)?);
    let queued =
        session_manager.queue_pending_steering_once(session_id, &pending_id, &content_json)?;
    if !queued {
        let Some(durable_content_json) =
            session_manager.load_pending_steering(session_id, &pending_id)?
        else {
            debug!(
                session_id,
                delegated_run_id, pending_id, "Child completion was already promoted"
            );
            return Ok(false);
        };
        ensure!(
            durable_content_json == content_json,
            "existing durable child completion content does not match the authoritative result"
        );
        debug!(
            session_id,
            delegated_run_id,
            pending_id,
            "Re-emitting wake for an existing pending child completion"
        );
    }
    if group_store.get_group(delegated_run_id)?.is_some() {
        ensure!(
            group_store.mark_parent_continuation_queued(delegated_run_id, &pending_id)?,
            "background child completion group lost its continuation queue fence"
        );
    }

    if let Err(event) = runtime.notify_completion(event) {
        let event = *event;
        let pending_id = event.pending_id.clone();
        let delegated_run_id = event.delegated_run_id;
        match runtime.request_completion_reconciliation(delegated_run_id.clone()) {
            Ok(()) => {
                warn!(
                    session_id,
                    delegated_run_id,
                    pending_id,
                    "Live completion listener closed; scheduled durable child wake reconciliation"
                );
            }
            Err(delegated_run_id) => {
                warn!(
                    session_id,
                    delegated_run_id,
                    pending_id,
                    "Child completion is durable but no live wake listener accepted it; startup recovery must resume the parent"
                );
            }
        }
    }
    Ok(true)
}
// ---------------------------------------------------------------------------
// Helper functions (ported from explore.rs and build.rs)
// ---------------------------------------------------------------------------

pub(super) struct SingleAgentArtifact {
    pub(super) payload: Value,
    pub(super) review_summary: String,
    pub(super) final_stage: DelegatedRunStage,
}

pub(super) fn build_single_agent_artifact(
    delegated_run_id: &str,
    result: &SubAgentResult,
    delegation_policy: &DelegationPolicy,
) -> SingleAgentArtifact {
    let usable = result.has_usable_evidence();
    let complete = result.success
        && result.termination == crate::agent::subagent::SubAgentTermination::Completed
        && usable
        && result.objective_status() == crate::agent::subagent::TaskObjectiveStatus::Complete;
    let degraded =
        usable && (result.termination.is_degraded_interruption() || result.is_degraded_success());
    let cancelled = result.termination == crate::agent::subagent::SubAgentTermination::Cancelled;
    let outcome_reason = result.outcome_reason();
    let mut agent = result.evidence_json();
    agent["success"] = json!(complete);
    agent["usable_evidence"] = json!(usable);
    agent["degraded_success"] = json!(degraded);
    agent["outcome_reason"] = json!(outcome_reason);
    let review_summary = bounded_review_summary(result);
    let payload = json!({
        "delegated_run_id": delegated_run_id,
        "findings": result.output,
        "files_examined": result.files_examined,
        "files_examined_count": result.files_examined.len(),
        "paths_examined": result.files_examined,
        "paths_examined_count": result.files_examined.len(),
        "duration_ms": result.duration_ms,
        "turns_used": result.turns_used,
        "success": complete,
        "outcome": if complete { "success" } else if degraded { "partial" } else if cancelled { "cancelled" } else { "failed" },
        "outcome_reason": outcome_reason,
        "termination": result.termination,
        "next_action_hint": if complete {
            "Synthesize and act on this delegated evidence. Do not repeat the same broad manual investigation."
        } else if degraded {
            "Continue from the retained evidence and close the interrupted provider response without repeating completed work."
        } else if cancelled {
            "The cancellation was acknowledged after in-flight governed work reached a quiescent boundary. Inspect retained evidence before deciding whether to resume."
        } else {
            "Report the child failure and choose a materially different next step."
        },
        "agent_count": 1,
        "successful_agents": if complete { 1 } else { 0 },
        "usable_agents": if usable { 1 } else { 0 },
        "degraded_agents": if degraded { 1 } else { 0 },
        "failed_agents": if complete || degraded || cancelled { 0 } else { 1 },
        "agents": [agent],
        "background_processes": result.background_processes,
        "delegation_policy": delegation_policy.audit_json(),
    });

    SingleAgentArtifact {
        payload,
        review_summary,
        final_stage: single_agent_final_stage(complete, degraded, cancelled),
    }
}

fn bounded_review_summary(result: &SubAgentResult) -> String {
    const MAX_BYTES: usize = 1_200;
    const HEAD_BYTES: usize = 400;
    const TAIL_BYTES: usize = 700;

    let output = result.output.trim();
    if output.is_empty() {
        return result
            .error
            .clone()
            .unwrap_or_else(|| "Delegated child produced no summary.".to_string());
    }
    if output.len() <= MAX_BYTES {
        return output.to_string();
    }

    let head = truncate_utf8(output, HEAD_BYTES).trim_end();
    let tail_start = output.len().saturating_sub(TAIL_BYTES);
    let mut tail_start = tail_start;
    while tail_start < output.len() && !output.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    format!("{head}\n…\n{}", output[tail_start..].trim_start())
}

pub(super) fn single_agent_final_stage(
    complete: bool,
    degraded: bool,
    cancelled: bool,
) -> DelegatedRunStage {
    if complete {
        DelegatedRunStage::Complete
    } else if degraded {
        DelegatedRunStage::Degraded
    } else if cancelled {
        DelegatedRunStage::Cancelled
    } else {
        DelegatedRunStage::Failed
    }
}

pub(super) fn persist_single_agent_artifact(
    store: &DelegatedRunStore,
    delegated_run_id: &str,
    artifact: &SingleAgentArtifact,
    resumable: bool,
    error_message: &str,
) -> anyhow::Result<DelegatedRunRecord> {
    persist_delegated_artifact(
        store,
        delegated_run_id,
        artifact.final_stage,
        &artifact.payload,
        &artifact.review_summary,
        resumable,
    )
    .with_context(|| error_message.to_string())
}

pub(super) fn persist_background_single_agent_artifact(
    lease: &DelegatedRunLease,
    delegated_run_id: &str,
    artifact: &SingleAgentArtifact,
    resumable: bool,
    agent_type: &str,
) -> anyhow::Result<DelegatedRunRecord> {
    persist_background_delegated_artifact(
        lease,
        delegated_run_id,
        artifact.final_stage,
        &artifact.payload,
        &artifact.review_summary,
        resumable,
    )
    .with_context(|| format!("Background {agent_type}: failed to persist final artifact"))
}

pub(super) fn persist_background_delegated_artifact(
    lease: &DelegatedRunLease,
    delegated_run_id: &str,
    final_stage: DelegatedRunStage,
    payload: &Value,
    review_summary: &str,
    resumable: bool,
) -> anyhow::Result<DelegatedRunRecord> {
    ensure_terminal_stage(delegated_run_id, final_stage)?;
    lease.finalize_background_run(
        delegated_run_id,
        final_stage,
        payload,
        Some(review_summary),
        resumable,
    )?;
    reload_and_validate_delegated_artifact(
        lease,
        delegated_run_id,
        final_stage,
        payload,
        review_summary,
    )
}

/// Finalize a delegated result and reload the row that actually won. The
/// storage layer intentionally returns `Ok(())` to every terminal contender
/// once one winner is durable, so callers must not trust their stale in-memory
/// outcome until this authoritative comparison passes.
pub(super) fn persist_delegated_artifact(
    store: &DelegatedRunStore,
    delegated_run_id: &str,
    final_stage: DelegatedRunStage,
    payload: &Value,
    review_summary: &str,
    resumable: bool,
) -> anyhow::Result<DelegatedRunRecord> {
    ensure_terminal_stage(delegated_run_id, final_stage)?;
    store.finalize_run(
        delegated_run_id,
        final_stage,
        payload,
        Some(review_summary),
        resumable,
    )?;
    reload_and_validate_delegated_artifact(
        store,
        delegated_run_id,
        final_stage,
        payload,
        review_summary,
    )
}

fn ensure_terminal_stage(
    delegated_run_id: &str,
    final_stage: DelegatedRunStage,
) -> anyhow::Result<()> {
    ensure!(
        matches!(
            final_stage,
            DelegatedRunStage::Complete
                | DelegatedRunStage::Degraded
                | DelegatedRunStage::Failed
                | DelegatedRunStage::Cancelled
        ),
        "delegated run '{delegated_run_id}' cannot publish non-terminal stage {final_stage:?}"
    );
    Ok(())
}

fn reload_and_validate_delegated_artifact(
    store: &DelegatedRunStore,
    delegated_run_id: &str,
    final_stage: DelegatedRunStage,
    payload: &Value,
    review_summary: &str,
) -> anyhow::Result<DelegatedRunRecord> {
    let record = store.get_run(delegated_run_id)?.with_context(|| {
        format!("delegated run '{delegated_run_id}' disappeared after finalization")
    })?;
    ensure!(
        record.stage == final_stage,
        "delegated run '{delegated_run_id}' authoritative stage is {:?}, not {:?}",
        record.stage,
        final_stage
    );
    ensure!(
        record.artifact.as_ref() == Some(payload),
        "delegated run '{delegated_run_id}' authoritative artifact does not match the completed child"
    );
    ensure!(
        record.human_review.as_deref() == Some(review_summary),
        "delegated run '{delegated_run_id}' authoritative review does not match the completed child"
    );
    ensure!(
        record.completed_at.is_some(),
        "delegated run '{delegated_run_id}' has no durable completion timestamp"
    );
    Ok(record)
}

pub(super) fn delegated_persistence_error(
    delegated_run_id: &str,
    artifact: Value,
    error: &anyhow::Error,
) -> ToolResult {
    ToolResult::error_with_details(
        "agent_persistence_error",
        format!(
            "Delegated work produced a result, but its terminal state was not durably finalized: {error}"
        ),
        Some(json!({
            "delegated_run_id": delegated_run_id,
            "unpersisted_result": artifact,
        })),
        None,
    )
}

pub(super) fn existing_continuation_error(
    resumed_from_run_id: &str,
    delegated_run_id: &str,
) -> ToolResult {
    ToolResult::error_with_details(
        "agent_continuation_exists",
        format!(
            "Delegated run '{resumed_from_run_id}' already has durable continuation '{delegated_run_id}'; the duplicate continuation was not started."
        ),
        Some(json!({
            "resumed_from_run_id": resumed_from_run_id,
            "existing_delegated_run_id": delegated_run_id,
        })),
        None,
    )
}

pub(super) fn agent_progress_for_terminal_stage(
    stage: DelegatedRunStage,
) -> (AgentProgressStatus, Option<String>) {
    let status = match stage {
        DelegatedRunStage::Complete => AgentProgressStatus::Complete,
        DelegatedRunStage::Degraded => AgentProgressStatus::Degraded,
        DelegatedRunStage::Cancelled => AgentProgressStatus::Cancelled,
        DelegatedRunStage::Created
        | DelegatedRunStage::Running
        | DelegatedRunStage::Synthesizing
        | DelegatedRunStage::Failed => AgentProgressStatus::Failed,
    };
    let current_action = match stage {
        DelegatedRunStage::Degraded => Some("degraded".to_string()),
        DelegatedRunStage::Cancelled => Some("cancelled".to_string()),
        _ => None,
    };
    (status, current_action)
}

pub(super) fn emit_single_agent_completion(
    progress_tx: &Option<mpsc::UnboundedSender<AgentProgress>>,
    delegated_run_id: &str,
    agent_type: &str,
    result: &SubAgentResult,
    authoritative_stage: DelegatedRunStage,
    review_summary: &str,
) {
    if let Some(tx) = progress_tx {
        let (status, current_action) = agent_progress_for_terminal_stage(authoritative_stage);
        if tx
            .send(AgentProgress {
                delegated_run_id: Some(delegated_run_id.to_string()),
                task_id: result.task_id.clone(),
                name: agent_type.to_string(),
                identity: None,
                status,
                tool_count: 0,
                tokens: 0,
                current_action,
                completion_summary: Some(review_summary.to_string()),
                lines_added: 0,
                lines_removed: 0,
                completed_plan_task: None,
                conversation_event: None,
            })
            .is_err()
        {
            debug!(
                "Background {} progress channel disconnected (parent returned)",
                agent_type
            );
        }
    }
}

pub(super) fn build_single_agent_warnings(result: &SubAgentResult, action: &str) -> Vec<String> {
    if result.success {
        return Vec::new();
    }

    match result.error.as_deref() {
        Some(err) => vec![format!("{} failed: {}", action, err)],
        None => vec![format!("{} completed without usable results.", action)],
    }
}

pub(super) fn concise_target_label(target: &str, index: usize) -> String {
    let trimmed = target.trim_matches('/');
    if trimmed.is_empty() {
        return format!("target-{}", index + 1);
    }

    let parts = trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    if parts.len() >= 2 {
        return format!("{}/{}", parts[parts.len() - 2], parts[parts.len() - 1]);
    }

    parts[0].to_string()
}

pub(super) fn resolve_explore_target(
    target: &str,
    project_dir: &Path,
    kind: &str,
) -> Result<PathBuf, String> {
    let candidate = if Path::new(target).is_absolute() {
        PathBuf::from(target)
    } else {
        project_dir.join(target)
    };

    let canonical = candidate.canonicalize().map_err(|_| {
        format!(
            "Missing explore target: {} '{}' does not exist under project root",
            kind, target
        )
    })?;

    let project_root = project_dir
        .canonicalize()
        .map_err(|err| format!("Invalid project root '{}': {}", project_dir.display(), err))?;

    if !canonical.starts_with(&project_root) {
        return Err(format!(
            "Invalid explore target: {} '{}' is outside project root",
            kind, target
        ));
    }

    match kind {
        "directory" if !canonical.is_dir() => Err(format!(
            "Invalid explore target: directory '{}' resolved to a non-directory path",
            target
        )),
        "file" if !canonical.is_file() => Err(format!(
            "Invalid explore target: file '{}' resolved to a non-file path",
            target
        )),
        _ => Ok(canonical),
    }
}

pub(super) fn relative_label(path: &Path, project_dir: &Path) -> String {
    path.strip_prefix(project_dir)
        .unwrap_or(path)
        .display()
        .to_string()
}

pub(super) fn delegated_workspace_scope(project_dir: &Path) -> Result<DelegatedRunScope, String> {
    let canonical = project_dir.canonicalize().map_err(|error| {
        format!(
            "Could not resolve delegated launch workspace '{}': {error}",
            project_dir.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!(
            "Delegated launch workspace '{}' is not a directory",
            canonical.display()
        ));
    }
    Ok(DelegatedRunScope {
        label: "launch workspace".to_string(),
        path: canonical.display().to_string(),
        kind: "workspace".to_string(),
    })
}

pub(super) fn delegated_scope(
    label: &str,
    path: &Path,
    kind: &str,
    project_dir: &Path,
) -> DelegatedRunScope {
    DelegatedRunScope {
        label: label.to_string(),
        path: relative_label(path, project_dir),
        kind: kind.to_string(),
    }
}

pub(super) fn open_delegated_run_store(ctx: &ToolContext) -> Option<DelegatedRunStore> {
    let db_path = ctx.db_path.as_ref()?;
    match Database::new(db_path) {
        Ok(db) => Some(DelegatedRunStore::new(db)),
        Err(error) => {
            warn!(
                session_id = ?ctx.session_id,
                tool_use_id = ?ctx.tool_use_id,
                db_path = %db_path.display(),
                error = %error,
                "Failed to open delegated run store"
            );
            None
        }
    }
}

pub(super) fn build_resume_seed(
    previous: &DelegatedRunRecord,
    target_label: &str,
) -> Option<String> {
    let artifact = previous.artifact.as_ref()?;
    let agent_artifact = artifact
        .get("agents")
        .and_then(|value| value.as_array())
        .and_then(|agents| {
            agents.iter().find(|entry| {
                entry
                    .get("agent")
                    .and_then(|value| value.as_str())
                    .is_some_and(|agent| agent == target_label)
            })
        });

    let summary = agent_artifact
        .and_then(|entry| entry.get("summary").and_then(|value| value.as_str()))
        .or(previous.human_review.as_deref())
        .or_else(|| {
            artifact
                .get("investigation_summary")
                .and_then(|value| value.as_str())
        })?;

    let paths = agent_artifact
        .and_then(|entry| {
            entry
                .get("paths_examined")
                .and_then(|value| value.as_array())
        })
        .map(|paths| {
            paths
                .iter()
                .filter_map(|value| value.as_str())
                .take(8)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|value| !value.trim().is_empty());

    let gaps = agent_artifact
        .and_then(|entry| entry.get("gaps").and_then(|value| value.as_array()))
        .map(|gaps| {
            gaps.iter()
                .filter_map(|value| value.as_str())
                .take(4)
                .collect::<Vec<_>>()
                .join("; ")
        })
        .filter(|value| !value.trim().is_empty());

    let mut lines = vec![format!(
        "Previous delegated run {} already covered this target.",
        previous.delegated_run_id
    )];
    lines.push(format!(
        "Reuse and extend this prior evidence: {}",
        summary.trim()
    ));
    if let Some(paths) = paths {
        lines.push(format!("Previously examined paths: {}", paths));
    }
    if let Some(gaps) = gaps {
        lines.push(format!("Previously identified gaps: {}", gaps));
    }
    lines.push(
        "Do not remap the same directory structure from scratch unless it is required to close a concrete gap."
            .to_string(),
    );
    Some(lines.join("\n"))
}

/// Build a brief summary of the parent conversation for context injection.
///
/// Extracts the last `max_turns` user/assistant messages and formats them as a
/// `[PARENT CONTEXT]` block that the sub-agent can use for upstream awareness.
pub(super) fn build_parent_context_brief(
    conversation: &[ModelMessage],
    max_turns: usize,
) -> String {
    let relevant: Vec<_> = conversation
        .iter()
        .filter(|msg| msg.role == Role::User || msg.role == Role::Assistant)
        .collect();

    let start = relevant.len().saturating_sub(max_turns);
    let window = &relevant[start..];

    if window.is_empty() {
        return String::new();
    }

    let mut lines = vec!["[PARENT CONTEXT]".to_string()];
    for msg in window {
        let role_label = match msg.role {
            Role::User => "User",
            Role::Assistant => "Assistant",
            _ => continue,
        };

        let text = msg
            .content
            .iter()
            .filter_map(|c| match c {
                Content::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ");

        if text.is_empty() {
            continue;
        }

        let truncated = if text.len() > 200 {
            let mut end = 200;
            while end > 0 && !text.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}...", &text[..end])
        } else {
            text
        };

        lines.push(format!("{}: {}", role_label, truncated));
    }
    lines.push("[/PARENT CONTEXT]".to_string());

    lines.join("\n")
}

// Build-specific helper functions

pub(super) fn classify_build_outcome(
    results_len: usize,
    failed_agents: usize,
    modified_files: usize,
) -> &'static str {
    if results_len == 0 || modified_files == 0 {
        return "failed";
    }
    if failed_agents == 0 {
        return "success";
    }
    "partial"
}

pub(super) fn build_confidence(failed_agents: usize, modified_files: usize) -> &'static str {
    if modified_files == 0 {
        return "low";
    }
    if failed_agents == 0 {
        return "high";
    }
    "medium"
}

pub(super) fn build_investigation_summary(
    successful_builders: usize,
    degraded_builders: usize,
    cancelled_builders: usize,
    failed_builders: usize,
    modified_files: usize,
    lines_added: usize,
    lines_removed: usize,
) -> String {
    let mut parts = vec![format!(
        "Delegated build settled with {} successful builders across {} modified files.",
        successful_builders, modified_files
    )];
    parts.push(format!(
        "Net code movement: +{} / -{} lines.",
        lines_added, lines_removed
    ));
    if degraded_builders > 0 {
        parts.push(format!(
            "{} builders retained usable evidence but ended with an interrupted provider response; treat their output as partial.",
            degraded_builders
        ));
    }
    if cancelled_builders > 0 {
        parts.push(format!(
            "{} builders acknowledged cancellation at a quiescent boundary; inspect any retained mutation evidence before resuming.",
            cancelled_builders
        ));
    }
    if failed_builders > 0 {
        parts.push(format!(
            "{} builders reported errors and should be reviewed before accepting the build output.",
            failed_builders
        ));
    }
    parts.join(" ")
}

pub(super) fn build_coverage_gap_notice(errors: &[String]) -> Option<String> {
    if errors.is_empty() {
        None
    } else {
        Some(format!(
            "These builders failed or reported issues: {}",
            errors.join(" | ")
        ))
    }
}

/// Truncate a string to at most `max_bytes` bytes without splitting a UTF-8
/// character. Returns the longest prefix that fits within the byte budget.
pub(super) fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
