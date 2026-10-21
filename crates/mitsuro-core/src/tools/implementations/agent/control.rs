use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::json;

use crate::agent::subagent::AgentCapability;
use crate::agent::DelegatedRunStage;
use crate::storage::{DelegatedRunRecord, DelegatedRunRole};
use crate::tools::{ToolContext, ToolResult};

use super::{open_delegated_run_store, truncate_utf8, AgentAction, AgentTool, Params};

const MAX_RESUME_EVIDENCE_CHARS: usize = 6_000;
const MAX_RESUME_DETAIL_CHARS: usize = 2_000;
const INTERRUPT_ACK_WAIT: Duration = Duration::from_secs(2);

fn session_id(ctx: &ToolContext) -> Result<&str, ToolResult> {
    ctx.session_id.as_deref().ok_or_else(|| {
        ToolResult::error_with_code(
            "agent_session_required",
            "Agent lifecycle actions require a persisted parent session.",
        )
    })
}

fn load_owned_run(
    ctx: &ToolContext,
    delegated_run_id: &str,
) -> Result<DelegatedRunRecord, ToolResult> {
    let parent_session_id = session_id(ctx)?;
    let store = open_delegated_run_store(ctx).ok_or_else(|| {
        ToolResult::error_with_code(
            "agent_store_unavailable",
            "Delegated run storage is unavailable for this session.",
        )
    })?;
    let record = store
        .get_run(delegated_run_id)
        .map_err(|error| ToolResult::error_with_code("agent_store_error", error.to_string()))?
        .ok_or_else(|| {
            ToolResult::error_with_code(
                "agent_run_not_found",
                format!("Delegated run '{delegated_run_id}' was not found."),
            )
        })?;
    if record.parent_session_id != parent_session_id {
        return Err(ToolResult::error_with_code(
            "agent_run_not_found",
            format!("Delegated run '{delegated_run_id}' was not found."),
        ));
    }
    Ok(record)
}

fn required_run_id(params: &Params) -> Result<&str, ToolResult> {
    params
        .delegated_run_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ToolResult::error_with_code(
                "delegated_run_id_required",
                "This agent lifecycle action requires delegated_run_id.",
            )
        })
}

fn is_terminal(stage: DelegatedRunStage) -> bool {
    matches!(
        stage,
        DelegatedRunStage::Complete
            | DelegatedRunStage::Degraded
            | DelegatedRunStage::Failed
            | DelegatedRunStage::Cancelled
    )
}

fn role_profile(role: &DelegatedRunRole) -> &'static str {
    match role {
        DelegatedRunRole::Explore => "explore",
        DelegatedRunRole::Build => "build",
        DelegatedRunRole::Planner => "plan",
        DelegatedRunRole::Verifier => "verify",
    }
}

fn should_resume_terminal_followup(action: AgentAction, record: &DelegatedRunRecord) -> bool {
    action == AgentAction::Followup && is_terminal(record.stage)
}

fn apply_persisted_child_contract(
    params: &mut Params,
    record: &DelegatedRunRecord,
    delegated_run_id: &str,
    current_project_dir: Option<&Path>,
) -> Result<(), ToolResult> {
    let has_explicit_contract = !record.capabilities.is_empty();
    params.profile = Some(if has_explicit_contract {
        "child".to_string()
    } else {
        role_profile(&record.role).to_string()
    });
    params.agent_type = None;
    params.capabilities = record
        .effective_capabilities()
        .into_iter()
        .map(|capability| match capability {
            AgentCapability::Read => "read".to_string(),
            AgentCapability::Write => "write".to_string(),
            AgentCapability::Execute => "execute".to_string(),
        })
        .collect();

    if params.name.is_none() {
        params.name = record.child_name.clone().or_else(|| {
            Some(format!(
                "resume-{}",
                &delegated_run_id[..delegated_run_id.len().min(8)]
            ))
        });
    }

    let origin_scopes = record
        .target_scope
        .iter()
        .filter(|scope| scope.kind == "workspace")
        .collect::<Vec<_>>();
    let [origin_scope] = origin_scopes.as_slice() else {
        return Err(ToolResult::error_with_code(
            "agent_resume_workspace_invalid",
            format!(
                "Delegated run '{delegated_run_id}' has no unique persisted launch workspace and cannot be resumed safely. Spawn a new child instead."
            ),
        ));
    };
    validate_resume_workspace(current_project_dir, &origin_scope.path, delegated_run_id)?;

    let target_scopes = record
        .target_scope
        .iter()
        .filter(|scope| scope.kind != "workspace")
        .collect::<Vec<_>>();
    let components = target_scopes
        .iter()
        .filter(|scope| scope.kind == "component")
        .map(|scope| scope.path.clone())
        .collect::<Vec<_>>();
    let primary_scopes = target_scopes
        .iter()
        .filter(|scope| scope.kind != "component")
        .copied()
        .collect::<Vec<_>>();

    if !primary_scopes.is_empty() && primary_scopes.iter().all(|scope| scope.kind == "task") {
        return Err(ToolResult::error_with_recovery(
            "agent_resume_task_graph_unsupported",
            format!(
                "Delegated run '{delegated_run_id}' is a completed task graph. Its reports are available through agent status and do not need a followup. Spawn a new task graph to perform additional work."
            ),
            false,
            "Use agent status to read the existing terminal artifact. Spawn a new graph only when additional work is actually required.",
            vec!["Do not resume or follow up merely to retrieve completed reports.".to_string()],
            Some(serde_json::json!({
                "delegated_run_id": delegated_run_id,
                "read_action": "status",
                "new_work_action": "spawn"
            })),
        ));
    }

    match (primary_scopes.as_slice(), components.as_slice()) {
        ([], []) => {
            return Err(ToolResult::error_with_code(
                "agent_resume_target_invalid",
                format!(
                    "Delegated run '{delegated_run_id}' has no persisted target scope and cannot be resumed safely. Spawn a new child instead."
                ),
            ));
        }
        ([scope], []) if matches!(scope.kind.as_str(), "directory" | "file") => {
            reject_conflicting_components(params, delegated_run_id)?;
            restore_primary_scope(params, delegated_run_id, scope)?;
        }
        ([scope], []) if scope.kind == "project" => {
            reject_conflicting_components(params, delegated_run_id)?;
            restore_primary_scope(params, delegated_run_id, scope)?;
        }
        ([], components) if !components.is_empty() => {
            reject_conflicting_scope(params, delegated_run_id)?;
            restore_exact_components(params, delegated_run_id, components)?;
        }
        ([scope], [component])
            if matches!(scope.kind.as_str(), "directory" | "file" | "project") =>
        {
            restore_primary_scope(params, delegated_run_id, scope)?;
            restore_exact_components(params, delegated_run_id, std::slice::from_ref(component))?;
        }
        _ => {
            return Err(ToolResult::error_with_code(
                "agent_resume_target_invalid",
                format!(
                    "Delegated run '{delegated_run_id}' has a legacy mixed target scope that cannot be resumed safely. Spawn a new child to retarget the work."
                ),
            ));
        }
    }

    Ok(())
}

fn validate_resume_workspace(
    current_project_dir: Option<&Path>,
    persisted_project_dir: &str,
    delegated_run_id: &str,
) -> Result<(), ToolResult> {
    let current = current_project_dir.ok_or_else(|| {
        ToolResult::error_with_code(
            "agent_resume_workspace_required",
            "Select the delegated run's original project before resuming it.",
        )
    })?;
    let canonical_current = canonical_workspace(current).map_err(|error| {
        ToolResult::error_with_code(
            "agent_resume_workspace_invalid",
            format!("Could not resolve the current project workspace: {error}"),
        )
    })?;
    let canonical_persisted =
        canonical_workspace(Path::new(persisted_project_dir)).map_err(|error| {
            ToolResult::error_with_code(
                "agent_resume_workspace_invalid",
                format!(
                    "Delegated run '{delegated_run_id}' launch workspace is unavailable: {error}"
                ),
            )
        })?;
    if canonical_current != canonical_persisted {
        return Err(ToolResult::error_with_code(
            "agent_resume_workspace_mismatch",
            format!(
                "Delegated run '{delegated_run_id}' belongs to '{}', but this session currently targets '{}'. Switch back to the original project or spawn a new child.",
                canonical_persisted.display(),
                canonical_current.display(),
            ),
        ));
    }
    Ok(())
}

fn canonical_workspace(path: &Path) -> std::io::Result<PathBuf> {
    path.canonicalize()
}

fn restore_primary_scope(
    params: &mut Params,
    delegated_run_id: &str,
    scope: &crate::storage::DelegatedRunScope,
) -> Result<(), ToolResult> {
    if let Some(requested_scope) = params.scope.as_deref() {
        if normalize_target(requested_scope) != normalize_target(&scope.path) {
            return Err(resume_target_conflict(delegated_run_id));
        }
    }
    params.scope = if normalize_target(&scope.path).is_empty() {
        None
    } else {
        Some(scope.path.clone())
    };
    Ok(())
}

fn normalize_target(value: &str) -> &str {
    let trimmed = value.trim().trim_end_matches('/');
    let normalized = trimmed.strip_prefix("./").unwrap_or(trimmed);
    if normalized == "." {
        ""
    } else {
        normalized
    }
}

fn resume_target_conflict(delegated_run_id: &str) -> ToolResult {
    ToolResult::error_with_code(
        "agent_resume_target_conflict",
        format!(
            "Resume must keep delegated run '{delegated_run_id}' on its persisted target. Spawn a new child to use a different scope or component set."
        ),
    )
}

fn reject_conflicting_scope(params: &Params, delegated_run_id: &str) -> Result<(), ToolResult> {
    if params
        .scope
        .as_deref()
        .is_some_and(|scope| !scope.trim().is_empty())
    {
        return Err(resume_target_conflict(delegated_run_id));
    }
    Ok(())
}

fn reject_conflicting_components(
    params: &Params,
    delegated_run_id: &str,
) -> Result<(), ToolResult> {
    if params
        .components
        .as_ref()
        .is_some_and(|components| !components.is_empty())
    {
        return Err(resume_target_conflict(delegated_run_id));
    }
    Ok(())
}

fn restore_exact_components(
    params: &mut Params,
    delegated_run_id: &str,
    components: &[String],
) -> Result<(), ToolResult> {
    if let Some(requested) = params.components.as_ref() {
        let requested = requested
            .iter()
            .map(|component| component.trim())
            .collect::<Vec<_>>();
        let persisted = components
            .iter()
            .map(|component| component.trim())
            .collect::<Vec<_>>();
        if requested != persisted {
            return Err(resume_target_conflict(delegated_run_id));
        }
    }
    params.components = Some(components.to_vec());
    Ok(())
}

fn compact_durable_resume_evidence(record: &DelegatedRunRecord) -> String {
    let mut lines = vec![format!(
        "Prior delegated run: {} (stage: {:?})",
        record.delegated_run_id, record.stage
    )];
    if let Some(review) = record
        .human_review
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        lines.push(format!(
            "Human review: {}",
            truncate_utf8(review, MAX_RESUME_DETAIL_CHARS)
        ));
    }
    if let Some(artifact) = record.artifact.as_ref() {
        let outcome = artifact
            .get("outcome")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !outcome.is_empty() {
            lines.push(format!("Outcome: {outcome}"));
        }
        if let Some(detail) = ["investigation_summary", "findings", "message"]
            .iter()
            .find_map(|key| artifact.get(key).and_then(serde_json::Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            lines.push(format!(
                "Evidence: {}",
                truncate_utf8(detail, MAX_RESUME_DETAIL_CHARS)
            ));
        }
        let paths = artifact
            .get("paths_examined")
            .or_else(|| artifact.get("files_examined"))
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .take(12)
                    .map(|path| truncate_utf8(path, 240))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !paths.is_empty() {
            lines.push(format!("Examined paths: {}", paths.join(", ")));
        }
        let errors = artifact
            .get("errors")
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .take(6)
                    .map(|error| truncate_utf8(error, 400))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !errors.is_empty() {
            lines.push(format!("Known errors: {}", errors.join("; ")));
        }
        if let Some(hint) = artifact
            .get("next_action_hint")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            lines.push(format!("Next action: {}", truncate_utf8(hint, 800)));
        }
    }
    if lines.len() == 1 {
        lines.push("No prior artifact was persisted.".to_string());
    }
    truncate_utf8(&lines.join("\n"), MAX_RESUME_EVIDENCE_CHARS).to_string()
}

impl AgentTool {
    async fn execute_resume_from_record(
        &self,
        mut params: Params,
        ctx: &ToolContext,
        delegated_run_id: String,
        record: DelegatedRunRecord,
    ) -> ToolResult {
        if self.runtime.contains(&delegated_run_id) {
            return ToolResult::error_with_code(
                "agent_run_still_live",
                format!(
                    "Delegated run '{delegated_run_id}' is still owned by this process. Send it a message, wait for completion, or interrupt it before resuming."
                ),
            );
        }
        if !record.resumable {
            return ToolResult::error_with_code(
                "agent_run_not_resumable",
                format!("Delegated run '{delegated_run_id}' is not resumable."),
            );
        }

        let prior_evidence = compact_durable_resume_evidence(&record);
        let requested_objective = params.prompt.trim();
        params.prompt = if requested_objective.is_empty() {
            format!(
                "Resume delegated run {delegated_run_id}. Continue from durable evidence and close remaining gaps.\n\n[PRIOR EVIDENCE]\n{prior_evidence}\n[/PRIOR EVIDENCE]"
            )
        } else {
            format!(
                "{requested_objective}\n\nResume delegated run {delegated_run_id}.\n[PRIOR EVIDENCE]\n{prior_evidence}\n[/PRIOR EVIDENCE]"
            )
        };
        params.action = AgentAction::Spawn;
        params.delegated_run_id = None;
        params.run_in_background = Some(
            params
                .run_in_background
                .unwrap_or_else(|| self.runtime.has_completion_listener()),
        );
        if let Err(error) = apply_persisted_child_contract(
            &mut params,
            &record,
            &delegated_run_id,
            ctx.project_dir.as_deref(),
        ) {
            return error;
        }
        params.resumed_from_run_id = Some(delegated_run_id.clone());
        self.execute_spawn(params, ctx).await
    }

    pub(super) async fn execute_control(
        &self,
        mut params: Params,
        ctx: &ToolContext,
    ) -> ToolResult {
        match params.action {
            AgentAction::Spawn => self.execute_spawn(params, ctx).await,
            AgentAction::List => {
                let parent_session_id = match session_id(ctx) {
                    Ok(value) => value,
                    Err(error) => return error,
                };
                let store = match open_delegated_run_store(ctx) {
                    Some(store) => store,
                    None => {
                        return ToolResult::error_with_code(
                            "agent_store_unavailable",
                            "Delegated run storage is unavailable for this session.",
                        )
                    }
                };
                let limit = params.limit.unwrap_or(20).clamp(1, 100);
                match store.list_runs_for_session(parent_session_id, limit) {
                    Ok(runs) => ToolResult::success_data(json!({
                        "runs": runs,
                        "live": self.runtime.snapshots_for_session(parent_session_id),
                    })),
                    Err(error) => {
                        ToolResult::error_with_code("agent_store_error", error.to_string())
                    }
                }
            }
            AgentAction::Status => {
                let delegated_run_id = match required_run_id(&params) {
                    Ok(value) => value,
                    Err(error) => return error,
                };
                match load_owned_run(ctx, delegated_run_id) {
                    Ok(run) => {
                        let live = self
                            .runtime
                            .snapshots()
                            .into_iter()
                            .find(|snapshot| snapshot.delegated_run_id == delegated_run_id);
                        ToolResult::success_data(json!({"run": run, "live": live}))
                    }
                    Err(error) => error,
                }
            }
            AgentAction::Wait => {
                if self.runtime.has_completion_listener() {
                    return ToolResult::error_with_code(
                        "agent_wait_unavailable_in_hosted_runtime",
                        "Hosted Chat/Code uses durable completion wake. Finish the foreground response normally; do not wait or poll.",
                    );
                }
                let delegated_run_id = match required_run_id(&params) {
                    Ok(value) => value.to_string(),
                    Err(error) => return error,
                };
                let timeout =
                    Duration::from_millis(params.wait_timeout_ms.unwrap_or(30_000).min(300_000));
                let started = Instant::now();
                loop {
                    let run = match load_owned_run(ctx, &delegated_run_id) {
                        Ok(run) => run,
                        Err(error) => return error,
                    };
                    if is_terminal(run.stage) || started.elapsed() >= timeout {
                        let terminal = is_terminal(run.stage);
                        let next_action = if terminal {
                            "Inspect the terminal evidence and integrate it into the parent outcome."
                        } else if self.runtime.has_completion_listener() {
                            "Finish the foreground response normally; do not poll. Durable completion will wake this session."
                        } else {
                            "Wait again only when explicitly synchronous monitoring is still required."
                        };
                        return ToolResult::success_data(json!({
                            "terminal": terminal,
                            "timed_out": !terminal,
                            "next_action": next_action,
                            "run": run,
                        }));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            AgentAction::Message | AgentAction::Followup => {
                let delegated_run_id = match required_run_id(&params) {
                    Ok(value) => value.to_string(),
                    Err(error) => return error,
                };
                let record = match load_owned_run(ctx, &delegated_run_id) {
                    Ok(record) => record,
                    Err(error) => return error,
                };
                let message = match params
                    .message
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    Some(value) => value.to_string(),
                    None => {
                        return ToolResult::error_with_code(
                            "agent_message_required",
                            "message or followup requires non-empty message.",
                        )
                    }
                };
                match self
                    .runtime
                    .send_message(&delegated_run_id, message.clone())
                {
                    Ok(()) => match load_owned_run(ctx, &delegated_run_id) {
                        Ok(latest)
                            if should_resume_terminal_followup(params.action, &latest) =>
                        {
                            params.prompt = message;
                            self.execute_resume_from_record(
                                params,
                                ctx,
                                delegated_run_id,
                                latest,
                            )
                            .await
                        }
                        Ok(latest) if is_terminal(latest.stage) => ToolResult::error_with_code(
                            "agent_not_live",
                            format!(
                                "Delegated run '{delegated_run_id}' completed while the message was being delivered. Use followup or resume to continue from its durable result."
                            ),
                        ),
                        Ok(_) => ToolResult::success_data(json!({
                            "status": "queued",
                            "delivery": "accepted_by_live_mailbox",
                            "delegated_run_id": delegated_run_id,
                        })),
                        Err(error) => error,
                    },
                    Err(_) if should_resume_terminal_followup(params.action, &record) => {
                        params.prompt = message;
                        self.execute_resume_from_record(params, ctx, delegated_run_id, record)
                            .await
                    }
                    Err(error) => ToolResult::error_with_code("agent_not_live", error),
                }
            }
            AgentAction::Interrupt => {
                let delegated_run_id = match required_run_id(&params) {
                    Ok(value) => value.to_string(),
                    Err(error) => return error,
                };
                let record = match load_owned_run(ctx, &delegated_run_id) {
                    Ok(record) => record,
                    Err(error) => return error,
                };
                if is_terminal(record.stage) {
                    return ToolResult::success_data(json!({
                        "status": "already_terminal",
                        "run": record,
                    }));
                }
                if let Err(error) = self.runtime.cancel(&delegated_run_id) {
                    match load_owned_run(ctx, &delegated_run_id) {
                        Ok(winner) if is_terminal(winner.stage) => {
                            return ToolResult::success_data(json!({
                                "status": "already_terminal",
                                "run": winner,
                            }));
                        }
                        _ => return ToolResult::error_with_code("agent_not_live", error),
                    }
                }
                // Cancellation is a request until the child reaches a
                // quiescent boundary. In particular, a dispatched write may
                // already have committed and must finish assembling its
                // producer-owned result before the durable row can truthfully
                // become terminal.
                let started = Instant::now();
                loop {
                    match load_owned_run(ctx, &delegated_run_id) {
                        Ok(winner) if winner.stage == DelegatedRunStage::Cancelled => {
                            return ToolResult::success_data(json!({
                                "status": "cancelled",
                                "run": winner,
                            }));
                        }
                        Ok(winner) if is_terminal(winner.stage) => {
                            return ToolResult::success_data(json!({
                                "status": "already_terminal",
                                "run": winner,
                            }));
                        }
                        Ok(running) if started.elapsed() >= INTERRUPT_ACK_WAIT => {
                            return ToolResult::success_data(json!({
                                "status": "cancellation_requested",
                                "quiescent": false,
                                "run": running,
                            }));
                        }
                        Ok(_) => tokio::time::sleep(Duration::from_millis(25)).await,
                        Err(error) => return error,
                    }
                }
            }
            AgentAction::Resume => {
                let delegated_run_id = match required_run_id(&params) {
                    Ok(value) => value.to_string(),
                    Err(error) => return error,
                };
                let record = match load_owned_run(ctx, &delegated_run_id) {
                    Ok(record) => record,
                    Err(error) => return error,
                };
                self.execute_resume_from_record(params, ctx, delegated_run_id, record)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rusqlite::params;
    use tempfile::TempDir;

    use super::*;
    use crate::storage::{Database, DelegatedRunScope, DelegatedRunStartInput, DelegatedRunStore};

    fn ownership_fixture() -> (ToolContext, ToolContext, TempDir) {
        let temp = TempDir::new().expect("tempdir");
        let project_dir = temp.path().join("project-a");
        std::fs::create_dir_all(&project_dir).expect("project dir");
        let db_path = temp.path().join("agent-control.db");
        let db = Database::new(&db_path).expect("database");
        let now = Utc::now().to_rfc3339();
        for session in ["parent-a", "parent-b"] {
            db.conn()
                .execute(
                    "INSERT INTO sessions (id, title, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
                    params![session, session, now, now],
                )
                .expect("seed session");
        }
        DelegatedRunStore::new(db)
            .create_run(&DelegatedRunStartInput {
                delegated_run_id: "run-owned".to_string(),
                parent_session_id: "parent-a".to_string(),
                parent_tool_call_id: Some("tool-1".to_string()),
                role: DelegatedRunRole::Explore,
                stage: DelegatedRunStage::Running,
                provider: None,
                model: None,
                resumable: true,
                resumed_from_run_id: None,
                target_scope: vec![
                    DelegatedRunScope {
                        label: "launch workspace".to_string(),
                        path: project_dir.to_string_lossy().into_owned(),
                        kind: "workspace".to_string(),
                    },
                    DelegatedRunScope {
                        label: "project".to_string(),
                        path: ".".to_string(),
                        kind: "project".to_string(),
                    },
                ],
            })
            .expect("seed delegated run");

        let owner = ToolContext {
            session_id: Some("parent-a".to_string()),
            db_path: Some(db_path.clone()),
            project_dir: Some(project_dir.clone()),
            ..Default::default()
        };
        let foreign = ToolContext {
            session_id: Some("parent-b".to_string()),
            db_path: Some(db_path),
            project_dir: Some(project_dir),
            ..Default::default()
        };
        (owner, foreign, temp)
    }

    #[test]
    fn lifecycle_lookup_hides_foreign_parent_runs() {
        let (owner, foreign, _temp) = ownership_fixture();
        assert_eq!(
            load_owned_run(&owner, "run-owned")
                .expect("owner can read")
                .delegated_run_id,
            "run-owned"
        );

        let error = load_owned_run(&foreign, "run-owned").expect_err("foreign run hidden");
        let envelope: serde_json::Value =
            serde_json::from_str(&error.output).expect("structured error");
        assert_eq!(envelope["error"]["code"], "agent_run_not_found");
    }

    #[test]
    fn only_terminal_followup_selects_durable_resume_path() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let running = load_owned_run(&owner, "run-owned").expect("running run");
        assert!(!should_resume_terminal_followup(
            AgentAction::Followup,
            &running
        ));

        let store = open_delegated_run_store(&owner).expect("delegated store");
        store
            .finalize_run(
                "run-owned",
                DelegatedRunStage::Complete,
                &json!({"success": true, "findings": "durable result"}),
                Some("durable result"),
                true,
            )
            .expect("finalize delegated run");
        let completed = load_owned_run(&owner, "run-owned").expect("completed run");

        assert!(should_resume_terminal_followup(
            AgentAction::Followup,
            &completed
        ));
        assert!(!should_resume_terminal_followup(
            AgentAction::Message,
            &completed
        ));
    }

    #[test]
    fn legacy_planner_and_verifier_resume_through_the_shared_child_contract() {
        use crate::agent::subagent::{AgentExecutionProfile, AgentSpec};

        let (owner, _foreign, _temp) = ownership_fixture();
        let mut record = load_owned_run(&owner, "run-owned").expect("persisted run");
        assert!(record.capabilities.is_empty());
        for (role, can_execute) in [
            (DelegatedRunRole::Planner, false),
            (DelegatedRunRole::Verifier, true),
        ] {
            record.role = role;
            let mut params = Params::default();
            apply_persisted_child_contract(
                &mut params,
                &record,
                "run-owned",
                owner.project_dir.as_deref(),
            )
            .expect("legacy role must remain resumable");
            let spec = AgentSpec::resolve(
                params.profile.as_deref(),
                params.agent_type.as_deref(),
                params.name.as_deref(),
                "Continue the persisted task",
                None,
                None,
                &params.capabilities,
                None,
                None,
                None,
            )
            .expect("resume must resolve through the shared child engine");
            assert_eq!(spec.execution_profile(), AgentExecutionProfile::Explore);
            assert_eq!(spec.allows_execute(), can_execute);
            assert!(!spec.capabilities.contains(&AgentCapability::Write));
        }
    }

    #[test]
    fn resume_restores_execute_only_contract_name_and_not_fake_components() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let db_path = owner.db_path.as_ref().expect("db path");
        let store = DelegatedRunStore::new(Database::new(db_path).expect("db"));
        let capabilities = [AgentCapability::Execute].into_iter().collect();
        store
            .create_run_with_child_contract(
                &DelegatedRunStartInput {
                    delegated_run_id: "run-exec".to_string(),
                    parent_session_id: "parent-a".to_string(),
                    parent_tool_call_id: Some("tool-exec".to_string()),
                    role: DelegatedRunRole::Explore,
                    stage: DelegatedRunStage::Complete,
                    provider: None,
                    model: None,
                    resumable: true,
                    resumed_from_run_id: None,
                    target_scope: vec![
                        DelegatedRunScope {
                            label: "launch workspace".to_string(),
                            path: owner
                                .project_dir
                                .as_ref()
                                .expect("project dir")
                                .to_string_lossy()
                                .into_owned(),
                            kind: "workspace".to_string(),
                        },
                        DelegatedRunScope {
                            label: "project".to_string(),
                            path: ".".to_string(),
                            kind: "project".to_string(),
                        },
                    ],
                },
                Some("focused validator"),
                &capabilities,
            )
            .expect("seed contracted run");
        let record = store
            .get_run("run-exec")
            .expect("read run")
            .expect("run exists");
        let mut params = Params::default();

        apply_persisted_child_contract(
            &mut params,
            &record,
            "run-exec",
            owner.project_dir.as_deref(),
        )
        .expect("persisted contract should restore");

        assert_eq!(params.profile.as_deref(), Some("child"));
        assert_eq!(params.name.as_deref(), Some("focused validator"));
        assert_eq!(params.capabilities, vec!["execute"]);
        assert!(params.components.is_none());
        assert!(params.scope.is_none());
    }

    #[test]
    fn resume_restores_single_scope_and_rejects_retargeting() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let mut record = load_owned_run(&owner, "run-owned").expect("seed run");
        record.target_scope.truncate(1);
        record.target_scope.push(DelegatedRunScope {
            label: "auth/mod.rs".to_string(),
            path: "src/auth/mod.rs".to_string(),
            kind: "file".to_string(),
        });

        let mut params = Params::default();
        apply_persisted_child_contract(
            &mut params,
            &record,
            "run-owned",
            owner.project_dir.as_deref(),
        )
        .expect("file scope should restore");
        assert_eq!(params.scope.as_deref(), Some("src/auth/mod.rs"));

        let mut conflicting = Params {
            scope: Some("src/billing/mod.rs".to_string()),
            ..Params::default()
        };
        let error = apply_persisted_child_contract(
            &mut conflicting,
            &record,
            "run-owned",
            owner.project_dir.as_deref(),
        )
        .expect_err("resume must reject a different target");
        assert!(error.output.contains("agent_resume_target_conflict"));
    }

    #[test]
    fn resume_restores_exact_parallel_components() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let mut record = load_owned_run(&owner, "run-owned").expect("seed run");
        record.role = DelegatedRunRole::Build;
        record.target_scope.truncate(1);
        record.target_scope.extend([
            DelegatedRunScope {
                label: "api".to_string(),
                path: "API component".to_string(),
                kind: "component".to_string(),
            },
            DelegatedRunScope {
                label: "ui".to_string(),
                path: "UI component".to_string(),
                kind: "component".to_string(),
            },
        ]);

        let mut params = Params::default();
        apply_persisted_child_contract(
            &mut params,
            &record,
            "run-owned",
            owner.project_dir.as_deref(),
        )
        .expect("components should restore");
        assert_eq!(
            params.components,
            Some(vec![
                "API component".to_string(),
                "UI component".to_string()
            ])
        );
    }

    #[test]
    fn resume_restores_unified_component_and_primary_scope_together() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let mut record = load_owned_run(&owner, "run-owned").expect("seed run");
        record.role = DelegatedRunRole::Build;
        record.target_scope.truncate(1);
        record.target_scope.extend([
            DelegatedRunScope {
                label: "auth".to_string(),
                path: "src/auth".to_string(),
                kind: "directory".to_string(),
            },
            DelegatedRunScope {
                label: "token refresh".to_string(),
                path: "Implement token refresh".to_string(),
                kind: "component".to_string(),
            },
        ]);

        let mut params = Params::default();
        apply_persisted_child_contract(
            &mut params,
            &record,
            "run-owned",
            owner.project_dir.as_deref(),
        )
        .expect("unified component contract should restore");
        assert_eq!(params.scope.as_deref(), Some("src/auth"));
        assert_eq!(
            params.components,
            Some(vec!["Implement token refresh".to_string()])
        );
    }

    #[test]
    fn resume_fails_closed_after_session_switches_projects() {
        let (owner, _foreign, temp) = ownership_fixture();
        let record = load_owned_run(&owner, "run-owned").expect("seed run");
        let other_project = temp.path().join("project-b");
        std::fs::create_dir_all(&other_project).expect("other project");
        let mut params = Params::default();

        let error =
            apply_persisted_child_contract(&mut params, &record, "run-owned", Some(&other_project))
                .expect_err("resume must remain bound to its launch project");
        assert!(error.output.contains("agent_resume_workspace_mismatch"));
    }

    #[test]
    fn legacy_run_without_workspace_identity_cannot_resume() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let mut record = load_owned_run(&owner, "run-owned").expect("seed run");
        record
            .target_scope
            .retain(|scope| scope.kind != "workspace");
        let mut params = Params::default();

        let error = apply_persisted_child_contract(
            &mut params,
            &record,
            "run-owned",
            owner.project_dir.as_deref(),
        )
        .expect_err("legacy scope cannot prove its origin project");
        assert!(error.output.contains("agent_resume_workspace_invalid"));
    }

    #[test]
    fn completed_task_graph_directs_readers_to_status_instead_of_legacy_resume_error() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let mut record = load_owned_run(&owner, "run-owned").expect("seed run");
        record.target_scope.truncate(1);
        record.target_scope.extend([
            DelegatedRunScope {
                label: "architecture".to_string(),
                path: "Sources docs".to_string(),
                kind: "task".to_string(),
            },
            DelegatedRunScope {
                label: "security".to_string(),
                path: "Sources Tools".to_string(),
                kind: "task".to_string(),
            },
        ]);
        let mut params = Params::default();

        let error = apply_persisted_child_contract(
            &mut params,
            &record,
            "run-owned",
            owner.project_dir.as_deref(),
        )
        .expect_err("task graphs must not be collapsed into a single resumed child");

        assert!(error.output.contains("agent_resume_task_graph_unsupported"));
        assert!(error.output.contains("agent status"));
        assert!(error.output.contains("Spawn a new task graph"));
    }

    #[test]
    fn durable_resume_evidence_is_bounded_and_drops_raw_builder_payloads() {
        let (owner, _foreign, _temp) = ownership_fixture();
        let mut record = load_owned_run(&owner, "run-owned").expect("seed run");
        record.human_review = None;
        record.artifact = Some(json!({
            "outcome": "partial",
            "findings": format!("useful prefix {}", "x".repeat(30_000)),
            "paths_examined": ["src/lib.rs", "src/main.rs"],
            "errors": ["one bounded failure"],
            "next_action_hint": "Close the remaining validation gap.",
            "builders": [{"raw_output": "RAW-BUILDER-SENTINEL"}]
        }));

        let evidence = compact_durable_resume_evidence(&record);
        assert!(evidence.len() <= MAX_RESUME_EVIDENCE_CHARS);
        assert!(evidence.contains("useful prefix"));
        assert!(evidence.contains("Close the remaining validation gap"));
        assert!(!evidence.contains("RAW-BUILDER-SENTINEL"));
    }
}
