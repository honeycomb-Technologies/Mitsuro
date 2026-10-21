use std::path::Path;

use tracing::{info, warn};
use uuid::Uuid;

use crate::agent::context::build_subagent_project_context;
use crate::agent::subagent::{
    execute_single_child, AgentCapability, SubAgentResult, SubAgentTask, SubAgentTermination,
};
use crate::agent::{
    CoordinatedSynthesisPermit, CoordinatedTaskPermit, DelegatedRunStage, DelegationCoordinator,
    DelegationTaskOutcome,
};
use crate::ai::models::ModelKey;
use crate::storage::{
    DelegatedRunCreateOutcome, DelegatedRunLease, DelegatedRunRole, DelegatedRunScope,
    DelegatedRunStartInput, DelegationCompletionPolicy, DelegationExecutionMode,
    DelegationExecutorEnvelopeV1, DelegationExecutorKind, DelegationExecutorSessionType,
    DelegationFailurePolicy, DelegationGovernance, DelegationGroupContract,
    DelegationGroupStartInput, DelegationGroupState, DelegationTaskSpec, DelegationWriterMode,
    SessionType, DELEGATION_EXECUTOR_ENVELOPE_VERSION,
};
use crate::tools::registry::DelegationPolicy;
use crate::tools::{ToolContext, ToolResult};
use crate::SessionManager;

use super::{
    background_started_result, build_parent_context_brief, build_resume_seed,
    build_single_agent_artifact, build_single_agent_warnings, concise_target_label,
    delegated_persistence_error, delegated_scope, delegated_workspace_scope,
    emit_single_agent_completion, existing_continuation_error, notify_child_completion,
    open_delegated_run_store, persist_background_single_agent_artifact,
    persist_single_agent_artifact, resolve_explore_target, AgentTool, Params,
};

struct ResolvedChildTarget {
    working_dir: std::path::PathBuf,
    target_path: std::path::PathBuf,
    label: String,
    kind: &'static str,
}

fn normalize_persisted_target(value: &str) -> &str {
    let trimmed = value.trim().trim_end_matches('/');
    let normalized = trimmed.strip_prefix("./").unwrap_or(trimmed);
    if normalized == "." {
        ""
    } else {
        normalized
    }
}

fn persisted_target_matches(previous: &[DelegatedRunScope], current: &[DelegatedRunScope]) -> bool {
    let previous_workspaces = previous
        .iter()
        .filter(|scope| scope.kind == "workspace")
        .collect::<Vec<_>>();
    let current_workspaces = current
        .iter()
        .filter(|scope| scope.kind == "workspace")
        .collect::<Vec<_>>();
    let ([previous_workspace], [current_workspace]) = (
        previous_workspaces.as_slice(),
        current_workspaces.as_slice(),
    ) else {
        return false;
    };
    if normalize_persisted_target(&previous_workspace.path)
        != normalize_persisted_target(&current_workspace.path)
    {
        return false;
    }

    let previous_primary = previous
        .iter()
        .filter(|scope| !matches!(scope.kind.as_str(), "workspace" | "component"))
        .collect::<Vec<_>>();
    let current_primary = current
        .iter()
        .filter(|scope| !matches!(scope.kind.as_str(), "workspace" | "component"))
        .collect::<Vec<_>>();
    let ([previous_primary], [current_primary]) =
        (previous_primary.as_slice(), current_primary.as_slice())
    else {
        return false;
    };
    let kind_matches = previous_primary.kind == current_primary.kind
        || (matches!(
            (
                previous_primary.kind.as_str(),
                current_primary.kind.as_str()
            ),
            ("project", "directory") | ("directory", "project")
        ) && normalize_persisted_target(&previous_primary.path).is_empty()
            && normalize_persisted_target(&current_primary.path).is_empty());
    if !kind_matches
        || normalize_persisted_target(&previous_primary.path)
            != normalize_persisted_target(&current_primary.path)
    {
        return false;
    }

    let mut previous_components = previous
        .iter()
        .filter(|scope| scope.kind == "component")
        .map(|scope| normalize_persisted_target(&scope.path))
        .collect::<Vec<_>>();
    let mut current_components = current
        .iter()
        .filter(|scope| scope.kind == "component")
        .map(|scope| normalize_persisted_target(&scope.path))
        .collect::<Vec<_>>();
    previous_components.sort_unstable();
    current_components.sort_unstable();
    previous_components == current_components
}

fn resolve_child_target(
    scope: Option<&str>,
    project_dir: &Path,
) -> Result<ResolvedChildTarget, String> {
    let Some(scope) = scope else {
        return Ok(ResolvedChildTarget {
            working_dir: project_dir.to_path_buf(),
            target_path: project_dir.to_path_buf(),
            label: "project".to_string(),
            kind: "directory",
        });
    };

    if let Ok(path) = resolve_explore_target(scope, project_dir, "directory") {
        return Ok(ResolvedChildTarget {
            working_dir: path.clone(),
            target_path: path,
            label: concise_target_label(scope, 0),
            kind: "directory",
        });
    }

    let path = resolve_explore_target(scope, project_dir, "file")?;
    let working_dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| project_dir.to_path_buf());
    Ok(ResolvedChildTarget {
        working_dir,
        target_path: path,
        label: concise_target_label(scope, 0),
        kind: "file",
    })
}

fn build_child_target_scope(
    project_dir: &Path,
    label: &str,
    target_path: &Path,
    target_kind: &str,
    assigned_component: Option<&str>,
) -> Result<Vec<DelegatedRunScope>, String> {
    let mut scopes = vec![
        delegated_workspace_scope(project_dir)?,
        delegated_scope(label, target_path, target_kind, project_dir),
    ];
    if let Some(component) = assigned_component
        .map(str::trim)
        .filter(|component| !component.is_empty())
    {
        scopes.push(DelegatedRunScope {
            label: "assigned component".to_string(),
            path: component.to_string(),
            kind: "component".to_string(),
        });
    }
    Ok(scopes)
}

fn background_persistence_precondition(
    ctx: &ToolContext,
    store_available: bool,
    agent_type: &str,
) -> Option<ToolResult> {
    let reason = if ctx.db_path.is_none() {
        Some("this session has no durable database")
    } else if !store_available {
        Some("the delegated-run database could not be opened")
    } else if ctx.session_id.is_none() {
        Some("there is no durable parent session")
    } else {
        None
    }?;

    Some(ToolResult::error_with_code(
        "agent_persistence_error",
        format!("Background {agent_type} was not started because {reason}."),
    ))
}

#[derive(Clone)]
struct SingleTaskDelegation {
    coordinator: DelegationCoordinator,
    group_id: String,
    task_id: String,
    turn_budget: Option<usize>,
}

impl SingleTaskDelegation {
    fn create(
        ctx: &ToolContext,
        delegated_run_id: &str,
        task_key: &str,
        objective: &str,
        role: DelegatedRunRole,
        target_scope: &[DelegatedRunScope],
        delegation_policy: &DelegationPolicy,
        background: bool,
        executor_envelope: Option<DelegationExecutorEnvelopeV1>,
    ) -> Result<Self, String> {
        let db_path = ctx
            .db_path
            .as_ref()
            .ok_or_else(|| "durable single-agent delegation has no database path".to_string())?;
        let parent_session_id = ctx
            .session_id
            .as_ref()
            .ok_or_else(|| "durable single-agent delegation has no parent session".to_string())?;
        let task_id = format!("{delegated_run_id}:task:0");
        if objective.len() > 32 * 1024 {
            return Err(
                "delegated objective exceeds the 32 KiB exact-replay contract; split the task or shorten its prompt"
                    .to_string(),
            );
        }
        let coordinator = DelegationCoordinator::new(db_path.clone());
        let mut durable_scope = target_scope.to_vec();
        if role == DelegatedRunRole::Build
            && !durable_scope.iter().any(|scope| scope.kind == "workspace")
        {
            durable_scope.push(DelegatedRunScope {
                label: "authoritative workspace".to_string(),
                path: ctx.working_dir.display().to_string(),
                kind: "workspace".to_string(),
            });
        }
        let turn_budget = delegation_policy.max_turns;
        coordinator
            .create_group(&DelegationGroupStartInput {
                delegation_group_id: delegated_run_id.to_string(),
                parent_session_id: parent_session_id.clone(),
                parent_tool_call_id: ctx.tool_use_id.clone(),
                contract: DelegationGroupContract {
                    execution_mode: if background {
                        DelegationExecutionMode::Detached
                    } else {
                        DelegationExecutionMode::Foreground
                    },
                    completion_policy: DelegationCompletionPolicy::AllSettled,
                    failure_policy: DelegationFailurePolicy::Continue,
                    governance: DelegationGovernance {
                        permission_mode: ctx.permission_mode,
                        reasoning_effort: ctx.delegated_reasoning_effort,
                        delegated_turn_budget: turn_budget,
                        max_parallelism: 1,
                        execution_tool_allowlist: delegation_policy
                            .execution_tool_allowlist
                            .clone(),
                        delegation_policy: delegation_policy.clone(),
                    },
                },
                tasks: vec![DelegationTaskSpec {
                    delegation_task_id: task_id.clone(),
                    task_key: task_key.to_string(),
                    objective: objective.to_string(),
                    role,
                    target_scope: durable_scope,
                    max_attempts: 2,
                    depends_on: Vec::new(),
                    write_intent: Vec::new(),
                    task_policy: Some(delegation_policy.clone()),
                    // A one-task writer still enters the shared authoritative
                    // partition. Parallel build groups must not mutate the
                    // same checkout concurrently merely because this group
                    // contains only one child.
                    writer_mode: DelegationWriterMode::Shared,
                    attempt_workspace: None,
                    workspace_baseline: None,
                    executor_envelope,
                }],
            })
            .map_err(|error| error.to_string())?;

        Ok(Self {
            coordinator,
            group_id: delegated_run_id.to_string(),
            task_id,
            turn_budget,
        })
    }

    fn attach(&self, task: SubAgentTask) -> SubAgentTask {
        // The immutable durable contract is the execution-time authority.
        // Absence means unlimited rather than a hidden product default.
        let task = if let Some(turn_budget) = self.turn_budget {
            task.with_max_turns(turn_budget)
        } else {
            task
        };
        task.with_delegation_task(self.group_id.clone(), self.task_id.clone())
    }

    async fn acquire(
        &self,
        task: &SubAgentTask,
        model: &str,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<CoordinatedTaskPermit, String> {
        self.coordinator
            .validate_task_runtime(
                &self.task_id,
                task.delegation_policy.as_ref(),
                task.reasoning_effort,
                &task.working_dir,
            )
            .map_err(|error| error.to_string())?;
        self.coordinator
            .acquire_task(&self.task_id, model, cancellation)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "delegated task was cancelled before scheduler admission".to_string())
    }

    fn settle(
        &self,
        result: &SubAgentResult,
        permit: CoordinatedTaskPermit,
    ) -> Result<Option<CoordinatedSynthesisPermit>, String> {
        let artifact = bounded_task_artifact(result);
        let outcome = if result.termination == SubAgentTermination::Cancelled {
            DelegationTaskOutcome::Cancelled
        } else if result.success
            && result.termination == SubAgentTermination::Completed
            && result.has_usable_evidence()
        {
            DelegationTaskOutcome::Complete(artifact)
        } else if result.termination.is_degraded_interruption() && result.has_usable_evidence() {
            DelegationTaskOutcome::Degraded {
                artifact,
                reason: bounded_text(
                    result.error.as_deref().unwrap_or(result.outcome_reason()),
                    1_200,
                ),
            }
        } else {
            DelegationTaskOutcome::Failed {
                error: bounded_text(
                    result.error.as_deref().unwrap_or(result.outcome_reason()),
                    1_200,
                ),
            }
        };
        let group_state = permit
            .complete(outcome)
            .map_err(|error| error.to_string())?;
        if group_state.is_terminal() {
            return Ok(None);
        }
        self.coordinator
            .begin_synthesis(&self.group_id)
            .map(Some)
            .map_err(|error| error.to_string())
    }

    fn finalize(
        &self,
        permit: Option<CoordinatedSynthesisPermit>,
        stage: DelegatedRunStage,
    ) -> Result<(), String> {
        if let Some(permit) = permit {
            debug_assert_eq!(permit.group().delegation_group_id, self.group_id);
            return permit
                .finalize(group_terminal_state(stage))
                .map(|_| ())
                .map_err(|error| error.to_string());
        }
        let group = self
            .coordinator
            .get_group(&self.group_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "durable single-agent group disappeared".to_string())?;
        if group.state.is_terminal() {
            Ok(())
        } else {
            Err(format!(
                "durable single-agent group is {:?} without synthesis ownership",
                group.state
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_detached_executor_envelope(
    ctx: &ToolContext,
    task_id: &str,
    task_name: &str,
    objective: &str,
    kind: DelegationExecutorKind,
    role: DelegatedRunRole,
    model_key: &ModelKey,
    resolved_model: &str,
    working_dir: &Path,
    sandbox_root: &Path,
) -> Result<DelegationExecutorEnvelopeV1, String> {
    let db_path = ctx
        .db_path
        .as_ref()
        .ok_or_else(|| "detached executor envelope has no database".to_string())?;
    let session_id = ctx
        .session_id
        .as_ref()
        .ok_or_else(|| "detached executor envelope has no parent session".to_string())?;
    let session = SessionManager::new(
        crate::storage::Database::new(db_path).map_err(|error| error.to_string())?,
    )
    .get_session(session_id)
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "detached executor envelope parent session disappeared".to_string())?;
    let session_type = match session.session_type {
        SessionType::Chat => DelegationExecutorSessionType::Chat,
        SessionType::Code => DelegationExecutorSessionType::Code,
        SessionType::Hive => {
            return Err("Hive does not yet support Chat/Code executor replay envelopes".to_string())
        }
    };
    if session.user_id != ctx.user_id {
        return Err("detached executor owner differs from the parent session owner".to_string());
    }
    let canonicalize = |path: &Path, label: &str| {
        path.canonicalize()
            .map_err(|error| format!("detached executor {label} is unavailable: {error}"))
    };
    let working_dir = canonicalize(working_dir, "working directory")?;
    let sandbox_root = canonicalize(sandbox_root, "sandbox root")?;
    if !working_dir.starts_with(&sandbox_root) {
        return Err("detached executor working directory escaped its sandbox".to_string());
    }
    let project_dir = session
        .project_dir
        .as_deref()
        .map(|path| canonicalize(Path::new(path), "parent project directory"))
        .transpose()?;
    let parent_working_dir = session
        .working_dir
        .as_deref()
        .map(|path| canonicalize(Path::new(path), "parent working directory"))
        .transpose()?;
    let authorized_workspace = project_dir
        .as_deref()
        .or(parent_working_dir.as_deref())
        .ok_or_else(|| "detached executor parent session has no workspace".to_string())?;
    let context_working_dir = canonicalize(&ctx.working_dir, "parent context working directory")?;
    if !context_working_dir.starts_with(authorized_workspace) {
        return Err("detached executor parent context escaped its session workspace".to_string());
    }
    let envelope = DelegationExecutorEnvelopeV1 {
        version: DELEGATION_EXECUTOR_ENVELOPE_VERSION,
        session_id: session_id.clone(),
        parent_tool_call_id: ctx.tool_use_id.clone(),
        session_type,
        user_id: session.user_id,
        task_id: task_id.to_string(),
        task_name: task_name.to_string(),
        kind,
        role,
        provider_id: model_key.provider.to_string(),
        model_key: model_key.clone(),
        resolved_model: resolved_model.to_string(),
        working_dir: working_dir.display().to_string(),
        project_dir: project_dir.map(|path| path.display().to_string()),
        sandbox_root: sandbox_root.display().to_string(),
        objective_sha256: DelegationExecutorEnvelopeV1::objective_digest(objective),
    };
    envelope
        .validate(objective)
        .map_err(|error| error.to_string())?;
    Ok(envelope)
}

fn bounded_task_artifact(result: &SubAgentResult) -> serde_json::Value {
    serde_json::json!({
        "task_id": bounded_text(&result.task_id, 256),
        "agent_name": bounded_text(&result.agent_name, 256),
        "success": result.success,
        "termination": result.termination,
        "objective_status": result.objective_status(),
        "handoff": result.bounded_delegated_handoff(),
        "summary": bounded_text(&result.brief_summary(), 16 * 1024),
        "files_examined": result.files_examined.iter().take(32).map(|path| bounded_text(path, 512)).collect::<Vec<_>>(),
        "files_examined_count": result.files_examined.len(),
        "duration_ms": result.duration_ms,
        "turns_used": result.turns_used,
        "outcome_reason": bounded_text(result.outcome_reason(), 1_200),
    })
}

fn bounded_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    const ELLIPSIS: &str = "...";
    if max_bytes <= ELLIPSIS.len() {
        return ELLIPSIS[..max_bytes].to_string();
    }
    let mut end = max_bytes - ELLIPSIS.len();
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &value[..end], ELLIPSIS)
}

fn group_terminal_state(stage: DelegatedRunStage) -> DelegationGroupState {
    match stage {
        DelegatedRunStage::Complete => DelegationGroupState::Complete,
        DelegatedRunStage::Degraded => DelegationGroupState::Degraded,
        DelegatedRunStage::Failed => DelegationGroupState::Failed,
        DelegatedRunStage::Cancelled => DelegationGroupState::Cancelled,
        DelegatedRunStage::Created
        | DelegatedRunStage::Running
        | DelegatedRunStage::Synthesizing => DelegationGroupState::Failed,
    }
}

impl AgentTool {
    // -----------------------------------------------------------------------
    // Unified parent-directed child
    // -----------------------------------------------------------------------

    pub(super) async fn execute_child(&self, params: Params, ctx: &ToolContext) -> ToolResult {
        let Some(project_dir) = ctx.project_dir.clone() else {
            return ToolResult::error(
                "No project directory is selected for this session. Select or create a project directory before spawning a child Agent.",
            );
        };

        let registry = match ctx.tool_registry.as_ref() {
            Some(r) => r.clone(),
            None => {
                return ToolResult::error(
                    "Tool registry not available in context. Cannot delegate child work.",
                );
            }
        };

        let client = self.resolve_client(ctx);

        // Resolve scope
        let resolved_target = match resolve_child_target(params.scope.as_deref(), &project_dir) {
            Ok(target) => target,
            Err(error) => {
                return ToolResult::error_with_code("invalid_explore_target", error);
            }
        };
        let working_dir = resolved_target.working_dir;
        let target_path = resolved_target.target_path;
        let scope_label = resolved_target.label;
        let scope_kind = resolved_target.kind;

        let delegated_run_id = Uuid::new_v4().to_string();
        let capabilities = params
            .capabilities
            .iter()
            .map(|capability| AgentCapability::parse(capability))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()
            .expect("AgentSpec validated child capabilities");
        let wants_read = capabilities.contains(&AgentCapability::Read);
        let wants_write = params.capabilities.iter().any(|c| c == "write");
        let wants_execute = params.capabilities.iter().any(|c| c == "execute");
        let delegation_policy = DelegationPolicy::for_subagent_child(
            ctx.permission_mode,
            params.max_turns,
            wants_read,
            wants_write,
            wants_execute,
        )
        .with_supervised_approval(ctx.supervised_approval_granted)
        .with_execution_tool_allowlist(ctx.execution_tool_allowlist.as_ref());

        let target_scope = match build_child_target_scope(
            &project_dir,
            &scope_label,
            &target_path,
            scope_kind,
            params.assigned_component.as_deref(),
        ) {
            Ok(scopes) => scopes,
            Err(error) => {
                return ToolResult::error_with_code("invalid_project_workspace", error);
            }
        };

        let role = if wants_write {
            DelegatedRunRole::Build
        } else {
            DelegatedRunRole::Explore
        };
        let child_name = params.name.clone().unwrap_or_else(|| scope_label.clone());

        // Persist the name and exact capability contract before execution so
        // a crash-safe resume cannot widen execute-only into read access.
        let mut delegated_lease = open_delegated_run_store(ctx).map(DelegatedRunLease::new);
        let background = params.run_in_background.unwrap_or(false);
        if background {
            if let Some(error) =
                background_persistence_precondition(ctx, delegated_lease.is_some(), "child")
            {
                return error;
            }
        }
        let explicit_resume = params.resumed_from_run_id.is_some();
        let resume_candidate = match (
            delegated_lease.as_ref(),
            ctx.session_id.as_ref(),
            params.resumed_from_run_id.as_deref(),
        ) {
            (Some(store), Some(session_id), Some(resumed_from_run_id)) => {
                match store.get_run(resumed_from_run_id) {
                    Ok(Some(record))
                        if record.parent_session_id == *session_id
                            && record.effective_capabilities() == capabilities
                            && persisted_target_matches(&record.target_scope, &target_scope) =>
                    {
                        Some(record)
                    }
                    Ok(Some(_)) => {
                        return ToolResult::error_with_code(
                            "agent_resume_contract_mismatch",
                            "The selected delegated run no longer matches this session, capability contract, or persisted target.",
                        );
                    }
                    Ok(None) => {
                        return ToolResult::error_with_code(
                            "agent_run_not_found",
                            format!("Delegated run '{resumed_from_run_id}' was not found."),
                        );
                    }
                    Err(error) => {
                        return ToolResult::error_with_code("agent_store_error", error.to_string());
                    }
                }
            }
            (Some(store), Some(session_id), None) => store
                .find_related_run(session_id, role.clone(), &target_scope)
                .ok()
                .flatten()
                .filter(|record| record.effective_capabilities() == capabilities),
            _ => None,
        };

        let durable_run_started = if let (Some(lease), Some(session_id)) =
            (delegated_lease.as_mut(), ctx.session_id.as_ref())
        {
            let start = DelegatedRunStartInput {
                delegated_run_id: delegated_run_id.clone(),
                parent_session_id: session_id.clone(),
                parent_tool_call_id: ctx.tool_use_id.clone(),
                role: role.clone(),
                stage: DelegatedRunStage::Created,
                provider: Some(client.provider_id().to_string()),
                model: Some(client.config().model.clone()),
                resumable: true,
                // Related-run discovery may seed a fresh child with useful
                // evidence, but only an explicit lifecycle resume consumes
                // the origin's unique durable continuation claim.
                resumed_from_run_id: params.resumed_from_run_id.clone(),
                target_scope: target_scope.clone(),
            };
            let create = if background {
                lease.create_background_run_with_child_contract(
                    &start,
                    Some(&child_name),
                    &capabilities,
                )
            } else {
                lease.create_run_with_child_contract(&start, Some(&child_name), &capabilities)
            };
            match create {
                Ok(DelegatedRunCreateOutcome::Created) => {}
                Ok(DelegatedRunCreateOutcome::ExistingContinuation {
                    delegated_run_id,
                    resumed_from_run_id,
                }) => {
                    return existing_continuation_error(&resumed_from_run_id, &delegated_run_id);
                }
                Ok(DelegatedRunCreateOutcome::ExistingActiveWorkspaceWriter {
                    delegated_run_id,
                    workspace_path,
                }) => {
                    return ToolResult::error_with_recovery(
                        "agent_active_workspace_writer",
                        format!(
                            "A write-capable Agent run is already active for workspace '{workspace_path}' (delegated run {delegated_run_id}). A second writer was not admitted."
                        ),
                        true,
                        "Use Agent status or wait on the active delegated run. Interrupt it explicitly before starting a replacement writer.",
                        vec![
                            format!("Continue with delegated_run_id '{delegated_run_id}' instead of spawning a duplicate writer."),
                            "Do not retry spawn with a new tool-call id while the active writer is live.".to_string(),
                        ],
                        Some(serde_json::json!({
                            "admitted": false,
                            "active_delegated_run_id": delegated_run_id,
                            "workspace_path": workspace_path,
                        })),
                    );
                }
                Err(error) => {
                    return ToolResult::error_with_code(
                        "agent_persistence_error",
                        format!(
                            "Delegated child was not started because its durable run record could not be created: {error}"
                        ),
                    );
                }
            }
            true
        } else {
            false
        };
        if background && !durable_run_started {
            return ToolResult::error_with_code(
                "agent_persistence_error",
                "Background child was not started because durable run creation was unavailable.",
            );
        }

        // Build task prompt with resume context if available
        let mut task_prompt =
            super::compose_child_objective(&params.prompt, &params.acceptance_checks);
        if !explicit_resume {
            if let Some(ref previous) = resume_candidate {
                if let Some(seed) = build_resume_seed(previous, &scope_label) {
                    task_prompt = format!("{}\n\n{}", task_prompt, seed);
                }
            }
        }

        // The executed and durable objectives must be byte-identical so a
        // detached restart cannot silently lose tail instructions.
        let durable_objective = task_prompt.clone();
        let model = self.resolve_model(ctx, &client);
        // Delegated reads and writes are scoped to the exact selected project,
        // even when a local parent session has broader filesystem authority.
        let inherited_sandbox = project_dir.canonicalize().map_err(|error| {
            ToolResult::error_with_code(
                "invalid_project_workspace",
                format!(
                    "Could not resolve delegated launch workspace '{}': {error}",
                    project_dir.display()
                ),
            )
        });
        let inherited_sandbox = match inherited_sandbox {
            Ok(path) => path,
            Err(result) => return result,
        };
        let executor_envelope = if background {
            let executor_kind = if role == DelegatedRunRole::Build {
                // Shared-writer side effects cannot be replayed safely after a
                // host crash. Classify every write-capable single child as a
                // build envelope so recovery fails closed just like a
                // component build until isolated patch restoration exists.
                DelegationExecutorKind::Build
            } else if params.agent_type.as_deref() == Some("explore") {
                DelegationExecutorKind::Explore
            } else {
                DelegationExecutorKind::Normal
            };
            match build_detached_executor_envelope(
                ctx,
                &format!("{delegated_run_id}:task:0"),
                &child_name,
                &durable_objective,
                executor_kind,
                role.clone(),
                &client.resolved_model().key,
                &model,
                &working_dir,
                &inherited_sandbox,
            ) {
                Ok(envelope) => Some(envelope),
                Err(error) => {
                    return ToolResult::error_with_code("agent_persistence_error", error);
                }
            }
        } else {
            None
        };

        let single_delegation = if durable_run_started {
            match SingleTaskDelegation::create(
                ctx,
                &delegated_run_id,
                "child-0",
                &durable_objective,
                role,
                &target_scope,
                &delegation_policy,
                background,
                executor_envelope,
            ) {
                Ok(delegation) => Some(delegation),
                Err(error) => {
                    return ToolResult::error_with_code(
                        "agent_persistence_error",
                        format!(
                            "Delegated child was not started because its durable group could not be created: {error}"
                        ),
                    );
                }
            }
        } else {
            None
        };

        let mut task = SubAgentTask::new("child-0", &task_prompt)
            .with_name(child_name.clone())
            .with_working_dir(working_dir)
            .with_sandbox_root(inherited_sandbox)
            .with_delegated_run_id(delegated_run_id.clone())
            .with_delegation_policy(delegation_policy.clone())
            .with_reasoning_effort(ctx.delegated_reasoning_effort)
            .with_process_context(
                ctx.process_registry.clone(),
                ctx.user_id.clone(),
                ctx.session_id.clone(),
            )
            .with_provider_call_trace(ctx.provider_call_trace.clone());
        if let Some(max_turns) = params.max_turns {
            task = task.with_max_turns(max_turns);
        }
        if let Some(delegation) = single_delegation.as_ref() {
            task = delegation.attach(task);
        }

        // Every capability class follows this same execution path and inherits
        // the parent's resolved model unless the run explicitly overrides it.
        // Build project context for the subagent, with optional parent conversation brief
        let mut project_context =
            build_subagent_project_context(&ctx.working_dir, ctx.project_dir.as_deref());
        if !params.parent_context_applied {
            if let Some(ref parent_conversation) = ctx.parent_conversation {
                let brief = build_parent_context_brief(parent_conversation, 10);
                if !brief.is_empty() {
                    project_context = format!("{}\n\n{}", brief, project_context);
                }
            }
        }

        let cancellation_token = if background {
            self.cancellation.child_token()
        } else {
            ctx.execution_cancellation
                .clone()
                .unwrap_or_else(|| self.cancellation.child_token())
        };
        let progress_tx = ctx.agent_progress_tx.clone();

        info!(
            delegated_run_id = %delegated_run_id,
            name = %child_name,
            model = %model,
            scope = %scope_label,
            background,
            "Agent tool: starting agnostic child"
        );

        // ── Background mode ──────────────────────────────────────────
        if background {
            let bg_delegation_policy = delegation_policy.clone();
            let bg_delegated_run_id = delegated_run_id.clone();
            let bg_db_path = ctx.db_path.clone();
            let bg_session_id = ctx.session_id.clone();
            let bg_user_id = ctx.user_id.clone();
            let bg_workspace_root = ctx.filesystem_access_root();
            let bg_child_name = child_name.clone();
            let bg_runtime = self.runtime.clone();
            let bg_single_delegation = single_delegation.clone();
            let mut bg_run_lease = delegated_lease
                .take()
                .expect("background child start has an armed durable lease");
            let bg_host_heartbeat = match bg_run_lease
                .start_background_host_heartbeat(&bg_delegated_run_id, cancellation_token.clone())
            {
                Ok(heartbeat) => heartbeat,
                Err(error) => {
                    return ToolResult::error_with_code(
                        "agent_persistence_error",
                        format!(
                            "Background child was not started because its durable host lease could not start: {error}"
                        ),
                    );
                }
            };
            let (mailbox, mut bg_runtime_registration) = bg_runtime.register_guarded(
                bg_delegated_run_id.clone(),
                bg_child_name.clone(),
                bg_session_id.clone(),
                cancellation_token.clone(),
            );
            task = task.with_mailbox(mailbox);

            tokio::spawn(async move {
                let _bg_host_heartbeat = bg_host_heartbeat;
                let (result, synthesis_permit) = if let Some(delegation) =
                    bg_single_delegation.as_ref()
                {
                    match delegation.acquire(&task, &model, &cancellation_token).await {
                        Ok(task_permit) => {
                            let execution_cancellation = task_permit.cancellation();
                            let result = execute_single_child(
                                client,
                                task,
                                registry,
                                bg_delegation_policy.clone(),
                                project_context,
                                model,
                                execution_cancellation,
                                progress_tx.clone(),
                            )
                            .await;
                            let synthesis_permit = match delegation.settle(&result, task_permit) {
                                Ok(permit) => permit,
                                Err(error) => {
                                    tracing::error!(
                                        delegated_run_id = %bg_delegated_run_id,
                                        %error,
                                        "Suppressing background child completion because durable task settlement failed"
                                    );
                                    return;
                                }
                            };
                            (result, synthesis_permit)
                        }
                        Err(error) => {
                            tracing::error!(
                                delegated_run_id = %bg_delegated_run_id,
                                %error,
                                "Suppressing background child completion because scheduler admission failed"
                            );
                            return;
                        }
                    }
                } else {
                    (
                        execute_single_child(
                            client,
                            task,
                            registry,
                            bg_delegation_policy.clone(),
                            project_context,
                            model,
                            cancellation_token,
                            progress_tx.clone(),
                        )
                        .await,
                        None,
                    )
                };

                let artifact = build_single_agent_artifact(
                    &bg_delegated_run_id,
                    &result,
                    &bg_delegation_policy,
                );

                let finalization = persist_background_single_agent_artifact(
                    &bg_run_lease,
                    &bg_delegated_run_id,
                    &artifact,
                    true,
                    &bg_child_name,
                );

                match finalization {
                    Ok(authoritative) => {
                        if let Some(delegation) = bg_single_delegation.as_ref() {
                            if let Err(error) =
                                delegation.finalize(synthesis_permit, authoritative.stage)
                            {
                                tracing::error!(
                                    delegated_run_id = %bg_delegated_run_id,
                                    %error,
                                    "Suppressing background child completion because durable group finalization failed"
                                );
                                return;
                            }
                        }
                        bg_run_lease.disarm(&bg_delegated_run_id);
                        let authoritative_success =
                            authoritative.stage == DelegatedRunStage::Complete;
                        let authoritative_summary = authoritative
                            .human_review
                            .as_deref()
                            .unwrap_or(&artifact.review_summary);
                        emit_single_agent_completion(
                            &progress_tx,
                            &bg_delegated_run_id,
                            &bg_child_name,
                            &result,
                            authoritative.stage,
                            authoritative_summary,
                        );
                        if authoritative.stage != DelegatedRunStage::Cancelled {
                            if let Err(error) = notify_child_completion(
                                &bg_runtime,
                                bg_db_path.as_deref(),
                                bg_session_id.as_deref(),
                                bg_user_id.as_deref(),
                                bg_workspace_root.as_deref(),
                                &bg_delegated_run_id,
                                &bg_child_name,
                                authoritative_success,
                                authoritative_summary,
                            ) {
                                warn!(
                                    delegated_run_id = %bg_delegated_run_id,
                                    %error,
                                    "Failed to queue background child completion"
                                );
                                let _ = bg_runtime
                                    .request_completion_reconciliation(bg_delegated_run_id.clone());
                            }
                        }
                        bg_runtime_registration.finish(authoritative_success);
                    }
                    Err(error) => {
                        tracing::error!(
                            delegated_run_id = %bg_delegated_run_id,
                            %error,
                            "Suppressing background child completion because terminal finalization was not authoritative"
                        );
                        // Leave the guard armed. Its Drop path asks the server
                        // to reconcile the lease's abnormal durable terminal.
                    }
                }
            });

            return background_started_result(
                &delegated_run_id,
                &child_name,
                Some(child_name.as_str()),
            );
        }

        // ── Synchronous mode (existing behavior) ─────────────────────
        let completion_tx = progress_tx.clone();
        let (result, synthesis_permit) = if let Some(delegation) = single_delegation.as_ref() {
            match delegation.acquire(&task, &model, &cancellation_token).await {
                Ok(task_permit) => {
                    let execution_cancellation = task_permit.cancellation();
                    let result = execute_single_child(
                        client,
                        task,
                        registry,
                        delegation_policy.clone(),
                        project_context,
                        model,
                        execution_cancellation,
                        progress_tx,
                    )
                    .await;
                    let synthesis_permit = match delegation.settle(&result, task_permit) {
                        Ok(permit) => permit,
                        Err(error) => {
                            let artifact = build_single_agent_artifact(
                                &delegated_run_id,
                                &result,
                                &delegation_policy,
                            );
                            let error = anyhow::anyhow!(
                                "durable single-agent task settlement failed: {error}"
                            );
                            return delegated_persistence_error(
                                &delegated_run_id,
                                artifact.payload,
                                &error,
                            );
                        }
                    };
                    (result, synthesis_permit)
                }
                Err(error) => {
                    return ToolResult::error_with_code(
                        "agent_persistence_error",
                        format!(
                            "Delegated child did not start because durable scheduler admission failed: {error}"
                        ),
                    );
                }
            }
        } else {
            (
                execute_single_child(
                    client,
                    task,
                    registry,
                    delegation_policy.clone(),
                    project_context,
                    model,
                    cancellation_token,
                    progress_tx,
                )
                .await,
                None,
            )
        };

        let artifact = build_single_agent_artifact(&delegated_run_id, &result, &delegation_policy);

        // Finalize the delegated run
        if durable_run_started {
            let lease = delegated_lease
                .as_mut()
                .expect("durable child start has an open store");
            let authoritative = match persist_single_agent_artifact(
                lease,
                &delegated_run_id,
                &artifact,
                true,
                "Failed to persist delegated child run final artifact",
            ) {
                Ok(authoritative) => authoritative,
                Err(error) => {
                    return delegated_persistence_error(
                        &delegated_run_id,
                        artifact.payload,
                        &error,
                    );
                }
            };
            if let Some(delegation) = single_delegation.as_ref() {
                if let Err(error) = delegation.finalize(synthesis_permit, authoritative.stage) {
                    let error = anyhow::anyhow!(error);
                    return delegated_persistence_error(
                        &delegated_run_id,
                        artifact.payload,
                        &error,
                    );
                }
            }
            lease.disarm(&delegated_run_id);
            // The child's own terminal frame precedes artifact persistence and
            // is intentionally kept Running by the server until this durable
            // aggregate boundary. Re-emit now so foreground cards settle
            // without waiting for a reconnect.
            emit_single_agent_completion(
                &completion_tx,
                &delegated_run_id,
                &child_name,
                &result,
                authoritative.stage,
                authoritative
                    .human_review
                    .as_deref()
                    .unwrap_or(&artifact.review_summary),
            );
        }

        let mut warnings = build_single_agent_warnings(&result, "Child Agent");
        if !durable_run_started {
            warnings.push(
                "This synchronous child ran without a durable delegated-run record.".to_string(),
            );
        }

        ToolResult::success_data_with(artifact.payload, warnings, None, None)
    }
}

#[cfg(test)]
mod child_scope_tests {
    use std::fs;

    use super::*;
    use crate::storage::{Database, SessionManager};
    use crate::tools::registry::PermissionMode;

    fn durable_test_context(temp: &tempfile::TempDir) -> (ToolContext, Vec<DelegatedRunScope>) {
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).expect("workspace");
        let workspace = workspace.canonicalize().expect("canonical workspace");
        let db_path = temp.path().join("single-delegation.db");
        let session_id = SessionManager::new(Database::new(&db_path).expect("database"))
            .create_session("single delegation", Some("test:model"), workspace.to_str())
            .expect("session");
        let ctx = ToolContext {
            db_path: Some(db_path),
            session_id: Some(session_id),
            tool_use_id: Some("tool-single".to_string()),
            working_dir: workspace.clone(),
            project_dir: Some(workspace.clone()),
            sandbox_root: Some(workspace.clone()),
            permission_mode: PermissionMode::Autonomous,
            ..ToolContext::default()
        };
        let scope = vec![DelegatedRunScope {
            label: "workspace".to_string(),
            path: workspace.display().to_string(),
            kind: "workspace".to_string(),
        }];
        (ctx, scope)
    }

    #[test]
    fn file_target_persists_the_file_not_its_working_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let project = temp.path().join("project");
        let source = project.join("src/auth");
        fs::create_dir_all(&source).expect("source directory");
        let file = source.join("mod.rs");
        fs::write(&file, "pub fn authenticate() {}\n").expect("source file");

        let resolved = resolve_child_target(Some("src/auth/mod.rs"), &project)
            .expect("file target should resolve");
        let scopes = build_child_target_scope(
            &project,
            &resolved.label,
            &resolved.target_path,
            resolved.kind,
            Some("authentication component"),
        )
        .expect("target lineage should build");

        assert_eq!(
            resolved.working_dir,
            source.canonicalize().expect("canonical source")
        );
        assert_eq!(scopes.len(), 3);
        assert_eq!(scopes[0].kind, "workspace");
        assert_eq!(
            scopes[0].path,
            project
                .canonicalize()
                .expect("canonical project")
                .display()
                .to_string()
        );
        assert_eq!(scopes[1].path, "src/auth/mod.rs");
        assert_eq!(scopes[1].kind, "file");
        assert_eq!(scopes[2].kind, "component");
        assert_eq!(scopes[2].path, "authentication component");
        assert!(persisted_target_matches(&scopes, &scopes));

        let mut widened = scopes.clone();
        widened[1].path = "src/auth".to_string();
        widened[1].kind = "directory".to_string();
        assert!(!persisted_target_matches(&scopes, &widened));

        let mut foreign_workspace = scopes.clone();
        foreign_workspace[0].path = temp.path().display().to_string();
        assert!(!persisted_target_matches(&scopes, &foreign_workspace));
    }

    #[test]
    fn background_start_requires_database_store_and_parent_session() {
        let missing_database =
            background_persistence_precondition(&ToolContext::default(), false, "child")
                .expect("missing database should reject background start");
        let envelope: serde_json::Value =
            serde_json::from_str(&missing_database.output).expect("structured start error");
        assert_eq!(envelope["error"]["code"], "agent_persistence_error");
        assert!(envelope["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("no durable database")));

        let temp = tempfile::tempdir().expect("tempdir");
        let missing_session = ToolContext {
            db_path: Some(temp.path().join("krusty.db")),
            ..ToolContext::default()
        };
        let error = background_persistence_precondition(&missing_session, true, "build")
            .expect("missing parent session should reject background start");
        let envelope: serde_json::Value =
            serde_json::from_str(&error.output).expect("structured start error");
        assert!(envelope["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("no durable parent session")));
    }

    #[test]
    fn durable_task_text_is_utf8_safe_and_strictly_bounded() {
        let bounded = bounded_text(&"worker-🐝".repeat(4_000), 8 * 1024);
        assert!(bounded.len() <= 8 * 1024);
        assert!(bounded.ends_with("..."));
        assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
    }

    #[test]
    fn durable_task_artifact_stays_below_store_limit() {
        let result = SubAgentResult {
            task_id: "task".repeat(100_000),
            agent_name: "agent".repeat(100_000),
            delegated_run_id: Some("run".to_string()),
            success: false,
            output: "finding".repeat(100_000),
            files_examined: (0..100)
                .map(|index| format!("{index}/{}", "path".repeat(100_000)))
                .collect(),
            duration_ms: 12,
            turns_used: 3,
            error: Some("provider failure".to_string()),
            termination: SubAgentTermination::Failed,
            policy_violations: Vec::new(),
            evidence: Default::default(),
            background_processes: Vec::new(),
        };
        let encoded = serde_json::to_vec(&bounded_task_artifact(&result)).expect("artifact json");
        assert!(encoded.len() < 256 * 1024, "{} bytes", encoded.len());
    }

    #[test]
    fn durable_task_artifact_keeps_luna_shaped_acceptance_evidence() {
        let result = SubAgentResult {
            task_id: "alpha".to_string(),
            agent_name: "alpha".to_string(),
            delegated_run_id: Some("run-alpha".to_string()),
            success: true,
            output: r#"Generic summary without the marker.
<delegated_handoff>{"status":"complete","summary":"proof complete","acceptance_checks":[{"id":"marker","status":"passed","evidence":"alpha-live-proof"}],"remaining_work":[],"blockers":[]}</delegated_handoff>"#.to_string(),
            files_examined: vec!["proof/alpha.txt".to_string()],
            duration_ms: 12,
            turns_used: 1,
            error: None,
            termination: SubAgentTermination::Completed,
            policy_violations: Vec::new(),
            evidence: Default::default(),
            background_processes: Vec::new(),
        };

        let artifact = bounded_task_artifact(&result);
        assert_eq!(artifact["objective_status"], "complete");
        assert_eq!(
            artifact["handoff"]["acceptance_checks"][0]["evidence"],
            "alpha-live-proof"
        );
    }

    #[tokio::test]
    async fn one_task_groups_preserve_role_governance_and_execution_mode() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (ctx, scope) = durable_test_context(&temp);
        let cases = [
            (
                "explore",
                DelegatedRunRole::Explore,
                DelegationPolicy::for_subagent_explore(PermissionMode::Autonomous, Some(7)),
                false,
                Some(7),
            ),
            (
                "build",
                DelegatedRunRole::Build,
                DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(7)),
                false,
                Some(7),
            ),
            (
                "plan",
                DelegatedRunRole::Planner,
                DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(7)),
                true,
                Some(7),
            ),
            (
                "verify",
                DelegatedRunRole::Verifier,
                DelegationPolicy::for_subagent_verify(PermissionMode::Autonomous, Some(7)),
                true,
                Some(7),
            ),
            (
                "default-budget",
                DelegatedRunRole::Explore,
                DelegationPolicy::for_subagent_explore(PermissionMode::Autonomous, None),
                false,
                None,
            ),
        ];

        for (name, role, policy, background, expected_turn_budget) in cases {
            let group_id = format!("single-{name}");
            let delegation = SingleTaskDelegation::create(
                &ctx,
                &group_id,
                name,
                &format!("run {name}"),
                role.clone(),
                &scope,
                &policy,
                background,
                None,
            )
            .expect("single-task group");
            let group = delegation
                .coordinator
                .get_group(&group_id)
                .expect("group lookup")
                .expect("group");

            assert_eq!(group.tasks.len(), 1);
            assert_eq!(group.tasks[0].specification.role, role);
            assert_eq!(
                group.tasks[0].specification.writer_mode,
                DelegationWriterMode::Shared
            );
            assert_eq!(
                group.contract.governance.delegated_turn_budget,
                expected_turn_budget
            );
            let attached = delegation.attach(
                SubAgentTask::new(name, format!("run {name}"))
                    .with_delegation_policy(policy.clone()),
            );
            assert_eq!(
                attached.max_turns_override, expected_turn_budget,
                "runtime task must apply the exact persisted budget"
            );
            assert_eq!(group.contract.governance.max_parallelism, 1);
            assert_eq!(group.contract.governance.delegation_policy, policy);
            assert_eq!(
                group.contract.execution_mode,
                if background {
                    DelegationExecutionMode::Detached
                } else {
                    DelegationExecutionMode::Foreground
                }
            );
        }
    }

    #[tokio::test]
    async fn one_task_terminal_failure_finalizes_without_synthesis_lease() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (ctx, scope) = durable_test_context(&temp);
        let policy = DelegationPolicy::for_subagent_explore(PermissionMode::Autonomous, Some(3));
        let mut delegation = SingleTaskDelegation::create(
            &ctx,
            "single-pipeline",
            "explore",
            "inspect the workspace",
            DelegatedRunRole::Explore,
            &scope,
            &policy,
            false,
            None,
        )
        .expect("single-task group");
        // This test exercises task settlement, not the process-wide adaptive
        // scheduler. A shared scheduler can be hosted by another Tokio test
        // runtime that is concurrently shutting down, making an uncancelled
        // request look like a genuine pre-admission cancellation. Keep a live
        // scheduler scoped to this test so `None` retains its real meaning.
        delegation.coordinator = DelegationCoordinator::with_scheduler(
            ctx.db_path.clone().expect("durable test database"),
            crate::agent::subagent::AgentScheduler::new(Default::default()),
        );
        let task = delegation.attach(
            SubAgentTask::new("explore", "inspect the workspace")
                .with_working_dir(ctx.working_dir.clone())
                .with_sandbox_root(ctx.working_dir.clone())
                .with_delegation_policy(policy),
        );
        let cancellation = tokio_util::sync::CancellationToken::new();
        let task_permit = delegation
            .acquire(&task, "test:model", &cancellation)
            .await
            .expect("task permit");
        let result = SubAgentResult {
            task_id: task.id.clone(),
            agent_name: task.name.clone(),
            delegated_run_id: task.delegated_run_id.clone(),
            success: false,
            output: String::new(),
            files_examined: Vec::new(),
            duration_ms: 0,
            turns_used: 0,
            error: Some("expected test failure".to_string()),
            termination: SubAgentTermination::Failed,
            policy_violations: Vec::new(),
            evidence: Default::default(),
            background_processes: Vec::new(),
        };
        let synthesis_permit = delegation
            .settle(&result, task_permit)
            .expect("task settlement");

        assert_eq!(
            delegation
                .coordinator
                .get_group("single-pipeline")
                .expect("group lookup")
                .expect("group")
                .state,
            DelegationGroupState::Failed
        );
        delegation
            .finalize(synthesis_permit, DelegatedRunStage::Failed)
            .expect("group finalization");
        assert_eq!(
            delegation
                .coordinator
                .get_group("single-pipeline")
                .expect("group lookup")
                .expect("group")
                .state,
            DelegationGroupState::Failed
        );
    }
}
