//! Sub-agent system for parallel task execution
//!
//! Enables spawning lightweight agents to explore the codebase.
//! Sub-agents have read-only access: glob, grep, read.
//! They cannot modify files or execute arbitrary commands.
//!
//! ## Provider-Agnostic Design
//! Sub-agents inherit the parent's immutable resolved model runtime.
//!
//! ## Module Structure
//! - `build_context`: Shared builder coordination context
//! - `types`: Core data types (progress, models, tasks, results)
//! - `tools`: Tool implementations for explorers and builders
//! - `execution`: Agent loop and API communication

pub mod build_context;
mod dependency;
mod execution;
mod identity;
mod isolation;
mod lifecycle;
mod scheduler;
mod spec;
mod tools;
mod types;

use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::agent::AgentCancellation;
use crate::agent::{DelegationCoordinator, DelegationTaskOutcome};
use crate::ai::client::AiClient;

use self::build_context::SharedBuildContext;

// Re-export public types
#[cfg(test)]
pub(crate) use dependency::MAX_DEPENDENCY_CONTEXT_BYTES;
pub use dependency::{attach_direct_dependency_evidence, direct_dependency_evidence_block};
pub use identity::AgentIdentity;
pub(crate) use isolation::BuildIsolationMaterializationGuard;
pub use isolation::BuildIsolationSet;
pub use lifecycle::{
    AgentMailbox, AgentRuntimeManager, AgentRuntimeSnapshot, AgentRuntimeStatus,
    ChildCompletionEvent,
};
pub(crate) use scheduler::SchedulerPermit;
pub use scheduler::{
    AdaptiveConcurrencyPolicy, AgentScheduler, BackpressureSignal, ScheduleRequest,
    SchedulerSnapshot, SchedulingClass,
};
pub use spec::{AgentCapability, AgentContextMode, AgentExecutionProfile, AgentSpec};
pub use tools::BuilderTools;
pub use types::{
    AgentConversationEvent, AgentConversationToolCall, AgentProgress, AgentProgressStatus,
    DelegatedAcceptanceCheck, DelegatedEvidenceKind, DelegatedEvidenceSummary,
    DelegatedProcessArtifact, DelegatedTaskHandoff, SubAgentApiError, SubAgentResult, SubAgentTask,
    SubAgentTermination, TaskObjectiveStatus,
};

// Re-export single agent entry points
pub use execution::execute_single_child;

// Internal execution functions
use execution::execute_builder_with_progress;

/// Pool for managing concurrent sub-agent execution (used by the Build tool)
pub struct SubAgentPool {
    client: Arc<AiClient>,
    cancellation: AgentCancellation,
    /// Optional user ceiling. The default scheduler limit is derived from host
    /// capacity and adapts to observed provider health.
    concurrency_ceiling: Option<usize>,
    /// Durable orchestration authority for normalized group/task execution.
    /// None keeps non-session and migration-era callers on the legacy path.
    delegation_coordinator: Option<DelegationCoordinator>,
}

impl SubAgentPool {
    pub fn new(client: Arc<AiClient>, cancellation: AgentCancellation) -> Self {
        Self {
            client,
            cancellation,
            concurrency_ceiling: None,
            delegation_coordinator: None,
        }
    }

    pub fn with_concurrency(mut self, max: usize) -> Self {
        self.concurrency_ceiling = Some(max.max(1));
        self
    }

    /// Backward-compatible confirmation of the inherited model. A delegated
    /// model change needs its own exactly resolved `AiClient`; silently
    /// reusing this client's credentials and transport would be unsafe.
    pub fn with_override_model(self, model: Option<String>) -> Self {
        if model
            .as_deref()
            .is_some_and(|model| model != self.client.resolved_model().wire_model_id.as_str())
        {
            warn!(
                requested_model = ?model,
                inherited_model = %self.client.resolved_model().wire_model_id,
                "Ignoring delegated model override without an exact resolved client"
            );
        }
        self
    }

    pub fn with_delegation_coordinator(mut self, coordinator: DelegationCoordinator) -> Self {
        self.delegation_coordinator = Some(coordinator);
        self
    }

    /// Get the model to use for sub-agent tasks
    ///
    fn resolve_model(&self) -> String {
        self.client.resolved_model().wire_model_id.clone()
    }

    fn scheduler(&self) -> AgentScheduler {
        // The process-wide scheduler owns provider/host admission. The pool's
        // optional ceiling is retained for API compatibility until group-level
        // durable admission replaces the remaining legacy pool call-sites.
        AgentScheduler::shared()
    }

    pub(crate) async fn acquire_integration_writer(
        &self,
        session_id: impl Into<String>,
        workspace_partition: impl Into<String>,
    ) -> Option<SchedulerPermit> {
        self.scheduler()
            .acquire(
                ScheduleRequest::new(
                    session_id,
                    workspace_partition,
                    SchedulingClass::WriteShared,
                )
                .in_capacity_domain("local/integration"),
                &self.cancellation.child_token(),
            )
            .await
    }

    /// Execute builder tasks with write access and shared context.
    /// The adaptive scheduler controls admission without delaying task creation.
    pub async fn execute_builders(
        &self,
        tasks: Vec<SubAgentTask>,
        context: Arc<SharedBuildContext>,
        progress_tx: mpsc::UnboundedSender<AgentProgress>,
    ) -> Vec<SubAgentResult> {
        let scheduler = self.scheduler();
        let client = self.client.clone();
        let cancellation = self.cancellation.clone();
        let task_count = tasks.len();
        let pool_admission = Arc::new(Semaphore::new(
            self.concurrency_ceiling.unwrap_or(task_count.max(1)).max(1),
        ));

        info!(
            count = task_count,
            concurrency_ceiling = ?self.concurrency_ceiling,
            "SubAgentPool: Spawning builder agents"
        );

        // Per-invocation ownership is the cancellation boundary: dropping this
        // foreground pool aborts its builders without touching another session's
        // independently-owned JoinSet. Background pools keep owning their set.
        let mut task_set = JoinSet::new();

        for (idx, mut task) in tasks.into_iter().enumerate() {
            // Stagger delay between spawns (skip first)
            task.ensure_identity("/root", "builder", idx);
            let scheduler = scheduler.clone();
            let pool_admission = pool_admission.clone();
            let client = client.clone();
            let cancel = cancellation.child_token();
            let context = context.clone();
            let task_id = task.id.clone();
            let progress_tx = progress_tx.clone();
            let resolved_model = self.resolve_model();
            let coordinator = self.delegation_coordinator.clone();

            task_set.spawn(async move {
                emit_task_lifecycle(&progress_tx, &task, AgentProgressStatus::Queued);
                let pool_permit = tokio::select! {
                    permit = pool_admission.acquire_owned() => permit.ok(),
                    _ = cancel.cancelled() => None,
                };
                let Some(_pool_permit) = pool_permit else {
                    emit_task_lifecycle(&progress_tx, &task, AgentProgressStatus::Cancelled);
                    return (idx, cancelled_result(&task));
                };
                if let (Some(coordinator), Some(delegation_task_id)) =
                    (coordinator, task.delegation_task_id.clone())
                {
                    if let Err(error) = coordinator.validate_task_runtime(
                        &delegation_task_id,
                        task.delegation_policy.as_ref(),
                        task.reasoning_effort,
                        &task.working_dir,
                    ) {
                        emit_task_lifecycle(&progress_tx, &task, AgentProgressStatus::Failed);
                        return (idx, coordinator_error_result(&task, error));
                    }
                    let lifecycle_tx = progress_tx.clone();
                    let lifecycle_task = task.clone();
                    let coordinated = coordinator
                        .acquire_task_with_lifecycle(
                            &delegation_task_id,
                            &resolved_model,
                            &cancel,
                            move |state| {
                                emit_task_lifecycle(&lifecycle_tx, &lifecycle_task, state.into());
                            },
                        )
                        .await;
                    let permit = match coordinated {
                        Ok(Some(permit)) => permit,
                        Ok(None) => {
                            if cancel.is_cancelled() {
                                emit_task_lifecycle(
                                    &progress_tx,
                                    &task,
                                    AgentProgressStatus::Cancelled,
                                );
                            }
                            return (idx, cancelled_result(&task));
                        }
                        Err(error) => {
                            emit_task_lifecycle(&progress_tx, &task, AgentProgressStatus::Failed);
                            return (idx, coordinator_error_result(&task, error));
                        }
                    };
                    let execution_cancellation = permit.cancellation();
                    let result = execute_builder_with_progress(
                        &client,
                        task,
                        &resolved_model,
                        execution_cancellation,
                        context,
                        progress_tx.clone(),
                    )
                    .await;
                    return (idx, finish_coordinated_task(result, permit, &progress_tx));
                }
                let request = ScheduleRequest::new(
                    task.parent_session_id
                        .clone()
                        .or_else(|| task.delegated_run_id.clone())
                        .unwrap_or_else(|| task_id.clone()),
                    task.sandbox_root
                        .as_ref()
                        .unwrap_or(&task.working_dir)
                        .display()
                        .to_string(),
                    SchedulingClass::WriteShared,
                )
                .in_capacity_domain(resolved_model.clone());
                let Some(permit) = scheduler.acquire(request, &cancel).await else {
                    emit_task_lifecycle(&progress_tx, &task, AgentProgressStatus::Cancelled);
                    return (idx, cancelled_result(&task));
                };

                if cancel.is_cancelled() {
                    emit_task_lifecycle(&progress_tx, &task, AgentProgressStatus::Cancelled);
                    return (idx, cancelled_result(&task));
                }

                let result = execute_builder_with_progress(
                    &client,
                    task,
                    &resolved_model,
                    cancel,
                    context,
                    progress_tx,
                )
                .await;
                permit.complete(BackpressureSignal::from_error(result.error.as_deref()));
                (idx, result)
            });
        }

        // Collect results in input order even though JoinSet completes them in
        // readiness order, preserving the existing public result contract.
        let mut indexed_results = Vec::with_capacity(task_count);
        let mut join_failures = Vec::new();
        while let Some(joined) = task_set.join_next().await {
            match joined {
                Ok(result) => indexed_results.push(result),
                Err(e) => {
                    warn!("Builder task panicked: {}", e);
                    join_failures.push(SubAgentResult {
                        task_id: "unknown".to_string(),
                        agent_name: "unknown".to_string(),
                        delegated_run_id: None,
                        success: false,
                        output: String::new(),
                        files_examined: vec![],
                        duration_ms: 0,
                        turns_used: 0,
                        error: Some(format!("Task panicked: {}", e)),
                        termination: SubAgentTermination::Failed,
                        policy_violations: vec![],
                        evidence: Default::default(),
                        background_processes: vec![],
                    });
                }
            }
        }
        indexed_results.sort_by_key(|(index, _)| *index);
        let mut results = indexed_results
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>();
        results.extend(join_failures);

        let stats = context.stats();
        info!("SubAgentPool: Builders complete | {}", stats);
        results
    }
}

fn cancelled_result(task: &SubAgentTask) -> SubAgentResult {
    SubAgentResult {
        task_id: task.id.clone(),
        agent_name: task.name.clone(),
        delegated_run_id: task.delegated_run_id.clone(),
        success: false,
        output: String::new(),
        files_examined: vec![],
        duration_ms: 0,
        turns_used: 0,
        error: Some("Cancelled".to_string()),
        termination: SubAgentTermination::Cancelled,
        policy_violations: vec![],
        evidence: Default::default(),
        background_processes: vec![],
    }
}

fn coordinator_error_result(task: &SubAgentTask, error: anyhow::Error) -> SubAgentResult {
    SubAgentResult {
        task_id: task.id.clone(),
        agent_name: task.name.clone(),
        delegated_run_id: task.delegated_run_id.clone(),
        success: false,
        output: String::new(),
        files_examined: vec![],
        duration_ms: 0,
        turns_used: 0,
        error: Some(format!("Delegation coordinator error: {error}")),
        termination: SubAgentTermination::Failed,
        policy_violations: vec![],
        evidence: Default::default(),
        background_processes: vec![],
    }
}

fn emit_task_lifecycle(
    progress_tx: &mpsc::UnboundedSender<AgentProgress>,
    task: &SubAgentTask,
    status: AgentProgressStatus,
) {
    let current_action = match status {
        AgentProgressStatus::Created => "created",
        AgentProgressStatus::Queued => "queued",
        AgentProgressStatus::Leased => "waiting for provider capacity",
        AgentProgressStatus::Running => "starting",
        AgentProgressStatus::Retrying => "retrying",
        AgentProgressStatus::Complete => "done",
        AgentProgressStatus::Degraded => "degraded",
        AgentProgressStatus::Failed => "failed",
        AgentProgressStatus::Cancelled => "cancelled",
    };
    let _ = progress_tx.send(AgentProgress {
        delegated_run_id: task.delegated_run_id.clone(),
        task_id: task.id.clone(),
        name: task.name.clone(),
        identity: task.identity.clone(),
        status,
        current_action: Some(current_action.to_string()),
        ..AgentProgress::default()
    });
}

fn finish_coordinated_task(
    mut result: SubAgentResult,
    permit: crate::agent::CoordinatedTaskPermit,
    progress_tx: &mpsc::UnboundedSender<AgentProgress>,
) -> SubAgentResult {
    let artifact = serde_json::json!({
        "task_id": result.task_id,
        "agent_name": result.agent_name,
        "success": result.success,
        "termination": result.termination,
        "objective_status": result.objective_status(),
        "handoff": result.bounded_delegated_handoff(),
        "summary": result.brief_summary(),
        "files_examined": result.files_examined.iter().take(50).collect::<Vec<_>>(),
        "duration_ms": result.duration_ms,
        "turns_used": result.turns_used,
        "evidence": result.evidence,
        "integration_state": if permit.task().specification.writer_mode == crate::storage::DelegationWriterMode::Isolated {
            "pending"
        } else {
            "ready"
        },
    });
    let (outcome, mut terminal_status) = if result.termination == SubAgentTermination::Cancelled {
        (
            DelegationTaskOutcome::Cancelled,
            AgentProgressStatus::Cancelled,
        )
    } else if result.success
        && result.objective_status() == TaskObjectiveStatus::Complete
        && !result.is_degraded_success()
    {
        (
            DelegationTaskOutcome::Complete(artifact),
            AgentProgressStatus::Complete,
        )
    } else if result.has_partial_evidence() || result.is_degraded_success() {
        (
            DelegationTaskOutcome::Degraded {
                artifact,
                reason: result
                    .error
                    .clone()
                    .unwrap_or_else(|| result.outcome_reason().to_string()),
            },
            AgentProgressStatus::Degraded,
        )
    } else {
        (
            DelegationTaskOutcome::Failed {
                error: result
                    .error
                    .clone()
                    .unwrap_or_else(|| result.outcome_reason().to_string()),
            },
            AgentProgressStatus::Failed,
        )
    };
    if let Err(error) = permit.complete(outcome) {
        result.success = false;
        result.termination = SubAgentTermination::Failed;
        terminal_status = AgentProgressStatus::Failed;
        let persistence_error = format!("Delegation coordinator completion failed: {error}");
        result.error = Some(match result.error.take() {
            Some(existing) => format!("{existing}; {persistence_error}"),
            None => persistence_error,
        });
    }
    let current_action = result
        .error
        .clone()
        .unwrap_or_else(|| result.outcome_reason().to_string());
    let _ = progress_tx.send(AgentProgress {
        delegated_run_id: result.delegated_run_id.clone(),
        task_id: result.task_id.clone(),
        name: result.agent_name.clone(),
        status: terminal_status,
        current_action: Some(current_action),
        completion_summary: Some(result.brief_summary()),
        ..AgentProgress::default()
    });
    result
}

#[cfg(test)]
mod cancellation_tests {
    use std::time::Duration;

    use tokio::task::JoinSet;

    #[tokio::test]
    async fn dropping_foreground_task_set_stops_delayed_write_without_affecting_other_run() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let cancelled_write = temp_dir.path().join("cancelled-session.txt");
        let unaffected_write = temp_dir.path().join("other-session.txt");

        let mut cancelled_session = JoinSet::new();
        cancelled_session.spawn({
            let path = cancelled_write.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(80)).await;
                tokio::fs::write(path, "late mutation")
                    .await
                    .expect("delayed write");
            }
        });

        let mut other_session = JoinSet::new();
        other_session.spawn({
            let path = unaffected_write.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                tokio::fs::write(path, "completed")
                    .await
                    .expect("unaffected write");
            }
        });

        // This is the ownership transition performed when the foreground agent
        // tool future is dropped after LoopInput::Cancel.
        drop(cancelled_session);

        tokio::time::timeout(Duration::from_secs(1), other_session.join_next())
            .await
            .expect("other session should not be cancelled")
            .expect("other session task should complete")
            .expect("other session task should succeed");
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(!cancelled_write.exists());
        assert_eq!(
            std::fs::read_to_string(unaffected_write).expect("other session output"),
            "completed"
        );
    }
}
