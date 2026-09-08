//! Wake parent chat/code sessions when a background child agent completes.
//!
//! Mirrors process completion wake so the parent does not thrash-poll
//! `agent action=status` for a finished delegated_run_id.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context};
use chrono::Utc;
use mitsuro_core::agent::context::build_subagent_project_context;
use mitsuro_core::agent::subagent::{
    direct_dependency_evidence_block, execute_single_child, AgentRuntimeManager, BuildIsolationSet,
    ChildCompletionEvent, DelegatedEvidenceSummary, SubAgentResult, SubAgentTask,
    SubAgentTermination,
};
use mitsuro_core::agent::{
    AgentCancellation, DelegatedRunStage, DelegationCoordinator, DelegationTaskOutcome, LoopInput,
};
use mitsuro_core::ai::providers::ReasoningEffort;
use mitsuro_core::storage::{
    Database, DelegatedRunRecord, DelegatedRunScope, DelegatedRunStartInput, DelegatedRunStore,
    DelegationExecutionMode, DelegationExecutorKind, DelegationExecutorSessionType,
    DelegationGroupRecord, DelegationGroupState, DelegationStore, DelegationTaskRecord,
    DelegationTaskState, DelegationWriterMode, SessionType,
};
use mitsuro_core::SessionManager;
use tokio::sync::mpsc;

use crate::routes::chat::{deliver_steering_with_rollover, resume_child_completion_session};
use crate::AppState;

const IDLE_RESUME_MAX_ATTEMPTS: usize = 3;
const IDLE_RESUME_RETRY_DELAY: Duration = Duration::from_millis(100);
const ABNORMAL_RECONCILE_MAX_ATTEMPTS: usize = 8;
const ABNORMAL_RECONCILE_RETRY_DELAY: Duration = Duration::from_millis(25);
const CHILD_WAKE_RECONCILE_INTERVAL: Duration = Duration::from_secs(15);
const REPLAY_OWNER_LEASE_TTL: Duration = Duration::from_secs(30);
const REPLAY_OWNER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const STARTUP_DELEGATION_RECOVERY_LIMIT: usize = 1_000;

/// Process-local acknowledgement that an exact durable pending ID has already
/// entered the currently active parent input channel. The durable message
/// remains the crash-replay authority; this set only prevents the 15-second
/// reconciler from enqueueing the same steering repeatedly while that parent
/// turn is still running.
#[derive(Clone, Default)]
struct ActiveChildWakeDeliveries {
    pending: Arc<tokio::sync::Mutex<HashSet<(String, String)>>>,
}

impl ActiveChildWakeDeliveries {
    async fn claim(&self, session_id: &str, pending_id: &str) -> bool {
        self.pending
            .lock()
            .await
            .insert((session_id.to_string(), pending_id.to_string()))
    }

    async fn release(&self, session_id: &str, pending_id: &str) {
        self.pending
            .lock()
            .await
            .remove(&(session_id.to_string(), pending_id.to_string()));
    }
}

/// Wire the shared agent runtime manager to session wake handling.
pub async fn install_child_completion_wake(runtime: AgentRuntimeManager, state: AppState) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ChildCompletionEvent>();
    runtime.set_completion_sender(tx.clone());
    let (reconcile_tx, mut reconcile_rx) = mpsc::unbounded_channel::<String>();
    runtime.set_completion_reconciliation_sender(reconcile_tx);
    let active_deliveries = ActiveChildWakeDeliveries::default();

    let recovery_state = state.clone();
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let state = state.clone();
            let active_deliveries = active_deliveries.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    handle_child_completion_with_tracker(&state, event, &active_deliveries).await
                {
                    tracing::warn!(%error, "Failed to deliver child agent completion wake");
                }
            });
        }
    });

    let reconciliation_state = recovery_state.clone();
    tokio::spawn(async move {
        while let Some(delegated_run_id) = reconcile_rx.recv().await {
            let state = reconciliation_state.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    reconcile_abnormal_child_completion(&state, &delegated_run_id).await
                {
                    tracing::warn!(
                        delegated_run_id,
                        %error,
                        "Failed to reconcile abnormal background Agent termination"
                    );
                }
            });
        }
    });

    let durable_recovery_state = recovery_state.clone();
    let durable_recovery_tx = tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(CHILD_WAKE_RECONCILE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Startup recovery below performs the initial scan.
        interval.tick().await;
        loop {
            interval.tick().await;
            // Replayable groups use their own cross-process owner lease. This
            // recurring adoption scan closes the window where a detached host
            // dies after this Honey process has already started.
            if let Err(error) = reconcile_replayable_detached_groups(&durable_recovery_state) {
                tracing::warn!(%error, "Failed to reconcile replayable delegation groups");
            }
            // Re-scan both newly expired owners and every durable pending or
            // unqueued wake. If terminalization won but materialization or
            // live delivery failed transiently, the next tick retries without
            // requiring another server restart.
            match recover_pending_child_completions(&durable_recovery_state) {
                Ok(events) => {
                    for event in events {
                        if durable_recovery_tx.send(event).is_err() {
                            tracing::warn!(
                                "Child completion listener closed during durable wake recovery"
                            );
                            return;
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "Failed to reconcile durable child Agent wakes");
                }
            }
        }
    });

    match reconcile_orphaned_delegation_groups_on_startup(&recovery_state) {
        Ok(report) if report.examined > 0 => {
            tracing::info!(
                examined = report.examined,
                fenced = report.fenced,
                finalized_from_aggregate = report.finalized_from_aggregate,
                live_detached = report.live_detached,
                replay_scheduled = report.replay_scheduled,
                hive_deferred = report.hive_deferred,
                failed = report.failed,
                "Reconciled recoverable delegation groups during server startup"
            );
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, "Failed to enumerate recoverable delegation groups during startup");
        }
    }

    match recover_pending_child_completions(&recovery_state) {
        Ok(events) => {
            for event in events {
                if tx.send(event).is_err() {
                    tracing::warn!("Child completion listener closed during startup recovery");
                    break;
                }
            }
        }
        Err(error) => {
            tracing::warn!(%error, "Failed to scan durable child completions during startup");
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct StartupDelegationRecoveryReport {
    examined: usize,
    fenced: usize,
    finalized_from_aggregate: usize,
    live_detached: usize,
    replay_scheduled: usize,
    hive_deferred: usize,
    failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupDelegationDisposition {
    Fenced,
    FinalizedFromAggregate,
    LiveDetached,
    ReplayScheduled,
    HiveDeferred,
}

/// Reconcile the bounded inventory of groups left by a previous server host.
///
/// Detached Chat/Code tasks carrying the versioned executor envelope are
/// reconstructed under fresh runtime dependencies and the existing durable
/// task/synthesis leases. Foreground work and legacy/malformed envelopes fail
/// closed. Hive sessions are never adopted by this recovery path.
fn reconcile_orphaned_delegation_groups_on_startup(
    state: &AppState,
) -> anyhow::Result<StartupDelegationRecoveryReport> {
    reconcile_orphaned_delegation_groups(state, true)
}

/// Periodic recovery runs while this server may own healthy foreground work.
/// Only detached groups carry the durable host/replay authority needed for a
/// second process to adopt them safely. Foreground groups are fenced only by
/// the one-time startup scan, after the prior server process is known to have
/// exited.
fn reconcile_replayable_detached_groups(
    state: &AppState,
) -> anyhow::Result<StartupDelegationRecoveryReport> {
    reconcile_orphaned_delegation_groups(state, false)
}

fn reconcile_orphaned_delegation_groups(
    state: &AppState,
    include_foreground: bool,
) -> anyhow::Result<StartupDelegationRecoveryReport> {
    let groups = DelegationStore::new(Database::new(&state.db_path)?)
        .list_recoverable_groups(STARTUP_DELEGATION_RECOVERY_LIMIT)?
        .into_iter()
        .filter(|group| {
            include_foreground || group.contract.execution_mode == DelegationExecutionMode::Detached
        })
        .collect::<Vec<_>>();
    let mut report = StartupDelegationRecoveryReport {
        examined: groups.len(),
        ..StartupDelegationRecoveryReport::default()
    };
    for group in groups {
        match reconcile_orphaned_delegation_group(state, &group) {
            Ok(StartupDelegationDisposition::Fenced) => report.fenced += 1,
            Ok(StartupDelegationDisposition::FinalizedFromAggregate) => {
                report.finalized_from_aggregate += 1;
            }
            Ok(StartupDelegationDisposition::LiveDetached) => report.live_detached += 1,
            Ok(StartupDelegationDisposition::ReplayScheduled) => report.replay_scheduled += 1,
            Ok(StartupDelegationDisposition::HiveDeferred) => report.hive_deferred += 1,
            Err(error) => {
                report.failed += 1;
                tracing::warn!(
                    delegation_group_id = %group.delegation_group_id,
                    parent_session_id = %group.parent_session_id,
                    %error,
                    "Failed to reconcile recoverable delegation group"
                );
            }
        }
    }
    Ok(report)
}

fn reconcile_orphaned_delegation_group(
    state: &AppState,
    group: &DelegationGroupRecord,
) -> anyhow::Result<StartupDelegationDisposition> {
    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let session = session_manager
        .get_session(&group.parent_session_id)?
        .with_context(|| {
            format!(
                "delegation group '{}' parent session no longer exists",
                group.delegation_group_id
            )
        })?;
    if session.session_type == SessionType::Hive {
        tracing::info!(
            delegation_group_id = %group.delegation_group_id,
            parent_session_id = %group.parent_session_id,
            "Deferring recoverable delegation group to Hive runtime authority"
        );
        return Ok(StartupDelegationDisposition::HiveDeferred);
    }
    ensure!(
        matches!(session.session_type, SessionType::Chat | SessionType::Code),
        "unsupported parent session type for delegation recovery"
    );

    let delegated_store = DelegatedRunStore::new(Database::new(&state.db_path)?);
    let mut delegated = delegated_store.get_run(&group.delegation_group_id)?;
    let reconstructed_compatibility_row = if delegated.is_none() {
        let task = group
            .tasks
            .first()
            .context("recoverable delegation group has no logical task")?;
        let mut target_scope = task.specification.target_scope.clone();
        if !target_scope.iter().any(|scope| scope.kind == "workspace") {
            let workspace = session
                .project_dir
                .as_deref()
                .or(session.working_dir.as_deref())
                .context("recoverable parent session has no workspace")?;
            target_scope.insert(
                0,
                DelegatedRunScope {
                    label: "recovered launch workspace".to_string(),
                    path: workspace.to_string(),
                    kind: "workspace".to_string(),
                },
            );
        }
        let input = DelegatedRunStartInput {
            delegated_run_id: group.delegation_group_id.clone(),
            parent_session_id: group.parent_session_id.clone(),
            parent_tool_call_id: group.parent_tool_call_id.clone(),
            role: task.specification.role.clone(),
            stage: DelegatedRunStage::Created,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope,
        };
        match group.contract.execution_mode {
            DelegationExecutionMode::Detached => {
                delegated_store.create_background_run(&input)?;
            }
            DelegationExecutionMode::Foreground => {
                delegated_store.create_run(&input)?;
            }
        }
        delegated = delegated_store.get_run(&group.delegation_group_id)?;
        true
    } else {
        false
    };
    let delegated = delegated.context("delegation compatibility row could not be recovered")?;
    ensure!(
        delegated.parent_session_id == group.parent_session_id,
        "delegation compatibility row belongs to another parent session"
    );
    ensure!(
        delegated.parent_tool_call_id == group.parent_tool_call_id,
        "delegation compatibility row belongs to another parent tool call"
    );

    // An existing detached host remains authoritative until its durable lease
    // expires. Fence before scheduling replay so a second server cannot claim
    // still-queued tasks while the live host is preparing to claim them.
    if group.contract.execution_mode == DelegationExecutionMode::Detached
        && !reconstructed_compatibility_row
        && matches!(
            delegated.stage,
            DelegatedRunStage::Created
                | DelegatedRunStage::Running
                | DelegatedRunStage::Synthesizing
        )
        && delegated_background_host_lease_is_live(state, &group.delegation_group_id)?
    {
        return Ok(StartupDelegationDisposition::LiveDetached);
    }

    if group.contract.execution_mode == DelegationExecutionMode::Detached
        && matches!(
            delegated.stage,
            DelegatedRunStage::Created
                | DelegatedRunStage::Running
                | DelegatedRunStage::Synthesizing
        )
        && group
            .tasks
            .iter()
            .any(|task| task.specification.executor_envelope.is_some())
    {
        if let Err(error) = validate_replayable_detached_group(group, &session) {
            fail_replayable_group(state, &group.delegation_group_id, &error.to_string())?;
            return Ok(StartupDelegationDisposition::Fenced);
        }
        let replay_owner_id = match DelegationStore::new(Database::new(&state.db_path)?)
            .try_claim_replay_owner(&group.delegation_group_id)?
        {
            Some(owner_id) => owner_id,
            None => return Ok(StartupDelegationDisposition::LiveDetached),
        };
        let recovery_state = state.clone();
        let delegation_group_id = group.delegation_group_id.clone();
        tokio::spawn(async move {
            if let Err(error) = replay_detached_delegation_group(
                &recovery_state,
                &delegation_group_id,
                &replay_owner_id,
            )
            .await
            {
                tracing::warn!(
                    delegation_group_id,
                    %error,
                    "Detached delegation replay failed closed"
                );
            }
        });
        return Ok(StartupDelegationDisposition::ReplayScheduled);
    }

    if matches!(
        delegated.stage,
        DelegatedRunStage::Complete | DelegatedRunStage::Degraded | DelegatedRunStage::Failed
    ) && matches!(
        group.state,
        DelegationGroupState::ReadyForParent | DelegationGroupState::Synthesizing
    ) {
        DelegationCoordinator::new(state.db_path.as_ref().clone()).finalize_group(
            &group.delegation_group_id,
            group_terminal_state(delegated.stage),
        )?;
        return Ok(StartupDelegationDisposition::FinalizedFromAggregate);
    }

    if matches!(
        delegated.stage,
        DelegatedRunStage::Created | DelegatedRunStage::Running | DelegatedRunStage::Synthesizing
    ) {
        delegated_store.finalize_caller_aborted_run(&group.delegation_group_id, true)?;
    }
    DelegationStore::new(Database::new(&state.db_path)?).fail_group_recovery(
        &group.delegation_group_id,
        "The orchestration host restarted without a replayable executor envelope; child side effects may have occurred and were not replayed.",
    )?;
    Ok(StartupDelegationDisposition::Fenced)
}

fn delegated_background_host_lease_is_live(
    state: &AppState,
    delegated_run_id: &str,
) -> anyhow::Result<bool> {
    let db = Database::new(&state.db_path)?;
    let live = db.conn().query_row(
        "SELECT EXISTS (
            SELECT 1 FROM delegated_runs
             WHERE delegated_run_id = ?1
               AND wake_parent = 1
               AND stage IN ('created', 'running', 'synthesizing')
               AND host_owner_id IS NOT NULL
               AND host_lease_expires_at_ms
                   > (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
         )",
        [delegated_run_id],
        |row| row.get::<_, bool>(0),
    )?;
    Ok(live)
}

fn validate_replayable_detached_group(
    group: &DelegationGroupRecord,
    session: &mitsuro_core::storage::SessionInfo,
) -> anyhow::Result<()> {
    ensure!(
        group.contract.execution_mode == DelegationExecutionMode::Detached,
        "foreground delegation tasks are never replayed"
    );
    ensure!(
        !group.tasks.is_empty(),
        "replayable delegation has no tasks"
    );
    let contains_build = group.tasks.iter().any(|task| {
        task.specification.role == mitsuro_core::storage::DelegatedRunRole::Build
            || task
                .specification
                .executor_envelope
                .as_ref()
                .is_some_and(|envelope| matches!(envelope.kind, DelegationExecutorKind::Build))
    });
    if contains_build {
        ensure!(
            matches!(
                group.contract.completion_policy,
                mitsuro_core::storage::DelegationCompletionPolicy::AllSettled
            ) && matches!(
                group.contract.failure_policy,
                mitsuro_core::storage::DelegationFailurePolicy::Continue
            ),
            "isolated build recovery requires the canonical all-settled continuation contract"
        );
    }
    let session_surface = match session.session_type {
        SessionType::Chat => DelegationExecutorSessionType::Chat,
        SessionType::Code => DelegationExecutorSessionType::Code,
        SessionType::Hive => anyhow::bail!("Hive recovery remains owned by the Hive runtime"),
    };
    let authorized_workspace = session
        .project_dir
        .as_deref()
        .or(session.working_dir.as_deref())
        .context("replayable parent session has no workspace")?;
    let authorized_workspace = PathBuf::from(authorized_workspace)
        .canonicalize()
        .context("replayable parent workspace is unavailable")?;
    let session_project_dir = session
        .project_dir
        .as_deref()
        .map(|path| {
            PathBuf::from(path)
                .canonicalize()
                .context("replayable parent project directory is unavailable")
        })
        .transpose()?;
    for task in &group.tasks {
        let envelope = task
            .specification
            .executor_envelope
            .as_ref()
            .context("detached task is missing its executor envelope")?;
        envelope.validate(&task.specification.objective)?;
        ensure!(
            envelope.session_id == group.parent_session_id && envelope.session_id == session.id,
            "executor envelope belongs to another session"
        );
        ensure!(
            envelope.parent_tool_call_id == group.parent_tool_call_id,
            "executor envelope belongs to another parent tool call"
        );
        ensure!(
            envelope.session_type == session_surface && envelope.user_id == session.user_id,
            "executor envelope session ownership changed"
        );
        ensure!(
            envelope.task_id == task.specification.delegation_task_id
                && envelope.role == task.specification.role,
            "executor envelope task identity changed"
        );
        if matches!(envelope.kind, DelegationExecutorKind::Build) {
            ensure!(
                task.specification.role == mitsuro_core::storage::DelegatedRunRole::Build,
                "build executor kind and role are incompatible"
            );
            if task.specification.writer_mode == DelegationWriterMode::Isolated {
                ensure!(
                    task.specification.attempt_workspace.is_some()
                        && task.specification.workspace_baseline.is_some(),
                    "isolated build replay is missing its durable patch contract"
                );
            }
        }
        let working_dir =
            canonical_replay_envelope_path(&envelope.working_dir, "executor working directory")?;
        let sandbox_root =
            canonical_replay_envelope_path(&envelope.sandbox_root, "executor sandbox root")?;
        let envelope_project_dir = envelope
            .project_dir
            .as_deref()
            .map(|path| canonical_replay_envelope_path(path, "executor project directory"))
            .transpose()?;
        ensure!(
            envelope_project_dir == session_project_dir,
            "executor project directory differs from the parent session"
        );
        ensure!(
            working_dir.starts_with(&sandbox_root),
            "executor working directory escaped its sandbox"
        );
        match task.specification.writer_mode {
            DelegationWriterMode::Isolated => {
                let durable_root = task
                    .specification
                    .attempt_workspace
                    .as_deref()
                    .context("isolated executor has no durable workspace")?;
                let durable_root = PathBuf::from(durable_root)
                    .canonicalize()
                    .context("isolated durable workspace is unavailable")?;
                ensure!(
                    sandbox_root == durable_root,
                    "isolated executor sandbox differs from its durable workspace"
                );
            }
            DelegationWriterMode::Shared => ensure!(
                working_dir.starts_with(&authorized_workspace),
                "shared executor working directory escaped the parent workspace"
            ),
        }
        let kind_matches = match envelope.kind {
            DelegationExecutorKind::Normal => {
                task.specification.role == mitsuro_core::storage::DelegatedRunRole::Explore
            }
            DelegationExecutorKind::Explore => {
                task.specification.role == mitsuro_core::storage::DelegatedRunRole::Explore
            }
            DelegationExecutorKind::Plan => {
                task.specification.role == mitsuro_core::storage::DelegatedRunRole::Planner
            }
            DelegationExecutorKind::Verify => {
                task.specification.role == mitsuro_core::storage::DelegatedRunRole::Verifier
            }
            DelegationExecutorKind::Build => {
                task.specification.role == mitsuro_core::storage::DelegatedRunRole::Build
            }
        };
        ensure!(kind_matches, "executor kind and role are incompatible");
    }
    Ok(())
}

fn canonical_replay_envelope_path(value: &str, label: &str) -> anyhow::Result<PathBuf> {
    let path = PathBuf::from(value);
    ensure!(path.is_absolute(), "{label} must be absolute");
    let canonical = path
        .canonicalize()
        .with_context(|| format!("{label} is unavailable"))?;
    ensure!(
        canonical == path,
        "{label} changed canonical identity after it was persisted"
    );
    Ok(canonical)
}

async fn replay_detached_delegation_group(
    state: &AppState,
    delegation_group_id: &str,
    replay_owner_id: &str,
) -> anyhow::Result<()> {
    let cancellation = AgentCancellation::new();
    let (heartbeat_stop_tx, mut heartbeat_stop_rx) = tokio::sync::oneshot::channel();
    let heartbeat_state = state.clone();
    let heartbeat_group_id = delegation_group_id.to_string();
    let heartbeat_owner_id = replay_owner_id.to_string();
    let heartbeat_cancellation = cancellation.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(REPLAY_OWNER_HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        let mut last_success = Instant::now();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let renewal = Database::new(&heartbeat_state.db_path).and_then(|db| {
                        DelegationStore::new(db)
                            .renew_replay_owner(&heartbeat_group_id, &heartbeat_owner_id)
                    });
                    match renewal {
                        Ok(true) => last_success = Instant::now(),
                        Ok(false) => {
                            heartbeat_cancellation.cancel();
                            anyhow::bail!("detached replay lost its durable group owner lease");
                        }
                        Err(error) if last_success.elapsed() >= REPLAY_OWNER_LEASE_TTL => {
                            heartbeat_cancellation.cancel();
                            return Err(error).context(
                                "detached replay could not renew its group owner lease before expiry",
                            );
                        }
                        Err(error) => {
                            tracing::warn!(
                                delegation_group_id = %heartbeat_group_id,
                                %error,
                                "Transient detached replay owner heartbeat failure"
                            );
                        }
                    }
                }
                _ = &mut heartbeat_stop_rx => return Ok::<_, anyhow::Error>(()),
            }
        }
    });

    let replay_result = replay_detached_delegation_group_inner(
        state,
        delegation_group_id,
        replay_owner_id,
        &cancellation,
    )
    .await;
    let _ = heartbeat_stop_tx.send(());
    let heartbeat_result = heartbeat
        .await
        .context("detached replay heartbeat task panicked")
        .and_then(|result| result);
    let result = replay_result.and(heartbeat_result);
    let replay_store = DelegationStore::new(Database::new(&state.db_path)?);
    let owner_is_current =
        replay_store.replay_owner_is_current(delegation_group_id, replay_owner_id)?;
    if let Err(error) = &result {
        cancellation.cancel();
        if owner_is_current {
            fail_replayable_group(state, delegation_group_id, &error.to_string())?;
        }
    }
    let _ = replay_store.release_replay_owner(delegation_group_id, replay_owner_id)?;
    result
}

async fn replay_detached_delegation_group_inner(
    state: &AppState,
    delegation_group_id: &str,
    replay_owner_id: &str,
    cancellation: &AgentCancellation,
) -> anyhow::Result<()> {
    let coordinator = DelegationCoordinator::new(state.db_path.as_ref().clone());
    let group = coordinator
        .get_group(delegation_group_id)?
        .context("replayable delegation group disappeared")?;
    let session = SessionManager::new(Database::new(&state.db_path)?)
        .get_session(&group.parent_session_id)?
        .context("replayable parent session disappeared")?;
    validate_replayable_detached_group(&group, &session)?;
    let isolated_build = group.tasks.iter().any(|task| {
        task.specification.writer_mode == DelegationWriterMode::Isolated
            && task
                .specification
                .executor_envelope
                .as_ref()
                .is_some_and(|envelope| matches!(envelope.kind, DelegationExecutorKind::Build))
    });

    for wave in delegation_task_waves(&group.tasks) {
        let wave_has_isolated = wave
            .iter()
            .any(|task| task.specification.writer_mode == DelegationWriterMode::Isolated);
        let mut workers = tokio::task::JoinSet::new();
        for task in wave {
            let worker_state = state.clone();
            let worker_cancellation = cancellation.clone();
            workers.spawn(async move {
                replay_detached_task(&worker_state, task, &worker_cancellation).await
            });
        }
        while let Some(result) = workers.join_next().await {
            result.context("detached replay worker panicked")??;
        }
        if wave_has_isolated {
            let store = DelegationStore::new(Database::new(&state.db_path)?);
            let wave_group = store
                .get_group(delegation_group_id)?
                .context("replayed delegation group disappeared before integration")?;
            let project_dir = session
                .project_dir
                .as_deref()
                .or(session.working_dir.as_deref())
                .context("isolated build parent session has no project directory")?;
            recover_isolated_build_integration(
                state,
                PathBuf::from(project_dir),
                &wave_group,
                delegation_group_id,
                replay_owner_id,
                cancellation,
            )
            .await?;
        }
    }

    let store = DelegationStore::new(Database::new(&state.db_path)?);
    let mut group = store
        .get_group(delegation_group_id)?
        .context("replayed delegation group disappeared")?;
    if isolated_build && group_has_pending_integration(&group) {
        let project_dir = session
            .project_dir
            .as_deref()
            .or(session.working_dir.as_deref())
            .context("isolated build parent session has no project directory")?;
        recover_isolated_build_integration(
            state,
            PathBuf::from(project_dir),
            &group,
            delegation_group_id,
            replay_owner_id,
            cancellation,
        )
        .await?;
    }
    let _ = store.reconcile_group(delegation_group_id)?;
    group = store
        .get_group(delegation_group_id)?
        .context("reconciled delegation group disappeared")?;
    let delegated_store = DelegatedRunStore::new(Database::new(&state.db_path)?);
    let delegated = delegated_store
        .get_run(delegation_group_id)?
        .context("replay compatibility row disappeared")?;
    if group.state.is_terminal() {
        let stage = group_stage_from_state(group.state);
        let artifact = replay_group_artifact(&group, "recovered_terminal");
        delegated_store.finalize_run(
            delegation_group_id,
            stage,
            &artifact,
            Some("Detached Agent recovered after server restart."),
            delegated.resumable,
        )?;
        return Ok(());
    }
    ensure!(
        group.state == DelegationGroupState::ReadyForParent
            || group.state == DelegationGroupState::Synthesizing,
        "replayed delegation group did not settle"
    );
    let synthesis = match coordinator.begin_synthesis(delegation_group_id) {
        Ok(synthesis) => synthesis,
        Err(error) => {
            let peer = coordinator
                .get_group(delegation_group_id)?
                .context("delegation group disappeared during synthesis election")?;
            if peer.state.is_terminal()
                || (peer.state == DelegationGroupState::Synthesizing
                    && peer.synthesis_owner_id.is_some()
                    && peer
                        .synthesis_lease_expires_at_ms
                        .is_some_and(|expiry| expiry >= Utc::now().timestamp_millis()))
            {
                // Another recovery process won the exact synthesis lease.
                // Its heartbeat and CAS publication remain authoritative.
                return Ok(());
            }
            return Err(error);
        }
    };
    let terminal = replay_terminal_group_state(&group);
    let stage = group_stage_from_state(terminal);
    let artifact = replay_group_artifact(&group, "recovered_after_restart");
    delegated_store.finalize_run(
        delegation_group_id,
        stage,
        &artifact,
        Some("Detached Agent recovered after server restart."),
        delegated.resumable,
    )?;
    synthesis.finalize(terminal)?;
    Ok(())
}

fn delegation_task_waves(tasks: &[DelegationTaskRecord]) -> Vec<Vec<DelegationTaskRecord>> {
    let mut depths = std::collections::BTreeMap::<String, usize>::new();
    for task in tasks {
        let depth = task
            .specification
            .depends_on
            .iter()
            .filter_map(|dependency| depths.get(dependency))
            .max()
            .copied()
            .unwrap_or(0)
            + usize::from(!task.specification.depends_on.is_empty());
        depths.insert(task.specification.task_key.clone(), depth);
    }
    let max_depth = depths.values().copied().max().unwrap_or(0);
    (0..=max_depth)
        .map(|depth| {
            tasks
                .iter()
                .filter(|task| {
                    depths
                        .get(&task.specification.task_key)
                        .copied()
                        .unwrap_or(0)
                        == depth
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .filter(|wave| !wave.is_empty())
        .collect()
}

fn group_has_pending_integration(group: &DelegationGroupRecord) -> bool {
    group.tasks.iter().any(|task| {
        task.result
            .as_ref()
            .and_then(|result| result.get("integration_state"))
            .and_then(serde_json::Value::as_str)
            == Some("pending")
    })
}

async fn recover_isolated_build_integration(
    state: &AppState,
    project_dir: PathBuf,
    group: &DelegationGroupRecord,
    delegation_group_id: &str,
    replay_owner_id: &str,
    cancellation: &AgentCancellation,
) -> anyhow::Result<()> {
    ensure!(
        !cancellation.child_token().is_cancelled(),
        "isolated build recovery lost ownership before patch restoration"
    );
    let isolated_tasks = group
        .tasks
        .iter()
        .filter(|task| {
            task.specification.writer_mode == DelegationWriterMode::Isolated
                && task.state.is_terminal()
                && task
                    .result
                    .as_ref()
                    .and_then(|result| result.get("integration_state"))
                    .and_then(serde_json::Value::as_str)
                    != Some("complete")
        })
        .collect::<Vec<_>>();
    if isolated_tasks.is_empty() {
        return Ok(());
    }
    let recovered_results = recovered_build_results(group);
    let mut batches = BTreeMap::<String, Vec<&DelegationTaskRecord>>::new();
    for task in &isolated_tasks {
        let workspace = PathBuf::from(
            task.specification
                .attempt_workspace
                .as_deref()
                .context("isolated build task has no durable workspace")?,
        );
        let batch_id = workspace
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            .context("isolated build workspace has no durable batch identity")?;
        ensure!(
            batch_id == delegation_group_id
                || batch_id.starts_with(&format!("{delegation_group_id}-wave-")),
            "isolated build workspace belongs to another durable batch"
        );
        batches.entry(batch_id.to_string()).or_default().push(task);
    }

    let mut integrated = Vec::new();
    for (batch_id, batch_tasks) in batches {
        let durable_workspaces = batch_tasks
            .iter()
            .map(|task| {
                Ok::<_, anyhow::Error>((
                    task.specification.task_key.clone(),
                    PathBuf::from(
                        task.specification
                            .attempt_workspace
                            .as_deref()
                            .context("isolated build task has no durable workspace")?,
                    ),
                    task.specification
                        .workspace_baseline
                        .clone()
                        .context("isolated build task has no durable baseline")?,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let isolation =
            BuildIsolationSet::restore(project_dir.clone(), batch_id, durable_workspaces).await?;
        let batch_results = recovered_results
            .iter()
            .filter(|result| {
                batch_tasks
                    .iter()
                    .any(|task| task.specification.task_key == result.task_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let db_path = state.db_path.clone();
        let fence_group_id = delegation_group_id.to_string();
        let fence_owner_id = replay_owner_id.to_string();
        let replay_cancellation = cancellation.child_token();
        let owner_fence = Arc::new(move || {
            ensure!(
                !replay_cancellation.is_cancelled(),
                "detached build recovery ownership was cancelled"
            );
            let store = DelegationStore::new(Database::new(&db_path)?);
            ensure!(
                store.renew_replay_owner(&fence_group_id, &fence_owner_id)?,
                "detached build recovery owner lease is no longer current"
            );
            ensure!(
                !replay_cancellation.is_cancelled(),
                "detached build recovery ownership was lost during renewal"
            );
            Ok(())
        });
        integrated.extend(
            isolation
                .integrate_recovered(batch_results, owner_fence)
                .await,
        );
    }
    let store = DelegationStore::new(Database::new(&state.db_path)?);
    for task in &isolated_tasks {
        if let Some(result) = integrated
            .iter()
            .find(|result| result.task_id == task.specification.task_key)
        {
            let _ = store.complete_task_integration(
                &task.specification.delegation_task_id,
                result.success,
                result.error.as_deref(),
            )?;
        }
    }
    ensure!(
        isolated_tasks.iter().all(|task| {
            task.state != DelegationTaskState::Complete
                || integrated
                    .iter()
                    .any(|result| result.task_id == task.specification.task_key && result.success)
        }),
        "isolated build aggregate patch integration failed; recovery worktrees were retained"
    );
    Ok(())
}

async fn replay_detached_task(
    state: &AppState,
    task: DelegationTaskRecord,
    replay_cancellation: &AgentCancellation,
) -> anyhow::Result<()> {
    if task.state.is_terminal() {
        return Ok(());
    }
    let envelope = task
        .specification
        .executor_envelope
        .clone()
        .context("replay task lost its executor envelope")?;
    let coordinator = DelegationCoordinator::new(state.db_path.as_ref().clone());
    let cancellation = replay_cancellation.child_token();
    let mut runtime_task = SubAgentTask::new(
        task.specification.task_key.clone(),
        task.specification.objective.clone(),
    )
    .with_name(envelope.task_name.clone())
    .with_working_dir(PathBuf::from(&envelope.working_dir))
    .with_sandbox_root(PathBuf::from(&envelope.sandbox_root))
    .with_delegated_run_id(task.delegation_group_id.clone())
    .with_delegation_task(
        task.delegation_group_id.clone(),
        task.specification.delegation_task_id.clone(),
    )
    .with_dependencies(task.specification.depends_on.clone());
    let group = coordinator
        .get_group(&task.delegation_group_id)?
        .context("replay task group disappeared")?;
    let policy = task
        .specification
        .task_policy
        .clone()
        .unwrap_or_else(|| group.contract.governance.delegation_policy.clone());
    runtime_task = runtime_task
        .with_delegation_policy(policy.clone())
        .with_reasoning_effort(group.contract.governance.reasoning_effort)
        .with_process_context(
            Some(state.process_registry.clone()),
            envelope.user_id.clone(),
            Some(envelope.session_id.clone()),
        );
    let dependency_results = recovered_dependency_results(&group, &runtime_task.depends_on);
    if let Some(block) = direct_dependency_evidence_block(
        &runtime_task,
        &dependency_results,
        &runtime_task.working_dir,
    ) {
        runtime_task.prompt.push_str("\n\n");
        runtime_task.prompt.push_str(&block);
    }
    if let Some(turn_budget) = policy
        .max_turns
        .or(group.contract.governance.delegated_turn_budget)
    {
        runtime_task = runtime_task.with_max_turns(turn_budget);
    }
    coordinator.validate_task_runtime(
        &task.specification.delegation_task_id,
        Some(&policy),
        runtime_task.reasoning_effort,
        &runtime_task.working_dir,
    )?;
    let Some(permit) = coordinator
        .acquire_task(
            &task.specification.delegation_task_id,
            &envelope.resolved_model,
            &cancellation,
        )
        .await?
    else {
        return Ok(());
    };
    if matches!(envelope.kind, DelegationExecutorKind::Build) {
        // Never rerun an expired writer in the same retained worktree. The old
        // process may still be writing despite losing its database lease. Its
        // worktree remains intact for inspection, while terminal successful
        // sibling worktrees can still be synthesized safely.
        permit.complete(DelegationTaskOutcome::Failed {
            error: "Detached isolated builder expired before terminal persistence; replay in the retained worktree is unsafe and remains fail closed."
                .to_string(),
        })?;
        return Ok(());
    }
    let client = state
        .resolve_ai_client_for_key_for_user(&envelope.model_key, envelope.user_id.as_deref())
        .await
        .context("replay model is no longer available")?;
    ensure!(
        client.resolved_model().key == envelope.model_key
            && client.resolved_model().wire_model_id == envelope.resolved_model,
        "resolved replay provider/model differs from the immutable envelope"
    );
    let project_dir = envelope.project_dir.as_deref().map(PathBuf::from);
    let project_context =
        build_subagent_project_context(&runtime_task.working_dir, project_dir.as_deref());
    let result = execute_single_child(
        client,
        runtime_task,
        state.tool_registry.clone(),
        policy,
        project_context,
        envelope.resolved_model,
        permit.cancellation(),
        None,
    )
    .await;
    permit.complete(replay_task_outcome(&result))?;
    Ok(())
}

fn recovered_dependency_results(
    group: &DelegationGroupRecord,
    dependency_ids: &[String],
) -> Vec<SubAgentResult> {
    dependency_ids
        .iter()
        .filter_map(|dependency_id| {
            let task = group
                .tasks
                .iter()
                .find(|task| task.specification.task_key == *dependency_id)?;
            let artifact = task.result.as_ref();
            let handoff = artifact.and_then(|value| value.get("handoff"));
            let output = handoff
                .filter(|value| !value.is_null())
                .and_then(|value| serde_json::to_string(value).ok())
                .map(|handoff| format!("<delegated_handoff>{handoff}</delegated_handoff>"))
                .unwrap_or_default();
            let termination = artifact
                .and_then(|value| value.get("termination"))
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or(match task.state {
                    DelegationTaskState::Complete | DelegationTaskState::Degraded => {
                        SubAgentTermination::Completed
                    }
                    DelegationTaskState::Cancelled => SubAgentTermination::Cancelled,
                    _ => SubAgentTermination::Failed,
                });
            let evidence = artifact
                .and_then(|value| value.get("evidence"))
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_else(DelegatedEvidenceSummary::default);
            Some(SubAgentResult {
                task_id: task.specification.task_key.clone(),
                agent_name: artifact
                    .and_then(|value| value.get("agent_name"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&task.specification.task_key)
                    .to_string(),
                delegated_run_id: Some(group.delegation_group_id.clone()),
                success: task.state == DelegationTaskState::Complete,
                output,
                files_examined: artifact
                    .and_then(|value| value.get("files_examined"))
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect(),
                duration_ms: artifact
                    .and_then(|value| value.get("duration_ms"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                turns_used: artifact
                    .and_then(|value| value.get("turns_used"))
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .unwrap_or_default(),
                error: task.error_summary.clone(),
                termination,
                policy_violations: Vec::new(),
                evidence,
                background_processes: Vec::new(),
            })
        })
        .collect()
}

fn recovered_build_results(group: &DelegationGroupRecord) -> Vec<SubAgentResult> {
    group
        .tasks
        .iter()
        .map(|task| {
            let success = task.state == DelegationTaskState::Complete;
            let artifact = task.result.as_ref();
            let output = artifact
                .and_then(|value| value.get("summary"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            let files_examined = artifact
                .and_then(|value| value.get("files_examined"))
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect();
            SubAgentResult {
                task_id: task.specification.task_key.clone(),
                agent_name: artifact
                    .and_then(|value| value.get("agent_name"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&task.specification.task_key)
                    .to_string(),
                delegated_run_id: Some(group.delegation_group_id.clone()),
                success,
                output,
                files_examined,
                duration_ms: artifact
                    .and_then(|value| value.get("duration_ms"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                turns_used: artifact
                    .and_then(|value| value.get("turns_used"))
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .unwrap_or_default(),
                error: task.error_summary.clone(),
                termination: if success {
                    SubAgentTermination::Completed
                } else if task.state == DelegationTaskState::Cancelled {
                    SubAgentTermination::Cancelled
                } else {
                    SubAgentTermination::Failed
                },
                policy_violations: Vec::new(),
                evidence: Default::default(),
                background_processes: Vec::new(),
            }
        })
        .collect()
}

fn replay_task_outcome(result: &SubAgentResult) -> DelegationTaskOutcome {
    let artifact = serde_json::json!({
        "recovered": true,
        "task_id": bounded_recovery_text(&result.task_id, 256),
        "agent_name": bounded_recovery_text(&result.agent_name, 256),
        "success": result.success,
        "termination": result.termination,
        "summary": bounded_recovery_text(&result.brief_summary(), 8 * 1024),
        "files_examined": result.files_examined.iter().take(32).map(|path| bounded_recovery_text(path, 512)).collect::<Vec<_>>(),
        "duration_ms": result.duration_ms,
        "turns_used": result.turns_used,
        "outcome_reason": bounded_recovery_text(result.outcome_reason(), 1_200),
        "handoff": result.bounded_delegated_handoff(),
        "evidence": result.evidence,
    });
    if result.termination == SubAgentTermination::Cancelled {
        DelegationTaskOutcome::Cancelled
    } else if result.success
        && result.termination == SubAgentTermination::Completed
        && result.has_usable_evidence()
    {
        DelegationTaskOutcome::Complete(artifact)
    } else if result.termination.is_degraded_interruption() && result.has_usable_evidence() {
        DelegationTaskOutcome::Degraded {
            artifact,
            reason: bounded_recovery_text(
                result.error.as_deref().unwrap_or(result.outcome_reason()),
                1_200,
            ),
        }
    } else {
        DelegationTaskOutcome::Failed {
            error: bounded_recovery_text(
                result.error.as_deref().unwrap_or(result.outcome_reason()),
                1_200,
            ),
        }
    }
}

fn replay_terminal_group_state(group: &DelegationGroupRecord) -> DelegationGroupState {
    if group
        .tasks
        .iter()
        .all(|task| task.state == DelegationTaskState::Complete)
    {
        DelegationGroupState::Complete
    } else if group.tasks.iter().any(|task| {
        matches!(
            task.state,
            DelegationTaskState::Complete | DelegationTaskState::Degraded
        )
    }) {
        DelegationGroupState::Degraded
    } else {
        DelegationGroupState::Failed
    }
}

fn group_stage_from_state(state: DelegationGroupState) -> DelegatedRunStage {
    match state {
        DelegationGroupState::Complete => DelegatedRunStage::Complete,
        DelegationGroupState::Degraded => DelegatedRunStage::Degraded,
        DelegationGroupState::Cancelled => DelegatedRunStage::Cancelled,
        DelegationGroupState::Failed => DelegatedRunStage::Failed,
        _ => DelegatedRunStage::Failed,
    }
}

fn replay_group_artifact(group: &DelegationGroupRecord, outcome: &str) -> serde_json::Value {
    serde_json::json!({
        "outcome": outcome,
        "recovered": true,
        "delegation_group_id": group.delegation_group_id,
        "tasks": group.tasks.iter().take(128).map(|task| serde_json::json!({
            "task_id": task.specification.delegation_task_id,
            "state": task.state,
            "summary": task.result.as_ref().and_then(|value| value.get("summary")).and_then(|value| value.as_str()).map(|value| bounded_recovery_text(value, 1024)),
            "error": task.error_summary.as_deref().map(|value| bounded_recovery_text(value, 512)),
        })).collect::<Vec<_>>(),
    })
}

fn fail_replayable_group(
    state: &AppState,
    delegation_group_id: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let reason = bounded_recovery_text(reason, 1_200);
    let artifact = serde_json::json!({
        "outcome": "recovery_failed_closed",
        "delegation_group_id": delegation_group_id,
        "error": reason,
        "warning": "The executor envelope was missing, incompatible, or could not be safely resumed; inspect the workspace before retrying.",
    });
    let delegated_store = DelegatedRunStore::new(Database::new(&state.db_path)?);
    let resumable = delegated_store
        .get_run(delegation_group_id)?
        .map(|run| run.resumable)
        .unwrap_or(false);
    delegated_store.finalize_run(
        delegation_group_id,
        DelegatedRunStage::Failed,
        &artifact,
        Some("Detached Agent recovery failed closed."),
        resumable,
    )?;
    DelegationStore::new(Database::new(&state.db_path)?)
        .fail_group_recovery(delegation_group_id, &reason)?;
    Ok(())
}

fn bounded_recovery_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes.saturating_sub(3);
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &value[..end])
}

#[derive(Clone, Debug)]
struct ValidatedChildCompletion {
    event: ChildCompletionEvent,
    session_id: String,
    workspace_root: PathBuf,
    reasoning_effort: Option<ReasoningEffort>,
}

fn recover_pending_child_completions(
    state: &AppState,
) -> anyhow::Result<Vec<ChildCompletionEvent>> {
    let delegated_store = DelegatedRunStore::new(Database::new(&state.db_path)?);
    let expired = delegated_store.expire_stale_background_host_leases()?;
    if !expired.is_empty() {
        tracing::warn!(
            count = expired.len(),
            "Recovered background Agent runs whose previous host lease expired"
        );
    }
    // First close the crash window where a background run persisted its
    // terminal artifact but the process died before pending steering was
    // queued. The receipt and pending row are committed atomically, so this is
    // safe to repeat on every startup.
    let unqueued = delegated_store.list_unqueued_parent_wakes()?;
    for delegated in unqueued {
        match materialize_durable_child_completion(state, &delegated.delegated_run_id) {
            Ok(
                DurableWakeMaterialization::Ready(_)
                | DurableWakeMaterialization::AlreadyPromoted
                | DurableWakeMaterialization::Suppressed,
            ) => {}
            Ok(DurableWakeMaterialization::NotTerminal) => {
                tracing::warn!(
                    delegated_run_id = %delegated.delegated_run_id,
                    "Startup wake scan returned a non-terminal delegated run"
                );
            }
            Err(error) => {
                tracing::warn!(
                    delegated_run_id = %delegated.delegated_run_id,
                    %error,
                    "Skipping unsafe unqueued child completion during startup recovery"
                );
            }
        }
    }

    let db = Database::new(&state.db_path)?;
    let mut stmt = db.conn().prepare(
        "SELECT session_id, role, content
           FROM messages
          WHERE role LIKE 'pending_user:child-wake-%'
          ORDER BY created_at ASC, id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut pending = Vec::new();
    for row in rows {
        pending.push(row?);
    }
    drop(stmt);
    drop(db);

    let mut events = Vec::new();
    for (session_id, role, content_json) in pending {
        match recover_pending_child_completion(state, &session_id, &role, &content_json) {
            Ok(event) => events.push(event),
            Err(error) => {
                tracing::warn!(
                    session_id,
                    role,
                    %error,
                    "Skipping unsafe durable child completion during startup recovery"
                );
            }
        }
    }
    Ok(events)
}

#[derive(Debug)]
enum DurableWakeMaterialization {
    Ready(Box<ChildCompletionEvent>),
    NotTerminal,
    Suppressed,
    AlreadyPromoted,
}

fn existing_wake_is_publishable(delegated: &DelegatedRunRecord) -> bool {
    delegated.should_wake_parent()
        // Compatibility for pending child-wake rows written before migration
        // 53. The pending row itself proves the old background launch intent;
        // old explicit cancellations were never queued.
        || (!delegated.wake_parent
            && matches!(
                delegated.stage,
                DelegatedRunStage::Complete
                    | DelegatedRunStage::Degraded
                    | DelegatedRunStage::Failed
            ))
}

fn materialize_durable_child_completion(
    state: &AppState,
    delegated_run_id: &str,
) -> anyhow::Result<DurableWakeMaterialization> {
    let delegated = DelegatedRunStore::new(Database::new(&state.db_path)?)
        .get_run(delegated_run_id)?
        .with_context(|| format!("unknown delegated run '{delegated_run_id}'"))?;
    if !matches!(
        delegated.stage,
        DelegatedRunStage::Complete
            | DelegatedRunStage::Degraded
            | DelegatedRunStage::Failed
            | DelegatedRunStage::Cancelled
    ) {
        return Ok(DurableWakeMaterialization::NotTerminal);
    }
    if !delegated.should_wake_parent() {
        return Ok(DurableWakeMaterialization::Suppressed);
    }

    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let session = session_manager
        .get_session(&delegated.parent_session_id)?
        .context("background child parent session no longer exists")?;
    ensure!(
        matches!(session.session_type, SessionType::Chat | SessionType::Code),
        "background child completion cannot wake a Hive-owned session"
    );
    let event = ChildCompletionEvent::from_durable_run(&delegated, session.user_id.clone())?;
    let group_store = DelegationStore::new(Database::new(&state.db_path)?);
    if let Some(group) = group_store.get_group(delegated_run_id)? {
        if delegated.stage == DelegatedRunStage::Cancelled
            && delegated.should_wake_parent()
            && !group.state.is_terminal()
        {
            group_store.fail_group_recovery(
                delegated_run_id,
                "Background Agent ownership expired before aggregate persistence; side effects may have occurred.",
            )?;
        }
        if matches!(
            group.state,
            DelegationGroupState::ReadyForParent | DelegationGroupState::Synthesizing
        ) {
            DelegationCoordinator::new(state.db_path.as_ref().clone())
                .finalize_group(delegated_run_id, group_terminal_state(delegated.stage))?;
        }
        ensure!(
            group_store.authorize_parent_continuation(delegated_run_id, &event.pending_id)?,
            "delegation group does not authorize this parent continuation"
        );
    }
    let event_workspace = event
        .workspace_root
        .as_deref()
        .context("durable child completion has no workspace")?;
    let session_workspace = session
        .project_dir
        .as_deref()
        .or(session.working_dir.as_deref())
        .context("background child parent session has no current project workspace")?;
    let session_workspace = PathBuf::from(session_workspace)
        .canonicalize()
        .context("canonicalizing background child parent workspace")?;
    ensure!(
        session_workspace == event_workspace,
        "background child parent session no longer matches its durable launch workspace"
    );

    let content_json = serde_json::to_string(&event.content)?;
    let queued = session_manager.queue_pending_steering_once(
        &delegated.parent_session_id,
        &event.pending_id,
        &content_json,
    )?;
    if !queued {
        let Some(existing) = session_manager
            .load_pending_steering(&delegated.parent_session_id, &event.pending_id)?
        else {
            if group_store.get_group(delegated_run_id)?.is_some() {
                let _ = group_store
                    .mark_parent_continuation_promoted(delegated_run_id, &event.pending_id)?;
            }
            return Ok(DurableWakeMaterialization::AlreadyPromoted);
        };
        ensure!(
            existing == content_json,
            "existing durable child completion differs from its authoritative terminal artifact"
        );
    }
    if group_store.get_group(delegated_run_id)?.is_some() {
        ensure!(
            group_store.mark_parent_continuation_queued(delegated_run_id, &event.pending_id)?,
            "delegation group lost its continuation queue fence"
        );
    }

    validate_child_completion(state, event.clone())?;
    Ok(DurableWakeMaterialization::Ready(Box::new(event)))
}

async fn reconcile_abnormal_child_completion(
    state: &AppState,
    delegated_run_id: &str,
) -> anyhow::Result<()> {
    let mut last_error = None;
    for attempt in 1..=ABNORMAL_RECONCILE_MAX_ATTEMPTS {
        match materialize_durable_child_completion(state, delegated_run_id) {
            Ok(DurableWakeMaterialization::Ready(event)) => {
                return handle_child_completion(state, *event).await;
            }
            Ok(
                DurableWakeMaterialization::Suppressed
                | DurableWakeMaterialization::AlreadyPromoted,
            ) => return Ok(()),
            Ok(DurableWakeMaterialization::NotTerminal) => {}
            Err(error) => last_error = Some(error),
        }

        if attempt < ABNORMAL_RECONCILE_MAX_ATTEMPTS {
            tokio::time::sleep(ABNORMAL_RECONCILE_RETRY_DELAY.saturating_mul(attempt as u32)).await;
        }
    }

    if let Some(error) = last_error {
        return Err(error.context("abnormal child wake reconciliation exhausted retries"));
    }
    anyhow::bail!(
        "delegated run '{delegated_run_id}' remained non-terminal after abnormal ownership ended"
    )
}

fn recover_pending_child_completion(
    state: &AppState,
    session_id: &str,
    role: &str,
    content_json: &str,
) -> anyhow::Result<ChildCompletionEvent> {
    let pending_id = role
        .strip_prefix("pending_user:")
        .context("recovered completion role is not pending steering")?;
    let delegated_run_id = pending_id
        .strip_prefix("child-wake-")
        .context("recovered completion is not a child wake")?;
    ensure!(
        !delegated_run_id.is_empty(),
        "recovered child wake has no run ID"
    );

    let delegated = DelegatedRunStore::new(Database::new(&state.db_path)?)
        .get_run(delegated_run_id)?
        .context("recovered child wake references an unknown delegated run")?;
    ensure!(
        delegated.parent_session_id == session_id,
        "recovered delegated run belongs to another parent session"
    );
    ensure!(
        existing_wake_is_publishable(&delegated),
        "recovered delegated run is not publishable"
    );
    let workspace_scopes = delegated
        .target_scope
        .iter()
        .filter(|scope| scope.kind == "workspace")
        .collect::<Vec<_>>();
    let [workspace_scope] = workspace_scopes.as_slice() else {
        anyhow::bail!("recovered delegated run has no unique launch workspace");
    };
    let workspace_root = PathBuf::from(&workspace_scope.path)
        .canonicalize()
        .context("canonicalizing recovered launch workspace")?;
    ensure!(
        workspace_root.is_dir(),
        "recovered launch workspace is not a directory"
    );

    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let session = session_manager
        .get_session(session_id)?
        .context("recovered parent session no longer exists")?;
    ensure!(
        matches!(session.session_type, SessionType::Chat | SessionType::Code),
        "recovered child completion cannot wake a Hive-owned session"
    );
    let summary = delegated
        .human_review
        .clone()
        .context("recovered delegated run has no durable review summary")?;
    let terminal_stage = delegated.stage;
    let outcome = delegated
        .artifact
        .as_ref()
        .and_then(|artifact| artifact.get("outcome"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| terminal_stage_label(terminal_stage).to_string());
    let usable_agents = delegated
        .artifact
        .as_ref()
        .and_then(|artifact| artifact.get("usable_agents"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(usize::from(terminal_stage == DelegatedRunStage::Complete));
    let event = ChildCompletionEvent {
        session_id: Some(session_id.to_string()),
        user_id: session.user_id,
        workspace_root: Some(workspace_root),
        pending_id: pending_id.to_string(),
        content: serde_json::from_str(content_json)
            .context("decoding recovered child completion content")?,
        delegated_run_id: delegated_run_id.to_string(),
        task_name: delegated.child_name.unwrap_or_else(|| "child".to_string()),
        terminal_stage,
        outcome,
        usable_agents,
        success: terminal_stage == DelegatedRunStage::Complete,
        summary,
    };
    validate_child_completion(state, event.clone())?;
    Ok(event)
}

async fn handle_child_completion(
    state: &AppState,
    event: ChildCompletionEvent,
) -> anyhow::Result<()> {
    handle_child_completion_with_tracker(state, event, &ActiveChildWakeDeliveries::default()).await
}

async fn handle_child_completion_with_tracker(
    state: &AppState,
    event: ChildCompletionEvent,
    active_deliveries: &ActiveChildWakeDeliveries,
) -> anyhow::Result<()> {
    if event.session_id.is_none() {
        tracing::debug!(
            delegated_run_id = %event.delegated_run_id,
            "Child agent completed without bound session; no wake"
        );
        return Ok(());
    }
    let completion = validate_child_completion(state, event)?;
    let session_id = completion.session_id.as_str();
    let sender = state.session_inputs.read().await.get(session_id).cloned();
    if let Some(sender) = sender {
        if !active_deliveries
            .claim(session_id, &completion.event.pending_id)
            .await
        {
            tracing::debug!(
                session_id,
                delegated_run_id = %completion.event.delegated_run_id,
                pending_id = %completion.event.pending_id,
                "Skipped duplicate child completion delivery to the active parent run"
            );
            return Ok(());
        }
        let input = LoopInput::Steer {
            pending_id: Some(completion.event.pending_id.clone()),
            content: completion.event.content.clone(),
        };
        let delivered = deliver_steering_with_rollover(state, session_id, sender, input).await;
        if delivered {
            tracing::info!(
                session_id,
                delegated_run_id = %completion.event.delegated_run_id,
                name = %completion.event.task_name,
                pending_id = %completion.event.pending_id,
                "Delivered durable child completion to active session"
            );

            // Acceptance by an input channel is not proof that the finishing
            // run promoted the durable row. Re-check after its canonical lock
            // is released and resume only if this exact pending ID remains.
            let state = state.clone();
            let active_deliveries = active_deliveries.clone();
            let claimed_session_id = session_id.to_string();
            let claimed_pending_id = completion.event.pending_id.clone();
            tokio::spawn(async move {
                let recovery = ensure_completion_resumed(&state, completion).await;
                active_deliveries
                    .release(&claimed_session_id, &claimed_pending_id)
                    .await;
                if let Err(error) = recovery {
                    tracing::warn!(%error, "Failed child completion post-run recovery");
                }
            });
            return Ok(());
        }
        active_deliveries
            .release(session_id, &completion.event.pending_id)
            .await;
    }

    active_deliveries
        .release(session_id, &completion.event.pending_id)
        .await;
    ensure_completion_resumed(state, completion).await?;
    Ok(())
}

fn validate_child_completion(
    state: &AppState,
    event: ChildCompletionEvent,
) -> anyhow::Result<ValidatedChildCompletion> {
    let session_id = event
        .session_id
        .clone()
        .context("child completion has no parent session")?;
    ensure!(
        event.pending_id == format!("child-wake-{}", event.delegated_run_id),
        "child completion pending ID does not match its delegated run"
    );

    let delegated = DelegatedRunStore::new(Database::new(&state.db_path)?)
        .get_run(&event.delegated_run_id)?
        .context("child completion references an unknown delegated run")?;
    ensure!(
        delegated.parent_session_id == session_id,
        "child completion delegated run belongs to a different parent session"
    );
    ensure!(
        existing_wake_is_publishable(&delegated),
        "child completion delegated run is not publishable"
    );
    let group_store = DelegationStore::new(Database::new(&state.db_path)?);
    let group = group_store.get_group(&event.delegated_run_id)?;
    if group.is_some() {
        ensure!(
            group_store
                .authorize_parent_continuation(&event.delegated_run_id, &event.pending_id,)?,
            "child completion delegation group is not publishable"
        );
    }
    ensure!(
        event.success == (delegated.stage == DelegatedRunStage::Complete),
        "child completion outcome does not match its durable terminal stage"
    );
    ensure!(
        event.terminal_stage == delegated.stage,
        "child completion terminal stage does not match its durable run"
    );
    let durable_outcome = delegated
        .artifact
        .as_ref()
        .and_then(|artifact| artifact.get("outcome"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| terminal_stage_label(delegated.stage));
    ensure!(
        event.outcome == durable_outcome,
        "child completion outcome label does not match its durable artifact"
    );
    let durable_usable_agents = delegated
        .artifact
        .as_ref()
        .and_then(|artifact| artifact.get("usable_agents"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .unwrap_or(usize::from(delegated.stage == DelegatedRunStage::Complete));
    ensure!(
        event.usable_agents == durable_usable_agents,
        "child completion usable-agent count does not match its durable artifact"
    );
    ensure!(
        delegated.human_review.as_deref() == Some(event.summary.as_str()),
        "child completion summary does not match its durable result"
    );
    ensure!(
        delegated.completed_at.is_some(),
        "child completion delegated run has no durable completion timestamp"
    );
    ensure!(
        delegated.artifact.is_some(),
        "child completion delegated run has no durable artifact"
    );

    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let session = session_manager
        .get_session(&session_id)?
        .context("child completion parent session no longer exists")?;
    ensure!(
        session.user_id == event.user_id,
        "child completion owner does not match its parent session"
    );
    ensure!(
        matches!(session.session_type, SessionType::Chat | SessionType::Code),
        "child completion cannot wake a Hive-owned session"
    );
    let session_workspace = session
        .project_dir
        .as_deref()
        .or(session.working_dir.as_deref())
        .context("child completion parent session has no current project workspace")?;

    let durable_content = session_manager
        .load_pending_steering(&session_id, &event.pending_id)?
        .context("child completion has no durable pending steering row")?;
    ensure!(
        durable_content == serde_json::to_string(&event.content)?,
        "child completion live content does not match its durable row"
    );

    let workspace_root = event
        .workspace_root
        .as_deref()
        .context("child completion has no captured workspace authority")?
        .canonicalize()
        .context("canonicalizing child completion workspace authority")?;
    ensure!(
        workspace_root.is_dir(),
        "child completion workspace authority is not a directory"
    );
    let workspace_scopes = delegated
        .target_scope
        .iter()
        .filter(|scope| scope.kind == "workspace")
        .collect::<Vec<_>>();
    let [workspace_scope] = workspace_scopes.as_slice() else {
        anyhow::bail!("child completion delegated run has no unique launch workspace");
    };
    let durable_workspace_root = PathBuf::from(&workspace_scope.path)
        .canonicalize()
        .context("canonicalizing delegated launch workspace")?;
    ensure!(
        durable_workspace_root.starts_with(&workspace_root),
        "child completion durable launch workspace escapes its captured authority"
    );
    let current_session_workspace = PathBuf::from(session_workspace)
        .canonicalize()
        .context("canonicalizing parent session project workspace")?;
    ensure!(
        current_session_workspace == durable_workspace_root,
        "child completion parent session project no longer matches its durable launch workspace"
    );

    Ok(ValidatedChildCompletion {
        event,
        session_id,
        workspace_root: durable_workspace_root,
        reasoning_effort: group.and_then(|group| group.contract.governance.reasoning_effort),
    })
}

fn terminal_stage_label(stage: DelegatedRunStage) -> &'static str {
    match stage {
        DelegatedRunStage::Created => "created",
        DelegatedRunStage::Running => "running",
        DelegatedRunStage::Synthesizing => "synthesizing",
        DelegatedRunStage::Complete => "complete",
        DelegatedRunStage::Degraded => "degraded",
        DelegatedRunStage::Failed => "failed",
        DelegatedRunStage::Cancelled => "cancelled",
    }
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

fn mark_group_parent_continuation_promoted(
    db_path: &std::path::Path,
    pending_id: &str,
) -> Result<(), crate::error::AppError> {
    let Some(delegation_group_id) = pending_id.strip_prefix("child-wake-") else {
        return Ok(());
    };
    let group_store =
        DelegationStore::new(Database::new(db_path).map_err(crate::error::AppError::from)?);
    if group_store
        .get_group(delegation_group_id)
        .map_err(crate::error::AppError::from)?
        .is_some()
        && !group_store
            .mark_parent_continuation_promoted(delegation_group_id, pending_id)
            .map_err(crate::error::AppError::from)?
    {
        return Err(crate::error::AppError::Conflict(
            "delegation group lost its promoted continuation fence".to_string(),
        ));
    }
    Ok(())
}

async fn ensure_completion_resumed(
    state: &AppState,
    completion: ValidatedChildCompletion,
) -> anyhow::Result<bool> {
    let pending_id = completion.event.pending_id.clone();
    let reasoning_effort = completion.reasoning_effort;
    ensure_completion_resumed_with(
        state,
        completion,
        move |state, session_id, user_id, workspace_root, guard| {
            let pending_id = pending_id.clone();
            let reasoning_effort = reasoning_effort;
            async move {
                let auto_resume = parent_stop_reason_allows_child_auto_resume(
                    state.db_path.as_path(),
                    &session_id,
                )?;
                let promoted = SessionManager::new(Database::new(&state.db_path)?)
                    .promote_pending_steering(&session_id, &pending_id)?;
                if promoted.is_none() {
                    return Ok(());
                }

                if !auto_resume {
                    mark_group_parent_continuation_promoted(
                        state.db_path.as_path(),
                        &pending_id,
                    )?;
                    tracing::info!(
                        session_id,
                        pending_id,
                        "Promoted child completion after an abnormal parent stop without starting a hidden continuation"
                    );
                    drop(guard);
                    return Ok(());
                }

                resume_child_completion_session(
                    &state,
                    &session_id,
                    user_id,
                    workspace_root,
                    reasoning_effort,
                    guard,
                )
                .await?;
                // `promoted` is the durable UI/proof fence that the
                // continuation has started, not merely that its steering row
                // moved into conversation history. Publish it only after the
                // detached run has registered its live input channel.
                mark_group_parent_continuation_promoted(state.db_path.as_path(), &pending_id)?;
                Ok(())
            }
        },
    )
    .await
}

fn parent_stop_reason_allows_child_auto_resume(
    db_path: &std::path::Path,
    session_id: &str,
) -> Result<bool, crate::error::AppError> {
    let db = Database::new(db_path).map_err(crate::error::AppError::from)?;
    let mut statement = db
        .conn()
        .prepare(
            "SELECT stop_reason
               FROM runtime_traces
              WHERE session_id = ?1
                AND event_type = 'finished'
                AND stop_reason IS NOT NULL
              ORDER BY sequence DESC
              LIMIT 1",
        )
        .map_err(|error| crate::error::AppError::from(anyhow::Error::new(error)))?;
    let mut rows = statement
        .query([session_id])
        .map_err(|error| crate::error::AppError::from(anyhow::Error::new(error)))?;
    let stop_reason = rows
        .next()
        .map_err(|error| crate::error::AppError::from(anyhow::Error::new(error)))?
        .map(|row| row.get::<_, String>(0))
        .transpose()
        .map_err(|error| crate::error::AppError::from(anyhow::Error::new(error)))?;
    Ok(stop_reason_allows_child_auto_resume(stop_reason.as_deref()))
}

fn stop_reason_allows_child_auto_resume(stop_reason: Option<&str>) -> bool {
    stop_reason.is_none_or(|reason| reason == "completed")
}

async fn ensure_completion_resumed_with<R, F>(
    state: &AppState,
    completion: ValidatedChildCompletion,
    resume: R,
) -> anyhow::Result<bool>
where
    R: FnMut(AppState, String, Option<String>, PathBuf, tokio::sync::OwnedMutexGuard<()>) -> F,
    F: std::future::Future<Output = Result<(), crate::error::AppError>>,
{
    ensure_completion_resumed_with_policy(
        state,
        completion,
        IDLE_RESUME_MAX_ATTEMPTS,
        IDLE_RESUME_RETRY_DELAY,
        resume,
    )
    .await
}

async fn ensure_completion_resumed_with_policy<R, F>(
    state: &AppState,
    completion: ValidatedChildCompletion,
    max_attempts: usize,
    retry_delay: Duration,
    mut resume: R,
) -> anyhow::Result<bool>
where
    R: FnMut(AppState, String, Option<String>, PathBuf, tokio::sync::OwnedMutexGuard<()>) -> F,
    F: std::future::Future<Output = Result<(), crate::error::AppError>>,
{
    let max_attempts = max_attempts.max(1);
    for attempt in 1..=max_attempts {
        // Every attempt reacquires the canonical session lock. The resume
        // future owns this guard, so a failed attempt releases it before the
        // bounded delay and next durable pending-row check.
        let guard = state.lock_session(&completion.session_id).await;
        // A live input channel is the canonical foreground-run handoff even
        // during the narrow interval after that run releases its session
        // mutex and before stream cleanup removes the channel. Starting a
        // detached continuation in that interval creates two concurrent
        // parent runs and can publish a false terminal state for the healthy
        // foreground run. Leave the durable wake pending; the active run will
        // promote it, or the recurring reconciler will revisit it after the
        // channel is removed and the foreground stop reason is durable.
        if state
            .session_inputs
            .read()
            .await
            .contains_key(&completion.session_id)
        {
            drop(guard);
            tracing::debug!(
                session_id = %completion.session_id,
                delegated_run_id = %completion.event.delegated_run_id,
                pending_id = %completion.event.pending_id,
                "Deferred child completion recovery while the foreground input channel remains active"
            );
            return Ok(false);
        }
        // The input registry is intentionally ephemeral and can have a short
        // handoff gap during transport rollover or recovery. Durable run
        // traces close that gap: a newer parent run without its Finished
        // boundary still owns continuation authority even when no channel is
        // momentarily registered. Keep the wake pending so the foreground run
        // can consume it, or so a later reconciliation can classify its
        // terminal stop reason.
        if parent_latest_run_is_unfinished(state.db_path.as_path(), &completion.session_id)? {
            drop(guard);
            tracing::debug!(
                session_id = %completion.session_id,
                delegated_run_id = %completion.event.delegated_run_id,
                pending_id = %completion.event.pending_id,
                "Deferred child completion recovery while the latest durable parent run remains unfinished"
            );
            return Ok(false);
        }
        let session_manager = SessionManager::new(Database::new(&state.db_path)?);
        if !session_manager
            .has_pending_steering(&completion.session_id, &completion.event.pending_id)?
        {
            let group_store = DelegationStore::new(Database::new(&state.db_path)?);
            if group_store
                .get_group(&completion.event.delegated_run_id)?
                .is_some()
            {
                if let Err(error) = group_store.mark_parent_continuation_promoted(
                    &completion.event.delegated_run_id,
                    &completion.event.pending_id,
                ) {
                    tracing::warn!(
                        %error,
                        delegated_run_id = %completion.event.delegated_run_id,
                        pending_id = %completion.event.pending_id,
                        "Pending child wake was promoted but group projection could not be advanced"
                    );
                }
            }
            tracing::debug!(
                session_id = %completion.session_id,
                delegated_run_id = %completion.event.delegated_run_id,
                pending_id = %completion.event.pending_id,
                "Child completion was already promoted by an active or replacement run"
            );
            return Ok(false);
        }

        match resume(
            state.clone(),
            completion.session_id.clone(),
            completion.event.user_id.clone(),
            completion.workspace_root.clone(),
            guard,
        )
        .await
        {
            Ok(()) => {
                tracing::info!(
                    session_id = %completion.session_id,
                    delegated_run_id = %completion.event.delegated_run_id,
                    pending_id = %completion.event.pending_id,
                    attempt,
                    "Started detached parent continuation for child completion"
                );
                return Ok(true);
            }
            Err(error) if resume_error_is_transient(&error) && attempt < max_attempts => {
                tracing::warn!(
                    session_id = %completion.session_id,
                    delegated_run_id = %completion.event.delegated_run_id,
                    pending_id = %completion.event.pending_id,
                    attempt,
                    max_attempts,
                    error = ?error,
                    "Detached child completion resume failed transiently; retrying"
                );
                tokio::time::sleep(retry_delay.saturating_mul(attempt as u32)).await;
            }
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "child completion resume failed on attempt {attempt}/{max_attempts}: {error:?}"
                ));
            }
        }
    }

    unreachable!("at least one child completion resume attempt is required")
}

fn parent_latest_run_is_unfinished(
    db_path: &std::path::Path,
    session_id: &str,
) -> anyhow::Result<bool> {
    let db = Database::new(db_path)?;
    let unfinished = db.conn().query_row(
        "WITH latest_run AS (
             SELECT run_id
               FROM runtime_traces
              WHERE session_id = ?1
                AND event_type = 'run_budget_resolved'
              ORDER BY sequence DESC
              LIMIT 1
         )
         SELECT EXISTS (
             SELECT 1
               FROM latest_run latest
              WHERE NOT EXISTS (
                    SELECT 1
                      FROM runtime_traces finished
                     WHERE finished.session_id = ?1
                       AND finished.run_id = latest.run_id
                       AND finished.event_type = 'finished'
              )
         )",
        [session_id],
        |row| row.get::<_, bool>(0),
    )?;
    Ok(unfinished)
}

fn resume_error_is_transient(error: &crate::error::AppError) -> bool {
    matches!(
        error,
        crate::error::AppError::Conflict(_)
            | crate::error::AppError::ServiceUnavailable(_)
            | crate::error::AppError::BadGateway(_)
            | crate::error::AppError::Internal(_)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use mitsuro_core::agent::{AgentCancellation, DelegatedRunStage};
    use mitsuro_core::ai::models::{create_model_registry, ApiFormat, ModelKey};
    use mitsuro_core::ai::providers::ProviderId;
    use mitsuro_core::ai::types::Content;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::{
        DelegatedRunRole, DelegatedRunScope, DelegatedRunStartInput, DelegationCompletionPolicy,
        DelegationExecutorEnvelopeV1, DelegationFailurePolicy, DelegationGovernance,
        DelegationGroupContract, DelegationGroupStartInput, DelegationTaskSpec,
        DelegationWriterMode, WorkspaceMode,
    };
    use mitsuro_core::tools::registry::{DelegationPolicy, PermissionMode, ToolRegistry};
    use tokio::sync::{mpsc, Mutex, RwLock};

    use super::*;

    #[test]
    fn child_auto_resume_requires_a_normal_parent_stop() {
        assert!(stop_reason_allows_child_auto_resume(None));
        assert!(stop_reason_allows_child_auto_resume(Some("completed")));
        for reason in [
            "provider_error",
            "budget_exhausted",
            "loop_guard",
            "cancelled",
        ] {
            assert!(!stop_reason_allows_child_auto_resume(Some(reason)));
        }
    }

    #[tokio::test]
    async fn recovered_dependency_uses_durable_handoff_without_summary_transcript() {
        let (state, _temp, workspace) = test_state();
        let (_event, session_id) = seed_completion(&state, &workspace);
        let group_id = "dependency-replay";
        seed_recoverable_group(
            &state,
            &workspace,
            &session_id,
            group_id,
            DelegationExecutionMode::Detached,
        );
        let mut group =
            DelegationStore::new(Database::new(&state.db_path).expect("group database"))
                .get_group(group_id)
                .expect("group lookup")
                .expect("group");
        let mut dependency = group.tasks[0].clone();
        dependency.specification.task_key = "foundation".to_string();
        dependency.state = DelegationTaskState::Complete;
        dependency.result = Some(serde_json::json!({
            "agent_name": "foundation",
            "termination": "completed",
            "summary": "SUMMARY TRANSCRIPT MUST NOT CROSS",
            "handoff": {
                "status": "complete",
                "summary": "durable contract ready",
                "acceptance_checks": [{
                    "id": "contract",
                    "status": "passed",
                    "evidence": "schema parsed"
                }],
                "remaining_work": [],
                "blockers": [],
                "generated_artifacts": []
            },
            "evidence": {
                "attempted": 1,
                "succeeded": 1,
                "observations": 1,
                "mutations": 0,
                "executions": 0,
                "acceptance_proofs": []
            }
        }));
        group.tasks.push(dependency);

        let recovered = recovered_dependency_results(&group, &["foundation".to_string()]);
        let task = SubAgentTask::new("consumer", "consume")
            .with_dependencies(vec!["foundation".to_string()]);
        let block = direct_dependency_evidence_block(&task, &recovered, &workspace)
            .expect("durable dependency evidence");

        assert!(block.contains("durable contract ready"));
        assert!(block.contains("schema parsed"));
        assert!(!block.contains("SUMMARY TRANSCRIPT MUST NOT CROSS"));
    }

    fn test_state() -> (AppState, tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().expect("tempdir");
        let db_path = temp.path().join("krusty.db");
        Database::new(&db_path).expect("database should initialize");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace should exist");
        let state = AppState {
            server_port: 3000,
            db_path: Arc::new(db_path),
            working_dir: Arc::new(workspace.clone()),
            ai_client: None,
            tool_registry: Arc::new(ToolRegistry::new()),
            process_registry: Arc::new(ProcessRegistry::new()),
            model_registry: create_model_registry(),
            credential_store: Arc::new(RwLock::new(CredentialStore::default())),
            mcp_manager: Arc::new(McpManager::new(workspace.clone())),
            hook_manager: Arc::new(RwLock::new(mitsuro_core::agent::UserHookManager::new())),
            skills_manager: Arc::new(RwLock::new(SkillsManager::with_defaults(&workspace))),
            cancellation: AgentCancellation::new(),
            session_locks: Arc::new(RwLock::new(HashMap::new())),
            session_inputs: Arc::new(RwLock::new(HashMap::new())),
            session_presence: Arc::new(RwLock::new(HashMap::new())),
            session_control_leases: Arc::new(RwLock::new(
                crate::control_leases::SessionControlLeaseRegistry::default(),
            )),
            agent_streams: Arc::new(RwLock::new(crate::agent_streams::AgentStreamHub::default())),
            delegated_state: Arc::new(RwLock::new(HashMap::new())),
            device_access: Default::default(),
            remote_access: Arc::new(RwLock::new(crate::remote_access::RemoteAccessConfig {
                enabled: true,
                token: String::new(),
            })),
            active_agent_streams: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_rss_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peak_virtual_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            push_service: None,
            apns_service: None,
            oauth_flows: Arc::new(Mutex::new(HashMap::new())),
            hive_runtime: crate::hive_runtime::HiveRuntimeManager::new(),
        };
        (state, temp, workspace)
    }

    fn seed_completion(
        state: &AppState,
        workspace: &std::path::Path,
    ) -> (ChildCompletionEvent, String) {
        let db = Database::new(&state.db_path).expect("database should open");
        db.conn()
            .execute(
                "INSERT INTO users (id, email, license_tier) VALUES ('alice', 'a@test', 'free')",
                [],
            )
            .expect("user should insert");
        let manager = SessionManager::new(db);
        let session_id = manager
            .create_session_for_user_with_config(
                "Parent",
                None,
                Some(workspace.to_string_lossy().as_ref()),
                Some(workspace.to_string_lossy().as_ref()),
                WorkspaceMode::Selected,
                Some("alice"),
                None,
                SessionType::Code,
            )
            .expect("session should create");
        let delegated_run_id = "child-run-1".to_string();
        let store = DelegatedRunStore::new(
            Database::new(&state.db_path).expect("delegated database should open"),
        );
        store
            .create_background_run_with_child_contract(
                &DelegatedRunStartInput {
                    delegated_run_id: delegated_run_id.clone(),
                    parent_session_id: session_id.clone(),
                    parent_tool_call_id: Some("tool-1".into()),
                    role: DelegatedRunRole::Explore,
                    stage: DelegatedRunStage::Running,
                    provider: None,
                    model: None,
                    resumable: true,
                    resumed_from_run_id: None,
                    target_scope: vec![
                        DelegatedRunScope {
                            label: "launch workspace".into(),
                            path: workspace
                                .canonicalize()
                                .expect("canonical workspace")
                                .to_string_lossy()
                                .into_owned(),
                            kind: "workspace".into(),
                        },
                        DelegatedRunScope {
                            label: "project".into(),
                            path: ".".into(),
                            kind: "project".into(),
                        },
                    ],
                },
                Some("research"),
                &Default::default(),
            )
            .expect("delegated run should create");
        store
            .finalize_run(
                &delegated_run_id,
                DelegatedRunStage::Complete,
                &serde_json::json!({"result": "done"}),
                Some("done"),
                true,
            )
            .expect("delegated run should finalize");

        let pending_id = format!("child-wake-{delegated_run_id}");
        let content = vec![Content::text(
            "[CHILD AGENT COMPLETE]\nsummary:\ndone".into(),
        )];
        let content_json = serde_json::to_string(&content).expect("content should serialize");
        assert!(SessionManager::new(
            Database::new(&state.db_path).expect("queue database should open")
        )
        .queue_pending_steering_once(&session_id, &pending_id, &content_json)
        .expect("completion should queue"));

        (
            ChildCompletionEvent {
                session_id: Some(session_id.clone()),
                user_id: Some("alice".into()),
                workspace_root: Some(workspace.to_path_buf()),
                pending_id,
                content,
                delegated_run_id,
                task_name: "research".into(),
                terminal_stage: DelegatedRunStage::Complete,
                outcome: "complete".into(),
                usable_agents: 1,
                success: true,
                summary: "done".into(),
            },
            session_id,
        )
    }

    fn seed_recoverable_group(
        state: &AppState,
        workspace: &std::path::Path,
        session_id: &str,
        group_id: &str,
        execution_mode: DelegationExecutionMode,
    ) {
        let store = DelegationStore::new(
            Database::new(&state.db_path).expect("delegation database should open"),
        );
        store
            .create_group(&DelegationGroupStartInput {
                delegation_group_id: group_id.to_string(),
                parent_session_id: session_id.to_string(),
                parent_tool_call_id: Some("tool-1".to_string()),
                contract: DelegationGroupContract {
                    execution_mode,
                    completion_policy: DelegationCompletionPolicy::AllSettled,
                    failure_policy: DelegationFailurePolicy::Continue,
                    governance: DelegationGovernance {
                        permission_mode: PermissionMode::Supervised,
                        reasoning_effort: None,
                        delegated_turn_budget: Some(12),
                        max_parallelism: 1,
                        execution_tool_allowlist: Some(BTreeSet::from(["read".to_string()])),
                        delegation_policy: DelegationPolicy::for_subagent_explore(
                            PermissionMode::Supervised,
                            Some(12),
                        ),
                    },
                },
                tasks: vec![DelegationTaskSpec {
                    delegation_task_id: format!("{group_id}:task:0"),
                    task_key: "recover".to_string(),
                    objective: "Recover without replaying an incomplete executor".to_string(),
                    role: DelegatedRunRole::Explore,
                    target_scope: vec![DelegatedRunScope {
                        label: "launch workspace".to_string(),
                        path: workspace
                            .canonicalize()
                            .expect("canonical workspace")
                            .to_string_lossy()
                            .into_owned(),
                        kind: "workspace".to_string(),
                    }],
                    max_attempts: 2,
                    depends_on: Vec::new(),
                    write_intent: Vec::new(),
                    task_policy: None,
                    writer_mode: DelegationWriterMode::Shared,
                    attempt_workspace: None,
                    workspace_baseline: None,
                    executor_envelope: None,
                }],
            })
            .expect("recoverable group should create");
        store
            .queue_group(group_id)
            .expect("recoverable group should queue");
    }

    fn attach_replay_envelope(
        state: &AppState,
        workspace: &std::path::Path,
        session_id: &str,
        group_id: &str,
        user_id: Option<&str>,
    ) {
        let objective = "Recover without replaying an incomplete executor";
        let envelope = DelegationExecutorEnvelopeV1 {
            version: mitsuro_core::storage::DELEGATION_EXECUTOR_ENVELOPE_VERSION,
            session_id: session_id.to_string(),
            parent_tool_call_id: Some("tool-1".to_string()),
            session_type: DelegationExecutorSessionType::Code,
            user_id: user_id.map(str::to_string),
            task_id: format!("{group_id}:task:0"),
            task_name: "recovery".to_string(),
            kind: DelegationExecutorKind::Explore,
            role: DelegatedRunRole::Explore,
            provider_id: ProviderId::OpenAI.to_string(),
            model_key: ModelKey::new(ProviderId::OpenAI, "test:model", ApiFormat::OpenAIResponses),
            resolved_model: "test:model".to_string(),
            working_dir: workspace
                .canonicalize()
                .expect("canonical workspace")
                .display()
                .to_string(),
            project_dir: Some(
                workspace
                    .canonicalize()
                    .expect("canonical workspace")
                    .display()
                    .to_string(),
            ),
            sandbox_root: workspace
                .canonicalize()
                .expect("canonical workspace")
                .display()
                .to_string(),
            objective_sha256: DelegationExecutorEnvelopeV1::objective_digest(objective),
        };
        let envelope_json = serde_json::to_string(&envelope).expect("serialize envelope");
        Database::new(&state.db_path)
            .expect("envelope database")
            .conn()
            .execute(
                "UPDATE delegation_tasks
                    SET executor_envelope_version = ?2, executor_envelope_json = ?3
                  WHERE delegation_task_id = ?1",
                (
                    envelope.task_id.as_str(),
                    i64::from(envelope.version),
                    envelope_json.as_str(),
                ),
            )
            .expect("attach replay envelope");
    }

    #[tokio::test]
    async fn replay_envelope_binds_session_owner_paths_and_task_identity() {
        let (state, _temp, workspace) = test_state();
        let (_event, session_id) = seed_completion(&state, &workspace);
        let group_id = "validated-replay";
        seed_recoverable_group(
            &state,
            &workspace,
            &session_id,
            group_id,
            DelegationExecutionMode::Detached,
        );
        attach_replay_envelope(&state, &workspace, &session_id, group_id, Some("alice"));
        let group = DelegationStore::new(Database::new(&state.db_path).expect("group database"))
            .get_group(group_id)
            .expect("group lookup")
            .expect("group");
        let session = SessionManager::new(Database::new(&state.db_path).expect("session database"))
            .get_session(&session_id)
            .expect("session lookup")
            .expect("session");
        validate_replayable_detached_group(&group, &session).expect("valid replay envelope");

        let mut shared_writer = group;
        let writer_task = shared_writer.tasks.first_mut().expect("writer task");
        writer_task.specification.role = DelegatedRunRole::Build;
        let writer_envelope = writer_task
            .specification
            .executor_envelope
            .as_mut()
            .expect("writer envelope");
        writer_envelope.role = DelegatedRunRole::Build;
        writer_envelope.kind = DelegationExecutorKind::Build;
        validate_replayable_detached_group(&shared_writer, &session)
            .expect("shared writers replay under durable wave and scheduler serialization");

        let mut isolated_writer = shared_writer;
        let isolated_task = isolated_writer.tasks.first_mut().expect("isolated task");
        isolated_task.specification.writer_mode = DelegationWriterMode::Isolated;
        isolated_task.specification.attempt_workspace = Some(
            workspace
                .canonicalize()
                .expect("canonical workspace")
                .display()
                .to_string(),
        );
        isolated_task.specification.workspace_baseline = Some("baseline".to_string());
        validate_replayable_detached_group(&isolated_writer, &session)
            .expect("isolated writers retain a recovery contract");

        attach_replay_envelope(&state, &workspace, &session_id, group_id, Some("mallory"));
        let stolen = DelegationStore::new(Database::new(&state.db_path).expect("group database"))
            .get_group(group_id)
            .expect("group lookup")
            .expect("group");
        assert!(validate_replayable_detached_group(&stolen, &session)
            .expect_err("owner mismatch must fail closed")
            .to_string()
            .contains("ownership"));
    }

    #[tokio::test]
    async fn active_completion_delivers_the_exact_durable_id() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let guard = state.lock_session(&session_id).await;
        let (input_tx, mut input_rx) = mpsc::unbounded_channel();
        state
            .session_inputs
            .write()
            .await
            .insert(session_id.clone(), input_tx);

        handle_child_completion(&state, event.clone())
            .await
            .expect("active completion should deliver");
        let delivered = input_rx.recv().await.expect("completion should arrive");
        let LoopInput::Steer {
            pending_id: Some(delivered_id),
            content: delivered_content,
        } = delivered
        else {
            panic!("completion should retain its exact durable steering identity");
        };
        assert_eq!(delivered_id, event.pending_id);
        assert_eq!(
            serde_json::to_string(&delivered_content).expect("serialize delivered completion"),
            serde_json::to_string(&event.content).expect("serialize expected completion")
        );

        SessionManager::new(Database::new(&state.db_path).expect("database should open"))
            .promote_pending_steering(&session_id, &event.pending_id)
            .expect("completion should promote");
        drop(guard);
    }

    #[tokio::test]
    async fn active_completion_is_enqueued_once_across_reconciliation_ticks() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let guard = state.lock_session(&session_id).await;
        let (input_tx, mut input_rx) = mpsc::unbounded_channel();
        state
            .session_inputs
            .write()
            .await
            .insert(session_id.clone(), input_tx);
        let deliveries = ActiveChildWakeDeliveries::default();

        handle_child_completion_with_tracker(&state, event.clone(), &deliveries)
            .await
            .expect("first reconciliation should deliver");
        handle_child_completion_with_tracker(&state, event.clone(), &deliveries)
            .await
            .expect("duplicate reconciliation should be idempotent");

        let delivered = input_rx.recv().await.expect("completion should arrive");
        assert!(matches!(
            delivered,
            LoopInput::Steer {
                pending_id: Some(ref pending_id),
                ..
            } if pending_id == &event.pending_id
        ));
        assert!(matches!(
            input_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        SessionManager::new(Database::new(&state.db_path).expect("database should open"))
            .promote_pending_steering(&session_id, &event.pending_id)
            .expect("completion should promote");
        drop(guard);
    }

    #[tokio::test]
    async fn startup_recovery_reconstructs_a_safe_pending_child_completion() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);

        let recovered =
            recover_pending_child_completions(&state).expect("startup recovery should scan");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].pending_id, event.pending_id);
        assert_eq!(
            recovered[0].session_id.as_deref(),
            Some(session_id.as_str())
        );
        assert_eq!(
            recovered[0].workspace_root.as_deref(),
            Some(
                workspace
                    .canonicalize()
                    .expect("canonical workspace")
                    .as_path()
            )
        );
        validate_child_completion(&state, recovered[0].clone())
            .expect("recovered event should pass the live validator");
    }

    #[tokio::test]
    async fn startup_group_recovery_fences_unreplayable_detached_work_and_queues_one_wake() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let db = Database::new(&state.db_path).expect("orphan database should open");
        db.conn()
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND role = ?2",
                [
                    session_id.as_str(),
                    format!("pending_user:{}", event.pending_id).as_str(),
                ],
            )
            .expect("remove pending completion fixture");
        db.conn()
            .execute(
                "DELETE FROM steering_idempotency WHERE session_id = ?1 AND pending_id = ?2",
                [session_id.as_str(), event.pending_id.as_str()],
            )
            .expect("remove completion receipt fixture");
        db.conn()
            .execute(
                "UPDATE delegated_runs
                    SET stage = 'running', artifact_json = NULL, human_review = NULL,
                        completed_at = NULL, host_lease_expires_at_ms = NULL
                  WHERE delegated_run_id = ?1",
                [&event.delegated_run_id],
            )
            .expect("simulate orphaned aggregate execution");
        drop(db);
        seed_recoverable_group(
            &state,
            &workspace,
            &session_id,
            &event.delegated_run_id,
            DelegationExecutionMode::Detached,
        );

        let report = reconcile_orphaned_delegation_groups_on_startup(&state)
            .expect("startup group recovery should run");
        assert_eq!(report.examined, 1);
        assert_eq!(report.fenced, 1);
        assert_eq!(report.failed, 0);
        let group = DelegationStore::new(
            Database::new(&state.db_path).expect("group database should open"),
        )
        .get_group(&event.delegated_run_id)
        .expect("group should load")
        .expect("group should exist");
        assert_eq!(group.state, DelegationGroupState::Failed);
        assert!(group.tasks.iter().all(|task| task.state.is_terminal()));

        let first = recover_pending_child_completions(&state)
            .expect("failed group should materialize its uncertainty wake");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].pending_id, event.pending_id);
        assert_eq!(first[0].terminal_stage, DelegatedRunStage::Cancelled);
        let second = recover_pending_child_completions(&state)
            .expect("retry scan should reuse the exact pending wake");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].pending_id, event.pending_id);
        let pending_count: i64 = Database::new(&state.db_path)
            .expect("pending database should open")
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = ?2",
                [
                    session_id.as_str(),
                    format!("pending_user:{}", event.pending_id).as_str(),
                ],
                |row| row.get(0),
            )
            .expect("pending count should load");
        assert_eq!(pending_count, 1, "recovery must not duplicate the wake row");
    }

    #[tokio::test]
    async fn startup_group_recovery_reconstructs_foreground_compatibility_but_never_wakes() {
        let (state, _temp, workspace) = test_state();
        let (_event, session_id) = seed_completion(&state, &workspace);
        let group_id = "foreground-orphan";
        seed_recoverable_group(
            &state,
            &workspace,
            &session_id,
            group_id,
            DelegationExecutionMode::Foreground,
        );

        let report = reconcile_orphaned_delegation_groups_on_startup(&state)
            .expect("foreground recovery should run");
        assert_eq!(report.fenced, 1);
        let run = DelegatedRunStore::new(
            Database::new(&state.db_path).expect("compatibility database should open"),
        )
        .get_run(group_id)
        .expect("compatibility row should load")
        .expect("compatibility row should be reconstructed");
        assert_eq!(run.stage, DelegatedRunStage::Cancelled);
        assert!(!run.wake_parent);
        assert!(recover_pending_child_completions(&state)
            .expect("wake scan should run")
            .iter()
            .all(|completion| completion.delegated_run_id != group_id));
    }

    #[tokio::test]
    async fn recurring_recovery_never_fences_live_foreground_work() {
        let (state, _temp, workspace) = test_state();
        let (_event, session_id) = seed_completion(&state, &workspace);
        let group_id = "live-foreground";
        seed_recoverable_group(
            &state,
            &workspace,
            &session_id,
            group_id,
            DelegationExecutionMode::Foreground,
        );

        let report = reconcile_replayable_detached_groups(&state)
            .expect("recurring recovery should scan only detached groups");
        assert_eq!(report.examined, 0);
        assert_eq!(report.fenced, 0);

        let group = DelegationStore::new(
            Database::new(&state.db_path).expect("group database should open"),
        )
        .get_group(group_id)
        .expect("group should load")
        .expect("group should exist");
        assert_eq!(group.state, DelegationGroupState::Queued);
        assert!(DelegatedRunStore::new(
            Database::new(&state.db_path).expect("compatibility database should open")
        )
        .get_run(group_id)
        .expect("compatibility lookup should succeed")
        .is_none());
    }

    #[tokio::test]
    async fn startup_group_recovery_does_not_steal_a_live_detached_host_lease() {
        let (state, _temp, workspace) = test_state();
        let (_event, session_id) = seed_completion(&state, &workspace);
        let group_id = "live-detached";
        DelegatedRunStore::new(
            Database::new(&state.db_path).expect("compatibility database should open"),
        )
        .create_background_run(&DelegatedRunStartInput {
            delegated_run_id: group_id.to_string(),
            parent_session_id: session_id.clone(),
            parent_tool_call_id: Some("tool-1".to_string()),
            role: DelegatedRunRole::Explore,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![DelegatedRunScope {
                label: "launch workspace".to_string(),
                path: workspace
                    .canonicalize()
                    .expect("canonical workspace")
                    .to_string_lossy()
                    .into_owned(),
                kind: "workspace".to_string(),
            }],
        })
        .expect("live background compatibility row should create");
        seed_recoverable_group(
            &state,
            &workspace,
            &session_id,
            group_id,
            DelegationExecutionMode::Detached,
        );
        attach_replay_envelope(&state, &workspace, &session_id, group_id, Some("alice"));

        let report = reconcile_orphaned_delegation_groups_on_startup(&state)
            .expect("live lease recovery scan should run");
        assert_eq!(report.live_detached, 1);
        assert_eq!(report.replay_scheduled, 0);
        assert_eq!(report.fenced, 0);
        let group = DelegationStore::new(
            Database::new(&state.db_path).expect("group database should open"),
        )
        .get_group(group_id)
        .expect("group should load")
        .expect("group should exist");
        assert_eq!(group.state, DelegationGroupState::Queued);
        let run = DelegatedRunStore::new(
            Database::new(&state.db_path).expect("compatibility database should open"),
        )
        .get_run(group_id)
        .expect("run should load")
        .expect("run should exist");
        assert_eq!(run.stage, DelegatedRunStage::Running);
    }

    #[tokio::test]
    async fn startup_group_recovery_leaves_hive_groups_to_hive_runtime() {
        let (state, _temp, workspace) = test_state();
        let (_event, session_id) = seed_completion(&state, &workspace);
        Database::new(&state.db_path)
            .expect("session database should open")
            .conn()
            .execute(
                "UPDATE sessions SET session_type = 'hive' WHERE id = ?1",
                [&session_id],
            )
            .expect("session should become Hive-owned");
        let group_id = "hive-orphan";
        seed_recoverable_group(
            &state,
            &workspace,
            &session_id,
            group_id,
            DelegationExecutionMode::Detached,
        );

        let report = reconcile_orphaned_delegation_groups_on_startup(&state)
            .expect("Hive-aware recovery should run");
        assert_eq!(report.hive_deferred, 1);
        assert_eq!(report.fenced, 0);
        let group = DelegationStore::new(
            Database::new(&state.db_path).expect("group database should open"),
        )
        .get_group(group_id)
        .expect("group should load")
        .expect("group should exist");
        assert_eq!(group.state, DelegationGroupState::Queued);
        assert!(DelegatedRunStore::new(
            Database::new(&state.db_path).expect("compatibility database should open")
        )
        .get_run(group_id)
        .expect("compatibility lookup should succeed")
        .is_none());
    }

    #[tokio::test]
    async fn startup_recovery_materializes_terminal_artifact_crash_window_once() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let manager = SessionManager::new(
            Database::new(&state.db_path).expect("recovery database should open"),
        );
        let pending_role = format!("pending_user:{}", event.pending_id);
        manager
            .db()
            .conn()
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND role = ?2",
                [&session_id, &pending_role],
            )
            .expect("remove preexisting pending fixture");
        manager
            .db()
            .conn()
            .execute(
                "DELETE FROM steering_idempotency WHERE session_id = ?1 AND pending_id = ?2",
                [&session_id, &event.pending_id],
            )
            .expect("remove preexisting receipt fixture");

        let recovered =
            recover_pending_child_completions(&state).expect("crash window should reconcile");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].delegated_run_id, event.delegated_run_id);
        assert!(manager
            .has_pending_steering(&session_id, &event.pending_id)
            .expect("pending completion should load"));

        manager
            .promote_pending_steering(&session_id, &event.pending_id)
            .expect("promote recovered completion");
        assert!(recover_pending_child_completions(&state)
            .expect("second recovery should scan")
            .is_empty());
    }

    #[tokio::test]
    async fn durable_reconciliation_retries_terminal_wake_after_materialization_failure() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let db = Database::new(&state.db_path).expect("reconciliation database should open");
        let pending_role = format!("pending_user:{}", event.pending_id);
        db.conn()
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND role = ?2",
                [&session_id, &pending_role],
            )
            .expect("remove pending fixture");
        db.conn()
            .execute(
                "DELETE FROM steering_idempotency WHERE session_id = ?1 AND pending_id = ?2",
                [&session_id, &event.pending_id],
            )
            .expect("remove receipt fixture");
        let original_scope: String = db
            .conn()
            .query_row(
                "SELECT target_scope_json FROM delegated_runs WHERE delegated_run_id = ?1",
                [&event.delegated_run_id],
                |row| row.get(0),
            )
            .expect("load original durable scope");
        db.conn()
            .execute(
                "UPDATE delegated_runs SET target_scope_json = '[]' WHERE delegated_run_id = ?1",
                [&event.delegated_run_id],
            )
            .expect("make first materialization fail");
        drop(db);

        assert!(recover_pending_child_completions(&state)
            .expect("failed materialization should not fail the whole scan")
            .is_empty());
        let manager = SessionManager::new(
            Database::new(&state.db_path).expect("receipt database should open"),
        );
        assert!(!manager
            .has_pending_steering(&session_id, &event.pending_id)
            .expect("failed materialization must not write a pending wake"));
        manager
            .db()
            .conn()
            .execute(
                "UPDATE delegated_runs SET target_scope_json = ?2 WHERE delegated_run_id = ?1",
                [&event.delegated_run_id, &original_scope],
            )
            .expect("restore durable scope after transient failure");

        let recovered = recover_pending_child_completions(&state)
            .expect("next periodic-style scan should retry the terminal row");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].delegated_run_id, event.delegated_run_id);
    }

    #[tokio::test]
    async fn startup_recovery_expires_a_dead_background_host_and_wakes_with_uncertainty() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let db = Database::new(&state.db_path).expect("host lease database should open");
        let pending_role = format!("pending_user:{}", event.pending_id);
        db.conn()
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND role = ?2",
                [&session_id, &pending_role],
            )
            .expect("remove preexisting pending fixture");
        db.conn()
            .execute(
                "DELETE FROM steering_idempotency WHERE session_id = ?1 AND pending_id = ?2",
                [&session_id, &event.pending_id],
            )
            .expect("remove preexisting receipt fixture");
        db.conn()
            .execute(
                "UPDATE delegated_runs
                    SET stage = 'running',
                        artifact_json = NULL,
                        human_review = NULL,
                        completed_at = NULL,
                        host_lease_expires_at_ms = 0
                  WHERE delegated_run_id = ?1",
                [&event.delegated_run_id],
            )
            .expect("simulate a previous server dying before terminal persistence");
        drop(db);

        let recovered = recover_pending_child_completions(&state)
            .expect("startup should recover the expired host lease");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].delegated_run_id, event.delegated_run_id);
        assert_eq!(recovered[0].terminal_stage, DelegatedRunStage::Cancelled);
        assert!(!recovered[0].success);
        assert_eq!(recovered[0].outcome, "cancelled");

        let durable = DelegatedRunStore::new(
            Database::new(&state.db_path).expect("recovered database should open"),
        )
        .get_run(&event.delegated_run_id)
        .expect("recovered run should load")
        .expect("recovered run should exist");
        assert_eq!(durable.stage, DelegatedRunStage::Cancelled);
        assert_eq!(
            durable.artifact.as_ref().unwrap()["outcome_reason"],
            "background_host_lease_expired"
        );
    }

    #[tokio::test]
    async fn abnormal_cancel_materializes_but_explicit_cancel_stays_quiet() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let db = Database::new(&state.db_path).expect("cancellation database should open");
        let pending_role = format!("pending_user:{}", event.pending_id);
        db.conn()
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND role = ?2",
                [&session_id, &pending_role],
            )
            .expect("remove pending fixture");
        db.conn()
            .execute(
                "DELETE FROM steering_idempotency WHERE session_id = ?1 AND pending_id = ?2",
                [&session_id, &event.pending_id],
            )
            .expect("remove receipt fixture");
        let abnormal_artifact = serde_json::json!({
            "outcome": "cancelled",
            "outcome_reason": "caller_aborted_before_terminal",
            "side_effects_may_have_occurred": true,
            "quiescent": false,
        })
        .to_string();
        db.conn()
            .execute(
                "UPDATE delegated_runs
                    SET stage = 'cancelled',
                        artifact_json = ?2,
                        human_review = 'caller disappeared',
                        completed_at = updated_at
                  WHERE delegated_run_id = ?1",
                [&event.delegated_run_id, &abnormal_artifact],
            )
            .expect("seed abnormal cancellation");
        drop(db);

        let materialized = materialize_durable_child_completion(&state, &event.delegated_run_id)
            .expect("abnormal cancellation should reconcile");
        let DurableWakeMaterialization::Ready(abnormal) = materialized else {
            panic!("abnormal cancellation must become a durable parent wake");
        };
        assert_eq!(abnormal.terminal_stage, DelegatedRunStage::Cancelled);
        assert_eq!(abnormal.outcome, "cancelled");
        assert!(!abnormal.success);

        let db = Database::new(&state.db_path).expect("explicit cancellation database");
        db.conn()
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND role = ?2",
                [&session_id, &pending_role],
            )
            .expect("remove abnormal pending fixture");
        db.conn()
            .execute(
                "DELETE FROM steering_idempotency WHERE session_id = ?1 AND pending_id = ?2",
                [&session_id, &event.pending_id],
            )
            .expect("remove abnormal receipt fixture");
        let explicit_artifact = serde_json::json!({
            "outcome": "cancelled",
            "outcome_reason": "cancelled",
        })
        .to_string();
        db.conn()
            .execute(
                "UPDATE delegated_runs SET artifact_json = ?2, human_review = 'cancelled by user'
                  WHERE delegated_run_id = ?1",
                [&event.delegated_run_id, &explicit_artifact],
            )
            .expect("seed explicit cancellation");
        drop(db);

        assert!(matches!(
            materialize_durable_child_completion(&state, &event.delegated_run_id)
                .expect("explicit cancellation should classify"),
            DurableWakeMaterialization::Suppressed
        ));
        assert!(!SessionManager::new(
            Database::new(&state.db_path).expect("pending verification database")
        )
        .has_pending_steering(&session_id, &event.pending_id)
        .expect("pending state should load"));
    }

    #[tokio::test]
    async fn detached_completion_retains_the_launch_reasoning_effort() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let group_store = DelegationStore::new(
            Database::new(&state.db_path).expect("delegation database should open"),
        );
        group_store
            .create_group(&DelegationGroupStartInput {
                delegation_group_id: event.delegated_run_id.clone(),
                parent_session_id: session_id,
                parent_tool_call_id: Some("tool-1".to_string()),
                contract: DelegationGroupContract {
                    execution_mode: DelegationExecutionMode::Detached,
                    completion_policy: DelegationCompletionPolicy::AllSettled,
                    failure_policy: DelegationFailurePolicy::Continue,
                    governance: DelegationGovernance {
                        permission_mode: PermissionMode::Autonomous,
                        reasoning_effort: Some(ReasoningEffort::XHigh),
                        delegated_turn_budget: None,
                        max_parallelism: 1,
                        execution_tool_allowlist: Some(BTreeSet::from(["read".to_string()])),
                        delegation_policy: DelegationPolicy::for_subagent_explore(
                            PermissionMode::Autonomous,
                            None,
                        ),
                    },
                },
                tasks: vec![DelegationTaskSpec {
                    delegation_task_id: format!("{}:task:0", event.delegated_run_id),
                    task_key: "child".to_string(),
                    objective: "Complete detached work".to_string(),
                    role: DelegatedRunRole::Explore,
                    target_scope: vec![DelegatedRunScope {
                        label: "launch workspace".to_string(),
                        path: workspace
                            .canonicalize()
                            .expect("canonical workspace")
                            .to_string_lossy()
                            .into_owned(),
                        kind: "workspace".to_string(),
                    }],
                    max_attempts: 1,
                    depends_on: Vec::new(),
                    write_intent: Vec::new(),
                    task_policy: None,
                    writer_mode: DelegationWriterMode::Shared,
                    attempt_workspace: None,
                    workspace_baseline: None,
                    executor_envelope: None,
                }],
            })
            .expect("detached group should create");
        group_store
            .fail_group_recovery(&event.delegated_run_id, "test terminal handoff")
            .expect("group should become terminal");

        let completion =
            validate_child_completion(&state, event).expect("completion authority should validate");
        assert_eq!(completion.reasoning_effort, Some(ReasoningEffort::XHigh));
    }

    #[tokio::test]
    async fn idle_completion_resumes_once_and_duplicate_event_is_a_noop() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let completion = validate_child_completion(&state, event.clone())
            .expect("completion authority should validate");
        let (resume_tx, mut resume_rx) = mpsc::unbounded_channel();

        assert!(ensure_completion_resumed_with(
            &state,
            completion.clone(),
            move |_state, resumed_session, owner, root, _guard| {
                let resume_tx = resume_tx.clone();
                async move {
                    resume_tx
                        .send((resumed_session, owner, root))
                        .expect("resume should be observed");
                    Ok(())
                }
            },
        )
        .await
        .expect("idle completion should dispatch resume"));
        let (resumed_session, owner, root) = resume_rx.recv().await.expect("resume marker");
        assert_eq!(resumed_session, session_id);
        assert_eq!(owner.as_deref(), Some("alice"));
        assert_eq!(root, workspace.canonicalize().expect("canonical workspace"));

        SessionManager::new(Database::new(&state.db_path).expect("database should open"))
            .promote_pending_steering(&session_id, &event.pending_id)
            .expect("completion should promote");
        assert!(!ensure_completion_resumed_with(
            &state,
            completion,
            |_state, _session, _owner, _root, _guard| async move {
                panic!("duplicate completion must not start another parent run")
            },
        )
        .await
        .expect("duplicate completion should be harmless"));
    }

    #[tokio::test]
    async fn completion_does_not_race_a_registered_foreground_input_channel() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let completion = validate_child_completion(&state, event.clone())
            .expect("completion authority should validate");
        let (input_tx, _input_rx) = mpsc::unbounded_channel();
        state
            .session_inputs
            .write()
            .await
            .insert(session_id.clone(), input_tx);

        assert!(!ensure_completion_resumed_with(
            &state,
            completion,
            |_state, _session, _owner, _root, _guard| async move {
                panic!("registered foreground input must retain continuation authority")
            },
        )
        .await
        .expect("active foreground handoff should defer recovery"));
        assert!(
            SessionManager::new(Database::new(&state.db_path).expect("database should open"))
                .has_pending_steering(&session_id, &event.pending_id)
                .expect("pending state should load")
        );
    }

    #[tokio::test]
    async fn completion_does_not_race_an_unfinished_durable_parent_run() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let completion = validate_child_completion(&state, event.clone())
            .expect("completion authority should validate");
        Database::new(&state.db_path)
            .expect("trace database should open")
            .conn()
            .execute(
                "INSERT INTO runtime_traces (
                     session_id, run_id, sequence, turn, event_type, payload_json, created_at
                 ) VALUES (?1, 'active-run', 1, 1, 'run_budget_resolved', '{}', ?2)",
                [session_id.as_str(), Utc::now().to_rfc3339().as_str()],
            )
            .expect("unfinished run boundary should insert");

        assert!(!ensure_completion_resumed_with(
            &state,
            completion,
            |_state, _session, _owner, _root, _guard| async move {
                panic!("unfinished durable parent run must retain continuation authority")
            },
        )
        .await
        .expect("unfinished foreground run should defer recovery"));
        assert!(
            SessionManager::new(Database::new(&state.db_path).expect("database should open"))
                .has_pending_steering(&session_id, &event.pending_id)
                .expect("pending state should load")
        );
    }

    #[tokio::test]
    async fn idle_completion_retries_transient_resume_failures_with_a_fresh_lock() {
        let (state, _temp, workspace) = test_state();
        let (event, _session_id) = seed_completion(&state, &workspace);
        let completion =
            validate_child_completion(&state, event).expect("completion authority should validate");
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = Arc::clone(&attempts);

        assert!(ensure_completion_resumed_with_policy(
            &state,
            completion,
            3,
            Duration::ZERO,
            move |_state, _session, _owner, _root, _guard| {
                let attempt = observed_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if attempt < 3 {
                        Err(crate::error::AppError::ServiceUnavailable(
                            "temporary startup failure".to_string(),
                        ))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await
        .expect("third attempt should start"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_does_not_start_twice_after_pending_completion_was_claimed() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let pending_id = event.pending_id.clone();
        let completion =
            validate_child_completion(&state, event).expect("completion authority should validate");
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = Arc::clone(&attempts);

        assert!(!ensure_completion_resumed_with_policy(
            &state,
            completion,
            3,
            Duration::ZERO,
            move |state, resumed_session, _owner, _root, _guard| {
                let attempt = observed_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                let pending_id = pending_id.clone();
                async move {
                    assert_eq!(attempt, 1, "resume closure must not run twice");
                    SessionManager::new(
                        Database::new(&state.db_path).expect("retry database should open"),
                    )
                    .promote_pending_steering(&resumed_session, &pending_id)
                    .expect("partial starter should claim pending completion");
                    Err(crate::error::AppError::ServiceUnavailable(
                        "starter response was lost".to_string(),
                    ))
                }
            },
        )
        .await
        .expect("claimed completion should make retry a no-op"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(
            !SessionManager::new(Database::new(&state.db_path).expect("database should open"))
                .has_pending_steering(&session_id, "child-wake-child-run-1")
                .expect("pending state should load")
        );
    }

    #[tokio::test]
    async fn idle_completion_stops_after_the_bounded_attempt_count() {
        let (state, _temp, workspace) = test_state();
        let (event, _session_id) = seed_completion(&state, &workspace);
        let completion =
            validate_child_completion(&state, event).expect("completion authority should validate");
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed_attempts = Arc::clone(&attempts);

        let error = ensure_completion_resumed_with_policy(
            &state,
            completion,
            3,
            Duration::ZERO,
            move |_state, _session, _owner, _root, _guard| {
                observed_attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    Err(crate::error::AppError::ServiceUnavailable(
                        "still unavailable".to_string(),
                    ))
                }
            },
        )
        .await
        .expect_err("resume must stop after its bounded attempts");
        assert!(error.to_string().contains("attempt 3/3"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn completion_authority_rejects_foreign_session_owner() {
        let (state, _temp, workspace) = test_state();
        let (mut event, _session_id) = seed_completion(&state, &workspace);
        event.user_id = Some("bob".into());

        let error = validate_child_completion(&state, event)
            .expect_err("foreign completion owner must be rejected");
        assert!(error.to_string().contains("owner does not match"));
    }

    #[tokio::test]
    async fn completion_authority_rejects_stale_outcome_metadata() {
        let (state, _temp, workspace) = test_state();
        let (mut event, _session_id) = seed_completion(&state, &workspace);
        event.success = false;

        let error = validate_child_completion(&state, event)
            .expect_err("stale completion outcome must be rejected");
        assert!(error
            .to_string()
            .contains("outcome does not match its durable terminal stage"));
    }

    #[tokio::test]
    async fn completion_authority_rejects_workspace_not_in_durable_lineage() {
        let (state, _temp, workspace) = test_state();
        let (mut event, _session_id) = seed_completion(&state, &workspace);
        let foreign_workspace = workspace
            .parent()
            .expect("workspace parent")
            .join("foreign-workspace");
        std::fs::create_dir_all(&foreign_workspace).expect("foreign workspace");
        event.workspace_root = Some(foreign_workspace);

        let error = validate_child_completion(&state, event)
            .expect_err("foreign workspace authority must be rejected");
        assert!(error.to_string().contains("escapes its captured authority"));
    }

    #[tokio::test]
    async fn changed_session_project_cannot_canonicalize_a_pending_child_wake() {
        let (state, _temp, workspace) = test_state();
        let (event, session_id) = seed_completion(&state, &workspace);
        let changed_project = workspace
            .parent()
            .expect("workspace parent")
            .join("changed-project");
        std::fs::create_dir_all(&changed_project).expect("changed project");
        Database::new(&state.db_path)
            .expect("database should open")
            .conn()
            .execute(
                "UPDATE sessions SET project_dir = ?1 WHERE id = ?2",
                [
                    changed_project.to_string_lossy().as_ref(),
                    session_id.as_str(),
                ],
            )
            .expect("session project should change");

        let error = validate_child_completion(&state, event.clone())
            .expect_err("changed session project must block automatic continuation");
        assert!(error
            .to_string()
            .contains("project no longer matches its durable launch workspace"));
        let manager = SessionManager::new(
            Database::new(&state.db_path).expect("promotion database should open"),
        );
        let promotion_error = manager
            .promote_pending_steering(&session_id, &event.pending_id)
            .expect_err("model-boundary promotion must recheck workspace authority");
        assert!(promotion_error
            .to_string()
            .contains("project no longer matches its durable launch workspace"));
        assert_eq!(
            manager
                .promote_orphaned_pending_steering(&session_id)
                .expect("ordinary chat recovery should ignore child wakes"),
            0,
            "ordinary chat recovery must not bypass child-wake authority"
        );
        assert!(manager
            .has_pending_steering(&session_id, &event.pending_id)
            .expect("pending completion should remain for user review"));
        assert!(
            manager
                .load_session_messages(&session_id)
                .expect("canonical history should load")
                .iter()
                .all(|(_, content)| !content.contains("[CHILD AGENT COMPLETE]")),
            "the rejected child wake must never enter canonical user history"
        );
    }

    #[tokio::test]
    async fn completion_authority_rejects_cancelled_terminal_winner() {
        let (state, _temp, workspace) = test_state();
        let (mut event, _session_id) = seed_completion(&state, &workspace);
        Database::new(&state.db_path)
            .expect("database should open")
            .conn()
            .execute(
                "UPDATE delegated_runs SET stage = 'cancelled' WHERE delegated_run_id = ?1",
                [&event.delegated_run_id],
            )
            .expect("test should install cancelled terminal winner");
        event.success = false;

        let error = validate_child_completion(&state, event)
            .expect_err("cancelled completion must never wake the parent");
        assert!(error.to_string().contains("not publishable"));
    }
}
