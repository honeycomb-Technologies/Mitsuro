use std::io::{Error as IoError, ErrorKind};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, OptionalExtension, Row, Transaction, TransactionBehavior};

use crate::ai::types::Content;
use crate::hive::{canonical_timestamp, normalize_timestamp, HiveRunStatus};
use crate::storage::hive_worker_conversations::{
    committed_worker_response_in_transaction, finalize_stopped_worker_conversation_in_transaction,
    materialize_oldest_staged_input_in_transaction,
    materialize_oldest_staged_input_with_authority_in_transaction,
    reconcile_committed_introduction_provider_calls_in_transaction,
    reconcile_expired_worker_response_in_transaction, ExpiredWorkerResponseDisposition,
    StoppedWorkerConversationFinalization, WorkerConversationPredecessorAuthority,
};
use crate::storage::{
    committed_worker_goal_outcome_in_transaction, hash_request_bytes,
    pause_worker_workflow_after_uncertain_run_in_transaction,
    reconcile_worker_workflow_provider_boundary_in_transaction,
    record_trusted_worker_idle_outcome_in_transaction,
    worker_goal_outcome_is_accounted_in_transaction, Database, WorkerConversationLane,
    WorkerGovernorGateReason, WorkerRunGovernorProjection, WorkerRunOrigin,
    WorkerWorkflowProviderRecovery,
};

use super::{
    ClaimRunRequest, ClaimedHiveRun, DaemonFence, HiveRun, HiveRunAttempt, HiveRunAttemptOutcome,
    HiveRunExecutionContextV1, HiveRunExecutionModeV1, HiveRunKind, LeaseReconciliation,
    ReconciledRun, RunCompletion, WORKER_CONVERSATION_STOP_REQUESTED_REASON,
};

const RUN_COLUMNS: &str = "id, controller_id, session_id, schedule_id, occurrence_id, kind, objective, config_json, status, priority, concurrency_key, scheduled_for, available_at, wake_at, attempt_count, max_attempts, lease_owner, lease_token, lease_epoch, lease_expires_at, heartbeat_at, last_stop_reason, last_error, outcome_json, created_at, started_at, finished_at, updated_at, worker_id, objective_message_id, governor_origin, governor_lane_key, governor_gate_reason, governor_next_eligible_at, governor_policy_revision, governor_override_id, execution_context_json, conversation_through_message_id, response_message_id, response_group_message_id, response_provider_call_id, workflow_goal_id, workflow_attempt_id";
const ATTEMPT_COLUMNS: &str = "id, run_id, attempt_no, executor_id, lease_token, lease_epoch, started_at, finished_at, outcome, stop_reason, error, retry_at, trace_sequence_start, trace_sequence_end";

pub struct HiveRunStore {
    db: Database,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinishClaimAuthority {
    Ordinary = 0,
    DisabledControllerCancellation = 1,
    StoppedWorkerConversation = 2,
    CancelledGroupTurn = 3,
}

impl HiveRunStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    pub fn insert_run(&self, run: &HiveRun) -> Result<()> {
        anyhow::ensure!(
            run.status == HiveRunStatus::Queued,
            "new Hive runs must enter the queue"
        );
        anyhow::ensure!(!run.id.trim().is_empty(), "run id is empty");
        anyhow::ensure!(
            !run.controller_id.trim().is_empty(),
            "run controller id is empty"
        );
        anyhow::ensure!(!run.objective.trim().is_empty(), "run objective is empty");
        anyhow::ensure!(run.max_attempts > 0, "max_attempts is zero");
        anyhow::ensure!(run.attempt_count == 0, "new run has prior attempts");
        anyhow::ensure!(run.wake_at.is_none(), "new queued run cannot be sleeping");
        anyhow::ensure!(
            run.lease_owner.is_none()
                && run.lease_token.is_none()
                && run.lease_epoch.is_none()
                && run.lease_expires_at.is_none()
                && run.heartbeat_at.is_none(),
            "new queued run cannot hold a worker lease"
        );
        anyhow::ensure!(
            run.started_at.is_none() && run.finished_at.is_none(),
            "new queued run cannot already be started or finished"
        );
        anyhow::ensure!(
            run.last_stop_reason.is_none() && run.last_error.is_none() && run.outcome.is_none(),
            "new queued run cannot have a prior outcome"
        );
        anyhow::ensure!(
            run.response_message_id.is_none()
                && run.response_group_message_id.is_none()
                && run.response_provider_call_id.is_none(),
            "new queued run cannot already have a response"
        );
        anyhow::ensure!(
            run.concurrency_key
                .as_deref()
                .is_none_or(|key| !key.trim().is_empty()),
            "run concurrency key is empty"
        );
        validate_new_run_authority(run)?;
        let config_json = serde_json::to_string(&run.config)?;
        let execution_context_json = run
            .execution_context
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let outcome_json = run
            .outcome
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let scheduled_for = normalize_optional_timestamp(run.scheduled_for.as_deref())?;
        let available_at = normalize_timestamp(&run.available_at)?;
        let wake_at = normalize_optional_timestamp(run.wake_at.as_deref())?;
        let created_at = normalize_timestamp(&run.created_at)?;
        let updated_at = normalize_timestamp(&run.updated_at)?;
        self.db.conn().execute(
            "INSERT INTO hive_runs (
                id, controller_id, session_id, schedule_id, occurrence_id, kind,
                objective, config_json, status, priority, concurrency_key,
                scheduled_for, available_at, wake_at, attempt_count, max_attempts,
                lease_owner, lease_token, lease_epoch, lease_expires_at, heartbeat_at,
                last_stop_reason, last_error, outcome_json, created_at, started_at,
                finished_at, updated_at, worker_id, objective_message_id,
                governor_origin, governor_lane_key, governor_gate_reason,
                governor_next_eligible_at, governor_policy_revision,
                governor_override_id, execution_context_json,
                conversation_through_message_id, response_message_id,
                response_group_message_id, response_provider_call_id,
                workflow_goal_id, workflow_attempt_id
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24,
                ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35,
                ?36, ?37, ?38, ?39, ?40, ?41, ?42, ?43
             )",
            params![
                run.id,
                run.controller_id,
                run.session_id,
                run.schedule_id,
                run.occurrence_id,
                run.kind.to_string(),
                run.objective,
                config_json,
                run.status.to_string(),
                run.priority,
                run.concurrency_key,
                scheduled_for,
                available_at,
                wake_at,
                run.attempt_count,
                run.max_attempts,
                run.lease_owner,
                run.lease_token,
                run.lease_epoch,
                run.lease_expires_at,
                run.heartbeat_at,
                run.last_stop_reason,
                run.last_error,
                outcome_json,
                created_at,
                run.started_at,
                run.finished_at,
                updated_at,
                run.worker_id,
                run.objective_message_id,
                run.governor
                    .as_ref()
                    .and_then(|projection| projection.origin)
                    .map(WorkerRunOrigin::as_str),
                run.governor
                    .as_ref()
                    .and_then(|projection| projection.lane_key.as_deref()),
                run.governor
                    .as_ref()
                    .and_then(|projection| projection.gate_reason)
                    .map(WorkerGovernorGateReason::as_str),
                run.governor
                    .as_ref()
                    .and_then(|projection| projection.next_eligible_at.as_deref()),
                run.governor
                    .as_ref()
                    .and_then(|projection| projection.policy_revision),
                run.governor
                    .as_ref()
                    .and_then(|projection| projection.override_grant_id.as_deref()),
                execution_context_json,
                run.conversation_through_message_id,
                run.response_message_id,
                run.response_group_message_id,
                run.response_provider_call_id,
                run.workflow_goal_id,
                run.workflow_attempt_id,
            ],
        )?;
        Ok(())
    }

    pub fn get_run(&self, id: &str) -> Result<Option<HiveRun>> {
        let sql = format!("SELECT {RUN_COLUMNS} FROM hive_runs WHERE id = ?1");
        self.db
            .conn()
            .query_row(&sql, [id], map_run)
            .optional()
            .context("reading Hive run")
    }

    /// Revalidate every identity boundary immediately before a claimed run is
    /// handed to an execution host (and again before live controls are
    /// delivered). The caller's `ClaimedHiveRun` is an immutable snapshot;
    /// comparing the persisted objective/configuration prevents a later
    /// session or schedule edit from silently changing work that was already
    /// claimed.
    pub fn validate_claimed_execution_fenced(
        &self,
        claim: &ClaimedHiveRun,
        daemon_fence: &DaemonFence,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let Some(session_id) = claim.run.session_id.as_deref() else {
            return Ok(false);
        };
        if claim.run.lease_owner.as_deref() != Some(daemon_fence.owner_id.as_str())
            || claim.run.lease_token.as_deref() != Some(claim.lease_token.as_str())
            || claim.run.lease_epoch != Some(daemon_fence.fencing_token)
        {
            return Ok(false);
        }

        let now = canonical_timestamp(now);
        let row = self
            .db
            .conn()
            .query_row(
                "SELECT r.objective, r.config_json, r.kind,
                        m.session_id, m.role, m.content,
                        r.worker_id, worker.model, worker.model_key_json,
                        worker.model_catalog_revision, worker.permission_mode,
                        r.objective_message_id, r.execution_context_json,
                        r.conversation_through_message_id,
                        r.response_message_id, r.response_group_message_id,
                        r.governor_origin, r.governor_lane_key, worker.revision,
                        s.workspace_mode, s.working_dir, s.project_dir,
                        r.response_provider_call_id, r.workflow_goal_id,
                        r.workflow_attempt_id
                 FROM hive_runs r
                 JOIN hive_controllers c ON c.id = r.controller_id
                 JOIN sessions s ON s.id = r.session_id
                 JOIN hive_run_attempts a ON a.id = ?7
                 JOIN hive_daemon_leases d ON d.lease_name = ?8
                 LEFT JOIN messages m ON m.id = r.objective_message_id
                 LEFT JOIN hive_workers worker ON worker.id = r.worker_id
                 WHERE r.id = ?1 AND r.controller_id = ?2 AND r.session_id = ?3
                   AND r.status = 'running'
                   AND r.lease_owner = ?4 AND r.lease_token = ?5
                   AND r.lease_epoch = ?6 AND r.lease_expires_at > ?9
                   AND c.session_id = r.session_id AND c.status = 'active'
                   AND c.user_id IS s.user_id AND s.session_type = 'hive'
                   AND (
                       r.worker_id IS NULL OR (
                           worker.status = 'active'
                           AND c.worker_id = worker.id
                           AND c.user_id IS worker.user_id
                           AND (
                               worker.dm_session_id = r.session_id
                               OR EXISTS (
                                   SELECT 1 FROM hive_group_worker_lanes lane
                                   WHERE lane.worker_id = worker.id
                                     AND lane.session_id = r.session_id
                               )
                           )
                       )
                   )
                   AND a.run_id = r.id AND a.attempt_no = ?10
                   AND a.executor_id = ?4 AND a.lease_token = ?5
                   AND a.lease_epoch = ?6 AND a.finished_at IS NULL
                   AND d.owner_id = ?4 AND d.fencing_token = ?6
                   AND d.expires_at > ?9",
                params![
                    claim.run.id,
                    claim.run.controller_id,
                    session_id,
                    daemon_fence.owner_id,
                    claim.lease_token,
                    daemon_fence.fencing_token,
                    claim.attempt_id,
                    daemon_fence.lease_name,
                    now,
                    claim.attempt_no,
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                        row.get::<_, Option<String>>(10)?,
                        row.get::<_, Option<i64>>(11)?,
                        row.get::<_, Option<String>>(12)?,
                        row.get::<_, Option<i64>>(13)?,
                        row.get::<_, Option<i64>>(14)?,
                        row.get::<_, Option<String>>(15)?,
                        row.get::<_, Option<String>>(16)?,
                        row.get::<_, Option<String>>(17)?,
                        row.get::<_, Option<i64>>(18)?,
                        row.get::<_, String>(19)?,
                        row.get::<_, Option<String>>(20)?,
                        row.get::<_, Option<String>>(21)?,
                        row.get::<_, Option<String>>(22)?,
                        row.get::<_, Option<String>>(23)?,
                        row.get::<_, Option<String>>(24)?,
                    ))
                },
            )
            .optional()
            .context("validating claimed Hive execution fence")?;
        let Some((
            objective,
            config_json,
            kind,
            message_session,
            message_role,
            message_content,
            worker_id,
            worker_model,
            worker_model_key_json,
            worker_catalog_revision,
            worker_permission_mode,
            objective_message_id,
            execution_context_json,
            conversation_through_message_id,
            response_message_id,
            response_group_message_id,
            governor_origin,
            governor_lane_key,
            worker_revision,
            workspace_mode,
            working_dir,
            project_dir,
            response_provider_call_id,
            workflow_goal_id,
            workflow_attempt_id,
        )) = row
        else {
            return Ok(false);
        };
        if objective != claim.run.objective
            || kind != claim.run.kind.as_str()
            || worker_id.as_deref() != claim.run.worker_id.as_deref()
            || objective_message_id != claim.run.objective_message_id
            || conversation_through_message_id != claim.run.conversation_through_message_id
            || response_message_id != claim.run.response_message_id
            || response_group_message_id.as_deref()
                != claim.run.response_group_message_id.as_deref()
            || response_provider_call_id.as_deref()
                != claim.run.response_provider_call_id.as_deref()
            || workflow_goal_id.as_deref() != claim.run.workflow_goal_id.as_deref()
            || workflow_attempt_id.as_deref() != claim.run.workflow_attempt_id.as_deref()
        {
            return Ok(false);
        }
        let persisted_config: serde_json::Value = serde_json::from_str(&config_json)
            .context("decoding claimed Hive run config during fence validation")?;
        if persisted_config != claim.run.config {
            return Ok(false);
        }
        let configured_worker_id = configured_worker_id(&persisted_config);
        if configured_worker_id.is_some() && worker_id.as_deref() != configured_worker_id {
            return Ok(false);
        }
        if worker_id.is_some() {
            let persisted_context = execution_context_json
                .as_deref()
                .map(serde_json::from_str::<HiveRunExecutionContextV1>)
                .transpose()
                .context("decoding claimed Worker execution context")?;
            if persisted_context.as_ref() != claim.run.execution_context.as_ref() {
                return Ok(false);
            }
            let Some(context) = persisted_context.as_ref() else {
                return Ok(false);
            };
            let expected_lane_key = context.lane().canonical_lane_key()?;
            if Some(context.worker_id()) != worker_id.as_deref()
                || worker_revision.and_then(|value| u64::try_from(value).ok())
                    != Some(context.worker_revision())
                || governor_lane_key.as_deref() != Some(expected_lane_key.as_str())
                || governor_origin.as_deref()
                    != claim
                        .run
                        .governor
                        .as_ref()
                        .and_then(|projection| projection.origin)
                        .map(WorkerRunOrigin::as_str)
                || governor_lane_key.as_deref()
                    != claim
                        .run
                        .governor
                        .as_ref()
                        .and_then(|projection| projection.lane_key.as_deref())
            {
                return Ok(false);
            }
            let workspace_is_current = match &context.mode {
                HiveRunExecutionModeV1::WorkerConversationNeutral { .. } => {
                    workspace_mode == "neutral"
                        && working_dir.as_deref().is_none_or(str::is_empty)
                        && project_dir.as_deref().is_none_or(str::is_empty)
                }
                HiveRunExecutionModeV1::WorkerWorkspaceAttached {
                    workspace_mode: frozen_mode,
                    working_dir: frozen_working_dir,
                    project_dir: frozen_project_dir,
                    ..
                } => {
                    workspace_mode == frozen_mode.to_string()
                        && working_dir.as_deref() == Some(frozen_working_dir.as_str())
                        && project_dir.as_deref() == frozen_project_dir.as_deref()
                }
                HiveRunExecutionModeV1::WorkerGoal {
                    workspace_mode: frozen_mode,
                    working_dir: frozen_working_dir,
                    project_dir: frozen_project_dir,
                    ..
                }
                | HiveRunExecutionModeV1::WorkerGoalAcceptance {
                    workspace_mode: frozen_mode,
                    working_dir: frozen_working_dir,
                    project_dir: frozen_project_dir,
                    ..
                } => {
                    workspace_mode == frozen_mode.to_string()
                        && working_dir.as_deref() == Some(frozen_working_dir.as_str())
                        && project_dir.as_deref() == Some(frozen_project_dir.as_str())
                }
            };
            if !workspace_is_current {
                return Ok(false);
            }
            if kind == HiveRunKind::WorkerWorkflow.as_str() {
                let HiveRunExecutionModeV1::WorkerGoal {
                    goal_id,
                    attempt_id,
                    ..
                } = &context.mode
                else {
                    return Ok(false);
                };
                if workflow_goal_id.as_deref() != Some(goal_id.as_str())
                    || workflow_attempt_id.as_deref() != Some(attempt_id.as_str())
                {
                    return Ok(false);
                }
            }
            let worker_model_key = worker_model_key_json
                .as_deref()
                .map(serde_json::from_str::<serde_json::Value>)
                .transpose()
                .context("decoding Worker model key during execution fence validation")?;
            let configured_model_key = persisted_config
                .get("model_key")
                .filter(|value| !value.is_null());
            if worker_model.as_deref()
                != persisted_config
                    .get("model")
                    .and_then(serde_json::Value::as_str)
                || worker_model_key.as_ref() != configured_model_key
                || worker_catalog_revision.as_deref()
                    != persisted_config
                        .get("model_catalog_revision")
                        .and_then(serde_json::Value::as_str)
                || worker_permission_mode.as_deref()
                    != persisted_config
                        .get("permission_mode")
                        .and_then(serde_json::Value::as_str)
            {
                return Ok(false);
            }
        }

        if worker_id.is_none() && matches!(kind.as_str(), "scheduled" | "controller_child") {
            let expected = format!("Hive {kind} objective:\n{}", objective.trim());
            let materialized = message_content
                .as_deref()
                .map(serde_json::from_str::<Vec<Content>>)
                .transpose()
                .context("decoding materialized Hive objective message")?;
            let objective_matches = matches!(
                materialized.as_deref(),
                Some([Content::Text { text, .. }]) if text == &expected
            );
            if message_session.as_deref() != Some(session_id)
                || message_role.as_deref() != Some("user")
                || !objective_matches
            {
                return Ok(false);
            }
        }

        if kind == HiveRunKind::WorkerConversation.as_str() {
            let materialized = message_content
                .as_deref()
                .map(serde_json::from_str::<Vec<Content>>)
                .transpose()
                .context("decoding Worker conversation objective message")?;
            let objective_matches = matches!(
                materialized.as_deref(),
                Some([Content::Text { text, .. }]) if text == &objective
            );
            if message_session.as_deref() != Some(session_id)
                || message_role.as_deref() != Some("user")
                || !objective_matches
                || objective_message_id.is_none()
                || objective_message_id != conversation_through_message_id
                || response_message_id.is_some()
                || response_group_message_id.is_some()
                || response_provider_call_id.is_some()
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub fn list_for_controller(&self, controller_id: &str, limit: usize) -> Result<Vec<HiveRun>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT {RUN_COLUMNS} FROM hive_runs
             WHERE controller_id = ?1
             ORDER BY created_at DESC, id ASC
             LIMIT ?2"
        );
        let mut statement = self.db.conn().prepare(&sql)?;
        let rows = statement
            .query_map(params![controller_id, limit as i64], map_run)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("listing Hive runs for controller")?;
        Ok(rows)
    }

    /// Atomically claims the next runnable item and opens its durable attempt row.
    pub fn claim_next(&self, request: &ClaimRunRequest) -> Result<Option<ClaimedHiveRun>> {
        self.claim_next_inner(request, None)
    }

    /// Claim only while the caller still owns the current scheduler generation.
    pub fn claim_next_fenced(
        &self,
        request: &ClaimRunRequest,
        daemon_fence: &DaemonFence,
    ) -> Result<Option<ClaimedHiveRun>> {
        anyhow::ensure!(
            request.lease_epoch == daemon_fence.fencing_token,
            "run lease epoch does not match daemon fence"
        );
        self.claim_next_inner(request, Some(daemon_fence))
    }

    fn claim_next_inner(
        &self,
        request: &ClaimRunRequest,
        daemon_fence: Option<&DaemonFence>,
    ) -> Result<Option<ClaimedHiveRun>> {
        anyhow::ensure!(
            !request.executor_id.trim().is_empty(),
            "executor id is empty"
        );
        anyhow::ensure!(
            request.lease_epoch <= i64::MAX as u64,
            "lease epoch exceeds SQLite integer range"
        );
        if request.global_concurrency_limit == 0 || request.lease_duration.is_zero() {
            return Ok(None);
        }
        let now = canonical_timestamp(request.now);
        let lease_delta = Duration::from_std(request.lease_duration)
            .context("Hive worker lease duration exceeds chrono range")?;
        let lease_expires_at = canonical_timestamp(
            request
                .now
                .checked_add_signed(lease_delta)
                .context("Hive worker lease expiry overflow")?,
        );
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        if let Some(daemon_fence) = daemon_fence {
            if !daemon_fence_is_current(&tx, daemon_fence, &now)? {
                tx.commit()?;
                return Ok(None);
            }
        }
        let candidate_id = tx
            .query_row(
                "SELECT r.id
                 FROM hive_runs r
                 JOIN hive_controllers c ON c.id = r.controller_id
                 WHERE r.status = 'queued'
                   AND c.status = 'active'
                   AND (
                       r.group_turn_id IS NULL OR EXISTS (
                           SELECT 1
                           FROM hive_group_turns claim_turn
                           JOIN hive_groups claim_group
                             ON claim_group.id = claim_turn.group_id
                           WHERE claim_turn.id = r.group_turn_id
                             AND claim_turn.group_id = r.group_id
                             AND claim_turn.status = 'running'
                             AND claim_group.status = 'active'
                       )
                   )
                   AND (
                       r.worker_id IS NULL OR EXISTS (
                           SELECT 1
                           FROM hive_workers worker
                           JOIN sessions session ON session.id = r.session_id
                           WHERE worker.id = r.worker_id
                             AND worker.status = 'active'
                             AND c.worker_id = worker.id
                             AND c.user_id IS worker.user_id
                             AND session.user_id IS worker.user_id
                             AND (
                                 worker.dm_session_id = r.session_id
                                 OR EXISTS (
                                     SELECT 1 FROM hive_group_worker_lanes lane
                                     WHERE lane.worker_id = worker.id
                                       AND lane.session_id = r.session_id
                                 )
                             )
                       )
                   )
                   AND r.available_at <= ?1
                   AND NOT EXISTS (
                       SELECT 1
                       FROM hive_worker_governor_override_grants recovery_grant
                       WHERE recovery_grant.id = r.governor_override_id
                         AND recovery_grant.bypass_unresolved_provider_call = 1
                         AND recovery_grant.bypass_daily_call_cap = 0
                         AND recovery_grant.bypass_daily_token_cap = 0
                         AND recovery_grant.bypass_quiet_hours = 0
                         AND recovery_grant.bypass_idle_backoff = 0
                         AND recovery_grant.expires_at <= ?1
                         AND NOT EXISTS (
                             SELECT 1
                             FROM hive_worker_governor_override_consumptions consumption
                             WHERE consumption.grant_id = recovery_grant.id
                         )
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM hive_runs uncertain
                       WHERE uncertain.controller_id = r.controller_id
                         AND uncertain.status = 'recovery_required'
                   )
                   AND (SELECT COUNT(*) FROM hive_runs active
                        WHERE active.status IN ('leased', 'running')) < ?2
                   AND (SELECT COUNT(*) FROM hive_runs active
                        WHERE active.controller_id = r.controller_id
                          AND active.status IN ('leased', 'running')) < c.max_concurrent_runs
                   AND (
                       r.concurrency_key IS NULL OR NOT EXISTS (
                           SELECT 1 FROM hive_runs active
                           WHERE active.concurrency_key = r.concurrency_key
                             AND active.status IN ('leased', 'running')
                       )
                   )
                 ORDER BY r.priority DESC, r.available_at ASC, r.created_at ASC, r.id ASC
                 LIMIT 1",
                params![now, request.global_concurrency_limit],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(candidate_id) = candidate_id else {
            tx.commit()?;
            return Ok(None);
        };

        let lease_token = uuid::Uuid::new_v4().to_string();
        let attempt_id = uuid::Uuid::new_v4().to_string();
        let changed = tx.execute(
            "UPDATE hive_runs
             SET status = 'leased', lease_owner = ?2, lease_token = ?3,
                 lease_epoch = ?4, lease_expires_at = ?5, heartbeat_at = ?6,
                 attempt_count = attempt_count + 1, updated_at = ?6
             WHERE id = ?1 AND status = 'queued' AND available_at <= ?6
               AND (
                   group_turn_id IS NULL OR EXISTS (
                       SELECT 1
                       FROM hive_group_turns claim_turn
                       JOIN hive_groups claim_group
                         ON claim_group.id = claim_turn.group_id
                       WHERE claim_turn.id = hive_runs.group_turn_id
                         AND claim_turn.group_id = hive_runs.group_id
                         AND claim_turn.status = 'running'
                         AND claim_group.status = 'active'
                   )
               )",
            params![
                candidate_id,
                request.executor_id,
                lease_token,
                request.lease_epoch,
                lease_expires_at,
                now,
            ],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(None);
        }

        let attempt_no = tx.query_row(
            "SELECT attempt_count FROM hive_runs WHERE id = ?1",
            [&candidate_id],
            |row| Ok(nonnegative_i64(row, 0)? as u32),
        )?;
        tx.execute(
            "INSERT INTO hive_run_attempts (
                id, run_id, attempt_no, executor_id, lease_token, lease_epoch,
                started_at, finished_at, outcome, stop_reason, error, retry_at,
                trace_sequence_start, trace_sequence_end
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 'leased', NULL, NULL, NULL, NULL, NULL)",
            params![
                attempt_id,
                candidate_id,
                attempt_no,
                request.executor_id,
                lease_token,
                request.lease_epoch,
                now,
            ],
        )?;
        let select = format!("SELECT {RUN_COLUMNS} FROM hive_runs WHERE id = ?1");
        let run = tx.query_row(&select, [&candidate_id], map_run)?;
        tx.commit()?;

        Ok(Some(ClaimedHiveRun {
            run,
            attempt_id,
            attempt_no,
            lease_token,
        }))
    }

    pub fn mark_running(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        self.mark_running_inner(run_id, lease_token, lease_epoch, now, None, None)
    }

    pub fn mark_running_fenced(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        now: DateTime<Utc>,
        daemon_fence: &DaemonFence,
    ) -> Result<bool> {
        self.mark_running_inner(
            run_id,
            lease_token,
            lease_epoch,
            now,
            None,
            Some(daemon_fence),
        )
    }

    /// Marks a lease as executing and anchors the attempt to the canonical trace stream.
    pub fn mark_running_with_trace(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        now: DateTime<Utc>,
        trace_sequence_start: Option<i64>,
    ) -> Result<bool> {
        self.mark_running_inner(
            run_id,
            lease_token,
            lease_epoch,
            now,
            trace_sequence_start,
            None,
        )
    }

    fn mark_running_inner(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        now: DateTime<Utc>,
        trace_sequence_start: Option<i64>,
        daemon_fence: Option<&DaemonFence>,
    ) -> Result<bool> {
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let now = canonical_timestamp(now);
        if let Some(daemon_fence) = daemon_fence {
            if lease_epoch != daemon_fence.fencing_token
                || !daemon_fence_is_current(&tx, daemon_fence, &now)?
            {
                tx.commit()?;
                return Ok(false);
            }
        }
        let changed = tx.execute(
            "UPDATE hive_runs
             SET status = 'running', started_at = COALESCE(started_at, ?4),
                 heartbeat_at = ?4, updated_at = ?4
             WHERE id = ?1 AND lease_token = ?2 AND lease_epoch = ?3
               AND status = 'leased' AND lease_expires_at > ?4
               AND (
                   group_turn_id IS NULL OR EXISTS (
                       SELECT 1
                       FROM hive_group_turns running_turn
                       JOIN hive_groups running_group
                         ON running_group.id = running_turn.group_id
                       WHERE running_turn.id = hive_runs.group_turn_id
                         AND running_turn.group_id = hive_runs.group_id
                         AND running_turn.status = 'running'
                         AND running_group.status = 'active'
                   )
               )
               AND EXISTS (
                   SELECT 1 FROM hive_controllers c
                   WHERE c.id = hive_runs.controller_id AND c.status = 'active'
                     AND (
                         hive_runs.worker_id IS NULL OR EXISTS (
                             SELECT 1 FROM hive_workers worker
                             WHERE worker.id = hive_runs.worker_id
                               AND worker.status = 'active'
                               AND c.worker_id = worker.id
                               AND c.user_id IS worker.user_id
                               AND (
                                   worker.dm_session_id = hive_runs.session_id
                                   OR EXISTS (
                                       SELECT 1 FROM hive_group_worker_lanes lane
                                       WHERE lane.worker_id = worker.id
                                         AND lane.session_id = hive_runs.session_id
                                   )
                               )
                         )
                     )
               )",
            params![run_id, lease_token, lease_epoch, now],
        )?;
        if changed == 1 {
            let attempt_changed = tx.execute(
                "UPDATE hive_run_attempts
                 SET trace_sequence_start = ?4
                 WHERE run_id = ?1 AND lease_token = ?2 AND lease_epoch = ?3
                   AND finished_at IS NULL",
                params![run_id, lease_token, lease_epoch, trace_sequence_start],
            )?;
            anyhow::ensure!(
                attempt_changed == 1,
                "leased Hive run has no matching open attempt"
            );
            materialize_objective_message(&tx, run_id, &now)?;
            update_derived_state(&tx, run_id, HiveRunStatus::Running, &now)?;
        }
        tx.commit()?;
        Ok(changed == 1)
    }

    pub fn heartbeat(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        now: DateTime<Utc>,
        lease_duration: std::time::Duration,
    ) -> Result<bool> {
        anyhow::ensure!(!lease_duration.is_zero(), "lease duration is zero");
        let delta = Duration::from_std(lease_duration).context("lease duration is too large")?;
        let expires_at = now
            .checked_add_signed(delta)
            .context("lease expiry overflow")?;
        let now = canonical_timestamp(now);
        let expires_at = canonical_timestamp(expires_at);
        let changed = self.db.conn().execute(
            "UPDATE hive_runs
             SET heartbeat_at = ?4, lease_expires_at = ?5, updated_at = ?4
             WHERE id = ?1 AND lease_token = ?2 AND lease_epoch = ?3
               AND status IN ('leased', 'running') AND lease_expires_at > ?4
               AND EXISTS (
                   SELECT 1 FROM hive_controllers c
                   WHERE c.id = hive_runs.controller_id AND c.status = 'active'
                     AND (
                         hive_runs.worker_id IS NULL OR EXISTS (
                             SELECT 1 FROM hive_workers worker
                             WHERE worker.id = hive_runs.worker_id
                               AND worker.status = 'active'
                               AND c.worker_id = worker.id
                               AND c.user_id IS worker.user_id
                               AND (
                                   worker.dm_session_id = hive_runs.session_id
                                   OR EXISTS (
                                       SELECT 1 FROM hive_group_worker_lanes lane
                                       WHERE lane.worker_id = worker.id
                                         AND lane.session_id = hive_runs.session_id
                                   )
                               )
                         )
                     )
               )",
            params![run_id, lease_token, lease_epoch, now, expires_at],
        )?;
        Ok(changed == 1)
    }

    pub fn heartbeat_fenced(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        now: DateTime<Utc>,
        lease_duration: std::time::Duration,
        daemon_fence: &DaemonFence,
    ) -> Result<bool> {
        anyhow::ensure!(!lease_duration.is_zero(), "lease duration is zero");
        let delta = Duration::from_std(lease_duration).context("lease duration is too large")?;
        let expires_at = now
            .checked_add_signed(delta)
            .context("lease expiry overflow")?;
        let now = canonical_timestamp(now);
        let expires_at = canonical_timestamp(expires_at);
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        if lease_epoch != daemon_fence.fencing_token
            || !daemon_fence_is_current(&tx, daemon_fence, &now)?
        {
            tx.commit()?;
            return Ok(false);
        }
        let changed = tx.execute(
            "UPDATE hive_runs
             SET heartbeat_at = ?4, lease_expires_at = ?5, updated_at = ?4
             WHERE id = ?1 AND lease_token = ?2 AND lease_epoch = ?3
               AND status IN ('leased', 'running') AND lease_expires_at > ?4
               AND EXISTS (
                   SELECT 1 FROM hive_controllers c
                   WHERE c.id = hive_runs.controller_id AND c.status = 'active'
                     AND (
                         hive_runs.worker_id IS NULL OR EXISTS (
                             SELECT 1 FROM hive_workers worker
                             WHERE worker.id = hive_runs.worker_id
                               AND worker.status = 'active'
                               AND c.worker_id = worker.id
                               AND c.user_id IS worker.user_id
                               AND (
                                   worker.dm_session_id = hive_runs.session_id
                                   OR EXISTS (
                                       SELECT 1 FROM hive_group_worker_lanes lane
                                       WHERE lane.worker_id = worker.id
                                         AND lane.session_id = hive_runs.session_id
                                   )
                               )
                         )
                     )
               )",
            params![run_id, lease_token, lease_epoch, now, expires_at],
        )?;
        tx.commit()?;
        Ok(changed == 1)
    }

    /// Finish an attempt using its lease fence. A stale worker receives `None`.
    pub fn finish_claimed(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        completion: &RunCompletion,
    ) -> Result<Option<HiveRunStatus>> {
        self.finish_claimed_inner(
            run_id,
            lease_token,
            lease_epoch,
            completion,
            None,
            FinishClaimAuthority::Ordinary,
        )
    }

    pub fn finish_claimed_fenced(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        completion: &RunCompletion,
        daemon_fence: &DaemonFence,
    ) -> Result<Option<HiveRunStatus>> {
        self.finish_claimed_inner(
            run_id,
            lease_token,
            lease_epoch,
            completion,
            Some(daemon_fence),
            FinishClaimAuthority::Ordinary,
        )
    }

    /// Authoritatively close a user-cancelled running claim after the
    /// execution host's cooperative grace period. This is deliberately
    /// stricter than a normal completion: the exact worker lease and current
    /// daemon generation must still own the open attempt, and the controller
    /// must already be disabled by a committed, ownership-checked
    /// `CancelSession` mutation.
    pub fn finish_cancelled_claim_fenced(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        completion: &RunCompletion,
        daemon_fence: &DaemonFence,
    ) -> Result<Option<HiveRunStatus>> {
        anyhow::ensure!(
            completion.target_status == HiveRunStatus::Cancelled,
            "forced cancellation completion must target cancelled"
        );
        self.finish_claimed_inner(
            run_id,
            lease_token,
            lease_epoch,
            completion,
            Some(daemon_fence),
            FinishClaimAuthority::DisabledControllerCancellation,
        )
    }

    /// Finish an exact ordinary Worker direct-chat claim after its owner has
    /// committed the typed Stop marker. Unlike whole-session cancellation,
    /// this authority deliberately keeps the Worker controller active.
    pub fn finish_stopped_worker_conversation_claim_fenced(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        completion: &RunCompletion,
        daemon_fence: &DaemonFence,
    ) -> Result<Option<HiveRunStatus>> {
        anyhow::ensure!(
            completion.target_status == HiveRunStatus::Cancelled,
            "Worker conversation Stop completion must target cancelled"
        );
        self.finish_claimed_inner(
            run_id,
            lease_token,
            lease_epoch,
            completion,
            Some(daemon_fence),
            FinishClaimAuthority::StoppedWorkerConversation,
        )
    }

    /// Finish one exact group-member claim after its owning turn was durably
    /// cancelled. Group lanes keep their Worker controller active, so this
    /// authority is distinct from whole-session cancellation and requires the
    /// exact turn, group owner, Worker, controller, lane, lease, and daemon
    /// generation to remain bound.
    pub fn finish_cancelled_group_turn_claim_fenced(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        completion: &RunCompletion,
        daemon_fence: &DaemonFence,
    ) -> Result<Option<HiveRunStatus>> {
        anyhow::ensure!(
            completion.target_status == HiveRunStatus::Cancelled,
            "group-turn cancellation completion must target cancelled"
        );
        self.finish_claimed_inner(
            run_id,
            lease_token,
            lease_epoch,
            completion,
            Some(daemon_fence),
            FinishClaimAuthority::CancelledGroupTurn,
        )
    }

    fn finish_claimed_inner(
        &self,
        run_id: &str,
        lease_token: &str,
        lease_epoch: u64,
        completion: &RunCompletion,
        daemon_fence: Option<&DaemonFence>,
        authority: FinishClaimAuthority,
    ) -> Result<Option<HiveRunStatus>> {
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let now = canonical_timestamp(completion.now);
        if let Some(daemon_fence) = daemon_fence {
            if lease_epoch != daemon_fence.fencing_token
                || !daemon_fence_is_current(&tx, daemon_fence, &now)?
            {
                tx.commit()?;
                return Ok(None);
            }
        }
        let daemon_owner = daemon_fence.map(|fence| fence.owner_id.as_str());
        let authority_code = authority as i64;
        let state = tx
            .query_row(
                "SELECT r.status, r.attempt_count, r.max_attempts
                 FROM hive_runs r
                 JOIN hive_controllers c ON c.id = r.controller_id
                 WHERE r.id = ?1 AND r.lease_token = ?2 AND r.lease_epoch = ?3
                   AND r.status IN ('leased', 'running') AND r.lease_expires_at > ?4
                   AND (?5 IS NULL OR r.lease_owner = ?5)
                   AND (
                       ?6 = 0
                       OR (?6 = 1 AND c.status = 'disabled')
                       OR (
                           ?6 = 2 AND c.status = 'active'
                           AND r.kind = 'worker_conversation'
                           AND r.session_id IS NOT NULL
                           AND r.schedule_id IS NULL AND r.group_id IS NULL
                           AND r.governor_origin = 'user_dm'
                           AND r.governor_lane_key = 'dm'
                           AND r.last_stop_reason = ?7
                           AND json_valid(r.execution_context_json)
                           AND json_extract(r.execution_context_json, '$.mode.kind')
                               IN (
                                   'worker_conversation_neutral',
                                   'worker_workspace_attached'
                               )
                           AND json_extract(r.execution_context_json, '$.mode.lane.kind')
                               = 'direct_message'
                           AND json_extract(r.execution_context_json, '$.mode.worker_id')
                               = r.worker_id
                           AND EXISTS (
                               SELECT 1 FROM hive_workers stopped_worker
                               WHERE stopped_worker.id = r.worker_id
                                 AND stopped_worker.dm_session_id = r.session_id
                                 AND json_extract(
                                     r.execution_context_json,
                                     '$.mode.worker_revision'
                                 ) = stopped_worker.revision
                           )
                           AND (
                               json_extract(r.execution_context_json, '$.mode.kind')
                                   = 'worker_conversation_neutral'
                               OR EXISTS (
                                   SELECT 1 FROM sessions stopped_session
                                   WHERE stopped_session.id = r.session_id
                                     AND stopped_session.workspace_mode = json_extract(
                                         r.execution_context_json,
                                         '$.mode.workspace_mode'
                                     )
                                     AND stopped_session.working_dir = json_extract(
                                         r.execution_context_json,
                                         '$.mode.working_dir'
                                     )
                                     AND stopped_session.project_dir IS json_extract(
                                         r.execution_context_json,
                                         '$.mode.project_dir'
                                     )
                               )
                           )
                       )
                       OR (
                           ?6 = 3 AND c.status = 'active'
                           AND r.kind = 'group_turn'
                           AND r.group_turn_id IS NOT NULL
                           AND r.group_id IS NOT NULL
                           AND r.worker_id IS NOT NULL
                           AND c.worker_id = r.worker_id
                           AND EXISTS (
                               SELECT 1
                               FROM hive_group_turns cancelled_turn
                               JOIN hive_groups cancelled_group
                                 ON cancelled_group.id = cancelled_turn.group_id
                               WHERE cancelled_turn.id = r.group_turn_id
                                 AND cancelled_turn.group_id = r.group_id
                                 AND (
                                     cancelled_turn.status = 'cancelled'
                                     OR cancelled_group.status = 'archived'
                                 )
                                 AND cancelled_group.user_id IS c.user_id
                           )
                           AND EXISTS (
                               SELECT 1
                               FROM hive_group_worker_lanes cancelled_lane
                               WHERE cancelled_lane.group_id = r.group_id
                                 AND cancelled_lane.worker_id = r.worker_id
                                 AND cancelled_lane.session_id = r.session_id
                           )
                       )
                   )
                   AND (
                       ?6 IN (1, 3) OR r.worker_id IS NULL OR EXISTS (
                           SELECT 1 FROM hive_workers worker
                           WHERE worker.id = r.worker_id
                             AND worker.status = 'active'
                             AND c.worker_id = worker.id
                             AND c.user_id IS worker.user_id
                             AND (
                                 worker.dm_session_id = r.session_id
                                 OR EXISTS (
                                     SELECT 1 FROM hive_group_worker_lanes lane
                                     WHERE lane.worker_id = worker.id
                                       AND lane.session_id = r.session_id
                                 )
                             )
                       )
                   )
                   AND EXISTS (
                       SELECT 1 FROM hive_run_attempts a
                       WHERE a.run_id = r.id AND a.attempt_no = r.attempt_count
                         AND a.lease_token = ?2 AND a.lease_epoch = ?3
                         AND a.finished_at IS NULL
                   )",
                params![
                    run_id,
                    lease_token,
                    lease_epoch,
                    now,
                    daemon_owner,
                    authority_code,
                    WORKER_CONVERSATION_STOP_REQUESTED_REASON,
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        nonnegative_i64(row, 1)? as u32,
                        nonnegative_i64(row, 2)? as u32,
                    ))
                },
            )
            .optional()?;
        let Some((current_raw, attempt_no, max_attempts)) = state else {
            tx.commit()?;
            return Ok(None);
        };
        let current = HiveRunStatus::parse(&current_raw)
            .ok_or_else(|| anyhow::anyhow!("invalid persisted run status: {current_raw}"))?;
        let mut target =
            if completion.target_status == HiveRunStatus::RetryWait && attempt_no >= max_attempts {
                HiveRunStatus::DeadLetter
            } else {
                completion.target_status
            };
        current.ensure_transition_to(target)?;
        anyhow::ensure!(
            target != HiveRunStatus::RetryWait || completion.available_at.is_some(),
            "retry_wait requires available_at"
        );
        anyhow::ensure!(
            target != HiveRunStatus::Sleeping || completion.wake_at.is_some(),
            "sleeping requires wake_at"
        );
        let requested_target = target;
        let (worker_id, kind): (Option<String>, String) = tx.query_row(
            "SELECT worker_id, kind FROM hive_runs WHERE id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut adopted_canonical_output = None;
        let mut forced_missing_output_recovery = false;
        if authority == FinishClaimAuthority::StoppedWorkerConversation {
            anyhow::ensure!(
                worker_id.is_some() && kind == HiveRunKind::WorkerConversation.as_str(),
                "Worker conversation Stop completion lost its exact run kind"
            );
            match finalize_stopped_worker_conversation_in_transaction(
                &tx,
                run_id,
                lease_token,
                lease_epoch,
                &now,
            )? {
                StoppedWorkerConversationFinalization::CanonicalResponseAdopted => {
                    let (response_message_id, response_group_message_id) =
                        committed_worker_response_in_transaction(&tx, run_id)?
                            .context("adopted stopped Worker response disappeared")?;
                    target = HiveRunStatus::Succeeded;
                    adopted_canonical_output = Some(serde_json::json!({
                        "kind": "succeeded",
                        "recovered": "canonical_worker_response",
                        "response_message_id": response_message_id,
                        "response_group_message_id": response_group_message_id,
                    }));
                }
                StoppedWorkerConversationFinalization::Cancelled => {
                    target = HiveRunStatus::Cancelled;
                }
            }
        } else if worker_id.is_some() && kind == HiveRunKind::WorkerIntroductionReview.as_str() {
            match reconcile_worker_introduction_review_in_transaction(
                &tx,
                run_id,
                lease_token,
                lease_epoch,
                &now,
            )? {
                WorkerIntroductionReviewRecovery::CanonicalAuditAdopted { review_id, status } => {
                    target = HiveRunStatus::Succeeded;
                    if requested_target != HiveRunStatus::Succeeded {
                        adopted_canonical_output = Some(serde_json::json!({
                            "kind": "succeeded",
                            "recovered": "canonical_worker_introduction_review",
                            "review_id": review_id,
                            "review_status": status,
                        }));
                    }
                }
                WorkerIntroductionReviewRecovery::PreProviderStale { review_id, reason } => {
                    target = HiveRunStatus::Succeeded;
                    adopted_canonical_output = Some(serde_json::json!({
                        "kind": "succeeded",
                        "recovered": "worker_introduction_review_pre_provider_stale",
                        "review_id": review_id,
                        "reason": reason,
                    }));
                }
                WorkerIntroductionReviewRecovery::TerminalFailure { review_id } => {
                    target = HiveRunStatus::Failed;
                    adopted_canonical_output = Some(serde_json::json!({
                        "kind": "failed",
                        "recovered": "terminal_worker_introduction_review_failure",
                        "review_id": review_id,
                    }));
                }
                WorkerIntroductionReviewRecovery::SafeBeforeProviderBoundary => {
                    if requested_target == HiveRunStatus::Succeeded {
                        target = HiveRunStatus::RecoveryRequired;
                        forced_missing_output_recovery = true;
                    } else if requested_target != HiveRunStatus::Sleeping {
                        tx.execute(
                            "UPDATE hive_worker_introduction_reviews
                             SET status = 'failed',
                                 last_error = 'review run failed before provider admission',
                                 completed_at = ?2, updated_at = ?2
                             WHERE run_id = ?1 AND status = 'queued'",
                            params![run_id, now],
                        )?;
                    }
                }
                WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit
                | WorkerIntroductionReviewRecovery::NotWorkerIntroductionReview => {
                    target = HiveRunStatus::RecoveryRequired;
                    forced_missing_output_recovery = true;
                }
            }
        } else if worker_id.is_some() && kind == HiveRunKind::WorkerWorkflow.as_str() {
            match reconcile_worker_workflow_provider_boundary_in_transaction(
                &tx,
                run_id,
                lease_token,
                lease_epoch,
                &now,
            )? {
                WorkerWorkflowProviderRecovery::CanonicalOutcomeAdopted => {
                    let committed = committed_worker_goal_outcome_in_transaction(&tx, run_id)?
                        .context("adopted Worker Goal outcome disappeared")?;
                    target = HiveRunStatus::Succeeded;
                    if requested_target != HiveRunStatus::Succeeded {
                        adopted_canonical_output = Some(serde_json::json!({
                            "kind": "succeeded",
                            "recovered": "canonical_worker_goal_outcome",
                            "workflow_goal_id": committed.workflow_goal_id,
                            "workflow_attempt_id": committed.workflow_attempt_id,
                        }));
                    }
                }
                WorkerWorkflowProviderRecovery::SafeBeforeProviderBoundary => {
                    if requested_target == HiveRunStatus::Succeeded {
                        target = HiveRunStatus::RecoveryRequired;
                        forced_missing_output_recovery = true;
                    }
                }
                WorkerWorkflowProviderRecovery::ProviderBoundaryWithoutOutcome
                | WorkerWorkflowProviderRecovery::CommittedOutcomeUnaccounted
                | WorkerWorkflowProviderRecovery::NotWorkerWorkflow => {
                    target = HiveRunStatus::RecoveryRequired;
                    forced_missing_output_recovery = true;
                }
            }
        } else if worker_id.is_some()
            && (target != HiveRunStatus::Cancelled
                || (authority == FinishClaimAuthority::Ordinary
                    && kind == HiveRunKind::WorkerConversation.as_str()))
        {
            if kind == HiveRunKind::WorkerIntroduction.as_str() {
                if let Some(opening_message_id) =
                    committed_worker_introduction_opening(&tx, run_id)?
                {
                    reconcile_committed_introduction_provider_calls_in_transaction(
                        &tx,
                        run_id,
                        lease_token,
                        lease_epoch,
                        &now,
                    )?;
                    target = HiveRunStatus::Succeeded;
                    if requested_target != HiveRunStatus::Succeeded {
                        adopted_canonical_output = Some(serde_json::json!({
                            "kind": "succeeded",
                            "recovered": "canonical_worker_introduction_opening",
                            "opening_message_id": opening_message_id,
                        }));
                    }
                } else {
                    match reconcile_expired_worker_response_in_transaction(
                        &tx,
                        run_id,
                        lease_token,
                        lease_epoch,
                        &now,
                    )? {
                        ExpiredWorkerResponseDisposition::SafeBeforeProviderBoundary => {}
                        ExpiredWorkerResponseDisposition::ProviderBoundaryWithoutResponse => {
                            target = HiveRunStatus::RecoveryRequired;
                            forced_missing_output_recovery = true;
                        }
                        ExpiredWorkerResponseDisposition::CanonicalResponseAdopted => {
                            anyhow::bail!("Worker Introduction adopted an ordinary response key")
                        }
                        ExpiredWorkerResponseDisposition::NotWorkerBound => {
                            anyhow::bail!("Worker Introduction lost its Worker binding")
                        }
                    }
                }
            } else {
                match reconcile_expired_worker_response_in_transaction(
                    &tx,
                    run_id,
                    lease_token,
                    lease_epoch,
                    &now,
                )? {
                    ExpiredWorkerResponseDisposition::CanonicalResponseAdopted => {
                        let (response_message_id, response_group_message_id) =
                            committed_worker_response_in_transaction(&tx, run_id)?
                                .context("adopted Worker response disappeared")?;
                        target = HiveRunStatus::Succeeded;
                        if requested_target != HiveRunStatus::Succeeded {
                            adopted_canonical_output = Some(serde_json::json!({
                                "kind": "succeeded",
                                "recovered": "canonical_worker_response",
                                "response_message_id": response_message_id,
                                "response_group_message_id": response_group_message_id,
                            }));
                        }
                    }
                    ExpiredWorkerResponseDisposition::SafeBeforeProviderBoundary => {}
                    ExpiredWorkerResponseDisposition::ProviderBoundaryWithoutResponse => {
                        target = HiveRunStatus::RecoveryRequired;
                        forced_missing_output_recovery = true;
                    }
                    ExpiredWorkerResponseDisposition::NotWorkerBound => {
                        anyhow::bail!("Worker completion lost its Worker binding")
                    }
                }
            }
        }
        current.ensure_transition_to(target)?;
        if target == HiveRunStatus::Succeeded {
            if worker_id.is_some() && kind == HiveRunKind::WorkerIntroductionReview.as_str() {
                anyhow::ensure!(
                    matches!(
                        reconcile_worker_introduction_review_in_transaction(
                            &tx,
                            run_id,
                            lease_token,
                            lease_epoch,
                            &now,
                        )?,
                        WorkerIntroductionReviewRecovery::CanonicalAuditAdopted { .. }
                            | WorkerIntroductionReviewRecovery::PreProviderStale { .. }
                    ),
                    "Worker Introduction review cannot succeed before its exact audit commits"
                );
            } else if worker_id.is_some() && kind == HiveRunKind::WorkerWorkflow.as_str() {
                anyhow::ensure!(
                    committed_worker_goal_outcome_in_transaction(&tx, run_id)?.is_some(),
                    "Worker Workflow cannot succeed before its exact outcome commits"
                );
                anyhow::ensure!(
                    worker_goal_outcome_is_accounted_in_transaction(&tx, run_id)?,
                    "Worker Workflow outcome has unresolved provider accounting"
                );
            } else if worker_id.is_some() && kind != HiveRunKind::WorkerIntroduction.as_str() {
                anyhow::ensure!(
                    committed_worker_response_in_transaction(&tx, run_id)?.is_some(),
                    "Worker run cannot succeed before its exact canonical response commits"
                );
                anyhow::ensure!(
                    reconcile_expired_worker_response_in_transaction(
                        &tx,
                        run_id,
                        lease_token,
                        lease_epoch,
                        &now,
                    )? == ExpiredWorkerResponseDisposition::CanonicalResponseAdopted,
                    "Worker run response has no exact terminal or adoptable provider call"
                );
            } else if worker_id.is_some() {
                anyhow::ensure!(
                    committed_worker_introduction_opening(&tx, run_id)?.is_some(),
                    "Worker Introduction cannot succeed before its exact opening commits"
                );
                reconcile_committed_introduction_provider_calls_in_transaction(
                    &tx,
                    run_id,
                    lease_token,
                    lease_epoch,
                    &now,
                )?;
            }
        }

        let available_at = if target == HiveRunStatus::RetryWait {
            completion.available_at.map(canonical_timestamp)
        } else {
            None
        };
        let wake_at = if target == HiveRunStatus::Sleeping {
            completion.wake_at.map(canonical_timestamp)
        } else {
            None
        };
        let retry_at = if target == HiveRunStatus::RetryWait {
            available_at.clone()
        } else {
            None
        };
        let adopted_stop_reason = adopted_canonical_output.as_ref().map(|outcome| {
            if outcome.get("recovered").and_then(serde_json::Value::as_str)
                == Some("canonical_worker_response")
            {
                "canonical Worker response adopted during recovery completion"
            } else {
                "canonical Worker output adopted during recovery completion"
            }
        });
        let missing_output_stop_reason = forced_missing_output_recovery
            .then_some("provider outcome is uncertain without canonical Worker output");
        let stopped_conversation_reason = (authority
            == FinishClaimAuthority::StoppedWorkerConversation
            && target == HiveRunStatus::Cancelled)
            .then_some(WORKER_CONVERSATION_STOP_REQUESTED_REASON);
        let effective_stop_reason = adopted_stop_reason
            .or(missing_output_stop_reason)
            .or(stopped_conversation_reason)
            .or(completion.stop_reason.as_deref());
        let effective_error = if adopted_canonical_output.is_some() {
            None
        } else if forced_missing_output_recovery {
            Some("explicit recovery is required before this Worker can run again")
        } else {
            completion.error.as_deref()
        };
        let outcome_json = if let Some(outcome) = adopted_canonical_output.as_ref() {
            Some(serde_json::to_string(outcome)?)
        } else if forced_missing_output_recovery {
            Some(serde_json::to_string(&serde_json::json!({
                "kind": "recovery_required",
                "reason": "canonical_worker_output_missing",
            }))?)
        } else {
            completion
                .outcome
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?
        };
        let finished_at = target.is_terminal().then_some(now.as_str());
        let changed = tx.execute(
            "UPDATE hive_runs
             SET status = ?5, available_at = COALESCE(?6, available_at), wake_at = ?7,
                 lease_owner = NULL, lease_token = NULL, lease_epoch = NULL,
                 lease_expires_at = NULL, heartbeat_at = NULL,
                 last_stop_reason = ?8, last_error = ?9, outcome_json = ?10,
                 finished_at = COALESCE(?11, finished_at), updated_at = ?4
             WHERE id = ?1 AND lease_token = ?2 AND lease_epoch = ?3
               AND status = ?12",
            params![
                run_id,
                lease_token,
                lease_epoch,
                now,
                target.to_string(),
                available_at,
                wake_at,
                effective_stop_reason,
                effective_error,
                outcome_json,
                finished_at,
                current.to_string(),
            ],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(None);
        }
        let attempt_changed = tx.execute(
            "UPDATE hive_run_attempts
             SET finished_at = ?4, outcome = ?5, stop_reason = ?6, error = ?7,
                 retry_at = ?8, trace_sequence_end = ?9
             WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3",
            params![
                run_id,
                attempt_no,
                lease_token,
                now,
                attempt_outcome(target).as_str(),
                effective_stop_reason,
                effective_error,
                retry_at,
                completion.trace_sequence_end,
            ],
        )?;
        anyhow::ensure!(
            attempt_changed == 1,
            "claimed Hive run has no matching open attempt during completion"
        );
        if target == HiveRunStatus::RecoveryRequired && kind == HiveRunKind::WorkerWorkflow.as_str()
        {
            let _ = pause_worker_workflow_after_uncertain_run_in_transaction(&tx, run_id, &now)?;
        }
        if target == HiveRunStatus::Succeeded && kind != HiveRunKind::WorkerWorkflow.as_str() {
            let _ = materialize_oldest_staged_input_in_transaction(&tx, run_id, &now)?;
        } else if authority == FinishClaimAuthority::StoppedWorkerConversation
            && target == HiveRunStatus::Cancelled
        {
            let _ = materialize_oldest_staged_input_with_authority_in_transaction(
                &tx,
                run_id,
                WorkerConversationPredecessorAuthority::StoppedWorkerConversation,
                &now,
            )?;
        } else if authority == FinishClaimAuthority::Ordinary
            && matches!(
                target,
                HiveRunStatus::Failed | HiveRunStatus::DeadLetter | HiveRunStatus::Cancelled
            )
        {
            // An accepted DM must not disappear merely because its exact
            // predecessor failed. The authority helper admits only an
            // ordinary direct-message run with no canonical response and a
            // still-current Worker/session binding, then materializes one
            // oldest successor atomically with this terminal transition.
            let _ = materialize_oldest_staged_input_with_authority_in_transaction(
                &tx,
                run_id,
                WorkerConversationPredecessorAuthority::TerminalWithoutCanonicalResponse,
                &now,
            )?;
        }
        update_derived_state(&tx, run_id, target, &now)?;
        if matches!(
            target,
            HiveRunStatus::Succeeded
                | HiveRunStatus::Failed
                | HiveRunStatus::Cancelled
                | HiveRunStatus::DeadLetter
                | HiveRunStatus::RecoveryRequired
        ) {
            discard_pending_controls(&tx, run_id, "run finished before control delivery", &now)?;
        }
        tx.commit()?;
        Ok(Some(target))
    }

    /// Reconcile expired leases without replaying uncertain mutating work.
    /// A run that never crossed the durable `running` boundary is safe to put
    /// back on the queue; a running attempt may have produced external side
    /// effects and therefore requires an explicit recovery decision.
    pub fn reconcile_expired_leases(&self, now: DateTime<Utc>) -> Result<LeaseReconciliation> {
        self.reconcile_expired_leases_inner(now, None)
    }

    pub fn reconcile_expired_leases_fenced(
        &self,
        now: DateTime<Utc>,
        daemon_fence: &DaemonFence,
    ) -> Result<LeaseReconciliation> {
        self.reconcile_expired_leases_inner(now, Some(daemon_fence))
    }

    fn reconcile_expired_leases_inner(
        &self,
        now: DateTime<Utc>,
        daemon_fence: Option<&DaemonFence>,
    ) -> Result<LeaseReconciliation> {
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let now = canonical_timestamp(now);
        if let Some(daemon_fence) = daemon_fence {
            if !daemon_fence_is_current(&tx, daemon_fence, &now)? {
                tx.commit()?;
                return Ok(LeaseReconciliation::default());
            }
        }
        let mut statement = tx.prepare(
            "SELECT id, status, attempt_count, lease_token, lease_epoch, kind
             FROM hive_runs
             WHERE status IN ('leased', 'running') AND lease_expires_at <= ?1
             ORDER BY lease_expires_at ASC",
        )?;
        let expired = statement
            .query_map([&now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    nonnegative_i64(row, 2)? as u32,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);

        let mut result = LeaseReconciliation::default();
        for (run_id, status, attempt_no, lease_token, lease_epoch, kind) in expired {
            let lease_epoch = lease_epoch
                .map(u64::try_from)
                .transpose()
                .context("expired Hive run has a negative lease epoch")?;
            let review_disposition = if kind == HiveRunKind::WorkerIntroductionReview.as_str() {
                match (lease_token.as_deref(), lease_epoch) {
                    (Some(lease_token), Some(lease_epoch)) => {
                        reconcile_worker_introduction_review_in_transaction(
                            &tx,
                            &run_id,
                            lease_token,
                            lease_epoch,
                            &now,
                        )?
                    }
                    _ => WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit,
                }
            } else {
                WorkerIntroductionReviewRecovery::NotWorkerIntroductionReview
            };
            if let WorkerIntroductionReviewRecovery::CanonicalAuditAdopted { review_id, .. }
            | WorkerIntroductionReviewRecovery::PreProviderStale { review_id, .. } =
                &review_disposition
            {
                let reason =
                    "committed Worker Introduction review recovered after executor lease expiry";
                let outcome_json = serde_json::to_string(&serde_json::json!({
                    "kind": "succeeded",
                    "recovered": "canonical_worker_introduction_review",
                    "review_id": review_id,
                    "review_status": match &review_disposition {
                        WorkerIntroductionReviewRecovery::CanonicalAuditAdopted { status, .. } => {
                            Some(status.as_str())
                        }
                        _ => None,
                    },
                }))?;
                let changed = tx.execute(
                    "UPDATE hive_runs
                     SET status = 'succeeded',
                         lease_owner = NULL, lease_token = NULL, lease_epoch = NULL,
                         lease_expires_at = NULL, heartbeat_at = NULL,
                         last_stop_reason = ?3, last_error = NULL,
                         outcome_json = ?4, finished_at = COALESCE(finished_at, ?2),
                         updated_at = ?2
                     WHERE id = ?1 AND status IN ('leased', 'running')",
                    params![run_id, now, reason, outcome_json],
                )?;
                anyhow::ensure!(
                    changed == 1,
                    "expired Worker Introduction review changed during audit adoption"
                );
                if let Some(lease_token) = lease_token.as_deref() {
                    let attempt_changed = tx.execute(
                        "UPDATE hive_run_attempts
                         SET finished_at = ?4, outcome = 'succeeded', stop_reason = ?5,
                             error = NULL
                         WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                           AND finished_at IS NULL",
                        params![run_id, attempt_no, lease_token, now, reason],
                    )?;
                    anyhow::ensure!(
                        attempt_changed == 1,
                        "expired Worker Introduction review has no matching open attempt"
                    );
                }
                let _ = materialize_oldest_staged_input_in_transaction(&tx, &run_id, &now)?;
                update_derived_state(&tx, &run_id, HiveRunStatus::Succeeded, &now)?;
                discard_pending_controls(
                    &tx,
                    &run_id,
                    "Worker Introduction review completed before control delivery",
                    &now,
                )?;
                result.recovered_succeeded += 1;
                result
                    .recovered_succeeded_runs
                    .push(ReconciledRun { run_id, attempt_no });
                continue;
            }
            if let WorkerIntroductionReviewRecovery::TerminalFailure { review_id } =
                &review_disposition
            {
                let reason =
                    "terminal Worker Introduction review failure recovered after lease expiry";
                let outcome_json = serde_json::to_string(&serde_json::json!({
                    "kind": "failed",
                    "recovered": "terminal_worker_introduction_review_failure",
                    "review_id": review_id,
                }))?;
                let changed = tx.execute(
                    "UPDATE hive_runs
                     SET status = 'failed', lease_owner = NULL, lease_token = NULL,
                         lease_epoch = NULL, lease_expires_at = NULL,
                         heartbeat_at = NULL, last_stop_reason = ?3,
                         last_error = ?3, outcome_json = ?4,
                         finished_at = COALESCE(finished_at, ?2), updated_at = ?2
                     WHERE id = ?1 AND status IN ('leased', 'running')",
                    params![run_id, now, reason, outcome_json],
                )?;
                anyhow::ensure!(
                    changed == 1,
                    "expired terminal Introduction review changed during recovery"
                );
                if let Some(lease_token) = lease_token.as_deref() {
                    let attempt_changed = tx.execute(
                        "UPDATE hive_run_attempts
                         SET finished_at = ?4, outcome = 'failed',
                             stop_reason = ?5, error = ?5
                         WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                           AND finished_at IS NULL",
                        params![run_id, attempt_no, lease_token, now, reason],
                    )?;
                    anyhow::ensure!(
                        attempt_changed == 1,
                        "expired terminal Introduction review has no open attempt"
                    );
                }
                let _ = materialize_oldest_staged_input_in_transaction(&tx, &run_id, &now)?;
                update_derived_state(&tx, &run_id, HiveRunStatus::Failed, &now)?;
                discard_pending_controls(
                    &tx,
                    &run_id,
                    "Worker Introduction review failed before control delivery",
                    &now,
                )?;
                result.recovered_failed += 1;
                result
                    .recovered_failed_runs
                    .push(ReconciledRun { run_id, attempt_no });
                continue;
            }
            let workflow_provider_disposition = if kind == HiveRunKind::WorkerWorkflow.as_str() {
                match (lease_token.as_deref(), lease_epoch) {
                    (Some(lease_token), Some(lease_epoch)) => {
                        reconcile_worker_workflow_provider_boundary_in_transaction(
                            &tx,
                            &run_id,
                            lease_token,
                            lease_epoch,
                            &now,
                        )?
                    }
                    _ => WorkerWorkflowProviderRecovery::NotWorkerWorkflow,
                }
            } else {
                WorkerWorkflowProviderRecovery::NotWorkerWorkflow
            };
            if workflow_provider_disposition
                == WorkerWorkflowProviderRecovery::CanonicalOutcomeAdopted
            {
                let reason = "committed Worker Goal outcome recovered after executor lease expiry";
                let outcome = committed_worker_goal_outcome_in_transaction(&tx, &run_id)?
                    .context("adopted Worker Goal outcome disappeared during reconciliation")?;
                let outcome_json = serde_json::to_string(&serde_json::json!({
                    "kind": "succeeded",
                    "recovered": "canonical_worker_goal_outcome",
                    "workflow_goal_id": outcome.workflow_goal_id,
                    "workflow_attempt_id": outcome.workflow_attempt_id,
                }))?;
                let changed = tx.execute(
                    "UPDATE hive_runs
                     SET status = 'succeeded',
                         lease_owner = NULL, lease_token = NULL, lease_epoch = NULL,
                         lease_expires_at = NULL, heartbeat_at = NULL,
                         last_stop_reason = ?3, last_error = NULL,
                         outcome_json = ?4, finished_at = COALESCE(finished_at, ?2),
                         updated_at = ?2
                     WHERE id = ?1 AND status IN ('leased', 'running')",
                    params![run_id, now, reason, outcome_json],
                )?;
                anyhow::ensure!(
                    changed == 1,
                    "expired Worker Workflow changed during outcome adoption"
                );
                if let Some(lease_token) = lease_token.as_deref() {
                    let attempt_changed = tx.execute(
                        "UPDATE hive_run_attempts
                         SET finished_at = ?4, outcome = 'succeeded', stop_reason = ?5,
                             error = NULL
                         WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                           AND finished_at IS NULL",
                        params![run_id, attempt_no, lease_token, now, reason],
                    )?;
                    anyhow::ensure!(
                        attempt_changed == 1,
                        "expired Worker Workflow has no matching open attempt"
                    );
                }
                update_derived_state(&tx, &run_id, HiveRunStatus::Succeeded, &now)?;
                discard_pending_controls(
                    &tx,
                    &run_id,
                    "Worker Workflow completed before control delivery",
                    &now,
                )?;
                result.recovered_succeeded += 1;
                result
                    .recovered_succeeded_runs
                    .push(ReconciledRun { run_id, attempt_no });
                continue;
            }
            let committed_introduction_opening = if status == HiveRunStatus::Running.as_str()
                && kind == HiveRunKind::WorkerIntroduction.as_str()
            {
                committed_worker_introduction_opening(&tx, &run_id)?
            } else {
                None
            };
            if let Some(opening_message_id) = committed_introduction_opening {
                if let (Some(lease_token), Some(_daemon_fence)) =
                    (lease_token.as_deref(), daemon_fence)
                {
                    reconcile_committed_introduction_provider_calls_in_transaction(
                        &tx,
                        &run_id,
                        lease_token,
                        lease_epoch.context("expired Introduction has no lease epoch")?,
                        &now,
                    )?;
                }
                let reason =
                    "committed Worker Introduction opening recovered after worker lease expiry";
                let outcome_json = serde_json::to_string(&serde_json::json!({
                    "kind": "succeeded",
                    "recovered": "committed_introduction_opening",
                    "opening_message_id": opening_message_id,
                }))?;
                let changed = tx.execute(
                    "UPDATE hive_runs
                     SET status = 'succeeded',
                         lease_owner = NULL, lease_token = NULL, lease_epoch = NULL,
                         lease_expires_at = NULL, heartbeat_at = NULL,
                         last_stop_reason = ?3, last_error = NULL,
                         outcome_json = ?4, finished_at = COALESCE(finished_at, ?2),
                         updated_at = ?2
                     WHERE id = ?1 AND status = 'running'",
                    params![run_id, now, reason, outcome_json],
                )?;
                anyhow::ensure!(
                    changed == 1,
                    "expired Worker Introduction changed during reconciliation"
                );
                let introduction_changed = tx.execute(
                    "UPDATE hive_worker_introductions
                     SET status = 'awaiting_context', opening_message_id = ?2,
                         last_error = NULL, completed_at = NULL, updated_at = ?3
                     WHERE run_id = ?1
                       AND status IN ('queued', 'running', 'awaiting_context', 'needs_recovery')
                       AND (opening_message_id IS NULL OR opening_message_id = ?2)",
                    params![run_id, opening_message_id, now],
                )?;
                anyhow::ensure!(
                    introduction_changed == 1,
                    "committed Worker Introduction opening has no compatible lifecycle row"
                );
                if let Some(lease_token) = lease_token {
                    let attempt_changed = tx.execute(
                        "UPDATE hive_run_attempts
                         SET finished_at = ?4, outcome = 'succeeded', stop_reason = ?5,
                             error = NULL
                         WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                           AND finished_at IS NULL",
                        params![run_id, attempt_no, lease_token, now, reason],
                    )?;
                    anyhow::ensure!(
                        attempt_changed == 1,
                        "expired Worker Introduction has no matching open attempt"
                    );
                }
                update_derived_state(&tx, &run_id, HiveRunStatus::Succeeded, &now)?;
                discard_pending_controls(
                    &tx,
                    &run_id,
                    "Worker Introduction completed before control delivery",
                    &now,
                )?;
                result.recovered_succeeded += 1;
                result
                    .recovered_succeeded_runs
                    .push(ReconciledRun { run_id, attempt_no });
                continue;
            }

            let stopped_conversation = if status == HiveRunStatus::Running.as_str()
                && kind == HiveRunKind::WorkerConversation.as_str()
            {
                match (lease_token.as_deref(), lease_epoch) {
                    (Some(lease_token), Some(lease_epoch))
                        if exact_stopped_worker_conversation_authority(
                            &tx,
                            &run_id,
                            lease_token,
                            lease_epoch,
                        )? =>
                    {
                        Some(finalize_stopped_worker_conversation_in_transaction(
                            &tx,
                            &run_id,
                            lease_token,
                            lease_epoch,
                            &now,
                        )?)
                    }
                    _ => None,
                }
            } else {
                None
            };
            if stopped_conversation == Some(StoppedWorkerConversationFinalization::Cancelled) {
                let outcome_json = serde_json::to_string(&serde_json::json!({
                    "kind": "cancelled",
                    "recovered": "committed_worker_conversation_stop",
                }))?;
                let lease_token = lease_token
                    .as_deref()
                    .context("stopped Worker conversation has no lease token")?;
                let lease_epoch =
                    lease_epoch.context("stopped Worker conversation has no lease epoch")?;
                let changed = tx.execute(
                    "UPDATE hive_runs
                     SET status = 'cancelled',
                         lease_owner = NULL, lease_token = NULL, lease_epoch = NULL,
                         lease_expires_at = NULL, heartbeat_at = NULL,
                         last_stop_reason = ?5, last_error = NULL,
                         outcome_json = ?6, finished_at = COALESCE(finished_at, ?4),
                         updated_at = ?4
                     WHERE id = ?1 AND status = 'running'
                       AND lease_token = ?2 AND lease_epoch = ?3",
                    params![
                        run_id,
                        lease_token,
                        lease_epoch,
                        now,
                        WORKER_CONVERSATION_STOP_REQUESTED_REASON,
                        outcome_json
                    ],
                )?;
                anyhow::ensure!(
                    changed == 1,
                    "expired stopped Worker conversation changed during finalization"
                );
                let attempt_changed = tx.execute(
                    "UPDATE hive_run_attempts
                     SET finished_at = ?5, outcome = 'cancelled', stop_reason = ?6,
                         error = NULL
                     WHERE run_id = ?1 AND attempt_no = ?2
                       AND lease_token = ?3 AND lease_epoch = ?4
                       AND finished_at IS NULL",
                    params![
                        run_id,
                        attempt_no,
                        lease_token,
                        lease_epoch,
                        now,
                        WORKER_CONVERSATION_STOP_REQUESTED_REASON
                    ],
                )?;
                anyhow::ensure!(
                    attempt_changed == 1,
                    "expired stopped Worker conversation has no matching open attempt"
                );
                let _ = materialize_oldest_staged_input_with_authority_in_transaction(
                    &tx,
                    &run_id,
                    WorkerConversationPredecessorAuthority::StoppedWorkerConversation,
                    &now,
                )?;
                update_derived_state(&tx, &run_id, HiveRunStatus::Cancelled, &now)?;
                discard_pending_controls(
                    &tx,
                    &run_id,
                    "Worker conversation Stop recovered before control delivery",
                    &now,
                )?;
                result.recovered_cancelled += 1;
                result
                    .recovered_cancelled_runs
                    .push(ReconciledRun { run_id, attempt_no });
                continue;
            }

            let worker_response_disposition = if stopped_conversation
                == Some(StoppedWorkerConversationFinalization::CanonicalResponseAdopted)
            {
                ExpiredWorkerResponseDisposition::CanonicalResponseAdopted
            } else if status == HiveRunStatus::Running.as_str()
                && kind != HiveRunKind::WorkerWorkflow.as_str()
                && kind != HiveRunKind::WorkerIntroductionReview.as_str()
                && daemon_fence.is_some()
            {
                if let Some(lease_token) = lease_token.as_deref() {
                    reconcile_expired_worker_response_in_transaction(
                        &tx,
                        &run_id,
                        lease_token,
                        lease_epoch.context("expired Worker run has no lease epoch")?,
                        &now,
                    )?
                } else {
                    ExpiredWorkerResponseDisposition::NotWorkerBound
                }
            } else {
                ExpiredWorkerResponseDisposition::NotWorkerBound
            };
            if worker_response_disposition
                == ExpiredWorkerResponseDisposition::CanonicalResponseAdopted
            {
                let reason = "canonical Worker response recovered after executor lease expiry";
                let response = committed_worker_response_in_transaction(&tx, &run_id)?
                    .context("adopted Worker response disappeared during reconciliation")?;
                let outcome_json = serde_json::to_string(&serde_json::json!({
                    "kind": "succeeded",
                    "recovered": "canonical_worker_response",
                    "response_message_id": response.0,
                    "response_group_message_id": response.1,
                }))?;
                let changed = tx.execute(
                    "UPDATE hive_runs
                     SET status = 'succeeded',
                         lease_owner = NULL, lease_token = NULL, lease_epoch = NULL,
                         lease_expires_at = NULL, heartbeat_at = NULL,
                         last_stop_reason = ?3, last_error = NULL,
                         outcome_json = ?4, finished_at = COALESCE(finished_at, ?2),
                         updated_at = ?2
                     WHERE id = ?1 AND status = 'running'",
                    params![run_id, now, reason, outcome_json],
                )?;
                anyhow::ensure!(
                    changed == 1,
                    "expired Worker response changed during reconciliation"
                );
                if let Some(lease_token) = lease_token.as_deref() {
                    let attempt_changed = tx.execute(
                        "UPDATE hive_run_attempts
                         SET finished_at = ?4, outcome = 'succeeded', stop_reason = ?5,
                             error = NULL
                         WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                           AND finished_at IS NULL",
                        params![run_id, attempt_no, lease_token, now, reason],
                    )?;
                    anyhow::ensure!(
                        attempt_changed == 1,
                        "expired Worker response has no matching open attempt"
                    );
                }
                let _ = materialize_oldest_staged_input_in_transaction(&tx, &run_id, &now)?;
                update_derived_state(&tx, &run_id, HiveRunStatus::Succeeded, &now)?;
                discard_pending_controls(
                    &tx,
                    &run_id,
                    "Worker response completed before control delivery",
                    &now,
                )?;
                result.recovered_succeeded += 1;
                result
                    .recovered_succeeded_runs
                    .push(ReconciledRun { run_id, attempt_no });
                continue;
            }

            let replayable =
                HiveRunKind::parse(&kind).is_some_and(HiveRunKind::replays_after_expired_running);
            // Introduction mutates its lifecycle ledger before provider admission. Without an
            // exact opening commit, even a zero-call running attempt requires an explicit choice.
            let worker_response_safe_before_provider = worker_response_disposition
                == ExpiredWorkerResponseDisposition::SafeBeforeProviderBoundary
                && kind != HiveRunKind::WorkerIntroduction.as_str();
            let safe_before_provider = worker_response_safe_before_provider
                || workflow_provider_disposition
                    == WorkerWorkflowProviderRecovery::SafeBeforeProviderBoundary
                || review_disposition
                    == WorkerIntroductionReviewRecovery::SafeBeforeProviderBoundary;
            let provider_boundary_without_response = worker_response_disposition
                == ExpiredWorkerResponseDisposition::ProviderBoundaryWithoutResponse
                || review_disposition
                    == WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit
                || matches!(
                    workflow_provider_disposition,
                    WorkerWorkflowProviderRecovery::ProviderBoundaryWithoutOutcome
                        | WorkerWorkflowProviderRecovery::CommittedOutcomeUnaccounted
                        | WorkerWorkflowProviderRecovery::NotWorkerWorkflow
                ) && kind == HiveRunKind::WorkerWorkflow.as_str();
            let leased_before_boundary = status == HiveRunStatus::Leased.as_str()
                && (kind != HiveRunKind::WorkerWorkflow.as_str() || safe_before_provider);
            let (target, message) = if leased_before_boundary
                || safe_before_provider
                || (replayable && !provider_boundary_without_response)
            {
                result.requeued_unstarted += 1;
                result.requeued_runs.push(ReconciledRun {
                    run_id: run_id.clone(),
                    attempt_no,
                });
                (
                    HiveRunStatus::Queued,
                    if status == HiveRunStatus::Leased.as_str() {
                        "worker lease expired before execution; requeued"
                    } else if safe_before_provider {
                        "Worker executor lease expired before any provider boundary; requeued"
                    } else {
                        "worker lease expired during replayable run; requeued"
                    },
                )
            } else {
                result.recovery_required += 1;
                result.recovery_required_runs.push(ReconciledRun {
                    run_id: run_id.clone(),
                    attempt_no,
                });
                (
                    HiveRunStatus::RecoveryRequired,
                    "worker lease expired; side effects may be uncertain",
                )
            };
            let message = if status == HiveRunStatus::Running.as_str()
                && kind == HiveRunKind::WorkerIntroduction.as_str()
                && target == HiveRunStatus::RecoveryRequired
            {
                "Worker Introduction lease expired without a committed canonical opening; explicit retry or skip is required"
            } else if status == HiveRunStatus::Running.as_str()
                && kind == HiveRunKind::WorkerWorkflow.as_str()
                && target == HiveRunStatus::RecoveryRequired
            {
                "Worker Workflow lease expired after execution began; workspace side effects are uncertain and explicit recovery is required"
            } else if status == HiveRunStatus::Running.as_str()
                && kind == HiveRunKind::WorkerIntroductionReview.as_str()
                && target == HiveRunStatus::RecoveryRequired
            {
                "Worker Introduction review crossed an ambiguous provider boundary without a committed audit; it was not replayed"
            } else {
                message
            };
            tx.execute(
                "UPDATE hive_runs
                 SET status = ?2,
                     lease_owner = NULL, lease_token = NULL, lease_epoch = NULL,
                     lease_expires_at = NULL, heartbeat_at = NULL,
                     last_error = ?4, updated_at = ?3
                 WHERE id = ?1 AND status = ?5",
                params![run_id, target.to_string(), now, message, status],
            )?;
            if let Some(lease_token) = lease_token {
                let attempt_outcome = if target == HiveRunStatus::RecoveryRequired {
                    HiveRunAttemptOutcome::RecoveryRequired
                } else {
                    HiveRunAttemptOutcome::Abandoned
                };
                tx.execute(
                    "UPDATE hive_run_attempts
                     SET finished_at = ?4, outcome = ?6, error = ?5
                     WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3",
                    params![
                        run_id,
                        attempt_no,
                        lease_token,
                        now,
                        message,
                        attempt_outcome.as_str(),
                    ],
                )?;
            }
            if status == HiveRunStatus::Running.as_str()
                && kind == HiveRunKind::WorkerIntroduction.as_str()
                && target == HiveRunStatus::RecoveryRequired
            {
                tx.execute(
                    "UPDATE hive_worker_introductions
                     SET status = 'needs_recovery', last_error = ?2,
                         completed_at = NULL, updated_at = ?3
                     WHERE run_id = ?1 AND status NOT IN ('confirmed', 'skipped')",
                    params![run_id, message, now],
                )?;
            }
            if kind == HiveRunKind::WorkerWorkflow.as_str()
                && target == HiveRunStatus::RecoveryRequired
            {
                let _ =
                    pause_worker_workflow_after_uncertain_run_in_transaction(&tx, &run_id, &now)?;
            }
            update_derived_state(&tx, &run_id, target, &now)?;
            if target == HiveRunStatus::RecoveryRequired {
                discard_pending_controls(
                    &tx,
                    &run_id,
                    "run entered recovery before control delivery",
                    &now,
                )?;
            }
        }
        tx.commit()?;
        Ok(result)
    }

    pub fn promote_due_runs(&self, now: DateTime<Utc>) -> Result<usize> {
        Ok(self.promote_due_runs_inner(now, None)?.len())
    }

    pub fn promote_due_runs_fenced(
        &self,
        now: DateTime<Utc>,
        daemon_fence: &DaemonFence,
    ) -> Result<Vec<ReconciledRun>> {
        self.promote_due_runs_inner(now, Some(daemon_fence))
    }

    fn promote_due_runs_inner(
        &self,
        now: DateTime<Utc>,
        daemon_fence: Option<&DaemonFence>,
    ) -> Result<Vec<ReconciledRun>> {
        let now = canonical_timestamp(now);
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        if let Some(daemon_fence) = daemon_fence {
            if !daemon_fence_is_current(&tx, daemon_fence, &now)? {
                tx.commit()?;
                return Ok(Vec::new());
            }
        }
        let run_ids = {
            let mut statement = tx.prepare(
                "SELECT id, attempt_count FROM hive_runs
                 WHERE (status = 'sleeping' AND wake_at IS NOT NULL AND wake_at <= ?1)
                    OR (status = 'retry_wait' AND available_at <= ?1)
                 ORDER BY id",
            )?;
            let run_ids = statement
                .query_map([&now], |row| {
                    Ok(ReconciledRun {
                        run_id: row.get(0)?,
                        attempt_no: nonnegative_i64(row, 1)? as u32,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            run_ids
        };
        tx.execute(
            "UPDATE hive_runs
             SET status = 'queued', wake_at = NULL, updated_at = ?1
             WHERE (status = 'sleeping' AND wake_at IS NOT NULL AND wake_at <= ?1)
                OR (status = 'retry_wait' AND available_at <= ?1)",
            [&now],
        )?;
        for run in &run_ids {
            update_derived_state(&tx, &run.run_id, HiveRunStatus::Queued, &now)?;
        }
        tx.commit()?;
        Ok(run_ids)
    }

    /// Cancels queued, delayed, or active work and fences any worker that still holds its lease.
    pub fn cancel(&self, run_id: &str, now: DateTime<Utc>, reason: &str) -> Result<bool> {
        anyhow::ensure!(!reason.trim().is_empty(), "cancellation reason is empty");
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let current = tx
            .query_row(
                "SELECT status, attempt_count, lease_token
                 FROM hive_runs WHERE id = ?1",
                [run_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        nonnegative_i64(row, 1)? as u32,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((current, attempt_no, lease_token)) = current else {
            tx.commit()?;
            return Ok(false);
        };
        let current = HiveRunStatus::parse(&current)
            .ok_or_else(|| anyhow::anyhow!("invalid persisted Hive run status"))?;
        current.ensure_transition_to(HiveRunStatus::Cancelled)?;
        if current == HiveRunStatus::Cancelled {
            tx.commit()?;
            return Ok(true);
        }

        let now = canonical_timestamp(now);
        let changed = tx.execute(
            "UPDATE hive_runs
             SET status = 'cancelled', lease_owner = NULL, lease_token = NULL,
                 lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                 wake_at = NULL, last_stop_reason = ?3, last_error = NULL,
                 finished_at = ?2, updated_at = ?2
             WHERE id = ?1 AND status = ?4",
            params![run_id, now, reason, current.to_string()],
        )?;
        if changed == 1 {
            if let Some(lease_token) = lease_token {
                tx.execute(
                    "UPDATE hive_run_attempts
                     SET finished_at = ?4, outcome = 'cancelled', stop_reason = ?5
                     WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                       AND finished_at IS NULL",
                    params![run_id, attempt_no, lease_token, now, reason],
                )?;
            }
            update_derived_state(&tx, run_id, HiveRunStatus::Cancelled, &now)?;
            discard_pending_controls(&tx, run_id, "run cancelled", &now)?;
        }
        tx.commit()?;
        Ok(changed == 1)
    }

    pub fn requeue(&self, run_id: &str, now: DateTime<Utc>) -> Result<bool> {
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let current = tx
            .query_row(
                "SELECT status FROM hive_runs WHERE id = ?1",
                [run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(current) = current else {
            tx.commit()?;
            return Ok(false);
        };
        let current = HiveRunStatus::parse(&current)
            .ok_or_else(|| anyhow::anyhow!("invalid persisted Hive run status"))?;
        current.ensure_transition_to(HiveRunStatus::Queued)?;
        let now = canonical_timestamp(now);
        let changed = tx.execute(
            "UPDATE hive_runs
             SET status = 'queued', available_at = ?2, wake_at = NULL,
                 finished_at = NULL, last_stop_reason = NULL, last_error = NULL,
                 outcome_json = NULL, updated_at = ?2
             WHERE id = ?1 AND status = ?3",
            params![run_id, now, current.to_string()],
        )?;
        if changed == 1 {
            update_derived_state(&tx, run_id, HiveRunStatus::Queued, &now)?;
        }
        tx.commit()?;
        Ok(changed == 1)
    }

    pub fn list_attempts(&self, run_id: &str) -> Result<Vec<HiveRunAttempt>> {
        let sql = format!(
            "SELECT {ATTEMPT_COLUMNS} FROM hive_run_attempts
             WHERE run_id = ?1 ORDER BY attempt_no ASC"
        );
        let mut statement = self.db.conn().prepare(&sql)?;
        let attempts = statement
            .query_map([run_id], map_attempt)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("listing Hive run attempts")?;
        Ok(attempts)
    }
}

/// Return the exact first assistant row committed by the one-time
/// Introduction writer. Merely finding assistant text is intentionally
/// insufficient: recovery adopts only the deterministic key on the Worker's
/// private DM, with a lifecycle row that either has no opening yet or already
/// points at this same row.
fn committed_worker_introduction_opening(
    tx: &Transaction<'_>,
    run_id: &str,
) -> Result<Option<i64>> {
    let candidate = tx
        .query_row(
            "SELECT message.id, message.content
             FROM hive_runs run
             JOIN hive_worker_introductions introduction
               ON introduction.run_id = run.id
              AND introduction.worker_id = run.worker_id
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN messages message ON message.session_id = run.session_id
             WHERE run.id = ?1 AND run.kind = 'worker_introduction'
               AND run.session_id = worker.dm_session_id
               AND message.role = 'assistant'
               AND message.idempotency_key = 'introduction:' || run.id || ':opening'
               AND (introduction.opening_message_id IS NULL
                    OR introduction.opening_message_id = message.id)
               AND NOT EXISTS (
                   SELECT 1 FROM messages earlier
                   WHERE earlier.session_id = message.session_id
                     AND earlier.id < message.id
               )",
            [run_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((message_id, content_json)) = candidate else {
        return Ok(None);
    };
    let contents = serde_json::from_str::<Vec<Content>>(&content_json).ok();
    let canonical = matches!(
        contents.as_deref(),
        Some([Content::Text { text, .. }]) if !text.trim().is_empty()
    );
    Ok(canonical.then_some(message_id))
}

/// Keep client-facing occurrence/runtime projections in the same transaction
/// as the canonical run state. These tables are derived, but allowing them to
/// commit later would make crash recovery and takeover observations disagree.
fn update_derived_state(
    tx: &Transaction<'_>,
    run_id: &str,
    status: HiveRunStatus,
    now: &str,
) -> Result<()> {
    if status == HiveRunStatus::Succeeded {
        let _ = record_trusted_worker_idle_outcome_in_transaction(tx, run_id)?;
    }
    let (controller_id, session_id, occurrence_id) = tx.query_row(
        "SELECT controller_id, session_id, occurrence_id FROM hive_runs WHERE id = ?1",
        [run_id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        },
    )?;

    if status == HiveRunStatus::RecoveryRequired {
        tx.execute(
            "UPDATE hive_controllers SET status = 'paused', updated_at = ?2 WHERE id = ?1",
            params![controller_id, now],
        )?;
    }

    if let Some(occurrence_id) = occurrence_id {
        let occurrence_status = match status {
            HiveRunStatus::Queued | HiveRunStatus::Leased => "queued",
            HiveRunStatus::Succeeded => "succeeded",
            HiveRunStatus::Cancelled => "cancelled",
            HiveRunStatus::Failed | HiveRunStatus::DeadLetter | HiveRunStatus::RecoveryRequired => {
                "failed"
            }
            HiveRunStatus::Running
            | HiveRunStatus::Sleeping
            | HiveRunStatus::RetryWait
            | HiveRunStatus::AwaitingInput => "running",
        };
        tx.execute(
            "UPDATE hive_schedule_occurrences
             SET status = ?2, updated_at = ?3 WHERE id = ?1",
            params![occurrence_id, occurrence_status, now],
        )?;
    }

    if let Some(session_id) = session_id {
        let recovery_run_id = tx
            .query_row(
                "SELECT id FROM hive_runs
                 WHERE controller_id = ?1 AND status = 'recovery_required'
                 ORDER BY updated_at DESC, id ASC LIMIT 1",
                [&controller_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let controller_status = tx.query_row(
            "SELECT status FROM hive_controllers WHERE id = ?1",
            [&controller_id],
            |row| row.get::<_, String>(0),
        )?;
        let active = tx
            .query_row(
                "SELECT id, status FROM hive_runs
                 WHERE controller_id = ?1
                   AND status IN ('queued', 'leased', 'running', 'sleeping', 'retry_wait', 'awaiting_input', 'recovery_required')
                 ORDER BY CASE status
                     WHEN 'running' THEN 0 WHEN 'leased' THEN 1
                     WHEN 'recovery_required' THEN 2 WHEN 'awaiting_input' THEN 3
                     WHEN 'sleeping' THEN 4 WHEN 'queued' THEN 5 ELSE 6 END,
                     updated_at DESC, id ASC LIMIT 1",
                [&controller_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let (runtime_status, current_run_id) = if let Some(recovery_run_id) = recovery_run_id {
            ("error", recovery_run_id)
        } else if controller_status == "paused" {
            (
                "paused",
                active
                    .map(|(active_run_id, _)| active_run_id)
                    .unwrap_or_else(|| run_id.to_string()),
            )
        } else if let Some((active_run_id, active_status)) = active {
            let runtime_status = match active_status.as_str() {
                "running" | "leased" => "running",
                "recovery_required" => "error",
                "awaiting_input" => "awaiting_input",
                "sleeping" => "sleeping",
                _ => "idle",
            };
            (runtime_status, active_run_id)
        } else {
            let runtime_status = match status {
                HiveRunStatus::Cancelled => "cancelled",
                HiveRunStatus::Failed
                | HiveRunStatus::DeadLetter
                | HiveRunStatus::RecoveryRequired => "error",
                _ => "idle",
            };
            (runtime_status, run_id.to_string())
        };
        tx.execute(
            "INSERT INTO hive_runtime_state (session_id, status, current_run_id, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(session_id) DO UPDATE SET status = excluded.status,
                 current_run_id = excluded.current_run_id,
                 updated_at = excluded.updated_at",
            params![session_id, runtime_status, current_run_id, now],
        )?;
    }
    Ok(())
}

pub(crate) fn update_derived_state_for_run_in_transaction(
    tx: &Transaction<'_>,
    run_id: &str,
    status: HiveRunStatus,
    now: &str,
) -> Result<()> {
    update_derived_state(tx, run_id, status, now)
}

/// Validate the exact cancelled direct-message recovery boundary and reactivate
/// its controller before the successor run is inserted. Callers must finish the
/// projection in the same SQLite transaction so an error rolls the reactivation
/// back together with every recovery mutation.
pub(crate) fn reactivate_worker_conversation_controller_after_governor_recovery_in_transaction(
    tx: &Transaction<'_>,
    recovery_run_id: &str,
    now: &str,
) -> Result<()> {
    let (controller_id, controller_status): (String, String) = tx.query_row(
        "SELECT run.controller_id, controller.status
         FROM hive_runs run
         JOIN hive_controllers controller ON controller.id = run.controller_id
         WHERE run.id = ?1 AND run.kind = 'worker_conversation'
           AND run.status = 'cancelled'",
        [recovery_run_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    anyhow::ensure!(
        controller_status == "paused",
        "Worker conversation recovery controller is not paused"
    );
    let other_recovery: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM hive_runs
             WHERE controller_id = ?1 AND status = 'recovery_required'
               AND id <> ?2
         )",
        params![controller_id, recovery_run_id],
        |row| row.get(0),
    )?;
    anyhow::ensure!(
        !other_recovery,
        "Worker conversation controller has another recovery boundary"
    );
    let changed = tx.execute(
        "UPDATE hive_controllers
         SET status = 'active', updated_at = ?2
         WHERE id = ?1 AND status = 'paused'",
        params![controller_id, now],
    )?;
    anyhow::ensure!(
        changed == 1,
        "Worker conversation recovery controller changed during reactivation"
    );
    Ok(())
}

/// Finalize the runtime projections after the queued recovery successor (if
/// any) has been inserted under the now-active controller. This is deliberately
/// separate from controller reactivation so the insert-time authority trigger
/// observes the same active controller contract as an ordinary queued DM.
pub(crate) fn finalize_worker_conversation_after_governor_recovery_in_transaction(
    tx: &Transaction<'_>,
    recovery_run_id: &str,
    materialized_run_id: Option<&str>,
    now: &str,
) -> Result<()> {
    let (session_id, controller_status): (String, String) = tx.query_row(
        "SELECT run.session_id, controller.status
         FROM hive_runs run
         JOIN hive_controllers controller ON controller.id = run.controller_id
         WHERE run.id = ?1 AND run.kind = 'worker_conversation'
           AND run.status = 'cancelled'",
        [recovery_run_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    anyhow::ensure!(
        controller_status == "active",
        "Worker conversation recovery controller is not active"
    );

    update_derived_state(tx, recovery_run_id, HiveRunStatus::Cancelled, now)?;
    let runtime_changed = tx.execute(
        "UPDATE hive_runtime_state
         SET status = 'idle', current_run_id = ?2,
             next_wake_at = NULL, sleep_reason = NULL, last_error = NULL,
             updated_at = ?3
         WHERE session_id = ?1",
        params![session_id, materialized_run_id, now],
    )?;
    anyhow::ensure!(
        runtime_changed == 1,
        "Worker conversation recovery runtime projection disappeared"
    );
    Ok(())
}

fn exact_stopped_worker_conversation_authority(
    tx: &Transaction<'_>,
    run_id: &str,
    lease_token: &str,
    lease_epoch: u64,
) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM hive_runs run
             JOIN hive_controllers controller ON controller.id = run.controller_id
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN sessions session ON session.id = run.session_id
             WHERE run.id = ?1 AND run.status = 'running'
               AND run.lease_token = ?2 AND run.lease_epoch = ?3
               AND run.kind = 'worker_conversation'
               AND run.schedule_id IS NULL AND run.group_id IS NULL
               AND run.governor_origin = 'user_dm'
               AND run.governor_lane_key = 'dm'
               AND run.response_message_id IS NULL
               AND run.response_group_message_id IS NULL
               AND run.response_provider_call_id IS NULL
               AND run.last_stop_reason = ?4
               AND controller.status = 'active'
               AND controller.worker_id = worker.id
               AND controller.session_id = run.session_id
               AND controller.user_id IS worker.user_id
               AND worker.status = 'active'
               AND worker.dm_session_id = run.session_id
               AND session.user_id IS worker.user_id
               AND json_valid(run.execution_context_json)
               AND json_extract(run.execution_context_json, '$.mode.kind')
                   IN ('worker_conversation_neutral', 'worker_workspace_attached')
               AND json_extract(run.execution_context_json, '$.mode.lane.kind')
                   = 'direct_message'
               AND json_extract(run.execution_context_json, '$.mode.worker_id')
                   = run.worker_id
               AND json_extract(run.execution_context_json, '$.mode.worker_revision')
                   = worker.revision
               AND (
                   json_extract(run.execution_context_json, '$.mode.kind')
                       = 'worker_conversation_neutral'
                   OR (
                       session.workspace_mode = json_extract(
                           run.execution_context_json, '$.mode.workspace_mode'
                       )
                       AND session.working_dir = json_extract(
                           run.execution_context_json, '$.mode.working_dir'
                       )
                       AND session.project_dir IS json_extract(
                           run.execution_context_json, '$.mode.project_dir'
                       )
                   )
               )
         )",
        params![
            run_id,
            lease_token,
            lease_epoch,
            WORKER_CONVERSATION_STOP_REQUESTED_REASON
        ],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn discard_pending_controls(
    tx: &Transaction<'_>,
    run_id: &str,
    reason: &str,
    now: &str,
) -> Result<()> {
    tx.execute(
        "UPDATE hive_control_outbox
         SET status = 'discarded', last_error = ?2, updated_at = ?3
         WHERE run_id = ?1 AND status = 'pending'",
        params![run_id, reason, now],
    )?;
    Ok(())
}

/// Scheduled and controller-child objectives are not necessarily represented
/// by an initiating chat message. Materialize them exactly once at the durable
/// running boundary so the hosted agent sees the actual objective in canonical
/// history before it begins, including after a process restart or retry.
fn materialize_objective_message(tx: &Transaction<'_>, run_id: &str, now: &str) -> Result<()> {
    let objective = tx
        .query_row(
            "SELECT session_id, kind, objective FROM hive_runs
             WHERE id = ?1 AND objective_message_id IS NULL
               AND kind IN ('scheduled', 'controller_child')",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((Some(session_id), kind, objective)) = objective else {
        return Ok(());
    };
    let objective = objective.trim();
    if objective.is_empty() {
        return Ok(());
    }
    let rendered = format!("Hive {kind} objective:\n{objective}");
    let content = serde_json::to_string(&vec![Content::text(rendered.clone())])?;
    tx.execute(
        "INSERT INTO messages (session_id, role, content, created_at)
         VALUES (?1, 'user', ?2, ?3)",
        params![session_id, content, now],
    )?;
    let message_id = tx.last_insert_rowid();
    let body = rendered.split_whitespace().collect::<Vec<_>>().join(" ");
    let body = truncate_utf8_bytes(&body, 16 * 1024).to_string();
    let mut hash_material = Vec::with_capacity("user".len() + 1 + body.len());
    hash_material.extend_from_slice(b"user");
    hash_material.push(0);
    hash_material.extend_from_slice(body.as_bytes());
    tx.execute(
        "INSERT INTO conversation_episodes (
            session_id, source_message_id, role, body, content_hash, occurred_at
         ) VALUES (?1, ?2, 'user', ?3, ?4, ?5)",
        params![
            session_id,
            message_id,
            body,
            hash_request_bytes(hash_material),
            now
        ],
    )?;
    tx.execute(
        "UPDATE hive_runs SET objective_message_id = ?2 WHERE id = ?1",
        params![run_id, message_id],
    )?;
    tx.execute(
        "UPDATE sessions SET updated_at = ?2 WHERE id = ?1",
        params![session_id, now],
    )?;
    Ok(())
}

fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

fn attempt_outcome(status: HiveRunStatus) -> HiveRunAttemptOutcome {
    match status {
        HiveRunStatus::Succeeded => HiveRunAttemptOutcome::Succeeded,
        HiveRunStatus::Failed => HiveRunAttemptOutcome::Failed,
        HiveRunStatus::RetryWait => HiveRunAttemptOutcome::RetryScheduled,
        HiveRunStatus::Sleeping => HiveRunAttemptOutcome::Sleeping,
        HiveRunStatus::AwaitingInput => HiveRunAttemptOutcome::AwaitingInput,
        HiveRunStatus::RecoveryRequired => HiveRunAttemptOutcome::RecoveryRequired,
        HiveRunStatus::Cancelled => HiveRunAttemptOutcome::Cancelled,
        HiveRunStatus::DeadLetter => HiveRunAttemptOutcome::DeadLetter,
        HiveRunStatus::Queued | HiveRunStatus::Leased | HiveRunStatus::Running => {
            HiveRunAttemptOutcome::Abandoned
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerIntroductionReviewRecovery {
    CanonicalAuditAdopted { review_id: String, status: String },
    PreProviderStale { review_id: String, reason: String },
    TerminalFailure { review_id: String },
    SafeBeforeProviderBoundary,
    ProviderBoundaryWithoutAudit,
    NotWorkerIntroductionReview,
}

/// Reconcile the one provider slot owned by a review run. The audit commit is
/// deliberately authoritative before permit completion: if it exists, an
/// exact unresolved Started row can be terminalized and adopted. A Started
/// row without that audit result is ambiguous and is never replayed.
pub fn reconcile_worker_introduction_review_in_transaction(
    tx: &Transaction<'_>,
    run_id: &str,
    lease_token: &str,
    lease_epoch: u64,
    now: &str,
) -> Result<WorkerIntroductionReviewRecovery> {
    let kind: Option<String> = tx
        .query_row(
            "SELECT kind FROM hive_runs WHERE id = ?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?;
    if kind.as_deref() != Some(HiveRunKind::WorkerIntroductionReview.as_str()) {
        return Ok(WorkerIntroductionReviewRecovery::NotWorkerIntroductionReview);
    }
    let review = tx
        .query_row(
            "SELECT id, status, provider_call_id, usage_json, last_error
             FROM hive_worker_introduction_reviews WHERE run_id = ?1",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((review_id, status, review_call_id, usage_json, last_error)) = review else {
        return Ok(WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit);
    };
    let mut statement = tx.prepare(
        "SELECT call.provider_call_id, call.call_kind,
                outcome.state, outcome.outcome, outcome.remote_acceptance
         FROM hive_worker_provider_calls call
         LEFT JOIN hive_worker_provider_call_outcomes outcome
           ON outcome.provider_call_id = call.provider_call_id
         WHERE call.run_id = ?1 AND call.run_lease_token = ?2
           AND call.run_lease_epoch = ?3
         ORDER BY call.started_at, call.rowid",
    )?;
    let calls = statement
        .query_map(params![run_id, lease_token, lease_epoch], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    anyhow::ensure!(
        calls.len() <= 1,
        "Introduction review attempt has multiple provider Started rows"
    );
    if let Some(call) = calls.first() {
        anyhow::ensure!(
            call.1 == "worker_introduction_review",
            "Introduction review attempt crossed another provider-call kind"
        );
    }

    let committed = matches!(
        status.as_str(),
        "gather_more" | "review_ready" | "confirmed" | "rejected" | "keep_talking" | "stale"
    );
    if committed {
        if status == "stale" && review_call_id.is_none() && calls.is_empty() {
            let reason = last_error
                .as_deref()
                .filter(|reason| !reason.trim().is_empty())
                .context("pre-provider stale Introduction review has no audit reason")?
                .to_string();
            return Ok(WorkerIntroductionReviewRecovery::PreProviderStale { review_id, reason });
        }
        let Some(review_call_id) = review_call_id.as_deref() else {
            return Ok(WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit);
        };
        let Some(call) = calls.first().filter(|call| call.0 == review_call_id) else {
            return Ok(WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit);
        };
        if call.2.as_deref() == Some("unknown") {
            return Ok(WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit);
        }
        if call.2.is_none() {
            let terminal_outcome = if status == "stale" {
                "canonical_commit_stale"
            } else {
                "completed"
            };
            tx.execute(
                "INSERT INTO hive_worker_provider_call_outcomes (
                     provider_call_id, state, outcome, remote_acceptance,
                     usage_json, usage_total_tokens, estimated_cost_microunits,
                     unknown_reason, finished_at
                 ) VALUES (
                     ?1, 'completed', ?2, 'acknowledged', ?3,
                     CASE WHEN ?3 IS NULL THEN NULL
                          ELSE json_extract(?3, '$.total_tokens') END,
                     NULL, NULL, ?4
                 )",
                params![review_call_id, terminal_outcome, usage_json, now],
            )?;
        } else {
            anyhow::ensure!(
                call.2.as_deref() == Some("completed")
                    && call.4.as_deref() == Some("acknowledged")
                    && matches!(
                        call.3.as_deref(),
                        Some("completed" | "semantic_invalid" | "canonical_commit_stale")
                    ),
                "committed Introduction review has incompatible provider accounting"
            );
        }
        return Ok(WorkerIntroductionReviewRecovery::CanonicalAuditAdopted { review_id, status });
    }

    if status == "failed" {
        if calls.is_empty() && review_call_id.is_none() {
            return Ok(WorkerIntroductionReviewRecovery::TerminalFailure { review_id });
        }
        let exact_call = calls
            .first()
            .filter(|call| review_call_id.as_deref() == Some(call.0.as_str()));
        if let Some(call) = exact_call {
            let semantic_invalid_audit = call.2.is_none()
                && last_error
                    .as_deref()
                    .is_some_and(|error| error.starts_with("invalid reviewer output:"));
            if semantic_invalid_audit {
                tx.execute(
                    "INSERT INTO hive_worker_provider_call_outcomes (
                         provider_call_id, state, outcome, remote_acceptance,
                         usage_json, usage_total_tokens, estimated_cost_microunits,
                         unknown_reason, finished_at
                     ) VALUES (
                         ?1, 'completed', 'semantic_invalid', 'acknowledged', ?2,
                         CASE WHEN ?2 IS NULL THEN NULL
                              ELSE json_extract(?2, '$.total_tokens') END,
                         NULL, NULL, ?3
                     )",
                    params![call.0, usage_json, now],
                )?;
                return Ok(WorkerIntroductionReviewRecovery::TerminalFailure { review_id });
            }
            if call.2.as_deref() == Some("completed")
                && matches!(call.4.as_deref(), Some("acknowledged" | "not_sent"))
            {
                return Ok(WorkerIntroductionReviewRecovery::TerminalFailure { review_id });
            }
        }
        return Ok(WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit);
    }

    if calls.is_empty() {
        if status == "claimed" {
            tx.execute(
                "UPDATE hive_worker_introduction_reviews
                 SET status = 'queued', claim_token = 'queued:' || run_id,
                     claim_expires_at = ?2, claimed_at = ?2,
                     last_error = NULL, completed_at = NULL, updated_at = ?2
                 WHERE id = ?1 AND status = 'claimed' AND provider_call_id IS NULL",
                params![review_id, now],
            )?;
        }
        return Ok(WorkerIntroductionReviewRecovery::SafeBeforeProviderBoundary);
    }

    let call = &calls[0];
    if call.2.is_none() {
        tx.execute(
            "INSERT INTO hive_worker_provider_call_outcomes (
                 provider_call_id, state, outcome, remote_acceptance,
                 usage_json, usage_total_tokens, estimated_cost_microunits,
                 unknown_reason, finished_at
             ) VALUES (
                 ?1, 'unknown', 'response_missing', 'possibly_sent',
                 NULL, NULL, NULL,
                 'Introduction review Started without a committed audit result', ?2
             )",
            params![call.0, now],
        )?;
    }
    Ok(WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit)
}

fn map_run(row: &Row<'_>) -> rusqlite::Result<HiveRun> {
    let id = row.get::<_, String>(0)?;
    let kind = parse_required(5, row.get::<_, String>(5)?, HiveRunKind::parse)?;
    let config_json = row.get::<_, String>(7)?;
    let config = serde_json::from_str(&config_json)
        .map_err(|error| conversion_error(7, format!("invalid run config JSON: {error}")))?;
    let status = parse_required(8, row.get::<_, String>(8)?, HiveRunStatus::parse)?;
    let outcome_json = row.get::<_, Option<String>>(23)?;
    let outcome = outcome_json
        .map(|json| {
            serde_json::from_str(&json)
                .map_err(|error| conversion_error(23, format!("invalid outcome JSON: {error}")))
        })
        .transpose()?;
    let governor_origin = row
        .get::<_, Option<String>>(30)?
        .map(|value| {
            WorkerRunOrigin::parse(&value)
                .ok_or_else(|| conversion_error(30, format!("invalid governor origin: {value}")))
        })
        .transpose()?;
    let governor_lane_key = row.get::<_, Option<String>>(31)?;
    let governor_gate_reason = row
        .get::<_, Option<String>>(32)?
        .map(|value| {
            WorkerGovernorGateReason::parse(&value).ok_or_else(|| {
                conversion_error(32, format!("invalid governor gate reason: {value}"))
            })
        })
        .transpose()?;
    let governor_next_eligible_at = row.get::<_, Option<String>>(33)?;
    let governor_policy_revision = optional_nonnegative_i64(row, 34)?.map(|value| value as u64);
    let governor_override_id = row.get::<_, Option<String>>(35)?;
    let governor = (governor_origin.is_some()
        || governor_lane_key.is_some()
        || governor_gate_reason.is_some()
        || governor_next_eligible_at.is_some()
        || governor_policy_revision.is_some()
        || governor_override_id.is_some())
    .then(|| WorkerRunGovernorProjection {
        run_id: id.clone(),
        origin: governor_origin,
        lane_key: governor_lane_key,
        gate_reason: governor_gate_reason,
        next_eligible_at: governor_next_eligible_at,
        policy_revision: governor_policy_revision,
        override_grant_id: governor_override_id,
    });
    let execution_context = row
        .get::<_, Option<String>>(36)?
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                conversion_error(36, format!("invalid execution context JSON: {error}"))
            })
        })
        .transpose()?;
    Ok(HiveRun {
        id,
        controller_id: row.get(1)?,
        session_id: row.get(2)?,
        schedule_id: row.get(3)?,
        occurrence_id: row.get(4)?,
        worker_id: row.get(28)?,
        objective_message_id: row.get(29)?,
        kind,
        objective: row.get(6)?,
        config,
        execution_context,
        conversation_through_message_id: row.get(37)?,
        response_message_id: row.get(38)?,
        response_group_message_id: row.get(39)?,
        response_provider_call_id: row.get(40)?,
        workflow_goal_id: row.get(41)?,
        workflow_attempt_id: row.get(42)?,
        governor,
        status,
        priority: row.get(9)?,
        concurrency_key: row.get(10)?,
        scheduled_for: row.get(11)?,
        available_at: row.get(12)?,
        wake_at: row.get(13)?,
        attempt_count: nonnegative_i64(row, 14)? as u32,
        max_attempts: nonnegative_i64(row, 15)? as u32,
        lease_owner: row.get(16)?,
        lease_token: row.get(17)?,
        lease_epoch: optional_nonnegative_i64(row, 18)?.map(|value| value as u64),
        lease_expires_at: row.get(19)?,
        heartbeat_at: row.get(20)?,
        last_stop_reason: row.get(21)?,
        last_error: row.get(22)?,
        outcome,
        created_at: row.get(24)?,
        started_at: row.get(25)?,
        finished_at: row.get(26)?,
        updated_at: row.get(27)?,
    })
}

fn validate_new_run_authority(run: &HiveRun) -> Result<()> {
    match (run.worker_id.as_deref(), run.execution_context.as_ref()) {
        (Some(worker_id), Some(context)) => {
            context.validate()?;
            anyhow::ensure!(
                context.worker_id() == worker_id,
                "run Worker does not match its execution context"
            );
            let governor = run
                .governor
                .as_ref()
                .context("Worker-bound run has no governor projection")?;
            let expected_lane_key = context.lane().canonical_lane_key()?;
            anyhow::ensure!(
                governor.run_id == run.id,
                "run governor projection belongs to another run"
            );
            anyhow::ensure!(
                governor.lane_key.as_deref() == Some(expected_lane_key.as_str()),
                "run governor lane does not match its execution context"
            );
            anyhow::ensure!(
                governor.origin.is_some(),
                "Worker-bound run has no governor origin"
            );
        }
        (Some(_), None) => anyhow::bail!("Worker-bound run has no execution context"),
        (None, Some(_)) => anyhow::bail!("non-Worker run has a Worker execution context"),
        (None, None) => anyhow::ensure!(
            run.governor.is_none(),
            "non-Worker run has a Worker governor projection"
        ),
    }

    if run.kind == HiveRunKind::WorkerConversation {
        anyhow::ensure!(
            run.session_id.is_some(),
            "Worker conversation has no session"
        );
        anyhow::ensure!(run.worker_id.is_some(), "Worker conversation has no Worker");
        anyhow::ensure!(
            run.objective_message_id.is_some()
                && run.objective_message_id == run.conversation_through_message_id,
            "Worker conversation has no exact initiating message boundary"
        );
        anyhow::ensure!(
            matches!(
                run.execution_context.as_ref().map(|context| context.lane()),
                Some(WorkerConversationLane::DirectMessage)
            ),
            "Worker conversation must use the direct-message lane"
        );
        anyhow::ensure!(
            run.governor
                .as_ref()
                .and_then(|projection| projection.origin)
                == Some(WorkerRunOrigin::UserDm),
            "Worker conversation must use the user-DM governor origin"
        );
        anyhow::ensure!(
            run.response_group_message_id.is_none(),
            "direct Worker conversation has a group response"
        );
    }
    if run.kind == HiveRunKind::WorkerIntroductionReview {
        anyhow::ensure!(
            run.session_id.is_some() && run.worker_id.is_some(),
            "Worker Introduction review has no private Worker session"
        );
        anyhow::ensure!(
            run.objective_message_id.is_none()
                && run.conversation_through_message_id.is_some()
                && run.response_message_id.is_none()
                && run.response_group_message_id.is_none()
                && run.response_provider_call_id.is_none(),
            "Worker Introduction review carries conversational output linkage"
        );
        anyhow::ensure!(
            matches!(
                run.execution_context.as_ref().map(|context| context.lane()),
                Some(WorkerConversationLane::DirectMessage)
            ),
            "Worker Introduction review must use the direct-message lane"
        );
        anyhow::ensure!(
            run.governor
                .as_ref()
                .and_then(|projection| projection.origin)
                == Some(WorkerRunOrigin::UserLifecycleAction),
            "Worker Introduction review must use the lifecycle governor origin"
        );
    }
    if run.kind == HiveRunKind::WorkerWorkflowAcceptance {
        anyhow::ensure!(
            run.session_id.is_some()
                && run.worker_id.is_some()
                && run.workflow_goal_id.is_some()
                && run.workflow_attempt_id.is_some(),
            "Worker Workflow acceptance has no exact source linkage"
        );
        anyhow::ensure!(
            run.objective_message_id.is_none()
                && run.conversation_through_message_id.is_none()
                && run.response_message_id.is_none()
                && run.response_group_message_id.is_none()
                && run.response_provider_call_id.is_none(),
            "Worker Workflow acceptance carries conversational output linkage"
        );
        anyhow::ensure!(
            matches!(
                run.execution_context
                    .as_ref()
                    .map(|context| &context.mode),
                Some(HiveRunExecutionModeV1::WorkerGoalAcceptance {
                    tool_allowlist,
                    ..
                }) if tool_allowlist.is_empty()
            ),
            "Worker Workflow acceptance has executable authority"
        );
        anyhow::ensure!(
            run.governor
                .as_ref()
                .and_then(|projection| projection.origin)
                == Some(WorkerRunOrigin::WorkflowAcceptance),
            "Worker Workflow acceptance has an invalid governor origin"
        );
    }
    if run.kind == HiveRunKind::WorkerWorkflow {
        anyhow::ensure!(
            run.workflow_goal_id.is_some() && run.workflow_attempt_id.is_some(),
            "Worker Workflow has no exact Goal/attempt linkage"
        );
        anyhow::ensure!(
            run.objective_message_id.is_none()
                && run.conversation_through_message_id.is_none()
                && run.response_message_id.is_none()
                && run.response_group_message_id.is_none()
                && run.response_provider_call_id.is_none(),
            "Worker Workflow cannot use conversational message linkage"
        );
        anyhow::ensure!(
            run.governor
                .as_ref()
                .and_then(|projection| projection.origin)
                .is_some_and(|origin| {
                    matches!(
                        origin,
                        WorkerRunOrigin::UserWorkflowActivation | WorkerRunOrigin::WorkflowRollover
                    )
                }),
            "Worker Workflow has an invalid governor origin"
        );
    } else if run.kind != HiveRunKind::WorkerWorkflowAcceptance {
        anyhow::ensure!(
            run.workflow_goal_id.is_none() && run.workflow_attempt_id.is_none(),
            "non-Workflow run carries Workflow linkage"
        );
    }
    Ok(())
}

fn map_attempt(row: &Row<'_>) -> rusqlite::Result<HiveRunAttempt> {
    let outcome = parse_required(8, row.get::<_, String>(8)?, HiveRunAttemptOutcome::parse)?;
    Ok(HiveRunAttempt {
        id: row.get(0)?,
        run_id: row.get(1)?,
        attempt_no: nonnegative_i64(row, 2)? as u32,
        executor_id: row.get(3)?,
        lease_token: row.get(4)?,
        lease_epoch: nonnegative_i64(row, 5)? as u64,
        started_at: row.get(6)?,
        finished_at: row.get(7)?,
        outcome,
        stop_reason: row.get(9)?,
        error: row.get(10)?,
        retry_at: row.get(11)?,
        trace_sequence_start: row.get(12)?,
        trace_sequence_end: row.get(13)?,
    })
}

fn configured_worker_id(config: &serde_json::Value) -> Option<&str> {
    config
        .get("worker_id")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            config
                .get("group")
                .and_then(|group| group.get("worker_id"))
                .and_then(serde_json::Value::as_str)
        })
}

fn daemon_fence_is_current(tx: &Transaction<'_>, fence: &DaemonFence, now: &str) -> Result<bool> {
    anyhow::ensure!(
        !fence.lease_name.trim().is_empty(),
        "daemon lease name is empty"
    );
    anyhow::ensure!(
        !fence.owner_id.trim().is_empty(),
        "daemon owner id is empty"
    );
    anyhow::ensure!(
        fence.fencing_token <= i64::MAX as u64,
        "daemon fencing token exceeds SQLite integer range"
    );
    let current = tx.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM hive_daemon_leases
             WHERE lease_name = ?1 AND owner_id = ?2 AND fencing_token = ?3
               AND expires_at > ?4
         )",
        params![fence.lease_name, fence.owner_id, fence.fencing_token, now],
        |row| row.get::<_, bool>(0),
    )?;
    Ok(current)
}

fn normalize_optional_timestamp(value: Option<&str>) -> Result<Option<String>> {
    value
        .map(normalize_timestamp)
        .transpose()
        .map_err(Into::into)
}

fn nonnegative_i64(row: &Row<'_>, index: usize) -> rusqlite::Result<i64> {
    let value = row.get::<_, i64>(index)?;
    if value < 0 {
        Err(conversion_error(index, "negative unsigned value"))
    } else {
        Ok(value)
    }
}

fn optional_nonnegative_i64(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<i64>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| {
            if value < 0 {
                Err(conversion_error(index, "negative unsigned value"))
            } else {
                Ok(value)
            }
        })
        .transpose()
}

fn parse_required<T>(
    index: usize,
    value: String,
    parse: impl FnOnce(&str) -> Option<T>,
) -> rusqlite::Result<T> {
    parse(&value).ok_or_else(|| conversion_error(index, format!("invalid enum value: {value}")))
}

fn conversion_error(index: usize, message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        rusqlite::types::Type::Text,
        Box::new(IoError::new(ErrorKind::InvalidData, message.into())),
    )
}
