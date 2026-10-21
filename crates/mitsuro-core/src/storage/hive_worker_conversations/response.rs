use std::path::{Path, PathBuf};

use anyhow::{Context, Result as AnyResult};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde_json::Value;
use thiserror::Error;

use crate::ai::models::ModelKey;
use crate::ai::types::{Content, Usage};
use crate::hive::{canonical_timestamp, parse_utc_timestamp, HiveRunStatus};
use crate::storage::hive_groups::append_message_with_conn;
use crate::storage::{
    bind_worker_governor_recovery_grant_to_run_in_transaction,
    finalize_worker_conversation_after_governor_recovery_in_transaction, hash_request_bytes,
    reactivate_worker_conversation_controller_after_governor_recovery_in_transaction,
    transfer_worker_governor_recovery_grant_to_successor_in_transaction,
    unresolved_worker_governor_recovery_calls_belong_to_run_in_transaction,
    validate_unbound_worker_governor_recovery_grant_in_transaction,
    worker_governor_recovery_grant_covers_unresolved_in_transaction,
    worker_has_unacknowledged_unresolved_provider_calls_in_transaction, DaemonFence, Database,
    HiveGroupMessage, HiveGroupSenderKind, HiveRunExecutionContextV1, HiveRunExecutionModeV1,
    NewHiveGroupMessage, WorkerConversationLane, MAX_HIVE_GROUP_MESSAGE_BYTES,
    WORKER_CONVERSATION_STOP_REQUESTED_REASON,
};

use super::accept::{canonical_input_message_key, insert_user_episode};

const MAX_RESPONSE_ID_BYTES: usize = 256;
const MAX_RESPONSE_TEXT_BYTES: usize = MAX_HIVE_GROUP_MESSAGE_BYTES;
const WORKER_CONVERSATION_GOVERNOR_RECOVERY_REASON: &str =
    "owner acknowledged unresolved provider accounting for one direct-message recovery call";
const WORKER_CONVERSATION_GOVERNOR_RECOVERY_OUTCOME: &str = "owner_acknowledged_governor_recovery";
const WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_REASON: &str =
    "owner acknowledged completed provider response loss for one direct-message recovery call";
const WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_OUTCOME: &str =
    "owner_acknowledged_provider_response_loss";

/// Exact durable authority and visible text for one Worker response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitWorkerConversationResponse {
    pub worker_id: String,
    pub worker_revision: u64,
    pub owner_user_id: Option<String>,
    pub session_id: String,
    pub lane: WorkerConversationLane,
    pub run_id: String,
    pub run_lease_token: String,
    pub run_lease_epoch: u64,
    pub provider_call_id: String,
    pub response_text: String,
    pub committed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerConversationResponseCommitDisposition {
    Inserted,
    AdoptedIdentical,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConversationResponseCommit {
    pub disposition: WorkerConversationResponseCommitDisposition,
    pub response_message_id: i64,
    pub response_group_message_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedWorkerConversationInput {
    pub input_id: String,
    pub canonical_message_id: i64,
    pub assigned_run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerConversationGovernorRecovery {
    NoBoundary,
    Recovered {
        predecessor_run_id: String,
        session_id: String,
        materialized_run_id: Option<String>,
    },
    UnsupportedBoundary {
        run_id: String,
        kind: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerConversationPredecessorAuthority {
    /// A canonical visible response (or the provider-free Introduction review
    /// audit) committed before the predecessor reached `succeeded`.
    CanonicalCompletion,
    /// The owner stopped the exact ordinary direct-message run before it
    /// committed a response.
    StoppedWorkerConversation,
    /// The exact ordinary direct-message run reached a non-retryable terminal
    /// state without a canonical response or ambiguous provider boundary.
    TerminalWithoutCanonicalResponse,
    /// The owner acknowledged that one completed, provider-acknowledged Agent
    /// turn lost its response before canonical conversation commit.
    AcknowledgedProviderResponseLoss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiredWorkerResponseDisposition {
    NotWorkerBound,
    SafeBeforeProviderBoundary,
    CanonicalResponseAdopted,
    ProviderBoundaryWithoutResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoppedWorkerConversationFinalization {
    CanonicalResponseAdopted,
    Cancelled,
}

#[derive(Debug)]
struct CommittedWorkerResponse {
    message_id: i64,
    group_message_id: Option<String>,
    provider_call_id: String,
}

/// Typed failure surface for the provider-accounting boundary.
///
/// Only `StaleRejected` proves that the current lifecycle/fence rejected the
/// response before any write. Conflicts and ambiguous commits deliberately
/// keep the provider Started row unresolved for fenced takeover recovery.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkerConversationResponseCommitError {
    #[error("Worker response rejected by a stale execution fence: {0}")]
    StaleRejected(String),
    #[error("Worker response conflicts with durable state: {0}")]
    ConflictOrCorrupt(String),
    #[error("Worker response commit outcome is uncertain: {0}")]
    CommitUncertain(String),
}

/// SQLite-backed canonical response writer with one frozen daemon generation.
#[derive(Debug, Clone)]
pub struct SqliteWorkerConversationResponseStore {
    database_path: PathBuf,
    daemon_fence: DaemonFence,
}

impl SqliteWorkerConversationResponseStore {
    pub fn new(database_path: impl AsRef<Path>, daemon_fence: DaemonFence) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
            daemon_fence,
        }
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn daemon_fence(&self) -> &DaemonFence {
        &self.daemon_fence
    }

    pub fn commit_response(
        &self,
        input: &CommitWorkerConversationResponse,
    ) -> Result<WorkerConversationResponseCommit, WorkerConversationResponseCommitError> {
        validate_commit_input(input)?;
        let database = Database::new(&self.database_path)
            .map_err(|error| conflict(format!("opening canonical response database: {error:#}")))?;
        let tx = Transaction::new_unchecked(database.conn(), TransactionBehavior::Immediate)
            .map_err(|error| conflict(format!("acquiring canonical response writer: {error}")))?;
        let response = commit_response_in_transaction(&tx, &self.daemon_fence, input)?;
        tx.commit().map_err(|error| {
            WorkerConversationResponseCommitError::CommitUncertain(bounded_reason(format!(
                "SQLite commit failed after canonical response writes: {error}"
            )))
        })?;
        Ok(response)
    }
}

#[derive(Debug)]
struct ResponseRunBinding {
    _controller_id: String,
    session_id: String,
    worker_id: String,
    kind: String,
    config_json: String,
    status: String,
    lease_owner: Option<String>,
    lease_token: Option<String>,
    lease_epoch: Option<u64>,
    lease_expires_at: Option<String>,
    execution_context_json: String,
    governor_origin: String,
    governor_lane_key: String,
    response_message_id: Option<i64>,
    response_group_message_id: Option<String>,
    response_provider_call_id: Option<String>,
    group_id: Option<String>,
    group_turn_id: Option<String>,
    trigger_message_id: Option<String>,
    last_stop_reason: Option<String>,
    attempt_count: u32,
    objective_message_id: Option<i64>,
    controller_worker_id: Option<String>,
    controller_user_id: Option<String>,
    controller_session_id: String,
    controller_status: String,
    worker_user_id: Option<String>,
    worker_status: String,
    worker_revision: u64,
    worker_dm_session_id: Option<String>,
    worker_model: String,
    worker_model_key_json: String,
    worker_model_catalog_revision: Option<String>,
    worker_permission_mode: String,
    session_user_id: Option<String>,
    session_type: String,
    workspace_mode: String,
    working_dir: Option<String>,
    project_dir: Option<String>,
}

#[derive(Debug)]
struct ResponseProviderCall {
    provider_call_id: String,
    worker_id: String,
    worker_revision: u64,
    owner_user_id: Option<String>,
    session_id: String,
    group_id: Option<String>,
    run_id: String,
    run_lease_token: String,
    run_lease_epoch: u64,
    origin: String,
    lane_key: String,
    provider_id: String,
    model_id: String,
    model_key_json: String,
    model_catalog_revision: Option<String>,
    permission_mode: String,
    call_kind: String,
    outcome_state: Option<String>,
    outcome: Option<String>,
    remote_acceptance: Option<String>,
}

fn commit_response_in_transaction(
    tx: &Transaction<'_>,
    daemon_fence: &DaemonFence,
    input: &CommitWorkerConversationResponse,
) -> Result<WorkerConversationResponseCommit, WorkerConversationResponseCommitError> {
    let now = canonical_timestamp(input.committed_at);
    let daemon_current = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM hive_daemon_leases
                 WHERE lease_name = ?1 AND owner_id = ?2 AND fencing_token = ?3
                   AND expires_at > ?4
             )",
            params![
                daemon_fence.lease_name,
                daemon_fence.owner_id,
                daemon_fence.fencing_token,
                now,
            ],
            |row| row.get::<_, bool>(0),
        )
        .map_err(read_conflict("validating current daemon generation"))?;
    if !daemon_current || input.run_lease_epoch != daemon_fence.fencing_token {
        return Err(stale("daemon generation is no longer current"));
    }

    let binding = load_response_run(tx, &input.run_id)?
        .ok_or_else(|| conflict("Worker response run does not exist"))?;
    validate_response_run(tx, daemon_fence, input, &binding, &now)?;
    let provider_call = validate_provider_call(tx, input, &binding)?;

    let response_text = input.response_text.trim();
    let content_json = serde_json::to_string(&vec![Content::text(response_text.to_string())])
        .map_err(|error| conflict(format!("encoding canonical response: {error}")))?;
    let response_key = if provider_call.call_kind == "worker_introduction_onboarding" {
        canonical_onboarding_response_key(
            &input.worker_id,
            binding
                .objective_message_id
                .ok_or_else(|| conflict("onboarding response has no user objective message"))?,
        )
    } else {
        canonical_response_message_key(&input.run_id)
    };
    let existing_messages = load_keyed_messages(tx, &response_key)?;
    let (response_message_id, response_created_at, inserted_message) = match existing_messages
        .as_slice()
    {
        [] => {
            tx.execute(
                "INSERT INTO messages (
                         session_id, role, content, created_at, idempotency_key
                     ) VALUES (?1, 'assistant', ?2, ?3, ?4)",
                params![binding.session_id, content_json, now, response_key],
            )
            .map_err(write_conflict("inserting canonical Worker response"))?;
            (tx.last_insert_rowid(), now.clone(), true)
        }
        [(message_id, session_id, role, content, created_at)] => {
            if session_id != &binding.session_id || role != "assistant" || content != &content_json
            {
                return Err(conflict(
                    "canonical Worker response key belongs to different content or binding",
                ));
            }
            (*message_id, created_at.clone(), false)
        }
        _ => {
            return Err(conflict(
                "canonical Worker response key exists in multiple sessions",
            ));
        }
    };

    insert_or_validate_assistant_episode(
        tx,
        &binding.session_id,
        response_message_id,
        response_text,
        &response_created_at,
    )?;

    let response_group_message_id = match &input.lane {
        WorkerConversationLane::DirectMessage => None,
        WorkerConversationLane::Group { group_id } => {
            let turn_id = binding
                .group_turn_id
                .as_deref()
                .ok_or_else(|| conflict("group response run has no turn id"))?;
            let trigger_message_id = binding
                .trigger_message_id
                .as_deref()
                .ok_or_else(|| conflict("group response run has no trigger message"))?;
            Some(insert_or_adopt_group_response(
                tx,
                group_id,
                turn_id,
                trigger_message_id,
                &binding.worker_id,
                &input.run_id,
                response_text,
                &now,
            )?)
        }
    };

    match (
        binding.response_message_id,
        binding.response_group_message_id.as_deref(),
        binding.response_provider_call_id.as_deref(),
        response_group_message_id.as_deref(),
    ) {
        (Some(existing), existing_group, Some(existing_call), expected_group)
            if existing == response_message_id
                && existing_group == expected_group
                && existing_call == input.provider_call_id => {}
        (None, None, None, expected_group) => {
            let changed = tx
                .execute(
                    "UPDATE hive_runs
                     SET response_message_id = ?2, response_group_message_id = ?3,
                         response_provider_call_id = ?4, updated_at = ?5
                     WHERE id = ?1 AND status = 'running'
                       AND lease_owner = ?6 AND lease_token = ?7 AND lease_epoch = ?8
                       AND lease_expires_at > ?5
                       AND response_message_id IS NULL
                       AND response_group_message_id IS NULL
                       AND response_provider_call_id IS NULL",
                    params![
                        input.run_id,
                        response_message_id,
                        expected_group,
                        input.provider_call_id,
                        now,
                        daemon_fence.owner_id,
                        input.run_lease_token,
                        input.run_lease_epoch,
                    ],
                )
                .map_err(write_conflict(
                    "linking canonical Worker response to its run",
                ))?;
            if changed != 1 {
                return Err(conflict(
                    "Worker response linkage changed inside the fenced transaction",
                ));
            }
        }
        _ => {
            return Err(conflict(
                "Worker run already links a different canonical response",
            ));
        }
    }

    tx.execute(
        "UPDATE sessions
         SET updated_at = CASE WHEN updated_at < ?2 THEN ?2 ELSE updated_at END
         WHERE id = ?1",
        params![binding.session_id, response_created_at],
    )
    .map_err(write_conflict("updating Worker response session"))?;

    Ok(WorkerConversationResponseCommit {
        disposition: if inserted_message {
            WorkerConversationResponseCommitDisposition::Inserted
        } else {
            WorkerConversationResponseCommitDisposition::AdoptedIdentical
        },
        response_message_id,
        response_group_message_id,
    })
}

fn load_response_run(
    tx: &Transaction<'_>,
    run_id: &str,
) -> Result<Option<ResponseRunBinding>, WorkerConversationResponseCommitError> {
    tx.query_row(
        "SELECT run.controller_id, run.session_id, run.worker_id, run.kind,
                run.config_json, run.status, run.lease_owner, run.lease_token,
                run.lease_epoch, run.lease_expires_at,
                run.execution_context_json, run.governor_origin,
                run.governor_lane_key, run.response_message_id,
                run.response_group_message_id, run.group_id,
                run.group_turn_id, run.trigger_message_id, run.attempt_count,
                controller.worker_id, controller.user_id, controller.session_id,
                controller.status, worker.user_id, worker.status,
                worker.revision, worker.dm_session_id, worker.model,
                worker.model_key_json, worker.model_catalog_revision,
                worker.permission_mode, session.user_id, session.session_type,
                session.workspace_mode, session.working_dir, session.project_dir,
                run.objective_message_id, run.response_provider_call_id,
                run.last_stop_reason
         FROM hive_runs run
         JOIN hive_controllers controller ON controller.id = run.controller_id
         JOIN hive_workers worker ON worker.id = run.worker_id
         JOIN sessions session ON session.id = run.session_id
         WHERE run.id = ?1",
        [run_id],
        map_response_run,
    )
    .optional()
    .map_err(read_conflict("loading exact Worker response run"))
}

fn map_response_run(row: &Row<'_>) -> rusqlite::Result<ResponseRunBinding> {
    Ok(ResponseRunBinding {
        _controller_id: row.get(0)?,
        session_id: row.get(1)?,
        worker_id: row.get(2)?,
        kind: row.get(3)?,
        config_json: row.get(4)?,
        status: row.get(5)?,
        lease_owner: row.get(6)?,
        lease_token: row.get(7)?,
        lease_epoch: optional_nonnegative(row, 8)?,
        lease_expires_at: row.get(9)?,
        execution_context_json: row.get(10)?,
        governor_origin: row.get(11)?,
        governor_lane_key: row.get(12)?,
        response_message_id: row.get(13)?,
        response_group_message_id: row.get(14)?,
        group_id: row.get(15)?,
        group_turn_id: row.get(16)?,
        trigger_message_id: row.get(17)?,
        attempt_count: nonnegative(row, 18)? as u32,
        controller_worker_id: row.get(19)?,
        controller_user_id: row.get(20)?,
        controller_session_id: row.get(21)?,
        controller_status: row.get(22)?,
        worker_user_id: row.get(23)?,
        worker_status: row.get(24)?,
        worker_revision: nonnegative(row, 25)? as u64,
        worker_dm_session_id: row.get(26)?,
        worker_model: row.get(27)?,
        worker_model_key_json: row.get(28)?,
        worker_model_catalog_revision: row.get(29)?,
        worker_permission_mode: row.get(30)?,
        session_user_id: row.get(31)?,
        session_type: row.get(32)?,
        workspace_mode: row.get(33)?,
        working_dir: row.get(34)?,
        project_dir: row.get(35)?,
        objective_message_id: row.get(36)?,
        response_provider_call_id: row.get(37)?,
        last_stop_reason: row.get(38)?,
    })
}

fn validate_response_run(
    tx: &Transaction<'_>,
    daemon_fence: &DaemonFence,
    input: &CommitWorkerConversationResponse,
    binding: &ResponseRunBinding,
    now: &str,
) -> Result<(), WorkerConversationResponseCommitError> {
    if binding.worker_id != input.worker_id
        || binding.session_id != input.session_id
        || binding.worker_user_id != input.owner_user_id
    {
        return Err(conflict(
            "response authority does not name the run's exact Worker, owner, and session",
        ));
    }
    if binding.status != "running"
        || binding.lease_owner.as_deref() != Some(daemon_fence.owner_id.as_str())
        || binding.lease_token.as_deref() != Some(input.run_lease_token.as_str())
        || binding.lease_epoch != Some(input.run_lease_epoch)
        || binding
            .lease_expires_at
            .as_deref()
            .is_none_or(|expiry| expiry <= now)
    {
        return Err(stale("run lease is no longer the current running claim"));
    }
    if binding.kind == "worker_conversation"
        && binding.last_stop_reason.as_deref() == Some(WORKER_CONVERSATION_STOP_REQUESTED_REASON)
    {
        return Err(stale(
            "Worker response was rejected because exact direct-chat Stop already committed",
        ));
    }
    if binding.controller_status != "active"
        || binding.controller_worker_id.as_deref() != Some(binding.worker_id.as_str())
        || binding.controller_user_id != binding.worker_user_id
        || binding.controller_session_id != binding.session_id
    {
        return Err(stale("Worker controller binding changed or was cancelled"));
    }
    if binding.worker_status != "active"
        || binding.worker_revision != input.worker_revision
        || binding.session_user_id != binding.worker_user_id
        || binding.session_type != "hive"
    {
        return Err(stale("Worker profile, owner, or session lifecycle changed"));
    }
    let open_attempt = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM hive_run_attempts
                 WHERE run_id = ?1 AND attempt_no = ?2 AND executor_id = ?3
                   AND lease_token = ?4 AND lease_epoch = ?5
                   AND finished_at IS NULL
             )",
            params![
                input.run_id,
                binding.attempt_count,
                daemon_fence.owner_id,
                input.run_lease_token,
                input.run_lease_epoch,
            ],
            |row| row.get::<_, bool>(0),
        )
        .map_err(read_conflict("validating open Worker run attempt"))?;
    if !open_attempt {
        return Err(stale("Worker run attempt is no longer open"));
    }

    let context: HiveRunExecutionContextV1 = serde_json::from_str(&binding.execution_context_json)
        .map_err(|error| conflict(format!("decoding frozen execution context: {error}")))?;
    context
        .validate()
        .map_err(|error| conflict(format!("validating frozen execution context: {error:#}")))?;
    if context.worker_id() != input.worker_id
        || context.worker_revision() != input.worker_revision
        || context.lane() != &input.lane
        || binding.governor_lane_key
            != input
                .lane
                .canonical_lane_key()
                .map_err(|error| conflict(format!("validating response lane: {error:#}")))?
    {
        return Err(conflict(
            "response authority differs from the run's frozen execution context",
        ));
    }
    if context.worker_revision() != binding.worker_revision {
        return Err(stale(
            "Worker profile revision changed after the run was frozen",
        ));
    }
    let expected_origin = expected_origin_for_kind(&binding.kind)
        .ok_or_else(|| conflict("run kind is not a Worker conversation response kind"))?;
    if !expected_origin.contains(&binding.governor_origin.as_str()) {
        return Err(conflict("Worker run kind and governor origin disagree"));
    }
    validate_workspace_binding(binding, &context)?;
    validate_model_binding(binding)?;
    validate_lane_binding(tx, input, binding)?;
    Ok(())
}

fn validate_workspace_binding(
    binding: &ResponseRunBinding,
    context: &HiveRunExecutionContextV1,
) -> Result<(), WorkerConversationResponseCommitError> {
    let current = match &context.mode {
        HiveRunExecutionModeV1::WorkerConversationNeutral { .. } => {
            binding.workspace_mode == "neutral"
                && binding.working_dir.as_deref().is_none_or(str::is_empty)
                && binding.project_dir.as_deref().is_none_or(str::is_empty)
        }
        HiveRunExecutionModeV1::WorkerWorkspaceAttached {
            workspace_mode,
            working_dir,
            project_dir,
            ..
        } => {
            binding.workspace_mode == workspace_mode.to_string()
                && binding.working_dir.as_deref() == Some(working_dir.as_str())
                && binding.project_dir.as_deref() == project_dir.as_deref()
        }
        HiveRunExecutionModeV1::WorkerGoal { .. }
        | HiveRunExecutionModeV1::WorkerGoalAcceptance { .. } => false,
    };
    if !current {
        return Err(stale(
            "Worker workspace binding changed before response commit",
        ));
    }
    Ok(())
}

fn validate_model_binding(
    binding: &ResponseRunBinding,
) -> Result<(), WorkerConversationResponseCommitError> {
    let config: Value = serde_json::from_str(&binding.config_json)
        .map_err(|error| conflict(format!("decoding frozen Worker run config: {error}")))?;
    let worker_model_key: Value = serde_json::from_str(&binding.worker_model_key_json)
        .map_err(|error| conflict(format!("decoding current Worker model key: {error}")))?;
    let model_current = config.get("model").and_then(Value::as_str)
        == Some(binding.worker_model.as_str())
        && config.get("model_key") == Some(&worker_model_key)
        && config.get("model_catalog_revision").and_then(Value::as_str)
            == binding.worker_model_catalog_revision.as_deref()
        && config.get("permission_mode").and_then(Value::as_str)
            == Some(binding.worker_permission_mode.as_str());
    if !model_current {
        return Err(stale(
            "Worker model, catalog, or permission binding changed before response commit",
        ));
    }
    Ok(())
}

fn validate_lane_binding(
    tx: &Transaction<'_>,
    input: &CommitWorkerConversationResponse,
    binding: &ResponseRunBinding,
) -> Result<(), WorkerConversationResponseCommitError> {
    match &input.lane {
        WorkerConversationLane::DirectMessage => {
            if binding.worker_dm_session_id.as_deref() != Some(binding.session_id.as_str())
                || binding.group_id.is_some()
                || binding.group_turn_id.is_some()
                || binding.trigger_message_id.is_some()
            {
                return Err(stale("Worker direct-message binding changed"));
            }
        }
        WorkerConversationLane::Group { group_id } => {
            if binding.group_id.as_deref() != Some(group_id.as_str())
                || binding.worker_dm_session_id.as_deref() == Some(binding.session_id.as_str())
            {
                return Err(conflict("Worker group lane does not match the frozen run"));
            }
            let group_current = tx
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1
                         FROM hive_group_worker_lanes lane
                         JOIN hive_groups group_room ON group_room.id = lane.group_id
                         JOIN hive_group_members member
                           ON member.group_id = lane.group_id
                          AND member.worker_id = lane.worker_id
                         JOIN hive_group_turns turn ON turn.id = ?4
                         JOIN hive_group_messages trigger ON trigger.id = ?5
                         WHERE lane.group_id = ?1 AND lane.worker_id = ?2
                           AND lane.session_id = ?3
                           AND group_room.status = 'active'
                           AND turn.group_id = lane.group_id
                           AND turn.status = 'running'
                           AND turn.trigger_message_id = trigger.id
                           AND trigger.group_id = lane.group_id
                     )",
                    params![
                        group_id,
                        binding.worker_id,
                        binding.session_id,
                        binding.group_turn_id,
                        binding.trigger_message_id,
                    ],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(read_conflict("validating active Worker group lane"))?;
            if !group_current {
                return Err(stale("Worker group lane or turn is no longer active"));
            }
        }
    }
    Ok(())
}

fn validate_provider_call(
    tx: &Transaction<'_>,
    input: &CommitWorkerConversationResponse,
    binding: &ResponseRunBinding,
) -> Result<ResponseProviderCall, WorkerConversationResponseCommitError> {
    let call = tx
        .query_row(
            "SELECT call.worker_id, call.worker_revision, call.owner_user_id,
                    call.session_id, call.group_id, call.run_id,
                    call.run_lease_token, call.run_lease_epoch, call.origin,
                    call.lane_key, call.provider_id, call.model_id,
                    call.model_key_json, call.model_catalog_revision,
                    call.permission_mode, call.call_kind, outcome.state,
                    outcome.outcome, outcome.remote_acceptance,
                    call.provider_call_id
             FROM hive_worker_provider_calls call
             LEFT JOIN hive_worker_provider_call_outcomes outcome
               ON outcome.provider_call_id = call.provider_call_id
             WHERE call.provider_call_id = ?1",
            [&input.provider_call_id],
            map_response_provider_call,
        )
        .optional()
        .map_err(read_conflict("loading exact provider Started row"))?
        .ok_or_else(|| conflict("provider Started row does not exist"))?;

    let expected_group_id = match &input.lane {
        WorkerConversationLane::DirectMessage => None,
        WorkerConversationLane::Group { group_id } => Some(group_id.as_str()),
    };
    if call.worker_id != input.worker_id
        || call.worker_revision != input.worker_revision
        || call.owner_user_id != input.owner_user_id
        || call.session_id != input.session_id
        || call.group_id.as_deref() != expected_group_id
        || call.run_id != input.run_id
        || call.run_lease_token != input.run_lease_token
        || call.run_lease_epoch != input.run_lease_epoch
        || call.origin != binding.governor_origin
        || call.lane_key != binding.governor_lane_key
    {
        return Err(conflict(
            "provider Started provenance differs from the exact Worker run",
        ));
    }
    let config: Value = serde_json::from_str(&binding.config_json)
        .map_err(|error| conflict(format!("decoding frozen provider config: {error}")))?;
    let model_key_value = config
        .get("model_key")
        .cloned()
        .ok_or_else(|| conflict("frozen Worker run has no model key"))?;
    let model_key: ModelKey = serde_json::from_value(model_key_value.clone())
        .map_err(|error| conflict(format!("decoding frozen model key: {error}")))?;
    let call_model_key: Value = serde_json::from_str(&call.model_key_json)
        .map_err(|error| conflict(format!("decoding provider Started model key: {error}")))?;
    if call.provider_id != model_key.provider.storage_key()
        || call.model_id != model_key.model_id
        || call_model_key != model_key_value
        || call.model_catalog_revision != binding.worker_model_catalog_revision
        || call.permission_mode != binding.worker_permission_mode
    {
        return Err(conflict(
            "provider Started model, catalog, or permission provenance differs from the run",
        ));
    }
    let onboarding_binding = onboarding_response_binding_current(tx, input, binding)?;
    let onboarding = call.call_kind == "worker_introduction_onboarding";
    if call.call_kind != "agent_turn" && !onboarding {
        return Err(conflict(
            "provider Started call kind cannot authorize a visible Worker response",
        ));
    }
    if onboarding && !onboarding_binding {
        return Err(stale(
            "Introduction onboarding lifecycle changed before response commit",
        ));
    }
    match (
        call.outcome_state.as_deref(),
        call.outcome.as_deref(),
        call.remote_acceptance.as_deref(),
    ) {
        (None, None, None) | (Some("completed"), Some("completed"), Some("acknowledged")) => {
            Ok(call)
        }
        (Some("completed"), Some("semantic_invalid"), Some("acknowledged"))
            if onboarding_binding =>
        {
            Ok(call)
        }
        _ => Err(conflict(
            "provider call already has an incompatible terminal outcome",
        )),
    }
}

fn onboarding_response_binding_current(
    tx: &Transaction<'_>,
    input: &CommitWorkerConversationResponse,
    binding: &ResponseRunBinding,
) -> Result<bool, WorkerConversationResponseCommitError> {
    let Some(objective_message_id) = binding.objective_message_id else {
        return Ok(false);
    };
    if binding.kind != "worker_conversation"
        || !matches!(&input.lane, WorkerConversationLane::DirectMessage)
    {
        return Ok(false);
    }
    let current = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM hive_worker_introductions introduction
                 JOIN messages objective ON objective.id = ?3
                 WHERE introduction.worker_id = ?1
                   AND introduction.status = 'awaiting_context'
                   AND introduction.opening_message_id IS NOT NULL
                   AND objective.session_id = ?2 AND objective.role = 'user'
             )",
            params![input.worker_id, binding.session_id, objective_message_id],
            |row| row.get::<_, bool>(0),
        )
        .map_err(read_conflict(
            "validating Introduction onboarding lifecycle",
        ))?;
    Ok(current)
}

fn map_response_provider_call(row: &Row<'_>) -> rusqlite::Result<ResponseProviderCall> {
    Ok(ResponseProviderCall {
        provider_call_id: row.get(19)?,
        worker_id: row.get(0)?,
        worker_revision: nonnegative(row, 1)? as u64,
        owner_user_id: row.get(2)?,
        session_id: row.get(3)?,
        group_id: row.get(4)?,
        run_id: row.get(5)?,
        run_lease_token: row.get(6)?,
        run_lease_epoch: nonnegative(row, 7)? as u64,
        origin: row.get(8)?,
        lane_key: row.get(9)?,
        provider_id: row.get(10)?,
        model_id: row.get(11)?,
        model_key_json: row.get(12)?,
        model_catalog_revision: row.get(13)?,
        permission_mode: row.get(14)?,
        call_kind: row.get(15)?,
        outcome_state: row.get(16)?,
        outcome: row.get(17)?,
        remote_acceptance: row.get(18)?,
    })
}

fn load_keyed_messages(
    tx: &Transaction<'_>,
    key: &str,
) -> Result<Vec<(i64, String, String, String, String)>, WorkerConversationResponseCommitError> {
    let mut statement = tx
        .prepare(
            "SELECT id, session_id, role, content, created_at
             FROM messages WHERE idempotency_key = ?1 ORDER BY id",
        )
        .map_err(read_conflict("preparing canonical response lookup"))?;
    let rows = statement
        .query_map([key], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .map_err(read_conflict("querying canonical response key"))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(read_conflict("reading canonical response rows"))?;
    Ok(rows)
}

fn insert_or_validate_assistant_episode(
    tx: &Transaction<'_>,
    session_id: &str,
    message_id: i64,
    response_text: &str,
    occurred_at: &str,
) -> Result<(), WorkerConversationResponseCommitError> {
    let body = truncate_utf8(
        &response_text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        16 * 1024,
    );
    let content_hash =
        hash_request_bytes([b"assistant".as_slice(), &[0], body.as_bytes()].concat());
    let existing = tx
        .query_row(
            "SELECT role, body, content_hash, occurred_at
             FROM conversation_episodes
             WHERE session_id = ?1 AND source_message_id = ?2",
            params![session_id, message_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(read_conflict("loading canonical response episode"))?;
    if let Some((role, existing_body, existing_hash, existing_at)) = existing {
        if role != "assistant"
            || existing_body != body
            || existing_hash != content_hash
            || existing_at != occurred_at
        {
            return Err(conflict(
                "canonical response episode conflicts with its source message",
            ));
        }
        return Ok(());
    }
    tx.execute(
        "INSERT INTO conversation_episodes (
             session_id, source_message_id, role, body, content_hash, occurred_at
         ) VALUES (?1, ?2, 'assistant', ?3, ?4, ?5)",
        params![session_id, message_id, body, content_hash, occurred_at],
    )
    .map_err(write_conflict("inserting canonical response episode"))?;
    Ok(())
}

fn insert_or_adopt_group_response(
    tx: &Transaction<'_>,
    group_id: &str,
    turn_id: &str,
    trigger_message_id: &str,
    worker_id: &str,
    run_id: &str,
    response_text: &str,
    now: &str,
) -> Result<String, WorkerConversationResponseCommitError> {
    let key = canonical_group_response_key(turn_id, worker_id, run_id);
    let existing = tx
        .query_row(
            "SELECT id, group_id, seq, sender_kind, sender_worker_id,
                    sender_run_id, content, reply_to_message_id, turn_id,
                    idempotency_key, created_at
             FROM hive_group_messages
             WHERE group_id = ?1 AND idempotency_key = ?2",
            params![group_id, key],
            map_group_message,
        )
        .optional()
        .map_err(read_conflict("loading canonical group response"))?;
    let message = if let Some(existing) = existing {
        existing
    } else {
        append_message_with_conn(
            tx,
            &NewHiveGroupMessage {
                group_id: group_id.to_string(),
                sender_kind: HiveGroupSenderKind::Worker,
                sender_worker_id: Some(worker_id.to_string()),
                sender_run_id: Some(run_id.to_string()),
                content: response_text.to_string(),
                reply_to_message_id: Some(trigger_message_id.to_string()),
                turn_id: Some(turn_id.to_string()),
                idempotency_key: Some(key.clone()),
            },
            now,
        )
        .map_err(|error| conflict(format!("inserting canonical group response: {error:#}")))?
    };
    if message.group_id != group_id
        || message.sender_kind != HiveGroupSenderKind::Worker
        || message.sender_worker_id.as_deref() != Some(worker_id)
        || message.sender_run_id.as_deref() != Some(run_id)
        || message.content != response_text
        || message.reply_to_message_id.as_deref() != Some(trigger_message_id)
        || message.turn_id.as_deref() != Some(turn_id)
        || message.idempotency_key.as_deref() != Some(key.as_str())
    {
        return Err(conflict(
            "canonical group response key belongs to different content or binding",
        ));
    }
    Ok(message.id)
}

fn map_group_message(row: &Row<'_>) -> rusqlite::Result<HiveGroupMessage> {
    let sender = row.get::<_, String>(3)?;
    let sender_kind = HiveGroupSenderKind::parse(&sender).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            rusqlite::types::Type::Text,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid Hive group sender: {sender}"),
            )
            .into(),
        )
    })?;
    Ok(HiveGroupMessage {
        id: row.get(0)?,
        group_id: row.get(1)?,
        seq: row.get(2)?,
        sender_kind,
        sender_worker_id: row.get(4)?,
        sender_run_id: row.get(5)?,
        content: row.get(6)?,
        reply_to_message_id: row.get(7)?,
        turn_id: row.get(8)?,
        idempotency_key: row.get(9)?,
        created_at: row.get(10)?,
    })
}

pub(crate) fn committed_worker_response_in_transaction(
    tx: &Transaction<'_>,
    run_id: &str,
) -> AnyResult<Option<(i64, Option<String>)>> {
    Ok(load_committed_worker_response(tx, run_id)?
        .map(|response| (response.message_id, response.group_message_id)))
}

fn load_committed_worker_response(
    tx: &Transaction<'_>,
    run_id: &str,
) -> AnyResult<Option<CommittedWorkerResponse>> {
    let row = tx
        .query_row(
            "SELECT worker_id, kind, session_id, execution_context_json,
                    response_message_id, response_group_message_id,
                    group_id, group_turn_id, trigger_message_id,
                    objective_message_id, response_provider_call_id
             FROM hive_runs WHERE id = ?1",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                ))
            },
        )
        .optional()
        .context("loading Worker response linkage")?;
    let Some((
        worker_id,
        kind,
        session_id,
        context_json,
        response_message_id,
        response_group_message_id,
        group_id,
        group_turn_id,
        trigger_message_id,
        objective_message_id,
        response_provider_call_id,
    )) = row
    else {
        return Ok(None);
    };
    let (Some(worker_id), Some(session_id), Some(context_json)) =
        (worker_id, session_id, context_json)
    else {
        return Ok(None);
    };
    if kind == "worker_introduction" {
        return Ok(None);
    }
    let response_key = canonical_response_message_key(run_id);
    let keyed = tx
        .query_row(
            "SELECT id FROM messages
             WHERE session_id = ?1 AND idempotency_key = ?2",
            params![session_id, response_key],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    let onboarding_key = objective_message_id
        .map(|message_id| canonical_onboarding_response_key(&worker_id, message_id));
    let keyed_onboarding = if let Some(onboarding_key) = onboarding_key.as_deref() {
        tx.query_row(
            "SELECT id FROM messages
             WHERE session_id = ?1 AND idempotency_key = ?2",
            params![session_id, onboarding_key],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    } else {
        None
    };
    let Some(response_message_id) = response_message_id else {
        anyhow::ensure!(
            keyed.is_none() && response_provider_call_id.is_none(),
            "keyed Worker response or provider provenance exists without immutable run linkage"
        );
        // The specialized onboarding runner historically commits its exact
        // assistant row before linking the WorkerConversation run. Fenced
        // reconciliation below is the only authority allowed to adopt it.
        return Ok(None);
    };
    anyhow::ensure!(
        keyed == Some(response_message_id) || keyed_onboarding == Some(response_message_id),
        "Worker response linkage does not name the exact keyed message"
    );
    let response_provider_call_id = response_provider_call_id
        .context("Worker response linkage has no exact provider Started provenance")?;
    let (message_session, role, content_json, created_at, message_key): (
        String,
        String,
        String,
        String,
        Option<String>,
    ) = tx.query_row(
        "SELECT session_id, role, content, created_at, idempotency_key
             FROM messages WHERE id = ?1",
        [response_message_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    let content: Vec<Content> =
        serde_json::from_str(&content_json).context("decoding committed Worker response")?;
    let response_text = match content.as_slice() {
        [Content::Text { text, .. }] if !text.trim().is_empty() => text.trim(),
        _ => anyhow::bail!("committed Worker response is not one visible text block"),
    };
    anyhow::ensure!(
        message_session == session_id
            && role == "assistant"
            && (message_key.as_deref() == Some(response_key.as_str())
                || message_key.as_deref() == onboarding_key.as_deref()),
        "committed Worker response has a different session or role"
    );
    insert_or_validate_assistant_episode(
        tx,
        &session_id,
        response_message_id,
        response_text,
        &created_at,
    )
    .map_err(anyhow::Error::new)?;
    let context: HiveRunExecutionContextV1 =
        serde_json::from_str(&context_json).context("decoding committed response context")?;
    anyhow::ensure!(
        context.worker_id() == worker_id,
        "committed response context names another Worker"
    );
    match context.lane() {
        WorkerConversationLane::DirectMessage => anyhow::ensure!(
            response_group_message_id.is_none()
                && group_id.is_none()
                && group_turn_id.is_none()
                && trigger_message_id.is_none(),
            "direct Worker response carries group linkage"
        ),
        WorkerConversationLane::Group {
            group_id: context_group_id,
        } => {
            let room_message_id = response_group_message_id
                .as_deref()
                .context("group Worker response has no room projection")?;
            let turn_id = group_turn_id
                .as_deref()
                .context("group Worker response has no turn id")?;
            let trigger_id = trigger_message_id
                .as_deref()
                .context("group Worker response has no trigger message")?;
            anyhow::ensure!(
                group_id.as_deref() == Some(context_group_id.as_str()),
                "group Worker response context differs from its run"
            );
            let room_message = tx.query_row(
                "SELECT id, group_id, seq, sender_kind, sender_worker_id,
                        sender_run_id, content, reply_to_message_id, turn_id,
                        idempotency_key, created_at
                 FROM hive_group_messages WHERE id = ?1",
                [room_message_id],
                map_group_message,
            )?;
            anyhow::ensure!(
                room_message.group_id == *context_group_id
                    && room_message.sender_kind == HiveGroupSenderKind::Worker
                    && room_message.sender_worker_id.as_deref() == Some(worker_id.as_str())
                    && room_message.sender_run_id.as_deref() == Some(run_id)
                    && room_message.content == response_text
                    && room_message.reply_to_message_id.as_deref() == Some(trigger_id)
                    && room_message.turn_id.as_deref() == Some(turn_id)
                    && room_message.idempotency_key.as_deref()
                        == Some(canonical_group_response_key(turn_id, &worker_id, run_id).as_str()),
                "group room response differs from its exact canonical run response"
            );
        }
    }
    Ok(Some(CommittedWorkerResponse {
        message_id: response_message_id,
        group_message_id: response_group_message_id,
        provider_call_id: response_provider_call_id,
    }))
}

pub(crate) fn reconcile_expired_worker_response_in_transaction(
    tx: &Transaction<'_>,
    run_id: &str,
    lease_token: &str,
    lease_epoch: u64,
    now: &str,
) -> AnyResult<ExpiredWorkerResponseDisposition> {
    let worker_bound: bool = tx.query_row(
        "SELECT worker_id IS NOT NULL FROM hive_runs WHERE id = ?1",
        [run_id],
        |row| row.get(0),
    )?;
    if !worker_bound {
        return Ok(ExpiredWorkerResponseDisposition::NotWorkerBound);
    }
    let calls = load_attempt_provider_calls(tx, run_id, lease_token, lease_epoch)?;
    terminalize_durable_introduction_reviews(tx, &calls, now)?;
    let calls = load_attempt_provider_calls(tx, run_id, lease_token, lease_epoch)?;
    adopt_unlinked_onboarding_response(tx, run_id, lease_token, lease_epoch, &calls, now)?;
    let response = load_committed_worker_response(tx, run_id)?;
    if let Some(response) = response {
        let response_call = calls
            .iter()
            .find(|call| call.provider_call_id == response.provider_call_id)
            .context("committed Worker response has no exact provider Started row")?;
        anyhow::ensure!(
            matches!(
                response_call.call_kind.as_str(),
                "agent_turn" | "worker_introduction_onboarding"
            ),
            "committed Worker response provider call cannot authorize visible output"
        );
        let durable_onboarding = durable_onboarding_run(tx, run_id, &response.provider_call_id)?;
        let compatible_terminal = response_call.outcome_state.as_deref() == Some("completed")
            && response_call.remote_acceptance.as_deref() == Some("acknowledged")
            && (matches!(
                response_call.outcome.as_deref(),
                Some("completed" | "canonical_response_adopted")
            ) || (durable_onboarding
                && response_call.outcome.as_deref() == Some("semantic_invalid")));
        anyhow::ensure!(
            response_call.outcome_state.is_none() || compatible_terminal,
            "committed Worker response has an incompatible provider terminal outcome"
        );
        terminalize_unresolved_call(
            tx,
            response_call,
            "completed",
            "canonical_response_adopted",
            "acknowledged",
            None,
            now,
        )?;
        let unrelated_ambiguous = calls.iter().any(|call| {
            call.provider_call_id != response.provider_call_id
                && (call.outcome_state.is_none()
                    || call.outcome_state.as_deref() == Some("unknown")
                    || (matches!(
                        call.call_kind.as_str(),
                        "agent_turn"
                            | "worker_introduction_opening"
                            | "worker_introduction_onboarding"
                    ) && call.outcome_state.as_deref() == Some("completed")
                        && call.outcome.as_deref() == Some("completed")
                        && call.remote_acceptance.as_deref() == Some("acknowledged")))
        });
        if unrelated_ambiguous {
            for call in calls
                .iter()
                .filter(|call| call.provider_call_id != response.provider_call_id)
            {
                terminalize_unresolved_call(
                    tx,
                    call,
                    "unknown",
                    "response_missing",
                    "possibly_sent",
                    Some("provider call is not the exact canonical Worker response provenance"),
                    now,
                )?;
            }
            return Ok(ExpiredWorkerResponseDisposition::ProviderBoundaryWithoutResponse);
        }
        return Ok(ExpiredWorkerResponseDisposition::CanonicalResponseAdopted);
    }
    let unresolved = calls.iter().any(|call| call.outcome_state.is_none());
    let already_unknown = calls
        .iter()
        .any(|call| call.outcome_state.as_deref() == Some("unknown"));
    let acknowledged_visible_success_without_output = calls.iter().any(|call| {
        matches!(
            call.call_kind.as_str(),
            "agent_turn" | "worker_introduction_opening" | "worker_introduction_onboarding"
        ) && call.outcome_state.as_deref() == Some("completed")
            && call.outcome.as_deref() == Some("completed")
            && call.remote_acceptance.as_deref() == Some("acknowledged")
    });
    if acknowledged_visible_success_without_output {
        if unresolved {
            terminalize_unresolved_calls(
                tx,
                &calls,
                "unknown",
                "response_missing",
                "possibly_sent",
                Some("provider call became uncertain without a canonical Worker response"),
                now,
            )?;
        }
        return Ok(ExpiredWorkerResponseDisposition::ProviderBoundaryWithoutResponse);
    }
    if !unresolved && !already_unknown {
        // No remote ambiguity remains. This includes a run whose bounded
        // provider attempt failed semantically or at transport and was
        // terminalized before a canonical response existed.
        return Ok(ExpiredWorkerResponseDisposition::SafeBeforeProviderBoundary);
    }
    if unresolved {
        terminalize_unresolved_calls(
            tx,
            &calls,
            "unknown",
            "response_missing",
            "possibly_sent",
            Some("provider call became uncertain without a canonical Worker response"),
            now,
        )?;
    }
    Ok(ExpiredWorkerResponseDisposition::ProviderBoundaryWithoutResponse)
}

pub(crate) fn finalize_stopped_worker_conversation_in_transaction(
    tx: &Transaction<'_>,
    run_id: &str,
    lease_token: &str,
    lease_epoch: u64,
    now: &str,
) -> AnyResult<StoppedWorkerConversationFinalization> {
    if load_committed_worker_response(tx, run_id)?.is_some() {
        return match reconcile_expired_worker_response_in_transaction(
            tx,
            run_id,
            lease_token,
            lease_epoch,
            now,
        )? {
            ExpiredWorkerResponseDisposition::CanonicalResponseAdopted => {
                Ok(StoppedWorkerConversationFinalization::CanonicalResponseAdopted)
            }
            _ => anyhow::bail!(
                "stopped Worker conversation has non-canonical provider provenance beside its response"
            ),
        };
    }

    let calls = load_attempt_provider_calls(tx, run_id, lease_token, lease_epoch)?;
    for call in &calls {
        anyhow::ensure!(
            call.call_kind == "agent_turn",
            "Worker conversation Stop found a non-chat provider call"
        );
        if call.outcome_state.is_none() {
            continue;
        }
        let details = tx.query_row(
            "SELECT state, outcome, remote_acceptance, unknown_reason
             FROM hive_worker_provider_call_outcomes
             WHERE provider_call_id = ?1",
            [&call.provider_call_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )?;
        anyhow::ensure!(
            details.0 == "completed"
                && !details.1.trim().is_empty()
                && matches!(
                    details.2.as_str(),
                    "not_sent" | "possibly_sent" | "acknowledged"
                )
                && details.3.is_none(),
            "Worker conversation Stop found an incompatible provider terminal outcome"
        );
    }
    terminalize_unresolved_calls(
        tx,
        &calls,
        "completed",
        "cancelled_by_user",
        "possibly_sent",
        None,
        now,
    )?;
    Ok(StoppedWorkerConversationFinalization::Cancelled)
}

fn durable_onboarding_run(
    tx: &Transaction<'_>,
    run_id: &str,
    provider_call_id: &str,
) -> AnyResult<bool> {
    tx.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM hive_runs run
             JOIN hive_worker_introductions introduction
               ON introduction.worker_id = run.worker_id
             JOIN messages objective ON objective.id = run.objective_message_id
             JOIN hive_worker_provider_calls call
               ON call.provider_call_id = run.response_provider_call_id
             WHERE run.id = ?1 AND run.kind = 'worker_conversation'
               AND call.provider_call_id = ?2
               AND call.run_id = run.id
               AND call.call_kind IN ('agent_turn', 'worker_introduction_onboarding')
               AND run.group_id IS NULL
               AND introduction.opening_message_id IS NOT NULL
               AND objective.session_id = run.session_id
               AND objective.role = 'user'
         )",
        params![run_id, provider_call_id],
        |row| row.get(0),
    )
    .context("classifying durable Introduction onboarding run")
}

fn adopt_unlinked_onboarding_response(
    tx: &Transaction<'_>,
    run_id: &str,
    lease_token: &str,
    lease_epoch: u64,
    calls: &[ResponseProviderCall],
    now: &str,
) -> AnyResult<()> {
    let onboarding_calls = calls
        .iter()
        .filter(|call| call.call_kind == "worker_introduction_onboarding")
        .collect::<Vec<_>>();
    if onboarding_calls.is_empty() {
        return Ok(());
    }
    for call in &onboarding_calls {
        anyhow::ensure!(
            call.outcome_state.is_none()
                || (call.outcome_state.as_deref() == Some("completed")
                    && call.remote_acceptance.as_deref() == Some("acknowledged")
                    && matches!(
                        call.outcome.as_deref(),
                        Some("completed" | "semantic_invalid")
                    )),
            "Introduction onboarding call has an incompatible terminal outcome"
        );
    }
    let response_provider_call_id = onboarding_calls
        .last()
        .expect("non-empty onboarding call list")
        .provider_call_id
        .as_str();
    let binding = tx
        .query_row(
            "SELECT worker_id, session_id, objective_message_id,
                    response_message_id, response_group_message_id, kind
             FROM hive_runs
             WHERE id = ?1 AND status = 'running'
               AND lease_token = ?2 AND lease_epoch = ?3",
            params![run_id, lease_token, lease_epoch],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((
        Some(worker_id),
        Some(session_id),
        Some(objective_message_id),
        response_message_id,
        response_group_message_id,
        kind,
    )) = binding
    else {
        return Ok(());
    };
    if response_message_id.is_some() {
        return Ok(());
    }
    anyhow::ensure!(
        kind == "worker_conversation" && response_group_message_id.is_none(),
        "Introduction onboarding response is not an exact DM WorkerConversation run"
    );
    let lifecycle_exists: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM hive_worker_introductions
             WHERE worker_id = ?1 AND opening_message_id IS NOT NULL
         )",
        [&worker_id],
        |row| row.get(0),
    )?;
    anyhow::ensure!(
        lifecycle_exists,
        "Introduction onboarding response has no durable lifecycle"
    );
    let key = canonical_onboarding_response_key(&worker_id, objective_message_id);
    let message = tx
        .query_row(
            "SELECT id, role, content, created_at
             FROM messages WHERE session_id = ?1 AND idempotency_key = ?2",
            params![session_id, key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((message_id, role, content_json, created_at)) = message else {
        return Ok(());
    };
    let content: Vec<Content> = serde_json::from_str(&content_json)
        .context("decoding specialized Introduction onboarding response")?;
    let response_text = match content.as_slice() {
        [Content::Text { text, .. }] if !text.trim().is_empty() => text.trim(),
        _ => anyhow::bail!("Introduction onboarding response is not one visible text block"),
    };
    anyhow::ensure!(
        role == "assistant",
        "Introduction onboarding row is not assistant output"
    );
    insert_or_validate_assistant_episode(tx, &session_id, message_id, response_text, &created_at)
        .map_err(anyhow::Error::new)?;
    let changed = tx.execute(
        "UPDATE hive_runs
         SET response_message_id = ?4, response_provider_call_id = ?5,
             updated_at = ?6
         WHERE id = ?1 AND status = 'running'
           AND lease_token = ?2 AND lease_epoch = ?3
           AND response_message_id IS NULL
           AND response_group_message_id IS NULL
           AND response_provider_call_id IS NULL",
        params![
            run_id,
            lease_token,
            lease_epoch,
            message_id,
            response_provider_call_id,
            now
        ],
    )?;
    anyhow::ensure!(
        changed == 1,
        "Introduction onboarding response linkage changed during adoption"
    );
    Ok(())
}

fn terminalize_durable_introduction_reviews(
    tx: &Transaction<'_>,
    calls: &[ResponseProviderCall],
    now: &str,
) -> AnyResult<()> {
    for call in calls {
        if call.call_kind != "worker_introduction_review" || call.outcome_state.is_some() {
            continue;
        }
        let durable = tx
            .query_row(
                "SELECT status, last_error, usage_json
                 FROM hive_worker_introduction_reviews
                 WHERE provider_call_id = ?1 AND worker_id = ?2 AND session_id = ?3",
                params![call.provider_call_id, call.worker_id, call.session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((status, last_error, usage_json)) = durable else {
            continue;
        };
        let outcome = match status.as_str() {
            "gather_more" | "review_ready" => "canonical_review_adopted",
            "stale" => "canonical_commit_stale",
            "failed"
                if last_error
                    .as_deref()
                    .is_some_and(|error| error.starts_with("invalid reviewer output:")) =>
            {
                "semantic_invalid"
            }
            _ => continue,
        };
        let usage = usage_json
            .as_deref()
            .map(serde_json::from_str::<Usage>)
            .transpose()
            .context("decoding durable Introduction review usage")?;
        let usage_total_tokens = usage
            .as_ref()
            .map(|usage| i64::try_from(usage.total_tokens))
            .transpose()
            .context("Introduction review usage exceeds SQLite range")?;
        tx.execute(
            "INSERT INTO hive_worker_provider_call_outcomes (
                 provider_call_id, state, outcome, remote_acceptance,
                 usage_json, usage_total_tokens, estimated_cost_microunits,
                 unknown_reason, finished_at
             ) VALUES (?1, 'completed', ?2, 'acknowledged', ?3, ?4, NULL, NULL, ?5)",
            params![
                call.provider_call_id,
                outcome,
                usage_json,
                usage_total_tokens,
                now,
            ],
        )?;
    }
    Ok(())
}

pub(crate) fn reconcile_committed_introduction_provider_calls_in_transaction(
    tx: &Transaction<'_>,
    run_id: &str,
    lease_token: &str,
    lease_epoch: u64,
    now: &str,
) -> AnyResult<()> {
    let calls = load_attempt_provider_calls(tx, run_id, lease_token, lease_epoch)?;
    for call in &calls {
        anyhow::ensure!(
            call.call_kind == "worker_introduction_opening",
            "Introduction opening recovery found another provider call kind"
        );
        anyhow::ensure!(
            call.outcome_state.as_deref() != Some("unknown"),
            "committed Introduction opening follows an Unknown provider call"
        );
        anyhow::ensure!(
            call.outcome_state.is_none()
                || (call.outcome_state.as_deref() == Some("completed")
                    && matches!(
                        call.outcome.as_deref(),
                        Some("completed" | "semantic_invalid")
                    )
                    && call.remote_acceptance.as_deref() == Some("acknowledged")),
            "committed Introduction opening has an incompatible provider outcome"
        );
    }
    terminalize_unresolved_calls(
        tx,
        &calls,
        "completed",
        "canonical_response_adopted",
        "acknowledged",
        None,
        now,
    )
}

fn load_attempt_provider_calls(
    tx: &Transaction<'_>,
    run_id: &str,
    lease_token: &str,
    lease_epoch: u64,
) -> AnyResult<Vec<ResponseProviderCall>> {
    let mut statement = tx.prepare(
        "SELECT call.worker_id, call.worker_revision, call.owner_user_id,
                call.session_id, call.group_id, call.run_id,
                call.run_lease_token, call.run_lease_epoch, call.origin,
                call.lane_key, call.provider_id, call.model_id,
                call.model_key_json, call.model_catalog_revision,
                call.permission_mode, call.call_kind, outcome.state,
                outcome.outcome, outcome.remote_acceptance,
                call.provider_call_id
         FROM hive_worker_provider_calls call
         LEFT JOIN hive_worker_provider_call_outcomes outcome
           ON outcome.provider_call_id = call.provider_call_id
         WHERE call.run_id = ?1 AND call.run_lease_token = ?2
           AND call.run_lease_epoch = ?3
         ORDER BY call.started_at, call.rowid",
    )?;
    let calls = statement
        .query_map(
            params![run_id, lease_token, lease_epoch],
            map_response_provider_call,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let run_binding = tx.query_row(
        "SELECT worker_id, session_id, execution_context_json, governor_origin,
                governor_lane_key, config_json, group_id,
                (SELECT user_id FROM hive_controllers
                 WHERE id = hive_runs.controller_id)
         FROM hive_runs WHERE id = ?1",
        [run_id],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        },
    )?;
    let (Some(worker_id), Some(session_id), Some(context_json), Some(origin), Some(lane_key)) = (
        run_binding.0,
        run_binding.1,
        run_binding.2,
        run_binding.3,
        run_binding.4,
    ) else {
        anyhow::bail!("Worker provider recovery run has incomplete frozen binding")
    };
    let context: HiveRunExecutionContextV1 = serde_json::from_str(&context_json)?;
    let config: Value = serde_json::from_str(&run_binding.5)?;
    let model_key_value = config
        .get("model_key")
        .context("Worker provider recovery run has no model key")?;
    let model_key: ModelKey = serde_json::from_value(model_key_value.clone())?;
    for call in &calls {
        let expected_group = match context.lane() {
            WorkerConversationLane::DirectMessage => None,
            WorkerConversationLane::Group { group_id } => Some(group_id.as_str()),
        };
        let call_model_key: Value = serde_json::from_str(&call.model_key_json)?;
        anyhow::ensure!(
            call.worker_id == worker_id
                && call.worker_revision == context.worker_revision()
                && call.owner_user_id.as_ref() == run_binding.7.as_ref()
                && call.session_id == session_id
                && call.group_id.as_deref() == expected_group
                && call.group_id.as_ref() == run_binding.6.as_ref()
                && call.run_id == run_id
                && call.run_lease_token == lease_token
                && call.run_lease_epoch == lease_epoch
                && call.origin == origin
                && call.lane_key == lane_key
                && call.provider_id == model_key.provider.storage_key()
                && call.model_id == model_key.model_id
                && call_model_key == *model_key_value
                && call.model_catalog_revision.as_deref()
                    == config.get("model_catalog_revision").and_then(Value::as_str)
                && call.permission_mode
                    == config
                        .get("permission_mode")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
            "provider Started row differs from its frozen Worker recovery run"
        );
    }
    let unresolved = calls
        .iter()
        .filter(|call| call.outcome_state.is_none())
        .count();
    anyhow::ensure!(
        unresolved <= 1,
        "one Worker attempt has multiple unresolved provider Started rows"
    );
    Ok(calls)
}

fn terminalize_unresolved_calls(
    tx: &Transaction<'_>,
    calls: &[ResponseProviderCall],
    state: &str,
    outcome: &str,
    remote_acceptance: &str,
    unknown_reason: Option<&str>,
    now: &str,
) -> AnyResult<()> {
    for call in calls {
        terminalize_unresolved_call(
            tx,
            call,
            state,
            outcome,
            remote_acceptance,
            unknown_reason,
            now,
        )?;
    }
    Ok(())
}

fn terminalize_unresolved_call(
    tx: &Transaction<'_>,
    call: &ResponseProviderCall,
    state: &str,
    outcome: &str,
    remote_acceptance: &str,
    unknown_reason: Option<&str>,
    now: &str,
) -> AnyResult<()> {
    if call.outcome_state.is_some() {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO hive_worker_provider_call_outcomes (
             provider_call_id, state, outcome, remote_acceptance,
             usage_json, usage_total_tokens, estimated_cost_microunits,
             unknown_reason, finished_at
         ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, ?5, ?6)",
        params![
            call.provider_call_id,
            state,
            outcome,
            remote_acceptance,
            unknown_reason,
            now,
        ],
    )?;
    Ok(())
}

#[doc(hidden)]
pub fn acknowledge_worker_conversation_governor_recovery_in_transaction(
    tx: &Transaction<'_>,
    worker_id: &str,
    owner_user_id: Option<&str>,
    grant_id: &str,
    now: &str,
) -> AnyResult<WorkerConversationGovernorRecovery> {
    anyhow::ensure!(!worker_id.trim().is_empty(), "Worker id is empty");
    anyhow::ensure!(!grant_id.trim().is_empty(), "recovery grant id is empty");
    parse_utc_timestamp(now)?;
    let grant = validate_unbound_worker_governor_recovery_grant_in_transaction(
        tx,
        worker_id,
        owner_user_id,
        grant_id,
        now,
    )?;

    let controller = tx
        .query_row(
            "SELECT controller.id, session.id, controller.status
             FROM hive_workers worker
             JOIN sessions session ON session.id = worker.dm_session_id
             JOIN hive_controllers controller
               ON controller.worker_id = worker.id
              AND controller.session_id = session.id
              AND controller.user_id IS worker.user_id
             WHERE worker.id = ?1 AND worker.user_id IS ?2
               AND worker.status = 'active'
               AND session.user_id IS worker.user_id
               AND session.session_type = 'hive'",
            params![worker_id, owner_user_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
        .context("active Hive Worker has no exact private-DM controller")?;
    let (controller_id, session_id, controller_status) = controller;

    let boundaries = {
        let mut statement = tx.prepare(
            "SELECT run.id, run.kind,
                    CASE WHEN run.kind = 'worker_conversation'
                    AND run.worker_id = ?2
                    AND run.session_id = ?3
                    AND run.schedule_id IS NULL
                    AND run.occurrence_id IS NULL
                    AND run.group_id IS NULL
                    AND run.workflow_goal_id IS NULL
                    AND run.workflow_attempt_id IS NULL
                    AND run.governor_origin = 'user_dm'
                    AND run.governor_lane_key = 'dm'
                    AND run.objective_message_id IS NOT NULL
                    AND run.conversation_through_message_id
                        = run.objective_message_id
                    AND run.response_message_id IS NULL
                    AND run.response_group_message_id IS NULL
                    AND run.response_provider_call_id IS NULL
                    AND run.lease_owner IS NULL
                    AND run.lease_token IS NULL
                    AND run.lease_epoch IS NULL
                    AND run.lease_expires_at IS NULL
                    AND run.heartbeat_at IS NULL
                    AND run.finished_at IS NULL
                    AND ?4 = 'paused'
                    AND json_valid(run.execution_context_json)
                    AND json_extract(
                        run.execution_context_json, '$.mode.kind'
                    ) IN (
                        'worker_conversation_neutral',
                        'worker_workspace_attached'
                    )
                    AND json_extract(
                        run.execution_context_json, '$.mode.lane.kind'
                    ) = 'direct_message'
                    AND json_extract(
                        run.execution_context_json, '$.mode.worker_id'
                    ) = run.worker_id
                    AND json_extract(
                        run.execution_context_json, '$.mode.worker_revision'
                    ) = worker.revision
                    AND (
                        (
                            json_extract(
                                run.execution_context_json, '$.mode.kind'
                            ) = 'worker_conversation_neutral'
                            AND session.workspace_mode = 'neutral'
                            AND (session.working_dir IS NULL OR session.working_dir = '')
                            AND (session.project_dir IS NULL OR session.project_dir = '')
                        )
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
                    AND NOT EXISTS (
                        SELECT 1 FROM hive_run_attempts attempt
                        WHERE attempt.run_id = run.id
                          AND attempt.finished_at IS NULL
                    ) THEN 1 ELSE 0 END AS exact_boundary
             FROM hive_runs run
             LEFT JOIN hive_workers worker ON worker.id = run.worker_id
             LEFT JOIN sessions session ON session.id = run.session_id
             WHERE run.controller_id = ?1
               AND run.status = 'recovery_required'
             ORDER BY run.updated_at ASC, run.id ASC",
        )?;
        let boundaries = statement
            .query_map(
                params![controller_id, worker_id, session_id, controller_status],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        boundaries
    };

    if boundaries.is_empty() {
        anyhow::ensure!(
            worker_governor_recovery_grant_covers_unresolved_in_transaction(tx, grant_id, now)?,
            "Worker recovery grant does not cover the unresolved provider boundary"
        );
        return Ok(WorkerConversationGovernorRecovery::NoBoundary);
    }
    if boundaries.len() != 1 || !boundaries[0].2 {
        return Ok(WorkerConversationGovernorRecovery::UnsupportedBoundary {
            run_id: boundaries[0].0.clone(),
            kind: boundaries[0].1.clone(),
        });
    }
    let predecessor_run_id = boundaries[0].0.clone();
    anyhow::ensure!(
        unresolved_worker_governor_recovery_calls_belong_to_run_in_transaction(
            tx,
            worker_id,
            &predecessor_run_id,
            &grant.created_at,
            now,
        )?,
        "Worker recovery grant does not cover the exact direct-message boundary"
    );
    HiveRunStatus::RecoveryRequired.ensure_transition_to(HiveRunStatus::Cancelled)?;

    let outcome_json = serde_json::to_string(&serde_json::json!({
        "kind": "cancelled",
        "reason": WORKER_CONVERSATION_GOVERNOR_RECOVERY_OUTCOME,
        "governor_recovery_grant_id": grant_id,
    }))?;
    let changed = tx.execute(
        "UPDATE hive_runs
         SET status = 'cancelled', last_stop_reason = ?2,
             last_error = NULL, outcome_json = ?3,
             finished_at = ?4, updated_at = ?4
         WHERE id = ?1 AND status = 'recovery_required'",
        params![
            predecessor_run_id,
            WORKER_CONVERSATION_GOVERNOR_RECOVERY_REASON,
            outcome_json,
            now,
        ],
    )?;
    anyhow::ensure!(
        changed == 1,
        "Worker recovery boundary changed during owner acknowledgment"
    );
    tx.execute(
        "UPDATE hive_control_outbox
         SET status = 'discarded',
             last_error = 'Worker conversation recovery acknowledged before control delivery',
             updated_at = ?2
         WHERE run_id = ?1 AND status = 'pending'",
        params![predecessor_run_id, now],
    )?;

    reactivate_worker_conversation_controller_after_governor_recovery_in_transaction(
        tx,
        &predecessor_run_id,
        now,
    )?;
    let materialized = materialize_oldest_staged_input_with_governor_recovery_in_transaction(
        tx,
        &predecessor_run_id,
        grant_id,
        &grant.created_at,
        now,
    )?;
    finalize_worker_conversation_after_governor_recovery_in_transaction(
        tx,
        &predecessor_run_id,
        materialized
            .as_ref()
            .map(|successor| successor.assigned_run_id.as_str()),
        now,
    )?;
    Ok(WorkerConversationGovernorRecovery::Recovered {
        predecessor_run_id,
        session_id,
        materialized_run_id: materialized.map(|successor| successor.assigned_run_id),
    })
}

#[doc(hidden)]
pub fn acknowledge_worker_conversation_response_loss_in_transaction(
    tx: &Transaction<'_>,
    worker_id: &str,
    owner_user_id: Option<&str>,
    recovery_grant_id: Option<&str>,
    now: &str,
) -> AnyResult<WorkerConversationGovernorRecovery> {
    anyhow::ensure!(!worker_id.trim().is_empty(), "Worker id is empty");
    parse_utc_timestamp(now)?;
    let recovery_grant = if let Some(grant_id) = recovery_grant_id {
        let grant = validate_unbound_worker_governor_recovery_grant_in_transaction(
            tx,
            worker_id,
            owner_user_id,
            grant_id,
            now,
        )?;
        anyhow::ensure!(
            worker_governor_recovery_grant_covers_unresolved_in_transaction(tx, grant_id, now)?,
            "Worker recovery grant does not cover the older unresolved provider boundary"
        );
        Some(grant)
    } else {
        anyhow::ensure!(
            !worker_has_unacknowledged_unresolved_provider_calls_in_transaction(
                tx, worker_id, now,
            )?,
            "Worker has an older unresolved provider boundary that requires a recovery grant"
        );
        None
    };

    let controller = tx
        .query_row(
            "SELECT controller.id, session.id, controller.status
             FROM hive_workers worker
             JOIN sessions session ON session.id = worker.dm_session_id
             JOIN hive_controllers controller
               ON controller.worker_id = worker.id
              AND controller.session_id = session.id
              AND controller.user_id IS worker.user_id
             WHERE worker.id = ?1 AND worker.user_id IS ?2
               AND worker.status = 'active'
               AND session.user_id IS worker.user_id
               AND session.session_type = 'hive'",
            params![worker_id, owner_user_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
        .context("active Hive Worker has no exact private-DM controller")?;
    let (controller_id, session_id, controller_status) = controller;

    let boundaries = {
        let mut statement = tx.prepare(
            "SELECT run.id, run.kind,
                    CASE WHEN run.kind = 'worker_conversation'
                    AND run.worker_id = ?2
                    AND run.session_id = ?3
                    AND run.schedule_id IS NULL
                    AND run.occurrence_id IS NULL
                    AND run.group_id IS NULL
                    AND run.workflow_goal_id IS NULL
                    AND run.workflow_attempt_id IS NULL
                    AND run.governor_origin = 'user_dm'
                    AND run.governor_lane_key = 'dm'
                    AND run.objective_message_id IS NOT NULL
                    AND run.conversation_through_message_id
                        = run.objective_message_id
                    AND run.response_message_id IS NULL
                    AND run.response_group_message_id IS NULL
                    AND run.response_provider_call_id IS NULL
                    AND run.lease_owner IS NULL
                    AND run.lease_token IS NULL
                    AND run.lease_epoch IS NULL
                    AND run.lease_expires_at IS NULL
                    AND run.heartbeat_at IS NULL
                    AND run.finished_at IS NULL
                    AND ?4 = 'paused'
                    AND json_valid(run.execution_context_json)
                    AND json_extract(
                        run.execution_context_json, '$.mode.kind'
                    ) IN (
                        'worker_conversation_neutral',
                        'worker_workspace_attached'
                    )
                    AND json_extract(
                        run.execution_context_json, '$.mode.lane.kind'
                    ) = 'direct_message'
                    AND json_extract(
                        run.execution_context_json, '$.mode.worker_id'
                    ) = run.worker_id
                    AND json_extract(
                        run.execution_context_json, '$.mode.worker_revision'
                    ) = worker.revision
                    AND (
                        (
                            json_extract(
                                run.execution_context_json, '$.mode.kind'
                            ) = 'worker_conversation_neutral'
                            AND session.workspace_mode = 'neutral'
                            AND (session.working_dir IS NULL OR session.working_dir = '')
                            AND (session.project_dir IS NULL OR session.project_dir = '')
                        )
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
                    AND NOT EXISTS (
                        SELECT 1 FROM hive_run_attempts attempt
                        WHERE attempt.run_id = run.id
                          AND attempt.finished_at IS NULL
                    )
                    AND (
                        SELECT COUNT(*)
                        FROM hive_worker_provider_calls call
                        WHERE call.run_id = run.id
                    ) = 1
                    AND EXISTS (
                        SELECT 1
                        FROM hive_worker_provider_calls call
                        JOIN hive_worker_provider_call_outcomes outcome
                          ON outcome.provider_call_id = call.provider_call_id
                        WHERE call.run_id = run.id
                          AND call.worker_id = worker.id
                          AND call.worker_revision = worker.revision
                          AND call.owner_user_id IS worker.user_id
                          AND call.session_id = session.id
                          AND call.group_id IS NULL
                          AND call.workflow_goal_id IS NULL
                          AND call.workflow_attempt_id IS NULL
                          AND call.origin = 'user_dm'
                          AND call.lane_key = 'dm'
                          AND call.call_kind = 'agent_turn'
                          AND outcome.state = 'completed'
                          AND outcome.outcome = 'completed'
                          AND outcome.remote_acceptance = 'acknowledged'
                    ) THEN 1 ELSE 0 END AS exact_boundary
             FROM hive_runs run
             LEFT JOIN hive_workers worker ON worker.id = run.worker_id
             LEFT JOIN sessions session ON session.id = run.session_id
             WHERE run.controller_id = ?1
               AND run.status = 'recovery_required'
             ORDER BY run.updated_at ASC, run.id ASC",
        )?;
        let boundaries = statement
            .query_map(
                params![controller_id, worker_id, session_id, controller_status],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        boundaries
    };

    if boundaries.is_empty() {
        return Ok(WorkerConversationGovernorRecovery::NoBoundary);
    }
    if boundaries.len() != 1 || !boundaries[0].2 {
        return Ok(WorkerConversationGovernorRecovery::UnsupportedBoundary {
            run_id: boundaries[0].0.clone(),
            kind: boundaries[0].1.clone(),
        });
    }
    let predecessor_run_id = boundaries[0].0.clone();
    HiveRunStatus::RecoveryRequired.ensure_transition_to(HiveRunStatus::Cancelled)?;
    let mut outcome = serde_json::json!({
        "kind": "cancelled",
        "reason": WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_OUTCOME,
    });
    if let Some(grant) = recovery_grant.as_ref() {
        outcome["governor_recovery_grant_id"] = Value::String(grant.id.clone());
    }
    let outcome_json = serde_json::to_string(&outcome)?;
    let changed = tx.execute(
        "UPDATE hive_runs
         SET status = 'cancelled', last_stop_reason = ?2,
             last_error = NULL, outcome_json = ?3,
             finished_at = ?4, updated_at = ?4
         WHERE id = ?1 AND status = 'recovery_required'",
        params![
            predecessor_run_id,
            WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_REASON,
            outcome_json,
            now,
        ],
    )?;
    anyhow::ensure!(
        changed == 1,
        "Worker response-loss boundary changed during owner acknowledgment"
    );
    tx.execute(
        "UPDATE hive_control_outbox
         SET status = 'discarded',
             last_error = 'Worker response-loss recovery acknowledged before control delivery',
             updated_at = ?2
         WHERE run_id = ?1 AND status = 'pending'",
        params![predecessor_run_id, now],
    )?;

    reactivate_worker_conversation_controller_after_governor_recovery_in_transaction(
        tx,
        &predecessor_run_id,
        now,
    )?;
    let materialized = materialize_oldest_staged_input_with_response_loss_recovery_in_transaction(
        tx,
        &predecessor_run_id,
        recovery_grant
            .as_ref()
            .map(|grant| (grant.id.as_str(), grant.created_at.as_str())),
        now,
    )?;
    finalize_worker_conversation_after_governor_recovery_in_transaction(
        tx,
        &predecessor_run_id,
        materialized
            .as_ref()
            .map(|successor| successor.assigned_run_id.as_str()),
        now,
    )?;
    Ok(WorkerConversationGovernorRecovery::Recovered {
        predecessor_run_id,
        session_id,
        materialized_run_id: materialized.map(|successor| successor.assigned_run_id),
    })
}

pub fn materialize_oldest_staged_input_in_transaction(
    tx: &Transaction<'_>,
    completed_run_id: &str,
    now: &str,
) -> AnyResult<Option<MaterializedWorkerConversationInput>> {
    materialize_oldest_staged_input_with_authority_in_transaction(
        tx,
        completed_run_id,
        WorkerConversationPredecessorAuthority::CanonicalCompletion,
        now,
    )
}

#[doc(hidden)]
pub fn materialize_oldest_staged_input_with_authority_in_transaction(
    tx: &Transaction<'_>,
    completed_run_id: &str,
    authority: WorkerConversationPredecessorAuthority,
    now: &str,
) -> AnyResult<Option<MaterializedWorkerConversationInput>> {
    materialize_oldest_staged_input_with_authority_internal(
        tx,
        completed_run_id,
        authority,
        None,
        true,
        now,
    )
}

fn materialize_oldest_staged_input_with_governor_recovery_in_transaction(
    tx: &Transaction<'_>,
    completed_run_id: &str,
    grant_id: &str,
    grant_created_at: &str,
    now: &str,
) -> AnyResult<Option<MaterializedWorkerConversationInput>> {
    materialize_oldest_staged_input_with_authority_internal(
        tx,
        completed_run_id,
        WorkerConversationPredecessorAuthority::TerminalWithoutCanonicalResponse,
        Some((grant_id, grant_created_at)),
        true,
        now,
    )
}

fn materialize_oldest_staged_input_with_response_loss_recovery_in_transaction(
    tx: &Transaction<'_>,
    completed_run_id: &str,
    recovery_grant: Option<(&str, &str)>,
    now: &str,
) -> AnyResult<Option<MaterializedWorkerConversationInput>> {
    materialize_oldest_staged_input_with_authority_internal(
        tx,
        completed_run_id,
        WorkerConversationPredecessorAuthority::AcknowledgedProviderResponseLoss,
        recovery_grant,
        recovery_grant.is_some(),
        now,
    )
}

fn materialize_oldest_staged_input_with_authority_internal(
    tx: &Transaction<'_>,
    completed_run_id: &str,
    authority: WorkerConversationPredecessorAuthority,
    governor_recovery: Option<(&str, &str)>,
    allow_successor_recovery_grant: bool,
    now: &str,
) -> AnyResult<Option<MaterializedWorkerConversationInput>> {
    let completed = tx
        .query_row(
            "SELECT controller_id, session_id, worker_id, config_json, priority,
                    concurrency_key, max_attempts, execution_context_json,
                    status, response_message_id, kind,
                    response_group_message_id, response_provider_call_id,
                    governor_origin, governor_lane_key, last_stop_reason,
                    schedule_id, occurrence_id, group_id,
                    workflow_goal_id, workflow_attempt_id
             FROM hive_runs WHERE id = ?1",
            [completed_run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i32>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Option<String>>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, Option<String>>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, Option<String>>(18)?,
                    row.get::<_, Option<String>>(19)?,
                    row.get::<_, Option<String>>(20)?,
                ))
            },
        )
        .optional()?
        .context("completed Worker run disappeared during staged-input promotion")?;
    let (Some(session_id), Some(worker_id), Some(context_json)) =
        (completed.1, completed.2, completed.7)
    else {
        return Ok(None);
    };
    let canonical_completion = completed.8 == "succeeded"
        && (completed.9.is_some() || completed.10 == "worker_introduction_review");
    let stopped_conversation = completed.8 == "cancelled"
        && completed.9.is_none()
        && completed.10 == "worker_conversation"
        && completed.11.is_none()
        && completed.12.is_none()
        && completed.13.as_deref() == Some("user_dm")
        && completed.14.as_deref() == Some("dm")
        && completed.15.as_deref() == Some(WORKER_CONVERSATION_STOP_REQUESTED_REASON);
    let terminal_without_canonical_response =
        matches!(completed.8.as_str(), "failed" | "dead_letter" | "cancelled")
            && completed.9.is_none()
            && completed.10 == "worker_conversation"
            && completed.11.is_none()
            && completed.12.is_none()
            && completed.13.as_deref() == Some("user_dm")
            && completed.14.as_deref() == Some("dm")
            && completed.16.is_none()
            && completed.17.is_none()
            && completed.18.is_none()
            && completed.19.is_none()
            && completed.20.is_none();
    let terminal_provider_boundary_resolved = if terminal_without_canonical_response {
        tx.query_row(
            "SELECT NOT EXISTS (
                 SELECT 1
                 FROM hive_worker_provider_calls call
                 LEFT JOIN hive_worker_provider_call_outcomes outcome
                   ON outcome.provider_call_id = call.provider_call_id
                 WHERE call.run_id = ?1
                   AND (
                       outcome.provider_call_id IS NULL
                       OR outcome.state = 'unknown'
                       OR (
                           call.call_kind IN (
                               'agent_turn', 'worker_introduction_opening',
                               'worker_introduction_onboarding'
                           )
                           AND outcome.state = 'completed'
                           AND outcome.outcome = 'completed'
                           AND outcome.remote_acceptance = 'acknowledged'
                       )
                   )
             )",
            [completed_run_id],
            |row| row.get::<_, bool>(0),
        )?
    } else {
        false
    };
    let governor_recovery_allowed = if let Some((grant_id, grant_created_at)) = governor_recovery {
        tx.query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM hive_runs predecessor
                 JOIN hive_workers worker ON worker.id = predecessor.worker_id
                 JOIN hive_worker_governor_override_grants grant_row
                   ON grant_row.id = ?2
                 WHERE predecessor.id = ?1
                   AND predecessor.status = 'cancelled'
                   AND predecessor.kind = 'worker_conversation'
                   AND predecessor.schedule_id IS NULL
                   AND predecessor.occurrence_id IS NULL
                   AND predecessor.group_id IS NULL
                   AND predecessor.workflow_goal_id IS NULL
                   AND predecessor.workflow_attempt_id IS NULL
                   AND predecessor.governor_origin = 'user_dm'
                   AND predecessor.governor_lane_key = 'dm'
                   AND predecessor.response_message_id IS NULL
                   AND predecessor.response_group_message_id IS NULL
                   AND predecessor.response_provider_call_id IS NULL
                   AND predecessor.last_stop_reason = ?4
                   AND json_valid(predecessor.outcome_json)
                   AND json_extract(
                       predecessor.outcome_json, '$.kind'
                   ) = 'cancelled'
                   AND json_extract(
                       predecessor.outcome_json, '$.reason'
                   ) = ?5
                   AND json_extract(
                       predecessor.outcome_json, '$.governor_recovery_grant_id'
                   ) = grant_row.id
                   AND grant_row.worker_id = worker.id
                   AND grant_row.owner_user_id IS worker.user_id
                   AND grant_row.bypass_unresolved_provider_call = 1
                   AND grant_row.bypass_daily_call_cap = 0
                   AND grant_row.bypass_daily_token_cap = 0
                   AND grant_row.bypass_quiet_hours = 0
                   AND grant_row.bypass_idle_backoff = 0
                   AND grant_row.created_at = ?3
                   AND predecessor.finished_at >= grant_row.created_at
             )",
            params![
                completed_run_id,
                grant_id,
                grant_created_at,
                WORKER_CONVERSATION_GOVERNOR_RECOVERY_REASON,
                WORKER_CONVERSATION_GOVERNOR_RECOVERY_OUTCOME,
            ],
            |row| row.get::<_, bool>(0),
        )?
    } else {
        false
    };
    let acknowledged_response_loss_allowed = if matches!(
        authority,
        WorkerConversationPredecessorAuthority::AcknowledgedProviderResponseLoss
    ) {
        tx.query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM hive_runs predecessor
                 JOIN hive_workers worker ON worker.id = predecessor.worker_id
                 JOIN sessions session ON session.id = predecessor.session_id
                 JOIN hive_controllers controller
                   ON controller.id = predecessor.controller_id
                 WHERE predecessor.id = ?1
                   AND predecessor.status = 'cancelled'
                   AND predecessor.kind = 'worker_conversation'
                   AND predecessor.schedule_id IS NULL
                   AND predecessor.occurrence_id IS NULL
                   AND predecessor.group_id IS NULL
                   AND predecessor.workflow_goal_id IS NULL
                   AND predecessor.workflow_attempt_id IS NULL
                   AND predecessor.governor_origin = 'user_dm'
                   AND predecessor.governor_lane_key = 'dm'
                   AND predecessor.objective_message_id IS NOT NULL
                   AND predecessor.conversation_through_message_id
                       = predecessor.objective_message_id
                   AND predecessor.response_message_id IS NULL
                   AND predecessor.response_group_message_id IS NULL
                   AND predecessor.response_provider_call_id IS NULL
                   AND predecessor.lease_owner IS NULL
                   AND predecessor.lease_token IS NULL
                   AND predecessor.lease_epoch IS NULL
                   AND predecessor.lease_expires_at IS NULL
                   AND predecessor.heartbeat_at IS NULL
                   AND predecessor.finished_at IS NOT NULL
                   AND predecessor.last_stop_reason = ?2
                   AND json_valid(predecessor.outcome_json)
                   AND json_extract(predecessor.outcome_json, '$.kind') = 'cancelled'
                   AND json_extract(predecessor.outcome_json, '$.reason') = ?3
                   AND (
                       (?4 IS NULL AND json_type(
                           predecessor.outcome_json,
                           '$.governor_recovery_grant_id'
                       ) IS NULL)
                       OR json_extract(
                           predecessor.outcome_json,
                           '$.governor_recovery_grant_id'
                       ) = ?4
                   )
                   AND worker.status = 'active'
                   AND worker.dm_session_id = predecessor.session_id
                   AND worker.user_id IS session.user_id
                   AND session.session_type = 'hive'
                   AND controller.worker_id = worker.id
                   AND controller.session_id = session.id
                   AND controller.user_id IS worker.user_id
                   AND controller.status = 'active'
                   AND NOT EXISTS (
                       SELECT 1 FROM hive_run_attempts attempt
                       WHERE attempt.run_id = predecessor.id
                         AND attempt.finished_at IS NULL
                   )
                   AND (
                       SELECT COUNT(*) FROM hive_worker_provider_calls call
                       WHERE call.run_id = predecessor.id
                   ) = 1
                   AND EXISTS (
                       SELECT 1
                       FROM hive_worker_provider_calls call
                       JOIN hive_worker_provider_call_outcomes outcome
                         ON outcome.provider_call_id = call.provider_call_id
                       WHERE call.run_id = predecessor.id
                         AND call.worker_id = worker.id
                         AND call.worker_revision = worker.revision
                         AND call.owner_user_id IS worker.user_id
                         AND call.session_id = session.id
                         AND call.group_id IS NULL
                         AND call.workflow_goal_id IS NULL
                         AND call.workflow_attempt_id IS NULL
                         AND call.origin = 'user_dm'
                         AND call.lane_key = 'dm'
                         AND call.call_kind = 'agent_turn'
                         AND outcome.state = 'completed'
                         AND outcome.outcome = 'completed'
                         AND outcome.remote_acceptance = 'acknowledged'
                   )
             )",
            params![
                completed_run_id,
                WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_REASON,
                WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_OUTCOME,
                governor_recovery.map(|(grant_id, _)| grant_id),
            ],
            |row| row.get::<_, bool>(0),
        )?
    } else {
        false
    };
    let predecessor_allowed = match authority {
        WorkerConversationPredecessorAuthority::CanonicalCompletion => canonical_completion,
        WorkerConversationPredecessorAuthority::StoppedWorkerConversation => stopped_conversation,
        WorkerConversationPredecessorAuthority::TerminalWithoutCanonicalResponse => {
            governor_recovery_allowed
                || (terminal_without_canonical_response && terminal_provider_boundary_resolved)
        }
        WorkerConversationPredecessorAuthority::AcknowledgedProviderResponseLoss => {
            acknowledged_response_loss_allowed
        }
    };
    if !predecessor_allowed {
        return Ok(None);
    }
    let context: HiveRunExecutionContextV1 = if completed.10 == "worker_introduction_review" {
        let worker_revision: i64 = tx.query_row(
            "SELECT revision FROM hive_workers WHERE id = ?1",
            [&worker_id],
            |row| row.get(0),
        )?;
        HiveRunExecutionContextV1::worker_conversation_neutral(
            worker_id.clone(),
            u64::try_from(worker_revision).context("Worker revision is negative")?,
            WorkerConversationLane::DirectMessage,
        )?
    } else {
        serde_json::from_str(&context_json)?
    };
    if context.worker_id() != worker_id
        || !matches!(context.lane(), WorkerConversationLane::DirectMessage)
    {
        return Ok(None);
    }
    if matches!(
        authority,
        WorkerConversationPredecessorAuthority::StoppedWorkerConversation
            | WorkerConversationPredecessorAuthority::TerminalWithoutCanonicalResponse
            | WorkerConversationPredecessorAuthority::AcknowledgedProviderResponseLoss
    ) {
        let worker_current: bool = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM hive_workers worker
                 WHERE worker.id = ?1 AND worker.revision = ?2
                   AND worker.dm_session_id = ?3
             )",
            params![worker_id, context.worker_revision(), session_id],
            |row| row.get(0),
        )?;
        let context_current = match &context.mode {
            HiveRunExecutionModeV1::WorkerConversationNeutral { .. } => tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sessions
                     WHERE id = ?1 AND workspace_mode = 'neutral'
                       AND (working_dir IS NULL OR working_dir = '')
                       AND (project_dir IS NULL OR project_dir = '')
                 )",
                [&session_id],
                |row| row.get(0),
            )?,
            HiveRunExecutionModeV1::WorkerWorkspaceAttached {
                workspace_mode,
                working_dir,
                project_dir,
                ..
            } => tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sessions
                     WHERE id = ?1 AND workspace_mode = ?2
                       AND working_dir = ?3 AND project_dir IS ?4
                 )",
                params![
                    session_id,
                    workspace_mode.to_string(),
                    working_dir,
                    project_dir,
                ],
                |row| row.get(0),
            )?,
            _ => false,
        };
        if !worker_current || !context_current {
            return Ok(None);
        }
    }
    let staged = tx
        .query_row(
            "WITH RECURSIVE conversation_chain(run_id) AS (
                 SELECT ?3
                 UNION
                 SELECT ledger.accepted_while_run_id
                 FROM hive_worker_conversation_inputs ledger
                 JOIN conversation_chain chain
                   ON ledger.assigned_run_id = chain.run_id
                 WHERE ledger.worker_id = ?1 AND ledger.session_id = ?2
                   AND ledger.owner_user_id IS (
                       SELECT user_id FROM hive_workers WHERE id = ?1
                   )
                   AND ledger.state = 'materialized'
                 UNION
                 SELECT ledger.assigned_run_id
                 FROM hive_worker_conversation_inputs ledger
                 JOIN conversation_chain chain
                   ON ledger.accepted_while_run_id = chain.run_id
                 WHERE ledger.worker_id = ?1 AND ledger.session_id = ?2
                   AND ledger.owner_user_id IS (
                       SELECT user_id FROM hive_workers WHERE id = ?1
                   )
                   AND ledger.state = 'materialized'
                   AND ledger.assigned_run_id IS NOT NULL
             ),
             component_tail(run_id) AS (
                 SELECT COALESCE(
                     (
                         SELECT ledger.assigned_run_id
                         FROM hive_worker_conversation_inputs ledger
                         JOIN conversation_chain component
                           ON component.run_id = ledger.accepted_while_run_id
                         WHERE ledger.worker_id = ?1
                           AND ledger.session_id = ?2
                           AND ledger.owner_user_id IS (
                               SELECT user_id FROM hive_workers WHERE id = ?1
                           )
                           AND ledger.state = 'materialized'
                           AND ledger.assigned_run_id IS NOT NULL
                         ORDER BY ledger.canonical_message_id DESC
                         LIMIT 1
                     ),
                     ?3
                 )
             )
             SELECT id, request_id, content_json
             FROM hive_worker_conversation_inputs input
             WHERE input.worker_id = ?1 AND input.session_id = ?2
               AND input.owner_user_id IS (
                   SELECT user_id FROM hive_workers WHERE id = ?1
               )
               AND input.state = 'staged'
               AND ?3 = (SELECT run_id FROM component_tail)
               AND input.accepted_while_run_id IN (
                   SELECT run_id FROM conversation_chain
               )
               AND EXISTS (
                   SELECT 1 FROM hive_runs accepted
                   WHERE accepted.id = input.accepted_while_run_id
                     AND (
                         (accepted.status = 'succeeded' AND (
                             accepted.response_message_id IS NOT NULL
                             OR accepted.kind = 'worker_introduction_review'
                         ))
                         OR (
                             accepted.status = 'cancelled'
                             AND accepted.kind = 'worker_conversation'
                             AND accepted.schedule_id IS NULL
                             AND accepted.occurrence_id IS NULL
                             AND accepted.group_id IS NULL
                             AND accepted.workflow_goal_id IS NULL
                             AND accepted.workflow_attempt_id IS NULL
                             AND accepted.governor_origin = 'user_dm'
                             AND accepted.governor_lane_key = 'dm'
                             AND accepted.response_message_id IS NULL
                             AND accepted.response_group_message_id IS NULL
                             AND accepted.response_provider_call_id IS NULL
                             AND accepted.last_stop_reason = ?4
                             AND json_valid(accepted.outcome_json)
                             AND json_extract(
                                 accepted.outcome_json, '$.kind'
                             ) = 'cancelled'
                             AND json_extract(
                                 accepted.outcome_json, '$.reason'
                             ) = ?5
                             AND EXISTS (
                                 SELECT 1
                                 FROM hive_worker_governor_override_grants recovery_grant
                                 WHERE recovery_grant.id = json_extract(
                                     accepted.outcome_json,
                                     '$.governor_recovery_grant_id'
                                 )
                                   AND recovery_grant.worker_id = accepted.worker_id
                                   AND recovery_grant.owner_user_id IS (
                                       SELECT recovery_worker.user_id
                                       FROM hive_workers recovery_worker
                                       WHERE recovery_worker.id = accepted.worker_id
                                   )
                                   AND recovery_grant.bypass_unresolved_provider_call = 1
                                   AND recovery_grant.bypass_daily_call_cap = 0
                                   AND recovery_grant.bypass_daily_token_cap = 0
                                   AND recovery_grant.bypass_quiet_hours = 0
                                   AND recovery_grant.bypass_idle_backoff = 0
                                   AND recovery_grant.created_at <= accepted.finished_at
                                   AND EXISTS (
                                       SELECT 1
                                       FROM hive_worker_provider_calls recovery_call
                                       LEFT JOIN hive_worker_provider_call_outcomes recovery_outcome
                                         ON recovery_outcome.provider_call_id
                                            = recovery_call.provider_call_id
                                       WHERE recovery_call.run_id = accepted.id
                                         AND (
                                             recovery_outcome.provider_call_id IS NULL
                                             OR recovery_outcome.state = 'unknown'
                                         )
                                   )
                                   AND NOT EXISTS (
                                       SELECT 1
                                       FROM hive_worker_provider_calls late_recovery_call
                                       LEFT JOIN hive_worker_provider_call_outcomes late_recovery_outcome
                                         ON late_recovery_outcome.provider_call_id
                                            = late_recovery_call.provider_call_id
                                       WHERE late_recovery_call.run_id = accepted.id
                                         AND (
                                             late_recovery_outcome.provider_call_id IS NULL
                                             OR late_recovery_outcome.state = 'unknown'
                                         )
                                         AND late_recovery_call.started_at
                                             >= recovery_grant.created_at
                                   )
                             )
                             AND json_valid(accepted.execution_context_json)
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.kind'
                             ) IN (
                                 'worker_conversation_neutral',
                                 'worker_workspace_attached'
                             )
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.lane.kind'
                             ) = 'direct_message'
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.worker_id'
                             ) = accepted.worker_id
                             AND EXISTS (
                                 SELECT 1
                                 FROM hive_workers recovery_worker
                                 JOIN sessions recovery_session
                                   ON recovery_session.id = accepted.session_id
                                 WHERE recovery_worker.id = accepted.worker_id
                                   AND recovery_worker.dm_session_id = accepted.session_id
                                   AND json_extract(
                                       accepted.execution_context_json,
                                       '$.mode.worker_revision'
                                   ) = recovery_worker.revision
                                   AND (
                                       (
                                           json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.kind'
                                           ) = 'worker_conversation_neutral'
                                           AND recovery_session.workspace_mode = 'neutral'
                                           AND (
                                               recovery_session.working_dir IS NULL
                                               OR recovery_session.working_dir = ''
                                           )
                                           AND (
                                               recovery_session.project_dir IS NULL
                                               OR recovery_session.project_dir = ''
                                           )
                                       )
                                       OR (
                                           recovery_session.workspace_mode = json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.workspace_mode'
                                           )
                                           AND recovery_session.working_dir = json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.working_dir'
                                           )
                                           AND recovery_session.project_dir IS json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.project_dir'
                                           )
                                       )
                                   )
                             )
                         )
                         OR (
                             accepted.status = 'cancelled'
                             AND accepted.kind = 'worker_conversation'
                             AND accepted.schedule_id IS NULL
                             AND accepted.occurrence_id IS NULL
                             AND accepted.group_id IS NULL
                             AND accepted.workflow_goal_id IS NULL
                             AND accepted.workflow_attempt_id IS NULL
                             AND accepted.governor_origin = 'user_dm'
                             AND accepted.governor_lane_key = 'dm'
                             AND accepted.response_message_id IS NULL
                             AND accepted.response_group_message_id IS NULL
                             AND accepted.response_provider_call_id IS NULL
                             AND accepted.last_stop_reason = ?6
                             AND json_valid(accepted.outcome_json)
                             AND json_extract(
                                 accepted.outcome_json, '$.kind'
                             ) = 'cancelled'
                             AND json_extract(
                                 accepted.outcome_json, '$.reason'
                             ) = ?7
                             AND (
                                 SELECT COUNT(*)
                                 FROM hive_worker_provider_calls response_loss_call
                                 WHERE response_loss_call.run_id = accepted.id
                             ) = 1
                             AND EXISTS (
                                 SELECT 1
                                 FROM hive_worker_provider_calls response_loss_call
                                 JOIN hive_worker_provider_call_outcomes response_loss_outcome
                                   ON response_loss_outcome.provider_call_id
                                      = response_loss_call.provider_call_id
                                 JOIN hive_workers response_loss_worker
                                   ON response_loss_worker.id
                                      = response_loss_call.worker_id
                                 WHERE response_loss_call.run_id = accepted.id
                                   AND response_loss_call.worker_id = accepted.worker_id
                                   AND response_loss_call.worker_revision
                                       = response_loss_worker.revision
                                   AND response_loss_call.owner_user_id
                                       IS response_loss_worker.user_id
                                   AND response_loss_call.session_id = accepted.session_id
                                   AND response_loss_call.group_id IS NULL
                                   AND response_loss_call.workflow_goal_id IS NULL
                                   AND response_loss_call.workflow_attempt_id IS NULL
                                   AND response_loss_call.origin = 'user_dm'
                                   AND response_loss_call.lane_key = 'dm'
                                   AND response_loss_call.call_kind = 'agent_turn'
                                   AND response_loss_outcome.state = 'completed'
                                   AND response_loss_outcome.outcome = 'completed'
                                   AND response_loss_outcome.remote_acceptance
                                       = 'acknowledged'
                             )
                             AND json_valid(accepted.execution_context_json)
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.kind'
                             ) IN (
                                 'worker_conversation_neutral',
                                 'worker_workspace_attached'
                             )
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.lane.kind'
                             ) = 'direct_message'
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.worker_id'
                             ) = accepted.worker_id
                             AND EXISTS (
                                 SELECT 1
                                 FROM hive_workers response_loss_worker
                                 JOIN sessions response_loss_session
                                   ON response_loss_session.id = accepted.session_id
                                 WHERE response_loss_worker.id = accepted.worker_id
                                   AND response_loss_worker.status = 'active'
                                   AND response_loss_worker.dm_session_id
                                       = accepted.session_id
                                   AND response_loss_worker.user_id
                                       IS response_loss_session.user_id
                                   AND json_extract(
                                       accepted.execution_context_json,
                                       '$.mode.worker_revision'
                                   ) = response_loss_worker.revision
                                   AND (
                                       (
                                           json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.kind'
                                           ) = 'worker_conversation_neutral'
                                           AND response_loss_session.workspace_mode = 'neutral'
                                           AND (
                                               response_loss_session.working_dir IS NULL
                                               OR response_loss_session.working_dir = ''
                                           )
                                           AND (
                                               response_loss_session.project_dir IS NULL
                                               OR response_loss_session.project_dir = ''
                                           )
                                       )
                                       OR (
                                           response_loss_session.workspace_mode = json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.workspace_mode'
                                           )
                                           AND response_loss_session.working_dir = json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.working_dir'
                                           )
                                           AND response_loss_session.project_dir IS json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.project_dir'
                                           )
                                       )
                                   )
                             )
                         )
                         OR (
                             accepted.status IN ('failed', 'dead_letter', 'cancelled')
                             AND accepted.kind = 'worker_conversation'
                             AND accepted.schedule_id IS NULL
                             AND accepted.occurrence_id IS NULL
                             AND accepted.group_id IS NULL
                             AND accepted.workflow_goal_id IS NULL
                             AND accepted.workflow_attempt_id IS NULL
                             AND accepted.governor_origin = 'user_dm'
                             AND accepted.governor_lane_key = 'dm'
                             AND accepted.response_message_id IS NULL
                             AND accepted.response_group_message_id IS NULL
                             AND accepted.response_provider_call_id IS NULL
                             AND NOT EXISTS (
                                 SELECT 1
                                 FROM hive_worker_provider_calls accepted_call
                                 LEFT JOIN hive_worker_provider_call_outcomes accepted_outcome
                                   ON accepted_outcome.provider_call_id
                                      = accepted_call.provider_call_id
                                 WHERE accepted_call.run_id = accepted.id
                                   AND (
                                       accepted_outcome.provider_call_id IS NULL
                                       OR accepted_outcome.state = 'unknown'
                                       OR (
                                           accepted_call.call_kind IN (
                                               'agent_turn',
                                               'worker_introduction_opening',
                                               'worker_introduction_onboarding'
                                           )
                                           AND accepted_outcome.state = 'completed'
                                           AND accepted_outcome.outcome = 'completed'
                                           AND accepted_outcome.remote_acceptance
                                               = 'acknowledged'
                                       )
                                   )
                             )
                             AND json_valid(accepted.execution_context_json)
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.kind'
                             ) IN (
                                 'worker_conversation_neutral',
                                 'worker_workspace_attached'
                             )
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.lane.kind'
                             ) = 'direct_message'
                             AND json_extract(
                                 accepted.execution_context_json, '$.mode.worker_id'
                             ) = accepted.worker_id
                             AND EXISTS (
                                 SELECT 1
                                 FROM hive_workers accepted_worker
                                 JOIN sessions accepted_session
                                   ON accepted_session.id = accepted.session_id
                                 WHERE accepted_worker.id = accepted.worker_id
                                   AND accepted_worker.dm_session_id = accepted.session_id
                                   AND json_extract(
                                       accepted.execution_context_json,
                                       '$.mode.worker_revision'
                                   ) = accepted_worker.revision
                                   AND (
                                       (
                                           json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.kind'
                                           ) = 'worker_conversation_neutral'
                                           AND accepted_session.workspace_mode = 'neutral'
                                           AND (
                                               accepted_session.working_dir IS NULL
                                               OR accepted_session.working_dir = ''
                                           )
                                           AND (
                                               accepted_session.project_dir IS NULL
                                               OR accepted_session.project_dir = ''
                                           )
                                       )
                                       OR (
                                           accepted_session.workspace_mode = json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.workspace_mode'
                                           )
                                           AND accepted_session.working_dir = json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.working_dir'
                                           )
                                           AND accepted_session.project_dir IS json_extract(
                                               accepted.execution_context_json,
                                               '$.mode.project_dir'
                                           )
                                       )
                                   )
                             )
                         )
                     )
               )
             ORDER BY input.accepted_at ASC, input.id ASC LIMIT 1",
            params![
                worker_id,
                session_id,
                completed_run_id,
                WORKER_CONVERSATION_GOVERNOR_RECOVERY_REASON,
                WORKER_CONVERSATION_GOVERNOR_RECOVERY_OUTCOME,
                WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_REASON,
                WORKER_CONVERSATION_RESPONSE_LOSS_RECOVERY_OUTCOME,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((input_id, request_id, content_json)) = staged else {
        return Ok(None);
    };
    let content: Vec<Content> =
        serde_json::from_str(&content_json).context("decoding staged Worker input")?;
    let body = match content.as_slice() {
        [Content::Text { text, .. }] if !text.trim().is_empty() => text.clone(),
        _ => anyhow::bail!("staged Worker input is not one canonical text block"),
    };
    let message_key = canonical_input_message_key(&request_id);
    let existing_message = tx
        .query_row(
            "SELECT id, role, content FROM messages
             WHERE session_id = ?1 AND idempotency_key = ?2",
            params![session_id, message_key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let message_id = if let Some((message_id, role, existing_content)) = existing_message {
        anyhow::ensure!(
            role == "user" && existing_content == content_json,
            "staged Worker input key belongs to different canonical content"
        );
        message_id
    } else {
        tx.execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES (?1, 'user', ?2, ?3, ?4)",
            params![session_id, content_json, now, message_key],
        )?;
        tx.last_insert_rowid()
    };
    insert_user_episode(tx, &session_id, message_id, &body, now)?;

    let assigned_run_id = staged_successor_run_id(&input_id, &request_id);
    let context_lane_key = context.lane().canonical_lane_key()?;
    anyhow::ensure!(
        context.worker_id() == worker_id && context_lane_key == "dm",
        "staged Worker successor context is not the exact DM Worker"
    );
    let config: Value = serde_json::from_str(&completed.3)?;
    let successor_context_json = serde_json::to_string(&context)?;
    anyhow::ensure!(
        config.get("model").and_then(Value::as_str).is_some()
            && config
                .get("model_key")
                .is_some_and(|value| !value.is_null())
            && config
                .get("permission_mode")
                .and_then(Value::as_str)
                .is_some(),
        "completed Worker run has no reusable frozen model binding"
    );
    let run_exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM hive_runs WHERE id = ?1)",
        [&assigned_run_id],
        |row| row.get(0),
    )?;
    anyhow::ensure!(!run_exists, "staged Worker successor id already exists");
    tx.execute(
        "INSERT INTO hive_runs (
             id, controller_id, session_id, kind, objective, config_json,
             status, priority, concurrency_key, available_at, attempt_count,
             max_attempts, created_at, updated_at, worker_id,
             objective_message_id, governor_origin, governor_lane_key,
             execution_context_json, conversation_through_message_id
         ) VALUES (
             ?1, ?2, ?3, 'worker_conversation', ?4, ?5, 'queued', ?6, ?7,
             ?8, 0, ?9, ?8, ?8, ?10, ?11, 'user_dm', 'dm', ?12, ?11
         )",
        params![
            assigned_run_id,
            completed.0,
            session_id,
            body,
            completed.3,
            completed.4,
            completed.5,
            now,
            completed.6,
            worker_id,
            message_id,
            successor_context_json,
        ],
    )?;
    let bound_recovery_grant = if allow_successor_recovery_grant {
        match transfer_worker_governor_recovery_grant_to_successor_in_transaction(
            tx,
            completed_run_id,
            &assigned_run_id,
        )? {
            Some(grant_id) => Some(grant_id),
            None => bind_worker_governor_recovery_grant_to_run_in_transaction(
                tx,
                &assigned_run_id,
                now,
            )?,
        }
    } else {
        None
    };
    if let Some((grant_id, _)) = governor_recovery {
        anyhow::ensure!(
            bound_recovery_grant.as_deref() == Some(grant_id),
            "materialized Worker recovery successor did not bind its exact grant"
        );
    }
    let changed = tx.execute(
        "UPDATE hive_worker_conversation_inputs
         SET state = 'materialized', canonical_message_id = ?2,
             assigned_run_id = ?3, materialized_at = ?4
         WHERE id = ?1 AND state = 'staged'",
        params![input_id, message_id, assigned_run_id, now],
    )?;
    anyhow::ensure!(changed == 1, "staged Worker input changed during promotion");
    tx.execute(
        "UPDATE sessions
         SET updated_at = CASE WHEN updated_at < ?2 THEN ?2 ELSE updated_at END
         WHERE id = ?1",
        params![session_id, now],
    )?;
    Ok(Some(MaterializedWorkerConversationInput {
        input_id,
        canonical_message_id: message_id,
        assigned_run_id,
    }))
}

fn staged_successor_run_id(input_id: &str, request_id: &str) -> String {
    let digest = hash_request_bytes([input_id.as_bytes(), &[0], request_id.as_bytes()].concat());
    format!("worker-staged-{digest}")
}

fn expected_origin_for_kind(kind: &str) -> Option<&'static [&'static str]> {
    match kind {
        "worker_conversation" => Some(&["user_dm"]),
        "group_turn" => Some(&["user_group", "scheduled_group"]),
        "worker_message" => Some(&["worker_peer"]),
        "worker_heartbeat" => Some(&["heartbeat"]),
        "scheduled" => Some(&["scheduled"]),
        // Introduction uses its specialized opening/onboarding persistence
        // keys and is deliberately excluded from this ordinary response API.
        _ => None,
    }
}

pub(crate) fn canonical_response_message_key(run_id: &str) -> String {
    format!("worker-run:{run_id}:assistant:final")
}

fn canonical_onboarding_response_key(worker_id: &str, objective_message_id: i64) -> String {
    format!("introduction:{worker_id}:user:{objective_message_id}:context-response")
}

pub(crate) fn canonical_group_response_key(turn_id: &str, worker_id: &str, run_id: &str) -> String {
    format!("group-turn:{turn_id}:worker:{worker_id}:run:{run_id}:final")
}

fn validate_commit_input(
    input: &CommitWorkerConversationResponse,
) -> Result<(), WorkerConversationResponseCommitError> {
    for (value, label) in [
        (input.worker_id.as_str(), "Worker id"),
        (input.session_id.as_str(), "session id"),
        (input.run_id.as_str(), "run id"),
        (input.run_lease_token.as_str(), "run lease token"),
        (input.provider_call_id.as_str(), "provider call id"),
    ] {
        if value.trim().is_empty()
            || value.len() > MAX_RESPONSE_ID_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(conflict(format!("invalid {label}")));
        }
    }
    if input.worker_revision == 0 {
        return Err(conflict("Worker revision is zero"));
    }
    if input.run_lease_epoch > i64::MAX as u64 {
        return Err(conflict("run lease epoch exceeds the SQLite range"));
    }
    let response_text = input.response_text.trim();
    if response_text.is_empty() || response_text.len() > MAX_RESPONSE_TEXT_BYTES {
        return Err(conflict("Worker response text is empty or too large"));
    }
    input
        .lane
        .canonical_lane_key()
        .map_err(|error| conflict(format!("invalid Worker response lane: {error:#}")))?;
    Ok(())
}

fn nonnegative(row: &Row<'_>, index: usize) -> rusqlite::Result<i64> {
    let value = row.get::<_, i64>(index)?;
    if value < 0 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            std::io::Error::new(std::io::ErrorKind::InvalidData, "negative durable integer").into(),
        ));
    }
    Ok(value)
}

fn optional_nonnegative(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Integer,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "negative durable integer",
                    )
                    .into(),
                )
            })
        })
        .transpose()
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value[..boundary].to_string()
}

fn bounded_reason(reason: impl Into<String>) -> String {
    truncate_utf8(&reason.into(), 1_024)
}

fn stale(reason: impl Into<String>) -> WorkerConversationResponseCommitError {
    WorkerConversationResponseCommitError::StaleRejected(bounded_reason(reason))
}

fn conflict(reason: impl Into<String>) -> WorkerConversationResponseCommitError {
    WorkerConversationResponseCommitError::ConflictOrCorrupt(bounded_reason(reason))
}

fn read_conflict(
    operation: &'static str,
) -> impl FnOnce(rusqlite::Error) -> WorkerConversationResponseCommitError {
    move |error| conflict(format!("{operation}: {error}"))
}

fn write_conflict(
    operation: &'static str,
) -> impl FnOnce(rusqlite::Error) -> WorkerConversationResponseCommitError {
    move |error| conflict(format!("{operation}: {error}"))
}
