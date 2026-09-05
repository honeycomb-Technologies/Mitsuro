use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::Value;

use crate::ai::types::Content;
use crate::hive::{canonical_timestamp, HiveRunStatus};
use crate::storage::{
    bind_worker_governor_recovery_grant_to_run_in_transaction, hash_request_bytes,
    resolve_worker_conversation_with_conn, update_derived_state_for_run_in_transaction,
    HiveRunExecutionContextV1, HiveRunExecutionModeV1, HiveWorkerStatus,
    StageWorkerConversationInput, StageWorkerConversationInputResult, WorkerConversationInput,
    WorkerConversationLane, WorkspaceMode,
};

use super::stage_worker_conversation_input_in_transaction;

const MAX_USER_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_STAGED_CONTENT_JSON_BYTES: usize = 256 * 1024;
const SUPERSEDED_PRE_PROVIDER_REVIEW_REASON: &str =
    "pre-provider stale: superseded by newer accepted user input";
pub const WORKER_DM_BLOCKED_BY_NON_CONVERSATION_RUN_PREFIX: &str =
    "Worker direct message is blocked by non-conversation run";

/// Complete authoritative inputs for accepting one direct Worker message.
/// The caller chooses identifiers, priority, and retry cap; core validates all
/// identity/capability fields and decides atomically whether this starts a run
/// or is staged behind the one unfinished lane occupant.
#[derive(Debug, Clone)]
pub struct AcceptWorkerConversationInput {
    pub input_id: String,
    pub request_id: String,
    pub worker_id: String,
    pub owner_user_id: Option<String>,
    pub session_id: String,
    pub controller_id: String,
    pub body: String,
    pub accepted_at: DateTime<Utc>,
    pub new_run_id: String,
    pub run_config: Value,
    pub execution_context: HiveRunExecutionContextV1,
    pub priority: i32,
    pub concurrency_key: Option<String>,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptWorkerConversationInputResult {
    Queued {
        run_id: String,
        message_id: i64,
    },
    Staged {
        active_run_id: String,
        input: Box<WorkerConversationInput>,
    },
}

pub fn accept_worker_conversation_input_in_transaction(
    tx: &Transaction<'_>,
    input: &AcceptWorkerConversationInput,
) -> Result<AcceptWorkerConversationInputResult> {
    validate_accept_input(input)?;
    reject_non_conversation_recovery_boundary(tx, input)?;
    validate_current_dm_binding(tx, input)?;
    let accepted_at = canonical_timestamp(input.accepted_at);
    let message_key = canonical_input_message_key(&input.request_id);
    let content_json = serde_json::to_string(&vec![Content::text(input.body.clone())])?;
    ensure!(
        content_json.len() <= MAX_STAGED_CONTENT_JSON_BYTES,
        "encoded Worker message exceeds the staged-input byte limit"
    );

    if let Some((
        existing_id,
        existing_worker_id,
        existing_owner_user_id,
        existing_content,
        existing_state,
        accepted_while_run_id,
        canonical_message_id,
        assigned_run_id,
        existing_accepted_at,
    )) = tx
        .query_row(
            "SELECT id, worker_id, owner_user_id, content_json, state,
                    accepted_while_run_id, canonical_message_id,
                    assigned_run_id, accepted_at
             FROM hive_worker_conversation_inputs
             WHERE session_id = ?1 AND request_id = ?2",
            params![input.session_id, input.request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()?
    {
        ensure!(
            existing_id == input.input_id
                && existing_worker_id == input.worker_id
                && existing_owner_user_id == input.owner_user_id
                && existing_content == content_json
                && existing_accepted_at == accepted_at,
            "Worker input request was reused with different content or binding"
        );
        ensure_supported_staging_predecessor(tx, input, &accepted_while_run_id, false)?;
        return match existing_state.as_str() {
            "staged" => {
                let staged = stage_worker_conversation_input_in_transaction(
                    tx,
                    &StageWorkerConversationInput {
                        id: input.input_id.clone(),
                        worker_id: input.worker_id.clone(),
                        owner_user_id: input.owner_user_id.clone(),
                        session_id: input.session_id.clone(),
                        request_id: input.request_id.clone(),
                        accepted_while_run_id: accepted_while_run_id.clone(),
                        body: input.body.clone(),
                        accepted_at: input.accepted_at,
                    },
                )?;
                let staged = match staged {
                    StageWorkerConversationInputResult::Inserted(input)
                    | StageWorkerConversationInputResult::Existing(input) => input,
                };
                Ok(AcceptWorkerConversationInputResult::Staged {
                    active_run_id: accepted_while_run_id,
                    input: Box::new(staged),
                })
            }
            "materialized" => Ok(AcceptWorkerConversationInputResult::Queued {
                run_id: assigned_run_id.context("materialized Worker input has no assigned run")?,
                message_id: canonical_message_id
                    .context("materialized Worker input has no canonical message")?,
            }),
            other => anyhow::bail!("invalid Worker conversation input state: {other}"),
        };
    }

    if let Some((message_id, existing_content, run_id)) = tx
        .query_row(
            "SELECT message.id, message.content, run.id
             FROM messages message
             JOIN hive_runs run ON run.objective_message_id = message.id
             WHERE message.session_id = ?1 AND message.idempotency_key = ?2
               AND run.kind = 'worker_conversation'",
            params![input.session_id, message_key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
    {
        ensure!(
            existing_content == content_json && run_id == input.new_run_id,
            "Worker input idempotency key was reused with different content or run"
        );
        return Ok(AcceptWorkerConversationInputResult::Queued { run_id, message_id });
    }

    supersede_introduction_review_for_user_input(
        tx,
        &input.worker_id,
        &input.session_id,
        &accepted_at,
    )?;

    let unfinished = {
        let mut statement = tx.prepare(
            "SELECT id, kind, status FROM hive_runs
             WHERE controller_id = ?1 AND worker_id = ?2 AND session_id = ?3
               AND status IN (
                   'queued', 'leased', 'running', 'sleeping', 'retry_wait',
                   'recovery_required', 'awaiting_input'
               )
             ORDER BY created_at ASC, id ASC",
        )?;
        let rows = statement
            .query_map(
                params![input.controller_id, input.worker_id, input.session_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    ensure!(
        unfinished.len() <= 1,
        "Worker DM lane has multiple unfinished runs"
    );
    let unfinished = unfinished.into_iter().next();
    if let Some((active_run_id, _active_kind, active_status)) = unfinished {
        ensure_supported_staging_predecessor(tx, input, &active_run_id, true)?;
        ensure!(
            active_status != "awaiting_input",
            "Worker run is awaiting an explicit UserResponse"
        );
        let staged = stage_worker_conversation_input_in_transaction(
            tx,
            &StageWorkerConversationInput {
                id: input.input_id.clone(),
                worker_id: input.worker_id.clone(),
                owner_user_id: input.owner_user_id.clone(),
                session_id: input.session_id.clone(),
                request_id: input.request_id.clone(),
                accepted_while_run_id: active_run_id.clone(),
                body: input.body.clone(),
                accepted_at: input.accepted_at,
            },
        )?;
        let staged = match staged {
            StageWorkerConversationInputResult::Inserted(input)
            | StageWorkerConversationInputResult::Existing(input) => input,
        };
        return Ok(AcceptWorkerConversationInputResult::Staged {
            active_run_id,
            input: Box::new(staged),
        });
    }

    tx.execute(
        "INSERT INTO messages (
             session_id, role, content, created_at, idempotency_key
         ) VALUES (?1, 'user', ?2, ?3, ?4)",
        params![input.session_id, content_json, accepted_at, message_key],
    )?;
    let message_id = tx.last_insert_rowid();
    insert_user_episode(tx, &input.session_id, message_id, &input.body, &accepted_at)?;
    insert_worker_conversation_run(tx, input, message_id, &accepted_at)?;
    bind_worker_governor_recovery_grant_to_run_in_transaction(tx, &input.new_run_id, &accepted_at)?;
    tx.execute(
        "UPDATE sessions SET updated_at = ?2 WHERE id = ?1",
        params![input.session_id, accepted_at],
    )?;
    Ok(AcceptWorkerConversationInputResult::Queued {
        run_id: input.new_run_id.clone(),
        message_id,
    })
}

/// Recovery pauses the controller, so the full current-binding validator
/// cannot classify a specialized recovery occupant. Detect that exact owned
/// lane first so callers receive the same stable non-conversation conflict as
/// queued/running specialized work, without mutating or exposing another lane.
fn reject_non_conversation_recovery_boundary(
    tx: &Transaction<'_>,
    input: &AcceptWorkerConversationInput,
) -> Result<()> {
    let blocked = tx
        .query_row(
            "SELECT run.id, run.kind
             FROM hive_runs run
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN sessions session ON session.id = run.session_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE run.controller_id = ?1
               AND run.worker_id = ?2
               AND run.session_id = ?3
               AND run.status = 'recovery_required'
               AND run.kind <> 'worker_conversation'
               AND worker.user_id IS ?4
               AND worker.dm_session_id = session.id
               AND session.user_id IS worker.user_id
               AND session.session_type = 'hive'
               AND controller.worker_id = worker.id
               AND controller.session_id = session.id
               AND controller.user_id IS worker.user_id
               AND controller.status IN ('active', 'paused')
             ORDER BY run.updated_at ASC, run.id ASC
             LIMIT 1",
            params![
                input.controller_id,
                input.worker_id,
                input.session_id,
                input.owner_user_id,
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((run_id, kind)) = blocked {
        anyhow::bail!("{WORKER_DM_BLOCKED_BY_NON_CONVERSATION_RUN_PREFIX} {run_id} ({kind})");
    }
    Ok(())
}

/// A real user reply always wins over a queued/running background review.
/// Queued work is cancelled immediately. A leased/running review retains its
/// scheduler lease but its audit is terminally stale before the input is
/// staged, so no later provider output can become a current proposal.
fn supersede_introduction_review_for_user_input(
    tx: &Transaction<'_>,
    worker_id: &str,
    session_id: &str,
    now: &str,
) -> Result<()> {
    tx.execute(
        "UPDATE hive_worker_introduction_reviews
         SET status = 'stale', last_error = ?3,
             completed_at = ?4, updated_at = ?4
         WHERE worker_id = ?1 AND session_id = ?2
           AND run_id IS NOT NULL AND status IN ('queued', 'claimed')
           AND EXISTS (
               SELECT 1 FROM hive_runs run
               WHERE run.id = hive_worker_introduction_reviews.run_id
                 AND run.kind = 'worker_introduction_review'
                 AND run.status IN ('queued', 'leased', 'running')
           )",
        params![
            worker_id,
            session_id,
            SUPERSEDED_PRE_PROVIDER_REVIEW_REASON,
            now
        ],
    )?;
    let queued_run_ids = {
        let mut statement = tx.prepare(
            "SELECT run.id
             FROM hive_runs run
             JOIN hive_worker_introduction_reviews review ON review.run_id = run.id
             WHERE run.worker_id = ?1 AND run.session_id = ?2
               AND run.kind = 'worker_introduction_review' AND run.status = 'queued'
               AND review.status = 'stale' AND review.last_error = ?3",
        )?;
        let rows = statement
            .query_map(
                params![worker_id, session_id, SUPERSEDED_PRE_PROVIDER_REVIEW_REASON],
                |row| row.get::<_, String>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    tx.execute(
        "UPDATE hive_runs
         SET status = 'cancelled',
             last_stop_reason = ?3, last_error = NULL,
             outcome_json = json_object(
                 'kind', 'cancelled', 'reason', 'user_input_superseded'
             ),
             finished_at = ?4, updated_at = ?4
         WHERE worker_id = ?1 AND session_id = ?2
           AND kind = 'worker_introduction_review' AND status = 'queued'
           AND EXISTS (
               SELECT 1 FROM hive_worker_introduction_reviews review
               WHERE review.run_id = hive_runs.id
                 AND review.status = 'stale' AND review.last_error = ?3
           )",
        params![
            worker_id,
            session_id,
            SUPERSEDED_PRE_PROVIDER_REVIEW_REASON,
            now
        ],
    )?;
    for run_id in queued_run_ids {
        update_derived_state_for_run_in_transaction(tx, &run_id, HiveRunStatus::Cancelled, now)?;
    }
    Ok(())
}

/// Only predecessors with a terminal path that deterministically drains the
/// staged DM ledger may accept another user message. The stale Introduction
/// review case is the one specialized exception: it is provider-free and its
/// scheduler recovery path is required to terminalize it as succeeded.
fn ensure_supported_staging_predecessor(
    tx: &Transaction<'_>,
    input: &AcceptWorkerConversationInput,
    run_id: &str,
    require_current_context: bool,
) -> Result<()> {
    let context_json = serde_json::to_string(&input.execution_context)?;
    let (kind, supported): (String, bool) = tx
        .query_row(
            "SELECT run.kind,
                    COALESCE((
                        (
                            run.kind = 'worker_conversation'
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
                            AND (?8 = 0 OR run.execution_context_json = ?6)
                        )
                        OR (
                            run.kind = 'worker_introduction_review'
                            AND run.status IN ('leased', 'running', 'succeeded')
                            AND run.schedule_id IS NULL
                            AND run.occurrence_id IS NULL
                            AND run.group_id IS NULL
                            AND run.workflow_goal_id IS NULL
                            AND run.workflow_attempt_id IS NULL
                            AND run.governor_origin = 'user_lifecycle_action'
                            AND run.governor_lane_key = 'dm'
                            AND run.objective_message_id IS NULL
                            AND run.response_message_id IS NULL
                            AND run.response_group_message_id IS NULL
                            AND run.response_provider_call_id IS NULL
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
                            AND (?8 = 0 OR run.execution_context_json = ?6)
                            AND EXISTS (
                                SELECT 1
                                FROM hive_worker_introduction_reviews review
                                WHERE review.run_id = run.id
                                  AND review.worker_id = run.worker_id
                                  AND review.session_id = run.session_id
                                  AND review.status = 'stale'
                                  AND review.provider_call_id IS NULL
                                  AND review.last_error = ?7
                            )
                            AND NOT EXISTS (
                                SELECT 1 FROM hive_worker_provider_calls call
                                WHERE call.run_id = run.id
                            )
                        )
                    ), 0)
             FROM hive_runs run
             JOIN hive_controllers controller ON controller.id = run.controller_id
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN sessions session ON session.id = run.session_id
             WHERE run.id = ?1
               AND run.controller_id = ?2
               AND run.worker_id = ?3
               AND run.session_id = ?4
               AND worker.user_id IS ?5
               AND worker.status = 'active'
               AND worker.dm_session_id = session.id
               AND session.user_id IS worker.user_id
               AND session.session_type = 'hive'
               AND controller.worker_id = worker.id
               AND controller.session_id = session.id
               AND controller.user_id IS worker.user_id
               AND controller.status = 'active'",
            params![
                run_id,
                input.controller_id,
                input.worker_id,
                input.session_id,
                input.owner_user_id,
                context_json,
                SUPERSEDED_PRE_PROVIDER_REVIEW_REASON,
                require_current_context,
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .with_context(|| format!("Worker DM predecessor run {run_id} is missing"))?;
    if !supported {
        anyhow::bail!("{WORKER_DM_BLOCKED_BY_NON_CONVERSATION_RUN_PREFIX} {run_id} ({kind})");
    }
    Ok(())
}

pub(crate) fn insert_worker_conversation_run(
    tx: &Transaction<'_>,
    input: &AcceptWorkerConversationInput,
    objective_message_id: i64,
    now: &str,
) -> Result<()> {
    let config_json = serde_json::to_string(&input.run_config)?;
    let context_json = serde_json::to_string(&input.execution_context)?;
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
            input.new_run_id,
            input.controller_id,
            input.session_id,
            input.body,
            config_json,
            input.priority,
            input.concurrency_key,
            now,
            input.max_attempts,
            input.worker_id,
            objective_message_id,
            context_json,
        ],
    )?;
    Ok(())
}

pub(crate) fn canonical_input_message_key(request_id: &str) -> String {
    format!("worker-request:{request_id}:canonical")
}

pub(crate) fn insert_user_episode(
    tx: &Transaction<'_>,
    session_id: &str,
    message_id: i64,
    body: &str,
    occurred_at: &str,
) -> Result<()> {
    let normalized = body.split_whitespace().collect::<Vec<_>>().join(" ");
    let normalized = truncate_utf8(&normalized, 16 * 1024);
    tx.execute(
        "INSERT INTO conversation_episodes (
             session_id, source_message_id, role, body, content_hash, occurred_at
         ) VALUES (?1, ?2, 'user', ?3, ?4, ?5)
         ON CONFLICT(session_id, source_message_id) DO UPDATE SET
             role = excluded.role, body = excluded.body,
             content_hash = excluded.content_hash, occurred_at = excluded.occurred_at",
        params![
            session_id,
            message_id,
            normalized,
            hash_request_bytes([b"user".as_slice(), &[0], normalized.as_bytes()].concat()),
            occurred_at,
        ],
    )?;
    Ok(())
}

fn validate_current_dm_binding(
    tx: &Transaction<'_>,
    input: &AcceptWorkerConversationInput,
) -> Result<()> {
    let binding = resolve_worker_conversation_with_conn(tx, &input.session_id)?
        .context("Worker DM session has no Worker binding")?;
    ensure!(
        binding.group_id.is_none(),
        "Worker message target is a group lane"
    );
    ensure!(
        binding.worker.id == input.worker_id,
        "Worker DM binding changed"
    );
    ensure!(
        binding.worker.user_id == input.owner_user_id,
        "Worker message owner mismatch"
    );
    ensure!(
        binding.worker.status == HiveWorkerStatus::Active,
        "Worker is not active"
    );
    ensure!(
        input.execution_context.worker_id() == binding.worker.id
            && input.execution_context.worker_revision() == binding.worker.revision
            && matches!(
                input.execution_context.lane(),
                WorkerConversationLane::DirectMessage
            ),
        "Worker execution context is stale or belongs to another lane"
    );
    input.execution_context.validate()?;

    let (session_user_id, session_type, working_dir, project_dir, workspace_mode): (
        Option<String>,
        String,
        Option<String>,
        Option<String>,
        String,
    ) = tx.query_row(
        "SELECT user_id, session_type, working_dir, project_dir, workspace_mode
         FROM sessions WHERE id = ?1",
        [&input.session_id],
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
    ensure!(
        session_user_id == input.owner_user_id,
        "Worker session owner mismatch"
    );
    ensure!(
        session_type == "hive",
        "Worker conversation is not a Hive session"
    );
    let parsed_workspace_mode = workspace_mode
        .parse::<WorkspaceMode>()
        .map_err(anyhow::Error::msg)?;
    match &input.execution_context.mode {
        HiveRunExecutionModeV1::WorkerConversationNeutral { .. } => ensure!(
            parsed_workspace_mode == WorkspaceMode::Neutral
                && working_dir.as_deref().is_none_or(str::is_empty)
                && project_dir.as_deref().is_none_or(str::is_empty)
                && input
                    .run_config
                    .get("working_dir")
                    .is_none_or(Value::is_null)
                && input
                    .run_config
                    .get("project_dir")
                    .is_none_or(Value::is_null),
            "neutral Worker run carries a filesystem workspace"
        ),
        HiveRunExecutionModeV1::WorkerWorkspaceAttached {
            workspace_mode,
            working_dir: frozen_working_dir,
            project_dir: frozen_project_dir,
            ..
        } => ensure!(
            parsed_workspace_mode == *workspace_mode
                && working_dir.as_deref() == Some(frozen_working_dir.as_str())
                && project_dir.as_deref() == frozen_project_dir.as_deref()
                && input.run_config.get("working_dir").and_then(Value::as_str)
                    == Some(frozen_working_dir.as_str())
                && input.run_config.get("project_dir").and_then(Value::as_str)
                    == frozen_project_dir.as_deref(),
            "attached Worker workspace binding changed"
        ),
        HiveRunExecutionModeV1::WorkerGoal { .. }
        | HiveRunExecutionModeV1::WorkerGoalAcceptance { .. } => {
            anyhow::bail!("Worker Goal authority cannot stage ordinary conversation input")
        }
    }

    let controller_current: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM hive_controllers
             WHERE id = ?1 AND worker_id = ?2 AND session_id = ?3
               AND user_id IS ?4 AND status = 'active'
         )",
        params![
            input.controller_id,
            input.worker_id,
            input.session_id,
            input.owner_user_id,
        ],
        |row| row.get(0),
    )?;
    ensure!(controller_current, "Worker controller binding is stale");

    let configured_model = input.run_config.get("model").and_then(Value::as_str);
    let configured_model_key = input
        .run_config
        .get("model_key")
        .filter(|value| !value.is_null());
    let worker_model_key = binding
        .worker
        .model_key
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    ensure!(
        configured_model == binding.worker.model.as_deref()
            && configured_model_key == worker_model_key.as_ref()
            && input
                .run_config
                .get("model_catalog_revision")
                .and_then(Value::as_str)
                == binding.worker.model_catalog_revision.as_deref()
            && input
                .run_config
                .get("permission_mode")
                .and_then(Value::as_str)
                == Some(binding.worker.permission_mode.as_str()),
        "Worker run model or permission binding is stale"
    );
    Ok(())
}

fn validate_accept_input(input: &AcceptWorkerConversationInput) -> Result<()> {
    for (value, label) in [
        (input.input_id.as_str(), "input id"),
        (input.request_id.as_str(), "request id"),
        (input.worker_id.as_str(), "Worker id"),
        (input.session_id.as_str(), "session id"),
        (input.controller_id.as_str(), "controller id"),
        (input.new_run_id.as_str(), "run id"),
    ] {
        ensure!(!value.trim().is_empty(), "{label} is empty");
        ensure!(value.len() <= 256, "{label} exceeds the byte limit");
        ensure!(
            !value.chars().any(char::is_control),
            "{label} contains control characters"
        );
    }
    ensure!(!input.body.trim().is_empty(), "Worker message is empty");
    ensure!(
        input.body.len() <= MAX_USER_MESSAGE_BYTES,
        "Worker message is too large"
    );
    ensure!(input.max_attempts > 0, "Worker run max_attempts is zero");
    ensure!(
        input
            .concurrency_key
            .as_deref()
            .is_none_or(|value| !value.trim().is_empty()),
        "Worker run concurrency key is empty"
    );
    ensure!(
        input
            .execution_context
            .lane()
            .canonical_lane_key()?
            .as_str()
            == "dm",
        "direct Worker message has a non-DM lane"
    );
    Ok(())
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
