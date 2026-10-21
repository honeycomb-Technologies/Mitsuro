use std::io::{Error as IoError, ErrorKind};

use anyhow::{ensure, Context, Result};
use rusqlite::{params, OptionalExtension, Row, Transaction, TransactionBehavior};

use crate::ai::types::Content;
use crate::hive::canonical_timestamp;
use crate::storage::{
    Database, HiveRunExecutionContextV1, HiveWorkerStatus, WorkerConversationLane,
};

use super::{
    StageWorkerConversationInput, StageWorkerConversationInputResult, WorkerConversationInput,
    WorkerConversationInputState,
};

const INPUT_COLUMNS: &str = "id, worker_id, owner_user_id, session_id, request_id, accepted_while_run_id, content_json, state, canonical_message_id, assigned_run_id, accepted_at, materialized_at";
const MAX_INPUT_ID_BYTES: usize = 256;
const MAX_INPUT_BODY_BYTES: usize = 64 * 1024;
const MAX_INPUT_CONTENT_JSON_BYTES: usize = 256 * 1024;

pub struct HiveWorkerConversationInputStore {
    db: Database,
}

impl HiveWorkerConversationInputStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    pub fn stage(
        &self,
        input: &StageWorkerConversationInput,
    ) -> Result<StageWorkerConversationInputResult> {
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let result = stage_worker_conversation_input_in_transaction(&tx, input)?;
        tx.commit()?;
        Ok(result)
    }

    pub fn get(&self, id: &str) -> Result<Option<WorkerConversationInput>> {
        let sql =
            format!("SELECT {INPUT_COLUMNS} FROM hive_worker_conversation_inputs WHERE id = ?1");
        self.db
            .conn()
            .query_row(&sql, [id], map_input)
            .optional()
            .context("reading staged Worker conversation input")
    }

    pub fn list_staged_for_run(&self, run_id: &str) -> Result<Vec<WorkerConversationInput>> {
        let sql = format!(
            "SELECT {INPUT_COLUMNS}
             FROM hive_worker_conversation_inputs
             WHERE accepted_while_run_id = ?1 AND state = 'staged'
             ORDER BY accepted_at ASC, id ASC"
        );
        let mut statement = self.db.conn().prepare(&sql)?;
        let rows = statement
            .query_map([run_id], map_input)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("listing staged Worker conversation input")?;
        Ok(rows)
    }
}

/// Persist user input accepted while a Worker response is active. This is
/// intentionally transaction-aware so the Hive command idempotency row and
/// input acceptance can commit together.
pub fn stage_worker_conversation_input_in_transaction(
    tx: &Transaction<'_>,
    input: &StageWorkerConversationInput,
) -> Result<StageWorkerConversationInputResult> {
    validate_input(input)?;
    let accepted_at = canonical_timestamp(input.accepted_at);
    let content_json = serde_json::to_string(&vec![Content::text(input.body.clone())])?;
    ensure!(
        content_json.len() <= MAX_INPUT_CONTENT_JSON_BYTES,
        "encoded Worker conversation input exceeds the byte limit"
    );

    if let Some(existing) =
        load_by_id_or_request(tx, &input.id, &input.session_id, &input.request_id)?
    {
        ensure_existing_matches(&existing, input, &accepted_at)?;
        return Ok(StageWorkerConversationInputResult::Existing(existing));
    }

    let run_binding = tx
        .query_row(
            "SELECT run.execution_context_json, worker.status
             FROM hive_runs run
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             JOIN sessions session ON session.id = run.session_id
             WHERE run.id = ?1 AND run.worker_id = ?2 AND run.session_id = ?3
               AND run.status IN (
                   'queued', 'leased', 'running', 'sleeping', 'retry_wait',
                   'recovery_required'
               )
               AND worker.status = 'active'
               AND worker.dm_session_id = run.session_id
               AND worker.user_id IS ?4
               AND controller.worker_id = worker.id
               AND controller.session_id = session.id
               AND controller.user_id IS worker.user_id
               AND controller.status = 'active'
               AND session.user_id IS worker.user_id
               AND session.session_type = 'hive'",
            params![
                input.accepted_while_run_id,
                input.worker_id,
                input.session_id,
                input.owner_user_id,
            ],
            |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .context("validating active Worker conversation for staged input")?;
    let Some((Some(execution_context_json), status)) = run_binding else {
        anyhow::bail!("staged input is not bound to an active owned Worker DM run")
    };
    ensure!(
        HiveWorkerStatus::parse(&status) == Some(HiveWorkerStatus::Active),
        "staged input Worker is inactive"
    );
    let context: HiveRunExecutionContextV1 = serde_json::from_str(&execution_context_json)
        .context("decoding staged input run execution context")?;
    ensure!(
        context.worker_id() == input.worker_id
            && matches!(context.lane(), WorkerConversationLane::DirectMessage),
        "staged input run is not the exact Worker DM lane"
    );

    tx.execute(
        "INSERT INTO hive_worker_conversation_inputs (
             id, worker_id, owner_user_id, session_id, request_id,
             accepted_while_run_id, content_json, state,
             canonical_message_id, assigned_run_id, accepted_at, materialized_at
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, 'staged', NULL, NULL, ?8, NULL
         )",
        params![
            input.id,
            input.worker_id,
            input.owner_user_id,
            input.session_id,
            input.request_id,
            input.accepted_while_run_id,
            content_json,
            accepted_at,
        ],
    )?;
    let inserted = load_by_id(tx, &input.id)?.context("inserted staged input disappeared")?;
    Ok(StageWorkerConversationInputResult::Inserted(inserted))
}

fn load_by_id_or_request(
    tx: &Transaction<'_>,
    id: &str,
    session_id: &str,
    request_id: &str,
) -> Result<Option<WorkerConversationInput>> {
    let sql = format!(
        "SELECT {INPUT_COLUMNS}
         FROM hive_worker_conversation_inputs
         WHERE id = ?1 OR (session_id = ?2 AND request_id = ?3)
         ORDER BY id = ?1 DESC LIMIT 1"
    );
    tx.query_row(&sql, params![id, session_id, request_id], map_input)
        .optional()
        .context("checking staged Worker input idempotency")
}

fn load_by_id(tx: &Transaction<'_>, id: &str) -> Result<Option<WorkerConversationInput>> {
    let sql = format!("SELECT {INPUT_COLUMNS} FROM hive_worker_conversation_inputs WHERE id = ?1");
    tx.query_row(&sql, [id], map_input)
        .optional()
        .context("reading staged Worker input")
}

fn ensure_existing_matches(
    existing: &WorkerConversationInput,
    input: &StageWorkerConversationInput,
    accepted_at: &str,
) -> Result<()> {
    ensure!(
        existing.id == input.id
            && existing.worker_id == input.worker_id
            && existing.owner_user_id == input.owner_user_id
            && existing.session_id == input.session_id
            && existing.request_id == input.request_id
            && existing.accepted_while_run_id == input.accepted_while_run_id
            && existing.body == input.body
            && existing.accepted_at == accepted_at,
        "Worker conversation input idempotency key was reused with different content or binding"
    );
    Ok(())
}

fn validate_input(input: &StageWorkerConversationInput) -> Result<()> {
    for (value, label) in [
        (input.id.as_str(), "input id"),
        (input.worker_id.as_str(), "Worker id"),
        (input.session_id.as_str(), "session id"),
        (input.request_id.as_str(), "request id"),
        (input.accepted_while_run_id.as_str(), "active run id"),
    ] {
        ensure!(!value.trim().is_empty(), "{label} is empty");
        ensure!(
            value.len() <= MAX_INPUT_ID_BYTES,
            "{label} exceeds the byte limit"
        );
        ensure!(
            !value.chars().any(char::is_control),
            "{label} contains control characters"
        );
    }
    ensure!(
        !input.body.trim().is_empty(),
        "Worker conversation input is empty"
    );
    ensure!(
        input.body.len() <= MAX_INPUT_BODY_BYTES,
        "Worker conversation input exceeds the byte limit"
    );
    Ok(())
}

fn map_input(row: &Row<'_>) -> rusqlite::Result<WorkerConversationInput> {
    let content_json = row.get::<_, String>(6)?;
    let content = serde_json::from_str::<Vec<Content>>(&content_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let body = match content.as_slice() {
        [Content::Text { text, .. }] if !text.trim().is_empty() => text.clone(),
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(IoError::new(
                    ErrorKind::InvalidData,
                    "staged Worker input is not one canonical text block",
                )),
            ))
        }
    };
    let state_raw = row.get::<_, String>(7)?;
    let state = WorkerConversationInputState::parse(&state_raw).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Text,
            Box::new(IoError::new(
                ErrorKind::InvalidData,
                format!("invalid Worker input state: {state_raw}"),
            )),
        )
    })?;
    Ok(WorkerConversationInput {
        id: row.get(0)?,
        worker_id: row.get(1)?,
        owner_user_id: row.get(2)?,
        session_id: row.get(3)?,
        request_id: row.get(4)?,
        accepted_while_run_id: row.get(5)?,
        body,
        state,
        canonical_message_id: row.get(8)?,
        assigned_run_id: row.get(9)?,
        accepted_at: row.get(10)?,
        materialized_at: row.get(11)?,
    })
}
