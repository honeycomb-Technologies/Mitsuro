use std::io::{Error as IoError, ErrorKind};

use anyhow::{ensure, Context, Result};
use chrono::Utc;
use rusqlite::{
    params, types::Type, Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::ai::models::ModelKey;
use crate::ai::providers::ProviderId;
use crate::ai::types::Usage;
use crate::storage::episodes::EpisodeStore;
use crate::storage::Database;
use crate::Content;

use super::{
    HiveWorkerIntroduction, HiveWorkerIntroductionStatus, WorkerIntroductionDecisionKind,
    WorkerIntroductionDecisionV1, WorkerIntroductionEvidenceCoverage, WorkerIntroductionProposalV1,
    WorkerIntroductionReviewProjection, WorkerIntroductionReviewProjectionState,
    WorkerIntroductionReviewReadiness, WorkerIntroductionReviewRecord,
    WorkerIntroductionReviewStatus, WorkerIntroductionReviewerOutputV1,
    MAX_WORKER_INTRODUCTION_FACTS, WORKER_INTRODUCTION_PROPOSAL_VERSION,
};

const INTRODUCTION_COLUMNS: &str = "worker_id, run_id, status, prompt_version, opening_message_id, proposal_json, proposal_revision, decision_json, last_error, created_at, updated_at, completed_at";
const REVIEW_COLUMNS: &str = "id, worker_id, session_id, status, claim_token, claim_expires_at, opening_message_id, through_message_id, user_message_ids_json, transcript_digest, base_identity_digest, base_soul_digest, worker_user_id, model, model_key_json, model_catalog_revision, provider_id, trace_run_id, provider_call_id, usage_json, proposal_id, proposal_revision, reviewer_output_json, proposal_json, decision_json, last_error, claimed_at, created_at, updated_at, completed_at, run_id, attempt_no";

const REVIEW_CLAIM_LEASE_MINUTES: i64 = 20;
const MAX_REVIEW_USAGE_BYTES: usize = 4 * 1024;
pub(crate) const MAX_AUTOMATIC_REVIEW_ATTEMPTS: i64 = 3;
const EXHAUSTED_REVIEW_ERROR: &str =
    "Introduction review needs attention after 3 attempts; retry review or keep talking";
const REVIEW_NEEDS_ATTENTION_PREFIX: &str = "Introduction review needs attention at message ";
const SKIPPED_REVIEW_REASON: &str = "Introduction review cancelled because the user skipped setup";

#[derive(Debug, Clone)]
#[cfg(test)]
pub(crate) struct NewWorkerIntroductionReviewClaim {
    pub worker_id: String,
    pub session_id: String,
    pub opening_message_id: i64,
    pub through_message_id: i64,
    pub user_message_ids: Vec<i64>,
    pub transcript_digest: String,
    pub base_identity_digest: String,
    pub base_soul_digest: String,
    pub worker_user_id: Option<String>,
    pub model: String,
    pub model_key: ModelKey,
    pub model_catalog_revision: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReviewProposalPersistence {
    ReviewReady,
    Stale,
}

/// Read and transition one-time Worker Introduction rows.
///
/// `from_connection` lets the Hive runtime share its caller-owned transaction;
/// `new` is convenient for API and reconciliation reads from a `Database`.
pub struct HiveWorkerIntroductionStore<'a> {
    conn: &'a Connection,
}

impl<'a> HiveWorkerIntroductionStore<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { conn: db.conn() }
    }

    pub fn from_connection(conn: &'a Connection) -> Self {
        Self { conn }
    }

    pub fn get_by_worker(&self, worker_id: &str) -> Result<Option<HiveWorkerIntroduction>> {
        let sql = format!(
            "SELECT {INTRODUCTION_COLUMNS} FROM hive_worker_introductions
             WHERE worker_id = ?1"
        );
        self.conn
            .query_row(&sql, [worker_id], map_introduction)
            .optional()
            .context("reading Hive Worker Introduction")
    }

    pub fn get_by_run(&self, run_id: &str) -> Result<Option<HiveWorkerIntroduction>> {
        let sql = format!(
            "SELECT {INTRODUCTION_COLUMNS} FROM hive_worker_introductions
             WHERE run_id = ?1"
        );
        self.conn
            .query_row(&sql, [run_id], map_introduction)
            .optional()
            .context("reading Hive Worker Introduction by run")
    }

    /// Derive setup coverage from previously completed partial reviews.
    ///
    /// Provider output never supplies coverage flags. Each candidate axis is
    /// re-bound to an exact canonical USER message in this Worker's private DM;
    /// deleted, malformed, cross-session, or non-user evidence simply cannot
    /// contribute. This makes the projection restart-safe without a shadow
    /// coverage column or a schema migration.
    pub fn evidence_coverage(
        &self,
        worker_id: &str,
        session_id: &str,
    ) -> Result<WorkerIntroductionEvidenceCoverage> {
        validate_ids(worker_id, None)?;
        ensure!(
            !session_id.trim().is_empty(),
            "Worker DM session id is empty"
        );
        let mut statement = self.conn.prepare(
            "SELECT reviewer_output_json
             FROM hive_worker_introduction_reviews
             WHERE worker_id = ?1 AND session_id = ?2
               AND status = 'gather_more'
               AND reviewer_output_json IS NOT NULL
             ORDER BY through_message_id ASC, id ASC",
        )?;
        let outputs = statement
            .query_map(params![worker_id, session_id], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut evidence_kinds = Vec::new();
        for raw in outputs {
            let output = serde_json::from_str::<WorkerIntroductionReviewerOutputV1>(&raw)
                .context("decoding persisted partial Worker Introduction review")?;
            ensure!(
                output.readiness == WorkerIntroductionReviewReadiness::GatherMore
                    && output.facts.len() <= MAX_WORKER_INTRODUCTION_FACTS,
                "persisted partial Worker Introduction review is inconsistent"
            );
            for fact in output.facts {
                let content_json = self
                    .conn
                    .query_row(
                        "SELECT content FROM messages
                         WHERE id = ?1 AND session_id = ?2 AND role = 'user'",
                        params![fact.evidence_message_id, session_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                let Some(content_json) = content_json else {
                    continue;
                };
                let Some(text) = canonical_user_evidence_text(&content_json)? else {
                    continue;
                };
                let excerpt = fact.evidence_excerpt.trim();
                if !excerpt.is_empty() && text.contains(excerpt) {
                    evidence_kinds.push(fact.kind);
                }
            }
        }
        Ok(WorkerIntroductionEvidenceCoverage::from_fact_kinds(
            evidence_kinds,
        ))
    }

    /// Project the review state for the Worker's current canonical DM
    /// exchange. Clients should poll only while `should_poll` is true; this
    /// keeps retry ceilings, pending-input fences, and covered decisions owned
    /// by core instead of re-derived in each UI.
    pub fn get_review_projection(
        &self,
        worker_id: &str,
    ) -> Result<Option<WorkerIntroductionReviewProjection>> {
        let Some(lifecycle) = self.get_by_worker(worker_id)? else {
            return Ok(None);
        };
        match lifecycle.status {
            HiveWorkerIntroductionStatus::Confirmed => {
                return Ok(Some(terminal_review_projection(
                    self.conn,
                    worker_id,
                    lifecycle,
                    WorkerIntroductionReviewProjectionState::Confirmed,
                )?));
            }
            HiveWorkerIntroductionStatus::Queued
            | HiveWorkerIntroductionStatus::Running
            | HiveWorkerIntroductionStatus::Skipped
            | HiveWorkerIntroductionStatus::Failed
            | HiveWorkerIntroductionStatus::NeedsRecovery => {
                return Ok(Some(terminal_review_projection(
                    self.conn,
                    worker_id,
                    lifecycle,
                    WorkerIntroductionReviewProjectionState::Inactive,
                )?));
            }
            HiveWorkerIntroductionStatus::AwaitingContext
            | HiveWorkerIntroductionStatus::ReviewReady => {}
        }
        let worker_projection = self
            .conn
            .query_row(
                "SELECT status, user_id, dm_session_id, model, model_key_json,
                        model_catalog_revision, permission_mode
                 FROM hive_workers WHERE id = ?1",
                [worker_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            worker_status,
            worker_user_id,
            session_id,
            model,
            model_key_json,
            model_catalog_revision,
            permission_mode,
        )) = worker_projection
        else {
            return Ok(Some(ineligible_review_projection(
                worker_id,
                lifecycle,
                WorkerIntroductionReviewProjectionState::NeedsAttention,
                "Hive Worker Introduction references a missing Worker",
            )));
        };
        if worker_status != "active" {
            return Ok(Some(ineligible_review_projection(
                worker_id,
                lifecycle,
                WorkerIntroductionReviewProjectionState::Inactive,
                &format!("Hive Worker is {worker_status}; Introduction review is inactive"),
            )));
        }
        let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) else {
            return Ok(Some(ineligible_review_projection(
                worker_id,
                lifecycle,
                WorkerIntroductionReviewProjectionState::NeedsAttention,
                "Hive Worker Introduction has no private DM binding",
            )));
        };
        let exact_worker_model = match (model.as_deref(), model_key_json.as_deref()) {
            (Some(model), Some(model_key_json)) if !model.trim().is_empty() => {
                serde_json::from_str::<ModelKey>(model_key_json)
                    .ok()
                    .filter(|model_key| model_key.model_id == model)
            }
            _ => None,
        };
        let Some(exact_worker_model) = exact_worker_model else {
            return Ok(Some(ineligible_review_projection(
                worker_id,
                lifecycle,
                WorkerIntroductionReviewProjectionState::NeedsAttention,
                "Hive Worker Introduction exact model binding is missing or invalid",
            )));
        };
        let session_projection = self
            .conn
            .query_row(
                "SELECT user_id, session_type, model, model_key_json,
                        model_catalog_revision, permission_mode
                 FROM sessions WHERE id = ?1
                   AND NOT EXISTS (
                       SELECT 1 FROM hive_group_worker_lanes lane
                       WHERE lane.session_id = sessions.id
                   )",
                [&session_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;
        let exact_private_dm = session_projection
            .and_then(
                |(user_id, session_type, session_model, session_model_key_json, revision, mode)| {
                    let session_model_key = session_model_key_json
                        .as_deref()
                        .and_then(|json| serde_json::from_str::<ModelKey>(json).ok())?;
                    (user_id == worker_user_id
                        && session_type == "hive"
                        && session_model == model
                        && session_model_key == exact_worker_model
                        && revision == model_catalog_revision
                        && mode == permission_mode)
                        .then_some(())
                },
            )
            .is_some();
        if !exact_private_dm {
            let reason = if lifecycle.status == HiveWorkerIntroductionStatus::ReviewReady {
                "Introduction proposal is stale after Worker or session changes; keep talking to review fresh context"
            } else {
                "Hive Worker Introduction private DM owner, model, or permission binding is invalid"
            };
            return Ok(Some(ineligible_review_projection(
                worker_id,
                lifecycle,
                WorkerIntroductionReviewProjectionState::NeedsAttention,
                reason,
            )));
        }
        let has_pending_user_input: bool = self.conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM messages
                 WHERE session_id = ?1 AND role LIKE 'pending_user:%'
             )",
            [&session_id],
            |row| row.get(0),
        )?;
        let current_through_message_id: Option<i64> = self.conn.query_row(
            "SELECT MAX(id) FROM messages
             WHERE session_id = ?1 AND role NOT LIKE 'pending_user:%'",
            [&session_id],
            |row| row.get(0),
        )?;
        let latest_review_id = self
            .conn
            .query_row(
                "SELECT id FROM hive_worker_introduction_reviews
                 WHERE worker_id = ?1
                 ORDER BY through_message_id DESC, claimed_at DESC, rowid DESC
                 LIMIT 1",
                [worker_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let latest_review = latest_review_id
            .as_deref()
            .map(|review_id| {
                WorkerIntroductionReviewStore::from_connection(self.conn)
                    .get_by_id(review_id)?
                    .context("latest Worker Introduction review disappeared")
            })
            .transpose()?;
        let review_through_message_id = latest_review
            .as_ref()
            .map(|review| review.through_message_id);
        let is_current_through = current_through_message_id.is_some()
            && current_through_message_id == review_through_message_id;
        let current_review = is_current_through
            .then_some(latest_review.as_ref())
            .flatten();
        let attempt_count = match current_through_message_id {
            Some(through_message_id) => self.conn.query_row(
                "SELECT COUNT(*) FROM hive_worker_introduction_reviews
                 WHERE worker_id = ?1 AND through_message_id = ?2
                   AND (
                       status = 'failed'
                       OR (status = 'stale' AND provider_call_id IS NOT NULL)
                   )",
                params![worker_id, through_message_id],
                |row| row.get::<_, i64>(0),
            )?,
            None => 0,
        };
        let attempt_count = u32::try_from(attempt_count)
            .context("Worker Introduction review attempt count overflow")?;
        let completed_exchange = if let (Some(opening_message_id), Some(_)) =
            (lifecycle.opening_message_id, current_through_message_id)
        {
            let has_user_reply: bool = self.conn.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM messages
                     WHERE session_id = ?1 AND role = 'user' AND id > ?2
                 )",
                params![session_id, opening_message_id],
                |row| row.get(0),
            )?;
            let latest_dialogue: Option<(i64, String)> = self
                .conn
                .query_row(
                    "SELECT id, role FROM messages
                     WHERE session_id = ?1 AND role IN ('user', 'assistant')
                     ORDER BY id DESC LIMIT 1",
                    [&session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            has_user_reply
                && latest_dialogue.as_ref().is_some_and(|(message_id, role)| {
                    Some(*message_id) == current_through_message_id && role == "assistant"
                })
        } else {
            false
        };

        let review_ready_basis_stale = if lifecycle.status
            == HiveWorkerIntroductionStatus::ReviewReady
        {
            match latest_review.as_ref() {
                Some(review) => {
                    let exact_binding_current = exact_review_worker_binding_current(
                        self.conn,
                        &review.worker_id,
                        &review.session_id,
                        review.worker_user_id.as_deref(),
                        &review.model,
                        &review.model_key,
                        review.model_catalog_revision.as_deref(),
                        review.provider_id,
                    )?;
                    let profile_basis_current = review_profile_basis_current(self.conn, review)?;
                    !is_current_through
                        || review.status != WorkerIntroductionReviewStatus::ReviewReady
                        || !exact_binding_current
                        || !profile_basis_current
                }
                None => true,
            }
        } else {
            false
        };

        let input_needs_attention = current_through_message_id.is_some_and(|through_message_id| {
            lifecycle
                .last_error
                .as_deref()
                .is_some_and(|error| review_input_error_matches_through(error, through_message_id))
        });
        let (state, should_poll) = match lifecycle.status {
            HiveWorkerIntroductionStatus::ReviewReady if review_ready_basis_stale => (
                WorkerIntroductionReviewProjectionState::NeedsAttention,
                false,
            ),
            HiveWorkerIntroductionStatus::ReviewReady => {
                (WorkerIntroductionReviewProjectionState::ReviewReady, false)
            }
            HiveWorkerIntroductionStatus::Confirmed => {
                (WorkerIntroductionReviewProjectionState::Confirmed, false)
            }
            HiveWorkerIntroductionStatus::AwaitingContext
                if has_pending_user_input || !completed_exchange =>
            {
                (
                    WorkerIntroductionReviewProjectionState::AwaitingContext,
                    false,
                )
            }
            HiveWorkerIntroductionStatus::AwaitingContext if input_needs_attention => (
                WorkerIntroductionReviewProjectionState::NeedsAttention,
                false,
            ),
            HiveWorkerIntroductionStatus::AwaitingContext => match current_review
                .map(|review| review.status)
            {
                None => (WorkerIntroductionReviewProjectionState::Pending, true),
                Some(WorkerIntroductionReviewStatus::Queued) => {
                    (WorkerIntroductionReviewProjectionState::Pending, true)
                }
                Some(WorkerIntroductionReviewStatus::Claimed) => {
                    (WorkerIntroductionReviewProjectionState::Claimed, true)
                }
                Some(
                    WorkerIntroductionReviewStatus::Failed | WorkerIntroductionReviewStatus::Stale,
                ) if i64::from(attempt_count) < MAX_AUTOMATIC_REVIEW_ATTEMPTS => {
                    (WorkerIntroductionReviewProjectionState::Retrying, true)
                }
                Some(
                    WorkerIntroductionReviewStatus::Failed | WorkerIntroductionReviewStatus::Stale,
                ) => (
                    WorkerIntroductionReviewProjectionState::NeedsAttention,
                    false,
                ),
                Some(WorkerIntroductionReviewStatus::GatherMore) => {
                    (WorkerIntroductionReviewProjectionState::GatherMore, false)
                }
                Some(WorkerIntroductionReviewStatus::ReviewReady) => {
                    (WorkerIntroductionReviewProjectionState::ReviewReady, false)
                }
                Some(WorkerIntroductionReviewStatus::Confirmed) => {
                    (WorkerIntroductionReviewProjectionState::Confirmed, false)
                }
                Some(WorkerIntroductionReviewStatus::Rejected) => {
                    (WorkerIntroductionReviewProjectionState::Rejected, false)
                }
                Some(WorkerIntroductionReviewStatus::KeepTalking) => {
                    (WorkerIntroductionReviewProjectionState::KeepTalking, false)
                }
            },
            HiveWorkerIntroductionStatus::Queued
            | HiveWorkerIntroductionStatus::Running
            | HiveWorkerIntroductionStatus::Skipped
            | HiveWorkerIntroductionStatus::Failed
            | HiveWorkerIntroductionStatus::NeedsRecovery => {
                (WorkerIntroductionReviewProjectionState::Inactive, false)
            }
        };
        let last_error = lifecycle
            .last_error
            .clone()
            .or_else(|| current_review.and_then(|review| review.last_error.clone()))
            .or_else(|| {
                review_ready_basis_stale.then(|| {
                    "Introduction proposal is stale after Worker or profile changes; keep talking to review fresh context"
                        .to_string()
                })
            })
            .map(|error| truncate_utf8(&error, 2_000).to_string());
        Ok(Some(WorkerIntroductionReviewProjection {
            worker_id: worker_id.to_string(),
            lifecycle_status: lifecycle.status,
            state,
            current_through_message_id,
            review_through_message_id,
            review_status: latest_review.as_ref().map(|review| review.status),
            is_current_through,
            has_pending_user_input,
            attempt_count,
            should_poll,
            last_error,
        }))
    }

    /// Claim a queued or recovery-required Introduction for its durable run.
    pub fn mark_running(&self, worker_id: &str, run_id: &str) -> Result<HiveWorkerIntroduction> {
        validate_ids(worker_id, Some(run_id))?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "UPDATE hive_worker_introductions
             SET run_id = COALESCE(run_id, ?2), status = 'running',
                 last_error = NULL, completed_at = NULL, updated_at = ?3
             WHERE worker_id = ?1
               AND status IN ('queued', 'running', 'needs_recovery')
               AND (run_id IS NULL OR run_id = ?2)
               AND EXISTS (
                   SELECT 1 FROM hive_runs run
                   WHERE run.id = ?2 AND run.worker_id = ?1
                     AND run.kind = 'worker_introduction'
               )
             RETURNING {INTRODUCTION_COLUMNS}"
        );
        let transitioned = self
            .conn
            .query_row(&sql, params![worker_id, run_id, now], map_introduction)
            .optional()?;
        self.finish_transition(worker_id, transitioned, "mark running")
    }

    /// Bind the exactly-once assistant opening and begin conversational
    /// context gathering. The opening must belong to this Worker's private DM.
    pub fn mark_opened(
        &self,
        worker_id: &str,
        run_id: &str,
        opening_message_id: i64,
    ) -> Result<HiveWorkerIntroduction> {
        validate_ids(worker_id, Some(run_id))?;
        ensure!(
            opening_message_id > 0,
            "opening message id must be positive"
        );
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "UPDATE hive_worker_introductions
             SET status = 'awaiting_context', opening_message_id = ?3,
                 last_error = NULL, completed_at = NULL, updated_at = ?4
             WHERE worker_id = ?1 AND run_id = ?2
               AND status IN ('running', 'awaiting_context')
               AND (opening_message_id IS NULL OR opening_message_id = ?3)
               AND EXISTS (
                   SELECT 1
                   FROM messages message
                   JOIN hive_workers worker ON worker.id = ?1
                   WHERE message.id = ?3
                     AND message.session_id = worker.dm_session_id
                     AND message.role = 'assistant'
               )
             RETURNING {INTRODUCTION_COLUMNS}"
        );
        let transitioned = self
            .conn
            .query_row(
                &sql,
                params![worker_id, run_id, opening_message_id, now],
                map_introduction,
            )
            .optional()?;
        self.finish_transition(worker_id, transitioned, "mark opened")
    }

    pub fn mark_failed(
        &self,
        worker_id: &str,
        run_id: &str,
        error: &str,
    ) -> Result<HiveWorkerIntroduction> {
        self.mark_run_problem(
            worker_id,
            run_id,
            error,
            HiveWorkerIntroductionStatus::Failed,
        )
    }

    pub fn mark_needs_recovery(
        &self,
        worker_id: &str,
        run_id: &str,
        error: &str,
    ) -> Result<HiveWorkerIntroduction> {
        self.mark_run_problem(
            worker_id,
            run_id,
            error,
            HiveWorkerIntroductionStatus::NeedsRecovery,
        )
    }

    /// Explicitly bypass the Introduction. Existing Workers without a ledger
    /// are already compatible and therefore return `None` from reads instead.
    pub fn skip(&self, worker_id: &str) -> Result<HiveWorkerIntroduction> {
        validate_ids(worker_id, None)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = format!(
            "UPDATE hive_worker_introductions
             SET status = 'skipped', proposal_json = NULL,
                 decision_json = NULL, last_error = NULL,
                 completed_at = COALESCE(completed_at, ?2), updated_at = ?2
             WHERE worker_id = ?1
               AND status IN (
                   'queued', 'running', 'awaiting_context', 'review_ready',
                   'failed', 'needs_recovery', 'skipped'
               )
             RETURNING {INTRODUCTION_COLUMNS}"
        );
        let transitioned = self
            .conn
            .query_row(&sql, params![worker_id, now], map_introduction)
            .optional()?;
        let transitioned = self.finish_transition(worker_id, transitioned, "skip")?;
        self.terminalize_claimed_reviews_for_skip(worker_id, &now)?;
        Ok(transitioned)
    }

    /// Terminalize any in-flight claim or frozen proposal when the surrounding
    /// Hive transaction skips setup. Callers that own a larger transaction
    /// should construct this store with `from_connection(tx)` so the lifecycle,
    /// run cancellation, review audit, receipt, and event commit together.
    pub fn terminalize_claimed_reviews_for_skip(
        &self,
        worker_id: &str,
        completed_at: &str,
    ) -> Result<usize> {
        validate_ids(worker_id, None)?;
        ensure!(
            chrono::DateTime::parse_from_rfc3339(completed_at).is_ok(),
            "skip completion timestamp is not RFC3339"
        );
        let reviews_changed = self
            .conn
            .execute(
                "UPDATE hive_worker_introduction_reviews
                 SET status = 'stale', last_error = ?3,
                     completed_at = COALESCE(completed_at, ?2), updated_at = ?2
                 WHERE worker_id = ?1 AND status IN ('claimed', 'review_ready')",
                params![worker_id, completed_at, SKIPPED_REVIEW_REASON],
            )
            .context("terminalizing Worker Introduction reviews for skip")?;
        self.conn.execute(
            "UPDATE hive_worker_introductions
             SET proposal_json = NULL, decision_json = NULL, updated_at = ?2
             WHERE worker_id = ?1 AND status = 'skipped'",
            params![worker_id, completed_at],
        )?;
        Ok(reviews_changed)
    }

    /// Persist a deterministic, provider-free review-input failure against the
    /// exact current message boundary. The due scanner and UI projection both
    /// understand this fence, so malformed legacy input cannot create an
    /// unaudited infinite retry/poll loop. A newer canonical message naturally
    /// changes the boundary and makes review eligible again.
    pub(crate) fn mark_review_input_needs_attention(
        &self,
        worker_id: &str,
        through_message_id: i64,
        error: &str,
    ) -> Result<bool> {
        validate_ids(worker_id, None)?;
        ensure!(
            through_message_id > 0,
            "review through message id is invalid"
        );
        ensure!(!error.trim().is_empty(), "review input error is empty");
        let detail = truncate_utf8(error.trim(), 1_500);
        let projected = format!("{REVIEW_NEEDS_ATTENTION_PREFIX}{through_message_id}: {detail}");
        let now = Utc::now().to_rfc3339();
        let changed = self.conn.execute(
            "UPDATE hive_worker_introductions
             SET last_error = ?3, updated_at = ?4
             WHERE worker_id = ?1 AND status = 'awaiting_context'
               AND ?2 = (
                   SELECT MAX(message.id) FROM messages message
                   JOIN hive_workers worker ON worker.id = ?1
                   WHERE message.session_id = worker.dm_session_id
                     AND message.role NOT LIKE 'pending_user:%'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM messages pending
                   JOIN hive_workers worker ON worker.id = ?1
                   WHERE pending.session_id = worker.dm_session_id
                     AND pending.role LIKE 'pending_user:%'
               )",
            params![worker_id, through_message_id, projected, now],
        )?;
        Ok(changed == 1)
    }

    /// Project a provider-free current-boundary configuration failure (for
    /// example unavailable credentials) so a host does not rediscover and
    /// advertise the same impossible review forever. Explicit retry may clear
    /// this fence by successfully creating a claim.
    pub fn mark_current_review_needs_attention(
        &self,
        worker_id: &str,
        error: &str,
    ) -> Result<bool> {
        validate_ids(worker_id, None)?;
        let through_message_id = self
            .conn
            .query_row(
                "SELECT MAX(message.id)
                 FROM messages message
                 JOIN hive_workers worker ON worker.id = ?1
                 WHERE message.session_id = worker.dm_session_id
                   AND message.role NOT LIKE 'pending_user:%'",
                [worker_id],
                |row| row.get::<_, Option<i64>>(0),
            )?
            .context("Worker Introduction has no current canonical message")?;
        self.mark_review_input_needs_attention(worker_id, through_message_id, error)
    }

    fn mark_run_problem(
        &self,
        worker_id: &str,
        run_id: &str,
        error: &str,
        target: HiveWorkerIntroductionStatus,
    ) -> Result<HiveWorkerIntroduction> {
        validate_ids(worker_id, Some(run_id))?;
        ensure!(!error.trim().is_empty(), "Introduction error is empty");
        ensure!(
            matches!(
                target,
                HiveWorkerIntroductionStatus::Failed | HiveWorkerIntroductionStatus::NeedsRecovery
            ),
            "invalid Introduction problem status"
        );
        let now = chrono::Utc::now().to_rfc3339();
        let completed_at = (target == HiveWorkerIntroductionStatus::Failed).then_some(now.as_str());
        let sql = format!(
            "UPDATE hive_worker_introductions
             SET status = ?4, last_error = ?3,
                 completed_at = ?5, updated_at = ?6
             WHERE worker_id = ?1 AND run_id = ?2
               AND (
                   (?4 = 'failed'
                    AND status IN ('queued', 'running', 'failed', 'needs_recovery'))
                   OR
                   (?4 = 'needs_recovery'
                    AND status IN ('queued', 'running', 'needs_recovery'))
               )
             RETURNING {INTRODUCTION_COLUMNS}"
        );
        let transitioned = self
            .conn
            .query_row(
                &sql,
                params![worker_id, run_id, error, target.as_str(), completed_at, now],
                map_introduction,
            )
            .optional()?;
        self.finish_transition(worker_id, transitioned, target.as_str())
    }

    fn finish_transition(
        &self,
        worker_id: &str,
        transitioned: Option<HiveWorkerIntroduction>,
        action: &str,
    ) -> Result<HiveWorkerIntroduction> {
        if let Some(transitioned) = transitioned {
            return Ok(transitioned);
        }
        let current = self.get_by_worker(worker_id)?;
        anyhow::bail!(
            "cannot {action} Hive Worker Introduction {worker_id}; current state: {}",
            current
                .as_ref()
                .map(|row| row.status.as_str())
                .unwrap_or("missing")
        );
    }
}

/// Durable reviewer claims and their append-only outcomes. Mutating methods
/// intentionally accept a caller-owned connection: passing a `Transaction`
/// (which dereferences to `Connection`) lets the daemon combine them with its
/// idempotency receipt, while higher-level core services can own an immediate
/// transaction around the same methods.
pub(crate) struct WorkerIntroductionReviewStore<'a> {
    conn: &'a Connection,
}

impl<'a> WorkerIntroductionReviewStore<'a> {
    pub(crate) fn from_connection(conn: &'a Connection) -> Self {
        Self { conn }
    }

    pub(crate) fn get_by_id(
        &self,
        review_id: &str,
    ) -> Result<Option<WorkerIntroductionReviewRecord>> {
        let sql = format!(
            "SELECT {REVIEW_COLUMNS}
             FROM hive_worker_introduction_reviews WHERE id = ?1"
        );
        self.conn
            .query_row(&sql, [review_id], map_review)
            .optional()
            .context("reading Hive Worker Introduction review")
    }

    pub(crate) fn get_by_run(
        &self,
        run_id: &str,
    ) -> Result<Option<WorkerIntroductionReviewRecord>> {
        let sql = format!(
            "SELECT {REVIEW_COLUMNS}
             FROM hive_worker_introduction_reviews WHERE run_id = ?1"
        );
        self.conn
            .query_row(&sql, [run_id], map_review)
            .optional()
            .context("reading Hive Worker Introduction review by run")
    }

    /// Claim the queued audit row owned by one exact running Hive attempt.
    /// No caller-selected transcript or model input participates here.
    pub(crate) fn claim_run(
        &self,
        run_id: &str,
        run_lease_token: &str,
        run_lease_epoch: u64,
    ) -> Result<Option<WorkerIntroductionReviewRecord>> {
        ensure!(!run_id.trim().is_empty(), "review run id is empty");
        ensure!(
            !run_lease_token.trim().is_empty(),
            "review run lease token is empty"
        );
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let claim_expires_at =
            (now + chrono::Duration::minutes(REVIEW_CLAIM_LEASE_MINUTES)).to_rfc3339();
        let claim_token = Uuid::new_v4().to_string();
        let changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = 'claimed', claim_token = ?4,
                 claim_expires_at = ?5, claimed_at = ?6, updated_at = ?6
             WHERE run_id = ?1 AND status = 'queued'
               AND EXISTS (
                   SELECT 1 FROM hive_runs run
                   WHERE run.id = ?1
                     AND run.kind = 'worker_introduction_review'
                     AND run.status = 'running'
                     AND run.lease_token = ?2
                     AND run.lease_epoch = ?3
                     AND run.worker_id = hive_worker_introduction_reviews.worker_id
                     AND run.session_id = hive_worker_introduction_reviews.session_id
                     AND run.conversation_through_message_id =
                         hive_worker_introduction_reviews.through_message_id
               )",
            params![
                run_id,
                run_lease_token,
                run_lease_epoch,
                claim_token,
                claim_expires_at,
                now_text,
            ],
        )?;
        if changed == 0 {
            return self.get_by_run(run_id);
        }
        ensure!(changed == 1, "review run claimed multiple audit rows");
        self.get_by_run(run_id)
    }

    pub(crate) fn get_by_proposal(
        &self,
        proposal_id: &str,
    ) -> Result<Option<WorkerIntroductionReviewRecord>> {
        let sql = format!(
            "SELECT {REVIEW_COLUMNS}
             FROM hive_worker_introduction_reviews WHERE proposal_id = ?1"
        );
        self.conn
            .query_row(&sql, [proposal_id], map_review)
            .optional()
            .context("reading Hive Worker Introduction review by proposal")
    }

    /// Close abandoned claims independently of Worker eligibility. Without a
    /// global sweep, pausing/archiving a Worker after claim could leave the
    /// audit row permanently `claimed` because that Worker is intentionally
    /// absent from the normal due set.
    pub(crate) fn reap_expired_claims(&self) -> Result<usize> {
        let now = Utc::now().to_rfc3339();
        self.conn
            .execute(
                "UPDATE hive_worker_introduction_reviews
                 SET status = 'failed',
                     last_error = 'review claim expired before completion',
                     completed_at = ?1, updated_at = ?1
                 WHERE status = 'claimed' AND run_id IS NULL
                   AND claim_expires_at <= ?1",
                [&now],
            )
            .context("reaping expired Worker Introduction review claims")
    }

    /// Claim one exact transcript snapshot. A completed `gather_more` review
    /// suppresses another provider call until a newer canonical message
    /// exists. Expired process claims are retained as failed audit rows and a
    /// fresh claim may then be inserted for the same basis.
    #[cfg(test)]
    pub(crate) fn claim(
        &self,
        input: &NewWorkerIntroductionReviewClaim,
        allow_exhausted: bool,
    ) -> Result<Option<WorkerIntroductionReviewRecord>> {
        validate_review_claim(input)?;
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = 'failed', last_error = 'review claim expired before completion',
                 completed_at = ?1, updated_at = ?1
             WHERE worker_id = ?2 AND status = 'claimed'
               AND claim_expires_at <= ?1",
            params![now_text, input.worker_id],
        )?;

        let lifecycle_matches = self.conn.query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM hive_worker_introductions introduction
                 JOIN hive_workers worker ON worker.id = introduction.worker_id
                 JOIN sessions session ON session.id = ?2
                 WHERE introduction.worker_id = ?1
                   AND introduction.status = 'awaiting_context'
                   AND introduction.opening_message_id = ?3
                   AND worker.dm_session_id = ?2
                   AND worker.status = 'active'
                   AND session.session_type = 'hive'
                   AND session.user_id IS worker.user_id
                   AND NOT EXISTS (
                       SELECT 1 FROM hive_group_worker_lanes lane
                       WHERE lane.session_id = ?2
                   )
             )",
            params![input.worker_id, input.session_id, input.opening_message_id],
            |row| row.get::<_, bool>(0),
        )?;
        ensure!(
            lifecycle_matches,
            "Hive Worker Introduction review is not bound to an awaiting private DM"
        );
        ensure!(
            exact_review_worker_binding_current(
                self.conn,
                &input.worker_id,
                &input.session_id,
                input.worker_user_id.as_deref(),
                &input.model,
                &input.model_key,
                input.model_catalog_revision.as_deref(),
                input.model_key.provider,
            )?,
            "Hive Worker Introduction review model or DM binding changed before claim"
        );
        ensure_no_newer_canonical_message(self.conn, &input.session_id, input.through_message_id)?;
        for message_id in &input.user_message_ids {
            let is_evidence: bool = self.conn.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM messages
                     WHERE id = ?1 AND session_id = ?2 AND role = 'user'
                 )",
                params![message_id, input.session_id],
                |row| row.get(0),
            )?;
            ensure!(
                is_evidence && *message_id > input.opening_message_id,
                "Introduction review user evidence is not a post-opening DM message"
            );
        }

        let already_covered: bool = self.conn.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM hive_worker_introduction_reviews
                 WHERE worker_id = ?1 AND through_message_id = ?2
                   AND status IN (
                       'claimed', 'gather_more', 'review_ready', 'confirmed',
                       'rejected', 'keep_talking'
                   )
             )",
            params![input.worker_id, input.through_message_id],
            |row| row.get(0),
        )?;
        if already_covered {
            return Ok(None);
        }

        let prior_attempts: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM hive_worker_introduction_reviews
             WHERE worker_id = ?1 AND through_message_id = ?2
               AND (
                   status = 'failed'
                   OR (status = 'stale' AND provider_call_id IS NOT NULL)
               )",
            params![input.worker_id, input.through_message_id],
            |row| row.get(0),
        )?;
        if !allow_exhausted && prior_attempts >= MAX_AUTOMATIC_REVIEW_ATTEMPTS {
            self.project_exhausted_error(&input.worker_id)?;
            return Ok(None);
        }

        let review_id = Uuid::new_v4().to_string();
        let claim_token = Uuid::new_v4().to_string();
        let claim_expires_at =
            (now + chrono::Duration::minutes(REVIEW_CLAIM_LEASE_MINUTES)).to_rfc3339();
        let user_message_ids_json = serde_json::to_string(&input.user_message_ids)?;
        let model_key_json = serde_json::to_string(&input.model_key)?;
        let provider_id = provider_id_storage_value(input.model_key.provider)?;
        let trace_run_id = format!("introduction-review:{review_id}");
        self.conn.execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest,
                 base_identity_digest, base_soul_digest, worker_user_id,
                 model, model_key_json, model_catalog_revision, provider_id,
                 trace_run_id, claimed_at, created_at, updated_at
             ) VALUES (
                 ?1, ?2, ?3, 'claimed', ?4, ?5, ?6, ?7, ?8, ?9,
                 ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?18, ?18
             )",
            params![
                review_id,
                input.worker_id,
                input.session_id,
                claim_token,
                claim_expires_at,
                input.opening_message_id,
                input.through_message_id,
                user_message_ids_json,
                input.transcript_digest,
                input.base_identity_digest,
                input.base_soul_digest,
                input.worker_user_id,
                input.model,
                model_key_json,
                input.model_catalog_revision,
                provider_id,
                trace_run_id,
                now_text,
            ],
        )?;
        self.conn.execute(
            "UPDATE hive_worker_introductions
             SET last_error = NULL, updated_at = ?2
             WHERE worker_id = ?1 AND status = 'awaiting_context'",
            params![input.worker_id, now_text],
        )?;
        self.get_by_id(&review_id)
    }

    pub(crate) fn mark_failed(
        &self,
        review_id: &str,
        claim_token: &str,
        error: &str,
    ) -> Result<()> {
        ensure!(!error.trim().is_empty(), "review failure is empty");
        let bounded_error = truncate_utf8(error.trim(), 2_000);
        let now = Utc::now().to_rfc3339();
        let changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = 'failed', last_error = ?3,
                 completed_at = ?4, updated_at = ?4
             WHERE id = ?1 AND claim_token = ?2 AND status = 'claimed'",
            params![review_id, claim_token, bounded_error, now],
        )?;
        ensure!(
            changed == 1,
            "Introduction review claim is no longer active"
        );
        self.project_exhaustion_for_review(review_id)?;
        Ok(())
    }

    /// Release a claim after a durable time-bounded governor gate. No Started
    /// row exists, so the same run/audit may sleep and be reclaimed without
    /// consuming a semantic review attempt.
    pub(crate) fn defer_claim(
        &self,
        review_id: &str,
        claim_token: &str,
        next_eligible_at: &str,
        reason: &str,
    ) -> Result<()> {
        ensure!(
            chrono::DateTime::parse_from_rfc3339(next_eligible_at).is_ok(),
            "review governor wake time is not RFC3339"
        );
        ensure!(!reason.trim().is_empty(), "review governor gate is empty");
        let reason = truncate_utf8(reason.trim(), 2_000);
        let now = Utc::now().to_rfc3339();
        let changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = 'queued', claim_token = 'queued:' || run_id,
                 claim_expires_at = ?3, claimed_at = ?3,
                 last_error = ?4, completed_at = NULL, updated_at = ?3
             WHERE id = ?1 AND claim_token = ?2 AND status = 'claimed'
               AND provider_call_id IS NULL",
            params![review_id, claim_token, now, reason],
        )?;
        ensure!(changed == 1, "Introduction review claim cannot be deferred");
        Ok(())
    }

    /// Attach the one provider call to its durable claim before parsing or
    /// proposal persistence. The normalized usage envelope is intentionally
    /// small and contains counters only, never transcript or model output.
    pub(crate) fn record_provider_call(
        &self,
        review_id: &str,
        claim_token: &str,
        provider_call_id: &str,
        usage: Option<&Usage>,
    ) -> Result<bool> {
        ensure!(
            !provider_call_id.trim().is_empty(),
            "provider call id is empty"
        );
        let usage_json = usage.map(serde_json::to_string).transpose()?;
        ensure!(
            usage_json
                .as_ref()
                .is_none_or(|json| json.len() <= MAX_REVIEW_USAGE_BYTES),
            "provider usage exceeds the Introduction audit limit"
        );
        let changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET provider_call_id = ?3, usage_json = ?4, updated_at = ?5
             WHERE id = ?1 AND claim_token = ?2
               AND status IN ('claimed', 'stale')
               AND (provider_call_id IS NULL OR provider_call_id = ?3)",
            params![
                review_id,
                claim_token,
                provider_call_id,
                usage_json,
                Utc::now().to_rfc3339()
            ],
        )?;
        ensure!(
            changed == 1,
            "Introduction review claim cannot accept provider provenance"
        );
        let status: String = self.conn.query_row(
            "SELECT status FROM hive_worker_introduction_reviews
             WHERE id = ?1 AND claim_token = ?2",
            params![review_id, claim_token],
            |row| row.get(0),
        )?;
        Ok(status == "claimed")
    }

    pub(crate) fn mark_gather_more(
        &self,
        review_id: &str,
        claim_token: &str,
        output: &WorkerIntroductionReviewerOutputV1,
    ) -> Result<()> {
        ensure!(
            output.readiness == WorkerIntroductionReviewReadiness::GatherMore
                && output.facts.len() <= MAX_WORKER_INTRODUCTION_FACTS
                && !output.evidence_coverage().is_complete(),
            "gather-more review output has invalid evidence coverage"
        );
        let output_json = serde_json::to_string(output)?;
        let now = Utc::now().to_rfc3339();
        let changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = 'gather_more', reviewer_output_json = ?3,
                 last_error = NULL, completed_at = ?4, updated_at = ?4
             WHERE id = ?1 AND claim_token = ?2 AND status = 'claimed'",
            params![review_id, claim_token, output_json, now],
        )?;
        ensure!(
            changed == 1,
            "Introduction review claim is no longer active"
        );
        self.conn.execute(
            "UPDATE hive_worker_introductions
             SET last_error = NULL, updated_at = ?2
             WHERE worker_id = (
                 SELECT worker_id FROM hive_worker_introduction_reviews WHERE id = ?1
             ) AND status = 'awaiting_context'",
            params![review_id, now],
        )?;
        Ok(())
    }

    pub(crate) fn mark_stale(
        &self,
        review_id: &str,
        claim_token: &str,
        reason: &str,
    ) -> Result<()> {
        ensure!(!reason.trim().is_empty(), "stale review reason is empty");
        let reason = truncate_utf8(reason.trim(), 2_000);
        let now = Utc::now().to_rfc3339();
        let changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = 'stale', last_error = ?3,
                 completed_at = ?4, updated_at = ?4
             WHERE id = ?1 AND claim_token = ?2 AND status = 'claimed'",
            params![review_id, claim_token, reason, now],
        )?;
        ensure!(
            changed == 1,
            "Introduction review claim is no longer active"
        );
        self.project_exhaustion_for_review(review_id)?;
        Ok(())
    }

    /// Persist a typed proposal only while the exact claimed transcript is
    /// still current. A message that commits while the model is running wins:
    /// the review is marked stale and the Introduction remains
    /// `awaiting_context` with no proposal.
    pub(crate) fn persist_proposal(
        &self,
        review_id: &str,
        claim_token: &str,
        output: &WorkerIntroductionReviewerOutputV1,
        proposal: &WorkerIntroductionProposalV1,
    ) -> Result<ReviewProposalPersistence> {
        ensure!(
            output.readiness == WorkerIntroductionReviewReadiness::ReviewReady
                && output.facts.len() <= MAX_WORKER_INTRODUCTION_FACTS
                && output.evidence_coverage().is_complete(),
            "review-ready output does not cover every required Introduction axis"
        );
        let review = self
            .get_by_id(review_id)?
            .ok_or_else(|| anyhow::anyhow!("Introduction review claim not found"))?;
        ensure!(
            review.status == WorkerIntroductionReviewStatus::Claimed
                && review.claim_token == claim_token,
            "Introduction review claim is no longer active"
        );
        validate_proposal_matches_review(proposal, &review)?;

        let lifecycle_status: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT status, proposal_revision
                 FROM hive_worker_introductions WHERE worker_id = ?1",
                [&review.worker_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let expected_revision = lifecycle_status
            .as_ref()
            .and_then(|(_, revision)| u32::try_from(*revision).ok())
            .and_then(|revision| revision.checked_add(1));
        let lifecycle_current = lifecycle_status
            .as_ref()
            .is_some_and(|(status, _)| status == "awaiting_context")
            && expected_revision == Some(proposal.revision);
        let worker_binding_current = exact_review_worker_binding_current(
            self.conn,
            &review.worker_id,
            &review.session_id,
            review.worker_user_id.as_deref(),
            &review.model,
            &review.model_key,
            review.model_catalog_revision.as_deref(),
            review.provider_id,
        )?;
        let transcript_current =
            no_newer_canonical_message(self.conn, &review.session_id, review.through_message_id)?;
        if !lifecycle_current || !worker_binding_current || !transcript_current {
            self.mark_stale(review_id, claim_token, "conversation changed during review")?;
            return Ok(ReviewProposalPersistence::Stale);
        }

        let proposal_json = serde_json::to_string(proposal)?;
        let output_json = serde_json::to_string(output)?;
        let now = Utc::now().to_rfc3339();
        let lifecycle_changed = self.conn.execute(
            "UPDATE hive_worker_introductions
             SET status = 'review_ready', proposal_json = ?2,
                 proposal_revision = ?3, decision_json = NULL,
                 last_error = NULL, updated_at = ?4
             WHERE worker_id = ?1 AND status = 'awaiting_context'
               AND proposal_revision = ?3 - 1",
            params![review.worker_id, proposal_json, proposal.revision, now],
        )?;
        ensure!(
            lifecycle_changed == 1,
            "Introduction lifecycle changed while persisting its proposal"
        );
        let review_changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = 'review_ready', proposal_id = ?3,
                 proposal_revision = ?4, reviewer_output_json = ?5,
                 proposal_json = ?6, last_error = NULL,
                 completed_at = ?7, updated_at = ?7
             WHERE id = ?1 AND claim_token = ?2 AND status = 'claimed'",
            params![
                review_id,
                claim_token,
                proposal.proposal_id,
                proposal.revision,
                output_json,
                proposal_json,
                now,
            ],
        )?;
        ensure!(
            review_changed == 1,
            "Introduction review changed while persisting its proposal"
        );
        Ok(ReviewProposalPersistence::ReviewReady)
    }

    pub(crate) fn mark_decided(
        &self,
        proposal_id: &str,
        proposal_revision: u32,
        decision: &WorkerIntroductionDecisionV1,
    ) -> Result<HiveWorkerIntroduction> {
        let target = match decision.decision {
            WorkerIntroductionDecisionKind::Confirmed => "confirmed",
            WorkerIntroductionDecisionKind::Rejected => "rejected",
            WorkerIntroductionDecisionKind::KeepTalking => "keep_talking",
        };
        let now = &decision.decided_at;
        let review_changed = self.conn.execute(
            "UPDATE hive_worker_introduction_reviews
             SET status = ?3, decision_json = ?4,
                 completed_at = COALESCE(completed_at, ?5), updated_at = ?5
             WHERE proposal_id = ?1 AND proposal_revision = ?2
               AND status = 'review_ready'",
            params![
                proposal_id,
                proposal_revision,
                target,
                serde_json::to_string(decision)?,
                now
            ],
        )?;
        ensure!(
            review_changed == 1,
            "Introduction proposal is no longer reviewable"
        );

        let decision_json = serde_json::to_string(decision)?;
        let (lifecycle_target, proposal_json) = match decision.decision {
            WorkerIntroductionDecisionKind::Confirmed => ("confirmed", Some("keep")),
            WorkerIntroductionDecisionKind::Rejected
            | WorkerIntroductionDecisionKind::KeepTalking => ("awaiting_context", None),
        };
        let sql = if proposal_json.is_some() {
            format!(
                "UPDATE hive_worker_introductions
                 SET status = ?4, decision_json = ?5, last_error = NULL,
                     completed_at = COALESCE(completed_at, ?6), updated_at = ?6
                 WHERE worker_id = ?1 AND status = 'review_ready'
                   AND proposal_revision = ?2
                   AND json_extract(proposal_json, '$.proposal_id') = ?3
                 RETURNING {INTRODUCTION_COLUMNS}"
            )
        } else {
            format!(
                "UPDATE hive_worker_introductions
                 SET status = ?4, proposal_json = NULL,
                     decision_json = ?5, last_error = NULL,
                     completed_at = NULL, updated_at = ?6
                 WHERE worker_id = ?1 AND status = 'review_ready'
                   AND proposal_revision = ?2
                   AND json_extract(proposal_json, '$.proposal_id') = ?3
                 RETURNING {INTRODUCTION_COLUMNS}"
            )
        };
        let transitioned = self
            .conn
            .query_row(
                &sql,
                params![
                    decision.worker_id,
                    proposal_revision,
                    proposal_id,
                    lifecycle_target,
                    decision_json,
                    now,
                ],
                map_introduction,
            )
            .optional()?;
        transitioned.ok_or_else(|| {
            anyhow::anyhow!("Introduction lifecycle changed before its decision was recorded")
        })
    }

    fn project_exhausted_error(&self, worker_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE hive_worker_introductions
             SET last_error = ?2, updated_at = ?3
             WHERE worker_id = ?1 AND status = 'awaiting_context'",
            params![worker_id, EXHAUSTED_REVIEW_ERROR, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    fn project_exhaustion_for_review(&self, review_id: &str) -> Result<()> {
        let (worker_id, through_message_id): (String, i64) = self.conn.query_row(
            "SELECT worker_id, through_message_id
             FROM hive_worker_introduction_reviews WHERE id = ?1",
            [review_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let attempts: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM hive_worker_introduction_reviews
             WHERE worker_id = ?1 AND through_message_id = ?2
               AND (
                   status = 'failed'
                   OR (status = 'stale' AND provider_call_id IS NOT NULL)
               )",
            params![worker_id, through_message_id],
            |row| row.get(0),
        )?;
        if attempts >= MAX_AUTOMATIC_REVIEW_ATTEMPTS {
            self.project_exhausted_error(&worker_id)?;
        }
        Ok(())
    }
}

fn canonical_user_evidence_text(content_json: &str) -> Result<Option<String>> {
    let content = serde_json::from_str::<Vec<Content>>(content_json)
        .context("decoding Worker Introduction evidence message")?;
    let text = content
        .into_iter()
        .filter_map(|block| match block {
            Content::Text { text, .. } => Some(text.trim().to_string()),
            Content::Image { .. }
            | Content::Document { .. }
            | Content::ToolUse { .. }
            | Content::ToolResult { .. }
            | Content::Thinking { .. }
            | Content::RedactedThinking { .. } => None,
        })
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    Ok((!text.is_empty()).then_some(text))
}

/// Atomically fence the Introduction lifecycle and append its one canonical
/// assistant-first opening.
///
/// The lifecycle row, run, Worker DM binding, empty transcript, keyed message,
/// and transition to `awaiting_context` are one IMMEDIATE transaction. A
/// concurrent Skip therefore either wins before this commit (and no opening
/// can appear) or observes the already-committed opening. The first committed
/// keyed payload remains authoritative across process retries.
pub fn save_worker_introduction_opening_once(
    db: &Database,
    worker_id: &str,
    run_id: &str,
    session_id: &str,
    content_json: &str,
    idempotency_key: &str,
) -> Result<i64> {
    validate_ids(worker_id, Some(run_id))?;
    ensure!(!session_id.trim().is_empty(), "Hive session id is empty");
    ensure!(
        !idempotency_key.trim().is_empty(),
        "message idempotency key is empty"
    );
    let canonical_content = serde_json::from_str::<Vec<Content>>(content_json)
        .context("decoding Worker Introduction opening")?;
    ensure!(
        matches!(
            canonical_content.as_slice(),
            [Content::Text { text, .. }] if !text.trim().is_empty()
        ),
        "Worker Introduction opening must be exactly one non-empty text block"
    );

    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let binding = crate::storage::resolve_worker_conversation_with_conn(&tx, session_id)?
        .context("Worker Introduction session is no longer Worker-bound")?;
    let worker = binding.worker;
    ensure!(
        binding.group_id.is_none()
            && worker.id == worker_id
            && worker.status == crate::storage::HiveWorkerStatus::Active
            && worker.dm_session_id.as_deref() == Some(session_id),
        "Worker Introduction requires the active Worker's exact private DM"
    );
    let session_binding = tx
        .query_row(
            "SELECT user_id, session_type, model, model_key_json,
                    model_catalog_revision, permission_mode
             FROM sessions WHERE id = ?1",
            [session_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?
        .context("Worker Introduction private DM is missing")?;
    let session_model_key = session_binding
        .3
        .as_deref()
        .map(serde_json::from_str::<ModelKey>)
        .transpose()
        .context("Worker Introduction private DM has an invalid exact model key")?;
    ensure!(
        session_binding.0.as_deref() == worker.user_id.as_deref()
            && session_binding.1 == "hive"
            && session_binding.2.as_deref() == worker.model.as_deref()
            && session_model_key.as_ref() == worker.model_key.as_ref()
            && session_binding.4.as_deref() == worker.model_catalog_revision.as_deref()
            && session_binding.5 == worker.permission_mode.as_str(),
        "Worker Introduction private DM owner or exact model binding changed"
    );
    let run_binding = tx
        .query_row(
            "SELECT worker_id, session_id, kind, status, config_json
             FROM hive_runs WHERE id = ?1",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?
        .context("Worker Introduction run is missing")?;
    let run_config: serde_json::Value = serde_json::from_str(&run_binding.4)
        .context("Worker Introduction run has invalid frozen configuration")?;
    let frozen_worker_id = run_config
        .get("worker_id")
        .and_then(serde_json::Value::as_str)
        .context("Worker Introduction run has no frozen Worker id")?;
    let frozen_model = run_config
        .get("model")
        .and_then(serde_json::Value::as_str)
        .context("Worker Introduction run has no frozen model")?;
    let frozen_model_key = serde_json::from_value::<ModelKey>(
        run_config
            .get("model_key")
            .cloned()
            .context("Worker Introduction run has no frozen exact model key")?,
    )
    .context("Worker Introduction run has an invalid frozen exact model key")?;
    let frozen_catalog_revision = match run_config.get("model_catalog_revision") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(revision)) => Some(revision.as_str()),
        Some(_) => anyhow::bail!("Worker Introduction run has an invalid catalog revision"),
    };
    let frozen_permission_mode = run_config
        .get("permission_mode")
        .and_then(serde_json::Value::as_str)
        .context("Worker Introduction run has no frozen permission mode")?;
    ensure!(
        run_binding.0.as_deref() == Some(worker_id)
            && run_binding.1.as_deref() == Some(session_id)
            && run_binding.2 == "worker_introduction"
            && run_binding.3 == "running"
            && frozen_worker_id == worker_id
            && worker.model.as_deref() == Some(frozen_model)
            && worker.model_key.as_ref() == Some(&frozen_model_key)
            && worker.model_catalog_revision.as_deref() == frozen_catalog_revision
            && worker.permission_mode.as_str() == frozen_permission_mode,
        "Worker Introduction run no longer matches its active Worker, session, or frozen model"
    );
    let lifecycle_ready = tx.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM hive_worker_introductions introduction
             JOIN hive_workers worker ON worker.id = introduction.worker_id
             JOIN hive_runs run ON run.id = introduction.run_id
             JOIN sessions session ON session.id = worker.dm_session_id
             WHERE introduction.worker_id = ?1
               AND introduction.run_id = ?2
               AND introduction.status IN ('running', 'awaiting_context')
               AND worker.dm_session_id = ?3
               AND worker.status = 'active'
               AND session.session_type = 'hive'
               AND session.user_id IS worker.user_id
               AND NOT EXISTS (
                   SELECT 1 FROM hive_group_worker_lanes lane
                   WHERE lane.session_id = worker.dm_session_id
               )
               AND run.worker_id = ?1
               AND run.session_id = ?3
               AND run.kind = 'worker_introduction'
               AND run.status = 'running'
         )",
        params![worker_id, run_id, session_id],
        |row| row.get::<_, bool>(0),
    )?;
    ensure!(
        lifecycle_ready,
        "Worker Introduction is no longer running; refusing a late opening"
    );

    let existing = tx
        .query_row(
            "SELECT id, role, content, created_at
             FROM messages
             WHERE session_id = ?1 AND idempotency_key = ?2",
            params![session_id, idempotency_key],
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
    let canonical = if let Some(existing) = existing {
        ensure!(
            existing.1 == "assistant",
            "Introduction idempotency key belongs to a non-assistant message"
        );
        let earlier: i64 = tx.query_row(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND id < ?2",
            params![session_id, existing.0],
            |row| row.get(0),
        )?;
        ensure!(
            earlier == 0,
            "keyed Introduction opening is not the session's first message"
        );
        existing
    } else {
        let message_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
            [session_id],
            |row| row.get(0),
        )?;
        ensure!(
            message_count == 0,
            "session already has messages; refusing a late Introduction opening"
        );
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES (?1, 'assistant', ?2, ?3, ?4)",
            params![session_id, content_json, now, idempotency_key],
        )?;
        let message_id = tx.last_insert_rowid();
        tx.execute(
            "UPDATE sessions SET updated_at = ?1 WHERE id = ?2",
            params![now, session_id],
        )?;
        (
            message_id,
            "assistant".to_string(),
            content_json.to_string(),
            now,
        )
    };

    let now = Utc::now().to_rfc3339();
    let transitioned = tx.execute(
        "UPDATE hive_worker_introductions
         SET status = 'awaiting_context', opening_message_id = ?3,
             last_error = NULL, completed_at = NULL, updated_at = ?4
         WHERE worker_id = ?1 AND run_id = ?2
           AND status IN ('running', 'awaiting_context')
           AND (opening_message_id IS NULL OR opening_message_id = ?3)",
        params![worker_id, run_id, canonical.0, now],
    )?;
    ensure!(
        transitioned == 1,
        "Worker Introduction changed before its opening could be committed"
    );
    tx.commit()?;

    if let Err(error) = EpisodeStore::new(db).record_message(
        session_id,
        canonical.0,
        &canonical.1,
        &canonical.2,
        &canonical.3,
    ) {
        tracing::warn!(
            session_id,
            message_id = canonical.0,
            error = %error,
            "Worker Introduction opening saved but episodic recall indexing failed"
        );
    }
    Ok(canonical.0)
}

fn ineligible_review_projection(
    worker_id: &str,
    lifecycle: HiveWorkerIntroduction,
    state: WorkerIntroductionReviewProjectionState,
    reason: &str,
) -> WorkerIntroductionReviewProjection {
    WorkerIntroductionReviewProjection {
        worker_id: worker_id.to_string(),
        lifecycle_status: lifecycle.status,
        state,
        current_through_message_id: None,
        review_through_message_id: None,
        review_status: None,
        is_current_through: false,
        has_pending_user_input: false,
        attempt_count: 0,
        should_poll: false,
        last_error: lifecycle
            .last_error
            .or_else(|| (!reason.is_empty()).then(|| truncate_utf8(reason, 2_000).to_string())),
    }
}

fn terminal_review_projection(
    conn: &Connection,
    worker_id: &str,
    lifecycle: HiveWorkerIntroduction,
    state: WorkerIntroductionReviewProjectionState,
) -> Result<WorkerIntroductionReviewProjection> {
    let latest_review_id = conn
        .query_row(
            "SELECT id FROM hive_worker_introduction_reviews
             WHERE worker_id = ?1
             ORDER BY through_message_id DESC, claimed_at DESC, rowid DESC
             LIMIT 1",
            [worker_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let latest_review = latest_review_id
        .as_deref()
        .map(|review_id| {
            WorkerIntroductionReviewStore::from_connection(conn)
                .get_by_id(review_id)?
                .context("latest terminal Worker Introduction review disappeared")
        })
        .transpose()?;
    let current_through_message_id = conn.query_row(
        "SELECT MAX(message.id) FROM messages message
         JOIN hive_workers worker ON worker.id = ?1
         WHERE message.session_id = worker.dm_session_id
           AND message.role NOT LIKE 'pending_user:%'",
        [worker_id],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    let review_through_message_id = latest_review
        .as_ref()
        .map(|review| review.through_message_id);
    let has_pending_user_input = conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM messages pending
             JOIN hive_workers worker ON worker.id = ?1
             WHERE pending.session_id = worker.dm_session_id
               AND pending.role LIKE 'pending_user:%'
         )",
        [worker_id],
        |row| row.get(0),
    )?;
    let last_error = lifecycle
        .last_error
        .clone()
        .or_else(|| {
            latest_review
                .as_ref()
                .and_then(|review| review.last_error.clone())
        })
        .map(|error| truncate_utf8(&error, 2_000).to_string());
    Ok(WorkerIntroductionReviewProjection {
        worker_id: worker_id.to_string(),
        lifecycle_status: lifecycle.status,
        state,
        current_through_message_id,
        review_through_message_id,
        review_status: latest_review.as_ref().map(|review| review.status),
        is_current_through: current_through_message_id.is_some()
            && current_through_message_id == review_through_message_id,
        has_pending_user_input,
        attempt_count: 0,
        should_poll: false,
        last_error,
    })
}

fn review_input_error_matches_through(error: &str, through_message_id: i64) -> bool {
    error.starts_with(&format!(
        "{REVIEW_NEEDS_ATTENTION_PREFIX}{through_message_id}:"
    ))
}

fn review_profile_basis_current(
    conn: &Connection,
    review: &WorkerIntroductionReviewRecord,
) -> Result<bool> {
    let mut digests = Vec::with_capacity(2);
    for kind in ["identity", "soul"] {
        let content = conn
            .query_row(
                "SELECT content FROM hive_worker_documents
                 WHERE worker_id = ?1 AND kind = ?2",
                params![review.worker_id, kind],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        digests.push(format!(
            "sha256:{:x}",
            Sha256::digest(content.unwrap_or_default().as_bytes())
        ));
    }
    Ok(digests[0] == review.base_identity_digest && digests[1] == review.base_soul_digest)
}

fn validate_ids(worker_id: &str, run_id: Option<&str>) -> Result<()> {
    ensure!(!worker_id.trim().is_empty(), "Hive Worker id is empty");
    if let Some(run_id) = run_id {
        ensure!(!run_id.trim().is_empty(), "Introduction run id is empty");
    }
    Ok(())
}

#[cfg(test)]
fn validate_review_claim(input: &NewWorkerIntroductionReviewClaim) -> Result<()> {
    validate_ids(&input.worker_id, None)?;
    ensure!(
        !input.session_id.trim().is_empty(),
        "Hive session id is empty"
    );
    ensure!(!input.model.trim().is_empty(), "review model is empty");
    ensure!(
        input.model_key.model_id.as_str() == input.model.as_str(),
        "review model does not match its exact model key"
    );
    if let Some(user_id) = input.worker_user_id.as_deref() {
        ensure!(!user_id.trim().is_empty(), "review Worker owner is empty");
    }
    if let Some(revision) = input.model_catalog_revision.as_deref() {
        ensure!(
            !revision.trim().is_empty(),
            "review model catalog revision is empty"
        );
    }
    ensure!(
        input.opening_message_id > 0,
        "opening message id must be positive"
    );
    ensure!(
        input.through_message_id >= input.opening_message_id,
        "review transcript ends before its opening"
    );
    ensure!(
        !input.user_message_ids.is_empty(),
        "Introduction review requires a real user reply"
    );
    let mut ids = input.user_message_ids.clone();
    ids.sort_unstable();
    ids.dedup();
    ensure!(
        ids == input.user_message_ids,
        "Introduction user message ids must be sorted and unique"
    );
    for digest in [
        &input.transcript_digest,
        &input.base_identity_digest,
        &input.base_soul_digest,
    ] {
        ensure!(
            valid_sha256_digest(digest),
            "invalid Introduction SHA-256 digest"
        );
    }
    Ok(())
}

#[cfg(test)]
fn provider_id_storage_value(provider_id: ProviderId) -> Result<String> {
    serde_json::to_value(provider_id)?
        .as_str()
        .map(ToOwned::to_owned)
        .context("serialized provider id is not a string")
}

#[allow(clippy::too_many_arguments)]
fn exact_review_worker_binding_current(
    conn: &Connection,
    worker_id: &str,
    session_id: &str,
    worker_user_id: Option<&str>,
    model: &str,
    model_key: &ModelKey,
    model_catalog_revision: Option<&str>,
    provider_id: ProviderId,
) -> Result<bool> {
    let Some(binding) = crate::storage::resolve_worker_conversation_with_conn(conn, session_id)?
    else {
        return Ok(false);
    };
    let worker = &binding.worker;
    if binding.group_id.is_some()
        || worker.id != worker_id
        || worker.status != crate::storage::HiveWorkerStatus::Active
        || worker.user_id.as_deref() != worker_user_id
        || worker.dm_session_id.as_deref() != Some(session_id)
        || worker.model.as_deref() != Some(model)
        || worker.model_key.as_ref() != Some(model_key)
        || worker.model_catalog_revision.as_deref() != model_catalog_revision
        || model_key.provider != provider_id
    {
        return Ok(false);
    }
    let session = conn
        .query_row(
            "SELECT user_id, session_type, model, model_key_json,
                    model_catalog_revision, permission_mode
             FROM sessions WHERE id = ?1",
            [session_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((user_id, session_type, session_model, session_key_json, revision, permission)) =
        session
    else {
        return Ok(false);
    };
    let session_key = session_key_json
        .as_deref()
        .map(serde_json::from_str::<ModelKey>)
        .transpose()?;
    Ok(user_id.as_deref() == worker_user_id
        && session_type == "hive"
        && session_model.as_deref() == Some(model)
        && session_key.as_ref() == Some(model_key)
        && revision.as_deref() == model_catalog_revision
        && permission == worker.permission_mode.as_str())
}

fn validate_proposal_matches_review(
    proposal: &WorkerIntroductionProposalV1,
    review: &WorkerIntroductionReviewRecord,
) -> Result<()> {
    ensure!(
        proposal.schema_version == WORKER_INTRODUCTION_PROPOSAL_VERSION,
        "unsupported Worker Introduction proposal version"
    );
    ensure!(
        !proposal.proposal_id.trim().is_empty(),
        "Introduction proposal id is empty"
    );
    ensure!(
        proposal.worker_id == review.worker_id && proposal.session_id == review.session_id,
        "Introduction proposal binding does not match its review claim"
    );
    ensure!(
        proposal.basis.opening_message_id == review.opening_message_id
            && proposal.basis.through_message_id == review.through_message_id
            && proposal.basis.user_message_ids == review.user_message_ids
            && proposal.basis.transcript_digest == review.transcript_digest,
        "Introduction proposal basis does not match its review claim"
    );
    ensure!(
        proposal.base_identity_digest == review.base_identity_digest
            && proposal.base_soul_digest == review.base_soul_digest,
        "Introduction proposal profile basis does not match its review claim"
    );
    ensure!(
        !proposal.facts.is_empty() && proposal.facts.len() <= MAX_WORKER_INTRODUCTION_FACTS,
        "Introduction proposal fact count is invalid"
    );
    Ok(())
}

#[cfg(test)]
fn valid_sha256_digest(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
fn ensure_no_newer_canonical_message(
    conn: &Connection,
    session_id: &str,
    through_message_id: i64,
) -> Result<()> {
    ensure!(
        no_newer_canonical_message(conn, session_id, through_message_id)?,
        "Hive Worker Introduction transcript changed before review claim"
    );
    Ok(())
}

fn no_newer_canonical_message(
    conn: &Connection,
    session_id: &str,
    through_message_id: i64,
) -> Result<bool> {
    conn.query_row(
        "SELECT NOT EXISTS(
             SELECT 1 FROM messages
             WHERE session_id = ?1
               AND (
                   role LIKE 'pending_user:%'
                   OR (id > ?2 AND role NOT LIKE 'pending_user:%')
               )
         )",
        params![session_id, through_message_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn map_introduction(row: &Row<'_>) -> rusqlite::Result<HiveWorkerIntroduction> {
    let raw_status: String = row.get(2)?;
    let status = HiveWorkerIntroductionStatus::parse(&raw_status).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            Type::Text,
            Box::new(IoError::new(
                ErrorKind::InvalidData,
                format!("invalid Hive Worker Introduction status: {raw_status}"),
            )),
        )
    })?;
    let prompt_version: i64 = row.get(3)?;
    let prompt_version = u32::try_from(prompt_version).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(3, Type::Integer, Box::new(error))
    })?;
    let proposal_json: Option<String> = row.get(5)?;
    let proposal = proposal_json
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(error))
        })?;
    let proposal_revision: i64 = row.get(6)?;
    let proposal_revision = u32::try_from(proposal_revision).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, Type::Integer, Box::new(error))
    })?;
    let decision_json: Option<String> = row.get(7)?;
    let decision = decision_json
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(7, Type::Text, Box::new(error))
        })?;
    Ok(HiveWorkerIntroduction {
        worker_id: row.get(0)?,
        run_id: row.get(1)?,
        status,
        prompt_version,
        opening_message_id: row.get(4)?,
        proposal,
        proposal_revision,
        decision,
        last_error: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
        completed_at: row.get(11)?,
    })
}

fn map_review(row: &Row<'_>) -> rusqlite::Result<WorkerIntroductionReviewRecord> {
    let raw_status: String = row.get(3)?;
    let status = WorkerIntroductionReviewStatus::parse(&raw_status).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            3,
            Type::Text,
            Box::new(IoError::new(
                ErrorKind::InvalidData,
                format!("invalid Hive Worker Introduction review status: {raw_status}"),
            )),
        )
    })?;
    let user_message_ids_json: String = row.get(8)?;
    let user_message_ids = serde_json::from_str(&user_message_ids_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(8, Type::Text, Box::new(error))
    })?;
    let model_key_json: String = row.get(14)?;
    let model_key = serde_json::from_str(&model_key_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(14, Type::Text, Box::new(error))
    })?;
    let provider_id_raw: String = row.get(16)?;
    let provider_id =
        serde_json::from_value(serde_json::Value::String(provider_id_raw)).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(16, Type::Text, Box::new(error))
        })?;
    let usage_json: Option<String> = row.get(19)?;
    let usage = usage_json
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(19, Type::Text, Box::new(error))
        })?;
    let proposal_revision = row
        .get::<_, Option<i64>>(21)?
        .map(|revision| {
            u32::try_from(revision).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(21, Type::Integer, Box::new(error))
            })
        })
        .transpose()?;
    let reviewer_output_json: Option<String> = row.get(22)?;
    let reviewer_output = reviewer_output_json
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(22, Type::Text, Box::new(error))
        })?;
    let proposal_json: Option<String> = row.get(23)?;
    let proposal = proposal_json
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(23, Type::Text, Box::new(error))
        })?;
    let decision_json: Option<String> = row.get(24)?;
    let decision = decision_json
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(24, Type::Text, Box::new(error))
        })?;
    Ok(WorkerIntroductionReviewRecord {
        id: row.get(0)?,
        worker_id: row.get(1)?,
        session_id: row.get(2)?,
        status,
        claim_token: row.get(4)?,
        claim_expires_at: row.get(5)?,
        opening_message_id: row.get(6)?,
        through_message_id: row.get(7)?,
        user_message_ids,
        transcript_digest: row.get(9)?,
        base_identity_digest: row.get(10)?,
        base_soul_digest: row.get(11)?,
        worker_user_id: row.get(12)?,
        model: row.get(13)?,
        model_key,
        model_catalog_revision: row.get(15)?,
        provider_id,
        trace_run_id: row.get(17)?,
        provider_call_id: row.get(18)?,
        usage,
        proposal_id: row.get(20)?,
        proposal_revision,
        reviewer_output,
        proposal,
        decision,
        last_error: row.get(25)?,
        claimed_at: row.get(26)?,
        created_at: row.get(27)?,
        updated_at: row.get(28)?,
        completed_at: row.get(29)?,
        run_id: row.get(30)?,
        attempt_no: row
            .get::<_, Option<i64>>(31)?
            .map(|value| {
                u32::try_from(value).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(31, Type::Integer, Box::new(error))
                })
            })
            .transpose()?,
    })
}
