use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::agent::subagent::AgentCapability;
use crate::agent::DelegatedRunStage;
use crate::storage::database::Database;
use crate::storage::delegation::cancel_foreground_group_on_caller_abort;

use super::codec::{delegated_stage_str, row_to_delegated_run, row_to_delegated_run_summary};
use super::model::{
    normalize_scope_key, DelegatedRunCreateOutcome, DelegatedRunRecord, DelegatedRunRole,
    DelegatedRunScope, DelegatedRunSnapshot, DelegatedRunStartInput, DelegatedRunSummary,
};

pub struct DelegatedRunStore {
    pub(super) db: Database,
}

/// A live host renews at a much shorter interval than this deadline. The
/// generous window prevents an overloaded runtime or a brief SQLite lock from
/// being mistaken for a dead process while still making hard-crash recovery
/// bounded.
const BACKGROUND_HOST_LEASE_TTL_MS: i64 = 120_000;
const BACKGROUND_HOST_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug)]
struct ArmedRun {
    resumable: bool,
    host_owner_id: Option<String>,
}

/// Owns the durable store for one Agent invocation and cancels any run that
/// loses its caller before a terminal result becomes durable.
///
/// The Agent tool itself is wrapped in cancellation and timeout boundaries.
/// Dropping that future also drops this lease, so a created/running row cannot
/// be left indefinitely non-terminal merely because the caller stopped
/// waiting. Terminal persistence uses the store's first-writer-wins CAS, which
/// means a completion or explicit interrupt that already won is never erased.
pub struct DelegatedRunLease {
    store: DelegatedRunStore,
    armed_runs: BTreeMap<String, ArmedRun>,
}

/// Stops the independent host heartbeat when authoritative finalization has
/// been reloaded, or when an unwinding background task drops its run lease.
pub(crate) struct DelegatedRunHostHeartbeat {
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl Drop for DelegatedRunHostHeartbeat {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl DelegatedRunLease {
    pub fn new(store: DelegatedRunStore) -> Self {
        Self {
            store,
            armed_runs: BTreeMap::new(),
        }
    }

    pub fn create_run(
        &mut self,
        input: &DelegatedRunStartInput,
    ) -> Result<DelegatedRunCreateOutcome> {
        self.create_run_with_wake_contract(input, None, &BTreeSet::new(), false)
    }

    pub fn create_run_with_child_contract(
        &mut self,
        input: &DelegatedRunStartInput,
        child_name: Option<&str>,
        capabilities: &BTreeSet<AgentCapability>,
    ) -> Result<DelegatedRunCreateOutcome> {
        self.create_run_with_wake_contract(input, child_name, capabilities, false)
    }

    pub fn create_background_run(
        &mut self,
        input: &DelegatedRunStartInput,
    ) -> Result<DelegatedRunCreateOutcome> {
        self.create_run_with_wake_contract(input, None, &BTreeSet::new(), true)
    }

    pub fn create_background_run_with_child_contract(
        &mut self,
        input: &DelegatedRunStartInput,
        child_name: Option<&str>,
        capabilities: &BTreeSet<AgentCapability>,
    ) -> Result<DelegatedRunCreateOutcome> {
        self.create_run_with_wake_contract(input, child_name, capabilities, true)
    }

    fn create_run_with_wake_contract(
        &mut self,
        input: &DelegatedRunStartInput,
        child_name: Option<&str>,
        capabilities: &BTreeSet<AgentCapability>,
        wake_parent: bool,
    ) -> Result<DelegatedRunCreateOutcome> {
        let host_owner_id = wake_parent.then(|| Uuid::new_v4().to_string());
        let outcome = self.store.create_run_with_wake_contract(
            input,
            child_name,
            capabilities,
            wake_parent,
            host_owner_id.as_deref(),
        )?;
        if outcome == DelegatedRunCreateOutcome::Created {
            self.arm(
                input.delegated_run_id.clone(),
                input.resumable,
                host_owner_id,
            );
        }
        Ok(outcome)
    }

    /// Arm the lease immediately after the durable row is created.
    fn arm(
        &mut self,
        delegated_run_id: impl Into<String>,
        resumable: bool,
        host_owner_id: Option<String>,
    ) {
        self.armed_runs.insert(
            delegated_run_id.into(),
            ArmedRun {
                resumable,
                host_owner_id,
            },
        );
    }

    /// Renew the durable process-owner lease for a background run.
    ///
    /// `false` means this invocation no longer owns a non-terminal row (for
    /// example an interrupt or a recovery worker won the terminal CAS). The
    /// caller must cancel local execution instead of continuing side effects.
    pub fn renew_background_host_lease(&self, delegated_run_id: &str) -> Result<bool> {
        let Some(armed) = self.armed_runs.get(delegated_run_id) else {
            return Ok(false);
        };
        let Some(host_owner_id) = armed.host_owner_id.as_deref() else {
            return Ok(false);
        };
        self.store
            .renew_background_host_lease(delegated_run_id, host_owner_id)
    }

    /// Publish a terminal result only while this exact background owner still
    /// holds an unexpired lease. Foreground and explicit user cancellation use
    /// the store's ordinary first-writer-wins terminal API instead.
    pub fn finalize_background_run(
        &self,
        delegated_run_id: &str,
        stage: DelegatedRunStage,
        artifact: &Value,
        human_review: Option<&str>,
        resumable: bool,
    ) -> Result<()> {
        let armed = self
            .armed_runs
            .get(delegated_run_id)
            .with_context(|| format!("delegated run '{delegated_run_id}' is not armed"))?;
        let host_owner_id = armed
            .host_owner_id
            .as_deref()
            .with_context(|| format!("delegated run '{delegated_run_id}' has no host owner"))?;
        self.store.finalize_owned_background_run(
            delegated_run_id,
            host_owner_id,
            stage,
            artifact,
            human_review,
            resumable,
        )
    }

    /// Start a heartbeat on its own Tokio task so a slow provider request does
    /// not prevent the durable host lease from being renewed.
    pub(crate) fn start_background_host_heartbeat(
        &self,
        delegated_run_id: &str,
        execution_cancellation: CancellationToken,
    ) -> Result<DelegatedRunHostHeartbeat> {
        let armed = self
            .armed_runs
            .get(delegated_run_id)
            .with_context(|| format!("delegated run '{delegated_run_id}' is not armed"))?;
        let host_owner_id = armed
            .host_owner_id
            .clone()
            .with_context(|| format!("delegated run '{delegated_run_id}' has no host owner"))?;
        anyhow::ensure!(
            self.store
                .renew_background_host_lease(delegated_run_id, &host_owner_id)?,
            "delegated run '{delegated_run_id}' lost its host lease before execution"
        );

        let database_path = self.store.database_path()?;
        let heartbeat_store = DelegatedRunStore::new(Database::new(&database_path)?);
        let delegated_run_id = delegated_run_id.to_string();
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(BACKGROUND_HOST_HEARTBEAT_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // `interval` ticks immediately once. The synchronous renewal above
            // already established this lease, so consume that tick first.
            interval.tick().await;
            let mut last_success = Instant::now();
            loop {
                tokio::select! {
                    _ = task_stop.cancelled() => break,
                    _ = interval.tick() => {
                        match heartbeat_store.renew_background_host_lease(
                            &delegated_run_id,
                            &host_owner_id,
                        ) {
                            Ok(true) => last_success = Instant::now(),
                            Ok(false) => {
                                execution_cancellation.cancel();
                                break;
                            }
                            Err(error) => {
                                warn!(
                                    delegated_run_id,
                                    %error,
                                    "Failed to renew background Agent host lease"
                                );
                                if last_success.elapsed()
                                    >= Duration::from_millis(BACKGROUND_HOST_LEASE_TTL_MS as u64)
                                {
                                    execution_cancellation.cancel();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(DelegatedRunHostHeartbeat {
            stop,
            task: Some(task),
        })
    }

    /// Disarm only after the authoritative terminal row has been reloaded.
    pub fn disarm(&mut self, delegated_run_id: &str) -> bool {
        self.armed_runs.remove(delegated_run_id).is_some()
    }

    /// Remove a compatibility row that was prepared but never admitted into
    /// its canonical delegation group. This is intentionally restricted to
    /// the exact still-created row owned by this lease; once execution or any
    /// other lifecycle transition begins, ordinary terminal recovery applies.
    pub fn discard_unadmitted_run(&mut self, delegated_run_id: &str) -> Result<bool> {
        let Some(armed) = self.armed_runs.get(delegated_run_id) else {
            return Ok(false);
        };
        let discarded = self
            .store
            .discard_unadmitted_run(delegated_run_id, armed.host_owner_id.as_deref())?;
        if discarded {
            self.armed_runs.remove(delegated_run_id);
        }
        Ok(discarded)
    }
}

impl Deref for DelegatedRunLease {
    type Target = DelegatedRunStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl Drop for DelegatedRunLease {
    fn drop(&mut self) {
        for (delegated_run_id, armed) in std::mem::take(&mut self.armed_runs) {
            let finalization = match armed.host_owner_id.as_deref() {
                Some(host_owner_id) => self.store.finalize_owned_background_caller_aborted_run(
                    &delegated_run_id,
                    host_owner_id,
                    armed.resumable,
                ),
                None => self
                    .store
                    .finalize_caller_aborted_run(&delegated_run_id, armed.resumable),
            };
            if let Err(error) = finalization {
                warn!(
                    delegated_run_id,
                    %error,
                    "Failed to finalize an abandoned delegated run while dropping its lease"
                );
            }
        }
    }
}

impl DelegatedRunStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    fn discard_unadmitted_run(
        &self,
        delegated_run_id: &str,
        host_owner_id: Option<&str>,
    ) -> Result<bool> {
        let deleted = self.db.conn().execute(
            "DELETE FROM delegated_runs
              WHERE delegated_run_id = ?1
                AND stage = 'created'
                AND host_owner_id IS ?2
                AND snapshot_json IS NULL
                AND artifact_json IS NULL
                AND completed_at IS NULL",
            params![delegated_run_id, host_owner_id],
        )?;
        Ok(deleted == 1)
    }

    fn database_path(&self) -> Result<PathBuf> {
        let mut stmt = self.db.conn().prepare("PRAGMA database_list")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?;
        for row in rows {
            let (name, file) = row?;
            if name == "main" && !file.is_empty() {
                return Ok(PathBuf::from(file));
            }
        }
        anyhow::bail!("delegated run storage is not backed by a durable database file")
    }

    pub fn create_run(&self, input: &DelegatedRunStartInput) -> Result<DelegatedRunCreateOutcome> {
        self.create_run_with_wake_contract(input, None, &BTreeSet::new(), false, None)
    }

    pub fn create_run_with_child_contract(
        &self,
        input: &DelegatedRunStartInput,
        child_name: Option<&str>,
        capabilities: &BTreeSet<AgentCapability>,
    ) -> Result<DelegatedRunCreateOutcome> {
        self.create_run_with_wake_contract(input, child_name, capabilities, false, None)
    }

    pub fn create_background_run(
        &self,
        input: &DelegatedRunStartInput,
    ) -> Result<DelegatedRunCreateOutcome> {
        let host_owner_id = Uuid::new_v4().to_string();
        self.create_run_with_wake_contract(
            input,
            None,
            &BTreeSet::new(),
            true,
            Some(&host_owner_id),
        )
    }

    pub fn create_background_run_with_child_contract(
        &self,
        input: &DelegatedRunStartInput,
        child_name: Option<&str>,
        capabilities: &BTreeSet<AgentCapability>,
    ) -> Result<DelegatedRunCreateOutcome> {
        let host_owner_id = Uuid::new_v4().to_string();
        self.create_run_with_wake_contract(
            input,
            child_name,
            capabilities,
            true,
            Some(&host_owner_id),
        )
    }

    fn create_run_with_wake_contract(
        &self,
        input: &DelegatedRunStartInput,
        child_name: Option<&str>,
        capabilities: &BTreeSet<AgentCapability>,
        wake_parent: bool,
        host_owner_id: Option<&str>,
    ) -> Result<DelegatedRunCreateOutcome> {
        let now = Utc::now().to_rfc3339();
        let scope_key = normalize_scope_key(&input.target_scope);
        let scope_json = serde_json::to_string(&input.target_scope)?;
        let capabilities_json = serde_json::to_string(capabilities)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;

        if let Some(resumed_from_run_id) = input.resumed_from_run_id.as_deref() {
            let existing = tx
                .query_row(
                    "SELECT delegated_run_id
                       FROM delegated_run_continuations
                      WHERE resumed_from_run_id = ?1",
                    params![resumed_from_run_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(delegated_run_id) = existing {
                tx.commit()?;
                return Ok(DelegatedRunCreateOutcome::ExistingContinuation {
                    delegated_run_id,
                    resumed_from_run_id: resumed_from_run_id.to_string(),
                });
            }
        }

        // One parent may coordinate many read-only teams, but two live writer
        // graphs against the same canonical workspace are ambiguous. A new
        // tool-call id is not sufficient ownership: models can accidentally
        // restate the same graph, and allowing both wastes provider capacity
        // while creating competing publication paths. The durable fence is
        // checked in the same transaction as insertion so reconnects and
        // multiple server processes observe one authority.
        if input.role == DelegatedRunRole::Build && capabilities.contains(&AgentCapability::Write) {
            let workspace_paths = input
                .target_scope
                .iter()
                .filter(|scope| scope.kind.trim() == "workspace")
                .map(|scope| scope.path.trim())
                .filter(|path| !path.is_empty())
                .collect::<Vec<_>>();
            for workspace_path in workspace_paths {
                let existing = tx
                    .query_row(
                        "SELECT active.delegated_run_id
                           FROM delegated_runs AS active
                          WHERE active.parent_session_id = ?1
                            AND active.role = 'build'
                            AND active.stage IN ('created', 'running', 'synthesizing')
                            AND EXISTS (
                                SELECT 1
                                  FROM json_each(active.capabilities_json) AS capability
                                 WHERE capability.value = 'write'
                            )
                            AND EXISTS (
                                SELECT 1
                                  FROM json_each(active.target_scope_json) AS scope
                                 WHERE json_extract(scope.value, '$.kind') = 'workspace'
                                   AND json_extract(scope.value, '$.path') = ?2
                            )
                          ORDER BY active.created_at ASC, active.delegated_run_id ASC
                          LIMIT 1",
                        params![input.parent_session_id, workspace_path],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                if let Some(delegated_run_id) = existing {
                    tx.commit()?;
                    return Ok(DelegatedRunCreateOutcome::ExistingActiveWorkspaceWriter {
                        delegated_run_id,
                        workspace_path: workspace_path.to_string(),
                    });
                }
            }
        }

        tx.execute(
            "INSERT INTO delegated_runs (
                delegated_run_id,
                parent_session_id,
                parent_tool_call_id,
                role,
                stage,
                provider,
                model,
                resumable,
                resumed_from_run_id,
                target_scope_key,
                target_scope_json,
                child_name,
                capabilities_json,
                wake_parent,
                host_owner_id,
                host_lease_expires_at_ms,
                created_at,
                updated_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                ?15,
                CASE WHEN ?15 IS NULL THEN NULL
                     ELSE (CAST(strftime('%s', 'now') AS INTEGER) * 1000) + ?16
                END,
                ?17, ?18
            )",
            params![
                input.delegated_run_id,
                input.parent_session_id,
                input.parent_tool_call_id,
                input.role.as_str(),
                delegated_stage_str(input.stage),
                input.provider,
                input.model,
                if input.resumable { 1 } else { 0 },
                input.resumed_from_run_id,
                scope_key,
                scope_json,
                child_name,
                capabilities_json,
                if wake_parent { 1 } else { 0 },
                host_owner_id,
                BACKGROUND_HOST_LEASE_TTL_MS,
                now,
                now,
            ],
        )?;
        if let Some(resumed_from_run_id) = input.resumed_from_run_id.as_deref() {
            tx.execute(
                "INSERT INTO delegated_run_continuations (
                    resumed_from_run_id,
                    delegated_run_id,
                    created_at
                 ) VALUES (?1, ?2, ?3)",
                params![resumed_from_run_id, input.delegated_run_id, now],
            )?;
        }
        tx.commit()?;
        Ok(DelegatedRunCreateOutcome::Created)
    }

    fn renew_background_host_lease(
        &self,
        delegated_run_id: &str,
        host_owner_id: &str,
    ) -> Result<bool> {
        let updated = self.db.conn().execute(
            "UPDATE delegated_runs
                SET host_lease_expires_at_ms =
                    (CAST(strftime('%s', 'now') AS INTEGER) * 1000) + ?3
              WHERE delegated_run_id = ?1
                AND host_owner_id = ?2
                AND wake_parent = 1
                AND stage IN ('created', 'running', 'synthesizing')
                AND host_lease_expires_at_ms
                    > (CAST(strftime('%s', 'now') AS INTEGER) * 1000)",
            params![
                delegated_run_id,
                host_owner_id,
                BACKGROUND_HOST_LEASE_TTL_MS,
            ],
        )?;
        Ok(updated == 1)
    }

    /// Terminalize only background rows whose exact observed owner lease has
    /// expired. The immediate transaction and compare-and-set predicates fence
    /// a concurrent heartbeat or normal completion from being overwritten.
    pub fn expire_stale_background_host_leases(&self) -> Result<Vec<String>> {
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let observed = {
            let mut stmt = tx.prepare(
                "SELECT delegated_run_id, host_owner_id, host_lease_expires_at_ms
                   FROM delegated_runs
                  WHERE wake_parent = 1
                    AND stage IN ('created', 'running', 'synthesizing')
                    AND NOT EXISTS (
                        SELECT 1 FROM delegation_tasks AS replay_tasks
                         WHERE replay_tasks.delegation_group_id = delegated_runs.delegated_run_id
                           AND replay_tasks.executor_envelope_version = 1
                           AND replay_tasks.executor_envelope_json IS NOT NULL
                    )
                    AND host_owner_id IS NOT NULL
                    AND host_lease_expires_at_ms IS NOT NULL
                    AND host_lease_expires_at_ms
                        <= (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
                  ORDER BY host_lease_expires_at_ms ASC, delegated_run_id ASC",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            let mut observed = Vec::new();
            for row in rows {
                observed.push(row?);
            }
            observed
        };

        let mut expired = Vec::new();
        for (delegated_run_id, host_owner_id, observed_expiry_ms) in observed {
            let artifact = json!({
                "outcome": "cancelled",
                "outcome_reason": "background_host_lease_expired",
                "warnings": [
                    "The server process stopped renewing this background Agent before terminal persistence; side effects may have occurred."
                ],
                "recovery": "Inspect the workspace before starting a replacement. The original process owner is no longer authoritative."
            });
            let artifact_json = serde_json::to_string(&artifact)?;
            let updated = tx.execute(
                "UPDATE delegated_runs
                    SET stage = 'cancelled',
                        artifact_json = ?4,
                        human_review = ?5,
                        updated_at = ?6,
                        completed_at = ?6,
                        host_lease_expires_at_ms = NULL
                  WHERE delegated_run_id = ?1
                    AND host_owner_id = ?2
                    AND host_lease_expires_at_ms = ?3
                    AND host_lease_expires_at_ms
                        <= (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
                    AND wake_parent = 1
                    AND stage IN ('created', 'running', 'synthesizing')
                    AND NOT EXISTS (
                        SELECT 1 FROM delegation_tasks AS replay_tasks
                         WHERE replay_tasks.delegation_group_id = delegated_runs.delegated_run_id
                           AND replay_tasks.executor_envelope_version = 1
                           AND replay_tasks.executor_envelope_json IS NOT NULL
                    )",
                params![
                    delegated_run_id,
                    host_owner_id,
                    observed_expiry_ms,
                    artifact_json,
                    "Background Agent ownership expired before terminal persistence.",
                    now_text,
                ],
            )?;
            if updated == 1 {
                expired.push(delegated_run_id);
            }
        }
        tx.commit()?;
        Ok(expired)
    }

    pub fn update_snapshot(
        &self,
        delegated_run_id: &str,
        stage: DelegatedRunStage,
        snapshot: &DelegatedRunSnapshot,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(
                stage,
                DelegatedRunStage::Created
                    | DelegatedRunStage::Running
                    | DelegatedRunStage::Synthesizing
            ),
            "delegated run '{delegated_run_id}' cannot publish a terminal stage through a progress snapshot"
        );
        let updated_at = Utc::now().to_rfc3339();
        let snapshot_json = serde_json::to_string(snapshot)?;
        let updated = self.db.conn().execute(
            "UPDATE delegated_runs
             SET stage = ?2,
                 snapshot_json = ?3,
                 updated_at = ?4
             WHERE delegated_run_id = ?1
               AND stage IN ('created', 'running', 'synthesizing')",
            params![
                delegated_run_id,
                delegated_stage_str(stage),
                snapshot_json,
                updated_at,
            ],
        )?;
        if updated == 1 {
            return Ok(());
        }

        match self.get_run(delegated_run_id)? {
            Some(record)
                if matches!(
                    record.stage,
                    DelegatedRunStage::Complete
                        | DelegatedRunStage::Degraded
                        | DelegatedRunStage::Failed
                        | DelegatedRunStage::Cancelled
                ) => Ok(()),
            Some(_) => anyhow::bail!(
                "delegated run '{delegated_run_id}' snapshot was not updated because its state changed"
            ),
            None => anyhow::bail!("delegated run '{delegated_run_id}' does not exist"),
        }
    }

    pub fn finalize_run(
        &self,
        delegated_run_id: &str,
        stage: DelegatedRunStage,
        artifact: &Value,
        human_review: Option<&str>,
        resumable: bool,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(
                stage,
                DelegatedRunStage::Complete
                    | DelegatedRunStage::Degraded
                    | DelegatedRunStage::Failed
                    | DelegatedRunStage::Cancelled
            ),
            "delegated run '{delegated_run_id}' cannot be finalized with non-terminal stage {stage:?}"
        );
        let updated_at = Utc::now().to_rfc3339();
        let artifact_json = serde_json::to_string(artifact)?;
        let completed_at = updated_at.clone();

        // All terminal outcomes compete in one compare-and-set. Whichever
        // writer transitions the non-terminal row first is authoritative;
        // completion cannot overwrite cancellation and a late cancellation
        // cannot erase a completion that already became durable.
        let updated = self.db.conn().execute(
            "UPDATE delegated_runs
             SET stage = ?2,
                 artifact_json = ?3,
                 human_review = ?4,
                 resumable = ?5,
                 updated_at = ?6,
                 completed_at = ?7,
                 host_lease_expires_at_ms = NULL
             WHERE delegated_run_id = ?1
               AND stage IN ('created', 'running', 'synthesizing')",
            params![
                delegated_run_id,
                delegated_stage_str(stage),
                artifact_json,
                human_review,
                if resumable { 1 } else { 0 },
                updated_at,
                completed_at,
            ],
        )?;
        if updated == 1 {
            return Ok(());
        }

        // A terminal loser is not a storage failure. Callers that publish an
        // outcome must reload and compare the authoritative row before doing
        // so; `Ok(())` only means that a terminal winner is durable.
        match self.get_run(delegated_run_id)? {
            Some(record)
                if matches!(
                    record.stage,
                    DelegatedRunStage::Complete
                        | DelegatedRunStage::Degraded
                        | DelegatedRunStage::Failed
                        | DelegatedRunStage::Cancelled
                ) =>
            {
                Ok(())
            }
            Some(_) => anyhow::bail!(
                "delegated run '{delegated_run_id}' was not finalized because its state changed"
            ),
            None => anyhow::bail!("delegated run '{delegated_run_id}' does not exist"),
        }
    }

    fn finalize_owned_background_run(
        &self,
        delegated_run_id: &str,
        host_owner_id: &str,
        stage: DelegatedRunStage,
        artifact: &Value,
        human_review: Option<&str>,
        resumable: bool,
    ) -> Result<()> {
        anyhow::ensure!(
            matches!(
                stage,
                DelegatedRunStage::Complete
                    | DelegatedRunStage::Degraded
                    | DelegatedRunStage::Failed
                    | DelegatedRunStage::Cancelled
            ),
            "delegated run '{delegated_run_id}' cannot be finalized with non-terminal stage {stage:?}"
        );
        let updated_at = Utc::now().to_rfc3339();
        let artifact_json = serde_json::to_string(artifact)?;
        let completed_at = updated_at.clone();
        let updated = self.db.conn().execute(
            "UPDATE delegated_runs
                SET stage = ?2,
                    artifact_json = ?3,
                    human_review = ?4,
                    resumable = ?5,
                    updated_at = ?6,
                    completed_at = ?7,
                    host_lease_expires_at_ms = NULL
              WHERE delegated_run_id = ?1
                AND host_owner_id = ?8
                AND wake_parent = 1
                AND host_lease_expires_at_ms
                    > (CAST(strftime('%s', 'now') AS INTEGER) * 1000)
                AND stage IN ('created', 'running', 'synthesizing')",
            params![
                delegated_run_id,
                delegated_stage_str(stage),
                artifact_json,
                human_review,
                if resumable { 1 } else { 0 },
                updated_at,
                completed_at,
                host_owner_id,
            ],
        )?;
        if updated == 1 {
            return Ok(());
        }

        match self.get_run(delegated_run_id)? {
            Some(record)
                if matches!(
                    record.stage,
                    DelegatedRunStage::Complete
                        | DelegatedRunStage::Degraded
                        | DelegatedRunStage::Failed
                        | DelegatedRunStage::Cancelled
                ) => Ok(()),
            Some(_) => anyhow::bail!(
                "delegated run '{delegated_run_id}' lost its background host lease before terminal persistence"
            ),
            None => anyhow::bail!("delegated run '{delegated_run_id}' does not exist"),
        }
    }

    /// Publish the conservative terminal record used when process ownership
    /// disappears before the child proves quiescence and persists its own
    /// result. This is shared by in-process guard drop and server-startup
    /// orphan reconciliation so both paths expose identical recovery facts.
    pub fn finalize_caller_aborted_run(
        &self,
        delegated_run_id: &str,
        resumable: bool,
    ) -> Result<()> {
        let artifact = json!({
            "delegated_run_id": delegated_run_id,
            "outcome": "cancelled",
            "outcome_reason": "caller_aborted_before_terminal",
            "quiescent": false,
            "side_effects_may_have_occurred": true,
            "next_action_hint": if resumable {
                "Inspect the workspace and retained evidence before resuming; the caller disappeared before quiescence was proven, so side effects may have occurred."
            } else {
                "Inspect the workspace before starting a replacement; the caller disappeared before quiescence was proven, so side effects may have occurred."
            },
        });
        let updated_at = Utc::now().to_rfc3339();
        let artifact_json = serde_json::to_string(&artifact)?;
        let tx = Transaction::new_unchecked(self.db.conn(), TransactionBehavior::Immediate)?;
        let updated = tx.execute(
            "UPDATE delegated_runs
                SET stage = 'cancelled',
                    artifact_json = ?2,
                    human_review = ?3,
                    resumable = ?4,
                    updated_at = ?5,
                    completed_at = ?5,
                    host_lease_expires_at_ms = NULL
              WHERE delegated_run_id = ?1
                AND stage IN ('created', 'running', 'synthesizing')",
            params![
                delegated_run_id,
                artifact_json,
                "Delegated run cancelled because its caller stopped before terminal persistence.",
                if resumable { 1 } else { 0 },
                updated_at,
            ],
        )?;
        if updated == 1 {
            cancel_foreground_group_on_caller_abort(&tx, delegated_run_id, &updated_at)?;
            tx.commit()?;
            return Ok(());
        }
        tx.commit()?;

        // A terminal writer that won before this guard is authoritative. Never
        // let a late Drop cancel its already-complete canonical group.
        match self.get_run(delegated_run_id)? {
            Some(record)
                if matches!(
                    record.stage,
                    DelegatedRunStage::Complete
                        | DelegatedRunStage::Degraded
                        | DelegatedRunStage::Failed
                        | DelegatedRunStage::Cancelled
                ) =>
            {
                Ok(())
            }
            Some(_) => anyhow::bail!(
                "delegated run '{delegated_run_id}' was not cancelled because its state changed"
            ),
            None => anyhow::bail!("delegated run '{delegated_run_id}' does not exist"),
        }
    }

    fn finalize_owned_background_caller_aborted_run(
        &self,
        delegated_run_id: &str,
        host_owner_id: &str,
        resumable: bool,
    ) -> Result<()> {
        let artifact = json!({
            "delegated_run_id": delegated_run_id,
            "outcome": "cancelled",
            "outcome_reason": "caller_aborted_before_terminal",
            "quiescent": false,
            "side_effects_may_have_occurred": true,
            "next_action_hint": if resumable {
                "Inspect the workspace and retained evidence before resuming; the caller disappeared before quiescence was proven, so side effects may have occurred."
            } else {
                "Inspect the workspace before starting a replacement; the caller disappeared before quiescence was proven, so side effects may have occurred."
            },
        });
        self.finalize_owned_background_run(
            delegated_run_id,
            host_owner_id,
            DelegatedRunStage::Cancelled,
            &artifact,
            Some("Delegated run cancelled because its caller stopped before terminal persistence."),
            resumable,
        )
    }

    pub fn get_run(&self, delegated_run_id: &str) -> Result<Option<DelegatedRunRecord>> {
        let mut stmt = self.db.conn().prepare(
            "SELECT
                delegated_run_id,
                parent_session_id,
                parent_tool_call_id,
                role,
                stage,
                provider,
                model,
                resumable,
                resumed_from_run_id,
                target_scope_key,
                target_scope_json,
                snapshot_json,
                artifact_json,
                human_review,
                created_at,
                updated_at,
                completed_at
                ,child_name
                ,capabilities_json
                ,wake_parent
             FROM delegated_runs
             WHERE delegated_run_id = ?1",
        )?;

        stmt.query_row(params![delegated_run_id], row_to_delegated_run)
            .optional()
            .map_err(Into::into)
    }

    /// Whether this parent has a live child that owns a durable completion
    /// wake. Query lifecycle columns directly so long terminal histories and
    /// large report payloads cannot hide or slow this execution decision.
    pub fn has_active_parent_wake(&self, session_id: &str) -> Result<bool> {
        self.db
            .conn()
            .query_row(
                "SELECT EXISTS (
                    SELECT 1 FROM delegated_runs
                     WHERE parent_session_id = ?1 AND wake_parent = 1
                       AND stage IN ('created', 'running', 'synthesizing')
                 )",
                [session_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn list_runs_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<DelegatedRunRecord>> {
        let limit = limit.max(1) as i64;
        let mut stmt = self.db.conn().prepare(
            "SELECT
                delegated_run_id,
                parent_session_id,
                parent_tool_call_id,
                role,
                stage,
                provider,
                model,
                resumable,
                resumed_from_run_id,
                target_scope_key,
                target_scope_json,
                snapshot_json,
                artifact_json,
                human_review,
                created_at,
                updated_at,
                completed_at
                ,child_name
                ,capabilities_json
                ,wake_parent
             FROM delegated_runs
             WHERE parent_session_id = ?1
             ORDER BY updated_at DESC
             LIMIT ?2",
        )?;

        let rows = stmt.query_map(params![session_id, limit], row_to_delegated_run)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        Ok(records)
    }

    /// List the newest durable run for each parent tool call without loading
    /// snapshots or terminal artifacts. This keeps transcript hydration
    /// bounded by compact rows instead of multiplying the full artifact
    /// window by the session message limit.
    pub fn list_run_summaries_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<DelegatedRunSummary>> {
        let limit = limit.max(1) as i64;
        let mut stmt = self.db.conn().prepare(
            "WITH ranked AS (
                SELECT
                    delegated_run_id,
                    parent_session_id,
                    parent_tool_call_id,
                    role,
                    stage,
                    updated_at,
                    child_name,
                    capabilities_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY parent_tool_call_id
                        ORDER BY updated_at DESC, delegated_run_id DESC
                    ) AS tool_rank
                FROM delegated_runs
                WHERE parent_session_id = ?1
                  AND parent_tool_call_id IS NOT NULL
            )
            SELECT
                delegated_run_id,
                parent_session_id,
                parent_tool_call_id,
                role,
                stage,
                updated_at,
                child_name,
                capabilities_json
            FROM ranked
            WHERE tool_rank = 1
            ORDER BY updated_at DESC, delegated_run_id DESC
            LIMIT ?2",
        )?;

        let rows = stmt.query_map(params![session_id, limit], row_to_delegated_run_summary)?;
        let mut summaries = Vec::new();
        for row in rows {
            summaries.push(row?);
        }
        Ok(summaries)
    }

    pub fn find_related_run(
        &self,
        session_id: &str,
        role: DelegatedRunRole,
        target_scope: &[DelegatedRunScope],
    ) -> Result<Option<DelegatedRunRecord>> {
        let target_scope_key = normalize_scope_key(target_scope);
        let mut stmt = self.db.conn().prepare(
            "SELECT
                delegated_run_id,
                parent_session_id,
                parent_tool_call_id,
                role,
                stage,
                provider,
                model,
                resumable,
                resumed_from_run_id,
                target_scope_key,
                target_scope_json,
                snapshot_json,
                artifact_json,
                human_review,
                created_at,
                updated_at,
                completed_at
                ,child_name
                ,capabilities_json
                ,wake_parent
             FROM delegated_runs
             WHERE parent_session_id = ?1
               AND role = ?2
               AND target_scope_key = ?3
               AND stage NOT IN ('created', 'running')
             ORDER BY updated_at DESC
             LIMIT 1",
        )?;

        stmt.query_row(
            params![session_id, role.as_str(), target_scope_key],
            row_to_delegated_run,
        )
        .optional()
        .map_err(Into::into)
    }

    /// Terminal background rows whose wake was never durably enqueued.
    ///
    /// The idempotency receipt is written atomically with pending steering, so
    /// its absence identifies the crash window between terminal persistence
    /// and enqueue without re-waking already promoted completions.
    pub fn list_unqueued_parent_wakes(&self) -> Result<Vec<DelegatedRunRecord>> {
        let mut stmt = self.db.conn().prepare(
            "SELECT
                delegated_run_id,
                parent_session_id,
                parent_tool_call_id,
                role,
                stage,
                provider,
                model,
                resumable,
                resumed_from_run_id,
                target_scope_key,
                target_scope_json,
                snapshot_json,
                artifact_json,
                human_review,
                created_at,
                updated_at,
                completed_at,
                child_name,
                capabilities_json,
                wake_parent
             FROM delegated_runs AS delegated
             WHERE delegated.wake_parent = 1
               AND delegated.stage IN ('complete', 'degraded', 'failed', 'cancelled')
               AND NOT EXISTS (
                   SELECT 1
                     FROM steering_idempotency AS steering
                    WHERE steering.session_id = delegated.parent_session_id
                      AND steering.pending_id = 'child-wake-' || delegated.delegated_run_id
               )
             ORDER BY delegated.completed_at ASC, delegated.delegated_run_id ASC",
        )?;
        let rows = stmt.query_map([], row_to_delegated_run)?;
        let mut records = Vec::new();
        for row in rows {
            let record = row?;
            if record.should_wake_parent() {
                records.push(record);
            }
        }
        Ok(records)
    }
}
