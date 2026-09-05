mod ipc;
mod notify;
mod outcome;
pub(crate) mod runner;
mod state;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;

use mitsuro_core::agent::LoopInput;
use mitsuro_core::hive::HiveRunStatus;
use mitsuro_core::storage::{
    resolve_worker_conversation_with_conn, Database, HiveRunPriority, HiveRuntimeStateStatus,
    HiveRuntimeStateStore, SessionManager, SessionType,
};
#[cfg(test)]
use mitsuro_core::storage::{WorkerConversationInput, WorkerConversationInputState};
use mitsuro_hive_protocol::{
    ClientError, EventEnvelope, EventSubscription, HiveEvent, WorkerConversationInputDisposition,
    WorkerConversationInputResponse,
};

use self::ipc::{map_daemon_event, DaemonInputAcceptance, HiveDaemonControl, HiveDaemonError};
#[cfg(test)]
use self::notify::hive_notification_title;
use self::runner::run_hive_session;
#[cfg(test)]
use self::state::{
    apply_runtime_event_state, refresh_snapshot_after_run, resolve_persisted_project_dir,
    with_registered_session_input,
};
use self::state::{ensure_runnable_hive_session, parse_wake_at, persist_runtime_state};
use crate::error::AppError;
use crate::types::AgenticEvent;
use crate::AppState;

const HIVE_EVENT_BUFFER: usize = 256;
const HIVE_SUBSCRIPTION_RECONNECT_ATTEMPTS: usize = 5;
#[cfg(not(test))]
const HIVE_SUBSCRIPTION_RECONNECT_BASE_DELAY: Duration = Duration::from_millis(100);
#[cfg(test)]
const HIVE_SUBSCRIPTION_RECONNECT_BASE_DELAY: Duration = Duration::from_millis(5);
#[cfg(not(test))]
const HIVE_SUBSCRIPTION_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(2);
#[cfg(test)]
const HIVE_SUBSCRIPTION_RECONNECT_MAX_DELAY: Duration = Duration::from_millis(25);
#[cfg(not(test))]
const HIVE_SUBSCRIPTION_STABLE_WINDOW: Duration = Duration::from_secs(30);
#[cfg(test)]
const HIVE_SUBSCRIPTION_STABLE_WINDOW: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const HIVE_SUBSCRIPTION_RECONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const HIVE_SUBSCRIPTION_RECONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(250);
const WORKER_STAGED_INPUT_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub struct HiveRuntimeManager {
    daemon: Option<HiveDaemonControl>,
    runtimes: RwLock<HashMap<String, ActiveHiveRuntime>>,
    event_streams: RwLock<HashMap<String, broadcast::Sender<AgenticEvent>>>,
    scheduled_wakes: RwLock<HashMap<String, JoinHandle<()>>>,
    wake_tx: mpsc::UnboundedSender<WakeCommand>,
    subscription_shutdown: broadcast::Sender<()>,
    started_at: Instant,
}

struct ActiveHiveRuntime {
    run_id: String,
    join_handle: JoinHandle<()>,
}

struct WakeCommand {
    state: AppState,
    session_id: String,
    wake_reason: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HiveRuntimeStats {
    /// Canonical independently supervised daemon counts.
    pub active_controller_count: usize,
    pub active_run_count: usize,
    pub queued_run_count: usize,
    pub recovery_required_run_count: usize,
    /// Compatibility projections used by the current diagnostics response.
    pub active_runtime_count: usize,
    pub scheduled_wake_count: usize,
    pub event_stream_count: usize,
    pub uptime_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiveDispatchResult {
    pub session_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiveSteerStatus {
    Accepted,
    Queued,
}

impl HiveSteerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Queued => "queued",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiveSteerResult {
    pub status: HiveSteerStatus,
    pub active_run_id: Option<String>,
    pub staged_input_id: Option<String>,
    pub successor_run_id: Option<String>,
}

impl HiveSteerResult {
    fn plain(status: HiveSteerStatus) -> Self {
        Self {
            status,
            active_run_id: None,
            staged_input_id: None,
            successor_run_id: None,
        }
    }

    fn worker(response: WorkerConversationInputResponse) -> Result<Self> {
        match response.disposition {
            WorkerConversationInputDisposition::Queued => Ok(Self {
                status: HiveSteerStatus::Queued,
                active_run_id: None,
                staged_input_id: None,
                successor_run_id: Some(response.run_id),
            }),
            WorkerConversationInputDisposition::Staged => Ok(Self {
                status: HiveSteerStatus::Queued,
                active_run_id: Some(response.run_id),
                staged_input_id: Some(
                    response
                        .staged_input_id
                        .context("staged Worker steer has no durable input id")?,
                ),
                successor_run_id: None,
            }),
        }
    }
}

#[cfg(test)]
fn validate_staged_worker_input_successor(
    input: WorkerConversationInput,
    worker_id: &str,
    owner_user_id: Option<&str>,
    session_id: &str,
    active_run_id: &str,
    staged_input_id: &str,
) -> Result<Option<String>> {
    anyhow::ensure!(
        input.worker_id == worker_id
            && input.owner_user_id.as_deref() == owner_user_id
            && input.session_id == session_id
            && input.accepted_while_run_id == active_run_id
            && input.id == staged_input_id,
        "staged Worker input durable binding changed"
    );
    match input.state {
        WorkerConversationInputState::Staged => {
            anyhow::ensure!(
                input.canonical_message_id.is_none()
                    && input.assigned_run_id.is_none()
                    && input.materialized_at.is_none(),
                "staged Worker input carries premature successor projections"
            );
            Ok(None)
        }
        WorkerConversationInputState::Materialized => {
            let successor_run_id = input
                .assigned_run_id
                .filter(|run_id| !run_id.trim().is_empty())
                .context("materialized Worker input has no assigned successor run")?;
            anyhow::ensure!(
                input
                    .canonical_message_id
                    .is_some_and(|message_id| message_id > 0)
                    && input.materialized_at.is_some(),
                "materialized Worker input has incomplete canonical projections"
            );
            Ok(Some(successor_run_id))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerRunFollowTarget {
    run_id: String,
    earliest_event_sequence: Option<i64>,
    status: HiveRunStatus,
    attempt_count: u32,
    started_at: Option<String>,
}

impl WorkerRunFollowTarget {
    fn can_wait_on_live_cursor_without_history(&self) -> bool {
        self.status == HiveRunStatus::Queued && self.attempt_count == 0 && self.started_at.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StagedWorkerFollowProjection {
    Waiting,
    Materialized(WorkerRunFollowTarget),
}

fn validate_exact_owned_worker_dm(
    tx: &Transaction<'_>,
    worker_id: &str,
    owner_user_id: Option<&str>,
    session_id: &str,
) -> Result<()> {
    let binding = resolve_worker_conversation_with_conn(tx, session_id)?
        .context("Worker input has no durable conversation binding")?;
    anyhow::ensure!(
        binding.group_id.is_none()
            && binding.worker.id == worker_id
            && binding.worker.user_id.as_deref() == owner_user_id
            && binding.worker.dm_session_id.as_deref() == Some(session_id),
        "Worker input durable conversation binding changed"
    );
    Ok(())
}

fn validate_worker_staging_predecessor(
    tx: &Transaction<'_>,
    worker_id: &str,
    owner_user_id: Option<&str>,
    session_id: &str,
    run_id: &str,
) -> Result<()> {
    let valid: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM hive_runs run
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN sessions session ON session.id = run.session_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE run.id = ?1
               AND run.worker_id = ?2
               AND run.session_id = ?3
               AND run.kind IN ('worker_conversation', 'worker_introduction_review')
               AND run.group_id IS NULL
               AND run.group_turn_id IS NULL
               AND worker.user_id IS ?4
               AND worker.dm_session_id = session.id
               AND session.user_id IS worker.user_id
               AND session.session_type = 'hive'
               AND controller.worker_id = worker.id
               AND controller.session_id = session.id
               AND controller.user_id IS worker.user_id
         )",
        params![run_id, worker_id, session_id, owner_user_id],
        |row| row.get(0),
    )?;
    anyhow::ensure!(valid, "staged Worker input predecessor binding changed");
    Ok(())
}

fn exact_worker_run_follow_target(
    tx: &Transaction<'_>,
    worker_id: &str,
    owner_user_id: Option<&str>,
    session_id: &str,
    run_id: &str,
    canonical_message_id: Option<i64>,
) -> Result<WorkerRunFollowTarget> {
    let row = tx
        .query_row(
            "SELECT run.status, run.attempt_count, run.started_at,
                    MIN(event.sequence)
             FROM hive_runs run
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN sessions session ON session.id = run.session_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             JOIN messages objective ON objective.id = run.objective_message_id
             LEFT JOIN hive_controller_events event
               ON event.controller_id = controller.id AND event.run_id = run.id
             WHERE run.id = ?1
               AND run.kind = 'worker_conversation'
               AND run.worker_id = ?2
               AND run.session_id = ?3
               AND run.group_id IS NULL
               AND run.group_turn_id IS NULL
               AND (?4 IS NULL OR run.objective_message_id = ?4)
               AND run.conversation_through_message_id IS run.objective_message_id
               AND worker.user_id IS ?5
               AND worker.dm_session_id = session.id
               AND session.user_id IS worker.user_id
               AND session.session_type = 'hive'
               AND controller.worker_id = worker.id
               AND controller.session_id = session.id
               AND controller.user_id IS worker.user_id
               AND objective.session_id = session.id
               AND objective.role = 'user'
             GROUP BY run.id, run.status, run.attempt_count, run.started_at",
            params![
                run_id,
                worker_id,
                session_id,
                canonical_message_id,
                owner_user_id,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            },
        )
        .optional()?
        .context("Worker response run has no exact durable DM binding")?;
    let status = HiveRunStatus::parse(&row.0).context("Worker response run has invalid status")?;
    let attempt_count = u32::try_from(row.1).context("Worker response attempt count is invalid")?;
    anyhow::ensure!(
        row.3.is_none_or(|sequence| sequence > 0),
        "Worker response run has an invalid event sequence"
    );
    Ok(WorkerRunFollowTarget {
        run_id: run_id.to_string(),
        earliest_event_sequence: row.3,
        status,
        attempt_count,
        started_at: row.2,
    })
}

fn queued_worker_follow_target(
    database_path: &Path,
    owner_user_id: Option<&str>,
    response: &WorkerConversationInputResponse,
) -> Result<WorkerRunFollowTarget> {
    anyhow::ensure!(
        response.disposition == WorkerConversationInputDisposition::Queued,
        "queued Worker follower received a staged response"
    );
    let canonical_message_id = response
        .canonical_message_id
        .filter(|message_id| *message_id > 0)
        .context("queued Worker response has no canonical message")?;
    let database = Database::new(database_path)?;
    let tx = Transaction::new_unchecked(database.conn(), TransactionBehavior::Deferred)?;
    validate_exact_owned_worker_dm(
        &tx,
        &response.worker_id,
        owner_user_id,
        &response.session_id,
    )?;
    let target = exact_worker_run_follow_target(
        &tx,
        &response.worker_id,
        owner_user_id,
        &response.session_id,
        &response.run_id,
        Some(canonical_message_id),
    )?;
    tx.commit()?;
    Ok(target)
}

fn staged_worker_follow_projection(
    database_path: &Path,
    worker_id: &str,
    owner_user_id: Option<&str>,
    session_id: &str,
    active_run_id: &str,
    staged_input_id: &str,
) -> Result<StagedWorkerFollowProjection> {
    let database = Database::new(database_path)?;
    let tx = Transaction::new_unchecked(database.conn(), TransactionBehavior::Deferred)?;
    validate_exact_owned_worker_dm(&tx, worker_id, owner_user_id, session_id)?;
    validate_worker_staging_predecessor(&tx, worker_id, owner_user_id, session_id, active_run_id)?;
    let input = tx
        .query_row(
            "SELECT worker_id, owner_user_id, session_id, accepted_while_run_id,
                    state, canonical_message_id, assigned_run_id, materialized_at
             FROM hive_worker_conversation_inputs WHERE id = ?1",
            [staged_input_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                ))
            },
        )
        .optional()?
        .context("durably accepted Worker input disappeared")?;
    anyhow::ensure!(
        input.0 == worker_id
            && input.1.as_deref() == owner_user_id
            && input.2 == session_id
            && input.3 == active_run_id,
        "staged Worker input durable binding changed"
    );
    let projection = match input.4.as_str() {
        "staged" => {
            anyhow::ensure!(
                input.5.is_none() && input.6.is_none() && input.7.is_none(),
                "staged Worker input carries premature successor projections"
            );
            StagedWorkerFollowProjection::Waiting
        }
        "materialized" => {
            let canonical_message_id = input
                .5
                .filter(|message_id| *message_id > 0)
                .context("materialized Worker input has no canonical message")?;
            let successor_run_id = input
                .6
                .filter(|run_id| !run_id.trim().is_empty())
                .context("materialized Worker input has no assigned successor run")?;
            anyhow::ensure!(
                input.7.is_some(),
                "materialized Worker input has no materialization time"
            );
            StagedWorkerFollowProjection::Materialized(exact_worker_run_follow_target(
                &tx,
                worker_id,
                owner_user_id,
                session_id,
                &successor_run_id,
                Some(canonical_message_id),
            )?)
        }
        _ => anyhow::bail!("Worker input has invalid durable state"),
    };
    tx.commit()?;
    Ok(projection)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum WorkerSuccessBoundary {
    #[default]
    None,
    Pending,
    Committed,
    TurnComplete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExactWorkerEventDisposition {
    Continue,
    Finish,
    Error,
    Invalid(String),
}

fn validate_worker_response_boundary(
    payload: &serde_json::Value,
    worker_id: &str,
    session_id: &str,
    run_id: &str,
) -> Result<()> {
    anyhow::ensure!(
        payload.get("worker_id").and_then(serde_json::Value::as_str) == Some(worker_id)
            && payload
                .get("session_id")
                .and_then(serde_json::Value::as_str)
                == Some(session_id)
            && payload.get("run_id").and_then(serde_json::Value::as_str) == Some(run_id),
        "Worker response boundary has a mismatched durable identity"
    );
    Ok(())
}

fn exact_worker_event_disposition(
    envelope: &EventEnvelope,
    worker_id: &str,
    session_id: &str,
    run_id: &str,
    success_boundary: &mut WorkerSuccessBoundary,
) -> Result<ExactWorkerEventDisposition> {
    match &envelope.event {
        HiveEvent::Extension(extension) if extension.name == "agentic_event" => {
            let payload = &extension.payload;
            match payload.get("type").and_then(serde_json::Value::as_str) {
                Some("worker_response_pending") => {
                    validate_worker_response_boundary(payload, worker_id, session_id, run_id)?;
                    if *success_boundary == WorkerSuccessBoundary::None {
                        *success_boundary = WorkerSuccessBoundary::Pending;
                    }
                    Ok(ExactWorkerEventDisposition::Continue)
                }
                Some("worker_response_committed") => {
                    validate_worker_response_boundary(payload, worker_id, session_id, run_id)?;
                    if *success_boundary == WorkerSuccessBoundary::Pending {
                        *success_boundary = WorkerSuccessBoundary::Committed;
                    }
                    Ok(ExactWorkerEventDisposition::Continue)
                }
                Some("turn_complete") => {
                    if payload
                        .get("has_more")
                        .and_then(serde_json::Value::as_bool)
                        == Some(false)
                        && *success_boundary == WorkerSuccessBoundary::Committed
                    {
                        *success_boundary = WorkerSuccessBoundary::TurnComplete;
                    }
                    Ok(ExactWorkerEventDisposition::Continue)
                }
                Some("finish") => {
                    anyhow::ensure!(
                        payload
                            .get("session_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(session_id),
                        "Worker finish boundary has a mismatched session"
                    );
                    let stop_reason = payload
                        .get("stop_reason")
                        .and_then(serde_json::Value::as_str)
                        .context("Worker finish boundary has no stop reason")?;
                    if stop_reason == "completed"
                        && *success_boundary != WorkerSuccessBoundary::TurnComplete
                    {
                        return Ok(ExactWorkerEventDisposition::Invalid(
                            "Exact Worker response replay is missing its pending, committed, or terminal turn boundary"
                                .to_string(),
                        ));
                    }
                    Ok(ExactWorkerEventDisposition::Finish)
                }
                Some("error") => Ok(ExactWorkerEventDisposition::Error),
                _ => Ok(ExactWorkerEventDisposition::Continue),
            }
        }
        HiveEvent::Runtime(runtime) => match runtime.event_type.as_str() {
            "run_completed" => Ok(ExactWorkerEventDisposition::Invalid(
                "Exact Worker response replay reached durable success without its completed stream boundary"
                    .to_string(),
            )),
            "run_failed" | "run_cancelled" | "run_dead_lettered" | "recovery_required" => {
                Ok(ExactWorkerEventDisposition::Error)
            }
            _ => Ok(ExactWorkerEventDisposition::Continue),
        },
        _ => Ok(ExactWorkerEventDisposition::Continue),
    }
}

fn send_exact_worker_error(sender: &broadcast::Sender<AgenticEvent>, message: impl Into<String>) {
    let _ = sender.send(AgenticEvent::Error {
        error: message.into(),
    });
}

#[allow(clippy::too_many_arguments)]
async fn relay_exact_worker_run(
    daemon: HiveDaemonControl,
    mut subscription: EventSubscription,
    owner_user_id: Option<String>,
    worker_id: String,
    session_id: String,
    target: WorkerRunFollowTarget,
    event_sender: broadcast::Sender<AgenticEvent>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let pre_send_high_water = subscription.accepted.high_water_sequence.unwrap_or(0);
    let replay_after = match target.earliest_event_sequence {
        Some(earliest) if earliest <= pre_send_high_water => Some(earliest.saturating_sub(1)),
        Some(_) => None,
        None if target.can_wait_on_live_cursor_without_history() => None,
        None => {
            send_exact_worker_error(
                &event_sender,
                "Exact Worker response history is no longer available for this terminal or already-started run",
            );
            return;
        }
    };
    let terminal_at_snapshot =
        target.status.is_terminal() || target.status == HiveRunStatus::RecoveryRequired;
    let mut replay_high_water = None;
    let mut last_sequence = subscription.accepted.high_water_sequence;
    if let Some(after_sequence) = replay_after {
        if event_sender
            .send(AgenticEvent::Lagged { skipped: 1 })
            .is_err()
        {
            return;
        }
        subscription = match tokio::select! {
            _ = event_sender.closed() => return,
            _ = shutdown.recv() => return,
            subscription = daemon.subscribe(
                owner_user_id.as_deref(),
                &session_id,
                Some(after_sequence),
                Some(HIVE_EVENT_BUFFER),
            ) => subscription,
        } {
            Ok(subscription) => subscription,
            Err(error) => {
                send_exact_worker_error(
                    &event_sender,
                    format!("Exact Worker response replay could not be opened: {error:#}"),
                );
                return;
            }
        };
        replay_high_water = subscription.accepted.high_water_sequence;
        last_sequence = Some(after_sequence);
    }

    let mut success_boundary = WorkerSuccessBoundary::None;
    let mut reconnect_attempts = 0usize;
    loop {
        let received = tokio::select! {
            _ = event_sender.closed() => return,
            _ = shutdown.recv() => return,
            received = subscription.next_event() => received,
        };
        match received {
            Ok(Some(envelope)) => {
                let session_matches = envelope.session_id.as_deref() == Some(session_id.as_str());
                let session_stream_control = session_matches
                    && envelope.run_id.is_none()
                    && matches!(
                        &envelope.event,
                        HiveEvent::ReplayGap(_) | HiveEvent::Lagged(_)
                    );
                let global_shutdown = envelope.session_id.is_none()
                    && matches!(&envelope.event, HiveEvent::DaemonShuttingDown { .. });
                if !session_matches && !global_shutdown {
                    send_exact_worker_error(
                        &event_sender,
                        "Exact Worker response subscription returned another session",
                    );
                    return;
                }
                if envelope
                    .sequence
                    .zip(last_sequence)
                    .is_some_and(|(sequence, cursor)| sequence <= cursor)
                {
                    continue;
                }
                last_sequence = max_sequence(last_sequence, envelope.sequence);

                if session_stream_control || global_shutdown {
                    let terminal = matches!(
                        &envelope.event,
                        HiveEvent::ReplayGap(_) | HiveEvent::DaemonShuttingDown { .. }
                    );
                    let mapped = map_daemon_event(envelope);
                    if event_sender.send(mapped).is_err() || terminal {
                        return;
                    }
                    continue;
                }

                if envelope.run_id.as_deref() == Some(target.run_id.as_str()) {
                    let disposition = match exact_worker_event_disposition(
                        &envelope,
                        &worker_id,
                        &session_id,
                        &target.run_id,
                        &mut success_boundary,
                    ) {
                        Ok(disposition) => disposition,
                        Err(error) => {
                            send_exact_worker_error(
                                &event_sender,
                                format!("Exact Worker response binding failed: {error:#}"),
                            );
                            return;
                        }
                    };
                    let mapped = map_daemon_event(envelope);
                    match disposition {
                        ExactWorkerEventDisposition::Continue => {
                            if event_sender.send(mapped).is_err() {
                                return;
                            }
                        }
                        ExactWorkerEventDisposition::Finish => {
                            let _ = event_sender.send(mapped);
                            return;
                        }
                        ExactWorkerEventDisposition::Error => {
                            let mapped_is_error = matches!(&mapped, AgenticEvent::Error { .. });
                            let _ = event_sender.send(mapped);
                            if !mapped_is_error {
                                send_exact_worker_error(
                                    &event_sender,
                                    "The exact Worker run ended without a replayable response; reload to reconcile it",
                                );
                            }
                            return;
                        }
                        ExactWorkerEventDisposition::Invalid(message) => {
                            send_exact_worker_error(&event_sender, message);
                            return;
                        }
                    }
                }

                if terminal_at_snapshot
                    && replay_high_water
                        .zip(last_sequence)
                        .is_some_and(|(high_water, cursor)| cursor >= high_water)
                {
                    send_exact_worker_error(
                        &event_sender,
                        "Bounded Worker response replay did not contain the exact terminal stream",
                    );
                    return;
                }
                reconnect_attempts = 0;
            }
            Ok(None) | Err(_) => {
                reconnect_attempts = reconnect_attempts.saturating_add(1);
                if reconnect_attempts >= HIVE_SUBSCRIPTION_RECONNECT_ATTEMPTS {
                    send_exact_worker_error(
                        &event_sender,
                        "Exact Worker response stream closed before its terminal event",
                    );
                    return;
                }
                let delay = daemon_subscription_reconnect_delay(&session_id, reconnect_attempts);
                tokio::select! {
                    _ = event_sender.closed() => return,
                    _ = shutdown.recv() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
                let after_sequence = last_sequence;
                let reconnected = tokio::select! {
                    _ = event_sender.closed() => return,
                    _ = shutdown.recv() => return,
                    subscription = daemon.subscribe(
                        owner_user_id.as_deref(),
                        &session_id,
                        after_sequence,
                        Some(HIVE_EVENT_BUFFER),
                    ) => subscription,
                };
                match reconnected {
                    Ok(next) => {
                        subscription = next;
                        replay_high_water = subscription.accepted.high_water_sequence;
                        let _ = event_sender.send(AgenticEvent::Lagged { skipped: 1 });
                    }
                    Err(error) => tracing::warn!(
                        session_id,
                        run_id = %target.run_id,
                        attempt = reconnect_attempts,
                        error = %error,
                        "Exact Worker response stream reconnect failed"
                    ),
                }
            }
        }
    }
}

pub fn control_plane_app_error(error: anyhow::Error) -> AppError {
    let daemon_error = error
        .chain()
        .find_map(|source| source.downcast_ref::<HiveDaemonError>());
    match daemon_error {
        Some(HiveDaemonError::Remote { code, message }) => match code.as_str() {
            "not_found" | "session_not_found" | "ownership_denied" | "ownership_mismatch"
            | "forbidden" => AppError::NotFound(message.clone()),
            "conflict"
            | "inactive"
            | "no_active_session"
            | "no_pending_interaction"
            | "already_running"
            | "idempotency_conflict"
            | "revision_conflict"
            | "state_conflict" => AppError::Conflict(message.clone()),
            "request_in_progress" => AppError::ServiceUnavailable(message.clone()),
            "invalid_request" | "invalid_command" | "bad_request" => {
                AppError::BadRequest(message.clone())
            }
            _ => AppError::BadGateway(format!("Hive service rejected the request: {message}")),
        },
        Some(HiveDaemonError::Unavailable(message)) => {
            AppError::BadGateway(format!("Hive service unavailable: {message}"))
        }
        None => AppError::BadGateway(format!("Hive service request failed: {error}")),
    }
}

impl HiveRuntimeManager {
    pub fn new() -> Arc<Self> {
        Self::build(None, true)
    }

    /// Construct the compatibility manager needed by the standalone
    /// execution host without spawning the legacy in-process wake consumer.
    /// Durable sleeps and retries are exclusively owned by `mitsuro-hive`.
    pub(crate) fn execution_host() -> Arc<Self> {
        Self::build(None, false)
    }

    pub async fn daemon_from_discovered() -> Result<Arc<Self>> {
        let daemon = HiveDaemonControl::connect_discovered().await?;
        Ok(Self::build(Some(daemon), false))
    }

    /// Connect to one explicitly selected Hive daemon without consulting
    /// process environment, runtime-directory discovery, or the production
    /// control key. The key is load-only; an acceptance client may never
    /// bootstrap new daemon authority.
    pub(crate) async fn daemon_from_explicit(
        socket_path: PathBuf,
        key_path: PathBuf,
    ) -> Result<Arc<Self>> {
        let daemon = HiveDaemonControl::connect_explicit(socket_path, key_path).await?;
        Ok(Self::build(Some(daemon), false))
    }

    fn build(daemon: Option<HiveDaemonControl>, enable_embedded_wakes: bool) -> Arc<Self> {
        let (wake_tx, mut wake_rx) = mpsc::unbounded_channel();
        let (subscription_shutdown, _subscription_shutdown_rx) = broadcast::channel(1);
        let manager = Arc::new(Self {
            daemon,
            runtimes: RwLock::new(HashMap::new()),
            event_streams: RwLock::new(HashMap::new()),
            scheduled_wakes: RwLock::new(HashMap::new()),
            wake_tx,
            subscription_shutdown,
            started_at: Instant::now(),
        });

        if enable_embedded_wakes {
            let weak_manager = Arc::downgrade(&manager);
            tokio::spawn(async move {
                while let Some(command) = wake_rx.recv().await {
                    let Some(manager) = weak_manager.upgrade() else {
                        break;
                    };

                    let WakeCommand {
                        state,
                        session_id,
                        wake_reason,
                    } = command;

                    if let Err(err) = manager
                        .start_or_restart_session(state, session_id.clone(), &wake_reason)
                        .await
                    {
                        tracing::error!(
                            session_id = %session_id,
                            error = %err,
                            "Failed to resume sleeping Hive session"
                        );
                    }
                }
            });
        } else {
            drop(wake_rx);
        }

        manager
    }

    pub fn is_daemon_backed(&self) -> bool {
        self.daemon.is_some()
    }

    async fn event_sender(&self, session_id: &str) -> broadcast::Sender<AgenticEvent> {
        if let Some(sender) = self.event_streams.read().await.get(session_id).cloned() {
            return sender;
        }

        let mut streams = self.event_streams.write().await;
        streams
            .entry(session_id.to_string())
            .or_insert_with(|| {
                let (event_tx, _event_rx) = broadcast::channel(HIVE_EVENT_BUFFER);
                event_tx
            })
            .clone()
    }

    pub async fn forget_session(&self, session_id: &str) {
        self.cancel_scheduled_wake(session_id).await;
        self.event_streams.write().await.remove(session_id);
    }

    pub async fn restore_persisted_sessions(&self, state: AppState) -> Result<()> {
        if let Some(daemon) = &self.daemon {
            daemon.recover(None, None, None).await?;
            return Ok(());
        }

        let runtime_store = HiveRuntimeStateStore::new(Database::new(&state.db_path)?);
        let session_manager = SessionManager::new(Database::new(&state.db_path)?);

        for runtime_state in runtime_store.list_recoverable_states()? {
            let Some(session) = session_manager.get_session(&runtime_state.session_id)? else {
                continue;
            };
            if session.session_type != SessionType::Hive {
                continue;
            }

            self.recover_persisted_state(state.clone(), &runtime_state, "startup_recover")
                .await?;
        }

        Ok(())
    }

    pub async fn recover_persisted_state_for_user(
        &self,
        state: AppState,
        runtime_state: &mitsuro_core::storage::HiveRuntimeState,
        wake_reason: &'static str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        if let Some(daemon) = &self.daemon {
            daemon
                .recover(user_id, Some(&runtime_state.session_id), idempotency_key)
                .await?;
            return Ok(());
        }
        self.recover_persisted_state(state, runtime_state, wake_reason)
            .await
    }

    pub async fn recover_all_for_user(
        &self,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<usize> {
        self.daemon
            .as_ref()
            .context("Hive recovery requires the daemon control plane")?
            .recover(user_id, None, idempotency_key)
            .await
    }

    pub async fn recover_persisted_state(
        &self,
        state: AppState,
        runtime_state: &mitsuro_core::storage::HiveRuntimeState,
        wake_reason: &'static str,
    ) -> Result<()> {
        match runtime_state.status {
            HiveRuntimeStateStatus::Running => {
                tracing::info!(
                    session_id = %runtime_state.session_id,
                    "Recovering persisted running Hive session"
                );
                self.start_or_restart_session(state, runtime_state.session_id.clone(), wake_reason)
                    .await
            }
            HiveRuntimeStateStatus::Sleeping => {
                let wake_at = runtime_state
                    .next_wake_at
                    .as_deref()
                    .and_then(parse_wake_at);
                match wake_at {
                    Some(wake_at) if wake_at > chrono::Utc::now() => {
                        tracing::info!(
                            session_id = %runtime_state.session_id,
                            wake_at = %wake_at,
                            "Scheduling persisted sleeping Hive session"
                        );
                        self.stop_active_run(&state, &runtime_state.session_id)
                            .await;
                        ensure_runnable_hive_session(&state.db_path, &runtime_state.session_id)?;
                        persist_runtime_state(
                            &state.db_path,
                            &runtime_state.session_id,
                            HiveRuntimeStateStatus::Sleeping,
                            Some(&wake_at.to_rfc3339()),
                            runtime_state.sleep_reason.as_deref(),
                            None,
                            None,
                            Some(wake_reason),
                        )?;
                        self.schedule_wake_at(
                            state,
                            runtime_state.session_id.clone(),
                            wake_at,
                            wake_reason,
                        )
                        .await;
                        Ok(())
                    }
                    _ => {
                        tracing::info!(
                            session_id = %runtime_state.session_id,
                            "Resuming persisted sleeping Hive session immediately"
                        );
                        self.start_or_restart_session(
                            state,
                            runtime_state.session_id.clone(),
                            wake_reason,
                        )
                        .await
                    }
                }
            }
            _ => Ok(()),
        }
    }

    pub async fn stats_for_sessions(&self, session_ids: &[String]) -> HiveRuntimeStats {
        let session_ids = session_ids
            .iter()
            .map(String::as_str)
            .collect::<std::collections::HashSet<_>>();
        let active_runtime_count = self
            .runtimes
            .read()
            .await
            .keys()
            .filter(|session_id| session_ids.contains(session_id.as_str()))
            .count();
        let scheduled_wake_count = self
            .scheduled_wakes
            .read()
            .await
            .keys()
            .filter(|session_id| session_ids.contains(session_id.as_str()))
            .count();
        let event_stream_count = self
            .event_streams
            .read()
            .await
            .keys()
            .filter(|session_id| session_ids.contains(session_id.as_str()))
            .count();

        HiveRuntimeStats {
            active_controller_count: active_runtime_count.saturating_add(scheduled_wake_count),
            active_run_count: active_runtime_count,
            queued_run_count: scheduled_wake_count,
            recovery_required_run_count: 0,
            active_runtime_count,
            scheduled_wake_count,
            event_stream_count,
            uptime_secs: self.started_at.elapsed().as_secs(),
        }
    }

    pub async fn stats_for_sessions_for_user(
        &self,
        session_ids: &[String],
        user_id: Option<&str>,
    ) -> Result<HiveRuntimeStats> {
        if let Some(daemon) = &self.daemon {
            return daemon.stats(user_id).await;
        }
        Ok(self.stats_for_sessions(session_ids).await)
    }

    pub async fn start_or_restart_session_for_user(
        &self,
        state: AppState,
        session_id: String,
        wake_reason: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        if let Some(daemon) = &self.daemon {
            return daemon
                .start(user_id, &session_id, wake_reason, idempotency_key)
                .await;
        }
        self.start_or_restart_session(state, session_id, wake_reason)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch_for_user(
        &self,
        user_id: Option<&str>,
        task: &str,
        working_dir: &str,
        project_dir: Option<&str>,
        model: Option<&str>,
        model_key: Option<&mitsuro_hive_protocol::ModelKey>,
        model_catalog_revision: Option<&str>,
        start_at: Option<chrono::DateTime<chrono::Utc>>,
        priority: HiveRunPriority,
        crew_slug: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<HiveDispatchResult> {
        let daemon = self
            .daemon
            .as_ref()
            .context("Hive dispatch requires the daemon control plane")?;
        let (session_id, status) = daemon
            .dispatch(
                user_id,
                task,
                working_dir,
                project_dir,
                model,
                model_key,
                model_catalog_revision,
                start_at.map(|value| value.timestamp_millis()),
                Some(priority.as_str()),
                crew_slug,
                idempotency_key,
            )
            .await?;
        Ok(HiveDispatchResult { session_id, status })
    }

    /// Create-and-meet is deliberately daemon-only: Worker identity, DM
    /// binding, controller state, and the first run must share one durable
    /// idempotency receipt and one SQLite transaction.
    pub async fn create_worker_introduction_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::CreateWorkerIntroductionCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerIntroductionResponse> {
        self.daemon
            .as_ref()
            .context("Hive Worker Introduction requires the daemon control plane")?
            .create_worker_introduction(user_id, command, idempotency_key)
            .await
    }

    pub async fn retry_worker_introduction_for_user(
        &self,
        user_id: Option<&str>,
        worker_id: &str,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerIntroductionActionResponse> {
        self.daemon
            .as_ref()
            .context("Hive Worker Introduction retry requires the daemon control plane")?
            .retry_worker_introduction(user_id, worker_id, idempotency_key)
            .await
    }

    pub async fn skip_worker_introduction_for_user(
        &self,
        user_id: Option<&str>,
        worker_id: &str,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerIntroductionActionResponse> {
        self.daemon
            .as_ref()
            .context("Hive Worker Introduction skip requires the daemon control plane")?
            .skip_worker_introduction(user_id, worker_id, idempotency_key)
            .await
    }

    pub async fn confirm_worker_introduction_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::ConfirmWorkerIntroductionCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerIntroductionActionResponse> {
        self.daemon
            .as_ref()
            .context("Hive Worker Introduction confirmation requires the daemon control plane")?
            .confirm_worker_introduction(user_id, command, idempotency_key)
            .await
    }

    pub async fn return_worker_introduction_to_context_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::ReturnWorkerIntroductionToContextCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerIntroductionActionResponse> {
        self.daemon
            .as_ref()
            .context("Hive Worker Introduction return requires the daemon control plane")?
            .return_worker_introduction_to_context(user_id, command, idempotency_key)
            .await
    }

    pub async fn update_worker_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::UpdateWorkerCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerMutationResponse> {
        self.daemon
            .as_ref()
            .context("Hive Worker updates require the daemon control plane")?
            .update_worker(user_id, command, idempotency_key)
            .await
    }

    pub async fn set_worker_status_for_user(
        &self,
        user_id: Option<&str>,
        worker_id: &str,
        expected_revision: u64,
        status: mitsuro_hive_protocol::WorkerTargetStatus,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerMutationResponse> {
        self.daemon
            .as_ref()
            .context("Hive Worker lifecycle changes require the daemon control plane")?
            .set_worker_status(
                user_id,
                worker_id,
                expected_revision,
                status,
                idempotency_key,
            )
            .await
    }

    pub async fn grant_worker_governor_recovery_for_user(
        &self,
        user_id: Option<&str>,
        worker_id: &str,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerGovernorRecoveryResponse> {
        self.daemon
            .as_ref()
            .context("Worker governor recovery requires the daemon control plane")?
            .grant_worker_governor_recovery(user_id, worker_id, idempotency_key)
            .await
    }

    pub async fn activate_or_resume_worker_workflow_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::ActivateOrResumeWorkerWorkflowCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerWorkflowResponse> {
        self.daemon
            .as_ref()
            .context("Worker Workflow activation requires the daemon control plane")?
            .activate_or_resume_worker_workflow(user_id, command, idempotency_key)
            .await
    }

    pub async fn pause_worker_workflow_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::WorkerWorkflowLifecycleCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerWorkflowResponse> {
        self.daemon
            .as_ref()
            .context("Worker Workflow pause requires the daemon control plane")?
            .pause_worker_workflow(user_id, command, idempotency_key)
            .await
    }

    pub async fn cancel_worker_workflow_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::WorkerWorkflowLifecycleCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerWorkflowResponse> {
        self.daemon
            .as_ref()
            .context("Worker Workflow cancellation requires the daemon control plane")?
            .cancel_worker_workflow(user_id, command, idempotency_key)
            .await
    }

    pub async fn resolve_worker_goal_acceptance_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::ResolveWorkerGoalAcceptanceCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerGoalAcceptanceResponse> {
        self.daemon
            .as_ref()
            .context("Worker Goal acceptance requires the daemon control plane")?
            .resolve_worker_goal_acceptance(user_id, command, idempotency_key)
            .await
    }

    pub async fn set_worker_workspace_for_user(
        &self,
        user_id: Option<&str>,
        command: mitsuro_hive_protocol::SetWorkerWorkspaceCommand,
        idempotency_key: &str,
    ) -> Result<mitsuro_hive_protocol::WorkerWorkspaceResponse> {
        self.daemon
            .as_ref()
            .context("Worker workspace changes require the daemon control plane")?
            .set_worker_workspace(user_id, command, idempotency_key)
            .await
    }

    pub async fn create_schedule_for_user(
        &self,
        user_id: Option<&str>,
        session_id: &str,
        definition: mitsuro_hive_protocol::ScheduleDefinition,
        idempotency_key: Option<&str>,
    ) -> Result<mitsuro_hive_protocol::ScheduleResponse> {
        self.daemon
            .as_ref()
            .context("Hive schedule mutations require the daemon control plane")?
            .create_schedule(user_id, session_id, definition, idempotency_key)
            .await
    }

    pub async fn replace_schedule_for_user(
        &self,
        user_id: Option<&str>,
        session_id: &str,
        schedule_id: &str,
        expected_revision: u64,
        definition: mitsuro_hive_protocol::ScheduleDefinition,
        idempotency_key: Option<&str>,
    ) -> Result<mitsuro_hive_protocol::ScheduleResponse> {
        self.daemon
            .as_ref()
            .context("Hive schedule mutations require the daemon control plane")?
            .replace_schedule(
                user_id,
                session_id,
                schedule_id,
                expected_revision,
                definition,
                idempotency_key,
            )
            .await
    }

    pub async fn set_schedule_status_for_user(
        &self,
        user_id: Option<&str>,
        session_id: &str,
        schedule_id: &str,
        expected_revision: u64,
        status: &str,
        idempotency_key: Option<&str>,
    ) -> Result<mitsuro_hive_protocol::ScheduleResponse> {
        self.daemon
            .as_ref()
            .context("Hive schedule mutations require the daemon control plane")?
            .set_schedule_status(
                user_id,
                session_id,
                schedule_id,
                expected_revision,
                status,
                idempotency_key,
            )
            .await
    }

    pub async fn start_or_restart_session(
        &self,
        state: AppState,
        session_id: String,
        wake_reason: &str,
    ) -> Result<()> {
        self.stop_active_run(&state, &session_id).await;
        ensure_runnable_hive_session(&state.db_path, &session_id)?;
        let run_id = Uuid::new_v4().to_string();
        persist_runtime_state(
            &state.db_path,
            &session_id,
            HiveRuntimeStateStatus::Running,
            None,
            None,
            None,
            Some(run_id.as_str()),
            Some(wake_reason),
        )?;

        let event_tx = self.event_sender(&session_id).await;
        let state_clone = state.clone();
        let session_id_clone = session_id.clone();
        let run_id_clone = run_id.clone();
        let event_tx_clone = event_tx.clone();
        let wake_reason_owned = wake_reason.to_string();
        let manager = state.hive_runtime.clone();
        let join_handle = tokio::spawn(async move {
            run_hive_session(
                state_clone,
                session_id_clone,
                run_id_clone,
                wake_reason_owned,
                event_tx_clone,
                manager,
            )
            .await;
        });

        self.runtimes.write().await.insert(
            session_id,
            ActiveHiveRuntime {
                run_id,
                join_handle,
            },
        );
        Ok(())
    }

    pub async fn subscribe(&self, session_id: &str) -> broadcast::Receiver<AgenticEvent> {
        self.event_sender(session_id).await.subscribe()
    }

    pub async fn subscribe_for_user(
        &self,
        session_id: &str,
        user_id: Option<&str>,
    ) -> Result<broadcast::Receiver<AgenticEvent>> {
        self.subscribe_for_user_from(session_id, user_id, None, Some(0))
            .await
    }

    pub async fn subscribe_for_user_from(
        &self,
        session_id: &str,
        user_id: Option<&str>,
        after_sequence: Option<i64>,
        replay_limit: Option<usize>,
    ) -> Result<broadcast::Receiver<AgenticEvent>> {
        let Some(daemon) = &self.daemon else {
            return Ok(self.subscribe(session_id).await);
        };

        // Every request opens its own authenticated daemon subscription. Sharing
        // a bridge by session ID would let a later caller inherit the first
        // caller's ownership check and would also destroy per-client replay
        // cursor semantics.
        let mut subscription = daemon
            .subscribe(user_id, session_id, after_sequence, replay_limit)
            .await?;
        let (event_sender, receiver) = broadcast::channel(HIVE_EVENT_BUFFER);
        let daemon = daemon.clone();
        let session_id_owned = session_id.to_string();
        let user_id_owned = user_id.map(ToOwned::to_owned);
        let mut shutdown = self.subscription_shutdown.subscribe();
        tokio::spawn(async move {
            let mut last_sequence = after_sequence.filter(|sequence| *sequence >= 0);
            if replay_limit == Some(0) {
                // A live-only subscription is established atomically at this
                // high-water mark. Keeping it as the cursor lets a reconnect
                // replay anything committed after that point instead of
                // silently losing the outage window.
                last_sequence =
                    max_sequence(last_sequence, subscription.accepted.high_water_sequence);
            }
            // Recovery is an internal catch-up operation, not a repeat of a
            // caller's initial sampling preference (including live-only or a
            // tiny replay page). Bound it to the bridge capacity so an outage
            // can be replayed without creating an immediate local lag burst.
            let reconnect_replay_limit = HIVE_EVENT_BUFFER;
            let mut reconnect_attempts = 0usize;
            let mut subscription_connected_at = Instant::now();

            loop {
                let received = tokio::select! {
                    _ = event_sender.closed() => break,
                    _ = shutdown.recv() => break,
                    received = subscription.next_event() => received,
                };
                match received {
                    Ok(Some(event)) => {
                        let session_matches =
                            event.session_id.as_deref() == Some(session_id_owned.as_str());
                        let global_shutdown = event.session_id.is_none()
                            && matches!(
                                &event.event,
                                mitsuro_hive_protocol::HiveEvent::DaemonShuttingDown { .. }
                            );
                        if !session_matches && !global_shutdown {
                            tracing::warn!(
                                session_id = %session_id_owned,
                                "Hive daemon event subscription returned an unexpected session"
                            );
                        } else {
                            let sequence = event.sequence;
                            if sequence
                                .zip(last_sequence)
                                .is_some_and(|(sequence, cursor)| sequence <= cursor)
                            {
                                continue;
                            }
                            if event_sender.send(map_daemon_event(event)).is_err() {
                                break;
                            }
                            last_sequence = max_sequence(last_sequence, sequence);
                            // Any successfully forwarded event proves that the
                            // replacement stream is healthy. A later outage gets
                            // its own bounded retry budget.
                            reconnect_attempts = 0;
                            continue;
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(
                            session_id = %session_id_owned,
                            "Hive daemon event subscription closed unexpectedly"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            session_id = %session_id_owned,
                            error_kind = daemon_subscription_stream_error_kind(&error),
                            "Hive daemon event subscription failed"
                        );
                    }
                }

                // An idle stream that stayed connected for a meaningful
                // interval was healthy even if it carried no events. Do not
                // accumulate retry debt across unrelated, widely separated
                // daemon restarts; only flapping connections exhaust the
                // consecutive retry budget.
                if subscription_connected_at.elapsed() >= HIVE_SUBSCRIPTION_STABLE_WINDOW {
                    reconnect_attempts = 0;
                }

                loop {
                    if reconnect_attempts >= HIVE_SUBSCRIPTION_RECONNECT_ATTEMPTS {
                        let _ = event_sender.send(AgenticEvent::Error {
                            error: "Hive event stream is unavailable after repeated reconnect attempts; reconnect to continue"
                                .to_string(),
                        });
                        tracing::error!(
                            session_id = %session_id_owned,
                            attempts = reconnect_attempts,
                            "Hive daemon event subscription reconnect budget exhausted"
                        );
                        return;
                    }

                    reconnect_attempts += 1;
                    let delay =
                        daemon_subscription_reconnect_delay(&session_id_owned, reconnect_attempts);
                    tokio::select! {
                        _ = event_sender.closed() => return,
                        _ = shutdown.recv() => return,
                        _ = tokio::time::sleep(delay) => {}
                    }

                    let reconnect = tokio::select! {
                        _ = event_sender.closed() => return,
                        _ = shutdown.recv() => return,
                        reconnect = tokio::time::timeout(
                            HIVE_SUBSCRIPTION_RECONNECT_ATTEMPT_TIMEOUT,
                            daemon.subscribe(
                                user_id_owned.as_deref(),
                                &session_id_owned,
                                last_sequence,
                                Some(reconnect_replay_limit),
                            ),
                        ) => reconnect,
                    };
                    match reconnect {
                        Ok(Ok(reconnected)) => {
                            subscription = reconnected;
                            subscription_connected_at = Instant::now();
                            break;
                        }
                        Ok(Err(error)) => {
                            tracing::warn!(
                                session_id = %session_id_owned,
                                attempt = reconnect_attempts,
                                error_kind = daemon_subscription_reconnect_error_kind(&error),
                                "Hive daemon event subscription reconnect failed"
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                session_id = %session_id_owned,
                                attempt = reconnect_attempts,
                                "Hive daemon event subscription reconnect timed out"
                            );
                        }
                    }
                }
            }
        });
        Ok(receiver)
    }

    fn follow_worker_input(
        &self,
        database_path: PathBuf,
        user_id: Option<&str>,
        requested_session_id: &str,
        expected_worker_id: &str,
        response: WorkerConversationInputResponse,
        initial_subscription: EventSubscription,
    ) -> Result<broadcast::Receiver<AgenticEvent>> {
        anyhow::ensure!(
            response.session_id == requested_session_id && response.worker_id == expected_worker_id,
            "Hive Worker input acceptance does not match the requested durable DM"
        );
        let worker_id = response.worker_id.clone();
        let session_id = response.session_id.clone();
        let owner_user_id = user_id.map(ToOwned::to_owned);
        let daemon = self
            .daemon
            .clone()
            .context("Worker input follow requires the daemon control plane")?;
        let (event_sender, receiver) = broadcast::channel(HIVE_EVENT_BUFFER);
        let mut shutdown = self.subscription_shutdown.subscribe();

        match response.disposition {
            WorkerConversationInputDisposition::Queued => {
                let response_for_projection = response;
                let projection_owner_user_id = owner_user_id.clone();
                tokio::spawn(async move {
                    let projection = tokio::task::spawn_blocking(move || {
                        queued_worker_follow_target(
                            &database_path,
                            projection_owner_user_id.as_deref(),
                            &response_for_projection,
                        )
                    })
                    .await;
                    let target = match projection {
                        Ok(Ok(target)) => target,
                        Ok(Err(error)) => {
                            send_exact_worker_error(
                                &event_sender,
                                format!("Exact Worker response binding failed: {error:#}"),
                            );
                            return;
                        }
                        Err(error) => {
                            send_exact_worker_error(
                                &event_sender,
                                format!("Worker response binding task failed: {error}"),
                            );
                            return;
                        }
                    };
                    relay_exact_worker_run(
                        daemon,
                        initial_subscription,
                        owner_user_id,
                        worker_id,
                        session_id,
                        target,
                        event_sender,
                        shutdown,
                    )
                    .await;
                });
            }
            WorkerConversationInputDisposition::Staged => {
                let staged_input_id = response
                    .staged_input_id
                    .clone()
                    .context("staged Worker input has no durable input id")?;
                let active_run_id = response.run_id;
                let _ = event_sender.send(AgenticEvent::WorkerInputStaged {
                    worker_id: worker_id.clone(),
                    session_id: session_id.clone(),
                    active_run_id: active_run_id.clone(),
                    staged_input_id: staged_input_id.clone(),
                    successor_run_id: None,
                });
                tokio::spawn(async move {
                    let target = loop {
                        let poll_database_path = database_path.clone();
                        let poll_worker_id = worker_id.clone();
                        let poll_session_id = session_id.clone();
                        let poll_active_run_id = active_run_id.clone();
                        let poll_staged_input_id = staged_input_id.clone();
                        let poll_owner_user_id = owner_user_id.clone();
                        let projection = tokio::task::spawn_blocking(move || {
                            staged_worker_follow_projection(
                                &poll_database_path,
                                &poll_worker_id,
                                poll_owner_user_id.as_deref(),
                                &poll_session_id,
                                &poll_active_run_id,
                                &poll_staged_input_id,
                            )
                        })
                        .await;
                        match projection {
                            Ok(Ok(StagedWorkerFollowProjection::Materialized(target))) => {
                                break target;
                            }
                            Ok(Ok(StagedWorkerFollowProjection::Waiting)) => {}
                            Ok(Err(error)) => {
                                send_exact_worker_error(
                                    &event_sender,
                                    format!(
                                        "Durable Worker input follow failed before materialization: {error:#}"
                                    ),
                                );
                                return;
                            }
                            Err(error) => {
                                send_exact_worker_error(
                                    &event_sender,
                                    format!("Worker input follow task failed: {error}"),
                                );
                                return;
                            }
                        }
                        tokio::select! {
                            _ = event_sender.closed() => return,
                            _ = shutdown.recv() => return,
                            _ = tokio::time::sleep(WORKER_STAGED_INPUT_POLL_INTERVAL) => {}
                        }
                    };
                    let _ = event_sender.send(AgenticEvent::WorkerInputStaged {
                        worker_id: worker_id.clone(),
                        session_id: session_id.clone(),
                        active_run_id,
                        staged_input_id,
                        successor_run_id: Some(target.run_id.clone()),
                    });
                    relay_exact_worker_run(
                        daemon,
                        initial_subscription,
                        owner_user_id,
                        worker_id,
                        session_id,
                        target,
                        event_sender,
                        shutdown,
                    )
                    .await;
                });
            }
        }
        Ok(receiver)
    }

    pub async fn begin_daemon_chat_turn_for_user(
        &self,
        database_path: PathBuf,
        session_id: &str,
        message: &str,
        user_id: Option<&str>,
        is_first_message: bool,
        idempotency_key: Option<&str>,
    ) -> Result<broadcast::Receiver<AgenticEvent>> {
        let daemon = self
            .daemon
            .as_ref()
            .context("Hive chat requires the daemon control plane")?;
        let target = classify_hive_conversation_at(&database_path, session_id, user_id)?;
        let expected_worker_id = match &target {
            HiveConversationTarget::Primary => None,
            HiveConversationTarget::WorkerDm { worker_id } => Some(worker_id.clone()),
        };
        if is_first_message {
            // Send creates the controller for legacy/new-chat sessions. Start
            // then queues the first run, and sequence-zero replay closes the
            // race between those durable mutations and subscription.
            let staged_cursor = daemon.subscribe(user_id, session_id, None, Some(0)).await?;
            let acceptance = match &target {
                HiveConversationTarget::Primary => {
                    daemon
                        .send_message(user_id, session_id, message, idempotency_key)
                        .await?
                }
                HiveConversationTarget::WorkerDm { .. } => {
                    daemon
                        .send_worker_message(user_id, session_id, message, idempotency_key)
                        .await?
                }
            };
            match (&target, acceptance) {
                (
                    HiveConversationTarget::WorkerDm { .. },
                    DaemonInputAcceptance::Worker(response),
                ) => {
                    // The atomic Worker input mutation already selected its
                    // exact queued run or staged successor. Follow only that
                    // durable identity; a generic StartSession would be both
                    // redundant and workspace-bound.
                    return self.follow_worker_input(
                        database_path,
                        user_id,
                        session_id,
                        expected_worker_id
                            .as_deref()
                            .context("Worker DM classification lost its Worker binding")?,
                        response,
                        staged_cursor,
                    );
                }
                (HiveConversationTarget::Primary, DaemonInputAcceptance::Ack(_)) => {
                    daemon
                        .start(user_id, session_id, "chat_first_message", idempotency_key)
                        .await?;
                }
                (HiveConversationTarget::WorkerDm { .. }, DaemonInputAcceptance::Ack(_)) => {
                    anyhow::bail!(
                        "Hive Worker input returned an untyped acknowledgement; refusing generic session startup"
                    );
                }
                (HiveConversationTarget::Primary, DaemonInputAcceptance::Worker(_)) => {
                    anyhow::bail!("primary Hive chat returned a Worker input acceptance");
                }
            }
            return self
                .subscribe_for_user_from(session_id, user_id, Some(0), Some(256))
                .await;
        }

        // Existing interactive controllers can subscribe live before the
        // message so no response event races past the stream.
        let staged_cursor = daemon.subscribe(user_id, session_id, None, Some(0)).await?;
        let primary_receiver = match &target {
            HiveConversationTarget::Primary => {
                Some(self.subscribe_for_user(session_id, user_id).await?)
            }
            HiveConversationTarget::WorkerDm { .. } => None,
        };
        let acceptance = match &target {
            HiveConversationTarget::Primary => {
                daemon
                    .send_message(user_id, session_id, message, idempotency_key)
                    .await?
            }
            HiveConversationTarget::WorkerDm { .. } => {
                daemon
                    .send_worker_message(user_id, session_id, message, idempotency_key)
                    .await?
            }
        };
        match (&target, acceptance) {
            (HiveConversationTarget::WorkerDm { .. }, DaemonInputAcceptance::Worker(response)) => {
                self.follow_worker_input(
                    database_path,
                    user_id,
                    session_id,
                    expected_worker_id
                        .as_deref()
                        .context("Worker DM classification lost its Worker binding")?,
                    response,
                    staged_cursor,
                )
            }
            (HiveConversationTarget::Primary, DaemonInputAcceptance::Ack(_)) => primary_receiver
                .context("primary Hive chat did not establish its live event subscription"),
            (HiveConversationTarget::WorkerDm { .. }, DaemonInputAcceptance::Ack(_)) => {
                anyhow::bail!("Hive Worker input returned an untyped acknowledgement")
            }
            (HiveConversationTarget::Primary, DaemonInputAcceptance::Worker(_)) => {
                anyhow::bail!("primary Hive chat returned a Worker input acceptance")
            }
        }
    }

    pub async fn pause_session(&self, state: &AppState, session_id: &str) -> Result<()> {
        self.stop_active_run(state, session_id).await;
        persist_runtime_state(
            &state.db_path,
            session_id,
            HiveRuntimeStateStatus::Paused,
            None,
            None,
            None,
            None,
            Some("paused"),
        )
    }

    pub async fn pause_session_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        self.ensure_primary_hive_generic_control(state, session_id, user_id, "pause")?;
        if let Some(daemon) = &self.daemon {
            return daemon.pause(user_id, session_id, idempotency_key).await;
        }
        self.pause_session(state, session_id).await
    }

    pub async fn schedule_session(
        &self,
        state: &AppState,
        session_id: String,
        wake_at: chrono::DateTime<chrono::Utc>,
        wake_reason: &'static str,
        sleep_reason: &'static str,
    ) -> Result<()> {
        self.stop_active_run(state, &session_id).await;
        ensure_runnable_hive_session(&state.db_path, &session_id)?;
        persist_runtime_state(
            &state.db_path,
            &session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some(&wake_at.to_rfc3339()),
            Some(sleep_reason),
            None,
            None,
            Some(wake_reason),
        )?;
        self.schedule_wake_at(state.clone(), session_id, wake_at, wake_reason)
            .await;
        Ok(())
    }

    pub async fn schedule_session_for_user(
        &self,
        state: &AppState,
        session_id: String,
        wake_at: chrono::DateTime<chrono::Utc>,
        wake_reason: &'static str,
        sleep_reason: &'static str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        self.ensure_primary_hive_generic_control(state, &session_id, user_id, "schedule")?;
        if let Some(daemon) = &self.daemon {
            return daemon
                .schedule(
                    user_id,
                    &session_id,
                    wake_at.timestamp_millis(),
                    wake_reason,
                    idempotency_key,
                )
                .await;
        }
        self.schedule_session(state, session_id, wake_at, wake_reason, sleep_reason)
            .await
    }

    pub async fn resume_session_for_user(
        &self,
        state: AppState,
        session_id: String,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        self.ensure_primary_hive_generic_control(&state, &session_id, user_id, "resume")?;
        if let Some(daemon) = &self.daemon {
            return daemon.resume(user_id, &session_id, idempotency_key).await;
        }
        self.start_or_restart_session(state, session_id, "resume")
            .await
    }

    pub async fn cancel_session_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        let target = classify_hive_conversation(state, session_id, user_id)?;
        if matches!(target, HiveConversationTarget::WorkerDm { .. }) {
            let daemon = self
                .daemon
                .as_ref()
                .context("Hive Worker Stop requires the daemon control plane")?;
            daemon
                .stop_worker_conversation(user_id, session_id, idempotency_key)
                .await?;
            return Ok(());
        }
        if let Some(daemon) = &self.daemon {
            return daemon.cancel(user_id, session_id, idempotency_key).await;
        }
        self.stop_active_run(state, session_id).await;
        Ok(())
    }

    pub async fn delete_session_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        self.ensure_generic_session_delete_allowed(state, session_id, user_id)?;
        if let Some(daemon) = &self.daemon {
            daemon.delete(user_id, session_id, idempotency_key).await?;
            self.forget_session(session_id).await;
            state.session_locks.write().await.remove(session_id);
            return Ok(());
        }

        self.stop_active_run(state, session_id).await;
        self.forget_session(session_id).await;
        SessionManager::new(Database::new(&state.db_path)?).delete_session(session_id)?;
        state.session_locks.write().await.remove(session_id);
        Ok(())
    }

    /// Generic session deletion is never an ownership surface for durable
    /// Worker DMs or internal group lanes. Keep this check at the manager
    /// boundary as defense in depth for callers outside the HTTP route.
    pub fn ensure_generic_session_delete_allowed(
        &self,
        state: &AppState,
        session_id: &str,
        user_id: Option<&str>,
    ) -> Result<()> {
        match classify_hive_conversation(state, session_id, user_id)? {
            HiveConversationTarget::Primary => Ok(()),
            HiveConversationTarget::WorkerDm { .. } => Err(HiveDaemonError::Remote {
                code: "state_conflict".to_string(),
                message: "Hive Worker conversations are durable product lanes; archive the Worker from /api/hive/workers instead".to_string(),
            }
            .into()),
        }
    }

    fn ensure_primary_hive_generic_control(
        &self,
        state: &AppState,
        session_id: &str,
        user_id: Option<&str>,
        operation: &str,
    ) -> Result<()> {
        match classify_hive_conversation(state, session_id, user_id)? {
            HiveConversationTarget::Primary => Ok(()),
            HiveConversationTarget::WorkerDm { .. } => Err(HiveDaemonError::Remote {
                code: "state_conflict".to_string(),
                message: format!(
                    "Hive Worker conversations require typed Worker {operation} controls"
                ),
            }
            .into()),
        }
    }

    pub async fn send_message_for_user(
        &self,
        state: AppState,
        session_id: String,
        message: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        let worker_dm = matches!(
            classify_hive_conversation(&state, &session_id, user_id)?,
            HiveConversationTarget::WorkerDm { .. }
        );
        if let Some(daemon) = &self.daemon {
            if worker_dm {
                daemon
                    .send_worker_message(user_id, &session_id, message, idempotency_key)
                    .await?;
            } else {
                daemon
                    .send_message(user_id, &session_id, message, idempotency_key)
                    .await?;
            }
            return Ok(());
        }
        if worker_dm {
            anyhow::bail!("Hive Worker messages require the daemon control plane");
        }

        let session_manager = SessionManager::new(Database::new(&state.db_path)?);
        let content_json = serde_json::json!([{ "type": "text", "text": message }]).to_string();
        session_manager.save_message(&session_id, "user", &content_json)?;
        self.start_or_restart_session(state, session_id, "user_message")
            .await
    }

    /// Send one room message into a group as a durable turn. Group turns are
    /// run-triggering mutations and therefore fail closed onto the daemon
    /// control plane, exactly like chat turns.
    pub async fn group_message_for_user(
        &self,
        group_id: &str,
        message: &str,
        mentions_override: Option<Vec<String>>,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<mitsuro_hive_protocol::GroupTurnResponse> {
        let daemon = self
            .daemon
            .as_ref()
            .context("Hive group turns require the daemon control plane")?;
        daemon
            .group_message(
                user_id,
                group_id,
                message,
                mentions_override,
                idempotency_key,
            )
            .await
    }

    /// Cancel the active turn's in-flight member runs through the daemon.
    pub async fn group_stop_for_user(
        &self,
        group_id: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        let daemon = self
            .daemon
            .as_ref()
            .context("Hive group turns require the daemon control plane")?;
        daemon.group_stop(user_id, group_id, idempotency_key).await
    }

    /// Atomically stop active member work and archive the group through the
    /// daemon's ownership-checked mutation transaction.
    pub async fn group_archive_for_user(
        &self,
        group_id: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        let daemon = self
            .daemon
            .as_ref()
            .context("Hive group archive requires the daemon control plane")?;
        daemon
            .group_archive(user_id, group_id, idempotency_key)
            .await
    }

    pub async fn set_priority_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        priority: HiveRunPriority,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        if let Some(daemon) = &self.daemon {
            return daemon
                .set_priority(user_id, session_id, priority.as_str(), idempotency_key)
                .await;
        }
        HiveRuntimeStateStore::new(Database::new(&state.db_path)?)
            .set_priority(session_id, priority)?;
        Ok(())
    }

    pub async fn set_crew_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        crew_slug: Option<&str>,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        if let Some(daemon) = &self.daemon {
            return daemon
                .set_crew(user_id, session_id, crew_slug, idempotency_key)
                .await;
        }
        HiveRuntimeStateStore::new(Database::new(&state.db_path)?)
            .set_crew_slug(session_id, crew_slug)?;
        Ok(())
    }

    pub async fn steer_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        pending_id: &str,
        content: Vec<mitsuro_core::ai::types::Content>,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<HiveSteerResult> {
        let worker_dm = matches!(
            classify_hive_conversation(state, session_id, user_id)?,
            HiveConversationTarget::WorkerDm { .. }
        );
        if let Some(daemon) = &self.daemon {
            let content = serde_json::to_value(content)?;
            let acceptance = if worker_dm {
                daemon
                    .steer_worker(user_id, session_id, pending_id, content, idempotency_key)
                    .await?
            } else {
                daemon
                    .steer(user_id, session_id, pending_id, content, idempotency_key)
                    .await?
            };
            return match acceptance {
                DaemonInputAcceptance::Worker(response) => HiveSteerResult::worker(response),
                DaemonInputAcceptance::Ack(acknowledgement) if acknowledgement.accepted => {
                    Ok(HiveSteerResult::plain(HiveSteerStatus::Accepted))
                }
                DaemonInputAcceptance::Ack(acknowledgement)
                    if acknowledgement.message.as_deref() == Some("queued") =>
                {
                    Ok(HiveSteerResult::plain(HiveSteerStatus::Queued))
                }
                DaemonInputAcceptance::Ack(acknowledgement) => Err(HiveDaemonError::Remote {
                    code: "conflict".to_string(),
                    message: format!(
                        "Hive declined steering: {}",
                        acknowledgement
                            .message
                            .unwrap_or_else(|| "no reason provided".to_string())
                    ),
                }
                .into()),
            };
        }
        if worker_dm {
            anyhow::bail!("Hive Worker steering requires the daemon control plane");
        }

        let content_json = serde_json::to_string(&content)?;
        SessionManager::new(Database::new(&state.db_path)?).queue_pending_steering(
            session_id,
            pending_id,
            &content_json,
        )?;
        let sender = state.session_inputs.read().await.get(session_id).cloned();
        let Some(sender) = sender else {
            return Ok(HiveSteerResult::plain(HiveSteerStatus::Queued));
        };
        let input = LoopInput::Steer {
            pending_id: Some(pending_id.to_string()),
            content,
        };
        Ok(HiveSteerResult::plain(if sender.send(input).is_ok() {
            HiveSteerStatus::Accepted
        } else {
            HiveSteerStatus::Queued
        }))
    }

    pub async fn tool_approval_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        run_id: &str,
        tool_call_id: &str,
        approved: bool,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        if let Some(daemon) = &self.daemon {
            return daemon
                .tool_approval(
                    user_id,
                    session_id,
                    run_id,
                    tool_call_id,
                    approved,
                    idempotency_key,
                )
                .await;
        }
        let sender = state
            .session_inputs
            .read()
            .await
            .get(session_id)
            .cloned()
            .context("No active Hive session")?;
        sender
            .send(LoopInput::ToolApproval {
                tool_call_id: tool_call_id.to_string(),
                approved,
            })
            .context("Hive session is no longer accepting tool approvals")
    }

    pub async fn user_response_and_subscribe_for_user(
        &self,
        state: &AppState,
        session_id: &str,
        run_id: &str,
        tool_call_id: &str,
        response: &str,
        user_id: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<broadcast::Receiver<AgenticEvent>> {
        let worker_dm = matches!(
            classify_hive_conversation(state, session_id, user_id)?,
            HiveConversationTarget::WorkerDm { .. }
        );
        if worker_dm && self.daemon.is_none() {
            anyhow::bail!("Hive Worker user responses require the daemon control plane");
        }
        let receiver = self.subscribe_for_user(session_id, user_id).await?;
        if let Some(daemon) = &self.daemon {
            if worker_dm {
                daemon
                    .worker_user_response(
                        user_id,
                        session_id,
                        run_id,
                        tool_call_id,
                        response,
                        idempotency_key,
                    )
                    .await?;
            } else {
                daemon
                    .user_response(
                        user_id,
                        session_id,
                        run_id,
                        tool_call_id,
                        response,
                        idempotency_key,
                    )
                    .await?;
            }
            return Ok(receiver);
        }
        let sender = state
            .session_inputs
            .read()
            .await
            .get(session_id)
            .cloned()
            .context("No active Hive session")?;
        sender
            .send(LoopInput::UserResponse {
                tool_call_id: tool_call_id.to_string(),
                response: response.to_string(),
            })
            .context("Hive session is no longer accepting user responses")?;
        Ok(receiver)
    }

    pub async fn stop_active_run(&self, state: &AppState, session_id: &str) {
        self.cancel_scheduled_wake(session_id).await;

        let runtime = self.runtimes.write().await.remove(session_id);
        let Some(runtime) = runtime else {
            return;
        };

        let maybe_input = {
            let inputs = state.session_inputs.read().await;
            inputs.get(session_id).cloned()
        };

        let join_handle = runtime.join_handle;
        let sent_cancel = maybe_input
            .as_ref()
            .map(|sender| sender.send(LoopInput::Cancel).is_ok())
            .unwrap_or(false);

        if !sent_cancel {
            join_handle.abort();
        }
        let _ = join_handle.await;
        state.session_inputs.write().await.remove(session_id);
    }

    pub async fn finish_run(&self, session_id: &str, run_id: &str) {
        let mut runtimes = self.runtimes.write().await;
        let should_remove = runtimes
            .get(session_id)
            .map(|runtime| runtime.run_id == run_id)
            .unwrap_or(false);
        if should_remove {
            runtimes.remove(session_id);
        }
    }

    async fn schedule_wake_at(
        &self,
        state: AppState,
        session_id: String,
        wake_at: chrono::DateTime<chrono::Utc>,
        wake_reason: &'static str,
    ) {
        self.cancel_scheduled_wake(&session_id).await;

        let delay = (wake_at - chrono::Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        let manager = state.hive_runtime.clone();
        let wake_tx = self.wake_tx.clone();
        let session_id_clone = session_id.clone();
        let wake_reason_owned = wake_reason.to_string();
        let join_handle = tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            manager.clear_scheduled_wake(&session_id_clone).await;
            if wake_tx
                .send(WakeCommand {
                    state,
                    session_id: session_id_clone.clone(),
                    wake_reason: wake_reason_owned,
                })
                .is_err()
            {
                tracing::error!(
                    session_id = %session_id_clone,
                    "Failed to queue sleeping Hive session for wake"
                );
            }
        });

        self.scheduled_wakes
            .write()
            .await
            .insert(session_id, join_handle);
    }

    async fn clear_scheduled_wake(&self, session_id: &str) {
        self.scheduled_wakes.write().await.remove(session_id);
    }

    async fn cancel_scheduled_wake(&self, session_id: &str) {
        let handle = self.scheduled_wakes.write().await.remove(session_id);
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HiveConversationTarget {
    Primary,
    WorkerDm { worker_id: String },
}

fn classify_hive_conversation(
    state: &AppState,
    session_id: &str,
    user_id: Option<&str>,
) -> Result<HiveConversationTarget> {
    classify_hive_conversation_at(state.db_path.as_path(), session_id, user_id)
}

fn classify_hive_conversation_at(
    database_path: &Path,
    session_id: &str,
    user_id: Option<&str>,
) -> Result<HiveConversationTarget> {
    let database = Database::new(database_path)?;
    let Some(binding) = resolve_worker_conversation_with_conn(database.conn(), session_id)? else {
        let worker_lane_claimed: bool = database.conn().query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM hive_controllers
                 WHERE session_id = ?1 AND worker_id IS NOT NULL
                 UNION ALL
                 SELECT 1 FROM hive_runs
                 WHERE session_id = ?1 AND worker_id IS NOT NULL
                 UNION ALL
                 SELECT 1 FROM hive_group_worker_lanes WHERE session_id = ?1
             )",
            [session_id],
            |row| row.get(0),
        )?;
        if !worker_lane_claimed {
            return Ok(HiveConversationTarget::Primary);
        }
        return Err(HiveDaemonError::Remote {
            code: "not_found".to_string(),
            message: "Hive session was not found".to_string(),
        }
        .into());
    };
    if binding.worker.user_id.as_deref() != user_id
        || binding.group_id.is_some()
        || binding.worker.dm_session_id.as_deref() != Some(session_id)
    {
        return Err(HiveDaemonError::Remote {
            code: "not_found".to_string(),
            message: "Hive session was not found".to_string(),
        }
        .into());
    }
    Ok(HiveConversationTarget::WorkerDm {
        worker_id: binding.worker.id,
    })
}

impl Drop for HiveRuntimeManager {
    fn drop(&mut self) {
        // Subscription bridges otherwise retain their daemon client after the
        // manager has gone away. Waking them here closes the authenticated IPC
        // streams promptly during shutdown and in short-lived server hosts.
        let _ = self.subscription_shutdown.send(());
    }
}

fn max_sequence(current: Option<i64>, candidate: Option<i64>) -> Option<i64> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (Some(current), None) => Some(current),
        (None, Some(candidate)) => Some(candidate),
        (None, None) => None,
    }
}

fn daemon_subscription_reconnect_delay(session_id: &str, attempt: usize) -> Duration {
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
    let multiplier = 2u32.checked_pow(exponent).unwrap_or(u32::MAX);
    let base = HIVE_SUBSCRIPTION_RECONNECT_BASE_DELAY
        .checked_mul(multiplier)
        .unwrap_or(HIVE_SUBSCRIPTION_RECONNECT_MAX_DELAY)
        .min(HIVE_SUBSCRIPTION_RECONNECT_MAX_DELAY);
    let jitter_room = HIVE_SUBSCRIPTION_RECONNECT_MAX_DELAY.saturating_sub(base);
    if jitter_room.is_zero() {
        return base;
    }

    // Stable per-session jitter avoids reconnect herds while keeping tests and
    // incident timing reproducible. It is deliberately bounded to 25% of the
    // exponential delay and by the absolute retry ceiling.
    let jitter_cap = (base / 4).min(jitter_room);
    if jitter_cap.is_zero() {
        return base;
    }
    let seed = session_id.bytes().fold(attempt as u64, |seed, byte| {
        seed.wrapping_mul(1_099_511_628_211)
            .wrapping_add(u64::from(byte))
    });
    let jitter_millis = seed % (jitter_cap.as_millis() as u64 + 1);
    base + Duration::from_millis(jitter_millis)
}

fn daemon_subscription_stream_error_kind(error: &ClientError) -> &'static str {
    match error {
        ClientError::Remote { .. } => "remote",
        ClientError::Auth(_) | ClientError::Peer(_) => "authentication",
        ClientError::Protocol(_) | ClientError::Frame(_) => "protocol",
        ClientError::ConnectTimeout | ClientError::RequestTimeout => "timeout",
        ClientError::Closed => "closed",
        ClientError::UnsupportedPlatform => "unsupported",
        ClientError::Io(_) => "io",
    }
}

fn daemon_subscription_reconnect_error_kind(error: &anyhow::Error) -> &'static str {
    match error
        .chain()
        .find_map(|source| source.downcast_ref::<HiveDaemonError>())
    {
        Some(HiveDaemonError::Remote { .. }) => "remote",
        Some(HiveDaemonError::Unavailable(_)) => "unavailable",
        None => "unexpected",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::loop_events::LoopStopReason;
    use mitsuro_core::agent::{AgentCancellation, LoopEvent, LoopInput, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::reports::{CreateReportInput, ReportScope};
    use mitsuro_core::storage::{
        get_current_snapshot, refresh_current_snapshot, Database, HiveRuntimeStateStatus,
        HiveRuntimeStateStore, MemoryStore, MemoryType, ReportStore, SessionType,
        WorkerConversationInput, WorkerConversationInputState, WorkspaceMode,
    };
    use mitsuro_core::tools::registry::ToolRegistry;
    use mitsuro_core::SessionManager;
    use mitsuro_hive_protocol::{
        EventEnvelope, ExtensionEvent, HiveEvent, ProtocolVersion, RuntimeEvent,
    };

    use super::{
        apply_runtime_event_state, control_plane_app_error, exact_worker_event_disposition,
        hive_notification_title, persist_runtime_state, refresh_snapshot_after_run,
        resolve_persisted_project_dir, validate_staged_worker_input_successor,
        with_registered_session_input, ActiveHiveRuntime, ExactWorkerEventDisposition,
        HiveDaemonError, HiveRuntimeManager, WorkerSuccessBoundary,
    };
    use crate::error::AppError;
    use crate::AppState;

    fn create_test_state() -> (AppState, PathBuf) {
        let temp_dir =
            std::env::temp_dir().join(format!("mitsuro-server-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
        let db_path = temp_dir.join("mitsuro.db");
        Database::new(&db_path).expect("database should initialize");
        let working_dir = temp_dir.join("workspace");
        std::fs::create_dir_all(&working_dir).expect("workspace should exist");

        (
            AppState {
                server_port: 3000,
                db_path: Arc::new(db_path),
                working_dir: Arc::new(working_dir.clone()),
                ai_client: None,
                tool_registry: Arc::new(ToolRegistry::new()),
                process_registry: Arc::new(ProcessRegistry::new()),
                model_registry: create_model_registry(),
                credential_store: Arc::new(RwLock::new(CredentialStore::default())),
                mcp_manager: Arc::new(McpManager::new(working_dir.clone())),
                hook_manager: Arc::new(RwLock::new(UserHookManager::new())),
                skills_manager: Arc::new(RwLock::new(SkillsManager::with_defaults(&working_dir))),
                cancellation: AgentCancellation::new(),
                session_locks: Arc::new(RwLock::new(HashMap::new())),
                session_inputs: Arc::new(RwLock::new(HashMap::new())),
                session_presence: Arc::new(RwLock::new(HashMap::new())),
                session_control_leases: Arc::new(RwLock::new(
                    crate::control_leases::SessionControlLeaseRegistry::default(),
                )),
                agent_streams: Arc::new(RwLock::new(
                    crate::agent_streams::AgentStreamHub::default(),
                )),
                delegated_state: Arc::new(RwLock::new(HashMap::new())),
                remote_access: Arc::new(RwLock::new(crate::remote_access::RemoteAccessConfig {
                    enabled: true,
                    token: "test-token".to_string(),
                })),
                active_agent_streams: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                peak_rss_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                peak_virtual_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                push_service: None,
                apns_service: None,
                oauth_flows: Arc::new(Mutex::new(HashMap::new())),
                hive_runtime: HiveRuntimeManager::new(),
            },
            temp_dir,
        )
    }

    #[test]
    fn hive_notification_title_prefers_explicit_title() {
        assert_eq!(
            hive_notification_title(Some("Verification complete"), "Auth refactor"),
            "Hive — Verification complete"
        );
    }

    #[test]
    fn daemon_protocol_codes_map_to_stable_http_classes() {
        let mapped = |code: &str| {
            control_plane_app_error(
                HiveDaemonError::Remote {
                    code: code.to_string(),
                    message: "test".to_string(),
                }
                .into(),
            )
        };

        assert!(matches!(mapped("ownership_denied"), AppError::NotFound(_)));
        assert!(matches!(mapped("invalid_command"), AppError::BadRequest(_)));
        assert!(matches!(
            mapped("idempotency_conflict"),
            AppError::Conflict(_)
        ));
        assert!(matches!(
            mapped("request_in_progress"),
            AppError::ServiceUnavailable(_)
        ));
        assert!(matches!(mapped("revision_conflict"), AppError::Conflict(_)));
        assert!(matches!(mapped("internal_error"), AppError::BadGateway(_)));
    }

    #[test]
    fn exact_worker_failure_terminals_do_not_require_a_success_boundary_chain() {
        let failure_events = [
            HiveEvent::Extension(ExtensionEvent {
                name: "agentic_event".to_string(),
                payload: serde_json::json!({"type": "error", "error": "provider failed"}),
            }),
            HiveEvent::Runtime(RuntimeEvent {
                event_type: "run_cancelled".to_string(),
                payload: serde_json::json!({"run_id": "run-1"}),
            }),
        ];
        for event in failure_events {
            let envelope = EventEnvelope {
                version: ProtocolVersion::CURRENT,
                session_id: Some("worker-dm".to_string()),
                run_id: Some("run-1".to_string()),
                sequence: Some(1),
                emitted_at_unix_ms: 0,
                event,
            };
            let mut boundary = WorkerSuccessBoundary::None;
            assert_eq!(
                exact_worker_event_disposition(
                    &envelope,
                    "worker-1",
                    "worker-dm",
                    "run-1",
                    &mut boundary,
                )
                .expect("failure terminal should be well-formed"),
                ExactWorkerEventDisposition::Error
            );
        }

        let completed_without_boundaries = EventEnvelope {
            version: ProtocolVersion::CURRENT,
            session_id: Some("worker-dm".to_string()),
            run_id: Some("run-1".to_string()),
            sequence: Some(1),
            emitted_at_unix_ms: 0,
            event: HiveEvent::Extension(ExtensionEvent {
                name: "agentic_event".to_string(),
                payload: serde_json::json!({
                    "type": "finish",
                    "session_id": "worker-dm",
                    "stop_reason": "completed",
                }),
            }),
        };
        assert!(matches!(
            exact_worker_event_disposition(
                &completed_without_boundaries,
                "worker-1",
                "worker-dm",
                "run-1",
                &mut WorkerSuccessBoundary::None,
            )
            .expect("completed terminal should be well-formed"),
            ExactWorkerEventDisposition::Invalid(_)
        ));
    }

    #[test]
    fn staged_worker_follow_waits_for_exact_durable_successor() {
        let staged = WorkerConversationInput {
            id: "input-1".to_string(),
            worker_id: "worker-1".to_string(),
            owner_user_id: Some("alice".to_string()),
            session_id: "worker-dm".to_string(),
            request_id: "request-1".to_string(),
            accepted_while_run_id: "active-run".to_string(),
            body: r#"[{"type":"text","text":"next"}]"#.to_string(),
            state: WorkerConversationInputState::Staged,
            canonical_message_id: None,
            assigned_run_id: None,
            accepted_at: "2026-08-25T12:00:00Z".to_string(),
            materialized_at: None,
        };
        assert_eq!(
            validate_staged_worker_input_successor(
                staged.clone(),
                "worker-1",
                Some("alice"),
                "worker-dm",
                "active-run",
                "input-1",
            )
            .expect("staged projection should be valid"),
            None
        );

        let materialized = WorkerConversationInput {
            state: WorkerConversationInputState::Materialized,
            canonical_message_id: Some(42),
            assigned_run_id: Some("successor-run".to_string()),
            materialized_at: Some("2026-08-25T12:00:01Z".to_string()),
            ..staged
        };
        assert_eq!(
            validate_staged_worker_input_successor(
                materialized.clone(),
                "worker-1",
                Some("alice"),
                "worker-dm",
                "active-run",
                "input-1",
            )
            .expect("materialized projection should be valid"),
            Some("successor-run".to_string())
        );
        assert!(validate_staged_worker_input_successor(
            materialized,
            "worker-1",
            Some("alice"),
            "worker-dm",
            "replacement-run",
            "input-1",
        )
        .is_err());
    }

    #[test]
    fn hive_notification_title_falls_back_to_session_label() {
        assert_eq!(
            hive_notification_title(Some("   "), "Auth refactor"),
            "Hive — Auth refactor"
        );
        assert_eq!(
            hive_notification_title(None, "Auth refactor"),
            "Hive — Auth refactor"
        );
    }

    fn create_session(state: &AppState, session_type: SessionType) -> String {
        let session_manager =
            SessionManager::new(Database::new(&state.db_path).expect("database should open"));
        session_manager
            .create_session_for_user_with_config(
                "Test Session",
                None,
                None,
                None,
                WorkspaceMode::Neutral,
                None,
                None,
                session_type,
            )
            .expect("session should create")
    }

    fn create_test_user(state: &AppState, user_id: &str) {
        let db = Database::new(&state.db_path).expect("database should open");
        db.conn()
            .execute(
                "INSERT INTO users (id, email, license_tier) VALUES (?1, ?2, ?3)",
                (user_id, format!("{user_id}@example.com"), "free"),
            )
            .expect("user should insert");
    }

    #[test]
    fn resolve_persisted_project_dir_supports_relative_and_absolute_paths() {
        let workspace = Path::new("/workspace");

        assert_eq!(
            resolve_persisted_project_dir(Some("repo"), workspace),
            Some(workspace.join("repo"))
        );
        assert_eq!(
            resolve_persisted_project_dir(Some("/tmp/project"), workspace),
            Some(PathBuf::from("/tmp/project"))
        );
        assert_eq!(resolve_persisted_project_dir(Some("   "), workspace), None);
    }

    #[tokio::test]
    async fn start_or_restart_session_rejects_missing_session_without_leaking_runtime_state() {
        let (state, _temp_dir) = create_test_state();

        let result = state
            .hive_runtime
            .start_or_restart_session(state.clone(), "missing-session".to_string(), "test")
            .await;

        assert!(result.is_err());
        assert!(!state
            .hive_runtime
            .event_streams
            .read()
            .await
            .contains_key("missing-session"));
        assert!(!state
            .hive_runtime
            .runtimes
            .read()
            .await
            .contains_key("missing-session"));

        let runtime_store = HiveRuntimeStateStore::new(
            Database::new(&state.db_path).expect("database should open"),
        );
        assert!(runtime_store
            .get_state("missing-session")
            .expect("runtime state lookup should succeed")
            .is_none());
    }

    #[tokio::test]
    async fn start_or_restart_session_rejects_non_hive_session_without_persisting_runtime_state() {
        let (state, _temp_dir) = create_test_state();
        let session_id = create_session(&state, SessionType::Code);

        let result = state
            .hive_runtime
            .start_or_restart_session(state.clone(), session_id.clone(), "test")
            .await;

        assert!(result.is_err());
        assert!(!state
            .hive_runtime
            .event_streams
            .read()
            .await
            .contains_key(&session_id));
        assert!(!state
            .hive_runtime
            .runtimes
            .read()
            .await
            .contains_key(&session_id));

        let runtime_store = HiveRuntimeStateStore::new(
            Database::new(&state.db_path).expect("database should open"),
        );
        assert!(runtime_store
            .get_state(&session_id)
            .expect("runtime state lookup should succeed")
            .is_none());
    }

    #[tokio::test]
    async fn with_registered_session_input_clears_entry_on_error() {
        let session_inputs = Arc::new(RwLock::new(HashMap::new()));
        let (input_tx, _input_rx) = tokio::sync::mpsc::unbounded_channel::<LoopInput>();

        let result: anyhow::Result<()> = with_registered_session_input(
            Arc::clone(&session_inputs),
            "session-1".to_string(),
            input_tx,
            async { anyhow::bail!("boom") },
        )
        .await;

        assert!(result.is_err());
        assert!(session_inputs.read().await.is_empty());
    }

    #[tokio::test]
    async fn stop_active_run_clears_session_input_when_runtime_is_aborted() {
        let (state, _temp_dir) = create_test_state();
        let session_id = "session-1".to_string();
        let join_handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });

        state.hive_runtime.runtimes.write().await.insert(
            session_id.clone(),
            ActiveHiveRuntime {
                run_id: "run-1".to_string(),
                join_handle,
            },
        );

        let (input_tx, input_rx) = tokio::sync::mpsc::unbounded_channel::<LoopInput>();
        drop(input_rx);
        state
            .session_inputs
            .write()
            .await
            .insert(session_id.clone(), input_tx);

        state
            .hive_runtime
            .stop_active_run(&state, &session_id)
            .await;

        assert!(!state
            .hive_runtime
            .runtimes
            .read()
            .await
            .contains_key(&session_id));
        assert!(state.session_inputs.read().await.get(&session_id).is_none());
    }

    #[tokio::test]
    async fn apply_runtime_event_state_preserves_existing_wake_reason_for_running_updates() {
        let (state, _temp_dir) = create_test_state();
        let session_id = create_session(&state, SessionType::Hive);
        persist_runtime_state(
            &state.db_path,
            &session_id,
            HiveRuntimeStateStatus::Running,
            None,
            None,
            None,
            Some("run-1"),
            Some("dispatch"),
        )
        .expect("seed runtime state");

        apply_runtime_event_state(
            &state.db_path,
            &session_id,
            "run-2",
            &LoopEvent::ToolCallStart {
                id: "tool-1".to_string(),
                name: "read".to_string(),
            },
        )
        .expect("running update should persist");

        let runtime_store = HiveRuntimeStateStore::new(
            Database::new(&state.db_path).expect("database should open"),
        );
        let runtime = runtime_store
            .get_state(&session_id)
            .expect("runtime lookup should succeed")
            .expect("runtime state should exist");

        assert_eq!(runtime.status, HiveRuntimeStateStatus::Running);
        assert_eq!(runtime.current_run_id.as_deref(), Some("run-2"));
        assert_eq!(runtime.last_wake_reason.as_deref(), Some("dispatch"));
    }

    #[tokio::test]
    async fn refresh_snapshot_after_run_updates_stale_snapshot_content() {
        let (state, temp_dir) = create_test_state();
        create_test_user(&state, "alice");
        let user_root = temp_dir.join("alice-home");
        let project_dir = user_root.join("repo");
        std::fs::create_dir_all(&project_dir).expect("project dir should exist");

        let session_manager =
            SessionManager::new(Database::new(&state.db_path).expect("database should open"));
        let session_id = session_manager
            .create_session_for_user_with_config(
                "Knowledge Run",
                None,
                Some(project_dir.to_string_lossy().as_ref()),
                Some(project_dir.to_string_lossy().as_ref()),
                WorkspaceMode::Selected,
                Some("alice"),
                None,
                SessionType::Hive,
            )
            .expect("session should create");

        let memory_store =
            MemoryStore::new(Database::new(&state.db_path).expect("database should open"));
        memory_store
            .save(
                MemoryType::Project,
                "Initial context",
                "Stale snapshot source",
                Some(project_dir.to_string_lossy().as_ref()),
                Some("alice"),
            )
            .expect("initial context should persist");
        let snapshot = refresh_current_snapshot(
            &state.db_path,
            Some(project_dir.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("initial snapshot should refresh")
        .expect("initial snapshot should exist");
        Database::new(&state.db_path)
            .expect("database should open")
            .conn()
            .execute(
                "UPDATE knowledge_snapshots SET updated_at = ?1 WHERE id = ?2",
                ("2025-01-01T00:00:00Z", snapshot.id.as_str()),
            )
            .expect("snapshot should backdate");

        let report_store =
            ReportStore::new(Database::new(&state.db_path).expect("database should open"));
        report_store
            .create_report(CreateReportInput {
                title: "Fresh findings",
                session_id: session_id.as_str(),
                project_dir: Some(project_dir.to_string_lossy().as_ref()),
                report_root: None,
                content: "Fresh findings",
                summary: "Fresh findings",
                tags: &[],
                sources: &[],
                scope: ReportScope::owner_shared(),
            })
            .expect("report should persist");

        refresh_snapshot_after_run(
            &state.db_path,
            Some(project_dir.to_string_lossy().as_ref()),
            Some("alice"),
            Some(&LoopStopReason::Completed),
        );

        let refreshed = get_current_snapshot(
            &state.db_path,
            Some(project_dir.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("snapshot should load")
        .expect("snapshot should still exist");

        assert_ne!(refreshed.updated_at, "2025-01-01T00:00:00Z");
        assert!(refreshed.content.contains("Fresh findings"));
    }
}
