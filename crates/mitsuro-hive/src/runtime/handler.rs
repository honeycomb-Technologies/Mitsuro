use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use mitsuro_core::agent::{
    confirm_worker_introduction_in_transaction,
    return_worker_introduction_to_context_in_transaction, ConfirmWorkerIntroductionRequest,
    ReturnWorkerIntroductionToContextRequest,
};
use mitsuro_core::ai::models::ModelKey as CoreModelKey;
use mitsuro_core::hive::{
    canonical_timestamp, parse_timezone, parse_utc_timestamp, DstPolicy, MisfireConfig,
    RecurrenceV1, RetryPolicy,
};
use mitsuro_core::storage::{
    accept_worker_conversation_input_in_transaction,
    acknowledge_worker_conversation_governor_recovery_in_transaction,
    acknowledge_worker_conversation_response_loss_in_transaction, display_name_from_slug,
    grant_worker_governor_recovery_in_transaction, hash_request_bytes, hive_groups,
    is_valid_crew_slug, load_worker_with_conn, materialize_oldest_staged_input_in_transaction,
    materialize_oldest_staged_input_with_authority_in_transaction,
    reconcile_worker_introduction_review_in_transaction,
    reconcile_worker_workflow_provider_boundary_in_transaction,
    refresh_worker_governor_recovery_run_binding_in_transaction,
    resolve_worker_conversation_with_conn, resolve_worker_for_crew_slug_with_conn,
    worker_governor_response_loss_recovery_required_in_transaction,
    worker_has_unacknowledged_unresolved_provider_calls_in_transaction,
    AcceptWorkerConversationInput, AcceptWorkerConversationInputResult,
    GrantWorkerGovernorRecoveryError, HiveGroupStatus, HiveRunExecutionContextV1,
    HiveRunExecutionModeV1, HiveWorkerConversationBinding, HiveWorkerIntroductionStatus,
    HiveWorkerIntroductionStore, HiveWorkerStatus, OverlapPolicy, SqliteWorkerGoalAcceptanceStore,
    WorkerConversationGovernorRecovery, WorkerConversationLane,
    WorkerConversationPredecessorAuthority, WorkerGoalAcceptanceStoreError,
    WorkerGovernorRecoveryRunBinding, WorkerIntroductionDecisionKind, WorkerIntroductionProposalV1,
    WorkerIntroductionReviewRecovery, WorkerIntroductionSelectedFactV1, WorkerRunOrigin,
    WorkerWorkflowProviderRecovery, WorkspaceMode, DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECS,
    MAX_HIVE_PROFILE_DOCUMENT_BYTES, MAX_WORKER_INTRODUCTION_FACTS,
    WORKER_CONVERSATION_STOP_REQUESTED_REASON,
};
use mitsuro_core::workflow::{
    activate_or_resume_worker_workflow_in_transaction,
    archive_worker_goal_acceptances_in_transaction, cancel_worker_workflow_in_transaction,
    pause_worker_workflow_in_transaction, UserGoalCriterionAcceptance, UserGoalCriterionDecision,
    UserWorkerGoalAcceptanceDecision, UserWorkerGoalAcceptanceRequest,
    WorkerWorkflowActivationDisposition as CoreWorkerWorkflowActivationDisposition,
    WorkerWorkflowActivationRequest, WorkerWorkflowActivationSource,
    WorkerWorkflowLifecycleRequest, WorkflowError,
};
use mitsuro_core::Content;
use mitsuro_hive_protocol::{
    unix_time_millis, AckResponse, ActivateOrResumeWorkerWorkflowCommand, Actor, Command,
    ConfirmWorkerIntroductionCommand, CreateScheduleCommand, CreateWorkerIntroductionCommand,
    DaemonRuntimeStats, DispatchCommand, DispatchResponse, EventEnvelope, ExtensionResponse,
    GrantWorkerGovernorRecoveryCommand, HiveEvent, LaggedEvent, ModelKey, ProtocolErrorPayload,
    ProtocolVersion, RecoverResponse, ReplaceScheduleCommand, ReplayGapEvent,
    ResolveWorkerGoalAcceptanceCommand, ResponsePayload, ReturnWorkerIntroductionToContextCommand,
    ScheduleDefinition, ScheduleResponse, SessionResponse, SetScheduleStatusCommand,
    SetWorkerStatusCommand, SetWorkerWorkspaceCommand, SubscribeCommand, SubscriptionAccepted,
    UpdateWorkerCommand, WorkerConversationInputDisposition, WorkerConversationInputResponse,
    WorkerGoalAcceptanceDecision, WorkerGoalAcceptanceResponse, WorkerGoalCriterionDecision,
    WorkerGovernorRecoveryResponse, WorkerIntroductionActionResponse, WorkerIntroductionCommand,
    WorkerIntroductionResponse, WorkerIntroductionReturnDecision, WorkerLaneAttention,
    WorkerMutationResponse, WorkerRunCancellation, WorkerTargetStatus, WorkerWorkflowDisposition,
    WorkerWorkflowLifecycleCommand, WorkerWorkflowResponse, WorkerWorkflowRunCancellation,
    WorkerWorkflowRunProjection, WorkerWorkspaceMode, WorkerWorkspaceResponse,
};
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, watch, Mutex, Notify};
use tokio::task::JoinHandle;

use crate::{CommandContext, CommandHandler, HandlerReply, HandlerResult};

use super::backend::{ExecutionBackend, ExecutionControl};
use super::config::HiveRuntimeConfig;
use super::events::EventHub;
use super::persistence::{
    append_event, get_or_create_controller, request_hash, require_owned_session,
    unix_millis_to_utc, ControllerRecord, Mutation, MutationOutcome, PersistedEvent,
    RuntimePersistence, RuntimeStoreError,
};
use super::pump;

pub(crate) const DAEMON_LEASE_NAME: &str = "hive-scheduler";
const WORKER_INTRODUCTION_MAX_CANONICAL_CONTENT_BYTES: usize = 48 * 1024;
pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 100;
pub(crate) const MAX_RETRY_DELAY_SECS: u64 = 7 * 24 * 60 * 60;

const PUMP_STARTING: u8 = 0;
const PUMP_RUNNING: u8 = 1;
const PUMP_STOPPED: u8 = 2;
const CANCELLATION_SIGNAL_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
pub(crate) enum CommittedCancellationKind {
    Session,
    WorkerIntroduction {
        run_id: String,
    },
    WorkerRun {
        worker_id: String,
        run_id: String,
    },
    WorkerWorkflow {
        worker_id: String,
        goal_id: String,
        run_id: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct CommittedCancellation {
    pub(crate) session_id: String,
    pub(crate) kind: CommittedCancellationKind,
}

#[derive(Debug)]
pub(crate) struct RuntimeHealth {
    pump_state: AtomicU8,
    scheduler_activated: AtomicBool,
    pump_stopped: Notify,
}

impl RuntimeHealth {
    fn new() -> Self {
        Self {
            pump_state: AtomicU8::new(PUMP_STARTING),
            scheduler_activated: AtomicBool::new(false),
            pump_stopped: Notify::new(),
        }
    }

    pub(crate) fn mark_pump_running(&self) {
        self.pump_state.store(PUMP_RUNNING, Ordering::Release);
    }

    pub(crate) fn mark_pump_stopped(&self) {
        self.scheduler_activated.store(false, Ordering::Release);
        self.pump_state.store(PUMP_STOPPED, Ordering::Release);
        self.pump_stopped.notify_waiters();
    }

    pub(crate) fn set_scheduler_activated(&self, activated: bool) {
        self.scheduler_activated.store(activated, Ordering::Release);
    }

    fn pump_alive(&self) -> bool {
        self.pump_state.load(Ordering::Acquire) == PUMP_RUNNING
    }

    fn scheduler_activated(&self) -> bool {
        self.scheduler_activated.load(Ordering::Acquire)
    }

    async fn wait_for_pump_stop(&self) {
        loop {
            let stopped = self.pump_stopped.notified();
            if self.pump_state.load(Ordering::Acquire) == PUMP_STOPPED {
                return;
            }
            stopped.await;
        }
    }
}

pub(crate) struct RuntimeShared {
    pub(crate) config: HiveRuntimeConfig,
    pub(crate) instance_id: String,
    pub(crate) persistence: RuntimePersistence,
    pub(crate) backend: Arc<dyn ExecutionBackend>,
    pub(crate) events: EventHub,
    pub(crate) cancellation_tx: broadcast::Sender<CommittedCancellation>,
    pub(crate) mutation_gate: Mutex<()>,
    pub(crate) control_gate: Mutex<()>,
    pub(crate) health: RuntimeHealth,
}

pub struct DurableHiveCommandHandler {
    pub(crate) shared: Arc<RuntimeShared>,
}

pub struct HiveRuntimeHandle {
    handler: Arc<DurableHiveCommandHandler>,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl HiveRuntimeHandle {
    pub fn handler(&self) -> Arc<DurableHiveCommandHandler> {
        Arc::clone(&self.handler)
    }

    pub async fn shutdown(mut self) {
        self.shutdown_tx.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }

    /// Wait for an unrequested scheduler-pump exit. The task is not removed
    /// until its liveness guard has reported a stop, so cancelling this future
    /// (for example because the IPC server shut down first) leaves graceful
    /// shutdown ownership intact.
    pub async fn wait_for_scheduler_failure(&mut self) -> anyhow::Error {
        self.handler.shared.health.wait_for_pump_stop().await;
        match self.task.take() {
            Some(task) => match task.await {
                Ok(()) => anyhow::anyhow!("Hive scheduler pump exited unexpectedly"),
                Err(error) if error.is_panic() => {
                    anyhow::anyhow!("Hive scheduler pump panicked: {error}")
                }
                Err(error) => anyhow::anyhow!("Hive scheduler pump stopped: {error}"),
            },
            None => anyhow::anyhow!("Hive scheduler pump stopped without a task handle"),
        }
    }

    #[cfg(test)]
    pub(crate) fn force_pump_exit_for_test(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Drop for HiveRuntimeHandle {
    fn drop(&mut self) {
        self.shutdown_tx.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub async fn start_runtime(
    config: HiveRuntimeConfig,
    daemon_instance_id: impl Into<String>,
    backend: Arc<dyn ExecutionBackend>,
) -> anyhow::Result<HiveRuntimeHandle> {
    config.validate()?;
    let persistence = RuntimePersistence::new(config.database_path.clone(), config.idempotency_ttl);
    persistence
        .initialize()
        .await
        .map_err(|error| anyhow::anyhow!(error.protocol().message))?;
    let instance_label = daemon_instance_id.into();
    let (cancellation_tx, _) = broadcast::channel(CANCELLATION_SIGNAL_CAPACITY);
    let shared = Arc::new(RuntimeShared {
        instance_id: format!("{instance_label}:boot:{}", uuid::Uuid::new_v4()),
        events: EventHub::new(config.live_event_capacity),
        persistence,
        backend,
        mutation_gate: Mutex::new(()),
        control_gate: Mutex::new(()),
        cancellation_tx,
        health: RuntimeHealth::new(),
        config,
    });
    let handler = Arc::new(DurableHiveCommandHandler {
        shared: Arc::clone(&shared),
    });
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(pump::run(shared, shutdown_rx));
    Ok(HiveRuntimeHandle {
        handler,
        shutdown_tx,
        task: Some(task),
    })
}

#[async_trait]
impl CommandHandler for DurableHiveCommandHandler {
    async fn handle(&self, context: CommandContext, command: Command) -> HandlerResult {
        if let Command::Subscribe(subscribe) = command {
            return self.subscribe(context.actor, subscribe).await;
        }
        if matches!(
            command,
            Command::Ping | Command::Stats | Command::Shutdown(_)
        ) {
            return Err(ProtocolErrorPayload::new(
                "invalid_routing",
                "foundation command reached the runtime handler",
                false,
            ));
        }

        let operation = command.name();
        let hash = request_hash(&context.actor, &command);
        let actor = context.actor.clone();
        let idempotency_key = context.idempotency_key.clone();
        let mut control = control_after_commit(&command, &actor, &idempotency_key);
        let committed_cancellation = match &command {
            Command::CancelSession(command) => Some(CommittedCancellation {
                session_id: command.session_id.clone(),
                kind: CommittedCancellationKind::Session,
            }),
            _ => None,
        };
        let _gate = self.shared.mutation_gate.lock().await;
        let mut outcome = self
            .mutate(actor, idempotency_key, operation, hash, command)
            .await
            .map_err(RuntimeStoreError::protocol)?;
        outcome.events.sort_by_key(|event| event.sequence);
        let introduction_cancellation = match &outcome.response {
            ResponsePayload::WorkerIntroductionAction(response)
                if response.cancellation_requested =>
            {
                response
                    .run_id
                    .as_ref()
                    .map(|run_id| CommittedCancellation {
                        session_id: response.session_id.clone(),
                        kind: CommittedCancellationKind::WorkerIntroduction {
                            run_id: run_id.clone(),
                        },
                    })
            }
            _ => None,
        };
        let worker_cancellations = match &outcome.response {
            ResponsePayload::WorkerMutation(response) => response
                .cancellation_requests
                .iter()
                .map(|request| CommittedCancellation {
                    session_id: request.session_id.clone(),
                    kind: CommittedCancellationKind::WorkerRun {
                        worker_id: request.worker_id.clone(),
                        run_id: request.run_id.clone(),
                    },
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };
        let worker_workflow_cancellations = match &outcome.response {
            ResponsePayload::WorkerWorkflow(response) => response
                .cancellation_requests
                .iter()
                .map(|request| CommittedCancellation {
                    session_id: request.session_id.clone(),
                    kind: CommittedCancellationKind::WorkerWorkflow {
                        worker_id: request.worker_id.clone(),
                        goal_id: request.goal_id.clone(),
                        run_id: request.run_id.clone(),
                    },
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };
        if matches!(
            &outcome.response,
            ResponsePayload::WorkerConversationInput(_)
        ) {
            // A direct Worker message is either already canonical behind its
            // own serialized response run or durably staged behind the exact
            // unfinished run. It must never be duplicated through the live
            // pending-user steering channel.
            control = None;
        }
        for event in &outcome.events {
            self.shared.events.publish(event.envelope());
        }
        // Reserve control-delivery order before releasing mutation order. A
        // later Resume/Start must not reach the backend before an earlier
        // Pause/Cancel, and steering must follow durable commit order.
        let control_guard = if !outcome.replayed && control.is_some() {
            Some(self.shared.control_gate.lock().await)
        } else {
            None
        };
        drop(_gate);

        // Wake every scheduler-owned execution only after the exact-owner
        // cancellation transaction has committed. Exact-run cancellation is
        // deliberately re-emitted when a receipt is replayed: the process may
        // have crashed after committing the mutation but before broadcasting
        // the first signal, and CancelRun delivery is idempotent. Durable
        // run/controller state remains the authority for every receiver.
        if let Some(cancellation) = committed_cancellation {
            let _ = self.shared.cancellation_tx.send(cancellation);
        }
        if let Some(cancellation) = introduction_cancellation {
            let _ = self.shared.cancellation_tx.send(cancellation);
        }
        for cancellation in worker_cancellations {
            let _ = self.shared.cancellation_tx.send(cancellation);
        }
        for cancellation in worker_workflow_cancellations {
            let _ = self.shared.cancellation_tx.send(cancellation);
        }
        if !outcome.replayed {
            if let Some((session_id, control)) = control {
                if let Err(error) = self.shared.backend.control(&session_id, control).await {
                    tracing::warn!(session_id, error = %error, "Hive backend control delivery failed after durable acceptance");
                }
            }
        }
        drop(control_guard);
        Ok(HandlerReply::Response(Box::new(outcome.response)))
    }

    async fn runtime_stats(&self, actor: &Actor) -> DaemonRuntimeStats {
        let pump_alive = self.shared.health.pump_alive();
        let mut stats = match self.shared.persistence.stats(actor).await {
            Ok(stats) => stats,
            Err(error) => {
                let failure = error.protocol();
                tracing::warn!(
                    error_code = %failure.code,
                    "Hive runtime stats are unavailable; reporting scheduler not ready"
                );
                return DaemonRuntimeStats {
                    pump_alive,
                    ..DaemonRuntimeStats::default()
                };
            }
        };
        let scheduler_ready = if pump_alive && self.shared.health.scheduler_activated() {
            self.shared
                .persistence
                .daemon_lease_is_current(DAEMON_LEASE_NAME, &self.shared.instance_id)
                .await
                .unwrap_or(false)
        } else {
            false
        };
        stats.pump_alive = pump_alive;
        stats.scheduler_ready = scheduler_ready;
        stats
    }
}

impl DurableHiveCommandHandler {
    async fn mutate(
        &self,
        actor: Actor,
        idempotency_key: String,
        operation: &'static str,
        hash: String,
        command: Command,
    ) -> Result<MutationOutcome, RuntimeStoreError> {
        let command = match command {
            Command::ResolveWorkerGoalAcceptance(command) => {
                let database_path = self.shared.config.database_path.clone();
                return self
                    .shared
                    .persistence
                    .mutate_external_idempotent(
                        actor,
                        idempotency_key,
                        operation,
                        hash,
                        move |actor| resolve_worker_goal_acceptance(&database_path, actor, command),
                    )
                    .await;
            }
            command => command,
        };
        let mutation_idempotency_key = idempotency_key.clone();
        self.shared
            .persistence
            .mutate(
                actor,
                idempotency_key,
                operation,
                hash,
                move |tx, actor, now| match command {
                    Command::Dispatch(command) => dispatch(tx, actor, now, command),
                    Command::CreateSchedule(command) => {
                        create_recurring_schedule(tx, actor, now, command)
                    }
                    Command::ReplaceSchedule(command) => {
                        replace_recurring_schedule(tx, actor, now, command)
                    }
                    Command::SetScheduleStatus(command) => {
                        set_schedule_status(tx, actor, now, command)
                    }
                    Command::StartSession(command) => {
                        start_session(tx, actor, now, &command.session_id)
                    }
                    Command::ScheduleSession(command) => schedule_session(
                        tx,
                        actor,
                        now,
                        &command.session_id,
                        command.wake_at_unix_ms,
                        &command.reason,
                    ),
                    Command::PauseSession(command) => {
                        set_controller_status(tx, actor, now, &command.session_id, "paused")
                    }
                    Command::ResumeSession(command) => {
                        set_controller_status(tx, actor, now, &command.session_id, "active")
                    }
                    Command::CancelSession(command) => {
                        cancel_session(tx, actor, now, &command.session_id)
                    }
                    Command::StopWorkerConversation(command) => {
                        stop_worker_conversation(tx, actor, now, &command.session_id)
                    }
                    Command::DeleteSession(command) => {
                        delete_session(tx, actor, now, &command.session_id)
                    }
                    Command::SendMessage(command) => {
                        reject_legacy_worker_conversation_input(tx, actor, &command.session_id)?;
                        let pending_id = pending_message_id(
                            actor,
                            &command.session_id,
                            &mutation_idempotency_key,
                        );
                        send_message(
                            tx,
                            actor,
                            now,
                            &command.session_id,
                            &command.message,
                            &pending_id,
                        )
                    }
                    Command::WorkerSendMessage(command) => {
                        require_typed_worker_conversation_input(tx, actor, &command.session_id)?;
                        let pending_id = pending_message_id(
                            actor,
                            &command.session_id,
                            &mutation_idempotency_key,
                        );
                        send_message(
                            tx,
                            actor,
                            now,
                            &command.session_id,
                            &command.message,
                            &pending_id,
                        )
                    }
                    Command::GroupMessage(command) => super::groups::group_message(
                        tx,
                        actor,
                        now,
                        command,
                        &mutation_idempotency_key,
                        WorkerRunOrigin::UserGroup,
                    ),
                    Command::GroupStop(command) => {
                        super::groups::group_stop(tx, actor, now, &command.group_id)
                    }
                    Command::GroupArchive(command) => {
                        super::groups::group_archive(tx, actor, now, &command.group_id)
                    }
                    Command::CreateWorkerIntroduction(command) => {
                        create_worker_introduction(tx, actor, now, command)
                    }
                    Command::RetryWorkerIntroduction(command) => {
                        retry_worker_introduction(tx, actor, now, command)
                    }
                    Command::SkipWorkerIntroduction(command) => {
                        skip_worker_introduction(tx, actor, now, command)
                    }
                    Command::ConfirmWorkerIntroduction(command) => {
                        confirm_reviewed_worker_introduction(tx, actor, now, command)
                    }
                    Command::ReturnWorkerIntroductionToContext(command) => {
                        return_reviewed_worker_introduction_to_context(tx, actor, now, command)
                    }
                    Command::UpdateWorker(command) => update_worker(tx, actor, now, command),
                    Command::SetWorkerStatus(command) => set_worker_status(tx, actor, now, command),
                    Command::GrantWorkerGovernorRecovery(command) => {
                        grant_worker_governor_recovery(
                            tx,
                            actor,
                            now,
                            &mutation_idempotency_key,
                            command,
                        )
                    }
                    Command::ActivateOrResumeWorkerWorkflow(command) => {
                        activate_or_resume_worker_workflow(
                            tx,
                            actor,
                            now,
                            &mutation_idempotency_key,
                            command,
                        )
                    }
                    Command::PauseWorkerWorkflow(command) => {
                        pause_worker_workflow(tx, actor, now, &mutation_idempotency_key, command)
                    }
                    Command::CancelWorkerWorkflow(command) => {
                        cancel_worker_workflow(tx, actor, now, &mutation_idempotency_key, command)
                    }
                    Command::SetWorkerWorkspace(command) => {
                        set_worker_workspace(tx, actor, now, command)
                    }
                    Command::Steer(command) => {
                        reject_legacy_worker_conversation_input(tx, actor, &command.session_id)?;
                        let pending_id = steer_pending_id(
                            actor,
                            &command.session_id,
                            &mutation_idempotency_key,
                            command.pending_id.as_deref(),
                        );
                        stage_steer(
                            tx,
                            actor,
                            now,
                            &command.session_id,
                            &pending_id,
                            command.content,
                        )
                    }
                    Command::WorkerSteer(command) => {
                        require_typed_worker_conversation_input(tx, actor, &command.session_id)?;
                        let pending_id = steer_pending_id(
                            actor,
                            &command.session_id,
                            &mutation_idempotency_key,
                            command.pending_id.as_deref(),
                        );
                        stage_steer(
                            tx,
                            actor,
                            now,
                            &command.session_id,
                            &pending_id,
                            command.content,
                        )
                    }
                    Command::ToolApproval(command) => stage_tool_approval(
                        tx,
                        actor,
                        now,
                        &command.session_id,
                        &command.run_id,
                        &command.tool_call_id,
                        command.approved,
                    ),
                    Command::UserResponse(command) => {
                        reject_legacy_worker_conversation_input(tx, actor, &command.session_id)?;
                        let pending_id = pending_message_id(
                            actor,
                            &command.session_id,
                            &mutation_idempotency_key,
                        );
                        user_response(
                            tx,
                            actor,
                            now,
                            &command.session_id,
                            &command.run_id,
                            &command.tool_call_id,
                            &command.response,
                            &pending_id,
                        )
                    }
                    Command::WorkerUserResponse(command) => {
                        require_typed_worker_conversation_input(tx, actor, &command.session_id)?;
                        let pending_id = pending_message_id(
                            actor,
                            &command.session_id,
                            &mutation_idempotency_key,
                        );
                        user_response(
                            tx,
                            actor,
                            now,
                            &command.session_id,
                            &command.run_id,
                            &command.tool_call_id,
                            &command.response,
                            &pending_id,
                        )
                    }
                    Command::SetPriority(command) => {
                        set_priority(tx, actor, now, &command.session_id, &command.priority)
                    }
                    Command::SetCrew(command) => set_crew(
                        tx,
                        actor,
                        now,
                        &command.session_id,
                        command.crew_slug.as_deref(),
                    ),
                    Command::Recover(command) => {
                        recover(tx, actor, now, command.session_id.as_deref())
                    }
                    Command::Extension(command) => extension(tx, actor, now, command),
                    Command::Ping
                    | Command::Stats
                    | Command::Shutdown(_)
                    | Command::Subscribe(_)
                    | Command::ResolveWorkerGoalAcceptance(_) => Err(RuntimeStoreError::Invalid(
                        "command is not a mutable runtime operation".into(),
                    )),
                },
            )
            .await
    }

    async fn subscribe(&self, actor: Actor, command: SubscribeCommand) -> HandlerResult {
        let mut live = self.shared.events.subscribe(&command.session_id);
        let requested_after = command.after_sequence.unwrap_or(0).max(0);
        let live_only = command.replay_limit == Some(0);
        let limit = if live_only {
            0
        } else {
            command
                .replay_limit
                .unwrap_or(self.shared.config.replay_limit)
                .clamp(1, self.shared.config.replay_limit)
        };
        let catchup_limit = self.shared.config.replay_limit;
        let snapshot = self
            .shared
            .persistence
            .replay(
                actor.clone(),
                command.session_id.clone(),
                requested_after,
                limit,
            )
            .await
            .map_err(RuntimeStoreError::protocol)?;
        let accepted = SubscriptionAccepted {
            session_id: command.session_id.clone(),
            high_water_sequence: snapshot.high_water,
        };
        let (sender, receiver) = mpsc::channel(self.shared.config.subscriber_capacity);
        let persistence = self.shared.persistence.clone();
        let session_id = command.session_id;
        tokio::spawn(async move {
            if let Some(earliest) = (!live_only)
                .then_some(snapshot.earliest_returned.or(snapshot.earliest_available))
                .flatten()
                .filter(|earliest| *earliest > snapshot.requested_after.saturating_add(1))
            {
                if sender
                    .send(replay_gap_event(
                        &session_id,
                        snapshot.requested_after,
                        earliest,
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let mut previous_replay_sequence = None::<i64>;
            for event in snapshot.events {
                if previous_replay_sequence
                    .is_some_and(|previous| event.sequence > previous.saturating_add(1))
                {
                    let previous = previous_replay_sequence.unwrap_or(snapshot.requested_after);
                    if sender
                        .send(replay_gap_event(&session_id, previous, event.sequence))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                previous_replay_sequence = Some(event.sequence);
                if sender.send(event.envelope()).await.is_err() {
                    return;
                }
            }
            let mut last_sequence = snapshot.high_water.unwrap_or(requested_after);
            loop {
                let received = tokio::select! {
                    _ = sender.closed() => return,
                    received = live.recv() => received,
                };
                match received {
                    Ok(event) => {
                        let Some(sequence) = event.sequence else {
                            if sender.send(event).await.is_err() {
                                return;
                            }
                            continue;
                        };
                        if sequence <= last_sequence {
                            continue;
                        }
                        if sequence > last_sequence.saturating_add(1) {
                            let skipped = sequence.saturating_sub(last_sequence + 1) as u64;
                            if sender
                                .send(lagged_event(&session_id, skipped, Some(last_sequence)))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        last_sequence = sequence;
                        if sender.send(event).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        if sender
                            .send(lagged_event(&session_id, skipped, Some(last_sequence)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        match persistence
                            .replay(
                                actor.clone(),
                                session_id.clone(),
                                last_sequence,
                                catchup_limit,
                            )
                            .await
                        {
                            Ok(catchup) => {
                                for event in catchup.events {
                                    if event.sequence > last_sequence.saturating_add(1)
                                        && sender
                                            .send(replay_gap_event(
                                                &session_id,
                                                last_sequence,
                                                event.sequence,
                                            ))
                                            .await
                                            .is_err()
                                    {
                                        return;
                                    }
                                    last_sequence = last_sequence.max(event.sequence);
                                    if sender.send(event.envelope()).await.is_err() {
                                        return;
                                    }
                                }
                                last_sequence = catchup.high_water.unwrap_or(last_sequence);
                            }
                            Err(error) => {
                                tracing::warn!(error = ?error, "Hive subscription catch-up failed");
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Ok(HandlerReply::Subscription {
            accepted,
            events: receiver,
        })
    }
}

fn grant_worker_governor_recovery(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    operation_id: &str,
    command: GrantWorkerGovernorRecoveryCommand,
) -> Result<Mutation, RuntimeStoreError> {
    if command.worker_id.trim().is_empty() || command.worker_id.len() > 256 {
        return Err(RuntimeStoreError::Invalid(
            "Worker recovery requires a valid Worker id".into(),
        ));
    }
    let status = tx
        .query_row(
            "SELECT status FROM hive_workers
             WHERE id = ?1 AND user_id IS ?2",
            params![command.worker_id, actor.user_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(status) = status else {
        return Err(RuntimeStoreError::NotFound(
            "Hive Worker was not found".into(),
        ));
    };
    if status != "active" {
        return Err(RuntimeStoreError::StateConflict(
            "only an active Hive Worker can receive recovery authority".into(),
        ));
    }
    let now = parse_utc_timestamp(now).map_err(|error| {
        RuntimeStoreError::Internal(anyhow::anyhow!(error).context("parsing mutation time"))
    })?;
    let now_text = canonical_timestamp(now);
    if worker_governor_response_loss_recovery_required_in_transaction(
        tx,
        &command.worker_id,
        actor.user_id.as_deref(),
    )
    .map_err(RuntimeStoreError::Internal)?
    {
        let response_loss_grant =
            if worker_has_unacknowledged_unresolved_provider_calls_in_transaction(
                tx,
                &command.worker_id,
                &now_text,
            )
            .map_err(RuntimeStoreError::Internal)?
            {
                Some(
                    grant_worker_governor_recovery_in_transaction(
                        tx,
                        &command.worker_id,
                        actor.user_id.as_deref(),
                        operation_id,
                        now,
                    )
                    .map_err(map_governor_recovery_grant_error)?
                    .0,
                )
            } else {
                None
            };
        return match acknowledge_worker_conversation_response_loss_in_transaction(
            tx,
            &command.worker_id,
            actor.user_id.as_deref(),
            response_loss_grant.as_ref().map(|grant| grant.id.as_str()),
            &now_text,
        )
        .map_err(RuntimeStoreError::Internal)?
        {
            WorkerConversationGovernorRecovery::Recovered {
                predecessor_run_id, ..
            } => Ok(Mutation {
                response: ResponsePayload::WorkerGovernorRecovery(WorkerGovernorRecoveryResponse {
                    worker_id: command.worker_id,
                    grant_id: response_loss_grant.as_ref().map(|grant| grant.id.clone()),
                    expires_at: response_loss_grant
                        .as_ref()
                        .map(|grant| grant.expires_at.clone()),
                    status: if response_loss_grant.is_some() {
                        "response_loss_acknowledged_with_grant".into()
                    } else {
                        "response_loss_acknowledged".into()
                    },
                    bypass_unresolved_provider_call: response_loss_grant.is_some(),
                }),
                resource_id: Some(
                    response_loss_grant
                        .map(|grant| grant.id)
                        .unwrap_or(predecessor_run_id),
                ),
                events: Vec::new(),
            }),
            WorkerConversationGovernorRecovery::NoBoundary => {
                Err(RuntimeStoreError::StateConflict(
                    "Worker response-loss recovery boundary is no longer current".into(),
                ))
            }
            WorkerConversationGovernorRecovery::UnsupportedBoundary { run_id, kind } => {
                Err(RuntimeStoreError::StateConflict(format!(
                    "Worker recovery boundary {run_id} ({kind}) requires its typed lifecycle action"
                )))
            }
        };
    }
    let (grant, created) = grant_worker_governor_recovery_in_transaction(
        tx,
        &command.worker_id,
        actor.user_id.as_deref(),
        operation_id,
        now,
    )
    .map_err(map_governor_recovery_grant_error)?;
    let run_binding = refresh_worker_governor_recovery_run_binding_in_transaction(
        tx,
        &command.worker_id,
        actor.user_id.as_deref(),
        &grant.id,
        &now_text,
    )
    .map_err(RuntimeStoreError::Internal)?;
    match run_binding {
        WorkerGovernorRecoveryRunBinding::Unbound => {
            match acknowledge_worker_conversation_governor_recovery_in_transaction(
                tx,
                &command.worker_id,
                actor.user_id.as_deref(),
                &grant.id,
                &now_text,
            )
            .map_err(RuntimeStoreError::Internal)?
            {
                WorkerConversationGovernorRecovery::NoBoundary
                | WorkerConversationGovernorRecovery::Recovered { .. } => {}
                WorkerConversationGovernorRecovery::UnsupportedBoundary { run_id, kind } => {
                    return Err(RuntimeStoreError::StateConflict(format!(
                        "Worker recovery boundary {run_id} ({kind}) requires its typed lifecycle action"
                    )));
                }
            }
        }
        WorkerGovernorRecoveryRunBinding::BlockedInFlight { run_id } => {
            return Err(RuntimeStoreError::StateConflict(format!(
                "Worker recovery run {run_id} is already in flight; retry after it returns to a recoverable state"
            )));
        }
        WorkerGovernorRecoveryRunBinding::Bound { .. }
        | WorkerGovernorRecoveryRunBinding::Rebound { .. } => {}
    }
    Ok(Mutation {
        response: ResponsePayload::WorkerGovernorRecovery(WorkerGovernorRecoveryResponse {
            worker_id: grant.worker_id.clone(),
            grant_id: Some(grant.id.clone()),
            expires_at: Some(grant.expires_at),
            status: if created {
                "granted".into()
            } else {
                "already_available".into()
            },
            bypass_unresolved_provider_call: grant.bypass_unresolved_provider_call,
        }),
        resource_id: Some(grant.id),
        events: Vec::new(),
    })
}

fn map_governor_recovery_grant_error(error: GrantWorkerGovernorRecoveryError) -> RuntimeStoreError {
    match error {
        GrantWorkerGovernorRecoveryError::WorkerNotFound
        | GrantWorkerGovernorRecoveryError::OwnerMismatch => {
            RuntimeStoreError::NotFound("Hive Worker was not found".into())
        }
        GrantWorkerGovernorRecoveryError::WorkerInactive
        | GrantWorkerGovernorRecoveryError::NoEligibleUnresolved
        | GrantWorkerGovernorRecoveryError::UnsupportedBoundary => {
            RuntimeStoreError::StateConflict(error.to_string())
        }
        GrantWorkerGovernorRecoveryError::Internal(error) => RuntimeStoreError::Internal(error),
    }
}

fn dispatch(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: DispatchCommand,
) -> Result<Mutation, RuntimeStoreError> {
    if command.task.trim().is_empty() || command.working_dir.trim().is_empty() {
        return Err(RuntimeStoreError::Invalid(
            "dispatch requires a task and working directory".into(),
        ));
    }
    let (model, model_key, model_catalog_revision) = normalize_model_identity(
        command.model.clone(),
        command.model_key.clone(),
        command.model_catalog_revision.clone(),
        "dispatch",
    )?;
    let model = model.ok_or_else(|| {
        RuntimeStoreError::Invalid("dispatch requires a frozen model identity".into())
    })?;
    let model_key_json = model_key
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    if command.task.len() > 64 * 1024 {
        return Err(RuntimeStoreError::Invalid(
            "dispatch task exceeds 65536 bytes".into(),
        ));
    }
    if command.working_dir.len() > 4096 || command.working_dir.as_bytes().contains(&0) {
        return Err(RuntimeStoreError::Invalid(
            "dispatch working directory is invalid or too long".into(),
        ));
    }
    if !Path::new(&command.working_dir).is_absolute() {
        return Err(RuntimeStoreError::Invalid(
            "dispatch working directory must be absolute".into(),
        ));
    }
    if command
        .project_dir
        .as_deref()
        .is_some_and(|path| path.len() > 4096 || path.as_bytes().contains(&0))
    {
        return Err(RuntimeStoreError::Invalid(
            "dispatch project directory is invalid or too long".into(),
        ));
    }
    if command
        .project_dir
        .as_deref()
        .is_some_and(|path| !Path::new(path).is_absolute())
    {
        return Err(RuntimeStoreError::Invalid(
            "dispatch project directory must be absolute".into(),
        ));
    }
    if command
        .crew_slug
        .as_deref()
        .is_some_and(|slug| !is_valid_crew_slug(slug))
    {
        return Err(RuntimeStoreError::Invalid("crew slug is invalid".into()));
    }
    if let Some(user_id) = actor.user_id.as_deref() {
        let exists = tx
            .query_row("SELECT 1 FROM users WHERE id = ?1", [user_id], |_| Ok(()))
            .optional()?
            .is_some();
        if !exists {
            return Err(RuntimeStoreError::Ownership);
        }
    }
    let scheduled_for = command
        .start_at_unix_ms
        .map(unix_millis_to_utc)
        .transpose()?
        .unwrap_or_else(Utc::now);
    let session_id = uuid::Uuid::new_v4().to_string();
    let controller_id = uuid::Uuid::new_v4().to_string();
    let run_id = uuid::Uuid::new_v4().to_string();
    let project_dir = command
        .project_dir
        .clone()
        .unwrap_or_else(|| command.working_dir.clone());
    tx.execute(
        "INSERT INTO sessions (
            id, title, created_at, updated_at, model, model_key_json,
            model_catalog_revision, working_dir, project_dir, workspace_mode,
            session_type, user_id, permission_mode
         ) VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, ?7, ?8, 'selected', 'hive', ?9, 'autonomous')",
        params![
            session_id,
            bounded_title(&command.task),
            now,
            model,
            model_key_json,
            model_catalog_revision,
            command.working_dir,
            project_dir,
            actor.user_id,
        ],
    )?;
    insert_canonical_user_message(tx, &session_id, &command.task, now)?;
    tx.execute(
        "INSERT INTO hive_controllers (
            id, scope_key, user_id, session_id, status, timezone,
            max_concurrent_runs, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, 'active', 'UTC', 1, ?5, ?5)",
        params![
            controller_id,
            format!("session:{session_id}"),
            actor.user_id,
            session_id,
            now
        ],
    )?;
    let controller = ControllerRecord {
        id: controller_id,
        session_id: session_id.clone(),
        status: "active".into(),
        timezone: "UTC".into(),
    };
    let priority = priority_value(command.priority.as_deref().unwrap_or("normal"))?;
    insert_run(
        tx,
        &run_id,
        &controller.id,
        Some(&session_id),
        None,
        None,
        "dispatch",
        &command.task,
        serde_json::json!({
            "working_dir": command.working_dir,
            "project_dir": project_dir,
            "model": model,
            "model_key": model_key,
            "model_catalog_revision": model_catalog_revision,
            "permission_mode": "autonomous",
            "crew_slug": command.crew_slug,
            "retry": RetryPolicy::default(),
        }),
        priority,
        &canonical_timestamp(scheduled_for),
        5,
        now,
        None,
    )?;
    tx.execute(
        "INSERT INTO hive_runtime_state (
            session_id, status, current_run_id, crew_slug, priority, updated_at
         ) VALUES (?1, 'idle', ?2, ?3, ?4, ?5)",
        params![
            session_id,
            run_id,
            command.crew_slug,
            priority_name(priority),
            now
        ],
    )?;
    let event = append_event(
        tx,
        &controller,
        "run_queued",
        Some(&run_id),
        None,
        Some(&format!("run:{run_id}:queued")),
        serde_json::json!({"run_id": run_id, "kind": "dispatch"}),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::Dispatch(DispatchResponse {
            session_id: session_id.clone(),
            status: "queued".into(),
        }),
        resource_id: Some(session_id),
        events: vec![event],
    })
}

fn create_worker_introduction(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: CreateWorkerIntroductionCommand,
) -> Result<Mutation, RuntimeStoreError> {
    let CreateWorkerIntroductionCommand {
        slug,
        display_name,
        avatar_color,
        model,
        model_key,
        model_catalog_revision,
        permission_mode,
        autonomy,
        heartbeat_interval_secs,
        identity,
        soul,
    } = command;

    let slug = slug.trim().to_string();
    if !is_valid_crew_slug(&slug) {
        return Err(RuntimeStoreError::Invalid(
            "invalid Worker slug; use 1-64 lowercase letters, digits, hyphens, or underscores"
                .into(),
        ));
    }

    let display_name = match display_name.trim() {
        "" => display_name_from_slug(&slug),
        value => value.to_string(),
    };
    validate_bounded_field(&display_name, "Worker display name", 256)?;
    let avatar_color = avatar_color
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if avatar_color
        .as_deref()
        .is_some_and(|value| value.len() > 64 || value.as_bytes().contains(&0))
    {
        return Err(RuntimeStoreError::Invalid(
            "Worker avatar color is invalid or exceeds 64 bytes".into(),
        ));
    }

    let (model, model_key, model_catalog_revision) = normalize_model_identity(
        Some(model),
        Some(model_key),
        model_catalog_revision,
        "Worker Introduction",
    )?;
    let model = model.ok_or_else(|| {
        RuntimeStoreError::Invalid(
            "Worker Introduction requires an exact provider and model identity".into(),
        )
    })?;
    let model_key = model_key.ok_or_else(|| {
        RuntimeStoreError::Invalid(
            "Worker Introduction requires an exact provider and model identity".into(),
        )
    })?;
    let model_key_json = serde_json::to_string(&model_key)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;

    let permission_mode = match permission_mode.trim() {
        value @ ("supervised" | "autonomous") => value.to_string(),
        _ => {
            return Err(RuntimeStoreError::Invalid(
                "Worker permission mode must be supervised or autonomous".into(),
            ))
        }
    };
    let autonomy = match autonomy.trim() {
        value @ ("manual" | "scheduled" | "always_on") => value.to_string(),
        _ => {
            return Err(RuntimeStoreError::Invalid(
                "Worker autonomy must be manual, scheduled, or always_on".into(),
            ))
        }
    };
    if heartbeat_interval_secs == Some(0) {
        return Err(RuntimeStoreError::Invalid(
            "Worker heartbeat interval must be positive".into(),
        ));
    }
    let heartbeat_interval_secs = match (autonomy.as_str(), heartbeat_interval_secs) {
        ("always_on", None) => Some(DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECS),
        (_, interval) => interval,
    };
    let identity = validate_introduction_document(identity, "identity")?;
    let soul = validate_introduction_document(soul, "soul")?;

    if let Some(user_id) = actor.user_id.as_deref() {
        let exists = tx
            .query_row("SELECT 1 FROM users WHERE id = ?1", [user_id], |_| Ok(()))
            .optional()?
            .is_some();
        if !exists {
            return Err(RuntimeStoreError::Ownership);
        }
    }
    let slug_conflict = tx
        .query_row(
            "SELECT 1 FROM hive_workers
             WHERE ((?1 IS NULL AND user_id IS NULL) OR user_id = ?1)
               AND slug = ?2 AND status <> 'archived'",
            params![actor.user_id, slug],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if slug_conflict {
        return Err(RuntimeStoreError::StateConflict(format!(
            "A Worker with slug '{slug}' already exists"
        )));
    }

    let worker_id = uuid::Uuid::new_v4().to_string();
    let session_id = uuid::Uuid::new_v4().to_string();
    let controller_id = uuid::Uuid::new_v4().to_string();
    let run_id = uuid::Uuid::new_v4().to_string();

    // The direct conversation is workspace-neutral and begins with no
    // canonical messages. The Introduction executor will append the first
    // assistant row only after the provider completes.
    tx.execute(
        "INSERT INTO sessions (
            id, title, created_at, updated_at, model, model_key_json,
            model_catalog_revision, working_dir, project_dir, workspace_mode,
            session_type, user_id, permission_mode
         ) VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, NULL, NULL, 'neutral',
                   'hive', ?7, ?8)",
        params![
            session_id,
            display_name,
            now,
            model,
            model_key_json,
            model_catalog_revision,
            actor.user_id,
            permission_mode,
        ],
    )?;
    tx.execute(
        "INSERT INTO hive_workers (
            id, user_id, slug, display_name, avatar_color, model,
            model_key_json, model_catalog_revision, permission_mode, autonomy,
            heartbeat_interval_secs, status, dm_session_id,
            memory_namespace_id, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                   'active', ?12, ?3, ?13, ?13)",
        params![
            worker_id,
            actor.user_id,
            slug,
            display_name,
            avatar_color,
            model,
            model_key_json,
            model_catalog_revision,
            permission_mode,
            autonomy,
            heartbeat_interval_secs,
            session_id,
            now,
        ],
    )?;
    for (kind, content) in [("identity", identity), ("soul", soul)] {
        if let Some(content) = content {
            tx.execute(
                "INSERT INTO hive_worker_documents (worker_id, kind, content, updated_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![worker_id, kind, content, now],
            )?;
        }
    }
    tx.execute(
        "INSERT INTO hive_controllers (
            id, scope_key, user_id, session_id, status, timezone,
            max_concurrent_runs, created_at, updated_at, worker_id
         ) VALUES (?1, ?2, ?3, ?4, 'active', 'UTC', 1, ?5, ?5, ?6)",
        params![
            controller_id,
            format!("worker:{worker_id}"),
            actor.user_id,
            session_id,
            now,
            worker_id,
        ],
    )?;
    let controller = ControllerRecord {
        id: controller_id,
        session_id: session_id.clone(),
        status: "active".into(),
        timezone: "UTC".into(),
    };
    let retry_policy = RetryPolicy {
        max_attempts: 1,
        ..RetryPolicy::default()
    };
    let execution_context = HiveRunExecutionContextV1::worker_conversation_neutral(
        worker_id.clone(),
        1,
        WorkerConversationLane::DirectMessage,
    )
    .map_err(RuntimeStoreError::Internal)?;
    insert_run(
        tx,
        &run_id,
        &controller.id,
        Some(&session_id),
        None,
        None,
        "worker_introduction",
        "Begin the one-time Worker Introduction and then wait for the user.",
        serde_json::json!({
            "model": model,
            "model_key": model_key,
            "model_catalog_revision": model_catalog_revision,
            "permission_mode": permission_mode,
            "worker_id": worker_id,
            "introduction": {
                "prompt_version": 1,
                "context_mode": "worker_identity_only",
                "tool_allowlist": [],
                "web_access": false,
                "project_access": false,
                "one_shot": true,
            },
            "retry": retry_policy,
        }),
        priority_value("normal")?,
        now,
        1,
        now,
        Some(WorkerRunBinding {
            worker_id: &worker_id,
            execution_context: &execution_context,
            origin: WorkerRunOrigin::UserLifecycleAction,
        }),
    )?;
    tx.execute(
        "INSERT INTO hive_worker_introductions (
            worker_id, run_id, status, prompt_version, created_at, updated_at
         ) VALUES (?1, ?2, 'queued', 1, ?3, ?3)",
        params![worker_id, run_id, now],
    )?;
    tx.execute(
        "INSERT INTO hive_runtime_state (
            session_id, status, current_run_id, priority, worker_id, updated_at
         ) VALUES (?1, 'idle', ?2, 'normal', ?3, ?4)",
        params![session_id, run_id, worker_id, now],
    )?;
    let event = append_event(
        tx,
        &controller,
        "run_queued",
        Some(&run_id),
        None,
        Some(&format!("run:{run_id}:queued")),
        serde_json::json!({
            "run_id": run_id,
            "kind": "worker_introduction",
            "worker_id": worker_id,
        }),
        now,
    )?;

    Ok(Mutation {
        response: ResponsePayload::WorkerIntroduction(WorkerIntroductionResponse {
            worker_id: worker_id.clone(),
            session_id,
            run_id,
            status: "queued".into(),
            revision: 1,
        }),
        resource_id: Some(worker_id),
        events: vec![event],
    })
}

fn retry_worker_introduction(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: WorkerIntroductionCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.worker_id, "Worker id", 256)?;
    let worker = load_worker_with_conn(tx, &command.worker_id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| RuntimeStoreError::Invalid("Worker was not found".into()))?;
    if worker.user_id != actor.user_id {
        return Err(RuntimeStoreError::Ownership);
    }
    if worker.status == mitsuro_core::storage::HiveWorkerStatus::Archived {
        return Err(RuntimeStoreError::StateConflict(
            "An archived Worker cannot retry its Introduction".into(),
        ));
    }
    let session_id = worker.dm_session_id.clone().ok_or_else(|| {
        RuntimeStoreError::StateConflict("Worker has no private conversation".into())
    })?;
    let session = require_owned_session(tx, actor, &session_id)?;
    let session_model = require_frozen_session_model(&session)?.to_string();
    let session_key = session.model_key.clone().ok_or_else(|| {
        RuntimeStoreError::StateConflict(
            "Worker Introduction retry requires an exact provider/model key".into(),
        )
    })?;
    let worker_model = worker
        .model
        .as_deref()
        .ok_or_else(|| RuntimeStoreError::StateConflict("Worker has no frozen model".into()))?;
    let worker_key = worker.model_key.as_ref().ok_or_else(|| {
        RuntimeStoreError::StateConflict("Worker has no exact provider/model key".into())
    })?;
    let worker_key_value = serde_json::to_value(worker_key)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let session_key_value = serde_json::to_value(&session_key)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    if worker_model != session_model
        || worker_key_value != session_key_value
        || worker.model_catalog_revision != session.model_catalog_revision
        || worker.permission_mode.as_str() != session.permission_mode
    {
        return Err(RuntimeStoreError::StateConflict(
            "Worker and private conversation no longer share one exact execution identity".into(),
        ));
    }

    let transcript_count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
        [&session_id],
        |row| row.get(0),
    )?;
    if transcript_count != 0 {
        return Err(RuntimeStoreError::StateConflict(
            "The private conversation is no longer empty; skip the Introduction instead".into(),
        ));
    }
    let introduction = tx
        .query_row(
            "SELECT run_id, status, opening_message_id
             FROM hive_worker_introductions WHERE worker_id = ?1",
            [&worker.id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| {
            RuntimeStoreError::StateConflict("Worker has no failed Introduction to retry".into())
        })?;
    if !matches!(introduction.1.as_str(), "failed" | "needs_recovery") || introduction.2.is_some() {
        return Err(RuntimeStoreError::StateConflict(format!(
            "Worker Introduction cannot retry from {}",
            introduction.1
        )));
    }
    let previous_run_id = introduction.0.ok_or_else(|| {
        RuntimeStoreError::StateConflict("Worker Introduction has no prior run".into())
    })?;
    let (previous_status, controller_id) = tx
        .query_row(
            "SELECT status, controller_id
             FROM hive_runs
             WHERE id = ?1 AND worker_id = ?2 AND session_id = ?3
               AND kind = 'worker_introduction'",
            params![previous_run_id, worker.id, session_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .ok_or_else(|| {
            RuntimeStoreError::StateConflict(
                "Worker Introduction prior run is missing or mismatched".into(),
            )
        })?;
    if !matches!(
        previous_status.as_str(),
        "failed" | "dead_letter" | "recovery_required" | "cancelled"
    ) {
        return Err(RuntimeStoreError::StateConflict(format!(
            "Worker Introduction run is still {previous_status}; retry after it reaches a terminal boundary"
        )));
    }
    let controller = get_or_create_controller(tx, &session, now)?;
    if controller.id != controller_id {
        return Err(RuntimeStoreError::StateConflict(
            "Worker Introduction controller identity changed".into(),
        ));
    }

    if previous_status == "recovery_required" {
        tx.execute(
            "UPDATE hive_runs
             SET status = 'cancelled', last_stop_reason = 'superseded by explicit Introduction retry',
                 finished_at = COALESCE(finished_at, ?2), updated_at = ?2
             WHERE id = ?1 AND status = 'recovery_required'",
            params![previous_run_id, now],
        )?;
    }
    let run_id = uuid::Uuid::new_v4().to_string();
    let retry_policy = RetryPolicy {
        max_attempts: 1,
        ..RetryPolicy::default()
    };
    let execution_context = HiveRunExecutionContextV1::worker_conversation_neutral(
        worker.id.clone(),
        worker.revision,
        WorkerConversationLane::DirectMessage,
    )
    .map_err(RuntimeStoreError::Internal)?;
    insert_run(
        tx,
        &run_id,
        &controller.id,
        Some(&session_id),
        None,
        None,
        "worker_introduction",
        "Retry the one-time Worker Introduction and then wait for the user.",
        serde_json::json!({
            "model": session_model,
            "model_key": session_key,
            "model_catalog_revision": session.model_catalog_revision,
            "permission_mode": session.permission_mode,
            "worker_id": worker.id,
            "introduction": {
                "prompt_version": 1,
                "context_mode": "worker_identity_only",
                "tool_allowlist": [],
                "web_access": false,
                "project_access": false,
                "one_shot": true,
            },
            "retry": retry_policy,
        }),
        priority_value("normal")?,
        now,
        1,
        now,
        Some(WorkerRunBinding {
            worker_id: &worker.id,
            execution_context: &execution_context,
            origin: WorkerRunOrigin::UserLifecycleAction,
        }),
    )?;
    let changed = tx.execute(
        "UPDATE hive_worker_introductions
         SET run_id = ?2, status = 'queued', opening_message_id = NULL,
             proposal_json = NULL, last_error = NULL, completed_at = NULL,
             updated_at = ?3
         WHERE worker_id = ?1 AND run_id = ?4
           AND status IN ('failed', 'needs_recovery')
           AND opening_message_id IS NULL",
        params![worker.id, run_id, now, previous_run_id],
    )?;
    if changed != 1 {
        return Err(RuntimeStoreError::StateConflict(
            "Worker Introduction changed while retrying".into(),
        ));
    }
    tx.execute(
        "UPDATE hive_controllers SET status = 'active', updated_at = ?2 WHERE id = ?1",
        params![controller.id, now],
    )?;
    tx.execute(
        "INSERT INTO hive_runtime_state (
             session_id, status, current_run_id, priority, worker_id, updated_at
         ) VALUES (?1, 'idle', ?2, 'normal', ?3, ?4)
         ON CONFLICT(session_id) DO UPDATE SET
             status = 'idle', current_run_id = excluded.current_run_id,
             worker_id = excluded.worker_id, last_error = NULL,
             sleep_reason = NULL, last_wake_reason = 'worker_introduction_retry',
             updated_at = excluded.updated_at",
        params![session_id, run_id, worker.id, now],
    )?;
    let event = append_event(
        tx,
        &controller,
        "run_queued",
        Some(&run_id),
        None,
        Some(&format!("run:{run_id}:queued")),
        serde_json::json!({
            "run_id": run_id,
            "kind": "worker_introduction",
            "worker_id": worker.id,
            "retry_of": previous_run_id,
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::WorkerIntroductionAction(WorkerIntroductionActionResponse {
            worker_id: worker.id.clone(),
            session_id,
            run_id: Some(run_id),
            status: "queued".into(),
            autonomy_eligible: false,
            cancellation_requested: false,
        }),
        resource_id: Some(worker.id),
        events: vec![event],
    })
}

fn skip_worker_introduction(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: WorkerIntroductionCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.worker_id, "Worker id", 256)?;
    let worker = load_worker_with_conn(tx, &command.worker_id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| RuntimeStoreError::Invalid("Worker was not found".into()))?;
    if worker.user_id != actor.user_id {
        return Err(RuntimeStoreError::Ownership);
    }
    if worker.status == mitsuro_core::storage::HiveWorkerStatus::Archived {
        return Err(RuntimeStoreError::StateConflict(
            "An archived Worker cannot change its Introduction".into(),
        ));
    }
    let session_id = worker.dm_session_id.clone().ok_or_else(|| {
        RuntimeStoreError::StateConflict("Worker has no private conversation".into())
    })?;
    let session = require_owned_session(tx, actor, &session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let introduction = tx
        .query_row(
            "SELECT run_id, status, opening_message_id
             FROM hive_worker_introductions WHERE worker_id = ?1",
            [&worker.id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()?;

    let (run_id, current_status, opening_message_id) = match introduction {
        Some(row) => row,
        None => {
            tx.execute(
                "INSERT INTO hive_worker_introductions (
                     worker_id, run_id, status, prompt_version, created_at,
                     updated_at, completed_at
                 ) VALUES (?1, NULL, 'skipped', 1, ?2, ?2, ?2)",
                params![worker.id, now],
            )?;
            let event = append_event(
                tx,
                &controller,
                "worker_introduction_skipped",
                None,
                None,
                Some(&format!("worker:{}:introduction:skipped", worker.id)),
                serde_json::json!({"worker_id": worker.id, "legacy": true}),
                now,
            )?;
            return Ok(Mutation {
                response: ResponsePayload::WorkerIntroductionAction(
                    WorkerIntroductionActionResponse {
                        worker_id: worker.id.clone(),
                        session_id,
                        run_id: None,
                        status: "skipped".into(),
                        autonomy_eligible: true,
                        cancellation_requested: false,
                    },
                ),
                resource_id: Some(worker.id),
                events: vec![event],
            });
        }
    };
    if current_status == "skipped" {
        return Ok(Mutation {
            response: ResponsePayload::WorkerIntroductionAction(WorkerIntroductionActionResponse {
                worker_id: worker.id.clone(),
                session_id,
                run_id,
                status: "skipped".into(),
                autonomy_eligible: true,
                cancellation_requested: false,
            }),
            resource_id: Some(worker.id),
            events: Vec::new(),
        });
    }
    if current_status == "confirmed" {
        return Err(RuntimeStoreError::StateConflict(
            "Worker Introduction is already confirmed".into(),
        ));
    }
    if !matches!(
        current_status.as_str(),
        "queued" | "running" | "awaiting_context" | "review_ready" | "failed" | "needs_recovery"
    ) {
        return Err(RuntimeStoreError::StateConflict(format!(
            "Worker Introduction cannot be skipped from {current_status}"
        )));
    }

    if matches!(current_status.as_str(), "awaiting_context" | "review_ready") {
        let active_conversation_work = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM hive_runs run
                 WHERE run.session_id = ?1
                   AND (?2 IS NULL OR run.id <> ?2)
                   AND run.status IN (
                       'queued', 'leased', 'running', 'sleeping', 'retry_wait',
                       'awaiting_input', 'recovery_required'
                   )
                 UNION ALL
                 SELECT 1 FROM messages pending
                 WHERE pending.session_id = ?1
                   AND pending.role LIKE 'pending_user:%'
             )",
            params![session_id, run_id],
            |row| row.get::<_, bool>(0),
        )?;
        if active_conversation_work {
            return Err(RuntimeStoreError::StateConflict(
                "Hive Worker Introduction cannot be skipped while its private conversation has active or pending user work; wait for that turn to finish"
                    .into(),
            ));
        }
    }

    let mut cancellation_requested = false;
    if let Some(run_id) = run_id.as_deref() {
        let run = tx
            .query_row(
                "SELECT status, attempt_count, lease_token
                 FROM hive_runs
                 WHERE id = ?1 AND worker_id = ?2 AND session_id = ?3
                   AND kind = 'worker_introduction' AND controller_id = ?4",
                params![run_id, worker.id, session_id, controller.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| {
                RuntimeStoreError::StateConflict(
                    "Worker Introduction run is missing or mismatched".into(),
                )
            })?;
        if matches!(
            run.0.as_str(),
            "queued"
                | "leased"
                | "running"
                | "sleeping"
                | "retry_wait"
                | "awaiting_input"
                | "recovery_required"
        ) {
            cancellation_requested = run.0 == "running";
            if let Some(lease_token) = run.2.as_deref() {
                tx.execute(
                    "UPDATE hive_run_attempts
                     SET finished_at = COALESCE(finished_at, ?4),
                         outcome = CASE WHEN finished_at IS NULL THEN 'cancelled' ELSE outcome END,
                         stop_reason = CASE WHEN finished_at IS NULL
                             THEN 'Introduction skipped by user' ELSE stop_reason END
                     WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3",
                    params![run_id, run.1, lease_token, now],
                )?;
            }
            tx.execute(
                "UPDATE hive_runs
                 SET status = 'cancelled', lease_owner = NULL, lease_token = NULL,
                     lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                     wake_at = NULL, last_stop_reason = 'Introduction skipped by user',
                     finished_at = COALESCE(finished_at, ?2), updated_at = ?2
                 WHERE id = ?1
                   AND status IN ('queued', 'leased', 'running', 'sleeping',
                                  'retry_wait', 'awaiting_input', 'recovery_required')",
                params![run_id, now],
            )?;
            tx.execute(
                "UPDATE hive_control_outbox
                 SET status = 'discarded', last_error = 'Introduction skipped by user',
                     updated_at = ?2
                 WHERE run_id = ?1 AND status = 'pending'",
                params![run_id, now],
            )?;
        }
    }

    let opening_preexists = opening_message_id.is_some();
    let changed = tx.execute(
        "UPDATE hive_worker_introductions
         SET status = 'skipped', last_error = NULL,
             completed_at = COALESCE(completed_at, ?2), updated_at = ?2
         WHERE worker_id = ?1 AND status = ?3
           AND opening_message_id IS ?4",
        params![worker.id, now, current_status, opening_message_id],
    )?;
    if changed != 1 {
        return Err(RuntimeStoreError::StateConflict(
            "Worker Introduction changed while skipping".into(),
        ));
    }
    HiveWorkerIntroductionStore::from_connection(tx)
        .terminalize_claimed_reviews_for_skip(&worker.id, now)
        .map_err(RuntimeStoreError::Internal)?;
    if let Some(run_id) = run_id.as_deref() {
        tx.execute(
            "UPDATE hive_runtime_state
             SET status = 'idle', current_run_id = NULL,
                 sleep_reason = 'Introduction skipped by user',
                 last_wake_reason = 'worker_introduction_skip',
                 last_error = NULL, updated_at = ?3
             WHERE session_id = ?1 AND current_run_id = ?2",
            params![session_id, run_id, now],
        )?;
    }
    let event = append_event(
        tx,
        &controller,
        "worker_introduction_skipped",
        run_id.as_deref(),
        None,
        Some(&format!("worker:{}:introduction:skipped", worker.id)),
        serde_json::json!({
            "worker_id": worker.id,
            "run_id": run_id,
            "opening_preexisted": opening_preexists,
            "cancellation_requested": cancellation_requested,
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::WorkerIntroductionAction(WorkerIntroductionActionResponse {
            worker_id: worker.id.clone(),
            session_id,
            run_id,
            status: "skipped".into(),
            autonomy_eligible: true,
            cancellation_requested,
        }),
        resource_id: Some(worker.id),
        events: vec![event],
    })
}

fn confirm_reviewed_worker_introduction(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: ConfirmWorkerIntroductionCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_review_decision_identity(
        &command.worker_id,
        &command.proposal_id,
        command.proposal_revision,
    )?;
    if command.selected_facts.is_empty()
        || command.selected_facts.len() > MAX_WORKER_INTRODUCTION_FACTS
    {
        return Err(RuntimeStoreError::Invalid(format!(
            "Introduction confirmation must select 1-{MAX_WORKER_INTRODUCTION_FACTS} facts"
        )));
    }
    let mut selected_facts = Vec::with_capacity(command.selected_facts.len());
    for selection in command.selected_facts {
        validate_bounded_field(&selection.fact_id, "Introduction fact id", 256)?;
        validate_bounded_field(
            &selection.final_statement,
            "Introduction fact statement",
            800,
        )?;
        selected_facts.push(WorkerIntroductionSelectedFactV1 {
            fact_id: selection.fact_id,
            final_statement: selection.final_statement,
        });
    }

    let (worker, session_id, controller) =
        load_review_decision_binding(tx, actor, now, &command.worker_id, true)?;
    let introduction = confirm_worker_introduction_in_transaction(
        tx,
        &ConfirmWorkerIntroductionRequest {
            user_id: actor.user_id.clone(),
            worker_id: worker.id.clone(),
            proposal_id: command.proposal_id.clone(),
            proposal_revision: command.proposal_revision,
            selected_facts,
        },
    )
    .map_err(map_review_decision_error)?;
    if introduction.status != HiveWorkerIntroductionStatus::Confirmed {
        return Err(RuntimeStoreError::StateConflict(
            "Introduction confirmation did not reach the confirmed lifecycle".into(),
        ));
    }

    let event = append_event(
        tx,
        &controller,
        "worker_introduction_confirmed",
        introduction.run_id.as_deref(),
        None,
        Some(&format!(
            "worker:{}:introduction:proposal:{}:{}:confirmed",
            worker.id, command.proposal_id, command.proposal_revision
        )),
        serde_json::json!({
            "worker_id": worker.id,
            "proposal_id": command.proposal_id,
            "proposal_revision": command.proposal_revision,
            "status": introduction.status.as_str(),
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::WorkerIntroductionAction(WorkerIntroductionActionResponse {
            worker_id: worker.id.clone(),
            session_id,
            run_id: introduction.run_id,
            status: "confirmed".into(),
            autonomy_eligible: true,
            cancellation_requested: false,
        }),
        resource_id: Some(worker.id),
        events: vec![event],
    })
}

fn return_reviewed_worker_introduction_to_context(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: ReturnWorkerIntroductionToContextCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_review_decision_identity(
        &command.worker_id,
        &command.proposal_id,
        command.proposal_revision,
    )?;
    let (decision, decision_name, event_type) = match command.decision {
        WorkerIntroductionReturnDecision::KeepTalking => (
            WorkerIntroductionDecisionKind::KeepTalking,
            "keep_talking",
            "worker_introduction_keep_talking",
        ),
        WorkerIntroductionReturnDecision::Rejected => (
            WorkerIntroductionDecisionKind::Rejected,
            "rejected",
            "worker_introduction_rejected",
        ),
    };
    let (worker, session_id, controller) =
        load_review_decision_binding(tx, actor, now, &command.worker_id, false)?;
    let introduction = return_worker_introduction_to_context_in_transaction(
        tx,
        &ReturnWorkerIntroductionToContextRequest {
            user_id: actor.user_id.clone(),
            worker_id: worker.id.clone(),
            proposal_id: command.proposal_id.clone(),
            proposal_revision: command.proposal_revision,
            decision,
        },
    )
    .map_err(map_review_decision_error)?;
    if introduction.status != HiveWorkerIntroductionStatus::AwaitingContext
        || introduction.proposal.is_some()
    {
        return Err(RuntimeStoreError::StateConflict(
            "Introduction return decision did not resume context gathering".into(),
        ));
    }

    let event = append_event(
        tx,
        &controller,
        event_type,
        introduction.run_id.as_deref(),
        None,
        Some(&format!(
            "worker:{}:introduction:proposal:{}:{}:{decision_name}",
            worker.id, command.proposal_id, command.proposal_revision
        )),
        serde_json::json!({
            "worker_id": worker.id,
            "proposal_id": command.proposal_id,
            "proposal_revision": command.proposal_revision,
            "decision": decision_name,
            "status": introduction.status.as_str(),
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::WorkerIntroductionAction(WorkerIntroductionActionResponse {
            worker_id: worker.id.clone(),
            session_id,
            run_id: introduction.run_id,
            status: "awaiting_context".into(),
            autonomy_eligible: false,
            cancellation_requested: false,
        }),
        resource_id: Some(worker.id),
        events: vec![event],
    })
}

fn validate_review_decision_identity(
    worker_id: &str,
    proposal_id: &str,
    proposal_revision: u32,
) -> Result<(), RuntimeStoreError> {
    validate_bounded_field(worker_id, "Worker id", 256)?;
    validate_bounded_field(proposal_id, "Introduction proposal id", 256)?;
    if proposal_revision == 0 {
        return Err(RuntimeStoreError::Invalid(
            "Introduction proposal revision must be positive".into(),
        ));
    }
    Ok(())
}

fn load_review_decision_binding(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    worker_id: &str,
    require_active: bool,
) -> Result<(mitsuro_core::storage::HiveWorker, String, ControllerRecord), RuntimeStoreError> {
    let worker = load_worker_with_conn(tx, worker_id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| RuntimeStoreError::Invalid("Worker was not found".into()))?;
    if worker.user_id != actor.user_id {
        return Err(RuntimeStoreError::Ownership);
    }
    if worker.status == mitsuro_core::storage::HiveWorkerStatus::Archived
        || (require_active && worker.status != mitsuro_core::storage::HiveWorkerStatus::Active)
    {
        return Err(RuntimeStoreError::StateConflict(
            if require_active {
                "only an active Worker can confirm its Introduction proposal"
            } else {
                "an archived Worker cannot return its Introduction to context"
            }
            .into(),
        ));
    }
    let session_id = worker.dm_session_id.clone().ok_or_else(|| {
        RuntimeStoreError::StateConflict("Worker has no private conversation".into())
    })?;
    let session = require_owned_session(tx, actor, &session_id)?;
    let binding = resolve_worker_conversation_with_conn(tx, &session_id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| {
            RuntimeStoreError::StateConflict(
                "Hive Worker private conversation binding is missing".into(),
            )
        })?;
    if binding.group_id.is_some()
        || binding.worker.id != worker.id
        || binding.worker.user_id != actor.user_id
        || binding.worker.dm_session_id.as_deref() != Some(session_id.as_str())
    {
        return Err(RuntimeStoreError::StateConflict(
            "Hive Worker Introduction is not bound to the exact private conversation".into(),
        ));
    }
    let controller = get_or_create_controller(tx, &session, now)?;
    Ok((worker, session_id, controller))
}

fn map_review_decision_error(error: anyhow::Error) -> RuntimeStoreError {
    if error.downcast_ref::<rusqlite::Error>().is_some() {
        RuntimeStoreError::Internal(error)
    } else {
        RuntimeStoreError::StateConflict(error.to_string())
    }
}

fn validate_introduction_document(
    content: Option<String>,
    label: &str,
) -> Result<Option<String>, RuntimeStoreError> {
    let Some(content) = content else {
        return Ok(None);
    };
    let content = content.trim().to_string();
    if content.is_empty()
        || content.len() > MAX_HIVE_PROFILE_DOCUMENT_BYTES
        || content.as_bytes().contains(&0)
    {
        return Err(RuntimeStoreError::Invalid(format!(
            "Worker {label} document is empty, invalid, or exceeds {MAX_HIVE_PROFILE_DOCUMENT_BYTES} bytes"
        )));
    }
    Ok(Some(content))
}

fn validate_bounded_field(
    value: &str,
    field: &str,
    max_bytes: usize,
) -> Result<(), RuntimeStoreError> {
    if value.trim().is_empty() || value.len() > max_bytes || value.as_bytes().contains(&0) {
        return Err(RuntimeStoreError::Invalid(format!(
            "{field} is invalid or exceeds {max_bytes} bytes"
        )));
    }
    Ok(())
}

fn update_worker(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: UpdateWorkerCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.worker_id, "Worker id", 256)?;
    if command.expected_revision == 0 {
        return Err(RuntimeStoreError::Invalid(
            "Worker expected_revision must be at least 1".into(),
        ));
    }
    let display_name = command.display_name.trim().to_string();
    validate_bounded_field(&display_name, "Worker display name", 256)?;
    let avatar_color = command
        .avatar_color
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if avatar_color
        .as_deref()
        .is_some_and(|value| value.len() > 64 || value.as_bytes().contains(&0))
    {
        return Err(RuntimeStoreError::Invalid(
            "Worker avatar color is invalid or exceeds 64 bytes".into(),
        ));
    }
    let (model, model_key, model_catalog_revision) = normalize_model_identity(
        command.model,
        command.model_key,
        command.model_catalog_revision,
        "Worker update",
    )?;
    if model.is_some() != model_key.is_some() {
        return Err(RuntimeStoreError::Invalid(
            "Worker model and exact model_key must either both be set or both be absent".into(),
        ));
    }
    let permission_mode = match command.permission_mode.trim() {
        value @ ("supervised" | "autonomous") => value.to_string(),
        _ => {
            return Err(RuntimeStoreError::Invalid(
                "Worker permission mode must be supervised or autonomous".into(),
            ))
        }
    };
    let autonomy = match command.autonomy.trim() {
        value @ ("manual" | "scheduled" | "always_on") => value.to_string(),
        _ => {
            return Err(RuntimeStoreError::Invalid(
                "Worker autonomy must be manual, scheduled, or always_on".into(),
            ))
        }
    };
    if command.heartbeat_interval_secs == Some(0) {
        return Err(RuntimeStoreError::Invalid(
            "Worker heartbeat interval must be positive".into(),
        ));
    }
    let heartbeat_interval_secs = match (autonomy.as_str(), command.heartbeat_interval_secs) {
        ("always_on", None) => Some(DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECS),
        (_, interval) => interval,
    };
    let identity = validate_introduction_document(command.identity, "identity")?;
    let soul = validate_introduction_document(command.soul, "soul")?;

    let (worker, session_id, controller) =
        require_owned_worker_dm(tx, actor, now, &command.worker_id)?;
    if worker.status == HiveWorkerStatus::Archived {
        return Err(RuntimeStoreError::StateConflict(
            "Archived Workers are immutable".into(),
        ));
    }
    if worker.revision != command.expected_revision {
        return Err(RuntimeStoreError::RevisionConflict(format!(
            "Worker revision changed from {} to {}",
            command.expected_revision, worker.revision
        )));
    }
    let open_run = tx
        .query_row(
            "SELECT id, status FROM hive_runs
             WHERE worker_id = ?1
               AND status IN ('queued', 'leased', 'running', 'sleeping',
                              'retry_wait', 'awaiting_input', 'recovery_required')
             ORDER BY updated_at ASC, id ASC LIMIT 1",
            [&worker.id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if let Some((run_id, status)) = open_run {
        return Err(RuntimeStoreError::StateConflict(format!(
            "Worker profile cannot change while run {run_id} is {status}; pause or resolve the lane first"
        )));
    }
    validate_worker_schedule_identity(
        tx,
        &worker.id,
        model.as_deref(),
        model_key.as_ref(),
        model_catalog_revision.as_deref(),
    )?;

    let introduction_store = HiveWorkerIntroductionStore::from_connection(tx);
    if let Some(introduction) = introduction_store
        .get_by_worker(&worker.id)
        .map_err(RuntimeStoreError::Internal)?
    {
        if introduction.status == HiveWorkerIntroductionStatus::ReviewReady {
            let proposal = introduction
                .proposal
                .ok_or_else(|| {
                    RuntimeStoreError::StateConflict(
                        "Review-ready Worker Introduction has no typed proposal".into(),
                    )
                })
                .and_then(|value| {
                    serde_json::from_value::<WorkerIntroductionProposalV1>(value).map_err(|error| {
                        RuntimeStoreError::StateConflict(format!(
                            "Review-ready Worker Introduction proposal is malformed: {error}"
                        ))
                    })
                })?;
            return_worker_introduction_to_context_in_transaction(
                tx,
                &ReturnWorkerIntroductionToContextRequest {
                    user_id: actor.user_id.clone(),
                    worker_id: worker.id.clone(),
                    proposal_id: proposal.proposal_id,
                    proposal_revision: proposal.revision,
                    decision: WorkerIntroductionDecisionKind::KeepTalking,
                },
            )
            .map_err(map_review_decision_error)?;
        }
    }

    let model_key_json = model_key
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let next_revision = worker
        .revision
        .checked_add(1)
        .ok_or_else(|| RuntimeStoreError::StateConflict("Worker revision overflow".into()))?;
    let changed = tx.execute(
        "UPDATE hive_workers
         SET display_name = ?2, avatar_color = ?3, model = ?4,
             model_key_json = ?5, model_catalog_revision = ?6,
             permission_mode = ?7, autonomy = ?8,
             heartbeat_interval_secs = ?9, revision = ?10, updated_at = ?11
         WHERE id = ?1 AND revision = ?12 AND status <> 'archived'",
        params![
            worker.id,
            display_name,
            avatar_color,
            model,
            model_key_json,
            model_catalog_revision,
            permission_mode,
            autonomy,
            heartbeat_interval_secs,
            next_revision,
            now,
            command.expected_revision,
        ],
    )?;
    if changed != 1 {
        return Err(RuntimeStoreError::RevisionConflict(
            "Worker changed while its profile update was committing".into(),
        ));
    }
    for (kind, content) in [("identity", identity), ("soul", soul)] {
        if let Some(content) = content {
            tx.execute(
                "INSERT INTO hive_worker_documents (worker_id, kind, content, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(worker_id, kind) DO UPDATE SET
                    content = excluded.content, updated_at = excluded.updated_at",
                params![worker.id, kind, content, now],
            )?;
        } else {
            tx.execute(
                "DELETE FROM hive_worker_documents WHERE worker_id = ?1 AND kind = ?2",
                params![worker.id, kind],
            )?;
        }
    }
    let session_changed = tx.execute(
        "UPDATE sessions
         SET title = ?2, model = ?3, model_key_json = ?4,
             model_catalog_revision = ?5, permission_mode = ?6, updated_at = ?7
         WHERE id = ?1 AND session_type = 'hive'
           AND user_id IS ?8",
        params![
            session_id,
            display_name,
            model,
            model_key_json,
            model_catalog_revision,
            permission_mode,
            now,
            actor.user_id,
        ],
    )?;
    if session_changed != 1 {
        return Err(RuntimeStoreError::StateConflict(
            "Worker DM changed while its profile update was committing".into(),
        ));
    }
    let event = append_event(
        tx,
        &controller,
        "worker_updated",
        None,
        None,
        Some(&format!("worker:{}:revision:{next_revision}", worker.id)),
        serde_json::json!({
            "worker_id": worker.id,
            "revision": next_revision,
            "status": worker.status.as_str(),
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::WorkerMutation(WorkerMutationResponse {
            worker_id: worker.id.clone(),
            revision: next_revision,
            status: worker.status.as_str().into(),
            cancellation_requests: Vec::new(),
            attention: Vec::new(),
        }),
        resource_id: Some(worker.id),
        events: vec![event],
    })
}

fn set_worker_status(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: SetWorkerStatusCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.worker_id, "Worker id", 256)?;
    if command.expected_revision == 0 {
        return Err(RuntimeStoreError::Invalid(
            "Worker expected_revision must be at least 1".into(),
        ));
    }
    let (worker, _session_id, dm_controller) =
        require_owned_worker_dm(tx, actor, now, &command.worker_id)?;
    if worker.revision != command.expected_revision {
        return Err(RuntimeStoreError::RevisionConflict(format!(
            "Worker revision changed from {} to {}",
            command.expected_revision, worker.revision
        )));
    }
    if worker.status == HiveWorkerStatus::Archived {
        return Err(RuntimeStoreError::StateConflict(
            "Archived Workers cannot be resumed or changed".into(),
        ));
    }
    let target = match command.status {
        WorkerTargetStatus::Paused => HiveWorkerStatus::Paused,
        WorkerTargetStatus::Active => HiveWorkerStatus::Active,
        WorkerTargetStatus::Archived => HiveWorkerStatus::Archived,
    };
    if target == worker.status {
        return Ok(Mutation {
            response: ResponsePayload::WorkerMutation(WorkerMutationResponse {
                worker_id: worker.id.clone(),
                revision: worker.revision,
                status: worker.status.as_str().into(),
                cancellation_requests: Vec::new(),
                attention: worker_lane_attention(tx, &worker.id)?,
            }),
            resource_id: Some(worker.id),
            events: Vec::new(),
        });
    }
    if target == HiveWorkerStatus::Active && worker.status != HiveWorkerStatus::Paused {
        return Err(RuntimeStoreError::StateConflict(
            "Only a paused Worker can resume".into(),
        ));
    }

    let lanes = exact_worker_lanes(tx, actor, &worker.id)?;
    let mut cancellations = Vec::new();
    let mut events = Vec::new();
    match target {
        HiveWorkerStatus::Paused => {
            for controller in &lanes {
                transition_worker_runs_for_pause(
                    tx,
                    &worker.id,
                    controller,
                    now,
                    &mut cancellations,
                )?;
                tx.execute(
                    "UPDATE hive_controllers SET status = 'paused', updated_at = ?2
                     WHERE id = ?1 AND worker_id = ?3",
                    params![controller.id, now, worker.id],
                )?;
                tx.execute(
                    "UPDATE hive_runtime_state SET status = 'paused',
                         next_wake_at = NULL, sleep_reason = 'worker_paused', updated_at = ?2
                     WHERE session_id = ?1
                       AND NOT EXISTS (
                           SELECT 1 FROM hive_runs run
                           WHERE run.controller_id = ?3 AND run.status = 'recovery_required'
                       )",
                    params![controller.session_id, now, controller.id],
                )?;
            }
            // Pending owner acceptance is provider-free and remains an exact,
            // immutable authority while the Worker is paused. The paused
            // controller gates action; resume restores the same candidate.
        }
        HiveWorkerStatus::Active => {
            for controller in &lanes {
                let recovery_count: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM hive_runs
                     WHERE controller_id = ?1 AND worker_id = ?2
                       AND status = 'recovery_required'",
                    params![controller.id, worker.id],
                    |row| row.get(0),
                )?;
                if recovery_count == 0 && controller.status != "disabled" {
                    tx.execute(
                        "UPDATE hive_controllers SET status = 'active', updated_at = ?2
                         WHERE id = ?1 AND worker_id = ?3",
                        params![controller.id, now, worker.id],
                    )?;
                    tx.execute(
                        "UPDATE hive_runtime_state SET status = 'idle', last_error = NULL,
                             sleep_reason = NULL, updated_at = ?2 WHERE session_id = ?1",
                        params![controller.session_id, now],
                    )?;
                }
            }
        }
        HiveWorkerStatus::Archived => {
            for controller in &lanes {
                transition_worker_runs_for_archive(
                    tx,
                    &worker.id,
                    controller,
                    now,
                    &mut cancellations,
                )?;
                tx.execute(
                    "UPDATE hive_controllers SET status = 'disabled', updated_at = ?2
                     WHERE id = ?1 AND worker_id = ?3",
                    params![controller.id, now, worker.id],
                )?;
                tx.execute(
                    "UPDATE hive_runtime_state SET status = 'cancelled',
                         next_wake_at = NULL, sleep_reason = 'worker_archived', updated_at = ?2
                     WHERE session_id = ?1
                       AND NOT EXISTS (
                           SELECT 1 FROM hive_runs run
                           WHERE run.controller_id = ?3 AND run.status = 'recovery_required'
                       )",
                    params![controller.session_id, now, controller.id],
                )?;
            }
            // The acceptance helper owns the dedicated acceptance run. It
            // must run only after the source Worker Workflow has adopted any
            // exact committed outcome and before the Worker status changes.
            archive_worker_goal_acceptances_in_transaction(tx, &worker.id, now)
                .map_err(map_worker_workflow_error)?;
            tx.execute(
                "UPDATE hive_schedules
                 SET status = 'cancelled', revision = revision + 1,
                     next_fire_at = NULL, updated_at = ?2
                 WHERE worker_id = ?1 AND status IN ('enabled', 'paused')",
                params![worker.id, now],
            )?;
            tx.execute(
                "UPDATE hive_schedule_occurrences
                 SET status = 'cancelled', decision_reason = 'worker_archived', updated_at = ?2
                 WHERE schedule_id IN (SELECT id FROM hive_schedules WHERE worker_id = ?1)
                   AND status IN ('pending', 'queued')",
                params![worker.id, now],
            )?;
            mitsuro_core::workflow::pause_worker_goals_for_archive_in_transaction(
                tx,
                &worker.id,
                actor.user_id.as_deref().unwrap_or("local"),
            )
            .map_err(|error| RuntimeStoreError::Internal(anyhow::anyhow!(error)))?;
        }
    }

    let changed = tx.execute(
        "UPDATE hive_workers SET status = ?2, updated_at = ?3
         WHERE id = ?1 AND revision = ?4 AND status = ?5",
        params![
            worker.id,
            target.as_str(),
            now,
            command.expected_revision,
            worker.status.as_str(),
        ],
    )?;
    if changed != 1 {
        return Err(RuntimeStoreError::RevisionConflict(
            "Worker changed while its lifecycle transition was committing".into(),
        ));
    }
    if target == HiveWorkerStatus::Active {
        let completed_review_runs = {
            let mut statement = tx.prepare(
                "SELECT DISTINCT run.id
                 FROM hive_runs run
                 JOIN hive_worker_conversation_inputs input
                   ON input.accepted_while_run_id = run.id
                 WHERE run.worker_id = ?1
                   AND run.kind = 'worker_introduction_review'
                   AND run.status = 'succeeded'
                   AND input.state = 'staged'
                 ORDER BY run.updated_at, run.id",
            )?;
            let rows = statement
                .query_map([&worker.id], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        for review_run_id in completed_review_runs {
            materialize_oldest_staged_input_in_transaction(tx, &review_run_id, now)
                .map_err(RuntimeStoreError::Internal)?;
        }
    }
    for controller in &lanes {
        events.push(append_event(
            tx,
            controller,
            match target {
                HiveWorkerStatus::Paused => "worker_paused",
                HiveWorkerStatus::Active => "worker_resumed",
                HiveWorkerStatus::Archived => "worker_archived",
            },
            None,
            None,
            Some(&format!(
                "worker:{}:status:{}:at:{now}",
                worker.id,
                target.as_str()
            )),
            serde_json::json!({
                "worker_id": worker.id,
                "revision": worker.revision,
                "status": target.as_str(),
            }),
            now,
        )?);
    }
    // Every first-class Worker has a DM lane; retain a defensive event even if
    // a legacy lane enumeration returned only the canonical DM controller.
    debug_assert!(events
        .iter()
        .any(|event| event.controller_id == dm_controller.id));
    let attention = worker_lane_attention(tx, &worker.id)?;
    Ok(Mutation {
        response: ResponsePayload::WorkerMutation(WorkerMutationResponse {
            worker_id: worker.id.clone(),
            revision: worker.revision,
            status: target.as_str().into(),
            cancellation_requests: cancellations,
            attention,
        }),
        resource_id: Some(worker.id),
        events,
    })
}

#[cfg(test)]
pub(super) fn set_worker_status_for_test(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: SetWorkerStatusCommand,
) -> Result<(), RuntimeStoreError> {
    set_worker_status(tx, actor, now, command).map(|_| ())
}

fn activate_or_resume_worker_workflow(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    operation_id: &str,
    command: ActivateOrResumeWorkerWorkflowCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.worker_id, "Worker id", 256)?;
    validate_bounded_field(&command.goal_id, "Workflow Goal id", 256)?;
    if command.expected_worker_revision == 0 || command.expected_goal_revision == 0 {
        return Err(RuntimeStoreError::Invalid(
            "Worker and Workflow Goal expected revisions must be at least 1".into(),
        ));
    }
    let activation = activate_or_resume_worker_workflow_in_transaction(
        tx,
        &WorkerWorkflowActivationRequest {
            worker_id: command.worker_id,
            expected_worker_revision: command.expected_worker_revision,
            owner_user_id: actor.user_id.clone(),
            goal_id: command.goal_id,
            expected_goal_revision: command.expected_goal_revision,
            operation_id: operation_id.to_string(),
            source: WorkerWorkflowActivationSource::UserActivation,
            now: parse_utc_timestamp(now).map_err(|error| {
                RuntimeStoreError::Internal(anyhow::anyhow!(error).context("parsing mutation time"))
            })?,
        },
    )
    .map_err(map_worker_workflow_error)?;
    let controller = super::persistence::require_controller(tx, &activation.session_id)?;
    if controller.id != activation.controller_id {
        return Err(RuntimeStoreError::StateConflict(
            "Worker Workflow controller binding changed during activation".into(),
        ));
    }
    let disposition = match activation.disposition {
        CoreWorkerWorkflowActivationDisposition::Created => WorkerWorkflowDisposition::Created,
        CoreWorkerWorkflowActivationDisposition::Existing => WorkerWorkflowDisposition::Existing,
    };
    let events = if activation.disposition == CoreWorkerWorkflowActivationDisposition::Created {
        vec![append_event(
            tx,
            &controller,
            "worker_workflow_activated",
            Some(&activation.run_id),
            None,
            Some(&format!(
                "worker-workflow:{}:{}:{}",
                activation.worker_id, activation.workflow_goal_id, activation.run_id
            )),
            serde_json::json!({
                "worker_id": activation.worker_id,
                "worker_revision": activation.worker_revision,
                "goal_id": activation.workflow_goal_id,
                "goal_revision": activation.goal_revision,
                "run_id": activation.run_id,
                "run_status": activation.run_status,
                "attempt_id": activation.workflow_attempt_id,
                "attempt_status": activation.workflow_attempt_status,
            }),
            now,
        )?]
    } else {
        Vec::new()
    };
    Ok(Mutation {
        response: ResponsePayload::WorkerWorkflow(WorkerWorkflowResponse {
            disposition,
            worker_id: activation.worker_id,
            worker_revision: activation.worker_revision,
            session_id: activation.session_id,
            goal_id: activation.workflow_goal_id.clone(),
            goal_revision: activation.goal_revision,
            goal_status: activation.goal_status,
            active: Some(WorkerWorkflowRunProjection {
                run_id: activation.run_id,
                run_status: activation.run_status,
                attempt_id: activation.workflow_attempt_id,
                attempt_status: activation.workflow_attempt_status,
            }),
            affected_run_ids: Vec::new(),
            affected_attempt_ids: Vec::new(),
            cancellation_requests: Vec::new(),
        }),
        resource_id: Some(activation.workflow_goal_id),
        events,
    })
}

fn set_worker_workspace(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: SetWorkerWorkspaceCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.worker_id, "Worker id", 256)?;
    if command.expected_worker_revision == 0 {
        return Err(RuntimeStoreError::Invalid(
            "Worker expected_revision must be at least 1".into(),
        ));
    }
    let (worker, session_id, controller) =
        require_owned_worker_dm(tx, actor, now, &command.worker_id)?;
    if worker.status == HiveWorkerStatus::Archived {
        return Err(RuntimeStoreError::StateConflict(
            "Archived Workers cannot change workspace".into(),
        ));
    }
    if worker.revision != command.expected_worker_revision {
        return Err(RuntimeStoreError::RevisionConflict(format!(
            "Worker revision changed from {} to {}",
            command.expected_worker_revision, worker.revision
        )));
    }
    let owned_session = require_owned_session(tx, actor, &session_id)?;
    enforce_worker_introduction_autonomy_gate(tx, &owned_session)?;

    let (workspace_mode, working_dir, project_dir) = normalize_worker_workspace(
        command.workspace_mode,
        command.working_dir,
        command.project_dir,
    )?;
    let current: (String, Option<String>, Option<String>) = tx.query_row(
        "SELECT workspace_mode, working_dir, project_dir
         FROM sessions WHERE id = ?1 AND session_type = 'hive' AND user_id IS ?2",
        params![session_id, actor.user_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if current.0 == workspace_mode.to_string()
        && current.1 == working_dir
        && current.2 == project_dir
    {
        return Ok(Mutation {
            response: ResponsePayload::WorkerWorkspace(WorkerWorkspaceResponse {
                worker_id: worker.id.clone(),
                revision: worker.revision,
                session_id,
                workspace_mode: protocol_workspace_mode(workspace_mode),
                working_dir,
                project_dir,
            }),
            resource_id: Some(worker.id),
            events: Vec::new(),
        });
    }

    let unfinished_run: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM hive_runs
             WHERE worker_id = ?1
               AND status IN (
                   'queued', 'leased', 'running', 'sleeping', 'awaiting_input',
                   'retry_wait', 'recovery_required'
               )
         )",
        [&worker.id],
        |row| row.get(0),
    )?;
    if unfinished_run {
        return Err(RuntimeStoreError::StateConflict(
            "Pause or resolve every unfinished Worker run before changing workspace".into(),
        ));
    }
    let unfinished_goal: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM workflow_goals
             WHERE session_id = ?1 AND status IN ('draft', 'active', 'paused', 'blocked')
         )",
        [&session_id],
        |row| row.get(0),
    )?;
    if unfinished_goal {
        return Err(RuntimeStoreError::StateConflict(
            "Complete or cancel the Worker's current Goal before changing workspace".into(),
        ));
    }
    let claimed_step: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM workflow_plan_steps step
             JOIN workflow_plan_revisions plan ON plan.id = step.plan_revision_id
             JOIN workflow_goals goal ON goal.id = plan.goal_id
             WHERE goal.session_id = ?1
               AND (step.claimed_attempt_id IS NOT NULL OR step.status = 'in_progress')
         )",
        [&session_id],
        |row| row.get(0),
    )?;
    if claimed_step {
        return Err(RuntimeStoreError::StateConflict(
            "A Workflow step is still claimed for this Worker workspace".into(),
        ));
    }

    let next_revision = worker
        .revision
        .checked_add(1)
        .ok_or_else(|| RuntimeStoreError::StateConflict("Worker revision overflow".into()))?;
    let session_changed = tx.execute(
        "UPDATE sessions
         SET workspace_mode = ?2, working_dir = ?3, project_dir = ?4, updated_at = ?5
         WHERE id = ?1 AND session_type = 'hive' AND user_id IS ?6
           AND workspace_mode = ?7 AND working_dir IS ?8 AND project_dir IS ?9",
        params![
            session_id,
            workspace_mode.to_string(),
            working_dir,
            project_dir,
            now,
            actor.user_id,
            current.0,
            current.1,
            current.2,
        ],
    )?;
    let worker_changed = tx.execute(
        "UPDATE hive_workers SET revision = ?2, updated_at = ?3
         WHERE id = ?1 AND revision = ?4 AND status <> 'archived'",
        params![
            worker.id,
            next_revision,
            now,
            command.expected_worker_revision
        ],
    )?;
    if session_changed != 1 || worker_changed != 1 {
        return Err(RuntimeStoreError::RevisionConflict(
            "Worker workspace changed while the update was committing".into(),
        ));
    }
    let event = append_event(
        tx,
        &controller,
        "worker_workspace_updated",
        None,
        None,
        Some(&format!(
            "worker:{}:workspace:revision:{next_revision}",
            worker.id
        )),
        serde_json::json!({
            "worker_id": worker.id,
            "revision": next_revision,
            "workspace_mode": workspace_mode.to_string(),
            "working_dir": working_dir,
            "project_dir": project_dir,
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::WorkerWorkspace(WorkerWorkspaceResponse {
            worker_id: worker.id.clone(),
            revision: next_revision,
            session_id,
            workspace_mode: protocol_workspace_mode(workspace_mode),
            working_dir,
            project_dir,
        }),
        resource_id: Some(worker.id),
        events: vec![event],
    })
}

fn normalize_worker_workspace(
    mode: WorkerWorkspaceMode,
    working_dir: Option<String>,
    project_dir: Option<String>,
) -> Result<(WorkspaceMode, Option<String>, Option<String>), RuntimeStoreError> {
    match mode {
        WorkerWorkspaceMode::Neutral => {
            if working_dir.is_some() || project_dir.is_some() {
                return Err(RuntimeStoreError::Invalid(
                    "A neutral Worker workspace cannot carry filesystem paths".into(),
                ));
            }
            Ok((WorkspaceMode::Neutral, None, None))
        }
        WorkerWorkspaceMode::Selected | WorkerWorkspaceMode::Created => {
            let working = canonical_worker_workspace_path(working_dir.as_deref(), "working_dir")?;
            let project = canonical_worker_workspace_path(project_dir.as_deref(), "project_dir")?;
            if working != project {
                return Err(RuntimeStoreError::Invalid(
                    "Worker Goal v1 requires identical canonical working_dir and project_dir"
                        .into(),
                ));
            }
            Ok((
                match mode {
                    WorkerWorkspaceMode::Selected => WorkspaceMode::Selected,
                    WorkerWorkspaceMode::Created => WorkspaceMode::Created,
                    WorkerWorkspaceMode::Neutral => unreachable!(),
                },
                Some(working),
                Some(project),
            ))
        }
    }
}

fn canonical_worker_workspace_path(
    value: Option<&str>,
    field: &str,
) -> Result<String, RuntimeStoreError> {
    let value = value.ok_or_else(|| {
        RuntimeStoreError::Invalid(format!("attached Worker workspace requires {field}"))
    })?;
    validate_bounded_field(value, field, 16 * 1024)?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(RuntimeStoreError::Invalid(format!(
            "Worker workspace {field} must be absolute"
        )));
    }
    let canonical = path.canonicalize().map_err(|error| {
        RuntimeStoreError::StateConflict(format!(
            "Worker workspace {field} could not be resolved: {error}"
        ))
    })?;
    if !canonical.is_dir() {
        return Err(RuntimeStoreError::StateConflict(format!(
            "Worker workspace {field} is not a directory"
        )));
    }
    Ok(canonical.to_string_lossy().into_owned())
}

fn protocol_workspace_mode(mode: WorkspaceMode) -> WorkerWorkspaceMode {
    match mode {
        WorkspaceMode::Neutral => WorkerWorkspaceMode::Neutral,
        WorkspaceMode::Selected => WorkerWorkspaceMode::Selected,
        WorkspaceMode::Created => WorkerWorkspaceMode::Created,
    }
}

fn pause_worker_workflow(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    operation_id: &str,
    command: WorkerWorkflowLifecycleCommand,
) -> Result<Mutation, RuntimeStoreError> {
    transition_worker_workflow(
        tx,
        actor,
        now,
        operation_id,
        command,
        WorkerWorkflowDisposition::Paused,
    )
}

fn cancel_worker_workflow(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    operation_id: &str,
    command: WorkerWorkflowLifecycleCommand,
) -> Result<Mutation, RuntimeStoreError> {
    transition_worker_workflow(
        tx,
        actor,
        now,
        operation_id,
        command,
        WorkerWorkflowDisposition::Cancelled,
    )
}

fn transition_worker_workflow(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    operation_id: &str,
    command: WorkerWorkflowLifecycleCommand,
    disposition: WorkerWorkflowDisposition,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.worker_id, "Worker id", 256)?;
    validate_bounded_field(&command.goal_id, "Workflow Goal id", 256)?;
    validate_bounded_field(&command.reason, "Workflow lifecycle reason", 2_048)?;
    if command.expected_worker_revision == 0 || command.expected_goal_revision == 0 {
        return Err(RuntimeStoreError::Invalid(
            "Worker and Workflow Goal expected revisions must be at least 1".into(),
        ));
    }
    let request = WorkerWorkflowLifecycleRequest {
        worker_id: command.worker_id,
        expected_worker_revision: command.expected_worker_revision,
        owner_user_id: actor.user_id.clone(),
        goal_id: command.goal_id,
        expected_goal_revision: command.expected_goal_revision,
        operation_id: operation_id.to_string(),
        reason: command.reason.trim().to_string(),
        now: parse_utc_timestamp(now).map_err(|error| {
            RuntimeStoreError::Internal(anyhow::anyhow!(error).context("parsing mutation time"))
        })?,
    };
    let reason = request.reason.clone();
    let result = match disposition {
        WorkerWorkflowDisposition::Paused => pause_worker_workflow_in_transaction(tx, &request),
        WorkerWorkflowDisposition::Cancelled => cancel_worker_workflow_in_transaction(tx, &request),
        WorkerWorkflowDisposition::Created | WorkerWorkflowDisposition::Existing => {
            unreachable!("activation dispositions do not enter lifecycle transitions")
        }
    }
    .map_err(map_worker_workflow_error)?;
    let controller = super::persistence::require_controller(tx, &result.session_id)?;
    let cancellations = result
        .affected_run_ids
        .iter()
        .map(|run_id| WorkerWorkflowRunCancellation {
            worker_id: result.worker_id.clone(),
            session_id: result.session_id.clone(),
            goal_id: result.workflow_goal_id.clone(),
            run_id: run_id.clone(),
            reason: reason.clone(),
        })
        .collect::<Vec<_>>();
    let events = if result.changed {
        vec![append_event(
            tx,
            &controller,
            match disposition {
                WorkerWorkflowDisposition::Paused => "worker_workflow_paused",
                WorkerWorkflowDisposition::Cancelled => "worker_workflow_cancelled",
                WorkerWorkflowDisposition::Created | WorkerWorkflowDisposition::Existing => {
                    unreachable!("activation dispositions do not emit lifecycle events")
                }
            },
            None,
            None,
            Some(&format!(
                "worker-workflow:{}:{}:{}",
                result.worker_id, result.workflow_goal_id, result.goal_revision
            )),
            serde_json::json!({
                "worker_id": result.worker_id,
                "worker_revision": result.worker_revision,
                "goal_id": result.workflow_goal_id,
                "goal_revision": result.goal_revision,
                "goal_status": result.goal_status,
                "affected_run_ids": result.affected_run_ids,
                "affected_attempt_ids": result.affected_attempt_ids,
            }),
            now,
        )?]
    } else {
        Vec::new()
    };
    Ok(Mutation {
        response: ResponsePayload::WorkerWorkflow(WorkerWorkflowResponse {
            disposition,
            worker_id: result.worker_id,
            worker_revision: result.worker_revision,
            session_id: result.session_id,
            goal_id: result.workflow_goal_id.clone(),
            goal_revision: result.goal_revision,
            goal_status: result.goal_status,
            active: None,
            affected_run_ids: result.affected_run_ids,
            affected_attempt_ids: result.affected_attempt_ids,
            cancellation_requests: cancellations,
        }),
        resource_id: Some(result.workflow_goal_id),
        events,
    })
}

fn map_worker_workflow_error(error: WorkflowError) -> RuntimeStoreError {
    match error {
        WorkflowError::NotFound(message) => RuntimeStoreError::NotFound(message),
        WorkflowError::Conflict(message) => RuntimeStoreError::RevisionConflict(message),
        WorkflowError::InvalidTransition(message) | WorkflowError::WorkspaceRequired(message) => {
            RuntimeStoreError::StateConflict(message)
        }
        WorkflowError::Validation(message) => RuntimeStoreError::Invalid(message),
        WorkflowError::Database(message) => {
            RuntimeStoreError::Internal(anyhow::anyhow!(message).context("Worker Workflow"))
        }
        WorkflowError::Sql(error) => RuntimeStoreError::Internal(error.into()),
        WorkflowError::Json(error) => RuntimeStoreError::Internal(error.into()),
    }
}

fn require_owned_worker_dm(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    worker_id: &str,
) -> Result<(mitsuro_core::storage::HiveWorker, String, ControllerRecord), RuntimeStoreError> {
    let worker = load_worker_with_conn(tx, worker_id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| RuntimeStoreError::NotFound("Worker was not found".into()))?;
    if worker.user_id != actor.user_id {
        return Err(RuntimeStoreError::Ownership);
    }
    let session_id = worker.dm_session_id.clone().ok_or_else(|| {
        RuntimeStoreError::StateConflict("Worker has no private conversation".into())
    })?;
    let session = require_owned_session(tx, actor, &session_id)?;
    let binding = resolve_worker_conversation_with_conn(tx, &session_id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| {
            RuntimeStoreError::StateConflict(
                "Worker private conversation binding is missing".into(),
            )
        })?;
    if binding.group_id.is_some()
        || binding.worker.id != worker.id
        || binding.worker.user_id != actor.user_id
        || binding.worker.dm_session_id.as_deref() != Some(session_id.as_str())
    {
        return Err(RuntimeStoreError::StateConflict(
            "Worker is not bound to its exact private conversation".into(),
        ));
    }
    let controller = get_or_create_controller(tx, &session, now)?;
    if !tx.query_row(
        "SELECT worker_id IS ?2 FROM hive_controllers WHERE id = ?1",
        params![controller.id, worker.id],
        |row| row.get::<_, bool>(0),
    )? {
        return Err(RuntimeStoreError::StateConflict(
            "Worker private controller binding is malformed".into(),
        ));
    }
    Ok((worker, session_id, controller))
}

fn validate_worker_schedule_identity(
    tx: &Transaction<'_>,
    worker_id: &str,
    model: Option<&str>,
    model_key: Option<&ModelKey>,
    model_catalog_revision: Option<&str>,
) -> Result<(), RuntimeStoreError> {
    let expected_key = model_key
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let mut statement = tx.prepare(
        "SELECT id, model, model_key_json, model_catalog_revision
         FROM hive_schedules
         WHERE worker_id = ?1 AND status IN ('enabled', 'paused')
           AND (model IS NOT NULL OR model_key_json IS NOT NULL
                OR model_catalog_revision IS NOT NULL)",
    )?;
    let rows = statement
        .query_map([worker_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for (schedule_id, schedule_model, schedule_key_json, schedule_revision) in rows {
        let schedule_key = schedule_key_json
            .as_deref()
            .map(serde_json::from_str::<Value>)
            .transpose()
            .map_err(|error| {
                RuntimeStoreError::StateConflict(format!(
                    "Worker schedule {schedule_id} has a malformed frozen model key: {error}"
                ))
            })?;
        if schedule_model.as_deref() != model
            || schedule_key.as_ref() != expected_key.as_ref()
            || schedule_revision.as_deref() != model_catalog_revision
        {
            return Err(RuntimeStoreError::StateConflict(format!(
                "Worker schedule {schedule_id} freezes a different model identity; replace or cancel it before changing the Worker"
            )));
        }
    }
    Ok(())
}

fn exact_worker_lanes(
    tx: &Transaction<'_>,
    actor: &Actor,
    worker_id: &str,
) -> Result<Vec<ControllerRecord>, RuntimeStoreError> {
    let mut statement = tx.prepare(
        "SELECT id, session_id, status, timezone FROM hive_controllers
         WHERE worker_id = ?1 ORDER BY session_id ASC, id ASC",
    )?;
    let controllers = statement
        .query_map([worker_id], |row| {
            Ok(ControllerRecord {
                id: row.get(0)?,
                session_id: row.get(1)?,
                status: row.get(2)?,
                timezone: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    if controllers.is_empty() {
        return Err(RuntimeStoreError::StateConflict(
            "Worker has no durable execution lane".into(),
        ));
    }
    for controller in &controllers {
        let session = require_owned_session(tx, actor, &controller.session_id)?;
        let binding = resolve_worker_conversation_with_conn(tx, &session.id)
            .map_err(RuntimeStoreError::Internal)?
            .ok_or_else(|| {
                RuntimeStoreError::StateConflict(format!(
                    "Worker lane {} has no exact conversation binding",
                    controller.session_id
                ))
            })?;
        if binding.worker.id != worker_id || binding.worker.user_id != actor.user_id {
            return Err(RuntimeStoreError::StateConflict(format!(
                "Worker lane {} is bound to a different Worker",
                controller.session_id
            )));
        }
    }
    Ok(controllers)
}

fn transition_worker_runs_for_pause(
    tx: &Transaction<'_>,
    worker_id: &str,
    controller: &ControllerRecord,
    now: &str,
    cancellations: &mut Vec<WorkerRunCancellation>,
) -> Result<(), RuntimeStoreError> {
    transition_worker_runs(tx, worker_id, controller, now, false, cancellations)
}

fn transition_worker_runs_for_archive(
    tx: &Transaction<'_>,
    worker_id: &str,
    controller: &ControllerRecord,
    now: &str,
    cancellations: &mut Vec<WorkerRunCancellation>,
) -> Result<(), RuntimeStoreError> {
    transition_worker_runs(tx, worker_id, controller, now, true, cancellations)
}

fn transition_worker_runs(
    tx: &Transaction<'_>,
    worker_id: &str,
    controller: &ControllerRecord,
    now: &str,
    archive: bool,
    cancellations: &mut Vec<WorkerRunCancellation>,
) -> Result<(), RuntimeStoreError> {
    let mut statement = tx.prepare(
        "SELECT run.id, run.status, run.attempt_count, run.lease_token, run.kind,
                introduction.opening_message_id, run.lease_epoch
         FROM hive_runs run
         LEFT JOIN hive_worker_introductions introduction ON introduction.run_id = run.id
         WHERE run.controller_id = ?1 AND run.worker_id = ?2
           AND run.status IN ('queued', 'leased', 'running', 'sleeping',
                              'retry_wait', 'awaiting_input', 'recovery_required')
         ORDER BY run.created_at ASC, run.id ASC",
    )?;
    let runs = statement
        .query_map(params![controller.id, worker_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for (run_id, status, attempt_count, lease_token, kind, opening_message_id, lease_epoch) in runs
    {
        // A Worker Workflow acceptance run is not executable and has a
        // result-before-terminal-state trigger. Its lifecycle helper owns the
        // exact candidate/result/run/attempt/step transition after source-run
        // reconciliation; the generic run loop must never preempt it.
        if kind == "worker_workflow_acceptance" {
            continue;
        }
        // Pause is a scheduling gate, not an execution-provenance change.
        // Work that has not crossed a lease/provider boundary keeps its exact
        // frozen context and becomes eligible again after resume. Archive is
        // terminal and continues through the cancellation path below.
        if !archive
            && matches!(
                status.as_str(),
                "queued" | "sleeping" | "retry_wait" | "awaiting_input"
            )
        {
            continue;
        }
        let pause_requeue_lease = !archive && status == "leased";
        let uncommitted_opening = kind == "worker_introduction" && opening_message_id.is_none();
        let introduction_review = kind == "worker_introduction_review";
        let worker_workflow = kind == "worker_workflow";
        let review_provider_started = if introduction_review && !pause_requeue_lease {
            tx.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM hive_worker_provider_calls
                     WHERE run_id = ?1 AND call_kind = 'worker_introduction_review'
                 )",
                [&run_id],
                |row| row.get::<_, bool>(0),
            )?
        } else {
            false
        };
        let review_recovery = if introduction_review && !pause_requeue_lease {
            match (lease_token.as_deref(), lease_epoch) {
                (Some(lease_token), Some(lease_epoch)) => {
                    reconcile_worker_introduction_review_in_transaction(
                        tx,
                        &run_id,
                        lease_token,
                        u64::try_from(lease_epoch).map_err(|_| {
                            RuntimeStoreError::StateConflict(
                                "Introduction review lease epoch is negative".into(),
                            )
                        })?,
                        now,
                    )
                    .map_err(RuntimeStoreError::Internal)?
                }
                _ if review_provider_started => {
                    WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit
                }
                _ => WorkerIntroductionReviewRecovery::SafeBeforeProviderBoundary,
            }
        } else {
            WorkerIntroductionReviewRecovery::NotWorkerIntroductionReview
        };
        let workflow_recovery = if worker_workflow && status == "running" {
            match (lease_token.as_deref(), lease_epoch) {
                (Some(lease_token), Some(lease_epoch)) => {
                    reconcile_worker_workflow_provider_boundary_in_transaction(
                        tx,
                        &run_id,
                        lease_token,
                        u64::try_from(lease_epoch).map_err(|_| {
                            RuntimeStoreError::StateConflict(
                                "Worker Workflow lease epoch is negative".into(),
                            )
                        })?,
                        now,
                    )?
                }
                _ => WorkerWorkflowProviderRecovery::CommittedOutcomeUnaccounted,
            }
        } else {
            WorkerWorkflowProviderRecovery::NotWorkerWorkflow
        };
        let (next_status, run_outcome_json, run_last_error) = if pause_requeue_lease {
            // A lease is reserved scheduling capacity. Backend and provider
            // work starts only after `mark_running`, so abandoning this exact
            // lease is sufficient for every run kind, including the first
            // greeting and Introduction review.
            ("queued", None, None)
        } else if introduction_review {
            match &review_recovery {
                WorkerIntroductionReviewRecovery::CanonicalAuditAdopted {
                    review_id,
                    status,
                } => (
                    "succeeded",
                    Some(
                        serde_json::to_string(&serde_json::json!({
                            "kind": "succeeded",
                            "recovered": "canonical_worker_introduction_review_during_lifecycle_transition",
                            "review_id": review_id,
                            "review_status": status,
                        }))
                        .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                    ),
                    None,
                ),
                WorkerIntroductionReviewRecovery::PreProviderStale { review_id, .. } => (
                    "succeeded",
                    Some(
                        serde_json::to_string(&serde_json::json!({
                            "kind": "succeeded",
                            "recovered": "pre_provider_stale_worker_introduction_review_during_lifecycle_transition",
                            "review_id": review_id,
                        }))
                        .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                    ),
                    None,
                ),
                WorkerIntroductionReviewRecovery::TerminalFailure { review_id } => (
                    "failed",
                    Some(
                        serde_json::to_string(&serde_json::json!({
                            "kind": "failed",
                            "recovered": "terminal_worker_introduction_review_failure_during_lifecycle_transition",
                            "review_id": review_id,
                        }))
                        .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                    ),
                    Some(
                        "Terminal Worker Introduction review failure adopted during lifecycle transition",
                    ),
                ),
                WorkerIntroductionReviewRecovery::SafeBeforeProviderBoundary => {
                    (
                        "cancelled",
                        Some(
                            serde_json::to_string(&serde_json::json!({
                                "kind": "cancelled",
                                "recovered": "pre_provider_worker_introduction_review_retired_during_lifecycle_transition",
                            }))
                            .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                        ),
                        None,
                    )
                }
                WorkerIntroductionReviewRecovery::ProviderBoundaryWithoutAudit
                | WorkerIntroductionReviewRecovery::NotWorkerIntroductionReview => {
                    (
                        "recovery_required",
                        Some(
                            serde_json::to_string(&serde_json::json!({
                                "kind": "recovery_required",
                                "reason": "worker_introduction_review_provider_boundary_without_committed_audit",
                            }))
                            .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                        ),
                        Some(
                            "Worker Introduction review crossed a provider boundary without a committed audit",
                        ),
                    )
                }
            }
        } else if worker_workflow && status == "running" {
            match workflow_recovery {
                WorkerWorkflowProviderRecovery::CanonicalOutcomeAdopted => (
                    "succeeded",
                    Some(
                        serde_json::to_string(&serde_json::json!({
                            "kind": "succeeded",
                            "recovered": "canonical_worker_goal_outcome_during_lifecycle_transition",
                            "run_id": run_id,
                        }))
                        .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                    ),
                    None,
                ),
                WorkerWorkflowProviderRecovery::SafeBeforeProviderBoundary => (
                    "recovery_required",
                    None,
                    Some(
                        "Worker Workflow was interrupted before its provider boundary; explicit recovery is required",
                    ),
                ),
                WorkerWorkflowProviderRecovery::ProviderBoundaryWithoutOutcome => (
                    "recovery_required",
                    None,
                    Some(
                        "Worker Workflow crossed a provider boundary without an exact committed outcome",
                    ),
                ),
                WorkerWorkflowProviderRecovery::CommittedOutcomeUnaccounted => (
                    "recovery_required",
                    None,
                    Some(
                        "Worker Workflow has a committed outcome that is not fully provider-accounted",
                    ),
                ),
                WorkerWorkflowProviderRecovery::NotWorkerWorkflow => (
                    "recovery_required",
                    None,
                    Some("Worker Workflow provider recovery binding is invalid"),
                ),
            }
        } else if status == "running" {
            ("recovery_required", None, None)
        } else if archive {
            if status == "recovery_required" {
                ("recovery_required", None, None)
            } else {
                ("cancelled", None, None)
            }
        } else if status == "leased" {
            ("queued", None, None)
        } else {
            continue;
        };
        if worker_workflow && status == "running" && next_status == "recovery_required" {
            terminalize_unresolved_worker_workflow_provider_calls(
                tx,
                &run_id,
                now,
                run_last_error
                    .unwrap_or("Worker Workflow lifecycle transition requires explicit recovery"),
            )?;
        }
        let reason = if worker_workflow && next_status == "succeeded" {
            "Canonical Worker Workflow outcome adopted during lifecycle transition"
        } else if introduction_review && next_status == "succeeded" {
            "Canonical Worker Introduction review audit adopted during lifecycle transition"
        } else if introduction_review && next_status == "failed" {
            "Terminal Worker Introduction review failure adopted during lifecycle transition"
        } else if uncommitted_opening && next_status == "recovery_required" {
            "Worker lifecycle changed before the Introduction greeting committed"
        } else if archive {
            "Worker archived"
        } else if status == "running" {
            "Worker paused during execution"
        } else {
            "Worker paused before execution started"
        };
        if let Some(lease_token) = lease_token.as_deref() {
            let attempt_outcome = match next_status {
                "queued" => "abandoned",
                "recovery_required" => "recovery_required",
                "succeeded" => "succeeded",
                "failed" => "failed",
                _ => "cancelled",
            };
            tx.execute(
                "UPDATE hive_run_attempts
                 SET finished_at = COALESCE(finished_at, ?5),
                     outcome = CASE WHEN finished_at IS NULL THEN ?4 ELSE outcome END,
                     stop_reason = CASE WHEN finished_at IS NULL THEN ?6 ELSE stop_reason END,
                     error = CASE
                         WHEN finished_at IS NULL
                          AND ?4 IN ('failed', 'recovery_required') THEN ?6
                         ELSE error END
                 WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3",
                params![
                    run_id,
                    attempt_count,
                    lease_token,
                    attempt_outcome,
                    now,
                    reason
                ],
            )?;
        }
        let finished_at = if matches!(next_status, "cancelled" | "succeeded" | "failed") {
            Some(now)
        } else {
            None
        };
        let run_changed = tx.execute(
            "UPDATE hive_runs
             SET status = ?2, lease_owner = NULL, lease_token = NULL,
                 lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                 available_at = CASE WHEN ?2 = 'queued' THEN ?3 ELSE available_at END,
                 wake_at = CASE WHEN ?2 IN ('queued', 'cancelled', 'recovery_required')
                     THEN NULL ELSE wake_at END,
                 last_stop_reason = ?4,
                 last_error = CASE
                     WHEN ?2 = 'succeeded' THEN NULL
                     WHEN ?8 IS NOT NULL THEN ?8
                     ELSE last_error END,
                 outcome_json = COALESCE(?9, outcome_json),
                 finished_at = CASE WHEN ?5 IS NULL THEN finished_at
                                    ELSE COALESCE(finished_at, ?5) END,
                 updated_at = ?3
             WHERE id = ?1 AND worker_id = ?6 AND status = ?7",
            params![
                run_id,
                next_status,
                now,
                reason,
                finished_at,
                worker_id,
                status,
                run_last_error,
                run_outcome_json,
            ],
        )?;
        if run_changed != 1 {
            return Err(RuntimeStoreError::StateConflict(format!(
                "Worker run {run_id} changed during lifecycle transition"
            )));
        }
        if introduction_review {
            match next_status {
                "cancelled" if !review_provider_started => {
                    tx.execute(
                        "UPDATE hive_worker_introduction_reviews
                         SET status = 'stale',
                             last_error = 'pre-provider stale: Worker archived before Introduction review admission',
                             completed_at = ?2, updated_at = ?2
                         WHERE run_id = ?1 AND status IN ('queued', 'claimed')
                           AND provider_call_id IS NULL",
                        params![run_id, now],
                    )?;
                }
                "recovery_required" => {
                    tx.execute(
                        "UPDATE hive_worker_introduction_reviews
                         SET status = 'failed', last_error = ?2,
                             completed_at = ?3, updated_at = ?3
                         WHERE run_id = ?1 AND status IN ('queued', 'claimed')",
                        params![
                            run_id,
                            "Worker lifecycle changed after Introduction review provider admission; explicit recovery is required",
                            now,
                        ],
                    )?;
                }
                _ => {}
            }
        }
        if matches!(
            next_status,
            "cancelled" | "failed" | "succeeded" | "recovery_required"
        ) {
            tx.execute(
                "UPDATE hive_control_outbox
                 SET status = 'discarded', last_error = ?2, updated_at = ?3
                 WHERE run_id = ?1 AND status = 'pending'",
                params![run_id, reason, now],
            )?;
        }
        if uncommitted_opening && next_status == "recovery_required" {
            tx.execute(
                "UPDATE hive_worker_introductions
                 SET status = 'needs_recovery', last_error = ?2, updated_at = ?3
                 WHERE worker_id = ?1 AND run_id = ?4
                   AND opening_message_id IS NULL
                   AND status IN ('queued', 'running', 'failed', 'needs_recovery')",
                params![worker_id, reason, now, run_id],
            )?;
        }
        if status == "running" {
            cancellations.push(WorkerRunCancellation {
                worker_id: worker_id.to_string(),
                session_id: controller.session_id.clone(),
                run_id: run_id.clone(),
                reason: reason.to_string(),
            });
        }
        align_run_projection(tx, &run_id, next_status, now)?;
    }
    Ok(())
}

fn terminalize_unresolved_worker_workflow_provider_calls(
    tx: &Transaction<'_>,
    run_id: &str,
    now: &str,
    reason: &str,
) -> Result<(), RuntimeStoreError> {
    tx.execute(
        "INSERT OR IGNORE INTO hive_worker_provider_call_outcomes (
             provider_call_id, state, outcome, remote_acceptance,
             unknown_reason, finished_at
         )
         SELECT call.provider_call_id, 'unknown',
                'worker_workflow_interrupted', 'possibly_sent', ?3, ?2
         FROM hive_worker_provider_calls call
         WHERE call.run_id = ?1
           AND NOT EXISTS (
               SELECT 1 FROM hive_worker_provider_call_outcomes terminal
               WHERE terminal.provider_call_id = call.provider_call_id
           )",
        params![run_id, now, reason],
    )?;
    Ok(())
}

fn worker_lane_attention(
    tx: &Transaction<'_>,
    worker_id: &str,
) -> Result<Vec<WorkerLaneAttention>, RuntimeStoreError> {
    let mut statement = tx.prepare(
        "SELECT controller.id, controller.session_id, run.id
         FROM hive_controllers controller
         JOIN hive_runs run ON run.controller_id = controller.id
         WHERE controller.worker_id = ?1 AND run.worker_id = ?1
           AND run.status = 'recovery_required'
         ORDER BY controller.session_id ASC, run.updated_at ASC, run.id ASC",
    )?;
    let rows = statement
        .query_map([worker_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    let mut attention = Vec::<WorkerLaneAttention>::new();
    for (controller_id, session_id, run_id) in rows {
        if let Some(existing) = attention
            .iter_mut()
            .find(|entry| entry.controller_id == controller_id)
        {
            existing.recovery_run_ids.push(run_id);
        } else {
            attention.push(WorkerLaneAttention {
                session_id,
                controller_id,
                recovery_run_ids: vec![run_id],
                reason: "A prior Worker run has uncertain effects and requires explicit recovery"
                    .into(),
            });
        }
    }
    Ok(attention)
}

fn start_session(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
) -> Result<Mutation, RuntimeStoreError> {
    let session = require_owned_session(tx, actor, session_id)?;
    reject_generic_worker_session_control(tx, session_id)?;
    enforce_worker_introduction_autonomy_gate(tx, &session)?;
    freeze_session_model_into_open_runs(tx, &session, now)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    if let Some((run_id, attempt_count)) = recovery_required_run(tx, &controller.id)? {
        // Never create or wake sibling work while a prior attempt has
        // uncertain side effects. Cancellation is the explicit abandon path;
        // until then the controller remains fenced and the projection stays
        // visibly in error.
        tx.execute(
            "UPDATE hive_controllers SET status = 'paused', updated_at = ?2 WHERE id = ?1",
            params![controller.id, now],
        )?;
        tx.execute(
            "INSERT INTO hive_runtime_state (session_id, status, current_run_id, updated_at)
             VALUES (?1, 'error', ?2, ?3)
             ON CONFLICT(session_id) DO UPDATE SET status = 'error',
                 current_run_id = excluded.current_run_id,
                 updated_at = excluded.updated_at",
            params![session_id, run_id, now],
        )?;
        let event = append_event(
            tx,
            &controller,
            "start_blocked_recovery_required",
            Some(&run_id),
            None,
            Some(&format!("recovery_block:{run_id}:{attempt_count}:start")),
            serde_json::json!({
                "run_id": run_id,
                "attempt_no": attempt_count,
                "resolution": "cancel the uncertain run to abandon it before starting new work"
            }),
            now,
        )?;
        return Ok(session_mutation(
            session_id,
            "recovery_required",
            Some(run_id),
            vec![event],
        ));
    }
    // Surfacing an uncertain prior attempt above takes precedence; everything
    // past this point wakes or creates work, which needs a claimable workspace.
    require_session_workspace(&session)?;
    tx.execute(
        "UPDATE hive_controllers SET status = 'active', updated_at = ?2 WHERE id = ?1",
        params![controller.id, now],
    )?;
    let existing: Option<(String, String, i64)> = tx
        .query_row(
            "SELECT id, status, attempt_count FROM hive_runs
             WHERE controller_id = ?1
               AND status IN ('queued', 'leased', 'running', 'sleeping', 'retry_wait', 'awaiting_input')
             ORDER BY created_at DESC LIMIT 1",
            [&controller.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (run_id, event_type, dedupe_key) = if let Some((existing, status, attempt_count)) = existing
    {
        if matches!(
            status.as_str(),
            "sleeping" | "retry_wait" | "awaiting_input"
        ) {
            tx.execute(
                "UPDATE hive_runs
                 SET status = 'queued', available_at = ?2, wake_at = NULL,
                     last_stop_reason = NULL, updated_at = ?2
                 WHERE id = ?1 AND status IN ('sleeping', 'retry_wait', 'awaiting_input')",
                params![existing, now],
            )?;
            align_run_projection(tx, &existing, "queued", now)?;
            let dedupe_key = format!("transition:{existing}:{attempt_count}:queued");
            (existing, "run_requeued", Some(dedupe_key))
        } else {
            (existing, "run_start_requested", None)
        }
    } else {
        let run_id = uuid::Uuid::new_v4().to_string();
        let objective = tx
            .query_row(
                "SELECT content FROM messages
                 WHERE session_id = ?1 AND role = 'user' ORDER BY id DESC LIMIT 1",
                [session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|content| canonical_text(&content))
            .unwrap_or_else(|| format!("Continue {}", session.title));
        insert_run(
            tx,
            &run_id,
            &controller.id,
            Some(session_id),
            None,
            None,
            "legacy_resume",
            &objective,
            serde_json::json!({
                "working_dir": session.working_dir,
                "project_dir": session.project_dir,
                "model": session.model,
                "model_key": session.model_key,
                "model_catalog_revision": session.model_catalog_revision,
                "permission_mode": require_frozen_session_permission_mode(&session)?,
                "retry": RetryPolicy::default(),
            }),
            0,
            now,
            5,
            now,
            None,
        )?;
        let dedupe_key = format!("run:{run_id}:queued");
        (run_id, "run_queued", Some(dedupe_key))
    };
    let event = append_event(
        tx,
        &controller,
        event_type,
        Some(&run_id),
        None,
        dedupe_key.as_deref(),
        serde_json::json!({"run_id": run_id, "kind": "legacy_resume"}),
        now,
    )?;
    Ok(session_mutation(
        session_id,
        "queued",
        Some(run_id),
        vec![event],
    ))
}

fn reject_generic_worker_session_control(
    tx: &Transaction<'_>,
    session_id: &str,
) -> Result<(), RuntimeStoreError> {
    let binding = resolve_worker_conversation_with_conn(tx, session_id)
        .map_err(RuntimeStoreError::Internal)?;
    let Some(binding) = binding else {
        return Ok(());
    };
    if binding.group_id.is_some() {
        return Err(RuntimeStoreError::NotFound(
            "Hive session was not found".into(),
        ));
    }
    Err(RuntimeStoreError::StateConflict(
        "Hive Worker direct messages use typed Worker chat, schedule, Goal, and lifecycle controls"
            .into(),
    ))
}

fn schedule_session(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    wake_at_unix_ms: i64,
    reason: &str,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(reason, "schedule reason", 8 * 1024)?;
    let session = require_owned_session(tx, actor, session_id)?;
    reject_generic_worker_session_control(tx, session_id)?;
    enforce_worker_introduction_autonomy_gate(tx, &session)?;
    let _ = require_frozen_session_model(&session)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let wake_at = unix_millis_to_utc(wake_at_unix_ms)?;
    let schedule_id = uuid::Uuid::new_v4().to_string();
    let recurrence = serde_json::to_string(&RecurrenceV1::Once { at: wake_at })
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let wake_at = canonical_timestamp(wake_at);
    tx.execute(
        "INSERT INTO hive_schedules (
            id, controller_id, title, summary, objective, recurrence_kind,
            recurrence_json, timezone, gap_policy, fold_policy, next_fire_at,
            last_scheduled_for, status, priority, project_dir, model,
            model_key_json, model_catalog_revision, crew_slug,
            misfire_policy, misfire_grace_secs, catch_up_limit, overlap_policy,
            max_attempts, retry_base_secs, retry_max_secs, retry_jitter,
            revision, created_by, created_at, updated_at
         ) SELECT ?1, ?2, ?3, ?3, ?3, 'once', ?4, ?5, 'shift_forward', 'first',
                  ?6, NULL, 'enabled', 0, s.project_dir, s.model,
                  s.model_key_json, s.model_catalog_revision, rs.crew_slug,
                  'fire_once', 300, 1, 'queue_one', 5, 15, 900, 'full', 0,
                  ?7, ?8, ?8
           FROM sessions s
           LEFT JOIN hive_runtime_state rs ON rs.session_id = s.id
          WHERE s.id = ?9",
        params![
            schedule_id,
            controller.id,
            reason,
            recurrence,
            controller.timezone,
            wake_at,
            actor.user_id.as_deref().unwrap_or("local"),
            now,
            session_id,
        ],
    )?;
    let event = append_event(
        tx,
        &controller,
        "schedule_created",
        None,
        Some(&schedule_id),
        Some(&format!("schedule:{schedule_id}:created")),
        serde_json::json!({"schedule_id": schedule_id, "next_fire_at": wake_at}),
        now,
    )?;
    Ok(session_mutation(
        session_id,
        "scheduled",
        Some(schedule_id),
        vec![event],
    ))
}

fn set_controller_status(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    status: &str,
) -> Result<Mutation, RuntimeStoreError> {
    let session = require_owned_session(tx, actor, session_id)?;
    reject_generic_worker_session_control(tx, session_id)?;
    if status == "active" {
        enforce_worker_introduction_autonomy_gate(tx, &session)?;
        freeze_session_model_into_open_runs(tx, &session, now)?;
    }
    let mut controller = get_or_create_controller(tx, &session, now)?;
    let previous = controller.status.clone();
    if status == "active" {
        if let Some((run_id, attempt_count)) = recovery_required_run(tx, &controller.id)? {
            tx.execute(
                "UPDATE hive_controllers SET status = 'paused', updated_at = ?2 WHERE id = ?1",
                params![controller.id, now],
            )?;
            controller.status = "paused".into();
            tx.execute(
                "INSERT INTO hive_runtime_state (session_id, status, current_run_id, updated_at)
                 VALUES (?1, 'error', ?2, ?3)
                 ON CONFLICT(session_id) DO UPDATE SET status = 'error',
                     current_run_id = excluded.current_run_id,
                     updated_at = excluded.updated_at",
                params![session_id, run_id, now],
            )?;
            let event = append_event(
                tx,
                &controller,
                "resume_blocked_recovery_required",
                Some(&run_id),
                None,
                Some(&format!("recovery_block:{run_id}:{attempt_count}:resume")),
                serde_json::json!({
                    "run_id": run_id,
                    "attempt_no": attempt_count,
                    "previous": previous,
                    "current": "paused",
                    "resolution": "cancel the uncertain run to abandon it before resuming"
                }),
                now,
            )?;
            return Ok(session_mutation(
                session_id,
                "recovery_required",
                Some(run_id),
                vec![event],
            ));
        }
    }
    tx.execute(
        "UPDATE hive_controllers SET status = ?2, updated_at = ?3 WHERE id = ?1",
        params![controller.id, status, now],
    )?;
    controller.status = status.to_string();
    let active_runs = if status == "paused" {
        let mut statement = tx.prepare(
            "SELECT id, status, attempt_count, lease_token FROM hive_runs
             WHERE controller_id = ?1 AND status IN ('leased', 'running')",
        )?;
        let active = statement
            .query_map([&controller.id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        active
    } else {
        Vec::new()
    };
    for (run_id, run_status, attempt_no, lease_token) in &active_runs {
        let (target, reason) = if run_status == "leased" {
            ("queued", "paused before execution; safely requeued")
        } else {
            (
                "recovery_required",
                "paused during execution; side effects may be uncertain",
            )
        };
        tx.execute(
            "UPDATE hive_runs
             SET status = ?2, lease_owner = NULL, lease_token = NULL,
                 lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                 last_stop_reason = 'paused by user', last_error = ?3, updated_at = ?4
             WHERE id = ?1 AND status = ?5",
            params![run_id, target, reason, now, run_status],
        )?;
        if let Some(lease_token) = lease_token {
            tx.execute(
                "UPDATE hive_run_attempts
                 SET finished_at = ?4, outcome = 'abandoned', stop_reason = 'paused by user',
                     error = ?5
                 WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                   AND finished_at IS NULL",
                params![run_id, attempt_no, lease_token, now, reason],
            )?;
        }
        if target == "recovery_required" {
            tx.execute(
                "UPDATE hive_control_outbox
                 SET status = 'discarded',
                     last_error = 'run entered recovery before control delivery',
                     updated_at = ?2
                 WHERE run_id = ?1 AND status = 'pending'",
                params![run_id, now],
            )?;
            tx.execute(
                "UPDATE hive_schedule_occurrences SET status = 'failed',
                     decision_reason = ?2, updated_at = ?3
                 WHERE run_id = ?1 AND status = 'running'",
                params![run_id, reason, now],
            )?;
        }
    }
    tx.execute(
        "INSERT INTO hive_runtime_state (session_id, status, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(session_id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at",
        params![session_id, if status == "paused" { "paused" } else { "idle" }, now],
    )?;
    let event_type = if status == "paused" {
        "controller_paused"
    } else {
        "controller_started"
    };
    let event = append_event(
        tx,
        &controller,
        event_type,
        None,
        None,
        None,
        serde_json::json!({"previous": previous, "current": status}),
        now,
    )?;
    let mut events = vec![event];
    for (run_id, run_status, attempt_count, _) in active_runs {
        let (event_type, reason, target_status) = if run_status == "leased" {
            (
                "run_lease_requeued",
                "paused before execution; safely requeued",
                "queued",
            )
        } else {
            (
                "recovery_required",
                "paused during execution; side effects may be uncertain",
                "recovery_required",
            )
        };
        let dedupe_key = format!("transition:{run_id}:{attempt_count}:{target_status}");
        events.push(append_event(
            tx,
            &controller,
            event_type,
            Some(&run_id),
            None,
            Some(&dedupe_key),
            serde_json::json!({"run_id": run_id, "reason": reason}),
            now,
        )?);
    }
    Ok(session_mutation(session_id, status, None, events))
}

fn stop_worker_conversation(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
) -> Result<Mutation, RuntimeStoreError> {
    let (session, binding) = require_exact_owned_worker_dm(tx, actor, session_id)?;
    let worker = binding.worker;
    let active_runs = {
        let mut statement = tx.prepare(
            "SELECT run.id, run.status, run.attempt_count, run.lease_token,
                    run.kind, run.schedule_id,
                    run.group_id, run.governor_origin, run.governor_lane_key,
                    run.execution_context_json,
                    run.response_message_id, run.response_group_message_id,
                    run.response_provider_call_id,
                    controller.id, controller.status, controller.timezone
             FROM hive_runs run
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE run.session_id = ?1 AND run.worker_id = ?2
               AND controller.worker_id = run.worker_id
               AND run.status IN ('queued', 'leased', 'running', 'sleeping', 'retry_wait')
               AND (
                   run.status IN ('leased', 'running')
                   OR (
                       run.kind = 'worker_conversation'
                       AND run.schedule_id IS NULL AND run.group_id IS NULL
                       AND run.governor_origin = 'user_dm'
                       AND run.governor_lane_key = 'dm'
                       AND json_valid(run.execution_context_json)
                       AND json_extract(run.execution_context_json, '$.mode.kind')
                           IN ('worker_conversation_neutral', 'worker_workspace_attached')
                       AND json_extract(run.execution_context_json, '$.mode.lane.kind')
                           = 'direct_message'
                       AND json_extract(run.execution_context_json, '$.mode.worker_id')
                           = run.worker_id
                       AND json_extract(run.execution_context_json, '$.mode.worker_revision')
                           = ?3
                   )
               )
             ORDER BY CASE run.status WHEN 'running' THEN 0 ELSE 1 END,
                      run.updated_at DESC, run.id DESC
             LIMIT 2",
        )?;
        let rows = statement
            .query_map(params![session_id, worker.id, worker.revision], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    ControllerRecord {
                        id: row.get(13)?,
                        session_id: session_id.to_string(),
                        status: row.get(14)?,
                        timezone: row.get(15)?,
                    },
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    if active_runs.len() > 1 {
        return Err(RuntimeStoreError::StateConflict(
            "Worker direct-message lane has more than one active run".into(),
        ));
    }
    let Some((
        run_id,
        status,
        attempt_count,
        lease_token,
        kind,
        schedule_id,
        group_id,
        governor_origin,
        governor_lane_key,
        execution_context_json,
        response_message_id,
        response_group_message_id,
        response_provider_call_id,
        controller,
    )) = active_runs.into_iter().next()
    else {
        return Ok(Mutation {
            response: ResponsePayload::WorkerMutation(WorkerMutationResponse {
                worker_id: worker.id.clone(),
                revision: worker.revision,
                status: worker.status.as_str().into(),
                cancellation_requests: Vec::new(),
                attention: worker_lane_attention(tx, &worker.id)?,
            }),
            resource_id: Some(worker.id),
            events: Vec::new(),
        });
    };

    let exact_direct_conversation = execution_context_json
        .as_deref()
        .and_then(|encoded| serde_json::from_str::<HiveRunExecutionContextV1>(encoded).ok())
        .is_some_and(|context| match context.mode {
            HiveRunExecutionModeV1::WorkerConversationNeutral {
                worker_id,
                worker_revision,
                lane: WorkerConversationLane::DirectMessage,
            } => worker_id == worker.id && worker_revision == worker.revision,
            HiveRunExecutionModeV1::WorkerWorkspaceAttached {
                worker_id,
                worker_revision,
                lane: WorkerConversationLane::DirectMessage,
                working_dir,
                project_dir,
                ..
            } => {
                worker_id == worker.id
                    && worker_revision == worker.revision
                    && session.working_dir.as_deref() == Some(working_dir.as_str())
                    && session.project_dir.as_deref() == project_dir.as_deref()
            }
            _ => false,
        });
    if kind != "worker_conversation"
        || schedule_id.is_some()
        || group_id.is_some()
        || governor_origin.as_deref() != Some("user_dm")
        || governor_lane_key.as_deref() != Some("dm")
        || !exact_direct_conversation
    {
        return Err(RuntimeStoreError::StateConflict(
            "Worker Stop applies only to the current ordinary direct-chat run".into(),
        ));
    }
    if response_message_id.is_some()
        || response_group_message_id.is_some()
        || response_provider_call_id.is_some()
    {
        return Ok(Mutation {
            response: ResponsePayload::WorkerMutation(WorkerMutationResponse {
                worker_id: worker.id.clone(),
                revision: worker.revision,
                status: worker.status.as_str().into(),
                cancellation_requests: Vec::new(),
                attention: worker_lane_attention(tx, &worker.id)?,
            }),
            resource_id: Some(worker.id),
            events: Vec::new(),
        });
    }

    let mut events = Vec::new();
    let mut cancellations = Vec::new();
    match status.as_str() {
        "queued" | "leased" | "sleeping" | "retry_wait" => {
            if status == "leased" && lease_token.is_none() {
                return Err(RuntimeStoreError::StateConflict(
                    "leased Worker conversation run has no lease token".into(),
                ));
            }
            let changed = tx.execute(
                "UPDATE hive_runs
                 SET status = 'cancelled', lease_owner = NULL, lease_token = NULL,
                     lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                     wake_at = NULL, last_stop_reason = ?7,
                     finished_at = ?6, updated_at = ?6
                 WHERE id = ?1 AND session_id = ?2 AND worker_id = ?3
                   AND controller_id = ?4 AND status = ?5
                   AND kind = 'worker_conversation'
                   AND schedule_id IS NULL AND group_id IS NULL
                   AND governor_origin = 'user_dm' AND governor_lane_key = 'dm'
                   AND response_message_id IS NULL
                   AND response_group_message_id IS NULL
                   AND response_provider_call_id IS NULL
                   AND (?5 <> 'leased' OR lease_token = ?8)",
                params![
                    run_id,
                    session_id,
                    worker.id,
                    controller.id,
                    status,
                    now,
                    WORKER_CONVERSATION_STOP_REQUESTED_REASON,
                    lease_token,
                ],
            )?;
            if changed != 1 {
                return Err(RuntimeStoreError::StateConflict(
                    "Worker conversation run changed while Stop was committing".into(),
                ));
            }
            if let Some(lease_token) = lease_token.as_deref() {
                tx.execute(
                    "UPDATE hive_run_attempts
                     SET finished_at = ?4, outcome = 'cancelled', stop_reason = ?5
                     WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                       AND finished_at IS NULL",
                    params![
                        run_id,
                        attempt_count,
                        lease_token,
                        now,
                        WORKER_CONVERSATION_STOP_REQUESTED_REASON
                    ],
                )?;
            }
            tx.execute(
                "UPDATE hive_schedule_occurrences
                 SET status = 'cancelled', decision_reason = ?3,
                     updated_at = ?2
                 WHERE run_id = ?1 AND status IN ('pending', 'queued', 'running')",
                params![run_id, now, WORKER_CONVERSATION_STOP_REQUESTED_REASON],
            )?;
            tx.execute(
                "UPDATE hive_control_outbox
                 SET status = 'discarded', last_error = ?3,
                     updated_at = ?2
                 WHERE run_id = ?1 AND status = 'pending'",
                params![run_id, now, WORKER_CONVERSATION_STOP_REQUESTED_REASON],
            )?;
            tx.execute(
                "UPDATE hive_runtime_state
                 SET status = 'idle', current_run_id = NULL, updated_at = ?2
                 WHERE session_id = ?1 AND current_run_id = ?3",
                params![session_id, now, run_id],
            )?;
            events.push(append_event(
                tx,
                &controller,
                "run_cancelled",
                Some(&run_id),
                schedule_id.as_deref(),
                Some(&format!("transition:{run_id}:{attempt_count}:cancelled")),
                serde_json::json!({
                    "run_id": run_id,
                    "reason": WORKER_CONVERSATION_STOP_REQUESTED_REASON,
                    "kind": kind,
                }),
                now,
            )?);
            materialize_oldest_staged_input_with_authority_in_transaction(
                tx,
                &run_id,
                WorkerConversationPredecessorAuthority::StoppedWorkerConversation,
                now,
            )
            .map_err(RuntimeStoreError::Internal)?;
        }
        "running" => {
            let changed = tx.execute(
                "UPDATE hive_runs
                 SET last_stop_reason = ?5, updated_at = ?6
                 WHERE id = ?1 AND session_id = ?2 AND worker_id = ?3
                   AND controller_id = ?4 AND status = 'running'
                   AND kind = 'worker_conversation'
                   AND schedule_id IS NULL AND group_id IS NULL
                   AND governor_origin = 'user_dm' AND governor_lane_key = 'dm'
                   AND response_message_id IS NULL
                   AND response_group_message_id IS NULL
                   AND response_provider_call_id IS NULL",
                params![
                    run_id,
                    session_id,
                    worker.id,
                    controller.id,
                    WORKER_CONVERSATION_STOP_REQUESTED_REASON,
                    now
                ],
            )?;
            if changed != 1 {
                return Err(RuntimeStoreError::StateConflict(
                    "Worker conversation run changed while Stop was committing".into(),
                ));
            }
            events.push(append_event(
                tx,
                &controller,
                "worker_run_stop_requested",
                Some(&run_id),
                schedule_id.as_deref(),
                Some(&format!(
                    "worker-run-stop-requested:{run_id}:{attempt_count}"
                )),
                serde_json::json!({
                    "run_id": run_id,
                    "attempt": attempt_count,
                    "kind": kind,
                }),
                now,
            )?);
            cancellations.push(WorkerRunCancellation {
                worker_id: worker.id.clone(),
                session_id: session_id.to_string(),
                run_id,
                reason: "cancelled by user".into(),
            });
        }
        _ => unreachable!("active Worker Stop query returned a terminal run"),
    }

    Ok(Mutation {
        response: ResponsePayload::WorkerMutation(WorkerMutationResponse {
            worker_id: worker.id.clone(),
            revision: worker.revision,
            status: worker.status.as_str().into(),
            cancellation_requests: cancellations,
            attention: worker_lane_attention(tx, &worker.id)?,
        }),
        resource_id: Some(worker.id),
        events,
    })
}

fn cancel_session(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
) -> Result<Mutation, RuntimeStoreError> {
    let session = require_owned_session(tx, actor, session_id)?;
    match resolve_worker_conversation_with_conn(tx, session_id)
        .map_err(RuntimeStoreError::Internal)?
    {
        Some(binding)
            if binding.group_id.is_some()
                || binding.worker.user_id != actor.user_id
                || (binding.worker.dm_session_id.as_deref() != Some(session_id)) =>
        {
            return Err(RuntimeStoreError::NotFound(
                "Hive session was not found".into(),
            ));
        }
        Some(_) => {
            return Err(RuntimeStoreError::StateConflict(
                "Worker conversations require typed Worker Stop; generic session cancellation is not allowed".into(),
            ));
        }
        None => {}
    }
    let hidden_worker_lane: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM hive_group_worker_lanes WHERE session_id = ?1
             UNION ALL
             SELECT 1 FROM hive_controllers
             WHERE session_id = ?1 AND worker_id IS NOT NULL
             UNION ALL
             SELECT 1 FROM hive_runs
             WHERE session_id = ?1 AND worker_id IS NOT NULL
         )",
        [session_id],
        |row| row.get(0),
    )?;
    if hidden_worker_lane {
        return Err(RuntimeStoreError::NotFound(
            "Hive session was not found".into(),
        ));
    }
    let controller = get_or_create_controller(tx, &session, now)?;
    let cancellable = {
        let mut statement = tx.prepare(
            "SELECT id, attempt_count, schedule_id, status, lease_token FROM hive_runs
             WHERE controller_id = ?1
               AND status IN ('queued', 'leased', 'running', 'sleeping', 'retry_wait', 'awaiting_input', 'recovery_required')",
        )?;
        let rows = statement
            .query_map([&controller.id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    tx.execute(
        "UPDATE hive_control_outbox
         SET status = 'discarded', last_error = 'session cancelled', updated_at = ?2
         WHERE controller_id = ?1 AND status = 'pending'",
        params![controller.id, now],
    )?;
    tx.execute(
        "UPDATE hive_schedule_occurrences
         SET status = 'cancelled', decision_reason = 'cancelled by user', updated_at = ?2
         WHERE run_id IN (
             SELECT id FROM hive_runs WHERE controller_id = ?1
               AND status IN ('queued', 'leased', 'sleeping', 'retry_wait', 'awaiting_input', 'recovery_required')
         ) AND status IN ('pending', 'queued', 'running')",
        params![controller.id, now],
    )?;
    tx.execute(
        "UPDATE hive_runs
         SET status = 'cancelled', lease_owner = NULL, lease_token = NULL,
             lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
             wake_at = NULL, last_stop_reason = 'cancelled by user',
             finished_at = ?2, updated_at = ?2
         WHERE controller_id = ?1
           AND status IN ('queued', 'leased', 'sleeping', 'retry_wait', 'awaiting_input', 'recovery_required')",
        params![controller.id, now],
    )?;
    tx.execute(
        "UPDATE hive_schedules SET status = 'cancelled', revision = revision + 1,
                 updated_at = ?2
         WHERE controller_id = ?1 AND status IN ('enabled', 'paused')",
        params![controller.id, now],
    )?;
    tx.execute(
        "UPDATE hive_controllers SET status = 'disabled', updated_at = ?2 WHERE id = ?1",
        params![controller.id, now],
    )?;
    tx.execute(
        "INSERT INTO hive_runtime_state (session_id, status, updated_at)
         VALUES (?1, 'cancelled', ?2)
         ON CONFLICT(session_id) DO UPDATE SET status = 'cancelled', updated_at = excluded.updated_at",
        params![session_id, now],
    )?;
    let mut events = Vec::with_capacity(cancellable.len() + 1);
    for (run_id, attempt_count, schedule_id, status, lease_token) in cancellable {
        if status == "leased" {
            if let Some(lease_token) = lease_token {
                tx.execute(
                    "UPDATE hive_run_attempts
                     SET finished_at = ?4, outcome = 'cancelled',
                         stop_reason = 'cancelled by user'
                     WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3
                       AND finished_at IS NULL",
                    params![run_id, attempt_count, lease_token, now],
                )?;
            }
        }
        if status == "running" {
            events.push(append_event(
                tx,
                &controller,
                "cancellation_requested",
                Some(&run_id),
                schedule_id.as_deref(),
                Some(&format!("cancel_requested:{run_id}:{attempt_count}")),
                serde_json::json!({"run_id": run_id, "attempt": attempt_count}),
                now,
            )?);
        } else {
            let dedupe_key = format!("transition:{run_id}:{attempt_count}:cancelled");
            events.push(append_event(
                tx,
                &controller,
                "run_cancelled",
                Some(&run_id),
                schedule_id.as_deref(),
                Some(&dedupe_key),
                serde_json::json!({"run_id": run_id, "reason": "cancelled by user"}),
                now,
            )?);
        }
    }
    events.push(append_event(
        tx,
        &controller,
        "session_cancelled",
        None,
        None,
        None,
        serde_json::json!({"reason": "cancelled by user"}),
        now,
    )?);
    Ok(session_mutation(session_id, "cancelled", None, events))
}

fn delete_session(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
) -> Result<Mutation, RuntimeStoreError> {
    let session = require_owned_session(tx, actor, session_id)?;
    match resolve_worker_conversation_with_conn(tx, session_id)
        .map_err(RuntimeStoreError::Internal)?
    {
        Some(binding)
            if binding.group_id.is_some()
                || binding.worker.user_id != actor.user_id
                || binding.worker.dm_session_id.as_deref() != Some(session_id) =>
        {
            return Err(RuntimeStoreError::NotFound(
                "Hive session was not found".into(),
            ));
        }
        Some(_) => {
            return Err(RuntimeStoreError::StateConflict(
                "Worker conversations are durable product lanes; archive the Worker instead of deleting this session".into(),
            ));
        }
        None => {}
    }
    let hidden_worker_lane: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM hive_group_worker_lanes WHERE session_id = ?1
             UNION ALL
             SELECT 1 FROM hive_controllers
             WHERE session_id = ?1 AND worker_id IS NOT NULL
             UNION ALL
             SELECT 1 FROM hive_runs
             WHERE session_id = ?1 AND worker_id IS NOT NULL
         )",
        [session_id],
        |row| row.get(0),
    )?;
    if hidden_worker_lane {
        return Err(RuntimeStoreError::NotFound(
            "Hive session was not found".into(),
        ));
    }
    let controller = get_or_create_controller(tx, &session, now)?;
    let has_active_run = tx.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM hive_runs
             WHERE controller_id = ?1 AND status IN ('leased', 'running', 'recovery_required')
         )",
        [&controller.id],
        |row| row.get::<_, bool>(0),
    )?;
    if has_active_run {
        return Err(RuntimeStoreError::StateConflict(
            "session has an active run; cancel it and wait for quiescence before deleting".into(),
        ));
    }
    let event = append_event(
        tx,
        &controller,
        "session_deleted",
        None,
        None,
        None,
        serde_json::json!({"deleted": true}),
        now,
    )?;
    tx.execute("DELETE FROM sessions WHERE id = ?1", [session_id])?;
    Ok(Mutation {
        response: ack("session deleted"),
        resource_id: Some(session_id.to_string()),
        events: vec![event],
    })
}

/// Fence every user-content ingress against the one-time Worker Introduction.
///
/// This runs inside the daemon's existing mutation transaction, before any
/// controller, message, pending-content, or run mutation. SQLite therefore
/// serializes it with the assistant opening and reviewed-proposal transitions:
/// either that lifecycle change commits first and this command observes it, or
/// this command commits its allowed content first and the reviewer must build
/// from the newer transcript.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerIntroductionContentGate {
    Ordinary,
    AwaitingContext,
}

fn enforce_worker_introduction_content_gate(
    tx: &Transaction<'_>,
    session: &super::persistence::OwnedSession,
) -> Result<WorkerIntroductionContentGate, RuntimeStoreError> {
    let binding = resolve_worker_conversation_with_conn(tx, &session.id)
        .map_err(RuntimeStoreError::Internal)?;
    let Some(binding) = binding else {
        // A missing typed binding is legacy-compatible only when no durable
        // controller or run claims that this is a Worker-owned lane. Malformed
        // first-class state must not silently fall back to an ordinary chat.
        let worker_lane_claimed = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM hive_controllers
                 WHERE session_id = ?1 AND worker_id IS NOT NULL
                 UNION ALL
                 SELECT 1 FROM hive_runs
                 WHERE session_id = ?1 AND worker_id IS NOT NULL
             )",
            [&session.id],
            |row| row.get::<_, bool>(0),
        )?;
        if worker_lane_claimed {
            return Err(RuntimeStoreError::StateConflict(
                "Hive Worker conversation binding is missing or inconsistent".into(),
            ));
        }
        return Ok(WorkerIntroductionContentGate::Ordinary);
    };

    if binding.worker.user_id != session.user_id {
        return Err(RuntimeStoreError::Ownership);
    }
    match binding.worker.status {
        HiveWorkerStatus::Active => {}
        HiveWorkerStatus::Paused => {
            return Err(RuntimeStoreError::StateConflict(
                "This Worker is paused; resume it before sending content".into(),
            ))
        }
        HiveWorkerStatus::Archived => {
            return Err(RuntimeStoreError::StateConflict(
                "This Worker is archived and its conversation is read-only".into(),
            ))
        }
    }
    if let Some(group_id) = binding.group_id.as_deref() {
        let exact_group_lane = tx.query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM hive_group_worker_lanes lane
                 JOIN hive_group_members member
                   ON member.group_id = lane.group_id
                  AND member.worker_id = lane.worker_id
                 JOIN hive_groups group_row ON group_row.id = lane.group_id
                 WHERE lane.session_id = ?1
                   AND lane.worker_id = ?2
                   AND lane.group_id = ?3
                   AND group_row.user_id IS ?4
             )",
            params![
                session.id,
                binding.worker.id,
                group_id,
                session.user_id.as_deref()
            ],
            |row| row.get::<_, bool>(0),
        )?;
        if !exact_group_lane {
            return Err(RuntimeStoreError::StateConflict(
                "Hive group Worker lane binding is missing or inconsistent".into(),
            ));
        }
        // Group messages have their own room lifecycle. A Worker's private DM
        // Introduction never freezes its validated group lane.
        return Ok(WorkerIntroductionContentGate::Ordinary);
    }

    if binding.worker.dm_session_id.as_deref() != Some(session.id.as_str()) {
        return Err(RuntimeStoreError::StateConflict(
            "Hive Worker direct-message binding is missing or inconsistent".into(),
        ));
    }
    let Some(introduction) = HiveWorkerIntroductionStore::from_connection(tx)
        .get_by_worker(&binding.worker.id)
        .map_err(RuntimeStoreError::Internal)?
    else {
        // Workers created before the Introduction ledger remain compatible.
        return Ok(WorkerIntroductionContentGate::Ordinary);
    };

    match introduction.status {
        HiveWorkerIntroductionStatus::AwaitingContext => {
            Ok(WorkerIntroductionContentGate::AwaitingContext)
        }
        HiveWorkerIntroductionStatus::Confirmed | HiveWorkerIntroductionStatus::Skipped => {
            Ok(WorkerIntroductionContentGate::Ordinary)
        }
        HiveWorkerIntroductionStatus::ReviewReady => Err(RuntimeStoreError::StateConflict(
            "Hive Worker Introduction context is frozen for review; confirm the proposal or choose Keep talking before sending more content".into(),
        )),
        HiveWorkerIntroductionStatus::Queued | HiveWorkerIntroductionStatus::Running => {
            Err(RuntimeStoreError::StateConflict(
                "the Hive Worker must send its first assistant Introduction before user content is accepted".into(),
            ))
        }
        HiveWorkerIntroductionStatus::Failed
        | HiveWorkerIntroductionStatus::NeedsRecovery => Err(RuntimeStoreError::StateConflict(
            "the Hive Worker Introduction needs Retry or Skip before user content is accepted".into(),
        )),
    }
}

fn enforce_worker_introduction_autonomy_gate(
    tx: &Transaction<'_>,
    session: &super::persistence::OwnedSession,
) -> Result<(), RuntimeStoreError> {
    match enforce_worker_introduction_content_gate(tx, session)? {
        WorkerIntroductionContentGate::Ordinary => Ok(()),
        WorkerIntroductionContentGate::AwaitingContext => Err(RuntimeStoreError::StateConflict(
            "complete or skip the Hive Worker Introduction before starting, scheduling, or resuming autonomous work".into(),
        )),
    }
}

fn ordinary_direct_worker_binding(
    tx: &Transaction<'_>,
    session: &super::persistence::OwnedSession,
) -> Result<Option<HiveWorkerConversationBinding>, RuntimeStoreError> {
    let binding = resolve_worker_conversation_with_conn(tx, &session.id)
        .map_err(RuntimeStoreError::Internal)?;
    if binding
        .as_ref()
        .is_some_and(|binding| binding.group_id.is_some())
    {
        return Err(RuntimeStoreError::StateConflict(
            "Hive group Worker lanes are private execution state; send user content to the Group room"
                .into(),
        ));
    }
    Ok(binding)
}

fn reject_legacy_worker_conversation_input(
    tx: &Transaction<'_>,
    actor: &Actor,
    session_id: &str,
) -> Result<(), RuntimeStoreError> {
    let session = require_owned_session(tx, actor, session_id)?;
    let binding = resolve_worker_conversation_with_conn(tx, &session.id)
        .map_err(RuntimeStoreError::Internal)?;
    if binding
        .as_ref()
        .is_some_and(|binding| binding.group_id.is_some())
    {
        return Err(RuntimeStoreError::NotFound(
            "Hive session was not found".into(),
        ));
    }
    if binding.is_some() {
        return Err(RuntimeStoreError::StateConflict(
            "Hive Worker direct messages require the typed Worker conversation protocol".into(),
        ));
    }
    let worker_lane_claimed: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM hive_group_worker_lanes WHERE session_id = ?1
             UNION ALL
             SELECT 1 FROM hive_controllers
             WHERE session_id = ?1 AND worker_id IS NOT NULL
             UNION ALL
             SELECT 1 FROM hive_runs
             WHERE session_id = ?1 AND worker_id IS NOT NULL
         )",
        [&session.id],
        |row| row.get(0),
    )?;
    if worker_lane_claimed {
        return Err(RuntimeStoreError::NotFound(
            "Hive session was not found".into(),
        ));
    }
    Ok(())
}

fn require_typed_worker_conversation_input(
    tx: &Transaction<'_>,
    actor: &Actor,
    session_id: &str,
) -> Result<(), RuntimeStoreError> {
    require_exact_owned_worker_dm(tx, actor, session_id).map(|_| ())
}

fn require_exact_owned_worker_dm(
    tx: &Transaction<'_>,
    actor: &Actor,
    session_id: &str,
) -> Result<
    (
        super::persistence::OwnedSession,
        HiveWorkerConversationBinding,
    ),
    RuntimeStoreError,
> {
    let session = require_owned_session(tx, actor, session_id)?;
    let binding = resolve_worker_conversation_with_conn(tx, &session.id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| {
            RuntimeStoreError::NotFound("Hive Worker direct message was not found".into())
        })?;
    if binding.group_id.is_some()
        || binding.worker.user_id != actor.user_id
        || binding.worker.dm_session_id.as_deref() != Some(session.id.as_str())
    {
        return Err(RuntimeStoreError::NotFound(
            "Hive Worker direct message was not found".into(),
        ));
    }
    Ok((session, binding))
}

fn resolve_worker_goal_acceptance(
    database_path: &Path,
    actor: &Actor,
    command: ResolveWorkerGoalAcceptanceCommand,
) -> Result<Mutation, RuntimeStoreError> {
    let request = UserWorkerGoalAcceptanceRequest {
        acceptance_run_id: command.acceptance_run_id,
        expected_goal_revision: command.expected_goal_revision,
        decision: match command.decision {
            WorkerGoalAcceptanceDecision::Accept => UserWorkerGoalAcceptanceDecision::Accept,
            WorkerGoalAcceptanceDecision::Reject => UserWorkerGoalAcceptanceDecision::Reject,
        },
        reason: command.reason,
        criteria: command
            .criteria
            .into_iter()
            .map(|criterion| UserGoalCriterionAcceptance {
                criterion_id: criterion.criterion_id,
                decision: match criterion.decision {
                    WorkerGoalCriterionDecision::Passed => UserGoalCriterionDecision::Passed,
                    WorkerGoalCriterionDecision::Failed => UserGoalCriterionDecision::Failed,
                    WorkerGoalCriterionDecision::Waived => UserGoalCriterionDecision::Waived,
                },
                evidence: criterion.evidence,
            })
            .collect(),
    };
    let resolution = SqliteWorkerGoalAcceptanceStore::new(database_path)
        .resolve_user(actor.user_id.as_deref(), &request)
        .map_err(map_worker_goal_acceptance_error)?;
    Ok(Mutation {
        resource_id: Some(resolution.acceptance_run_id.clone()),
        events: Vec::new(),
        response: ResponsePayload::WorkerGoalAcceptance(WorkerGoalAcceptanceResponse {
            acceptance_run_id: resolution.acceptance_run_id,
            source_run_id: resolution.source_run_id,
            workflow_goal_id: resolution.workflow_goal_id,
            source_attempt_id: resolution.source_attempt_id,
            step_id: resolution.step_id,
            decision: match resolution.decision {
                UserWorkerGoalAcceptanceDecision::Accept => WorkerGoalAcceptanceDecision::Accept,
                UserWorkerGoalAcceptanceDecision::Reject => WorkerGoalAcceptanceDecision::Reject,
            },
            goal_revision: resolution.goal_revision,
            goal_status: resolution.goal_status,
            step_status: resolution.step_status,
        }),
    })
}

fn map_worker_goal_acceptance_error(error: WorkerGoalAcceptanceStoreError) -> RuntimeStoreError {
    match error {
        WorkerGoalAcceptanceStoreError::Validation(error) => {
            RuntimeStoreError::Invalid(error.to_string())
        }
        WorkerGoalAcceptanceStoreError::NotFound(_) | WorkerGoalAcceptanceStoreError::Forbidden => {
            RuntimeStoreError::NotFound("Worker Goal acceptance was not found".into())
        }
        WorkerGoalAcceptanceStoreError::Stale(message) => {
            RuntimeStoreError::RevisionConflict(message)
        }
        WorkerGoalAcceptanceStoreError::Conflict(message) => {
            RuntimeStoreError::StateConflict(message)
        }
        WorkerGoalAcceptanceStoreError::Database(message) => {
            RuntimeStoreError::Internal(anyhow::anyhow!(message).context("Worker Goal acceptance"))
        }
        WorkerGoalAcceptanceStoreError::CommitUncertain(message) => RuntimeStoreError::Internal(
            anyhow::anyhow!(message).context("Worker Goal acceptance commit uncertain"),
        ),
    }
}

fn accept_direct_worker_input(
    tx: &Transaction<'_>,
    session: &super::persistence::OwnedSession,
    controller: &ControllerRecord,
    binding: &HiveWorkerConversationBinding,
    body: &str,
    input_id: &str,
    now: &str,
) -> Result<Mutation, RuntimeStoreError> {
    let controller_bound = tx.execute(
        "UPDATE hive_controllers
         SET worker_id = ?2, scope_key = ?3, updated_at = ?4
         WHERE id = ?1 AND session_id = ?5 AND user_id IS ?6
           AND (worker_id IS NULL OR worker_id = ?2)",
        params![
            controller.id,
            binding.worker.id,
            format!("worker:{}", binding.worker.id),
            now,
            session.id,
            session.user_id,
        ],
    )?;
    if controller_bound != 1 {
        return Err(RuntimeStoreError::StateConflict(
            "Hive Worker DM controller belongs to another Worker".into(),
        ));
    }
    let accepted_at = chrono::DateTime::parse_from_rfc3339(now)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?
        .with_timezone(&Utc);
    let (execution_context, working_dir, project_dir) =
        direct_worker_conversation_execution_context(tx, session, binding)?;
    let (priority_name, crew_slug) = tx
        .query_row(
            "SELECT priority, crew_slug FROM hive_runtime_state WHERE session_id = ?1",
            [&session.id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
        .unwrap_or_else(|| ("normal".to_string(), None));
    let priority = priority_value(&priority_name).unwrap_or(0);
    let new_run_id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("mitsuro:hive:worker-conversation:{input_id}").as_bytes(),
    )
    .to_string();
    let result = accept_worker_conversation_input_in_transaction(
        tx,
        &AcceptWorkerConversationInput {
            input_id: input_id.to_string(),
            request_id: input_id.to_string(),
            worker_id: binding.worker.id.clone(),
            owner_user_id: session.user_id.clone(),
            session_id: session.id.clone(),
            controller_id: controller.id.clone(),
            body: body.to_string(),
            accepted_at,
            new_run_id,
            run_config: serde_json::json!({
                "worker_id": binding.worker.id,
                "worker_revision": binding.worker.revision,
                "crew_slug": crew_slug,
                "model": binding.worker.model,
                "model_key": binding.worker.model_key,
                "model_catalog_revision": binding.worker.model_catalog_revision,
                "permission_mode": binding.worker.permission_mode.as_str(),
                "working_dir": working_dir,
                "project_dir": project_dir,
                "retry": RetryPolicy::default(),
            }),
            execution_context,
            priority,
            concurrency_key: Some(format!("worker:{}:dm", binding.worker.id)),
            max_attempts: 5,
        },
    )
    .map_err(|error| RuntimeStoreError::StateConflict(error.to_string()))?;

    let (response, events) = match result {
        AcceptWorkerConversationInputResult::Queued { run_id, message_id } => {
            tx.execute(
                "INSERT INTO hive_runtime_state (session_id, status, current_run_id, updated_at)
                 VALUES (?1, 'idle', ?2, ?3)
                 ON CONFLICT(session_id) DO UPDATE SET
                    current_run_id = excluded.current_run_id,
                    status = CASE WHEN hive_runtime_state.status = 'paused'
                                  THEN 'paused' ELSE excluded.status END,
                    updated_at = excluded.updated_at",
                params![session.id, run_id, now],
            )?;
            let received = append_event(
                tx,
                controller,
                "message_received",
                Some(&run_id),
                None,
                Some(&format!("worker-input:{input_id}:received")),
                serde_json::json!({
                    "run_id": run_id,
                    "message_bytes": body.len(),
                    "message_chars": body.chars().count(),
                }),
                now,
            )?;
            let queued = append_event(
                tx,
                controller,
                "run_queued",
                Some(&run_id),
                None,
                Some(&format!("run:{run_id}:queued")),
                serde_json::json!({
                    "run_id": run_id,
                    "kind": "worker_conversation",
                }),
                now,
            )?;
            (
                WorkerConversationInputResponse {
                    worker_id: binding.worker.id.clone(),
                    session_id: session.id.clone(),
                    disposition: WorkerConversationInputDisposition::Queued,
                    run_id,
                    canonical_message_id: Some(message_id),
                    staged_input_id: None,
                },
                vec![received, queued],
            )
        }
        AcceptWorkerConversationInputResult::Staged {
            active_run_id,
            input,
        } => {
            let staged = append_event(
                tx,
                controller,
                "message_staged",
                Some(&active_run_id),
                None,
                Some(&format!("worker-input:{input_id}:staged")),
                serde_json::json!({
                    "run_id": active_run_id,
                    "input_id": input.id,
                }),
                now,
            )?;
            (
                WorkerConversationInputResponse {
                    worker_id: binding.worker.id.clone(),
                    session_id: session.id.clone(),
                    disposition: WorkerConversationInputDisposition::Staged,
                    run_id: active_run_id,
                    canonical_message_id: None,
                    staged_input_id: Some(input.id),
                },
                vec![staged],
            )
        }
    };
    Ok(Mutation {
        response: ResponsePayload::WorkerConversationInput(response),
        resource_id: Some(session.id.clone()),
        events,
    })
}

fn direct_worker_conversation_execution_context(
    tx: &Transaction<'_>,
    session: &super::persistence::OwnedSession,
    binding: &HiveWorkerConversationBinding,
) -> Result<(HiveRunExecutionContextV1, Option<String>, Option<String>), RuntimeStoreError> {
    let resolved = super::worker_context::resolve_worker_conversation_execution_binding(
        tx,
        &session.id,
        &binding.worker.id,
        binding.worker.revision,
        WorkerConversationLane::DirectMessage,
    )
    .map_err(RuntimeStoreError::Internal)?;
    Ok((resolved.context, resolved.working_dir, resolved.project_dir))
}

fn send_message(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    message: &str,
    pending_id: &str,
) -> Result<Mutation, RuntimeStoreError> {
    if message.trim().is_empty() {
        return Err(RuntimeStoreError::Invalid("message is empty".into()));
    }
    if message.len() > 64 * 1024 {
        return Err(RuntimeStoreError::Invalid(
            "message exceeds 65536 bytes".into(),
        ));
    }
    let session = require_owned_session(tx, actor, session_id)?;
    let introduction_gate = enforce_worker_introduction_content_gate(tx, &session)?;
    if introduction_gate == WorkerIntroductionContentGate::AwaitingContext {
        let content_json = serde_json::to_string(&vec![Content::text(message.to_string())])
            .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
        enforce_worker_introduction_canonical_content_bound(&content_json)?;
    }
    if let Some(binding) = ordinary_direct_worker_binding(tx, &session)? {
        let controller = get_or_create_controller(tx, &session, now)?;
        return accept_direct_worker_input(
            tx,
            &session,
            &controller,
            &binding,
            message,
            pending_id,
            now,
        );
    }
    freeze_session_model_into_open_runs(tx, &session, now)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let active_run_id = tx
        .query_row(
            "SELECT id FROM hive_runs
             WHERE controller_id = ?1 AND status IN ('leased', 'running')
             ORDER BY updated_at DESC LIMIT 1",
            [&controller.id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(run_id) = active_run_id {
        insert_pending_user_message(tx, session_id, pending_id, message, now)?;
        let event = append_event(
            tx,
            &controller,
            "message_staged",
            Some(&run_id),
            None,
            Some(&format!("pending_message:{pending_id}")),
            serde_json::json!({
                "run_id": run_id,
                "pending_id": pending_id,
            }),
            now,
        )?;
        return Ok(Mutation {
            response: ack("message staged for the active run"),
            resource_id: Some(session_id.to_string()),
            events: vec![event],
        });
    }
    insert_canonical_user_message(tx, session_id, message, now)?;
    let event = append_event(
        tx,
        &controller,
        "message_received",
        None,
        None,
        None,
        serde_json::json!({
            "message_bytes": message.len(),
            "message_chars": message.chars().count(),
        }),
        now,
    )?;
    let mut events = vec![event];
    let resumed = resume_waiting_run(tx, &controller, now)?;
    if let Some((run_id, previous_status, attempt_count)) = resumed {
        let dedupe_key = format!("transition:{run_id}:{attempt_count}:queued");
        events.push(append_event(
            tx,
            &controller,
            "run_requeued",
            Some(&run_id),
            None,
            Some(&dedupe_key),
            serde_json::json!({
                "run_id": run_id,
                "previous_status": previous_status,
                "reason": "user message received"
            }),
            now,
        )?);
    } else if let Some(event) = queue_message_turn_if_idle(tx, &session, &controller, message, now)?
    {
        events.push(event);
    }
    Ok(Mutation {
        response: ack("message accepted"),
        resource_id: Some(session_id.to_string()),
        events,
    })
}

fn stage_steer(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    pending_id: &str,
    content: Value,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(pending_id, "pending id", 256)?;
    let content = serde_json::from_value::<Vec<Content>>(content).map_err(|error| {
        RuntimeStoreError::Invalid(format!("invalid steering content: {error}"))
    })?;
    if content.is_empty() {
        return Err(RuntimeStoreError::Invalid(
            "steering content is empty".into(),
        ));
    }
    let content_json = serde_json::to_string(&content)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    if content_json.len() > 256 * 1024 {
        return Err(RuntimeStoreError::Invalid(
            "steering content exceeds 262144 bytes".into(),
        ));
    }
    let session = require_owned_session(tx, actor, session_id)?;
    let introduction_gate = enforce_worker_introduction_content_gate(tx, &session)?;
    if introduction_gate == WorkerIntroductionContentGate::AwaitingContext {
        if content
            .iter()
            .any(|item| !matches!(item, Content::Text { .. }))
        {
            return Err(RuntimeStoreError::Invalid(
                "Hive Worker Introduction context accepts text only".into(),
            ));
        }
        if !content
            .iter()
            .any(|item| matches!(item, Content::Text { text, .. } if !text.trim().is_empty()))
        {
            return Err(RuntimeStoreError::Invalid(
                "Hive Worker Introduction context requires non-empty text".into(),
            ));
        }
    }
    if introduction_gate == WorkerIntroductionContentGate::AwaitingContext {
        enforce_worker_introduction_canonical_content_bound(&content_json)?;
    }
    if let Some(binding) = ordinary_direct_worker_binding(tx, &session)? {
        if content
            .iter()
            .any(|item| !matches!(item, Content::Text { .. }))
        {
            return Err(RuntimeStoreError::Invalid(
                "Hive Worker direct conversations currently accept text only".into(),
            ));
        }
        let body = steering_objective(&content);
        if body.trim().is_empty() {
            return Err(RuntimeStoreError::Invalid(
                "Hive Worker direct conversation requires non-empty text".into(),
            ));
        }
        let controller = get_or_create_controller(tx, &session, now)?;
        return accept_direct_worker_input(
            tx,
            &session,
            &controller,
            &binding,
            &body,
            pending_id,
            now,
        );
    }
    freeze_session_model_into_open_runs(tx, &session, now)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    insert_pending_user_content(tx, session_id, pending_id, &content_json, now)?;
    let active_run_id = tx
        .query_row(
            "SELECT id FROM hive_runs
             WHERE controller_id = ?1 AND status IN ('leased', 'running')
             ORDER BY updated_at DESC LIMIT 1",
            [&controller.id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let run_id = active_run_id.as_deref();
    let mut events = vec![append_event(
        tx,
        &controller,
        "steer_staged",
        run_id,
        None,
        Some(&format!("pending_steer:{pending_id}")),
        serde_json::json!({
            "run_id": run_id,
            "pending_id": pending_id,
        }),
        now,
    )?];
    if active_run_id.is_none() {
        if let Some((run_id, previous_status, attempt_count)) =
            resume_waiting_run(tx, &controller, now)?
        {
            let dedupe_key = format!("transition:{run_id}:{attempt_count}:queued");
            events.push(append_event(
                tx,
                &controller,
                "run_requeued",
                Some(&run_id),
                None,
                Some(&dedupe_key),
                serde_json::json!({
                    "run_id": run_id,
                    "previous_status": previous_status,
                    "reason": "durable steering received"
                }),
                now,
            )?);
        } else {
            let objective = steering_objective(&content);
            if let Some(event) =
                queue_message_turn_if_idle(tx, &session, &controller, &objective, now)?
            {
                events.push(event);
            }
        }
    }
    Ok(Mutation {
        response: ack("steering durably staged"),
        resource_id: Some(session_id.to_string()),
        events,
    })
}

fn steering_objective(content: &[Content]) -> String {
    let text = content
        .iter()
        .filter_map(|item| match item {
            Content::Text { text, .. } if !text.trim().is_empty() => Some(text.trim()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    if text.is_empty() {
        "Process the durable user steering attached to this session".into()
    } else {
        text
    }
}

fn stage_tool_approval(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    requested_run_id: &str,
    tool_call_id: &str,
    approved: bool,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(requested_run_id, "run id", 256)?;
    validate_bounded_field(tool_call_id, "tool call id", 512)?;
    let session = require_owned_session(tx, actor, session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let run_id = tx
        .query_row(
            "SELECT id FROM hive_runs
             WHERE controller_id = ?1 AND id = ?2 AND status IN ('leased', 'running')",
            params![controller.id, requested_run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| {
            RuntimeStoreError::Invalid("the exact run is no longer accepting tool approvals".into())
        })?;
    let pending_event_type = tx
        .query_row(
            "SELECT json_extract(payload_json, '$.type')
             FROM hive_controller_events
             WHERE controller_id = ?1 AND run_id = ?2 AND event_type = 'agentic_event'
               AND json_extract(payload_json, '$.id') = ?3
               AND json_extract(payload_json, '$.type') IN (
                   'tool_approval_required', 'tool_approved', 'tool_denied', 'tool_result'
               )
             ORDER BY sequence DESC LIMIT 1",
            params![controller.id, run_id, tool_call_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if pending_event_type.as_deref() != Some("tool_approval_required") {
        return Err(RuntimeStoreError::StateConflict(format!(
            "tool call {tool_call_id} is not awaiting approval on run {run_id}"
        )));
    }
    let existing = tx
        .query_row(
            "SELECT payload_json FROM hive_control_outbox
             WHERE controller_id = ?1 AND control_kind = 'tool_approval'
               AND dedupe_key = ?2",
            params![controller.id, format!("{run_id}:{tool_call_id}")],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        let existing = serde_json::from_str::<Value>(&existing)
            .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
        if existing.get("approved").and_then(Value::as_bool) != Some(approved) {
            return Err(RuntimeStoreError::Invalid(format!(
                "tool call {tool_call_id} already has a different approval decision"
            )));
        }
    } else {
        let id = crate::legacy_identity::tool_approval_id(&controller.id, &run_id, tool_call_id);
        let payload = serde_json::to_string(&serde_json::json!({
            "tool_call_id": tool_call_id,
            "approved": approved,
        }))
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
        tx.execute(
            "INSERT INTO hive_control_outbox (
                id, controller_id, session_id, run_id, control_kind, dedupe_key,
                payload_json, status, attempt_count, available_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, 'tool_approval', ?5, ?6, 'pending', 0, ?7, ?7, ?7)",
            params![
                id,
                controller.id,
                session_id,
                run_id,
                format!("{run_id}:{tool_call_id}"),
                payload,
                now
            ],
        )?;
    }
    let event = append_event(
        tx,
        &controller,
        "tool_approval_queued",
        Some(&run_id),
        None,
        Some(&format!("tool_approval:{run_id}:{tool_call_id}")),
        serde_json::json!({
            "run_id": run_id,
            "tool_call_id": tool_call_id,
            "approved": approved,
        }),
        now,
    )?;
    Ok(Mutation {
        response: ack("tool approval durably queued"),
        resource_id: Some(session_id.to_string()),
        events: vec![event],
    })
}

fn user_response(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    requested_run_id: &str,
    tool_call_id: &str,
    response: &str,
    pending_id: &str,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(requested_run_id, "run id", 256)?;
    validate_bounded_field(tool_call_id, "tool call id", 512)?;
    validate_bounded_field(response, "user response", 64 * 1024)?;
    validate_bounded_field(pending_id, "pending id", 256)?;
    let session = require_owned_session(tx, actor, session_id)?;
    let introduction_gate = enforce_worker_introduction_content_gate(tx, &session)?;
    let _direct_worker_binding = ordinary_direct_worker_binding(tx, &session)?;
    let durable_response = format!("Response to {tool_call_id}:\n{response}");
    if introduction_gate == WorkerIntroductionContentGate::AwaitingContext {
        let content_json = serde_json::to_string(&vec![Content::text(durable_response.clone())])
            .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
        enforce_worker_introduction_canonical_content_bound(&content_json)?;
    }
    let controller = get_or_create_controller(tx, &session, now)?;
    let exact_run_status = tx
        .query_row(
            "SELECT status FROM hive_runs WHERE controller_id = ?1 AND id = ?2
             AND status IN ('leased', 'running', 'awaiting_input')",
            params![controller.id, requested_run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| {
            RuntimeStoreError::StateConflict(
                "the exact run is no longer accepting a user response".into(),
            )
        })?;
    let pending_event_type = tx
        .query_row(
            "SELECT CASE
                 WHEN event_type = 'agentic_event'
                   THEN json_extract(payload_json, '$.type')
                 ELSE event_type
             END
             FROM hive_controller_events
             WHERE controller_id = ?1 AND run_id = ?2
               AND (
                   (event_type = 'agentic_event'
                    AND json_extract(payload_json, '$.type') = 'awaiting_input'
                    AND json_extract(payload_json, '$.tool_call_id') = ?3)
                   OR
                   (event_type IN ('user_response_received', 'user_response_staged')
                    AND json_extract(payload_json, '$.tool_call_id') = ?3)
               )
             ORDER BY sequence DESC LIMIT 1",
            params![controller.id, requested_run_id, tool_call_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if pending_event_type.as_deref() != Some("awaiting_input") {
        return Err(RuntimeStoreError::StateConflict(format!(
            "tool call {tool_call_id} is not awaiting a response on run {requested_run_id}"
        )));
    }
    let active_run_id = tx
        .query_row(
            "SELECT id FROM hive_runs
             WHERE controller_id = ?1 AND status IN ('leased', 'running')
             ORDER BY updated_at DESC LIMIT 1",
            [&controller.id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if active_run_id
        .as_deref()
        .is_some_and(|run_id| run_id != requested_run_id)
    {
        return Err(RuntimeStoreError::StateConflict(
            "a replacement run is active; refusing to redirect the response".into(),
        ));
    }
    if exact_run_status == "awaiting_input" && active_run_id.is_some() {
        return Err(RuntimeStoreError::StateConflict(
            "persisted run state disagrees with the active execution".into(),
        ));
    }

    // Ask-user completion and the durable run transition are not one atomic
    // operation. If the answer arrives while the run is still active, stage
    // it under an idempotent non-canonical role. A live loop can promote it at
    // its next boundary; otherwise finish_execution observes the staging row,
    // yields immediately, and the replacement run promotes it before loading
    // history. This prevents an answer from being stranded behind a just-
    // committed awaiting_input transition.
    if let Some(run_id) = active_run_id {
        insert_pending_user_message(tx, session_id, pending_id, &durable_response, now)?;
        let event = append_event(
            tx,
            &controller,
            "user_response_staged",
            Some(&run_id),
            None,
            Some(&format!("pending_response:{pending_id}")),
            serde_json::json!({
                "run_id": run_id,
                "pending_id": pending_id,
                "tool_call_id": tool_call_id,
            }),
            now,
        )?;
        return Ok(Mutation {
            response: ack("user response staged for the active run"),
            resource_id: Some(session_id.to_string()),
            events: vec![event],
        });
    }

    insert_canonical_user_message(tx, session_id, &durable_response, now)?;
    let mut events = vec![append_event(
        tx,
        &controller,
        "user_response_received",
        Some(requested_run_id),
        None,
        None,
        serde_json::json!({
            "tool_call_id": tool_call_id,
            "response_bytes": response.len(),
            "response_chars": response.chars().count(),
        }),
        now,
    )?];
    let attempt_count = tx
        .query_row(
            "SELECT attempt_count FROM hive_runs
             WHERE id = ?1 AND controller_id = ?2 AND status = 'awaiting_input'",
            params![requested_run_id, controller.id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .ok_or_else(|| {
            RuntimeStoreError::StateConflict(
                "the exact run is no longer awaiting the requested response".into(),
            )
        })?;
    let changed = tx.execute(
        "UPDATE hive_runs
         SET status = 'queued', available_at = ?3, wake_at = NULL, updated_at = ?3
         WHERE id = ?1 AND controller_id = ?2 AND status = 'awaiting_input'",
        params![requested_run_id, controller.id, now],
    )?;
    if changed != 1 {
        return Err(RuntimeStoreError::StateConflict(
            "the exact run changed state before its response could be resumed".into(),
        ));
    }
    align_run_projection(tx, requested_run_id, "queued", now)?;
    let dedupe_key = format!("transition:{requested_run_id}:{attempt_count}:queued");
    events.push(append_event(
        tx,
        &controller,
        "run_requeued",
        Some(requested_run_id),
        None,
        Some(&dedupe_key),
        serde_json::json!({
            "run_id": requested_run_id,
            "previous_status": "awaiting_input",
            "reason": "user response received"
        }),
        now,
    )?);
    Ok(Mutation {
        response: ack("user response accepted"),
        resource_id: Some(session_id.to_string()),
        events,
    })
}

fn enforce_worker_introduction_canonical_content_bound(
    content_json: &str,
) -> Result<(), RuntimeStoreError> {
    if content_json.len() > WORKER_INTRODUCTION_MAX_CANONICAL_CONTENT_BYTES {
        return Err(RuntimeStoreError::Invalid(format!(
            "Hive Worker Introduction content exceeds the {WORKER_INTRODUCTION_MAX_CANONICAL_CONTENT_BYTES}-byte canonical review limit"
        )));
    }
    Ok(())
}

fn resume_waiting_run(
    tx: &Transaction<'_>,
    controller: &ControllerRecord,
    now: &str,
) -> Result<Option<(String, String, i64)>, RuntimeStoreError> {
    let waiting = {
        let mut statement = tx.prepare(
            "SELECT id, status, attempt_count FROM hive_runs
             WHERE controller_id = ?1 AND status IN ('sleeping', 'awaiting_input')
             ORDER BY updated_at DESC, created_at DESC LIMIT 2",
        )?;
        let waiting = statement
            .query_map([&controller.id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        waiting
    };
    if waiting.len() > 1 {
        return Err(RuntimeStoreError::StateConflict(
            "multiple runs are waiting; an exact run id is required to resume one".into(),
        ));
    }
    let Some((run_id, previous_status, attempt_count)) = waiting.into_iter().next() else {
        return Ok(None);
    };
    let changed = tx.execute(
        "UPDATE hive_runs
         SET status = 'queued', available_at = ?2, wake_at = NULL,
             last_stop_reason = NULL, updated_at = ?2
         WHERE id = ?1 AND status IN ('sleeping', 'awaiting_input')",
        params![run_id, now],
    )?;
    if changed != 1 {
        return Ok(None);
    }
    align_run_projection(tx, &run_id, "queued", now)?;
    Ok(Some((run_id, previous_status, attempt_count)))
}

fn queue_message_turn_if_idle(
    tx: &Transaction<'_>,
    session: &super::persistence::OwnedSession,
    controller: &ControllerRecord,
    message: &str,
    now: &str,
) -> Result<Option<PersistedEvent>, RuntimeStoreError> {
    let _ = require_frozen_session_model(session)?;
    require_session_workspace(session)?;
    let unfinished: i64 = tx.query_row(
        "SELECT COUNT(*) FROM hive_runs
         WHERE controller_id = ?1
           AND status IN ('queued', 'leased', 'running', 'sleeping', 'retry_wait', 'awaiting_input', 'recovery_required')",
        [&controller.id],
        |row| row.get(0),
    )?;
    if unfinished > 0 {
        return Ok(None);
    }
    let (priority_name, crew_slug) = tx
        .query_row(
            "SELECT priority, crew_slug FROM hive_runtime_state WHERE session_id = ?1",
            [&session.id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
        .unwrap_or_else(|| ("normal".to_string(), None));
    let priority = priority_value(&priority_name).unwrap_or(0);
    let run_id = uuid::Uuid::new_v4().to_string();
    insert_run(
        tx,
        &run_id,
        &controller.id,
        Some(&session.id),
        None,
        None,
        "legacy_resume",
        message,
        serde_json::json!({
            "working_dir": session.working_dir,
            "project_dir": session.project_dir,
            "model": session.model,
            "model_key": session.model_key,
            "model_catalog_revision": session.model_catalog_revision,
            "permission_mode": require_frozen_session_permission_mode(session)?,
            "crew_slug": crew_slug,
            "retry": RetryPolicy::default(),
        }),
        priority,
        now,
        5,
        now,
        None,
    )?;
    let runtime_status = if controller.status == "paused" {
        "paused"
    } else {
        "idle"
    };
    tx.execute(
        "INSERT INTO hive_runtime_state (session_id, status, current_run_id, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(session_id) DO UPDATE SET current_run_id = excluded.current_run_id,
             status = CASE WHEN hive_runtime_state.status = 'paused' THEN 'paused' ELSE excluded.status END,
             updated_at = excluded.updated_at",
        params![session.id, runtime_status, run_id, now],
    )?;
    let event = append_event(
        tx,
        controller,
        "run_queued",
        Some(&run_id),
        None,
        Some(&format!("run:{run_id}:queued")),
        serde_json::json!({
            "run_id": run_id,
            "kind": "message_turn",
        }),
        now,
    )?;
    Ok(Some(event))
}

fn normalize_model_identity(
    model: Option<String>,
    model_key: Option<ModelKey>,
    model_catalog_revision: Option<String>,
    context: &str,
) -> Result<(Option<String>, Option<ModelKey>, Option<String>), RuntimeStoreError> {
    let model = model
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if model
        .as_deref()
        .is_some_and(|value| value.len() > 512 || value.as_bytes().contains(&0))
    {
        return Err(RuntimeStoreError::Invalid(format!(
            "{context} model is invalid or exceeds 512 bytes"
        )));
    }

    if let Some(key) = model_key.as_ref() {
        for (field, value) in [
            ("provider", key.provider.as_str()),
            ("model_id", key.model_id.as_str()),
            ("api_format", key.api_format.as_str()),
        ] {
            if value.trim().is_empty()
                || value != value.trim()
                || value.len() > 512
                || value.as_bytes().contains(&0)
            {
                return Err(RuntimeStoreError::Invalid(format!(
                    "{context} model_key.{field} is invalid"
                )));
            }
        }
        if key.auth_scope.as_deref().is_some_and(|value| {
            value.trim().is_empty()
                || value != value.trim()
                || value.len() > 128
                || value.as_bytes().contains(&0)
        }) {
            return Err(RuntimeStoreError::Invalid(format!(
                "{context} model_key.auth_scope is invalid"
            )));
        }
        if model.as_deref().is_some_and(|model| model != key.model_id) {
            return Err(RuntimeStoreError::Invalid(format!(
                "{context} model must match model_key.model_id"
            )));
        }
        let serialized =
            serde_json::to_value(key).map_err(|error| RuntimeStoreError::Internal(error.into()))?;
        serde_json::from_value::<CoreModelKey>(serialized).map_err(|error| {
            RuntimeStoreError::Invalid(format!("{context} model_key is unsupported: {error}"))
        })?;
    }

    let model = model.or_else(|| model_key.as_ref().map(|key| key.model_id.clone()));
    let model_catalog_revision = model_catalog_revision
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if model_catalog_revision
        .as_deref()
        .is_some_and(|value| value.len() > 512 || value.as_bytes().contains(&0))
    {
        return Err(RuntimeStoreError::Invalid(format!(
            "{context} model catalog revision is invalid or exceeds 512 bytes"
        )));
    }
    if model_key.is_none() && model_catalog_revision.is_some() {
        return Err(RuntimeStoreError::Invalid(format!(
            "{context} model catalog revision requires model_key"
        )));
    }
    Ok((model, model_key, model_catalog_revision))
}

fn require_frozen_session_model(
    session: &super::persistence::OwnedSession,
) -> Result<&str, RuntimeStoreError> {
    normalize_model_identity(
        session.model.clone(),
        session.model_key.clone(),
        session.model_catalog_revision.clone(),
        "Hive session",
    )?;
    let model = session
        .model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            RuntimeStoreError::StateConflict(
                "Hive session has no frozen model; select a model before starting it".into(),
            )
        })?;
    if session
        .model_key
        .as_ref()
        .is_some_and(|key| key.model_id != model)
    {
        return Err(RuntimeStoreError::StateConflict(
            "Hive session model does not match its frozen model key".into(),
        ));
    }
    if session.model_key.is_none() && session.model_catalog_revision.is_some() {
        return Err(RuntimeStoreError::StateConflict(
            "Hive session catalog revision has no frozen model key".into(),
        ));
    }
    Ok(model)
}

fn require_frozen_session_permission_mode(
    session: &super::persistence::OwnedSession,
) -> Result<&str, RuntimeStoreError> {
    match session.permission_mode.as_str() {
        "supervised" | "autonomous" => Ok(session.permission_mode.as_str()),
        _ => Err(RuntimeStoreError::StateConflict(
            "Hive session has an invalid permission mode".into(),
        )),
    }
}

/// Enqueue-time mirror of the execution host's claim validation, which
/// refuses a daemon-default workspace and requires absolute paths. Sessions
/// predating explicit Hive workspaces would otherwise enqueue runs that are
/// doomed to fail their claim with a redacted, non-actionable error.
fn require_session_workspace(
    session: &super::persistence::OwnedSession,
) -> Result<(), RuntimeStoreError> {
    let paths: Vec<&str> = [
        session.working_dir.as_deref(),
        session.project_dir.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::trim)
    .filter(|path| !path.is_empty())
    .collect();
    if paths.is_empty() {
        return Err(RuntimeStoreError::StateConflict(
            "Hive session has no working or project directory; set a workspace before starting it"
                .into(),
        ));
    }
    if paths.iter().any(|path| !Path::new(path).is_absolute()) {
        return Err(RuntimeStoreError::StateConflict(
            "Hive session workspace paths must be absolute".into(),
        ));
    }
    Ok(())
}

fn freeze_session_model_into_open_runs(
    tx: &Transaction<'_>,
    session: &super::persistence::OwnedSession,
    now: &str,
) -> Result<(), RuntimeStoreError> {
    let model = require_frozen_session_model(session)?;
    let permission_mode = require_frozen_session_permission_mode(session)?;
    let session_key_value = session
        .model_key
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let mut statement = tx.prepare(
        "SELECT id, schedule_id, config_json FROM hive_runs
         WHERE session_id = ?1
           AND status IN ('queued', 'sleeping', 'retry_wait', 'awaiting_input')",
    )?;
    let rows = statement
        .query_map([&session.id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);

    for (run_id, schedule_id, serialized) in rows {
        let mut config = serde_json::from_str::<Value>(&serialized)
            .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
        let object = config.as_object_mut().ok_or_else(|| {
            RuntimeStoreError::StateConflict("Hive run config is not a JSON object".into())
        })?;
        let missing_model = object
            .get("model")
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty());
        let missing_permission = object.get("permission_mode").is_none();
        let should_backfill_model = missing_model && schedule_id.is_none();
        let run_model_matches_session = object
            .get("model")
            .and_then(Value::as_str)
            .is_some_and(|value| value.trim() == model);
        // Scheduled runs own their selection. Only legacy, non-scheduled runs
        // may inherit provider-aware identity from their parent session.
        let can_inherit_session_identity =
            schedule_id.is_none() && (missing_model || run_model_matches_session);
        let missing_key = object.get("model_key").is_none_or(Value::is_null);
        let should_backfill_key =
            can_inherit_session_identity && session_key_value.is_some() && missing_key;
        let key_matches_session = session_key_value
            .as_ref()
            .is_some_and(|key| object.get("model_key") == Some(key));
        let missing_revision = object
            .get("model_catalog_revision")
            .is_none_or(Value::is_null);
        let should_backfill_revision = can_inherit_session_identity
            && session.model_catalog_revision.is_some()
            && missing_revision
            && (should_backfill_key || key_matches_session);
        if !should_backfill_model
            && !missing_permission
            && !should_backfill_key
            && !should_backfill_revision
        {
            continue;
        }
        if should_backfill_model {
            object.insert("model".into(), Value::String(model.to_string()));
        }
        if should_backfill_key {
            object.insert(
                "model_key".into(),
                session_key_value
                    .clone()
                    .expect("backfill requires a frozen session key"),
            );
        }
        if should_backfill_revision {
            object.insert(
                "model_catalog_revision".into(),
                Value::String(
                    session
                        .model_catalog_revision
                        .clone()
                        .expect("backfill requires a frozen catalog revision"),
                ),
            );
        }
        if missing_permission {
            object.insert(
                "permission_mode".into(),
                Value::String(permission_mode.to_string()),
            );
        }
        tx.execute(
            "UPDATE hive_runs SET config_json = ?2, updated_at = ?3 WHERE id = ?1",
            params![
                run_id,
                serde_json::to_string(&config)
                    .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                now
            ],
        )?;
    }
    Ok(())
}

fn recovery_required_run(
    tx: &Transaction<'_>,
    controller_id: &str,
) -> Result<Option<(String, i64)>, RuntimeStoreError> {
    tx.query_row(
        "SELECT id, attempt_count FROM hive_runs
         WHERE controller_id = ?1 AND status = 'recovery_required'
         ORDER BY updated_at DESC, id ASC LIMIT 1",
        [controller_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(RuntimeStoreError::from)
}

fn align_run_projection(
    tx: &Transaction<'_>,
    run_id: &str,
    status: &str,
    now: &str,
) -> Result<(), RuntimeStoreError> {
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
    if status == "recovery_required" {
        tx.execute(
            "UPDATE hive_controllers SET status = 'paused', updated_at = ?2 WHERE id = ?1",
            params![controller_id, now],
        )?;
    }
    if let Some(occurrence_id) = occurrence_id {
        let occurrence_status = match status {
            "queued" | "leased" => "queued",
            "succeeded" => "succeeded",
            "cancelled" => "cancelled",
            "failed" | "dead_letter" | "recovery_required" => "failed",
            _ => "running",
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
                "cancelled" => "cancelled",
                "failed" | "dead_letter" | "recovery_required" => "error",
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

fn set_priority(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    priority: &str,
) -> Result<Mutation, RuntimeStoreError> {
    let session = require_owned_session(tx, actor, session_id)?;
    reject_generic_worker_session_control(tx, session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let value = priority_value(priority)?;
    tx.execute(
        "UPDATE hive_runs SET priority = ?2, updated_at = ?3
         WHERE controller_id = ?1 AND status IN ('queued', 'sleeping', 'retry_wait')",
        params![controller.id, value, now],
    )?;
    tx.execute(
        "INSERT INTO hive_runtime_state (session_id, status, priority, updated_at)
         VALUES (?1, 'idle', ?2, ?3)
         ON CONFLICT(session_id) DO UPDATE SET priority = excluded.priority, updated_at = excluded.updated_at",
        params![session_id, priority_name(value), now],
    )?;
    let event = append_event(
        tx,
        &controller,
        "priority_changed",
        None,
        None,
        None,
        serde_json::json!({"priority": priority, "value": value}),
        now,
    )?;
    Ok(session_mutation(session_id, "updated", None, vec![event]))
}

fn set_crew(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: &str,
    crew_slug: Option<&str>,
) -> Result<Mutation, RuntimeStoreError> {
    if let Some(crew_slug) = crew_slug {
        if !is_valid_crew_slug(crew_slug) {
            return Err(RuntimeStoreError::Invalid("crew slug is invalid".into()));
        }
    }
    let session = require_owned_session(tx, actor, session_id)?;
    reject_generic_worker_session_control(tx, session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    tx.execute(
        "UPDATE hive_schedules SET crew_slug = ?2, revision = revision + 1, updated_at = ?3
         WHERE controller_id = ?1 AND status IN ('enabled', 'paused')",
        params![controller.id, crew_slug, now],
    )?;
    tx.execute(
        "UPDATE hive_runs
         SET config_json = json_set(config_json, '$.crew_slug', ?2), updated_at = ?3
         WHERE controller_id = ?1 AND status = 'queued'",
        params![controller.id, crew_slug, now],
    )?;
    tx.execute(
        "INSERT INTO hive_runtime_state (session_id, status, crew_slug, updated_at)
         VALUES (?1, 'idle', ?2, ?3)
         ON CONFLICT(session_id) DO UPDATE SET crew_slug = excluded.crew_slug, updated_at = excluded.updated_at",
        params![session_id, crew_slug, now],
    )?;
    let event = append_event(
        tx,
        &controller,
        "crew_changed",
        None,
        None,
        None,
        serde_json::json!({"crew_slug": crew_slug}),
        now,
    )?;
    Ok(session_mutation(session_id, "updated", None, vec![event]))
}

fn recover(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    session_id: Option<&str>,
) -> Result<Mutation, RuntimeStoreError> {
    let mut controllers = Vec::new();
    if let Some(session_id) = session_id {
        let session = require_owned_session(tx, actor, session_id)?;
        reject_generic_worker_session_control(tx, session_id)?;
        controllers.push(get_or_create_controller(tx, &session, now)?);
    } else {
        let mut statement = tx.prepare(
            "SELECT c.id, c.session_id, c.status, c.timezone
             FROM hive_controllers c JOIN sessions s ON s.id = c.session_id
             WHERE ((?1 IS NULL AND s.user_id IS NULL) OR s.user_id = ?1)
               AND c.worker_id IS NULL",
        )?;
        controllers = statement
            .query_map([actor.user_id.as_deref()], |row| {
                Ok(ControllerRecord {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    status: row.get(2)?,
                    timezone: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
    }
    let mut events = Vec::new();
    let mut recovered_count = 0usize;
    for controller in &controllers {
        let mut statement = tx.prepare(
            "SELECT id, status, attempt_count, lease_token, worker_id FROM hive_runs
             WHERE controller_id = ?1 AND status IN ('leased', 'running')
               AND lease_expires_at <= ?2",
        )?;
        let expired = statement
            .query_map(params![controller.id, now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        for (run_id, previous_status, attempt_no, lease_token, worker_id) in expired {
            // Every Worker-bound kind has a typed fenced reconciler in
            // HiveRunStore (canonical response, Introduction opening/review,
            // or Workflow outcome). Generic recovery must never clear that
            // lease before the typed authority inspects durable provenance.
            if worker_id.is_some() {
                continue;
            }
            if previous_status == "leased" {
                let session = require_owned_session(tx, actor, &controller.session_id)?;
                freeze_session_model_into_open_runs(tx, &session, now)?;
            }
            let (target_status, reason, event_type) = if previous_status == "leased" {
                (
                    "queued",
                    "worker lease expired before execution; requeued",
                    "run_lease_requeued",
                )
            } else {
                (
                    "recovery_required",
                    "worker lease expired; side effects may be uncertain",
                    "recovery_required",
                )
            };
            tx.execute(
                "UPDATE hive_runs SET status = ?2, lease_owner = NULL,
                     lease_token = NULL, lease_epoch = NULL, lease_expires_at = NULL,
                     heartbeat_at = NULL, last_error = ?3,
                     updated_at = ?4 WHERE id = ?1 AND status = ?5",
                params![run_id, target_status, reason, now, previous_status],
            )?;
            if target_status == "recovery_required" {
                tx.execute(
                    "UPDATE hive_control_outbox
                     SET status = 'discarded', last_error = ?2, updated_at = ?3
                     WHERE run_id = ?1 AND status = 'pending'",
                    params![run_id, reason, now],
                )?;
            }
            align_run_projection(tx, &run_id, target_status, now)?;
            if let Some(lease_token) = lease_token {
                tx.execute(
                    "UPDATE hive_run_attempts SET finished_at = ?4, outcome = 'abandoned',
                         error = ?5
                     WHERE run_id = ?1 AND attempt_no = ?2 AND lease_token = ?3",
                    params![run_id, attempt_no, lease_token, now, reason],
                )?;
            }
            let dedupe_key = format!("transition:{run_id}:{attempt_no}:{target_status}");
            events.push(append_event(
                tx,
                controller,
                event_type,
                Some(&run_id),
                None,
                Some(&dedupe_key),
                serde_json::json!({
                    "run_id": run_id,
                    "reason": reason
                }),
                now,
            )?);
            recovered_count += 1;
        }
    }
    Ok(Mutation {
        response: ResponsePayload::Recover(RecoverResponse { recovered_count }),
        resource_id: session_id.map(ToOwned::to_owned),
        events,
    })
}

struct ParsedScheduleDefinition {
    title: String,
    summary: String,
    objective: String,
    recurrence: RecurrenceV1,
    timezone: String,
    dst_policy: DstPolicy,
    next_fire_at: String,
    priority: i32,
    project_dir: Option<String>,
    model: Option<String>,
    model_key: Option<ModelKey>,
    model_catalog_revision: Option<String>,
    model_was_explicit: bool,
    crew_slug: Option<String>,
    worker_id: Option<String>,
    group_id: Option<String>,
    misfire: MisfireConfig,
    overlap_policy: OverlapPolicy,
    retry: RetryPolicy,
}

fn parse_schedule_definition(
    definition: ScheduleDefinition,
    now: &str,
) -> Result<ParsedScheduleDefinition, RuntimeStoreError> {
    fn required(value: String, field: &str, max_bytes: usize) -> Result<String, RuntimeStoreError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(RuntimeStoreError::Invalid(format!(
                "schedule {field} must not be empty"
            )));
        }
        if value.len() > max_bytes {
            return Err(RuntimeStoreError::Invalid(format!(
                "schedule {field} exceeds {max_bytes} bytes"
            )));
        }
        Ok(value.to_string())
    }

    let model_was_explicit = definition
        .model
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || definition.model_key.is_some();
    let title = required(definition.title, "title", 512)?;
    let objective = required(definition.objective, "objective", 64 * 1024)?;
    let summary = definition.summary.trim().to_string();
    if summary.len() > 8 * 1024 {
        return Err(RuntimeStoreError::Invalid(
            "schedule summary exceeds 8192 bytes".into(),
        ));
    }
    let timezone = required(definition.timezone, "timezone", 128)?;
    let timezone_parsed =
        parse_timezone(&timezone).map_err(|error| RuntimeStoreError::Invalid(error.to_string()))?;
    let recurrence: RecurrenceV1 = serde_json::from_value(definition.recurrence)
        .map_err(|error| RuntimeStoreError::Invalid(format!("invalid recurrence: {error}")))?;
    recurrence
        .validate()
        .map_err(|error| RuntimeStoreError::Invalid(error.to_string()))?;
    let dst_policy: DstPolicy = serde_json::from_value(definition.dst_policy)
        .map_err(|error| RuntimeStoreError::Invalid(format!("invalid DST policy: {error}")))?;
    let now_instant = mitsuro_core::hive::parse_utc_timestamp(now)
        .map_err(|error| RuntimeStoreError::Invalid(error.to_string()))?;
    let next_fire_at = recurrence
        .next_after(timezone_parsed, now_instant, dst_policy)
        .map_err(|error| RuntimeStoreError::Invalid(error.to_string()))?
        .ok_or_else(|| {
            RuntimeStoreError::Invalid("schedule has no occurrence after the current time".into())
        })?;
    let misfire: MisfireConfig = serde_json::from_value(definition.misfire)
        .map_err(|error| RuntimeStoreError::Invalid(format!("invalid misfire policy: {error}")))?;
    if misfire.catch_up_limit > 10_000 {
        return Err(RuntimeStoreError::Invalid(
            "misfire catch_up_limit exceeds 10000".into(),
        ));
    }
    let overlap_policy = match definition.overlap_policy.as_str() {
        "skip" => OverlapPolicy::Skip,
        "queue_one" => OverlapPolicy::QueueOne,
        "allow" => OverlapPolicy::Allow,
        _ => {
            return Err(RuntimeStoreError::Invalid(
                "overlap_policy must be skip, queue_one, or allow".into(),
            ));
        }
    };
    let retry: RetryPolicy = serde_json::from_value(definition.retry)
        .map_err(|error| RuntimeStoreError::Invalid(format!("invalid retry policy: {error}")))?;
    if retry.max_attempts == 0
        || retry.max_attempts > MAX_RETRY_ATTEMPTS
        || retry.base_delay_secs == 0
        || retry.max_delay_secs < retry.base_delay_secs
        || retry.max_delay_secs > MAX_RETRY_DELAY_SECS
    {
        return Err(RuntimeStoreError::Invalid(
            format!(
                "retry policy requires 1..={MAX_RETRY_ATTEMPTS} attempts, a nonzero base delay, and base <= max <= {MAX_RETRY_DELAY_SECS} seconds"
            ),
        ));
    }
    let crew_slug = definition
        .crew_slug
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if crew_slug
        .as_deref()
        .is_some_and(|slug| !is_valid_crew_slug(slug))
    {
        return Err(RuntimeStoreError::Invalid("crew slug is invalid".into()));
    }
    let worker_id = definition
        .worker_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let group_id = definition
        .group_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if worker_id.is_some() && group_id.is_some() {
        return Err(RuntimeStoreError::Invalid(
            "schedule cannot target both a Worker and a Group".into(),
        ));
    }

    let project_dir = definition
        .project_dir
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if project_dir
        .as_deref()
        .is_some_and(|value| value.len() > 4096 || value.as_bytes().contains(&0))
    {
        return Err(RuntimeStoreError::Invalid(
            "schedule project_dir is invalid or too long".into(),
        ));
    }
    if project_dir
        .as_deref()
        .is_some_and(|value| !Path::new(value).is_absolute())
    {
        return Err(RuntimeStoreError::Invalid(
            "schedule project_dir must be absolute".into(),
        ));
    }
    let (model, model_key, model_catalog_revision) = normalize_model_identity(
        definition.model,
        definition.model_key,
        definition.model_catalog_revision,
        "schedule",
    )?;

    Ok(ParsedScheduleDefinition {
        title,
        summary,
        objective,
        recurrence,
        timezone,
        dst_policy,
        next_fire_at: canonical_timestamp(next_fire_at),
        priority: definition.priority,
        project_dir,
        model,
        model_key,
        model_catalog_revision,
        model_was_explicit,
        crew_slug,
        worker_id,
        group_id,
        misfire,
        overlap_policy,
        retry,
    })
}

fn bind_schedule_worker(
    tx: &Transaction<'_>,
    actor: &Actor,
    definition: &mut ParsedScheduleDefinition,
) -> Result<(), RuntimeStoreError> {
    let worker = if let Some(worker_id) = definition.worker_id.as_deref() {
        Some(
            load_worker_with_conn(tx, worker_id)
                .map_err(RuntimeStoreError::Internal)?
                .ok_or_else(|| {
                    RuntimeStoreError::Invalid("schedule worker was not found".into())
                })?,
        )
    } else if let Some(crew_slug) = definition.crew_slug.as_deref() {
        resolve_worker_for_crew_slug_with_conn(tx, actor.user_id.as_deref(), crew_slug)
            .map_err(RuntimeStoreError::Internal)?
    } else {
        None
    };
    let Some(worker) = worker else {
        return Ok(());
    };
    if worker.user_id != actor.user_id {
        return Err(RuntimeStoreError::Invalid(
            "schedule worker does not belong to this owner".into(),
        ));
    }
    if worker.status == mitsuro_core::storage::HiveWorkerStatus::Archived {
        return Err(RuntimeStoreError::Invalid(
            "schedule cannot target an archived Worker".into(),
        ));
    }
    if let Some(introduction) = HiveWorkerIntroductionStore::from_connection(tx)
        .get_by_worker(&worker.id)
        .map_err(RuntimeStoreError::Internal)?
    {
        if !introduction.status.allows_autonomy() {
            return Err(RuntimeStoreError::Invalid(
                "schedule cannot target a Worker until its Introduction is confirmed or skipped"
                    .into(),
            ));
        }
    }
    let exact_model_key_matches = serde_json::to_value(definition.model_key.as_ref())
        .and_then(|key| {
            serde_json::to_value(worker.model_key.as_ref()).map(|worker_key| key == worker_key)
        })
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    if definition.model_was_explicit
        && (definition.model != worker.model
            || !exact_model_key_matches
            || definition.model_catalog_revision != worker.model_catalog_revision)
    {
        return Err(RuntimeStoreError::Invalid(
            "schedule model identity must exactly match its target Worker".into(),
        ));
    }
    if definition.crew_slug.is_none() {
        definition.crew_slug = Some(worker.slug.clone());
    }
    definition.worker_id = Some(worker.id);
    Ok(())
}

fn bind_schedule_group(
    tx: &Transaction<'_>,
    actor: &Actor,
    definition: &mut ParsedScheduleDefinition,
) -> Result<(), RuntimeStoreError> {
    let Some(group_id) = definition.group_id.as_deref() else {
        return Ok(());
    };
    let group = hive_groups::load_group(tx, group_id)
        .map_err(RuntimeStoreError::Internal)?
        .ok_or_else(|| RuntimeStoreError::Invalid("schedule group was not found".into()))?;
    if group.user_id != actor.user_id {
        return Err(RuntimeStoreError::Invalid(
            "schedule group does not belong to this owner".into(),
        ));
    }
    if group.status == HiveGroupStatus::Archived {
        return Err(RuntimeStoreError::Invalid(
            "schedule cannot target an archived Group".into(),
        ));
    }
    definition.group_id = Some(group.id);
    Ok(())
}

fn create_recurring_schedule(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: CreateScheduleCommand,
) -> Result<Mutation, RuntimeStoreError> {
    let session = require_owned_session(tx, actor, &command.session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let mut definition = parse_schedule_definition(command.definition, now)?;
    if definition.crew_slug.is_none() {
        definition.crew_slug = tx
            .query_row(
                "SELECT crew_slug FROM hive_runtime_state WHERE session_id = ?1",
                [&session.id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
    }
    bind_schedule_worker(tx, actor, &mut definition)?;
    bind_schedule_group(tx, actor, &mut definition)?;
    // A Worker or Group target owns its execution context. Inheriting the
    // parent Hive session's workspace here would freeze an unrelated path
    // into the schedule and make a neutral target deterministically
    // unexecutable at materialization time. Ordinary schedules retain the
    // existing session-owned fallback.
    if definition.worker_id.is_none() && definition.group_id.is_none() {
        definition.project_dir = definition
            .project_dir
            .or_else(|| session.project_dir.clone())
            .or_else(|| session.working_dir.clone());
    }
    // An omitted model tuple on a Worker schedule is a durable inheritance
    // signal, not permission to copy the mutable parent-session identity.
    // The materializer resolves and freezes the Worker's exact tuple at each
    // occurrence. Ordinary schedules keep their existing session fallback.
    if !definition.model_was_explicit && definition.worker_id.is_none() {
        require_frozen_session_model(&session)?;
        definition.model.clone_from(&session.model);
        definition.model_key.clone_from(&session.model_key);
        definition
            .model_catalog_revision
            .clone_from(&session.model_catalog_revision);
    }
    if definition.project_dir.is_none()
        && definition.worker_id.is_none()
        && definition.group_id.is_none()
    {
        return Err(RuntimeStoreError::Invalid(
            "schedule requires an explicit or session-owned workspace".into(),
        ));
    }
    if definition
        .project_dir
        .as_deref()
        .is_some_and(|path| !Path::new(path).is_absolute())
    {
        return Err(RuntimeStoreError::Invalid(
            "schedule project_dir must be absolute".into(),
        ));
    }
    if definition.model.is_none() && definition.worker_id.is_none() {
        return Err(RuntimeStoreError::Invalid(
            "schedule requires an explicit or session-persisted model".into(),
        ));
    }
    let schedule_id = uuid::Uuid::new_v4().to_string();
    let recurrence_json = serde_json::to_string(&definition.recurrence)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let model_key_json = definition
        .model_key
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    tx.execute(
        "INSERT INTO hive_schedules (
            id, controller_id, title, summary, objective, recurrence_kind,
            recurrence_json, timezone, gap_policy, fold_policy, next_fire_at,
            last_scheduled_for, status, priority, project_dir, model,
            model_key_json, model_catalog_revision, crew_slug,
            misfire_policy, misfire_grace_secs, catch_up_limit, overlap_policy,
            max_attempts, retry_base_secs, retry_max_secs, retry_jitter,
            revision, created_by, created_at, updated_at, worker_id, group_id
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, NULL,
            'enabled', ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, 0, ?26, ?27, ?27, ?28, ?29
         )",
        params![
            schedule_id,
            controller.id,
            definition.title,
            definition.summary,
            definition.objective,
            definition.recurrence.kind_name(),
            recurrence_json,
            definition.timezone,
            definition.dst_policy.gap.as_str(),
            definition.dst_policy.fold.as_str(),
            definition.next_fire_at,
            definition.priority,
            definition.project_dir,
            definition.model,
            model_key_json,
            definition.model_catalog_revision,
            definition.crew_slug,
            definition.misfire.policy.as_str(),
            definition.misfire.grace_secs,
            definition.misfire.catch_up_limit as u64,
            definition.overlap_policy.as_str(),
            definition.retry.max_attempts,
            definition.retry.base_delay_secs,
            definition.retry.max_delay_secs,
            definition.retry.jitter.as_str(),
            actor.user_id.as_deref().unwrap_or("local"),
            now,
            definition.worker_id,
            definition.group_id,
        ],
    )?;
    let event = append_event(
        tx,
        &controller,
        "schedule_created",
        None,
        Some(&schedule_id),
        Some(&format!("schedule:{schedule_id}:revision:0")),
        serde_json::json!({"schedule_id": schedule_id, "revision": 0}),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::Schedule(ScheduleResponse {
            schedule_id: schedule_id.clone(),
            revision: 0,
            status: "enabled".into(),
        }),
        resource_id: Some(schedule_id),
        events: vec![event],
    })
}

fn replace_recurring_schedule(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: ReplaceScheduleCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.schedule_id, "schedule id", 256)?;
    let session = require_owned_session(tx, actor, &command.session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let mut definition = parse_schedule_definition(command.definition, now)?;
    bind_schedule_worker(tx, actor, &mut definition)?;
    bind_schedule_group(tx, actor, &mut definition)?;
    if definition.worker_id.is_none() && definition.group_id.is_none() {
        definition.project_dir = definition
            .project_dir
            .or_else(|| session.project_dir.clone())
            .or_else(|| session.working_dir.clone());
    }
    if !definition.model_was_explicit && definition.worker_id.is_none() {
        require_frozen_session_model(&session)?;
        definition.model.clone_from(&session.model);
        definition.model_key.clone_from(&session.model_key);
        definition
            .model_catalog_revision
            .clone_from(&session.model_catalog_revision);
    }
    if definition.project_dir.is_none()
        && definition.worker_id.is_none()
        && definition.group_id.is_none()
    {
        return Err(RuntimeStoreError::Invalid(
            "schedule requires an explicit or session-owned workspace".into(),
        ));
    }
    if definition
        .project_dir
        .as_deref()
        .is_some_and(|path| !Path::new(path).is_absolute())
    {
        return Err(RuntimeStoreError::Invalid(
            "schedule project_dir must be absolute".into(),
        ));
    }
    if definition.model.is_none() && definition.worker_id.is_none() {
        return Err(RuntimeStoreError::Invalid(
            "schedule requires an explicit or session-persisted model".into(),
        ));
    }
    let current = tx
        .query_row(
            "SELECT status, revision FROM hive_schedules
             WHERE id = ?1 AND controller_id = ?2",
            params![command.schedule_id, controller.id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((status, revision)) = current else {
        return Err(RuntimeStoreError::NotFound("schedule not found".into()));
    };
    if revision < 0 || revision as u64 != command.expected_revision {
        return Err(RuntimeStoreError::RevisionConflict(format!(
            "schedule revision is {revision}, not {}",
            command.expected_revision
        )));
    }
    if matches!(status.as_str(), "completed" | "cancelled") {
        return Err(RuntimeStoreError::StateConflict(format!(
            "{status} schedules cannot be replaced"
        )));
    }
    let recurrence_json = serde_json::to_string(&definition.recurrence)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let model_key_json = definition
        .model_key
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let changed = tx.execute(
        "UPDATE hive_schedules SET
            title = ?3, summary = ?4, objective = ?5, recurrence_kind = ?6,
            recurrence_json = ?7, timezone = ?8, gap_policy = ?9,
            fold_policy = ?10, next_fire_at = ?11, priority = ?12,
            project_dir = ?13, model = ?14, model_key_json = ?15,
            model_catalog_revision = ?16, crew_slug = ?17,
            misfire_policy = ?18, misfire_grace_secs = ?19,
            catch_up_limit = ?20, overlap_policy = ?21, max_attempts = ?22,
            retry_base_secs = ?23, retry_max_secs = ?24, retry_jitter = ?25,
            revision = revision + 1, updated_at = ?26, worker_id = ?27, group_id = ?28
         WHERE id = ?1 AND controller_id = ?2 AND revision = ?29",
        params![
            command.schedule_id,
            controller.id,
            definition.title,
            definition.summary,
            definition.objective,
            definition.recurrence.kind_name(),
            recurrence_json,
            definition.timezone,
            definition.dst_policy.gap.as_str(),
            definition.dst_policy.fold.as_str(),
            definition.next_fire_at,
            definition.priority,
            definition.project_dir,
            definition.model,
            model_key_json,
            definition.model_catalog_revision,
            definition.crew_slug,
            definition.misfire.policy.as_str(),
            definition.misfire.grace_secs,
            definition.misfire.catch_up_limit as u64,
            definition.overlap_policy.as_str(),
            definition.retry.max_attempts,
            definition.retry.base_delay_secs,
            definition.retry.max_delay_secs,
            definition.retry.jitter.as_str(),
            now,
            definition.worker_id,
            definition.group_id,
            command.expected_revision,
        ],
    )?;
    if changed != 1 {
        return Err(RuntimeStoreError::RevisionConflict(
            "schedule changed concurrently".into(),
        ));
    }
    let revision = command.expected_revision.saturating_add(1);
    let event = append_event(
        tx,
        &controller,
        "schedule_updated",
        None,
        Some(&command.schedule_id),
        Some(&format!(
            "schedule:{}:revision:{revision}",
            command.schedule_id
        )),
        serde_json::json!({"schedule_id": command.schedule_id, "revision": revision}),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::Schedule(ScheduleResponse {
            schedule_id: command.schedule_id.clone(),
            revision,
            status,
        }),
        resource_id: Some(command.schedule_id),
        events: vec![event],
    })
}

fn set_schedule_status(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: SetScheduleStatusCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.schedule_id, "schedule id", 256)?;
    let session = require_owned_session(tx, actor, &command.session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let current = tx
        .query_row(
            "SELECT status, revision FROM hive_schedules
             WHERE id = ?1 AND controller_id = ?2",
            params![command.schedule_id, controller.id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((current_status, revision)) = current else {
        return Err(RuntimeStoreError::NotFound("schedule not found".into()));
    };
    if revision < 0 || revision as u64 != command.expected_revision {
        return Err(RuntimeStoreError::RevisionConflict(format!(
            "schedule revision is {revision}, not {}",
            command.expected_revision
        )));
    }
    let allowed = current_status == command.status
        || matches!(
            (current_status.as_str(), command.status.as_str()),
            ("enabled", "paused" | "completed" | "cancelled") | ("paused", "enabled" | "cancelled")
        );
    if !allowed {
        return Err(RuntimeStoreError::StateConflict(format!(
            "illegal schedule transition from {current_status} to {}",
            command.status
        )));
    }
    if !matches!(
        command.status.as_str(),
        "enabled" | "paused" | "completed" | "cancelled"
    ) {
        return Err(RuntimeStoreError::Invalid("invalid schedule status".into()));
    }
    let changed = tx.execute(
        "UPDATE hive_schedules SET status = ?3, revision = revision + 1,
             updated_at = ?4
         WHERE id = ?1 AND controller_id = ?2 AND revision = ?5 AND status = ?6",
        params![
            command.schedule_id,
            controller.id,
            command.status,
            now,
            command.expected_revision,
            current_status,
        ],
    )?;
    if changed != 1 {
        return Err(RuntimeStoreError::RevisionConflict(
            "schedule changed concurrently".into(),
        ));
    }
    let revision = command.expected_revision.saturating_add(1);
    let event = append_event(
        tx,
        &controller,
        "schedule_status_changed",
        None,
        Some(&command.schedule_id),
        Some(&format!(
            "schedule:{}:revision:{revision}",
            command.schedule_id
        )),
        serde_json::json!({
            "schedule_id": command.schedule_id,
            "previous": current_status,
            "current": command.status,
            "revision": revision,
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::Schedule(ScheduleResponse {
            schedule_id: command.schedule_id.clone(),
            revision,
            status: command.status,
        }),
        resource_id: Some(command.schedule_id),
        events: vec![event],
    })
}

fn extension(
    tx: &Transaction<'_>,
    actor: &Actor,
    now: &str,
    command: mitsuro_hive_protocol::ExtensionCommand,
) -> Result<Mutation, RuntimeStoreError> {
    validate_bounded_field(&command.name, "extension name", 256)?;
    let payload_bytes = serde_json::to_vec(&command.payload).map_err(|error| {
        RuntimeStoreError::Invalid(format!("invalid extension payload: {error}"))
    })?;
    if payload_bytes.len() > 256 * 1024 {
        return Err(RuntimeStoreError::Invalid(
            "extension payload exceeds 262144 bytes".into(),
        ));
    }
    let session_id = command
        .payload
        .get("session_id")
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeStoreError::Invalid("extension requires session_id".into()))?
        .to_string();
    let session = require_owned_session(tx, actor, &session_id)?;
    let controller = get_or_create_controller(tx, &session, now)?;
    let event = append_event(
        tx,
        &controller,
        "extension_received",
        None,
        None,
        None,
        serde_json::json!({
            "name": &command.name,
            "payload_kind": json_value_kind(&command.payload),
            "payload_bytes": payload_bytes.len(),
            "top_level_fields": command.payload.as_object().map_or(0, serde_json::Map::len),
        }),
        now,
    )?;
    Ok(Mutation {
        response: ResponsePayload::Extension(ExtensionResponse {
            name: command.name,
            payload: serde_json::json!({"accepted": true}),
        }),
        resource_id: Some(session_id),
        events: vec![event],
    })
}

fn json_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

struct WorkerRunBinding<'a> {
    worker_id: &'a str,
    execution_context: &'a HiveRunExecutionContextV1,
    origin: WorkerRunOrigin,
}

#[allow(clippy::too_many_arguments)]
fn insert_run(
    tx: &Transaction<'_>,
    id: &str,
    controller_id: &str,
    session_id: Option<&str>,
    schedule_id: Option<&str>,
    occurrence_id: Option<&str>,
    kind: &str,
    objective: &str,
    config: Value,
    priority: i32,
    available_at: &str,
    max_attempts: u32,
    now: &str,
    worker_binding: Option<WorkerRunBinding<'_>>,
) -> Result<(), RuntimeStoreError> {
    let controller_worker_id = tx.query_row(
        "SELECT worker_id FROM hive_controllers WHERE id = ?1",
        [controller_id],
        |row| row.get::<_, Option<String>>(0),
    )?;
    match (controller_worker_id.as_deref(), worker_binding.as_ref()) {
        (Some(controller_worker_id), Some(binding))
            if controller_worker_id == binding.worker_id => {}
        (Some(_), Some(_)) => {
            return Err(RuntimeStoreError::StateConflict(
                "Hive Worker controller and run execution bindings disagree".into(),
            ))
        }
        (Some(_), None) => {
            return Err(RuntimeStoreError::StateConflict(
                "Hive Worker runs require a typed execution context and governor origin".into(),
            ))
        }
        (None, Some(_)) => {
            return Err(RuntimeStoreError::StateConflict(
                "typed Hive Worker run has no Worker-bound controller".into(),
            ))
        }
        (None, None) => {}
    }
    let config_json = serde_json::to_string(&config)
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    let (worker_id, governor_origin, governor_lane_key, execution_context_json) =
        match worker_binding {
            Some(binding) => (
                Some(binding.worker_id),
                Some(binding.origin.as_str()),
                Some(
                    binding
                        .execution_context
                        .lane()
                        .canonical_lane_key()
                        .map_err(RuntimeStoreError::Internal)?,
                ),
                Some(
                    serde_json::to_string(binding.execution_context)
                        .map_err(|error| RuntimeStoreError::Internal(error.into()))?,
                ),
            ),
            None => (None, None, None, None),
        };
    tx.execute(
        "INSERT INTO hive_runs (
            id, controller_id, session_id, schedule_id, occurrence_id, kind,
            objective, config_json, status, priority, concurrency_key,
            scheduled_for, available_at, wake_at, attempt_count, max_attempts,
            lease_owner, lease_token, lease_epoch, lease_expires_at, heartbeat_at,
            last_stop_reason, last_error, outcome_json, created_at, started_at,
            finished_at, updated_at, worker_id, governor_origin,
            governor_lane_key, execution_context_json
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued', ?9, NULL,
            ?10, ?10, NULL, 0, ?11, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, ?12, NULL, NULL, ?12, ?13, ?14, ?15, ?16
         )",
        params![
            id,
            controller_id,
            session_id,
            schedule_id,
            occurrence_id,
            kind,
            objective,
            config_json,
            priority,
            available_at,
            max_attempts,
            now,
            worker_id,
            governor_origin,
            governor_lane_key,
            execution_context_json,
        ],
    )?;
    Ok(())
}

pub(super) fn insert_canonical_user_message(
    tx: &Transaction<'_>,
    session_id: &str,
    message: &str,
    now: &str,
) -> Result<(), RuntimeStoreError> {
    let content = serde_json::to_string(&vec![Content::text(message.to_string())])
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    tx.execute(
        "INSERT INTO messages (session_id, role, content, created_at)
         VALUES (?1, 'user', ?2, ?3)",
        params![session_id, content, now],
    )?;
    let message_id = tx.last_insert_rowid();
    if let Some(body) = canonical_text(&content) {
        let mut hash_material = Vec::with_capacity("user".len() + 1 + body.len());
        hash_material.extend_from_slice(b"user");
        hash_material.push(0);
        hash_material.extend_from_slice(body.as_bytes());
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
                body,
                hash_request_bytes(hash_material),
                now
            ],
        )?;
    }
    tx.execute(
        "UPDATE sessions SET updated_at = ?2 WHERE id = ?1",
        params![session_id, now],
    )?;
    Ok(())
}

fn insert_pending_user_message(
    tx: &Transaction<'_>,
    session_id: &str,
    pending_id: &str,
    message: &str,
    now: &str,
) -> Result<(), RuntimeStoreError> {
    let content = serde_json::to_string(&vec![Content::text(message.to_string())])
        .map_err(|error| RuntimeStoreError::Internal(error.into()))?;
    insert_pending_user_content(tx, session_id, pending_id, &content, now)
}

pub(super) fn insert_pending_user_content(
    tx: &Transaction<'_>,
    session_id: &str,
    pending_id: &str,
    content_json: &str,
    now: &str,
) -> Result<(), RuntimeStoreError> {
    let role = format!("pending_user:{pending_id}");
    tx.execute(
        "INSERT INTO messages (session_id, role, content, created_at)
         SELECT ?1, ?2, ?3, ?4
         WHERE NOT EXISTS (
             SELECT 1 FROM messages WHERE session_id = ?1 AND role = ?2
         )",
        params![session_id, role, content_json, now],
    )?;
    tx.execute(
        "UPDATE sessions SET updated_at = ?2 WHERE id = ?1",
        params![session_id, now],
    )?;
    Ok(())
}

fn canonical_text(content_json: &str) -> Option<String> {
    let joined = serde_json::from_str::<Vec<Content>>(content_json)
        .ok()?
        .into_iter()
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text),
            _ => None,
        })
        .map(|text| text.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if joined.is_empty() {
        return None;
    }
    const MAX_EPISODE_BYTES: usize = 16 * 1024;
    if joined.len() <= MAX_EPISODE_BYTES {
        return Some(joined);
    }
    let mut boundary = MAX_EPISODE_BYTES;
    while !joined.is_char_boundary(boundary) {
        boundary -= 1;
    }
    Some(joined[..boundary].to_string())
}

fn session_mutation(
    session_id: &str,
    status: &str,
    resource_id: Option<String>,
    events: Vec<PersistedEvent>,
) -> Mutation {
    Mutation {
        response: ResponsePayload::Session(SessionResponse {
            session_id: session_id.to_string(),
            state: serde_json::json!({"status": status}),
        }),
        resource_id: resource_id.or_else(|| Some(session_id.to_string())),
        events,
    }
}

pub(super) fn ack(message: &str) -> ResponsePayload {
    ResponsePayload::Ack(AckResponse {
        accepted: true,
        message: Some(message.to_string()),
    })
}

fn priority_value(priority: &str) -> Result<i32, RuntimeStoreError> {
    match priority {
        "low" => Ok(-10),
        "normal" => Ok(0),
        "high" => Ok(50),
        "critical" => Ok(100),
        value => Err(RuntimeStoreError::Invalid(format!(
            "unsupported priority: {value}"
        ))),
    }
}

fn priority_name(priority: i32) -> &'static str {
    if priority < 0 {
        "low"
    } else if priority >= 50 {
        "high"
    } else {
        "normal"
    }
}

fn bounded_title(task: &str) -> String {
    task.chars().take(80).collect()
}

fn pending_message_id(actor: &Actor, session_id: &str, idempotency_key: &str) -> String {
    crate::legacy_identity::pending_message_id(
        actor.user_id.as_deref().unwrap_or("local"),
        &actor.client_kind,
        session_id,
        idempotency_key,
    )
}

fn steer_pending_id(
    actor: &Actor,
    session_id: &str,
    idempotency_key: &str,
    requested: Option<&str>,
) -> String {
    requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| pending_message_id(actor, session_id, idempotency_key))
}

fn control_after_commit(
    command: &Command,
    actor: &Actor,
    idempotency_key: &str,
) -> Option<(String, ExecutionControl)> {
    match command {
        Command::StartSession(command) | Command::ResumeSession(command) => {
            Some((command.session_id.clone(), ExecutionControl::Start))
        }
        Command::PauseSession(command) => Some((
            command.session_id.clone(),
            ExecutionControl::Cancel {
                reason: "paused by user".into(),
            },
        )),
        Command::SendMessage(command) => Some((
            command.session_id.clone(),
            ExecutionControl::Steer {
                pending_id: Some(pending_message_id(
                    actor,
                    &command.session_id,
                    idempotency_key,
                )),
                content: serde_json::json!([{
                    "type": "text",
                    "text": command.message,
                }]),
            },
        )),
        Command::Steer(command) => Some((
            command.session_id.clone(),
            ExecutionControl::Steer {
                pending_id: Some(steer_pending_id(
                    actor,
                    &command.session_id,
                    idempotency_key,
                    command.pending_id.as_deref(),
                )),
                content: command.content.clone(),
            },
        )),
        // Tool approvals are scheduler-delivered from hive_control_outbox.
        // Returning success means the decision is durable, even if the host
        // input channel is between registrations at commit time.
        Command::ToolApproval(_) => None,
        Command::UserResponse(command) => Some((
            command.session_id.clone(),
            ExecutionControl::UserResponse {
                run_id: command.run_id.clone(),
                pending_id: pending_message_id(actor, &command.session_id, idempotency_key),
                tool_call_id: command.tool_call_id.clone(),
                response: command.response.clone(),
            },
        )),
        Command::CancelSession(command) => Some((
            command.session_id.clone(),
            ExecutionControl::Cancel {
                reason: "cancelled by user".into(),
            },
        )),
        // Delete is accepted only after active workers are quiescent, so no
        // best-effort post-commit cancellation is necessary or safe.
        Command::DeleteSession(_) => None,
        _ => None,
    }
}

fn replay_gap_event(session_id: &str, requested_after: i64, earliest: i64) -> EventEnvelope {
    EventEnvelope {
        version: ProtocolVersion::CURRENT,
        session_id: Some(session_id.to_string()),
        run_id: None,
        sequence: None,
        emitted_at_unix_ms: unix_time_millis(),
        event: HiveEvent::ReplayGap(ReplayGapEvent {
            requested_after,
            earliest_available: earliest,
        }),
    }
}

fn lagged_event(
    session_id: &str,
    skipped: u64,
    resume_after_sequence: Option<i64>,
) -> EventEnvelope {
    EventEnvelope {
        version: ProtocolVersion::CURRENT,
        session_id: Some(session_id.to_string()),
        run_id: None,
        sequence: None,
        emitted_at_unix_ms: unix_time_millis(),
        event: HiveEvent::Lagged(LaggedEvent {
            skipped,
            resume_after_sequence,
        }),
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use mitsuro_core::storage::Database;
    use mitsuro_hive_protocol::UserResponseCommand;
    use rusqlite::TransactionBehavior;
    use tempfile::TempDir;

    fn introduction_gate_fixture(status: &str) -> (TempDir, Database) {
        let temp = TempDir::new().expect("temp dir");
        let db = Database::new(&temp.path().join("introduction-gate.db")).expect("database");
        let now = canonical_timestamp(Utc::now());
        let model_key_json =
            r#"{"provider":"grok","model_id":"test:model","api_format":"open_ai_responses"}"#;
        db.conn()
            .execute(
                "INSERT INTO sessions (
                     id, title, created_at, updated_at, working_dir, model, model_key_json,
                     workspace_mode, session_type, permission_mode
                 ) VALUES (
                     'worker-dm', 'Worker DM', ?1, ?1, NULL, 'test:model', ?2,
                     'neutral', 'hive', 'supervised'
                 )",
                params![now, model_key_json],
            )
            .expect("session");
        db.conn()
            .execute(
                "INSERT INTO hive_workers (
                     id, slug, display_name, model, model_key_json, permission_mode, autonomy,
                     status, dm_session_id, memory_namespace_id, created_at, updated_at
                 ) VALUES (
                     'worker-1', 'worker-one', 'Worker One', 'test:model', ?2,
                     'supervised', 'manual', 'active', 'worker-dm', 'worker-1', ?1, ?1
                 )",
                params![now, model_key_json],
            )
            .expect("worker");
        db.conn()
            .execute(
                "INSERT INTO hive_worker_introductions (
                     worker_id, run_id, status, prompt_version, created_at, updated_at
                 ) VALUES ('worker-1', NULL, ?1, 1, ?2, ?2)",
                params![status, now],
            )
            .expect("introduction");
        (temp, db)
    }

    fn local_worker_session(tx: &Transaction<'_>) -> super::super::persistence::OwnedSession {
        require_owned_session(tx, &Actor::local("test"), "worker-dm").expect("owned session")
    }

    #[test]
    fn user_response_control_preserves_exact_run_and_pending_identity() {
        let actor = Actor::local("test");
        let command = Command::UserResponse(UserResponseCommand {
            session_id: "session-1".into(),
            run_id: "run-a".into(),
            tool_call_id: "question-1".into(),
            response: "continue".into(),
        });

        let (session_id, control) =
            control_after_commit(&command, &actor, "response-key").expect("control");
        assert_eq!(session_id, "session-1");
        assert!(matches!(
            control,
            ExecutionControl::UserResponse {
                run_id,
                pending_id,
                tool_call_id,
                response,
            } if run_id == "run-a"
                && !pending_id.trim().is_empty()
                && tool_call_id == "question-1"
                && response == "continue"
        ));
    }

    #[test]
    fn queued_worker_introduction_rejects_every_user_content_ingress_before_mutation() {
        let (_temp, db) = introduction_gate_fixture("queued");
        let actor = Actor::local("test");
        let now = canonical_timestamp(Utc::now());

        for result in [
            {
                let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                    .expect("send transaction");
                send_message(&tx, &actor, &now, "worker-dm", "too early", "pending-send")
            },
            {
                let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                    .expect("steer transaction");
                stage_steer(
                    &tx,
                    &actor,
                    &now,
                    "worker-dm",
                    "pending-steer",
                    serde_json::json!([{"type": "text", "text": "too early"}]),
                )
            },
            {
                let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                    .expect("response transaction");
                user_response(
                    &tx,
                    &actor,
                    &now,
                    "worker-dm",
                    "run-1",
                    "question-1",
                    "too early",
                    "pending-response",
                )
            },
        ] {
            assert!(matches!(
                result,
                Err(RuntimeStoreError::StateConflict(message))
                    if message.contains("first assistant Introduction")
            ));
        }

        let (messages, controllers): (i64, i64) = db
            .conn()
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM messages WHERE session_id = 'worker-dm'),
                     (SELECT COUNT(*) FROM hive_controllers WHERE session_id = 'worker-dm')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("mutation counts");
        assert_eq!((messages, controllers), (0, 0));
    }

    #[test]
    fn review_ready_worker_introduction_freezes_stale_proposal() {
        let (_temp, db) = introduction_gate_fixture("review_ready");
        db.conn()
            .execute(
                "UPDATE hive_worker_introductions
                 SET proposal_json = json_object('proposal_id', 'proposal-1'),
                     proposal_revision = 1
                 WHERE worker_id = 'worker-1'",
                [],
            )
            .expect("proposal");
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("transaction");
        let session = local_worker_session(&tx);
        let result = enforce_worker_introduction_content_gate(&tx, &session);
        assert!(matches!(
            result,
            Err(RuntimeStoreError::StateConflict(message))
                if message.contains("confirm the proposal") && message.contains("Keep talking")
        ));
        drop(tx);
        assert_eq!(
            db.conn()
                .query_row(
                    "SELECT proposal_revision FROM hive_worker_introductions
                     WHERE worker_id = 'worker-1'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("proposal revision"),
            1
        );
    }

    #[test]
    fn awaiting_context_worker_introduction_allows_a_real_user_reply() {
        let (_temp, db) = introduction_gate_fixture("awaiting_context");
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("transaction");
        let mutation = send_message(
            &tx,
            &Actor::local("test"),
            &canonical_timestamp(Utc::now()),
            "worker-dm",
            "I want you to help me test releases carefully.",
            "pending-allowed",
        )
        .expect("awaiting-context reply accepted");
        assert!(matches!(
            mutation.response,
            ResponsePayload::WorkerConversationInput(WorkerConversationInputResponse {
                disposition: WorkerConversationInputDisposition::Queued,
                canonical_message_id: Some(_),
                staged_input_id: None,
                ..
            })
        ));
        tx.commit().expect("commit");

        let (users, runs): (i64, i64) = db
            .conn()
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM messages
                      WHERE session_id = 'worker-dm' AND role = 'user'),
                     (SELECT COUNT(*) FROM hive_runs
                      WHERE session_id = 'worker-dm'
                        AND kind = 'worker_conversation'
                        AND worker_id = 'worker-1'
                        AND governor_origin = 'user_dm'
                        AND governor_lane_key = 'dm'
                        AND objective_message_id = conversation_through_message_id)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("accepted content");
        assert_eq!((users, runs), (1, 1));
    }

    #[test]
    fn confirmed_worker_dm_atomically_queues_then_stages_without_live_pending_rows() {
        let (_temp, db) = introduction_gate_fixture("confirmed");
        db.conn()
            .execute(
                "UPDATE sessions
                 SET working_dir = NULL, project_dir = NULL, workspace_mode = 'neutral'
                 WHERE id = 'worker-dm'",
                [],
            )
            .expect("neutral Worker DM");
        let now = canonical_timestamp(Utc::now());
        db.conn()
            .execute(
                "INSERT INTO hive_controllers (
                     id, scope_key, user_id, session_id, status, timezone,
                     max_concurrent_runs, created_at, updated_at, worker_id
                 ) VALUES (
                     'worker-controller', 'worker:worker-1', NULL, 'worker-dm',
                     'active', 'UTC', 1, ?1, ?1, 'worker-1'
                 )",
                [&now],
            )
            .expect("Worker controller");
        let first = {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("first transaction");
            let mutation = send_message(
                &tx,
                &Actor::local("test"),
                &now,
                "worker-dm",
                "First exact Worker turn",
                "worker-input-first",
            )
            .expect("first Worker input");
            tx.commit().expect("first commit");
            mutation
        };
        let first_run_id = match first.response {
            ResponsePayload::WorkerConversationInput(response) => {
                assert_eq!(
                    response.disposition,
                    WorkerConversationInputDisposition::Queued
                );
                assert!(response.canonical_message_id.is_some());
                assert!(response.staged_input_id.is_none());
                response.run_id
            }
            other => panic!("expected typed Worker acceptance, got {other:?}"),
        };

        let second = {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("second transaction");
            let mutation = send_message(
                &tx,
                &Actor::local("test"),
                &now,
                "worker-dm",
                "Second serialized Worker turn",
                "worker-input-second",
            )
            .expect("second Worker input");
            tx.commit().expect("second commit");
            mutation
        };
        match second.response {
            ResponsePayload::WorkerConversationInput(response) => {
                assert_eq!(
                    response.disposition,
                    WorkerConversationInputDisposition::Staged
                );
                assert_eq!(response.run_id, first_run_id);
                assert_eq!(
                    response.staged_input_id.as_deref(),
                    Some("worker-input-second")
                );
                assert!(response.canonical_message_id.is_none());
            }
            other => panic!("expected staged Worker acceptance, got {other:?}"),
        }

        let (canonical_users, worker_runs, staged_inputs, pending_users): (i64, i64, i64, i64) = db
            .conn()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM messages
                     WHERE session_id = 'worker-dm' AND role = 'user'),
                    (SELECT COUNT(*) FROM hive_runs
                     WHERE session_id = 'worker-dm' AND kind = 'worker_conversation'
                       AND worker_id = 'worker-1'
                       AND governor_origin = 'user_dm'
                       AND governor_lane_key = 'dm'
                       AND execution_context_json IS NOT NULL),
                    (SELECT COUNT(*) FROM hive_worker_conversation_inputs
                     WHERE session_id = 'worker-dm' AND state = 'staged'),
                    (SELECT COUNT(*) FROM messages
                     WHERE session_id = 'worker-dm' AND role LIKE 'pending_user:%')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("Worker conversation projections");
        assert_eq!(
            (canonical_users, worker_runs, staged_inputs, pending_users),
            (1, 1, 1, 0)
        );
    }

    #[test]
    fn stopping_queued_worker_conversation_promotes_exact_staged_successor() {
        let (_temp, db) = introduction_gate_fixture("confirmed");
        let now = canonical_timestamp(Utc::now());
        db.conn()
            .execute(
                "INSERT INTO hive_controllers (
                     id, scope_key, user_id, session_id, status, timezone,
                     max_concurrent_runs, created_at, updated_at, worker_id
                 ) VALUES (
                     'worker-controller', 'worker:worker-1', NULL, 'worker-dm',
                     'active', 'UTC', 1, ?1, ?1, 'worker-1'
                 )",
                [&now],
            )
            .expect("Worker controller");
        let first_run_id = {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("first transaction");
            let mutation = send_message(
                &tx,
                &Actor::local("test"),
                &now,
                "worker-dm",
                "Stop this exact queued turn",
                "worker-stop-first",
            )
            .expect("first Worker input");
            tx.commit().expect("first commit");
            match mutation.response {
                ResponsePayload::WorkerConversationInput(response) => response.run_id,
                other => panic!("expected queued Worker input, got {other:?}"),
            }
        };
        {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("staged transaction");
            let mutation = send_message(
                &tx,
                &Actor::local("test"),
                &now,
                "worker-dm",
                "Run this after the stopped turn",
                "worker-stop-second",
            )
            .expect("staged Worker input");
            assert!(matches!(
                mutation.response,
                ResponsePayload::WorkerConversationInput(WorkerConversationInputResponse {
                    disposition: WorkerConversationInputDisposition::Staged,
                    ..
                })
            ));
            tx.commit().expect("staged commit");
        }
        let mutation = {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("Stop transaction");
            let mutation = stop_worker_conversation(&tx, &Actor::local("test"), &now, "worker-dm")
                .expect("queued Worker Stop");
            tx.commit().expect("Stop commit");
            mutation
        };
        assert!(matches!(
            mutation.response,
            ResponsePayload::WorkerMutation(WorkerMutationResponse {
                cancellation_requests,
                ..
            }) if cancellation_requests.is_empty()
        ));

        let projection: (String, Option<String>, i64, i64, i64, String) = db
            .conn()
            .query_row(
                "SELECT
                     (SELECT status FROM hive_runs WHERE id = ?1),
                     (SELECT last_stop_reason FROM hive_runs WHERE id = ?1),
                     (SELECT COUNT(*) FROM hive_worker_conversation_inputs
                      WHERE request_id = 'worker-stop-second'
                        AND state = 'materialized' AND assigned_run_id IS NOT NULL),
                     (SELECT COUNT(*) FROM hive_runs
                      WHERE session_id = 'worker-dm' AND worker_id = 'worker-1'
                        AND kind = 'worker_conversation' AND status = 'queued'
                        AND id <> ?1),
                     (SELECT COUNT(*) FROM hive_worker_conversation_inputs
                      WHERE session_id = 'worker-dm' AND state = 'staged'),
                     (SELECT status FROM hive_controllers
                      WHERE id = 'worker-controller')",
                [&first_run_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .expect("stopped Worker projections");
        assert_eq!(
            projection,
            (
                "cancelled".into(),
                Some(WORKER_CONVERSATION_STOP_REQUESTED_REASON.into()),
                1,
                1,
                0,
                "active".into(),
            )
        );
    }

    #[test]
    fn stopping_workspace_attached_worker_dm_preserves_workspace_and_controller() {
        let (temp, db) = introduction_gate_fixture("confirmed");
        let now = canonical_timestamp(Utc::now());
        let workspace = temp.path().join("repo");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let workspace = workspace
            .canonicalize()
            .expect("canonical workspace")
            .to_string_lossy()
            .into_owned();
        db.conn()
            .execute(
                "UPDATE sessions
                 SET workspace_mode = 'selected', working_dir = ?1, project_dir = ?1
                 WHERE id = 'worker-dm'",
                [&workspace],
            )
            .expect("attached Worker workspace");
        db.conn()
            .execute(
                "INSERT INTO hive_controllers (
                     id, scope_key, user_id, session_id, status, timezone,
                     max_concurrent_runs, created_at, updated_at, worker_id
                 ) VALUES (
                     'worker-controller', 'worker:worker-1', NULL, 'worker-dm',
                     'active', 'UTC', 1, ?1, ?1, 'worker-1'
                 )",
                [&now],
            )
            .expect("attached Worker controller");
        let run_id = {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("message transaction");
            let mutation = send_message(
                &tx,
                &Actor::local("test"),
                &now,
                "worker-dm",
                "Run in the selected workspace",
                "attached-worker-stop",
            )
            .expect("attached Worker message");
            tx.commit().expect("message commit");
            match mutation.response {
                ResponsePayload::WorkerConversationInput(response) => response.run_id,
                other => panic!("expected attached Worker run, got {other:?}"),
            }
        };
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("Stop transaction");
        stop_worker_conversation(&tx, &Actor::local("test"), &now, "worker-dm")
            .expect("attached Worker Stop");
        tx.commit().expect("Stop commit");

        let projection: (String, String, String, String) = db
            .conn()
            .query_row(
                "SELECT run.status,
                        json_extract(run.execution_context_json, '$.mode.kind'),
                        session.working_dir, controller.status
                 FROM hive_runs run
                 JOIN sessions session ON session.id = run.session_id
                 JOIN hive_controllers controller ON controller.id = run.controller_id
                 WHERE run.id = ?1",
                [&run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("attached Stop projection");
        assert_eq!(
            projection,
            (
                "cancelled".into(),
                "worker_workspace_attached".into(),
                workspace,
                "active".into(),
            )
        );
    }

    #[test]
    fn ordinary_worker_message_does_not_stage_behind_awaiting_input() {
        let (_temp, db) = introduction_gate_fixture("confirmed");
        let now = canonical_timestamp(Utc::now());
        db.conn()
            .execute(
                "INSERT INTO hive_controllers (
                     id, scope_key, user_id, session_id, status, timezone,
                     max_concurrent_runs, created_at, updated_at, worker_id
                 ) VALUES (
                     'worker-controller', 'worker:worker-1', NULL, 'worker-dm',
                     'active', 'UTC', 1, ?1, ?1, 'worker-1'
                 )",
                [&now],
            )
            .expect("Worker controller");
        let queued_run_id = {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("queue transaction");
            let mutation = send_message(
                &tx,
                &Actor::local("test"),
                &now,
                "worker-dm",
                "Start the exact Worker turn",
                "worker-input-awaiting-first",
            )
            .expect("first Worker input");
            tx.commit().expect("queue commit");
            match mutation.response {
                ResponsePayload::WorkerConversationInput(response) => response.run_id,
                other => panic!("expected typed Worker acceptance, got {other:?}"),
            }
        };
        db.conn()
            .execute(
                "UPDATE hive_runs SET status = 'awaiting_input' WHERE id = ?1",
                [&queued_run_id],
            )
            .expect("awaiting-input fixture");

        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("second transaction");
        let result = send_message(
            &tx,
            &Actor::local("test"),
            &now,
            "worker-dm",
            "This must use the explicit response path",
            "worker-input-awaiting-second",
        );
        assert!(matches!(
            result,
            Err(RuntimeStoreError::StateConflict(message))
                if message.contains("awaiting an explicit UserResponse")
        ));
        drop(tx);
        let (canonical_users, staged): (i64, i64) = db
            .conn()
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM messages
                      WHERE session_id = 'worker-dm' AND role = 'user'),
                     (SELECT COUNT(*) FROM hive_worker_conversation_inputs
                      WHERE session_id = 'worker-dm')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("awaiting-input projections");
        assert_eq!((canonical_users, staged), (1, 0));
    }

    #[test]
    fn awaiting_context_worker_introduction_rejects_non_text_steering() {
        let (_temp, db) = introduction_gate_fixture("awaiting_context");
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("transaction");
        let result = stage_steer(
            &tx,
            &Actor::local("test"),
            &canonical_timestamp(Utc::now()),
            "worker-dm",
            "pending-image",
            serde_json::json!([{
                "type": "image",
                "image": {"url": "https://example.invalid/canary.png"},
                "detail": "low"
            }]),
        );
        assert!(matches!(
            result,
            Err(RuntimeStoreError::Invalid(message)) if message.contains("text only")
        ));
        drop(tx);
        assert_eq!(
            db.conn()
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE session_id = 'worker-dm'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("message count"),
            0
        );
    }

    #[test]
    fn awaiting_context_rejects_escape_heavy_content_above_canonical_review_limit() {
        let escape_heavy = "\\\"".repeat(20_000);
        for ingress in ["message", "steer", "response"] {
            let (_temp, db) = introduction_gate_fixture("awaiting_context");
            let now = canonical_timestamp(Utc::now());
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("transaction");
            let result = match ingress {
                "message" => send_message(
                    &tx,
                    &Actor::local("test"),
                    &now,
                    "worker-dm",
                    &escape_heavy,
                    "pending-large-message",
                ),
                "steer" => stage_steer(
                    &tx,
                    &Actor::local("test"),
                    &now,
                    "worker-dm",
                    "pending-large-steer",
                    serde_json::json!([{"type": "text", "text": &escape_heavy}]),
                ),
                "response" => user_response(
                    &tx,
                    &Actor::local("test"),
                    &now,
                    "worker-dm",
                    "not-a-run",
                    "not-a-question",
                    &escape_heavy,
                    "pending-large-response",
                ),
                _ => unreachable!(),
            };
            assert!(matches!(
                result,
                Err(RuntimeStoreError::Invalid(message))
                    if message.contains("canonical review limit")
            ));
            drop(tx);
            let (messages, controllers): (i64, i64) = db
                .conn()
                .query_row(
                    "SELECT
                         (SELECT COUNT(*) FROM messages WHERE session_id = 'worker-dm'),
                         (SELECT COUNT(*) FROM hive_controllers WHERE session_id = 'worker-dm')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("no mutation counts");
            assert_eq!((messages, controllers), (0, 0));
        }
    }

    #[test]
    fn paused_and_archived_worker_reject_every_direct_content_ingress_before_mutation() {
        for worker_status in ["paused", "archived"] {
            for ingress in ["message", "steer", "response"] {
                let (_temp, db) = introduction_gate_fixture("confirmed");
                db.conn()
                    .execute(
                        "UPDATE hive_workers SET status = ?1 WHERE id = 'worker-1'",
                        [worker_status],
                    )
                    .expect("Worker status fixture");
                let now = canonical_timestamp(Utc::now());
                let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                    .expect("transaction");
                let result = match ingress {
                    "message" => send_message(
                        &tx,
                        &Actor::local("test"),
                        &now,
                        "worker-dm",
                        "must not persist",
                        "pending-inactive-message",
                    ),
                    "steer" => stage_steer(
                        &tx,
                        &Actor::local("test"),
                        &now,
                        "worker-dm",
                        "pending-inactive-steer",
                        serde_json::json!([{"type": "text", "text": "must not stage"}]),
                    ),
                    "response" => user_response(
                        &tx,
                        &Actor::local("test"),
                        &now,
                        "worker-dm",
                        "run-1",
                        "question-1",
                        "must not answer",
                        "pending-inactive-response",
                    ),
                    _ => unreachable!(),
                };
                assert!(matches!(
                    result,
                    Err(RuntimeStoreError::StateConflict(message))
                        if message.contains(worker_status)
                ));
                drop(tx);
                let (messages, controllers, runs): (i64, i64, i64) = db
                    .conn()
                    .query_row(
                        "SELECT
                             (SELECT COUNT(*) FROM messages WHERE session_id = 'worker-dm'),
                             (SELECT COUNT(*) FROM hive_controllers WHERE session_id = 'worker-dm'),
                             (SELECT COUNT(*) FROM hive_runs WHERE session_id = 'worker-dm')",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .expect("no-mutation projection");
                assert_eq!((messages, controllers, runs), (0, 0, 0));
            }
        }
    }

    #[test]
    fn claimed_worker_session_with_missing_direct_binding_fails_closed() {
        let (_temp, db) = introduction_gate_fixture("awaiting_context");
        let now = canonical_timestamp(Utc::now());
        db.conn()
            .execute(
                "UPDATE hive_workers SET dm_session_id = NULL WHERE id = 'worker-1'",
                [],
            )
            .expect("break direct binding");
        db.conn()
            .execute(
                "INSERT INTO hive_controllers (
                     id, scope_key, session_id, status, timezone,
                     max_concurrent_runs, created_at, updated_at, worker_id
                 ) VALUES (
                     'worker-controller', 'worker:worker-1', 'worker-dm', 'active',
                     'UTC', 1, ?1, ?1, 'worker-1'
                 )",
                [&now],
            )
            .expect("claimed Worker controller");
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("transaction");
        let result = send_message(
            &tx,
            &Actor::local("test"),
            &now,
            "worker-dm",
            "must fail closed",
            "pending-malformed",
        );
        assert!(matches!(
            result,
            Err(RuntimeStoreError::StateConflict(message))
                if message.contains("binding is missing or inconsistent")
        ));
        drop(tx);
        assert_eq!(
            db.conn()
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE session_id = 'worker-dm'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("message count"),
            0
        );
    }

    #[test]
    fn validated_group_worker_lane_bypasses_private_introduction_freeze() {
        let (_temp, db) = introduction_gate_fixture("queued");
        let now = canonical_timestamp(Utc::now());
        db.conn()
            .execute_batch(&format!(
                r#"
                INSERT INTO sessions (
                    id, title, created_at, updated_at, working_dir, model,
                    workspace_mode, session_type, permission_mode
                ) VALUES (
                    'group-lane', 'Group Worker Lane', '{now}', '{now}', '/work',
                    'test:model', 'neutral', 'hive', 'supervised'
                );
                INSERT INTO hive_groups (
                    id, title, execution_mode, created_at, updated_at
                ) VALUES ('group-1', 'Test Group', 'workbench', '{now}', '{now}');
                INSERT INTO hive_group_members (group_id, worker_id, position, added_at)
                VALUES ('group-1', 'worker-1', 0, '{now}');
                INSERT INTO hive_group_worker_lanes (
                    group_id, worker_id, session_id, created_at, updated_at
                ) VALUES ('group-1', 'worker-1', 'group-lane', '{now}', '{now}');
                "#
            ))
            .expect("valid group Worker lane");
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("transaction");
        let session =
            require_owned_session(&tx, &Actor::local("test"), "group-lane").expect("session");
        assert_eq!(
            enforce_worker_introduction_content_gate(&tx, &session)
                .expect("validated group lane bypass"),
            WorkerIntroductionContentGate::Ordinary
        );
    }

    #[test]
    fn worker_dm_blocks_generic_start_schedule_and_resume_before_mutation() {
        for operation in ["start", "schedule", "pause", "resume"] {
            let (_temp, db) = introduction_gate_fixture("awaiting_context");
            let now = canonical_timestamp(Utc::now());
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("transaction");
            let result = match operation {
                "start" => start_session(&tx, &Actor::local("test"), &now, "worker-dm"),
                "schedule" => schedule_session(
                    &tx,
                    &Actor::local("test"),
                    &now,
                    "worker-dm",
                    Utc::now().timestamp_millis() + 60_000,
                    "wake later",
                ),
                "pause" => {
                    set_controller_status(&tx, &Actor::local("test"), &now, "worker-dm", "paused")
                }
                "resume" => {
                    set_controller_status(&tx, &Actor::local("test"), &now, "worker-dm", "active")
                }
                _ => unreachable!(),
            };
            assert!(matches!(
                result,
                Err(RuntimeStoreError::StateConflict(message))
                    if message.contains("typed Worker chat")
            ));
            drop(tx);
            let (controllers, schedules, runs): (i64, i64, i64) = db
                .conn()
                .query_row(
                    "SELECT
                         (SELECT COUNT(*) FROM hive_controllers),
                         (SELECT COUNT(*) FROM hive_schedules),
                         (SELECT COUNT(*) FROM hive_runs)",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .expect("mutation counts");
            assert_eq!((controllers, schedules, runs), (0, 0, 0));
        }
    }

    #[test]
    fn worker_dm_and_group_lane_cannot_be_deleted_as_ordinary_sessions() {
        let (_temp, db) = introduction_gate_fixture("confirmed");
        let now = canonical_timestamp(Utc::now());
        db.conn()
            .execute_batch(&format!(
                r#"
                INSERT INTO sessions (
                    id, title, created_at, updated_at, working_dir, model,
                    workspace_mode, session_type, permission_mode
                ) VALUES (
                    'group-delete-lane', 'Group Worker Lane', '{now}', '{now}', '/work',
                    'test:model', 'neutral', 'hive', 'supervised'
                );
                INSERT INTO hive_groups (
                    id, title, execution_mode, created_at, updated_at
                ) VALUES ('group-delete', 'Delete Test Group', 'workbench', '{now}', '{now}');
                INSERT INTO hive_group_members (group_id, worker_id, position, added_at)
                VALUES ('group-delete', 'worker-1', 0, '{now}');
                INSERT INTO hive_group_worker_lanes (
                    group_id, worker_id, session_id, created_at, updated_at
                ) VALUES (
                    'group-delete', 'worker-1', 'group-delete-lane', '{now}', '{now}'
                );
                "#
            ))
            .expect("group lane");

        for (session_id, expected) in [
            ("worker-dm", "state_conflict"),
            ("group-delete-lane", "not_found"),
        ] {
            let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
                .expect("delete transaction");
            let result = delete_session(&tx, &Actor::local("test"), &now, session_id);
            match (expected, result) {
                ("state_conflict", Err(RuntimeStoreError::StateConflict(message))) => {
                    assert!(message.contains("durable product lanes"));
                }
                ("not_found", Err(RuntimeStoreError::NotFound(_))) => {}
                (_, result) => panic!("unexpected delete result for {session_id}: {result:?}"),
            }
        }
        let sessions: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sessions
                 WHERE id IN ('worker-dm', 'group-delete-lane')",
                [],
                |row| row.get(0),
            )
            .expect("protected sessions");
        assert_eq!(sessions, 2);
    }

    #[test]
    fn skip_rejects_active_or_pending_awaiting_context_turns() {
        let (_temp, db) = introduction_gate_fixture("awaiting_context");
        let now = canonical_timestamp(Utc::now());
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("active transaction");
        let session = local_worker_session(&tx);
        let controller = get_or_create_controller(&tx, &session, &now).expect("controller");
        insert_run(
            &tx,
            "active-message-turn",
            &controller.id,
            Some("worker-dm"),
            None,
            None,
            "legacy_resume",
            "answer the accepted user message",
            serde_json::json!({}),
            0,
            &now,
            5,
            &now,
            None,
        )
        .expect("message run");
        tx.execute(
            "UPDATE hive_runs SET status = 'running' WHERE id = 'active-message-turn'",
            [],
        )
        .expect("active run");
        let active = skip_worker_introduction(
            &tx,
            &Actor::local("test"),
            &now,
            WorkerIntroductionCommand {
                worker_id: "worker-1".into(),
            },
        );
        assert!(matches!(
            active,
            Err(RuntimeStoreError::StateConflict(message))
                if message.contains("active or pending user work")
        ));
        drop(tx);

        let (_temp, db) = introduction_gate_fixture("review_ready");
        let seed = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("pending seed transaction");
        insert_pending_user_message(
            &seed,
            "worker-dm",
            "pending-before-skip",
            "accepted but not answered",
            &now,
        )
        .expect("pending user message");
        seed.commit().expect("pending seed commit");
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("skip transaction");
        let pending = skip_worker_introduction(
            &tx,
            &Actor::local("test"),
            &now,
            WorkerIntroductionCommand {
                worker_id: "worker-1".into(),
            },
        );
        assert!(matches!(
            pending,
            Err(RuntimeStoreError::StateConflict(message))
                if message.contains("active or pending user work")
        ));
    }

    #[test]
    fn skip_terminalizes_an_inflight_introduction_review_claim() {
        let (_temp, db) = introduction_gate_fixture("awaiting_context");
        let now = canonical_timestamp(Utc::now());
        db.conn()
            .execute(
                "INSERT INTO messages (session_id, role, content, created_at)
                 VALUES ('worker-dm', 'assistant', '[{\"type\":\"text\",\"text\":\"opening\"}]', ?1)",
                [&now],
            )
            .expect("opening message");
        let opening_message_id = db.conn().last_insert_rowid();
        db.conn()
            .execute(
                "INSERT INTO messages (session_id, role, content, created_at)
                 VALUES ('worker-dm', 'user', '[{\"type\":\"text\",\"text\":\"context\"}]', ?1)",
                [&now],
            )
            .expect("user message");
        let user_message_id = db.conn().last_insert_rowid();
        db.conn()
            .execute(
                "INSERT INTO messages (session_id, role, content, created_at)
                 VALUES ('worker-dm', 'assistant', '[{\"type\":\"text\",\"text\":\"follow-up\"}]', ?1)",
                [&now],
            )
            .expect("assistant response");
        let through_message_id = db.conn().last_insert_rowid();
        db.conn()
            .execute(
                "UPDATE hive_worker_introductions SET opening_message_id = ?1
                 WHERE worker_id = 'worker-1'",
                [opening_message_id],
            )
            .expect("opening projection");
        db.conn()
            .execute(
                "INSERT INTO hive_worker_introduction_reviews (
                     id, worker_id, session_id, status, claim_token,
                     claim_expires_at, opening_message_id, through_message_id,
                     user_message_ids_json, transcript_digest,
                     base_identity_digest, base_soul_digest, model,
                     model_key_json, provider_id, trace_run_id, claimed_at,
                     created_at, updated_at
                 ) VALUES (
                     'review-inflight', 'worker-1', 'worker-dm', 'claimed',
                     'review-claim-token', ?1, ?2, ?3, json_array(?4),
                     'sha256:transcript', 'sha256:identity', 'sha256:soul',
                     'test:model',
                     json_object('provider', 'grok', 'model_id', 'test:model',
                                 'api_format', 'open_ai_responses'),
                     'grok', 'introduction-review:review-inflight', ?1, ?1, ?1
                 )",
                params![now, opening_message_id, through_message_id, user_message_id],
            )
            .expect("claimed review");

        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)
            .expect("skip transaction");
        skip_worker_introduction(
            &tx,
            &Actor::local("test"),
            &now,
            WorkerIntroductionCommand {
                worker_id: "worker-1".into(),
            },
        )
        .expect("skip succeeds while review provider is in flight");
        tx.commit().expect("skip commit");

        let (lifecycle, review_status, last_error, completed_at): (
            String,
            String,
            Option<String>,
            Option<String>,
        ) = db
            .conn()
            .query_row(
                "SELECT introduction.status, review.status, review.last_error,
                        review.completed_at
                 FROM hive_worker_introductions introduction
                 JOIN hive_worker_introduction_reviews review
                   ON review.worker_id = introduction.worker_id
                 WHERE introduction.worker_id = 'worker-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("terminal projection");
        assert_eq!(lifecycle, "skipped");
        assert_eq!(review_status, "stale");
        assert!(last_error
            .as_deref()
            .is_some_and(|error| error.contains("user skipped setup")));
        assert!(completed_at.is_some());
        assert_eq!(
            db.conn()
                .execute(
                    "UPDATE hive_worker_introduction_reviews
                     SET status = 'gather_more'
                     WHERE id = 'review-inflight' AND status = 'claimed'",
                    [],
                )
                .expect("post-call claim fence"),
            0,
            "a late reviewer cannot persist through the terminalized claim"
        );
    }
}
