use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use mitsuro_core::agent::{
    WorkerGoalAttemptOutcome, WorkerGoalEffectSummary, WorkerGoalEvidence, WorkerGoalEvidenceKind,
    WorkerGoalOutcomeCounters, WorkerProviderAdmission, WorkerProviderCallGovernor,
    WorkerProviderCallKind, WorkerProviderCallSlot, WorkerProviderCompletion,
    WorkerProviderGovernorBinding, WorkerProviderTerminalOutcome,
};
use mitsuro_core::ai::providers::ProviderId;
use mitsuro_core::hive::{
    canonical_timestamp, DstPolicy, HiveRunStatus, MisfireConfig, MisfireDispatch,
    MisfireResolution, RecurrenceV1, RetryPolicy,
};
use mitsuro_core::storage::{
    accept_worker_conversation_input_in_transaction, AcceptWorkerConversationInput,
    AcceptWorkerConversationInputResult, ClaimRunRequest, CommitWorkerConversationResponse,
    DaemonFence, DaemonLeaseAcquire, Database, HiveDaemonLeaseStore, HiveRunExecutionContextV1,
    HiveRunStore, HiveScheduleStore, RunCompletion, SessionManager, SessionType,
    SqliteWorkerConversationResponseStore, WorkerConversationLane, WorkerRunOrigin, WorkspaceMode,
};
use mitsuro_core::tools::registry::PermissionMode;
use mitsuro_core::workflow::{
    activate_or_resume_worker_workflow_in_transaction, WorkerWorkflowActivationRequest,
    WorkerWorkflowActivationSource,
};
use mitsuro_hive_protocol::{
    Actor, Command, ConfirmWorkerIntroductionCommand, CreateScheduleCommand,
    CreateWorkerIntroductionCommand, DispatchCommand, ExtensionCommand,
    GrantWorkerGovernorRecoveryCommand, GroupArchiveCommand, GroupMessageCommand, HiveEvent,
    MessageCommand, ModelKey, PeerIdentity, RecoverCommand, ReplaceScheduleCommand,
    ResponsePayload, ReturnWorkerIntroductionToContextCommand, ScheduleCommand, ScheduleDefinition,
    SessionCommand, SetPriorityCommand, SetWorkerStatusCommand, SetWorkerWorkspaceCommand,
    SteerCommand, SubscribeCommand, ToolApprovalCommand, UpdateWorkerCommand, UserResponseCommand,
    WorkerIntroductionCommand, WorkerIntroductionReturnDecision, WorkerIntroductionSelectedFact,
    WorkerTargetStatus, WorkerWorkspaceMode,
};
use rusqlite::{params, TransactionBehavior};
use tempfile::TempDir;
use tokio::sync::Notify;

use crate::{CommandContext, CommandHandler, HandlerReply};

use super::pump::materialize_schedule_transaction;
use super::{
    start_runtime, DurableHiveCommandHandler, ExecutionBackend, ExecutionControl, ExecutionEvent,
    ExecutionOutcome, ExecutionRequest, HiveRuntimeConfig,
};

// Each case below launches a complete scheduler with its own Tokio blocking
// pool and SQLite WAL. Running many of those daemons inside one test process
// can starve their pump tasks on constrained CI hosts, even though production
// runs one scheduler per database. Serialize this integration harness while
// leaving the lightweight protocol/server unit tests parallel.
static RUNTIME_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn runtime_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    RUNTIME_TEST_LOCK.lock().await
}

#[derive(Default)]
struct FakeBackend {
    executions: Mutex<Vec<ExecutionRequest>>,
    controls: Mutex<Vec<(String, ExecutionControl)>>,
    outcomes: Mutex<VecDeque<ExecutionOutcome>>,
}

impl FakeBackend {
    fn with_outcomes(outcomes: impl IntoIterator<Item = ExecutionOutcome>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            ..Self::default()
        }
    }

    fn execution_count(&self) -> usize {
        self.executions.lock().unwrap().len()
    }
}

#[async_trait]
impl ExecutionBackend for FakeBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        self.executions.lock().unwrap().push(request);
        self.outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| ExecutionOutcome::Succeeded {
                output: serde_json::json!({"ok": true}),
            })
    }

    async fn control(&self, session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        self.controls
            .lock()
            .unwrap()
            .push((session_id.to_string(), control));
        Ok(())
    }
}

#[derive(Default)]
struct BlockingBackend {
    executions: AtomicUsize,
    execution_dropped: AtomicBool,
    controls: Mutex<Vec<(String, ExecutionControl)>>,
}

struct ExecutionDropFlag<'a>(&'a AtomicBool);

impl Drop for ExecutionDropFlag<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct CancellableEventBackend {
    executions: AtomicUsize,
    active_run_id: Mutex<Option<String>>,
    started: Notify,
    cancel: Notify,
    cancellation_observed: Notify,
    terminal_event_accepted: Notify,
}

#[derive(Default)]
struct JournalExhaustionBackend {
    executions: AtomicUsize,
    controls: Mutex<Vec<(String, ExecutionControl)>>,
}

#[derive(Default)]
struct EventBackend;

#[derive(Default)]
struct PrivacyBoundaryBackend {
    executions: AtomicUsize,
}

#[derive(Default)]
struct DroppedProducerBackend {
    executions: AtomicUsize,
}

struct LateSteerBackend {
    database_path: PathBuf,
    executions: AtomicUsize,
    release_first: Notify,
}

struct AwaitingResponseBoundaryBackend {
    database_path: PathBuf,
    executions: AtomicUsize,
    release_first: Notify,
}

#[derive(Default)]
struct FlakyApprovalBackend {
    executions: AtomicUsize,
    approval_attempts: AtomicUsize,
    delivered: AtomicBool,
}

impl LateSteerBackend {
    fn new(database_path: PathBuf) -> Self {
        Self {
            database_path,
            executions: AtomicUsize::new(0),
            release_first: Notify::new(),
        }
    }
}

impl AwaitingResponseBoundaryBackend {
    fn new(database_path: PathBuf) -> Self {
        Self {
            database_path,
            executions: AtomicUsize::new(0),
            release_first: Notify::new(),
        }
    }
}

#[async_trait]
impl ExecutionBackend for LateSteerBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        let execution = self.executions.fetch_add(1, Ordering::SeqCst);
        if execution == 0 {
            self.release_first.notified().await;
        } else if let Some(session_id) = request.claim.run.session_id.as_deref() {
            SessionManager::new(Database::new(&self.database_path).unwrap())
                .promote_orphaned_pending_steering(session_id)
                .unwrap();
        }
        ExecutionOutcome::Succeeded {
            output: serde_json::json!({"execution": execution}),
        }
    }

    async fn control(&self, _session_id: &str, _control: ExecutionControl) -> anyhow::Result<()> {
        // Model an input channel that accepted the steer while the agent was
        // already crossing its terminal boundary. The durable staging row is
        // the source of truth, not this best-effort acknowledgement.
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for AwaitingResponseBoundaryBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        let execution = self.executions.fetch_add(1, Ordering::SeqCst);
        if execution == 0 {
            request
                .events
                .agentic(serde_json::json!({
                    "type": "awaiting_input",
                    "tool_call_id": "question-1",
                }))
                .await
                .expect("awaiting-input event should be durable");
            // Hold the first attempt after it has entered the active state.
            // The response control acknowledges delivery, then this attempt
            // models AskUser crossing its terminal boundary without consuming
            // the input.
            self.release_first.notified().await;
            ExecutionOutcome::AwaitingInput {
                details: serde_json::json!({"tool_call_id": "question-1"}),
            }
        } else {
            if let Some(session_id) = request.claim.run.session_id.as_deref() {
                SessionManager::new(Database::new(&self.database_path).unwrap())
                    .promote_orphaned_pending_steering(session_id)
                    .unwrap();
            }
            ExecutionOutcome::Succeeded {
                output: serde_json::json!({"execution": execution}),
            }
        }
    }

    async fn control(&self, _session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        if matches!(
            control,
            ExecutionControl::Steer { .. } | ExecutionControl::UserResponse { .. }
        ) {
            self.release_first.notify_one();
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for FlakyApprovalBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        self.executions.fetch_add(1, Ordering::SeqCst);
        request
            .events
            .agentic(serde_json::json!({
                "type": "tool_approval_required",
                "id": "tool-1",
            }))
            .await
            .expect("approval event should be durable");
        std::future::pending().await
    }

    async fn control(&self, _session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        if matches!(control, ExecutionControl::ToolApproval { .. }) {
            let attempt = self.approval_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                anyhow::bail!("simulated host registration race");
            }
            self.delivered.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for EventBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        for index in 0..6 {
            request
                .events
                .agentic(serde_json::json!({
                    "type": "tick_injected",
                    "tick_number": index,
                }))
                .await
                .expect("bounded execution event should be accepted");
        }
        request
            .events
            .agentic(serde_json::json!({"body": "x".repeat(2_048)}))
            .await
            .expect("oversized live event must be replaced by a bounded summary");
        ExecutionOutcome::Succeeded {
            output: serde_json::json!({"ok": true}),
        }
    }

    async fn control(&self, _session_id: &str, _control: ExecutionControl) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for PrivacyBoundaryBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        const SENTINEL: &str = "HIVE_PRIVATE_SENTINEL_91f6";
        for payload in [
            serde_json::json!({
                "type": "thinking_complete",
                "thinking": SENTINEL,
                "signature": SENTINEL,
            }),
            serde_json::json!({
                "type": "tool_call_complete",
                "id": "tool-private",
                "name": "bash",
                "arguments": {"command": SENTINEL},
            }),
            serde_json::json!({
                "type": "tool_output_delta",
                "id": "tool-private",
                "delta": SENTINEL,
            }),
            serde_json::json!({
                "type": "web_fetch_result",
                "url": "https://example.invalid/private",
                "body": SENTINEL,
            }),
        ] {
            request.events.agentic(payload).await.unwrap();
        }
        request
            .events
            .agentic(serde_json::json!({
                "type": "tool_result",
                "id": "tool-private",
                "output": SENTINEL,
                "is_error": false,
            }))
            .await
            .unwrap();
        if self.executions.fetch_add(1, Ordering::SeqCst) == 0 {
            ExecutionOutcome::Failed {
                error: format!("provider failure containing {SENTINEL}"),
                retryable: false,
                retry_after: None,
            }
        } else {
            ExecutionOutcome::Succeeded {
                output: serde_json::json!({"private": SENTINEL}),
            }
        }
    }

    async fn control(&self, _session_id: &str, _control: ExecutionControl) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for DroppedProducerBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        self.executions.fetch_add(1, Ordering::SeqCst);
        request
            .events
            .agentic(serde_json::json!({
                "type": "tool_result",
                "id": "tool-with-uncertain-side-effect",
                "output": "producer disappeared after the external write",
                "is_error": false,
            }))
            .await
            .expect("tool event should enter the bounded scheduler sink");
        ExecutionOutcome::RecoveryRequired {
            reason: "agent event producer disappeared before Finished; side effects are uncertain"
                .into(),
        }
    }

    async fn control(&self, _session_id: &str, _control: ExecutionControl) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for BlockingBackend {
    async fn execute(&self, _request: ExecutionRequest) -> ExecutionOutcome {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let _drop_flag = ExecutionDropFlag(&self.execution_dropped);
        std::future::pending().await
    }

    async fn control(&self, session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        self.controls
            .lock()
            .unwrap()
            .push((session_id.to_string(), control));
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for CancellableEventBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.active_run_id
            .lock()
            .unwrap()
            .replace(request.claim.run.id.clone());
        self.started.notify_one();
        self.cancel.notified().await;
        self.cancellation_observed.notify_one();
        request
            .events
            .agentic(serde_json::json!({
                "type": "finish",
                "session_id": request.claim.run.session_id.as_deref(),
                "stop_reason": "user_abort",
            }))
            .await
            .expect("the current cancelled run may persist its terminal event");
        self.terminal_event_accepted.notify_one();
        // Deliberately model a backend that acknowledged cancellation but
        // raced back success. The durable CancelSession commit must win.
        ExecutionOutcome::Succeeded {
            output: serde_json::json!({"ignored_cancel": true}),
        }
    }

    async fn control(&self, _session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        let active_run_id = self
            .active_run_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        match control {
            ExecutionControl::Cancel { reason } => {
                anyhow::ensure!(
                    reason == "cancelled by user",
                    "unexpected generic cancellation reason: {reason}"
                );
                anyhow::ensure!(
                    active_run_id.is_some(),
                    "generic cancellation arrived before the test backend started"
                );
                // CancelSession delivers this generic control synchronously
                // after its durable commit. It is the deterministic signal
                // for this terminal-event canary; the pump's exact CancelRun
                // is a separate best-effort optimization.
                self.cancel.notify_one();
            }
            ExecutionControl::CancelRun { run_id, .. } => {
                let Some(active_run_id) = active_run_id else {
                    // Runtime teardown may fence a queued claim whose backend
                    // future never started. There is no hosted execution to
                    // wake, and cleanup must not obscure the primary failure.
                    return Ok(());
                };
                anyhow::ensure!(
                    active_run_id == run_id,
                    "CancelRun targeted {run_id}, but the test backend hosts {active_run_id}"
                );
            }
            _ => {}
        }
        Ok(())
    }
}

#[async_trait]
impl ExecutionBackend for JournalExhaustionBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        self.executions.fetch_add(1, Ordering::SeqCst);
        request
            .events
            .emit(ExecutionEvent {
                event_type: "agentic_event".into(),
                payload: serde_json::json!({"type": "finish"}),
                // This fits the execution channel's configured byte bound but
                // deliberately exceeds the tighter durable journal contract
                // after allow-list sanitization.
                durable_payload: Some(serde_json::json!({
                    "type": "finish",
                    "session_id": "s".repeat(16 * 1024),
                })),
            })
            .await
            .expect("the event should reach the scheduler persistence boundary");
        std::future::pending().await
    }

    async fn control(&self, session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        self.controls
            .lock()
            .unwrap()
            .push((session_id.to_string(), control));
        Ok(())
    }
}

fn config(temp: &TempDir) -> HiveRuntimeConfig {
    let mut config = HiveRuntimeConfig::for_database(temp.path().join("runtime.db"));
    config.scheduler_poll_interval = Duration::from_millis(20);
    // Keep the shared integration-test leases short enough for fast feedback,
    // but long enough that normal CI scheduling and SQLite retention work do
    // not impersonate a crashed daemon. Tests that exercise real expiry use
    // explicit, tighter durations below.
    config.daemon_lease_duration = Duration::from_secs(2);
    config.worker_lease_duration = Duration::from_secs(2);
    config.worker_heartbeat_interval = Duration::from_millis(100);
    config.cancellation_grace_period = Duration::from_millis(200);
    config.live_event_capacity = 8;
    config.subscriber_capacity = 8;
    config
}

fn context(actor: Actor, key: &str) -> CommandContext {
    CommandContext {
        request_id: format!("request-{key}"),
        idempotency_key: key.to_string(),
        actor,
        deadline_unix_ms: i64::MAX,
        peer: PeerIdentity {
            uid: 1,
            gid: 1,
            pid: None,
        },
        daemon_instance_id: "daemon-test".into(),
    }
}

fn dispatch_command() -> Command {
    Command::Dispatch(DispatchCommand {
        task: "Audit the repository".into(),
        working_dir: "/work/repo".into(),
        project_dir: Some("/work/repo".into()),
        model: Some("test:model".into()),
        model_key: Some(ModelKey {
            provider: "grok".into(),
            model_id: "test:model".into(),
            auth_scope: Some("oauth".into()),
            api_format: "open_ai_responses".into(),
        }),
        model_catalog_revision: Some("catalog-42".into()),
        start_at_unix_ms: None,
        priority: Some("normal".into()),
        crew_slug: None,
    })
}

fn worker_introduction_command() -> Command {
    Command::CreateWorkerIntroduction(CreateWorkerIntroductionCommand {
        slug: "tester-friend".into(),
        display_name: "Tester Friend".into(),
        avatar_color: Some("#7743DB".into()),
        model: "test:model".into(),
        model_key: ModelKey {
            provider: "grok".into(),
            model_id: "test:model".into(),
            auth_scope: Some("oauth".into()),
            api_format: "open_ai_responses".into(),
        },
        model_catalog_revision: Some("catalog-42".into()),
        permission_mode: "supervised".into(),
        autonomy: "manual".into(),
        heartbeat_interval_secs: None,
        identity: Some("A careful testing collaborator.".into()),
        soul: None,
    })
}

fn seed_expired_worker_introduction(db: &Database, with_exact_opening: bool) -> Option<i64> {
    let now = canonical_timestamp(chrono::Utc::now());
    let expired = canonical_timestamp(chrono::Utc::now() - chrono::Duration::seconds(1));
    let model_key = serde_json::json!({
        "provider": "grok",
        "model_id": "test:model",
        "auth_scope": "oauth",
        "api_format": "open_ai_responses",
    });
    let model_key_json = model_key.to_string();
    let run_config_json = serde_json::json!({
        "model": "test:model",
        "model_key": model_key,
        "model_catalog_revision": "catalog-42",
        "permission_mode": "supervised",
        "worker_id": "introduction-worker",
    })
    .to_string();
    let execution_context = serde_json::to_string(
        &mitsuro_core::storage::HiveRunExecutionContextV1::worker_conversation_neutral(
            "introduction-worker",
            1,
            mitsuro_core::storage::WorkerConversationLane::DirectMessage,
        )
        .expect("Introduction context"),
    )
    .expect("serialize Introduction context");
    db.conn()
        .execute_batch(&format!(
            r#"
            INSERT INTO sessions (
                id, title, created_at, updated_at, model, model_key_json,
                model_catalog_revision, workspace_mode, session_type,
                permission_mode
            ) VALUES (
                'introduction-session', 'Tester Friend', '{now}', '{now}',
                'test:model', '{model_key_json}', 'catalog-42', 'neutral',
                'hive', 'supervised'
            );
            INSERT INTO hive_workers (
                id, slug, display_name, model, model_key_json,
                model_catalog_revision, permission_mode, autonomy, status,
                dm_session_id, memory_namespace_id, created_at, updated_at
            ) VALUES (
                'introduction-worker', 'tester-friend', 'Tester Friend',
                'test:model', '{model_key_json}', 'catalog-42', 'supervised',
                'manual', 'active',
                'introduction-session', 'introduction-worker', '{now}', '{now}'
            );
            INSERT INTO hive_controllers (
                id, scope_key, session_id, status, timezone, max_concurrent_runs,
                created_at, updated_at, worker_id
            ) VALUES (
                'introduction-controller', 'worker:introduction-worker',
                'introduction-session', 'active', 'UTC', 1, '{now}', '{now}',
                'introduction-worker'
            );
            INSERT INTO hive_runs (
                id, controller_id, session_id, kind, objective, config_json, status,
                priority, available_at, attempt_count, max_attempts, lease_owner,
                lease_token, lease_epoch, lease_expires_at, heartbeat_at,
                created_at, started_at, updated_at, worker_id,
                governor_origin, governor_lane_key, execution_context_json
            ) VALUES (
                'introduction-run', 'introduction-controller', 'introduction-session',
                'worker_introduction', 'Begin the one-time Worker Introduction',
                '{run_config_json}', 'running', 0, '{now}',
                1, 1, 'old-daemon', 'old-token', 1, '{expired}', '{expired}',
                '{now}', '{now}', '{now}', 'introduction-worker',
                'user_lifecycle_action', 'dm', '{execution_context}'
            );
            INSERT INTO hive_run_attempts (
                id, run_id, attempt_no, executor_id, lease_token, lease_epoch,
                started_at, outcome
            ) VALUES (
                'introduction-attempt', 'introduction-run', 1, 'old-daemon',
                'old-token', 1, '{now}', 'leased'
            );
            INSERT INTO hive_worker_introductions (
                worker_id, run_id, status, prompt_version, created_at, updated_at
            ) VALUES (
                'introduction-worker', 'introduction-run', 'running', 1,
                '{now}', '{now}'
            );
            INSERT INTO hive_runtime_state (
                session_id, status, current_run_id, worker_id, updated_at
            ) VALUES (
                'introduction-session', 'running', 'introduction-run',
                'introduction-worker', '{now}'
            );
            "#
        ))
        .unwrap();
    with_exact_opening.then(|| {
        db.conn()
            .execute(
                "INSERT INTO messages (
                     session_id, role, content, created_at, idempotency_key
                 ) VALUES (
                     'introduction-session', 'assistant',
                     '[{\"type\":\"text\",\"text\":\"What would you like us to build together?\"}]',
                     ?1, 'introduction:introduction-run:opening'
                 )",
                [&now],
            )
            .unwrap();
        db.conn().last_insert_rowid()
    })
}

async fn response(
    handler: &dyn CommandHandler,
    context: CommandContext,
    command: Command,
) -> ResponsePayload {
    match handler.handle(context, command).await.unwrap() {
        HandlerReply::Response(response) => *response,
        HandlerReply::Subscription { .. } => panic!("expected response"),
    }
}

fn dispatch_session_id(response: &ResponsePayload) -> String {
    match response {
        ResponsePayload::Dispatch(response) => response.session_id.clone(),
        response => panic!("expected dispatch response, got {response:?}"),
    }
}

async fn wait_for(condition: impl Fn() -> bool) {
    // The runtime polls every 20ms in tests, but a full workspace test run can
    // initialize many isolated SQLite databases concurrently. Keep this bound
    // finite while allowing slow CI hosts to make durable scheduler progress.
    tokio::time::timeout(Duration::from_secs(30), async {
        while !condition() {
            // Most predicates reopen SQLite. Poll slowly enough that the
            // observation connection cannot starve the writer transaction it
            // is waiting for when tests run in parallel.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_runtime_health(
    handler: &DurableHiveCommandHandler,
    pump_alive: bool,
    scheduler_ready: bool,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let actor = Actor::local("health-test");
        loop {
            let stats = handler.runtime_stats(&actor).await;
            if stats.pump_alive == pump_alive && stats.scheduler_ready == scheduler_ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("runtime health did not reach the expected state");
}

#[tokio::test]
async fn foreign_scheduler_lease_keeps_runtime_not_ready_until_takeover() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let lease =
        match HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .acquire(
                "hive-scheduler",
                "foreign-daemon",
                chrono::Utc::now(),
                Duration::from_secs(30),
            )
            .unwrap()
        {
            DaemonLeaseAcquire::Acquired(lease) => lease,
            other => panic!("expected foreign lease acquisition, got {other:?}"),
        };

    let backend = Arc::new(FakeBackend::default());
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-b",
        Arc::clone(&backend) as Arc<dyn ExecutionBackend>,
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, false).await;

    assert!(
        HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .release("hive-scheduler", "foreign-daemon", lease.fencing_token,)
            .unwrap()
    );
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn forced_pump_exit_marks_health_down_and_surfaces_supervision_failure() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let mut runtime = start_runtime(config(&temp), "daemon-a", Arc::new(FakeBackend::default()))
        .await
        .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;

    runtime.force_pump_exit_for_test();
    wait_for_runtime_health(handler.as_ref(), false, false).await;
    let failure =
        tokio::time::timeout(Duration::from_secs(1), runtime.wait_for_scheduler_failure())
            .await
            .expect("scheduler supervisor did not observe the forced exit");
    assert!(failure.to_string().contains("scheduler pump stopped"));
}

#[tokio::test]
async fn worker_introduction_create_is_atomic_idempotent_and_skip_fences_running() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(BlockingBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    let command = worker_introduction_command();
    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "create-introduction"),
        command.clone(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "create-introduction"),
        command,
    )
    .await;
    assert_eq!(first, replay);
    let created = match first {
        ResponsePayload::WorkerIntroduction(response) => response,
        other => panic!("expected Worker Introduction response, got {other:?}"),
    };

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (worker_count, session_count, run_count, introduction_count, message_count): (
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM hive_workers WHERE id = ?1 AND dm_session_id = ?2),
                 (SELECT COUNT(*) FROM sessions WHERE id = ?2),
                 (SELECT COUNT(*) FROM hive_runs WHERE id = ?3
                    AND worker_id = ?1 AND kind = 'worker_introduction'
                    AND max_attempts = 1),
                 (SELECT COUNT(*) FROM hive_worker_introductions
                    WHERE worker_id = ?1 AND run_id = ?3),
                 (SELECT COUNT(*) FROM messages WHERE session_id = ?2)",
            rusqlite::params![created.worker_id, created.session_id, created.run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        (
            worker_count,
            session_count,
            run_count,
            introduction_count,
            message_count
        ),
        (1, 1, 1, 1, 0)
    );
    drop(db);

    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    let skipped = response(
        handler.as_ref(),
        context(Actor::local("test"), "skip-introduction"),
        Command::SkipWorkerIntroduction(WorkerIntroductionCommand {
            worker_id: created.worker_id.clone(),
        }),
    )
    .await;
    let skipped = match skipped {
        ResponsePayload::WorkerIntroductionAction(response) => response,
        other => panic!("expected Introduction action response, got {other:?}"),
    };
    assert_eq!(skipped.status, "skipped");
    assert!(skipped.autonomy_eligible);
    assert!(skipped.cancellation_requested);
    wait_for(|| backend.execution_dropped.load(Ordering::SeqCst)).await;
    assert!(backend
        .controls
        .lock()
        .unwrap()
        .iter()
        .any(|(controlled_session, control)| {
            controlled_session == &created.session_id
                && matches!(
                    control,
                    ExecutionControl::CancelRun { run_id, reason }
                        if run_id == &created.run_id && reason == "cancelled by user"
                )
        }));

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (run_status, introduction_status, controller_status, message_count): (
        String,
        String,
        String,
        i64,
    ) = db
        .conn()
        .query_row(
            "SELECT run.status, introduction.status, controller.status,
                    (SELECT COUNT(*) FROM messages WHERE session_id = ?2)
             FROM hive_runs run
             JOIN hive_worker_introductions introduction
               ON introduction.run_id = run.id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE run.id = ?1",
            rusqlite::params![created.run_id, created.session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(run_status, "cancelled");
    assert_eq!(introduction_status, "skipped");
    assert_eq!(controller_status, "active");
    assert_eq!(message_count, 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn explicit_introduction_retry_replaces_only_a_recovery_required_empty_run() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(FakeBackend::with_outcomes([
        ExecutionOutcome::RecoveryRequired {
            reason: "ambiguous provider boundary".into(),
        },
    ]));
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    let created = match response(
        handler.as_ref(),
        context(Actor::local("test"), "create-retry-fixture"),
        worker_introduction_command(),
    )
    .await
    {
        ResponsePayload::WorkerIntroduction(response) => response,
        other => panic!("expected Worker Introduction response, got {other:?}"),
    };
    wait_for(|| {
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT status = 'recovery_required' FROM hive_runs WHERE id = ?1",
                [&created.run_id],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
    })
    .await;
    Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_worker_introductions
             SET status = 'needs_recovery', last_error = 'ambiguous provider boundary'
             WHERE worker_id = ?1 AND run_id = ?2",
            rusqlite::params![created.worker_id, created.run_id],
        )
        .unwrap();

    let retry_command = Command::RetryWorkerIntroduction(WorkerIntroductionCommand {
        worker_id: created.worker_id.clone(),
    });
    let retried = response(
        handler.as_ref(),
        context(Actor::local("test"), "retry-introduction"),
        retry_command.clone(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "retry-introduction"),
        retry_command,
    )
    .await;
    assert_eq!(retried, replay);
    let retried = match retried {
        ResponsePayload::WorkerIntroductionAction(response) => response,
        other => panic!("expected Introduction action response, got {other:?}"),
    };
    let retry_run_id = retried.run_id.clone().expect("retry run id");
    assert_ne!(retry_run_id, created.run_id);
    assert_eq!(retried.status, "queued");
    assert!(!retried.autonomy_eligible);
    assert!(!retried.cancellation_requested);

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (old_status, ledger_run_id, new_kind, max_attempts, config_json, messages): (
        String,
        String,
        String,
        i64,
        String,
        i64,
    ) = db
        .conn()
        .query_row(
            "SELECT old.status, introduction.run_id, new.kind, new.max_attempts,
                    new.config_json,
                    (SELECT COUNT(*) FROM messages WHERE session_id = ?4)
             FROM hive_runs old
             JOIN hive_worker_introductions introduction
               ON introduction.worker_id = ?3
             JOIN hive_runs new ON new.id = introduction.run_id
             WHERE old.id = ?1 AND new.id = ?2",
            rusqlite::params![
                created.run_id,
                retry_run_id,
                created.worker_id,
                created.session_id
            ],
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
        .unwrap();
    assert_eq!(old_status, "cancelled");
    assert_eq!(ledger_run_id, retry_run_id);
    assert_eq!(new_kind, "worker_introduction");
    assert_eq!(max_attempts, 1);
    assert_eq!(messages, 0);
    let config: serde_json::Value = serde_json::from_str(&config_json).unwrap();
    assert_eq!(config["model"], "test:model");
    assert_eq!(config["model_key"]["provider"], "grok");
    assert_eq!(config["retry"]["max_attempts"], 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn reviewed_introduction_keep_talking_is_owner_bound_idempotent_and_event_deduped() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
        .acquire(
            "hive-scheduler",
            "foreign-daemon",
            chrono::Utc::now(),
            Duration::from_secs(30),
        )
        .unwrap();
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-under-test",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let created = match response(
        handler.as_ref(),
        context(Actor::local("test"), "create-review-decision-fixture"),
        worker_introduction_command(),
    )
    .await
    {
        ResponsePayload::WorkerIntroduction(response) => response,
        other => panic!("expected Worker Introduction response, got {other:?}"),
    };

    let now = chrono::Utc::now().to_rfc3339();
    let proposal = serde_json::json!({
        "schema_version": 1,
        "proposal_id": "proposal-1",
        "revision": 1,
        "worker_id": created.worker_id.clone(),
        "session_id": created.session_id.clone(),
        "basis": {
            "opening_message_id": 1,
            "through_message_id": 3,
            "user_message_ids": [2],
            "transcript_digest": "transcript-digest"
        },
        "base_identity_digest": "identity-digest",
        "base_soul_digest": "soul-digest",
        "facts": [{
            "fact_id": "fact-1",
            "kind": "purpose",
            "statement": "Help verify runtime reliability.",
            "evidence_message_id": 2,
            "evidence_excerpt": "verify runtime reliability"
        }]
    });
    let db = Database::new(&runtime_config.database_path).unwrap();
    db.conn()
        .execute(
            "UPDATE hive_worker_introductions
             SET status = 'review_ready', proposal_json = ?2,
                 proposal_revision = 1, updated_at = ?3
             WHERE worker_id = ?1",
            rusqlite::params![created.worker_id, proposal.to_string(), now],
        )
        .unwrap();
    let model_key_json: String = db
        .conn()
        .query_row(
            "SELECT model_key_json FROM hive_workers WHERE id = ?1",
            [&created.worker_id],
            |row| row.get(0),
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest, base_identity_digest,
                 base_soul_digest, worker_user_id, model, model_key_json,
                 model_catalog_revision, provider_id, trace_run_id, proposal_id,
                 proposal_revision, proposal_json, claimed_at, created_at,
                 updated_at, completed_at
             ) VALUES (
                 'review-1', ?1, ?2, 'review_ready', 'claim-1', ?3,
                 1, 3, '[2]', 'transcript-digest', 'identity-digest',
                 'soul-digest', NULL, 'test:model', ?4, 'catalog-42',
                 'grok', 'introduction-review:review-1', 'proposal-1', 1,
                 ?5, ?3, ?3, ?3, ?3
             )",
            rusqlite::params![
                created.worker_id,
                created.session_id,
                now,
                model_key_json,
                proposal.to_string()
            ],
        )
        .unwrap();
    drop(db);

    let command =
        Command::ReturnWorkerIntroductionToContext(ReturnWorkerIntroductionToContextCommand {
            worker_id: created.worker_id.clone(),
            proposal_id: "proposal-1".into(),
            proposal_revision: 1,
            decision: WorkerIntroductionReturnDecision::KeepTalking,
        });
    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "keep-talking-1"),
        command.clone(),
    )
    .await;
    let receipt_replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "keep-talking-1"),
        command.clone(),
    )
    .await;
    let semantic_replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "keep-talking-2"),
        command,
    )
    .await;
    assert_eq!(first, receipt_replay);
    assert_eq!(first, semantic_replay);
    let action = match first {
        ResponsePayload::WorkerIntroductionAction(response) => response,
        other => panic!("expected Introduction action response, got {other:?}"),
    };
    assert_eq!(action.status, "awaiting_context");
    assert!(!action.autonomy_eligible);

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (lifecycle, proposal_cleared, review_status, event_count): (String, bool, String, i64) = db
        .conn()
        .query_row(
            "SELECT introduction.status, introduction.proposal_json IS NULL,
                    review.status,
                    (SELECT COUNT(*) FROM hive_controller_events
                     WHERE event_type = 'worker_introduction_keep_talking')
             FROM hive_worker_introductions introduction
             JOIN hive_worker_introduction_reviews review
               ON review.worker_id = introduction.worker_id
             WHERE introduction.worker_id = ?1",
            [&created.worker_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(lifecycle, "awaiting_context");
    assert!(proposal_cleared);
    assert_eq!(review_status, "keep_talking");
    assert_eq!(event_count, 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn reviewed_introduction_commands_reject_foreign_actor_and_invalid_selection() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
        .acquire(
            "hive-scheduler",
            "foreign-daemon",
            chrono::Utc::now(),
            Duration::from_secs(30),
        )
        .unwrap();
    let runtime = start_runtime(
        runtime_config,
        "daemon-under-test",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let created = match response(
        handler.as_ref(),
        context(Actor::local("test"), "create-review-guard-fixture"),
        worker_introduction_command(),
    )
    .await
    {
        ResponsePayload::WorkerIntroduction(response) => response,
        other => panic!("expected Worker Introduction response, got {other:?}"),
    };

    let invalid = handler
        .handle(
            context(Actor::local("test"), "invalid-review-selection"),
            Command::ConfirmWorkerIntroduction(ConfirmWorkerIntroductionCommand {
                worker_id: created.worker_id.clone(),
                proposal_id: "proposal-1".into(),
                proposal_revision: 1,
                selected_facts: Vec::new(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(invalid.code, "invalid_command");

    let foreign = handler
        .handle(
            context(
                Actor {
                    user_id: Some("alice".into()),
                    client_kind: "test".into(),
                },
                "foreign-review-decision",
            ),
            Command::ConfirmWorkerIntroduction(ConfirmWorkerIntroductionCommand {
                worker_id: created.worker_id,
                proposal_id: "proposal-1".into(),
                proposal_revision: 1,
                selected_facts: vec![WorkerIntroductionSelectedFact {
                    fact_id: "fact-1".into(),
                    final_statement: "Help verify runtime reliability.".into(),
                }],
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign.code, "ownership_denied");
    runtime.shutdown().await;
}

#[tokio::test]
async fn introduction_retry_rejects_foreign_owner_model_drift_and_nonempty_dm() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let foreign_lease =
        match HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .acquire(
                "hive-scheduler",
                "foreign-daemon",
                chrono::Utc::now(),
                Duration::from_secs(30),
            )
            .unwrap()
        {
            DaemonLeaseAcquire::Acquired(lease) => lease,
            other => panic!("expected foreign scheduler lease, got {other:?}"),
        };
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-under-test",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let created = match response(
        handler.as_ref(),
        context(Actor::local("test"), "create-retry-guard-fixture"),
        worker_introduction_command(),
    )
    .await
    {
        ResponsePayload::WorkerIntroduction(response) => response,
        other => panic!("expected Worker Introduction response, got {other:?}"),
    };
    let db = Database::new(&runtime_config.database_path).unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs
             SET status = 'failed', finished_at = updated_at
             WHERE id = ?1",
            [&created.run_id],
        )
        .unwrap();
    db.conn()
        .execute(
            "UPDATE hive_worker_introductions
             SET status = 'failed', last_error = 'provider rejected request'
             WHERE worker_id = ?1 AND run_id = ?2",
            rusqlite::params![created.worker_id, created.run_id],
        )
        .unwrap();
    drop(db);

    let retry = || {
        Command::RetryWorkerIntroduction(WorkerIntroductionCommand {
            worker_id: created.worker_id.clone(),
        })
    };
    let foreign_error = handler
        .handle(
            context(
                Actor {
                    user_id: Some("alice".into()),
                    client_kind: "test".into(),
                },
                "foreign-introduction-retry",
            ),
            retry(),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign_error.code, "ownership_denied");

    Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_workers SET model = 'different:model' WHERE id = ?1",
            [&created.worker_id],
        )
        .unwrap();
    let drift_error = handler
        .handle(
            context(Actor::local("test"), "model-drift-introduction-retry"),
            retry(),
        )
        .await
        .unwrap_err();
    assert_eq!(drift_error.code, "state_conflict");

    let db = Database::new(&runtime_config.database_path).unwrap();
    db.conn()
        .execute(
            "UPDATE hive_workers SET model = 'test:model' WHERE id = ?1",
            [&created.worker_id],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES (?1, 'user', '[]', ?2)",
            rusqlite::params![created.session_id, canonical_timestamp(chrono::Utc::now())],
        )
        .unwrap();
    drop(db);
    let transcript_error = handler
        .handle(
            context(Actor::local("test"), "nonempty-introduction-retry"),
            retry(),
        )
        .await
        .unwrap_err();
    assert_eq!(transcript_error.code, "state_conflict");

    runtime.shutdown().await;
    HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
        .release(
            "hive-scheduler",
            "foreign-daemon",
            foreign_lease.fencing_token,
        )
        .unwrap();
}

#[tokio::test]
async fn restart_adopts_committed_worker_introduction_opening_without_backend_replay() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let db = Database::new(&runtime_config.database_path).unwrap();
    let opening_message_id = seed_expired_worker_introduction(&db, true).unwrap();
    drop(db);

    let backend = Arc::new(FakeBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-new", backend.clone())
        .await
        .unwrap();
    wait_for(|| {
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT EXISTS(
                     SELECT 1
                     FROM hive_runs run
                     JOIN hive_worker_introductions introduction
                       ON introduction.run_id = run.id
                     WHERE run.id = 'introduction-run'
                       AND run.status = 'succeeded'
                       AND introduction.status = 'awaiting_context'
                       AND EXISTS (
                           SELECT 1 FROM hive_controller_events event
                           WHERE event.run_id = run.id
                             AND event.event_type = 'run_completed'
                       )
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        backend.execution_count(),
        0,
        "reconciliation must not invoke the provider backend"
    );

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (
        run_status,
        introduction_status,
        persisted_opening_id,
        attempt_outcome,
        runtime_status,
        controller_status,
        finished,
        message_count,
        completion_events,
    ): (
        String,
        String,
        Option<i64>,
        String,
        String,
        String,
        bool,
        i64,
        i64,
    ) = db
        .conn()
        .query_row(
            "SELECT run.status, introduction.status,
                    introduction.opening_message_id, attempt.outcome,
                    runtime.status, controller.status,
                    run.finished_at IS NOT NULL,
                    (SELECT COUNT(*) FROM messages message
                     WHERE message.session_id = run.session_id
                       AND message.role = 'assistant'
                       AND message.idempotency_key =
                           'introduction:' || run.id || ':opening'),
                    (SELECT COUNT(*) FROM hive_controller_events event
                     WHERE event.run_id = run.id
                       AND event.event_type = 'run_completed'
                       AND json_extract(event.payload_json, '$.status') = 'succeeded')
             FROM hive_runs run
             JOIN hive_worker_introductions introduction
               ON introduction.run_id = run.id
             JOIN hive_run_attempts attempt ON attempt.run_id = run.id
             JOIN hive_runtime_state runtime ON runtime.session_id = run.session_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE run.id = 'introduction-run'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(run_status, "succeeded");
    assert_eq!(introduction_status, "awaiting_context");
    assert_eq!(persisted_opening_id, Some(opening_message_id));
    assert_eq!(attempt_outcome, "succeeded");
    assert_eq!(runtime_status, "idle");
    assert_eq!(controller_status, "active");
    assert!(finished);
    assert_eq!(message_count, 1);
    assert_eq!(completion_events, 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn restart_quarantines_worker_introduction_without_exact_opening_and_never_replays() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(seed_expired_worker_introduction(&db, false), None);
    drop(db);

    let backend = Arc::new(FakeBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-new", backend.clone())
        .await
        .unwrap();
    wait_for(|| {
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT EXISTS(
                     SELECT 1
                     FROM hive_runs run
                     JOIN hive_worker_introductions introduction
                       ON introduction.run_id = run.id
                     WHERE run.id = 'introduction-run'
                       AND run.status = 'recovery_required'
                       AND introduction.status = 'needs_recovery'
                       AND EXISTS (
                           SELECT 1 FROM hive_controller_events event
                           WHERE event.run_id = run.id
                             AND event.event_type = 'recovery_required'
                       )
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        backend.execution_count(),
        0,
        "an uncertain Introduction must await explicit retry or skip"
    );

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (
        run_status,
        introduction_status,
        introduction_error,
        attempt_outcome,
        runtime_status,
        controller_status,
        message_count,
        completion_events,
        recovery_events,
    ): (
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
        i64,
        i64,
    ) = db
        .conn()
        .query_row(
            "SELECT run.status, introduction.status, introduction.last_error,
                    attempt.outcome, runtime.status, controller.status,
                    (SELECT COUNT(*) FROM messages message
                     WHERE message.session_id = run.session_id),
                    (SELECT COUNT(*) FROM hive_controller_events event
                     WHERE event.run_id = run.id AND event.event_type = 'run_completed'),
                    (SELECT COUNT(*) FROM hive_controller_events event
                     WHERE event.run_id = run.id
                       AND event.event_type = 'recovery_required')
             FROM hive_runs run
             JOIN hive_worker_introductions introduction
               ON introduction.run_id = run.id
             JOIN hive_run_attempts attempt ON attempt.run_id = run.id
             JOIN hive_runtime_state runtime ON runtime.session_id = run.session_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE run.id = 'introduction-run'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(run_status, "recovery_required");
    assert_eq!(introduction_status, "needs_recovery");
    assert!(introduction_error.contains("explicit retry or skip"));
    assert_eq!(attempt_outcome, "recovery_required");
    assert_eq!(runtime_status, "error");
    assert_eq!(controller_status, "paused");
    assert_eq!(message_count, 0);
    assert_eq!(completion_events, 0);
    assert_eq!(recovery_events, 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn manual_recover_defers_expired_worker_introduction_to_scheduler_reconciliation() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let db = Database::new(&runtime_config.database_path).unwrap();
    let opening_message_id = seed_expired_worker_introduction(&db, true).unwrap();
    drop(db);
    let lease_store = HiveDaemonLeaseStore::new(
        Database::new(&runtime_config.database_path).expect("foreign lease database"),
    );
    let foreign = match lease_store
        .acquire(
            "hive-scheduler",
            "foreign-daemon",
            chrono::Utc::now(),
            Duration::from_secs(30),
        )
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        other => panic!("expected foreign scheduler lease, got {other:?}"),
    };

    let backend = Arc::new(FakeBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-new", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, false).await;
    let recovered = response(
        handler.as_ref(),
        context(Actor::local("test"), "manual-introduction-recover"),
        Command::Recover(RecoverCommand { session_id: None }),
    )
    .await;
    match recovered {
        ResponsePayload::Recover(response) => assert_eq!(response.recovered_count, 0),
        other => panic!("expected recover response, got {other:?}"),
    }
    assert_eq!(backend.execution_count(), 0);

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (
        run_status,
        introduction_status,
        persisted_opening_id,
        attempt_finished,
        recovery_events,
    ): (String, String, Option<i64>, bool, i64) = db
        .conn()
        .query_row(
            "SELECT run.status, introduction.status,
                    introduction.opening_message_id,
                    attempt.finished_at IS NOT NULL,
                    (SELECT COUNT(*) FROM hive_controller_events event
                     WHERE event.run_id = run.id
                       AND event.event_type IN ('run_completed', 'recovery_required'))
             FROM hive_runs run
             JOIN hive_worker_introductions introduction
               ON introduction.run_id = run.id
             JOIN hive_run_attempts attempt ON attempt.run_id = run.id
             WHERE run.id = 'introduction-run'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    assert_eq!(run_status, "running");
    assert_eq!(introduction_status, "running");
    assert_eq!(persisted_opening_id, None);
    assert!(!attempt_finished);
    assert_eq!(recovery_events, 0);
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT id FROM messages WHERE id = ?1",
                [opening_message_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        opening_message_id
    );
    drop(db);

    runtime.shutdown().await;
    assert!(
        HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .release("hive-scheduler", "foreign-daemon", foreign.fencing_token,)
            .unwrap()
    );
}

#[tokio::test]
async fn generic_wake_is_ambiguous_but_user_response_resumes_only_its_exact_run() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let foreign =
        match HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .acquire(
                "hive-scheduler",
                "foreign-daemon",
                chrono::Utc::now(),
                Duration::from_secs(30),
            )
            .unwrap()
        {
            DaemonLeaseAcquire::Acquired(lease) => lease,
            other => panic!("expected foreign lease, got {other:?}"),
        };
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-b",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "two-waiting-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    let db = Database::new(&runtime_config.database_path).unwrap();
    let (controller_id, run_a): (String, String) = db
        .conn()
        .query_row(
            "SELECT controller_id, id FROM hive_runs WHERE session_id = ?1",
            [&session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let now = canonical_timestamp(chrono::Utc::now());
    db.conn()
        .execute(
            "UPDATE hive_runs SET status = 'awaiting_input', updated_at = ?2 WHERE id = ?1",
            rusqlite::params![run_a, now],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                id, controller_id, session_id, kind, objective, config_json,
                status, available_at, max_attempts, created_at, updated_at
             ) VALUES ('run-b', ?1, ?2, 'dispatch', 'second waiting run',
                       '{\"model\":\"test:model\",\"permission_mode\":\"autonomous\"}',
                       'awaiting_input', ?3, 5, ?3, ?3)",
            rusqlite::params![controller_id, session_id, now],
        )
        .unwrap();
    let run_b = "run-b".to_string();
    for (run_id, tool_call_id) in [(&run_a, "question-a"), (&run_b, "question-b")] {
        let sequence = db
            .conn()
            .query_row(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM hive_controller_events
                 WHERE controller_id = ?1",
                [&controller_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO hive_controller_events (
                    controller_id, sequence, event_type, run_id, payload_json, created_at
                 ) VALUES (?1, ?2, 'agentic_event', ?3, ?4, ?5)",
                rusqlite::params![
                    controller_id,
                    sequence,
                    run_id,
                    serde_json::json!({
                        "type": "awaiting_input",
                        "tool_call_id": tool_call_id,
                    })
                    .to_string(),
                    now,
                ],
            )
            .unwrap();
    }
    drop(db);

    let ambiguous = handler
        .handle(
            context(Actor::local("test"), "ambiguous-message"),
            Command::SendMessage(MessageCommand {
                session_id: session_id.clone(),
                message: "MESSAGE_PRIVATE_SENTINEL".into(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(ambiguous.code, "state_conflict");

    response(
        handler.as_ref(),
        context(Actor::local("test"), "exact-response"),
        Command::UserResponse(UserResponseCommand {
            session_id: session_id.clone(),
            run_id: run_a.clone(),
            tool_call_id: "question-a".into(),
            response: "RESPONSE_PRIVATE_SENTINEL".into(),
        }),
    )
    .await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "private-extension"),
        Command::Extension(ExtensionCommand {
            name: "privacy-test".into(),
            payload: serde_json::json!({
                "session_id": session_id,
                "secret": "EXTENSION_PRIVATE_SENTINEL",
            }),
        }),
    )
    .await;

    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE id = ?1",
                [&run_a],
                |row| { row.get::<_, String>(0) }
            )
            .unwrap(),
        "queued"
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE id = 'run-b'",
                [],
                |row| { row.get::<_, String>(0) }
            )
            .unwrap(),
        "awaiting_input"
    );
    let event_leaks: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_controller_events
             WHERE payload_json LIKE '%MESSAGE_PRIVATE_SENTINEL%'
                OR payload_json LIKE '%RESPONSE_PRIVATE_SENTINEL%'
                OR payload_json LIKE '%EXTENSION_PRIVATE_SENTINEL%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_leaks, 0);
    drop(db);

    HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
        .release("hive-scheduler", "foreign-daemon", foreign.fencing_token)
        .unwrap();
    runtime.shutdown().await;
}

#[tokio::test]
async fn graceful_runtime_shutdown_releases_lease_without_supervision_failure() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    let owner = handler.shared.instance_id.clone();

    runtime.shutdown().await;

    let lease = HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
        .get("hive-scheduler")
        .unwrap()
        .expect("scheduler lease row should remain as a fencing record");
    assert_eq!(lease.owner_id, owner);
    assert!(lease.expires_at <= canonical_timestamp(chrono::Utc::now()));
}

#[tokio::test]
async fn duplicate_dispatch_replays_without_creating_a_second_run() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let backend = Arc::new(FakeBackend::default());
    let runtime = start_runtime(config(&temp), "daemon-a", backend)
        .await
        .unwrap();
    let handler = runtime.handler();
    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "same-key"),
        dispatch_command(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "same-key"),
        dispatch_command(),
    )
    .await;
    assert_eq!(first, replay);

    let db = Database::new(&config(&temp).database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row("SELECT COUNT(*) FROM hive_runs", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn recurring_worker_schedule_defers_model_identity_and_rejects_stale_revision() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "schedule-parent"),
            dispatch_command(),
        )
        .await,
    );
    Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_runtime_state SET crew_slug = 'ops' WHERE session_id = ?1",
            [&session_id],
        )
        .unwrap();
    let ops_worker = mitsuro_core::storage::HiveWorkerStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    )
    .create(&mitsuro_core::storage::NewHiveWorker::new("ops"))
    .unwrap();

    let definition = ScheduleDefinition {
        title: "Daily repository audit".into(),
        summary: "Verify the backend remains healthy".into(),
        objective: "Audit the repository and report durable findings".into(),
        recurrence: serde_json::to_value(RecurrenceV1::Once {
            at: chrono::Utc::now() + chrono::Duration::days(1),
        })
        .unwrap(),
        timezone: "UTC".into(),
        dst_policy: serde_json::to_value(DstPolicy::default()).unwrap(),
        priority: 0,
        project_dir: None,
        model: None,
        model_key: None,
        model_catalog_revision: None,
        crew_slug: None,
        worker_id: None,
        group_id: None,
        misfire: serde_json::to_value(MisfireConfig::default()).unwrap(),
        overlap_policy: "queue_one".into(),
        retry: serde_json::to_value(RetryPolicy::default()).unwrap(),
    };
    let mut relative_definition = definition.clone();
    relative_definition.project_dir = Some("relative/workspace".into());
    let relative_error = handler
        .handle(
            context(Actor::local("test"), "relative-recurring-schedule"),
            Command::CreateSchedule(CreateScheduleCommand {
                session_id: session_id.clone(),
                definition: relative_definition,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(relative_error.code, "invalid_command");
    let mut unsafe_retry_definition = definition.clone();
    unsafe_retry_definition.retry = serde_json::json!({
        "max_attempts": 101,
        "base_delay_secs": 0,
        "max_delay_secs": 604_801,
        "jitter": "full",
    });
    let unsafe_retry_error = handler
        .handle(
            context(Actor::local("test"), "unsafe-retry-schedule"),
            Command::CreateSchedule(CreateScheduleCommand {
                session_id: session_id.clone(),
                definition: unsafe_retry_definition,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(unsafe_retry_error.code, "invalid_command");
    let created = response(
        handler.as_ref(),
        context(Actor::local("test"), "create-recurring-schedule"),
        Command::CreateSchedule(CreateScheduleCommand {
            session_id: session_id.clone(),
            definition: definition.clone(),
        }),
    )
    .await;
    let (schedule_id, revision) = match created {
        ResponsePayload::Schedule(response) => (response.schedule_id, response.revision),
        other => panic!("expected schedule response, got {other:?}"),
    };
    assert_eq!(revision, 0);

    let schedule = HiveScheduleStore::new(Database::new(&runtime_config.database_path).unwrap())
        .get_schedule(&schedule_id)
        .unwrap()
        .unwrap();
    assert!(schedule.project_dir.is_none());
    assert!(schedule.model.is_none());
    assert!(schedule.model_key.is_none());
    assert!(schedule.model_catalog_revision.is_none());
    assert_eq!(schedule.crew_slug.as_deref(), Some("ops"));
    assert_eq!(schedule.worker_id.as_deref(), Some(ops_worker.id.as_str()));

    let error = handler
        .handle(
            context(Actor::local("test"), "replace-stale-schedule"),
            Command::ReplaceSchedule(ReplaceScheduleCommand {
                session_id,
                schedule_id,
                expected_revision: revision + 1,
                definition,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "revision_conflict");
    runtime.shutdown().await;
}

#[tokio::test]
async fn daemon_authority_rejects_oversized_or_ambiguous_inputs() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime = start_runtime(config(&temp), "daemon-a", Arc::new(FakeBackend::default()))
        .await
        .unwrap();
    let handler = runtime.handler();
    let mut command = match dispatch_command() {
        Command::Dispatch(command) => command,
        _ => unreachable!(),
    };
    command.task = "x".repeat(64 * 1024 + 1);
    let error = handler
        .handle(
            context(Actor::local("test"), "oversized-dispatch"),
            Command::Dispatch(command),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "invalid_command");

    let mut relative_working_dir = match dispatch_command() {
        Command::Dispatch(command) => command,
        _ => unreachable!(),
    };
    relative_working_dir.working_dir = "relative/workspace".into();
    relative_working_dir.project_dir = None;
    let error = handler
        .handle(
            context(Actor::local("test"), "relative-working-directory"),
            Command::Dispatch(relative_working_dir),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "invalid_command");

    let mut relative_project_dir = match dispatch_command() {
        Command::Dispatch(command) => command,
        _ => unreachable!(),
    };
    relative_project_dir.project_dir = Some("relative/project".into());
    let error = handler
        .handle(
            context(Actor::local("test"), "relative-project-directory"),
            Command::Dispatch(relative_project_dir),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "invalid_command");

    // Legacy session commands resolve an exact owned surface before exposing
    // deeper command semantics. A missing session therefore remains an
    // ownership denial even when another field is oversized.
    for (key, command, expected_code) in [
        (
            "oversized-user-response",
            Command::UserResponse(UserResponseCommand {
                session_id: "missing-session".into(),
                run_id: "run-1".into(),
                tool_call_id: "question-1".into(),
                response: "x".repeat(64 * 1024 + 1),
            }),
            "ownership_denied",
        ),
        (
            "oversized-pending-id",
            Command::Steer(SteerCommand {
                session_id: "missing-session".into(),
                pending_id: Some("x".repeat(257)),
                content: serde_json::json!([{"type": "text", "text": "continue"}]),
            }),
            "ownership_denied",
        ),
        (
            "oversized-extension-payload",
            Command::Extension(ExtensionCommand {
                name: "test.extension".into(),
                payload: serde_json::json!({
                    "session_id": "missing-session",
                    "content": "x".repeat(256 * 1024 + 1),
                }),
            }),
            "invalid_command",
        ),
    ] {
        let error = handler
            .handle(context(Actor::local("test"), key), command)
            .await
            .unwrap_err();
        assert_eq!(error.code, expected_code);
    }
    runtime.shutdown().await;
}

#[tokio::test]
async fn message_after_completion_queues_exactly_one_idempotent_followup_run() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let backend = Arc::new(FakeBackend::default());
    let runtime = start_runtime(config(&temp), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "first-turn"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.execution_count() == 1).await;
    wait_for(|| {
        let db = Database::new(&config(&temp).database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE status = 'succeeded'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    let followup = Command::SendMessage(MessageCommand {
        session_id: session_id.clone(),
        message: "Now do the second turn".into(),
    });
    response(
        handler.as_ref(),
        context(Actor::local("test"), "second-turn"),
        followup.clone(),
    )
    .await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "second-turn"),
        followup,
    )
    .await;
    wait_for(|| backend.execution_count() == 2).await;
    let db = Database::new(&config(&temp).database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM sessions
                 WHERE id = ?1
                   AND json_extract(model_key_json, '$.provider') = 'grok'
                   AND model_catalog_revision = 'catalog-42'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1,
        "dispatch must freeze exact identity on the session",
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE session_id = ?1",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs
                 WHERE json_extract(config_json, '$.model') = 'test:model'
                   AND json_extract(config_json, '$.model_key.provider') = 'grok'
                   AND json_extract(config_json, '$.model_catalog_revision') = 'catalog-42'
                   AND json_extract(config_json, '$.permission_mode') = 'autonomous'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2,
        "dispatch and message follow-up must freeze model and permission mode",
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'user'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn every_session_command_reloads_and_enforces_exact_actor_ownership() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime = start_runtime(config(&temp), "daemon-a", Arc::new(FakeBackend::default()))
        .await
        .unwrap();
    let db = Database::new(&config(&temp).database_path).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO users (id, email) VALUES ('alice', 'alice@example.com');
             INSERT INTO users (id, email) VALUES ('bob', 'bob@example.com');",
        )
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(
                Actor {
                    user_id: Some("alice".into()),
                    client_kind: "test".into(),
                },
                "alice-dispatch",
            ),
            dispatch_command(),
        )
        .await,
    );
    let error = handler
        .handle(
            context(
                Actor {
                    user_id: Some("bob".into()),
                    client_kind: "test".into(),
                },
                "bob-pause",
            ),
            Command::PauseSession(SessionCommand { session_id }),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "ownership_denied");

    let sessions = SessionManager::new(Database::new(&config(&temp).database_path).unwrap());
    let code_session_id = sessions
        .create_session_for_user_with_config(
            "Code session",
            Some("test:model"),
            Some("/work/repo"),
            Some("/work/repo"),
            WorkspaceMode::Selected,
            None,
            None,
            SessionType::Code,
        )
        .unwrap();
    for (key, command) in [
        (
            "reject-code-start",
            Command::StartSession(SessionCommand {
                session_id: code_session_id.clone(),
            }),
        ),
        (
            "reject-code-delete",
            Command::DeleteSession(SessionCommand {
                session_id: code_session_id.clone(),
            }),
        ),
    ] {
        let error = handler
            .handle(context(Actor::local("test"), key), command)
            .await
            .unwrap_err();
        assert_eq!(error.code, "ownership_denied");
    }
    assert!(sessions.get_session(&code_session_id).unwrap().is_some());
    assert_eq!(
        Database::new(&config(&temp).database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controllers WHERE session_id = ?1",
                [&code_session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn dropped_producer_after_tool_event_is_recovery_required_and_unclaimable() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(DroppedProducerBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "uncertain-tool-dispatch"),
            dispatch_command(),
        )
        .await,
    );

    wait_for(|| {
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE session_id = ?1",
                [&session_id],
                |row| row.get::<_, String>(0),
            )
            .is_ok_and(|status| status == "recovery_required")
    })
    .await;
    assert_eq!(backend.executions.load(Ordering::SeqCst), 1);

    let claim = HiveRunStore::new(Database::new(&runtime_config.database_path).unwrap())
        .claim_next(&ClaimRunRequest {
            executor_id: "replacement-executor".into(),
            lease_epoch: 999,
            now: chrono::Utc::now(),
            lease_duration: Duration::from_secs(10),
            global_concurrency_limit: 8,
        })
        .unwrap();
    assert!(
        claim.is_none(),
        "recovery-required work must not auto-replay"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn queue_claim_execution_and_once_schedule_survive_the_process_boundary() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let backend = Arc::new(FakeBackend::with_outcomes([
        ExecutionOutcome::Succeeded {
            output: serde_json::json!({"dispatch": true}),
        },
        ExecutionOutcome::Succeeded {
            output: serde_json::json!({"scheduled": true}),
        },
    ]));
    let runtime = start_runtime(config(&temp), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.execution_count() >= 1).await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "schedule"),
        Command::ScheduleSession(ScheduleCommand {
            session_id: session_id.clone(),
            wake_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            reason: "Follow-up health check".into(),
        }),
    )
    .await;
    wait_for(|| backend.execution_count() >= 2).await;
    wait_for(|| {
        let db = Database::new(&config(&temp).database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE status = 'succeeded'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 2
    })
    .await;

    let db = Database::new(&config(&temp).database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE status = 'succeeded'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
    assert_eq!(
        db.conn()
            .query_row("SELECT status FROM hive_schedules LIMIT 1", [], |row| {
                row.get::<_, String>(0)
            },)
            .unwrap(),
        "completed"
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs
                 WHERE json_extract(config_json, '$.model') = 'test:model'
                   AND json_extract(config_json, '$.model_key.provider') = 'grok'
                   AND json_extract(config_json, '$.model_catalog_revision') = 'catalog-42'
                   AND json_extract(config_json, '$.permission_mode') = 'autonomous'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2,
        "dispatch and one-shot materialization must freeze model and permission mode",
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn restart_marks_expired_running_work_recovery_required_without_replay() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let db = Database::new(&runtime_config.database_path).unwrap();
    let now = canonical_timestamp(chrono::Utc::now());
    let expired = canonical_timestamp(chrono::Utc::now() - chrono::Duration::seconds(1));
    db.conn()
        .execute_batch(&format!(
            "INSERT INTO sessions (id, title, created_at, updated_at, working_dir, project_dir, model, session_type)
             VALUES ('session-1', 'Hive', '{now}', '{now}', '/work', '/work', 'test:model', 'hive');
             INSERT INTO hive_controllers (
                 id, scope_key, session_id, status, timezone, max_concurrent_runs, created_at, updated_at
             ) VALUES ('controller-1', 'session:session-1', 'session-1', 'active', 'UTC', 1, '{now}', '{now}');
             INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json, status,
                 priority, available_at, attempt_count, max_attempts, lease_owner,
                 lease_token, lease_epoch, lease_expires_at, heartbeat_at, created_at, updated_at
             ) VALUES ('run-1', 'controller-1', 'session-1', 'dispatch', 'work', '{{}}',
                 'running', 0, '{now}', 1, 3, 'old-daemon', 'old-token', 1, '{expired}',
                 '{expired}', '{now}', '{now}');
             INSERT INTO hive_run_attempts (
                 id, run_id, attempt_no, executor_id, lease_token, lease_epoch, started_at, outcome
             ) VALUES ('attempt-1', 'run-1', 1, 'old-daemon', 'old-token', 1, '{now}', 'leased');"
        ))
        .unwrap();
    drop(db);

    let backend = Arc::new(FakeBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-new", backend.clone())
        .await
        .unwrap();
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE id = 'run-1'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            == "recovery_required"
    })
    .await;
    assert_eq!(backend.execution_count(), 0);

    let handler = runtime.handler();
    for (key, command) in [
        (
            "blocked-start",
            Command::StartSession(SessionCommand {
                session_id: "session-1".into(),
            }),
        ),
        (
            "blocked-resume",
            Command::ResumeSession(SessionCommand {
                session_id: "session-1".into(),
            }),
        ),
    ] {
        let blocked = response(
            handler.as_ref(),
            context(Actor::local("test"), key),
            command,
        )
        .await;
        assert!(matches!(
            blocked,
            ResponsePayload::Session(ref response)
                if response.state["status"] == "recovery_required"
        ));
    }
    let db = Database::new(&runtime_config.database_path).unwrap();
    let (runs, run_status, runtime_status, controller_status): (i64, String, String, String) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM hive_runs WHERE session_id = 'session-1'),
                 (SELECT status FROM hive_runs WHERE id = 'run-1'),
                 (SELECT status FROM hive_runtime_state WHERE session_id = 'session-1'),
                 (SELECT status FROM hive_controllers WHERE id = 'controller-1')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(runs, 1);
    assert_eq!(run_status, "recovery_required");
    assert_eq!(runtime_status, "error");
    assert_eq!(controller_status, "paused");
    drop(db);

    // Cancellation is the explicit abandon decision. Only after that durable
    // resolution may StartSession create fresh work for the session.
    response(
        handler.as_ref(),
        context(Actor::local("test"), "abandon-uncertain-run"),
        Command::CancelSession(SessionCommand {
            session_id: "session-1".into(),
        }),
    )
    .await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "start-after-abandon"),
        Command::StartSession(SessionCommand {
            session_id: "session-1".into(),
        }),
    )
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE session_id = 'session-1'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE id = 'run-1'",
                [],
                |row| { row.get::<_, String>(0) }
            )
            .unwrap(),
        "cancelled"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn takeover_reconciles_a_worker_lease_that_expires_after_acquisition() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let mut runtime_config = config(&temp);
    // Shutdown releases the daemon lease, so the replacement still acquires
    // immediately. Keep the worker lease longer than ordinary CI scheduling
    // jitter while leaving ample room for it to expire inside `wait_for`.
    runtime_config.daemon_lease_duration = Duration::from_secs(2);
    runtime_config.worker_lease_duration = Duration::from_secs(1);
    runtime_config.worker_heartbeat_interval = Duration::from_millis(100);

    let first_backend = Arc::new(BlockingBackend::default());
    let first = start_runtime(runtime_config.clone(), "daemon-a", first_backend.clone())
        .await
        .unwrap();
    let handler = first.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "late-expiry-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| first_backend.executions.load(Ordering::SeqCst) == 1).await;
    first.shutdown().await;

    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE session_id = ?1",
                [&session_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "running"
    );
    drop(db);

    let second = start_runtime(
        runtime_config.clone(),
        "daemon-b",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    // Acquisition happens immediately, before the 500ms worker lease has
    // expired. Periodic reconciliation must catch it on a later tick.
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE session_id = ?1",
                [&session_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            == "recovery_required"
    })
    .await;
    second.shutdown().await;
}

#[tokio::test]
async fn replay_and_live_events_remain_monotonic_and_report_a_retention_gap() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime = start_runtime(config(&temp), "daemon-a", Arc::new(FakeBackend::default()))
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "dispatch"),
            dispatch_command(),
        )
        .await,
    );
    response(
        handler.as_ref(),
        context(Actor::local("test"), "pause"),
        Command::PauseSession(SessionCommand {
            session_id: session_id.clone(),
        }),
    )
    .await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "priority"),
        Command::SetPriority(SetPriorityCommand {
            session_id: session_id.clone(),
            priority: "high".into(),
        }),
    )
    .await;
    let db = Database::new(&config(&temp).database_path).unwrap();
    db.conn()
        .execute(
            "DELETE FROM hive_controller_events
             WHERE sequence = (SELECT MIN(sequence) FROM hive_controller_events)",
            [],
        )
        .unwrap();
    drop(db);

    let reply = handler
        .handle(
            context(Actor::local("test"), "subscribe"),
            Command::Subscribe(SubscribeCommand {
                session_id,
                after_sequence: Some(0),
                replay_limit: Some(100),
            }),
        )
        .await
        .unwrap();
    let HandlerReply::Subscription {
        accepted,
        mut events,
    } = reply
    else {
        panic!("expected subscription");
    };
    assert!(accepted.high_water_sequence.is_some());
    assert!(matches!(
        events.recv().await.unwrap().event,
        HiveEvent::ReplayGap(_)
    ));
    let mut previous = 0;
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(100), events.recv()).await
    {
        if let Some(sequence) = event.sequence {
            assert!(sequence > previous);
            previous = sequence;
        }
        if previous == accepted.high_water_sequence.unwrap_or_default() {
            break;
        }
    }
    runtime.shutdown().await;
}

#[tokio::test]
async fn active_message_is_canonical_exactly_once_and_pause_fences_execution() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let backend = Arc::new(BlockingBackend::default());
    let runtime = start_runtime(config(&temp), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "blocking-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    let message_command = Command::SendMessage(MessageCommand {
        session_id: session_id.clone(),
        message: "Remember this exact message".into(),
    });
    response(
        handler.as_ref(),
        context(Actor::local("test"), "one-message"),
        message_command.clone(),
    )
    .await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "one-message"),
        message_command,
    )
    .await;

    let db = Database::new(&config(&temp).database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role = 'user'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1 AND role LIKE 'pending_user:%'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM conversation_episodes WHERE session_id = ?1",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let latest_content = db
        .conn()
        .query_row(
            "SELECT content FROM messages WHERE session_id = ?1 ORDER BY id DESC LIMIT 1",
            [&session_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Vec<mitsuro_core::Content>>(&latest_content)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE session_id = ?1",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    drop(db);
    let message_controls = backend
        .controls
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, control)| {
            matches!(
                control,
                ExecutionControl::Steer {
                    pending_id: Some(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(message_controls, 1);

    response(
        handler.as_ref(),
        context(Actor::local("test"), "pause-active"),
        Command::PauseSession(SessionCommand {
            session_id: session_id.clone(),
        }),
    )
    .await;
    let db = Database::new(&config(&temp).database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE session_id = ?1",
                [&session_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "recovery_required"
    );
    assert!(backend.controls.lock().unwrap().iter().any(|(_, control)| {
        matches!(control, ExecutionControl::Cancel { reason } if reason == "paused by user")
    }));
    runtime.shutdown().await;
}

#[tokio::test]
async fn steering_missed_at_terminal_boundary_is_resumed_and_promoted_exactly_once() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(LateSteerBackend::new(runtime_config.database_path.clone()));
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "late-steer-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "late-steer-message"),
        Command::SendMessage(MessageCommand {
            session_id: session_id.clone(),
            message: "Do not lose this boundary message".into(),
        }),
    )
    .await;
    backend.release_first.notify_one();
    wait_for(|| backend.executions.load(Ordering::SeqCst) >= 2).await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE status = 'succeeded'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (canonical, pending): (i64, i64) = db
        .conn()
        .query_row(
            "SELECT
                 SUM(CASE WHEN role = 'user' THEN 1 ELSE 0 END),
                 SUM(CASE WHEN role LIKE 'pending_user:%' THEN 1 ELSE 0 END)
             FROM messages WHERE session_id = ?1",
            [&session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(canonical, 2);
    assert_eq!(pending, 0);
    runtime.shutdown().await;
}

#[tokio::test]
async fn generic_steer_without_a_client_pending_id_is_durable() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(LateSteerBackend::new(runtime_config.database_path.clone()));
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "generic-steer-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "generic-steer"),
        Command::Steer(SteerCommand {
            session_id: session_id.clone(),
            pending_id: None,
            content: serde_json::json!([{
                "type": "text",
                "text": "Preserve this generic steer",
            }]),
        }),
    )
    .await;
    backend.release_first.notify_one();
    wait_for(|| backend.executions.load(Ordering::SeqCst) >= 2).await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE session_id = ?1 AND role = 'user' AND content LIKE '%generic steer%'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE session_id = ?1 AND role LIKE 'pending_user:%'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn user_response_at_awaiting_boundary_is_not_stranded() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(AwaitingResponseBoundaryBackend::new(
        runtime_config.database_path.clone(),
    ));
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "response-boundary-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controller_events e
                 JOIN hive_controllers c ON c.id = e.controller_id
                 WHERE c.session_id = ?1 AND e.event_type = 'agentic_event'
                   AND json_extract(e.payload_json, '$.type') = 'awaiting_input'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    let run_id = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT id FROM hive_runs WHERE session_id = ?1 ORDER BY created_at LIMIT 1",
            [&session_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap();

    response(
        handler.as_ref(),
        context(Actor::local("test"), "response-boundary-answer"),
        Command::UserResponse(UserResponseCommand {
            session_id: session_id.clone(),
            run_id,
            tool_call_id: "question-1".into(),
            response: "continue".into(),
        }),
    )
    .await;

    wait_for(|| backend.executions.load(Ordering::SeqCst) >= 2).await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE session_id = ?1 AND status = 'succeeded'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (canonical_responses, pending_responses, indexed_responses): (i64, i64, i64) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM messages
                  WHERE session_id = ?1 AND role = 'user' AND content LIKE '%continue%'),
                 (SELECT COUNT(*) FROM messages
                  WHERE session_id = ?1 AND role LIKE 'pending_user:%'),
                 (SELECT COUNT(*) FROM conversation_episodes
                  WHERE session_id = ?1 AND role = 'user' AND body LIKE '%continue%')",
            [&session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(canonical_responses, 1);
    assert_eq!(pending_responses, 0);
    assert_eq!(indexed_responses, 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn tool_approval_outbox_retries_until_the_active_host_accepts_it() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(FlakyApprovalBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "approval-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controller_events e
                 JOIN hive_controllers c ON c.id = e.controller_id
                 WHERE c.session_id = ?1 AND e.event_type = 'agentic_event'
                   AND json_extract(e.payload_json, '$.type') = 'tool_approval_required'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    let run_id = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT id FROM hive_runs WHERE session_id = ?1 ORDER BY created_at LIMIT 1",
            [&session_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap();

    let approval = Command::ToolApproval(ToolApprovalCommand {
        session_id: session_id.clone(),
        run_id: run_id.clone(),
        tool_call_id: "tool-1".into(),
        approved: true,
    });
    response(
        handler.as_ref(),
        context(Actor::local("test"), "approval-tool-1"),
        approval.clone(),
    )
    .await;
    // Idempotent transport replay must neither create another outbox row nor
    // bypass the scheduler-owned delivery contract.
    response(
        handler.as_ref(),
        context(Actor::local("test"), "approval-tool-1"),
        approval,
    )
    .await;

    wait_for(|| backend.delivered.load(Ordering::SeqCst)).await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_control_outbox
                 WHERE session_id = ?1 AND dedupe_key = ?2 AND status = 'delivered'",
                rusqlite::params![session_id, format!("{run_id}:tool-1")],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    let (rows, status, attempts): (i64, String, i64) = db
        .conn()
        .query_row(
            "SELECT COUNT(*), MAX(status), MAX(attempt_count)
             FROM hive_control_outbox WHERE session_id = ?1 AND dedupe_key = ?2",
            rusqlite::params![session_id, format!("{run_id}:tool-1")],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(status, "delivered");
    assert!(attempts >= 2);
    drop(db);
    runtime.shutdown().await;
}

#[tokio::test]
async fn durable_event_exhaustion_cancels_exact_run_and_requires_recovery_immediately() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let mut runtime_config = config(&temp);
    runtime_config.max_execution_event_bytes = 32 * 1024;
    let backend = Arc::new(JournalExhaustionBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "journal-exhaustion-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs
                 WHERE session_id = ?1 AND status = 'recovery_required'",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (run_id, lease_token, attempt_outcome, finished_at): (
        String,
        Option<String>,
        String,
        Option<String>,
    ) = db
        .conn()
        .query_row(
            "SELECT r.id, r.lease_token, a.outcome, a.finished_at
             FROM hive_runs r JOIN hive_run_attempts a ON a.run_id = r.id
             WHERE r.session_id = ?1",
            [&session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert!(lease_token.is_none());
    assert_eq!(attempt_outcome, "recovery_required");
    assert!(finished_at.is_some());
    let oversized_finish_events = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_controller_events e
             JOIN hive_controllers c ON c.id = e.controller_id
             WHERE c.session_id = ?1 AND e.event_type = 'agentic_event'
               AND json_extract(e.payload_json, '$.type') = 'finish'",
            [&session_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(oversized_finish_events, 0);
    drop(db);

    assert!(backend.controls.lock().unwrap().iter().any(
        |(controlled_session, control)| matches!(
            control,
            ExecutionControl::CancelRun { run_id: controlled_run, reason }
                if controlled_session == &session_id
                    && controlled_run == &run_id
                    && reason == "durable event journal exhausted; execution side effects may be uncertain"
        )
    ));
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_cancel_persists_terminal_event_then_quiesces_for_delete() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let mut runtime_config = config(&temp);
    // This case proves the cooperative path, including draining an accepted
    // terminal event before cancellation closes the run. The shared 200 ms
    // test grace deliberately makes forced-cancellation cases fast. Give this
    // cooperative canary suite-scale lease headroom so a saturated 132-test
    // Hive process cannot impersonate a lost scheduler/worker fence before
    // the backend starts; lease-expiry behavior is covered separately.
    runtime_config.daemon_lease_duration = Duration::from_secs(30);
    runtime_config.worker_lease_duration = Duration::from_secs(30);
    runtime_config.cancellation_grace_period = Duration::from_secs(2);
    let backend = Arc::new(CancellableEventBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "cancel-terminal-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    tokio::time::timeout(Duration::from_secs(30), backend.started.notified())
        .await
        .expect(
            "active-cancel backend did not enter execute within the suite startup budget after scheduler readiness",
        );
    assert_eq!(backend.executions.load(Ordering::SeqCst), 1);

    response(
        handler.as_ref(),
        context(Actor::local("test"), "cancel-terminal-request"),
        Command::CancelSession(SessionCommand {
            session_id: session_id.clone(),
        }),
    )
    .await;
    tokio::time::timeout(
        runtime_config.cancellation_grace_period,
        backend.cancellation_observed.notified(),
    )
    .await
    .expect(
        "active-cancel backend did not observe the committed generic Cancel within its grace period",
    );
    tokio::time::timeout(
        runtime_config.cancellation_grace_period,
        backend.terminal_event_accepted.notified(),
    )
    .await
    .expect("active-cancel backend did not submit its finish event within its grace period");

    let quiescence_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (run_status, open_attempts, cancelled_attempts, finish_events, outcome_kind) = loop {
        let db = Database::new(&runtime_config.database_path).unwrap();
        let state = db
            .conn()
            .query_row(
                "SELECT
                     (SELECT status FROM hive_runs
                      WHERE session_id = ?1 LIMIT 1),
                     (SELECT COUNT(*) FROM hive_run_attempts a
                      JOIN hive_runs r ON r.id = a.run_id
                      WHERE r.session_id = ?1 AND a.finished_at IS NULL),
                     (SELECT COUNT(*) FROM hive_run_attempts a
                      JOIN hive_runs r ON r.id = a.run_id
                      WHERE r.session_id = ?1 AND a.outcome = 'cancelled'),
                     (SELECT COUNT(*) FROM hive_controller_events e
                      JOIN hive_controllers c ON c.id = e.controller_id
                      WHERE c.session_id = ?1 AND e.event_type = 'agentic_event'
                        AND json_extract(e.payload_json, '$.type') = 'finish'),
                     (SELECT json_extract(outcome_json, '$.kind')
                      FROM hive_runs WHERE session_id = ?1 LIMIT 1)",
                [&session_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .unwrap();
        if state.0.as_deref() == Some("cancelled")
            && state.1 == 0
            && state.2 == 1
            && state.3 == 1
            && state.4.as_deref() == Some("cancelled")
        {
            break state;
        }
        assert!(
            tokio::time::Instant::now() < quiescence_deadline,
            "active cancellation did not quiesce after finish acceptance: \
             run_status={:?}, open_attempts={}, cancelled_attempts={}, \
             finish_events={}, outcome_kind={:?}",
            state.0,
            state.1,
            state.2,
            state.3,
            state.4,
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(run_status.as_deref(), Some("cancelled"));
    assert_eq!(open_attempts, 0);
    assert_eq!(cancelled_attempts, 1);
    assert_eq!(finish_events, 1);
    assert_eq!(outcome_kind.as_deref(), Some("cancelled"));

    response(
        handler.as_ref(),
        context(Actor::local("test"), "cancel-terminal-delete"),
        Command::DeleteSession(SessionCommand {
            session_id: session_id.clone(),
        }),
    )
    .await;
    assert_eq!(
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn leased_cancel_closes_unstarted_attempt_immediately() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let foreign =
        match HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .acquire(
                "hive-scheduler",
                "foreign-daemon",
                chrono::Utc::now(),
                Duration::from_secs(30),
            )
            .unwrap()
        {
            DaemonLeaseAcquire::Acquired(lease) => lease,
            other => panic!("expected foreign scheduler lease, got {other:?}"),
        };
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-b",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "leased-cancel-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    let claimed = HiveRunStore::new(Database::new(&runtime_config.database_path).unwrap())
        .claim_next(&ClaimRunRequest {
            executor_id: "test-executor".into(),
            lease_epoch: 9,
            now: chrono::Utc::now(),
            lease_duration: Duration::from_secs(30),
            global_concurrency_limit: 1,
        })
        .unwrap()
        .expect("queued run should enter the leased pre-execution state");

    response(
        handler.as_ref(),
        context(Actor::local("test"), "leased-cancel-request"),
        Command::CancelSession(SessionCommand {
            session_id: session_id.clone(),
        }),
    )
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    let (status, lease_token, attempt_outcome, finished_at): (
        String,
        Option<String>,
        String,
        Option<String>,
    ) = db
        .conn()
        .query_row(
            "SELECT r.status, r.lease_token, a.outcome, a.finished_at
             FROM hive_runs r JOIN hive_run_attempts a ON a.run_id = r.id
             WHERE r.id = ?1 AND a.attempt_no = ?2",
            rusqlite::params![claimed.run.id, claimed.attempt_no],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(status, "cancelled");
    assert!(lease_token.is_none());
    assert_eq!(attempt_outcome, "cancelled");
    assert!(finished_at.is_some());
    drop(db);

    response(
        handler.as_ref(),
        context(Actor::local("test"), "leased-cancel-delete"),
        Command::DeleteSession(SessionCommand { session_id }),
    )
    .await;
    HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
        .release("hive-scheduler", "foreign-daemon", foreign.fencing_token)
        .unwrap();
    runtime.shutdown().await;
}

#[tokio::test]
async fn noncooperative_cancel_terminalizes_after_grace_and_rejects_late_worker_writes() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let backend = Arc::new(BlockingBackend::default());
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "cancel-race-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;

    let (run_id, lease_token, lease_epoch): (String, String, u64) = {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT id, lease_token, lease_epoch FROM hive_runs
                 WHERE session_id = ?1 AND status = 'running'",
                [&session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, i64>(2)? as u64)),
            )
            .unwrap()
    };
    let cancellation_started = tokio::time::Instant::now();

    response(
        handler.as_ref(),
        context(Actor::local("test"), "cancel-race-first"),
        Command::CancelSession(SessionCommand {
            session_id: session_id.clone(),
        }),
    )
    .await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT
                     EXISTS(SELECT 1 FROM hive_runs
                            WHERE id = ?1 AND status = 'cancelled'),
                     EXISTS(SELECT 1 FROM hive_controller_events
                            WHERE run_id = ?1 AND event_type = 'run_cancelled')",
                [&run_id],
                |row| Ok(row.get::<_, bool>(0)? && row.get::<_, bool>(1)?),
            )
            .unwrap()
    })
    .await;
    assert!(
        cancellation_started.elapsed() >= runtime_config.cancellation_grace_period,
        "noncooperative execution was terminalized before its cooperative grace elapsed"
    );

    let db = Database::new(&runtime_config.database_path).unwrap();
    let terminal: (
        String,
        Option<String>,
        Option<i64>,
        Option<String>,
        String,
        String,
        i64,
        i64,
        i64,
    ) = db
        .conn()
        .query_row(
            "SELECT status, lease_token, lease_epoch, finished_at,
                    last_stop_reason, last_error,
                    json_extract(outcome_json, '$.forced'),
                    json_extract(outcome_json, '$.side_effects_may_be_uncertain'),
                    json_extract(outcome_json, '$.abort_delivery_confirmed')
             FROM hive_runs WHERE id = ?1",
            [&run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(terminal.0, "cancelled");
    assert!(terminal.1.is_none());
    assert!(terminal.2.is_none());
    assert!(terminal.3.is_some());
    assert_eq!(terminal.4, "cancellation grace elapsed");
    assert!(terminal.5.contains("side effects may be uncertain"));
    assert_eq!(terminal.6, 1);
    assert_eq!(terminal.7, 1);
    assert_eq!(terminal.8, 1);

    let attempt: (String, Option<String>, String, String) = db
        .conn()
        .query_row(
            "SELECT outcome, finished_at, stop_reason, error
             FROM hive_run_attempts WHERE run_id = ?1",
            [&run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(attempt.0, "cancelled");
    assert!(attempt.1.is_some());
    assert_eq!(attempt.2, "cancellation grace elapsed");
    assert!(attempt.3.contains("side effects may be uncertain"));

    let runtime_projection: (String, String) = db
        .conn()
        .query_row(
            "SELECT status, current_run_id FROM hive_runtime_state WHERE session_id = ?1",
            [&session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(runtime_projection, ("cancelled".into(), run_id.clone()));
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controller_events
                 WHERE run_id = ?1 AND event_type = 'run_cancelled'
                   AND dedupe_key = ?2",
                rusqlite::params![run_id, format!("transition:{run_id}:1:cancelled")],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    drop(db);

    let store = HiveRunStore::new(Database::new(&runtime_config.database_path).unwrap());
    let late_success = RunCompletion {
        target_status: HiveRunStatus::Succeeded,
        now: chrono::Utc::now(),
        available_at: None,
        wake_at: None,
        stop_reason: Some("late success".into()),
        error: None,
        outcome: Some(serde_json::json!({"late": true})),
        trace_sequence_end: None,
    };
    assert_eq!(
        store
            .finish_claimed(&run_id, &lease_token, lease_epoch, &late_success)
            .unwrap(),
        None
    );
    assert!(!store
        .heartbeat(
            &run_id,
            &lease_token,
            lease_epoch,
            chrono::Utc::now(),
            runtime_config.worker_lease_duration,
        )
        .unwrap());
    assert_eq!(
        store.get_run(&run_id).unwrap().unwrap().status,
        HiveRunStatus::Cancelled
    );
    wait_for(|| backend.execution_dropped.load(Ordering::SeqCst)).await;

    {
        let controls = backend.controls.lock().unwrap();
        assert!(controls.iter().any(|(controlled_session, control)| {
            controlled_session == &session_id
                && matches!(control, ExecutionControl::Cancel { reason } if reason == "cancelled by user")
        }));
        assert!(controls.iter().any(|(controlled_session, control)| {
            controlled_session == &session_id
                && matches!(control, ExecutionControl::CancelRun { run_id: controlled_run, reason }
                    if controlled_run == &run_id && reason == "cancelled by user")
        }));
        assert!(controls.iter().any(|(controlled_session, control)| {
            controlled_session == &session_id
                && matches!(control, ExecutionControl::AbortRun { run_id: controlled_run, reason }
                    if controlled_run == &run_id && reason == "cancellation grace elapsed")
        }));
    }

    response(
        handler.as_ref(),
        context(Actor::local("test"), "delete-after-cancel-grace"),
        Command::DeleteSession(SessionCommand {
            session_id: session_id.clone(),
        }),
    )
    .await;
    assert_eq!(
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE id = ?1",
                [&session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn workspace_less_legacy_session_rejects_enqueue_instead_of_dooming_runs() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let now = canonical_timestamp(chrono::Utc::now());
    let db = Database::new(&runtime_config.database_path).unwrap();
    db.conn()
        .execute(
            "INSERT INTO sessions (
                id, title, created_at, updated_at, model, session_type
             ) VALUES ('workspace-less', 'Legacy', ?1, ?1, 'test:model', 'hive')",
            [&now],
        )
        .unwrap();
    drop(db);
    let handler = runtime.handler();

    // The execution host refuses claims without an explicit workspace, so the
    // enqueue surfaces must fail with an actionable error instead of
    // manufacturing runs that instantly fail with a redacted generic error.
    let start = handler
        .handle(
            context(Actor::local("test"), "workspace-less-start"),
            Command::StartSession(SessionCommand {
                session_id: "workspace-less".into(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(start.code, "state_conflict");
    assert!(
        start.message.contains("working or project directory"),
        "unexpected start message: {}",
        start.message
    );

    let message = handler
        .handle(
            context(Actor::local("test"), "workspace-less-message"),
            Command::SendMessage(MessageCommand {
                session_id: "workspace-less".into(),
                message: "please run".into(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(message.code, "state_conflict");
    assert!(
        message.message.contains("working or project directory"),
        "unexpected message-turn message: {}",
        message.message
    );

    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE session_id = 'workspace-less'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "no run may be enqueued for a workspace-less session"
    );
    drop(db);
    runtime.shutdown().await;
}

#[tokio::test]
async fn server_created_session_gets_controller_and_waiting_runs_resume_durably() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let now = canonical_timestamp(chrono::Utc::now());
    let initial_content = serde_json::to_string(&vec![mitsuro_core::Content::text(
        "legacy objective".into(),
    )])
    .unwrap();
    let db = Database::new(&runtime_config.database_path).unwrap();
    db.conn()
        .execute(
            "INSERT INTO sessions (
                id, title, created_at, updated_at, working_dir, project_dir, model, session_type
             ) VALUES ('legacy-session', 'Legacy', ?1, ?1, '/work', '/work', 'test:model', 'hive')",
            [&now],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES ('legacy-session', 'user', ?1, ?2)",
            rusqlite::params![initial_content, now],
        )
        .unwrap();
    drop(db);
    let handler = runtime.handler();
    response(
        handler.as_ref(),
        context(Actor::local("test"), "legacy-start"),
        Command::StartSession(SessionCommand {
            session_id: "legacy-session".into(),
        }),
    )
    .await;
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controllers WHERE session_id = 'legacy-session'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    response(
        handler.as_ref(),
        context(Actor::local("test"), "legacy-pause"),
        Command::PauseSession(SessionCommand {
            session_id: "legacy-session".into(),
        }),
    )
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs SET status = 'sleeping', wake_at = ?1,
                 finished_at = NULL, updated_at = ?2
             WHERE session_id = 'legacy-session'",
            rusqlite::params![
                canonical_timestamp(chrono::Utc::now() + chrono::Duration::hours(1)),
                canonical_timestamp(chrono::Utc::now())
            ],
        )
        .unwrap();
    drop(db);
    response(
        handler.as_ref(),
        context(Actor::local("test"), "wake-message"),
        Command::SendMessage(MessageCommand {
            session_id: "legacy-session".into(),
            message: "wake now".into(),
        }),
    )
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE session_id = 'legacy-session'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "queued"
    );
    db.conn()
        .execute(
            "UPDATE hive_runs SET status = 'awaiting_input' WHERE session_id = 'legacy-session'",
            [],
        )
        .unwrap();
    let (run_id, controller_id): (String, String) = db
        .conn()
        .query_row(
            "SELECT id, controller_id FROM hive_runs WHERE session_id = 'legacy-session'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let sequence = db
        .conn()
        .query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1
             FROM hive_controller_events WHERE controller_id = ?1",
            [&controller_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_controller_events (
                controller_id, sequence, event_type, run_id, payload_json, created_at
             ) VALUES (?1, ?2, 'agentic_event', ?3, ?4, ?5)",
            rusqlite::params![
                controller_id,
                sequence,
                run_id,
                serde_json::json!({
                    "type": "awaiting_input",
                    "tool_call_id": "question-1",
                })
                .to_string(),
                canonical_timestamp(chrono::Utc::now()),
            ],
        )
        .unwrap();
    drop(db);
    response(
        handler.as_ref(),
        context(Actor::local("test"), "wake-response"),
        Command::UserResponse(UserResponseCommand {
            session_id: "legacy-session".into(),
            run_id,
            tool_call_id: "question-1".into(),
            response: "continue".into(),
        }),
    )
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE session_id = 'legacy-session'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "queued"
    );
    let (canonical_response, indexed_response): (i64, i64) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM messages
                  WHERE session_id = 'legacy-session' AND role = 'user'
                    AND content LIKE '%continue%'),
                 (SELECT COUNT(*) FROM conversation_episodes
                  WHERE session_id = 'legacy-session' AND role = 'user'
                    AND body LIKE '%continue%')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(canonical_response, 1);
    assert_eq!(indexed_response, 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn replay_limit_zero_is_live_only_with_an_atomic_high_water() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime = start_runtime(config(&temp), "daemon-a", Arc::new(FakeBackend::default()))
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "live-only-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| {
        let db = Database::new(&config(&temp).database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE status = 'succeeded'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    let reply = handler
        .handle(
            context(Actor::local("test"), "live-only-subscribe"),
            Command::Subscribe(SubscribeCommand {
                session_id: session_id.clone(),
                after_sequence: Some(0),
                replay_limit: Some(0),
            }),
        )
        .await
        .unwrap();
    let HandlerReply::Subscription {
        accepted,
        mut events,
    } = reply
    else {
        panic!("expected subscription");
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(75), events.recv())
            .await
            .is_err()
    );
    response(
        handler.as_ref(),
        context(Actor::local("test"), "live-only-priority"),
        Command::SetPriority(SetPriorityCommand {
            session_id,
            priority: "high".into(),
        }),
    )
    .await;
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.sequence > accepted.high_water_sequence);
    assert!(matches!(event.event, HiveEvent::Runtime(_)));
    runtime.shutdown().await;
}

#[tokio::test]
async fn live_only_unsequenced_events_are_forwarded_without_advancing_cursor() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime = start_runtime(config(&temp), "daemon-a", Arc::new(FakeBackend::default()))
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "ephemeral-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    let HandlerReply::Subscription { mut events, .. } = handler
        .handle(
            context(Actor::local("test"), "ephemeral-subscribe"),
            Command::Subscribe(SubscribeCommand {
                session_id: session_id.clone(),
                after_sequence: None,
                replay_limit: Some(0),
            }),
        )
        .await
        .unwrap()
    else {
        panic!("expected subscription");
    };
    handler
        .shared
        .events
        .publish(mitsuro_hive_protocol::EventEnvelope {
            version: mitsuro_hive_protocol::ProtocolVersion::CURRENT,
            session_id: Some(session_id),
            run_id: Some("run-ephemeral".into()),
            sequence: None,
            emitted_at_unix_ms: 0,
            event: HiveEvent::Runtime(mitsuro_hive_protocol::RuntimeEvent {
                event_type: "live_delta".into(),
                payload: serde_json::json!({"delta": "authenticated live content"}),
            }),
        });
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.sequence, None);
    assert!(matches!(
        event.event,
        HiveEvent::Runtime(runtime) if runtime.event_type == "live_delta"
    ));
    runtime.shutdown().await;
}

#[tokio::test]
async fn execution_events_are_bounded_ordered_replayable_and_drained_before_completion() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let mut runtime_config = config(&temp);
    runtime_config.execution_event_capacity = 1;
    runtime_config.max_execution_event_bytes = 512;
    let backend = Arc::new(EventBackend);
    let runtime = start_runtime(runtime_config.clone(), "daemon-a", backend.clone())
        .await
        .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "eventful-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| {
        let db = Database::new(&runtime_config.database_path).unwrap();
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE status = 'succeeded'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 1
    })
    .await;
    let db = Database::new(&runtime_config.database_path).unwrap();
    let rows = {
        let mut statement = db
            .conn()
            .prepare(
                "SELECT sequence, event_type, payload_json
                 FROM hive_controller_events ORDER BY sequence",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
    let agentic = rows
        .iter()
        .filter(|(_, event_type, _)| event_type == "agentic_event")
        .collect::<Vec<_>>();
    assert_eq!(agentic.len(), 6);
    for (index, (_, _, payload)) in agentic.iter().enumerate() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(payload).unwrap()["tick_number"].as_u64(),
            Some(index as u64)
        );
    }
    assert_eq!(rows.last().unwrap().1, "run_completed");
    drop(db);

    let reply = handler
        .handle(
            context(Actor::local("test"), "eventful-replay"),
            Command::Subscribe(SubscribeCommand {
                session_id,
                after_sequence: Some(0),
                replay_limit: Some(100),
            }),
        )
        .await
        .unwrap();
    let HandlerReply::Subscription {
        accepted,
        mut events,
    } = reply
    else {
        panic!("expected subscription");
    };
    let high_water = accepted.high_water_sequence.unwrap();
    let mut replayed_agentic = 0;
    while let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(1), events.recv()).await {
        if matches!(
            event.event,
            HiveEvent::Extension(ref extension) if extension.name == "agentic_event"
        ) {
            replayed_agentic += 1;
        }
        if event.sequence == Some(high_water) {
            break;
        }
    }
    assert_eq!(replayed_agentic, 6);
    runtime.shutdown().await;
}

#[tokio::test]
async fn backend_private_error_output_and_tool_payload_never_enter_durable_state() {
    const SENTINEL: &str = "HIVE_PRIVATE_SENTINEL_91f6";
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(PrivacyBoundaryBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let failed_session = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "private-failure"),
            dispatch_command(),
        )
        .await,
    );
    let succeeded_session = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "private-success"),
            dispatch_command(),
        )
        .await,
    );
    wait_for(|| {
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE status IN ('failed', 'succeeded')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            == 2
    })
    .await;

    let db = Database::new(&runtime_config.database_path).unwrap();
    let pattern = format!("%{SENTINEL}%");
    let leaked: i64 = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM hive_runs
                  WHERE COALESCE(last_error, '') LIKE ?1
                     OR COALESCE(outcome_json, '') LIKE ?1)
               + (SELECT COUNT(*) FROM hive_run_attempts
                  WHERE COALESCE(error, '') LIKE ?1
                     OR COALESCE(stop_reason, '') LIKE ?1)
               + (SELECT COUNT(*) FROM hive_controller_events
                  WHERE payload_json LIKE ?1)",
            [&pattern],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(leaked, 0);
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controller_events
                 WHERE event_type = 'agentic_event'
                   AND json_extract(payload_json, '$.type') = 'tool_result'
                   AND json_extract(payload_json, '$.output_redacted') = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2
    );
    drop(db);

    for (index, session_id) in [failed_session, succeeded_session].into_iter().enumerate() {
        let reply = handler
            .handle(
                context(Actor::local("test"), &format!("privacy-replay-{index}")),
                Command::Subscribe(SubscribeCommand {
                    session_id,
                    after_sequence: Some(0),
                    replay_limit: Some(1_000),
                }),
            )
            .await
            .unwrap();
        let HandlerReply::Subscription {
            accepted,
            mut events,
        } = reply
        else {
            panic!("expected privacy replay subscription");
        };
        let high_water = accepted.high_water_sequence.unwrap_or_default();
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_secs(1), events.recv()).await
        {
            assert!(!serde_json::to_string(&event).unwrap().contains(SENTINEL));
            if event.sequence == Some(high_water) {
                break;
            }
        }
    }
    runtime.shutdown().await;

    for path in [
        runtime_config.database_path.clone(),
        runtime_config.database_path.with_extension("db-wal"),
        runtime_config.database_path.with_extension("db-shm"),
    ] {
        if let Ok(bytes) = std::fs::read(&path) {
            assert!(
                !bytes
                    .windows(SENTINEL.len())
                    .any(|window| window == SENTINEL.as_bytes()),
                "private execution payload reached {}",
                path.display()
            );
        }
    }
}

#[tokio::test]
async fn stale_daemon_cannot_materialize_or_advance_a_schedule_after_takeover() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "dispatch-fenced-schedule"),
            dispatch_command(),
        )
        .await,
    );
    let fire_at = chrono::Utc::now() + chrono::Duration::hours(1);
    response(
        handler.as_ref(),
        context(Actor::local("test"), "future-schedule"),
        Command::ScheduleSession(ScheduleCommand {
            session_id,
            wake_at_unix_ms: fire_at.timestamp_millis(),
            reason: "future fenced work".into(),
        }),
    )
    .await;
    wait_for(|| {
        HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .get("hive-scheduler")
            .unwrap()
            .is_some()
    })
    .await;
    let stale_lease =
        HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .get("hive-scheduler")
            .unwrap()
            .unwrap();
    runtime.shutdown().await;

    let lease_store =
        HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap());
    let current = match lease_store
        .acquire(
            "hive-scheduler",
            "daemon-b:boot:test",
            chrono::Utc::now(),
            Duration::from_secs(60),
        )
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        value => panic!("takeover failed: {value:?}"),
    };
    assert!(current.fencing_token > stale_lease.fencing_token);

    let db = Database::new(&runtime_config.database_path).unwrap();
    let schedule_id = db
        .conn()
        .query_row("SELECT id FROM hive_schedules LIMIT 1", [], |row| {
            row.get::<_, String>(0)
        })
        .unwrap();
    drop(db);
    let schedule = HiveScheduleStore::new(Database::new(&runtime_config.database_path).unwrap())
        .get_schedule(&schedule_id)
        .unwrap()
        .unwrap();
    let scheduled_for =
        mitsuro_core::hive::parse_utc_timestamp(schedule.next_fire_at.as_deref().unwrap()).unwrap();
    let events = materialize_schedule_transaction(
        runtime_config.database_path.clone(),
        schedule,
        MisfireResolution {
            enqueue: vec![MisfireDispatch {
                scheduled_for,
                coalesced_count: 0,
            }],
            skipped: Vec::new(),
        },
        scheduled_for,
        None,
        DaemonFence {
            lease_name: stale_lease.lease_name,
            owner_id: stale_lease.owner_id,
            fencing_token: stale_lease.fencing_token,
        },
    )
    .unwrap();
    assert!(events.is_empty());
    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT revision FROM hive_schedules WHERE id = ?1",
                [&schedule_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_schedule_occurrences",
                [],
                |row| { row.get::<_, i64>(0) }
            )
            .unwrap(),
        0
    );
}

fn seed_retention_controller(
    db: &Database,
    suffix: &str,
    run_status: Option<&str>,
) -> super::persistence::ControllerRecord {
    let session_id = format!("session-{suffix}");
    let controller_id = format!("controller-{suffix}");
    let run_id = format!("run-{suffix}");
    let now = "2026-07-17T00:00:00.000000Z";
    db.conn()
        .execute(
            "INSERT INTO sessions (
                 id, title, created_at, updated_at, model, session_type
             ) VALUES (?1, 'Hive', ?2, ?2, 'test:model', 'hive')",
            rusqlite::params![session_id, now],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_controllers (
                 id, scope_key, session_id, status, timezone,
                 max_concurrent_runs, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 'active', 'UTC', 1, ?4, ?4)",
            rusqlite::params![
                controller_id,
                format!("session:{session_id}"),
                session_id,
                now
            ],
        )
        .unwrap();
    if let Some(run_status) = run_status {
        db.conn()
            .execute(
                "INSERT INTO hive_runs (
                     id, controller_id, session_id, kind, objective, config_json,
                     status, available_at, attempt_count, max_attempts, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, 'dispatch', 'retention test', '{}',
                           ?4, ?5, 1, 3, ?5, ?5)",
                rusqlite::params![run_id, controller_id, session_id, run_status, now],
            )
            .unwrap();
    }
    super::persistence::ControllerRecord {
        id: controller_id,
        session_id,
        status: "active".into(),
        timezone: "UTC".into(),
    }
}

#[tokio::test]
async fn durable_runtime_stats_count_each_scheduler_state_exactly() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("stats.db");
    let db = Database::new(&database_path).unwrap();
    for (suffix, status) in [
        ("leased", "leased"),
        ("running", "running"),
        ("queued-a", "queued"),
        ("queued-b", "queued"),
        ("recovery", "recovery_required"),
        ("sleeping", "sleeping"),
        ("inactive", "succeeded"),
        ("alice-running", "running"),
    ] {
        seed_retention_controller(&db, suffix, Some(status));
    }
    db.conn()
        .execute(
            "UPDATE hive_controllers SET status = 'paused' WHERE id = 'controller-inactive'",
            [],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO users (id, email) VALUES ('alice', 'alice@example.invalid')",
            [],
        )
        .unwrap();
    db.conn()
        .execute(
            "UPDATE hive_controllers SET user_id = 'alice'
             WHERE id = 'controller-alice-running'",
            [],
        )
        .unwrap();
    db.conn()
        .execute(
            "UPDATE sessions SET user_id = 'alice' WHERE id = 'session-alice-running'",
            [],
        )
        .unwrap();
    drop(db);

    let persistence =
        super::persistence::RuntimePersistence::new(database_path, Duration::from_secs(60));
    let stats = persistence
        .stats(&Actor::local("stats-test"))
        .await
        .unwrap();

    assert_eq!(stats.active_controllers, 6);
    assert_eq!(stats.active_runs, 2);
    assert_eq!(stats.queued_runs, 2);
    assert_eq!(stats.recovery_required, 1);
    assert!(!stats.pump_alive);
    assert!(!stats.scheduler_ready);

    let alice = persistence
        .stats(&Actor {
            user_id: Some("alice".to_string()),
            client_kind: "stats-test".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(alice.active_controllers, 1);
    assert_eq!(alice.active_runs, 1);
    assert_eq!(alice.queued_runs, 0);
    assert_eq!(alice.recovery_required, 0);
}

#[tokio::test]
async fn external_mutation_receipt_replays_exactly_and_rejects_changed_body() {
    let temp = TempDir::new().unwrap();
    let database_path = temp.path().join("external-idempotency.db");
    Database::new(&database_path).unwrap();
    let persistence =
        super::persistence::RuntimePersistence::new(database_path, Duration::from_secs(60));
    let actor = Actor::local("acceptance-test");
    let calls = Arc::new(AtomicUsize::new(0));

    let first_calls = Arc::clone(&calls);
    let first = persistence
        .mutate_external_idempotent(
            actor.clone(),
            "acceptance-key".into(),
            "resolve_worker_goal_acceptance",
            "body-a".into(),
            move |_| {
                first_calls.fetch_add(1, Ordering::SeqCst);
                Ok(super::persistence::Mutation {
                    response: super::handler::ack("acceptance resolved"),
                    resource_id: Some("acceptance-1".into()),
                    events: Vec::new(),
                })
            },
        )
        .await
        .unwrap();
    assert!(!first.replayed);

    let replay_calls = Arc::clone(&calls);
    let replay = persistence
        .mutate_external_idempotent(
            actor.clone(),
            "acceptance-key".into(),
            "resolve_worker_goal_acceptance",
            "body-a".into(),
            move |_| {
                replay_calls.fetch_add(1, Ordering::SeqCst);
                panic!("a finalized exact replay must not rerun the core mutation")
            },
        )
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.response, first.response);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let changed_calls = Arc::clone(&calls);
    let changed = persistence
        .mutate_external_idempotent(
            actor,
            "acceptance-key".into(),
            "resolve_worker_goal_acceptance",
            "body-b".into(),
            move |_| {
                changed_calls.fetch_add(1, Ordering::SeqCst);
                panic!("a changed-body replay must conflict before core mutation")
            },
        )
        .await;
    assert!(matches!(
        changed,
        Err(super::persistence::RuntimeStoreError::Conflict(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn old_recovery_run_cannot_defeat_the_hard_event_retention_cap() {
    // This test deliberately performs thousands of SQLite retention passes.
    // Keep it behind the same guard as the lease-sensitive runtime tests so
    // the test harness cannot manufacture worker lease expiry through local
    // I/O contention on slower CI hosts.
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let db = Database::new(&temp.path().join("retention-recovery.db")).unwrap();
    let controller = seed_retention_controller(&db, "old-recovery", Some("recovery_required"));
    let run_id = "run-old-recovery";
    let tx =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    super::persistence::append_event(
        &tx,
        &controller,
        "recovery_required",
        Some(run_id),
        None,
        None,
        serde_json::json!({"run_id": run_id, "status": "recovery_required"}),
        "2026-07-17T00:00:00.000000Z",
    )
    .unwrap();
    for index in 0..3_000 {
        super::persistence::append_event(
            &tx,
            &controller,
            "newer_event",
            None,
            None,
            None,
            serde_json::json!({"status": "completed", "attempt": index}),
            "2026-07-17T00:00:00.000000Z",
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let (count, bytes, minimum, maximum): (i64, i64, i64, i64) = db
        .conn()
        .query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(
                      length(CAST(payload_json AS BLOB))
                      + length(CAST(event_type AS BLOB))
                      + COALESCE(length(CAST(dedupe_key AS BLOB)), 0)
                    ), 0),
                    MIN(sequence), MAX(sequence)
               FROM hive_controller_events
              WHERE controller_id = ?1",
            [&controller.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(count, 2_048);
    assert!(bytes <= 2 * 1024 * 1024);
    assert!(minimum > 1, "old recovery history must be prefix-prunable");
    assert_eq!(maximum, 3_001, "the high-water sequence must survive");
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controller_events
                  WHERE controller_id = ?1 AND sequence = 1",
                [&controller.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn unresolved_interaction_spam_is_rejected_without_losing_pending_truth() {
    // See the retention-cap test above: this intentionally expensive fixture
    // shares the runtime serialization guard to avoid cross-test lease races.
    let _test_guard = runtime_test_guard().await;
    const SENTINEL: &str = "HIVE_RETENTION_ARGUMENT_SENTINEL";
    let temp = TempDir::new().unwrap();
    let db = Database::new(&temp.path().join("retention-pending.db")).unwrap();
    let controller = seed_retention_controller(&db, "pending", Some("running"));
    let run_id = "run-pending";
    let tx =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    for interaction in 0..32 {
        super::persistence::append_event(
            &tx,
            &controller,
            "agentic_event",
            Some(run_id),
            None,
            None,
            serde_json::json!({
                "type": "tool_approval_required",
                "id": format!("tool-pending-{interaction}"),
                "name": "bash",
                "arguments": {"command": SENTINEL},
            }),
            "2026-07-17T00:00:00.000000Z",
        )
        .unwrap();
    }
    for index in 0..1_984 {
        super::persistence::append_event(
            &tx,
            &controller,
            "newer_event",
            None,
            None,
            None,
            serde_json::json!({"status": "completed", "attempt": index}),
            "2026-07-17T00:00:00.000000Z",
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let rejected =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    let error = super::persistence::append_event(
        &rejected,
        &controller,
        "agentic_event",
        Some(run_id),
        None,
        None,
        serde_json::json!({
            "type": "tool_approval_required",
            "id": "tool-pending-overflow",
            "name": "bash",
            "arguments": {},
        }),
        "2026-07-17T00:00:00.000000Z",
    )
    .unwrap_err();
    assert!(matches!(
        error,
        super::persistence::RuntimeStoreError::ResourceExhausted(_)
    ));
    drop(rejected);

    let (count, pending): (i64, String) = db
        .conn()
        .query_row(
            "SELECT COUNT(*),
                    MAX(CASE WHEN sequence = 1 THEN payload_json END)
               FROM hive_controller_events
              WHERE controller_id = ?1",
            [&controller.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, 2_016);
    assert!(!pending.contains(SENTINEL));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&pending).unwrap()["id"],
        "tool-pending-0"
    );

    let unrelated =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    let unrelated_error = super::persistence::append_event(
        &unrelated,
        &controller,
        "unrelated_overflow",
        None,
        None,
        None,
        serde_json::json!({"status": "completed"}),
        "2026-07-17T00:00:00.000000Z",
    )
    .unwrap_err();
    assert!(matches!(
        unrelated_error,
        super::persistence::RuntimeStoreError::ResourceExhausted(_)
    ));
    drop(unrelated);

    let resolving =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    let resolution = super::persistence::append_event(
        &resolving,
        &controller,
        "tool_approval_queued",
        Some(run_id),
        None,
        None,
        serde_json::json!({
            "run_id": run_id,
            "tool_call_id": "tool-pending-31",
            "approved": false,
        }),
        "2026-07-17T00:00:00.000000Z",
    )
    .unwrap();
    assert_eq!(resolution.sequence, 2_017);
    resolving.commit().unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controller_events WHERE controller_id = ?1",
                [&controller.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2_017
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT MIN(sequence) FROM hive_controller_events WHERE controller_id = ?1",
                [&controller.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn dedupe_hit_runs_retention_and_terminal_outbox_maintenance() {
    let temp = TempDir::new().unwrap();
    let db = Database::new(&temp.path().join("retention-dedupe.db")).unwrap();
    let controller = seed_retention_controller(&db, "dedupe", Some("succeeded"));
    let tx =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    for sequence in 1_i64..=2_050 {
        tx.execute(
            "INSERT INTO hive_controller_events (
                 controller_id, sequence, event_type, dedupe_key, payload_json, created_at
             ) VALUES (?1, ?2, 'historical_event', ?3, '{\"status\":\"completed\"}', ?4)",
            rusqlite::params![
                controller.id,
                sequence,
                (sequence == 2_050).then_some("dedupe-latest"),
                "2026-07-17T00:00:00.000000Z",
            ],
        )
        .unwrap();
    }
    tx.execute(
        "INSERT INTO hive_control_outbox (
             id, controller_id, session_id, run_id, control_kind, dedupe_key,
             payload_json, status, available_at, delivered_at, created_at, updated_at
         ) VALUES (
             'outbox-old', ?1, ?2, 'run-dedupe', 'tool_approval', 'old-control',
             '{\"tool_call_id\":\"old-tool\",\"approved\":true}', 'delivered',
             '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z',
             '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z'
         )",
        rusqlite::params![controller.id, controller.session_id],
    )
    .unwrap();
    tx.commit().unwrap();

    let maintenance =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    let existing = super::persistence::append_event(
        &maintenance,
        &controller,
        "historical_event",
        None,
        None,
        Some("dedupe-latest"),
        serde_json::json!({"ignored": "duplicate"}),
        "2026-07-17T00:00:00.000000Z",
    )
    .unwrap();
    assert_eq!(existing.sequence, 2_050);
    maintenance.commit().unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_controller_events WHERE controller_id = ?1",
                [&controller.id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        2_048
    );
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_control_outbox WHERE id = 'outbox-old'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

fn test_worker_model_key() -> mitsuro_core::ai::models::ModelKey {
    mitsuro_core::ai::models::ModelKey::new(
        ProviderId::OpenAI,
        "test:model",
        mitsuro_core::ai::models::ApiFormat::OpenAIResponses,
    )
}

fn freeze_test_session_model(
    db_path: &std::path::Path,
    session_id: &str,
    model_key: &mitsuro_core::ai::models::ModelKey,
    model_catalog_revision: &str,
) {
    Database::new(db_path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE sessions SET model_key_json = ?2, model_catalog_revision = ?3
             WHERE id = ?1",
            params![
                session_id,
                serde_json::to_string(model_key).unwrap(),
                model_catalog_revision,
            ],
        )
        .unwrap();
}

/// The real backend commits the canonical Worker response and provider
/// accounting before it reports success. Runtime tests need the same durable
/// boundary; a bare `ExecutionOutcome::Succeeded` is deliberately rejected by
/// `HiveRunStore` for Worker-bound runs.
fn commit_test_worker_response(
    db_path: &std::path::Path,
    request: &ExecutionRequest,
) -> anyhow::Result<()> {
    let claim = &request.claim;
    let context = claim
        .run
        .execution_context
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("test Worker run has no execution context"))?;
    let worker_id = claim
        .run
        .worker_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("test Worker run has no Worker binding"))?;
    let session_id = claim
        .run
        .session_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("test Worker run has no session binding"))?;
    let run_lease_epoch = claim
        .run
        .lease_epoch
        .ok_or_else(|| anyhow::anyhow!("test Worker run has no lease epoch"))?;
    let model_key: mitsuro_core::ai::models::ModelKey = serde_json::from_value(
        claim
            .run
            .config
            .get("model_key")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("test Worker run has no frozen model key"))?,
    )?;
    let model_catalog_revision = claim
        .run
        .config
        .get("model_catalog_revision")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let permission_mode = claim
        .run
        .config
        .get("permission_mode")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("test Worker run has no permission mode"))?
        .parse::<PermissionMode>()
        .map_err(anyhow::Error::msg)?;
    let origin = claim
        .run
        .governor
        .as_ref()
        .and_then(|governor| governor.origin)
        .ok_or_else(|| anyhow::anyhow!("test Worker run has no governor origin"))?;
    let lane = context.lane().clone();
    let owner_user_id = Database::new(db_path)?.conn().query_row(
        "SELECT user_id FROM hive_workers WHERE id = ?1",
        [worker_id],
        |row| row.get::<_, Option<String>>(0),
    )?;
    let fence = DaemonFence {
        lease_name: "hive-scheduler".into(),
        owner_id: request.daemon_instance_id.clone(),
        fencing_token: run_lease_epoch,
    };
    let governor = WorkerProviderCallGovernor::new(WorkerProviderGovernorBinding {
        db_path: db_path.to_path_buf(),
        worker_id: worker_id.to_string(),
        worker_revision: context.worker_revision(),
        owner_user_id: owner_user_id.clone(),
        session_id: session_id.to_string(),
        conversation_lane: lane.clone(),
        run_id: claim.run.id.clone(),
        run_lease_token: claim.lease_token.clone(),
        run_lease_epoch,
        model_key,
        model_catalog_revision,
        permission_mode,
        origin,
        workflow_goal_id: claim.run.workflow_goal_id.clone(),
        workflow_attempt_id: claim.run.workflow_attempt_id.clone(),
        pricing: None,
        override_grant_id: None,
    })?;
    let permit = match governor.admit(
        WorkerProviderCallSlot::new(WorkerProviderCallKind::AgentTurn, 0, 0),
        1,
    )? {
        WorkerProviderAdmission::Allowed(permit) => permit,
        WorkerProviderAdmission::Gated(decision) => {
            anyhow::bail!("test Worker provider call was gated: {decision:?}")
        }
        WorkerProviderAdmission::AlreadyStarted(call) => anyhow::bail!(
            "test Worker provider call was already started: {}",
            call.provider_call_id
        ),
    };
    let provider_call_id = permit.provider_call_id().to_string();
    SqliteWorkerConversationResponseStore::new(db_path, fence).commit_response(
        &CommitWorkerConversationResponse {
            worker_id: worker_id.to_string(),
            worker_revision: context.worker_revision(),
            owner_user_id,
            session_id: session_id.to_string(),
            lane,
            run_id: claim.run.id.clone(),
            run_lease_token: claim.lease_token.clone(),
            run_lease_epoch,
            provider_call_id,
            response_text: format!("Canonical test response for run {}.", claim.run.id),
            committed_at: Utc::now(),
        },
    )?;
    permit.complete(WorkerProviderCompletion::acknowledged(
        WorkerProviderTerminalOutcome::Completed,
        None,
    ))?;
    Ok(())
}

/// Maps each Worker lane to a fixed outcome while using the production-like
/// canonical response boundary for every successful Worker execution.
struct CanonicalWorkerBackend {
    database_path: std::path::PathBuf,
    executions: Mutex<Vec<(String, String)>>,
    outcomes_by_session: Mutex<std::collections::HashMap<String, ExecutionOutcome>>,
    controls: Mutex<Vec<(String, ExecutionControl)>>,
}

impl CanonicalWorkerBackend {
    fn new(database_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            database_path: database_path.into(),
            executions: Mutex::new(Vec::new()),
            outcomes_by_session: Mutex::new(std::collections::HashMap::new()),
            controls: Mutex::new(Vec::new()),
        }
    }

    fn fail_session(&self, session_id: &str) {
        self.outcomes_by_session.lock().unwrap().insert(
            session_id.to_string(),
            ExecutionOutcome::Failed {
                error: "provider unavailable".into(),
                retryable: false,
                retry_after: None,
            },
        );
    }

    fn execution_sessions(&self) -> Vec<String> {
        self.executions
            .lock()
            .unwrap()
            .iter()
            .map(|(session, _)| session.clone())
            .collect()
    }
}

#[async_trait]
impl ExecutionBackend for CanonicalWorkerBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        let session_id = request.claim.run.session_id.clone().unwrap_or_default();
        self.executions
            .lock()
            .unwrap()
            .push((session_id.clone(), request.claim.run.id.clone()));
        let outcome = self
            .outcomes_by_session
            .lock()
            .unwrap()
            .get(&session_id)
            .cloned()
            .unwrap_or(ExecutionOutcome::Succeeded {
                output: serde_json::json!({"ok": true}),
            });
        if matches!(&outcome, ExecutionOutcome::Succeeded { .. }) {
            commit_test_worker_response(&self.database_path, &request)
                .expect("successful test Worker execution must commit its canonical response");
        }
        outcome
    }

    async fn control(&self, session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        self.controls
            .lock()
            .unwrap()
            .push((session_id.to_string(), control));
        Ok(())
    }
}

struct GroupFixture {
    group_id: String,
    /// (worker_id, dm_session_id) in roster order.
    members: Vec<(String, String)>,
}

fn bind_worker_private_controller(db_path: &std::path::Path, worker_id: &str, dm_session_id: &str) {
    let now = canonical_timestamp(Utc::now());
    Database::new(db_path)
        .unwrap()
        .conn()
        .execute(
            "INSERT INTO hive_controllers (
                 id, scope_key, user_id, session_id, status, timezone,
                 max_concurrent_runs, created_at, updated_at, worker_id
             ) VALUES (?1, ?2, NULL, ?3, 'active', 'UTC', 1, ?4, ?4, ?5)",
            params![
                format!("worker-controller:{worker_id}"),
                format!("worker:{worker_id}"),
                dm_session_id,
                now,
                worker_id,
            ],
        )
        .unwrap();
}

fn seed_group(
    db_path: &std::path::Path,
    slugs: &[&str],
    mode: mitsuro_core::storage::HiveGroupExecutionMode,
    max_rounds: u32,
) -> GroupFixture {
    let model_key = test_worker_model_key();
    let model_catalog_revision = "group-test-catalog-1";
    let session_manager = SessionManager::new(Database::new(db_path).unwrap());
    let worker_store = mitsuro_core::storage::HiveWorkerStore::new(Database::new(db_path).unwrap());
    let mut members = Vec::new();
    for slug in slugs {
        let dm_session_id = session_manager
            .create_session_for_user_with_config(
                &format!("{slug} DM"),
                Some("test:model"),
                None,
                None,
                WorkspaceMode::Neutral,
                None,
                None,
                SessionType::Hive,
            )
            .unwrap();
        freeze_test_session_model(db_path, &dm_session_id, &model_key, model_catalog_revision);
        let worker = worker_store
            .create(&mitsuro_core::storage::NewHiveWorker {
                model: Some("test:model".into()),
                model_key: Some(model_key.clone()),
                model_catalog_revision: Some(model_catalog_revision.into()),
                dm_session_id: Some(dm_session_id.clone()),
                ..mitsuro_core::storage::NewHiveWorker::new(*slug)
            })
            .unwrap();
        bind_worker_private_controller(db_path, &worker.id, &dm_session_id);
        members.push((worker.id, dm_session_id));
    }
    let group = mitsuro_core::storage::HiveGroupStore::new(Database::new(db_path).unwrap())
        .create(&mitsuro_core::storage::NewHiveGroup {
            user_id: None,
            title: "Integration Room".into(),
            execution_mode: mode,
            max_rounds: Some(max_rounds),
            max_member_messages_per_turn: Some(2),
            parallelism: Some(2),
            context_window_messages: Some(24),
            default_assignee_worker_id: None,
            member_worker_ids: members.iter().map(|(id, _)| id.clone()).collect(),
        })
        .unwrap();
    GroupFixture {
        group_id: group.id,
        members,
    }
}

fn group_turn_response(response: &ResponsePayload) -> mitsuro_hive_protocol::GroupTurnResponse {
    match response {
        ResponsePayload::GroupTurn(turn) => turn.clone(),
        other => panic!("expected group turn response, got {other:?}"),
    }
}

fn queue_group_turn_without_runtime(
    db_path: &std::path::Path,
    group_id: &str,
    idempotency_key: &str,
) -> mitsuro_hive_protocol::GroupTurnResponse {
    let db = Database::new(db_path).unwrap();
    let tx =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    let mutation = super::groups::group_message(
        &tx,
        &Actor::local("claim-race-test"),
        &canonical_timestamp(Utc::now()),
        GroupMessageCommand {
            group_id: group_id.to_string(),
            message: "Queue one group turn without starting the runtime".into(),
            mentions_override: None,
        },
        idempotency_key,
        mitsuro_core::storage::WorkerRunOrigin::UserGroup,
    )
    .unwrap();
    tx.commit().unwrap();
    group_turn_response(&mutation.response)
}

fn load_turn_status(
    db_path: &std::path::Path,
    turn_id: &str,
) -> (
    mitsuro_core::storage::HiveGroupTurnStatus,
    Option<serde_json::Value>,
    u32,
) {
    let db = Database::new(db_path).unwrap();
    let turn = mitsuro_core::storage::hive_groups::load_turn(db.conn(), turn_id)
        .unwrap()
        .unwrap();
    (turn.status, turn.member_outcomes, turn.next_speaker_index)
}

#[test]
fn archived_group_turn_is_ineligible_at_claim_and_mark_running_boundaries() {
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let archived_before_claim = seed_group(
        &runtime_config.database_path,
        &["archive-before-claim"],
        mitsuro_core::storage::HiveGroupExecutionMode::Roundtable,
        1,
    );
    let first_turn = queue_group_turn_without_runtime(
        &runtime_config.database_path,
        &archived_before_claim.group_id,
        "archive-before-claim-turn",
    );
    mitsuro_core::storage::HiveGroupStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    )
    .set_status(
        &archived_before_claim.group_id,
        mitsuro_core::storage::HiveGroupStatus::Archived,
    )
    .unwrap();

    let lease =
        match HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .acquire(
                "hive-scheduler",
                "archive-claim-race-daemon",
                Utc::now(),
                Duration::from_secs(30),
            )
            .unwrap()
        {
            DaemonLeaseAcquire::Acquired(lease) => lease,
            held => panic!("expected scheduler lease, got {held:?}"),
        };
    let fence = DaemonFence {
        lease_name: lease.lease_name,
        owner_id: lease.owner_id,
        fencing_token: lease.fencing_token,
    };
    let store = HiveRunStore::new(Database::new(&runtime_config.database_path).unwrap());
    assert!(store
        .claim_next_fenced(
            &ClaimRunRequest {
                executor_id: "archive-claim-race-daemon".into(),
                lease_epoch: fence.fencing_token,
                now: Utc::now(),
                lease_duration: Duration::from_secs(10),
                global_concurrency_limit: 8,
            },
            &fence,
        )
        .unwrap()
        .is_none());
    assert_eq!(
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT status FROM hive_runs WHERE group_turn_id = ?1",
                [&first_turn.turn_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "queued"
    );

    let archived_after_claim = seed_group(
        &runtime_config.database_path,
        &["archive-after-claim"],
        mitsuro_core::storage::HiveGroupExecutionMode::Roundtable,
        1,
    );
    let second_turn = queue_group_turn_without_runtime(
        &runtime_config.database_path,
        &archived_after_claim.group_id,
        "archive-after-claim-turn",
    );
    let claim = store
        .claim_next_fenced(
            &ClaimRunRequest {
                executor_id: "archive-claim-race-daemon".into(),
                lease_epoch: fence.fencing_token,
                now: Utc::now(),
                lease_duration: Duration::from_secs(10),
                global_concurrency_limit: 8,
            },
            &fence,
        )
        .unwrap()
        .expect("active group turn should be claimable");
    assert_eq!(
        claim.run.kind,
        mitsuro_core::storage::HiveRunKind::GroupTurn
    );
    assert_eq!(
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT group_turn_id FROM hive_runs WHERE id = ?1",
                [&claim.run.id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        second_turn.turn_id
    );
    mitsuro_core::storage::HiveGroupStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    )
    .set_status(
        &archived_after_claim.group_id,
        mitsuro_core::storage::HiveGroupStatus::Archived,
    )
    .unwrap();
    assert!(!store
        .mark_running_fenced(
            &claim.run.id,
            &claim.lease_token,
            fence.fencing_token,
            Utc::now(),
            &fence,
        )
        .unwrap());
    assert_eq!(
        store.get_run(&claim.run.id).unwrap().unwrap().status,
        mitsuro_core::hive::HiveRunStatus::Leased
    );
    assert_eq!(
        store
            .finish_cancelled_group_turn_claim_fenced(
                &claim.run.id,
                &claim.lease_token,
                fence.fencing_token,
                &RunCompletion {
                    target_status: mitsuro_core::hive::HiveRunStatus::Cancelled,
                    now: Utc::now(),
                    available_at: None,
                    wake_at: None,
                    stop_reason: Some("legacy group archive".into()),
                    error: None,
                    outcome: Some(serde_json::json!({"kind": "cancelled"})),
                    trace_sequence_end: None,
                },
                &fence,
            )
            .unwrap(),
        Some(mitsuro_core::hive::HiveRunStatus::Cancelled)
    );
    assert_eq!(
        load_turn_status(&runtime_config.database_path, &second_turn.turn_id).0,
        mitsuro_core::storage::HiveGroupTurnStatus::Running,
        "the specialized claim authority must not depend on pump turn repair"
    );
}

#[tokio::test]
async fn group_archive_atomically_stops_active_work_and_replays_for_exact_owner() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let fixture = seed_group(
        &runtime_config.database_path,
        &["archive-alpha", "archive-beta"],
        mitsuro_core::storage::HiveGroupExecutionMode::Workbench,
        1,
    );
    let backend = Arc::new(BlockingBackend::default());
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-group-archive",
        backend.clone(),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let turn = group_turn_response(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "group-archive-turn"),
            Command::GroupMessage(GroupMessageCommand {
                group_id: fixture.group_id.clone(),
                message: "Both members should stop when this room is archived".into(),
                mentions_override: None,
            }),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 2).await;

    let db = Database::new(&runtime_config.database_path).unwrap();
    let running = {
        let mut statement = db
            .conn()
            .prepare(
                "SELECT id, session_id FROM hive_runs
                 WHERE group_turn_id = ?1 AND status = 'running'
                 ORDER BY id",
            )
            .unwrap();
        statement
            .query_map([&turn.turn_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert_eq!(running.len(), 2);
    drop(db);

    let command = Command::GroupArchive(GroupArchiveCommand {
        group_id: fixture.group_id.clone(),
    });
    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "archive-active-group"),
        command.clone(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "archive-active-group"),
        command.clone(),
    )
    .await;
    assert_eq!(first, replay);
    assert!(matches!(
        first,
        ResponsePayload::Ack(ref ack) if ack.message.as_deref() == Some("group archived")
    ));

    let foreign = handler
        .handle(
            context(
                Actor {
                    user_id: Some("foreign-owner".into()),
                    client_kind: "test".into(),
                },
                "foreign-archive-active-group",
            ),
            command,
        )
        .await
        .unwrap_err();
    assert_eq!(foreign.code, "ownership_denied");

    let db_path = runtime_config.database_path.clone();
    let turn_id = turn.turn_id.clone();
    let group_id = fixture.group_id.clone();
    wait_for(move || {
        let db = Database::new(&db_path).unwrap();
        let (status, active_runs): (String, i64) = db
            .conn()
            .query_row(
                "SELECT status,
                        (SELECT COUNT(*) FROM hive_runs
                         WHERE group_turn_id = ?1
                           AND status IN ('queued', 'leased', 'running', 'sleeping',
                                          'retry_wait', 'awaiting_input', 'recovery_required'))
                 FROM hive_groups WHERE id = ?2",
                rusqlite::params![turn_id, group_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        status == "archived" && active_runs == 0
    })
    .await;

    assert_eq!(
        load_turn_status(&runtime_config.database_path, &turn.turn_id).0,
        mitsuro_core::storage::HiveGroupTurnStatus::Cancelled
    );
    {
        let controls = backend.controls.lock().unwrap();
        for (run_id, session_id) in &running {
            assert!(controls.iter().any(|(controlled_session, control)| {
                controlled_session == session_id
                    && matches!(
                        control,
                        ExecutionControl::CancelRun { run_id: controlled_run, .. }
                            if controlled_run == run_id
                    )
            }));
        }
    }
    let stop_messages = mitsuro_core::storage::HiveGroupStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    )
    .list_recent_messages(&fixture.group_id, 50)
    .unwrap()
    .into_iter()
    .filter(|message| message.content.contains("Turn stopped"))
    .count();
    assert_eq!(
        stop_messages, 1,
        "archive replay must not duplicate effects"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn archived_roundtable_reconciliation_cancels_current_run_without_dispatching_next() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let fixture = seed_group(
        &runtime_config.database_path,
        &["roundtable-alpha", "roundtable-beta"],
        mitsuro_core::storage::HiveGroupExecutionMode::Roundtable,
        1,
    );
    let backend = Arc::new(BlockingBackend::default());
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-archived-roundtable",
        backend.clone(),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let turn = group_turn_response(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "archived-roundtable-turn"),
            Command::GroupMessage(GroupMessageCommand {
                group_id: fixture.group_id.clone(),
                message: "Do not advance after a mixed-version archive".into(),
                mentions_override: None,
            }),
        )
        .await,
    );
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    let (run_id, session_id): (String, String) = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT id, session_id FROM hive_runs
             WHERE group_turn_id = ?1 AND status = 'running'",
            [&turn.turn_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    // Model an older HTTP server that wrote only the group status. The pump
    // must fail closed before evaluating the next roundtable speaker.
    mitsuro_core::storage::HiveGroupStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    )
    .set_status(
        &fixture.group_id,
        mitsuro_core::storage::HiveGroupStatus::Archived,
    )
    .unwrap();

    let db_path = runtime_config.database_path.clone();
    let turn_id = turn.turn_id.clone();
    wait_for(move || {
        let db = Database::new(&db_path).unwrap();
        db.conn()
            .query_row(
                "SELECT status = 'cancelled'
                        AND (SELECT COUNT(*) FROM hive_runs
                             WHERE group_turn_id = ?1) = 1
                        AND (SELECT COUNT(*) FROM hive_runs
                             WHERE group_turn_id = ?1 AND status = 'cancelled') = 1
                 FROM hive_group_turns WHERE id = ?1",
                [&turn_id],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
    })
    .await;
    assert_eq!(backend.executions.load(Ordering::SeqCst), 1);
    assert!(backend
        .controls
        .lock()
        .unwrap()
        .iter()
        .any(|(controlled_session, control)| {
            controlled_session == &session_id
                && matches!(
                    control,
                    ExecutionControl::CancelRun { run_id: controlled_run, .. }
                        if controlled_run == &run_id
                )
        }));
    let (_, _, next_speaker_index) = load_turn_status(&runtime_config.database_path, &turn.turn_id);
    assert_eq!(next_speaker_index, 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn pausing_worker_cancels_only_its_exact_running_group_lane() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let fixture = seed_group(
        &runtime_config.database_path,
        &["pause-target", "running-sibling"],
        mitsuro_core::storage::HiveGroupExecutionMode::Workbench,
        1,
    );
    let backend = Arc::new(BlockingBackend::default());
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-pause-group-lane",
        backend.clone(),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let turn = group_turn_response(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "pause-group-turn"),
            Command::GroupMessage(GroupMessageCommand {
                group_id: fixture.group_id.clone(),
                message: "Work independently until one lane is paused".into(),
                mentions_override: None,
            }),
        )
        .await,
    );
    assert_eq!(turn.target_worker_ids.len(), 2);
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 2).await;

    let target_worker_id = fixture.members[0].0.clone();
    let sibling_worker_id = fixture.members[1].0.clone();
    let db = Database::new(&runtime_config.database_path).unwrap();
    let (target_run_id, target_session_id): (String, String) = db
        .conn()
        .query_row(
            "SELECT id, session_id FROM hive_runs
             WHERE worker_id = ?1 AND kind = 'group_turn' AND status = 'running'",
            [&target_worker_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let sibling_run_id: String = db
        .conn()
        .query_row(
            "SELECT id FROM hive_runs
             WHERE worker_id = ?1 AND kind = 'group_turn' AND status = 'running'",
            [&sibling_worker_id],
            |row| row.get(0),
        )
        .unwrap();
    drop(db);

    let paused = response(
        handler.as_ref(),
        context(Actor::local("test"), "pause-group-worker"),
        Command::SetWorkerStatus(SetWorkerStatusCommand {
            worker_id: target_worker_id.clone(),
            expected_revision: 1,
            status: WorkerTargetStatus::Paused,
        }),
    )
    .await;
    let ResponsePayload::WorkerMutation(paused) = paused else {
        panic!("expected Worker mutation response");
    };
    assert_eq!(paused.cancellation_requests.len(), 1);
    assert_eq!(paused.cancellation_requests[0].run_id, target_run_id);
    assert_eq!(
        paused.cancellation_requests[0].session_id,
        target_session_id
    );

    wait_for(|| {
        backend
            .controls
            .lock()
            .unwrap()
            .iter()
            .any(|(session, control)| {
                session == &target_session_id
                    && matches!(
                        control,
                        ExecutionControl::CancelRun { run_id, .. } if run_id == &target_run_id
                    )
            })
    })
    .await;
    assert!(backend.controls.lock().unwrap().iter().all(|(_, control)| {
        !matches!(
            control,
            ExecutionControl::CancelRun { run_id, .. } if run_id == &sibling_run_id
        )
    }));
    let (target_status, sibling_status): (String, String) =
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT
                     (SELECT status FROM hive_runs WHERE id = ?1),
                     (SELECT status FROM hive_runs WHERE id = ?2)",
                rusqlite::params![target_run_id, sibling_run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
    assert_eq!(target_status, "recovery_required");
    assert_eq!(sibling_status, "running");
    runtime.shutdown().await;
}

#[tokio::test]
async fn group_workbench_turn_executes_members_and_aggregates_partial_failure() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let fixture = seed_group(
        &runtime_config.database_path,
        &["healthy", "flaky"],
        mitsuro_core::storage::HiveGroupExecutionMode::Workbench,
        3,
    );
    let backend = Arc::new(CanonicalWorkerBackend::new(
        runtime_config.database_path.clone(),
    ));
    let flaky_lane =
        super::groups::group_worker_lane_session_id(&fixture.group_id, &fixture.members[1].0);
    backend.fail_session(&flaky_lane);

    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::clone(&backend) as Arc<dyn ExecutionBackend>,
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let turn = group_turn_response(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "group-workbench-1"),
            Command::GroupMessage(GroupMessageCommand {
                group_id: fixture.group_id.clone(),
                message: "everyone take a look".into(),
                mentions_override: None,
            }),
        )
        .await,
    );
    assert_eq!(turn.status, "running");
    assert_eq!(turn.target_worker_ids.len(), 2);

    let db_path = runtime_config.database_path.clone();
    let turn_id = turn.turn_id.clone();
    wait_for(move || {
        load_turn_status(&db_path, &turn_id).0
            == mitsuro_core::storage::HiveGroupTurnStatus::Partial
    })
    .await;

    // Both members executed on isolated private group lanes; neither direct
    // DM was used, and the healthy sibling was never cancelled by the flaky
    // one.
    let sessions = backend.execution_sessions();
    assert_eq!(sessions.len(), 2);
    for (worker_id, dm_session_id) in &fixture.members {
        let lane = super::groups::group_worker_lane_session_id(&fixture.group_id, worker_id);
        assert!(sessions.contains(&lane));
        assert!(!sessions.contains(dm_session_id));
    }

    let (_, outcomes, _) = load_turn_status(&runtime_config.database_path, &turn.turn_id);
    let outcomes = outcomes.unwrap();
    assert_eq!(outcomes[&fixture.members[0].0]["status"], "succeeded");
    assert_eq!(outcomes[&fixture.members[1].0]["status"], "failed");
    runtime.shutdown().await;
}

#[tokio::test]
async fn group_roundtable_advances_rotating_speakers_to_completion() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let fixture = seed_group(
        &runtime_config.database_path,
        &["alpha", "beta"],
        mitsuro_core::storage::HiveGroupExecutionMode::Roundtable,
        2,
    );
    let backend = Arc::new(CanonicalWorkerBackend::new(
        runtime_config.database_path.clone(),
    ));
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::clone(&backend) as Arc<dyn ExecutionBackend>,
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let turn = group_turn_response(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "group-roundtable-1"),
            Command::GroupMessage(GroupMessageCommand {
                group_id: fixture.group_id.clone(),
                message: "round table please".into(),
                mentions_override: None,
            }),
        )
        .await,
    );

    let db_path = runtime_config.database_path.clone();
    let turn_id = turn.turn_id.clone();
    wait_for(move || {
        load_turn_status(&db_path, &turn_id).0
            == mitsuro_core::storage::HiveGroupTurnStatus::Completed
    })
    .await;

    // Two rounds over [alpha, beta] rotate the speaker order per round and
    // execute strictly one at a time.
    let alpha =
        super::groups::group_worker_lane_session_id(&fixture.group_id, &fixture.members[0].0);
    let beta =
        super::groups::group_worker_lane_session_id(&fixture.group_id, &fixture.members[1].0);
    assert_eq!(
        backend.execution_sessions(),
        vec![alpha.clone(), beta.clone(), beta, alpha]
    );
    let (_, outcomes, next_speaker_index) =
        load_turn_status(&runtime_config.database_path, &turn.turn_id);
    assert_eq!(next_speaker_index, 4);
    let outcomes = outcomes.unwrap();
    assert_eq!(outcomes[&fixture.members[0].0]["status"], "succeeded");
    assert_eq!(outcomes[&fixture.members[1].0]["status"], "succeeded");
    runtime.shutdown().await;
}

struct GatedGroupBackend {
    started: AtomicUsize,
    hold: AtomicBool,
    release: Notify,
    inner: CanonicalWorkerBackend,
}

impl GatedGroupBackend {
    fn holding(database_path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            started: AtomicUsize::new(0),
            hold: AtomicBool::new(true),
            release: Notify::new(),
            inner: CanonicalWorkerBackend::new(database_path),
        }
    }
}

#[async_trait]
impl ExecutionBackend for GatedGroupBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        self.started.fetch_add(1, Ordering::SeqCst);
        if self.hold.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        self.inner.execute(request).await
    }

    async fn control(&self, session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        self.inner.control(session_id, control).await
    }
}

fn expire_all_run_leases(db_path: &std::path::Path) {
    let db = Database::new(db_path).unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs
             SET lease_expires_at = '2020-01-01T00:00:00.000000Z'
             WHERE lease_expires_at IS NOT NULL",
            [],
        )
        .unwrap();
}

fn count_group_user_messages(db_path: &std::path::Path, group_id: &str) -> usize {
    mitsuro_core::storage::HiveGroupStore::new(Database::new(db_path).unwrap())
        .list_recent_messages(group_id, 50)
        .unwrap()
        .into_iter()
        .filter(|message| message.sender_kind == mitsuro_core::storage::HiveGroupSenderKind::User)
        .count()
}

fn group_turn_member_runs(db_path: &std::path::Path, group_id: &str) -> Vec<(String, String)> {
    let db = Database::new(db_path).unwrap();
    let mut statement = db
        .conn()
        .prepare(
            "SELECT worker_id, status
             FROM hive_runs
             WHERE group_id = ?1 AND kind = 'group_turn'
             ORDER BY created_at ASC, id ASC",
        )
        .unwrap();
    statement
        .query_map([group_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

#[tokio::test]
async fn group_workbench_turn_survives_daemon_restart_without_duplicate_room_messages() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let fixture = seed_group(
        &runtime_config.database_path,
        &["alpha", "beta"],
        mitsuro_core::storage::HiveGroupExecutionMode::Workbench,
        3,
    );
    let gated = Arc::new(GatedGroupBackend::holding(
        runtime_config.database_path.clone(),
    ));
    let first = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::clone(&gated) as Arc<dyn ExecutionBackend>,
    )
    .await
    .unwrap();
    let handler = first.handler();
    let turn = group_turn_response(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "group-restart-1"),
            Command::GroupMessage(GroupMessageCommand {
                group_id: fixture.group_id.clone(),
                message: "keep going after the crash".into(),
                mentions_override: None,
            }),
        )
        .await,
    );
    assert_eq!(turn.status, "running");
    wait_for(|| gated.started.load(Ordering::SeqCst) >= 1).await;
    first.shutdown().await;

    expire_all_run_leases(&runtime_config.database_path);

    let second = start_runtime(
        runtime_config.clone(),
        "daemon-b",
        Arc::new(CanonicalWorkerBackend::new(
            runtime_config.database_path.clone(),
        )) as Arc<dyn ExecutionBackend>,
    )
    .await
    .unwrap();
    let db_path = runtime_config.database_path.clone();
    let turn_id = turn.turn_id.clone();
    wait_for(move || {
        load_turn_status(&db_path, &turn_id).0
            == mitsuro_core::storage::HiveGroupTurnStatus::Completed
    })
    .await;

    assert_eq!(
        count_group_user_messages(&runtime_config.database_path, &fixture.group_id),
        1
    );
    let member_runs = group_turn_member_runs(&runtime_config.database_path, &fixture.group_id);
    assert_eq!(member_runs.len(), 2);
    assert!(member_runs.iter().all(|(_, status)| status == "succeeded"));
    let worker_ids: std::collections::BTreeSet<_> = member_runs
        .into_iter()
        .map(|(worker_id, _)| worker_id)
        .collect();
    assert_eq!(
        worker_ids,
        fixture
            .members
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<std::collections::BTreeSet<_>>()
    );

    let replay = group_turn_response(
        &response(
            second.handler().as_ref(),
            context(Actor::local("test"), "group-restart-1"),
            Command::GroupMessage(GroupMessageCommand {
                group_id: fixture.group_id.clone(),
                message: "keep going after the crash".into(),
                mentions_override: None,
            }),
        )
        .await,
    );
    assert_eq!(replay.turn_id, turn.turn_id);
    assert_eq!(
        count_group_user_messages(&runtime_config.database_path, &fixture.group_id),
        1
    );
    assert_eq!(
        group_turn_member_runs(&runtime_config.database_path, &fixture.group_id).len(),
        2
    );
    second.shutdown().await;
}

fn seed_worker_with_dm(
    db_path: &std::path::Path,
    slug: &str,
    autonomy: mitsuro_core::storage::HiveWorkerAutonomy,
    heartbeat_interval_secs: Option<u32>,
) -> (String, String) {
    let session_manager = SessionManager::new(Database::new(db_path).unwrap());
    let dm_session_id = session_manager
        .create_session_for_user_with_config(
            &format!("{slug} DM"),
            Some("test:model"),
            None,
            None,
            WorkspaceMode::Neutral,
            None,
            None,
            SessionType::Hive,
        )
        .unwrap();
    let worker = mitsuro_core::storage::HiveWorkerStore::new(Database::new(db_path).unwrap())
        .create(&mitsuro_core::storage::NewHiveWorker {
            model: Some("test:model".into()),
            dm_session_id: Some(dm_session_id.clone()),
            autonomy,
            heartbeat_interval_secs,
            ..mitsuro_core::storage::NewHiveWorker::new(slug)
        })
        .unwrap();
    bind_worker_private_controller(db_path, &worker.id, &dm_session_id);
    (worker.id, dm_session_id)
}

struct PendingAcceptanceLifecycleRace {
    worker_id: String,
    source_run_id: String,
    acceptance_run_id: String,
}

struct RunningWorkflowLifecycleFixture {
    activation: mitsuro_core::workflow::WorkerWorkflowActivation,
    lease_token: String,
}

fn seed_running_workflow_lifecycle_fixture(
    db_path: &std::path::Path,
) -> RunningWorkflowLifecycleFixture {
    const NOW: &str = "2026-08-25T00:00:00.000000Z";
    const WORKSPACE: &str = "/tmp/mitsuro-worker-lifecycle-race";
    let db = Database::new(db_path).unwrap();
    db.conn()
        .execute_batch(&format!(
            r#"
            INSERT INTO sessions (
                id, title, created_at, updated_at, session_type, permission_mode,
                working_dir, project_dir, workspace_mode
            ) VALUES (
                'lifecycle-worker-dm', 'Lifecycle Worker DM', '{NOW}', '{NOW}',
                'hive', 'autonomous', '{WORKSPACE}', '{WORKSPACE}', 'selected'
            );
            INSERT INTO hive_workers (
                id, slug, display_name, model, model_key_json,
                model_catalog_revision, permission_mode, autonomy, status,
                dm_session_id, memory_namespace_id, created_at, updated_at
            ) VALUES (
                'lifecycle-worker', 'lifecycle-worker', 'Lifecycle Worker',
                'test-model',
                '{{"provider":"grok","model_id":"test-model","api_format":"open_ai_responses"}}',
                'catalog-1', 'autonomous', 'manual', 'active',
                'lifecycle-worker-dm', 'lifecycle-worker', '{NOW}', '{NOW}'
            );
            INSERT INTO hive_controllers (
                id, scope_key, session_id, status, timezone,
                max_concurrent_runs, worker_id, created_at, updated_at
            ) VALUES (
                'lifecycle-controller', 'worker:lifecycle-worker',
                'lifecycle-worker-dm', 'active', 'UTC', 1,
                'lifecycle-worker', '{NOW}', '{NOW}'
            );
            INSERT INTO hive_worker_introductions (
                worker_id, run_id, status, prompt_version,
                created_at, updated_at, completed_at
            ) VALUES (
                'lifecycle-worker', NULL, 'confirmed', 1,
                '{NOW}', '{NOW}', '{NOW}'
            );
            INSERT INTO workflow_goals (
                id, session_id, title, objective, constraints_json, status,
                needs_definition, revision, source, created_at, updated_at
            ) VALUES (
                'lifecycle-goal', 'lifecycle-worker-dm', 'Lifecycle Goal',
                'Exercise the post-Progressed lifecycle crash window', '[]',
                'draft', 0, 1, 'user', '{NOW}', '{NOW}'
            );
            INSERT INTO workflow_goal_criteria (
                id, goal_id, position, description, required, status
            ) VALUES (
                'lifecycle-criterion', 'lifecycle-goal', 0,
                'The exact result is accepted', 1, 'pending'
            );
            INSERT INTO workflow_plan_revisions (
                id, goal_id, revision_number, status, title, created_at,
                approved_at
            ) VALUES (
                'lifecycle-plan', 'lifecycle-goal', 1, 'active',
                'Lifecycle plan', '{NOW}', '{NOW}'
            );
            INSERT INTO workflow_plan_steps (
                id, plan_revision_id, display_key, position, description,
                acceptance_criteria_json, required, status, evidence_json,
                revision, created_at
            ) VALUES (
                'lifecycle-step', 'lifecycle-plan', '1', 0,
                'Commit one governed workspace change',
                '["Owner verifies the exact bounded result"]', 1, 'pending',
                '[]', 1, '{NOW}'
            );
            INSERT INTO hive_runtime_state (
                session_id, status, worker_id, updated_at
            ) VALUES (
                'lifecycle-worker-dm', 'idle', 'lifecycle-worker', '{NOW}'
            );
            "#
        ))
        .unwrap();

    let activation_tx =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    let activation = activate_or_resume_worker_workflow_in_transaction(
        &activation_tx,
        &WorkerWorkflowActivationRequest {
            worker_id: "lifecycle-worker".into(),
            expected_worker_revision: 1,
            owner_user_id: None,
            goal_id: "lifecycle-goal".into(),
            expected_goal_revision: 1,
            operation_id: "activate-lifecycle-race".into(),
            source: WorkerWorkflowActivationSource::UserActivation,
            now: chrono::Utc::now(),
        },
    )
    .unwrap();
    activation_tx.commit().unwrap();

    let run_store = HiveRunStore::new(Database::new(db_path).unwrap());
    let claimed = run_store
        .claim_next(&ClaimRunRequest {
            executor_id: "lifecycle-executor".into(),
            lease_epoch: 7,
            now: chrono::Utc::now(),
            lease_duration: Duration::from_secs(600),
            global_concurrency_limit: 1,
        })
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run.id, activation.run_id);
    assert!(run_store
        .mark_running(
            &activation.run_id,
            &claimed.lease_token,
            7,
            chrono::Utc::now(),
        )
        .unwrap());

    RunningWorkflowLifecycleFixture {
        activation,
        lease_token: claimed.lease_token,
    }
}

fn seed_pending_acceptance_lifecycle_race(
    db_path: &std::path::Path,
) -> PendingAcceptanceLifecycleRace {
    const NOW: &str = "2026-08-25T00:00:00.000000Z";
    const WORKSPACE: &str = "/tmp/mitsuro-worker-lifecycle-race";
    let running = seed_running_workflow_lifecycle_fixture(db_path);
    let activation = running.activation;
    let db = Database::new(db_path).unwrap();
    db.conn()
        .execute(
            r#"INSERT INTO hive_worker_provider_calls (
                 provider_call_id, worker_id, worker_revision, owner_user_id,
                 session_id, run_id, run_lease_token, run_lease_epoch,
                 run_lease_expires_at, workflow_goal_id, workflow_attempt_id,
                 origin, lane_key, call_kind, provider_id, model_id,
                 model_key_json, model_key_fingerprint, model_catalog_revision,
                 permission_mode, policy_revision, timezone, local_day,
                 reserved_tokens, started_at
             ) VALUES (
                 'lifecycle-final-call', 'lifecycle-worker', 1, NULL,
                 'lifecycle-worker-dm', ?1, ?2, 7,
                 '2099-08-25T00:10:00.000000Z', 'lifecycle-goal', ?3,
                 'user_workflow_activation', 'dm', 'agent_turn', 'grok',
                 'test-model',
                 '{"provider":"grok","model_id":"test-model","api_format":"open_ai_responses"}',
                 ?4, 'catalog-1', 'autonomous', 1, 'UTC', '2026-08-25',
                 16, ?5
             )"#,
            rusqlite::params![
                activation.run_id,
                running.lease_token,
                activation.workflow_attempt_id,
                "a".repeat(64),
                NOW,
            ],
        )
        .unwrap();
    // The production constructor for a trusted WorkerGoalOutcomeCommitInput is
    // intentionally core-private. This cross-crate handler canary stages the
    // same durable post-commit/pre-permit crash state through the schema's
    // fail-closed binding triggers, without widening that trust boundary.
    let evidence = vec![WorkerGoalEvidence::new(
        WorkerGoalEvidenceKind::WorkspaceMutation,
        "bounded lifecycle-race workspace effect",
    )
    .unwrap()];
    let effect = WorkerGoalEffectSummary::new("one governed workspace change", true).unwrap();
    let counters = WorkerGoalOutcomeCounters {
        provider_calls: 1,
        turns: 1,
        tool_calls: 1,
        successful_tool_calls: 1,
        failed_tool_calls: 0,
        research_actions: 0,
    };
    let source_payload = serde_json::json!({
        "run_id": &activation.run_id,
        "outcome": WorkerGoalAttemptOutcome::Progressed,
        "evidence": &evidence,
        "effect": &effect,
        "counters": counters,
    });
    let source_outcome_sha256 =
        mitsuro_core::storage::hash_request_bytes(serde_json::to_string(&source_payload).unwrap());
    let acceptance_contract = mitsuro_core::storage::WorkerGoalAcceptanceContractV1 {
        schema_version: 1,
        step_specs: vec![mitsuro_core::workflow::WorkflowAcceptanceSpecV1::user_review()],
        goal_specs: vec![mitsuro_core::storage::WorkerGoalCriterionAcceptanceSpecV1 {
            criterion_id: "lifecycle-criterion".into(),
            spec: mitsuro_core::workflow::WorkflowAcceptanceSpecV1::user_review(),
        }],
    };
    let acceptance_contract_json = serde_json::to_string(&acceptance_contract).unwrap();
    let acceptance_contract_sha256 =
        mitsuro_core::storage::hash_request_bytes(&acceptance_contract_json);
    let acceptance_run_id = format!(
        "worker-acceptance-{}",
        mitsuro_core::storage::hash_request_bytes(format!(
            "worker-goal-acceptance-v1:{}",
            activation.run_id
        ))
    );
    let acceptance_goal_revision = activation.goal_revision + 1;
    let acceptance_context = HiveRunExecutionContextV1::worker_goal_acceptance(
        "lifecycle-worker",
        1,
        WorkspaceMode::Selected,
        WORKSPACE,
        WORKSPACE,
        &activation.run_id,
        "lifecycle-goal",
        acceptance_goal_revision,
        acceptance_goal_revision,
        &activation.workflow_attempt_id,
        &activation.plan_revision_id,
        activation.plan_revision_number,
        &activation.step_id,
        activation.step_revision,
        &acceptance_contract_sha256,
        &source_outcome_sha256,
    )
    .unwrap();
    let governor_policy_revision: u64 = db
        .conn()
        .query_row(
            "SELECT revision FROM hive_worker_governor_policies
             WHERE worker_id = 'lifecycle-worker'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stage_tx =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    stage_tx
        .execute(
            "INSERT INTO hive_worker_goal_outcomes (
                 run_id, worker_id, owner_user_id, session_id,
                 workflow_goal_id, workflow_attempt_id, plan_revision_id,
                 step_id, workspace_dir, provider_call_ids_json, outcome,
                 evidence_json, effect_json, counters_json,
                 no_progress_streak, committed_at
             ) VALUES (
                 ?1, 'lifecycle-worker', NULL, 'lifecycle-worker-dm',
                 'lifecycle-goal', ?2, ?3, ?4, ?5,
                 json_array('lifecycle-final-call'), 'progressed', ?6, ?7,
                 ?8, 0, ?9
             )",
            rusqlite::params![
                activation.run_id,
                activation.workflow_attempt_id,
                activation.plan_revision_id,
                activation.step_id,
                WORKSPACE,
                serde_json::to_string(&evidence).unwrap(),
                serde_json::to_string(&effect).unwrap(),
                serde_json::to_string(&counters).unwrap(),
                NOW,
            ],
        )
        .unwrap();
    stage_tx
        .execute(
            "UPDATE workflow_execution_attempts
             SET status = 'paused', stop_reason = 'awaiting_acceptance',
                 progress_revision = progress_revision + 1, updated_at = ?2
             WHERE id = ?1 AND status = 'running'",
            rusqlite::params![activation.workflow_attempt_id, NOW],
        )
        .unwrap();
    stage_tx
        .execute(
            "UPDATE workflow_goals
             SET status = 'active', status_reason = 'awaiting_acceptance',
                 revision = ?2, updated_at = ?3
             WHERE id = 'lifecycle-goal' AND revision = ?1",
            rusqlite::params![activation.goal_revision, acceptance_goal_revision, NOW],
        )
        .unwrap();
    stage_tx
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, priority, concurrency_key, available_at,
                 attempt_count, max_attempts, created_at, updated_at,
                 worker_id, governor_origin, governor_lane_key,
                 governor_policy_revision, execution_context_json,
                 workflow_goal_id, workflow_attempt_id
             ) VALUES (
                 ?1, 'lifecycle-controller', 'lifecycle-worker-dm',
                 'worker_workflow_acceptance', ?2, ?3, 'awaiting_input', 40,
                 'worker:lifecycle-worker', ?4, 0, 1, ?4, ?4,
                 'lifecycle-worker', 'workflow_acceptance', 'dm', ?5, ?6,
                 'lifecycle-goal', ?7
             )",
            rusqlite::params![
                acceptance_run_id,
                format!("Review acceptance for Workflow step {}", activation.step_id),
                serde_json::json!({
                    "worker_id": "lifecycle-worker",
                    "acceptance_mode": "user_review",
                    "source_run_id": &activation.run_id,
                    "workflow_goal_id": "lifecycle-goal",
                    "workflow_attempt_id": &activation.workflow_attempt_id,
                    "automatic_acceptance_enabled": false,
                })
                .to_string(),
                NOW,
                governor_policy_revision,
                serde_json::to_string(&acceptance_context).unwrap(),
                activation.workflow_attempt_id,
            ],
        )
        .unwrap();
    stage_tx
        .execute(
            "INSERT INTO hive_worker_goal_acceptance_candidates (
                 acceptance_run_id, source_run_id, worker_id, worker_revision,
                 owner_user_id, session_id, workflow_goal_id,
                 source_attempt_id, plan_revision_id, plan_revision_number,
                 step_id, goal_revision, workflow_aggregate_revision,
                 step_revision, workspace_dir, acceptance_contract_json,
                 acceptance_contract_sha256, source_outcome_sha256, state,
                 created_at, updated_at
             ) VALUES (
                 ?1, ?2, 'lifecycle-worker', 1, NULL,
                 'lifecycle-worker-dm', 'lifecycle-goal', ?3, ?4, ?5, ?6,
                 ?7, ?7, ?8, ?9, ?10, ?11, ?12, 'awaiting_user', ?13, ?13
             )",
            rusqlite::params![
                acceptance_run_id,
                activation.run_id,
                activation.workflow_attempt_id,
                activation.plan_revision_id,
                activation.plan_revision_number,
                activation.step_id,
                acceptance_goal_revision,
                activation.step_revision,
                WORKSPACE,
                acceptance_contract_json,
                acceptance_contract_sha256,
                source_outcome_sha256,
                NOW,
            ],
        )
        .unwrap();
    stage_tx.commit().unwrap();
    PendingAcceptanceLifecycleRace {
        worker_id: "lifecycle-worker".into(),
        source_run_id: activation.run_id,
        acceptance_run_id,
    }
}

fn apply_worker_lifecycle_for_test(
    db_path: &std::path::Path,
    worker_id: &str,
    expected_revision: u64,
    status: WorkerTargetStatus,
) {
    let db = Database::new(db_path).unwrap();
    let tx =
        rusqlite::Transaction::new_unchecked(db.conn(), rusqlite::TransactionBehavior::Immediate)
            .unwrap();
    super::handler::set_worker_status_for_test(
        &tx,
        &Actor::local("test"),
        &canonical_timestamp(chrono::Utc::now()),
        SetWorkerStatusCommand {
            worker_id: worker_id.into(),
            expected_revision,
            status,
        },
    )
    .unwrap();
    tx.commit().unwrap();
}

fn assert_lifecycle_race_settled(
    db_path: &std::path::Path,
    fixture: &PendingAcceptanceLifecycleRace,
    expected_worker_status: &str,
    expected_goal_status: &str,
    expected_step_status: &str,
) {
    let db = Database::new(db_path).unwrap();
    let state: (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
    ) = db
        .conn()
        .query_row(
            "SELECT source.status, acceptance.status, candidate.state,
                    result.authority, provider.state, provider.remote_acceptance,
                    goal.status, step.status
             FROM hive_runs source
             JOIN hive_worker_goal_acceptance_candidates candidate
               ON candidate.source_run_id = source.id
             JOIN hive_runs acceptance
               ON acceptance.id = candidate.acceptance_run_id
             JOIN hive_worker_goal_acceptance_results result
               ON result.acceptance_run_id = candidate.acceptance_run_id
             JOIN hive_worker_provider_call_outcomes provider
               ON provider.provider_call_id = 'lifecycle-final-call'
             JOIN workflow_goals goal ON goal.id = candidate.workflow_goal_id
             JOIN workflow_plan_steps step ON step.id = candidate.step_id
             WHERE source.id = ?1 AND acceptance.id = ?2",
            rusqlite::params![fixture.source_run_id, fixture.acceptance_run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        state,
        (
            "succeeded".into(),
            "cancelled".into(),
            "stale".into(),
            "lifecycle".into(),
            "completed".into(),
            "acknowledged".into(),
            expected_goal_status.into(),
            expected_step_status.into(),
        )
    );
    let (worker_status, recovery_runs, unresolved_provider_calls): (String, i64, i64) = db
        .conn()
        .query_row(
            "SELECT worker.status,
                    (SELECT COUNT(*) FROM hive_runs
                     WHERE worker_id = worker.id AND status = 'recovery_required'),
                    (SELECT COUNT(*) FROM hive_worker_provider_calls call
                     LEFT JOIN hive_worker_provider_call_outcomes terminal
                       ON terminal.provider_call_id = call.provider_call_id
                     WHERE call.run_id = ?2 AND terminal.provider_call_id IS NULL)
             FROM hive_workers worker WHERE worker.id = ?1",
            rusqlite::params![fixture.worker_id, fixture.source_run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(worker_status, expected_worker_status);
    assert_eq!(recovery_runs, 0);
    assert_eq!(unresolved_provider_calls, 0);
}

fn assert_paused_acceptance_race_preserved(
    db_path: &std::path::Path,
    fixture: &PendingAcceptanceLifecycleRace,
    expected_worker_status: &str,
    expected_controller_status: &str,
) {
    let db = Database::new(db_path).unwrap();
    let state: (
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
    ) = db
        .conn()
        .query_row(
            "SELECT source.status, acceptance.status, candidate.state,
                    provider.state, provider.remote_acceptance,
                    goal.status, goal.status_reason, step.status,
                    attempt.status, attempt.stop_reason,
                    controller.status,
                    (SELECT COUNT(*)
                     FROM hive_worker_goal_acceptance_results result
                     WHERE result.acceptance_run_id = candidate.acceptance_run_id)
             FROM hive_runs source
             JOIN hive_worker_goal_acceptance_candidates candidate
               ON candidate.source_run_id = source.id
             JOIN hive_runs acceptance
               ON acceptance.id = candidate.acceptance_run_id
             JOIN hive_worker_provider_call_outcomes provider
               ON provider.provider_call_id = 'lifecycle-final-call'
             JOIN workflow_goals goal ON goal.id = candidate.workflow_goal_id
             JOIN workflow_plan_steps step ON step.id = candidate.step_id
             JOIN workflow_execution_attempts attempt
               ON attempt.id = candidate.source_attempt_id
             JOIN hive_controllers controller
               ON controller.id = source.controller_id
             WHERE source.id = ?1 AND acceptance.id = ?2",
            rusqlite::params![fixture.source_run_id, fixture.acceptance_run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        state,
        (
            "succeeded".into(),
            "awaiting_input".into(),
            "awaiting_user".into(),
            "completed".into(),
            "acknowledged".into(),
            "active".into(),
            "awaiting_acceptance".into(),
            "in_progress".into(),
            "paused".into(),
            "awaiting_acceptance".into(),
            expected_controller_status.into(),
            0,
        )
    );
    let (worker_status, recovery_runs, unresolved_provider_calls): (String, i64, i64) = db
        .conn()
        .query_row(
            "SELECT worker.status,
                    (SELECT COUNT(*) FROM hive_runs
                     WHERE worker_id = worker.id AND status = 'recovery_required'),
                    (SELECT COUNT(*) FROM hive_worker_provider_calls call
                     LEFT JOIN hive_worker_provider_call_outcomes terminal
                       ON terminal.provider_call_id = call.provider_call_id
                     WHERE call.run_id = ?2 AND terminal.provider_call_id IS NULL)
             FROM hive_workers worker WHERE worker.id = ?1",
            rusqlite::params![fixture.worker_id, fixture.source_run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(worker_status, expected_worker_status);
    assert_eq!(recovery_runs, 0);
    assert_eq!(unresolved_provider_calls, 0);
}

fn seed_preserved_lifecycle_run(
    db_path: &std::path::Path,
    run_status: &str,
    introduction: bool,
) -> (String, DaemonFence) {
    let db = Database::new(db_path).unwrap();
    let now = canonical_timestamp(chrono::Utc::now());
    let context = HiveRunExecutionContextV1::worker_conversation_neutral(
        "preserved-worker",
        1,
        WorkerConversationLane::DirectMessage,
    )
    .unwrap();
    let run_kind = if introduction {
        "worker_introduction"
    } else {
        "worker_heartbeat"
    };
    let origin = if introduction {
        WorkerRunOrigin::UserLifecycleAction.as_str()
    } else {
        WorkerRunOrigin::Heartbeat.as_str()
    };
    let initial_status = if matches!(run_status, "leased" | "running") {
        "queued"
    } else {
        run_status
    };
    let wake_at = (run_status == "sleeping").then_some("2099-08-25T00:10:00.000000Z");
    let available_at = if run_status == "retry_wait" {
        "2099-08-25T00:10:00.000000Z"
    } else {
        now.as_str()
    };
    db.conn()
        .execute_batch(&format!(
            r#"
            INSERT INTO sessions (
                id, title, created_at, updated_at, session_type,
                permission_mode, workspace_mode
            ) VALUES (
                'preserved-worker-dm', 'Preserved Worker DM', '{now}', '{now}',
                'hive', 'autonomous', 'neutral'
            );
            INSERT INTO hive_workers (
                id, slug, display_name, model, permission_mode, autonomy,
                status, dm_session_id, memory_namespace_id, created_at, updated_at
            ) VALUES (
                'preserved-worker', 'preserved-worker', 'Preserved Worker',
                'test-model', 'autonomous', 'manual', 'active',
                'preserved-worker-dm', 'preserved-worker', '{now}', '{now}'
            );
            INSERT INTO hive_controllers (
                id, scope_key, session_id, status, timezone,
                max_concurrent_runs, worker_id, created_at, updated_at
            ) VALUES (
                'preserved-controller', 'worker:preserved-worker',
                'preserved-worker-dm', 'active', 'UTC', 1,
                'preserved-worker', '{now}', '{now}'
            );
            "#
        ))
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, priority, concurrency_key, available_at, wake_at,
                 attempt_count, max_attempts, created_at, updated_at,
                 worker_id, governor_origin, governor_lane_key,
                 execution_context_json
             ) VALUES (
                 'preserved-run', 'preserved-controller',
                 'preserved-worker-dm', ?1, ?2, ?3, ?4, 0,
                 'preserved-worker-run', ?5, ?6, ?7, 3, ?8, ?8,
                 'preserved-worker', ?9, 'dm', ?10
             )",
            rusqlite::params![
                run_kind,
                if introduction {
                    "Begin the one-time Worker Introduction"
                } else {
                    "Continue bounded autonomous work"
                },
                serde_json::json!({
                    "worker_id": "preserved-worker",
                    "model": "test-model",
                    "permission_mode": "autonomous",
                })
                .to_string(),
                initial_status,
                available_at,
                wake_at,
                if matches!(run_status, "sleeping" | "retry_wait") {
                    1
                } else {
                    0
                },
                now,
                origin,
                serde_json::to_string(&context).unwrap(),
            ],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version,
                 created_at, updated_at, completed_at
             ) VALUES (
                 'preserved-worker', ?1, ?2, 1, ?3, ?3, ?4
             )",
            rusqlite::params![
                introduction.then_some("preserved-run"),
                if introduction { "queued" } else { "confirmed" },
                now,
                (!introduction).then_some(now.as_str()),
            ],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_daemon_leases (
                 lease_name, owner_id, fencing_token, acquired_at,
                 heartbeat_at, expires_at
             ) VALUES (
                 'preserved-lifecycle', 'preserved-executor', 11, ?1, ?1,
                 '2099-08-25T00:10:00.000000Z'
             )",
            [&now],
        )
        .unwrap();
    let fence = DaemonFence {
        lease_name: "preserved-lifecycle".into(),
        owner_id: "preserved-executor".into(),
        fencing_token: 11,
    };
    if matches!(run_status, "leased" | "running") {
        let claimed = HiveRunStore::new(Database::new(db_path).unwrap())
            .claim_next(&ClaimRunRequest {
                executor_id: fence.owner_id.clone(),
                lease_epoch: fence.fencing_token,
                now: chrono::Utc::now(),
                lease_duration: Duration::from_secs(600),
                global_concurrency_limit: 1,
            })
            .unwrap()
            .unwrap();
        assert_eq!(claimed.run.id, "preserved-run");
        if run_status == "running" {
            assert!(HiveRunStore::new(Database::new(db_path).unwrap())
                .mark_running(
                    &claimed.run.id,
                    &claimed.lease_token,
                    fence.fencing_token,
                    chrono::Utc::now(),
                )
                .unwrap());
        }
    }
    ("preserved-worker".into(), fence)
}

#[test]
fn pause_resume_preserves_pre_provider_runs_with_exact_execution_revision() {
    let temp = TempDir::new().unwrap();
    for (name, initial_status, introduction) in [
        ("initial-greeting", "queued", true),
        ("leased-initial-greeting", "leased", true),
        ("queued", "queued", false),
        ("leased", "leased", false),
        ("sleeping", "sleeping", false),
        ("retry-wait", "retry_wait", false),
        ("awaiting-input", "awaiting_input", false),
    ] {
        let db_path = temp.path().join(format!("preserved-{name}.db"));
        let (worker_id, fence) =
            seed_preserved_lifecycle_run(&db_path, initial_status, introduction);
        apply_worker_lifecycle_for_test(&db_path, &worker_id, 1, WorkerTargetStatus::Paused);
        let paused = Database::new(&db_path).unwrap();
        let (worker_revision, persisted_status, context_revision): (u64, String, u64) = paused
            .conn()
            .query_row(
                "SELECT worker.revision, run.status,
                        json_extract(run.execution_context_json,
                                     '$.mode.worker_revision')
                 FROM hive_workers worker
                 JOIN hive_runs run ON run.worker_id = worker.id
                 WHERE worker.id = ?1 AND run.id = 'preserved-run'",
                [&worker_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(worker_revision, 1, "{name}");
        assert_eq!(context_revision, 1, "{name}");
        assert_eq!(
            persisted_status,
            if initial_status == "leased" {
                "queued"
            } else {
                initial_status
            },
            "{name}"
        );
        drop(paused);

        apply_worker_lifecycle_for_test(&db_path, &worker_id, 1, WorkerTargetStatus::Active);
        let resumed = Database::new(&db_path).unwrap();
        let resumed_revision: u64 = resumed
            .conn()
            .query_row(
                "SELECT revision FROM hive_workers WHERE id = ?1",
                [&worker_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(resumed_revision, 1, "{name}");
        if matches!(initial_status, "sleeping" | "retry_wait" | "awaiting_input") {
            resumed
                .conn()
                .execute(
                    "UPDATE hive_runs
                     SET status = 'queued', available_at = ?2, wake_at = NULL
                     WHERE id = 'preserved-run' AND status = ?1",
                    rusqlite::params![initial_status, canonical_timestamp(chrono::Utc::now())],
                )
                .unwrap();
        }
        drop(resumed);

        let run_store = HiveRunStore::new(Database::new(&db_path).unwrap());
        let claimed = run_store
            .claim_next(&ClaimRunRequest {
                executor_id: fence.owner_id.clone(),
                lease_epoch: fence.fencing_token,
                now: chrono::Utc::now(),
                lease_duration: Duration::from_secs(600),
                global_concurrency_limit: 1,
            })
            .unwrap()
            .unwrap();
        assert_eq!(claimed.run.id, "preserved-run", "{name}");
        assert!(run_store
            .mark_running(
                &claimed.run.id,
                &claimed.lease_token,
                fence.fencing_token,
                chrono::Utc::now(),
            )
            .unwrap());
        assert!(
            run_store
                .validate_claimed_execution_fenced(&claimed, &fence, chrono::Utc::now())
                .unwrap(),
            "{name}"
        );
    }
}

#[test]
fn pause_fails_closed_for_running_introduction_before_provider() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("running-introduction-before-provider.db");
    let (worker_id, _) = seed_preserved_lifecycle_run(&db_path, "running", true);
    apply_worker_lifecycle_for_test(&db_path, &worker_id, 1, WorkerTargetStatus::Paused);

    let db = Database::new(&db_path).unwrap();
    let state: (String, String, String, u64, u64, i64) = db
        .conn()
        .query_row(
            "SELECT run.status, introduction.status, worker.status,
                    worker.revision,
                    json_extract(run.execution_context_json,
                                 '$.mode.worker_revision'),
                    (SELECT COUNT(*) FROM hive_worker_provider_calls call
                     WHERE call.run_id = run.id)
             FROM hive_runs run
             JOIN hive_workers worker ON worker.id = run.worker_id
             JOIN hive_worker_introductions introduction
               ON introduction.run_id = run.id
             WHERE run.id = 'preserved-run'",
            [],
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
        .unwrap();
    assert_eq!(
        state,
        (
            "recovery_required".into(),
            "needs_recovery".into(),
            "paused".into(),
            1,
            1,
            0,
        )
    );
}

#[test]
fn pausing_after_progress_commit_adopts_source_and_preserves_acceptance() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("pause-lifecycle-race.db");
    let fixture = seed_pending_acceptance_lifecycle_race(&db_path);
    apply_worker_lifecycle_for_test(&db_path, &fixture.worker_id, 1, WorkerTargetStatus::Paused);
    assert_paused_acceptance_race_preserved(&db_path, &fixture, "paused", "paused");

    apply_worker_lifecycle_for_test(&db_path, &fixture.worker_id, 1, WorkerTargetStatus::Active);
    assert_paused_acceptance_race_preserved(&db_path, &fixture, "active", "active");
    let run_store = HiveRunStore::new(Database::new(&db_path).unwrap());
    assert!(run_store
        .claim_next(&ClaimRunRequest {
            executor_id: "post-resume-probe".into(),
            lease_epoch: 8,
            now: chrono::Utc::now(),
            lease_duration: Duration::from_secs(60),
            global_concurrency_limit: 1,
        })
        .unwrap()
        .is_none());
}

#[test]
fn archiving_after_progress_commit_adopts_source_before_invalidating_acceptance() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("archive-lifecycle-race.db");
    let fixture = seed_pending_acceptance_lifecycle_race(&db_path);
    apply_worker_lifecycle_for_test(
        &db_path,
        &fixture.worker_id,
        1,
        WorkerTargetStatus::Archived,
    );
    assert_lifecycle_race_settled(&db_path, &fixture, "archived", "cancelled", "cancelled");
    assert!(HiveRunStore::new(Database::new(&db_path).unwrap())
        .claim_next(&ClaimRunRequest {
            executor_id: "post-archive-probe".into(),
            lease_epoch: 8,
            now: chrono::Utc::now(),
            lease_duration: Duration::from_secs(60),
            global_concurrency_limit: 1,
        })
        .unwrap()
        .is_none());
}

fn seed_provider_aware_worker_with_dm(
    db_path: &std::path::Path,
    slug: &str,
) -> (
    String,
    String,
    mitsuro_core::ai::models::ModelKey,
    &'static str,
) {
    seed_provider_aware_worker_with_dm_options(
        db_path,
        slug,
        mitsuro_core::storage::HiveWorkerAutonomy::Manual,
        None,
    )
}

fn seed_provider_aware_worker_with_dm_options(
    db_path: &std::path::Path,
    slug: &str,
    autonomy: mitsuro_core::storage::HiveWorkerAutonomy,
    heartbeat_interval_secs: Option<u32>,
) -> (
    String,
    String,
    mitsuro_core::ai::models::ModelKey,
    &'static str,
) {
    let model_key = test_worker_model_key();
    let model_catalog_revision = "worker-catalog-7";
    let session_manager = SessionManager::new(Database::new(db_path).unwrap());
    let dm_session_id = session_manager
        .create_session_for_user_with_config(
            &format!("{slug} DM"),
            Some("test:model"),
            None,
            None,
            WorkspaceMode::Neutral,
            None,
            None,
            SessionType::Hive,
        )
        .unwrap();
    freeze_test_session_model(db_path, &dm_session_id, &model_key, model_catalog_revision);
    let worker = mitsuro_core::storage::HiveWorkerStore::new(Database::new(db_path).unwrap())
        .create(&mitsuro_core::storage::NewHiveWorker {
            model: Some("test:model".into()),
            model_key: Some(model_key.clone()),
            model_catalog_revision: Some(model_catalog_revision.into()),
            autonomy,
            heartbeat_interval_secs,
            dm_session_id: Some(dm_session_id.clone()),
            ..mitsuro_core::storage::NewHiveWorker::new(slug)
        })
        .unwrap();
    bind_worker_private_controller(db_path, &worker.id, &dm_session_id);
    (worker.id, dm_session_id, model_key, model_catalog_revision)
}

fn seed_worker_governor_recovery_call(
    db_path: &std::path::Path,
    slug: &str,
    acknowledged_response_loss: bool,
) -> (String, String) {
    let (worker_id, session_id, model_key, model_catalog_revision) =
        seed_provider_aware_worker_with_dm(db_path, slug);
    let db = Database::new(db_path).unwrap();
    let controller_id = db
        .conn()
        .query_row(
            "SELECT id FROM hive_controllers
             WHERE worker_id = ?1 AND session_id = ?2",
            params![worker_id, session_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    let accepted_at = Utc::now() - chrono::Duration::minutes(20);
    let tx =
        rusqlite::Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let accepted = accept_worker_conversation_input_in_transaction(
        &tx,
        &AcceptWorkerConversationInput {
            input_id: format!("{slug}-uncertain-input"),
            request_id: format!("{slug}-uncertain-request"),
            worker_id: worker_id.clone(),
            owner_user_id: None,
            session_id: session_id.clone(),
            controller_id: controller_id.clone(),
            body: "Start the uncertain provider turn".into(),
            accepted_at,
            new_run_id: format!("{slug}-uncertain-run"),
            run_config: serde_json::json!({
                "model": "test:model",
                "model_key": model_key,
                "model_catalog_revision": model_catalog_revision,
                "permission_mode": "autonomous",
                "working_dir": null,
                "project_dir": null,
            }),
            execution_context: HiveRunExecutionContextV1::worker_conversation_neutral(
                &worker_id,
                1,
                WorkerConversationLane::DirectMessage,
            )
            .unwrap(),
            priority: 10,
            concurrency_key: Some(format!("worker-dm:{worker_id}")),
            max_attempts: 2,
        },
    )
    .unwrap();
    let run_id = match accepted {
        AcceptWorkerConversationInputResult::Queued { run_id, .. } => run_id,
        AcceptWorkerConversationInputResult::Staged { .. } => {
            panic!("fresh Worker DM must queue")
        }
    };
    let started_at = canonical_timestamp(accepted_at);
    let lease_expires_at = canonical_timestamp(accepted_at + chrono::Duration::minutes(10));
    tx.execute(
        "UPDATE hive_runs
         SET status = 'running', lease_owner = 'crashed-executor',
             lease_token = 'uncertain-lease', lease_epoch = 7,
             lease_expires_at = ?2, started_at = ?3, updated_at = ?3
         WHERE id = ?1 AND status = 'queued'",
        params![run_id, lease_expires_at, started_at],
    )
    .unwrap();
    let (model_id, model_key_json, permission_mode, policy_revision, timezone): (
        String,
        String,
        String,
        i64,
        String,
    ) = tx
        .query_row(
            "SELECT worker.model, worker.model_key_json, worker.permission_mode,
                    policy.revision, policy.timezone
             FROM hive_workers worker
             JOIN hive_worker_governor_policies policy
               ON policy.worker_id = worker.id
             WHERE worker.id = ?1",
            [&worker_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    tx.execute(
        "INSERT INTO hive_worker_provider_calls (
             provider_call_id, worker_id, worker_revision, owner_user_id,
             session_id, run_id, run_lease_token, run_lease_epoch,
             run_lease_expires_at, origin, lane_key, call_kind, provider_id,
             model_id, model_key_json, model_key_fingerprint,
             model_catalog_revision, permission_mode, policy_revision,
             timezone, local_day, reserved_tokens, started_at
         ) VALUES (
             ?1, ?2, 1, NULL, ?3, ?4, 'uncertain-lease', 7,
             ?5, 'user_dm', 'dm', 'agent_turn', 'openai',
             ?6, ?7, ?8, ?9, ?10, ?11, ?12,
             strftime('%Y-%m-%d', ?13), 1, ?13
         )",
        params![
            format!("{slug}-uncertain-call"),
            worker_id,
            session_id,
            run_id,
            lease_expires_at,
            model_id,
            model_key_json,
            "a".repeat(64),
            model_catalog_revision,
            permission_mode,
            policy_revision,
            timezone,
            started_at,
        ],
    )
    .unwrap();
    if acknowledged_response_loss {
        tx.execute(
            "INSERT INTO hive_worker_provider_call_outcomes (
                 provider_call_id, state, outcome, remote_acceptance,
                 finished_at
             ) VALUES (?1, 'completed', 'completed', 'acknowledged', ?2)",
            params![
                format!("{slug}-uncertain-call"),
                canonical_timestamp(Utc::now() - chrono::Duration::minutes(19)),
            ],
        )
        .unwrap();
        tx.execute(
            "UPDATE hive_runs
             SET status = 'recovery_required', lease_owner = NULL,
                 lease_token = NULL, lease_epoch = NULL,
                 lease_expires_at = NULL, heartbeat_at = NULL, updated_at = ?2
             WHERE id = ?1 AND status = 'running'",
            params![run_id, canonical_timestamp(Utc::now())],
        )
        .unwrap();
        tx.execute(
            "UPDATE hive_controllers SET status = 'paused', updated_at = ?2
             WHERE id = ?1",
            params![controller_id, canonical_timestamp(Utc::now())],
        )
        .unwrap();
    } else {
        tx.execute(
            "INSERT INTO hive_worker_provider_call_outcomes (
                 provider_call_id, state, outcome, remote_acceptance,
                 unknown_reason, finished_at
             ) VALUES (?1, 'unknown', 'transport_uncertain', 'possibly_sent', ?2, ?3)",
            params![
                format!("{slug}-uncertain-call"),
                "provider response was not observed",
                canonical_timestamp(Utc::now() - chrono::Duration::minutes(19)),
            ],
        )
        .unwrap();
        tx.execute(
            "UPDATE hive_runs
             SET status = 'cancelled', lease_owner = NULL, lease_token = NULL,
                 lease_epoch = NULL, lease_expires_at = NULL,
                 finished_at = ?2, updated_at = ?2
             WHERE id = ?1 AND status = 'running'",
            params![run_id, canonical_timestamp(Utc::now())],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    (worker_id, session_id)
}

#[tokio::test]
async fn worker_governor_recovery_is_owner_bound_replay_safe_and_conflict_typed() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let (worker_id, _) = seed_worker_governor_recovery_call(
        &runtime_config.database_path,
        "governor-recovery",
        false,
    );
    let (response_loss_worker_id, _) = seed_worker_governor_recovery_call(
        &runtime_config.database_path,
        "governor-response-loss",
        true,
    );
    let (clean_worker_id, _, _, _) =
        seed_provider_aware_worker_with_dm(&runtime_config.database_path, "governor-clean");
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-governor-recovery",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    let command = Command::GrantWorkerGovernorRecovery(GrantWorkerGovernorRecoveryCommand {
        worker_id: worker_id.clone(),
    });
    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "grant-governor-recovery"),
        command.clone(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "grant-governor-recovery"),
        command.clone(),
    )
    .await;
    assert_eq!(first, replay);
    let recovery = match first {
        ResponsePayload::WorkerGovernorRecovery(response) => response,
        other => panic!("expected Worker governor recovery response, got {other:?}"),
    };
    assert_eq!(recovery.worker_id, worker_id);
    assert_eq!(recovery.status, "granted");
    assert!(recovery.bypass_unresolved_provider_call);
    assert!(recovery.grant_id.is_some());
    assert_eq!(
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_worker_governor_override_grants
                 WHERE worker_id = ?1",
                [&worker_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );

    let foreign = handler
        .handle(
            context(
                Actor {
                    user_id: Some("alice".into()),
                    client_kind: "test".into(),
                },
                "foreign-governor-recovery",
            ),
            command,
        )
        .await
        .unwrap_err();
    assert_eq!(foreign.code, "not_found");
    let stale = handler
        .handle(
            context(Actor::local("test"), "stale-governor-recovery"),
            Command::GrantWorkerGovernorRecovery(GrantWorkerGovernorRecoveryCommand {
                worker_id: clean_worker_id,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, "state_conflict");
    assert!(stale.message.contains("no owner-acknowledgeable"));

    let response_loss = response(
        handler.as_ref(),
        context(Actor::local("test"), "acknowledge-governor-response-loss"),
        Command::GrantWorkerGovernorRecovery(GrantWorkerGovernorRecoveryCommand {
            worker_id: response_loss_worker_id.clone(),
        }),
    )
    .await;
    let ResponsePayload::WorkerGovernorRecovery(response_loss) = response_loss else {
        panic!("expected Worker response-loss recovery response")
    };
    assert_eq!(response_loss.status, "response_loss_acknowledged");
    assert!(response_loss.grant_id.is_none());
    assert!(response_loss.expires_at.is_none());
    assert!(!response_loss.bypass_unresolved_provider_call);
    let response_loss_state: (String, i64) = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT run.status,
                    (SELECT COUNT(*)
                     FROM hive_worker_governor_override_grants grant_row
                     WHERE grant_row.worker_id = ?1)
             FROM hive_runs run
             WHERE run.worker_id = ?1 AND run.kind = 'worker_conversation'",
            [&response_loss_worker_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(response_loss_state, ("cancelled".to_string(), 0));
    runtime.shutdown().await;
}

#[tokio::test]
async fn attached_worker_workspace_is_replay_stable_and_frozen_into_next_dm_run() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let workspace = temp.path().join("worker-workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let workspace = workspace
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let runtime_config = config(&temp);
    let (worker_id, dm_session_id, _, _) =
        seed_provider_aware_worker_with_dm(&runtime_config.database_path, "workspace-bound");
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-worker-workspace",
        Arc::new(CanonicalWorkerBackend::new(
            runtime_config.database_path.clone(),
        )),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    let command = Command::SetWorkerWorkspace(SetWorkerWorkspaceCommand {
        worker_id: worker_id.clone(),
        expected_worker_revision: 1,
        workspace_mode: WorkerWorkspaceMode::Selected,
        working_dir: Some(workspace.clone()),
        project_dir: Some(workspace.clone()),
    });
    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "worker-workspace-attach"),
        command.clone(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "worker-workspace-attach"),
        command,
    )
    .await;
    assert_eq!(first, replay);
    let ResponsePayload::WorkerWorkspace(attached) = first else {
        panic!("expected Worker workspace response")
    };
    assert_eq!(attached.revision, 2);
    assert_eq!(attached.workspace_mode, WorkerWorkspaceMode::Selected);
    assert_eq!(attached.working_dir.as_deref(), Some(workspace.as_str()));

    let accepted = response(
        handler.as_ref(),
        context(Actor::local("test"), "worker-workspace-message"),
        Command::WorkerSendMessage(MessageCommand {
            session_id: dm_session_id.clone(),
            message: "Keep this conversation tool-free while the Goal workspace stays attached."
                .into(),
        }),
    )
    .await;
    assert!(matches!(
        accepted,
        ResponsePayload::WorkerConversationInput(_)
    ));
    let (mode, frozen_working, frozen_project): (String, String, String) =
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT
                     json_extract(execution_context_json, '$.mode.kind'),
                     json_extract(execution_context_json, '$.mode.working_dir'),
                     json_extract(execution_context_json, '$.mode.project_dir')
                 FROM hive_runs
                 WHERE worker_id = ?1 AND session_id = ?2
                   AND kind = 'worker_conversation'
                 ORDER BY created_at DESC LIMIT 1",
                rusqlite::params![worker_id, dm_session_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
    assert_eq!(mode, "worker_workspace_attached");
    assert_eq!(frozen_working, workspace);
    assert_eq!(frozen_project, frozen_working);
    runtime.shutdown().await;
}

#[test]
fn worker_workflow_terminal_path_reconciles_before_deterministic_rollover() {
    let source = include_str!("pump.rs");
    let finish_start = source
        .find("async fn reconcile_finished_worker_workflow")
        .expect("Worker Workflow terminal reconciler");
    let finish_end = source[finish_start..]
        .find("async fn cancellation_committed_for_claim")
        .map(|offset| finish_start + offset)
        .expect("bounded terminal reconciler region");
    let terminal = &source[finish_start..finish_end];
    let reconcile = terminal
        .find(".reconcile_worker_workflow_run")
        .expect("provider usage reconciliation");
    let succeeded = terminal
        .find("status == HiveRunStatus::Succeeded")
        .expect("succeeded-only rollover gate");
    let finalize = terminal
        .find(".finalize_worker_workflow_attempt")
        .expect("core rollover facade");
    assert!(reconcile < succeeded && succeeded < finalize);
    assert!(terminal.contains("reconciliation.goal_status == \"active\""));
    assert!(terminal.contains("!reconciliation.recovery_required"));
    assert!(terminal.contains("worker-workflow-rollover:{run_id}"));

    let tick = source
        .find("if let Err(error) = materialize_due_worker_workflow_rollovers(")
        .expect("periodic/startup crash recovery hook");
    let claim_loop = source
        .find("match claim_next(&shared, token).await")
        .expect("claim loop");
    assert!(tick < claim_loop);
    assert!(source.contains("MAX_DUE_WORKER_WORKFLOW_ROLLOVERS_PER_TICK"));
    assert!(source.contains("worker_workflow_rollover_queued"));
}

#[test]
fn every_direct_worker_wake_uses_the_persisted_workspace_binding() {
    for (name, source) in [
        ("direct input", include_str!("handler.rs")),
        ("peer delivery", include_str!("deliveries.rs")),
        ("heartbeat", include_str!("heartbeat.rs")),
        ("schedule", include_str!("pump.rs")),
    ] {
        assert!(
            source.contains("resolve_worker_conversation_execution_binding"),
            "{name} did not freeze the persisted Worker workspace"
        );
    }
    for (name, source) in [
        ("peer delivery", include_str!("deliveries.rs")),
        ("heartbeat", include_str!("heartbeat.rs")),
    ] {
        assert!(
            !source.contains("worker_conversation_neutral("),
            "{name} still hard-codes a neutral DM context"
        );
    }
    let pump = include_str!("pump.rs");
    assert_eq!(
        pump.matches("JOIN hive_runs r ON r.id = o.run_id").count(),
        1,
        "durable control query must have one exact run join"
    );
}

#[test]
fn worker_workflow_cancellation_is_exact_and_replayed_after_commit() {
    let pump = include_str!("pump.rs");
    let start = pump
        .find("fn cancellation_matches_claim")
        .expect("cancellation matcher");
    let end = pump[start..]
        .find("fn configured_worker_id")
        .map(|offset| start + offset)
        .expect("bounded cancellation matcher region");
    let matcher = &pump[start..end];
    for exact in [
        "claim.run.id == *run_id",
        "claim.run.worker_id.as_deref() == Some(worker_id.as_str())",
        "claim.run.workflow_goal_id.as_deref() == Some(goal_id.as_str())",
        "claim.run.kind == HiveRunKind::WorkerWorkflow",
    ] {
        assert!(matcher.contains(exact), "missing exact fence: {exact}");
    }

    let handler = include_str!("handler.rs");
    let replay_delivery = handler
        .find("for cancellation in worker_workflow_cancellations")
        .expect("Workflow cancellation broadcast");
    let non_replay_controls = handler[replay_delivery..]
        .find("if !outcome.replayed")
        .map(|offset| replay_delivery + offset)
        .expect("non-idempotent generic control gate");
    assert!(replay_delivery < non_replay_controls);
}

#[tokio::test]
async fn worker_profile_update_is_revisioned_owner_scoped_and_replay_stable() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let (worker_id, dm_session_id, worker_model_key, worker_catalog_revision) =
        seed_provider_aware_worker_with_dm(&runtime_config.database_path, "revisioned-profile");
    let protocol_model_key: ModelKey =
        serde_json::from_value(serde_json::to_value(worker_model_key).unwrap()).unwrap();
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-worker-profile-update",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    wait_for_runtime_health(handler.as_ref(), true, true).await;
    let command = Command::UpdateWorker(UpdateWorkerCommand {
        worker_id: worker_id.clone(),
        expected_revision: 1,
        display_name: "Revisioned Profile".into(),
        avatar_color: Some("#7743DB".into()),
        model: Some("test:model".into()),
        model_key: Some(protocol_model_key.clone()),
        model_catalog_revision: Some(worker_catalog_revision.into()),
        permission_mode: "supervised".into(),
        autonomy: "manual".into(),
        heartbeat_interval_secs: None,
        identity: Some("Own exact runtime reliability work.".into()),
        soul: Some("Calm, curious, and precise.".into()),
    });

    let foreign_error = handler
        .handle(
            context(
                Actor {
                    user_id: Some("other-user".into()),
                    client_kind: "test".into(),
                },
                "worker-profile-foreign",
            ),
            command.clone(),
        )
        .await
        .expect_err("a foreign owner must not mutate a Worker");
    assert!(matches!(
        foreign_error.code.as_str(),
        "not_found" | "ownership_denied" | "ownership_mismatch"
    ));

    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "worker-profile-update"),
        command.clone(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "worker-profile-update"),
        command,
    )
    .await;
    assert_eq!(first, replay);
    let ResponsePayload::WorkerMutation(updated) = first else {
        panic!("expected Worker mutation response");
    };
    assert_eq!(updated.worker_id, worker_id);
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.status, "active");

    let db = Database::new(&runtime_config.database_path).unwrap();
    let worker = mitsuro_core::storage::HiveWorkerStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    )
    .get(&worker_id)
    .unwrap()
    .unwrap();
    assert_eq!(worker.display_name, "Revisioned Profile");
    assert_eq!(worker.revision, 2);
    let session_title: String = db
        .conn()
        .query_row(
            "SELECT title FROM sessions WHERE id = ?1",
            [&dm_session_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(session_title, "Revisioned Profile");
    let documents: Vec<(String, String)> = {
        let mut statement = db
            .conn()
            .prepare(
                "SELECT kind, content FROM hive_worker_documents
                 WHERE worker_id = ?1 ORDER BY kind",
            )
            .unwrap();
        statement
            .query_map([&worker_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert_eq!(documents.len(), 2);

    let stale_error = handler
        .handle(
            context(Actor::local("test"), "worker-profile-stale"),
            Command::UpdateWorker(UpdateWorkerCommand {
                worker_id: worker_id.clone(),
                expected_revision: 1,
                display_name: "Stale overwrite".into(),
                avatar_color: None,
                model: Some("test:model".into()),
                model_key: Some(protocol_model_key),
                model_catalog_revision: Some(worker_catalog_revision.into()),
                permission_mode: "supervised".into(),
                autonomy: "manual".into(),
                heartbeat_interval_secs: None,
                identity: None,
                soul: None,
            }),
        )
        .await
        .expect_err("a stale Worker revision must conflict");
    assert_eq!(stale_error.code, "revision_conflict");
    runtime.shutdown().await;
}

fn worker_schedule_definition(
    worker_id: &str,
    model_key: Option<ModelKey>,
    model_catalog_revision: Option<&str>,
) -> ScheduleDefinition {
    ScheduleDefinition {
        title: "Worker provider identity".into(),
        summary: "Verify exact Worker routing".into(),
        objective: "Run using only the targeted Worker's exact provider identity".into(),
        recurrence: serde_json::to_value(RecurrenceV1::Once {
            at: chrono::Utc::now() + chrono::Duration::days(1),
        })
        .unwrap(),
        timezone: "UTC".into(),
        dst_policy: serde_json::to_value(DstPolicy::default()).unwrap(),
        priority: 0,
        project_dir: None,
        model: model_key.as_ref().map(|key| key.model_id.clone()),
        model_key,
        model_catalog_revision: model_catalog_revision.map(str::to_string),
        crew_slug: None,
        worker_id: Some(worker_id.to_string()),
        group_id: None,
        misfire: serde_json::to_value(MisfireConfig::default()).unwrap(),
        overlap_policy: "queue_one".into(),
        retry: serde_json::to_value(RetryPolicy::default()).unwrap(),
    }
}

async fn create_worker_targeted_schedule(
    handler: &dyn CommandHandler,
    session_id: &str,
    worker_id: &str,
    model_key: Option<ModelKey>,
    model_catalog_revision: Option<&str>,
    idempotency_key: &str,
) -> String {
    match response(
        handler,
        context(Actor::local("test"), idempotency_key),
        Command::CreateSchedule(CreateScheduleCommand {
            session_id: session_id.to_string(),
            definition: worker_schedule_definition(worker_id, model_key, model_catalog_revision),
        }),
    )
    .await
    {
        ResponsePayload::Schedule(response) => response.schedule_id,
        other => panic!("expected schedule response, got {other:?}"),
    }
}

async fn current_daemon_fence(db_path: &std::path::Path) -> DaemonFence {
    wait_for(|| {
        HiveDaemonLeaseStore::new(Database::new(db_path).unwrap())
            .get("hive-scheduler")
            .unwrap()
            .is_some()
    })
    .await;
    let lease = HiveDaemonLeaseStore::new(Database::new(db_path).unwrap())
        .get("hive-scheduler")
        .unwrap()
        .unwrap();
    DaemonFence {
        lease_name: lease.lease_name,
        owner_id: lease.owner_id,
        fencing_token: lease.fencing_token,
    }
}

fn materialize_worker_schedule(db_path: &std::path::Path, schedule_id: &str, fence: DaemonFence) {
    let schedule = HiveScheduleStore::new(Database::new(db_path).unwrap())
        .get_schedule(schedule_id)
        .unwrap()
        .unwrap();
    let scheduled_for = chrono::Utc::now();
    materialize_schedule_transaction(
        db_path.to_path_buf(),
        schedule,
        MisfireResolution {
            enqueue: vec![MisfireDispatch {
                scheduled_for,
                coalesced_count: 0,
            }],
            skipped: Vec::new(),
        },
        scheduled_for,
        None,
        fence,
    )
    .unwrap();
}

#[tokio::test]
async fn worker_schedule_without_identity_inherits_exact_worker_provider_identity() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-worker-schedule-inherit",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "worker-schedule-inherit-parent"),
            dispatch_command(),
        )
        .await,
    );
    let (worker_id, dm_session_id, worker_key, worker_revision) =
        seed_provider_aware_worker_with_dm(&runtime_config.database_path, "schedule-inherit");
    let schedule_id = create_worker_targeted_schedule(
        handler.as_ref(),
        &session_id,
        &worker_id,
        None,
        None,
        "worker-schedule-inherit-create",
    )
    .await;
    let omitted = HiveScheduleStore::new(Database::new(&runtime_config.database_path).unwrap())
        .get_schedule(&schedule_id)
        .unwrap()
        .unwrap();
    assert!(omitted.model.is_none());
    assert!(omitted.model_key.is_none());
    assert!(omitted.model_catalog_revision.is_none());

    let replaced = response(
        handler.as_ref(),
        context(Actor::local("test"), "worker-schedule-inherit-replace"),
        Command::ReplaceSchedule(ReplaceScheduleCommand {
            session_id: session_id.clone(),
            schedule_id: schedule_id.clone(),
            expected_revision: 0,
            definition: worker_schedule_definition(&worker_id, None, None),
        }),
    )
    .await;
    assert!(matches!(
        replaced,
        ResponsePayload::Schedule(ref response) if response.revision == 1
    ));
    let replaced = HiveScheduleStore::new(Database::new(&runtime_config.database_path).unwrap())
        .get_schedule(&schedule_id)
        .unwrap()
        .unwrap();
    assert!(replaced.model.is_none());
    assert!(replaced.model_key.is_none());
    assert!(replaced.model_catalog_revision.is_none());

    let fence = current_daemon_fence(&runtime_config.database_path).await;
    materialize_worker_schedule(&runtime_config.database_path, &schedule_id, fence);

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (run_session_id, config_json): (String, String) = db
        .conn()
        .query_row(
            "SELECT session_id, config_json FROM hive_runs WHERE schedule_id = ?1",
            [&schedule_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let config: serde_json::Value = serde_json::from_str(&config_json).unwrap();
    assert_eq!(run_session_id, dm_session_id);
    assert_eq!(config["model"], "test:model");
    assert_eq!(
        config["model_key"],
        serde_json::to_value(&worker_key).unwrap()
    );
    assert_eq!(config["model_catalog_revision"], worker_revision);
    assert_eq!(config["worker_id"], worker_id);
    runtime.shutdown().await;
}

#[tokio::test]
async fn worker_schedule_accepts_only_an_exact_explicit_provider_identity() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-worker-schedule-exact",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "worker-schedule-exact-parent"),
            dispatch_command(),
        )
        .await,
    );
    let (worker_id, dm_session_id, worker_key, worker_revision) =
        seed_provider_aware_worker_with_dm(&runtime_config.database_path, "schedule-exact");
    let explicit_key: ModelKey =
        serde_json::from_value(serde_json::to_value(&worker_key).unwrap()).unwrap();
    let schedule_id = create_worker_targeted_schedule(
        handler.as_ref(),
        &session_id,
        &worker_id,
        Some(explicit_key),
        Some(worker_revision),
        "worker-schedule-exact-create",
    )
    .await;

    let fence = current_daemon_fence(&runtime_config.database_path).await;
    materialize_worker_schedule(&runtime_config.database_path, &schedule_id, fence);

    let db = Database::new(&runtime_config.database_path).unwrap();
    let (run_session_id, config_json): (String, String) = db
        .conn()
        .query_row(
            "SELECT session_id, config_json FROM hive_runs WHERE schedule_id = ?1",
            [&schedule_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let config: serde_json::Value = serde_json::from_str(&config_json).unwrap();
    assert_eq!(run_session_id, dm_session_id);
    assert_eq!(config["model"], "test:model");
    assert_eq!(
        config["model_key"],
        serde_json::to_value(&worker_key).unwrap()
    );
    assert_eq!(config["model_catalog_revision"], worker_revision);
    assert_eq!(config["worker_id"], worker_id);
    runtime.shutdown().await;
}

#[tokio::test]
async fn worker_schedule_requires_completed_introduction_at_create_and_materialization() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-worker-schedule-introduction",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "worker-schedule-introduction-parent"),
            dispatch_command(),
        )
        .await,
    );
    let (worker_id, _, _, _) =
        seed_provider_aware_worker_with_dm(&runtime_config.database_path, "schedule-introduction");
    let now = canonical_timestamp(Utc::now());
    let db = Database::new(&runtime_config.database_path).unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version, created_at, updated_at
             ) VALUES (?1, NULL, 'awaiting_context', 1, ?2, ?2)",
            params![worker_id, now],
        )
        .unwrap();
    let create_error = handler
        .handle(
            context(
                Actor::local("test"),
                "worker-schedule-introduction-create-rejected",
            ),
            Command::CreateSchedule(CreateScheduleCommand {
                session_id: session_id.clone(),
                definition: worker_schedule_definition(&worker_id, None, None),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(create_error.code, "invalid_command");
    assert!(create_error.message.contains("Introduction"));

    db.conn()
        .execute(
            "DELETE FROM hive_worker_introductions WHERE worker_id = ?1",
            [&worker_id],
        )
        .unwrap();
    let schedule_id = create_worker_targeted_schedule(
        handler.as_ref(),
        &session_id,
        &worker_id,
        None,
        None,
        "worker-schedule-introduction-legacy-create",
    )
    .await;
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version, created_at, updated_at
             ) VALUES (?1, NULL, 'queued', 1, ?2, ?2)",
            params![worker_id, now],
        )
        .unwrap();

    let fence = current_daemon_fence(&runtime_config.database_path).await;
    materialize_worker_schedule(&runtime_config.database_path, &schedule_id, fence);
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE schedule_id = ?1",
                [&schedule_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let (status, reason): (String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT status, decision_reason FROM hive_schedule_occurrences
             WHERE schedule_id = ?1",
            [&schedule_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "skipped");
    assert_eq!(
        reason.as_deref(),
        Some("targeted Worker has not completed or skipped its Introduction")
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn worker_schedule_rejects_provider_mismatch_without_fallback() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-worker-schedule-mismatch",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "worker-schedule-mismatch-parent"),
            dispatch_command(),
        )
        .await,
    );
    let (worker_id, _, worker_key, worker_revision) =
        seed_provider_aware_worker_with_dm(&runtime_config.database_path, "schedule-mismatch");
    let mismatched_key = mitsuro_core::ai::models::ModelKey::new(
        ProviderId::Grok,
        "test:model",
        mitsuro_core::ai::models::ApiFormat::OpenAIResponses,
    );
    let mismatched_protocol_key: ModelKey =
        serde_json::from_value(serde_json::to_value(&mismatched_key).unwrap()).unwrap();
    let create_error = handler
        .handle(
            context(
                Actor::local("test"),
                "worker-schedule-mismatch-create-rejected",
            ),
            Command::CreateSchedule(CreateScheduleCommand {
                session_id: session_id.clone(),
                definition: worker_schedule_definition(
                    &worker_id,
                    Some(mismatched_protocol_key),
                    Some(worker_revision),
                ),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(create_error.code, "invalid_command");

    let explicit_key: ModelKey =
        serde_json::from_value(serde_json::to_value(&worker_key).unwrap()).unwrap();
    let schedule_id = create_worker_targeted_schedule(
        handler.as_ref(),
        &session_id,
        &worker_id,
        Some(explicit_key),
        Some(worker_revision),
        "worker-schedule-mismatch-create",
    )
    .await;
    Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_schedules SET model_key_json = ?2 WHERE id = ?1",
            rusqlite::params![schedule_id, serde_json::to_string(&mismatched_key).unwrap()],
        )
        .unwrap();

    let fence = current_daemon_fence(&runtime_config.database_path).await;
    materialize_worker_schedule(&runtime_config.database_path, &schedule_id, fence);

    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE schedule_id = ?1",
                [&schedule_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "a provider mismatch must not fall back to the schedule or parent session"
    );
    let (status, reason): (String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT status, decision_reason FROM hive_schedule_occurrences
             WHERE schedule_id = ?1",
            [&schedule_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "skipped");
    assert_eq!(
        reason.as_deref(),
        Some("schedule model identity does not match targeted Worker")
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn worker_schedule_rejects_worker_without_exact_provider_key() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-worker-schedule-legacy-key",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "worker-schedule-legacy-key-parent"),
            dispatch_command(),
        )
        .await,
    );
    let (worker_id, _) = seed_worker_with_dm(
        &runtime_config.database_path,
        "schedule-legacy-key",
        mitsuro_core::storage::HiveWorkerAutonomy::Manual,
        None,
    );
    let schedule_id = create_worker_targeted_schedule(
        handler.as_ref(),
        &session_id,
        &worker_id,
        None,
        None,
        "worker-schedule-legacy-key-create",
    )
    .await;

    let fence = current_daemon_fence(&runtime_config.database_path).await;
    materialize_worker_schedule(&runtime_config.database_path, &schedule_id, fence);

    let db = Database::new(&runtime_config.database_path).unwrap();
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT COUNT(*) FROM hive_runs WHERE schedule_id = ?1",
                [&schedule_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let reason = db
        .conn()
        .query_row(
            "SELECT decision_reason FROM hive_schedule_occurrences
             WHERE schedule_id = ?1",
            [&schedule_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap();
    assert_eq!(
        reason.as_deref(),
        Some("targeted Worker has no exact provider model identity")
    );
    runtime.shutdown().await;
}

fn count_worker_heartbeat_runs(db_path: &std::path::Path, worker_id: &str) -> i64 {
    Database::new(db_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_runs
             WHERE worker_id = ?1 AND kind = 'worker_heartbeat'",
            [worker_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn count_finished_worker_heartbeats(db_path: &std::path::Path, worker_id: &str) -> i64 {
    Database::new(db_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_runs
             WHERE worker_id = ?1 AND kind = 'worker_heartbeat' AND finished_at IS NOT NULL",
            [worker_id],
            |row| row.get(0),
        )
        .unwrap()
}

#[tokio::test]
async fn create_schedule_rejects_archived_worker_target() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "archived-schedule-parent"),
            dispatch_command(),
        )
        .await,
    );
    let worker_store = mitsuro_core::storage::HiveWorkerStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    );
    let worker = worker_store
        .create(&mitsuro_core::storage::NewHiveWorker::new("archived-ops"))
        .unwrap();
    assert!(worker_store
        .set_status(
            &worker.id,
            mitsuro_core::storage::HiveWorkerStatus::Archived
        )
        .unwrap());

    let error = handler
        .handle(
            context(Actor::local("test"), "archived-worker-schedule"),
            Command::CreateSchedule(CreateScheduleCommand {
                session_id,
                definition: ScheduleDefinition {
                    title: "Should fail".into(),
                    summary: "Archived target".into(),
                    objective: "Must not bind an archived Worker".into(),
                    recurrence: serde_json::to_value(RecurrenceV1::Once {
                        at: chrono::Utc::now() + chrono::Duration::days(1),
                    })
                    .unwrap(),
                    timezone: "UTC".into(),
                    dst_policy: serde_json::to_value(DstPolicy::default()).unwrap(),
                    priority: 0,
                    project_dir: None,
                    model: None,
                    model_key: None,
                    model_catalog_revision: None,
                    crew_slug: None,
                    worker_id: Some(worker.id),
                    group_id: None,
                    misfire: serde_json::to_value(MisfireConfig::default()).unwrap(),
                    overlap_policy: "queue_one".into(),
                    retry: serde_json::to_value(RetryPolicy::default()).unwrap(),
                },
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "invalid_command");
    runtime.shutdown().await;
}

#[tokio::test]
async fn create_schedule_rejects_archived_group_and_worker_group_together() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "group-schedule-parent"),
            dispatch_command(),
        )
        .await,
    );
    let fixture = seed_group(
        &runtime_config.database_path,
        &["ops"],
        mitsuro_core::storage::HiveGroupExecutionMode::Workbench,
        1,
    );
    let worker_id = fixture.members[0].0.clone();
    assert!(mitsuro_core::storage::HiveGroupStore::new(
        Database::new(&runtime_config.database_path).unwrap()
    )
    .set_status(
        &fixture.group_id,
        mitsuro_core::storage::HiveGroupStatus::Archived
    )
    .unwrap());

    let mut definition = ScheduleDefinition {
        title: "Should fail".into(),
        summary: "Archived target".into(),
        objective: "Must not bind an archived Group".into(),
        recurrence: serde_json::to_value(RecurrenceV1::Once {
            at: chrono::Utc::now() + chrono::Duration::days(1),
        })
        .unwrap(),
        timezone: "UTC".into(),
        dst_policy: serde_json::to_value(DstPolicy::default()).unwrap(),
        priority: 0,
        project_dir: None,
        model: None,
        model_key: None,
        model_catalog_revision: None,
        crew_slug: None,
        worker_id: None,
        group_id: Some(fixture.group_id.clone()),
        misfire: serde_json::to_value(MisfireConfig::default()).unwrap(),
        overlap_policy: "queue_one".into(),
        retry: serde_json::to_value(RetryPolicy::default()).unwrap(),
    };
    let archived = handler
        .handle(
            context(Actor::local("test"), "archived-group-schedule"),
            Command::CreateSchedule(CreateScheduleCommand {
                session_id: session_id.clone(),
                definition: definition.clone(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(archived.code, "invalid_command");

    definition.group_id = Some(fixture.group_id);
    definition.worker_id = Some(worker_id);
    let both = handler
        .handle(
            context(Actor::local("test"), "worker-and-group-schedule"),
            Command::CreateSchedule(CreateScheduleCommand {
                session_id,
                definition,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(both.code, "invalid_command");
    runtime.shutdown().await;
}

#[tokio::test]
async fn group_targeted_schedule_materializes_a_group_turn() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(CanonicalWorkerBackend::new(
            runtime_config.database_path.clone(),
        )) as Arc<dyn ExecutionBackend>,
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    let session_id = dispatch_session_id(
        &response(
            handler.as_ref(),
            context(Actor::local("test"), "group-schedule-dispatch"),
            dispatch_command(),
        )
        .await,
    );
    let fixture = seed_group(
        &runtime_config.database_path,
        &["alpha", "beta"],
        mitsuro_core::storage::HiveGroupExecutionMode::Workbench,
        1,
    );
    let created = response(
        handler.as_ref(),
        context(Actor::local("test"), "create-group-schedule"),
        Command::CreateSchedule(CreateScheduleCommand {
            session_id,
            definition: ScheduleDefinition {
                title: "Group standup".into(),
                summary: "Wake the room".into(),
                objective: "Scheduled group standup".into(),
                recurrence: serde_json::to_value(RecurrenceV1::Once {
                    at: chrono::Utc::now() + chrono::Duration::days(1),
                })
                .unwrap(),
                timezone: "UTC".into(),
                dst_policy: serde_json::to_value(DstPolicy::default()).unwrap(),
                priority: 0,
                project_dir: None,
                model: None,
                model_key: None,
                model_catalog_revision: None,
                crew_slug: None,
                worker_id: None,
                group_id: Some(fixture.group_id.clone()),
                misfire: serde_json::to_value(MisfireConfig::default()).unwrap(),
                overlap_policy: "queue_one".into(),
                retry: serde_json::to_value(RetryPolicy::default()).unwrap(),
            },
        }),
    )
    .await;
    let schedule_id = match created {
        ResponsePayload::Schedule(response) => response.schedule_id,
        other => panic!("expected schedule response, got {other:?}"),
    };
    wait_for(|| {
        HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
            .get("hive-scheduler")
            .unwrap()
            .is_some()
    })
    .await;
    let lease = HiveDaemonLeaseStore::new(Database::new(&runtime_config.database_path).unwrap())
        .get("hive-scheduler")
        .unwrap()
        .unwrap();
    let schedule = HiveScheduleStore::new(Database::new(&runtime_config.database_path).unwrap())
        .get_schedule(&schedule_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        schedule.group_id.as_deref(),
        Some(fixture.group_id.as_str())
    );
    let scheduled_for = chrono::Utc::now();
    materialize_schedule_transaction(
        runtime_config.database_path.clone(),
        schedule,
        MisfireResolution {
            enqueue: vec![MisfireDispatch {
                scheduled_for,
                coalesced_count: 0,
            }],
            skipped: Vec::new(),
        },
        scheduled_for,
        None,
        DaemonFence {
            lease_name: lease.lease_name,
            owner_id: lease.owner_id,
            fencing_token: lease.fencing_token,
        },
    )
    .unwrap();

    let turns = mitsuro_core::storage::HiveGroupStore::new(
        Database::new(&runtime_config.database_path).unwrap(),
    )
    .list_turns(&fixture.group_id, 5)
    .unwrap();
    assert_eq!(turns.len(), 1);
    let scheduled_runs: i64 = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_runs
             WHERE schedule_id = ?1 AND kind = 'group_turn'
               AND worker_id IS NOT NULL
               AND governor_origin = 'scheduled_group'
               AND governor_lane_key = ?2
               AND json_extract(execution_context_json, '$.mode.kind')
                   = 'worker_conversation_neutral'
               AND json_extract(execution_context_json, '$.mode.lane.kind') = 'group'
               AND json_extract(execution_context_json, '$.mode.lane.group_id') = ?3",
            rusqlite::params![
                schedule_id,
                format!("group:{}", fixture.group_id),
                fixture.group_id,
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(scheduled_runs, 2);

    let db_path = runtime_config.database_path.clone();
    let group_id = fixture.group_id.clone();
    wait_for(move || {
        let runs = group_turn_member_runs(&db_path, &group_id);
        runs.len() == 2 && runs.iter().all(|(_, status)| status == "succeeded")
    })
    .await;
    let idle_rows: i64 = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT COUNT(*)
             FROM hive_worker_idle_state idle
             JOIN hive_runs run ON run.id = idle.last_outcome_run_id
             WHERE run.schedule_id = ?1 AND run.governor_origin = 'scheduled_group'",
            [&schedule_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        idle_rows, 0,
        "a successful scheduled group response is not typed material or idle evidence"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn pausing_running_worker_replays_exact_direct_run_cancellation() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let (worker_id, dm_session_id, _, _) = seed_provider_aware_worker_with_dm_options(
        &runtime_config.database_path,
        "pause-direct",
        mitsuro_core::storage::HiveWorkerAutonomy::AlwaysOn,
        Some(1),
    );
    let backend = Arc::new(BlockingBackend::default());
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-pause-direct",
        backend.clone(),
    )
    .await
    .unwrap();
    let handler = runtime.handler();
    wait_for(|| backend.executions.load(Ordering::SeqCst) == 1).await;
    let run_id: String = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT id FROM hive_runs
             WHERE worker_id = ?1 AND session_id = ?2 AND status = 'running'",
            rusqlite::params![worker_id, dm_session_id],
            |row| row.get(0),
        )
        .unwrap();
    let command = Command::SetWorkerStatus(SetWorkerStatusCommand {
        worker_id: worker_id.clone(),
        expected_revision: 1,
        status: WorkerTargetStatus::Paused,
    });
    let first = response(
        handler.as_ref(),
        context(Actor::local("test"), "pause-direct-running"),
        command.clone(),
    )
    .await;
    let replay = response(
        handler.as_ref(),
        context(Actor::local("test"), "pause-direct-running"),
        command,
    )
    .await;
    assert_eq!(first, replay);
    let ResponsePayload::WorkerMutation(paused) = first else {
        panic!("expected Worker mutation response");
    };
    assert_eq!(paused.status, "paused");
    assert_eq!(paused.revision, 1);
    assert_eq!(paused.cancellation_requests.len(), 1);
    assert_eq!(paused.cancellation_requests[0].run_id, run_id);
    assert_eq!(paused.cancellation_requests[0].session_id, dm_session_id);

    wait_for(|| {
        backend
            .controls
            .lock()
            .unwrap()
            .iter()
            .any(|(session, control)| {
                session == &dm_session_id
                    && matches!(
                        control,
                        ExecutionControl::CancelRun { run_id: cancelled, .. }
                            if cancelled == &run_id
                    )
            })
    })
    .await;
    assert!(backend.controls.lock().unwrap().iter().all(|(_, control)| {
        !matches!(
            control,
            ExecutionControl::CancelRun { run_id: cancelled, .. } if cancelled != &run_id
        )
    }));
    let (run_status, worker_status, controller_status): (String, String, String) =
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT run.status, worker.status, controller.status
                 FROM hive_runs run
                 JOIN hive_workers worker ON worker.id = run.worker_id
                 JOIN hive_controllers controller ON controller.id = run.controller_id
                 WHERE run.id = ?1",
                [&run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
    assert_eq!(run_status, "recovery_required");
    assert_eq!(worker_status, "paused");
    assert_eq!(controller_status, "paused");
    runtime.shutdown().await;
}

#[tokio::test]
async fn always_on_worker_receives_heartbeat_and_stops_when_paused() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let (worker_id, dm_session_id, _, _) = seed_provider_aware_worker_with_dm_options(
        &runtime_config.database_path,
        "pulse",
        mitsuro_core::storage::HiveWorkerAutonomy::AlwaysOn,
        Some(1),
    );
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(CanonicalWorkerBackend::new(
            runtime_config.database_path.clone(),
        )),
    )
    .await
    .unwrap();

    let db_path = runtime_config.database_path.clone();
    let waiting_id = worker_id.clone();
    wait_for(move || count_finished_worker_heartbeats(&db_path, &waiting_id) >= 1).await;
    let (run_id, finished_at): (String, String) = Database::new(&runtime_config.database_path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT id, finished_at FROM hive_runs
                 WHERE worker_id = ?1 AND kind = 'worker_heartbeat'
                   AND status = 'succeeded'
                 ORDER BY finished_at ASC, id ASC LIMIT 1",
            [&worker_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let (idle_streak, not_before, last_outcome_run_id): (i64, String, String) =
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT idle_streak, not_before, last_outcome_run_id
                 FROM hive_worker_idle_state
                 WHERE worker_id = ?1 AND lane_key = 'dm'",
                [&worker_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
    assert_eq!(idle_streak, 1);
    assert_eq!(last_outcome_run_id, run_id);
    assert_eq!(
        mitsuro_core::hive::parse_utc_timestamp(&not_before).unwrap()
            - mitsuro_core::hive::parse_utc_timestamp(&finished_at).unwrap(),
        chrono::Duration::seconds(900),
        "the first structurally idle heartbeat must arm the configured base backoff"
    );
    assert_eq!(
        Database::new(&runtime_config.database_path)
            .unwrap()
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE session_id = ?1 AND role = 'user'",
                [&dm_session_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
        "an automated heartbeat must not manufacture a user-authored turn"
    );

    assert!(mitsuro_core::storage::HiveWorkerStore::new(
        Database::new(&runtime_config.database_path).unwrap()
    )
    .set_status(&worker_id, mitsuro_core::storage::HiveWorkerStatus::Paused)
    .unwrap());

    let paused_count = count_worker_heartbeat_runs(&runtime_config.database_path, &worker_id);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        count_worker_heartbeat_runs(&runtime_config.database_path, &worker_id),
        paused_count,
        "paused AlwaysOn workers must not receive further heartbeats"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn manual_worker_does_not_receive_heartbeat() {
    let _test_guard = runtime_test_guard().await;
    let temp = TempDir::new().unwrap();
    let runtime_config = config(&temp);
    let (worker_id, _) = seed_worker_with_dm(
        &runtime_config.database_path,
        "manual",
        mitsuro_core::storage::HiveWorkerAutonomy::Manual,
        None,
    );
    let runtime = start_runtime(
        runtime_config.clone(),
        "daemon-a",
        Arc::new(FakeBackend::default()),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        count_worker_heartbeat_runs(&runtime_config.database_path, &worker_id),
        0
    );
    runtime.shutdown().await;
}
