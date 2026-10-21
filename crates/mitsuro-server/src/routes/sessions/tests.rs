use axum::http::HeaderMap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::Utc;
use tokio::sync::{Mutex, RwLock};

use mitsuro_core::agent::loop_events::{LoopEvent, LoopStopReason};
use mitsuro_core::agent::{
    effective_context_window_for_runtime, AgentCancellation,
    DelegatedRunStage as CoreDelegatedRunStage, LoopInput, UserHookManager,
};
use mitsuro_core::ai::models::{
    create_model_registry, ApiFormat, ModelCatalogSource, ModelKey, ModelMetadata,
};
use mitsuro_core::ai::providers::ProviderId;
use mitsuro_core::mcp::McpManager;
use mitsuro_core::plan::{PlanFile, PlanManager};
use mitsuro_core::process::ProcessRegistry;
use mitsuro_core::skills::SkillsManager;
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::storage::{
    Database, DelegatedRunAgentSnapshot, DelegatedRunRole, DelegatedRunScope, DelegatedRunSnapshot,
    DelegatedRunStartInput, DelegatedRunStore, HiveGroupStore, HiveGroupWorkerLaneStore,
    HiveWorkerStore, NewHiveGroup, NewHiveGroupWorkerLane, NewHiveWorker, PartialAssistantState,
    PendingInteractionSnapshot, PendingPlanTaskSnapshot, RecoveryDecision,
    RecoveryNonResumableReason, RecoveryStatus, RecoveryToolCall, RuntimeTraceEvent,
    RuntimeTraceStore, SessionRecoveryState, SessionType, WorkspaceMode,
};
use mitsuro_core::tools::registry::ToolRegistry;
use mitsuro_core::workflow::{
    CreateGoalInput, CriterionInput, PlanProposalInput, StepProposalInput, WorkflowManager,
};

use super::crud::{GetSessionQuery, ListSessionsQuery};
use super::workflow::{execute_workflow_command, get_workflow, WorkflowCommand};
use super::*;
use crate::routes::test_support::current_user;
use crate::types::{
    CreateSessionRequest, PinchRequest, SessionPresenceHeartbeatRequest, UpdateSessionRequest,
};
use crate::AppState;

fn create_test_state() -> (AppState, PathBuf) {
    let temp_dir =
        std::env::temp_dir().join(format!("mitsuro-server-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
    let db_path = temp_dir.join("krusty.db");
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
            agent_streams: Arc::new(RwLock::new(crate::agent_streams::AgentStreamHub::default())),
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
            hive_runtime: crate::hive_runtime::HiveRuntimeManager::new(),
        },
        temp_dir,
    )
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

async fn code_control_headers(state: &AppState, session_id: &str) -> HeaderMap {
    let lease = state
        .session_control_leases
        .write()
        .await
        .take(session_id, "server-test-controller".to_owned(), false)
        .expect("test controller should acquire the exact Code session lease");
    let mut headers = HeaderMap::new();
    headers.insert(
        crate::control_leases::CONTROL_LEASE_HEADER,
        lease.lease_token.parse().expect("lease token header value"),
    );
    headers
}

fn create_owned_code_session(state: &AppState, user_id: &str, title: &str) -> String {
    SessionManager::new(Database::new(&state.db_path).expect("database should open"))
        .create_session_for_user_with_config(
            title,
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some(user_id),
            None,
            SessionType::Code,
        )
        .expect("Code session should create")
}

#[tokio::test]
async fn code_agent_control_is_exact_owner_scoped_and_public_summary_has_no_token() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let session_id = create_owned_code_session(&state, "alice", "Controlled Code Agent");

    let denied = get_session_control(
        State(state.clone()),
        Some(current_user("bob", state.working_dir.as_ref())),
        Path(session_id.clone()),
    )
    .await;
    assert!(matches!(denied, Err(AppError::NotFound(_))));

    let Json(taken) = take_session_control(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(crate::types::TakeSessionControlRequest {
            client_id: "canvas-a".to_owned(),
            force: false,
        }),
    )
    .await
    .expect("owner should take control");
    assert_eq!(taken.lease.client_id, "canvas-a");

    let Json(summary) = get_session_control(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        Path(session_id),
    )
    .await
    .expect("owner should read summary");
    assert_eq!(summary.ttl_seconds, 15);
    assert_eq!(
        summary
            .controller
            .as_ref()
            .map(|value| value.client_id.as_str()),
        Some("canvas-a")
    );
    let json = serde_json::to_value(summary).expect("summary should serialize");
    assert!(json.get("lease_token").is_none());
    assert!(json["controller"].get("lease_token").is_none());
}

#[tokio::test]
async fn code_agent_observer_stream_is_exact_owner_scoped() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let session_id = create_owned_code_session(&state, "alice", "Observed Code Agent");

    let denied = observe_agent_stream(
        State(state.clone()),
        Some(current_user("bob", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Query(super::stream::ObserveAgentStreamQuery::default()),
    )
    .await;
    assert!(matches!(denied, Err(AppError::NotFound(_))));

    let stream = observe_agent_stream(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id),
        Query(super::stream::ObserveAgentStreamQuery::default()),
    )
    .await
    .expect("owner can attach an observer");
    drop(stream);
}

#[tokio::test]
async fn code_agent_observer_cannot_mutate_and_explicit_transfer_invalidates_old_token() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_id = create_owned_code_session(&state, "alice", "Lease Transfer");
    let Json(first) = take_session_control(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(crate::types::TakeSessionControlRequest {
            client_id: "canvas-a".to_owned(),
            force: false,
        }),
    )
    .await
    .expect("first claim");

    let denied = take_session_control(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(crate::types::TakeSessionControlRequest {
            client_id: "canvas-b".to_owned(),
            force: false,
        }),
    )
    .await;
    assert!(matches!(denied, Err(AppError::Conflict(_))));

    let Json(second) = take_session_control(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(crate::types::TakeSessionControlRequest {
            client_id: "canvas-b".to_owned(),
            force: true,
        }),
    )
    .await
    .expect("explicit transfer");

    let old = state
        .session_control_leases
        .write()
        .await
        .authorize_mutation(&session_id, Some(&first.lease.lease_token));
    assert!(matches!(old, Err(AppError::Conflict(_))));
    state
        .session_control_leases
        .write()
        .await
        .authorize_mutation(&session_id, Some(&second.lease.lease_token))
        .expect("replacement token should authorize");

    let Json(heartbeat) = heartbeat_session_control(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id),
        Json(crate::types::HeartbeatSessionControlRequest {
            client_id: "canvas-b".to_owned(),
            lease_token: second.lease.lease_token,
        }),
    )
    .await
    .expect("current controller heartbeat");
    assert_eq!(heartbeat.ttl_seconds, 15);
}

#[tokio::test]
async fn workflow_routes_require_ownership_and_explicit_activation() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Workflow contract",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("session should create");
    session_manager
        .update_session_work_mode(&session_id, mitsuro_core::storage::WorkMode::Plan)
        .expect("test should enter Plan mode");

    let denied = get_workflow(
        State(state.clone()),
        Some(current_user("bob", state.working_dir.as_ref())),
        Path(session_id.clone()),
    )
    .await;
    assert!(matches!(denied, Err(AppError::NotFound(_))));

    let Json(created) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::CreateGoal {
            operation_id: "route-create".into(),
            goal: CreateGoalInput {
                title: "Reliable workflow".into(),
                objective: "Prove explicit workflow transitions".into(),
                constraints: vec!["Never elevate permissions".into()],
                criteria: vec![CriterionInput {
                    description: "Lifecycle is persisted".into(),
                    required: true,
                }],
                token_budget: None,
            },
        }),
    )
    .await
    .expect("goal should create");
    assert_eq!(
        session_manager
            .get_session(&session_id)
            .expect("session should load")
            .expect("session should exist")
            .work_mode,
        mitsuro_core::storage::WorkMode::Plan,
        "creating a draft must not activate work"
    );

    let Json(proposed) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::ProposePlan {
            operation_id: "route-propose".into(),
            goal_id: created.snapshot.goal.id.clone(),
            expected_revision: created.snapshot.aggregate_revision,
            plan: PlanProposalInput {
                title: "One step".into(),
                rationale: None,
                source_message_id: None,
                predecessor_id: None,
                legacy_markdown: None,
                steps: vec![StepProposalInput {
                    display_key: "1".into(),
                    description: "Verify the contract".into(),
                    context: None,
                    parent_display_key: None,
                    dependencies: vec![],
                    acceptance_criteria: vec!["State is durable".into()],
                    required: true,
                }],
            },
        }),
    )
    .await
    .expect("plan should propose");
    assert_eq!(
        proposed.snapshot.goal.status,
        mitsuro_core::workflow::GoalStatus::Draft
    );

    let plan_id = proposed
        .snapshot
        .plan_revision
        .as_ref()
        .expect("proposal should exist")
        .id
        .clone();
    let Json(approved) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::ApprovePlan {
            operation_id: "route-approve".into(),
            goal_id: proposed.snapshot.goal.id.clone(),
            plan_revision_id: plan_id,
            expected_revision: proposed.snapshot.aggregate_revision,
        }),
    )
    .await
    .expect("plan should approve");
    assert_eq!(
        approved.snapshot.goal.status,
        mitsuro_core::workflow::GoalStatus::Draft
    );

    // Model an already-running planning loop. Activation must wake that loop
    // through the control plane instead of requiring a synthetic chat message.
    let active_guard = state
        .try_lock_session(&session_id)
        .await
        .expect("test should hold the session run lock");
    let (workflow_tx, mut workflow_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), workflow_tx);

    let Json(activated) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::ActivateGoal {
            operation_id: "route-activate".into(),
            goal_id: approved.snapshot.goal.id.clone(),
            expected_revision: approved.snapshot.aggregate_revision,
        }),
    )
    .await
    .expect("goal should activate");
    assert_eq!(
        activated.snapshot.goal.status,
        mitsuro_core::workflow::GoalStatus::Active
    );
    assert!(matches!(
        workflow_rx.recv().await,
        Some(LoopInput::WorkflowUpdated {
            goal_id,
            aggregate_revision,
            operation_id,
        }) if goal_id == activated.snapshot.goal.id
            && aggregate_revision == activated.snapshot.aggregate_revision
            && operation_id == "route-activate"
    ));
    drop(active_guard);
    assert_eq!(
        session_manager
            .get_session(&session_id)
            .expect("session should load")
            .expect("session should exist")
            .work_mode,
        mitsuro_core::storage::WorkMode::Build
    );

    let Json(snapshot) = get_workflow(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
    )
    .await
    .expect("workflow should load");
    assert_eq!(
        snapshot.expect("workflow should exist").aggregate_revision,
        activated.snapshot.aggregate_revision
    );

    let Json(paused) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::PauseGoal {
            operation_id: "route-pause".into(),
            goal_id: activated.snapshot.goal.id.clone(),
            expected_revision: activated.snapshot.aggregate_revision,
            reason: "User paused execution".into(),
        }),
    )
    .await
    .expect("goal should pause");
    assert_eq!(
        paused.snapshot.goal.status,
        mitsuro_core::workflow::GoalStatus::Paused
    );
    assert!(
        matches!(workflow_rx.try_recv(), Ok(LoopInput::WorkflowUpdated { operation_id, .. }) if operation_id == "route-pause")
    );
    assert!(
        matches!(workflow_rx.try_recv(), Ok(LoopInput::Cancel)),
        "pause must interrupt an in-flight provider"
    );
    assert!(workflow_rx.try_recv().is_err());

    let Json(resumed) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::ResumeGoal {
            operation_id: "route-resume".into(),
            goal_id: paused.snapshot.goal.id.clone(),
            expected_revision: paused.snapshot.aggregate_revision,
        }),
    )
    .await
    .expect("goal should resume");
    assert!(
        matches!(workflow_rx.try_recv(), Ok(LoopInput::WorkflowUpdated { operation_id, .. }) if operation_id == "route-resume")
    );
    let Json(replayed_pause) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::PauseGoal {
            operation_id: "route-pause".into(),
            goal_id: activated.snapshot.goal.id.clone(),
            expected_revision: activated.snapshot.aggregate_revision,
            reason: "User paused execution".into(),
        }),
    )
    .await
    .expect("pause retry should replay its original receipt");
    assert_eq!(
        replayed_pause.snapshot.aggregate_revision,
        paused.snapshot.aggregate_revision
    );
    assert!(
        workflow_rx.try_recv().is_err(),
        "stale pause receipt must not cancel the resumed run"
    );
    assert_eq!(
        WorkflowManager::new(state.db_path.as_ref().clone())
            .unwrap()
            .get_snapshot(&session_id)
            .unwrap()
            .unwrap()
            .goal
            .status,
        mitsuro_core::workflow::GoalStatus::Active
    );

    let Json(cancelled) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::CancelGoal {
            operation_id: "route-cancel".into(),
            goal_id: paused.snapshot.goal.id.clone(),
            expected_revision: resumed.snapshot.aggregate_revision,
            reason: Some("User cancelled execution".into()),
        }),
    )
    .await
    .expect("goal should cancel");
    assert_eq!(
        cancelled.snapshot.goal.status,
        mitsuro_core::workflow::GoalStatus::Cancelled
    );
    assert!(
        matches!(workflow_rx.try_recv(), Ok(LoopInput::WorkflowUpdated { operation_id, .. }) if operation_id == "route-cancel")
    );
    assert!(matches!(workflow_rx.try_recv(), Ok(LoopInput::Cancel)));
    assert!(workflow_rx.try_recv().is_err());

    let Json(replayed_activation) = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        Json(WorkflowCommand::ActivateGoal {
            operation_id: "route-activate".into(),
            goal_id: approved.snapshot.goal.id.clone(),
            expected_revision: approved.snapshot.aggregate_revision,
        }),
    )
    .await
    .expect("old activation receipt should still replay");
    assert_eq!(
        replayed_activation.snapshot.aggregate_revision,
        activated.snapshot.aggregate_revision
    );
    assert!(
        workflow_rx.try_recv().is_err(),
        "stale activation must not wake a cancelled Goal"
    );
}

#[tokio::test]
async fn worker_dm_generic_workflow_commands_are_fenced_before_mutation_or_wake() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Worker DM",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("Worker DM should create");
    session_manager
        .update_session_work_mode(&session_id, mitsuro_core::storage::WorkMode::Plan)
        .expect("test should enter Plan mode");
    HiveWorkerStore::new(Database::new(&state.db_path).expect("database should open"))
        .create(&NewHiveWorker {
            user_id: Some("alice".into()),
            dm_session_id: Some(session_id.clone()),
            ..NewHiveWorker::new("workflow-worker")
        })
        .expect("Worker should create");
    let user = || Some(current_user("alice", state.working_dir.as_ref()));
    let denied_create = execute_workflow_command(
        State(state.clone()),
        user(),
        Path(session_id.clone()),
        Json(WorkflowCommand::CreateGoal {
            operation_id: "worker-goal-create".into(),
            goal: CreateGoalInput {
                title: "Worker-owned Goal".into(),
                objective: "Never enter the generic Agent loop".into(),
                constraints: vec![],
                criteria: vec![CriterionInput {
                    description: "Daemon owns execution".into(),
                    required: true,
                }],
                token_budget: None,
            },
        }),
    )
    .await;
    assert!(matches!(
        denied_create,
        Err(AppError::Conflict(message)) if message.contains("/hive/workers/")
    ));
    assert!(WorkflowManager::new(state.db_path.as_ref().clone())
        .unwrap()
        .get_snapshot(&session_id)
        .unwrap()
        .is_none());

    let created = WorkflowManager::new(state.db_path.as_ref().clone())
        .unwrap()
        .create_goal_with_plan(
            &session_id,
            CreateGoalInput {
                title: "Worker-owned Goal".into(),
                objective: "Never enter the generic Agent loop".into(),
                constraints: vec![],
                criteria: vec![CriterionInput {
                    description: "Daemon owns execution".into(),
                    required: true,
                }],
                token_budget: None,
            },
            PlanProposalInput {
                title: "Worker plan".into(),
                rationale: None,
                source_message_id: None,
                predecessor_id: None,
                legacy_markdown: None,
                steps: vec![StepProposalInput {
                    display_key: "1".into(),
                    description: "Use the Worker control plane".into(),
                    context: None,
                    parent_display_key: None,
                    dependencies: vec![],
                    acceptance_criteria: vec!["No generic wake".into()],
                    required: true,
                }],
            },
            "worker-goal-test-fixture",
            "test",
        )
        .expect("Worker Goal fixture should create atomically");
    let original_revision = created.snapshot.aggregate_revision;
    let (workflow_tx, mut workflow_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), workflow_tx);

    let denied = execute_workflow_command(
        State(state.clone()),
        user(),
        Path(session_id.clone()),
        Json(WorkflowCommand::ActivateGoal {
            operation_id: "generic-worker-activate".into(),
            goal_id: created.snapshot.goal.id.clone(),
            expected_revision: original_revision,
        }),
    )
    .await;
    assert!(matches!(
        denied,
        Err(AppError::Conflict(message)) if message.contains("/hive/workers/")
    ));
    assert!(
        workflow_rx.try_recv().is_err(),
        "no generic wake may escape"
    );
    assert_eq!(
        session_manager
            .get_session(&session_id)
            .unwrap()
            .unwrap()
            .work_mode,
        mitsuro_core::storage::WorkMode::Plan
    );
    let Json(after) = get_workflow(State(state.clone()), user(), Path(session_id))
        .await
        .expect("Worker Goal should remain readable");
    let after = after.expect("Worker Goal should still exist");
    assert_eq!(after.aggregate_revision, original_revision);
    assert_eq!(after.goal.status, mitsuro_core::workflow::GoalStatus::Draft);
}

#[tokio::test]
async fn every_worker_dm_generic_command_is_rejected_while_primary_hive_is_unchanged() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let neutral_dm = session_manager
        .create_session_for_user_with_config(
            "Neutral Worker DM",
            None,
            None,
            None,
            WorkspaceMode::Neutral,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("neutral DM should create");
    HiveWorkerStore::new(Database::new(&state.db_path).expect("database should open"))
        .create(&NewHiveWorker {
            user_id: Some("alice".into()),
            dm_session_id: Some(neutral_dm.clone()),
            ..NewHiveWorker::new("neutral-worker")
        })
        .expect("Worker should create");
    let goal = CreateGoalInput {
        title: "Workspace-first Goal".into(),
        objective: "Attach one exact workspace first".into(),
        constraints: vec![],
        criteria: vec![CriterionInput {
            description: "Workspace is explicit".into(),
            required: true,
        }],
        token_budget: None,
    };
    let denied = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(neutral_dm.clone()),
        Json(WorkflowCommand::CreateGoal {
            operation_id: "neutral-worker-create".into(),
            goal: goal.clone(),
        }),
    )
    .await;
    assert!(matches!(
        denied,
        Err(AppError::Conflict(message)) if message.contains("/hive/workers/")
    ));
    assert!(WorkflowManager::new(state.db_path.as_ref().clone())
        .unwrap()
        .get_snapshot(&neutral_dm)
        .unwrap()
        .is_none());

    let primary_hive = session_manager
        .create_session_for_user_with_config(
            "Primary Hive",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("primary Hive should create");
    let primary = execute_workflow_command(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(primary_hive),
        Json(WorkflowCommand::CreateGoal {
            operation_id: "primary-hive-create".into(),
            goal,
        }),
    )
    .await;
    assert!(
        primary.is_ok(),
        "primary Hive compatibility must remain unchanged"
    );
}

#[tokio::test]
async fn generic_worker_dm_delete_fails_closed_without_daemon_or_mutation() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let worker_dm = session_manager
        .create_session_for_user_with_config(
            "Durable Worker DM",
            None,
            None,
            None,
            WorkspaceMode::Neutral,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("Worker DM should create");
    let worker = HiveWorkerStore::new(Database::new(&state.db_path).expect("database should open"))
        .create(&NewHiveWorker {
            user_id: Some("alice".into()),
            dm_session_id: Some(worker_dm.clone()),
            ..NewHiveWorker::new("durable-worker")
        })
        .expect("Worker should create");

    for guarded in [
        state
            .hive_runtime
            .pause_session_for_user(&state, &worker_dm, Some("alice"), None)
            .await,
        state
            .hive_runtime
            .schedule_session_for_user(
                &state,
                worker_dm.clone(),
                chrono::Utc::now() + chrono::Duration::minutes(5),
                "test_worker_wake",
                "test worker sleep",
                Some("alice"),
                None,
            )
            .await,
        state
            .hive_runtime
            .resume_session_for_user(state.clone(), worker_dm.clone(), Some("alice"), None)
            .await,
    ] {
        assert!(matches!(
            crate::hive_runtime::control_plane_app_error(
                guarded.expect_err("generic Worker control must fail closed")
            ),
            AppError::Conflict(message) if message.contains("typed Worker")
        ));
    }
    let stop_error = state
        .hive_runtime
        .cancel_session_for_user(&state, &worker_dm, Some("alice"), None)
        .await
        .expect_err("Worker Stop must fail closed without the daemon");
    assert!(matches!(
        crate::hive_runtime::control_plane_app_error(stop_error),
        AppError::BadGateway(message) if message.contains("daemon control plane")
    ));
    let send_error = state
        .hive_runtime
        .send_message_for_user(
            state.clone(),
            worker_dm.clone(),
            "must not persist",
            Some("alice"),
            None,
        )
        .await
        .expect_err("Worker send must fail closed without the daemon");
    let steer_error = state
        .hive_runtime
        .steer_for_user(
            &state,
            &worker_dm,
            "pending-stop-test",
            Vec::new(),
            Some("alice"),
            None,
        )
        .await
        .expect_err("Worker steering must fail closed without the daemon");
    let response_error = state
        .hive_runtime
        .user_response_and_subscribe_for_user(
            &state,
            &worker_dm,
            "run-stop-test",
            "tool-stop-test",
            "must not persist",
            Some("alice"),
            None,
        )
        .await
        .expect_err("Worker response must fail closed without the daemon");
    for error in [send_error, steer_error, response_error] {
        assert!(error.to_string().contains("daemon control plane"));
    }

    let manager_error = state
        .hive_runtime
        .delete_session_for_user(&state, &worker_dm, Some("alice"), None)
        .await
        .expect_err("manager defense must reject generic Worker deletion");
    assert!(matches!(
        crate::hive_runtime::control_plane_app_error(manager_error),
        AppError::Conflict(message) if message.contains("/api/hive/workers")
    ));
    let route_error = delete_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(worker_dm.clone()),
    )
    .await
    .expect_err("route defense must reject generic Worker deletion");
    assert!(matches!(
        route_error,
        AppError::Conflict(message) if message.contains("/api/hive/workers")
    ));

    let database = Database::new(&state.db_path).expect("database should reopen");
    assert!(SessionManager::new(Database::new(&state.db_path).unwrap())
        .get_session(&worker_dm)
        .unwrap()
        .is_some());
    let persisted_binding: Option<String> = database
        .conn()
        .query_row(
            "SELECT dm_session_id FROM hive_workers WHERE id = ?1",
            [worker.id.as_str()],
            |row| row.get(0),
        )
        .expect("Worker binding should remain");
    assert_eq!(persisted_binding.as_deref(), Some(worker_dm.as_str()));
    let controller_count: i64 = database
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_controllers WHERE session_id = ?1",
            [worker_dm.as_str()],
            |row| row.get(0),
        )
        .expect("controller count should load");
    assert_eq!(
        controller_count, 0,
        "rejection must not create a controller"
    );
    let message_count: i64 = database
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
            [worker_dm.as_str()],
            |row| row.get(0),
        )
        .expect("message count should load");
    assert_eq!(message_count, 0, "rejection must not persist content");
}

#[tokio::test]
async fn generic_session_routes_preserve_hive_ownership_but_allow_user_organization() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let create = create_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Orphan Mako".into()),
            model: Some("test:model".into()),
            model_key: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: Some(SessionType::Hive),
            permission_mode: None,
        }),
    )
    .await;
    assert!(matches!(create, Err(AppError::Conflict(_))));

    let manager = SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    assert!(manager
        .list_sessions_for_user_by_type(None, Some("alice"), SessionType::Hive)
        .expect("sessions should list")
        .is_empty());
    let session_id = manager
        .create_session_for_user_with_config(
            "Daemon Mako",
            Some("test:model"),
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("test Mako session should create");

    let Json(organized) = update_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Path(session_id.clone()),
        Json(UpdateSessionRequest {
            title: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            mode: None,
            model: None,
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: Some(true),
            archived: Some(true),
        }),
    )
    .await
    .expect("Hive organization metadata should update");
    assert!(organized.pinned_at.is_some());
    assert!(organized.archived_at.is_some());

    let update = update_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Path(session_id.clone()),
        Json(UpdateSessionRequest {
            title: Some("Bypassed title".into()),
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            mode: None,
            model: None,
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: None,
            archived: None,
        }),
    )
    .await;
    assert!(matches!(update, Err(AppError::Conflict(_))));

    let pinch = pinch_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Path(session_id.clone()),
        Json(PinchRequest {
            preservation_hints: None,
            direction: None,
        }),
    )
    .await;
    assert!(matches!(pinch, Err(AppError::Conflict(_))));
    assert_eq!(
        manager
            .get_session(&session_id)
            .expect("session should load")
            .expect("session should exist")
            .title,
        "Daemon Mako"
    );
}

#[tokio::test]
async fn session_create_persists_full_continuation_contract() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let project_parent = user_root.join("projects");
    std::fs::create_dir_all(&project_parent).expect("project parent should exist");
    let project_dir = project_parent.join("continuation-contract");

    let (_, Json(created)) = create_session(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Continuation Contract".to_string()),
            model: Some("openai/gpt-5.5".to_string()),
            model_key: None,
            project_dir: Some(project_dir.to_string_lossy().to_string()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Created),
            target_branch: Some("feature/continue".to_string()),
            session_type: Some(SessionType::Code),
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session creation should succeed"));

    let expected_project = project_dir.to_string_lossy().to_string();
    assert_eq!(created.session_type, SessionType::Code);
    assert_eq!(created.workspace_mode, WorkspaceMode::Created);
    assert_eq!(
        created.working_dir.as_deref(),
        Some(expected_project.as_str())
    );
    assert_eq!(
        created.project_dir.as_deref(),
        Some(expected_project.as_str())
    );
    assert_eq!(created.target_branch.as_deref(), Some("feature/continue"));
    assert_eq!(created.model.as_deref(), Some("openai/gpt-5.5"));

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let persisted = session_manager
        .get_session(&created.id)
        .expect("session lookup should succeed")
        .expect("session should exist");
    assert_eq!(persisted.session_type, SessionType::Code);
    assert_eq!(persisted.workspace_mode, WorkspaceMode::Created);
    assert_eq!(
        persisted.working_dir.as_deref(),
        Some(expected_project.as_str())
    );
    assert_eq!(
        persisted.project_dir.as_deref(),
        Some(expected_project.as_str())
    );
    assert_eq!(persisted.target_branch.as_deref(), Some("feature/continue"));
    assert_eq!(persisted.model.as_deref(), Some("openai/gpt-5.5"));
}

#[tokio::test]
async fn create_session_persists_user_ownership() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let result = create_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Owned Session".to_string()),
            model: None,
            model_key: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await;
    let (_, Json(response)) = match result {
        Ok(response) => response,
        Err(_) => panic!("session creation should succeed"),
    };

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session = session_manager
        .get_session(&response.id)
        .expect("session lookup should succeed")
        .expect("session should exist");

    assert_eq!(session.user_id.as_deref(), Some("alice"));
}

#[tokio::test]
async fn create_session_resolves_relative_workspace_paths_within_user_root() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    std::fs::create_dir_all(&user_root).expect("user root should exist");

    let (_, Json(created)) = create_session(
        State(state),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Relative Workspace".to_string()),
            model: None,
            model_key: None,
            project_dir: Some("repo".to_string()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Selected),
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session creation should succeed"));

    let expected = user_root.join("repo").to_string_lossy().to_string();
    assert_eq!(created.project_dir.as_deref(), Some(expected.as_str()));
    assert_eq!(created.working_dir.as_deref(), Some(expected.as_str()));
}

#[tokio::test]
async fn create_session_accepts_fresh_absolute_workspace_path_with_existing_ancestor() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let parent_dir = user_root.join("projects");
    std::fs::create_dir_all(&parent_dir).expect("parent dir should exist");

    let fresh_project_dir = parent_dir.join("fresh-repo");

    let (_, Json(created)) = create_session(
        State(state),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Fresh Workspace".to_string()),
            model: None,
            model_key: None,
            project_dir: Some(fresh_project_dir.to_string_lossy().to_string()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Selected),
            target_branch: None,
            session_type: Some(SessionType::Code),
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session creation should succeed"));

    let expected = fresh_project_dir.to_string_lossy().to_string();
    assert_eq!(created.project_dir.as_deref(), Some(expected.as_str()));
    assert_eq!(created.working_dir.as_deref(), Some(expected.as_str()));
    assert_eq!(created.workspace_mode, WorkspaceMode::Selected);
    assert_eq!(created.session_type, SessionType::Code);
}

#[tokio::test]
async fn create_session_rejects_invalid_workspace_payloads() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let missing_project_dir = create_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Invalid Selected Workspace".to_string()),
            model: None,
            model_key: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Selected),
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await;

    match missing_project_dir {
        Err(AppError::BadRequest(message)) => {
            assert_eq!(
                message,
                "workspace modes 'selected' and 'created' require a project_dir"
            );
        }
        Ok(_) => panic!("invalid selected workspace should fail"),
        Err(_) => panic!("invalid selected workspace should fail with bad request"),
    }

    let neutral_with_project = create_session(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Invalid Neutral Workspace".to_string()),
            model: None,
            model_key: None,
            project_dir: Some("/tmp/repo".to_string()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Neutral),
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await;

    match neutral_with_project {
        Err(AppError::BadRequest(message)) => {
            assert_eq!(
                message,
                "workspace mode 'neutral' cannot include a project_dir"
            );
        }
        Ok(_) => panic!("neutral workspace with project should fail"),
        Err(_) => panic!("neutral workspace with project should fail with bad request"),
    }
}

#[tokio::test]
async fn get_session_rejects_foreign_owner() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Owned Session", None, None, Some("alice"))
        .expect("session creation should succeed");
    session_manager
        .save_message(&session_id, "user", r#"[{"type":"text","text":"hello"}]"#)
        .expect("message should save");

    let result = get_session(
        State(state),
        Some(current_user("bob", std::path::Path::new("/tmp"))),
        HeaderMap::new(),
        Path(session_id),
        Query(GetSessionQuery {
            limit: None,
            offset: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(AppError::NotFound(_))));
}

#[tokio::test]
async fn session_cancel_signals_the_owned_active_run() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Active Session", None, None, Some("alice"))
        .expect("session creation should succeed");
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);
    let child_cancellation = AgentCancellation::new().child_token();
    state.tool_registry.agent_runtime_manager().register(
        "owned-active-child",
        "active child",
        Some(session_id.clone()),
        child_cancellation.clone(),
    );

    let control_headers = code_control_headers(&state, &session_id).await;
    let Json(response) = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id),
        control_headers,
    )
    .await
    .unwrap_or_else(|_| panic!("owned cancellation should succeed"));

    assert!(response.ok);
    assert!(matches!(input_rx.recv().await, Some(LoopInput::Cancel)));
    assert!(child_cancellation.is_cancelled());
}

#[tokio::test]
async fn legacy_code_agent_cancel_yields_to_explicit_native_control() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_id = create_owned_code_session(&state, "alice", "Controlled Cancellation");
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);

    let legacy = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        HeaderMap::new(),
    )
    .await
    .expect("installed client may claim an unowned Code Agent");
    assert!(legacy.0.ok);
    assert!(matches!(input_rx.try_recv(), Ok(LoopInput::Cancel)));
    let renewed = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        HeaderMap::new(),
    )
    .await
    .expect("the installed client may renew its compatibility control");
    assert!(renewed.0.ok);
    assert!(matches!(input_rx.try_recv(), Ok(LoopInput::Cancel)));

    let lease = state
        .session_control_leases
        .write()
        .await
        .take(&session_id, "canvas-a".to_owned(), true)
        .expect("control lease");

    let denied = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        HeaderMap::new(),
    )
    .await;
    assert!(matches!(denied, Err(AppError::Conflict(_))));
    assert!(input_rx.try_recv().is_err());

    let mut headers = HeaderMap::new();
    headers.insert(
        crate::control_leases::CONTROL_LEASE_HEADER,
        lease.lease_token.parse().expect("header value"),
    );
    let Json(response) = cancel_session(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        Path(session_id),
        headers,
    )
    .await
    .expect("controller cancellation");
    assert!(response.ok);
    assert!(matches!(input_rx.recv().await, Some(LoopInput::Cancel)));
}

#[tokio::test]
async fn expired_code_agent_lease_rejects_stale_tokens_but_allows_legacy_claim() {
    let (mut state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_id = create_owned_code_session(&state, "alice", "Expired Control");
    state.session_control_leases = Arc::new(RwLock::new(
        crate::control_leases::SessionControlLeaseRegistry::new(std::time::Duration::from_millis(
            2,
        )),
    ));
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);
    let lease = state
        .session_control_leases
        .write()
        .await
        .take(&session_id, "canvas-a".to_owned(), false)
        .expect("short lease");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let mut stale_headers = HeaderMap::new();
    stale_headers.insert(
        crate::control_leases::CONTROL_LEASE_HEADER,
        lease.lease_token.parse().expect("lease header"),
    );
    let denied = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        stale_headers,
    )
    .await;
    assert!(matches!(denied, Err(AppError::Conflict(_))));
    assert!(input_rx.try_recv().is_err());
    let legacy = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id),
        HeaderMap::new(),
    )
    .await
    .expect("installed client may claim after native expiry");
    assert!(legacy.0.ok);
    assert!(matches!(input_rx.try_recv(), Ok(LoopInput::Cancel)));
}

#[tokio::test]
async fn session_cancel_rejects_a_foreign_owner_without_signalling() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Alice Session", None, None, Some("alice"))
        .expect("session creation should succeed");
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);
    let child_cancellation = AgentCancellation::new().child_token();
    state.tool_registry.agent_runtime_manager().register(
        "foreign-owned-child",
        "foreign child",
        Some(session_id.clone()),
        child_cancellation.clone(),
    );

    let result = cancel_session(
        State(state.clone()),
        Some(current_user("bob", state.working_dir.as_ref())),
        Path(session_id.clone()),
        axum::http::HeaderMap::new(),
    )
    .await;

    assert!(matches!(result, Err(AppError::NotFound(_))));
    assert!(input_rx.try_recv().is_err());
    assert!(!child_cancellation.is_cancelled());
    assert!(
        state
            .session_control_leases
            .write()
            .await
            .snapshot(&session_id)
            .controller
            .is_none(),
        "a foreign tenant must not acquire even compatibility control"
    );
}

#[tokio::test]
async fn session_cancel_is_idempotent_when_the_run_is_inactive() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Idle Session", None, None, Some("alice"))
        .expect("session creation should succeed");
    let now = Utc::now().to_rfc3339();
    let group_id = "idle-session-detached-group";
    let pending_id = format!("child-wake-{group_id}");
    let database = Database::new(&state.db_path).expect("database should open");
    database
        .conn()
        .execute(
            "INSERT INTO delegation_groups (
                delegation_group_id, parent_session_id, state, contract_json,
                parent_continuation_state, parent_continuation_id, created_at, updated_at
             ) VALUES (?1, ?2, 'running', '{}', 'pending', ?3, ?4, ?4)",
            (
                group_id,
                session_id.as_str(),
                pending_id.as_str(),
                now.as_str(),
            ),
        )
        .expect("detached group should insert");
    session_manager
        .queue_pending_steering_once(
            &session_id,
            &pending_id,
            r#"[{"type":"text","text":"late child wake"}]"#,
        )
        .expect("pending wake should queue");
    let child_cancellation = AgentCancellation::new().child_token();
    state.tool_registry.agent_runtime_manager().register(
        "owned-idle-child",
        "idle parent child",
        Some(session_id.clone()),
        child_cancellation.clone(),
    );

    let control_headers = code_control_headers(&state, &session_id).await;
    let Json(first_response) = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        control_headers.clone(),
    )
    .await
    .unwrap_or_else(|_| panic!("idle cancellation should be idempotent"));
    let Json(second_response) = cancel_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
        control_headers,
    )
    .await
    .unwrap_or_else(|_| panic!("repeated idle cancellation should be idempotent"));

    assert!(first_response.ok);
    assert!(second_response.ok);
    assert!(child_cancellation.is_cancelled());
    let database = Database::new(&state.db_path).expect("database should reopen");
    let continuation_state: String = database
        .conn()
        .query_row(
            "SELECT parent_continuation_state FROM delegation_groups
              WHERE delegation_group_id = ?1",
            [group_id],
            |row| row.get(0),
        )
        .expect("continuation state should load");
    assert_eq!(continuation_state, "not_requested");
    assert!(SessionManager::new(database)
        .load_pending_steering(&session_id, &pending_id)
        .expect("pending wake lookup should succeed")
        .is_none());
}

#[tokio::test]
async fn load_owned_session_rejects_legacy_userless_session_for_authenticated_user() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session("Legacy Session", None, None)
        .expect("session creation should succeed");
    let user = current_user("alice", std::path::Path::new("/tmp"));

    let result = super::load_owned_session(&session_manager, &session_id, Some(&user));

    assert!(matches!(result, Err(AppError::NotFound(_))));
}

#[tokio::test]
async fn load_owned_session_rejects_authenticated_session_for_local_actor() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Alice Session", None, None, Some("alice"))
        .expect("session creation should succeed");

    let result = super::load_owned_session(&session_manager, &session_id, None);

    assert!(matches!(result, Err(AppError::NotFound(_))));
}

#[tokio::test]
async fn internal_group_worker_lanes_are_hidden_from_generic_session_surfaces() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user = current_user("alice", &temp_dir);
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let dm_session_id = session_manager
        .create_session_for_user_with_config(
            "Researcher DM",
            Some("test:model"),
            Some(temp_dir.to_string_lossy().as_ref()),
            Some(temp_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .unwrap();
    let lane_session_id = session_manager
        .create_session_for_user_with_config(
            "Researcher in Release Room",
            Some("test:model"),
            Some(temp_dir.to_string_lossy().as_ref()),
            Some(temp_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .unwrap();
    let worker = HiveWorkerStore::new(Database::new(&state.db_path).unwrap())
        .create(&NewHiveWorker {
            user_id: Some("alice".into()),
            dm_session_id: Some(dm_session_id.clone()),
            ..NewHiveWorker::new("researcher")
        })
        .unwrap();
    let group = HiveGroupStore::new(Database::new(&state.db_path).unwrap())
        .create(&NewHiveGroup {
            user_id: Some("alice".into()),
            title: "Release Room".into(),
            member_worker_ids: vec![worker.id.clone()],
            ..NewHiveGroup::default()
        })
        .unwrap();
    HiveGroupWorkerLaneStore::new(Database::new(&state.db_path).unwrap())
        .upsert(&NewHiveGroupWorkerLane::new(
            group.id,
            worker.id,
            lane_session_id.clone(),
        ))
        .unwrap();
    session_manager
        .set_agent_state(&lane_session_id, "streaming")
        .unwrap();

    assert!(matches!(
        super::load_owned_session(&session_manager, &lane_session_id, Some(&user)),
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        delete_session(
            State(state.clone()),
            Some(current_user("alice", &temp_dir)),
            Path(lane_session_id.clone()),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(session_manager
        .get_session(&lane_session_id)
        .unwrap()
        .is_some());
    let Json(sessions) = list_sessions(
        State(state),
        Some(user),
        HeaderMap::new(),
        Query(ListSessionsQuery {
            working_dir: None,
            include_archived: true,
        }),
    )
    .await
    .unwrap();
    assert!(sessions.iter().any(|session| session.id == dm_session_id));
    assert!(sessions.iter().all(|session| session.id != lane_session_id));
    assert!(session_manager
        .list_active_sessions()
        .unwrap()
        .iter()
        .all(|(session_id, _)| session_id != &lane_session_id));
    assert!(session_manager
        .list_active_session_details_for_user(Some("alice"))
        .unwrap()
        .iter()
        .all(|(session, _)| session.id != lane_session_id));
}

#[tokio::test]
async fn list_sessions_resolves_relative_working_dir_filter_within_user_root() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let repo_dir = user_root.join("repo");
    std::fs::create_dir_all(&repo_dir).expect("repo dir should exist");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Scoped Session",
            None,
            Some(repo_dir.to_string_lossy().as_ref()),
            Some(repo_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("session creation should succeed");
    session_manager
        .set_agent_state(&session_id, "tool_executing")
        .expect("agent state should update");

    let Json(response) = list_sessions(
        State(state),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Query(ListSessionsQuery {
            working_dir: Some("repo".to_string()),
            include_archived: false,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session list should succeed"));

    assert_eq!(response.len(), 1);
    assert_eq!(response[0].agent_state, "tool_executing");
    assert_eq!(
        response[0].working_dir.as_deref(),
        Some(repo_dir.to_string_lossy().as_ref())
    );
}

#[tokio::test]
async fn presence_heartbeat_tracks_active_controller_for_owned_session() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Owned Session", None, None, Some("alice"))
        .expect("session creation should succeed");

    let Json(response) = heartbeat_session_presence(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        Path(session_id),
        Json(SessionPresenceHeartbeatRequest {
            client_id: "client-1".to_string(),
            surface: "web".to_string(),
            capability: crate::presence::PresenceCapability::Controller,
            last_event_sequence: Some(12),
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("presence heartbeat should succeed"));

    assert_eq!(response.active_viewers, 1);
    assert_eq!(response.active_controllers, 1);
    assert_eq!(response.clients.len(), 1);
    assert_eq!(response.clients[0].client_id, "client-1");
    assert_eq!(response.clients[0].last_event_sequence, Some(12));
}

#[tokio::test]
async fn session_state_exposes_recovery_live_partial_and_trace_sequence() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Recoverable Session",
            Some("openai/gpt-5.5"),
            None,
            None,
            WorkspaceMode::Neutral,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("session creation should succeed");
    session_manager
        .set_agent_state(&session_id, "streaming")
        .expect("agent state should update");

    let recovery = SessionRecoveryState::new(
        RecoveryStatus::Streaming,
        Some(LoopStopReason::StreamIdleTimeout),
        None,
        PartialAssistantState {
            citations: Vec::new(),
            text: "partial answer".to_string(),
            thinking: "working notes".to_string(),
            tool_calls: vec![RecoveryToolCall::summary("tool-1", "edit")],
        },
        RecoveryDecision::Resumable {
            latest_user_objective: "finish the contract harness".to_string(),
        },
    );
    session_manager
        .update_recovery_state(&session_id, &recovery)
        .expect("recovery state should persist");

    let trace_event = RuntimeTraceEvent::from_loop_event(
        "run-1",
        42,
        1,
        &LoopEvent::TextDelta {
            delta: "partial answer".to_string(),
        },
    );
    RuntimeTraceStore::new(session_manager.db())
        .append_event(&session_id, &trace_event)
        .expect("runtime trace should persist");

    let Json(response) = get_session_state(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        Path(session_id.clone()),
        Query(state::GetSessionStateQuery::default()),
    )
    .await
    .unwrap_or_else(|_| panic!("session state should load"));

    assert_eq!(response.id, session_id);
    assert_eq!(response.agent_state, "streaming");
    assert_eq!(response.recovery.as_ref(), Some(&recovery));
    assert_eq!(
        response
            .live_partial_assistant
            .as_ref()
            .map(|partial| partial.text.as_str()),
        Some("partial answer")
    );
    assert_eq!(response.last_event_sequence, Some(42));
}

#[tokio::test]
async fn session_state_projects_registered_run_across_durable_idle_launch_gap() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Continuation launch", None, None, Some("alice"))
        .expect("session creation should succeed");
    session_manager
        .set_agent_state(&session_id, "idle")
        .expect("durable state should model the launch gap");

    let (input_tx, _input_rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);

    let Json(response) = get_session_state(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id),
        Query(state::GetSessionStateQuery::default()),
    )
    .await
    .expect("session state should load");

    assert_eq!(response.agent_state, "streaming");
}

fn seed_trace_snapshot(state: &AppState, session_id: &str) -> Vec<RuntimeTraceEvent> {
    let db = Database::new(&state.db_path).expect("database should open");
    let store = RuntimeTraceStore::new(&db);
    let events = vec![
        LoopEvent::ThinkingDelta {
            thinking: "inspect".to_string(),
        },
        LoopEvent::TextDelta {
            delta: "answer".to_string(),
        },
        LoopEvent::TurnComplete {
            turn: 1,
            has_more: false,
        },
        LoopEvent::Finished {
            session_id: session_id.to_string(),
            stop_reason: LoopStopReason::Completed,
        },
        LoopEvent::TitleGenerated {
            title: "Snapshot complete".to_string(),
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, event)| {
        RuntimeTraceEvent::from_loop_event(
            "snapshot-run",
            i64::try_from(index + 1).expect("small sequence"),
            1,
            &event,
        )
    })
    .collect::<Vec<_>>();

    for event in &events {
        store
            .append_event(session_id, event)
            .expect("trace event should persist");
    }
    events
}

#[tokio::test]
async fn session_trace_latest_limit_uses_one_coherent_snapshot() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user(
            "Trace Snapshot",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("session should create");
    seed_trace_snapshot(&state, &session_id);

    let Json(response) = get_session_trace(
        State(state),
        Some(current_user("alice", &temp_dir)),
        Path(session_id),
        Query(state::GetSessionTraceQuery {
            limit: Some(2),
            after_sequence: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("trace endpoint should succeed"));

    assert_eq!(response.summary.total_events, 5);
    assert_eq!(
        response.summary.last_stop_reason,
        Some(LoopStopReason::Completed)
    );
    assert_eq!(response.latest_sequence, Some(5));
    assert_eq!(
        response
            .events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
}

#[tokio::test]
async fn session_trace_after_sequence_applies_limit_without_changing_snapshot_metadata() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user(
            "Incremental Trace Snapshot",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("session should create");
    seed_trace_snapshot(&state, &session_id);

    let Json(response) = get_session_trace(
        State(state),
        Some(current_user("alice", &temp_dir)),
        Path(session_id),
        Query(state::GetSessionTraceQuery {
            limit: Some(2),
            after_sequence: Some(2),
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("trace endpoint should succeed"));

    assert_eq!(response.summary.total_events, 5);
    assert_eq!(
        response.summary.last_stop_reason,
        Some(LoopStopReason::Completed)
    );
    assert_eq!(response.latest_sequence, Some(5));
    assert_eq!(
        response
            .events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
}

#[tokio::test]
async fn get_session_state_restores_active_delegated_snapshot_from_durable_run() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Durable delegated state", None, None, None)
        .expect("session should create");
    let store =
        DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"));
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "durable-live-run".to_string(),
            parent_session_id: session_id.clone(),
            parent_tool_call_id: Some("tool-durable-live".to_string()),
            role: DelegatedRunRole::Build,
            stage: CoreDelegatedRunStage::Created,
            provider: Some("test".to_string()),
            model: Some("test:model".to_string()),
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![DelegatedRunScope {
                label: "workspace".to_string(),
                path: ".".to_string(),
                kind: "workspace".to_string(),
            }],
        })
        .expect("delegated run should create");
    store
        .update_snapshot(
            "durable-live-run",
            CoreDelegatedRunStage::Running,
            &DelegatedRunSnapshot {
                stage: CoreDelegatedRunStage::Running,
                agents: vec![DelegatedRunAgentSnapshot {
                    task_id: "builder-a".to_string(),
                    agent_name: "builder-a".to_string(),
                    status: "degraded".to_string(),
                    tool_count: 4,
                    tokens: 120,
                    current_action: Some("degraded".to_string()),
                    completion_summary: Some("Partial evidence retained".to_string()),
                    lines_added: 8,
                    lines_removed: 2,
                    completed_plan_task: None,
                }],
            },
        )
        .expect("live snapshot should persist");

    // The in-memory map is intentionally empty, as it would be after a server
    // restart. The session route must reconstruct active progress from the
    // durable delegated run snapshot.
    let Json(response) = get_session_state(
        State(state),
        None,
        Path(session_id),
        Query(state::GetSessionStateQuery::default()),
    )
    .await
    .unwrap_or_else(|_| panic!("session state should load"));
    assert_eq!(response.delegated_tools.len(), 1);
    let snapshot = &response.delegated_tools[0];
    assert_eq!(snapshot.delegated_run_id, "durable-live-run");
    assert_eq!(snapshot.tool_call_id, "tool-durable-live");
    assert!(matches!(
        snapshot.stage,
        crate::types::DelegatedRunStage::Running
    ));
    assert!(matches!(
        snapshot.agents.first().map(|agent| agent.status),
        Some(crate::types::DelegatedProgressStatus::Degraded)
    ));
}

#[tokio::test]
async fn get_session_state_evicts_stale_live_running_and_indexes_old_terminal_runs() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Canonical delegated state", None, None, None)
        .expect("session should create");
    let store =
        DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"));

    let create_run = |run_id: &str, tool_call_id: &str| {
        store
            .create_run(&DelegatedRunStartInput {
                delegated_run_id: run_id.to_string(),
                parent_session_id: session_id.clone(),
                parent_tool_call_id: Some(tool_call_id.to_string()),
                role: DelegatedRunRole::Build,
                stage: CoreDelegatedRunStage::Created,
                provider: None,
                model: None,
                resumable: true,
                resumed_from_run_id: None,
                target_scope: vec![DelegatedRunScope {
                    label: "project".to_string(),
                    path: ".".to_string(),
                    kind: "project".to_string(),
                }],
            })
            .expect("delegated run should create");
        store
            .finalize_run(
                run_id,
                CoreDelegatedRunStage::Complete,
                &serde_json::json!({"outcome": "success"}),
                Some("complete"),
                true,
            )
            .expect("delegated run should finalize");
    };

    create_run("old-terminal-run", "old-terminal-tool");
    Database::new(&state.db_path)
        .expect("database should open")
        .conn()
        .execute(
            "UPDATE delegated_runs SET updated_at = '2020-01-01T00:00:00Z'
             WHERE delegated_run_id = 'old-terminal-run'",
            [],
        )
        .expect("old run should age out of artifact window");
    for index in 0..24 {
        create_run(
            &format!("newer-terminal-run-{index:02}"),
            &format!("newer-terminal-tool-{index:02}"),
        );
    }

    state.delegated_state.write().await.insert(
        session_id.clone(),
        vec![crate::types::DelegatedToolStateResponse {
            delegated_run_id: "old-terminal-run".to_string(),
            tool_call_id: "old-terminal-tool".to_string(),
            kind: crate::types::DelegatedToolKind::Build,
            stage: crate::types::DelegatedRunStage::Running,
            parent_session_id: Some(session_id.clone()),
            agents: Vec::new(),
        }],
    );

    let Json(response) = get_session_state(
        State(state),
        None,
        Path(session_id),
        Query(state::GetSessionStateQuery {
            include_delegated_history: true,
            delegation_after_cursor: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session state should load"));
    assert!(response.delegated_tools.is_empty());
    assert!(response
        .recent_delegated_runs
        .iter()
        .all(|run| run.delegated_run_id != "old-terminal-run"));
    let old_summary = response
        .delegated_run_summaries
        .iter()
        .find(|summary| summary.delegated_run_id == "old-terminal-run")
        .expect("compact hydration index should retain the old terminal run");
    assert!(matches!(
        old_summary.stage,
        crate::types::DelegatedRunStage::Complete
    ));
}

#[tokio::test]
async fn get_session_state_exposes_awaiting_input_details_after_reload() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Awaiting Input Session",
            Some("openai/gpt-5.5"),
            None,
            None,
            WorkspaceMode::Neutral,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("session creation should succeed");
    session_manager
        .set_agent_state(&session_id, "idle")
        .expect("agent state should simulate a fresh server process");

    let pending_interactions = vec![
        PendingInteractionSnapshot::ask_user_from_call(
            "ask-1",
            &serde_json::json!({
                "questions": [{
                    "header": "Choose deploy target",
                    "question": "Which environment should Krusty continue against?",
                    "options": [{ "label": "staging", "description": "Safe validation" }],
                    "multi_select": false
                }]
            }),
        ),
        PendingInteractionSnapshot::tool_approval_from_call(
            "tool-1",
            "edit",
            &serde_json::json!({
                "file_path": "src/lib.rs",
                "api_token": "super-secret-token",
                "content": "raw file content should not be replayed to clients"
            }),
        ),
        PendingInteractionSnapshot::plan_confirm(
            "plan-1",
            "Ship reload-safe prompts",
            2,
            vec![PendingPlanTaskSnapshot {
                description: "Expose the server contract".to_string(),
                completed: false,
            }],
        ),
    ];
    let recovery = SessionRecoveryState::new_with_pending_interactions(
        RecoveryStatus::AwaitingInput,
        Some(LoopStopReason::AwaitingInput),
        None,
        PartialAssistantState::default(),
        pending_interactions.clone(),
        RecoveryDecision::NonResumable {
            reason: RecoveryNonResumableReason::AwaitingHumanInput,
        },
    );
    session_manager
        .update_recovery_state(&session_id, &recovery)
        .expect("recovery state should persist");

    let Json(response) = get_session_state(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        Path(session_id.clone()),
        Query(state::GetSessionStateQuery::default()),
    )
    .await
    .unwrap_or_else(|_| panic!("session state should load"));

    assert_eq!(response.id, session_id);
    assert_eq!(response.agent_state, "idle");
    assert_eq!(response.recovery.as_ref(), Some(&recovery));
    assert_eq!(response.pending_interactions, pending_interactions);

    let PendingInteractionSnapshot::ToolApproval { tool_call } = &response.pending_interactions[1]
    else {
        panic!("expected a tool approval pending interaction");
    };
    assert_eq!(tool_call.arguments.value["file_path"], "src/lib.rs");
    assert_eq!(tool_call.arguments.value["api_token"], "[REDACTED]");
    assert!(tool_call
        .arguments
        .redacted_paths
        .contains(&"$.api_token".to_string()));
}

#[tokio::test]
async fn pinch_model_contract_resolves_persisted_exact_key_and_runtime_context() {
    let (state, _temp_dir) = create_test_state();
    let narrow = ModelMetadata::new("shared-pinch", "Narrow", ProviderId::MiniMax)
        .with_context(8_192, 2_048)
        .with_transport(ApiFormat::Anthropic);
    let exact = ModelMetadata::new("shared-pinch", "Exact", ProviderId::OpenRouter)
        .with_context(500_000, 32_768)
        .with_transport(ApiFormat::OpenAI);
    let exact_key = exact.key();
    state
        .model_registry
        .set_models(ProviderId::MiniMax, vec![narrow])
        .await;
    state
        .model_registry
        .set_models(ProviderId::OpenRouter, vec![exact])
        .await;
    state
        .credential_store
        .write()
        .await
        .set(ProviderId::OpenRouter, "openrouter-test-key".to_string());

    let manager =
        open_session_manager(&state).unwrap_or_else(|_| panic!("session manager should open"));
    let session_id = manager
        .create_session("Exact pinch", Some("shared-pinch"), None)
        .expect("session should create");
    manager
        .update_session_model_selection(&session_id, Some(&exact_key), Some("pinch-runtime"))
        .expect("exact selection should persist");

    let source = manager
        .get_session(&session_id)
        .expect("session should load")
        .expect("session should exist");
    let persisted_key = source.model_key.as_ref().expect("exact key should load");
    let client = state
        .resolve_ai_client_for_key_for_user(persisted_key, source.user_id.as_deref())
        .await
        .expect("exact pinch client should resolve");
    let effective_window = effective_context_window_for_runtime(
        client.config().uses_chatgpt_codex_format(),
        client.resolved_model().capabilities.context_window,
    );

    assert_eq!(client.resolved_model().key, exact_key);
    assert_eq!(client.provider_id(), ProviderId::OpenRouter);
    assert_eq!(effective_window, 500_000);
}

#[tokio::test]
async fn pinch_session_includes_project_context_and_ranked_files() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let exact_metadata = ModelMetadata::new(
        "claude-3-5-sonnet",
        "Claude 3.5 Sonnet",
        ProviderId::Anthropic,
    )
    .with_context(200_000, 8_192)
    .with_transport(ApiFormat::Anthropic);
    let exact_key = exact_metadata.key();
    state
        .model_registry
        .set_models_with_catalog(
            ProviderId::Anthropic,
            vec![exact_metadata],
            Some(ModelCatalogSource::Curated),
            Some("pinch-catalog".to_string()),
        )
        .await;

    let workspace = state.working_dir.as_ref();
    let source_file = workspace.join("src/lib.rs");
    std::fs::create_dir_all(source_file.parent().expect("parent dir should exist"))
        .expect("src dir should exist");
    std::fs::write(
        workspace.join("AGENTS.md"),
        "# Workspace Rules\nPreserve session context.\n",
    )
    .expect("project instructions should write");
    std::fs::write(
        &source_file,
        "pub fn important() -> &'static str { \"hello\" }\n",
    )
    .expect("source file should write");

    let session_manager = match open_session_manager(&state) {
        Ok(session_manager) => session_manager,
        Err(_) => panic!("session manager should open"),
    };
    let session_id = session_manager
        .create_session_for_user(
            "Pinch Source",
            Some("claude-3-5-sonnet"),
            Some(workspace.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("session should create");
    session_manager
        .update_session_model_selection(&session_id, Some(&exact_key), Some("pinch-catalog"))
        .expect("exact pinch model should persist");

    let user_message =
        serde_json::json!([{ "type": "text", "text": "Continue refining the server pinch flow." }])
            .to_string();
    let assistant_message =
            serde_json::json!([{ "type": "text", "text": "I inspected the route and found missing continuation context." }])
                .to_string();
    session_manager
        .save_message(&session_id, "user", &user_message)
        .expect("user message should save");
    session_manager
        .save_message(&session_id, "assistant", &assistant_message)
        .expect("assistant message should save");

    let plan_manager =
        PlanManager::new((*state.db_path).clone()).expect("plan manager should open");
    let plan = PlanFile::from_markdown(
        r#"# Plan: Server Pinch Follow-up

Created: 2026-04-06 12:00 UTC
Session: placeholder
Working Directory: placeholder
Status: in_progress

---

## Phase 1: Continuation

- [ ] Task 1.1: Keep session continuity
"#,
    )
    .expect("plan should parse");
    plan_manager
        .save_plan_for_session(&session_id, &plan)
        .expect("plan should save");

    session_manager
            .db()
            .conn()
            .execute(
                "INSERT INTO file_activity (session_id, file_path, read_count, write_count, edit_count, last_accessed, user_referenced)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                (
                    &session_id,
                    "src/lib.rs",
                    2_i64,
                    1_i64,
                    0_i64,
                    Utc::now().to_rfc3339(),
                    1_i64,
                ),
            )
            .expect("file activity should insert");

    let Json(response) = pinch_session(
        State(state.clone()),
        Some(current_user("alice", workspace)),
        HeaderMap::new(),
        Path(session_id.clone()),
        Json(PinchRequest {
            preservation_hints: Some("Keep the route semantics intact.".to_string()),
            direction: Some("Continue the server audit.".to_string()),
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("pinch should succeed"));

    assert_eq!(response.session.id, session_id);
    assert_eq!(response.session.model_key, Some(exact_key));
    assert_eq!(
        response.session.model_catalog_revision.as_deref(),
        Some("pinch-catalog")
    );
    assert!(response.checkpoint_id.is_some());
    assert!(response.replaced_messages.unwrap_or(0) > 0);

    let messages = session_manager
        .load_session_messages(&response.session.id)
        .expect("compacted messages should load");
    let combined = messages
        .iter()
        .map(|(_, content)| content.as_str())
        .collect::<String>();

    assert!(combined.contains("Conversation Compacted"));
    assert!(combined.contains("Continue the server audit."));
    assert!(combined.contains("src/lib.rs"));
    assert!(
        !combined.contains("Task 1.1: Keep session continuity"),
        "canonical active plan state must be reinjected at request time, not duplicated in history"
    );
    assert!(plan_manager
        .get_active_plan(&session_id)
        .expect("active plan lookup")
        .is_some());
}

#[tokio::test]
async fn pinch_session_rejects_an_active_session_writer() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let workspace = state.working_dir.as_ref();
    let session_manager = match open_session_manager(&state) {
        Ok(manager) => manager,
        Err(_) => panic!("session manager"),
    };
    let session_id = session_manager
        .create_session_for_user(
            "Busy Pinch",
            None,
            Some(workspace.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("session");
    let _guard = state
        .try_lock_session(&session_id)
        .await
        .expect("first session writer lock");

    let result = pinch_session(
        State(state.clone()),
        Some(current_user("alice", workspace)),
        HeaderMap::new(),
        Path(session_id.clone()),
        Json(PinchRequest {
            preservation_hints: None,
            direction: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(AppError::Conflict(message)) if message.contains("busy")));
}

#[tokio::test]
async fn pinch_session_resolves_legacy_relative_working_dir_against_user_home() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let repo_dir = user_root.join("repo");
    std::fs::create_dir_all(&repo_dir).expect("repo dir should exist");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Legacy Relative Session",
            None,
            Some("repo"),
            Some("repo"),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("session should create");
    let user_message =
        serde_json::json!([{ "type": "text", "text": "Continue from the last session." }])
            .to_string();
    let assistant_message =
        serde_json::json!([{ "type": "text", "text": "I will resume the work." }]).to_string();
    session_manager
        .save_message(&session_id, "user", &user_message)
        .expect("user message should save");
    session_manager
        .save_message(&session_id, "assistant", &assistant_message)
        .expect("assistant message should save");

    let Json(response) = pinch_session(
        State(state),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Path(session_id.clone()),
        Json(PinchRequest {
            preservation_hints: None,
            direction: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("pinch should succeed"));

    assert_eq!(response.session.id, session_id);

    let expected = repo_dir.to_string_lossy().to_string();
    assert_eq!(
        response.session.working_dir.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        response.session.project_dir.as_deref(),
        Some(expected.as_str())
    );
}

#[test]
fn live_partial_assistant_only_surfaces_for_active_states() {
    let recovery = mitsuro_core::storage::SessionRecoveryState::new(
        mitsuro_core::storage::RecoveryStatus::Streaming,
        None,
        None,
        mitsuro_core::storage::PartialAssistantState {
            citations: Vec::new(),
            text: "partial".to_string(),
            thinking: "reasoning".to_string(),
            tool_calls: Vec::new(),
        },
        mitsuro_core::storage::RecoveryDecision::Resumable {
            latest_user_objective: "finish task".to_string(),
        },
    );

    assert!(super::state::live_partial_assistant_for_state("idle", Some(&recovery)).is_none());

    let live = super::state::live_partial_assistant_for_state("streaming", Some(&recovery))
        .expect("active state should surface live partial");
    assert_eq!(live.text, "partial");
    assert_eq!(live.thinking, "reasoning");
}

#[tokio::test]
async fn create_session_persists_exact_model_key_and_catalog_revision() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let mut metadata = ModelMetadata::new("shared-model", "Shared via Grok", ProviderId::Grok);
    metadata.api_format = ApiFormat::OpenAIResponses;
    let key = ModelKey::from_metadata(&metadata);
    state
        .model_registry
        .set_models_with_catalog(
            ProviderId::Grok,
            vec![metadata],
            Some(ModelCatalogSource::LiveDynamic),
            Some("catalog-exact-test".to_string()),
        )
        .await;

    let (_, Json(created)) = create_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Exact model".to_string()),
            model: Some("shared-model".to_string()),
            model_key: Some(key.clone()),
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("exact session creation should succeed"));

    assert_eq!(created.model.as_deref(), Some("shared-model"));
    assert_eq!(created.model_key, Some(key.clone()));
    assert_eq!(
        created.model_catalog_revision.as_deref(),
        Some("catalog-exact-test")
    );
    let persisted = open_session_manager(&state)
        .unwrap_or_else(|_| panic!("session manager should open"))
        .get_session(&created.id)
        .expect("session should load")
        .expect("session should exist");
    assert_eq!(persisted.model_key, Some(key));
    assert_eq!(
        persisted.model_catalog_revision.as_deref(),
        Some("catalog-exact-test")
    );
}

#[tokio::test]
async fn session_routes_normalize_blank_model_input_to_none() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let (_, Json(created)) = create_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Whitespace Model".to_string()),
            model: Some("   ".to_string()),
            model_key: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session creation should succeed"));

    assert_eq!(created.model, None);

    let Json(updated) = update_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Path(created.id.clone()),
        Json(UpdateSessionRequest {
            title: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            mode: None,
            model: Some("  gpt-5  ".to_string()),
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: None,
            archived: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session update should succeed"));

    assert_eq!(updated.model.as_deref(), Some("gpt-5"));

    let Json(cleared) = update_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Path(created.id),
        Json(UpdateSessionRequest {
            title: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            mode: None,
            model: Some("   ".to_string()),
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: None,
            archived: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session update should succeed"));

    assert_eq!(cleared.model, None);
}

#[tokio::test]
async fn session_routes_apply_workspace_updates() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let project_dir = state.working_dir.join("demo-app");
    std::fs::create_dir_all(&project_dir).expect("project dir should exist");

    let (_, Json(created)) = create_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Workspace Update".to_string()),
            model: None,
            model_key: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session creation should succeed"));

    let Json(updated) = update_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Path(created.id.clone()),
        Json(UpdateSessionRequest {
            title: None,
            project_dir: Some(project_dir.to_string_lossy().to_string()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Created),
            mode: None,
            model: None,
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: None,
            archived: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("workspace update should succeed"));

    assert_eq!(
        updated.project_dir.as_deref(),
        Some(project_dir.to_string_lossy().as_ref())
    );
    assert_eq!(
        updated.working_dir.as_deref(),
        Some(project_dir.to_string_lossy().as_ref())
    );
    assert_eq!(updated.workspace_mode, WorkspaceMode::Created);

    let Json(neutral) = update_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Path(created.id),
        Json(UpdateSessionRequest {
            title: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Neutral),
            mode: None,
            model: None,
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: None,
            archived: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("neutral workspace update should succeed"));

    assert_eq!(neutral.project_dir, None);
    assert_eq!(neutral.working_dir, None);
    assert_eq!(neutral.workspace_mode, WorkspaceMode::Neutral);
}

#[tokio::test]
async fn session_routes_reject_invalid_workspace_payloads() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let (_, Json(created)) = create_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Workspace Validation".to_string()),
            model: None,
            model_key: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session creation should succeed"));

    let result = update_session(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        HeaderMap::new(),
        Path(created.id),
        Json(UpdateSessionRequest {
            title: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Created),
            mode: None,
            model: None,
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: None,
            archived: None,
        }),
    )
    .await;

    match result {
        Err(AppError::BadRequest(message)) => {
            assert_eq!(
                message,
                "workspace modes 'selected' and 'created' require a project_dir"
            );
        }
        Ok(_) => panic!("invalid workspace update should fail"),
        Err(_) => panic!("invalid workspace update should fail with bad request"),
    }
}

#[tokio::test]
async fn session_routes_reject_working_dir_updates_outside_user_root() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let outside_root = temp_dir.join("outside");
    std::fs::create_dir_all(&user_root).expect("user root should exist");
    std::fs::create_dir_all(&outside_root).expect("outside root should exist");

    let (_, Json(created)) = create_session(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Json(CreateSessionRequest {
            title: Some("Workspace Validation".to_string()),
            model: None,
            model_key: None,
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            permission_mode: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("session creation should succeed"));

    let result = update_session(
        State(state),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Path(created.id),
        Json(UpdateSessionRequest {
            title: None,
            project_dir: None,
            working_dir: Some(outside_root.to_string_lossy().to_string()),
            workspace_mode: None,
            mode: None,
            model: None,
            model_key: None,
            target_branch: None,
            permission_mode: None,
            pinned: None,
            archived: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(AppError::BadRequest(_))));
}
