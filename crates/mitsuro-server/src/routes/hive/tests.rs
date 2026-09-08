use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::Json;
use tokio::sync::{Mutex, RwLock};
use tower::ServiceExt;

use mitsuro_core::agent::{
    loop_events::LoopStopReason, AgentCancellation, DelegatedRunStage, LoopEvent, UserHookManager,
};
use mitsuro_core::ai::models::{create_model_registry, ApiFormat, ModelAuthScope, ModelMetadata};
use mitsuro_core::ai::providers::ProviderId;
use mitsuro_core::mcp::McpManager;
use mitsuro_core::paths;
use mitsuro_core::process::ProcessRegistry;
use mitsuro_core::skills::SkillsManager;
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::storage::reports::{CreateReportInput, ReportScope};
use mitsuro_core::storage::{
    bootstrap_hive_home, refresh_current_snapshot, AutonomousTaskStore, Database, DelegatedRunRole,
    DelegatedRunScope, DelegatedRunStartInput, DelegatedRunStore, HiveGroupWorkerLaneStore,
    HiveProfileDocumentKind, HiveProfileOwner, HiveProfileStore, HiveRunPriority,
    HiveRuntimeStateStatus, HiveRuntimeStateStore, HiveWorkerStore, MemoryStore, MemoryType,
    NewHiveGroupWorkerLane, Preferences, ReportStore, RuntimeTraceEvent, RuntimeTraceStore,
    SessionType, WorkspaceMode, CURRENT_SNAPSHOT_TITLE,
};
use mitsuro_core::tools::registry::ToolRegistry;
use mitsuro_core::SessionManager;

use super::attention::{attention, AttentionQuery};
use super::control_plane::{
    cancel_schedule, create_schedule, pause_schedule, replace_schedule, resume_schedule,
    ScheduleWriteRequest,
};
use super::current::current;
use super::governor::{get_worker_governor, grant_worker_governor_recovery};
use super::groups::{
    archive_group, create_group, get_group, get_group_turn, list_group_messages, list_groups,
    send_group_message, stop_group, update_group, CreateGroupRequest, ListGroupMessagesQuery,
    SendGroupMessageRequest, UpdateGroupRequest,
};
use super::hive_home_dir_for_user;
use super::home::{
    build_hive_bootstrap_response_from_dir, build_hive_channels_response_from_dir,
    build_hive_crew_response_from_dir_and_sessions, build_hive_home_response_from_dir,
    update_crew_document, update_home_document, DocumentWriteRequest,
};
use super::sessions::{
    cancel_session, dispatch, legacy_main_session, list_sessions, main_session,
    map_runtime_trace_event, pause_session, recover_daemon, resume_session, schedule_session,
    session_status, set_priority, DispatchRequest, PriorityRequest, ScheduleRequest,
};
use super::workers::{
    archive_worker, confirm_worker_introduction, create_worker, ensure_worker_dm, get_worker,
    get_worker_by_session, keep_talking_worker_introduction, list_workers,
    load_introduction_action_result, pause_worker, retry_worker_introduction,
    skip_worker_introduction, update_worker, ConfirmWorkerIntroductionRequest, CreateWorkerRequest,
    HiveWorkerSessionBindingResponse, KeepTalkingWorkerIntroductionRequest, UpdateWorkerRequest,
};
use crate::error::AppError;
use crate::routes::test_support::current_user;
use crate::AppState;

fn create_test_state() -> (AppState, PathBuf) {
    let temp_dir =
        std::env::temp_dir().join(format!("mitsuro-server-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
    let db_path = temp_dir.join("mitsuro.db");
    Database::new(&db_path).expect("database should initialize");
    let working_dir = temp_dir.join("workspace");
    std::fs::create_dir_all(&working_dir).expect("workspace should exist");

    let mut credential_store = CredentialStore::default();
    credential_store.set(ProviderId::OpenAI, "test-openai-key".to_string());

    (
        AppState {
            server_port: 3000,
            db_path: Arc::new(db_path),
            working_dir: Arc::new(working_dir.clone()),
            ai_client: None,
            tool_registry: Arc::new(ToolRegistry::new()),
            process_registry: Arc::new(ProcessRegistry::new()),
            model_registry: create_model_registry(),
            credential_store: Arc::new(RwLock::new(credential_store)),
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
            device_access: Default::default(),
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
    Preferences::for_user(
        Database::new(&state.db_path).expect("database should open"),
        user_id,
    )
    .set_current_model("gpt-5.5")
    .expect("test model preference should persist");
}

fn app_error_description(error: AppError) -> String {
    match error {
        AppError::NotFound(message) => format!("not found: {message}"),
        AppError::BadRequest(message) => format!("bad request: {message}"),
        AppError::Conflict(message) => format!("conflict: {message}"),
        AppError::ServiceUnavailable(message) => format!("service unavailable: {message}"),
        AppError::BadGateway(message) => format!("bad gateway: {message}"),
        AppError::Internal(message) => format!("internal: {message}"),
    }
}

async fn configure_test_model(state: &AppState) {
    let mut model = ModelMetadata::new("gpt-5.5", "GPT-5.5 Test", ProviderId::OpenAI)
        .with_transport(ApiFormat::OpenAIResponses);
    model.auth_scope = Some(ModelAuthScope::ApiKey);
    state.model_registry.upsert_model(model).await;
}

#[tokio::test]
async fn main_session_is_reused_and_isolated_by_owner() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");

    let Json(first) = main_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .expect("alice companion should create");
    let Json(second) = main_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .expect("alice companion should reuse");
    let Json(other) = main_session(
        State(state.clone()),
        Some(current_user("bob", state.working_dir.as_ref())),
    )
    .await
    .expect("bob companion should create");

    assert!(first.created);
    assert!(!second.created);
    assert!(other.created);
    assert_eq!(first.session_id, second.session_id);
    assert_ne!(first.session_id, other.session_id);
    assert_eq!(first.session_type, "hive");

    let Json(legacy) = legacy_main_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .expect("old route companion should reuse");
    assert_eq!(legacy.session_id, first.session_id);
    assert_eq!(legacy.session_type, "mako");

    let manager = SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    assert!(manager
        .verify_session_ownership(&first.session_id, Some("alice"))
        .expect("ownership check should succeed"));
    assert!(!manager
        .verify_session_ownership(&first.session_id, Some("bob"))
        .expect("foreign ownership check should succeed"));
}

#[tokio::test]
async fn dispatch_normalizes_model_before_persisting_session() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;

    let (_, Json(response)) = dispatch(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(DispatchRequest {
            task: "Investigate issue".to_string(),
            project_dir: None,
            model: Some("  gpt-5.5  ".to_string()),
            model_key: None,
            start_at: None,
            priority: None,
            crew_slug: None,
        }),
    )
    .await
    .unwrap_or_else(|error| panic!("dispatch should succeed: {}", app_error_description(error)));

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session = session_manager
        .get_session(&response.session_id)
        .expect("session lookup should succeed")
        .expect("session should exist");
    assert_eq!(session.model.as_deref(), Some("gpt-5.5"));
    let model_key = session
        .model_key
        .as_ref()
        .expect("dispatch should freeze an exact executable key");
    assert_eq!(model_key.provider, ProviderId::OpenAI);
    assert_eq!(model_key.model_id, "gpt-5.5");
    assert_eq!(model_key.auth_scope, Some(ModelAuthScope::ApiKey));
    assert_eq!(model_key.api_format, ApiFormat::OpenAIResponses);
}

#[test]
fn hive_home_response_surfaces_documents_and_sorted_crew() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitsuro-hive-home-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
    let crew_builder = temp_dir.join("crew").join("builder");
    let crew_reviewer = temp_dir.join("crew").join("reviewer");
    std::fs::create_dir_all(&crew_builder).expect("builder dir should exist");
    std::fs::create_dir_all(&crew_reviewer).expect("reviewer dir should exist");

    std::fs::write(
        temp_dir.join(mitsuro_core::paths::HIVE_SOUL_FILE),
        "Always Swimming.",
    )
    .expect("soul should write");
    std::fs::write(temp_dir.join("CHANNELS.md"), "Signal line").expect("channels should write");
    std::fs::write(crew_reviewer.join("IDENTITY.md"), "Reviewer").expect("reviewer identity");
    std::fs::write(crew_builder.join("SOUL.md"), "Builder soul").expect("builder soul");

    let response = build_hive_home_response_from_dir(&temp_dir);

    assert_eq!(
        response
            .soul
            .as_ref()
            .map(|document| document.file_name.as_str()),
        Some("HIVE_SOUL.md")
    );
    assert_eq!(
        response
            .channels
            .as_ref()
            .map(|document| document.file_name.as_str()),
        Some("CHANNELS.md")
    );
    assert_eq!(response.crew_count, 2);
    assert_eq!(response.crew[0].slug, "builder");
    assert_eq!(response.crew[1].slug, "reviewer");
}

#[test]
fn hive_bootstrap_response_creates_default_home_and_crew() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitsuro-hive-bootstrap-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");

    let response = build_hive_bootstrap_response_from_dir(&temp_dir)
        .unwrap_or_else(|_| panic!("bootstrap should work"));

    assert!(response.ok);
    assert!(response
        .created_files
        .iter()
        .any(|path| path == paths::HIVE_SOUL_FILE));
    assert!(response
        .created_files
        .iter()
        .any(|path| path == "crew/reviewer/SOUL.md"));
    assert_eq!(response.home.crew_count, 3);
    assert_eq!(
        response
            .home
            .soul
            .as_ref()
            .map(|document| document.file_name.as_str()),
        Some(paths::HIVE_SOUL_FILE)
    );
}

#[tokio::test]
async fn hive_channels_response_surfaces_runtime_delivery_state() {
    let (state, temp_dir) = create_test_state();
    std::fs::write(
        temp_dir.join("CHANNELS.md"),
        "# Hive Channels\n- [x] iPhone push: urgent approvals only",
    )
    .expect("channels should write");

    let response = build_hive_channels_response_from_dir(&state, &temp_dir, 0);
    assert!(response.items.iter().any(|item| item.id == "main-thread"));

    let push = response
        .items
        .iter()
        .find(|item| item.id == "iphone-push")
        .expect("push channel should exist");
    assert_eq!(push.status, "attention");
    assert_eq!(push.kind, "mobile_push");
}

#[tokio::test]
async fn update_home_document_writes_to_user_scoped_database_profile() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_home = temp_dir.join("alice-home");
    std::fs::create_dir_all(&user_home).expect("user home should exist");

    let Json(response) = update_home_document(
        State(state.clone()),
        Some(current_user("alice", &user_home)),
        Path("soul".to_string()),
        Json(DocumentWriteRequest {
            content: "Stay watchful.".to_string(),
            expected_revision: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("document update should succeed"));

    let owner = HiveProfileOwner::user("alice").expect("owner should be valid");
    let stored =
        HiveProfileStore::new(Database::new(&state.db_path).expect("database should open"))
            .load(&owner)
            .expect("profile should load")
            .expect("profile should exist");
    assert_eq!(
        stored
            .document(HiveProfileDocumentKind::Soul)
            .map(|document| document.content.as_str()),
        Some("Stay watchful.")
    );
    assert_eq!(
        response
            .soul
            .as_ref()
            .map(|document| document.file_name.as_str()),
        Some(paths::HIVE_SOUL_FILE)
    );
}

#[tokio::test]
async fn update_crew_document_rejects_invalid_slug() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_home = temp_dir.join("alice-home");
    std::fs::create_dir_all(&user_home).expect("user home should exist");

    let result = update_crew_document(
        State(state),
        Some(current_user("alice", &user_home)),
        Path(("../oops".to_string(), "soul".to_string())),
        Json(DocumentWriteRequest {
            content: "Nope".to_string(),
            expected_revision: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(AppError::BadRequest(message)) if message == "invalid crew slug"));
}

#[test]
fn hive_crew_response_merges_home_profiles_with_runtime_state() {
    let temp_dir = std::env::temp_dir().join(format!("mitsuro-hive-crew-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
    bootstrap_hive_home(&temp_dir).expect("bootstrap should work");
    let db_path = temp_dir.join("mitsuro.db");
    let db = Database::new(&db_path).expect("db should open");
    db.conn()
        .execute(
            "INSERT INTO users (id, email, license_tier) VALUES (?1, ?2, ?3)",
            ("alice", "alice@example.com", "free"),
        )
        .expect("user should insert");
    let session_manager = SessionManager::new(Database::new(&db_path).expect("db should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Crew runtime",
            None,
            Some(temp_dir.to_string_lossy().as_ref()),
            Some(temp_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("session should create");
    let sessions = session_manager
        .list_sessions_for_user_by_type(None, Some("alice"), SessionType::Hive)
        .expect("sessions should load");
    let task_store = AutonomousTaskStore::new(Database::new(&db_path).expect("db should open"));
    let delegated_store = mitsuro_core::storage::DelegatedRunStore::new(
        Database::new(&db_path).expect("db should open"),
    );
    let runtime_states =
        HiveRuntimeStateStore::new(Database::new(&db_path).expect("db should open"))
            .list_states_for_sessions(
                &sessions
                    .iter()
                    .map(|session| session.id.clone())
                    .collect::<Vec<_>>(),
            )
            .expect("runtime states should load");
    let task_id = task_store
        .create_task(&session_id, "Review", "Review current run", &[])
        .expect("task should create");
    task_store
        .claim_task(&task_id, "reviewer")
        .expect("task should claim");

    let response = build_hive_crew_response_from_dir_and_sessions(
        &temp_dir,
        &sessions,
        &runtime_states,
        &task_store,
        &delegated_store,
    )
    .unwrap_or_else(|_| panic!("crew response should build"));

    let reviewer = response
        .members
        .iter()
        .find(|member| member.slug == "reviewer")
        .expect("reviewer should exist");
    assert!(reviewer.known_to_home);
    assert_eq!(reviewer.status, "running");
    assert_eq!(reviewer.active_task_count, 1);
    assert!(reviewer.identity.is_some());
    assert!(reviewer.soul.is_some());
}

#[test]
fn user_scoped_hive_home_dir_prefers_current_user_home() {
    let temp_dir =
        std::env::temp_dir().join(format!("mitsuro-hive-user-home-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
    let user = current_user("alice", &temp_dir);

    assert_eq!(
        hive_home_dir_for_user(Some(&user)),
        paths::hive_dir_for_home(&temp_dir)
    );
}

#[tokio::test]
async fn dispatch_resolves_relative_project_dir_against_user_home() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;
    let user_root = temp_dir.join("alice-home");
    std::fs::create_dir_all(&user_root).expect("user root should exist");

    let (_, Json(response)) = dispatch(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Json(DispatchRequest {
            task: "Investigate issue".to_string(),
            project_dir: Some("repo".to_string()),
            model: None,
            model_key: None,
            start_at: None,
            priority: None,
            crew_slug: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("dispatch should succeed"));

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session = session_manager
        .get_session(&response.session_id)
        .expect("session lookup should succeed")
        .expect("session should exist");
    let expected = user_root.join("repo").to_string_lossy().to_string();
    assert_eq!(session.project_dir.as_deref(), Some(expected.as_str()));
    assert_eq!(session.working_dir.as_deref(), Some(expected.as_str()));
}

#[tokio::test]
async fn dispatch_rejects_blank_task() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let result = dispatch(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(DispatchRequest {
            task: "   ".to_string(),
            project_dir: None,
            model: None,
            model_key: None,
            start_at: None,
            priority: None,
            crew_slug: None,
        }),
    )
    .await;

    match result {
        Err(AppError::BadRequest(message)) => assert_eq!(message, "task must not be empty"),
        Ok(_) => panic!("blank dispatch should fail"),
        Err(_) => panic!("blank dispatch should fail with bad request"),
    }
}

#[tokio::test]
async fn dispatch_rejects_project_dir_outside_user_root() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let outside_root = temp_dir.join("outside");
    std::fs::create_dir_all(&user_root).expect("user root should exist");
    std::fs::create_dir_all(&outside_root).expect("outside root should exist");

    let result = dispatch(
        State(state),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Json(DispatchRequest {
            task: "Investigate issue".to_string(),
            project_dir: Some(outside_root.to_string_lossy().to_string()),
            model: None,
            model_key: None,
            start_at: None,
            priority: None,
            crew_slug: None,
        }),
    )
    .await;

    assert!(matches!(result, Err(AppError::BadRequest(_))));
}

#[tokio::test]
async fn dispatch_can_schedule_future_run() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;
    let wake_at = chrono::Utc::now() + chrono::Duration::minutes(30);

    let (_, Json(response)) = dispatch(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(DispatchRequest {
            task: "Check CI later".to_string(),
            project_dir: None,
            model: None,
            model_key: None,
            start_at: Some(wake_at.to_rfc3339()),
            priority: None,
            crew_slug: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("scheduled dispatch should succeed"));

    assert_eq!(response.status, "scheduled");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    let runtime = runtime_store
        .get_state(&response.session_id)
        .expect("runtime lookup should succeed")
        .expect("runtime should exist");
    assert_eq!(runtime.status, HiveRuntimeStateStatus::Sleeping);
    assert_eq!(runtime.sleep_reason.as_deref(), Some("scheduled"));
    assert_eq!(
        runtime.last_wake_reason.as_deref(),
        Some("scheduled_dispatch")
    );
    assert!(runtime.next_wake_at.is_some());

    let Json(summary) = current(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));

    assert_eq!(summary.status.scheduled_count, 1);
    assert_eq!(summary.status.sleeping_count, 0);
}

#[tokio::test]
async fn dispatch_persists_requested_priority() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;

    let (_, Json(response)) = dispatch(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(DispatchRequest {
            task: "Escalate production fix".to_string(),
            project_dir: None,
            model: None,
            model_key: None,
            start_at: None,
            priority: Some(HiveRunPriority::High),
            crew_slug: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("dispatch should succeed"));

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    let runtime = runtime_store
        .get_state(&response.session_id)
        .expect("runtime lookup should succeed")
        .expect("runtime should exist");
    assert_eq!(runtime.priority, HiveRunPriority::High);
}

#[tokio::test]
async fn schedule_session_can_reschedule_existing_run() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;

    let (_, Json(response)) = dispatch(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(DispatchRequest {
            task: "Investigate issue".to_string(),
            project_dir: None,
            model: None,
            model_key: None,
            start_at: None,
            priority: None,
            crew_slug: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("dispatch should succeed"));

    let wake_at = chrono::Utc::now() + chrono::Duration::hours(2);
    let Json(_) = schedule_session(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(response.session_id.clone()),
        HeaderMap::new(),
        Json(ScheduleRequest {
            start_at: wake_at.to_rfc3339(),
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("schedule should succeed"));

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    let runtime = runtime_store
        .get_state(&response.session_id)
        .expect("runtime lookup should succeed")
        .expect("runtime should exist");
    let expected_wake_at = wake_at.to_rfc3339();
    assert_eq!(runtime.status, HiveRuntimeStateStatus::Sleeping);
    assert_eq!(runtime.sleep_reason.as_deref(), Some("scheduled"));
    assert_eq!(runtime.last_wake_reason.as_deref(), Some("manual_schedule"));
    assert_eq!(
        runtime.next_wake_at.as_deref(),
        Some(expected_wake_at.as_str())
    );
}

#[tokio::test]
async fn session_status_rejects_non_hive_session() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Code Session",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("session should create");

    let result = session_status(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(session_id.clone()),
    )
    .await;

    match result {
        Err(AppError::BadRequest(message)) => {
            assert_eq!(
                message,
                format!("Session {} is not a Hive session", session_id)
            )
        }
        Ok(_) => panic!("code session should not load through hive status"),
        Err(_) => panic!("code session should fail with bad request"),
    }
}

#[tokio::test]
async fn list_sessions_only_returns_hive_sessions_with_runtime_state() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let hive_session_id = session_manager
        .create_session_for_user_with_config(
            "Hive Session",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("hive session should create");
    session_manager
        .create_session_for_user_with_config(
            "Code Session",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("code session should create");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &hive_session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some("2026-01-01T00:00:00Z"),
            Some("waiting"),
            None,
            None,
            Some("sleep"),
            HiveRunPriority::Normal,
        )
        .expect("runtime state should persist");

    let Json(sessions) = list_sessions(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("list sessions should succeed"));

    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, hive_session_id);
    assert_eq!(
        sessions[0].runtime.as_ref().map(|runtime| runtime.status),
        Some(HiveRuntimeStateStatus::Sleeping)
    );
}

#[tokio::test]
async fn current_summarizes_waiting_and_sleeping_runs() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let waiting_session_id = session_manager
        .create_session_for_user_with_config(
            "Waiting Run",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("waiting session should create");
    let sleeping_session_id = session_manager
        .create_session_for_user_with_config(
            "Sleeping Run",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("sleeping session should create");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &waiting_session_id,
            HiveRuntimeStateStatus::AwaitingInput,
            None,
            Some("approval"),
            None,
            None,
            Some("user"),
            HiveRunPriority::Normal,
        )
        .expect("waiting state should persist");
    runtime_store
        .set_state(
            &sleeping_session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some("2026-01-01T00:00:00Z"),
            Some("waiting"),
            None,
            None,
            Some("sleep"),
            HiveRunPriority::High,
        )
        .expect("sleeping state should persist");

    let Json(summary) = current(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));

    assert_eq!(summary.status.home_status, "blocked");
    assert_eq!(summary.status.waiting_count, 1);
    assert_eq!(summary.status.sleeping_count, 1);
    assert_eq!(summary.status.high_priority_count, 1);
    assert_eq!(summary.status.pending_approvals_count, 0);
    assert_eq!(
        summary.status.next_wake_at.as_deref(),
        Some("2026-01-01T00:00:00Z")
    );
    assert_eq!(summary.diagnostics.daemon.active_runtime_count, 0);
    assert_eq!(summary.diagnostics.daemon.scheduled_wake_count, 0);
    assert_eq!(summary.diagnostics.daemon.recoverable_session_count, 1);
    assert_eq!(summary.runs.len(), 2);
}

#[tokio::test]
async fn current_surfaces_pending_tool_approvals() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Approval Run",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("approval session should create");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &session_id,
            HiveRuntimeStateStatus::AwaitingInput,
            None,
            Some("approval"),
            None,
            Some("durable-run-1"),
            Some("user"),
            HiveRunPriority::High,
        )
        .expect("waiting state should persist");

    let now = chrono::Utc::now().to_rfc3339();
    let durable_db = Database::new(&state.db_path).expect("database should open");
    durable_db
        .conn()
        .execute_batch(&format!(
            "INSERT INTO hive_controllers (
                id, scope_key, user_id, session_id, status, timezone,
                max_concurrent_runs, created_at, updated_at
             ) VALUES (
                'controller-approval', 'session:{session_id}', 'alice', '{session_id}',
                'active', 'UTC', 1, '{now}', '{now}'
             );
             INSERT INTO hive_runs (
                id, controller_id, session_id, kind, objective, config_json, status,
                priority, available_at, attempt_count, max_attempts, created_at, updated_at
             ) VALUES (
                'durable-run-1', 'controller-approval', '{session_id}', 'dispatch', 'work',
                '{{}}', 'running',
                0, '{now}', 1, 3, '{now}', '{now}'
             );"
        ))
        .expect("durable approval run should insert");
    durable_db
        .conn()
        .execute(
            "INSERT INTO hive_controller_events (
                controller_id, sequence, event_type, run_id, payload_json, created_at
             ) VALUES ('controller-approval', 1, 'agentic_event', 'durable-run-1', ?1, ?2)",
            (
                serde_json::json!({
                    "type": "tool_approval_required",
                    "id": "tool-1",
                    "name": "bash",
                    "arguments": {
                        "command": "git push",
                        "cwd": "/workspace"
                    }
                })
                .to_string(),
                now.as_str(),
            ),
        )
        .expect("durable approval event should persist");

    let trace_db = Database::new(&state.db_path).expect("database should open");
    let trace_store = RuntimeTraceStore::new(&trace_db);
    let approval_event = RuntimeTraceEvent::from_loop_event(
        "trace-run-deliberately-different",
        1,
        0,
        &LoopEvent::ToolApprovalRequired {
            id: "tool-1".to_string(),
            name: "bash".to_string(),
            arguments: serde_json::json!({
                "command": "git push",
                "cwd": "/workspace"
            }),
        },
    );
    trace_store
        .append_event(&session_id, &approval_event)
        .expect("approval event should persist");

    let Json(summary) = current(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));

    assert_eq!(summary.status.pending_approvals_count, 1);
    assert_eq!(summary.approvals.len(), 1);
    assert_eq!(summary.approvals[0].session_id, session_id);
    assert_eq!(summary.approvals[0].run_id, "durable-run-1");
    assert_eq!(summary.approvals[0].tool_call_id, "tool-1");
    assert_eq!(summary.approvals[0].tool_name, "bash");
    assert_eq!(summary.approvals[0].priority, HiveRunPriority::High);
    assert_eq!(summary.diagnostics.attention_run_count, 1);
    assert_eq!(summary.diagnostics.queue_pressure, "attention");
    assert_eq!(summary.diagnostics.health_state, "attention");
    assert_eq!(
        summary.runs[0]
            .diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.kind.as_str()),
        Some("awaiting_approval")
    );
}

#[tokio::test]
async fn current_surfaces_overdue_wake_diagnostics() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Overdue Wake",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("session should create");

    let overdue_wake_at = (chrono::Utc::now() - chrono::Duration::minutes(20)).to_rfc3339();
    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some(overdue_wake_at.as_str()),
            Some("scheduled"),
            None,
            None,
            Some("scheduled_dispatch"),
            HiveRunPriority::Normal,
        )
        .expect("runtime state should persist");

    let Json(summary) = current(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));

    assert_eq!(summary.diagnostics.degraded_count, 1);
    assert_eq!(summary.diagnostics.stalled_count, 1);
    assert_eq!(summary.diagnostics.overdue_wake_count, 1);
    assert_eq!(summary.diagnostics.open_run_count, 1);
    assert_eq!(summary.diagnostics.health_state, "degraded");
    assert_eq!(summary.diagnostics.queue_pressure, "calm");
    assert_eq!(
        summary.runs[0]
            .diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.kind.as_str()),
        Some("overdue_wake")
    );
    assert_eq!(
        summary.runs[0]
            .diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.severity.as_str()),
        Some("critical")
    );
}

#[tokio::test]
async fn attention_surfaces_scheduled_start_and_delegated_completion_updates() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Scheduled Release Watch",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("session should create");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &session_id,
            HiveRuntimeStateStatus::Running,
            None,
            None,
            None,
            Some("run-1"),
            Some("scheduled_dispatch"),
            HiveRunPriority::Normal,
        )
        .expect("runtime state should persist");

    let trace_db = Database::new(&state.db_path).expect("database should open");
    let trace_store = RuntimeTraceStore::new(&trace_db);
    let started_event = RuntimeTraceEvent::from_loop_event(
        "run-1",
        1,
        0,
        &LoopEvent::TickInjected { tick_number: 1 },
    );
    trace_store
        .append_event(&session_id, &started_event)
        .expect("scheduled run should write trace");

    let delegated_store =
        DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"));
    delegated_store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "delegated-1".to_string(),
            parent_session_id: session_id.clone(),
            parent_tool_call_id: Some("tool-crew-1".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Created,
            provider: None,
            model: None,
            resumable: false,
            resumed_from_run_id: None,
            target_scope: vec![DelegatedRunScope {
                label: "release notes".to_string(),
                path: "docs/release.md".to_string(),
                kind: "file".to_string(),
            }],
        })
        .expect("delegated run should create");
    delegated_store
        .finalize_run(
            "delegated-1",
            DelegatedRunStage::Complete,
            &serde_json::json!({
                "human_review": "Release notes review completed."
            }),
            Some("Release notes review completed."),
            false,
        )
        .expect("delegated run should finalize");

    let Json(response) = attention(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Query(AttentionQuery {
            thread_session_id: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("attention should succeed"));

    assert!(response
        .items
        .iter()
        .any(|item| item.kind == "scheduled_run_started"));
    assert!(response
        .items
        .iter()
        .any(|item| item.kind == "delegated_task_completed"));
}

#[tokio::test]
async fn attention_uses_scheduled_completion_kind_for_scheduled_run_finishes() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Nightly Summary",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("session should create");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &session_id,
            HiveRuntimeStateStatus::Idle,
            None,
            None,
            None,
            None,
            Some("manual_schedule"),
            HiveRunPriority::Normal,
        )
        .expect("runtime state should persist");

    let trace_db = Database::new(&state.db_path).expect("database should open");
    let trace_store = RuntimeTraceStore::new(&trace_db);
    let started_event = RuntimeTraceEvent::from_loop_event(
        "run-2",
        1,
        0,
        &LoopEvent::TickInjected { tick_number: 1 },
    );
    trace_store
        .append_event(&session_id, &started_event)
        .expect("scheduled run should write start trace");
    let finished_event = RuntimeTraceEvent::from_loop_event(
        "run-2",
        2,
        1,
        &LoopEvent::Finished {
            session_id: session_id.clone(),
            stop_reason: LoopStopReason::Completed,
        },
    );
    trace_store
        .append_event(&session_id, &finished_event)
        .expect("scheduled run should write finish trace");

    let Json(response) = attention(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Query(AttentionQuery {
            thread_session_id: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("attention should succeed"));

    assert!(response
        .items
        .iter()
        .any(|item| item.kind == "scheduled_run_completed"));
    assert!(!response
        .items
        .iter()
        .any(|item| item.kind == "run_completed"));
}

#[tokio::test]
async fn attention_does_not_treat_future_scheduled_sleep_as_started() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Tomorrow Morning Check",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("session should create");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some(&(chrono::Utc::now() + chrono::Duration::minutes(20)).to_rfc3339()),
            Some("scheduled"),
            None,
            None,
            Some("scheduled_dispatch"),
            HiveRunPriority::Normal,
        )
        .expect("runtime state should persist");

    let Json(response) = attention(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Query(AttentionQuery {
            thread_session_id: None,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("attention should succeed"));

    assert!(!response
        .items
        .iter()
        .any(|item| item.kind == "scheduled_run_started"));
}

#[tokio::test]
async fn current_summarizes_knowledge_snapshot_health() {
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

    let Json(initial_summary) = current(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));

    assert_eq!(initial_summary.diagnostics.knowledge.scope_count, 1);
    assert_eq!(
        initial_summary.diagnostics.knowledge.missing_snapshot_count,
        1
    );
    assert_eq!(
        initial_summary.diagnostics.knowledge.stale_snapshot_count,
        0
    );

    // A legacy snapshot-titled row in `agent_memories` must not satisfy
    // knowledge health: migration 39 moved generated snapshots into the
    // dedicated `knowledge_snapshots` store.
    let memory_store =
        MemoryStore::new(Database::new(&state.db_path).expect("database should open"));
    memory_store
        .save(
            MemoryType::Project,
            CURRENT_SNAPSHOT_TITLE,
            "Legacy snapshot location",
            Some(project_dir.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("legacy memory should persist");

    let Json(legacy_summary) = current(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));
    assert_eq!(
        legacy_summary.diagnostics.knowledge.missing_snapshot_count, 1,
        "agent_memories rows must not count as knowledge snapshots"
    );

    // A real snapshot produced by the canonical refresh path is healthy.
    memory_store
        .save(
            MemoryType::Project,
            "Build notes",
            "Use the staging branch for release work",
            Some(project_dir.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("memory should persist");
    let snapshot = refresh_current_snapshot(
        &state.db_path,
        Some(project_dir.to_string_lossy().as_ref()),
        Some("alice"),
    )
    .expect("refresh should succeed")
    .expect("snapshot should materialize");

    let Json(fresh_summary) = current(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));
    assert_eq!(fresh_summary.diagnostics.knowledge.scope_count, 1);
    assert_eq!(
        fresh_summary.diagnostics.knowledge.missing_snapshot_count,
        0
    );
    assert_eq!(fresh_summary.diagnostics.knowledge.stale_snapshot_count, 0);
    let latest_snapshot_at = fresh_summary
        .diagnostics
        .knowledge
        .latest_snapshot_at
        .expect("snapshot timestamp should be reported");
    assert!(
        chrono::DateTime::parse_from_rfc3339(&latest_snapshot_at).is_ok(),
        "latest_snapshot_at should be RFC 3339, got {latest_snapshot_at}"
    );

    // Backdate the stored snapshot using the store's native SQLite timestamp
    // format; newer session/report signals must mark the scope stale.
    Database::new(&state.db_path)
        .expect("database should open")
        .conn()
        .execute(
            "UPDATE knowledge_snapshots SET updated_at = '2025-01-01 00:00:00' WHERE id = ?1",
            [snapshot.id.as_str()],
        )
        .expect("snapshot should backdate");

    let report_store =
        ReportStore::new(Database::new(&state.db_path).expect("database should open"));
    report_store
        .create_report(CreateReportInput {
            title: "Knowledge refresh",
            session_id: session_id.as_str(),
            project_dir: Some(project_dir.to_string_lossy().as_ref()),
            report_root: None,
            content: "Updated project findings",
            summary: "Updated project findings",
            tags: &[],
            sources: &[],
            scope: ReportScope::owner_shared(),
        })
        .expect("report should persist");

    let Json(updated_summary) = current(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));

    assert_eq!(updated_summary.diagnostics.knowledge.scope_count, 1);
    assert_eq!(
        updated_summary.diagnostics.knowledge.missing_snapshot_count,
        0
    );
    assert_eq!(
        updated_summary.diagnostics.knowledge.stale_snapshot_count,
        1
    );
}

#[tokio::test]
async fn recover_daemon_only_recovers_owned_sessions() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    configure_test_model(&state).await;

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let alice_session_id = session_manager
        .create_session_for_user_with_config(
            "Alice Sleeping",
            Some("gpt-5.5"),
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("alice session should create");
    let bob_session_id = session_manager
        .create_session_for_user_with_config(
            "Bob Sleeping",
            Some("gpt-5.5"),
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("bob"),
            None,
            SessionType::Hive,
        )
        .expect("bob session should create");

    let wake_at = (chrono::Utc::now() + chrono::Duration::minutes(30)).to_rfc3339();
    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &alice_session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some(wake_at.as_str()),
            Some("scheduled"),
            None,
            None,
            Some("dispatch"),
            HiveRunPriority::Normal,
        )
        .expect("alice runtime state should persist");
    runtime_store
        .set_state(
            &bob_session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some(wake_at.as_str()),
            Some("scheduled"),
            None,
            None,
            Some("dispatch"),
            HiveRunPriority::Normal,
        )
        .expect("bob runtime state should persist");

    let Json(response) = recover_daemon(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
    )
    .await
    .unwrap_or_else(|_| panic!("recover should succeed"));

    assert!(response.ok);
    assert_eq!(response.recovered_count, 1);

    let Json(alice_summary) = current(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("alice current should succeed"));
    assert_eq!(alice_summary.diagnostics.daemon.scheduled_wake_count, 1);
    assert_eq!(
        alice_summary.diagnostics.daemon.recoverable_session_count,
        1
    );

    let Json(bob_summary) = current(
        State(state.clone()),
        Some(current_user("bob", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("bob current should succeed"));
    assert_eq!(bob_summary.diagnostics.daemon.scheduled_wake_count, 0);
    assert_eq!(bob_summary.diagnostics.daemon.recoverable_session_count, 1);
}

#[tokio::test]
async fn set_priority_updates_runtime_state_and_current_ordering() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let first_session_id = session_manager
        .create_session_for_user_with_config(
            "First Run",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("first session should create");
    let second_session_id = session_manager
        .create_session_for_user_with_config(
            "Second Run",
            None,
            Some(state.working_dir.to_string_lossy().as_ref()),
            Some(state.working_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("second session should create");

    let runtime_store =
        HiveRuntimeStateStore::new(Database::new(&state.db_path).expect("database should open"));
    runtime_store
        .set_state(
            &first_session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some("2026-01-01T00:00:00Z"),
            Some("scheduled"),
            None,
            None,
            Some("dispatch"),
            HiveRunPriority::Normal,
        )
        .expect("first runtime state should persist");
    runtime_store
        .set_state(
            &second_session_id,
            HiveRuntimeStateStatus::Sleeping,
            Some("2026-01-01T01:00:00Z"),
            Some("scheduled"),
            None,
            None,
            Some("dispatch"),
            HiveRunPriority::Low,
        )
        .expect("second runtime state should persist");

    let Json(_) = set_priority(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        Path(second_session_id.clone()),
        HeaderMap::new(),
        Json(PriorityRequest {
            priority: HiveRunPriority::High,
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("priority update should succeed"));

    let runtime = runtime_store
        .get_state(&second_session_id)
        .expect("runtime lookup should succeed")
        .expect("runtime should exist");
    assert_eq!(runtime.priority, HiveRunPriority::High);

    let Json(summary) = current(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
    )
    .await
    .unwrap_or_else(|_| panic!("current should succeed"));

    assert_eq!(summary.status.high_priority_count, 1);
    assert_eq!(
        summary.runs.first().map(|run| run.session_id.as_str()),
        Some(second_session_id.as_str())
    );
}

#[tokio::test]
async fn session_status_includes_resolved_cadence() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let project_dir = user_root.join("repo");
    std::fs::create_dir_all(project_dir.join(".mitsuro")).expect("project settings dir");
    std::fs::write(
        project_dir.join(".mitsuro").join("settings.json"),
        r#"{ "hive": { "tick_interval_secs": 15, "max_ticks": 50 } }"#,
    )
    .expect("project settings should write");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Configured Run",
            None,
            Some(project_dir.to_string_lossy().as_ref()),
            Some(project_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("configured session should create");

    let Json(status) = session_status(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
        Path(session_id),
    )
    .await
    .unwrap_or_else(|_| panic!("session status should succeed"));

    assert_eq!(status.cadence.tick_interval_secs, 15);
    assert_eq!(status.cadence.max_ticks, 50);
}

#[test]
fn map_runtime_trace_event_skips_malformed_payload() {
    let event = RuntimeTraceEvent {
        run_id: "run-1".to_string(),
        sequence: 7,
        turn: 0,
        event_type: "user_message".to_string(),
        call_kind: None,
        operation: None,
        payload: serde_json::json!({ "level": "info" }),
        failure_category: None,
        stop_reason: None,
        created_at: "2026-01-01T00:00:00Z".to_string(),
    };

    assert!(map_runtime_trace_event(event).is_none());
}

fn create_group_request(title: &str, member_worker_ids: Vec<String>) -> CreateGroupRequest {
    CreateGroupRequest {
        title: title.to_string(),
        execution_mode: None,
        max_rounds: None,
        max_member_messages_per_turn: None,
        parallelism: None,
        context_window_messages: None,
        default_assignee_worker_id: None,
        member_worker_ids,
    }
}

fn empty_update_group_request() -> UpdateGroupRequest {
    UpdateGroupRequest {
        title: None,
        execution_mode: None,
        max_rounds: None,
        max_member_messages_per_turn: None,
        parallelism: None,
        context_window_messages: None,
        default_assignee_worker_id: None,
        member_worker_ids: None,
    }
}

/// Two Workers owned by the given user, returning their ids.
async fn seed_two_workers(state: &AppState, user_id: &str) -> Vec<String> {
    let mut worker_ids = Vec::new();
    for slug in ["researcher", "builder"] {
        let (_, Json(created)) = create_worker(
            State(state.clone()),
            Some(current_user(user_id, state.working_dir.as_ref())),
            Json(create_worker_request(slug)),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "worker create should succeed: {}",
                app_error_description(error)
            )
        });
        worker_ids.push(created.worker.id.clone());
    }
    worker_ids
}

#[tokio::test]
async fn groups_crud_lifecycle_is_exact_owner_scoped() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let bob = || Some(current_user("bob", state.working_dir.as_ref()));
    let worker_ids = seed_two_workers(&state, "alice").await;

    // Creation validates members: empty and cross-owner picks fail closed.
    assert!(matches!(
        create_group(
            State(state.clone()),
            alice(),
            Json(create_group_request("Empty", Vec::new())),
        )
        .await,
        Err(AppError::BadRequest(_))
    ));
    assert!(matches!(
        create_group(
            State(state.clone()),
            bob(),
            Json(create_group_request("Stolen", worker_ids.clone())),
        )
        .await,
        Err(AppError::BadRequest(_))
    ));

    let (created_status, Json(created)) = create_group(
        State(state.clone()),
        alice(),
        Json(CreateGroupRequest {
            parallelism: Some(2),
            ..create_group_request("Release Room", worker_ids.clone())
        }),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "group create should succeed: {}",
            app_error_description(error)
        )
    });
    assert_eq!(created_status, axum::http::StatusCode::CREATED);
    assert_eq!(created.group.title, "Release Room");
    assert_eq!(created.group.execution_mode, "workbench");
    assert_eq!(created.group.parallelism, 2);
    assert_eq!(created.group.members.len(), 2);
    assert_eq!(created.group.members[0].slug, "researcher");
    assert!(created.active_turn.is_none());
    let group_id = created.group.id.clone();

    // Reads and mutations are exact-owner scoped.
    assert!(matches!(
        get_group(State(state.clone()), bob(), Path(group_id.clone())).await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        update_group(
            State(state.clone()),
            bob(),
            Path(group_id.clone()),
            Json(empty_update_group_request()),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        archive_group(
            State(state.clone()),
            bob(),
            Path(group_id.clone()),
            HeaderMap::new(),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    let Json(bob_list) = list_groups(State(state.clone()), bob())
        .await
        .expect("bob list should succeed");
    assert!(bob_list.groups.is_empty());
    let Json(alice_list) = list_groups(State(state.clone()), alice())
        .await
        .expect("alice list should succeed");
    assert_eq!(alice_list.groups.len(), 1);

    // Update: rename, switch to direct with an assignee, then remove the
    // assignee through membership replacement.
    let Json(updated) = update_group(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        Json(UpdateGroupRequest {
            title: Some("War Room".to_string()),
            execution_mode: Some(
                serde_json::from_value(serde_json::json!("direct")).expect("mode should parse"),
            ),
            default_assignee_worker_id: Some(worker_ids[1].clone()),
            ..empty_update_group_request()
        }),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "group update should succeed: {}",
            app_error_description(error)
        )
    });
    assert_eq!(updated.group.title, "War Room");
    assert_eq!(updated.group.execution_mode, "direct");
    assert_eq!(
        updated.group.default_assignee_worker_id.as_deref(),
        Some(worker_ids[1].as_str())
    );

    let Json(shrunk) = update_group(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        Json(UpdateGroupRequest {
            member_worker_ids: Some(vec![worker_ids[0].clone()]),
            ..empty_update_group_request()
        }),
    )
    .await
    .expect("membership replacement should succeed");
    assert_eq!(shrunk.group.members.len(), 1);
    assert!(shrunk.group.default_assignee_worker_id.is_none());

    // A PATCH is one mutation boundary: invalid settings cannot commit an
    // otherwise-valid roster replacement before the request is rejected.
    assert!(matches!(
        update_group(
            State(state.clone()),
            alice(),
            Path(group_id.clone()),
            Json(UpdateGroupRequest {
                title: Some("   ".to_string()),
                member_worker_ids: Some(vec![worker_ids[1].clone()]),
                ..empty_update_group_request()
            }),
        )
        .await,
        Err(AppError::BadRequest(_))
    ));
    let Json(after_rejected_update) =
        get_group(State(state.clone()), alice(), Path(group_id.clone()))
            .await
            .expect("rejected update must leave the group readable");
    assert_eq!(after_rejected_update.group.title, "War Room");
    assert_eq!(after_rejected_update.group.members.len(), 1);
    assert_eq!(
        after_rejected_update.group.members[0].worker_id,
        worker_ids[0]
    );

    // Archive is lifecycle control and fails closed without the daemon. Seed
    // the read-only archived projection directly after proving that boundary.
    let archive_error = archive_group(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        HeaderMap::new(),
    )
    .await
    .expect_err("embedded archive must require the daemon control plane");
    assert!(app_error_description(archive_error).contains("daemon control plane"));
    mitsuro_core::storage::HiveGroupStore::new(Database::new(&state.db_path).unwrap())
        .set_status(&group_id, mitsuro_core::storage::HiveGroupStatus::Archived)
        .unwrap();
    let Json(after_archive) = list_groups(State(state.clone()), alice())
        .await
        .expect("list should succeed");
    assert!(after_archive.groups.is_empty());
    assert!(matches!(
        update_group(
            State(state.clone()),
            alice(),
            Path(group_id.clone()),
            Json(empty_update_group_request()),
        )
        .await,
        Err(AppError::Conflict(_))
    ));
}

#[tokio::test]
async fn group_messages_and_turns_read_with_exact_ownership_and_cursors() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let bob = || Some(current_user("bob", state.working_dir.as_ref()));
    let worker_ids = seed_two_workers(&state, "alice").await;
    let (_, Json(created)) = create_group(
        State(state.clone()),
        alice(),
        Json(create_group_request("Reading Room", worker_ids.clone())),
    )
    .await
    .expect("group create should succeed");
    let group_id = created.group.id.clone();

    // Seed a small timeline and one turn directly through the store.
    let store = mitsuro_core::storage::HiveGroupStore::new(Database::new(&state.db_path).unwrap());
    let trigger = store
        .append_message(&mitsuro_core::storage::NewHiveGroupMessage::user(
            &group_id,
            "hello room",
        ))
        .unwrap();
    store
        .append_message(&mitsuro_core::storage::NewHiveGroupMessage::worker(
            &group_id,
            &worker_ids[0],
            "hello back",
        ))
        .unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    let turn = mitsuro_core::storage::HiveGroupTurn {
        id: uuid::Uuid::new_v4().to_string(),
        group_id: group_id.clone(),
        trigger_message_id: trigger.id.clone(),
        execution_mode: created_mode(),
        policy: mitsuro_core::storage::HiveGroupTurnPolicy {
            max_rounds: 3,
            max_member_messages_per_turn: 2,
            parallelism: 3,
            context_window_messages: 24,
        },
        speaker_plan: worker_ids.clone(),
        next_speaker_index: 0,
        status: mitsuro_core::storage::HiveGroupTurnStatus::Running,
        member_outcomes: None,
        started_at: now.clone(),
        finished_at: None,
        created_at: now.clone(),
        updated_at: now,
    };
    mitsuro_core::storage::hive_groups::insert_turn_with_conn(
        Database::new(&state.db_path).unwrap().conn(),
        &turn,
    )
    .unwrap();

    // A limited backlog advances only through the returned page. Returning a
    // table-wide high-water mark here would make the event tail skip seq 2.
    let Json(first_page) = list_group_messages(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        Query(ListGroupMessagesQuery {
            after_seq: Some(0),
            limit: Some(1),
        }),
    )
    .await
    .expect("limited message list should succeed");
    assert_eq!(first_page.messages.len(), 1);
    assert_eq!(first_page.messages[0].seq, 1);
    assert_eq!(first_page.latest_seq, 1);

    // Cursor pagination returns strictly-after rows.
    let Json(page) = list_group_messages(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        Query(ListGroupMessagesQuery {
            after_seq: Some(1),
            limit: Some(10),
        }),
    )
    .await
    .expect("message list should succeed");
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].seq, 2);
    assert_eq!(page.latest_seq, 2);

    // Ownership: bob sees neither messages nor turns.
    assert!(matches!(
        list_group_messages(
            State(state.clone()),
            bob(),
            Path(group_id.clone()),
            Query(ListGroupMessagesQuery {
                after_seq: None,
                limit: None,
            }),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        get_group_turn(
            State(state.clone()),
            bob(),
            Path((group_id.clone(), turn.id.clone())),
        )
        .await,
        Err(AppError::NotFound(_))
    ));

    let Json(turn_view) = get_group_turn(
        State(state.clone()),
        alice(),
        Path((group_id.clone(), turn.id.clone())),
    )
    .await
    .expect("turn read should succeed");
    assert_eq!(turn_view.status, "running");
    assert_eq!(turn_view.speaker_plan, worker_ids);

    // A turn id from another group is not addressable through this group.
    let (_, Json(other)) = create_group(
        State(state.clone()),
        alice(),
        Json(create_group_request("Other Room", worker_ids.clone())),
    )
    .await
    .expect("second group create should succeed");
    assert!(matches!(
        get_group_turn(
            State(state.clone()),
            alice(),
            Path((other.group.id.clone(), turn.id.clone())),
        )
        .await,
        Err(AppError::NotFound(_))
    ));

    // The group detail surfaces the active turn.
    let Json(detail) = get_group(State(state.clone()), alice(), Path(group_id.clone()))
        .await
        .expect("group detail should succeed");
    assert_eq!(
        detail.active_turn.as_ref().map(|turn| turn.id.as_str()),
        Some(turn.id.as_str())
    );
    assert_eq!(
        detail.group.active_turn_id.as_deref(),
        Some(turn.id.as_str())
    );
}

fn created_mode() -> mitsuro_core::storage::HiveGroupExecutionMode {
    mitsuro_core::storage::HiveGroupExecutionMode::Workbench
}

#[tokio::test]
async fn group_sends_fail_closed_without_the_daemon_but_prepare_dm_lanes() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let bob = || Some(current_user("bob", state.working_dir.as_ref()));
    let worker_ids = seed_two_workers(&state, "alice").await;
    let (_, Json(created)) = create_group(
        State(state.clone()),
        alice(),
        Json(create_group_request("Send Room", worker_ids.clone())),
    )
    .await
    .expect("group create should succeed");
    let group_id = created.group.id.clone();

    // Bad inputs fail before any daemon interaction.
    assert!(matches!(
        send_group_message(
            State(state.clone()),
            alice(),
            Path(group_id.clone()),
            HeaderMap::new(),
            Json(SendGroupMessageRequest {
                message: "   ".into(),
                mentions_override: None,
            }),
        )
        .await,
        Err(AppError::BadRequest(_))
    ));
    let mut oversized_key = HeaderMap::new();
    oversized_key.insert(
        "idempotency-key",
        axum::http::HeaderValue::from_str(&"k".repeat(300)).unwrap(),
    );
    assert!(matches!(
        send_group_message(
            State(state.clone()),
            alice(),
            Path(group_id.clone()),
            oversized_key,
            Json(SendGroupMessageRequest {
                message: "hello".into(),
                mentions_override: None,
            }),
        )
        .await,
        Err(AppError::BadRequest(_))
    ));

    // Ownership before side effects.
    assert!(matches!(
        send_group_message(
            State(state.clone()),
            bob(),
            Path(group_id.clone()),
            HeaderMap::new(),
            Json(SendGroupMessageRequest {
                message: "hello".into(),
                mentions_override: None,
            }),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        stop_group(
            State(state.clone()),
            bob(),
            Path(group_id.clone()),
            HeaderMap::new(),
        )
        .await,
        Err(AppError::NotFound(_))
    ));

    // Run-triggering sends fail closed onto the daemon control plane in the
    // embedded test state instead of silently running in-process.
    let send_error = send_group_message(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        HeaderMap::new(),
        Json(SendGroupMessageRequest {
            message: "@researcher take a look".into(),
            mentions_override: None,
        }),
    )
    .await
    .expect_err("embedded state has no daemon control plane");
    assert!(
        app_error_description(send_error).contains("daemon control plane"),
        "send must fail closed onto the daemon"
    );
    let stop_error = stop_group(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        HeaderMap::new(),
    )
    .await
    .expect_err("embedded state has no daemon control plane");
    assert!(app_error_description(stop_error).contains("daemon control plane"));

    // The send prepared every member's DM lane before reaching the daemon
    // boundary, so a daemon-backed retry has controllers to queue on.
    let worker_store =
        mitsuro_core::storage::HiveWorkerStore::new(Database::new(&state.db_path).unwrap());
    for worker_id in &worker_ids {
        let worker = worker_store.get(worker_id).unwrap().unwrap();
        assert!(
            worker.dm_session_id.is_some(),
            "send should ensure the member DM lane"
        );
    }

    // Archived groups refuse sends outright. The embedded route cannot
    // perform lifecycle control without the daemon, so seed the projection.
    let archive_error = archive_group(
        State(state.clone()),
        alice(),
        Path(group_id.clone()),
        HeaderMap::new(),
    )
    .await
    .expect_err("embedded archive must require the daemon control plane");
    assert!(app_error_description(archive_error).contains("daemon control plane"));
    mitsuro_core::storage::HiveGroupStore::new(Database::new(&state.db_path).unwrap())
        .set_status(&group_id, mitsuro_core::storage::HiveGroupStatus::Archived)
        .unwrap();
    assert!(matches!(
        send_group_message(
            State(state.clone()),
            alice(),
            Path(group_id.clone()),
            HeaderMap::new(),
            Json(SendGroupMessageRequest {
                message: "hello".into(),
                mentions_override: None,
            }),
        )
        .await,
        Err(AppError::Conflict(_))
    ));
}

fn create_worker_request(slug: &str) -> CreateWorkerRequest {
    CreateWorkerRequest {
        slug: slug.to_string(),
        display_name: None,
        avatar_color: None,
        model: None,
        model_key: None,
        permission_mode: None,
        autonomy: None,
        heartbeat_interval_secs: None,
        identity: None,
        soul: None,
    }
}

#[tokio::test]
async fn every_public_worker_create_route_requires_assistant_first_idempotency() {
    for (label, legacy_wire, path) in [
        ("canonical collection", false, "/workers"),
        (
            "canonical introductions alias",
            false,
            "/workers/introductions",
        ),
        ("legacy collection", true, "/workers"),
        ("legacy introductions alias", true, "/workers/introductions"),
    ] {
        let (state, _temp_dir) = create_test_state();
        let router = if legacy_wire {
            super::legacy_router()
        } else {
            super::router()
        };
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({ "slug": "route-contract" }).to_string(),
            ))
            .expect("route contract request should build");

        let response = router
            .with_state(state.clone())
            .oneshot(request)
            .await
            .expect("Worker create route should respond");
        let status = response.status();
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("Worker create rejection body should load");
        let error: serde_json::Value =
            serde_json::from_slice(&body).expect("Worker create rejection should be JSON");
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{label} must require the assistant-first Idempotency-Key"
        );
        assert_eq!(
            error["error"], "Idempotency-Key is required when creating and meeting a Worker",
            "{label} must route through the atomic assistant-first handler"
        );

        let db = Database::new(&state.db_path).expect("route contract database should open");
        let counts = db
            .conn()
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM hive_workers),
                    (SELECT COUNT(*) FROM hive_worker_introductions),
                    (SELECT COUNT(*) FROM sessions WHERE session_type = 'hive'),
                    (SELECT COUNT(*) FROM messages)",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .expect("route contract counts should load");
        assert_eq!(
            counts,
            (0, 0, 0, 0),
            "{label} must not quiet-create a Worker, DM, Introduction, or fabricated user row"
        );
    }
}

fn empty_update_worker_request() -> UpdateWorkerRequest {
    UpdateWorkerRequest {
        expected_revision: 1,
        display_name: None,
        avatar_color: None,
        model: None,
        model_key: None,
        permission_mode: None,
        autonomy: None,
        heartbeat_interval_secs: None,
        identity: None,
        soul: None,
    }
}

fn introduction_action_headers(key: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("idempotency-key", HeaderValue::from_static(key));
    headers
}

fn worker_schedule_request(worker_id: Option<&str>) -> ScheduleWriteRequest {
    serde_json::from_value(serde_json::json!({
        "title": "Worker check-in",
        "objective": "Review the current Worker objective",
        "recurrence": {
            "kind": "once",
            "at": (chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339(),
        },
        "timezone": "UTC",
        "worker_id": worker_id,
    }))
    .expect("schedule request fixture should deserialize")
}

fn schedule_mutation_headers(key: &'static str) -> HeaderMap {
    let mut headers = introduction_action_headers(key);
    headers.insert("if-match", HeaderValue::from_static("\"0\""));
    headers
}

fn worker_dm_control_snapshot(state: &AppState, session_id: &str) -> (i64, i64, i64, i64) {
    Database::new(&state.db_path)
        .expect("database should open")
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM sessions WHERE id = ?1),
                 (SELECT COUNT(*) FROM hive_runtime_state WHERE session_id = ?1),
                 (SELECT COUNT(*) FROM hive_schedules schedule
                    JOIN hive_controllers controller ON controller.id = schedule.controller_id
                   WHERE controller.session_id = ?1),
                 (SELECT COUNT(*) FROM hive_controller_events event
                    JOIN hive_controllers controller ON controller.id = event.controller_id
                   WHERE controller.session_id = ?1)",
            [session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("control snapshot should load")
}

fn assert_worker_control_conflict<T>(result: Result<T, AppError>, worker_id: &str) {
    match result {
        Err(AppError::Conflict(message)) => {
            assert!(message.contains(worker_id));
        }
        _ => panic!("Worker DM generic control must fail with a conflict"),
    }
}

#[tokio::test]
async fn generic_session_controls_reject_worker_dm_and_hide_group_lane_without_mutation() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("control-fence")),
    )
    .await
    .expect("Worker fixture should create");
    let worker_id = created.worker.id;
    let Json(dm) = ensure_worker_dm(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("Worker DM fixture should create");
    let before = worker_dm_control_snapshot(&state, &dm.session_id);

    assert_worker_control_conflict(
        pause_session(
            State(state.clone()),
            alice(),
            Path(dm.session_id.clone()),
            HeaderMap::new(),
        )
        .await,
        &worker_id,
    );
    assert_worker_control_conflict(
        resume_session(
            State(state.clone()),
            alice(),
            Path(dm.session_id.clone()),
            HeaderMap::new(),
        )
        .await,
        &worker_id,
    );
    assert_worker_control_conflict(
        schedule_session(
            State(state.clone()),
            alice(),
            Path(dm.session_id.clone()),
            HeaderMap::new(),
            Json(ScheduleRequest {
                start_at: (chrono::Utc::now() + chrono::Duration::hours(2)).to_rfc3339(),
            }),
        )
        .await,
        &worker_id,
    );
    assert_worker_control_conflict(
        cancel_session(
            State(state.clone()),
            alice(),
            Path(dm.session_id.clone()),
            HeaderMap::new(),
        )
        .await,
        &worker_id,
    );
    assert_eq!(worker_dm_control_snapshot(&state, &dm.session_id), before);

    let (_, Json(group)) = create_group(
        State(state.clone()),
        alice(),
        Json(create_group_request(
            "Hidden control room",
            vec![worker_id.clone()],
        )),
    )
    .await
    .expect("group fixture should create");
    let hidden_lane =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"))
            .create_session_for_user_with_config(
                "Hidden Worker lane",
                None,
                None,
                None,
                WorkspaceMode::Neutral,
                Some("alice"),
                None,
                SessionType::Hive,
            )
            .expect("hidden lane session should create");
    HiveGroupWorkerLaneStore::new(Database::new(&state.db_path).expect("database should open"))
        .upsert(&NewHiveGroupWorkerLane::new(
            group.group.id,
            worker_id,
            hidden_lane.clone(),
        ))
        .expect("hidden group lane should bind");
    let hidden_before = worker_dm_control_snapshot(&state, &hidden_lane);
    assert!(matches!(
        pause_session(
            State(state.clone()),
            alice(),
            Path(hidden_lane.clone()),
            HeaderMap::new(),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        create_schedule(
            State(state.clone()),
            alice(),
            Path(hidden_lane.clone()),
            HeaderMap::new(),
            Json(worker_schedule_request(None)),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        cancel_session(
            State(state.clone()),
            alice(),
            Path(hidden_lane.clone()),
            HeaderMap::new(),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert_eq!(
        worker_dm_control_snapshot(&state, &hidden_lane),
        hidden_before
    );
}

#[tokio::test]
async fn worker_dm_schedule_controls_require_exact_worker_binding() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(CreateWorkerRequest {
            model: Some("gpt-5.5".into()),
            ..create_worker_request("calendar-fence")
        }),
    )
    .await
    .expect("Worker fixture should create");
    let worker_id = created.worker.id;
    let Json(dm) = ensure_worker_dm(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("Worker DM fixture should create");

    assert_worker_control_conflict(
        create_schedule(
            State(state.clone()),
            alice(),
            Path(dm.session_id.clone()),
            HeaderMap::new(),
            Json(worker_schedule_request(None)),
        )
        .await,
        &worker_id,
    );
    assert_worker_control_conflict(
        create_schedule(
            State(state.clone()),
            alice(),
            Path(dm.session_id.clone()),
            HeaderMap::new(),
            Json(worker_schedule_request(Some("different-worker"))),
        )
        .await,
        &worker_id,
    );
    assert!(matches!(
        create_schedule(
            State(state.clone()),
            alice(),
            Path(dm.session_id.clone()),
            HeaderMap::new(),
            Json(worker_schedule_request(Some(&worker_id))),
        )
        .await,
        Err(AppError::BadGateway(message)) if message.contains("daemon control plane")
    ));

    let controller_id = format!("calendar-controller-{worker_id}");
    let now = chrono::Utc::now().to_rfc3339();
    let recurrence = serde_json::json!({
        "kind": "once",
        "at": (chrono::Utc::now() + chrono::Duration::hours(3)).to_rfc3339(),
    })
    .to_string();
    let db = Database::new(&state.db_path).expect("database should open");
    db.conn()
        .execute(
            "INSERT INTO hive_controllers (
                 id, scope_key, user_id, session_id, status, timezone,
                 max_concurrent_runs, created_at, updated_at, worker_id
             ) VALUES (?1, ?2, 'alice', ?3, 'active', 'UTC', 1, ?4, ?4, ?5)",
            (
                controller_id.as_str(),
                format!("worker-calendar:{worker_id}"),
                dm.session_id.as_str(),
                now.as_str(),
                worker_id.as_str(),
            ),
        )
        .expect("Worker controller fixture should persist");
    for (schedule_id, bound_worker_id) in [
        ("typed-worker-schedule", Some(worker_id.as_str())),
        ("untyped-worker-schedule", None),
    ] {
        db.conn()
            .execute(
                "INSERT INTO hive_schedules (
                     id, controller_id, title, summary, objective, recurrence_kind,
                     recurrence_json, timezone, gap_policy, fold_policy, status,
                     priority, misfire_policy, misfire_grace_secs, catch_up_limit,
                     overlap_policy, max_attempts, retry_base_secs, retry_max_secs,
                     retry_jitter, revision, created_by, created_at, updated_at,
                     worker_id
                 ) VALUES (
                     ?1, ?2, 'Worker check-in', '', 'Review current objective', 'once',
                     ?3, 'UTC', 'shift_forward', 'first', 'enabled', 0,
                     'fire_once', 300, 1, 'queue_one', 3, 15, 900, 'full', 0,
                     'alice', ?4, ?4, ?5
                 )",
                (
                    schedule_id,
                    controller_id.as_str(),
                    recurrence.as_str(),
                    now.as_str(),
                    bound_worker_id,
                ),
            )
            .expect("schedule fixture should persist");
    }
    drop(db);

    assert_worker_control_conflict(
        pause_schedule(
            State(state.clone()),
            alice(),
            Path((dm.session_id.clone(), "untyped-worker-schedule".into())),
            schedule_mutation_headers("pause-untyped-worker-schedule"),
        )
        .await,
        &worker_id,
    );
    for result in [
        pause_schedule(
            State(state.clone()),
            alice(),
            Path((dm.session_id.clone(), "typed-worker-schedule".into())),
            schedule_mutation_headers("pause-typed-worker-schedule"),
        )
        .await,
        resume_schedule(
            State(state.clone()),
            alice(),
            Path((dm.session_id.clone(), "typed-worker-schedule".into())),
            schedule_mutation_headers("resume-typed-worker-schedule"),
        )
        .await,
        cancel_schedule(
            State(state.clone()),
            alice(),
            Path((dm.session_id.clone(), "typed-worker-schedule".into())),
            schedule_mutation_headers("cancel-typed-worker-schedule"),
        )
        .await,
    ] {
        assert!(matches!(
            result,
            Err(AppError::BadGateway(message)) if message.contains("daemon control plane")
        ));
    }
    assert_worker_control_conflict(
        replace_schedule(
            State(state.clone()),
            alice(),
            Path((dm.session_id.clone(), "typed-worker-schedule".into())),
            schedule_mutation_headers("replace-mismatched-worker-schedule"),
            Json(worker_schedule_request(Some("different-worker"))),
        )
        .await,
        &worker_id,
    );
    assert!(matches!(
        replace_schedule(
            State(state.clone()),
            alice(),
            Path((dm.session_id.clone(), "typed-worker-schedule".into())),
            schedule_mutation_headers("replace-typed-worker-schedule"),
            Json(worker_schedule_request(Some(&worker_id))),
        )
        .await,
        Err(AppError::BadGateway(message)) if message.contains("daemon control plane")
    ));

    let db = Database::new(&state.db_path).expect("database should reopen");
    let unchanged: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_schedules
             WHERE controller_id = ?1 AND status = 'enabled' AND revision = 0",
            [&controller_id],
            |row| row.get(0),
        )
        .expect("schedule state should load");
    assert_eq!(
        unchanged, 2,
        "failed or rejected controls must not mutate schedules"
    );
}

#[tokio::test]
async fn worker_introduction_action_routes_require_replay_keys_and_hide_foreign_workers() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let bob = || Some(current_user("bob", state.working_dir.as_ref()));

    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("introduction-guard")),
    )
    .await
    .expect("legacy Worker fixture should create");
    let worker_id = created.worker.id;

    assert!(matches!(
        retry_worker_introduction(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));
    assert!(matches!(
        skip_worker_introduction(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));
    assert!(matches!(
        confirm_worker_introduction(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
            Json(ConfirmWorkerIntroductionRequest {
                proposal_id: "proposal-1".into(),
                proposal_revision: 1,
                selected_facts: vec![],
            }),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));
    assert!(matches!(
        keep_talking_worker_introduction(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
            Json(KeepTalkingWorkerIntroductionRequest {
                proposal_id: "proposal-1".into(),
                proposal_revision: 1,
            }),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));

    // Ownership is resolved before the daemon call, so a foreign caller gets
    // the same not-found surface whether it guesses retry or skip.
    assert!(matches!(
        retry_worker_introduction(
            State(state.clone()),
            bob(),
            Path(worker_id.clone()),
            introduction_action_headers("foreign-retry"),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        skip_worker_introduction(
            State(state.clone()),
            bob(),
            Path(worker_id.clone()),
            introduction_action_headers("foreign-skip"),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        confirm_worker_introduction(
            State(state.clone()),
            bob(),
            Path(worker_id.clone()),
            introduction_action_headers("foreign-confirm"),
            Json(ConfirmWorkerIntroductionRequest {
                proposal_id: "proposal-1".into(),
                proposal_revision: 1,
                selected_facts: vec![],
            }),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        keep_talking_worker_introduction(
            State(state.clone()),
            bob(),
            Path(worker_id.clone()),
            introduction_action_headers("foreign-keep"),
            Json(KeepTalkingWorkerIntroductionRequest {
                proposal_id: "proposal-1".into(),
                proposal_revision: 1,
            }),
        )
        .await,
        Err(AppError::NotFound(_))
    ));

    HiveWorkerStore::new(Database::new(&state.db_path).expect("database should open"))
        .set_status(
            &worker_id,
            mitsuro_core::storage::HiveWorkerStatus::Archived,
        )
        .expect("fixture should archive");
    assert!(matches!(
        skip_worker_introduction(
            State(state.clone()),
            alice(),
            Path(worker_id),
            introduction_action_headers("archived-skip"),
        )
        .await,
        Err(AppError::Conflict(_))
    ));
}

#[tokio::test]
async fn worker_introduction_action_result_must_match_durable_run_and_eligibility() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("result-contract")),
    )
    .await
    .expect("legacy Worker fixture should create");
    let worker_id = created.worker.id;
    let Json(dm) = ensure_worker_dm(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("Worker DM fixture should create");
    let now = chrono::Utc::now().to_rfc3339();
    Database::new(&state.db_path)
        .expect("database should open")
        .conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version, created_at,
                 updated_at, completed_at
             ) VALUES (?1, NULL, 'skipped', 1, ?2, ?2, ?2)",
            (worker_id.as_str(), now.as_str()),
        )
        .expect("explicit legacy skip fixture should persist");
    let store = HiveWorkerStore::new(
        Database::new(&state.db_path).expect("Worker store database should open"),
    );
    let accepted = mitsuro_hive_protocol::WorkerIntroductionActionResponse {
        worker_id: worker_id.clone(),
        session_id: dm.session_id.clone(),
        run_id: None,
        status: "skipped".into(),
        autonomy_eligible: true,
        cancellation_requested: false,
    };
    let Json(detail) = load_introduction_action_result(
        &state,
        &store,
        Some("alice"),
        &worker_id,
        accepted.clone(),
    )
    .expect("matching action response should project");
    assert_eq!(
        detail.introduction.as_ref().map(|row| row.status.as_str()),
        Some("skipped")
    );

    let wrong_run = mitsuro_hive_protocol::WorkerIntroductionActionResponse {
        run_id: Some("wrong-run".into()),
        ..accepted.clone()
    };
    assert!(matches!(
        load_introduction_action_result(
            &state,
            &store,
            Some("alice"),
            &worker_id,
            wrong_run,
        ),
        Err(AppError::Internal(message)) if message.contains("durable lifecycle run")
    ));
    let wrong_eligibility = mitsuro_hive_protocol::WorkerIntroductionActionResponse {
        autonomy_eligible: false,
        ..accepted
    };
    assert!(matches!(
        load_introduction_action_result(
            &state,
            &store,
            Some("alice"),
            &worker_id,
            wrong_eligibility,
        ),
        Err(AppError::Internal(message)) if message.contains("durable lifecycle state")
    ));

    let db = Database::new(&state.db_path).expect("database should open");
    let confirmed_proposal = serde_json::json!({
        "schema_version": 1,
        "proposal_id": "confirmed-proposal",
        "revision": 1,
        "worker_id": worker_id.clone(),
        "session_id": dm.session_id,
        "basis": {
            "opening_message_id": 1,
            "through_message_id": 3,
            "user_message_ids": [2],
            "transcript_digest": "digest"
        },
        "base_identity_digest": "identity",
        "base_soul_digest": "soul",
        "facts": [{
            "fact_id": "fact-1",
            "kind": "purpose",
            "statement": "Help with runtime reliability.",
            "evidence_message_id": 2,
            "evidence_excerpt": "runtime reliability"
        }]
    });
    db.conn()
        .execute(
            "UPDATE hive_worker_introductions
             SET status = 'confirmed', proposal_json = ?2,
                 proposal_revision = 1, completed_at = ?3, updated_at = ?3
             WHERE worker_id = ?1",
            (
                worker_id.as_str(),
                confirmed_proposal.to_string(),
                now.as_str(),
            ),
        )
        .expect("confirmed fixture should persist");
    let Json(confirmed) = load_introduction_action_result(
        &state,
        &store,
        Some("alice"),
        &worker_id,
        mitsuro_hive_protocol::WorkerIntroductionActionResponse {
            worker_id: worker_id.clone(),
            session_id: dm.session_id.clone(),
            run_id: None,
            status: "confirmed".into(),
            autonomy_eligible: true,
            cancellation_requested: false,
        },
    )
    .expect("confirmed review decision should project");
    assert_eq!(
        confirmed
            .introduction
            .as_ref()
            .map(|row| row.status.as_str()),
        Some("confirmed")
    );

    db.conn()
        .execute(
            "UPDATE hive_worker_introductions
             SET status = 'awaiting_context', proposal_json = NULL,
                 completed_at = NULL, updated_at = ?2
             WHERE worker_id = ?1",
            (worker_id.as_str(), now.as_str()),
        )
        .expect("return-to-context fixture should persist");
    let Json(returned) = load_introduction_action_result(
        &state,
        &store,
        Some("alice"),
        &worker_id,
        mitsuro_hive_protocol::WorkerIntroductionActionResponse {
            worker_id: worker_id.clone(),
            session_id: dm.session_id.clone(),
            run_id: None,
            status: "awaiting_context".into(),
            autonomy_eligible: false,
            cancellation_requested: false,
        },
    )
    .expect("return decision should project its current durable lifecycle");
    assert_eq!(
        returned
            .introduction
            .as_ref()
            .map(|row| row.status.as_str()),
        Some("awaiting_context")
    );

    // A retry commits queued, but the scheduler is free to advance that exact
    // run before the route reloads its response projection.
    db.conn()
        .execute(
            "DELETE FROM hive_worker_introductions WHERE worker_id = ?1",
            [&worker_id],
        )
        .expect("skipped fixture should clear");
    db.conn()
        .execute(
            "INSERT INTO hive_controllers (
                 id, scope_key, user_id, session_id, status, timezone,
                 max_concurrent_runs, created_at, updated_at, worker_id
             ) VALUES (
                 'projection-controller', 'worker:projection', 'alice', ?1,
                 'active', 'UTC', 1, ?2, ?2, ?3
             )",
            (dm.session_id.as_str(), now.as_str(), worker_id.as_str()),
        )
        .expect("controller fixture should persist");
    let projection_context = serde_json::to_string(
        &mitsuro_core::storage::HiveRunExecutionContextV1::worker_conversation_neutral(
            worker_id.clone(),
            1,
            mitsuro_core::storage::WorkerConversationLane::DirectMessage,
        )
        .expect("Introduction context"),
    )
    .expect("serialized Introduction context");
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, available_at, max_attempts, created_at, updated_at,
                 worker_id, governor_origin, governor_lane_key,
                 execution_context_json
             ) VALUES (
                 'projection-run', 'projection-controller', ?1,
                 'worker_introduction', 'introduce', '{}', 'running', ?2,
                 1, ?2, ?2, ?3, 'user_lifecycle_action', 'dm', ?4
            )",
            (
                dm.session_id.as_str(),
                now.as_str(),
                worker_id.as_str(),
                projection_context,
            ),
        )
        .expect("running retry fixture should persist");
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version, created_at, updated_at
             ) VALUES (?1, 'projection-run', 'running', 1, ?2, ?2)",
            (worker_id.as_str(), now.as_str()),
        )
        .expect("running lifecycle fixture should persist");
    drop(db);
    let queued_response = mitsuro_hive_protocol::WorkerIntroductionActionResponse {
        worker_id: worker_id.clone(),
        session_id: dm.session_id,
        run_id: Some("projection-run".into()),
        status: "queued".into(),
        autonomy_eligible: false,
        cancellation_requested: false,
    };
    let Json(advanced) =
        load_introduction_action_result(&state, &store, Some("alice"), &worker_id, queued_response)
            .expect("queued retry should accept an already-running durable projection");
    assert_eq!(
        advanced
            .introduction
            .as_ref()
            .map(|introduction| introduction.status.as_str()),
        Some("running")
    );
}

#[tokio::test]
async fn worker_detail_fails_closed_on_malformed_review_ready_proposal() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("malformed-proposal")),
    )
    .await
    .expect("Worker fixture should create");
    let worker_id = created.worker.id;
    let Json(dm) = ensure_worker_dm(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("Worker DM fixture should create");
    assert_eq!(dm.worker_id, worker_id);
    let now = chrono::Utc::now().to_rfc3339();
    Database::new(&state.db_path)
        .expect("database should open")
        .conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version, proposal_json,
                 proposal_revision, created_at, updated_at
             ) VALUES (?1, NULL, 'review_ready', 1, '{\"schema_version\":1}',
                       1, ?2, ?2)",
            (worker_id.as_str(), now.as_str()),
        )
        .expect("malformed typed proposal fixture should persist as valid JSON");

    assert!(matches!(
        get_worker(State(state.clone()), alice(), Path(worker_id)).await,
        Err(AppError::Internal(message))
            if message.contains("proposal is not strict V1")
    ));
}

#[tokio::test]
async fn workers_crud_and_mutation_boundaries_are_exact_owner_scoped() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let bob = || Some(current_user("bob", state.working_dir.as_ref()));

    // Slug validation and creation.
    assert!(matches!(
        create_worker(
            State(state.clone()),
            alice(),
            Json(create_worker_request("Bad Slug")),
        )
        .await,
        Err(AppError::BadRequest(_))
    ));
    let (created_status, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(CreateWorkerRequest {
            display_name: Some("Deep Researcher".to_string()),
            avatar_color: Some("#7743DB".to_string()),
            identity: Some("You research deeply.".to_string()),
            soul: Some("Calm and exact.".to_string()),
            permission_mode: Some("supervised".parse().expect("permission mode should parse")),
            ..create_worker_request("researcher")
        }),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "worker create should succeed: {}",
            app_error_description(error)
        )
    });
    assert_eq!(created_status, axum::http::StatusCode::CREATED);
    assert_eq!(created.worker.slug, "researcher");
    assert_eq!(created.worker.display_name, "Deep Researcher");
    assert_eq!(created.worker.status, "active");
    assert_eq!(created.worker.permission_mode, "supervised");
    assert_eq!(created.identity.as_deref(), Some("You research deeply."));
    assert_eq!(created.soul.as_deref(), Some("Calm and exact."));

    // Duplicate active slug for the same owner conflicts; other owners are free.
    assert!(matches!(
        create_worker(
            State(state.clone()),
            alice(),
            Json(create_worker_request("researcher")),
        )
        .await,
        Err(AppError::Conflict(_))
    ));
    let (_, Json(_)) = create_worker(
        State(state.clone()),
        bob(),
        Json(create_worker_request("researcher")),
    )
    .await
    .expect("same slug under another owner should create");

    // Reads and mutations are exact-owner scoped: bob never sees alice's worker.
    let worker_id = created.worker.id.clone();
    assert!(matches!(
        get_worker(State(state.clone()), bob(), Path(worker_id.clone())).await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        update_worker(
            State(state.clone()),
            bob(),
            Path(worker_id.clone()),
            introduction_action_headers("foreign-update"),
            Json(empty_update_worker_request()),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    assert!(matches!(
        ensure_worker_dm(State(state.clone()), bob(), Path(worker_id.clone())).await,
        Err(AppError::NotFound(_))
    ));

    // Owner list shows only the owner's workers.
    let Json(alice_list) = list_workers(State(state.clone()), alice())
        .await
        .expect("alice list should succeed");
    assert_eq!(alice_list.workers.len(), 1);
    assert_eq!(alice_list.workers[0].id, worker_id);

    // Every mutation requires both a revision and a replay key before the
    // server will approach the independently supervised daemon.
    assert!(matches!(
        update_worker(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
            Json(empty_update_worker_request()),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));
    assert!(matches!(
        pause_worker(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
            Json(super::workers::SetWorkerStatusRequest {
                expected_revision: created.worker.revision,
            }),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));
    assert!(matches!(
        archive_worker(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
            Json(super::workers::SetWorkerStatusRequest {
                expected_revision: created.worker.revision,
            }),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));

    // The route unit harness intentionally has no daemon. A replay-keyed
    // request must fail without partially mutating local state.
    assert!(matches!(
        update_worker(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            introduction_action_headers("update-without-daemon"),
            Json(UpdateWorkerRequest {
                display_name: Some("Lead Researcher".to_string()),
                ..empty_update_worker_request()
            }),
        )
        .await,
        Err(AppError::BadGateway(_))
    ));
    let Json(unchanged) = get_worker(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("failed daemon mutation must leave the Worker readable");
    assert_eq!(unchanged.worker.display_name, "Deep Researcher");
    assert_eq!(unchanged.worker.revision, 1);

    // Archive projection remains non-destructive and frees the slug. The
    // daemon's atomic lifecycle transition itself is covered in runtime tests.
    HiveWorkerStore::new(Database::new(&state.db_path).expect("database should open"))
        .set_status(
            &worker_id,
            mitsuro_core::storage::HiveWorkerStatus::Archived,
        )
        .expect("fixture should archive");
    let Json(after_archive) = list_workers(State(state.clone()), alice())
        .await
        .expect("list should succeed");
    assert!(after_archive.workers.is_empty());
    let Json(archived) = get_worker(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("archived worker remains readable");
    assert_eq!(archived.worker.status, "archived");
    assert_eq!(archived.identity.as_deref(), Some("You research deeply."));
    assert!(matches!(
        update_worker(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            introduction_action_headers("archived-update"),
            Json(empty_update_worker_request()),
        )
        .await,
        Err(AppError::Conflict(_))
    ));
    assert!(matches!(
        ensure_worker_dm(State(state.clone()), alice(), Path(worker_id.clone())).await,
        Err(AppError::Conflict(_))
    ));
    let (_, Json(_)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("researcher")),
    )
    .await
    .expect("archived slug should be reusable");
}

#[tokio::test]
async fn worker_governor_is_read_only_exact_owner_and_exact_dm_scoped() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let bob = || Some(current_user("bob", state.working_dir.as_ref()));

    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("governed-worker")),
    )
    .await
    .expect("Worker fixture should create");
    let worker_id = created.worker.id.clone();
    let Json(dm) = ensure_worker_dm(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("Worker DM should create");

    let Json(projection) =
        get_worker_governor(State(state.clone()), alice(), Path(worker_id.clone()))
            .await
            .expect("exact owner should read governor projection");
    assert_eq!(projection.schema_version, 1);
    assert_eq!(projection.worker_id, worker_id);
    assert_eq!(projection.worker_revision, created.worker.revision);
    assert_eq!(projection.dm_session_id, dm.session_id);
    assert_eq!(projection.policy.worker_id, projection.worker_id);
    assert_eq!(projection.daily.calls_used, 0);
    assert_eq!(projection.daily.tokens_used_or_reserved, 0);
    assert_eq!(projection.autonomous_dm.lane_key, "dm");
    assert_eq!(projection.foreground_dm.lane_key, "dm");
    assert_eq!(projection.unresolved_started_count, 0);
    assert!(!projection.response_loss_recovery_required);

    assert!(matches!(
        grant_worker_governor_recovery(
            State(state.clone()),
            alice(),
            Path(worker_id.clone()),
            HeaderMap::new(),
        )
        .await,
        Err(AppError::BadRequest(message)) if message.contains("Idempotency-Key")
    ));

    let public_shape = serde_json::to_value(&projection).expect("projection should serialize");
    let public_shape = public_shape.to_string();
    assert!(!public_shape.contains("request_body"));
    assert!(!public_shape.contains("prompt"));
    assert!(!public_shape.contains("output"));

    assert!(matches!(
        get_worker_governor(State(state.clone()), bob(), Path(worker_id.clone())).await,
        Err(AppError::NotFound(_))
    ));

    let (_, Json(unbound)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("unbound-governor")),
    )
    .await
    .expect("unbound Worker fixture should create");
    assert!(matches!(
        get_worker_governor(State(state.clone()), alice(), Path(unbound.worker.id)).await,
        Err(AppError::NotFound(_))
    ));
}

#[tokio::test]
async fn worker_dm_ensure_is_idempotent_and_freezes_worker_model() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));

    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(CreateWorkerRequest {
            model: Some("gpt-5.5".to_string()),
            permission_mode: Some("supervised".parse().expect("permission mode should parse")),
            ..create_worker_request("builder")
        }),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "worker create should succeed: {}",
            app_error_description(error)
        )
    });
    assert_eq!(created.worker.model.as_deref(), Some("gpt-5.5"));
    assert!(created.worker.model_key.is_some());

    let Json(first) = ensure_worker_dm(
        State(state.clone()),
        alice(),
        Path(created.worker.id.clone()),
    )
    .await
    .unwrap_or_else(|error| panic!("dm ensure should succeed: {}", app_error_description(error)));
    assert!(first.created);
    assert_eq!(first.session_type, "hive");
    assert_eq!(first.title, "Builder");
    assert_eq!(first.permission_mode, "supervised");

    let Json(second) = ensure_worker_dm(
        State(state.clone()),
        alice(),
        Path(created.worker.id.clone()),
    )
    .await
    .expect("second dm ensure should reuse");
    assert!(!second.created);
    assert_eq!(second.session_id, first.session_id);

    // The DM session row carries the Worker's frozen identity: hive type,
    // Worker title, Worker permission mode, and the exact model key, so chat
    // turns honoring the persisted session model run as this Worker.
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session = session_manager
        .get_session(&first.session_id)
        .expect("session lookup should succeed")
        .expect("dm session should exist");
    assert_eq!(session.session_type, SessionType::Hive);
    assert_eq!(session.title, "Builder");
    assert_eq!(session.user_id.as_deref(), Some("alice"));
    assert_eq!(session.model.as_deref(), Some("gpt-5.5"));
    assert_eq!(
        session.model_key.as_ref().map(|key| key.model_id.as_str()),
        Some("gpt-5.5")
    );
    assert_eq!(session.permission_mode.as_str(), "supervised");
    assert!(session.parent_session_id.is_none());
    assert!(session.project_dir.is_none());

    // The list surface exposes the binding and its idle DM state.
    let Json(list) = list_workers(State(state.clone()), alice())
        .await
        .expect("list should succeed");
    assert_eq!(
        list.workers[0].dm_session_id.as_deref(),
        Some(first.session_id.as_str())
    );
    assert_eq!(list.workers[0].dm_agent_state.as_deref(), Some("idle"));
}

#[tokio::test]
async fn worker_lookup_by_session_includes_archived_dm_and_hides_foreign_or_group_lanes() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));
    let bob = || Some(current_user("bob", state.working_dir.as_ref()));

    // Establish the primary relationship thread before any Worker DM exists,
    // then prove that the typed lookup distinguishes the two surfaces.
    let Json(primary) = main_session(State(state.clone()), alice())
        .await
        .expect("primary Hive session should exist");
    let Json(primary_binding) = get_worker_by_session(
        State(state.clone()),
        alice(),
        Path(primary.session_id.clone()),
    )
    .await
    .expect("primary Hive lookup should succeed");
    assert!(matches!(
        primary_binding,
        HiveWorkerSessionBindingResponse::PrimaryHive { session_id }
            if session_id == primary.session_id
    ));

    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("archived-friend")),
    )
    .await
    .expect("Worker fixture should create");
    let worker_id = created.worker.id;
    let Json(dm) = ensure_worker_dm(State(state.clone()), alice(), Path(worker_id.clone()))
        .await
        .expect("Worker DM should exist");

    let (_, Json(group)) = create_group(
        State(state.clone()),
        alice(),
        Json(create_group_request(
            "Private room",
            vec![worker_id.clone()],
        )),
    )
    .await
    .expect("group fixture should create");
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let hidden_lane = session_manager
        .create_session_for_user_with_config(
            "Hidden Worker lane",
            None,
            None,
            None,
            WorkspaceMode::Neutral,
            Some("alice"),
            None,
            SessionType::Hive,
        )
        .expect("hidden lane session should create");
    HiveGroupWorkerLaneStore::new(Database::new(&state.db_path).expect("database should open"))
        .upsert(&NewHiveGroupWorkerLane::new(
            group.group.id,
            worker_id.clone(),
            hidden_lane.clone(),
        ))
        .expect("hidden group lane should bind");
    assert!(matches!(
        get_worker_by_session(State(state.clone()), alice(), Path(hidden_lane)).await,
        Err(AppError::NotFound(_))
    ));

    HiveWorkerStore::new(Database::new(&state.db_path).expect("database should open"))
        .set_status(
            &worker_id,
            mitsuro_core::storage::HiveWorkerStatus::Archived,
        )
        .expect("fixture should archive Worker");
    let Json(binding) =
        get_worker_by_session(State(state.clone()), alice(), Path(dm.session_id.clone()))
            .await
            .expect("archived Worker DM should remain resolvable");
    let HiveWorkerSessionBindingResponse::WorkerDm { session_id, worker } = binding else {
        panic!("archived direct session must remain a Worker DM")
    };
    assert_eq!(session_id, dm.session_id);
    assert_eq!(worker.worker.id, worker_id);
    assert_eq!(worker.worker.status, "archived");

    let Json(roster) = list_workers(State(state.clone()), alice())
        .await
        .expect("roster should load");
    assert!(
        roster.workers.is_empty(),
        "archived Worker stays out of roster"
    );
    assert!(matches!(
        get_worker_by_session(State(state.clone()), bob(), Path(dm.session_id)).await,
        Err(AppError::NotFound(_))
    ));
}

#[tokio::test]
async fn worker_model_patch_without_daemon_rolls_back_worker_and_dm() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    configure_test_model(&state).await;
    let alice = || Some(current_user("alice", state.working_dir.as_ref()));

    let (_, Json(created)) = create_worker(
        State(state.clone()),
        alice(),
        Json(create_worker_request("builder")),
    )
    .await
    .expect("worker create should succeed");
    let Json(dm) = ensure_worker_dm(
        State(state.clone()),
        alice(),
        Path(created.worker.id.clone()),
    )
    .await
    .expect("dm ensure should succeed");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let before = session_manager
        .get_session(&dm.session_id)
        .expect("session lookup should succeed")
        .expect("dm session should exist");
    assert!(before.model.as_deref().unwrap_or("").is_empty());
    assert!(before.model_key.is_none());

    assert!(matches!(
        update_worker(
            State(state.clone()),
            alice(),
            Path(created.worker.id.clone()),
            introduction_action_headers("model-update-without-daemon"),
            Json(UpdateWorkerRequest {
                expected_revision: created.worker.revision,
                model: Some("gpt-5.5".to_string()),
                ..empty_update_worker_request()
            }),
        )
        .await,
        Err(AppError::BadGateway(_))
    ));
    let worker = HiveWorkerStore::new(Database::new(&state.db_path).expect("database should open"))
        .get(&created.worker.id)
        .expect("Worker should load")
        .expect("Worker should remain");
    assert!(worker.model.is_none());
    assert_eq!(worker.revision, created.worker.revision);

    let after = session_manager
        .get_session(&dm.session_id)
        .expect("session lookup should succeed")
        .expect("dm session should exist");
    assert!(after.model.as_deref().unwrap_or("").is_empty());
    assert!(after.model_key.is_none());
}
