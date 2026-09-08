use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::{Mutex, RwLock};

use mitsuro_core::agent::{AgentCancellation, UserHookManager};
use mitsuro_core::ai::models::create_model_registry;
use mitsuro_core::mcp::McpManager;
use mitsuro_core::process::ProcessRegistry;
use mitsuro_core::skills::SkillsManager;
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::storage::{
    CanvasNodeBinding, CanvasOperation, CanvasPatchInput, CanvasResourceCreateStatus,
    CreateCanvasInput, Database, ReserveCanvasResourceCreateInput, SessionManager, SpatialCanvas,
    SpatialCanvasStore, SpatialCanvasSurface, WorldRect,
};
use mitsuro_core::tools::registry::ToolRegistry;

use super::*;

struct TestHarness {
    state: AppState,
    _temp: TempDir,
    home: PathBuf,
    user_id: String,
}

impl TestHarness {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary resource route root");
        let db_path = temp.path().join("mitsuro.db");
        Database::new(&db_path).expect("database should initialize");
        let working_dir = temp.path().join("workspace");
        let home = working_dir.join("owner");
        std::fs::create_dir_all(&home).expect("test owner home should create");
        let user_id = format!("canvas-resource-owner-{}", Uuid::new_v4());
        Database::new(&db_path)
            .expect("database should open")
            .conn()
            .execute(
                "INSERT INTO users (id, email, license_tier) VALUES (?1, ?2, 'free')",
                (&user_id, format!("{user_id}@canvas.test")),
            )
            .expect("test user should insert");

        let state = AppState {
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
        };
        Self {
            state,
            _temp: temp,
            home,
            user_id,
        }
    }

    fn user(&self) -> AuthenticatedUser {
        AuthenticatedUser {
            user_id: Some(self.user_id.clone()),
            home_dir: Some(self.home.clone()),
        }
    }

    fn other_user(&self) -> AuthenticatedUser {
        AuthenticatedUser {
            user_id: Some(format!("other-{}", self.user_id)),
            home_dir: Some(self.home.clone()),
        }
    }

    fn project(&self, label: &str) -> PathBuf {
        let project = self.home.join(label);
        std::fs::create_dir(&project).expect("test project should create");
        project
            .canonicalize()
            .expect("test project should canonicalize")
    }

    fn canvas(&self, project: &Path, label: &str) -> SpatialCanvas {
        SpatialCanvasStore::new(Database::new(&self.state.db_path).expect("database"))
            .create_for_exact_owner(
                Some(&self.user_id),
                CreateCanvasInput {
                    create_op_id: format!("canvas-{label}-{}", Uuid::new_v4()),
                    project_dir: project.to_string_lossy().into_owned(),
                    surface: SpatialCanvasSurface::Code,
                    name: "Main".to_string(),
                    shared_theme_id: "graphite".to_string(),
                },
            )
            .expect("canvas should create")
            .canvas
    }
}

fn resource_request(resource: CanvasResourceSpec, title: &str) -> CreateCanvasResourceRequest {
    CreateCanvasResourceRequest {
        create_op_id: Uuid::new_v4().to_string(),
        client_id: "native-canvas-resource-test".to_string(),
        node: crate::types::CanvasResourceNodeRequest {
            id: Uuid::new_v4().to_string(),
            title: title.to_string(),
            rect: WorldRect {
                x: -240.0,
                y: 80.0,
                width: 640.0,
                height: 440.0,
            },
            z_index: 3,
        },
        resource,
        initial_prompt: None,
        retry_ambiguous: false,
    }
}

#[cfg(unix)]
fn fake_browser_runtime(temp: &TempDir, label: &str) -> (PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let executable = temp.path().join(format!("{label}-agent-browser"));
    let command_log = temp.path().join(format!("{label}-commands.log"));
    std::fs::write(
        &executable,
        format!(
            "#!/bin/sh\nprintf 'called\\n' >> '{}'\ncat >/dev/null\nprintf '%s\\n' '{{\"data\":{{\"port\":43123}}}}'\n",
            command_log.display()
        ),
    )
    .expect("fake Browser runtime should write");
    let mut permissions = std::fs::metadata(&executable)
        .expect("fake Browser runtime metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&executable, permissions)
        .expect("fake Browser runtime should be executable");
    (executable, command_log)
}

async fn call_create(
    harness: &TestHarness,
    user: AuthenticatedUser,
    canvas_id: &str,
    request: CreateCanvasResourceRequest,
) -> Result<CreateCanvasResourceResponse, AppError> {
    create_resource(
        State(harness.state.clone()),
        CurrentUser(user),
        AxumPath(canvas_id.to_string()),
        Json(request),
    )
    .await
    .map(|Json(response)| response)
}

async fn call_cleanup(
    harness: &TestHarness,
    user: AuthenticatedUser,
    canvas_id: &str,
    request: &CreateCanvasResourceRequest,
) -> Result<CleanupCanvasResourceResponse, AppError> {
    cleanup_resource(
        State(harness.state.clone()),
        CurrentUser(user),
        AxumPath((canvas_id.to_string(), request.create_op_id.clone())),
        axum::http::HeaderMap::new(),
        Some(Json(request.clone())),
    )
    .await
    .map(|Json(response)| response)
}

async fn call_cleanup_with_headers(
    harness: &TestHarness,
    user: AuthenticatedUser,
    canvas_id: &str,
    request: &CreateCanvasResourceRequest,
    headers: axum::http::HeaderMap,
) -> Result<CleanupCanvasResourceResponse, AppError> {
    cleanup_resource(
        State(harness.state.clone()),
        CurrentUser(user),
        AxumPath((canvas_id.to_string(), request.create_op_id.clone())),
        headers,
        Some(Json(request.clone())),
    )
    .await
    .map(|Json(response)| response)
}

async fn agent_cleanup_headers(
    harness: &TestHarness,
    session_id: &str,
    client_id: &str,
) -> axum::http::HeaderMap {
    let lease = harness
        .state
        .session_control_leases
        .write()
        .await
        .take(session_id, client_id.to_owned(), false)
        .expect("Agent cleanup lease");
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        crate::control_leases::CONTROL_LEASE_HEADER,
        lease.lease_token.parse().expect("lease header"),
    );
    headers
}

fn reserve_provisioned_intent(
    harness: &TestHarness,
    canvas: &SpatialCanvas,
    request: CreateCanvasResourceRequest,
    result: serde_json::Value,
) -> CreateCanvasResourceRequest {
    let request = normalize_request(request).expect("request should normalize");
    let store = SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"));
    store
        .reserve_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            ReserveCanvasResourceCreateInput {
                create_op_id: request.create_op_id.clone(),
                bind_op_id: bind_operation_id(&request.create_op_id),
                client_id: request.client_id.clone(),
                node_id: request.node.id.clone(),
                resource_kind: resource_kind(&request.resource),
                request_json: canonical_request_json(&request).expect("canonical request"),
            },
        )
        .expect("intent should reserve");
    let claim = match store
        .claim_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            &request.create_op_id,
            resource_executor_id(),
            false,
        )
        .expect("intent should claim")
    {
        CanvasResourceCreateClaim::Acquired(intent) => intent,
        other => panic!("unexpected provision claim: {other:?}"),
    };
    store
        .mark_resource_create_provisioned_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            &request.create_op_id,
            resource_executor_id(),
            claim.claim_token.as_deref().expect("claim token"),
            &serde_json::to_string(&result).expect("result JSON"),
        )
        .expect("intent should be provisioned");
    request
}

async fn wait_for_bound(
    harness: &TestHarness,
    canvas_id: &str,
    create_op_id: &str,
) -> CanvasResourceCreateIntent {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let intent = SpatialCanvasStore::new(
                Database::new(&harness.state.db_path).expect("database should open"),
            )
            .get_resource_create_for_exact_owner(canvas_id, Some(&harness.user_id), create_op_id)
            .expect("intent lookup")
            .expect("intent should exist");
            if intent.status == CanvasResourceCreateStatus::Bound {
                return intent;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached resource worker should bind within timeout")
}

#[tokio::test]
async fn client_abort_after_provision_still_binds_agent_terminal_and_browser_once() {
    let harness = TestHarness::new();
    let project = harness.project("abort-after-provision");
    let canvas = harness.canvas(&project, "abort");
    let cases = [
        (CanvasResourceSpec::Agent, "Agent"),
        (
            CanvasResourceSpec::Terminal {
                size: crate::types::TerminalSize { cols: 80, rows: 24 },
            },
            "Terminal",
        ),
        (
            CanvasResourceSpec::Browser {
                url: Some("about:blank".to_string()),
            },
            "Browser",
        ),
    ];

    for (resource, title) in cases {
        let request = resource_request(resource.clone(), title);
        if matches!(resource, CanvasResourceSpec::Browser { .. }) {
            super::super::browser::insert_canvas_test_browser(
                &request.node.id,
                Some(&harness.user_id),
                &project,
            )
            .await
            .expect("test Browser should seed");
        }
        let gate = install_resource_job_test_gate(&request.create_op_id).await;
        let first = tokio::spawn(create_resource(
            State(harness.state.clone()),
            CurrentUser(harness.user()),
            AxumPath(canvas.id.clone()),
            Json(request.clone()),
        ));
        let provisioned = tokio::time::timeout(Duration::from_secs(5), gate.provisioned.acquire())
            .await
            .expect("resource should provision before timeout")
            .expect("provision gate should remain open");
        provisioned.forget();

        // This retry races the lost first response. It must observe the same
        // durable intent and wait, never allocate another resource.
        let duplicate = tokio::spawn(create_resource(
            State(harness.state.clone()),
            CurrentUser(harness.user()),
            AxumPath(canvas.id.clone()),
            Json(request.clone()),
        ));
        tokio::task::yield_now().await;
        first.abort();
        assert!(first
            .await
            .expect_err("first HTTP waiter should be aborted")
            .is_cancelled());
        gate.release.add_permits(1);
        remove_resource_job_test_gate(&request.create_op_id).await;

        let response = tokio::time::timeout(Duration::from_secs(5), duplicate)
            .await
            .expect("duplicate request should finish")
            .expect("duplicate request task should not panic")
            .expect("duplicate request should reconcile")
            .0;
        assert!(
            !response.created,
            "duplicate {title} retry is not a new intent"
        );
        assert_eq!(response.node.id, request.node.id);
        let intent = wait_for_bound(&harness, &canvas.id, &request.create_op_id).await;
        assert!(
            intent.result_json.is_some(),
            "{title} must retain result evidence"
        );

        let store = SpatialCanvasStore::new(
            Database::new(&harness.state.db_path).expect("database should open"),
        );
        let before_replay = store
            .get_for_exact_owner(&canvas.id, Some(&harness.user_id))
            .expect("canvas before replay")
            .expect("owned canvas");
        let events_before_replay = store
            .list_events_after_for_exact_owner(&canvas.id, Some(&harness.user_id), 0, 256)
            .expect("events before replay")
            .expect("owned event page");

        let replay = call_create(&harness, harness.user(), &canvas.id, request.clone())
            .await
            .expect("completed replay should succeed");
        assert!(!replay.created);
        assert_eq!(replay.node.id, request.node.id);
        let after_replay = store
            .get_for_exact_owner(&canvas.id, Some(&harness.user_id))
            .expect("canvas after replay")
            .expect("owned canvas");
        let events_after_replay = store
            .list_events_after_for_exact_owner(&canvas.id, Some(&harness.user_id), 0, 256)
            .expect("events after replay")
            .expect("owned event page");
        assert_eq!(after_replay.revision, before_replay.revision);
        assert_eq!(
            events_after_replay.events.len(),
            events_before_replay.events.len(),
            "completed POST replay must not append another bind event"
        );

        match resource {
            CanvasResourceSpec::Agent => {
                let session = SessionManager::new(
                    Database::new(&harness.state.db_path).expect("database should open"),
                )
                .get_session(&request.node.id)
                .expect("Agent lookup")
                .expect("Agent should exist");
                assert_eq!(session.user_id.as_deref(), Some(harness.user_id.as_str()));
                assert_eq!(session.project_dir.as_deref(), project.to_str());
            }
            CanvasResourceSpec::Terminal { .. } => {
                super::super::terminals::validate_terminal_canvas_binding(
                    &harness.user(),
                    &request.node.id,
                    &project,
                )
                .await
                .expect("exact Terminal should remain bound");
                super::super::terminals::remove_canvas_test_terminal(
                    &harness.user(),
                    &request.node.id,
                )
                .await
                .expect("test Terminal should terminate");
            }
            CanvasResourceSpec::Browser { .. } => {
                super::super::browser::validate_browser_canvas_binding(
                    &harness.user(),
                    &request.node.id,
                    &project,
                )
                .await
                .expect("exact Browser should remain bound");
                super::super::browser::remove_canvas_test_browser(
                    &request.node.id,
                    Some(&harness.user_id),
                )
                .await;
            }
        }
    }

    let final_canvas = SpatialCanvasStore::new(
        Database::new(&harness.state.db_path).expect("database should open"),
    )
    .get_for_exact_owner(&canvas.id, Some(&harness.user_id))
    .expect("canvas lookup")
    .expect("canvas should exist");
    assert_eq!(final_canvas.state.nodes.len(), 3);
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_canvas_browser_waiter_cannot_race_owner_from_three_to_five() {
    let harness = TestHarness::new();
    let first_project = harness.project("browser-cap-first");
    let second_project = harness.project("browser-cap-second");
    let first_canvas = harness.canvas(&first_project, "browser-cap-first");
    let second_canvas = harness.canvas(&second_project, "browser-cap-second");
    let mut seeded_ids = Vec::new();
    for _ in 0..3 {
        let id = Uuid::new_v4().to_string();
        super::super::browser::insert_canvas_test_browser(
            &id,
            Some(&harness.user_id),
            &first_project,
        )
        .await
        .expect("seed one of three live Browsers");
        seeded_ids.push(id);
    }

    let mut first_request = resource_request(
        CanvasResourceSpec::Browser {
            url: Some("about:blank".to_owned()),
        },
        "Browser",
    );
    first_request.client_id = "canvas-browser-client-first".to_owned();
    let mut second_request = resource_request(
        CanvasResourceSpec::Browser {
            url: Some("about:blank".to_owned()),
        },
        "Browser",
    );
    second_request.client_id = "canvas-browser-client-second".to_owned();

    let runtime_temp = tempfile::tempdir().expect("fake Browser runtimes");
    let (first_runtime, first_commands) = fake_browser_runtime(&runtime_temp, "first");
    let (second_runtime, second_commands) = fake_browser_runtime(&runtime_temp, "second");
    let first_gate = super::super::browser::install_canvas_browser_ensure_test_gate(
        &first_request.node.id,
        first_runtime,
    )
    .await;
    let second_gate = super::super::browser::install_canvas_browser_ensure_test_gate(
        &second_request.node.id,
        second_runtime,
    )
    .await;

    let first_waiter = tokio::spawn(create_resource(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(first_canvas.id.clone()),
        Json(first_request.clone()),
    ));
    let permit = tokio::time::timeout(Duration::from_secs(5), first_gate.before_insert.acquire())
        .await
        .expect("first detached worker should reach Browser insertion")
        .expect("first insertion gate remains open");
    permit.forget();
    first_waiter.abort();
    assert!(first_waiter
        .await
        .expect_err("first HTTP waiter should be cancelled")
        .is_cancelled());

    let second_waiter = tokio::spawn(create_resource(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(second_canvas.id.clone()),
        Json(second_request.clone()),
    ));
    let permit = tokio::time::timeout(Duration::from_secs(5), second_gate.before_insert.acquire())
        .await
        .expect("second detached worker should reach Browser insertion")
        .expect("second insertion gate remains open");
    permit.forget();

    first_gate.release_insert.add_permits(1);
    let permit = tokio::time::timeout(Duration::from_secs(5), first_gate.after_insert.acquire())
        .await
        .expect("first worker should reserve the fourth slot")
        .expect("first launch gate remains open");
    permit.forget();
    second_gate.release_insert.add_permits(1);

    let second_error = tokio::time::timeout(Duration::from_secs(5), second_waiter)
        .await
        .expect("second Canvas create should finish")
        .expect("second Canvas create task should not panic")
        .expect_err("the fifth Canvas Browser must be rejected");
    assert!(matches!(
        second_error,
        AppError::Conflict(message) if message.contains("Browser tab limit")
    ));
    assert!(second_gate.after_insert.try_acquire().is_err());
    assert!(
        !second_commands.exists(),
        "the rejected fifth Browser must not invoke its sidecar"
    );

    first_gate.release_launch.add_permits(1);
    let first_intent =
        wait_for_bound(&harness, &first_canvas.id, &first_request.create_op_id).await;
    assert_eq!(first_intent.status, CanvasResourceCreateStatus::Bound);
    let second_intent = SpatialCanvasStore::new(
        Database::new(&harness.state.db_path).expect("database should open"),
    )
    .get_resource_create_for_exact_owner(
        &second_canvas.id,
        Some(&harness.user_id),
        &second_request.create_op_id,
    )
    .expect("second intent lookup")
    .expect("second durable intent");
    assert_eq!(second_intent.status, CanvasResourceCreateStatus::Failed);
    assert!(second_intent
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("Browser tab limit")));
    assert_eq!(
        super::super::browser::canvas_test_active_browser_count(Some(&harness.user_id)).await,
        4
    );
    assert!(
        first_commands.exists(),
        "the admitted Browser should launch"
    );
    assert!(super::super::browser::validate_browser_canvas_binding(
        &harness.user(),
        &second_request.node.id,
        &second_project,
    )
    .await
    .is_err());

    super::super::browser::remove_canvas_browser_ensure_test_gate(&first_request.node.id).await;
    super::super::browser::remove_canvas_browser_ensure_test_gate(&second_request.node.id).await;
    for id in seeded_ids
        .into_iter()
        .chain([first_request.node.id, second_request.node.id])
    {
        super::super::browser::remove_canvas_test_browser(&id, Some(&harness.user_id)).await;
    }
}

#[tokio::test]
async fn unfinished_intent_stays_visible_when_its_project_is_unavailable() {
    let harness = TestHarness::new();
    let project = harness.project("temporarily-offline");
    let canvas = harness.canvas(&project, "offline-intent");
    let request = normalize_request(resource_request(CanvasResourceSpec::Agent, "Agent"))
        .expect("request should normalize");
    SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
        .reserve_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            ReserveCanvasResourceCreateInput {
                create_op_id: request.create_op_id.clone(),
                bind_op_id: bind_operation_id(&request.create_op_id),
                client_id: request.client_id.clone(),
                node_id: request.node.id.clone(),
                resource_kind: CanvasResourceKind::Agent,
                request_json: canonical_request_json(&request).expect("canonical request"),
            },
        )
        .expect("intent should reserve");
    std::fs::remove_dir(&project).expect("empty test project should go offline");

    let response = list_resource_intents(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(canvas.id.clone()),
    )
    .await
    .expect("intent listing should not require the project mount")
    .0;
    assert_eq!(response.intents.len(), 1);
    assert_eq!(
        response.intents[0].status,
        CanvasResourceCreateStatus::Reserved
    );
    assert!(response.intents[0]
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("unavailable")));
    assert!(
        SessionManager::new(Database::new(&harness.state.db_path).expect("database"))
            .get_session(&request.node.id)
            .expect("Agent lookup")
            .is_none()
    );
}

#[tokio::test]
async fn resource_intent_listing_is_passive_for_an_available_project() {
    let harness = TestHarness::new();
    let project = harness.project("passive-list");
    let canvas = harness.canvas(&project, "passive-list");
    let request = normalize_request(resource_request(CanvasResourceSpec::Agent, "Agent"))
        .expect("request should normalize");
    let store = SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"));
    store
        .reserve_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            ReserveCanvasResourceCreateInput {
                create_op_id: request.create_op_id.clone(),
                bind_op_id: bind_operation_id(&request.create_op_id),
                client_id: request.client_id.clone(),
                node_id: request.node.id.clone(),
                resource_kind: CanvasResourceKind::Agent,
                request_json: canonical_request_json(&request).expect("canonical request"),
            },
        )
        .expect("intent should reserve");

    let response = list_resource_intents(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(canvas.id.clone()),
    )
    .await
    .expect("intent listing should succeed")
    .0;
    assert_eq!(response.intents.len(), 1);
    assert_eq!(
        response.intents[0].status,
        CanvasResourceCreateStatus::Reserved
    );
    let persisted = store
        .get_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            &request.create_op_id,
        )
        .expect("intent lookup")
        .expect("intent");
    assert_eq!(persisted.status, CanvasResourceCreateStatus::Reserved);
    assert!(
        SessionManager::new(Database::new(&harness.state.db_path).expect("database"))
            .get_session(&request.node.id)
            .expect("Agent lookup")
            .is_none()
    );
}

#[tokio::test]
async fn offline_reserved_cleanup_is_exact_owner_idempotent_and_permanent() {
    let harness = TestHarness::new();
    let project = harness.project("offline-cleanup");
    let canvas = harness.canvas(&project, "offline-cleanup");
    let request = normalize_request(resource_request(CanvasResourceSpec::Agent, "Agent"))
        .expect("request should normalize");
    SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
        .reserve_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            ReserveCanvasResourceCreateInput {
                create_op_id: request.create_op_id.clone(),
                bind_op_id: bind_operation_id(&request.create_op_id),
                client_id: request.client_id.clone(),
                node_id: request.node.id.clone(),
                resource_kind: CanvasResourceKind::Agent,
                request_json: canonical_request_json(&request).expect("canonical request"),
            },
        )
        .expect("intent should reserve");
    assert!(matches!(
        call_cleanup(&harness, harness.other_user(), &canvas.id, &request,).await,
        Err(AppError::NotFound(_))
    ));
    std::fs::remove_dir(&project).expect("empty project should go offline");

    let cleaned = call_cleanup(&harness, harness.user(), &canvas.id, &request)
        .await
        .expect("Reserved Agent cleanup must not require the project mount");
    assert_eq!(cleaned.status, CanvasResourceCreateStatus::Abandoned);
    let replay = call_cleanup(&harness, harness.user(), &canvas.id, &request)
        .await
        .expect("DELETE cleanup replay is idempotent");
    assert_eq!(replay, cleaned);
    assert!(matches!(
        call_create(&harness, harness.user(), &canvas.id, request.clone()).await,
        Err(AppError::Conflict(message)) if message.contains("permanently")
    ));
    let listed = list_resource_intents(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(canvas.id),
    )
    .await
    .expect("terminal cleanup tombstone is excluded from unfinished intents")
    .0;
    assert!(listed.intents.is_empty());
    assert!(
        SessionManager::new(Database::new(&harness.state.db_path).expect("database"))
            .get_session(&request.node.id)
            .expect("Agent lookup")
            .is_none()
    );
}

#[tokio::test]
async fn archived_canvas_rejects_new_resource_before_any_side_effect_but_accepts_tombstone() {
    let harness = TestHarness::new();
    let project = harness.project("archive-create-race");
    let canvas = harness.canvas(&project, "archive-create-race");
    SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: "archive-before-resource".to_owned(),
                client_id: "archive-test".to_owned(),
                expected_revision: canvas.revision,
                operations: vec![CanvasOperation::SetArchived { archived: true }],
            },
        )
        .expect("archive Canvas");
    let request = resource_request(CanvasResourceSpec::Agent, "Invisible Agent");
    assert!(matches!(
        call_create(&harness, harness.user(), &canvas.id, request.clone()).await,
        Err(AppError::Conflict(message)) if message.contains("archived canvas")
    ));
    let store = SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"));
    assert!(store
        .get_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            &request.create_op_id,
        )
        .expect("intent lookup")
        .is_none());
    assert!(
        SessionManager::new(Database::new(&harness.state.db_path).expect("database"))
            .get_session(&request.node.id)
            .expect("Agent lookup")
            .is_none()
    );

    let tombstone = call_cleanup(&harness, harness.user(), &canvas.id, &request)
        .await
        .expect("archived Canvas keeps exact cancellation authority");
    assert_eq!(tombstone.status, CanvasResourceCreateStatus::Abandoned);
    assert!(tombstone.canvas.archived_at.is_some());
}

#[tokio::test]
async fn bound_agent_cleanup_uses_immutable_identity_after_session_title_change() {
    let harness = TestHarness::new();
    let project = harness.project("renamed-agent-cleanup");
    let canvas = harness.canvas(&project, "renamed-agent-cleanup");
    let request = resource_request(CanvasResourceSpec::Agent, "Original Agent Title");
    call_create(&harness, harness.user(), &canvas.id, request.clone())
        .await
        .expect("bind Agent");
    SessionManager::new(Database::new(&harness.state.db_path).expect("database"))
        .update_session_title(&request.node.id, "Renamed after binding")
        .expect("rename durable Agent session");
    let headers = agent_cleanup_headers(&harness, &request.node.id, &request.client_id).await;
    let cleaned =
        call_cleanup_with_headers(&harness, harness.user(), &canvas.id, &request, headers)
            .await
            .expect("mutable title cannot strand exact Agent cleanup");
    assert_eq!(cleaned.status, CanvasResourceCreateStatus::Abandoned);
    assert!(cleaned
        .canvas
        .state
        .nodes
        .iter()
        .all(|node| node.id != request.node.id));
    let session = SessionManager::new(Database::new(&harness.state.db_path).expect("database"))
        .get_session(&request.node.id)
        .expect("Agent lookup")
        .expect("durable Agent remains after Canvas cleanup");
    assert_eq!(session.title, "Renamed after binding");
}

#[tokio::test]
async fn bound_browser_cleanup_removes_node_after_mutable_last_url_changes() {
    let harness = TestHarness::new();
    let project = harness.project("navigated-browser-cleanup");
    let canvas = harness.canvas(&project, "navigated-browser-cleanup");
    let request = resource_request(
        CanvasResourceSpec::Browser {
            url: Some("about:blank".to_owned()),
        },
        "Browser",
    );
    super::super::browser::insert_canvas_test_browser(
        &request.node.id,
        Some(&harness.user_id),
        &project,
    )
    .await
    .expect("seed exact Browser");
    let bound = call_create(&harness, harness.user(), &canvas.id, request.clone())
        .await
        .expect("bind Browser");
    let mut navigated = bound.node;
    navigated.title = "Navigated Browser".to_owned();
    navigated.binding = CanvasNodeBinding::Browser {
        browser_session_id: request.node.id.clone(),
        last_url: Some("https://example.test/navigated".to_owned()),
    };
    SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: "persist-navigated-browser".to_owned(),
                client_id: "navigation-observer".to_owned(),
                expected_revision: bound.canvas.revision,
                operations: vec![CanvasOperation::UpsertNode { node: navigated }],
            },
        )
        .expect("persist mutable Browser URL");
    super::super::browser::mark_canvas_test_browser_confirmed_stopped(
        &request.node.id,
        Some(&harness.user_id),
    )
    .await
    .expect("Browser is authoritatively ended");
    let cleaned = call_cleanup(&harness, harness.user(), &canvas.id, &request)
        .await
        .expect("cleanup matches immutable Browser identity");
    assert_eq!(cleaned.status, CanvasResourceCreateStatus::Cleaned);
    assert!(cleaned
        .canvas
        .state
        .nodes
        .iter()
        .all(|node| node.id != request.node.id));
    super::super::browser::remove_canvas_test_browser(&request.node.id, Some(&harness.user_id))
        .await;
}

#[tokio::test]
async fn aborted_cleanup_finishes_terminal_and_confirmed_browser_identity_stays_reserved() {
    let harness = TestHarness::new();
    let project = harness.project("confirmed-cleanup");
    let canvas = harness.canvas(&project, "confirmed-cleanup");

    let terminal_request = resource_request(
        CanvasResourceSpec::Terminal {
            size: crate::types::TerminalSize { cols: 80, rows: 24 },
        },
        "Terminal",
    );
    let terminal_descriptor = super::super::terminals::ensure_terminal_for_canvas(
        &harness.user(),
        &terminal_request.node.id,
        &project,
        crate::types::TerminalSize { cols: 80, rows: 24 },
    )
    .await
    .expect("test Terminal should create");
    let terminal_request = reserve_provisioned_intent(
        &harness,
        &canvas,
        terminal_request,
        serde_json::to_value(terminal_descriptor).expect("Terminal descriptor JSON"),
    );
    let gate = install_resource_job_test_gate(&terminal_request.create_op_id).await;
    let cleanup_waiter = tokio::spawn(cleanup_resource(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath((canvas.id.clone(), terminal_request.create_op_id.clone())),
        axum::http::HeaderMap::new(),
        None,
    ));
    let terminated = tokio::time::timeout(Duration::from_secs(5), gate.provisioned.acquire())
        .await
        .expect("Terminal cleanup should reach its durable completion gate")
        .expect("cleanup gate remains open");
    terminated.forget();
    cleanup_waiter.abort();
    assert!(cleanup_waiter
        .await
        .expect_err("HTTP cleanup waiter should abort")
        .is_cancelled());
    gate.release.add_permits(1);
    remove_resource_job_test_gate(&terminal_request.create_op_id).await;
    let terminal_cleaned = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let intent =
                SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
                    .get_resource_create_for_exact_owner(
                        &canvas.id,
                        Some(&harness.user_id),
                        &terminal_request.create_op_id,
                    )
                    .expect("Terminal intent lookup")
                    .expect("Terminal intent");
            if intent.status == CanvasResourceCreateStatus::Cleaned {
                break intent;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached cleanup must finish after HTTP cancellation");
    assert_eq!(terminal_cleaned.status, CanvasResourceCreateStatus::Cleaned);
    assert!(super::super::terminals::validate_terminal_canvas_binding(
        &harness.user(),
        &terminal_request.node.id,
        &project,
    )
    .await
    .is_err());

    let browser_request = resource_request(
        CanvasResourceSpec::Browser {
            url: Some("about:blank".to_string()),
        },
        "Browser",
    );
    super::super::browser::insert_canvas_test_browser(
        &browser_request.node.id,
        Some(&harness.user_id),
        &project,
    )
    .await
    .expect("test Browser should seed");
    super::super::browser::mark_canvas_test_browser_confirmed_stopped(
        &browser_request.node.id,
        Some(&harness.user_id),
    )
    .await
    .expect("test Browser should be authoritatively stopped");
    let browser_request = reserve_provisioned_intent(
        &harness,
        &canvas,
        browser_request,
        serde_json::json!({"id": "browser"}),
    );
    let browser_cleaned = call_cleanup(&harness, harness.user(), &canvas.id, &browser_request)
        .await
        .expect("confirmed Browser cleanup");
    assert_eq!(browser_cleaned.status, CanvasResourceCreateStatus::Cleaned);

    let mut replacement = browser_request.clone();
    replacement.create_op_id = Uuid::new_v4().to_string();
    super::super::browser::insert_canvas_test_browser(
        &replacement.node.id,
        Some(&harness.user_id),
        &project,
    )
    .await
    .expect("cleaned Browser id should accept a new authoritative record");
    let rebound = call_create(&harness, harness.user(), &canvas.id, replacement.clone())
        .await
        .expect("new create operation may reuse a cleaned Browser node id");
    assert_eq!(rebound.node.id, replacement.node.id);
    super::super::browser::remove_canvas_test_browser(&replacement.node.id, Some(&harness.user_id))
        .await;
}

#[tokio::test]
async fn unconfirmed_browser_cleanup_stays_ambiguous_and_retains_reservation() {
    let harness = TestHarness::new();
    let project = harness.project("failed-browser-cleanup");
    let canvas = harness.canvas(&project, "failed-browser-cleanup");
    let request = resource_request(
        CanvasResourceSpec::Browser {
            url: Some("about:blank".to_string()),
        },
        "Browser",
    );
    super::super::browser::insert_canvas_test_browser_with_runtime(
        &request.node.id,
        Some(&harness.user_id),
        &project,
        Some(project.join("definitely-missing-agent-browser")),
    )
    .await
    .expect("test Browser should seed");
    let request = reserve_provisioned_intent(
        &harness,
        &canvas,
        request,
        serde_json::json!({"id": "browser"}),
    );
    assert!(
        call_cleanup(&harness, harness.user(), &canvas.id, &request,)
            .await
            .is_err()
    );
    let store = SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"));
    let intent = store
        .get_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            &request.create_op_id,
        )
        .expect("Browser intent lookup")
        .expect("Browser intent");
    assert_eq!(intent.status, CanvasResourceCreateStatus::Ambiguous);
    assert!(store
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: bind_operation_id(&request.create_op_id),
                client_id: request.client_id.clone(),
                expected_revision: canvas.revision,
                operations: vec![CanvasOperation::UpsertNode {
                    node: bound_node(&request),
                }],
            },
        )
        .expect_err("unconfirmed cleanup must retain its reservation")
        .to_string()
        .contains("reserved"));
    super::super::browser::remove_canvas_test_browser(&request.node.id, Some(&harness.user_id))
        .await;
}

#[tokio::test]
async fn bound_exact_replay_succeeds_while_project_is_unavailable() {
    let harness = TestHarness::new();
    let project = harness.project("bound-offline");
    let canvas = harness.canvas(&project, "bound-offline");
    let request = resource_request(CanvasResourceSpec::Agent, "Agent");
    let first = call_create(&harness, harness.user(), &canvas.id, request.clone())
        .await
        .expect("initial Agent should create");
    assert!(first.created);
    std::fs::remove_dir(&project).expect("empty test project should go offline");

    let replay = call_create(&harness, harness.user(), &canvas.id, request)
        .await
        .expect("bound replay performs no project or runtime work");
    assert!(!replay.created);
}

#[tokio::test]
async fn completed_replay_rejects_a_removed_or_rebound_node() {
    let harness = TestHarness::new();
    let project = harness.project("completed-node-drift");
    let canvas = harness.canvas(&project, "completed-node-drift");
    let removed = resource_request(CanvasResourceSpec::Agent, "Removed Agent");
    call_create(&harness, harness.user(), &canvas.id, removed.clone())
        .await
        .expect("removed Agent should create");
    let store = SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"));
    let current = store
        .get_for_exact_owner(&canvas.id, Some(&harness.user_id))
        .expect("canvas lookup")
        .expect("canvas");
    store
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: Uuid::new_v4().to_string(),
                client_id: "node-drift-test".to_string(),
                expected_revision: current.revision,
                operations: vec![CanvasOperation::RemoveNode {
                    node_id: removed.node.id.clone(),
                }],
            },
        )
        .expect("completed node should remove");
    assert!(matches!(
        call_create(&harness, harness.user(), &canvas.id, removed).await,
        Err(AppError::Conflict(message)) if message.contains("removed or rebound")
    ));

    let rebound = resource_request(CanvasResourceSpec::Agent, "Rebound Agent");
    call_create(&harness, harness.user(), &canvas.id, rebound.clone())
        .await
        .expect("rebound Agent should create");
    let current = store
        .get_for_exact_owner(&canvas.id, Some(&harness.user_id))
        .expect("canvas lookup")
        .expect("canvas");
    let mut replacement = bound_node(&rebound);
    replacement.binding = CanvasNodeBinding::Changes;
    store
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: Uuid::new_v4().to_string(),
                client_id: "node-drift-test".to_string(),
                expected_revision: current.revision,
                operations: vec![CanvasOperation::UpsertNode { node: replacement }],
            },
        )
        .expect("completed node should rebind");
    assert!(matches!(
        call_create(&harness, harness.user(), &canvas.id, rebound).await,
        Err(AppError::Conflict(message)) if message.contains("removed or rebound")
    ));
}

#[tokio::test]
async fn forged_matching_bind_before_provision_does_not_skip_agent_creation() {
    let harness = TestHarness::new();
    let project = harness.project("forged-bind");
    let canvas = harness.canvas(&project, "forged-bind");
    let request = normalize_request(resource_request(CanvasResourceSpec::Agent, "Agent"))
        .expect("request should normalize");
    let store = SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"));
    store
        .reserve_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            ReserveCanvasResourceCreateInput {
                create_op_id: request.create_op_id.clone(),
                bind_op_id: bind_operation_id(&request.create_op_id),
                client_id: request.client_id.clone(),
                node_id: request.node.id.clone(),
                resource_kind: CanvasResourceKind::Agent,
                request_json: canonical_request_json(&request).expect("canonical request"),
            },
        )
        .expect("intent should reserve");
    let forged = store
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: bind_operation_id(&request.create_op_id),
                client_id: request.client_id.clone(),
                expected_revision: canvas.revision,
                operations: vec![CanvasOperation::UpsertNode {
                    node: bound_node(&request),
                }],
            },
        )
        .expect_err("generic PATCH must reject the deterministic bind operation");
    assert!(forged.to_string().contains("reserved"));
    let sessions = SessionManager::new(Database::new(&harness.state.db_path).expect("database"));
    assert!(sessions
        .get_session(&request.node.id)
        .expect("Agent lookup")
        .is_none());

    call_create(&harness, harness.user(), &canvas.id, request.clone())
        .await
        .expect("Reserved intent must still provision the Agent");
    assert!(sessions
        .get_session(&request.node.id)
        .expect("Agent lookup")
        .is_some());
    let intent = store
        .get_resource_create_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            &request.create_op_id,
        )
        .expect("intent lookup")
        .expect("intent");
    assert_eq!(intent.status, CanvasResourceCreateStatus::Bound);
    assert!(intent.result_json.is_some());

    assert!(matches!(
        call_create(&harness, harness.other_user(), &canvas.id, request).await,
        Err(AppError::NotFound(_))
    ));
}

#[test]
fn browser_runtime_title_limit_is_validated_before_reservation() {
    let mut request = resource_request(
        CanvasResourceSpec::Browser {
            url: Some("about:blank".to_string()),
        },
        "Browser",
    );
    request.node.title = "b".repeat(121);
    assert!(matches!(
        normalize_request(request),
        Err(AppError::BadRequest(message)) if message.contains("120")
    ));
}

#[test]
fn reserved_node_and_capacity_errors_are_http_conflicts() {
    for message in [
        "canvas node is reserved by an unfinished resource create operation",
        "canvas has no unreserved node capacity for another resource",
        "cannot create a resource in an archived canvas",
    ] {
        assert!(matches!(
            map_intent_error(anyhow::anyhow!(message)),
            AppError::Conflict(mapped) if mapped == message
        ));
    }
}

#[cfg(any())]
#[tokio::test]
async fn bound_agent_prompt_list_is_passive_and_ack_requires_assistant_evidence() {
    let harness = TestHarness::new();
    let project = harness.project("prompt-passive");
    let canvas = harness.canvas(&project, "prompt-passive");
    let mut request = resource_request(CanvasResourceSpec::Agent, "Prompt Agent");
    request.initial_prompt = Some("Start with the failing test".to_string());
    call_create(&harness, harness.user(), &canvas.id, request.clone())
        .await
        .expect("Agent prompt resource should bind");

    let request_json_before: String = Database::new(&harness.state.db_path)
        .expect("database")
        .conn()
        .query_row(
            "SELECT request_json FROM spatial_canvas_resource_creates
             WHERE canvas_id = ?1 AND create_op_id = ?2",
            (&canvas.id, &request.create_op_id),
            |row| row.get(0),
        )
        .expect("prompt ledger row");
    let Json(first_list) = list_resource_intents(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(canvas.id.clone()),
    )
    .await
    .expect("passive list");
    assert_eq!(first_list.intents.len(), 1);
    assert!(!first_list.intents[0].initial_prompt_completed);
    assert_eq!(
        first_list.intents[0].request.initial_prompt.as_deref(),
        Some("Start with the failing test")
    );

    let key = canvas_agent_prompt_idempotency_key(&request.create_op_id, &request.node.id);
    let content_json = canonical_initial_prompt_json("Start with the failing test")
        .expect("canonical prompt content");
    let database = Database::new(&harness.state.db_path).expect("database");
    let receipt = MessageStore::new(&database)
        .reserve_bound_canvas_agent_prompt(
            &canvas.id,
            Some(&harness.user_id),
            &request.create_op_id,
            &request.node.id,
            "Start with the failing test",
            &content_json,
            &key,
        )
        .expect("atomic prompt admission");
    assert!(receipt.created);

    let Json(receipt_only_list) = list_resource_intents(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(canvas.id.clone()),
    )
    .await
    .expect("receipt-only list");
    assert_eq!(receipt_only_list.intents.len(), 1);
    assert!(!receipt_only_list.intents[0].initial_prompt_completed);
    assert!(matches!(
        acknowledge_initial_prompt(
            State(harness.state.clone()),
            CurrentUser(harness.user()),
            AxumPath((canvas.id.clone(), request.create_op_id.clone())),
        )
        .await,
        Err(AppError::Conflict(message)) if message.contains("assistant evidence")
    ));

    MessageStore::new(&database)
        .save_message(
            &request.node.id,
            "assistant",
            r#"[{"type":"text","text":"Done"}]"#,
        )
        .expect("durable assistant evidence");
    let Json(completed_list) = list_resource_intents(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(canvas.id.clone()),
    )
    .await
    .expect("completed list");
    assert_eq!(completed_list.intents.len(), 1);
    assert!(completed_list.intents[0].initial_prompt_completed);
    let request_json_after_get: String = database
        .conn()
        .query_row(
            "SELECT request_json FROM spatial_canvas_resource_creates
             WHERE canvas_id = ?1 AND create_op_id = ?2",
            (&canvas.id, &request.create_op_id),
            |row| row.get(0),
        )
        .expect("prompt ledger row after GET");
    assert_eq!(request_json_after_get, request_json_before);

    assert!(matches!(
        acknowledge_initial_prompt(
            State(harness.state.clone()),
            CurrentUser(harness.other_user()),
            AxumPath((canvas.id.clone(), request.create_op_id.clone())),
        )
        .await,
        Err(AppError::NotFound(_))
    ));
    let Json(first_ack) = acknowledge_initial_prompt(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath((canvas.id.clone(), request.create_op_id.clone())),
    )
    .await
    .expect("completed prompt acknowledgement");
    assert!(first_ack.redacted);
    let Json(repeated_ack) = acknowledge_initial_prompt(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath((canvas.id.clone(), request.create_op_id.clone())),
    )
    .await
    .expect("idempotent repeated acknowledgement");
    assert!(!repeated_ack.redacted);
    let Json(after_ack) = list_resource_intents(
        State(harness.state.clone()),
        CurrentUser(harness.user()),
        AxumPath(canvas.id),
    )
    .await
    .expect("list after redaction");
    assert!(after_ack.intents.is_empty());
}

#[cfg(any())]
#[tokio::test]
async fn canvas_node_removal_and_prompt_admission_are_atomically_ordered() {
    let harness = TestHarness::new();
    let project = harness.project("prompt-ordering");
    let canvas = harness.canvas(&project, "prompt-ordering");
    let mut removed_first = resource_request(CanvasResourceSpec::Agent, "Removed First");
    removed_first.initial_prompt = Some("Do not run invisibly".to_string());
    let bound = call_create(&harness, harness.user(), &canvas.id, removed_first.clone())
        .await
        .expect("first Agent should bind")
        .canvas;
    SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: Uuid::new_v4().to_string(),
                client_id: "prompt-ordering-test".to_string(),
                expected_revision: bound.revision,
                operations: vec![CanvasOperation::RemoveNode {
                    node_id: removed_first.node.id.clone(),
                }],
            },
        )
        .expect("remove before prompt admission");
    let removed_key =
        canvas_agent_prompt_idempotency_key(&removed_first.create_op_id, &removed_first.node.id);
    let removed_content =
        canonical_initial_prompt_json("Do not run invisibly").expect("canonical prompt content");
    let database = Database::new(&harness.state.db_path).expect("database");
    assert!(MessageStore::new(&database)
        .reserve_bound_canvas_agent_prompt(
            &canvas.id,
            Some(&harness.user_id),
            &removed_first.create_op_id,
            &removed_first.node.id,
            "Do not run invisibly",
            &removed_content,
            &removed_key,
        )
        .expect_err("removal must win before first admission")
        .to_string()
        .contains("fingerprint changed"));
    let removed_message_count: i64 = database
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
            [&removed_first.node.id],
            |row| row.get(0),
        )
        .expect("removed session message count");
    assert_eq!(removed_message_count, 0);

    let current = SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
        .get_for_exact_owner(&canvas.id, Some(&harness.user_id))
        .expect("Canvas lookup")
        .expect("Canvas");
    let mut admitted_first = resource_request(CanvasResourceSpec::Agent, "Admitted First");
    admitted_first.initial_prompt = Some("Resume exactly once".to_string());
    call_create(&harness, harness.user(), &canvas.id, admitted_first.clone())
        .await
        .expect("second Agent should bind");
    let admitted_key =
        canvas_agent_prompt_idempotency_key(&admitted_first.create_op_id, &admitted_first.node.id);
    let admitted_content =
        canonical_initial_prompt_json("Resume exactly once").expect("canonical prompt content");
    assert!(MessageStore::new(&database)
        .reserve_bound_canvas_agent_prompt(
            &canvas.id,
            Some(&harness.user_id),
            &admitted_first.create_op_id,
            &admitted_first.node.id,
            "Resume exactly once",
            &admitted_content,
            "canvas-agent-prompt:v1:alternate:key",
        )
        .expect_err("one bound prompt cannot be admitted under an alternate key")
        .to_string()
        .contains("does not match its exact resource intent"));
    let admitted = MessageStore::new(&database)
        .reserve_bound_canvas_agent_prompt(
            &canvas.id,
            Some(&harness.user_id),
            &admitted_first.create_op_id,
            &admitted_first.node.id,
            "Resume exactly once",
            &admitted_content,
            &admitted_key,
        )
        .expect("admission before removal");
    assert!(admitted.created);
    let after_second_bind =
        SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
            .get_for_exact_owner(&canvas.id, Some(&harness.user_id))
            .expect("Canvas lookup")
            .expect("Canvas");
    assert!(after_second_bind.revision > current.revision);
    SpatialCanvasStore::new(Database::new(&harness.state.db_path).expect("database"))
        .patch_for_exact_owner(
            &canvas.id,
            Some(&harness.user_id),
            CanvasPatchInput {
                op_id: Uuid::new_v4().to_string(),
                client_id: "prompt-ordering-test".to_string(),
                expected_revision: after_second_bind.revision,
                operations: vec![CanvasOperation::RemoveNode {
                    node_id: admitted_first.node.id.clone(),
                }],
            },
        )
        .expect("remove after admission");
    let replay = MessageStore::new(&database)
        .reserve_bound_canvas_agent_prompt(
            &canvas.id,
            Some(&harness.user_id),
            &admitted_first.create_op_id,
            &admitted_first.node.id,
            "Resume exactly once",
            &admitted_content,
            &admitted_key,
        )
        .expect("admitted receipt remains exactly replayable after removal");
    assert!(!replay.created);
    assert_eq!(replay.message.id, admitted.message.id);
}
