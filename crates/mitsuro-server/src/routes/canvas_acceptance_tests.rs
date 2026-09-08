use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, Response, StatusCode};
use axum::Router;
use futures::StreamExt;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::{Mutex, RwLock};
use tower::ServiceExt;

use mitsuro_core::agent::{AgentCancellation, UserHookManager};
use mitsuro_core::ai::models::create_model_registry;
use mitsuro_core::mcp::McpManager;
use mitsuro_core::process::ProcessRegistry;
use mitsuro_core::skills::SkillsManager;
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::storage::{
    CreateCanvasInput, Database, SessionType, SpatialCanvasStore, SpatialCanvasSurface,
    WorkspaceMode,
};
use mitsuro_core::tools::registry::ToolRegistry;
use mitsuro_core::SessionManager;

use crate::auth::AuthenticatedUser;
use crate::AppState;

const MAX_TEST_BODY: usize = 2 * 1024 * 1024;

struct TestHarness {
    state: AppState,
    _temp: TempDir,
    alice_home: PathBuf,
    bob_home: PathBuf,
}

impl TestHarness {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temp dir should create");
        let db_path = temp.path().join("mitsuro.db");
        Database::new(&db_path).expect("database should initialize");
        let working_dir = temp.path().join("workspace");
        let alice_home = working_dir.join("alice");
        let bob_home = working_dir.join("bob");
        for path in [&working_dir, &alice_home, &bob_home] {
            std::fs::create_dir_all(path).expect("workspace directory should create");
        }

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
            agent_streams: Arc::new(RwLock::new(
                crate::agent_streams::AgentStreamHub::default(),
            )),
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
        let harness = Self {
            state,
            _temp: temp,
            alice_home,
            bob_home,
        };
        harness.insert_user("alice");
        harness.insert_user("bob");
        harness
    }

    fn insert_user(&self, user_id: &str) {
        Database::new(&self.state.db_path)
            .expect("database should open")
            .conn()
            .execute(
                "INSERT INTO users (id, email, license_tier) VALUES (?1, ?2, 'free')",
                (user_id, format!("{user_id}@canvas.test")),
            )
            .expect("test user should insert");
    }

    fn user(&self, user_id: &str) -> AuthenticatedUser {
        let home_dir = match user_id {
            "alice" => self.alice_home.clone(),
            "bob" => self.bob_home.clone(),
            other => panic!("unknown test user {other}"),
        };
        AuthenticatedUser {
            user_id: Some(user_id.to_string()),
            home_dir: Some(home_dir),
        }
    }

    fn local_user(&self) -> AuthenticatedUser {
        AuthenticatedUser::local()
    }

    fn canvas_router(&self) -> Router {
        super::router().with_state(self.state.clone())
    }

    fn projects_router(&self) -> Router {
        crate::routes::projects::router().with_state(self.state.clone())
    }

    fn terminals_router(&self) -> Router {
        crate::routes::terminals::router().with_state(self.state.clone())
    }

    async fn create_canvas(
        &self,
        user: AuthenticatedUser,
        create_op_id: &str,
        project_dir: &Path,
    ) -> Value {
        let response = self
            .canvas_router()
            .oneshot(json_request(
                Method::POST,
                "/",
                user,
                json!({
                    "create_op_id": create_op_id,
                    "project_dir": project_dir,
                    "surface": "code",
                    "name": "Main",
                    "shared_theme_id": "graphite"
                }),
            ))
            .await
            .expect("canvas create route should respond");
        assert_eq!(response.status(), StatusCode::OK);
        json_response(response).await
    }
}

fn json_request(
    method: Method,
    uri: &str,
    user: AuthenticatedUser,
    body: Value,
) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request should build");
    request.extensions_mut().insert(user);
    request
}

fn empty_request(method: Method, uri: &str, user: AuthenticatedUser) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request should build");
    request.extensions_mut().insert(user);
    request
}

async fn json_response(response: Response<Body>) -> Value {
    let bytes = to_bytes(response.into_body(), MAX_TEST_BODY)
        .await
        .expect("response body should load");
    serde_json::from_slice(&bytes).expect("response body should be JSON")
}

async fn canvas_sse_until(response: Response<Body>, required_type: &str) -> Vec<Value> {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );

    let mut chunks = response.into_body().into_data_stream();
    let mut buffer = String::new();
    let mut envelopes = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(3), chunks.next())
            .await
            .expect("canvas SSE should produce a bounded acceptance event")
            .expect("canvas SSE should remain open")
            .expect("canvas SSE chunk should load");
        buffer.push_str(&String::from_utf8_lossy(&next));

        while let Some(end) = buffer.find("\n\n") {
            let event = buffer[..end].to_string();
            buffer.drain(..end + 2);
            let event_name = event
                .lines()
                .find_map(|line| line.strip_prefix("event: "));
            let data = event.lines().find_map(|line| line.strip_prefix("data: "));
            if event_name != Some("canvas") {
                continue;
            }
            let Some(data) = data else {
                continue;
            };
            let envelope: Value = serde_json::from_str(data).expect("canvas event should be JSON");
            let is_required = envelope["type"] == required_type;
            envelopes.push(envelope);
            if is_required {
                return envelopes;
            }
        }
    }
}

fn changes_node(node_id: &str) -> Value {
    json!({
        "id": node_id,
        "title": "Changes",
        "rect": {
            "x": -320.0,
            "y": 48.0,
            "width": 640.0,
            "height": 480.0
        },
        "z_index": 2,
        "binding": { "kind": "changes" },
        "revision": 0
    })
}

fn patch_body(op_id: &str, expected_revision: u64, operations: Vec<Value>) -> Value {
    json!({
        "op_id": op_id,
        "client_id": "canvas-route-acceptance",
        "expected_revision": expected_revision,
        "operations": operations
    })
}

async fn patch_canvas(
    harness: &TestHarness,
    canvas_id: &str,
    user: AuthenticatedUser,
    body: Value,
) -> Response<Body> {
    harness
        .canvas_router()
        .oneshot(json_request(
            Method::PATCH,
            &format!("/{canvas_id}"),
            user,
            body,
        ))
        .await
        .expect("canvas patch route should respond")
}

async fn create_terminal(
    harness: &TestHarness,
    user: AuthenticatedUser,
    working_dir: &Path,
) -> String {
    let response = harness
        .terminals_router()
        .oneshot(json_request(
            Method::POST,
            "/",
            user,
            json!({
                "working_dir": working_dir,
                "cols": 80,
                "rows": 24
            }),
        ))
        .await
        .expect("terminal create route should respond");
    assert_eq!(response.status(), StatusCode::OK);
    json_response(response).await["id"]
        .as_str()
        .expect("terminal id")
        .to_string()
}

async fn terminate_terminal(harness: &TestHarness, user: AuthenticatedUser, terminal_id: &str) {
    let control = harness
        .terminals_router()
        .oneshot(json_request(
            Method::POST,
            &format!("/{terminal_id}/control"),
            user.clone(),
            json!({ "client_id": "canvas-route-acceptance-cleanup" }),
        ))
        .await
        .expect("terminal control route should respond");
    let lease_token = if control.status() == StatusCode::OK {
        Some(
            json_response(control).await["lease"]["lease_token"]
                .as_str()
                .expect("terminal lease token")
                .to_string(),
        )
    } else {
        // A very short-lived configured shell can exit before control is
        // taken. Exited terminals intentionally allow lease-free cleanup.
        None
    };

    let mut request = empty_request(Method::DELETE, &format!("/{terminal_id}"), user);
    if let Some(lease_token) = lease_token {
        request.headers_mut().insert(
            "x-mitsuro-control-lease",
            lease_token.parse().expect("terminal lease header"),
        );
    }
    let deleted = harness
        .terminals_router()
        .oneshot(request)
        .await
        .expect("terminal delete route should respond");
    assert_eq!(deleted.status(), StatusCode::OK);
}

#[tokio::test]
async fn authenticated_canvas_ids_are_exact_owner_and_local_null_is_not_a_wildcard() {
    let harness = TestHarness::new();
    let alice_project = harness.alice_home.join("project");
    std::fs::create_dir(&alice_project).expect("alice project should create");
    let alice_canvas = harness
        .create_canvas(harness.user("alice"), "alice-create", &alice_project)
        .await;
    let alice_canvas_id = alice_canvas["canvas"]["id"]
        .as_str()
        .expect("alice canvas id");

    let local_canvas = SpatialCanvasStore::new(
        Database::new(&harness.state.db_path).expect("database should open"),
    )
    .create_for_exact_owner(
        None,
        CreateCanvasInput {
            create_op_id: "local-create".to_string(),
            project_dir: "/tmp/local-canvas-contract".to_string(),
            surface: SpatialCanvasSurface::Code,
            name: "Local".to_string(),
            shared_theme_id: "graphite".to_string(),
        },
    )
    .expect("local canvas should seed")
    .canvas;

    for (label, request) in [
        (
            "other tenant GET",
            empty_request(
                Method::GET,
                &format!("/{alice_canvas_id}"),
                harness.user("bob"),
            ),
        ),
        (
            "local GET of tenant id",
            empty_request(
                Method::GET,
                &format!("/{alice_canvas_id}"),
                harness.local_user(),
            ),
        ),
        (
            "tenant GET of local id",
            empty_request(
                Method::GET,
                &format!("/{}", local_canvas.id),
                harness.user("alice"),
            ),
        ),
        (
            "other tenant stream",
            empty_request(
                Method::GET,
                &format!("/{alice_canvas_id}/stream?after_sequence=0"),
                harness.user("bob"),
            ),
        ),
        (
            "local stream of tenant id",
            empty_request(
                Method::GET,
                &format!("/{alice_canvas_id}/stream?after_sequence=0"),
                harness.local_user(),
            ),
        ),
        (
            "tenant stream of local id",
            empty_request(
                Method::GET,
                &format!("/{}/stream?after_sequence=0", local_canvas.id),
                harness.user("alice"),
            ),
        ),
    ] {
        let response = harness
            .canvas_router()
            .oneshot(request)
            .await
            .expect("ownership probe should respond");
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{label}");
    }

    for (label, target, user) in [
        (
            "other tenant PATCH",
            alice_canvas_id,
            harness.user("bob"),
        ),
        (
            "local PATCH of tenant id",
            alice_canvas_id,
            harness.local_user(),
        ),
        (
            "tenant PATCH of local id",
            local_canvas.id.as_str(),
            harness.user("alice"),
        ),
    ] {
        let response = patch_canvas(
            &harness,
            target,
            user,
            patch_body(
                &format!("forged-patch-{label}"),
                0,
                vec![json!({ "type": "rename_canvas", "name": "Stolen" })],
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{label}");
    }

    let owner_response = harness
        .canvas_router()
        .oneshot(empty_request(
            Method::GET,
            &format!("/{alice_canvas_id}"),
            harness.user("alice"),
        ))
        .await
        .expect("owner GET should respond");
    assert_eq!(owner_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_and_patch_retries_are_idempotent_and_reject_changed_fingerprints() {
    let harness = TestHarness::new();
    let project = harness.alice_home.join("idempotent-project");
    std::fs::create_dir(&project).expect("project should create");

    let first = harness
        .create_canvas(harness.user("alice"), "create-once", &project)
        .await;
    assert_eq!(first["created"], true);
    let replay = harness
        .create_canvas(harness.user("alice"), "create-once", &project)
        .await;
    assert_eq!(replay["created"], false);
    assert_eq!(replay["canvas"]["id"], first["canvas"]["id"]);
    let canvas_id = first["canvas"]["id"].as_str().expect("canvas id");

    let changed_create = harness
        .canvas_router()
        .oneshot(json_request(
            Method::POST,
            "/",
            harness.user("alice"),
            json!({
                "create_op_id": "create-once",
                "project_dir": project,
                "surface": "code",
                "name": "Changed",
                "shared_theme_id": "graphite"
            }),
        ))
        .await
        .expect("changed create retry should respond");
    assert_eq!(changed_create.status(), StatusCode::BAD_REQUEST);
    assert!(json_response(changed_create).await["error"]
        .as_str()
        .is_some_and(|message| message.contains("reused with different input")));

    let patch = patch_body(
        "patch-once",
        0,
        vec![json!({ "type": "upsert_node", "node": changes_node("changes-1") })],
    );
    let applied = patch_canvas(
        &harness,
        canvas_id,
        harness.user("alice"),
        patch.clone(),
    )
    .await;
    assert_eq!(applied.status(), StatusCode::OK);
    let applied = json_response(applied).await;
    assert_eq!(applied["applied"], true);
    assert_eq!(applied["canvas"]["revision"], 1);

    let replayed = patch_canvas(
        &harness,
        canvas_id,
        harness.user("alice"),
        patch,
    )
    .await;
    assert_eq!(replayed.status(), StatusCode::OK);
    let replayed = json_response(replayed).await;
    assert_eq!(replayed["applied"], false);
    assert_eq!(replayed["canvas"]["revision"], 1);
    assert_eq!(replayed["canvas"]["state"]["nodes"].as_array().map(Vec::len), Some(1));

    let changed_patch = patch_canvas(
        &harness,
        canvas_id,
        harness.user("alice"),
        patch_body(
            "patch-once",
            0,
            vec![json!({ "type": "rename_canvas", "name": "Changed" })],
        ),
    )
    .await;
    assert_eq!(changed_patch.status(), StatusCode::BAD_REQUEST);
    assert!(json_response(changed_patch).await["error"]
        .as_str()
        .is_some_and(|message| message.contains("reused with different input")));
}

#[tokio::test]
async fn unavailable_projects_remain_listed_and_existing_canvas_metadata_stays_openable() {
    let harness = TestHarness::new();
    let project = harness.alice_home.join("temporarily-unavailable");
    std::fs::create_dir(&project).expect("project should create");
    let created = harness
        .create_canvas(harness.user("alice"), "offline-create", &project)
        .await;
    let canvas_id = created["canvas"]["id"].as_str().expect("canvas id");
    std::fs::remove_dir(&project).expect("project should become unavailable");

    let projects = harness
        .projects_router()
        .oneshot(empty_request(
            Method::GET,
            "/?surface=code",
            harness.user("alice"),
        ))
        .await
        .expect("project list route should respond");
    assert_eq!(projects.status(), StatusCode::OK);
    let projects = json_response(projects).await;
    let unavailable = projects["projects"]
        .as_array()
        .expect("projects array")
        .iter()
        .find(|candidate| candidate["project_dir"] == project.to_string_lossy().as_ref())
        .expect("unavailable project should remain projected");
    assert_eq!(unavailable["available"], false);
    assert_eq!(unavailable["canvas_count"], 1);

    let opened = harness
        .canvas_router()
        .oneshot(empty_request(
            Method::GET,
            &format!("/{canvas_id}"),
            harness.user("alice"),
        ))
        .await
        .expect("existing canvas should remain openable");
    assert_eq!(opened.status(), StatusCode::OK);
    let opened = json_response(opened).await;
    assert_eq!(opened["project_dir"], project.to_string_lossy().as_ref());

    let replay = harness
        .create_canvas(harness.user("alice"), "offline-create", &project)
        .await;
    assert_eq!(replay["created"], false);
    assert_eq!(replay["canvas"]["id"], canvas_id);

    let fresh_missing = harness
        .canvas_router()
        .oneshot(json_request(
            Method::POST,
            "/",
            harness.user("alice"),
            json!({
                "create_op_id": "fresh-missing-create",
                "project_dir": project,
                "surface": "code",
                "name": "Another",
                "shared_theme_id": "graphite"
            }),
        ))
        .await
        .expect("fresh missing project create should respond");
    assert_eq!(fresh_missing.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn canvas_sse_snapshots_replays_in_order_and_reconnects_after_the_exact_cursor() {
    let harness = TestHarness::new();
    let project = harness.alice_home.join("stream-project");
    std::fs::create_dir(&project).expect("project should create");
    let created = harness
        .create_canvas(harness.user("alice"), "stream-create", &project)
        .await;
    let canvas_id = created["canvas"]["id"].as_str().expect("canvas id");

    let snapshot = harness
        .canvas_router()
        .oneshot(empty_request(
            Method::GET,
            &format!("/{canvas_id}/stream"),
            harness.user("alice"),
        ))
        .await
        .expect("snapshot stream should respond");
    let snapshot_envelopes = canvas_sse_until(snapshot, "collaboration_snapshot").await;
    assert_eq!(snapshot_envelopes[0]["type"], "snapshot");
    assert_eq!(snapshot_envelopes[0]["canvas"]["revision"], 0);
    assert!(snapshot_envelopes
        .iter()
        .any(|envelope| envelope["type"] == "collaboration_snapshot"));

    for (revision, operation) in [(0, "one"), (1, "two")] {
        let response = patch_canvas(
            &harness,
            canvas_id,
            harness.user("alice"),
            patch_body(
                operation,
                revision,
                vec![json!({
                    "type": "set_shared_theme",
                    "theme_id": format!("graphite-{operation}")
                })],
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    let replay = harness
        .canvas_router()
        .oneshot(empty_request(
            Method::GET,
            &format!("/{canvas_id}/stream?after_sequence=0"),
            harness.user("alice"),
        ))
        .await
        .expect("replay stream should respond");
    let replay_envelopes = canvas_sse_until(replay, "events").await;
    assert!(replay_envelopes
        .iter()
        .all(|envelope| envelope["type"] != "snapshot"));
    let replay_page = replay_envelopes
        .iter()
        .find(|envelope| envelope["type"] == "events")
        .expect("event replay envelope");
    let replay_sequences = replay_page["page"]["events"]
        .as_array()
        .expect("replay events")
        .iter()
        .map(|event| event["sequence"].as_u64().expect("event sequence"))
        .collect::<Vec<_>>();
    assert_eq!(replay_sequences, [1, 2]);

    let third = patch_canvas(
        &harness,
        canvas_id,
        harness.user("alice"),
        patch_body(
            "three",
            2,
            vec![json!({
                "type": "set_shared_theme",
                "theme_id": "graphite-three"
            })],
        ),
    )
    .await;
    assert_eq!(third.status(), StatusCode::OK);

    let reconnected = harness
        .canvas_router()
        .oneshot(empty_request(
            Method::GET,
            &format!("/{canvas_id}/stream?after_sequence=2"),
            harness.user("alice"),
        ))
        .await
        .expect("reconnect stream should respond");
    let reconnect_envelopes = canvas_sse_until(reconnected, "events").await;
    let reconnect_page = reconnect_envelopes
        .iter()
        .find(|envelope| envelope["type"] == "events")
        .expect("reconnect event envelope");
    let reconnect_sequences = reconnect_page["page"]["events"]
        .as_array()
        .expect("reconnect events")
        .iter()
        .map(|event| event["sequence"].as_u64().expect("event sequence"))
        .collect::<Vec<_>>();
    assert_eq!(reconnect_sequences, [3]);
}

#[tokio::test]
async fn canvas_sse_replay_gap_resynchronizes_with_the_authoritative_snapshot() {
    let harness = TestHarness::new();
    let project = harness.alice_home.join("gap-project");
    std::fs::create_dir(&project).expect("project should create");
    let created = harness
        .create_canvas(harness.user("alice"), "gap-create", &project)
        .await;
    let canvas_id = created["canvas"]["id"].as_str().expect("canvas id");

    for (revision, operation) in [(0, "gap-one"), (1, "gap-two")] {
        let response = patch_canvas(
            &harness,
            canvas_id,
            harness.user("alice"),
            patch_body(
                operation,
                revision,
                vec![json!({
                    "type": "set_shared_theme",
                    "theme_id": operation
                })],
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    Database::new(&harness.state.db_path)
        .expect("database should open")
        .conn()
        .execute(
            "DELETE FROM spatial_canvas_events WHERE canvas_id = ?1 AND sequence < 2",
            [canvas_id],
        )
        .expect("bounded replay gap should simulate");

    let response = harness
        .canvas_router()
        .oneshot(empty_request(
            Method::GET,
            &format!("/{canvas_id}/stream?after_sequence=0"),
            harness.user("alice"),
        ))
        .await
        .expect("gap stream should respond");
    let envelopes = canvas_sse_until(response, "snapshot").await;
    let snapshot = envelopes
        .iter()
        .find(|envelope| envelope["type"] == "snapshot")
        .expect("gap must emit an authoritative snapshot");
    assert_eq!(snapshot["canvas"]["revision"], 2);
    assert_eq!(snapshot["canvas"]["shared_theme_id"], "gap-two");
    assert!(envelopes
        .iter()
        .all(|envelope| envelope["type"] != "events"));
}

#[tokio::test]
async fn public_patch_rejects_resource_bindings_from_another_project() {
    let harness = TestHarness::new();
    let canvas_project = harness.alice_home.join("canvas-project");
    let other_project = harness.alice_home.join("other-project");
    std::fs::create_dir(&canvas_project).expect("canvas project should create");
    std::fs::create_dir(&other_project).expect("other project should create");
    let created = harness
        .create_canvas(
            harness.user("alice"),
            "binding-create",
            &canvas_project,
        )
        .await;
    let canvas_id = created["canvas"]["id"].as_str().expect("canvas id");

    let manager = SessionManager::new(
        Database::new(&harness.state.db_path).expect("database should open"),
    );
    let wrong_project_session = manager
        .create_session_for_user_with_config(
            "Wrong project",
            None,
            Some(other_project.to_string_lossy().as_ref()),
    let wrong_project_browser = "0d46aa6d-15ac-4ca5-9180-96ba6d23f657";
    crate::routes::browser::insert_canvas_test_browser(
        wrong_project_browser,
        Some("alice"),
        other_project.as_path(),
    )
    .await
    .expect("wrong-project Browser should seed");
            Some(other_project.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("wrong-project Agent should seed");
    let same_project_session = manager
        .create_session_for_user_with_config(
            "Same project",
            None,
            Some(canvas_project.to_string_lossy().as_ref()),
            Some(canvas_project.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("same-project Agent should seed");
    let wrong_project_terminal = create_terminal(
        &harness,
        harness.user("alice"),
        other_project.as_path(),
    )
    .await;

    let agent_node = |node_id: &str, session_id: &str| {
        json!({
            "id": node_id,
            "title": "Agent",
            "rect": {
                "x": 0.0,
                "y": 0.0,
                "width": 640.0,
                "height": 480.0
            },
            "z_index": 1,
            "binding": {
                "kind": "agent",
                "session_id": session_id
            },
            "revision": 0
        })
    };

    let rejected_agent = patch_canvas(
        &harness,
        canvas_id,
        harness.user("alice"),
        patch_body(
            "wrong-project-bind",
            0,
            vec![json!({
                "type": "upsert_node",
                "node": agent_node("wrong-agent", &wrong_project_session)
            })],
        ),
    )
    .await;
    let rejected_terminal = patch_canvas(
        &harness,
        canvas_id,
        harness.user("alice"),
        patch_body(
            "wrong-project-terminal-bind",
            0,
            vec![json!({
                "type": "upsert_node",
                "node": {
                    "id": "wrong-terminal",
                    "title": "Terminal",
                    "rect": {
                        "x": 0.0,
                        "y": 0.0,
                        "width": 640.0,
                        "height": 480.0
                    },
                    "z_index": 1,
                    "binding": {
                        "kind": "terminal",
                        "terminal_id": wrong_project_terminal.clone()
                    },
                    "revision": 0
                }
            })],
        ),
    )
    .await;
    terminate_terminal(
        &harness,
        harness.user("alice"),
        &wrong_project_terminal,
    )
    .await;

    assert_eq!(rejected_agent.status(), StatusCode::BAD_REQUEST);
    assert!(json_response(rejected_agent).await["error"]
        .as_str()
        .is_some_and(|message| message.contains("same project")));
    assert_eq!(rejected_terminal.status(), StatusCode::BAD_REQUEST);
    assert!(json_response(rejected_terminal).await["error"]
        .as_str()
        .is_some_and(|message| message.contains("different canvas project")));
    assert_eq!(rejected_browser.status(), StatusCode::BAD_REQUEST);
    assert!(json_response(rejected_browser).await["error"]
        .as_str()
        .is_some_and(|message| message.contains("different canvas project")));

    let accepted = patch_canvas(
        &harness,
        canvas_id,
        harness.user("alice"),
        patch_body(
            "same-project-bind",
            0,
            vec![json!({
                "type": "upsert_node",
                "node": agent_node("same-agent", &same_project_session)
            })],
        ),
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(json_response(accepted).await["canvas"]["revision"], 1);
}
