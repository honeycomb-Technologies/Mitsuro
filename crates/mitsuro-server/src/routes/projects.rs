use std::collections::BTreeMap;
use std::path::Path;

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use chrono::Utc;

use mitsuro_core::storage::{Database, ProjectCandidate, SpatialCanvasStore, SpatialCanvasSurface};

use super::session_access::{current_user_id, request_workspace_scope};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{ListProjectsQuery, ListProjectsResponse, ProjectSummaryResponse};
use crate::utils::workspace::resolve_scoped_workspace_path;
use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(list_projects))
}

async fn list_projects(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Query(query): Query<ListProjectsQuery>,
) -> Result<Json<ListProjectsResponse>, AppError> {
    let current_user = CurrentUser(user);
    let owner_user_id = current_user_id(Some(&current_user));
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let candidates = store
        .list_project_candidates_for_exact_owner(owner_user_id, query.surface)
        .map_err(AppError::from)?;
    let scope = request_workspace_scope(&state, Some(&current_user));
    let mut projects = BTreeMap::<String, ProjectCandidate>::new();
    for candidate in candidates {
        if resolve_scoped_workspace_path(
            Some(candidate.project_dir.as_str()),
            &scope.base_dir,
            &scope.allowed_root,
        )
        .is_ok()
        {
            projects.insert(candidate.project_dir.clone(), candidate);
        }
    }

    if query.surface == SpatialCanvasSurface::Code {
        if let Ok(path) = resolve_scoped_workspace_path(
            Some(state.working_dir.to_string_lossy().as_ref()),
            &scope.base_dir,
            &scope.allowed_root,
        ) {
            if let Ok(canonical) = path.canonicalize() {
                let project_dir = canonical.to_string_lossy().into_owned();
                projects
                    .entry(project_dir.clone())
                    .or_insert(ProjectCandidate {
                        project_dir,
                        updated_at: Utc::now().to_rfc3339(),
                        canvas_count: 0,
                    });
            }
        }
    }

    let mut projects = projects
        .into_values()
        .map(|candidate| {
            let name = Path::new(&candidate.project_dir)
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .unwrap_or(&candidate.project_dir)
                .to_string();
            let available = Path::new(&candidate.project_dir).is_dir();
            ProjectSummaryResponse {
                name,
                project_dir: candidate.project_dir,
                updated_at: candidate.updated_at,
                canvas_count: candidate.canvas_count,
                available,
            }
        })
        .collect::<Vec<_>>();
    projects.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.project_dir.cmp(&right.project_dir))
    });
    Ok(Json(ListProjectsResponse { projects }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::{AgentCancellation, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::{CreateCanvasInput, Database};
    use mitsuro_core::tools::registry::ToolRegistry;

    use crate::routes::test_support::current_user;

    fn create_test_state() -> (AppState, tempfile::TempDir) {
        let temp = tempfile::tempdir().expect("temp dir");
        let db_path = temp.path().join("mitsuro.db");
        Database::new(&db_path).expect("database should initialize");
        let working_dir = temp.path().join("workspace");
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
            temp,
        )
    }

    fn insert_user(state: &AppState, user_id: &str) {
        let db = Database::new(&state.db_path).expect("database should open");
        db.conn()
            .execute(
                "INSERT INTO users (id, email, license_tier) VALUES (?1, ?2, 'free')",
                (user_id, format!("{user_id}@example.com")),
            )
            .expect("user should insert");
    }

    fn create_canvas_for(state: &AppState, owner: &str, project_dir: &Path, operation: &str) {
        let store =
            SpatialCanvasStore::new(Database::new(&state.db_path).expect("database should open"));
        store
            .create_for_exact_owner(
                Some(owner),
                CreateCanvasInput {
                    create_op_id: operation.to_owned(),
                    project_dir: project_dir.to_string_lossy().into_owned(),
                    surface: SpatialCanvasSurface::Code,
                    name: "Main".to_owned(),
                    shared_theme_id: "graphite".to_owned(),
                },
            )
            .expect("canvas should create");
    }

    #[test]
    fn project_name_uses_last_path_component() {
        let path = Path::new("/tmp/work/Mitsuro");
        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some("Mitsuro")
        );
    }

    #[tokio::test]
    async fn projects_are_exact_owner_and_report_temporarily_unavailable_paths() {
        let (state, temp) = create_test_state();
        insert_user(&state, "alice");
        insert_user(&state, "bob");

        let alice_home = temp.path().join("alice");
        let alice_project = alice_home.join("temporarily-offline");
        let bob_home = temp.path().join("bob");
        let bob_project = bob_home.join("private-project");
        std::fs::create_dir_all(&alice_project).expect("alice project should exist");
        std::fs::create_dir_all(&bob_project).expect("bob project should exist");
        create_canvas_for(&state, "alice", &alice_project, "alice-create");
        create_canvas_for(&state, "bob", &bob_project, "bob-create");

        std::fs::remove_dir(&alice_project).expect("simulate unavailable project");
        let Json(response) = list_projects(
            State(state),
            current_user("alice", &alice_home),
            Query(ListProjectsQuery {
                surface: SpatialCanvasSurface::Code,
            }),
        )
        .await
        .expect("project list should load");

        assert_eq!(response.projects.len(), 1);
        assert_eq!(
            response.projects[0].project_dir,
            alice_project.to_string_lossy()
        );
        assert_eq!(response.projects[0].canvas_count, 1);
        assert!(!response.projects[0].available);
        assert!(response
            .projects
            .iter()
            .all(|project| project.project_dir != bob_project.to_string_lossy().as_ref()));
    }
}
