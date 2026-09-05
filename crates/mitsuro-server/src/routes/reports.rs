//! Report/Paper endpoints

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use mitsuro_core::storage::reports::promote_report_content;
use mitsuro_core::storage::{
    refresh_current_snapshot, Database, MemoryStore, MemoryType, ReportStore,
};

use super::memories::{memory_to_response, MemoryResponse};
use super::session_access::{current_user_id, request_workspace_scope};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::utils::workspace::resolve_optional_workspace_path;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct ListReportsQuery {
    pub project_dir: Option<String>,
    pub query: Option<String>,
}

#[derive(Serialize)]
pub struct ReportSummaryResponse {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub tags: Vec<String>,
    pub created_at: String,
    pub project_dir: Option<String>,
}

#[derive(Serialize)]
pub struct ReportDetailResponse {
    pub id: String,
    pub title: String,
    pub content: String,
    pub summary: String,
    pub tags: Vec<String>,
    pub sources: Vec<String>,
    pub session_id: String,
    pub project_dir: Option<String>,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct ListReportsResponse {
    pub reports: Vec<ReportSummaryResponse>,
}

#[derive(Debug, Deserialize)]
pub struct PromoteReportRequest {
    pub memory_type: Option<String>,
}

#[derive(Serialize)]
pub struct PromoteReportResponse {
    pub created: bool,
    pub memory: MemoryResponse,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_reports))
        .route("/:id", get(get_report))
        .route("/:id/promote", post(promote_report))
}

async fn list_reports(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<ListReportsQuery>,
) -> Result<Json<ListReportsResponse>, AppError> {
    let store = ReportStore::new(Database::new(&state.db_path)?);
    let workspace_scope = request_workspace_scope(&state, user.as_ref());
    let project_dir = resolve_optional_workspace_path(
        query.project_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?;
    let user_id = current_user_id(user.as_ref());
    let reports = match query
        .query
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(search_query) => {
            store.search_reports_for_user(search_query, project_dir.as_deref(), user_id)?
        }
        None => store.list_reports_for_user(project_dir.as_deref(), user_id)?,
    };

    let summaries = reports
        .into_iter()
        .map(|report| ReportSummaryResponse {
            id: report.id,
            title: report.title,
            summary: report.summary,
            tags: report.tags,
            created_at: report.created_at,
            project_dir: report.project_dir,
        })
        .collect();

    Ok(Json(ListReportsResponse { reports: summaries }))
}

async fn get_report(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<ReportDetailResponse>, AppError> {
    let store = ReportStore::new(Database::new(&state.db_path)?);
    let report = store
        .get_report_for_user(&id, current_user_id(user.as_ref()))?
        .ok_or_else(|| AppError::NotFound(format!("Report {} not found", id)))?;

    Ok(Json(ReportDetailResponse {
        id: report.id,
        title: report.title,
        content: report.content,
        summary: report.summary,
        tags: report.tags,
        sources: report.sources,
        session_id: report.session_id,
        project_dir: report.project_dir,
        created_at: report.created_at,
    }))
}

async fn promote_report(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    Json(payload): Json<PromoteReportRequest>,
) -> Result<Json<PromoteReportResponse>, AppError> {
    let report_store = ReportStore::new(Database::new(&state.db_path)?);
    let memory_store = MemoryStore::new(Database::new(&state.db_path)?);
    let user_id = current_user_id(user.as_ref());
    let report = report_store
        .get_report_for_user(&id, user_id)?
        .ok_or_else(|| AppError::NotFound(format!("Report {} not found", id)))?;

    let memory_type = payload
        .memory_type
        .as_deref()
        .unwrap_or("project")
        .parse::<MemoryType>()
        .map_err(AppError::BadRequest)?;
    let memory_content = promote_report_content(&report);
    let (memory, created) = memory_store.save_or_update_by_title(
        memory_type,
        &report.title,
        &memory_content,
        report.project_dir.as_deref(),
        user_id,
    )?;
    refresh_current_snapshot(&state.db_path, report.project_dir.as_deref(), user_id)?;

    Ok(Json(PromoteReportResponse {
        created,
        memory: memory_to_response(memory),
    }))
}

#[cfg(test)]
mod tests {
    use crate::routes::test_support::current_user;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use axum::{
        extract::{Path, Query, State},
        Json,
    };
    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::{AgentCancellation, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::{
        credentials::CredentialStore, is_current_snapshot, reports::CreateReportInput, Database,
        MemoryStore, MemoryType, ReportScope, ReportStore,
    };
    use mitsuro_core::tools::registry::ToolRegistry;
    use mitsuro_core::SessionManager;

    use super::{get_report, list_reports, promote_report, ListReportsQuery, PromoteReportRequest};
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

    fn seed_report(
        state: &AppState,
        user_id: &str,
        title: &str,
        project_dir: &std::path::Path,
    ) -> String {
        let session_manager =
            SessionManager::new(Database::new(&state.db_path).expect("database should open"));
        let session_id = session_manager
            .create_session_for_user_with_config(
                "Research Session",
                None,
                Some(project_dir.to_string_lossy().as_ref()),
                Some(project_dir.to_string_lossy().as_ref()),
                mitsuro_core::storage::WorkspaceMode::Selected,
                Some(user_id),
                None,
                mitsuro_core::storage::SessionType::Code,
            )
            .expect("session should create");
        let store = ReportStore::new(Database::new(&state.db_path).expect("database should open"));
        store
            .create_report(CreateReportInput {
                title,
                session_id: &session_id,
                project_dir: Some(project_dir.to_string_lossy().as_ref()),
                report_root: Some(project_dir),
                content: "content",
                summary: "summary",
                tags: &[],
                sources: &[],
                scope: ReportScope::owner_shared(),
            })
            .expect("report should create")
    }

    #[tokio::test]
    async fn list_reports_resolves_relative_project_dir_filter_within_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let project_dir = user_root.join("repo");
        std::fs::create_dir_all(&project_dir).expect("project dir should exist");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        create_test_user(&state, "alice");
        seed_report(&state, "alice", "Workspace Report", &project_dir);

        let Json(response) = list_reports(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListReportsQuery {
                project_dir: Some("repo".to_string()),
                query: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("report listing should succeed"));

        assert_eq!(response.reports.len(), 1);
        assert_eq!(response.reports[0].title, "Workspace Report");
        assert_eq!(
            response.reports[0].project_dir.as_deref(),
            Some(project_dir.to_string_lossy().as_ref())
        );
    }

    #[tokio::test]
    async fn list_reports_hides_reports_from_other_users() {
        let (state, temp_dir) = create_test_state();
        let alice_root = temp_dir.join("alice");
        let bob_root = temp_dir.join("bob");
        let alice_project = alice_root.join("repo");
        let bob_project = bob_root.join("repo");
        std::fs::create_dir_all(&alice_project).expect("alice project should exist");
        std::fs::create_dir_all(&bob_project).expect("bob project should exist");
        create_test_user(&state, "alice");
        create_test_user(&state, "bob");
        seed_report(&state, "alice", "Alice Report", &alice_project);
        seed_report(&state, "bob", "Bob Report", &bob_project);

        let Json(response) = list_reports(
            State(state),
            Some(current_user("alice", &alice_root)),
            Query(ListReportsQuery {
                project_dir: None,
                query: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("report listing should succeed"));

        assert_eq!(response.reports.len(), 1);
        assert_eq!(response.reports[0].title, "Alice Report");
    }

    #[tokio::test]
    async fn list_reports_rejects_project_dir_filter_outside_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let outside_root = temp_dir.join("outside");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        std::fs::create_dir_all(&outside_root).expect("outside root should exist");

        let result = list_reports(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListReportsQuery {
                project_dir: Some(outside_root.to_string_lossy().to_string()),
                query: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn get_report_rejects_foreign_owner() {
        let (state, temp_dir) = create_test_state();
        let alice_root = temp_dir.join("alice");
        let bob_root = temp_dir.join("bob");
        let alice_project = alice_root.join("repo");
        std::fs::create_dir_all(&alice_project).expect("alice project should exist");
        std::fs::create_dir_all(&bob_root).expect("bob root should exist");
        create_test_user(&state, "alice");
        create_test_user(&state, "bob");
        let report_id = seed_report(&state, "alice", "Alice Report", &alice_project);

        let result = get_report(
            State(state),
            Some(current_user("bob", &bob_root)),
            Path(report_id),
        )
        .await;

        assert!(matches!(result, Err(AppError::NotFound(_))));
    }

    #[tokio::test]
    async fn promote_report_creates_or_updates_project_memory() {
        let (state, temp_dir) = create_test_state();
        let alice_root = temp_dir.join("alice");
        let alice_project = alice_root.join("repo");
        std::fs::create_dir_all(&alice_project).expect("alice project should exist");
        create_test_user(&state, "alice");
        let report_id = seed_report(&state, "alice", "Architecture Report", &alice_project);

        let Json(created_response) = promote_report(
            State(state.clone()),
            Some(current_user("alice", &alice_root)),
            Path(report_id.clone()),
            Json(PromoteReportRequest { memory_type: None }),
        )
        .await
        .unwrap_or_else(|_| panic!("first promotion should succeed"));

        assert!(created_response.created);
        assert_eq!(
            created_response.memory.memory_type,
            MemoryType::Project.as_str()
        );

        let Json(updated_response) = promote_report(
            State(state.clone()),
            Some(current_user("alice", &alice_root)),
            Path(report_id),
            Json(PromoteReportRequest {
                memory_type: Some("project".to_string()),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("second promotion should succeed"));

        assert!(!updated_response.created);
        assert_eq!(created_response.memory.id, updated_response.memory.id);

        let store = MemoryStore::new(Database::new(&state.db_path).expect("database should open"));
        let memories = store.list(
            Some(alice_project.to_string_lossy().as_ref()),
            Some("alice"),
        );
        let durable_memories: Vec<_> = memories
            .iter()
            .filter(|memory| !is_current_snapshot(memory))
            .collect();
        assert_eq!(durable_memories.len(), 1);
        assert_eq!(durable_memories[0].title, "Architecture Report");
        assert_eq!(durable_memories[0].content, "summary");
    }

    #[tokio::test]
    async fn list_reports_supports_query_filter() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let project_dir = user_root.join("repo");
        std::fs::create_dir_all(&project_dir).expect("project dir should exist");
        create_test_user(&state, "alice");
        seed_report(&state, "alice", "Queue Health Review", &project_dir);
        seed_report(&state, "alice", "Runtime Notes", &project_dir);

        let Json(response) = list_reports(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListReportsQuery {
                project_dir: Some("repo".to_string()),
                query: Some("queue".to_string()),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("report search should succeed"));

        assert_eq!(response.reports.len(), 1);
        assert_eq!(response.reports[0].title, "Queue Health Review");
    }
}
