//! Memory endpoints

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use mitsuro_core::storage::{
    is_compaction_flush_memory, is_current_snapshot, refresh_current_snapshot, AgentMemory,
    Database, KnowledgeSnapshot, MemoryStore, MemoryType,
};

use super::session_access::{current_user_id, request_workspace_scope};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::utils::workspace::resolve_optional_workspace_path;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct ListMemoriesQuery {
    pub project_dir: Option<String>,
    pub memory_type: Option<String>,
    pub include_content: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct GetMemorySnapshotQuery {
    pub project_dir: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MemoryResponse {
    pub id: String,
    pub memory_type: String,
    pub title: String,
    pub content: String,
    pub project_dir: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_chars: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
}

#[derive(Serialize)]
pub struct ListMemoriesResponse {
    pub memories: Vec<MemoryResponse>,
}

#[derive(Serialize)]
pub struct MemorySnapshotResponse {
    pub snapshot: Option<MemoryResponse>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_memories))
        .route("/snapshot", get(get_memory_snapshot))
}

const MEMORY_LIST_PREVIEW_CHARS: usize = 500;

pub(super) fn memory_to_response(memory: AgentMemory) -> MemoryResponse {
    MemoryResponse {
        id: memory.id,
        memory_type: memory.memory_type.as_str().to_string(),
        title: memory.title,
        content: memory.content,
        project_dir: memory.project_dir,
        created_at: memory.created_at,
        updated_at: memory.updated_at,
        content_preview: None,
        content_chars: None,
        truncated: None,
    }
}

fn knowledge_snapshot_to_response(snapshot: KnowledgeSnapshot) -> MemoryResponse {
    MemoryResponse {
        id: snapshot.id,
        // Preserve the existing HTTP shape while keeping generated snapshots
        // out of the canonical memory store.
        memory_type: MemoryType::Project.as_str().to_string(),
        title: snapshot.title,
        content: snapshot.content,
        project_dir: snapshot.project_dir,
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
        content_preview: None,
        content_chars: None,
        truncated: None,
    }
}

fn memory_to_list_response(memory: AgentMemory, include_content: bool) -> MemoryResponse {
    let compact = memory
        .content
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let content_chars = compact.chars().count();
    let preview = truncate_preview(&compact, MEMORY_LIST_PREVIEW_CHARS);
    let truncated = content_chars > MEMORY_LIST_PREVIEW_CHARS;
    MemoryResponse {
        id: memory.id,
        memory_type: memory.memory_type.as_str().to_string(),
        title: memory.title,
        content: if include_content {
            memory.content
        } else {
            preview.clone()
        },
        project_dir: memory.project_dir,
        created_at: memory.created_at,
        updated_at: memory.updated_at,
        content_preview: Some(preview),
        content_chars: Some(content_chars),
        truncated: Some(truncated),
    }
}

fn truncate_preview(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

async fn list_memories(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<ListMemoriesQuery>,
) -> Result<Json<ListMemoriesResponse>, AppError> {
    let store = MemoryStore::new(Database::new(&state.db_path)?);
    let workspace_scope = request_workspace_scope(&state, user.as_ref());
    let project_dir = resolve_optional_workspace_path(
        query.project_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?;
    let user_id = current_user_id(user.as_ref());

    let memories = match query.memory_type.as_deref() {
        Some(type_str) => {
            let memory_type = type_str
                .parse::<MemoryType>()
                .map_err(AppError::BadRequest)?;
            store.list_by_type(memory_type, project_dir.as_deref(), user_id)
        }
        None => store.list(project_dir.as_deref(), user_id),
    };

    let include_content = query.include_content.unwrap_or(false);
    Ok(Json(ListMemoriesResponse {
        memories: memories
            .into_iter()
            .filter(|memory| !is_current_snapshot(memory))
            .filter(|memory| !is_compaction_flush_memory(memory))
            .map(|memory| memory_to_list_response(memory, include_content))
            .collect(),
    }))
}

async fn get_memory_snapshot(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<GetMemorySnapshotQuery>,
) -> Result<Json<MemorySnapshotResponse>, AppError> {
    let workspace_scope = request_workspace_scope(&state, user.as_ref());
    let project_dir = resolve_optional_workspace_path(
        query.project_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?;
    let user_id = current_user_id(user.as_ref());
    let snapshot = refresh_current_snapshot(&state.db_path, project_dir.as_deref(), user_id)?
        .map(knowledge_snapshot_to_response);

    Ok(Json(MemorySnapshotResponse { snapshot }))
}

#[cfg(test)]
mod tests {
    use crate::routes::test_support::current_user;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use axum::{
        extract::{Query, State},
        Json,
    };
    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::{AgentCancellation, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::{credentials::CredentialStore, Database, MemoryStore, MemoryType};
    use mitsuro_core::tools::registry::ToolRegistry;

    use super::{get_memory_snapshot, list_memories, GetMemorySnapshotQuery, ListMemoriesQuery};
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

    fn seed_memory(
        state: &AppState,
        user_id: &str,
        title: &str,
        project_dir: Option<&str>,
        memory_type: MemoryType,
    ) {
        seed_memory_with_content(state, user_id, title, "content", project_dir, memory_type);
    }

    fn seed_memory_with_content(
        state: &AppState,
        user_id: &str,
        title: &str,
        content: &str,
        project_dir: Option<&str>,
        memory_type: MemoryType,
    ) {
        let store = MemoryStore::new(Database::new(&state.db_path).expect("database should open"));
        store
            .save(memory_type, title, content, project_dir, Some(user_id))
            .expect("memory should create");
    }

    #[tokio::test]
    async fn list_memories_resolves_relative_project_dir_filter_within_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let project_dir = user_root.join("repo");
        std::fs::create_dir_all(&project_dir).expect("project dir should exist");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        seed_memory(
            &state,
            "alice",
            "Architecture",
            Some(project_dir.to_string_lossy().as_ref()),
            MemoryType::Project,
        );

        let Json(response) = list_memories(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListMemoriesQuery {
                project_dir: Some("repo".to_string()),
                memory_type: Some("project".to_string()),
                include_content: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("memory listing should succeed"));

        assert_eq!(response.memories.len(), 1);
        assert_eq!(response.memories[0].title, "Architecture");
        assert_eq!(
            response.memories[0].project_dir.as_deref(),
            Some(project_dir.to_string_lossy().as_ref())
        );
    }

    #[tokio::test]
    async fn list_memories_hides_memories_from_other_users() {
        let (state, temp_dir) = create_test_state();
        let alice_root = temp_dir.join("alice");
        let bob_root = temp_dir.join("bob");
        std::fs::create_dir_all(&alice_root).expect("alice root should exist");
        std::fs::create_dir_all(&bob_root).expect("bob root should exist");
        seed_memory(&state, "alice", "Alice Memory", None, MemoryType::Project);
        seed_memory(&state, "bob", "Bob Memory", None, MemoryType::Project);

        let Json(response) = list_memories(
            State(state),
            Some(current_user("alice", &alice_root)),
            Query(ListMemoriesQuery {
                project_dir: None,
                memory_type: None,
                include_content: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("memory listing should succeed"));

        assert_eq!(response.memories.len(), 1);
        assert_eq!(response.memories[0].title, "Alice Memory");
    }

    #[tokio::test]
    async fn list_memories_hides_current_snapshot_entries() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        seed_memory(&state, "alice", "Architecture", None, MemoryType::Project);
        seed_memory(
            &state,
            "alice",
            mitsuro_core::storage::CURRENT_SNAPSHOT_TITLE,
            None,
            MemoryType::Project,
        );

        let Json(response) = list_memories(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListMemoriesQuery {
                project_dir: None,
                memory_type: None,
                include_content: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("memory listing should succeed"));

        assert_eq!(response.memories.len(), 1);
        assert_eq!(response.memories[0].title, "Architecture");
    }

    #[tokio::test]
    async fn list_memories_returns_previews_by_default_and_full_content_on_request() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        let long_content = "alpha ".repeat(140);
        seed_memory_with_content(
            &state,
            "alice",
            "Long Memory",
            &long_content,
            None,
            MemoryType::Project,
        );

        let Json(response) = list_memories(
            State(state.clone()),
            Some(current_user("alice", &user_root)),
            Query(ListMemoriesQuery {
                project_dir: None,
                memory_type: None,
                include_content: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("memory listing should succeed"));

        assert_eq!(response.memories.len(), 1);
        assert_eq!(response.memories[0].title, "Long Memory");
        assert!(response.memories[0].content.len() < long_content.len());
        assert_eq!(response.memories[0].truncated, Some(true));

        let Json(full_response) = list_memories(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListMemoriesQuery {
                project_dir: None,
                memory_type: None,
                include_content: Some(true),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("memory listing should succeed"));

        assert_eq!(full_response.memories[0].content, long_content);
    }

    #[tokio::test]
    async fn list_memories_hides_compaction_flush_entries() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        seed_memory(&state, "alice", "Architecture", None, MemoryType::Project);
        seed_memory_with_content(
            &state,
            "alice",
            &format!("{}1", mitsuro_core::storage::COMPACTION_FLUSH_TITLE_PREFIX),
            "full old transcript",
            None,
            MemoryType::Project,
        );

        let Json(response) = list_memories(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListMemoriesQuery {
                project_dir: None,
                memory_type: None,
                include_content: Some(true),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("memory listing should succeed"));

        assert_eq!(response.memories.len(), 1);
        assert_eq!(response.memories[0].title, "Architecture");
        assert!(!response.memories[0].content.contains("full old transcript"));
    }

    #[tokio::test]
    async fn get_memory_snapshot_refreshes_and_returns_snapshot() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let project_dir = user_root.join("repo");
        std::fs::create_dir_all(&project_dir).expect("project dir should exist");
        std::fs::create_dir_all(&user_root).expect("user root should exist");

        let memory_store =
            MemoryStore::new(Database::new(&state.db_path).expect("database should open"));
        memory_store
            .save(
                MemoryType::Project,
                "Wake cadence",
                "Favor a faster cadence while the queue is active.",
                Some(project_dir.to_string_lossy().as_ref()),
                Some("alice"),
            )
            .expect("memory should create");

        let Json(response) = get_memory_snapshot(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(GetMemorySnapshotQuery {
                project_dir: Some("repo".to_string()),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("snapshot fetch should succeed"));

        let snapshot = response.snapshot.expect("snapshot");
        assert_eq!(
            snapshot.title,
            mitsuro_core::storage::CURRENT_SNAPSHOT_TITLE
        );
        assert!(snapshot.content.contains("Wake cadence"));
    }

    #[tokio::test]
    async fn list_memories_rejects_project_dir_filter_outside_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let outside_root = temp_dir.join("outside");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        std::fs::create_dir_all(&outside_root).expect("outside root should exist");

        let result = list_memories(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(ListMemoriesQuery {
                project_dir: Some(outside_root.to_string_lossy().to_string()),
                memory_type: None,
                include_content: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }
}
