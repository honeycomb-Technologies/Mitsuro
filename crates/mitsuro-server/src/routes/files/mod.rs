//! File operations endpoints

mod browse;
mod policy;
mod workspace;

use axum::{routing::get, Router};

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(workspace::read_file).put(workspace::write_file))
        .route("/tree", get(workspace::get_tree))
        .route("/browse", get(browse::browse_directories))
}

#[cfg(test)]
mod tests {
    use crate::routes::test_support::current_user;
    use std::collections::HashMap;
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
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::Database;
    use mitsuro_core::tools::registry::ToolRegistry;

    use super::browse::browse_directories;
    use super::workspace::{get_tree, read_file, resolve_file_path};
    use crate::error::AppError;
    use crate::types::{BrowseQuery, FileQuery, TreeQuery};
    use crate::AppState;

    use std::path::PathBuf;

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

    #[tokio::test]
    async fn resolve_file_path_rejects_paths_outside_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let outside_root = temp_dir.join("outside");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        std::fs::create_dir_all(&outside_root).expect("outside root should exist");

        let result = resolve_file_path(
            &state,
            Some(&current_user("alice", &user_root)),
            Some(outside_root.to_string_lossy().as_ref()),
        );

        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn resolve_file_path_allows_relative_paths_within_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let repo_dir = user_root.join("repo");
        std::fs::create_dir_all(&repo_dir).expect("repo dir should exist");

        let result = resolve_file_path(
            &state,
            Some(&current_user("alice", &user_root)),
            Some("repo"),
        );
        let path = match result {
            Ok(path) => path,
            Err(_) => panic!("file path should resolve"),
        };

        assert_eq!(path, repo_dir);
    }

    #[tokio::test]
    async fn browse_directories_resolves_relative_paths_within_user_root() {
        let (_state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let repo_dir = user_root.join("repo");
        std::fs::create_dir_all(&repo_dir).expect("repo dir should exist");

        let Json(response) = browse_directories(
            Some(current_user("alice", &user_root)),
            Query(BrowseQuery {
                path: Some("repo".to_string()),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("browse should succeed"));

        let canonical_repo = repo_dir.canonicalize().expect("repo should canonicalize");
        assert_eq!(response.current, canonical_repo.to_string_lossy());
    }

    #[tokio::test]
    async fn browse_directories_rejects_paths_outside_user_root() {
        let (_state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let outside_root = temp_dir.join("outside");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        std::fs::create_dir_all(&outside_root).expect("outside root should exist");

        let result = browse_directories(
            Some(current_user("alice", &user_root)),
            Query(BrowseQuery {
                path: Some(outside_root.to_string_lossy().to_string()),
            }),
        )
        .await;

        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn read_file_rejects_non_utf8_content() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        std::fs::write(user_root.join("binary.bin"), [0, 159, 146, 150])
            .expect("binary file should exist");

        let result = read_file(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(FileQuery {
                path: "binary.bin".to_string(),
            }),
        )
        .await;

        match result {
            Err(AppError::BadRequest(message)) => assert!(message.contains("UTF-8")),
            _ => panic!("expected UTF-8 rejection"),
        }
    }

    #[tokio::test]
    async fn browse_directories_hides_hidden_directories() {
        let (_state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        std::fs::create_dir_all(user_root.join("repo")).expect("repo dir should exist");
        std::fs::create_dir_all(user_root.join(".git")).expect("hidden dir should exist");

        let Json(response) = browse_directories(
            Some(current_user("alice", &user_root)),
            Query(BrowseQuery { path: None }),
        )
        .await
        .unwrap_or_else(|_| panic!("browse should succeed"));

        let names: Vec<_> = response
            .directories
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        assert_eq!(names, vec!["repo"]);
    }

    #[tokio::test]
    async fn get_tree_hides_policy_filtered_entries() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let repo_dir = user_root.join("repo");
        std::fs::create_dir_all(repo_dir.join("src")).expect("src dir should exist");
        std::fs::create_dir_all(repo_dir.join(".git")).expect("hidden dir should exist");
        std::fs::create_dir_all(repo_dir.join("node_modules")).expect("node_modules should exist");
        std::fs::create_dir_all(repo_dir.join("target")).expect("target should exist");

        let Json(response) = get_tree(
            State(state),
            Some(current_user("alice", &user_root)),
            Query(TreeQuery {
                root: Some("repo".to_string()),
                depth: 3,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("tree should succeed"));

        let names: Vec<_> = response
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .collect();
        assert_eq!(names, vec!["src"]);
    }
}
