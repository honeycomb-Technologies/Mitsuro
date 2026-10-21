//! Git status and branch/worktree endpoints.

use std::path::PathBuf;

use axum::{
    extract::{Query, State},
    routing::{get, post},
    Json, Router,
};

use super::session_access::request_workspace_scope;
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{
    GitBranchResponse, GitBranchesResponse, GitChangedFileResponse, GitChangesResponse,
    GitCheckoutRequest, GitDiffQuery, GitFileDiffResponse, GitQuery, GitStatusResponse,
    GitWorktreeResponse, GitWorktreesResponse,
};
use crate::utils::workspace::resolve_scoped_workspace_path;
use crate::AppState;

/// Build the git router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/status", get(get_status))
        .route("/changes", get(list_changes))
        .route("/diff", get(get_file_diff))
        .route("/branches", get(list_branches))
        .route("/worktrees", get(list_worktrees))
        .route("/checkout", post(checkout_branch))
}

async fn list_changes(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<GitQuery>,
) -> Result<Json<GitChangesResponse>, AppError> {
    let path = resolve_git_path(&state, user.as_ref(), query.path.as_deref())?;
    let changes = mitsuro_core::git::changes(&path).map_err(to_bad_request)?;
    let Some(changes) = changes else {
        return Ok(Json(GitChangesResponse {
            in_repo: false,
            repo_root: None,
            files: Vec::new(),
            truncated: false,
        }));
    };

    Ok(Json(GitChangesResponse {
        in_repo: true,
        repo_root: Some(changes.repo_root.display().to_string()),
        files: changes
            .files
            .into_iter()
            .map(|file| GitChangedFileResponse {
                path: file.path,
                status: file.status,
                additions: file.additions,
                deletions: file.deletions,
            })
            .collect(),
        truncated: changes.truncated,
    }))
}

async fn get_file_diff(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<GitDiffQuery>,
) -> Result<Json<GitFileDiffResponse>, AppError> {
    let path = resolve_git_path(&state, user.as_ref(), query.path.as_deref())?;
    let diff = mitsuro_core::git::file_diff(&path, &query.file)
        .map_err(to_bad_request)?
        .ok_or_else(|| AppError::BadRequest("Path is not inside a git repository".to_string()))?;
    Ok(Json(GitFileDiffResponse {
        path: diff.path,
        patch: diff.patch,
        truncated: diff.truncated,
        binary: diff.binary,
    }))
}

async fn get_status(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<GitQuery>,
) -> Result<Json<GitStatusResponse>, AppError> {
    let path = resolve_git_path(&state, user.as_ref(), query.path.as_deref())?;
    let status = mitsuro_core::git::status(&path).map_err(to_bad_request)?;

    if let Some(status) = status {
        return Ok(Json(to_status_response(status)));
    }

    Ok(Json(GitStatusResponse {
        in_repo: false,
        repo_root: None,
        branch: None,
        head: None,
        upstream: None,
        branch_files: 0,
        branch_additions: 0,
        branch_deletions: 0,
        pr_number: None,
        ahead: 0,
        behind: 0,
        staged: 0,
        modified: 0,
        untracked: 0,
        conflicted: 0,
        total_changes: 0,
    }))
}

async fn list_branches(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<GitQuery>,
) -> Result<Json<GitBranchesResponse>, AppError> {
    let path = resolve_git_path(&state, user.as_ref(), query.path.as_deref())?;
    let repo_root = mitsuro_core::git::resolve_repo_root(&path)
        .map_err(to_bad_request)?
        .ok_or_else(|| AppError::BadRequest("Path is not inside a git repository".to_string()))?;

    let branches = mitsuro_core::git::branches(&path)
        .map_err(to_bad_request)?
        .unwrap_or_default()
        .into_iter()
        .map(|b| GitBranchResponse {
            name: b.name,
            is_current: b.is_current,
            upstream: b.upstream,
            is_remote: b.is_remote,
        })
        .collect();

    Ok(Json(GitBranchesResponse {
        repo_root: repo_root.display().to_string(),
        branches,
    }))
}

async fn list_worktrees(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Query(query): Query<GitQuery>,
) -> Result<Json<GitWorktreesResponse>, AppError> {
    let path = resolve_git_path(&state, user.as_ref(), query.path.as_deref())?;
    let repo_root = mitsuro_core::git::resolve_repo_root(&path)
        .map_err(to_bad_request)?
        .ok_or_else(|| AppError::BadRequest("Path is not inside a git repository".to_string()))?;

    let worktrees = mitsuro_core::git::worktrees(&path)
        .map_err(to_bad_request)?
        .unwrap_or_default()
        .into_iter()
        .map(|wt| GitWorktreeResponse {
            path: wt.path.display().to_string(),
            branch: wt.branch,
            head: wt.head,
            is_current: wt.is_current,
        })
        .collect();

    Ok(Json(GitWorktreesResponse {
        repo_root: repo_root.display().to_string(),
        worktrees,
    }))
}

async fn checkout_branch(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Json(req): Json<GitCheckoutRequest>,
) -> Result<Json<GitStatusResponse>, AppError> {
    let path = resolve_git_path(&state, user.as_ref(), req.path.as_deref())?;

    mitsuro_core::git::checkout(&path, &req.branch, req.create, req.start_point.as_deref())
        .map_err(to_bad_request)?;

    let status = mitsuro_core::git::status(&path)
        .map_err(to_bad_request)?
        .ok_or_else(|| AppError::BadRequest("Path is not inside a git repository".to_string()))?;

    Ok(Json(to_status_response(status)))
}

fn to_bad_request(err: anyhow::Error) -> AppError {
    AppError::BadRequest(err.to_string())
}

fn resolve_git_path(
    state: &AppState,
    user: Option<&CurrentUser>,
    requested: Option<&str>,
) -> Result<PathBuf, AppError> {
    let workspace_scope = request_workspace_scope(state, user);
    resolve_scoped_workspace_path(
        requested,
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )
}

fn to_status_response(status: mitsuro_core::git::GitStatusSummary) -> GitStatusResponse {
    let total_changes = status.total_changes();
    GitStatusResponse {
        in_repo: true,
        repo_root: Some(status.repo_root.display().to_string()),
        branch: status.branch,
        head: status.head,
        upstream: status.upstream,
        branch_files: status.branch_files,
        branch_additions: status.branch_additions,
        branch_deletions: status.branch_deletions,
        pr_number: status.pr_number,
        ahead: status.ahead,
        behind: status.behind,
        staged: status.staged,
        modified: status.modified,
        untracked: status.untracked,
        conflicted: status.conflicted,
        total_changes,
    }
}

#[cfg(test)]
mod tests {
    use crate::routes::test_support::current_user;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::{AgentCancellation, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::Database;
    use mitsuro_core::tools::registry::ToolRegistry;

    use super::resolve_git_path;
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

    #[tokio::test]
    async fn resolve_git_path_rejects_paths_outside_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let outside_root = temp_dir.join("outside");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        std::fs::create_dir_all(&outside_root).expect("outside root should exist");

        let result = resolve_git_path(
            &state,
            Some(&current_user("alice", &user_root)),
            Some(outside_root.to_string_lossy().as_ref()),
        );

        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn resolve_git_path_allows_relative_paths_within_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let repo_dir = user_root.join("repo");
        std::fs::create_dir_all(&repo_dir).expect("repo dir should exist");

        let result = resolve_git_path(
            &state,
            Some(&current_user("alice", &user_root)),
            Some("repo"),
        );
        let path = match result {
            Ok(path) => path,
            Err(_) => panic!("git path should resolve"),
        };

        assert_eq!(path, repo_dir);
    }
}
