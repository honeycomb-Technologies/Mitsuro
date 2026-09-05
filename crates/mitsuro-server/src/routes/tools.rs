//! Tool execution endpoint

use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;

use super::session_access::{request_workspace_scope, RequestWorkspaceScope};
use mitsuro_core::tools::registry::{FilesystemAccess, PermissionMode, ToolContext};

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{ToolExecuteRequest, ToolExecuteResponse};
use crate::utils::workspace::resolve_scoped_workspace_path;
use crate::AppState;

/// Build the tools router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_tools))
        .route("/execute", post(execute_tool))
}

/// Tool info for API response
#[derive(Serialize)]
pub struct ToolResponse {
    pub name: String,
    pub description: String,
}

/// List all available tools
async fn list_tools(State(state): State<AppState>) -> Json<Vec<ToolResponse>> {
    let tools = state.tool_registry.get_ai_tools_all().await;

    let response: Vec<ToolResponse> = tools
        .into_iter()
        .map(|t| ToolResponse {
            name: t.name,
            description: t.description,
        })
        .collect();

    Json(response)
}

/// Execute a tool
async fn execute_tool(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Json(req): Json<ToolExecuteRequest>,
) -> Result<Json<ToolExecuteResponse>, AppError> {
    let (working_dir, workspace_scope) =
        resolve_tool_working_dir(&state, user.as_ref(), req.working_dir.as_deref())?;
    let user_id = user
        .as_ref()
        .and_then(|current_user| current_user.0.user_id.as_deref());

    // Direct execution is an explicit operator surface: there is no session
    // to inherit a permission mode from, so the endpoint's contract is
    // autonomous execution inside the authenticated workspace scope (the
    // classifier hook and registry policy still apply). Delegated turn
    // budgets come from the shared project-settings contract, exactly as the
    // orchestrated path resolves them.
    let project_settings =
        mitsuro_core::storage::ProjectSettings::load(workspace_scope.allowed_root.as_path());
    let ctx = ToolContext {
        working_dir,
        process_registry: Some(state.process_registry.clone()),
        plan_mode: req.mode == Some(mitsuro_core::storage::WorkMode::Plan),
        ..Default::default()
    }
    .with_permission_mode(PermissionMode::Autonomous)
    .with_filesystem_access(FilesystemAccess::scoped(
        workspace_scope.allowed_root.clone(),
    ))
    .with_subagent_max_turns(project_settings.subagent_max_turns)
    .with_mcp_manager(state.mcp_manager.clone())
    .with_skills_manager(state.skills_manager.clone())
    .with_tool_registry(Arc::clone(&state.tool_registry));

    // Direct execution must inherit the authenticated tenant just like the
    // orchestrated chat path. Without this, process tools silently fall back
    // to the shared single-user bucket even for authenticated requests.
    let ctx = if let Some(user_id) = user_id {
        ctx.with_user_id(user_id.to_owned())
    } else {
        ctx
    };

    let ctx = if let Some(client) = state.resolve_ai_client_for_user(None, user_id).await {
        ctx.with_ai_client(client)
    } else {
        ctx
    };

    // Execute tool
    let result = state
        .tool_registry
        .execute(&req.tool_name, req.params, &ctx)
        .await
        .ok_or_else(|| AppError::NotFound(format!("Tool '{}' not found", req.tool_name)))?;

    Ok(Json(ToolExecuteResponse {
        output: result.output,
        is_error: result.is_error,
    }))
}

fn resolve_tool_working_dir(
    state: &AppState,
    user: Option<&CurrentUser>,
    requested: Option<&str>,
) -> Result<(PathBuf, RequestWorkspaceScope), AppError> {
    let workspace_scope = request_workspace_scope(state, user);
    let working_dir = resolve_scoped_workspace_path(
        requested,
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?;
    Ok((working_dir, workspace_scope))
}

#[cfg(test)]
mod tests {
    use crate::routes::test_support::current_user;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use axum::extract::State;
    use axum::Json;
    use serde_json::json;
    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::{AgentCancellation, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::Database;
    use mitsuro_core::tools::registry::ToolRegistry;
    use mitsuro_core::tools::ProcessesTool;

    use super::{execute_tool, resolve_tool_working_dir};
    use crate::error::AppError;
    use crate::types::ToolExecuteRequest;
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
    async fn resolve_tool_working_dir_rejects_paths_outside_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let outside_root = temp_dir.join("outside");
        std::fs::create_dir_all(&user_root).expect("user root should exist");
        std::fs::create_dir_all(&outside_root).expect("outside root should exist");

        let result = resolve_tool_working_dir(
            &state,
            Some(&current_user("alice", &user_root)),
            Some(outside_root.to_string_lossy().as_ref()),
        );

        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn resolve_tool_working_dir_allows_relative_paths_within_user_root() {
        let (state, temp_dir) = create_test_state();
        let user_root = temp_dir.join("user");
        let repo_dir = user_root.join("repo");
        std::fs::create_dir_all(&repo_dir).expect("repo dir should exist");

        let result = resolve_tool_working_dir(
            &state,
            Some(&current_user("alice", &user_root)),
            Some("repo"),
        );
        let (working_dir, scope) = match result {
            Ok(resolved) => resolved,
            Err(_) => panic!("working dir should resolve"),
        };

        assert_eq!(working_dir, repo_dir);
        assert_eq!(scope.allowed_root, user_root);
    }

    #[tokio::test]
    async fn resolve_tool_working_dir_rejects_absolute_paths_outside_server_root_without_user() {
        let (state, _temp_dir) = create_test_state();
        let result = resolve_tool_working_dir(&state, None, Some("/etc"));

        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }

    #[tokio::test]
    async fn direct_process_execution_is_scoped_to_authenticated_user() {
        let (state, temp_dir) = create_test_state();
        state.tool_registry.register(Arc::new(ProcessesTool)).await;

        state
            .process_registry
            .register_external_for_user(
                "alice",
                "alice-process".to_string(),
                "alice-preview".to_string(),
                None,
                None,
                temp_dir.clone(),
            )
            .await
            .expect("register alice process");
        state
            .process_registry
            .register_external_for_user(
                "bob",
                "bob-process".to_string(),
                "bob-preview".to_string(),
                None,
                None,
                temp_dir.clone(),
            )
            .await
            .expect("register bob process");

        let response = match execute_tool(
            State(state.clone()),
            Some(current_user("alice", &temp_dir)),
            Json(ToolExecuteRequest {
                tool_name: "processes".to_string(),
                params: json!({"action": "list"}),
                working_dir: None,
                mode: None,
            }),
        )
        .await
        {
            Ok(response) => response.0,
            Err(_) => panic!("same-user process list should execute"),
        };

        assert!(!response.is_error);
        assert!(response.output.contains("alice-process"));
        assert!(!response.output.contains("bob-process"));

        let response = match execute_tool(
            State(state),
            Some(current_user("alice", &temp_dir)),
            Json(ToolExecuteRequest {
                tool_name: "processes".to_string(),
                params: json!({"action": "status", "process_id": "bob-process"}),
                working_dir: None,
                mode: None,
            }),
        )
        .await
        {
            Ok(response) => response.0,
            Err(_) => panic!("foreign-user lookup should return a tool error"),
        };

        assert!(response.is_error);
        assert!(response.output.contains("Process not found"));
    }
}
