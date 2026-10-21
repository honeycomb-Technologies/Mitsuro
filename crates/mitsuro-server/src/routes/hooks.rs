//! User hooks management endpoints

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, patch},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use mitsuro_core::agent::{UserHook, UserHookSource, UserHookType};
use mitsuro_core::storage::Database;

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::routes::session_access::current_user_id;
use crate::AppState;

/// Build the hooks router
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_hooks).post(create_hook))
        .route("/:id", delete(delete_hook))
        .route("/:id/toggle", patch(toggle_hook))
}

/// Hook info for API response
#[derive(Serialize)]
pub struct HookResponse {
    pub id: String,
    pub hook_type: String,
    pub tool_pattern: String,
    pub command: String,
    pub enabled: bool,
    pub created_at: String,
    pub source: UserHookSource,
    #[serde(rename = "readOnly")]
    pub read_only: bool,
}

impl From<&UserHook> for HookResponse {
    fn from(hook: &UserHook) -> Self {
        Self::for_requester(hook, None)
    }
}

impl HookResponse {
    fn for_requester(hook: &UserHook, user_id: Option<&str>) -> Self {
        Self {
            id: hook.id.clone(),
            hook_type: hook.hook_type.display_name().to_string(),
            tool_pattern: hook.tool_pattern.clone(),
            command: hook.command.clone(),
            enabled: hook.enabled,
            created_at: hook.created_at.clone(),
            source: hook.source.clone(),
            read_only: hook.is_package_hook()
                || (user_id.is_some() && hook.owner_user_id().is_none()),
        }
    }
}

/// Request to create a new hook
#[derive(Deserialize)]
pub struct CreateHookRequest {
    pub hook_type: String,
    pub tool_pattern: String,
    pub command: String,
}

/// List all user hooks
async fn list_hooks(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
) -> Result<Json<Vec<HookResponse>>, AppError> {
    let user_id = current_user_id(user.as_ref());
    let manager = state.hook_manager.read().await;
    let hooks = manager
        .hooks_for_user(user_id)
        .into_iter()
        .map(|hook| HookResponse::for_requester(hook, user_id))
        .collect();
    Ok(Json(hooks))
}

/// Create a new hook
async fn create_hook(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Json(req): Json<CreateHookRequest>,
) -> Result<(StatusCode, Json<HookResponse>), AppError> {
    let user_id = current_user_id(user.as_ref());
    // Parse hook type
    let hook_type = UserHookType::parse(&req.hook_type)
        .ok_or_else(|| AppError::BadRequest(format!("Invalid hook type: {}", req.hook_type)))?;

    // Validate regex pattern
    if regex::Regex::new(&req.tool_pattern).is_err() {
        return Err(AppError::BadRequest(format!(
            "Invalid regex pattern: {}",
            req.tool_pattern
        )));
    }

    // Validate command not empty
    if req.command.trim().is_empty() {
        return Err(AppError::BadRequest("Command cannot be empty".to_string()));
    }

    // Create the hook
    let hook = UserHook::new(hook_type, req.tool_pattern, req.command);
    let hook_id = hook.id.clone();

    // Save to database
    let db = Database::new(&state.db_path)?;
    let mut manager = state.hook_manager.write().await;
    manager
        .save_for_user(&db, hook, user_id)
        .map_err(|e| AppError::Internal(format!("Failed to save hook: {}", e)))?;
    let response = manager
        .hooks_for_user(user_id)
        .into_iter()
        .find(|hook| hook.id == hook_id)
        .map(|hook| HookResponse::for_requester(hook, user_id))
        .ok_or_else(|| AppError::Internal("Saved hook was not available at runtime".to_string()))?;

    Ok((StatusCode::CREATED, Json(response)))
}

/// Toggle a hook's enabled state
async fn toggle_hook(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<HookResponse>, AppError> {
    let user_id = current_user_id(user.as_ref());
    let db = Database::new(&state.db_path)?;
    let mut manager = state.hook_manager.write().await;

    manager
        .toggle_for_user(&db, &id, user_id)
        .map_err(|error| hook_mutation_error("toggle", error))?;

    // Find the updated hook
    let hook = manager
        .hooks_for_user(user_id)
        .into_iter()
        .find(|h| h.id == id)
        .ok_or_else(|| AppError::NotFound(format!("Hook {} not found", id)))?;

    Ok(Json(HookResponse::for_requester(hook, user_id)))
}

/// Delete a hook
async fn delete_hook(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let user_id = current_user_id(user.as_ref());
    let db = Database::new(&state.db_path)?;
    let mut manager = state.hook_manager.write().await;

    manager
        .delete_for_user(&db, &id, user_id)
        .map_err(|error| hook_mutation_error("delete", error))?;

    Ok(StatusCode::NO_CONTENT)
}

fn hook_mutation_error(action: &str, error: anyhow::Error) -> AppError {
    if error.to_string().contains("read-only") {
        AppError::Conflict(format!(
            "Package hooks are read-only and cannot be {action}d; change the contributing plugin instead"
        ))
    } else if error.to_string().contains("not found") {
        AppError::NotFound("Hook not found for current user".to_string())
    } else {
        AppError::Internal(format!("Failed to {action} hook: {error}"))
    }
}
