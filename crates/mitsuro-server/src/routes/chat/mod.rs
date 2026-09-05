//! Chat endpoint with SSE streaming via core orchestrator.

mod content;
mod interactions;
mod session;
mod stream;
mod stream_notify;
mod tools;

use std::convert::Infallible;
use std::path::PathBuf;

use axum::{
    extract::State,
    http::HeaderMap,
    response::sse::{Event, Sse},
    routing::post,
    Json, Router,
};
use mitsuro_core::ai::providers::ReasoningEffort;
use mitsuro_core::ai::types::{ModelMessage, Role};
use mitsuro_core::storage::{Database, SessionType, WorkMode};
use mitsuro_core::tools::registry::PermissionMode;
use mitsuro_core::SessionManager;
use tokio_stream::wrappers::ReceiverStream;

use self::content::{build_user_content, content_blocks_include_images, validate_content_blocks};
pub(crate) use self::interactions::deliver_steering_with_rollover;
pub(crate) use self::interactions::submit_tool_approval;
use self::interactions::{
    hive_control_error, hive_response_sse, steer, tool_approval, tool_result,
};
#[cfg(test)]
use self::session::{prepare_chat_contract_for_test, select_model_for_chat_request};
use self::session::{
    prepare_chat_route_session, refresh_chat_code_tool_surface, setup_chat_session,
    setup_chat_session_with_guard, ChatSessionContext, RequestedModel,
};
#[cfg(test)]
use self::stream::{
    forward_loop_event, run_delegated_progress_bridge, run_orchestrator_event_bridge,
};
use self::stream::{start_orchestrator_detached, start_orchestrator_sse};
use self::tools::{restrict_tools_to_allowlist, should_suppress_code_tools};
use super::session_access::{current_user_id, load_owned_session};
use crate::ai_bootstrap::{persist_current_model_key_selection, persist_current_model_selection};
use crate::auth::{AuthenticatedUser, CurrentUser};
use crate::error::AppError;
use crate::types::ChatRequest;
use crate::AppState;

const SSE_CHANNEL_BUFFER: usize = 256;
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(chat))
        .route("/steer", post(steer))
        .route("/tool-result", post(tool_result))
        .route("/tool-approval", post(tool_approval))
}

/// Start the explicit headless continuation contract for a durable child
/// completion. The completion event is trusted only after child_wake validates
/// its delegated-run/session ownership and captured workspace boundary.
pub(crate) async fn resume_child_completion_session(
    state: &AppState,
    session_id: &str,
    user_id: Option<String>,
    workspace_root: PathBuf,
    reasoning_effort: Option<ReasoningEffort>,
    guard: tokio::sync::OwnedMutexGuard<()>,
) -> Result<(), AppError> {
    let user = CurrentUser(AuthenticatedUser {
        user_id,
        home_dir: Some(workspace_root),
    });
    // Detached Agent groups durably inherit the launch turn's reasoning
    // effort. Preserve that immutable contract across the completion wake;
    // legacy groups without it retain the explicit safe default.
    let thinking_level = reasoning_effort
        .map(crate::types::ThinkingLevel::from_reasoning_effort)
        .unwrap_or(crate::types::ThinkingLevel::Off);
    let ctx = setup_chat_session_with_guard(
        state,
        Some(&user),
        session_id,
        RequestedModel::Unspecified,
        thinking_level,
        false,
        false,
        Some(guard),
    )
    .await?;
    let work_mode = ctx.work_mode;
    let permission_mode = ctx.permission_mode;
    start_orchestrator_detached(state, ctx, work_mode, permission_mode).await
}

/// Start an approved Goal using the same persisted session contract as chat.
/// The workflow route acquires the canonical session guard before mutating the
/// Goal, preventing approval from racing a second foreground loop.
pub(crate) async fn start_workflow_session_with_guard(
    state: &AppState,
    user: Option<&CurrentUser>,
    session_id: &str,
    guard: tokio::sync::OwnedMutexGuard<()>,
) -> Result<(), AppError> {
    let ctx = setup_chat_session_with_guard(
        state,
        user,
        session_id,
        RequestedModel::Unspecified,
        crate::types::ThinkingLevel::Off,
        false,
        false,
        Some(guard),
    )
    .await?;
    let work_mode = ctx.work_mode;
    let permission_mode = ctx.permission_mode;
    start_orchestrator_detached(state, ctx, work_mode, permission_mode).await
}
// ── Handlers ─────────────────────────────────────────────────────────

async fn chat(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    headers: HeaderMap,
    Json(req): Json<ChatRequest>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    validate_content_blocks(&req.content)?;

    let user_id = current_user_id(user.as_ref()).map(ToOwned::to_owned);
    let idempotency_key = super::hive::idempotency_key_from_headers(&headers)?;
    let requested_model =
        RequestedModel::from_request_parts(req.model.as_deref(), req.model_key.as_ref())?;
    let requested_session_type =
        validate_chat_session_request(&state, &req, user.as_ref(), &headers).await?;
    let requires_vision = content_blocks_include_images(&req.content);
    let prepared = prepare_chat_route_session(
        &state,
        user.as_ref(),
        &req,
        requested_model,
        requested_session_type,
        requires_vision,
    )
    .await?;
    let session_id = prepared.session_id;
    let session_type = prepared.session_type;
    let is_first_message = prepared.is_first_message;
    let pending_model_update = prepared.pending_model_update;

    if session_type == SessionType::Hive {
        return dispatch_hive_session_message(
            &state,
            state.db_path.as_ref().clone(),
            &req,
            &session_id,
            user_id.as_deref(),
            is_first_message,
            idempotency_key.as_deref(),
        )
        .await;
    }

    let mut ctx = setup_chat_session(
        &state,
        user.as_ref(),
        &session_id,
        requested_model,
        req.thinking_enabled,
        req.fast_mode,
        requires_vision,
    )
    .await?;

    apply_pending_model_update(&state, &mut ctx, pending_model_update, user_id.as_deref()).await?;

    let (permission_mode, work_mode) =
        resolve_run_modes(&state, &mut ctx, &req, &session_id).await?;

    apply_code_turn_tool_policy(&state, &mut ctx, &req).await?;

    let user_content = build_user_content(&req.message, &req.content)?;
    let user_content_json = serde_json::to_string(&user_content)?;

    ctx.conversation.push(ModelMessage {
        role: Role::User,
        content: user_content,
    });
    ctx.session_manager
        .save_message(&session_id, "user", &user_content_json)?;

    ctx.session_manager
        .update_session_permission_mode(&session_id, permission_mode)?;

    start_orchestrator_sse(&state, ctx, work_mode, permission_mode, is_first_message).await
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Validate the session-targeting parts of a chat request and return the
/// effective requested session type.
///
/// Hive sessions are daemon-owned: messages may not create them implicitly and
/// may not smuggle mutation overrides.
async fn validate_chat_session_request(
    state: &AppState,
    req: &ChatRequest,
    user: Option<&CurrentUser>,
    headers: &HeaderMap,
) -> Result<SessionType, AppError> {
    let requested_session_type = req.session_type.unwrap_or(SessionType::Code);
    if req.session_id.is_none() && requested_session_type == SessionType::Hive {
        return Err(AppError::Conflict(
            "Create Hive sessions through POST /hive/dispatch before sending chat messages".into(),
        ));
    }
    if let Some(session_id) = req.session_id.as_deref() {
        let manager = SessionManager::new(Database::new(&state.db_path)?);
        let existing = load_owned_session(&manager, session_id, user)?;
        if existing.session_type == SessionType::Hive
            && (req.model.is_some()
                || req.model_key.is_some()
                || req.target_branch.is_some()
                || req.mode.is_some()
                || req.permission_mode.is_some())
        {
            return Err(AppError::Conflict(
                "Hive model, branch, work mode, and permission mode are daemon-owned; send the message without mutation overrides".into(),
            ));
        }
        crate::control_leases::authorize_code_agent_mutation(
            state,
            session_id,
            existing.session_type,
            headers,
        )
        .await?;
    }
    Ok(requested_session_type)
}

/// Dispatch a message to a Hive session through its owning runtime.
async fn dispatch_hive_session_message(
    state: &AppState,
    db_path: std::path::PathBuf,
    req: &ChatRequest,
    session_id: &str,
    user_id: Option<&str>,
    is_first_message: bool,
    idempotency_key: Option<&str>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    if !req.content.is_empty() {
        return Err(AppError::BadRequest(
            "Hive daemon messages currently support text content only".to_string(),
        ));
    }
    let message = req.message.trim();
    if message.is_empty() {
        return Err(AppError::BadRequest(
            "A Hive message cannot be empty".to_string(),
        ));
    }
    let receiver = if state.hive_runtime.is_daemon_backed() {
        state
            .hive_runtime
            .begin_daemon_chat_turn_for_user(
                db_path,
                session_id,
                message,
                user_id,
                is_first_message,
                idempotency_key,
            )
            .await
            .map_err(hive_control_error)?
    } else {
        // Preserve the embedded runner used by focused tests. Its message
        // method persists and starts the run itself, unlike daemon IPC.
        let receiver = state
            .hive_runtime
            .subscribe_for_user(session_id, user_id)
            .await
            .map_err(hive_control_error)?;
        state
            .hive_runtime
            .send_message_for_user(
                state.clone(),
                session_id.to_string(),
                message,
                user_id,
                idempotency_key,
            )
            .await
            .map_err(hive_control_error)?;
        receiver
    };
    Ok(hive_response_sse(receiver))
}

/// Persist an explicit per-request model override on top of the resolved
/// session selection.
async fn apply_pending_model_update(
    state: &AppState,
    ctx: &mut ChatSessionContext,
    pending_model_update: Option<Option<String>>,
    user_id: Option<&str>,
) -> Result<(), AppError> {
    let Some(model_override) = pending_model_update else {
        return Ok(());
    };
    let session_id = &ctx.session_id;
    if model_override.is_none() {
        ctx.session_manager
            .update_session_model_selection(session_id, None, None)?;
        return Ok(());
    }
    let runtime = ctx.ai_client.resolved_model();
    if let Some(metadata) = state.model_registry.get_model_by_key(&runtime.key).await {
        ctx.session_manager.update_session_model_selection(
            session_id,
            Some(&runtime.key),
            metadata.catalog_revision.as_deref(),
        )?;
        persist_current_model_key_selection(
            &state.model_registry,
            state.db_path.as_ref().as_path(),
            user_id,
            &runtime.key,
        )
        .await?;
    } else if let Some(model) = model_override.as_deref() {
        ctx.session_manager
            .update_session_model(session_id, Some(model))?;
        persist_current_model_selection(
            &state.model_registry,
            state.db_path.as_ref().as_path(),
            user_id,
            model,
        )
        .await?;
    }
    Ok(())
}

/// Resolve the effective permission and work modes for this turn, applying
/// requested mode transitions to the governed tool surface.
async fn resolve_run_modes(
    state: &AppState,
    ctx: &mut ChatSessionContext,
    req: &ChatRequest,
    session_id: &str,
) -> Result<(PermissionMode, WorkMode), AppError> {
    // Hive sessions always run in autonomous mode (classifier provides safety gate).
    // Resolve this before a mode refresh so the rebuilt schema uses the same
    // permission contract that execution will enforce.
    let permission_mode = if ctx.session_type == SessionType::Hive {
        PermissionMode::Autonomous
    } else {
        req.permission_mode.unwrap_or(ctx.permission_mode)
    };

    let mut work_mode = ctx.work_mode;
    if let Some(requested_mode) = req.mode {
        if ctx.session_type != SessionType::Code {
            return Err(AppError::Conflict(
                "Build and Plan modes are only available for Code conversations".into(),
            ));
        }
        if requested_mode != work_mode {
            ctx.session_manager
                .update_session_work_mode(session_id, requested_mode)?;
            work_mode = requested_mode;
            refresh_chat_code_tool_surface(state, ctx, work_mode, permission_mode).await;
        }
    }
    Ok((permission_mode, work_mode))
}

/// Apply this turn's Code tool-surface policy: deterministic suppression for
/// non-tool turns and explicit per-turn allowlist restriction.
async fn apply_code_turn_tool_policy(
    state: &AppState,
    ctx: &mut ChatSessionContext,
    req: &ChatRequest,
) -> Result<(), AppError> {
    let _ = state;
    if ctx.session_type == SessionType::Code
        && should_suppress_code_tools(&req.message, !req.content.is_empty())
    {
        tracing::info!(
            session_id = %ctx.session_id,
            "Suppressing coding tools for a deterministic non-tool turn"
        );
        ctx.options.tools = None;
        ctx.options.parallel_tool_calls = Some(false);
        ctx.options.codex_parallel_tool_calls = false;
    }

    if let Some(allowed_tools) = req.allowed_tools.as_deref() {
        ctx.execution_tool_allowlist = Some(
            restrict_tools_to_allowlist(&mut ctx.options, allowed_tools)
                .map_err(AppError::BadRequest)?,
        );
    }
    Ok(())
}

/// Tools allowed in Chat sessions — conversation only, no file/bash/code tools.
/// Web search/fetch are the only tools; research behavior is automatic.
#[cfg(test)]
mod tests;
