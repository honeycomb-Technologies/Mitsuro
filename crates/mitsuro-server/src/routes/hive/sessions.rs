use std::convert::Infallible;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, Sse},
    Json,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use mitsuro_core::ai::models::ModelKey;
use mitsuro_core::storage::{
    AutonomousTask, AutonomousTaskStore, Database, HiveRunPriority, HiveRuntimeState,
    HiveRuntimeStateStore, RuntimeTraceEvent, SessionType, WorkspaceMode,
};
use mitsuro_core::SessionManager;

use super::super::session_access::{
    current_user_id, ensure_owned_session_of_type, load_agent_state_or_idle,
    load_owned_hive_session_control_scope, load_owned_session_of_type, request_workspace_scope,
    session_visible_to_user,
};
use super::{
    current, idempotency_key_from_headers, open_session_manager, resolve_hive_model, OkResponse,
};
use crate::ai_bootstrap::{resolve_preferred_model, resolve_preferred_model_key};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::AgenticEvent;
use crate::utils::text::trimmed_nonempty;
use crate::utils::workspace::resolve_optional_workspace_path;
use crate::AppState;

const DEFAULT_HIVE_REPLAY_LIMIT: usize = 50;
const MAX_HIVE_REPLAY_LIMIT: usize = 200;
const HIVE_EVENT_STREAM_BUFFER: usize = 256;
const CANONICAL_HIVE_SESSION_TYPE: &str = "hive";

#[derive(Debug, Deserialize)]
pub(super) struct DispatchRequest {
    pub(super) task: String,
    pub(super) project_dir: Option<String>,
    pub(super) model: Option<String>,
    #[serde(default)]
    pub(super) model_key: Option<ModelKey>,
    pub(super) start_at: Option<String>,
    pub(super) priority: Option<HiveRunPriority>,
    pub(super) crew_slug: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct MessageRequest {
    pub(super) message: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ScheduleRequest {
    pub(super) start_at: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct PriorityRequest {
    pub(super) priority: HiveRunPriority,
}

#[derive(Debug, Deserialize)]
pub(super) struct CrewRequest {
    pub(super) crew_slug: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ObserveEventsQuery {
    pub(super) replay_limit: Option<usize>,
    pub(super) after_sequence: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(super) struct DispatchResponse {
    pub(super) session_id: String,
    pub(super) status: String,
}

#[derive(Debug, Serialize)]
pub(super) struct HiveSessionSummary {
    pub(super) session_id: String,
    pub(super) title: String,
    pub(super) updated_at: String,
    pub(super) project_dir: Option<String>,
    pub(super) target_branch: Option<String>,
    pub(super) agent_state: String,
    pub(super) runtime: Option<HiveRuntimeState>,
}

#[derive(Debug, Serialize)]
pub(super) struct HiveSessionStatus {
    pub(super) session_id: String,
    pub(super) session_type: &'static str,
    pub(super) title: String,
    pub(super) tasks: Vec<AutonomousTask>,
    pub(super) agent_state: String,
    pub(super) runtime: Option<HiveRuntimeState>,
    pub(super) cadence: current::HiveCadenceSummary,
}

#[derive(Debug, Serialize)]
pub(super) struct RecoverDaemonResponse {
    pub(super) ok: bool,
    pub(super) recovered_count: usize,
}

#[derive(Debug, Serialize)]
pub(super) struct HiveMainResponse {
    pub(super) session_id: String,
    pub(super) title: String,
    pub(super) session_type: &'static str,
    pub(super) permission_mode: String,
    pub(super) created: bool,
    pub(super) agent_state: String,
}

/// Ensure/get the singleton Hive companion chat for the current user.
///
/// This is the durable relationship thread (Telegram/OpenClaw-style), not a job
/// run. Dispatch continues to create separate autonomous work sessions.
pub(super) async fn main_session(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
) -> Result<Json<HiveMainResponse>, AppError> {
    main_session_with_wire_identity(state, user, CANONICAL_HIVE_SESSION_TYPE).await
}

pub(super) async fn legacy_main_session(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
) -> Result<Json<HiveMainResponse>, AppError> {
    main_session_with_wire_identity(state, user, crate::legacy_identity::HIVE_SESSION_TYPE).await
}

async fn main_session_with_wire_identity(
    state: AppState,
    user: Option<CurrentUser>,
    wire_session_type: &'static str,
) -> Result<Json<HiveMainResponse>, AppError> {
    let session_manager = open_session_manager(&state)?;
    let user_id = current_user_id(user.as_ref());
    let before = session_manager
        .list_sessions_for_user_by_type(None, user_id, SessionType::Hive)?
        .into_iter()
        .filter(|session| {
            session.parent_session_id.is_none()
                && session.project_dir.is_none()
                && matches!(session.workspace_mode, WorkspaceMode::Neutral)
        })
        .map(|session| session.id)
        .collect::<std::collections::HashSet<_>>();

    let session = session_manager.ensure_hive_main_session(user_id)?;
    if !session_visible_to_user(&session, user_id) {
        return Err(AppError::NotFound("Hive main session".into()));
    }

    let agent_state = load_agent_state_or_idle(&session_manager, &session.id)?.state;
    Ok(Json(HiveMainResponse {
        session_id: session.id.clone(),
        title: session.title,
        session_type: wire_session_type,
        permission_mode: session.permission_mode.as_str().to_string(),
        created: !before.contains(&session.id),
        agent_state,
    }))
}

pub(super) async fn dispatch(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    headers: HeaderMap,
    Json(req): Json<DispatchRequest>,
) -> Result<(StatusCode, Json<DispatchResponse>), AppError> {
    let session_manager = open_session_manager(&state)?;
    let idempotency_key = idempotency_key_from_headers(&headers)?;
    let user_id = current_user_id(user.as_ref());
    let workspace_scope = request_workspace_scope(&state, user.as_ref());
    let task = req.task.trim();
    if task.is_empty() {
        return Err(AppError::BadRequest("task must not be empty".to_string()));
    }

    let working_dir = resolve_optional_workspace_path(
        req.project_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?
    .unwrap_or_else(|| workspace_scope.base_dir.to_string_lossy().into_owned());
    let start_at = parse_requested_wake_at(req.start_at.as_deref())?;
    let explicit_model = trimmed_nonempty(req.model.as_deref()).map(ToOwned::to_owned);
    let preferred_key = (explicit_model.is_none() && req.model_key.is_none())
        .then(|| resolve_preferred_model_key(state.db_path.as_ref().as_path(), user_id))
        .flatten();
    let requested_model = explicit_model.or_else(|| {
        (preferred_key.is_none())
            .then(|| resolve_preferred_model(state.db_path.as_ref().as_path(), user_id))
            .flatten()
    });
    let resolved_model = resolve_hive_model(
        &state,
        user_id,
        requested_model.as_deref(),
        req.model_key.as_ref().or(preferred_key.as_ref()),
    )
    .await?;
    let protocol_model_key = resolved_model.protocol_key()?;
    let priority = req.priority.unwrap_or(HiveRunPriority::Normal);
    let crew_slug = req
        .crew_slug
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if let Some(crew_slug) = crew_slug.as_deref() {
        if !mitsuro_core::storage::is_valid_crew_slug(crew_slug) {
            return Err(AppError::BadRequest("invalid crew slug".to_string()));
        }
    }

    if state.hive_runtime.is_daemon_backed() {
        let result = state
            .hive_runtime
            .dispatch_for_user(
                user_id,
                task,
                &working_dir,
                Some(&working_dir),
                Some(resolved_model.model.as_str()),
                Some(&protocol_model_key),
                resolved_model.catalog_revision.as_deref(),
                start_at,
                priority,
                crew_slug.as_deref(),
                idempotency_key.as_deref(),
            )
            .await
            .map_err(hive_control_error)?;
        return Ok((
            StatusCode::CREATED,
            Json(DispatchResponse {
                session_id: result.session_id,
                status: result.status,
            }),
        ));
    }

    // Embedded mode remains available for focused runtime tests. Production
    // router construction is fail-closed and always takes the daemon branch.
    let session_id = session_manager.create_session_for_user_with_config(
        task,
        Some(resolved_model.model.as_str()),
        Some(working_dir.as_str()),
        Some(working_dir.as_str()),
        WorkspaceMode::Selected,
        current_user_id(user.as_ref()),
        None,
        SessionType::Hive,
    )?;
    session_manager.update_session_model_selection(
        &session_id,
        Some(&resolved_model.key),
        resolved_model.catalog_revision.as_deref(),
    )?;
    let runtime_store = HiveRuntimeStateStore::new(Database::new(&state.db_path)?);
    runtime_store.set_priority(&session_id, priority)?;
    runtime_store.set_crew_slug(&session_id, crew_slug.as_deref())?;

    let content_json = serde_json::json!([{ "type": "text", "text": task }]).to_string();
    session_manager.save_message(&session_id, "user", &content_json)?;
    let status = if let Some(wake_at) = start_at {
        state
            .hive_runtime
            .schedule_session_for_user(
                &state,
                session_id.clone(),
                wake_at,
                "scheduled_dispatch",
                "scheduled",
                user_id,
                idempotency_key.as_deref(),
            )
            .await
            .map_err(hive_control_error)?;
        "scheduled"
    } else {
        state
            .hive_runtime
            .start_or_restart_session_for_user(
                state.clone(),
                session_id.clone(),
                "dispatch",
                user_id,
                idempotency_key.as_deref(),
            )
            .await
            .map_err(hive_control_error)?;
        "started"
    };

    Ok((
        StatusCode::CREATED,
        Json(DispatchResponse {
            session_id,
            status: status.to_string(),
        }),
    ))
}

pub(super) async fn list_sessions(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
) -> Result<Json<Vec<HiveSessionSummary>>, AppError> {
    let session_manager = open_session_manager(&state)?;
    let runtime_store = HiveRuntimeStateStore::new(Database::new(&state.db_path)?);

    let user_id = user.as_ref().and_then(|u| u.0.user_id.as_deref());
    let all_sessions = session_manager
        .list_sessions_for_user_by_type(None, user_id, SessionType::Hive)?
        .into_iter()
        .filter(|session| session_visible_to_user(session, user_id))
        .collect::<Vec<_>>();
    let runtime_states = runtime_store.list_states_for_sessions(
        &all_sessions
            .iter()
            .map(|session| session.id.clone())
            .collect::<Vec<_>>(),
    )?;
    let mut hive_sessions = Vec::new();

    for session in all_sessions {
        let agent_state = load_agent_state_or_idle(&session_manager, &session.id)?.state;
        let runtime = runtime_states.get(&session.id).cloned();

        hive_sessions.push(HiveSessionSummary {
            session_id: session.id,
            title: session.title,
            updated_at: session.updated_at.to_rfc3339(),
            project_dir: session.project_dir,
            target_branch: session.target_branch,
            agent_state,
            runtime,
        });
    }

    Ok(Json(hive_sessions))
}

pub(super) async fn recover_daemon(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    headers: HeaderMap,
) -> Result<Json<RecoverDaemonResponse>, AppError> {
    let idempotency_key = idempotency_key_from_headers(&headers)?;
    let user_id = user.as_ref().and_then(|u| u.0.user_id.as_deref());
    let recoverable_states = {
        let session_manager = open_session_manager(&state)?;
        let runtime_store = HiveRuntimeStateStore::new(Database::new(&state.db_path)?);
        let mut states = Vec::new();
        for runtime_state in runtime_store.list_recoverable_states()? {
            let Some(session) = session_manager.get_session(&runtime_state.session_id)? else {
                continue;
            };
            if session.session_type != SessionType::Hive {
                continue;
            }
            if session.user_id.as_deref() != user_id {
                continue;
            }
            states.push(runtime_state);
        }
        states
    };
    for runtime_state in &recoverable_states {
        bind_frozen_session_model(&state, user.as_ref(), &runtime_state.session_id).await?;
    }
    if state.hive_runtime.is_daemon_backed() {
        let recovered_count = state
            .hive_runtime
            .recover_all_for_user(user_id, idempotency_key.as_deref())
            .await
            .map_err(hive_control_error)?;
        return Ok(Json(RecoverDaemonResponse {
            ok: true,
            recovered_count,
        }));
    }
    let mut recovered_count = 0usize;

    for runtime_state in recoverable_states {
        state
            .hive_runtime
            .recover_persisted_state_for_user(
                state.clone(),
                &runtime_state,
                "manual_recover",
                user_id,
                idempotency_key.as_deref(),
            )
            .await
            .map_err(hive_control_error)?;
        recovered_count += 1;
    }

    Ok(Json(RecoverDaemonResponse {
        ok: true,
        recovered_count,
    }))
}

pub(super) async fn session_status(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<HiveSessionStatus>, AppError> {
    session_status_with_wire_identity(state, user, id, CANONICAL_HIVE_SESSION_TYPE).await
}

pub(super) async fn legacy_session_status(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
) -> Result<Json<HiveSessionStatus>, AppError> {
    session_status_with_wire_identity(state, user, id, crate::legacy_identity::HIVE_SESSION_TYPE)
        .await
}

async fn session_status_with_wire_identity(
    state: AppState,
    user: Option<CurrentUser>,
    id: String,
    wire_session_type: &'static str,
) -> Result<Json<HiveSessionStatus>, AppError> {
    let session_manager = open_session_manager(&state)?;
    let workspace_scope = request_workspace_scope(&state, user.as_ref());
    let session = load_owned_session_of_type(
        &session_manager,
        &id,
        SessionType::Hive,
        "Hive",
        user.as_ref(),
    )?;

    let agent_state = load_agent_state_or_idle(&session_manager, &id)?.state;

    let task_store = AutonomousTaskStore::new(Database::new(&state.db_path)?);
    let tasks = task_store.list_tasks(&id)?;
    let runtime = HiveRuntimeStateStore::new(Database::new(&state.db_path)?).get_state(&id)?;
    let cadence = current::load_hive_cadence(
        session.project_dir.as_deref(),
        session.working_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    );

    Ok(Json(HiveSessionStatus {
        session_id: id,
        session_type: wire_session_type,
        title: session.title,
        tasks,
        agent_state,
        runtime,
        cadence,
    }))
}

pub(super) async fn observe_events(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    Query(query): Query<ObserveEventsQuery>,
) -> Result<Sse<ReceiverStream<std::result::Result<Event, Infallible>>>, AppError> {
    let session_manager = open_session_manager(&state)?;
    ensure_owned_session_of_type(
        &session_manager,
        &id,
        SessionType::Hive,
        "Hive",
        user.as_ref(),
    )?;

    let user_id = current_user_id(user.as_ref());
    let replay_limit = query
        .replay_limit
        .unwrap_or(DEFAULT_HIVE_REPLAY_LIMIT)
        .min(MAX_HIVE_REPLAY_LIMIT);
    let (receiver, replay_events) = if state.hive_runtime.is_daemon_backed() {
        // The daemon sequence is the sole production replay cursor. Mixing it
        // with the server's legacy runtime-trace sequence duplicates events for
        // the first observer and drops history for later observers.
        let receiver = state
            .hive_runtime
            .subscribe_for_user_from(&id, user_id, query.after_sequence, Some(replay_limit))
            .await
            .map_err(hive_control_error)?;
        (receiver, Vec::new())
    } else {
        (
            state
                .hive_runtime
                .subscribe_for_user(&id, user_id)
                .await
                .map_err(hive_control_error)?,
            load_hive_replay_events(&session_manager, &id, &query)?,
        )
    };
    let (tx, rx) =
        mpsc::channel::<std::result::Result<Event, Infallible>>(HIVE_EVENT_STREAM_BUFFER);
    crate::routes::sse::spawn_broadcast_sse_pump(receiver, tx, replay_events, false);
    Ok(crate::routes::sse::sse_response(rx))
}

pub(super) async fn send_message(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<MessageRequest>,
) -> Result<Json<OkResponse>, AppError> {
    let session_manager = open_session_manager(&state)?;
    ensure_owned_session_of_type(
        &session_manager,
        &id,
        SessionType::Hive,
        "Hive",
        user.as_ref(),
    )?;

    let message = req.message.trim();
    if message.is_empty() {
        return Err(AppError::BadRequest(
            "message must not be empty".to_string(),
        ));
    }

    bind_frozen_session_model(&state, user.as_ref(), &id).await?;
    let user_id = current_user_id(user.as_ref());
    let idempotency_key = idempotency_key_from_headers(&headers)?;
    state
        .hive_runtime
        .send_message_for_user(
            state.clone(),
            id.clone(),
            message,
            user_id,
            idempotency_key.as_deref(),
        )
        .await
        .map_err(hive_control_error)?;

    Ok(Json(OkResponse { ok: true }))
}

pub(super) async fn pause_session(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<OkResponse>, AppError> {
    let session_manager = open_session_manager(&state)?;
    load_owned_hive_session_control_scope(&state, &session_manager, &id, user.as_ref())?
        .reject_generic_worker_control()?;
    let idempotency_key = idempotency_key_from_headers(&headers)?;
    state
        .hive_runtime
        .pause_session_for_user(
            &state,
            &id,
            current_user_id(user.as_ref()),
            idempotency_key.as_deref(),
        )
        .await
        .map_err(hive_control_error)?;
    Ok(Json(OkResponse { ok: true }))
}

pub(super) async fn schedule_session(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<ScheduleRequest>,
) -> Result<Json<OkResponse>, AppError> {
    let session_manager = open_session_manager(&state)?;
    load_owned_hive_session_control_scope(&state, &session_manager, &id, user.as_ref())?
        .reject_generic_worker_control()?;
    let wake_at = parse_requested_wake_at(Some(req.start_at.as_str()))?
        .ok_or_else(|| AppError::BadRequest("start_at must be provided".to_string()))?;
    bind_frozen_session_model(&state, user.as_ref(), &id).await?;
    let idempotency_key = idempotency_key_from_headers(&headers)?;
    state
        .hive_runtime
        .schedule_session_for_user(
            &state,
            id,
            wake_at,
            "manual_schedule",
            "scheduled",
            current_user_id(user.as_ref()),
            idempotency_key.as_deref(),
        )
        .await
        .map_err(hive_control_error)?;
    Ok(Json(OkResponse { ok: true }))
}

pub(super) async fn set_priority(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<PriorityRequest>,
) -> Result<Json<OkResponse>, AppError> {
    let session_manager = open_session_manager(&state)?;
    ensure_owned_session_of_type(
        &session_manager,
        &id,
        SessionType::Hive,
        "Hive",
        user.as_ref(),
    )?;
    let idempotency_key = idempotency_key_from_headers(&headers)?;
    state
        .hive_runtime
        .set_priority_for_user(
            &state,
            &id,
            req.priority,
            current_user_id(user.as_ref()),
            idempotency_key.as_deref(),
        )
        .await
        .map_err(hive_control_error)?;
    Ok(Json(OkResponse { ok: true }))
}

pub(super) async fn set_crew(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<CrewRequest>,
) -> Result<Json<OkResponse>, AppError> {
    let session_manager = open_session_manager(&state)?;
    ensure_owned_session_of_type(
        &session_manager,
        &id,
        SessionType::Hive,
        "Hive",
        user.as_ref(),
    )?;

    let crew_slug = req
        .crew_slug
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if let Some(crew_slug) = crew_slug.as_deref() {
        if !mitsuro_core::storage::is_valid_crew_slug(crew_slug) {
            return Err(AppError::BadRequest("invalid crew slug".to_string()));
        }
    }

    let idempotency_key = idempotency_key_from_headers(&headers)?;
    state
        .hive_runtime
        .set_crew_for_user(
            &state,
            &id,
            crew_slug.as_deref(),
            current_user_id(user.as_ref()),
            idempotency_key.as_deref(),
        )
        .await
        .map_err(hive_control_error)?;
    Ok(Json(OkResponse { ok: true }))
}

pub(super) async fn resume_session(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<OkResponse>, AppError> {
    let session_manager = open_session_manager(&state)?;
    load_owned_hive_session_control_scope(&state, &session_manager, &id, user.as_ref())?
        .reject_generic_worker_control()?;
    bind_frozen_session_model(&state, user.as_ref(), &id).await?;
    let idempotency_key = idempotency_key_from_headers(&headers)?;
    state
        .hive_runtime
        .resume_session_for_user(
            state.clone(),
            id.clone(),
            current_user_id(user.as_ref()),
            idempotency_key.as_deref(),
        )
        .await
        .map_err(hive_control_error)?;
    Ok(Json(OkResponse { ok: true }))
}

pub(super) async fn cancel_session(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, AppError> {
    let session_manager = open_session_manager(&state)?;
    load_owned_hive_session_control_scope(&state, &session_manager, &id, user.as_ref())?
        .reject_generic_worker_control()?;

    let idempotency_key = idempotency_key_from_headers(&headers)?;
    state
        .hive_runtime
        .delete_session_for_user(
            &state,
            &id,
            current_user_id(user.as_ref()),
            idempotency_key.as_deref(),
        )
        .await
        .map_err(hive_control_error)?;

    Ok(StatusCode::NO_CONTENT)
}

pub(super) fn hive_control_error(error: anyhow::Error) -> AppError {
    crate::hive_runtime::control_plane_app_error(error)
}

async fn bind_frozen_session_model(
    state: &AppState,
    user: Option<&CurrentUser>,
    session_id: &str,
) -> Result<(), AppError> {
    let session_manager = open_session_manager(state)?;
    let session = load_owned_session_of_type(
        &session_manager,
        session_id,
        SessionType::Hive,
        "Hive",
        user,
    )?;
    if session.model.is_none() && session.model_key.is_none() {
        return Err(AppError::Conflict(
            "Hive session has no frozen model; start a new Hive session".into(),
        ));
    }
    let resolved = resolve_hive_model(
        state,
        current_user_id(user),
        session.model.as_deref(),
        session.model_key.as_ref(),
    )
    .await
    .map_err(|error| match error {
        AppError::BadRequest(message) => AppError::Conflict(message),
        error => error,
    })?;
    let should_backfill_key = session.model_key.is_none();
    let should_backfill_revision = session.model_key.as_ref() == Some(&resolved.key)
        && session.model_catalog_revision.is_none()
        && resolved.catalog_revision.is_some();
    if should_backfill_key || should_backfill_revision {
        session_manager.update_session_model_selection(
            session_id,
            Some(&resolved.key),
            session
                .model_catalog_revision
                .as_deref()
                .or(resolved.catalog_revision.as_deref()),
        )?;
    }
    Ok(())
}

fn load_hive_replay_events(
    session_manager: &SessionManager,
    session_id: &str,
    query: &ObserveEventsQuery,
) -> Result<Vec<AgenticEvent>, AppError> {
    let limit = query
        .replay_limit
        .unwrap_or(DEFAULT_HIVE_REPLAY_LIMIT)
        .min(MAX_HIVE_REPLAY_LIMIT);

    let trace_events = match query.after_sequence {
        Some(after_sequence) => session_manager.load_runtime_trace_events_after(
            session_id,
            after_sequence,
            Some(limit),
        )?,
        None if limit == 0 => Vec::new(),
        None => session_manager.load_runtime_trace_events(session_id, Some(limit))?,
    };

    Ok(trace_events
        .into_iter()
        .filter_map(map_runtime_trace_event)
        .collect())
}

pub(super) fn map_runtime_trace_event(event: RuntimeTraceEvent) -> Option<AgenticEvent> {
    let sequence = event.sequence;
    let event_type = event.event_type.clone();
    let mapped = AgenticEvent::from_runtime_trace(event);
    if mapped.is_none() {
        tracing::warn!(
            sequence,
            event_type,
            "Skipping persisted runtime trace event that could not be replayed"
        );
    }
    mapped
}

fn parse_requested_wake_at(
    value: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, AppError> {
    let Some(raw) = trimmed_nonempty(value) else {
        return Ok(None);
    };

    let wake_at = chrono::DateTime::parse_from_rfc3339(raw)
        .map(|date| date.with_timezone(&chrono::Utc))
        .map_err(|_| {
            AppError::BadRequest("start_at must be a valid RFC3339 timestamp".to_string())
        })?;
    if wake_at <= chrono::Utc::now() {
        return Err(AppError::BadRequest(
            "start_at must be in the future".to_string(),
        ));
    }

    Ok(Some(wake_at))
}
