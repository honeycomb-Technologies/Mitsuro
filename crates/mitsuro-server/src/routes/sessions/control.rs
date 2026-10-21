use axum::{
    extract::{Path, State},
    Json,
};
use mitsuro_core::storage::SessionType;

use super::{load_owned_session, open_session_manager};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{
    HeartbeatSessionControlRequest, SessionControlStateResponse, TakeSessionControlRequest,
    TakeSessionControlResponse,
};
use crate::AppState;

pub(super) async fn get_session_control(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionControlStateResponse>, AppError> {
    ensure_code_session(&state, user.as_ref(), &session_id)?;
    let response = state
        .session_control_leases
        .write()
        .await
        .snapshot(&session_id);
    Ok(Json(response))
}

pub(super) async fn take_session_control(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(session_id): Path<String>,
    Json(request): Json<TakeSessionControlRequest>,
) -> Result<Json<TakeSessionControlResponse>, AppError> {
    ensure_code_session(&state, user.as_ref(), &session_id)?;
    let lease = state.session_control_leases.write().await.take(
        &session_id,
        request.client_id,
        request.force,
    )?;
    Ok(Json(TakeSessionControlResponse { lease }))
}

pub(super) async fn heartbeat_session_control(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(session_id): Path<String>,
    Json(request): Json<HeartbeatSessionControlRequest>,
) -> Result<Json<SessionControlStateResponse>, AppError> {
    ensure_code_session(&state, user.as_ref(), &session_id)?;
    let response = state.session_control_leases.write().await.heartbeat(
        &session_id,
        &request.client_id,
        &request.lease_token,
    )?;
    Ok(Json(response))
}

fn ensure_code_session(
    state: &AppState,
    user: Option<&CurrentUser>,
    session_id: &str,
) -> Result<(), AppError> {
    let manager = open_session_manager(state)?;
    let session = load_owned_session(&manager, session_id, user)?;
    if session.session_type != SessionType::Code {
        return Err(AppError::Conflict(
            "Only interactive Code Agent sessions use client control leases".to_owned(),
        ));
    }
    Ok(())
}
