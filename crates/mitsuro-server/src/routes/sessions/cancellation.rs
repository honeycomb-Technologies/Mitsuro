use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};

use mitsuro_core::agent::LoopInput;
use mitsuro_core::storage::{Database, DelegationStore};

use super::{current_user_id, load_owned_session, open_session_manager};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::SimpleOkResponse;
use crate::AppState;

pub(super) async fn cancel_session(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<SimpleOkResponse>, AppError> {
    let manager = open_session_manager(&state)?;
    let session = load_owned_session(&manager, &session_id, user.as_ref())?;

    if session.session_type == mitsuro_core::storage::SessionType::Hive {
        state
            .hive_runtime
            .cancel_session_for_user(&state, &session_id, current_user_id(user.as_ref()), None)
            .await
            .map_err(crate::hive_runtime::control_plane_app_error)?;
        return Ok(Json(SimpleOkResponse { ok: true }));
    }

    crate::control_leases::authorize_code_agent_mutation(
        &state,
        &session_id,
        session.session_type,
        &headers,
    )
    .await?;

    // Close the durable wake fence before signalling live work. This ordering
    // prevents a racing child finalizer from re-queuing a cancelled session.
    DelegationStore::new(Database::new(&state.db_path)?)
        .suppress_parent_continuations_for_session(&session_id)?;

    // A hosted background Agent run intentionally survives the foreground
    // loop that launched it. Cancel it by durable parent-session ownership as
    // well as signalling any foreground loop that is still active.
    state
        .tool_registry
        .agent_runtime_manager()
        .cancel_for_session(&session_id);

    let sender = state.session_inputs.read().await.get(&session_id).cloned();
    if let Some(sender) = sender {
        sender.send(LoopInput::Cancel).map_err(|_| {
            AppError::Conflict(format!(
                "Session {session_id} is no longer accepting cancellation"
            ))
        })?;
    }

    Ok(Json(SimpleOkResponse { ok: true }))
}
