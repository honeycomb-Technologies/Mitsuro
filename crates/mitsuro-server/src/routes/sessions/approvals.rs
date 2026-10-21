use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::routes::chat::submit_tool_approval;
use crate::types::ToolApprovalRequest;
use crate::AppState;

#[derive(Debug, Deserialize)]
pub(super) struct SessionToolApprovalRequest {
    #[serde(default)]
    run_id: Option<String>,
    tool_call_id: String,
    approved: bool,
}

pub(super) async fn tool_approval_for_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    user: Option<CurrentUser>,
    headers: HeaderMap,
    Json(req): Json<SessionToolApprovalRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let idempotency_key = crate::routes::hive::idempotency_key_from_headers(&headers)?;
    submit_tool_approval(
        &state,
        user.as_ref(),
        ToolApprovalRequest {
            session_id,
            run_id: req.run_id,
            tool_call_id: req.tool_call_id,
            approved: req.approved,
        },
        idempotency_key.as_deref(),
        &headers,
    )
    .await
}
