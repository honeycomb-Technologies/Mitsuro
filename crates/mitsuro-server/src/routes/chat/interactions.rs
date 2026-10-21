use std::collections::HashSet;
use std::convert::Infallible;
use std::path::PathBuf;
use std::time::Duration;

use axum::{
    extract::State,
    http::HeaderMap,
    response::sse::{Event, Sse},
    Json,
};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use mitsuro_core::agent::plan_handler::parse_plan_confirm_choice;
use mitsuro_core::agent::LoopInput;
use mitsuro_core::ai::types::{Content, ModelMessage, Role};
use mitsuro_core::plan::PlanManager;
use mitsuro_core::storage::{
    hash_request_bytes, Database, HiveControllerEventStore, HiveControllerStore,
    HiveIdempotencyStore, IdempotencyClaim, PendingInteractionSnapshot, SessionRecoveryState,
    SessionType, WorkMode,
};
use mitsuro_core::tools::registry::PermissionMode;
use mitsuro_core::SessionManager;

use super::super::session_access::{current_user_id, load_owned_session};
use super::content::{build_user_content, validate_content_blocks};
use super::session::{refresh_chat_code_tool_surface, setup_chat_session, RequestedModel};
use super::stream::start_orchestrator_sse;
use super::tools::apply_thinking_config;
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{
    AgenticEvent, SteerRequest, ThinkingLevel, ToolApprovalRequest, ToolResultRequest,
};
use crate::AppState;

/// Releases only the transient continuation execution lease when route setup
/// fails after an answer was durably accepted. The accepted response remains
/// in recovery and can be reclaimed with the same value after retry/restart.
struct ContinuationClaimLease {
    db_path: PathBuf,
    session_id: String,
    interaction_id: String,
    accepted_response: String,
    armed: bool,
}

impl ContinuationClaimLease {
    fn new(
        db_path: PathBuf,
        session_id: &str,
        interaction_id: &str,
        accepted_response: &str,
    ) -> Self {
        Self {
            db_path,
            session_id: session_id.to_string(),
            interaction_id: interaction_id.to_string(),
            accepted_response: accepted_response.to_string(),
            armed: true,
        }
    }

    fn complete<T>(mut self, result: Result<T, AppError>) -> Result<T, AppError> {
        if result.is_ok() {
            self.armed = false;
        }
        result
    }
}

impl Drop for ContinuationClaimLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let result = Database::new(&self.db_path).and_then(|db| {
            SessionManager::new(db)
                .yield_awaiting_interaction_claim(
                    &self.session_id,
                    &self.interaction_id,
                    &self.accepted_response,
                )
                .map(|_| ())
        });
        if let Err(error) = result {
            tracing::error!(
                session_id = %self.session_id,
                interaction_id = %self.interaction_id,
                %error,
                "Failed to yield continuation execution lease after route failure"
            );
        }
    }
}

pub(super) async fn steer(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    headers: HeaderMap,
    Json(req): Json<SteerRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    validate_content_blocks(&req.content)?;
    if req.message.trim().is_empty() && req.content.is_empty() {
        return Err(AppError::BadRequest(
            "A live steering message cannot be empty".to_string(),
        ));
    }

    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let session = load_owned_session(&session_manager, &req.session_id, user.as_ref())?;

    let content = build_user_content(&req.message, &req.content)?;
    let pending_id = uuid::Uuid::new_v4().to_string();
    if session.session_type == SessionType::Hive {
        let idempotency_key = super::super::hive::idempotency_key_from_headers(&headers)?;
        let result = state
            .hive_runtime
            .steer_for_user(
                &state,
                &req.session_id,
                &pending_id,
                content,
                current_user_id(user.as_ref()),
                idempotency_key.as_deref(),
            )
            .await
            .map_err(hive_control_error)?;
        return Ok(Json(json!({
            "status": result.status.as_str(),
            "pending_id": pending_id,
            "active_run_id": result.active_run_id,
            "staged_input_id": result.staged_input_id,
            "successor_run_id": result.successor_run_id,
        })));
    }

    crate::control_leases::authorize_code_agent_mutation(
        &state,
        &req.session_id,
        session.session_type,
        &headers,
    )
    .await?;

    let sender = state
        .session_inputs
        .read()
        .await
        .get(&req.session_id)
        .cloned()
        .ok_or_else(|| {
            AppError::Conflict(format!(
                "Session {} has no active run to steer",
                req.session_id
            ))
        })?;

    let content_json = serde_json::to_string(&content)?;
    session_manager.queue_pending_steering(&req.session_id, &pending_id, &content_json)?;

    let input = LoopInput::Steer {
        pending_id: Some(pending_id.clone()),
        content,
    };
    let status = if deliver_steering_with_rollover(&state, &req.session_id, sender, input).await {
        "accepted"
    } else {
        // The message is already durable. A subsequent session start promotes
        // it in chronological order even if no replacement run was available.
        "queued"
    };

    Ok(Json(json!({
        "status": status,
        "pending_id": pending_id,
    })))
}

pub(crate) async fn deliver_steering_with_rollover(
    state: &AppState,
    session_id: &str,
    initial_sender: tokio::sync::mpsc::UnboundedSender<LoopInput>,
    input: LoopInput,
) -> bool {
    if initial_sender.send(input.clone()).is_ok() {
        return true;
    }

    // A run can roll over between cloning its sender and delivery. Retry a
    // replacement sender once with the same durable ID; never create another
    // staging row or duplicate canonical history.
    let replacement = state
        .session_inputs
        .read()
        .await
        .get(session_id)
        .cloned()
        .filter(|candidate| !candidate.same_channel(&initial_sender));
    replacement.is_some_and(|replacement| replacement.send(input).is_ok())
}

pub(super) async fn tool_result(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    headers: HeaderMap,
    Json(req): Json<ToolResultRequest>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let session = load_owned_session(&session_manager, &req.session_id, user.as_ref())?;
    if session.session_type == SessionType::Hive {
        let run_id = resolve_pending_hive_run(
            &state,
            &req.session_id,
            &req.tool_call_id,
            req.run_id.as_deref(),
            PendingHiveInteraction::UserResponse,
        )?;
        let idempotency_key = super::super::hive::idempotency_key_from_headers(&headers)?;
        let receiver = state
            .hive_runtime
            .user_response_and_subscribe_for_user(
                &state,
                &req.session_id,
                &run_id,
                &req.tool_call_id,
                &req.result,
                current_user_id(user.as_ref()),
                idempotency_key.as_deref(),
            )
            .await
            .map_err(hive_control_error)?;
        return Ok(hive_response_sse(receiver));
    }

    crate::control_leases::authorize_code_agent_mutation(
        &state,
        &req.session_id,
        session.session_type,
        &headers,
    )
    .await?;

    let mut ctx = setup_chat_session(
        &state,
        user.as_ref(),
        &req.session_id,
        RequestedModel::Unspecified,
        req.thinking_level,
        req.fast_mode,
        false,
    )
    .await?;

    // Permission mode and exact tool scope are one continuation contract. Load
    // the recovery snapshot once and fail the request if it cannot be read or
    // decoded; treating a recovery error as "no snapshot" would silently
    // resume with an unrestricted tool surface.
    let (recovery_state, pending_interaction) = claim_matching_continuation_recovery_state(
        &ctx.session_manager,
        &ctx.session_id,
        &req.tool_call_id,
        &req.result,
    )?;
    let claim_lease = ContinuationClaimLease::new(
        (*state.db_path).clone(),
        &req.session_id,
        &req.tool_call_id,
        &req.result,
    );
    let permission_mode = continuation_permission_mode(&ctx, req.permission_mode, &recovery_state);
    ctx.execution_tool_allowlist = continuation_execution_tool_allowlist(&recovery_state);
    ctx.session_manager
        .update_session_permission_mode(&req.session_id, permission_mode)?;

    // Plan confirmation is an internal orchestrator event, not a real tool call.
    // Don't add a ToolResult — instead add a user message to resume the conversation.
    if is_plan_confirmation(&pending_interaction) {
        let choice = parse_plan_confirm_choice(&req.result);
        let work_mode = if choice.as_deref() == Some("execute") {
            ctx.session_manager
                .update_session_work_mode(&req.session_id, WorkMode::Build)?;
            refresh_chat_code_tool_surface(&state, &mut ctx, WorkMode::Build, permission_mode)
                .await;
            persist_user_text_once(
                &mut ctx,
                "The plan has been approved. Begin executing the plan, starting with Task 1.1.",
            )?;
            WorkMode::Build
        } else {
            let plan_manager = PlanManager::new((*state.db_path).clone())?;
            plan_manager.abandon_plan(&req.session_id)?;
            persist_user_text_once(
                &mut ctx,
                "The plan has been abandoned. What would you like to do instead?",
            )?;
            ctx.work_mode
        };
        let result = start_orchestrator_sse(&state, ctx, work_mode, permission_mode, false).await;
        return claim_lease.complete(result);
    }

    let has_thinking = ctx.conversation.iter().any(|msg| {
        msg.content
            .iter()
            .any(|content| matches!(content, Content::Thinking { .. }))
    });
    // Older persisted turns may contain a thinking block without the exact
    // request-level setting. Preserve the level restored by setup_chat_session
    // and use High only as a legacy fallback when no setting was recovered.
    if has_thinking && ctx.options.thinking.is_none() {
        apply_thinking_config(ThinkingLevel::High, &mut ctx.options);
    }

    let merged = if let Some(last_msg) = ctx.conversation.last_mut() {
        if last_msg.role == Role::User
            && last_msg.content.iter().any(|content| {
                matches!(content, Content::ToolResult { tool_use_id, .. } if tool_use_id == req.tool_call_id.as_str())
            })
        {
            if let Some(output) = last_msg.content.iter_mut().find_map(|content| match content {
                Content::ToolResult {
                    tool_use_id, output, ..
                } if tool_use_id == req.tool_call_id.as_str() => Some(output),
                _ => None,
            }) {
                *output = serde_json::Value::String(req.result.clone());
            }
            let updated_json = serde_json::to_string(&last_msg.content)?;
            ctx.session_manager
                .update_last_message(&req.session_id, "user", &updated_json)?;
            true
        } else {
            false
        }
    } else {
        false
    };

    if !merged {
        let tool_result_content = vec![Content::ToolResult {
            tool_use_id: req.tool_call_id.clone(),
            output: serde_json::Value::String(req.result.clone()),
            is_error: None,
        }];
        let tool_result_json = serde_json::to_string(&tool_result_content)?;
        ctx.conversation.push(ModelMessage {
            role: Role::User,
            content: tool_result_content,
        });
        ctx.session_manager
            .save_message(&req.session_id, "user", &tool_result_json)?;
    }

    let work_mode = ctx.work_mode;
    let result = start_orchestrator_sse(&state, ctx, work_mode, permission_mode, false).await;
    claim_lease.complete(result)
}

fn persist_user_text_once(
    ctx: &mut super::session::ChatSessionContext,
    text: &str,
) -> Result<(), AppError> {
    let already_persisted = ctx.conversation.last().is_some_and(|message| {
        message.role == Role::User
            && matches!(message.content.as_slice(), [Content::Text { text: existing, .. }] if existing == text)
    });
    if already_persisted {
        return Ok(());
    }

    let content = vec![Content::text(text.to_string())];
    let content_json = serde_json::to_string(&content)?;
    ctx.conversation.push(ModelMessage {
        role: Role::User,
        content,
    });
    ctx.session_manager
        .save_message(&ctx.session_id, "user", &content_json)?;
    Ok(())
}

fn continuation_permission_mode(
    ctx: &super::session::ChatSessionContext,
    requested_permission_mode: Option<PermissionMode>,
    recovered_state: &SessionRecoveryState,
) -> PermissionMode {
    resumed_permission_mode(
        ctx.session_type,
        requested_permission_mode,
        recovered_state.permission_mode,
        ctx.permission_mode,
    )
}

fn continuation_execution_tool_allowlist(
    recovered_state: &SessionRecoveryState,
) -> Option<HashSet<String>> {
    resumed_execution_tool_allowlist(recovered_state.execution_tool_allowlist.clone())
}

fn claim_matching_continuation_recovery_state(
    session_manager: &SessionManager,
    session_id: &str,
    tool_call_id: &str,
    accepted_response: &str,
) -> Result<(SessionRecoveryState, PendingInteractionSnapshot), AppError> {
    session_manager
        .claim_awaiting_interaction(session_id, tool_call_id, accepted_response)?
        .ok_or_else(|| {
            AppError::Conflict(format!(
                "Session {session_id} has no awaiting-input continuation for tool call {tool_call_id}"
            ))
        })
}

fn is_plan_confirmation(pending: &PendingInteractionSnapshot) -> bool {
    matches!(pending, PendingInteractionSnapshot::PlanConfirm { .. })
}

fn resumed_execution_tool_allowlist(recovered: Option<Vec<String>>) -> Option<HashSet<String>> {
    recovered.map(|names| names.into_iter().collect())
}

fn resumed_permission_mode(
    session_type: SessionType,
    requested_permission_mode: Option<PermissionMode>,
    recovered_permission_mode: Option<PermissionMode>,
    session_permission_mode: PermissionMode,
) -> PermissionMode {
    if session_type == SessionType::Hive {
        PermissionMode::Autonomous
    } else {
        recovered_permission_mode
            .or(requested_permission_mode)
            .unwrap_or(session_permission_mode)
    }
}

pub(super) async fn tool_approval(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    headers: HeaderMap,
    Json(req): Json<ToolApprovalRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let idempotency_key = super::super::hive::idempotency_key_from_headers(&headers)?;
    submit_tool_approval(
        &state,
        user.as_ref(),
        req,
        idempotency_key.as_deref(),
        &headers,
    )
    .await
}

pub(crate) async fn submit_tool_approval(
    state: &AppState,
    user: Option<&CurrentUser>,
    req: ToolApprovalRequest,
    idempotency_key: Option<&str>,
    headers: &HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let session = load_owned_session(&session_manager, &req.session_id, user)?;

    if session.session_type == SessionType::Hive {
        let run_id = resolve_pending_hive_run(
            state,
            &req.session_id,
            &req.tool_call_id,
            req.run_id.as_deref(),
            PendingHiveInteraction::ToolApproval,
        )?;
        state
            .hive_runtime
            .tool_approval_for_user(
                state,
                &req.session_id,
                &run_id,
                &req.tool_call_id,
                req.approved,
                current_user_id(user),
                idempotency_key,
            )
            .await
            .map_err(hive_control_error)?;
        return Ok(Json(json!({"status": "ok"})));
    }

    crate::control_leases::authorize_code_agent_mutation(
        state,
        &req.session_id,
        session.session_type,
        headers,
    )
    .await?;

    let request_hash = idempotency_key.map(|_| {
        hash_request_bytes(
            serde_json::to_vec(&json!({
                "session_id": req.session_id,
                "tool_call_id": req.tool_call_id,
                "approved": req.approved,
            }))
            .unwrap_or_default(),
        )
    });
    let idempotency_scope = format!("chat-session:{}", req.session_id);
    let sender = {
        let inputs = state.session_inputs.read().await;
        inputs.get(&req.session_id).cloned()
    };
    let Some(sender) = sender else {
        if recovery_has_pending_tool_approval(&session_manager, &req.session_id, &req.tool_call_id)?
        {
            return Err(AppError::Conflict(format!(
                "Session {} has recoverable pending tool approval {}, but the approval channel unavailable after reload or restart. Reload /sessions/{}/state for pending_interactions and retry once the session is active.",
                req.session_id, req.tool_call_id, req.session_id
            )));
        }
        return Err(AppError::NotFound("No active session".into()));
    };
    if let (Some(key), Some(request_hash)) = (idempotency_key, request_hash.as_deref()) {
        match HiveIdempotencyStore::new(Database::new(&state.db_path)?).claim(
            &idempotency_scope,
            "tool_approval",
            key,
            request_hash,
            chrono::Utc::now(),
            Duration::from_secs(24 * 60 * 60),
        )? {
            IdempotencyClaim::Replay(record) => {
                return Ok(Json(
                    record.response.unwrap_or_else(|| json!({"status": "ok"})),
                ));
            }
            IdempotencyClaim::InProgress(_) => {
                if !recovery_has_pending_tool_approval(
                    &session_manager,
                    &req.session_id,
                    &req.tool_call_id,
                )? {
                    let response = json!({"status": "ok"});
                    let _ = HiveIdempotencyStore::new(Database::new(&state.db_path)?).complete(
                        &idempotency_scope,
                        "tool_approval",
                        key,
                        request_hash,
                        Some(&req.tool_call_id),
                        &response,
                        chrono::Utc::now(),
                    );
                    return Ok(Json(response));
                }
                return Err(AppError::Conflict(
                    "This tool approval is already being processed".into(),
                ));
            }
            IdempotencyClaim::Conflict { .. } => {
                return Err(AppError::Conflict(
                    "Idempotency-Key was already used for a different tool approval".into(),
                ));
            }
            IdempotencyClaim::Claimed(_) => {}
        }
    }
    if sender
        .send(LoopInput::ToolApproval {
            tool_call_id: req.tool_call_id.clone(),
            approved: req.approved,
        })
        .is_err()
    {
        if let (Some(key), Some(request_hash)) = (idempotency_key, request_hash.as_deref()) {
            let _ = HiveIdempotencyStore::new(Database::new(&state.db_path)?).release(
                &idempotency_scope,
                "tool_approval",
                key,
                request_hash,
            );
        }
        return Err(AppError::Conflict(format!(
            "Session {} is no longer accepting tool approvals",
            req.session_id
        )));
    }
    let response = json!({"status": "ok"});
    if let (Some(key), Some(request_hash)) = (idempotency_key, request_hash.as_deref()) {
        if let Err(error) = HiveIdempotencyStore::new(Database::new(&state.db_path)?).complete(
            &idempotency_scope,
            "tool_approval",
            key,
            request_hash,
            Some(&req.tool_call_id),
            &response,
            chrono::Utc::now(),
        ) {
            tracing::warn!(
                session_id = %req.session_id,
                tool_call_id = %req.tool_call_id,
                error = %error,
                "Tool approval succeeded but idempotency completion could not be persisted"
            );
        }
    }
    Ok(Json(response))
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PendingHiveInteraction {
    ToolApproval,
    UserResponse,
}

pub(super) fn resolve_pending_hive_run(
    state: &AppState,
    session_id: &str,
    tool_call_id: &str,
    requested_run_id: Option<&str>,
    interaction: PendingHiveInteraction,
) -> Result<String, AppError> {
    let controller = HiveControllerStore::new(Database::new(&state.db_path)?)
        .get_by_session(session_id)?
        .ok_or_else(|| AppError::Conflict("Hive session has no durable controller".into()))?;
    let event_store = HiveControllerEventStore::new(Database::new(&state.db_path)?);
    let pending = match interaction {
        PendingHiveInteraction::ToolApproval => event_store
            .list_pending_tool_approvals(&controller.id)?
            .into_iter()
            .filter(|event| {
                event.payload.get("id").and_then(serde_json::Value::as_str) == Some(tool_call_id)
            })
            .filter_map(|event| event.run_id)
            .collect::<std::collections::BTreeSet<_>>(),
        PendingHiveInteraction::UserResponse => event_store
            .list_pending_user_response_runs(&controller.id, tool_call_id)?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
    };

    if let Some(requested_run_id) = requested_run_id {
        if pending.contains(requested_run_id) {
            return Ok(requested_run_id.to_string());
        }
        return Err(AppError::Conflict(format!(
            "Run {requested_run_id} is not awaiting this interaction"
        )));
    }

    if pending.len() != 1 {
        return Err(AppError::Conflict(if pending.is_empty() {
            "No exact pending Hive run matches this interaction".into()
        } else {
            "Multiple Hive runs match this interaction; run_id is required".into()
        }));
    }
    Ok(pending.into_iter().next().expect("one pending run"))
}

pub(super) fn hive_response_sse(
    receiver: tokio::sync::broadcast::Receiver<AgenticEvent>,
) -> Sse<ReceiverStream<Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel(32);
    crate::routes::sse::spawn_broadcast_sse_pump(receiver, tx, Vec::new(), true);
    crate::routes::sse::sse_response(rx)
}

pub(super) fn hive_control_error(error: anyhow::Error) -> AppError {
    crate::hive_runtime::control_plane_app_error(error)
}

fn recovery_has_pending_tool_approval(
    session_manager: &SessionManager,
    session_id: &str,
    tool_call_id: &str,
) -> Result<bool, AppError> {
    let Some(recovery) = session_manager.load_recovery_state(session_id)? else {
        return Ok(false);
    };

    Ok(recovery.pending_interactions.iter().any(|pending| {
        matches!(
            pending,
            PendingInteractionSnapshot::ToolApproval { tool_call }
                if tool_call.id == tool_call_id
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitsuro_core::agent::loop_events::LoopStopReason;
    use mitsuro_core::storage::{
        PartialAssistantState, RecoveryDecision, RecoveryNonResumableReason, RecoveryStatus,
    };

    fn awaiting_recovery(pending: PendingInteractionSnapshot) -> SessionRecoveryState {
        SessionRecoveryState::new_with_pending_interactions(
            RecoveryStatus::AwaitingInput,
            Some(LoopStopReason::AwaitingInput),
            None,
            PartialAssistantState::default(),
            vec![pending],
            RecoveryDecision::NonResumable {
                reason: RecoveryNonResumableReason::AwaitingHumanInput,
            },
        )
    }

    #[test]
    fn resumed_permission_mode_preserves_supervised_for_code_sessions() {
        assert_eq!(
            resumed_permission_mode(
                SessionType::Code,
                Some(PermissionMode::Supervised),
                None,
                PermissionMode::Autonomous
            ),
            PermissionMode::Supervised
        );
    }

    #[test]
    fn resumed_permission_mode_preserves_autonomous_for_code_sessions() {
        assert_eq!(
            resumed_permission_mode(
                SessionType::Code,
                Some(PermissionMode::Autonomous),
                None,
                PermissionMode::Supervised
            ),
            PermissionMode::Autonomous
        );
    }

    #[test]
    fn resumed_permission_mode_prefers_recovered_mode_for_code_sessions() {
        assert_eq!(
            resumed_permission_mode(
                SessionType::Code,
                Some(PermissionMode::Autonomous),
                Some(PermissionMode::Supervised),
                PermissionMode::Autonomous
            ),
            PermissionMode::Supervised
        );
    }

    #[test]
    fn resumed_permission_mode_uses_session_mode_when_request_omits_it() {
        assert_eq!(
            resumed_permission_mode(SessionType::Code, None, None, PermissionMode::Supervised),
            PermissionMode::Supervised
        );
    }

    #[test]
    fn resumed_permission_mode_keeps_hive_sessions_autonomous() {
        assert_eq!(
            resumed_permission_mode(
                SessionType::Hive,
                Some(PermissionMode::Supervised),
                Some(PermissionMode::Supervised),
                PermissionMode::Supervised
            ),
            PermissionMode::Autonomous
        );
    }

    #[test]
    fn resumed_tool_scope_preserves_none_empty_and_exact_names() {
        assert_eq!(resumed_execution_tool_allowlist(None), None);
        assert_eq!(
            resumed_execution_tool_allowlist(Some(Vec::new())),
            Some(HashSet::new())
        );
        assert_eq!(
            resumed_execution_tool_allowlist(Some(vec![
                "read".to_string(),
                "tool_search".to_string(),
            ])),
            Some(HashSet::from([
                "read".to_string(),
                "tool_search".to_string(),
            ]))
        );
    }

    #[test]
    fn continuation_recovery_claim_fails_closed_on_malformed_state() {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let db_path = temp_dir.path().join("mitsuro.db");
        let session_manager =
            SessionManager::new(Database::new(&db_path).expect("database should initialize"));
        let session_id = session_manager
            .create_session("Malformed recovery", None, None)
            .expect("session should be created");

        let db = Database::new(&db_path).expect("database should reopen");
        db.conn()
            .execute(
                "UPDATE sessions SET recovery_json = ?1 WHERE id = ?2",
                ("{malformed", session_id.as_str()),
            )
            .expect("malformed recovery fixture should persist");

        assert!(matches!(
            claim_matching_continuation_recovery_state(
                &session_manager,
                &session_id,
                "ask-1",
                "answer"
            ),
            Err(AppError::Internal(_))
        ));
    }

    #[test]
    fn continuation_recovery_requires_present_matching_interaction() {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let db_path = temp_dir.path().join("mitsuro.db");
        let session_manager =
            SessionManager::new(Database::new(&db_path).expect("database should initialize"));
        let session_id = session_manager
            .create_session("Matching recovery", None, None)
            .expect("session should be created");

        assert!(matches!(
            claim_matching_continuation_recovery_state(
                &session_manager,
                &session_id,
                "ask-1",
                "answer"
            ),
            Err(AppError::Conflict(_))
        ));

        let scope = HashSet::from(["AskUserQuestion".to_string()]);
        let recovery = awaiting_recovery(PendingInteractionSnapshot::ask_user_from_call(
            "ask-1",
            &json!({}),
        ))
        .with_permission_mode(PermissionMode::Supervised)
        .with_execution_tool_allowlist(Some(&scope));
        session_manager
            .update_recovery_state(&session_id, &recovery)
            .expect("recovery should persist");

        let (loaded, pending) = claim_matching_continuation_recovery_state(
            &session_manager,
            &session_id,
            "ask-1",
            "answer",
        )
        .unwrap_or_else(|_| panic!("matching continuation should be claimed"));
        assert_eq!(loaded.permission_mode, Some(PermissionMode::Supervised));
        assert_eq!(
            resumed_execution_tool_allowlist(loaded.execution_tool_allowlist),
            Some(scope)
        );
        assert!(!is_plan_confirmation(&pending));
        let durable_claim = session_manager
            .load_recovery_state(&session_id)
            .expect("recovery should load")
            .expect("accepted response should remain durable");
        assert_eq!(
            durable_claim
                .continuation_claim
                .as_ref()
                .map(|claim| claim.accepted_response.as_str()),
            Some("answer")
        );
        session_manager
            .yield_awaiting_interaction_claim(&session_id, "ask-1", "answer")
            .expect("test claim lease should yield");

        session_manager
            .update_recovery_state(&session_id, &recovery)
            .expect("recovery should be restored for mismatch test");
        assert!(matches!(
            claim_matching_continuation_recovery_state(
                &session_manager,
                &session_id,
                "different-call",
                "answer"
            ),
            Err(AppError::Conflict(_))
        ));

        let empty_scope = HashSet::new();
        let plan_recovery = awaiting_recovery(PendingInteractionSnapshot::plan_confirm(
            "opaque-id-without-plan-prefix",
            "Plan",
            0,
            Vec::new(),
        ))
        .with_permission_mode(PermissionMode::Supervised)
        .with_execution_tool_allowlist(Some(&empty_scope));
        session_manager
            .update_recovery_state(&session_id, &plan_recovery)
            .expect("plan recovery should persist");
        let (loaded, pending) = claim_matching_continuation_recovery_state(
            &session_manager,
            &session_id,
            "opaque-id-without-plan-prefix",
            "execute",
        )
        .unwrap_or_else(|_| panic!("matching plan continuation should be claimed"));
        assert_eq!(
            resumed_execution_tool_allowlist(loaded.execution_tool_allowlist),
            Some(empty_scope)
        );
        assert!(is_plan_confirmation(&pending));

        let prefixed_ask =
            PendingInteractionSnapshot::ask_user_from_call("plan-confirm-spoofed", &json!({}));
        assert!(!is_plan_confirmation(&prefixed_ask));
    }
}
