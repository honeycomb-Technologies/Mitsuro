//! Crash-reconcilable, exact-owner Canvas resource creation.
//!
//! Resource creation crosses SQLite and external runtime boundaries. Each
//! request first reserves a durable intent, then a detached server task
//! provisions the exact stable resource id and binds it to the originating
//! Canvas. Dropping the HTTP future cannot cancel that task.

use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::{
    extract::{Path as AxumPath, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use mitsuro_core::ai::types::Content;
use mitsuro_core::storage::{
    CanvasNode, CanvasNodeBinding, CanvasResourceCleanupClaim, CanvasResourceCreateClaim,
    CanvasResourceCreateIntent, CanvasResourceCreateStatus, CanvasResourceKind, Database,
    ReserveCanvasResourceCreateInput, SessionManager, SessionType, SpatialCanvas,
    SpatialCanvasStore, SpatialCanvasSurface, WorkspaceMode,
};
use mitsuro_core::tools::registry::PermissionMode;

use super::session_access::current_user_id;
use crate::auth::{AuthenticatedUser, CurrentUser};
use crate::error::AppError;
use crate::types::{
    AcknowledgeCanvasAgentPromptResponse, CanvasResourceIntentResponse, CanvasResourceSpec,
    CleanupCanvasResourceResponse, CreateCanvasResourceRequest, CreateCanvasResourceResponse,
    DetachCanvasResourceRequest, DetachCanvasResourceResponse, ListCanvasResourceIntentsResponse,
};
use crate::AppState;

const RESOURCE_WAIT_TIMEOUT: Duration = Duration::from_secs(20);
const RESOURCE_WAIT_POLL: Duration = Duration::from_millis(40);

static RESOURCE_EXECUTOR_ID: OnceLock<String> = OnceLock::new();

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/:id/resources",
            get(list_resource_intents).post(create_resource),
        )
        .route(
            "/:id/resources/:create_op_id",
            get(get_resource_intent).delete(cleanup_resource),
        )
        .route(
            "/:id/resources/:create_op_id/detach",
            axum::routing::post(detach_resource),
        )
        .route(
            "/:id/resources/:create_op_id/initial-prompt",
            axum::routing::delete(acknowledge_initial_prompt),
        )
}

pub(crate) async fn create_resource(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Json(request): Json<CreateCanvasResourceRequest>,
) -> Result<Json<CreateCanvasResourceResponse>, AppError> {
    let request = normalize_request(request)?;
    let owner_user_id = user.user_id.clone();
    let canvas = load_code_canvas(&state, &user, &canvas_id)?;
    let canonical_request = canonical_request_json(&request)?;
    let resource_kind = resource_kind(&request.resource);
    let bind_op_id = bind_operation_id(&request.create_op_id);
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let reservation = store
        .reserve_resource_create_for_exact_owner(
            &canvas_id,
            owner_user_id.as_deref(),
            ReserveCanvasResourceCreateInput {
                create_op_id: request.create_op_id.clone(),
                bind_op_id,
                client_id: request.client_id.clone(),
                node_id: request.node.id.clone(),
                resource_kind,
                request_json: canonical_request,
            },
        )
        .map_err(map_intent_error)?;

    // A fully completed exact replay performs no filesystem or runtime work.
    // Keep it available while the project mount is temporarily offline, but
    // still reject a response whose formerly bound node was removed/rebound.
    if reservation.intent.status == CanvasResourceCreateStatus::Bound {
        ensure_completed_evidence(&reservation.intent)?;
        return completed_response(
            &state,
            owner_user_id.as_deref(),
            &canvas_id,
            &request,
            false,
        )
        .map(Json);
    }
    if matches!(
        reservation.intent.status,
        CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned
    ) {
        return Err(AppError::Conflict(
            "Canvas resource create operation was permanently cleaned or abandoned; create a new operation to provision another resource"
                .to_string(),
        ));
    }
    if reservation.intent.status == CanvasResourceCreateStatus::Cleaning {
        return Err(AppError::Conflict(
            "Canvas resource cleanup is still in progress; retry the cleanup operation".to_string(),
        ));
    }

    ensure_canvas_project_available(&canvas)?;

    let response = match store
        .claim_resource_create_for_exact_owner(
            &canvas_id,
            owner_user_id.as_deref(),
            &request.create_op_id,
            resource_executor_id(),
            request.retry_ambiguous,
        )
        .map_err(map_intent_error)?
    {
        CanvasResourceCreateClaim::Acquired(intent) => {
            let claim_token = active_claim_token(&intent)?;
            let (result_tx, result_rx) = oneshot::channel();
            spawn_resource_job(
                state,
                user,
                canvas_id,
                request,
                claim_token,
                reservation.created,
                Some(result_tx),
            );
            result_rx.await.map_err(|_| {
                AppError::Internal(
                    "Canvas resource worker stopped before publishing its result; the durable intent will become retryable after the bounded stale window"
                        .to_string(),
                )
            })??
        }
        CanvasResourceCreateClaim::Busy(_) => {
            await_resource_result(
                &state,
                owner_user_id.as_deref(),
                &canvas_id,
                &request,
                false,
            )
            .await?
        }
        CanvasResourceCreateClaim::Completed(_) => completed_response(
            &state,
            owner_user_id.as_deref(),
            &canvas_id,
            &request,
            false,
        )?,
        CanvasResourceCreateClaim::Ambiguous(intent) => return Err(ambiguous_error(&intent)),
    };
    Ok(Json(response))
}

async fn list_resource_intents(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
) -> Result<Json<ListCanvasResourceIntentsResponse>, AppError> {
    let owner_user_id = user.user_id.clone();
    let canvas = load_code_canvas(&state, &user, &canvas_id)?;
    let project_error = ensure_canvas_project_available(&canvas)
        .err()
        .map(|error| app_error_message(&error));
    let intents = SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .list_unfinished_resource_creates_for_exact_owner(&canvas_id, owner_user_id.as_deref())
        .map_err(map_intent_error)?
        .into_iter()
        .map(|intent| intent_response(intent, project_error.as_deref(), false))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListCanvasResourceIntentsResponse { intents }))
}

async fn cleanup_resource(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath((canvas_id, create_op_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    maybe_request: Option<Json<CreateCanvasResourceRequest>>,
) -> Result<Json<CleanupCanvasResourceResponse>, AppError> {
    let owner_user_id = user.user_id.clone();
    let canvas = load_code_canvas(&state, &user, &canvas_id)?;
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let current = match store
        .get_resource_create_for_exact_owner(&canvas_id, owner_user_id.as_deref(), &create_op_id)
        .map_err(map_intent_error)?
    {
        Some(current) => current,
        None => {
            let Json(request) = maybe_request.ok_or_else(|| {
                AppError::NotFound("Canvas resource intent not found".to_string())
            })?;
            let request = normalize_request(request)?;
            if request.create_op_id != create_op_id {
                return Err(AppError::Conflict(
                    "Canvas resource cleanup path and request operation do not match".to_string(),
                ));
            }
            let reservation = store
                .reserve_resource_cleanup_tombstone_for_exact_owner(
                    &canvas_id,
                    owner_user_id.as_deref(),
                    ReserveCanvasResourceCreateInput {
                        create_op_id: request.create_op_id.clone(),
                        bind_op_id: bind_operation_id(&request.create_op_id),
                        client_id: request.client_id.clone(),
                        node_id: request.node.id.clone(),
                        resource_kind: resource_kind(&request.resource),
                        request_json: canonical_request_json(&request)?,
                    },
                )
                .map_err(map_intent_error)?;
            return Ok(Json(cleanup_response(
                &state,
                owner_user_id.as_deref(),
                &canvas_id,
                &reservation.intent,
            )?));
        }
    };
    if current.status == CanvasResourceCreateStatus::Bound {
        let request = stored_request(&current)?;
        authorize_bound_resource_cleanup(&state, &user, &canvas, &request, &headers, false).await?;
    }
    let response = match store
        .claim_resource_cleanup_for_exact_owner(
            &canvas_id,
            owner_user_id.as_deref(),
            &create_op_id,
            resource_executor_id(),
        )
        .map_err(map_intent_error)?
    {
        CanvasResourceCleanupClaim::BoundRequiresAuthorization(intent) => {
            let request = stored_request(&intent)?;
            authorize_bound_resource_cleanup(&state, &user, &canvas, &request, &headers, false)
                .await?;
            match store
                .claim_resource_cleanup_for_exact_owner_with_bound_authorization(
                    &canvas_id,
                    owner_user_id.as_deref(),
                    &create_op_id,
                    resource_executor_id(),
                    true,
                )
                .map_err(map_intent_error)?
            {
                CanvasResourceCleanupClaim::Acquired {
                    intent,
                    prior_status,
                    absence_is_authoritative,
                } => {
                    let claim_token = active_claim_token(&intent)?;
                    let (result_tx, result_rx) = oneshot::channel();
                    spawn_resource_cleanup_job(
                        state,
                        user,
                        canvas,
                        intent,
                        claim_token,
                        absence_is_authoritative,
                        (prior_status == CanvasResourceCreateStatus::Bound).then_some(headers),
                        Some(result_tx),
                    );
                    result_rx.await.map_err(|_| {
                        AppError::Internal(
                            "Canvas resource cleanup worker stopped before publishing its result; the durable cleanup claim remains visible and retryable"
                                .to_string(),
                        )
                    })??
                }
                CanvasResourceCleanupClaim::Busy(_) => {
                    await_resource_cleanup_result(
                        &state,
                        owner_user_id.as_deref(),
                        &canvas_id,
                        &create_op_id,
                    )
                    .await?
                }
                CanvasResourceCleanupClaim::Completed(intent) => {
                    cleanup_response(&state, owner_user_id.as_deref(), &canvas_id, &intent)?
                }
                CanvasResourceCleanupClaim::BoundRequiresAuthorization(_) => {
                    return Err(AppError::Conflict(
                        "Canvas resource cleanup still requires a live control lease".to_string(),
                    ))
                }
            }
        }
        CanvasResourceCleanupClaim::Acquired {
            intent,
            prior_status,
            absence_is_authoritative,
        } => {
            let claim_token = active_claim_token(&intent)?;
            let (result_tx, result_rx) = oneshot::channel();
            spawn_resource_cleanup_job(
                state,
                user,
                canvas,
                intent,
                claim_token,
                absence_is_authoritative,
                (prior_status == CanvasResourceCreateStatus::Bound).then_some(headers),
                Some(result_tx),
            );
            result_rx.await.map_err(|_| {
                AppError::Internal(
                    "Canvas resource cleanup worker stopped before publishing its result; the durable cleanup claim remains visible and retryable"
                        .to_string(),
                )
            })??
        }
        CanvasResourceCleanupClaim::Busy(_) => {
            await_resource_cleanup_result(
                &state,
                owner_user_id.as_deref(),
                &canvas_id,
                &create_op_id,
            )
            .await?
        }
        CanvasResourceCleanupClaim::Completed(intent) => {
            cleanup_response(&state, owner_user_id.as_deref(), &canvas_id, &intent)?
        }
    };
    Ok(Json(response))
}

async fn acknowledge_initial_prompt(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath((canvas_id, create_op_id)): AxumPath<(String, String)>,
) -> Result<Json<AcknowledgeCanvasAgentPromptResponse>, AppError> {
    let _ = (state, user, canvas_id);
    Ok(Json(AcknowledgeCanvasAgentPromptResponse {
        create_op_id,
        redacted: false,
    }))
}

async fn get_resource_intent(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath((canvas_id, create_op_id)): AxumPath<(String, String)>,
) -> Result<Json<CanvasResourceIntentResponse>, AppError> {
    let owner_user_id = user.user_id.clone();
    let _canvas = load_code_canvas(&state, &user, &canvas_id)?;
    let intent = SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .get_resource_create_for_exact_owner(&canvas_id, owner_user_id.as_deref(), &create_op_id)
        .map_err(map_intent_error)?
        .ok_or_else(|| AppError::NotFound("Canvas resource intent not found".to_string()))?;
    intent_response(intent, None, false).map(Json)
}

async fn detach_resource(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath((canvas_id, create_op_id)): AxumPath<(String, String)>,
    Json(detach): Json<DetachCanvasResourceRequest>,
) -> Result<Json<DetachCanvasResourceResponse>, AppError> {
    let request = normalize_request(detach.request)?;
    if request.create_op_id != create_op_id {
        return Err(AppError::Conflict(
            "Canvas resource detach path and request operation do not match".to_string(),
        ));
    }
    if request.resource == CanvasResourceSpec::Agent {
        return Err(AppError::Conflict(
            "Canvas Agent resources must be abandoned through authoritative cleanup".to_string(),
        ));
    }
    let owner_user_id = user.user_id.clone();
    let _canvas = load_code_canvas(&state, &user, &canvas_id)?;
    let canonical_request = canonical_request_json(&request)?;
    let (intent, canvas, binding_absent, detach_committed) =
        SpatialCanvasStore::new(Database::new(&state.db_path)?)
            .detach_bound_resource_for_exact_owner(
                &canvas_id,
                owner_user_id.as_deref(),
                &create_op_id,
                detach.expected_detach_generation,
                &canonical_request,
            )
            .map_err(map_intent_error)?;
    Ok(Json(DetachCanvasResourceResponse {
        create_op_id: intent.create_op_id,
        node_id: intent.node_id,
        status: intent.status,
        canvas,
        binding_absent,
        detach_committed,
        detach_generation: intent.detach_generation,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundCleanupProof {
    LiveLease,
    AuthoritativelyEnded,
}

async fn authorize_bound_resource_cleanup(
    state: &AppState,
    user: &AuthenticatedUser,
    canvas: &SpatialCanvas,
    request: &CreateCanvasResourceRequest,
    headers: &HeaderMap,
    same_executor_absence_is_authoritative: bool,
) -> Result<BoundCleanupProof, AppError> {
    let lease_token = cleanup_control_lease_token(headers)?;
    match &request.resource {
        CanvasResourceSpec::Agent => {
            let manager = SessionManager::new(Database::new(&state.db_path)?);
            let Some(session) = manager.get_session(&request.node.id)? else {
                return Ok(BoundCleanupProof::AuthoritativelyEnded);
            };
            let project = session
                .project_dir
                .as_deref()
                .or(session.working_dir.as_deref());
            if session.user_id.as_deref() != user.user_id.as_deref() {
                return Err(AppError::NotFound("Agent session not found".to_string()));
            }
            if session.session_type != SessionType::Code
                || session.workspace_mode != WorkspaceMode::Selected
                || project.map(Path::new) != Some(Path::new(&canvas.project_dir))
            {
                return Err(AppError::Conflict(
                    "Agent cleanup request does not match the exact Canvas resource spec"
                        .to_string(),
                ));
            }
            crate::control_leases::authorize_code_agent_mutation(
                state,
                &request.node.id,
                SessionType::Code,
                headers,
            )
            .await?;
            Ok(BoundCleanupProof::LiveLease)
        }
        CanvasResourceSpec::Terminal { size } => {
            super::terminals::classify_terminal_cleanup_for_canvas(
                user,
                &request.node.id,
                Path::new(&canvas.project_dir),
                *size,
                lease_token,
                same_executor_absence_is_authoritative,
            )
            .await
        }
        CanvasResourceSpec::Browser { url } => {
            super::browser::classify_browser_cleanup_for_canvas(
                user,
                &request.node.id,
                Path::new(&canvas.project_dir),
                &request.node.title,
                url.as_deref(),
                lease_token,
                same_executor_absence_is_authoritative,
            )
            .await
        }
    }
}

fn cleanup_control_lease_token(headers: &HeaderMap) -> Result<Option<&str>, AppError> {
    let Some(value) = headers.get(crate::control_leases::CONTROL_LEASE_HEADER) else {
        return Ok(None);
    };
    let token = value.to_str().map_err(|_| {
        AppError::BadRequest("Canvas resource control lease header is invalid".to_string())
    })?;
    if token.trim().is_empty() {
        return Err(AppError::BadRequest(
            "Canvas resource control lease header cannot be empty".to_string(),
        ));
    }
    Ok(Some(token))
}

fn spawn_resource_job(
    state: AppState,
    user: AuthenticatedUser,
    canvas_id: String,
    request: CreateCanvasResourceRequest,
    claim_token: String,
    created: bool,
    result_tx: Option<oneshot::Sender<Result<CreateCanvasResourceResponse, AppError>>>,
) {
    tokio::spawn(async move {
        let result =
            drive_resource_create(&state, &user, &canvas_id, &request, &claim_token, created).await;
        if let Some(result_tx) = result_tx {
            let _ = result_tx.send(result);
        } else if let Err(error) = result {
            tracing::warn!(
                canvas_id,
                create_op_id = request.create_op_id,
                ?error,
                "Detached Canvas resource reconciliation did not complete"
            );
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn spawn_resource_cleanup_job(
    state: AppState,
    user: AuthenticatedUser,
    canvas: SpatialCanvas,
    intent: CanvasResourceCreateIntent,
    claim_token: String,
    absence_is_authoritative: bool,
    bound_reauthorization_headers: Option<HeaderMap>,
    result_tx: Option<oneshot::Sender<Result<CleanupCanvasResourceResponse, AppError>>>,
) {
    tokio::spawn(async move {
        let result = drive_resource_cleanup(
            &state,
            &user,
            &canvas,
            &intent,
            &claim_token,
            absence_is_authoritative,
            bound_reauthorization_headers.as_ref(),
        )
        .await;
        if let Some(result_tx) = result_tx {
            let _ = result_tx.send(result);
        } else if let Err(error) = result {
            tracing::warn!(
                canvas_id = canvas.id,
                create_op_id = intent.create_op_id,
                ?error,
                "Detached Canvas resource cleanup did not complete"
            );
        }
    });
}

async fn drive_resource_cleanup(
    state: &AppState,
    user: &AuthenticatedUser,
    canvas: &SpatialCanvas,
    intent: &CanvasResourceCreateIntent,
    claim_token: &str,
    absence_is_authoritative: bool,
    bound_reauthorization_headers: Option<&HeaderMap>,
) -> Result<CleanupCanvasResourceResponse, AppError> {
    let request = stored_request(intent)?;
    if request.node.id != intent.node_id || resource_kind(&request.resource) != intent.resource_kind
    {
        return Err(AppError::Conflict(
            "Canvas resource cleanup request no longer matches its durable intent".to_string(),
        ));
    }
    if let Some(headers) = bound_reauthorization_headers {
        if let Err(error) =
            authorize_bound_resource_cleanup(state, user, canvas, &request, headers, true).await
        {
            let message = app_error_message(&error);
            SpatialCanvasStore::new(Database::new(&state.db_path)?)
                .release_bound_resource_cleanup_claim_for_exact_owner(
                    &canvas.id,
                    user.user_id.as_deref(),
                    &intent.create_op_id,
                    resource_executor_id(),
                    claim_token,
                    &message,
                )
                .map_err(map_intent_error)?;
            return Err(error);
        }
    }
    let cleanup_absence_is_authoritative =
        absence_is_authoritative || bound_reauthorization_headers.is_some();
    let project_dir = Path::new(&canvas.project_dir);
    let cleanup = match &request.resource {
        CanvasResourceSpec::Agent => Ok(()),
        CanvasResourceSpec::Terminal { size } => {
            super::terminals::cleanup_terminal_for_canvas(
                user,
                &request.node.id,
                project_dir,
                *size,
                cleanup_absence_is_authoritative,
            )
            .await
        }
        CanvasResourceSpec::Browser { url } => {
            super::browser::cleanup_browser_for_canvas(
                user,
                &request.node.id,
                project_dir,
                &request.node.title,
                url.as_deref(),
                cleanup_absence_is_authoritative,
            )
            .await
        }
    };
    if let Err(error) = cleanup {
        let message = app_error_message(&error);
        let _ = SpatialCanvasStore::new(Database::new(&state.db_path)?)
            .record_resource_cleanup_failure_for_exact_owner(
                &canvas.id,
                user.user_id.as_deref(),
                &intent.create_op_id,
                resource_executor_id(),
                claim_token,
                &message,
            );
        return Err(error);
    }
    #[cfg(test)]
    wait_at_resource_job_test_gate(&intent.create_op_id).await;

    let final_status = if intent.resource_kind == CanvasResourceKind::Agent {
        CanvasResourceCreateStatus::Abandoned
    } else {
        CanvasResourceCreateStatus::Cleaned
    };
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    if intent.result_json.is_some() {
        let (intent, canvas) = store
            .finish_bound_resource_cleanup_and_unbind_for_exact_owner(
                &canvas.id,
                user.user_id.as_deref(),
                &intent.create_op_id,
                resource_executor_id(),
                claim_token,
                final_status,
            )
            .map_err(map_intent_error)?;
        cleanup_response_with_canvas(&intent, canvas)
    } else {
        let intent = store
            .finish_resource_cleanup_for_exact_owner(
                &canvas.id,
                user.user_id.as_deref(),
                &intent.create_op_id,
                resource_executor_id(),
                claim_token,
                final_status,
            )
            .map_err(map_intent_error)?;
        cleanup_response(state, user.user_id.as_deref(), &canvas.id, &intent)
    }
}

async fn drive_resource_create(
    state: &AppState,
    user: &AuthenticatedUser,
    canvas_id: &str,
    request: &CreateCanvasResourceRequest,
    claim_token: &str,
    created: bool,
) -> Result<CreateCanvasResourceResponse, AppError> {
    let owner_user_id = user.user_id.as_deref();
    let canvas = SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .get_for_exact_owner(canvas_id, owner_user_id)
        .map_err(map_intent_error)?
        .ok_or_else(|| AppError::NotFound("Canvas not found".to_string()))?;
    let result = provision_resource(state, user, &canvas, request).await;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            let message = app_error_message(&error);
            let _ = SpatialCanvasStore::new(Database::new(&state.db_path)?)
                .mark_resource_create_failed_for_exact_owner(
                    canvas_id,
                    owner_user_id,
                    &request.create_op_id,
                    resource_executor_id(),
                    claim_token,
                    &message,
                );
            return Err(error);
        }
    };
    #[cfg(test)]
    wait_at_resource_job_test_gate(&request.create_op_id).await;
    let result_json = serde_json::to_string(&result)
        .map_err(|error| AppError::Internal(format!("Could not store resource result: {error}")))?;
    SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .mark_resource_create_provisioned_for_exact_owner(
            canvas_id,
            owner_user_id,
            &request.create_op_id,
            resource_executor_id(),
            claim_token,
            &result_json,
        )
        .map_err(map_intent_error)?;

    let node = bound_node(request);
    let canvas = match SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .bind_resource_create_for_exact_owner(
            canvas_id,
            owner_user_id,
            &request.create_op_id,
            resource_executor_id(),
            claim_token,
        ) {
        Ok(canvas) => canvas,
        Err(error) => {
            let error = map_intent_error(error);
            let message = app_error_message(&error);
            let _ = SpatialCanvasStore::new(Database::new(&state.db_path)?)
                .record_resource_bind_failure_for_exact_owner(
                    canvas_id,
                    owner_user_id,
                    &request.create_op_id,
                    resource_executor_id(),
                    claim_token,
                    &message,
                );
            return Err(error);
        }
    };
    Ok(CreateCanvasResourceResponse {
        canvas,
        node,
        created,
    })
}

async fn provision_resource(
    state: &AppState,
    user: &AuthenticatedUser,
    canvas: &SpatialCanvas,
    request: &CreateCanvasResourceRequest,
) -> Result<serde_json::Value, AppError> {
    let project_dir = Path::new(&canvas.project_dir);
    match &request.resource {
        CanvasResourceSpec::Agent => ensure_agent_for_canvas(
            state,
            user,
            &request.node.id,
            project_dir,
            &request.node.title,
        ),
        CanvasResourceSpec::Terminal { size } => {
            let descriptor = super::terminals::ensure_terminal_for_canvas(
                user,
                &request.node.id,
                project_dir,
                *size,
            )
            .await?;
            serde_json::to_value(descriptor).map_err(AppError::from)
        }
        CanvasResourceSpec::Browser { url } => {
            super::browser::ensure_browser_for_canvas(
                user,
                &request.node.id,
                project_dir,
                &request.node.title,
                url.as_deref(),
            )
            .await
        }
    }
}

fn ensure_agent_for_canvas(
    state: &AppState,
    user: &AuthenticatedUser,
    session_id: &str,
    canonical_project_dir: &Path,
    title: &str,
) -> Result<serde_json::Value, AppError> {
    let manager = SessionManager::new(Database::new(&state.db_path)?);
    if let Some(existing) = manager.get_session(session_id)? {
        let existing_project = existing
            .project_dir
            .as_deref()
            .or(existing.working_dir.as_deref());
        if existing.user_id.as_deref() != user.user_id.as_deref() {
            return Err(AppError::NotFound("Agent session not found".to_string()));
        }
        if existing.session_type != SessionType::Code
            || existing.workspace_mode != WorkspaceMode::Selected
            || existing_project.map(Path::new) != Some(canonical_project_dir)
            || existing.title != title
        {
            return Err(AppError::Conflict(
                "Agent create operation was reused with different input".to_string(),
            ));
        }
        return serde_json::to_value(existing).map_err(AppError::from);
    }
    manager.create_session_for_user_with_id_config_and_permission(
        session_id,
        title,
        None,
        canonical_project_dir.to_str(),
        canonical_project_dir.to_str(),
        WorkspaceMode::Selected,
        user.user_id.as_deref(),
        None,
        SessionType::Code,
        PermissionMode::default(),
    )?;
    let session = manager
        .get_session(session_id)?
        .ok_or_else(|| AppError::Internal("Created Agent could not be reloaded".to_string()))?;
    serde_json::to_value(session).map_err(AppError::from)
}

async fn await_resource_result(
    state: &AppState,
    owner_user_id: Option<&str>,
    canvas_id: &str,
    request: &CreateCanvasResourceRequest,
    created: bool,
) -> Result<CreateCanvasResourceResponse, AppError> {
    let started = Instant::now();
    loop {
        let intent = SpatialCanvasStore::new(Database::new(&state.db_path)?)
            .get_resource_create_for_exact_owner(canvas_id, owner_user_id, &request.create_op_id)
            .map_err(map_intent_error)?
            .ok_or_else(|| AppError::NotFound("Canvas resource intent not found".to_string()))?;
        match intent.status {
            CanvasResourceCreateStatus::Bound => {
                ensure_completed_evidence(&intent)?;
                return completed_response(state, owner_user_id, canvas_id, request, created);
            }
            CanvasResourceCreateStatus::Failed => {
                return Err(AppError::ServiceUnavailable(
                    intent.last_error.unwrap_or_else(|| {
                        "Canvas resource provisioning failed; retry the same operation".to_string()
                    }),
                ))
            }
            CanvasResourceCreateStatus::Ambiguous => return Err(ambiguous_error(&intent)),
            CanvasResourceCreateStatus::Reserved
            | CanvasResourceCreateStatus::Provisioning
            | CanvasResourceCreateStatus::Provisioned => {}
            CanvasResourceCreateStatus::Cleaning => {
                return Err(AppError::Conflict(
                    "Canvas resource cleanup is in progress".to_string(),
                ))
            }
            CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned => {
                return Err(AppError::Conflict(
                    "Canvas resource create operation was permanently cleaned or abandoned"
                        .to_string(),
                ))
            }
        }
        if started.elapsed() >= RESOURCE_WAIT_TIMEOUT {
            return Err(AppError::ServiceUnavailable(
                "Canvas resource creation is still running on the server; reopening this Canvas will reconcile its durable intent"
                    .to_string(),
            ));
        }
        tokio::time::sleep(RESOURCE_WAIT_POLL).await;
    }
}

async fn await_resource_cleanup_result(
    state: &AppState,
    owner_user_id: Option<&str>,
    canvas_id: &str,
    create_op_id: &str,
) -> Result<CleanupCanvasResourceResponse, AppError> {
    let started = Instant::now();
    loop {
        let intent = SpatialCanvasStore::new(Database::new(&state.db_path)?)
            .get_resource_create_for_exact_owner(canvas_id, owner_user_id, create_op_id)
            .map_err(map_intent_error)?
            .ok_or_else(|| AppError::NotFound("Canvas resource intent not found".to_string()))?;
        match intent.status {
            CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned => {
                return cleanup_response(state, owner_user_id, canvas_id, &intent)
            }
            CanvasResourceCreateStatus::Ambiguous => return Err(ambiguous_error(&intent)),
            CanvasResourceCreateStatus::Bound => {
                return Err(AppError::Conflict(
                    "Canvas resource was bound before cleanup could claim it".to_string(),
                ))
            }
            CanvasResourceCreateStatus::Cleaning => {}
            CanvasResourceCreateStatus::Reserved
            | CanvasResourceCreateStatus::Provisioning
            | CanvasResourceCreateStatus::Provisioned
            | CanvasResourceCreateStatus::Failed => {
                return Err(AppError::Conflict(
                    "Canvas resource cleanup claim was superseded".to_string(),
                ))
            }
        }
        if started.elapsed() >= RESOURCE_WAIT_TIMEOUT {
            return Err(AppError::ServiceUnavailable(
                "Canvas resource cleanup is still running on the server; the durable cleanup intent remains visible"
                    .to_string(),
            ));
        }
        tokio::time::sleep(RESOURCE_WAIT_POLL).await;
    }
}

fn cleanup_response(
    state: &AppState,
    owner_user_id: Option<&str>,
    canvas_id: &str,
    intent: &CanvasResourceCreateIntent,
) -> Result<CleanupCanvasResourceResponse, AppError> {
    let canvas = SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .get_for_exact_owner(canvas_id, owner_user_id)
        .map_err(map_intent_error)?
        .ok_or_else(|| AppError::NotFound("Canvas not found".to_string()))?;
    cleanup_response_with_canvas(intent, canvas)
}

fn cleanup_response_with_canvas(
    intent: &CanvasResourceCreateIntent,
    canvas: SpatialCanvas,
) -> Result<CleanupCanvasResourceResponse, AppError> {
    if !matches!(
        intent.status,
        CanvasResourceCreateStatus::Cleaned | CanvasResourceCreateStatus::Abandoned
    ) {
        return Err(AppError::Conflict(
            "Canvas resource cleanup has not reached a terminal state".to_string(),
        ));
    }
    Ok(CleanupCanvasResourceResponse {
        create_op_id: intent.create_op_id.clone(),
        node_id: intent.node_id.clone(),
        status: intent.status,
        canvas,
    })
}

fn completed_response(
    state: &AppState,
    owner_user_id: Option<&str>,
    canvas_id: &str,
    request: &CreateCanvasResourceRequest,
    created: bool,
) -> Result<CreateCanvasResourceResponse, AppError> {
    let canvas = SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .get_for_exact_owner(canvas_id, owner_user_id)
        .map_err(map_intent_error)?
        .ok_or_else(|| AppError::NotFound("Canvas not found".to_string()))?;
    let node = bound_node(request);
    if !canvas_has_resource_identity(&canvas, &node) {
        return Err(AppError::Conflict(
            "Canvas resource creation completed, but its node was later removed or rebound"
                .to_string(),
        ));
    }
    Ok(CreateCanvasResourceResponse {
        canvas,
        node,
        created,
    })
}

fn normalize_request(
    mut request: CreateCanvasResourceRequest,
) -> Result<CreateCanvasResourceRequest, AppError> {
    request.create_op_id = request.create_op_id.trim().to_string();
    request.client_id = request.client_id.trim().to_string();
    request.node.id = request.node.id.trim().to_string();
    request.node.title = request.node.title.trim().to_string();
    if Uuid::parse_str(&request.create_op_id).is_err()
        || request.create_op_id.len() > 192
        || Uuid::parse_str(&request.node.id).is_err()
    {
        return Err(AppError::BadRequest(
            "Canvas resource create_op_id and node id must be UUIDs".to_string(),
        ));
    }
    if request.client_id.is_empty() || request.client_id.len() > 256 {
        return Err(AppError::BadRequest(
            "Canvas resource client_id must contain 1 to 256 bytes".to_string(),
        ));
    }
    if matches!(request.resource, CanvasResourceSpec::Browser { .. })
        && request.node.title.len() > 120
    {
        return Err(AppError::BadRequest(
            "Browser titles must contain at most 120 bytes".to_string(),
        ));
    }
    if request.node.title.is_empty() || request.node.title.len() > 512 {
        return Err(AppError::BadRequest(
            "Canvas resource title must contain 1 to 512 bytes".to_string(),
        ));
    }
    request.node.rect.validate().map_err(AppError::BadRequest)?;
    match &mut request.resource {
        CanvasResourceSpec::Agent => {}
        CanvasResourceSpec::Terminal { size } => {
            size.cols = size.cols.clamp(1, 500);
            size.rows = size.rows.clamp(1, 500);
        }
        CanvasResourceSpec::Browser { url } => {
            let normalized = url.as_deref().unwrap_or("about:blank").trim().to_string();
            if normalized.len() > 8 * 1024
                || !(normalized == "about:blank"
                    || normalized.starts_with("http://")
                    || normalized.starts_with("https://"))
            {
                return Err(AppError::BadRequest(
                    "Browser URLs must use http or https".to_string(),
                ));
            }
            *url = Some(normalized);
        }
    }
    Ok(request)
}

fn load_code_canvas(
    state: &AppState,
    user: &AuthenticatedUser,
    canvas_id: &str,
) -> Result<SpatialCanvas, AppError> {
    let current_user = CurrentUser(user.clone());
    let canvas = SpatialCanvasStore::new(Database::new(&state.db_path)?)
        .get_for_exact_owner(canvas_id, current_user_id(Some(&current_user)))
        .map_err(map_intent_error)?
        .ok_or_else(|| AppError::NotFound("Canvas not found".to_string()))?;
    if canvas.surface != SpatialCanvasSurface::Code {
        return Err(AppError::Conflict(
            "Live Agent, Terminal, and Browser resources belong to Code canvases".to_string(),
        ));
    }
    Ok(canvas)
}

fn ensure_canvas_project_available(canvas: &SpatialCanvas) -> Result<(), AppError> {
    let canonical = Path::new(&canvas.project_dir)
        .canonicalize()
        .map_err(|_| AppError::NotFound("Canvas project is temporarily unavailable".to_string()))?;
    if !canonical.is_dir() || canonical != Path::new(&canvas.project_dir) {
        return Err(AppError::Conflict(
            "Canvas project is no longer its exact canonical directory".to_string(),
        ));
    }
    Ok(())
}

fn canonical_request_json(request: &CreateCanvasResourceRequest) -> Result<String, AppError> {
    let mut canonical = request.clone();
    canonical.retry_ambiguous = false;
    serde_json::to_string(&canonical).map_err(AppError::from)
}

#[allow(dead_code)]
fn canonical_initial_prompt_json(prompt: &str) -> Result<String, AppError> {
    serde_json::to_string(&vec![Content::text(prompt.to_string())]).map_err(AppError::from)
}

fn stored_request(
    intent: &CanvasResourceCreateIntent,
) -> Result<CreateCanvasResourceRequest, AppError> {
    let mut request: CreateCanvasResourceRequest =
        serde_json::from_str(&intent.request_json).map_err(AppError::from)?;
    request.retry_ambiguous = false;
    Ok(request)
}

fn intent_response(
    intent: CanvasResourceCreateIntent,
    project_error: Option<&str>,
    initial_prompt_completed: bool,
) -> Result<CanvasResourceIntentResponse, AppError> {
    Ok(CanvasResourceIntentResponse {
        request: stored_request(&intent)?,
        status: intent.status,
        last_error: intent
            .last_error
            .or_else(|| project_error.map(str::to_string)),
        retry_requires_confirmation: intent.status == CanvasResourceCreateStatus::Ambiguous,
        initial_prompt_completed,
        detach_generation: intent.detach_generation,
    })
}

fn resource_kind(spec: &CanvasResourceSpec) -> CanvasResourceKind {
    match spec {
        CanvasResourceSpec::Agent => CanvasResourceKind::Agent,
        CanvasResourceSpec::Terminal { .. } => CanvasResourceKind::Terminal,
        CanvasResourceSpec::Browser { .. } => CanvasResourceKind::Browser,
    }
}

fn bound_node(request: &CreateCanvasResourceRequest) -> CanvasNode {
    let binding = match &request.resource {
        CanvasResourceSpec::Agent => CanvasNodeBinding::Agent {
            session_id: request.node.id.clone(),
        },
        CanvasResourceSpec::Terminal { .. } => CanvasNodeBinding::Terminal {
            terminal_id: request.node.id.clone(),
        },
        CanvasResourceSpec::Browser { url } => CanvasNodeBinding::Browser {
            browser_session_id: request.node.id.clone(),
            last_url: url.clone(),
        },
    };
    CanvasNode {
        id: request.node.id.clone(),
        title: request.node.title.clone(),
        rect: request.node.rect,
        z_index: request.node.z_index,
        binding,
        revision: 0,
    }
}

fn canvas_has_resource_identity(canvas: &SpatialCanvas, requested: &CanvasNode) -> bool {
    canvas
        .state
        .nodes
        .iter()
        .find(|node| node.id == requested.id)
        .is_some_and(|node| resource_identity_matches(node, requested))
}

fn resource_identity_matches(existing: &CanvasNode, requested: &CanvasNode) -> bool {
    if existing.id != requested.id {
        return false;
    }
    match (&existing.binding, &requested.binding) {
        (
            CanvasNodeBinding::Agent { session_id },
            CanvasNodeBinding::Agent {
                session_id: requested_id,
            },
        ) => session_id == requested_id,
        (
            CanvasNodeBinding::Terminal { terminal_id },
            CanvasNodeBinding::Terminal {
                terminal_id: requested_id,
            },
        ) => terminal_id == requested_id,
        (
            CanvasNodeBinding::Browser {
                browser_session_id, ..
            },
            CanvasNodeBinding::Browser {
                browser_session_id: requested_id,
                ..
            },
        ) => browser_session_id == requested_id,
        _ => false,
    }
}

fn active_claim_token(intent: &CanvasResourceCreateIntent) -> Result<String, AppError> {
    intent.claim_token.clone().ok_or_else(|| {
        AppError::Internal("Acquired Canvas resource claim has no fencing token".to_string())
    })
}

fn ensure_completed_evidence(intent: &CanvasResourceCreateIntent) -> Result<(), AppError> {
    if intent.result_json.is_some() {
        Ok(())
    } else {
        Err(AppError::Conflict(
            "Canvas resource intent is marked bound without provision result evidence".to_string(),
        ))
    }
}

fn bind_operation_id(create_op_id: &str) -> String {
    format!("resource-bind:{create_op_id}")
}

fn resource_executor_id() -> &'static str {
    RESOURCE_EXECUTOR_ID
        .get_or_init(|| format!("server-{}", Uuid::new_v4()))
        .as_str()
}

fn ambiguous_error(intent: &CanvasResourceCreateIntent) -> AppError {
    AppError::Conflict(intent.last_error.clone().unwrap_or_else(|| {
        "The server stopped while the external resource lifecycle was uncertain; explicit retry or cleanup is required"
            .to_string()
    }))
}

fn app_error_message(error: &AppError) -> String {
    match error {
        AppError::NotFound(message)
        | AppError::BadRequest(message)
        | AppError::Conflict(message)
        | AppError::ServiceUnavailable(message)
        | AppError::BadGateway(message)
        | AppError::Internal(message) => message.clone(),
    }
}

fn map_intent_error(error: anyhow::Error) -> AppError {
    let message = error.to_string();
    if message.contains("not found") {
        AppError::NotFound("Canvas not found".to_string())
    } else if message.contains("reused with different input")
        || message.contains("already exists")
        || message.contains("reserved")
        || message.contains("capacity")
        || message.contains("cleanup")
        || message.contains("detach")
        || message.contains("archived canvas")
        || message.contains("bound canvas resources")
        || message.contains("permanently cleaned")
        || message.contains("Canvas Agent prompt")
    {
        AppError::Conflict(message)
    } else if message.contains("cannot be empty")
        || message.contains("too long")
        || message.contains("must be")
    {
        AppError::BadRequest(message)
    } else {
        AppError::from(error)
    }
}

#[cfg(test)]
struct ResourceJobTestGate {
    provisioned: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
impl ResourceJobTestGate {
    fn new() -> Self {
        Self {
            provisioned: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

#[cfg(test)]
fn resource_job_test_gates() -> &'static tokio::sync::Mutex<
    std::collections::HashMap<String, std::sync::Arc<ResourceJobTestGate>>,
> {
    static GATES: OnceLock<
        tokio::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<ResourceJobTestGate>>>,
    > = OnceLock::new();
    GATES.get_or_init(|| tokio::sync::Mutex::new(std::collections::HashMap::new()))
}

#[cfg(test)]
async fn install_resource_job_test_gate(create_op_id: &str) -> std::sync::Arc<ResourceJobTestGate> {
    let gate = std::sync::Arc::new(ResourceJobTestGate::new());
    resource_job_test_gates()
        .lock()
        .await
        .insert(create_op_id.to_string(), std::sync::Arc::clone(&gate));
    gate
}

#[cfg(test)]
async fn remove_resource_job_test_gate(create_op_id: &str) {
    resource_job_test_gates().lock().await.remove(create_op_id);
}

#[cfg(test)]
async fn wait_at_resource_job_test_gate(create_op_id: &str) {
    let gate = resource_job_test_gates()
        .lock()
        .await
        .get(create_op_id)
        .cloned();
    let Some(gate) = gate else {
        return;
    };
    gate.provisioned.add_permits(1);
    if let Ok(permit) = gate.release.acquire().await {
        permit.forget();
    };
}

#[cfg(test)]
#[path = "canvas_resources_tests.rs"]
mod tests;
