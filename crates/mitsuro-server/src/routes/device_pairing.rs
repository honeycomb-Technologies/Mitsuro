use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::{header, HeaderMap, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{
    auth::AuthenticatedUser,
    device_access::{now, AccessRejection, DeviceIdentity},
    AppState,
};

pub(crate) fn owner_router() -> Router<AppState> {
    Router::new()
        .route("/", get(list))
        .route("/invitations", post(create))
        .route("/invitations/:id/approve", post(approve))
        .route("/invitations/:id", delete(cancel))
        .route("/devices/:id", delete(revoke))
        .route_layer(middleware::from_fn(require_owner))
        .layer(DefaultBodyLimit::max(4096))
}

pub(crate) fn public_router() -> Router<AppState> {
    Router::new()
        .route("/api/pairing/claim", post(claim))
        .route("/api/pairing/status", get(status))
        .layer(DefaultBodyLimit::max(4096))
}

async fn require_owner(request: axum::extract::Request, next: middleware::Next) -> Response {
    if request.extensions().get::<DeviceIdentity>().is_some()
        || !request
            .extensions()
            .get::<AuthenticatedUser>()
            .is_some_and(|u| u.user_id.is_none())
    {
        return (
            StatusCode::FORBIDDEN,
            "Device management requires the server owner",
        )
            .into_response();
    }
    next.run(request).await
}

fn private_json(value: impl Serialize) -> Response {
    (
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        Json(value),
    )
        .into_response()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRequest {
    origin: String,
}

/// Origins in invitations are explicit destinations, never redirect targets.
/// HTTPS is required except loopback for local development and tests.
fn pairing_origin(input: &str) -> Result<String, AccessRejection> {
    let url = reqwest::Url::parse(input)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid server origin"))?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if input.len() > 2048
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Use the server's HTTPS origin, without a path or credentials",
        ));
    }
    Ok(url.origin().ascii_serialization())
}

async fn enabled(state: &AppState) -> Result<(), AccessRejection> {
    if state.remote_access.read().await.enabled {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "Enable remote access before pairing a device",
        ))
    }
}

async fn create(
    State(state): State<AppState>,
    Json(request): Json<CreateRequest>,
) -> Result<Response, AccessRejection> {
    enabled(&state).await?;
    let origin = pairing_origin(&request.origin)?;
    let invitation = state
        .device_access
        .with_store(&state.db_path, |s| s.create_invitation(&origin, now()))?;
    Ok(private_json(invitation))
}

async fn list(State(state): State<AppState>) -> Result<Response, AccessRejection> {
    let (devices, pending) = state.device_access.with_store(&state.db_path, |s| {
        Ok((s.devices(now())?, s.pending(now())?))
    })?;
    Ok(private_json(
        serde_json::json!({"devices":devices,"pending":pending,"pairing_version":1}),
    ))
}

async fn approve(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, AccessRejection> {
    enabled(&state).await?;
    let device = state
        .device_access
        .with_store(&state.db_path, |s| s.approve(&id, now()))?;
    tracing::info!(device_id = %device.id, "Device paired");
    Ok(private_json(device))
}

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, AccessRejection> {
    state
        .device_access
        .with_store(&state.db_path, |s| s.cancel(&id))?;
    Ok(private_json(serde_json::json!({"ok":true})))
}

async fn revoke(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, AccessRejection> {
    state
        .device_access
        .with_store(&state.db_path, |s| s.revoke(&id, now()))?;
    tracing::info!(device_id = %id, "Device revoked");
    Ok(private_json(serde_json::json!({"ok":true})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimRequest {
    secret: String,
    origin: String,
    name: String,
}

async fn claim(
    State(state): State<AppState>,
    Json(request): Json<ClaimRequest>,
) -> Result<Response, AccessRejection> {
    enabled(&state).await?;
    state.device_access.admit(true)?;
    let origin = pairing_origin(&request.origin)?;
    let claim = state.device_access.with_store(&state.db_path, |s| {
        s.claim(&request.secret, &origin, &request.name, now())
    })?;
    Ok(private_json(claim))
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, AccessRejection> {
    enabled(&state).await?;
    state.device_access.admit(false)?;
    let credential = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .ok_or((StatusCode::UNAUTHORIZED, "Pairing credential required"))?;
    let status = state
        .device_access
        .with_store(&state.db_path, |s| s.status(credential, now()))?;
    Ok(private_json(status))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamTicketRequest {
    path: String,
}

pub(crate) async fn stream_ticket(
    State(state): State<AppState>,
    identity: Option<axum::Extension<DeviceIdentity>>,
    Json(request): Json<StreamTicketRequest>,
) -> Result<Response, AccessRejection> {
    let axum::Extension(identity) = identity.ok_or((
        StatusCode::FORBIDDEN,
        "A paired device credential is required",
    ))?;
    let ticket = state.device_access.with_store(&state.db_path, |s| {
        s.create_stream_ticket(&identity.0, &request.path, now())
    })?;
    Ok(private_json(
        serde_json::json!({"ticket": ticket, "expires_in": 30}),
    ))
}

/// Lightweight authenticated liveness check; public /health is not proof that
/// a stored device grant is still valid.
pub(crate) async fn device_session(
    identity: Option<axum::Extension<DeviceIdentity>>,
) -> Result<Response, AccessRejection> {
    let axum::Extension(identity) = identity.ok_or((
        StatusCode::FORBIDDEN,
        "A paired device credential is required",
    ))?;
    Ok(private_json(serde_json::json!({"device_id": identity.0})))
}
