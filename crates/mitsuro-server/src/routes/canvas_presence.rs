//! Bounded, process-local collaboration state for spatial canvases.
//!
//! This module deliberately does not write presence, cursors, previews, or
//! advisory locks to SQLite. HTTP requests cannot reliably prove a client has
//! disconnected, so clients heartbeat and the server marks them stale after
//! 15 seconds, releases their soft locks at that point, and expires presence
//! after 60 seconds. An explicit leave/release request takes effect at once.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Mutex, MutexGuard, OnceLock};

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::HeaderMap,
    routing::put,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use mitsuro_core::storage::{Database, SpatialCanvas, SpatialCanvasStore};

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{
    CanvasCollaborationEvent, CanvasCollaborationEventPage, CanvasCollaborationResponse,
    CanvasCollaborationSnapshot, CanvasCursor, CanvasGeometryPreview,
    CanvasGeometryPreviewEndReason, CanvasGeometryPreviewReleaseQuery,
    CanvasGeometryPreviewRequest, CanvasPresenceClient, CanvasPresenceHeartbeatRequest,
    CanvasPresenceLeaveReason,
};
use crate::AppState;

pub(crate) const PRESENCE_STALE_AFTER_SECS: i64 = 15;
pub(crate) const PRESENCE_EXPIRE_AFTER_SECS: i64 = 60;
const MAX_TRACKED_CANVASES: usize = 256;
const MAX_CLIENTS_PER_CANVAS: usize = 64;
const MAX_PREVIEWS_PER_CANVAS: usize = 256;
const MAX_EVENTS_PER_CANVAS: usize = 1_024;
const MAX_EVENT_PAGE: usize = 128;
const MAX_ID_BYTES: usize = 256;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_PRESENCE_TOKEN_BYTES: usize = 128;
const WORLD_COORDINATE_LIMIT: f64 = 10_000_000.0;
const PRESENCE_TOKEN_HEADER: &str = "x-mitsuro-presence-token";

static COLLABORATION_HUB: OnceLock<Mutex<CanvasCollaborationHub>> = OnceLock::new();

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/:id/presence", put(heartbeat_presence))
        .route(
            "/:id/presence/:client_id",
            axum::routing::delete(leave_presence),
        )
        .route(
            "/:id/geometry-previews/:node_id",
            put(update_geometry_preview).delete(release_geometry_preview),
        )
}

async fn heartbeat_presence(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Json(request): Json<CanvasPresenceHeartbeatRequest>,
) -> Result<Json<CanvasCollaborationResponse>, AppError> {
    let canvas = load_owned_canvas(&state.db_path, &canvas_id, user.user_id.as_deref())?;
    let key = collaboration_key(&state.db_path, &canvas);
    let outcome = lock_hub().heartbeat(key, request, Utc::now())?;
    Ok(Json(CanvasCollaborationResponse {
        snapshot: outcome.snapshot,
        presence_token: Some(outcome.presence_token),
    }))
}

async fn leave_presence(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath((canvas_id, client_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<CanvasCollaborationResponse>, AppError> {
    let canvas = load_owned_canvas(&state.db_path, &canvas_id, user.user_id.as_deref())?;
    validate_identifier("canvas client", &client_id)?;
    let presence_token = require_presence_token(&headers)?;
    let key = collaboration_key(&state.db_path, &canvas);
    let snapshot = lock_hub().leave(key, &client_id, presence_token, Utc::now())?;
    Ok(Json(CanvasCollaborationResponse {
        snapshot,
        presence_token: None,
    }))
}

async fn update_geometry_preview(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath((canvas_id, node_id)): AxumPath<(String, String)>,
    Json(request): Json<CanvasGeometryPreviewRequest>,
) -> Result<Json<CanvasCollaborationResponse>, AppError> {
    let canvas = load_owned_canvas(&state.db_path, &canvas_id, user.user_id.as_deref())?;
    validate_identifier("canvas node", &node_id)?;
    if !canvas.state.nodes.iter().any(|node| node.id == node_id) {
        return Err(AppError::NotFound(format!(
            "Canvas node {node_id} not found"
        )));
    }
    let key = collaboration_key(&state.db_path, &canvas);
    let snapshot = lock_hub().update_preview(key, node_id, request, Utc::now())?;
    Ok(Json(CanvasCollaborationResponse {
        snapshot,
        presence_token: None,
    }))
}

async fn release_geometry_preview(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath((canvas_id, node_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<CanvasGeometryPreviewReleaseQuery>,
) -> Result<Json<CanvasCollaborationResponse>, AppError> {
    let canvas = load_owned_canvas(&state.db_path, &canvas_id, user.user_id.as_deref())?;
    validate_identifier("canvas client", &query.client_id)?;
    validate_identifier("canvas node", &node_id)?;
    let presence_token = require_presence_token(&headers)?;
    let key = collaboration_key(&state.db_path, &canvas);
    let snapshot = lock_hub().release_preview(
        key,
        &node_id,
        &query.client_id,
        presence_token,
        query.committed,
        Utc::now(),
    )?;
    Ok(Json(CanvasCollaborationResponse {
        snapshot,
        presence_token: None,
    }))
}

fn require_presence_token(headers: &HeaderMap) -> Result<&str, AppError> {
    let token = headers
        .get(PRESENCE_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| AppError::BadRequest("Canvas presence token is required".to_string()))?;
    validate_presence_token(token)?;
    Ok(token)
}

fn load_owned_canvas(
    db_path: &Path,
    canvas_id: &str,
    owner_user_id: Option<&str>,
) -> Result<SpatialCanvas, AppError> {
    let store = SpatialCanvasStore::new(Database::new(db_path)?);
    store
        .get_for_exact_owner(canvas_id, owner_user_id)
        .map_err(AppError::from)?
        .ok_or_else(|| AppError::NotFound(format!("Canvas {canvas_id} not found")))
}

pub(crate) fn collaboration_snapshot(
    db_path: &Path,
    canvas: &SpatialCanvas,
) -> CanvasCollaborationSnapshot {
    let key = collaboration_key(db_path, canvas);
    lock_hub().snapshot(key, Utc::now())
}

pub(crate) fn collaboration_events_after(
    db_path: &Path,
    canvas: &SpatialCanvas,
    after_sequence: u64,
) -> CanvasCollaborationEventPage {
    let key = collaboration_key(db_path, canvas);
    lock_hub().events_after(key, after_sequence, MAX_EVENT_PAGE, Utc::now())
}

fn collaboration_key(db_path: &Path, canvas: &SpatialCanvas) -> CanvasCollaborationKey {
    CanvasCollaborationKey {
        namespace: db_path.to_string_lossy().into_owned(),
        canvas_id: canvas.id.clone(),
        owner_user_id: canvas.owner_user_id.clone(),
    }
}

fn lock_hub() -> MutexGuard<'static, CanvasCollaborationHub> {
    COLLABORATION_HUB
        .get_or_init(|| Mutex::new(CanvasCollaborationHub::default()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn validate_identifier(label: &str, value: &str) -> Result<(), AppError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(AppError::BadRequest(format!("{label} id cannot be empty")));
    }
    if value.len() > MAX_ID_BYTES {
        return Err(AppError::BadRequest(format!("{label} id is too long")));
    }
    Ok(())
}

fn validate_cursor(cursor: CanvasCursor) -> Result<(), AppError> {
    if !cursor.x.is_finite()
        || !cursor.y.is_finite()
        || cursor.x.abs() > WORLD_COORDINATE_LIMIT
        || cursor.y.abs() > WORLD_COORDINATE_LIMIT
    {
        return Err(AppError::BadRequest(
            "Canvas cursor must contain finite in-bounds world coordinates".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CanvasCollaborationKey {
    namespace: String,
    canvas_id: String,
    owner_user_id: Option<String>,
}

#[derive(Debug, Default)]
struct CanvasCollaborationHub {
    canvases: HashMap<CanvasCollaborationKey, CanvasCollaborationState>,
}

impl CanvasCollaborationHub {
    fn heartbeat(
        &mut self,
        key: CanvasCollaborationKey,
        request: CanvasPresenceHeartbeatRequest,
        now: DateTime<Utc>,
    ) -> Result<CanvasHeartbeatOutcome, AppError> {
        validate_identifier("canvas client", &request.client_id)?;
        if request
            .display_name
            .as_ref()
            .is_some_and(|name| name.len() > MAX_DISPLAY_NAME_BYTES)
        {
            return Err(AppError::BadRequest(
                "Canvas presence display name is too long".to_string(),
            ));
        }
        if let Some(cursor) = request.cursor {
            validate_cursor(cursor)?;
        }

        let state = self.state_mut(key, now)?;
        state.cleanup(now);
        let was_registered = state.clients.contains_key(&request.client_id);
        if !was_registered && state.clients.len() >= MAX_CLIENTS_PER_CANVAS {
            return Err(AppError::Conflict(format!(
                "Canvas already has the maximum of {MAX_CLIENTS_PER_CANVAS} presence clients"
            )));
        }
        if let Some(token) = request.presence_token.as_deref() {
            validate_presence_token(token)?;
        }
        let record = state
            .clients
            .entry(request.client_id.clone())
            .or_insert_with(|| PresenceRecord {
                client_id: request.client_id.clone(),
                presence_token: Uuid::new_v4().to_string(),
                display_name: None,
                cursor: None,
                last_seen_at: now,
                stale_announced: false,
            });
        if was_registered
            && !request
                .presence_token
                .as_deref()
                .is_some_and(|token| tokens_equal(&record.presence_token, token))
        {
            return Err(AppError::Conflict(
                "Canvas presence token does not control this client id".to_string(),
            ));
        }
        record.display_name = request.display_name;
        if request.cursor.is_some() {
            record.cursor = request.cursor;
        }
        record.last_seen_at = now;
        record.stale_announced = false;
        let client = record.to_wire(now);
        let presence_token = record.presence_token.clone();
        let canvas_id = state.canvas_id.clone();
        state.push_event(|sequence| CanvasCollaborationEvent::PresenceUpdated {
            canvas_id,
            sequence,
            client,
        });
        Ok(CanvasHeartbeatOutcome {
            snapshot: state.snapshot(now),
            presence_token,
        })
    }

    fn leave(
        &mut self,
        key: CanvasCollaborationKey,
        client_id: &str,
        presence_token: &str,
        now: DateTime<Utc>,
    ) -> Result<CanvasCollaborationSnapshot, AppError> {
        let Some(state) = self.canvases.get_mut(&key) else {
            return Err(AppError::BadRequest(
                "Canvas presence heartbeat is required before leaving".to_string(),
            ));
        };
        state.cleanup(now);
        authorize_client(state, client_id, presence_token)?;
        if state.clients.remove(client_id).is_some() {
            state.end_previews_for_client(client_id, CanvasGeometryPreviewEndReason::ClientLeft);
            let canvas_id = state.canvas_id.clone();
            state.push_event(|sequence| CanvasCollaborationEvent::PresenceLeft {
                canvas_id,
                sequence,
                client_id: client_id.to_string(),
                reason: CanvasPresenceLeaveReason::Explicit,
            });
        }
        Ok(state.snapshot(now))
    }

    fn update_preview(
        &mut self,
        key: CanvasCollaborationKey,
        node_id: String,
        request: CanvasGeometryPreviewRequest,
        now: DateTime<Utc>,
    ) -> Result<CanvasCollaborationSnapshot, AppError> {
        validate_identifier("canvas client", &request.client_id)?;
        request.rect.validate().map_err(AppError::BadRequest)?;

        let Some(state) = self.canvases.get_mut(&key) else {
            return Err(AppError::BadRequest(
                "Canvas presence heartbeat is required before publishing geometry previews"
                    .to_string(),
            ));
        };
        state.cleanup(now);
        authorize_client(state, &request.client_id, &request.presence_token)?;
        let Some(client) = state.clients.get_mut(&request.client_id) else {
            return Err(AppError::BadRequest(
                "Canvas presence heartbeat is required before publishing geometry previews"
                    .to_string(),
            ));
        };
        client.last_seen_at = now;
        client.stale_announced = false;

        if let Some(existing) = state.previews.get(&node_id) {
            if existing.client_id != request.client_id {
                return Err(AppError::Conflict(format!(
                    "Canvas node {node_id} is being moved by another client"
                )));
            }
        } else if state.previews.len() >= MAX_PREVIEWS_PER_CANVAS {
            return Err(AppError::Conflict(format!(
                "Canvas already has the maximum of {MAX_PREVIEWS_PER_CANVAS} geometry previews"
            )));
        }

        let preview = GeometryPreviewRecord {
            node_id: node_id.clone(),
            client_id: request.client_id,
            rect: request.rect,
            z_index: request.z_index,
            updated_at: now,
        };
        state.previews.insert(node_id, preview.clone());
        let canvas_id = state.canvas_id.clone();
        state.push_event(
            |sequence| CanvasCollaborationEvent::GeometryPreviewUpdated {
                canvas_id,
                sequence,
                preview: preview.to_wire(),
            },
        );
        Ok(state.snapshot(now))
    }

    fn release_preview(
        &mut self,
        key: CanvasCollaborationKey,
        node_id: &str,
        client_id: &str,
        presence_token: &str,
        committed: bool,
        now: DateTime<Utc>,
    ) -> Result<CanvasCollaborationSnapshot, AppError> {
        let canvas_id = key.canvas_id.clone();
        let Some(state) = self.canvases.get_mut(&key) else {
            return Ok(CanvasCollaborationState::new(canvas_id, now).snapshot(now));
        };
        state.cleanup(now);
        authorize_client(state, client_id, presence_token)?;
        match state.previews.get(node_id) {
            Some(preview) if preview.client_id != client_id => {
                return Err(AppError::Conflict(format!(
                    "Canvas node {node_id} soft lock belongs to another client"
                )));
            }
            Some(_) => {}
            None => return Ok(state.snapshot(now)),
        }
        state.previews.remove(node_id);
        let canvas_id = state.canvas_id.clone();
        let reason = if committed {
            CanvasGeometryPreviewEndReason::Committed
        } else {
            CanvasGeometryPreviewEndReason::Released
        };
        state.push_event(|sequence| CanvasCollaborationEvent::GeometryPreviewEnded {
            canvas_id,
            sequence,
            node_id: node_id.to_string(),
            client_id: client_id.to_string(),
            reason,
        });
        Ok(state.snapshot(now))
    }

    fn snapshot(
        &mut self,
        key: CanvasCollaborationKey,
        now: DateTime<Utc>,
    ) -> CanvasCollaborationSnapshot {
        let canvas_id = key.canvas_id.clone();
        let Some(state) = self.canvases.get_mut(&key) else {
            return CanvasCollaborationState::new(canvas_id, now).snapshot(now);
        };
        state.cleanup(now);
        state.snapshot(now)
    }

    fn events_after(
        &mut self,
        key: CanvasCollaborationKey,
        after_sequence: u64,
        limit: usize,
        now: DateTime<Utc>,
    ) -> CanvasCollaborationEventPage {
        let canvas_id = key.canvas_id.clone();
        let Some(state) = self.canvases.get_mut(&key) else {
            return CanvasCollaborationState::new(canvas_id, now)
                .events_after(after_sequence, limit);
        };
        state.cleanup(now);
        state.events_after(after_sequence, limit)
    }

    fn state_mut(
        &mut self,
        key: CanvasCollaborationKey,
        now: DateTime<Utc>,
    ) -> Result<&mut CanvasCollaborationState, AppError> {
        if !self.canvases.contains_key(&key) {
            self.evict_for_new_canvas(now);
            if self.canvases.len() >= MAX_TRACKED_CANVASES {
                return Err(AppError::Conflict(format!(
                    "Canvas collaboration is at its bounded capacity of {MAX_TRACKED_CANVASES} active canvases"
                )));
            }
        }
        let canvas_id = key.canvas_id.clone();
        Ok(self
            .canvases
            .entry(key)
            .or_insert_with(|| CanvasCollaborationState::new(canvas_id, now)))
    }

    fn evict_for_new_canvas(&mut self, now: DateTime<Utc>) {
        let expire_before = now - Duration::seconds(PRESENCE_EXPIRE_AFTER_SECS);
        self.canvases.retain(|_, state| {
            !(state.clients.is_empty()
                && state.previews.is_empty()
                && state.last_touched_at < expire_before)
        });
        if self.canvases.len() < MAX_TRACKED_CANVASES {
            return;
        }
        if let Some(oldest) = self
            .canvases
            .iter()
            .filter(|(_, state)| state.clients.is_empty() && state.previews.is_empty())
            .min_by_key(|(_, state)| state.last_touched_at)
            .map(|(key, _)| key.clone())
        {
            self.canvases.remove(&oldest);
        }
    }
}

#[derive(Debug)]
struct CanvasCollaborationState {
    canvas_id: String,
    sequence: u64,
    clients: HashMap<String, PresenceRecord>,
    previews: HashMap<String, GeometryPreviewRecord>,
    events: VecDeque<CanvasCollaborationEvent>,
    last_touched_at: DateTime<Utc>,
}

impl CanvasCollaborationState {
    fn new(canvas_id: String, now: DateTime<Utc>) -> Self {
        Self {
            canvas_id,
            sequence: 0,
            clients: HashMap::new(),
            previews: HashMap::new(),
            events: VecDeque::new(),
            last_touched_at: now,
        }
    }

    fn cleanup(&mut self, now: DateTime<Utc>) {
        self.last_touched_at = now;
        let stale_after = Duration::seconds(PRESENCE_STALE_AFTER_SECS);
        let expire_after = Duration::seconds(PRESENCE_EXPIRE_AFTER_SECS);

        let newly_stale = self
            .clients
            .values_mut()
            .filter_map(|client| {
                let stale = now.signed_duration_since(client.last_seen_at) > stale_after;
                if stale && !client.stale_announced {
                    client.stale_announced = true;
                    Some(client.client_id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for client_id in newly_stale {
            let canvas_id = self.canvas_id.clone();
            self.push_event(|sequence| CanvasCollaborationEvent::PresenceStale {
                canvas_id,
                sequence,
                client_id,
            });
        }

        let stale_previews = self
            .previews
            .values()
            .filter_map(|preview| {
                let client = self.clients.get(&preview.client_id)?;
                (now.signed_duration_since(client.last_seen_at) > stale_after)
                    .then(|| (preview.node_id.clone(), preview.client_id.clone()))
            })
            .collect::<Vec<_>>();
        for (node_id, client_id) in stale_previews {
            self.previews.remove(&node_id);
            let canvas_id = self.canvas_id.clone();
            self.push_event(|sequence| CanvasCollaborationEvent::GeometryPreviewEnded {
                canvas_id,
                sequence,
                node_id,
                client_id,
                reason: CanvasGeometryPreviewEndReason::Stale,
            });
        }

        let expired_clients = self
            .clients
            .values()
            .filter(|client| now.signed_duration_since(client.last_seen_at) > expire_after)
            .map(|client| client.client_id.clone())
            .collect::<Vec<_>>();
        for client_id in expired_clients {
            self.clients.remove(&client_id);
            self.end_previews_for_client(&client_id, CanvasGeometryPreviewEndReason::Expired);
            let canvas_id = self.canvas_id.clone();
            self.push_event(|sequence| CanvasCollaborationEvent::PresenceLeft {
                canvas_id,
                sequence,
                client_id,
                reason: CanvasPresenceLeaveReason::Expired,
            });
        }
    }

    fn end_previews_for_client(&mut self, client_id: &str, reason: CanvasGeometryPreviewEndReason) {
        let nodes = self
            .previews
            .values()
            .filter(|preview| preview.client_id == client_id)
            .map(|preview| preview.node_id.clone())
            .collect::<Vec<_>>();
        for node_id in nodes {
            self.previews.remove(&node_id);
            let canvas_id = self.canvas_id.clone();
            self.push_event(|sequence| CanvasCollaborationEvent::GeometryPreviewEnded {
                canvas_id,
                sequence,
                node_id,
                client_id: client_id.to_string(),
                reason,
            });
        }
    }

    fn push_event(&mut self, make_event: impl FnOnce(u64) -> CanvasCollaborationEvent) {
        self.sequence = self.sequence.saturating_add(1);
        self.events.push_back(make_event(self.sequence));
        while self.events.len() > MAX_EVENTS_PER_CANVAS {
            self.events.pop_front();
        }
    }

    fn snapshot(&self, now: DateTime<Utc>) -> CanvasCollaborationSnapshot {
        let stale_after = Duration::seconds(PRESENCE_STALE_AFTER_SECS);
        let mut clients = self
            .clients
            .values()
            .map(|client| client.to_wire(now))
            .collect::<Vec<_>>();
        clients.sort_by(|left, right| left.client_id.cmp(&right.client_id));
        for client in &mut clients {
            if let Some(record) = self.clients.get(&client.client_id) {
                client.stale = now.signed_duration_since(record.last_seen_at) > stale_after;
            }
        }
        let mut geometry_previews = self
            .previews
            .values()
            .map(GeometryPreviewRecord::to_wire)
            .collect::<Vec<_>>();
        geometry_previews.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        CanvasCollaborationSnapshot {
            canvas_id: self.canvas_id.clone(),
            sequence: self.sequence,
            clients,
            geometry_previews,
        }
    }

    fn events_after(&self, after_sequence: u64, limit: usize) -> CanvasCollaborationEventPage {
        let earliest_sequence = self.events.front().map(CanvasCollaborationEvent::sequence);
        let gap = after_sequence > self.sequence
            || earliest_sequence
                .is_some_and(|earliest| after_sequence.saturating_add(1) < earliest);
        let events = self
            .events
            .iter()
            .filter(|event| event.sequence() > after_sequence)
            .take(limit.clamp(1, MAX_EVENT_PAGE))
            .cloned()
            .collect();
        CanvasCollaborationEventPage {
            events,
            earliest_sequence,
            latest_sequence: self.sequence,
            gap,
        }
    }
}

#[derive(Debug, Clone)]
struct PresenceRecord {
    client_id: String,
    presence_token: String,
    display_name: Option<String>,
    cursor: Option<CanvasCursor>,
    last_seen_at: DateTime<Utc>,
    stale_announced: bool,
}

#[derive(Debug)]
struct CanvasHeartbeatOutcome {
    snapshot: CanvasCollaborationSnapshot,
    presence_token: String,
}

fn validate_presence_token(token: &str) -> Result<(), AppError> {
    if token.is_empty() || token.len() > MAX_PRESENCE_TOKEN_BYTES {
        return Err(AppError::BadRequest(
            "Canvas presence token is invalid".to_string(),
        ));
    }
    Ok(())
}

fn authorize_client(
    state: &CanvasCollaborationState,
    client_id: &str,
    presence_token: &str,
) -> Result<(), AppError> {
    validate_presence_token(presence_token)?;
    let Some(client) = state.clients.get(client_id) else {
        return Err(AppError::BadRequest(
            "Canvas presence heartbeat is required before this operation".to_string(),
        ));
    };
    if !tokens_equal(&client.presence_token, presence_token) {
        return Err(AppError::Conflict(
            "Canvas presence token does not control this client id".to_string(),
        ));
    }
    Ok(())
}

fn tokens_equal(expected: &str, provided: &str) -> bool {
    if expected.len() != provided.len() {
        return false;
    }
    expected
        .bytes()
        .zip(provided.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

impl PresenceRecord {
    fn to_wire(&self, now: DateTime<Utc>) -> CanvasPresenceClient {
        CanvasPresenceClient {
            client_id: self.client_id.clone(),
            display_name: self.display_name.clone(),
            cursor: self.cursor,
            last_seen_at: self.last_seen_at.to_rfc3339(),
            stale: now.signed_duration_since(self.last_seen_at)
                > Duration::seconds(PRESENCE_STALE_AFTER_SECS),
        }
    }
}

#[derive(Debug, Clone)]
struct GeometryPreviewRecord {
    node_id: String,
    client_id: String,
    rect: mitsuro_core::storage::WorldRect,
    z_index: i64,
    updated_at: DateTime<Utc>,
}

impl GeometryPreviewRecord {
    fn to_wire(&self) -> CanvasGeometryPreview {
        CanvasGeometryPreview {
            node_id: self.node_id.clone(),
            client_id: self.client_id.clone(),
            rect: self.rect,
            z_index: self.z_index,
            updated_at: self.updated_at.to_rfc3339(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitsuro_core::storage::{CreateCanvasInput, SpatialCanvasSurface, WorldRect};
    use tempfile::TempDir;

    fn create_canvas(
        temp: &TempDir,
        owner: Option<&str>,
        operation: &str,
    ) -> (std::path::PathBuf, SpatialCanvas) {
        let db_path = temp.path().join("canvas-presence.db");
        let store = SpatialCanvasStore::new(Database::new(&db_path).expect("database"));
        let canvas = store
            .create_for_exact_owner(
                owner,
                CreateCanvasInput {
                    create_op_id: operation.to_string(),
                    project_dir: "/tmp/project".to_string(),
                    surface: SpatialCanvasSurface::Code,
                    name: operation.to_string(),
                    shared_theme_id: "graphite".to_string(),
                },
            )
            .expect("canvas")
            .canvas;
        (db_path, canvas)
    }

    fn heartbeat(client_id: &str) -> CanvasPresenceHeartbeatRequest {
        CanvasPresenceHeartbeatRequest {
            client_id: client_id.to_string(),
            presence_token: None,
            display_name: Some(client_id.to_string()),
            cursor: Some(CanvasCursor { x: -8.0, y: 14.0 }),
        }
    }

    fn heartbeat_with_token(
        client_id: &str,
        presence_token: &str,
    ) -> CanvasPresenceHeartbeatRequest {
        CanvasPresenceHeartbeatRequest {
            presence_token: Some(presence_token.to_string()),
            ..heartbeat(client_id)
        }
    }

    fn register(
        hub: &mut CanvasCollaborationHub,
        key: CanvasCollaborationKey,
        client_id: &str,
        now: DateTime<Utc>,
    ) -> String {
        hub.heartbeat(key, heartbeat(client_id), now)
            .expect("presence registration")
            .presence_token
    }

    fn preview(client_id: &str, presence_token: &str, x: f64) -> CanvasGeometryPreviewRequest {
        CanvasGeometryPreviewRequest {
            client_id: client_id.to_string(),
            presence_token: presence_token.to_string(),
            rect: WorldRect {
                x,
                y: -20.0,
                width: 320.0,
                height: 180.0,
            },
            z_index: 4,
        }
    }

    #[test]
    fn owner_lookup_is_exact_including_local_null() {
        let temp = TempDir::new().expect("temp");
        let (db_path, local) = create_canvas(&temp, None, "local-main");
        let (_, alice) = create_canvas(&temp, Some("alice"), "alice-main");

        assert!(load_owned_canvas(&db_path, &local.id, None).is_ok());
        assert!(matches!(
            load_owned_canvas(&db_path, &alice.id, None),
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            load_owned_canvas(&db_path, &local.id, Some("alice")),
            Err(AppError::NotFound(_))
        ));
        assert!(matches!(
            load_owned_canvas(&db_path, "missing", Some("alice")),
            Err(AppError::NotFound(_))
        ));
    }

    #[test]
    fn two_clients_contend_then_recover_after_stale_window() {
        let now = Utc::now();
        let key = CanvasCollaborationKey {
            namespace: "test-contention".to_string(),
            canvas_id: "canvas".to_string(),
            owner_user_id: Some("alice".to_string()),
        };
        let mut hub = CanvasCollaborationHub::default();
        let token_a = register(&mut hub, key.clone(), "a", now);
        let token_b = register(&mut hub, key.clone(), "b", now);
        hub.update_preview(
            key.clone(),
            "node".to_string(),
            preview("a", &token_a, 1.0),
            now,
        )
        .expect("client a lock");

        assert!(matches!(
            hub.update_preview(
                key.clone(),
                "node".to_string(),
                preview("b", &token_b, 2.0),
                now
            ),
            Err(AppError::Conflict(_))
        ));

        let recovered = hub
            .update_preview(
                key,
                "node".to_string(),
                preview("b", &token_b, 3.0),
                now + Duration::seconds(PRESENCE_STALE_AFTER_SECS + 1),
            )
            .expect("stale lock should be recoverable");
        assert_eq!(recovered.geometry_previews[0].client_id, "b");
        assert_eq!(recovered.geometry_previews[0].rect.x, 3.0);
    }

    #[test]
    fn presence_is_stale_at_fifteen_and_expires_at_sixty_seconds() {
        let now = Utc::now();
        let key = CanvasCollaborationKey {
            namespace: "test-expiry".to_string(),
            canvas_id: "canvas".to_string(),
            owner_user_id: None,
        };
        let mut hub = CanvasCollaborationHub::default();
        let token = register(&mut hub, key.clone(), "client", now);
        hub.update_preview(
            key.clone(),
            "node".to_string(),
            preview("client", &token, 0.0),
            now,
        )
        .expect("preview");

        let stale = hub.snapshot(
            key.clone(),
            now + Duration::seconds(PRESENCE_STALE_AFTER_SECS + 1),
        );
        assert!(stale.clients[0].stale);
        assert!(stale.geometry_previews.is_empty());

        let expired = hub.snapshot(key, now + Duration::seconds(PRESENCE_EXPIRE_AFTER_SECS + 1));
        assert!(expired.clients.is_empty());
        assert!(expired.geometry_previews.is_empty());
    }

    #[test]
    fn preview_and_event_buffers_are_bounded() {
        let now = Utc::now();
        let key = CanvasCollaborationKey {
            namespace: "test-bounds".to_string(),
            canvas_id: "canvas".to_string(),
            owner_user_id: None,
        };
        let mut hub = CanvasCollaborationHub::default();
        let token = register(&mut hub, key.clone(), "client", now);
        for index in 0..MAX_PREVIEWS_PER_CANVAS {
            hub.update_preview(
                key.clone(),
                format!("node-{index}"),
                preview("client", &token, index as f64),
                now,
            )
            .expect("bounded preview");
        }
        assert!(matches!(
            hub.update_preview(
                key.clone(),
                "one-too-many".to_string(),
                preview("client", &token, 300.0),
                now
            ),
            Err(AppError::Conflict(_))
        ));
        for index in 0..(MAX_EVENTS_PER_CANVAS + 20) {
            hub.heartbeat(
                key.clone(),
                CanvasPresenceHeartbeatRequest {
                    presence_token: Some(token.clone()),
                    cursor: Some(CanvasCursor {
                        x: index as f64,
                        y: 0.0,
                    }),
                    ..heartbeat("client")
                },
                now,
            )
            .expect("cursor update");
        }
        let state = hub.canvases.get(&key).expect("canvas state");
        assert_eq!(state.previews.len(), MAX_PREVIEWS_PER_CANVAS);
        assert_eq!(state.events.len(), MAX_EVENTS_PER_CANVAS);
    }

    #[test]
    fn capacity_rejects_a_new_canvas_without_evicting_active_locks() {
        let now = Utc::now();
        let mut hub = CanvasCollaborationHub::default();
        for index in 0..MAX_TRACKED_CANVASES {
            let key = CanvasCollaborationKey {
                namespace: "test-capacity".to_string(),
                canvas_id: format!("canvas-{index}"),
                owner_user_id: Some("alice".to_string()),
            };
            let token = register(&mut hub, key.clone(), "client", now);
            hub.update_preview(
                key,
                "node".to_string(),
                preview("client", &token, index as f64),
                now,
            )
            .expect("active canvas lock");
        }

        let overflow = CanvasCollaborationKey {
            namespace: "test-capacity".to_string(),
            canvas_id: "overflow".to_string(),
            owner_user_id: Some("alice".to_string()),
        };
        assert!(matches!(
            hub.heartbeat(overflow, heartbeat("client"), now),
            Err(AppError::Conflict(_))
        ));
        assert_eq!(hub.canvases.len(), MAX_TRACKED_CANVASES);
        assert!(hub
            .canvases
            .values()
            .all(|state| state.clients.len() == 1 && state.previews.len() == 1));
    }

    #[test]
    fn previews_never_change_canvas_snapshot_or_event_ledger() {
        let temp = TempDir::new().expect("temp");
        let (db_path, canvas) = create_canvas(&temp, None, "non-persistent-main");
        let store = SpatialCanvasStore::new(Database::new(&db_path).expect("database"));
        let initial_event_page = store
            .list_events_after_for_exact_owner(&canvas.id, None, 0, 256)
            .expect("events")
            .expect("canvas");
        let key = collaboration_key(&db_path, &canvas);
        let now = Utc::now();
        let mut hub = CanvasCollaborationHub::default();
        let token = register(&mut hub, key.clone(), "client", now);
        hub.update_preview(
            key,
            "not-a-persisted-node".to_string(),
            preview("client", &token, 72.0),
            now,
        )
        .expect("ephemeral preview");

        let reloaded = store
            .get_for_exact_owner(&canvas.id, None)
            .expect("load")
            .expect("canvas");
        let final_event_page = store
            .list_events_after_for_exact_owner(&canvas.id, None, 0, 256)
            .expect("events")
            .expect("canvas");
        assert_eq!(reloaded, canvas);
        assert_eq!(final_event_page, initial_event_page);
    }

    #[test]
    fn another_client_cannot_spoof_presence_or_release_soft_locks() {
        let now = Utc::now();
        let key = CanvasCollaborationKey {
            namespace: "test-token-isolation".to_string(),
            canvas_id: "canvas".to_string(),
            owner_user_id: Some("alice".to_string()),
        };
        let mut hub = CanvasCollaborationHub::default();
        let token_a = register(&mut hub, key.clone(), "a", now);
        let token_b = register(&mut hub, key.clone(), "b", now);
        hub.update_preview(
            key.clone(),
            "node".to_string(),
            preview("a", &token_a, 1.0),
            now,
        )
        .expect("client a lock");

        assert!(matches!(
            hub.heartbeat(key.clone(), heartbeat_with_token("a", &token_b), now),
            Err(AppError::Conflict(_))
        ));
        assert!(matches!(
            hub.update_preview(
                key.clone(),
                "node".to_string(),
                preview("a", &token_b, 2.0),
                now
            ),
            Err(AppError::Conflict(_))
        ));
        assert!(matches!(
            hub.release_preview(key.clone(), "node", "a", &token_b, false, now),
            Err(AppError::Conflict(_))
        ));
        assert!(matches!(
            hub.leave(key.clone(), "a", &token_b, now),
            Err(AppError::Conflict(_))
        ));

        let snapshot = hub.snapshot(key, now);
        assert!(snapshot
            .clients
            .iter()
            .any(|client| client.client_id == "a"));
        assert_eq!(snapshot.geometry_previews[0].client_id, "a");
    }
}
