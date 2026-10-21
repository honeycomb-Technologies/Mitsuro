use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::{
    extract::{Path as AxumPath, Query, State},
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Json, Router,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use mitsuro_core::storage::{
    CanvasNodeBinding, CanvasOperation, CanvasPatchInput, CreateCanvasInput, Database,
    SpatialCanvas, SpatialCanvasStore,
};
use mitsuro_core::SessionManager;

use super::session_access::{current_user_id, load_owned_session, request_workspace_scope};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{
    CanvasStreamEnvelope, CanvasStreamQuery, CreateCanvasRequest, CreateCanvasResponse,
    ListCanvasesQuery, ListCanvasesResponse, PatchCanvasRequest, PatchCanvasResponse,
};
use crate::utils::workspace::resolve_scoped_workspace_path;
use crate::AppState;

const CANVAS_POLL_INTERVAL: Duration = Duration::from_millis(250);

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_canvases).post(create_canvas))
        .route("/:id", get(get_canvas).patch(patch_canvas))
        .route("/:id/stream", get(stream_canvas))
        .merge(super::canvas_presence::router())
}

async fn list_canvases(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Query(query): Query<ListCanvasesQuery>,
) -> Result<Json<ListCanvasesResponse>, AppError> {
    let user = CurrentUser(user);
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let canvases = store
        .list_for_exact_owner(current_user_id(Some(&user)), query.surface)
        .map_err(AppError::from)?;
    Ok(Json(ListCanvasesResponse { canvases }))
}

async fn create_canvas(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Json(request): Json<CreateCanvasRequest>,
) -> Result<Json<CreateCanvasResponse>, AppError> {
    let user = CurrentUser(user);
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let owner_user_id = current_user_id(Some(&user));
    if let Some((outcome, stored_request)) = store
        .create_replay_for_exact_owner(owner_user_id, request.create_op_id.trim())
        .map_err(map_create_error)?
    {
        let requested_project = request.project_dir.trim();
        let project_matches = requested_project == stored_request.project_dir
            || canonical_project_dir(&state, &user, requested_project)
                .is_ok_and(|canonical| canonical == stored_request.project_dir);
        if !project_matches
            || request.surface != stored_request.surface
            || request.name.trim() != stored_request.name
            || request.shared_theme_id.trim() != stored_request.shared_theme_id
        {
            return Err(AppError::BadRequest(
                "canvas create operation was reused with different input".to_string(),
            ));
        }
        return Ok(Json(CreateCanvasResponse {
            canvas: outcome.canvas,
            created: false,
        }));
    }
    let project_dir = canonical_project_dir(&state, &user, &request.project_dir)?;
    let outcome = store
        .create_for_exact_owner(
            owner_user_id,
            CreateCanvasInput {
                create_op_id: request.create_op_id,
                project_dir,
                surface: request.surface,
                name: request.name.trim().to_string(),
                shared_theme_id: request.shared_theme_id.trim().to_string(),
            },
        )
        .map_err(map_create_error)?;
    Ok(Json(CreateCanvasResponse {
        canvas: outcome.canvas,
        created: outcome.created,
    }))
}

async fn get_canvas(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
) -> Result<Json<SpatialCanvas>, AppError> {
    let user = CurrentUser(user);
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let canvas = store
        .get_for_exact_owner(&canvas_id, current_user_id(Some(&user)))
        .map_err(AppError::from)?
        .ok_or_else(|| canvas_not_found(&canvas_id))?;
    Ok(Json(canvas))
}

async fn patch_canvas(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Json(request): Json<PatchCanvasRequest>,
) -> Result<Json<PatchCanvasResponse>, AppError> {
    let user = CurrentUser(user);
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let canvas = store
        .get_for_exact_owner(&canvas_id, current_user_id(Some(&user)))
        .map_err(AppError::from)?
        .ok_or_else(|| canvas_not_found(&canvas_id))?;
    let patch_input = CanvasPatchInput {
        op_id: request.op_id,
        client_id: request.client_id,
        expected_revision: request.expected_revision,
        operations: request.operations,
    };
    if let Some(outcome) = store
        .patch_replay_for_exact_owner(&canvas_id, current_user_id(Some(&user)), &patch_input)
        .map_err(map_patch_error)?
    {
        return Ok(Json(PatchCanvasResponse {
            canvas: outcome.canvas,
            applied: false,
        }));
    }
    validate_patch_bindings(&state, &user, &canvas, &patch_input.operations).await?;
    let outcome = store
        .patch_for_exact_owner(&canvas_id, current_user_id(Some(&user)), patch_input)
        .map_err(map_patch_error)?;
    Ok(Json(PatchCanvasResponse {
        canvas: outcome.canvas,
        applied: outcome.applied,
    }))
}

async fn stream_canvas(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    AxumPath(canvas_id): AxumPath<String>,
    Query(query): Query<CanvasStreamQuery>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    let owner_user_id = user.user_id;
    let store = SpatialCanvasStore::new(Database::new(&state.db_path)?);
    let canvas = store
        .get_for_exact_owner(&canvas_id, owner_user_id.as_deref())
        .map_err(AppError::from)?
        .ok_or_else(|| canvas_not_found(&canvas_id))?;
    let initial_sequence = query.after_sequence;
    let db_path = state.db_path.as_ref().clone();
    let (tx, rx) = mpsc::channel(32);

    std::thread::Builder::new()
        .name("mitsuro-canvas-stream".to_string())
        .spawn(move || {
            let Ok(db) = Database::new(&db_path) else {
                return;
            };
            let store = SpatialCanvasStore::new(db);
            let mut cursor = initial_sequence.unwrap_or(canvas.revision);
            if initial_sequence.is_none()
                && send_envelope_blocking(
                    &tx,
                    CanvasStreamEnvelope::Snapshot {
                        canvas: canvas.clone(),
                    },
                )
                .is_err()
            {
                return;
            }
            let collaboration = super::canvas_presence::collaboration_snapshot(&db_path, &canvas);
            let mut collaboration_cursor = collaboration.sequence;
            if send_envelope_blocking(
                &tx,
                CanvasStreamEnvelope::CollaborationSnapshot { collaboration },
            )
            .is_err()
            {
                return;
            }
            loop {
                std::thread::sleep(CANVAS_POLL_INTERVAL);
                if tx.is_closed() {
                    break;
                }
                let page = match store.list_events_after_for_exact_owner(
                    &canvas_id,
                    owner_user_id.as_deref(),
                    cursor,
                    256,
                ) {
                    Ok(Some(page)) => page,
                    Ok(None) | Err(_) => break,
                };
                if page.gap {
                    let snapshot =
                        match store.get_for_exact_owner(&canvas_id, owner_user_id.as_deref()) {
                            Ok(Some(snapshot)) => snapshot,
                            _ => break,
                        };
                    cursor = snapshot.revision;
                    if send_envelope_blocking(
                        &tx,
                        CanvasStreamEnvelope::Snapshot { canvas: snapshot },
                    )
                    .is_err()
                    {
                        break;
                    }
                    continue;
                }
                if page.events.is_empty() {
                    // Persisted canvas updates and transient collaboration
                    // updates share one SSE connection but keep independent
                    // sequence domains.
                } else {
                    cursor = page
                        .events
                        .last()
                        .map(|event| event.sequence)
                        .unwrap_or(cursor);
                    if send_envelope_blocking(&tx, CanvasStreamEnvelope::Events { page }).is_err() {
                        break;
                    }
                }

                let collaboration_page = super::canvas_presence::collaboration_events_after(
                    &db_path,
                    &canvas,
                    collaboration_cursor,
                );
                if collaboration_page.gap {
                    let collaboration =
                        super::canvas_presence::collaboration_snapshot(&db_path, &canvas);
                    collaboration_cursor = collaboration.sequence;
                    if send_envelope_blocking(
                        &tx,
                        CanvasStreamEnvelope::CollaborationSnapshot { collaboration },
                    )
                    .is_err()
                    {
                        break;
                    }
                } else if !collaboration_page.events.is_empty() {
                    collaboration_cursor = collaboration_page
                        .events
                        .last()
                        .map(crate::types::CanvasCollaborationEvent::sequence)
                        .unwrap_or(collaboration_cursor);
                    if send_envelope_blocking(
                        &tx,
                        CanvasStreamEnvelope::CollaborationEvents {
                            page: collaboration_page,
                        },
                    )
                    .is_err()
                    {
                        break;
                    }
                }
            }
        })
        .map_err(|error| AppError::Internal(format!("Failed to start canvas stream: {error}")))?;

    Ok(Sse::new(ReceiverStream::new(rx)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("canvas-keepalive"),
    ))
}

fn send_envelope_blocking(
    sender: &mpsc::Sender<Result<Event, Infallible>>,
    envelope: CanvasStreamEnvelope,
) -> Result<(), ()> {
    let event = Event::default()
        .event("canvas")
        .json_data(envelope)
        .map_err(|_| ())?;
    sender.blocking_send(Ok(event)).map_err(|_| ())
}

fn canonical_project_dir(
    state: &AppState,
    user: &CurrentUser,
    requested: &str,
) -> Result<String, AppError> {
    let scope = request_workspace_scope(state, Some(user));
    canonicalize_project_dir(requested, &scope.base_dir, &scope.allowed_root)
}

fn canonicalize_project_dir(
    requested: &str,
    base_dir: &Path,
    allowed_root: &Path,
) -> Result<String, AppError> {
    let resolved = resolve_scoped_workspace_path(Some(requested), base_dir, allowed_root)?;
    let canonical = resolved.canonicalize().map_err(|_| {
        AppError::NotFound(format!(
            "Project directory not found: {}",
            resolved.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(AppError::BadRequest(format!(
            "Project path is not a directory: {}",
            canonical.display()
        )));
    }
    let canonical_root = allowed_root.canonicalize().map_err(|error| {
        AppError::Internal(format!("Failed to canonicalize allowed root: {error}"))
    })?;
    if !canonical.starts_with(canonical_root) {
        return Err(AppError::BadRequest(
            "Project directory must be within the allowed workspace root".to_string(),
        ));
    }
    Ok(canonical.to_string_lossy().into_owned())
}

async fn validate_patch_bindings(
    state: &AppState,
    user: &CurrentUser,
    canvas: &SpatialCanvas,
    operations: &[CanvasOperation],
) -> Result<(), AppError> {
    for operation in operations {
        let CanvasOperation::UpsertNode { node } = operation else {
            continue;
        };
        match &node.binding {
            CanvasNodeBinding::Agent { session_id } => {
                let manager = SessionManager::new(Database::new(&state.db_path)?);
                let session = load_owned_session(&manager, session_id, Some(user))?;
                let session_dir = session
                    .project_dir
                    .as_deref()
                    .or(session.working_dir.as_deref())
                    .ok_or_else(|| {
                        AppError::BadRequest(
                            "Agent session is not attached to a project".to_string(),
                        )
                    })?;
                let session_dir = PathBuf::from(session_dir).canonicalize().map_err(|_| {
                    AppError::BadRequest("Agent session project is unavailable".to_string())
                })?;
                if session_dir != Path::new(&canvas.project_dir) {
                    return Err(AppError::BadRequest(
                        "Agent session must belong to the same project as the canvas".to_string(),
                    ));
                }
            }
            CanvasNodeBinding::Files { path: Some(path) } => {
                resolve_scoped_workspace_path(
                    Some(path),
                    Path::new(&canvas.project_dir),
                    Path::new(&canvas.project_dir),
                )?;
            }
            CanvasNodeBinding::Terminal { terminal_id } => {
                super::terminals::validate_terminal_canvas_binding(
                    &user.0,
                    terminal_id,
                    Path::new(&canvas.project_dir),
                )
                .await?;
            }
            CanvasNodeBinding::Browser {
                browser_session_id, ..
            } => {
                super::browser::validate_browser_canvas_binding(
                    &user.0,
                    browser_session_id,
                    Path::new(&canvas.project_dir),
                )
                .await?;
            }
            CanvasNodeBinding::Files { path: None } | CanvasNodeBinding::Changes => {}
        }
    }
    Ok(())
}

fn canvas_not_found(canvas_id: &str) -> AppError {
    AppError::NotFound(format!("Canvas {canvas_id} not found"))
}

fn map_create_error(error: anyhow::Error) -> AppError {
    let message = error.to_string();
    if message.contains("already exists") || message.contains("already owned") {
        AppError::Conflict(message)
    } else if message.contains("cannot be empty")
        || message.contains("too long")
        || message.contains("absolute canonical")
        || message.contains("reused with different input")
    {
        AppError::BadRequest(message)
    } else {
        AppError::from(error)
    }
}

fn map_patch_error(error: anyhow::Error) -> AppError {
    let message = error.to_string();
    if message.contains("revision conflict")
        || message.contains("changed during mutation")
        || message.contains("already exists")
    {
        AppError::Conflict(message)
    } else if message.contains("not found")
        || message.contains("cannot be empty")
        || message.contains("must contain")
        || message.contains("canvas node")
        || message.contains("supported")
        || message.contains("too long")
        || message.contains("reused with different input")
        || message.contains("agent session")
    {
        AppError::BadRequest(message)
    } else {
        AppError::from(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_project_rejects_symlink_escape() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), root.path().join("escape"))
            .expect("create symlink");

        #[cfg(unix)]
        assert!(matches!(
            canonicalize_project_dir("escape", root.path(), root.path()),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn canonical_project_accepts_existing_directory_inside_root() {
        let root = tempfile::tempdir().expect("root");
        let project = root.path().join("project");
        std::fs::create_dir(&project).expect("project");
        assert_eq!(
            canonicalize_project_dir("project", root.path(), root.path()).expect("canonical"),
            project
                .canonicalize()
                .expect("canonical path")
                .to_string_lossy()
        );
    }
}
