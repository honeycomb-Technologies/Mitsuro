use std::sync::atomic::Ordering;

use axum::{extract::State, routing::get, Json, Router};

use mitsuro_core::storage::Database;
use mitsuro_core::SessionManager;

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::presence::snapshot_presence;
use crate::remote_access::RemoteAccessConfig;
use crate::types::{
    ActiveSessionStatusResponse, ServerAccessResponse, ServerMemoryStatusResponse,
    ServerStatusResponse, TailscaleAccessResponse, UpdateServerAccessRequest,
};
use crate::{observe_process_memory, AppState};

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/access",
            get(get_server_access).patch(update_server_access),
        )
        .route("/status", get(get_server_status))
}

async fn get_server_access(
    State(state): State<AppState>,
) -> Result<Json<ServerAccessResponse>, AppError> {
    let remote_access = state.remote_access.read().await.clone();
    Ok(Json(server_access_response(
        &remote_access,
        state.server_port,
        false,
    )))
}

async fn update_server_access(
    State(state): State<AppState>,
    Json(req): Json<UpdateServerAccessRequest>,
) -> Result<Json<ServerAccessResponse>, AppError> {
    let mut remote_access = state.remote_access.write().await;

    if let Some(enabled) = req.enabled {
        remote_access.enabled = enabled;
    }

    let rotate_token = req.rotate_token.unwrap_or(false);
    if rotate_token {
        remote_access.rotate(&state.db_path)?;
    } else {
        remote_access.persist(&state.db_path)?;
    }

    let reveal_token = rotate_token || req.reveal_token.unwrap_or(false);

    Ok(Json(server_access_response(
        &remote_access,
        state.server_port,
        reveal_token,
    )))
}

async fn get_server_status(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
) -> Result<Json<ServerStatusResponse>, AppError> {
    let session_manager = SessionManager::new(Database::new(&state.db_path)?);
    let user_id = user.as_ref().and_then(|u| u.0.user_id.as_deref());
    let visible_sessions = load_visible_active_sessions(&session_manager, user_id)?;

    let mut presence = state.session_presence.write().await;
    let mut active_sessions = visible_sessions
        .into_iter()
        .map(|(session, agent_state)| {
            let presence_snapshot = snapshot_presence(&mut presence, &session.id);

            ActiveSessionStatusResponse {
                id: session.id,
                title: session.title,
                agent_state: agent_state.state,
                started_at: agent_state.started_at,
                last_event_at: agent_state.last_event_at,
                working_dir: session.working_dir,
                project_dir: session.project_dir,
                workspace_mode: session.workspace_mode,
                active_viewers: presence_snapshot.active_viewers,
                active_controllers: presence_snapshot.active_controllers,
                stale_clients: presence_snapshot.stale_clients,
            }
        })
        .collect::<Vec<_>>();
    active_sessions.sort_by(|left, right| left.id.cmp(&right.id));
    let memory = observe_process_memory(&state);

    Ok(Json(ServerStatusResponse {
        active_agent_streams: state.active_agent_streams.load(Ordering::Relaxed),
        active_sessions,
        memory: ServerMemoryStatusResponse {
            rss_bytes: memory.rss_bytes,
            virtual_bytes: memory.virtual_bytes,
            peak_rss_bytes: Some(state.peak_rss_bytes.load(Ordering::Relaxed)),
            peak_virtual_bytes: Some(state.peak_virtual_bytes.load(Ordering::Relaxed)),
        },
        tailscale: tailscale_status(state.server_port),
    }))
}

fn server_access_response(
    remote_access: &RemoteAccessConfig,
    server_port: u16,
    reveal_token: bool,
) -> ServerAccessResponse {
    let tailscale = tailscale_status(server_port);

    ServerAccessResponse {
        local_url: format!("http://localhost:{server_port}"),
        remote_access_enabled: remote_access.enabled,
        remote_access_token_available: !remote_access.token.trim().is_empty(),
        revealed_remote_access_token: reveal_token.then(|| remote_access.token.clone()),
        remote_launch_url: if remote_access.enabled {
            tailscale.url.clone()
        } else {
            None
        },
        tailscale,
    }
}

fn load_visible_active_sessions(
    session_manager: &SessionManager,
    user_id: Option<&str>,
) -> Result<
    Vec<(
        mitsuro_core::storage::SessionInfo,
        mitsuro_core::storage::AgentState,
    )>,
    AppError,
> {
    Ok(session_manager.list_active_session_details_for_user(user_id)?)
}

fn tailscale_status(port: u16) -> TailscaleAccessResponse {
    if !mitsuro_core::tailscale::is_installed() {
        return TailscaleAccessResponse {
            status: "not_installed".to_string(),
            url: None,
            detail: None,
        };
    }

    match mitsuro_core::tailscale::device_info() {
        Ok(info) if !info.online => TailscaleAccessResponse {
            status: "offline".to_string(),
            url: None,
            detail: None,
        },
        Ok(_) => TailscaleAccessResponse {
            status: "available".to_string(),
            url: mitsuro_core::tailscale::device_url(port).ok(),
            detail: None,
        },
        Err(error) => TailscaleAccessResponse {
            status: "failed".to_string(),
            url: None,
            detail: Some(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::routes::test_support::current_user;
    use std::collections::HashMap;
    use std::sync::{atomic::AtomicUsize, Arc};

    use axum::extract::State;
    use axum::Json;
    use chrono::Utc;
    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::{AgentCancellation, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::Database;
    use mitsuro_core::tools::registry::ToolRegistry;
    use mitsuro_core::SessionManager;

    use super::{get_server_access, get_server_status, update_server_access};
    use crate::presence::{PresenceCapability, SessionPresenceRecord};
    use crate::types::UpdateServerAccessRequest;
    use crate::AppState;

    fn create_test_state() -> (AppState, std::path::PathBuf) {
        let temp_dir =
            std::env::temp_dir().join(format!("mitsuro-server-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
        let db_path = temp_dir.join("mitsuro.db");
        Database::new(&db_path).expect("database should initialize");

        (
            AppState {
                server_port: 3000,
                db_path: Arc::new(db_path),
                working_dir: Arc::new(temp_dir.clone()),
                ai_client: None,
                tool_registry: Arc::new(ToolRegistry::new()),
                process_registry: Arc::new(ProcessRegistry::new()),
                model_registry: create_model_registry(),
                credential_store: Arc::new(RwLock::new(CredentialStore::default())),
                mcp_manager: Arc::new(McpManager::new(temp_dir.clone())),
                hook_manager: Arc::new(RwLock::new(UserHookManager::new())),
                skills_manager: Arc::new(RwLock::new(SkillsManager::with_defaults(&temp_dir))),
                cancellation: AgentCancellation::new(),
                session_locks: Arc::new(RwLock::new(HashMap::new())),
                session_inputs: Arc::new(RwLock::new(HashMap::new())),
                session_presence: Arc::new(RwLock::new(HashMap::new())),
                session_control_leases: Arc::new(RwLock::new(
                    crate::control_leases::SessionControlLeaseRegistry::default(),
                )),
                agent_streams: Arc::new(RwLock::new(
                    crate::agent_streams::AgentStreamHub::default(),
                )),
                delegated_state: Arc::new(RwLock::new(HashMap::new())),
                device_access: Default::default(),
                remote_access: Arc::new(RwLock::new(crate::remote_access::RemoteAccessConfig {
                    enabled: true,
                    token: "test-token".to_string(),
                })),
                active_agent_streams: Arc::new(AtomicUsize::new(0)),
                peak_rss_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                peak_virtual_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                push_service: None,
                apns_service: None,
                oauth_flows: Arc::new(Mutex::new(HashMap::new())),
                hive_runtime: crate::hive_runtime::HiveRuntimeManager::new(),
            },
            temp_dir,
        )
    }

    fn create_test_user(state: &AppState, user_id: &str) {
        let db = Database::new(&state.db_path).expect("database should open");
        db.conn()
            .execute(
                "INSERT INTO users (id, email, license_tier) VALUES (?1, ?2, ?3)",
                (user_id, format!("{user_id}@example.com"), "free"),
            )
            .expect("user should insert");
    }

    #[tokio::test]
    async fn get_server_access_hides_remote_token_by_default() {
        let (state, _temp_dir) = create_test_state();

        let Json(response) = get_server_access(State(state))
            .await
            .unwrap_or_else(|_| panic!("access request should succeed"));

        assert!(response.remote_access_enabled);
        assert!(response.remote_access_token_available);
        assert!(response.revealed_remote_access_token.is_none());
        assert_eq!(response.remote_launch_url, response.tailscale.url);
    }

    #[tokio::test]
    async fn update_server_access_rotates_token() {
        let (state, _temp_dir) = create_test_state();
        let Json(before) = match get_server_access(State(state.clone())).await {
            Ok(response) => response,
            Err(_) => panic!("access request should succeed"),
        };

        let Json(after) = update_server_access(
            State(state),
            Json(UpdateServerAccessRequest {
                enabled: None,
                rotate_token: Some(true),
                reveal_token: None,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("rotate request should succeed"));

        assert!(before.revealed_remote_access_token.is_none());
        assert!(before.remote_access_token_available);
        assert!(after.remote_access_token_available);
        assert!(after.revealed_remote_access_token.is_some());
        assert_ne!(
            before.revealed_remote_access_token,
            after.revealed_remote_access_token
        );
    }

    #[tokio::test]
    async fn update_server_access_reveals_existing_token_without_rotation() {
        let (state, _temp_dir) = create_test_state();

        let Json(response) = update_server_access(
            State(state),
            Json(UpdateServerAccessRequest {
                enabled: None,
                rotate_token: Some(false),
                reveal_token: Some(true),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("reveal request should succeed"));

        assert_eq!(
            response.revealed_remote_access_token.as_deref(),
            Some("test-token")
        );
    }

    #[tokio::test]
    async fn get_server_status_filters_active_sessions_by_owner() {
        let (state, temp_dir) = create_test_state();
        create_test_user(&state, "user-a");
        create_test_user(&state, "user-b");
        let session_manager =
            SessionManager::new(Database::new(&state.db_path).expect("database should open"));

        let my_session = session_manager
            .create_session_for_user(
                "Mine",
                None,
                Some(temp_dir.to_str().expect("temp dir should be utf-8")),
                Some("user-a"),
            )
            .expect("session should create");
        let foreign_session = session_manager
            .create_session_for_user(
                "Theirs",
                None,
                Some(temp_dir.to_str().expect("temp dir should be utf-8")),
                Some("user-b"),
            )
            .expect("session should create");
        session_manager
            .set_agent_state(&my_session, "streaming")
            .expect("agent state should persist");
        session_manager
            .set_agent_state(&foreign_session, "tool_executing")
            .expect("agent state should persist");

        let mut presence = state.session_presence.write().await;
        presence.insert(
            my_session.clone(),
            HashMap::from([(
                "client-1".to_string(),
                SessionPresenceRecord {
                    client_id: "client-1".to_string(),
                    surface: "web".to_string(),
                    capability: PresenceCapability::Controller,
                    user_id: Some("user-a".to_string()),
                    last_seen_at: Utc::now(),
                    last_event_sequence: Some(42),
                },
            )]),
        );
        presence.insert(
            foreign_session.clone(),
            HashMap::from([(
                "client-2".to_string(),
                SessionPresenceRecord {
                    client_id: "client-2".to_string(),
                    surface: "web".to_string(),
                    capability: PresenceCapability::Observer,
                    user_id: Some("user-b".to_string()),
                    last_seen_at: Utc::now(),
                    last_event_sequence: Some(7),
                },
            )]),
        );
        drop(presence);

        let Json(response) =
            match get_server_status(State(state), Some(current_user("user-a", &temp_dir))).await {
                Ok(response) => response,
                Err(_) => panic!("status should succeed"),
            };

        assert_eq!(response.active_sessions.len(), 1);
        let session = &response.active_sessions[0];
        assert_eq!(session.id, my_session);
        assert_eq!(session.title, "Mine");
        assert_eq!(session.active_viewers, 1);
        assert_eq!(session.active_controllers, 1);
        assert_eq!(session.stale_clients, 0);
    }

    #[tokio::test]
    async fn get_server_status_includes_all_active_sessions_in_local_mode() {
        let (state, temp_dir) = create_test_state();
        let session_manager =
            SessionManager::new(Database::new(&state.db_path).expect("database should open"));

        let first = session_manager
            .create_session("A", None, Some(temp_dir.to_str().expect("utf-8 path")))
            .expect("session should create");
        let second = session_manager
            .create_session("B", None, Some(temp_dir.to_str().expect("utf-8 path")))
            .expect("session should create");
        session_manager
            .set_agent_state(&first, "streaming")
            .expect("agent state should persist");
        session_manager
            .set_agent_state(&second, "awaiting_input")
            .expect("agent state should persist");

        let Json(response) = match get_server_status(State(state), None).await {
            Ok(response) => response,
            Err(_) => panic!("status should succeed"),
        };

        let mut expected = vec![first, second];
        expected.sort();
        let actual = response
            .active_sessions
            .into_iter()
            .map(|session| session.id)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
