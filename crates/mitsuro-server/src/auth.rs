//! Lightweight auth middleware for self-host deployments.
//!
//! This keeps request-level user context optional while preserving a hard trust
//! boundary between loopback development and remote bearer access:
//! - No identity headers => single-tenant mode.
//! - Loopback-only `X-User-Id` + optional `X-Workspace-Dir` => scoped development mode.
//! - A remote shared bearer token authenticates the server's single tenant and
//!   never delegates an arbitrary user identity supplied by the caller.

use axum::{
    async_trait,
    extract::{ConnectInfo, FromRequestParts, Request, State},
    http::{header, request::Parts, HeaderMap, StatusCode, Uri},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::utils::workspace::resolve_scoped_workspace_path;
use crate::AppState;

const USER_ID_HEADER: &str = "X-User-Id";
const WORKSPACE_DIR_HEADER: &str = "X-Workspace-Dir";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestSurface {
    Loopback,
    Remote,
}

/// User context attached to request extensions by middleware.
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub user_id: Option<String>,
    pub home_dir: Option<PathBuf>,
}

impl AuthenticatedUser {
    pub fn local() -> Self {
        Self {
            user_id: None,
            home_dir: None,
        }
    }
}

/// Extractor for routes that want user context.
pub struct CurrentUser(pub AuthenticatedUser);

#[async_trait]
impl<S> FromRequestParts<S> for CurrentUser
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthenticatedUser>()
            .cloned()
            .map(CurrentUser)
            .ok_or((StatusCode::UNAUTHORIZED, "Not authenticated"))
    }
}

/// Guard for process-wide extensibility managers that are not tenant scoped.
///
/// Local requests remain the single-tenant administrator. A request carrying a
/// user identity must fail closed until extension, skill, MCP/OAuth, and plugin
/// managers are keyed by that identity and workspace.
pub async fn shared_extensibility_admin_middleware(request: Request, next: Next) -> Response {
    match authorize_shared_extensibility_admin(request.extensions().get::<AuthenticatedUser>()) {
        Ok(()) => next.run(request).await,
        Err(rejection) => rejection.into_response(),
    }
}

fn authorize_shared_extensibility_admin(
    user: Option<&AuthenticatedUser>,
) -> Result<(), (StatusCode, &'static str)> {
    match user {
        Some(user) if user.user_id.is_none() => Ok(()),
        Some(_) => Err((
            StatusCode::FORBIDDEN,
            "Shared extensibility administration is unavailable in tenant-scoped mode",
        )),
        None => Err((StatusCode::UNAUTHORIZED, "Not authenticated")),
    }
}

/// Middleware that attaches optional user info to request extensions.
pub async fn auth_middleware(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    mut request: Request,
    next: Next,
) -> Response {
    let supplied_token = bearer_token(request.headers())
        .map(str::to_owned)
        .or_else(|| websocket_query_token(request.headers(), request.uri()));
    if let Some(token) = supplied_token.as_deref().filter(|value| {
        value.starts_with(mitsuro_core::storage::device_access::DEVICE_TOKEN_PREFIX)
            || value.starts_with(mitsuro_core::storage::device_access::STREAM_TICKET_PREFIX)
    }) {
        // A supplied device credential must never fall through to implicit
        // loopback administrator access, even when invalid or revoked.
        if !state.remote_access.read().await.enabled {
            return (StatusCode::FORBIDDEN, "Remote API access is disabled").into_response();
        }
        if request.headers().contains_key(USER_ID_HEADER)
            || request.headers().contains_key(WORKSPACE_DIR_HEADER)
        {
            return (
                StatusCode::FORBIDDEN,
                "Device credentials cannot select a user or workspace",
            )
                .into_response();
        }
        if addr.ip().is_loopback()
            && request
                .headers()
                .get(header::HOST)
                .and_then(|h| h.to_str().ok())
                .is_some_and(is_local_host)
        {
            if let Err(rejection) = authorize_local_browser_origin(request.headers()) {
                return rejection.into_response();
            }
        }
        let device_id = match state.device_access.with_store(&state.db_path, |s| {
            if token.starts_with(mitsuro_core::storage::device_access::STREAM_TICKET_PREFIX) {
                if websocket_query_token(request.headers(), request.uri()).is_none() {
                    return Ok(None);
                }
                s.consume_stream_ticket(token, request.uri().path(), crate::device_access::now())
            } else if bearer_token(request.headers()).is_some() {
                s.authenticate(token, crate::device_access::now())
            } else {
                Ok(None)
            }
        }) {
            Ok(Some(id)) => id,
            Ok(None) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    "Device credential is invalid or revoked",
                )
                    .into_response()
            }
            Err(rejection) => return rejection.into_response(),
        };
        if !device_request_allowed(request.method(), request.uri().path()) {
            return (
                StatusCode::FORBIDDEN,
                "This action requires the server owner",
            )
                .into_response();
        }
        let identity = crate::device_access::DeviceIdentity(device_id);
        request.extensions_mut().insert(identity.clone());
        request.extensions_mut().insert(AuthenticatedUser::local());
        let response = next.run(request).await;
        return crate::device_access::guard_sse(&state, identity, response);
    }
    let surface =
        match authorize_request_surface(&state, addr, request.headers(), request.uri()).await {
            Ok(surface) => surface,
            Err(rejection) => {
                tracing::warn!(
                    remote_ip = %addr.ip(),
                    "Rejected API request at the transport authorization boundary"
                );
                return rejection.into_response();
            }
        };

    let user = match authenticated_user_for_surface(&state, surface, request.headers()) {
        Ok(user) => user,
        Err(rejection) => {
            tracing::warn!(
                remote_ip = %addr.ip(),
                "Rejected unbound request identity"
            );
            return rejection.into_response();
        }
    };

    request.extensions_mut().insert(user);
    next.run(request).await
}

fn device_request_allowed(method: &axum::http::Method, path: &str) -> bool {
    let under = |prefix: &str| {
        path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    };
    if under("/api/device-pairing") {
        return false;
    }
    if method != axum::http::Method::GET
        && (under("/api/server/access")
            || under("/api/credentials")
            || under("/api/auth/oauth")
            || under("/api/extensions")
            || under("/api/mcp")
            || under("/api/plugins")
            || under("/api/skills")
            || under("/api/hooks"))
    {
        return false;
    }
    true
}

async fn authorize_request_surface(
    state: &AppState,
    addr: SocketAddr,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<RequestSurface, (StatusCode, &'static str)> {
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if addr.ip().is_loopback() && is_local_host(host) {
        authorize_local_browser_origin(headers).map(|()| RequestSurface::Loopback)
    } else {
        authorize_remote_request(state, headers, uri)
            .await
            .map(|()| RequestSurface::Remote)
    }
}

fn authenticated_user_for_surface(
    state: &AppState,
    surface: RequestSurface,
    headers: &HeaderMap,
) -> Result<AuthenticatedUser, (StatusCode, &'static str)> {
    // The remote token is one server-wide capability, not a user credential.
    // Letting its holder choose a principal would make every ownership check
    // below this boundary caller-controlled. Reject both identity-bearing
    // headers even when they are empty or malformed so the policy fails closed.
    if surface == RequestSurface::Remote {
        if headers.contains_key(USER_ID_HEADER) || headers.contains_key(WORKSPACE_DIR_HEADER) {
            return Err((
                StatusCode::FORBIDDEN,
                "Remote bearer access does not authorize user or workspace identity headers",
            ));
        }
        return Ok(AuthenticatedUser::local());
    }

    let mut user = AuthenticatedUser::local();
    if let Some(user_id) = headers
        .get(USER_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
    {
        user.user_id = Some(user_id.to_string());
        let requested_workspace = headers
            .get(WORKSPACE_DIR_HEADER)
            .and_then(|value| value.to_str().ok());
        user.home_dir = Some(resolve_workspace_dir(
            &state.working_dir,
            requested_workspace,
        )?);
    }
    Ok(user)
}

fn authorize_local_browser_origin(headers: &HeaderMap) -> Result<(), (StatusCode, &'static str)> {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(());
    };

    if is_trusted_local_origin(origin) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            "Loopback API requests must come from a trusted local origin",
        ))
    }
}

async fn authorize_remote_request(
    state: &AppState,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<(), (StatusCode, &'static str)> {
    let remote_access = state.remote_access.read().await.clone();
    if !remote_access.enabled {
        return Err((
            StatusCode::FORBIDDEN,
            "Remote API access is disabled for this server",
        ));
    }

    let provided_token = bearer_token(headers)
        .map(str::to_owned)
        .or_else(|| websocket_query_token(headers, uri));

    let Some(provided_token) = provided_token else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Remote API access requires a valid bearer token",
        ));
    };

    if provided_token == remote_access.token {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            "Remote API access requires a valid bearer token",
        ))
    }
}

/// Extract a remote-access token from the WebSocket upgrade query string.
///
/// Browser WebSocket clients cannot set arbitrary Authorization headers, so the
/// terminal UI passes `?token=` (URL-encoded). Values are percent-decoded before
/// comparison so tokens containing `+`, `/`, `=`, etc. survive `encodeURIComponent`.
///
/// Only honored on WebSocket upgrades — never on ordinary HTTP requests — to
/// reduce accidental leakage of tokens via shared query-parameter auth.
fn websocket_query_token(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    if !is_websocket_upgrade(headers) {
        return None;
    }

    uri.query()?.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        if key != "token" {
            return None;
        }
        let decoded = percent_decode_query_component(value)?;
        let trimmed = decoded.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

/// Percent-decode a single application/x-www-form-urlencoded query component.
///
/// Handles `%XX` escapes and `+` as space (common form encoding). Returns `None`
/// on malformed sequences so we never accept a half-decoded token.
fn percent_decode_query_component(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return None;
                }
                let hi = (bytes[i + 1] as char).to_digit(16)?;
                let lo = (bytes[i + 2] as char).to_digit(16)?;
                out.push(((hi << 4) | lo) as u8);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let upgrade = headers
        .get(header::UPGRADE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
    let connection = headers
        .get(header::CONNECTION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
        });

    upgrade && connection
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub(crate) fn is_local_host(host: &str) -> bool {
    let host = host.trim().trim_end_matches('.');
    if host.is_empty() {
        return true;
    }

    let host = extract_host_authority(host).to_ascii_lowercase();

    matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1")
}

pub(crate) fn is_trusted_local_origin(origin: &str) -> bool {
    let origin = origin.trim().trim_end_matches('/');
    if origin.is_empty() || origin.eq_ignore_ascii_case("null") {
        return false;
    }

    let Some((_, remainder)) = origin.split_once("://") else {
        return false;
    };

    let authority = remainder.split('/').next().unwrap_or_default();
    let host = extract_host_authority(authority).to_ascii_lowercase();

    is_local_host(&host) || host.ends_with(".localhost")
}

fn extract_host_authority(host: &str) -> &str {
    if let Some(stripped) = host.strip_prefix('[') {
        return stripped
            .split_once(']')
            .map(|(inner, _)| inner)
            .unwrap_or(host);
    }

    match host.rsplit_once(':') {
        Some((candidate, port))
            if !candidate.contains(':') && port.chars().all(|c| c.is_ascii_digit()) =>
        {
            candidate
        }
        _ => host,
    }
}

fn resolve_workspace_dir(
    server_root: &Path,
    requested: Option<&str>,
) -> Result<PathBuf, (StatusCode, &'static str)> {
    resolve_scoped_workspace_path(requested, server_root, server_root).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Workspace directory must stay within the configured server root",
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{atomic::AtomicUsize, Arc};

    use axum::http::{header, HeaderMap, HeaderValue, Uri};
    use tokio::sync::{Mutex, RwLock};

    use mitsuro_core::agent::{AgentCancellation, UserHookManager};
    use mitsuro_core::ai::models::create_model_registry;
    use mitsuro_core::mcp::McpManager;
    use mitsuro_core::process::ProcessRegistry;
    use mitsuro_core::skills::SkillsManager;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::tools::registry::ToolRegistry;

    use super::{
        authenticated_user_for_surface, authorize_request_surface,
        authorize_shared_extensibility_admin, is_local_host, is_trusted_local_origin,
        percent_decode_query_component, resolve_workspace_dir, AuthenticatedUser, RequestSurface,
        USER_ID_HEADER, WORKSPACE_DIR_HEADER,
    };
    use crate::{remote_access::RemoteAccessConfig, AppState};

    #[test]
    fn is_local_host_accepts_loopback_names() {
        assert!(is_local_host("localhost:3000"));
        assert!(is_local_host("127.0.0.1:3000"));
        assert!(is_local_host("[::1]:3000"));
        assert!(is_local_host("::1"));
        assert!(!is_local_host("mitsuro.example.ts.net"));
    }

    #[test]
    fn is_trusted_local_origin_accepts_first_party_local_surfaces() {
        assert!(is_trusted_local_origin("http://localhost:5173"));
        assert!(is_trusted_local_origin("http://127.0.0.1:3000"));
        assert!(is_trusted_local_origin("tauri://localhost"));
        assert!(is_trusted_local_origin("https://tauri.localhost"));
        assert!(!is_trusted_local_origin("https://evil.example"));
        assert!(!is_trusted_local_origin("null"));
    }

    #[test]
    fn shared_extensibility_admin_is_local_only_and_fails_closed() {
        assert!(authorize_shared_extensibility_admin(Some(&AuthenticatedUser::local())).is_ok());

        let tenant = AuthenticatedUser {
            user_id: Some("tenant-a".to_string()),
            home_dir: None,
        };
        let rejection = authorize_shared_extensibility_admin(Some(&tenant))
            .expect_err("tenant-scoped requests must not use shared managers");
        assert_eq!(rejection.0, axum::http::StatusCode::FORBIDDEN);

        let missing = authorize_shared_extensibility_admin(None)
            .expect_err("missing auth context must fail closed");
        assert_eq!(missing.0, axum::http::StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn resolve_workspace_dir_rejects_paths_outside_server_root() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitsuro-auth-test-{}", uuid::Uuid::new_v4()));
        let server_root = temp_dir.join("workspace");
        let outside = temp_dir.join("outside");
        std::fs::create_dir_all(&server_root).expect("server root should exist");
        std::fs::create_dir_all(&outside).expect("outside dir should exist");

        let result = resolve_workspace_dir(&server_root, Some(outside.to_string_lossy().as_ref()));
        assert!(result.is_err());
    }

    #[test]
    fn resolve_workspace_dir_allows_relative_paths_within_server_root() {
        let temp_dir =
            std::env::temp_dir().join(format!("mitsuro-auth-test-{}", uuid::Uuid::new_v4()));
        let server_root = temp_dir.join("workspace");
        let project_dir = server_root.join("project");
        std::fs::create_dir_all(&project_dir).expect("project dir should exist");

        let result = resolve_workspace_dir(&server_root, Some("project"))
            .expect("relative project dir should resolve");
        assert_eq!(result, project_dir);
    }

    pub(super) fn test_state() -> AppState {
        let temp_dir =
            std::env::temp_dir().join(format!("mitsuro-auth-test-{}", uuid::Uuid::new_v4()));
        let db_path = temp_dir.join("test.db");
        std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
        mitsuro_core::storage::Database::new(&db_path).expect("database should initialize");

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
            agent_streams: Arc::new(RwLock::new(crate::agent_streams::AgentStreamHub::default())),
            delegated_state: Arc::new(RwLock::new(HashMap::new())),
            device_access: Default::default(),
            remote_access: Arc::new(RwLock::new(RemoteAccessConfig {
                enabled: true,
                token: "secret".to_string(),
            })),
            active_agent_streams: Arc::new(AtomicUsize::new(0)),
            peak_rss_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peak_virtual_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            push_service: None,
            apns_service: None,
            oauth_flows: Arc::new(Mutex::new(HashMap::new())),
            hive_runtime: crate::hive_runtime::HiveRuntimeManager::new(),
        }
    }

    #[tokio::test]
    async fn authorize_request_surface_rejects_remote_without_token() {
        let state = test_state();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), 3000);
        let headers = HeaderMap::new();

        let uri = Uri::from_static("/api/sessions");
        let result = authorize_request_surface(&state, addr, &headers, &uri).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn authorize_request_surface_accepts_remote_with_valid_token() {
        let state = test_state();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), 3000);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );

        let uri = Uri::from_static("/api/sessions");
        let surface = authorize_request_surface(&state, addr, &headers, &uri)
            .await
            .expect("valid remote bearer should authorize the request surface");
        assert_eq!(surface, RequestSurface::Remote);
        let user = authenticated_user_for_surface(&state, surface, &headers)
            .expect("remote bearer without identity headers should remain valid");
        assert!(user.user_id.is_none());
        assert!(user.home_dir.is_none());
    }

    #[tokio::test]
    async fn remote_bearer_rejects_caller_supplied_identity_headers() {
        let state = test_state();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), 3000);
        let uri = Uri::from_static("/api/hive/sessions/victim/status");

        for (header_name, value) in [
            (USER_ID_HEADER, "victim"),
            (WORKSPACE_DIR_HEADER, "victim-workspace"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_static("Bearer secret"),
            );
            headers.insert(
                header_name.parse::<header::HeaderName>().unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );

            let surface = authorize_request_surface(&state, addr, &headers, &uri)
                .await
                .expect("the bearer itself is valid");
            let rejection = authenticated_user_for_surface(&state, surface, &headers)
                .expect_err("an unbound remote identity must be rejected");
            assert_eq!(rejection.0, axum::http::StatusCode::FORBIDDEN);
        }
    }

    #[tokio::test]
    async fn loopback_scoped_identity_remains_available_for_local_development() {
        let state = test_state();
        let project_dir = state.working_dir.join("project");
        std::fs::create_dir_all(&project_dir).expect("project directory should exist");
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost:3000"));
        headers.insert(USER_ID_HEADER, HeaderValue::from_static("local-user"));
        headers.insert(WORKSPACE_DIR_HEADER, HeaderValue::from_static("project"));

        let surface =
            authorize_request_surface(&state, addr, &headers, &Uri::from_static("/api/sessions"))
                .await
                .expect("loopback request should be authorized");
        assert_eq!(surface, RequestSurface::Loopback);
        let user = authenticated_user_for_surface(&state, surface, &headers)
            .expect("loopback identity should resolve");
        assert_eq!(user.user_id.as_deref(), Some("local-user"));
        assert_eq!(user.home_dir.as_deref(), Some(project_dir.as_path()));
    }

    #[tokio::test]
    async fn authorize_request_surface_accepts_remote_websocket_query_token() {
        let state = test_state();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), 3000);
        let mut headers = HeaderMap::new();
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        let uri = Uri::from_static("/ws/terminal?token=secret");

        let result = authorize_request_surface(&state, addr, &headers, &uri).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn authorize_request_surface_accepts_url_encoded_websocket_query_token() {
        // Token "a+b/c=d" encodes as a%2Bb%2Fc%3Dd via encodeURIComponent.
        let state = {
            let mut state = test_state();
            state.remote_access = Arc::new(RwLock::new(RemoteAccessConfig {
                enabled: true,
                token: "a+b/c=d".to_string(),
            }));
            state
        };
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), 3000);
        let mut headers = HeaderMap::new();
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        let uri = Uri::from_static("/ws/terminal?token=a%2Bb%2Fc%3Dd");

        let result = authorize_request_surface(&state, addr, &headers, &uri).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn percent_decode_query_component_handles_reserved_chars() {
        assert_eq!(
            percent_decode_query_component("a%2Bb%2Fc%3Dd").as_deref(),
            Some("a+b/c=d")
        );
        assert_eq!(
            percent_decode_query_component("hello%20world").as_deref(),
            Some("hello world")
        );
        assert_eq!(
            percent_decode_query_component("plus+space").as_deref(),
            Some("plus space")
        );
        assert!(percent_decode_query_component("%zz").is_none());
        assert!(percent_decode_query_component("%2").is_none());
    }

    #[tokio::test]
    async fn authorize_request_surface_rejects_remote_query_token_without_websocket_upgrade() {
        let state = test_state();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)), 3000);
        let headers = HeaderMap::new();
        let uri = Uri::from_static("/api/sessions?token=secret");

        let result = authorize_request_surface(&state, addr, &headers, &uri).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn authorize_request_surface_rejects_untrusted_loopback_browser_origin() {
        let state = test_state();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost:3000"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://evil.example"),
        );

        let uri = Uri::from_static("/api/sessions");
        let result = authorize_request_surface(&state, addr, &headers, &uri).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn authorize_request_surface_accepts_trusted_loopback_browser_origin() {
        let state = test_state();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost:3000"));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://localhost:5173"),
        );

        let uri = Uri::from_static("/api/sessions");
        let result = authorize_request_surface(&state, addr, &headers, &uri).await;
        assert!(result.is_ok());
    }
}

#[cfg(test)]
mod device_pairing_tests;
