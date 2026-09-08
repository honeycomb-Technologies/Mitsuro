use std::sync::OnceLock;
use std::time::Duration;

use axum::{
    body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Request, State,
    },
    http::{header, HeaderMap, HeaderName, Method, Response, Uri},
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use reqwest::redirect::Policy;
use tokio_tungstenite::{connect_async, tungstenite::Message as UpstreamMessage};

use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::AppState;

use super::super::preview_settings::load_preview_settings;
use super::probe::probe_port_previewability;

const MAX_PROXY_REQUEST_BODY_BYTES: usize = 8 * 1024 * 1024;
const PREVIEW_PROXY_SANDBOX_CSP: &str =
    "sandbox allow-downloads allow-forms allow-modals allow-popups allow-pointer-lock allow-scripts";

pub(super) async fn proxy_root(
    State(state): State<AppState>,
    Path(port): Path<u16>,
    user: Option<CurrentUser>,
    ws: Option<WebSocketUpgrade>,
    method: Method,
    uri: Uri,
    request: Request,
) -> Result<Response<axum::body::Body>, AppError> {
    proxy_request(ProxyRequest {
        state,
        user,
        port,
        path: None,
        ws,
        method,
        uri,
        request,
    })
    .await
}

pub(super) async fn proxy_path(
    State(state): State<AppState>,
    Path((port, path)): Path<(u16, String)>,
    user: Option<CurrentUser>,
    ws: Option<WebSocketUpgrade>,
    method: Method,
    uri: Uri,
    request: Request,
) -> Result<Response<axum::body::Body>, AppError> {
    proxy_request(ProxyRequest {
        state,
        user,
        port,
        path: Some(path),
        ws,
        method,
        uri,
        request,
    })
    .await
}

struct ProxyRequest {
    state: AppState,
    user: Option<CurrentUser>,
    port: u16,
    path: Option<String>,
    ws: Option<WebSocketUpgrade>,
    method: Method,
    uri: Uri,
    request: Request,
}

async fn proxy_request(input: ProxyRequest) -> Result<Response<axum::body::Body>, AppError> {
    let ProxyRequest {
        state,
        user,
        port,
        path,
        ws,
        method,
        uri,
        request,
    } = input;

    let settings = load_preview_settings(&state, user.as_ref())?;
    if !settings.enabled {
        return Err(AppError::BadRequest(
            "Preview forwarding is disabled in settings".to_string(),
        ));
    }
    if port == state.server_port {
        return Err(AppError::BadRequest(
            "Refusing to proxy the Mitsuro server port".to_string(),
        ));
    }
    if settings.is_blocked(port) {
        return Err(AppError::BadRequest(format!(
            "Port {} is blocked by preview settings",
            port
        )));
    }
    if !settings.allow_force_open_non_http {
        let probe = probe_port_previewability(port, settings.probe_timeout_ms).await;
        if !probe.is_previewable_http {
            return Err(AppError::BadRequest(format!(
                "Port {} did not pass the HTTP preview probe; enable non-HTTP embed to force open it",
                port
            )));
        }
    }

    let upstream_path = build_upstream_path(path.as_deref(), uri.query());
    let upstream_http_url = format!("http://127.0.0.1:{}{}", port, upstream_path);

    let request_headers = request.headers().clone();
    let wants_ws = method == Method::GET && is_websocket_upgrade(&request_headers);
    if wants_ws {
        let Some(ws) = ws else {
            return Err(AppError::BadRequest(
                "WebSocket upgrade requested but upgrade failed".to_string(),
            ));
        };
        let guard = crate::device_access::DeviceConnectionGuard::new(
            &state,
            request
                .extensions()
                .get::<crate::device_access::DeviceIdentity>()
                .cloned(),
        );
        let upstream_ws_url = format!("ws://127.0.0.1:{}{}", port, upstream_path);
        return Ok(ws
            .on_upgrade(move |socket| guard.run(proxy_websocket(socket, upstream_ws_url)))
            .into_response());
    }

    proxy_http_request(method, upstream_http_url, request_headers, request).await
}

async fn proxy_http_request(
    method: Method,
    upstream_url: String,
    request_headers: HeaderMap,
    request: Request,
) -> Result<Response<axum::body::Body>, AppError> {
    let body_bytes = body::to_bytes(request.into_body(), MAX_PROXY_REQUEST_BODY_BYTES)
        .await
        .map_err(|e| AppError::BadRequest(format!("Request body too large: {}", e)))?;

    let mut upstream = proxy_http_client()
        .request(method, &upstream_url)
        .body(body_bytes);

    for (name, value) in &request_headers {
        if should_forward_request_header(name) {
            upstream = upstream.header(name, value);
        }
    }

    if let Some(host) = request_headers.get(header::HOST) {
        upstream = upstream.header("x-forwarded-host", host.clone());
    }
    upstream = upstream
        .header("x-forwarded-proto", "http")
        .header("x-forwarded-for", "127.0.0.1");

    let upstream_response = upstream.send().await.map_err(|e| {
        AppError::BadGateway(format!(
            "Failed to reach upstream on {}: {}",
            upstream_url, e
        ))
    })?;

    let status = upstream_response.status();
    let response_headers = upstream_response.headers().clone();
    let response_body = upstream_response.bytes().await.map_err(|e| {
        AppError::BadGateway(format!("Failed reading upstream response body: {}", e))
    })?;

    let mut response_builder = Response::builder().status(status);
    for (name, value) in &response_headers {
        if should_forward_response_header(name) {
            response_builder = response_builder.header(name, value);
        }
    }
    response_builder = apply_preview_proxy_security_headers(response_builder);

    response_builder
        .body(axum::body::Body::from(response_body))
        .map_err(|e| AppError::Internal(format!("Failed to build proxy response: {}", e)))
}

async fn proxy_websocket(client_socket: WebSocket, upstream_url: String) {
    let upstream = connect_async(&upstream_url).await;
    let (upstream_socket, _) = match upstream {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(
                "Failed to connect upstream websocket for preview proxy ({}): {}",
                upstream_url,
                err
            );
            return;
        }
    };

    let (mut client_tx, mut client_rx) = client_socket.split();
    let (mut upstream_tx, mut upstream_rx) = upstream_socket.split();

    let client_to_upstream = async {
        while let Some(msg) = client_rx.next().await {
            let Ok(msg) = msg else {
                break;
            };
            match msg {
                Message::Text(text) => {
                    if upstream_tx
                        .send(UpstreamMessage::Text(text.to_string()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Message::Binary(binary) => {
                    if upstream_tx
                        .send(UpstreamMessage::Binary(binary.to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Message::Ping(data) => {
                    if upstream_tx
                        .send(UpstreamMessage::Ping(data.to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Message::Pong(data) => {
                    if upstream_tx
                        .send(UpstreamMessage::Pong(data.to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Message::Close(_) => {
                    let _ = upstream_tx.send(UpstreamMessage::Close(None)).await;
                    break;
                }
            }
        }
    };

    let upstream_to_client = async {
        while let Some(msg) = upstream_rx.next().await {
            let Ok(msg) = msg else {
                break;
            };
            match msg {
                UpstreamMessage::Text(text) => {
                    if client_tx
                        .send(Message::Text(text.to_string()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                UpstreamMessage::Binary(data) => {
                    if client_tx.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                UpstreamMessage::Ping(data) => {
                    if client_tx.send(Message::Ping(data)).await.is_err() {
                        break;
                    }
                }
                UpstreamMessage::Pong(data) => {
                    if client_tx.send(Message::Pong(data)).await.is_err() {
                        break;
                    }
                }
                UpstreamMessage::Close(_) => {
                    let _ = client_tx.send(Message::Close(None)).await;
                    break;
                }
                UpstreamMessage::Frame(_) => {}
            }
        }
    };

    tokio::select! {
        _ = client_to_upstream => {}
        _ = upstream_to_client => {}
    }
}

fn proxy_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(20))
            .build()
            .expect("proxy client should initialize")
    })
}

fn build_upstream_path(path: Option<&str>, query: Option<&str>) -> String {
    let mut full = String::new();
    match path {
        Some(path) if !path.is_empty() => {
            full.push('/');
            full.push_str(path);
        }
        _ => full.push('/'),
    }
    if let Some(query) = query {
        full.push('?');
        full.push_str(query);
    }
    full
}

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let has_upgrade = headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);

    let has_connection_upgrade = headers
        .get(header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("upgrade"))
        })
        .unwrap_or(false);

    has_upgrade && has_connection_upgrade
}

fn apply_preview_proxy_security_headers(
    response_builder: axum::http::response::Builder,
) -> axum::http::response::Builder {
    response_builder
        .header(header::CONTENT_SECURITY_POLICY, PREVIEW_PROXY_SANDBOX_CSP)
        .header(header::REFERRER_POLICY, "no-referrer")
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
}

fn should_forward_request_header(name: &HeaderName) -> bool {
    !is_hop_by_hop_header(name) && *name != header::HOST
}

fn should_forward_response_header(name: &HeaderName) -> bool {
    !is_hop_by_hop_header(name)
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "proxy-connection"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::preview_settings::PreviewSettings;

    #[test]
    fn build_upstream_path_handles_root_and_query() {
        assert_eq!(build_upstream_path(None, None), "/");
        assert_eq!(build_upstream_path(Some("foo/bar"), None), "/foo/bar");
        assert_eq!(
            build_upstream_path(Some("foo"), Some("a=1&b=2")),
            "/foo?a=1&b=2"
        );
    }

    #[test]
    fn preview_proxy_security_headers_sandbox_without_same_origin() {
        let response = apply_preview_proxy_security_headers(Response::builder())
            .body(axum::body::Body::empty())
            .expect("response should build");

        let csp = response
            .headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .and_then(|value| value.to_str().ok())
            .expect("content security policy should be present");
        assert!(csp.starts_with("sandbox"));
        assert!(csp.contains("allow-scripts"));
        assert!(!csp.contains("allow-same-origin"));
        assert_eq!(
            response.headers().get(header::REFERRER_POLICY).unwrap(),
            "no-referrer"
        );
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "*"
        );
    }

    #[test]
    fn default_preview_settings_block_sensitive_admin_ports() {
        let settings = PreviewSettings::default();

        assert!(settings.is_blocked(22));
        assert!(settings.is_blocked(2375));
        assert!(settings.is_blocked(2376));
        assert!(settings.is_blocked(6443));
        assert!(settings.is_blocked(10250));
        assert!(!settings.is_blocked(3000));
    }
}
