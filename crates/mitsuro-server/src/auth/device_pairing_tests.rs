use axum::{
    body::{to_bytes, Body},
    extract::ConnectInfo,
    http::{Request, StatusCode},
    middleware,
    response::Response,
    routing::{get, patch, post},
    Router,
};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::{tests::test_state, *};

fn router(state: AppState) -> Router {
    let protected = Router::new()
        .nest(
            "/api/device-pairing",
            crate::routes::device_pairing::owner_router(),
        )
        .route(
            "/api/stream-ticket",
            post(crate::routes::device_pairing::stream_ticket),
        )
        .route(
            "/api/device-session",
            get(crate::routes::device_pairing::device_session),
        )
        .route("/api/probe", get(|| async { "authorized" }))
        .route("/api/server/access", patch(|| async { "owner-only" }))
        .route(
            "/api/stream",
            get(|| async {
                let stream = futures::stream::unfold(0u64, |count| async move {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    Some((
                        Ok::<_, std::convert::Infallible>(format!("data: {count}\n\n")),
                        count + 1,
                    ))
                });
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(stream))
                    .unwrap()
            }),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));
    protected
        .merge(crate::routes::device_pairing::public_router())
        .with_state(state)
}

async fn request(
    app: &Router,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Value,
) -> Response {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "remote.example")
        .header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    app.clone()
        .oneshot(
            request
                .extension(ConnectInfo("10.0.0.5:2000".parse::<SocketAddr>().unwrap()))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn json_response(response: Response) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap()
}

async fn claim(app: &Router) -> (String, String) {
    let invitation = json_response(
        request(
            app,
            "POST",
            "/api/device-pairing/invitations",
            Some("secret"),
            json!({"origin":"https://remote.example"}),
        )
        .await,
    )
    .await;
    let claim = json_response(request(app, "POST", "/api/pairing/claim", None, json!({"secret":invitation["secret"],"origin":"https://remote.example","name":"My phone"})).await).await;
    assert!(claim["comparison_code"].as_str().unwrap().contains('-'));
    (
        invitation["id"].as_str().unwrap().to_owned(),
        claim["credential"].as_str().unwrap().to_owned(),
    )
}

#[tokio::test]
async fn pairing_routes_require_owner_confirmation_and_revoke_only_the_chosen_device() {
    let state = test_state();
    let app = router(state);
    let overview = json_response(
        request(
            &app,
            "GET",
            "/api/device-pairing",
            Some("secret"),
            Value::Null,
        )
        .await,
    )
    .await;
    assert_eq!(overview["pairing_version"], 1);
    assert_eq!(
        request(
            &app,
            "POST",
            "/api/device-pairing/invitations",
            None,
            json!({"origin":"https://remote.example"})
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let (id, token) = claim(&app).await;
    assert_eq!(
        request(
            &app,
            "GET",
            "/api/device-session",
            Some(&token),
            Value::Null
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let status = json_response(
        request(
            &app,
            "GET",
            "/api/pairing/status",
            Some(&token),
            Value::Null,
        )
        .await,
    )
    .await;
    assert_eq!(status["status"], "pending");
    assert_eq!(
        request(
            &app,
            "POST",
            &format!("/api/device-pairing/invitations/{id}/approve"),
            Some(&token),
            Value::Null
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    json_response(
        request(
            &app,
            "POST",
            &format!("/api/device-pairing/invitations/{id}/approve"),
            Some("secret"),
            Value::Null,
        )
        .await,
    )
    .await;
    assert_eq!(
        request(
            &app,
            "GET",
            "/api/device-session",
            Some(&token),
            Value::Null
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(
            &app,
            "PATCH",
            "/api/server/access",
            Some(&token),
            json!({"reveal_token":true})
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(
            &app,
            "POST",
            "/api/device-pairing/invitations",
            Some(&token),
            json!({"origin":"https://remote.example"})
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let (other_id, other_token) = claim(&app).await;
    json_response(
        request(
            &app,
            "POST",
            &format!("/api/device-pairing/invitations/{other_id}/approve"),
            Some("secret"),
            Value::Null,
        )
        .await,
    )
    .await;
    json_response(
        request(
            &app,
            "DELETE",
            &format!("/api/device-pairing/devices/{id}"),
            Some("secret"),
            Value::Null,
        )
        .await,
    )
    .await;
    assert_eq!(
        request(
            &app,
            "GET",
            "/api/device-session",
            Some(&token),
            Value::Null
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(
            &app,
            "GET",
            "/api/device-session",
            Some(&other_token),
            Value::Null
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(&app, "GET", "/api/probe", Some("secret"), Value::Null)
            .await
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn device_tokens_never_fall_back_to_loopback_owner_or_caller_selected_identity() {
    let state = test_state();
    let app = router(state);
    let (_, token) = claim(&app).await;
    let request = Request::builder()
        .uri("/api/probe")
        .header("host", "localhost:3000")
        .header("authorization", format!("Bearer {token}"))
        .extension(ConnectInfo("127.0.0.1:2000".parse::<SocketAddr>().unwrap()))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let request = Request::builder()
        .uri("/api/probe")
        .header("host", "remote.example")
        .header("authorization", format!("Bearer {token}"))
        .header("x-user-id", "another-owner")
        .extension(ConnectInfo("10.0.0.5:2000".parse::<SocketAddr>().unwrap()))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.oneshot(request).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn revocation_closes_a_busy_sse_stream_within_the_documented_bound() {
    use futures::StreamExt;
    let state = test_state();
    let app = router(state.clone());
    let (id, token) = claim(&app).await;
    state
        .device_access
        .with_store(&state.db_path, |s| {
            s.approve(&id, crate::device_access::now())
        })
        .unwrap();
    let response = request(&app, "GET", "/api/stream", Some(&token), Value::Null).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    assert!(body.next().await.is_some());
    state
        .device_access
        .with_store(&state.db_path, |s| {
            s.revoke(&id, crate::device_access::now())
        })
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(6), async {
        while body.next().await.is_some() {}
    })
    .await
    .expect("a busy SSE stream must not postpone its revocation check");
}

#[tokio::test]
async fn disabled_remote_access_and_unsafe_pairing_origins_fail_closed() {
    let state = test_state();
    let app = router(state.clone());
    for origin in [
        "http://192.168.1.2:3000",
        "https://user:password@remote.example",
        "https://remote.example/path",
        "https://remote.example/#token",
        "file:///tmp/server",
    ] {
        assert_eq!(
            request(
                &app,
                "POST",
                "/api/device-pairing/invitations",
                Some("secret"),
                json!({"origin":origin})
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }
    let (_, token) = claim(&app).await;
    state.remote_access.write().await.enabled = false;
    assert_eq!(
        request(
            &app,
            "GET",
            "/api/pairing/status",
            Some(&token),
            Value::Null
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        request(&app, "GET", "/api/probe", Some(&token), Value::Null)
            .await
            .status(),
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn websocket_tickets_cannot_be_replayed_or_used_for_another_route() {
    let app = router(test_state());
    let (id, token) = claim(&app).await;
    assert_eq!(
        request(
            &app,
            "POST",
            &format!("/api/device-pairing/invitations/{id}/approve"),
            Some("secret"),
            Value::Null
        )
        .await
        .status(),
        StatusCode::OK
    );
    let ticket = json_response(
        request(
            &app,
            "POST",
            "/api/stream-ticket",
            Some(&token),
            json!({"path":"/api/probe"}),
        )
        .await,
    )
    .await;
    let ticket = ticket["ticket"].as_str().unwrap();
    async fn upgrade(app: &Router, path: &str, token: &str) -> StatusCode {
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("{path}?token={token}"))
                    .header("host", "remote.example")
                    .header("upgrade", "websocket")
                    .header("connection", "upgrade")
                    .extension(ConnectInfo("10.0.0.5:2000".parse::<SocketAddr>().unwrap()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }
    assert_eq!(
        upgrade(&app, "/api/probe", &token).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&app, "GET", "/api/probe", Some(ticket), Value::Null)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(upgrade(&app, "/api/probe", ticket).await, StatusCode::OK);
    assert_eq!(
        upgrade(&app, "/api/probe", ticket).await,
        StatusCode::UNAUTHORIZED
    );
    let ticket = json_response(
        request(
            &app,
            "POST",
            "/api/stream-ticket",
            Some(&token),
            json!({"path":"/api/probe"}),
        )
        .await,
    )
    .await;
    assert_eq!(
        upgrade(
            &app,
            "/api/server/access",
            ticket["ticket"].as_str().unwrap()
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
}

/// Runs real enrollment/auth routes with a disposable database and no execution
/// routes. Intended for the web/mobile acceptance pass, never a production host.
#[tokio::test]
#[ignore = "manual loopback UI acceptance fixture"]
async fn pairing_ui_acceptance_server() {
    let app = router(test_state())
        .route(
            "/health",
            get(|| async {
                axum::Json(json!({"status":"ok","version":"0.10.1","api_protocol":1}))
            }),
        )
        .layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(
                    "http://localhost:5187"
                        .parse::<axum::http::HeaderValue>()
                        .unwrap(),
                )
                .allow_methods([
                    axum::http::Method::GET,
                    axum::http::Method::POST,
                    axum::http::Method::DELETE,
                ])
                .allow_headers([
                    axum::http::header::AUTHORIZATION,
                    axum::http::header::CONTENT_TYPE,
                    axum::http::HeaderName::from_static("x-mitsuro-wire-version"),
                ]),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3107")
        .await
        .unwrap();
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}
