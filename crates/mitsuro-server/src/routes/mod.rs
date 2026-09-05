//! API routes

use axum::{middleware, Router};

use crate::{auth, AppState};

mod apns;
pub(crate) mod browser;
mod canvas_files;
mod canvas_git;
pub(crate) mod canvas_presence;
mod canvas_resources;
mod canvases;
pub(crate) mod chat;
mod credentials;
mod extensions;
mod files;
mod git;
mod hive;
mod hooks;
mod mcp;
mod memories;
mod mobile_diagnostics;
mod models;
pub mod oauth;
pub(crate) mod plugins;
mod ports;
mod preview_settings;
mod processes;
mod projects;
mod push;
mod reports;
mod server;
mod session_access;
mod sessions;
mod skills;
mod sse;
mod terminals;
#[cfg(test)]
pub(crate) mod test_support;
mod tools;

/// Build the API router with all endpoints
pub fn api_router() -> Router<AppState> {
    let shared_extensibility = Router::new()
        .nest("/extensions", extensions::router())
        .nest("/mcp", mcp::router())
        .nest("/plugins", plugins::router())
        .nest("/skills", skills::router())
        .route_layer(middleware::from_fn(
            auth::shared_extensibility_admin_middleware,
        ));

    Router::new()
        .nest("/browser", browser::router())
        .nest(
            "/canvases",
            canvases::router()
                .merge(canvas_files::router())
                .merge(canvas_git::router())
                .merge(canvas_resources::router()),
        )
        .nest("/sessions", sessions::router())
        .nest("/chat", chat::router())
        .nest("/models", models::router())
        .nest("/tools", tools::router())
        .nest("/git", git::router())
        .nest("/files", files::router())
        .nest("/credentials", credentials::router())
        .nest("/hive", hive::router())
        .nest(
            crate::legacy_identity::HIVE_API_PREFIX,
            hive::legacy_router(),
        )
        .nest("/memories", memories::router())
        .nest("/mobile-diagnostics", mobile_diagnostics::router())
        .nest("/processes", processes::router())
        .nest("/ports", ports::router())
        .nest("/projects", projects::router())
        .nest("/settings/preview", preview_settings::router())
        .nest("/hooks", hooks::router())
        .nest("/push", push::router())
        .nest("/apns", apns::router())
        .nest("/reports", reports::router())
        .nest("/server", server::router())
        .nest("/terminals", terminals::router())
        .nest("/auth/oauth", oauth::router())
        .merge(shared_extensibility)
}

/// Minimal HTTP surface for disposable provider/orchestration evaluation.
///
/// Global credentials are intentionally resolved by `AppState`, but mutation
/// surfaces for credentials, plugins, hooks, MCP, push, Hive, remote access,
/// and server settings are absent from this router.
pub(crate) fn evaluation_api_router() -> Router<AppState> {
    Router::new()
        .nest("/sessions", sessions::router())
        .nest("/chat", chat::router())
        .nest("/models", models::router())
        .nest("/tools", tools::router())
        .nest("/processes", processes::router())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HiveAcceptanceRouteSurface {
    SessionsAndTraces,
    Chat,
    Models,
    CredentialsReadOnly,
    Hive,
}

const HIVE_ACCEPTANCE_ROUTE_SURFACES: [HiveAcceptanceRouteSurface; 5] = [
    HiveAcceptanceRouteSurface::SessionsAndTraces,
    HiveAcceptanceRouteSurface::Chat,
    HiveAcceptanceRouteSurface::Models,
    HiveAcceptanceRouteSurface::CredentialsReadOnly,
    HiveAcceptanceRouteSurface::Hive,
];

/// Narrow HTTP surface for disposable, Hive-enabled live acceptance.
///
/// The route manifest is executable: adding a surface requires an explicit
/// enum variant and test update. In particular, tools, processes, push,
/// credential mutation, OAuth, MCP, plugins, hooks, and server settings are
/// absent.
pub(crate) fn hive_acceptance_api_router() -> Router<AppState> {
    HIVE_ACCEPTANCE_ROUTE_SURFACES.into_iter().fold(
        Router::new(),
        |router, surface| match surface {
            HiveAcceptanceRouteSurface::SessionsAndTraces => {
                router.nest("/sessions", sessions::router())
            }
            HiveAcceptanceRouteSurface::Chat => router.nest("/chat", chat::router()),
            HiveAcceptanceRouteSurface::Models => router.nest("/models", models::router()),
            HiveAcceptanceRouteSurface::CredentialsReadOnly => {
                router.nest("/credentials", credentials::read_only_router())
            }
            HiveAcceptanceRouteSurface::Hive => router.nest("/hive", hive::router()),
        },
    )
}

#[cfg(test)]
mod acceptance_tests {
    use super::{HiveAcceptanceRouteSurface, HIVE_ACCEPTANCE_ROUTE_SURFACES};

    #[test]
    fn hive_acceptance_route_manifest_is_exact_and_minimal() {
        assert_eq!(
            HIVE_ACCEPTANCE_ROUTE_SURFACES,
            [
                HiveAcceptanceRouteSurface::SessionsAndTraces,
                HiveAcceptanceRouteSurface::Chat,
                HiveAcceptanceRouteSurface::Models,
                HiveAcceptanceRouteSurface::CredentialsReadOnly,
                HiveAcceptanceRouteSurface::Hive,
            ]
        );
    }
}
