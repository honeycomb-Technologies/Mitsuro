//! Server lifecycle and admission policy for device credentials. Durable
//! enrollment and credential decisions are owned by the shared core store.

use std::{
    future::Future,
    sync::Mutex,
    time::{Duration, Instant},
};

use axum::{body::Body, http::StatusCode, response::Response};
use futures::StreamExt;
use mitsuro_core::storage::{
    device_access::{DeviceAccessError, DeviceAccessStore},
    Database,
};

use crate::AppState;

pub(crate) type AccessRejection = (StatusCode, &'static str);

#[derive(Default)]
pub(crate) struct DeviceAccessRuntime {
    store: Mutex<Option<DeviceAccessStore>>,
    admission: Mutex<AdmissionWindow>,
}

struct AdmissionWindow {
    start: Instant,
    claims: u32,
    polls: u32,
}

impl Default for AdmissionWindow {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            claims: 0,
            polls: 0,
        }
    }
}

impl DeviceAccessRuntime {
    pub(crate) fn with_store<T>(
        &self,
        path: &std::path::Path,
        f: impl FnOnce(&mut DeviceAccessStore) -> Result<T, DeviceAccessError>,
    ) -> Result<T, AccessRejection> {
        let mut store = self.store.lock().map_err(|_| unavailable())?;
        if store.is_none() {
            let db = Database::new(path).map_err(|_| unavailable())?;
            *store = Some(DeviceAccessStore::new(db));
        }
        f(store.as_mut().ok_or_else(unavailable)?).map_err(|error| match error {
            DeviceAccessError::InvalidInvitation | DeviceAccessError::InvalidClaim => (
                StatusCode::GONE,
                "Pairing request is invalid, expired, or already used",
            ),
            DeviceAccessError::InvalidName => (
                StatusCode::BAD_REQUEST,
                "Enter a device name of 1 to 80 printable characters",
            ),
            DeviceAccessError::Limit => (
                StatusCode::TOO_MANY_REQUESTS,
                "Device or invitation limit reached",
            ),
            DeviceAccessError::Random | DeviceAccessError::Storage(_) => unavailable(),
        })
    }

    /// Bound unauthenticated work globally. Peer addresses behind a trusted
    /// proxy are not independent identities and must not bypass this limit.
    pub(crate) fn admit(&self, claim: bool) -> Result<(), AccessRejection> {
        let mut window = self.admission.lock().map_err(|_| unavailable())?;
        if window.start.elapsed() >= Duration::from_secs(60) {
            *window = AdmissionWindow::default();
        }
        let (count, max) = if claim {
            (&mut window.claims, 30)
        } else {
            (&mut window.polls, 300)
        };
        if *count >= max {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "Pairing rate limit reached; wait one minute",
            ));
        }
        *count += 1;
        Ok(())
    }
}

fn unavailable() -> AccessRejection {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "Device access is temporarily unavailable",
    )
}

pub(crate) fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

#[derive(Clone)]
pub(crate) struct DeviceIdentity(pub String);

/// A per-connection check shared by SSE and upgraded sockets. Revocation,
/// expiration, disabled remote access, and storage failures all close access.
/// The interval lives across stream reads, so a busy stream cannot reset it.
pub(crate) struct DeviceConnectionGuard {
    identity: Option<DeviceIdentity>,
    state: AppState,
    interval: tokio::time::Interval,
}

impl DeviceConnectionGuard {
    pub(crate) fn new(state: &AppState, identity: Option<DeviceIdentity>) -> Self {
        Self {
            identity,
            state: state.clone(),
            interval: tokio::time::interval(Duration::from_secs(5)),
        }
    }

    pub(crate) async fn revoked(&mut self) {
        let Some(identity) = &self.identity else {
            return std::future::pending::<()>().await;
        };
        loop {
            self.interval.tick().await;
            if !self.state.remote_access.read().await.enabled
                || !self
                    .state
                    .device_access
                    .with_store(&self.state.db_path, |store| {
                        store.active(&identity.0, now())
                    })
                    .unwrap_or(false)
            {
                return;
            }
        }
    }

    pub(crate) async fn run(mut self, future: impl Future<Output = ()>) {
        tokio::select! {
            biased;
            () = self.revoked() => {},
            () = future => {},
        }
    }
}

pub(crate) fn guard_sse(
    state: &AppState,
    identity: DeviceIdentity,
    response: Response,
) -> Response {
    if !response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"text/event-stream"))
    {
        return response;
    }
    let (parts, body) = response.into_parts();
    let guard = DeviceConnectionGuard::new(state, Some(identity));
    let stream = futures::stream::unfold(
        (body.into_data_stream(), guard),
        |(mut body, mut guard)| async move {
            tokio::select! {
                biased;
                () = guard.revoked() => None,
                chunk = body.next() => chunk.map(|chunk| (chunk, (body, guard))),
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}
