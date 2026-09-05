use std::convert::Infallible;
use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    response::sse::{Event, KeepAlive, Sse},
};
use mitsuro_core::storage::SessionType;
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use super::{load_owned_session, open_session_manager};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::AppState;

const OBSERVER_QUEUE_CAPACITY: usize = 256;
const SSE_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Default, Deserialize)]
pub(super) struct ObserveAgentStreamQuery {
    #[serde(default)]
    after_generation: u64,
    #[serde(default)]
    after_sequence: u64,
}

pub(super) async fn observe_agent_stream(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(id): Path<String>,
    Query(query): Query<ObserveAgentStreamQuery>,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    let manager = open_session_manager(&state)?;
    let session = load_owned_session(&manager, &id, user.as_ref())?;
    if session.session_type != SessionType::Code {
        return Err(AppError::Conflict(
            "Live Agent observation is available only for Code sessions".to_owned(),
        ));
    }

    let subscription = state.agent_streams.write().await.subscribe(
        &id,
        query.after_generation,
        query.after_sequence,
    );
    let (sender, receiver) = mpsc::channel(OBSERVER_QUEUE_CAPACITY);
    tokio::spawn(async move {
        for envelope in subscription.replay {
            let Ok(event) = Event::default().json_data(envelope) else {
                continue;
            };
            if sender.send(Ok(event)).await.is_err() {
                return;
            }
        }

        let mut live = subscription.receiver;
        loop {
            match live.recv().await {
                Ok(envelope) => {
                    let Ok(event) = Event::default().json_data(envelope) else {
                        continue;
                    };
                    if sender.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
                // Closing forces a reconnect from the client's last contiguous
                // cursor, which replays from the larger bounded server ring.
                Err(broadcast::error::RecvError::Lagged(_))
                | Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });

    Ok(Sse::new(ReceiverStream::new(receiver))
        .keep_alive(KeepAlive::new().interval(SSE_KEEP_ALIVE_INTERVAL)))
}
