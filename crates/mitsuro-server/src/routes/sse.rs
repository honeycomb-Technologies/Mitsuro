//! Shared SSE bridge policies.
//!
//! Every SSE surface follows one of two contracts, implemented exactly once
//! here:
//!
//! 1. **Skip-count bridge** (`forward_sse_event`): a bounded mpsc queue with
//!    explicit lag signaling. Non-critical events are dropped with a running
//!    skip counter that is flushed as a single `AgenticEvent::Lagged`; events
//!    that the client contract requires (approvals, `Finished`, ...) get a
//!    bounded delivery timeout instead of blocking forever on a stalled
//!    client.
//! 2. **Broadcast pump** (`spawn_broadcast_sse_pump`): a `broadcast` channel
//!    whose lag semantics are forwarded to the client verbatim as
//!    `AgenticEvent::Lagged`.

use std::convert::Infallible;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::types::AgenticEvent;

pub(crate) const SSE_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
pub(crate) const SSE_REQUIRED_DELIVERY_TIMEOUT: Duration = Duration::from_millis(250);

pub(crate) type SseItem = Result<Event, Infallible>;
pub(crate) type SseStream = Sse<ReceiverStream<SseItem>>;

/// Wrap a receiver half into the standard SSE response with keep-alive.
pub(crate) fn sse_response(rx: mpsc::Receiver<SseItem>) -> SseStream {
    Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::new().interval(SSE_KEEP_ALIVE_INTERVAL))
}

/// Serialize one event; serialization failures are dropped (malformed payloads
/// must not terminate a stream that is otherwise healthy).
pub(crate) fn event_to_sse(event: &AgenticEvent) -> Option<Event> {
    Event::default().json_data(event).ok()
}

/// Bounded delivery for client-contract-required events.
async fn send_required(sse_tx: &mpsc::Sender<SseItem>, event: Event) -> bool {
    matches!(
        tokio::time::timeout(SSE_REQUIRED_DELIVERY_TIMEOUT, sse_tx.send(Ok(event))).await,
        Ok(Ok(()))
    )
}

/// Forward one event through the skip-count bridge.
///
/// Returns `true` while the stream stays healthy; `false` once the client is
/// gone and the bridge should stop forwarding.
pub(crate) async fn forward_sse_event(
    sse_tx: &mpsc::Sender<SseItem>,
    session_id: &str,
    event: AgenticEvent,
    requires_delivery: bool,
    skipped_events: &mut usize,
    drop_log: &'static str,
) -> bool {
    if *skipped_events > 0 {
        let lagged_event = AgenticEvent::Lagged {
            skipped: *skipped_events,
        };

        if let Some(sse_event) = event_to_sse(&lagged_event) {
            if requires_delivery {
                if !send_required(sse_tx, sse_event).await {
                    return false;
                }
                *skipped_events = 0;
            } else {
                match sse_tx.try_send(Ok(sse_event)) {
                    Ok(()) => *skipped_events = 0,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        *skipped_events = skipped_events.saturating_add(1);
                        tracing::warn!(
                            session_id,
                            skipped = *skipped_events,
                            "Dropping SSE event because client queue is full"
                        );
                        return true;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return false,
                }
            }
        } else {
            *skipped_events = 0;
        }
    }

    let Some(sse_event) = event_to_sse(&event) else {
        return true;
    };

    if requires_delivery {
        return send_required(sse_tx, sse_event).await;
    }
    match sse_tx.try_send(Ok(sse_event)) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            *skipped_events = skipped_events.saturating_add(1);
            tracing::warn!(session_id, skipped = *skipped_events, "{drop_log}");
            true
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

/// Pump a broadcast event stream into an SSE channel.
///
/// Replay events are delivered first. Broadcast lag is surfaced to the client
/// as `AgenticEvent::Lagged`. With `break_on_terminal`, the pump stops after a
/// terminal (`Finish`/`Error`) event has been delivered.
pub(crate) fn spawn_broadcast_sse_pump(
    mut receiver: broadcast::Receiver<AgenticEvent>,
    tx: mpsc::Sender<SseItem>,
    replay: Vec<AgenticEvent>,
    break_on_terminal: bool,
) {
    tokio::spawn(async move {
        for event in replay {
            let Some(sse_event) = event_to_sse(&event) else {
                continue;
            };
            if tx.send(Ok(sse_event)).await.is_err() {
                return;
            }
        }

        loop {
            match receiver.recv().await {
                Ok(event) => {
                    let terminal = break_on_terminal
                        && matches!(
                            event,
                            AgenticEvent::Finish { .. } | AgenticEvent::Error { .. }
                        );
                    let Some(sse_event) = event_to_sse(&event) else {
                        continue;
                    };
                    if tx.send(Ok(sse_event)).await.is_err() || terminal {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let event = AgenticEvent::Lagged {
                        skipped: usize::try_from(skipped).unwrap_or(usize::MAX),
                    };
                    let Some(sse_event) = event_to_sse(&event) else {
                        continue;
                    };
                    if tx.send(Ok(sse_event)).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}
