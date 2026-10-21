//! Client-neutral wire types for ephemeral spatial-canvas collaboration.
//!
//! Presence, cursors, geometry previews, and soft locks are intentionally
//! transient. Final geometry is persisted separately with a normal canvas
//! patch containing [`crate::CanvasOperation::SetGeometry`].

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::WorldRect;

pub const CANVAS_PRESENCE_STALE_AFTER_SECS: u64 = 15;
pub const CANVAS_PRESENCE_EXPIRE_AFTER_SECS: u64 = 60;
pub const CANVAS_PRESENCE_TOKEN_HEADER: &str = "x-mitsuro-presence-token";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct CanvasCursor {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasPresenceClient {
    pub client_id: String,
    pub display_name: Option<String>,
    pub cursor: Option<CanvasCursor>,
    pub last_seen_at: String,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasGeometryPreview {
    pub node_id: String,
    pub client_id: String,
    pub rect: WorldRect,
    pub z_index: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasCollaborationSnapshot {
    pub canvas_id: String,
    pub sequence: u64,
    #[serde(default)]
    pub clients: Vec<CanvasPresenceClient>,
    #[serde(default)]
    pub geometry_previews: Vec<CanvasGeometryPreview>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanvasCollaborationEvent {
    PresenceUpdated {
        canvas_id: String,
        sequence: u64,
        client: CanvasPresenceClient,
    },
    PresenceStale {
        canvas_id: String,
        sequence: u64,
        client_id: String,
    },
    PresenceLeft {
        canvas_id: String,
        sequence: u64,
        client_id: String,
        reason: CanvasPresenceLeaveReason,
    },
    GeometryPreviewUpdated {
        canvas_id: String,
        sequence: u64,
        preview: CanvasGeometryPreview,
    },
    GeometryPreviewEnded {
        canvas_id: String,
        sequence: u64,
        node_id: String,
        client_id: String,
        reason: CanvasGeometryPreviewEndReason,
    },
}

impl CanvasCollaborationEvent {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::PresenceUpdated { sequence, .. }
            | Self::PresenceStale { sequence, .. }
            | Self::PresenceLeft { sequence, .. }
            | Self::GeometryPreviewUpdated { sequence, .. }
            | Self::GeometryPreviewEnded { sequence, .. } => *sequence,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CanvasPresenceLeaveReason {
    Explicit,
    Expired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CanvasGeometryPreviewEndReason {
    Released,
    Committed,
    Stale,
    ClientLeft,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasCollaborationEventPage {
    #[serde(default)]
    pub events: Vec<CanvasCollaborationEvent>,
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: u64,
    pub gap: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CanvasPresenceHeartbeatRequest {
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_token: Option<String>,
    pub display_name: Option<String>,
    pub cursor: Option<CanvasCursor>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CanvasGeometryPreviewRequest {
    pub client_id: String,
    pub presence_token: String,
    pub rect: WorldRect,
    pub z_index: i64,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CanvasCollaborationResponse {
    pub snapshot: CanvasCollaborationSnapshot,
    #[serde(default)]
    pub presence_token: Option<String>,
}

/// Client-side projection of the server's ephemeral collaboration stream.
/// It deliberately contains no camera, selection, Stage, or draft state.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CanvasCollaborationState {
    pub canvas_id: Option<String>,
    pub sequence: u64,
    pub clients: BTreeMap<String, CanvasPresenceClient>,
    pub geometry_previews: BTreeMap<String, CanvasGeometryPreview>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanvasCollaborationApply {
    Applied,
    Ignored,
    Gap,
}

impl CanvasCollaborationState {
    pub fn replace(&mut self, snapshot: CanvasCollaborationSnapshot) -> bool {
        if self.canvas_id.as_deref() == Some(snapshot.canvas_id.as_str())
            && snapshot.sequence < self.sequence
        {
            return false;
        }
        self.canvas_id = Some(snapshot.canvas_id);
        self.sequence = snapshot.sequence;
        self.clients = snapshot
            .clients
            .into_iter()
            .map(|client| (client.client_id.clone(), client))
            .collect();
        self.geometry_previews = snapshot
            .geometry_previews
            .into_iter()
            .map(|preview| (preview.node_id.clone(), preview))
            .collect();
        true
    }

    pub fn apply_page(&mut self, page: CanvasCollaborationEventPage) -> CanvasCollaborationApply {
        if page.gap {
            return CanvasCollaborationApply::Gap;
        }
        let pending = page
            .events
            .into_iter()
            .filter(|event| event.sequence() > self.sequence)
            .collect::<Vec<_>>();
        let mut expected = self.sequence.saturating_add(1);
        for event in &pending {
            if event.sequence() != expected {
                return CanvasCollaborationApply::Gap;
            }
            expected = expected.saturating_add(1);
        }
        let mut applied = false;
        for event in pending {
            if event.sequence() <= self.sequence {
                continue;
            }
            self.apply_event(event);
            applied = true;
        }
        if page.latest_sequence > self.sequence && !applied {
            return CanvasCollaborationApply::Gap;
        }
        if applied {
            CanvasCollaborationApply::Applied
        } else {
            CanvasCollaborationApply::Ignored
        }
    }

    pub fn apply_event(&mut self, event: CanvasCollaborationEvent) {
        let sequence = event.sequence();
        match event {
            CanvasCollaborationEvent::PresenceUpdated {
                canvas_id, client, ..
            } => {
                self.canvas_id = Some(canvas_id);
                self.clients.insert(client.client_id.clone(), client);
            }
            CanvasCollaborationEvent::PresenceStale { client_id, .. } => {
                if let Some(client) = self.clients.get_mut(&client_id) {
                    client.stale = true;
                }
                self.geometry_previews
                    .retain(|_, preview| preview.client_id != client_id);
            }
            CanvasCollaborationEvent::PresenceLeft { client_id, .. } => {
                self.clients.remove(&client_id);
                self.geometry_previews
                    .retain(|_, preview| preview.client_id != client_id);
            }
            CanvasCollaborationEvent::GeometryPreviewUpdated {
                canvas_id, preview, ..
            } => {
                self.canvas_id = Some(canvas_id);
                self.geometry_previews
                    .insert(preview.node_id.clone(), preview);
            }
            CanvasCollaborationEvent::GeometryPreviewEnded {
                node_id, client_id, ..
            } => {
                if self
                    .geometry_previews
                    .get(&node_id)
                    .is_some_and(|preview| preview.client_id == client_id)
                {
                    self.geometry_previews.remove(&node_id);
                }
            }
        }
        self.sequence = self.sequence.max(sequence);
    }

    pub fn locked_by_other(&self, node_id: &str, local_client_id: &str) -> Option<&str> {
        self.geometry_previews
            .get(node_id)
            .filter(|preview| preview.client_id != local_client_id)
            .map(|preview| preview.client_id.as_str())
    }
}

/// Deterministic leading-edge throttle used for cursor and drag preview
/// publication. Callers provide elapsed monotonic time, which makes the policy
/// independently testable and avoids wall-clock behavior.
#[derive(Debug, Clone)]
pub struct CanvasPublishThrottle {
    interval: Duration,
    last_published_at: Option<Duration>,
}

impl CanvasPublishThrottle {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_published_at: None,
        }
    }

    pub fn should_publish(&mut self, now: Duration) -> bool {
        let due = self
            .last_published_at
            .is_none_or(|last| now.saturating_sub(last) >= self.interval);
        if due {
            self.last_published_at = Some(now);
        }
        due
    }

    pub fn reset(&mut self) {
        self.last_published_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str) -> CanvasPresenceClient {
        CanvasPresenceClient {
            client_id: id.to_owned(),
            display_name: None,
            cursor: None,
            last_seen_at: "now".to_owned(),
            stale: false,
        }
    }

    #[test]
    fn reducer_replaces_snapshots_and_releases_stale_clients_locks() {
        let mut state = CanvasCollaborationState::default();
        assert!(state.replace(CanvasCollaborationSnapshot {
            canvas_id: "canvas".to_owned(),
            sequence: 4,
            clients: vec![client("peer")],
            geometry_previews: vec![CanvasGeometryPreview {
                node_id: "node".to_owned(),
                client_id: "peer".to_owned(),
                rect: WorldRect {
                    x: -2.0,
                    y: 3.0,
                    width: 400.0,
                    height: 240.0,
                },
                z_index: 3,
                updated_at: "now".to_owned(),
            }],
        }));
        state.apply_event(CanvasCollaborationEvent::PresenceStale {
            canvas_id: "canvas".to_owned(),
            sequence: 5,
            client_id: "peer".to_owned(),
        });
        assert!(state.clients["peer"].stale);
        assert!(state.geometry_previews.is_empty());
        assert_eq!(state.sequence, 5);
        assert!(!state.replace(CanvasCollaborationSnapshot {
            canvas_id: "canvas".to_owned(),
            sequence: 4,
            clients: Vec::new(),
            geometry_previews: Vec::new(),
        }));
        assert!(state.clients.contains_key("peer"));
    }

    #[test]
    fn reducer_ignores_duplicates_and_surfaces_replay_gaps() {
        let mut state = CanvasCollaborationState {
            sequence: 7,
            ..Default::default()
        };
        let duplicate = CanvasCollaborationEvent::PresenceUpdated {
            canvas_id: "canvas".to_owned(),
            sequence: 7,
            client: client("peer"),
        };
        assert_eq!(
            state.apply_page(CanvasCollaborationEventPage {
                events: vec![duplicate],
                earliest_sequence: Some(7),
                latest_sequence: 7,
                gap: false,
            }),
            CanvasCollaborationApply::Ignored
        );
        assert!(state.clients.is_empty());
        assert_eq!(
            state.apply_page(CanvasCollaborationEventPage {
                events: Vec::new(),
                earliest_sequence: Some(12),
                latest_sequence: 12,
                gap: true,
            }),
            CanvasCollaborationApply::Gap
        );
        assert_eq!(
            state.apply_page(CanvasCollaborationEventPage {
                events: vec![CanvasCollaborationEvent::PresenceStale {
                    canvas_id: "canvas".to_owned(),
                    sequence: 9,
                    client_id: "peer".to_owned(),
                }],
                earliest_sequence: Some(9),
                latest_sequence: 9,
                gap: false,
            }),
            CanvasCollaborationApply::Gap
        );
        assert_eq!(state.sequence, 7);
    }

    #[test]
    fn publication_throttle_is_leading_edge_and_bounded() {
        let mut throttle = CanvasPublishThrottle::new(Duration::from_millis(50));
        assert!(throttle.should_publish(Duration::ZERO));
        assert!(!throttle.should_publish(Duration::from_millis(49)));
        assert!(throttle.should_publish(Duration::from_millis(50)));
        assert!(!throttle.should_publish(Duration::from_millis(70)));
        throttle.reset();
        assert!(throttle.should_publish(Duration::from_millis(71)));
    }
}
