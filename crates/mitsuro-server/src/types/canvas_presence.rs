use serde::{Deserialize, Serialize};

use mitsuro_core::storage::WorldRect;

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

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CanvasPresenceHeartbeatRequest {
    pub client_id: String,
    #[serde(default)]
    pub presence_token: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub cursor: Option<CanvasCursor>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CanvasGeometryPreviewRequest {
    pub client_id: String,
    pub presence_token: String,
    pub rect: WorldRect,
    pub z_index: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CanvasGeometryPreviewReleaseQuery {
    pub client_id: String,
    #[serde(default)]
    pub committed: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CanvasCollaborationResponse {
    pub snapshot: CanvasCollaborationSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_token: Option<String>,
}
