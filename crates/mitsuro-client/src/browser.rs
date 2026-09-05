//! Typed contracts for server-owned Browser sessions.
//!
//! Browser pixels and input are transported by Mitsuro's authenticated stream;
//! clients never receive raw CDP endpoints or create local WebKit runtimes.

use serde::{Deserialize, Serialize};

pub const BROWSER_CONTROL_LEASE_HEADER: &str = "x-mitsuro-control-lease";

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrowserSessionKind {
    #[default]
    Interactive,
    Agent,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrowserSessionStatus {
    Starting,
    Ready,
    Running,
    Stopped,
    Error,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrowserViewportMode {
    Mobile,
    Desktop,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrowserCapability {
    pub available: bool,
    pub runtime: String,
    pub version: String,
    pub executable: Option<String>,
    pub live_stream: bool,
    pub semantic_actions: bool,
    pub agent_chat: bool,
    pub control_leases: bool,
    pub control_lease_version: u16,
    pub control_lease_ttl_seconds: u64,
    pub reason: Option<String>,
}

/// Public controller identity. The bearer-like lease token is intentionally
/// excluded so observers can report who controls a Browser without receiving
/// mutation authority.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrowserControlLeaseSummary {
    pub client_id: String,
    pub expires_at: String,
}

/// Ephemeral authority for mutating one server-owned Browser session.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrowserControlLease {
    pub resource_id: String,
    pub client_id: String,
    pub lease_token: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrowserSession {
    pub id: String,
    pub title: String,
    pub project_dir: String,
    pub kind: BrowserSessionKind,
    pub status: BrowserSessionStatus,
    pub url: Option<String>,
    /// Raw CDP is deliberately never exposed by the server.
    pub cdp_url: Option<String>,
    pub debug_port: Option<u16>,
    pub stream_url: Option<String>,
    pub viewers: usize,
    pub controllers: usize,
    pub controller: Option<BrowserControlLeaseSummary>,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub viewport_mode: BrowserViewportMode,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrowserSessionListResponse {
    pub sessions: Vec<BrowserSession>,
    pub capability: BrowserCapability,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CreateBrowserSessionRequest {
    pub title: Option<String>,
    pub project_dir: Option<String>,
    #[serde(default)]
    pub kind: BrowserSessionKind,
    pub url: Option<String>,
    pub launch_local: Option<bool>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrowserPresenceCapability {
    Viewer,
    Controller,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrowserHeartbeatRequest {
    pub capability: BrowserPresenceCapability,
    pub client_id: Option<String>,
    pub lease_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TakeBrowserControlRequest {
    pub client_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TakeBrowserControlResponse {
    pub lease: BrowserControlLease,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct StopBrowserSessionRequest {
    pub lease_token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrowserActionBatchRequest {
    pub lease_token: String,
    pub actions: Vec<BrowserAction>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrowserScrollDirection {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BrowserGetProperty {
    Text,
    Html,
    Value,
    Title,
    Url,
    Count,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BrowserAction {
    Navigate {
        url: String,
    },
    Snapshot {
        #[serde(default)]
        interactive: bool,
        #[serde(default)]
        compact: bool,
        depth: Option<u16>,
    },
    Click {
        target: String,
    },
    Fill {
        target: String,
        value: String,
    },
    Type {
        target: String,
        value: String,
    },
    Press {
        key: String,
    },
    Hover {
        target: String,
    },
    Select {
        target: String,
        values: Vec<String>,
    },
    Scroll {
        direction: BrowserScrollDirection,
        amount: Option<u32>,
    },
    Back,
    Forward,
    Reload,
    Wait {
        ms: u32,
    },
    Get {
        property: BrowserGetProperty,
        target: Option<String>,
    },
    Attribute {
        target: String,
        name: String,
    },
    Viewport {
        mode: BrowserViewportMode,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct BrowserActionResponse {
    pub ok: bool,
    #[serde(default)]
    pub results: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_actions_use_the_server_tagged_contract() {
        let action = BrowserAction::Viewport {
            mode: BrowserViewportMode::Desktop,
        };
        assert_eq!(
            serde_json::to_value(action).expect("action should serialize"),
            serde_json::json!({"type": "viewport", "mode": "desktop"})
        );
    }

    #[test]
    fn control_contract_matches_server_json_without_leaking_token_in_summary() {
        let summary: BrowserControlLeaseSummary = serde_json::from_value(serde_json::json!({
            "client_id": "canvas-a",
            "expires_at": "2026-08-30T12:00:00Z"
        }))
        .expect("summary should deserialize");
        assert_eq!(summary.client_id, "canvas-a");
        let summary_json = serde_json::to_value(&summary).expect("summary should serialize");
        assert!(summary_json.get("lease_token").is_none());

        let response: TakeBrowserControlResponse = serde_json::from_value(serde_json::json!({
            "lease": {
                "resource_id": "browser-1",
                "client_id": "canvas-a",
                "lease_token": "opaque-resource-token",
                "expires_at": "2026-08-30T12:01:00Z"
            }
        }))
        .expect("lease response should deserialize");
        assert_eq!(response.lease.resource_id, "browser-1");
        assert_eq!(response.lease.lease_token, "opaque-resource-token");
    }

    #[test]
    fn controller_heartbeat_and_actions_carry_the_opaque_lease() {
        let heartbeat = BrowserHeartbeatRequest {
            capability: BrowserPresenceCapability::Controller,
            client_id: Some("canvas-a".to_owned()),
            lease_token: Some("lease-1".to_owned()),
        };
        assert_eq!(
            serde_json::to_value(heartbeat).expect("heartbeat should serialize"),
            serde_json::json!({
                "capability": "controller",
                "client_id": "canvas-a",
                "lease_token": "lease-1"
            })
        );

        let batch = BrowserActionBatchRequest {
            lease_token: "lease-1".to_owned(),
            actions: vec![BrowserAction::Reload],
        };
        assert_eq!(
            serde_json::to_value(batch).expect("batch should serialize"),
            serde_json::json!({
                "lease_token": "lease-1",
                "actions": [{"type": "reload"}]
            })
        );

        let stop = StopBrowserSessionRequest {
            lease_token: "lease-1".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(stop).expect("stop should serialize"),
            serde_json::json!({"lease_token": "lease-1"})
        );
    }
}
