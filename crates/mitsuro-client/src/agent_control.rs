//! Typed control-lease contract for interactive Code Agent sessions.

use serde::{Deserialize, Serialize};

pub const AGENT_CONTROL_LEASE_HEADER: &str = "x-mitsuro-control-lease";

/// Public controller identity. The private mutation token is intentionally
/// absent so observer clients can render honest control state without gaining
/// authority.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentControlLeaseSummary {
    pub client_id: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentControlState {
    pub session_id: String,
    pub controller: Option<AgentControlLeaseSummary>,
    pub ttl_seconds: u64,
}

/// Ephemeral authority for mutating one interactive Code Agent. Callers must
/// keep it in memory and must never persist or log it.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentControlLease {
    pub resource_id: String,
    pub client_id: String,
    pub lease_token: String,
    pub expires_at: String,
}

impl std::fmt::Debug for AgentControlLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentControlLease")
            .field("resource_id", &self.resource_id)
            .field("client_id", &self.client_id)
            .field("lease_token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TakeAgentControlRequest {
    pub client_id: String,
    /// `false` claims only an unowned lease or this same client identity.
    /// `true` is the user's explicit Take control action.
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TakeAgentControlResponse {
    pub lease: AgentControlLease,
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HeartbeatAgentControlRequest {
    pub client_id: String,
    pub lease_token: String,
}

/// Typed human response for an AskUserQuestion continuation. The server owns
/// recovery and validates the exact pending tool call before resuming.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentInputResponseRequest {
    pub session_id: String,
    pub tool_call_id: String,
    pub result: String,
}

impl std::fmt::Debug for HeartbeatAgentControlRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HeartbeatAgentControlRequest")
            .field("client_id", &self.client_id)
            .field("lease_token", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_summary_omits_token_and_private_debug_redacts_it() {
        let summary: AgentControlState = serde_json::from_value(serde_json::json!({
            "session_id": "code-1",
            "controller": {
                "client_id": "canvas-a",
                "expires_at": "2026-08-30T12:00:00Z"
            },
            "ttl_seconds": 15
        }))
        .expect("summary");
        let json = serde_json::to_value(summary).expect("serialize");
        assert!(json["controller"].get("lease_token").is_none());

        let lease = AgentControlLease {
            resource_id: "code-1".into(),
            client_id: "canvas-a".into(),
            lease_token: "private-token".into(),
            expires_at: "later".into(),
        };
        assert!(!format!("{lease:?}").contains("private-token"));
    }

    #[test]
    fn take_request_serializes_explicit_transfer_intent() {
        let value = serde_json::to_value(TakeAgentControlRequest {
            client_id: "canvas-b".into(),
            force: true,
        })
        .expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({"client_id":"canvas-b", "force":true})
        );
    }

    #[test]
    fn input_response_serializes_the_exact_pending_tool_call() {
        let value = serde_json::to_value(AgentInputResponseRequest {
            session_id: "code-1".into(),
            tool_call_id: "ask-1".into(),
            result: r#"{"answers":{"Target":"staging"}}"#.into(),
        })
        .expect("serialize");
        assert_eq!(value["session_id"], "code-1");
        assert_eq!(value["tool_call_id"], "ask-1");
        assert_eq!(value["result"], r#"{"answers":{"Target":"staging"}}"#);
    }
}
