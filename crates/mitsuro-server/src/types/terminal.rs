use base64ct::{Base64, Encoding};
use serde::{Deserialize, Serialize};

pub const DEFAULT_TERMINAL_COLS: u16 = 80;
pub const DEFAULT_TERMINAL_ROWS: u16 = 24;

/// Library-neutral terminal dimensions. The PTY implementation remains a
/// private server detail.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct TerminalSize {
    #[serde(default = "default_cols")]
    pub cols: u16,
    #[serde(default = "default_rows")]
    pub rows: u16,
}

impl Default for TerminalSize {
    fn default() -> Self {
        Self {
            cols: DEFAULT_TERMINAL_COLS,
            rows: DEFAULT_TERMINAL_ROWS,
        }
    }
}

const fn default_cols() -> u16 {
    DEFAULT_TERMINAL_COLS
}

const fn default_rows() -> u16 {
    DEFAULT_TERMINAL_ROWS
}

/// Request used to create a server-owned terminal.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TerminalSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(flatten)]
    pub size: TerminalSize,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    Running,
    Terminating,
    Exited,
    Disconnected,
}

/// Public, serializable description of a terminal. It deliberately contains
/// no `portable_pty` types so native and non-native clients can share it.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TerminalDescriptor {
    pub id: String,
    pub working_dir: String,
    pub size: TerminalSize,
    pub status: TerminalStatus,
    pub pid: Option<u32>,
    pub stream_url: String,
    pub available_from: u64,
    pub next_offset: u64,
    pub exit_code: Option<i32>,
    pub last_error: Option<String>,
    pub controller: Option<ControlLeaseSummary>,
    pub created_at: String,
    pub updated_at: String,
}

/// Public controller identity without the bearer-like lease token.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ControlLeaseSummary {
    pub client_id: String,
    pub expires_at: String,
}

/// Opaque authority returned only to the client that explicitly takes control.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ControlLease {
    pub resource_id: String,
    pub client_id: String,
    pub lease_token: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TakeTerminalControlRequest {
    pub client_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TakeTerminalControlResponse {
    pub lease: ControlLease,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TerminalListResponse {
    pub terminals: Vec<TerminalDescriptor>,
    pub max_sessions: usize,
}

/// Messages sent from an attached client to the server-owned terminal.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TerminalTransportCommand {
    Input {
        lease_token: String,
        #[serde(with = "base64_bytes")]
        bytes: Vec<u8>,
    },
    InputText {
        lease_token: String,
        text: String,
    },
    Resize {
        lease_token: String,
        cols: u16,
        rows: u16,
    },
    Ping {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_token: Option<String>,
    },
    Detach,
    Terminate {
        lease_token: String,
    },
}

/// Replayable transport events. Output offsets count raw PTY bytes and are
/// monotonic for the life of one terminal.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TerminalTransportEvent {
    Ready {
        terminal: TerminalDescriptor,
        replay_from: u64,
        next_offset: u64,
    },
    Output {
        offset: u64,
        #[serde(with = "base64_bytes")]
        bytes: Vec<u8>,
    },
    Gap {
        requested_offset: u64,
        available_from: u64,
        next_offset: u64,
    },
    Disconnected {
        reason: String,
    },
    ControlChanged {
        controller: Option<ControlLeaseSummary>,
    },
    CommandRejected {
        code: String,
        message: String,
    },
    Exited {
        exit_code: Option<i32>,
        next_offset: u64,
    },
    Pong,
}

mod base64_bytes {
    use super::*;
    use serde::{de::Error as _, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&Base64::encode_string(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        Base64::decode_vec(&encoded).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_wire_bytes_are_base64_and_round_trip() {
        let event = TerminalTransportEvent::Output {
            offset: 7,
            bytes: vec![0, 0xff, b'\n'],
        };

        let encoded = serde_json::to_string(&event).expect("event should serialize");
        assert!(encoded.contains("AP8K"));
        assert_eq!(
            serde_json::from_str::<TerminalTransportEvent>(&encoded)
                .expect("event should deserialize"),
            event
        );
    }

    #[test]
    fn terminal_spec_defaults_to_standard_size() {
        let spec: TerminalSpec =
            serde_json::from_str(r#"{"working_dir":"/tmp"}"#).expect("spec should parse");
        assert_eq!(spec.size, TerminalSize::default());
    }
}
