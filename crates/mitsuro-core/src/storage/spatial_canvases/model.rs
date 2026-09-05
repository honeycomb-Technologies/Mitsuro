use std::fmt;
use std::str::FromStr;

use anyhow::Error as AnyhowError;
use serde::{Deserialize, Serialize};

pub const SPATIAL_CANVAS_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialCanvasSurface {
    Code,
    Hive,
}

impl fmt::Display for SpatialCanvasSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Code => f.write_str("code"),
            Self::Hive => f.write_str("hive"),
        }
    }
}

impl FromStr for SpatialCanvasSurface {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "code" => Ok(Self::Code),
            "hive" => Ok(Self::Hive),
            other => Err(format!("unsupported canvas surface: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WorldRect {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

impl WorldRect {
    pub fn validate(self) -> Result<(), String> {
        if !self.x.is_finite()
            || !self.y.is_finite()
            || !self.width.is_finite()
            || !self.height.is_finite()
        {
            return Err("canvas geometry must contain only finite numbers".to_string());
        }
        if self.width < 120.0 || self.height < 80.0 {
            return Err("canvas nodes must be at least 120 by 80 world units".to_string());
        }
        if self.width > 16_384.0 || self.height > 16_384.0 {
            return Err("canvas node dimensions exceed the supported limit".to_string());
        }
        if self.x.abs() > 10_000_000.0 || self.y.abs() > 10_000_000.0 {
            return Err("canvas node coordinates exceed the supported world bounds".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanvasNodeBinding {
    Agent {
        session_id: String,
    },
    Terminal {
        terminal_id: String,
    },
    Browser {
        browser_session_id: String,
        #[serde(default)]
        last_url: Option<String>,
    },
    Files {
        #[serde(default)]
        path: Option<String>,
    },
    Changes,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanvasNode {
    pub id: String,
    pub title: String,
    pub rect: WorldRect,
    pub z_index: i64,
    pub binding: CanvasNodeBinding,
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanvasState {
    pub schema_version: u32,
    #[serde(default)]
    pub nodes: Vec<CanvasNode>,
}

impl Default for CanvasState {
    fn default() -> Self {
        Self {
            schema_version: SPATIAL_CANVAS_SCHEMA_VERSION,
            nodes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpatialCanvas {
    pub id: String,
    pub owner_user_id: Option<String>,
    pub project_dir: String,
    pub surface: SpatialCanvasSurface,
    pub name: String,
    pub schema_version: u32,
    pub revision: u64,
    pub shared_theme_id: String,
    pub state: CanvasState,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub archived_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectCandidate {
    pub project_dir: String,
    pub updated_at: String,
    pub canvas_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCanvasInput {
    pub create_op_id: String,
    pub project_dir: String,
    pub surface: SpatialCanvasSurface,
    pub name: String,
    pub shared_theme_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanvasOperation {
    UpsertNode {
        node: CanvasNode,
    },
    RemoveNode {
        node_id: String,
    },
    SetGeometry {
        node_id: String,
        rect: WorldRect,
        z_index: i64,
    },
    SetTitle {
        node_id: String,
        title: String,
    },
    RenameCanvas {
        name: String,
    },
    SetSharedTheme {
        theme_id: String,
    },
    SetArchived {
        archived: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanvasPatchInput {
    pub op_id: String,
    pub client_id: String,
    pub expected_revision: u64,
    pub operations: Vec<CanvasOperation>,
}

/// Resource kind provisioned by a server-owned Canvas create-and-bind intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanvasResourceKind {
    Agent,
    Terminal,
    Browser,
}

impl fmt::Display for CanvasResourceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Agent => "agent",
            Self::Terminal => "terminal",
            Self::Browser => "browser",
        })
    }
}

impl FromStr for CanvasResourceKind {
    type Err = AnyhowError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "agent" => Ok(Self::Agent),
            "terminal" => Ok(Self::Terminal),
            "browser" => Ok(Self::Browser),
            _ => anyhow::bail!("unsupported canvas resource kind: {value}"),
        }
    }
}

/// Durable lifecycle for a create-and-bind operation.
///
/// `Ambiguous` means a server exited after starting an external lifecycle, so
/// retrying could duplicate a process. `Cleaning`/`Cleaned`/`Abandoned` cover
/// explicit cleanup. Detach uses `detach_generation` so a restart cannot trap
/// the user on a stale cleanup marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanvasResourceCreateStatus {
    Reserved,
    Provisioning,
    Provisioned,
    Bound,
    Failed,
    Ambiguous,
    Cleaning,
    Cleaned,
    Abandoned,
}

impl fmt::Display for CanvasResourceCreateStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Reserved => "reserved",
            Self::Provisioning => "provisioning",
            Self::Provisioned => "provisioned",
            Self::Bound => "bound",
            Self::Failed => "failed",
            Self::Ambiguous => "ambiguous",
            Self::Cleaning => "cleaning",
            Self::Cleaned => "cleaned",
            Self::Abandoned => "abandoned",
        })
    }
}

impl FromStr for CanvasResourceCreateStatus {
    type Err = AnyhowError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "reserved" => Ok(Self::Reserved),
            "provisioning" => Ok(Self::Provisioning),
            "provisioned" => Ok(Self::Provisioned),
            "bound" => Ok(Self::Bound),
            "failed" => Ok(Self::Failed),
            "ambiguous" => Ok(Self::Ambiguous),
            "cleaning" => Ok(Self::Cleaning),
            "cleaned" => Ok(Self::Cleaned),
            "abandoned" => Ok(Self::Abandoned),
            _ => anyhow::bail!("unsupported canvas resource create status: {value}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveCanvasResourceCreateInput {
    pub create_op_id: String,
    pub bind_op_id: String,
    pub client_id: String,
    pub node_id: String,
    pub resource_kind: CanvasResourceKind,
    pub request_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanvasResourceCreateIntent {
    pub canvas_id: String,
    pub owner_user_id: Option<String>,
    pub create_op_id: String,
    pub bind_op_id: String,
    pub client_id: String,
    pub node_id: String,
    pub resource_kind: CanvasResourceKind,
    pub request_json: String,
    pub request_fingerprint: String,
    pub status: CanvasResourceCreateStatus,
    pub executor_id: Option<String>,
    pub claim_token: Option<String>,
    pub result_json: Option<String>,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub detach_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanvasEvent {
    pub canvas_id: String,
    pub sequence: u64,
    pub op_id: String,
    pub client_id: String,
    pub operations: Vec<CanvasOperation>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanvasEventPage {
    pub events: Vec<CanvasEvent>,
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: u64,
    pub gap: bool,
}
