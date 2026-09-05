use serde::{Deserialize, Serialize};

use mitsuro_core::storage::{
    CanvasEventPage, CanvasNode, CanvasOperation, CanvasResourceCreateStatus, SpatialCanvas,
    SpatialCanvasSurface, WorldRect,
};

use super::{CanvasCollaborationEventPage, CanvasCollaborationSnapshot};

#[derive(Debug, Clone, Deserialize)]
pub struct ListProjectsQuery {
    #[serde(default = "default_surface")]
    pub surface: SpatialCanvasSurface,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectSummaryResponse {
    pub name: String,
    pub project_dir: String,
    pub updated_at: String,
    pub canvas_count: u64,
    pub available: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListProjectsResponse {
    pub projects: Vec<ProjectSummaryResponse>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListCanvasesQuery {
    pub surface: Option<SpatialCanvasSurface>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListCanvasesResponse {
    pub canvases: Vec<SpatialCanvas>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateCanvasRequest {
    pub create_op_id: String,
    pub project_dir: String,
    #[serde(default = "default_surface")]
    pub surface: SpatialCanvasSurface,
    #[serde(default = "default_canvas_name")]
    pub name: String,
    #[serde(default = "default_theme")]
    pub shared_theme_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateCanvasResponse {
    pub canvas: SpatialCanvas,
    pub created: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PatchCanvasRequest {
    pub op_id: String,
    pub client_id: String,
    pub expected_revision: u64,
    pub operations: Vec<CanvasOperation>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PatchCanvasResponse {
    pub canvas: SpatialCanvas,
    pub applied: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CanvasStreamQuery {
    pub after_sequence: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanvasStreamEnvelope {
    Snapshot {
        canvas: SpatialCanvas,
    },
    Events {
        page: CanvasEventPage,
    },
    CollaborationSnapshot {
        collaboration: CanvasCollaborationSnapshot,
    },
    CollaborationEvents {
        page: CanvasCollaborationEventPage,
    },
}

fn default_surface() -> SpatialCanvasSurface {
    SpatialCanvasSurface::Code
}

fn default_canvas_name() -> String {
    "Main".to_string()
}

fn default_theme() -> String {
    "graphite".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasResourceNodeRequest {
    pub id: String,
    pub title: String,
    pub rect: WorldRect,
    pub z_index: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanvasResourceSpec {
    Agent,
    Terminal {
        size: super::TerminalSize,
    },
    Browser {
        #[serde(default)]
        url: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateCanvasResourceRequest {
    pub create_op_id: String,
    pub client_id: String,
    pub node: CanvasResourceNodeRequest,
    pub resource: CanvasResourceSpec,
    #[serde(default)]
    pub initial_prompt: Option<String>,
    #[serde(default)]
    pub retry_ambiguous: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CreateCanvasResourceResponse {
    pub canvas: SpatialCanvas,
    pub node: CanvasNode,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CanvasResourceIntentResponse {
    pub request: CreateCanvasResourceRequest,
    pub status: CanvasResourceCreateStatus,
    pub last_error: Option<String>,
    pub retry_requires_confirmation: bool,
    pub initial_prompt_completed: bool,
    #[serde(default)]
    pub detach_generation: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ListCanvasResourceIntentsResponse {
    pub intents: Vec<CanvasResourceIntentResponse>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CleanupCanvasResourceResponse {
    pub create_op_id: String,
    pub node_id: String,
    pub status: CanvasResourceCreateStatus,
    pub canvas: SpatialCanvas,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct DetachCanvasResourceRequest {
    pub request: CreateCanvasResourceRequest,
    pub expected_detach_generation: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DetachCanvasResourceResponse {
    pub create_op_id: String,
    pub node_id: String,
    pub status: CanvasResourceCreateStatus,
    pub canvas: SpatialCanvas,
    pub binding_absent: bool,
    pub detach_committed: bool,
    pub detach_generation: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AcknowledgeCanvasAgentPromptResponse {
    pub create_op_id: String,
    pub redacted: bool,
}
