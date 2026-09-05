use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct FileResponse {
    pub path: String,
    pub content: String,
    pub size: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct FileTreeEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    #[serde(default)]
    pub children: Option<Vec<FileTreeEntry>>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct FileTreeResponse {
    pub root: String,
    #[serde(default)]
    pub entries: Vec<FileTreeEntry>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GitChangedFile {
    pub path: String,
    pub status: String,
    pub additions: usize,
    pub deletions: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GitChangesResponse {
    pub in_repo: bool,
    pub repo_root: Option<String>,
    #[serde(default)]
    pub files: Vec<GitChangedFile>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GitFileDiffResponse {
    pub path: String,
    pub patch: String,
    pub truncated: bool,
    pub binary: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BrowseEntry {
    pub name: String,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BrowseResponse {
    pub current: String,
    pub parent: Option<String>,
    #[serde(default)]
    pub directories: Vec<BrowseEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpatialCanvasSurface {
    Code,
    Hive,
}

impl SpatialCanvasSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Code => "code",
            Self::Hive => "hive",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct WorldRect {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasNode {
    pub id: String,
    pub title: String,
    pub rect: WorldRect,
    pub z_index: i64,
    pub binding: CanvasNodeBinding,
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasState {
    pub schema_version: u32,
    #[serde(default)]
    pub nodes: Vec<CanvasNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CreateCanvasRequest {
    pub create_op_id: String,
    pub project_dir: String,
    pub surface: SpatialCanvasSurface,
    pub name: String,
    pub shared_theme_id: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CreateCanvasResponse {
    pub canvas: SpatialCanvas,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PatchCanvasRequest {
    pub op_id: String,
    pub client_id: String,
    pub expected_revision: u64,
    pub operations: Vec<CanvasOperation>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PatchCanvasResponse {
    pub canvas: SpatialCanvas,
    pub applied: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ListCanvasesResponse {
    #[serde(default)]
    pub canvases: Vec<SpatialCanvas>,
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
        size: crate::terminal::TerminalSize,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct CreateCanvasResourceResponse {
    pub canvas: SpatialCanvas,
    pub node: CanvasNode,
    pub created: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct CanvasResourceIntentResponse {
    pub request: CreateCanvasResourceRequest,
    pub status: String,
    pub last_error: Option<String>,
    pub retry_requires_confirmation: bool,
    #[serde(default)]
    pub initial_prompt_completed: bool,
    #[serde(default)]
    pub detach_generation: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct ListCanvasResourceIntentsResponse {
    pub intents: Vec<CanvasResourceIntentResponse>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct CleanupCanvasResourceResponse {
    pub create_op_id: String,
    pub node_id: String,
    pub status: String,
    pub canvas: SpatialCanvas,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DetachCanvasResourceRequest {
    pub request: CreateCanvasResourceRequest,
    pub expected_detach_generation: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct DetachCanvasResourceResponse {
    pub create_op_id: String,
    pub node_id: String,
    pub status: String,
    pub canvas: SpatialCanvas,
    pub binding_absent: bool,
    pub detach_committed: bool,
    pub detach_generation: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ProjectSummary {
    pub name: String,
    pub project_dir: String,
    pub updated_at: String,
    pub canvas_count: u64,
    pub available: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ListProjectsResponse {
    #[serde(default)]
    pub projects: Vec<ProjectSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasEvent {
    pub canvas_id: String,
    pub sequence: u64,
    pub op_id: String,
    pub client_id: String,
    pub operations: Vec<CanvasOperation>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanvasEventPage {
    pub events: Vec<CanvasEvent>,
    pub earliest_sequence: Option<u64>,
    pub latest_sequence: u64,
    pub gap: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CanvasStreamEnvelope {
    Snapshot {
        canvas: SpatialCanvas,
    },
    Events {
        page: CanvasEventPage,
    },
    CollaborationSnapshot {
        collaboration: crate::CanvasCollaborationSnapshot,
    },
    CollaborationEvents {
        page: crate::CanvasCollaborationEventPage,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SkillInfo {
    pub name: String,
    pub description: String,
    pub path: String,
    pub source: String,
    pub origin: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExtensionOverview {
    #[serde(default)]
    pub extensions: Vec<ExtensionStatus>,
    #[serde(default)]
    pub diagnostics: Vec<Value>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ExtensionStatus {
    pub id: String,
    pub name: String,
    pub version: String,
    pub path: String,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub commands: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct McpServer {
    pub name: String,
    pub enabled: bool,
    pub connected: bool,
    pub status: String,
    pub tool_count: usize,
    #[serde(default)]
    pub tools: Vec<Value>,
    #[serde(default)]
    pub server_info: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BackgroundProcess {
    pub id: String,
    pub command: String,
    pub description: Option<String>,
    pub pid: Option<u32>,
    pub status_code: String,
    pub elapsed_secs: u64,
    pub error: Option<String>,
    pub exit_code: Option<i32>,
    pub working_dir: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveStatusSummary {
    pub home_status: String,
    pub total_count: usize,
    pub running_count: usize,
    pub sleeping_count: usize,
    pub scheduled_count: usize,
    pub paused_count: usize,
    pub failed_count: usize,
    pub idle_count: usize,
    pub pending_approvals_count: usize,
    pub next_wake_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveRunDiagnostic {
    pub kind: String,
    pub severity: String,
    pub summary: String,
    pub detail: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct HiveCurrentRun {
    pub session_id: String,
    pub title: String,
    pub updated_at: String,
    pub project_dir: Option<String>,
    pub target_branch: Option<String>,
    pub agent_state: String,
    pub runtime: Option<HiveRuntimeState>,
    pub pending_tasks: usize,
    pub in_progress_tasks: usize,
    pub completed_tasks: usize,
    pub failed_tasks: usize,
    pub blocked_tasks: usize,
    pub diagnostic: Option<HiveRunDiagnostic>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum HiveRunPriority {
    Low,
    #[default]
    Normal,
    High,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveRuntimeState {
    pub session_id: String,
    pub status: String,
    pub next_wake_at: Option<String>,
    pub sleep_reason: Option<String>,
    pub last_error: Option<String>,
    pub current_run_id: Option<String>,
    pub last_wake_reason: Option<String>,
    pub crew_slug: Option<String>,
    #[serde(default)]
    pub priority: HiveRunPriority,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveTask {
    pub id: String,
    pub session_id: String,
    pub subject: String,
    pub description: String,
    pub status: String,
    pub owner: Option<String>,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub result: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveCadence {
    pub tick_interval_secs: u64,
    pub max_ticks: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveSessionStatus {
    pub session_id: String,
    pub session_type: String,
    pub title: String,
    #[serde(default)]
    pub tasks: Vec<HiveTask>,
    pub agent_state: String,
    pub runtime: Option<HiveRuntimeState>,
    pub cadence: HiveCadence,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HiveDispatchRequest {
    pub task: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_key: Option<ModelKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<HiveRunPriority>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crew_slug: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveDispatchResponse {
    pub session_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HiveMessageRequest {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HivePriorityRequest {
    pub priority: HiveRunPriority,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HiveCrewRequest {
    pub crew_slug: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct HiveCurrentResponse {
    pub status: HiveStatusSummary,
    pub diagnostics: Value,
    #[serde(default)]
    pub runs: Vec<HiveCurrentRun>,
    #[serde(default)]
    pub approvals: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HiveScheduleWeekday {
    Sunday,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HiveMonthlyDayPolicy {
    Skip,
    LastDay,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HiveScheduleRecurrence {
    Once {
        at: String,
    },
    Daily {
        start_date: String,
        time: String,
    },
    Weekdays {
        start_date: String,
        time: String,
    },
    Weekly {
        start_date: String,
        time: String,
        weekdays: Vec<HiveScheduleWeekday>,
    },
    Monthly {
        start_date: String,
        time: String,
        day: u8,
        invalid_day_policy: HiveMonthlyDayPolicy,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HiveDstGapPolicy {
    ShiftForward,
    Skip,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HiveDstFoldPolicy {
    First,
    Second,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HiveDstPolicy {
    pub gap: HiveDstGapPolicy,
    pub fold: HiveDstFoldPolicy,
}

impl Default for HiveDstPolicy {
    fn default() -> Self {
        Self {
            gap: HiveDstGapPolicy::ShiftForward,
            fold: HiveDstFoldPolicy::First,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HiveMisfirePolicy {
    Skip,
    FireOnce,
    CatchUp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HiveMisfireConfig {
    pub policy: HiveMisfirePolicy,
    pub grace_secs: u64,
    pub catch_up_limit: usize,
}

impl Default for HiveMisfireConfig {
    fn default() -> Self {
        Self {
            policy: HiveMisfirePolicy::FireOnce,
            grace_secs: 300,
            catch_up_limit: 3,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HiveOverlapPolicy {
    Skip,
    #[default]
    QueueOne,
    Allow,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HiveRetryJitter {
    None,
    Full,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HiveRetryPolicy {
    pub max_attempts: u32,
    pub base_delay_secs: u64,
    pub max_delay_secs: u64,
    pub jitter: HiveRetryJitter,
}

impl Default for HiveRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay_secs: 15,
            max_delay_secs: 900,
            jitter: HiveRetryJitter::Full,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HiveScheduleWriteRequest {
    pub title: String,
    pub summary: String,
    pub objective: String,
    pub recurrence: HiveScheduleRecurrence,
    pub timezone: String,
    pub dst_policy: HiveDstPolicy,
    pub priority: i32,
    pub project_dir: Option<String>,
    pub model: Option<String>,
    pub model_key: Option<ModelKey>,
    pub crew_slug: Option<String>,
    pub misfire: HiveMisfireConfig,
    pub overlap_policy: HiveOverlapPolicy,
    pub retry: HiveRetryPolicy,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveScheduleSummary {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub objective: String,
    pub recurrence: HiveScheduleRecurrence,
    pub next_fire_at: Option<String>,
    pub last_scheduled_for: Option<String>,
    pub status: String,
    pub timezone: String,
    pub dst_policy: HiveDstPolicy,
    pub priority: i32,
    pub project_dir: Option<String>,
    pub model: Option<String>,
    pub model_key: Option<ModelKey>,
    pub model_catalog_revision: Option<String>,
    pub crew_slug: Option<String>,
    pub misfire: HiveMisfireConfig,
    pub overlap_policy: HiveOverlapPolicy,
    pub retry: HiveRetryPolicy,
    pub revision: u64,
    pub controller_session_id: String,
}

/// Response returned by Hive schedule create/status mutation routes.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HiveScheduleMutationResponse {
    pub schedule_id: String,
    pub revision: u64,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {
    pub status: String,
    #[serde(default)]
    pub identity: String,
    #[serde(default)]
    pub pid: u32,
    pub version: String,
    #[serde(default)]
    pub api_protocol: u16,
    #[serde(default)]
    pub capabilities: std::collections::HashMap<String, u16>,
    #[serde(default)]
    pub features: std::collections::HashMap<String, bool>,
}

impl HealthResponse {
    /// Return the advertised version of a named contract. Missing
    /// capabilities from older servers are version 0.
    pub fn capability_version(&self, name: &str) -> u16 {
        self.capabilities.get(name).copied().unwrap_or_default()
    }

    /// Return whether the server advertises at least `minimum_version` of a
    /// named contract.
    pub fn supports_capability(&self, name: &str, minimum_version: u16) -> bool {
        self.capability_version(name) >= minimum_version
    }
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct ModelsResponse {
    #[serde(default)]
    pub models: Vec<ModelInfo>,
    pub default_model: Option<String>,
    #[serde(default)]
    pub default_model_key: Option<ModelKey>,
}

/// Provider-aware executable model identity returned by modern Mitsuro servers.
/// String-valued wire fields keep this client forward-compatible with new
/// providers and transport formats.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct ModelKey {
    pub provider: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_scope: Option<String>,
    pub api_format: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ModelInfo {
    #[serde(default)]
    pub key: Option<ModelKey>,
    pub id: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub context_window: usize,
    #[serde(default)]
    pub max_output: usize,
    #[serde(default)]
    pub supports_thinking: bool,
    #[serde(default)]
    pub reasoning_control: Option<ReasoningControl>,
    #[serde(default)]
    pub supported_reasoning_levels: Vec<ReasoningEffort>,
    #[serde(default)]
    pub default_reasoning_level: Option<ReasoningEffort>,
    #[serde(default)]
    pub reasoning_is_mandatory: bool,
    #[serde(default)]
    pub supports_fast_mode: bool,
    #[serde(default)]
    pub fast_mode: Option<FastMode>,
    #[serde(default)]
    pub supports_tools: bool,
    #[serde(default)]
    pub supports_vision: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
    Ultra,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningControl {
    OpenAiEffort,
    AnthropicAdaptive,
    AnthropicBudget,
    Boolean,
    OutputOnly,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FastMode {
    Priority,
    AnthropicFast,
    #[serde(other)]
    Unknown,
}

impl ModelInfo {
    pub fn selectable_thinking_levels(&self) -> Vec<ThinkingLevel> {
        if self.reasoning_control == Some(ReasoningControl::OutputOnly) {
            return vec![ThinkingLevel::Off];
        }

        let mut levels = self
            .supported_reasoning_levels
            .iter()
            .filter(|effort| **effort != ReasoningEffort::Ultra)
            .filter_map(|effort| ThinkingLevel::from_reasoning_effort(*effort))
            .collect::<Vec<_>>();
        levels.dedup();

        // Older Mitsuro servers only advertised the coarse `supports_thinking`
        // flag. Keep those models usable while newer servers provide the exact
        // ordered effort list.
        if levels.is_empty() {
            if !self.supports_thinking {
                return vec![ThinkingLevel::Off];
            }

            let fallback = self
                .default_reasoning_level
                .and_then(ThinkingLevel::from_reasoning_effort)
                .filter(|level| !matches!(level, ThinkingLevel::Off | ThinkingLevel::Ultra))
                .unwrap_or(ThinkingLevel::Medium);
            return if self.reasoning_is_mandatory {
                vec![fallback]
            } else {
                vec![ThinkingLevel::Off, fallback]
            };
        }

        if self.reasoning_is_mandatory {
            levels.retain(|level| *level != ThinkingLevel::Off);
            if levels.is_empty() {
                levels.push(
                    self.default_reasoning_level
                        .and_then(ThinkingLevel::from_reasoning_effort)
                        .filter(|level| !matches!(level, ThinkingLevel::Off | ThinkingLevel::Ultra))
                        .unwrap_or(ThinkingLevel::Medium),
                );
            }
        } else if !levels.contains(&ThinkingLevel::Off) {
            levels.insert(0, ThinkingLevel::Off);
        }
        levels
    }

    pub fn normalize_thinking_level(&self, current: ThinkingLevel) -> ThinkingLevel {
        let levels = self.selectable_thinking_levels();
        if levels.is_empty() {
            return if self.supports_thinking {
                current
            } else {
                ThinkingLevel::Off
            };
        }
        if levels.contains(&current) {
            return current;
        }
        self.default_reasoning_level
            .and_then(ThinkingLevel::from_reasoning_effort)
            .filter(|level| levels.contains(level))
            .unwrap_or(levels[0])
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    ApiKey,
    OAuthBrowser,
    OAuthDevice,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ProviderStatus {
    pub id: String,
    pub name: String,
    pub configured: bool,
    pub has_oauth: bool,
    pub supports_oauth: bool,
    #[serde(default)]
    pub auth_methods: Vec<AuthMethod>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SetCredentialRequest {
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OAuthStartRequest {
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_type: Option<String>,
}

impl OAuthStartRequest {
    pub fn new(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            flow_type: None,
        }
    }

    pub fn with_flow_type(provider: impl Into<String>, flow_type: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            flow_type: Some(flow_type.into()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OAuthDeviceCodeInfo {
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    pub expires_in: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OAuthStartResponse {
    pub auth_url: String,
    pub provider: String,
    pub flow_type: String,
    pub paste_code: bool,
    #[serde(default)]
    pub device_code: Option<OAuthDeviceCodeInfo>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OAuthStatusResponse {
    pub has_token: bool,
    pub flow_active: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OAuthExchangeRequest {
    pub provider: String,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct OAuthExchangeResponse {
    pub success: bool,
}

impl ModelInfo {
    pub fn label(&self) -> &str {
        if self.display_name.is_empty() {
            &self.id
        } else {
            &self.display_name
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionType {
    Chat,
    #[default]
    Code,
    #[serde(alias = "mako")]
    Hive,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceMode {
    Created,
    Selected,
    #[default]
    Neutral,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkMode {
    #[default]
    Build,
    Plan,
}

impl WorkMode {
    pub fn toggle(self) -> Self {
        match self {
            Self::Build => Self::Plan,
            Self::Plan => Self::Build,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Plan => "plan",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    Supervised,
    #[default]
    Autonomous,
}

impl PermissionMode {
    pub fn toggle(self) -> Self {
        match self {
            Self::Supervised => Self::Autonomous,
            Self::Autonomous => Self::Supervised,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Supervised => "supervised",
            Self::Autonomous => "auto",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingLevel {
    Off,
    Minimal,
    Low,
    #[default]
    Medium,
    High,
    XHigh,
    Max,
    Ultra,
}

impl ThinkingLevel {
    pub fn api_value(self) -> Option<&'static str> {
        match self {
            Self::Off => None,
            Self::Minimal => Some("minimal"),
            Self::Low => Some("low"),
            Self::Medium => Some("medium"),
            Self::High => Some("high"),
            Self::XHigh => Some("xhigh"),
            Self::Max => Some("max"),
            Self::Ultra => Some("ultra"),
        }
    }

    pub fn cycle(self) -> Self {
        match self {
            Self::Off => Self::Minimal,
            Self::Minimal => Self::Low,
            Self::Low => Self::Medium,
            Self::Medium => Self::High,
            Self::High => Self::XHigh,
            Self::XHigh => Self::Max,
            Self::Max | Self::Ultra => Self::Off,
        }
    }

    pub fn cycle_for_model(self, model: &ModelInfo) -> Self {
        let levels = model.selectable_thinking_levels();
        if levels.is_empty() {
            return model.normalize_thinking_level(self);
        }
        let index = levels.iter().position(|level| *level == self);
        levels[index.map_or(0, |index| (index + 1) % levels.len())]
    }

    fn from_reasoning_effort(effort: ReasoningEffort) -> Option<Self> {
        match effort {
            ReasoningEffort::None => Some(Self::Off),
            ReasoningEffort::Minimal => Some(Self::Minimal),
            ReasoningEffort::Low => Some(Self::Low),
            ReasoningEffort::Medium => Some(Self::Medium),
            ReasoningEffort::High => Some(Self::High),
            ReasoningEffort::XHigh => Some(Self::XHigh),
            ReasoningEffort::Max => Some(Self::Max),
            ReasoningEffort::Ultra => Some(Self::Ultra),
            ReasoningEffort::Unknown => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Minimal => "min",
            Self::Low => "low",
            Self::Medium => "med",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
            Self::Ultra => "ultra",
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SessionInfo {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub token_count: Option<usize>,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub project_dir: Option<String>,
    #[serde(default)]
    pub workspace_mode: WorkspaceMode,
    #[serde(default)]
    pub session_type: SessionType,
    #[serde(default)]
    pub mode: WorkMode,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub model_key: Option<ModelKey>,
    #[serde(default)]
    pub model_catalog_revision: Option<String>,
    #[serde(default)]
    pub target_branch: Option<String>,
    #[serde(default)]
    pub permission_mode: PermissionMode,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct SessionWithMessages {
    pub session: SessionInfo,
    #[serde(default)]
    pub messages: Vec<MessageResponse>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct MessageResponse {
    pub role: String,
    pub content: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct CreateSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_key: Option<ModelKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_mode: Option<WorkspaceMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_type: Option<SessionType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<PermissionMode>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct UpdateSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_key: Option<ModelKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_mode: Option<WorkspaceMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_branch: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<WorkMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<PermissionMode>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct SessionStateResponse {
    pub id: String,
    pub agent_state: String,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub last_event_at: Option<String>,
    #[serde(default)]
    pub mode: WorkMode,
    #[serde(default)]
    pub permission_mode: PermissionMode,
    #[serde(default)]
    pub recovery: Option<SessionRecoveryState>,
    #[serde(default)]
    pub pending_interactions: Vec<PendingInteractionSnapshot>,
    #[serde(default)]
    pub live_partial_assistant: Option<PartialAssistantState>,
    #[serde(default)]
    pub delegated_tools: Vec<DelegatedToolStateResponse>,
    #[serde(default)]
    pub recent_delegated_runs: Vec<DelegatedRunResponse>,
    #[serde(default)]
    pub delegated_run_summaries: Vec<DelegatedRunSummaryResponse>,
    #[serde(default)]
    pub delegation_groups: Vec<DelegationGroupStateResponse>,
    #[serde(default)]
    pub delegation_events: Vec<DelegationEventResponse>,
    #[serde(default)]
    pub delegation_event_cursor: Option<i64>,
    #[serde(default)]
    pub last_event_sequence: Option<i64>,
    #[serde(default)]
    pub agent_stream_generation: u64,
    #[serde(default)]
    pub agent_stream_sequence: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationGroupState {
    Created,
    Queued,
    Running,
    ReadyForParent,
    Synthesizing,
    Complete,
    Degraded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationTaskState {
    Created,
    Queued,
    Leased,
    Running,
    Retrying,
    Complete,
    Degraded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationExecutionMode {
    Foreground,
    Detached,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationParentContinuationState {
    NotRequested,
    Pending,
    Queued,
    Promoted,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedRunRole {
    Explore,
    Build,
    Planner,
    Verifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DelegationEventKind {
    GroupCreated,
    GroupQueued,
    GroupStateChanged,
    TaskClaimed,
    TaskRunning,
    TaskStateChanged,
    ParentContinuationQueued,
    ParentContinuationPromoted,
    Other(String),
}

impl<'de> Deserialize<'de> for DelegationEventKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "group_created" => Self::GroupCreated,
            "group_queued" => Self::GroupQueued,
            "group_state_changed" => Self::GroupStateChanged,
            "task_claimed" => Self::TaskClaimed,
            "task_running" => Self::TaskRunning,
            "task_state_changed" => Self::TaskStateChanged,
            "parent_continuation_queued" => Self::ParentContinuationQueued,
            "parent_continuation_promoted" => Self::ParentContinuationPromoted,
            _ => Self::Other(value),
        })
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DelegationTaskStateResponse {
    pub delegation_task_id: String,
    pub task_key: String,
    pub role: DelegatedRunRole,
    pub state: DelegationTaskState,
    pub attempt_count: usize,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DelegationGroupStateResponse {
    pub delegation_group_id: String,
    #[serde(default)]
    pub parent_tool_call_id: Option<String>,
    pub state: DelegationGroupState,
    pub execution_mode: DelegationExecutionMode,
    pub parent_continuation_state: DelegationParentContinuationState,
    #[serde(default)]
    pub tasks: Vec<DelegationTaskStateResponse>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DelegationEventResponse {
    pub event_id: i64,
    pub parent_session_id: String,
    pub delegation_group_id: String,
    #[serde(default)]
    pub delegation_task_id: Option<String>,
    pub event_type: DelegationEventKind,
    #[serde(default)]
    pub payload: Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedToolKind {
    Explore,
    Plan,
    Verify,
    Build,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedRunStage {
    Created,
    Running,
    Synthesizing,
    Complete,
    Degraded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedProgressStatus {
    Created,
    Queued,
    Leased,
    Running,
    Retrying,
    Complete,
    Degraded,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DelegatedProgressEvent {
    pub delegated_run_id: String,
    pub tool_call_id: String,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    pub parent_session_id: String,
    pub task_id: String,
    pub agent_name: String,
    pub status: DelegatedProgressStatus,
    pub tool_count: usize,
    pub tokens: usize,
    #[serde(default)]
    pub current_action: Option<String>,
    #[serde(default)]
    pub completion_summary: Option<String>,
    pub lines_added: usize,
    pub lines_removed: usize,
    #[serde(default)]
    pub completed_plan_task: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DelegatedAgentStateResponse {
    pub task_id: String,
    pub agent_name: String,
    pub status: DelegatedProgressStatus,
    pub tool_count: usize,
    pub tokens: usize,
    #[serde(default)]
    pub current_action: Option<String>,
    #[serde(default)]
    pub completion_summary: Option<String>,
    pub lines_added: usize,
    pub lines_removed: usize,
    #[serde(default)]
    pub completed_plan_task: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DelegatedToolStateResponse {
    pub delegated_run_id: String,
    pub tool_call_id: String,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub agents: Vec<DelegatedAgentStateResponse>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunScopeResponse {
    pub label: String,
    pub path: String,
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct DelegatedRunResponse {
    pub delegated_run_id: String,
    #[serde(default)]
    pub parent_tool_call_id: Option<String>,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    pub resumable: bool,
    #[serde(default)]
    pub resumed_from_run_id: Option<String>,
    #[serde(default)]
    pub child_name: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub target_scope: Vec<DelegatedRunScopeResponse>,
    #[serde(default)]
    pub human_review: Option<String>,
    #[serde(default)]
    pub artifact: Option<Value>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DelegatedRunSummaryResponse {
    pub delegated_run_id: String,
    pub parent_tool_call_id: String,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    #[serde(default)]
    pub child_name: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionStateOptions {
    pub include_delegated_history: bool,
    pub delegation_after_cursor: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct SessionRecoveryState {
    pub schema_version: usize,
    pub status: String,
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    pub partial_assistant: PartialAssistantState,
    #[serde(default)]
    pub pending_interactions: Vec<PendingInteractionSnapshot>,
    #[serde(default)]
    pub decision: Value,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
pub struct PartialAssistantState {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub citations: Vec<Citation>,
    #[serde(default)]
    pub thinking: Option<String>,
    #[serde(default)]
    pub tool_calls: Vec<RecoveryToolCall>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RecoveryToolArguments {
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub redacted_paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct RecoveryToolCall {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Option<RecoveryToolArguments>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PendingInteractionSnapshot {
    ToolApproval {
        tool_call: RecoveryToolCall,
    },
    AskUserQuestion {
        tool_call_id: String,
        #[serde(default)]
        questions: Vec<Value>,
    },
    PlanConfirm {
        tool_call_id: String,
        title: String,
        task_count: usize,
        #[serde(default)]
        tasks: Vec<Value>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ServerAccessResponse {
    pub local_url: String,
    pub remote_access_enabled: bool,
    pub remote_access_token_available: bool,
    #[serde(default)]
    pub revealed_remote_access_token: Option<String>,
    #[serde(default)]
    pub remote_launch_url: Option<String>,
    pub tailscale: TailscaleAccessResponse,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct UpdateServerAccessRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotate_token: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reveal_token: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TailscaleAccessResponse {
    pub status: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ServerStatusResponse {
    pub active_agent_streams: usize,
    #[serde(default)]
    pub active_sessions: Vec<ActiveSessionStatus>,
    #[serde(default)]
    pub memory: Value,
    pub tailscale: TailscaleAccessResponse,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ActiveSessionStatus {
    pub id: String,
    pub title: String,
    pub agent_state: String,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub last_event_at: Option<String>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub project_dir: Option<String>,
    #[serde(default)]
    pub workspace_mode: WorkspaceMode,
    pub active_viewers: usize,
    pub active_controllers: usize,
    pub stale_clients: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ContentBlock {
    Text { text: String },
    Image { source: ImageSource },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ImageSource {
    Base64 { media_type: String, data: String },
    Url { url: String },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ChatRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<ContentBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_mode: Option<WorkspaceMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_type: Option<SessionType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_key: Option<ModelKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_enabled: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fast_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<PermissionMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<WorkMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Deprecated compatibility field. Chat research is automatic and this value is ignored.
    pub research_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SteerRequest {
    pub session_id: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<ContentBlock>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SteerResponse {
    pub status: String,
    pub pending_id: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolApprovalRequest {
    pub session_id: String,
    pub tool_call_id: String,
    pub approved: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SimpleOkResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlanItem {
    pub content: String,
    pub completed: bool,
}

/// Source metadata attached to the assistant prose it supports.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
pub struct Citation {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub cited_text: String,
}

pub fn citations_from_value(value: Option<&Value>) -> Vec<Citation> {
    let Some(entries) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut citations = Vec::new();
    for entry in entries {
        if let Ok(citation) = serde_json::from_value::<Citation>(entry.clone()) {
            if (!citation.url.is_empty() || !citation.title.is_empty())
                && !citations.contains(&citation)
            {
                citations.push(citation);
            }
        }
    }
    citations
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChatStreamEvent {
    TextDelta {
        delta: String,
    },
    TextDeltaWithCitations {
        delta: String,
        citations: Vec<Citation>,
    },
    ThinkingDelta {
        thinking: String,
    },
    ThinkingComplete {
        thinking: String,
        signature: String,
    },
    ToolCallStart {
        id: String,
        name: String,
    },
    ToolCallComplete {
        id: String,
        name: String,
        arguments: Value,
    },
    ToolExecuting {
        id: String,
        name: String,
    },
    ToolOutputDelta {
        id: String,
        delta: String,
    },
    ToolResult {
        id: String,
        output: String,
        is_error: bool,
    },
    ServerToolStart {
        id: String,
        name: String,
    },
    ServerToolComplete {
        id: String,
        name: String,
    },
    ServerToolError {
        tool_use_id: String,
        error_code: String,
    },
    WebSearchResults {
        tool_use_id: String,
        payload: Value,
    },
    WebFetchResult {
        tool_use_id: String,
        payload: Value,
    },
    DelegatedProgress {
        payload: DelegatedProgressEvent,
    },
    DelegationEvent {
        event: DelegationEventResponse,
    },
    AwaitingInput {
        tool_call_id: String,
        tool_name: String,
    },
    ModeChange {
        mode: String,
        reason: Option<String>,
    },
    PlanUpdate {
        items: Vec<PlanItem>,
    },
    PlanComplete {
        tool_call_id: String,
        title: String,
        task_count: usize,
    },
    AgentSleeping {
        duration_secs: u64,
        reason: String,
    },
    TurnComplete {
        turn: usize,
        has_more: bool,
    },
    TickInjected {
        tick_number: usize,
    },
    Usage {
        prompt_tokens: usize,
        input_tokens: usize,
        completion_tokens: usize,
        reasoning_tokens: usize,
        cache_creation_input_tokens: usize,
        cache_read_input_tokens: usize,
        total_tokens: usize,
    },
    SessionPinched {
        reason: String,
        source_session_id: String,
        new_session_id: String,
        estimated_tokens_before: usize,
    },
    ContextCompactionStarted {
        reason: String,
    },
    ContextCompacted {
        payload: Value,
    },
    Lagged {
        skipped: usize,
    },
    Finish {
        session_id: String,
        stop_reason: String,
    },
    TitleUpdate {
        title: String,
    },
    ToolApprovalRequired {
        id: String,
        name: String,
        arguments: Value,
    },
    ToolApproved {
        id: String,
    },
    ToolDenied {
        id: String,
    },
    SteeringInjected {
        pending_id: Option<String>,
        message: String,
    },
    Error {
        error: String,
    },
    AgentBackgroundStarted {
        payload: Value,
    },
    AgentBackgroundCompleted {
        payload: Value,
    },
    UserMessage {
        title: Option<String>,
        message: String,
        level: String,
    },
    ClassifierDecision {
        tool_name: String,
        decision: String,
        reason: String,
        stage: u8,
    },
    Other {
        event_type: String,
        payload: Value,
    },
}

impl ChatStreamEvent {
    pub fn from_json_value(value: Value) -> Self {
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        match event_type.as_str() {
            "text_delta" => Self::TextDelta {
                delta: string_field(&value, "delta"),
            },
            "text_delta_with_citations" => Self::TextDeltaWithCitations {
                delta: string_field(&value, "delta"),
                citations: citations_from_value(value.get("citations")),
            },
            "thinking_delta" => Self::ThinkingDelta {
                thinking: string_field(&value, "thinking"),
            },
            "thinking_complete" => Self::ThinkingComplete {
                thinking: string_field(&value, "thinking"),
                signature: string_field(&value, "signature"),
            },
            "tool_call_start" => Self::ToolCallStart {
                id: string_field(&value, "id"),
                name: string_field(&value, "name"),
            },
            "tool_call_complete" => Self::ToolCallComplete {
                id: string_field(&value, "id"),
                name: string_field(&value, "name"),
                arguments: value.get("arguments").cloned().unwrap_or(Value::Null),
            },
            "tool_executing" => Self::ToolExecuting {
                id: string_field(&value, "id"),
                name: string_field(&value, "name"),
            },
            "tool_output_delta" => Self::ToolOutputDelta {
                id: string_field(&value, "id"),
                delta: string_field(&value, "delta"),
            },
            "tool_result" => Self::ToolResult {
                id: string_field(&value, "id"),
                output: string_field(&value, "output"),
                is_error: bool_field(&value, "is_error"),
            },
            "server_tool_start" => Self::ServerToolStart {
                id: string_field(&value, "id"),
                name: string_field(&value, "name"),
            },
            "server_tool_complete" => Self::ServerToolComplete {
                id: string_field(&value, "id"),
                name: string_field(&value, "name"),
            },
            "server_tool_error" => Self::ServerToolError {
                tool_use_id: string_field(&value, "tool_use_id"),
                error_code: string_field(&value, "error_code"),
            },
            "web_search_results" => Self::WebSearchResults {
                tool_use_id: string_field(&value, "tool_use_id"),
                payload: value,
            },
            "web_fetch_result" => Self::WebFetchResult {
                tool_use_id: string_field(&value, "tool_use_id"),
                payload: value,
            },
            "delegated_progress" => serde_json::from_value(value.clone())
                .map(|payload| Self::DelegatedProgress { payload })
                .unwrap_or_else(|_| Self::Other {
                    event_type: event_type.clone(),
                    payload: value,
                }),
            "delegation_event" => value
                .get("event")
                .cloned()
                .and_then(|event| serde_json::from_value(event).ok())
                .map(|event| Self::DelegationEvent { event })
                .unwrap_or_else(|| Self::Other {
                    event_type: event_type.clone(),
                    payload: value,
                }),
            "awaiting_input" => Self::AwaitingInput {
                tool_call_id: string_field(&value, "tool_call_id"),
                tool_name: string_field(&value, "tool_name"),
            },
            "mode_change" => Self::ModeChange {
                mode: string_field(&value, "mode"),
                reason: optional_string_field(&value, "reason"),
            },
            "plan_update" => Self::PlanUpdate {
                items: value
                    .get("items")
                    .and_then(|items| serde_json::from_value(items.clone()).ok())
                    .unwrap_or_default(),
            },
            "plan_complete" => Self::PlanComplete {
                tool_call_id: string_field(&value, "tool_call_id"),
                title: string_field(&value, "title"),
                task_count: usize_field(&value, "task_count"),
            },
            "agent_sleeping" => Self::AgentSleeping {
                duration_secs: u64_field(&value, "duration_secs"),
                reason: string_field(&value, "reason"),
            },
            "turn_complete" => Self::TurnComplete {
                turn: usize_field(&value, "turn"),
                has_more: bool_field(&value, "has_more"),
            },
            "tick_injected" => Self::TickInjected {
                tick_number: usize_field(&value, "tick_number"),
            },
            "usage" => {
                let prompt_tokens = usize_field(&value, "prompt_tokens");
                let completion_tokens = usize_field(&value, "completion_tokens");
                let reasoning_tokens =
                    usize_field(&value, "reasoning_tokens").min(completion_tokens);
                let cache_creation_input_tokens =
                    usize_field(&value, "cache_creation_input_tokens");
                let cache_read_input_tokens = usize_field(&value, "cache_read_input_tokens");
                let input_tokens = value
                    .get("input_tokens")
                    .and_then(Value::as_u64)
                    .map(|tokens| tokens as usize)
                    .unwrap_or_else(|| {
                        prompt_tokens
                            .saturating_add(cache_creation_input_tokens)
                            .saturating_add(cache_read_input_tokens)
                    });
                Self::Usage {
                    prompt_tokens,
                    input_tokens,
                    completion_tokens,
                    reasoning_tokens,
                    cache_creation_input_tokens,
                    cache_read_input_tokens,
                    total_tokens: usize_field(&value, "total_tokens"),
                }
            }
            "session_pinched" => Self::SessionPinched {
                reason: string_field(&value, "reason"),
                source_session_id: string_field(&value, "source_session_id"),
                new_session_id: string_field(&value, "new_session_id"),
                estimated_tokens_before: usize_field(&value, "estimated_tokens_before"),
            },
            "context_compaction_started" => Self::ContextCompactionStarted {
                reason: string_field(&value, "reason"),
            },
            "context_compacted" => Self::ContextCompacted { payload: value },
            "lagged" => Self::Lagged {
                skipped: usize_field(&value, "skipped"),
            },
            "finish" => Self::Finish {
                session_id: string_field(&value, "session_id"),
                stop_reason: string_field(&value, "stop_reason"),
            },
            "title_update" => Self::TitleUpdate {
                title: string_field(&value, "title"),
            },
            "tool_approval_required" => Self::ToolApprovalRequired {
                id: string_field(&value, "id"),
                name: string_field(&value, "name"),
                arguments: value.get("arguments").cloned().unwrap_or(Value::Null),
            },
            "tool_approved" => Self::ToolApproved {
                id: string_field(&value, "id"),
            },
            "tool_denied" => Self::ToolDenied {
                id: string_field(&value, "id"),
            },
            "steering_injected" => Self::SteeringInjected {
                pending_id: optional_string_field(&value, "pending_id"),
                message: string_field(&value, "message"),
            },
            "error" => Self::Error {
                error: string_field(&value, "error"),
            },
            "agent_background_started" => Self::AgentBackgroundStarted { payload: value },
            "agent_background_completed" => Self::AgentBackgroundCompleted { payload: value },
            "user_message" => Self::UserMessage {
                title: optional_string_field(&value, "title"),
                message: string_field(&value, "message"),
                level: string_field(&value, "level"),
            },
            "classifier_decision" => Self::ClassifierDecision {
                tool_name: string_field(&value, "tool_name"),
                decision: string_field(&value, "decision"),
                reason: string_field(&value, "reason"),
                stage: usize_field(&value, "stage") as u8,
            },
            _ => Self::Other {
                event_type,
                payload: value,
            },
        }
    }
}

fn string_field(value: &Value, field: &str) -> String {
    optional_string_field(value, field).unwrap_or_default()
}

fn optional_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn bool_field(value: &Value, field: &str) -> bool {
    value.get(field).and_then(Value::as_bool).unwrap_or(false)
}

fn usize_field(value: &Value, field: &str) -> usize {
    value.get(field).and_then(Value::as_u64).unwrap_or(0) as usize
}

fn u64_field(value: &Value, field: &str) -> u64 {
    value.get(field).and_then(Value::as_u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        ChatStreamEvent, ContentBlock, DelegatedProgressStatus, DelegatedRunRole,
        DelegationEventKind, DelegationExecutionMode, DelegationParentContinuationState, FastMode,
        HealthResponse, HiveCurrentResponse, HiveRunPriority, HiveScheduleRecurrence,
        HiveScheduleSummary, ModelInfo, ReasoningControl, ReasoningEffort, SessionStateResponse,
        SessionType, SteerRequest, ThinkingLevel,
    };
    use serde_json::json;

    #[test]
    fn health_response_reads_versioned_desktop_capabilities() {
        let response: HealthResponse = serde_json::from_value(json!({
            "status": "ok",
            "identity": "mitsuro-server",
            "pid": 42,
            "version": "0.10.1",
            "api_protocol": 1,
            "capabilities": {
                "desktop_canvases": 1,
                "terminal_sessions": 2
            },
            "features": {"chat": false}
        }))
        .expect("current health payload");

        assert_eq!(response.identity, "mitsuro-server");
        assert_eq!(response.pid, 42);
        assert_eq!(response.api_protocol, 1);
        assert!(response.supports_capability("desktop_canvases", 1));
        assert!(response.supports_capability("terminal_sessions", 2));
        assert!(!response.supports_capability("terminal_sessions", 3));
        assert!(!response.supports_capability("unknown", 1));
    }

    #[test]
    fn health_response_remains_compatible_with_legacy_servers() {
        let response: HealthResponse = serde_json::from_value(json!({
            "status": "ok",
            "version": "0.9.0",
            "features": {"chat": true}
        }))
        .expect("legacy health payload");

        assert!(response.identity.is_empty());
        assert_eq!(response.pid, 0);
        assert_eq!(response.api_protocol, 0);
        assert!(response.capabilities.is_empty());
        assert!(response.features["chat"]);
    }

    #[test]
    fn steer_request_uses_the_chat_steering_contract() {
        let request = SteerRequest {
            session_id: "session-1".to_owned(),
            message: "focus on the failing test".to_owned(),
            content: vec![ContentBlock::Text {
                text: "extra context".to_owned(),
            }],
        };
        let value = serde_json::to_value(request).unwrap();
        assert_eq!(value["session_id"], "session-1");
        assert_eq!(value["message"], "focus on the failing test");
        assert_eq!(value["content"][0]["type"], "text");
    }

    #[test]
    fn hive_read_models_accept_the_server_control_plane_shape() {
        let current: HiveCurrentResponse = serde_json::from_value(json!({
            "status": {
                "home_status": "idle", "total_count": 1, "running_count": 0,
                "sleeping_count": 0, "scheduled_count": 0, "paused_count": 0,
                "failed_count": 0, "idle_count": 1, "pending_approvals_count": 0,
                "next_wake_at": null, "high_priority_count": 0, "waiting_count": 0
            },
            "diagnostics": {"health_state": "healthy"},
            "runs": [{
                "session_id": "hive-1", "title": "Audit", "updated_at": "now",
                "project_dir": null, "target_branch": null, "agent_state": "idle",
                "runtime": {
                    "session_id": "hive-1", "status": "error", "next_wake_at": null,
                    "sleep_reason": null, "last_error": "provider unavailable",
                    "current_run_id": "run-1", "last_wake_reason": "dispatch",
                    "crew_slug": "release", "priority": "high", "updated_at": "now"
                }, "pending_tasks": 0, "in_progress_tasks": 0,
                "completed_tasks": 2, "failed_tasks": 0, "blocked_tasks": 0,
                "cadence": {"tick_interval_secs": 60, "max_ticks": 10},
                "diagnostic": null
            }],
            "approvals": []
        }))
        .expect("Hive current response");
        assert_eq!(current.runs[0].session_id, "hive-1");
        let runtime = current.runs[0].runtime.as_ref().expect("typed runtime");
        assert_eq!(runtime.status, "error");
        assert_eq!(runtime.last_error.as_deref(), Some("provider unavailable"));
        assert_eq!(runtime.priority, HiveRunPriority::High);

        let schedules: Vec<HiveScheduleSummary> = serde_json::from_value(json!([{
            "id": "schedule-1", "controller_id": "controller-1", "title": "Sweep",
            "summary": "Nightly", "objective": "Inspect",
            "recurrence": {"kind": "daily", "start_date": "2026-08-10", "time": "02:00:00"},
            "timezone": "UTC", "dst_policy": {"gap": "shift_forward", "fold": "first"},
            "next_fire_at": null, "last_scheduled_for": null, "status": "enabled", "priority": 0,
            "project_dir": null, "model": null, "model_key": null,
            "model_catalog_revision": null, "crew_slug": null,
            "misfire": {"policy": "fire_once", "grace_secs": 300, "catch_up_limit": 3},
            "overlap_policy": "skip",
            "retry": {"max_attempts": 5, "base_delay_secs": 15, "max_delay_secs": 900, "jitter": "full"},
            "revision": 1,
            "created_by": "user", "created_at": "now", "updated_at": "now",
            "controller_session_id": "hive-1"
        }]))
        .expect("Hive schedule list");
        assert_eq!(schedules[0].controller_session_id, "hive-1");
        assert!(matches!(
            schedules[0].recurrence,
            HiveScheduleRecurrence::Daily { .. }
        ));
    }

    #[test]
    fn session_type_accepts_deprecated_wire_value_and_emits_canonical_value() {
        assert_eq!(
            serde_json::from_str::<SessionType>(r#""mako""#).expect("deprecated session type"),
            SessionType::Hive
        );
        assert_eq!(
            serde_json::to_string(&SessionType::Hive).expect("canonical session type"),
            r#""hive""#
        );
    }

    #[test]
    fn usage_parses_explicit_logical_input_tokens() {
        let event = ChatStreamEvent::from_json_value(json!({
            "type": "usage",
            "prompt_tokens": 100,
            "input_tokens": 1_000,
            "completion_tokens": 50,
            "reasoning_tokens": 40,
            "cache_creation_input_tokens": 200,
            "cache_read_input_tokens": 700,
            "total_tokens": 1_050
        }));

        assert!(matches!(
            event,
            ChatStreamEvent::Usage {
                input_tokens: 1_000,
                reasoning_tokens: 40,
                ..
            }
        ));
    }

    #[test]
    fn usage_derives_input_tokens_for_older_servers() {
        let event = ChatStreamEvent::from_json_value(json!({
            "type": "usage",
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "cache_creation_input_tokens": 200,
            "cache_read_input_tokens": 700,
            "total_tokens": 1_050
        }));

        assert!(matches!(
            event,
            ChatStreamEvent::Usage {
                input_tokens: 1_000,
                reasoning_tokens: 0,
                ..
            }
        ));
    }

    #[test]
    fn session_state_parses_typed_durable_delegation_contracts() {
        let state: SessionStateResponse = serde_json::from_value(json!({
            "id": "session-1",
            "agent_state": "idle",
            "mode": "build",
            "permission_mode": "supervised",
            "delegated_tools": [],
            "recent_delegated_runs": [],
            "delegated_run_summaries": [{
                "delegated_run_id": "run-1",
                "parent_tool_call_id": "tool-1",
                "kind": "plan",
                "stage": "complete",
                "child_name": "planner",
                "capabilities": ["read"],
                "updated_at": "2026-08-08T00:00:00Z"
            }],
            "delegation_groups": [{
                "delegation_group_id": "group-1",
                "parent_tool_call_id": "tool-1",
                "state": "running",
                "execution_mode": "detached",
                "parent_continuation_state": "pending",
                "tasks": [{
                    "delegation_task_id": "task-1",
                    "task_key": "plan",
                    "role": "planner",
                    "state": "leased",
                    "attempt_count": 1,
                    "updated_at": "2026-08-08T00:00:00Z"
                }],
                "updated_at": "2026-08-08T00:00:00Z"
            }],
            "delegation_events": [{
                "event_id": 42,
                "parent_session_id": "session-1",
                "delegation_group_id": "group-1",
                "delegation_task_id": "task-1",
                "event_type": "task_claimed",
                "payload": {},
                "created_at": "2026-08-08T00:00:00Z"
            }],
            "delegation_event_cursor": 42
        }))
        .expect("typed delegation session state");

        assert_eq!(
            state.delegation_groups[0].execution_mode,
            DelegationExecutionMode::Detached
        );
        assert_eq!(
            state.delegation_groups[0].parent_continuation_state,
            DelegationParentContinuationState::Pending
        );
        assert_eq!(
            state.delegation_groups[0].tasks[0].role,
            DelegatedRunRole::Planner
        );
        assert_eq!(
            state.delegation_events[0].event_type,
            DelegationEventKind::TaskClaimed
        );
        assert_eq!(state.delegated_run_summaries[0].delegated_run_id, "run-1");
    }

    #[test]
    fn delegated_progress_stream_event_is_fully_typed() {
        let event = ChatStreamEvent::from_json_value(json!({
            "type": "delegated_progress",
            "delegated_run_id": "run-1",
            "tool_call_id": "tool-1",
            "kind": "build",
            "stage": "running",
            "parent_session_id": "session-1",
            "task_id": "task-1",
            "agent_name": "builder",
            "status": "retrying",
            "tool_count": 3,
            "tokens": 1200,
            "current_action": "testing",
            "completion_summary": null,
            "lines_added": 10,
            "lines_removed": 2,
            "completed_plan_task": null
        }));

        match event {
            ChatStreamEvent::DelegatedProgress { payload } => {
                assert_eq!(payload.status, DelegatedProgressStatus::Retrying);
                assert_eq!(payload.tool_count, 3);
                assert_eq!(payload.current_action.as_deref(), Some("testing"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn delegation_event_stream_preserves_unknown_durable_event_kinds() {
        let event = ChatStreamEvent::from_json_value(json!({
            "type": "delegation_event",
            "event": {
                "event_id": 43,
                "parent_session_id": "session-1",
                "delegation_group_id": "group-1",
                "delegation_task_id": null,
                "event_type": "future_scheduler_event",
                "payload": {"domain": "workspace"},
                "created_at": "2026-08-08T00:00:01Z"
            }
        }));

        match event {
            ChatStreamEvent::DelegationEvent { event } => {
                assert_eq!(
                    event.event_type,
                    DelegationEventKind::Other("future_scheduler_event".to_owned())
                );
                assert_eq!(event.payload["domain"], "workspace");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn model_capabilities_drive_reasoning_cycle_and_fast_support() {
        let model = ModelInfo {
            key: None,
            id: "gpt-test".to_string(),
            display_name: "GPT Test".to_string(),
            provider: "OpenAI".to_string(),
            context_window: 128_000,
            max_output: 16_384,
            supports_thinking: true,
            reasoning_control: Some(ReasoningControl::OpenAiEffort),
            supported_reasoning_levels: vec![
                ReasoningEffort::Minimal,
                ReasoningEffort::Low,
                ReasoningEffort::Medium,
                ReasoningEffort::High,
                ReasoningEffort::XHigh,
                ReasoningEffort::Max,
                ReasoningEffort::Ultra,
            ],
            default_reasoning_level: Some(ReasoningEffort::High),
            reasoning_is_mandatory: true,
            supports_fast_mode: true,
            fast_mode: Some(FastMode::Priority),
            supports_tools: true,
            supports_vision: true,
        };

        assert_eq!(
            model.selectable_thinking_levels(),
            vec![
                ThinkingLevel::Minimal,
                ThinkingLevel::Low,
                ThinkingLevel::Medium,
                ThinkingLevel::High,
                ThinkingLevel::XHigh,
                ThinkingLevel::Max
            ]
        );
        assert_eq!(
            model.normalize_thinking_level(ThinkingLevel::Off),
            ThinkingLevel::High
        );
        assert_eq!(
            ThinkingLevel::XHigh.cycle_for_model(&model),
            ThinkingLevel::Max
        );
        assert_eq!(
            ThinkingLevel::Max.cycle_for_model(&model),
            ThinkingLevel::Minimal
        );
        assert!(model.supports_fast_mode);
    }

    #[test]
    fn legacy_reasoning_metadata_keeps_a_safe_selectable_fallback() {
        let mut model = ModelInfo {
            key: None,
            id: "legacy-model".to_string(),
            display_name: "Legacy Model".to_string(),
            provider: "Legacy".to_string(),
            context_window: 0,
            max_output: 0,
            supports_thinking: true,
            reasoning_control: Some(ReasoningControl::AnthropicBudget),
            supported_reasoning_levels: Vec::new(),
            default_reasoning_level: None,
            reasoning_is_mandatory: false,
            supports_fast_mode: false,
            fast_mode: None,
            supports_tools: false,
            supports_vision: false,
        };

        assert_eq!(
            model.selectable_thinking_levels(),
            vec![ThinkingLevel::Off, ThinkingLevel::Medium]
        );

        model.reasoning_is_mandatory = true;
        model.default_reasoning_level = Some(ReasoningEffort::High);
        assert_eq!(
            model.selectable_thinking_levels(),
            vec![ThinkingLevel::High]
        );
        assert_eq!(
            model.normalize_thinking_level(ThinkingLevel::Off),
            ThinkingLevel::High
        );

        model.supported_reasoning_levels = vec![ReasoningEffort::None];
        model.default_reasoning_level = None;
        assert_eq!(
            model.selectable_thinking_levels(),
            vec![ThinkingLevel::Medium]
        );

        model.reasoning_control = Some(ReasoningControl::OutputOnly);
        assert_eq!(model.selectable_thinking_levels(), vec![ThinkingLevel::Off]);
        assert_eq!(
            model.normalize_thinking_level(ThinkingLevel::High),
            ThinkingLevel::Off
        );
    }
}
