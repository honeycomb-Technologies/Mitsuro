use mitsuro_core::ai::models::ModelKey;
use mitsuro_core::storage::{
    DelegatedRunRecord, DelegatedRunRole, DelegatedRunScope, DelegatedRunSummary,
    DelegationEventRecord, DelegationExecutionMode, DelegationGroupRecord, DelegationGroupState,
    DelegationParentContinuationState, DelegationTaskState, PartialAssistantState,
    PendingInteractionSnapshot, RuntimeTraceEvent, RuntimeTraceSummary, SessionInfo,
    SessionRecoveryState, SessionType, WorkMode, WorkspaceMode,
};
use mitsuro_core::tools::registry::PermissionMode;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeSet;

use super::{DelegatedProgressStatus, DelegatedRunStage, DelegatedToolKind};

// ============================================================================
// Session Types
// ============================================================================

#[derive(Deserialize)]
pub struct CreateSessionRequest {
    pub title: Option<String>,
    pub model: Option<String>,
    #[serde(default)]
    pub model_key: Option<ModelKey>,
    pub project_dir: Option<String>,
    pub working_dir: Option<String>,
    pub workspace_mode: Option<WorkspaceMode>,
    pub target_branch: Option<String>,
    #[serde(default)]
    pub session_type: Option<SessionType>,
    #[serde(default)]
    pub permission_mode: Option<PermissionMode>,
}

#[derive(Deserialize)]
pub struct UpdateSessionRequest {
    pub title: Option<String>,
    pub project_dir: Option<String>,
    pub working_dir: Option<String>,
    pub workspace_mode: Option<WorkspaceMode>,
    pub mode: Option<WorkMode>,
    pub model: Option<String>,
    #[serde(default)]
    pub model_key: Option<ModelKey>,
    #[serde(
        default,
        alias = "targetBranch",
        deserialize_with = "deserialize_target_branch_update"
    )]
    pub target_branch: Option<Option<String>>,
    pub permission_mode: Option<PermissionMode>,
    pub pinned: Option<bool>,
    pub archived: Option<bool>,
}

fn deserialize_target_branch_update<'de, D>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::SessionTypeResponse;
    use crate::legacy_identity::SessionWireFormat;
    use mitsuro_core::storage::SessionType;

    use serde_json::json;

    use super::UpdateSessionRequest;

    #[test]
    fn update_session_request_accepts_null_target_branch_clear() {
        let req: UpdateSessionRequest = serde_json::from_value(json!({
            "target_branch": null,
        }))
        .expect("request should deserialize");

        assert_eq!(req.target_branch, Some(None));
    }

    #[test]
    fn update_session_request_accepts_camel_case_target_branch_alias() {
        let req: UpdateSessionRequest = serde_json::from_value(json!({
            "targetBranch": "feature/mobile-continuation",
        }))
        .expect("request should deserialize");

        assert_eq!(
            req.target_branch
                .as_ref()
                .and_then(|branch| branch.as_deref()),
            Some("feature/mobile-continuation")
        );
    }

    #[test]
    fn update_session_request_accepts_pin_and_archive_changes() {
        let req: UpdateSessionRequest = serde_json::from_value(json!({
            "pinned": true,
            "archived": false,
        }))
        .expect("request should deserialize");

        assert_eq!(req.pinned, Some(true));
        assert_eq!(req.archived, Some(false));
    }

    #[test]
    fn generic_session_type_projection_is_typed_and_negotiated() {
        let legacy = SessionTypeResponse::new(SessionType::Hive, SessionWireFormat::Legacy);
        let canonical = SessionTypeResponse::new(SessionType::Hive, SessionWireFormat::Canonical);
        assert_eq!(serde_json::to_value(legacy).expect("legacy wire"), "mako");
        assert_eq!(
            serde_json::to_value(canonical).expect("canonical wire"),
            "hive"
        );
        assert_eq!(
            serde_json::to_value(SessionTypeResponse::new(
                SessionType::Code,
                SessionWireFormat::Legacy
            ))
            .expect("code wire"),
            "code"
        );
    }
}

#[derive(Deserialize)]
pub struct PinchRequest {
    /// Optional hints about what to preserve
    #[serde(alias = "hints")]
    pub preservation_hints: Option<String>,
    /// Optional direction for the new session
    pub direction: Option<String>,
}

#[derive(Serialize)]
pub struct PinchResponse {
    /// The compacted session (same id as source)
    pub session: SessionResponse,
    /// Summary of what was preserved
    pub summary: String,
    /// Key decisions preserved
    pub key_decisions: Vec<String>,
    /// Pending tasks carried forward
    pub pending_tasks: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_tokens_before: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_tokens_after: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaced_messages: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compaction_count: Option<u32>,
}

#[derive(Serialize)]
pub struct SessionResponse {
    pub id: String,
    pub title: String,
    pub agent_state: String,
    pub updated_at: String,
    pub token_count: Option<usize>,
    pub parent_session_id: Option<String>,
    pub working_dir: Option<String>,
    pub project_dir: Option<String>,
    pub workspace_mode: WorkspaceMode,
    pub session_type: SessionTypeResponse,
    pub mode: WorkMode,
    pub model: Option<String>,
    pub model_key: Option<ModelKey>,
    pub model_catalog_revision: Option<String>,
    pub target_branch: Option<String>,
    pub permission_mode: PermissionMode,
    pub pinned_at: Option<String>,
    pub archived_at: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct SessionTypeResponse {
    value: SessionType,
    wire_format: crate::legacy_identity::SessionWireFormat,
}

impl SessionTypeResponse {
    fn new(value: SessionType, wire_format: crate::legacy_identity::SessionWireFormat) -> Self {
        Self { value, wire_format }
    }
}

impl Serialize for SessionTypeResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match (self.value, self.wire_format) {
            (SessionType::Hive, crate::legacy_identity::SessionWireFormat::Legacy) => {
                crate::legacy_identity::HIVE_SESSION_TYPE
            }
            (SessionType::Hive, crate::legacy_identity::SessionWireFormat::Canonical) => "hive",
            (SessionType::Chat, _) => "chat",
            (SessionType::Code, _) => "code",
        };
        serializer.serialize_str(value)
    }
}

impl PartialEq<SessionType> for SessionTypeResponse {
    fn eq(&self, other: &SessionType) -> bool {
        self.value == *other
    }
}

impl SessionResponse {
    pub(crate) fn from_session(
        s: SessionInfo,
        wire_format: crate::legacy_identity::SessionWireFormat,
    ) -> Self {
        Self {
            id: s.id,
            title: s.title,
            agent_state: s.agent_state,
            updated_at: s.updated_at.to_rfc3339(),
            token_count: s.token_count,
            parent_session_id: s.parent_session_id,
            working_dir: s.working_dir,
            project_dir: s.project_dir,
            workspace_mode: s.workspace_mode,
            session_type: SessionTypeResponse::new(s.session_type, wire_format),
            mode: s.work_mode,
            model: s.model,
            model_key: s.model_key,
            model_catalog_revision: s.model_catalog_revision,
            target_branch: s.target_branch,
            permission_mode: s.permission_mode,
            pinned_at: s.pinned_at.map(|value| value.to_rfc3339()),
            archived_at: s.archived_at.map(|value| value.to_rfc3339()),
        }
    }
}

impl From<SessionInfo> for SessionResponse {
    fn from(s: SessionInfo) -> Self {
        Self::from_session(s, crate::legacy_identity::SessionWireFormat::Canonical)
    }
}

#[derive(Serialize)]
pub struct SessionWithMessagesResponse {
    pub session: SessionResponse,
    pub messages: Vec<MessageResponse>,
}

/// Agent execution state for a session
#[derive(Serialize)]
pub struct SessionStateResponse {
    /// Session ID
    pub id: String,
    /// Agent state: "idle", "streaming", "tool_executing", "awaiting_input", "error"
    pub agent_state: String,
    /// When the agent started (if not idle)
    pub started_at: Option<String>,
    /// Last event timestamp (for activity tracking)
    pub last_event_at: Option<String>,
    /// Current persisted work mode
    pub mode: WorkMode,
    /// Current persisted permission mode.
    pub permission_mode: PermissionMode,
    /// Canonical durable Goal and plan aggregate, if the session has one.
    pub workflow: Option<mitsuro_core::workflow::WorkflowSnapshot>,
    /// Interrupted-turn recovery state, if any.
    pub recovery: Option<SessionRecoveryState>,
    /// Reload-safe pending tool approvals, user questions, and plan confirmations.
    pub pending_interactions: Vec<PendingInteractionSnapshot>,
    /// Authoritative in-flight partial assistant state for active sessions.
    pub live_partial_assistant: Option<PartialAssistantState>,
    /// Active delegated tool snapshots for this session, keyed by top-level tool call.
    pub delegated_tools: Vec<DelegatedToolStateResponse>,
    /// Recent persisted delegated runs for this session.
    pub recent_delegated_runs: Vec<DelegatedRunResponse>,
    /// Compact newest-run index for delegated tool calls in the hydrated
    /// transcript. Unlike recent_delegated_runs, these rows never carry large
    /// snapshots or artifacts.
    pub delegated_run_summaries: Vec<DelegatedRunSummaryResponse>,
    /// Canonical group/task snapshots. These deliberately exclude objectives,
    /// result artifacts, tool output, and error text.
    pub delegation_groups: Vec<DelegationGroupStateResponse>,
    /// Cursor-replay window from the append-only delegation event stream.
    pub delegation_events: Vec<DelegationEventRecord>,
    pub delegation_event_cursor: Option<i64>,
    /// Latest persisted runtime trace sequence observed for this session.
    pub last_event_sequence: Option<i64>,
    /// Cursor for the process-local canonical Code Agent live/replay stream.
    pub agent_stream_generation: u64,
    pub agent_stream_sequence: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegationGroupStateResponse {
    pub delegation_group_id: String,
    pub parent_tool_call_id: Option<String>,
    pub state: DelegationGroupState,
    pub execution_mode: DelegationExecutionMode,
    pub max_parallelism: usize,
    pub effective_parallelism: usize,
    pub parent_continuation_state: DelegationParentContinuationState,
    pub tasks: Vec<DelegationTaskStateResponse>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegationTaskStateResponse {
    pub delegation_task_id: String,
    pub task_key: String,
    pub role: DelegatedRunRole,
    pub objective: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub working_dir: Option<String>,
    pub state: DelegationTaskState,
    pub attempt_count: usize,
    pub integration_state: Option<String>,
    pub depends_on: Vec<String>,
    pub write_intent: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

fn effective_parallelism(group: &DelegationGroupRecord) -> usize {
    let tasks = group
        .tasks
        .iter()
        .map(|task| {
            (
                task.specification.task_key.clone(),
                task.specification.depends_on.clone(),
            )
        })
        .collect::<Vec<_>>();
    effective_parallelism_for(&tasks, group.contract.governance.max_parallelism)
}

fn effective_parallelism_for(tasks: &[(String, Vec<String>)], max_parallelism: usize) -> usize {
    let mut remaining = tasks
        .iter()
        .map(|(task_key, _)| task_key.clone())
        .collect::<BTreeSet<_>>();
    let mut settled = BTreeSet::new();
    let mut widest_wave = 0usize;
    while !remaining.is_empty() {
        let ready = tasks
            .iter()
            .filter(|(task_key, _)| remaining.contains(task_key))
            .filter(|(_, depends_on)| {
                depends_on
                    .iter()
                    .all(|dependency| settled.contains(dependency))
            })
            .map(|(task_key, _)| task_key.clone())
            .collect::<Vec<_>>();
        if ready.is_empty() {
            break;
        }
        widest_wave = widest_wave.max(ready.len());
        for task_key in ready {
            remaining.remove(&task_key);
            settled.insert(task_key);
        }
    }
    widest_wave.max(1).min(max_parallelism.max(1))
}

impl From<DelegationGroupRecord> for DelegationGroupStateResponse {
    fn from(group: DelegationGroupRecord) -> Self {
        let effective_parallelism = effective_parallelism(&group);
        Self {
            delegation_group_id: group.delegation_group_id,
            parent_tool_call_id: group.parent_tool_call_id,
            state: group.state,
            execution_mode: group.contract.execution_mode,
            max_parallelism: group.contract.governance.max_parallelism,
            effective_parallelism,
            parent_continuation_state: group.parent_continuation_state,
            tasks: group
                .tasks
                .into_iter()
                .map(|task| {
                    let provider = task
                        .specification
                        .executor_envelope
                        .as_ref()
                        .map(|envelope| envelope.provider_id.clone());
                    let model = task
                        .specification
                        .executor_envelope
                        .as_ref()
                        .map(|envelope| envelope.resolved_model.clone());
                    let working_dir = task
                        .specification
                        .executor_envelope
                        .as_ref()
                        .map(|envelope| envelope.working_dir.clone());
                    let objective = task.specification.objective.chars().take(320).collect();
                    let integration_state = task
                        .result
                        .as_ref()
                        .and_then(|result| result.get("integration_state"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|state| matches!(*state, "pending" | "ready" | "failed"))
                        .map(ToString::to_string);
                    DelegationTaskStateResponse {
                        delegation_task_id: task.specification.delegation_task_id,
                        task_key: task.specification.task_key,
                        role: task.specification.role,
                        objective,
                        provider,
                        model,
                        working_dir,
                        state: task.state,
                        attempt_count: task.attempt_count,
                        integration_state,
                        depends_on: task.specification.depends_on,
                        write_intent: task.specification.write_intent,
                        created_at: task.created_at.to_rfc3339(),
                        updated_at: task.updated_at.to_rfc3339(),
                        completed_at: task.completed_at.map(|value| value.to_rfc3339()),
                    }
                })
                .collect(),
            created_at: group.created_at.to_rfc3339(),
            updated_at: group.updated_at.to_rfc3339(),
            completed_at: group.completed_at.map(|value| value.to_rfc3339()),
        }
    }
}

#[cfg(test)]
mod delegation_projection_tests {
    use super::effective_parallelism_for;

    #[test]
    fn effective_parallelism_reflects_dependency_waves_and_capacity() {
        let chain = vec![
            ("plan".to_string(), vec![]),
            ("build".to_string(), vec!["plan".to_string()]),
            ("verify".to_string(), vec!["build".to_string()]),
        ];
        assert_eq!(effective_parallelism_for(&chain, 8), 1);

        let fan_out = vec![
            ("plan".to_string(), vec![]),
            ("ui".to_string(), vec!["plan".to_string()]),
            ("server".to_string(), vec!["plan".to_string()]),
            ("tests".to_string(), vec!["plan".to_string()]),
        ];
        assert_eq!(effective_parallelism_for(&fan_out, 8), 3);
        assert_eq!(effective_parallelism_for(&fan_out, 2), 2);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegatedAgentStateResponse {
    pub task_id: String,
    pub agent_name: String,
    pub status: DelegatedProgressStatus,
    pub tool_count: usize,
    pub tokens: usize,
    pub current_action: Option<String>,
    pub completion_summary: Option<String>,
    pub lines_added: usize,
    pub lines_removed: usize,
    pub completed_plan_task: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegatedToolStateResponse {
    pub delegated_run_id: String,
    pub tool_call_id: String,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    pub parent_session_id: Option<String>,
    pub agents: Vec<DelegatedAgentStateResponse>,
}

impl DelegatedToolStateResponse {
    pub(crate) fn from_active_durable_snapshot(record: &DelegatedRunRecord) -> Option<Self> {
        if !matches!(
            record.stage,
            mitsuro_core::agent::DelegatedRunStage::Created
                | mitsuro_core::agent::DelegatedRunStage::Running
                | mitsuro_core::agent::DelegatedRunStage::Synthesizing
        ) {
            return None;
        }
        let tool_call_id = record.parent_tool_call_id.clone()?;
        let snapshot = record.snapshot.as_ref()?;
        let status = |status: &str| match status {
            "created" => DelegatedProgressStatus::Created,
            "queued" => DelegatedProgressStatus::Queued,
            "leased" => DelegatedProgressStatus::Leased,
            "retrying" => DelegatedProgressStatus::Retrying,
            "complete" => DelegatedProgressStatus::Complete,
            "degraded" => DelegatedProgressStatus::Degraded,
            "cancelled" => DelegatedProgressStatus::Cancelled,
            "failed" => DelegatedProgressStatus::Failed,
            _ => DelegatedProgressStatus::Running,
        };

        Some(Self {
            delegated_run_id: record.delegated_run_id.clone(),
            tool_call_id,
            kind: match record.role {
                mitsuro_core::storage::DelegatedRunRole::Explore => DelegatedToolKind::Explore,
                mitsuro_core::storage::DelegatedRunRole::Planner => DelegatedToolKind::Plan,
                mitsuro_core::storage::DelegatedRunRole::Verifier => DelegatedToolKind::Verify,
                mitsuro_core::storage::DelegatedRunRole::Build => DelegatedToolKind::Build,
            },
            stage: DelegatedRunStage::from(record.stage),
            parent_session_id: Some(record.parent_session_id.clone()),
            agents: snapshot
                .agents
                .iter()
                .map(|agent| DelegatedAgentStateResponse {
                    task_id: agent.task_id.clone(),
                    agent_name: agent.agent_name.clone(),
                    status: status(&agent.status),
                    tool_count: agent.tool_count,
                    tokens: agent.tokens,
                    current_action: agent.current_action.clone(),
                    completion_summary: agent.completion_summary.clone(),
                    lines_added: agent.lines_added,
                    lines_removed: agent.lines_removed,
                    completed_plan_task: agent.completed_plan_task.clone(),
                })
                .collect(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegatedRunScopeResponse {
    pub label: String,
    pub path: String,
    pub kind: String,
}

impl From<DelegatedRunScope> for DelegatedRunScopeResponse {
    fn from(value: DelegatedRunScope) -> Self {
        Self {
            label: value.label,
            path: value.path,
            kind: value.kind,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegatedRunResponse {
    pub delegated_run_id: String,
    pub parent_tool_call_id: Option<String>,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub resumable: bool,
    pub resumed_from_run_id: Option<String>,
    pub child_name: Option<String>,
    pub capabilities: Vec<String>,
    pub target_scope: Vec<DelegatedRunScopeResponse>,
    pub human_review: Option<String>,
    pub artifact: Option<Value>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DelegatedRunSummaryResponse {
    pub delegated_run_id: String,
    pub parent_tool_call_id: String,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    pub child_name: Option<String>,
    pub capabilities: Vec<String>,
    pub updated_at: String,
}

impl From<DelegatedRunSummary> for DelegatedRunSummaryResponse {
    fn from(value: DelegatedRunSummary) -> Self {
        let capabilities = value
            .effective_capabilities()
            .into_iter()
            .map(|capability| match capability {
                mitsuro_core::agent::subagent::AgentCapability::Read => "read".to_string(),
                mitsuro_core::agent::subagent::AgentCapability::Write => "write".to_string(),
                mitsuro_core::agent::subagent::AgentCapability::Execute => "execute".to_string(),
            })
            .collect();
        Self {
            delegated_run_id: value.delegated_run_id,
            parent_tool_call_id: value.parent_tool_call_id,
            kind: match value.role {
                mitsuro_core::storage::DelegatedRunRole::Explore => DelegatedToolKind::Explore,
                mitsuro_core::storage::DelegatedRunRole::Planner => DelegatedToolKind::Plan,
                mitsuro_core::storage::DelegatedRunRole::Verifier => DelegatedToolKind::Verify,
                mitsuro_core::storage::DelegatedRunRole::Build => DelegatedToolKind::Build,
            },
            stage: DelegatedRunStage::from(value.stage),
            child_name: value.child_name,
            capabilities,
            updated_at: value.updated_at.to_rfc3339(),
        }
    }
}

impl From<DelegatedRunRecord> for DelegatedRunResponse {
    fn from(value: DelegatedRunRecord) -> Self {
        let capabilities = value
            .effective_capabilities()
            .into_iter()
            .map(|capability| match capability {
                mitsuro_core::agent::subagent::AgentCapability::Read => "read".to_string(),
                mitsuro_core::agent::subagent::AgentCapability::Write => "write".to_string(),
                mitsuro_core::agent::subagent::AgentCapability::Execute => "execute".to_string(),
            })
            .collect();
        Self {
            delegated_run_id: value.delegated_run_id,
            parent_tool_call_id: value.parent_tool_call_id,
            kind: match value.role {
                mitsuro_core::storage::DelegatedRunRole::Explore => DelegatedToolKind::Explore,
                mitsuro_core::storage::DelegatedRunRole::Planner => DelegatedToolKind::Plan,
                mitsuro_core::storage::DelegatedRunRole::Verifier => DelegatedToolKind::Verify,
                mitsuro_core::storage::DelegatedRunRole::Build => DelegatedToolKind::Build,
            },
            stage: match value.stage {
                mitsuro_core::agent::DelegatedRunStage::Created => DelegatedRunStage::Created,
                mitsuro_core::agent::DelegatedRunStage::Running => DelegatedRunStage::Running,
                mitsuro_core::agent::DelegatedRunStage::Synthesizing => {
                    DelegatedRunStage::Synthesizing
                }
                mitsuro_core::agent::DelegatedRunStage::Complete => DelegatedRunStage::Complete,
                mitsuro_core::agent::DelegatedRunStage::Degraded => DelegatedRunStage::Degraded,
                mitsuro_core::agent::DelegatedRunStage::Failed => DelegatedRunStage::Failed,
                mitsuro_core::agent::DelegatedRunStage::Cancelled => DelegatedRunStage::Cancelled,
            },
            provider: value.provider,
            model: value.model,
            resumable: value.resumable,
            resumed_from_run_id: value.resumed_from_run_id,
            child_name: value.child_name,
            capabilities,
            target_scope: value.target_scope.into_iter().map(Into::into).collect(),
            human_review: value.human_review,
            artifact: value.artifact,
            updated_at: value.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
pub struct SessionTraceResponse {
    pub id: String,
    pub summary: RuntimeTraceSummary,
    pub events: Vec<RuntimeTraceEvent>,
    pub latest_sequence: Option<i64>,
}

#[derive(Deserialize)]
pub struct SessionPresenceHeartbeatRequest {
    pub client_id: String,
    pub surface: String,
    pub capability: crate::presence::PresenceCapability,
    pub last_event_sequence: Option<i64>,
}

#[derive(Serialize)]
pub struct SessionPresenceClientResponse {
    pub client_id: String,
    pub surface: String,
    pub capability: crate::presence::PresenceCapability,
    pub user_id: Option<String>,
    pub last_seen_at: String,
    pub last_event_sequence: Option<i64>,
    pub stale: bool,
}

#[derive(Serialize)]
pub struct SessionPresenceResponse {
    pub session_id: String,
    pub active_viewers: usize,
    pub active_controllers: usize,
    pub stale_clients: usize,
    pub clients: Vec<SessionPresenceClientResponse>,
}

/// Public identity for the client currently controlling a Code Agent. The
/// bearer-like lease token is intentionally excluded.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionControlLeaseSummary {
    pub client_id: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionControlStateResponse {
    pub session_id: String,
    pub controller: Option<SessionControlLeaseSummary>,
    pub ttl_seconds: u64,
}

/// Private mutation authority returned only to the client that takes control.
/// Route code must never include this structure in public summaries or logs.
#[derive(Clone, Serialize, PartialEq, Eq)]
pub struct SessionControlLease {
    pub resource_id: String,
    pub client_id: String,
    pub lease_token: String,
    pub expires_at: String,
}

#[derive(Deserialize)]
pub struct TakeSessionControlRequest {
    pub client_id: String,
    #[serde(default)]
    pub force: bool,
}

#[derive(Serialize)]
pub struct TakeSessionControlResponse {
    pub lease: SessionControlLease,
}

#[derive(Deserialize)]
pub struct HeartbeatSessionControlRequest {
    pub client_id: String,
    pub lease_token: String,
}

#[derive(Serialize)]
pub struct ServerAccessResponse {
    pub local_url: String,
    pub remote_access_enabled: bool,
    pub remote_access_token_available: bool,
    pub revealed_remote_access_token: Option<String>,
    pub remote_launch_url: Option<String>,
    pub tailscale: TailscaleAccessResponse,
}

#[derive(Deserialize)]
pub struct UpdateServerAccessRequest {
    pub enabled: Option<bool>,
    pub rotate_token: Option<bool>,
    pub reveal_token: Option<bool>,
}

#[derive(Serialize)]
pub struct TailscaleAccessResponse {
    pub status: String,
    pub url: Option<String>,
    pub detail: Option<String>,
}

#[derive(Serialize)]
pub struct ActiveSessionStatusResponse {
    pub id: String,
    pub title: String,
    pub agent_state: String,
    pub started_at: Option<String>,
    pub last_event_at: Option<String>,
    pub working_dir: Option<String>,
    pub project_dir: Option<String>,
    pub workspace_mode: WorkspaceMode,
    pub active_viewers: usize,
    pub active_controllers: usize,
    pub stale_clients: usize,
}

#[derive(Serialize)]
pub struct ServerStatusResponse {
    pub active_agent_streams: usize,
    pub active_sessions: Vec<ActiveSessionStatusResponse>,
    pub memory: ServerMemoryStatusResponse,
    pub tailscale: TailscaleAccessResponse,
}

#[derive(Serialize)]
pub struct ServerMemoryStatusResponse {
    pub rss_bytes: Option<u64>,
    pub virtual_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub peak_virtual_bytes: Option<u64>,
}

#[derive(Serialize)]
pub struct MessageResponse {
    pub role: String,
    pub content: serde_json::Value,
}
