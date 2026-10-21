//! Canonical construction boundary for production agent runs.
//!
//! Product surfaces should resolve their mutable inputs into a `RunSpec`
//! before starting the orchestrator. This keeps workspace identity, provider
//! request policy, and the provider cache key consistent across callers.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::ai::client::{AiClient, CallOptions};
use crate::ai::types::{Content, ModelMessage, Role};
use crate::storage::{
    HiveGroupRunContext, HiveProfileSnapshot, SessionType, WorkMode, WorkerConversationLane,
    WorkerRunOrigin,
};
use crate::tools::registry::PermissionMode;
use crate::workflow::{
    AttemptStatus, CollaborationMode, GoalStatus, PlanRevisionStatus, WorkflowSnapshot,
    WorkflowStepStatus,
};

use super::loop_events::{LoopEvent, LoopInput, LoopStopReason};
use super::orchestrator::{AgenticOrchestrator, OrchestratorConfig, OrchestratorServices};
use super::state::RunBudget;
use super::{
    DelegatedProgressEvent, WorkerConversationResponseCommitter, WorkerGoalAttemptOutcome,
    WorkerGoalEffectSummary, WorkerGoalEvidence, WorkerGoalOutcomeCommitInput,
    WorkerGoalOutcomeCommitter, WorkerGoalOutcomeCounters, WorkerGoalOutcomeInputError,
    WorkerProviderCallGovernor,
};

/// Product surface that resolved a canonical run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunProvenance {
    Server,
    Tui,
    Acp,
    #[serde(alias = "mako")]
    Hive,
    Delegated,
}

/// Execution kernel owned by a run surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunKernel {
    StreamingOrchestrator,
    DelegatedToolLoop,
}

/// Built-in tools that a durable Worker Goal may explicitly request.
///
/// This is a capability ceiling, not a default grant. The Hive runtime must
/// still provide an explicit per-run subset, the model must receive only that
/// subset, and every execution remains subject to the normal permission mode.
/// Higher-order delegation, discovery, web, skills, extensions, MCP wrappers,
/// cross-Worker messaging, and workspace switching are intentionally absent.
pub const WORKER_GOAL_TOOL_CAPABILITY_CEILING: &[&str] = &[
    "apply_patch",
    "bash",
    "edit",
    "glob",
    "grep",
    "list",
    "multiedit",
    "read",
    "write",
];

/// Immutable identities frozen when a durable Hive Worker Goal attempt is
/// claimed. The runtime constructs this from the canonical Goal/attempt/run
/// transaction; prompt text and client metadata are never authority.
#[derive(Clone, PartialEq, Eq)]
pub struct WorkerGoalExecutionBinding {
    pub worker_id: String,
    pub worker_revision: u64,
    pub owner_user_id: Option<String>,
    pub session_id: String,
    pub run_id: String,
    pub run_lease_token: String,
    pub run_lease_epoch: u64,
    pub run_origin: WorkerRunOrigin,
    pub goal_id: String,
    pub goal_revision: u64,
    pub workflow_aggregate_revision: u64,
    pub attempt_id: String,
    pub plan_revision_id: String,
    pub plan_revision_number: u64,
    pub step_id: String,
    pub step_revision: u64,
    pub workspace_dir: PathBuf,
}

impl std::fmt::Debug for WorkerGoalExecutionBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerGoalExecutionBinding")
            .field("worker_id", &self.worker_id)
            .field("worker_revision", &self.worker_revision)
            .field("owner_user_id", &self.owner_user_id)
            .field("session_id", &self.session_id)
            .field("run_id", &self.run_id)
            .field("run_lease_token", &"[REDACTED]")
            .field("run_lease_epoch", &self.run_lease_epoch)
            .field("run_origin", &self.run_origin)
            .field("goal_id", &self.goal_id)
            .field("goal_revision", &self.goal_revision)
            .field(
                "workflow_aggregate_revision",
                &self.workflow_aggregate_revision,
            )
            .field("attempt_id", &self.attempt_id)
            .field("plan_revision_id", &self.plan_revision_id)
            .field("plan_revision_number", &self.plan_revision_number)
            .field("step_id", &self.step_id)
            .field("step_revision", &self.step_revision)
            .field("workspace_dir", &self.workspace_dir)
            .finish()
    }
}

/// Frozen context payload for one bounded durable Hive Worker Goal attempt.
///
/// The snapshot is rendered through a bounded, purpose-built context path. It
/// is never routed through the ordinary session plan, global Hive profile, or
/// conversation context assemblers.
#[derive(Clone)]
pub struct WorkerGoalExecutionContext {
    binding: WorkerGoalExecutionBinding,
    workflow_snapshot: Arc<WorkflowSnapshot>,
}

impl WorkerGoalExecutionContext {
    pub fn new(
        binding: WorkerGoalExecutionBinding,
        workflow_snapshot: Arc<WorkflowSnapshot>,
    ) -> Self {
        Self {
            binding,
            workflow_snapshot,
        }
    }

    pub fn binding(&self) -> &WorkerGoalExecutionBinding {
        &self.binding
    }

    pub fn workflow_snapshot(&self) -> &WorkflowSnapshot {
        &self.workflow_snapshot
    }

    /// Deterministic, content-bounded trigger for this exact claimed attempt.
    ///
    /// It is provider input only. Callers must never insert it into canonical
    /// chat history or present it as a user-authored message.
    pub fn ephemeral_trigger_message(&self) -> ModelMessage {
        let step_description = self
            .workflow_snapshot
            .steps
            .iter()
            .find(|step| step.id == self.binding.step_id)
            .map(|step| step.description.as_str())
            .unwrap_or("the exact claimed step");
        ModelMessage {
            role: Role::User,
            content: vec![Content::text(format!(
                    "[WORKER GOAL TRIGGER v1]\nBegin the exact claimed durable Worker Goal attempt now. Goal id: {}. Attempt id: {}. Plan revision id: {}. Step id: {}. Objective: {}. Exact step: {}. Use only the frozen context and advertised capabilities; report concrete outcome and evidence for this attempt.",
                    self.binding.goal_id,
                    self.binding.attempt_id,
                    self.binding.plan_revision_id,
                    self.binding.step_id,
                    truncate_worker_goal_trigger_field(&self.workflow_snapshot.goal.objective),
                    truncate_worker_goal_trigger_field(step_description),
                ))],
        }
    }

    pub(crate) fn outcome_commit_input(
        &self,
        provider_call_ids: Vec<String>,
        outcome: WorkerGoalAttemptOutcome,
        evidence: Vec<WorkerGoalEvidence>,
        effect: WorkerGoalEffectSummary,
        counters: WorkerGoalOutcomeCounters,
    ) -> Result<WorkerGoalOutcomeCommitInput, WorkerGoalOutcomeInputError> {
        let binding = &self.binding;
        let attempt = self
            .workflow_snapshot
            .latest_attempt
            .as_ref()
            .expect("Worker Goal execution context is validated before orchestration");
        if counters.turns > attempt.max_turns.saturating_sub(attempt.turn_count)
            || counters.tool_calls
                > attempt
                    .max_tool_calls
                    .saturating_sub(attempt.tool_call_count)
            || counters.research_actions
                > attempt
                    .max_research_actions
                    .saturating_sub(attempt.research_action_count)
        {
            return Err(WorkerGoalOutcomeInputError::BudgetExceeded);
        }
        WorkerGoalOutcomeCommitInput::from_validated_run(
            binding.worker_id.clone(),
            binding.worker_revision,
            binding.owner_user_id.clone(),
            binding.session_id.clone(),
            binding.run_id.clone(),
            binding.run_lease_token.clone(),
            binding.run_lease_epoch,
            binding.run_origin,
            binding.goal_id.clone(),
            binding.goal_revision,
            binding.workflow_aggregate_revision,
            binding.attempt_id.clone(),
            binding.plan_revision_id.clone(),
            binding.plan_revision_number,
            binding.step_id.clone(),
            binding.step_revision,
            binding.workspace_dir.clone(),
            provider_call_ids,
            outcome,
            evidence,
            effect,
            counters,
        )
    }

    pub(crate) fn permits_additional_attempt_work(
        &self,
        observed_tool_calls: u32,
        requested_tool_calls: usize,
        observed_research_actions: u32,
        requested_research_actions: usize,
    ) -> bool {
        let Some(attempt) = self.workflow_snapshot.latest_attempt.as_ref() else {
            return false;
        };
        let Ok(requested_tool_calls) = u32::try_from(requested_tool_calls) else {
            return false;
        };
        let Ok(requested_research_actions) = u32::try_from(requested_research_actions) else {
            return false;
        };
        let remaining_tool_calls = attempt
            .max_tool_calls
            .saturating_sub(attempt.tool_call_count);
        let remaining_research_actions = attempt
            .max_research_actions
            .saturating_sub(attempt.research_action_count);
        observed_tool_calls
            .checked_add(requested_tool_calls)
            .is_some_and(|total| total <= remaining_tool_calls)
            && observed_research_actions
                .checked_add(requested_research_actions)
                .is_some_and(|total| total <= remaining_research_actions)
    }
}

fn truncate_worker_goal_trigger_field(value: &str) -> &str {
    const MAX_BYTES: usize = 768;
    if value.len() <= MAX_BYTES {
        return value;
    }
    let mut boundary = MAX_BYTES;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

impl std::fmt::Debug for WorkerGoalExecutionContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerGoalExecutionContext")
            .field("binding", &self.binding)
            .field(
                "workflow_schema_version",
                &self.workflow_snapshot.schema_version,
            )
            .finish_non_exhaustive()
    }
}

/// Request-context and execution capability profile for one orchestrator run.
///
/// `WorkerConversation` is deliberately narrower than ordinary Hive. It is a
/// single-response, tool-free conversation surface whose identity comes only
/// from the exact durable Worker/lane binding. The retained `working_dir` on
/// `RunSpec` is runtime plumbing and is never a prompt/context fallback in
/// this mode. `WorkerGoal` is a separate, bounded workspace execution surface:
/// it carries one frozen canonical Workflow attempt and an explicit small tool
/// subset, but none of ordinary Hive's ambient or higher-order capabilities.
#[derive(Clone, Default)]
pub enum RunContextMode {
    #[default]
    Standard,
    WorkerConversation {
        worker_id: String,
        response_committer: Arc<dyn WorkerConversationResponseCommitter>,
    },
    WorkerGoal {
        context: Arc<WorkerGoalExecutionContext>,
        outcome_committer: Arc<dyn WorkerGoalOutcomeCommitter>,
    },
}

impl RunContextMode {
    pub fn worker_conversation(
        worker_id: impl Into<String>,
        response_committer: Arc<dyn WorkerConversationResponseCommitter>,
    ) -> Self {
        Self::WorkerConversation {
            worker_id: worker_id.into(),
            response_committer,
        }
    }

    pub const fn is_worker_conversation(&self) -> bool {
        matches!(self, Self::WorkerConversation { .. })
    }

    pub fn worker_goal(
        context: Arc<WorkerGoalExecutionContext>,
        outcome_committer: Arc<dyn WorkerGoalOutcomeCommitter>,
    ) -> Self {
        Self::WorkerGoal {
            context,
            outcome_committer,
        }
    }

    pub const fn is_worker_goal(&self) -> bool {
        matches!(self, Self::WorkerGoal { .. })
    }

    /// Worker-scoped modes fail closed around ambient project/Hive/plugin
    /// state. This is deliberately broader than conversation-only behavior.
    pub const fn is_isolated_worker(&self) -> bool {
        matches!(
            self,
            Self::WorkerConversation { .. } | Self::WorkerGoal { .. }
        )
    }

    pub(crate) fn worker_goal_context(&self) -> Option<&WorkerGoalExecutionContext> {
        match self {
            Self::WorkerGoal { context, .. } => Some(context),
            Self::Standard | Self::WorkerConversation { .. } => None,
        }
    }

    pub(crate) fn response_committer(
        &self,
    ) -> Option<&Arc<dyn WorkerConversationResponseCommitter>> {
        match self {
            Self::Standard => None,
            Self::WorkerConversation {
                response_committer, ..
            } => Some(response_committer),
            Self::WorkerGoal { .. } => None,
        }
    }

    pub(crate) fn worker_goal_outcome_committer(
        &self,
    ) -> Option<&Arc<dyn WorkerGoalOutcomeCommitter>> {
        match self {
            Self::WorkerGoal {
                outcome_committer, ..
            } => Some(outcome_committer),
            Self::Standard | Self::WorkerConversation { .. } => None,
        }
    }
}

impl std::fmt::Debug for RunContextMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Standard => formatter.write_str("Standard"),
            Self::WorkerConversation { worker_id, .. } => formatter
                .debug_struct("WorkerConversation")
                .field("worker_id", worker_id)
                .finish_non_exhaustive(),
            Self::WorkerGoal { context, .. } => {
                formatter.debug_tuple("WorkerGoal").field(context).finish()
            }
        }
    }
}

impl RunKernel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StreamingOrchestrator => "streaming_orchestrator",
            Self::DelegatedToolLoop => "delegated_tool_loop",
        }
    }
}

impl RunProvenance {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Tui => "tui",
            Self::Acp => "acp",
            Self::Hive => "hive",
            Self::Delegated => "delegated",
        }
    }

    pub const fn kernel(self) -> RunKernel {
        match self {
            Self::Delegated => RunKernel::DelegatedToolLoop,
            Self::Server | Self::Tui | Self::Acp | Self::Hive => RunKernel::StreamingOrchestrator,
        }
    }
}

/// Invalid or internally inconsistent run input.
#[derive(Debug, Error)]
pub enum RunSpecError {
    #[error("agent run session_id must not be empty")]
    EmptySessionId,
    #[error("agent run session_id must not contain surrounding whitespace")]
    SessionIdWhitespace,
    #[error("agent run session_id contains a control character")]
    InvalidSessionId,
    #[error("{kind} must be an absolute path: '{path}'")]
    RelativeWorkspacePath { kind: &'static str, path: PathBuf },
    #[error("{kind} is not accessible as a directory: '{path}': {source}")]
    InaccessibleWorkspacePath {
        kind: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{kind} is not a directory: '{path}'")]
    WorkspacePathNotDirectory { kind: &'static str, path: PathBuf },
    #[error("project_dir '{project_dir}' must be within working_dir '{working_dir}'")]
    ProjectOutsideWorkingDirectory {
        working_dir: PathBuf,
        project_dir: PathBuf,
    },
    #[error("run surface '{provenance}' cannot execute a '{session_type}' session")]
    SurfaceSessionTypeMismatch {
        provenance: &'static str,
        session_type: SessionType,
    },
    #[error(
        "provider cache session_id '{cache_session_id}' does not match run session_id '{run_session_id}'"
    )]
    CacheSessionMismatch {
        run_session_id: String,
        cache_session_id: String,
    },
    #[error("max_tokens must be greater than zero")]
    ZeroMaxTokens,
    #[error("temperature must be finite")]
    NonFiniteTemperature,
    #[error("tool names must not be empty")]
    EmptyToolName,
    #[error("duplicate tool name in request: '{0}'")]
    DuplicateToolName(String),
    #[error("run provenance '{actual}' cannot be consumed by '{expected}' driver")]
    DriverProvenanceMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    #[error("run surface '{provenance}' is owned by the '{kernel}' kernel, not RunSpec")]
    UnsupportedKernel {
        provenance: &'static str,
        kernel: &'static str,
    },
    #[error("neutral Worker conversation context requires the Hive run surface")]
    WorkerConversationRequiresHiveSurface,
    #[error("neutral Worker conversation context requires an exact provider-governor binding")]
    WorkerConversationRequiresGovernor,
    #[error("neutral Worker conversation context has an invalid Worker id")]
    InvalidWorkerConversationId,
    #[error("neutral Worker conversation {field} does not match its provider-governor binding")]
    WorkerConversationBindingMismatch { field: &'static str },
    #[error("neutral Worker conversation cannot carry {capability}")]
    WorkerConversationForbiddenCapability { capability: &'static str },
    #[error("Worker provider governor does not match the resolved run model")]
    WorkerProviderModelMismatch,
    #[error("Worker provider governor does not match the run permission mode")]
    WorkerProviderPermissionMismatch,
    #[error("Worker provider governor database does not match the orchestrator database")]
    WorkerProviderDatabaseMismatch,
    #[error("neutral Worker conversation runs cannot be consumed by a higher-order driver")]
    WorkerConversationHigherOrderDriver,
    #[error("Worker Goal context requires the Hive run surface")]
    WorkerGoalRequiresHiveSurface,
    #[error("Worker Goal context requires an exact provider-governor binding")]
    WorkerGoalRequiresGovernor,
    #[error("Worker Goal {field} is invalid")]
    InvalidWorkerGoalBinding { field: &'static str },
    #[error("Worker Goal {field} does not match its provider-governor binding")]
    WorkerGoalGovernorBindingMismatch { field: &'static str },
    #[error("Worker Goal {field} does not match its frozen Workflow snapshot")]
    WorkerGoalSnapshotMismatch { field: &'static str },
    #[error("Worker Goal requires one exact selected workspace")]
    WorkerGoalRequiresWorkspace,
    #[error("Worker Goal workspace does not match the resolved run workspace")]
    WorkerGoalWorkspaceMismatch,
    #[error("Worker Goal requires an explicit workspace-tool allowlist")]
    WorkerGoalRequiresToolAllowlist,
    #[error("Worker Goal cannot carry {capability}")]
    WorkerGoalForbiddenCapability { capability: &'static str },
    #[error("Worker Goal tool '{tool}' is outside the capability ceiling")]
    WorkerGoalForbiddenTool { tool: String },
    #[error("Worker Goal run budget does not match its frozen Workflow attempt")]
    WorkerGoalRunBudgetMismatch,
    #[error("Worker Goal runs cannot be consumed by a higher-order driver")]
    WorkerGoalHigherOrderDriver,
}

/// Fully validated orchestration configuration and provider request options.
pub struct RunSpec {
    provenance: RunProvenance,
    config: OrchestratorConfig,
    call_options: CallOptions,
}

impl RunSpec {
    pub const fn provenance(&self) -> RunProvenance {
        self.provenance
    }

    #[cfg(test)]
    pub(crate) fn config(&self) -> &OrchestratorConfig {
        &self.config
    }

    pub fn call_options(&self) -> &CallOptions {
        &self.call_options
    }

    /// Start the canonical orchestrator from already validated inputs.
    pub fn start(
        self,
        services: OrchestratorServices,
        conversation: Vec<ModelMessage>,
    ) -> (
        mpsc::UnboundedReceiver<LoopEvent>,
        mpsc::UnboundedSender<LoopInput>,
    ) {
        if let Err(error) = self.validate_start_database_path(&services.db_path) {
            let session_id = self.config.session_id;
            tracing::error!(
                %session_id,
                error = %error,
                "Rejected isolated Worker run before orchestrator start"
            );
            let (event_tx, event_rx) = mpsc::unbounded_channel();
            let (input_tx, input_rx) = mpsc::unbounded_channel();
            drop(input_rx);
            let _ = event_tx.send(LoopEvent::Error {
                error: error.to_string(),
            });
            let _ = event_tx.send(LoopEvent::Finished {
                session_id,
                stop_reason: LoopStopReason::ProviderError,
            });
            drop(event_tx);
            return (event_rx, input_tx);
        }
        let conversation = self.starting_conversation(conversation);
        let (provenance, config, call_options) = self.into_parts();
        tracing::info!(
            surface = provenance.as_str(),
            session_id = %config.session_id,
            session_type = %config.session_type,
            working_dir = %config.working_dir.display(),
            project_dir = ?config.project_dir,
            "Starting resolved agent run"
        );
        AgenticOrchestrator::new(services, config).run(conversation, call_options)
    }

    /// Bind isolated Worker provider accounting to the exact database used by
    /// the orchestrator. This check belongs at `start`, where both otherwise
    /// independently resolved authorities are finally present.
    fn validate_start_database_path(&self, service_db_path: &Path) -> Result<(), RunSpecError> {
        if !self.config.context_mode.is_isolated_worker() {
            return Ok(());
        }
        let Some(governor) = self.config.provider_governor.as_ref() else {
            return Err(if self.config.context_mode.is_worker_goal() {
                RunSpecError::WorkerGoalRequiresGovernor
            } else {
                RunSpecError::WorkerConversationRequiresGovernor
            });
        };
        let governed = canonical_database_path(&governor.binding().db_path)
            .ok_or(RunSpecError::WorkerProviderDatabaseMismatch)?;
        let service = canonical_database_path(service_db_path)
            .ok_or(RunSpecError::WorkerProviderDatabaseMismatch)?;
        if governed != service {
            return Err(RunSpecError::WorkerProviderDatabaseMismatch);
        }
        Ok(())
    }

    fn starting_conversation(&self, conversation: Vec<ModelMessage>) -> Vec<ModelMessage> {
        let Some(goal) = self.config.context_mode.worker_goal_context() else {
            return conversation;
        };
        if !conversation.is_empty() {
            tracing::warn!(
                supplied_messages = conversation.len(),
                goal_id = %goal.binding().goal_id,
                attempt_id = %goal.binding().attempt_id,
                "Ignoring caller-supplied conversation history for isolated Worker Goal run"
            );
        }
        vec![goal.ephemeral_trigger_message()]
    }

    /// Decompose a validated run for a higher-order driver such as Hive's
    /// tick engine. The returned settings remain canonical and aligned.
    pub(crate) fn into_parts(self) -> (RunProvenance, OrchestratorConfig, CallOptions) {
        (self.provenance, self.config, self.call_options)
    }

    /// Decompose only when the higher-order driver owns this run surface.
    pub(crate) fn into_parts_for(
        self,
        expected: RunProvenance,
    ) -> Result<(OrchestratorConfig, CallOptions), RunSpecError> {
        if self.provenance != expected {
            return Err(RunSpecError::DriverProvenanceMismatch {
                expected: expected.as_str(),
                actual: self.provenance.as_str(),
            });
        }
        match &self.config.context_mode {
            RunContextMode::WorkerConversation { .. } => {
                return Err(RunSpecError::WorkerConversationHigherOrderDriver);
            }
            RunContextMode::WorkerGoal { .. } => {
                return Err(RunSpecError::WorkerGoalHigherOrderDriver);
            }
            RunContextMode::Standard => {}
        }
        Ok((self.config, self.call_options))
    }
}

/// Builder for the one canonical production run contract.
pub struct RunSpecBuilder {
    provenance: RunProvenance,
    config: OrchestratorConfig,
    call_options: CallOptions,
}

impl RunSpecBuilder {
    pub fn new(
        provenance: RunProvenance,
        session_id: impl Into<String>,
        working_dir: impl Into<PathBuf>,
        session_type: SessionType,
    ) -> Self {
        Self {
            provenance,
            config: OrchestratorConfig {
                session_id: session_id.into(),
                working_dir: working_dir.into(),
                session_type,
                ..Default::default()
            },
            call_options: CallOptions::default(),
        }
    }

    pub fn project_dir(mut self, project_dir: Option<PathBuf>) -> Self {
        self.config.project_dir = project_dir;
        self
    }

    pub fn hive_crew_slug(mut self, hive_crew_slug: Option<String>) -> Self {
        self.config.hive_crew_slug = hive_crew_slug;
        self
    }

    /// Attach group linkage when this run is one member of a Hive group turn.
    pub fn hive_group_run(mut self, hive_group_run: Option<HiveGroupRunContext>) -> Self {
        self.config.hive_group_run = hive_group_run;
        self
    }

    pub fn hive_profile(mut self, hive_profile: Option<Arc<HiveProfileSnapshot>>) -> Self {
        self.config.hive_profile = hive_profile;
        self
    }

    /// Select the exact request-context capability profile. Ordinary callers
    /// leave the default `Standard` mode unchanged.
    pub fn context_mode(mut self, context_mode: RunContextMode) -> Self {
        self.config.context_mode = context_mode;
        self
    }

    pub fn permission_mode(mut self, permission_mode: PermissionMode) -> Self {
        self.config.permission_mode = permission_mode;
        self
    }

    /// Constrain execution to an explicit per-turn capability set. This is
    /// intentionally separate from provider-advertised tools because an
    /// unrestricted `tool_search` may normally dispatch hidden specialists.
    pub fn execution_tool_allowlist(
        mut self,
        execution_tool_allowlist: Option<HashSet<String>>,
    ) -> Self {
        self.config.execution_tool_allowlist = execution_tool_allowlist;
        self
    }

    pub fn run_budget(mut self, run_budget: Option<RunBudget>) -> Self {
        self.config.run_budget = run_budget;
        self
    }

    pub fn stream_idle_timeout(mut self, stream_idle_timeout: Duration) -> Self {
        self.config.stream_idle_timeout = stream_idle_timeout;
        self
    }

    pub fn user_id(mut self, user_id: Option<String>) -> Self {
        self.config.user_id = user_id;
        self
    }

    pub fn initial_work_mode(mut self, initial_work_mode: WorkMode) -> Self {
        self.config.initial_work_mode = initial_work_mode;
        self
    }

    /// Declare that the caller's Code schemas came from the canonical mode
    /// policy and may be rebuilt from the registry as work mode changes.
    ///
    /// This is deliberately opt-in: an arbitrary caller-provided subset stays
    /// an immutable capability ceiling. An exact `execution_tool_allowlist`
    /// remains an upper bound even for policy-derived callers that opt in.
    pub fn mode_aware_code_tools(mut self, enabled: bool) -> Self {
        self.config.refresh_code_tools_on_mode_change =
            enabled && self.config.session_type == SessionType::Code;
        self
    }

    pub fn generate_title(mut self, generate_title: bool) -> Self {
        self.config.generate_title = generate_title;
        self
    }

    pub fn delegated_progress_tx(
        mut self,
        delegated_progress_tx: Option<mpsc::UnboundedSender<DelegatedProgressEvent>>,
    ) -> Self {
        self.config.delegated_progress_tx = delegated_progress_tx;
        self
    }

    /// Attach the exact claimed Worker/run provider capability. Non-Worker
    /// surfaces leave this absent and preserve their existing behavior.
    pub fn provider_governor(
        mut self,
        provider_governor: Option<Arc<WorkerProviderCallGovernor>>,
    ) -> Self {
        self.config.provider_governor = provider_governor;
        self
    }

    pub fn call_options(mut self, call_options: CallOptions) -> Self {
        self.call_options = call_options;
        self
    }

    /// Validate surface/workspace identity and freeze request policy against
    /// the immutable model runtime held by this client.
    pub fn build(mut self, ai_client: &AiClient) -> Result<RunSpec, RunSpecError> {
        validate_session_id(&self.config.session_id)?;
        validate_surface(self.provenance, self.config.session_type)?;
        validate_context_mode(self.provenance, &self.config, &self.call_options)?;
        validate_provider_governor(&self.config, ai_client)?;

        self.config.working_dir = canonical_directory("working_dir", &self.config.working_dir)?;
        if let Some(project_dir) = self.config.project_dir.take() {
            let project_dir = canonical_directory("project_dir", &project_dir)?;
            if !project_dir.starts_with(&self.config.working_dir) {
                return Err(RunSpecError::ProjectOutsideWorkingDirectory {
                    working_dir: self.config.working_dir,
                    project_dir,
                });
            }
            self.config.project_dir = Some(project_dir);
        }
        canonicalize_and_validate_worker_goal_workspace(
            &mut self.config.context_mode,
            &self.config.working_dir,
            self.config.project_dir.as_deref(),
        )?;

        match self.call_options.session_id.as_deref() {
            Some(cache_session_id) if cache_session_id != self.config.session_id => {
                return Err(RunSpecError::CacheSessionMismatch {
                    run_session_id: self.config.session_id,
                    cache_session_id: cache_session_id.to_string(),
                });
            }
            Some(_) => {}
            None => self.call_options.session_id = Some(self.config.session_id.clone()),
        }
        if self.config.context_mode.is_worker_conversation() {
            // Neutral Worker conversations are exactly one provider response.
            // Durable always-on behavior is a sequence of claimed Hive runs,
            // never an unbounded loop or an implicit tool/delegation surface.
            self.config.execution_tool_allowlist = Some(HashSet::new());
            self.config.refresh_code_tools_on_mode_change = false;
            self.config.run_budget = Some(RunBudget::with_max_turns(1));
            self.config.delegated_progress_tx = None;
            self.call_options.tools = None;
            self.call_options.web_search = None;
            self.call_options.web_fetch = None;
            self.call_options.parallel_tool_calls = Some(false);
            self.call_options.codex_parallel_tool_calls = false;
        }
        if let Some(goal) = self.config.context_mode.worker_goal_context() {
            let expected_budget = RunBudget::with_max_turns(
                usize::try_from(
                    goal.workflow_snapshot()
                        .latest_attempt
                        .as_ref()
                        .expect("Worker Goal snapshot validated above")
                        .max_turns
                        .saturating_sub(
                            goal.workflow_snapshot()
                                .latest_attempt
                                .as_ref()
                                .expect("Worker Goal snapshot validated above")
                                .turn_count,
                        ),
                )
                .unwrap_or(usize::MAX),
            );
            if self
                .config
                .run_budget
                .is_some_and(|budget| budget != expected_budget)
            {
                return Err(RunSpecError::WorkerGoalRunBudgetMismatch);
            }
            self.config.run_budget = Some(expected_budget);
            self.config.refresh_code_tools_on_mode_change = false;
            self.config.delegated_progress_tx = None;
            self.config.generate_title = false;
            self.call_options.web_search = None;
            self.call_options.web_fetch = None;
            self.call_options.parallel_tool_calls = Some(false);
            self.call_options.codex_parallel_tool_calls = false;
        }

        // Opt-in never overrides a deliberately tool-free request.
        self.config.refresh_code_tools_on_mode_change &= self.call_options.tools.is_some();
        apply_execution_tool_allowlist(
            &mut self.call_options,
            self.config.execution_tool_allowlist.as_ref(),
        );
        validate_call_options(&mut self.call_options)?;
        self.call_options =
            ai_client.canonical_call_options(&ai_client.config().model, &self.call_options);
        if self.config.context_mode.is_worker_conversation() {
            // Canonicalization may remove unsupported capabilities but must
            // never be trusted to establish this least-privilege boundary.
            self.call_options.tools = None;
            self.call_options.web_search = None;
            self.call_options.web_fetch = None;
            self.call_options.parallel_tool_calls = Some(false);
            self.call_options.codex_parallel_tool_calls = false;
        }
        if self.config.context_mode.is_worker_goal() {
            // Provider canonicalization may remove unsupported tools. It must
            // never widen this surface or restore provider-hosted web access.
            apply_execution_tool_allowlist(
                &mut self.call_options,
                self.config.execution_tool_allowlist.as_ref(),
            );
            self.call_options.web_search = None;
            self.call_options.web_fetch = None;
            self.call_options.parallel_tool_calls = Some(false);
            self.call_options.codex_parallel_tool_calls = false;
            if self
                .call_options
                .tools
                .as_ref()
                .is_none_or(|tools| tools.is_empty())
            {
                return Err(RunSpecError::WorkerGoalRequiresToolAllowlist);
            }
        }
        validate_call_options(&mut self.call_options)?;

        Ok(RunSpec {
            provenance: self.provenance,
            config: self.config,
            call_options: self.call_options,
        })
    }
}

fn validate_provider_governor(
    config: &OrchestratorConfig,
    ai_client: &AiClient,
) -> Result<(), RunSpecError> {
    let Some(governor) = config.provider_governor.as_ref() else {
        return Ok(());
    };
    let binding = governor.binding();
    let resolved = ai_client.resolved_model();
    if resolved.key != binding.model_key {
        return Err(RunSpecError::WorkerProviderModelMismatch);
    }
    // A catalog revision fingerprints the whole mutable catalog, not this
    // executable row. The exact ModelKey above is the runtime identity fence;
    // an unrelated catalog refresh must not invalidate a persistent Worker.
    if config.permission_mode != binding.permission_mode {
        return Err(RunSpecError::WorkerProviderPermissionMismatch);
    }
    Ok(())
}

fn validate_context_mode(
    provenance: RunProvenance,
    config: &OrchestratorConfig,
    call_options: &CallOptions,
) -> Result<(), RunSpecError> {
    match &config.context_mode {
        RunContextMode::Standard => Ok(()),
        RunContextMode::WorkerConversation { worker_id, .. } => {
            validate_worker_conversation_context(provenance, config, worker_id)
        }
        RunContextMode::WorkerGoal { context, .. } => {
            validate_worker_goal_context(provenance, config, call_options, context)
        }
    }
}

fn validate_worker_conversation_context(
    provenance: RunProvenance,
    config: &OrchestratorConfig,
    worker_id: &str,
) -> Result<(), RunSpecError> {
    if provenance != RunProvenance::Hive || config.session_type != SessionType::Hive {
        return Err(RunSpecError::WorkerConversationRequiresHiveSurface);
    }
    for (present, capability) in [
        (config.project_dir.is_some(), "project_dir"),
        (config.hive_profile.is_some(), "a global Hive profile"),
        (
            config.hive_crew_slug.is_some(),
            "a global Hive crew identity",
        ),
        (
            config.delegated_progress_tx.is_some(),
            "a delegated progress channel",
        ),
    ] {
        if present {
            return Err(RunSpecError::WorkerConversationForbiddenCapability { capability });
        }
    }
    if worker_id.trim() != worker_id
        || worker_id.is_empty()
        || worker_id.chars().any(char::is_control)
    {
        return Err(RunSpecError::InvalidWorkerConversationId);
    }
    let governor = config
        .provider_governor
        .as_ref()
        .ok_or(RunSpecError::WorkerConversationRequiresGovernor)?;
    let binding = governor.binding();
    for (matches, field) in [
        (binding.worker_id.as_str() == worker_id, "Worker id"),
        (
            binding.session_id.as_str() == config.session_id.as_str(),
            "session",
        ),
        (
            binding.owner_user_id.as_deref() == config.user_id.as_deref(),
            "owner",
        ),
    ] {
        if !matches {
            return Err(RunSpecError::WorkerConversationBindingMismatch { field });
        }
    }
    let lane_matches = match (&binding.conversation_lane, &config.hive_group_run) {
        (WorkerConversationLane::DirectMessage, None) => true,
        (WorkerConversationLane::DirectMessage, Some(_)) => false,
        (WorkerConversationLane::Group { .. }, None) => false,
        (WorkerConversationLane::Group { group_id }, Some(group)) => {
            group.group_id.as_str() == group_id.as_str()
                && group.worker_id.as_str() == binding.worker_id.as_str()
                && group.run_id.as_str() == binding.run_id.as_str()
        }
    };
    if !lane_matches {
        return Err(RunSpecError::WorkerConversationBindingMismatch { field: "lane" });
    }
    Ok(())
}

fn validate_worker_goal_context(
    provenance: RunProvenance,
    config: &OrchestratorConfig,
    call_options: &CallOptions,
    context: &WorkerGoalExecutionContext,
) -> Result<(), RunSpecError> {
    if provenance != RunProvenance::Hive || config.session_type != SessionType::Hive {
        return Err(RunSpecError::WorkerGoalRequiresHiveSurface);
    }
    for (present, capability) in [
        (config.hive_profile.is_some(), "a global Hive profile"),
        (
            config.hive_crew_slug.is_some(),
            "a global Hive crew identity",
        ),
        (
            config.hive_group_run.is_some(),
            "a group or hidden group lane",
        ),
        (
            config.delegated_progress_tx.is_some(),
            "a delegated progress channel",
        ),
        (
            config.refresh_code_tools_on_mode_change,
            "mode-driven tool expansion",
        ),
        (config.generate_title, "chat title generation"),
        (
            call_options.web_search.is_some(),
            "provider-hosted web search",
        ),
        (
            call_options.web_fetch.is_some(),
            "provider-hosted web fetch",
        ),
    ] {
        if present {
            return Err(RunSpecError::WorkerGoalForbiddenCapability { capability });
        }
    }
    if config.initial_work_mode != WorkMode::Build {
        return Err(RunSpecError::WorkerGoalForbiddenCapability {
            capability: "plan-mode mutation",
        });
    }

    let allowlist = config
        .execution_tool_allowlist
        .as_ref()
        .ok_or(RunSpecError::WorkerGoalRequiresToolAllowlist)?;
    if allowlist.is_empty() {
        return Err(RunSpecError::WorkerGoalRequiresToolAllowlist);
    }
    for tool in allowlist {
        if !WORKER_GOAL_TOOL_CAPABILITY_CEILING.contains(&tool.as_str()) {
            return Err(RunSpecError::WorkerGoalForbiddenTool { tool: tool.clone() });
        }
    }

    let binding = context.binding();
    for (value, field) in [
        (binding.worker_id.as_str(), "Worker id"),
        (binding.session_id.as_str(), "session id"),
        (binding.run_id.as_str(), "run id"),
        (binding.run_lease_token.as_str(), "run lease token"),
        (binding.goal_id.as_str(), "Goal id"),
        (binding.attempt_id.as_str(), "attempt id"),
        (binding.plan_revision_id.as_str(), "plan revision id"),
        (binding.step_id.as_str(), "step id"),
    ] {
        if !is_valid_binding_text(value) {
            return Err(RunSpecError::InvalidWorkerGoalBinding { field });
        }
    }
    if binding
        .owner_user_id
        .as_deref()
        .is_some_and(|owner| !is_valid_binding_text(owner))
    {
        return Err(RunSpecError::InvalidWorkerGoalBinding {
            field: "owner user id",
        });
    }
    for (valid, field) in [
        (binding.worker_revision >= 1, "Worker revision"),
        (binding.run_lease_epoch >= 1, "run lease epoch"),
        (binding.goal_revision >= 1, "Goal revision"),
        (
            binding.workflow_aggregate_revision >= 1,
            "Workflow aggregate revision",
        ),
        (binding.plan_revision_number >= 1, "plan revision number"),
        (binding.step_revision >= 1, "step revision"),
    ] {
        if !valid {
            return Err(RunSpecError::InvalidWorkerGoalBinding { field });
        }
    }
    if !binding.workspace_dir.is_absolute() {
        return Err(RunSpecError::WorkerGoalRequiresWorkspace);
    }
    if !matches!(
        binding.run_origin,
        WorkerRunOrigin::UserWorkflowActivation | WorkerRunOrigin::WorkflowRollover
    ) {
        return Err(RunSpecError::InvalidWorkerGoalBinding {
            field: "Workflow origin",
        });
    }

    let governor = config
        .provider_governor
        .as_ref()
        .ok_or(RunSpecError::WorkerGoalRequiresGovernor)?;
    let governed = governor.binding();
    for (matches, field) in [
        (
            governed.worker_id.as_str() == binding.worker_id.as_str(),
            "Worker id",
        ),
        (
            governed.worker_revision == binding.worker_revision,
            "Worker revision",
        ),
        (governed.run_id == binding.run_id, "run id"),
        (
            governed.run_lease_token == binding.run_lease_token,
            "run lease token",
        ),
        (
            governed.run_lease_epoch == binding.run_lease_epoch,
            "run lease epoch",
        ),
        (
            governed.session_id.as_str() == binding.session_id.as_str(),
            "session",
        ),
        (
            binding.session_id.as_str() == config.session_id.as_str(),
            "run session",
        ),
        (
            governed.owner_user_id.as_deref() == binding.owner_user_id.as_deref(),
            "owner",
        ),
        (
            binding.owner_user_id.as_deref() == config.user_id.as_deref(),
            "run owner",
        ),
        (
            governed.workflow_goal_id.as_deref() == Some(binding.goal_id.as_str()),
            "Goal id",
        ),
        (
            governed.workflow_attempt_id.as_deref() == Some(binding.attempt_id.as_str()),
            "attempt id",
        ),
        (
            governed.conversation_lane == WorkerConversationLane::DirectMessage,
            "direct Worker lane",
        ),
        (governed.origin == binding.run_origin, "Workflow origin"),
    ] {
        if !matches {
            return Err(RunSpecError::WorkerGoalGovernorBindingMismatch { field });
        }
    }

    validate_worker_goal_snapshot(config, context)
}

fn validate_worker_goal_snapshot(
    config: &OrchestratorConfig,
    context: &WorkerGoalExecutionContext,
) -> Result<(), RunSpecError> {
    let binding = context.binding();
    let snapshot = context.workflow_snapshot();
    let goal = &snapshot.goal;
    for (matches, field) in [
        (snapshot.schema_version >= 1, "Workflow schema version"),
        (
            snapshot.collaboration_mode == CollaborationMode::Default,
            "Workflow collaboration mode",
        ),
        (
            snapshot.permission_mode == config.permission_mode.as_str(),
            "Workflow permission mode",
        ),
        (goal.id == binding.goal_id, "Goal id"),
        (goal.session_id == binding.session_id, "Goal session"),
        (goal.revision == binding.goal_revision, "Goal revision"),
        (goal.status == GoalStatus::Active, "Goal status"),
        (
            snapshot.aggregate_revision == goal.revision,
            "canonical Workflow aggregate revision",
        ),
        (
            snapshot.aggregate_revision == binding.workflow_aggregate_revision,
            "Workflow aggregate revision",
        ),
    ] {
        if !matches {
            return Err(RunSpecError::WorkerGoalSnapshotMismatch { field });
        }
    }
    if snapshot
        .criteria
        .iter()
        .any(|criterion| criterion.goal_id != binding.goal_id)
    {
        return Err(RunSpecError::WorkerGoalSnapshotMismatch {
            field: "criterion ownership",
        });
    }

    let plan = snapshot
        .plan_revision
        .as_ref()
        .ok_or(RunSpecError::WorkerGoalSnapshotMismatch {
            field: "active plan revision",
        })?;
    for (matches, field) in [
        (plan.id == binding.plan_revision_id, "plan revision id"),
        (plan.goal_id == binding.goal_id, "plan Goal id"),
        (
            plan.revision_number == binding.plan_revision_number,
            "plan revision number",
        ),
        (plan.status == PlanRevisionStatus::Active, "plan status"),
    ] {
        if !matches {
            return Err(RunSpecError::WorkerGoalSnapshotMismatch { field });
        }
    }
    if snapshot
        .steps
        .iter()
        .any(|step| step.plan_revision_id != binding.plan_revision_id)
    {
        return Err(RunSpecError::WorkerGoalSnapshotMismatch {
            field: "step plan ownership",
        });
    }
    let step = snapshot
        .steps
        .iter()
        .find(|step| step.id == binding.step_id)
        .ok_or(RunSpecError::WorkerGoalSnapshotMismatch { field: "step id" })?;
    for (matches, field) in [
        (step.revision == binding.step_revision, "step revision"),
        (step.status == WorkflowStepStatus::InProgress, "step status"),
        (
            step.claimed_attempt_id.as_deref() == Some(binding.attempt_id.as_str()),
            "step attempt claim",
        ),
    ] {
        if !matches {
            return Err(RunSpecError::WorkerGoalSnapshotMismatch { field });
        }
    }
    if snapshot.dependencies.iter().any(|dependency| {
        !snapshot
            .steps
            .iter()
            .any(|step| step.id == dependency.step_id)
            || !snapshot
                .steps
                .iter()
                .any(|step| step.id == dependency.depends_on_step_id)
    }) {
        return Err(RunSpecError::WorkerGoalSnapshotMismatch {
            field: "dependency ownership",
        });
    }

    let attempt =
        snapshot
            .latest_attempt
            .as_ref()
            .ok_or(RunSpecError::WorkerGoalSnapshotMismatch {
                field: "running attempt",
            })?;
    for (matches, field) in [
        (attempt.id == binding.attempt_id, "attempt id"),
        (attempt.goal_id == binding.goal_id, "attempt Goal id"),
        (
            attempt.plan_revision_id.as_deref() == Some(binding.plan_revision_id.as_str()),
            "attempt plan revision",
        ),
        (
            attempt.step_id.as_deref() == Some(binding.step_id.as_str()),
            "attempt step",
        ),
        (attempt.status == AttemptStatus::Running, "attempt status"),
        (
            attempt.goal_revision_at_start == binding.goal_revision,
            "attempt Goal revision",
        ),
        (
            attempt.permission_mode == config.permission_mode.as_str(),
            "attempt permission mode",
        ),
        (attempt.max_turns > 0, "attempt turn budget"),
        (
            attempt.turn_count < attempt.max_turns,
            "remaining attempt turn budget",
        ),
        (attempt.max_tool_calls > 0, "attempt tool budget"),
        (
            attempt.tool_call_count < attempt.max_tool_calls,
            "remaining attempt tool budget",
        ),
        (attempt.max_wall_time_secs > 0, "attempt wall-time budget"),
        (attempt.max_research_actions > 0, "attempt research budget"),
        (
            attempt.research_action_count <= attempt.max_research_actions,
            "attempt research usage",
        ),
    ] {
        if !matches {
            return Err(RunSpecError::WorkerGoalSnapshotMismatch { field });
        }
    }
    Ok(())
}

fn canonicalize_and_validate_worker_goal_workspace(
    context_mode: &mut RunContextMode,
    working_dir: &Path,
    project_dir: Option<&Path>,
) -> Result<(), RunSpecError> {
    let RunContextMode::WorkerGoal { context, .. } = context_mode else {
        return Ok(());
    };
    let project_dir = project_dir.ok_or(RunSpecError::WorkerGoalRequiresWorkspace)?;
    let canonical_workspace =
        canonical_directory("Worker Goal workspace", &context.binding.workspace_dir)?;
    if canonical_workspace != working_dir || canonical_workspace != project_dir {
        return Err(RunSpecError::WorkerGoalWorkspaceMismatch);
    }
    let mut canonical_context = context.as_ref().clone();
    canonical_context.binding.workspace_dir = canonical_workspace;
    *context = Arc::new(canonical_context);
    Ok(())
}

fn is_valid_binding_text(value: &str) -> bool {
    !value.is_empty()
        && value.trim() == value
        && !value.chars().any(char::is_control)
        && value.len() <= 256
}

pub(crate) fn apply_execution_tool_allowlist(
    options: &mut CallOptions,
    execution_tool_allowlist: Option<&HashSet<String>>,
) {
    let Some(allowlist) = execution_tool_allowlist else {
        return;
    };

    if let Some(tools) = options.tools.as_mut() {
        tools.retain(|tool| allowlist.contains(&tool.name));
        if tools.is_empty() {
            options.tools = None;
        }
    }
    if !allowlist.contains("web_search") {
        options.web_search = None;
    }
    if !allowlist.contains("web_fetch") {
        options.web_fetch = None;
    }
    if options.tools.as_ref().is_none_or(|tools| tools.len() <= 1) {
        options.parallel_tool_calls = Some(false);
        options.codex_parallel_tool_calls = false;
    }
}

fn validate_session_id(session_id: &str) -> Result<(), RunSpecError> {
    if session_id.is_empty() {
        return Err(RunSpecError::EmptySessionId);
    }
    if session_id.trim() != session_id {
        return Err(RunSpecError::SessionIdWhitespace);
    }
    if session_id.chars().any(char::is_control) {
        return Err(RunSpecError::InvalidSessionId);
    }
    Ok(())
}

fn validate_surface(
    provenance: RunProvenance,
    session_type: SessionType,
) -> Result<(), RunSpecError> {
    if provenance.kernel() != RunKernel::StreamingOrchestrator {
        return Err(RunSpecError::UnsupportedKernel {
            provenance: provenance.as_str(),
            kernel: provenance.kernel().as_str(),
        });
    }
    let valid = match provenance {
        RunProvenance::Server => session_type != SessionType::Hive,
        RunProvenance::Acp => session_type == SessionType::Code,
        RunProvenance::Hive => session_type == SessionType::Hive,
        RunProvenance::Tui => session_type == SessionType::Code,
        RunProvenance::Delegated => unreachable!("delegated kernel rejected above"),
    };
    if valid {
        Ok(())
    } else {
        Err(RunSpecError::SurfaceSessionTypeMismatch {
            provenance: provenance.as_str(),
            session_type,
        })
    }
}

fn canonical_directory(kind: &'static str, path: &Path) -> Result<PathBuf, RunSpecError> {
    if !path.is_absolute() {
        return Err(RunSpecError::RelativeWorkspacePath {
            kind,
            path: path.to_path_buf(),
        });
    }
    let canonical =
        path.canonicalize()
            .map_err(|source| RunSpecError::InaccessibleWorkspacePath {
                kind,
                path: path.to_path_buf(),
                source,
            })?;
    if !canonical.is_dir() {
        return Err(RunSpecError::WorkspacePathNotDirectory {
            kind,
            path: canonical,
        });
    }
    Ok(canonical)
}

/// Resolve a database identity without requiring the database file itself to
/// exist yet. Existing paths resolve through symlinks in full; for a newly
/// created database, its parent must already exist and is canonicalized before
/// the final filename is appended.
fn canonical_database_path(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    path.canonicalize().ok().or_else(|| {
        let parent = path.parent()?.canonicalize().ok()?;
        let file_name = path.file_name()?;
        Some(parent.join(file_name))
    })
}

fn validate_call_options(options: &mut CallOptions) -> Result<(), RunSpecError> {
    if options.max_tokens == Some(0) {
        return Err(RunSpecError::ZeroMaxTokens);
    }
    if options
        .temperature
        .is_some_and(|temperature| !temperature.is_finite())
    {
        return Err(RunSpecError::NonFiniteTemperature);
    }

    let Some(tools) = options.tools.as_mut() else {
        return Ok(());
    };
    if tools.is_empty() {
        options.tools = None;
        return Ok(());
    }

    let mut names = HashSet::with_capacity(tools.len());
    for tool in tools {
        if tool.name.trim().is_empty() {
            return Err(RunSpecError::EmptyToolName);
        }
        if !names.insert(tool.name.as_str()) {
            return Err(RunSpecError::DuplicateToolName(tool.name.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::Arc;

    use super::{validate_call_options, validate_session_id, validate_surface};
    use super::{
        RunBudget, RunContextMode, RunKernel, RunProvenance, RunSpecBuilder, RunSpecError,
        WorkerGoalExecutionBinding, WorkerGoalExecutionContext,
    };
    use crate::agent::{
        WorkerConversationResponseCommit, WorkerConversationResponseCommitDisposition,
        WorkerConversationResponseCommitInput, WorkerConversationResponseCommitter,
        WorkerGoalOutcomeCommit, WorkerGoalOutcomeCommitDisposition, WorkerGoalOutcomeCommitError,
        WorkerGoalOutcomeCommitInput, WorkerGoalOutcomeCommitter, WorkerProviderCallGovernor,
        WorkerProviderGovernorBinding,
    };
    use crate::ai::client::{AiClient, AiClientConfig, CallOptions};
    use crate::ai::models::{resolve_model_metadata, ApiFormat, ModelCatalogSource, ModelKey};
    use crate::ai::providers::ProviderId;
    use crate::ai::types::{AiTool, Content, ModelMessage, Role, WebFetchConfig, WebSearchConfig};
    use crate::storage::{SessionType, WorkerConversationLane, WorkerRunOrigin};
    use crate::tools::registry::PermissionMode;
    use crate::workflow::{
        AttemptStatus, CollaborationMode, ExecutionAttempt, Goal, GoalCriterion, GoalStatus,
        PlanRevision, PlanRevisionStatus, WorkflowSnapshot, WorkflowStep, WorkflowStepStatus,
    };

    struct TestResponseCommitter;

    impl WorkerConversationResponseCommitter for TestResponseCommitter {
        fn commit_response(
            &self,
            _input: &WorkerConversationResponseCommitInput,
        ) -> Result<
            WorkerConversationResponseCommit,
            crate::agent::WorkerConversationResponseCommitError,
        > {
            Ok(WorkerConversationResponseCommit {
                disposition: WorkerConversationResponseCommitDisposition::Inserted,
                response_message_id: 1,
                response_group_message_id: None,
            })
        }
    }

    struct TestGoalOutcomeCommitter;

    impl WorkerGoalOutcomeCommitter for TestGoalOutcomeCommitter {
        fn commit_outcome(
            &self,
            _input: &WorkerGoalOutcomeCommitInput,
        ) -> Result<WorkerGoalOutcomeCommit, WorkerGoalOutcomeCommitError> {
            Ok(WorkerGoalOutcomeCommit {
                disposition: WorkerGoalOutcomeCommitDisposition::Inserted,
            })
        }
    }

    fn worker_goal_mode(context: Arc<WorkerGoalExecutionContext>) -> RunContextMode {
        RunContextMode::worker_goal(context, Arc::new(TestGoalOutcomeCommitter))
    }

    fn direct_worker_governor(db_path: &Path) -> Arc<WorkerProviderCallGovernor> {
        direct_worker_governor_with_model(
            db_path,
            ModelKey::new(ProviderId::OpenAI, "gpt-5.5", ApiFormat::OpenAIResponses),
            None,
        )
    }

    fn direct_worker_governor_with_model(
        db_path: &Path,
        model_key: ModelKey,
        model_catalog_revision: Option<&str>,
    ) -> Arc<WorkerProviderCallGovernor> {
        Arc::new(
            WorkerProviderCallGovernor::new(WorkerProviderGovernorBinding {
                db_path: db_path.to_path_buf(),
                worker_id: "worker-1".into(),
                worker_revision: 1,
                owner_user_id: Some("user-1".into()),
                session_id: "worker-session".into(),
                conversation_lane: WorkerConversationLane::DirectMessage,
                run_id: "run-1".into(),
                run_lease_token: "lease-1".into(),
                run_lease_epoch: 1,
                model_key,
                model_catalog_revision: model_catalog_revision.map(ToOwned::to_owned),
                permission_mode: PermissionMode::Supervised,
                origin: WorkerRunOrigin::UserDm,
                workflow_goal_id: None,
                workflow_attempt_id: None,
                pricing: None,
                override_grant_id: None,
            })
            .unwrap(),
        )
    }

    fn worker_goal_governor(
        db_path: &Path,
        goal_id: &str,
        attempt_id: &str,
    ) -> Arc<WorkerProviderCallGovernor> {
        Arc::new(
            WorkerProviderCallGovernor::new(WorkerProviderGovernorBinding {
                db_path: db_path.to_path_buf(),
                worker_id: "worker-1".into(),
                worker_revision: 4,
                owner_user_id: Some("user-1".into()),
                session_id: "worker-session".into(),
                conversation_lane: WorkerConversationLane::DirectMessage,
                run_id: "goal-run-1".into(),
                run_lease_token: "goal-lease-1".into(),
                run_lease_epoch: 2,
                model_key: ModelKey::new(ProviderId::OpenAI, "gpt-5.5", ApiFormat::OpenAIResponses),
                model_catalog_revision: None,
                permission_mode: PermissionMode::Supervised,
                origin: WorkerRunOrigin::UserWorkflowActivation,
                workflow_goal_id: Some(goal_id.into()),
                workflow_attempt_id: Some(attempt_id.into()),
                pricing: None,
                override_grant_id: None,
            })
            .unwrap(),
        )
    }

    fn worker_goal_context(workspace: &Path) -> Arc<WorkerGoalExecutionContext> {
        let timestamp = "2026-08-25T00:00:00.000000Z".to_string();
        let snapshot = WorkflowSnapshot {
            schema_version: 1,
            aggregate_revision: 3,
            collaboration_mode: CollaborationMode::Default,
            permission_mode: "supervised".into(),
            goal: Goal {
                id: "goal-1".into(),
                session_id: "worker-session".into(),
                title: "Ship the bounded Worker Goal".into(),
                objective: "Implement and verify the exact assigned step".into(),
                constraints: vec!["Do not delegate".into()],
                status: GoalStatus::Active,
                status_reason: None,
                needs_definition: false,
                revision: 3,
                token_budget: None,
                tokens_used: 0,
                source: "hive_worker".into(),
                legacy_plan_id: None,
                created_at: timestamp.clone(),
                updated_at: timestamp.clone(),
                activated_at: Some(timestamp.clone()),
                completed_at: None,
                cancelled_at: None,
            },
            criteria: vec![GoalCriterion {
                id: "criterion-1".into(),
                goal_id: "goal-1".into(),
                position: 0,
                description: "Focused verification passes".into(),
                required: true,
                status: crate::workflow::CriterionStatus::Pending,
                evidence: Vec::new(),
                verifier: None,
                verified_at: None,
            }],
            plan_revision: Some(PlanRevision {
                id: "plan-1".into(),
                goal_id: "goal-1".into(),
                revision_number: 2,
                status: PlanRevisionStatus::Active,
                title: "Bounded plan".into(),
                rationale: None,
                source_message_id: None,
                predecessor_id: None,
                legacy_markdown: None,
                created_at: timestamp.clone(),
                approved_at: Some(timestamp.clone()),
                completed_at: None,
            }),
            steps: vec![WorkflowStep {
                id: "step-1".into(),
                plan_revision_id: "plan-1".into(),
                parent_step_id: None,
                display_key: "1".into(),
                position: 0,
                description: "Implement the exact Worker Goal boundary".into(),
                context: Some("Keep the surface isolated".into()),
                acceptance_criteria: vec!["No ambient capabilities".into()],
                required: true,
                status: WorkflowStepStatus::InProgress,
                outcome: None,
                evidence: Vec::new(),
                claimed_attempt_id: Some("attempt-1".into()),
                revision: 5,
                created_at: timestamp.clone(),
                started_at: Some(timestamp.clone()),
                completed_at: None,
            }],
            dependencies: Vec::new(),
            latest_attempt: Some(ExecutionAttempt {
                id: "attempt-1".into(),
                goal_id: "goal-1".into(),
                plan_revision_id: Some("plan-1".into()),
                step_id: Some("step-1".into()),
                status: AttemptStatus::Running,
                stop_reason: None,
                permission_mode: "supervised".into(),
                goal_revision_at_start: 3,
                max_turns: 4,
                max_tool_calls: 12,
                max_wall_time_secs: 600,
                max_research_actions: 4,
                turn_count: 0,
                tool_call_count: 0,
                research_action_count: 0,
                progress_revision: 0,
                blocker_fingerprint: None,
                blocker_streak: 0,
                started_at: timestamp.clone(),
                updated_at: timestamp,
                ended_at: None,
            }),
            allowed_actions: vec!["pause_goal".into()],
        };
        Arc::new(WorkerGoalExecutionContext::new(
            WorkerGoalExecutionBinding {
                worker_id: "worker-1".into(),
                worker_revision: 4,
                owner_user_id: Some("user-1".into()),
                session_id: "worker-session".into(),
                run_id: "goal-run-1".into(),
                run_lease_token: "goal-lease-1".into(),
                run_lease_epoch: 2,
                run_origin: WorkerRunOrigin::UserWorkflowActivation,
                goal_id: "goal-1".into(),
                goal_revision: 3,
                workflow_aggregate_revision: 3,
                attempt_id: "attempt-1".into(),
                plan_revision_id: "plan-1".into(),
                plan_revision_number: 2,
                step_id: "step-1".into(),
                step_revision: 5,
                workspace_dir: workspace.to_path_buf(),
            },
            Arc::new(snapshot),
        ))
    }

    #[test]
    fn session_identity_rejects_whitespace_and_control_characters() {
        assert!(matches!(
            validate_session_id(" session-1"),
            Err(RunSpecError::SessionIdWhitespace)
        ));
        assert!(matches!(
            validate_session_id("session\n1"),
            Err(RunSpecError::InvalidSessionId)
        ));
    }

    #[test]
    fn surface_session_contract_prevents_hive_and_acp_drift() {
        assert!(validate_surface(RunProvenance::Acp, SessionType::Code).is_ok());
        assert!(matches!(
            validate_surface(RunProvenance::Acp, SessionType::Chat),
            Err(RunSpecError::SurfaceSessionTypeMismatch { .. })
        ));
        assert!(matches!(
            validate_surface(RunProvenance::Server, SessionType::Hive),
            Err(RunSpecError::SurfaceSessionTypeMismatch { .. })
        ));
        assert!(validate_surface(RunProvenance::Hive, SessionType::Hive).is_ok());
        assert!(validate_surface(RunProvenance::Tui, SessionType::Code).is_ok());
        assert!(matches!(
            validate_surface(RunProvenance::Tui, SessionType::Chat),
            Err(RunSpecError::SurfaceSessionTypeMismatch { .. })
        ));
        assert_eq!(
            RunProvenance::Delegated.kernel(),
            RunKernel::DelegatedToolLoop
        );
        assert!(matches!(
            validate_surface(RunProvenance::Delegated, SessionType::Code),
            Err(RunSpecError::UnsupportedKernel {
                provenance: "delegated",
                kernel: "delegated_tool_loop"
            })
        ));
    }

    #[test]
    fn call_options_normalize_empty_tools_and_reject_duplicates() {
        let mut empty = CallOptions {
            tools: Some(Vec::new()),
            ..Default::default()
        };
        validate_call_options(&mut empty).unwrap();
        assert!(empty.tools.is_none());

        let tool = AiTool {
            name: "read".into(),
            description: "Read".into(),
            input_schema: serde_json::json!({"type": "object"}),
            prompt: None,
        };
        let mut duplicate = CallOptions {
            tools: Some(vec![tool.clone(), tool]),
            ..Default::default()
        };
        assert!(matches!(
            validate_call_options(&mut duplicate),
            Err(RunSpecError::DuplicateToolName(name)) if name == "read"
        ));
    }

    #[test]
    fn canonical_directory_rejects_relative_paths() {
        assert!(matches!(
            super::canonical_directory("working_dir", Path::new("relative")),
            Err(RunSpecError::RelativeWorkspacePath { .. })
        ));
    }

    #[test]
    fn builder_aligns_provider_cache_identity_and_freezes_model_limits() {
        let workspace = tempfile::tempdir().unwrap();
        let project = workspace.path().join("project");
        std::fs::create_dir(&project).unwrap();
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".to_string(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            String::new(),
        );

        let spec = RunSpecBuilder::new(
            RunProvenance::Server,
            "session-1",
            workspace.path(),
            SessionType::Code,
        )
        .project_dir(Some(project.clone()))
        .execution_tool_allowlist(Some(HashSet::from(["tool_search".to_string()])))
        .call_options(CallOptions {
            max_tokens: Some(usize::MAX),
            tools: Some(vec![
                AiTool {
                    name: "tool_search".into(),
                    description: "Deferred tool search".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                },
                AiTool {
                    name: "read".into(),
                    description: "Read".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                },
            ]),
            web_search: Some(WebSearchConfig::default()),
            codex_parallel_tool_calls: true,
            ..Default::default()
        })
        .build(&client)
        .unwrap();

        assert_eq!(
            spec.call_options().session_id.as_deref(),
            Some(spec.config().session_id.as_str())
        );
        assert_eq!(
            spec.config().working_dir,
            workspace.path().canonicalize().unwrap()
        );
        assert_eq!(
            spec.config().project_dir.as_deref(),
            Some(project.canonicalize().unwrap().as_path())
        );
        assert_eq!(
            spec.config().execution_tool_allowlist,
            Some(HashSet::from(["tool_search".to_string()]))
        );
        assert_eq!(
            spec.call_options()
                .tools
                .as_deref()
                .expect("exact scope should retain the wrapper")
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            vec!["tool_search"]
        );
        assert!(spec.call_options().web_search.is_none());
        assert!(!spec.call_options().codex_parallel_tool_calls);
        assert_eq!(spec.call_options().parallel_tool_calls, Some(false));
        assert!(
            spec.call_options().max_tokens.unwrap()
                <= client.resolved_model().capabilities.max_output
        );
    }

    #[test]
    fn builder_rejects_cache_and_workspace_identity_drift() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let client = AiClient::new(Default::default(), String::new());

        let cache_error = RunSpecBuilder::new(
            RunProvenance::Server,
            "session-1",
            workspace.path(),
            SessionType::Code,
        )
        .call_options(CallOptions {
            session_id: Some("session-2".into()),
            ..Default::default()
        })
        .build(&client)
        .err()
        .unwrap();
        assert!(matches!(
            cache_error,
            RunSpecError::CacheSessionMismatch { .. }
        ));

        let workspace_error = RunSpecBuilder::new(
            RunProvenance::Server,
            "session-1",
            workspace.path(),
            SessionType::Code,
        )
        .project_dir(Some(outside.path().to_path_buf()))
        .build(&client)
        .err()
        .unwrap();
        assert!(matches!(
            workspace_error,
            RunSpecError::ProjectOutsideWorkingDirectory { .. }
        ));
    }

    #[test]
    fn higher_order_driver_cannot_consume_another_surfaces_spec() {
        let workspace = tempfile::tempdir().unwrap();
        let client = AiClient::new(Default::default(), String::new());
        let spec = RunSpecBuilder::new(
            RunProvenance::Server,
            "session-1",
            workspace.path(),
            SessionType::Code,
        )
        .build(&client)
        .unwrap();

        assert!(matches!(
            spec.into_parts_for(RunProvenance::Hive),
            Err(RunSpecError::DriverProvenanceMismatch {
                expected: "hive",
                actual: "server"
            })
        ));
    }

    #[test]
    fn builder_keeps_mode_refresh_explicit_and_never_infers_it_from_a_subset() {
        let workspace = tempfile::tempdir().unwrap();
        let client = AiClient::new(Default::default(), String::new());
        let tool = AiTool {
            name: "read".into(),
            description: "Read".into(),
            input_schema: serde_json::json!({"type": "object"}),
            prompt: None,
        };

        let tool_bearing = RunSpecBuilder::new(
            RunProvenance::Server,
            "tool-bearing",
            workspace.path(),
            SessionType::Code,
        )
        .call_options(CallOptions {
            tools: Some(vec![tool]),
            ..Default::default()
        })
        .build(&client)
        .unwrap();
        assert!(!tool_bearing.config().refresh_code_tools_on_mode_change);

        let mode_aware = RunSpecBuilder::new(
            RunProvenance::Server,
            "mode-aware",
            workspace.path(),
            SessionType::Code,
        )
        .mode_aware_code_tools(true)
        .call_options(CallOptions {
            tools: Some(vec![AiTool {
                name: "read".into(),
                description: "Read".into(),
                input_schema: serde_json::json!({"type": "object"}),
                prompt: None,
            }]),
            ..Default::default()
        })
        .build(&client)
        .unwrap();
        assert!(mode_aware.config().refresh_code_tools_on_mode_change);

        let tool_free = RunSpecBuilder::new(
            RunProvenance::Server,
            "tool-free",
            workspace.path(),
            SessionType::Code,
        )
        .mode_aware_code_tools(true)
        .call_options(CallOptions::default())
        .build(&client)
        .unwrap();
        assert!(!tool_free.config().refresh_code_tools_on_mode_change);
    }

    #[test]
    fn neutral_worker_mode_is_exactly_bound_tool_free_and_not_tick_driven() {
        let workspace = tempfile::tempdir().unwrap();
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".into(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            String::new(),
        );
        let governor = direct_worker_governor(&workspace.path().join("mitsuro.db"));
        let forbidden_project = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .project_dir(Some(workspace.path().to_path_buf()))
        .user_id(Some("user-1".into()))
        .provider_governor(Some(governor.clone()))
        .context_mode(RunContextMode::worker_conversation(
            "worker-1",
            Arc::new(TestResponseCommitter),
        ))
        .build(&client)
        .err()
        .expect("neutral mode must reject project context");
        assert!(matches!(
            forbidden_project,
            RunSpecError::WorkerConversationForbiddenCapability {
                capability: "project_dir"
            }
        ));

        let spec = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(governor))
        .context_mode(RunContextMode::worker_conversation(
            "worker-1",
            Arc::new(TestResponseCommitter),
        ))
        .call_options(CallOptions {
            tools: Some(vec![AiTool {
                name: "tool_search".into(),
                description: "must be removed".into(),
                input_schema: serde_json::json!({"type": "object"}),
                prompt: None,
            }]),
            web_search: Some(WebSearchConfig::default()),
            web_fetch: Some(WebFetchConfig::default()),
            codex_parallel_tool_calls: true,
            ..Default::default()
        })
        .build(&client)
        .unwrap();

        assert!(spec.call_options().tools.is_none());
        assert!(spec.call_options().web_search.is_none());
        assert!(spec.call_options().web_fetch.is_none());
        assert!(!spec.call_options().codex_parallel_tool_calls);
        assert_eq!(spec.call_options().parallel_tool_calls, Some(false));
        assert_eq!(spec.config().run_budget, Some(RunBudget::with_max_turns(1)));
        assert!(matches!(
            spec.into_parts_for(RunProvenance::Hive),
            Err(RunSpecError::WorkerConversationHigherOrderDriver)
        ));
    }

    #[test]
    fn worker_governor_allows_catalog_refresh_for_same_key_but_rejects_key_drift() {
        let workspace = tempfile::tempdir().unwrap();
        let config = AiClientConfig {
            model: "gpt-5.5".into(),
            provider_id: ProviderId::OpenAI,
            api_format: ApiFormat::OpenAIResponses,
            ..Default::default()
        };
        let current_runtime =
            resolve_model_metadata(config.provider_id, &config.model, config.api_format)
                .with_catalog_provenance(ModelCatalogSource::LiveDynamic, Some("catalog-r2".into()))
                .resolve_runtime();
        let exact_key = current_runtime.key.clone();
        let client = AiClient::new_with_resolved_model(config, String::new(), current_runtime)
            .expect("current exact runtime should build");
        let durable_r1 = direct_worker_governor_with_model(
            &workspace.path().join("mitsuro.db"),
            exact_key.clone(),
            Some("catalog-r1"),
        );
        assert_eq!(
            durable_r1.binding().model_catalog_revision.as_deref(),
            Some("catalog-r1")
        );
        assert_eq!(
            client.resolved_model().catalog_revision.as_deref(),
            Some("catalog-r2")
        );

        let build = |governor| {
            RunSpecBuilder::new(
                RunProvenance::Hive,
                "worker-session",
                workspace.path(),
                SessionType::Hive,
            )
            .user_id(Some("user-1".into()))
            .permission_mode(PermissionMode::Supervised)
            .provider_governor(Some(governor))
            .context_mode(RunContextMode::worker_conversation(
                "worker-1",
                Arc::new(TestResponseCommitter),
            ))
            .build(&client)
        };

        assert!(
            build(durable_r1).is_ok(),
            "a whole-catalog refresh must not invalidate the same exact model key"
        );

        let different_key =
            ModelKey::new(exact_key.provider, exact_key.model_id, ApiFormat::OpenAI);
        let drifted = build(direct_worker_governor_with_model(
            &workspace.path().join("mitsuro.db"),
            different_key,
            Some("catalog-r2"),
        ))
        .err()
        .expect("a different executable model key must remain fenced");
        assert!(matches!(drifted, RunSpecError::WorkerProviderModelMismatch));
    }

    #[test]
    fn isolated_worker_start_requires_the_exact_canonical_governor_database() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::create_dir(workspace.path().join("db")).unwrap();
        let canonical_db = workspace.path().join("mitsuro.db");
        let canonical_alias = workspace.path().join("db").join("..").join("mitsuro.db");
        let other_db = workspace.path().join("other.db");
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".into(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            String::new(),
        );

        let conversation = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(direct_worker_governor(&canonical_alias)))
        .context_mode(RunContextMode::worker_conversation(
            "worker-1",
            Arc::new(TestResponseCommitter),
        ))
        .build(&client)
        .unwrap();
        assert!(conversation
            .validate_start_database_path(&canonical_db)
            .is_ok());
        assert!(matches!(
            conversation.validate_start_database_path(&other_db),
            Err(RunSpecError::WorkerProviderDatabaseMismatch)
        ));

        let goal = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .project_dir(Some(workspace.path().to_path_buf()))
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(worker_goal_governor(
            &canonical_alias,
            "goal-1",
            "attempt-1",
        )))
        .context_mode(worker_goal_mode(worker_goal_context(workspace.path())))
        .execution_tool_allowlist(Some(HashSet::from(["read".to_string()])))
        .call_options(CallOptions {
            tools: Some(vec![AiTool {
                name: "read".into(),
                description: "Read".into(),
                input_schema: serde_json::json!({"type": "object"}),
                prompt: None,
            }]),
            ..Default::default()
        })
        .build(&client)
        .unwrap();
        assert!(goal.validate_start_database_path(&canonical_db).is_ok());
        assert!(matches!(
            goal.validate_start_database_path(&other_db),
            Err(RunSpecError::WorkerProviderDatabaseMismatch)
        ));

        let standard = RunSpecBuilder::new(
            RunProvenance::Server,
            "standard-session",
            workspace.path(),
            SessionType::Code,
        )
        .build(&client)
        .unwrap();
        assert!(standard
            .validate_start_database_path(Path::new("relative-standard.db"))
            .is_ok());
    }

    #[test]
    fn worker_goal_is_exactly_bound_to_workspace_snapshot_and_small_tool_surface() {
        let workspace = tempfile::tempdir().unwrap();
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".into(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            String::new(),
        );
        let spec = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .project_dir(Some(workspace.path().to_path_buf()))
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(worker_goal_governor(
            &workspace.path().join("mitsuro.db"),
            "goal-1",
            "attempt-1",
        )))
        .context_mode(worker_goal_mode(worker_goal_context(workspace.path())))
        .execution_tool_allowlist(Some(HashSet::from([
            "apply_patch".to_string(),
            "read".to_string(),
        ])))
        .call_options(CallOptions {
            tools: Some(vec![
                AiTool {
                    name: "read".into(),
                    description: "Read".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                },
                AiTool {
                    name: "apply_patch".into(),
                    description: "Patch".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                },
                AiTool {
                    name: "agent".into(),
                    description: "Must be removed".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                },
                AiTool {
                    name: "tool_search".into(),
                    description: "Must be removed".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                },
            ]),
            codex_parallel_tool_calls: true,
            ..Default::default()
        })
        .build(&client)
        .unwrap();

        let tool_names = spec
            .call_options()
            .tools
            .as_deref()
            .unwrap()
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(tool_names, HashSet::from(["read", "apply_patch"]));
        assert!(spec.call_options().web_search.is_none());
        assert!(spec.call_options().web_fetch.is_none());
        assert!(!spec.call_options().codex_parallel_tool_calls);
        assert_eq!(spec.call_options().parallel_tool_calls, Some(false));
        assert_eq!(spec.config().run_budget, Some(RunBudget::with_max_turns(4)));
        let goal_context = spec
            .config()
            .context_mode
            .worker_goal_context()
            .expect("Worker Goal context remains attached");
        assert!(goal_context.permits_additional_attempt_work(0, 12, 0, 4));
        assert!(!goal_context.permits_additional_attempt_work(0, 13, 0, 4));
        assert!(!goal_context.permits_additional_attempt_work(0, 12, 0, 5));
        assert_eq!(
            spec.config().project_dir.as_deref(),
            Some(workspace.path().canonicalize().unwrap().as_path())
        );
        let isolated_start = spec.starting_conversation(vec![ModelMessage {
            role: Role::User,
            content: vec![Content::text("ORDINARY-CHAT-HISTORY-LEAK-CANARY".into())],
        }]);
        assert_eq!(isolated_start.len(), 1);
        assert!(matches!(
            isolated_start[0].content.as_slice(),
            [Content::Text { text, .. }]
                if text.contains("[WORKER GOAL TRIGGER v1]")
                    && text.contains("goal-1")
                    && !text.contains("ORDINARY-CHAT-HISTORY-LEAK-CANARY")
        ));
        assert!(matches!(
            spec.into_parts_for(RunProvenance::Hive),
            Err(RunSpecError::WorkerGoalHigherOrderDriver)
        ));
    }

    #[test]
    fn worker_goal_rejects_empty_or_canonicalized_away_tool_surfaces() {
        let workspace = tempfile::tempdir().unwrap();
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".into(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            String::new(),
        );
        let builder = || {
            RunSpecBuilder::new(
                RunProvenance::Hive,
                "worker-session",
                workspace.path(),
                SessionType::Hive,
            )
            .project_dir(Some(workspace.path().to_path_buf()))
            .user_id(Some("user-1".into()))
            .permission_mode(PermissionMode::Supervised)
            .provider_governor(Some(worker_goal_governor(
                &workspace.path().join("mitsuro.db"),
                "goal-1",
                "attempt-1",
            )))
            .context_mode(worker_goal_mode(worker_goal_context(workspace.path())))
        };

        let empty_allowlist = builder()
            .execution_tool_allowlist(Some(HashSet::new()))
            .call_options(CallOptions {
                tools: Some(vec![AiTool {
                    name: "read".into(),
                    description: "Read".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                }]),
                ..Default::default()
            })
            .build(&client)
            .err()
            .expect("an empty Worker Goal grant must fail closed");
        assert!(matches!(
            empty_allowlist,
            RunSpecError::WorkerGoalRequiresToolAllowlist
        ));

        let no_advertised_match = builder()
            .execution_tool_allowlist(Some(HashSet::from(["read".to_string()])))
            .call_options(CallOptions {
                tools: Some(vec![AiTool {
                    name: "glob".into(),
                    description: "Glob".into(),
                    input_schema: serde_json::json!({"type": "object"}),
                    prompt: None,
                }]),
                ..Default::default()
            })
            .build(&client)
            .err()
            .expect("a canonicalized-away Worker Goal grant must fail closed");
        assert!(matches!(
            no_advertised_match,
            RunSpecError::WorkerGoalRequiresToolAllowlist
        ));
    }

    #[test]
    fn worker_goal_rejects_neutral_workspace_binding_drift_and_forbidden_capabilities() {
        let workspace = tempfile::tempdir().unwrap();
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".into(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            String::new(),
        );
        let governor =
            worker_goal_governor(&workspace.path().join("mitsuro.db"), "goal-1", "attempt-1");
        let neutral = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(governor.clone()))
        .context_mode(worker_goal_mode(worker_goal_context(workspace.path())))
        .execution_tool_allowlist(Some(HashSet::from(["read".to_string()])))
        .build(&client)
        .err()
        .unwrap();
        assert!(matches!(neutral, RunSpecError::WorkerGoalRequiresWorkspace));

        let mut mismatched = worker_goal_context(workspace.path()).as_ref().clone();
        mismatched.binding.goal_id = "other-goal".into();
        let drift = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .project_dir(Some(workspace.path().to_path_buf()))
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(governor.clone()))
        .context_mode(worker_goal_mode(Arc::new(mismatched)))
        .execution_tool_allowlist(Some(HashSet::from(["read".to_string()])))
        .build(&client)
        .err()
        .unwrap();
        assert!(matches!(
            drift,
            RunSpecError::WorkerGoalGovernorBindingMismatch { field: "Goal id" }
        ));

        let mut stale_run = worker_goal_context(workspace.path()).as_ref().clone();
        stale_run.binding.run_lease_epoch += 1;
        let stale_run = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .project_dir(Some(workspace.path().to_path_buf()))
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(governor.clone()))
        .context_mode(worker_goal_mode(Arc::new(stale_run)))
        .execution_tool_allowlist(Some(HashSet::from(["read".to_string()])))
        .build(&client)
        .err()
        .unwrap();
        assert!(matches!(
            stale_run,
            RunSpecError::WorkerGoalGovernorBindingMismatch {
                field: "run lease epoch"
            }
        ));

        let mut aggregate_drift = worker_goal_context(workspace.path()).as_ref().clone();
        Arc::make_mut(&mut aggregate_drift.workflow_snapshot).aggregate_revision += 1;
        let aggregate_drift = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .project_dir(Some(workspace.path().to_path_buf()))
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(governor.clone()))
        .context_mode(worker_goal_mode(Arc::new(aggregate_drift)))
        .execution_tool_allowlist(Some(HashSet::from(["read".to_string()])))
        .build(&client)
        .err()
        .unwrap();
        assert!(matches!(
            aggregate_drift,
            RunSpecError::WorkerGoalSnapshotMismatch {
                field: "canonical Workflow aggregate revision"
            }
        ));

        for forbidden_name in ["tool_search", "agent", "task_complete", "workflow_update"] {
            let forbidden_tool = RunSpecBuilder::new(
                RunProvenance::Hive,
                "worker-session",
                workspace.path(),
                SessionType::Hive,
            )
            .project_dir(Some(workspace.path().to_path_buf()))
            .user_id(Some("user-1".into()))
            .permission_mode(PermissionMode::Supervised)
            .provider_governor(Some(governor.clone()))
            .context_mode(worker_goal_mode(worker_goal_context(workspace.path())))
            .execution_tool_allowlist(Some(HashSet::from([forbidden_name.to_string()])))
            .build(&client)
            .err()
            .unwrap();
            assert!(matches!(
                forbidden_tool,
                RunSpecError::WorkerGoalForbiddenTool { tool } if tool == forbidden_name
            ));
        }

        let forbidden_web = RunSpecBuilder::new(
            RunProvenance::Hive,
            "worker-session",
            workspace.path(),
            SessionType::Hive,
        )
        .project_dir(Some(workspace.path().to_path_buf()))
        .user_id(Some("user-1".into()))
        .permission_mode(PermissionMode::Supervised)
        .provider_governor(Some(governor))
        .context_mode(worker_goal_mode(worker_goal_context(workspace.path())))
        .execution_tool_allowlist(Some(HashSet::from(["read".to_string()])))
        .call_options(CallOptions {
            web_search: Some(WebSearchConfig::default()),
            ..Default::default()
        })
        .build(&client)
        .err()
        .unwrap();
        assert!(matches!(
            forbidden_web,
            RunSpecError::WorkerGoalForbiddenCapability {
                capability: "provider-hosted web search"
            }
        ));
    }
}
