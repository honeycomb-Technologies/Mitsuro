//! Agent execution surface hosted inside the standalone Hive process.
//!
//! This module deliberately exposes no HTTP router. It reuses Mitsuro's mature
//! provider/tool/orchestrator bootstrap while keeping process ownership in the
//! independently supervised daemon.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use mitsuro_core::ai::models::{ModelKey, ModelLookupError, ResolvedModelRuntime};
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::storage::{
    ClaimedHiveRun, DaemonFence, Database, HiveGroupRunContext, HiveRunExecutionModeV1,
    HiveRunKind, HiveRunStore, WorkerConversationLane, WorkerRunOrigin,
};
use mitsuro_core::tools::registry::PermissionMode;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::AbortHandle;

use mitsuro_core::agent::{
    freeze_worker_model_pricing, LoopInput, WorkerProviderCallGovernor,
    WorkerProviderGovernorBinding,
};

use crate::ai_bootstrap::{initialize_models, spawn_model_catalog_refresh};
use crate::hive_runtime::runner::{run_hive_session_inner, HiveExecutionEventSink};
use crate::types::AgenticEvent;
use crate::{build_app_state, AppState, HiveRuntimeMode, ServerConfig};

const DEFAULT_EVENT_CAPACITY: usize = 32;
const MAX_EVENT_CAPACITY: usize = 64;
const MAX_EXECUTION_ERROR_BYTES: usize = 8 * 1024;
const DEFAULT_INPUT_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_CANCEL_GRACE: Duration = Duration::from_secs(5);
const DAEMON_LEASE_NAME: &str = "hive-scheduler";

#[derive(Debug, Clone)]
pub struct HiveExecutionHostConfig {
    pub database_path: PathBuf,
    pub working_dir: PathBuf,
    pub event_capacity: usize,
    pub input_registration_timeout: Duration,
    pub cancel_grace: Duration,
}

impl HiveExecutionHostConfig {
    pub fn new(database_path: PathBuf, working_dir: PathBuf) -> Self {
        Self {
            database_path,
            working_dir,
            event_capacity: DEFAULT_EVENT_CAPACITY,
            input_registration_timeout: DEFAULT_INPUT_REGISTRATION_TIMEOUT,
            cancel_grace: DEFAULT_CANCEL_GRACE,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.event_capacity == 0 || self.event_capacity > MAX_EVENT_CAPACITY {
            bail!("Hive execution event capacity must be between 1 and {MAX_EVENT_CAPACITY}");
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ActiveExecution {
    spec: HiveExecutionSpec,
    abort: AbortHandle,
}

/// Immutable execution inputs captured by the durable scheduler claim. The
/// execution host must never re-resolve these fields from mutable session or
/// runtime-state rows after the run has been leased.
#[derive(Debug, Clone)]
pub(crate) struct HiveExecutionSpec {
    pub(crate) claim: ClaimedHiveRun,
    pub(crate) daemon_instance_id: String,
    pub(crate) working_dir: Option<PathBuf>,
    pub(crate) project_dir: Option<PathBuf>,
    pub(crate) model: String,
    pub(crate) model_key: Option<ModelKey>,
    pub(crate) model_catalog_revision: Option<String>,
    pub(crate) crew_slug: Option<String>,
    pub(crate) worker_id: Option<String>,
    pub(crate) permission_mode: PermissionMode,
    /// Frozen group linkage when this run is one member of a group turn.
    pub(crate) hive_group_run: Option<HiveGroupRunContext>,
    /// Exact durable Goal/attempt/plan/step binding for Worker Workflow runs.
    pub(crate) worker_goal: Option<HiveWorkerGoalExecutionSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HiveWorkerGoalExecutionSpec {
    pub(crate) goal_id: String,
    pub(crate) goal_revision: u64,
    pub(crate) workflow_aggregate_revision: u64,
    pub(crate) attempt_id: String,
    pub(crate) plan_revision_id: String,
    pub(crate) plan_revision_number: u64,
    pub(crate) step_id: String,
    pub(crate) step_revision: u64,
    pub(crate) workspace_dir: PathBuf,
    pub(crate) tool_allowlist: Vec<String>,
}

impl HiveExecutionSpec {
    fn from_claim(claim: ClaimedHiveRun, daemon_instance_id: String) -> Result<Self> {
        if claim.run.kind == HiveRunKind::WorkerWorkflowAcceptance
            || claim.run.execution_context.as_ref().is_some_and(|context| {
                matches!(
                    &context.mode,
                    HiveRunExecutionModeV1::WorkerGoalAcceptance { .. }
                )
            })
        {
            bail!(
                "Worker Workflow acceptance is an awaiting-input boundary and cannot be executed"
            );
        }
        let session_id = claim
            .run
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .context("claimed Hive run has no session")?;
        let lease_owner = claim
            .run
            .lease_owner
            .as_deref()
            .context("claimed Hive run has no lease owner")?;
        if lease_owner != daemon_instance_id {
            bail!("claimed Hive run belongs to a different daemon generation");
        }
        if claim.run.lease_token.as_deref() != Some(claim.lease_token.as_str())
            || claim.run.lease_epoch.is_none()
        {
            bail!("claimed Hive run has inconsistent worker lease metadata");
        }
        if claim.run.objective.trim().is_empty() {
            bail!("claimed Hive run objective is empty");
        }
        let configured_working_dir = optional_config_path(&claim.run.config, "working_dir")?;
        let configured_project_dir = optional_config_path(&claim.run.config, "project_dir")?;
        let model = optional_config_string(&claim.run.config, "model")?
            .context("claimed Hive run has no explicit model")?;
        let model_key = optional_config_model_key(&claim.run.config)?;
        let model_catalog_revision =
            optional_config_string(&claim.run.config, "model_catalog_revision")?;
        if model_key.as_ref().is_some_and(|key| key.model_id != model) {
            bail!("claimed Hive model does not match model_key.model_id");
        }
        if model_key.is_none() && model_catalog_revision.is_some() {
            bail!("claimed Hive catalog revision has no model key");
        }
        let crew_slug = optional_config_string(&claim.run.config, "crew_slug")?;
        let configured_worker_id = configured_worker_id(&claim.run.config)?;
        let permission_mode = optional_config_string(&claim.run.config, "permission_mode")?
            .context("claimed Hive run has no explicit permission_mode")?
            .parse::<PermissionMode>()
            .map_err(|error| anyhow::anyhow!("invalid claimed Hive permission_mode: {error}"))?;
        let hive_group_run = optional_config_group_run(&claim.run.config, &claim.run.id)?;
        let (worker_id, working_dir, project_dir, worker_goal) = match (
            claim.run.worker_id.as_deref(),
            claim.run.execution_context.as_ref(),
        ) {
            (Some(worker_id), Some(context)) => {
                context.validate()?;
                if model_key.is_none() {
                    bail!("claimed Worker run has no exact model key");
                }
                if context.worker_id() != worker_id
                    || configured_worker_id.as_deref() != Some(worker_id)
                {
                    bail!("claimed Worker identity does not match its execution binding");
                }
                let governor = claim
                    .run
                    .governor
                    .as_ref()
                    .context("claimed Worker run has no governor projection")?;
                if governor.run_id != claim.run.id
                    || governor.origin.is_none()
                    || governor.lane_key.as_deref()
                        != Some(context.lane().canonical_lane_key()?.as_str())
                {
                    bail!("claimed Worker governor projection is inconsistent");
                }
                validate_worker_run_origin(claim.run.kind, governor.origin.expect("checked"))?;
                match (context.lane(), hive_group_run.as_ref()) {
                    (WorkerConversationLane::DirectMessage, None) => {}
                    (WorkerConversationLane::Group { group_id }, Some(group_run))
                        if group_run.group_id == *group_id && group_run.worker_id == worker_id => {}
                    (WorkerConversationLane::DirectMessage, Some(_)) => {
                        bail!("direct Worker run carries group linkage")
                    }
                    (WorkerConversationLane::Group { .. }, None) => {
                        bail!("group Worker run has no exact group linkage")
                    }
                    _ => bail!("claimed Worker group linkage changed"),
                }
                let (working_dir, project_dir, worker_goal) = match &context.mode {
                    HiveRunExecutionModeV1::WorkerConversationNeutral { .. } => {
                        if claim.run.kind == HiveRunKind::WorkerWorkflow
                            || claim.run.workflow_goal_id.is_some()
                            || claim.run.workflow_attempt_id.is_some()
                        {
                            bail!("neutral Worker run carries Workflow authority");
                        }
                        if configured_working_dir.is_some() || configured_project_dir.is_some() {
                            bail!("neutral Worker run carries a filesystem workspace");
                        }
                        (None, None, None)
                    }
                    HiveRunExecutionModeV1::WorkerWorkspaceAttached {
                        working_dir,
                        project_dir,
                        ..
                    } => {
                        if claim.run.kind == HiveRunKind::WorkerWorkflow
                            || claim.run.workflow_goal_id.is_some()
                            || claim.run.workflow_attempt_id.is_some()
                        {
                            bail!("ordinary attached Worker run carries Workflow authority");
                        }
                        let frozen_working_dir = PathBuf::from(working_dir);
                        let frozen_project_dir = project_dir.as_ref().map(PathBuf::from);
                        if configured_working_dir.as_ref() != Some(&frozen_working_dir)
                            || configured_project_dir != frozen_project_dir
                        {
                            bail!("attached Worker workspace binding changed");
                        }
                        (Some(frozen_working_dir), frozen_project_dir, None)
                    }
                    HiveRunExecutionModeV1::WorkerGoal {
                        working_dir,
                        project_dir,
                        goal_id,
                        goal_revision,
                        workflow_aggregate_revision,
                        attempt_id,
                        plan_revision_id,
                        plan_revision_number,
                        step_id,
                        step_revision,
                        tool_allowlist,
                        ..
                    } => {
                        if claim.run.kind != HiveRunKind::WorkerWorkflow
                            || claim.run.workflow_goal_id.as_deref() != Some(goal_id.as_str())
                            || claim.run.workflow_attempt_id.as_deref() != Some(attempt_id.as_str())
                        {
                            bail!(
                                "Worker Workflow run columns disagree with its execution context"
                            );
                        }
                        if hive_group_run.is_some()
                            || !matches!(context.lane(), WorkerConversationLane::DirectMessage)
                        {
                            bail!("Worker Workflow must use its private direct-message lane");
                        }
                        let workspace_dir = PathBuf::from(working_dir);
                        let frozen_project_dir = PathBuf::from(project_dir);
                        if workspace_dir != frozen_project_dir
                            || configured_working_dir.as_ref() != Some(&workspace_dir)
                            || configured_project_dir.as_ref() != Some(&frozen_project_dir)
                        {
                            bail!("Worker Workflow workspace binding changed");
                        }
                        let goal = HiveWorkerGoalExecutionSpec {
                            goal_id: goal_id.clone(),
                            goal_revision: *goal_revision,
                            workflow_aggregate_revision: *workflow_aggregate_revision,
                            attempt_id: attempt_id.clone(),
                            plan_revision_id: plan_revision_id.clone(),
                            plan_revision_number: *plan_revision_number,
                            step_id: step_id.clone(),
                            step_revision: *step_revision,
                            workspace_dir: workspace_dir.clone(),
                            tool_allowlist: tool_allowlist.clone(),
                        };
                        (Some(workspace_dir), Some(frozen_project_dir), Some(goal))
                    }
                    HiveRunExecutionModeV1::WorkerGoalAcceptance { .. } => {
                        bail!(
                            "Worker Workflow acceptance is an awaiting-input boundary and cannot be executed"
                        )
                    }
                };
                (
                    Some(worker_id.to_string()),
                    working_dir,
                    project_dir,
                    worker_goal,
                )
            }
            (Some(_), None) => bail!("claimed Worker run has no execution context"),
            (None, Some(_)) => bail!("non-Worker run carries a Worker execution context"),
            (None, None) => {
                if claim.run.governor.is_some() || configured_worker_id.is_some() {
                    bail!("non-Worker run carries Worker execution authority");
                }
                if configured_working_dir.is_none() && configured_project_dir.is_none() {
                    bail!(
                        "claimed Hive run has no explicit working_dir or project_dir; refusing daemon-default workspace"
                    );
                }
                if configured_working_dir
                    .as_ref()
                    .is_some_and(|path| !path.is_absolute())
                    || configured_project_dir
                        .as_ref()
                        .is_some_and(|path| !path.is_absolute())
                {
                    bail!("every claimed Hive workspace path must be absolute");
                }
                (None, configured_working_dir, configured_project_dir, None)
            }
        };
        tracing::debug!(
            run_id = %claim.run.id,
            session_id,
            model = ?model,
            model_key = ?model_key,
            model_catalog_revision = ?model_catalog_revision,
            project_dir = ?project_dir,
            crew_slug = ?crew_slug,
            "Captured immutable Hive execution inputs"
        );
        Ok(Self {
            claim,
            daemon_instance_id,
            working_dir,
            project_dir,
            model,
            model_key,
            model_catalog_revision,
            crew_slug,
            worker_id,
            permission_mode,
            hive_group_run,
            worker_goal,
        })
    }

    pub(crate) fn session_id(&self) -> &str {
        self.claim
            .run
            .session_id
            .as_deref()
            .expect("Hive execution spec was validated with a session")
    }

    pub(crate) fn run_id(&self) -> &str {
        &self.claim.run.id
    }

    fn same_execution(&self, other: &Self) -> bool {
        self.run_id() == other.run_id()
            && self.claim.lease_token == other.claim.lease_token
            && self.claim.run.lease_epoch == other.claim.run.lease_epoch
            && self.daemon_instance_id == other.daemon_instance_id
    }

    pub(crate) fn fence(&self) -> DaemonFence {
        DaemonFence {
            lease_name: DAEMON_LEASE_NAME.to_string(),
            owner_id: self.daemon_instance_id.clone(),
            fencing_token: self
                .claim
                .run
                .lease_epoch
                .expect("Hive execution spec was validated with a lease epoch"),
        }
    }

    /// Build the exact run-scoped provider capability from the immutable
    /// scheduler claim. No mutable Worker/session default participates here.
    pub(crate) fn worker_provider_governor(
        &self,
        database_path: &Path,
        owner_user_id: Option<&str>,
        resolved_model: &ResolvedModelRuntime,
    ) -> Result<Option<Arc<WorkerProviderCallGovernor>>> {
        let Some(worker_id) = self.worker_id.as_deref() else {
            return Ok(None);
        };
        let context = self
            .claim
            .run
            .execution_context
            .as_ref()
            .context("claimed Worker run lost its execution context")?;
        let governor = self
            .claim
            .run
            .governor
            .as_ref()
            .context("claimed Worker run lost its governor projection")?;
        let origin = governor
            .origin
            .context("claimed Worker run has no provider origin")?;
        let model_key = self
            .model_key
            .clone()
            .context("claimed Worker run has no exact model key")?;
        let run_lease_epoch = self
            .claim
            .run
            .lease_epoch
            .context("claimed Worker run has no lease epoch")?;
        let pricing = freeze_worker_model_pricing(&model_key, resolved_model)?;
        let capability = WorkerProviderCallGovernor::new(WorkerProviderGovernorBinding {
            db_path: database_path.to_path_buf(),
            worker_id: worker_id.to_string(),
            worker_revision: context.worker_revision(),
            owner_user_id: owner_user_id.map(ToOwned::to_owned),
            session_id: self.session_id().to_string(),
            conversation_lane: context.lane().clone(),
            run_id: self.run_id().to_string(),
            run_lease_token: self.claim.lease_token.clone(),
            run_lease_epoch,
            model_key,
            model_catalog_revision: self.model_catalog_revision.clone(),
            permission_mode: self.permission_mode,
            origin,
            workflow_goal_id: self.worker_goal.as_ref().map(|goal| goal.goal_id.clone()),
            workflow_attempt_id: self
                .worker_goal
                .as_ref()
                .map(|goal| goal.attempt_id.clone()),
            pricing,
            override_grant_id: governor.override_grant_id.clone(),
        })?;
        Ok(Some(Arc::new(capability)))
    }
}

fn configured_worker_id(config: &serde_json::Value) -> Result<Option<String>> {
    let top_level = optional_config_string(config, "worker_id")?;
    let group_worker = match config.get("group") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Object(group)) => match group.get("worker_id") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::String(value)) if !value.trim().is_empty() => {
                Some(value.clone())
            }
            Some(_) => bail!("claimed Hive group worker_id is invalid"),
        },
        Some(_) => bail!("claimed Hive group linkage must be an object or null"),
    };
    if top_level.is_some() && group_worker.is_some() && top_level != group_worker {
        bail!("claimed Hive Worker mirrors disagree");
    }
    Ok(top_level.or(group_worker))
}

fn validate_worker_run_origin(kind: HiveRunKind, origin: WorkerRunOrigin) -> Result<()> {
    let valid = match kind {
        HiveRunKind::WorkerConversation => origin == WorkerRunOrigin::UserDm,
        HiveRunKind::WorkerIntroduction | HiveRunKind::WorkerIntroductionReview => {
            origin == WorkerRunOrigin::UserLifecycleAction
        }
        HiveRunKind::GroupTurn => {
            matches!(
                origin,
                WorkerRunOrigin::UserGroup | WorkerRunOrigin::ScheduledGroup
            )
        }
        HiveRunKind::WorkerHeartbeat => origin == WorkerRunOrigin::Heartbeat,
        HiveRunKind::WorkerMessage => origin == WorkerRunOrigin::WorkerPeer,
        HiveRunKind::Scheduled => origin == WorkerRunOrigin::Scheduled,
        HiveRunKind::WorkerWorkflow => matches!(
            origin,
            WorkerRunOrigin::UserWorkflowActivation | WorkerRunOrigin::WorkflowRollover
        ),
        // Acceptance is provider-free and must never reach execution-host
        // origin validation, even if a corrupt caller manufactures a claim.
        HiveRunKind::WorkerWorkflowAcceptance => false,
        _ => false,
    };
    if !valid {
        bail!("claimed Worker run has an invalid governor origin for its kind");
    }
    Ok(())
}

fn optional_config_path(config: &serde_json::Value, key: &str) -> Result<Option<PathBuf>> {
    optional_config_string(config, key).map(|value| value.map(PathBuf::from))
}

fn optional_config_string(config: &serde_json::Value, key: &str) -> Result<Option<String>> {
    match config.get(key) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if !value.trim().is_empty() => {
            Ok(Some(value.clone()))
        }
        Some(serde_json::Value::String(_)) => bail!("claimed Hive {key} is empty"),
        Some(_) => bail!("claimed Hive {key} must be a string or null"),
    }
}

fn optional_config_model_key(config: &serde_json::Value) -> Result<Option<ModelKey>> {
    match config.get("model_key") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => serde_json::from_value::<ModelKey>(value.clone())
            .map(Some)
            .context("claimed Hive model_key is invalid"),
    }
}

/// Frozen group linkage from the claimed config. The run id is filled from
/// the claim itself so the per-run posting cap is bound to this exact run.
fn optional_config_group_run(
    config: &serde_json::Value,
    run_id: &str,
) -> Result<Option<HiveGroupRunContext>> {
    let Some(group) = config.get("group").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    #[derive(serde::Deserialize)]
    struct ClaimedGroup {
        group_id: String,
        group_turn_id: String,
        worker_id: String,
        max_member_messages_per_turn: u32,
        context_window_messages: u32,
    }
    let claimed = serde_json::from_value::<ClaimedGroup>(group.clone())
        .context("claimed Hive group linkage is invalid")?;
    if claimed.group_id.trim().is_empty()
        || claimed.group_turn_id.trim().is_empty()
        || claimed.worker_id.trim().is_empty()
    {
        bail!("claimed Hive group linkage has empty identifiers");
    }
    Ok(Some(HiveGroupRunContext {
        group_id: claimed.group_id,
        group_turn_id: claimed.group_turn_id,
        run_id: run_id.to_string(),
        worker_id: claimed.worker_id,
        max_member_messages_per_turn: claimed.max_member_messages_per_turn.max(1),
        context_window_messages: claimed.context_window_messages.max(1),
    }))
}

pub struct HiveExecutionHost {
    state: AppState,
    active: RwLock<HashMap<String, ActiveExecution>>,
    credential_refresh: Mutex<()>,
    config: HiveExecutionHostConfig,
}

/// Typed boundary between durable scheduling and process-local execution.
/// Deterministic claim/fence failures must not be mistaken for transient
/// provider failures and retried until their attempt budget is exhausted.
#[derive(Debug)]
pub enum HiveExecutionStartError {
    InvalidClaim(String),
    FenceLost(String),
    CredentialReload(String),
    AlreadyExecuting(String),
}

impl HiveExecutionStartError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::AlreadyExecuting(_))
    }
}

impl std::fmt::Display for HiveExecutionStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidClaim(error) => write!(formatter, "invalid Hive execution claim: {error}"),
            Self::FenceLost(error) => write!(formatter, "Hive execution fence rejected: {error}"),
            Self::CredentialReload(error) => {
                write!(
                    formatter,
                    "Hive credential snapshot could not be reloaded: {error}"
                )
            }
            Self::AlreadyExecuting(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for HiveExecutionStartError {}

/// Keeps a hosted run owned by its caller. Dropping the guard aborts the
/// underlying agent task, so a lost scheduler lease cannot leave detached
/// side effects running in the daemon.
pub struct HiveExecutionGuard {
    abort: AbortHandle,
}

impl Drop for HiveExecutionGuard {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub struct HiveExecutionRun {
    pub events: mpsc::Receiver<AgenticEvent>,
    pub completion: oneshot::Receiver<std::result::Result<(), String>>,
    guard: HiveExecutionGuard,
}

impl HiveExecutionRun {
    pub fn into_parts(
        self,
    ) -> (
        mpsc::Receiver<AgenticEvent>,
        oneshot::Receiver<std::result::Result<(), String>>,
        HiveExecutionGuard,
    ) {
        (self.events, self.completion, self.guard)
    }
}

impl HiveExecutionHost {
    pub async fn build(config: HiveExecutionHostConfig) -> Result<Arc<Self>> {
        // AgenticEvent has variable-sized tool/web variants. Keep the ingress
        // count small; the scheduler's next bounded hop replaces encoded live
        // payloads above 32 KiB with a fixed safe summary.
        config.validate()?;
        let server_config = ServerConfig {
            port: 0,
            working_dir: config.working_dir.clone(),
            database_path: None,
        };
        let state = build_app_state(
            &server_config,
            HiveRuntimeMode::ExecutionHost,
            Some(config.database_path.clone()),
        )
        .await
        .context("initializing the Hive execution host")?;
        Ok(Arc::new(Self {
            state,
            active: RwLock::new(HashMap::new()),
            credential_refresh: Mutex::new(()),
            config,
        }))
    }

    pub async fn start(
        self: &Arc<Self>,
        claim: ClaimedHiveRun,
        daemon_instance_id: String,
        wake_reason: String,
    ) -> std::result::Result<HiveExecutionRun, HiveExecutionStartError> {
        let mut spec = HiveExecutionSpec::from_claim(claim, daemon_instance_id)
            .map_err(|error| HiveExecutionStartError::InvalidClaim(error.to_string()))?;
        self.validate_execution(&spec)
            .await
            .map_err(|error| HiveExecutionStartError::FenceLost(error.to_string()))?;
        self.refresh_credentials_and_catalog_if_needed(&spec)
            .await
            .map_err(|error| HiveExecutionStartError::CredentialReload(error.to_string()))?;
        let resolved_key = match spec.model_key.as_ref() {
            Some(key) => self
                .state
                .model_registry
                .get_model_by_key(key)
                .await
                .map(|_| key.clone())
                .ok_or_else(|| {
                    HiveExecutionStartError::InvalidClaim(format!(
                        "claimed model key {key:?} is absent from the refreshed model catalog"
                    ))
                })?,
            None => self
                .state
                .model_registry
                .resolve_legacy_key(&spec.model)
                .await
                .map_err(|error| match error {
                    ModelLookupError::Ambiguous { .. } => HiveExecutionStartError::InvalidClaim(
                        format!("legacy claimed model cannot be resolved safely: {error}"),
                    ),
                    ModelLookupError::NotFound { .. } => HiveExecutionStartError::InvalidClaim(
                        format!("claimed model is absent from the refreshed catalog: {error}"),
                    ),
                })?,
        };
        spec.model_key = Some(resolved_key);
        // Dynamic catalog refresh can involve network I/O. Close the window
        // by validating the exact durable lease again before spawning any
        // model or tool execution.
        self.validate_execution(&spec)
            .await
            .map_err(|error| HiveExecutionStartError::FenceLost(error.to_string()))?;
        let session_id = spec.session_id().to_string();
        let run_id = spec.run_id().to_string();
        let mut active = self.active.write().await;
        if let Some(existing) = active.get(&session_id) {
            return Err(HiveExecutionStartError::AlreadyExecuting(format!(
                "Hive session {session_id} is already executing run {}",
                existing.spec.run_id()
            )));
        }

        let (event_tx, event_rx) = mpsc::channel(self.config.event_capacity);
        let sink = HiveExecutionEventSink::Bounded(event_tx);
        let state = self.state.clone();
        let manager = state.hive_runtime.clone();
        let session_for_task = session_id.clone();
        let run_for_task = run_id.clone();
        let spec_for_task = spec.clone();
        let error_sink = sink.clone();
        let runner = tokio::spawn(async move {
            let result = run_hive_session_inner(
                state,
                session_for_task,
                run_for_task,
                wake_reason,
                Some(spec_for_task),
                sink,
                manager,
                false,
            )
            .await;
            if let Err(error) = &result {
                let _ = error_sink
                    .send(AgenticEvent::Error {
                        error: bounded_execution_error(error.to_string()),
                    })
                    .await;
            }
            result.map_err(|error| bounded_execution_error(error.to_string()))
        });
        let abort = runner.abort_handle();
        active.insert(
            session_id.clone(),
            ActiveExecution {
                spec,
                abort: abort.clone(),
            },
        );
        drop(active);

        let (completion_tx, completion_rx) = oneshot::channel();
        let weak_host = Arc::downgrade(self);
        tokio::spawn(async move {
            let result = match runner.await {
                Ok(result) => result,
                Err(error) if error.is_cancelled() => {
                    Err("Hive execution task was cancelled".to_string())
                }
                Err(error) => Err(bounded_execution_error(format!(
                    "Hive execution task failed: {error}"
                ))),
            };
            if let Some(host) = weak_host.upgrade() {
                let mut active = host.active.write().await;
                if active
                    .get(&session_id)
                    .is_some_and(|entry| entry.spec.run_id() == run_id)
                {
                    active.remove(&session_id);
                }
            }
            let _ = completion_tx.send(result);
        });

        Ok(HiveExecutionRun {
            events: event_rx,
            completion: completion_rx,
            guard: HiveExecutionGuard { abort },
        })
    }

    /// Reload static credentials before every durable claim. The server
    /// process writes the file atomically; this host loads a complete snapshot
    /// off-thread, swaps it under one write lock, and refreshes catalogs only
    /// when credential content actually changed. No credential values or
    /// fingerprints are logged.
    async fn refresh_credentials_and_catalog_if_needed(
        &self,
        spec: &HiveExecutionSpec,
    ) -> Result<bool> {
        let _refresh_guard = self.credential_refresh.lock().await;
        let loaded = tokio::task::spawn_blocking(CredentialStore::load)
            .await
            .context("joining Hive credential snapshot reload")?
            .context("loading Hive credential snapshot")?;
        let changed = {
            let mut current = self.state.credential_store.write().await;
            replace_credential_snapshot_if_changed(&mut current, loaded.clone())
        };
        let claimed_model_unknown = match spec.model_key.as_ref() {
            Some(key) => self
                .state
                .model_registry
                .get_model_by_key(key)
                .await
                .is_none(),
            None => self
                .state
                .model_registry
                .resolve_legacy_key(&spec.model)
                .await
                .is_err(),
        };
        if changed || claimed_model_unknown {
            initialize_models(&self.state.model_registry, self.state.db_path.as_path()).await;
            spawn_model_catalog_refresh(
                self.state.model_registry.clone(),
                self.state.credential_store.clone(),
                self.state.db_path.clone(),
            );
            tracing::info!(
                credentials_changed = changed,
                claimed_model_was_unknown = claimed_model_unknown,
                "Refreshed Hive credential/model execution snapshot"
            );
        }
        Ok(changed)
    }

    /// Deliver an already-governed input to the active orchestrator. The short
    /// registration wait closes the startup race between scheduler claim and
    /// the runner publishing its input channel.
    pub async fn send_input(&self, session_id: &str, input: LoopInput) -> Result<()> {
        self.send_input_to_execution(session_id, None, input).await
    }

    /// Deliver a control only to the exact durable run that owns it. This is
    /// required for approval outbox items: a late decision for run A must
    /// never be accepted by a replacement run B in the same session.
    pub async fn send_input_for_run(
        &self,
        session_id: &str,
        run_id: &str,
        input: LoopInput,
    ) -> Result<()> {
        self.send_input_to_execution(session_id, Some(run_id), input)
            .await
    }

    async fn send_input_to_execution(
        &self,
        session_id: &str,
        expected_run_id: Option<&str>,
        input: LoopInput,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.config.input_registration_timeout;
        loop {
            let execution = self.active_execution(session_id, expected_run_id).await?;
            if let Some(sender) = self
                .state
                .session_inputs
                .read()
                .await
                .get(session_id)
                .cloned()
            {
                // Revalidate after the channel becomes visible, not merely
                // before waiting for registration. This closes the lease-loss
                // window during host startup.
                self.validate_execution(&execution.spec).await?;
                let current = self.active_execution(session_id, expected_run_id).await?;
                return deliver_input_to_exact_execution(
                    &current.spec,
                    &execution.spec,
                    expected_run_id,
                    &sender,
                    input,
                );
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("Hive execution input channel was not registered in time");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn active_execution(
        &self,
        session_id: &str,
        expected_run_id: Option<&str>,
    ) -> Result<ActiveExecution> {
        let active = self.active.read().await;
        let execution = active
            .get(session_id)
            .cloned()
            .context("Hive session has no active execution")?;
        if expected_run_id.is_some_and(|run_id| run_id != execution.spec.run_id()) {
            bail!("Hive control targets a stale run");
        }
        Ok(execution)
    }

    async fn validate_execution(&self, spec: &HiveExecutionSpec) -> Result<()> {
        validate_execution_spec(self.state.db_path.as_ref().clone(), spec.clone()).await
    }

    /// Cooperatively cancel, then enforce a finite grace period. This is used
    /// both for user cancellation and scheduler fencing loss.
    pub async fn cancel(&self, session_id: &str, expected_run_id: Option<&str>) -> Result<()> {
        let execution = match self.active_execution(session_id, expected_run_id).await {
            Ok(execution) => execution,
            Err(_) if expected_run_id.is_some() => return Ok(()),
            Err(_) => return Ok(()),
        };
        let sender = self
            .state
            .session_inputs
            .read()
            .await
            .get(session_id)
            .cloned();
        if let Some(sender) = sender {
            let _ = sender.send(LoopInput::Cancel);
        }
        if !self
            .active
            .read()
            .await
            .get(session_id)
            .is_some_and(|entry| entry.spec.same_execution(&execution.spec))
        {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.config.cancel_grace;
        loop {
            if !self
                .active
                .read()
                .await
                .get(session_id)
                .is_some_and(|entry| entry.spec.same_execution(&execution.spec))
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                execution.abort.abort();
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn abort(&self, session_id: &str, expected_run_id: Option<&str>) {
        if let Ok(execution) = self.active_execution(session_id, expected_run_id).await {
            execution.abort.abort();
        }
    }
}

fn bounded_execution_error(mut error: String) -> String {
    if error.len() <= MAX_EXECUTION_ERROR_BYTES {
        return error;
    }
    let mut cutoff = MAX_EXECUTION_ERROR_BYTES.saturating_sub(" [truncated]".len());
    while cutoff > 0 && !error.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    error.truncate(cutoff);
    error.push_str(" [truncated]");
    error
}

fn replace_credential_snapshot_if_changed(
    current: &mut CredentialStore,
    loaded: CredentialStore,
) -> bool {
    if *current == loaded {
        return false;
    }
    *current = loaded;
    true
}

fn deliver_input_to_exact_execution(
    current: &HiveExecutionSpec,
    initially_observed: &HiveExecutionSpec,
    expected_run_id: Option<&str>,
    sender: &tokio::sync::mpsc::UnboundedSender<LoopInput>,
    input: LoopInput,
) -> Result<()> {
    if expected_run_id.is_some_and(|run_id| run_id != current.run_id()) {
        bail!("Hive control targets a stale run");
    }
    if !current.same_execution(initially_observed) {
        bail!("Hive execution changed while delivering input");
    }
    sender
        .send(input)
        .map_err(|_| anyhow::anyhow!("Hive execution no longer accepts input"))
}

pub(crate) async fn validate_execution_spec(
    database_path: PathBuf,
    spec: HiveExecutionSpec,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let store = HiveRunStore::new(Database::new(&database_path)?);
        anyhow::ensure!(
            store.validate_claimed_execution_fenced(&spec.claim, &spec.fence(), Utc::now())?,
            "Hive execution fence is no longer current"
        );
        Ok(())
    })
    .await
    .context("joining Hive execution fence validation")?
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use chrono::Utc;
    use mitsuro_core::agent::LoopInput;
    use mitsuro_core::ai::models::{ApiFormat, ModelAuthScope, ModelCatalogSource, ModelMetadata};
    use mitsuro_core::ai::providers::ProviderId;
    use mitsuro_core::ai::types::Content;
    use mitsuro_core::hive::HiveRunStatus;
    use mitsuro_core::storage::credentials::CredentialStore;
    use mitsuro_core::storage::{
        ClaimedHiveRun, HiveRun, HiveRunExecutionContextV1, HiveRunKind, WorkerConversationLane,
        WorkerRunGovernorProjection, WorkerRunOrigin, WorkspaceMode,
    };
    use mitsuro_core::tools::registry::PermissionMode;

    use super::{
        bounded_execution_error, deliver_input_to_exact_execution,
        replace_credential_snapshot_if_changed, HiveExecutionHostConfig, HiveExecutionSpec,
        DEFAULT_EVENT_CAPACITY, MAX_EVENT_CAPACITY, MAX_EXECUTION_ERROR_BYTES,
    };

    #[test]
    fn execution_host_buffers_and_errors_are_bounded() {
        let config = HiveExecutionHostConfig::new("runtime.db".into(), "/work".into());
        assert_eq!(config.event_capacity, DEFAULT_EVENT_CAPACITY);
        assert!(config.validate().is_ok());

        let mut oversized = config;
        oversized.event_capacity = MAX_EVENT_CAPACITY + 1;
        assert!(oversized.validate().is_err());

        let bounded = bounded_execution_error("error".repeat(MAX_EXECUTION_ERROR_BYTES));
        assert!(bounded.len() <= MAX_EXECUTION_ERROR_BYTES);
        assert!(bounded.ends_with(" [truncated]"));
    }

    fn claimed_run_for_test(run_id: &str, config: serde_json::Value) -> ClaimedHiveRun {
        let lease_token = format!("lease-{run_id}");
        let run = HiveRun {
            id: run_id.into(),
            controller_id: "controller-1".into(),
            session_id: Some("session-1".into()),
            schedule_id: None,
            occurrence_id: None,
            worker_id: None,
            objective_message_id: None,
            execution_context: None,
            conversation_through_message_id: None,
            response_message_id: None,
            response_provider_call_id: None,
            response_group_message_id: None,
            workflow_goal_id: None,
            workflow_attempt_id: None,
            governor: None,
            kind: HiveRunKind::Dispatch,
            objective: "ship it".into(),
            config,
            status: HiveRunStatus::Leased,
            priority: 0,
            concurrency_key: None,
            scheduled_for: None,
            available_at: Utc::now().to_rfc3339(),
            wake_at: None,
            attempt_count: 1,
            max_attempts: 5,
            lease_owner: Some("daemon:boot:one".into()),
            lease_token: Some(lease_token.clone()),
            lease_epoch: Some(9),
            lease_expires_at: Some(Utc::now().to_rfc3339()),
            heartbeat_at: Some(Utc::now().to_rfc3339()),
            last_stop_reason: None,
            last_error: None,
            outcome: None,
            created_at: Utc::now().to_rfc3339(),
            started_at: None,
            finished_at: None,
            updated_at: Utc::now().to_rfc3339(),
        };
        ClaimedHiveRun {
            run,
            attempt_id: format!("attempt-{run_id}"),
            attempt_no: 1,
            lease_token,
        }
    }

    fn execution_spec_for_test(run_id: &str, config: serde_json::Value) -> HiveExecutionSpec {
        HiveExecutionSpec::from_claim(
            claimed_run_for_test(run_id, config),
            "daemon:boot:one".into(),
        )
        .expect("test execution spec should be valid")
    }

    #[test]
    fn claimed_config_is_frozen_into_execution_spec() {
        let run = HiveRun {
            id: "run-1".into(),
            controller_id: "controller-1".into(),
            session_id: Some("session-1".into()),
            schedule_id: None,
            occurrence_id: None,
            worker_id: None,
            objective_message_id: None,
            execution_context: None,
            conversation_through_message_id: None,
            response_message_id: None,
            response_provider_call_id: None,
            response_group_message_id: None,
            workflow_goal_id: None,
            workflow_attempt_id: None,
            governor: None,
            kind: HiveRunKind::Scheduled,
            objective: "ship it".into(),
            config: serde_json::json!({
                "working_dir": "/claimed/work",
                "project_dir": "/claimed/project",
                "model": "provider:claimed",
                "model_key": {
                    "provider": "grok",
                    "model_id": "provider:claimed",
                    "auth_scope": "oauth",
                    "api_format": "open_ai_responses"
                },
                "model_catalog_revision": "catalog-42",
                "crew_slug": "release",
                "permission_mode": "supervised"
            }),
            status: HiveRunStatus::Leased,
            priority: 0,
            concurrency_key: None,
            scheduled_for: None,
            available_at: Utc::now().to_rfc3339(),
            wake_at: None,
            attempt_count: 1,
            max_attempts: 5,
            lease_owner: Some("daemon:boot:one".into()),
            lease_token: Some("lease-token".into()),
            lease_epoch: Some(9),
            lease_expires_at: Some(Utc::now().to_rfc3339()),
            heartbeat_at: Some(Utc::now().to_rfc3339()),
            last_stop_reason: None,
            last_error: None,
            outcome: None,
            created_at: Utc::now().to_rfc3339(),
            started_at: None,
            finished_at: None,
            updated_at: Utc::now().to_rfc3339(),
        };
        let claim = ClaimedHiveRun {
            run,
            attempt_id: "attempt-1".into(),
            attempt_no: 1,
            lease_token: "lease-token".into(),
        };
        let spec = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into()).unwrap();
        assert_eq!(
            spec.working_dir.as_deref(),
            Some(std::path::Path::new("/claimed/work"))
        );
        assert_eq!(
            spec.project_dir.as_deref(),
            Some(std::path::Path::new("/claimed/project"))
        );
        assert_eq!(spec.model, "provider:claimed");
        let model_key = spec.model_key.as_ref().expect("exact key must be frozen");
        assert_eq!(model_key.provider, ProviderId::Grok);
        assert_eq!(model_key.model_id, "provider:claimed");
        assert_eq!(model_key.auth_scope, Some(ModelAuthScope::OAuth));
        assert_eq!(model_key.api_format, ApiFormat::OpenAIResponses);
        assert_eq!(spec.model_catalog_revision.as_deref(), Some("catalog-42"));
        assert_eq!(spec.crew_slug.as_deref(), Some("release"));
        assert_eq!(spec.permission_mode, PermissionMode::Supervised);
    }

    #[test]
    fn claimed_config_rejects_model_key_mismatch() {
        let claim = claimed_run_for_test(
            "run-mismatch",
            serde_json::json!({
                "working_dir": "/claimed/work",
                "model": "grok-4.5",
                "model_key": {
                    "provider": "grok",
                    "model_id": "different-model",
                    "auth_scope": "oauth",
                    "api_format": "open_ai_responses"
                },
                "model_catalog_revision": "catalog-42",
                "permission_mode": "autonomous"
            }),
        );
        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("a mismatched legacy mirror must fail closed");
        assert!(error
            .to_string()
            .contains("does not match model_key.model_id"));
    }

    #[test]
    fn claimed_execution_without_workspace_fails_closed() {
        let mut run = HiveRun {
            id: "run-1".into(),
            controller_id: "controller-1".into(),
            session_id: Some("session-1".into()),
            schedule_id: None,
            occurrence_id: None,
            worker_id: None,
            objective_message_id: None,
            execution_context: None,
            conversation_through_message_id: None,
            response_message_id: None,
            response_provider_call_id: None,
            response_group_message_id: None,
            workflow_goal_id: None,
            workflow_attempt_id: None,
            governor: None,
            kind: HiveRunKind::Scheduled,
            objective: "ship it".into(),
            config: serde_json::json!({
                "model": "provider:claimed",
                "permission_mode": "autonomous"
            }),
            status: HiveRunStatus::Leased,
            priority: 0,
            concurrency_key: None,
            scheduled_for: None,
            available_at: Utc::now().to_rfc3339(),
            wake_at: None,
            attempt_count: 1,
            max_attempts: 5,
            lease_owner: Some("daemon:boot:one".into()),
            lease_token: Some("lease-token".into()),
            lease_epoch: Some(9),
            lease_expires_at: Some(Utc::now().to_rfc3339()),
            heartbeat_at: Some(Utc::now().to_rfc3339()),
            last_stop_reason: None,
            last_error: None,
            outcome: None,
            created_at: Utc::now().to_rfc3339(),
            started_at: None,
            finished_at: None,
            updated_at: Utc::now().to_rfc3339(),
        };
        let claim = ClaimedHiveRun {
            run: run.clone(),
            attempt_id: "attempt-1".into(),
            attempt_no: 1,
            lease_token: "lease-token".into(),
        };
        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("missing claimed workspace must be rejected");
        assert!(error.to_string().contains("no explicit working_dir"));

        run.config = serde_json::json!({
            "project_dir": "/claimed/project",
            "model": "provider:claimed",
            "permission_mode": "autonomous"
        });
        let claim = ClaimedHiveRun {
            run,
            attempt_id: "attempt-2".into(),
            attempt_no: 1,
            lease_token: "lease-token".into(),
        };
        assert!(HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into()).is_ok());
    }

    #[test]
    fn neutral_worker_claim_allows_no_workspace_and_rejects_path_leakage() {
        let mut claim = claimed_run_for_test(
            "worker-neutral",
            serde_json::json!({
                "worker_id": "worker-1",
                "model": "grok-4.5",
                "model_key": {
                    "provider": "grok",
                    "model_id": "grok-4.5",
                    "auth_scope": "oauth",
                    "api_format": "open_ai_responses"
                },
                "model_catalog_revision": "catalog-priced",
                "permission_mode": "supervised"
            }),
        );
        claim.run.kind = HiveRunKind::WorkerConversation;
        claim.run.worker_id = Some("worker-1".into());
        claim.run.execution_context = Some(
            HiveRunExecutionContextV1::worker_conversation_neutral(
                "worker-1",
                3,
                WorkerConversationLane::DirectMessage,
            )
            .expect("neutral context"),
        );
        claim.run.governor = Some(WorkerRunGovernorProjection {
            run_id: claim.run.id.clone(),
            origin: Some(WorkerRunOrigin::UserDm),
            lane_key: Some("dm".into()),
            gate_reason: None,
            next_eligible_at: None,
            policy_revision: Some(1),
            override_grant_id: None,
        });
        let spec = HiveExecutionSpec::from_claim(claim.clone(), "daemon:boot:one".into())
            .expect("neutral Worker claim");
        assert!(spec.working_dir.is_none());
        assert!(spec.project_dir.is_none());
        assert_eq!(spec.worker_id.as_deref(), Some("worker-1"));
        let mut metadata = ModelMetadata::new("grok-4.5", "Grok 4.5", ProviderId::Grok)
            .with_transport(ApiFormat::OpenAIResponses)
            .with_catalog_provenance(
                ModelCatalogSource::LiveDynamic,
                Some("catalog-priced".into()),
            );
        metadata.auth_scope = Some(ModelAuthScope::OAuth);
        metadata.input_price = Some(1.25);
        metadata.output_price = Some(2.5);
        let resolved_model = metadata.resolve_runtime();
        let provider_governor = spec
            .worker_provider_governor(
                Path::new("/tmp/mitsuro-worker-runtime.db"),
                Some("alice"),
                &resolved_model,
            )
            .expect("exact provider governor")
            .expect("Worker provider governor");
        assert_eq!(provider_governor.binding().worker_id, "worker-1");
        assert_eq!(provider_governor.binding().worker_revision, 3);
        assert_eq!(provider_governor.binding().run_id, "worker-neutral");
        assert_eq!(
            provider_governor.binding().conversation_lane,
            WorkerConversationLane::DirectMessage
        );
        assert_eq!(provider_governor.binding().origin, WorkerRunOrigin::UserDm);
        let pricing = provider_governor
            .binding()
            .pricing
            .as_ref()
            .expect("known exact catalog prices must be frozen");
        assert_eq!(pricing.currency.as_deref(), Some("USD"));
        assert_eq!(pricing.input_microunits_per_million, Some(1_250_000));
        assert_eq!(pricing.output_microunits_per_million, Some(2_500_000));
        assert_eq!(pricing.catalog_source, "live_dynamic");
        assert_eq!(pricing.catalog_revision.as_deref(), Some("catalog-priced"));

        let mut unknown_runtime = resolved_model.clone();
        unknown_runtime.input_price = None;
        unknown_runtime.output_price = None;
        let unknown_governor = spec
            .worker_provider_governor(
                Path::new("/tmp/mitsuro-worker-runtime.db"),
                Some("alice"),
                &unknown_runtime,
            )
            .expect("unknown pricing is a valid exact model runtime")
            .expect("Worker provider governor");
        assert!(unknown_governor.binding().pricing.is_none());

        let mut refreshed_runtime = resolved_model.clone();
        refreshed_runtime.catalog_revision = Some("catalog-refreshed".into());
        refreshed_runtime.input_price = Some(3.0);
        let refreshed_governor = spec
            .worker_provider_governor(
                Path::new("/tmp/mitsuro-worker-runtime.db"),
                Some("alice"),
                &refreshed_runtime,
            )
            .expect("an unrelated catalog refresh must not brick the exact model key")
            .expect("Worker provider governor");
        assert_eq!(
            refreshed_governor
                .binding()
                .model_catalog_revision
                .as_deref(),
            Some("catalog-priced")
        );
        let refreshed_pricing = refreshed_governor
            .binding()
            .pricing
            .as_ref()
            .expect("refreshed exact row retains known pricing");
        assert_eq!(
            refreshed_pricing.catalog_revision.as_deref(),
            Some("catalog-refreshed")
        );
        assert_eq!(
            refreshed_pricing.input_microunits_per_million,
            Some(3_000_000)
        );

        let mut wrong_model_runtime = resolved_model;
        wrong_model_runtime.key.model_id = "other-model".into();
        wrong_model_runtime.wire_model_id = "other-model".into();
        let error = spec
            .worker_provider_governor(
                Path::new("/tmp/mitsuro-worker-runtime.db"),
                Some("alice"),
                &wrong_model_runtime,
            )
            .expect_err("cross-model pricing must fail closed");
        assert!(error.to_string().contains("durable model key"));

        let mut missing_key = claim.clone();
        missing_key
            .run
            .config
            .as_object_mut()
            .expect("object config")
            .remove("model_key");
        missing_key
            .run
            .config
            .as_object_mut()
            .expect("object config")
            .remove("model_catalog_revision");
        let error = HiveExecutionSpec::from_claim(missing_key, "daemon:boot:one".into())
            .expect_err("Worker execution must not resolve a legacy model key");
        assert!(error.to_string().contains("no exact model key"));

        claim.run.config["working_dir"] = serde_json::json!("/daemon/cwd");
        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("neutral Worker must reject workspace leakage");
        assert!(error.to_string().contains("carries a filesystem workspace"));
    }

    #[test]
    fn worker_goal_claim_freezes_exact_goal_workspace_and_governor_authority() {
        let mut claim = claimed_run_for_test(
            "worker-goal",
            serde_json::json!({
                "worker_id": "worker-1",
                "working_dir": "/claimed/work",
                "project_dir": "/claimed/work",
                "model": "grok-4.5",
                "model_key": {
                    "provider": "grok",
                    "model_id": "grok-4.5",
                    "auth_scope": "oauth",
                    "api_format": "open_ai_responses"
                },
                "permission_mode": "supervised"
            }),
        );
        claim.run.kind = HiveRunKind::WorkerWorkflow;
        claim.run.worker_id = Some("worker-1".into());
        claim.run.workflow_goal_id = Some("goal-1".into());
        claim.run.workflow_attempt_id = Some("workflow-attempt-1".into());
        claim.run.execution_context = Some(
            HiveRunExecutionContextV1::worker_goal(
                "worker-1",
                7,
                WorkspaceMode::Selected,
                "/claimed/work",
                "/claimed/work",
                "goal-1",
                11,
                11,
                "workflow-attempt-1",
                "plan-1",
                3,
                "step-1",
                5,
                vec!["read".into(), "apply_patch".into()],
            )
            .expect("Worker Goal context"),
        );
        claim.run.governor = Some(WorkerRunGovernorProjection {
            run_id: claim.run.id.clone(),
            origin: Some(WorkerRunOrigin::UserWorkflowActivation),
            lane_key: Some("dm".into()),
            gate_reason: None,
            next_eligible_at: None,
            policy_revision: Some(2),
            override_grant_id: None,
        });

        let spec = HiveExecutionSpec::from_claim(claim.clone(), "daemon:boot:one".into())
            .expect("exact Worker Goal claim");
        let goal = spec.worker_goal.expect("Goal authority");
        assert_eq!(goal.goal_id, "goal-1");
        assert_eq!(goal.attempt_id, "workflow-attempt-1");
        assert_eq!(goal.workspace_dir, Path::new("/claimed/work"));
        assert_eq!(
            goal.tool_allowlist,
            vec!["read".to_string(), "apply_patch".to_string()]
        );

        let mut mismatched = claim.clone();
        mismatched.run.workflow_goal_id = Some("other-goal".into());
        let error = HiveExecutionSpec::from_claim(mismatched, "daemon:boot:one".into())
            .expect_err("Goal column drift must fail closed");
        assert!(error.to_string().contains("columns disagree"));

        claim.run.governor.as_mut().expect("governor").origin = Some(WorkerRunOrigin::Heartbeat);
        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("Worker Goal must reject an unrelated origin");
        assert!(error.to_string().contains("invalid governor origin"));
    }

    #[test]
    fn worker_goal_acceptance_claim_is_rejected_before_host_execution() {
        let mut claim = claimed_run_for_test(
            "worker-goal-acceptance",
            serde_json::json!({
                "working_dir": "/claimed/work",
                "project_dir": "/claimed/work",
                "model": "grok-4.5",
                "permission_mode": "supervised"
            }),
        );
        claim.run.kind = HiveRunKind::WorkerWorkflowAcceptance;

        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("awaiting-input acceptance must never become a hosted execution");
        assert!(error
            .to_string()
            .contains("awaiting-input boundary and cannot be executed"));
    }

    #[test]
    fn claimed_execution_without_model_fails_closed() {
        let run = HiveRun {
            id: "run-no-model".into(),
            controller_id: "controller-1".into(),
            session_id: Some("session-1".into()),
            schedule_id: None,
            occurrence_id: None,
            worker_id: None,
            objective_message_id: None,
            execution_context: None,
            conversation_through_message_id: None,
            response_message_id: None,
            response_provider_call_id: None,
            response_group_message_id: None,
            workflow_goal_id: None,
            workflow_attempt_id: None,
            governor: None,
            kind: HiveRunKind::Dispatch,
            objective: "ship it".into(),
            config: serde_json::json!({
                "working_dir": "/claimed/work",
                "permission_mode": "autonomous"
            }),
            status: HiveRunStatus::Leased,
            priority: 0,
            concurrency_key: None,
            scheduled_for: None,
            available_at: Utc::now().to_rfc3339(),
            wake_at: None,
            attempt_count: 1,
            max_attempts: 5,
            lease_owner: Some("daemon:boot:one".into()),
            lease_token: Some("lease-token".into()),
            lease_epoch: Some(9),
            lease_expires_at: Some(Utc::now().to_rfc3339()),
            heartbeat_at: Some(Utc::now().to_rfc3339()),
            last_stop_reason: None,
            last_error: None,
            outcome: None,
            created_at: Utc::now().to_rfc3339(),
            started_at: None,
            finished_at: None,
            updated_at: Utc::now().to_rfc3339(),
        };
        let claim = ClaimedHiveRun {
            run,
            attempt_id: "attempt-1".into(),
            attempt_no: 1,
            lease_token: "lease-token".into(),
        };
        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("missing claimed model must be rejected");
        assert!(error.to_string().contains("no explicit model"));
    }

    #[test]
    fn claimed_execution_without_permission_mode_fails_closed() {
        let mut claim = execution_spec_for_test(
            "run-with-permission",
            serde_json::json!({
                "working_dir": "/claimed/work",
                "model": "provider:claimed",
                "permission_mode": "supervised"
            }),
        )
        .claim;
        claim
            .run
            .config
            .as_object_mut()
            .unwrap()
            .remove("permission_mode");
        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("missing claimed permission mode must be rejected");
        assert!(error.to_string().contains("no explicit permission_mode"));
    }

    #[test]
    fn every_present_claimed_workspace_path_must_be_absolute() {
        let run = HiveRun {
            id: "run-relative-project".into(),
            controller_id: "controller-1".into(),
            session_id: Some("session-1".into()),
            schedule_id: None,
            occurrence_id: None,
            worker_id: None,
            objective_message_id: None,
            execution_context: None,
            conversation_through_message_id: None,
            response_message_id: None,
            response_provider_call_id: None,
            response_group_message_id: None,
            workflow_goal_id: None,
            workflow_attempt_id: None,
            governor: None,
            kind: HiveRunKind::Dispatch,
            objective: "ship it".into(),
            config: serde_json::json!({
                "working_dir": "/claimed/work",
                "project_dir": "relative/project",
                "model": "provider:claimed",
                "permission_mode": "autonomous"
            }),
            status: HiveRunStatus::Leased,
            priority: 0,
            concurrency_key: None,
            scheduled_for: None,
            available_at: Utc::now().to_rfc3339(),
            wake_at: None,
            attempt_count: 1,
            max_attempts: 5,
            lease_owner: Some("daemon:boot:one".into()),
            lease_token: Some("lease-token".into()),
            lease_epoch: Some(9),
            lease_expires_at: Some(Utc::now().to_rfc3339()),
            heartbeat_at: Some(Utc::now().to_rfc3339()),
            last_stop_reason: None,
            last_error: None,
            outcome: None,
            created_at: Utc::now().to_rfc3339(),
            started_at: None,
            finished_at: None,
            updated_at: Utc::now().to_rfc3339(),
        };
        let claim = ClaimedHiveRun {
            run,
            attempt_id: "attempt-1".into(),
            attempt_no: 1,
            lease_token: "lease-token".into(),
        };
        let error = HiveExecutionSpec::from_claim(claim, "daemon:boot:one".into())
            .expect_err("relative project_dir must be rejected even with absolute working_dir");
        assert!(error
            .to_string()
            .contains("every claimed Hive workspace path"));
    }

    #[test]
    fn credential_snapshot_is_replaced_only_when_content_changes() {
        let mut current = CredentialStore::default();
        current.set(ProviderId::OpenAI, "first-value".into());

        let unchanged = current.clone();
        assert!(!replace_credential_snapshot_if_changed(
            &mut current,
            unchanged
        ));

        let mut rotated = current.clone();
        rotated.set(ProviderId::OpenAI, "rotated-value".into());
        assert!(replace_credential_snapshot_if_changed(
            &mut current,
            rotated
        ));
        assert_eq!(
            current.get(&ProviderId::OpenAI).map(String::as_str),
            Some("rotated-value")
        );
    }

    #[test]
    fn stale_exact_run_input_cannot_reach_replacement_execution() {
        let replacement = execution_spec_for_test(
            "run-b",
            serde_json::json!({
                "working_dir": "/claimed/work",
                "model": "provider:claimed",
                "permission_mode": "supervised"
            }),
        );
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let error = deliver_input_to_exact_execution(
            &replacement,
            &replacement,
            Some("run-a"),
            &sender,
            LoopInput::Steer {
                pending_id: Some("pending-a".into()),
                content: vec![Content::text("Response to question-a:\nyes".into())],
            },
        )
        .expect_err("run A response must not be delivered to replacement run B");
        assert!(error.to_string().contains("stale run"));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}
