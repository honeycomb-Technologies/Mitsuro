//! Agent system for Mitsuro
//!
//! ## Orchestrator (the canonical agentic loop)
//! - `AgenticOrchestrator` - Unified loop: streaming, tools, plans, failure detection
//! - `LoopEvent` / `LoopInput` - Event protocol between orchestrator and consumers
//! - `OrchestratorConfig` / `OrchestratorServices` - Configuration and dependencies
//!
//! ## Core Components
//! - `LoopEvent` / `LoopInput` - The single canonical event protocol
//! - `AgentCancellation` - Proper task cancellation
//!
//! ## Hooks
//! - `SafetyHook` - Blocks dangerous bash commands
//! - `LoggingHook` - Logs all tool executions
//! - `UserHookManager` - User-configurable hooks
//!
//! ## Pinch (Context Continuation)
//! - `PinchContext` - Structured context for session transitions
//! - `SummarizationResult` - Output from summarization agent
//!
//! ## Sub-agents
//! - `SubAgentPool` - Concurrent execution of lightweight agents
//! - `SubAgentTask` - Task configuration for sub-agents
//!
//! ## Autonomy (Hive)
//! - `TickEngine` - Autonomous wake/sleep driver for Hive sessions
//! - `coordinator_prompt` - Hive coordinator system prompt surface
//! - dynamic delegated agents - Background work through AgentSpec and lifecycle controls
//! - `AutoClassifierHook` - Autonomous tool-call guardrail hook
//!
//! ## Builder Swarm (Octopod)
//! - `subagent::build_context` - Shared coordination for builder agents
//! - Type registry, file locks, conventions

pub mod agent_types;
pub mod autonomy;
pub mod cancellation;
pub mod compaction;
pub mod constants;
pub mod context;
pub mod context_ledger;
pub mod delegation;
pub mod executor;
pub mod failure;
pub mod history_policy;
pub mod hooks;
pub mod learning;
pub mod loop_events;
pub(crate) mod loop_kernel;
mod observability;
mod orchestrator;
pub mod pinch_context;
pub mod pinch_session;
pub mod plan_handler;
pub mod progress;
pub mod provider_governance;
pub mod run_spec;
pub mod state;
pub mod stream;
pub mod subagent;
pub mod summarizer;
mod tool_control;
pub mod user_hooks;
pub mod worker_conversation;
pub mod worker_goal;
pub mod worker_introduction;

use serde::{Deserialize, Serialize};

pub use autonomy::auto_classifier;
pub use autonomy::auto_classifier::AutoClassifierHook;
pub use autonomy::{coordinator_prompt, tick_engine};
pub use cancellation::AgentCancellation;
pub(crate) use compaction::estimate_with_usage as estimate_tokens_with_usage;
pub use compaction::{
    effective_context_window_for_runtime, estimate_rendered_request_tokens,
    run_compaction_pipeline, CompactionManager, CompactionRequest, CompactionRequestBudget,
    CompactionResult, CompactionTrigger, RenderedRequestTokenEstimate,
};
pub use context::{
    build_plan_context, build_project_context, build_skills_context, inject_context,
};
pub use delegation::{
    CoordinatedSynthesisOwnerFence, CoordinatedSynthesisPermit, CoordinatedTaskPermit,
    DelegationCoordinator, DelegationTaskOutcome,
};
pub use hooks::{LoggingHook, PlanModeHook, SafetyHook};
pub use loop_events::{LoopEvent, LoopInput, PlanTaskInfo, ProviderRequestSnapshot};
pub use observability::{ProviderCallTraceContext, ProviderCallTraceOutcome};
pub use orchestrator::OrchestratorServices;
pub use pinch_context::{PinchContext, PinchContextInput};
pub use pinch_session::{
    create_pinched_session, CreatePinchedSessionRequest, CreatePinchedSessionResult,
};
pub use progress::{ActionClass, ProgressGuardAction, ProgressGuardTelemetry, ProgressLedger};
pub use provider_governance::{
    bounded_reservation, conservative_text_token_reservation, freeze_worker_model_pricing,
    WorkerProviderAdmission, WorkerProviderCallGovernor, WorkerProviderCallKind,
    WorkerProviderCallPermit, WorkerProviderCallSlot, WorkerProviderCompletion,
    WorkerProviderCompletionAcceptance, WorkerProviderGovernorBinding,
    WorkerProviderTerminalOutcome,
};
pub use run_spec::{
    RunContextMode, RunKernel, RunProvenance, RunSpec, RunSpecBuilder, RunSpecError,
    WorkerGoalExecutionBinding, WorkerGoalExecutionContext, WORKER_GOAL_TOOL_CAPABILITY_CEILING,
};
pub use state::{AgentConfig, RunBudget, RunBudgetResolution, RunBudgetSource};
pub use subagent::build_context;
pub use subagent::build_context::SharedBuildContext;
pub use summarizer::{generate_summary, SummarizationResult};
pub use user_hooks::{
    PackageHookConfig, PackageHookLoadReport, UserHook, UserHookExecutor, UserHookManager,
    UserHookResult, UserHookSource, UserHookType, UserPostToolHook, UserPreToolHook,
};
pub use worker_conversation::{
    SqliteWorkerConversationResponseCommitter, WorkerConversationResponseCommit,
    WorkerConversationResponseCommitDisposition, WorkerConversationResponseCommitError,
    WorkerConversationResponseCommitInput, WorkerConversationResponseCommitter,
};
pub use worker_goal::{
    WorkerGoalAttemptOutcome, WorkerGoalEffectSummary, WorkerGoalEvidence, WorkerGoalEvidenceKind,
    WorkerGoalOutcomeCommit, WorkerGoalOutcomeCommitDisposition, WorkerGoalOutcomeCommitError,
    WorkerGoalOutcomeCommitInput, WorkerGoalOutcomeCommitter, WorkerGoalOutcomeCounters,
    WorkerGoalOutcomeInputError, MAX_WORKER_GOAL_EFFECT_SUMMARY_BYTES,
    MAX_WORKER_GOAL_EVIDENCE_ITEMS, MAX_WORKER_GOAL_EVIDENCE_SUMMARY_BYTES,
    MAX_WORKER_GOAL_PROVIDER_CALL_IDS,
};
pub use worker_introduction::{
    confirm_worker_introduction, confirm_worker_introduction_in_transaction,
    fallback_worker_introduction_onboarding_reply_intent,
    fallback_worker_introduction_opening_intent, list_due_worker_introduction_reviews,
    materialize_due_worker_introduction_review_runs_fenced,
    materialize_worker_introduction_review_run_fenced,
    parse_worker_introduction_onboarding_reply_intent, parse_worker_introduction_opening_intent,
    render_worker_introduction_onboarding_reply, render_worker_introduction_opening,
    return_worker_introduction_to_context, return_worker_introduction_to_context_in_transaction,
    review_worker_introduction, worker_introduction_onboarding_reply_intent_instructions,
    worker_introduction_opening_intent_instructions, ConfirmWorkerIntroductionRequest,
    DueWorkerIntroductionReview, MaterializedWorkerIntroductionReviewRun,
    ReturnWorkerIntroductionToContextRequest, WorkerIntroductionAcknowledgement,
    WorkerIntroductionOnboardingReplyIntentV1, WorkerIntroductionOpeningIntentV1,
    WorkerIntroductionOpeningTone, WorkerIntroductionPresentationContext,
    WorkerIntroductionQuestionTopic, WorkerIntroductionReviewOutcome,
    WorkerIntroductionReviewRequest, WORKER_INTRODUCTION_PRESENTATION_VERSION,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedToolKind {
    Explore,
    Build,
    Plan,
    Verify,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegatedProgressEvent {
    pub delegated_run_id: String,
    pub parent_session_id: String,
    pub tool_call_id: String,
    pub kind: DelegatedToolKind,
    pub stage: DelegatedRunStage,
    pub progress: subagent::AgentProgress,
}
