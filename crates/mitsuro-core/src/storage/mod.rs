//! Persistence layer
//!
//! SQLite-based storage for:
//! - Session storage and management
//! - Plan storage with session linkage
//! - User preferences
//! - File activity tracking for context
//! - API credentials

use std::time::{SystemTime, UNIX_EPOCH};

mod agent_state;
pub mod apns_devices;
pub mod autonomous_tasks;
mod block_ui;
mod compaction;
pub mod credentials;
mod database;
#[cfg(test)]
mod database_tests;
mod delegated_runs;
mod delegation;
mod episodes;
pub mod expo_push_devices;
mod file_activity;
mod hive_attention_state;
mod hive_controller_events;
mod hive_controllers;
mod hive_daemon_leases;
mod hive_deliveries;
mod hive_group_worker_lanes;
pub mod hive_groups;
mod hive_home;
mod hive_idempotency;
mod hive_profiles;
mod hive_runs;
mod hive_runtime_state;
mod hive_schedules;
mod hive_worker_conversations;
mod hive_worker_governor;
mod hive_worker_introductions;
mod hive_worker_workflows;
mod hive_workers;
mod knowledge;
mod learning_candidates;
pub mod live_activity_tokens;
mod memories;
mod messages;
pub mod mobile_diagnostics;
pub mod notification_intents;
mod plans;
mod preferences;
mod project_settings;
pub mod push_delivery_attempts;
pub mod push_subscriptions;
mod recovery;
pub mod reports;
mod runtime_traces;
mod sessions;
mod spatial_canvases;

pub use agent_state::AgentState;
pub use apns_devices::{ApnsDevice, ApnsDeviceRegistration, ApnsDeviceStore};
pub use autonomous_tasks::{AutonomousTask, AutonomousTaskStore, TaskStatus};
pub use block_ui::BlockUiState;
pub use compaction::{CompactionSegmentRecord, CompactionStore};
pub use credentials::CredentialStore;
pub use database::{Database, SharedDatabase};
pub use delegated_runs::{
    normalize_scope_key, DelegatedRunAgentSnapshot, DelegatedRunCreateOutcome, DelegatedRunLease,
    DelegatedRunRecord, DelegatedRunRole, DelegatedRunScope, DelegatedRunSnapshot,
    DelegatedRunStartInput, DelegatedRunStore, DelegatedRunSummary,
};
pub use delegation::{
    DelegationCapacityClass, DelegationCapacityFeedback, DelegationCapacityPolicy,
    DelegationCapacityRequest, DelegationCompletionPolicy, DelegationEventRecord,
    DelegationEventType, DelegationExecutionMode, DelegationExecutorEnvelopeV1,
    DelegationExecutorKind, DelegationExecutorSessionType, DelegationFailurePolicy,
    DelegationGovernance, DelegationGroupContract, DelegationGroupRecord,
    DelegationGroupStartInput, DelegationGroupState, DelegationLeaseRenewalBatchResult,
    DelegationParentContinuationState, DelegationStore, DelegationSynthesisLease,
    DelegationSynthesisLeaseRenewal, DelegationTaskActivity, DelegationTaskLease,
    DelegationTaskLeaseRenewal, DelegationTaskRecord, DelegationTaskSpec, DelegationTaskState,
    DelegationWriterMode, DELEGATION_EXECUTOR_ENVELOPE_VERSION,
};
pub use episodes::{ConversationEpisode, EpisodeSearch, EpisodeStore};
pub use expo_push_devices::{ExpoPushDevice, ExpoPushDeviceRegistration, ExpoPushDeviceStore};
pub use file_activity::{FileActivityTracker, RankedFile};
pub use hive_attention_state::{HiveAttentionItemState, HiveAttentionStateStore};
pub use hive_controller_events::{
    HiveControllerEvent, HiveControllerEventStore, HiveControllerEventType, NewHiveControllerEvent,
};
pub use hive_controllers::{HiveController, HiveControllerStatus, HiveControllerStore};
pub use hive_daemon_leases::{DaemonLease, DaemonLeaseAcquire, HiveDaemonLeaseStore};
pub use hive_deliveries::{
    ack_for_terminal_runs_with_conn, claim_due_with_conn, enqueue_with_conn,
    fail_attempt_with_conn, hive_delivery_retry_backoff, load_delivery, mark_delivered_with_conn,
    revert_wait_with_conn, HiveDelivery, HiveDeliveryEnqueue, HiveDeliveryKind,
    HiveDeliveryPriority, HiveDeliveryStatus, HiveDeliveryStore, NewHiveDelivery,
    DEFAULT_HIVE_DELIVERY_MAX_ATTEMPTS, MAX_HIVE_DELIVERY_BODY_BYTES,
};
pub use hive_group_worker_lanes::{
    load_group_worker_lane_with_conn, upsert_group_worker_lane_with_conn, HiveGroupWorkerLane,
    HiveGroupWorkerLaneStore, NewHiveGroupWorkerLane,
};
pub use hive_groups::{
    parse_group_mentions, GroupMentionTarget, HiveGroup, HiveGroupExecutionMode, HiveGroupMember,
    HiveGroupMessage, HiveGroupRunContext, HiveGroupSenderKind, HiveGroupStatus, HiveGroupStore,
    HiveGroupTurn, HiveGroupTurnPolicy, HiveGroupTurnStatus, HiveGroupUpdate, HiveMemberCursor,
    MentionResolution, NewHiveGroup, NewHiveGroupMessage, MAX_HIVE_GROUP_MESSAGE_BYTES,
};
pub use hive_home::{
    bootstrap_hive_home, is_valid_crew_slug, summarize_channel_bindings, summarize_crew_runtime,
    write_hive_crew_document, write_hive_home_document, HiveBootstrapResult, HiveChannelBinding,
    HiveChannelKind, HiveContextLayer, HiveCrewDocumentKind, HiveCrewProfile,
    HiveCrewRuntimeStatus, HiveCrewRuntimeSummary, HiveHomeDocument, HiveHomeDocumentKind,
    HiveHomeProfile,
};
pub use hive_idempotency::{
    hash_request_bytes, HiveIdempotencyStore, IdempotencyClaim, IdempotencyRecord,
};
pub use hive_profiles::{
    default_profile_seed, HiveCrewProfileDocumentKind, HiveCrewProfileSeed,
    HiveCrewProfileSnapshot, HiveLegacyImportResult, HiveProfileDocument, HiveProfileDocumentKind,
    HiveProfileMergeResult, HiveProfileOwner, HiveProfileOwnerError, HiveProfileSeed,
    HiveProfileSnapshot, HiveProfileStore, HiveProfileStoreError, MAX_HIVE_PROFILE_DOCUMENT_BYTES,
};
#[doc(hidden)]
pub use hive_runs::reconcile_worker_introduction_review_in_transaction;
pub(crate) use hive_runs::{
    finalize_worker_conversation_after_governor_recovery_in_transaction,
    reactivate_worker_conversation_controller_after_governor_recovery_in_transaction,
    update_derived_state_for_run_in_transaction,
};
pub use hive_runs::{
    ClaimRunRequest, ClaimedHiveRun, DaemonFence, HiveRun, HiveRunAttempt, HiveRunAttemptOutcome,
    HiveRunExecutionContextV1, HiveRunExecutionModeV1, HiveRunKind, HiveRunStore,
    LeaseReconciliation, ReconciledRun, RunCompletion, WorkerIntroductionReviewRecovery,
    HIVE_RUN_EXECUTION_CONTEXT_VERSION, WORKER_CONVERSATION_STOP_REQUESTED_REASON,
};
pub use hive_runtime_state::{
    HiveRunPriority, HiveRuntimeState, HiveRuntimeStateStatus, HiveRuntimeStateStore,
};
pub use hive_schedules::{
    HiveSchedule, HiveScheduleOccurrence, HiveScheduleOccurrenceStatus, HiveScheduleStatus,
    HiveScheduleStore, OverlapPolicy, OwnedHiveSchedule,
};
pub use hive_worker_conversations::{
    accept_worker_conversation_input_in_transaction,
    acknowledge_worker_conversation_governor_recovery_in_transaction,
    acknowledge_worker_conversation_response_loss_in_transaction,
    materialize_oldest_staged_input_in_transaction,
    materialize_oldest_staged_input_with_authority_in_transaction,
    stage_worker_conversation_input_in_transaction, AcceptWorkerConversationInput,
    AcceptWorkerConversationInputResult, CommitWorkerConversationResponse,
    HiveWorkerConversationInputStore, MaterializedWorkerConversationInput,
    SqliteWorkerConversationResponseStore, StageWorkerConversationInput,
    StageWorkerConversationInputResult, WorkerConversationGovernorRecovery,
    WorkerConversationInput, WorkerConversationInputState, WorkerConversationPredecessorAuthority,
    WorkerConversationResponseCommit, WorkerConversationResponseCommitDisposition,
    WorkerConversationResponseCommitError, WORKER_DM_BLOCKED_BY_NON_CONVERSATION_RUN_PREFIX,
};
#[doc(hidden)]
pub use hive_worker_governor::{
    bind_worker_governor_recovery_grant_to_run_in_transaction,
    grant_worker_governor_recovery_in_transaction,
    refresh_worker_governor_recovery_run_binding_in_transaction,
    transfer_worker_governor_recovery_grant_to_successor_in_transaction,
    worker_governor_response_loss_recovery_required_in_transaction,
    worker_has_unacknowledged_unresolved_provider_calls_in_transaction,
};
pub(crate) use hive_worker_governor::{
    record_trusted_worker_idle_outcome_in_transaction,
    unresolved_worker_governor_recovery_calls_belong_to_run_in_transaction,
    validate_unbound_worker_governor_recovery_grant_in_transaction,
    worker_governor_recovery_grant_covers_unresolved_in_transaction,
};
pub use hive_worker_governor::{
    worker_local_day_window, worker_quiet_window_at, BeginWorkerProviderCall,
    BeginWorkerProviderCallResult, FinishWorkerProviderCall, FinishWorkerProviderCallResult,
    FrozenModelPriceSnapshot, GrantWorkerGovernorOverride, GrantWorkerGovernorRecoveryError,
    HiveWorkerGovernorPolicy, HiveWorkerGovernorPolicyUpdate, HiveWorkerGovernorProjection,
    HiveWorkerGovernorStore, ProviderCallRemoteAcceptance, ProviderCallTerminalState,
    ReconcileUnknownProviderCall, RecordWorkerIdleOutcome, WorkerConversationLane,
    WorkerGovernorCurrencyCost, WorkerGovernorDailyCostProjection, WorkerGovernorDailyUsage,
    WorkerGovernorDecision, WorkerGovernorDisposition, WorkerGovernorGateReason,
    WorkerGovernorIdleProjection, WorkerGovernorLaneDecisionProjection,
    WorkerGovernorOverrideGrant, WorkerGovernorPolicyCas, WorkerGovernorRecoveryRunBinding,
    WorkerIdleOutcome, WorkerLocalDayWindow, WorkerProviderCall, WorkerProviderCallOutcome,
    WorkerQuietWindow, WorkerRunGovernorProjection, WorkerRunOrigin,
    DEFAULT_WORKER_DAILY_CALL_LIMIT, DEFAULT_WORKER_DAILY_TOKEN_LIMIT,
    DEFAULT_WORKER_GOVERNOR_TIMEZONE, DEFAULT_WORKER_IDLE_BASE_SECS, DEFAULT_WORKER_IDLE_MAX_SECS,
    MAX_WORKER_DAILY_CALL_LIMIT, MAX_WORKER_DAILY_TOKEN_LIMIT, MAX_WORKER_IDLE_SECS,
    WORKER_GOVERNOR_RECOVERY_GRANT_TTL_SECS,
};
#[cfg(test)]
pub(crate) use hive_worker_introductions::NewWorkerIntroductionReviewClaim;
pub use hive_worker_introductions::{
    save_worker_introduction_opening_once, HiveWorkerIntroduction, HiveWorkerIntroductionStatus,
    HiveWorkerIntroductionStore, WorkerIntroductionDecisionKind, WorkerIntroductionDecisionV1,
    WorkerIntroductionEvidenceAxis, WorkerIntroductionEvidenceCoverage, WorkerIntroductionFactKind,
    WorkerIntroductionProposalBasisV1, WorkerIntroductionProposalFactV1,
    WorkerIntroductionProposalV1, WorkerIntroductionReviewProjection,
    WorkerIntroductionReviewProjectionState, WorkerIntroductionReviewReadiness,
    WorkerIntroductionReviewRecord, WorkerIntroductionReviewStatus,
    WorkerIntroductionReviewerFactV1, WorkerIntroductionReviewerOutputV1,
    WorkerIntroductionSelectedFactV1, MAX_WORKER_INTRODUCTION_FACTS,
    WORKER_INTRODUCTION_PROPOSAL_VERSION,
};
pub(crate) use hive_worker_introductions::{
    ReviewProposalPersistence, WorkerIntroductionReviewStore, MAX_AUTOMATIC_REVIEW_ATTEMPTS,
};
pub(crate) use hive_worker_workflows::{
    committed_worker_goal_outcome_in_transaction,
    pause_worker_workflow_after_uncertain_run_in_transaction,
    pending_worker_goal_acceptance_exists_in_transaction,
    terminalize_pending_worker_goal_acceptances_in_transaction,
    worker_goal_outcome_is_accounted_in_transaction, WorkerGoalAcceptanceLifecycle,
    WorkerGoalAcceptanceStageError,
};
pub use hive_worker_workflows::{
    reconcile_worker_workflow_provider_boundary_in_transaction, SqliteWorkerGoalAcceptanceStore,
    SqliteWorkerGoalOutcomeStore, WorkerGoalAcceptanceAssessment, WorkerGoalAcceptanceAuthority,
    WorkerGoalAcceptanceCandidateRecord, WorkerGoalAcceptanceCandidateState,
    WorkerGoalAcceptanceCommitDisposition, WorkerGoalAcceptanceContractV1,
    WorkerGoalAcceptanceIntentV1, WorkerGoalAcceptanceReceipt, WorkerGoalAcceptanceReceiptKind,
    WorkerGoalAcceptanceResolution, WorkerGoalAcceptanceResultRecord,
    WorkerGoalAcceptanceSourceSummary, WorkerGoalAcceptanceStoreError,
    WorkerGoalCriterionAcceptanceSpecV1, WorkerGoalOutcomeRecord, WorkerWorkflowProviderRecovery,
    MAX_WORKER_GOAL_ACCEPTANCE_RECEIPTS, MAX_WORKER_GOAL_ACCEPTANCE_RECEIPT_DURATION_MILLIS,
    MAX_WORKER_GOAL_ACCEPTANCE_RECEIPT_SUMMARY_BYTES, WORKER_GOAL_ACCEPTANCE_CONTRACT_VERSION,
    WORKER_GOAL_ACCEPTANCE_INTENT_VERSION, WORKER_GOAL_AUTOMATIC_ACCEPTANCE_ENABLED,
};
pub use hive_workers::{
    display_name_from_slug, load_worker_with_conn, resolve_worker_conversation_with_conn,
    resolve_worker_for_crew_slug_with_conn, HiveWorker, HiveWorkerAutonomy,
    HiveWorkerConversationBinding, HiveWorkerDocument, HiveWorkerDocumentKind,
    HiveWorkerProfileUpdate, HiveWorkerStatus, HiveWorkerStore, NewHiveWorker,
    DEFAULT_WORKER_HEARTBEAT_INTERVAL_SECS,
};
pub use knowledge::{
    get_current_snapshot, is_current_snapshot, is_current_snapshot_title, refresh_current_snapshot,
    KnowledgeSnapshot, CURRENT_SNAPSHOT_TITLE,
};
pub(crate) use learning_candidates::load_candidate_owned_from_connection;
pub use learning_candidates::{
    LearningCandidate, LearningCandidateInput, LearningCandidateStatus, LearningCandidateStore,
    LearningKind, LearningSensitivity, LearningThroughState,
};
pub use live_activity_tokens::{
    LiveActivityToken, LiveActivityTokenRegistration, LiveActivityTokenStore,
};
pub use memories::{
    is_compaction_flush_memory, AgentMemory, AgentMemoryRevision, CanonicalMemoryInput,
    HiveMemoryReader, MemoryAclScope, MemoryNamespace, MemoryRevisionEvent, MemorySensitivity,
    MemorySource, MemoryStatus, MemoryStore, MemoryType, COMPACTION_FLUSH_TITLE_PREFIX,
};
pub(crate) use memories::{
    load_canonical_for_provenance_from_connection, save_canonical_in_transaction,
};
pub use messages::{MessageStore, StoredMessageRecord};
pub use mobile_diagnostics::{
    MobileDiagnosticCategoryCount, MobileDiagnosticEvent, MobileDiagnosticEventInput,
    MobileDiagnosticNativePayload, MobileDiagnosticNativePayloadInput, MobileDiagnosticReport,
    MobileDiagnosticRun, MobileDiagnosticRunInput, MobileDiagnosticStore,
};
pub use notification_intents::{NotificationIntent, NotificationIntentStore};
pub use plans::{PlanStore, PlanSummary};
pub use preferences::Preferences;
pub use project_settings::{DelegationMode, ProjectAgentExtensionSettings, ProjectSettings};
pub use push_delivery_attempts::{
    PushDeliveryAttempt, PushDeliveryAttemptInput, PushDeliveryAttemptStore, PushDeliverySummary,
};
pub use push_subscriptions::{PushSubscription, PushSubscriptionStore};
pub use recovery::{
    ContinuationClaimSnapshot, PartialAssistantState, PendingInteractionSnapshot,
    PendingPlanTaskSnapshot, PendingQuestionOptionSnapshot, PendingQuestionSnapshot,
    RecoveryDecision, RecoveryNonResumableReason, RecoveryStatus, RecoveryToolArguments,
    RecoveryToolCall, SessionRecoveryState, REDACTED_ARGUMENT_VALUE,
};
pub use reports::{Report, ReportScope, ReportStore};
pub use runtime_traces::{
    ReplayExpectations, ReplayGateResult, RuntimeTraceEvent, RuntimeTraceStore,
    RuntimeTraceSummary, TraceEventCount, TraceFailureCategory,
};
pub use sessions::{SessionInfo, SessionManager, SessionType, WorkMode, WorkspaceMode};
pub use spatial_canvases::{
    CanvasEvent, CanvasEventPage, CanvasNode, CanvasNodeBinding, CanvasOperation, CanvasPatchInput,
    CanvasResourceCleanupClaim, CanvasResourceCreateClaim, CanvasResourceCreateIntent,
    CanvasResourceCreateStatus, CanvasResourceKind, CanvasState, CreateCanvasInput,
    CreateCanvasOutcome, PatchCanvasOutcome, ProjectCandidate, ReserveCanvasResourceCreateInput,
    ReserveCanvasResourceCreateOutcome, SpatialCanvas, SpatialCanvasStore, SpatialCanvasSurface,
    WorldRect, SPATIAL_CANVAS_SCHEMA_VERSION,
};

/// Get current Unix timestamp in seconds
#[inline]
pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
