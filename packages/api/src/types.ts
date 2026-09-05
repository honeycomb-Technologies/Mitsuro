// ============================================================================
// Session Types
// ============================================================================

export interface SessionResponse {
  id: string;
  title: string;
  /** Latest durable execution state. Present on session-list responses. */
  agent_state?: string | null;
  token_count?: number | null;
  working_dir: string | null;
  project_dir: string | null;
  workspace_mode: WorkspaceMode;
  session_type: SessionType;
  parent_session_id: string | null;
  mode: SessionMode;
  updated_at: string;
  model?: string | null;
  /** Exact provider/auth/transport identity selected for this session. */
  model_key?: ModelKey | null;
  /** Catalog revision observed when model_key was selected. */
  model_catalog_revision?: string | null;
  target_branch?: string | null;
  permission_mode: PermissionMode;
  pinned_at?: string | null;
  archived_at?: string | null;
}

export interface SessionWithMessagesResponse {
  session: SessionResponse;
  messages: MessageResponse[];
}

export interface RecoveryToolArguments {
  value: unknown;
  redacted_paths?: string[];
}

export interface RecoveryToolCall {
  id: string;
  name: string;
  arguments?: RecoveryToolArguments | null;
}

export interface PendingQuestionOptionSnapshot {
  label: string;
  description?: string | null;
}

export interface PendingQuestionSnapshot {
  header: string;
  question: string;
  options?: PendingQuestionOptionSnapshot[];
  multi_select?: boolean;
}

export interface PendingPlanTaskSnapshot {
  description: string;
  completed: boolean;
}

export type PendingInteractionSnapshot =
  | {
    kind: "tool_approval";
    tool_call: RecoveryToolCall;
  }
  | {
    kind: "ask_user_question";
    tool_call_id: string;
    questions: PendingQuestionSnapshot[];
  }
  | {
    kind: "plan_confirm";
    tool_call_id: string;
    title: string;
    task_count: number;
    tasks?: PendingPlanTaskSnapshot[];
  };

export interface SessionRecoveryState {
  schema_version: number;
  status: string;
  stop_reason: string | null;
  last_error: string | null;
  partial_assistant: PartialAssistantState;
  pending_interactions?: PendingInteractionSnapshot[];
  decision: Record<string, unknown>;
}

export interface PartialAssistantState {
  text: string;
  citations?: Citation[];
  thinking?: string;
  tool_calls: RecoveryToolCall[];
}

export interface SessionStateResponse {
  id: string;
  agent_state: string;
  started_at: string | null;
  last_event_at: string | null;
  mode: SessionMode;
  permission_mode: PermissionMode;
  workflow?: WorkflowSnapshot | null;
  recovery?: SessionRecoveryState | null;
  pending_interactions?: PendingInteractionSnapshot[];
  live_partial_assistant?: PartialAssistantState | null;
  delegated_tools?: DelegatedToolStateResponse[];
  recent_delegated_runs?: DelegatedRunResponse[];
  delegated_run_summaries?: DelegatedRunSummaryResponse[];
  delegation_groups?: DelegationGroupStateResponse[];
  delegation_events?: DelegationEventResponse[];
  delegation_event_cursor?: number | null;
  last_event_sequence?: number | null;
  agent_stream_generation?: number;
  agent_stream_sequence?: number;
}

export type DelegationGroupState =
  | "created"
  | "queued"
  | "running"
  | "ready_for_parent"
  | "synthesizing"
  | "complete"
  | "degraded"
  | "failed"
  | "cancelled";

export type DelegationTaskState =
  | "created"
  | "queued"
  | "leased"
  | "running"
  | "retrying"
  | "complete"
  | "degraded"
  | "failed"
  | "cancelled";

export interface DelegationTaskStateResponse {
  delegation_task_id: string;
  task_key: string;
  role: "explore" | "build" | "planner" | "verifier";
  objective?: string;
  provider?: string | null;
  model?: string | null;
  working_dir?: string | null;
  state: DelegationTaskState;
  attempt_count: number;
  integration_state?: "pending" | "ready" | "failed" | null;
  depends_on?: string[];
  write_intent?: string[];
  created_at?: string;
  updated_at: string;
  completed_at?: string | null;
}

export interface DelegationGroupStateResponse {
  delegation_group_id: string;
  parent_tool_call_id?: string | null;
  state: DelegationGroupState;
  execution_mode: "foreground" | "detached";
  max_parallelism?: number;
  effective_parallelism?: number;
  parent_continuation_state:
    | "not_requested"
    | "pending"
    | "queued"
    | "promoted";
  tasks: DelegationTaskStateResponse[];
  created_at?: string;
  updated_at: string;
  completed_at?: string | null;
}

export type KnownDelegationEventType =
  | "group_created"
  | "group_queued"
  | "group_state_changed"
  | "task_claimed"
  | "task_running"
  | "task_activity"
  | "task_conversation"
  | "task_state_changed"
  | "parent_continuation_queued"
  | "parent_continuation_promoted";

/**
 * Event kinds are forward-compatible protocol strings. Known values retain
 * literal completion while newer values remain visible to older clients.
 */
export type DelegationEventType =
  | KnownDelegationEventType
  | (string & Record<never, never>);

export interface DelegationEventResponse {
  event_id: number;
  parent_session_id: string;
  delegation_group_id: string;
  delegation_task_id?: string | null;
  event_type: DelegationEventType;
  payload: Record<string, unknown>;
  created_at: string;
}

export type GoalStatus =
  | "draft"
  | "active"
  | "paused"
  | "blocked"
  | "completed"
  | "cancelled";
export type CriterionStatus = "pending" | "passed" | "failed" | "waived";
export type PlanRevisionStatus =
  | "proposed"
  | "approved"
  | "active"
  | "superseded"
  | "completed"
  | "cancelled";
export type WorkflowStepStatus =
  | "pending"
  | "in_progress"
  | "blocked"
  | "completed"
  | "failed"
  | "skipped"
  | "cancelled";
export type AttemptStatus =
  | "running"
  | "paused"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface WorkflowGoal {
  id: string;
  session_id: string;
  title: string;
  objective: string;
  constraints: string[];
  status: GoalStatus;
  status_reason?: string | null;
  needs_definition: boolean;
  revision: number;
  token_budget?: number | null;
  tokens_used: number;
  source: string;
  legacy_plan_id?: string | null;
  created_at: string;
  updated_at: string;
  activated_at?: string | null;
  completed_at?: string | null;
  cancelled_at?: string | null;
}

export interface GoalCriterion {
  id: string;
  goal_id: string;
  position: number;
  description: string;
  required: boolean;
  status: CriterionStatus;
  evidence: string[];
  verifier?: string | null;
  verified_at?: string | null;
}

export interface WorkflowPlanRevision {
  id: string;
  goal_id: string;
  revision_number: number;
  status: PlanRevisionStatus;
  title: string;
  rationale?: string | null;
  source_message_id?: number | null;
  predecessor_id?: string | null;
  legacy_markdown?: string | null;
  created_at: string;
  approved_at?: string | null;
  completed_at?: string | null;
}

export interface WorkflowStep {
  id: string;
  plan_revision_id: string;
  parent_step_id?: string | null;
  display_key: string;
  position: number;
  description: string;
  context?: string | null;
  acceptance_criteria: string[];
  required: boolean;
  status: WorkflowStepStatus;
  outcome?: string | null;
  evidence: string[];
  claimed_attempt_id?: string | null;
  revision: number;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
}

export interface WorkflowStepDependency {
  step_id: string;
  depends_on_step_id: string;
}

export interface WorkflowExecutionAttempt {
  id: string;
  goal_id: string;
  plan_revision_id?: string | null;
  step_id?: string | null;
  status: AttemptStatus;
  stop_reason?: string | null;
  permission_mode: string;
  goal_revision_at_start: number;
  max_turns: number;
  max_tool_calls: number;
  max_wall_time_secs: number;
  max_research_actions: number;
  turn_count: number;
  tool_call_count: number;
  research_action_count: number;
  progress_revision: number;
  blocker_fingerprint?: string | null;
  blocker_streak: number;
  started_at: string;
  updated_at: string;
  ended_at?: string | null;
}

export interface WorkflowSnapshot {
  schema_version: number;
  aggregate_revision: number;
  collaboration_mode: "default" | "plan";
  permission_mode: string;
  goal: WorkflowGoal;
  criteria: GoalCriterion[];
  plan_revision?: WorkflowPlanRevision | null;
  steps: WorkflowStep[];
  dependencies: WorkflowStepDependency[];
  latest_attempt?: WorkflowExecutionAttempt | null;
  allowed_actions: string[];
}

export interface WorkflowMutation {
  changed: boolean;
  operation_id: string;
  snapshot: WorkflowSnapshot;
}

export interface CriterionInput {
  description: string;
  required?: boolean;
}

export interface CreateGoalInput {
  title: string;
  objective: string;
  constraints?: string[];
  criteria: CriterionInput[];
  token_budget?: number | null;
}

export interface EditGoalInput {
  title?: string;
  objective?: string;
  constraints?: string[];
  criteria?: CriterionInput[];
  token_budget?: number | null;
}

export interface WorkflowStepProposalInput {
  display_key: string;
  description: string;
  context?: string | null;
  parent_display_key?: string | null;
  dependencies?: string[];
  acceptance_criteria?: string[];
  required?: boolean;
}

export interface WorkflowPlanProposalInput {
  title: string;
  rationale?: string | null;
  source_message_id?: number | null;
  predecessor_id?: string | null;
  legacy_markdown?: string | null;
  steps: WorkflowStepProposalInput[];
}

interface WorkflowCommandBase {
  operation_id: string;
  goal_id: string;
  expected_revision: number;
}

export type WorkflowCommand =
  | { action: "create_goal"; operation_id: string; goal: CreateGoalInput }
  | {
    action: "import_legacy_plan";
    operation_id: string;
    goal: CreateGoalInput;
  }
  | ({ action: "edit_goal"; goal: EditGoalInput } & WorkflowCommandBase)
  | (
    & { action: "propose_plan"; plan: WorkflowPlanProposalInput }
    & WorkflowCommandBase
  )
  | ({
    action: "approve_plan";
    plan_revision_id: string;
  } & WorkflowCommandBase)
  | ({ action: "activate_goal" } & WorkflowCommandBase)
  | ({ action: "pause_goal"; reason: string } & WorkflowCommandBase)
  | ({ action: "resume_goal" } & WorkflowCommandBase)
  | ({ action: "block_goal"; reason: string } & WorkflowCommandBase)
  | ({ action: "cancel_goal"; reason?: string | null } & WorkflowCommandBase)
  | ({
    action: "start_attempt";
    attempt: {
      step_id?: string | null;
      permission_mode: PermissionMode;
      max_turns: number;
      max_tool_calls: number;
      max_wall_time_secs: number;
      max_research_actions: number;
    };
  } & WorkflowCommandBase)
  | ({
    action: "claim_step";
    attempt_id: string;
    step_id: string;
  } & WorkflowCommandBase)
  | ({
    action: "record_attempt_progress";
    attempt_id: string;
    progress: {
      turn_count: number;
      tool_call_count: number;
      research_action_count: number;
      material_progress: boolean;
      blocker_fingerprint?: string | null;
    };
  } & WorkflowCommandBase)
  | ({
    action: "complete_step";
    step_id: string;
    completion: {
      attempt_id: string;
      outcome: string;
      evidence: string[];
    };
  } & WorkflowCommandBase)
  | ({
    action: "finish_attempt";
    attempt_id: string;
    status: Exclude<AttemptStatus, "running">;
    reason: string;
  } & WorkflowCommandBase)
  | ({
    action: "set_criterion";
    criterion_id: string;
    criterion: {
      status: CriterionStatus;
      evidence: string[];
      verifier: string;
    };
  } & WorkflowCommandBase)
  | ({ action: "complete_goal" } & WorkflowCommandBase);

// ============================================================================
// Message Types
// ============================================================================

export interface MessageContent {
  type: "text" | "tool_use" | "tool_result" | "thinking";
  text?: string;
  id?: string;
  name?: string;
  input?: Record<string, unknown>;
  tool_use_id?: string;
  content?: string;
  thinking?: string;
  citations?: Citation[];
}

/** Source annotation attached to the text block it supports. */
export interface Citation {
  url: string;
  title: string;
  cited_text: string;
}

export interface MessageResponse {
  role: "user" | "assistant";
  content: MessageContent[];
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  thinking?: string;
  attachments?: ChatMessageAttachment[];
  toolCalls?: ToolCall[];
  renderParts?: ChatRenderPart[];
  isQueued?: boolean;
  queuedUntilNextRun?: boolean;
  kind?: "recovery_notice" | "live_partial" | "streaming";
}

export type ChatRenderPart =
  | {
    type: "text";
    id: string;
    content: string;
    citations?: Citation[];
  }
  | {
    type: "thinking";
    id: string;
    content: string;
  }
  | {
    type: "tool";
    id: string;
    toolCallId: string;
  }
  | {
    type: "attachments";
    id: string;
  };

export interface ChatMessageAttachment {
  type: "image" | "file";
  name?: string;
  mimeType?: string;
  uri?: string;
  base64?: string;
}

export interface ToolCall {
  id: string;
  name: string;
  description?: string;
  arguments?: Record<string, unknown>;
  output?: string;
  delegatedRunId?: string;
  delegated?: DelegatedArtifactState;
  status:
    | "pending"
    | "running"
    | "success"
    | "partial"
    | "error"
    | "awaiting_approval";
}

// ============================================================================
// Chat Request
// ============================================================================

export interface ToolResultRequest {
  session_id: string;
  tool_call_id: string;
  result: string;
  fast_mode?: boolean;
  thinking_enabled?: ThinkingLevel | boolean;
  permission_mode?: PermissionMode;
}

export interface ChatRequest {
  session_id?: string;
  message: string;
  content?: ContentBlock[];
  project_dir?: string | null;
  working_dir?: string | null;
  workspace_mode?: WorkspaceMode;
  target_branch?: string | null;
  targetBranch?: string | null;
  session_type?: SessionType;
  /** @deprecated Research is automatic in Chat; this compatibility field is ignored. */
  research_enabled?: boolean;
  /** Legacy model slug. Prefer model_key when it is available. */
  model?: string;
  /** Exact provider/auth/transport identity for a new or continued turn. */
  model_key?: ModelKey;
  thinking_enabled?: boolean | string;
  fast_mode?: boolean;
  permission_mode?: PermissionMode;
  /** Optional per-turn subset of the tools selected by server policy. */
  allowed_tools?: string[];
  mode?: SessionMode;
}

export interface SteerRequest {
  session_id: string;
  message: string;
  content?: ContentBlock[];
}

export interface SteerResponse {
  status: "accepted" | "queued";
  pending_id: string;
  active_run_id?: string | null;
  staged_input_id?: string | null;
  successor_run_id?: string | null;
}

export interface AgentControlLeaseSummary {
  client_id: string;
  expires_at: string;
}

export interface AgentControlState {
  session_id: string;
  controller?: AgentControlLeaseSummary | null;
  ttl_seconds: number;
}

export interface AgentControlLease {
  resource_id: string;
  client_id: string;
  lease_token: string;
  expires_at: string;
}

export interface TakeAgentControlResponse {
  lease: AgentControlLease;
}

export type ContentBlock = TextContent | ImageContent;

export interface TextContent {
  type: "text";
  text: string;
  citations?: Citation[];
}

export type ImageSource = Base64ImageSource | UrlImageSource;

export interface Base64ImageSource {
  type: "base64";
  media_type: string;
  data: string;
}

export interface UrlImageSource {
  type: "url";
  url: string;
}

export interface ImageContent {
  type: "image";
  source: ImageSource;
}

// ============================================================================
// Delegation Types
// ============================================================================

export type DelegatedToolKind = "explore" | "plan" | "verify" | "build";
export type DelegatedProgressStatus =
  | "created"
  | "queued"
  | "leased"
  | "running"
  | "retrying"
  | "complete"
  | "degraded"
  | "cancelled"
  | "failed";
export type DelegatedRunStage =
  | "created"
  | "running"
  | "synthesizing"
  | "complete"
  | "degraded"
  | "failed"
  | "cancelled";

export interface DelegatedProgressEvent {
  delegated_run_id: string;
  tool_call_id: string;
  kind: DelegatedToolKind;
  stage: DelegatedRunStage;
  parent_session_id: string;
  task_id: string;
  agent_name: string;
  status: DelegatedProgressStatus;
  tool_count: number;
  tokens: number;
  current_action?: string | null;
  completion_summary?: string | null;
  lines_added: number;
  lines_removed: number;
  completed_plan_task?: string | null;
}

export interface DelegatedAgentState {
  taskId: string;
  name: string;
  status:
    | "pending"
    | "running"
    | "complete"
    | "degraded"
    | "cancelled"
    | "failed";
  success?: boolean;
  usableEvidence?: boolean;
  degradedSuccess?: boolean;
  termination?: string;
  outcomeReason?: string;
  toolCount: number;
  tokens: number;
  currentAction?: string;
  completionSummary?: string;
  linesAdded: number;
  linesRemoved: number;
  completedPlanTask?: string;
  /** Exact durable task state; `status` remains the compatibility projection. */
  taskState?: DelegationTaskState;
  /** Number of execution attempts already started for this logical task. */
  attemptCount?: number;
}

export interface DelegatedArtifactState {
  kind: DelegatedToolKind;
  name?: string;
  capabilities?: Array<"read" | "write" | "execute">;
  delegatedRunId?: string;
  stage?: DelegatedRunStage;
  /** Exact durable group state; `stage` remains the compatibility projection. */
  groupState?: DelegationGroupState;
  maxParallelism?: number;
  effectiveParallelism?: number;
  thinking?: string;
  message?: string;
  investigationSummary?: string;
  humanReview?: string;
  outcome?: "success" | "partial" | "failed" | "cancelled";
  confidence?: "high" | "medium" | "low";
  structuralCoverage?: "high" | "medium" | "low";
  semanticCoverage?: "high" | "medium" | "low";
  agents: DelegatedAgentState[];
  filesExamined: string[];
  errors: string[];
  agentCount?: number;
  usableAgents?: number;
  degradedAgents?: number;
  cancelledAgents?: number;
  successfulAgents?: number;
  failedAgents?: number;
  filesExaminedCount?: number;
  outcomeReason?: string;
  totalTurns?: number;
  totalDurationMs?: number;
  coverageGapNotice?: string;
  linesAdded?: number;
  linesRemoved?: number;
  filesModified?: number;
  lockContentions?: number;
  totalLockWaitMs?: number;
  totalTargets?: number;
  activeTargets?: number;
  waitingTargets?: number;
  integratingTargets?: number;
  completedTargets?: number;
  pendingTargets?: number;
}

export interface DelegatedAgentStateResponse {
  task_id: string;
  agent_name: string;
  status: DelegatedProgressStatus;
  tool_count: number;
  tokens: number;
  current_action?: string | null;
  completion_summary?: string | null;
  lines_added: number;
  lines_removed: number;
  completed_plan_task?: string | null;
}

export interface DelegatedToolStateResponse {
  delegated_run_id: string;
  tool_call_id: string;
  kind: DelegatedToolKind;
  stage: DelegatedRunStage;
  parent_session_id?: string | null;
  agents: DelegatedAgentStateResponse[];
}

export interface DelegatedRunScopeResponse {
  label: string;
  path: string;
  kind: string;
}

export interface DelegatedRunResponse {
  delegated_run_id: string;
  parent_tool_call_id?: string | null;
  kind: DelegatedToolKind;
  stage: DelegatedRunStage;
  provider?: string | null;
  model?: string | null;
  resumable: boolean;
  resumed_from_run_id?: string | null;
  child_name?: string | null;
  capabilities?: Array<"read" | "write" | "execute">;
  target_scope: DelegatedRunScopeResponse[];
  human_review?: string | null;
  artifact?: Record<string, unknown> | null;
  updated_at: string;
}

export interface DelegatedRunSummaryResponse {
  delegated_run_id: string;
  parent_tool_call_id: string;
  kind: DelegatedToolKind;
  stage: DelegatedRunStage;
  child_name?: string | null;
  capabilities?: Array<"read" | "write" | "execute">;
  updated_at: string;
}

// ============================================================================
// Hive Types
// ============================================================================

export type HiveRuntimeStatus =
  | "idle"
  | "running"
  | "sleeping"
  | "awaiting_input"
  | "paused"
  | "error"
  | "cancelled";
export type HiveHomeStatus =
  | "awake"
  | "sleeping"
  | "paused"
  | "blocked"
  | "idle";
export type HiveRunPriority = "low" | "normal" | "high";
export type HiveChannelKind =
  | "main_thread"
  | "mobile_push"
  | "crew"
  | "web"
  | "email"
  | "webhook"
  | "unknown";
export type HiveChannelStatus =
  | "ready"
  | "configured"
  | "attention"
  | "inactive";
export type AutonomousTaskStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed";
export type HiveDiagnosticSeverity = "info" | "warning" | "critical";
export type HiveRunDiagnosticKind =
  | "awaiting_approval"
  | "awaiting_input"
  | "failed"
  | "stalled_stream"
  | "overdue_wake"
  | "stale_active"
  | "stale_waiting"
  | "stale_queued";
export type HiveHealthState = "healthy" | "attention" | "degraded";
export type HiveQueuePressure = "calm" | "busy" | "attention";

export interface HiveDispatchResponse {
  session_id: string;
  status: string;
}

/** Singleton companion chat for the Hive surface (not a job/run session). */
export interface HiveMainResponse {
  session_id: string;
  title: string;
  session_type: SessionType;
  permission_mode: string;
  created: boolean;
  agent_state: string;
}

export type HiveScheduleStatus =
  | "enabled"
  | "paused"
  | "completed"
  | "cancelled";

export type HiveScheduleOverlapPolicy = "skip" | "queue_one" | "allow";

export type HiveScheduleWeekday =
  | "sunday"
  | "monday"
  | "tuesday"
  | "wednesday"
  | "thursday"
  | "friday"
  | "saturday";

export type HiveMonthlyDayPolicy = "skip" | "last_day";

/** Tagged recurrence payload matching server `RecurrenceV1`. */
export type HiveRecurrenceV1 =
  | { kind: "once"; at: string }
  | { kind: "daily"; start_date: string; time: string }
  | { kind: "weekdays"; start_date: string; time: string }
  | {
    kind: "weekly";
    start_date: string;
    time: string;
    weekdays: HiveScheduleWeekday[];
  }
  | {
    kind: "monthly";
    start_date: string;
    time: string;
    day: number;
    invalid_day_policy: HiveMonthlyDayPolicy;
  };

export interface HiveDstPolicy {
  gap: "shift_forward" | "skip";
  fold: "first" | "second";
}

export interface HiveMisfireConfig {
  policy: "skip" | "fire_once" | "catch_up";
  grace_secs: number;
  catch_up_limit: number;
}

export interface HiveRetryPolicy {
  max_attempts: number;
  base_delay_secs: number;
  max_delay_secs: number;
  jitter: "none" | "full";
}

/** Durable schedule commitment for the Hive Schedule secondary surface. */
export interface HiveSchedule {
  id: string;
  controller_id: string;
  title: string;
  summary: string;
  objective: string;
  recurrence: HiveRecurrenceV1;
  timezone: string;
  dst_policy: HiveDstPolicy;
  next_fire_at?: string | null;
  last_scheduled_for?: string | null;
  status: HiveScheduleStatus;
  priority: number;
  project_dir?: string | null;
  model?: string | null;
  model_key?: ModelKey | null;
  model_catalog_revision?: string | null;
  crew_slug?: string | null;
  worker_id?: string | null;
  misfire: HiveMisfireConfig;
  overlap_policy: HiveScheduleOverlapPolicy;
  retry: HiveRetryPolicy;
  revision: number;
  created_by: string;
  created_at: string;
  updated_at: string;
}

/** User-scoped schedule response with the owning session needed by mutations. */
export interface HiveGlobalSchedule extends HiveSchedule {
  controller_session_id: string;
}

/** Response envelope returned by create and status-mutation schedule routes. */
export interface HiveScheduleMutationResponse {
  schedule_id: string;
  revision: number;
  status: HiveScheduleStatus;
}

export interface HiveScheduleWriteRequest {
  title: string;
  summary?: string;
  objective: string;
  recurrence: HiveRecurrenceV1;
  timezone: string;
  dst_policy?: HiveDstPolicy;
  priority?: number;
  project_dir?: string | null;
  model?: string | null;
  model_key?: ModelKey | null;
  crew_slug?: string | null;
  worker_id?: string | null;
  misfire?: HiveMisfireConfig;
  overlap_policy?: HiveScheduleOverlapPolicy;
  retry?: HiveRetryPolicy;
}

export interface HiveDispatchOptions {
  projectDir?: string;
  /** Legacy model slug retained for older servers. */
  model?: string;
  /** Exact provider/auth/transport identity for the durable Hive run. */
  modelKey?: ModelKey;
  startAt?: string;
  priority?: HiveRunPriority;
  crewSlug?: string | null;
}

export interface SimpleOkResponse {
  ok: boolean;
}

export interface ApnsRegisterResponse {
  id: string;
  registered: boolean;
}

export interface ApnsStatusResponse {
  apns_configured: boolean;
  device_count: number;
  last_success_at?: string | null;
  last_failure_at?: string | null;
  last_failure_reason?: string | null;
}

export type NotificationDeliveryLevel = "all" | "important" | "silent";
export type PushPlatform = "ios" | "android";

export interface ExpoPushRegisterResponse {
  id: string;
  registered: boolean;
}

export interface LiveActivityRegisterRequest {
  sessionId: string;
  pushToken: string;
  contentState: Record<string, unknown>;
  startedAtMs: number;
  bundleId?: string;
  environment?: "sandbox" | "production";
}

export type MemoryType = "user" | "feedback" | "project" | "reference";

export interface AgentMemory {
  id: string;
  memory_type: MemoryType;
  title: string;
  content: string;
  project_dir?: string;
  created_at: string;
  updated_at: string;
  content_preview?: string;
  content_chars?: number;
  truncated?: boolean;
}

export interface PromoteReportToMemoryResponse {
  created: boolean;
  memory: AgentMemory;
}

export interface MemorySnapshotResponse {
  snapshot: AgentMemory | null;
}

export interface AutonomousTask {
  id: string;
  session_id: string;
  subject: string;
  description: string;
  status: AutonomousTaskStatus;
  owner?: string | null;
  blocked_by: string[];
  created_at: string;
  updated_at: string;
  completed_at?: string | null;
  result?: string | null;
}

export interface HiveRuntimeState {
  session_id: string;
  status: HiveRuntimeStatus;
  next_wake_at?: string | null;
  sleep_reason?: string | null;
  last_error?: string | null;
  current_run_id?: string | null;
  last_wake_reason?: string | null;
  crew_slug?: string | null;
  priority: HiveRunPriority;
  updated_at: string;
}

export interface HiveSessionSummary {
  session_id: string;
  title: string;
  updated_at: string;
  project_dir?: string | null;
  target_branch?: string | null;
  agent_state: string;
  runtime?: HiveRuntimeState | null;
}

export interface HiveSessionStatus {
  session_id: string;
  session_type: SessionType;
  title: string;
  tasks: AutonomousTask[];
  agent_state: string;
  runtime?: HiveRuntimeState | null;
  cadence: HiveCadenceSummary;
}

export interface HiveCurrentRunSummary {
  session_id: string;
  title: string;
  updated_at: string;
  project_dir?: string | null;
  target_branch?: string | null;
  agent_state: string;
  runtime?: HiveRuntimeState | null;
  pending_tasks: number;
  in_progress_tasks: number;
  completed_tasks: number;
  failed_tasks: number;
  blocked_tasks: number;
  cadence: HiveCadenceSummary;
  diagnostic?: HiveRunDiagnostic | null;
}

export interface HiveRunDiagnostic {
  kind: HiveRunDiagnosticKind;
  severity: HiveDiagnosticSeverity;
  summary: string;
  detail: string;
  last_activity_at?: string | null;
  last_trace_at?: string | null;
  stalled_for_secs?: number | null;
  overdue_by_secs?: number | null;
  failure_streak: number;
}

export interface HivePendingApproval {
  session_id: string;
  session_title: string;
  project_dir?: string | null;
  target_branch?: string | null;
  tool_call_id: string;
  tool_name: string;
  arguments: unknown;
  requested_at: string;
  priority: HiveRunPriority;
}

export type HiveAttentionItemKind =
  | "approval_required"
  | "input_required"
  | "run_completed"
  | "run_failed"
  | "run_stalled"
  | "scheduled_run_started"
  | "scheduled_run_completed"
  | "delegated_task_completed";

export type HiveAttentionSection = "needs_action" | "updates";

export interface HiveAttentionItem {
  id: string;
  kind: HiveAttentionItemKind;
  section: HiveAttentionSection;
  title: string;
  summary: string;
  detail: string;
  created_at: string;
  read: boolean;
  cleared: boolean;
  active: boolean;
  session_id?: string | null;
  run_id?: string | null;
  project_dir?: string | null;
  target_branch?: string | null;
  tool_call_id?: string | null;
  thread_session_id?: string | null;
  thread_message_id?: string | null;
}

export interface HiveAttentionResponse {
  items: HiveAttentionItem[];
  unread_count: number;
  badge_count: number;
}

export interface HiveStatusSummary {
  home_status: HiveHomeStatus;
  total_count: number;
  running_count: number;
  sleeping_count: number;
  scheduled_count: number;
  high_priority_count: number;
  paused_count: number;
  waiting_count: number;
  failed_count: number;
  idle_count: number;
  pending_approvals_count: number;
  next_wake_at?: string | null;
}

export interface HiveKnowledgeHealthSummary {
  scope_count: number;
  healthy_scope_count: number;
  missing_snapshot_count: number;
  stale_snapshot_count: number;
  latest_snapshot_at?: string | null;
}

export interface HiveDaemonSummary {
  uptime_secs: number;
  active_runtime_count: number;
  scheduled_wake_count: number;
  event_stream_count: number;
  recoverable_session_count: number;
}

export interface HiveDiagnosticsSummary {
  degraded_count: number;
  stalled_count: number;
  overdue_wake_count: number;
  repeating_failure_count: number;
  open_run_count: number;
  attention_run_count: number;
  due_soon_wake_count: number;
  health_state: HiveHealthState;
  queue_pressure: HiveQueuePressure;
  latest_trace_at?: string | null;
  daemon: HiveDaemonSummary;
  knowledge: HiveKnowledgeHealthSummary;
}

export interface HiveCadenceSummary {
  tick_interval_secs: number;
  max_ticks: number;
}

export interface HiveCurrentResponse {
  status: HiveStatusSummary;
  diagnostics: HiveDiagnosticsSummary;
  runs: HiveCurrentRunSummary[];
  approvals: HivePendingApproval[];
}

export interface HiveHomeDocument {
  file_name: string;
  content: string;
  preview: string;
}

export interface HiveCrewMember {
  slug: string;
  identity?: HiveHomeDocument | null;
  soul?: HiveHomeDocument | null;
  memory?: HiveHomeDocument | null;
}

export interface HiveHomeResponse {
  soul?: HiveHomeDocument | null;
  identity?: HiveHomeDocument | null;
  heartbeat?: HiveHomeDocument | null;
  memory?: HiveHomeDocument | null;
  channels?: HiveHomeDocument | null;
  crew: HiveCrewMember[];
  crew_count: number;
}

export type HiveHomeDocumentKind =
  | "soul"
  | "identity"
  | "heartbeat"
  | "memory"
  | "channels";
export type HiveCrewDocumentKind = "identity" | "soul" | "memory";
export type HiveCrewRuntimeStatus = "idle" | "running" | "waiting" | "degraded";

export interface HiveBootstrapResponse {
  ok: boolean;
  created_files: string[];
  home: HiveHomeResponse;
}

export interface HiveCrewRuntimeMember {
  slug: string;
  known_to_home: boolean;
  status: HiveCrewRuntimeStatus;
  active_run_count: number;
  recent_run_count: number;
  failed_run_count: number;
  queued_task_count: number;
  active_task_count: number;
  completed_task_count: number;
  failed_task_count: number;
  latest_activity_at?: string | null;
  identity?: HiveHomeDocument | null;
  soul?: HiveHomeDocument | null;
  memory?: HiveHomeDocument | null;
}

export interface HiveCrewResponse {
  members: HiveCrewRuntimeMember[];
}

export interface HiveChannelItem {
  id: string;
  label: string;
  kind: HiveChannelKind;
  source: string;
  enabled: boolean;
  status: HiveChannelStatus;
  detail: string;
}

export interface HiveChannelsResponse {
  items: HiveChannelItem[];
  apns_configured: boolean;
  apns_device_count: number;
}

export interface HiveRecoverDaemonResponse {
  ok: boolean;
  recovered_count: number;
}

export type HiveWorkerStatus = "active" | "paused" | "archived";
export type HiveWorkerAutonomy = "manual" | "scheduled" | "always_on";

/** A durable Hive Worker identity with its own persona, model, and DM lane. */
export interface HiveWorker {
  id: string;
  revision: number;
  slug: string;
  display_name: string;
  avatar_color?: string | null;
  model?: string | null;
  model_key?: ModelKey | null;
  permission_mode: string;
  autonomy: HiveWorkerAutonomy;
  heartbeat_interval_secs?: number | null;
  status: HiveWorkerStatus;
  dm_session_id?: string | null;
  /** Agent state of the bound DM session ("idle", "running", ...), when bound. */
  dm_agent_state?: string | null;
  /** One-time Introduction lifecycle for roster recovery controls. */
  introduction_status?: HiveWorkerIntroductionStatus | null;
  introduction_last_error?: string | null;
  created_at: string;
  updated_at: string;
}

export interface HiveWorkersResponse {
  workers: HiveWorker[];
}

/** Worker plus its persona documents. */
export interface HiveWorkerDetail extends HiveWorker {
  identity?: string | null;
  soul?: string | null;
  introduction?: HiveWorkerIntroduction | null;
  attention: HiveWorkerLaneAttention[];
}

export type HiveWorkerSessionBindingResponse =
  | {
    kind: "worker_dm";
    session_id: string;
    worker: HiveWorkerDetail;
  }
  | {
    kind: "primary_hive";
    session_id: string;
  };

export interface HiveWorkerLaneAttention {
  session_id: string;
  controller_id: string;
  recovery_run_ids: string[];
  reason: string;
}

export type HiveWorkerIntroductionStatus =
  | "queued"
  | "running"
  | "awaiting_context"
  | "review_ready"
  | "confirmed"
  | "skipped"
  | "failed"
  | "needs_recovery";

export type HiveWorkerIntroductionFactKind =
  | "role"
  | "purpose"
  | "responsibility"
  | "working_style"
  | "boundary"
  | "tool_expectation"
  | "memory_expectation"
  | "cadence"
  | "user_preference"
  | "user_correction"
  | "relationship_context";

export interface HiveWorkerIntroductionProposalBasisV1 {
  opening_message_id: number;
  through_message_id: number;
  user_message_ids: number[];
  transcript_digest: string;
}

export interface HiveWorkerIntroductionProposalFactV1 {
  fact_id: string;
  kind: HiveWorkerIntroductionFactKind;
  statement: string;
  evidence_message_id: number;
  evidence_excerpt: string;
}

export interface HiveWorkerIntroductionProposalV1 {
  schema_version: 1;
  proposal_id: string;
  revision: number;
  worker_id: string;
  session_id: string;
  basis: HiveWorkerIntroductionProposalBasisV1;
  base_identity_digest: string;
  base_soul_digest: string;
  facts: HiveWorkerIntroductionProposalFactV1[];
}

export type HiveWorkerIntroductionReviewStatus =
  | "queued"
  | "claimed"
  | "gather_more"
  | "review_ready"
  | "confirmed"
  | "rejected"
  | "keep_talking"
  | "failed"
  | "stale";

export type HiveWorkerIntroductionReviewState =
  | "inactive"
  | "awaiting_context"
  | "pending"
  | "claimed"
  | "retrying"
  | "gather_more"
  | "review_ready"
  | "needs_attention"
  | "confirmed"
  | "rejected"
  | "keep_talking";

export interface HiveWorkerIntroductionReviewProjection {
  worker_id: string;
  lifecycle_status: HiveWorkerIntroductionStatus;
  state: HiveWorkerIntroductionReviewState;
  current_through_message_id?: number | null;
  review_through_message_id?: number | null;
  review_status?: HiveWorkerIntroductionReviewStatus | null;
  is_current_through: boolean;
  has_pending_user_input: boolean;
  attempt_count: number;
  should_poll: boolean;
  last_error?: string | null;
}

export interface HiveWorkerIntroduction {
  run_id?: string | null;
  status: HiveWorkerIntroductionStatus;
  prompt_version: number;
  opening_message_id?: number | null;
  proposal?: HiveWorkerIntroductionProposalV1 | null;
  proposal_revision: number;
  review_projection: HiveWorkerIntroductionReviewProjection;
  last_error?: string | null;
  completed_at?: string | null;
}

export interface HiveWorkerIntroductionSelectedFact {
  fact_id: string;
  final_statement: string;
}

export interface ConfirmHiveWorkerIntroductionRequest {
  proposal_id: string;
  proposal_revision: number;
  selected_facts: HiveWorkerIntroductionSelectedFact[];
}

export interface KeepTalkingHiveWorkerIntroductionRequest {
  proposal_id: string;
  proposal_revision: number;
}

export interface CreateHiveWorkerRequest {
  slug: string;
  display_name?: string;
  avatar_color?: string;
  model?: string;
  model_key?: ModelKey;
  permission_mode?: string;
  autonomy?: HiveWorkerAutonomy;
  heartbeat_interval_secs?: number;
  identity?: string;
  soul?: string;
}

/** Partial update: absent fields keep their current value. */
export interface UpdateHiveWorkerRequest {
  expected_revision: number;
  display_name?: string;
  avatar_color?: string;
  model?: string;
  model_key?: ModelKey;
  permission_mode?: string;
  autonomy?: HiveWorkerAutonomy;
  heartbeat_interval_secs?: number;
  identity?: string;
  soul?: string;
}

export interface SetHiveWorkerStatusRequest {
  expected_revision: number;
}

export type HiveWorkerGovernorDisposition = "allow" | "defer" | "deny";
export type HiveWorkerGovernorGateReason =
  | "policy_unavailable"
  | "unresolved_provider_call"
  | "daily_call_cap_reached"
  | "daily_token_cap_reached"
  | "quiet_hours"
  | "idle_backoff";
export type HiveWorkerGovernorOrigin = "user_dm" | "workflow_rollover";

export interface HiveWorkerGovernorPolicy {
  worker_id: string;
  revision: number;
  daily_call_limit: number;
  daily_token_limit: number;
  timezone: string;
  quiet_start_minute?: number | null;
  quiet_end_minute?: number | null;
  quiet_gap_policy: "shift_forward" | "skip";
  quiet_fold_policy: "first" | "second";
  idle_base_secs: number;
  idle_max_secs: number;
  tracking_started_at: string;
  created_at: string;
  updated_at: string;
}

export interface HiveWorkerGovernorDailyUsage {
  local_day: string;
  timezone: string;
  starts_at: string;
  resets_at: string;
  calls_used: number;
  calls_limit: number;
  tokens_used_or_reserved: number;
  tokens_limit: number;
}

export interface HiveWorkerGovernorIdleProjection {
  lane_key: string;
  idle_streak: number;
  not_before?: string | null;
  last_material_at?: string | null;
  last_outcome_run_id?: string | null;
}

export interface HiveWorkerGovernorDecision {
  disposition: HiveWorkerGovernorDisposition;
  primary_reason?: HiveWorkerGovernorGateReason | null;
  reasons: HiveWorkerGovernorGateReason[];
  evaluated_at: string;
  next_eligible_at?: string | null;
  policy_revision: number;
  tracking_started_at: string;
  daily: HiveWorkerGovernorDailyUsage;
  idle: HiveWorkerGovernorIdleProjection;
  override_grant_id?: string | null;
}

export interface HiveWorkerGovernorLaneDecision {
  origin: HiveWorkerGovernorOrigin;
  lane_key: "dm";
  /** Minimum reservation probe; the executor rechecks its exact request. */
  reservation_tokens: number;
  decision: HiveWorkerGovernorDecision;
}

export interface HiveWorkerGovernorCurrencyCost {
  currency: string;
  /** Exact decimal string; JSON numbers cannot preserve the SQLite u64 range. */
  estimated_cost_microunits: string;
  priced_call_count: number;
}

export interface HiveWorkerGovernorDailyCost {
  local_day: string;
  timezone: string;
  starts_at: string;
  resets_at: string;
  by_currency: HiveWorkerGovernorCurrencyCost[];
  unpriced_call_count: number;
}

/** Aggregate-only exact Worker-DM projection; no provider request content. */
export interface HiveWorkerGovernorProjection {
  schema_version: 1;
  worker_id: string;
  worker_revision: number;
  dm_session_id: string;
  evaluated_at: string;
  policy: HiveWorkerGovernorPolicy;
  daily: HiveWorkerGovernorDailyUsage;
  autonomous_dm: HiveWorkerGovernorLaneDecision;
  foreground_dm: HiveWorkerGovernorLaneDecision;
  unresolved_started_count: number;
  response_loss_recovery_required: boolean;
  estimated_daily_cost: HiveWorkerGovernorDailyCost;
}

export type HiveWorkerGovernorRecoveryResponse = {
  worker_id: string;
  grant_id: string;
  expires_at: string;
  status:
    | "granted"
    | "already_available"
    | "response_loss_acknowledged_with_grant";
  bypass_unresolved_provider_call: true;
} | {
  worker_id: string;
  grant_id: null;
  expires_at: null;
  status: "response_loss_acknowledged";
  bypass_unresolved_provider_call: false;
};

export type HiveWorkerGoalAction =
  | "create_goal"
  | "approve_plan"
  | "activate"
  | "pause"
  | "cancel"
  | "set_workspace"
  | "resolve_acceptance";

export interface HiveWorkerGoalWorkspace {
  mode: WorkspaceMode;
  working_dir?: string | null;
  project_dir?: string | null;
}

export interface HiveWorkerGoalRun {
  run_id: string;
  run_status: string;
  attempt_id: string;
  attempt_status: string;
}

export interface HiveWorkerGoalPendingCriterion {
  criterion_id: string;
  description: string;
}

export type HiveWorkerGoalAttemptOutcome =
  | "succeeded"
  | "progressed"
  | "blocked"
  | "failed"
  | "cancelled"
  | "budget_exhausted"
  | "needs_attention";

export type HiveWorkerGoalEvidenceKind =
  | "workspace_observation"
  | "workspace_mutation"
  | "verification"
  | "tool_failure"
  | "runtime";

export interface HiveWorkerGoalSourceEvidence {
  kind: HiveWorkerGoalEvidenceKind;
  summary: string;
}

export interface HiveWorkerGoalSourceEffect {
  summary: string;
  workspace_mutated: boolean;
}

export interface HiveWorkerGoalOutcomeCounters {
  provider_calls: number;
  turns: number;
  tool_calls: number;
  successful_tool_calls: number;
  failed_tool_calls: number;
  research_actions: number;
}

export interface HiveWorkerGoalAcceptanceSourceSummary {
  outcome: HiveWorkerGoalAttemptOutcome;
  evidence: HiveWorkerGoalSourceEvidence[];
  effect: HiveWorkerGoalSourceEffect;
  counters: HiveWorkerGoalOutcomeCounters;
}

/** Privacy-safe owner review projection; raw provider output is never exposed. */
export interface HiveWorkerGoalPendingAcceptance {
  acceptance_run_id: string;
  source_run_id: string;
  goal_id: string;
  attempt_id: string;
  step_id: string;
  expected_worker_revision: number;
  expected_goal_revision: number;
  step_revision: number;
  step_description: string;
  is_final_step: boolean;
  required_goal_criteria: HiveWorkerGoalPendingCriterion[];
  source_summary: HiveWorkerGoalAcceptanceSourceSummary;
}

/** Worker-aware Goal truth; generic Workflow actions are presentation-only. */
export interface HiveWorkerGoalProjection {
  schema_version: number;
  worker_id: string;
  worker_revision: number;
  worker_status: HiveWorkerStatus;
  session_id: string;
  workspace: HiveWorkerGoalWorkspace;
  introduction_status?: HiveWorkerIntroductionStatus | null;
  introduction_ready: boolean;
  workflow?: WorkflowSnapshot | null;
  active_run?: HiveWorkerGoalRun | null;
  pending_acceptance?: HiveWorkerGoalPendingAcceptance | null;
  attention: string[];
  read_only_reason?: string | null;
  allowed_actions: HiveWorkerGoalAction[];
}

export interface ApproveHiveWorkerGoalRequest {
  goal_id: string;
  plan_revision_id: string;
  expected_worker_revision: number;
  expected_goal_revision: number;
}

export interface CreateHiveWorkerGoalRequest {
  expected_worker_revision: number;
  goal: CreateGoalInput;
  plan: WorkflowPlanProposalInput;
}

export interface MutateHiveWorkerGoalRequest {
  goal_id: string;
  expected_worker_revision: number;
  expected_goal_revision: number;
}

export interface TransitionHiveWorkerGoalRequest
  extends MutateHiveWorkerGoalRequest {
  reason: string;
}

export interface SetHiveWorkerWorkspaceRequest {
  expected_worker_revision: number;
  workspace_mode: WorkspaceMode;
  working_dir?: string | null;
  project_dir?: string | null;
}

export type HiveWorkerGoalAcceptanceDecision = "accept" | "reject";
export type HiveWorkerGoalCriterionDecision = "passed" | "failed" | "waived";

export interface HiveWorkerGoalCriterionAcceptance {
  criterion_id: string;
  decision: HiveWorkerGoalCriterionDecision;
  evidence: string[];
}

export interface ResolveHiveWorkerGoalAcceptanceRequest {
  expected_worker_revision: number;
  acceptance_run_id: string;
  expected_goal_revision: number;
  decision: HiveWorkerGoalAcceptanceDecision;
  reason: string;
  criteria: HiveWorkerGoalCriterionAcceptance[];
}

export interface HiveWorkerDmResponse {
  worker_id: string;
  session_id: string;
  title: string;
  session_type: string;
  permission_mode: string;
  created: boolean;
  agent_state: string;
}

export type HiveDeliveryStatus =
  | "pending"
  | "delivering"
  | "delivered"
  | "acked"
  | "dead_letter";

export type HiveDeliveryPriority = "normal" | "high";

export interface HiveWorkerDelivery {
  id: string;
  kind: string;
  from_worker_id?: string | null;
  to_worker_id: string;
  group_id?: string | null;
  body: string;
  priority: HiveDeliveryPriority;
  status: HiveDeliveryStatus;
  attempt_count: number;
  max_attempts: number;
  available_at: string;
  delivered_at?: string | null;
  acked_at?: string | null;
  last_error?: string | null;
  run_id?: string | null;
  created_at: string;
  updated_at: string;
}

export interface HiveWorkerDeliveriesResponse {
  deliveries: HiveWorkerDelivery[];
}

export type HiveGroupExecutionMode = "workbench" | "roundtable" | "direct";
export type HiveGroupStatus = "active" | "archived";
export type HiveGroupTurnStatus =
  | "running"
  | "completed"
  | "partial"
  | "failed"
  | "cancelled";
export type HiveGroupSenderKind = "user" | "worker" | "system";

/** One member of a group, with roster display data. */
export interface HiveGroupMember {
  worker_id: string;
  slug: string;
  display_name: string;
  avatar_color?: string | null;
  model?: string | null;
  provider?: string | null;
  status: string;
}

/** A group room referencing Workers, with its turn execution policy. */
export interface HiveGroup {
  id: string;
  title: string;
  execution_mode: HiveGroupExecutionMode;
  max_rounds: number;
  max_member_messages_per_turn: number;
  parallelism: number;
  context_window_messages: number;
  status: HiveGroupStatus;
  default_assignee_worker_id?: string | null;
  members: HiveGroupMember[];
  active_turn_id?: string | null;
  latest_seq: number;
  created_at: string;
  updated_at: string;
}

/** The durable aggregate of one group turn with per-member outcomes. */
export interface HiveGroupTurn {
  id: string;
  group_id: string;
  trigger_message_id: string;
  execution_mode: HiveGroupExecutionMode;
  status: HiveGroupTurnStatus;
  speaker_plan: string[];
  next_speaker_index: number;
  /** Worker-id keyed outcome summaries ({status, run_id?, error?}). */
  member_outcomes?:
    | Record<
      string,
      { status: string; run_id?: string; error?: string }
    >
    | null;
  started_at: string;
  finished_at?: string | null;
}

export interface HiveGroupDetail extends HiveGroup {
  active_turn?: HiveGroupTurn | null;
}

export interface HiveGroupsResponse {
  groups: HiveGroup[];
}

/** One append-only room message with a per-group monotonic sequence. */
export interface HiveGroupMessage {
  id: string;
  group_id: string;
  seq: number;
  sender_kind: HiveGroupSenderKind;
  sender_worker_id?: string | null;
  sender_run_id?: string | null;
  content: string;
  reply_to_message_id?: string | null;
  turn_id?: string | null;
  created_at: string;
}

export interface HiveGroupMessagesResponse {
  messages: HiveGroupMessage[];
  /** Highest sequence represented by this page; safe as the next event cursor. */
  latest_seq: number;
}

export interface CreateHiveGroupRequest {
  title: string;
  execution_mode?: HiveGroupExecutionMode;
  max_rounds?: number;
  max_member_messages_per_turn?: number;
  parallelism?: number;
  context_window_messages?: number;
  default_assignee_worker_id?: string;
  member_worker_ids: string[];
}

/**
 * Partial update: absent fields keep their value. An empty
 * default_assignee_worker_id clears the assignment; member_worker_ids
 * replaces the ordered membership (add/remove/reorder).
 */
export interface UpdateHiveGroupRequest {
  title?: string;
  execution_mode?: HiveGroupExecutionMode;
  max_rounds?: number;
  max_member_messages_per_turn?: number;
  parallelism?: number;
  context_window_messages?: number;
  default_assignee_worker_id?: string;
  member_worker_ids?: string[];
}

export interface SendHiveGroupMessageRequest {
  message: string;
  /** Explicit target slugs; omitted = server-side mention parsing. */
  mentions_override?: string[];
}

/** Durable acceptance of one group turn. */
export interface SendHiveGroupMessageResponse {
  group_id: string;
  turn_id: string;
  message_id: string;
  message_seq: number;
  status: string;
  target_worker_ids: string[];
}

/** Room event stream payloads: message appends and turn transitions. */
export type HiveGroupEvent =
  | { type: "message"; message: HiveGroupMessage }
  | { type: "turn"; turn: HiveGroupTurn };

// Content-free mobile diagnostics. Keep this contract operational and bounded:
// never add prompts, responses, credentials, terminal/file contents, or raw URLs.
export interface MobileDiagnosticUploadBatch {
  run: {
    id: string;
    installation_id: string;
    app_version: string;
    build_number: string;
    platform: "ios" | "android" | "web";
    os_version: string;
    device_class: string;
    capture_level: "baseline" | "stress";
    started_at_ms: number;
    ended_at_ms: number | null;
    dropped_event_count: number;
  };
  events: Array<{
    sequence: number;
    occurred_at_ms: number;
    monotonic_ms: number;
    category: string;
    name: string;
    duration_ms: number | null;
    severity: "info" | "warning" | "error";
    attributes: Record<string, string | number | boolean>;
  }>;
  native_payloads: Array<{
    payload_id: string;
    kind: "metric" | "diagnostic";
    received_at_ms: number;
    payload_json: string;
  }>;
  completed: boolean;
}

export interface MobileDiagnosticUploadResponse {
  run_id: string;
  accepted_events: number;
  accepted_native_payloads: number;
  dropped_attributes: number;
}

export interface HiveRunWakeEvent {
  id: string;
  timestamp: string;
  title: string;
  detail?: string | null;
  kind: "runtime" | "task";
  status: string;
}

// ============================================================================
// Stream Types
// ============================================================================

export interface PlanItem {
  id?: string;
  content: string;
  completed: boolean;
}

export interface SessionPinchedEvent {
  type: "session_pinched";
  reason: string;
  source_session_id: string;
  new_session_id: string;
  estimated_tokens_before: number;
}

export interface ContextCompactionStartedEvent {
  type: "context_compaction_started";
  reason: string;
}

export interface ContextCompactedEvent {
  type: "context_compacted";
  reason: string;
  estimated_tokens_before: number;
  estimated_tokens_after: number;
  replaced_messages: number;
  checkpoint_id: string;
  compaction_count: number;
}

export type SessionContinuationEvent =
  | SessionPinchedEvent
  | ContextCompactedEvent;

export interface WorkerInputStagedEvent {
  type: "worker_input_staged";
  worker_id: string;
  session_id: string;
  active_run_id: string;
  staged_input_id: string;
  successor_run_id?: string | null;
}

export interface WorkerResponseBoundaryEvent {
  type: "worker_response_pending" | "worker_response_committed";
  worker_id: string;
  session_id: string;
  run_id: string;
}

export interface UsageMetrics {
  /** Uncached input tokens billed at the provider's normal input rate. */
  promptTokens: number;
  /** Logical input: uncached input + cache writes + cache reads. */
  inputTokens: number;
  /** Generated output, including reasoning tokens. */
  completionTokens: number;
  /** Reasoning tokens contained within completionTokens. */
  reasoningTokens: number;
  cacheCreationInputTokens: number;
  cacheReadInputTokens: number;
  /** Logical input plus generated output. */
  totalTokens: number;
}

export type StreamEvent =
  | { type: "text_delta"; delta: string }
  | { type: "text_delta_with_citations"; delta: string; citations: Citation[] }
  | { type: "thinking_delta"; thinking: string }
  | { type: "thinking_complete"; thinking: string; signature: string }
  | { type: "tool_call_start"; id: string; name: string }
  | {
    type: "tool_call_preparing";
    id: string;
    name: string;
    received_bytes: number;
  }
  | {
    type: "tool_call_complete";
    id: string;
    name: string;
    arguments: Record<string, unknown>;
  }
  | { type: "tool_executing"; id: string; name: string }
  | { type: "tool_output_delta"; id: string; delta: string }
  | ({ type: "delegated_progress" } & DelegatedProgressEvent)
  | { type: "delegation_event"; event: DelegationEventResponse }
  | { type: "tool_result"; id: string; output: string; is_error: boolean }
  | { type: "server_tool_start"; id: string; name: string }
  | { type: "server_tool_complete"; id: string; name: string }
  | { type: "web_search_results"; tool_use_id: string; results: unknown[] }
  | { type: "web_fetch_result"; tool_use_id: string; content: unknown }
  | { type: "server_tool_error"; tool_use_id: string; error_code: string }
  | { type: "plan_update"; items: PlanItem[] }
  | {
    type: "workflow_updated";
    goal_id: string;
    aggregate_revision: number;
    operation_id: string;
  }
  | { type: "mode_change"; mode: string; reason?: string }
  | {
    type: "plan_complete";
    tool_call_id: string;
    title: string;
    task_count: number;
  }
  | {
    type: "usage";
    prompt_tokens: number;
    input_tokens?: number;
    completion_tokens: number;
    reasoning_tokens?: number;
    cache_creation_input_tokens?: number;
    cache_read_input_tokens?: number;
    total_tokens?: number;
  }
  | ContextCompactionStartedEvent
  | SessionPinchedEvent
  | ContextCompactedEvent
  | { type: "lagged"; skipped: number }
  | { type: "title_update"; title: string }
  | {
    type: "tool_approval_required";
    id: string;
    name: string;
    arguments: Record<string, unknown>;
  }
  | { type: "tool_approved"; id: string }
  | { type: "tool_denied"; id: string }
  | {
    type: "steering_injected";
    pending_id?: string;
    message: string;
  }
  | WorkerInputStagedEvent
  | WorkerResponseBoundaryEvent
  | { type: "turn_complete"; turn: number; has_more: boolean }
  | { type: "finish"; session_id: string; stop_reason: string }
  | { type: "error"; error: string }
  // Hive autonomous agent events
  | { type: "user_message"; title?: string; message: string; level: string }
  | { type: "agent_sleeping"; duration_secs: number; reason: string }
  | { type: "tick_injected"; tick_number: number }
  | {
    type: "classifier_decision";
    tool_name: string;
    decision: string;
    reason: string;
    stage: number;
  }
  | { type: "teammate_spawned"; name: string; role: string }
  | {
    type: "teammate_task_completed";
    name: string;
    task_id: string;
    result: string;
  }
  | {
    type: "teammate_task_failed";
    name: string;
    task_id: string;
    error: string;
  }
  | { type: "teammate_cancelled"; name: string };

export interface StreamCallbacks {
  onTextDelta: (delta: string, citations?: Citation[]) => void;
  onThinkingDelta: (thinking: string) => void;
  onToolCallStart: (id: string, name: string) => void;
  onToolCallPreparing?: (
    id: string,
    name: string,
    receivedBytes: number,
  ) => void;
  onToolCallComplete: (
    id: string,
    name: string,
    args: Record<string, unknown>,
  ) => void;
  onToolResult: (id: string, output: string, isError: boolean) => void;
  onToolOutputDelta: (id: string, delta: string) => void;
  onDelegatedProgress?: (event: DelegatedProgressEvent) => void;
  onDelegationEvent?: (event: DelegationEventResponse) => void;
  onToolApprovalRequired?: (
    id: string,
    name: string,
    args: Record<string, unknown>,
  ) => void;
  onToolApproved?: (id: string) => void;
  onToolDenied?: (id: string) => void;
  onSteeringInjected?: (pendingId: string | undefined, message: string) => void;
  onWorkerInputStaged?: (event: WorkerInputStagedEvent) => void;
  onWorkerResponsePending?: (event: WorkerResponseBoundaryEvent) => void;
  onWorkerResponseCommitted?: (event: WorkerResponseBoundaryEvent) => void;
  onTurnComplete?: (turn: number, hasMore: boolean) => void;
  onPlanUpdate: (items: PlanItem[]) => void;
  onWorkflowUpdated?: (
    goalId: string,
    aggregateRevision: number,
    operationId: string,
  ) => void;
  onModeChange: (mode: string, reason?: string) => void;
  onPlanComplete: (
    toolCallId: string,
    title: string,
    taskCount: number,
  ) => void;
  onUsage: (
    promptTokens: number,
    completionTokens: number,
    metrics?: UsageMetrics,
  ) => void;
  onLagged?: (skipped: number) => void;
  onContextCompactionStarted?: (event: ContextCompactionStartedEvent) => void;
  onSessionPinched?: (event: SessionContinuationEvent) => void;
  onTitleUpdate: (title: string) => void;
  onFinish: (sessionId: string, stopReason?: string) => void;
  onError: (error: string) => void;
  // Hive autonomous agent callbacks
  onUserMessage?: (
    title: string | undefined,
    message: string,
    level: string,
  ) => void;
  onAgentSleeping?: (durationSecs: number, reason: string) => void;
  onTickInjected?: (tickNumber: number) => void;
  onClassifierDecision?: (
    toolName: string,
    decision: string,
    reason: string,
    stage: number,
  ) => void;
  onTeammateSpawned?: (name: string, role: string) => void;
  onTeammateTaskCompleted?: (
    name: string,
    taskId: string,
    result: string,
  ) => void;
  onTeammateTaskFailed?: (name: string, taskId: string, error: string) => void;
  onTeammateCancelled?: (name: string) => void;
}

// ============================================================================
// Git Types
// ============================================================================

export interface GitStatusResponse {
  in_repo: boolean;
  repo_root: string | null;
  branch: string | null;
  head: string | null;
  upstream: string | null;
  branch_files: number;
  branch_additions: number;
  branch_deletions: number;
  pr_number: number | null;
  ahead: number;
  behind: number;
  staged: number;
  modified: number;
  untracked: number;
  conflicted: number;
  total_changes: number;
}

export interface GitChangedFile {
  path: string;
  status: string;
  additions: number;
  deletions: number;
}

export interface GitChangesResponse {
  in_repo: boolean;
  repo_root: string | null;
  files: GitChangedFile[];
  truncated?: boolean;
}

export interface GitFileDiffResponse {
  path: string;
  patch: string;
  truncated: boolean;
  binary: boolean;
}

export interface GitBranch {
  name: string;
  is_current: boolean;
  upstream: string | null;
  is_remote: boolean;
}

export interface GitBranchesResponse {
  repo_root: string;
  branches: GitBranch[];
}

export interface GitWorktree {
  path: string;
  branch: string | null;
  head: string | null;
  is_current: boolean;
}

export interface GitWorktreesResponse {
  repo_root: string;
  worktrees: GitWorktree[];
}

// ============================================================================
// Auth & Provider Types
// ============================================================================

export interface ProviderStatus {
  id: string;
  name: string;
  configured: boolean;
  has_oauth: boolean;
  supports_oauth: boolean;
  auth_methods?: Array<"api_key" | "oauth_browser" | "oauth_device">;
}

export interface OAuthDeviceCodeInfo {
  user_code: string;
  verification_uri: string;
  verification_uri_complete?: string | null;
  expires_in: number;
}

export interface OAuthStartResponse {
  auth_url: string;
  provider: string;
  flow_type: "browser_callback" | "browser_process" | "device" | "paste_code";
  paste_code: boolean;
  device_code?: OAuthDeviceCodeInfo | null;
}

export interface OAuthStatusResponse {
  has_token: boolean;
  flow_active: boolean;
}

export interface OAuthExchangeResponse {
  success: boolean;
}

export interface TailscaleAccessResponse {
  status: string;
  url?: string | null;
  detail?: string | null;
}

export interface ServerAccessResponse {
  local_url: string;
  remote_access_enabled: boolean;
  remote_access_token_available: boolean;
  revealed_remote_access_token?: string | null;
  remote_launch_url?: string | null;
  tailscale: TailscaleAccessResponse;
}

// ============================================================================
// Presence Types
// ============================================================================

export type PresenceCapability = "observer" | "controller";

export interface SessionPresenceClientResponse {
  client_id: string;
  surface: string;
  capability: PresenceCapability;
  user_id?: string | null;
  last_seen_at: string;
  last_event_sequence?: number | null;
  stale: boolean;
}

export interface SessionPresenceResponse {
  session_id: string;
  active_viewers: number;
  active_controllers: number;
  stale_clients: number;
  clients: SessionPresenceClientResponse[];
}

// ============================================================================
// Model Types
// ============================================================================

/**
 * Provider-aware executable model identity.
 *
 * String-valued wire fields keep this client forward-compatible with new
 * providers, auth scopes, and API transports added by the server.
 */
export interface ModelKey {
  provider: string;
  model_id: string;
  auth_scope?: string | null;
  api_format: string;
}

export interface ModelInfo {
  /** Absent only when reading an older Mitsuro server response. */
  key?: ModelKey | null;
  id: string;
  display_name: string;
  provider: string;
  context_window: number;
  max_output: number;
  /** Legacy flag retained for compatibility; prefer supported_reasoning_levels. */
  supports_thinking: boolean;
  reasoning_control?: ReasoningControl | null;
  supported_reasoning_levels?: ReasoningEffort[];
  default_reasoning_level?: ReasoningEffort | null;
  reasoning_is_mandatory?: boolean;
  supports_fast_mode?: boolean;
  fast_mode?: FastMode | null;
  supports_tools: boolean;
  supports_vision: boolean;
}

export type ReasoningEffort =
  | "none"
  | "minimal"
  | "low"
  | "medium"
  | "high"
  | "xhigh"
  | "max"
  | "ultra";

export type ReasoningControl =
  | "open_ai_effort"
  | "anthropic_adaptive"
  | "anthropic_budget"
  | "boolean"
  | "output_only";

export type FastMode = "priority" | "anthropic_fast";

export interface ModelsResponse {
  models: ModelInfo[];
  default_model: string | null;
  /** Exact default selection; absent only on older Mitsuro servers. */
  default_model_key?: ModelKey | null;
}

// ============================================================================
// File Types
// ============================================================================

export interface TreeEntry {
  name: string;
  path: string;
  is_dir: boolean;
  children?: TreeEntry[];
}

// ============================================================================
// Preview / MCP / Skills Types
// ============================================================================

export type BrowserSessionKind = "interactive" | "agent";
export type BrowserSessionStatus =
  | "starting"
  | "ready"
  | "running"
  | "stopped"
  | "error";

export interface BrowserCapability {
  available: boolean;
  runtime: "agent-browser";
  version: string;
  executable?: string | null;
  live_stream: boolean;
  semantic_actions: boolean;
  agent_chat: boolean;
  control_leases: boolean;
  control_lease_version: number;
  control_lease_ttl_seconds: number;
  reason?: string | null;
}

/** Public controller identity; mutation authority is intentionally omitted. */
export interface BrowserControlLeaseSummary {
  client_id: string;
  expires_at: string;
}

/** Ephemeral, in-memory authority for one server-owned Browser session. */
export interface BrowserControlLease {
  resource_id: string;
  client_id: string;
  lease_token: string;
  expires_at: string;
}

export interface TakeBrowserControlResponse {
  lease: BrowserControlLease;
}

export interface BrowserSession {
  id: string;
  title: string;
  /** Canonical project directory frozen when the server-owned session is created. */
  project_dir: string;
  kind: BrowserSessionKind;
  status: BrowserSessionStatus;
  url?: string | null;
  /** Raw CDP is intentionally never exposed by Honey. */
  cdp_url?: null;
  debug_port?: null;
  stream_url?: string | null;
  viewers: number;
  controllers: number;
  controller?: BrowserControlLeaseSummary | null;
  last_error?: string | null;
  created_at: string;
  updated_at: string;
  viewport_mode: "mobile" | "desktop";
}

export interface BrowserSessionListResponse {
  sessions: BrowserSession[];
  capability: BrowserCapability;
}

export interface CreateBrowserSessionRequest {
  title?: string;
  /** Defaults to the authenticated workspace for older clients. */
  project_dir?: string;
  kind?: BrowserSessionKind;
  url?: string;
  launch_local?: boolean;
}

export type BrowserAction =
  | { type: "navigate"; url: string }
  | {
    type: "snapshot";
    interactive?: boolean;
    compact?: boolean;
    depth?: number;
  }
  | { type: "click"; target: string }
  | { type: "fill" | "type"; target: string; value: string }
  | { type: "press"; key: string }
  | { type: "hover"; target: string }
  | { type: "select"; target: string; values: string[] }
  | {
    type: "scroll";
    direction: "up" | "down" | "left" | "right";
    amount?: number;
  }
  | { type: "back" | "forward" | "reload" }
  | { type: "wait"; ms: number }
  | {
    type: "get";
    property: "text" | "html" | "value" | "title" | "url" | "count";
    target?: string;
  }
  | { type: "attribute"; target: string; name: string }
  | { type: "viewport"; mode: "mobile" | "desktop" };

export interface BrowserActionResponse {
  ok: boolean;
  results: unknown;
}

export interface BrowserAgentRequest {
  task: string;
  model?: string;
  max_steps?: number;
}

export interface BrowserAgentResponse {
  ok: boolean;
  result?: string | null;
  error?: string | null;
}

export interface PreviewSettings {
  enabled: boolean;
  auto_refresh_secs: number;
  show_only_http_like: boolean;
  probe_timeout_ms: number;
  allow_force_open_non_http: boolean;
  pinned_ports: number[];
  hidden_ports: number[];
  blocked_ports: number[];
}

export interface PreviewSettingsPatch {
  enabled?: boolean;
  auto_refresh_secs?: number;
  show_only_http_like?: boolean;
  probe_timeout_ms?: number;
  allow_force_open_non_http?: boolean;
  pinned_ports?: number[];
  hidden_ports?: number[];
  blocked_ports?: number[];
}

export type PortProbeStatus =
  | "ok"
  | "timeout"
  | "conn_refused"
  | "non_http"
  | "error";

export interface PortEntry {
  port: number;
  name: string;
  description: string | null;
  command: string | null;
  pid: number | null;
  source: string;
  active: boolean;
  pinned: boolean;
  is_http_like: boolean;
  is_previewable_http: boolean;
  probe_status: PortProbeStatus;
  last_probe_ms: number | null;
  preview_path: string;
}

export interface PortListResponse {
  ports: PortEntry[];
  settings: PreviewSettings;
  discovery_error?: string | null;
}

export interface McpToolResponse {
  name: string;
  description?: string | null;
}

export interface McpServerResponse {
  name: string;
  server_type: string;
  status: string;
  connected: boolean;
  tool_count: number;
  tools: McpToolResponse[];
  error?: string | null;
}

export type SkillSource = "global" | "project" | "package";

export interface SkillInfo {
  name: string;
  description: string;
  version?: string | null;
  author?: string | null;
  tags: string[];
  source: SkillSource;
  origin: string;
  path: string;
  enabled: boolean;
  permission: "allow" | "ask" | "deny";
  model_invocable: boolean;
}

// ============================================================================
// Common Types
// ============================================================================

export type SessionMode = "build" | "plan";
export type SessionType = "chat" | "code" | "hive";
export type PermissionMode = "supervised" | "autonomous";
export type WorkspaceMode = "neutral" | "selected" | "created";
export type ThinkingLevel =
  | "off"
  | "minimal"
  | "low"
  | "medium"
  | "high"
  | "xhigh"
  | "max"
  | "ultra";

// ============================================================================
// Report Types
// ============================================================================

export interface ReportSummary {
  id: string;
  title: string;
  summary: string;
  tags: string[];
  created_at: string;
  project_dir?: string;
}

export interface Report extends ReportSummary {
  content: string;
  sources: string[];
  session_id: string;
}
