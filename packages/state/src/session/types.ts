import type {
  Citation,
  DelegatedRunStage,
  DelegatedToolKind,
  DelegationGroupState,
  DelegationTaskState,
  ModelInfo,
  ModelKey,
  SessionType,
  ThinkingLevel as ApiThinkingLevel,
  UsageMetrics,
  WorkflowCommand,
  WorkflowMutation,
  WorkspaceMode,
} from "@mitsuro/api";

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
  attemptCount?: number;
  taskState?: DelegationTaskState;
  integrationState?: "pending" | "ready" | "failed" | null;
}

export interface DelegatedArtifactState {
  kind: DelegatedToolKind;
  name?: string;
  capabilities?: Array<"read" | "write" | "execute">;
  delegatedRunId?: string;
  stage?: DelegatedRunStage;
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

export interface ChatMessageAttachment {
  type: "image" | "file";
  name?: string;
  mimeType?: string;
  uri?: string;
  base64?: string;
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
  workerStagedInputId?: string;
  successorRunId?: string;
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

export type SessionMode = "build" | "plan";
export type PermissionMode = "supervised" | "autonomous";
export type ThinkingLevel = ApiThinkingLevel;

export interface Attachment {
  name: string;
  type: "image" | "file";
  mimeType: string;
  uri?: string;
  base64?: string;
  text?: string;
}

export interface SendMessageOptions {
  projectDir?: string | null;
  workingDir?: string | null;
  workspaceMode?: WorkspaceMode;
  sessionType?: SessionType;
  targetBranch?: string | null;
  /** Client-only ownership hint. Never serialized into the chat request. */
  hiveConversationKind?: "worker_dm" | "primary_hive";
  /**
   * Client-only handoff for a queue claimed by the just-finished stream.
   * The store retains this exact batch until the successor is accepted or
   * restores it to its originating session after a definite failure.
   */
  queuedSuccessor?: QueuedSuccessorClaimInput;
}

export interface QueuedMessage {
  /** Stable optimistic row identity used for exact retry and deduplication. */
  id: string;
  /** Stable per-store ordering reservation for concurrent steer fallbacks. */
  orderKey?: string;
  /** Durable transport intent for direct Worker input. Legacy queues are Chat. */
  workerOperation?: "chat" | "steer";
  /**
   * Exact Worker retry identity, persisted before the matching transport can
   * begin. Keeping this on the individual input (rather than only on the
   * currently claimed batch) also protects a steer queued behind a live Chat.
   */
  workerInput?: {
    operation: "chat" | "steer";
    fingerprint: string;
    key: string;
  };
  /** Canonical user-turn count before this optimistic row was appended. */
  canonicalUserCountBefore?: number;
  content: string;
  attachments: Attachment[];
  sendOptions?: SendMessageOptions;
}

export interface QueuedSuccessorClaimInput {
  id: string;
  sessionId: string;
  /** Original durable owner when a validated pinch moves the queue to sessionId. */
  sourceSessionId?: string;
  queuedMessages: QueuedMessage[];
  /** Process-local authority for this exact claim. Never serialized. */
  attemptToken?: string;
}

export interface StopStreamingOptions {
  expectedSessionId?: string;
  hiveConversationKind?: "worker_dm" | "primary_hive";
}

export interface SessionDeletionAdmission {
  /** No-throw release after the server confirms deletion. */
  commit(): void;
  /**
   * Restore the exact pre-delete recovery record when server deletion fails.
   * A persistence failure keeps this lease admitted so rollback can be retried.
   */
  rollback(): Promise<void>;
}

export interface SessionStoreState {
  sessionId: string | null;
  sessionType: SessionType | null;
  title: string;
  mode: SessionMode;
  permissionMode: PermissionMode;
  messages: ChatMessage[];
  queuedMessages: QueuedMessage[];
  queuedRecoveryBlocked: boolean;
  isLoading: boolean;
  isStreaming: boolean;
  isThinking: boolean;
  thinkingContent: string;
  thinkingEnabled: boolean;
  thinkingLevel: ThinkingLevel;
  fastModeEnabled: boolean;
  tokenCount: number;
  /** Last live usage snapshot, retaining uncached/cache/output buckets. */
  tokenUsage: UsageMetrics | null;
  lastEventSequence: number | null;
  /** Cursor for the canonical append-only delegation event stream. */
  delegationEventCursor: number | null;
  error: string | null;
  model: string | null;
  modelKey: ModelKey | null;
  modelProvider: string | null;
  modelInfo: ModelInfo | null;

  sendMessage: (
    content: string,
    attachments?: Attachment[],
    sendOptions?: SendMessageOptions,
  ) => Promise<void>;
  retryQueuedRecovery: () => Promise<void>;
  discardQueuedRecovery: (sessionId?: string) => Promise<void>;
  /**
   * Concurrent calls reject; one lease owns one DELETE. After rollback storage
   * failure, the next call repairs that rollback before acquiring a fresh lease.
   */
  beginSessionDeletionAdmission: (
    sessionId: string,
  ) => Promise<SessionDeletionAdmission>;
  loadSession: (sessionId: string, isRefresh?: boolean) => Promise<void>;
  /**
   * Invalidate pending hydration work without clearing the optimistic shell.
   * Hidden mobile modes use this to prevent stale transcript processing.
   */
  cancelPendingSessionLoad: () => void;
  /**
   * Ensure and load the durable per-user Hive companion session.
   * Does not create a new job/run session — resolves GET/POST /hive/main.
   */
  ensureHiveMainSession: () => Promise<string | null>;
  clearSession: () => void;
  initSession: (
    sessionId: string,
    title: string,
    permissionMode?: PermissionMode,
    sessionType?: SessionType,
  ) => void;
  setTitle: (title: string) => void;
  updateTitle: (sessionId: string, title: string) => Promise<void>;
  setMode: (mode: SessionMode) => void;
  executeWorkflowCommand: (
    command: WorkflowCommand,
  ) => Promise<WorkflowMutation>;
  setModel: (
    model: string | null,
    provider?: string | null,
    modelInfo?: ModelInfo | null,
    modelKey?: ModelKey | null,
  ) => void;
  setThinkingLevel: (level: ThinkingLevel) => void;
  toggleThinking: () => void;
  setFastModeEnabled: (enabled: boolean) => void;
  toggleFastMode: () => void;
  togglePermissionMode: () => void;
  submitToolResult: (toolCallId: string, result: string) => Promise<void>;
  submitToolApproval: (toolCallId: string, approved: boolean) => Promise<void>;
  /**
   * Disconnect the local UI from the active stream without cancelling the
   * server-side session. Used when navigating between conversations or modes.
   */
  detachSession: () => void;
  stopStreaming: (options?: StopStreamingOptions) => void;
  startStatePolling: (sessionId: string) => void;
  stopStatePolling: () => void;
  refreshDelegationState: (sessionId: string) => void;
  startPresenceHeartbeat: (sessionId: string) => void;
  stopPresenceHeartbeat: (sessionId?: string | null) => void;
  cleanup: () => void;
}

export interface AssistantMessageRef {
  current: ChatMessage;
}
