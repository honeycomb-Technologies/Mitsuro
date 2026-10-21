import { create } from "zustand";
import { MitsuroApiError } from "@mitsuro/api";
import type {
  MitsuroClient,
  ModelInfo,
  ModelKey,
  SessionStateResponse as ApiSessionStateResponse,
  SessionType,
  StreamCallbacks,
} from "@mitsuro/api";
import type { createPlanStore } from "../plan";
import type { createSessionsStore } from "../sessions";
import type { MitsuroStorage } from "../storage";
import type { createWorkspaceStore } from "../workspace";
import {
  beginMitsuroPerformanceSpan,
  recordMitsuroPerformanceMetric,
  trackMitsuroPerformanceResource,
} from "../performance";

import {
  MAX_LAST_KNOWN_SERVER_STATE,
  MAX_QUEUED_MESSAGES,
  PRESENCE_CLIENT_STORAGE_KEY,
  PRESENCE_HEARTBEAT_INTERVAL,
  STATE_POLL_DEGRADED_AFTER,
  STATE_POLL_DEGRADED_MESSAGE,
  STATE_POLL_INTERVAL,
  STATE_POLL_MAX_BACKOFF,
  STATE_POLL_MAX_FAILURES,
} from "./constants";
import {
  buildContentBlocks,
  getUnsupportedImageAttachment,
  processStoredMessagesCooperatively,
  unsupportedImageMimeTypeMessage,
} from "./messages";
import { modelKeysEqual } from "./modelSelection";
import {
  persistCurrentModel,
  persistSessionMode,
  persistSessionModel,
  persistSessionPermissionMode,
  syncSessionPresence,
} from "./persistence";
import { applyDelegatedSessionState } from "./delegated";
import {
  applySessionSnapshot,
  hasActiveDelegationGroups,
  isActionableSessionAgentState,
  isActiveSessionAgentState,
  isTerminalSessionAgentState,
  pendingInteractionsFromSnapshot,
  sessionAgentErrorMessage,
  shouldStopSessionStatePolling,
} from "./serverState";
import {
  buildOptimisticSessionShell,
  buildSessionSnapshotFromResponse,
  type CachedSessionSnapshot,
  normalizeDisplayTitle,
  SessionSnapshotCache,
} from "./sessionCache";
import { createStreamCallbacks } from "./streaming";
import { WorkerInputIdempotency } from "./workerInputIdempotency";
import {
  canPersistQueuedRecovery,
  QueuedSuccessorRecovery,
  type QueuedSuccessorRecoveryClaim,
  type QueuedSuccessorRecoveryRecord,
  type QueuedWorkerInputIdentity,
} from "./queuedSuccessorRecovery";
import {
  applyLivePartialAssistant,
  applyRecoveryParity,
  createChatMessageId,
  createStreamingAssistantMessage,
  discardTransientAssistantMessages,
  finalizeTransientAssistantMessages,
  pruneEmptyAssistantMessages,
  toErrorMessage,
  upsertTransientAssistantMessage,
} from "./transient";
import type {
  AssistantMessageRef,
  Attachment,
  ChatMessage,
  ChatMessageAttachment,
  PermissionMode,
  QueuedMessage,
  QueuedSuccessorClaimInput,
  SendMessageOptions,
  SessionDeletionAdmission,
  SessionMode,
  SessionStoreState,
  ThinkingLevel,
} from "./types";
import {
  cycleThinkingLevel,
  isThinkingEnabled,
  normalizeThinkingLevel,
  supportsFastMode,
  thinkingLevelToApiValue,
} from "./thinking";

function hasOwnProperty<T extends object>(value: T, key: PropertyKey): boolean {
  return Object.hasOwn(value, key);
}

const WORKER_DM_BLOCKED_BY_NON_CONVERSATION_RUN =
  "Worker direct message is blocked by non-conversation run";
const QUEUED_RECOVERY_UNCERTAIN_MESSAGE =
  "A queued message may already have been delivered. Review the conversation, then retry or discard that queued message.";
const SESSION_STOP_PENDING_MESSAGE =
  "The response is still stopping. Try this message again when Stop finishes.";

function isWorkerDmBlockedByNonConversationRunMessage(
  message: string,
): boolean {
  return message.includes(WORKER_DM_BLOCKED_BY_NON_CONVERSATION_RUN);
}

function isWorkerDmBlockedByNonConversationRun(
  error: unknown,
): error is MitsuroApiError {
  return error instanceof MitsuroApiError &&
    error.status === 409 &&
    (isWorkerDmBlockedByNonConversationRunMessage(error.message) ||
      isWorkerDmBlockedByNonConversationRunMessage(error.responseBody));
}

function normalizeTargetBranch(
  targetBranch: string | null | undefined,
): string | null {
  const trimmed = targetBranch?.trim();
  return trimmed ? trimmed : null;
}

function isNotFoundApiError(err: unknown): boolean {
  if (err instanceof MitsuroApiError) {
    return err.status === 404;
  }
  if (!err || typeof err !== "object") {
    return false;
  }
  const status = (err as { status?: unknown }).status;
  return status === 404;
}

// Recovery tests replace the global timer API to drive only their polling
// clock. Capture the real host scheduler when the store module loads so a
// cooperative hydration turn cannot be stranded in an unrelated fake queue.
const scheduleSessionHydrationHostTurn = globalThis.setTimeout.bind(globalThis);

function yieldSessionHydrationHost(): Promise<void> {
  return new Promise((resolve) => scheduleSessionHydrationHostTurn(resolve, 0));
}

function buildDisplayAttachments(
  attachments: Attachment[],
): ChatMessageAttachment[] {
  return attachments
    .map((attachment) => ({
      type: attachment.type,
      name: attachment.name,
      mimeType: attachment.mimeType,
      uri: attachment.uri,
      base64: attachment.type === "image" ? attachment.base64 : undefined,
    }))
    .filter((attachment) =>
      attachment.type !== "image" ||
      Boolean(attachment.uri || attachment.base64)
    );
}

function workerInputFingerprint(request: object): string {
  return JSON.stringify(request);
}

function workerQueuedRetryPolicyFingerprint(request: {
  session_id?: string;
  fast_mode?: boolean;
  thinking_enabled?: unknown;
}): string {
  // The durable queue record already owns the exact message/content blocks.
  // Persist only the small mutable execution-policy boundary needed to prove a
  // later retry still describes the same request, never a second copy of rich
  // attachment/base64 payloads.
  return JSON.stringify({
    session_id: request.session_id,
    fast_mode: request.fast_mode,
    thinking_enabled: request.thinking_enabled,
  });
}

export function createSessionStore(
  client: MitsuroClient,
  storage: MitsuroStorage,
  workspace: ReturnType<typeof createWorkspaceStore>,
  sessionsStore: ReturnType<typeof createSessionsStore>,
  planStore: ReturnType<typeof createPlanStore>,
  ownerSessionType?: SessionType,
) {
  let statePollingTimer: ReturnType<typeof setTimeout> | null = null;
  let delegationRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  let statePollingGeneration = 0;
  let streamAttachmentGeneration = 0;
  let sessionSelectionGeneration = 0;
  const sessionCache = new SessionSnapshotCache();
  const inFlightSessionLoads = new Map<string, Promise<void>>();
  const inFlightSessionHydrations = new Map<
    string,
    Promise<{
      data: Awaited<ReturnType<MitsuroClient["getSession"]>>;
      serverStateResult: {
        ok: boolean;
        state: ApiSessionStateResponse | null;
      };
    }>
  >();
  const lastKnownServerState = new Map<string, ApiSessionStateResponse>();
  let presenceHeartbeatInterval: ReturnType<typeof setInterval> | null = null;
  let presenceHeartbeatSessionId: string | null = null;
  let presenceDesired = false;
  let releaseStatePollingResource: (() => void) | null = null;
  let releasePresenceHeartbeatResource: (() => void) | null = null;
  let abortController: AbortController | null = null;
  let presenceClientId: string | null = null;
  let readStoreState: (() => SessionStoreState) | null = null;
  const workerInputIdempotency = new WorkerInputIdempotency();
  const queuedSuccessorRecovery = new QueuedSuccessorRecovery(
    storage,
    ownerSessionType ?? "shared",
  );
  const hiveConversationKinds = new Map<
    string,
    "worker_dm" | "primary_hive"
  >();
  const pendingHiveConversationKinds = new Map<
    string,
    Promise<"worker_dm" | "primary_hive" | null>
  >();

  let activeWorkerResponseBoundary: {
    sessionId: string;
    runId: string;
  } | null = null;
  let activeHiveStreamOwner: {
    sessionId: string;
    generation: number;
    kind: "worker_dm" | "primary_hive";
  } | null = null;
  let pendingStoppedWorkerResponse: {
    sessionId: string;
    /** Null when Stop preceded the first response boundary or recovered mid-run. */
    runId: string | null;
  } | null = null;
  let activeWorkerStopSettlement: {
    sessionId: string;
    generation: number;
  } | null = null;
  let stopGeneration = 0;
  // Ordinary responses keep their visible draft on Stop, but detaching SSE is
  // not evidence that the server stopped. Reconcile the cancellation receipt
  // against a fresh snapshot before allowing another send.
  let activeSessionStopSettlement: {
    sessionId: string;
    generation: number;
    readyForSnapshot: boolean;
    error: string | null;
  } | null = null;

  let localStreamGeneration: number | null = null;
  const stoppedStreamGenerations = new Set<number>();
  const deferredCanonicalReloads = new Set<string>();
  const queuedSuccessorClaims = new Map<string, string>();
  const steeringInFlightSessions = new Set<string>();
  const queuedAppendInFlightSessions = new Map<string, number>();
  type SessionDeletionAdmissionState = {
    phase:
      | "opening"
      | "open"
      | "rolling_back"
      | "rollback_failed"
      | "repairing";
  };
  const sessionDeletionAdmissions = new Map<
    string,
    SessionDeletionAdmissionState
  >();
  let queuedInputSequence = 0;
  let disposed = false;

  function beginQueuedAppend(sessionId: string): void {
    queuedAppendInFlightSessions.set(
      sessionId,
      (queuedAppendInFlightSessions.get(sessionId) ?? 0) + 1,
    );
  }

  function endQueuedAppend(sessionId: string): void {
    const remaining = (queuedAppendInFlightSessions.get(sessionId) ?? 0) - 1;
    if (remaining > 0) {
      queuedAppendInFlightSessions.set(sessionId, remaining);
    } else {
      queuedAppendInFlightSessions.delete(sessionId);
    }
  }

  function hasQueuedAppend(sessionId: string): boolean {
    return (queuedAppendInFlightSessions.get(sessionId) ?? 0) > 0;
  }

  async function resolveHiveConversationKind(
    sessionId: string,
  ): Promise<"worker_dm" | "primary_hive" | null> {
    const known = hiveConversationKinds.get(sessionId);
    if (known) return known;
    const pending = pendingHiveConversationKinds.get(sessionId);
    if (pending) return pending;

    const request = (async () => {
      try {
        const binding = await client.getHiveWorkerBySession(sessionId);
        if (binding.session_id !== sessionId) return null;
        if (binding.kind !== "worker_dm" && binding.kind !== "primary_hive") {
          return null;
        }
        hiveConversationKinds.set(sessionId, binding.kind);
        return binding.kind;
      } catch {
        // An older or temporarily unavailable classifier must not cause an
        // unkeyed Worker mutation. Unknown Hive requests conservatively use
        // an idempotency key; ordinary non-Hive Chat remains untouched.
        return null;
      } finally {
        pendingHiveConversationKinds.delete(sessionId);
      }
    })();
    pendingHiveConversationKinds.set(sessionId, request);
    return request;
  }

  function stopPresenceTransport(sessionId?: string | null) {
    const ownedSessionId = presenceHeartbeatSessionId;
    if (sessionId && ownedSessionId && sessionId !== ownedSessionId) return;
    if (presenceHeartbeatInterval) {
      clearInterval(presenceHeartbeatInterval);
      presenceHeartbeatInterval = null;
    }
    releasePresenceHeartbeatResource?.();
    releasePresenceHeartbeatResource = null;
    presenceHeartbeatSessionId = null;

    if (!ownedSessionId) return;
    const clientId = getPresenceClientId();
    if (!clientId) return;
    void client.removeSessionPresence(ownedSessionId, clientId).catch(() => {});
  }

  function startPresenceTransport(sessionId: string) {
    if (!presenceDesired) return;
    if (
      presenceHeartbeatSessionId === sessionId &&
      presenceHeartbeatInterval
    ) {
      return;
    }
    stopPresenceTransport();
    presenceHeartbeatSessionId = sessionId;
    const getState = readStoreState;
    if (!getState) return;
    void syncPresence(sessionId, getState);
    presenceHeartbeatInterval = setInterval(() => {
      void syncPresence(sessionId, getState);
    }, PRESENCE_HEARTBEAT_INTERVAL);
    releasePresenceHeartbeatResource = trackMitsuroPerformanceResource(
      "presence_heartbeats",
    );
  }

  function getSessionHydration(
    sessionId: string,
    prefetchedServerState?: ApiSessionStateResponse | null,
  ) {
    const existing = inFlightSessionHydrations.get(sessionId);
    if (existing) return existing;

    const statePromise = prefetchedServerState !== undefined
      ? Promise.resolve({
        ok: true,
        state: prefetchedServerState,
      })
      : client.getSessionState
      ? client.getSessionState(sessionId, { includeDelegatedHistory: true })
        .then(
          (state) => ({ ok: true, state }),
          () => ({ ok: false, state: null }),
        )
      : Promise.resolve({ ok: false, state: null });
    const hydration = Promise.all([
      client.getSession(sessionId),
      statePromise,
    ]).then(([data, serverStateResult]) => ({ data, serverStateResult }));
    inFlightSessionHydrations.set(sessionId, hydration);
    void hydration.finally(() => {
      if (inFlightSessionHydrations.get(sessionId) === hydration) {
        inFlightSessionHydrations.delete(sessionId);
      }
    }).catch(() => {});
    return hydration;
  }

  function isLocalStreamAttached(): boolean {
    // True only while this client is actively consuming an SSE stream.
    // Must not stay true after the transport ends, or recovery/poll will skip
    // full transcript remaps that restore approvals and canonical history.
    return localStreamGeneration !== null &&
      localStreamGeneration === streamAttachmentGeneration;
  }

  function withoutPendingStoppedWorkerPartial(
    sessionId: string,
    serverState: ApiSessionStateResponse,
  ): ApiSessionStateResponse {
    if (pendingStoppedWorkerResponse?.sessionId !== sessionId) {
      return serverState;
    }
    return {
      ...serverState,
      live_partial_assistant: null,
    };
  }

  function clearWorkerStopSettlement(
    sessionId: string,
    generation: number,
  ): void {
    if (
      stopGeneration === generation &&
      activeWorkerStopSettlement?.sessionId === sessionId &&
      activeWorkerStopSettlement.generation === generation
    ) {
      activeWorkerStopSettlement = null;
    }
  }

  async function reconcileStoppedWorkerResponse(
    sessionId: string,
    runId: string | null,
    generation: number,
    getState: () => SessionStoreState,
    settlePresentation: () => void,
  ): Promise<void> {
    const isCurrent = () =>
      stopGeneration === generation &&
      getState().sessionId === sessionId &&
      activeWorkerStopSettlement?.sessionId === sessionId &&
      activeWorkerStopSettlement.generation === generation &&
      pendingStoppedWorkerResponse?.sessionId === sessionId &&
      pendingStoppedWorkerResponse.runId === runId;

    // The Stop receipt commits before a running host necessarily reaches its
    // terminal fence. Rehydrate immediately, but keep the exact run's partial
    // suppressed until durable Hive runtime state moves to its successor/idle.
    if (isCurrent()) {
      try {
        await getState().loadSession(sessionId, true);
      } catch {
        // The exact status loop below remains authoritative and retryable.
      }
    }

    let retryDelayMs = 250;
    while (isCurrent()) {
      try {
        const status = await client.getHiveSessionStatus(sessionId);
        if (!isCurrent()) return;
        const runtime = status.runtime;
        const stoppedRunSettled = !runtime ||
          runtime.current_run_id == null ||
          ["cancelled", "error", "idle"].includes(runtime.status) ||
          (runId !== null && runtime.current_run_id !== runId);
        if (stoppedRunSettled) {
          getState().stopStatePolling();
          try {
            // Keep the stopped-response sentinel installed through the final
            // canonical reload. A forced host abort can terminalize Hive runtime
            // before the generic session agent/recovery projection leaves its
            // stale streaming state; that projection must not resurrect the
            // discarded Worker draft or retain a recovery poll.
            await getState().loadSession(sessionId, true);
          } catch {
            // loadSession owns the visible error; the untrusted draft is gone.
          } finally {
            if (isCurrent()) {
              getState().stopStatePolling();
              settlePresentation();
              pendingStoppedWorkerResponse = null;
              clearWorkerStopSettlement(sessionId, generation);
            }
          }
          return;
        }
      } catch {
        // A transient status failure must not authenticate the stopped draft.
      }
      await new Promise<void>((resolve) => setTimeout(resolve, retryDelayMs));
      retryDelayMs = Math.min(retryDelayMs * 2, 2_000);
    }
  }

  function rememberServerState(
    sessionId: string,
    serverState: ApiSessionStateResponse | null | undefined,
  ) {
    if (!serverState) return;
    // LRU-ish: reinsert so oldest keys are the first Map iteration order.
    lastKnownServerState.delete(sessionId);
    lastKnownServerState.set(sessionId, {
      ...serverState,
      // Keep only status metadata; drop heavy live partials from the side map.
      live_partial_assistant: null,
      delegated_tools: [],
      recent_delegated_runs: [],
      delegated_run_summaries: [],
    });
    while (lastKnownServerState.size > MAX_LAST_KNOWN_SERVER_STATE) {
      const oldest = lastKnownServerState.keys().next().value;
      if (!oldest) break;
      lastKnownServerState.delete(oldest);
    }
  }

  function getPresenceClientId(): string | null {
    if (presenceClientId) return presenceClientId;
    try {
      const existing = storage.get(PRESENCE_CLIENT_STORAGE_KEY);
      if (existing) {
        presenceClientId = existing;
        return existing;
      }
      const generated = createChatMessageId("presence");
      storage.set(PRESENCE_CLIENT_STORAGE_KEY, generated);
      presenceClientId = generated;
      return generated;
    } catch {
      return null;
    }
  }

  function loadPermissionMode(): PermissionMode {
    try {
      const stored = storage.get("mitsuro-permission-mode");
      if (stored === "supervised" || stored === "autonomous") return stored;
    } catch {
      /* ignore */
    }
    return "autonomous";
  }

  const persistMode = (getState: () => SessionStoreState, mode: SessionMode) =>
    persistSessionMode(
      client,
      sessionsStore,
      getState,
      mode,
      ownerSessionType,
    );

  const persistModel = (
    getState: () => SessionStoreState,
    model: string | null,
    modelKey?: ModelKey | null,
  ) =>
    persistSessionModel(
      client,
      sessionsStore,
      getState,
      model,
      modelKey,
      ownerSessionType,
    );

  const persistPermissionMode = (
    getState: () => SessionStoreState,
    permissionMode: PermissionMode,
  ) =>
    persistSessionPermissionMode(
      client,
      sessionsStore,
      getState,
      permissionMode,
      ownerSessionType,
    );

  const persistCurrentSelectedModel = (
    model: string | null,
    modelKey?: ModelKey | null,
  ) =>
    ownerSessionType === "hive"
      ? Promise.resolve()
      : persistCurrentModel(client, model, modelKey);

  const syncPresence = (
    sessionId: string,
    getState: () => SessionStoreState,
  ) => syncSessionPresence(client, sessionId, getPresenceClientId(), getState);

  function attachLiveStreamLifecycle(
    callbacks: StreamCallbacks,
    generation: number,
  ): StreamCallbacks {
    const release = () => {
      if (localStreamGeneration === generation) {
        localStreamGeneration = null;
      }
      if (activeHiveStreamOwner?.generation === generation) {
        activeHiveStreamOwner = null;
      }
    };
    return {
      ...callbacks,
      onFinish: (sessionId, stopReason) => {
        release();
        callbacks.onFinish?.(sessionId, stopReason);
      },
      onError: (error) => {
        release();
        callbacks.onError?.(error);
      },
    };
  }

  function guardStreamCallbacks(
    callbacks: StreamCallbacks,
    isAttached: () => boolean,
    terminalRef?: { current: boolean },
  ): StreamCallbacks {
    return new Proxy(callbacks, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver);
        if (typeof value !== "function") return value;
        return (...args: unknown[]) => {
          if (!isAttached()) return undefined;
          const terminal = property === "onFinish" || property === "onError";
          try {
            return value.apply(target, args);
          } finally {
            if (terminal && terminalRef) terminalRef.current = true;
          }
        };
      },
    });
  }

  const initialState: Omit<
    SessionStoreState,
    | "sendMessage"
    | "retryQueuedRecovery"
    | "discardQueuedRecovery"
    | "beginSessionDeletionAdmission"
    | "loadSession"
    | "cancelPendingSessionLoad"
    | "ensureHiveMainSession"
    | "clearSession"
    | "initSession"
    | "setTitle"
    | "updateTitle"
    | "setMode"
    | "executeWorkflowCommand"
    | "setModel"
    | "setThinkingLevel"
    | "toggleThinking"
    | "setFastModeEnabled"
    | "toggleFastMode"
    | "togglePermissionMode"
    | "submitToolResult"
    | "submitToolApproval"
    | "detachSession"
    | "stopStreaming"
    | "startStatePolling"
    | "stopStatePolling"
    | "refreshDelegationState"
    | "startPresenceHeartbeat"
    | "stopPresenceHeartbeat"
    | "cleanup"
  > = {
    sessionId: null,
    sessionType: null,
    title: "",
    mode: "build",
    permissionMode: loadPermissionMode(),
    messages: [],
    queuedMessages: [],
    queuedRecoveryBlocked: false,
    isLoading: false,
    isStreaming: false,
    isThinking: false,
    thinkingContent: "",
    thinkingEnabled: true,
    thinkingLevel: "medium",
    fastModeEnabled: false,
    tokenCount: 0,
    tokenUsage: null,
    lastEventSequence: null,
    delegationEventCursor: null,
    error: null,
    model: null,
    modelKey: null,
    modelProvider: null,
    modelInfo: null,
  };

  // -------------------------------------------------------------------------
  // Create the Zustand store
  // -------------------------------------------------------------------------

  return create<SessionStoreState>((set, get) => {
    readStoreState = get;

    function pendingSessionStop(sessionId: string | null) {
      const pending = activeSessionStopSettlement;
      return pending?.sessionId === sessionId &&
          pending.generation === stopGeneration
        ? pending
        : null;
    }

    function reconcileSessionStopSnapshot(
      sessionId: string,
      serverState: ApiSessionStateResponse | null,
    ): void {
      const pending = pendingSessionStop(sessionId);
      if (!pending || get().sessionId !== sessionId) return;
      if (
        pending.readyForSnapshot && serverState &&
        (pending.error !== null ||
          isTerminalSessionAgentState(serverState.agent_state) ||
          isActionableSessionAgentState(serverState.agent_state))
      ) {
        // AskUser/PlanConfirm can finish before Cancel reaches the server.
        // Their authoritative awaiting_input snapshot settles this response
        // while preserving the interaction for a later user continuation.
        // A live tool approval remains tool_executing and keeps Stop pending.
        activeSessionStopSettlement = null;
        if (pending.error) set({ error: pending.error });
        return;
      }
      // A missing/old snapshot or a still-running accepted Stop must not make
      // the composer look idle. Polling owns the terminal/actionable transition.
      set({
        isLoading: !pending.readyForSnapshot,
        isStreaming: true,
        ...(pending.error ? { error: pending.error } : {}),
      });
    }

    async function stopOrdinarySession(sessionId: string, generation: number) {
      const settlement = pendingSessionStop(sessionId);
      if (!settlement || settlement.generation !== generation) return;
      const isCurrent = () =>
        get().sessionId === sessionId &&
        pendingSessionStop(sessionId) === settlement;
      try {
        await client.cancelSession(sessionId);
      } catch (error) {
        if (!isCurrent()) return;
        settlement.error = toErrorMessage(error, "Unable to stop the response");
        set({ error: settlement.error });
      }
      if (!isCurrent()) return;
      // A load started before the receipt cannot prove Stop's outcome. Let it
      // drain, then request a fresh canonical transcript/status pair.
      await inFlightSessionLoads.get(sessionId);
      await inFlightSessionHydrations.get(sessionId)?.catch(() => {});
      if (!isCurrent()) return;
      settlement.readyForSnapshot = true;
      await get().loadSession(sessionId, true);
      if (!isCurrent()) return;
      // loadSession also handles errors internally. If it could not establish
      // terminal state, retain pending ownership and retry through normal polls.
      reconcileSessionStopSnapshot(sessionId, null);
      get().startStatePolling(sessionId);
    }

    function applyStreamFailure(err: unknown) {
      set((s) => ({
        isLoading: false,
        isStreaming: false,
        isThinking: false,
        thinkingContent: "",
        messages: pruneEmptyAssistantMessages(
          finalizeTransientAssistantMessages(s.messages),
        ),
        error: toErrorMessage(err),
      }));
    }

    async function recoverAfterStreamInterruption(
      sessionId: string,
    ): Promise<boolean> {
      const recoveryAttachmentGeneration = streamAttachmentGeneration;
      const isRecoveryOriginCurrent = () =>
        get().sessionId === sessionId &&
        streamAttachmentGeneration === recoveryAttachmentGeneration;

      // A stale recovery belongs to the detached session. Treat it as handled
      // so its deferred error callback cannot fall through and mutate the
      // newly selected transcript.
      if (!isRecoveryOriginCurrent()) return true;

      try {
        const serverState = withoutPendingStoppedWorkerPartial(
          sessionId,
          await client.getSessionState(sessionId),
        );
        if (!isRecoveryOriginCurrent()) return true;
        rememberServerState(sessionId, serverState);
        // Recovery runs only when SSE is unavailable; always remap transcript.
        applySessionSnapshot(sessionId, serverState, true, set, get, planStore);
        if (!isRecoveryOriginCurrent()) return true;

        if (
          isActiveSessionAgentState(serverState.agent_state) ||
          hasActiveDelegationGroups(serverState.delegation_groups)
        ) {
          set({ isLoading: false, error: null });
          if (!isRecoveryOriginCurrent()) return true;
          get().startStatePolling(sessionId);
          return true;
        }

        if (isActionableSessionAgentState(serverState.agent_state)) {
          get().stopStatePolling();
          set({
            isLoading: false,
            isStreaming: false,
            isThinking: false,
            thinkingContent: "",
            error: null,
          });
          if (!isRecoveryOriginCurrent()) return true;
          await get().loadSession(sessionId, true);
          return true;
        }

        if (isTerminalSessionAgentState(serverState.agent_state)) {
          const terminalError = sessionAgentErrorMessage(serverState);
          get().stopStatePolling();
          set({
            isLoading: false,
            isStreaming: false,
            isThinking: false,
            thinkingContent: "",
            error: terminalError,
          });
          if (!isRecoveryOriginCurrent()) return true;
          await get().loadSession(sessionId, true);
          if (!isRecoveryOriginCurrent()) return true;
          if (terminalError && isRecoveryOriginCurrent()) {
            set({ error: terminalError });
          }
          // An idle snapshot with no canonical error does not prove that a
          // response completed. Let the original stream error surface after
          // refreshing the transcript instead of silently swallowing a clean
          // EOF that arrived before `finish`.
          return terminalError !== null;
        }
      } catch {
        if (!isRecoveryOriginCurrent()) return true;
        // The stream and snapshot endpoints can fail independently during a
        // reconnect. Keep the session protected from duplicate sends and let
        // the bounded polling policy recover canonical state.
        set({
          isLoading: false,
          isStreaming: true,
          error: STATE_POLL_DEGRADED_MESSAGE,
        });
        if (!isRecoveryOriginCurrent()) return true;
        get().startStatePolling(sessionId);
        return true;
      }

      return false;
    }

    function createRecoveringStreamCallbacks(
      callbacks: StreamCallbacks,
      sessionId: string | null,
      recoveryRef: { promise: Promise<boolean> | null },
    ): StreamCallbacks {
      return {
        ...callbacks,
        onError: (error) => {
          if (!sessionId) {
            callbacks.onError(error);
            return;
          }

          if (!recoveryRef.promise) {
            recoveryRef.promise = recoverAfterStreamInterruption(sessionId)
              .then(
                (recovered) => {
                  if (!recovered) {
                    callbacks.onError(error);
                  }
                  return recovered;
                },
              );
          }
        },
      };
    }

    function mergeClaimedRows(
      messages: ChatMessage[],
      rows: ChatMessage[],
    ): ChatMessage[] {
      const knownIds = new Set(messages.map((message) => message.id));
      const missing = rows.filter((message) => !knownIds.has(message.id));
      return missing.length > 0 ? [...messages, ...missing] : messages;
    }

    function queuedRecoveryForSession(sessionId: string) {
      const record = queuedSuccessorRecovery.get(sessionId);
      return {
        messages: (base: ChatMessage[]) =>
          record
            ? mergeClaimedRows(
              base,
              record.rows.map((message) => ({ ...message, isQueued: true })),
            )
            : base,
        queuedMessages: queuedSuccessorRecovery.tail(sessionId),
        blocked: queuedSuccessorRecovery.isOrdinaryUncertain(sessionId),
      };
    }

    function mergeQueuedPayloads(
      recovered: QueuedMessage[],
      live: QueuedMessage[],
    ): QueuedMessage[] {
      const seen = new Set<string>();
      return [...recovered, ...live].filter((message) => {
        if (seen.has(message.id)) return false;
        seen.add(message.id);
        return true;
      }).slice(0, MAX_QUEUED_MESSAGES);
    }

    function updateClaimedRowsInCache(
      sessionId: string,
      updater: (messages: ChatMessage[]) => ChatMessage[],
    ) {
      const cached = sessionCache.get(sessionId);
      if (!cached) return;
      sessionCache.set({
        ...cached,
        messages: updater(cached.messages),
        updatedAt: Date.now(),
      });
    }

    async function registerQueuedSuccessorClaim(
      input: QueuedSuccessorClaimInput,
      rows: ChatMessage[],
    ): Promise<QueuedSuccessorRecoveryClaim> {
      return await queuedSuccessorRecovery.claim(input, rows);
    }

    function activateQueuedSuccessorClaim(
      input: QueuedSuccessorClaimInput | undefined,
    ): void {
      if (!input) return;
      const claimedIds = new Set(
        input.queuedMessages.map((message) => message.id),
      );
      set((state) => ({
        queuedMessages: state.queuedMessages.filter(
          (message) => !claimedIds.has(message.id),
        ),
      }));
    }

    function commitAcceptedQueuedRows(
      input: QueuedSuccessorClaimInput,
    ): void {
      const messageIds = new Set(
        input.queuedMessages.map((message) => message.id),
      );
      const canonicalThresholds = input.queuedMessages.flatMap((message) =>
        message.canonicalUserCountBefore === undefined
          ? []
          : [message.canonicalUserCountBefore + 1]
      );
      const commit = (messages: ChatMessage[]) => {
        const canonicalUserCount = messages.filter((message) =>
          message.role === "user" && !message.isQueued &&
          !messageIds.has(message.id)
        ).length;
        const canonicalThreshold = canonicalThresholds.length > 0
          // One claimed batch is one canonical user turn even when several
          // queued UI rows were combined. Use the oldest row's baseline; a
          // max threshold would duplicate repeated/combined turns on reload.
          ? Math.min(...canonicalThresholds)
          : null;
        if (
          canonicalThreshold !== null &&
          canonicalUserCount >= canonicalThreshold
        ) {
          return messages.filter((message) =>
            !messageIds.has(message.id)
          );
        }
        return messages.map((message) =>
          messageIds.has(message.id) ? { ...message, isQueued: false } : message
        );
      };
      if (get().sessionId === input.sessionId) {
        set((state) => ({
          messages: commit(state.messages),
          queuedMessages: mergeQueuedPayloads(
            queuedSuccessorRecovery.tail(input.sessionId),
            state.queuedMessages.filter((message) =>
              !messageIds.has(message.id)
            ),
          ),
          queuedRecoveryBlocked: queuedSuccessorRecovery.isOrdinaryUncertain(
            input.sessionId,
          ),
        }));
      }
      updateClaimedRowsInCache(input.sessionId, commit);
    }

    async function acceptQueuedSuccessorClaim(
      input: QueuedSuccessorClaimInput | undefined,
    ): Promise<boolean> {
      if (!input?.attemptToken) return false;
      const accepted = await queuedSuccessorRecovery.acceptRemote(
        input.sessionId,
        input.id,
        input.attemptToken,
      );
      if (!accepted) return false;
      commitAcceptedQueuedRows(input);
      return true;
    }

    function observeQueuedAcceptance(
      acceptance: Promise<boolean>,
    ): Promise<boolean> {
      // A Worker response boundary may arrive long before streamChat settles.
      // Observe persistence immediately so a storage failure cannot become an
      // unhandled rejection while the transport is still open.
      void acceptance.catch(() => undefined);
      return acceptance;
    }

    async function abandonStoppedQueuedSuccessor(
      input: QueuedSuccessorClaimInput | undefined,
    ): Promise<boolean> {
      if (!input?.attemptToken) return false;
      const abandoned = await queuedSuccessorRecovery.accept(
        input.sessionId,
        input.id,
        input.attemptToken,
      );
      if (!abandoned) return false;
      const messageIds = new Set(
        input.queuedMessages.map((message) => message.id),
      );
      const discard = (messages: ChatMessage[]) =>
        messages.filter((message) => !messageIds.has(message.id));
      if (!disposed && get().sessionId === input.sessionId) {
        set((state) => ({
          messages: discard(state.messages),
          queuedMessages: mergeQueuedPayloads(
            queuedSuccessorRecovery.tail(input.sessionId),
            state.queuedMessages.filter((message) =>
              !messageIds.has(message.id)
            ),
          ),
          queuedRecoveryBlocked: queuedSuccessorRecovery.isOrdinaryUncertain(
            input.sessionId,
          ),
        }));
      }
      updateClaimedRowsInCache(input.sessionId, discard);
      return true;
    }

    async function rejectQueuedSuccessorClaim(
      input: QueuedSuccessorClaimInput | undefined,
      error: unknown,
    ): Promise<void> {
      if (!input?.attemptToken) return;
      const record = await queuedSuccessorRecovery.reject(
        input.sessionId,
        input.id,
        input.attemptToken,
      );
      if (!record || record.id !== input.id) return;
      const restoreRows = (messages: ChatMessage[]) =>
        mergeClaimedRows(
          messages,
          record.rows.map((message) => ({ ...message, isQueued: true })),
        );
      if (get().sessionId === input.sessionId) {
        set((state) => ({
          queuedMessages: record.phase === "rejected"
            ? [
              ...record.queuedMessages,
              ...state.queuedMessages.filter((message) =>
                !record.queuedMessages.some((pending) =>
                  pending.id === message.id
                )
              ),
            ].slice(0, MAX_QUEUED_MESSAGES)
            : state.queuedMessages,
          messages: restoreRows(state.messages),
          queuedRecoveryBlocked: queuedSuccessorRecovery.isOrdinaryUncertain(
            input.sessionId,
          ),
          error: toErrorMessage(error),
        }));
      }
      updateClaimedRowsInCache(input.sessionId, restoreRows);
    }

    async function releaseUndispatchedQueuedSuccessor(
      input: QueuedSuccessorClaimInput | undefined,
    ): Promise<void> {
      if (!input?.attemptToken) return;
      const record = await queuedSuccessorRecovery.releaseUndispatched(
        input.sessionId,
        input.id,
        input.attemptToken,
      );
      if (!record) return;
      const restoreRows = (messages: ChatMessage[]) =>
        mergeClaimedRows(
          messages,
          record.rows.map((message) => ({ ...message, isQueued: true })),
        );
      if (!disposed && get().sessionId === input.sessionId) {
        set((state) => ({
          queuedMessages: mergeQueuedPayloads(
            queuedSuccessorRecovery.tail(input.sessionId),
            state.queuedMessages,
          ),
          messages: restoreRows(state.messages),
          isLoading: false,
          isStreaming: false,
          queuedRecoveryBlocked: false,
        }));
      }
      updateClaimedRowsInCache(input.sessionId, restoreRows);
    }

    function resumeQueuedRecovery(sessionId: string): void {
      if (disposed || get().sessionId !== sessionId) {
        return;
      }
      const queuedMessages = queuedSuccessorRecovery.claimable(sessionId);
      const record = queuedSuccessorRecovery.get(sessionId);
      if (!record || queuedMessages.length === 0) return;
      const operation = record.workerInput?.operation ??
        queuedMessages[0]?.workerInput?.operation ??
        queuedMessages[0]?.workerOperation ?? "chat";
      if (get().isStreaming && operation !== "steer") return;
      const first = queuedMessages[0];
      void get().sendMessage(
        queuedMessages.map((message) => message.content).join("\n\n"),
        queuedMessages.flatMap((message) => message.attachments),
        {
          ...first?.sendOptions,
          queuedSuccessor: {
            id: record.id,
            sessionId,
            queuedMessages,
          },
        },
      ).catch(() => {
        // Exact rows and payload remain durable and visibly queued.
      });
    }

    return {
      ...initialState,

      // -- sendMessage --------------------------------------------------------

      async sendMessage(
        content: string,
        attachments: Attachment[] = [],
        sendOptions: SendMessageOptions = {},
      ) {
        if (disposed) {
          throw new Error("This session store is no longer active.");
        }
        const invocationSessionId = get().sessionId;
        const invocationSelectionGeneration = sessionSelectionGeneration;
        const invocationStopSettlementGeneration =
          stopGeneration;
        if (
          invocationSessionId &&
          queuedSuccessorRecovery.isDeletionAdmitted(invocationSessionId)
        ) {
          throw new Error(
            "This conversation is being deleted; input was not sent.",
          );
        }
        const isInvocationCurrent = () =>
          !disposed &&
          sessionSelectionGeneration === invocationSelectionGeneration &&
          get().sessionId === invocationSessionId;
        let queuedSuccessor = sendOptions.queuedSuccessor;
        let queuedClaimToken: string | null = null;
        if (
          !queuedSuccessor && invocationSessionId &&
          sendOptions.hiveConversationKind === undefined &&
          (get().sessionType === "hive" || sendOptions.sessionType === "hive")
        ) {
          const knownKind = hiveConversationKinds.get(invocationSessionId);
          if (!knownKind) void resolveHiveConversationKind(invocationSessionId);
          // Unknown Hive sessions use the conservative Worker contract. The
          // primary companion is the only Hive conversation allowed to use
          // the ordinary unkeyed path.
          sendOptions = {
            ...sendOptions,
            hiveConversationKind: knownKind ?? "worker_dm",
          };
        }
        if (
          queuedSuccessor &&
          queuedSuccessor.sessionId !== get().sessionId
        ) {
          throw new Error(
            "Queued successor no longer owns the active session.",
          );
        }
        const pendingStop = activeWorkerStopSettlement;
        if (
          pendingStop &&
          pendingStop.sessionId === get().sessionId &&
          pendingStop.generation === stopGeneration
        ) {
          throw new Error(
            "The Hive Worker is still stopping. Try this message again when Stop finishes.",
          );
        }
        if (pendingSessionStop(get().sessionId)) {
          throw new Error(SESSION_STOP_PENDING_MESSAGE);
        }
        if (queuedSuccessor) {
          const claimSessionId = queuedSuccessor.sessionId;
          queuedClaimToken =
            `${queuedSuccessor.id}:${Date.now()}:${Math.random()}`;
          if (queuedSuccessorClaims.has(claimSessionId)) {
            throw new Error("The queued successor is already being prepared.");
          }
          queuedSuccessorClaims.set(claimSessionId, queuedClaimToken);
          const claimedIds = new Set(
            queuedSuccessor.queuedMessages.map((message) => message.id),
          );
          const claimRows = get().messages.filter((message) =>
            claimedIds.has(message.id)
          );
          if (isInvocationCurrent()) {
            // Close the same-turn window between predecessor finish and durable
            // claim preparation. Any newer input will append behind this claim.
            set({ isLoading: true, isStreaming: true, error: null });
          }
          try {
            const claim = await registerQueuedSuccessorClaim(
              queuedSuccessor,
              claimRows,
            );
            queuedSuccessor = {
              id: claim.id,
              sessionId: claim.sessionId,
              queuedMessages: claim.queuedMessages,
              attemptToken: claim.attemptToken,
            };
            content = claim.queuedMessages
              .map((message) => message.content)
              .join("\n\n");
            attachments = claim.queuedMessages.flatMap((message) =>
              message.attachments
            );
            sendOptions = {
              ...claim.queuedMessages[0]?.sendOptions,
              queuedSuccessor,
            };
            if (
              !isInvocationCurrent() ||
              stopGeneration !==
                invocationStopSettlementGeneration ||
              queuedSuccessorRecovery.isDeletionAdmitted(claimSessionId) ||
              queuedSuccessorClaims.get(claimSessionId) !== queuedClaimToken
            ) {
              await releaseUndispatchedQueuedSuccessor(queuedSuccessor);
              throw new Error(
                "Queued input stayed with its original conversation after navigation.",
              );
            }
          } catch (error) {
            if (isInvocationCurrent()) {
              set({
                isLoading: false,
                isStreaming: false,
                error: toErrorMessage(error),
              });
            }
            if (
              queuedSuccessorClaims.get(claimSessionId) === queuedClaimToken
            ) {
              queuedSuccessorClaims.delete(claimSessionId);
            }
            throw error;
          }
        } else if (!get().isStreaming && invocationSessionId) {
          if (!queuedSuccessorRecovery.isReady()) {
            void queuedSuccessorRecovery.ready().then(() => {
              if (isInvocationCurrent()) {
                resumeQueuedRecovery(invocationSessionId);
              }
            }).catch(() => {});
            throw new Error(
              "Queued draft recovery is still opening. Your draft was kept; try Send again.",
            );
          }
          if (
            queuedSuccessorClaims.has(invocationSessionId) ||
            steeringInFlightSessions.has(invocationSessionId) ||
            hasQueuedAppend(invocationSessionId) ||
            queuedSuccessorRecovery.isDelivering(invocationSessionId)
          ) {
            throw new Error(
              "An older message is still securing its delivery slot. Your draft was kept; try Send again.",
            );
          }
          if (
            queuedSuccessorRecovery.isOrdinaryUncertain(invocationSessionId)
          ) {
            const error = new Error(QUEUED_RECOVERY_UNCERTAIN_MESSAGE);
            set({ error: error.message });
            throw error;
          }
          if (
            queuedSuccessorRecovery.claimable(invocationSessionId).length > 0
          ) {
            const record = queuedSuccessorRecovery.get(invocationSessionId);
            const claimable = queuedSuccessorRecovery.claimable(
              invocationSessionId,
            );
            if (
              record && claimable.length === 1 &&
              claimable[0].content === content &&
              JSON.stringify(claimable[0].attachments) ===
                JSON.stringify(attachments)
            ) {
              return await get().sendMessage(
                claimable[0].content,
                claimable[0].attachments,
                {
                  ...claimable[0].sendOptions,
                  queuedSuccessor: {
                    id: record.id,
                    sessionId: invocationSessionId,
                    queuedMessages: claimable,
                  },
                },
              );
            }
            resumeQueuedRecovery(invocationSessionId);
            throw new Error(
              "An older queued message is being restored before this draft.",
            );
          }
        }
        const state = get();
        const ws = workspace.getState();
        const normalizedContent = content.trim();
        const requestMessage = normalizedContent.length > 0
          ? normalizedContent
          : attachments.length > 0
          ? "Please review the attached content."
          : content;

        const createDurableWorkerChatIdentity = (
          sessionId: string,
          reserveDistinctTurn = false,
        ): NonNullable<QueuedMessage["workerInput"]> => {
          const identityRequest = {
            session_id: sessionId,
            message: requestMessage,
            content: attachments.length > 0
              ? buildContentBlocks(requestMessage, attachments)
              : undefined,
            fast_mode: state.fastModeEnabled || undefined,
            thinking_enabled: thinkingLevelToApiValue(state.thinkingLevel),
          };
          return {
            operation: "chat",
            fingerprint: workerQueuedRetryPolicyFingerprint(identityRequest),
            key: reserveDistinctTurn
              ? workerInputIdempotency.reserve(
                sessionId,
                "chat",
                workerInputFingerprint(identityRequest),
              )
              : workerInputIdempotency.keyFor(
                sessionId,
                "chat",
                workerInputFingerprint(identityRequest),
              ),
          };
        };

        const createDurableWorkerSteerIdentity = (
          sessionId: string,
        ): NonNullable<QueuedMessage["workerInput"]> => {
          const identityRequest = {
            session_id: sessionId,
            message: requestMessage,
          };
          return {
            operation: "steer",
            fingerprint: workerQueuedRetryPolicyFingerprint(identityRequest),
            key: workerInputIdempotency.keyFor(
              sessionId,
              "steer",
              workerInputFingerprint(identityRequest),
            ),
          };
        };

        const unsupportedImage = getUnsupportedImageAttachment(attachments);
        if (unsupportedImage) {
          const unsupportedError = new Error(
            unsupportedImageMimeTypeMessage(unsupportedImage.mimeType),
          );
          await releaseUndispatchedQueuedSuccessor(queuedSuccessor);
          if (isInvocationCurrent()) {
            set({
              isLoading: false,
              isStreaming: false,
              error: unsupportedImageMimeTypeMessage(unsupportedImage.mimeType),
            });
          }
          if (
            queuedSuccessor && queuedClaimToken &&
            queuedSuccessorClaims.get(queuedSuccessor.sessionId) ===
              queuedClaimToken
          ) {
            queuedSuccessorClaims.delete(queuedSuccessor.sessionId);
          }
          throw unsupportedError;
        }

        const attachmentLabel = attachments.length > 0
          ? `[Attachments: ${attachments.map((a) => a.name).join(", ")}]`
          : "";
        const displayContent = attachmentLabel
          ? normalizedContent.length > 0
            ? `${normalizedContent}\n\n${attachmentLabel}`
            : attachmentLabel
          : requestMessage;
        const displayAttachments = buildDisplayAttachments(attachments);

        if (
          !queuedSuccessor && !state.isStreaming && state.sessionId &&
          sendOptions.hiveConversationKind === "worker_dm"
        ) {
          const directSessionId = state.sessionId;
          const directId = createChatMessageId("user-worker-durable");
          const directWorkerInput = createDurableWorkerChatIdentity(
            directSessionId,
          );
          const directMessage: QueuedMessage = {
            id: directId,
            orderKey: `${Date.now().toString(36).padStart(12, "0")}:` +
              `${(++queuedInputSequence).toString(36).padStart(8, "0")}`,
            workerOperation: "chat",
            workerInput: directWorkerInput,
            canonicalUserCountBefore: state.messages.filter(
              (message) => message.role === "user",
            ).length,
            content,
            attachments,
            sendOptions,
          };
          if (!canPersistQueuedRecovery([directMessage])) {
            throw new Error(
              "Queued input is too large for crash-safe recovery. Remove an attachment and try again.",
            );
          }
          const directRow: ChatMessage = {
            id: directId,
            role: "user",
            content: displayContent,
            attachments: displayAttachments.length > 0
              ? displayAttachments
              : undefined,
            isQueued: true,
          };
          let record: QueuedSuccessorRecoveryRecord;
          beginQueuedAppend(directSessionId);
          try {
            set((current) => ({
              queuedMessages: [...current.queuedMessages, directMessage],
              messages: [...current.messages, directRow],
              error: null,
            }));
            record = await queuedSuccessorRecovery.appendPending(
              directSessionId,
              directMessage,
              directRow,
            );
            updateClaimedRowsInCache(
              directSessionId,
              (messages) => mergeClaimedRows(messages, [directRow]),
            );
          } catch (error) {
            const removeDirectRow = (messages: ChatMessage[]) =>
              messages.filter((message) => message.id !== directId);
            if (isInvocationCurrent()) {
              set((current) => ({
                queuedMessages: current.queuedMessages.filter(
                  (message) => message.id !== directId,
                ),
                messages: removeDirectRow(current.messages),
                error: toErrorMessage(error),
              }));
            }
            updateClaimedRowsInCache(directSessionId, removeDirectRow);
            throw error;
          } finally {
            endQueuedAppend(directSessionId);
          }
          return await get().sendMessage(content, attachments, {
            ...sendOptions,
            queuedSuccessor: {
              id: record.id,
              sessionId: directSessionId,
              queuedMessages: [directMessage],
            },
          });
        }

        if (
          queuedSuccessor?.queuedMessages[0]?.workerOperation === "steer"
        ) {
          const steerSessionId = queuedSuccessor.sessionId;
          const steeringRequest = {
            session_id: steerSessionId,
            message: requestMessage,
          };
          const requestFingerprint = workerInputFingerprint(steeringRequest);
          const retryPolicyFingerprint = workerQueuedRetryPolicyFingerprint(
            steeringRequest,
          );
          const recoveredIdentity = queuedSuccessor.queuedMessages[0]
            ?.workerInput ?? queuedSuccessorRecovery.get(steerSessionId)
            ?.workerInput;
          if (
            recoveredIdentity &&
            ((recoveredIdentity.operation ?? "chat") !== "steer" ||
              recoveredIdentity.fingerprint !== retryPolicyFingerprint)
          ) {
            await releaseUndispatchedQueuedSuccessor(queuedSuccessor);
            throw new Error(
              "Recovered Worker steering settings changed before delivery.",
            );
          }
          const steeringIdempotencyKey = recoveredIdentity?.key ??
            workerInputIdempotency.keyFor(
              steerSessionId,
              "steer",
              requestFingerprint,
            );
          workerInputIdempotency.restore(
            steerSessionId,
            "steer",
            requestFingerprint,
            steeringIdempotencyKey,
          );
          if (!queuedSuccessor.attemptToken) {
            throw new Error(
              "Worker steering lost its exact attempt authority before transport.",
            );
          }
          const markedInFlight = await queuedSuccessorRecovery.markInFlight(
            steerSessionId,
            queuedSuccessor.id,
            queuedSuccessor.attemptToken,
            {
              operation: "steer",
              fingerprint: retryPolicyFingerprint,
              key: steeringIdempotencyKey,
            },
          );
          if (!markedInFlight) {
            throw new Error(
              "Worker steering was superseded before transport started.",
            );
          }
          if (
            !isInvocationCurrent() ||
            queuedSuccessorRecovery.isDeletionAdmitted(steerSessionId) ||
            stopGeneration !==
              invocationStopSettlementGeneration
          ) {
            await releaseUndispatchedQueuedSuccessor(queuedSuccessor);
            throw new Error(
              "Worker steering stayed with its original conversation after navigation.",
            );
          }
          activateQueuedSuccessorClaim(queuedSuccessor);
          if (
            queuedClaimToken &&
            queuedSuccessorClaims.get(steerSessionId) === queuedClaimToken
          ) {
            queuedSuccessorClaims.delete(steerSessionId);
          }
          steeringInFlightSessions.add(steerSessionId);
          try {
            const response = await client.steerSession(
              steeringRequest,
              { idempotencyKey: steeringIdempotencyKey },
            );
            workerInputIdempotency.accept(
              steerSessionId,
              "steer",
              steeringIdempotencyKey,
            );
            const accepted = await acceptQueuedSuccessorClaim(queuedSuccessor);
            if (!accepted) {
              throw new Error(
                "Worker steering was staged after its local authority changed.",
              );
            }
            if (!isInvocationCurrent()) return;
            const sourceId = queuedSuccessor.queuedMessages[0]?.id;
            const durableId = `user-steering-${response.pending_id}`;
            set((current) => ({
              messages:
                current.messages.some((message) => message.id === durableId)
                  ? current.messages.filter((message) =>
                    message.id !== sourceId
                  )
                  : current.messages.map((message) =>
                    message.id === sourceId
                      ? {
                        ...message,
                        id: durableId,
                        isQueued: response.status === "queued" ||
                          !current.isStreaming,
                        queuedUntilNextRun: response.status === "queued" ||
                          !current.isStreaming,
                        workerStagedInputId: response.staged_input_id ??
                          undefined,
                        successorRunId: response.successor_run_id ?? undefined,
                      }
                      : message
                  ),
            }));
          } catch (error) {
            const workerLaneBlocked = isWorkerDmBlockedByNonConversationRun(
              error,
            );
            const recoverableRace = error instanceof MitsuroApiError &&
              (error.status === 404 || error.status === 409) &&
              !workerLaneBlocked;
            const definitiveClientRejection =
              error instanceof MitsuroApiError &&
              error.status >= 400 && error.status < 500 && !recoverableRace &&
              !workerLaneBlocked;
            if (recoverableRace) {
              const fallbackChatIdentity = createDurableWorkerChatIdentity(
                steerSessionId,
                true,
              );
              const pending = await queuedSuccessorRecovery
                .fallbackToPendingChat(
                  steerSessionId,
                  queuedSuccessor.id,
                  queuedSuccessor.attemptToken,
                  fallbackChatIdentity,
                );
              if (pending && isInvocationCurrent()) {
                set((current) => ({
                  queuedMessages: mergeQueuedPayloads(
                    pending.queuedMessages,
                    current.queuedMessages,
                  ),
                  messages: mergeClaimedRows(
                    current.messages,
                    pending.rows.map((message) => ({
                      ...message,
                      isQueued: true,
                    })),
                  ),
                }));
              }
              steeringInFlightSessions.delete(steerSessionId);
              workerInputIdempotency.accept(
                steerSessionId,
                "steer",
                steeringIdempotencyKey,
              );
              if (isInvocationCurrent() && !get().isStreaming) {
                resumeQueuedRecovery(steerSessionId);
              }
              return;
            }
            if (definitiveClientRejection) {
              await abandonStoppedQueuedSuccessor(queuedSuccessor);
              workerInputIdempotency.accept(
                steerSessionId,
                "steer",
                steeringIdempotencyKey,
              );
              if (isInvocationCurrent()) {
                set({
                  error: `Message was not sent. ${toErrorMessage(error)}`,
                });
              }
              throw error;
            }
            await rejectQueuedSuccessorClaim(queuedSuccessor, error);
            throw error;
          } finally {
            steeringInFlightSessions.delete(steerSessionId);
            if (
              queuedClaimToken &&
              queuedSuccessorClaims.get(steerSessionId) === queuedClaimToken
            ) {
              queuedSuccessorClaims.delete(steerSessionId);
            }
          }
          return;
        }

        if (state.isStreaming && !queuedSuccessor) {
          // Reserve chronological ownership before any steering await. If the
          // steer later falls back to the durable queue, an input that arrived
          // while it was pending must remain behind it.
          const queuedOrderKey =
            `${Date.now().toString(36).padStart(12, "0")}:` +
            `${(++queuedInputSequence).toString(36).padStart(8, "0")}`;
          const queueLocally = async (
            messageId = createChatMessageId("user-queued"),
          ) => {
            const queueSessionId = state.sessionId;
            if (!queueSessionId) {
              throw new Error(
                "Wait for this conversation to finish opening before queuing another message.",
              );
            }
            const queuedMessage: QueuedMessage = {
              id: messageId,
              orderKey: queuedOrderKey,
              workerOperation: sendOptions.hiveConversationKind === "worker_dm"
                ? "chat"
                : undefined,
              workerInput: sendOptions.hiveConversationKind === "worker_dm"
                ? createDurableWorkerChatIdentity(queueSessionId, true)
                : undefined,
              canonicalUserCountBefore: get().messages.filter(
                (message) => message.role === "user",
              ).length,
              content,
              attachments,
              sendOptions,
            };
            if (!canPersistQueuedRecovery([queuedMessage])) {
              throw new Error(
                "Queued input is too large for crash-safe recovery. Remove an attachment and try again.",
              );
            }
            const queuedRow: ChatMessage = {
              id: messageId,
              role: "user",
              content: displayContent,
              attachments: displayAttachments.length > 0
                ? displayAttachments
                : undefined,
              isQueued: true,
            };
            const queueSelectionGeneration = sessionSelectionGeneration;
            const isQueueOriginCurrent = () =>
              !disposed &&
              sessionSelectionGeneration === queueSelectionGeneration &&
              get().sessionId === queueSessionId;

            beginQueuedAppend(queueSessionId);
            try {
              if (isQueueOriginCurrent()) {
                set((s) => ({
                  queuedMessages:
                    s.queuedMessages.some((message) => message.id === messageId)
                      ? s.queuedMessages
                      : [...s.queuedMessages, queuedMessage],
                  messages:
                    s.messages.some((message) => message.id === messageId)
                      ? s.messages
                      : [...s.messages, queuedRow],
                  error: null,
                }));
              }
              await queuedSuccessorRecovery.appendPending(
                queueSessionId,
                queuedMessage,
                queuedRow,
              );
              updateClaimedRowsInCache(
                queueSessionId,
                (messages) => mergeClaimedRows(messages, [queuedRow]),
              );
            } catch (error) {
              const rollback = (messages: ChatMessage[]) =>
                messages.filter((message) => message.id !== messageId);
              if (isQueueOriginCurrent()) {
                set((s) => ({
                  queuedMessages: s.queuedMessages.filter((message) =>
                    message.id !== messageId
                  ),
                  messages: rollback(s.messages),
                  error: toErrorMessage(error),
                }));
              }
              updateClaimedRowsInCache(queueSessionId, rollback);
              throw error;
            } finally {
              endQueuedAppend(queueSessionId);
            }
          };

          if (state.sessionId) {
            const recoverable = queuedSuccessorRecovery.claimable(
              state.sessionId,
            );
            const record = queuedSuccessorRecovery.get(state.sessionId);
            const firstRecoverable = recoverable[0];
            if (
              record && recoverable.length === 1 &&
              (firstRecoverable?.workerInput?.operation ??
                  firstRecoverable?.workerOperation) === "steer" &&
              firstRecoverable.content === content &&
              JSON.stringify(firstRecoverable.attachments) ===
                JSON.stringify(attachments)
            ) {
              return await get().sendMessage(
                firstRecoverable.content,
                firstRecoverable.attachments,
                {
                  ...firstRecoverable.sendOptions,
                  queuedSuccessor: {
                    id: record.id,
                    sessionId: state.sessionId,
                    queuedMessages: recoverable,
                  },
                },
              );
            }
          }

          // Rich follow-ups can change the model contract, so they remain a
          // separate turn. Plain text can steer the active core loop without
          // waiting for it to finish first.
          if (
            !state.sessionId || attachments.length > 0 ||
            queuedSuccessorClaims.has(state.sessionId) ||
            steeringInFlightSessions.has(state.sessionId) ||
            state.queuedMessages.length > 0 ||
            queuedSuccessorRecovery.pendingIds(state.sessionId).size > 0
          ) {
            await queueLocally();
            return;
          }

          if (sendOptions.hiveConversationKind === "worker_dm") {
            const steeringSessionId = state.sessionId;
            const durableSteerId = createChatMessageId(
              "user-worker-steer-durable",
            );
            const durableSteerIdentity = createDurableWorkerSteerIdentity(
              steeringSessionId,
            );
            const durableSteerMessage: QueuedMessage = {
              id: durableSteerId,
              orderKey: queuedOrderKey,
              workerOperation: "steer",
              workerInput: durableSteerIdentity,
              canonicalUserCountBefore: state.messages.filter(
                (message) => message.role === "user",
              ).length,
              content,
              attachments,
              sendOptions,
            };
            const durableSteerRow: ChatMessage = {
              id: durableSteerId,
              role: "user",
              content: displayContent,
              isQueued: true,
            };
            beginQueuedAppend(steeringSessionId);
            try {
              set((current) => ({
                queuedMessages: [
                  ...current.queuedMessages,
                  durableSteerMessage,
                ],
                messages: [...current.messages, durableSteerRow],
                error: null,
              }));
              await queuedSuccessorRecovery.appendPending(
                steeringSessionId,
                durableSteerMessage,
                durableSteerRow,
              );
              updateClaimedRowsInCache(
                steeringSessionId,
                (messages) => mergeClaimedRows(messages, [durableSteerRow]),
              );
            } catch (error) {
              const removeSteerRow = (messages: ChatMessage[]) =>
                messages.filter((message) => message.id !== durableSteerId);
              if (get().sessionId === steeringSessionId) {
                set((current) => ({
                  queuedMessages: current.queuedMessages.filter(
                    (message) => message.id !== durableSteerId,
                  ),
                  messages: removeSteerRow(current.messages),
                  error: toErrorMessage(error),
                }));
              }
              updateClaimedRowsInCache(steeringSessionId, removeSteerRow);
              throw error;
            } finally {
              endQueuedAppend(steeringSessionId);
            }
            if (get().sessionId !== steeringSessionId) {
              throw new Error(
                "Worker input stayed with its original conversation after navigation.",
              );
            }
            if (queuedSuccessorRecovery.isDeletionAdmitted(steeringSessionId)) {
              throw new Error(
                "This conversation is being deleted; input was not sent.",
              );
            }
            const steeringRequest = {
              session_id: steeringSessionId,
              message: requestMessage,
            };
            let resumeChatFallback = false;
            steeringInFlightSessions.add(steeringSessionId);
            try {
              const response = await client.steerSession(
                steeringRequest,
                { idempotencyKey: durableSteerIdentity.key },
              );
              const accepted = await queuedSuccessorRecovery
                .acceptPendingRemote(
                  steeringSessionId,
                  durableSteerId,
                  durableSteerIdentity,
                );
              if (!accepted) {
                throw new Error(
                  "Worker steering was staged after its durable input changed.",
                );
              }
              workerInputIdempotency.accept(
                steeringSessionId,
                "steer",
                durableSteerIdentity.key,
              );
              const durableId = `user-steering-${response.pending_id}`;
              const commitResponse = (messages: ChatMessage[]) =>
                messages.some((message) => message.id === durableId)
                  ? messages.filter((message) => message.id !== durableSteerId)
                  : messages.map((message) =>
                    message.id === durableSteerId
                      ? {
                        ...message,
                        id: durableId,
                        isQueued: response.status === "queued" ||
                          !get().isStreaming,
                        queuedUntilNextRun: response.status === "queued" ||
                          !get().isStreaming,
                        workerStagedInputId: response.staged_input_id ??
                          undefined,
                        successorRunId: response.successor_run_id ?? undefined,
                      }
                      : message
                  );
              if (isInvocationCurrent()) {
                set((current) => ({
                  queuedMessages: current.queuedMessages.filter((message) =>
                    message.id !== durableSteerId
                  ),
                  messages: commitResponse(current.messages),
                }));
              }
              updateClaimedRowsInCache(steeringSessionId, commitResponse);
            } catch (error) {
              const workerLaneBlocked = isWorkerDmBlockedByNonConversationRun(
                error,
              );
              const recoverableRace = error instanceof MitsuroApiError &&
                (error.status === 404 || error.status === 409) &&
                !workerLaneBlocked;
              const definitiveClientRejection =
                error instanceof MitsuroApiError &&
                error.status >= 400 && error.status < 500 &&
                !recoverableRace && !workerLaneBlocked;
              if (recoverableRace) {
                const chatIdentity = createDurableWorkerChatIdentity(
                  steeringSessionId,
                  true,
                );
                const pending = await queuedSuccessorRecovery
                  .fallbackPendingSteerToChat(
                    steeringSessionId,
                    durableSteerId,
                    chatIdentity,
                  );
                workerInputIdempotency.accept(
                  steeringSessionId,
                  "steer",
                  durableSteerIdentity.key,
                );
                if (pending && isInvocationCurrent()) {
                  set((current) => ({
                    queuedMessages: current.queuedMessages.map((message) =>
                      message.id === durableSteerId
                        ? {
                          ...message,
                          workerOperation: "chat",
                          workerInput: chatIdentity,
                        }
                        : message
                    ),
                  }));
                }
                resumeChatFallback = Boolean(pending) && isInvocationCurrent();
              } else if (workerLaneBlocked || definitiveClientRejection) {
                await queuedSuccessorRecovery.discardPending(
                  steeringSessionId,
                  durableSteerId,
                );
                const removeSteerRow = (messages: ChatMessage[]) =>
                  messages.filter((message) => message.id !== durableSteerId);
                if (isInvocationCurrent()) {
                  set((current) => ({
                    queuedMessages: current.queuedMessages.filter((message) =>
                      message.id !== durableSteerId
                    ),
                    messages: removeSteerRow(current.messages),
                    error: `Message was not sent. ${toErrorMessage(error)}`,
                  }));
                }
                updateClaimedRowsInCache(
                  steeringSessionId,
                  removeSteerRow,
                );
                if (definitiveClientRejection) {
                  workerInputIdempotency.accept(
                    steeringSessionId,
                    "steer",
                    durableSteerIdentity.key,
                  );
                }
                throw error;
              } else {
                throw error;
              }
            } finally {
              steeringInFlightSessions.delete(steeringSessionId);
            }
            if (resumeChatFallback && !get().isStreaming) {
              // The recursive Chat retry must acquire the slot only after this
              // steer releases its lock; otherwise it rejects itself.
              resumeQueuedRecovery(steeringSessionId);
            }
            return;
          }

          const steeringSessionId = state.sessionId;
          const steeringStreamGeneration = streamAttachmentGeneration;
          const isSteeringOriginCurrent = () =>
            get().sessionId === steeringSessionId &&
            streamAttachmentGeneration === steeringStreamGeneration;

          const optimisticId = createChatMessageId("user-steering-pending");
          set((s) => ({
            messages: [
              ...s.messages,
              {
                id: optimisticId,
                role: "user" as const,
                content: displayContent,
                isQueued: true,
              },
            ],
            error: null,
          }));

          const steeringRequest = {
            session_id: steeringSessionId,
            message: requestMessage,
          };
          let steeringIdempotencyKey: string | undefined;
          const effectiveSteeringSessionType = state.sessionType ??
            (hasOwnProperty(sendOptions, "sessionType")
              ? sendOptions.sessionType
              : undefined);
          if (effectiveSteeringSessionType === "hive") {
            const conversationKind = hiveConversationKinds.get(
              steeringSessionId,
            );
            if (!conversationKind) {
              void resolveHiveConversationKind(steeringSessionId);
            }
            if (conversationKind !== "primary_hive") {
              steeringIdempotencyKey = workerInputIdempotency.keyFor(
                steeringSessionId,
                "steer",
                workerInputFingerprint(steeringRequest),
              );
            }
          }

          steeringInFlightSessions.add(steeringSessionId);
          try {
            const response = await client.steerSession(
              steeringRequest,
              steeringIdempotencyKey
                ? { idempotencyKey: steeringIdempotencyKey }
                : undefined,
            );
            if (steeringIdempotencyKey) {
              workerInputIdempotency.accept(
                steeringSessionId,
                "steer",
                steeringIdempotencyKey,
              );
            }
            if (!isSteeringOriginCurrent()) return;
            const durableId = `user-steering-${response.pending_id}`;
            set((s) => {
              const eventAlreadyRendered = s.messages.some(
                (message) => message.id === durableId,
              );
              return {
                messages: eventAlreadyRendered
                  ? s.messages.filter((message) => message.id !== optimisticId)
                  : s.messages.map((message) =>
                    message.id === optimisticId
                      ? {
                        ...message,
                        id: durableId,
                        isQueued: true,
                        queuedUntilNextRun: response.status === "queued" ||
                          !s.isStreaming,
                        workerStagedInputId: response.staged_input_id ??
                          undefined,
                        successorRunId: response.successor_run_id ?? undefined,
                      }
                      : message
                  ),
              };
            });
          } catch (error) {
            const workerLaneBlocked = isWorkerDmBlockedByNonConversationRun(
              error,
            );
            const recoverableRace = error instanceof MitsuroApiError &&
              (error.status === 404 || error.status === 409) &&
              !workerLaneBlocked;
            if (!isSteeringOriginCurrent()) {
              if (steeringIdempotencyKey) throw error;
              return;
            }
            if (workerLaneBlocked) {
              set((s) => ({
                messages: s.messages.filter(
                  (message) => message.id !== optimisticId,
                ),
                error: `Message was not sent. ${toErrorMessage(error)}`,
              }));
              throw error;
            }
            if (recoverableRace && get().isStreaming) {
              if (!isSteeringOriginCurrent()) return;
              await queueLocally(optimisticId);
              return;
            }
            if (recoverableRace) {
              set((s) => ({
                messages: s.messages.filter(
                  (message) => message.id !== optimisticId,
                ),
              }));
              if (!isSteeringOriginCurrent()) return;
              steeringInFlightSessions.delete(steeringSessionId);
              await get().sendMessage(
                content,
                attachments,
                sendOptions,
              );
              return;
            }
            set((s) => ({
              messages: s.messages.filter(
                (message) => message.id !== optimisticId,
              ),
              error: toErrorMessage(error),
            }));
            if (steeringIdempotencyKey) throw error;
          } finally {
            steeringInFlightSessions.delete(steeringSessionId);
          }
          return;
        }

        const ref: AssistantMessageRef = {
          current: createStreamingAssistantMessage(),
        };
        const optimisticUserId = createChatMessageId("user");

        set((s) => ({
          messages: [
            ...s.messages,
            ...(queuedSuccessor ? [] : [{
              id: optimisticUserId,
              role: "user" as const,
              content: displayContent,
              attachments: displayAttachments.length > 0
                ? displayAttachments
                : undefined,
            }]),
            ref.current,
          ],
          isLoading: true,
          isStreaming: true,
          error: null,
        }));

        abortController = new AbortController();
        const streamController = abortController;
        const streamGeneration = ++streamAttachmentGeneration;
        localStreamGeneration = streamGeneration;
        const streamTerminal = { current: false };
        const isStreamGenerationCurrent = () =>
          streamGeneration === streamAttachmentGeneration;
        const isStreamAttached = () =>
          isStreamGenerationCurrent() && !streamTerminal.current;
        const finishConnectSpan = beginMitsuroPerformanceSpan(
          "stream.connect",
          state.sessionId ?? undefined,
        );
        const releaseStreamConnection = trackMitsuroPerformanceResource(
          "stream_connections",
        );

        const pollingSessionId = state.sessionId;
        const explicitHiveConversationKind = sendOptions.hiveConversationKind;
        const effectiveHiveConversationKind = explicitHiveConversationKind ??
          (pollingSessionId
            ? hiveConversationKinds.get(pollingSessionId)
            : undefined);
        if (
          pollingSessionId &&
          effectiveHiveConversationKind &&
          (state.sessionType === "hive" || sendOptions.sessionType === "hive")
        ) {
          hiveConversationKinds.set(
            pollingSessionId,
            effectiveHiveConversationKind,
          );
          activeHiveStreamOwner = {
            sessionId: pollingSessionId,
            generation: streamGeneration,
            kind: effectiveHiveConversationKind,
          };
        } else {
          activeHiveStreamOwner = null;
        }
        if (pollingSessionId) {
          get().startStatePolling(pollingSessionId);
        }

        const streamRecovery: { promise: Promise<boolean> | null } = {
          promise: null,
        };
        let chatIdempotencyKey: string | undefined;
        let queuedWorkerIdentity: QueuedWorkerInputIdentity | undefined;
        let transportStarted = false;
        let workerRequestAccepted = false;
        let workerLaneBlockedError: Error | null = null;
        let terminalStreamError: Error | null = null;
        let queuedSuccessorAcceptance: Promise<boolean> | null = null;
        let streamOwnedSessionId = pollingSessionId;
        const guardedCallbacks = guardStreamCallbacks(
          attachLiveStreamLifecycle(
            createRecoveringStreamCallbacks(
              createStreamCallbacks(ref, set, get, {
                planStore,
                sessionsStore,
                persistSessionMode: persistMode,
                isActive: isStreamAttached,
                isWorkerResponseExpected: () => Boolean(chatIdempotencyKey),
                isSessionCurrent: (sessionId) =>
                  isStreamGenerationCurrent() && get().sessionId === sessionId,
                expectedSessionId: pollingSessionId,
                onSessionOwnershipChange: (sessionId) => {
                  streamOwnedSessionId = sessionId;
                },
                onFirstEvent: finishConnectSpan,
                onDelegationEvent: () => {
                  if (pollingSessionId) {
                    get().refreshDelegationState(pollingSessionId);
                  }
                },
                onWorkerResponseBoundaryChange: (boundary) => {
                  activeWorkerResponseBoundary = boundary;
                  if (boundary) {
                    if (boundary.sessionId !== streamOwnedSessionId) return;
                    hiveConversationKinds.set(boundary.sessionId, "worker_dm");
                    if (
                      boundary.sessionId === pollingSessionId &&
                      streamGeneration === streamAttachmentGeneration
                    ) {
                      activeHiveStreamOwner = {
                        sessionId: boundary.sessionId,
                        generation: streamGeneration,
                        kind: "worker_dm",
                      };
                    }
                    if (chatIdempotencyKey) {
                      workerInputIdempotency.accept(
                        boundary.sessionId,
                        "chat",
                        chatIdempotencyKey,
                      );
                      workerRequestAccepted = true;
                    }
                    queuedSuccessorAcceptance ??= observeQueuedAcceptance(
                      acceptQueuedSuccessorClaim(queuedSuccessor),
                    );
                  }
                },
                deferCanonicalReload: (sessionId) => {
                  deferredCanonicalReloads.add(sessionId);
                },
                consumeCanonicalReload: (sessionId) =>
                  deferredCanonicalReloads.delete(sessionId),
              }),
              pollingSessionId,
              streamRecovery,
            ),
            streamGeneration,
          ),
          isStreamAttached,
          streamTerminal,
        );
        const callbacks: StreamCallbacks = {
          ...guardedCallbacks,
          onFinish: (sessionId, stopReason) => {
            if (sessionId !== streamOwnedSessionId) {
              terminalStreamError = new Error(
                "The stream finished for a different conversation.",
              );
              guardedCallbacks.onError(terminalStreamError.message);
              return;
            }
            if (isStreamAttached()) {
              if (chatIdempotencyKey && !workerRequestAccepted) {
                terminalStreamError = new Error(
                  "The Worker response finished without its exact acceptance boundary.",
                );
              } else if (!chatIdempotencyKey) {
                queuedSuccessorAcceptance ??= observeQueuedAcceptance(
                  acceptQueuedSuccessorClaim(queuedSuccessor),
                );
              }
            }
            guardedCallbacks.onFinish(sessionId, stopReason);
          },
          onError: (streamError) => {
            if (!isStreamAttached()) return;
            terminalStreamError = new Error(streamError);
            if (
              chatIdempotencyKey &&
              isWorkerDmBlockedByNonConversationRunMessage(streamError)
            ) {
              workerLaneBlockedError = new Error(streamError);
              activeWorkerResponseBoundary = null;
              set((s) => ({
                isLoading: false,
                isStreaming: false,
                isThinking: false,
                thinkingContent: "",
                messages: s.messages.filter(
                  (message) =>
                    message.id !== optimisticUserId &&
                    message.id !== ref.current.id,
                ),
                error: `Message was not sent. ${streamError}`,
              }));
              streamTerminal.current = true;
              if (localStreamGeneration === streamGeneration) {
                localStreamGeneration = null;
              }
              if (activeHiveStreamOwner?.generation === streamGeneration) {
                activeHiveStreamOwner = null;
              }
              return;
            }
            guardedCallbacks.onError(streamError);
          },
        };
        let keepStatePolling = false;

        try {
          const contentBlocks = attachments.length > 0
            ? buildContentBlocks(requestMessage, attachments)
            : undefined;
          const isNewSessionRequest = !state.sessionId;
          const sendOptionHasProjectDir = sendOptions
            ? hasOwnProperty(sendOptions, "projectDir")
            : false;
          const sendOptionHasWorkingDir = sendOptions
            ? hasOwnProperty(sendOptions, "workingDir")
            : false;
          const sendOptionHasWorkspaceMode = sendOptions
            ? hasOwnProperty(sendOptions, "workspaceMode")
            : false;
          const sendOptionHasSessionType = sendOptions
            ? hasOwnProperty(sendOptions, "sessionType")
            : false;
          const sendOptionHasTargetBranch = sendOptions
            ? hasOwnProperty(sendOptions, "targetBranch")
            : false;
          const requestedProjectDir = isNewSessionRequest
            ? sendOptionHasProjectDir
              ? sendOptions?.projectDir ?? null
              : ws.directory
            : undefined;
          const requestedWorkingDir = isNewSessionRequest
            ? sendOptionHasWorkingDir
              ? sendOptions?.workingDir ?? null
              : sendOptionHasProjectDir
              ? requestedProjectDir
              : ws.directory
            : undefined;
          const requestedWorkspaceMode = isNewSessionRequest
            ? sendOptionHasWorkspaceMode ? sendOptions?.workspaceMode : ws.mode
            : undefined;
          const requestedSessionType = isNewSessionRequest
            ? sendOptionHasSessionType ? sendOptions?.sessionType : undefined
            : undefined;
          const effectiveRequestedSessionType = sendOptionHasSessionType
            ? sendOptions?.sessionType
            : undefined;
          const effectiveSessionType = state.sessionType ??
            effectiveRequestedSessionType ?? "code";
          const requestedTargetBranch = isNewSessionRequest
            ? normalizeTargetBranch(
              sendOptionHasTargetBranch
                ? sendOptions?.targetBranch
                : ws.targetBranch,
            )
            : undefined;

          const chatRequest = {
            session_id: state.sessionId ?? undefined,
            message: requestMessage,
            content: contentBlocks,
            project_dir: requestedProjectDir,
            working_dir: requestedWorkingDir,
            workspace_mode: requestedWorkspaceMode,
            session_type: requestedSessionType,
            target_branch: sendOptionHasTargetBranch || requestedTargetBranch
              ? requestedTargetBranch
              : undefined,
            model: effectiveSessionType === "hive"
              ? undefined
              : state.model ?? undefined,
            model_key: effectiveSessionType === "hive"
              ? undefined
              : state.modelKey ?? undefined,
            fast_mode: state.fastModeEnabled || undefined,
            thinking_enabled: thinkingLevelToApiValue(state.thinkingLevel),
            permission_mode: effectiveSessionType === "hive"
              ? undefined
              : state.permissionMode,
            mode: effectiveSessionType === "code" ? state.mode : undefined,
          };

          if (state.sessionId && effectiveSessionType === "hive") {
            const conversationKind = hiveConversationKinds.get(state.sessionId);
            if (!conversationKind) {
              void resolveHiveConversationKind(state.sessionId);
            }
            if (conversationKind !== "primary_hive") {
              const fingerprint = workerInputFingerprint(chatRequest);
              const retryPolicyFingerprint = workerQueuedRetryPolicyFingerprint(
                chatRequest,
              );
              const recoveredIdentity = queuedSuccessor
                ? queuedSuccessor.queuedMessages[0]?.workerInput ??
                  queuedSuccessorRecovery.get(state.sessionId)?.workerInput
                : undefined;
              if (recoveredIdentity) {
                if (
                  (recoveredIdentity.operation ?? "chat") !== "chat" ||
                  recoveredIdentity.fingerprint !== retryPolicyFingerprint
                ) {
                  throw new Error(
                    "Queued Worker retry settings changed before delivery. Restore the prior settings or resend the recovered draft explicitly.",
                  );
                }
                workerInputIdempotency.restore(
                  state.sessionId,
                  "chat",
                  fingerprint,
                  recoveredIdentity.key,
                );
                queuedWorkerIdentity = recoveredIdentity;
              }
              chatIdempotencyKey = recoveredIdentity?.key ??
                workerInputIdempotency.keyFor(
                  state.sessionId,
                  "chat",
                  fingerprint,
                );
              queuedWorkerIdentity ??= {
                operation: "chat",
                fingerprint: retryPolicyFingerprint,
                key: chatIdempotencyKey,
              };
            }
          }

          if (queuedSuccessor) {
            if (!queuedSuccessor.attemptToken) {
              throw new Error(
                "Queued delivery lost its exact attempt authority before transport.",
              );
            }
            const markedInFlight = await queuedSuccessorRecovery.markInFlight(
              queuedSuccessor.sessionId,
              queuedSuccessor.id,
              queuedSuccessor.attemptToken,
              queuedWorkerIdentity,
            );
            if (!markedInFlight) {
              throw new Error(
                "Queued delivery was superseded before transport started.",
              );
            }
            if (
              disposed || !isStreamGenerationCurrent() ||
              get().sessionId !== queuedSuccessor.sessionId ||
              queuedSuccessorRecovery.isDeletionAdmitted(
                queuedSuccessor.sessionId,
              ) ||
              stopGeneration !==
                invocationStopSettlementGeneration
            ) {
              await releaseUndispatchedQueuedSuccessor(queuedSuccessor);
              throw new Error(
                "Queued input stayed with its original conversation after navigation.",
              );
            }
          }
          activateQueuedSuccessorClaim(queuedSuccessor);
          if (
            queuedSuccessor && queuedClaimToken &&
            queuedSuccessorClaims.get(queuedSuccessor.sessionId) ===
              queuedClaimToken
          ) {
            queuedSuccessorClaims.delete(queuedSuccessor.sessionId);
          }

          transportStarted = true;
          await client.streamChat(
            chatRequest,
            callbacks,
            streamController.signal,
            chatIdempotencyKey
              ? { idempotencyKey: chatIdempotencyKey }
              : undefined,
          );

          if (
            chatIdempotencyKey && !workerRequestAccepted &&
            !terminalStreamError
          ) {
            terminalStreamError = !isStreamGenerationCurrent()
              ? new Error(
                "The Worker send detached before remote acceptance was confirmed.",
              )
              : new Error(
                "The Worker transport ended without its exact acceptance boundary.",
              );
          }

          if (queuedSuccessorAcceptance) {
            try {
              const persisted = await queuedSuccessorAcceptance;
              if (!persisted && workerRequestAccepted && queuedSuccessor) {
                commitAcceptedQueuedRows(queuedSuccessor);
              }
            } catch (error) {
              if (!workerRequestAccepted || !queuedSuccessor) throw error;
              // The remote Worker boundary is authoritative for delivery. A
              // local recovery write failure must not reject the accepted turn
              // and restore it under a fresh idempotency key. The durable
              // record retains its original key for safe cleanup/replay.
              commitAcceptedQueuedRows(queuedSuccessor);
            }
          }

          if (
            !isStreamGenerationCurrent() &&
            stoppedStreamGenerations.has(streamGeneration)
          ) {
            await abandonStoppedQueuedSuccessor(queuedSuccessor);
            return;
          }

          const resolvedWorkerLaneError = workerLaneBlockedError as
            | Error
            | null;
          if (resolvedWorkerLaneError) {
            await abandonStoppedQueuedSuccessor(queuedSuccessor);
            if (isStreamGenerationCurrent()) {
              set({
                error:
                  `Message was not sent. ${resolvedWorkerLaneError.message}`,
              });
            }
            throw resolvedWorkerLaneError;
          }
          if (terminalStreamError && streamRecovery.promise) {
            keepStatePolling = await streamRecovery.promise;
          }
          if (terminalStreamError && !workerRequestAccepted) {
            await rejectQueuedSuccessorClaim(
              queuedSuccessor,
              terminalStreamError,
            );
            if (chatIdempotencyKey) {
              throw terminalStreamError;
            }
            if (queuedSuccessor || !keepStatePolling) return;
          }
          if (!isStreamGenerationCurrent()) {
            if (stoppedStreamGenerations.has(streamGeneration)) return;
            const detachedError = new Error(
              "The Worker send detached before remote acceptance was confirmed.",
            );
            if (!workerRequestAccepted) {
              await rejectQueuedSuccessorClaim(queuedSuccessor, detachedError);
            }
            if (chatIdempotencyKey && !workerRequestAccepted) {
              throw detachedError;
            }
            return;
          }
          if (
            !terminalStreamError && !queuedSuccessorAcceptance &&
            !chatIdempotencyKey
          ) {
            await acceptQueuedSuccessorClaim(queuedSuccessor);
          }

          const completedSessionId = get().sessionId;
          if (isNewSessionRequest && completedSessionId) {
            const nextDirectory = requestedProjectDir ?? requestedWorkingDir ??
              null;
            workspace.getState().setWorkspace(
              nextDirectory,
              completedSessionId,
              requestedWorkspaceMode ??
                (nextDirectory ? "selected" : "neutral"),
              requestedTargetBranch ?? null,
            );
          }
        } catch (err) {
          if (
            transportStarted && !isStreamGenerationCurrent() &&
            stoppedStreamGenerations.has(streamGeneration)
          ) {
            await abandonStoppedQueuedSuccessor(queuedSuccessor);
            return;
          }
          if (queuedSuccessor && !transportStarted) {
            await releaseUndispatchedQueuedSuccessor(queuedSuccessor);
            updateClaimedRowsInCache(
              queuedSuccessor.sessionId,
              (messages) =>
                messages.filter((message) => message.id !== ref.current.id),
            );
            if (
              !disposed && isStreamGenerationCurrent() &&
              get().sessionId === queuedSuccessor.sessionId
            ) {
              set((current) => ({
                messages: current.messages.filter((message) =>
                  message.id !== ref.current.id
                ),
                isLoading: false,
                isStreaming: false,
                isThinking: false,
                thinkingContent: "",
                error: toErrorMessage(err),
              }));
            }
            throw err;
          }
          const resolvedWorkerLaneError = workerLaneBlockedError as
            | Error
            | null;
          if (resolvedWorkerLaneError) {
            await abandonStoppedQueuedSuccessor(queuedSuccessor);
            if (isStreamGenerationCurrent()) {
              set({
                error:
                  `Message was not sent. ${resolvedWorkerLaneError.message}`,
              });
            }
            throw resolvedWorkerLaneError;
          }
          if (!isStreamGenerationCurrent()) {
            if (stoppedStreamGenerations.has(streamGeneration)) return;
            if (!workerRequestAccepted) {
              await rejectQueuedSuccessorClaim(queuedSuccessor, err);
            }
            if (chatIdempotencyKey && !workerRequestAccepted) throw err;
            return;
          }
          if (pollingSessionId) {
            streamRecovery.promise ??= recoverAfterStreamInterruption(
              pollingSessionId,
            );
            keepStatePolling = await streamRecovery.promise;
          }
          if (!isStreamGenerationCurrent()) {
            if (stoppedStreamGenerations.has(streamGeneration)) return;
            if (!workerRequestAccepted) {
              await rejectQueuedSuccessorClaim(queuedSuccessor, err);
            }
            if (chatIdempotencyKey && !workerRequestAccepted) throw err;
            return;
          }
          if (!keepStatePolling) {
            await rejectQueuedSuccessorClaim(queuedSuccessor, err);
            if (isStreamGenerationCurrent()) applyStreamFailure(err);
            if (chatIdempotencyKey) {
              // The dedicated Worker composer clears optimistically and restores
              // its exact-session draft only when the send rejects. A definite,
              // unrecovered provider failure must not look accepted locally.
              throw err;
            }
          }
          if (chatIdempotencyKey && !workerRequestAccepted) {
            await rejectQueuedSuccessorClaim(queuedSuccessor, err);
            throw err;
          }
          if (queuedSuccessor && !workerRequestAccepted) {
            await rejectQueuedSuccessorClaim(queuedSuccessor, err);
          }
        } finally {
          finishConnectSpan();
          releaseStreamConnection();
          if (localStreamGeneration === streamGeneration) {
            localStreamGeneration = null;
          }
          if (activeHiveStreamOwner?.generation === streamGeneration) {
            activeHiveStreamOwner = null;
          }
          if (
            queuedSuccessor && queuedClaimToken &&
            queuedSuccessorClaims.get(queuedSuccessor.sessionId) ===
              queuedClaimToken
          ) {
            queuedSuccessorClaims.delete(queuedSuccessor.sessionId);
          }
          if (streamRecovery.promise) {
            keepStatePolling = await streamRecovery.promise;
          }
          // Terminal callbacks deliberately detach presentation events. Cleanup
          // still belongs to this exact generation until a successor starts.
          if (isStreamGenerationCurrent() && !keepStatePolling) {
            get().stopStatePolling();
          }
          stoppedStreamGenerations.delete(streamGeneration);
        }
      },

      async retryQueuedRecovery() {
        const sessionId = get().sessionId;
        const selectionGeneration = sessionSelectionGeneration;
        if (!sessionId) return;
        const record = await queuedSuccessorRecovery.retryOrdinaryUncertain(
          sessionId,
        );
        if (
          disposed || selectionGeneration !== sessionSelectionGeneration ||
          get().sessionId !== sessionId || !record
        ) return;
        const recovery = queuedRecoveryForSession(sessionId);
        set((state) => ({
          messages: recovery.messages(state.messages),
          queuedMessages: mergeQueuedPayloads(
            recovery.queuedMessages,
            state.queuedMessages,
          ),
          queuedRecoveryBlocked: false,
          error: null,
        }));
        resumeQueuedRecovery(sessionId);
      },

      async discardQueuedRecovery(targetSessionId?: string) {
        const sessionId = targetSessionId ?? get().sessionId;
        if (!sessionId) return;
        const discarded = await queuedSuccessorRecovery.delete(sessionId);
        if (!discarded) return;
        const discardedIds = new Set(
          discarded.queuedMessages.map((message) => message.id),
        );
        const removeDiscardedRows = (messages: ChatMessage[]) =>
          messages.filter((message) => !discardedIds.has(message.id));
        updateClaimedRowsInCache(sessionId, removeDiscardedRows);
        if (!disposed && get().sessionId === sessionId) {
          set((state) => ({
            messages: removeDiscardedRows(state.messages),
            queuedMessages: state.queuedMessages.filter((message) =>
              !discardedIds.has(message.id)
            ),
            queuedRecoveryBlocked: false,
            error: null,
          }));
        }
      },

      beginSessionDeletionAdmission(
        sessionId: string,
      ): Promise<SessionDeletionAdmission> {
        if (!sessionId.trim()) {
          return Promise.reject(
            new Error("A conversation is required before deletion."),
          );
        }

        const existingAdmission = sessionDeletionAdmissions.get(sessionId);
        if (existingAdmission) {
          if (
            existingAdmission.phase === "rollback_failed" &&
            queuedSuccessorRecovery.canRepairFailedDeletionAdmission(sessionId)
          ) {
            // Transfer the failed local lease to a fresh repair acquisition.
            // The old lease observes `repairing` and cannot settle concurrently.
            existingAdmission.phase = "repairing";
            sessionDeletionAdmissions.delete(sessionId);
            return get().beginSessionDeletionAdmission(sessionId);
          }
          if (
            existingAdmission.phase === "rollback_failed" &&
            !queuedSuccessorRecovery.isDeletionAdmitted(sessionId)
          ) {
            // A reentrant replacement store can win shared rollback repair
            // after this store observed it as repairable but before it claimed
            // the marker. Once that winner settles, discard this loser's stale
            // local bookkeeping so it cannot block a genuinely fresh DELETE.
            sessionDeletionAdmissions.delete(sessionId);
            return get().beginSessionDeletionAdmission(sessionId);
          }
          return Promise.reject(
            new Error("This conversation already has a deletion in progress."),
          );
        }

        const renewFailedAdmission = queuedSuccessorRecovery
          .isDeletionAdmitted(sessionId);
        if (
          renewFailedAdmission &&
          !queuedSuccessorRecovery.canRepairFailedDeletionAdmission(sessionId)
        ) {
          return Promise.reject(
            new Error("This conversation already has a deletion in progress."),
          );
        }
        if (!renewFailedAdmission) {
          // This remains before any promise is created. It is the admission edge
          // that prevents a newer send from entering while an older recovery
          // mutation drains into the scrub below.
          queuedSuccessorRecovery.acquireDeletionAdmission(sessionId);
        }
        queuedSuccessorClaims.delete(sessionId);
        steeringInFlightSessions.delete(sessionId);

        const isCurrentSession = get().sessionId === sessionId;
        const locallyQueuedIds = new Set(
          isCurrentSession
            ? get().queuedMessages.map((message) => message.id)
            : [],
        );
        if (isCurrentSession) {
          sessionSelectionGeneration += 1;
          streamAttachmentGeneration += 1;
          abortController?.abort();
          abortController = null;
          localStreamGeneration = null;
          activeHiveStreamOwner = null;
          activeWorkerResponseBoundary = null;
          pendingStoppedWorkerResponse = null;
          activeWorkerStopSettlement = null;
          stopGeneration += 1;
          deferredCanonicalReloads.delete(sessionId);
          get().stopStatePolling();
          stopPresenceTransport(sessionId);
          set((state) => ({
            isLoading: false,
            isStreaming: false,
            isThinking: false,
            thinkingContent: "",
            messages: discardTransientAssistantMessages(state.messages),
          }));
        }

        const admissionState: SessionDeletionAdmissionState = {
          phase: renewFailedAdmission ? "repairing" : "opening",
        };
        const admissionPromise = (async (): Promise<
          SessionDeletionAdmission
        > => {
          let snapshot: QueuedSuccessorRecoveryRecord | null;
          try {
            snapshot = renewFailedAdmission
              ? await queuedSuccessorRecovery.renewFailedDeletionAdmission(
                sessionId,
              )
              : await queuedSuccessorRecovery.scrubForDeletion(sessionId);
          } catch (error) {
            if (renewFailedAdmission) {
              admissionState.phase = "rollback_failed";
            } else {
              queuedSuccessorRecovery.releaseDeletionAdmission(sessionId);
              sessionDeletionAdmissions.delete(sessionId);
            }
            throw error;
          }
          admissionState.phase = "open";

          const scrubbedIds = new Set(
            [
              ...(snapshot?.queuedMessages.map((message) => message.id) ?? []),
              ...locallyQueuedIds,
            ],
          );
          const removeScrubbedRows = (messages: ChatMessage[]) =>
            messages.filter((message) => !scrubbedIds.has(message.id));
          if (scrubbedIds.size > 0) {
            updateClaimedRowsInCache(sessionId, removeScrubbedRows);
            if (!disposed && get().sessionId === sessionId) {
              set((state) => ({
                messages: removeScrubbedRows(state.messages),
                queuedMessages: state.queuedMessages.filter((message) =>
                  !scrubbedIds.has(message.id)
                ),
                queuedRecoveryBlocked: false,
                error: null,
              }));
            }
          }

          let settled = false;
          let rollbackInFlight: Promise<void> | null = null;

          const commitDeletion = () => {
            workerInputIdempotency.discardSession(sessionId);
            sessionCache.delete(sessionId);
            lastKnownServerState.delete(sessionId);
            queuedSuccessorRecovery.commitDeletionAdmission(sessionId);
            sessionDeletionAdmissions.delete(sessionId);
          };

          const restorePresentationAfterRollback = () => {
            if (!disposed && get().sessionId === sessionId) {
              const recovery = queuedRecoveryForSession(sessionId);
              set((state) => ({
                messages: recovery.messages(state.messages),
                queuedMessages: mergeQueuedPayloads(
                  recovery.queuedMessages,
                  state.queuedMessages,
                ),
                queuedRecoveryBlocked: recovery.blocked,
                error: recovery.blocked
                  ? QUEUED_RECOVERY_UNCERTAIN_MESSAGE
                  : null,
              }));
            }
          };

          const rollback = (): Promise<void> => {
            if (settled) return Promise.resolve();
            if (rollbackInFlight) return rollbackInFlight;
            if (
              admissionState.phase === "repairing" ||
              (admissionState.phase === "rollback_failed" &&
                !queuedSuccessorRecovery.canRepairFailedDeletionAdmission(
                  sessionId,
                ))
            ) {
              return Promise.reject(
                new Error(
                  "Deletion rollback repair is already owned by another store.",
                ),
              );
            }
            admissionState.phase = "rolling_back";
            rollbackInFlight = (async () => {
              try {
                await queuedSuccessorRecovery.rollbackDeletionAdmission(
                  sessionId,
                  snapshot,
                );
                settled = true;
                sessionDeletionAdmissions.delete(sessionId);
                restorePresentationAfterRollback();
              } catch (error) {
                // The scrub remains admitted. The same owner can retry, while
                // a later begin repairs this rollback before acquiring a fresh
                // lease for a new DELETE transport.
                rollbackInFlight = null;
                admissionState.phase = "rollback_failed";
                throw error;
              }
            })();
            return rollbackInFlight;
          };

          return {
            commit() {
              if (
                settled || rollbackInFlight ||
                admissionState.phase !== "open"
              ) return;
              settled = true;
              commitDeletion();
            },
            rollback,
          };
        })();
        sessionDeletionAdmissions.set(sessionId, admissionState);
        return admissionPromise;
      },

      // -- ensureHiveMainSession ----------------------------------------------

      async ensureHiveMainSession() {
        const originSessionId = get().sessionId;
        const originSelectionGeneration = sessionSelectionGeneration;
        const isEnsureOriginCurrent = () =>
          sessionSelectionGeneration === originSelectionGeneration &&
          get().sessionId === originSessionId;
        let resolvedMainId: string | null = null;
        try {
          const main = await client.ensureHiveMain();
          if (!isEnsureOriginCurrent()) return null;
          const mainId = main.session_id?.trim();
          if (!mainId) {
            set({
              error: "Hive companion session is unavailable.",
              isLoading: false,
            });
            return null;
          }
          resolvedMainId = mainId;
          hiveConversationKinds.set(mainId, "primary_hive");

          if (get().sessionId === mainId && get().sessionType === "hive") {
            // Already on companion — soft refresh without interrupting a stream.
            if (!get().isStreaming) {
              await get().loadSession(mainId, true);
            }
            return get().sessionId === mainId ? mainId : null;
          }

          if (get().isStreaming) {
            get().stopStreaming();
          }
          await get().loadSession(mainId);
          if (get().sessionId !== mainId) return null;
          // Guarantee sessionType is hive even if list metadata lags.
          if (get().sessionId === mainId && get().sessionType !== "hive") {
            set({ sessionType: "hive" });
          }
          return mainId;
        } catch (err) {
          const currentSessionId = get().sessionId;
          if (
            currentSessionId !== originSessionId &&
            currentSessionId !== resolvedMainId
          ) {
            return null;
          }
          set({
            isLoading: false,
            error: toErrorMessage(err, "Failed to open Hive companion"),
          });
          return null;
        }
      },

      // -- loadSession --------------------------------------------------------

      async loadSession(sessionId: string, isRefresh = false) {
        const isNewSelectionIntent = get().sessionId !== sessionId;
        if (isNewSelectionIntent) {
          workerInputIdempotency.transitionTo(sessionId);
        }
        const selectionGeneration = isNewSelectionIntent
          ? ++sessionSelectionGeneration
          : sessionSelectionGeneration;
        const existing = inFlightSessionLoads.get(sessionId);
        if (existing && !isNewSelectionIntent) {
          return existing;
        }
        const finishOpenSpan = beginMitsuroPerformanceSpan(
          "session.open",
          sessionId,
        );
        const releaseRequest = trackMitsuroPerformanceResource(
          "session_requests",
        );

        const run = (async () => {
          const previousSessionId = get().sessionId;
          const isSessionSwitch = previousSessionId !== sessionId;
          const listedSessions = sessionsStore.getState().sessions ?? [];

          if (isSessionSwitch) {
            // Detach any in-flight stream callbacks for the leaving session without
            // cancelling its server-side run.
            streamAttachmentGeneration += 1;
            abortController?.abort();
            abortController = null;
            localStreamGeneration = null;
            activeHiveStreamOwner = null;
            activeWorkerResponseBoundary = null;
            pendingStoppedWorkerResponse = null;
            activeWorkerStopSettlement = null;
            stopGeneration += 1;
            get().stopStatePolling();
            stopPresenceTransport(previousSessionId);
            planStore.getState().setWorkflow(null);

            // Keep the leaving session warm so back-navigation can paint instantly.
            if (previousSessionId) {
              const previous = get();
              if (
                previous.sessionId === previousSessionId &&
                previous.messages.length > 0
              ) {
                const previousListItem = listedSessions.find((session) =>
                  session.id === previousSessionId
                ) ?? null;
                sessionCache.set({
                  sessionId: previousSessionId,
                  sessionType: previous.sessionType,
                  title: previous.title,
                  mode: previous.mode,
                  permissionMode: previous.permissionMode,
                  model: previous.model,
                  modelKey: previous.modelKey,
                  tokenCount: previous.tokenCount,
                  messages: previous.messages,
                  projectDir: previousListItem?.project_dir ??
                    workspace.getState().directory ??
                    null,
                  workingDir: previousListItem?.working_dir ??
                    workspace.getState().directory ??
                    null,
                  workspaceMode: previousListItem?.workspace_mode ??
                    workspace.getState().mode ??
                    null,
                  targetBranch: previousListItem?.target_branch ??
                    workspace.getState().targetBranch ??
                    null,
                  serverState: lastKnownServerState.get(previousSessionId) ??
                    null,
                  updatedAt: Date.now(),
                });
              }
            }
          }

          const listItem = listedSessions.find((session) =>
            session.id === sessionId
          ) ?? null;
          const cached = sessionCache.get(sessionId);
          const optimistic = buildOptimisticSessionShell(
            sessionId,
            listItem,
            cached,
          );
          const optimisticQueuedRecovery = queuedRecoveryForSession(sessionId);
          const optimisticMessages = optimisticQueuedRecovery.messages(
            optimistic.messages,
          );
          const hasCachedMessages = optimisticMessages.length > 0;
          const cachedServerState = optimistic.serverState ??
            lastKnownServerState.get(sessionId) ??
            null;
          const cachedIsStreaming = isActiveSessionAgentState(
            cachedServerState?.agent_state,
          );

          if (isSessionSwitch) {
            // Activate destination shell immediately so navigation never waits on network.
            const current = get();
            const sameExactSelection = Boolean(optimistic.modelKey) &&
              modelKeysEqual(optimistic.modelKey, current.modelKey);
            const nextModelProvider = optimistic.model
              ? optimistic.modelKey?.provider ??
                (optimistic.model === current.model
                  ? current.modelProvider
                  : null)
              : current.modelProvider;
            const nextModelInfo = optimistic.model
              ? sameExactSelection ||
                  (!optimistic.modelKey && optimistic.model === current.model)
                ? current.modelInfo
                : null
              : current.modelInfo;
            const capabilityInput = nextModelInfo ?? optimistic.model ??
              current.model;
            const nextThinkingLevel = normalizeThinkingLevel(
              current.thinkingLevel,
              capabilityInput,
            );
            const directory = optimistic.projectDir ?? optimistic.workingDir ??
              null;
            const workspaceMode = (optimistic.workspaceMode ??
              (directory ? "selected" : "neutral")) as
                | "neutral"
                | "selected"
                | "created";

            set({
              sessionId: optimistic.sessionId,
              sessionType: optimistic.sessionType,
              title: optimistic.title,
              mode: optimistic.mode,
              permissionMode: optimistic.permissionMode,
              model: optimistic.model ?? current.model,
              modelKey: optimistic.model
                ? optimistic.modelKey
                : current.modelKey,
              modelProvider: nextModelProvider,
              modelInfo: nextModelInfo,
              thinkingLevel: nextThinkingLevel,
              thinkingEnabled: isThinkingEnabled(nextThinkingLevel),
              fastModeEnabled: optimistic.model
                ? current.fastModeEnabled &&
                  supportsFastMode(capabilityInput, nextModelProvider)
                : current.fastModeEnabled,
              tokenCount: optimistic.tokenCount,
              tokenUsage: null,
              messages: optimisticMessages,
              queuedMessages: optimisticQueuedRecovery.queuedMessages,
              queuedRecoveryBlocked: optimisticQueuedRecovery.blocked,
              isLoading: !hasCachedMessages,
              isStreaming: cachedIsStreaming,
              isThinking: Boolean(
                cachedServerState?.live_partial_assistant?.thinking?.trim(),
              ),
              thinkingContent:
                cachedServerState?.live_partial_assistant?.thinking || "",
              lastEventSequence: cachedServerState?.last_event_sequence ?? null,
              delegationEventCursor:
                cachedServerState?.delegation_event_cursor ?? null,
              error: optimisticQueuedRecovery.blocked
                ? QUEUED_RECOVERY_UNCERTAIN_MESSAGE
                : null,
            });
            try {
              storage.set("mitsuro-permission-mode", optimistic.permissionMode);
            } catch {
              /* ignore */
            }
            // Keep plan chrome collapsed by default; server snapshot can expand it.
            planStore.getState().setVisible(false);
            if (cachedServerState?.workflow) {
              planStore.getState().setWorkflow(cachedServerState.workflow);
            }
            workspace
              .getState()
              .initFromSession(
                optimistic.sessionId,
                directory,
                workspaceMode,
                optimistic.targetBranch,
              );
            startPresenceTransport(optimistic.sessionId);
            if (
              cachedIsStreaming ||
              hasActiveDelegationGroups(cachedServerState?.delegation_groups)
            ) {
              get().startStatePolling(optimistic.sessionId);
            }
          } else if (!get().messages.length) {
            set({ isLoading: true, error: null });
          }

          void queuedSuccessorRecovery.ready().then(() => {
            if (
              selectionGeneration !== sessionSelectionGeneration ||
              get().sessionId !== sessionId
            ) return;
            const recovery = queuedRecoveryForSession(sessionId);
            set((state) => ({
              messages: recovery.messages(state.messages),
              queuedMessages: mergeQueuedPayloads(
                recovery.queuedMessages,
                state.queuedMessages,
              ),
              queuedRecoveryBlocked: recovery.blocked,
              error: recovery.blocked
                ? QUEUED_RECOVERY_UNCERTAIN_MESSAGE
                : state.error,
            }));
          }).catch(() => {
            if (
              selectionGeneration === sessionSelectionGeneration &&
              get().sessionId === sessionId
            ) {
              set({
                error:
                  "Queued draft recovery is temporarily unavailable. Your current draft was not sent.",
              });
            }
          });

          try {
            let prefetchedServerState: ApiSessionStateResponse | null = null;
            let hasPrefetchedServerState = false;

            // Same-session refresh: fetch state once up front so recovery/poll
            // paths can reuse it instead of draining the state endpoint twice.
            if (
              isRefresh &&
              !isSessionSwitch &&
              get().messages.length > 0 &&
              get().sessionId === sessionId &&
              client.getSessionState
            ) {
              try {
                const softState = withoutPendingStoppedWorkerPartial(
                  sessionId,
                  await client.getSessionState(sessionId, {
                    includeDelegatedHistory: true,
                  }),
                );
                if (
                  selectionGeneration !== sessionSelectionGeneration ||
                  get().sessionId !== sessionId
                ) {
                  return;
                }
                prefetchedServerState = softState;
                hasPrefetchedServerState = true;
                rememberServerState(sessionId, softState);
                applySessionSnapshot(
                  sessionId,
                  softState,
                  true,
                  set,
                  get,
                  planStore,
                  {
                    // Only skip transcript remap while this client still owns SSE.
                    metadataOnly: isLocalStreamAttached(),
                  },
                );
                if (
                  isActiveSessionAgentState(softState.agent_state) ||
                  hasActiveDelegationGroups(softState.delegation_groups)
                ) {
                  get().startStatePolling(sessionId);
                }
                if (isLocalStreamAttached()) {
                  set({ isLoading: false });
                  return;
                }
              } catch {
                // Fall through to full load when soft refresh fails.
              }
            }

            const { data, serverStateResult } = await (async () => {
              const finishFetchDecodeSpan = beginMitsuroPerformanceSpan(
                "session.fetch_decode",
              );
              try {
                return await getSessionHydration(
                  sessionId,
                  hasPrefetchedServerState ? prefetchedServerState : undefined,
                );
              } finally {
                finishFetchDecodeSpan();
              }
            })();
            if (
              selectionGeneration !== sessionSelectionGeneration ||
              get().sessionId !== sessionId
            ) {
              return;
            }

            const previousMessages = get().messages;
            const finishMessageTransformSpan = beginMitsuroPerformanceSpan(
              "session.snapshot_transform",
            );
            let messageTransform;
            try {
              messageTransform = await processStoredMessagesCooperatively(
                data.messages,
                previousMessages,
                {
                  yieldToHost: yieldSessionHydrationHost,
                  shouldContinue: () =>
                    selectionGeneration === sessionSelectionGeneration &&
                    get().sessionId === sessionId,
                },
              );
            } finally {
              finishMessageTransformSpan();
            }
            recordMitsuroPerformanceMetric(
              "session.snapshot_max_slice",
              { durationMs: messageTransform.maxSliceDurationMs },
            );
            recordMitsuroPerformanceMetric(
              "session.snapshot_yields",
              { count: messageTransform.yieldCount },
            );
            if (
              messageTransform.cancelled ||
              selectionGeneration !== sessionSelectionGeneration ||
              get().sessionId !== sessionId
            ) {
              return;
            }

            // Give the optimistic shell and any queued input a host turn before
            // snapshot transforms and the single atomic transcript commit.
            await yieldSessionHydrationHost();
            if (
              selectionGeneration !== sessionSelectionGeneration ||
              get().sessionId !== sessionId
            ) {
              return;
            }
            const processedMessages = messageTransform.messages;
            const serverState = serverStateResult.ok && serverStateResult.state
              ? withoutPendingStoppedWorkerPartial(
                sessionId,
                serverStateResult.state,
              )
              : null;
            rememberServerState(sessionId, serverState);
            await queuedSuccessorRecovery.ready();
            if (
              selectionGeneration !== sessionSelectionGeneration ||
              get().sessionId !== sessionId
            ) {
              return;
            }

            const previousModel = get().model;
            const previousModelKey = get().modelKey;
            const finishSnapshotPublishSpan = beginMitsuroPerformanceSpan(
              "session.snapshot_publish",
            );
            const snapshot = (() => {
              try {
                const nextSnapshot = buildSessionSnapshotFromResponse(
                  data,
                  processedMessages,
                  serverState,
                );
                const hydratedMessages = applyDelegatedSessionState(
                  applyLivePartialAssistant(
                    applyRecoveryParity(
                      nextSnapshot.messages,
                      serverState?.recovery,
                      serverState?.agent_state ?? "idle",
                    ),
                    serverState?.live_partial_assistant,
                    serverState?.agent_state ?? "idle",
                    pendingInteractionsFromSnapshot(serverState),
                  ),
                  serverState?.delegated_tools,
                  serverState?.recent_delegated_runs,
                  serverState?.delegated_run_summaries,
                  serverState?.delegation_groups,
                );
                const hydratedQueuedRecovery = queuedRecoveryForSession(
                  sessionId,
                );

                set((s) => {
                  const sameExactSelection = Boolean(nextSnapshot.modelKey) &&
                    modelKeysEqual(nextSnapshot.modelKey, s.modelKey);
                  const nextModelProvider = nextSnapshot.model
                    ? nextSnapshot.modelKey?.provider ??
                      (nextSnapshot.model === s.model ? s.modelProvider : null)
                    : s.modelProvider;
                  const nextModelInfo = nextSnapshot.model
                    ? sameExactSelection ||
                        (!nextSnapshot.modelKey &&
                          nextSnapshot.model === s.model)
                      ? s.modelInfo
                      : null
                    : s.modelInfo;
                  const capabilityInput = nextModelInfo ?? nextSnapshot.model ??
                    s.model;
                  const nextThinkingLevel = normalizeThinkingLevel(
                    s.thinkingLevel,
                    capabilityInput,
                  );
                  const nextMode = serverState?.mode ?? nextSnapshot.mode;
                  const nextPermissionMode = serverState?.permission_mode ??
                    nextSnapshot.permissionMode;
                  return {
                    ...s,
                    sessionId: nextSnapshot.sessionId,
                    sessionType: nextSnapshot.sessionType,
                    title: nextSnapshot.title,
                    mode: nextMode,
                    permissionMode: nextPermissionMode,
                    model: nextSnapshot.model ?? s.model,
                    modelKey: nextSnapshot.model
                      ? nextSnapshot.modelKey
                      : s.modelKey,
                    modelProvider: nextModelProvider,
                    modelInfo: nextModelInfo,
                    thinkingLevel: nextThinkingLevel,
                    thinkingEnabled: isThinkingEnabled(nextThinkingLevel),
                    fastModeEnabled: nextSnapshot.model
                      ? s.fastModeEnabled &&
                        supportsFastMode(capabilityInput, nextModelProvider)
                      : s.fastModeEnabled,
                    tokenCount: nextSnapshot.tokenCount,
                    tokenUsage: null,
                    error: hydratedQueuedRecovery.blocked
                      ? QUEUED_RECOVERY_UNCERTAIN_MESSAGE
                      : serverState !== null
                      ? sessionAgentErrorMessage(serverState)
                      : previousSessionId === nextSnapshot.sessionId
                      ? s.error
                      : null,
                    messages: hydratedQueuedRecovery.messages(
                      hydratedMessages,
                    ),
                    queuedMessages: mergeQueuedPayloads(
                      hydratedQueuedRecovery.queuedMessages,
                      s.queuedMessages,
                    ),
                    queuedRecoveryBlocked: hydratedQueuedRecovery.blocked,
                    isLoading: false,
                    isStreaming: isActiveSessionAgentState(
                      serverState?.agent_state,
                    ),
                    isThinking: serverState?.agent_state === "streaming"
                      ? Boolean(
                        serverState.live_partial_assistant?.thinking?.trim(),
                      ) || s.isThinking
                      : false,
                    thinkingContent:
                      serverState?.live_partial_assistant?.thinking || "",
                    lastEventSequence: serverState?.last_event_sequence ?? null,
                    delegationEventCursor:
                      serverState?.delegation_event_cursor ?? null,
                  };
                });
                return nextSnapshot;
              } finally {
                finishSnapshotPublishSpan();
              }
            })();
            reconcileSessionStopSnapshot(sessionId, serverState);
            queueMicrotask(() => resumeQueuedRecovery(sessionId));
            try {
              storage.set(
                "mitsuro-permission-mode",
                serverState?.permission_mode ?? snapshot.permissionMode,
              );
            } catch {
              /* ignore */
            }

            planStore.getState().setWorkflow(serverState?.workflow ?? null);
            planStore
              .getState()
              .setVisible(
                Boolean(serverState?.workflow) || snapshot.mode === "plan",
              );
            const directory = snapshot.projectDir ?? snapshot.workingDir ??
              null;
            workspace
              .getState()
              .initFromSession(
                snapshot.sessionId,
                directory,
                (snapshot.workspaceMode ??
                  (directory ? "selected" : "neutral")) as
                    | "neutral"
                    | "selected"
                    | "created",
                snapshot.targetBranch,
              );

            if (
              serverState &&
              (
                isActiveSessionAgentState(serverState.agent_state) ||
                hasActiveDelegationGroups(serverState.delegation_groups)
              ) &&
              get().sessionId === sessionId
            ) {
              get().startStatePolling(sessionId);
            }
            startPresenceTransport(sessionId);
            if (
              snapshot.model &&
              (
                snapshot.model !== previousModel ||
                !modelKeysEqual(snapshot.modelKey, previousModelKey)
              )
            ) {
              void persistCurrentSelectedModel(
                snapshot.model,
                snapshot.modelKey,
              );
            }

            // Cache compaction is useful for the next visit, not for this first
            // paint. Keep it out of the visible transcript publication task.
            await yieldSessionHydrationHost();
            if (
              selectionGeneration === sessionSelectionGeneration &&
              get().sessionId === sessionId
            ) {
              const finishCacheSpan = beginMitsuroPerformanceSpan(
                "session.cache_compact",
              );
              try {
                sessionCache.set(snapshot);
              } finally {
                finishCacheSpan();
              }
            }
          } catch (err) {
            if (
              selectionGeneration !== sessionSelectionGeneration ||
              get().sessionId !== sessionId
            ) {
              return;
            }
            if (isNotFoundApiError(err)) {
              const current = get();
              sessionCache.delete(sessionId);
              void queuedSuccessorRecovery.delete(sessionId).catch(() => {});
              lastKnownServerState.delete(sessionId);
              stopPresenceTransport(previousSessionId);
              if (workspace.getState().sessionId === sessionId) {
                workspace.getState().setSession(null);
              }
              set({
                ...initialState,
                permissionMode: current.permissionMode,
                model: current.model,
                modelKey: current.modelKey,
                modelProvider: current.modelProvider,
                modelInfo: current.modelInfo,
                thinkingLevel: current.thinkingLevel,
                thinkingEnabled: current.thinkingEnabled,
                fastModeEnabled: current.fastModeEnabled,
                isLoading: false,
                error: null,
              });
              sessionsStore.getState().loadSessions();
              return;
            }
            set({
              isLoading: false,
              error: toErrorMessage(err),
            });
          }
        })().finally(() => {
          finishOpenSpan();
          releaseRequest();
        });

        inFlightSessionLoads.set(sessionId, run);
        try {
          await run;
        } finally {
          if (inFlightSessionLoads.get(sessionId) === run) {
            inFlightSessionLoads.delete(sessionId);
          }
        }
      },
      cancelPendingSessionLoad() {
        sessionSelectionGeneration += 1;
        // Allow a newly visible consumer to join/restart immediately. The
        // shared network hydration remains single-flight, but stale consumers
        // will fail their generation guard before transcript processing.
        inFlightSessionLoads.clear();
      },
      // -- clearSession -------------------------------------------------------

      clearSession() {
        const current = get();
        workerInputIdempotency.transitionTo(null);
        sessionSelectionGeneration += 1;
        streamAttachmentGeneration += 1;
        abortController?.abort();
        abortController = null;
        localStreamGeneration = null;
        activeHiveStreamOwner = null;
        deferredCanonicalReloads.clear();
        activeWorkerResponseBoundary = null;
        pendingStoppedWorkerResponse = null;
        activeWorkerStopSettlement = null;
        stopGeneration += 1;
        get().stopStatePolling();
        stopPresenceTransport(current.sessionId);
        set({
          ...initialState,
          permissionMode: current.permissionMode,
          model: current.model,
          modelKey: current.modelKey,
          modelProvider: current.modelProvider,
          modelInfo: current.modelInfo,
          thinkingLevel: current.thinkingLevel,
          thinkingEnabled: current.thinkingEnabled,
          fastModeEnabled: current.fastModeEnabled,
        });
        workspace.getState().clear();
        planStore.getState().setWorkflow(null);
      },

      // -- initSession --------------------------------------------------------

      initSession(
        sessionId: string,
        title: string,
        permissionMode?: PermissionMode,
        sessionType?: import("@mitsuro/api").SessionType,
      ) {
        const current = get();
        workerInputIdempotency.transitionTo(sessionId);
        sessionSelectionGeneration += 1;
        streamAttachmentGeneration += 1;
        abortController?.abort();
        abortController = null;
        localStreamGeneration = null;
        activeHiveStreamOwner = null;
        activeWorkerResponseBoundary = null;
        pendingStoppedWorkerResponse = null;
        activeWorkerStopSettlement = null;
        stopGeneration += 1;
        get().stopStatePolling();
        stopPresenceTransport(current.sessionId);
        const nextPermissionMode = permissionMode ?? current.permissionMode;
        try {
          storage.set("mitsuro-permission-mode", nextPermissionMode);
        } catch {
          /* ignore */
        }
        set({
          ...initialState,
          permissionMode: nextPermissionMode,
          model: current.model,
          modelKey: current.modelKey,
          modelProvider: current.modelProvider,
          modelInfo: current.modelInfo,
          thinkingLevel: current.thinkingLevel,
          thinkingEnabled: current.thinkingEnabled,
          fastModeEnabled: current.fastModeEnabled,
          sessionId,
          sessionType: sessionType ?? null,
          title: normalizeDisplayTitle(title),
        });
        planStore.getState().setWorkflow(null);
        // The active-mode lifecycle owns presence intent. A creation can finish
        // after its mode was hidden, so binding identity must not opt itself back in.
        startPresenceTransport(sessionId);
      },

      // -- setTitle ------------------------------------------------------------

      setTitle(title: string) {
        set({ title });
      },

      // -- updateTitle --------------------------------------------------------

      async updateTitle(sessionId: string, title: string) {
        const titleSelectionGeneration = sessionSelectionGeneration;
        try {
          await client.updateSession(sessionId, { title });
          sessionsStore.getState().loadSessions();
          if (
            sessionSelectionGeneration === titleSelectionGeneration &&
            get().sessionId === sessionId
          ) {
            set({ title });
          }
        } catch {
          // Failed to update title
        }
      },

      // -- setMode ------------------------------------------------------------

      setMode(mode: SessionMode) {
        if (get().sessionType !== "code") return;
        set({ mode });
        planStore.getState().setVisible(mode === "plan");
        void persistMode(get, mode);
      },

      async executeWorkflowCommand(command) {
        const sessionId = get().sessionId;
        if (!sessionId) {
          throw new Error("No active session");
        }
        const commandSelectionGeneration = sessionSelectionGeneration;
        const isCommandOriginCurrent = () =>
          sessionSelectionGeneration === commandSelectionGeneration &&
          get().sessionId === sessionId;
        const mutation = await client.executeWorkflowCommand(
          sessionId,
          command,
        );
        if (!isCommandOriginCurrent()) {
          throw new Error("Workflow command target changed before completion");
        }
        planStore.getState().setWorkflow(mutation.snapshot);
        if (!isCommandOriginCurrent()) {
          throw new Error("Workflow command target changed before completion");
        }
        if (mutation.snapshot.goal.status === "active") {
          set({ mode: "build" });
        }
        return mutation;
      },

      // -- setModel -----------------------------------------------------------

      setModel(
        model: string | null,
        provider?: string | null,
        modelInfo?: ModelInfo | null,
        modelKey?: ModelKey | null,
      ) {
        const current = get();
        const nextModelInfo = model
          ? modelInfo !== undefined
            ? modelInfo
            : model === current.model
            ? current.modelInfo
            : null
          : null;
        const nextModelKey = model
          ? modelKey !== undefined ? modelKey : nextModelInfo?.key ??
            (model === current.model ? current.modelKey : null)
          : null;
        const nextProvider = nextModelInfo?.provider ??
          nextModelKey?.provider ??
          provider ??
          (model === current.model ? current.modelProvider : null);
        if (
          current.model === model &&
          modelKeysEqual(current.modelKey, nextModelKey) &&
          current.modelProvider === nextProvider &&
          current.modelInfo === nextModelInfo
        ) {
          return;
        }

        set((s) => {
          const capabilityInput = nextModelInfo ?? model;
          const thinkingLevel = normalizeThinkingLevel(
            s.thinkingLevel,
            capabilityInput,
          );
          return {
            model,
            modelKey: nextModelKey,
            modelProvider: nextProvider,
            modelInfo: nextModelInfo,
            thinkingLevel,
            thinkingEnabled: isThinkingEnabled(thinkingLevel),
            fastModeEnabled: model
              ? s.fastModeEnabled &&
                supportsFastMode(capabilityInput, nextProvider)
              : false,
          };
        });
        void persistCurrentSelectedModel(model, nextModelKey);
        void persistModel(get, model, nextModelKey);
      },

      // -- setThinkingLevel ---------------------------------------------------

      setThinkingLevel(level: ThinkingLevel) {
        set((state) => {
          const normalized = normalizeThinkingLevel(
            level,
            state.modelInfo ?? state.model,
          );
          return {
            thinkingLevel: normalized,
            thinkingEnabled: isThinkingEnabled(normalized),
          };
        });
      },

      // -- toggleThinking -----------------------------------------------------

      toggleThinking() {
        set((s) => {
          const newLevel = cycleThinkingLevel(
            s.thinkingLevel,
            s.modelInfo ?? s.model,
          );
          return {
            thinkingEnabled: isThinkingEnabled(newLevel),
            thinkingLevel: newLevel,
          };
        });
      },

      // -- setFastModeEnabled -------------------------------------------------

      setFastModeEnabled(enabled: boolean) {
        set((s) => ({
          fastModeEnabled: enabled &&
            supportsFastMode(s.modelInfo ?? s.model, s.modelProvider),
        }));
      },

      // -- toggleFastMode -----------------------------------------------------

      toggleFastMode() {
        set((s) => {
          if (!supportsFastMode(s.modelInfo ?? s.model, s.modelProvider)) {
            return { fastModeEnabled: false };
          }
          return { fastModeEnabled: !s.fastModeEnabled };
        });
      },

      // -- togglePermissionMode -----------------------------------------------

      togglePermissionMode() {
        set((s) => {
          const newMode: PermissionMode = s.permissionMode === "supervised"
            ? "autonomous"
            : "supervised";
          try {
            storage.set("mitsuro-permission-mode", newMode);
          } catch {
            /* ignore */
          }
          void persistPermissionMode(get, newMode);
          return { permissionMode: newMode };
        });
      },

      // -- submitToolResult ---------------------------------------------------

      async submitToolResult(toolCallId: string, result: string) {
        const state = get();
        if (!state.sessionId) {
          throw new Error("No active session");
        }
        if (pendingSessionStop(state.sessionId)) {
          throw new Error(SESSION_STOP_PENDING_MESSAGE);
        }

        set((s) => ({
          messages: s.messages.map((m) => ({
            ...m,
            toolCalls: m.toolCalls?.map((tc) =>
              tc.id === toolCallId
                ? { ...tc, output: result, status: "success" as const }
                : tc
            ),
          })),
          isStreaming: true,
          isLoading: true,
        }));

        abortController = new AbortController();
        const streamController = abortController;
        const streamGeneration = ++streamAttachmentGeneration;
        localStreamGeneration = streamGeneration;
        const streamTerminal = { current: false };
        const isStreamGenerationCurrent = () =>
          streamGeneration === streamAttachmentGeneration &&
          get().sessionId === state.sessionId;
        const isStreamAttached = () =>
          isStreamGenerationCurrent() && !streamTerminal.current;
        const finishConnectSpan = beginMitsuroPerformanceSpan(
          "stream.connect",
          state.sessionId,
        );
        const releaseStreamConnection = trackMitsuroPerformanceResource(
          "stream_connections",
        );
        get().startStatePolling(state.sessionId);

        const ref: AssistantMessageRef = {
          current: createStreamingAssistantMessage(),
        };

        set((s) => ({
          messages: upsertTransientAssistantMessage(s.messages, ref.current),
        }));

        const streamRecovery: { promise: Promise<boolean> | null } = {
          promise: null,
        };
        let terminalStreamError: Error | null = null;
        const guardedCallbacks = guardStreamCallbacks(
          attachLiveStreamLifecycle(
            createRecoveringStreamCallbacks(
              createStreamCallbacks(ref, set, get, {
                planStore,
                sessionsStore,
                persistSessionMode: persistMode,
                isActive: isStreamAttached,
                isSessionCurrent: (sessionId) =>
                  isStreamGenerationCurrent() && sessionId === state.sessionId,
                onFirstEvent: finishConnectSpan,
                onDelegationEvent: () => {
                  if (state.sessionId) {
                    get().refreshDelegationState(state.sessionId);
                  }
                },
                onWorkerResponseBoundaryChange: (boundary) => {
                  activeWorkerResponseBoundary = boundary;
                },
                deferCanonicalReload: (sessionId) => {
                  deferredCanonicalReloads.add(sessionId);
                },
                consumeCanonicalReload: (sessionId) =>
                  deferredCanonicalReloads.delete(sessionId),
              }),
              state.sessionId,
              streamRecovery,
            ),
            streamGeneration,
          ),
          isStreamAttached,
          streamTerminal,
        );
        const callbacks: StreamCallbacks = {
          ...guardedCallbacks,
          onError: (streamError) => {
            if (!isStreamAttached()) return;
            terminalStreamError = new Error(streamError);
            guardedCallbacks.onError(streamError);
          },
        };
        let keepStatePolling = false;

        try {
          await client.streamToolResult(
            {
              session_id: state.sessionId,
              tool_call_id: toolCallId,
              result,
              fast_mode: state.fastModeEnabled,
              thinking_enabled: thinkingLevelToApiValue(state.thinkingLevel),
              permission_mode: state.permissionMode,
            },
            callbacks,
            streamController.signal,
          );
          const resolvedTerminalError = terminalStreamError as Error | null;
          if (resolvedTerminalError && streamRecovery.promise) {
            keepStatePolling = await streamRecovery.promise;
          }
          if (resolvedTerminalError && !keepStatePolling) {
            applyStreamFailure(resolvedTerminalError);
            throw resolvedTerminalError;
          }
        } catch (err) {
          if (!isStreamGenerationCurrent()) return;
          streamRecovery.promise ??= recoverAfterStreamInterruption(
            state.sessionId,
          );
          keepStatePolling = await streamRecovery.promise;
          if (!isStreamGenerationCurrent()) return;
          if (!keepStatePolling) {
            applyStreamFailure(err);
            throw err;
          }
        } finally {
          finishConnectSpan();
          releaseStreamConnection();
          if (localStreamGeneration === streamGeneration) {
            localStreamGeneration = null;
          }
          if (streamRecovery.promise) {
            keepStatePolling = await streamRecovery.promise;
          }
          if (isStreamGenerationCurrent() && !keepStatePolling) {
            get().stopStatePolling();
          }
        }
      },

      // -- submitToolApproval -------------------------------------------------

      async submitToolApproval(toolCallId: string, approved: boolean) {
        const state = get();
        if (!state.sessionId) return;
        if (pendingSessionStop(state.sessionId)) {
          throw new Error(SESSION_STOP_PENDING_MESSAGE);
        }
        const targetSessionId = state.sessionId;
        const approvalAttachmentGeneration = streamAttachmentGeneration;
        const isApprovalOriginCurrent = () =>
          get().sessionId === targetSessionId &&
          streamAttachmentGeneration === approvalAttachmentGeneration;

        set((s) => ({
          messages: s.messages.map((m) => ({
            ...m,
            toolCalls: m.toolCalls?.map((tc) =>
              tc.id === toolCallId
                ? approved
                  ? { ...tc, status: "running" as const, output: undefined }
                  : {
                    ...tc,
                    status: "error" as const,
                    output: tc.output ?? "Denied by user",
                  }
                : tc
            ),
          })),
        }));

        await client.submitToolApproval(targetSessionId, toolCallId, approved);
        if (!isApprovalOriginCurrent()) return;
        set({ isStreaming: true, isLoading: true });
        if (!isApprovalOriginCurrent()) return;
        get().startStatePolling(targetSessionId);
      },

      // -- detachSession / stopStreaming -------------------------------------

      detachSession() {
        const activeSessionId = get().sessionId;
        streamAttachmentGeneration += 1;
        abortController?.abort();
        abortController = null;
        localStreamGeneration = null;
        activeHiveStreamOwner = null;
        activeWorkerResponseBoundary = null;
        pendingStoppedWorkerResponse = null;
        activeWorkerStopSettlement = null;
        stopGeneration += 1;
        get().stopStatePolling();
        get().stopPresenceHeartbeat(activeSessionId);
        set((s) => ({
          isLoading: false,
          isStreaming: false,
          isThinking: false,
          thinkingContent: "",
          messages: pruneEmptyAssistantMessages(
            finalizeTransientAssistantMessages(s.messages),
          ),
        }));
      },

      stopStreaming(options = {}) {
        const activeSessionId = get().sessionId;
        if (
          options.expectedSessionId &&
          options.expectedSessionId !== activeSessionId
        ) {
          return;
        }
        if (pendingSessionStop(activeSessionId)?.readyForSnapshot === false) {
          return;
        }
        const workerBoundary = activeSessionId !== null &&
            activeWorkerResponseBoundary?.sessionId === activeSessionId
          ? activeWorkerResponseBoundary
          : null;
        const workerStreamOwner = activeSessionId !== null &&
            activeHiveStreamOwner?.sessionId === activeSessionId &&
            activeHiveStreamOwner.generation === streamAttachmentGeneration &&
            activeHiveStreamOwner.kind === "worker_dm"
          ? activeHiveStreamOwner
          : null;
        const explicitlyBoundWorker = activeSessionId !== null &&
          options.expectedSessionId === activeSessionId &&
          options.hiveConversationKind === "worker_dm";
        const isWorkerResponse = workerBoundary !== null ||
          workerStreamOwner !== null || explicitlyBoundWorker;
        const shouldCancel = activeSessionId !== null && get().isStreaming;
        const stoppedStreamGeneration = streamAttachmentGeneration;
        if (shouldCancel) stoppedStreamGenerations.add(stoppedStreamGeneration);
        streamAttachmentGeneration += 1;
        abortController?.abort();
        abortController = null;
        localStreamGeneration = null;
        activeHiveStreamOwner = null;
        activeWorkerResponseBoundary = null;
        const stopSettlementGeneration = ++stopGeneration;
        pendingStoppedWorkerResponse = shouldCancel && isWorkerResponse &&
            activeSessionId !== null
          ? workerBoundary ?? { sessionId: activeSessionId, runId: null }
          : null;
        activeWorkerStopSettlement = shouldCancel && isWorkerResponse &&
            activeSessionId !== null
          ? {
            sessionId: activeSessionId,
            generation: stopSettlementGeneration,
          }
          : null;
        activeSessionStopSettlement = shouldCancel && !isWorkerResponse &&
            activeSessionId
          ? {
            sessionId: activeSessionId,
            generation: stopSettlementGeneration,
            readyForSnapshot: false,
            error: null,
          }
          : null;
        get().stopStatePolling();
        set((s) => ({
          isLoading: shouldCancel && !isWorkerResponse,
          isStreaming: shouldCancel && !isWorkerResponse,
          isThinking: false,
          thinkingContent: "",
          messages: pruneEmptyAssistantMessages(
            isWorkerResponse
              ? discardTransientAssistantMessages(s.messages)
              : finalizeTransientAssistantMessages(s.messages),
          ),
        }));
        if (!shouldCancel || !activeSessionId) return;
        if (!workerBoundary && !isWorkerResponse) {
          void stopOrdinarySession(activeSessionId, stopSettlementGeneration);
          return;
        }
        void (async () => {
          if (!workerBoundary) {
            try {
              await client.cancelSession(activeSessionId);
            } catch (error) {
              if (
                stopGeneration !== stopSettlementGeneration ||
                get().sessionId !== activeSessionId
              ) {
                return;
              }
              pendingStoppedWorkerResponse = null;
              const stopError = toErrorMessage(
                error,
                "Unable to stop the Hive response",
              );
              try {
                await get().loadSession(activeSessionId, true);
              } catch {
                // Preserve the exact cancellation failure below even if reload fails.
              } finally {
                clearWorkerStopSettlement(
                  activeSessionId,
                  stopSettlementGeneration,
                );
              }
              if (
                stopGeneration === stopSettlementGeneration &&
                get().sessionId === activeSessionId
              ) {
                set({ error: stopError });
              }
              return;
            }
            if (
              stopGeneration !== stopSettlementGeneration ||
              get().sessionId !== activeSessionId
            ) {
              return;
            }
            await reconcileStoppedWorkerResponse(
              activeSessionId,
              null,
              stopSettlementGeneration,
              get,
              () => {
                set((state) => ({
                  isLoading: false,
                  isStreaming: false,
                  isThinking: false,
                  thinkingContent: "",
                  messages: pruneEmptyAssistantMessages(
                    discardTransientAssistantMessages(state.messages),
                  ),
                }));
              },
            );
            return;
          }

          try {
            await client.cancelSession(activeSessionId);
          } catch (error) {
            if (
              stopGeneration !== stopSettlementGeneration ||
              get().sessionId !== activeSessionId
            ) {
              return;
            }
            pendingStoppedWorkerResponse = null;
            const stopError = toErrorMessage(
              error,
              "Unable to stop the Hive Worker response",
            );
            try {
              await get().loadSession(activeSessionId, true);
            } catch {
              // Preserve the exact cancellation failure below even if reload fails.
            } finally {
              clearWorkerStopSettlement(
                activeSessionId,
                stopSettlementGeneration,
              );
            }
            if (
              stopGeneration === stopSettlementGeneration &&
              get().sessionId === activeSessionId
            ) {
              set({ error: stopError });
            }
            return;
          }
          await reconcileStoppedWorkerResponse(
            activeSessionId,
            workerBoundary.runId,
            stopSettlementGeneration,
            get,
            () => {
              set((state) => ({
                isLoading: false,
                isStreaming: false,
                isThinking: false,
                thinkingContent: "",
                messages: pruneEmptyAssistantMessages(
                  discardTransientAssistantMessages(state.messages),
                ),
              }));
            },
          );
        })();
      },

      // -- state polling ------------------------------------------------------

      startStatePolling(sessionId: string) {
        get().stopStatePolling();
        releaseStatePollingResource = trackMitsuroPerformanceResource(
          "state_polling",
        );
        const generation = statePollingGeneration;
        let consecutiveFailures = 0;

        const schedule = (delay: number) => {
          if (generation !== statePollingGeneration) return;
          statePollingTimer = setTimeout(poll, delay);
        };

        const poll = async () => {
          if (generation !== statePollingGeneration) return;
          try {
            const delegationEventCursor = get().delegationEventCursor;
            const serverState = withoutPendingStoppedWorkerPartial(
              sessionId,
              await client.getSessionState(sessionId, {
                delegationAfterCursor: delegationEventCursor ?? undefined,
              }),
            );
            if (
              generation !== statePollingGeneration ||
              get().sessionId !== sessionId
            ) return;
            consecutiveFailures = 0;
            rememberServerState(sessionId, serverState);
            applySessionSnapshot(
              sessionId,
              serverState,
              true,
              set,
              get,
              planStore,
              {
                // While the local SSE stream is attached it owns the live transcript.
                // Poll only for metadata so we don't remount the whole chat every 3s.
                metadataOnly: isLocalStreamAttached(),
              },
            );
            reconcileSessionStopSnapshot(sessionId, serverState);
            if (
              generation !== statePollingGeneration ||
              get().sessionId !== sessionId
            ) return;

            if (get().error === STATE_POLL_DEGRADED_MESSAGE) {
              set({ error: null });
            }

            if (
              shouldStopSessionStatePolling(
                serverState.agent_state,
                serverState.delegation_groups,
              ) && !pendingSessionStop(sessionId)
            ) {
              const terminalError = sessionAgentErrorMessage(serverState);
              const terminalSelectionGeneration = sessionSelectionGeneration;
              get().stopStatePolling();
              set({
                isStreaming: false,
                isThinking: false,
                thinkingContent: "",
                error: terminalError,
              });
              if (
                sessionSelectionGeneration !== terminalSelectionGeneration ||
                get().sessionId !== sessionId
              ) return;
              await get().loadSession(sessionId, true);
              if (
                sessionSelectionGeneration !== terminalSelectionGeneration ||
                get().sessionId !== sessionId
              ) return;
              if (terminalError) {
                set({ error: terminalError });
              }
              return;
            }

            schedule(STATE_POLL_INTERVAL);
          } catch {
            if (generation !== statePollingGeneration) return;
            consecutiveFailures += 1;

            if (consecutiveFailures >= STATE_POLL_MAX_FAILURES) {
              get().stopStatePolling();
              set({
                isLoading: false,
                error:
                  `Unable to refresh session status after ${STATE_POLL_MAX_FAILURES} attempts. ` +
                  "The run may still be active; reconnect or refresh before sending another message.",
              });
              return;
            }

            if (consecutiveFailures >= STATE_POLL_DEGRADED_AFTER) {
              set({ error: STATE_POLL_DEGRADED_MESSAGE });
            }

            const backoff = Math.min(
              STATE_POLL_INTERVAL * 2 ** (consecutiveFailures - 1),
              STATE_POLL_MAX_BACKOFF,
            );
            schedule(backoff);
          }
        };

        schedule(STATE_POLL_INTERVAL);
      },

      stopStatePolling() {
        statePollingGeneration += 1;
        if (statePollingTimer) {
          clearTimeout(statePollingTimer);
          statePollingTimer = null;
        }
        releaseStatePollingResource?.();
        releaseStatePollingResource = null;
      },

      refreshDelegationState(sessionId: string) {
        if (get().sessionId !== sessionId || delegationRefreshTimer) return;
        const selectionGeneration = sessionSelectionGeneration;
        // Coalesce lifecycle bursts while keeping the UI event-driven. The
        // canonical snapshot remains authoritative; SSE is the prompt trigger.
        delegationRefreshTimer = setTimeout(() => {
          delegationRefreshTimer = null;
          const delegationEventCursor = get().delegationEventCursor;
          void client
            .getSessionState(sessionId, {
              delegationAfterCursor: delegationEventCursor ?? undefined,
            })
            .then((serverState) => {
              if (
                selectionGeneration !== sessionSelectionGeneration ||
                get().sessionId !== sessionId
              ) {
                return;
              }
              serverState = withoutPendingStoppedWorkerPartial(
                sessionId,
                serverState,
              );
              rememberServerState(sessionId, serverState);
              applySessionSnapshot(
                sessionId,
                serverState,
                true,
                set,
                get,
                planStore,
                {
                  metadataOnly: isLocalStreamAttached(),
                },
              );
            })
            .catch(() => {
              // The ordinary bounded state poll retains retry/backoff ownership.
            });
        }, 50);
      },

      // -- presence heartbeat -------------------------------------------------

      startPresenceHeartbeat(sessionId: string) {
        presenceDesired = true;
        startPresenceTransport(sessionId);
      },

      stopPresenceHeartbeat(sessionId?: string | null) {
        if (
          sessionId &&
          presenceHeartbeatSessionId &&
          sessionId !== presenceHeartbeatSessionId
        ) {
          return;
        }
        presenceDesired = false;
        stopPresenceTransport(sessionId);
      },

      // -- cleanup ------------------------------------------------------------

      cleanup() {
        disposed = true;
        queuedSuccessorRecovery.dispose();
        sessionSelectionGeneration += 1;
        streamAttachmentGeneration += 1;
        abortController?.abort();
        abortController = null;
        localStreamGeneration = null;
        activeHiveStreamOwner = null;
        activeWorkerResponseBoundary = null;
        pendingStoppedWorkerResponse = null;
        activeWorkerStopSettlement = null;
        stopGeneration += 1;
        workerInputIdempotency.clear();
        hiveConversationKinds.clear();
        pendingHiveConversationKinds.clear();
        deferredCanonicalReloads.clear();
        queuedSuccessorClaims.clear();
        steeringInFlightSessions.clear();
        queuedAppendInFlightSessions.clear();
        get().stopStatePolling();
        if (delegationRefreshTimer) {
          clearTimeout(delegationRefreshTimer);
          delegationRefreshTimer = null;
        }
        const state = get();
        get().stopPresenceHeartbeat(state.sessionId);
        // Dispose heavy in-memory retention so mode/store teardown cannot sludge RAM.
        sessionCache.clear();
        lastKnownServerState.clear();
        inFlightSessionLoads.clear();
        inFlightSessionHydrations.clear();
        set({
          messages: [],
          queuedMessages: [],
          queuedRecoveryBlocked: false,
          thinkingContent: "",
          tokenUsage: null,
          error: null,
          isLoading: false,
          isStreaming: false,
          isThinking: false,
        });
      },
    };
  });
}
