import assert from "node:assert/strict";
import test from "node:test";

import type {
  ChatRequest,
  SessionResponse,
  StreamCallbacks,
} from "../packages/api/src/types";
import {
  MemoryStorage,
  createSessionStore,
  createWorkspaceStore,
} from "../packages/state/src";
import {
  chronologicalSessions,
  codeDirectoryToAutoExpand,
  codeProjectThreadGroups,
} from "../apps/mobile/components/navigation/threadSections";
import { formatWorkspaceContextMetadata } from "../apps/mobile/components/chat/composerMetadata";
import { displayThreadTitle } from "../apps/mobile/components/navigation/threadTitle";
import { modeForHorizontalSwipe } from "../apps/mobile/components/navigation/modeSwipe";
import { resolveAppBottomSheetHeight } from "../apps/mobile/components/sheets/sheetMetrics";

function session(
  id: string,
  type: SessionResponse["session_type"],
  updatedAt: string,
  projectDir: string | null = null,
): SessionResponse {
  return {
    id,
    title: id,
    token_count: 0,
    working_dir: projectDir,
    project_dir: projectDir,
    workspace_mode: projectDir ? "selected" : "neutral",
    session_type: type,
    parent_session_id: null,
    mode: "build",
    permission_mode: "autonomous",
    updated_at: updatedAt,
    model: null,
    target_branch: null,
  };
}

test("thread selectors isolate modes and sort newest first", () => {
  const sessions = [
    session("chat-old", "chat", "2026-01-01T00:00:00Z"),
    session("code-new", "code", "2026-04-01T00:00:00Z", "/repo/b"),
    session("chat-new", "chat", "2026-03-01T00:00:00Z"),
    session("hive", "hive", "2026-05-01T00:00:00Z"),
  ];

  assert.deepEqual(
    chronologicalSessions(sessions, "chat").map((item) => item.id),
    ["chat-new", "chat-old"],
  );
});

test("Code project groups are ordered by their latest thread", () => {
  const groups = codeProjectThreadGroups([
    session("a-old", "code", "2026-01-01T00:00:00Z", "/repo/a"),
    session("a-new", "code", "2026-04-01T00:00:00Z", "/repo/a"),
    session("b-new", "code", "2026-05-01T00:00:00Z", "/repo/b"),
    session("chat", "chat", "2026-06-01T00:00:00Z"),
  ]);

  assert.deepEqual(
    groups.map((group) => group.directory),
    ["/repo/b", "/repo/a"],
  );
  assert.deepEqual(
    groups[1]?.sessions.map((item) => item.id),
    ["a-new", "a-old"],
  );
});

test("active Code project auto-expands once without fighting manual collapse", () => {
  const sessions = [
    session("code-a", "code", "2026-04-01T00:00:00Z", "/repo/a"),
  ];

  assert.equal(codeDirectoryToAutoExpand(sessions, "code-a", null), "/repo/a");
  assert.equal(
    codeDirectoryToAutoExpand(sessions, "code-a", "code-a"),
    null,
  );
});

test("Threads and Toolbox share the same safe-area sheet metric", () => {
  const threadHeight = resolveAppBottomSheetHeight(852, 59);
  const toolboxHeight = resolveAppBottomSheetHeight(852, 59);

  assert.equal(threadHeight, 787);
  assert.equal(toolboxHeight, threadHeight);
});

test("Code composer metadata names the connected workspace and branch", () => {
  assert.deepEqual(
    formatWorkspaceContextMetadata(
      "/Users/Jacob/Documents/Mitsuro",
      "codex/navigation-shell",
    ),
    {
      label: "Mitsuro · codex/navigation-shell",
      hasBranch: true,
    },
  );
  assert.deepEqual(formatWorkspaceContextMetadata("C:\\work\\Forum", null), {
    label: "Forum",
    hasBranch: false,
  });
  assert.equal(formatWorkspaceContextMetadata(null, "main"), null);
});

test("header titles hide placeholders and keep meaningful thread names", () => {
  assert.equal(displayThreadTitle("New chat"), "");
  assert.equal(displayThreadTitle("New Session"), "");
  assert.equal(displayThreadTitle("Session 2025-08-06 01:37"), "");
  assert.equal(
    displayThreadTitle("Navigation shell and drawer polish"),
    "Navigation shell and drawer polish",
  );
});

test("horizontal swipes move between adjacent modes without wrapping", () => {
  assert.equal(modeForHorizontalSwipe("chat", -80, 0), "code");
  assert.equal(modeForHorizontalSwipe("code", -80, 0), "hive");
  assert.equal(modeForHorizontalSwipe("hive", 80, 0), "code");
  assert.equal(modeForHorizontalSwipe("chat", 80, 0), null);
  assert.equal(modeForHorizontalSwipe("hive", -80, 0), null);
  assert.equal(modeForHorizontalSwipe("code", 20, -800), "hive");
  assert.equal(modeForHorizontalSwipe("code", 20, 200), null);
});

test("mode workspaces persist independently while Code keeps the legacy key", () => {
  const storage = new MemoryStorage();
  const chatWorkspace = createWorkspaceStore(
    storage,
    "mitsuro:workspace:chat",
  );
  const codeWorkspace = createWorkspaceStore(storage);
  const hiveWorkspace = createWorkspaceStore(
    storage,
    "mitsuro:workspace:hive",
  );

  chatWorkspace
    .getState()
    .setWorkspace(null, "chat-session", "neutral");
  codeWorkspace
    .getState()
    .setWorkspace("/repo/mitsuro", "code-session", "selected", "feature/ui");
  hiveWorkspace
    .getState()
    .setWorkspace(null, "hive-session", "neutral");

  assert.equal(
    createWorkspaceStore(storage, "mitsuro:workspace:chat").getState().sessionId,
    "chat-session",
  );
  assert.deepEqual(
    {
      directory: createWorkspaceStore(storage).getState().directory,
      sessionId: createWorkspaceStore(storage).getState().sessionId,
      targetBranch: createWorkspaceStore(storage).getState().targetBranch,
    },
    {
      directory: "/repo/mitsuro",
      sessionId: "code-session",
      targetBranch: "feature/ui",
    },
  );
  assert.equal(
    createWorkspaceStore(storage, "mitsuro:workspace:hive").getState().sessionId,
    "hive-session",
  );
});

function createStreamingHarness() {
  let cancelCalls = 0;
  const client = {
    async streamChat(
      _request: ChatRequest,
      _callbacks: StreamCallbacks,
      signal?: AbortSignal,
    ) {
      await new Promise<void>((resolve, reject) => {
        signal?.addEventListener(
          "abort",
          () => reject(new Error("local stream detached")),
          { once: true },
        );
        if (!signal) {
          resolve();
        }
      });
    },
    async cancelSession() {
      cancelCalls += 1;
    },
    async getSession(id: string) {
      return {
        session: { id, title: "Running", session_type: "chat", mode: "build", model: null, token_count: 0 },
        messages: [],
      };
    },
    async getSessionState() {
      return {
        id: "session-running", agent_state: "idle", mode: "build",
        permission_mode: "autonomous", recovery: null,
        pending_interactions: [], delegated_tools: [], recent_delegated_runs: [],
        last_event_sequence: null, live_partial_assistant: null,
      };
    },
    async setSessionPresence() {
      return undefined;
    },
    async removeSessionPresence() {
      return undefined;
    },
    async setCurrentModel() {
      return { ok: true };
    },
    async updateSession() {
      return undefined;
    },
  };
  const storage = new MemoryStorage();
  const workspace = createWorkspaceStore(storage);
  const sessionsStore = {
    getState: () => ({
      loadSessions: async () => undefined,
    }),
  };
  const planStore = {
    getState: () => ({
      setVisible: () => undefined,
      setWorkflow: () => undefined,
      setItems: () => undefined,
    }),
  };
  const store = createSessionStore(
    client as never,
    storage,
    workspace,
    sessionsStore as never,
    planStore as never,
  );
  store
    .getState()
    .initSession("session-running", "Running", "autonomous", "chat");

  return {
    store,
    cancelCalls: () => cancelCalls,
  };
}

test("detaching for navigation does not cancel server work", async () => {
  const harness = createStreamingHarness();
  const send = harness.store.getState().sendMessage("keep working");
  await Promise.resolve();

  harness.store.getState().detachSession();
  await send;

  assert.equal(harness.cancelCalls(), 0);
  assert.equal(harness.store.getState().isStreaming, false);
  harness.store.getState().cleanup();
});

test("explicit Stop still cancels server work", async () => {
  const harness = createStreamingHarness();
  const send = harness.store.getState().sendMessage("stop this");
  await Promise.resolve();

  harness.store.getState().stopStreaming();
  assert.equal(harness.store.getState().isStreaming, true);
  await send;

  for (let attempt = 0; attempt < 100 && harness.store.getState().isStreaming; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 5));
  }

  assert.equal(harness.cancelCalls(), 1);
  assert.equal(harness.store.getState().isStreaming, false);
  harness.store.getState().cleanup();
});
