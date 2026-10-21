import { createSessionStore } from "../src/session/store.ts";
import type { StreamCallbacks } from "@mitsuro/api";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function equal(actual: unknown, expected: unknown, message: string) {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${expected}, got ${actual}`);
  }
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

const hostTimeout = globalThis.setTimeout;
async function until(condition: () => boolean) {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (condition()) return;
    await new Promise((resolve) => hostTimeout(resolve, 5));
  }
  assert(
    condition(),
    "asynchronous Stop settlement did not reach the expected state",
  );
}

function pollingClock() {
  const setTimeout = globalThis.setTimeout;
  const clearTimeout = globalThis.clearTimeout;
  const polls = new Map<number, () => Promise<void>>();
  let next = 1;
  globalThis.setTimeout = ((callback: () => Promise<void>, delay?: number) => {
    if ((delay ?? 0) < 3000) return setTimeout(callback, delay);
    const id = next++;
    polls.set(id, callback);
    return id as unknown as ReturnType<typeof setTimeout>;
  }) as typeof setTimeout;
  globalThis.clearTimeout = ((id: ReturnType<typeof setTimeout>) => {
    if (!polls.delete(Number(id))) clearTimeout(id);
  }) as typeof clearTimeout;
  return {
    get count() {
      return polls.size;
    },
    async tick() {
      const entry = [...polls.entries()].at(-1);
      assert(entry, "expected an authoritative state poll");
      polls.delete(entry[0]);
      await entry[1]();
    },
    restore() {
      globalThis.setTimeout = setTimeout;
      globalThis.clearTimeout = clearTimeout;
    },
  };
}

function state(agent_state: string, text = "") {
  return {
    id: "session-1",
    agent_state,
    mode: "build",
    permission_mode: "autonomous",
    recovery: null,
    pending_interactions: [],
    delegated_tools: [],
    recent_delegated_runs: [],
    last_event_sequence: null,
    live_partial_assistant: text
      ? { text, thinking: "", tool_calls: [] }
      : null,
  };
}

function response(id = "session-1") {
  return {
    session: {
      id,
      title: id,
      session_type: "code",
      mode: "build",
      model: null,
      token_count: 0,
    },
    messages: [{
      role: "user",
      content: [{ type: "text", text: "Please begin" }],
    }],
  };
}

function awaitingInputState() {
  return {
    ...state("awaiting_input", "Choose how to continue."),
    pending_interactions: [{
      kind: "ask_user_question",
      tool_call_id: "question-1",
      questions: [{
        header: "Next step",
        question: "Continue the work?",
        options: [{ label: "Continue" }],
        multi_select: false,
      }],
    }],
  };
}

function fixture(overrides: Record<string, unknown> = {}) {
  let cancellations = 0;
  let reloads = 0;
  let snapshot = state("idle");
  const client = {
    cancelSession: async () => {
      cancellations++;
    },
    getSession: async (id: string) => {
      reloads++;
      return response(id);
    },
    getSessionState: async () => snapshot,
    updateSession: async () => ({}),
    setCurrentModel: async () => ({}),
    ...overrides,
  };
  const data = new Map<string, string>();
  const store = createSessionStore(
    client as never,
    {
      get: (key) => data.get(key) ?? null,
      set: (key, value) => {
        data.set(key, value);
      },
      delete: (key) => {
        data.delete(key);
      },
    },
    {
      getState: () => ({
        directory: null,
        mode: "neutral",
        initFromSession() {},
        clear() {},
      }),
    } as never,
    { getState: () => ({ loadSessions() {} }) } as never,
    {
      getState: () => ({ setVisible() {}, setItems() {}, setWorkflow() {} }),
    } as never,
  );
  store.getState().initSession("session-1", "Code", undefined, "code");
  store.setState({
    isStreaming: true,
    messages: [{
      id: "local-partial",
      role: "assistant",
      content: "local answer",
      kind: "live_partial",
    }],
  });
  return {
    store,
    client,
    get cancellations() {
      return cancellations;
    },
    get reloads() {
      return reloads;
    },
    set snapshot(value: ReturnType<typeof state>) {
      snapshot = value;
    },
  };
}

async function assertSendPending(store: ReturnType<typeof createSessionStore>) {
  let message = "";
  try {
    await store.getState().sendMessage("Do another thing");
  } catch (error) {
    message = (error as Error).message;
  }
  assert(
    message.includes("still stopping"),
    "another send must remain blocked until Stop is resolved",
  );
}

Deno.test("ordinary Stop stays pending through a delayed receipt and rejects a duplicate send", async () => {
  const receipt = deferred<void>();
  let count = 0;
  const { store } = fixture({
    cancelSession: () => {
      count++;
      return receipt.promise;
    },
  });
  try {
    store.getState().stopStreaming();
    store.getState().stopStreaming();
    equal(count, 1, "duplicate clicks share the pending request");
    equal(
      store.getState().isStreaming,
      true,
      "local detach must not claim server termination",
    );
    equal(
      store.getState().isLoading,
      true,
      "the pending receipt remains visible as work in progress",
    );
    equal(
      store.getState().messages[0].content,
      "local answer",
      "ordinary Stop retains visible prose",
    );
    await assertSendPending(store);
    for (
      const resume of [
        () => store.getState().submitToolResult("question-1", "Proceed"),
        () => store.getState().submitToolApproval("tool-1", true),
      ]
    ) {
      let failure = "";
      try {
        await resume();
      } catch (error) {
        failure = (error as Error).message;
      }
      assert(
        failure.includes("still stopping"),
        "an interaction cannot resume a response whose Stop is unresolved",
      );
    }
    receipt.resolve();
    await until(() => !store.getState().isStreaming);
    equal(store.getState().error, null, "confirmed idle Stop has no error");
  } finally {
    store.getState().cleanup();
  }
});

Deno.test("detaching the real stream cannot let its delayed callbacks settle ordinary Stop", async () => {
  const receipt = deferred<void>();
  const attached = deferred<StreamCallbacks>();
  let detached = false;
  const f = fixture({
    cancelSession: () => receipt.promise,
    streamChat: async (
      _request: unknown,
      callbacks: StreamCallbacks,
      signal: AbortSignal,
    ) => {
      attached.resolve(callbacks);
      await new Promise<void>((resolve) =>
        signal.addEventListener("abort", () => resolve(), { once: true })
      );
      detached = true;
      callbacks.onTextDelta("stale provider tail");
      callbacks.onFinish("session-1", "completed");
    },
  });
  try {
    f.store.setState({ isStreaming: false });
    const sending = f.store.getState().sendMessage("Please begin");
    const callbacks = await attached.promise;
    callbacks.onTextDelta("visible provider text");
    await until(() =>
      f.store.getState().messages.some((m) =>
        m.content === "visible provider text"
      )
    );
    f.store.getState().stopStreaming();
    await sending;
    assert(detached, "Stop must detach the local SSE transport");
    equal(
      f.store.getState().isStreaming,
      true,
      "local stream cleanup cannot settle the pending remote cancellation",
    );
    assert(
      !f.store.getState().messages.some((m) =>
        m.content.includes("stale provider tail")
      ),
      "late provider text stays fenced",
    );
    receipt.resolve();
    await until(() => !f.store.getState().isStreaming);
  } finally {
    f.store.getState().cleanup();
  }
});

Deno.test("rejected ordinary Stop restores the running transcript and exposes the exact failure", async () => {
  const clock = pollingClock();
  const f = fixture({
    cancelSession: async () => {
      throw new Error("cancel transport unavailable");
    },
  });
  f.snapshot = state("streaming", "authoritative partial");
  try {
    f.store.getState().stopStreaming();
    await until(() =>
      f.store.getState().messages.some((m) =>
        m.content === "authoritative partial"
      )
    );
    equal(f.reloads, 1, "a rejected cancel must reload canonical history");
    equal(f.store.getState().isStreaming, true, "the server is still running");
    equal(
      f.store.getState().error,
      "cancel transport unavailable",
      "failure is actionable",
    );
    assert(clock.count > 0, "the detached run must remain observed");
    // A second observer reaches the same running state from the same authority.
    const observer = fixture(f.client);
    try {
      await observer.store.getState().loadSession("session-1", true);
      equal(
        observer.store.getState().isStreaming,
        true,
        "another client must also observe the un-cancelled run",
      );
    } finally {
      observer.store.getState().cleanup();
    }
  } finally {
    f.store.getState().cleanup();
    clock.restore();
  }
});

Deno.test("accepted Stop remains pending while the server drains and settles at the canonical terminal state", async () => {
  const clock = pollingClock();
  const f = fixture();
  f.snapshot = state("tool_executing", "finishing cleanup");
  try {
    f.store.getState().stopStreaming();
    await until(() => clock.count > 0);
    equal(
      f.store.getState().isStreaming,
      true,
      "a cancel receipt precedes terminal cleanup",
    );
    await assertSendPending(f.store);
    f.snapshot = state("idle");
    await clock.tick();
    equal(
      f.store.getState().isStreaming,
      false,
      "idle is authoritative completion",
    );
    equal(clock.count, 0, "terminal cleanup stops observation");
  } finally {
    f.store.getState().cleanup();
    clock.restore();
  }
});

Deno.test("accepted Stop settles a finished awaiting-input response and preserves its actionable question", async () => {
  const clock = pollingClock();
  let submitted: { session_id: string; tool_call_id: string; result: string } | null = null;
  const f = fixture({
    getSessionState: async () => awaitingInputState(),
    streamToolResult: async (
      request: NonNullable<typeof submitted>,
      callbacks: StreamCallbacks,
    ) => {
      submitted = request;
      callbacks.onFinish("session-1", "completed");
    },
  });
  try {
    f.store.getState().stopStreaming();
    await until(() => !f.store.getState().isStreaming);
    equal(f.cancellations, 1, "the accepted cancellation is still sent once");
    equal(f.store.getState().isLoading, false, "the question is not shown as pending work");
    equal(f.store.getState().error, null, "an already-finished response is not a cancel failure");
    equal(clock.count, 0, "a settled question does not retain Stop polling");
    const question = f.store.getState().messages.flatMap((message) => message.toolCalls ?? [])
      .find((tool) => tool.id === "question-1");
    assert(question, "canonical pending interaction must remain visible");
    equal(question.name, "AskUserQuestion", "the question retains its interaction type");
    equal(question.status, "awaiting_approval", "the question remains actionable");
    equal((question.arguments as { questions: { question: string }[] }).questions[0].question,
      "Continue the work?", "the original prompt is preserved");
    await f.store.getState().submitToolResult("question-1", "Continue");
    assert(submitted, "answering must reach the real store continuation path after Stop settles");
    equal((submitted as { session_id: string }).session_id, "session-1", "the answer targets its session");
  } finally {
    f.store.getState().cleanup();
    clock.restore();
  }
});

Deno.test("ordinary Stop keeps a live approval pending and can later settle awaiting input through polling", async () => {
  const clock = pollingClock();
  let waitingForQuestion = false;
  let approvals = 0;
  const f = fixture({
    getSessionState: async () => waitingForQuestion ? awaitingInputState() : {
      ...state("tool_executing"),
      pending_interactions: [{
        kind: "tool_approval",
        tool_call: { id: "approval-1", name: "Bash", arguments: { value: { command: "true" } } },
      }],
    },
    submitToolApproval: async () => { approvals++; },
  });
  try {
    f.store.getState().stopStreaming();
    await until(() => clock.count > 0 && !f.store.getState().isLoading);
    equal(f.store.getState().isStreaming, true, "a live approval wait is not terminal cancellation evidence");
    await assertSendPending(f.store);
    let failure = "";
    try { await f.store.getState().submitToolApproval("approval-1", true); }
    catch (error) { failure = (error as Error).message; }
    assert(failure.includes("still stopping"), "the in-flight approval cannot race Stop");
    equal(approvals, 0, "pending cancellation cannot issue a tool approval");
    waitingForQuestion = true;
    await clock.tick();
    equal(f.store.getState().isStreaming, false, "an authoritative finished question settles the pending Stop");
    equal(clock.count, 0, "settled actionable state stops polling");
    assert(f.store.getState().messages.some((message) => message.toolCalls?.some((tool) => tool.id === "question-1")),
      "poll settlement retains the new canonical question");
  } finally {
    f.store.getState().cleanup();
    clock.restore();
  }
});

Deno.test("a late awaiting-input Stop snapshot cannot overwrite another session", async () => {
  const snapshot = deferred<ReturnType<typeof awaitingInputState>>();
  let requested = false;
  const f = fixture({
    getSessionState: () => { requested = true; return snapshot.promise; },
  });
  try {
    f.store.getState().stopStreaming();
    await until(() => requested);
    f.store.getState().initSession("session-2", "Other", undefined, "code");
    f.store.setState({ isStreaming: true, error: "other session state" });
    snapshot.resolve(awaitingInputState());
    await new Promise((resolve) => hostTimeout(resolve, 15));
    equal(f.store.getState().sessionId, "session-2", "late actionable state cannot navigate back");
    equal(f.store.getState().isStreaming, true, "another session's run remains active");
    equal(f.store.getState().error, "other session state", "late Stop cannot replace another session's error");
    assert(!f.store.getState().messages.some((message) => message.toolCalls?.some((tool) => tool.id === "question-1")),
      "late question metadata stays in its original session");
  } finally {
    f.store.getState().cleanup();
  }
});

Deno.test("a delayed rejected Stop cannot affect a newer attachment to the same session", async () => {
  const receipt = deferred<void>();
  const f = fixture({ cancelSession: () => receipt.promise });
  try {
    f.store.getState().stopStreaming();
    f.store.getState().initSession("session-2", "Other", undefined, "code");
    f.store.getState().initSession("session-1", "Back", undefined, "code");
    f.store.setState({ isStreaming: true, error: "new attachment error" });
    receipt.reject(new Error("stale cancel rejection"));
    await new Promise((resolve) => hostTimeout(resolve, 15));
    equal(f.reloads, 0, "stale settlement cannot reload the newer attachment");
    equal(
      f.store.getState().isStreaming,
      true,
      "new attachment state remains intact",
    );
    equal(
      f.store.getState().error,
      "new attachment error",
      "stale error cannot cross the generation fence",
    );
  } finally {
    f.store.getState().cleanup();
  }
});

Deno.test("navigation fences a Stop reload already in flight", async () => {
  const reload = deferred<ReturnType<typeof response>>();
  let requested = false;
  const f = fixture({
    getSession: () => {
      requested = true;
      return reload.promise;
    },
  });
  try {
    f.store.getState().stopStreaming();
    await until(() => requested);
    f.store.getState().initSession("session-2", "Other", undefined, "code");
    f.store.setState({ isStreaming: true, error: "other session state" });
    reload.resolve(response());
    await new Promise((resolve) => hostTimeout(resolve, 15));
    equal(
      f.store.getState().sessionId,
      "session-2",
      "reload cannot switch sessions back",
    );
    equal(
      f.store.getState().isStreaming,
      true,
      "reload cannot stop another conversation",
    );
    equal(
      f.store.getState().error,
      "other session state",
      "reload cannot overwrite another session's error",
    );
  } finally {
    f.store.getState().cleanup();
  }
});

Deno.test("a failed Stop and unavailable snapshot remain pending until polling recovers canonical state", async () => {
  const clock = pollingClock();
  let available = false;
  const f = fixture({
    cancelSession: async () => {
      throw new Error("cancel failed");
    },
    getSession: async () => {
      if (!available) throw new Error("history offline");
      return response();
    },
    getSessionState: async () => {
      if (!available) throw new Error("status offline");
      return state("streaming", "still working");
    },
  });
  try {
    f.store.getState().stopStreaming();
    await until(() => clock.count > 0);
    equal(
      f.store.getState().isStreaming,
      true,
      "unavailable state must not be interpreted as idle",
    );
    equal(
      f.store.getState().error,
      "cancel failed",
      "the original cancellation failure is retained",
    );
    await assertSendPending(f.store);
    available = true;
    await clock.tick();
    assert(
      f.store.getState().messages.some((m) => m.content === "still working"),
      "polling restores the authoritative running partial",
    );
    equal(
      f.store.getState().error,
      "cancel failed",
      "recovery does not silently hide a rejected Stop",
    );
  } finally {
    f.store.getState().cleanup();
    clock.restore();
  }
});

Deno.test("a snapshot started before Stop's receipt cannot settle it", async () => {
  const clock = pollingClock();
  const oldStatus = deferred<ReturnType<typeof state>>();
  let stateReads = 0;
  const f = fixture({
    getSessionState: () =>
      ++stateReads === 1
        ? oldStatus.promise
        : Promise.resolve(state("streaming", "server still draining")),
  });
  try {
    const oldLoad = f.store.getState().loadSession("session-1", true);
    f.store.getState().stopStreaming();
    oldStatus.resolve(state("idle"));
    await oldLoad;
    await until(() => stateReads === 2 && clock.count > 0);
    equal(
      f.store.getState().isStreaming,
      true,
      "only a post-receipt snapshot may settle Stop",
    );
    await assertSendPending(f.store);
  } finally {
    f.store.getState().cleanup();
    clock.restore();
  }
});
