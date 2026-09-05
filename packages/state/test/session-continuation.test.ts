import { createSessionStore } from "../src/session/store.ts";
import { STATE_POLL_DEGRADED_MESSAGE } from "../src/session/constants.ts";
import type { ChatMessage, ToolCall } from "../src/session/types.ts";
import { MitsuroApiError } from "@mitsuro/api";

declare const Deno: {
	test(name: string, fn: () => void | Promise<void>): void;
};

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) {
		throw new Error(message);
	}
}

function assertEquals<T>(actual: T, expected: T, message: string) {
	if (!Object.is(actual, expected)) {
		throw new Error(
			`${message}\nexpected: ${String(expected)}\nactual: ${String(actual)}`,
		);
	}
}

function assertDeepEquals(actual: unknown, expected: unknown, message: string) {
	const actualJson = JSON.stringify(actual);
	const expectedJson = JSON.stringify(expected);
	if (actualJson !== expectedJson) {
		throw new Error(
			`${message}\nexpected: ${expectedJson}\nactual: ${actualJson}`,
		);
	}
}

type IntervalCallback = () => void | Promise<void>;

function installFakeIntervals() {
	const originalSetInterval = globalThis.setInterval;
	const originalClearInterval = globalThis.clearInterval;
	const originalSetTimeout = globalThis.setTimeout;
	const originalClearTimeout = globalThis.clearTimeout;
	let nextId = 1;
	const polling = new Map<number, { callback: IntervalCallback; delay: number }>();
	const other = new Map<number, IntervalCallback>();

	globalThis.setInterval = ((callback: IntervalCallback, delay?: number) => {
		const id = nextId++;
		if (delay === 3000) {
			polling.set(id, { callback, delay });
		} else {
			other.set(id, callback);
		}
		return id as unknown as ReturnType<typeof setInterval>;
	}) as typeof setInterval;

	globalThis.clearInterval = ((id?: ReturnType<typeof setInterval>) => {
		const key = Number(id);
		polling.delete(key);
		other.delete(key);
	}) as typeof clearInterval;

	globalThis.setTimeout = ((callback: IntervalCallback, delay?: number) => {
		const id = nextId++;
		if ((delay ?? 0) >= 3000) {
			polling.set(id, { callback, delay: delay ?? 0 });
		} else {
			other.set(id, callback);
		}
		return id as unknown as ReturnType<typeof setTimeout>;
	}) as typeof setTimeout;

	globalThis.clearTimeout = ((id?: ReturnType<typeof setTimeout>) => {
		const key = Number(id);
		polling.delete(key);
		other.delete(key);
	}) as typeof clearTimeout;

	function startLatestPollingTick(): void | Promise<void> {
		const entries = Array.from(polling.entries());
		const entry = entries[entries.length - 1];
		const callback = entry?.[1].callback;
		assert(callback, "expected an active session-state polling interval");
		polling.delete(entry[0]);
		return callback();
	}

	return {
		activePollingCount: () => polling.size,
		latestPollingDelay: () => Array.from(polling.values()).at(-1)?.delay ?? null,
		startLatestPollingTick,
		async runLatestPollingTick() {
			await startLatestPollingTick();
		},
		restore() {
			globalThis.setInterval = originalSetInterval;
			globalThis.clearInterval = originalClearInterval;
			globalThis.setTimeout = originalSetTimeout;
			globalThis.clearTimeout = originalClearTimeout;
		},
	};
}

function createStorage() {
	const data = new Map<string, string>();
	return {
		get: (key: string) => data.get(key) ?? null,
		set: (key: string, value: string) => {
			data.set(key, value);
		},
		delete: (key: string) => {
			data.delete(key);
		},
	};
}

function createWorkspace() {
	return {
		getState: () => ({
			directory: null,
			mode: "neutral" as const,
			initFromSession: () => {},
			clear: () => {},
		}),
	};
}

function createSessionsStore() {
	let loadCount = 0;
	return {
		getState: () => ({
			loadSessions: () => {
				loadCount += 1;
			},
		}),
		get loadCount() {
			return loadCount;
		},
	};
}

function createPlanStore() {
	let visible = false;
	return {
		getState: () => ({
			setVisible: (...args: [boolean]) => {
				const [nextVisible] = args;
				visible = nextVisible;
			},
			setItems: () => {},
			setWorkflow: () => {},
		}),
		get visible() {
			return visible;
		},
	};
}

function sessionState(
	agentState: string,
	overrides: Record<string, unknown> = {},
) {
	return {
		id: "session-1",
		agent_state: agentState,
		started_at: null,
		last_event_at: null,
		mode: "build",
		permission_mode: "autonomous",
		recovery: null,
		live_partial_assistant: null,
		pending_interactions: [],
		delegated_tools: [],
		recent_delegated_runs: [],
		last_event_sequence: null,
		...overrides,
	};
}

function sessionResponse() {
	return {
		session: {
			id: "session-1",
			title: "Recovered session",
			token_count: 0,
			working_dir: null,
			project_dir: null,
			workspace_mode: "neutral",
			session_type: "chat",
			parent_session_id: null,
			mode: "build",
			permission_mode: "autonomous",
			updated_at: "2026-05-08T00:00:00Z",
			model: null,
			target_branch: null,
		},
		messages: [],
	};
}

Deno.test("streamChat network drop keeps polling, recovers snapshot, and stops at actionable pending approval", async () => {
	const timers = installFakeIntervals();
	try {
		const snapshots = [
			sessionState("streaming", {
				live_partial_assistant: {
					text: "still running after reconnect",
					thinking: "",
					tool_calls: [],
				},
				last_event_sequence: 41,
			}),
			sessionState("awaiting_input", {
				live_partial_assistant: {
					text: "",
					thinking: "",
					tool_calls: [
						{
							id: "tool-approval-1",
							name: "Bash",
							arguments: { value: { command: "npm test -- --watch=false" } },
						},
					],
				},
				pending_interactions: [
					{
						kind: "tool_approval",
						tool_call: {
							id: "tool-approval-1",
							name: "Bash",
							arguments: { value: { command: "npm test -- --watch=false" } },
						},
					},
				],
				last_event_sequence: 42,
			}),
			sessionState("awaiting_input", {
				live_partial_assistant: {
					text: "",
					thinking: "",
					tool_calls: [
						{
							id: "tool-approval-1",
							name: "Bash",
							arguments: { value: { command: "npm test -- --watch=false" } },
						},
					],
				},
				pending_interactions: [
					{
						kind: "tool_approval",
						tool_call: {
							id: "tool-approval-1",
							name: "Bash",
							arguments: { value: { command: "npm test -- --watch=false" } },
						},
					},
				],
				last_event_sequence: 42,
			}),
		];

		let getSessionCount = 0;
		const client = {
			streamChat: async (
				_request: unknown,
				callbacks: { onTextDelta: (delta: string) => void },
			) => {
				callbacks.onTextDelta("partial before drop");
				throw new Error("network connection dropped");
			},
			getSessionState: async () => {
				const snapshot = snapshots.shift();
				assert(snapshot, "expected a queued session-state snapshot");
				return snapshot;
			},
			getSession: async () => {
				getSessionCount += 1;
				return sessionResponse();
			},
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};

		const sessionsStore = createSessionsStore();
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			sessionsStore as never,
			createPlanStore() as never,
		);

		store.getState().initSession("session-1", "Recovered session");
		await store.getState().sendMessage("keep going");

		assertEquals(
			store.getState().isStreaming,
			true,
			"SSE drop should keep the active session in a tracked streaming state while the server snapshot is still streaming",
		);
		assertEquals(
			store.getState().error,
			null,
			"recoverable SSE drop should not leave a stale user-facing error while the server run is still active",
		);
		assertEquals(
			timers.activePollingCount(),
			1,
			"SSE drop should keep or restart exactly one session-state polling interval",
		);
		assertEquals(
			store.getState().lastEventSequence,
			41,
			"the recovery snapshot should update lastEventSequence",
		);

		await timers.runLatestPollingTick();

		assertEquals(
			timers.activePollingCount(),
			0,
			"polling should stop after reaching an actionable awaiting_input snapshot",
		);
		assertEquals(
			store.getState().isStreaming,
			false,
			"awaiting input is actionable and should not keep the transcript in streaming mode",
		);

		const recoveredTool = store
			.getState()
			.messages.flatMap((message: ChatMessage) => message.toolCalls ?? [])
			.find((toolCall: ToolCall) => toolCall.id === "tool-approval-1");

		assert(
			recoveredTool,
			"expected pending approval tool call to be restored from the server snapshot",
		);
		assertEquals(
			recoveredTool.status,
			"awaiting_approval",
			"pending approval should render as an actionable approval widget after recovery",
		);
		assertDeepEquals(
			recoveredTool.arguments,
			{ command: "npm test -- --watch=false" },
			"pending approval should expose the recovered tool argument preview to the widget",
		);
		assertEquals(
			getSessionCount > 0,
			true,
			"reaching an actionable state should refresh the persisted transcript once",
		);
	} finally {
		timers.restore();
	}
});

Deno.test("stream and snapshot failure keeps bounded recovery polling alive", async () => {
	const timers = installFakeIntervals();
	try {
		let snapshotAttempts = 0;
		const client = {
			streamChat: async () => {
				throw new Error("SSE transport lost");
			},
			getSessionState: async () => {
				snapshotAttempts += 1;
				if (snapshotAttempts === 1) {
					throw new Error("snapshot endpoint temporarily unavailable");
				}
				return sessionState("idle");
			},
			getSession: async () => sessionResponse(),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("session-1", "Double transport failure");

		await store.getState().sendMessage("keep recovering");

		assertEquals(
			store.getState().error,
			STATE_POLL_DEGRADED_MESSAGE,
			"a failed snapshot recovery must be visible without declaring the run dead",
		);
		assertEquals(
			store.getState().isStreaming,
			true,
			"the uncertain remote run stays protected from duplicate sends",
		);
		assertEquals(
			timers.activePollingCount(),
			1,
			"snapshot transport failure should retain one bounded recovery poll",
		);

		await timers.runLatestPollingTick();
		assertEquals(
			timers.activePollingCount(),
			0,
			"canonical idle state ends recovery polling",
		);
		assertEquals(store.getState().error, null, "healthy state clears degraded warning");
	} finally {
		timers.restore();
	}
});

Deno.test("lagged stream reloads the canonical session after finish", async () => {
	let getSessionCount = 0;
	const client = {
		streamChat: async (
			_request: unknown,
			callbacks: {
				onLagged?: (skipped: number) => void;
				onFinish: (sessionId: string) => void;
			},
		) => {
			callbacks.onLagged?.(3);
			callbacks.onFinish("session-1");
		},
		getSession: async () => {
			getSessionCount += 1;
			return sessionResponse();
		},
		getSessionState: async () => sessionState("idle"),
		heartbeatSessionPresence: async () => ({}),
		removeSessionPresence: async () => ({}),
		updateSession: async () => ({}),
		setCurrentModel: async () => ({}),
	};

	const store = createSessionStore(
		client as never,
		createStorage(),
		createWorkspace() as never,
		createSessionsStore() as never,
		createPlanStore() as never,
	);
	store.getState().initSession("session-1", "Lag recovery");

	await store.getState().sendMessage("continue");
	await new Promise((resolve) => setTimeout(resolve, 0));

	assertEquals(
		getSessionCount,
		1,
		"a lag signal should force one canonical session reload after finish",
	);
});

Deno.test("Worker Stop suppresses the exact provisional response until its run settles", async () => {
	let callbacks:
		| {
			onWorkerResponsePending?: (event: {
				type: "worker_response_pending";
				worker_id: string;
				session_id: string;
				run_id: string;
			}) => void;
		}
		| undefined;
	let resolveStreamStarted!: () => void;
	const streamStarted = new Promise<void>((resolve) => {
		resolveStreamStarted = resolve;
	});
	let cancelCount = 0;
	let getSessionCount = 0;
	let stateCount = 0;
	let statusCount = 0;
	const client = {
		streamChat: async (
			_request: unknown,
			streamCallbacks: typeof callbacks,
			signal: AbortSignal,
		) => {
			callbacks = streamCallbacks;
			resolveStreamStarted();
			await new Promise<void>((resolve) => {
				signal.addEventListener("abort", () => resolve(), { once: true });
			});
		},
		cancelSession: async () => {
			cancelCount += 1;
		},
		getSession: async () => {
			getSessionCount += 1;
			const response = sessionResponse();
			return {
				...response,
				session: { ...response.session, session_type: "hive" },
			};
		},
		getSessionState: async () => {
			stateCount += 1;
			return stateCount === 1
				? sessionState("streaming", {
					live_partial_assistant: {
						text: "chopped provider text",
						thinking: "",
						tool_calls: [],
					},
				})
				: sessionState("idle");
		},
		getHiveSessionStatus: async () => {
			statusCount += 1;
			return {
				session_id: "session-1",
				session_type: "hive",
				title: "Worker DM",
				tasks: [],
				agent_state: statusCount === 1 ? "streaming" : "idle",
				runtime: {
					session_id: "session-1",
					status: statusCount === 1 ? "running" : "cancelled",
					current_run_id: "run-1",
					priority: "normal",
					updated_at: "2026-08-25T04:00:00.000000Z",
				},
				cadence: {},
			};
		},
		heartbeatSessionPresence: async () => ({}),
		removeSessionPresence: async () => ({}),
		updateSession: async () => ({}),
		setCurrentModel: async () => ({}),
	};
	const store = createSessionStore(
		client as never,
		createStorage(),
		createWorkspace() as never,
		createSessionsStore() as never,
		createPlanStore() as never,
	);
	store.getState().initSession("session-1", "Worker DM", undefined, "hive");
	const send = store.getState().sendMessage("Please begin");
	await streamStarted;
	assert(callbacks, "Worker stream callbacks should be attached");
	callbacks.onWorkerResponsePending?.({
		type: "worker_response_pending",
		worker_id: "worker-1",
		session_id: "session-1",
		run_id: "run-1",
	});
	store.setState((state) => ({
		messages: [
			...state.messages.filter((message) => message.role === "user"),
			{
				id: "worker-live-partial",
				role: "assistant",
				content: "chopped provider text",
				kind: "live_partial",
			},
		],
	}));

	store.getState().stopStreaming();
	await send;
	assertEquals(
		cancelCount,
		1,
		"Stop should issue one exact cancellation request",
	);
	assert(
		!store.getState().messages.some((message) => message.content.includes("chopped provider text")),
		"the provisional response must disappear before Stop settles",
	);

	for (let attempt = 0; attempt < 50 && getSessionCount < 1; attempt += 1) {
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
	assertEquals(
		getSessionCount,
		1,
		"Stop receipt should trigger an immediate canonical reload",
	);
	assert(
		!store.getState().messages.some((message) => message.content.includes("chopped provider text")),
		"a still-running Stop refresh must keep live_partial suppressed",
	);

	for (let attempt = 0; attempt < 100 && getSessionCount < 2; attempt += 1) {
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
	assertEquals(
		statusCount,
		2,
		"the exact stopped run should be polled through terminal state",
	);
	assertEquals(
		getSessionCount,
		2,
		"terminal state should trigger one final canonical reload",
	);
	assert(
		!store.getState().messages.some((message) => message.content.includes("chopped provider text")),
		"terminal rehydration must never promote the discarded provider draft",
	);
	store.getState().cleanup();
});

Deno.test("rejected Worker Stop restores authoritative partial state and exposes the error", async () => {
	let callbacks:
		| {
			onWorkerResponsePending?: (event: {
				type: "worker_response_pending";
				worker_id: string;
				session_id: string;
				run_id: string;
			}) => void;
		}
		| undefined;
	let resolveStreamStarted!: () => void;
	const streamStarted = new Promise<void>((resolve) => {
		resolveStreamStarted = resolve;
	});
	let reloadCount = 0;
	let statusCount = 0;
	const client = {
		streamChat: async (
			_request: unknown,
			streamCallbacks: typeof callbacks,
			signal: AbortSignal,
		) => {
			callbacks = streamCallbacks;
			resolveStreamStarted();
			await new Promise<void>((resolve) => {
				signal.addEventListener("abort", () => resolve(), { once: true });
			});
		},
		cancelSession: async () => {
			throw new Error("Hive daemon is unavailable");
		},
		getSession: async () => {
			reloadCount += 1;
			const response = sessionResponse();
			return {
				...response,
				session: { ...response.session, session_type: "hive" },
			};
		},
		getSessionState: async () =>
			sessionState("streaming", {
				live_partial_assistant: {
					text: "authoritative running partial",
					thinking: "",
					tool_calls: [],
				},
			}),
		getHiveSessionStatus: async () => {
			statusCount += 1;
			return {};
		},
		heartbeatSessionPresence: async () => ({}),
		removeSessionPresence: async () => ({}),
		updateSession: async () => ({}),
		setCurrentModel: async () => ({}),
	};
	const store = createSessionStore(
		client as never,
		createStorage(),
		createWorkspace() as never,
		createSessionsStore() as never,
		createPlanStore() as never,
	);
	store.getState().initSession("session-1", "Worker DM", undefined, "hive");
	const send = store.getState().sendMessage("Please begin");
	await streamStarted;
	assert(callbacks, "Worker stream callbacks should be attached");
	callbacks.onWorkerResponsePending?.({
		type: "worker_response_pending",
		worker_id: "worker-1",
		session_id: "session-1",
		run_id: "run-1",
	});
	store.setState((state) => ({
		messages: [
			...state.messages.filter((message) => message.role === "user"),
			{
				id: "untrusted-local-partial",
				role: "assistant",
				content: "untrusted local partial",
				kind: "live_partial",
			},
		],
	}));

	store.getState().stopStreaming();
	await send;
	for (
		let attempt = 0;
		attempt < 50 && !store.getState().error;
		attempt += 1
	) {
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
	assertEquals(
		reloadCount,
		1,
		"a rejected Stop must reload canonical running state",
	);
	assertEquals(
		statusCount,
		0,
		"a rejected Stop must not enter terminal settlement polling",
	);
	assert(
		store.getState().messages.some((message) =>
			message.kind === "live_partial" &&
			message.content === "authoritative running partial"
		),
		"the authoritative still-running partial should be restored as provisional",
	);
	assert(
		store.getState().error?.includes("Hive daemon is unavailable"),
		"the exact Stop rejection must remain actionable",
	);
	store.getState().cleanup();
});

Deno.test("primary Hive Stop retains generic transient finalization", async () => {
	let cancelCount = 0;
	let reloadCount = 0;
	let workerStatusCount = 0;
	const client = {
		cancelSession: async () => {
			cancelCount += 1;
		},
		getSession: async () => {
			reloadCount += 1;
			return sessionResponse();
		},
		getSessionState: async () => sessionState("idle"),
		getHiveSessionStatus: async () => {
			workerStatusCount += 1;
			return {};
		},
	};
	const store = createSessionStore(
		client as never,
		createStorage(),
		createWorkspace() as never,
		createSessionsStore() as never,
		createPlanStore() as never,
	);
	store.getState().initSession("session-1", "Primary Hive", undefined, "hive");
	store.setState({
		isStreaming: true,
		messages: [{
			id: "primary-live-partial",
			role: "assistant",
			content: "ordinary Hive partial",
			kind: "live_partial",
		}],
	});

	store.getState().stopStreaming();
	assertEquals(
		cancelCount,
		1,
		"primary Hive Stop should retain generic cancellation",
	);
	assertEquals(
		store.getState().messages[0]?.kind,
		undefined,
		"ordinary Hive partials retain the existing generic finalization behavior",
	);
	for (let attempt = 0; attempt < 50 && store.getState().isStreaming; attempt += 1) {
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
	assertEquals(reloadCount, 1, "ordinary Hive Stop also reconciles canonical state");
	assertEquals(store.getState().isStreaming, false, "the ordinary response settles at idle");
	assertEquals(
		workerStatusCount,
		0,
		"session_type=hive alone must not enter exact Worker run settlement",
	);
	store.getState().cleanup();
});

Deno.test("provider error snapshot stops polling and preserves the provider error", async () => {
	const timers = installFakeIntervals();
	try {
		const providerError =
			'AI error: API error: 402 Payment Required - {"error":"Grok Build usage balance exhausted"}';
		let getSessionCount = 0;
		const failedState = sessionState("error", {
			recovery: {
				schema_version: 1,
				status: "interrupted",
				stop_reason: "provider_error",
				last_error: providerError,
				partial_assistant: {
					text: "Useful answer emitted before the provider error.",
					thinking: "",
					tool_calls: [],
				},
				decision: { kind: "resumable", latest_user_objective: "Sir?" },
			},
		});
		const client = {
			streamChat: async (
				_request: unknown,
				callbacks: { onError: (error: string) => void },
			) => {
				callbacks.onError("provider request failed");
			},
			getSessionState: async () => failedState,
			getSession: async () => {
				getSessionCount += 1;
				return sessionResponse();
			},
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};

		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("session-1", "Provider failure");

		await store.getState().sendMessage("Sir?");

		assertEquals(
			store.getState().isStreaming,
			false,
			"the server's error state must terminate mobile streaming",
		);
		assertEquals(
			timers.activePollingCount(),
			0,
			"the server's error state must stop session polling",
		);
		assertEquals(
			store.getState().error,
			providerError,
			"canonical provider detail must survive the recovery reload for the visible error banner",
		);
		assertEquals(
			getSessionCount,
			1,
			"provider failure recovery should refresh persisted messages once",
		);
		assert(
			store.getState().messages.some(
				(message: ChatMessage) =>
					message.role === "assistant"
					&& message.content === "Useful answer emitted before the provider error.",
			),
			"a provider error must not remove assistant text already captured in the recovery checkpoint",
		);
	} finally {
		timers.restore();
	}
});

Deno.test("healthy terminal snapshot clears a stale provider error", async () => {
	const timers = installFakeIntervals();
	try {
		const client = {
			getSessionState: async () => sessionState("idle"),
			getSession: async () => sessionResponse(),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("session-1", "Healthy recovery");
		store.setState({ error: "stale provider failure" });
		store.getState().startStatePolling("session-1");

		await timers.runLatestPollingTick();

		assertEquals(
			store.getState().error,
			null,
			"a healthy idle snapshot should clear an error from the previous turn",
		);
		assertEquals(
			timers.activePollingCount(),
			0,
			"a healthy idle snapshot should stop state polling",
		);
	} finally {
		timers.restore();
	}
});

Deno.test("state polling backs off through transient failures and clears the visible degraded state", async () => {
	const timers = installFakeIntervals();
	try {
		let attempts = 0;
		const client = {
			getSessionState: async () => {
				attempts += 1;
				if (attempts <= 2) throw new Error("temporary network failure");
				return sessionState("streaming");
			},
			getSession: async () => sessionResponse(),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("session-1", "Polling recovery");
		store.getState().startStatePolling("session-1");

		assertEquals(timers.latestPollingDelay(), 3000, "first poll uses base delay");
		await timers.runLatestPollingTick();
		assertEquals(
			timers.latestPollingDelay(),
			3000,
			"first retry remains at the base delay",
		);
		assertEquals(store.getState().error, null, "one missed poll is not yet degraded");

		await timers.runLatestPollingTick();
		assertEquals(
			timers.latestPollingDelay(),
			6000,
			"consecutive failures use exponential backoff",
		);
		assertEquals(
			store.getState().error,
			STATE_POLL_DEGRADED_MESSAGE,
			"repeated failures expose a non-terminal degraded state",
		);

		await timers.runLatestPollingTick();
		assertEquals(
			store.getState().error,
			null,
			"a healthy snapshot clears the polling-only degraded warning",
		);
		assertEquals(timers.latestPollingDelay(), 3000, "success resets backoff");
		store.getState().stopStatePolling();
	} finally {
		timers.restore();
	}
});

Deno.test("state polling stops after its bounded retry budget with an actionable warning", async () => {
	const timers = installFakeIntervals();
	try {
		const client = {
			getSessionState: async () => {
				throw new Error("server unavailable");
			},
			getSession: async () => sessionResponse(),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("session-1", "Polling exhausted");
		store.setState({ isStreaming: true });
		store.getState().startStatePolling("session-1");

		for (let attempt = 0; attempt < 5; attempt += 1) {
			await timers.runLatestPollingTick();
		}

		assertEquals(
			timers.activePollingCount(),
			0,
			"retry exhaustion must not leave a hidden poll running forever",
		);
		assert(
			store.getState().error?.includes("after 5 attempts"),
			"retry exhaustion should be visible and actionable",
		);
		assertEquals(
			store.getState().isStreaming,
			true,
			"an unconfirmed remote run stays protected from duplicate sends",
		);
	} finally {
		timers.restore();
	}
});

Deno.test("a stopped in-flight poll cannot apply stale session state", async () => {
	const timers = installFakeIntervals();
	try {
		const deferred: {
			resolve?: (value: ReturnType<typeof sessionState>) => void;
		} = {};
		const client = {
			getSessionState: () =>
				new Promise<ReturnType<typeof sessionState>>((resolve) => {
					deferred.resolve = resolve;
				}),
			getSession: async () => sessionResponse(),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("session-1", "Stopped poll");
		store.setState({ isStreaming: true, error: "newer state" });
		store.getState().startStatePolling("session-1");

		const inFlight = timers.startLatestPollingTick();
		store.getState().stopStatePolling();
		assert(deferred.resolve, "poll should be waiting on a snapshot");
		deferred.resolve(sessionState("idle"));
		await inFlight;

		assertEquals(
			store.getState().isStreaming,
			true,
			"a response from a cancelled poll generation must not end a newer run",
		);
		assertEquals(
			store.getState().error,
			"newer state",
			"a stale poll must not overwrite newer visible state",
		);
	} finally {
		timers.restore();
	}
});

Deno.test("switching sessions detaches the old stream without cancelling its server run", async () => {
	const timers = installFakeIntervals();
	try {
		let capturedCallbacks:
			| { onTextDelta: (delta: string) => void }
			| undefined;
		let cancelCount = 0;
		const client = {
			streamChat: async (
				_request: unknown,
				callbacks: { onTextDelta: (delta: string) => void },
				signal: AbortSignal,
			) => {
				capturedCallbacks = callbacks;
				await new Promise<void>((resolve) => {
					signal.addEventListener("abort", () => resolve(), { once: true });
				});
			},
			cancelSession: async () => {
				cancelCount += 1;
			},
			getSession: async (sessionId: string) => {
				const response = sessionResponse();
				return {
					...response,
					session: {
						...response.session,
						id: sessionId,
						title: sessionId,
					},
				};
			},
			getSessionState: async (sessionId: string) =>
				sessionState(sessionId === "new-session" ? "streaming" : "idle", {
					id: sessionId,
				}),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("old-session", "Old");

		const oldSend = store.getState().sendMessage("Keep running remotely");
		await Promise.resolve();
		assert(capturedCallbacks, "the old stream should be attached");

		await store.getState().loadSession("new-session", true);
		await oldSend;
		capturedCallbacks.onTextDelta("stale data from old stream");

		assertEquals(
			store.getState().sessionId,
			"new-session",
			"the requested session should remain selected",
		);
		assertEquals(
			store.getState().messages.length,
			0,
			"callbacks from the detached stream must not mutate the new transcript",
		);
		assertEquals(
			cancelCount,
			0,
			"navigation should detach locally without cancelling the remote run",
		);
		assertEquals(
			store.getState().isStreaming,
			true,
			"an active snapshot loaded from notification navigation should resume polling",
		);
		assertEquals(
			timers.activePollingCount(),
			1,
			"the newly selected active session should own exactly one polling timer",
		);
		store.getState().cleanup();
	} finally {
		timers.restore();
	}
});

Deno.test("premature stream end remains visible when the server is idle without a response", async () => {
	const timers = installFakeIntervals();
	try {
		const prematureEnd =
			"Stream ended before the server reported completion. Recovering the session state.";
		const client = {
			streamChat: async (
				_request: unknown,
				callbacks: { onError: (error: string) => void },
			) => {
				callbacks.onError(prematureEnd);
			},
			getSessionState: async () => sessionState("idle"),
			getSession: async () => sessionResponse(),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);
		store.getState().initSession("session-1", "Premature EOF");

		await store.getState().sendMessage("Hello");
		await Promise.resolve();

		assertEquals(
			store.getState().error,
			prematureEnd,
			"an idle snapshot without a response must not swallow the stream lifecycle error",
		);
		assertEquals(
			store.getState().isStreaming,
			false,
			"a terminal stream lifecycle error must restore composer controls",
		);
	} finally {
		timers.restore();
	}
});

Deno.test("direct session load derives provider error and clears it when switching healthy", async () => {
	const timers = installFakeIntervals();
	try {
		const providerError = "Provider quota exhausted";
		const client = {
			getSession: async (sessionId: string) => {
				const response = sessionResponse();
				return {
					...response,
					session: {
						...response.session,
						id: sessionId,
						title: sessionId,
					},
				};
			},
			getSessionState: async (sessionId: string) =>
				sessionId === "failed-session"
					? sessionState("error", {
							id: sessionId,
							recovery: {
								schema_version: 1,
								status: "interrupted",
								stop_reason: "provider_error",
								last_error: providerError,
								partial_assistant: {
									text: "",
									thinking: "",
									tool_calls: [],
								},
								decision: {
									kind: "resumable",
									latest_user_objective: "Hello",
								},
							},
						})
					: sessionState("idle", { id: sessionId }),
			heartbeatSessionPresence: async () => ({}),
			removeSessionPresence: async () => ({}),
			updateSession: async () => ({}),
			setCurrentModel: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);

		await store.getState().loadSession("failed-session");
		assertEquals(
			store.getState().error,
			providerError,
			"directly loading a failed session should expose its canonical recovery error",
		);

		await store.getState().loadSession("healthy-session");
		assertEquals(
			store.getState().sessionId,
			"healthy-session",
			"the healthy session should become active",
		);
		assertEquals(
			store.getState().error,
			null,
			"switching directly to a healthy session must clear the previous provider error",
		);
		store.getState().cleanup();
	} finally {
		timers.restore();
	}
});

Deno.test("a stale persisted session is cleared without discarding its workspace", async () => {
	let workspaceSessionId: string | null = "deleted-session";
	const workspace = {
		getState: () => ({
			directory: "/work/project",
			mode: "selected" as const,
			sessionId: workspaceSessionId,
			setSession: (sessionId: string | null) => {
				workspaceSessionId = sessionId;
			},
			initFromSession: () => {},
			clear: () => {},
		}),
	};
	const sessions = createSessionsStore();
	const client = {
		getSession: async () => {
			throw new MitsuroApiError(404, "Session deleted-session not found", "");
		},
		removeSessionPresence: async () => ({}),
	};
	const store = createSessionStore(
		client as never,
		createStorage(),
		workspace as never,
		sessions as never,
		createPlanStore() as never,
	);

	await store.getState().loadSession("deleted-session", true);

	assertEquals(store.getState().sessionId, null, "stale session should be detached");
	assertEquals(workspaceSessionId, null, "persisted workspace session should be cleared");
	assertEquals(
		workspace.getState().directory,
		"/work/project",
		"the selected workspace directory should remain available",
	);
	assertEquals(store.getState().error, null, "404 should not leave a blocking chat error");
	assertEquals(sessions.loadCount, 1, "session list should refresh after stale-session recovery");
	store.getState().cleanup();
});

Deno.test("idle parent keeps polling an active delegation group and advances its replay cursor", async () => {
	const timers = installFakeIntervals();
	try {
		const requestedCursors: Array<number | undefined> = [];
		let nextCursor = 7;
		const client = {
			getSessionState: async (
				_sessionId: string,
				options?: { delegationAfterCursor?: number },
			) => {
				requestedCursors.push(options?.delegationAfterCursor);
				return sessionState("idle", {
					delegation_groups: [{
						delegation_group_id: "group-detached",
						parent_tool_call_id: "tool-detached",
						state: "running",
						execution_mode: "detached",
						parent_continuation_state: "not_requested",
						tasks: [{
							delegation_task_id: "task-1",
							task_key: "worker-1",
							role: "build",
							state: "running",
							attempt_count: 1,
							updated_at: "2026-08-08T00:00:00Z",
						}],
						updated_at: "2026-08-08T00:00:00Z",
					}],
					delegation_events: [],
					delegation_event_cursor: nextCursor++,
				});
			},
			removeSessionPresence: async () => ({}),
		};
		const store = createSessionStore(
			client as never,
			createStorage(),
			createWorkspace() as never,
			createSessionsStore() as never,
			createPlanStore() as never,
		);

		store.getState().initSession("session-1", "Detached delegation");
		store.getState().startStatePolling("session-1");
		await timers.runLatestPollingTick();

		assertEquals(
			timers.activePollingCount(),
			1,
			"an idle parent must not stop polling while a detached group is active",
		);
		assertEquals(
			store.getState().delegationEventCursor,
			7,
			"the canonical delegation cursor should advance from the snapshot",
		);

		await timers.runLatestPollingTick();
		assertDeepEquals(
			requestedCursors,
			[undefined, 7],
			"the next poll should request only delegation events after the applied cursor",
		);
		store.getState().cleanup();
	} finally {
		timers.restore();
	}
});
