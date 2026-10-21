import { describe, expect, it } from "bun:test";

import {
	KrustyApiError,
	KrustyClient,
	type ChatRequest,
	type HiveWorkerGovernorRecoveryResponse,
	type HiveWorkerIntroductionReviewStatus,
	type ModelKey,
	type ModelsResponse,
} from "../src";

const queuedIntroductionReviewStatus: HiveWorkerIntroductionReviewStatus = "queued";

const exactGrokKey: ModelKey = {
	provider: "grok",
	model_id: "grok-4.5",
	auth_scope: "oauth",
	api_format: "open_ai_responses",
};

describe("KrustyClient request errors", () => {
	it("preserves HTTP status and response body in a typed error", async () => {
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async () =>
				new Response(JSON.stringify({ error: "Session missing not found" }), {
					status: 404,
					headers: { "content-type": "application/json" },
				}),
		});

		try {
			await client.getSession("missing");
			throw new Error("expected request to fail");
		} catch (error) {
			expect(error).toBeInstanceOf(KrustyApiError);
			const apiError = error as KrustyApiError;
			expect(apiError.status).toBe(404);
			expect(apiError.responseBody).toContain("Session missing not found");
			expect(apiError.message).toBe("API 404: Session missing not found");
		}
	});

	it("preserves a 402 limit response for the visible provider error path", async () => {
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async () =>
				new Response(
					JSON.stringify({ error: "Grok Build usage balance exhausted" }),
					{
						status: 402,
						headers: { "content-type": "application/json" },
					},
				),
		});

		await expect(client.getSession("limited")).rejects.toMatchObject({
			status: 402,
			message: "API 402: Grok Build usage balance exhausted",
		});
	});
});

describe("MitsuroClient Browser control leases", () => {
	it("carries the opaque lease on every controller mutation", async () => {
		const requests: Array<{ path: string; body: unknown }> = [];
		const client = new KrustyClient({
			baseUrl: "https://mitsuro.test",
			fetchImpl: async (input, init) => {
				const path = new URL(String(input)).pathname;
				requests.push({
					path,
					body: init?.body ? JSON.parse(String(init.body)) : null,
				});
				if (path.endsWith("/control")) {
					return Response.json({
						lease: {
							resource_id: "browser-1",
							client_id: "mobile-a",
							lease_token: "opaque-lease",
							expires_at: "2026-08-30T12:01:00Z",
						},
					});
				}
				if (path.endsWith("/actions")) {
					return Response.json({ ok: true, results: [] });
				}
				return Response.json({ id: "browser-1" });
			},
		});

		const lease = (await client.takeBrowserControl("browser-1", "mobile-a"))
			.lease;
		await client.heartbeatBrowserSession(
			"browser-1",
			"controller",
			"mobile-a",
			lease,
		);
		await client.runBrowserActions("browser-1", lease, [{ type: "reload" }]);
		await client.stopBrowserSession("browser-1", lease);

		expect(requests).toEqual([
			{
				path: "/api/browser/browser-1/control",
				body: { client_id: "mobile-a" },
			},
			{
				path: "/api/browser/browser-1/heartbeat",
				body: {
					capability: "controller",
					client_id: "mobile-a",
					lease_token: "opaque-lease",
				},
			},
			{
				path: "/api/browser/browser-1/actions",
				body: {
					lease_token: "opaque-lease",
					actions: [{ type: "reload" }],
				},
			},
			{
				path: "/api/browser/browser-1/stop",
				body: { lease_token: "opaque-lease" },
			},
		]);
	});

	it("keeps account authorization out of Browser stream query strings", () => {
		const client = new KrustyClient({
			baseUrl: "https://mitsuro.test",
			token: "private-account-bearer",
		});
		const lease = {
			resource_id: "browser-1",
			client_id: "mobile-a",
			lease_token: "opaque-lease",
			expires_at: "2026-08-30T12:01:00Z",
		};
		const controller = client.browserStreamUrl(
			"/api/browser/browser-1/stream",
			"controller",
			"mobile-a",
			lease,
		);
		expect(controller).toBe(
			"wss://mitsuro.test/api/browser/browser-1/stream?capability=controller&client_id=mobile-a&lease_token=opaque-lease",
		);
		expect(controller).not.toContain("private-account-bearer");
		expect(
			client.browserStreamUrl(
				"/api/browser/browser-1/stream",
				"viewer",
				"mobile-b",
			),
		).toBe(
			"wss://mitsuro.test/api/browser/browser-1/stream?capability=viewer&client_id=mobile-b",
		);
		expect(() =>
			client.browserStreamUrl(
				"https://attacker.invalid/api/browser/browser-1/stream",
				"viewer",
				"mobile-b",
			),
		).toThrow("Invalid Mitsuro Browser stream path");
		expect(() =>
			client.browserStreamUrl(
				"/api/browser/browser-1/stream",
				"controller",
				"mobile-a",
			),
		).toThrow("Browser control lease is required");
	});
});

describe("KrustyClient content-free request diagnostics", () => {
	it("requests delegated history only for full session hydration", async () => {
		const urls: string[] = [];
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async (input) => {
				urls.push(String(input));
				return Response.json({});
			},
		});

		await client.getSessionState("session-id");
		await client.getSessionState("session-id", {
			includeDelegatedHistory: true,
			delegationAfterCursor: 41.9,
		});

		expect(new URL(urls[0] as string).search).toBe("");
		expect(new URL(urls[1] as string).searchParams.get("include_delegated_history"))
			.toBe("true");
		expect(new URL(urls[1] as string).searchParams.get("delegation_after_cursor"))
			.toBe("41");
	});

	it("reports a sanitized route family and terminal timing", async () => {
		const events: Array<{
			name: string;
			outcome: string;
			durationMs?: number;
			code?: string;
		}> = [];
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async () => Response.json({
				id: "session-private-id",
				messages: [],
			}),
			requestObserver: (event) => events.push(event),
		});

		await client.getSession("session-private-id");

		expect(events.map(({ name, outcome, code }) => ({ name, outcome, code })))
			.toEqual([
				{ name: "api.sessions.detail", outcome: "start", code: undefined },
				{ name: "api.sessions.detail", outcome: "complete", code: "http.2xx" },
			]);
		expect(events[1]?.durationMs).toBeGreaterThanOrEqual(0);
		expect(JSON.stringify(events)).not.toContain("session-private-id");
	});

	it("separates privacy-safe session request families without identifiers", async () => {
		const names: string[] = [];
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async (input) => {
				const path = new URL(String(input)).pathname;
				if (path === "/sessions") return Response.json([]);
				if (path === "/sessions/directories") return Response.json([]);
				return Response.json({});
			},
			requestObserver: (event) => {
				if (event.outcome === "start") names.push(event.name);
			},
		});

		await client.getSessions();
		await client.getSessionState("private-session-id");
		await client.getWorkflow("private-session-id");
		await client.getSessionPresence("private-session-id");
		await client.pinchSession("private-session-id");
		await client.getDirectories();

		expect(names).toEqual([
			"api.sessions.catalog",
			"api.sessions.state",
			"api.sessions.workflow",
			"api.sessions.presence",
			"api.sessions.action",
			"api.sessions.directories",
		]);
		expect(JSON.stringify(names)).not.toContain("private-session-id");
	});

	it("separates HTTP and transport failures without response content", async () => {
		const httpEvents: Array<{ outcome: string; code?: string }> = [];
		const httpClient = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async () => new Response("private provider detail", { status: 503 }),
			requestObserver: (event) => httpEvents.push(event),
		});
		await expect(httpClient.getModels()).rejects.toBeInstanceOf(KrustyApiError);

		const networkEvents: Array<{ outcome: string; code?: string }> = [];
		const networkClient = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async () => {
				throw new TypeError("private network detail");
			},
			requestObserver: (event) => networkEvents.push(event),
		});
		await expect(networkClient.getModels()).rejects.toBeInstanceOf(TypeError);

		expect(httpEvents.at(-1)).toMatchObject({
			outcome: "error",
			code: "http.5xx",
		});
		expect(networkEvents.at(-1)).toMatchObject({
			outcome: "error",
			code: "network.error",
		});
		expect(JSON.stringify([...httpEvents, ...networkEvents]))
			.not.toContain("private");
	});

	it("uses the configured native fetch implementation for health checks", async () => {
		let requestUrl = "";
		const events: string[] = [];
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async (input) => {
				requestUrl = String(input);
				return new Response(null, { status: 204 });
			},
			requestObserver: (event) =>
				events.push(`${event.name}:${event.outcome}:${event.code ?? ""}`),
		});

		expect(await client.checkHealth()).toBe(true);
		expect(requestUrl).toBe("http://krusty.test/health");
		expect(events).toEqual([
			"api.health:start:",
			"api.health:complete:http.2xx",
		]);
	});

	it("cannot let a diagnostics observer change request behavior", async () => {
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: async () => Response.json({
				models: [],
				default_model: null,
			}),
			requestObserver: () => {
				throw new Error("observer failure");
			},
		});

		expect((await client.getModels()).models).toEqual([]);
	});
});

describe("KrustyClient provider-aware model identity", () => {
	it("sends the exact key and legacy model mirror when dispatching Mako", async () => {
		let requestUrl = "";
		let requestInit: RequestInit | undefined;
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: (async (input, init) => {
				const url = String(input);
				if (url.includes("/capabilities") || url.endsWith("/sessions")) {
					return Response.json({ ok: true });
				}
				requestUrl = url;
				requestInit = init;
				return Response.json({ session_id: "mako-1", status: "started" });
			}) as typeof fetch,
		});

		await client.dispatchMako("Audit this project", {
			model: exactGrokKey.model_id,
			modelKey: exactGrokKey,
			projectDir: "/work/project",
		});

		expect(requestUrl).toBe("http://krusty.test/api/hive/dispatch");
		expect(requestInit?.method).toBe("POST");
		expect(JSON.parse(String(requestInit?.body))).toEqual({
			task: "Audit this project",
			project_dir: "/work/project",
			model: "grok-4.5",
			model_key: exactGrokKey,
		});
	});

	it("keeps legacy Mako dispatches compatible by omitting model_key", async () => {
		let body: unknown;
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: (async (input, init) => {
				const url = String(input);
				if (url.includes("/capabilities") || url.endsWith("/sessions")) {
					return Response.json({ ok: true });
				}
				body = JSON.parse(String(init?.body));
				return Response.json({ session_id: "mako-legacy", status: "started" });
			}) as typeof fetch,
		});

		await client.dispatchMako("Continue", { model: "grok-4.5" });

		expect(body).toEqual({ task: "Continue", model: "grok-4.5" });
	});

	it("persists and reads an exact default model selection", async () => {
		const requests: Array<{ url: string; body?: unknown }> = [];
		const response: ModelsResponse = {
			models: [],
			default_model: exactGrokKey.model_id,
			default_model_key: exactGrokKey,
		};
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: (async (input, init) => {
				requests.push({
					url: String(input),
					body: init?.body ? JSON.parse(String(init.body)) : undefined,
				});
				return String(input).endsWith("/models")
					? Response.json(response)
					: Response.json({ ok: true });
			}) as typeof fetch,
		});

		await client.setCurrentModel(exactGrokKey.model_id, exactGrokKey);
		const models = await client.getModels();

		expect(requests[0]).toEqual({
			url: "http://krusty.test/api/models/current",
			body: { model: "grok-4.5", model_key: exactGrokKey },
		});
		expect(models.default_model_key).toEqual(exactGrokKey);
	});

	it("types a chat start with the same exact model identity", () => {
		const request: ChatRequest = {
			message: "Build the project",
			model: exactGrokKey.model_id,
			model_key: exactGrokKey,
			allowed_tools: ["read", "grep"],
		};

		expect(request.model_key).toEqual(exactGrokKey);
		expect(request.allowed_tools).toEqual(["read", "grep"]);
	});
});

describe("KrustyClient Mako schedules", () => {
	it("uses a strong quoted revision for schedule status mutations", async () => {
		let requestInit: RequestInit | undefined;
		const client = new KrustyClient({
			baseUrl: "http://krusty.test",
			fetchImpl: (async (_input, init) => {
				requestInit = init;
				return Response.json({
					schedule_id: "schedule-1",
					revision: 8,
					status: "paused",
				});
			}) as typeof fetch,
		});

		await client.pauseMakoSchedule("session-1", "schedule-1", 7);

		expect(requestInit?.method).toBe("POST");
		expect((requestInit?.headers as Record<string, string>)["If-Match"]).toBe(
			'"7"',
		);
	});
});

describe("MitsuroClient Hive Worker Introduction controls", () => {
	it("keeps the queued durable review status in the shared client contract", () => {
		expect(queuedIntroductionReviewStatus).toBe("queued");
	});
	it("sends exact replay keys and typed decision bodies", async () => {
		const requests: Array<{
			url: string;
			method?: string;
			headers: Record<string, string>;
			body?: unknown;
		}> = [];
		const client = new KrustyClient({
			baseUrl: "http://mitsuro.test",
			hiveTransport: "canonical",
			fetchImpl: (async (input, init) => {
				requests.push({
					url: String(input),
					method: init?.method,
					headers: init?.headers as Record<string, string>,
					body: init?.body ? JSON.parse(String(init.body)) : undefined,
				});
				return Response.json({
					id: "worker-1",
					slug: "researcher",
					display_name: "Researcher",
					permission_mode: "supervised",
					autonomy: "manual",
					status: "active",
					created_at: "2026-08-24T00:00:00Z",
					updated_at: "2026-08-24T00:00:00Z",
				});
			}) as typeof fetch,
		});

		await client.createHiveWorker(
			{
				slug: "researcher",
				model: exactGrokKey.model_id,
				model_key: exactGrokKey,
			},
			{ idempotencyKey: "create-introduction-key" },
		);
		await client.retryHiveWorkerIntroduction("worker-1", {
			idempotencyKey: "retry-introduction-key",
		});
		await client.skipHiveWorkerIntroduction("worker-1", {
			idempotencyKey: "skip-introduction-key",
		});
		await client.confirmHiveWorkerIntroduction(
			"worker-1",
			{
				proposal_id: "proposal-1",
				proposal_revision: 2,
				selected_facts: [
					{
						fact_id: "fact-1",
						final_statement: "Help with runtime reliability.",
					},
				],
			},
			{ idempotencyKey: "confirm-introduction-key" },
		);
		await client.keepTalkingHiveWorkerIntroduction(
			"worker-1",
			{ proposal_id: "proposal-1", proposal_revision: 2 },
			{ idempotencyKey: "keep-introduction-key" },
		);

		expect(requests.map(({ url, method }) => ({ url, method }))).toEqual([
			{
				url: "http://mitsuro.test/api/hive/workers",
				method: "POST",
			},
			{
				url: "http://mitsuro.test/api/hive/workers/worker-1/introduction/retry",
				method: "POST",
			},
			{
				url: "http://mitsuro.test/api/hive/workers/worker-1/introduction/skip",
				method: "POST",
			},
			{
				url:
					"http://mitsuro.test/api/hive/workers/worker-1/introduction/confirm",
				method: "POST",
			},
			{
				url:
					"http://mitsuro.test/api/hive/workers/worker-1/introduction/keep-talking",
				method: "POST",
			},
		]);
		expect(
			requests.map((request) => request.headers["Idempotency-Key"]),
		).toEqual([
			"create-introduction-key",
			"retry-introduction-key",
			"skip-introduction-key",
			"confirm-introduction-key",
			"keep-introduction-key",
		]);
		expect(requests[0]?.body).toEqual({
			slug: "researcher",
			model: "grok-4.5",
			model_key: exactGrokKey,
		});
		expect(requests[1]?.body).toBeUndefined();
		expect(requests[2]?.body).toBeUndefined();
		expect(requests[3]?.body).toEqual({
			proposal_id: "proposal-1",
			proposal_revision: 2,
			selected_facts: [
				{
					fact_id: "fact-1",
					final_statement: "Help with runtime reliability.",
				},
			],
		});
		expect(requests[4]?.body).toEqual({
			proposal_id: "proposal-1",
			proposal_revision: 2,
		});
	});

	it("forwards AbortSignal when loading Worker detail", async () => {
		const controller = new AbortController();
		let observedSignal: AbortSignal | null | undefined;
		const client = new KrustyClient({
			baseUrl: "http://mitsuro.test",
			hiveTransport: "canonical",
			fetchImpl: (async (_input, init) => {
				observedSignal = init?.signal;
				return Response.json({
					id: "worker-1",
					slug: "worker",
					display_name: "Worker",
					permission_mode: "supervised",
					autonomy: "manual",
					status: "active",
					created_at: "2026-08-24T00:00:00Z",
					updated_at: "2026-08-24T00:00:00Z",
				});
			}) as typeof fetch,
		});

		await client.getHiveWorker("worker-1", { signal: controller.signal });
		expect(observedSignal).toBe(controller.signal);
	});

	it("loads the aggregate Worker governor over the exact encoded GET path", async () => {
		const controller = new AbortController();
		let observedUrl = "";
		let observedInit: RequestInit | undefined;
		const projection = {
			schema_version: 1 as const,
			worker_id: "worker/id",
			worker_revision: 4,
			dm_session_id: "dm-1",
			evaluated_at: "2026-08-25T12:00:00.000000Z",
			policy: {
				worker_id: "worker/id",
				revision: 2,
				daily_call_limit: 128,
				daily_token_limit: 1_000_000,
				timezone: "UTC",
				quiet_start_minute: null,
				quiet_end_minute: null,
				quiet_gap_policy: "shift_forward",
				quiet_fold_policy: "first",
				idle_base_secs: 900,
				idle_max_secs: 21_600,
				tracking_started_at: "2026-08-25T00:00:00.000000Z",
				created_at: "2026-08-25T00:00:00.000000Z",
				updated_at: "2026-08-25T00:00:00.000000Z",
			},
			daily: {
				local_day: "2026-08-25",
				timezone: "UTC",
				starts_at: "2026-08-25T00:00:00.000000Z",
				resets_at: "2026-08-26T00:00:00.000000Z",
				calls_used: 3,
				calls_limit: 128,
				tokens_used_or_reserved: 1_250,
				tokens_limit: 1_000_000,
			},
			autonomous_dm: {
				origin: "workflow_rollover",
				lane_key: "dm",
				reservation_tokens: 1,
				decision: {
					disposition: "allow",
					primary_reason: null,
					reasons: [],
					evaluated_at: "2026-08-25T12:00:00.000000Z",
					next_eligible_at: null,
					policy_revision: 2,
					tracking_started_at: "2026-08-25T00:00:00.000000Z",
					daily: {},
					idle: { lane_key: "dm", idle_streak: 0 },
					override_grant_id: null,
				},
			},
			foreground_dm: {
				origin: "user_dm",
				lane_key: "dm",
				reservation_tokens: 1,
				decision: {
					disposition: "allow",
					primary_reason: null,
					reasons: [],
					evaluated_at: "2026-08-25T12:00:00.000000Z",
					next_eligible_at: null,
					policy_revision: 2,
					tracking_started_at: "2026-08-25T00:00:00.000000Z",
					daily: {},
					idle: { lane_key: "dm", idle_streak: 0 },
					override_grant_id: null,
				},
			},
			unresolved_started_count: 0,
			response_loss_recovery_required: false,
			estimated_daily_cost: {
				local_day: "2026-08-25",
				timezone: "UTC",
				starts_at: "2026-08-25T00:00:00.000000Z",
				resets_at: "2026-08-26T00:00:00.000000Z",
				by_currency: [{
					currency: "USD",
					estimated_cost_microunits: "40",
					priced_call_count: 1,
				}],
				unpriced_call_count: 0,
			},
		};
		const client = new KrustyClient({
			baseUrl: "http://mitsuro.test",
			hiveTransport: "canonical",
			fetchImpl: (async (input, init) => {
				observedUrl = String(input);
				observedInit = init;
				return Response.json(projection);
			}) as typeof fetch,
		});

		const response = await client.getHiveWorkerGovernor("worker/id", {
			signal: controller.signal,
		});
		expect(observedUrl).toBe(
			"http://mitsuro.test/api/hive/workers/worker%2Fid/governor",
		);
		expect(observedInit?.signal).toBe(controller.signal);
		expect(observedInit?.method).toBeUndefined();
		expect(observedInit?.body).toBeUndefined();
		expect(response.daily.calls_used).toBe(3);
		expect(
			response.estimated_daily_cost.by_currency[0]?.estimated_cost_microunits,
		)
			.toBe("40");
	});

	it("requests one exact-owner Worker governor recovery without a request body", async () => {
		let observedUrl = "";
		let observedInit: RequestInit | undefined;
		let recovery: HiveWorkerGovernorRecoveryResponse = {
			worker_id: "worker/id",
			grant_id: "grant-1",
			expires_at: "2026-08-25T12:05:00.000000Z",
			status: "granted" as const,
			bypass_unresolved_provider_call: true as const,
		};
		const client = new KrustyClient({
			baseUrl: "http://mitsuro.test",
			hiveTransport: "canonical",
			fetchImpl: (async (input, init) => {
				observedUrl = String(input);
				observedInit = init;
				return Response.json(recovery);
			}) as typeof fetch,
		});

		const response = await client.grantHiveWorkerGovernorRecovery("worker/id", {
			idempotencyKey: "recover-worker-id-attempt-1",
		});
		expect(observedUrl).toBe(
			"http://mitsuro.test/api/hive/workers/worker%2Fid/governor/recovery",
		);
		expect(observedInit?.method).toBe("POST");
		expect((observedInit?.headers as Record<string, string>)["Idempotency-Key"])
			.toBe("recover-worker-id-attempt-1");
		expect(observedInit?.body).toBeUndefined();
		expect(response).toEqual(recovery);

		recovery = {
			worker_id: "worker/id",
			grant_id: null,
			expires_at: null,
			status: "response_loss_acknowledged",
			bypass_unresolved_provider_call: false,
		};
		const responseLoss = await client.grantHiveWorkerGovernorRecovery(
			"worker/id",
			{ idempotencyKey: "acknowledge-worker-response-loss" },
		);
		expect(responseLoss).toEqual(recovery);
	});

	it("looks up an archived Worker by exact direct session with cancellation", async () => {
		const controller = new AbortController();
		let observedUrl = "";
		let observedSignal: AbortSignal | null | undefined;
		const client = new KrustyClient({
			baseUrl: "http://mitsuro.test",
			hiveTransport: "canonical",
			fetchImpl: (async (input, init) => {
				observedUrl = String(input);
				observedSignal = init?.signal;
				return Response.json({
					kind: "worker_dm",
					session_id: "worker dm/archived",
					worker: {
						id: "worker-archived",
						revision: 9,
						slug: "archived-friend",
						display_name: "Archived Friend",
						permission_mode: "supervised",
						autonomy: "manual",
						status: "archived",
						dm_session_id: "worker dm/archived",
						attention: [],
						created_at: "2026-08-24T00:00:00Z",
						updated_at: "2026-08-25T00:00:00Z",
					},
				});
			}) as typeof fetch,
		});

		const response = await client.getHiveWorkerBySession(
			"worker dm/archived",
			{ signal: controller.signal },
		);
		expect(observedUrl).toBe(
			"http://mitsuro.test/api/hive/workers/by-session/worker%20dm%2Farchived",
		);
		expect(observedSignal).toBe(controller.signal);
		expect(response.kind).toBe("worker_dm");
		if (response.kind === "worker_dm") {
			expect(response.worker.status).toBe("archived");
		}
	});

	it("revision-fences Worker updates and lifecycle actions with replay keys", async () => {
		const requests: Array<{
			url: string;
			method?: string;
			headers: Record<string, string>;
			body?: unknown;
		}> = [];
		const client = new KrustyClient({
			baseUrl: "http://mitsuro.test",
			hiveTransport: "canonical",
			fetchImpl: (async (input, init) => {
				requests.push({
					url: String(input),
					method: init?.method,
					headers: init?.headers as Record<string, string>,
					body: init?.body ? JSON.parse(String(init.body)) : undefined,
				});
				return Response.json({
					id: "worker-1",
					revision: 8,
					slug: "researcher",
					display_name: "Researcher",
					permission_mode: "supervised",
					autonomy: "manual",
					status: "paused",
					attention: [],
					created_at: "2026-08-24T00:00:00Z",
					updated_at: "2026-08-24T00:00:00Z",
				});
			}) as typeof fetch,
		});

		await client.updateHiveWorker(
			"worker-1",
			{ expected_revision: 7, display_name: "Researcher" },
			{ idempotencyKey: "worker-update-key" },
		);
		await client.pauseHiveWorker("worker-1", 8, {
			idempotencyKey: "worker-pause-key",
		});
		await client.resumeHiveWorker("worker-1", 9, {
			idempotencyKey: "worker-resume-key",
		});
		await client.archiveHiveWorker("worker-1", 10, {
			idempotencyKey: "worker-archive-key",
		});

		expect(requests.map((request) => request.method)).toEqual([
			"PATCH",
			"POST",
			"POST",
			"DELETE",
		]);
		expect(
			requests.map((request) => request.headers["Idempotency-Key"]),
		).toEqual([
			"worker-update-key",
			"worker-pause-key",
			"worker-resume-key",
			"worker-archive-key",
		]);
		expect(requests.map((request) => request.body)).toEqual([
			{ expected_revision: 7, display_name: "Researcher" },
			{ expected_revision: 8 },
			{ expected_revision: 9 },
			{ expected_revision: 10 },
		]);
	});

	it("keeps Worker Goal lifecycle on dedicated revision-fenced routes", async () => {
		const requests: Array<{
			url: string;
			method?: string;
			headers: Record<string, string>;
			body?: unknown;
		}> = [];
		const projection = {
			schema_version: 1,
			worker_id: "worker/id",
			worker_revision: 4,
			worker_status: "active",
			session_id: "dm-1",
			workspace: {
				mode: "selected",
				working_dir: "/work",
				project_dir: "/work",
			},
			introduction_ready: true,
			workflow: null,
			active_run: null,
			attention: [],
			allowed_actions: [],
		};
		const client = new KrustyClient({
			baseUrl: "http://mitsuro.test",
			hiveTransport: "canonical",
			fetchImpl: (async (input, init) => {
				requests.push({
					url: String(input),
					method: init?.method,
					headers: init?.headers as Record<string, string>,
					body: init?.body ? JSON.parse(String(init.body)) : undefined,
				});
				return Response.json(projection);
			}) as typeof fetch,
		});
		const fence = {
			goal_id: "goal-1",
			expected_worker_revision: 4,
			expected_goal_revision: 7,
		};

		await client.getHiveWorkerGoal("worker/id");
		await client.createHiveWorkerGoal(
			"worker/id",
			{
				expected_worker_revision: 4,
				goal: {
					title: "Ship the bridge",
					objective: "Verify the Worker Goal vertical",
					criteria: [{ description: "All focused tests pass", required: true }],
				},
				plan: {
					title: "One bounded step",
					steps: [{
						display_key: "1",
						description: "Run focused validation",
						acceptance_criteria: ["Validation is green"],
						required: true,
					}],
				},
			},
			{ idempotencyKey: "goal-create" },
		);
		await client.approveHiveWorkerGoal(
			"worker/id",
			{ ...fence, plan_revision_id: "plan-2" },
			{ idempotencyKey: "goal-approve" },
		);
		await client.activateHiveWorkerGoal("worker/id", fence, {
			idempotencyKey: "goal-activate",
		});
		await client.pauseHiveWorkerGoal(
			"worker/id",
			{ ...fence, reason: "paused_from_mobile" },
			{ idempotencyKey: "goal-pause" },
		);
		await client.cancelHiveWorkerGoal(
			"worker/id",
			{ ...fence, reason: "cancelled_from_mobile" },
			{ idempotencyKey: "goal-cancel" },
		);
		await client.resolveHiveWorkerGoalAcceptance(
			"worker/id",
			{
				expected_worker_revision: 4,
				acceptance_run_id: "acceptance-1",
				expected_goal_revision: 7,
				decision: "accept",
				reason: "Reviewed the result",
				criteria: [{
					criterion_id: "criterion-1",
					decision: "passed",
					evidence: ["Focused validation passed"],
				}],
			},
			{ idempotencyKey: "goal-acceptance" },
		);
		await client.setHiveWorkerWorkspace(
			"worker/id",
			{
				expected_worker_revision: 4,
				workspace_mode: "selected",
				working_dir: "/work",
				project_dir: "/work",
			},
			{ idempotencyKey: "workspace-set" },
		);

		expect(requests.map(({ url }) => new URL(url).pathname)).toEqual([
			"/api/hive/workers/worker%2Fid/workflow",
			"/api/hive/workers/worker%2Fid/workflow",
			"/api/hive/workers/worker%2Fid/workflow/approve",
			"/api/hive/workers/worker%2Fid/workflow/activate",
			"/api/hive/workers/worker%2Fid/workflow/pause",
			"/api/hive/workers/worker%2Fid/workflow/cancel",
			"/api/hive/workers/worker%2Fid/workflow/acceptance",
			"/api/hive/workers/worker%2Fid/workspace",
		]);
		expect(
			requests.some(({ url }) =>
				url.includes("/sessions/") && url.includes("/workflow/commands")
			),
		).toBe(false);
		expect(requests.slice(1).map(({ method }) => method)).toEqual([
			"POST",
			"POST",
			"POST",
			"POST",
			"POST",
			"POST",
			"PUT",
		]);
		expect(
			requests.slice(1).map(({ headers }) => headers["Idempotency-Key"]),
		).toEqual([
			"goal-create",
			"goal-approve",
			"goal-activate",
			"goal-pause",
			"goal-cancel",
			"goal-acceptance",
			"workspace-set",
		]);
		expect(requests[6]?.body).toEqual({
			expected_worker_revision: 4,
			acceptance_run_id: "acceptance-1",
			expected_goal_revision: 7,
			decision: "accept",
			reason: "Reviewed the result",
			criteria: [{
				criterion_id: "criterion-1",
				decision: "passed",
				evidence: ["Focused validation passed"],
			}],
		});
	});
});
