import { normalizeCitations } from "./citations";
import type {
	AgentMemory,
	AgentControlState,
	ApnsRegisterResponse,
	ApnsStatusResponse,
	ApproveHiveWorkerGoalRequest,
	BrowserAction,
	BrowserActionResponse,
	BrowserAgentRequest,
	BrowserAgentResponse,
	BrowserControlLease,
	BrowserSession,
	BrowserSessionListResponse,
	ChatRequest,
	ConfirmHiveWorkerIntroductionRequest,
	CreateBrowserSessionRequest,
	CreateHiveGroupRequest,
	CreateHiveWorkerGoalRequest,
	CreateHiveWorkerRequest,
	DelegatedProgressEvent,
	DelegationEventResponse,
	GitBranchesResponse,
	GitChangesResponse,
	GitFileDiffResponse,
	GitStatusResponse,
	GitWorktreesResponse,
	HiveAttentionResponse,
	HiveBootstrapResponse,
	HiveChannelsResponse,
	HiveCrewDocumentKind,
	HiveCrewResponse,
	HiveCurrentResponse,
	HiveDeliveryStatus,
	HiveDispatchOptions,
	HiveDispatchResponse,
	HiveGlobalSchedule,
	HiveGroupDetail,
	HiveGroupEvent,
	HiveGroupMessagesResponse,
	HiveGroupsResponse,
	HiveGroupTurn,
	HiveHomeDocumentKind,
	HiveHomeResponse,
	HiveMainResponse,
	HiveRecoverDaemonResponse,
	HiveRunPriority,
	HiveSchedule,
	HiveScheduleMutationResponse,
	HiveScheduleWriteRequest,
	HiveSessionStatus,
	HiveSessionSummary,
	HiveWorker,
	HiveWorkerDeliveriesResponse,
	HiveWorkerDetail,
	HiveWorkerDmResponse,
	HiveWorkerGoalProjection,
	HiveWorkerGovernorProjection,
	HiveWorkerGovernorRecoveryResponse,
	HiveWorkerSessionBindingResponse,
	HiveWorkersResponse,
	KeepTalkingHiveWorkerIntroductionRequest,
	McpServerResponse,
	McpToolResponse,
	MemorySnapshotResponse,
	MemoryType,
	MobileDiagnosticUploadBatch,
	MobileDiagnosticUploadResponse,
	ModelKey,
	ModelsResponse,
	MutateHiveWorkerGoalRequest,
	OAuthExchangeResponse,
	OAuthStartResponse,
	OAuthStatusResponse,
	PermissionMode,
	PortListResponse,
	PreviewSettings,
	PreviewSettingsPatch,
	PromoteReportToMemoryResponse,
	ProviderStatus,
	Report,
	ReportSummary,
	ResolveHiveWorkerGoalAcceptanceRequest,
	SendHiveGroupMessageRequest,
	SendHiveGroupMessageResponse,
	ServerAccessResponse,
	SessionPresenceResponse,
	SessionResponse,
	SessionStateResponse,
	SessionType,
	SessionWithMessagesResponse,
	SetHiveWorkerWorkspaceRequest,
	SimpleOkResponse,
	SkillInfo,
	SteerRequest,
	SteerResponse,
	StreamCallbacks,
	StreamEvent,
	TakeBrowserControlResponse,
	TakeAgentControlResponse,
	ToolResultRequest,
	TransitionHiveWorkerGoalRequest,
	TreeEntry,
	UpdateHiveGroupRequest,
	UpdateHiveWorkerRequest,
	UsageMetrics,
	WorkflowCommand,
	WorkflowMutation,
	WorkflowSnapshot,
	WorkspaceMode,
} from "./types";
import {
	encodeLegacyRequestIdentity,
	isLegacyRouteFallbackStatus,
	legacyHiveApiPath,
	normalizeLegacyResponseIdentity,
} from "./legacy-wire";

type UsageStreamEvent = Extract<StreamEvent, { type: "usage" }>;

export function normalizeUsageMetrics(event: UsageStreamEvent): UsageMetrics {
	const cacheCreationInputTokens = event.cache_creation_input_tokens ?? 0;
	const cacheReadInputTokens = event.cache_read_input_tokens ?? 0;
	const reasoningTokens = Math.max(
		0,
		Math.min(event.reasoning_tokens ?? 0, Math.max(0, event.completion_tokens)),
	);
	const inputTokens =
		event.input_tokens ??
		event.prompt_tokens + cacheCreationInputTokens + cacheReadInputTokens;
	const representedTotal = inputTokens + event.completion_tokens;

	return {
		promptTokens: event.prompt_tokens,
		inputTokens,
		completionTokens: event.completion_tokens,
		reasoningTokens,
		cacheCreationInputTokens,
		cacheReadInputTokens,
		totalTokens: Math.max(event.total_tokens ?? 0, representedTotal),
	};
}

const STREAM_ACTIVITY_TIMEOUT = 240_000;
const MITSURO_WIRE_VERSION_HEADER = "X-Mitsuro-Wire-Version";
const MITSURO_WIRE_VERSION = "2";
const MITSURO_CONTROL_LEASE_HEADER = "X-Mitsuro-Control-Lease";
const MAX_COMPATIBILITY_AGENT_LEASES = 512;

function monotonicNow(): number {
	return globalThis.performance?.now?.() ?? Date.now();
}

function createAgentControlClientId(): string {
	const random = globalThis.crypto?.randomUUID?.();
	if (random) return `mitsuro-api-${random}`;
	return `mitsuro-api-${Date.now().toString(36)}-${Math.random()
		.toString(36)
		.slice(2)}`;
}

function isAbortError(error: unknown): boolean {
	return error instanceof Error && error.name === "AbortError";
}

function httpStatusClass(status: number): string {
	if (status >= 200 && status < 300) return "http.2xx";
	if (status >= 300 && status < 400) return "http.3xx";
	if (status >= 400 && status < 500) return "http.4xx";
	if (status >= 500) return "http.5xx";
	return "http.unknown";
}

function requestDiagnosticName(path: string): string {
	const route = path.split("?", 1)[0] ?? path;
	if (route === "/sessions") return "api.sessions.catalog";
	if (route === "/sessions/directories") return "api.sessions.directories";
	if (route.startsWith("/sessions/")) {
		const segments = route.split("/").filter(Boolean);
		const subroute = segments[2];
		if (!subroute) return "api.sessions.detail";
		if (subroute === "state") return "api.sessions.state";
		if (subroute === "workflow") return "api.sessions.workflow";
		if (subroute === "presence") return "api.sessions.presence";
		if (subroute === "cancel" || subroute === "pinch") {
			return "api.sessions.action";
		}
		return "api.sessions";
	}
	if (route.startsWith("/models")) return "api.models";
	if (route.startsWith("/credentials")) return "api.credentials";
	if (route.startsWith("/auth")) return "api.auth";
	if (route.startsWith("/mcp")) return "api.mcp";
	if (route.startsWith("/skills")) return "api.skills";
	if (route.startsWith("/ports") || route.startsWith("/settings/preview")) {
		return "api.ports";
	}
	if (route.startsWith("/hive")) return "api.hive";
	if (route.startsWith("/git")) return "api.git";
	if (route.startsWith("/files")) return "api.files";
	if (route.startsWith("/apns") || route.startsWith("/push")) {
		return "api.notifications";
	}
	if (route.startsWith("/mobile-diagnostics")) {
		return "api.mobile_diagnostics";
	}
	if (route.startsWith("/chat")) return "api.stream";
	return "api.other";
}

function apiErrorMessage(body: string, fallback: string): string {
	if (!body) return fallback;
	try {
		const parsed = JSON.parse(body) as { error?: unknown; message?: unknown };
		if (typeof parsed.error === "string" && parsed.error.trim()) {
			return parsed.error;
		}
		if (typeof parsed.message === "string" && parsed.message.trim()) {
			return parsed.message;
		}
	} catch {
		// Plain-text provider and proxy errors are already human-readable.
	}
	return body;
}

export interface MitsuroClientConfig {
	baseUrl: string;
	token?: string;
	/** Custom fetch implementation for environments without streaming support (e.g. React Native). */
	fetchImpl?: typeof fetch;
	/** Content-free request lifecycle observer for app-owned diagnostics. */
	requestObserver?: (event: MitsuroRequestDiagnostic) => void;
	/** Route generation for mixed-version Hive servers. Defaults to safe auto-detection. */
	hiveTransport?: HiveTransportMode;
}

export type HiveTransportMode = "auto" | "canonical" | "legacy";

export type MitsuroRequestDiagnosticOutcome =
	"start" | "complete" | "cancel" | "error";

export interface MitsuroRequestDiagnostic {
	name: string;
	outcome: MitsuroRequestDiagnosticOutcome;
	durationMs?: number;
	code?: string;
}

export class MitsuroApiError extends Error {
	constructor(
		public readonly status: number,
		message: string,
		public readonly responseBody: string,
	) {
		super(`API ${status}: ${message}`);
		this.name = "MitsuroApiError";
	}
}

export class MitsuroClient {
	private baseUrl: string;
	private token: string | undefined;
	private fetchFn: typeof fetch;
	private requestObserver: MitsuroClientConfig["requestObserver"];
	private resolvedHiveTransport: Exclude<HiveTransportMode, "auto"> | null;
	private hiveTransportProbe: Promise<
		Exclude<HiveTransportMode, "auto">
	> | null = null;
	private delegationEventListeners = new Set<
		(event: DelegationEventResponse) => void
	>();
	private readonly agentControlClientId = createAgentControlClientId();
	private readonly agentControlLeases = new Map<string, string>();
	private readonly agentControlClaims = new Map<
		string,
		Promise<string | null>
	>();

	constructor(config: MitsuroClientConfig) {
		this.baseUrl = config.baseUrl.replace(/\/+$/, "");
		this.token = config.token;
		this.fetchFn = config.fetchImpl ?? globalThis.fetch.bind(globalThis);
		this.requestObserver = config.requestObserver;
		this.resolvedHiveTransport =
			config.hiveTransport === "legacy" || config.hiveTransport === "canonical"
				? config.hiveTransport
				: null;
	}

	/**
	 * Observe durable child lifecycle/conversation events already carried by an
	 * attached chat stream. Consumers own their initial HTTP hydration and only
	 * subscribe while the child surface is mounted.
	 */
	subscribeDelegationEvents(
		listener: (event: DelegationEventResponse) => void,
	): () => void {
		this.delegationEventListeners.add(listener);
		return () => this.delegationEventListeners.delete(listener);
	}

	private headers(): Record<string, string> {
		const h: Record<string, string> = {
			"Content-Type": "application/json",
			[MITSURO_WIRE_VERSION_HEADER]: MITSURO_WIRE_VERSION,
		};
		if (this.token) {
			h["Authorization"] = `Bearer ${this.token}`;
		}
		return h;
	}

	private ensureAgentControl(sessionId: string): Promise<string | null> {
		const existing = this.agentControlClaims.get(sessionId);
		if (existing) return existing;
		const claim = this.resolveAgentControl(sessionId).finally(() => {
			if (this.agentControlClaims.get(sessionId) === claim) {
				this.agentControlClaims.delete(sessionId);
			}
		});
		this.agentControlClaims.set(sessionId, claim);
		return claim;
	}

	private async resolveAgentControl(sessionId: string): Promise<string | null> {
		const path = `/sessions/${encodeURIComponent(sessionId)}/control`;
		const summaryResponse = await this.fetchWithHiveCompatibility(path, {
			method: "GET",
			headers: this.headers(),
		});
		if (summaryResponse.status === 409) {
			return null;
		}
		if (!summaryResponse.ok) {
			const text = await summaryResponse.text().catch(() => "Request failed");
			throw new MitsuroApiError(
				summaryResponse.status,
				apiErrorMessage(text, "Could not inspect Agent control"),
				text,
			);
		}
		const summary = (await summaryResponse.json()) as AgentControlState;
		if (summary.controller) {
			if (summary.controller.client_id !== this.agentControlClientId) {
				throw new MitsuroApiError(
					409,
					`Code Agent is controlled by another client (${summary.controller.client_id})`,
					"",
				);
			}
			const cached = this.agentControlLeases.get(sessionId);
			if (cached) return cached;
		}

		const takeResponse = await this.fetchWithHiveCompatibility(path, {
			method: "POST",
			headers: this.headers(),
			body: JSON.stringify({
				client_id: this.agentControlClientId,
				force: false,
			}),
		});
		if (!takeResponse.ok) {
			const text = await takeResponse.text().catch(() => "Request failed");
			throw new MitsuroApiError(
				takeResponse.status,
				apiErrorMessage(text, "Could not take Agent control"),
				text,
			);
		}
		const taken = (await takeResponse.json()) as TakeAgentControlResponse;
		const token = taken.lease.lease_token;
		if (this.agentControlLeases.size >= MAX_COMPATIBILITY_AGENT_LEASES) {
			const oldest = this.agentControlLeases.keys().next().value;
			if (typeof oldest === "string") this.agentControlLeases.delete(oldest);
		}
		this.agentControlLeases.set(sessionId, token);
		return token;
	}

	private async agentMutationHeaders(
		sessionId: string,
	): Promise<Record<string, string>> {
		const lease = await this.ensureAgentControl(sessionId);
		return lease ? { [MITSURO_CONTROL_LEASE_HEADER]: lease } : {};
	}

	private async request<T>(
		path: string,
		options: RequestInit = {},
	): Promise<T> {
		const diagnosticName = requestDiagnosticName(path);
		const startedAt = monotonicNow();
		this.observeRequest(diagnosticName, "start");
		let response: Response;
		try {
			const requestOptions = {
				...options,
				headers: {
					...this.headers(),
					...(options.headers as Record<string, string>),
				},
			};
			response = await this.fetchWithHiveCompatibility(path, requestOptions);
		} catch (error) {
			this.observeRequest(
				diagnosticName,
				isAbortError(error) ? "cancel" : "error",
				startedAt,
				isAbortError(error) ? "request.abort" : "network.error",
			);
			throw error;
		}

		if (!response.ok) {
			const text = await response.text().catch(() => "Request failed");
			const message = apiErrorMessage(text, "Request failed");
			this.observeRequest(
				diagnosticName,
				"error",
				startedAt,
				httpStatusClass(response.status),
			);
			throw new MitsuroApiError(response.status, message, text);
		}

		try {
			const result = normalizeLegacyResponseIdentity(
				path,
				await response.json(),
			) as T;
			this.observeRequest(
				diagnosticName,
				"complete",
				startedAt,
				httpStatusClass(response.status),
			);
			return result;
		} catch (error) {
			this.observeRequest(diagnosticName, "error", startedAt, "decode.error");
			throw error;
		}
	}

	private async fetchWithHiveCompatibility(
		path: string,
		options: RequestInit,
	): Promise<Response> {
		const legacyPath = legacyHiveApiPath(path);
		if (!legacyPath) {
			return this.fetchFn(`${this.baseUrl}/api${path}`, options);
		}
		if (this.resolvedHiveTransport === "legacy") {
			return this.fetchFn(`${this.baseUrl}/api${legacyPath}`, options);
		}
		if (this.resolvedHiveTransport === "canonical") {
			return this.fetchFn(`${this.baseUrl}/api${path}`, options);
		}

		const method = (options.method ?? "GET").toUpperCase();
		const canRetryWithoutReplayingMutation =
			method === "GET" || method === "HEAD";
		if (!canRetryWithoutReplayingMutation) {
			const transport = await this.detectHiveTransport(options.signal);
			const selectedPath = transport === "legacy" ? legacyPath : path;
			return this.fetchFn(`${this.baseUrl}/api${selectedPath}`, options);
		}

		const canonicalResponse = await this.fetchFn(
			`${this.baseUrl}/api${path}`,
			options,
		);
		if (!isLegacyRouteFallbackStatus(canonicalResponse.status)) {
			this.resolvedHiveTransport = "canonical";
			return canonicalResponse;
		}
		const legacyResponse = await this.fetchFn(
			`${this.baseUrl}/api${legacyPath}`,
			options,
		);
		if (!isLegacyRouteFallbackStatus(legacyResponse.status)) {
			this.resolvedHiveTransport = "legacy";
			return legacyResponse;
		}
		return canonicalResponse;
	}

	private detectHiveTransport(
		signal?: AbortSignal | null,
	): Promise<Exclude<HiveTransportMode, "auto">> {
		if (this.resolvedHiveTransport) {
			return Promise.resolve(this.resolvedHiveTransport);
		}
		if (this.hiveTransportProbe) return this.hiveTransportProbe;

		const probe = (async (): Promise<"canonical" | "legacy"> => {
			const headers = this.headers();
			const canonicalCapability = await this.fetchFn(
				`${this.baseUrl}/api/hive/capabilities`,
				{ method: "GET", headers, signal },
			);
			if (!isLegacyRouteFallbackStatus(canonicalCapability.status)) {
				return "canonical";
			}
			const legacyCapability = await this.fetchFn(
				`${this.baseUrl}/api/mako/capabilities`,
				{ method: "GET", headers, signal },
			);
			if (!isLegacyRouteFallbackStatus(legacyCapability.status)) {
				return "legacy";
			}

			// Pre-bridge servers do not expose a capability route. Fall back to
			// read-only session lists rather than the state-creating `/main` route.
			const canonicalSessions = await this.fetchFn(
				`${this.baseUrl}/api/hive/sessions`,
				{ method: "GET", headers, signal },
			);
			if (!isLegacyRouteFallbackStatus(canonicalSessions.status)) {
				return "canonical";
			}
			const legacySessions = await this.fetchFn(
				`${this.baseUrl}/api/mako/sessions`,
				{ method: "GET", headers, signal },
			);
			return isLegacyRouteFallbackStatus(legacySessions.status)
				? "canonical"
				: "legacy";
		})();
		this.hiveTransportProbe = probe;
		return probe
			.then((transport) => {
				this.resolvedHiveTransport = transport;
				return transport;
			})
			.finally(() => {
				if (this.hiveTransportProbe === probe) this.hiveTransportProbe = null;
			});
	}

	private async encodeRequestIdentityForServer<
		T extends Record<string, unknown>,
	>(
		body: T,
		sessionType: SessionType | undefined,
		signal?: AbortSignal,
	): Promise<T> {
		if (sessionType !== "hive") return body;
		const transport = await this.detectHiveTransport(signal);
		return transport === "legacy" ? encodeLegacyRequestIdentity(body) : body;
	}

	private observeRequest(
		name: string,
		outcome: MitsuroRequestDiagnosticOutcome,
		startedAt?: number,
		code?: string,
	): void {
		try {
			this.requestObserver?.({
				name,
				outcome,
				durationMs:
					startedAt === undefined
						? undefined
						: Math.max(0, monotonicNow() - startedAt),
				code,
			});
		} catch {
			// Diagnostics must never change request behavior.
		}
	}

	// Health & Auth
	async checkHealth(): Promise<boolean> {
		const diagnosticName = "api.health";
		const startedAt = monotonicNow();
		this.observeRequest(diagnosticName, "start");
		try {
			const resp = await this.fetchFn(`${this.baseUrl}/health`, {
				headers: this.headers(),
			});
			this.observeRequest(
				diagnosticName,
				resp.ok ? "complete" : "error",
				startedAt,
				httpStatusClass(resp.status),
			);
			return resp.ok;
		} catch (error) {
			this.observeRequest(
				diagnosticName,
				isAbortError(error) ? "cancel" : "error",
				startedAt,
				isAbortError(error) ? "request.abort" : "network.error",
			);
			return false;
		}
	}

	async bootstrapAuth(): Promise<boolean> {
		// The current server authenticates each API request with the bearer token
		// directly and does not expose a separate bootstrap route.
		return true;
	}

	// Notifications
	async registerApnsDevice(
		deviceToken: string,
		bundleId?: string,
		notificationLevel?: import("./types").NotificationDeliveryLevel,
		environment?: "sandbox" | "production",
	): Promise<ApnsRegisterResponse> {
		return this.request("/apns/register", {
			method: "POST",
			body: JSON.stringify({
				device_token: deviceToken,
				bundle_id: bundleId ?? undefined,
				notification_level: notificationLevel ?? undefined,
				environment: environment ?? undefined,
			}),
		});
	}

	async registerExpoPushDevice(
		expoPushToken: string,
		platform: import("./types").PushPlatform,
		notificationLevel: import("./types").NotificationDeliveryLevel,
	): Promise<import("./types").ExpoPushRegisterResponse> {
		return this.request("/push/expo/register", {
			method: "POST",
			body: JSON.stringify({
				expo_push_token: expoPushToken,
				platform,
				notification_level: notificationLevel,
			}),
		});
	}

	async unregisterExpoPushDevice(
		expoPushToken: string,
	): Promise<{ removed: boolean }> {
		return this.request("/push/expo/register", {
			method: "DELETE",
			body: JSON.stringify({
				expo_push_token: expoPushToken,
			}),
		});
	}

	async uploadMobileDiagnostics(
		batch: MobileDiagnosticUploadBatch,
	): Promise<MobileDiagnosticUploadResponse> {
		return this.request("/mobile-diagnostics/batches", {
			method: "POST",
			body: JSON.stringify(batch),
		});
	}

	async registerLiveActivity(
		request: import("./types").LiveActivityRegisterRequest,
	): Promise<ApnsRegisterResponse> {
		return this.request("/apns/live-activities/register", {
			method: "POST",
			body: JSON.stringify({
				session_id: request.sessionId,
				push_token: request.pushToken,
				content_state: request.contentState,
				started_at_ms: request.startedAtMs,
				bundle_id: request.bundleId,
				environment: request.environment,
			}),
		});
	}

	async updateLiveActivityState(
		sessionId: string,
		pushToken: string,
		contentState: Record<string, unknown>,
	): Promise<{ updated: boolean }> {
		return this.request("/apns/live-activities/state", {
			method: "POST",
			body: JSON.stringify({
				session_id: sessionId,
				push_token: pushToken,
				content_state: contentState,
			}),
		});
	}

	async unregisterLiveActivity(
		sessionId: string,
		pushToken: string,
	): Promise<{ removed: boolean }> {
		return this.request("/apns/live-activities/unregister", {
			method: "POST",
			body: JSON.stringify({
				session_id: sessionId,
				push_token: pushToken,
			}),
		});
	}

	async getApnsStatus(): Promise<ApnsStatusResponse> {
		return this.request("/apns/status");
	}

	// Sessions
	async getSessions(options?: { includeArchived?: boolean }): Promise<SessionResponse[]> {
		const params = new URLSearchParams();
		if (options?.includeArchived) {
			params.set("include_archived", "true");
		}
		const query = params.toString();
		return this.request(`/sessions${query ? `?${query}` : ""}`);
	}

	async getSession(id: string): Promise<SessionWithMessagesResponse> {
		return this.request(`/sessions/${id}`);
	}

	async createSession(
		title?: string,
		projectDir?: string,
		targetBranch?: string | null,
		workspaceMode?: WorkspaceMode,
		sessionType?: SessionType,
		permissionMode?: PermissionMode,
	): Promise<SessionResponse> {
		const body = await this.encodeRequestIdentityForServer(
			{
				title: title ?? undefined,
				project_dir: projectDir ?? undefined,
				target_branch: targetBranch ?? undefined,
				workspace_mode: workspaceMode ?? undefined,
				session_type: sessionType ?? undefined,
				permission_mode: permissionMode ?? undefined,
			},
			sessionType,
		);
		return this.request("/sessions", {
			method: "POST",
			body: JSON.stringify(body),
		});
	}

	async updateSession(
		id: string,
		data: Partial<{
			title: string;
			mode: string;
			model: string | null;
			model_key: ModelKey | null;
			target_branch: string | null;
			targetBranch: string | null;
			permission_mode: PermissionMode;
			pinned: boolean;
			archived: boolean;
		}>,
	): Promise<SessionResponse> {
		return this.request(`/sessions/${id}`, {
			method: "PATCH",
			body: JSON.stringify(data),
		});
	}

	async deleteSession(id: string): Promise<void> {
		await this.request(`/sessions/${id}`, { method: "DELETE" });
	}

	async getSessionState(
		id: string,
		options?: {
			includeDelegatedHistory?: boolean;
			delegationAfterCursor?: number;
			signal?: AbortSignal;
		},
	): Promise<SessionStateResponse> {
		const params = new URLSearchParams();
		if (options?.includeDelegatedHistory) {
			params.set("include_delegated_history", "true");
		}
		if (options?.delegationAfterCursor !== undefined) {
			params.set(
				"delegation_after_cursor",
				String(Math.max(0, Math.trunc(options.delegationAfterCursor))),
			);
		}
		const encoded = params.toString();
		const query = encoded ? `?${encoded}` : "";
		return this.request(`/sessions/${id}/state${query}`, {
			signal: options?.signal,
		});
	}

	async getWorkflow(id: string): Promise<WorkflowSnapshot | null> {
		return this.request(`/sessions/${id}/workflow`);
	}

	async executeWorkflowCommand(
		id: string,
		command: WorkflowCommand,
	): Promise<WorkflowMutation> {
		return this.request(`/sessions/${id}/workflow/commands`, {
			method: "POST",
			body: JSON.stringify(command),
		});
	}

	// Presence
	async getSessionPresence(id: string): Promise<SessionPresenceResponse> {
		return this.request(`/sessions/${id}/presence`);
	}

	async heartbeatPresence(
		sessionId: string,
		clientId: string,
		surface: string,
		capability: string,
		lastEventSequence?: number | null,
	): Promise<SessionPresenceResponse> {
		return this.request(`/sessions/${sessionId}/presence`, {
			method: "PUT",
			body: JSON.stringify({
				client_id: clientId,
				surface,
				capability,
				last_event_sequence: lastEventSequence ?? undefined,
			}),
		});
	}

	// Models
	async getModels(): Promise<ModelsResponse> {
		return this.request("/models");
	}

	async setCurrentModel(
		model: string | null,
		modelKey?: ModelKey | null,
	): Promise<SimpleOkResponse> {
		return this.request("/models/current", {
			method: "PUT",
			body: JSON.stringify({
				model,
				model_key: modelKey ?? undefined,
			}),
		});
	}

	// Git
	async getGitStatus(path?: string): Promise<GitStatusResponse> {
		const q = path ? `?path=${encodeURIComponent(path)}` : "";
		return this.request(`/git/status${q}`);
	}

	async getGitChanges(path?: string): Promise<GitChangesResponse> {
		const q = path ? `?path=${encodeURIComponent(path)}` : "";
		return this.request(`/git/changes${q}`);
	}

	async getGitFileDiff(
		file: string,
		path?: string,
	): Promise<GitFileDiffResponse> {
		const params = new URLSearchParams({ file });
		if (path) params.set("path", path);
		return this.request(`/git/diff?${params}`);
	}

	async getGitBranches(path?: string): Promise<GitBranchesResponse> {
		const q = path ? `?path=${encodeURIComponent(path)}` : "";
		return this.request(`/git/branches${q}`);
	}

	async getGitWorktrees(path?: string): Promise<GitWorktreesResponse> {
		const q = path ? `?path=${encodeURIComponent(path)}` : "";
		return this.request(`/git/worktrees${q}`);
	}

	async checkoutGitBranch(
		branch: string,
		path?: string,
		create?: boolean,
		startPoint?: string,
	): Promise<GitStatusResponse> {
		return this.request("/git/checkout", {
			method: "POST",
			body: JSON.stringify({
				branch,
				path: path ?? undefined,
				create: create ?? undefined,
				start_point: startPoint ?? undefined,
			}),
		});
	}

	// Credentials
	async getCredentials(): Promise<ProviderStatus[]> {
		return this.request("/credentials");
	}

	async setCredential(
		providerId: string,
		apiKey: string,
	): Promise<ProviderStatus> {
		return this.request(`/credentials/${providerId}`, {
			method: "POST",
			body: JSON.stringify({ api_key: apiKey }),
		});
	}

	async deleteCredential(providerId: string): Promise<void> {
		await this.request(`/credentials/${providerId}`, { method: "DELETE" });
	}

	async startOAuth(provider: string): Promise<OAuthStartResponse> {
		return this.request("/auth/oauth/start", {
			method: "POST",
			body: JSON.stringify({ provider }),
		});
	}

	async getOAuthStatus(provider: string): Promise<OAuthStatusResponse> {
		return this.request(`/auth/oauth/status/${provider}`);
	}

	async exchangeOAuthCode(
		provider: string,
		code: string,
	): Promise<OAuthExchangeResponse> {
		return this.request("/auth/oauth/exchange", {
			method: "POST",
			body: JSON.stringify({ provider, code }),
		});
	}

	async revokeOAuth(provider: string): Promise<OAuthStatusResponse> {
		return this.request(`/auth/oauth/revoke/${provider}`, { method: "DELETE" });
	}

	// Files
	async getFileTree(
		root?: string,
		depth = 3,
	): Promise<{ root: string; entries: TreeEntry[] }> {
		const params = new URLSearchParams();
		if (root) params.set("root", root);
		params.set("depth", String(depth));
		return this.request(`/files/tree?${params}`);
	}

	async getFile(
		path: string,
	): Promise<{ path: string; content: string; size: number }> {
		return this.request(`/files?path=${encodeURIComponent(path)}`);
	}

	// Server
	async getServerAccess(): Promise<ServerAccessResponse> {
		return this.request("/server/access");
	}

	// Preview / Ports
	async listBrowserSessions(): Promise<BrowserSessionListResponse> {
		return this.request("/browser");
	}

	async createBrowserSession(
		request: CreateBrowserSessionRequest,
	): Promise<BrowserSession> {
		return this.request("/browser", {
			method: "POST",
			body: JSON.stringify(request),
		});
	}

	async getBrowserSession(id: string): Promise<BrowserSession> {
		return this.request(`/browser/${encodeURIComponent(id)}`);
	}

	async stopBrowserSession(
		id: string,
		lease: BrowserControlLease,
	): Promise<BrowserSession> {
		const leaseToken = this.browserLeaseToken(id, lease);
		return this.request(`/browser/${encodeURIComponent(id)}/stop`, {
			method: "POST",
			body: JSON.stringify({ lease_token: leaseToken }),
		});
	}

	async takeBrowserControl(
		id: string,
		clientId: string,
	): Promise<TakeBrowserControlResponse> {
		const normalizedClientId = clientId.trim();
		if (!normalizedClientId) {
			throw new Error("Browser controller client id cannot be empty");
		}
		return this.request(`/browser/${encodeURIComponent(id)}/control`, {
			method: "POST",
			body: JSON.stringify({ client_id: normalizedClientId }),
		});
	}

	async heartbeatBrowserSession(
		id: string,
		capability: "viewer" | "controller" = "viewer",
		clientId?: string,
		lease?: BrowserControlLease,
	): Promise<BrowserSession> {
		const leaseToken = capability === "controller"
			? this.browserLeaseToken(id, lease)
			: undefined;
		if (capability === "viewer" && lease) {
			throw new Error("Browser observers must not carry a control lease");
		}
		return this.request(`/browser/${encodeURIComponent(id)}/heartbeat`, {
			method: "POST",
			body: JSON.stringify({
				capability,
				client_id: clientId,
				lease_token: leaseToken,
			}),
		});
	}

	async runBrowserActions(
		id: string,
		lease: BrowserControlLease,
		actions: BrowserAction[],
	): Promise<BrowserActionResponse> {
		const leaseToken = this.browserLeaseToken(id, lease);
		return this.request(`/browser/${encodeURIComponent(id)}/actions`, {
			method: "POST",
			body: JSON.stringify({ lease_token: leaseToken, actions }),
		});
	}

	/**
	 * Build the browser frame WebSocket URL. The configured account bearer is
	 * deliberately never copied into this URL; HTTP authentication remains an
	 * Authorization header and the only query secret is the resource-scoped
	 * Browser control lease required by the server stream contract.
	 */
	browserStreamUrl(
		streamPath: string,
		capability: "viewer" | "controller",
		clientId: string,
		lease?: BrowserControlLease,
	): string {
		const match = streamPath.match(/^\/api\/browser\/([^/?#]+)\/stream$/);
		if (!match) throw new Error("Invalid Mitsuro Browser stream path");
		const normalizedClientId = clientId.trim();
		if (!normalizedClientId) throw new Error("Browser client id cannot be empty");
		const resourceId = decodeURIComponent(match[1] as string);
		const leaseToken = capability === "controller"
			? this.browserLeaseToken(resourceId, lease)
			: undefined;
		if (capability === "viewer" && lease) {
			throw new Error("Browser observers must not carry a control lease");
		}

		const base = new URL(this.baseUrl);
		if (base.protocol !== "http:" && base.protocol !== "https:") {
			throw new Error("Unsupported Mitsuro Browser server URL scheme");
		}
		const url = new URL(streamPath, `${base.origin}/`);
		url.protocol = base.protocol === "https:" ? "wss:" : "ws:";
		url.searchParams.set("capability", capability);
		url.searchParams.set("client_id", normalizedClientId);
		if (leaseToken) url.searchParams.set("lease_token", leaseToken);
		return url.toString();
	}

	private browserLeaseToken(
		resourceId: string,
		lease?: BrowserControlLease,
	): string {
		if (!lease || lease.resource_id !== resourceId || !lease.lease_token.trim()) {
			throw new Error("Browser control lease is required for mutations");
		}
		return lease.lease_token.trim();
	}

	async runBrowserAgent(
		id: string,
		request: BrowserAgentRequest,
	): Promise<BrowserAgentResponse> {
		return this.request(`/browser/${encodeURIComponent(id)}/agent`, {
			method: "POST",
			body: JSON.stringify(request),
		});
	}

	async getPorts(): Promise<PortListResponse> {
		return this.request("/ports");
	}

	async getPreviewSettings(): Promise<PreviewSettings> {
		return this.request("/settings/preview");
	}

	async updatePreviewSettings(
		patch: PreviewSettingsPatch,
	): Promise<PreviewSettings> {
		return this.request("/settings/preview", {
			method: "PATCH",
			body: JSON.stringify(patch),
		});
	}

	async addPinnedPort(port: number): Promise<PreviewSettings> {
		return this.request("/settings/preview/pins", {
			method: "POST",
			body: JSON.stringify({ port }),
		});
	}

	async removePinnedPort(port: number): Promise<PreviewSettings> {
		return this.request(`/settings/preview/pins/${port}`, { method: "DELETE" });
	}

	async addHiddenPort(port: number): Promise<PreviewSettings> {
		return this.request("/settings/preview/hidden", {
			method: "POST",
			body: JSON.stringify({ port }),
		});
	}

	async removeHiddenPort(port: number): Promise<PreviewSettings> {
		return this.request(`/settings/preview/hidden/${port}`, {
			method: "DELETE",
		});
	}

	// MCP
	async getMcpServers(): Promise<McpServerResponse[]> {
		return this.request("/mcp");
	}

	async reloadMcpConfig(): Promise<McpServerResponse[]> {
		return this.request("/mcp/reload", { method: "POST" });
	}

	async connectMcpServer(name: string): Promise<McpServerResponse> {
		return this.request(`/mcp/${encodeURIComponent(name)}/connect`, {
			method: "POST",
		});
	}

	async disconnectMcpServer(name: string): Promise<McpServerResponse> {
		return this.request(`/mcp/${encodeURIComponent(name)}/disconnect`, {
			method: "POST",
		});
	}

	async getMcpServerTools(name: string): Promise<McpToolResponse[]> {
		return this.request(`/mcp/${encodeURIComponent(name)}/tools`);
	}

	// Skills
	async getSkills(scope: "all" | "global" = "all"): Promise<SkillInfo[]> {
		const query = scope === "global" ? "?scope=global" : "";
		return this.request(`/skills${query}`);
	}

	async updateSkillPolicy(
		name: string,
		update: { enabled?: boolean; permission?: SkillInfo["permission"] },
	): Promise<SkillInfo> {
		return this.request(`/skills/${encodeURIComponent(name)}/policy`, {
			method: "POST",
			body: JSON.stringify(update),
		});
	}

	// Tools
	async steerSession(
		request: SteerRequest,
		options?: { idempotencyKey?: string },
	): Promise<SteerResponse> {
		const controlHeaders = await this.agentMutationHeaders(request.session_id);
		return this.request("/chat/steer", {
			method: "POST",
			headers: {
				...controlHeaders,
				...(options?.idempotencyKey
					? { "Idempotency-Key": options.idempotencyKey }
					: {}),
			},
			body: JSON.stringify(request),
		});
	}

	async submitToolApproval(
		sessionId: string,
		toolCallId: string,
		approved: boolean,
		idempotencyKey?: string,
	): Promise<{ status: string }> {
		const controlHeaders = await this.agentMutationHeaders(sessionId);
		return this.request("/chat/tool-approval", {
			method: "POST",
			headers: {
				...controlHeaders,
				...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {}),
			},
			body: JSON.stringify({
				session_id: sessionId,
				tool_call_id: toolCallId,
				approved,
			}),
		});
	}

	async cancelSession(sessionId: string): Promise<SimpleOkResponse> {
		const controlHeaders = await this.agentMutationHeaders(sessionId);
		return this.request(`/sessions/${encodeURIComponent(sessionId)}/cancel`, {
			method: "POST",
			headers: controlHeaders,
		});
	}

	async submitToolResult(
		sessionId: string,
		toolCallId: string,
		result: string,
		fastMode = false,
		permissionMode?: ToolResultRequest["permission_mode"],
	): Promise<void> {
		const controlHeaders = await this.agentMutationHeaders(sessionId);
		await this.request("/chat/tool-result", {
			method: "POST",
			headers: controlHeaders,
			body: JSON.stringify({
				session_id: sessionId,
				tool_call_id: toolCallId,
				result,
				fast_mode: fastMode,
				permission_mode: permissionMode,
			}),
		});
	}

	// Presence (aliases for backward compat)
	async heartbeatSessionPresence(
		sessionId: string,
		data: {
			client_id: string;
			surface: string;
			capability: string;
			last_event_sequence?: number | null;
		},
	): Promise<SessionPresenceResponse> {
		return this.heartbeatPresence(
			sessionId,
			data.client_id,
			data.surface,
			data.capability,
			data.last_event_sequence,
		);
	}

	async removeSessionPresence(
		sessionId: string,
		clientId: string,
	): Promise<SessionPresenceResponse> {
		return this.request(`/sessions/${sessionId}/presence/${clientId}`, {
			method: "DELETE",
		});
	}

	// Pinch
	async pinchSession(
		sessionId: string,
		preservationHints?: string,
		direction?: string,
	): Promise<{
		session: SessionResponse;
		summary: string;
		key_decisions: string[];
		pending_tasks: string[];
		estimated_tokens_before?: number;
		estimated_tokens_after?: number;
		replaced_messages?: number;
		checkpoint_id?: string;
		compaction_count?: number;
	}> {
		return this.request(`/sessions/${sessionId}/pinch`, {
			method: "POST",
			body: JSON.stringify({
				preservation_hints: preservationHints,
				direction,
			}),
		});
	}

	// Directories
	async getDirectories(): Promise<string[]> {
		return this.request("/sessions/directories");
	}

	async browseDirectories(path?: string): Promise<{
		current: string;
		parent: string | null;
		directories: Array<{ name: string; path: string }>;
	}> {
		const q = path ? `?path=${encodeURIComponent(path)}` : "";
		return this.request(`/files/browse${q}`);
	}

	// Reports
	async getReports(options?: {
		projectDir?: string;
		query?: string;
	}): Promise<{ reports: ReportSummary[] }> {
		const params: string[] = [];
		if (options?.projectDir) {
			params.push(`project_dir=${encodeURIComponent(options.projectDir)}`);
		}
		if (options?.query) {
			params.push(`query=${encodeURIComponent(options.query)}`);
		}
		const q = params.length > 0 ? `?${params.join("&")}` : "";
		return this.request(`/reports${q}`);
	}

	async getReport(id: string): Promise<Report> {
		return this.request(`/reports/${id}`);
	}

	async promoteReportToMemory(
		id: string,
		options?: { memoryType?: MemoryType },
	): Promise<PromoteReportToMemoryResponse> {
		return this.request(`/reports/${id}/promote`, {
			method: "POST",
			body: JSON.stringify({
				memory_type: options?.memoryType ?? undefined,
			}),
		});
	}

	async getMemories(
		projectDir?: string,
		memoryType?: MemoryType,
		options?: { includeContent?: boolean },
	): Promise<{ memories: AgentMemory[] }> {
		const params: string[] = [];
		if (projectDir) {
			params.push(`project_dir=${encodeURIComponent(projectDir)}`);
		}
		if (memoryType) {
			params.push(`memory_type=${encodeURIComponent(memoryType)}`);
		}
		if (options?.includeContent) {
			params.push("include_content=true");
		}
		const q = params.join("&");
		return this.request(`/memories${q ? `?${q}` : ""}`);
	}

	async getMemorySnapshot(
		projectDir?: string,
	): Promise<MemorySnapshotResponse> {
		const q = projectDir
			? `?project_dir=${encodeURIComponent(projectDir)}`
			: "";
		return this.request(`/memories/snapshot${q}`);
	}

	// Hive
	async dispatchHive(
		task: string,
		options?: HiveDispatchOptions,
	): Promise<HiveDispatchResponse> {
		return this.request("/hive/dispatch", {
			method: "POST",
			body: JSON.stringify({
				task,
				project_dir: options?.projectDir ?? undefined,
				model: options?.model ?? undefined,
				model_key: options?.modelKey ?? undefined,
				start_at: options?.startAt ?? undefined,
				priority: options?.priority ?? undefined,
				crew_slug: options?.crewSlug ?? undefined,
			}),
		});
	}

	/** Ensure/get the durable singleton Hive companion chat for this user. */
	async getHiveMain(): Promise<HiveMainResponse> {
		return this.request("/hive/main");
	}

	/** Same as getHiveMain — POST is accepted for ensure semantics. */
	async ensureHiveMain(): Promise<HiveMainResponse> {
		return this.request("/hive/main", { method: "POST" });
	}

	/**
	 * User-scoped global schedule list for the Hive Schedule secondary surface.
	 * Ordered by next fire time across all of the caller's controllers.
	 */
	async listHiveSchedules(options?: {
		limit?: number;
	}): Promise<HiveGlobalSchedule[]> {
		const params: string[] = [];
		if (options?.limit != null) {
			params.push(`limit=${encodeURIComponent(String(options.limit))}`);
		}
		const q = params.length > 0 ? `?${params.join("&")}` : "";
		return this.request(`/hive/schedules${q}`);
	}

	/** List schedules attached to a specific Hive controller session. */
	async listHiveSessionSchedules(
		sessionId: string,
		options?: { limit?: number },
	): Promise<HiveSchedule[]> {
		const params: string[] = [];
		if (options?.limit != null) {
			params.push(`limit=${encodeURIComponent(String(options.limit))}`);
		}
		const q = params.length > 0 ? `?${params.join("&")}` : "";
		return this.request(
			`/hive/sessions/${encodeURIComponent(sessionId)}/schedules${q}`,
		);
	}

	async createHiveSchedule(
		sessionId: string,
		request: HiveScheduleWriteRequest,
		options?: { idempotencyKey?: string },
	): Promise<HiveScheduleMutationResponse> {
		const headers: Record<string, string> = {};
		if (options?.idempotencyKey) {
			headers["Idempotency-Key"] = options.idempotencyKey;
		}
		return this.request(
			`/hive/sessions/${encodeURIComponent(sessionId)}/schedules`,
			{
				method: "POST",
				headers,
				body: JSON.stringify(request),
			},
		);
	}

	async pauseHiveSchedule(
		sessionId: string,
		scheduleId: string,
		revision: number,
		options?: { idempotencyKey?: string },
	): Promise<HiveScheduleMutationResponse> {
		const headers: Record<string, string> = {
			"If-Match": `"${revision}"`,
		};
		if (options?.idempotencyKey) {
			headers["Idempotency-Key"] = options.idempotencyKey;
		}
		return this.request(
			`/hive/sessions/${encodeURIComponent(sessionId)}/schedules/${encodeURIComponent(
				scheduleId,
			)}/pause`,
			{ method: "POST", headers },
		);
	}

	async resumeHiveSchedule(
		sessionId: string,
		scheduleId: string,
		revision: number,
		options?: { idempotencyKey?: string },
	): Promise<HiveScheduleMutationResponse> {
		const headers: Record<string, string> = {
			"If-Match": `"${revision}"`,
		};
		if (options?.idempotencyKey) {
			headers["Idempotency-Key"] = options.idempotencyKey;
		}
		return this.request(
			`/hive/sessions/${encodeURIComponent(sessionId)}/schedules/${encodeURIComponent(
				scheduleId,
			)}/resume`,
			{ method: "POST", headers },
		);
	}

	async getHiveCurrent(): Promise<HiveCurrentResponse> {
		return this.request("/hive/current");
	}

	async getHiveAttention(options?: {
		threadSessionId?: string | null;
	}): Promise<HiveAttentionResponse> {
		const params: string[] = [];
		if (options?.threadSessionId) {
			params.push(
				`thread_session_id=${encodeURIComponent(options.threadSessionId)}`,
			);
		}
		const q = params.length > 0 ? `?${params.join("&")}` : "";
		return this.request(`/hive/attention${q}`);
	}

	async setHiveAttentionRead(
		itemId: string,
		read: boolean,
	): Promise<SimpleOkResponse> {
		return this.request(`/hive/attention/${encodeURIComponent(itemId)}/read`, {
			method: "POST",
			body: JSON.stringify({ read }),
		});
	}

	async setHiveAttentionCleared(
		itemId: string,
		cleared: boolean,
	): Promise<SimpleOkResponse> {
		return this.request(`/hive/attention/${encodeURIComponent(itemId)}/clear`, {
			method: "POST",
			body: JSON.stringify({ cleared }),
		});
	}

	async getHiveHome(): Promise<HiveHomeResponse> {
		return this.request("/hive/home");
	}

	async bootstrapHiveHome(): Promise<HiveBootstrapResponse> {
		return this.request("/hive/home/bootstrap", { method: "POST" });
	}

	async updateHiveHomeDocument(
		kind: HiveHomeDocumentKind,
		content: string,
	): Promise<HiveHomeResponse> {
		return this.request(`/hive/home/${encodeURIComponent(kind)}`, {
			method: "PUT",
			body: JSON.stringify({ content }),
		});
	}

	async updateHiveCrewDocument(
		slug: string,
		kind: HiveCrewDocumentKind,
		content: string,
	): Promise<HiveHomeResponse> {
		return this.request(
			`/hive/home/crew/${encodeURIComponent(slug)}/${encodeURIComponent(kind)}`,
			{
				method: "PUT",
				body: JSON.stringify({ content }),
			},
		);
	}

	async getHiveCrew(): Promise<HiveCrewResponse> {
		return this.request("/hive/crew");
	}

	async getHiveChannels(): Promise<HiveChannelsResponse> {
		return this.request("/hive/channels");
	}

	async recoverHiveDaemon(): Promise<HiveRecoverDaemonResponse> {
		return this.request("/hive/daemon/recover", { method: "POST" });
	}

	async listHiveSessions(): Promise<HiveSessionSummary[]> {
		return this.request("/hive/sessions");
	}

	async getHiveSessionStatus(id: string): Promise<HiveSessionStatus> {
		return this.request(`/hive/sessions/${id}/status`);
	}

	async sendHiveMessage(
		id: string,
		message: string,
	): Promise<SimpleOkResponse> {
		return this.request(`/hive/sessions/${id}/message`, {
			method: "POST",
			body: JSON.stringify({ message }),
		});
	}

	async scheduleHiveSession(
		id: string,
		startAt: string,
	): Promise<SimpleOkResponse> {
		return this.request(`/hive/sessions/${id}/schedule`, {
			method: "POST",
			body: JSON.stringify({ start_at: startAt }),
		});
	}

	async setHiveSessionPriority(
		id: string,
		priority: HiveRunPriority,
	): Promise<SimpleOkResponse> {
		return this.request(`/hive/sessions/${id}/priority`, {
			method: "POST",
			body: JSON.stringify({ priority }),
		});
	}

	async setHiveSessionCrew(
		id: string,
		crewSlug?: string | null,
	): Promise<SimpleOkResponse> {
		return this.request(`/hive/sessions/${id}/crew`, {
			method: "POST",
			body: JSON.stringify({ crew_slug: crewSlug ?? null }),
		});
	}

	async pauseHiveSession(id: string): Promise<SimpleOkResponse> {
		return this.request(`/hive/sessions/${id}/pause`, { method: "POST" });
	}

	async resumeHiveSession(id: string): Promise<SimpleOkResponse> {
		return this.request(`/hive/sessions/${id}/resume`, { method: "POST" });
	}

	async cancelHiveSession(id: string): Promise<void> {
		await this.request(`/hive/sessions/${id}`, { method: "DELETE" });
	}

	async observeHiveSession(
		id: string,
		callbacks: StreamCallbacks,
		signal?: AbortSignal,
	): Promise<void> {
		return this.streamSSERequest(
			`/hive/sessions/${id}/events`,
			"GET",
			undefined,
			callbacks,
			signal,
		);
	}

	// ============================================================================
	// Hive Workers
	// ============================================================================

	async listHiveWorkers(): Promise<HiveWorkersResponse> {
		return this.request("/hive/workers");
	}

	async createHiveWorker(
		request: CreateHiveWorkerRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request("/hive/workers", {
			method: "POST",
			headers: { "Idempotency-Key": options.idempotencyKey },
			body: JSON.stringify(request),
		});
	}

	async getHiveWorker(
		id: string,
		options?: { signal?: AbortSignal },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}`, { signal: options?.signal });
	}

	async getHiveWorkerBySession(
		sessionId: string,
		options?: { signal?: AbortSignal },
	): Promise<HiveWorkerSessionBindingResponse> {
		return this.request(
			`/hive/workers/by-session/${encodeURIComponent(sessionId)}`,
			{ signal: options?.signal },
		);
	}

	async getHiveWorkerGovernor(
		id: string,
		options?: { signal?: AbortSignal },
	): Promise<HiveWorkerGovernorProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/governor`,
			{ signal: options?.signal },
		);
	}

	async grantHiveWorkerGovernorRecovery(
		id: string,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGovernorRecoveryResponse> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/governor/recovery`,
			{
				method: "POST",
				headers: { "Idempotency-Key": options.idempotencyKey },
			},
		);
	}

	async getHiveWorkerGoal(
		id: string,
		options?: { signal?: AbortSignal },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workflow`,
			{ signal: options?.signal },
		);
	}

	async approveHiveWorkerGoal(
		id: string,
		request: ApproveHiveWorkerGoalRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workflow/approve`,
			{
				method: "POST",
				headers: { "Idempotency-Key": options.idempotencyKey },
				body: JSON.stringify(request),
			},
		);
	}

	async createHiveWorkerGoal(
		id: string,
		request: CreateHiveWorkerGoalRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workflow`,
			{
				method: "POST",
				headers: { "Idempotency-Key": options.idempotencyKey },
				body: JSON.stringify(request),
			},
		);
	}

	async activateHiveWorkerGoal(
		id: string,
		request: MutateHiveWorkerGoalRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workflow/activate`,
			{
				method: "POST",
				headers: { "Idempotency-Key": options.idempotencyKey },
				body: JSON.stringify(request),
			},
		);
	}

	async pauseHiveWorkerGoal(
		id: string,
		request: TransitionHiveWorkerGoalRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workflow/pause`,
			{
				method: "POST",
				headers: { "Idempotency-Key": options.idempotencyKey },
				body: JSON.stringify(request),
			},
		);
	}

	async cancelHiveWorkerGoal(
		id: string,
		request: TransitionHiveWorkerGoalRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workflow/cancel`,
			{
				method: "POST",
				headers: { "Idempotency-Key": options.idempotencyKey },
				body: JSON.stringify(request),
			},
		);
	}

	async resolveHiveWorkerGoalAcceptance(
		id: string,
		request: ResolveHiveWorkerGoalAcceptanceRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workflow/acceptance`,
			{
				method: "POST",
				headers: { "Idempotency-Key": options.idempotencyKey },
				body: JSON.stringify(request),
			},
		);
	}

	async setHiveWorkerWorkspace(
		id: string,
		request: SetHiveWorkerWorkspaceRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerGoalProjection> {
		return this.request(
			`/hive/workers/${encodeURIComponent(id)}/workspace`,
			{
				method: "PUT",
				headers: { "Idempotency-Key": options.idempotencyKey },
				body: JSON.stringify(request),
			},
		);
	}

	async retryHiveWorkerIntroduction(
		id: string,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}/introduction/retry`, {
			method: "POST",
			headers: { "Idempotency-Key": options.idempotencyKey },
		});
	}

	async skipHiveWorkerIntroduction(
		id: string,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}/introduction/skip`, {
			method: "POST",
			headers: { "Idempotency-Key": options.idempotencyKey },
		});
	}

	async confirmHiveWorkerIntroduction(
		id: string,
		request: ConfirmHiveWorkerIntroductionRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}/introduction/confirm`, {
			method: "POST",
			headers: { "Idempotency-Key": options.idempotencyKey },
			body: JSON.stringify(request),
		});
	}

	async keepTalkingHiveWorkerIntroduction(
		id: string,
		request: KeepTalkingHiveWorkerIntroductionRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}/introduction/keep-talking`, {
			method: "POST",
			headers: { "Idempotency-Key": options.idempotencyKey },
			body: JSON.stringify(request),
		});
	}

	async updateHiveWorker(
		id: string,
		request: UpdateHiveWorkerRequest,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}`, {
			method: "PATCH",
			headers: { "Idempotency-Key": options.idempotencyKey },
			body: JSON.stringify(request),
		});
	}

	async pauseHiveWorker(
		id: string,
		expectedRevision: number,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}/pause`, {
			method: "POST",
			headers: { "Idempotency-Key": options.idempotencyKey },
			body: JSON.stringify({ expected_revision: expectedRevision }),
		});
	}

	async resumeHiveWorker(
		id: string,
		expectedRevision: number,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}/resume`, {
			method: "POST",
			headers: { "Idempotency-Key": options.idempotencyKey },
			body: JSON.stringify({ expected_revision: expectedRevision }),
		});
	}

	/** Archive (never hard-delete); the Worker's history and DM survive. */
	async archiveHiveWorker(
		id: string,
		expectedRevision: number,
		options: { idempotencyKey: string },
	): Promise<HiveWorkerDetail> {
		return this.request(`/hive/workers/${id}`, {
			method: "DELETE",
			headers: { "Idempotency-Key": options.idempotencyKey },
			body: JSON.stringify({ expected_revision: expectedRevision }),
		});
	}

	/** Ensure the Worker's private DM session exists and return its summary. */
	async ensureHiveWorkerDm(id: string): Promise<HiveWorkerDmResponse> {
		return this.request(`/hive/workers/${id}/dm`, { method: "POST" });
	}

	async listHiveWorkerDeliveries(
		id: string,
		query?: { status?: HiveDeliveryStatus; limit?: number },
	): Promise<HiveWorkerDeliveriesResponse> {
		const params = new URLSearchParams();
		if (query?.status) params.set("status", query.status);
		if (query?.limit != null) params.set("limit", String(query.limit));
		const encoded = params.toString();
		const suffix = encoded ? `?${encoded}` : "";
		return this.request(`/hive/workers/${id}/deliveries${suffix}`);
	}

	// ============================================================================
	// Hive Groups
	// ============================================================================

	async listHiveGroups(): Promise<HiveGroupsResponse> {
		return this.request("/hive/groups");
	}

	async createHiveGroup(
		request: CreateHiveGroupRequest,
	): Promise<HiveGroupDetail> {
		return this.request("/hive/groups", {
			method: "POST",
			body: JSON.stringify(request),
		});
	}

	async getHiveGroup(id: string): Promise<HiveGroupDetail> {
		return this.request(`/hive/groups/${id}`);
	}

	async updateHiveGroup(
		id: string,
		request: UpdateHiveGroupRequest,
	): Promise<HiveGroupDetail> {
		return this.request(`/hive/groups/${id}`, {
			method: "PATCH",
			body: JSON.stringify(request),
		});
	}

	/** Archive (never hard-delete); the room timeline and Workers survive. */
	async archiveHiveGroup(id: string): Promise<SimpleOkResponse> {
		return this.request(`/hive/groups/${id}`, { method: "DELETE" });
	}

	/** Send one room message; the daemon fans out a durable group turn. */
	async sendHiveGroupMessage(
		id: string,
		request: SendHiveGroupMessageRequest,
		idempotencyKey?: string,
	): Promise<SendHiveGroupMessageResponse> {
		return this.request(`/hive/groups/${id}/messages`, {
			method: "POST",
			body: JSON.stringify(request),
			headers: idempotencyKey
				? { "Idempotency-Key": idempotencyKey }
				: undefined,
		});
	}

	async listHiveGroupMessages(
		id: string,
		options: { afterSeq?: number; limit?: number } = {},
	): Promise<HiveGroupMessagesResponse> {
		const query = new URLSearchParams();
		if (options.afterSeq !== undefined) {
			query.set("after_seq", String(options.afterSeq));
		}
		if (options.limit !== undefined) {
			query.set("limit", String(options.limit));
		}
		const encoded = query.toString();
		const suffix = encoded ? `?${encoded}` : "";
		return this.request(`/hive/groups/${id}/messages${suffix}`);
	}

	async getHiveGroupTurn(id: string, turnId: string): Promise<HiveGroupTurn> {
		return this.request(`/hive/groups/${id}/turns/${turnId}`);
	}

	/** Cancel the active turn's in-flight member runs. */
	async stopHiveGroup(id: string): Promise<SimpleOkResponse> {
		return this.request(`/hive/groups/${id}/stop`, { method: "POST" });
	}

	/**
	 * Tail the room event stream (message appends and turn transitions).
	 * Passing `afterSeq` replays messages after that cursor; omitting it
	 * starts live at the current high-water mark.
	 */
	async observeHiveGroup(
		id: string,
		onEvent: (event: HiveGroupEvent) => void,
		options: { afterSeq?: number; signal?: AbortSignal } = {},
	): Promise<void> {
		const query =
			options.afterSeq !== undefined ? `?after_seq=${options.afterSeq}` : "";
		const response = await this.fetchWithHiveCompatibility(
			`/hive/groups/${id}/events${query}`,
			{
				method: "GET",
				headers: this.headers(),
				signal: options.signal,
			},
		);
		if (!response.ok || !response.body) {
			const text = await response.text().catch(() => "Stream failed");
			throw new MitsuroApiError(
				response.status,
				apiErrorMessage(text, "Group event stream failed"),
				text,
			);
		}
		const reader = response.body.getReader();
		const decoder = new TextDecoder();
		let buffer = "";
		try {
			for (;;) {
				const { done, value } = await reader.read();
				if (done) break;
				buffer += decoder.decode(value, { stream: true });
				const frames = buffer.split("\n\n");
				buffer = frames.pop() ?? "";
				for (const frame of frames) {
					for (const line of frame.split("\n")) {
						if (!line.startsWith("data:")) continue;
						const payload = line.slice(5).trim();
						if (!payload) continue;
						try {
							onEvent(JSON.parse(payload) as HiveGroupEvent);
						} catch {
							// Skip malformed frames; the durable cursor makes
							// a reconnect lossless.
						}
					}
				}
			}
		} finally {
			reader.releaseLock();
		}
	}

	// ============================================================================
	// Streaming
	// ============================================================================

	async streamChat(
		request: ChatRequest,
		callbacks: StreamCallbacks,
		signal?: AbortSignal,
		options?: { idempotencyKey?: string },
	): Promise<void> {
		const controlHeaders =
			request.session_id &&
			(request.session_type === undefined || request.session_type === "code")
				? await this.agentMutationHeaders(request.session_id)
				: {};
		const body = await this.encodeRequestIdentityForServer(
			request as ChatRequest & Record<string, unknown>,
			request.session_type,
			signal,
		);
		return this.streamSSERequest(
			"/chat",
			"POST",
			body,
			callbacks,
			signal,
			{ ...options, headers: controlHeaders },
		);
	}

	async streamToolResult(
		request: ToolResultRequest,
		callbacks: StreamCallbacks,
		signal?: AbortSignal,
	): Promise<void> {
		const controlHeaders = await this.agentMutationHeaders(request.session_id);
		return this.streamSSERequest(
			"/chat/tool-result",
			"POST",
			request,
			callbacks,
			signal,
			{ headers: controlHeaders },
		);
	}

	private async streamSSERequest(
		path: string,
		method: "GET" | "POST",
		body: object | undefined,
		callbacks: StreamCallbacks,
		signal?: AbortSignal,
		options?: {
			idempotencyKey?: string;
			headers?: Record<string, string>;
		},
	): Promise<void> {
		const diagnosticName = "api.stream";
		const startedAt = monotonicNow();
		this.observeRequest(diagnosticName, "start");
		let response: Response;
		try {
			const requestOptions: RequestInit = {
				method,
				headers: {
					...this.headers(),
					Accept: "text/event-stream",
					...(options?.idempotencyKey
						? { "Idempotency-Key": options.idempotencyKey }
						: {}),
					...options?.headers,
				},
				body: body ? JSON.stringify(body) : undefined,
				signal,
			};
			response = await this.fetchWithHiveCompatibility(path, requestOptions);
		} catch (error) {
			this.observeRequest(
				diagnosticName,
				signal?.aborted ? "cancel" : "error",
				startedAt,
				signal?.aborted ? "request.abort" : "network.error",
			);
			if (signal?.aborted) return;
			callbacks.onError(
				error instanceof Error ? error.message : "Stream error",
			);
			return;
		}

		if (!response.ok) {
			this.observeRequest(
				diagnosticName,
				"error",
				startedAt,
				httpStatusClass(response.status),
			);
			const text = await response.text().catch(() => "Stream failed");
			callbacks.onError(
				`API ${response.status}: ${apiErrorMessage(text, "Stream failed")}`,
			);
			return;
		}
		this.observeRequest(
			diagnosticName,
			"complete",
			startedAt,
			httpStatusClass(response.status),
		);

		let terminalEventSeen = false;
		let errorReported = false;
		const trackedCallbacks: StreamCallbacks = {
			...callbacks,
			onFinish: (sessionId, stopReason) => {
				terminalEventSeen = true;
				callbacks.onFinish(sessionId, stopReason);
			},
			onError: (error) => {
				terminalEventSeen = true;
				if (errorReported) return;
				errorReported = true;
				callbacks.onError(error);
			},
		};
		const reportPrematureEnd = () => {
			if (!signal?.aborted && !terminalEventSeen) {
				trackedCallbacks.onError(
					"Stream ended before the server reported completion. Recovering the session state.",
				);
			}
		};

		const reader = response.body?.getReader?.();
		if (!reader) {
			const fallbackText = await response.text().catch(() => "");
			if (!fallbackText) {
				trackedCallbacks.onError("No response body");
				return;
			}
			this.processSSEChunk(fallbackText, trackedCallbacks, true);
			reportPrematureEnd();
			return;
		}

		const decoder = new TextDecoder();
		let buffer = "";
		let lastActivity = Date.now();

		const activityCheck = setInterval(() => {
			if (Date.now() - lastActivity > STREAM_ACTIVITY_TIMEOUT) {
				trackedCallbacks.onError(
					`Stream timeout — no activity for ${Math.round(
						STREAM_ACTIVITY_TIMEOUT / 1000,
					)}s`,
				);
				reader.cancel();
				clearInterval(activityCheck);
			}
		}, 5000);

		try {
			while (true) {
				const { done, value } = await reader.read();
				if (done) break;

				lastActivity = Date.now();
				buffer = this.processSSEChunk(
					buffer + decoder.decode(value, { stream: true }),
					trackedCallbacks,
				);
			}

			buffer += decoder.decode();
			this.processSSEChunk(buffer, trackedCallbacks, true);
			reportPrematureEnd();
		} catch (err) {
			if (signal?.aborted) return;
			if (!terminalEventSeen) {
				trackedCallbacks.onError(
					err instanceof Error ? err.message : "Stream error",
				);
			}
		} finally {
			clearInterval(activityCheck);
		}
	}

	private processSSEChunk(
		chunk: string,
		callbacks: StreamCallbacks,
		flush = false,
	): string {
		let remainder = chunk;
		const eventBoundary = /\r?\n\r?\n/;

		while (true) {
			const match = eventBoundary.exec(remainder);
			if (!match) break;
			this.processSSEEvent(remainder.slice(0, match.index), callbacks);
			remainder = remainder.slice(match.index + match[0].length);
		}

		if (flush && remainder) {
			this.processSSEEvent(remainder, callbacks);
			return "";
		}

		return remainder;
	}

	private processSSEEvent(block: string, callbacks: StreamCallbacks): void {
		const data: string[] = [];
		for (const rawLine of block.split(/\r?\n/)) {
			if (!rawLine || rawLine.startsWith(":")) continue;
			const colon = rawLine.indexOf(":");
			const field = colon === -1 ? rawLine : rawLine.slice(0, colon);
			if (field !== "data") continue;
			let value = colon === -1 ? "" : rawLine.slice(colon + 1);
			if (value.startsWith(" ")) value = value.slice(1);
			data.push(value);
		}

		if (data.length === 0) return;
		const payload = data.join("\n");
		if (!payload) return;

		try {
			const event = JSON.parse(payload) as StreamEvent;
			this.handleEvent(event, callbacks);
		} catch {
			// A malformed event is isolated to its own SSE record. The next
			// record remains parseable and can still complete the stream.
		}
	}

	private handleEvent(event: StreamEvent, callbacks: StreamCallbacks): void {
		switch (event.type) {
			case "text_delta":
				callbacks.onTextDelta(event.delta);
				break;
			case "text_delta_with_citations":
				callbacks.onTextDelta(event.delta, normalizeCitations(event.citations));
				break;
			case "thinking_delta":
				callbacks.onThinkingDelta(event.thinking);
				break;
			case "tool_call_start":
				callbacks.onToolCallStart(event.id, event.name);
				break;
			case "tool_call_preparing":
				callbacks.onToolCallPreparing?.(
					event.id,
					event.name,
					event.received_bytes,
				);
				break;
			case "tool_call_complete":
				callbacks.onToolCallComplete(event.id, event.name, event.arguments);
				break;
			case "tool_result":
				callbacks.onToolResult(event.id, event.output, event.is_error);
				break;
			case "tool_output_delta":
				callbacks.onToolOutputDelta(event.id, event.delta);
				break;
			case "delegated_progress":
				callbacks.onDelegatedProgress?.(event as DelegatedProgressEvent);
				break;
			case "delegation_event":
				for (const listener of this.delegationEventListeners) {
					try {
						listener(event.event);
					} catch {
						// A presentation subscriber must never interrupt the chat stream.
					}
				}
				callbacks.onDelegationEvent?.(event.event);
				break;
			case "tool_approval_required":
				callbacks.onToolApprovalRequired?.(
					event.id,
					event.name,
					event.arguments,
				);
				break;
			case "tool_approved":
				callbacks.onToolApproved?.(event.id);
				break;
			case "tool_denied":
				callbacks.onToolDenied?.(event.id);
				break;
			case "steering_injected":
				callbacks.onSteeringInjected?.(event.pending_id, event.message);
				break;
			case "worker_input_staged":
				callbacks.onWorkerInputStaged?.(event);
				break;
			case "worker_response_pending":
				callbacks.onWorkerResponsePending?.(event);
				break;
			case "worker_response_committed":
				callbacks.onWorkerResponseCommitted?.(event);
				break;
			case "turn_complete":
				callbacks.onTurnComplete?.(event.turn, event.has_more);
				break;
			case "plan_update":
				callbacks.onPlanUpdate(event.items);
				break;
			case "workflow_updated":
				callbacks.onWorkflowUpdated?.(
					event.goal_id,
					event.aggregate_revision,
					event.operation_id,
				);
				break;
			case "mode_change":
				callbacks.onModeChange(event.mode, event.reason);
				break;
			case "plan_complete":
				callbacks.onPlanComplete(
					event.tool_call_id,
					event.title,
					event.task_count,
				);
				break;
			case "usage":
				callbacks.onUsage(
					event.prompt_tokens,
					event.completion_tokens,
					normalizeUsageMetrics(event),
				);
				break;
			case "lagged":
				callbacks.onLagged?.(event.skipped);
				break;
			case "context_compaction_started":
				callbacks.onContextCompactionStarted?.(event);
				break;
			case "session_pinched":
			case "context_compacted":
				callbacks.onSessionPinched?.(event);
				break;
			case "title_update":
				callbacks.onTitleUpdate(event.title);
				break;
			case "finish":
				callbacks.onFinish(event.session_id, event.stop_reason);
				break;
			case "error":
				callbacks.onError(event.error);
				break;
			// Hive autonomous agent events
			case "user_message":
				callbacks.onUserMessage?.(event.title, event.message, event.level);
				break;
			case "agent_sleeping":
				callbacks.onAgentSleeping?.(event.duration_secs, event.reason);
				break;
			case "tick_injected":
				callbacks.onTickInjected?.(event.tick_number);
				break;
			case "classifier_decision":
				callbacks.onClassifierDecision?.(
					event.tool_name,
					event.decision,
					event.reason,
					event.stage,
				);
				break;
			case "teammate_spawned":
				callbacks.onTeammateSpawned?.(event.name, event.role);
				break;
			case "teammate_task_completed":
				callbacks.onTeammateTaskCompleted?.(
					event.name,
					event.task_id,
					event.result,
				);
				break;
			case "teammate_task_failed":
				callbacks.onTeammateTaskFailed?.(
					event.name,
					event.task_id,
					event.error,
				);
				break;
			case "teammate_cancelled":
				callbacks.onTeammateCancelled?.(event.name);
				break;
		}
	}
}
