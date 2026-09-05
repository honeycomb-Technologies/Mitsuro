import type {
	Citation,
	PlanItem,
	SessionContinuationEvent,
	StreamCallbacks,
} from "@mitsuro/api";
import { mergeCitations } from "@mitsuro/api";
import {
	MAX_LIVE_MESSAGE_CONTENT_LENGTH,
	MAX_LIVE_THINKING_CONTENT_LENGTH,
	MAX_LIVE_TOOL_OUTPUT_LENGTH,
} from "./constants";
import type { createPlanStore } from "../plan";
import type { createSessionsStore } from "../sessions";
import { beginMitsuroPerformanceSpan } from "../performance";
import {
	applyDelegatedProgress,
	createDelegatedArtifactState,
	formatToolOutputForDisplay,
	mergeDelegatedArtifactState,
	parseDelegatedArtifactState,
	resolveDelegatedKind,
} from "./delegated";
import {
	createChatMessageId,
	createStreamingAssistantMessage,
	discardTransientAssistantMessages,
	finalizeTransientAssistantMessages,
	pruneEmptyAssistantMessages,
	upsertTransientAssistantMessage,
} from "./transient";
import type {
	AssistantMessageRef,
	ChatRenderPart,
	SessionMode,
	SessionStoreState,
	ToolCall,
} from "./types";

type SessionStateSetter = (
	partial:
		| Partial<SessionStoreState>
		| ((state: SessionStoreState) => Partial<SessionStoreState>),
) => void;

interface StreamCallbackDependencies {
	planStore: ReturnType<typeof createPlanStore>;
	sessionsStore: ReturnType<typeof createSessionsStore>;
	persistSessionMode: (
		getState: () => SessionStoreState,
		mode: SessionMode,
	) => Promise<void>;
	isActive?: () => boolean;
	/** Worker prose is provisional until its exact response boundary commits. */
	isWorkerResponseExpected?: () => boolean;
	/** Terminal-independent exact-session ownership for queued successors. */
	isSessionCurrent?: (sessionId: string) => boolean;
	expectedSessionId?: string | null;
	onSessionOwnershipChange?: (sessionId: string) => void;
	onFirstEvent?: () => void;
	onDelegationEvent?: () => void;
	onWorkerResponseBoundaryChange?: (
		boundary:
			| { sessionId: string; runId: string }
			| null,
	) => void;
	deferCanonicalReload?: (sessionId: string) => void;
	consumeCanonicalReload?: (sessionId: string) => boolean;
}

function appendBounded(existing: string, delta: string, max: number): string {
	const next = existing + delta;
	if (next.length <= max) return next;
	// Keep the start of the stream. Tail-slicing deleted early prose mid-turn and
	// made long assistant replies look like earlier text was erased while tools ran.
	return next.slice(0, max);
}

function appendRenderPart(ref: AssistantMessageRef, part: ChatRenderPart) {
	ref.current.renderParts = [...(ref.current.renderParts || []), part];
}

function appendTextRenderPart(ref: AssistantMessageRef, content: string, citations?: Citation[]) {
	if (!content && !citations?.length) return;

	const parts = [...(ref.current.renderParts || [])];
	const lastPart = parts[parts.length - 1];
	if (lastPart?.type === "text") {
		parts[parts.length - 1] = {
			...lastPart,
			content: lastPart.content + content,
			...(citations?.length ? { citations: mergeCitations(lastPart.citations, citations) } : {}),
		};
	} else {
		const textCount = parts.filter((part) => part.type === "text").length;
		parts.push({
			type: "text",
			id: `text-${textCount}`,
			content,
			...(citations?.length ? { citations } : {}),
		});
	}
	ref.current.renderParts = parts;
}

function appendThinkingRenderPart(ref: AssistantMessageRef, content: string) {
	if (!content) return;

	const parts = [...(ref.current.renderParts || [])];
	const lastPart = parts[parts.length - 1];
	if (lastPart?.type === "thinking") {
		parts[parts.length - 1] = {
			...lastPart,
			content: lastPart.content + content,
		};
	} else {
		parts.push({
			type: "thinking",
			id: `thinking-${parts.length}`,
			content,
		});
	}
	ref.current.renderParts = parts;
}

function appendToolRenderPart(ref: AssistantMessageRef, toolCallId: string) {
	if (!toolCallId) return;

	const parts = ref.current.renderParts || [];
	if (
		parts.some(
			(part) => part.type === "tool" && part.toolCallId === toolCallId,
		)
	) {
		return;
	}

	appendRenderPart(ref, {
		type: "tool",
		id: `tool-${toolCallId}`,
		toolCallId,
	});
}

export function createStreamCallbacks(
	ref: AssistantMessageRef,
	set: SessionStateSetter,
	get: () => SessionStoreState,
	{
		planStore,
		sessionsStore,
		persistSessionMode,
		isActive = () => true,
		isWorkerResponseExpected = () => false,
		isSessionCurrent = () => isActive(),
		expectedSessionId = null,
		onSessionOwnershipChange,
		onFirstEvent,
		onDelegationEvent,
		onWorkerResponseBoundaryChange,
		deferCanonicalReload,
		consumeCanonicalReload,
	}: StreamCallbackDependencies,
): StreamCallbacks {
	let pinchedSessionId: string | null = null;
	let queuedRecoverySourceSessionId: string | null = null;
	let ownedSessionId = expectedSessionId;
	let compactedInPlace = false;
	let streamLagged = false;
	let pendingTextDelta = "";
	let pendingTextCitations: Citation[] | undefined;
	let pendingThinkingDelta = "";
	const pendingToolOutputDeltas = new Map<string, string>();
	let streamFlushScheduled = false;
	let firstEventPending = true;
	let workerResponseBoundary: {
		workerId: string;
		sessionId: string;
		runId: string;
		committed: boolean;
	} | null = null;
	const finishFirstEventSpan = beginMitsuroPerformanceSpan("stream.first_event");

	function clearPendingPresentationDeltas() {
		pendingTextDelta = "";
		pendingTextCitations = undefined;
		pendingThinkingDelta = "";
		pendingToolOutputDeltas.clear();
	}

	function hasPendingText() {
		return pendingTextDelta.length > 0 || Boolean(pendingTextCitations?.length);
	}

	function noteFirstEvent() {
		if (!firstEventPending) return;
		firstEventPending = false;
		finishFirstEventSpan();
		onFirstEvent?.();
	}

	function updateLastAssistantMessage(
		updater?: (state: SessionStoreState) => Partial<SessionStoreState>,
	) {
		set((state) => {
			const messages = upsertTransientAssistantMessage(state.messages, {
				...ref.current,
			});
			return { messages, ...updater?.(state) };
		});
	}

	function flushPendingDeltas() {
		if (!isActive()) {
			clearPendingPresentationDeltas();
			return;
		}
		const finishFlushSpan = beginMitsuroPerformanceSpan("stream.flush");
		let changed = false;
		let flushedText = false;
		let flushedThinking = false;

		if (hasPendingText()) {
			ref.current.content = appendBounded(
				ref.current.content,
				pendingTextDelta,
				MAX_LIVE_MESSAGE_CONTENT_LENGTH,
			);
			appendTextRenderPart(ref, pendingTextDelta, pendingTextCitations);
			pendingTextDelta = "";
			pendingTextCitations = undefined;
			changed = true;
			flushedText = true;
		}

		if (pendingThinkingDelta) {
			ref.current.thinking = appendBounded(
				ref.current.thinking || "",
				pendingThinkingDelta,
				MAX_LIVE_THINKING_CONTENT_LENGTH,
			);
			appendThinkingRenderPart(ref, pendingThinkingDelta);
			pendingThinkingDelta = "";
			const delegatedIndex = (ref.current.toolCalls || []).findIndex(
				(toolCall) =>
					resolveDelegatedKind(
						toolCall.name,
						toolCall.arguments,
						toolCall.delegated?.kind,
					) !== undefined,
			);
			if (delegatedIndex >= 0) {
				const toolCalls = [...(ref.current.toolCalls || [])];
				const delegatedTool = toolCalls[delegatedIndex];
				const delegatedKind = resolveDelegatedKind(
					delegatedTool.name,
					delegatedTool.arguments,
					delegatedTool.delegated?.kind,
				);
				if (delegatedKind) {
					toolCalls[delegatedIndex] = {
						...delegatedTool,
						delegated: mergeDelegatedArtifactState(delegatedTool.delegated, {
							...(delegatedTool.delegated ||
								createDelegatedArtifactState(
									delegatedKind,
									delegatedTool.arguments,
								)),
							kind: delegatedKind,
							thinking: ref.current.thinking || "",
						}),
					};
					ref.current.toolCalls = toolCalls;
				}
			}
			changed = true;
			flushedThinking = true;
		}

		if (pendingToolOutputDeltas.size > 0) {
			let toolCallsChanged = false;
			if (ref.current.toolCalls?.length) {
				ref.current.toolCalls = ref.current.toolCalls.map((toolCall) => {
					const delta = pendingToolOutputDeltas.get(toolCall.id);
					if (!delta) return toolCall;
					toolCallsChanged = true;
					return {
						...toolCall,
						output: appendBounded(toolCall.output || "", delta, MAX_LIVE_TOOL_OUTPUT_LENGTH),
					};
				});
			}
			pendingToolOutputDeltas.clear();
			changed = changed || toolCallsChanged;
		}

		if (!changed) {
			finishFlushSpan();
			return;
		}
		updateLastAssistantMessage(() => ({
			...(flushedText
				? { isLoading: false, isThinking: false }
				: {}),
			...(flushedThinking
				? {
						isThinking: true,
						thinkingContent: ref.current.thinking || "",
					}
				: {}),
		}));
		finishFlushSpan();
	}

	function scheduleStreamFlush() {
		if (streamFlushScheduled) return;
		streamFlushScheduled = true;
		type FrameScheduler = (callback: (timestamp: number) => void) => unknown;
		const runtime = globalThis as typeof globalThis & {
			requestAnimationFrame?: FrameScheduler;
		};
		const schedule: FrameScheduler = runtime.requestAnimationFrame
			? runtime.requestAnimationFrame.bind(runtime)
			: (callback) => setTimeout(() => callback(Date.now()), 16);
		schedule(() => {
			streamFlushScheduled = false;
			if (!isActive()) {
				clearPendingPresentationDeltas();
				return;
			}
			flushPendingDeltas();
		});
	}

	function mapToolCalls(id: string, mapper: (toolCall: ToolCall) => ToolCall) {
		const toolCalls = ref.current.toolCalls;
		if (!toolCalls || toolCalls.length === 0) return;

		const index = toolCalls.findIndex((toolCall) => toolCall.id === id);
		if (index < 0) return;

		const nextToolCalls = [...toolCalls];
		nextToolCalls[index] = mapper(nextToolCalls[index]);
		ref.current.toolCalls = nextToolCalls;
		updateLastAssistantMessage();
	}

	return {
		onTextDelta: (delta, citations) => {
			noteFirstEvent();
			if (pendingThinkingDelta || pendingToolOutputDeltas.size > 0) {
				flushPendingDeltas();
			}
			pendingTextDelta += delta;
			pendingTextCitations = mergeCitations(pendingTextCitations, citations);
			scheduleStreamFlush();
		},

		onThinkingDelta: (thinking) => {
			noteFirstEvent();
			if (hasPendingText() || pendingToolOutputDeltas.size > 0) {
				flushPendingDeltas();
			}
			pendingThinkingDelta += thinking;
			scheduleStreamFlush();
		},

		onToolCallStart: (id, name) => {
			noteFirstEvent();
			flushPendingDeltas();
			if ((ref.current.toolCalls || []).some((toolCall) => toolCall.id === id)) {
				appendToolRenderPart(ref, id);
				return;
			}
			const delegatedKind = resolveDelegatedKind(name);
			ref.current.toolCalls = [
				...(ref.current.toolCalls || []),
				{
					id,
					name,
					delegated: delegatedKind
						? createDelegatedArtifactState(delegatedKind)
						: undefined,
					status: "running",
				},
			];
			appendToolRenderPart(ref, id);
			updateLastAssistantMessage();
		},

		onToolCallPreparing: (id, _name, receivedBytes) => {
			flushPendingDeltas();
			const kilobytes = Math.max(1, Math.round(receivedBytes / 1024));
			mapToolCalls(id, (toolCall) => ({
				...toolCall,
				description: `Preparing input · ${kilobytes} KB`,
			}));
		},

		onToolCallComplete: (id, _name, args) => {
			flushPendingDeltas();
			mapToolCalls(id, (toolCall) => {
				const delegatedKind = resolveDelegatedKind(
					toolCall.name,
					args,
					toolCall.delegated?.kind,
				);
				return {
					...toolCall,
					description: undefined,
					arguments: args,
					delegated: delegatedKind
						? mergeDelegatedArtifactState(
								toolCall.delegated,
								createDelegatedArtifactState(delegatedKind, args),
							)
						: toolCall.delegated,
				};
			});
		},

		onToolResult: (id, output, isError) => {
			flushPendingDeltas();
			mapToolCalls(id, (toolCall) => {
				const delegatedKind = resolveDelegatedKind(
					toolCall.name,
					toolCall.arguments,
					toolCall.delegated?.kind,
				);
				const delegated = delegatedKind
					? mergeDelegatedArtifactState(
							toolCall.delegated,
							parseDelegatedArtifactState(
								toolCall.name,
								output,
								toolCall.arguments,
								delegatedKind,
							) ||
								createDelegatedArtifactState(delegatedKind, toolCall.arguments),
						)
					: toolCall.delegated;
				const status: ToolCall["status"] =
					delegated?.outcome === "partial"
						? "partial"
						: delegated?.outcome === "failed" || delegated?.outcome === "cancelled"
							? "error"
							: isError
								? "error"
								: "success";
				return {
					...toolCall,
					output: formatToolOutputForDisplay(
						toolCall.name,
						output,
						toolCall.arguments,
					),
					delegated,
					status,
				};
			});
		},

		onToolOutputDelta: (id, delta) => {
			noteFirstEvent();
			if (hasPendingText() || pendingThinkingDelta) {
				flushPendingDeltas();
			}
			pendingToolOutputDeltas.set(
				id,
				(pendingToolOutputDeltas.get(id) || "") + delta,
			);
			scheduleStreamFlush();
		},

		onDelegatedProgress: (event) => {
			flushPendingDeltas();
			mapToolCalls(event.tool_call_id, (toolCall) =>
				applyDelegatedProgress(toolCall, event),
			);
		},

		onDelegationEvent: (event) => {
			noteFirstEvent();
			// Event IDs are global across sessions, so a session's valid sequence
			// is inherently sparse. Keep the last HTTP-applied cursor until the
			// canonical snapshot maps this event into group/task UI state; advancing
			// it here would make the next metadata poll skip that remap entirely.
			// The normal active-run poll applies it promptly, while this flag also
			// guarantees a full reconciliation when the stream finishes first.
			if (event.event_id > (get().delegationEventCursor ?? 0)) {
				streamLagged = true;
				onDelegationEvent?.();
			}
		},

		onPlanUpdate: (items: PlanItem[]) => {
			flushPendingDeltas();
			planStore.getState().setItems(items);
		},

		onWorkflowUpdated: (_goalId, aggregateRevision) => {
			flushPendingDeltas();
			planStore.getState().noteWorkflowRevision(aggregateRevision);
		},

		onModeChange: (mode) => {
			flushPendingDeltas();
			const nextMode: SessionMode = mode === "plan" ? "plan" : "build";
			set({ mode: nextMode });
			planStore.getState().setVisible(nextMode === "plan");
			void persistSessionMode(get, nextMode);
		},

		onPlanComplete: (toolCallId, title, taskCount) => {
			flushPendingDeltas();
			const planConfirmCall: ToolCall = {
				id: toolCallId,
				name: "PlanConfirm",
				arguments: { title, task_count: taskCount },
				status: "pending",
			};
			ref.current.toolCalls = [
				...(ref.current.toolCalls || []),
				planConfirmCall,
			];
			appendToolRenderPart(ref, toolCallId);
			updateLastAssistantMessage();
		},

		onTurnComplete: (_turn, hasMore) => {
			flushPendingDeltas();
			if (hasMore) {
				const completed = ref.current;
				const hasRenderableContent = Boolean(
					completed.content.trim()
						|| completed.thinking?.trim()
						|| (completed.toolCalls?.length ?? 0) > 0,
				);
				if (!hasRenderableContent) return;

				set((state) => ({
					messages: finalizeTransientAssistantMessages(state.messages),
				}));
				ref.current = createStreamingAssistantMessage();
			}
		},

		onToolApprovalRequired: (id, _name, args) => {
			flushPendingDeltas();
			mapToolCalls(id, (toolCall) => ({
				...toolCall,
				arguments: args,
				status: "awaiting_approval",
			}));
		},

		onToolApproved: (id) => {
			flushPendingDeltas();
			mapToolCalls(id, (toolCall) => ({ ...toolCall, status: "running" }));
		},

		onToolDenied: (id) => {
			flushPendingDeltas();
			mapToolCalls(id, (toolCall) => ({
				...toolCall,
				status: "error",
				output: "Denied by user",
			}));
		},

		onSteeringInjected: (pendingId, message) => {
			flushPendingDeltas();
			const id = pendingId
				? `user-steering-${pendingId}`
				: createChatMessageId("user-steering");
			set((state) => {
				if (state.messages.some((candidate) => candidate.id === id)) {
					return {
						messages: state.messages.map((candidate) =>
							candidate.id === id
								? {
									...candidate,
									isQueued: false,
									queuedUntilNextRun: false,
								}
								: candidate,
						),
					};
				}
				return {
					messages: [
						...pruneEmptyAssistantMessages(
							finalizeTransientAssistantMessages(state.messages),
						),
						{ id, role: "user" as const, content: message },
					],
				};
			});
		},

		onWorkerInputStaged: (event) => {
			noteFirstEvent();
			flushPendingDeltas();
			set((state) => {
				const targetIndex = state.messages.findIndex(
					(message) => message.workerStagedInputId === event.staged_input_id,
				);
				if (targetIndex < 0) return {};
				const successorRunId = event.successor_run_id ?? undefined;
				return {
					messages: state.messages.map((message, index) =>
						index === targetIndex
							? {
								...message,
								isQueued: successorRunId === undefined,
								queuedUntilNextRun: successorRunId === undefined,
								workerStagedInputId: event.staged_input_id,
								successorRunId,
							}
							: message
					),
				};
			});
		},

		onWorkerResponsePending: (event) => {
			noteFirstEvent();
			if (ownedSessionId !== null && event.session_id !== ownedSessionId) {
				streamLagged = true;
				return;
			}
			workerResponseBoundary = {
				workerId: event.worker_id,
				sessionId: event.session_id,
				runId: event.run_id,
				committed: false,
			};
			onWorkerResponseBoundaryChange?.({
				sessionId: event.session_id,
				runId: event.run_id,
			});
		},

		onWorkerResponseCommitted: (event) => {
			const boundary = workerResponseBoundary;
			if (
				boundary &&
				boundary.workerId === event.worker_id &&
				boundary.sessionId === event.session_id &&
				boundary.runId === event.run_id
			) {
				boundary.committed = true;
				return;
			}
			// A commit boundary for another Worker/run cannot authenticate the
			// draft currently on screen. Keep it provisional and force the normal
			// terminal path to discard it.
			if (!boundary) {
				workerResponseBoundary = {
					workerId: event.worker_id,
					sessionId: event.session_id,
					runId: event.run_id,
					committed: false,
				};
			}
			streamLagged = true;
		},

		onUsage: (promptTokens, completionTokens, metrics) => {
			flushPendingDeltas();
			set({
				tokenCount:
					metrics?.totalTokens ?? promptTokens + completionTokens,
				tokenUsage: metrics ?? null,
			});
		},

		onLagged: () => {
			streamLagged = true;
		},

		onSessionPinched: (event: SessionContinuationEvent) => {
			flushPendingDeltas();
			if (event.type === "session_pinched") {
				if (
					ownedSessionId !== null &&
					event.source_session_id !== ownedSessionId
				) {
					streamLagged = true;
					return;
				}
				pinchedSessionId = event.new_session_id;
				queuedRecoverySourceSessionId ??= event.source_session_id;
				ownedSessionId = event.new_session_id;
				onSessionOwnershipChange?.(event.new_session_id);
				return;
			}

			if (event.type === "context_compacted") {
				compactedInPlace = true;
				set({
					tokenCount: event.estimated_tokens_after,
					tokenUsage: null,
				});
			}
		},

		onTitleUpdate: (title) => {
			flushPendingDeltas();
			set({ title });
			sessionsStore.getState().loadSessions();
		},

		onFinish: (sessionId, stopReason) => {
			if (ownedSessionId !== null && sessionId !== ownedSessionId) {
				clearPendingPresentationDeltas();
				onWorkerResponseBoundaryChange?.(null);
				set({
					isLoading: false,
					isStreaming: false,
					isThinking: false,
					thinkingContent: "",
					error: "The stream finished for a different conversation. Reload to reconcile it.",
				});
				return;
			}
			const responseBoundary = workerResponseBoundary;
			const discardWorkerDraft = isWorkerResponseExpected()
				? responseBoundary === null ||
					responseBoundary.sessionId !== sessionId ||
					!responseBoundary.committed ||
					stopReason !== "completed"
				: responseBoundary !== null &&
					(
						responseBoundary.sessionId !== sessionId ||
						!responseBoundary.committed ||
						stopReason !== "completed"
					);
			if (discardWorkerDraft) {
				clearPendingPresentationDeltas();
			} else {
				flushPendingDeltas();
			}
			workerResponseBoundary = null;
			onWorkerResponseBoundaryChange?.(null);
			const currentState = get();
			const queued = currentState.queuedMessages;
			const activeSessionId = pinchedSessionId ?? sessionId;
			const queuedSourceSessionId = queuedRecoverySourceSessionId;
			const queuedSendOptions = queued[0]?.sendOptions;
			const localQueuedMessageIds = new Set(
				queued.map((message) => message.id),
			);
			const sendQueuedMessages = (expectedSessionId: string) => {
				if (
					queued.length === 0 || !isSessionCurrent(expectedSessionId)
				) {
					return;
				}
				const combinedContent = queued
					.map((message) => message.content)
					.join("\n\n");
				const combinedAttachments = queued.flatMap(
					(message) => message.attachments,
				);
				void get().sendMessage(
					combinedContent,
					combinedAttachments,
					{
						...queuedSendOptions,
						queuedSuccessor: {
							id: createChatMessageId("queued-successor"),
							sessionId: expectedSessionId,
							sourceSessionId: queuedSourceSessionId ?? undefined,
							queuedMessages: queued,
						},
					},
				).catch(() => {
					// The store restores the exact claimed batch. Always observe the
					// promise so a provider rejection cannot become an unhandled error.
				});
			};
			const shouldLoadPinchedSession =
				pinchedSessionId !== null && pinchedSessionId !== sessionId;
			const inheritedCanonicalReload =
				consumeCanonicalReload?.(activeSessionId) ?? false;
			const shouldReloadCurrentSession =
				(inheritedCanonicalReload || compactedInPlace || streamLagged) &&
				!shouldLoadPinchedSession;

			const terminalMessages = currentState.messages.map((message) =>
				localQueuedMessageIds.has(message.id)
					? message
					:
					message.isQueued && message.id.startsWith("user-steering-")
						? { ...message, queuedUntilNextRun: true }
						: message.isQueued && !message.queuedUntilNextRun
							? { ...message, isQueued: false }
							: message,
				);
			const messages = discardWorkerDraft
				? discardTransientAssistantMessages(terminalMessages)
				: finalizeTransientAssistantMessages(terminalMessages);

			set({
				sessionId: activeSessionId,
				messages: pruneEmptyAssistantMessages(messages),
				queuedMessages: queued,
				isStreaming: false,
				isThinking: false,
				thinkingContent: "",
			});
			sessionsStore.getState().loadSessions();

			if (queued.length > 0) {
				// Start the bound follow-up in the same JavaScript turn. Waiting for
				// transcript rehydration (or even a timer) opened a navigation window
				// where A's input could be dropped or sent to B. The server owns the
				// canonical compacted/pinched context, so it is safe to start the exact
				// next turn before a presentation-only reload.
				if (shouldLoadPinchedSession || shouldReloadCurrentSession) {
					deferCanonicalReload?.(activeSessionId);
				}
				pinchedSessionId = null;
				queuedRecoverySourceSessionId = null;
				compactedInPlace = false;
				streamLagged = false;
				sendQueuedMessages(activeSessionId);
				return;
			}

			if (shouldLoadPinchedSession) {
				const nextSessionId = pinchedSessionId;
				pinchedSessionId = null;
				queuedRecoverySourceSessionId = null;
				if (nextSessionId) {
					void (async () => {
						try {
							await get().loadSession(nextSessionId, true);
						} catch {
							// loadSession already updates error state
						}

						sendQueuedMessages(nextSessionId);
					})();
				}
				return;
			}

			pinchedSessionId = null;
			queuedRecoverySourceSessionId = null;

			if (shouldReloadCurrentSession) {
				compactedInPlace = false;
				streamLagged = false;
				void (async () => {
					try {
						await get().loadSession(sessionId, true);
					} catch {
						// loadSession already updates error state
					}

					sendQueuedMessages(sessionId);
				})();
				return;
			}

			compactedInPlace = false;
			streamLagged = false;

		},

		onError: (error) => {
			const discardWorkerDraft = isWorkerResponseExpected() ||
				workerResponseBoundary !== null;
			if (discardWorkerDraft) {
				// Provider text was never authenticated by a completed exact-run
				// response boundary. Never promote it into local transcript state.
				clearPendingPresentationDeltas();
			} else {
				// Flush ordinary frame-batched stream content so the final update is
				// not dropped.
				flushPendingDeltas();
			}
			workerResponseBoundary = null;
			onWorkerResponseBoundaryChange?.(null);
			set((state) => ({
				isLoading: false,
				isStreaming: false,
				isThinking: false,
				thinkingContent: "",
				messages: discardWorkerDraft
					? discardTransientAssistantMessages(state.messages)
					: pruneEmptyAssistantMessages(
					finalizeTransientAssistantMessages(state.messages),
				),
				error,
			}));
		},
	};
}
