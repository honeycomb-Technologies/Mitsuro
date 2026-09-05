import type {
  PartialAssistantState as ApiPartialAssistantState,
  PendingInteractionSnapshot as ApiPendingInteractionSnapshot,
  RecoveryToolArguments as ApiRecoveryToolArguments,
  SessionRecoveryState as ApiRecoveryState,
} from '@mitsuro/api';
import { normalizeCitations } from '@mitsuro/api';

import {
  createDelegatedArtifactState,
  resolveDelegatedKind,
} from './delegated';
import type { ChatMessage, ToolCall } from './types';

export function toErrorMessage(err: unknown, fallback = 'Unknown error'): string {
  return err instanceof Error ? err.message : fallback;
}

function generateRuntimeId(prefix: string): string {
  const randomUuid = globalThis.crypto?.randomUUID?.();
  if (typeof randomUuid === 'string' && randomUuid.length > 0) {
    return `${prefix}-${randomUuid}`;
  }

  return [
    prefix,
    Date.now().toString(36),
    Math.random().toString(36).slice(2, 10),
  ].join('-');
}

export function createChatMessageId(prefix: string): string {
  return generateRuntimeId(prefix);
}

export function createStreamingAssistantMessage(): ChatMessage {
  return {
    id: createChatMessageId('assistant-stream'),
    role: 'assistant',
    content: '',
    thinking: '',
    toolCalls: [],
    renderParts: [],
    kind: 'streaming',
  };
}

export function buildStoredMessageId(index: number, message: ChatMessage): string {
  const firstToolId = message.toolCalls?.[0]?.id;
  if (firstToolId) {
    return `stored-${index}-${message.role}-${firstToolId}`;
  }

  return [
    'stored',
    index,
    message.role,
    message.content.length,
    message.thinking?.length ?? 0,
  ].join('-');
}

function isTransientAssistantKind(
  kind: ChatMessage['kind'] | undefined,
): kind is 'live_partial' | 'streaming' {
  return kind === 'live_partial' || kind === 'streaming';
}

function isTransientAssistantMessage(
  message: ChatMessage | undefined,
): boolean {
  return message?.role === 'assistant' && isTransientAssistantKind(message.kind);
}

export function upsertTransientAssistantMessage(
  messages: ChatMessage[],
  message: ChatMessage,
): ChatMessage[] {
  const lastIndex = messages.length - 1;
  const lastMessage = messages[lastIndex];

  if (lastIndex >= 0 && isTransientAssistantMessage(lastMessage)) {
    const nextMessages = messages.slice();
    nextMessages[lastIndex] = { ...message, id: lastMessage.id };
    return nextMessages;
  }

  // Recovered snapshots can contain an older live_partial away from the tail.
  // Keep that uncommon cleanup path, but do not filter the entire transcript
  // for every ordinary stream presentation flush.
  const stalePartialIndex = messages.findIndex(
    (entry) => entry.kind === 'live_partial',
  );
  if (stalePartialIndex >= 0) {
    return [
      ...messages.slice(0, stalePartialIndex),
      ...messages.slice(stalePartialIndex + 1),
      message,
    ];
  }

  return [...messages, message];
}

export function finalizeTransientAssistantMessages(
  messages: ChatMessage[],
): ChatMessage[] {
  return messages.map((message) =>
    isTransientAssistantMessage(message)
      ? { ...message, kind: undefined }
      : message,
  );
}

export function discardTransientAssistantMessages(
  messages: ChatMessage[],
): ChatMessage[] {
  return messages.filter(
    (message) => !(message.role === 'assistant' && isTransientAssistantKind(message.kind)),
  );
}

export function pruneEmptyAssistantMessages(messages: ChatMessage[]): ChatMessage[] {
  return messages.filter((message) => {
    if (message.role !== 'assistant') {
      return true;
    }

    return (
      message.content.trim().length > 0
      || (message.thinking?.trim().length ?? 0) > 0
      || (message.toolCalls?.length ?? 0) > 0
      || Boolean(message.renderParts?.some((part) => part.type === 'text' && part.citations?.length))
    );
  });
}

function buildRecoveryNotice(recovery: ApiRecoveryState): string {
  const headline =
    recovery.stop_reason === 'stream_idle_timeout'
      ? 'Previous turn stopped after the provider stream went idle.'
      : recovery.stop_reason === 'provider_error'
        ? 'Previous turn stopped after a provider error.'
        : recovery.stop_reason === 'user_abort'
          ? 'Previous turn was interrupted by user cancellation.'
          : recovery.status === 'tool_executing'
            ? 'Previous turn ended while tool execution was in progress.'
            : recovery.status === 'streaming'
              ? 'Previous turn ended while the assistant was still streaming.'
              : 'Previous turn ended before Mitsuro could safely finalize it.';

  const details: string[] = [];
  if (recovery.partial_assistant.text.trim()) {
    details.push(`Partial output: ${recovery.partial_assistant.text.trim()}`);
  }
  if (recovery.last_error?.trim()) {
    details.push(`Last error: ${recovery.last_error.trim()}`);
  }

  return details.length > 0 ? `${headline}\n\n${details.join('\n')}` : headline;
}

export function applyRecoveryParity(
  messages: ChatMessage[],
  recovery: ApiRecoveryState | null | undefined,
  agentState: string,
): ChatMessage[] {
  const recoveryPartialPrefix = 'recovery-partial-';
  let nextMessages = messages.filter(
    (message) =>
      message.kind !== 'recovery_notice'
      && !message.id.startsWith(recoveryPartialPrefix),
  );
  const isTerminal = ['idle', 'error', 'failed'].includes(agentState);

  if (recovery && isTerminal) {
    nextMessages = nextMessages.map((message) => ({
      ...message,
      toolCalls: message.toolCalls?.map((toolCall) => {
        if (
          (toolCall.status === 'pending' || toolCall.status === 'running')
          && !toolCall.output
        ) {
          return {
            ...toolCall,
            status: 'error' as const,
            output: '[Session interrupted - tool execution was cancelled]',
          };
        }
        return toolCall;
      }),
    }));

    const partial = recovery.partial_assistant;
    const citations = normalizeCitations(partial.citations);
    const partialThinking = partial.thinking ?? '';
    const hasPartialText = partial.text.trim().length > 0;
    const hasPartialThinking = partialThinking.trim().length > 0;
    const hasPartialTools = partial.tool_calls.length > 0;
    const canonicalAlreadyContainsPartial = nextMessages.some(
      (message) =>
        message.role === 'assistant'
        && hasPartialText
        && message.content.trim() === partial.text.trim(),
    );

    if (
      (hasPartialText || hasPartialThinking || hasPartialTools || citations.length > 0)
      && !canonicalAlreadyContainsPartial
    ) {
      const toolCalls = partial.tool_calls.map((toolCall) => ({
        id: toolCall.id,
        name: toolCall.name,
        arguments: unwrapRecoveryToolArguments(toolCall.arguments),
        output: '[Turn interrupted before this tool could complete]',
        status: 'error' as const,
      }));
      const renderParts = [
        ...(hasPartialThinking
          ? [{ type: 'thinking' as const, id: 'recovery-thinking', content: partialThinking }]
          : []),
        ...toolCalls.map((toolCall) => ({
          type: 'tool' as const,
          id: `recovery-tool-${toolCall.id}`,
          toolCallId: toolCall.id,
        })),
        ...(hasPartialText || citations.length > 0
          ? [{ type: 'text' as const, id: 'recovery-text', content: partial.text, ...(citations.length ? { citations } : {}) }]
          : []),
      ];

      nextMessages.push({
        id: [
          recoveryPartialPrefix,
          recovery.schema_version,
          recovery.status,
          recovery.stop_reason ?? 'unknown',
        ].join('-'),
        role: 'assistant',
        content: partial.text,
        thinking: partialThinking || undefined,
        toolCalls,
        renderParts,
      });
    } else if (agentState === 'idle' && !canonicalAlreadyContainsPartial) {
      nextMessages.unshift({
        id: [
          'recovery-notice',
          recovery.schema_version,
          recovery.status,
          recovery.stop_reason ?? 'unknown',
        ].join('-'),
        role: 'assistant',
        content: `[Recovery Notice] ${buildRecoveryNotice(recovery)}`,
        kind: 'recovery_notice',
      });
    }
  }

  return nextMessages;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function unwrapRecoveryToolArguments(
  args: ApiRecoveryToolArguments | null | undefined,
): Record<string, unknown> | undefined {
  if (!args || args.value === undefined || args.value === null) {
    return undefined;
  }
  return isRecord(args.value) ? args.value : { value: args.value };
}

function buildPendingInteractionToolCall(
  interaction: ApiPendingInteractionSnapshot,
): ToolCall {
  switch (interaction.kind) {
    case 'tool_approval': {
      const delegatedKind = resolveDelegatedKind(
        interaction.tool_call.name,
        unwrapRecoveryToolArguments(interaction.tool_call.arguments),
      );
      return {
        id: interaction.tool_call.id,
        name: interaction.tool_call.name,
        arguments: unwrapRecoveryToolArguments(interaction.tool_call.arguments),
        delegated: delegatedKind
          ? createDelegatedArtifactState(delegatedKind)
          : undefined,
        status: 'awaiting_approval',
      };
    }
    case 'ask_user_question':
      return {
        id: interaction.tool_call_id,
        name: 'AskUserQuestion',
        arguments: {
          questions: interaction.questions.map((question) => ({
            header: question.header,
            question: question.question,
            options: question.options ?? [],
            multiSelect: question.multi_select ?? false,
          })),
        },
        status: 'awaiting_approval',
      };
    case 'plan_confirm':
      return {
        id: interaction.tool_call_id,
        name: 'PlanConfirm',
        arguments: {
          title: interaction.title,
          task_count: interaction.task_count,
          tasks: interaction.tasks,
        },
        status: 'awaiting_approval',
      };
  }
}

function applyPendingInteractionsToToolCalls(
  toolCalls: ToolCall[],
  pendingInteractions: ApiPendingInteractionSnapshot[] | null | undefined,
): ToolCall[] {
  if (!pendingInteractions?.length) {
    return toolCalls;
  }

  const pendingToolCalls = pendingInteractions.map(buildPendingInteractionToolCall);
  const pendingById = new Map(
    pendingToolCalls.map((toolCall) => [toolCall.id, toolCall]),
  );
  const nextToolCalls = toolCalls.map((toolCall) => {
    const pendingToolCall = pendingById.get(toolCall.id);
    if (!pendingToolCall) return toolCall;
    pendingById.delete(toolCall.id);
    return {
      ...toolCall,
      ...pendingToolCall,
      delegated: pendingToolCall.delegated ?? toolCall.delegated,
    };
  });

  return [...nextToolCalls, ...pendingById.values()];
}

function livePartialToolStatus(agentState: string): ToolCall['status'] {
  switch (agentState) {
    case 'tool_executing':
      return 'running';
    case 'awaiting_input':
      return 'awaiting_approval';
    default:
      return 'pending';
  }
}

export function applyLivePartialAssistant(
  messages: ChatMessage[],
  livePartial: ApiPartialAssistantState | null | undefined,
  agentState: string,
  pendingInteractions?: ApiPendingInteractionSnapshot[] | null,
): ChatMessage[] {
  const nextMessages = messages.filter((message) => message.kind !== 'live_partial');
  if (!['streaming', 'tool_executing', 'awaiting_input'].includes(agentState)) {
    return nextMessages;
  }

  // Prefer the SSE-built chronological timeline only while the agent is still
  // actively streaming/tool-executing. Once the server is idle/awaiting input,
  // live_partial + pending interactions are the source of truth for recovery.
  const lastMessage = nextMessages[nextMessages.length - 1];
  if (
    (agentState === 'streaming' || agentState === 'tool_executing')
    && lastMessage?.kind === 'streaming'
    && ((lastMessage.renderParts?.length ?? 0) > 0 || (lastMessage.toolCalls?.length ?? 0) > 0)
  ) {
    return nextMessages;
  }

  const hasContent = (livePartial?.text.trim().length ?? 0) > 0;
  const citations = normalizeCitations(livePartial?.citations);
  const hasThinking = (livePartial?.thinking?.trim().length ?? 0) > 0;
  const liveToolCalls = livePartial?.tool_calls ?? [];
  const toolCalls = applyPendingInteractionsToToolCalls(
    liveToolCalls.map((toolCall) => {
      const argumentsValue = unwrapRecoveryToolArguments(toolCall.arguments);
      const delegatedKind = resolveDelegatedKind(toolCall.name, argumentsValue);
      return {
        id: toolCall.id,
        name: toolCall.name,
        arguments: argumentsValue,
        delegated: delegatedKind
          ? createDelegatedArtifactState(delegatedKind)
          : undefined,
        status: livePartialToolStatus(agentState),
      } satisfies ToolCall;
    }),
    pendingInteractions,
  );

  if (!hasContent && !hasThinking && toolCalls.length === 0 && citations.length === 0) {
    return nextMessages;
  }

  // Best-effort chronology when only a snapshot is available:
  // thinking -> text (if any) -> tools when executing, else thinking -> tools -> text.
  const thinking = livePartial?.thinking?.trim() ? livePartial.thinking : undefined;
  const text = livePartial?.text ?? '';
  const renderParts = [] as NonNullable<ChatMessage['renderParts']>;
  if (thinking) {
    renderParts.push({ type: 'thinking', id: 'live-partial-thinking', content: thinking });
  }
  const toolsFirst = agentState === 'tool_executing' || agentState === 'awaiting_input';
  const toolParts = toolCalls.map((toolCall) => ({
    type: 'tool' as const,
    id: `live-partial-tool-${toolCall.id}`,
    toolCallId: toolCall.id,
  }));
  const textParts = text || citations.length > 0
    ? [{ type: 'text' as const, id: 'live-partial-text', content: text, ...(citations.length ? { citations } : {}) }]
    : [];
  if (toolsFirst) {
    renderParts.push(...toolParts);
    renderParts.push(...textParts);
  } else {
    renderParts.push(...textParts);
    renderParts.push(...toolParts);
  }

  return upsertTransientAssistantMessage(nextMessages, {
    id: createChatMessageId('live-partial'),
    role: 'assistant',
    content: text,
    thinking,
    toolCalls,
    renderParts,
    kind: 'live_partial',
  });
}
