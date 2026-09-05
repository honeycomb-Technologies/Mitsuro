import { mergeCitations, normalizeCitations, type Citation, type ContentBlock } from '@mitsuro/api';

import {
  MAX_MESSAGE_CONTENT_LENGTH,
  SUPPORTED_IMAGE_MIME_TYPES,
} from './constants';
import { formatToolOutputForDisplay, parseDelegatedArtifactState } from './delegated';
import { buildStoredMessageId } from './transient';
import type {
  Attachment,
  ChatMessage,
  ChatMessageAttachment,
  ChatRenderPart,
  ToolCall,
} from './types';

function extensionForImageMimeType(mimeType: string | null | undefined): string {
  switch ((mimeType || '').trim().toLowerCase()) {
    case 'image/png':
      return 'png';
    case 'image/gif':
      return 'gif';
    case 'image/webp':
      return 'webp';
    default:
      return 'jpg';
  }
}

function parseImageAttachment(
  block: Record<string, any>,
  index: number,
): ChatMessageAttachment | null {
  const source = block.source;
  if (!source || typeof source !== 'object') return null;

  if (source.type === 'base64' && typeof source.data === 'string') {
    const mimeType =
      typeof source.media_type === 'string' ? source.media_type : 'image/png';
    return {
      type: 'image',
      name: `image-${index + 1}.${extensionForImageMimeType(mimeType)}`,
      mimeType,
      base64: source.data,
    };
  }

  if (source.type === 'url' && typeof source.url === 'string') {
    return {
      type: 'image',
      name: `image-${index + 1}`,
      uri: source.url,
    };
  }

  return null;
}

function extractTextContent(content: unknown): string {
  if (typeof content === 'string') return content;

  if (Array.isArray(content)) {
    let text = '';
    for (const block of content) {
      if (!block || typeof block !== 'object') continue;
      if (block.type !== 'text' || typeof block.text !== 'string') continue;
      text += text ? `\n${block.text}` : block.text;
    }
    return text;
  }

  if (content && typeof content === 'object' && 'text' in content) {
    const textValue = (content as Record<string, unknown>).text;
    if (typeof textValue === 'string') return textValue;
  }

  return '';
}

function appendRenderPart(
  message: ChatMessage,
  part: ChatRenderPart,
): void {
  const parts = message.renderParts ?? (message.renderParts = []);
  parts.push(part);
}

/**
 * Join adjacent text into one render part.
 *
 * Must match the live stream path (`streaming.ts`): raw concatenation.
 * Stored history used to insert `\n` between consecutive text blocks, which
 * made reloaded messages diverge from the live timeline for the same content.
 */
function joinAdjacentText(existing: string, next: string): string {
  if (!existing) return next;
  if (!next) return existing;
  return existing + next;
}

function appendTextRenderPart(message: ChatMessage, content: string, citations?: Citation[]): void {
  if (!content && !citations?.length) return;

  const parts = message.renderParts ?? (message.renderParts = []);
  const lastPart = parts[parts.length - 1];
  if (lastPart?.type === 'text') {
    parts[parts.length - 1] = {
      ...lastPart,
      content: joinAdjacentText(lastPart.content, content),
      ...(citations?.length ? { citations: mergeCitations(lastPart.citations, citations) } : {}),
    };
  } else {
    parts.push({
      type: 'text',
      id: `text-${parts.length}`,
      content,
      ...(citations?.length ? { citations } : {}),
    });
  }
}

function appendThinkingRenderPart(message: ChatMessage, content: string): void {
  if (!content) return;

  const parts = message.renderParts ?? (message.renderParts = []);
  const lastPart = parts[parts.length - 1];
  if (lastPart?.type === 'thinking') {
    parts[parts.length - 1] = {
      ...lastPart,
      content: lastPart.content ? `${lastPart.content}\n\n${content}` : content,
    };
  } else {
    parts.push({
      type: 'thinking',
      id: `thinking-${parts.length}`,
      content,
    });
  }
}

function appendAttachmentsRenderPart(message: ChatMessage): void {
  const parts = message.renderParts || [];
  if (parts[parts.length - 1]?.type === 'attachments') return;
  if (parts.some((part) => part.type === 'attachments')) return;

  appendRenderPart(message, {
    type: 'attachments',
    id: `attachments-${parts.length}`,
  });
}

interface StoredMessageInput {
  role: string;
  content: unknown;
}

interface StoredToolResult {
  output: string;
  isError: boolean;
}

function createParsedStoredMessage(message: StoredMessageInput): ChatMessage {
  const role: 'user' | 'assistant' =
    message.role === 'user' || message.role === 'assistant'
      ? message.role
      : 'assistant';
  const parsed: ChatMessage = {
    id: '',
    role,
    content: '',
    thinking: '',
    toolCalls: [],
    renderParts: [],
  };
  return parsed;
}

function applyStoredContentBlock(
  parsed: ChatMessage,
  block: Record<string, any>,
  imageIndex: number,
  toolResults?: Map<string, StoredToolResult>,
): number {
  if (block.type === 'text' || ('text' in block && !block.type)) {
    const text = typeof block.text === 'string' ? block.text : '';
    if (parsed.content.length < MAX_MESSAGE_CONTENT_LENGTH) {
      parsed.content += text.slice(
        0,
        MAX_MESSAGE_CONTENT_LENGTH - parsed.content.length,
      );
    }
    appendTextRenderPart(parsed, text, normalizeCitations(block.citations));
  } else if (block.type === 'thinking' || 'thinking' in block) {
    const thinkingContent = block.thinking || '';
    parsed.thinking = parsed.thinking
      ? `${parsed.thinking}\n\n${thinkingContent}`
      : thinkingContent;
    appendThinkingRenderPart(parsed, thinkingContent);
  } else if (block.type === 'image') {
    const attachment = parseImageAttachment(block, imageIndex);
    if (attachment) {
      const attachments = parsed.attachments ?? (parsed.attachments = []);
      attachments.push(attachment);
      appendAttachmentsRenderPart(parsed);
      imageIndex += 1;
    }
  } else if (
    block.type === 'tool_use'
    || ('id' in block && 'name' in block && 'input' in block)
  ) {
    parsed.toolCalls = parsed.toolCalls || [];
    const toolResult = toolResults?.get(block.id);
    const delegated = parseDelegatedArtifactState(
      block.name,
      toolResult?.output,
      block.input,
    );
    const status: ToolCall['status'] = toolResult
      ? delegated?.outcome === 'partial'
        ? 'partial'
        : delegated?.outcome === 'failed' || delegated?.outcome === 'cancelled'
          ? 'error'
          : toolResult.isError
            ? 'error'
            : 'success'
      : 'pending';
    parsed.toolCalls.push({
      id: block.id,
      name: block.name,
      arguments: block.input,
      output: formatToolOutputForDisplay(
        block.name,
        toolResult?.output,
        block.input,
      ),
      delegatedRunId: delegated?.delegatedRunId,
      delegated,
      status,
    });
    appendRenderPart(parsed, {
      type: 'tool',
      id: `tool-${block.id}`,
      toolCallId: block.id,
    });
  }

  return imageIndex;
}

function finalizeParsedStoredMessage(
  parsed: ChatMessage,
  message: StoredMessageInput,
): void {
  if (
    !parsed.content
    && !parsed.thinking
    && (!parsed.toolCalls || parsed.toolCalls.length === 0)
  ) {
    parsed.content = extractTextContent(message.content);
    appendTextRenderPart(parsed, parsed.content);
  }
}

function parseStoredMessage(
  message: StoredMessageInput,
  toolResults?: Map<string, StoredToolResult>,
): ChatMessage {
  const parsed = createParsedStoredMessage(message);
  const contentArray = Array.isArray(message.content) ? message.content : [];
  let imageIndex = 0;

  for (const block of contentArray) {
    if (!block || typeof block !== 'object') continue;
    imageIndex = applyStoredContentBlock(
      parsed,
      block,
      imageIndex,
      toolResults,
    );
  }

  finalizeParsedStoredMessage(parsed, message);
  return parsed;
}

function collectStoredToolResult(
  block: Record<string, any>,
  toolResults: Map<string, StoredToolResult>,
): void {
  if (block.type !== 'tool_result' && !('tool_use_id' in block)) return;
  if (!block.tool_use_id) return;
  const output =
    typeof block.output === 'string'
      ? block.output
      : typeof block.content === 'string'
        ? block.content
        : JSON.stringify(block.output || block.content || '');
  toolResults.set(block.tool_use_id, {
    output,
    isError: block.is_error === true,
  });
}

function buildPreviousMessageIndex(
  previousMessages: ChatMessage[],
): Map<string, ChatMessage> {
  const previousByFingerprint = new Map<string, ChatMessage>();
  for (const previous of previousMessages) {
    if (previous.kind === 'streaming' || previous.kind === 'live_partial') {
      continue;
    }
    previousByFingerprint.set(messageStabilityFingerprint(previous), previous);
  }
  return previousByFingerprint;
}

function appendParsedStoredMessage(
  result: ChatMessage[],
  parsed: ChatMessage,
  previousByFingerprint: Map<string, ChatMessage>,
): void {
  const hasContent = parsed.content.trim().length > 0;
  const hasThinking = (parsed.thinking?.trim().length ?? 0) > 0;
  const hasToolCalls = (parsed.toolCalls?.length ?? 0) > 0;
  const hasAttachments = (parsed.attachments?.length ?? 0) > 0;
  const hasCitations = parsed.renderParts?.some((part) => part.type === 'text' && part.citations?.length);
  if (!hasContent && !hasThinking && !hasToolCalls && !hasAttachments && !hasCitations) return;

  if (previousByFingerprint.size > 0) {
    const fingerprint = messageStabilityFingerprint(parsed);
    const previous = previousByFingerprint.get(fingerprint);
    parsed.id = previous?.id || buildStoredMessageId(result.length, parsed);
    // Prefer one-to-one reuse so later duplicates still get unique IDs.
    if (previous) {
      previousByFingerprint.delete(fingerprint);
    }
  } else {
    // Cold hydration has no stable IDs to recover, so avoid building and
    // hashing large concatenated fingerprints that cannot produce a match.
    parsed.id = buildStoredMessageId(result.length, parsed);
  }
  result.push(parsed);
}

export function processStoredMessages(
  rawMessages: StoredMessageInput[],
  previousMessages: ChatMessage[] = [],
): ChatMessage[] {
  const result: ChatMessage[] = [];
  const toolResults = new Map<string, StoredToolResult>();
  const previousByFingerprint = buildPreviousMessageIndex(previousMessages);

  for (const message of rawMessages) {
    const contentArray = Array.isArray(message.content) ? message.content : [];
    for (const block of contentArray) {
      if (!block || typeof block !== 'object') continue;
      collectStoredToolResult(block, toolResults);
    }
  }

  for (const message of rawMessages) {
    const parsed = parseStoredMessage(message, toolResults);
    appendParsedStoredMessage(result, parsed, previousByFingerprint);
  }

  return result;
}

export interface CooperativeStoredMessageOptions {
  /** Maximum synchronous work per slice before yielding to native input. */
  timeSliceMs?: number;
  now?: () => number;
  yieldToHost?: () => Promise<void>;
  /** False invalidates partial work; callers must never publish it. */
  shouldContinue?: () => boolean;
}

export interface CooperativeStoredMessageResult {
  messages: ChatMessage[];
  cancelled: boolean;
  yieldCount: number;
  maxSliceDurationMs: number;
}

const DEFAULT_STORED_MESSAGE_SLICE_MS = 4;

function defaultStoredMessageClock(): number {
  return globalThis.performance?.now?.() ?? Date.now();
}

function defaultStoredMessageHostYield(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/**
 * Parse a stored transcript cooperatively while preserving one atomic result.
 *
 * The two passes and every content block keep the synchronous parser's exact
 * ordering and tool-result semantics. Partial arrays are discarded when a
 * newer session selection invalidates the caller.
 */
export async function processStoredMessagesCooperatively(
  rawMessages: StoredMessageInput[],
  previousMessages: ChatMessage[] = [],
  options: CooperativeStoredMessageOptions = {},
): Promise<CooperativeStoredMessageResult> {
  const timeSliceMs = Math.max(
    1,
    options.timeSliceMs ?? DEFAULT_STORED_MESSAGE_SLICE_MS,
  );
  const now = options.now ?? defaultStoredMessageClock;
  const yieldToHost = options.yieldToHost ?? defaultStoredMessageHostYield;
  const shouldContinue = options.shouldContinue ?? (() => true);
  const result: ChatMessage[] = [];
  const toolResults = new Map<string, StoredToolResult>();
  const previousByFingerprint = new Map<string, ChatMessage>();
  let sliceStartedAt = now();
  let yieldCount = 0;
  let maxSliceDurationMs = 0;

  const checkpoint = (): true | Promise<boolean> => {
    const sliceDurationMs = Math.max(0, now() - sliceStartedAt);
    maxSliceDurationMs = Math.max(maxSliceDurationMs, sliceDurationMs);
    if (sliceDurationMs < timeSliceMs) return true;
    return yieldToHost().then(() => {
      yieldCount += 1;
      if (!shouldContinue()) return false;
      sliceStartedAt = now();
      return true;
    });
  };
  const cancelledResult = (): CooperativeStoredMessageResult => ({
    messages: [],
    cancelled: true,
    yieldCount,
    maxSliceDurationMs,
  });

  if (!shouldContinue()) return cancelledResult();

  for (const previous of previousMessages) {
    if (previous.kind !== 'streaming' && previous.kind !== 'live_partial') {
      previousByFingerprint.set(
        messageStabilityFingerprint(previous),
        previous,
      );
    }
    const next = checkpoint();
    if (next !== true && !await next) return cancelledResult();
  }

  for (const message of rawMessages) {
    const contentArray = Array.isArray(message.content) ? message.content : [];
    for (const block of contentArray) {
      if (block && typeof block === 'object') {
        collectStoredToolResult(block, toolResults);
      }
      const next = checkpoint();
      if (next !== true && !await next) return cancelledResult();
    }
    const next = checkpoint();
    if (next !== true && !await next) return cancelledResult();
  }

  for (const message of rawMessages) {
    const parsed = createParsedStoredMessage(message);
    const contentArray = Array.isArray(message.content) ? message.content : [];
    let imageIndex = 0;
    for (const block of contentArray) {
      if (block && typeof block === 'object') {
        imageIndex = applyStoredContentBlock(
          parsed,
          block,
          imageIndex,
          toolResults,
        );
      }
      const next = checkpoint();
      if (next !== true && !await next) return cancelledResult();
    }
    finalizeParsedStoredMessage(parsed, message);
    appendParsedStoredMessage(result, parsed, previousByFingerprint);
    const next = checkpoint();
    if (next !== true && !await next) return cancelledResult();
  }

  maxSliceDurationMs = Math.max(
    maxSliceDurationMs,
    Math.max(0, now() - sliceStartedAt),
  );
  if (!shouldContinue()) return cancelledResult();
  return {
    messages: result,
    cancelled: false,
    yieldCount,
    maxSliceDurationMs,
  };
}

function messageStabilityFingerprint(message: ChatMessage): string {
  const toolIds = (message.toolCalls || []).map((toolCall) => toolCall.id).join(',');
  return [
    message.role,
    message.content,
    message.thinking || '',
    toolIds,
    (message.attachments || [])
      .map((attachment) =>
        [
          attachment.type,
          attachment.name || '',
          attachment.uri || '',
          attachment.base64?.length || 0,
        ].join(':'),
      )
      .join('|'),
  ].join('::');
}

export function buildContentBlocks(
  text: string,
  attachments: Attachment[],
): ContentBlock[] {
  const blocks: ContentBlock[] = [];

  for (const attachment of attachments) {
    if (attachment.type === 'image' && attachment.base64) {
      const mediaType = normalizeSupportedImageMimeType(attachment.mimeType);
      if (!mediaType) {
        throw new Error(unsupportedImageMimeTypeMessage(attachment.mimeType));
      }
      blocks.push({
        type: 'image',
        source: {
          type: 'base64',
          media_type: mediaType,
          data: attachment.base64,
        },
      });
    }
  }

  const fileSections: string[] = [];
  const attachedFileNames: string[] = [];
  for (const attachment of attachments) {
    if (attachment.type !== 'file') continue;
    attachedFileNames.push(attachment.name);
    if (attachment.text) {
      fileSections.push(`\n\n--- ${attachment.name} ---\n${attachment.text}`);
    }
  }

  const fileContent = fileSections.join('');
  const fallbackFileLabel =
    attachedFileNames.length > 0 && fileContent.length === 0
      ? `\n\n[Attached files: ${attachedFileNames.join(', ')}]`
      : '';
  const fullText = `${text}${fileContent}${fallbackFileLabel}`.trim();
  blocks.push({
    type: 'text',
    text: fullText.length > 0 ? fullText : 'Attached content',
  });

  return blocks;
}

function normalizeSupportedImageMimeType(
  mimeType: string | null | undefined,
): string | null {
  const normalized = (mimeType || 'image/png').trim().toLowerCase();
  switch (normalized) {
    case 'image/jpeg':
    case 'image/jpg':
    case 'image/pjpeg':
      return 'image/jpeg';
    case 'image/png':
      return 'image/png';
    case 'image/gif':
      return 'image/gif';
    case 'image/webp':
      return 'image/webp';
    default:
      return null;
  }
}

export function unsupportedImageMimeTypeMessage(mimeType: string): string {
  const normalized = mimeType.trim().toLowerCase();
  const hint =
    normalized === 'image/heic' || normalized === 'image/heif'
      ? ' Convert HEIC/HEIF images to JPEG or PNG before uploading.'
      : '';
  return `Image format '${mimeType.trim()}' is not supported. Supported formats: ${SUPPORTED_IMAGE_MIME_TYPES.join(', ')}.${hint}`;
}

export function getUnsupportedImageAttachment(
  attachments: Attachment[],
): Attachment | undefined {
  return attachments.find(
    (attachment) =>
      attachment.type === 'image'
      && Boolean(attachment.base64)
      && !normalizeSupportedImageMimeType(attachment.mimeType),
  );
}
