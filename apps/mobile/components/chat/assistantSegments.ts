export interface AssistantRenderSegment {
  id: string;
  content: string;
}

interface SegmentCacheEntry {
  fullText: string;
  prefixText: string;
  prefixSegments: AssistantRenderSegment[];
  suffixSegments: AssistantRenderSegment[];
}

const TARGET_TAIL_CHARACTERS = 1200;
const MAX_TAIL_CHARACTERS = 2400;
const MINIMUM_REUSABLE_PREFIX_CHARACTERS = 280;
const MAX_CACHE_ENTRIES = 128;
const TRIM_CACHE_TO = 96;

const segmentCache = new Map<string, SegmentCacheEntry>();
const accessOrder: string[] = [];

export function assistantRenderSegments(
  messageId: string,
  text: string,
): AssistantRenderSegment[] {
  const cached = segmentCache.get(messageId);
  if (cached?.fullText === text) {
    touch(messageId);
    return cached.prefixSegments.concat(cached.suffixSegments);
  }

  const nextEntry = makeCacheEntry(messageId, text, cached);
  segmentCache.set(messageId, nextEntry);
  touch(messageId);
  trimCacheIfNeeded();
  return nextEntry.prefixSegments.concat(nextEntry.suffixSegments);
}

export function shouldSegmentAssistantContent(isStreaming: boolean): boolean {
  return isStreaming;
}

function makeCacheEntry(
  messageId: string,
  text: string,
  existing?: SegmentCacheEntry,
): SegmentCacheEntry {
  if (
    existing &&
    existing.prefixText.length > 0 &&
    text.startsWith(existing.fullText) &&
    text.startsWith(existing.prefixText)
  ) {
    const suffixText = text.slice(existing.prefixText.length);
    if (suffixText.length <= MAX_TAIL_CHARACTERS) {
      return {
        fullText: text,
        prefixText: existing.prefixText,
        prefixSegments: existing.prefixSegments,
        suffixSegments: splitAssistantMarkdown(
          messageId,
          suffixText,
          `tail-${existing.prefixText.length}`,
        ),
      };
    }
  }

  return rebuildCacheEntry(messageId, text);
}

function rebuildCacheEntry(messageId: string, text: string): SegmentCacheEntry {
  const anchor = stableAnchorOffset(text);
  const prefixText = text.slice(0, anchor);
  const suffixText = text.slice(anchor);

  return {
    fullText: text,
    prefixText,
    prefixSegments: prefixText
      ? splitAssistantMarkdown(messageId, prefixText, `prefix-${anchor}`)
      : [],
    suffixSegments: splitAssistantMarkdown(
      messageId,
      suffixText,
      `tail-${anchor}`,
    ),
  };
}

function stableAnchorOffset(text: string): number {
  if (
    text.length <=
    TARGET_TAIL_CHARACTERS + MINIMUM_REUSABLE_PREFIX_CHARACTERS
  ) {
    return 0;
  }

  const maxPrefixLength = Math.max(0, text.length - TARGET_TAIL_CHARACTERS);
  if (maxPrefixLength < MINIMUM_REUSABLE_PREFIX_CHARACTERS) {
    return 0;
  }

  const lines = text.split("\n");
  let consumed = 0;
  let insideFence = false;
  let lastBlankLineBoundary = 0;
  let lastLineBoundary = 0;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    const trimmed = line.trim();

    if (trimmed.startsWith("```") || trimmed.startsWith("~~~")) {
      insideFence = !insideFence;
    }

    consumed += line.length;
    if (index < lines.length - 1) {
      consumed += 1;
    }

    if (consumed > maxPrefixLength || insideFence) {
      continue;
    }

    lastLineBoundary = consumed;
    if (!trimmed) {
      lastBlankLineBoundary = consumed;
    }
  }

  if (lastBlankLineBoundary >= MINIMUM_REUSABLE_PREFIX_CHARACTERS) {
    return lastBlankLineBoundary;
  }
  if (lastLineBoundary >= MINIMUM_REUSABLE_PREFIX_CHARACTERS) {
    return lastLineBoundary;
  }
  return 0;
}

function splitAssistantMarkdown(
  messageId: string,
  text: string,
  namespace: string,
): AssistantRenderSegment[] {
  const blocks = splitMarkdownBlocks(text);
  if (blocks.length === 0) {
    return [
      {
        id: `${messageId}-${namespace}-empty`,
        content: "",
      },
    ];
  }

  // Keep keys stable across streaming growth so markdown blocks mutate in place.
  return blocks.map((content, index) => ({
    id: `${messageId}-${namespace}-${index}`,
    content,
  }));
}

function splitMarkdownBlocks(text: string): string[] {
  const lines = text.split("\n");
  const blocks: string[] = [];
  let current: string[] = [];
  let insideFence = false;
  let insideTable = false;

  const flush = () => {
    const block = current.join("\n").trim();
    if (block) {
      blocks.push(block);
    }
    current = [];
  };

  for (const line of lines) {
    const trimmed = line.trim();

    if (trimmed.startsWith("```") || trimmed.startsWith("~~~")) {
      insideFence = !insideFence;
      current.push(line);
      continue;
    }

    if (insideFence) {
      current.push(line);
      continue;
    }

    const isTableLine =
      trimmed.startsWith("|") || (insideTable && trimmed.includes("|"));
    if (isTableLine) {
      if (!insideTable && current.length > 0) {
        flush();
      }
      insideTable = true;
      current.push(line);
      continue;
    }

    if (insideTable) {
      flush();
      insideTable = false;
    }

    if (!trimmed) {
      flush();
      continue;
    }

    current.push(line);
  }

  flush();
  return coalesceTrailingPunctuation(blocks);
}

function coalesceTrailingPunctuation(blocks: string[]): string[] {
  if (blocks.length < 2) return blocks;
  const last = blocks[blocks.length - 1] ?? "";
  // If the final block is only punctuation/spacing, keep it attached to the previous block.
  if (/^[\s.,!?;:…)\]}'"]+$/u.test(last)) {
    const merged = blocks.slice(0, -1);
    merged[merged.length - 1] = `${merged[merged.length - 1] ?? ""}${last}`;
    return merged;
  }
  return blocks;
}

function touch(messageId: string) {
  const existingIndex = accessOrder.indexOf(messageId);
  if (existingIndex >= 0) {
    accessOrder.splice(existingIndex, 1);
  }
  accessOrder.push(messageId);
}

function trimCacheIfNeeded() {
  if (segmentCache.size <= MAX_CACHE_ENTRIES) {
    return;
  }

  while (segmentCache.size > TRIM_CACHE_TO) {
    const oldest = accessOrder.shift();
    if (!oldest) {
      return;
    }
    segmentCache.delete(oldest);
  }
}
