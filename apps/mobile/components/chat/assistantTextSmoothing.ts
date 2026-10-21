import { mergeCitations, type Citation, type ToolCall } from "@mitsuro/api";

export type AssistantVisualSegment =
  | {
      type: "thinking";
      id: string;
      content: string;
    }
  | {
      type: "text";
      id: string;
      content: string;
      citations?: Citation[];
    }
  | {
      type: "attachments";
      id: string;
    }
  | {
      type: "tool";
      id: string;
      toolCall: ToolCall;
    }
  | {
      type: "exploration";
      id: string;
      tools: ToolCall[];
    };

function isSoftInterruption(segment: AssistantVisualSegment): boolean {
  // Only tool-like interruptions should rejoin split prose.
  // Thinking is a real phase boundary and must stay sticky.
  return segment.type === "exploration" || segment.type === "tool";
}

function startsLikeNewBlock(content: string): boolean {
  return /^(#{1,6}\s|[-*+]\s|\d+[.)]\s|```|>|<\w)/.test(content);
}

interface ActiveTextAccumulator {
  index: number;
  chunks: string[];
  trimmedSuffix: string;
  endsWithWhitespace: boolean;
}

function createTextAccumulator(
  index: number,
  content: string,
): ActiveTextAccumulator {
  return {
    index,
    chunks: [content],
    trimmedSuffix: content.trimEnd().slice(-64),
    endsWithWhitespace: /\s$/u.test(content),
  };
}

function shouldMergeAccumulator(
  accumulator: ActiveTextAccumulator,
  nextContent: string,
  hasInterveningSegments: boolean,
  interveningSegmentsAreSoft: boolean,
): boolean {
  if (!hasInterveningSegments || !interveningSegmentsAreSoft) return false;
  const next = nextContent.trimStart();
  const previous = accumulator.trimmedSuffix;
  if (!previous || !next || startsLikeNewBlock(next)) return false;
  if (previous.endsWith("```") || next.startsWith("```")) return false;

  const previousLast = previous[previous.length - 1] ?? "";
  const previousLooksUnfinished = /[A-Za-z0-9_/'")\]]/.test(previousLast);
  const nextLooksContinued =
    /^[a-z,.;:!?'"()\]}]/.test(next)
    || (/^[A-Za-z][A-Za-z0-9/_-]*[.!?]?$/.test(next)
      && previousLooksUnfinished);
  return previousLooksUnfinished && nextLooksContinued;
}

function appendToAccumulator(
  accumulator: ActiveTextAccumulator,
  nextContent: string,
): void {
  let chunk: string;
  if (accumulator.endsWithWhitespace || /^\s/u.test(nextContent)) {
    chunk = nextContent;
  } else {
    const left =
      accumulator.trimmedSuffix[accumulator.trimmedSuffix.length - 1] ?? "";
    const trimmedNext = nextContent.trimStart();
    const right = trimmedNext[0] ?? "";
    chunk =
      /[A-Za-z0-9_/'")\]]/.test(left) && /[A-Za-z0-9("'[]/.test(right)
        ? ` ${trimmedNext}`
        : trimmedNext;
  }
  accumulator.chunks.push(chunk);
  if (chunk.length > 0) {
    accumulator.endsWithWhitespace = /\s$/u.test(chunk);
    accumulator.trimmedSuffix = `${accumulator.trimmedSuffix}${chunk}`
      .trimEnd()
      .slice(-64);
  }
}

/**
 * Smooth interrupted text in one pass. Tracking the last text slot and the
 * intervening segment state avoids the former reverse scans and array slices.
 * Merged prose is held as chunks and joined once so repeated continuations do
 * not copy the entire accumulated message after every tool.
 */
export function smoothInterruptedText(
  segments: AssistantVisualSegment[],
): AssistantVisualSegment[] {
  const smoothed: AssistantVisualSegment[] = [];
  let previousTextIndex = -1;
  let interveningSegmentCount = 0;
  let interveningSegmentsAreSoft = true;
  let activeText: ActiveTextAccumulator | null = null;
  const finalizeActiveText = () => {
    if (!activeText || activeText.chunks.length === 1) return;
    const previousText = smoothed[activeText.index];
    if (previousText?.type !== "text") return;
    smoothed[activeText.index] = {
      ...previousText,
      content: activeText.chunks.join(""),
    };
  };

  for (const segment of segments) {
    if (segment.type !== "text") {
      smoothed.push(segment);
      if (previousTextIndex >= 0) {
        interveningSegmentCount += 1;
        interveningSegmentsAreSoft =
          interveningSegmentsAreSoft && isSoftInterruption(segment);
      }
      continue;
    }

    if (
      activeText
      && shouldMergeAccumulator(
        activeText,
        segment.content,
        interveningSegmentCount > 0,
        interveningSegmentsAreSoft,
      )
    ) {
      appendToAccumulator(activeText, segment.content);
      if (segment.citations?.length) {
        const previous = smoothed[activeText.index];
        if (previous.type === "text") {
          smoothed[activeText.index] = {
            ...previous,
            citations: mergeCitations(previous.citations, segment.citations),
          };
        }
      }
      continue;
    }

    finalizeActiveText();
    smoothed.push(segment);
    previousTextIndex = smoothed.length - 1;
    activeText = createTextAccumulator(previousTextIndex, segment.content);
    interveningSegmentCount = 0;
    interveningSegmentsAreSoft = true;
  }

  finalizeActiveText();
  return smoothed;
}
