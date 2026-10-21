import type { ChatMessage } from "@mitsuro/api";
import {
  compactHistoricalMessage,
  isTurnInRichWindow,
} from "./presentationRetention";

export interface TranscriptTurn {
  id: string;
  messages: ChatMessage[];
  isLive: boolean;
  /** Opaque identity token; stable only when the complete rendered turn is reused. */
  renderSignature: object;
}

export interface SplitTranscriptTurns {
  /** Completed/previous turns rendered as stable FlatList rows. */
  historicalTurns: TranscriptTurn[];
  /** Active/latest turn isolated from historical row updates. */
  liveTurn: TranscriptTurn | null;
  /** Full turn list (historical + live) for callers that need both. */
  turns: TranscriptTurn[];
}

/**
 * A committed transcript derivation that may be reused by the next render.
 * Callers must only retain this after commit; render-time mutation can leak a
 * speculative Concurrent React render into the visible transcript.
 */
export interface TranscriptTurnsCache {
  sourceMessages: ChatMessage[];
  liveStartIndex: number;
  isStreaming: boolean;
  split: SplitTranscriptTurns;
}

export interface CachedSplitTranscriptTurns extends SplitTranscriptTurns {
  cache: TranscriptTurnsCache;
}

/**
 * Split the transcript so streaming updates only invalidate the live turn.
 * Historical turns keep stable identities/signatures while the tail grows.
 */
export function splitTranscriptTurns(
  messages: ChatMessage[],
  isStreaming: boolean,
): SplitTranscriptTurns {
  const { historicalTurns, liveTurn, turns } = splitTranscriptTurnsCached(
    messages,
    isStreaming,
  );
  return { historicalTurns, liveTurn, turns };
}

/**
 * Rebuild only the isolated tail when the finalized prefix is unchanged.
 *
 * Prefix validity deliberately uses message identity for every finalized
 * entry. Session state updates are immutable, so an equal-length replacement
 * anywhere in history invalidates the cache without recomputing content
 * signatures for the common live-token path.
 */
export function splitTranscriptTurnsCached(
  messages: ChatMessage[],
  isStreaming: boolean,
  previous?: TranscriptTurnsCache | null,
): CachedSplitTranscriptTurns {
  if (
    previous
    && previous.sourceMessages === messages
    && previous.isStreaming === isStreaming
  ) {
    return { ...previous.split, cache: previous };
  }
  if (previous && canReuseFinalizedPrefix(previous, messages)) {
    const tailMessages = messages.slice(previous.liveStartIndex);
    // A new user message creates a new turn and changes the rich-retention
    // window, so it must take the full rebuild path.
    if (!tailMessages.some((message, index) => index > 0 && message.role === "user")) {
      const liveTurn = buildTurn(
        tailMessages,
        turnId(tailMessages, previous.split.turns.length - 1),
        isStreaming,
        true,
      );
      const historicalTurns = previous.split.historicalTurns;
      const turns = liveTurn ? [...historicalTurns, liveTurn] : historicalTurns;
      const split = { historicalTurns, liveTurn, turns };
      return {
        ...split,
        cache: {
          sourceMessages: messages,
          liveStartIndex: previous.liveStartIndex,
          isStreaming,
          split,
        },
      };
    }
  }

  return buildTranscriptTurnsFromScratch(messages, isStreaming);
}

function buildTranscriptTurnsFromScratch(
  messages: ChatMessage[],
  isStreaming: boolean,
): CachedSplitTranscriptTurns {
  const groupedMessages: ChatMessage[][] = [];
  const groupStartIndexes: number[] = [];
  let currentGroup: ChatMessage[] = [];

  for (let messageIndex = 0; messageIndex < messages.length; messageIndex += 1) {
    const message = messages[messageIndex]!;
    const startsNewTurn = message.role === "user" && currentGroup.length > 0;
    if (startsNewTurn) {
      groupedMessages.push(currentGroup);
      groupStartIndexes.push(messageIndex - currentGroup.length);
      currentGroup = [message];
      continue;
    }

    currentGroup.push(message);
  }

  if (currentGroup.length > 0) {
    groupedMessages.push(currentGroup);
    groupStartIndexes.push(messages.length - currentGroup.length);
  }

  const lastIndex = groupedMessages.length - 1;
  const turnCount = groupedMessages.length;
  const turns = groupedMessages.map((turnMessages, index) => {
    const id = turnId(turnMessages, index);
    // Only the isolated tail row is live; historical rows never flip live state
    // during stream ticks.
    const isLive = isStreaming && index === lastIndex;
    // Litter-style retention: only recent turns keep full tool/thinking detail.
    const rich = isTurnInRichWindow(index, turnCount) || isLive;
    return buildTurn(turnMessages, id, isLive, rich)!;
  });

  if (turns.length === 0) {
    const split = {
      historicalTurns: [],
      liveTurn: null,
      turns,
    };
    return {
      ...split,
      cache: {
        sourceMessages: messages,
        liveStartIndex: 0,
        isStreaming,
        split,
      },
    };
  }

  // Always isolate the latest turn from FlatList row recycling so stream
  // deltas do not rebuild historical cells.
  const split = {
    historicalTurns: turns.slice(0, -1),
    liveTurn: turns[turns.length - 1] ?? null,
    turns,
  };
  return {
    ...split,
    cache: {
      sourceMessages: messages,
      liveStartIndex: groupStartIndexes[groupStartIndexes.length - 1] ?? 0,
      isStreaming,
      split,
    },
  };
}

function canReuseFinalizedPrefix(
  previous: TranscriptTurnsCache,
  messages: ChatMessage[],
): boolean {
  if (previous.split.liveTurn === null || messages.length < previous.liveStartIndex) {
    return false;
  }
  for (let index = 0; index < previous.liveStartIndex; index += 1) {
    if (previous.sourceMessages[index] !== messages[index]) return false;
  }
  return true;
}

function turnId(messages: ChatMessage[], fallbackIndex: number): string {
  const firstMessage = messages[0];
  return firstMessage ? `turn-${firstMessage.id}` : `turn-${fallbackIndex}`;
}

function buildTurn(
  turnMessages: ChatMessage[],
  id: string,
  isLive: boolean,
  rich: boolean,
): TranscriptTurn | null {
  if (turnMessages.length === 0) return null;
  const displayMessages = rich
    ? turnMessages
    : turnMessages.map(compactHistoricalMessage);
  return {
    id,
    messages: displayMessages,
    isLive,
    renderSignature: {},
  };
}

export function findTurnIndexForMessage(
  turns: TranscriptTurn[],
  messageId: string,
): number {
  return turns.findIndex((turn) =>
    turn.messages.some((message) => message.id === messageId),
  );
}

export function turnContainsMessage(
  turn: TranscriptTurn | null | undefined,
  messageId: string,
): boolean {
  if (!turn) return false;
  return turn.messages.some((message) => message.id === messageId);
}
