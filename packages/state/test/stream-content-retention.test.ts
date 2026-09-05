/**
 * Live stream content retention.
 *
 * When a single assistant message exceeds MAX_LIVE_MESSAGE_CONTENT_LENGTH,
 * earlier builds kept only the *tail* of the text. That made the start of long
 * replies disappear mid-turn (especially during tool-heavy mobile sessions).
 * Keep the head of the stream; renderParts still accumulate full text for UI.
 */

import {
  MAX_LIVE_MESSAGE_CONTENT_LENGTH,
} from "../src/session/constants.ts";
import { createStreamCallbacks } from "../src/session/streaming.ts";
import { createStreamingAssistantMessage } from "../src/session/transient.ts";
import { getMessageText } from "../src/session/messageText.ts";
import { processStoredMessages } from "../src/session/messages.ts";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEquals(actual: unknown, expected: unknown, message: string) {
  if (actual !== expected) {
    throw new Error(
      `${message}\nexpected: ${String(expected)}\nactual: ${String(actual)}`,
    );
  }
}

function testHarness() {
  const ref = { current: createStreamingAssistantMessage() };
  let state: any = {
    messages: [ref.current],
    queuedMessages: [],
    isLoading: true,
    isStreaming: true,
    isThinking: false,
    thinkingContent: "",
  };
  const set = (partial: any) => {
    const update = typeof partial === "function" ? partial(state) : partial;
    state = { ...state, ...update };
  };
  const callbacks = createStreamCallbacks(ref, set, () => state, {
    planStore: { getState: () => ({ setItems() {} }) } as never,
    sessionsStore: { getState: () => ({ loadSessions() {} }) } as never,
    persistSessionMode: async () => {},
  });

  return { callbacks, ref };
}

Deno.test("stream content keeps the head when live bound is exceeded", async () => {
  const { callbacks, ref } = testHarness();
  const prefix = "HEAD_MARKER_";
  const filler = "x".repeat(MAX_LIVE_MESSAGE_CONTENT_LENGTH);
  // Chunk so we exercise delta coalescing + appendBounded.
  callbacks.onTextDelta(prefix);
  for (let i = 0; i < filler.length; i += 4_000) {
    callbacks.onTextDelta(filler.slice(i, i + 4_000));
  }
  callbacks.onTextDelta("TAIL_SHOULD_NOT_REPLACE_HEAD");
  await new Promise((resolve) => setTimeout(resolve, 30));

  assert(
    ref.current.content.startsWith(prefix),
    "bounded content must retain the start of the stream, not only the tail",
  );
  assertEquals(
    ref.current.content.length,
    MAX_LIVE_MESSAGE_CONTENT_LENGTH,
    "bounded content length must equal the live max",
  );
  assert(
    !ref.current.content.includes("TAIL_SHOULD_NOT_REPLACE_HEAD"),
    "overflow tail must not replace the retained head in content",
  );

  const textParts = (ref.current.renderParts || []).filter(
    (part) => part.type === "text",
  );
  const rendered = textParts.map((part) =>
    part.type === "text" ? part.content : ""
  ).join("");
  assert(
    rendered.startsWith(prefix),
    "renderParts must still start with the original head",
  );
  assert(
    rendered.includes("TAIL_SHOULD_NOT_REPLACE_HEAD"),
    "renderParts must keep the full stream including the overflow tail for display",
  );
  assertEquals(
    getMessageText(ref.current),
    rendered,
    "copy/export must include the complete visible answer beyond the content bound",
  );
  const [reloaded] = processStoredMessages([{
    role: "assistant",
    content: [{ type: "text", text: rendered }],
  }]);
  assertEquals(
    getMessageText(reloaded),
    getMessageText(ref.current),
    "the full copied answer must survive canonical reload",
  );
});
