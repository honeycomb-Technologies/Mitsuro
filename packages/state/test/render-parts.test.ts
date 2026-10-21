/**
 * Deno tests for render-part text join parity (live stream vs stored reload).
 *
 * Run: deno test packages/state/test/render-parts.test.ts
 */

import { processStoredMessages } from "../src/session/messages.ts";
import { getMessageText } from "../src/session/messageText.ts";
import { MAX_MESSAGE_CONTENT_LENGTH } from "../src/session/constants.ts";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assertEquals(actual: unknown, expected: unknown, message: string) {
  if (actual !== expected) {
    throw new Error(
      `${message}\nexpected: ${String(expected)}\nactual: ${String(actual)}`,
    );
  }
}

/** Must match streaming.ts and messages.ts joinAdjacentText policy. */
function joinAdjacentText(existing: string, next: string): string {
  if (!existing) return next;
  if (!next) return existing;
  return existing + next;
}

Deno.test("live and stored text joins use raw concatenation", () => {
  // Stream deltas: "Hel" + "lo" + " world"
  const live = ["Hel", "lo", " world"].reduce(
    (acc, chunk) => joinAdjacentText(acc, chunk),
    "",
  );
  // Stored consecutive text blocks for the same continuous prose
  const stored = ["Hello", " world"].reduce(
    (acc, chunk) => joinAdjacentText(acc, chunk),
    "",
  );
  assertEquals(live, "Hello world", "live stream should concat");
  assertEquals(stored, "Hello world", "stored blocks should match live");
  assertEquals(live, stored, "parity between live and stored join");
});

Deno.test("joinAdjacentText does not invent separators", () => {
  assertEquals(joinAdjacentText("code", "base"), "codebase", "no glue space");
  assertEquals(joinAdjacentText("a\n", "b"), "a\nb", "preserve existing newline");
  assertEquals(joinAdjacentText("", "only"), "only", "empty existing");
  assertEquals(joinAdjacentText("only", ""), "only", "empty next");
});

Deno.test("canonical reload and plain text preserve ordered prose and exact separators", () => {
  const [message] = processStoredMessages([{
    role: "assistant",
    content: [
      { type: "thinking", thinking: "Private reasoning is separate" },
      { type: "text", text: "Hello " },
      { type: "text", text: "world.\n" },
      { type: "tool_use", id: "read-1", name: "Read", input: { path: "a.ts" } },
      { type: "text", text: "The answer is complete." },
    ],
  }]);
  assertEquals(message.content, "Hello world.\nThe answer is complete.", "reload must not invent newlines");
  assertEquals(getMessageText(message), message.content, "ordered copy and convenience text agree below the bound");
  assertEquals(getMessageText({ content: "legacy prose" }), "legacy prose", "old messages without parts retain copy support");
});

Deno.test("canonical reload bounds only the convenience text and keeps the complete answer", () => {
  const answer = "a".repeat(MAX_MESSAGE_CONTENT_LENGTH) + "COMPLETE_TAIL";
  const [message] = processStoredMessages([{
    role: "assistant",
    content: [{ type: "text", text: answer }],
  }]);
  assertEquals(message.content.length, MAX_MESSAGE_CONTENT_LENGTH, "one oversized block must respect the convenience bound");
  assertEquals(getMessageText(message), answer, "the semantic answer must remain complete");
});
