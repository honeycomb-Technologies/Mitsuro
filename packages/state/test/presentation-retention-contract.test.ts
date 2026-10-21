import {
  compactHistoricalMessage,
  isTurnInRichWindow,
  RICH_RECENT_TURN_COUNT,
} from "../../../apps/mobile/components/chat/presentationRetention.ts";
import { assistantVisualSegments } from "../../../apps/mobile/components/chat/assistantRenderPlan.ts";
import { getMessageText } from "../src/session/messageText.ts";
import type { ChatMessage } from "../src/session/types.ts";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEquals<T>(actual: T, expected: T, message: string) {
  if (!Object.is(actual, expected)) {
    throw new Error(`${message}\nexpected: ${String(expected)}\nactual: ${String(actual)}`);
  }
}

Deno.test("only recent turns stay in the rich window", () => {
  assertEquals(isTurnInRichWindow(0, 10), false, "old turn is compact");
  assertEquals(isTurnInRichWindow(9, 10), true, "latest turn is rich");
  assertEquals(
    isTurnInRichWindow(10 - RICH_RECENT_TURN_COUNT, 10),
    true,
    "rich window includes configured recent turns",
  );
});

Deno.test("historical assistant messages collapse heavy tool detail", () => {
  const compact = compactHistoricalMessage({
    id: "a1",
    role: "assistant",
    content: "done",
    thinking: "t".repeat(5_000),
    toolCalls: [
      {
        id: "tool-1",
        name: "Bash",
        status: "success",
        output: "o".repeat(5_000),
      },
    ],
    attachments: [
      {
        type: "image",
        name: "x.png",
        mimeType: "image/png",
        base64: "abc",
        uri: "file://x.png",
      },
    ],
  });

  assert((compact.thinking?.length ?? 0) < 2_000, "thinking collapsed");
  assert((compact.toolCalls?.[0]?.output?.length ?? 0) < 2_000, "tool output collapsed");
  assert(!compact.attachments?.[0]?.base64, "historical base64 stripped");
  assertEquals(compact.attachments?.[0]?.uri, "file://x.png", "uri retained");
});

Deno.test("historical rendered thinking is compact while copy and canonical history stay complete", () => {
  const thinking = "reasoning ".repeat(1_000);
  const finalAnswer = "complete answer ".repeat(10_000) + "FINAL_TAIL";
  const source: ChatMessage = {
    id: "history-answer",
    role: "assistant",
    content: finalAnswer.slice(0, 120_000),
    thinking,
    renderParts: [
      { type: "thinking", id: "thought-1", content: thinking },
      { type: "tool", id: "tool-part-1", toolCallId: "read-1" },
      { type: "text", id: "answer-1", content: finalAnswer },
    ],
    toolCalls: [{ id: "read-1", name: "Read", status: "success", output: "file ".repeat(1_000) }],
  };
  const before = JSON.stringify(source);
  const compact = compactHistoricalMessage(source);
  const segments = assistantVisualSegments(compact, false);
  const renderedThinking = segments.filter((segment) => segment.type === "thinking");
  assertEquals(renderedThinking.length, 1, "historical thinking remains expandable in its original slot");
  assert(renderedThinking[0].content.endsWith("…[older thinking collapsed]"), "expansion must clearly identify the retained preview");
  assert(renderedThinking[0].content.length < 1_000, "the actual rendered payload must be bounded");
  assertEquals(getMessageText(compact), finalAnswer, "copy must keep the complete answer in a historical turn");
  assertEquals(compact.renderParts?.[2], source.renderParts?.[2], "full answer parts retain stable identity");
  assertEquals(JSON.stringify(source), before, "historical projection must never mutate canonical history");
});
