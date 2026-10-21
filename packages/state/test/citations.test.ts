import { type Citation, citationWebUrl } from "@mitsuro/api";
import { createStreamCallbacks } from "../src/session/streaming.ts";
import { processStoredMessages } from "../src/session/messages.ts";
import {
  formatCitationSources,
  getMessageCitations,
  getMessageCopyText,
  getMessageText,
} from "../src/session/messageText.ts";
import {
  applyLivePartialAssistant,
  createStreamingAssistantMessage,
  pruneEmptyAssistantMessages,
} from "../src/session/transient.ts";
import { assistantVisualSegments } from "../../../apps/mobile/components/chat/assistantRenderPlan.ts";
import { compactHistoricalMessage } from "../../../apps/mobile/components/chat/presentationRetention.ts";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function equal(actual: unknown, expected: unknown, message: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${message}: expected ${JSON.stringify(expected)}, got ${
        JSON.stringify(actual)
      }`,
    );
  }
}
function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

const first: Citation = {
  url: "https://example.test/first",
  title: "First source",
  cited_text: "Sourced",
};
const second: Citation = {
  url: "https://example.test/second",
  title: "Second source",
  cited_text: "answer",
};

Deno.test("live, stored, historical, and copied citations preserve their text association", async () => {
  const ref = { current: createStreamingAssistantMessage() };
  let state: any = {
    messages: [ref.current],
    queuedMessages: [],
    isLoading: true,
    isStreaming: true,
  };
  const callbacks = createStreamCallbacks(
    ref,
    (partial: any) => {
      state = {
        ...state,
        ...(typeof partial === "function" ? partial(state) : partial),
      };
    },
    () => state,
    {
      planStore: { getState: () => ({ setItems() {} }) } as never,
      sessionsStore: { getState: () => ({ loadSessions() {} }) } as never,
      persistSessionMode: async () => {},
    },
  );
  callbacks.onTextDelta("Sourced ", [first]);
  callbacks.onToolCallStart("read-1", "Read");
  callbacks.onTextDelta("answer");
  callbacks.onTextDelta("", [second, second]);
  await new Promise((resolve) => setTimeout(resolve, 30));
  const live = ref.current;
  const textParts = live.renderParts?.filter((part) => part.type === "text");
  equal(
    textParts?.map((part) => part.citations),
    [[first], [second]],
    "sources stay on their ordered text segments and deduplicate replay",
  );
  const [stored] = processStoredMessages([{
    role: "assistant",
    content: [
      { type: "text", text: "Sourced ", citations: [first] },
      { type: "tool_use", id: "read-1", name: "Read", input: {} },
      { type: "text", text: "answer", citations: [second] },
    ],
  }]);
  equal(
    getMessageText(stored),
    "Sourced answer",
    "prose extraction remains annotation-free",
  );
  equal(
    getMessageCitations(stored),
    getMessageCitations(live),
    "reload retains all source metadata",
  );
  equal(
    getMessageCopyText(stored),
    getMessageCopyText(live),
    "live and reload exports have the same full answer and source list",
  );
  equal(
    getMessageCopyText(compactHistoricalMessage(stored)),
    getMessageCopyText(stored),
    "historical retention cannot discard attribution",
  );
  const segments = assistantVisualSegments(stored, false);
  equal(
    segments.filter((segment) => segment.type === "text").flatMap((segment) =>
      segment.citations ?? []
    ),
    [first, second],
    "smoothing across tools retains both source annotations",
  );
});

Deno.test("annotation-only text remains visible through stored and reconnect projections", () => {
  const [stored] = processStoredMessages([{
    role: "assistant",
    content: [{ type: "text", text: "", citations: [first] }],
  }]);
  assert(stored, "a source annotation is not an empty assistant message");
  const partial = applyLivePartialAssistant([], {
    text: "",
    citations: [first],
    tool_calls: [],
  }, "streaming");
  equal(
    getMessageCitations(partial[0]),
    [first],
    "reconnect retains annotations that arrive after text",
  );
  equal(
    pruneEmptyAssistantMessages(partial).length,
    1,
    "citation-only partials cannot be pruned",
  );
  equal(
    assistantVisualSegments(stored, false)[0].type,
    "text",
    "source-only content remains renderable",
  );
});

Deno.test("sources permit web links only and preserve unsafe original metadata as plain text", () => {
  const unsafe: Citation = {
    url: "javascript:alert(1)",
    title: "[run](javascript:alert(1))",
    cited_text: "claim",
  };
  const data: Citation = {
    url: "data:text/html,test",
    title: "Inline source",
    cited_text: "claim",
  };
  const file: Citation = {
    url: "file:///private",
    title: "Local source",
    cited_text: "claim",
  };
  equal(citationWebUrl(unsafe), null, "script URLs cannot open");
  equal(citationWebUrl(data), null, "inline URLs cannot open");
  equal(citationWebUrl(file), null, "local URLs cannot open");
  equal(citationWebUrl(first), first.url, "normal source links remain usable");
  const markdown = formatCitationSources([unsafe, data, file, first]);
  assert(
    !markdown.includes("](<javascript:") && !markdown.includes("](<data:") &&
      !markdown.includes("](<file:"),
    "copy and visible source markdown cannot contain active unsafe links",
  );
  assert(
    markdown.includes("\\[run\\]"),
    "provider labels cannot inject Markdown links",
  );
  const [stored] = processStoredMessages([{
    role: "assistant",
    content: [{ type: "text", text: "claim", citations: [unsafe] }],
  }]);
  equal(
    getMessageCitations(stored),
    [unsafe],
    "safety is a rendering policy, not loss of canonical metadata",
  );
});

Deno.test("distinct quoted passages survive source-list deduplication and old plain text stays unchanged", () => {
  const anotherQuote = { ...first, cited_text: "another passage" };
  const [stored] = processStoredMessages([{
    role: "assistant",
    content: [{
      type: "text",
      text: "answer",
      citations: [first, anotherQuote, first],
    }],
  }]);
  equal(
    getMessageCitations(stored),
    [first, anotherQuote],
    "annotation identity includes the cited passage",
  );
  equal(
    formatCitationSources(getMessageCitations(stored)).match(/- /g)?.length,
    1,
    "the displayed source list repeats each source once",
  );
  equal(
    getMessageCopyText({ content: "Legacy answer" }),
    "Legacy answer",
    "old text remains compatible",
  );
});
