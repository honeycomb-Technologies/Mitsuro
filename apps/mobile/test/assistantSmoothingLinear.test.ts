import {
  smoothInterruptedText,
  type AssistantVisualSegment,
} from "../components/chat/assistantTextSmoothing";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function tool(id: string): AssistantVisualSegment {
  return {
    type: "exploration",
    id,
    tools: [],
  };
}

Deno.test("assistant smoothing joins prose while preserving Markdown boundaries", () => {
  for (const [previous, next, expected] of [
    ["Looking at the ", "registry next.", "Looking at the registry next."],
    ["The helper returns", "null when missing.", "The helper returns null when missing."],
    ["server/mobile", "edges.", "server/mobile edges."],
    ["See the following:", "- item one", null],
    ["Example:", "```ts\nconst x = 1\n```", null],
    ["```\ncode\n```", "more prose here", null],
    ["All done.", "Next we restart.", null],
  ] as const) {
    const input: AssistantVisualSegment[] = [
      { type: "text", id: "before", content: previous },
      tool("tool"),
      { type: "text", id: "after", content: next },
    ];
    const output = smoothInterruptedText(input);
    if (expected === null) {
      assert(JSON.stringify(output) === JSON.stringify(input), `boundary changed: ${next}`);
    } else {
      assert(output.length === 2 && output[0]?.type === "text"
        && output[0].content === expected && output[1] === input[1],
      `continuation changed: ${previous} / ${next}`);
    }
  }
});

Deno.test("assistant smoothing preserves soft interruption order in one pass", () => {
  const input: AssistantVisualSegment[] = [
    { type: "text", id: "t1", content: "we only" },
    tool("tool-1"),
    { type: "text", id: "t2", content: "skimmed." },
    {
      type: "thinking",
      id: "thinking-1",
      content: "boundary",
    },
    { type: "text", id: "t3", content: "New block." },
  ];

  const output = smoothInterruptedText(input);
  assert(output.length === 4, "one continuation should merge");
  assert(output[0]?.type === "text", "first item remains text");
  assert(
    output[0]?.type === "text" && output[0].content === "we only skimmed.",
    "merged prose must stay byte-exact",
  );
  assert(output[1]?.id === "tool-1", "tool slot order must remain stable");
  assert(output[2]?.type === "thinking", "hard boundaries must remain stable");
  assert(output[3]?.id === "t3", "text after a hard boundary must not merge");
});

Deno.test("assistant smoothing preserves tool slots in large committed turns", () => {
  const input: AssistantVisualSegment[] = [
    { type: "text", id: "text-start", content: "fragment" },
  ];
  for (let index = 0; index < 2_000; index += 1) {
    input.push(tool(`tool-${index}`));
    input.push({ type: "text", id: `text-${index}`, content: `part${index}` });
  }

  const output = smoothInterruptedText(input);
  assert(output.length === 2_001, "every continuation must merge into one text slot");
  for (let index = 0; index < 2_000; index += 1) {
    assert(output[index + 1] === input[index * 2 + 1], "tool identity and order must survive");
  }
});
