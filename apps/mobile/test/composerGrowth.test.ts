import {
  COMPOSER_MAX_HEIGHT,
  COMPOSER_PILL_HEIGHT,
  countVisualLinesForSegment,
  estimateCompactInputHeight,
  INPUT_EXPANDED_VERTICAL_PADDING,
  INPUT_GROWTH_CHROME,
  INPUT_LINE_HEIGHT,
  maxComposerContentHeight,
  maxComposerVisibleLines,
  resolveComposerLayout,
} from "../components/chat/composerGrowth";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
  readTextFile(path: string): Promise<string>;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertLayout(
  value: string,
  expectedInputHeight: number,
  expectedBarHeight: number,
) {
  const layout = resolveComposerLayout(value, 240);
  assert(
    layout.inputHeight === expectedInputHeight,
    `expected input ${expectedInputHeight}, got ${layout.inputHeight}`,
  );
  assert(
    layout.barHeight === expectedBarHeight,
    `expected bar ${expectedBarHeight}, got ${layout.barHeight}`,
  );
  return layout;
}

Deno.test("single-line drafts stay at pill height", () => {
  const layout = assertLayout(
    "hello agent",
    INPUT_LINE_HEIGHT,
    COMPOSER_PILL_HEIGHT,
  );
  assert(!layout.expanded, "single-line composer must stay compact");
  assert(!layout.scrollEnabled, "single-line composer must not scroll");
});

Deno.test("hard newlines produce exact deterministic line steps", () => {
  const layout = assertLayout(
    "line one\nline two\nline three",
    INPUT_LINE_HEIGHT * 3,
    90,
  );
  assert(layout.expanded, "three-line composer must expand");
});

Deno.test("soft-wrapped typing grows without hard newlines", () => {
  const value =
    "this is a long draft that should wrap across multiple visual lines inside the compact composer";
  const layout = resolveComposerLayout(value, 160);
  assert(layout.expanded, "soft-wrapped text must expand");
  assert(
    layout.inputHeight > INPUT_LINE_HEIGHT,
    `expected multiple rows, got ${layout.inputHeight}`,
  );
  assert(
    layout.barHeight > COMPOSER_PILL_HEIGHT,
    `expected expanded bar, got ${layout.barHeight}`,
  );
});

Deno.test("soft wrap expands by visual line two at typical widths", () => {
  for (const width of [160, 200, 240, 280]) {
    let value = "typing";
    let layout = resolveComposerLayout(value, width);
    let guard = 0;
    while (!layout.expanded && guard < 80) {
      value += " word";
      layout = resolveComposerLayout(value, width);
      guard += 1;
    }
    assert(layout.expanded, `width=${width}: composer never expanded`);
    assert(value.length < 120, `width=${width}: composer expanded too late`);
  }
});

Deno.test("long words wrap without spaces", () => {
  const lines = countVisualLinesForSegment(
    "hello supercalifragilisticexpialidocious world",
    12,
  );
  assert(lines >= 3, `expected at least three lines, got ${lines}`);
});

Deno.test("deleting lines shrinks on every exact line boundary", () => {
  const three = assertLayout("a\na\na", INPUT_LINE_HEIGHT * 3, 90);
  const two = assertLayout("a\na", INPUT_LINE_HEIGHT * 2, 68);
  const one = assertLayout("a", INPUT_LINE_HEIGHT, COMPOSER_PILL_HEIGHT);
  assert(
    three.barHeight - two.barHeight === INPUT_LINE_HEIGHT,
    "three-to-two line deletion must shrink by one line",
  );
  assert(
    two.barHeight > one.barHeight,
    "two-to-one line deletion must collapse",
  );
});

Deno.test("repeated layout for identical input cannot ratchet or oscillate", () => {
  const value = "line one\nline two\nline three";
  const first = resolveComposerLayout(value, 240);
  for (let index = 0; index < 20; index += 1) {
    const next = resolveComposerLayout(value, 240);
    assert(
      JSON.stringify(next) === JSON.stringify(first),
      `layout changed on pass ${index}: ${JSON.stringify(next)}`,
    );
  }
});

Deno.test("recording forces pill height without changing text layout", () => {
  const normal = resolveComposerLayout("one\ntwo\nthree", 240);
  const recording = resolveComposerLayout("one\ntwo\nthree", 240, true);
  assert(
    normal.barHeight === 90,
    `expected normal bar 90, got ${normal.barHeight}`,
  );
  assert(
    recording.barHeight === COMPOSER_PILL_HEIGHT,
    `recording must force pill height, got ${recording.barHeight}`,
  );
  assert(
    recording.inputHeight === normal.inputHeight,
    "recording must not mutate text measurement",
  );
});

Deno.test("composer caps at four visible lines and enables scrolling", () => {
  const value = Array.from({ length: 20 }, (_, index) => `line ${index}`).join(
    "\n",
  );
  const layout = resolveComposerLayout(value, 240);
  assert(maxComposerVisibleLines() === 4, "expected four visible rows");
  assert(
    maxComposerContentHeight() === INPUT_LINE_HEIGHT * 4,
    "max content height must equal four line heights",
  );
  assert(
    layout.inputHeight === INPUT_LINE_HEIGHT * 4,
    "input must cap at four rows",
  );
  assert(
    layout.barHeight === COMPOSER_MAX_HEIGHT,
    "bar must cap at max height",
  );
  assert(layout.scrollEnabled, "capped input must scroll");
});

Deno.test("large pasted drafts take the bounded max-layout path", () => {
  const layout = resolveComposerLayout("x".repeat(500_000), 240);
  assert(
    layout.barHeight === COMPOSER_MAX_HEIGHT,
    "large paste must cap bar height",
  );
  assert(layout.scrollEnabled, "large paste must enable scrolling");
});

Deno.test("width changes recompute a bounded layout", () => {
  const value = "a moderately long composer draft with several words";
  const narrow = resolveComposerLayout(value, 140);
  const wide = resolveComposerLayout(value, 320);
  assert(
    narrow.inputHeight >= wide.inputHeight,
    "narrow layout must not use fewer rows than wide layout",
  );
  assert(narrow.barHeight <= COMPOSER_MAX_HEIGHT, "narrow bar exceeded max");
  assert(wide.barHeight <= COMPOSER_MAX_HEIGHT, "wide bar exceeded max");
});

Deno.test("visual inset belongs to bar, not explicit TextInput height", () => {
  const layout = resolveComposerLayout("one\ntwo\nthree", 240);
  assert(
    layout.inputHeight === INPUT_LINE_HEIGHT * 3,
    "input must contain text only",
  );
  assert(
    layout.barHeight ===
      layout.inputHeight +
        INPUT_EXPANDED_VERTICAL_PADDING * 2 +
        INPUT_GROWTH_CHROME,
    "bar must own the expanded visual inset",
  );
});

Deno.test("empty draft estimates zero but resolves to one text row", () => {
  assert(
    estimateCompactInputHeight("", 240) === 0,
    "empty estimate must be zero",
  );
  const layout = resolveComposerLayout("", 240);
  assert(
    layout.inputHeight === INPUT_LINE_HEIGHT,
    "empty input needs one text row",
  );
  assert(
    layout.barHeight === COMPOSER_PILL_HEIGHT,
    "empty bar must stay compact",
  );
});
