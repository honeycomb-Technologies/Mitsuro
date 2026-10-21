import type { ModelInfo, ModelKey } from "@mitsuro/api";
import {
  buildWorkerModelCreateFields,
  buildWorkerModelUpdateFields,
} from "../components/hive/worker-model-request-fields.ts";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEquals(actual: unknown, expected: unknown, message: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${message}\nexpected: ${JSON.stringify(expected)}\nactual: ${
        JSON.stringify(actual)
      }`,
    );
  }
}

function model(id: string, key?: ModelKey | null): ModelInfo {
  return {
    id,
    key,
    display_name: id,
    provider: key?.provider ?? "legacy",
    context_window: 128_000,
    max_output: 8_192,
    supports_thinking: true,
    supports_tools: true,
    supports_vision: false,
  };
}

const grokKey: ModelKey = {
  provider: "grok",
  model_id: "grok-4.6",
  auth_scope: null,
  api_format: "responses",
};

Deno.test("Worker create requires and sends an exact model key", () => {
  assertEquals(
    buildWorkerModelCreateFields(model("grok-4.6", grokKey)),
    { model: "grok-4.6", model_key: grokKey },
    "create must send the exact selected provider/model identity",
  );
  assertEquals(
    buildWorkerModelCreateFields(null),
    null,
    "create without a selection must remain invalid",
  );
  assertEquals(
    buildWorkerModelCreateFields(model("legacy-model")),
    null,
    "a keyless legacy catalog row cannot create an ambiguously pinned Worker",
  );
});

Deno.test("unchanged Worker model is omitted from unrelated updates", () => {
  const selected: ModelInfo = {
    ...model("grok-4.6", {
      ...grokKey,
      auth_scope: undefined,
    }),
    display_name: "Renamed catalog label",
    provider: "renamed catalog provider label",
    context_window: 256_000,
    max_output: 16_384,
    supports_thinking: false,
    supports_tools: false,
    supports_vision: true,
  };
  assertEquals(
    buildWorkerModelUpdateFields(selected, grokKey),
    {},
    "the exact persisted key must be omitted despite nullability and non-key metadata drift",
  );
  const heartbeatOnlyUpdate = {
    ...buildWorkerModelUpdateFields(selected, grokKey),
    heartbeat_interval_secs: 901,
  };
  assertEquals(
    heartbeatOnlyUpdate,
    { heartbeat_interval_secs: 901 },
    "a cadence-only edit must not resend model or model_key",
  );
  const serialized = JSON.stringify(heartbeatOnlyUpdate);
  assert(
    !serialized.includes('"model":') &&
      !serialized.includes('"model_key":'),
    `the serialized cadence-only request must omit model fields: ${serialized}`,
  );
});

const exactKeyDriftCases: Array<{ axis: string; key: ModelKey }> = [
  {
    axis: "provider",
    key: { ...grokKey, provider: "openrouter" },
  },
  {
    axis: "model_id",
    key: { ...grokKey, model_id: "grok-4.7" },
  },
  {
    axis: "auth_scope",
    key: { ...grokKey, auth_scope: "team" },
  },
  {
    axis: "api_format",
    key: { ...grokKey, api_format: "chat_completions" },
  },
];

for (const { axis, key } of exactKeyDriftCases) {
  Deno.test(`Worker model update sends one-axis ${axis} drift`, () => {
    const selected = model(key.model_id, key);
    assertEquals(
      buildWorkerModelUpdateFields(selected, grokKey),
      { model: key.model_id, model_key: key },
      `${axis} drift must send the newly selected exact key`,
    );
  });
}

Deno.test("changed Worker model sends its exact key", () => {
  const changedKey: ModelKey = {
    provider: "openrouter",
    model_id: "free-reasoner",
    auth_scope: "default",
    api_format: "chat_completions",
  };
  assertEquals(
    buildWorkerModelUpdateFields(model("free-reasoner", changedKey), grokKey),
    { model: "free-reasoner", model_key: changedKey },
    "a changed selection must retain provider, auth scope, and transport",
  );
  assertEquals(
    buildWorkerModelUpdateFields(model("grok-4.6", grokKey), null),
    { model: "grok-4.6", model_key: grokKey },
    "a persisted legacy/no-key Worker must explicitly upgrade to the selected exact pin",
  );
  const sameBareIdKey: ModelKey = {
    ...grokKey,
    provider: "openrouter",
  };
  assertEquals(
    buildWorkerModelUpdateFields(
      model("grok-4.6", sameBareIdKey),
      grokKey,
    ),
    { model: "grok-4.6", model_key: sameBareIdKey },
    "the same bare model ID with a different exact key must still be sent",
  );
});

Deno.test("null and legacy Worker edit states remain non-destructive and honest", () => {
  assertEquals(
    buildWorkerModelUpdateFields(null, grokKey),
    {},
    "a persisted exact key absent from the catalog must remain unchanged",
  );
  assertEquals(
    buildWorkerModelUpdateFields(model("legacy-model"), null),
    null,
    "a keyless legacy selection must be rejected instead of sending a bare slug",
  );
});
