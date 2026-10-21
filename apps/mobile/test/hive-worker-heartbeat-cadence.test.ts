import {
  buildWorkerHeartbeatCreateFields,
  buildWorkerHeartbeatUpdateFields,
  MAX_HIVE_WORKER_HEARTBEAT_INTERVAL_SECS,
  parseWorkerHeartbeatCadence,
} from "../components/hive/worker-heartbeat-cadence.ts";

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

Deno.test("heartbeat cadence parses blank, exact positive u32 values, and leading zeroes", () => {
  assertEquals(
    parseWorkerHeartbeatCadence("   "),
    { value: null, error: null },
    "blank must remain an explicit local null",
  );
  assertEquals(
    parseWorkerHeartbeatCadence("000900"),
    { value: 900, error: null },
    "a valid whole number must retain its exact numeric value",
  );
  assertEquals(
    parseWorkerHeartbeatCadence(
      String(MAX_HIVE_WORKER_HEARTBEAT_INTERVAL_SECS),
    ),
    { value: MAX_HIVE_WORKER_HEARTBEAT_INTERVAL_SECS, error: null },
    "the server's inclusive u32 maximum must be accepted",
  );
});

Deno.test("heartbeat cadence rejects zero, fractions, signs, text, and u32 overflow", () => {
  for (
    const invalid of [
      "0",
      "1.5",
      "-1",
      "+1",
      "one hour",
      String(MAX_HIVE_WORKER_HEARTBEAT_INTERVAL_SECS + 1),
    ]
  ) {
    const parsed = parseWorkerHeartbeatCadence(invalid);
    assert(parsed.value === null, `${invalid} must not produce a value`);
    assert(
      parsed.error !== null,
      `${invalid} must explain its validation error`,
    );
  }
});

Deno.test("heartbeat cadence request fields preserve create null and unchanged update semantics", () => {
  assertEquals(
    buildWorkerHeartbeatCreateFields(null),
    {},
    "blank create must omit the field for core's autonomy-aware default",
  );
  assertEquals(
    buildWorkerHeartbeatCreateFields(120),
    { heartbeat_interval_secs: 120 },
    "create must send the exact entered cadence",
  );
  assertEquals(
    buildWorkerHeartbeatUpdateFields(null, 900),
    {},
    "blank edit must not clear or clobber a stored cadence",
  );
  assertEquals(
    buildWorkerHeartbeatUpdateFields(900, 900),
    {},
    "an unchanged edit must omit the cadence field",
  );
  assertEquals(
    buildWorkerHeartbeatUpdateFields(120, null),
    { heartbeat_interval_secs: 120 },
    "a new edit value must be sent when persisted state was null",
  );
  assertEquals(
    buildWorkerHeartbeatUpdateFields(120, 900),
    { heartbeat_interval_secs: 120 },
    "a changed edit must send the exact new cadence",
  );
});
