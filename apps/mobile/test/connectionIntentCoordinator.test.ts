import {
  createConnectionIntentCoordinator,
  createCredentialOperationQueue,
} from "../platform/connection-intent-coordinator.ts";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

Deno.test("only the latest connection intent can publish", async () => {
  const coordinator = createConnectionIntentCoordinator(
    createCredentialOperationQueue(),
  );
  const initialLoad = coordinator.begin();
  const connect = coordinator.begin();

  assert(
    !coordinator.isCurrent(initialLoad) && coordinator.isCurrent(connect),
    "a newer connect must invalidate initial-load publication",
  );

  const staleRead = await coordinator.runCurrentCredentialOperation(
    initialLoad,
    () => Promise.resolve("old-account"),
  );
  const currentWrite = await coordinator.runCurrentCredentialOperation(
    connect,
    () => Promise.resolve("new-account"),
  );
  assert(
    staleRead.status === "stale" &&
      currentWrite.status === "executed" &&
      currentWrite.value === "new-account",
    "queued stale work must be skipped while the latest intent executes",
  );
});

Deno.test("provider cleanup invalidates late work before replacement", async () => {
  const credentials = createCredentialOperationQueue();
  const oldProvider = createConnectionIntentCoordinator(credentials);
  const oldIntent = oldProvider.begin();
  oldProvider.begin();
  const replacementProvider = createConnectionIntentCoordinator(credentials);
  const replacementIntent = replacementProvider.begin();
  const oldResult = await oldProvider.runCurrentCredentialOperation(
    oldIntent,
    () => Promise.resolve("old-provider"),
  );
  const replacementResult = await replacementProvider
    .runCurrentCredentialOperation(
      replacementIntent,
      () => Promise.resolve("replacement-provider"),
    );

  assert(
    oldResult.status === "stale" &&
      replacementResult.status === "executed" &&
      replacementResult.value === "replacement-provider",
    "cleanup must prevent the old provider from scheduling late credential work",
  );
});

Deno.test("a newer credential operation wins after an old operation already started", async () => {
  const coordinator = createConnectionIntentCoordinator(
    createCredentialOperationQueue(),
  );
  let stored: string | null = "saved-account";
  const oldWriteMayFinish = deferred<void>();
  const oldIntent = coordinator.begin();
  const oldWrite = coordinator.runCurrentCredentialOperation(
    oldIntent,
    async () => {
      await oldWriteMayFinish.promise;
      stored = "stale-account";
    },
  );

  // Let the first queue callback start before the newer Disconnect intent.
  await Promise.resolve();
  coordinator.begin();
  const disconnectDelete = coordinator.runCredentialOperation(async () => {
    stored = null;
  });
  oldWriteMayFinish.resolve();
  await Promise.all([oldWrite, disconnectDelete]);

  assert(
    stored === null,
    "a serialized Disconnect delete must win after an unavoidable stale write",
  );
});

Deno.test("source order survives provider replacement and operation failures", async () => {
  const credentials = createCredentialOperationQueue();
  const oldProvider = createConnectionIntentCoordinator(credentials);
  const replacementProvider = createConnectionIntentCoordinator(credentials);
  const order: string[] = [];
  const migrationMayFinish = deferred<void>();

  const migration = oldProvider.runCredentialOperation(async () => {
    order.push("migration:start");
    await migrationMayFinish.promise;
    order.push("migration:end");
    throw new Error("old provider failed after migration");
  }).catch(() => undefined);
  const replacementDelete = replacementProvider.runCredentialOperation(
    async () => {
      order.push("replacement:delete");
    },
  );
  const replacementWrite = replacementProvider.runCredentialOperation(
    async () => {
      order.push("replacement:write");
    },
  );

  migrationMayFinish.resolve();
  await Promise.all([migration, replacementDelete, replacementWrite]);
  assert(
    order.join(",") ===
      "migration:start,migration:end,replacement:delete,replacement:write",
    "all providers must share one failure-tolerant credential lane",
  );
});

Deno.test("Reconnect read becomes stale when Disconnect is newer", async () => {
  const coordinator = createConnectionIntentCoordinator(
    createCredentialOperationQueue(),
  );
  const savedReadMayFinish = deferred<string>();
  const reconnect = coordinator.begin();
  const read = coordinator.runCurrentCredentialOperation(
    reconnect,
    () => savedReadMayFinish.promise,
  );

  await Promise.resolve();
  const disconnect = coordinator.begin();
  savedReadMayFinish.resolve("saved-account");
  const result = await read;

  assert(
    result.status === "executed" &&
      !coordinator.isCurrent(reconnect) &&
      coordinator.isCurrent(disconnect),
    "an already-started read may finish, but only Disconnect may publish",
  );
});

Deno.test("liveness snapshots observe an intent without superseding it", () => {
  const coordinator = createConnectionIntentCoordinator();
  const connection = coordinator.begin();
  const liveness = coordinator.current();
  assert(
    coordinator.isCurrent(connection) && coordinator.isCurrent(liveness),
    "reading current intent must not invalidate credential persistence",
  );
  coordinator.begin();
  assert(
    !coordinator.isCurrent(liveness),
    "an old liveness result must not clear a newer connection",
  );
});
