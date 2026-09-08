export type ConnectionIntent = Readonly<{
  generation: number;
}>;

export type CurrentCredentialOperationResult<T> =
  | Readonly<{ status: "executed"; value: T }>
  | Readonly<{ status: "stale" }>;

export interface CredentialOperationQueue {
  run<T>(operation: () => Promise<T>): Promise<T>;
}

export interface ConnectionIntentCoordinator {
  begin(): ConnectionIntent;
  current(): ConnectionIntent;
  isCurrent(intent: ConnectionIntent): boolean;
  runCredentialOperation<T>(operation: () => Promise<T>): Promise<T>;
  runCurrentCredentialOperation<T>(
    intent: ConnectionIntent,
    operation: () => Promise<T>,
  ): Promise<CurrentCredentialOperationResult<T>>;
}

/**
 * Serializes every access to the one committed connection-credential record.
 *
 * The queue is deliberately shared by every ConnectionProvider in this JS
 * process. React remounts (including development Strict Mode) therefore cannot
 * let an old migration/write/delete race a replacement provider's operation.
 */
export function createCredentialOperationQueue(): CredentialOperationQueue {
  let tail: Promise<void> = Promise.resolve();

  return {
    run<T>(operation: () => Promise<T>): Promise<T> {
      const result = tail.then(operation, operation);
      tail = result.then(
        () => undefined,
        () => undefined,
      );
      return result;
    },
  };
}

const processCredentialOperations = createCredentialOperationQueue();

/**
 * Couples latest-intent publication fences with source-ordered credential IO.
 *
 * An operation which has already started cannot be cancelled safely. A newer
 * operation is queued behind it and therefore owns the final stored value.
 * Work that has not started is skipped once its connection intent is stale.
 */
export function createConnectionIntentCoordinator(
  credentials = processCredentialOperations,
): ConnectionIntentCoordinator {
  let generation = 0;

  const isCurrent = (intent: ConnectionIntent) =>
    intent.generation === generation;

  return {
    begin() {
      generation += 1;
      return { generation };
    },

    current: () => ({ generation }),

    isCurrent,

    runCredentialOperation(operation) {
      return credentials.run(operation);
    },

    runCurrentCredentialOperation(intent, operation) {
      return credentials.run(async () => {
        if (!isCurrent(intent)) return { status: "stale" } as const;
        return {
          status: "executed",
          value: await operation(),
        } as const;
      });
    },
  };
}
