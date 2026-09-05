import { createMessageCopyAction, type MessageCopyFeedback } from "../components/chat/messageCopyAction.ts";

declare const Deno: { test(name: string, fn: () => void | Promise<void>): void };

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

Deno.test("message Copy shows success only after the clipboard write resolves", async () => {
  const write = deferred<void>();
  const feedback: MessageCopyFeedback[] = [];
  let written = "";
  const action = createMessageCopyAction((text) => { written = text; return write.promise; }, (value) => feedback.push(value));
  const result = action.copy("full answer with sources");
  assert(written === "full answer with sources", "the actual payload must reach the clipboard");
  assert(feedback.length === 1 && feedback[0] === null, "pending writes must not claim success");
  write.resolve();
  assert(await result, "a completed void-returning web write must succeed");
  assert(feedback.at(-1) === "Copied", "success follows the write receipt");
  action.cancel(); // Also proves successful feedback timers clean up on unmount.
});

Deno.test("message Copy handles rejection and false native receipts without false success", async () => {
  for (const write of [() => Promise.reject(new Error("permission denied")), () => Promise.resolve(false)]) {
    const feedback: MessageCopyFeedback[] = [];
    const action = createMessageCopyAction(write, (value) => feedback.push(value));
    assert(!await action.copy("answer"), "failed writes must return failure");
    assert(!feedback.includes("Copied") && feedback.at(-1) === "Could not copy", "failure feedback must be truthful");
    action.cancel();
  }
});

Deno.test("message Copy ignores late receipts after navigation or a newer copy", async () => {
  const first = deferred<void>();
  const second = deferred<void>();
  const feedback: MessageCopyFeedback[] = [];
  let calls = 0;
  const action = createMessageCopyAction(() => ++calls === 1 ? first.promise : second.promise, (value) => feedback.push(value));
  const stale = action.copy("older answer");
  const current = action.copy("newer answer");
  first.resolve();
  assert(!await stale && !feedback.includes("Copied"), "superseded receipts cannot update feedback");
  action.cancel();
  const before = feedback.length;
  second.reject(new Error("late permission rejection"));
  assert(!await current && feedback.length === before, "navigation/unmount must invalidate late failure feedback");
});
