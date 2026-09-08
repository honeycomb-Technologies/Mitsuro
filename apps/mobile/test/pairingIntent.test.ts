import {
  clearPairingIntent,
  completePairingConnection,
  getPairingIntent,
  receivePairingIntent,
  requestPairingScreen,
  savePairingClaim,
} from "../platform/pairing-intent.ts";
declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};
function assert(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}
const link =
  `mitsuro://pair#v=1&origin=https%3A%2F%2Fserver.example&secret=mitsuro_pair_${
    "a".repeat(64)
  }`;
Deno.test("pairing intent survives a screen remount and rejects stale claim responses", () => {
  clearPairingIntent();
  requestPairingScreen();
  assert(
    getPairingIntent()?.destination === null,
    "empty pairing screen can be restored",
  );
  assert(receivePairingIntent(link), "valid code");
  const old = getPairingIntent()!;
  receivePairingIntent(link);
  savePairingClaim(old, {
    credential: "stale",
    comparison_code: "OLD",
    expires_at: 100,
  });
  assert(
    getPairingIntent()?.claim === null,
    "old request cannot replace a newer invitation",
  );
  const current = getPairingIntent()!;
  savePairingClaim(current, {
    credential: "current",
    comparison_code: "NEW",
    expires_at: 100,
  });
  requestPairingScreen();
  assert(
    getPairingIntent()?.claim?.credential === "current",
    "remount keeps the pending claim",
  );
  clearPairingIntent();
});
Deno.test("connection completion is single-flight across screen remounts", async () => {
  receivePairingIntent(link);
  savePairingClaim(getPairingIntent()!, {
    credential: "single-flight",
    comparison_code: "CODE",
    expires_at: 100,
  });
  const intent = getPairingIntent()!;
  let calls = 0;
  let resolve!: (success: boolean) => void;
  const connect = () => {
    calls++;
    return new Promise<boolean>((done) => {
      resolve = done;
    });
  };
  const first = completePairingConnection(intent, connect);
  const remounted = completePairingConnection(intent, connect);
  assert(
    first === remounted && calls === 1,
    "remount must not replace an in-flight connection",
  );
  resolve(true);
  await first;
  assert(
    getPairingIntent() === null,
    "successful connection clears ephemeral secrets",
  );
});
