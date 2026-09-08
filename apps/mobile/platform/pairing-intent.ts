import { type PairingClaim, parsePairingLink } from "@mitsuro/api";

export interface PairingIntent {
  destination: ReturnType<typeof parsePairingLink> | null;
  claim: PairingClaim | null;
}
// Only memory: neither bootstrap nor pending device secrets enter navigation,
// diagnostics, or persistence. Retain them across the store-graph handoff.
let pending: PairingIntent | null = null;
let completion: { credential: string; promise: Promise<boolean> } | null = null;
const listeners = new Set<() => void>();
const publish = () => listeners.forEach((listener) => listener());
export function requestPairingScreen() {
  if (!pending) {
    pending = { destination: null, claim: null };
    publish();
  }
}
export function receivePairingIntent(value: string): boolean {
  try {
    pending = { destination: parsePairingLink(value), claim: null };
    publish();
    return true;
  } catch {
    return false;
  }
}
export function savePairingClaim(expected: PairingIntent, claim: PairingClaim) {
  if (pending === expected) {
    pending = { ...expected, claim };
    publish();
  }
}
export function completePairingConnection(
  intent: PairingIntent,
  connect: (origin: string, token: string) => Promise<boolean>,
): Promise<boolean> {
  const { destination, claim } = intent;
  if (!destination || !claim) return Promise.resolve(false);
  if (completion?.credential === claim.credential) return completion.promise;
  const promise = connect(destination.origin, claim.credential).then(
    (success) => {
      if (success && pending?.claim?.credential === claim.credential) {
        clearPairingIntent();
      }
      return success;
    },
  ).finally(() => {
    if (completion?.credential === claim.credential) completion = null;
  });
  completion = { credential: claim.credential, promise };
  return promise;
}
export const getPairingIntent = () => pending;
export function clearPairingIntent() {
  pending = null;
  publish();
}
export function subscribePairingIntent(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
