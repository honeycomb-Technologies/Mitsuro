/** Versioned bootstrap links contain a one-use invitation, never an owner token. */
export interface PairingInvitation {
  id: string;
  secret: string;
  origin: string;
  expires_at: number;
}
export interface PairingClaim {
  credential: string;
  comparison_code: string;
  expires_at: number;
}
export interface PairedDevice {
  id: string;
  name: string;
  created_at: number;
  expires_at: number;
  revoked_at: number | null;
}
export interface PendingDevice {
  id: string;
  name: string;
  comparison_code: string;
  expires_at: number;
}
export interface PairingOverview {
  pairing_version: 1;
  devices: PairedDevice[];
  pending: PendingDevice[];
}
export type PairingStatus = { status: "pending" | "unavailable" } | {
  status: "approved";
  device_id: string;
};

export function pairingOrigin(value: string): string {
  const url = new URL(value);
  if (
    value.length > 2048 || url.username || url.password || url.search ||
    url.hash || url.pathname !== "/" ||
    !(url.protocol === "https:" ||
      (url.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)))
  ) {
    throw new Error(
      "Use an HTTPS server address without a path or credentials.",
    );
  }
  return url.origin;
}

export function pairingLink(invitation: PairingInvitation): string {
  const origin = pairingOrigin(invitation.origin);
  if (!/^mitsuro_pair_[a-f0-9]{64}$/.test(invitation.secret)) {
    throw new Error("Invalid pairing invitation.");
  }
  const params = new URLSearchParams({
    v: "1",
    origin,
    secret: invitation.secret,
  });
  return `mitsuro://pair#${params}`;
}

export function parsePairingLink(
  value: string,
): { origin: string; secret: string } {
  if (value.length > 4096) throw new Error("Invalid pairing code.");
  const url = new URL(value);
  if (
    url.protocol !== "mitsuro:" || url.hostname !== "pair" || url.pathname ||
    url.search || url.username || url.password || url.port
  ) throw new Error("Invalid pairing code.");
  const params = new URLSearchParams(url.hash.slice(1));
  if (
    [...params.keys()].length !== 3 || params.get("v") !== "1" ||
    !/^mitsuro_pair_[a-f0-9]{64}$/.test(params.get("secret") ?? "")
  ) throw new Error("Invalid pairing code.");
  return {
    origin: pairingOrigin(params.get("origin") ?? ""),
    secret: params.get("secret")!,
  };
}

/** Isolated from the connected client: existing credentials must never follow a scanned URL. */
export async function pairingRequest<T>(
  origin: string,
  action: "claim" | "status",
  signal: AbortSignal,
  input: { secret: string; name: string } | { credential: string },
  fetchImpl: typeof fetch = fetch,
): Promise<T> {
  const destination = pairingOrigin(origin);
  const claiming = action === "claim";
  const response = await fetchImpl(`${destination}/api/pairing/${action}`, {
    method: claiming ? "POST" : "GET",
    signal,
    redirect: "error",
    credentials: "omit",
    cache: "no-store",
    headers: claiming ? { "Content-Type": "application/json" } : {
      Authorization: `Bearer ${(input as { credential: string }).credential}`,
    },
    ...(claiming
      ? { body: JSON.stringify({ ...input, origin: destination }) }
      : {}),
  });
  if (!response.ok) {
    throw new Error(
      response.status === 429
        ? "Pairing is busy. Wait a moment and try again."
        : "Pairing is unavailable. Create a new code on your server.",
    );
  }
  return response.json() as Promise<T>;
}

/** Resolve before opening a socket. New device credentials never enter a URL. */
export async function authorizeDeviceStream(
  streamUrl: string,
  credential: string,
  signal?: AbortSignal,
  fetchImpl: typeof fetch = fetch,
): Promise<string> {
  const url = new URL(streamUrl);
  if (!["ws:", "wss:"].includes(url.protocol) || url.username || url.password) {
    throw new Error("Invalid stream address.");
  }
  const origin = pairingOrigin(url.origin.replace(/^ws/, "http"));
  const response = await fetchImpl(`${origin}/api/stream-ticket`, {
    method: "POST",
    signal,
    redirect: "error",
    credentials: "omit",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${credential}`,
    },
    body: JSON.stringify({ path: url.pathname }),
  });
  if (!response.ok) {
    throw new Error("Stream authorization failed. Reconnect your device.");
  }
  const { ticket } = await response.json() as { ticket: string };
  if (!/^mitsuro_stream_[a-f0-9]{64}$/.test(ticket)) {
    throw new Error("Invalid stream ticket.");
  }
  url.searchParams.set("token", ticket);
  return url.toString();
}
