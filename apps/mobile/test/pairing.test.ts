import {
  authorizeDeviceStream,
  pairingLink,
  pairingRequest,
  parsePairingLink,
} from "../../../packages/api/src/pairing.ts";
declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
};
function assert(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}
Deno.test("QR round trip accepts only versioned invitation secrets and safe origins", () => {
  const invite = {
    id: "id",
    origin: "https://server.example:8443",
    secret: `mitsuro_pair_${"a".repeat(64)}`,
    expires_at: 100,
  };
  const link = pairingLink(invite);
  assert(parsePairingLink(link).origin === invite.origin, "origin preserved");
  for (
    const value of [
      link.replace("v=1", "v=2"),
      link + "&v=1",
      link.replace("mitsuro_pair_", "mitsuro_remote_"),
      link.replace("https%3A", "http%3A"),
      "https://attacker.example/" + link,
      link.replace("server.example", "user%40server.example"),
    ]
  ) {
    let rejected = false;
    try {
      parsePairingLink(value);
    } catch {
      rejected = true;
    }
    assert(rejected, "unsafe QR must be rejected");
  }
});
Deno.test("claim isolates credentials and stream ticket keeps reusable token out of URL", async () => {
  const original = globalThis.fetch;
  const calls: { url: string; options: RequestInit }[] = [];
  globalThis.fetch =
    (async (input: RequestInfo | URL, options: RequestInit = {}) => {
      calls.push({ url: String(input), options });
      return new Response(
        JSON.stringify({ ticket: `mitsuro_stream_${"b".repeat(64)}` }),
        { status: 200 },
      );
    }) as typeof fetch;
  try {
    await pairingRequest(
      "https://server.example",
      "claim",
      new AbortController().signal,
      { secret: `mitsuro_pair_${"a".repeat(64)}`, name: "Phone" },
    );
    assert(
      calls[0].options.credentials === "omit" &&
        calls[0].options.redirect === "error",
      "no ambient cookies or redirects",
    );
    assert(
      !new Headers(calls[0].options.headers).has("Authorization"),
      "claim never receives owner auth",
    );
    const url = await authorizeDeviceStream(
      "wss://server.example/ws/terminal",
      "mitsuro_device_secret",
    );
    assert(
      !url.includes("mitsuro_device_") && url.includes("mitsuro_stream_"),
      "URL contains only ticket",
    );
    assert(
      new Headers(calls[1].options.headers).get("Authorization") ===
        "Bearer mitsuro_device_secret",
      "ticket requested through header",
    );
  } finally {
    globalThis.fetch = original;
  }
});

Deno.test("paired client refuses redirects and omits ambient credentials on normal API requests", async () => {
  const { MitsuroClient } = await import("../../../packages/api/src/client.ts");
  const calls: RequestInit[] = [];
  const client = new MitsuroClient({
    baseUrl: "https://server.example",
    token: `mitsuro_device_${"c".repeat(64)}`,
    fetchImpl:
      (async (_input: RequestInfo | URL, options: RequestInit = {}) => {
        calls.push(options);
        return new Response("{}", { status: 200 });
      }) as typeof fetch,
  });
  await client.checkHealth();
  await client.listPairedDevices();
  assert(
    calls.length === 3 &&
      calls.every((call) =>
        call.redirect === "error" && call.credentials === "omit"
      ),
    "device requests must refuse redirects and cookies",
  );
});

Deno.test("public health cannot make a revoked device appear connected", async () => {
  const { MitsuroClient } = await import("../../../packages/api/src/client.ts");
  const client = new MitsuroClient({
    baseUrl: "https://server.example",
    token: "mitsuro_device_revoked",
    fetchImpl: (async (url: RequestInfo | URL) =>
      new Response("{}", {
        status: String(url).endsWith("/health") ? 200 : 401,
      })) as typeof fetch,
  });
  assert(
    !await client.checkHealth(),
    "authenticated device liveness must fail",
  );
});
