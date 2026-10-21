import { withBrowserStreamAuthQuery } from "../platform/browser-stream-auth.ts";

declare const Deno: {
  test(name: string, fn: () => void | Promise<void>): void;
  readTextFile(path: URL): Promise<string>;
};

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

Deno.test("Atlas keeps Browser control leases ephemeral and observer-safe", async () => {
  const source = await Deno.readTextFile(
    new URL("../components/toolbox/AtlasMobileSurface.tsx", import.meta.url),
  );

  assert(
    source.includes("useRef(new Map<string, BrowserControlLease>())") &&
      source.includes("takeBrowserControl(session.id, browserClientId)") &&
      source.includes("setInterval(() => void heartbeat(), 5_000)"),
    "Browser authority must remain in a mounted in-memory map and be acquired explicitly",
  );
  assert(
    source.includes(
      "'controller',\n          browserClientId,\n          selectedLease",
    ) &&
      source.includes("runBrowserActions(selected.id, selectedLease"),
    "controller heartbeats and actions must carry the acquired lease",
  );
  assert(
    source.includes("'viewer'") &&
      source.includes("Take control") &&
      source.includes("const mutate=(value)=>interactive&&send(value)"),
    "a client without authority must render an honest observer stream with an explicit takeover",
  );
  assert(
    source.includes("withBrowserStreamAuthQuery(url, serverToken)") &&
      !source.includes("console.log(serverToken)") &&
      !source.includes("JSON.stringify(serverToken)"),
    "Atlas must keep its browser-only WebSocket auth compatibility boundary explicit and private",
  );
});

Deno.test("Atlas stream auth preserves lease parameters and adds only its account token", () => {
  const controller = withBrowserStreamAuthQuery(
    "wss://mitsuro.test/api/browser/browser-1/stream?capability=controller&client_id=mobile-a&lease_token=opaque-lease",
    "  private-account-bearer  ",
  );
  const url = new URL(controller);
  assert(
    url.searchParams.get("capability") === "controller",
    "capability was lost",
  );
  assert(
    url.searchParams.get("client_id") === "mobile-a",
    "client identity was lost",
  );
  assert(
    url.searchParams.get("lease_token") === "opaque-lease",
    "lease was lost",
  );
  assert(
    url.searchParams.get("token") === "private-account-bearer",
    "account auth was not added",
  );
  assert(
    withBrowserStreamAuthQuery(
      "ws://127.0.0.1/api/browser/browser-1/stream",
      null,
    ) ===
      "ws://127.0.0.1/api/browser/browser-1/stream",
    "an unauthenticated local stream should remain unchanged",
  );
});
