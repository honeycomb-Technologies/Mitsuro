import { expect, test } from "bun:test";

import { MitsuroApiError, MitsuroClient } from "../src/client.ts";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

test("Code mutations re-probe an upgraded server and claim a non-forced lease", async () => {
  const requests: Array<{
    path: string;
    method: string;
    headers: Headers;
    body?: Record<string, unknown>;
  }> = [];
  let clientId = "";
  let supportsControl = false;
  const fetchImpl: typeof fetch = async (input, init) => {
    const path = new URL(String(input)).pathname;
    const method = init?.method ?? "GET";
    const body =
      typeof init?.body === "string"
        ? (JSON.parse(init.body) as Record<string, unknown>)
        : undefined;
    const headers = new Headers(init?.headers);
    requests.push({ path, method, headers, body });

    if (path === "/api/sessions/code-1/control" && method === "GET") {
      if (!supportsControl) return json({ error: "Not found" }, 404);
      return json({ session_id: "code-1", controller: null, ttl_seconds: 15 });
    }
    if (path === "/api/sessions/code-1/control" && method === "POST") {
      clientId = String(body?.client_id ?? "");
      return json({
        lease: {
          resource_id: "code-1",
          client_id: clientId,
          lease_token: "opaque-secret",
          expires_at: "later",
        },
      });
    }
    if (path === "/api/chat/steer" && method === "POST") {
      return json({ status: "accepted", pending_id: "pending-1" });
    }
    return json({ error: "unexpected request" }, 500);
  };

  const client = new MitsuroClient({
    baseUrl: "https://example.test",
    fetchImpl,
  });
  await client.steerSession({ session_id: "code-1", message: "before upgrade" });
  expect(requests.map(({ path, method }) => `${method} ${path}`)).toEqual([
    "GET /api/sessions/code-1/control",
    "POST /api/chat/steer",
  ]);
  expect(requests[1]?.headers.get("X-Mitsuro-Control-Lease")).toBeNull();
  supportsControl = true;
  requests.length = 0;
  await client.steerSession({
    session_id: "code-1",
    message: "continue",
  });

  expect(requests.map(({ path, method }) => `${method} ${path}`)).toEqual([
    "GET /api/sessions/code-1/control",
    "POST /api/sessions/code-1/control",
    "POST /api/chat/steer",
  ]);
  expect(clientId.startsWith("mitsuro-api-")).toBe(true);
  expect(requests[1]?.body?.force).toBe(false);
  expect(
    requests[2]?.headers.get("X-Mitsuro-Control-Lease"),
  ).toBe("opaque-secret");
});

test("old SPA control fallbacks send the mutation exactly once without a lease", async () => {
  const fallbacks = [
    new Response("<!DOCTYPE html><html><body>Mitsuro</body></html>", {
      headers: { "content-type": "text/html; charset=utf-8" },
    }),
    new Response("Mitsuro API server running. Web frontend not embedded in this build.", {
      headers: { "content-type": "text/plain" },
    }),
  ];
  for (const fallback of fallbacks) {
    const calls: string[] = [];
    const client = new MitsuroClient({
      baseUrl: "https://example.test",
      fetchImpl: async (input, init) => {
        const path = new URL(String(input)).pathname;
        calls.push(`${init?.method ?? "GET"} ${path}`);
        if (path.endsWith("/control")) return fallback;
        expect(new Headers(init?.headers).get("X-Mitsuro-Control-Lease")).toBeNull();
        return json({ ok: true });
      },
    });
    await client.cancelSession("code-1");
    expect(calls).toEqual([
      "GET /api/sessions/code-1/control",
      "POST /api/sessions/code-1/cancel",
    ]);
  }
});

test("control auth, server, network and malformed responses never send mutations", async () => {
  const networkError = new TypeError("Network unavailable");
  const redirectedHtml = new Response("<!DOCTYPE html><html>Sign in</html>", {
    headers: { "content-type": "text/html" },
  });
  Object.defineProperty(redirectedHtml, "redirected", { value: true });
  const redirectedMissing = json({ error: "Login route missing" }, 404);
  Object.defineProperty(redirectedMissing, "redirected", { value: true });
  const failures = [
    ...[401, 403, 503].map((status) => ({
      response: json({ error: "Rejected" }, status), error: MitsuroApiError,
    })),
    { response: new Response("invalid JSON", { headers: { "content-type": "application/json" } }), error: SyntaxError },
    { response: json({}), error: MitsuroApiError },
    { response: json({ session_id: "other-session", controller: null, ttl_seconds: 15 }), error: MitsuroApiError },
    { response: json({ session_id: "code-1", controller: {}, ttl_seconds: 15 }), error: MitsuroApiError },
    { response: new Response("Unexpected proxy response", { headers: { "content-type": "text/plain" } }), error: SyntaxError },
    { response: redirectedHtml, error: MitsuroApiError },
    { response: redirectedMissing, error: MitsuroApiError },
    { response: null, error: TypeError },
  ];
  for (const { response, error } of failures) {
    const requests: string[] = [];
    const client = new MitsuroClient({
      baseUrl: "https://example.test",
      fetchImpl: async (input, init) => {
        requests.push(`${init?.method ?? "GET"} ${new URL(String(input)).pathname}`);
        if (!response) throw networkError;
        return response;
      },
    });
    await expect(client.cancelSession("code-1")).rejects.toBeInstanceOf(error);
    expect(requests).toEqual(["GET /api/sessions/code-1/control"]);
  }
});

test("Chat compatibility mutations bypass Code control without a token", async () => {
  const requests: Array<{ path: string; method: string; lease: string | null }> = [];
  const fetchImpl: typeof fetch = async (input, init) => {
    const path = new URL(String(input)).pathname;
    const method = init?.method ?? "GET";
    requests.push({
      path,
      method,
      lease: new Headers(init?.headers).get("X-Mitsuro-Control-Lease"),
    });
    if (path === "/api/sessions/chat-1/control") {
      return json({ error: "Code only" }, 409);
    }
    if (path === "/api/sessions/chat-1/cancel") {
      return json({ ok: true });
    }
    return json({ error: "unexpected request" }, 500);
  };
  const client = new MitsuroClient({
    baseUrl: "https://example.test",
    fetchImpl,
  });

  await client.cancelSession("chat-1");
  expect(requests).toEqual([
    {
      path: "/api/sessions/chat-1/control",
      method: "GET",
      lease: null,
    },
    {
      path: "/api/sessions/chat-1/cancel",
      method: "POST",
      lease: null,
    },
  ]);
});
