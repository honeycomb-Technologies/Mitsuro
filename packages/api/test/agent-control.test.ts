import { expect, test } from "bun:test";

import { MitsuroClient } from "../src/client.ts";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

test("compatibility Code mutations claim non-forcibly and carry the opaque lease", async () => {
  const requests: Array<{
    path: string;
    method: string;
    headers: Headers;
    body?: Record<string, unknown>;
  }> = [];
  let clientId = "";
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
