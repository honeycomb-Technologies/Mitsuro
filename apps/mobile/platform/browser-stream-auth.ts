/**
 * Browser and WebView WebSocket constructors cannot attach an Authorization
 * header. Keep the existing query-token compatibility at this mobile-only
 * boundary until the server exposes a short-lived stream ticket.
 */
export function withBrowserStreamAuthQuery(
  streamUrl: string,
  accountToken: string | null,
): string {
  const token = accountToken?.trim();
  if (!token) return streamUrl;

  const url = new URL(streamUrl);
  if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new Error("Browser stream authentication requires a WebSocket URL");
  }
  url.searchParams.set("token", token);
  return url.toString();
}
