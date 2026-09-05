import type { Citation } from "./types";

/** Normalize untrusted live and stored annotations without losing valid prose. */
export function normalizeCitations(value: unknown): Citation[] {
  if (!Array.isArray(value)) return [];
  const citations: Citation[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== "object") continue;
    const url = typeof entry.url === "string" ? entry.url : "";
    const title = typeof entry.title === "string" ? entry.title : "";
    if (!url && !title) continue;
    citations.push({
      url,
      title,
      cited_text: typeof entry.cited_text === "string" ? entry.cited_text : "",
    });
  }
  return mergeCitations(undefined, citations) ?? [];
}

/** Preserve annotation order and distinct quoted passages from the same source. */
export function mergeCitations(
  existing?: Citation[],
  incoming?: Citation[],
): Citation[] | undefined {
  if (!incoming?.length) return existing;
  const merged = [...(existing ?? [])];
  const key = (citation: Citation) =>
    JSON.stringify([citation.url, citation.title, citation.cited_text]);
  const seen = new Set(merged.map(key));
  for (const citation of incoming) {
    const identity = key(citation);
    if (seen.has(identity)) continue;
    seen.add(identity);
    merged.push(citation);
  }
  return merged;
}

/** Source URLs can open web pages, never execute provider-supplied URI schemes. */
export function citationWebUrl(citation: Citation): string | null {
  try {
    const url = new URL(citation.url);
    return url.protocol === "https:" || url.protocol === "http:"
      ? url.href
      : null;
  } catch {
    return null;
  }
}

/** A source list needs one entry per source, while text parts keep every quote. */
export function uniqueCitationSources(citations: Citation[]): Citation[] {
  const seen = new Set<string>();
  return citations.filter((citation) => {
    const key = JSON.stringify([citation.url, citation.title]);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
