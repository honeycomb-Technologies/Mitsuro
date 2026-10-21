import type { ChatMessage } from "./types";
import {
  type Citation,
  citationWebUrl,
  mergeCitations,
  uniqueCitationSources,
} from "@mitsuro/api";

/**
 * Full ordered prose for copy/export. `content` is a bounded convenience field;
 * render parts retain the complete answer and the provider's original spacing.
 * Thinking and tool output are separate from the assistant's prose.
 */
export function getMessageText(
  message: Pick<ChatMessage, "content" | "renderParts">,
): string {
  if (!message.renderParts?.length) return message.content;
  return message.renderParts
    .filter((part) => part.type === "text")
    .map((part) => part.content)
    .join("");
}

export function getMessageCitations(
  message: Pick<ChatMessage, "renderParts">,
): Citation[] {
  let citations: Citation[] | undefined;
  for (const part of message.renderParts ?? []) {
    if (part.type === "text") {
      citations = mergeCitations(citations, part.citations);
    }
  }
  return citations ?? [];
}

/** Shared by visible sources and copy/export so attribution cannot drift. */
export function formatCitationSources(citations: Citation[]): string {
  const sources = uniqueCitationSources(citations);
  if (!sources.length) return "";
  const lines = sources.map((citation) => {
    const url = citationWebUrl(citation);
    const label = (citation.title || citation.url)
      .replace(/\s+/g, " ")
      .replace(/[\\`*_{}\[\]()#+\-.!>|~]/g, "\\$&");
    return url ? `- [${label}](<${url.replace(/>/g, "%3E")}>)` : `- ${label}`;
  });
  return `Sources:\n\n${lines.join("\n")}`;
}

export function getMessageCopyText(
  message: Pick<ChatMessage, "content" | "renderParts">,
): string {
  const prose = getMessageText(message);
  const sources = formatCitationSources(getMessageCitations(message));
  return sources ? `${prose}${prose ? "\n\n" : ""}${sources}` : prose;
}
