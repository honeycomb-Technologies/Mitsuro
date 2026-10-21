import type {
  ChatMessage,
  SessionType,
} from "@mitsuro/api";
import type { ToolCall } from "@mitsuro/state";
export { isModelUsable, normalizeProviderId } from "@mitsuro/state";
export const CHAT_BAR_ZONE = 130;
export const TAB_TYPES: SessionType[] = ["chat", "code", "hive"];

export type WorkspaceMode = "neutral" | "selected" | "created";

export function sessionTypeForTab(index: number): SessionType {
  return TAB_TYPES[index] ?? "code";
}

export function tabForSessionType(type: SessionType): number {
  switch (type) {
    case "chat":
      return 0;
    case "hive":
      return 2;
    default:
      return 1;
  }
}

export function flattenToolCalls(messages: ChatMessage[]): ToolCall[] {
  const toolCalls: ToolCall[] = [];
  for (const message of messages) {
    if (message.toolCalls?.length) {
      toolCalls.push(...message.toolCalls);
    }
  }
  return toolCalls;
}

export function getWorkspaceMode(path: string | null): WorkspaceMode {
  return path ? "selected" : "neutral";
}
