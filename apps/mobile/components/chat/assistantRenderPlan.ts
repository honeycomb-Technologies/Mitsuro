import type { ChatMessage, ChatRenderPart, ToolCall } from "@mitsuro/api";
import {
  isExplorationToolName,
  isHiddenToolName,
} from "./toolPresentation";
import {
  smoothInterruptedText,
  type AssistantVisualSegment,
} from "./assistantTextSmoothing";

export {
  appendContinuationText,
  shouldMergeContinuationText,
  smoothInterruptedText,
  startsLikeNewBlock,
} from "./assistantTextSmoothing";
export type { AssistantVisualSegment } from "./assistantTextSmoothing";

/** Keep in sync with plan/mode tools filtered from UI in core tool policy. */
const INTERNAL_TOOL_NAMES = new Set([
  "enter_plan_mode",
  "set_work_mode",
  "set_workspace_context",
  "task_start",
  "task_complete",
  "add_subtask",
  "set_dependency",
]);

export function isDelegatedTool(toolCall: ToolCall): boolean {
  return (
    toolCall.name === "agent" ||
    toolCall.name === "explore" ||
    toolCall.name === "plan" ||
    toolCall.name === "verify" ||
    toolCall.name === "build"
  );
}

export function isQuestionTool(toolCall: ToolCall): boolean {
  return toolCall.name === "AskUserQuestion";
}

export function isPlanConfirmTool(toolCall: ToolCall): boolean {
  return toolCall.name === "PlanConfirm";
}

function isInternalTool(toolCall: ToolCall): boolean {
  return INTERNAL_TOOL_NAMES.has(toolCall.name) || isHiddenToolName(toolCall.name);
}

export function isExplorationTool(toolCall: ToolCall): boolean {
  return isExplorationToolName(toolCall.name);
}

function legacyRenderParts(
  message: ChatMessage,
  isLast: boolean,
  isThinking?: boolean,
): ChatRenderPart[] {
  const parts: ChatRenderPart[] = [];

  if (message.thinking || (isLast && isThinking)) {
    parts.push({
      type: "thinking",
      id: "legacy-thinking",
      content: message.thinking ?? "",
    });
  }

  for (const toolCall of message.toolCalls ?? []) {
    parts.push({
      type: "tool",
      id: `legacy-tool-${toolCall.id}`,
      toolCallId: toolCall.id,
    });
  }

  if ((message.attachments?.length ?? 0) > 0) {
    parts.push({ type: "attachments", id: "legacy-attachments" });
  }

  if (message.content.length > 0) {
    parts.push({
      type: "text",
      id: "legacy-text",
      content: message.content,
    });
  }

  return parts;
}

export function assistantVisualSegments(
  message: ChatMessage,
  isLast: boolean,
  isThinking?: boolean,
): AssistantVisualSegment[] {
  const toolById = new Map(
    (message.toolCalls ?? []).map((toolCall) => [toolCall.id, toolCall]),
  );
  const renderParts =
    message.renderParts && message.renderParts.length > 0
      ? message.renderParts
      : legacyRenderParts(message, isLast, isThinking);
  // Sticky tool slots: one visual slot per recorded tool/thinking part.
  // Only rejoin prose fragments that were split mid-sentence by tools.
  const segments: AssistantVisualSegment[] = [];

  for (const part of renderParts) {
    switch (part.type) {
      case "thinking": {
        if (!part.content && !(isLast && isThinking)) break;
        segments.push({
          type: "thinking",
          id: part.id,
          content: part.content,
        });
        break;
      }
      case "tool": {
        const toolCall = toolById.get(part.toolCallId);
        if (!toolCall || isInternalTool(toolCall)) break;
        segments.push({
          type: "tool",
          id: part.id,
          toolCall,
        });
        break;
      }
      case "attachments": {
        if ((message.attachments?.length ?? 0) === 0) break;
        segments.push({
          type: "attachments",
          id: part.id,
        });
        break;
      }
      case "text": {
        if (part.content.length === 0 && !part.citations?.length) break;
        segments.push({
          type: "text",
          id: part.id,
          content: part.content,
          ...(part.citations?.length ? { citations: part.citations } : {}),
        });
        break;
      }
    }
  }

  if (
    segments.length === 0 &&
    (message.thinking || (isLast && isThinking))
  ) {
    segments.push({
      type: "thinking",
      id: "fallback-thinking",
      content: message.thinking ?? "",
    });
  }

  // Rejoin "we only" + tool + "skimmed." into one prose block, keeping the tool
  // after the completed sentence instead of splitting words across it.
  return smoothInterruptedText(segments);
}
