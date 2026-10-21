import type { ChatMessage, ToolCall } from "@mitsuro/api";

/** Keep newest N turns fully rich; older turns are presentation-light. */
export const RICH_RECENT_TURN_COUNT = 3;

/** Cap retained tool output in historical (non-rich) turns. */
export const HISTORICAL_TOOL_OUTPUT_PREVIEW = 800;

function compactThinking(content: string): string {
  return content.length > HISTORICAL_TOOL_OUTPUT_PREVIEW
    ? `${content.slice(0, HISTORICAL_TOOL_OUTPUT_PREVIEW)}\n…[older thinking collapsed]`
    : content;
}

export function isTurnInRichWindow(
  turnIndex: number,
  turnCount: number,
  richRecentCount = RICH_RECENT_TURN_COUNT,
): boolean {
  return turnIndex >= Math.max(0, turnCount - richRecentCount);
}

export function compactHistoricalToolCall(toolCall: ToolCall): ToolCall {
  const output = toolCall.output;
  if (!output || output.length <= HISTORICAL_TOOL_OUTPUT_PREVIEW) {
    return toolCall;
  }
  return {
    ...toolCall,
    output: `${output.slice(0, HISTORICAL_TOOL_OUTPUT_PREVIEW)}
…[older tool output collapsed]`,
    delegated: toolCall.delegated
      ? {
          ...toolCall.delegated,
          thinking: toolCall.delegated.thinking
            ? toolCall.delegated.thinking.slice(0, HISTORICAL_TOOL_OUTPUT_PREVIEW)
            : toolCall.delegated.thinking,
          agents: toolCall.delegated.agents.slice(0, 4),
          filesExamined: toolCall.delegated.filesExamined.slice(0, 8),
          errors: toolCall.delegated.errors.slice(0, 4),
        }
      : undefined,
  };
}

export function compactHistoricalMessage(message: ChatMessage): ChatMessage {
  if (message.role !== "assistant") {
    return {
      ...message,
      // Historical attachments should not keep base64 blobs hot in render path.
      attachments: message.attachments?.map((attachment) => ({
        type: attachment.type,
        name: attachment.name,
        mimeType: attachment.mimeType,
        uri: attachment.uri,
      })),
    };
  }

  return {
    ...message,
    thinking: message.thinking
      ? compactThinking(message.thinking)
      : message.thinking,
    // The renderer prefers ordered parts. Compact that projection too, without
    // mutating canonical history or truncating any part of the final answer.
    renderParts: message.renderParts?.map((part) =>
      part.type === "thinking" && part.content.length > HISTORICAL_TOOL_OUTPUT_PREVIEW
        ? { ...part, content: compactThinking(part.content) }
        : part,
    ),
    toolCalls: message.toolCalls?.map(compactHistoricalToolCall),
    attachments: message.attachments?.map((attachment) => ({
      type: attachment.type,
      name: attachment.name,
      mimeType: attachment.mimeType,
      uri: attachment.uri,
    })),
  };
}
