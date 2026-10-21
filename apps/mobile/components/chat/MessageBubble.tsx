import { memo, useEffect, useMemo, useState } from "react";
import { Image, View, Text, Pressable, StyleSheet } from "react-native";
import {
  Brain,
  ChevronDown,
  ChevronRight,
  Clock,
  Search,
} from "lucide-react-native";
import { useThemeContext } from "../../hooks/useTheme";
import { AssistantSegmentedContent } from "./AssistantSegmentedContent";
import { MarkdownContent } from "./MarkdownContent";
import { ToolCallCard } from "./ToolCallCard";
import { DotEchoIndicator } from "./DotEchoIndicator";
import { ToolApprovalWidget } from "./ToolApprovalWidget";
import { AskUserQuestionWidget } from "./AskUserQuestionWidget";
import { PlanConfirmWidget } from "./PlanConfirmWidget";
import { ImagePreviewModal, imagePreviewUri } from "./ImagePreviewModal";
import {
  assistantVisualSegments,
  isDelegatedTool,
  isPlanConfirmTool,
  isQuestionTool,
  type AssistantVisualSegment,
} from "./assistantRenderPlan";
import {
  presentTool,
  shouldExpandToolByPolicy,
} from "./toolPresentation";
import type { ChatMessage, ChatMessageAttachment, ToolCall } from "@mitsuro/api";
import { beginMitsuroPerformanceSpan, formatCitationSources, getMessageCopyText } from "@mitsuro/state";
import * as Clipboard from "../../platform/clipboard";
import * as Haptics from "../../platform/haptics";
import { createMessageCopyAction, type MessageCopyFeedback } from "./messageCopyAction";

interface MessageBubbleProps {
  message: ChatMessage;
  sessionId?: string | null;
  isLast: boolean;
  isStreaming: boolean;
  isThinking?: boolean;
  activeToolCallId?: string | null;
  onApproveTool?: (toolCallId: string) => void;
  onDenyTool?: (toolCallId: string) => void;
  onSubmitToolResult?: (
    toolCallId: string,
    result: string,
  ) => void | Promise<void>;
  onPlanConfirm?: (
    toolCallId: string,
    choice: "execute" | "abandon",
  ) => void | Promise<void>;
}

export const MessageBubble = memo(function MessageBubble({
  message,
  sessionId,
  isLast,
  isStreaming,
  isThinking,
  activeToolCallId,
  onApproveTool,
  onDenyTool,
  onSubmitToolResult,
  onPlanConfirm,
}: MessageBubbleProps) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const isUser = message.role === "user";
  const [copyFeedback, setCopyFeedback] = useState<MessageCopyFeedback>(null);
  const copyAction = useMemo(
    () => createMessageCopyAction(Clipboard.setStringAsync, setCopyFeedback),
    [sessionId, message.id],
  );
  useEffect(() => {
    setCopyFeedback(null);
    return copyAction.cancel;
  }, [copyAction]);

  const assistantSegments = useMemo(
    () => {
      if (isUser) return [];
      const finishVisualPlanSpan = beginMitsuroPerformanceSpan(
        "transcript.visual_plan",
      );
      try {
        return assistantVisualSegments(message, isLast, isThinking);
      } finally {
        finishVisualPlanSpan();
      }
    },
    [isLast, isThinking, isUser, message],
  );
  const handleCopy = async () => {
    const value = getMessageCopyText(message).trim();
    if (!value) {
      return;
    }

    if (await copyAction.copy(value)) {
      void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light).catch(() => {});
    }
  };

  const renderToolSegment = (toolCall: ToolCall) => {
    const toolIsStreaming =
      isLast && isStreaming && toolCall.status === "running";
    if (isQuestionTool(toolCall) && onSubmitToolResult) {
      return (
        <AskUserQuestionWidget
          key={toolCall.id}
          toolCall={toolCall}
          isSubmitting={activeToolCallId === toolCall.id}
          onSubmit={(result) => onSubmitToolResult(toolCall.id, result)}
        />
      );
    }

    if (isPlanConfirmTool(toolCall) && onPlanConfirm) {
      return (
        <PlanConfirmWidget
          key={toolCall.id}
          toolCall={toolCall}
          isSubmitting={activeToolCallId === toolCall.id}
          onConfirm={(choice) => onPlanConfirm(toolCall.id, choice)}
        />
      );
    }

    if (
      toolCall.status === "awaiting_approval" &&
      onApproveTool &&
      onDenyTool
    ) {
      return (
        <ToolApprovalWidget
          key={toolCall.id}
          toolCall={toolCall}
          isSubmitting={activeToolCallId === toolCall.id}
          onApprove={() => onApproveTool(toolCall.id)}
          onDeny={() => onDenyTool(toolCall.id)}
        />
      );
    }

    return (
      <ToolCallCard
        key={toolCall.id}
        toolCall={toolCall}
        sessionId={sessionId}
        isStreaming={
          toolIsStreaming || (isDelegatedTool(toolCall) && isLast && isStreaming)
        }
        defaultExpanded={shouldExpandToolByPolicy(
          toolCall,
          toolIsStreaming || (isDelegatedTool(toolCall) && isLast && isStreaming),
        )}
      />
    );
  };

  const renderAssistantSegment = (segment: AssistantVisualSegment) => {
    switch (segment.type) {
      case "thinking":
        return (
          <ThinkingBlock
            key={segment.id}
            content={segment.content}
            isStreaming={
              isLast && (isThinking || (isStreaming && !message.content))
            }
          />
        );
      case "exploration":
        return (
          <ToolClusterCard
            key={segment.id}
            tools={segment.tools}
            isStreaming={isLast && isStreaming}
          />
        );
      case "tool":
        return renderToolSegment(segment.toolCall);
      case "attachments":
        return (
          <MessageAttachments
            key={segment.id}
            attachments={message.attachments ?? []}
          />
        );
      case "text":
        return (
          <Pressable
            key={segment.id}
            onLongPress={handleCopy}
            delayLongPress={250}
            style={styles.assistantText}
          >
            <AssistantSegmentedContent
              messageId={`${message.id}-${segment.id}`}
              content={segment.content}
              isStreaming={isLast && isStreaming}
            />
            {segment.citations?.length ? (
              <MarkdownContent content={formatCitationSources(segment.citations)} />
            ) : null}
          </Pressable>
        );
    }
  };

  return (
    <View
      style={[
        styles.container,
        isUser ? styles.containerUser : styles.containerAssistant,
      ]}
    >
      {isUser &&
        (message.content.length > 0 ||
          (message.attachments?.length ?? 0) > 0) && (
          <View
            style={[styles.userWrap, message.isQueued && styles.userQueuedWrap]}
          >
            {message.isQueued && (
              <View style={styles.queuedRow}>
                <Clock size={12} color={t.warning} strokeWidth={2} />
                <Text style={[styles.queuedLabel, { color: t.warning }]}>
                  {message.queuedUntilNextRun ? "Queued for next run" : "Queued"}
                </Text>
              </View>
            )}
            {(message.attachments?.length ?? 0) > 0 ? (
              <MessageAttachments
                attachments={message.attachments ?? []}
                isUser
              />
            ) : null}
            {message.content.length > 0 ? (
              <Pressable
                onLongPress={handleCopy}
                delayLongPress={250}
                style={[
                  styles.userMessage,
                  {
                    borderColor: t.userMessage,
                    backgroundColor: t.background,
                  },
                ]}
              >
                <MarkdownContent content={message.content} isUser />
              </Pressable>
            ) : null}
            {copyFeedback ? (
              <Text style={[styles.copyStatus, { color: t.mutedForeground }]}>
                {copyFeedback}
              </Text>
            ) : null}
          </View>
        )}

      {!isUser && (
        <View style={styles.assistantWrap}>
          {assistantSegments.map(renderAssistantSegment)}

          {copyFeedback ? (
            <Text style={[styles.copyStatus, { color: t.mutedForeground }]}>
              {copyFeedback}
            </Text>
          ) : null}
        </View>
      )}
    </View>
  );
}, (prev, next) => (
  prev.sessionId === next.sessionId &&
  prev.isLast === next.isLast &&
  prev.isStreaming === next.isStreaming &&
  prev.isThinking === next.isThinking &&
  prev.activeToolCallId === next.activeToolCallId &&
  prev.onApproveTool === next.onApproveTool &&
  prev.onDenyTool === next.onDenyTool &&
  prev.onSubmitToolResult === next.onSubmitToolResult &&
  prev.onPlanConfirm === next.onPlanConfirm &&
  (
    prev.message.role === 'assistant' && next.message.role === 'assistant'
      ? prev.message.id === next.message.id &&
        prev.message.content === next.message.content &&
        prev.message.thinking === next.message.thinking &&
        prev.message.kind === next.message.kind &&
        prev.message.toolCalls === next.message.toolCalls &&
        prev.message.renderParts === next.message.renderParts &&
        prev.message.attachments === next.message.attachments
      : prev.message.id === next.message.id &&
        prev.message.content === next.message.content &&
        prev.message.isQueued === next.message.isQueued &&
        prev.message.attachments === next.message.attachments
  )
));

function ToolClusterCard({
  tools,
  isStreaming,
}: {
  tools: ToolCall[];
  isStreaming: boolean;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const [expanded, setExpanded] = useState(false);
  const runningCount = tools.filter((tool) => tool.status === "running").length;
  const errorCount = tools.filter((tool) => tool.status === "error").length;
  const label =
    tools.length === 1
      ? presentTool(tools[0]!).label
      : `${tools.length} exploration actions`;
  const detail = [
    runningCount > 0 ? `${runningCount} running` : null,
    errorCount > 0 ? `${errorCount} failed` : null,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <View style={styles.toolCluster}>
      <Pressable
        onPress={() => setExpanded((current) => !current)}
        style={styles.toolClusterHeader}
      >
        <Search size={14} color={t.mutedForeground} strokeWidth={1.6} />
        <Text style={[styles.toolClusterTitle, { color: t.mutedForeground }]}>
          {label}
        </Text>
        {detail ? (
          <Text
            style={[styles.toolClusterDetail, { color: t.mutedForeground }]}
          >
            {detail}
          </Text>
        ) : null}
        {expanded ? (
          <ChevronDown size={14} color={t.mutedForeground} />
        ) : (
          <ChevronRight size={14} color={t.mutedForeground} />
        )}
      </Pressable>
      {expanded ? (
        <View style={styles.toolClusterBody}>
          {tools.map((toolCall) => {
            const toolStreaming = isStreaming && toolCall.status === "running";
            return (
              <ToolCallCard
                key={toolCall.id}
                toolCall={toolCall}
                isStreaming={toolStreaming}
                defaultExpanded={shouldExpandToolByPolicy(
                  toolCall,
                  toolStreaming,
                )}
                compact
              />
            );
          })}
        </View>
      ) : null}
    </View>
  );
}

function MessageAttachments({
  attachments,
  isUser,
}: {
  attachments: ChatMessageAttachment[];
  isUser?: boolean;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const [previewAttachment, setPreviewAttachment] =
    useState<ChatMessageAttachment | null>(null);
  const [hoveredAttachmentIndex, setHoveredAttachmentIndex] =
    useState<number | null>(null);
  const previewUri = imagePreviewUri(previewAttachment);

  return (
    <View
      style={[styles.attachmentStrip, isUser && styles.attachmentStripUser]}
    >
      {attachments.map((attachment, index) => {
        const uri = imagePreviewUri(attachment);
        if (attachment.type === "image" && uri) {
          return (
            <Pressable
              key={`${attachment.name ?? "image"}-${index}`}
              onPress={(event) => {
                event.stopPropagation();
                void Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
                setPreviewAttachment(attachment);
              }}
              onHoverIn={() => setHoveredAttachmentIndex(index)}
              onHoverOut={() =>
                setHoveredAttachmentIndex((current) =>
                  current === index ? null : current,
                )
              }
              style={({ pressed }) => [
                styles.messageImageThumb,
                {
                  backgroundColor: t.glass.background,
                  borderColor:
                    hoveredAttachmentIndex === index || pressed
                      ? t.userMessage
                      : t.border,
                  opacity: pressed ? 0.86 : 1,
                },
              ]}
            >
              <Image
                source={{ uri }}
                style={styles.messageImage}
                resizeMode="cover"
                accessibilityLabel={attachment.name ?? "Image attachment"}
              />
            </Pressable>
          );
        }

        return (
          <View
            key={`${attachment.name ?? "file"}-${index}`}
            style={[
              styles.messageFileChip,
              { borderColor: t.border, backgroundColor: t.card },
            ]}
          >
            <Text
              style={[styles.messageFileName, { color: t.mutedForeground }]}
              numberOfLines={1}
            >
              {attachment.name ?? "Attached file"}
            </Text>
          </View>
        );
      })}
      {previewAttachment ? (
        <ImagePreviewModal
          visible
          uri={previewUri}
          title={previewAttachment.name}
          onClose={() => setPreviewAttachment(null)}
        />
      ) : null}
    </View>
  );
}

function ThinkingBlock({
  content,
  isStreaming,
}: {
  content: string;
  isStreaming: boolean;
}) {
  const { theme } = useThemeContext();
  const t = theme.colors;
  const [expanded, setExpanded] = useState(false);
  const canExpand = content.trim().length > 0;

  return (
    <Pressable
      onPress={() => canExpand && setExpanded(!expanded)}
      disabled={!canExpand}
      style={styles.thinkingBlock}
    >
      <View style={styles.thinkingHeader}>
        {isStreaming ? (
          <DotEchoIndicator color={t.thinking} />
        ) : (
          <Brain size={14} color={t.mutedForeground} strokeWidth={1.8} />
        )}
        <Text
          style={[
            styles.thinkingLabel,
            { color: isStreaming ? t.foreground : t.mutedForeground },
          ]}
        >
          {isStreaming ? "Thinking…" : "Thought"}
        </Text>
        {canExpand ? (
          expanded ? (
            <ChevronDown size={14} color={t.mutedForeground} />
          ) : (
            <ChevronRight size={14} color={t.mutedForeground} />
          )
        ) : null}
      </View>
      {expanded && (
        <Text
          style={[styles.thinkingContent, { color: t.mutedForeground }]}
          selectable
        >
          {content}
        </Text>
      )}
    </Pressable>
  );
}

const styles = StyleSheet.create({
  container: {
    marginTop: 4,
    marginBottom: 10,
  },
  containerUser: {
    alignItems: "flex-end",
  },
  containerAssistant: {
    width: "100%",
  },
  assistantWrap: {
    width: "100%",
    maxWidth: "100%",
    gap: 10,
  },
  userWrap: {
    maxWidth: "88%",
    gap: 6,
  },
  userQueuedWrap: {
    alignItems: "flex-end",
  },
  queuedRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 5,
    justifyContent: "flex-end",
  },
  queuedLabel: {
    fontSize: 11,
    fontWeight: "600",
  },
  userMessage: {
    alignSelf: "flex-end",
    maxWidth: "100%",
    paddingVertical: 6,
    paddingHorizontal: 10,
    borderWidth: 1,
    borderRadius: 12,
  },
  attachmentStrip: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    alignItems: "center",
  },
  attachmentStripUser: {
    alignSelf: "flex-end",
    justifyContent: "flex-end",
  },
  messageImageThumb: {
    width: 132,
    height: 96,
    borderRadius: 14,
    overflow: "hidden",
    borderWidth: StyleSheet.hairlineWidth,
  },
  messageImage: {
    width: "100%",
    height: "100%",
  },
  messageFileChip: {
    maxWidth: 180,
    borderRadius: 12,
    borderWidth: StyleSheet.hairlineWidth,
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  messageFileName: {
    fontSize: 12,
    fontWeight: "600",
  },
  assistantText: {
    paddingLeft: 2,
    paddingVertical: 2,
  },
  copyStatus: {
    fontSize: 12,
    fontWeight: "500",
    marginTop: -2,
    marginLeft: 2,
  },
  toolCluster: {
    paddingVertical: 2,
  },
  toolClusterHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  toolClusterTitle: {
    flex: 1,
    fontSize: 12,
    fontWeight: "600",
  },
  toolClusterDetail: {
    fontSize: 11,
    fontWeight: "500",
  },
  toolClusterBody: {
    marginTop: 6,
    gap: 4,
  },
  thinkingBlock: {
    paddingVertical: 3,
  },
  thinkingHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  thinkingLabel: {
    fontSize: 13,
    fontWeight: "600",
    flex: 1,
  },
  thinkingContent: {
    fontSize: 13,
    lineHeight: 19,
    marginTop: 6,
    fontFamily: "Courier",
  },
});
