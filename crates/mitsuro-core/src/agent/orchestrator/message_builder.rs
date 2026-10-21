use crate::ai::types::{AiToolCall, Citation, Content, ModelMessage, Role};

use super::super::stream::ThinkingBlock;

pub(super) fn build_assistant_message(
    text: &str,
    thinking_blocks: &[ThinkingBlock],
    tool_calls: &[AiToolCall],
    citations: &[Citation],
) -> ModelMessage {
    let mut content = Vec::with_capacity(
        thinking_blocks.len()
            + tool_calls.len()
            + usize::from(!text.is_empty() || !citations.is_empty()),
    );

    for block in thinking_blocks {
        if block.thinking.is_empty() && block.signature.is_empty() {
            continue;
        }
        content.push(Content::Thinking {
            thinking: block.thinking.clone(),
            signature: block.signature.clone(),
        });
    }

    if !text.is_empty() || !citations.is_empty() {
        content.push(Content::Text {
            text: text.to_string(),
            citations: citations.to_vec(),
        });
    }

    for call in tool_calls {
        content.push(Content::ToolUse {
            id: call.id.clone(),
            name: call.name.clone(),
            input: call.arguments.clone(),
        });
    }

    ModelMessage {
        role: Role::Assistant,
        content,
    }
}

pub(super) fn finalize_explore_only_turn(
    tool_calls: &[AiToolCall],
    tool_results: &[Content],
) -> Option<String> {
    if tool_calls.len() != 1 || tool_calls.first()?.name != "explore" {
        return None;
    }

    let Content::ToolResult {
        tool_use_id,
        output,
        is_error,
    } = tool_results.first()?
    else {
        return None;
    };

    if tool_use_id != &tool_calls.first()?.id || is_error.unwrap_or(false) {
        return None;
    }

    let parsed = match output {
        serde_json::Value::String(text) => serde_json::from_str::<serde_json::Value>(text).ok()?,
        other => other.clone(),
    };
    let payload = parsed.get("result").unwrap_or(&parsed);
    let outcome = payload
        .get("outcome")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let usable_agents = payload
        .get("usable_agents")
        .or_else(|| payload.get("successful_agents"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    if !matches!(outcome, "success" | "partial") || usable_agents == 0 {
        return None;
    }

    let message = payload
        .get("message")
        .and_then(|value| value.as_str())
        .unwrap_or("Explore completed.");
    if let Some(human_review) = payload
        .get("human_review")
        .and_then(|value| value.as_str())
        .filter(|review| !review.trim().is_empty())
    {
        return Some(human_review.to_string());
    }
    payload
        .get("investigation_summary")
        .and_then(|value| value.as_str())
        .filter(|summary| !summary.trim().is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            Some(message)
                .filter(|message| !message.trim().is_empty())
                .map(ToString::to_string)
        })
}

#[cfg(test)]
mod tests {
    use super::build_assistant_message;
    use crate::agent::stream::ThinkingBlock;
    use crate::ai::types::Content;

    #[test]
    fn assistant_message_skips_only_empty_unsigned_thinking() {
        let message = build_assistant_message(
            "",
            &[
                ThinkingBlock {
                    thinking: String::new(),
                    signature: String::new(),
                },
                ThinkingBlock {
                    thinking: "visible reasoning".to_string(),
                    signature: String::new(),
                },
                ThinkingBlock {
                    thinking: String::new(),
                    signature: "opaque-signature".to_string(),
                },
            ],
            &[],
            &[],
        );

        assert_eq!(message.content.len(), 2);
        assert!(matches!(
            &message.content[0],
            Content::Thinking { thinking, signature }
                if thinking == "visible reasoning" && signature.is_empty()
        ));
        assert!(matches!(
            &message.content[1],
            Content::Thinking { thinking, signature }
                if thinking.is_empty() && signature == "opaque-signature"
        ));
    }
}
