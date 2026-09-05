use serde_json::Value;

use crate::ai::types::Content;

pub(super) fn convert_content(
    content: &Content,
    include_thinking: bool,
    include_signature: bool,
    strip_images: bool,
) -> Option<Value> {
    match content {
        Content::Text { text, citations } => Some(serde_json::json!({
            "type": "text",
            "text": crate::ai::format::provider_text(text, citations)
        })),
        Content::ToolUse { id, name, input } => Some(serde_json::json!({
            "type": "tool_use",
            "id": id,
            "name": name,
            "input": input
        })),
        Content::ToolResult {
            tool_use_id,
            output,
            is_error,
        } => {
            let content_str = match output {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            Some(serde_json::json!({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": content_str,
                "is_error": is_error.unwrap_or(false)
            }))
        }
        Content::Image { image, detail: _ } => Some(image_content(
            image.base64.as_deref(),
            image.url.as_deref(),
            image.media_type.as_deref(),
            strip_images,
        )),
        Content::Document { source } => Some(document_content(
            source.data.as_deref(),
            source.url.as_deref(),
            Some(source.media_type.as_str()),
            strip_images,
        )),
        Content::Thinking {
            thinking,
            signature,
        } => thinking_content(thinking, signature, include_thinking, include_signature),
        Content::RedactedThinking { data } => {
            if include_thinking {
                Some(serde_json::json!({
                    "type": "redacted_thinking",
                    "data": data
                }))
            } else {
                None
            }
        }
    }
}

fn image_content(
    base64: Option<&str>,
    url: Option<&str>,
    media_type: Option<&str>,
    strip_images: bool,
) -> Value {
    if strip_images {
        return serde_json::json!({
            "type": "text",
            "text": "[Image attached - use MCP vision tools to analyze]"
        });
    }

    if let Some(base64_data) = base64 {
        serde_json::json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type.unwrap_or("image/png"),
                "data": base64_data
            }
        })
    } else if let Some(url) = url {
        serde_json::json!({
            "type": "image",
            "source": {
                "type": "url",
                "url": url
            }
        })
    } else {
        serde_json::json!({
            "type": "text",
            "text": "[Invalid image content]"
        })
    }
}

fn document_content(
    data: Option<&str>,
    url: Option<&str>,
    media_type: Option<&str>,
    strip_images: bool,
) -> Value {
    if strip_images {
        return serde_json::json!({
            "type": "text",
            "text": "[Document attached - use MCP vision tools to analyze]"
        });
    }

    if let Some(data) = data {
        serde_json::json!({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": data
            }
        })
    } else if let Some(url) = url {
        serde_json::json!({
            "type": "document",
            "source": {
                "type": "url",
                "url": url
            }
        })
    } else {
        serde_json::json!({
            "type": "text",
            "text": "[Invalid document content]"
        })
    }
}

fn thinking_content(
    thinking: &str,
    signature: &str,
    include_thinking: bool,
    include_signature: bool,
) -> Option<Value> {
    if !include_thinking || (include_signature && signature.is_empty()) {
        return None;
    }

    if include_signature {
        Some(serde_json::json!({
            "type": "thinking",
            "thinking": thinking,
            "signature": signature
        }))
    } else {
        Some(serde_json::json!({
            "type": "thinking",
            "thinking": thinking
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::thinking_content;

    #[test]
    fn signed_anthropic_thinking_rejects_unsigned_cross_provider_history() {
        assert!(thinking_content("reasoning", "", true, true).is_none());

        let signed = thinking_content("reasoning", "signature", true, true)
            .expect("signed Anthropic thinking should be retained");
        assert_eq!(signed["signature"], "signature");
    }

    #[test]
    fn signature_free_provider_thinking_remains_supported() {
        let thinking = thinking_content("reasoning", "", true, false)
            .expect("signature-free provider thinking should be retained");
        assert_eq!(thinking["thinking"], "reasoning");
        assert!(thinking.get("signature").is_none());
    }
}
