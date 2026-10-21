//! OpenAI API format handler
//!
//! Handles conversion to OpenAI chat/completions and responses API formats.
//! Includes message alternation validation and model-facing separation of
//! durable thinking blocks from OpenAI-compatible assistant text.

mod messages;
mod request;
pub mod responses_input;

use serde_json::Value;

use super::{FormatHandler, RequestOptions};
use crate::ai::models::ApiFormat;
use crate::ai::providers::ProviderId;
use crate::ai::types::{AiTool, ModelMessage};

/// OpenAI format handler
pub struct OpenAIFormat {
    api_format: ApiFormat,
    endpoint: String,
}

impl OpenAIFormat {
    pub fn new(format: ApiFormat) -> Self {
        let endpoint = match format {
            ApiFormat::OpenAIResponses => "/v1/responses".to_string(),
            _ => "/v1/chat/completions".to_string(),
        };
        Self {
            api_format: format,
            endpoint,
        }
    }

    pub(super) fn is_responses_format(&self) -> bool {
        matches!(self.api_format, ApiFormat::OpenAIResponses)
    }
}

impl FormatHandler for OpenAIFormat {
    fn convert_messages(
        &self,
        messages: &[ModelMessage],
        provider_id: Option<ProviderId>,
    ) -> Vec<Value> {
        self.convert_messages_impl(messages, provider_id)
    }

    fn convert_tools(&self, tools: &[AiTool]) -> Vec<Value> {
        self.convert_tools_impl(tools)
    }

    fn build_request_body(
        &self,
        model: &str,
        messages: Vec<Value>,
        options: &RequestOptions,
    ) -> Value {
        self.build_request_body_impl(model, messages, options)
    }

    fn endpoint_path(&self, _model: &str) -> &str {
        &self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::types::{Content, ImageContent, Role};

    #[test]
    fn convert_messages_openai_chat_preserves_user_image_parts() {
        let format = OpenAIFormat::new(ApiFormat::OpenAI);
        let messages = vec![ModelMessage {
            role: Role::User,
            content: vec![
                Content::text("Describe this".to_string()),
                Content::Image {
                    image: ImageContent {
                        url: None,
                        base64: Some("AAA".to_string()),
                        media_type: Some("image/jpeg".to_string()),
                    },
                    detail: Some("high".to_string()),
                },
            ],
        }];

        let converted = format.convert_messages(&messages, None);
        assert_eq!(converted.len(), 1);
        let content = converted[0]
            .get("content")
            .and_then(|c| c.as_array())
            .expect("content should be a multimodal array");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "Describe this");
        assert_eq!(content[1]["type"], "image_url");
        assert_eq!(content[1]["image_url"]["url"], "data:image/jpeg;base64,AAA");
        assert_eq!(content[1]["image_url"]["detail"], "high");
    }

    #[test]
    fn convert_messages_openai_responses_uses_input_image() {
        let format = OpenAIFormat::new(ApiFormat::OpenAIResponses);
        let messages = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::Image {
                image: ImageContent {
                    url: Some("https://example.com/cat.png".to_string()),
                    base64: None,
                    media_type: None,
                },
                detail: Some("low".to_string()),
            }],
        }];

        let converted = format.convert_messages(&messages, None);
        assert_eq!(converted.len(), 1);
        let content = converted[0]
            .get("content")
            .and_then(|c| c.as_array())
            .expect("content should be a multimodal array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "input_image");
        assert_eq!(content[0]["image_url"], "https://example.com/cat.png");
        assert_eq!(content[0]["detail"], "low");
    }

    #[test]
    fn convert_messages_openai_omits_thinking_from_ordinary_assistant_text() {
        for api_format in [ApiFormat::OpenAI, ApiFormat::OpenAIResponses] {
            let format = OpenAIFormat::new(api_format);
            let messages = vec![ModelMessage {
                role: Role::Assistant,
                content: vec![
                    Content::Thinking {
                        thinking: "private ordinary reasoning".to_string(),
                        signature: "opaque-signature".to_string(),
                    },
                    Content::text("Visible answer".to_string()),
                ],
            }];

            let converted = format.convert_messages(&messages, None);

            assert_eq!(converted.len(), 1);
            assert_eq!(converted[0]["role"], "assistant");
            assert_eq!(converted[0]["content"], "Visible answer");
            let serialized = converted[0].to_string();
            assert!(!serialized.contains("private ordinary reasoning"));
            assert!(!serialized.contains("[Thinking]"));
            assert!(matches!(messages[0].content[0], Content::Thinking { .. }));
        }
    }

    #[test]
    fn convert_messages_openai_omits_thinking_but_preserves_assistant_tool_call() {
        for api_format in [ApiFormat::OpenAI, ApiFormat::OpenAIResponses] {
            let format = OpenAIFormat::new(api_format);
            let messages = vec![
                ModelMessage {
                    role: Role::Assistant,
                    content: vec![
                        Content::Thinking {
                            thinking: "private tool reasoning".to_string(),
                            signature: "opaque-signature".to_string(),
                        },
                        Content::text("I will inspect that file.".to_string()),
                        Content::ToolUse {
                            id: "call_read".to_string(),
                            name: "read".to_string(),
                            input: serde_json::json!({ "path": "README.md" }),
                        },
                    ],
                },
                ModelMessage {
                    role: Role::Tool,
                    content: vec![Content::ToolResult {
                        tool_use_id: "call_read".to_string(),
                        output: Value::String("contents".to_string()),
                        is_error: Some(false),
                    }],
                },
            ];

            let converted = format.convert_messages(&messages, None);

            assert_eq!(converted.len(), 2);
            assert_eq!(converted[0]["role"], "assistant");
            assert_eq!(converted[0]["content"], "I will inspect that file.");
            assert_eq!(converted[0]["tool_calls"][0]["id"], "call_read");
            assert_eq!(converted[0]["tool_calls"][0]["function"]["name"], "read");
            assert_eq!(
                converted[0]["tool_calls"][0]["function"]["arguments"],
                r#"{"path":"README.md"}"#
            );
            assert_eq!(converted[1]["role"], "tool");
            assert_eq!(converted[1]["tool_call_id"], "call_read");
            let serialized = converted[0].to_string();
            assert!(!serialized.contains("private tool reasoning"));
            assert!(!serialized.contains("[Thinking]"));
            assert!(matches!(messages[0].content[0], Content::Thinking { .. }));
        }
    }
}
