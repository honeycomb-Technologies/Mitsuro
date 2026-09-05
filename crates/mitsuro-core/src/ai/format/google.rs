//! Google/Gemini API format handler
//!
//! Handles conversion to Google AI API format (contents, parts, functionDeclarations).

use serde_json::Value;

use super::{FormatHandler, RequestOptions};
use crate::ai::providers::ProviderId;
use crate::ai::types::{AiTool, Content, ModelMessage, Role};

/// Google format handler
pub struct GoogleFormat {
    endpoint_template: String,
}

impl GoogleFormat {
    pub fn new() -> Self {
        Self {
            endpoint_template: "/v1/models/{}:streamGenerateContent".to_string(),
        }
    }
}

impl Default for GoogleFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatHandler for GoogleFormat {
    /// Convert messages to Google contents format
    /// Note: provider_id is unused for Google format (no thinking block handling needed)
    fn convert_messages(
        &self,
        messages: &[ModelMessage],
        _provider_id: Option<ProviderId>,
    ) -> Vec<Value> {
        messages
            .iter()
            .filter(|m| m.role != Role::System) // System handled separately
            .map(|m| {
                let role = match m.role {
                    Role::User | Role::Tool => "user", // Tool results are user role in Google format
                    Role::Assistant => "model",
                    Role::System => "user", // Should be filtered out
                };

                let parts: Vec<Value> = m
                    .content
                    .iter()
                    .filter_map(convert_content_to_part)
                    .collect();

                serde_json::json!({
                    "role": role,
                    "parts": parts
                })
            })
            .collect()
    }

    /// Convert tools to Google function declarations format
    fn convert_tools(&self, tools: &[AiTool]) -> Vec<Value> {
        tools
            .iter()
            .map(|tool| {
                serde_json::json!({
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema
                })
            })
            .collect()
    }

    fn build_request_body(
        &self,
        _model: &str,
        messages: Vec<Value>,
        options: &RequestOptions,
    ) -> Value {
        let mut body = serde_json::json!({
            "contents": messages,
            "generationConfig": {
                "maxOutputTokens": options.max_tokens,
            }
        });

        // Add system instruction
        if let Some(system) = options.system_prompt {
            body["systemInstruction"] = serde_json::json!({
                "parts": [{"text": system}]
            });
        }

        // Add temperature
        if let Some(temp) = options.temperature {
            body["generationConfig"]["temperature"] = serde_json::json!(temp);
        }

        // Add tools if present
        if let Some(tools) = options.tools {
            if !tools.is_empty() {
                let google_tools = self.convert_tools(tools);
                body["tools"] = serde_json::json!([{
                    "functionDeclarations": google_tools
                }]);
            }
        }

        body
    }

    fn endpoint_path(&self, _model: &str) -> &str {
        // Note: The caller should use the model to construct the full path
        // This returns a template that needs model substitution
        // In practice, the AiClient handles the full URL construction
        &self.endpoint_template
    }
}

/// Convert a single content block to Google parts format
fn convert_content_to_part(content: &Content) -> Option<Value> {
    match content {
        Content::Text { text, citations } => {
            Some(serde_json::json!({"text": crate::ai::format::provider_text(text, citations)}))
        }
        Content::Image { image, .. } => {
            // Google expects inline_data for images
            let mime = image.media_type.as_deref().unwrap_or("image/png");
            image
                .base64
                .as_ref()
                .map(|data| {
                    serde_json::json!({
                        "inline_data": {
                            "mime_type": mime,
                            "data": data
                        }
                    })
                })
                .or_else(|| {
                    // Google also supports file_data with URI
                    image.url.as_ref().map(|url| {
                        serde_json::json!({
                            "file_data": {
                                "file_uri": url,
                                "mime_type": mime
                            }
                        })
                    })
                })
        }
        Content::ToolUse { name, input, .. } => {
            // Function call in assistant message
            Some(serde_json::json!({
                "functionCall": {
                    "name": name,
                    "args": input
                }
            }))
        }
        Content::ToolResult {
            tool_use_id,
            output,
            ..
        } => {
            // Function response in user message
            Some(serde_json::json!({
                "functionResponse": {
                    "name": tool_use_id,
                    "response": {
                        "content": output
                    }
                }
            }))
        }
        _ => None,
    }
}
