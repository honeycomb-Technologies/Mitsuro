use crate::ai::client::config::{apply_parallel_tool_call_policy, ParallelToolCallTransport};
use crate::ai::format::openai::responses_input::transform_request_input_for_api_format;
use crate::ai::models::ApiFormat;
use crate::ai::providers::ProviderId;
use serde_json::Value;

use super::sanitize::{
    requires_tool_call_id_sanitization, sanitize_tool_call_id, sanitize_tool_call_ids_in_value,
};

/// Transform message for provider-specific requirements
pub fn transform_message_for_provider(
    message: &Value,
    model_id: &str,
    _provider_id: ProviderId,
) -> Value {
    let id = model_id.to_lowercase();

    if id.contains("mistral") {
        return transform_mistral_message(message);
    }

    if id.contains("deepseek") || id.contains("glm") || id.contains("minimax") {
        return transform_glm_message(message);
    }

    message.clone()
}

/// Apply a final request-body transform immediately before dispatch.
///
/// This keeps provider quirks out of the transport call-sites and gives us one
/// seam to patch request bodies as model families drift.
pub fn apply_request_body_transform(
    mut body: Value,
    provider_id: ProviderId,
    api_format: ApiFormat,
    model_id: &str,
) -> Value {
    if let Some(messages) = body
        .get_mut("messages")
        .and_then(|value| value.as_array_mut())
    {
        for message in messages.iter_mut() {
            *message = transform_message_for_provider(message, model_id, provider_id);
            if requires_tool_call_id_sanitization(model_id) {
                sanitize_tool_call_ids_in_value(message);
            }
        }
    }

    if matches!(api_format, ApiFormat::OpenAIResponses)
        && body
            .get("tools")
            .and_then(|value| value.as_array())
            .is_some()
        && body.get("parallel_tool_calls").is_none()
    {
        apply_parallel_tool_call_policy(&mut body, None, ParallelToolCallTransport::Responses);
    }

    transform_request_input_for_api_format(&mut body, api_format, "input");
    transform_request_input_for_api_format(&mut body, api_format, "messages");

    body
}

/// Transform message for Mistral/GLM/MiniMax (tool call ID sanitization)
fn transform_mistral_message(message: &Value) -> Value {
    let mut msg = message.clone();

    if let Some(obj) = msg.as_object_mut() {
        if let Some(content) = obj.get_mut("content").and_then(|c| c.as_array_mut()) {
            for part in content.iter_mut() {
                if let Some(part_obj) = part.as_object_mut() {
                    if let Some(tool_call_id) = part_obj.get("toolCallId") {
                        if let Some(id_str) = tool_call_id.as_str() {
                            part_obj.insert(
                                "toolCallId".to_string(),
                                Value::String(sanitize_tool_call_id(id_str)),
                            );
                        }
                    }
                }
            }
        }
    }

    msg
}

/// Transform message for GLM/MiniMax/DeepSeek (move reasoning content to provider options)
fn transform_glm_message(message: &Value) -> Value {
    let mut msg = message.clone();

    if let Some(obj) = msg.as_object_mut() {
        if let Some(role) = obj.get("role").and_then(|r| r.as_str()) {
            if role == "assistant" {
                if let Some(content) = obj.get_mut("content").and_then(|c| c.as_array_mut()) {
                    let reasoning_text: String = content
                        .iter()
                        .filter_map(|part| {
                            part.as_object()
                                .and_then(|o| o.get("type").and_then(|t| t.as_str()))
                                .filter(|t| *t == "reasoning")
                                .and_then(|_| {
                                    part.as_object().and_then(|o| {
                                        o.get("text")
                                            .and_then(|t| t.as_str())
                                            .map(|s| s.to_string())
                                    })
                                })
                        })
                        .collect();

                    if !reasoning_text.is_empty() {
                        let filtered_content: Vec<Value> = content
                            .iter()
                            .filter(|part| {
                                part.as_object()
                                    .and_then(|o| o.get("type").and_then(|t| t.as_str()))
                                    != Some("reasoning")
                            })
                            .cloned()
                            .collect();

                        obj.insert("content".to_string(), Value::Array(filtered_content));

                        let provider_options = obj
                            .entry("providerOptions")
                            .or_insert_with(|| Value::Object(serde_json::Map::new()));

                        if let Some(opts) = provider_options.as_object_mut() {
                            if let Some(compat) = opts
                                .entry("openaiCompatible")
                                .or_insert_with(|| Value::Object(serde_json::Map::new()))
                                .as_object_mut()
                            {
                                compat.insert(
                                    "reasoning_content".to_string(),
                                    Value::String(reasoning_text),
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    msg
}
