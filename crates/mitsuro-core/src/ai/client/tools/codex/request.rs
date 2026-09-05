use anyhow::Result;
use serde_json::Value;
use url::Url;

use super::super::super::config::{
    apply_parallel_tool_call_policy, normalized_prompt_cache_key, CallOptions,
    CodexReasoningEffort, ParallelToolCallTransport,
};
use super::super::super::core::AiClient;
use crate::ai::format::response::extract_text_from_content;
use crate::ai::retry::safe_provider_event_error;

fn collect_text_content_with_separator(content_arr: &[Value], separator: &str) -> String {
    let mut text_content = String::new();

    for item in content_arr {
        if item.get("type").and_then(|t| t.as_str()) != Some("text") {
            continue;
        }
        let Some(text) = item.get("text").and_then(|t| t.as_str()) else {
            continue;
        };

        if !text_content.is_empty() {
            text_content.push_str(separator);
        }
        text_content.push_str(text);
    }

    text_content
}

impl AiClient {
    pub(super) fn build_codex_tool_call_body(
        &self,
        model: &str,
        options: &CallOptions,
        messages: Vec<Value>,
        tools: Vec<Value>,
        thinking_enabled: bool,
    ) -> Value {
        let system_prompt = options.system_prompt.as_deref().unwrap_or_default();
        let mut codex_input: Vec<Value> = vec![];

        for msg in &messages {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
            let content = msg.get("content");

            if role == "assistant" {
                if let Some(content_arr) = content.and_then(|c| c.as_array()) {
                    let has_tool_use = content_arr
                        .iter()
                        .any(|c| c.get("type").and_then(|t| t.as_str()) == Some("tool_use"));

                    if has_tool_use {
                        let text_content = collect_text_content_with_separator(content_arr, "\n");

                        if !text_content.is_empty() {
                            codex_input.push(serde_json::json!({
                                "type": "message",
                                "role": "assistant",
                                "content": [{
                                    "type": "output_text",
                                    "text": text_content
                                }]
                            }));
                        }

                        for item in content_arr {
                            if item.get("type").and_then(|t| t.as_str()) == Some("tool_use") {
                                let id = item.get("id").and_then(|i| i.as_str()).unwrap_or("");
                                let name = item.get("name").and_then(|n| n.as_str()).unwrap_or("");
                                let input = item.get("input").cloned().unwrap_or(Value::Null);
                                codex_input.push(serde_json::json!({
                                    "type": "function_call",
                                    "call_id": id,
                                    "name": name,
                                    "arguments": input.to_string()
                                }));
                            }
                        }
                        continue;
                    }
                }

                let text = extract_text_from_content(content);
                if !text.is_empty() {
                    codex_input.push(serde_json::json!({
                        "type": "message",
                        "role": "assistant",
                        "content": [{
                            "type": "output_text",
                            "text": text
                        }]
                    }));
                }
            } else if role == "user" {
                if let Some(content_arr) = content.and_then(|c| c.as_array()) {
                    let has_tool_result = content_arr
                        .iter()
                        .any(|c| c.get("type").and_then(|t| t.as_str()) == Some("tool_result"));

                    if has_tool_result {
                        for item in content_arr {
                            if item.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                                let tool_use_id = item
                                    .get("tool_use_id")
                                    .and_then(|i| i.as_str())
                                    .unwrap_or("");
                                let output =
                                    item.get("content").and_then(|c| c.as_str()).unwrap_or("");
                                codex_input.push(serde_json::json!({
                                    "type": "function_call_output",
                                    "call_id": tool_use_id,
                                    "output": output
                                }));
                            }
                        }
                        continue;
                    }
                }

                let text = extract_text_from_content(content);
                if !text.is_empty() {
                    codex_input.push(serde_json::json!({
                        "type": "message",
                        "role": "user",
                        "content": [{
                            "type": "input_text",
                            "text": text
                        }]
                    }));
                }
            }
        }

        let codex_tools: Vec<Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "type": "function",
                    "name": t.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                    "description": t.get("description").and_then(|d| d.as_str()).unwrap_or(""),
                    "parameters": t.get("input_schema").cloned().unwrap_or(Value::Null)
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "model": model,
            "instructions": system_prompt,
            "input": codex_input,
            "tools": codex_tools,
            "tool_choice": "auto",
            "store": false,
            "stream": true,
            "text": {
                "verbosity": "low"
            }
        });

        if let Some(cache_key) = normalized_prompt_cache_key(options) {
            body["prompt_cache_key"] = serde_json::json!(cache_key);
        }

        if thinking_enabled {
            body["reasoning"] = serde_json::json!({
                "effort": options
                    .codex_reasoning_effort
                    .unwrap_or(CodexReasoningEffort::Medium)
                    .normalized_for_model(model)
                    .as_str(),
                "summary": "auto"
            });
            body["include"] = serde_json::json!(["reasoning.encrypted_content"]);
        }

        if let Some(service_tier) = options.service_tier_for_provider(self.provider_id()) {
            body["service_tier"] = serde_json::json!(service_tier);
        }

        if codex_tools.is_empty() {
            if let Some(obj) = body.as_object_mut() {
                obj.remove("tools");
                obj.remove("tool_choice");
            }
        }

        apply_parallel_tool_call_policy(
            &mut body,
            Some(options),
            ParallelToolCallTransport::ChatGptCodex,
        );
        body
    }
}

pub(super) fn resolve_codex_ws_url_for_tools(api_url: &str) -> Result<Url> {
    let mut url = Url::parse(api_url).map_err(|_| {
        anyhow::Error::msg(safe_provider_event_error(
            "Invalid sub-agent Codex API URL",
            None,
            Some("invalid_request_error"),
            Some(api_url),
        ))
    })?;

    url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
        .map_err(|_| {
            anyhow::Error::msg(safe_provider_event_error(
                "Failed to set sub-agent Codex websocket scheme",
                None,
                Some("invalid_request_error"),
                Some(api_url),
            ))
        })?;

    Ok(url)
}

#[cfg(test)]
mod tests {
    use crate::ai::client::{AiClient, AiClientConfig, CallOptions};
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;

    fn openai_responses_client() -> AiClient {
        AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".to_string(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                base_url: Some("https://chatgpt.com/backend-api/codex/responses".to_string()),
                ..Default::default()
            },
            "test-key".to_string(),
        )
    }

    #[test]
    fn codex_tool_parallel_policy_preserves_legacy_and_shared_override() {
        let client = openai_responses_client();
        for legacy in [false, true] {
            for requested in [None, Some(false), Some(true)] {
                let options = client.canonical_call_options(
                    "gpt-5.5",
                    &CallOptions {
                        parallel_tool_calls: requested,
                        codex_parallel_tool_calls: legacy,
                        ..Default::default()
                    },
                );
                let body =
                    client.build_codex_tool_call_body("gpt-5.5", &options, vec![], vec![], false);
                assert_eq!(body["parallel_tool_calls"], requested.unwrap_or(legacy));
            }
        }
    }

    #[test]
    fn chatgpt_codex_tool_body_carries_fast_mode_service_tier() {
        let client = openai_responses_client();
        let options = client.canonical_call_options(
            "gpt-5.5",
            &CallOptions {
                fast_mode: true,
                system_prompt: Some("system".to_string()),
                ..Default::default()
            },
        );

        let body = client.build_codex_tool_call_body(
            "gpt-5.5",
            &options,
            vec![serde_json::json!({
                "role": "user",
                "content": [{"type": "text", "text": "Use a tool"}]
            })],
            vec![serde_json::json!({
                "name": "read",
                "description": "Read a file",
                "input_schema": {"type": "object"}
            })],
            false,
        );

        assert_eq!(body["model"], "gpt-5.5");
        assert_eq!(body["service_tier"], "priority");
        assert_eq!(body["text"]["verbosity"], "low");
    }

    #[test]
    fn chatgpt_codex_subagent_body_uses_oauth_cache_contract() {
        let client = openai_responses_client();
        let options = CallOptions {
            session_id: Some("subagent-1".to_string()),
            ..Default::default()
        };

        let body = client.build_codex_tool_call_body(
            "gpt-5.6",
            &options,
            vec![serde_json::json!({
                "role": "user",
                "content": [{"type": "text", "text": "Continue"}]
            })],
            Vec::new(),
            false,
        );

        assert_eq!(body["prompt_cache_key"], "subagent-1");
        assert!(body.get("prompt_cache_options").is_none());
        assert!(body.get("prompt_cache_retention").is_none());
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        assert_eq!(body["model"], "gpt-5.6");
    }
}
