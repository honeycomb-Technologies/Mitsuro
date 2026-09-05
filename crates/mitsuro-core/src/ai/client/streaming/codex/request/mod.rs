mod content;
mod continuation;
mod shared;

pub(super) use continuation::{assistant_fingerprint_from_response, prepare_codex_ws_request};

use serde_json::Value;
use tracing::debug;

use super::super::super::config::{
    apply_parallel_tool_call_policy, normalized_prompt_cache_key, CallOptions,
    CodexReasoningEffort, ParallelToolCallTransport,
};
use super::super::super::core::AiClient;
use crate::ai::format::openai::OpenAIFormat;
use crate::ai::format::FormatHandler;
use crate::ai::transform::apply_request_body_transform;
use crate::ai::types::{Content, ModelMessage, Role};

fn build_codex_input_messages(
    messages: &[ModelMessage],
    volatile_context: Option<&str>,
) -> Vec<Value> {
    let mut input_messages: Vec<Value> = Vec::new();

    for msg in messages.iter().filter(|m| m.role != Role::System) {
        let role = match msg.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::Tool => "tool",
            Role::System => continue,
        };

        let has_tool_results = msg
            .content
            .iter()
            .any(|c| matches!(c, Content::ToolResult { .. }));

        if has_tool_results {
            for content in &msg.content {
                if let Content::ToolResult {
                    tool_use_id,
                    output,
                    ..
                } = content
                {
                    let output_str = match output {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    input_messages.push(serde_json::json!({
                        "type": "function_call_output",
                        "call_id": tool_use_id,
                        "output": output_str
                    }));
                }
            }
            continue;
        }

        let has_tool_use = msg
            .content
            .iter()
            .any(|c| matches!(c, Content::ToolUse { .. }));

        if has_tool_use && role == "assistant" {
            let text_content = content::collect_message_text(&msg.content, "\n");

            if !text_content.is_empty() {
                input_messages.push(serde_json::json!({
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": text_content
                    }]
                }));
            }

            for content in &msg.content {
                if let Content::ToolUse { id, name, input } = content {
                    input_messages.push(serde_json::json!({
                        "type": "function_call",
                        "call_id": id,
                        "name": name,
                        "arguments": input.to_string()
                    }));
                }
            }
            continue;
        }

        if role == "user" {
            let user_content = content::build_codex_user_content(&msg.content);
            if !user_content.is_empty() {
                input_messages.push(serde_json::json!({
                    "type": "message",
                    "role": "user",
                    "content": user_content
                }));
            }
            continue;
        }

        let text = content::collect_message_text(&msg.content, "\n");
        if !text.is_empty() {
            let content_type = if role == "assistant" {
                "output_text"
            } else {
                "input_text"
            };
            input_messages.push(serde_json::json!({
                "type": "message",
                "role": role,
                "content": [{
                    "type": content_type,
                    "text": text
                }]
            }));
        }
    }

    if let Some(context) = volatile_context
        .map(str::trim)
        .filter(|context| !context.is_empty())
    {
        input_messages.push(serde_json::json!({
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": format!(
                    "[CURRENT RUNTIME CONTEXT]\nThis snapshot supersedes any earlier runtime-context snapshot.\n\n{}",
                    context
                )
            }]
        }));
    }

    input_messages
}

impl AiClient {
    pub(crate) fn build_chatgpt_codex_body(
        &self,
        messages: &[ModelMessage],
        system_prompt: &str,
        volatile_context: Option<&str>,
        _max_tokens: usize,
        options: &CallOptions,
        format_handler: &OpenAIFormat,
    ) -> Value {
        let input_messages = build_codex_input_messages(messages, volatile_context);

        let prompt_cache_key = normalized_prompt_cache_key(options);
        let thinking_enabled = options.thinking.is_some();
        let reasoning_effort = options
            .codex_reasoning_effort
            .unwrap_or(CodexReasoningEffort::Medium)
            .normalized_for_model(&self.config().model)
            .as_str();

        let mut body = serde_json::json!({
            "model": self.config().model,
            "instructions": system_prompt,
            "input": input_messages,
            "tools": [],
            "tool_choice": "auto",
            "store": false,
            "stream": true,
            "include": [],
            "text": {
                "verbosity": "low"
            }
        });

        if let Some(cache_key) = prompt_cache_key {
            body["prompt_cache_key"] = serde_json::json!(cache_key);
        }
        // The ChatGPT Codex OAuth endpoint follows the upstream Codex wire
        // contract: it accepts a stable prompt_cache_key, but not the
        // API-key Responses prompt_cache_options/prompt_cache_retention
        // controls. Keep those controls isolated to the direct OpenAI
        // Responses path.

        if let Some(service_tier) = options.service_tier_for_provider(self.provider_id()) {
            body["service_tier"] = serde_json::json!(service_tier);
        }

        if thinking_enabled {
            body["reasoning"] = serde_json::json!({
                "effort": reasoning_effort,
                "summary": "auto"
            });
            body["include"] = serde_json::json!(["reasoning.encrypted_content"]);
            debug!(
                "ChatGPT Codex: reasoning enabled (effort={}, summary=auto)",
                reasoning_effort
            );
        } else {
            debug!("ChatGPT Codex: reasoning disabled");
        }

        if let Some(tools) = &options.tools {
            let mut sorted_tools = tools.clone();
            sorted_tools.sort_by(|left, right| left.name.cmp(&right.name));
            let codex_tools = format_handler.convert_tools(&sorted_tools);
            if !codex_tools.is_empty() {
                body["tools"] = serde_json::json!(codex_tools);
            }
        }

        debug!(
            "ChatGPT Codex request: model={}, {} messages, {} tools",
            self.config().model,
            input_messages.len(),
            options.tools.as_ref().map(|t| t.len()).unwrap_or(0)
        );

        apply_parallel_tool_call_policy(
            &mut body,
            Some(options),
            ParallelToolCallTransport::ChatGptCodex,
        );
        apply_request_body_transform(
            body,
            self.provider_id(),
            self.config().api_format,
            &self.config().model,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::ai::client::{AiClient, AiClientConfig, CallOptions, PromptCacheRetention};
    use crate::ai::format::openai::OpenAIFormat;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;
    use crate::ai::types::{Content, ModelMessage, Role};

    #[test]
    fn codex_history_preserves_citations_for_user_assistant_and_tool_preambles() {
        let citations = vec![crate::ai::types::Citation {
            url: "https://example.com/docs".into(),
            title: "Docs".into(),
            cited_text: "Answer".into(),
        }];
        let expected = crate::ai::format::provider_text("Answer", &citations).into_owned();
        for role in [Role::User, Role::Assistant] {
            let message = ModelMessage {
                role: role.clone(),
                content: vec![Content::Text {
                    text: "Answer".into(),
                    citations: citations.clone(),
                }],
            };
            let (role, kind) = if role == Role::User {
                ("user", "input_text")
            } else {
                ("assistant", "output_text")
            };
            assert_eq!(
                super::build_codex_input_messages(&[message], None),
                vec![
                    serde_json::json!({"type":"message","role":role,"content":[{"type":kind,"text":expected}]})
                ]
            );
        }
        let message = ModelMessage {
            role: Role::Assistant,
            content: vec![
                Content::Text {
                    text: "Answer".into(),
                    citations,
                },
                Content::ToolUse {
                    id: "call-1".into(),
                    name: "read".into(),
                    input: serde_json::json!({}),
                },
            ],
        };
        let items = super::build_codex_input_messages(&[message], None);
        assert_eq!(
            items[0],
            serde_json::json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":expected}]})
        );
        assert_eq!(items[1]["type"], "function_call");
    }

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
    fn codex_parallel_policy_matches_body_and_transport_diagnostics() {
        let client = openai_responses_client();
        let format = OpenAIFormat::new(ApiFormat::OpenAIResponses);
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
                    client.build_chatgpt_codex_body(&[], "system", None, 128, &options, &format);
                let expected = requested.unwrap_or(legacy);
                assert_eq!(body["parallel_tool_calls"], expected);
                assert_eq!(
                    client
                        .request_diagnostics(&[], &options)
                        .effective_request
                        .parallel_tool_calls,
                    expected
                );
            }
        }
    }

    #[test]
    fn chatgpt_codex_streaming_body_carries_fast_mode_service_tier() {
        let client = openai_responses_client();
        let format_handler = OpenAIFormat::new(ApiFormat::OpenAIResponses);
        let options = client.canonical_call_options(
            "gpt-5.5",
            &CallOptions {
                fast_mode: true,
                ..Default::default()
            },
        );
        let messages = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::text("hello".to_string())],
        }];

        let body = client.build_chatgpt_codex_body(
            &messages,
            "system",
            None,
            4096,
            &options,
            &format_handler,
        );

        assert_eq!(body["model"], "gpt-5.5");
        assert_eq!(body["service_tier"], "priority");
        assert_eq!(body["text"]["verbosity"], "low");
    }

    #[test]
    fn chatgpt_codex_body_keeps_volatile_context_out_of_instructions() {
        let client = openai_responses_client();
        let format_handler = OpenAIFormat::new(ApiFormat::OpenAIResponses);
        let options = CallOptions::default();
        let messages = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::text("continue".to_string())],
        }];

        let body = client.build_chatgpt_codex_body(
            &messages,
            "stable instructions",
            Some("volatile plan progress"),
            4096,
            &options,
            &format_handler,
        );

        assert_eq!(body["instructions"], "stable instructions");
        assert_eq!(
            body["input"].as_array().unwrap().last().unwrap()["role"],
            "developer"
        );
        assert_eq!(
            body["input"]
                .as_array()
                .and_then(|items| items.last())
                .and_then(|item| {
                    item.get("content")
                        .and_then(|content| content.as_array())
                        .and_then(|content| content.first())
                        .and_then(|content| content.get("text"))
                        .and_then(|text| text.as_str())
                }),
            Some(
                "[CURRENT RUNTIME CONTEXT]\nThis snapshot supersedes any earlier runtime-context snapshot.\n\nvolatile plan progress"
            )
        );
    }

    #[test]
    fn gpt_5_6_codex_body_uses_chatgpt_prompt_cache_contract() {
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.6".to_string(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                base_url: Some("https://chatgpt.com/backend-api/codex/responses".to_string()),
                ..Default::default()
            },
            "test-key".to_string(),
        );
        let format_handler = OpenAIFormat::new(ApiFormat::OpenAIResponses);
        let options = CallOptions {
            session_id: Some("session-1".into()),
            prompt_cache_retention: PromptCacheRetention::Extended,
            ..Default::default()
        };
        let messages = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::text("continue".to_string())],
        }];

        let body = client.build_chatgpt_codex_body(
            &messages,
            "stable",
            None,
            4096,
            &options,
            &format_handler,
        );

        assert_eq!(body["prompt_cache_key"], "session-1");
        assert!(body.get("prompt_cache_options").is_none());
        assert!(body.get("prompt_cache_retention").is_none());
    }

    #[test]
    fn disabled_caching_omits_codex_cache_fields() {
        let client = openai_responses_client();
        let format_handler = OpenAIFormat::new(ApiFormat::OpenAIResponses);
        let options = CallOptions {
            enable_caching: false,
            session_id: Some("session-1".into()),
            ..Default::default()
        };

        let body =
            client.build_chatgpt_codex_body(&[], "stable", None, 4096, &options, &format_handler);

        assert!(body.get("prompt_cache_key").is_none());
        assert!(body.get("prompt_cache_options").is_none());
    }
}
