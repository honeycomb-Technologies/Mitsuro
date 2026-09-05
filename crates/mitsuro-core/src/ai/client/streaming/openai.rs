use anyhow::Result;
use serde_json::Value;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, info};

use super::super::config::{
    apply_parallel_tool_call_policy, normalized_prompt_cache_key, openai_prompt_cache_options,
    openai_prompt_cache_retention, CallOptions, CodexReasoningEffort, OpenAiPromptCacheMode,
};
use super::super::core::AiClient;
use super::super::RemoteAttemptPolicy;
use super::shared::{ensure_success_stream_response, log_request_metrics, start_sse_stream};
use crate::ai::format::openai::OpenAIFormat;
use crate::ai::format::FormatHandler;
use crate::ai::models::ApiFormat;
use crate::ai::parsers::OpenAIParser;
use crate::ai::providers::ProviderId;
use crate::ai::streaming::StreamPart;
use crate::ai::transform::apply_request_body_transform;
use crate::ai::types::ModelMessage;

fn append_openai_hosted_web_search_tool(
    body: &mut Value,
    options: &CallOptions,
    api_format: ApiFormat,
) {
    if options.web_search.is_none() || !matches!(api_format, ApiFormat::OpenAIResponses) {
        return;
    }

    let hosted_tool = serde_json::json!({ "type": "web_search" });
    match body.get_mut("tools") {
        Some(Value::Array(tools)) => {
            remove_openai_function_tool_named(tools, "web_search");
            let already_present = tools
                .iter()
                .any(|tool| tool.get("type").and_then(Value::as_str) == Some("web_search"));
            if !already_present {
                tools.push(hosted_tool);
            }
        }
        _ => {
            body["tools"] = serde_json::json!([hosted_tool]);
        }
    }
}

fn remove_openai_function_tool_named(tools: &mut Vec<Value>, name: &str) {
    tools.retain(|tool| {
        if tool.get("type").and_then(Value::as_str) != Some("function") {
            return true;
        }

        let direct_name = tool.get("name").and_then(Value::as_str);
        let nested_name = tool
            .get("function")
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str);
        !matches!(direct_name.or(nested_name), Some(tool_name) if tool_name == name)
    });
}

fn append_stream_usage_options(body: &mut Value, provider_id: ProviderId, api_format: ApiFormat) {
    if provider_id == ProviderId::OpenAI && matches!(api_format, ApiFormat::OpenAI) {
        body["stream_options"] = serde_json::json!({"include_usage": true});
    }
}

fn append_zai_reasoning_config(body: &mut Value, options: &CallOptions) {
    let enabled = options.thinking.is_some();
    body["thinking"] = serde_json::json!({
        "type": if enabled { "enabled" } else { "disabled" }
    });
    if enabled {
        if let Some(effort) = options.codex_reasoning_effort {
            body["reasoning_effort"] = serde_json::json!(effort.as_str());
        }
    } else if let Some(object) = body.as_object_mut() {
        object.remove("reasoning_effort");
    }
}

fn stable_system_prompt(sections: &crate::ai::model_profile::SystemPromptSections) -> String {
    [
        sections.base_prompt.trim(),
        sections.identity_context.trim(),
        sections.project_context.trim(),
    ]
    .into_iter()
    .filter(|section| !section.is_empty())
    .collect::<Vec<_>>()
    .join("\n\n---\n\n")
}

fn responses_instructions(system_prompt: String, explicit_breakpoint: bool) -> Value {
    if !explicit_breakpoint {
        return Value::String(system_prompt);
    }

    let mut text = serde_json::json!({
        "type": "input_text",
        "text": system_prompt,
    });
    if explicit_breakpoint {
        text["prompt_cache_breakpoint"] = serde_json::json!({"mode": "explicit"});
    }
    serde_json::json!([{
        "type": "message",
        "role": "developer",
        "content": [text],
    }])
}

fn append_runtime_instructions(messages: &mut Vec<Value>, session_context: &str) {
    let context = session_context.trim();
    if context.is_empty() {
        return;
    }
    messages.push(serde_json::json!({
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

impl AiClient {
    /// Streaming call using OpenAI format
    pub(super) async fn call_streaming_openai(
        &self,
        messages: Vec<ModelMessage>,
        options: &CallOptions,
        call_start: Instant,
        attempt_policy: RemoteAttemptPolicy,
    ) -> Result<mpsc::UnboundedReceiver<StreamPart>> {
        // Check if we're using ChatGPT Codex API (OAuth) vs standard OpenAI API
        let is_chatgpt_codex = self.config().uses_chatgpt_codex_format();

        if is_chatgpt_codex {
            info!(
                "Using ChatGPT Codex format for {} (OAuth)",
                self.config().model
            );
            return self
                .call_streaming_chatgpt_codex_ws(messages, options, call_start, attempt_policy)
                .await;
        } else {
            info!(
                "Using OpenAI-compatible {:?} format for {}",
                self.config().api_format,
                self.config().model
            );
        }

        let format_handler = OpenAIFormat::new(self.config().api_format);
        let prompt_sections = self.system_prompt_sections(
            &self.config().model,
            &messages,
            options.system_prompt.as_deref(),
            options.tools.as_deref(),
        );
        let responses_format = matches!(self.config().api_format, ApiFormat::OpenAIResponses);
        let prompt_cache_key = if responses_format {
            normalized_prompt_cache_key(options)
        } else {
            None
        };
        let prompt_cache_options = if responses_format {
            openai_prompt_cache_options(
                options,
                &self.config().model,
                OpenAiPromptCacheMode::Explicit,
            )
        } else {
            None
        };
        let supports_cache_options = prompt_cache_options.is_some();
        let system_prompt = if responses_format {
            stable_system_prompt(&prompt_sections)
        } else {
            prompt_sections.combined()
        };

        let max_tokens = options.max_tokens.unwrap_or(self.config().max_tokens);

        // Standard OpenAI format (Chat Completions or Responses API)
        let openai_messages = format_handler.convert_messages(&messages, Some(self.provider_id()));

        // Responses API uses "input", Chat Completions uses "messages"
        let (messages_key, max_tokens_key) = if matches!(
            self.config().api_format,
            crate::ai::models::ApiFormat::OpenAIResponses
        ) {
            ("input", "max_output_tokens")
        } else {
            ("messages", "max_tokens")
        };

        let mut body = serde_json::json!({
            "model": self.config().model,
            "stream": true,
        });
        body[max_tokens_key] = serde_json::json!(max_tokens);
        body[messages_key] = serde_json::json!(openai_messages);

        // Keep reusable instructions first. Volatile runtime directives remain
        // developer-authority input at the tail so they cannot be overridden
        // as user content while exact-prefix caching still reuses the stable
        // prefix.
        if let Some(msgs) = body.get_mut(messages_key).and_then(|m| m.as_array_mut()) {
            if responses_format {
                append_runtime_instructions(msgs, &prompt_sections.session_context);
            } else {
                msgs.insert(
                    0,
                    serde_json::json!({
                        "role": "system",
                        "content": system_prompt
                    }),
                );
            }
        }
        if responses_format {
            body["instructions"] = responses_instructions(system_prompt, supports_cache_options);
        }

        // Add temperature
        if options.thinking.is_none() {
            if let Some(temp) = options.temperature {
                body["temperature"] = serde_json::json!(temp);
            }
        }

        if options.thinking.is_some()
            && matches!(self.config().api_format, ApiFormat::OpenAIResponses)
        {
            let effort = options
                .codex_reasoning_effort
                .unwrap_or(CodexReasoningEffort::Medium)
                .normalized_for_model(&self.config().model)
                .as_str();
            body["reasoning"] = serde_json::json!({
                "effort": effort
            });
        }

        if self.provider_id() == ProviderId::ZAi {
            append_zai_reasoning_config(&mut body, options);
        }

        if let Some(service_tier) = options.service_tier_for_provider(self.provider_id()) {
            body["service_tier"] = serde_json::json!(service_tier);
        }

        append_stream_usage_options(&mut body, self.provider_id(), self.config().api_format);

        if let Some(cache_key) = prompt_cache_key.as_deref() {
            body["prompt_cache_key"] = serde_json::json!(cache_key);
        }
        if let Some(cache_options) = prompt_cache_options {
            body["prompt_cache_options"] = cache_options;
        }
        if responses_format {
            if let Some(retention) = openai_prompt_cache_retention(options, &self.config().model) {
                body["prompt_cache_retention"] = retention;
            }
        }
        if responses_format {
            body["text"] = serde_json::json!({"verbosity": "low"});
        }

        // Add tools — sorted deterministically for stable prefix ordering.
        // OpenAI uses automatic prefix caching; consistent tool order maximizes hits.
        if let Some(tools) = &options.tools {
            let mut sorted: Vec<_> = tools.to_vec();
            sorted.sort_by(|a, b| a.name.cmp(&b.name));
            let openai_tools = format_handler.convert_tools(&sorted);
            if !openai_tools.is_empty() {
                body["tools"] = serde_json::json!(openai_tools);
            }
        }
        append_openai_hosted_web_search_tool(&mut body, options, self.config().api_format);
        apply_parallel_tool_call_policy(
            &mut body,
            Some(options),
            self.config().parallel_tool_call_transport(),
        );
        let body = apply_request_body_transform(
            body,
            self.provider_id(),
            self.config().api_format,
            &self.config().model,
        );
        log_request_metrics(
            "openai_stream",
            &prompt_sections,
            &messages,
            options.tools.as_deref(),
            options.system_prompt.is_some(),
            if supports_cache_options {
                "explicit_prefix_plus_implicit_tail"
            } else if responses_format && options.enable_caching {
                "automatic"
            } else if responses_format {
                "disabled"
            } else {
                "provider_default"
            },
            prompt_cache_key.is_some(),
            serde_json::to_vec(&body).map_or(0, |value| value.len()),
        );

        debug!("OpenAI request to: {}", self.config().api_url());

        let request = self.build_request(&self.config().api_url());

        info!("Sending OpenAI format request...");
        let response = request.json(&body).send().await?;
        let response =
            ensure_success_stream_response(response, call_start, "API response", "API error")
                .await?;

        info!("Starting OpenAI stream processing task");
        Ok(start_sse_stream(
            response,
            OpenAIParser::new(),
            "OpenAI",
            self.provider_id(),
            self.config().api_format,
            &self.config().model,
        ))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::ai::types::WebSearchConfig;

    #[test]
    fn hosted_web_search_is_added_for_responses_api() {
        let mut body = json!({
            "model": "gpt-5.5",
            "tools": [
                {"type": "function", "name": "read"},
                {"type": "function", "name": "web_search"}
            ]
        });
        let options = CallOptions {
            web_search: Some(WebSearchConfig::default()),
            ..Default::default()
        };

        append_openai_hosted_web_search_tool(&mut body, &options, ApiFormat::OpenAIResponses);

        let tools = body["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0], json!({"type": "function", "name": "read"}));
        assert_eq!(tools[1], json!({"type": "web_search"}));
    }

    #[test]
    fn hosted_web_search_is_not_added_for_chat_completions() {
        let mut body = json!({"model": "gpt-4.1"});
        let options = CallOptions {
            web_search: Some(WebSearchConfig::default()),
            ..Default::default()
        };

        append_openai_hosted_web_search_tool(&mut body, &options, ApiFormat::OpenAI);

        assert!(body.get("tools").is_none());
    }

    #[test]
    fn official_chat_completions_requests_usage_frames() {
        let mut body = json!({"model": "gpt-4.1", "stream": true});
        append_stream_usage_options(&mut body, ProviderId::OpenAI, ApiFormat::OpenAI);

        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn compatible_providers_do_not_receive_openai_only_stream_options() {
        let mut body = json!({"model": "grok", "stream": true});
        append_stream_usage_options(&mut body, ProviderId::Grok, ApiFormat::OpenAI);

        assert!(body.get("stream_options").is_none());
    }

    #[test]
    fn zai_uses_top_level_thinking_and_effort_controls() {
        let mut enabled = json!({});
        append_zai_reasoning_config(
            &mut enabled,
            &CallOptions {
                thinking: Some(crate::ai::types::ThinkingConfig::default()),
                codex_reasoning_effort: Some(CodexReasoningEffort::Max),
                ..Default::default()
            },
        );
        assert_eq!(enabled["thinking"]["type"], "enabled");
        assert_eq!(enabled["reasoning_effort"], "max");

        let mut disabled = json!({"reasoning_effort": "high"});
        append_zai_reasoning_config(&mut disabled, &CallOptions::default());
        assert_eq!(disabled["thinking"]["type"], "disabled");
        assert!(disabled.get("reasoning_effort").is_none());
    }

    #[test]
    fn responses_instructions_mark_the_stable_prefix_without_transform_loss() {
        let options = CallOptions {
            session_id: Some("session".into()),
            ..Default::default()
        };
        let instructions = responses_instructions("stable instructions".into(), true);
        let cache_options =
            openai_prompt_cache_options(&options, "gpt-5.6", OpenAiPromptCacheMode::Explicit)
                .unwrap();
        let body = apply_request_body_transform(
            json!({
                "model": "gpt-5.6",
                "instructions": instructions,
                "input": [{"role": "user", "content": "task"}],
                "prompt_cache_options": cache_options
            }),
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.6",
        );

        assert_eq!(body["instructions"][0]["role"], "developer");
        assert_eq!(body["instructions"][0]["content"][0]["type"], "input_text");
        assert_eq!(
            body["instructions"][0]["content"][0]["prompt_cache_breakpoint"]["mode"],
            "explicit"
        );
        assert_eq!(body["prompt_cache_options"]["mode"], "explicit");
        assert_eq!(body["prompt_cache_options"]["ttl"], "30m");
    }

    #[test]
    fn volatile_runtime_directives_keep_developer_authority_at_the_tail() {
        let mut messages = vec![json!({"role": "user", "content": "task"})];
        append_runtime_instructions(
            &mut messages,
            "[ACTIVE PLAN]\nplan changed\n\n[HIVE COORDINATOR]\nkeep delegating",
        );
        let body = apply_request_body_transform(
            json!({"model": "gpt-5.6", "input": messages}),
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.6",
        );
        let messages = body["input"].as_array().unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1]["role"], "developer");
        assert!(messages[1]["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.contains("supersedes")
                && text.contains("[ACTIVE PLAN]")
                && text.contains("[HIVE COORDINATOR]")));
    }
}
