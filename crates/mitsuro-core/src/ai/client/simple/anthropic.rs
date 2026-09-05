use anyhow::Result;
use serde_json::Value;
use tracing::debug;

use super::super::config::{anthropic_prompt_cache_control, CallOptions};
use super::super::core::AiClient;
use super::shared::{collect_anthropic_text, trim_or_empty};
use super::SimpleCallResult;
use crate::ai::format::anthropic::AnthropicFormat;
use crate::ai::format::FormatHandler;
use crate::ai::model_profile::SystemPromptSections;
use crate::ai::transform::apply_request_body_transform;
use crate::ai::types::ModelMessage;
use crate::ai::usage::parse_anthropic_usage;

impl AiClient {
    /// Simple non-streaming call using Anthropic format
    ///
    /// Uses cache_control on the system prompt when the provider supports it,
    /// so repeated calls with the same system prompt benefit from caching.
    pub(super) async fn call_simple_anthropic(
        &self,
        model: &str,
        system_prompt: &str,
        user_message: &str,
        max_tokens: usize,
        options: &CallOptions,
    ) -> Result<SimpleCallResult> {
        // Only apply cache_control for providers that support prompt caching.
        // MiniMax, Z.ai, etc. use Anthropic format but may reject cache_control blocks.
        let cache_control = anthropic_prompt_cache_control(options, self.provider_id());

        let system_value: serde_json::Value = if let Some(cache_control) = &cache_control {
            serde_json::json!([{
                "type": "text",
                "text": system_prompt,
                "cache_control": cache_control
            }])
        } else {
            serde_json::Value::String(system_prompt.to_string())
        };

        let mut body = serde_json::json!({
            "model": model,
            "max_tokens": max_tokens,
            "messages": [{
                "role": "user",
                "content": user_message
            }],
            "system": system_value
        });

        // Auto-caching: API places breakpoint on the last cacheable block
        if let Some(cache_control) = cache_control {
            body["cache_control"] = cache_control;
        }

        let body =
            apply_request_body_transform(body, self.provider_id(), self.config().api_format, model);
        let request = self.build_request(&self.config().api_url());
        let response = request.json(&body).send().await?;
        let response = self.handle_error_response(response).await?;

        let json: Value = response.json().await?;

        let text = json
            .get("content")
            .and_then(|c| c.as_array())
            .map(|arr| collect_anthropic_text(arr))
            .unwrap_or_default();

        Ok(SimpleCallResult {
            text: trim_or_empty(Some(&text)),
            usage: parse_anthropic_usage(&json),
        })
    }

    /// Cache-safe conversation call using Anthropic format.
    ///
    /// Builds the same multi-block system prompt structure as the streaming path:
    /// base prompt (cached) → identity context (cached) → project context (cached)
    /// → session context (not cached).
    /// Conversation messages are converted using the same format handler, so the
    /// entire prefix matches what the parent conversation built.
    pub(super) async fn call_conversation_anthropic(
        &self,
        model: &str,
        base_system_prompt: &str,
        conversation: &[ModelMessage],
        appended_user_message: &str,
        max_tokens: usize,
        options: &CallOptions,
    ) -> Result<SimpleCallResult> {
        let cache_control = anthropic_prompt_cache_control(options, self.provider_id());
        let format_handler = AnthropicFormat::new();
        let prompt_sections =
            self.system_prompt_sections(model, conversation, Some(base_system_prompt), None);

        // Convert parent conversation messages (System role filtered by format handler)
        let mut api_messages =
            format_handler.convert_messages(conversation, Some(self.provider_id()));

        // Append the new user message at the end
        api_messages.push(serde_json::json!({
            "role": "user",
            "content": [{"type": "text", "text": appended_user_message}]
        }));

        // Build system prompt with the same multi-block structure as streaming.
        // This ensures the cached prefix from the parent conversation is reused.
        let system_value =
            anthropic_conversation_system_value(&prompt_sections, cache_control.as_ref());

        let mut body = serde_json::json!({
            "model": model,
            "max_tokens": max_tokens,
            "system": system_value,
            "messages": api_messages,
        });

        // Auto-caching: API places breakpoint on the last cacheable block.
        // Combined with block-level caching on system prompt blocks, this
        // ensures both the static prefix and conversation are cached.
        if let Some(cache_control) = cache_control {
            body["cache_control"] = cache_control;
        }

        debug!(
            "Cache-safe Anthropic call: {} conversation messages + appended user message",
            conversation.len()
        );

        let body =
            apply_request_body_transform(body, self.provider_id(), self.config().api_format, model);
        let request = self.build_request(&self.config().api_url());
        let response = request.json(&body).send().await?;
        let response = self.handle_error_response(response).await?;

        let json: Value = response.json().await?;

        let text = json
            .get("content")
            .and_then(|c| c.as_array())
            .map(|arr| collect_anthropic_text(arr))
            .unwrap_or_default();

        Ok(SimpleCallResult {
            text: trim_or_empty(Some(&text)),
            usage: parse_anthropic_usage(&json),
        })
    }
}

fn anthropic_conversation_system_value(
    prompt_sections: &SystemPromptSections,
    cache_control: Option<&Value>,
) -> Value {
    let Some(cache_control) = cache_control else {
        return Value::String(prompt_sections.combined());
    };

    let mut blocks: Vec<Value> = Vec::new();

    // Stable prefix order must match streaming exactly.
    for text in [
        prompt_sections.base_prompt.as_str(),
        prompt_sections.identity_context.as_str(),
        prompt_sections.project_context.as_str(),
    ] {
        if !text.is_empty() {
            blocks.push(serde_json::json!({
                "type": "text",
                "text": text,
                "cache_control": cache_control
            }));
        }
    }

    if !prompt_sections.session_context.is_empty() {
        blocks.push(serde_json::json!({
            "type": "text",
            "text": prompt_sections.session_context.as_str()
        }));
    }

    Value::Array(blocks)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::anthropic_conversation_system_value;
    use crate::ai::model_profile::build_system_prompt_sections;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;
    use crate::ai::types::{Content, ModelMessage, Role};
    use crate::ai::usage::parse_anthropic_usage;

    #[test]
    fn conversation_system_blocks_match_streaming_identity_order() {
        let messages = [
            ModelMessage {
                role: Role::System,
                content: vec![Content::text(
                    "[HIVE SOUL - HIVE_SOUL.md]\nidentity-layer".into(),
                )],
            },
            ModelMessage {
                role: Role::System,
                content: vec![Content::text(
                    "[PROJECT INSTRUCTIONS - AGENTS.md]\nproject-layer".into(),
                )],
            },
            ModelMessage {
                role: Role::System,
                content: vec![Content::text("[HIVE HEARTBEAT]\nsession-layer".into())],
            },
        ];
        let sections = build_system_prompt_sections(
            ProviderId::Anthropic,
            ApiFormat::Anthropic,
            "claude-sonnet-4-5",
            &messages,
            Some("base-layer"),
            &[],
        );
        let system =
            anthropic_conversation_system_value(&sections, Some(&json!({"type": "ephemeral"})));
        let blocks = system.as_array().expect("cached system blocks");

        assert_eq!(blocks.len(), 4);
        for (index, marker) in [
            "base-layer",
            "identity-layer",
            "project-layer",
            "session-layer",
        ]
        .iter()
        .enumerate()
        {
            assert!(blocks[index]["text"].as_str().unwrap().contains(marker));
        }
        assert!(blocks[..3]
            .iter()
            .all(|block| block.get("cache_control").is_some()));
        assert!(blocks[3].get("cache_control").is_none());
    }

    #[test]
    fn response_usage_preserves_anthropic_cache_buckets() {
        let usage = parse_anthropic_usage(&json!({
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_creation_input_tokens": 200,
                "cache_read_input_tokens": 700
            }
        }))
        .expect("usage");

        assert_eq!(usage.input_tokens(), 1_000);
        assert_eq!(usage.logical_total_tokens(), 1_050);
    }
}
