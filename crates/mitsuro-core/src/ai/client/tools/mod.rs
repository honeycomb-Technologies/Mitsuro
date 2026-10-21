//! Tool-calling API methods
//!
//! Non-streaming calls with tool support, used by sub-agents.

mod anthropic;
mod codex;
mod google;
mod openai;

use anyhow::Result;
use serde_json::{json, Value};

use super::config::CallOptions;
use super::core::AiClient;
use super::RemoteAttemptPolicy;
use crate::ai::providers::ReasoningEffort;
use crate::ai::retry::{with_retry, RetryConfig};
use crate::ai::types::{AiTool, Content, ModelMessage, Role, ThinkingConfig};

/// Convert Mitsuro's typed conversation into the portable tool-call envelope
/// consumed by every non-streaming provider adapter.
///
/// This is deliberately provider-neutral. Provider adapters remain responsible
/// for their final wire shape, but delegated execution no longer hand-builds a
/// second, drift-prone message representation or leaks persisted reasoning into
/// another model's visible history.
fn canonical_tool_messages(messages: &[ModelMessage]) -> Vec<Value> {
    messages
        .iter()
        .filter_map(|message| {
            let role = match message.role {
                Role::Assistant => "assistant",
                Role::System | Role::User | Role::Tool => "user",
            };

            let content = message
                .content
                .iter()
                .filter_map(|block| match block {
                    Content::Text { text, citations } if !text.is_empty() || !citations.is_empty() => {
                        Some(json!({"type": "text", "text": crate::ai::format::provider_text(text, citations)}))
                    }
                    Content::ToolUse { id, name, input } => Some(json!({
                        "type": "tool_use",
                        "id": id,
                        "name": name,
                        "input": input,
                    })),
                    Content::ToolResult {
                        tool_use_id,
                        output,
                        is_error,
                    } => Some(json!({
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": match output {
                            Value::String(text) => text.clone(),
                            other => other.to_string(),
                        },
                        "is_error": is_error.unwrap_or(false),
                    })),
                    // Thinking signatures and redacted reasoning are bound to
                    // their source provider and must never be flattened into
                    // portable assistant text during a model handoff.
                    Content::Thinking { .. } | Content::RedactedThinking { .. } => None,
                    Content::Image { .. } => Some(json!({
                        "type": "text",
                        "text": "[image attachment omitted from delegated tool call]",
                    })),
                    Content::Document { .. } => Some(json!({
                        "type": "text",
                        "text": "[document attachment omitted from delegated tool call]",
                    })),
                    Content::Text { .. } => None,
                })
                .collect::<Vec<_>>();

            (!content.is_empty()).then(|| json!({"role": role, "content": content}))
        })
        .collect()
}

fn canonical_tool_definitions(tools: &[AiTool]) -> Vec<Value> {
    let mut tools = tools.iter().collect::<Vec<_>>();
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    tools
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
            })
        })
        .collect()
}

impl AiClient {
    /// Call the API with tools (non-streaming, for sub-agents)
    ///
    /// Used by sub-agents that need tool execution but don't need streaming.
    /// Routes to appropriate format handler based on API format.
    pub async fn call_with_tools(
        &self,
        model: &str,
        system_prompt: &str,
        messages: &[ModelMessage],
        tools: &[AiTool],
        max_tokens: usize,
        thinking_enabled: bool,
        session_id: Option<&str>,
        prompt_cache_key: Option<&str>,
    ) -> Result<Value> {
        self.call_with_tools_at_reasoning(
            model,
            system_prompt,
            messages,
            tools,
            max_tokens,
            thinking_enabled.then_some(ReasoningEffort::Medium),
            session_id,
            prompt_cache_key,
        )
        .await
    }

    /// Typed non-streaming tool call used by delegated execution.
    ///
    /// Unlike the compatibility wrapper above, this preserves the exact
    /// parent-selected reasoning level until immutable model capability
    /// normalization maps it to the provider wire contract.
    #[allow(clippy::too_many_arguments)]
    pub async fn call_with_tools_at_reasoning(
        &self,
        model: &str,
        system_prompt: &str,
        messages: &[ModelMessage],
        tools: &[AiTool],
        max_tokens: usize,
        reasoning_effort: Option<ReasoningEffort>,
        session_id: Option<&str>,
        prompt_cache_key: Option<&str>,
    ) -> Result<Value> {
        self.call_with_tools_at_reasoning_and_attempt_policy(
            model,
            system_prompt,
            messages,
            tools,
            max_tokens,
            reasoning_effort,
            session_id,
            prompt_cache_key,
            RemoteAttemptPolicy::ConfiguredRetries,
        )
        .await
    }

    /// Typed non-streaming tool call with an explicit remote-attempt policy.
    #[allow(clippy::too_many_arguments)]
    pub async fn call_with_tools_at_reasoning_and_attempt_policy(
        &self,
        model: &str,
        system_prompt: &str,
        messages: &[ModelMessage],
        tools: &[AiTool],
        max_tokens: usize,
        reasoning_effort: Option<ReasoningEffort>,
        session_id: Option<&str>,
        prompt_cache_key: Option<&str>,
        attempt_policy: RemoteAttemptPolicy,
    ) -> Result<Value> {
        self.ensure_run_model(model)?;
        let requested_tool_count = tools.len();
        let thinking_enabled =
            reasoning_effort.is_some_and(|effort| effort != ReasoningEffort::None);
        let options = self.canonical_call_options(
            model,
            &CallOptions {
                max_tokens: Some(max_tokens),
                tools: (!tools.is_empty()).then(|| tools.to_vec()),
                system_prompt: Some(system_prompt.to_string()),
                thinking: thinking_enabled.then(ThinkingConfig::default),
                reasoning_effort,
                codex_parallel_tool_calls: true,
                session_id: session_id.map(ToString::to_string),
                prompt_cache_key: prompt_cache_key.map(ToString::to_string),
                ..Default::default()
            },
        );

        let messages = canonical_tool_messages(messages);
        let tools = canonical_tool_definitions(options.tools.as_deref().unwrap_or_default());
        if requested_tool_count > 0 && tools.is_empty() {
            anyhow::bail!(
                "model '{}' does not advertise tool calling; delegated execution requires an exact tool-capable model catalog row",
                model
            );
        }
        if !attempt_policy.allows_retry() {
            return self
                .call_with_tools_once(model, &options, messages, tools)
                .await;
        }

        // This method is the sole retry owner for non-streaming tool turns.
        // A retry is allowed only before any result has been exposed or any
        // local tool side effect has occurred.
        let retry_config = RetryConfig::gentle();
        with_retry(&retry_config, || async {
            self.call_with_tools_once(model, &options, messages.clone(), tools.clone())
                .await
        })
        .await
    }

    async fn call_with_tools_once(
        &self,
        model: &str,
        options: &CallOptions,
        messages: Vec<Value>,
        tools: Vec<Value>,
    ) -> Result<Value> {
        if self.config().uses_openai_format() {
            return self
                .call_with_tools_openai(model, options, messages, tools)
                .await;
        }

        if self.config().uses_google_format() {
            return self
                .call_with_tools_google(model, options, messages, tools)
                .await;
        }

        self.call_with_tools_anthropic(model, options, messages, tools)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{canonical_tool_definitions, canonical_tool_messages};
    use crate::ai::client::{AiClient, AiClientConfig, RemoteAttemptPolicy};
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::{AuthHeader, ProviderId};
    use crate::ai::types::{AiTool, Content, ModelMessage, Role};
    use serde_json::json;
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Duration;
    use tiny_http::{Header, Response, Server};

    #[test]
    fn portable_tool_history_preserves_citations_as_plain_reference_text() {
        let citations = vec![crate::ai::types::Citation {
            url: "https://example.com/docs".into(),
            title: "Docs".into(),
            cited_text: "Answer".into(),
        }];
        let expected = crate::ai::format::provider_text("Answer", &citations).into_owned();
        let messages = vec![ModelMessage {
            role: Role::Assistant,
            content: vec![Content::Text {
                text: "Answer".into(),
                citations,
            }],
        }];
        assert_eq!(
            canonical_tool_messages(&messages),
            vec![json!({"role":"assistant","content":[{"type":"text","text":expected}]})]
        );
    }

    fn openai_test_client(url: String) -> AiClient {
        AiClient::new(
            AiClientConfig {
                model: "test-model".to_string(),
                max_tokens: 128,
                base_url: Some(url),
                auth_header: AuthHeader::Bearer,
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAI,
                custom_headers: Default::default(),
            },
            "test-key".to_string(),
        )
    }

    fn user_message() -> ModelMessage {
        ModelMessage {
            role: Role::User,
            content: vec![Content::text("reply with ok".to_string())],
        }
    }

    fn retry_after_zero() -> Header {
        Header::from_bytes("Retry-After", "0").expect("retry header should be valid")
    }

    fn successful_tool_response() -> Response<std::io::Cursor<Vec<u8>>> {
        Response::from_string(
            r#"{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#,
        )
        .with_header(
            Header::from_bytes("Content-Type", "application/json")
                .expect("content type should be valid"),
        )
    }

    #[test]
    fn canonical_history_drops_nonportable_reasoning_and_keeps_tool_lifecycle() {
        let messages = vec![
            ModelMessage {
                role: Role::Assistant,
                content: vec![
                    Content::Thinking {
                        thinking: "private reasoning".to_string(),
                        signature: "provider-bound".to_string(),
                    },
                    Content::ToolUse {
                        id: "call-1".to_string(),
                        name: "read".to_string(),
                        input: json!({"file_path": "README.md"}),
                    },
                ],
            },
            ModelMessage {
                role: Role::Tool,
                content: vec![Content::ToolResult {
                    tool_use_id: "call-1".to_string(),
                    output: json!({"ok": true}),
                    is_error: Some(false),
                }],
            },
        ];

        let canonical = canonical_tool_messages(&messages);
        let serialized = serde_json::to_string(&canonical).unwrap();
        assert!(!serialized.contains("private reasoning"));
        assert!(!serialized.contains("provider-bound"));
        assert!(serialized.contains("tool_use"));
        assert!(serialized.contains("tool_result"));
        assert!(serialized.contains("call-1"));
    }

    #[test]
    fn canonical_tools_are_stably_sorted() {
        let tool = |name: &str| AiTool {
            name: name.to_string(),
            description: String::new(),
            input_schema: json!({"type": "object"}),
            prompt: None,
        };
        let tools = canonical_tool_definitions(&[tool("write"), tool("read")]);
        assert_eq!(tools[0]["name"], "read");
        assert_eq!(tools[1]["name"], "write");
    }

    #[tokio::test]
    async fn configured_tool_call_retains_bounded_retry() {
        let server = Server::http("127.0.0.1:0").expect("test server should bind");
        let url = format!("http://{}", server.server_addr());
        let (request_tx, request_rx) = std_mpsc::channel();
        let server_thread = thread::spawn(move || {
            for attempt in 0..2 {
                let request = server.recv().expect("request should arrive");
                request_tx.send(()).expect("request should be counted");
                if attempt == 0 {
                    request
                        .respond(
                            Response::from_string("capacity")
                                .with_status_code(429)
                                .with_header(retry_after_zero()),
                        )
                        .expect("429 should be sent");
                } else {
                    request
                        .respond(successful_tool_response())
                        .expect("success should be sent");
                }
            }
        });

        let client = openai_test_client(url);
        client
            .call_with_tools_at_reasoning_and_attempt_policy(
                "test-model",
                "system",
                &[user_message()],
                &[],
                128,
                None,
                None,
                None,
                RemoteAttemptPolicy::ConfiguredRetries,
            )
            .await
            .expect("configured tool call should retry once");

        server_thread.join().expect("server thread should finish");
        request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first request should be recorded");
        request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("retry should be recorded");
        assert!(request_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn governed_tool_call_does_not_retry_transient_http_failure() {
        let server = Server::http("127.0.0.1:0").expect("test server should bind");
        let url = format!("http://{}", server.server_addr());
        let (request_tx, request_rx) = std_mpsc::channel();
        let server_thread = thread::spawn(move || {
            let request = server.recv().expect("one request should arrive");
            request_tx.send(()).expect("request should be counted");
            request
                .respond(
                    Response::from_string("capacity")
                        .with_status_code(429)
                        .with_header(retry_after_zero()),
                )
                .expect("429 should be sent");
        });

        let client = openai_test_client(url);
        client
            .call_with_tools_at_reasoning_and_attempt_policy(
                "test-model",
                "system",
                &[user_message()],
                &[],
                128,
                None,
                None,
                None,
                RemoteAttemptPolicy::GovernedSingleAttempt,
            )
            .await
            .expect_err("governed tool call must expose the first failure");

        server_thread.join().expect("server thread should finish");
        request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("one request should be recorded");
        assert!(request_rx.try_recv().is_err());
    }
}
