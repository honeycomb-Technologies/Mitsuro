//! Token budgeting and compaction trigger thresholds.

use crate::ai::client::{AiClient, CallOptions};
use crate::ai::context_policy::ContextBudgetPolicy;
use crate::ai::models::ApiFormat;
use crate::ai::providers::{ProviderCapabilities, ProviderId};
use crate::ai::types::{AiTool, Content, ModelMessage, Role};
use crate::constants;

use super::{DEFAULT_KEEP_RECENT_TOKENS, DEFAULT_RESERVE_TOKENS};

const CHARS_PER_TOKEN_ESTIMATE: usize = 4;
const MESSAGE_FRAMING_TOKENS: usize = 4;
const SYSTEM_SECTION_SEPARATOR: &str = "\n\n---\n\n";
const CODEX_RUNTIME_CONTEXT_PREFIX: &str =
    "[CURRENT RUNTIME CONTEXT]\nThis snapshot supersedes any earlier runtime-context snapshot.\n\n";

/// Component-level estimate of the request Mitsuro is about to render.
///
/// Unlike provider billing usage, this represents the complete logical input:
/// cached prefixes are still part of the model context and therefore still
/// count toward compaction pressure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RenderedRequestTokenEstimate {
    pub base_prompt_tokens: usize,
    pub identity_context_tokens: usize,
    pub project_context_tokens: usize,
    pub session_context_tokens: usize,
    pub message_tokens: usize,
    pub tool_tokens: usize,
    pub total_tokens: usize,
}

impl RenderedRequestTokenEstimate {
    /// Tokens that compaction cannot remove from conversation history.
    pub fn fixed_overhead_tokens(self) -> usize {
        self.base_prompt_tokens
            .saturating_add(self.identity_context_tokens)
            .saturating_add(self.project_context_tokens)
            .saturating_add(self.session_context_tokens)
            .saturating_add(self.tool_tokens)
    }

    pub fn compaction_budget(self, pressure_tokens: usize) -> CompactionRequestBudget {
        CompactionRequestBudget {
            total_tokens: pressure_tokens.max(self.total_tokens),
            fixed_overhead_tokens: self.fixed_overhead_tokens(),
        }
    }
}

/// Caller-supplied request pressure split into reducible and irreducible parts.
///
/// Keeping this structured prevents the compactor from guessing fixed prompt
/// overhead by subtracting a differently-shaped conversation estimate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionRequestBudget {
    pub total_tokens: usize,
    pub fixed_overhead_tokens: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct CompactionManager {
    trigger_tokens: usize,
    target_tokens: usize,
    hard_failure_tokens: usize,
}

impl Default for CompactionManager {
    fn default() -> Self {
        Self::for_model(
            ProviderId::MiniMax,
            ApiFormat::Anthropic,
            constants::ai::DEFAULT_MODEL,
            constants::ai::CONTEXT_WINDOW_TOKENS,
        )
    }
}

impl CompactionManager {
    pub fn for_model(
        provider: ProviderId,
        api_format: ApiFormat,
        model_id: &str,
        context_window: usize,
    ) -> Self {
        let _model_id = model_id;
        let budgets =
            ContextBudgetPolicy::resolve(provider, api_format).compaction_budgets(context_window);

        Self {
            trigger_tokens: budgets.trigger_tokens,
            target_tokens: budgets.target_tokens,
            hard_failure_tokens: budgets.hard_failure_tokens,
        }
    }

    pub fn should_compact(&self, estimated_tokens: usize) -> bool {
        estimated_tokens >= self.trigger_tokens
    }

    pub fn target_tokens(&self) -> usize {
        self.target_tokens
    }

    pub fn trigger_tokens(&self) -> usize {
        self.trigger_tokens
    }

    pub(crate) fn keep_recent_tokens_for_attempt(
        &self,
        estimated_request_tokens: usize,
        raw_conversation_tokens: usize,
        fixed_overhead_tokens: usize,
        attempt: u32,
    ) -> usize {
        let target_history_budget = self
            .target_tokens
            .saturating_sub(fixed_overhead_tokens)
            .saturating_sub(DEFAULT_RESERVE_TOKENS);
        let raw_tail_cap = raw_conversation_tokens.saturating_sub(1).max(1);
        let default_tail = DEFAULT_KEEP_RECENT_TOKENS.min(raw_tail_cap).max(1);
        let base_tail = target_history_budget
            .min(default_tail)
            .min(raw_tail_cap)
            .max(1);
        let hard_pressure = estimated_request_tokens >= self.hard_failure_tokens;

        let requested = match (hard_pressure, attempt.min(3)) {
            (true, 0) => base_tail.saturating_mul(3) / 4,
            (true, 1) => base_tail / 2,
            (true, 2) => base_tail / 4,
            (true, _) => base_tail / 8,
            (false, 0) => base_tail,
            (false, 1) => base_tail.saturating_mul(3) / 4,
            (false, 2) => base_tail / 2,
            (false, _) => base_tail / 4,
        };

        requested.max(1).min(raw_tail_cap)
    }
}

#[cfg(test)]
impl CompactionManager {
    fn with_budgets(
        trigger_tokens: usize,
        target_tokens: usize,
        hard_failure_tokens: usize,
    ) -> Self {
        Self {
            trigger_tokens,
            target_tokens,
            hard_failure_tokens,
        }
    }
}

pub(crate) fn estimate_tokens(messages: &[ModelMessage]) -> usize {
    let total_chars: usize = messages
        .iter()
        .flat_map(|message| &message.content)
        .map(content_char_len)
        .sum();

    total_chars / CHARS_PER_TOKEN_ESTIMATE
}

/// Estimate the full provider request before it is sent.
///
/// The system layers come from the same `AiClient::system_prompt_sections`
/// builder as streaming, and tools are serialized in their provider-specific
/// wire shape. This closes the pre-first-response blind spot where the old
/// estimator counted only conversation messages and ignored fixed prompts and
/// schemas entirely.
pub fn estimate_rendered_request_tokens(
    client: &AiClient,
    messages: &[ModelMessage],
    options: &CallOptions,
) -> RenderedRequestTokenEstimate {
    let model = client.config().model.as_str();
    let options = client.canonical_call_options(model, options);
    let sections = client.system_prompt_sections(
        model,
        messages,
        options.system_prompt.as_deref(),
        options.tools.as_deref(),
    );

    let capabilities = ProviderCapabilities::for_provider(client.provider_id());
    let uses_codex_transport = client.config().uses_chatgpt_codex_format();
    let uses_split_anthropic_blocks = !client.config().uses_openai_format()
        && !client.config().uses_google_format()
        && options.enable_caching
        && capabilities.prompt_caching;

    let mut base_prompt_bytes = sections.base_prompt.len();
    if client.provider_id() == ProviderId::Anthropic
        && crate::auth::is_anthropic_oauth_token(client.api_key())
        && options.enable_caching
        && capabilities.prompt_caching
    {
        base_prompt_bytes = base_prompt_bytes
            .saturating_add("You are Claude Code, Anthropic's official CLI for Claude.".len());
    }

    let identity_context_bytes = sections.identity_context.len().saturating_add(
        usize::from(
            !sections.base_prompt.is_empty()
                && !sections.identity_context.is_empty()
                && !uses_split_anthropic_blocks,
        ) * SYSTEM_SECTION_SEPARATOR.len(),
    );
    let project_context_bytes = sections.project_context.len().saturating_add(
        usize::from(
            !sections.project_context.is_empty()
                && (!sections.base_prompt.is_empty() || !sections.identity_context.is_empty())
                && !uses_split_anthropic_blocks,
        ) * SYSTEM_SECTION_SEPARATOR.len(),
    );
    let uses_runtime_context_message =
        uses_codex_transport || matches!(client.config().api_format, ApiFormat::OpenAIResponses);
    let session_context_bytes =
        sections
            .session_context
            .len()
            .saturating_add(if sections.session_context.is_empty() {
                0
            } else if uses_runtime_context_message {
                CODEX_RUNTIME_CONTEXT_PREFIX.len()
            } else if !uses_split_anthropic_blocks
                && (!sections.base_prompt.is_empty()
                    || !sections.identity_context.is_empty()
                    || !sections.project_context.is_empty())
            {
                SYSTEM_SECTION_SEPARATOR.len()
            } else {
                0
            });

    let base_prompt_tokens = bytes_to_tokens(base_prompt_bytes);
    let identity_context_tokens = bytes_to_tokens(identity_context_bytes);
    let project_context_tokens = bytes_to_tokens(project_context_bytes);
    let session_context_tokens = bytes_to_tokens(session_context_bytes);
    let non_system_messages = messages
        .iter()
        .filter(|message| message.role != Role::System)
        .cloned()
        .collect::<Vec<_>>();
    let message_tokens = estimate_tokens(&non_system_messages).saturating_add(
        non_system_messages
            .len()
            .saturating_mul(MESSAGE_FRAMING_TOKENS),
    );
    let tool_tokens = estimate_tool_wire_tokens(client, &options);
    let total_tokens = base_prompt_tokens
        .saturating_add(identity_context_tokens)
        .saturating_add(project_context_tokens)
        .saturating_add(session_context_tokens)
        .saturating_add(message_tokens)
        .saturating_add(tool_tokens);

    RenderedRequestTokenEstimate {
        base_prompt_tokens,
        identity_context_tokens,
        project_context_tokens,
        session_context_tokens,
        message_tokens,
        tool_tokens,
        total_tokens,
    }
}

/// ChatGPT Codex transport currently has a smaller usable runtime window than
/// some catalog entries advertise. All surfaces must use the same cap.
pub fn effective_context_window_for_runtime(
    uses_chatgpt_codex: bool,
    resolved_context_window: usize,
) -> usize {
    const CHATGPT_CODEX_EFFECTIVE_CONTEXT_WINDOW: usize = 256_000;

    if uses_chatgpt_codex {
        resolved_context_window.min(CHATGPT_CODEX_EFFECTIVE_CONTEXT_WINDOW)
    } else {
        resolved_context_window
    }
}

fn estimate_tool_wire_tokens(client: &AiClient, options: &CallOptions) -> usize {
    let api_format = client.config().api_format;
    let use_hosted_openai_search = client.provider_id() == ProviderId::OpenAI
        && matches!(api_format, ApiFormat::OpenAIResponses)
        && !client.config().uses_chatgpt_codex_format()
        && options.web_search.is_some();

    let mut wire_tools = options
        .tools
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|tool| !(use_hosted_openai_search && tool.name == "web_search"))
        .map(|tool| tool_wire_value(api_format, tool))
        .collect::<Vec<_>>();

    append_native_tool_values(
        &mut wire_tools,
        client.provider_id(),
        api_format,
        options,
        use_hosted_openai_search,
    );

    if wire_tools.is_empty() {
        return 0;
    }

    let wire_payload = if matches!(api_format, ApiFormat::Google) {
        serde_json::json!([{"functionDeclarations": wire_tools}])
    } else {
        serde_json::Value::Array(wire_tools)
    };

    serde_json::to_vec(&wire_payload)
        .map(|bytes| bytes_to_tokens(bytes.len()))
        .unwrap_or_default()
}

fn tool_wire_value(api_format: ApiFormat, tool: &AiTool) -> serde_json::Value {
    match api_format {
        ApiFormat::Anthropic => serde_json::json!({
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.input_schema,
        }),
        ApiFormat::OpenAIResponses => serde_json::json!({
            "type": "function",
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema,
        }),
        ApiFormat::Google => serde_json::json!({
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.input_schema,
        }),
        ApiFormat::OpenAI => serde_json::json!({
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
            }
        }),
    }
}

fn append_native_tool_values(
    wire_tools: &mut Vec<serde_json::Value>,
    provider: ProviderId,
    api_format: ApiFormat,
    options: &CallOptions,
    use_hosted_openai_search: bool,
) {
    if use_hosted_openai_search {
        wire_tools.push(serde_json::json!({ "type": "web_search" }));
    }

    let capabilities = ProviderCapabilities::for_provider(provider);
    if provider == ProviderId::Anthropic {
        if capabilities.web_search {
            if let Some(search) = &options.web_search {
                let mut spec = serde_json::json!({
                    "type": "web_search_20250305",
                    "name": "web_search",
                });
                if let Some(max_uses) = search.max_uses {
                    spec["max_uses"] = serde_json::json!(max_uses);
                }
                wire_tools.push(spec);
            }
        }
        if capabilities.web_fetch {
            if let Some(fetch) = &options.web_fetch {
                let mut spec = serde_json::json!({
                    "type": "web_fetch_20250910",
                    "name": "web_fetch",
                    "citations": { "enabled": fetch.citations_enabled },
                });
                if let Some(max_uses) = fetch.max_uses {
                    spec["max_uses"] = serde_json::json!(max_uses);
                }
                if let Some(max_tokens) = fetch.max_content_tokens {
                    spec["max_content_tokens"] = serde_json::json!(max_tokens);
                }
                wire_tools.push(spec);
            }
        }
    } else if provider == ProviderId::OpenRouter && matches!(api_format, ApiFormat::Anthropic) {
        if capabilities.web_search && options.web_search.is_some() {
            wire_tools.push(serde_json::json!({ "type": "openrouter:web_search" }));
        }
        if capabilities.web_fetch {
            if let Some(fetch) = &options.web_fetch {
                let mut parameters = serde_json::Map::new();
                if let Some(max_uses) = fetch.max_uses {
                    parameters.insert("max_uses".to_string(), serde_json::json!(max_uses));
                }
                if let Some(max_tokens) = fetch.max_content_tokens {
                    parameters.insert(
                        "max_content_tokens".to_string(),
                        serde_json::json!(max_tokens),
                    );
                }
                let mut spec = serde_json::json!({ "type": "openrouter:web_fetch" });
                if !parameters.is_empty() {
                    spec["parameters"] = serde_json::Value::Object(parameters);
                }
                wire_tools.push(spec);
            }
        }
    }
}

fn bytes_to_tokens(bytes: usize) -> usize {
    bytes.saturating_add(CHARS_PER_TOKEN_ESTIMATE - 1) / CHARS_PER_TOKEN_ESTIMATE
}

pub fn estimate_with_usage(
    messages: &[ModelMessage],
    last_usage_prompt_tokens: Option<usize>,
    messages_after_usage: usize,
) -> usize {
    if let Some(usage_tokens) = last_usage_prompt_tokens {
        let tail_start = messages.len().saturating_sub(messages_after_usage);
        let tail_estimate = estimate_tokens(&messages[tail_start..]);
        return usage_tokens.saturating_add(tail_estimate);
    }
    estimate_tokens(messages)
}

fn content_char_len(content: &Content) -> usize {
    match content {
        Content::Text { text, citations } => crate::ai::format::provider_text_len(text, citations),
        Content::ToolUse { name, input, .. } => name.len() + input.to_string().len(),
        Content::ToolResult { output, .. } => output.to_string().len(),
        Content::Image { .. } => 1_000,
        Content::Document { .. } => 5_000,
        Content::Thinking { thinking, .. } => thinking.len(),
        Content::RedactedThinking { .. } => 100,
    }
}

#[cfg(test)]
mod tests {
    use super::{estimate_rendered_request_tokens, estimate_tokens, CompactionManager};
    use crate::ai::client::{AiClient, AiClientConfig, CallOptions};
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;
    use crate::ai::types::{AiTool, Content, DocumentSource, ModelMessage, Role};

    #[test]
    fn citation_context_counts_toward_the_actual_rendered_history_budget() {
        let citations = vec![crate::ai::types::Citation {
            url: "https://example.com/docs".into(),
            title: "Docs".into(),
            cited_text: "Reference detail ".repeat(300),
        }];
        let expected_bytes = crate::ai::format::provider_text("Answer", &citations).len();
        let messages = vec![ModelMessage {
            role: Role::Assistant,
            content: vec![Content::Text {
                text: "Answer".into(),
                citations,
            }],
        }];
        assert_eq!(estimate_tokens(&messages), expected_bytes / 4);
        assert!(estimate_tokens(&messages) > "Answer".len() / 4);
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".into(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            "test-key".into(),
        );
        let request = estimate_rendered_request_tokens(&client, &messages, &CallOptions::default());
        assert_eq!(request.message_tokens, expected_bytes / 4 + 4);
    }

    #[test]
    fn estimate_tokens_counts_mixed_content() {
        let messages = vec![
            ModelMessage {
                role: Role::User,
                content: vec![Content::text("abcdabcd".to_string())],
            },
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::Document {
                    source: DocumentSource {
                        source_type: "base64".to_string(),
                        media_type: "application/pdf".to_string(),
                        data: Some("ignored".to_string()),
                        url: None,
                    },
                }],
            },
        ];

        assert_eq!(estimate_tokens(&messages), (8 + 5_000) / 4);
    }

    #[test]
    fn should_compact_at_trigger_threshold() {
        let manager = CompactionManager::with_budgets(1_000, 800, 1_200);

        assert!(!manager.should_compact(999));
        assert!(manager.should_compact(1_000));
        assert!(manager.should_compact(1_100));
    }

    #[test]
    fn keep_budgets_shrink_monotonically_and_respect_fixed_overhead() {
        let manager = CompactionManager::with_budgets(80_000, 50_000, 95_000);
        let attempts = (0..4)
            .map(|attempt| manager.keep_recent_tokens_for_attempt(90_000, 70_000, 20_000, attempt))
            .collect::<Vec<_>>();

        assert!(attempts.windows(2).all(|pair| pair[1] < pair[0]));
        assert!(attempts[0] <= 50_000 - 20_000 - super::DEFAULT_RESERVE_TOKENS);
    }

    #[test]
    fn rendered_request_estimate_includes_fixed_prompt_and_wire_tools() {
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".to_string(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            "test-key".to_string(),
        );
        let messages = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::text("hello".to_string())],
        }];
        let without_tools =
            estimate_rendered_request_tokens(&client, &messages, &CallOptions::default());
        let with_tools = estimate_rendered_request_tokens(
            &client,
            &messages,
            &CallOptions {
                tools: Some(vec![AiTool {
                    name: "read".to_string(),
                    description: "Read a file safely.".repeat(20),
                    input_schema: serde_json::json!({
                        "type": "object",
                        "properties": {"file_path": {"type": "string"}},
                        "required": ["file_path"]
                    }),
                    prompt: None,
                }]),
                ..Default::default()
            },
        );

        assert!(without_tools.base_prompt_tokens > 0);
        assert!(without_tools.total_tokens > without_tools.message_tokens);
        assert!(with_tools.tool_tokens > without_tools.tool_tokens);
        assert_eq!(
            with_tools.fixed_overhead_tokens(),
            with_tools.base_prompt_tokens
                + with_tools.identity_context_tokens
                + with_tools.project_context_tokens
                + with_tools.session_context_tokens
                + with_tools.tool_tokens
        );
        assert_eq!(
            with_tools.total_tokens,
            with_tools.base_prompt_tokens
                + with_tools.identity_context_tokens
                + with_tools.project_context_tokens
                + with_tools.session_context_tokens
                + with_tools.message_tokens
                + with_tools.tool_tokens
        );
    }

    #[test]
    fn rendered_request_estimate_counts_frozen_hive_identity_context() {
        let client = AiClient::new(
            AiClientConfig {
                model: "gpt-5.5".to_string(),
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAIResponses,
                ..Default::default()
            },
            "test-key".to_string(),
        );
        let hive_identity = format!(
            "[HIVE SOUL - profile-id=test]\n{}",
            "personality and behavioral continuity ".repeat(128)
        );
        let user_message = ModelMessage {
            role: Role::User,
            content: vec![Content::text("hello".to_string())],
        };
        let without_identity = estimate_rendered_request_tokens(
            &client,
            std::slice::from_ref(&user_message),
            &CallOptions::default(),
        );
        let with_identity = estimate_rendered_request_tokens(
            &client,
            &[
                ModelMessage {
                    role: Role::System,
                    content: vec![Content::text(hive_identity.clone())],
                },
                user_message,
            ],
            &CallOptions::default(),
        );

        let expected_identity_tokens =
            super::bytes_to_tokens(hive_identity.len() + super::SYSTEM_SECTION_SEPARATOR.len());
        assert_eq!(
            with_identity.identity_context_tokens,
            expected_identity_tokens
        );
        assert_eq!(without_identity.identity_context_tokens, 0);
        assert_eq!(
            with_identity.total_tokens,
            without_identity.total_tokens + expected_identity_tokens
        );
        assert_eq!(
            with_identity.fixed_overhead_tokens(),
            without_identity.fixed_overhead_tokens() + expected_identity_tokens
        );
    }
}
