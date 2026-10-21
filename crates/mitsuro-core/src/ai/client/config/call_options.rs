use crate::ai::models::{
    resolve_model_metadata, ApiFormat, ModelCapabilities, ResolvedModelRuntime,
};
use crate::ai::providers::{
    FastMode, ProviderCapabilities, ProviderId, ReasoningControl, ReasoningEffort, ReasoningFormat,
};
use crate::ai::types::{
    AiTool, ContextManagement, ThinkingConfig, WebFetchConfig, WebSearchConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::effort::{AnthropicAdaptiveEffort, CodexReasoningEffort};
use super::parallel_tools::{resolve_parallel_tool_calls, ParallelToolCallTransport};

/// Provider prompt-cache lifetime preference.
///
/// `Standard` uses OpenAI's model default (30 minutes minimum on GPT-5.6+) and
/// Anthropic's default 5 minutes. `Extended` requests 24 hours only on the
/// earlier OpenAI families that support it and 1 hour on Anthropic. GPT-5.6+
/// remains fixed at its only supported `30m` TTL.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PromptCacheRetention {
    #[default]
    Standard,
    Extended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenAiPromptCacheMode {
    Implicit,
    Explicit,
}

/// Redacted, provider-resolved request settings suitable for diagnostics and
/// persisted runtime evidence. Credentials, prompts, message contents and tool
/// schemas are deliberately excluded.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EffectiveRequestSettings {
    pub provider: String,
    pub model: String,
    pub api_format: String,
    pub max_tokens: Option<usize>,
    pub temperature: Option<f32>,
    pub tool_count: usize,
    pub thinking_enabled: bool,
    pub reasoning_format: Option<String>,
    pub reasoning_control: Option<String>,
    pub reasoning_effort: Option<String>,
    pub parallel_tool_calls: bool,
    pub caching_enabled: bool,
    pub fast_mode: bool,
    pub service_tier: Option<String>,
    pub hosted_web_search: bool,
    pub hosted_web_fetch: bool,
    pub context_management: bool,
    pub warnings: Vec<String>,
}

impl OpenAiPromptCacheMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Implicit => "implicit",
            Self::Explicit => "explicit",
        }
    }
}

impl PromptCacheRetention {
    fn from_env() -> Self {
        crate::identity::env_var("MITSURO_CACHE_RETENTION")
            .ok()
            .as_deref()
            .map(Self::from_config_value)
            .unwrap_or_default()
    }

    fn from_config_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "long" | "extended" | "24h" | "1h" => Self::Extended,
            _ => Self::Standard,
        }
    }
}

fn normalize_reasoning_effort(
    requested: Option<ReasoningEffort>,
    capabilities: &ModelCapabilities,
    control: Option<ReasoningControl>,
) -> Option<ReasoningEffort> {
    if !capabilities.supports_thinking || control == Some(ReasoningControl::OutputOnly) {
        return None;
    }

    let mut supported = capabilities.supported_reasoning_levels.clone();
    supported.dedup();
    let fallback = capabilities
        .default_reasoning_level
        .filter(|level| *level != ReasoningEffort::None)
        .or_else(|| {
            supported
                .iter()
                .copied()
                .find(|level| *level != ReasoningEffort::None)
        })
        .unwrap_or(ReasoningEffort::Medium);

    let mut requested = requested?;
    if requested == ReasoningEffort::None {
        return if capabilities.reasoning_is_mandatory {
            Some(fallback)
        } else {
            Some(ReasoningEffort::None)
        };
    }
    if supported.is_empty() || supported.contains(&requested) {
        return Some(requested);
    }
    if requested == ReasoningEffort::Ultra && supported.contains(&ReasoningEffort::Max) {
        requested = ReasoningEffort::Max;
        return Some(requested);
    }
    Some(fallback)
}

const fn codex_effort_for_reasoning_level(level: ReasoningEffort) -> Option<CodexReasoningEffort> {
    Some(match level {
        ReasoningEffort::None => return None,
        ReasoningEffort::Minimal => CodexReasoningEffort::Minimal,
        ReasoningEffort::Low => CodexReasoningEffort::Low,
        ReasoningEffort::Medium => CodexReasoningEffort::Medium,
        ReasoningEffort::High => CodexReasoningEffort::High,
        ReasoningEffort::XHigh => CodexReasoningEffort::XHigh,
        ReasoningEffort::Max | ReasoningEffort::Ultra => CodexReasoningEffort::Max,
    })
}

const fn anthropic_effort_for_reasoning_level(
    level: ReasoningEffort,
) -> Option<AnthropicAdaptiveEffort> {
    Some(match level {
        ReasoningEffort::None => return None,
        ReasoningEffort::Minimal | ReasoningEffort::Low => AnthropicAdaptiveEffort::Low,
        ReasoningEffort::Medium => AnthropicAdaptiveEffort::Medium,
        ReasoningEffort::High => AnthropicAdaptiveEffort::High,
        ReasoningEffort::XHigh => AnthropicAdaptiveEffort::XHigh,
        ReasoningEffort::Max | ReasoningEffort::Ultra => AnthropicAdaptiveEffort::Max,
    })
}

const fn reasoning_level_for_codex_effort(level: CodexReasoningEffort) -> ReasoningEffort {
    match level {
        CodexReasoningEffort::Minimal => ReasoningEffort::Minimal,
        CodexReasoningEffort::Low => ReasoningEffort::Low,
        CodexReasoningEffort::Medium => ReasoningEffort::Medium,
        CodexReasoningEffort::High => ReasoningEffort::High,
        CodexReasoningEffort::XHigh => ReasoningEffort::XHigh,
        CodexReasoningEffort::Max => ReasoningEffort::Max,
    }
}

const fn reasoning_level_for_anthropic_effort(level: AnthropicAdaptiveEffort) -> ReasoningEffort {
    match level {
        AnthropicAdaptiveEffort::Low => ReasoningEffort::Low,
        AnthropicAdaptiveEffort::Medium => ReasoningEffort::Medium,
        AnthropicAdaptiveEffort::High => ReasoningEffort::High,
        AnthropicAdaptiveEffort::XHigh => ReasoningEffort::XHigh,
        AnthropicAdaptiveEffort::Max => ReasoningEffort::Max,
    }
}

/// Call options for API requests
#[derive(Debug, Clone)]
pub struct CallOptions {
    pub max_tokens: Option<usize>,
    pub temperature: Option<f32>,
    pub tools: Option<Vec<AiTool>>,
    pub system_prompt: Option<String>,
    /// Extended thinking configuration (Anthropic-style)
    pub thinking: Option<ThinkingConfig>,
    /// Reasoning capability advertised by the selected model. This does not
    /// enable reasoning; `thinking` remains the single source of truth for the
    /// request's active/off state.
    pub reasoning_format: Option<ReasoningFormat>,
    /// Catalog-selected wire control. When present, this is authoritative over
    /// model-name heuristics and keeps dynamically discovered models usable.
    pub reasoning_control: Option<ReasoningControl>,
    /// Provider-independent reasoning level selected for this exact run.
    ///
    /// `None` preserves legacy/unspecified behavior. An explicit
    /// `Some(ReasoningEffort::None)` is an Off request which mandatory-
    /// reasoning models normalize against their immutable capability snapshot.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Enable prompt caching (default: true)
    pub enable_caching: bool,
    /// Requested provider prompt-cache lifetime. Defaults from
    /// `MITSURO_CACHE_RETENTION` (`long`/`extended` opt in to extended caching).
    pub prompt_cache_retention: PromptCacheRetention,
    /// Context management for automatic clearing of old content
    pub context_management: Option<ContextManagement>,
    /// Web search configuration (server-executed)
    pub web_search: Option<WebSearchConfig>,
    /// Web fetch configuration (server-executed, beta)
    pub web_fetch: Option<WebFetchConfig>,
    /// Transport/conversation identity used for provider affinity and stateful continuation.
    pub session_id: Option<String>,
    /// Optional prompt-cache routing scope. When omitted, `session_id` remains
    /// the backward-compatible cache key. Keeping this separate allows related
    /// requests to share an immutable prompt prefix without sharing transport
    /// or continuation state.
    pub prompt_cache_key: Option<String>,
    /// Codex-specific reasoning effort
    pub codex_reasoning_effort: Option<CodexReasoningEffort>,
    /// Explicit Responses/Codex tool parallelism override. `None` preserves
    /// regular Responses' enabled default and Codex's legacy toggle below.
    /// This is request policy, not a guarantee about local tool scheduling.
    pub parallel_tool_calls: Option<bool>,
    /// Compatibility input for Codex only, used when the shared override is absent.
    pub codex_parallel_tool_calls: bool,
    /// Anthropic Opus 4.6 adaptive thinking effort
    pub anthropic_adaptive_effort: Option<AnthropicAdaptiveEffort>,
    /// Request provider priority/fast service tier without changing the selected model.
    pub fast_mode: bool,
    /// Resolved model-specific fast strategy. Callers with dynamic catalog
    /// metadata may set this directly; canonicalization otherwise uses the
    /// curated capability overlay.
    pub fast_mode_format: Option<FastMode>,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            max_tokens: None,
            temperature: None,
            tools: None,
            system_prompt: None,
            thinking: None,
            reasoning_format: None,
            reasoning_control: None,
            reasoning_effort: None,
            enable_caching: true,
            prompt_cache_retention: PromptCacheRetention::from_env(),
            context_management: None,
            web_search: None,
            web_fetch: None,
            session_id: None,
            prompt_cache_key: None,
            codex_reasoning_effort: None,
            parallel_tool_calls: None,
            codex_parallel_tool_calls: false,
            anthropic_adaptive_effort: None,
            fast_mode: false,
            fast_mode_format: None,
        }
    }
}

impl CallOptions {
    /// Resolve this request and return a safe explanation of the effective
    /// settings. This is the canonical source for `debug request` output and
    /// makes capability downgrades observable instead of silent.
    pub fn effective_request_settings(
        &self,
        provider: ProviderId,
        model: &str,
        api_format: ApiFormat,
    ) -> EffectiveRequestSettings {
        let canonical = self.canonicalized_for(provider, model, api_format);
        self.effective_settings_from_canonical(provider, model, api_format, &canonical)
    }

    fn effective_settings_from_canonical(
        &self,
        provider: ProviderId,
        model: &str,
        api_format: ApiFormat,
        canonical: &Self,
    ) -> EffectiveRequestSettings {
        let mut warnings = Vec::new();

        if self.fast_mode && !canonical.fast_mode {
            warnings.push("fast mode was requested but is unsupported by this model".to_string());
        }
        if self.thinking.is_some() && canonical.thinking.is_none() {
            warnings.push(
                "reasoning was requested but the selected transport cannot accept it".to_string(),
            );
        }
        if self.codex_reasoning_effort.is_some() && canonical.codex_reasoning_effort.is_none() {
            warnings.push("reasoning effort was removed by model capability policy".to_string());
        }
        if (self.parallel_tool_calls == Some(true)
            || (self.parallel_tool_calls.is_none() && self.codex_parallel_tool_calls))
            && !matches!(api_format, ApiFormat::OpenAIResponses)
        {
            warnings.push("parallel tool calls are unsupported by the selected API".to_string());
        }
        if self.context_management.is_some() && canonical.context_management.is_none() {
            warnings.push("provider context management is unsupported".to_string());
        }
        if self.web_search.is_some() && canonical.web_search.is_none() {
            warnings.push("hosted web search is unsupported; use the portable tool".to_string());
        }
        if self.web_fetch.is_some() && canonical.web_fetch.is_none() {
            warnings.push("hosted web fetch is unsupported; use the portable tool".to_string());
        }

        let mut settings = EffectiveRequestSettings {
            provider: provider.storage_key().to_string(),
            model: model.to_string(),
            api_format: format!("{api_format:?}"),
            max_tokens: canonical.max_tokens,
            temperature: canonical.temperature,
            tool_count: canonical.tools.as_ref().map_or(0, Vec::len),
            thinking_enabled: canonical.thinking.is_some(),
            reasoning_format: canonical.reasoning_format.map(|value| format!("{value:?}")),
            reasoning_control: canonical
                .reasoning_control
                .map(|value| format!("{value:?}")),
            reasoning_effort: canonical.reasoning_effort.map(|value| format!("{value:?}")),
            // Without client transport configuration this API describes regular
            // Responses. AiClient diagnostics resolves the exact OAuth/API path.
            parallel_tool_calls: resolve_parallel_tool_calls(
                Some(canonical),
                if matches!(api_format, ApiFormat::OpenAIResponses) {
                    ParallelToolCallTransport::Responses
                } else {
                    ParallelToolCallTransport::Uncontrolled
                },
                canonical
                    .tools
                    .as_ref()
                    .is_some_and(|tools| !tools.is_empty())
                    || canonical.web_search.is_some(),
            )
            .unwrap_or(false),
            caching_enabled: canonical.enable_caching,
            fast_mode: canonical.fast_mode,
            service_tier: canonical
                .service_tier_for_provider(provider)
                .map(ToString::to_string),
            hosted_web_search: canonical.web_search.is_some(),
            hosted_web_fetch: canonical.web_fetch.is_some(),
            context_management: canonical.context_management.is_some(),
            warnings,
        };
        settings.warnings.sort();
        settings.warnings.dedup();
        settings
    }

    /// Resolve request policy against the exact catalog row frozen for this
    /// run. This is the authoritative path for production clients.
    pub fn effective_request_settings_for_runtime(
        &self,
        runtime: &ResolvedModelRuntime,
    ) -> EffectiveRequestSettings {
        let canonical = self.canonicalized_for_runtime(runtime);
        let mut settings = self.effective_settings_from_canonical(
            runtime.key.provider,
            &runtime.wire_model_id,
            runtime.key.api_format,
            &canonical,
        );

        if self.tools.is_some() && canonical.tools.is_none() {
            settings.warnings.push(
                "tools were removed because the selected model does not advertise tool calling"
                    .to_string(),
            );
        }
        if self
            .max_tokens
            .is_some_and(|requested| requested > runtime.capabilities.max_output)
        {
            settings.warnings.push(format!(
                "max_tokens was capped at the model limit ({})",
                runtime.capabilities.max_output
            ));
        }

        settings.warnings.sort();
        settings.warnings.dedup();
        settings
    }

    /// Canonicalize against immutable run-scoped capabilities rather than
    /// model-name inference or a mutable global catalog.
    pub fn canonicalized_for_runtime(&self, runtime: &ResolvedModelRuntime) -> Self {
        self.canonicalized_with_capabilities(
            runtime.key.provider,
            runtime.key.api_format,
            &runtime.capabilities,
        )
    }

    /// Build a canonicalized options set for a specific provider/model pipeline.
    ///
    /// This prevents per-surface drift by enforcing provider capabilities and
    /// model reasoning constraints before request construction.
    pub fn canonicalized_for(
        &self,
        provider: ProviderId,
        model: &str,
        api_format: ApiFormat,
    ) -> Self {
        let inferred = resolve_model_metadata(provider, model, api_format);
        self.canonicalized_with_capabilities(
            provider,
            api_format,
            &inferred.resolve_runtime().capabilities,
        )
    }

    fn canonicalized_with_capabilities(
        &self,
        provider: ProviderId,
        api_format: ApiFormat,
        model_capabilities: &ModelCapabilities,
    ) -> Self {
        let caps = ProviderCapabilities::for_provider(provider);
        let mut options = self.clone();

        if options.fast_mode {
            options.fast_mode_format = options.fast_mode_format.or(model_capabilities.fast_mode);
            if options.fast_mode_format.is_none() {
                options.fast_mode = false;
            }
        } else {
            options.fast_mode_format = None;
        }

        let authoritative_reasoning_metadata = options.reasoning_control.is_some();
        let resolved_reasoning_format = if authoritative_reasoning_metadata {
            options.reasoning_format
        } else {
            model_capabilities.reasoning_format
        };
        let resolved_reasoning_control = options
            .reasoning_control
            .or(model_capabilities.reasoning_control);
        // Provider-specific fields are compatibility readers. Translate them
        // once into the shared typed contract, then regenerate the exact wire
        // control from the canonical value below.
        let requested_reasoning_effort = options
            .reasoning_effort
            .or_else(|| {
                options
                    .codex_reasoning_effort
                    .map(reasoning_level_for_codex_effort)
            })
            .or_else(|| {
                options
                    .anthropic_adaptive_effort
                    .map(reasoning_level_for_anthropic_effort)
            });
        options.reasoning_effort = normalize_reasoning_effort(
            requested_reasoning_effort,
            model_capabilities,
            resolved_reasoning_control,
        );

        if options.reasoning_effort == Some(ReasoningEffort::None) {
            options.thinking = None;
        } else if options.reasoning_effort.is_some() {
            options.thinking = Some(ThinkingConfig::default());
        }

        if resolved_reasoning_control == Some(ReasoningControl::OutputOnly) {
            // Output-only transports (classic Grok Build / Composer rows) can
            // emit reasoning blocks but reject explicit effort controls. Keep
            // parsing reasoning output; never send a thinking knob.
            options.reasoning_format = resolved_reasoning_format;
            options.reasoning_control = Some(ReasoningControl::OutputOnly);
            options.thinking = None;
            options.reasoning_effort = None;
            options.codex_reasoning_effort = None;
            options.anthropic_adaptive_effort = None;
        } else if resolved_reasoning_format.is_none() {
            options.reasoning_format = None;
            options.reasoning_control = None;
            options.thinking = None;
            options.reasoning_effort = None;
            options.codex_reasoning_effort = None;
            options.anthropic_adaptive_effort = None;
        } else {
            options.reasoning_format = resolved_reasoning_format;
            options.reasoning_control = resolved_reasoning_control;

            // Mandatory-reasoning models cannot honor an Off request. Internal
            // call paths that bypass the UI still need a valid request.
            if model_capabilities.reasoning_is_mandatory && options.thinking.is_none() {
                options.thinking = Some(ThinkingConfig::default());
            }

            match options.reasoning_control {
                Some(ReasoningControl::OpenAiEffort) => {
                    options.codex_reasoning_effort = options
                        .reasoning_effort
                        .and_then(codex_effort_for_reasoning_level);
                    options.anthropic_adaptive_effort = None;
                }
                Some(ReasoningControl::AnthropicAdaptive) => {
                    options.anthropic_adaptive_effort = options
                        .reasoning_effort
                        .and_then(anthropic_effort_for_reasoning_level);
                    options.codex_reasoning_effort = None;
                }
                Some(ReasoningControl::AnthropicBudget | ReasoningControl::Boolean) | None => {
                    options.codex_reasoning_effort = None;
                    options.anthropic_adaptive_effort = None;
                }
                Some(ReasoningControl::OutputOnly) => unreachable!("handled above"),
            }

            if options.thinking.is_none() {
                options.reasoning_effort = None;
                options.codex_reasoning_effort = None;
                options.anthropic_adaptive_effort = None;
            }
        }

        if !caps.context_management {
            options.context_management = None;
        }

        if !model_capabilities.supports_tools {
            options.tools = None;
            options.web_search = None;
            options.web_fetch = None;
        }

        if let Some(max_tokens) = options.max_tokens.as_mut() {
            *max_tokens = (*max_tokens).min(model_capabilities.max_output);
        }

        let hosted_web_search = model_capabilities.supports_tools
            && provider_hosted_web_search_supported(provider, api_format, &caps);
        let hosted_web_fetch = model_capabilities.supports_tools && caps.web_fetch;

        if !hosted_web_search {
            options.web_search = None;
        }
        if !hosted_web_fetch {
            options.web_fetch = None;
        }

        // When a provider-native hosted web tool is active, remove the local
        // function tool with the same name to avoid duplicate tool definitions.
        // Unsupported providers keep the local portable web tools instead.
        // OpenAI transport selection happens after canonicalization; it removes
        // the local web_search function only on the standard hosted Responses path.
        if options.web_search.is_some() && provider != ProviderId::OpenAI {
            remove_ai_tool_named(&mut options.tools, "web_search");
        }
        if options.web_fetch.is_some() {
            remove_ai_tool_named(&mut options.tools, "web_fetch");
        }

        if !(matches!(provider, ProviderId::OpenAI | ProviderId::Grok)
            && matches!(api_format, ApiFormat::OpenAIResponses))
        {
            options.codex_parallel_tool_calls = false;
        }

        options
    }

    /// Map Mitsuro's provider-independent fast-mode toggle to provider wire values.
    ///
    /// This intentionally does not mutate the model ID: `gpt-5.5` and
    /// `gpt-5.5-mini` can both run in standard or fast service tiers.
    pub fn service_tier_for_provider(&self, provider: ProviderId) -> Option<&'static str> {
        match (provider, self.fast_mode, self.fast_mode_format) {
            (
                ProviderId::OpenAI | ProviderId::OpenRouter | ProviderId::MiniMax,
                true,
                Some(FastMode::Priority),
            ) => Some("priority"),
            _ => None,
        }
    }

    /// Whether this request must use Anthropic's Fast Mode body/header pair.
    pub fn uses_anthropic_fast_mode(&self, provider: ProviderId) -> bool {
        provider == ProviderId::Anthropic
            && self.fast_mode
            && self.fast_mode_format == Some(FastMode::AnthropicFast)
    }
}

fn provider_hosted_web_search_supported(
    provider: ProviderId,
    api_format: ApiFormat,
    caps: &ProviderCapabilities,
) -> bool {
    match provider {
        ProviderId::OpenAI => caps.web_search && matches!(api_format, ApiFormat::OpenAIResponses),
        ProviderId::OpenRouter => caps.web_plugins || caps.web_search,
        _ => caps.web_search,
    }
}

fn remove_ai_tool_named(tools: &mut Option<Vec<AiTool>>, name: &str) {
    if let Some(items) = tools {
        items.retain(|tool| tool.name != name);
        if items.is_empty() {
            *tools = None;
        }
    }
}

const MAX_PROMPT_CACHE_KEY_CHARS: usize = 64;

/// Normalize a provider prompt-cache key without creating prefix collisions.
///
/// OpenAI accepts at most 64 characters. Preserve already-valid identifiers so
/// existing sessions keep their cache affinity, but hash the complete UTF-8
/// value when it is longer instead of truncating a potentially shared prefix.
pub(crate) fn normalize_prompt_cache_key(key: &str) -> Option<String> {
    if key.is_empty() {
        return None;
    }
    if key.chars().count() <= MAX_PROMPT_CACHE_KEY_CHARS {
        return Some(key.to_string());
    }

    Some(format!("{:x}", Sha256::digest(key.as_bytes())))
}

/// Resolve the normalized cache key for a call, honoring the caching toggle.
pub(crate) fn normalized_prompt_cache_key(options: &CallOptions) -> Option<String> {
    if !options.enable_caching {
        return None;
    }
    options
        .prompt_cache_key
        .as_deref()
        .or(options.session_id.as_deref())
        .and_then(normalize_prompt_cache_key)
}

/// Build the current OpenAI cache-options envelope when that model supports it.
pub(crate) fn openai_prompt_cache_options(
    options: &CallOptions,
    model: &str,
    mode: OpenAiPromptCacheMode,
) -> Option<Value> {
    if normalized_prompt_cache_key(options).is_none() || !supports_prompt_cache_options(model) {
        return None;
    }

    Some(serde_json::json!({
        "mode": mode.as_str(),
        "ttl": "30m",
    }))
}

/// Resolve the deprecated extended-retention field for earlier model families.
/// GPT-5.6+ must use `prompt_cache_options.ttl = "30m"` instead.
pub(crate) fn openai_prompt_cache_retention(options: &CallOptions, model: &str) -> Option<Value> {
    if options.prompt_cache_retention != PromptCacheRetention::Extended
        || normalized_prompt_cache_key(options).is_none()
        || !supports_extended_prompt_cache_retention(model)
    {
        return None;
    }

    Some(serde_json::json!("24h"))
}

fn supports_extended_prompt_cache_retention(model: &str) -> bool {
    let model = model.rsplit('/').next().unwrap_or(model);
    const SUPPORTED_FAMILIES: &[&str] = &[
        "gpt-5.5-pro",
        "gpt-5.5",
        "gpt-5.4",
        "gpt-5.2",
        "gpt-5.1-codex-max",
        "gpt-5.1-codex-mini",
        "gpt-5.1-codex",
        "gpt-5.1-chat-latest",
        "gpt-5.1",
        "gpt-5-codex",
        "gpt-5",
        "gpt-4.1",
    ];

    SUPPORTED_FAMILIES.iter().any(|family| {
        model == *family
            || model.strip_prefix(family).is_some_and(|suffix| {
                suffix
                    .strip_prefix('-')
                    .and_then(|suffix| suffix.chars().next())
                    .is_some_and(|character| character.is_ascii_digit())
            })
    })
}

/// Build Anthropic-style cache control for capable transports. The documented
/// one-hour TTL is sent only to Anthropic itself; compatible proxies retain
/// their existing default policy rather than receiving an assumed extension.
pub(crate) fn anthropic_prompt_cache_control(
    options: &CallOptions,
    provider: ProviderId,
) -> Option<Value> {
    let capabilities = ProviderCapabilities::for_provider(provider);
    if !options.enable_caching || !capabilities.prompt_caching {
        return None;
    }

    let mut control = serde_json::json!({"type": "ephemeral"});
    if options.prompt_cache_retention == PromptCacheRetention::Extended
        && provider == ProviderId::Anthropic
    {
        control["ttl"] = serde_json::json!("1h");
    }
    Some(control)
}

/// GPT-5.6 introduced request-wide prompt cache options and explicit
/// breakpoints. Earlier models reject those fields.
pub(crate) fn supports_prompt_cache_options(model: &str) -> bool {
    let model = model.rsplit('/').next().unwrap_or(model);
    let Some(version) = model.strip_prefix("gpt-") else {
        return false;
    };
    let numeric = version
        .chars()
        .take_while(|character| character.is_ascii_digit() || *character == '.')
        .collect::<String>();
    let mut parts = numeric.split('.');
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u32>().ok());
    matches!((major, minor), (Some(major), Some(minor)) if (major, minor) >= (5, 6))
        || matches!(major, Some(major) if major > 5)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        anthropic_prompt_cache_control, normalize_prompt_cache_key, normalized_prompt_cache_key,
        openai_prompt_cache_options, openai_prompt_cache_retention, supports_prompt_cache_options,
        CallOptions, OpenAiPromptCacheMode, PromptCacheRetention,
    };
    use crate::ai::client::config::CodexReasoningEffort;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::{
        FastMode, ProviderId, ReasoningControl, ReasoningEffort, ReasoningFormat,
    };
    use crate::ai::types::{
        AiTool, ContextManagement, ThinkingConfig, WebFetchConfig, WebSearchConfig,
    };

    fn tool(name: &str) -> AiTool {
        AiTool {
            name: name.to_string(),
            description: format!("{name} description"),
            input_schema: json!({"type": "object"}),
            prompt: None,
        }
    }

    #[test]
    fn prompt_cache_options_are_model_gated() {
        assert!(!supports_prompt_cache_options("gpt-5.5"));
        assert!(supports_prompt_cache_options("gpt-5.6"));
        assert!(supports_prompt_cache_options("openai/gpt-5.7-codex"));
        assert!(supports_prompt_cache_options("gpt-6"));
        assert!(!supports_prompt_cache_options("claude-opus-4-6"));
    }

    #[test]
    fn prompt_cache_key_preserves_short_and_boundary_values() {
        let short = "session:01JZ-example";
        assert_eq!(normalize_prompt_cache_key(short).as_deref(), Some(short));

        let boundary = "x".repeat(64);
        assert_eq!(normalize_prompt_cache_key(&boundary), Some(boundary));
        assert_eq!(normalize_prompt_cache_key(""), None);
    }

    #[test]
    fn prompt_cache_key_hashes_the_complete_long_value_deterministically() {
        let long = "session/".to_string() + &"a".repeat(80);
        let normalized = normalize_prompt_cache_key(&long).expect("long key should normalize");

        assert_eq!(normalized.len(), 64);
        assert_eq!(
            normalized,
            "ccd91d5ebd1f7899dbe3de69cacf02b3809a63ed5a3a0b16f0234fc438786e49"
        );
        assert_eq!(normalized, normalize_prompt_cache_key(&long).unwrap());

        let different_suffix = "session/".to_string() + &"a".repeat(79) + "b";
        assert_ne!(
            normalized,
            normalize_prompt_cache_key(&different_suffix).unwrap()
        );
    }

    #[test]
    fn prompt_cache_key_hashes_long_composite_session_identifiers() {
        let shared_prefix = format!("openai:gpt-5.6:{}", "project/".repeat(8));
        let first = format!("{shared_prefix}:delegated-run-a");
        let second = format!("{shared_prefix}:delegated-run-b");

        let first_key = normalize_prompt_cache_key(&first).unwrap();
        let second_key = normalize_prompt_cache_key(&second).unwrap();
        assert_eq!(first_key.len(), 64);
        assert_eq!(second_key.len(), 64);
        assert_ne!(first_key, second_key);

        let disabled = CallOptions {
            enable_caching: false,
            session_id: Some(first),
            ..Default::default()
        };
        assert!(normalized_prompt_cache_key(&disabled).is_none());
    }

    #[test]
    fn explicit_prompt_cache_scope_is_independent_from_transport_session() {
        let first = CallOptions {
            session_id: Some("transport-child-a".into()),
            prompt_cache_key: Some("shared-parent-builder-prefix".into()),
            ..Default::default()
        };
        let second = CallOptions {
            session_id: Some("transport-child-b".into()),
            prompt_cache_key: Some("shared-parent-builder-prefix".into()),
            ..Default::default()
        };

        assert_eq!(
            normalized_prompt_cache_key(&first),
            normalized_prompt_cache_key(&second)
        );
        assert_eq!(
            normalized_prompt_cache_key(&first).as_deref(),
            Some("shared-parent-builder-prefix")
        );

        let legacy = CallOptions {
            session_id: Some("transport-child-a".into()),
            ..Default::default()
        };
        assert_eq!(
            normalized_prompt_cache_key(&legacy).as_deref(),
            Some("transport-child-a")
        );
    }

    #[test]
    fn prompt_cache_retention_maps_default_and_extended_provider_values() {
        let standard = CallOptions {
            session_id: Some("session".into()),
            prompt_cache_retention: PromptCacheRetention::Standard,
            ..Default::default()
        };
        assert_eq!(
            openai_prompt_cache_options(&standard, "gpt-5.6", OpenAiPromptCacheMode::Implicit)
                .unwrap()["ttl"],
            "30m"
        );
        assert_eq!(
            anthropic_prompt_cache_control(&standard, ProviderId::Anthropic).unwrap(),
            json!({"type": "ephemeral"})
        );

        let extended = CallOptions {
            prompt_cache_retention: PromptCacheRetention::Extended,
            ..standard
        };
        assert_eq!(
            openai_prompt_cache_options(&extended, "gpt-5.6", OpenAiPromptCacheMode::Explicit)
                .unwrap()["ttl"],
            "30m"
        );
        assert_eq!(
            openai_prompt_cache_options(&extended, "gpt-5.6", OpenAiPromptCacheMode::Explicit)
                .unwrap()["mode"],
            "explicit"
        );
        assert!(openai_prompt_cache_retention(&extended, "gpt-5.6").is_none());
        assert_eq!(
            openai_prompt_cache_retention(&extended, "gpt-5.5").unwrap(),
            "24h"
        );
        assert_eq!(
            anthropic_prompt_cache_control(&extended, ProviderId::Anthropic).unwrap(),
            json!({"type": "ephemeral", "ttl": "1h"})
        );
    }

    #[test]
    fn prompt_cache_retention_is_omitted_for_unsupported_or_disabled_paths() {
        let extended = CallOptions {
            session_id: Some("session".into()),
            prompt_cache_retention: PromptCacheRetention::Extended,
            ..Default::default()
        };

        assert!(
            openai_prompt_cache_options(&extended, "gpt-5.5", OpenAiPromptCacheMode::Implicit)
                .is_none()
        );
        assert!(openai_prompt_cache_retention(&extended, "gpt-4o").is_none());
        assert!(anthropic_prompt_cache_control(&extended, ProviderId::MiniMax).is_none());
        assert_eq!(
            anthropic_prompt_cache_control(&extended, ProviderId::OpenRouter).unwrap(),
            json!({"type": "ephemeral"})
        );

        let disabled = CallOptions {
            enable_caching: false,
            ..extended
        };
        assert!(
            openai_prompt_cache_options(&disabled, "gpt-5.6", OpenAiPromptCacheMode::Implicit)
                .is_none()
        );
        assert!(anthropic_prompt_cache_control(&disabled, ProviderId::Anthropic).is_none());
    }

    #[test]
    fn prompt_cache_retention_parses_environment_style_values() {
        assert_eq!(
            PromptCacheRetention::from_config_value("long"),
            PromptCacheRetention::Extended
        );
        assert_eq!(
            PromptCacheRetention::from_config_value(" 24H "),
            PromptCacheRetention::Extended
        );
        assert_eq!(
            PromptCacheRetention::from_config_value("short"),
            PromptCacheRetention::Standard
        );
        assert_eq!(
            PromptCacheRetention::from_config_value("unexpected"),
            PromptCacheRetention::Standard
        );
    }

    #[test]
    fn canonicalization_drops_unsupported_provider_features() {
        let options = CallOptions {
            context_management: Some(ContextManagement::default_tools_only()),
            web_search: Some(WebSearchConfig::default()),
            web_fetch: Some(WebFetchConfig::default()),
            codex_parallel_tool_calls: true,
            ..Default::default()
        };

        let canonical =
            options.canonicalized_for(ProviderId::MiniMax, "MiniMax-M2.5", ApiFormat::Anthropic);

        assert!(canonical.context_management.is_none());
        assert!(canonical.web_search.is_none());
        assert!(canonical.web_fetch.is_none());
        assert!(!canonical.codex_parallel_tool_calls);
    }

    #[test]
    fn canonicalization_prefers_hosted_web_tools_when_supported() {
        let options = CallOptions {
            tools: Some(vec![tool("read"), tool("web_search"), tool("web_fetch")]),
            web_search: Some(WebSearchConfig::default()),
            web_fetch: Some(WebFetchConfig::default()),
            ..Default::default()
        };

        let canonical = options.canonicalized_for(
            ProviderId::Anthropic,
            "claude-opus-4-8",
            ApiFormat::Anthropic,
        );

        assert!(canonical.web_search.is_some());
        assert!(canonical.web_fetch.is_some());
        let names = canonical
            .tools
            .unwrap()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["read"]);
    }

    #[test]
    fn canonicalization_keeps_local_web_fallbacks_when_hosted_is_unsupported() {
        let options = CallOptions {
            tools: Some(vec![tool("web_search"), tool("web_fetch")]),
            web_search: Some(WebSearchConfig::default()),
            web_fetch: Some(WebFetchConfig::default()),
            ..Default::default()
        };

        let canonical =
            options.canonicalized_for(ProviderId::MiniMax, "MiniMax-M2.5", ApiFormat::Anthropic);

        assert!(canonical.web_search.is_none());
        assert!(canonical.web_fetch.is_none());
        let names = canonical
            .tools
            .unwrap()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["web_search", "web_fetch"]);
    }

    #[test]
    fn canonicalization_preserves_openai_local_web_tool_until_transport_selection() {
        let options = CallOptions {
            tools: Some(vec![tool("web_search"), tool("web_fetch")]),
            web_search: Some(WebSearchConfig::default()),
            web_fetch: Some(WebFetchConfig::default()),
            ..Default::default()
        };

        let canonical =
            options.canonicalized_for(ProviderId::OpenAI, "gpt-5.5", ApiFormat::OpenAIResponses);

        assert!(canonical.web_search.is_some());
        assert!(canonical.web_fetch.is_none());
        let names = canonical
            .tools
            .unwrap()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["web_search", "web_fetch"]);
    }

    #[test]
    fn canonicalization_uses_openrouter_server_tools_for_search_and_fetch() {
        let options = CallOptions {
            tools: Some(vec![tool("read"), tool("web_search"), tool("web_fetch")]),
            web_search: Some(WebSearchConfig::default()),
            web_fetch: Some(WebFetchConfig::default()),
            ..Default::default()
        };

        let canonical = options.canonicalized_for(
            ProviderId::OpenRouter,
            "openai/gpt-5.6-sol",
            ApiFormat::Anthropic,
        );

        assert!(canonical.web_search.is_some());
        assert!(canonical.web_fetch.is_some());
        let names = canonical
            .tools
            .unwrap()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["read"]);
    }

    #[test]
    fn canonicalization_keeps_local_web_fallbacks_for_grok_proxy() {
        let options = CallOptions {
            tools: Some(vec![tool("web_search"), tool("web_fetch")]),
            web_search: Some(WebSearchConfig::default()),
            web_fetch: Some(WebFetchConfig::default()),
            ..Default::default()
        };

        let canonical =
            options.canonicalized_for(ProviderId::Grok, "grok-build", ApiFormat::OpenAIResponses);

        assert!(canonical.web_search.is_none());
        assert!(canonical.web_fetch.is_none());
        let names = canonical
            .tools
            .unwrap()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["web_search", "web_fetch"]);
    }

    #[test]
    fn canonicalization_aligns_reasoning_controls_with_model_family() {
        let options = CallOptions {
            reasoning_format: Some(ReasoningFormat::OpenAI),
            codex_reasoning_effort: Some(CodexReasoningEffort::High),
            ..Default::default()
        };

        let canonical = options.canonicalized_for(
            ProviderId::Anthropic,
            "claude-opus-4-8",
            ApiFormat::Anthropic,
        );

        assert_eq!(canonical.reasoning_format, Some(ReasoningFormat::Anthropic));
        assert!(canonical.codex_reasoning_effort.is_none());
    }

    #[test]
    fn canonicalization_preserves_builtin_reasoning_models() {
        let options = CallOptions {
            thinking: Some(ThinkingConfig::default()),
            ..Default::default()
        };

        let canonical =
            options.canonicalized_for(ProviderId::MiniMax, "MiniMax-M2.5", ApiFormat::Anthropic);

        assert_eq!(canonical.reasoning_format, Some(ReasoningFormat::Anthropic));
        assert!(canonical.thinking.is_some());
    }

    #[test]
    fn canonicalization_strips_grok_proxy_reasoning_controls() {
        let options = CallOptions {
            thinking: Some(ThinkingConfig::default()),
            codex_reasoning_effort: Some(CodexReasoningEffort::XHigh),
            codex_parallel_tool_calls: true,
            ..Default::default()
        };

        let canonical =
            options.canonicalized_for(ProviderId::Grok, "grok-build", ApiFormat::OpenAIResponses);

        assert_eq!(canonical.reasoning_format, Some(ReasoningFormat::OpenAI));
        assert_eq!(
            canonical.reasoning_control,
            Some(ReasoningControl::OutputOnly)
        );
        assert!(canonical.codex_reasoning_effort.is_none());
        assert!(canonical.codex_parallel_tool_calls);
        assert!(canonical.thinking.is_none());
    }

    #[test]
    fn canonicalization_keeps_grok_46_graded_effort() {
        let options = CallOptions {
            thinking: Some(ThinkingConfig::default()),
            codex_reasoning_effort: Some(CodexReasoningEffort::Medium),
            reasoning_format: Some(ReasoningFormat::OpenAI),
            reasoning_control: Some(ReasoningControl::OpenAiEffort),
            ..Default::default()
        };

        let canonical =
            options.canonicalized_for(ProviderId::Grok, "grok-4.6", ApiFormat::OpenAIResponses);

        assert_eq!(canonical.reasoning_format, Some(ReasoningFormat::OpenAI));
        assert_eq!(
            canonical.reasoning_control,
            Some(ReasoningControl::OpenAiEffort)
        );
        assert_eq!(
            canonical.codex_reasoning_effort,
            Some(CodexReasoningEffort::Medium)
        );
        assert!(canonical.thinking.is_some());
    }

    #[test]
    fn canonicalization_maps_typed_reasoning_to_provider_controls() {
        let grok = CallOptions {
            reasoning_effort: Some(ReasoningEffort::Medium),
            ..Default::default()
        }
        .canonicalized_for(ProviderId::Grok, "grok-4.6", ApiFormat::OpenAIResponses);
        assert_eq!(grok.reasoning_effort, Some(ReasoningEffort::Medium));
        assert_eq!(
            grok.codex_reasoning_effort,
            Some(CodexReasoningEffort::Medium)
        );
        assert!(grok.thinking.is_some());

        let luna = CallOptions {
            reasoning_effort: Some(ReasoningEffort::XHigh),
            ..Default::default()
        }
        .canonicalized_for(
            ProviderId::Anthropic,
            "claude-opus-4-8",
            ApiFormat::Anthropic,
        );
        assert_eq!(luna.reasoning_effort, Some(ReasoningEffort::XHigh));
        assert!(luna.thinking.is_some());
    }

    #[test]
    fn fast_mode_maps_to_provider_service_tiers_without_changing_models() {
        let options = CallOptions {
            fast_mode: true,
            fast_mode_format: Some(FastMode::Priority),
            ..Default::default()
        };

        assert_eq!(
            options.service_tier_for_provider(ProviderId::OpenAI),
            Some("priority")
        );
        assert_eq!(
            options.service_tier_for_provider(ProviderId::Anthropic),
            None
        );
        assert_eq!(
            options.service_tier_for_provider(ProviderId::OpenRouter),
            Some("priority")
        );
        assert_eq!(
            options.service_tier_for_provider(ProviderId::MiniMax),
            Some("priority")
        );
        assert_eq!(options.service_tier_for_provider(ProviderId::ZAi), None);
    }

    #[test]
    fn standard_mode_does_not_request_priority_tiers() {
        let options = CallOptions::default();

        assert_eq!(options.service_tier_for_provider(ProviderId::OpenAI), None);
        assert_eq!(
            options.service_tier_for_provider(ProviderId::Anthropic),
            None
        );
        assert_eq!(
            options.service_tier_for_provider(ProviderId::OpenRouter),
            None
        );
    }

    #[test]
    fn reasoning_capability_does_not_turn_an_off_request_on() {
        let options = CallOptions {
            reasoning_format: Some(ReasoningFormat::Anthropic),
            reasoning_control: Some(ReasoningControl::AnthropicAdaptive),
            ..Default::default()
        };

        let canonical = options.canonicalized_for(
            ProviderId::Anthropic,
            "claude-opus-4-8",
            ApiFormat::Anthropic,
        );

        assert_eq!(canonical.reasoning_format, Some(ReasoningFormat::Anthropic));
        assert!(canonical.thinking.is_none());
        assert!(canonical.anthropic_adaptive_effort.is_none());
    }

    #[test]
    fn effective_settings_explain_dropped_grok_build_reasoning_controls() {
        let options = CallOptions {
            thinking: Some(ThinkingConfig::default()),
            codex_reasoning_effort: Some(CodexReasoningEffort::High),
            ..Default::default()
        };

        // Classic subscription rows remain output-only.
        let settings = options.effective_request_settings(
            ProviderId::Grok,
            "grok-build",
            ApiFormat::OpenAIResponses,
        );

        assert_eq!(settings.provider, "grok");
        assert_eq!(settings.model, "grok-build");
        assert!(!settings.thinking_enabled);
        assert_eq!(settings.reasoning_control.as_deref(), Some("OutputOnly"));
        assert!(settings
            .warnings
            .iter()
            .any(|warning| warning.contains("cannot accept")));
        assert!(settings
            .warnings
            .iter()
            .any(|warning| warning.contains("reasoning effort")));
    }

    #[test]
    fn effective_settings_keep_grok_46_graded_effort() {
        let options = CallOptions {
            thinking: Some(ThinkingConfig::default()),
            codex_reasoning_effort: Some(CodexReasoningEffort::Medium),
            reasoning_format: Some(ReasoningFormat::OpenAI),
            reasoning_control: Some(ReasoningControl::OpenAiEffort),
            ..Default::default()
        };

        let settings = options.effective_request_settings(
            ProviderId::Grok,
            "grok-4.6",
            ApiFormat::OpenAIResponses,
        );

        assert_eq!(settings.provider, "grok");
        assert_eq!(settings.model, "grok-4.6");
        assert!(settings.thinking_enabled);
        assert_eq!(settings.reasoning_control.as_deref(), Some("OpenAiEffort"));
        assert_eq!(settings.reasoning_effort.as_deref(), Some("Medium"));
        assert!(settings.warnings.is_empty());
    }

    #[test]
    fn effective_settings_keep_grok_46_xhigh_effort() {
        let options = CallOptions {
            thinking: Some(ThinkingConfig::default()),
            codex_reasoning_effort: Some(CodexReasoningEffort::XHigh),
            reasoning_format: Some(ReasoningFormat::OpenAI),
            reasoning_control: Some(ReasoningControl::OpenAiEffort),
            ..Default::default()
        };

        let settings = options.effective_request_settings(
            ProviderId::Grok,
            "grok-4.6",
            ApiFormat::OpenAIResponses,
        );

        assert_eq!(settings.model, "grok-4.6");
        assert!(settings.thinking_enabled);
        assert_eq!(settings.reasoning_control.as_deref(), Some("OpenAiEffort"));
        assert_eq!(settings.reasoning_effort.as_deref(), Some("XHigh"));
        assert!(settings.warnings.is_empty());
    }

    #[test]
    fn frozen_runtime_capabilities_override_unknown_model_inference() {
        use crate::ai::models::ModelMetadata;

        let mut metadata =
            ModelMetadata::new("catalog-only-model", "Catalog only", ProviderId::OpenRouter)
                .with_context(256_000, 12_345);
        metadata.supports_tools = true;
        metadata.api_format = ApiFormat::Anthropic;
        let runtime = metadata.resolve_runtime();
        let options = CallOptions {
            max_tokens: Some(50_000),
            tools: Some(vec![tool("read")]),
            ..Default::default()
        };

        let canonical = options.canonicalized_for_runtime(&runtime);

        assert_eq!(canonical.max_tokens, Some(12_345));
        assert_eq!(canonical.tools.as_ref().map(Vec::len), Some(1));
    }

    #[test]
    fn frozen_runtime_without_tool_capability_removes_tool_surfaces() {
        use crate::ai::models::ModelMetadata;

        let mut metadata =
            ModelMetadata::new("text-only-model", "Text only", ProviderId::OpenRouter);
        metadata.supports_tools = false;
        metadata.api_format = ApiFormat::Anthropic;
        let runtime = metadata.resolve_runtime();
        let options = CallOptions {
            tools: Some(vec![tool("read")]),
            web_search: Some(WebSearchConfig::default()),
            ..Default::default()
        };

        let settings = options.effective_request_settings_for_runtime(&runtime);

        assert_eq!(settings.tool_count, 0);
        assert!(!settings.hosted_web_search);
        assert!(settings
            .warnings
            .iter()
            .any(|warning| warning.contains("does not advertise tool calling")));
    }
}
