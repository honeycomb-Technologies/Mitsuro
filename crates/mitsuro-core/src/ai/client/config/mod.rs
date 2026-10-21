//! AI Client configuration
//!
//! Provider-agnostic configuration for AI API clients.

mod ai_client;
mod call_options;
mod effort;
mod parallel_tools;

pub use ai_client::AiClientConfig;
pub(crate) use call_options::{
    anthropic_prompt_cache_control, normalized_prompt_cache_key, openai_prompt_cache_options,
    openai_prompt_cache_retention, OpenAiPromptCacheMode,
};
pub use call_options::{CallOptions, EffectiveRequestSettings, PromptCacheRetention};
pub use effort::{supports_openai_xhigh_reasoning, AnthropicAdaptiveEffort, CodexReasoningEffort};
pub(crate) use parallel_tools::{
    apply_parallel_tool_call_policy, resolve_parallel_tool_calls, ParallelToolCallTransport,
};
