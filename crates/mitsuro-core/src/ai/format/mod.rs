//! API Format handling
//!
//! Abstracts the differences between Anthropic, OpenAI, and Google API formats.
//! Each format handler knows how to convert messages, tools, and build request bodies.

pub mod anthropic;
pub mod google;
pub mod openai;
pub mod response;
mod source_context;

pub(crate) use source_context::{provider_text, provider_text_len};

use serde_json::Value;

use crate::ai::client::config::CallOptions;
use crate::ai::providers::ProviderId;
use crate::ai::types::{AiTool, ModelMessage};

/// Trait for handling different API formats
///
/// Implementations convert between our unified domain types and provider-specific
/// API formats (Anthropic, OpenAI, Google).
pub trait FormatHandler: Send + Sync {
    /// Convert domain messages to API-specific format
    ///
    /// The provider_id parameter allows provider-specific handling:
    /// - MiniMax: Preserve ALL thinking blocks (per their docs)
    /// - Anthropic: Only preserve last thinking with pending tools (signature validation)
    fn convert_messages(
        &self,
        messages: &[ModelMessage],
        provider_id: Option<ProviderId>,
    ) -> Vec<Value>;

    /// Convert tools to API-specific format
    fn convert_tools(&self, tools: &[AiTool]) -> Vec<Value>;

    /// Build the complete request body
    fn build_request_body(
        &self,
        model: &str,
        messages: Vec<Value>,
        options: &RequestOptions,
    ) -> Value;

    /// Get the API endpoint path for this format
    fn endpoint_path(&self, model: &str) -> &str;
}

/// Options for building API requests
pub struct RequestOptions<'a> {
    pub max_tokens: usize,
    pub system_prompt: Option<&'a str>,
    pub tools: Option<&'a [AiTool]>,
    pub temperature: Option<f32>,
    pub streaming: bool,
    pub call_options: Option<&'a CallOptions>,
}

impl<'a> Default for RequestOptions<'a> {
    fn default() -> Self {
        Self {
            max_tokens: 16384,
            system_prompt: None,
            tools: None,
            temperature: None,
            streaming: false,
            call_options: None,
        }
    }
}

/// Select the appropriate format handler based on API format
pub fn get_format_handler(format: crate::ai::models::ApiFormat) -> Box<dyn FormatHandler> {
    use crate::ai::models::ApiFormat;
    match format {
        ApiFormat::Anthropic => Box::new(anthropic::AnthropicFormat::new()),
        ApiFormat::OpenAI | ApiFormat::OpenAIResponses => {
            Box::new(openai::OpenAIFormat::new(format))
        }
        ApiFormat::Google => Box::new(google::GoogleFormat::new()),
    }
}

/// Check if a filler message is needed to maintain role alternation.
///
/// Returns Some(filler_role) if a filler message should be inserted,
/// None if no filler is needed.
///
/// # Arguments
/// * `last_role` - The role of the previous message, if any
/// * `current_role` - The role of the current message
/// * `exclude_roles` - Roles to exclude from alternation check (e.g., "tool" for OpenAI)
pub fn needs_role_alternation_filler(
    last_role: Option<&str>,
    current_role: &str,
    exclude_roles: &[&str],
) -> Option<&'static str> {
    if let Some(prev_role) = last_role {
        // Check if current role should be excluded from alternation
        if exclude_roles.contains(&current_role) {
            return None;
        }
        // Check for consecutive same-role messages
        if prev_role == current_role {
            // Return the opposite role for filler
            return Some(if current_role == "user" {
                "assistant"
            } else {
                "user"
            });
        }
    }
    None
}
