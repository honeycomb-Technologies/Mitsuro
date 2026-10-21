//! Centralized request building for AI client
//!
//! Uses FormatHandler trait to build requests consistently across
//! streaming, tools, and simple call paths.

use serde_json::Value;

use super::config::CallOptions;
use super::core::AiClient;
use crate::ai::format::{FormatHandler, RequestOptions};
use crate::ai::types::{AiTool, ModelMessage};

/// Request builder using FormatHandler
///
/// Centralizes request building logic to avoid duplication across
/// streaming, tools, and simple call paths.
pub struct RequestBuilder<'a, 'b> {
    client: &'a AiClient,
    format_handler: &'b dyn FormatHandler,
}

impl<'a, 'b> RequestBuilder<'a, 'b> {
    /// Create a new request builder
    pub fn new(client: &'a AiClient, format_handler: &'b dyn FormatHandler) -> Self {
        Self {
            client,
            format_handler,
        }
    }

    /// Build request body for API call
    pub fn build_request_body(
        &self,
        model: &str,
        messages: Vec<ModelMessage>,
        options: &BuildOptions,
    ) -> Value {
        // Convert messages using format handler
        let converted_messages = self
            .format_handler
            .convert_messages(&messages, Some(self.client.provider_id()));

        // Build RequestOptions
        let request_options = RequestOptions {
            max_tokens: options.max_tokens,
            system_prompt: options.system_prompt.as_deref(),
            tools: options.tools.as_deref(),
            temperature: options.temperature,
            streaming: options.streaming,
            call_options: options.call_options,
        };

        // Use format handler to build body
        self.format_handler
            .build_request_body(model, converted_messages, &request_options)
    }
}

/// Options for building requests
pub struct BuildOptions<'a> {
    pub max_tokens: usize,
    pub system_prompt: Option<String>,
    pub tools: Option<Vec<AiTool>>,
    pub temperature: Option<f32>,
    pub streaming: bool,
    pub call_options: Option<&'a CallOptions>,
}

impl<'a> Default for BuildOptions<'a> {
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
