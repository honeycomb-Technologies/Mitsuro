use std::collections::HashMap;

use crate::ai::models::ApiFormat;
use crate::ai::providers::{AuthHeader, ProviderId};
use crate::constants;

/// Configuration for the AI client
#[derive(Debug, Clone)]
pub struct AiClientConfig {
    /// Model ID to use for API calls
    pub model: String,
    /// Maximum output tokens
    pub max_tokens: usize,
    /// Optional base URL override (defaults to provider default)
    pub base_url: Option<String>,
    /// How to send authentication header
    pub auth_header: AuthHeader,
    /// Which provider this config is for
    pub provider_id: ProviderId,
    /// API format for this model (Anthropic, OpenAI, Google)
    pub api_format: ApiFormat,
    /// Custom headers to send with requests
    pub custom_headers: HashMap<String, String>,
}

impl Default for AiClientConfig {
    fn default() -> Self {
        Self {
            model: constants::ai::DEFAULT_MODEL.to_string(),
            max_tokens: constants::ai::MAX_OUTPUT_TOKENS,
            base_url: None,
            auth_header: AuthHeader::XApiKey,
            provider_id: ProviderId::MiniMax,
            api_format: ApiFormat::Anthropic,
            custom_headers: HashMap::new(),
        }
    }
}

impl AiClientConfig {
    /// Get the API URL to use
    pub fn api_url(&self) -> String {
        const DEFAULT_API_URL: &str = "https://api.minimax.io/anthropic/v1/messages";

        if let Some(base) = &self.base_url {
            base.clone()
        } else {
            DEFAULT_API_URL.to_string()
        }
    }

    /// Get the provider ID
    pub fn provider_id(&self) -> ProviderId {
        self.provider_id
    }

    /// Check if this config uses OpenAI chat/completions format
    pub fn uses_openai_format(&self) -> bool {
        matches!(
            self.api_format,
            ApiFormat::OpenAI | ApiFormat::OpenAIResponses
        )
    }

    /// Check if this config uses Google/Gemini format
    pub fn uses_google_format(&self) -> bool {
        matches!(self.api_format, ApiFormat::Google)
    }

    /// Check if this config uses the ChatGPT Codex OAuth transport.
    pub fn uses_chatgpt_codex_format(&self) -> bool {
        self.provider_id == ProviderId::OpenAI
            && self
                .base_url
                .as_deref()
                .is_some_and(|url| url.contains("chatgpt.com"))
    }

    pub(crate) fn parallel_tool_call_transport(&self) -> super::ParallelToolCallTransport {
        if self.uses_chatgpt_codex_format() {
            super::ParallelToolCallTransport::ChatGptCodex
        } else if matches!(self.api_format, ApiFormat::OpenAIResponses) {
            super::ParallelToolCallTransport::Responses
        } else {
            super::ParallelToolCallTransport::Uncontrolled
        }
    }

    /// Check if this provider uses Anthropic-compatible API
    ///
    /// All providers (OpenRouter, Z.ai, MiniMax) use Anthropic Messages API
    /// Exception: OpenAI uses its own format
    pub fn uses_anthropic_api(&self) -> bool {
        !self.uses_openai_format() && !self.uses_google_format()
    }

    /// Create an OpenAI config from a model-catalog-selected auth surface.
    ///
    /// Live API-key and ChatGPT catalogs can advertise the same slug with
    /// different capabilities. Callers that have catalog provenance must use
    /// this constructor so request routing matches the metadata they selected.
    pub fn for_openai_with_auth_resolution(
        model: &str,
        auth_resolution: crate::auth::OpenAIAuthResolution,
    ) -> Self {
        use crate::ai::providers::ProviderConfig;

        let auth_type = auth_resolution.auth_type;
        let base_url = ProviderConfig::openai_url_for_auth(model, auth_type);
        let api_format = ProviderConfig::openai_format_for_auth(model, auth_type);

        tracing::info!(
            "OpenAI auth detection: {:?} -> {} (format: {:?})",
            auth_type,
            base_url,
            api_format
        );

        Self {
            model: model.to_string(),
            max_tokens: constants::ai::MAX_OUTPUT_TOKENS,
            base_url: Some(base_url.to_string()),
            auth_header: AuthHeader::Bearer,
            provider_id: ProviderId::OpenAI,
            api_format,
            custom_headers: {
                let mut headers = HashMap::new();
                if matches!(auth_type, crate::auth::OpenAIAuthType::ChatGptOAuth) {
                    if let Some(account_id) = auth_resolution.account_id {
                        headers.insert("ChatGPT-Account-Id".to_string(), account_id);
                    }
                }
                headers
            },
        }
    }

    /// Create config for Grok/X subscription authentication.
    pub fn for_grok(model: &str) -> Self {
        use crate::ai::format_detection::detect_api_format;
        use crate::ai::models::resolve_model_metadata;

        let api_format = detect_api_format(ProviderId::Grok, model);
        let metadata = resolve_model_metadata(ProviderId::Grok, model, api_format);
        Self::for_grok_with_metadata(&metadata)
    }

    /// Create Grok transport from an exact catalog row.
    ///
    /// The Grok catalog advertises its API backend per model. Using the frozen
    /// metadata prevents a chat-completions model from being sent to the
    /// Responses endpoint merely because both share the Grok provider.
    pub fn for_grok_with_metadata(metadata: &crate::ai::models::ModelMetadata) -> Self {
        use crate::ai::providers::get_provider;

        debug_assert_eq!(metadata.provider, ProviderId::Grok);
        let provider = get_provider(ProviderId::Grok);
        let model = metadata.id.as_str();
        let api_format = metadata.api_format;
        let mut custom_headers = provider
            .map(|config| config.custom_headers.clone())
            .unwrap_or_default();
        custom_headers.insert(
            "x-grok-client-version".to_string(),
            std::env::var("GROK_CLIENT_VERSION")
                .unwrap_or_else(|_| grok_auth::DEFAULT_CLIENT_VERSION.to_string()),
        );
        custom_headers.insert("X-XAI-Token-Auth".to_string(), "xai-grok-cli".to_string());
        custom_headers.insert("x-grok-model-override".to_string(), model.to_string());
        custom_headers.insert(
            "x-grok-context-window".to_string(),
            metadata.context_window.to_string(),
        );
        custom_headers.insert(
            "x-grok-max-completion-tokens".to_string(),
            metadata.max_output.to_string(),
        );

        let base_url = std::env::var("GROK_CLI_CHAT_PROXY_BASE_URL")
            .map(|base| grok_endpoint_url(&base, api_format))
            .unwrap_or_else(|_| {
                provider
                    .map(|config| grok_endpoint_url(&config.base_url, api_format))
                    .unwrap_or_else(|| {
                        grok_endpoint_url("https://cli-chat-proxy.grok.com/v1", api_format)
                    })
            });

        Self {
            model: model.to_string(),
            max_tokens: metadata.max_output,
            base_url: Some(base_url),
            auth_header: AuthHeader::Bearer,
            provider_id: ProviderId::Grok,
            api_format,
            custom_headers,
        }
    }

    /// Create config for Anthropic with automatic auth type detection
    ///
    /// Detects whether OAuth token or API key is being used:
    /// - OAuth (sk-ant-oat*): Bearer auth + CC identity headers
    /// - API Key (sk-ant-*): x-api-key auth
    pub fn for_anthropic_with_auth_detection(
        model: &str,
        credentials: &crate::storage::CredentialStore,
    ) -> Self {
        use crate::ai::providers::ProviderConfig;
        use crate::auth::resolve_anthropic_auth;

        let auth_resolution = resolve_anthropic_auth(credentials);
        let auth_type = auth_resolution.auth_type;
        let auth_header = ProviderConfig::anthropic_auth_header_for_auth(auth_type);

        tracing::info!(
            "Anthropic auth detection: {:?} -> auth_header={:?}",
            auth_type,
            auth_header,
        );

        let mut custom_headers = HashMap::new();
        if matches!(auth_type, crate::auth::AnthropicAuthType::OAuth) {
            custom_headers.insert(
                "user-agent".to_string(),
                "claude-cli/2.1.2 (external, cli)".to_string(),
            );
            custom_headers.insert("x-app".to_string(), "cli".to_string());
        }

        Self {
            model: model.to_string(),
            max_tokens: constants::ai::MAX_OUTPUT_TOKENS,
            base_url: Some("https://api.anthropic.com/v1/messages".to_string()),
            auth_header,
            provider_id: ProviderId::Anthropic,
            api_format: ApiFormat::Anthropic,
            custom_headers,
        }
    }
}

fn grok_endpoint_url(base_url: &str, api_format: ApiFormat) -> String {
    let base = base_url
        .trim()
        .trim_end_matches('/')
        .trim_end_matches("/chat/completions")
        .trim_end_matches("/responses")
        .trim_end_matches("/messages");
    let path = match api_format {
        ApiFormat::OpenAIResponses => "responses",
        ApiFormat::OpenAI => "chat/completions",
        ApiFormat::Anthropic => "messages",
        ApiFormat::Google => "models",
    };
    format!("{base}/{path}")
}

#[cfg(test)]
mod tests {
    use super::AiClientConfig;
    use crate::ai::models::{ApiFormat, ModelMetadata};
    use crate::ai::providers::ProviderId;

    #[test]
    fn grok_config_uses_build_responses_endpoint_and_cli_headers() {
        let config = AiClientConfig::for_grok("grok-build");

        assert_eq!(config.provider_id, ProviderId::Grok);
        assert_eq!(config.api_format, ApiFormat::OpenAIResponses);
        assert!(config
            .base_url
            .as_deref()
            .is_some_and(|url| url.ends_with("/responses")));
        assert_eq!(
            config
                .custom_headers
                .get("x-grok-model-override")
                .map(String::as_str),
            Some("grok-build")
        );
        assert_eq!(
            config
                .custom_headers
                .get("X-XAI-Token-Auth")
                .map(String::as_str),
            Some("xai-grok-cli")
        );
        assert_eq!(
            config
                .custom_headers
                .get("x-grok-context-window")
                .map(String::as_str),
            Some("512000")
        );
        assert!(!config.uses_chatgpt_codex_format());
        assert!(config.uses_openai_format());
    }

    #[test]
    fn grok_config_uses_catalog_selected_transport() {
        let metadata = ModelMetadata::new("grok-chat", "Grok Chat", ProviderId::Grok)
            .with_transport(ApiFormat::OpenAI)
            .with_context(131_072, 16_384);

        let config = AiClientConfig::for_grok_with_metadata(&metadata);

        assert_eq!(config.api_format, ApiFormat::OpenAI);
        assert!(config
            .base_url
            .as_deref()
            .is_some_and(|url| url.ends_with("/chat/completions")));
        assert_eq!(config.max_tokens, 16_384);
    }

    #[test]
    fn grok_config_strips_endpoint_suffix_from_proxy_base_override() {
        let url = super::grok_endpoint_url(
            "https://proxy.example/v1/chat/completions",
            ApiFormat::OpenAIResponses,
        );

        assert_eq!(url, "https://proxy.example/v1/responses");
    }
}
