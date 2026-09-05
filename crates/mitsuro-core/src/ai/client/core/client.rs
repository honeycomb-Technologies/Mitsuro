use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::ai::client::config::{
    normalized_prompt_cache_key, resolve_parallel_tool_calls, AiClientConfig, CallOptions,
    EffectiveRequestSettings,
};
use crate::ai::client::streaming::codex::session::CodexWsPool;
use crate::ai::model_profile::{build_system_prompt_sections, SystemPromptSections};
use crate::ai::models::{resolve_model_metadata, ResolvedModelRuntime};
use crate::ai::providers::ProviderId;
use crate::ai::types::{AiTool, ModelMessage};

/// Redacted explanation of the exact request contract prepared for one model
/// turn. No credentials, prompt contents, user text, or tool schemas appear in
/// this structure, so it is safe for runtime traces and debug surfaces.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PreparedRequestDiagnostics {
    pub model: ResolvedModelRuntime,
    pub effective_request: EffectiveRequestSettings,
    /// Exact sorted function-tool names advertised on the provider request.
    /// Names are safe diagnostics; descriptions and schemas remain redacted.
    #[serde(default)]
    pub tool_names: Vec<String>,
    pub prompt_manifest: serde_json::Value,
    #[serde(default)]
    pub stable_request_fingerprint: String,
    #[serde(default)]
    pub stable_instruction_bytes: usize,
    #[serde(default)]
    pub volatile_session_bytes: usize,
    #[serde(default)]
    pub tool_schema_bytes: usize,
    #[serde(default)]
    pub history_bytes: usize,
    #[serde(default)]
    pub cache_key_present: bool,
    #[serde(default)]
    pub cache_mode: String,
    #[serde(default)]
    pub continuation_mode: Option<String>,
    pub message_count: usize,
    pub system_message_count: usize,
    pub user_message_count: usize,
    pub assistant_message_count: usize,
}

/// AI API client supporting multiple providers
pub struct AiClient {
    pub(super) http: Client,
    pub(super) config: AiClientConfig,
    pub(super) api_key: String,
    pub(crate) codex_ws_pool: CodexWsPool,
    /// Immutable provider/model/capability row used for the lifetime of this
    /// client. Dynamic catalog refreshes cannot alter an in-flight run.
    resolved_model: ResolvedModelRuntime,
}

impl AiClient {
    /// Create a new client with API key
    pub fn new(config: AiClientConfig, api_key: String) -> Self {
        let resolved_model =
            resolve_model_metadata(config.provider_id, &config.model, config.api_format)
                .resolve_runtime();
        Self {
            http: Self::create_http_client(),
            config,
            api_key,
            codex_ws_pool: CodexWsPool::default(),
            resolved_model,
        }
    }

    /// Create a client from an exact catalog row and fail closed if its
    /// executable identity does not match the configured transport.
    pub fn new_with_resolved_model(
        config: AiClientConfig,
        api_key: String,
        resolved_model: ResolvedModelRuntime,
    ) -> anyhow::Result<Self> {
        if resolved_model.key.provider != config.provider_id {
            anyhow::bail!(
                "resolved model provider {:?} does not match client provider {:?}",
                resolved_model.key.provider,
                config.provider_id
            );
        }
        if resolved_model.wire_model_id != config.model {
            anyhow::bail!(
                "resolved wire model '{}' does not match client model '{}'",
                resolved_model.wire_model_id,
                config.model
            );
        }
        if resolved_model.key.api_format != config.api_format {
            anyhow::bail!(
                "resolved model API format {:?} does not match client format {:?}",
                resolved_model.key.api_format,
                config.api_format
            );
        }

        Ok(Self {
            http: Self::create_http_client(),
            config,
            api_key,
            codex_ws_pool: CodexWsPool::default(),
            resolved_model,
        })
    }

    /// Get the API key
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Get the provider ID for this client
    pub fn provider_id(&self) -> ProviderId {
        self.config.provider_id()
    }

    /// Get the current configuration
    pub fn config(&self) -> &AiClientConfig {
        &self.config
    }

    /// Run-scoped model identity and capabilities used by all request policy.
    pub fn resolved_model(&self) -> &ResolvedModelRuntime {
        &self.resolved_model
    }

    /// Reject an attempt to reuse this run-scoped client for a different model.
    /// A different provider/model/auth/API identity requires a separately
    /// resolved client so it cannot inherit the wrong capabilities or wire
    /// transport from this snapshot.
    pub(crate) fn ensure_run_model(&self, model: &str) -> anyhow::Result<()> {
        if model != self.resolved_model.wire_model_id {
            anyhow::bail!(
                "model override '{}' does not match this run's resolved model '{}'; resolve a new AI client for the exact model key",
                model,
                self.resolved_model.wire_model_id
            );
        }
        Ok(())
    }

    /// Build diagnostics through the same canonical option and prompt paths as
    /// the subsequent provider call.
    pub fn request_diagnostics(
        &self,
        messages: &[ModelMessage],
        options: &CallOptions,
    ) -> PreparedRequestDiagnostics {
        let canonical = self.canonical_call_options(&self.config.model, options);
        let prompt_sections = self.system_prompt_sections(
            &self.config.model,
            messages,
            canonical.system_prompt.as_deref(),
            canonical.tools.as_deref(),
        );
        let mut effective_request =
            options.effective_request_settings_for_runtime(&self.resolved_model);
        effective_request.max_tokens = canonical.max_tokens;
        effective_request.tool_count = canonical.tools.as_ref().map_or(0, Vec::len);
        effective_request.parallel_tool_calls = resolve_parallel_tool_calls(
            Some(&canonical),
            self.config.parallel_tool_call_transport(),
            canonical
                .tools
                .as_ref()
                .is_some_and(|tools| !tools.is_empty())
                || canonical.web_search.is_some(),
        )
        .unwrap_or(false);
        let mut tool_names = canonical
            .tools
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|tool| tool.name.clone())
            .collect::<Vec<_>>();
        tool_names.sort();
        let component_metrics = crate::ai::client::streaming::shared::request_component_metrics(
            &prompt_sections,
            messages,
            canonical.tools.as_deref(),
        );
        let cache_key_present = normalized_prompt_cache_key(&canonical).is_some();
        let cache_mode = if !canonical.enable_caching {
            "disabled"
        } else if cache_key_present {
            "session_key"
        } else {
            "automatic"
        };

        PreparedRequestDiagnostics {
            model: self.resolved_model.clone(),
            effective_request,
            tool_names,
            prompt_manifest: prompt_sections.diagnostic_manifest(),
            stable_request_fingerprint: component_metrics.request_shape_fingerprint,
            stable_instruction_bytes: component_metrics
                .base_bytes
                .saturating_add(component_metrics.identity_bytes)
                .saturating_add(component_metrics.project_bytes),
            volatile_session_bytes: component_metrics.session_bytes,
            tool_schema_bytes: component_metrics.tool_schema_bytes,
            history_bytes: component_metrics.history_bytes,
            cache_key_present,
            cache_mode: cache_mode.to_string(),
            continuation_mode: self
                .config
                .uses_chatgpt_codex_format()
                .then(|| "delta".to_string()),
            message_count: messages.len(),
            system_message_count: messages
                .iter()
                .filter(|message| message.role == crate::ai::types::Role::System)
                .count(),
            user_message_count: messages
                .iter()
                .filter(|message| message.role == crate::ai::types::Role::User)
                .count(),
            assistant_message_count: messages
                .iter()
                .filter(|message| message.role == crate::ai::types::Role::Assistant)
                .count(),
        }
    }

    pub(crate) fn canonical_call_options(&self, model: &str, options: &CallOptions) -> CallOptions {
        if model != self.resolved_model.wire_model_id {
            tracing::warn!(
                configured_model = %self.resolved_model.wire_model_id,
                requested_model = %model,
                "Ignoring mismatched model while canonicalizing against the immutable run snapshot"
            );
        }
        let mut canonical = options.canonicalized_for_runtime(&self.resolved_model);

        let requested_max = options.max_tokens.unwrap_or(self.config.max_tokens);
        let runtime_max = self.resolved_model.capabilities.max_output;
        canonical.max_tokens = Some(requested_max.min(runtime_max));
        canonical
    }

    pub(crate) fn system_prompt_sections(
        &self,
        model: &str,
        messages: &[ModelMessage],
        custom_system_prompt: Option<&str>,
        _tools: Option<&[AiTool]>,
    ) -> SystemPromptSections {
        build_system_prompt_sections(
            self.provider_id(),
            self.config().api_format,
            model,
            messages,
            custom_system_prompt,
            &[],
        )
    }
}
