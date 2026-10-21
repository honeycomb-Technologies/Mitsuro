use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

use super::super::providers::{
    FastMode, ProviderId, ReasoningControl, ReasoningEffort, ReasoningFormat,
};

/// Metadata describing a cached dynamic model catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DynamicModelCacheMetadata {
    pub fetched_at: u64,
    pub ttl_seconds: u64,
    pub model_count: usize,
    pub fingerprint: u64,
}

/// Authentication surface that advertised a model in a live catalog.
///
/// This is intentionally internal model-routing metadata, not a UI capability.
/// OpenAI API-key and ChatGPT OAuth catalogs may contain overlapping IDs with
/// different capabilities, so the selected row must retain its transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAuthScope {
    ApiKey,
    #[serde(rename = "oauth")]
    OAuth,
}

/// API format for model requests
///
/// Different model families route to different provider endpoints based on format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ApiFormat {
    /// Anthropic Messages API (/v1/messages)
    #[default]
    Anthropic,
    /// OpenAI Chat Completions API (/v1/chat/completions)
    #[serde(rename = "open_ai", alias = "open_a_i")]
    OpenAI,
    /// OpenAI Responses API (/v1/responses) - GPT-5 models
    #[serde(rename = "open_ai_responses", alias = "open_a_i_responses")]
    OpenAIResponses,
    /// Google AI API (/v1/models/{model})
    Google,
}

/// Stable source classification for a model-catalog row.
///
/// Keeping this on the row makes it possible to distinguish a bundled safety
/// fallback from a credential-scoped live discovery result without inferring
/// provenance from the model name later in the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ModelCatalogSource {
    Curated,
    CachedDynamic,
    LiveDynamic,
    Custom,
    /// Rows written before provenance became explicit.
    #[default]
    Legacy,
}

/// Provider-aware identity for one executable model transport.
///
/// A bare model slug is not globally unique. The same slug may be advertised
/// by multiple providers, credential surfaces, or wire APIs. This key is used
/// by the registry, preferences, and session persistence so those variants do
/// not overwrite or silently impersonate one another.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelKey {
    pub provider: ProviderId,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_scope: Option<ModelAuthScope>,
    pub api_format: ApiFormat,
}

impl ModelKey {
    pub fn new(provider: ProviderId, model_id: impl Into<String>, api_format: ApiFormat) -> Self {
        Self {
            provider,
            model_id: model_id.into(),
            auth_scope: None,
            api_format,
        }
    }

    pub fn with_auth_scope(mut self, auth_scope: ModelAuthScope) -> Self {
        self.auth_scope = Some(auth_scope);
        self
    }

    pub fn from_metadata(metadata: &ModelMetadata) -> Self {
        Self {
            provider: metadata.provider,
            model_id: metadata.id.clone(),
            auth_scope: metadata.auth_scope,
            api_format: metadata.api_format,
        }
    }
}

/// Project-owned model selection.
///
/// Legacy settings stored a bare model slug. New settings may store the full
/// provider/auth/transport key so routing stays exact when multiple catalogs
/// advertise the same slug. The untagged representation preserves existing
/// `.mitsuro/settings.json` files without guessing when a slug is ambiguous.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProjectModelRef {
    Exact(ModelKey),
    Legacy(String),
}

impl ProjectModelRef {
    pub fn model_id(&self) -> &str {
        match self {
            Self::Exact(key) => &key.model_id,
            Self::Legacy(model_id) => model_id,
        }
    }

    pub fn exact_key(&self) -> Option<&ModelKey> {
        match self {
            Self::Exact(key) => Some(key),
            Self::Legacy(_) => None,
        }
    }
}

/// Capability snapshot used for the lifetime of one run.
///
/// This is deliberately separate from mutable catalog storage. Once resolved,
/// a catalog refresh cannot change the request contract under an active run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilities {
    pub context_window: usize,
    pub max_output: usize,
    pub supports_thinking: bool,
    pub reasoning_format: Option<ReasoningFormat>,
    pub supported_reasoning_levels: Vec<ReasoningEffort>,
    pub default_reasoning_level: Option<ReasoningEffort>,
    pub reasoning_is_mandatory: bool,
    pub reasoning_control: Option<ReasoningControl>,
    pub fast_mode: Option<FastMode>,
    pub supports_tools: bool,
    pub supports_vision: bool,
}

/// Immutable executable model selection resolved from one exact catalog row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedModelRuntime {
    pub key: ModelKey,
    /// Exact model identifier sent on the wire. Kept separate from future UI
    /// aliases even though it currently matches `key.model_id`.
    pub wire_model_id: String,
    pub display_name: String,
    pub capabilities: ModelCapabilities,
    pub catalog_source: ModelCatalogSource,
    pub catalog_revision: Option<String>,
    pub input_price: Option<f64>,
    pub output_price: Option<f64>,
}

/// Rich model metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    /// Unique model ID (e.g., "claude-opus-4-5-20251101")
    pub id: String,
    /// Human-readable name (e.g., "Claude Opus 4.5")
    pub display_name: String,
    /// Which provider offers this model
    pub provider: ProviderId,
    /// Credential surface that supplied this catalog row, when transport-sensitive.
    #[serde(default)]
    pub auth_scope: Option<ModelAuthScope>,
    /// Maximum context window in tokens
    pub context_window: usize,
    /// Maximum output tokens
    pub max_output: usize,

    // Capabilities
    /// Supports extended thinking/reasoning (legacy boolean)
    pub supports_thinking: bool,
    /// Reasoning/thinking format (None = not supported, Some = supported with specific format)
    pub reasoning_format: Option<ReasoningFormat>,
    /// Exact user-selectable levels, ordered for presentation.
    #[serde(default)]
    pub supported_reasoning_levels: Vec<ReasoningEffort>,
    /// Catalog default when no explicit level is selected.
    #[serde(default)]
    pub default_reasoning_level: Option<ReasoningEffort>,
    /// Some reasoning models do not permit an off/none choice.
    #[serde(default)]
    pub reasoning_is_mandatory: bool,
    /// Wire control used by the configured transport.
    #[serde(default)]
    pub reasoning_control: Option<ReasoningControl>,
    /// Per-model implementation of Standard/Fast.
    #[serde(default)]
    pub fast_mode: Option<FastMode>,
    /// Supports function/tool calling
    pub supports_tools: bool,
    /// Supports image input (vision)
    pub supports_vision: bool,

    // Pricing (per million tokens, None if unknown)
    /// Input/prompt price per million tokens
    pub input_price: Option<f64>,
    /// Output/completion price per million tokens
    pub output_price: Option<f64>,

    // Provider-specific metadata
    /// Sub-provider for OpenRouter models (e.g., "anthropic", "openai")
    #[serde(default)]
    pub sub_provider: Option<String>,
    /// Whether this is a free model (OpenRouter :free suffix)
    #[serde(default)]
    pub is_free: bool,
    /// API format for this model, used for provider routing.
    #[serde(default)]
    pub api_format: ApiFormat,
    /// Where this exact catalog row came from.
    #[serde(default)]
    pub catalog_source: ModelCatalogSource,
    /// Opaque revision/fingerprint for the catalog snapshot that supplied it.
    #[serde(default)]
    pub catalog_revision: Option<String>,
}

impl ModelMetadata {
    /// Create basic model metadata
    pub fn new(id: &str, display_name: &str, provider: ProviderId) -> Self {
        Self {
            id: id.to_string(),
            display_name: display_name.to_string(),
            provider,
            auth_scope: None,
            context_window: 128_000,
            max_output: 4096,
            supports_thinking: false,
            reasoning_format: None,
            supported_reasoning_levels: Vec::new(),
            default_reasoning_level: None,
            reasoning_is_mandatory: false,
            reasoning_control: None,
            fast_mode: None,
            supports_tools: true,
            supports_vision: false,
            input_price: None,
            output_price: None,
            sub_provider: None,
            is_free: false,
            api_format: match provider {
                ProviderId::Anthropic | ProviderId::MiniMax | ProviderId::OpenRouter => {
                    ApiFormat::Anthropic
                }
                ProviderId::OpenAI | ProviderId::ZAi => ApiFormat::OpenAI,
                ProviderId::Grok => ApiFormat::OpenAIResponses,
            },
            catalog_source: ModelCatalogSource::Legacy,
            catalog_revision: None,
        }
    }

    pub fn key(&self) -> ModelKey {
        ModelKey::from_metadata(self)
    }

    pub fn with_transport(mut self, api_format: ApiFormat) -> Self {
        self.api_format = api_format;
        self
    }
    pub fn with_catalog_provenance(
        mut self,
        source: ModelCatalogSource,
        revision: Option<String>,
    ) -> Self {
        self.catalog_source = source;
        self.catalog_revision = revision;
        self
    }

    /// Freeze this mutable catalog row into a run-scoped capability snapshot.
    pub fn resolve_runtime(&self) -> ResolvedModelRuntime {
        ResolvedModelRuntime {
            key: self.key(),
            wire_model_id: self.id.clone(),
            display_name: self.display_name.clone(),
            capabilities: ModelCapabilities {
                context_window: self.context_window,
                max_output: self.max_output,
                supports_thinking: self.supports_thinking,
                reasoning_format: self.reasoning_format,
                supported_reasoning_levels: self.supported_reasoning_levels.clone(),
                default_reasoning_level: self.default_reasoning_level,
                reasoning_is_mandatory: self.reasoning_is_mandatory,
                reasoning_control: self.reasoning_control,
                fast_mode: self.fast_mode,
                supports_tools: self.supports_tools,
                supports_vision: self.supports_vision,
            },
            catalog_source: self.catalog_source,
            catalog_revision: self.catalog_revision.clone(),
            input_price: self.input_price,
            output_price: self.output_price,
        }
    }

    /// Builder: set context window
    pub fn with_context(mut self, context: usize, max_output: usize) -> Self {
        self.context_window = context;
        self.max_output = max_output;
        self
    }

    /// Builder: enable thinking support with specified format
    pub fn with_thinking(mut self, format: ReasoningFormat) -> Self {
        self.supports_thinking = true;
        self.reasoning_format = Some(format);
        self.reasoning_control = Some(match format {
            ReasoningFormat::OpenAI => ReasoningControl::OpenAiEffort,
            ReasoningFormat::Anthropic => ReasoningControl::AnthropicBudget,
            ReasoningFormat::DeepSeek => ReasoningControl::Boolean,
        });
        self
    }

    /// Builder: advertise exact reasoning controls from a provider catalog.
    pub fn with_reasoning_levels(
        mut self,
        levels: Vec<ReasoningEffort>,
        default: Option<ReasoningEffort>,
        mandatory: bool,
    ) -> Self {
        self.supports_thinking = true;
        self.supported_reasoning_levels = levels;
        self.default_reasoning_level = default;
        self.reasoning_is_mandatory = mandatory;
        self
    }

    pub fn with_reasoning_control(mut self, control: ReasoningControl) -> Self {
        self.reasoning_control = Some(control);
        self
    }

    pub fn with_fast_mode(mut self, fast_mode: FastMode) -> Self {
        self.fast_mode = Some(fast_mode);
        self
    }
}

/// Provider-specific TTL for dynamic model catalog refresh.
pub fn dynamic_model_cache_ttl(provider: ProviderId) -> u64 {
    match provider {
        // Match the official Codex catalog manager's five-minute freshness
        // window so newly entitled ChatGPT models appear promptly.
        ProviderId::OpenAI => 5 * 60,
        ProviderId::Anthropic | ProviderId::MiniMax => 6 * 60 * 60,
        ProviderId::OpenRouter => 12 * 60 * 60,
        _ => 24 * 60 * 60,
    }
}

/// Stable fingerprint for a model catalog, used to validate cached metadata.
pub fn model_catalog_fingerprint(models: &[ModelMetadata]) -> u64 {
    let mut items = models
        .iter()
        .map(|model| {
            format!(
                "{}|{}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{}|{:?}|{:?}|{}|{}|{:?}|{:?}",
                model.id,
                model.display_name,
                model.context_window,
                model.max_output,
                model.provider,
                model.auth_scope,
                model.api_format,
                model.reasoning_format,
                model.supported_reasoning_levels,
                model.default_reasoning_level,
                model.reasoning_is_mandatory,
                model.reasoning_control,
                model.fast_mode,
                model.supports_tools,
                model.supports_vision,
                model.input_price,
                model.output_price,
            )
        })
        .collect::<Vec<_>>();
    items.sort();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for item in items {
        item.hash(&mut hasher);
    }
    hasher.finish()
}
