//! Authentication helpers for the TUI
//!
//! Extracted auth logic to reduce app.rs complexity.

use crate::ai::client::AiClientConfig;
use crate::ai::models::{ModelAuthScope, ModelLookupError, ModelMetadata, SharedModelRegistry};
use crate::ai::providers::{get_provider, ProviderId};
use crate::storage::CredentialStore;
use mitsuro_core::ai::catalog::{
    credentials_for_dynamic_models, CatalogAuthKind, CatalogCredential,
};
use mitsuro_core::auth::{OpenAIAuthResolution, OpenAIAuthType};

/// Create AiClientConfig for a provider/model combination
///
/// Handles special cases:
/// - OpenAI: OAuth vs API key detection for endpoint routing
/// - Others: exact catalog-selected wire format and output limit
pub fn create_client_config(
    metadata: &ModelMetadata,
    credential_store: &CredentialStore,
) -> AiClientConfig {
    let provider = metadata.provider;
    let model = metadata.id.as_str();

    // Anthropic requires special handling to detect OAuth vs API key
    // and set the correct auth header (Bearer for OAuth, x-api-key for API key)
    if provider == ProviderId::Anthropic {
        let mut config = AiClientConfig::for_anthropic_with_auth_detection(model, credential_store);
        config.api_format = metadata.api_format;
        config.max_tokens = metadata.max_output;
        return config;
    }

    // OpenAI requires special handling to detect OAuth vs API key
    // and route to the correct endpoint (ChatGPT Responses API vs OpenAI Chat Completions)
    if provider == ProviderId::OpenAI {
        let resolution = resolve_openai_auth_for_metadata(metadata, credential_store);
        let mut config = AiClientConfig::for_openai_with_auth_resolution(model, resolution);
        config.api_format = metadata.api_format;
        config.max_tokens = metadata.max_output;
        return config;
    }

    // Grok uses the Grok CLI proxy with model-specific OpenAI-compatible routing.
    if provider == ProviderId::Grok {
        return AiClientConfig::for_grok_with_metadata(metadata);
    }

    let provider_config = match get_provider(provider) {
        Some(config) => config,
        None => {
            tracing::warn!("Provider {:?} not found, falling back to MiniMax", provider);
            match get_provider(ProviderId::MiniMax) {
                Some(config) => config,
                None => {
                    tracing::error!(
                        "MiniMax fallback provider not available, using default config"
                    );
                    return AiClientConfig {
                        model: model.to_string(),
                        ..AiClientConfig::default()
                    };
                }
            }
        }
    };

    AiClientConfig {
        model: model.to_string(),
        max_tokens: metadata.max_output,
        base_url: Some(provider_config.base_url.clone()),
        auth_header: provider_config.auth_header,
        provider_id: provider_config.id,
        api_format: metadata.api_format,
        custom_headers: provider_config.custom_headers.clone(),
    }
}

/// Resolve the credential surface that advertised the selected OpenAI model.
///
/// API-key and ChatGPT OAuth catalogs can contain the same model ID with
/// different request transports. When live metadata has provenance, fail
/// closed if that exact credential surface is no longer available instead of
/// silently routing the model through the other account.
pub fn resolve_openai_auth_for_metadata(
    metadata: &ModelMetadata,
    credential_store: &CredentialStore,
) -> OpenAIAuthResolution {
    let Some(auth_scope) = metadata.auth_scope else {
        return mitsuro_core::auth::resolve_openai_auth(credential_store, &metadata.id);
    };

    resolve_scoped_openai_auth(
        auth_scope,
        credentials_for_dynamic_models(ProviderId::OpenAI, credential_store),
    )
}

fn resolve_scoped_openai_auth(
    auth_scope: ModelAuthScope,
    identities: Vec<CatalogCredential>,
) -> OpenAIAuthResolution {
    let desired = match auth_scope {
        ModelAuthScope::ApiKey => CatalogAuthKind::ApiKey,
        ModelAuthScope::OAuth => CatalogAuthKind::OAuth,
    };

    identities
        .into_iter()
        .find(|identity| identity.kind == desired)
        .map(openai_resolution_for_catalog_identity)
        .unwrap_or(OpenAIAuthResolution {
            auth_type: OpenAIAuthType::None,
            credential: None,
            account_id: None,
        })
}

fn openai_resolution_for_catalog_identity(identity: CatalogCredential) -> OpenAIAuthResolution {
    OpenAIAuthResolution {
        auth_type: match identity.kind {
            CatalogAuthKind::ApiKey => OpenAIAuthType::ApiKey,
            CatalogAuthKind::OAuth => OpenAIAuthType::ChatGptOAuth,
        },
        credential: Some(identity.credential().to_string()),
        account_id: identity.account_id,
    }
}

pub fn infer_provider_for_model(
    model_registry: &SharedModelRegistry,
    model: &str,
) -> Option<ProviderId> {
    let model = model.trim();
    if model.is_empty() {
        return None;
    }

    match model_registry.try_resolve_legacy_key(model) {
        Ok(key) => return Some(key.provider),
        Err(ModelLookupError::Ambiguous { .. }) => return None,
        Err(ModelLookupError::NotFound { .. }) => {}
    }

    let mut candidates = ProviderId::all().iter().filter_map(|provider| {
        get_provider(*provider)
            .filter(|config| config.has_model(model))
            .map(|config| config.id)
    });
    let first = candidates.next()?;
    candidates.next().is_none().then_some(first)
}

#[cfg(test)]
mod tests {
    use super::{create_client_config, infer_provider_for_model, resolve_scoped_openai_auth};
    use crate::ai::models::{create_model_registry, ApiFormat, ModelAuthScope, ModelMetadata};
    use crate::ai::providers::ProviderId;
    use crate::storage::CredentialStore;
    use mitsuro_core::ai::catalog::CatalogCredential;
    use mitsuro_core::auth::OpenAIAuthType;

    #[test]
    fn scoped_openai_auth_uses_the_exact_catalog_identity() {
        let identities = vec![
            CatalogCredential::api_key("api-secret".to_string()),
            CatalogCredential::oauth("oauth-secret".to_string(), Some("account-id".to_string())),
        ];

        let api_key = resolve_scoped_openai_auth(ModelAuthScope::ApiKey, identities.clone());
        assert_eq!(api_key.auth_type, OpenAIAuthType::ApiKey);
        assert_eq!(api_key.credential.as_deref(), Some("api-secret"));
        assert_eq!(api_key.account_id, None);

        let oauth = resolve_scoped_openai_auth(ModelAuthScope::OAuth, identities);
        assert_eq!(oauth.auth_type, OpenAIAuthType::ChatGptOAuth);
        assert_eq!(oauth.credential.as_deref(), Some("oauth-secret"));
        assert_eq!(oauth.account_id.as_deref(), Some("account-id"));
    }

    #[test]
    fn scoped_openai_auth_does_not_fall_back_to_another_surface() {
        let resolution = resolve_scoped_openai_auth(
            ModelAuthScope::OAuth,
            vec![CatalogCredential::api_key("api-secret".to_string())],
        );

        assert_eq!(resolution.auth_type, OpenAIAuthType::None);
        assert!(resolution.credential.is_none());
        assert!(resolution.account_id.is_none());
    }

    #[test]
    fn grok_client_config_uses_the_exact_catalog_transport() {
        let metadata = ModelMetadata::new("grok-chat", "Grok Chat", ProviderId::Grok)
            .with_transport(ApiFormat::OpenAI)
            .with_context(196_608, 12_345);

        let config = create_client_config(&metadata, &CredentialStore::default());

        assert_eq!(config.model, "grok-chat");
        assert_eq!(config.api_format, ApiFormat::OpenAI);
        assert_eq!(config.max_tokens, 12_345);
        assert!(config
            .base_url
            .as_deref()
            .is_some_and(|url| url.ends_with("/chat/completions")));
        assert_eq!(
            config
                .custom_headers
                .get("x-grok-context-window")
                .map(String::as_str),
            Some("196608")
        );
    }

    #[test]
    fn legacy_provider_inference_rejects_duplicate_slugs() {
        let registry = create_model_registry();
        futures::executor::block_on(async {
            registry
                .upsert_model(ModelMetadata::new(
                    "shared-test-model",
                    "Shared",
                    ProviderId::OpenAI,
                ))
                .await;
            registry
                .upsert_model(ModelMetadata::new(
                    "shared-test-model",
                    "Shared",
                    ProviderId::OpenRouter,
                ))
                .await;
        });

        assert_eq!(
            infer_provider_for_model(&registry, "shared-test-model"),
            None
        );
    }
}
