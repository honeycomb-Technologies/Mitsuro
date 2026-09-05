use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::RwLock;

use super::super::providers::ProviderId;
use super::{
    model_catalog_fingerprint, ModelCatalogSource, ModelKey, ModelMetadata, OrganizedModels,
    ProjectModelRef, ResolvedModelRuntime,
};

/// A legacy bare model ID could not be resolved to exactly one executable row.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelLookupError {
    #[error("model '{model_id}' is not available")]
    NotFound { model_id: String },
    #[error("model '{model_id}' is ambiguous; select an explicit provider/model key")]
    Ambiguous {
        model_id: String,
        candidates: Vec<ModelKey>,
    },
}

/// Central provider-aware model registry.
///
/// The exact index uses [`ModelKey`]. A secondary bare-ID index exists only to
/// migrate old clients and preferences. Legacy resolution succeeds solely when
/// the slug maps to one row; ambiguity is never settled by insertion order.
pub struct ModelRegistry {
    /// All models indexed by provider for grouped display.
    models: RwLock<HashMap<ProviderId, Vec<ModelMetadata>>>,

    /// Exact executable identity -> index in that provider's vector.
    model_index: RwLock<HashMap<ModelKey, usize>>,

    /// Compatibility index for old bare-ID callers.
    legacy_index: RwLock<HashMap<String, Vec<ModelKey>>>,

    /// Recently used exact identities (most recent first).
    recent_keys: RwLock<Vec<ModelKey>>,

    max_recent: usize,
}

impl ModelRegistry {
    pub fn new() -> Self {
        Self {
            models: RwLock::new(HashMap::new()),
            model_index: RwLock::new(HashMap::new()),
            legacy_index: RwLock::new(HashMap::new()),
            recent_keys: RwLock::new(Vec::new()),
            max_recent: 10,
        }
    }

    /// Set models using whatever provenance is already encoded on each row.
    pub async fn set_models(&self, provider: ProviderId, models: Vec<ModelMetadata>) {
        self.set_models_with_catalog(provider, models, None, None)
            .await;
    }

    /// Replace one provider catalog while stamping explicit provenance.
    pub async fn set_models_with_catalog(
        &self,
        provider: ProviderId,
        models: Vec<ModelMetadata>,
        source: Option<ModelCatalogSource>,
        revision: Option<String>,
    ) {
        if models.is_empty() {
            tracing::warn!(?provider, "Ignoring empty model catalog refresh");
            return;
        }

        let revision = revision
            .or_else(|| source.map(|_| format!("{:016x}", model_catalog_fingerprint(&models))));
        let mut seen = HashSet::new();
        let models = models
            .into_iter()
            .filter_map(|mut model| {
                if model.provider != provider {
                    tracing::warn!(
                        requested_provider = ?provider,
                        row_provider = ?model.provider,
                        model = %model.id,
                        "Ignoring model catalog row assigned to the wrong provider"
                    );
                    return None;
                }
                if let Some(source) = source {
                    model.catalog_source = source;
                }
                if revision.is_some() {
                    model.catalog_revision.clone_from(&revision);
                }
                seen.insert(model.key()).then_some(model)
            })
            .collect::<Vec<_>>();

        if models.is_empty() {
            tracing::warn!(?provider, "Ignoring model catalog with no valid rows");
            return;
        }

        let mut exact_index = self.model_index.write().await;
        let mut legacy_index = self.legacy_index.write().await;
        let mut all_models = self.models.write().await;
        all_models.insert(provider, models);
        let (exact, legacy) = build_indexes(&all_models);
        *exact_index = exact;
        *legacy_index = legacy;
        drop(all_models);
        drop(legacy_index);
        drop(exact_index);
        self.prune_recents().await;
    }

    /// Insert or replace a single exact model identity.
    pub async fn upsert_model(&self, mut metadata: ModelMetadata) {
        if metadata.catalog_source == ModelCatalogSource::Legacy {
            metadata.catalog_source = ModelCatalogSource::Custom;
        }
        let provider = metadata.provider;
        let key = metadata.key();

        let mut exact_index = self.model_index.write().await;
        let mut legacy_index = self.legacy_index.write().await;
        let mut all_models = self.models.write().await;
        let provider_models = all_models.entry(provider).or_default();
        if let Some(slot) = provider_models.iter_mut().find(|model| model.key() == key) {
            *slot = metadata;
        } else {
            provider_models.push(metadata);
        }
        let (exact, legacy) = build_indexes(&all_models);
        *exact_index = exact;
        *legacy_index = legacy;
        drop(all_models);
        drop(legacy_index);
        drop(exact_index);
        self.prune_recents().await;
    }

    async fn prune_recents(&self) {
        let index = self.model_index.read().await;
        let mut recent = self.recent_keys.write().await;
        recent.retain(|key| index.contains_key(key));
        recent.truncate(self.max_recent);
    }

    /// Resolve one exact provider/auth/transport identity.
    pub async fn get_model_by_key(&self, key: &ModelKey) -> Option<ModelMetadata> {
        let index = self.model_index.read().await;
        let row = *index.get(key)?;
        self.models
            .read()
            .await
            .get(&key.provider)
            .and_then(|models| models.get(row))
            .cloned()
    }

    pub fn try_get_model_by_key(&self, key: &ModelKey) -> Option<ModelMetadata> {
        let index = self.model_index.try_read().ok()?;
        let row = *index.get(key)?;
        self.models
            .try_read()
            .ok()?
            .get(&key.provider)
            .and_then(|models| models.get(row))
            .cloned()
    }

    /// Freeze one exact catalog row for use by a run.
    pub async fn resolve_runtime(&self, key: &ModelKey) -> Option<ResolvedModelRuntime> {
        self.get_model_by_key(key)
            .await
            .map(|metadata| metadata.resolve_runtime())
    }

    /// Resolve a legacy bare ID, rejecting cross-provider/auth/transport ambiguity.
    pub async fn resolve_legacy_key(&self, model_id: &str) -> Result<ModelKey, ModelLookupError> {
        let index = self.legacy_index.read().await;
        resolve_legacy_from_index(&index, model_id)
    }

    /// Resolve a project setting to one exact executable catalog row.
    ///
    /// Exact keys never degrade to a bare-ID lookup, while legacy strings are
    /// accepted only when they map to exactly one row. Missing, stale, and
    /// ambiguous settings therefore fail closed before an `AiClient` exists.
    pub async fn resolve_project_model_ref(
        &self,
        model_ref: &ProjectModelRef,
    ) -> Result<ModelMetadata, ModelLookupError> {
        let key = match model_ref {
            ProjectModelRef::Exact(key) => key.clone(),
            ProjectModelRef::Legacy(model_id) => self.resolve_legacy_key(model_id.trim()).await?,
        };

        self.get_model_by_key(&key)
            .await
            .ok_or(ModelLookupError::NotFound {
                model_id: key.model_id,
            })
    }

    pub fn try_resolve_legacy_key(&self, model_id: &str) -> Result<ModelKey, ModelLookupError> {
        let index = self
            .legacy_index
            .try_read()
            .map_err(|_| ModelLookupError::NotFound {
                model_id: model_id.to_string(),
            })?;
        resolve_legacy_from_index(&index, model_id)
    }

    /// Legacy compatibility accessor. Ambiguous IDs intentionally return None.
    pub async fn get_model(&self, model_id: &str) -> Option<ModelMetadata> {
        let key = match self.resolve_legacy_key(model_id).await {
            Ok(key) => key,
            Err(ModelLookupError::Ambiguous { candidates, .. }) => {
                tracing::warn!(%model_id, ?candidates, "Refusing ambiguous bare model ID");
                return None;
            }
            Err(ModelLookupError::NotFound { .. }) => return None,
        };
        self.get_model_by_key(&key).await
    }

    pub async fn mark_recent_key(&self, key: &ModelKey) -> bool {
        if !self.model_index.read().await.contains_key(key) {
            return false;
        }
        let mut recent = self.recent_keys.write().await;
        recent.retain(|candidate| candidate != key);
        recent.insert(0, key.clone());
        recent.truncate(self.max_recent);
        true
    }

    pub async fn set_recent_keys(&self, keys: Vec<ModelKey>) {
        let index = self.model_index.read().await;
        let mut seen = HashSet::new();
        let keys = keys
            .into_iter()
            .filter(|key| index.contains_key(key) && seen.insert(key.clone()))
            .take(self.max_recent)
            .collect();
        *self.recent_keys.write().await = keys;
    }

    /// Load old recent model IDs only when each is unambiguous.
    pub async fn set_recent_ids(&self, ids: Vec<String>) {
        let legacy = self.legacy_index.read().await;
        let mut seen = HashSet::new();
        let keys = ids
            .iter()
            .filter_map(|id| resolve_legacy_from_index(&legacy, id).ok())
            .filter(|key| seen.insert(key.clone()))
            .take(self.max_recent)
            .collect();
        *self.recent_keys.write().await = keys;
    }

    pub async fn recent_keys(&self) -> Vec<ModelKey> {
        self.recent_keys.read().await.clone()
    }

    pub async fn get_organized_models(
        &self,
        configured_providers: &[ProviderId],
    ) -> OrganizedModels {
        let index = self.model_index.read().await;
        let models = self.models.read().await;
        let recent_keys = self.recent_keys.read().await;

        let recent_models = recent_keys
            .iter()
            .filter_map(|key| {
                let row = index.get(key)?;
                let model = models.get(&key.provider)?.get(*row)?;
                configured_providers
                    .contains(&model.provider)
                    .then(|| model.clone())
            })
            .collect();

        let by_provider = configured_providers
            .iter()
            .filter_map(|provider| {
                models
                    .get(provider)
                    .filter(|provider_models| !provider_models.is_empty())
                    .map(|provider_models| (*provider, provider_models.clone()))
            })
            .collect();

        (recent_models, by_provider)
    }
}

fn resolve_legacy_from_index(
    index: &HashMap<String, Vec<ModelKey>>,
    model_id: &str,
) -> Result<ModelKey, ModelLookupError> {
    let Some(candidates) = index.get(model_id) else {
        return Err(ModelLookupError::NotFound {
            model_id: model_id.to_string(),
        });
    };
    if candidates.len() == 1 {
        return Ok(candidates[0].clone());
    }
    Err(ModelLookupError::Ambiguous {
        model_id: model_id.to_string(),
        candidates: candidates.clone(),
    })
}

fn build_indexes(
    all_models: &HashMap<ProviderId, Vec<ModelMetadata>>,
) -> (HashMap<ModelKey, usize>, HashMap<String, Vec<ModelKey>>) {
    let mut exact = HashMap::new();
    let mut legacy: HashMap<String, Vec<ModelKey>> = HashMap::new();
    for models in all_models.values() {
        for (index, model) in models.iter().enumerate() {
            let key = model.key();
            exact.insert(key.clone(), index);
            legacy.entry(model.id.clone()).or_default().push(key);
        }
    }
    for keys in legacy.values_mut() {
        keys.sort_by_key(model_key_sort_key);
    }
    (exact, legacy)
}

fn model_key_sort_key(key: &ModelKey) -> (String, String, String, String) {
    (
        key.provider.storage_key().to_string(),
        format!("{:?}", key.auth_scope),
        format!("{:?}", key.api_format),
        key.model_id.clone(),
    )
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedModelRegistry = Arc<ModelRegistry>;

pub fn create_model_registry() -> SharedModelRegistry {
    Arc::new(ModelRegistry::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::models::{ApiFormat, ModelAuthScope};

    fn metadata(
        provider: ProviderId,
        id: &str,
        format: ApiFormat,
        auth_scope: Option<ModelAuthScope>,
    ) -> ModelMetadata {
        let mut metadata = ModelMetadata::new(id, id, provider).with_transport(format);
        metadata.auth_scope = auth_scope;
        metadata
    }

    #[tokio::test]
    async fn exact_keys_preserve_same_slug_across_providers() {
        let registry = ModelRegistry::new();
        let openai = metadata(
            ProviderId::OpenAI,
            "shared-model",
            ApiFormat::OpenAIResponses,
            Some(ModelAuthScope::ApiKey),
        );
        let router = metadata(
            ProviderId::OpenRouter,
            "shared-model",
            ApiFormat::Anthropic,
            None,
        );
        let openai_key = openai.key();
        let router_key = router.key();

        registry.set_models(ProviderId::OpenAI, vec![openai]).await;
        registry
            .set_models(ProviderId::OpenRouter, vec![router])
            .await;

        assert_eq!(
            registry
                .get_model_by_key(&openai_key)
                .await
                .unwrap()
                .provider,
            ProviderId::OpenAI
        );
        assert_eq!(
            registry
                .get_model_by_key(&router_key)
                .await
                .unwrap()
                .provider,
            ProviderId::OpenRouter
        );
        assert!(matches!(
            registry.resolve_legacy_key("shared-model").await,
            Err(ModelLookupError::Ambiguous { candidates, .. }) if candidates.len() == 2
        ));
        assert!(registry.get_model("shared-model").await.is_none());
    }

    #[tokio::test]
    async fn exact_keys_preserve_auth_and_transport_variants() {
        let registry = ModelRegistry::new();
        let api_key = metadata(
            ProviderId::OpenAI,
            "gpt-shared",
            ApiFormat::OpenAI,
            Some(ModelAuthScope::ApiKey),
        );
        let oauth = metadata(
            ProviderId::OpenAI,
            "gpt-shared",
            ApiFormat::OpenAIResponses,
            Some(ModelAuthScope::OAuth),
        );

        registry
            .set_models(ProviderId::OpenAI, vec![api_key.clone(), oauth.clone()])
            .await;

        assert!(registry.get_model_by_key(&api_key.key()).await.is_some());
        assert!(registry.get_model_by_key(&oauth.key()).await.is_some());
        assert!(matches!(
            registry.resolve_legacy_key("gpt-shared").await,
            Err(ModelLookupError::Ambiguous { candidates, .. }) if candidates.len() == 2
        ));
    }

    #[tokio::test]
    async fn project_model_refs_resolve_unique_legacy_and_exact_rows() {
        let registry = ModelRegistry::new();
        let grok = metadata(
            ProviderId::Grok,
            "grok-4.5",
            ApiFormat::OpenAIResponses,
            None,
        );
        let key = grok.key();
        registry.set_models(ProviderId::Grok, vec![grok]).await;

        let legacy = ProjectModelRef::Legacy("grok-4.5".to_string());
        assert_eq!(
            registry
                .resolve_project_model_ref(&legacy)
                .await
                .unwrap()
                .key(),
            key
        );
        assert_eq!(
            registry
                .resolve_project_model_ref(&ProjectModelRef::Exact(key.clone()))
                .await
                .unwrap()
                .key(),
            key
        );
    }

    #[tokio::test]
    async fn project_model_ref_rejects_ambiguous_legacy_slug() {
        let registry = ModelRegistry::new();
        registry
            .set_models(
                ProviderId::OpenAI,
                vec![metadata(
                    ProviderId::OpenAI,
                    "shared-model",
                    ApiFormat::OpenAIResponses,
                    Some(ModelAuthScope::ApiKey),
                )],
            )
            .await;
        registry
            .set_models(
                ProviderId::OpenRouter,
                vec![metadata(
                    ProviderId::OpenRouter,
                    "shared-model",
                    ApiFormat::Anthropic,
                    None,
                )],
            )
            .await;

        assert!(matches!(
            registry
                .resolve_project_model_ref(&ProjectModelRef::Legacy(
                    "shared-model".to_string()
                ))
                .await,
            Err(ModelLookupError::Ambiguous { candidates, .. }) if candidates.len() == 2
        ));
    }

    #[tokio::test]
    async fn project_model_ref_rejects_stale_exact_key() {
        let registry = ModelRegistry::new();
        let stale = ModelKey::new(ProviderId::Grok, "retired-grok", ApiFormat::OpenAIResponses);

        assert!(matches!(
            registry
                .resolve_project_model_ref(&ProjectModelRef::Exact(stale))
                .await,
            Err(ModelLookupError::NotFound { model_id }) if model_id == "retired-grok"
        ));
    }

    #[tokio::test]
    async fn runtime_snapshot_does_not_change_after_catalog_refresh() {
        let registry = ModelRegistry::new();
        let original = metadata(
            ProviderId::Grok,
            "grok-4.5",
            ApiFormat::OpenAIResponses,
            None,
        )
        .with_context(500_000, 32_768);
        let key = original.key();
        registry.set_models(ProviderId::Grok, vec![original]).await;
        let runtime = registry.resolve_runtime(&key).await.unwrap();

        let refreshed = metadata(
            ProviderId::Grok,
            "grok-4.5",
            ApiFormat::OpenAIResponses,
            None,
        )
        .with_context(256_000, 16_384);
        registry.set_models(ProviderId::Grok, vec![refreshed]).await;

        assert_eq!(runtime.capabilities.context_window, 500_000);
        assert_eq!(
            registry
                .resolve_runtime(&key)
                .await
                .unwrap()
                .capabilities
                .context_window,
            256_000
        );
    }
}
