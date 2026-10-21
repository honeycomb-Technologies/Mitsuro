//! Shared provider model catalog helpers.

use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use reqwest::RequestBuilder;
use serde::de::DeserializeOwned;
use tracing::debug;

use crate::auth::{
    resolve_anthropic_auth, resolve_grok_auth, resolve_openai_auth, AnthropicAuthType,
    OpenAIAuthMode, OpenAIAuthType,
};
use crate::storage::CredentialStore;

use super::models::ModelMetadata;
use super::providers::{get_provider, ProviderId};

pub(crate) const MAX_CATALOG_PAGES: usize = 100;

/// A provider model-catalog page: raw entries plus the cursor contract.
pub(crate) trait CatalogPage {
    type Entry;
    fn entries(self) -> Vec<Self::Entry>;
    fn has_more(&self) -> bool;
    fn last_id(&self) -> Option<&str>;
}

/// Fetch every page of a cursor-paginated provider model catalog.
///
/// Shared by providers whose `/models` endpoints share the same page contract
/// (`data` + `has_more` + `last_id`). A partial catalog is unsafe to publish
/// because it would replace the last-known-good snapshot, so any pagination
/// failure aborts the whole refresh.
pub(crate) async fn fetch_catalog_pages<P, F>(
    provider: &'static str,
    mut build_request: impl FnMut(Option<&str>) -> RequestBuilder,
    mut parse_entry: F,
) -> Result<Vec<ModelMetadata>>
where
    P: DeserializeOwned + CatalogPage,
    F: FnMut(P::Entry) -> Option<ModelMetadata>,
{
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut models = Vec::new();
    let mut page_count = 0usize;
    loop {
        page_count += 1;
        if page_count > MAX_CATALOG_PAGES {
            bail!("{provider} model catalog exceeded {MAX_CATALOG_PAGES} pages");
        }
        let response = build_request(cursor.as_deref()).send().await?;
        let status = response.status();
        if !status.is_success() {
            let detail = response.text().await.unwrap_or_default();
            bail!("{provider} models API error: {status} - {detail}");
        }

        let page = response.json::<P>().await?;
        let next_cursor =
            next_catalog_cursor(provider, page.has_more(), page.last_id(), &mut seen_cursors)?;
        models.extend(page.entries().into_iter().filter_map(&mut parse_entry));

        match next_cursor {
            Some(next_cursor) => cursor = Some(next_cursor),
            None => break,
        }
    }

    models.sort_by(|left, right| left.id.cmp(&right.id));
    models.dedup_by(|left, right| left.id == right.id);
    debug!(count = models.len(), "Fetched {provider} model catalog");
    Ok(models)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogAuthKind {
    ApiKey,
    OAuth,
}

/// Authentication context for account-scoped model discovery.
///
/// The credential is deliberately private so debug output cannot accidentally
/// print it. Provider adapters access it through the read-only accessor.
#[derive(Clone)]
pub struct CatalogCredential {
    credential: String,
    pub kind: CatalogAuthKind,
    pub account_id: Option<String>,
}

impl CatalogCredential {
    pub fn api_key(credential: String) -> Self {
        Self {
            credential,
            kind: CatalogAuthKind::ApiKey,
            account_id: None,
        }
    }

    pub fn oauth(credential: String, account_id: Option<String>) -> Self {
        Self {
            credential,
            kind: CatalogAuthKind::OAuth,
            account_id,
        }
    }

    pub fn credential(&self) -> &str {
        &self.credential
    }
}

/// Whether the provider supports runtime model discovery.
pub fn supports_dynamic_models(provider: ProviderId) -> bool {
    get_provider(provider)
        .map(|config| config.dynamic_models)
        .unwrap_or(false)
}

/// Providers that expose a live `/models` (or equivalent) catalog at runtime.
pub fn dynamic_model_providers() -> Vec<ProviderId> {
    ProviderId::all()
        .iter()
        .copied()
        .filter(|provider| supports_dynamic_models(*provider))
        .collect()
}

/// Validate a provider's cursor-based pagination contract.
///
/// A partial catalog is unsafe to publish because it would replace the
/// last-known-good snapshot. Missing or cycling cursors therefore fail the
/// entire refresh instead of returning the pages collected so far.
pub(crate) fn next_catalog_cursor(
    provider: &str,
    has_more: bool,
    last_id: Option<&str>,
    seen: &mut HashSet<String>,
) -> Result<Option<String>> {
    if !has_more {
        return Ok(None);
    }

    let cursor = last_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{provider} model catalog has another page but no cursor"))?
        .to_string();
    if !seen.insert(cursor.clone()) {
        anyhow::bail!("{provider} model catalog repeated pagination cursor {cursor}");
    }
    Ok(Some(cursor))
}

/// Resolve every usable catalog identity for a provider.
///
/// OpenAI may legitimately have both an API key and a ChatGPT OAuth account;
/// their catalogs are fetched independently and merged by model ID. Other
/// providers currently expose one effective catalog identity.
pub fn credentials_for_dynamic_models(
    provider: ProviderId,
    credentials: &CredentialStore,
) -> Vec<CatalogCredential> {
    credentials_for_dynamic_models_with_env(provider, credentials, env_credential)
}

fn credentials_for_dynamic_models_with_env(
    provider: ProviderId,
    credentials: &CredentialStore,
    env: impl Fn(&str) -> Option<String>,
) -> Vec<CatalogCredential> {
    match provider {
        ProviderId::OpenRouter => credentials
            .get(&ProviderId::OpenRouter)
            .cloned()
            .or_else(|| env("OPENROUTER_API_KEY"))
            .map(CatalogCredential::api_key)
            .into_iter()
            .collect(),
        ProviderId::OpenAI => {
            let mode = OpenAIAuthMode::from_env();
            let mut auth = Vec::new();
            if mode != OpenAIAuthMode::OAuth {
                if let Some(key) = credentials
                    .get(&ProviderId::OpenAI)
                    .cloned()
                    .or_else(|| env("OPENAI_API_KEY"))
                {
                    auth.push(CatalogCredential::api_key(key));
                }
            }

            if mode != OpenAIAuthMode::ApiKey {
                let resolved = resolve_openai_auth(credentials, "gpt-5.3-codex");
                if resolved.auth_type == OpenAIAuthType::ChatGptOAuth {
                    if let Some(token) = resolved.credential {
                        if !auth.iter().any(|entry| entry.credential == token) {
                            auth.push(CatalogCredential::oauth(token, resolved.account_id));
                        }
                    }
                }
            }
            auth
        }
        ProviderId::Anthropic => {
            let resolved = resolve_anthropic_auth(credentials);
            resolved
                .credential
                .or_else(|| env("ANTHROPIC_API_KEY"))
                .map(|credential| {
                    if resolved.auth_type == AnthropicAuthType::OAuth {
                        CatalogCredential::oauth(credential, None)
                    } else {
                        CatalogCredential::api_key(credential)
                    }
                })
                .into_iter()
                .collect()
        }
        ProviderId::MiniMax => credentials
            .get(&ProviderId::MiniMax)
            .cloned()
            .or_else(|| env("MINIMAX_API_KEY"))
            .map(CatalogCredential::api_key)
            .into_iter()
            .collect(),
        ProviderId::Grok => resolve_grok_auth(credentials)
            .credential
            .or_else(|| env("GROK_ACCESS_TOKEN"))
            .map(|credential| CatalogCredential::oauth(credential, None))
            .into_iter()
            .collect(),
        ProviderId::ZAi => Vec::new(),
    }
}

fn env_credential(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Fetch runtime model metadata using one catalog identity.
pub async fn fetch_dynamic_models(
    provider: ProviderId,
    auth: &CatalogCredential,
) -> Result<Vec<ModelMetadata>> {
    match provider {
        ProviderId::OpenRouter => super::openrouter::fetch_models(auth.credential()).await,
        ProviderId::OpenAI if auth.kind == CatalogAuthKind::OAuth => {
            super::openai::fetch_chatgpt_models(auth.credential(), auth.account_id.as_deref()).await
        }
        ProviderId::OpenAI => super::openai::fetch_models(auth.credential()).await,
        ProviderId::Anthropic => {
            super::anthropic_catalog::fetch_models(
                auth.credential(),
                auth.kind == CatalogAuthKind::OAuth,
            )
            .await
        }
        ProviderId::MiniMax => super::minimax_catalog::fetch_models(auth.credential()).await,
        ProviderId::Grok => super::grok::fetch_models(auth.credential()).await,
        ProviderId::ZAi => anyhow::bail!("Z.ai does not expose a model-list endpoint"),
    }
}

/// Fetch and combine every configured catalog identity for a provider.
///
/// Every configured identity must succeed. Publishing only the API-key or only
/// the OAuth half of an OpenAI catalog would erase models available through the
/// failed sibling identity and incorrectly mark that reduced snapshot fresh.
pub async fn fetch_dynamic_models_for_store(
    provider: ProviderId,
    credentials: &CredentialStore,
) -> Result<Vec<ModelMetadata>> {
    let auth = credentials_for_dynamic_models(provider, credentials);
    if auth.is_empty() {
        anyhow::bail!("No catalog credential configured for {provider}");
    }

    let mut catalogs = Vec::with_capacity(auth.len());
    for identity in auth {
        let kind = identity.kind;
        catalogs.push(
            fetch_dynamic_models(provider, &identity)
                .await
                .with_context(|| format!("{provider} {kind:?} catalog identity failed")),
        );
    }

    merge_identity_catalogs(catalogs)
}

fn merge_identity_catalogs(
    catalogs: impl IntoIterator<Item = Result<Vec<ModelMetadata>>>,
) -> Result<Vec<ModelMetadata>> {
    let mut merged = Vec::<ModelMetadata>::new();
    for catalog in catalogs {
        let models = catalog?;
        if models.is_empty() {
            anyhow::bail!("Provider returned an empty model catalog");
        }
        for model in models {
            let key = model.key();
            if let Some(existing) = merged.iter_mut().find(|item| item.key() == key) {
                // Refreshes may replace the same exact provider/auth/transport
                // row without reordering. Rows that share a wire slug but use
                // different credential surfaces remain independently routable.
                *existing = model;
            } else {
                merged.push(model);
            }
        }
    }

    if merged.is_empty() {
        anyhow::bail!("Model catalog refresh produced no models");
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        credentials_for_dynamic_models_with_env, merge_identity_catalogs, next_catalog_cursor,
        CatalogAuthKind,
    };
    use crate::ai::models::{ModelAuthScope, ModelMetadata};
    use crate::ai::providers::ProviderId;
    use crate::storage::CredentialStore;

    #[test]
    fn openai_api_catalog_reads_stored_key() {
        let mut credentials = CredentialStore::default();
        credentials.set(ProviderId::OpenAI, "sk-openai".to_string());

        let auth =
            credentials_for_dynamic_models_with_env(ProviderId::OpenAI, &credentials, |_| None);

        assert!(auth.iter().any(|entry| {
            entry.kind == CatalogAuthKind::ApiKey && entry.credential() == "sk-openai"
        }));
    }

    #[test]
    fn static_zai_provider_has_no_runtime_catalog_identity() {
        let mut credentials = CredentialStore::default();
        credentials.set(ProviderId::ZAi, "zai-key".to_string());

        assert!(
            credentials_for_dynamic_models_with_env(ProviderId::ZAi, &credentials, |_| Some(
                "env-key".to_string()
            ),)
            .is_empty()
        );
    }

    #[test]
    fn dynamic_provider_list_includes_new_direct_catalogs() {
        let providers = super::dynamic_model_providers();
        assert!(providers.contains(&ProviderId::OpenRouter));
        assert!(providers.contains(&ProviderId::OpenAI));
        assert!(providers.contains(&ProviderId::Grok));
        assert!(providers.contains(&ProviderId::MiniMax));
        assert!(providers.contains(&ProviderId::Anthropic));
        assert!(!providers.contains(&ProviderId::ZAi));
    }

    #[test]
    fn pagination_rejects_missing_and_repeated_cursors() {
        let mut seen = HashSet::new();
        assert!(next_catalog_cursor("Test", true, None, &mut seen).is_err());
        assert_eq!(
            next_catalog_cursor("Test", true, Some("page-2"), &mut seen).unwrap(),
            Some("page-2".to_string())
        );
        assert!(next_catalog_cursor("Test", true, Some("page-2"), &mut seen).is_err());
        assert_eq!(
            next_catalog_cursor("Test", false, Some("ignored"), &mut seen).unwrap(),
            None
        );
    }

    #[test]
    fn multi_identity_merge_fails_closed_and_preserves_exact_auth_variants() {
        let mut api = ModelMetadata::new("shared", "API", ProviderId::OpenAI);
        api.auth_scope = Some(ModelAuthScope::ApiKey);
        let mut oauth = ModelMetadata::new("shared", "OAuth", ProviderId::OpenAI);
        oauth.auth_scope = Some(ModelAuthScope::OAuth);
        let merged = merge_identity_catalogs(vec![Ok(vec![api]), Ok(vec![oauth])]).unwrap();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].display_name, "API");
        assert_eq!(merged[0].auth_scope, Some(ModelAuthScope::ApiKey));
        assert_eq!(merged[1].display_name, "OAuth");
        assert_eq!(merged[1].auth_scope, Some(ModelAuthScope::OAuth));

        let mut refreshed = ModelMetadata::new("shared", "API refreshed", ProviderId::OpenAI);
        refreshed.auth_scope = Some(ModelAuthScope::ApiKey);
        let replaced =
            merge_identity_catalogs(vec![Ok(vec![merged[0].clone()]), Ok(vec![refreshed])])
                .unwrap();
        assert_eq!(replaced.len(), 1);
        assert_eq!(replaced[0].display_name, "API refreshed");

        let partial = merge_identity_catalogs(vec![
            Ok(vec![ModelMetadata::new(
                "api-only",
                "API only",
                ProviderId::OpenAI,
            )]),
            Err(anyhow::anyhow!("OAuth unavailable")),
        ]);
        assert!(partial.is_err());
    }
}
