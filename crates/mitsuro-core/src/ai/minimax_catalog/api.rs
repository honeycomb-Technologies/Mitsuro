use anyhow::{bail, Result};
use reqwest::{Client, RequestBuilder};

use crate::ai::catalog::fetch_catalog_pages;
use crate::ai::models::ModelMetadata;

use super::mapping::parse_model;
use super::types::ModelsResponse;

const MINIMAX_MODELS_URL: &str = "https://api.minimax.io/anthropic/v1/models";

/// Fetch the live model IDs available to a MiniMax API key.
pub async fn fetch_models(api_key: &str) -> Result<Vec<ModelMetadata>> {
    fetch_models_with_client(api_key, None).await
}

/// Fetch MiniMax models with an optional shared HTTP client.
pub async fn fetch_models_with_client(
    api_key: &str,
    client: Option<&Client>,
) -> Result<Vec<ModelMetadata>> {
    if api_key.trim().is_empty() {
        bail!("MiniMax model discovery requires an API key");
    }

    let owned_client;
    let client = match client {
        Some(client) => client,
        None => {
            owned_client = Client::new();
            &owned_client
        }
    };

    fetch_catalog_pages::<ModelsResponse, _>(
        "MiniMax",
        |after_id| build_request(client, api_key, after_id),
        parse_model,
    )
    .await
}

fn build_request(client: &Client, api_key: &str, after_id: Option<&str>) -> RequestBuilder {
    let mut request = client.get(MINIMAX_MODELS_URL).header("x-api-key", api_key);
    if let Some(after_id) = after_id {
        request = request.query(&[("after_id", after_id)]);
    }
    request
}

#[cfg(test)]
mod tests {
    use super::build_request;
    use reqwest::Client;

    #[test]
    fn builds_x_api_key_catalog_request() {
        let request = build_request(&Client::new(), "minimax-test", Some("cursor"))
            .build()
            .expect("request builds");

        assert_eq!(request.headers()["x-api-key"], "minimax-test");
        assert!(request.headers().get("authorization").is_none());
        assert_eq!(request.url().query(), Some("after_id=cursor"));
    }
}
