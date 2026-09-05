//! Live Mitsuro Grok provider smoke test.
//!
//! This uses the Mitsuro AiClient path, not just the standalone grok-auth crate.
//! It requires a real X/Grok session and may consume quota, so it is gated:
//!
//!   MITSURO_GROK_LIVE=1 cargo run -p mitsuro-core --example grok_live_smoke

use anyhow::{anyhow, Context, Result};
use mitsuro_core::ai::client::{AiClient, AiClientConfig, CallOptions};
use mitsuro_core::ai::streaming::StreamPart;
use mitsuro_core::ai::types::{Content, ModelMessage, Role};
use mitsuro_core::auth::resolve_grok_auth;
use mitsuro_core::storage::CredentialStore;

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("MITSURO_GROK_LIVE").as_deref() != Ok("1") {
        println!("set MITSURO_GROK_LIVE=1 to run the live Grok provider smoke test");
        return Ok(());
    }

    let credentials = CredentialStore::load().unwrap_or_default();
    let resolved = resolve_grok_auth(&credentials);
    let token = resolved
        .credential
        .context("no Grok auth available; run `grok login` or Mitsuro's Grok OAuth flow")?;

    println!(
        "Resolved Grok auth source: {}",
        resolved.issuer_key.as_deref().unwrap_or("<unknown>")
    );

    let model =
        std::env::var("MITSURO_GROK_LIVE_MODEL").unwrap_or_else(|_| "grok-build".to_string());
    let config = AiClientConfig::for_grok(&model);
    let client = AiClient::new(config, token);

    let simple = client
        .call_simple(
            &model,
            "You are a concise smoke-test assistant.",
            "Reply with exactly: mitsuro grok simple ok",
            40,
        )
        .await?;
    println!("Simple response: {simple}");
    if !simple
        .to_ascii_lowercase()
        .contains("mitsuro grok simple ok")
    {
        return Err(anyhow!("unexpected simple response: {simple}"));
    }

    let messages = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Reply with exactly: mitsuro grok stream ok".to_string(),
        )],
    }];
    let mut rx = client
        .call_streaming(
            messages,
            &CallOptions {
                max_tokens: Some(40),
                system_prompt: Some("You are a concise smoke-test assistant.".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let mut streamed = String::new();
    while let Some(part) = rx.recv().await {
        match part {
            StreamPart::TextDelta { delta } => streamed.push_str(&delta),
            StreamPart::TextDeltaWithCitations { delta, .. } => streamed.push_str(&delta),
            StreamPart::Error { error } => return Err(anyhow!("stream error: {error}")),
            _ => {}
        }
    }

    println!("Streaming response: {streamed}");
    if !streamed
        .to_ascii_lowercase()
        .contains("mitsuro grok stream ok")
    {
        return Err(anyhow!("unexpected streaming response: {streamed}"));
    }

    Ok(())
}
