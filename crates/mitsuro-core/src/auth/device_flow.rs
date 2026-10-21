//! Device code OAuth flow
//!
//! Implements RFC 8628 for OAuth 2.0 Device Authorization Grant.
//! This flow is designed for devices with limited input capabilities
//! or headless environments (SSH, containers, etc.).
//!
//! Flow:
//! 1. Request device code from authorization server
//! 2. Display user code and verification URL to user
//! 3. Poll token endpoint until user completes authorization

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use super::extract_openai_account_id;
use super::http::{read_auth_response, OAuthControlCode};
use super::types::{OAuthConfig, OAuthTokenData};

/// Response from the device authorization endpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceCodeResponse {
    /// The device verification code
    pub device_code: String,
    /// The end-user verification code to display
    pub user_code: String,
    /// The verification URI to show the user
    pub verification_uri: String,
    /// Optional verification URI with user_code embedded
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    /// Lifetime in seconds of the device_code and user_code
    pub expires_in: u64,
    /// Minimum interval in seconds between polling requests
    #[serde(default = "default_interval")]
    pub interval: u64,
}

fn default_interval() -> u64 {
    5
}

/// Device code OAuth flow handler
pub struct DeviceCodeFlow {
    config: OAuthConfig,
}

impl DeviceCodeFlow {
    /// Create a new device code flow handler
    pub fn new(config: OAuthConfig) -> Self {
        Self { config }
    }

    /// Request a device code from the authorization server
    pub async fn request_code(&self) -> Result<DeviceCodeResponse> {
        let device_auth_url = self
            .config
            .device_auth_url
            .as_ref()
            .ok_or_else(|| anyhow!("Provider does not support device code flow"))?;

        let client = reqwest::Client::new();

        let params = [
            ("client_id", self.config.client_id.as_str()),
            ("scope", &self.config.scopes.join(" ")),
        ];

        let response = client
            .post(device_auth_url)
            .form(&params)
            .send()
            .await
            .context("Failed to send device code request")?;
        let response = read_auth_response(response)
            .await
            .context("Failed to read device code response")?;

        if !response.status().is_success() {
            return Err(response.safe_error("Device code request failed"));
        }

        let device_response = response.parse_json("Device code response")?;

        Ok(device_response)
    }

    /// Poll the token endpoint for authorization completion
    ///
    /// This will poll at the specified interval until:
    /// - The user completes authorization (returns Ok with tokens)
    /// - The device code expires (returns Err)
    /// - The authorization is denied (returns Err)
    pub async fn poll_for_token(&self, device_code: &str, interval: u64) -> Result<OAuthTokenData> {
        let client = reqwest::Client::new();
        let poll_interval = Duration::from_secs(interval.max(1));

        loop {
            tokio::time::sleep(poll_interval).await;

            let params = [
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", &self.config.client_id),
                ("device_code", device_code),
            ];

            let response = client
                .post(&self.config.token_url)
                .form(&params)
                .send()
                .await
                .context("Failed to send token poll request")?;
            let response = read_auth_response(response)
                .await
                .context("Failed to read token poll response")?;

            let status = response.status();

            // Try to parse as token response first
            if status.is_success() {
                let token_response: TokenResponse = response.parse_json("OAuth token response")?;

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                let expires_at = token_response.expires_in.map(|secs| now + secs);

                let account_id =
                    extract_openai_account_id(&token_response.access_token).or_else(|| {
                        token_response
                            .id_token
                            .as_deref()
                            .and_then(extract_openai_account_id)
                    });

                return Ok(OAuthTokenData {
                    access_token: token_response.access_token,
                    refresh_token: token_response.refresh_token,
                    id_token: token_response.id_token,
                    expires_at,
                    last_refresh: now,
                    account_id,
                });
            }

            match response.oauth_control_code() {
                Some(OAuthControlCode::AuthorizationPending) => {
                    // User hasn't completed authorization yet, continue polling
                    continue;
                }
                Some(OAuthControlCode::SlowDown) => {
                    // We're polling too fast, wait an extra interval
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
                Some(OAuthControlCode::ExpiredToken) => {
                    return Err(anyhow!(
                        "Device code expired. Please restart the authorization process."
                    ));
                }
                Some(OAuthControlCode::AccessDenied) => {
                    return Err(anyhow!("Authorization was denied by the user."));
                }
                None => return Err(response.safe_error("Authorization failed")),
            }
        }
    }
}

/// Token response from the OAuth server
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(rename = "token_type", default)]
    _token_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::providers::ProviderId;

    #[test]
    fn test_device_code_response_deserialization() {
        let json = r#"{
            "device_code": "GmRhmhcxhwAzkoEqiMEg_DnyEysNkuNhszIySk9eS",
            "user_code": "WDJB-MJHT",
            "verification_uri": "https://example.com/device",
            "verification_uri_complete": "https://example.com/device?user_code=WDJB-MJHT",
            "expires_in": 1800,
            "interval": 5
        }"#;

        let response: DeviceCodeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.user_code, "WDJB-MJHT");
        assert_eq!(response.verification_uri, "https://example.com/device");
        assert_eq!(response.expires_in, 1800);
        assert_eq!(response.interval, 5);
    }

    #[test]
    fn test_device_code_response_minimal() {
        // Test with minimal required fields
        let json = r#"{
            "device_code": "abc123",
            "user_code": "XYZ-789",
            "verification_uri": "https://example.com/device",
            "expires_in": 600
        }"#;

        let response: DeviceCodeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.device_code, "abc123");
        assert_eq!(response.interval, 5); // Default value
        assert!(response.verification_uri_complete.is_none());
    }

    #[test]
    fn test_no_device_auth_url() {
        let config = OAuthConfig {
            provider_id: ProviderId::OpenAI,
            client_id: "test".to_string(),
            authorization_url: "https://auth.example.com/authorize".to_string(),
            token_url: "https://auth.example.com/token".to_string(),
            device_auth_url: None, // No device flow support
            scopes: vec![],
            refresh_days: 28,
            extra_auth_params: vec![],
        };

        let flow = DeviceCodeFlow::new(config);
        // Note: We can't easily test the async method without a mock server,
        // but we can verify the flow is created
        assert!(flow.config.device_auth_url.is_none());
    }
}
