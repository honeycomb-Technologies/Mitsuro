//! Status messages emitted by terminal authentication flows.

/// OAuth authentication status update from background tasks
pub struct OAuthStatusUpdate {
    /// Provider being authenticated
    pub provider: mitsuro_core::ai::providers::ProviderId,
    /// Whether authentication succeeded
    pub success: bool,
    /// Status message or error
    pub message: String,
    /// Device code info (for device flow)
    pub device_code: Option<DeviceCodeInfo>,
    /// OAuth token data (on success)
    pub token: Option<mitsuro_core::auth::OAuthTokenData>,
}

/// Device code information for OAuth device flow
pub struct DeviceCodeInfo {
    pub user_code: String,
    pub verification_uri: String,
}
