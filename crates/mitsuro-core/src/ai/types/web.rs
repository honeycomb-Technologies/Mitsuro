use serde::{Deserialize, Serialize};

/// Web search tool configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebSearchConfig {
    /// Maximum number of searches per request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<u32>,
}

/// Web fetch tool configuration (beta)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebFetchConfig {
    /// Maximum number of fetches per request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<u32>,
    /// Enable citations for fetched content
    pub citations_enabled: bool,
    /// Maximum content length in tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_content_tokens: Option<u32>,
}

impl Default for WebFetchConfig {
    fn default() -> Self {
        Self {
            max_uses: Some(10),
            citations_enabled: true,
            max_content_tokens: Some(100_000),
        }
    }
}

/// A single web search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSearchResult {
    pub url: String,
    pub title: String,
    /// Encrypted content (must be passed back for citations)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted_content: Option<String>,
    /// When the page was last updated
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_age: Option<String>,
}

/// Web fetch result content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebFetchContent {
    pub url: String,
    /// The fetched content (text or base64 for PDFs)
    pub content: String,
    /// Media type (text/plain, application/pdf, etc.)
    pub media_type: String,
    /// Document title if available
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// When content was retrieved
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrieved_at: Option<String>,
}

/// Citation from web search or fetch
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Citation {
    pub url: String,
    pub title: String,
    /// The cited text (up to 150 chars for search)
    pub cited_text: String,
}
