use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Citation;

/// AI SDK Tool definition (for provider communication only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    /// Extended prompt for system prompt injection (internal only, not sent to providers)
    #[serde(skip)]
    pub prompt: Option<String>,
}

/// AI SDK Tool call (for provider communication only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// Message role in a conversation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

/// Content types that can be in a message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Content {
    #[serde(rename = "text")]
    Text {
        text: String,
        /// Source references associated with this text block. Older stored
        /// messages omit this field; older readers still recognize the text.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        citations: Vec<Citation>,
    },

    #[serde(rename = "image")]
    Image {
        image: ImageContent,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },

    /// Document content (PDF)
    #[serde(rename = "document")]
    Document { source: DocumentSource },

    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },

    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        output: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
    },

    /// Extended thinking content block
    #[serde(rename = "thinking")]
    Thinking { thinking: String, signature: String },

    /// Redacted thinking (when thinking contains sensitive content)
    #[serde(rename = "redacted_thinking")]
    RedactedThinking { data: String },
}

impl Content {
    /// Plain text without provider source annotations.
    pub fn text(text: String) -> Self {
        Self::Text {
            text,
            citations: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

/// Document source for PDF content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentSource {
    /// Source type: "base64" or "url"
    #[serde(rename = "type")]
    pub source_type: String,
    /// MIME type (e.g., "application/pdf")
    pub media_type: String,
    /// Base64-encoded content (when source_type is "base64")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    /// URL to fetch (when source_type is "url")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Unified message format for provider communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: Role,
    pub content: Vec<Content>,
}

/// Finish reasons for model generation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    Other(String),
}

#[cfg(test)]
mod citation_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn annotated_text_preserves_old_wire_shape_and_roundtrips_sources() {
        let old = json!({"type": "text", "text": "Old answer."});
        let decoded: Content = serde_json::from_value(old.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), old);

        let annotated = json!({
            "type": "text", "text": "A supported finding.",
            "citations": [{"url":"https://example.org/evidence", "title":"Evidence", "cited_text":"Supported finding"}]
        });
        let decoded: Content = serde_json::from_value(annotated.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), annotated);
        #[derive(Deserialize)]
        struct LegacyText {
            text: String,
        }
        let legacy: LegacyText = serde_json::from_value(annotated).unwrap();
        assert_eq!(legacy.text, "A supported finding.");
    }
}
