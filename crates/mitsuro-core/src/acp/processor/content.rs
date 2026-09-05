use agent_client_protocol::{ContentBlock as AcpContent, EmbeddedResourceResource};

use crate::ai::types::Content;

/// Convert ACP content block to Mitsuro's Content type.
pub(super) fn convert_acp_content(block: AcpContent) -> Option<Content> {
    match block {
        AcpContent::Text(text) => Some(Content::text(text.text)),
        AcpContent::Resource(embedded) => match embedded.resource {
            EmbeddedResourceResource::TextResourceContents(text_resource) => {
                let formatted = format!(
                    "File: {}\n```\n{}\n```",
                    text_resource.uri, text_resource.text
                );
                tracing::debug!(
                    "Embedded resource: {} ({} bytes)",
                    text_resource.uri,
                    text_resource.text.len()
                );
                Some(Content::text(formatted))
            }
            EmbeddedResourceResource::BlobResourceContents(blob) => {
                let formatted = format!(
                    "[Binary file: {} ({})]",
                    blob.uri,
                    blob.mime_type.as_deref().unwrap_or("unknown type")
                );
                tracing::debug!("Binary resource: {}", blob.uri);
                Some(Content::text(formatted))
            }
            _ => {
                tracing::warn!("Unknown embedded resource type, skipping");
                None
            }
        },
        AcpContent::ResourceLink(link) => {
            let formatted = if let Some(desc) = link.description {
                format!("[File reference: {} - {}]", link.uri, desc)
            } else {
                format!("[File reference: {}]", link.uri)
            };
            tracing::debug!("Resource link: {}", link.uri);
            Some(Content::text(formatted))
        }
        AcpContent::Image(_) => {
            tracing::warn!("Image content not yet supported, skipping");
            None
        }
        AcpContent::Audio(_) => {
            tracing::warn!("Audio content not yet supported, skipping");
            None
        }
        _ => {
            tracing::warn!("Unknown content block type, skipping");
            None
        }
    }
}
