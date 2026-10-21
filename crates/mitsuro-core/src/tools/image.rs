//! File loading utilities for vision and document support
//!
//! Handles loading images and PDFs from local files, URLs, and clipboard data.

use anyhow::{bail, Result};
use base64::Engine;
use std::path::Path;

use crate::ai::types::{Content, DocumentSource, ImageContent};

/// A loaded file ready for API submission (image or document)
pub struct LoadedFile {
    /// The Content block for the API (Image or Document)
    pub content: Content,
    /// Display name for UI (filename or truncated URL)
    pub display_name: String,
}

/// Load file from a local path (image or PDF)
pub fn load_from_path(path: &Path) -> Result<LoadedFile> {
    // Validate file exists
    if !path.exists() {
        bail!("File not found: {}", path.display());
    }

    // Get extension
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .unwrap_or_default();

    let display_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file")
        .to_string();

    // Handle PDF documents
    if ext == "pdf" {
        return load_pdf(path, display_name);
    }

    // Handle images
    let media_type = image_media_type(&ext)
        .ok_or_else(|| anyhow::anyhow!("Unsupported file format: .{}", ext))?;

    // Read and encode file
    let bytes = std::fs::read(path)?;

    // Check file size (allow up to 50MB for images - matches current web limit)
    if bytes.len() > 50 * 1024 * 1024 {
        bail!("Image too large: {} bytes (max 50MB)", bytes.len());
    }

    let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

    Ok(LoadedFile {
        content: Content::Image {
            image: ImageContent {
                url: None,
                base64: Some(base64_data),
                media_type: Some(media_type.to_string()),
            },
            detail: None,
        },
        display_name,
    })
}

/// Load PDF document from path
fn load_pdf(path: &Path, display_name: String) -> Result<LoadedFile> {
    let bytes = std::fs::read(path)?;

    // PDF size limit is 32MB
    if bytes.len() > 32 * 1024 * 1024 {
        bail!("PDF too large: {} bytes (max 32MB)", bytes.len());
    }

    let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

    Ok(LoadedFile {
        content: Content::Document {
            source: DocumentSource {
                source_type: "base64".to_string(),
                media_type: "application/pdf".to_string(),
                data: Some(base64_data),
                url: None,
            },
        },
        display_name,
    })
}

/// Load file from URL (passed through to API - Anthropic fetches it)
/// Note: URL-based loading only works for images, not PDFs
pub fn load_from_url(url: &str) -> Result<LoadedFile> {
    // Basic URL validation
    if !url.starts_with("http://") && !url.starts_with("https://") {
        bail!("Invalid URL: must start with http:// or https://");
    }

    // Truncate URL for display
    let display_name = if url.len() > 50 {
        format!("{}...", &url[..47])
    } else {
        url.to_string()
    };

    Ok(LoadedFile {
        content: Content::Image {
            image: ImageContent {
                url: Some(url.to_string()),
                base64: None,
                media_type: None,
            },
            detail: None,
        },
        display_name,
    })
}

/// Load image from clipboard RGBA data
pub fn load_from_clipboard_rgba(
    width: usize,
    height: usize,
    rgba_bytes: &[u8],
) -> Result<LoadedFile> {
    use image::{ImageBuffer, Rgba};

    // Create image from RGBA bytes
    let img: ImageBuffer<Rgba<u8>, Vec<u8>> =
        ImageBuffer::from_raw(width as u32, height as u32, rgba_bytes.to_vec())
            .ok_or_else(|| anyhow::anyhow!("Failed to create image from clipboard data"))?;

    // Encode to PNG
    let mut png_bytes = Vec::new();
    let mut cursor = std::io::Cursor::new(&mut png_bytes);
    img.write_to(&mut cursor, image::ImageFormat::Png)?;

    // Check size (allow up to 50MB)
    if png_bytes.len() > 50 * 1024 * 1024 {
        bail!(
            "Clipboard image too large: {} bytes (max 50MB)",
            png_bytes.len()
        );
    }

    let base64_data = base64::engine::general_purpose::STANDARD.encode(&png_bytes);

    Ok(LoadedFile {
        content: Content::Image {
            image: ImageContent {
                url: None,
                base64: Some(base64_data),
                media_type: Some("image/png".to_string()),
            },
            detail: None,
        },
        display_name: "clipboard.png".to_string(),
    })
}

/// Get image media type from file extension
fn image_media_type(ext: &str) -> Option<&'static str> {
    match ext {
        "jpg" | "jpeg" => Some("image/jpeg"),
        "png" => Some("image/png"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        _ => None,
    }
}

/// Check if a file extension is a supported format (image or PDF)
pub fn is_supported_file(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| {
            let ext = e.to_lowercase();
            image_media_type(&ext).is_some() || ext == "pdf"
        })
        .unwrap_or(false)
}
