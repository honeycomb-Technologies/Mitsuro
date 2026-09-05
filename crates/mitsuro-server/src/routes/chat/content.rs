use mitsuro_core::ai::types::{Content, ImageContent};
use reqwest::Url;

use crate::error::AppError;
use crate::types::{ContentBlock, ImageSource};

const SUPPORTED_BASE64_IMAGE_MEDIA_TYPES: &[&str] =
    &["image/jpeg", "image/png", "image/gif", "image/webp"];
const SUPPORTED_REMOTE_IMAGE_URL_SCHEMES: &[&str] = &["http", "https"];

enum NormalizedImageSource<'a> {
    Base64 {
        media_type: &'static str,
        data: &'a str,
    },
    Url {
        url: String,
    },
}

fn normalize_supported_base64_image_media_type(media_type: &str) -> Option<&'static str> {
    match media_type.trim().to_ascii_lowercase().as_str() {
        "image/jpeg" | "image/jpg" | "image/pjpeg" => Some("image/jpeg"),
        "image/png" => Some("image/png"),
        "image/gif" => Some("image/gif"),
        "image/webp" => Some("image/webp"),
        _ => None,
    }
}

fn normalize_remote_image_url(url: &str) -> Result<String, AppError> {
    let trimmed = url.trim();
    let parsed = Url::parse(trimmed).map_err(|_| invalid_image_url_error(url))?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    if !SUPPORTED_REMOTE_IMAGE_URL_SCHEMES.contains(&scheme.as_str()) || parsed.host_str().is_none()
    {
        return Err(invalid_image_url_error(url));
    }

    Ok(parsed.to_string())
}

fn normalize_image_source(source: &ImageSource) -> Result<NormalizedImageSource<'_>, AppError> {
    match source {
        ImageSource::Base64 { media_type, data } => {
            let media_type = normalize_supported_base64_image_media_type(media_type)
                .ok_or_else(|| unsupported_image_media_type_error(media_type))?;
            Ok(NormalizedImageSource::Base64 { media_type, data })
        }
        ImageSource::Url { url } => Ok(NormalizedImageSource::Url {
            url: normalize_remote_image_url(url)?,
        }),
    }
}

fn unsupported_image_media_type_error(media_type: &str) -> AppError {
    let normalized = media_type.trim().to_ascii_lowercase();
    let hint = if matches!(normalized.as_str(), "image/heic" | "image/heif") {
        " Convert HEIC/HEIF images to JPEG or PNG before uploading."
    } else {
        ""
    };

    AppError::BadRequest(format!(
        "Image format '{}' is not supported. Supported formats: {}.{hint}",
        media_type.trim(),
        SUPPORTED_BASE64_IMAGE_MEDIA_TYPES.join(", "),
    ))
}

fn invalid_image_url_error(url: &str) -> AppError {
    AppError::BadRequest(format!(
        "Image URL '{}' is invalid. Use an absolute http or https URL.",
        url.trim()
    ))
}

fn build_image_content(source: &ImageSource) -> Result<ImageContent, AppError> {
    match normalize_image_source(source)? {
        NormalizedImageSource::Base64 { media_type, data } => {
            tracing::debug!(
                "Content block: Image (base64, media_type={}, data_len={})",
                media_type,
                data.len()
            );
            Ok(ImageContent {
                base64: Some(data.to_string()),
                url: None,
                media_type: Some(media_type.to_string()),
            })
        }
        NormalizedImageSource::Url { url } => {
            tracing::debug!("Content block: Image (url={})", url);
            Ok(ImageContent {
                base64: None,
                url: Some(url),
                media_type: None,
            })
        }
    }
}

pub(super) fn validate_content_blocks(content_blocks: &[ContentBlock]) -> Result<(), AppError> {
    for block in content_blocks {
        if let ContentBlock::Image { source } = block {
            normalize_image_source(source)?;
        }
    }

    Ok(())
}

/// Build user message content from content blocks (images) and text message.
pub(super) fn build_user_content(
    message: &str,
    content_blocks: &[ContentBlock],
) -> Result<Vec<Content>, AppError> {
    let mut contents = Vec::with_capacity(content_blocks.len() + usize::from(!message.is_empty()));
    let mut has_text_block = false;

    for block in content_blocks {
        match block {
            ContentBlock::Text { text } => {
                tracing::debug!("Content block: Text ({} chars)", text.len());
                has_text_block = true;
                contents.push(Content::text(text.clone()));
            }
            ContentBlock::Image { source } => {
                contents.push(Content::Image {
                    image: build_image_content(source)?,
                    detail: None,
                });
            }
        }
    }

    if !message.is_empty() && !has_text_block {
        contents.push(Content::text(message.to_string()));
    }

    if contents.is_empty() {
        contents.push(Content::text(message.to_string()));
    }

    Ok(contents)
}

pub(super) fn content_blocks_include_images(content_blocks: &[ContentBlock]) -> bool {
    content_blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::Image { .. }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_content_blocks_accepts_supported_base64_image_formats() {
        let blocks = vec![ContentBlock::Image {
            source: ImageSource::Base64 {
                media_type: " image/jPg ".to_string(),
                data: "ZmFrZQ==".to_string(),
            },
        }];

        assert!(validate_content_blocks(&blocks).is_ok());
    }

    #[test]
    fn validate_content_blocks_rejects_unsupported_base64_image_formats() {
        let blocks = vec![ContentBlock::Image {
            source: ImageSource::Base64 {
                media_type: "image/heic".to_string(),
                data: "ZmFrZQ==".to_string(),
            },
        }];

        match validate_content_blocks(&blocks) {
            Err(AppError::BadRequest(message)) => {
                assert!(message.contains("image/heic"));
                assert!(message.contains("Convert HEIC/HEIF"));
            }
            Err(_) => panic!("unexpected error variant"),
            Ok(_) => panic!("heic should fail"),
        }
    }

    #[test]
    fn validate_content_blocks_accepts_http_and_https_image_urls() {
        for url in [
            "https://example.com/image.png",
            "http://example.com/image.png",
        ] {
            let blocks = vec![ContentBlock::Image {
                source: ImageSource::Url {
                    url: url.to_string(),
                },
            }];
            assert!(
                validate_content_blocks(&blocks).is_ok(),
                "url should pass: {url}"
            );
        }
    }

    #[test]
    fn validate_content_blocks_rejects_non_http_image_urls() {
        let blocks = vec![ContentBlock::Image {
            source: ImageSource::Url {
                url: "file:///tmp/image.png".to_string(),
            },
        }];

        match validate_content_blocks(&blocks) {
            Err(AppError::BadRequest(message)) => {
                assert!(message.contains("absolute http or https URL"));
            }
            Err(_) => panic!("unexpected error variant"),
            Ok(_) => panic!("file url should fail"),
        }
    }

    #[test]
    fn build_user_content_normalizes_remote_image_urls() {
        let content = match build_user_content(
            "describe this",
            &[ContentBlock::Image {
                source: ImageSource::Url {
                    url: "  https://example.com/image.png?x=1  ".to_string(),
                },
            }],
        ) {
            Ok(content) => content,
            Err(_) => panic!("url image content should build"),
        };

        assert!(matches!(
            content.first(),
            Some(Content::Image {
                image: ImageContent { url: Some(url), .. },
                ..
            }) if url == "https://example.com/image.png?x=1"
        ));
    }
}
