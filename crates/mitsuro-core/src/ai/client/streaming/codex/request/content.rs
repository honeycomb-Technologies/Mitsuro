use serde_json::Value;

use crate::ai::types::{Content, ImageContent};

pub(super) fn collect_message_text(content: &[Content], separator: &str) -> String {
    let mut combined = String::new();
    for block in content {
        if let Content::Text { text, citations } = block {
            if !combined.is_empty() {
                combined.push_str(separator);
            }
            combined.push_str(&crate::ai::format::provider_text(text, citations));
        }
    }
    combined
}

fn codex_image_url(image: &ImageContent) -> Option<String> {
    if let Some(url) = &image.url {
        return Some(url.clone());
    }

    image.base64.as_ref().map(|base64| {
        let media_type = image.media_type.as_deref().unwrap_or("image/png");
        format!("data:{};base64,{}", media_type, base64)
    })
}

pub(super) fn build_codex_user_content(content: &[Content]) -> Vec<Value> {
    let mut items: Vec<Value> = Vec::new();

    for block in content {
        match block {
            Content::Text { text, citations } if !text.is_empty() || !citations.is_empty() => {
                items.push(serde_json::json!({
                    "type": "input_text",
                    "text": crate::ai::format::provider_text(text, citations)
                }));
            }
            Content::Image { image, detail } => {
                if let Some(image_url) = codex_image_url(image) {
                    let mut item = serde_json::json!({
                        "type": "input_image",
                        "image_url": image_url
                    });
                    if let Some(detail) = detail.as_deref().filter(|d| !d.is_empty()) {
                        item["detail"] = serde_json::json!(detail);
                    }
                    items.push(item);
                }
            }
            Content::Thinking { thinking, .. } if !thinking.is_empty() => {
                items.push(serde_json::json!({
                    "type": "input_text",
                    "text": format!("[Thinking]\n{}\n[/Thinking]", thinking)
                }));
            }
            _ => {}
        }
    }

    items
}
