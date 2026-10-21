use mitsuro_core::ai::types::{Content, ModelMessage, Role};

/// Parse persisted user/assistant messages back into model-facing history.
///
/// Stored histories can also contain other roles like `system` or `tool`; those
/// are intentionally ignored here because chat/pinch only replay user and
/// assistant turns into the model conversation.
pub fn parse_stored_model_messages(
    session_id: &str,
    raw_messages: Vec<(String, String)>,
    context: &'static str,
) -> Vec<ModelMessage> {
    raw_messages
        .into_iter()
        .filter_map(|(role, content_json)| {
            let role = match role.as_str() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                _ => return None,
            };
            match serde_json::from_str::<Vec<Content>>(&content_json) {
                Ok(content) => Some(ModelMessage { role, content }),
                Err(error) => {
                    tracing::warn!(
                        session_id = %session_id,
                        context,
                        role = ?role,
                        error = %error,
                        "Failed to parse stored model message content"
                    );
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use mitsuro_core::ai::types::Content;

    use super::parse_stored_model_messages;

    #[test]
    fn parse_stored_model_messages_keeps_user_and_assistant_roles() {
        let parsed = parse_stored_model_messages(
            "session-1",
            vec![
                (
                    "user".to_string(),
                    serde_json::json!([{ "type": "text", "text": "hello" }]).to_string(),
                ),
                (
                    "assistant".to_string(),
                    serde_json::json!([{ "type": "text", "text": "world" }]).to_string(),
                ),
                (
                    "system".to_string(),
                    serde_json::json!([{ "type": "text", "text": "ignored" }]).to_string(),
                ),
            ],
            "test context",
        );

        assert_eq!(parsed.len(), 2);
        assert!(matches!(
            parsed[0].content.first(),
            Some(Content::Text { text, .. }) if text == "hello"
        ));
        assert!(matches!(
            parsed[1].content.first(),
            Some(Content::Text { text, .. }) if text == "world"
        ));
    }

    #[test]
    fn parse_stored_model_messages_skips_malformed_content() {
        let parsed = parse_stored_model_messages(
            "session-1",
            vec![
                (
                    "user".to_string(),
                    serde_json::json!([{ "type": "text", "text": "keep me" }]).to_string(),
                ),
                ("assistant".to_string(), "{not json}".to_string()),
            ],
            "test context",
        );

        assert_eq!(parsed.len(), 1);
        assert!(matches!(
            parsed[0].content.first(),
            Some(Content::Text { text, .. }) if text == "keep me"
        ));
    }
}
