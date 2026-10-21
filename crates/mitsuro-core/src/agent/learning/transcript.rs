use serde::Serialize;

use crate::ai::types::Content;
use crate::storage::StoredMessageRecord;

const MAX_TRANSCRIPT_MESSAGES: usize = 16;
const MAX_TRANSCRIPT_BYTES: usize = 24 * 1024;
const MAX_MESSAGE_BYTES: usize = 6 * 1024;

/// A bounded view of one completed canonical exchange.
///
/// Only persisted `user` and `assistant` text blocks are admitted. Tool calls,
/// tool results, thinking, attachments, pending steering, and ephemeral tick
/// prompts never enter this structure.
#[derive(Debug, Clone, Serialize)]
pub(super) struct LearningTranscript {
    pub through_message_id: i64,
    pub latest_user_message_id: i64,
    pub messages: Vec<LearningTranscriptMessage>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct LearningTranscriptMessage {
    pub message_id: i64,
    pub role: &'static str,
    pub text: String,
}

impl LearningTranscript {
    pub(super) fn from_records(records: &[StoredMessageRecord]) -> Option<Self> {
        let canonical = records
            .iter()
            .filter_map(canonical_text_message)
            .collect::<Vec<_>>();
        let assistant_index = canonical
            .iter()
            .rposition(|message| message.role == "assistant")?;
        let user_index = canonical[..assistant_index]
            .iter()
            .rposition(|message| message.role == "user")?;

        let through_message_id = canonical[assistant_index].message_id;
        let latest_user_message_id = canonical[user_index].message_id;
        let exchange = &canonical[user_index..=assistant_index];
        let mut selected = Vec::new();
        let mut remaining_bytes = MAX_TRANSCRIPT_BYTES;

        // Preserve the exact user evidence first, then fill from the newest
        // assistant context backwards. This keeps the trust anchor even after
        // a long tool-heavy turn with many assistant records.
        let user = bounded_message(&exchange[0], remaining_bytes)?;
        remaining_bytes = remaining_bytes.saturating_sub(user.text.len());
        selected.push(user);

        let mut tail = exchange[1..]
            .iter()
            .rev()
            .take(MAX_TRANSCRIPT_MESSAGES.saturating_sub(1))
            .filter_map(|message| {
                let bounded = bounded_message(message, remaining_bytes)?;
                remaining_bytes = remaining_bytes.saturating_sub(bounded.text.len());
                Some(bounded)
            })
            .collect::<Vec<_>>();
        tail.reverse();
        selected.extend(tail);

        // A transcript without the completed assistant response is not a
        // completed exchange and must not claim a review checkpoint.
        if !selected
            .iter()
            .any(|message| message.message_id == through_message_id)
        {
            return None;
        }

        Some(Self {
            through_message_id,
            latest_user_message_id,
            messages: selected,
        })
    }

    pub(super) fn exact_user_evidence(&self, message_id: i64, excerpt: &str) -> bool {
        let excerpt = normalize_whitespace(excerpt);
        !excerpt.is_empty()
            && self.messages.iter().any(|message| {
                message.role == "user"
                    && message.message_id == message_id
                    && normalize_whitespace(&message.text).contains(&excerpt)
            })
    }

    pub(super) fn has_user_message(&self, message_id: i64) -> bool {
        self.messages
            .iter()
            .any(|message| message.role == "user" && message.message_id == message_id)
    }

    pub(super) fn prompt_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }
}

fn canonical_text_message(record: &StoredMessageRecord) -> Option<LearningTranscriptMessage> {
    let role = match record.role.as_str() {
        "user" => "user",
        "assistant" => "assistant",
        _ => return None,
    };
    let text = canonical_text_content(&record.content_json)?;
    Some(LearningTranscriptMessage {
        message_id: record.id,
        role,
        text,
    })
}

pub(super) fn canonical_text_content(content_json: &str) -> Option<String> {
    let content = serde_json::from_str::<Vec<Content>>(content_json).ok()?;
    let text = content
        .into_iter()
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text),
            Content::Image { .. }
            | Content::Document { .. }
            | Content::ToolUse { .. }
            | Content::ToolResult { .. }
            | Content::Thinking { .. }
            | Content::RedactedThinking { .. } => None,
        })
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if text.is_empty() {
        return None;
    }
    Some(text)
}

fn bounded_message(
    message: &LearningTranscriptMessage,
    remaining_bytes: usize,
) -> Option<LearningTranscriptMessage> {
    let limit = remaining_bytes.min(MAX_MESSAGE_BYTES);
    if limit == 0 {
        return None;
    }
    let text = truncate_utf8(&message.text, limit).trim().to_string();
    if text.is_empty() {
        return None;
    }
    Some(LearningTranscriptMessage {
        message_id: message.message_id,
        role: message.role,
        text,
    })
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

pub(super) fn normalize_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use crate::storage::StoredMessageRecord;

    use super::{LearningTranscript, MAX_MESSAGE_BYTES};

    fn record(id: i64, role: &str, content_json: &str) -> StoredMessageRecord {
        StoredMessageRecord {
            id,
            role: role.to_string(),
            content_json: content_json.to_string(),
            created_at: "2026-07-16T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn transcript_contains_only_canonical_user_and_assistant_text() {
        let records = vec![
            record(
                1,
                "user",
                r#"[{"type":"text","text":"Please keep updates concise."},{"type":"image","image":{"url":"https://example.invalid/private.png"},"detail":null}]"#,
            ),
            record(
                2,
                "assistant",
                r#"[{"type":"thinking","thinking":"private reasoning","signature":"sig"},{"type":"tool_use","id":"call","name":"bash","input":{"command":"secret"}}]"#,
            ),
            record(
                3,
                "tool",
                r#"[{"type":"tool_result","tool_use_id":"call","output":"raw secret"}]"#,
            ),
            record(
                4,
                "assistant",
                r#"[{"type":"text","text":"Understood."},{"type":"thinking","thinking":"hidden","signature":"sig"}]"#,
            ),
            record(
                5,
                "pending_user:steer",
                r#"[{"type":"text","text":"not canonical yet"}]"#,
            ),
        ];

        let transcript = LearningTranscript::from_records(&records).unwrap();
        assert_eq!(transcript.through_message_id, 4);
        assert_eq!(transcript.latest_user_message_id, 1);
        assert_eq!(transcript.messages.len(), 2);
        assert_eq!(transcript.messages[0].text, "Please keep updates concise.");
        assert_eq!(transcript.messages[1].text, "Understood.");
        let encoded = transcript.prompt_json().unwrap();
        assert!(!encoded.contains("private reasoning"));
        assert!(!encoded.contains("raw secret"));
        assert!(!encoded.contains("not canonical yet"));
    }

    #[test]
    fn evidence_requires_the_exact_user_message_and_excerpt() {
        let records = vec![
            record(
                10,
                "user",
                r#"[{"type":"text","text":"Please keep the progress updates concise."}]"#,
            ),
            record(11, "assistant", r#"[{"type":"text","text":"I will."}]"#),
        ];
        let transcript = LearningTranscript::from_records(&records).unwrap();
        assert!(transcript.exact_user_evidence(10, "keep   the progress updates concise."));
        assert!(!transcript.exact_user_evidence(11, "I will."));
        assert!(!transcript.exact_user_evidence(10, "prefer long updates"));
    }

    #[test]
    fn transcript_is_utf8_safe_and_bounded() {
        let long = "🦀".repeat(MAX_MESSAGE_BYTES);
        let records = vec![
            record(
                1,
                "user",
                &serde_json::to_string(&serde_json::json!([{"type":"text","text":long}])).unwrap(),
            ),
            record(2, "assistant", r#"[{"type":"text","text":"done"}]"#),
        ];
        let transcript = LearningTranscript::from_records(&records).unwrap();
        assert!(transcript.messages[0].text.len() <= MAX_MESSAGE_BYTES);
        assert!(transcript.messages[0]
            .text
            .is_char_boundary(transcript.messages[0].text.len()));
        assert_eq!(transcript.through_message_id, 2);
    }
}
