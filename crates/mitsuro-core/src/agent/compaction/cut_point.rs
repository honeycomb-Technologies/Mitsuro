//! Cut-point selection for compaction — preserves recent verbatim tail.

use crate::ai::types::{Content, ModelMessage, Role};

use super::budget::estimate_tokens;

#[derive(Debug, Clone)]
pub(crate) struct IndexedMessage {
    pub id: i64,
    pub message: ModelMessage,
}

#[derive(Debug, Clone)]
pub(crate) struct CutPointResult {
    pub first_kept_index: usize,
    pub first_kept_message_id: i64,
    pub messages_to_summarize: Vec<IndexedMessage>,
    pub kept_messages: Vec<IndexedMessage>,
}

pub(crate) fn find_cut_point(
    messages: &[IndexedMessage],
    compaction_window_start: usize,
    keep_recent_tokens: usize,
) -> Option<CutPointResult> {
    if compaction_window_start >= messages.len() {
        return None;
    }

    let window = &messages[compaction_window_start..];
    if window.is_empty() {
        return None;
    }

    let mut accumulated = 0usize;
    let mut budget_index = window.len();

    for (offset, indexed) in window.iter().enumerate().rev() {
        accumulated =
            accumulated.saturating_add(estimate_tokens(std::slice::from_ref(&indexed.message)));
        if accumulated >= keep_recent_tokens {
            budget_index = offset;
            break;
        }
    }

    if budget_index == window.len() && window.len() > 2 {
        budget_index = window.len() / 2;
    } else if budget_index == 0 && window.len() > 1 {
        budget_index = 1;
    }

    let mut cut_index = budget_index;
    while cut_index < window.len() && !is_valid_cut_point(&window[cut_index].message) {
        cut_index += 1;
    }

    if cut_index >= window.len() {
        if window.len() <= 1 {
            return None;
        }
        cut_index = window.len() - 1;
        while cut_index > 0 && !is_valid_cut_point(&window[cut_index].message) {
            cut_index -= 1;
        }
        if !is_valid_cut_point(&window[cut_index].message) {
            return None;
        }
    }

    let absolute_cut = compaction_window_start + cut_index;
    let first_kept = &messages[absolute_cut];
    let messages_to_summarize = messages[compaction_window_start..absolute_cut].to_vec();
    let kept_messages = messages[absolute_cut..].to_vec();

    if messages_to_summarize.is_empty() {
        return None;
    }

    Some(CutPointResult {
        first_kept_index: absolute_cut,
        first_kept_message_id: first_kept.id,
        messages_to_summarize,
        kept_messages,
    })
}

pub(crate) fn find_last_compaction_index(messages: &[ModelMessage]) -> usize {
    messages
        .iter()
        .rposition(|message| is_compaction_boundary(message) || is_compaction_summary(message))
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn is_valid_cut_point(message: &ModelMessage) -> bool {
    match message.role {
        Role::Assistant => true,
        Role::User => message
            .content
            .iter()
            .any(|content| matches!(content, Content::Text { .. })),
        _ => false,
    }
}

pub(crate) fn is_compaction_boundary(message: &ModelMessage) -> bool {
    message.role == Role::User
        && message.content.iter().any(|content| {
            matches!(content, Content::Text { text, .. } if text.starts_with(super::COMPACTION_BOUNDARY_PREFIX))
        })
}

pub(crate) fn is_compaction_summary(message: &ModelMessage) -> bool {
    // User-role summaries remain readable for checkpoints created before the
    // generated context was correctly attributed to the assistant.
    matches!(message.role, Role::User | Role::Assistant)
        && message.content.iter().any(|content| {
            matches!(content, Content::Text { text, .. } if text.starts_with(super::COMPACTION_SUMMARY_PREFIX))
        })
}

pub(crate) fn find_aggressive_cut_point(
    messages: &[IndexedMessage],
    compaction_window_start: usize,
) -> Option<CutPointResult> {
    if compaction_window_start >= messages.len() {
        return None;
    }

    let window = &messages[compaction_window_start..];
    if window.len() <= 1 {
        return None;
    }

    let mut cut_index = window.len() - 1;
    while cut_index > 0 && !is_valid_cut_point(&window[cut_index].message) {
        cut_index -= 1;
    }
    if !is_valid_cut_point(&window[cut_index].message) {
        return None;
    }

    let absolute_cut = compaction_window_start + cut_index;
    let first_kept = &messages[absolute_cut];
    let messages_to_summarize = messages[compaction_window_start..absolute_cut].to_vec();
    if messages_to_summarize.is_empty() {
        return None;
    }

    Some(CutPointResult {
        first_kept_index: absolute_cut,
        first_kept_message_id: first_kept.id,
        messages_to_summarize,
        kept_messages: messages[absolute_cut..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::types::Content;

    fn indexed(id: i64, role: Role, text: &str) -> IndexedMessage {
        IndexedMessage {
            id,
            message: ModelMessage {
                role,
                content: vec![Content::text(text.to_string())],
            },
        }
    }

    #[test]
    fn find_cut_point_preserves_recent_tail() {
        let messages = vec![
            indexed(1, Role::User, "start"),
            indexed(2, Role::Assistant, "work"),
            indexed(3, Role::User, "more"),
            indexed(4, Role::Assistant, "recent"),
        ];

        let cut = find_cut_point(&messages, 0, 1).expect("cut point");
        assert_eq!(cut.first_kept_message_id, 4);
        assert_eq!(cut.messages_to_summarize.len(), 3);
        assert_eq!(cut.kept_messages.len(), 1);
    }

    #[test]
    fn find_cut_point_skips_tool_only_user_messages() {
        let messages = vec![
            indexed(1, Role::User, "question"),
            indexed(2, Role::Assistant, "calling tool"),
            IndexedMessage {
                id: 3,
                message: ModelMessage {
                    role: Role::User,
                    content: vec![Content::ToolResult {
                        tool_use_id: "t1".to_string(),
                        output: serde_json::json!({"summary": "ok"}),
                        is_error: None,
                    }],
                },
            },
            indexed(4, Role::Assistant, "done"),
        ];

        let cut = find_cut_point(&messages, 0, 1);
        assert!(cut.is_some());
    }
}
