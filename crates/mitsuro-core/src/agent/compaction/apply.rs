//! Apply compaction results to the active session message list.

use crate::ai::types::{Content, ModelMessage, Role};

use super::cut_point::IndexedMessage;
use super::summarize::{
    boundary_user_text, format_boundary_message, format_summary_message, CompactionSummaryInput,
};

pub(crate) fn build_compacted_conversation(
    summary_input: &CompactionSummaryInput,
    trigger: &str,
    tokens_before: usize,
    tokens_after: usize,
    first_kept_message_id: i64,
    kept_messages: &[IndexedMessage],
) -> Vec<ModelMessage> {
    let boundary_payload = format_boundary_message(
        trigger,
        tokens_before,
        tokens_after,
        first_kept_message_id,
        &summary_input.checkpoint_id,
        summary_input.compaction_count,
    );

    let mut compacted = Vec::new();
    compacted.push(ModelMessage {
        role: Role::User,
        content: vec![Content::text(boundary_user_text(&boundary_payload))],
    });
    compacted.push(ModelMessage {
        // A generated summary is Agent-authored context, never a new user instruction.
        role: Role::Assistant,
        content: vec![Content::text(format_summary_message(summary_input))],
    });

    for indexed in kept_messages {
        compacted.push(indexed.message.clone());
    }

    compacted
}
