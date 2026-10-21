use std::path::Path;
use tokio::sync::mpsc;

use crate::ai::derive_title;
use crate::ai::types::{Content, ModelMessage, Role};

use super::super::loop_events::LoopEvent;
use super::persistence::save_title;

pub(super) fn maybe_generate_title(
    conversation: &[ModelMessage],
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    session_id: &str,
    db_path: &Path,
) {
    let first_user_msg = conversation
        .iter()
        .find(|m| m.role == Role::User)
        .and_then(|m| {
            m.content.iter().find_map(|c| {
                if let Content::Text { text, .. } = c {
                    Some(text.clone())
                } else {
                    None
                }
            })
        })
        .unwrap_or_default();

    if first_user_msg.is_empty() {
        return;
    }

    let title = derive_title(&first_user_msg);
    if !title.is_empty() {
        save_title(db_path, session_id, &title);
        let _ = event_tx.send(LoopEvent::TitleGenerated { title });
    }
}
