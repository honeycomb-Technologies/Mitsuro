use std::path::Path;

use crate::ai::types::{Content, ModelMessage, Role};
use crate::storage::{Database, SessionManager};

pub(super) fn load_tick_conversation(
    db_path: &Path,
    session_id: &str,
    tick_number: usize,
) -> Result<Vec<ModelMessage>, String> {
    let db = Database::new(db_path)
        .map_err(|error| format!("Failed to open database for tick reload: {error}"))?;
    let session_manager = SessionManager::new(db);
    let raw_messages = session_manager
        .load_session_messages(session_id)
        .map_err(|error| format!("Failed to load session messages for tick reload: {error}"))?;

    let mut conversation = raw_messages
        .into_iter()
        .filter_map(|(role_str, content_json)| {
            let role = match role_str.as_str() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                _ => return None,
            };

            serde_json::from_str(&content_json)
                .ok()
                .map(|content| ModelMessage { role, content })
        })
        .collect::<Vec<_>>();
    conversation.push(build_tick_message(tick_number));
    Ok(conversation)
}

fn build_tick_message(tick_number: usize) -> ModelMessage {
    ModelMessage {
        role: Role::User,
        content: vec![Content::text(format!(
                "<tick>\nAutonomous wake #{tick_number}. Reassess the current task graph, recent progress, and whether to act, communicate, or sleep again.\n</tick>"
            ))],
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{SessionType, WorkspaceMode};

    use super::*;

    #[test]
    fn load_tick_conversation_appends_ephemeral_tick_message() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("mitsuro.db");
        let db = Database::new(&db_path).unwrap();
        let session_manager = SessionManager::new(db);
        let session_id = session_manager
            .create_session_for_user_with_config(
                "Tick Test",
                None,
                None,
                None,
                WorkspaceMode::Neutral,
                None,
                None,
                SessionType::Hive,
            )
            .unwrap();

        session_manager
            .save_message(
                &session_id,
                "user",
                r#"[{"type":"text","text":"Set course for auth cleanup"}]"#,
            )
            .unwrap();
        session_manager
            .save_message(
                &session_id,
                "assistant",
                r#"[{"type":"text","text":"I will coordinate that work."}]"#,
            )
            .unwrap();

        let conversation = load_tick_conversation(&db_path, &session_id, 2).unwrap();

        assert_eq!(conversation.len(), 3);
        assert!(matches!(
            &conversation[2].content[0],
            Content::Text { text, .. }
                if text.contains("<tick>")
                    && text.contains("Autonomous wake #2")
                    && text.contains("sleep again")
        ));
    }
}
