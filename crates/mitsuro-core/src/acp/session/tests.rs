use std::path::PathBuf;
use std::sync::Arc;

use agent_client_protocol::SessionId;
use tempfile::tempdir;
use tokio::sync::Mutex;

use super::SessionManager;
use crate::ai::types::Role;
use crate::storage::{
    Database, PartialAssistantState, RecoveryDecision, RecoveryStatus,
    SessionManager as StorageSessionManager, SessionRecoveryState,
};

#[test]
fn test_session_creation() {
    let manager = SessionManager::new();
    let session = manager.create_session(Some(PathBuf::from("/tmp")), None);

    assert_eq!(session.cwd, PathBuf::from("/tmp"));
    assert!(!session.is_cancelled());
    assert_eq!(manager.session_count(), 1);
}

#[test]
fn test_session_cancellation() {
    let manager = SessionManager::new();
    let session = manager.create_session(None, None);

    assert!(!session.is_cancelled());
    session.cancel();
    assert!(session.is_cancelled());
}

#[test]
fn test_session_lookup() {
    let manager = SessionManager::new();
    let session = manager.create_session(None, None);
    let id = session.id.clone();

    assert!(manager.has_session(&id));
    assert!(manager.get_session(&id).is_ok());

    let fake_id = SessionId::from("nonexistent".to_string());
    assert!(!manager.has_session(&fake_id));
    assert!(manager.get_session(&fake_id).is_err());
}

#[tokio::test]
async fn test_session_with_storage() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::new(&db_path).unwrap();
    let storage = Arc::new(Mutex::new(StorageSessionManager::new(db)));

    let manager = SessionManager::with_storage(storage);
    let session = manager.create_session(Some(PathBuf::from("/test")), None);

    let storage_id = session.init_storage_session("Test Session").await;
    assert!(storage_id.is_some());

    session.add_user_message("Hello, world!".to_string()).await;

    let stored_id = session.get_storage_session_id().await;
    assert_eq!(stored_id, storage_id);
}

#[tokio::test]
async fn test_session_load_from_storage() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::new(&db_path).unwrap();
    let storage = Arc::new(Mutex::new(StorageSessionManager::new(db)));

    let manager = SessionManager::with_storage(Arc::clone(&storage));
    let session1 = manager.create_session(Some(PathBuf::from("/test")), None);
    let storage_id = session1.init_storage_session("Test Session").await.unwrap();
    session1.add_user_message("First message".to_string()).await;
    session1.add_assistant_message("Response".to_string()).await;
    {
        let storage = storage.lock().await;
        storage
            .update_recovery_state(
                &storage_id,
                &SessionRecoveryState::new(
                    RecoveryStatus::Interrupted,
                    None,
                    Some("stream stopped".to_string()),
                    PartialAssistantState {
                        citations: Vec::new(),
                        text: "Partial answer".to_string(),
                        thinking: String::new(),
                        tool_calls: Vec::new(),
                    },
                    RecoveryDecision::Resumable {
                        latest_user_objective: "Finish the answer".to_string(),
                    },
                ),
            )
            .unwrap();
    }

    let session2 = manager
        .create_session_from_storage(&storage_id, Some(PathBuf::from("/test")), None)
        .await
        .unwrap();

    assert_eq!(session2.id.to_string(), storage_id);

    let messages = session2.get_messages().await;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].role, Role::User);
    assert_eq!(messages[1].role, Role::Assistant);

    let recovery_notice = session2
        .take_recovery_notice()
        .await
        .expect("expected recovery notice");
    assert_eq!(recovery_notice.role, Role::System);
    assert!(recovery_notice.content.iter().any(|content| matches!(
        content,
        crate::ai::types::Content::Text { text, .. }
            if text.contains("Finish the answer")
    )));
}
