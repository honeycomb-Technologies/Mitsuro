//! Wake parent chat/code sessions when a background process completes.
//!
//! Detached shell jobs previously only updated registry status. Without a
//! completion signal the agent re-entered with poll loops and tripped the
//! no-progress guard. This module queues durable steering and, when a run is
//! active, delivers `LoopInput::Steer` so the model continues with the result.

use std::sync::Arc;

use mitsuro_core::agent::LoopInput;
use mitsuro_core::ai::types::Content;
use mitsuro_core::process::{ProcessCompletionEvent, ProcessRegistry, ProcessStatus};
use mitsuro_core::storage::Database;
use mitsuro_core::SessionManager;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::routes::chat::deliver_steering_with_rollover;
use crate::AppState;

/// Wire the shared process registry to session wake handling for this server.
pub async fn install_process_completion_wake(
    process_registry: Arc<ProcessRegistry>,
    state: AppState,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<ProcessCompletionEvent>();
    process_registry.set_completion_sender(tx).await;

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(error) = handle_process_completion(&state, event).await {
                tracing::warn!(%error, "Failed to deliver process completion wake");
            }
        }
    });
}

async fn handle_process_completion(
    state: &AppState,
    event: ProcessCompletionEvent,
) -> anyhow::Result<()> {
    let Some(session_id) = event.session_id.as_deref() else {
        tracing::debug!(
            process_id = %event.process_id,
            "Background process completed without bound session; no wake"
        );
        return Ok(());
    };

    let content = completion_steer_content(&event);
    let pending_id = format!("proc-wake-{}", Uuid::new_v4());
    let content_json = serde_json::to_string(&content)?;

    SessionManager::new(Database::new(&state.db_path)?).queue_pending_steering(
        session_id,
        &pending_id,
        &content_json,
    )?;

    let sender = state.session_inputs.read().await.get(session_id).cloned();
    if let Some(sender) = sender {
        let input = LoopInput::Steer {
            pending_id: Some(pending_id.clone()),
            content,
        };
        let delivered = deliver_steering_with_rollover(state, session_id, sender, input).await;
        tracing::info!(
            session_id = %session_id,
            process_id = %event.process_id,
            pending_id = %pending_id,
            delivered,
            "Queued process completion wake for active session"
        );
    } else {
        tracing::info!(
            session_id = %session_id,
            process_id = %event.process_id,
            pending_id = %pending_id,
            "Queued process completion wake for idle session (promoted on next start)"
        );
    }

    Ok(())
}

fn completion_steer_content(event: &ProcessCompletionEvent) -> Vec<Content> {
    let status_line = match &event.status {
        ProcessStatus::Completed {
            exit_code,
            duration_ms,
        } => format!("completed exit_code={exit_code} duration_ms={duration_ms}"),
        ProcessStatus::Failed { error, duration_ms } => {
            format!("failed duration_ms={duration_ms}: {error}")
        }
        ProcessStatus::Killed { duration_ms } => format!("killed duration_ms={duration_ms}"),
        ProcessStatus::Running => "running".to_string(),
        ProcessStatus::Suspended => "suspended".to_string(),
    };

    let mut body = format!(
        "[BACKGROUND PROCESS COMPLETE]\nprocess_id: {}\nstatus: {}\ncommand: {}\n",
        event.process_id, status_line, event.command
    );
    if let Some(description) = &event.description {
        body.push_str(&format!("description: {description}\n"));
    }
    if let Some(preview) = &event.output_preview {
        body.push_str("output_preview:\n");
        body.push_str(preview);
        body.push('\n');
    }
    body.push_str(
        "\nContinue from this result. Do not re-poll process status for this process_id unless you need more output.\n",
    );

    vec![Content::text(body)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn completion_message_includes_process_id_and_exit() {
        let event = ProcessCompletionEvent {
            user_id: "u".into(),
            process_id: "p1".into(),
            session_id: Some("s1".into()),
            command: "sleep 1".into(),
            description: Some("wait".into()),
            status: ProcessStatus::Completed {
                exit_code: 0,
                duration_ms: 1000,
            },
            output_preview: Some("done".into()),
        };
        let content = completion_steer_content(&event);
        let text = match &content[0] {
            Content::Text { text, .. } => text.as_str(),
            _ => panic!("expected text"),
        };
        assert!(text.contains("process_id: p1"));
        assert!(text.contains("exit_code=0"));
        assert!(text.contains("done"));
        assert!(text.contains("BACKGROUND PROCESS COMPLETE"));
    }

    #[tokio::test]
    async fn completion_sender_receives_terminal_events() {
        let registry = Arc::new(ProcessRegistry::new());
        let (tx, mut rx) = mpsc::unbounded_channel();
        registry.set_completion_sender(tx).await;

        let temp = tempfile::tempdir().expect("tempdir");
        let id = registry
            .spawn_for_user(
                "user",
                "true".to_string(),
                temp.path().to_path_buf(),
                Some("quick".into()),
                Some("sess-1".into()),
            )
            .await
            .expect("spawn");

        let event = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(event) = rx.recv().await {
                    if event.process_id == id {
                        return event;
                    }
                } else {
                    panic!("channel closed");
                }
            }
        })
        .await
        .expect("timeout waiting for completion");

        assert_eq!(event.session_id.as_deref(), Some("sess-1"));
        assert!(matches!(
            event.status,
            ProcessStatus::Completed { exit_code: 0, .. }
        ));
    }
}
