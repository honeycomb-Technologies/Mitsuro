use std::path::Path;

use tracing::warn;

use crate::storage::{AutonomousTaskStore, DelegatedRunStore, TaskStatus};

use super::{open_context_database, truncate_utf8};

const MAX_ACTIVE_TASKS: usize = 12;
const MAX_TASK_SUBJECT_CHARS: usize = 200;
const MAX_DELEGATED_SCOPES: usize = 6;
const MAX_DELEGATED_SCOPE_CHARS: usize = 120;
const MAX_DELEGATED_REVIEW_CHARS: usize = 240;

pub(super) fn build_delegated_context(db_path: &Path, session_id: &str) -> String {
    let Some(db) = open_context_database(db_path, "building delegated context") else {
        return String::new();
    };
    let store = DelegatedRunStore::new(db);
    let recent = match store.list_runs_for_session(session_id, 3) {
        Ok(runs) => runs,
        Err(error) => {
            warn!(session_id = %session_id, error = %error, "Failed to load delegated runs for context");
            return String::new();
        }
    };
    if recent.is_empty() {
        return String::new();
    }

    let mut lines = vec![
        "[RECENT DELEGATED RUNS]".to_string(),
        String::new(),
        "Recent delegated investigations exist for this session.".to_string(),
        "- Reuse relevant completed evidence. The `agent` tool's status action reads terminal evidence; do not poll running children.".to_string(),
        "- The `agent` tool's message action steers live work; followup/resume starts new work. Follow the session delegation mode, including ExplicitOnly's user-request requirement.".to_string(),
        "- Continue useful independent work directly. Prior delegation does not require delegating again or replacing fresh inspection with another child.".to_string(),
        String::new(),
        "Most recent delegated runs:".to_string(),
    ];

    for run in recent {
        let scopes = if run.target_scope.is_empty() {
            "(no scope)".to_string()
        } else {
            run.target_scope
                .iter()
                .take(MAX_DELEGATED_SCOPES)
                .map(|scope| truncate_utf8(&scope.label, MAX_DELEGATED_SCOPE_CHARS))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let role = match run.role {
            crate::storage::DelegatedRunRole::Explore => "explore",
            crate::storage::DelegatedRunRole::Build => "build",
            crate::storage::DelegatedRunRole::Planner => "planner",
            crate::storage::DelegatedRunRole::Verifier => "verifier",
        };
        let review = run
            .human_review
            .as_deref()
            .unwrap_or("No finalized review was recorded.")
            .lines()
            .next()
            .unwrap_or("No finalized review was recorded.");
        lines.push(format!(
            "- {} run {} on [{}]: stage={:?}, resumable={}, semantic_review=\"{}\"",
            role,
            run.delegated_run_id,
            scopes,
            run.stage,
            run.resumable,
            truncate_utf8(review, MAX_DELEGATED_REVIEW_CHARS)
        ));
    }

    lines.join("\n")
}

/// Build context for autonomous tasks in this session.
pub(super) fn build_autonomous_task_context(db_path: &Path, session_id: &str) -> String {
    let Some(db) = open_context_database(db_path, "building autonomous task context") else {
        return String::new();
    };
    let store = AutonomousTaskStore::new(db);
    let tasks = match store.list_tasks(session_id) {
        Ok(t) => t,
        Err(error) => {
            warn!(session_id = %session_id, error = %error, "Failed to load autonomous tasks for context");
            return String::new();
        }
    };
    if tasks.is_empty() {
        return String::new();
    }

    let mut lines = vec!["[AUTONOMOUS TASKS]".to_string()];

    let in_progress: Vec<_> = tasks
        .iter()
        .filter(|t| t.status == TaskStatus::InProgress)
        .take(MAX_ACTIVE_TASKS)
        .collect();
    let pending_limit = MAX_ACTIVE_TASKS.saturating_sub(in_progress.len());
    let pending: Vec<_> = tasks
        .iter()
        .filter(|t| t.status == TaskStatus::Pending)
        .take(pending_limit)
        .collect();

    if in_progress.is_empty() && pending.is_empty() {
        return String::new();
    }

    if !in_progress.is_empty() {
        lines.push("In Progress:".to_string());
        for t in &in_progress {
            let owner = t
                .owner
                .as_deref()
                .map(|o| format!(" (owner: {})", truncate_utf8(o, 80)))
                .unwrap_or_default();
            lines.push(format!(
                "  - {}: {}{}",
                t.id,
                truncate_utf8(&t.subject, MAX_TASK_SUBJECT_CHARS),
                owner
            ));
        }
    }
    if !pending.is_empty() {
        lines.push("Pending:".to_string());
        for t in &pending {
            lines.push(format!(
                "  - {}: {}",
                t.id,
                truncate_utf8(&t.subject, MAX_TASK_SUBJECT_CHARS)
            ));
        }
    }

    let active_count = tasks
        .iter()
        .filter(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::InProgress))
        .count();
    if active_count > MAX_ACTIVE_TASKS {
        lines.push(format!(
            "  ... {} additional active tasks omitted",
            active_count - MAX_ACTIVE_TASKS
        ));
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{build_autonomous_task_context, MAX_ACTIVE_TASKS};
    use crate::storage::{AutonomousTaskStore, Database, SessionManager};

    #[test]
    fn autonomous_context_excludes_completed_tasks_and_caps_active_tasks() {
        let temp = TempDir::new().expect("temp dir");
        let db_path = temp.path().join("tasks.db");
        let manager = SessionManager::new(Database::new(&db_path).expect("db"));
        let session_id = manager
            .create_session("tasks", None, None)
            .expect("session");
        let store = AutonomousTaskStore::new(Database::new(&db_path).expect("db"));
        let completed = store
            .create_task(&session_id, "finished sentinel", "", &[])
            .expect("completed task");
        store.complete_task(&completed, "done").expect("complete");
        for index in 0..(MAX_ACTIVE_TASKS + 4) {
            store
                .create_task(&session_id, &format!("active task {index}"), "", &[])
                .expect("active task");
        }

        let context = build_autonomous_task_context(&db_path, &session_id);

        assert!(!context.contains("finished sentinel"));
        assert!(context.contains("additional active tasks omitted"));
        assert_eq!(context.matches(": active task").count(), MAX_ACTIVE_TASKS);
    }
}
