//! Tests for database migrations
//!
//! These tests verify that:
//! - All migrations apply successfully
//! - Schema version is tracked correctly
//! - Migrations can be rolled back (conceptually - we don't actually rollback)
//! - Data survives through migrations

mod core;
mod migrations;

use tempfile::TempDir;

use crate::storage::database::Database;

fn create_test_db() -> (Database, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let db = Database::new(&db_path).expect("Failed to create database");
    (db, temp_dir)
}

/// Handcrafted fixtures that claim schema 23 or newer must include the table
/// created by migration 23. Keeping that historical invariant in the fixture
/// lets later delegated-run migrations stay fail-closed for genuinely corrupt
/// databases instead of silently publishing a current schema with no runtime
/// persistence table.
fn seed_legacy_delegated_runs_schema(conn: &rusqlite::Connection) {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS delegated_runs (
            delegated_run_id TEXT PRIMARY KEY,
            parent_session_id TEXT NOT NULL,
            parent_tool_call_id TEXT,
            role TEXT NOT NULL
                CHECK (role IN ('explore', 'build', 'planner', 'verifier')),
            stage TEXT NOT NULL
                CHECK (stage IN ('created', 'running', 'synthesizing', 'complete', 'degraded', 'failed', 'cancelled')),
            provider TEXT,
            model TEXT,
            resumable INTEGER NOT NULL DEFAULT 0,
            resumed_from_run_id TEXT,
            target_scope_key TEXT NOT NULL,
            target_scope_json TEXT NOT NULL,
            snapshot_json TEXT,
            artifact_json TEXT,
            human_review TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY (parent_session_id) REFERENCES sessions(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_delegated_runs_session_updated
            ON delegated_runs(parent_session_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_delegated_runs_session_scope
            ON delegated_runs(parent_session_id, role, target_scope_key, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_delegated_runs_parent_tool
            ON delegated_runs(parent_tool_call_id);
        "#,
    )
    .expect("seed historical delegated_runs schema");
}
