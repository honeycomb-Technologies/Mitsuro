use rusqlite::{params, Connection};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

use crate::storage::database::{Database, SCHEMA_VERSION};

use super::{create_test_db, seed_legacy_delegated_runs_schema};

#[test]
fn fresh_database_has_current_schema_and_runtime_contracts() {
    let (db, _temp) = create_test_db();
    let conn = db.conn();
    assert_eq!(db.get_schema_version(), SCHEMA_VERSION);
    let foreign_keys: i32 = conn
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .expect("foreign key setting");
    assert_eq!(foreign_keys, 1, "foreign keys should be enabled");
    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode");
    assert_eq!(journal_mode.to_lowercase(), "wal");

    for table in [
        "sessions",
        "messages",
        "runtime_traces",
        "delegated_runs",
        "delegated_run_continuations",
        "hive_runtime_state",
        "hive_attention_state",
        "workflow_goals",
        "workflow_goal_criteria",
        "workflow_plan_revisions",
        "workflow_plan_steps",
        "workflow_step_dependencies",
        "workflow_execution_attempts",
        "workflow_events",
        "workflow_idempotency",
        "pinch_metadata",
        "block_ui_state",
        "file_activity",
        "push_delivery_attempts",
        "delegation_attempts",
        "delegation_capacity_hosts",
        "delegation_capacity_domains",
        "delegation_capacity_waiters",
        "delegation_capacity_leases",
    ] {
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
            .expect("query schema table");
        assert_eq!(exists, 1, "missing table {table}");
    }

    let required_columns: &[(&str, &[&str])] = &[
        (
            "sessions",
            &[
                "id",
                "title",
                "created_at",
                "updated_at",
                "user_id",
                "working_dir",
                "session_type",
                "work_mode",
                "target_branch",
                "context_ledger_json",
                "continuation_json",
                "recovery_json",
                "agent_state",
                "agent_started_at",
                "agent_last_event_at",
                "pinned_at",
                "archived_at",
                "token_count",
                "model",
                "model_key_json",
                "model_catalog_revision",
            ],
        ),
        (
            "runtime_traces",
            &[
                "session_id",
                "run_id",
                "sequence",
                "turn",
                "event_type",
                "payload_json",
                "failure_category",
                "stop_reason",
            ],
        ),
        (
            "delegated_runs",
            &[
                "delegated_run_id",
                "parent_session_id",
                "role",
                "stage",
                "target_scope_json",
                "snapshot_json",
                "artifact_json",
                "delegation_group_id",
                "delegation_task_id",
                "attempt_number",
            ],
        ),
        (
            "hive_runtime_state",
            &[
                "session_id",
                "status",
                "next_wake_at",
                "sleep_reason",
                "last_error",
                "current_run_id",
                "last_wake_reason",
                "crew_slug",
                "priority",
                "updated_at",
                "worker_id",
            ],
        ),
        (
            "hive_attention_state",
            &["user_scope", "item_id", "read", "cleared", "updated_at"],
        ),
        (
            "pinch_metadata",
            &["source_session_id", "target_session_id", "summary"],
        ),
        (
            "file_activity",
            &["read_count", "write_count", "edit_count", "last_accessed"],
        ),
        (
            "hive_schedules",
            &[
                "model",
                "model_key_json",
                "model_catalog_revision",
                "worker_id",
            ],
        ),
        (
            "hive_workers",
            &[
                "id",
                "user_id",
                "slug",
                "display_name",
                "avatar_color",
                "model",
                "model_key_json",
                "model_catalog_revision",
                "permission_mode",
                "autonomy",
                "heartbeat_interval_secs",
                "status",
                "dm_session_id",
                "memory_namespace_id",
            ],
        ),
        ("hive_worker_documents", &["kind"]),
        ("hive_run_attempts", &["executor_id"]),
        ("hive_controllers", &["worker_id"]),
        ("hive_runs", &["worker_id"]),
        (
            "push_subscriptions",
            &[
                "last_success_at",
                "last_failure_at",
                "last_failure_reason",
                "failure_count",
            ],
        ),
        (
            "delegation_tasks",
            &["executor_envelope_version", "executor_envelope_json"],
        ),
        (
            "delegation_groups",
            &[
                "replay_owner_id",
                "replay_lease_expires_at_ms",
                "replay_attempt_count",
            ],
        ),
    ];
    for (table, required) in required_columns {
        let columns = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .expect("prepare table columns")
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query table columns")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect table columns");
        for column in *required {
            assert!(
                columns.iter().any(|actual| actual == column),
                "{table} missing {column}"
            );
        }
    }

    let legacy_attempt_columns: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('hive_run_attempts') WHERE name = 'worker_id'",
            [],
            |row| row.get(0),
        )
        .expect("query legacy attempt column");
    assert_eq!(legacy_attempt_columns, 0, "attempts use executor_id");

    let messages_ddl: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'messages'",
            [],
            |row| row.get(0),
        )
        .expect("messages table DDL");
    assert!(messages_ddl.contains("FOREIGN KEY"));
    assert!(messages_ddl.contains("ON DELETE CASCADE"));
    for index in [
        "idx_workflow_goals_one_unfinished",
        "idx_workflow_one_serial_step",
        "idx_workflow_one_running_attempt",
    ] {
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [index],
                |row| row.get(0),
            )
            .expect("query workflow index");
        assert_eq!(exists, 1, "missing index {index}");
    }
}

#[test]
fn test_migration_idempotency() {
    let (db, _temp) = create_test_db();

    let version1 = db.get_schema_version();
    db.run_migrations().expect("Re-running migrations failed");
    let version2 = db.get_schema_version();

    assert_eq!(version1, version2, "Schema version should not change");
}

#[test]
fn concurrent_process_initialization_serializes_migrations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("concurrent-startup.db");
    let barrier = Arc::new(Barrier::new(4));
    let handles = (0..4)
        .map(|_| {
            let db_path = db_path.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                Database::new(&db_path).map(|db| db.get_schema_version())
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        let version = handle
            .join()
            .expect("database initializer thread should not panic")
            .expect("concurrent database initialization should succeed");
        assert_eq!(version, SCHEMA_VERSION);
    }
}

#[test]
fn privacy_migration_releases_exclusive_lock_while_first_handle_stays_open() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("privacy-lock-release.db");

    let seed = Database::new(&db_path).expect("create current database");
    seed.conn()
        .execute("DELETE FROM schema_version WHERE version >= 44", [])
        .expect("rewind to physical privacy checkpoint");
    drop(seed);

    let first = Database::new(&db_path).expect("complete privacy migration");
    assert_eq!(first.get_schema_version(), 80);

    // Keep the migration-winning handle alive. A locking-mode restore without
    // a subsequent database access retains SQLite's exclusive lock and makes
    // this second independently supervised process time out.
    let second = Database::new(&db_path)
        .expect("second process should open while migration winner remains alive");
    assert_eq!(second.get_schema_version(), 80);
    assert_eq!(first.get_schema_version(), 80);
}

#[test]
fn privacy_migration_never_publishes_completion_while_a_peer_pins_wal() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("privacy-busy-peer.db");

    let seed = Database::new(&db_path).expect("create current database");
    seed.conn()
        .execute_batch(
            "CREATE TABLE privacy_busy_probe (value INTEGER NOT NULL);
             INSERT INTO privacy_busy_probe VALUES (1);
             DELETE FROM schema_version WHERE version >= 44;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .expect("rewind and checkpoint fixture");
    drop(seed);

    let reader = Connection::open(&db_path).expect("open external reader");
    reader.execute_batch("BEGIN").expect("begin read snapshot");
    let version: i32 = reader
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |row| row.get(0),
        )
        .expect("pin schema snapshot");
    assert_eq!(version, 43);

    let writer = Connection::open(&db_path).expect("open fixture writer");
    writer
        .execute("INSERT INTO privacy_busy_probe VALUES (2)", [])
        .expect("append a WAL frame newer than the reader snapshot");
    drop(writer);

    // EXCLUSIVE privacy cleanup may fail while reserving the migration lock or
    // at its mandatory truncate checkpoint. Either is deliberately fail-closed:
    // schema 44 must never claim physical erasure completed while a peer can
    // retain pre-cleanup WAL pages.
    assert!(
        Database::new(&db_path).is_err(),
        "privacy migration must not complete while another process pins WAL"
    );
    let observer = Connection::open(&db_path).expect("open completion observer");
    let durable_version: i32 = observer
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |row| row.get(0),
        )
        .expect("read durable schema version");
    assert_eq!(durable_version, 43);
    drop(observer);

    reader
        .execute_batch("ROLLBACK")
        .expect("release peer snapshot");
    drop(reader);
    let recovered = Database::new(&db_path).expect("retry privacy migration after peer release");
    assert_eq!(recovered.get_schema_version(), 80);
}

#[test]
fn migration_33_removes_legacy_compaction_memory_and_duplicate_history() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let conn = Connection::open(&db_path).expect("open seed db");
    conn.execute_batch(
        r#"CREATE TABLE schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO schema_version (version) VALUES (32);
        CREATE TABLE agent_memories (
            id TEXT PRIMARY KEY,
            memory_type TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            project_dir TEXT,
            user_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO agent_memories (id, memory_type, title, content)
            VALUES ('flush', 'project', 'Compaction flush #1', 'old transcript');
        INSERT INTO agent_memories (id, memory_type, title, content)
            VALUES ('keep', 'project', 'Architecture', 'durable fact');
        CREATE TABLE compaction_checkpoints (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            prompt_index_at_compaction INTEGER NOT NULL,
            pre_compact_message_ids_json TEXT NOT NULL,
            compacted_history_json TEXT NOT NULL,
            original_user_info TEXT,
            reread_file_paths_json TEXT NOT NULL DEFAULT '[]',
            schema_version INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        INSERT INTO compaction_checkpoints (
            id, session_id, prompt_index_at_compaction, pre_compact_message_ids_json,
            compacted_history_json, reread_file_paths_json, created_at
        ) VALUES ('checkpoint', 'session', 1, '[1]', 'old history', '[]', CURRENT_TIMESTAMP);"#,
    )
    .expect("seed schema");
    seed_legacy_delegated_runs_schema(&conn);
    drop(conn);

    let db = Database::new(&db_path).expect("migrate db");
    assert_eq!(db.get_schema_version(), 80);

    let flush_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM agent_memories WHERE title LIKE 'Compaction flush #%';",
            [],
            |row| row.get(0),
        )
        .expect("flush count");
    assert_eq!(flush_count, 0);

    let kept_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM agent_memories WHERE title = 'Architecture';",
            [],
            |row| row.get(0),
        )
        .expect("kept count");
    assert_eq!(kept_count, 1);

    let compacted_history: String = db
        .conn()
        .query_row(
            "SELECT compacted_history_json FROM compaction_checkpoints WHERE id = 'checkpoint';",
            [],
            |row| row.get(0),
        )
        .expect("history json");
    assert_eq!(compacted_history, "[]");
}

#[test]
fn migration_34_backfills_provider_call_classification() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("provider-traces.db");
    let conn = Connection::open(&db_path).expect("open seed db");
    conn.execute_batch(
        r#"CREATE TABLE schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO schema_version (version) VALUES (33);
        CREATE TABLE runtime_traces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            turn INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            failure_category TEXT,
            stop_reason TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(session_id, sequence)
        );
        INSERT INTO runtime_traces (
            session_id, run_id, sequence, turn, event_type, payload_json, created_at
        ) VALUES (
            'session', 'run', 1, 1, 'provider_call',
            '{"call_kind":"auxiliary","operation":"compaction_summary"}',
            CURRENT_TIMESTAMP
        );"#,
    )
    .expect("seed schema");
    seed_legacy_delegated_runs_schema(&conn);
    drop(conn);

    let db = Database::new(&db_path).expect("migrate db");
    assert_eq!(db.get_schema_version(), 80);
    let (call_kind, operation): (Option<String>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT call_kind, operation FROM runtime_traces WHERE sequence = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("classification");
    assert_eq!(call_kind.as_deref(), Some("auxiliary"));
    assert_eq!(operation.as_deref(), Some("compaction_summary"));
}

#[test]
fn migrations_35_through_44_create_mako_backend_contracts() {
    let (db, _temp) = create_test_db();
    let expected_tables = [
        "hive_profiles",
        "hive_profile_documents",
        "hive_controllers",
        "hive_schedules",
        "hive_schedule_occurrences",
        "hive_runs",
        "hive_run_attempts",
        "hive_daemon_leases",
        "hive_idempotency_keys",
        "hive_controller_events",
        "hive_control_outbox",
        "conversation_episodes",
        "hive_learning_runs",
        "hive_learning_candidates",
        "agent_memory_revisions",
        "knowledge_snapshots",
    ];

    for table in expected_tables {
        let exists: i64 = db
            .conn()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                [table],
                |row| row.get(0),
            )
            .unwrap_or_else(|error| panic!("query table {table}: {error}"));
        assert_eq!(exists, 1, "missing migrated table {table}");
    }
}

#[test]
fn migration_39_upgrades_legacy_memories_and_separates_generated_snapshot() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("legacy-memory.db");
    let conn = Connection::open(&db_path).expect("open seed db");
    conn.execute_batch(
        r#"CREATE TABLE schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO schema_version (version) VALUES (38);
        CREATE TABLE agent_memories (
            id TEXT PRIMARY KEY,
            memory_type TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            project_dir TEXT,
            user_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO agent_memories (id, memory_type, title, content, project_dir)
            VALUES ('fact', 'project', 'Architecture', 'Use durable leases', '/repo');
        INSERT INTO agent_memories (id, memory_type, title, content, project_dir)
            VALUES ('snapshot', 'project', 'Current Snapshot', 'generated', '/repo');"#,
    )
    .expect("seed legacy memories");
    seed_legacy_delegated_runs_schema(&conn);
    drop(conn);

    let db = Database::new(&db_path).expect("migrate legacy memories");
    assert_eq!(db.get_schema_version(), 80);

    let fact_metadata: (String, String, f64) = db
        .conn()
        .query_row(
            "SELECT namespace, status, confidence FROM agent_memories WHERE id = 'fact'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("legacy fact metadata");
    assert_eq!(fact_metadata, ("shared".into(), "active".into(), 1.0));

    let snapshot_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM knowledge_snapshots WHERE id = 'snapshot'",
            [],
            |row| row.get(0),
        )
        .expect("migrated snapshot");
    assert_eq!(snapshot_count, 1);

    let legacy_snapshot_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM agent_memories WHERE id = 'snapshot'",
            [],
            |row| row.get(0),
        )
        .expect("legacy snapshot removal");
    assert_eq!(legacy_snapshot_count, 0);

    let revision_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM agent_memory_revisions WHERE memory_id = 'fact'",
            [],
            |row| row.get(0),
        )
        .expect("legacy revision seed");
    assert_eq!(revision_count, 1);
}

#[test]
fn migration_43_redacts_legacy_mako_payloads_and_physically_erases_secrets() {
    const SENTINEL: &str = "MAKO_LEGACY_SECRET_SENTINEL_43";
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("legacy-mako-privacy.db");
    let db = Database::new(&db_path).expect("create current database");
    db.conn()
        .execute_batch(
            "INSERT INTO sessions (
                 id, title, created_at, updated_at, model, session_type
             ) VALUES (
                 'session-privacy', 'Mako', '2026-07-17T00:00:00.000000Z',
                 '2026-07-17T00:00:00.000000Z', 'test:model', 'hive'
             );
             INSERT INTO hive_controllers (
                 id, scope_key, session_id, status, timezone,
                 max_concurrent_runs, created_at, updated_at
             ) VALUES (
                 'controller-privacy', 'session:session-privacy', 'session-privacy',
                 'active', 'UTC', 1, '2026-07-17T00:00:00.000000Z',
                 '2026-07-17T00:00:00.000000Z'
             );",
        )
        .expect("seed ownership rows");
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, available_at, attempt_count, max_attempts,
                 last_stop_reason, last_error, outcome_json, created_at, updated_at
             ) VALUES (
                 'run-privacy', 'controller-privacy', 'session-privacy', 'dispatch',
                 'safe objective', '{}', 'recovery_required',
                 '2026-07-17T00:00:00.000000Z', 1, 3, ?1, ?1, ?2,
                 '2026-07-17T00:00:00.000000Z', '2026-07-17T00:00:00.000000Z'
             )",
            params![SENTINEL, serde_json::json!({"error": SENTINEL}).to_string()],
        )
        .expect("seed raw run copies");
    db.conn()
        .execute(
            "INSERT INTO hive_run_attempts (
                 id, run_id, attempt_no, executor_id, lease_token, lease_epoch,
                 started_at, outcome, stop_reason, error
             ) VALUES (
                 'attempt-privacy', 'run-privacy', 1, 'legacy-worker',
                 'legacy-token', 1, '2026-07-17T00:00:00.000000Z',
                 'recovery_required', ?1, ?1
             )",
            [SENTINEL],
        )
        .expect("seed raw attempt copies");
    for (sequence, payload) in [
        (
            1_i64,
            serde_json::json!({
                "type": "thinking_complete",
                "thinking": SENTINEL,
                "signature": SENTINEL,
            }),
        ),
        (
            2_i64,
            serde_json::json!({
                "type": "tool_approval_required",
                "id": "tool-call-privacy",
                "name": "bash",
                "arguments": {"command": SENTINEL},
            }),
        ),
    ] {
        db.conn()
            .execute(
                "INSERT INTO hive_controller_events (
                     controller_id, sequence, event_type, run_id, payload_json, created_at
                 ) VALUES (
                     'controller-privacy', ?1, 'agentic_event', 'run-privacy', ?2,
                     '2026-07-17T00:00:00.000000Z'
                 )",
                params![sequence, payload.to_string()],
            )
            .expect("seed raw legacy event");
    }
    db.conn()
        .execute(
            "INSERT INTO hive_runtime_state (
                 session_id, status, last_error, current_run_id, updated_at
             ) VALUES (
                 'session-privacy', 'error', ?1, 'run-privacy',
                 '2026-07-17T00:00:00.000000Z'
             )",
            [SENTINEL],
        )
        .expect("seed raw runtime error");
    db.conn()
        .execute(
            "INSERT INTO hive_control_outbox (
                 id, controller_id, session_id, run_id, control_kind, dedupe_key,
                 payload_json, status, available_at, last_error, created_at, updated_at
             ) VALUES (
                 'outbox-privacy', 'controller-privacy', 'session-privacy',
                 'run-privacy', 'tool_approval', 'approval-privacy', ?1,
                 'pending', '2026-07-17T00:00:00.000000Z', ?2,
                 '2026-07-17T00:00:00.000000Z', '2026-07-17T00:00:00.000000Z'
             )",
            params![
                serde_json::json!({
                    "tool_call_id": "tool-call-privacy",
                    "approved": true,
                    "raw": SENTINEL,
                })
                .to_string(),
                SENTINEL,
            ],
        )
        .expect("seed raw outbox copies");
    // Privacy migration 43 still targets legacy mako_* table names. Put the
    // seeded rows back on those names before rewinding schema_version so the
    // redaction path can run, then migration 55 renames them to hive_*.
    db.conn()
        .execute_batch(
            r#"
            ALTER TABLE hive_controllers RENAME TO mako_controllers;
            ALTER TABLE hive_runs RENAME TO mako_runs;
            ALTER TABLE hive_run_attempts RENAME TO mako_run_attempts;
            ALTER TABLE hive_controller_events RENAME TO mako_controller_events;
            ALTER TABLE hive_runtime_state RENAME TO mako_runtime_state;
            ALTER TABLE hive_control_outbox RENAME TO mako_control_outbox;
            "#,
        )
        .expect("restore legacy table names for privacy migration replay");
    db.conn()
        .execute("DELETE FROM schema_version WHERE version >= 43", [])
        .expect("rewind to migration 42");
    drop(db);

    let raw_fixture_contains_secret = [
        db_path.clone(),
        db_path.with_extension("db-wal"),
        db_path.with_extension("db-shm"),
    ]
    .into_iter()
    .filter_map(|path| std::fs::read(path).ok())
    .any(|bytes| {
        bytes
            .windows(SENTINEL.len())
            .any(|window| window == SENTINEL.as_bytes())
    });
    assert!(
        raw_fixture_contains_secret,
        "privacy migration fixture must contain the raw sentinel before cleanup"
    );

    let migrated = Database::new(&db_path).expect("apply privacy migration");
    assert_eq!(migrated.get_schema_version(), 80);
    let event_payloads: String = migrated
        .conn()
        .query_row(
            "SELECT group_concat(payload_json, '|')
               FROM hive_controller_events
              WHERE controller_id = 'controller-privacy'",
            [],
            |row| row.get(0),
        )
        .expect("read migrated events");
    assert!(!event_payloads.contains(SENTINEL));
    let approval_payload: String = migrated
        .conn()
        .query_row(
            "SELECT payload_json FROM hive_controller_events
              WHERE controller_id = 'controller-privacy' AND sequence = 2",
            [],
            |row| row.get(0),
        )
        .expect("read migrated approval");
    let approval_payload: serde_json::Value =
        serde_json::from_str(&approval_payload).expect("valid approval summary");
    assert_eq!(approval_payload["id"], "tool-call-privacy");
    assert_eq!(approval_payload["name"], "bash");
    assert_eq!(approval_payload["arguments_redacted"], true);

    for sql in [
        "SELECT COALESCE(last_error, '') || COALESCE(last_stop_reason, '') || COALESCE(outcome_json, '') FROM hive_runs WHERE id = 'run-privacy'",
        "SELECT COALESCE(error, '') || COALESCE(stop_reason, '') FROM hive_run_attempts WHERE id = 'attempt-privacy'",
        "SELECT COALESCE(last_error, '') FROM hive_runtime_state WHERE session_id = 'session-privacy'",
        "SELECT COALESCE(last_error, '') || payload_json FROM hive_control_outbox WHERE id = 'outbox-privacy'",
    ] {
        let durable_copy: String = migrated
            .conn()
            .query_row(sql, [], |row| row.get(0))
            .expect("read migrated durable copy");
        assert!(!durable_copy.contains(SENTINEL), "secret remains in {sql}");
    }
    drop(migrated);

    for path in [
        db_path.clone(),
        db_path.with_extension("db-wal"),
        db_path.with_extension("db-shm"),
    ] {
        if let Ok(bytes) = std::fs::read(&path) {
            assert!(
                !bytes
                    .windows(SENTINEL.len())
                    .any(|window| window == SENTINEL.as_bytes()),
                "secret remains in raw SQLite file {}",
                path.display()
            );
        }
    }
}

#[test]
fn migration_44_resumes_physical_privacy_cleanup_after_a_crash_checkpoint() {
    const SENTINEL: &str = "MAKO_CRASH_WINDOW_SECRET_SENTINEL_44";
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("mako-privacy-crash-window.db");
    let db = Database::new(&db_path).expect("create current database");

    // Model a process that committed migration 43's logical redaction and
    // then died before VACUUM and the schema-44 completion checkpoint. A
    // dropped scratch table leaves the sentinel only in reclaimable pages.
    db.conn()
        .execute_batch(
            "PRAGMA secure_delete = OFF; CREATE TABLE privacy_crash_probe (payload TEXT);",
        )
        .expect("create crash probe");
    db.conn()
        .execute(
            "INSERT INTO privacy_crash_probe (payload) VALUES (?1)",
            [SENTINEL.repeat(2_048)],
        )
        .expect("write crash probe");
    db.conn()
        .execute_batch(
            "DROP TABLE privacy_crash_probe;
             DELETE FROM schema_version WHERE version >= 44;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .expect("leave logical migration checkpoint at 43");
    drop(db);

    let raw_before = std::fs::read(&db_path).expect("read pre-recovery database");
    assert!(
        raw_before
            .windows(SENTINEL.len())
            .any(|window| window == SENTINEL.as_bytes()),
        "crash fixture must contain the stale sentinel before recovery"
    );

    let recovered = Database::new(&db_path).expect("resume physical privacy cleanup");
    assert_eq!(recovered.get_schema_version(), 80);
    drop(recovered);

    for path in [
        db_path.clone(),
        db_path.with_extension("db-wal"),
        db_path.with_extension("db-shm"),
    ] {
        if let Ok(bytes) = std::fs::read(&path) {
            assert!(
                !bytes
                    .windows(SENTINEL.len())
                    .any(|window| window == SENTINEL.as_bytes()),
                "secret remains after resumed cleanup in {}",
                path.display()
            );
        }
    }
}
