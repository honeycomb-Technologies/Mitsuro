use super::{create_test_db, seed_legacy_delegated_runs_schema};

#[test]
fn test_pinch_metadata_table_migration() {
    let (db, _temp) = create_test_db();

    let conn = db.conn();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='pinch_metadata'")
        .expect("Failed to prepare query");

    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("Failed to query tables")
        .filter_map(Result::ok)
        .collect();

    assert!(tables.contains(&"pinch_metadata".to_string()));

    let mut stmt = conn
        .prepare("PRAGMA table_info(pinch_metadata)")
        .expect("Failed to prepare PRAGMA");

    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("Failed to get columns")
        .filter_map(Result::ok)
        .collect();

    assert!(columns.contains(&"source_session_id".to_string()));
    assert!(columns.contains(&"target_session_id".to_string()));
    assert!(columns.contains(&"summary".to_string()));
}

#[test]
fn test_block_ui_state_table_migration() {
    let (db, _temp) = create_test_db();

    let conn = db.conn();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='block_ui_state'")
        .expect("Failed to prepare query");

    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("Failed to query tables")
        .filter_map(Result::ok)
        .collect();

    assert!(tables.contains(&"block_ui_state".to_string()));
}

#[test]
fn test_file_activity_table_migration() {
    let (db, _temp) = create_test_db();

    let conn = db.conn();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='file_activity'")
        .expect("Failed to prepare query");

    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("Failed to query tables")
        .filter_map(Result::ok)
        .collect();

    assert!(tables.contains(&"file_activity".to_string()));

    let mut stmt = conn
        .prepare("PRAGMA table_info(file_activity)")
        .expect("Failed to prepare PRAGMA");

    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("Failed to get columns")
        .filter_map(Result::ok)
        .collect();

    assert!(columns.contains(&"read_count".to_string()));
    assert!(columns.contains(&"write_count".to_string()));
    assert!(columns.contains(&"edit_count".to_string()));
    assert!(columns.contains(&"last_accessed".to_string()));
}

#[test]
fn test_agent_state_columns_migration() {
    let (db, _temp) = create_test_db();

    let conn = db.conn();
    let mut stmt = conn
        .prepare("PRAGMA table_info(sessions)")
        .expect("Failed to prepare PRAGMA");

    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("Failed to get columns")
        .filter_map(Result::ok)
        .collect();

    assert!(columns.contains(&"agent_state".to_string()));
    assert!(columns.contains(&"agent_started_at".to_string()));
    assert!(columns.contains(&"agent_last_event_at".to_string()));
}

#[test]
fn test_session_list_metadata_migration() {
    let (db, _temp) = create_test_db();
    let columns: Vec<String> = db
        .conn()
        .prepare("PRAGMA table_info(sessions)")
        .expect("prepare sessions columns")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query sessions columns")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect sessions columns");

    assert!(columns.contains(&"pinned_at".to_string()));
    assert!(columns.contains(&"archived_at".to_string()));
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn test_token_count_column_migration() {
    let (db, _temp) = create_test_db();

    let conn = db.conn();
    let mut stmt = conn
        .prepare("PRAGMA table_info(sessions)")
        .expect("Failed to prepare PRAGMA");

    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("Failed to get columns")
        .filter_map(Result::ok)
        .collect();

    assert!(columns.contains(&"token_count".to_string()));
}

#[test]
fn provider_aware_model_identity_columns_are_additive() {
    let (db, _temp) = create_test_db();
    let columns: Vec<String> = db
        .conn()
        .prepare("PRAGMA table_info(sessions)")
        .expect("prepare session columns")
        .query_map([], |row| row.get(1))
        .expect("query session columns")
        .collect::<rusqlite::Result<_>>()
        .expect("collect session columns");

    assert!(columns.contains(&"model".to_string()));
    assert!(columns.contains(&"model_key_json".to_string()));
    assert!(columns.contains(&"model_catalog_revision".to_string()));

    let schedule_columns: Vec<String> = db
        .conn()
        .prepare("PRAGMA table_info(hive_schedules)")
        .expect("prepare Hive schedule columns")
        .query_map([], |row| row.get(1))
        .expect("query Mako schedule columns")
        .collect::<rusqlite::Result<_>>()
        .expect("collect Mako schedule columns");
    assert!(schedule_columns.contains(&"model".to_string()));
    assert!(schedule_columns.contains(&"model_key_json".to_string()));
    assert!(schedule_columns.contains(&"model_catalog_revision".to_string()));
}

#[test]
fn migration_45_upgrades_schema_44_without_rewriting_legacy_model() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-44.db");
    let conn = rusqlite::Connection::open(&path).expect("seed database");
    conn.execute_batch(
        "CREATE TABLE schema_version (
             version INTEGER PRIMARY KEY,
             applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
         );
         INSERT INTO schema_version (version) VALUES (44);
         CREATE TABLE sessions (
             id TEXT PRIMARY KEY,
             model TEXT
         );
         INSERT INTO sessions (id, model) VALUES ('legacy', 'shared-model');",
    )
    .expect("seed schema 44");
    seed_legacy_delegated_runs_schema(&conn);
    drop(conn);

    let db = crate::storage::database::Database::new(&path).expect("migrate schema 44");
    assert_eq!(db.get_schema_version(), 80);
    let row: (Option<String>, Option<String>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT model, model_key_json, model_catalog_revision FROM sessions WHERE id = 'legacy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read migrated session");
    assert_eq!(row.0.as_deref(), Some("shared-model"));
    assert!(row.1.is_none());
    assert!(row.2.is_none());
}

#[test]
fn migration_46_upgrades_schema_45_without_guessing_legacy_schedule_identity() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-45.db");
    let conn = rusqlite::Connection::open(&path).expect("seed database");
    conn.execute_batch(
        "CREATE TABLE schema_version (
             version INTEGER PRIMARY KEY,
             applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
         );
         INSERT INTO schema_version (version) VALUES (45);
         CREATE TABLE mako_schedules (
             id TEXT PRIMARY KEY,
             model TEXT
         );
         INSERT INTO mako_schedules (id, model) VALUES ('legacy', 'shared-model');",
    )
    .expect("seed schema 45");
    seed_legacy_delegated_runs_schema(&conn);
    drop(conn);

    let db = crate::storage::database::Database::new(&path).expect("migrate schema 45");
    assert_eq!(db.get_schema_version(), 80);
    let row: (Option<String>, Option<String>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT model, model_key_json, model_catalog_revision
               FROM hive_schedules WHERE id = 'legacy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read migrated schedule");
    assert_eq!(row.0.as_deref(), Some("shared-model"));
    assert!(row.1.is_none());
    assert!(row.2.is_none());
}

#[test]
fn migration_52_backfills_one_deterministic_claim_and_is_idempotent() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-51-continuations.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO sessions (id, title, created_at, updated_at)
            VALUES ('parent', 'Parent', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z');

            DROP TABLE delegated_run_continuations;
            DELETE FROM schema_version WHERE version >= 52;

            INSERT INTO delegated_runs (
                delegated_run_id, parent_session_id, role, stage, resumable,
                resumed_from_run_id, target_scope_key, target_scope_json,
                created_at, updated_at
            ) VALUES (
                'origin', 'parent', 'explore', 'complete', 1,
                NULL, 'project', '[]',
                '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z'
            );
            INSERT INTO delegated_runs (
                delegated_run_id, parent_session_id, role, stage, resumable,
                resumed_from_run_id, target_scope_key, target_scope_json,
                created_at, updated_at
            ) VALUES (
                'continuation-later', 'parent', 'explore', 'complete', 1,
                'origin', 'project', '[]',
                '2026-08-01T00:02:00Z', '2026-08-01T00:02:00Z'
            );
            INSERT INTO delegated_runs (
                delegated_run_id, parent_session_id, role, stage, resumable,
                resumed_from_run_id, target_scope_key, target_scope_json,
                created_at, updated_at
            ) VALUES (
                'continuation-first', 'parent', 'explore', 'complete', 1,
                'origin', 'project', '[]',
                '2026-08-01T00:01:00Z', '2026-08-01T00:01:00Z'
            );
            "#,
        )
        .expect("seed schema-51 duplicate continuation history");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 52");
    assert_eq!(db.get_schema_version(), 80);
    let claimed: String = db
        .conn()
        .query_row(
            "SELECT delegated_run_id FROM delegated_run_continuations WHERE resumed_from_run_id = 'origin'",
            [],
            |row| row.get(0),
        )
        .expect("read migrated continuation claim");
    assert_eq!(claimed, "continuation-first");

    let duplicate = db.conn().execute(
        "INSERT INTO delegated_run_continuations (resumed_from_run_id, delegated_run_id, created_at) VALUES ('origin', 'continuation-later', '2026-08-01T00:03:00Z')",
        [],
    );
    assert!(duplicate.is_err(), "one origin must have only one claim");

    db.conn()
        .execute("DELETE FROM schema_version WHERE version >= 52", [])
        .expect("rewind migration marker");
    db.run_migrations().expect("reapply migration 52");
    let claim_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM delegated_run_continuations WHERE resumed_from_run_id = 'origin'",
            [],
            |row| row.get(0),
        )
        .expect("count idempotent claims");
    assert_eq!(claim_count, 1);
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_53_adds_durable_background_wake_intent_idempotently() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-52-wake-intent.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO sessions (id, title, created_at, updated_at)
            VALUES ('parent-wake', 'Parent', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z');
            INSERT INTO delegated_runs (
                delegated_run_id, parent_session_id, role, stage, resumable,
                target_scope_key, target_scope_json, created_at, updated_at
            ) VALUES (
                'legacy-foreground', 'parent-wake', 'explore', 'complete', 1,
                'project', '[]', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z'
            );
            DROP INDEX idx_delegated_runs_expired_host_lease;
            ALTER TABLE delegated_runs DROP COLUMN host_lease_expires_at_ms;
            ALTER TABLE delegated_runs DROP COLUMN host_owner_id;
            DROP INDEX idx_delegated_runs_unqueued_wake;
            ALTER TABLE delegated_runs DROP COLUMN wake_parent;
            DELETE FROM schema_version WHERE version >= 53;
            "#,
        )
        .expect("rewind durable wake intent migration");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 53");
    assert_eq!(db.get_schema_version(), 80);
    let wake_parent: i64 = db
        .conn()
        .query_row(
            "SELECT wake_parent FROM delegated_runs WHERE delegated_run_id = 'legacy-foreground'",
            [],
            |row| row.get(0),
        )
        .expect("read migrated wake intent");
    assert_eq!(
        wake_parent, 0,
        "legacy runs must not invent background intent"
    );
    let index_exists: i64 = db
        .conn()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_delegated_runs_unqueued_wake')",
            [],
            |row| row.get(0),
        )
        .expect("read wake index");
    assert_eq!(index_exists, 1);
    db.run_migrations().expect("migration 53 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_54_adds_conservative_background_host_leases_idempotently() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-53-host-lease.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO sessions (id, title, created_at, updated_at)
            VALUES ('lease-parent', 'Parent', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z');
            INSERT INTO delegated_runs (
                delegated_run_id, parent_session_id, role, stage, resumable,
                target_scope_key, target_scope_json, wake_parent, created_at, updated_at
            ) VALUES (
                'legacy-background', 'lease-parent', 'explore', 'running', 1,
                'project', '[]', 1, '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z'
            );
            DROP INDEX idx_delegated_runs_expired_host_lease;
            ALTER TABLE delegated_runs DROP COLUMN host_lease_expires_at_ms;
            ALTER TABLE delegated_runs DROP COLUMN host_owner_id;
            DELETE FROM schema_version WHERE version >= 54;
            "#,
        )
        .expect("rewind background host lease migration");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 54");
    assert_eq!(db.get_schema_version(), 80);
    let (owner, expiry): (Option<String>, Option<i64>) = db
        .conn()
        .query_row(
            "SELECT host_owner_id, host_lease_expires_at_ms FROM delegated_runs WHERE delegated_run_id = 'legacy-background'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read migrated host lease");
    assert!(owner.is_none());
    assert!(
        expiry.is_none(),
        "migration must not steal mixed-version work"
    );
    let index_exists: i64 = db
        .conn()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_delegated_runs_expired_host_lease')",
            [],
            |row| row.get(0),
        )
        .expect("read host lease index");
    assert_eq!(index_exists, 1);
    db.run_migrations().expect("migration 54 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_63_backfills_workers_from_crew_and_companion_and_renames_executor() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-62-workers.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");

    // Seed a schema-62-shaped world: crew profiles for two owners, the
    // durable Hive companion session per owner, a controller on the local
    // companion, and crew_slug assignments on runtime state and schedules.
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO users (id, email) VALUES ('alice', 'alice@example.com');

            INSERT INTO sessions (id, title, created_at, updated_at, session_type, user_id)
            VALUES
                ('companion-local', 'Hive', '2026-08-01T00:00:00.000000Z',
                 '2026-08-01T00:00:00.000000Z', 'hive', NULL),
                ('companion-alice', 'Hive', '2026-08-01T00:00:00.000000Z',
                 '2026-08-01T00:00:01.000000Z', 'hive', 'alice'),
                ('work-1', 'Crew job', '2026-08-01T00:00:00.000000Z',
                 '2026-08-01T00:00:00.000000Z', 'hive', NULL);

            INSERT INTO hive_profiles (id, user_id, revision)
            VALUES ('local', NULL, 0), ('user:alice-hash', 'alice', 0);
            INSERT INTO hive_crew_profiles (profile_id, slug, revision)
            VALUES ('local', 'builder', 0), ('local', 'researcher', 0),
                   ('user:alice-hash', 'builder', 0);
            INSERT INTO hive_crew_documents (profile_id, slug, kind, content, updated_at)
            VALUES
                ('local', 'builder', 'identity', 'Local builder identity',
                 '2026-08-01T00:00:00.000000Z'),
                ('local', 'builder', 'soul', 'Local builder soul',
                 '2026-08-01T00:00:00.000000Z'),
                ('user:alice-hash', 'builder', 'identity', 'Alice builder identity',
                 '2026-08-01T00:00:00.000000Z');

            INSERT INTO hive_controllers (
                id, scope_key, user_id, session_id, status, timezone,
                max_concurrent_runs, created_at, updated_at
            ) VALUES (
                'controller-local', 'scope-local', NULL, 'companion-local',
                'active', 'UTC', 1, '2026-08-01T00:00:00.000000Z',
                '2026-08-01T00:00:00.000000Z'
            );

            INSERT INTO hive_runtime_state (session_id, status, crew_slug, priority, updated_at)
            VALUES ('work-1', 'idle', 'builder', 'normal', '2026-08-01T00:00:00.000000Z');

            INSERT INTO hive_schedules (
                id, controller_id, title, summary, objective, recurrence_kind,
                recurrence_json, timezone, gap_policy, fold_policy, status,
                crew_slug, misfire_policy, misfire_grace_secs, catch_up_limit,
                overlap_policy, max_attempts, retry_base_secs, retry_max_secs,
                retry_jitter, created_by, created_at, updated_at
            ) VALUES (
                'schedule-1', 'controller-local', 'Sweep', 'Sweep', 'Do the sweep',
                'interval', '{}', 'UTC', 'shift_forward', 'first', 'enabled',
                'researcher', 'skip', 0, 0,
                'skip', 1, 0, 0,
                'none', 'test', '2026-08-01T00:00:00.000000Z', '2026-08-01T00:00:00.000000Z'
            );

            INSERT INTO hive_runs (
                id, controller_id, session_id, kind, objective, config_json,
                status, available_at, max_attempts, created_at, updated_at
            ) VALUES (
                'run-1', 'controller-local', 'companion-local', 'dispatch', 'obj',
                '{}', 'succeeded', '2026-08-01T00:00:00.000000Z', 3,
                '2026-08-01T00:00:00.000000Z', '2026-08-01T00:00:00.000000Z'
            );
            INSERT INTO hive_run_attempts (
                id, run_id, attempt_no, executor_id, lease_token, lease_epoch,
                started_at, outcome
            ) VALUES (
                'attempt-1', 'run-1', 1, 'daemon-a', 'token-1', 1,
                '2026-08-01T00:00:00.000000Z', 'succeeded'
            );
            "#,
        )
        .expect("seed schema-62 fixture data");

    // Remove post-63 schema objects in dependency order. In particular,
    // SQLite validates surviving trigger SQL during ALTER TABLE, so current
    // Worker governor/conversation guards cannot remain while worker linkage
    // columns are temporarily absent.
    rewind_migration_78_for_test(&db);
    rewind_migration_77_for_test(&db);
    rewind_migrations_75_and_76_for_test(&db);
    rewind_migration_74_for_test(&db);
    release_migration_72_report_guards_for_test(&db);
    drop_post_68_worker_conversation_tables_for_test(&db);

    // Rewind the schema 63 surface so the migration re-applies on top of the
    // seeded rows: drop the worker tables/columns and restore the legacy
    // attempt claimant column name.
    db.conn()
        .execute_batch(
            r#"
            DROP TABLE hive_worker_documents;
            DROP INDEX idx_hive_controllers_worker;
            ALTER TABLE hive_controllers DROP COLUMN worker_id;
            DROP INDEX idx_hive_runs_worker;
            ALTER TABLE hive_runs DROP COLUMN worker_id;
            DROP INDEX idx_hive_runtime_state_worker;
            ALTER TABLE hive_runtime_state DROP COLUMN worker_id;
            DROP INDEX idx_hive_schedules_worker;
            ALTER TABLE hive_schedules DROP COLUMN worker_id;
            ALTER TABLE hive_run_attempts RENAME COLUMN executor_id TO worker_id;
            DROP TABLE hive_workers;
            DELETE FROM schema_version WHERE version >= 63;
            "#,
        )
        .expect("rewind worker identity migration");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 63");
    assert_eq!(db.get_schema_version(), 80);

    let count_workers = |predicate: &str| -> i64 {
        db.conn()
            .query_row(
                &format!("SELECT COUNT(*) FROM hive_workers WHERE {predicate}"),
                [],
                |row| row.get(0),
            )
            .expect("count workers")
    };
    assert_eq!(count_workers("1 = 1"), 5, "3 crew + 2 companions");
    assert_eq!(
        count_workers(
            "slug = 'builder' AND user_id IS NULL AND memory_namespace_id = 'builder' \
         AND display_name = 'Builder' AND status = 'active' AND autonomy = 'manual'"
        ),
        1
    );
    assert_eq!(count_workers("slug = 'researcher' AND user_id IS NULL"), 1);
    assert_eq!(count_workers("slug = 'builder' AND user_id = 'alice'"), 1);
    assert_eq!(
        count_workers(
            "slug = 'assistant' AND user_id IS NULL AND dm_session_id = 'companion-local'"
        ),
        1
    );
    assert_eq!(
        count_workers(
            "slug = 'assistant' AND user_id = 'alice' AND dm_session_id = 'companion-alice'"
        ),
        1
    );

    // Crew persona documents were copied onto the backfilled workers.
    let local_builder_documents: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_worker_documents d
             JOIN hive_workers w ON w.id = d.worker_id
             WHERE w.slug = 'builder' AND w.user_id IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("count local builder documents");
    assert_eq!(local_builder_documents, 2);
    let alice_builder_identity: String = db
        .conn()
        .query_row(
            "SELECT d.content FROM hive_worker_documents d
             JOIN hive_workers w ON w.id = d.worker_id
             WHERE w.slug = 'builder' AND w.user_id = 'alice' AND d.kind = 'identity'",
            [],
            |row| row.get(0),
        )
        .expect("read alice builder identity");
    assert_eq!(alice_builder_identity, "Alice builder identity");

    // The companion controller became the assistant worker's execution lane.
    let controller_link_matches: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_controllers c
             JOIN hive_workers w ON w.id = c.worker_id
             WHERE c.id = 'controller-local' AND w.slug = 'assistant'
               AND w.user_id IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("read controller worker link");
    assert_eq!(controller_link_matches, 1);

    // crew_slug assignments resolved to worker ids while keeping the slug.
    let runtime_link_matches: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_runtime_state r
             JOIN hive_workers w ON w.id = r.worker_id
             WHERE r.session_id = 'work-1' AND r.crew_slug = 'builder'
               AND w.slug = 'builder' AND w.user_id IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("read runtime state worker link");
    assert_eq!(runtime_link_matches, 1);
    let schedule_link_matches: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_schedules s
             JOIN hive_workers w ON w.id = s.worker_id
             WHERE s.id = 'schedule-1' AND s.crew_slug = 'researcher'
               AND w.slug = 'researcher' AND w.user_id IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("read schedule worker link");
    assert_eq!(schedule_link_matches, 1);

    // The rename preserved attempt data under the new executor column.
    let executor: String = db
        .conn()
        .query_row(
            "SELECT executor_id FROM hive_run_attempts WHERE id = 'attempt-1'",
            [],
            |row| row.get(0),
        )
        .expect("read renamed executor column");
    assert_eq!(executor, "daemon-a");
    let attempt_columns: Vec<String> = db
        .conn()
        .prepare("PRAGMA table_info(hive_run_attempts)")
        .expect("prepare attempt columns")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query attempt columns")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect attempt columns");
    assert!(!attempt_columns.contains(&"worker_id".to_string()));

    // Re-running the migration is a no-op: no duplicate workers, links keep
    // their targets, and the rename guard skips cleanly.
    let assistant_id: String = db
        .conn()
        .query_row(
            "SELECT id FROM hive_workers WHERE slug = 'assistant' AND user_id IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("read assistant id");
    db.conn()
        .execute("DELETE FROM schema_version WHERE version >= 63", [])
        .expect("rewind migration marker");
    db.run_migrations().expect("reapply migration 63");
    assert_eq!(db.get_schema_version(), 80);
    assert_eq!(count_workers("1 = 1"), 5, "idempotent backfill");
    let assistant_id_after: String = db
        .conn()
        .query_row(
            "SELECT id FROM hive_workers WHERE slug = 'assistant' AND user_id IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("read assistant id after replay");
    assert_eq!(assistant_id, assistant_id_after);
}

#[test]
fn migration_63_fresh_database_uses_executor_id_and_worker_linkage_columns() {
    let (db, _temp) = create_test_db();

    let table_columns = |table: &str| -> Vec<String> {
        db.conn()
            .prepare(&format!("PRAGMA table_info({table})"))
            .expect("prepare table columns")
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query table columns")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect table columns")
    };

    let worker_columns = table_columns("hive_workers");
    for column in [
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
    ] {
        assert!(
            worker_columns.contains(&column.to_string()),
            "hive_workers missing {column}"
        );
    }
    assert!(table_columns("hive_worker_documents").contains(&"kind".to_string()));

    let attempt_columns = table_columns("hive_run_attempts");
    assert!(attempt_columns.contains(&"executor_id".to_string()));
    assert!(!attempt_columns.contains(&"worker_id".to_string()));

    for table in [
        "hive_controllers",
        "hive_runs",
        "hive_runtime_state",
        "hive_schedules",
    ] {
        assert!(
            table_columns(table).contains(&"worker_id".to_string()),
            "{table} missing worker_id"
        );
    }
}

#[test]
fn migration_64_upgrades_schema_63_with_group_rooms_and_run_linkage() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-63-groups.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    rewind_migration_78_for_test(&db);
    rewind_migration_77_for_test(&db);
    rewind_migrations_75_and_76_for_test(&db);
    rewind_migration_74_for_test(&db);
    release_migration_72_report_guards_for_test(&db);
    drop_post_68_worker_conversation_tables_for_test(&db);
    // Rewind to a schema-63-shaped world: no group tables and no group
    // linkage on hive_runs. The partial index must go before its column.
    db.conn()
        .execute_batch(
            r#"
            DROP TABLE hive_member_cursors;
            DROP TABLE hive_group_turns;
            DROP TABLE hive_group_messages;
            DROP TABLE hive_group_members;
            DROP TABLE hive_groups;
            DROP INDEX idx_hive_runs_group_turn;
            ALTER TABLE hive_runs DROP COLUMN group_id;
            ALTER TABLE hive_runs DROP COLUMN group_turn_id;
            ALTER TABLE hive_runs DROP COLUMN trigger_message_id;
            DELETE FROM schema_version WHERE version >= 64;
            "#,
        )
        .expect("rewind group room migration");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 64");
    assert_eq!(db.get_schema_version(), 80);

    let table_exists = |table: &str| -> bool {
        db.conn()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                [table],
                |row| row.get(0),
            )
            .expect("read table existence")
    };
    for table in [
        "hive_groups",
        "hive_group_members",
        "hive_group_messages",
        "hive_group_turns",
        "hive_member_cursors",
    ] {
        assert!(table_exists(table), "missing table {table}");
    }
    let run_columns: Vec<String> = db
        .conn()
        .prepare("PRAGMA table_info(hive_runs)")
        .expect("prepare run columns")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query run columns")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect run columns");
    for column in ["group_id", "group_turn_id", "trigger_message_id"] {
        assert!(
            run_columns.contains(&column.to_string()),
            "hive_runs missing {column}"
        );
    }

    // The per-group sequence is unique and the mode CHECK holds.
    db.conn()
        .execute_batch(
            "INSERT INTO hive_groups (id, user_id, title, created_at, updated_at)
             VALUES ('group-1', NULL, 'Room', '2026-08-16T00:00:00Z', '2026-08-16T00:00:00Z');
             INSERT INTO hive_group_messages (
                 id, group_id, seq, sender_kind, content, created_at
             ) VALUES ('message-1', 'group-1', 1, 'user', 'hi', '2026-08-16T00:00:00Z');",
        )
        .expect("seed a group and message");
    let duplicate_seq = db.conn().execute(
        "INSERT INTO hive_group_messages (id, group_id, seq, sender_kind, content, created_at)
         VALUES ('message-2', 'group-1', 1, 'user', 'again', '2026-08-16T00:00:01Z')",
        [],
    );
    assert!(duplicate_seq.is_err(), "seq must be unique per group");
    let invalid_mode = db.conn().execute(
        "UPDATE hive_groups SET execution_mode = 'swarm' WHERE id = 'group-1'",
        [],
    );
    assert!(invalid_mode.is_err(), "execution mode CHECK must hold");

    db.run_migrations().expect("migration 64 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_65_upgrades_schema_64_with_delivery_ledger() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-64-deliveries.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    db.conn()
        .execute_batch(
            r#"
            DROP TABLE hive_deliveries;
            DELETE FROM schema_version WHERE version >= 65;
            "#,
        )
        .expect("rewind delivery ledger migration");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 65");
    assert_eq!(db.get_schema_version(), 80);

    let exists: bool = db
        .conn()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hive_deliveries')",
            [],
            |row| row.get(0),
        )
        .expect("read table existence");
    assert!(exists, "missing hive_deliveries");

    let runs_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
            [],
            |row| row.get(0),
        )
        .expect("read hive_runs sql");
    assert!(
        runs_sql.contains("worker_message") || !runs_sql.contains("kind IN"),
        "hive_runs kind CHECK must accept worker_message"
    );

    db.run_migrations().expect("migration 65 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_66_upgrades_schema_65_with_memory_acl_scopes() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-65-memory-acl.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO agent_memories (
                id, memory_type, title, content, namespace, namespace_id,
                status, source, confidence, sensitivity, pinned, access_count
            ) VALUES (
                'crew-1', 'project', 'Private', 'crew-private-marker',
                'crew', 'researcher', 'active', 'agent', 1.0, 'normal', 0, 0
            );
            INSERT INTO agent_memories (
                id, memory_type, title, content, namespace,
                status, source, confidence, sensitivity, pinned, access_count
            ) VALUES (
                'shared-1', 'project', 'Shared', 'shared-memory-marker',
                'shared', 'active', 'agent', 1.0, 'normal', 0, 0
            );
            DROP INDEX IF EXISTS idx_agent_memories_acl;
            ALTER TABLE agent_memories DROP COLUMN acl_scope;
            ALTER TABLE agent_memories DROP COLUMN conversation_id;
            DELETE FROM schema_version WHERE version >= 66;
            "#,
        )
        .expect("rewind memory ACL migration");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 66");
    assert_eq!(db.get_schema_version(), 80);

    let columns: Vec<String> = db
        .conn()
        .prepare("PRAGMA table_info(agent_memories)")
        .expect("prepare memory columns")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query memory columns")
        .filter_map(Result::ok)
        .collect();
    assert!(columns.contains(&"acl_scope".to_string()));
    assert!(columns.contains(&"conversation_id".to_string()));

    let crew_scope: String = db
        .conn()
        .query_row(
            "SELECT acl_scope FROM agent_memories WHERE id = 'crew-1'",
            [],
            |row| row.get(0),
        )
        .expect("read crew ACL");
    assert_eq!(crew_scope, "worker");
    let shared_scope: String = db
        .conn()
        .query_row(
            "SELECT acl_scope FROM agent_memories WHERE id = 'shared-1'",
            [],
            |row| row.get(0),
        )
        .expect("read shared ACL");
    assert_eq!(shared_scope, "owner");

    db.run_migrations().expect("migration 66 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_67_upgrades_schema_66_with_worker_heartbeat_kind() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-66-heartbeat.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    const DELIVERY_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message')";
    const HEARTBEAT_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat')";
    db.conn()
        .pragma_update(None, "writable_schema", "ON")
        .expect("enable writable_schema");
    db.conn()
        .execute(
            "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
             WHERE type = 'table' AND name = 'hive_runs'
               AND instr(sql, ?1) > 0",
            [HEARTBEAT_KINDS, DELIVERY_KINDS],
        )
        .expect("rewind hive_runs kind CHECK");
    db.conn()
        .pragma_update(None, "writable_schema", "RESET")
        .expect("reload schema after CHECK rewind");
    db.conn()
        .execute("DELETE FROM schema_version WHERE version >= 67", [])
        .expect("rewind heartbeat kind migration");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 67");
    assert_eq!(db.get_schema_version(), 80);

    let runs_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
            [],
            |row| row.get(0),
        )
        .expect("read hive_runs sql");
    assert!(
        runs_sql.contains("worker_heartbeat") || !runs_sql.contains("kind IN"),
        "hive_runs kind CHECK must accept worker_heartbeat"
    );

    db.run_migrations().expect("migration 67 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_69_adds_isolated_lanes_idempotent_messages_and_introduction_ledger() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-68-worker-introductions.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    let now = "2026-08-24T00:00:00Z";
    db.conn()
        .execute_batch(&format!(
            r#"
            INSERT INTO sessions (
                id, title, created_at, updated_at, session_type
            ) VALUES
                ('migration-69-run', 'Run lane', '{now}', '{now}', 'hive'),
                ('migration-69-other', 'Other lane', '{now}', '{now}', 'hive');
            INSERT INTO hive_controllers (
                id, scope_key, session_id, status, timezone,
                max_concurrent_runs, created_at, updated_at
            ) VALUES (
                'migration-69-controller', 'local:migration-69',
                'migration-69-run', 'active', 'UTC', 1, '{now}', '{now}'
            );
            INSERT INTO hive_runs (
                id, controller_id, session_id, kind, objective, config_json,
                status, available_at, max_attempts, created_at, updated_at
            ) VALUES (
                'migration-69-existing-run', 'migration-69-controller',
                'migration-69-run', 'dispatch', 'preserve this row', '{{}}',
                'queued', '{now}', 3, '{now}', '{now}'
            );
            CREATE INDEX idx_hive_runs_migration69_canary
                ON hive_runs(objective);
            INSERT INTO messages (
                session_id, role, content, created_at
            ) VALUES (
                'migration-69-run', 'assistant', '[]', '{now}'
            );
            "#
        ))
        .expect("seed schema-68 migration canaries");

    let run_columns_before = db
        .conn()
        .prepare("PRAGMA table_info(hive_runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let run_indexes_before = db
        .conn()
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'index' AND tbl_name = 'hive_runs' AND sql IS NOT NULL
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();

    // Preserve the complete current run schema/index baseline above, then
    // release every downstream object that depends on migration-69 tables or
    // message columns before destructively shaping the fixture as schema 68.
    // Reapplying migrations 69-78 must restore those same indexes.
    drop_migrations_75_and_76_run_dependents_for_test(&db);
    release_migration_72_report_guards_for_test(&db);
    db.conn()
        .execute_batch(
            r#"
            DROP TABLE hive_worker_introductions;
            DROP TABLE hive_group_worker_lanes;
            DROP INDEX idx_messages_session_idempotency;
            ALTER TABLE messages DROP COLUMN idempotency_key;
            "#,
        )
        .expect("rewind migration-69 tables and message idempotency");

    const HEARTBEAT_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat')";
    const INTRODUCTION_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction')";
    db.conn()
        .pragma_update(None, "writable_schema", "ON")
        .expect("enable writable_schema");
    db.conn()
        .execute(
            "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
             WHERE type = 'table' AND name = 'hive_runs' AND instr(sql, ?1) > 0",
            [INTRODUCTION_KINDS, HEARTBEAT_KINDS],
        )
        .expect("rewind introduction run kind");
    db.conn()
        .pragma_update(None, "writable_schema", "RESET")
        .expect("reload rewound schema");
    db.conn()
        .execute("DELETE FROM schema_version WHERE version >= 69", [])
        .expect("rewind migration marker");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 69");
    assert_eq!(db.get_schema_version(), 80);

    let run_columns_after = db
        .conn()
        .prepare("PRAGMA table_info(hive_runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let run_indexes_after = db
        .conn()
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'index' AND tbl_name = 'hive_runs' AND sql IS NOT NULL
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(run_columns_after, run_columns_before);
    assert_eq!(run_indexes_after, run_indexes_before);
    let preserved: (String, i64) = db
        .conn()
        .query_row(
            "SELECT objective, max_attempts FROM hive_runs
             WHERE id = 'migration-69-existing-run'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("load preserved run");
    assert_eq!(preserved, ("preserve this row".into(), 3));
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, available_at, max_attempts, created_at, updated_at
             ) VALUES (
                 'migration-69-introduction-run', 'migration-69-controller',
                 'migration-69-run', 'worker_introduction', 'meet the Worker', '{}',
                 'queued', ?1, 1, ?1, ?1
             )",
            [now],
        )
        .expect("worker_introduction must satisfy the run kind CHECK");

    let message_columns = db
        .conn()
        .prepare("PRAGMA table_info(messages)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(message_columns.contains(&"idempotency_key".to_string()));
    db.conn()
        .execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES ('migration-69-run', 'assistant', '[]', ?1, 'opening:v1')",
            [now],
        )
        .unwrap();
    assert!(db
        .conn()
        .execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES ('migration-69-run', 'assistant', '[]', ?1, 'opening:v1')",
            [now],
        )
        .is_err());
    db.conn()
        .execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES ('migration-69-other', 'assistant', '[]', ?1, 'opening:v1')",
            [now],
        )
        .expect("message keys are session scoped");
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at) VALUES
                 ('migration-69-run', 'assistant', '[]', ?1),
                 ('migration-69-run', 'assistant', '[]', ?1)",
            [now],
        )
        .expect("NULL idempotency keys remain repeatable");

    for table in ["hive_group_worker_lanes", "hive_worker_introductions"] {
        let exists: bool = db
            .conn()
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
                 )",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists, "missing {table}");
    }
    let migration_indexes = db
        .conn()
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type = 'index' AND name IN (
                 'idx_messages_session_idempotency',
                 'idx_hive_group_worker_lanes_worker',
                 'idx_hive_worker_introductions_status',
                 'idx_hive_worker_introductions_opening_message'
             )
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(migration_indexes.len(), 4);
    let message_index_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_messages_session_idempotency'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(message_index_sql.contains("WHERE idempotency_key IS NOT NULL"));
    let lane_foreign_keys = db
        .conn()
        .prepare(
            "SELECT \"table\", \"from\", \"to\", on_delete
             FROM pragma_foreign_key_list('hive_group_worker_lanes')",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(lane_foreign_keys.contains(&(
        "hive_groups".into(),
        "group_id".into(),
        "id".into(),
        "RESTRICT".into()
    )));
    assert!(lane_foreign_keys.contains(&(
        "hive_workers".into(),
        "worker_id".into(),
        "id".into(),
        "RESTRICT".into()
    )));
    assert!(lane_foreign_keys.contains(&(
        "sessions".into(),
        "session_id".into(),
        "id".into(),
        "CASCADE".into()
    )));

    db.conn()
        .execute(
            "INSERT INTO hive_workers (
                 id, slug, display_name, memory_namespace_id, created_at, updated_at
             ) VALUES (
                 'migration-69-worker', 'migration-69-worker', 'Migration Worker',
                 'migration-69-worker', ?1, ?1
             )",
            [now],
        )
        .unwrap();
    let opening_message_id: i64 = db
        .conn()
        .query_row(
            "SELECT id FROM messages
             WHERE session_id = 'migration-69-run' AND idempotency_key = 'opening:v1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version, opening_message_id,
                 proposal_json, created_at, updated_at
             ) VALUES (
                 'migration-69-worker', 'migration-69-introduction-run',
                 'awaiting_context', 1, ?1, '{}', ?2, ?2
             )",
            rusqlite::params![opening_message_id, now],
        )
        .expect("insert introduction ledger row");
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_worker_introductions SET status = 'invented'
             WHERE worker_id = 'migration-69-worker'",
            [],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_worker_introductions SET proposal_json = 'not-json'
             WHERE worker_id = 'migration-69-worker'",
            [],
        )
        .is_err());
    db.conn()
        .execute("DELETE FROM messages WHERE id = ?1", [opening_message_id])
        .unwrap();
    let opening_after_delete: Option<i64> = db
        .conn()
        .query_row(
            "SELECT opening_message_id FROM hive_worker_introductions
             WHERE worker_id = 'migration-69-worker'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(opening_after_delete.is_none());
    db.conn()
        .execute(
            "DELETE FROM hive_workers WHERE id = 'migration-69-worker'",
            [],
        )
        .unwrap();
    let introduction_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_worker_introductions",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(introduction_count, 0);

    db.run_migrations().expect("migration 69 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_70_backfills_provable_worker_learning_and_rejects_unresolved_pending_rows() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-69-learning-scopes.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    let now = "2026-08-24T00:00:00Z";
    db.conn()
        .execute_batch(&format!(
            r#"
            INSERT INTO sessions (id, title, created_at, updated_at, session_type)
            VALUES
                ('legacy-worker-dm', 'Worker DM', '{now}', '{now}', 'hive'),
                ('legacy-primary', 'Primary Hive', '{now}', '{now}', 'hive');
            INSERT INTO messages (session_id, role, content, created_at)
            VALUES
                ('legacy-worker-dm', 'user', '[{{"type":"text","text":"worker evidence"}}]', '{now}'),
                ('legacy-primary', 'user', '[{{"type":"text","text":"primary evidence"}}]', '{now}');
            INSERT INTO hive_workers (
                id, slug, display_name, dm_session_id, memory_namespace_id,
                created_at, updated_at
            ) VALUES (
                'legacy-worker', 'legacy-worker', 'Legacy Worker',
                'legacy-worker-dm', 'stable-worker-namespace', '{now}', '{now}'
            );
            INSERT INTO hive_learning_candidates (
                id, canonical_key, kind, proposed_content,
                evidence_session_id, evidence_message_id, evidence_excerpt,
                explicit, confidence, sensitivity, status, reason, created_at
            )
            SELECT
                'legacy-worker-candidate', 'preference.worker', 'user_preference',
                'worker preference', 'legacy-worker-dm', id, 'worker evidence',
                1, 0.99, 'normal', 'pending', 'legacy pending', '{now}'
            FROM messages WHERE session_id = 'legacy-worker-dm';
            INSERT INTO hive_learning_candidates (
                id, canonical_key, kind, proposed_content,
                evidence_session_id, evidence_message_id, evidence_excerpt,
                explicit, confidence, sensitivity, status, reason, created_at
            )
            SELECT
                'legacy-unresolved-candidate', 'preference.primary', 'user_preference',
                'primary preference', 'legacy-primary', id, 'primary evidence',
                1, 0.99, 'normal', 'pending', 'legacy pending', '{now}'
            FROM messages WHERE session_id = 'legacy-primary';
            INSERT INTO hive_learning_candidates (
                id, user_id, canonical_key, kind, proposed_content,
                evidence_session_id, evidence_message_id, evidence_excerpt,
                explicit, confidence, sensitivity, status, reason, created_at
            )
            SELECT
                'legacy-owner-mismatch', 'alice', 'preference.mismatched',
                'user_preference', 'mismatched preference',
                'legacy-worker-dm', id, 'worker evidence',
                1, 0.99, 'normal', 'pending', 'legacy pending', '{now}'
            FROM messages WHERE session_id = 'legacy-worker-dm';
            DELETE FROM schema_version WHERE version >= 70;
            "#
        ))
        .expect("seed schema-69 learning candidates");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 70");
    assert_eq!(db.get_schema_version(), 80);
    let worker_scope: (String, Option<String>, String, i64, String) = db
        .conn()
        .query_row(
            "SELECT memory_namespace, memory_namespace_id, memory_acl_scope,
                    memory_scope_resolved, status
             FROM hive_learning_candidates
             WHERE id = 'legacy-worker-candidate'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        worker_scope,
        (
            "crew".into(),
            Some("stable-worker-namespace".into()),
            "worker".into(),
            1,
            "pending".into(),
        )
    );
    let unresolved: (i64, String, String) = db
        .conn()
        .query_row(
            "SELECT memory_scope_resolved, status, reason
             FROM hive_learning_candidates
             WHERE id = 'legacy-unresolved-candidate'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(unresolved.0, 0);
    assert_eq!(unresolved.1, "rejected");
    assert!(unresolved
        .2
        .contains("legacy memory scope could not be proven"));

    let owner_mismatch: (i64, String, String) = db
        .conn()
        .query_row(
            "SELECT memory_scope_resolved, status, memory_namespace
             FROM hive_learning_candidates
             WHERE id = 'legacy-owner-mismatch'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(owner_mismatch, (0, "rejected".into(), "shared".into()));

    db.run_migrations().expect("migration 70 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_71_adds_fenced_worker_introduction_review_audit() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-70-worker-introduction-review.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    rewind_migration_78_for_test(&db);
    rewind_migration_77_for_test(&db);
    db.conn()
        .execute_batch(
            "DROP TABLE hive_worker_introduction_reviews;
             ALTER TABLE hive_worker_introductions DROP COLUMN decision_json;
             ALTER TABLE hive_worker_introductions DROP COLUMN proposal_revision;
             DELETE FROM schema_version WHERE version >= 71;",
        )
        .expect("rewind the migration-71 surface");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 71");
    assert_eq!(db.get_schema_version(), 80);
    let introduction_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_worker_introductions)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(introduction_columns.contains(&"proposal_revision".to_string()));
    assert!(introduction_columns.contains(&"decision_json".to_string()));
    let review_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_worker_introduction_reviews)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for column in [
        "claim_token",
        "claim_expires_at",
        "opening_message_id",
        "through_message_id",
        "user_message_ids_json",
        "transcript_digest",
        "base_identity_digest",
        "base_soul_digest",
        "worker_user_id",
        "model",
        "model_key_json",
        "model_catalog_revision",
        "provider_id",
        "trace_run_id",
        "provider_call_id",
        "usage_json",
        "proposal_id",
        "proposal_revision",
        "reviewer_output_json",
        "proposal_json",
        "decision_json",
    ] {
        assert!(
            review_columns.contains(&column.to_string()),
            "missing {column}"
        );
    }

    let now = "2026-08-25T00:00:00Z";
    db.conn()
        .execute_batch(&format!(
            r#"
            INSERT INTO sessions (id, title, created_at, updated_at, session_type)
            VALUES ('review-dm', 'Review DM', '{now}', '{now}', 'hive');
            INSERT INTO hive_workers (
                id, slug, display_name, dm_session_id, memory_namespace_id,
                created_at, updated_at
            ) VALUES (
                'review-worker', 'review-worker', 'Review Worker', 'review-dm',
                'review-worker', '{now}', '{now}'
            );
            INSERT INTO hive_worker_introductions (
                worker_id, status, prompt_version, created_at, updated_at
            ) VALUES ('review-worker', 'awaiting_context', 1, '{now}', '{now}');
            "#,
        ))
        .unwrap();
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_worker_introductions SET decision_json = 'not-json'
             WHERE worker_id = 'review-worker'",
            [],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest,
                 base_identity_digest, base_soul_digest, model,
                 model_key_json, provider_id, trace_run_id, claimed_at,
                 created_at, updated_at
             ) VALUES (
                 'bad-json', 'review-worker', 'review-dm', 'claimed', 'bad-json',
                 ?1, 1, 2, 'not-json', 'sha256:t', 'sha256:i', 'sha256:s',
                 'test-model',
                 json_object('provider', 'grok', 'model_id', 'test-model',
                             'api_format', 'open_ai_responses'),
                 'grok', 'introduction-review:bad-json', ?1, ?1, ?1
             )",
            [now],
        )
        .is_err());
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest,
                 base_identity_digest, base_soul_digest, model,
                 model_key_json, provider_id, trace_run_id, claimed_at,
                 created_at, updated_at
             ) VALUES (
                 'valid-claim', 'review-worker', 'review-dm', 'claimed', 'claim-token',
                 ?1, 1, 2, '[2]', 'sha256:t', 'sha256:i', 'sha256:s',
                 'test-model',
                 json_object('provider', 'grok', 'model_id', 'test-model',
                             'api_format', 'open_ai_responses'),
                 'grok', 'introduction-review:valid-claim', ?1, ?1, ?1
             )",
            [now],
        )
        .unwrap();
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_worker_introduction_reviews SET usage_json = 'not-json'
             WHERE id = 'valid-claim'",
            [],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_worker_introduction_reviews SET provider_id = 'anthropic'
             WHERE id = 'valid-claim'",
            [],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest,
                 base_identity_digest, base_soul_digest, model,
                 model_key_json, provider_id, trace_run_id, claimed_at,
                 created_at, updated_at
             ) VALUES (
                 'duplicate-token', 'review-worker', 'review-dm', 'claimed', 'claim-token',
                 ?1, 1, 2, '[2]', 'sha256:t', 'sha256:i', 'sha256:s',
                 'test-model',
                 json_object('provider', 'grok', 'model_id', 'test-model',
                             'api_format', 'open_ai_responses'),
                 'grok', 'introduction-review:duplicate-token', ?1, ?1, ?1
             )",
            [now],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest,
                 base_identity_digest, base_soul_digest, model,
                 model_key_json, provider_id, trace_run_id, claimed_at,
                 created_at, updated_at
             ) VALUES (
                 'unbound-ready', 'review-worker', 'review-dm', 'review_ready', 'ready-token',
                 ?1, 1, 2, '[2]', 'sha256:t', 'sha256:i', 'sha256:s',
                 'test-model',
                 json_object('provider', 'grok', 'model_id', 'test-model',
                             'api_format', 'open_ai_responses'),
                 'grok', 'introduction-review:unbound-ready', ?1, ?1, ?1
             )",
            [now],
        )
        .is_err());

    db.run_migrations().expect("migration 71 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn test_push_delivery_tables_migration() {
    let (db, _temp) = create_test_db();
    let conn = db.conn();

    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='push_delivery_attempts'",
        )
        .expect("Failed to prepare query");
    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("Failed to query tables")
        .filter_map(Result::ok)
        .collect();
    assert!(tables.contains(&"push_delivery_attempts".to_string()));

    let mut stmt = conn
        .prepare("PRAGMA table_info(push_subscriptions)")
        .expect("Failed to prepare PRAGMA");
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .expect("Failed to get columns")
        .filter_map(Result::ok)
        .collect();

    assert!(columns.contains(&"last_success_at".to_string()));
    assert!(columns.contains(&"last_failure_at".to_string()));
    assert!(columns.contains(&"last_failure_reason".to_string()));
    assert!(columns.contains(&"failure_count".to_string()));
}

#[test]
fn migration_72_freezes_provable_report_scopes_and_survives_dm_rebind() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-71-report-scopes.db");
    let fixture = rusqlite::Connection::open(&path).expect("open fixture");
    fixture
        .execute_batch(
            r#"
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO schema_version(version) VALUES (71);
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                session_type TEXT NOT NULL
            );
            CREATE TABLE reports (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                session_id TEXT NOT NULL,
                project_dir TEXT,
                content TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '',
                tags TEXT NOT NULL DEFAULT '[]',
                sources TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE hive_workers (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                dm_session_id TEXT,
                memory_namespace_id TEXT NOT NULL
            );
            CREATE TABLE hive_group_worker_lanes (
                worker_id TEXT NOT NULL,
                session_id TEXT NOT NULL
            );
            CREATE TABLE hive_controllers (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                worker_id TEXT,
                user_id TEXT
            );
            CREATE TABLE hive_runs (
                id TEXT PRIMARY KEY,
                controller_id TEXT,
                session_id TEXT,
                worker_id TEXT,
                kind TEXT NOT NULL DEFAULT 'dispatch',
                status TEXT NOT NULL DEFAULT 'succeeded',
                available_at TEXT NOT NULL DEFAULT '2026-08-25T00:00:00Z',
                lease_token TEXT,
                lease_epoch INTEGER,
                objective_message_id INTEGER
            );
            CREATE TABLE hive_worker_introduction_reviews (
                id TEXT PRIMARY KEY,
                worker_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'claimed', 'gather_more', 'review_ready', 'confirmed',
                    'rejected', 'keep_talking', 'failed', 'stale'
                )),
                claim_token TEXT NOT NULL UNIQUE,
                claim_expires_at TEXT NOT NULL,
                opening_message_id INTEGER NOT NULL,
                through_message_id INTEGER NOT NULL,
                user_message_ids_json TEXT NOT NULL
                    CHECK(json_valid(user_message_ids_json)),
                transcript_digest TEXT NOT NULL,
                base_identity_digest TEXT NOT NULL,
                base_soul_digest TEXT NOT NULL,
                worker_user_id TEXT,
                model TEXT NOT NULL,
                model_key_json TEXT NOT NULL CHECK(json_valid(model_key_json)),
                model_catalog_revision TEXT,
                provider_id TEXT NOT NULL,
                trace_run_id TEXT NOT NULL,
                provider_call_id TEXT UNIQUE,
                usage_json TEXT CHECK(usage_json IS NULL OR json_valid(usage_json)),
                proposal_id TEXT UNIQUE,
                proposal_revision INTEGER
                    CHECK(proposal_revision IS NULL OR proposal_revision > 0),
                reviewer_output_json TEXT
                    CHECK(reviewer_output_json IS NULL OR json_valid(reviewer_output_json)),
                proposal_json TEXT
                    CHECK(proposal_json IS NULL OR json_valid(proposal_json)),
                decision_json TEXT
                    CHECK(decision_json IS NULL OR json_valid(decision_json)),
                last_error TEXT,
                claimed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                CHECK(
                    json_extract(model_key_json, '$.model_id') = model
                    AND json_extract(model_key_json, '$.provider') = provider_id
                ),
                CHECK(
                    (status IN ('review_ready', 'confirmed', 'rejected', 'keep_talking')
                     AND proposal_id IS NOT NULL
                     AND proposal_revision IS NOT NULL
                     AND proposal_json IS NOT NULL)
                    OR status IN ('claimed', 'gather_more', 'failed', 'stale')
                )
            );

            INSERT INTO sessions VALUES ('ordinary', 'alice', 'code');
            INSERT INTO sessions VALUES ('primary-hive', 'alice', 'hive');
            INSERT INTO sessions VALUES ('worker-a-dm', 'alice', 'hive');
            INSERT INTO sessions VALUES ('worker-a-group', 'alice', 'hive');
            INSERT INTO sessions VALUES ('historical-controller', 'alice', 'hive');
            INSERT INTO sessions VALUES ('historical-run', 'alice', 'hive');
            INSERT INTO sessions VALUES ('historical-run-controller', 'alice', 'hive');
            INSERT INTO sessions VALUES ('controller-root', 'alice', 'hive');
            INSERT INTO sessions VALUES ('worker-a-new-dm', 'alice', 'hive');
            INSERT INTO hive_workers VALUES ('worker-a', 'alice', 'worker-a-dm', 'crew-a');
            INSERT INTO hive_workers VALUES ('worker-b', 'alice', NULL, 'crew-b');
            INSERT INTO hive_group_worker_lanes VALUES ('worker-a', 'worker-a-group');
            INSERT INTO hive_controllers VALUES (
                'controller-a', 'historical-controller', 'worker-a', 'alice'
            );
            INSERT INTO hive_controllers VALUES (
                'controller-run-a', 'controller-root', 'worker-a', 'alice'
            );
            INSERT INTO hive_runs (id, controller_id, session_id, worker_id) VALUES (
                'run-b', NULL, 'historical-run', 'worker-b'
            );
            INSERT INTO hive_runs (id, controller_id, session_id, worker_id) VALUES (
                'run-controller-a', 'controller-run-a',
                'historical-run-controller', NULL
            );

            INSERT INTO reports(id, title, session_id, content) VALUES
                ('ordinary-report', 'ordinary', 'ordinary', 'ordinary'),
                ('primary-report', 'primary', 'primary-hive', 'primary'),
                ('dm-report', 'dm', 'worker-a-dm', 'dm'),
                ('group-report', 'group', 'worker-a-group', 'group'),
                ('controller-report', 'controller', 'historical-controller', 'controller'),
                ('run-report', 'run', 'historical-run', 'run'),
                ('run-controller-report', 'run controller',
                    'historical-run-controller', 'run controller');
            "#,
        )
        .expect("seed schema-71 report fixture");
    drop(fixture);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 72");
    assert_eq!(db.get_schema_version(), 80);
    {
        let load_scope = |report_id: &str| {
            db.conn()
                .query_row(
                    "SELECT owner_user_id, memory_namespace, namespace_id,
                        acl_scope, source_worker_id
                 FROM reports WHERE id = ?1",
                    [report_id],
                    |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    },
                )
                .expect("load migrated report scope")
        };
        let shared = (
            Some("alice".to_string()),
            "shared".to_string(),
            None,
            "owner".to_string(),
            None,
        );
        assert_eq!(load_scope("ordinary-report"), shared);
        assert_eq!(load_scope("primary-report"), shared);
        for report_id in [
            "dm-report",
            "group-report",
            "controller-report",
            "run-controller-report",
        ] {
            assert_eq!(
                load_scope(report_id),
                (
                    Some("alice".to_string()),
                    "crew".to_string(),
                    Some("crew-a".to_string()),
                    "worker".to_string(),
                    Some("worker-a".to_string()),
                )
            );
        }
        assert_eq!(
            load_scope("run-report"),
            (
                Some("alice".to_string()),
                "crew".to_string(),
                Some("crew-b".to_string()),
                "worker".to_string(),
                Some("worker-b".to_string()),
            )
        );

        db.conn()
            .execute(
                "UPDATE hive_workers SET dm_session_id = 'worker-a-new-dm'
             WHERE id = 'worker-a'",
                [],
            )
            .expect("rebind Worker DM");
        assert_eq!(
            load_scope("dm-report").4.as_deref(),
            Some("worker-a"),
            "historical report remains frozen to its source Worker"
        );
        assert!(db
            .conn()
            .execute(
                "UPDATE reports SET acl_scope = 'owner', memory_namespace = 'shared',
                    namespace_id = NULL, source_worker_id = NULL
             WHERE id = 'dm-report'",
                [],
            )
            .is_err());
        assert_eq!(
            db.conn()
                .execute(
                    "UPDATE reports SET content = 'updated content'
                 WHERE id = 'dm-report'",
                    [],
                )
                .expect("non-scope report content remains mutable"),
            1
        );
    }
    drop(db);
    let reopened =
        crate::storage::database::Database::new(&path).expect("migration 72 is forward-idempotent");
    assert_eq!(reopened.get_schema_version(), 80);
}

#[test]
fn migration_72_preserves_ordinary_only_inserts_and_non_scope_updates() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-71-ordinary-only-reports.db");
    let fixture = rusqlite::Connection::open(&path).expect("open fixture");
    fixture
        .execute_batch(
            r#"
            CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
            INSERT INTO schema_version VALUES (71);
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY, user_id TEXT, session_type TEXT NOT NULL
            );
            CREATE TABLE reports (
                id TEXT PRIMARY KEY, title TEXT NOT NULL, session_id TEXT NOT NULL,
                project_dir TEXT, content TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '', tags TEXT NOT NULL DEFAULT '[]',
                sources TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL
            );
            INSERT INTO sessions VALUES ('ordinary', 'alice', 'code');
            INSERT INTO reports VALUES (
                'legacy', 'legacy', 'ordinary', NULL, 'content', '', '[]', '[]',
                '2026-08-25T00:00:00Z'
            );
            "#,
        )
        .expect("seed ordinary-only report fixture");
    drop(fixture);

    let db = crate::storage::database::Database::new(&path)
        .expect("migrate an ordinary-only report schema");
    assert_eq!(db.get_schema_version(), 80);
    assert_eq!(
        db.conn()
            .execute(
                "INSERT INTO reports (
                     id, title, session_id, project_dir, content, summary,
                     tags, sources, created_at, owner_user_id,
                     memory_namespace, namespace_id, source_worker_id, acl_scope
                 ) VALUES (
                     'new', 'new', 'ordinary', NULL, 'new content', '', '[]', '[]',
                     '2026-08-25T00:01:00Z', 'alice', 'shared', NULL, NULL, 'owner'
                 )",
                [],
            )
            .expect("exact-owner ordinary report insert remains valid"),
        1
    );
    assert!(db
        .conn()
        .execute(
            "INSERT INTO reports (
                 id, title, session_id, project_dir, content, summary,
                 tags, sources, created_at, owner_user_id,
                 memory_namespace, namespace_id, source_worker_id, acl_scope
             ) VALUES (
                 'forged-owner', 'forged', 'ordinary', NULL, 'content', '', '[]', '[]',
                 '2026-08-25T00:02:00Z', 'mallory', 'shared', NULL, NULL, 'owner'
             )",
            [],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "INSERT INTO reports (
                 id, title, session_id, project_dir, content, summary,
                 tags, sources, created_at, owner_user_id,
                 memory_namespace, namespace_id, source_worker_id, acl_scope
             ) VALUES (
                 'forged-worker', 'forged', 'ordinary', NULL, 'content', '', '[]', '[]',
                 '2026-08-25T00:03:00Z', 'alice', 'crew', 'crew-a', 'worker-a', 'worker'
             )",
            [],
        )
        .is_err());
    assert_eq!(
        db.conn()
            .execute(
                "UPDATE reports SET title = 'renamed', content = 'revised'
                 WHERE id = 'legacy'",
                [],
            )
            .expect("non-scope report fields remain mutable"),
        1
    );
    assert!(db
        .conn()
        .execute(
            "UPDATE reports SET owner_user_id = 'mallory' WHERE id = 'legacy'",
            [],
        )
        .is_err());
}

#[test]
fn migration_72_rejects_conflicting_worker_report_claims() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-71-conflicting-report-scopes.db");
    let fixture = rusqlite::Connection::open(&path).expect("open fixture");
    fixture
        .execute_batch(
            r#"
            CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
            INSERT INTO schema_version VALUES (71);
            CREATE TABLE sessions (
                id TEXT PRIMARY KEY, user_id TEXT, session_type TEXT NOT NULL
            );
            CREATE TABLE reports (
                id TEXT PRIMARY KEY, title TEXT NOT NULL, session_id TEXT NOT NULL,
                project_dir TEXT, content TEXT NOT NULL,
                summary TEXT NOT NULL DEFAULT '', tags TEXT NOT NULL DEFAULT '[]',
                sources TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL
            );
            CREATE TABLE hive_workers (
                id TEXT PRIMARY KEY, user_id TEXT, dm_session_id TEXT,
                memory_namespace_id TEXT NOT NULL
            );
            CREATE TABLE hive_runs (
                id TEXT PRIMARY KEY, controller_id TEXT,
                session_id TEXT, worker_id TEXT
            );
            INSERT INTO sessions VALUES ('claimed', 'alice', 'hive');
            INSERT INTO hive_workers VALUES ('worker-a', 'alice', 'claimed', 'crew-a');
            INSERT INTO hive_workers VALUES ('worker-b', 'alice', NULL, 'crew-b');
            INSERT INTO hive_runs VALUES ('run-b', NULL, 'claimed', 'worker-b');
            INSERT INTO reports VALUES (
                'report', 'report', 'claimed', NULL, 'content', '', '[]', '[]',
                '2026-08-25T00:00:00Z'
            );
            "#,
        )
        .expect("seed conflicting report fixture");
    drop(fixture);

    let error = crate::storage::database::Database::new(&path)
        .err()
        .expect("conflicting Worker claims must abort migration 72");
    assert!(error
        .to_string()
        .contains("conflicting Worker scope evidence"));
    let fixture = rusqlite::Connection::open(&path).expect("reopen rejected fixture");
    let version: i64 = fixture
        .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, 71);
}

#[test]
fn migration_72_rejects_missing_worker_or_source_session() {
    for (name, include_session, worker_id) in [
        ("missing-worker", true, Some("deleted-worker")),
        ("missing-session", false, None),
    ] {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let path = temp.path().join(format!("schema-71-{name}.db"));
        let fixture = rusqlite::Connection::open(&path).expect("open fixture");
        fixture
            .execute_batch(
                r#"
                CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
                INSERT INTO schema_version VALUES (71);
                CREATE TABLE sessions (
                    id TEXT PRIMARY KEY, user_id TEXT, session_type TEXT NOT NULL
                );
                CREATE TABLE reports (
                    id TEXT PRIMARY KEY, title TEXT NOT NULL, session_id TEXT NOT NULL,
                    project_dir TEXT, content TEXT NOT NULL,
                    summary TEXT NOT NULL DEFAULT '', tags TEXT NOT NULL DEFAULT '[]',
                    sources TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL
                );
                CREATE TABLE hive_workers (
                    id TEXT PRIMARY KEY, user_id TEXT, dm_session_id TEXT,
                    memory_namespace_id TEXT NOT NULL
                );
                CREATE TABLE hive_runs (
                    id TEXT PRIMARY KEY, controller_id TEXT,
                    session_id TEXT, worker_id TEXT
                );
                INSERT INTO reports VALUES (
                    'report', 'report', 'source', NULL, 'content', '', '[]', '[]',
                    '2026-08-25T00:00:00Z'
                );
                "#,
            )
            .expect("seed unresolved report fixture");
        if include_session {
            fixture
                .execute(
                    "INSERT INTO sessions VALUES ('source', 'alice', 'hive')",
                    [],
                )
                .unwrap();
        }
        if let Some(worker_id) = worker_id {
            fixture
                .execute(
                    "INSERT INTO hive_runs VALUES ('run', NULL, 'source', ?1)",
                    [worker_id],
                )
                .unwrap();
        }
        drop(fixture);

        let error = crate::storage::database::Database::new(&path)
            .err()
            .expect("unresolved report provenance must abort migration 72");
        let rendered = error.to_string();
        if include_session {
            assert!(rendered.contains("unresolved Worker report scope claims"));
        } else {
            assert!(rendered.contains("reports with no source session"));
        }
    }
}

#[test]
fn migration_74_adds_append_only_worker_governor_with_visible_defaults() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-73-worker-governor.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    rewind_migration_78_for_test(&db);
    rewind_migration_77_for_test(&db);
    rewind_migration_74_for_test(&db);
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO users (id, email)
            VALUES ('alice', 'alice@example.test');
            INSERT INTO sessions (
                id, user_id, title, created_at, updated_at, session_type
            ) VALUES (
                'governor-dm', 'alice', 'Governor DM',
                '2026-08-25T00:00:00.000000Z',
                '2026-08-25T00:00:00.000000Z', 'hive'
            );
            INSERT INTO hive_workers (
                id, user_id, slug, display_name, model, model_key_json,
                model_catalog_revision, permission_mode, autonomy, status,
                dm_session_id, memory_namespace_id, created_at, updated_at
            ) VALUES (
                'governor-worker', 'alice', 'governor-worker', 'Governor Worker',
                'grok-worker-test',
                '{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"}',
                'catalog-v1', 'autonomous', 'always_on', 'active',
                'governor-dm', 'governor-worker',
                '2026-08-25T00:00:00.000000Z',
                '2026-08-25T00:00:00.000000Z'
            );
            INSERT INTO hive_controllers (
                id, scope_key, user_id, session_id, status, timezone,
                max_concurrent_runs, worker_id, created_at, updated_at
            ) VALUES (
                'governor-controller', 'worker:governor', 'alice',
                'governor-dm', 'active', 'UTC', 1, 'governor-worker',
                '2026-08-25T00:00:00.000000Z',
                '2026-08-25T00:00:00.000000Z'
            );
            INSERT INTO hive_runs (
                id, controller_id, session_id, kind, objective, config_json,
                status, priority, available_at, attempt_count, max_attempts,
                lease_owner, lease_token, lease_epoch, lease_expires_at,
                created_at, started_at, updated_at, worker_id,
                execution_context_json
            ) VALUES (
                'governor-run', 'governor-controller', 'governor-dm',
                'legacy_resume', 'governor test', '{}', 'running', 0,
                '2026-08-25T00:00:00.000000Z', 1, 3, 'executor',
                'governor-lease', 7, '2099-08-25T00:10:00.000000Z',
                '2026-08-25T00:00:00.000000Z',
                '2026-08-25T00:00:00.000000Z',
                '2026-08-25T00:00:00.000000Z', 'governor-worker',
                '{"schema_version":1,"mode":{"kind":"worker_conversation_neutral","worker_id":"governor-worker","worker_revision":1,"lane":{"kind":"direct_message"}}}'
            );
            "#,
        )
        .expect("rewind and seed schema-73 Worker governor fixture");
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 74");
    assert_eq!(db.get_schema_version(), 80);
    let tables = db
        .conn()
        .prepare(
            "SELECT name FROM sqlite_master
             WHERE type = 'table' AND name LIKE 'hive_worker_%'
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for table in [
        "hive_worker_governor_policies",
        "hive_worker_governor_override_grants",
        "hive_worker_governor_override_consumptions",
        "hive_worker_provider_calls",
        "hive_worker_provider_call_outcomes",
        "hive_worker_idle_state",
    ] {
        assert!(tables.contains(&table.to_string()), "missing {table}");
    }
    let defaults: (i64, i64, String, Option<i64>, Option<i64>, i64, i64, String) = db
        .conn()
        .query_row(
            "SELECT daily_call_limit, daily_token_limit, timezone,
                    quiet_start_minute, quiet_end_minute, idle_base_secs,
                    idle_max_secs, tracking_started_at
             FROM hive_worker_governor_policies
             WHERE worker_id = 'governor-worker'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(defaults.0, 128);
    assert_eq!(defaults.1, 1_000_000);
    assert_eq!(defaults.2, "UTC");
    assert_eq!(defaults.3, None);
    assert_eq!(defaults.4, None);
    assert_eq!(defaults.5, 900);
    assert_eq!(defaults.6, 21_600);
    assert!(chrono::DateTime::parse_from_rfc3339(&defaults.7).is_ok());

    let run_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for column in [
        "governor_origin",
        "governor_lane_key",
        "governor_gate_reason",
        "governor_next_eligible_at",
        "governor_policy_revision",
        "governor_override_id",
    ] {
        assert!(
            run_columns.contains(&column.to_string()),
            "missing {column}"
        );
    }
    let provider_call_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_worker_provider_calls)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(provider_call_columns.contains(&"worker_revision".to_string()));
    db.conn()
        .execute(
            r#"INSERT INTO hive_runs (
                   id, controller_id, session_id, kind, objective, config_json,
                   status, priority, available_at, attempt_count, max_attempts,
                   lease_owner, lease_token, lease_epoch, lease_expires_at,
                   created_at, started_at, updated_at, worker_id,
                   execution_context_json, governor_origin, governor_lane_key
               ) VALUES (
                   'governor-call-run', 'governor-controller', 'governor-dm',
                   'worker_heartbeat', 'governor call test',
                   '{"worker_id":"governor-worker","worker_revision":1,"model":"grok-worker-test","model_key":{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"},"model_catalog_revision":"catalog-v1","permission_mode":"autonomous"}',
                   'running', 0, '2026-08-25T00:00:00.000000Z', 1, 3,
                   'executor', 'governor-call-lease', 8,
                   '2099-08-25T00:10:00.000000Z',
                   '2026-08-25T00:00:00.000000Z',
                   '2026-08-25T00:00:00.000000Z',
                   '2026-08-25T00:00:00.000000Z', 'governor-worker',
                   '{"schema_version":1,"mode":{"kind":"worker_conversation_neutral","worker_id":"governor-worker","worker_revision":1,"lane":{"kind":"direct_message"}}}',
                   'heartbeat', 'dm'
               )"#,
            [],
        )
        .unwrap();
    db.conn()
        .execute(
            r#"INSERT INTO hive_worker_provider_calls (
                provider_call_id, worker_id, worker_revision, owner_user_id,
                session_id, run_id,
                run_lease_token, run_lease_epoch, run_lease_expires_at,
                origin, lane_key, call_kind, provider_id, model_id,
                model_key_json, model_key_fingerprint, model_catalog_revision,
                permission_mode, policy_revision, timezone, local_day,
                reserved_tokens, started_at
            ) VALUES (
                'migration-74-call', 'governor-worker', 1, 'alice', 'governor-dm',
                'governor-call-run', 'governor-call-lease', 8,
                '2099-08-25T00:10:00.000000Z', 'heartbeat', 'dm', 'agent_turn',
                'grok', 'grok-worker-test',
                '{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"}',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'catalog-v1', 'autonomous', 1, 'UTC', '2026-08-25', 100,
                '2026-08-25T00:00:01.000000Z'
            )"#,
            [],
        )
        .expect("insert exact-bound Started row");
    assert!(db
        .conn()
        .execute(
            r#"INSERT INTO hive_worker_provider_calls (
                provider_call_id, worker_id, worker_revision, owner_user_id,
                session_id, run_id, run_lease_token, run_lease_epoch,
                run_lease_expires_at, origin, lane_key, call_kind, provider_id,
                model_id, model_key_json, model_key_fingerprint,
                model_catalog_revision, permission_mode, policy_revision,
                timezone, local_day, reserved_tokens, started_at
            ) VALUES (
                'wrong-worker-revision', 'governor-worker', 2, 'alice',
                'governor-dm', 'governor-call-run', 'governor-call-lease', 8,
                '2099-08-25T00:10:00.000000Z', 'heartbeat', 'dm',
                'agent_turn', 'grok', 'grok-worker-test',
                '{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"}',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                'catalog-v1', 'autonomous', 1, 'UTC', '2026-08-25', 100,
                '2026-08-25T00:00:01.000000Z'
            )"#,
            [],
        )
        .is_err());
    db.conn()
        .execute(
            "INSERT INTO hive_worker_provider_call_outcomes (
                provider_call_id, state, outcome, remote_acceptance,
                usage_json, usage_total_tokens, finished_at
             ) VALUES (
                'migration-74-call', 'completed', 'success', 'acknowledged',
                '{\"prompt_tokens\":10,\"completion_tokens\":5,\"reasoning_tokens\":0,\"total_tokens\":15,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0}',
                15, '2026-08-25T00:00:02.000000Z'
             )",
            [],
        )
        .expect("append terminal outcome");
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_worker_provider_calls SET reserved_tokens = 1
             WHERE provider_call_id = 'migration-74-call'",
            [],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_worker_provider_call_outcomes SET outcome = 'forged'
             WHERE provider_call_id = 'migration-74-call'",
            [],
        )
        .is_err());

    db.conn()
        .execute_batch(
            "INSERT INTO sessions (
                 id, user_id, title, created_at, updated_at, session_type
             ) VALUES (
                 'new-worker-dm', 'alice', 'New Worker DM',
                 '2026-08-25T01:00:00.000000Z',
                 '2026-08-25T01:00:00.000000Z', 'hive'
             );
             INSERT INTO hive_workers (
                 id, user_id, slug, display_name, permission_mode, autonomy,
                 status, dm_session_id, memory_namespace_id, created_at, updated_at
             ) VALUES (
                 'new-worker', 'alice', 'new-worker', 'New Worker',
                 'autonomous', 'manual', 'active', 'new-worker-dm', 'new-worker',
                 '2026-08-25T01:00:00.000000Z',
                 '2026-08-25T01:00:00.000000Z'
             );",
        )
        .expect("create post-migration Worker");
    let new_policy_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_worker_governor_policies
             WHERE worker_id = 'new-worker'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(new_policy_count, 1);

    db.run_migrations().expect("migration 74 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migrations_75_and_76_add_conversation_and_worker_workflow_authority() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-75-worker-conversation.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    rewind_migration_78_for_test(&db);
    rewind_migration_77_for_test(&db);
    rewind_migrations_75_and_76_for_test(&db);
    drop(db);

    let db = crate::storage::database::Database::new(&path).expect("apply migration 75");
    assert_eq!(db.get_schema_version(), 80);

    let run_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for column in [
        "execution_context_json",
        "conversation_through_message_id",
        "response_message_id",
        "response_group_message_id",
        "response_provider_call_id",
        "workflow_goal_id",
        "workflow_attempt_id",
    ] {
        assert!(
            run_columns.contains(&column.to_string()),
            "missing {column}"
        );
    }
    let input_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_worker_conversation_inputs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for column in [
        "worker_id",
        "owner_user_id",
        "session_id",
        "request_id",
        "accepted_while_run_id",
        "content_json",
        "state",
        "canonical_message_id",
        "assigned_run_id",
    ] {
        assert!(
            input_columns.contains(&column.to_string()),
            "missing {column}"
        );
    }
    let provider_call_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_worker_provider_calls)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(provider_call_columns.contains(&"worker_revision".to_string()));
    let run_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(run_sql.contains("worker_conversation"));
    assert!(run_sql.contains("worker_workflow"));
    let guards: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'trigger' AND name IN (
                 'hive_runs_worker_context_insert_guard',
                 'hive_runs_response_insert_guard',
                 'hive_runs_worker_conversation_insert_guard',
                 'hive_runs_response_tuple_guard',
                 'hive_runs_response_provider_call_guard',
                 'hive_worker_conversation_inputs_insert_guard',
                 'hive_worker_conversation_inputs_no_delete',
                 'hive_worker_conversation_inputs_materialize_guard'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(guards, 8);
    let workflow_guards: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'trigger' AND name IN (
                 'hive_runs_worker_workflow_insert_guard',
                 'hive_runs_worker_workflow_link_immutable',
                 'hive_runs_non_workflow_link_insert_guard',
                 'hive_runs_non_workflow_link_update_guard',
                 'hive_worker_provider_calls_workflow_guard',
                 'hive_worker_goal_outcomes_insert_guard',
                 'hive_runs_worker_workflow_success_guard'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(workflow_guards, 7);
    let outcome_table: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'hive_worker_goal_outcomes'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(outcome_table, 1);
    let group_response_guard_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'trigger' AND name = 'hive_runs_group_response_link_guard'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(group_response_guard_sql.contains("':run:' || NEW.id || ':final'"));
    let response_guard_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'trigger' AND name = 'hive_runs_response_message_link_guard'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(response_guard_sql.contains("'worker-run:' || NEW.id || ':assistant:final'"));
    assert!(response_guard_sql.contains("':context-response'"));
    assert!(response_guard_sql.contains("NEW.response_provider_call_id IS NULL"));

    db.run_migrations().expect("migration 76 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_76_repairs_a_schema_75_preview_missing_response_provenance() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let path = temp.path().join("schema-75-preview-catchup.db");
    let db = crate::storage::database::Database::new(&path).expect("create current database");
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO sessions (id, title, created_at, updated_at)
            VALUES ('preserved-session', 'Preserve me',
                    '2026-08-25T00:00:00Z', '2026-08-25T00:00:00Z');
            "#,
        )
        .expect("seed preserved schema-75 preview data");
    rewind_migration_78_for_test(&db);
    rewind_migration_77_for_test(&db);
    db.conn()
        .execute_batch(
            r#"
            DROP TRIGGER IF EXISTS hive_runs_response_insert_guard;
            DROP TRIGGER IF EXISTS hive_runs_response_tuple_guard;
            DROP TRIGGER IF EXISTS hive_runs_response_provider_call_guard;
            DROP TRIGGER IF EXISTS hive_runs_response_message_immutable;
            DROP TRIGGER IF EXISTS hive_runs_response_message_link_guard;
            DROP TRIGGER IF EXISTS hive_runs_group_response_link_guard;
            DROP INDEX IF EXISTS idx_hive_runs_response_provider_call;
            ALTER TABLE hive_runs DROP COLUMN response_provider_call_id;
            DELETE FROM schema_version WHERE version = 76;
            "#,
        )
        .expect("create stamped-75 preview gap");
    assert_eq!(db.get_schema_version(), 75);
    drop(db);

    let repaired = crate::storage::database::Database::new(&path).expect("apply catch-up");
    assert_eq!(repaired.get_schema_version(), 80);
    let columns = repaired
        .conn()
        .prepare("PRAGMA table_info(hive_runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(columns.contains(&"response_provider_call_id".to_string()));
    assert!(columns.contains(&"workflow_goal_id".to_string()));
    assert!(columns.contains(&"workflow_attempt_id".to_string()));
    let preserved: String = repaired
        .conn()
        .query_row(
            "SELECT title FROM sessions WHERE id = 'preserved-session'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(preserved, "Preserve me");
    let restored_guards: i64 = repaired
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'trigger' AND name IN (
                 'hive_runs_response_insert_guard',
                 'hive_runs_response_message_link_guard',
                 'hive_runs_response_tuple_guard',
                 'hive_runs_response_provider_call_guard',
                 'hive_runs_response_message_immutable',
                 'hive_runs_group_response_link_guard'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(restored_guards, 6);
}

fn rewind_migration_78_for_test(db: &crate::storage::database::Database) {
    const ACCEPTANCE_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow', 'worker_introduction_review', 'worker_workflow_acceptance')";
    const REVIEW_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow', 'worker_introduction_review')";
    const ACCEPTANCE_ORIGIN_TAIL: &str =
        "'workflow_rollover', 'workflow_acceptance', 'lifecycle_sweep', 'controller_child'";
    const REVIEW_ORIGIN_TAIL: &str = "'workflow_rollover', 'lifecycle_sweep', 'controller_child'";

    db.conn()
        .execute_batch(
            r#"
            DROP TABLE IF EXISTS spatial_canvas_resource_creates;
            DROP TABLE IF EXISTS spatial_canvas_events;
            DROP TABLE IF EXISTS spatial_canvases;
            DELETE FROM schema_version WHERE version >= 79;
            DROP TRIGGER hive_runs_worker_context_insert_guard;
            DROP TRIGGER hive_runs_non_workflow_link_insert_guard;
            DROP TRIGGER hive_runs_non_workflow_link_update_guard;
            DROP TRIGGER hive_runs_worker_acceptance_insert_guard;
            DROP TRIGGER hive_runs_worker_acceptance_immutable;
            DROP TRIGGER hive_worker_goal_acceptance_candidate_insert_guard;
            DROP TRIGGER hive_worker_goal_acceptance_candidate_update_guard;
            DROP TRIGGER hive_worker_goal_acceptance_candidate_no_delete;
            DROP TRIGGER hive_worker_goal_acceptance_result_insert_guard;
            DROP TRIGGER hive_worker_goal_acceptance_result_no_update;
            DROP TRIGGER hive_worker_goal_acceptance_result_no_delete;
            DROP TRIGGER hive_runs_worker_acceptance_status_guard;
            DROP TRIGGER hive_worker_provider_calls_acceptance_disabled;
            DROP TRIGGER hive_runs_worker_workflow_progressed_acceptance_guard;
            DROP TRIGGER hive_worker_conversation_inputs_insert_guard;
            CREATE TRIGGER hive_worker_conversation_inputs_insert_guard
            BEFORE INSERT ON hive_worker_conversation_inputs
            WHEN NEW.state <> 'staged'
              OR NEW.canonical_message_id IS NOT NULL
              OR NEW.assigned_run_id IS NOT NULL
              OR NEW.materialized_at IS NOT NULL
              OR NOT EXISTS (
                  SELECT 1
                  FROM hive_workers worker
                  JOIN sessions session ON session.id = NEW.session_id
                  JOIN hive_runs active
                    ON active.id = NEW.accepted_while_run_id
                  JOIN hive_controllers controller
                    ON controller.id = active.controller_id
                  WHERE worker.id = NEW.worker_id
                    AND worker.user_id IS NEW.owner_user_id
                    AND worker.status = 'active'
                    AND worker.dm_session_id = session.id
                    AND session.user_id IS worker.user_id
                    AND session.session_type = 'hive'
                    AND active.worker_id = worker.id
                    AND active.session_id = session.id
                    AND active.status IN (
                        'queued', 'leased', 'running', 'sleeping',
                        'retry_wait', 'recovery_required'
                    )
                    AND controller.worker_id = worker.id
                    AND controller.session_id = session.id
                    AND controller.user_id IS worker.user_id
                    AND controller.status = 'active'
              )
            BEGIN
                SELECT RAISE(ABORT, 'invalid Worker conversation input binding');
            END;
            DROP TRIGGER hive_worker_conversation_inputs_materialize_guard;
            CREATE TRIGGER hive_worker_conversation_inputs_materialize_guard
            BEFORE UPDATE OF state, canonical_message_id, assigned_run_id, materialized_at
            ON hive_worker_conversation_inputs
            WHEN NEW.state = 'materialized' AND NOT EXISTS (
                SELECT 1 FROM hive_runs completed
                WHERE completed.id = NEW.accepted_while_run_id
                  AND completed.response_message_id IS NOT NULL
            )
            BEGIN
                SELECT RAISE(ABORT, 'migration-77 materialized Worker input guard');
            END;
            DROP TABLE hive_worker_goal_acceptance_results;
            DROP TABLE hive_worker_goal_acceptance_candidates;
            DROP INDEX idx_hive_worker_workflow_attempt;
            CREATE UNIQUE INDEX idx_hive_worker_workflow_attempt
                ON hive_runs(workflow_attempt_id)
                WHERE workflow_attempt_id IS NOT NULL;
            DELETE FROM schema_version WHERE version = 78;
            "#,
        )
        .expect("rewind migration-78 objects");
    db.conn()
        .pragma_update(None, "writable_schema", "ON")
        .expect("enable writable_schema for migration-78 rewind");
    db.conn()
        .execute(
            "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
             WHERE type = 'table' AND name = 'hive_runs'",
            [ACCEPTANCE_KINDS, REVIEW_KINDS],
        )
        .expect("rewind acceptance run kind CHECK");
    db.conn()
        .execute(
            "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
             WHERE type = 'table' AND name = 'hive_runs'",
            [ACCEPTANCE_ORIGIN_TAIL, REVIEW_ORIGIN_TAIL],
        )
        .expect("rewind acceptance governor-origin CHECK");
    db.conn()
        .pragma_update(None, "writable_schema", "RESET")
        .expect("reload migration-77 schema");
    assert_eq!(db.get_schema_version(), 77);
}

fn rewind_migration_77_for_test(db: &crate::storage::database::Database) {
    const WORKFLOW_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow')";
    const REVIEW_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow', 'worker_introduction_review')";

    db.conn()
        .pragma_update(None, "writable_schema", "ON")
        .expect("enable writable_schema for migration-77 rewind");
    db.conn()
        .execute(
            "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
             WHERE type = 'table' AND name = 'hive_runs' AND instr(sql, ?1) > 0",
            [REVIEW_KINDS, WORKFLOW_KINDS],
        )
        .expect("rewind Introduction review run kind CHECK");
    db.conn()
        .execute(
            "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
             WHERE type = 'table' AND name = 'hive_worker_introduction_reviews'",
            [
                "'queued', 'claimed', 'gather_more', 'review_ready'",
                "'claimed', 'gather_more', 'review_ready'",
            ],
        )
        .expect("rewind queued Introduction review status CHECK");
    db.conn()
        .execute(
            "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
             WHERE type = 'table' AND name = 'hive_worker_introduction_reviews'",
            [
                "OR status IN ('queued', 'claimed', 'gather_more', 'failed', 'stale')",
                "OR status IN ('claimed', 'gather_more', 'failed', 'stale')",
            ],
        )
        .expect("rewind queued Introduction review proposal CHECK");
    db.conn()
        .pragma_update(None, "writable_schema", "RESET")
        .expect("reload migration-76 CHECK constraints");
    db.conn()
        .execute_batch(
            r#"
            DROP TRIGGER hive_worker_introduction_review_run_insert_guard;
            DROP TRIGGER hive_worker_introduction_review_run_immutable;
            DROP TRIGGER hive_worker_introduction_review_provider_guard;
            DROP TRIGGER hive_worker_introduction_review_call_kind_guard;
            DROP TRIGGER hive_runs_worker_introduction_review_success_guard;
            DROP INDEX idx_hive_worker_introduction_review_run;
            DROP INDEX idx_hive_worker_introduction_review_attempt;
            DELETE FROM schema_version WHERE version = 77;
            "#,
        )
        .expect("rewind migration-77 guards");
    assert_eq!(db.get_schema_version(), 76);
}

fn drop_migrations_75_and_76_run_dependents_for_test(db: &crate::storage::database::Database) {
    db.conn()
        .execute_batch(
            r#"
            DROP TRIGGER IF EXISTS hive_worker_provider_calls_binding_guard;
            DROP TRIGGER IF EXISTS hive_runs_worker_context_insert_guard;
            DROP TRIGGER IF EXISTS hive_runs_response_insert_guard;
            DROP TRIGGER IF EXISTS hive_runs_worker_legacy_resume_guard;
            DROP TRIGGER IF EXISTS hive_runs_worker_conversation_insert_guard;
            DROP TRIGGER IF EXISTS hive_runs_worker_binding_immutable;
            DROP TRIGGER IF EXISTS hive_runs_response_message_link_guard;
            DROP TRIGGER IF EXISTS hive_runs_response_tuple_guard;
            DROP TRIGGER IF EXISTS hive_runs_response_provider_call_guard;
            DROP TRIGGER IF EXISTS hive_runs_response_message_immutable;
            DROP TRIGGER IF EXISTS hive_runs_group_response_link_guard;
            DROP TRIGGER IF EXISTS hive_runs_worker_workflow_insert_guard;
            DROP TRIGGER IF EXISTS hive_runs_worker_workflow_link_immutable;
            DROP TRIGGER IF EXISTS hive_runs_non_workflow_link_insert_guard;
            DROP TRIGGER IF EXISTS hive_runs_non_workflow_link_update_guard;
            DROP TRIGGER IF EXISTS hive_worker_provider_calls_workflow_guard;
            DROP TRIGGER IF EXISTS hive_worker_goal_outcomes_insert_guard;
            DROP TRIGGER IF EXISTS hive_runs_worker_workflow_success_guard;
            DROP TRIGGER IF EXISTS hive_worker_goal_outcomes_no_update;
            DROP TRIGGER IF EXISTS hive_worker_goal_outcomes_no_delete;
            DROP INDEX IF EXISTS idx_hive_worker_conversation_objective;
            DROP INDEX IF EXISTS idx_hive_runs_response_message;
            DROP INDEX IF EXISTS idx_hive_runs_response_group_message;
            DROP INDEX IF EXISTS idx_hive_runs_response_provider_call;
            DROP INDEX IF EXISTS idx_hive_worker_conversation_recovery;
            DROP INDEX IF EXISTS idx_hive_worker_workflow_attempt;
            DROP INDEX IF EXISTS idx_hive_worker_workflow_one_nonterminal;
            DROP INDEX IF EXISTS idx_hive_worker_workflow_recovery;
            "#,
        )
        .expect("drop migration-75/76 run dependents");
}

fn rewind_migrations_75_and_76_for_test(db: &crate::storage::database::Database) {
    drop_migrations_75_and_76_run_dependents_for_test(db);
    db.conn()
        .execute_batch(
            r#"
            DROP TABLE IF EXISTS hive_worker_goal_outcomes;
            DROP TABLE hive_worker_conversation_inputs;
            ALTER TABLE hive_worker_provider_calls DROP COLUMN worker_revision;
            ALTER TABLE hive_runs DROP COLUMN response_provider_call_id;
            ALTER TABLE hive_runs DROP COLUMN response_group_message_id;
            ALTER TABLE hive_runs DROP COLUMN response_message_id;
            ALTER TABLE hive_runs DROP COLUMN conversation_through_message_id;
            ALTER TABLE hive_runs DROP COLUMN execution_context_json;
            ALTER TABLE hive_runs DROP COLUMN workflow_attempt_id;
            ALTER TABLE hive_runs DROP COLUMN workflow_goal_id;
            DELETE FROM schema_version WHERE version >= 75;
            "#,
        )
        .expect("rewind Worker conversation and Workflow surfaces to migration 74");
    assert_eq!(db.get_schema_version(), 74);
}

fn rewind_migration_74_for_test(db: &crate::storage::database::Database) {
    // Migration 75 replaces several migration-74 guards under the same names.
    // Remove the complete downstream run-dependent set before taking columns
    // and ledger tables back to their schema-73 shape.
    drop_migrations_75_and_76_run_dependents_for_test(db);
    db.conn()
        .execute_batch(
            r#"
            DROP TRIGGER IF EXISTS hive_worker_governor_override_consumption_guard;
            DROP TRIGGER IF EXISTS hive_worker_governor_override_owner_guard;
            DROP TRIGGER IF EXISTS hive_workers_governor_policy_after_insert;
            DROP TRIGGER IF EXISTS hive_worker_governor_policy_identity_immutable;
            DROP TRIGGER IF EXISTS hive_worker_provider_calls_no_update;
            DROP TRIGGER IF EXISTS hive_worker_provider_calls_no_delete;
            DROP TRIGGER IF EXISTS hive_worker_provider_call_outcomes_no_update;
            DROP TRIGGER IF EXISTS hive_worker_provider_call_outcomes_no_delete;
            DROP TRIGGER IF EXISTS hive_worker_governor_override_grants_no_update;
            DROP TRIGGER IF EXISTS hive_worker_governor_override_grants_no_delete;
            DROP TRIGGER IF EXISTS hive_worker_governor_override_consumptions_no_update;
            DROP TRIGGER IF EXISTS hive_worker_governor_override_consumptions_no_delete;
            DROP INDEX IF EXISTS idx_hive_runs_governor_gate;
            ALTER TABLE hive_runs DROP COLUMN governor_override_id;
            ALTER TABLE hive_runs DROP COLUMN governor_policy_revision;
            ALTER TABLE hive_runs DROP COLUMN governor_next_eligible_at;
            ALTER TABLE hive_runs DROP COLUMN governor_gate_reason;
            ALTER TABLE hive_runs DROP COLUMN governor_lane_key;
            ALTER TABLE hive_runs DROP COLUMN governor_origin;
            DROP TABLE hive_worker_governor_override_consumptions;
            DROP TABLE hive_worker_provider_call_outcomes;
            DROP TABLE hive_worker_provider_calls;
            DROP TABLE hive_worker_governor_override_grants;
            DROP TABLE hive_worker_idle_state;
            DROP TABLE hive_worker_governor_policies;
            DELETE FROM schema_version WHERE version >= 74;
            "#,
        )
        .expect("rewind Worker governor surface to migration 73");
    assert_eq!(db.get_schema_version(), 73);
}

fn release_migration_72_report_guards_for_test(db: &crate::storage::database::Database) {
    db.conn()
        .execute_batch(
            r#"
            DROP TRIGGER IF EXISTS reports_scope_insert_guard;
            DROP TRIGGER IF EXISTS reports_scope_immutable;
            DROP INDEX IF EXISTS idx_reports_frozen_reader_scope;
            "#,
        )
        .expect("release migration-72 report guards");
}

fn drop_post_68_worker_conversation_tables_for_test(db: &crate::storage::database::Database) {
    db.conn()
        .execute_batch(
            r#"
            DROP TABLE hive_worker_introduction_reviews;
            DROP TABLE hive_worker_introductions;
            DROP TABLE hive_group_worker_lanes;
            "#,
        )
        .expect("drop post-68 Worker conversation tables");
}

#[test]
fn migration_77_adds_crash_safe_worker_introduction_review_runs() {
    let (db, _temp) = create_test_db();
    rewind_migration_78_for_test(&db);
    let now = "2026-08-25T00:00:00Z";
    db.conn()
        .execute_batch(&format!(
            r#"
            INSERT INTO sessions (
                id, title, model, model_key_json, permission_mode,
                created_at, updated_at, session_type
            ) VALUES (
                'migration-77-dm', 'Review DM', 'grok-worker-test',
                '{{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"}}',
                'autonomous', '{now}', '{now}', 'hive'
            );
            INSERT INTO hive_workers (
                id, slug, display_name, model, model_key_json,
                permission_mode, autonomy, status, dm_session_id,
                memory_namespace_id, created_at, updated_at
            ) VALUES (
                'migration-77-worker', 'migration-77-worker', 'Migration 77 Worker',
                'grok-worker-test',
                '{{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"}}',
                'autonomous', 'manual', 'active', 'migration-77-dm',
                'migration-77-worker', '{now}', '{now}'
            );
            INSERT INTO hive_controllers (
                id, scope_key, user_id, session_id, status, timezone,
                max_concurrent_runs, worker_id, created_at, updated_at
            ) VALUES (
                'migration-77-controller', 'worker:migration-77-worker', NULL,
                'migration-77-dm', 'active', 'UTC', 1,
                'migration-77-worker', '{now}', '{now}'
            );
            INSERT INTO messages (id, session_id, role, content, created_at) VALUES
                (77001, 'migration-77-dm', 'assistant',
                 '[{{"type":"text","text":"opening"}}]', '{now}'),
                (77002, 'migration-77-dm', 'assistant',
                 '[{{"type":"text","text":"response"}}]', '{now}');
            INSERT INTO hive_worker_introduction_reviews (
                id, worker_id, session_id, status, claim_token,
                claim_expires_at, opening_message_id, through_message_id,
                user_message_ids_json, transcript_digest,
                base_identity_digest, base_soul_digest, model,
                model_key_json, provider_id, trace_run_id, last_error,
                claimed_at, created_at, updated_at, completed_at
            ) VALUES (
                'migration-77-legacy-review', 'migration-77-worker',
                'migration-77-dm', 'failed', 'migration-77-legacy-claim',
                '{now}', 77001, 77002, '[77002]', 'transcript', 'identity', 'soul',
                'grok-worker-test',
                '{{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"}}',
                'grok', 'migration-77-legacy-trace', 'legacy failure',
                '{now}', '{now}', '{now}', '{now}'
            );
            "#,
        ))
        .expect("seed a migration-76-compatible legacy review audit");
    rewind_migration_77_for_test(&db);
    let pre_migration_run_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let pre_migration_review_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'hive_worker_introduction_reviews'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!pre_migration_run_sql.contains("worker_introduction_review"));
    assert!(!pre_migration_review_sql.contains("'queued', 'claimed', 'gather_more'"));

    db.run_migrations().expect("apply migration 77");
    assert_eq!(db.get_schema_version(), 80);
    let review_columns = db
        .conn()
        .prepare("PRAGMA table_info(hive_worker_introduction_reviews)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(review_columns.contains(&"run_id".to_string()));
    assert!(review_columns.contains(&"attempt_no".to_string()));
    let review_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'hive_worker_introduction_reviews'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(review_sql.contains("'queued', 'claimed', 'gather_more'"));
    let run_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(run_sql.contains("worker_introduction_review"));
    let guards: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'trigger' AND name IN (
                 'hive_worker_introduction_review_run_insert_guard',
                 'hive_worker_introduction_review_run_immutable',
                 'hive_worker_introduction_review_provider_guard',
                 'hive_worker_introduction_review_call_kind_guard',
                 'hive_runs_worker_introduction_review_success_guard'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(guards, 5);
    let preserved: (String, Option<String>, Option<i64>) = db
        .conn()
        .query_row(
            "SELECT status, run_id, attempt_no
             FROM hive_worker_introduction_reviews
             WHERE id = 'migration-77-legacy-review'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(preserved, ("failed".into(), None, None));
    assert!(db
        .conn()
        .execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest,
                 base_identity_digest, base_soul_digest, model,
                 model_key_json, provider_id, trace_run_id, claimed_at,
                 created_at, updated_at, run_id, attempt_no
             ) VALUES (
                 'bad-review-binding', 'migration-77-worker', 'migration-77-dm',
                 'queued', 'bad-review-binding-claim', ?1, 1, 2, '[2]',
                 'transcript', 'identity', 'soul', 'grok-worker-test',
                 '{\"provider\":\"grok\",\"model_id\":\"grok-worker-test\",\"api_format\":\"open_ai_responses\"}',
                 'grok', 'missing-review-run', ?1, ?1, ?1,
                 'missing-review-run', 1
             )",
            [now],
        )
        .is_err());
    db.conn()
        .execute(
            r#"INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, priority, available_at, attempt_count, max_attempts,
                 created_at, updated_at, worker_id, governor_origin,
                 governor_lane_key, execution_context_json,
                 conversation_through_message_id
             ) VALUES (
                 'migration-77-unproven-run', 'migration-77-controller',
                 'migration-77-dm', 'worker_introduction_review',
                 'must not succeed without its audit', '{}', 'queued', 60,
                 ?1, 0, 1, ?1, ?1, 'migration-77-worker',
                 'user_lifecycle_action', 'dm',
                 '{"schema_version":1,"mode":{"kind":"worker_conversation_neutral","worker_id":"migration-77-worker","worker_revision":1,"lane":{"kind":"direct_message"}}}',
                 77002
             )"#,
            [now],
        )
        .expect("insert unproven review run canary");
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_runs SET status = 'succeeded', updated_at = ?2
             WHERE id = ?1",
            rusqlite::params!["migration-77-unproven-run", now],
        )
        .is_err());
    assert!(db
        .conn()
        .execute(
            "INSERT INTO hive_worker_provider_calls (
                 provider_call_id, worker_id, worker_revision, session_id,
                 run_id, run_lease_token, run_lease_epoch,
                 run_lease_expires_at, origin, lane_key, call_kind,
                 provider_id, model_id, model_key_json,
                 model_key_fingerprint, permission_mode, policy_revision,
                 timezone, local_day, reserved_tokens, started_at
             ) VALUES (
                 'bad-review-call', 'migration-77-worker', 1,
                 'migration-77-dm', 'missing-review-run', 'lease', 1,
                 '2099-01-01T00:00:00Z', 'user_lifecycle_action', 'dm',
                 'worker_introduction_review', 'grok', 'grok-worker-test',
                 '{\"provider\":\"grok\",\"model_id\":\"grok-worker-test\",\"api_format\":\"open_ai_responses\"}',
                 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 'autonomous', 1, 'UTC', '2026-08-25', 100, ?1
             )",
            [now],
        )
        .is_err());
    let integrity: String = db
        .conn()
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let foreign_key_errors: i64 = db
        .conn()
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(foreign_key_errors, 0);
    db.run_migrations().expect("migration 77 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
}

#[test]
fn migration_78_adds_fail_closed_worker_workflow_acceptance_authority_idempotently() {
    let (mut db, temp) = create_test_db();
    assert_eq!(db.get_schema_version(), 80);
    rewind_migration_78_for_test(&db);

    db.run_migrations().expect("upgrade migration 77 to 78");
    assert_eq!(db.get_schema_version(), 80);
    let run_sql: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'hive_runs'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(run_sql.contains("worker_workflow_acceptance"));
    assert!(run_sql.contains("'workflow_acceptance'"));
    let tables: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name IN (
                 'hive_worker_goal_acceptance_candidates',
                 'hive_worker_goal_acceptance_results'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tables, 2);
    let result_projection_columns: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info(
                 'hive_worker_goal_acceptance_results'
             )
             WHERE name IN (
                 'resulting_goal_revision', 'resulting_goal_status',
                 'resulting_step_status'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(result_projection_columns, 3);
    let guards: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'trigger' AND name IN (
                 'hive_runs_worker_acceptance_insert_guard',
                 'hive_runs_worker_acceptance_immutable',
                 'hive_worker_goal_acceptance_candidate_insert_guard',
                 'hive_worker_goal_acceptance_candidate_update_guard',
                 'hive_worker_goal_acceptance_candidate_no_delete',
                 'hive_worker_goal_acceptance_result_insert_guard',
                 'hive_worker_goal_acceptance_result_no_update',
                 'hive_worker_goal_acceptance_result_no_delete',
                 'hive_runs_worker_acceptance_status_guard',
                 'hive_worker_provider_calls_acceptance_disabled',
                 'hive_runs_worker_workflow_progressed_acceptance_guard'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(guards, 11);
    let terminal_promotion_guard: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'trigger'
               AND name = 'hive_worker_conversation_inputs_materialize_guard'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(terminal_promotion_guard.contains("predecessor.status IN"));
    for terminal_status in ["'failed'", "'dead_letter'", "'cancelled'"] {
        assert!(terminal_promotion_guard.contains(terminal_status));
    }
    assert!(terminal_promotion_guard.contains("worker_conversation_neutral"));
    assert!(terminal_promotion_guard.contains("worker_workspace_attached"));
    assert!(terminal_promotion_guard.contains("response_message_id IS NULL"));
    assert!(terminal_promotion_guard.contains("hive_worker_provider_call_outcomes"));
    assert!(terminal_promotion_guard.contains("outcome.state = 'unknown'"));
    assert!(terminal_promotion_guard.contains("hive_worker_governor_override_grants"));
    assert!(terminal_promotion_guard.contains("owner_acknowledged_governor_recovery"));
    assert!(terminal_promotion_guard.contains("owner_acknowledged_provider_response_loss"));
    assert!(terminal_promotion_guard.contains("governor_recovery_grant_id"));
    assert!(terminal_promotion_guard.contains("WITH RECURSIVE conversation_chain"));
    assert!(terminal_promotion_guard.contains("component_tail"));
    assert!(terminal_promotion_guard.contains("ledger.id <> NEW.id"));
    assert!(terminal_promotion_guard.contains("ORDER BY ledger.canonical_message_id DESC"));
    assert!(!terminal_promotion_guard.contains("bridge_grant"));
    assert!(terminal_promotion_guard.contains("assigned.governor_override_id"));
    assert!(terminal_promotion_guard.contains("late_recovery_call.started_at"));
    assert!(terminal_promotion_guard.contains(">= recovery_grant.created_at"));
    assert!(terminal_promotion_guard.contains("recovery_grant.expires_at"));
    assert!(terminal_promotion_guard.contains("response_loss_grant.expires_at"));
    assert!(terminal_promotion_guard.contains("hive_worker_governor_override_consumptions"));
    for exact_null_link in [
        "predecessor.schedule_id IS NULL",
        "predecessor.occurrence_id IS NULL",
        "predecessor.group_id IS NULL",
        "predecessor.workflow_goal_id IS NULL",
        "predecessor.workflow_attempt_id IS NULL",
    ] {
        assert!(terminal_promotion_guard.contains(exact_null_link));
    }
    let input_insert_guard: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'trigger'
               AND name = 'hive_worker_conversation_inputs_insert_guard'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(input_insert_guard.contains("active.kind = 'worker_conversation'"));
    assert!(input_insert_guard.contains("active.kind = 'worker_introduction_review'"));
    assert!(input_insert_guard.contains("review.status = 'stale'"));
    assert!(input_insert_guard.contains("provider_call.run_id = active.id"));
    for exact_null_link in [
        "active.schedule_id IS NULL",
        "active.occurrence_id IS NULL",
        "active.group_id IS NULL",
        "active.workflow_goal_id IS NULL",
        "active.workflow_attempt_id IS NULL",
    ] {
        assert!(input_insert_guard.contains(exact_null_link));
    }
    let workflow_attempt_index: String = db
        .conn()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'idx_hive_worker_workflow_attempt'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(workflow_attempt_index.contains("kind = 'worker_workflow'"));

    let db_path = temp.path().join("test.db");
    drop(db);
    db = crate::storage::Database::new(&db_path).expect("reopen migrated database");
    db.run_migrations().expect("migration 78 is idempotent");
    assert_eq!(db.get_schema_version(), 80);
    let integrity: String = db
        .conn()
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let foreign_key_errors: i64 = db
        .conn()
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(foreign_key_errors, 0);
}
