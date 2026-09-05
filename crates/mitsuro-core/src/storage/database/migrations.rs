use anyhow::{ensure, Context, Result};
use rusqlite::{Transaction, TransactionBehavior};
use tracing::{debug, info};

use super::{Database, SCHEMA_VERSION};

impl Database {
    /// Get the current schema version from database
    #[cfg(test)]
    pub(crate) fn get_schema_version(&self) -> i32 {
        // Create version table if it doesn't exist
        if let Err(e) = self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        ) {
            tracing::warn!("Failed to create schema_version table: {}", e);
            // Table creation failed, assume version 0
            return 0;
        }

        self.conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0)
    }

    /// Set schema version within a transaction
    fn set_schema_version_tx(&self, tx: &rusqlite::Transaction, version: i32) -> Result<()> {
        tx.execute(
            "INSERT INTO schema_version (version) VALUES (?1)",
            [version],
        )?;
        Ok(())
    }

    /// Check if a column exists in a table (for safe ALTER TABLE migrations).
    fn column_exists(conn: &rusqlite::Connection, table: &str, column: &str) -> bool {
        conn.prepare(&format!("SELECT {} FROM {} LIMIT 0", column, table))
            .is_ok()
    }

    /// Check if a table exists (for data-cleanup migrations against lazily-created tables).
    fn table_exists(conn: &rusqlite::Connection, table: &str) -> bool {
        conn.query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
            [table],
            |_| Ok(()),
        )
        .is_ok()
    }

    /// Rebuild a table by rewriting its CREATE TABLE SQL so CHECK constraints
    /// can accept a renamed discriminator without data loss.
    ///
    /// Optional `value_rewrites` map column values during the copy so data that
    /// only exists under the old CHECK enum (e.g. session_type='mako') can land
    /// in a table whose rewritten CHECK only accepts the canonical value
    /// ('hive'). Plain UPDATE-before-rebuild cannot do this: the old CHECK
    /// rejects the canonical spelling.
    fn rebuild_table_with_sql_rewrite_and_values(
        tx: &rusqlite::Transaction,
        table: &str,
        from: &[&str],
        to: &[&str],
        value_rewrites: &[(&str, &str, &str)],
    ) -> Result<()> {
        assert_eq!(from.len(), to.len());
        let create_sql: String = match tx.query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        ) {
            Ok(sql) => sql,
            Err(_) => return Ok(()),
        };

        let mut rewritten = create_sql.clone();
        for (old, new) in from.iter().zip(to.iter()) {
            rewritten = rewritten.replace(old, new);
        }
        if rewritten == create_sql {
            if value_rewrites.is_empty() {
                return Ok(());
            }

            // A current-schema database may legitimately re-enter an older
            // migration after a repaired or mixed-version schema_version row.
            // Do not rebuild an already-canonical table merely because this
            // migration *can* translate legacy values: dropping that table can
            // invalidate newer triggers which correctly reference it.  Skip
            // only after proving that every present rewrite column contains no
            // legacy value. Missing columns make that individual mapping
            // irrelevant; query failures on present columns remain fatal.
            let quoted_table = table.replace('"', "\"\"");
            let mut legacy_value_exists = false;
            for (column, from_value, _) in value_rewrites {
                let quoted_column = column.replace('"', "\"\"");
                let column_exists = tx
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2
                         )",
                        [table, *column],
                        |row| row.get::<_, bool>(0),
                    )
                    .with_context(|| format!("inspect columns for {table}"))?;
                if !column_exists {
                    continue;
                }
                let has_legacy_value = tx
                    .query_row(
                        &format!(
                            "SELECT EXISTS(SELECT 1 FROM \"{quoted_table}\" \
                             WHERE \"{quoted_column}\" = ?1 LIMIT 1)"
                        ),
                        [*from_value],
                        |row| row.get::<_, bool>(0),
                    )
                    .with_context(|| {
                        format!("inspect legacy value {from_value:?} in {table}.{column}")
                    })?;
                if has_legacy_value {
                    legacy_value_exists = true;
                    break;
                }
            }
            if !legacy_value_exists {
                return Ok(());
            }
        }

        let tmp = format!("{table}__hive_rewrite");
        tx.execute_batch(&format!("DROP TABLE IF EXISTS \"{tmp}\";"))?;
        let create_tmp = rewritten.replacen(table, &tmp, 1);
        tx.execute_batch(&create_tmp)
            .with_context(|| format!("create rewritten table {tmp}"))?;

        let has_rows = tx.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM \"{table}\" LIMIT 1)"),
            [],
            |row| row.get::<_, bool>(0),
        )?;
        // Some migration tests intentionally model only the table being
        // rewritten and omit its referenced parent tables. SQLite resolves
        // those references when an INSERT executes, even if its SELECT would
        // return no rows. Skipping a provably empty copy keeps those synthetic
        // schemas migratable without disabling or weakening production FKs.
        if has_rows {
            let insert_sql = if value_rewrites.is_empty() {
                format!("INSERT INTO \"{tmp}\" SELECT * FROM \"{table}\";")
            } else {
                let mut columns: Vec<String> = Vec::new();
                {
                    let mut stmt = tx
                        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
                        .with_context(|| format!("inspect columns for {table}"))?;
                    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
                    for row in rows {
                        columns.push(row?);
                    }
                }
                ensure!(
                    !columns.is_empty(),
                    "cannot rewrite values for empty table definition {table}"
                );
                let select_list = columns
                    .iter()
                    .map(|column| {
                        if let Some((_, from_value, to_value)) = value_rewrites
                            .iter()
                            .find(|(name, _, _)| *name == column.as_str())
                        {
                            format!(
                                "CASE WHEN \"{column}\" = '{from_value}' THEN '{to_value}' ELSE \"{column}\" END AS \"{column}\""
                            )
                        } else {
                            format!("\"{column}\"")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("INSERT INTO \"{tmp}\" SELECT {select_list} FROM \"{table}\";")
            };
            tx.execute_batch(&insert_sql)
                .with_context(|| format!("copy rows into {tmp}"))?;
        }

        let mut index_sqls: Vec<String> = Vec::new();
        {
            let mut stmt = tx.prepare(
                "SELECT sql FROM sqlite_master
                 WHERE type = 'index'
                   AND tbl_name = ?1
                   AND sql IS NOT NULL",
            )?;
            let rows = stmt.query_map([table], |row| row.get::<_, String>(0))?;
            for row in rows {
                index_sqls.push(row?);
            }
        }

        tx.execute_batch(&format!("DROP TABLE \"{table}\";"))?;
        tx.execute_batch(&format!("ALTER TABLE \"{tmp}\" RENAME TO \"{table}\";"))?;

        for sql in index_sqls {
            tx.execute_batch(&sql)
                .with_context(|| format!("recreate index for {table}: {sql}"))?;
        }
        Ok(())
    }

    fn checkpoint_wal_without_busy_readers(&self, phase: &str) -> Result<()> {
        let (busy, log_frames, checkpointed_frames) = self
            .conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .with_context(|| format!("{phase} Mako privacy WAL checkpoint"))?;
        ensure!(
            busy == 0,
            "{phase} Mako privacy WAL checkpoint was busy (log_frames={log_frames}, checkpointed_frames={checkpointed_frames})"
        );
        Ok(())
    }

    /// Return a privacy-migration connection to normal WAL locking and force
    /// one database access so SQLite actually releases the exclusive lock.
    /// Merely changing `locking_mode` updates the requested mode; the lock is
    /// retained until the connection performs a subsequent database access.
    fn restore_normal_locking_after_privacy_migration(&self) -> Result<()> {
        self.conn
            .pragma_update(None, "locking_mode", "NORMAL")
            .context("restoring normal SQLite locking after Mako privacy migration")?;
        self.conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version",
                [],
                |row| row.get::<_, i32>(0),
            )
            .context("releasing exclusive SQLite lock after Mako privacy migration")?;
        Ok(())
    }

    /// Run database migrations incrementally
    pub(crate) fn run_migrations(&self) -> Result<()> {
        // The steady-state path is read-only. Runtime stores open short-lived
        // connections frequently, so do not acquire the global SQLite writer
        // lock once this process has observed the target schema.
        let observed_version = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version",
                [],
                |row| row.get::<_, i32>(0),
            )
            .ok();
        if observed_version.is_some_and(|version| version >= SCHEMA_VERSION) {
            return Ok(());
        }

        // Migration 43 rewrites secret-bearing legacy journal pages. Schema
        // 44 is committed only after the post-transaction checkpoint/VACUUM
        // completes, so a crash at any point before physical erasure resumes
        // cleanup on the next open instead of treating redaction as finished.
        let privacy_cleanup_requested =
            observed_version.is_some_and(|version| version > 0 && version < 44);
        if privacy_cleanup_requested {
            self.conn
                .pragma_update(None, "secure_delete", "ON")
                .context("enabling secure deletion for Mako privacy migration")?;
            self.conn
                .pragma_update(None, "locking_mode", "EXCLUSIVE")
                .context("reserving exclusive access for Mako privacy migration")?;
        }

        // The HTTP process and the independently supervised Mako daemon can
        // open the same database at the same time. Acquire the SQLite write
        // reservation before reading the version so a waiter observes every
        // migration committed by the process that won the startup race.
        let tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .context("acquiring database migration lock")?;
        tx.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )
        .context("ensuring database schema version table")?;
        let current_version: i32 = tx
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version",
                [],
                |row| row.get::<_, i32>(0),
            )
            .context("reading database schema version under migration lock")?;
        debug!(
            "Database schema version: {} (target: {})",
            current_version, SCHEMA_VERSION
        );

        // A missing version table and a transient read blocked by another
        // process look identical during the optimistic preflight. Once the
        // immediate transaction reveals an existing pre-44 database, release
        // the shared migration reservation and restart under the privacy
        // migration's exclusive/secure-delete policy. Fresh version-0
        // databases never request exclusive locking, so concurrent first opens
        // serialize on the ordinary writer lock instead of deadlocking.
        if current_version > 0 && current_version < 44 && !privacy_cleanup_requested {
            tx.commit()?;
            self.conn
                .pragma_update(None, "secure_delete", "ON")
                .context("enabling secure deletion for Mako privacy migration")?;
            self.conn
                .pragma_update(None, "locking_mode", "EXCLUSIVE")
                .context("reserving exclusive access for Mako privacy migration")?;
            return self.run_migrations();
        }

        if current_version >= SCHEMA_VERSION {
            tx.commit()?;
            if privacy_cleanup_requested {
                self.restore_normal_locking_after_privacy_migration()?;
            }
            return Ok(());
        }
        // The optimistic version read above may have failed while another
        // process held SQLite's migration lock. The value read under this
        // transaction is authoritative for deciding whether physical cleanup
        // is required; never publish 44 merely because preflight assumed 0.
        let privacy_cleanup_required = current_version > 0 && current_version < 44;

        // Migration 1: Initial schema
        if current_version < 1 {
            info!("Running migration 1: Initial schema");
            tx.execute_batch(
                r#"
                -- Sessions table
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    model TEXT,
                    working_dir TEXT,
                    session_type TEXT NOT NULL DEFAULT 'code'
                        CHECK (session_type IN ('chat', 'code', 'hive')),
                    permission_mode TEXT NOT NULL DEFAULT 'autonomous'
                        CHECK (permission_mode IN ('supervised', 'autonomous'))
                );

                -- Messages table
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    tool_calls TEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );

                -- Index for faster message lookups
                CREATE INDEX IF NOT EXISTS idx_messages_session
                    ON messages(session_id);

                -- Index for session sorting
                CREATE INDEX IF NOT EXISTS idx_sessions_updated
                    ON sessions(updated_at DESC);

                -- User preferences
                CREATE TABLE IF NOT EXISTS user_preferences (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                "#,
            )?;
            self.set_schema_version_tx(&tx, 1)?;
        }

        // Migration 2: Add token_count to sessions
        if current_version < 2 {
            info!("Running migration 2: Add token_count to sessions");
            tx.execute_batch("ALTER TABLE sessions ADD COLUMN token_count INTEGER DEFAULT 0;")?;
            self.set_schema_version_tx(&tx, 2)?;
        }

        // Migration 3: Block UI state table for session restoration
        if current_version < 3 {
            info!("Running migration 3: Add block_ui_state table");
            tx.execute_batch(
                r#"
                -- Block UI state for session restoration
                -- Stores collapsed/expanded state and scroll position per block
                CREATE TABLE IF NOT EXISTS block_ui_state (
                    session_id TEXT NOT NULL,
                    block_id TEXT NOT NULL,
                    block_type TEXT NOT NULL,
                    collapsed INTEGER NOT NULL DEFAULT 1,
                    scroll_offset INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (session_id, block_id),
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );

                -- Index for fast lookup by session
                CREATE INDEX IF NOT EXISTS idx_block_ui_state_session
                    ON block_ui_state(session_id);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 3)?;
        }

        // Migration 4: Pinch support
        if current_version < 4 {
            info!("Running migration 4: Pinch support");
            tx.execute_batch(
                r#"
                -- Add parent_session_id to sessions for chain tracking
                ALTER TABLE sessions ADD COLUMN parent_session_id TEXT REFERENCES sessions(id);

                -- File activity tracking for importance scoring
                CREATE TABLE IF NOT EXISTS file_activity (
                    session_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    read_count INTEGER NOT NULL DEFAULT 0,
                    write_count INTEGER NOT NULL DEFAULT 0,
                    edit_count INTEGER NOT NULL DEFAULT 0,
                    last_accessed TEXT NOT NULL,
                    user_referenced INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (session_id, file_path),
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );

                -- Index for fast file activity lookups
                CREATE INDEX IF NOT EXISTS idx_file_activity_session
                    ON file_activity(session_id);

                -- Pinch metadata for tracking context transfers
                CREATE TABLE IF NOT EXISTS pinch_metadata (
                    id TEXT PRIMARY KEY,
                    source_session_id TEXT NOT NULL,
                    target_session_id TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    key_files TEXT NOT NULL,
                    user_preservation_hints TEXT,
                    user_direction TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (source_session_id) REFERENCES sessions(id),
                    FOREIGN KEY (target_session_id) REFERENCES sessions(id)
                );
                "#,
            )?;
            self.set_schema_version_tx(&tx, 4)?;
        }

        // Migration 5: Rename handoff_metadata to pinch_metadata
        if current_version < 5 {
            info!("Running migration 5: Rename to pinch_metadata");
            // Check if old table exists and rename it, or create new one
            let has_old_table: bool = tx.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='handoff_metadata'",
                [],
                |row| row.get(0),
            ).unwrap_or(0) > 0;

            if has_old_table {
                tx.execute_batch("ALTER TABLE handoff_metadata RENAME TO pinch_metadata;")?;
            } else {
                // Create fresh if neither exists
                tx.execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS pinch_metadata (
                        id TEXT PRIMARY KEY,
                        source_session_id TEXT NOT NULL,
                        target_session_id TEXT NOT NULL,
                        summary TEXT NOT NULL,
                        key_files TEXT NOT NULL,
                        user_preservation_hints TEXT,
                        user_direction TEXT,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY (source_session_id) REFERENCES sessions(id),
                        FOREIGN KEY (target_session_id) REFERENCES sessions(id)
                    );
                    "#,
                )?;
            }
            self.set_schema_version_tx(&tx, 5)?;
        }

        // Migration 6: Plans table for strict session-plan linkage
        if current_version < 6 {
            info!("Running migration 6: Plans table with session linkage");
            tx.execute_batch(
                r#"
                -- Plans table with strict 1:1 session linkage
                -- session_id UNIQUE enforces one plan per session
                -- ON DELETE CASCADE removes plan when session is deleted
                CREATE TABLE IF NOT EXISTS plans (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'in_progress',
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );

                -- Index for fast plan lookup by session
                CREATE INDEX IF NOT EXISTS idx_plans_session
                    ON plans(session_id);

                -- Index for listing plans by status
                CREATE INDEX IF NOT EXISTS idx_plans_status
                    ON plans(status);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 6)?;
        }

        // Migration 7: User hooks table
        if current_version < 7 {
            info!("Running migration 7: User hooks table");
            tx.execute_batch(
                r#"
                -- User-configurable hooks for tool execution
                -- hook_type: PreToolUse, PostToolUse, Notification, UserPromptSubmit
                -- tool_pattern: regex pattern to match tool names (e.g., "Write|Edit", "Bash", ".*")
                -- command: shell command to execute (receives JSON on stdin)
                CREATE TABLE IF NOT EXISTS user_hooks (
                    id TEXT PRIMARY KEY,
                    hook_type TEXT NOT NULL,
                    tool_pattern TEXT NOT NULL,
                    command TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                -- Index for fast lookup by hook type
                CREATE INDEX IF NOT EXISTS idx_user_hooks_type
                    ON user_hooks(hook_type);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 7)?;
        }

        // Migration 8: Agent state tracking for background execution
        if current_version < 8 {
            info!("Running migration 8: Agent state tracking");
            tx.execute_batch(
                r#"
                -- Add agent execution state to sessions
                -- agent_state: 'idle', 'streaming', 'tool_executing', 'awaiting_input', 'error'
                ALTER TABLE sessions ADD COLUMN agent_state TEXT NOT NULL DEFAULT 'idle';

                -- When the agent started processing (for monitoring)
                ALTER TABLE sessions ADD COLUMN agent_started_at TEXT;

                -- Last event time (for stale detection)
                ALTER TABLE sessions ADD COLUMN agent_last_event_at TEXT;

                -- Index for finding active sessions quickly
                CREATE INDEX IF NOT EXISTS idx_sessions_agent_state
                    ON sessions(agent_state) WHERE agent_state != 'idle';
                "#,
            )?;
            self.set_schema_version_tx(&tx, 8)?;
        }

        // Migration 9: Multi-tenant core tables (users, workspaces)
        if current_version < 9 {
            info!("Running migration 9: Multi-tenant core tables");
            tx.execute_batch(
                r#"
                -- Users table for multi-tenant SaaS
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    display_name TEXT,
                    avatar_url TEXT,
                    tailscale_user_id TEXT UNIQUE,
                    oauth_subject TEXT UNIQUE,
                    license_tier TEXT NOT NULL DEFAULT 'free',
                    license_expires_at TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_login_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
                CREATE INDEX IF NOT EXISTS idx_users_tailscale ON users(tailscale_user_id);

                -- Workspaces (team containers)
                CREATE TABLE IF NOT EXISTS workspaces (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    slug TEXT NOT NULL UNIQUE,
                    owner_id TEXT NOT NULL REFERENCES users(id),
                    settings TEXT DEFAULT '{}',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_workspaces_owner ON workspaces(owner_id);
                CREATE INDEX IF NOT EXISTS idx_workspaces_slug ON workspaces(slug);

                -- Workspace membership
                CREATE TABLE IF NOT EXISTS workspace_members (
                    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    role TEXT NOT NULL DEFAULT 'member',
                    joined_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (workspace_id, user_id)
                );

                CREATE INDEX IF NOT EXISTS idx_workspace_members_user ON workspace_members(user_id);

                -- Usage tracking for billing
                CREATE TABLE IF NOT EXISTS usage_tracking (
                    id TEXT PRIMARY KEY,
                    workspace_id TEXT REFERENCES workspaces(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL REFERENCES users(id),
                    resource_type TEXT NOT NULL,
                    resource_id TEXT,
                    quantity INTEGER NOT NULL DEFAULT 1,
                    metadata TEXT,
                    period_start TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_usage_workspace_period ON usage_tracking(workspace_id, period_start);
                CREATE INDEX IF NOT EXISTS idx_usage_user_period ON usage_tracking(user_id, period_start);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 9)?;
        }

        // Migration 10: Add user_id columns to existing tables
        if current_version < 10 {
            info!("Running migration 10: Add user_id to existing tables");
            tx.execute_batch(
                r#"
                -- Add user_id to sessions
                ALTER TABLE sessions ADD COLUMN user_id TEXT REFERENCES users(id);
                ALTER TABLE sessions ADD COLUMN workspace_id TEXT REFERENCES workspaces(id);

                -- Add user_id to user_preferences (nullable for backwards compat)
                ALTER TABLE user_preferences ADD COLUMN user_id TEXT REFERENCES users(id);

                -- Add user_id to user_hooks
                ALTER TABLE user_hooks ADD COLUMN user_id TEXT REFERENCES users(id);
                ALTER TABLE user_hooks ADD COLUMN workspace_id TEXT REFERENCES workspaces(id);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 10)?;
        }

        // Migration 11: Indexes for user-scoped queries
        if current_version < 11 {
            info!("Running migration 11: User-scoped indexes");
            tx.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
                CREATE INDEX IF NOT EXISTS idx_sessions_workspace ON sessions(workspace_id);
                CREATE INDEX IF NOT EXISTS idx_sessions_user_workspace ON sessions(user_id, workspace_id);
                CREATE INDEX IF NOT EXISTS idx_prefs_user ON user_preferences(user_id);
                CREATE INDEX IF NOT EXISTS idx_hooks_user ON user_hooks(user_id);
                CREATE INDEX IF NOT EXISTS idx_hooks_workspace ON user_hooks(workspace_id);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 11)?;
        }

        // Migration 12: Smart Codebase Memory System
        if current_version < 12 {
            info!("Running migration 12: Smart Codebase Memory System");
            self.conn.execute_batch(
                r#"
                -- Codebases: First-class codebase entity
                CREATE TABLE IF NOT EXISTS codebases (
                    id TEXT PRIMARY KEY,
                    path TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    indexed_at TEXT,
                    index_version INTEGER NOT NULL DEFAULT 0,
                    config TEXT DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS idx_codebases_path ON codebases(path);

                -- Codebase index: Semantic code symbol index
                CREATE TABLE IF NOT EXISTS codebase_index (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    codebase_id TEXT NOT NULL REFERENCES codebases(id) ON DELETE CASCADE,
                    symbol_type TEXT NOT NULL,
                    symbol_name TEXT NOT NULL,
                    symbol_path TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    line_start INTEGER NOT NULL,
                    line_end INTEGER NOT NULL,
                    signature TEXT,
                    summary TEXT,
                    embedding BLOB,
                    calls TEXT DEFAULT '[]',
                    indexed_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_codebase_index_codebase ON codebase_index(codebase_id);
                CREATE INDEX IF NOT EXISTS idx_codebase_index_symbol ON codebase_index(symbol_name);
                CREATE INDEX IF NOT EXISTS idx_codebase_index_file ON codebase_index(file_path);
                CREATE INDEX IF NOT EXISTS idx_codebase_index_type ON codebase_index(symbol_type);

                -- Codebase insights: Accumulated knowledge from sessions
                CREATE TABLE IF NOT EXISTS codebase_insights (
                    id TEXT PRIMARY KEY,
                    codebase_id TEXT NOT NULL REFERENCES codebases(id) ON DELETE CASCADE,
                    insight_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    embedding BLOB,
                    confidence REAL NOT NULL DEFAULT 0.5,
                    source_session_id TEXT REFERENCES sessions(id) ON DELETE SET NULL,
                    access_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_accessed_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_codebase_insights_codebase ON codebase_insights(codebase_id);
                CREATE INDEX IF NOT EXISTS idx_codebase_insights_type ON codebase_insights(insight_type);
                CREATE INDEX IF NOT EXISTS idx_codebase_insights_confidence ON codebase_insights(confidence DESC);

                -- Session memories: Session-level learnings (may promote to insights)
                CREATE TABLE IF NOT EXISTS session_memories (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                    memory_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    promoted_to_insight_id TEXT REFERENCES codebase_insights(id) ON DELETE SET NULL
                );

                CREATE INDEX IF NOT EXISTS idx_session_memories_session ON session_memories(session_id);
                CREATE INDEX IF NOT EXISTS idx_session_memories_type ON session_memories(memory_type);

                -- Link sessions to codebases
                ALTER TABLE sessions ADD COLUMN codebase_id TEXT REFERENCES codebases(id);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 12)?;
        }

        // Migration 13: Push notification subscriptions
        if current_version < 13 {
            info!("Running migration 13: Push notification subscriptions");
            tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS push_subscriptions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    endpoint TEXT NOT NULL,
                    p256dh TEXT NOT NULL,
                    auth TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TEXT,
                    UNIQUE(endpoint)
                );

                CREATE INDEX IF NOT EXISTS idx_push_subscriptions_user
                    ON push_subscriptions(user_id);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 13)?;
        }

        // Migration 14: Push delivery observability + subscription health fields
        if current_version < 14 {
            info!("Running migration 14: Push delivery observability");
            tx.execute_batch(
                r#"
                ALTER TABLE push_subscriptions ADD COLUMN last_success_at TEXT;
                ALTER TABLE push_subscriptions ADD COLUMN last_failure_at TEXT;
                ALTER TABLE push_subscriptions ADD COLUMN last_failure_reason TEXT;
                ALTER TABLE push_subscriptions ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0;

                CREATE TABLE IF NOT EXISTS push_delivery_attempts (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    session_id TEXT,
                    endpoint_hash TEXT NOT NULL,
                    provider_host TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    http_status INTEGER,
                    error_message TEXT,
                    latency_ms INTEGER,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_push_delivery_attempts_user_created
                    ON push_delivery_attempts(user_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_push_delivery_attempts_outcome_created
                    ON push_delivery_attempts(outcome, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_push_delivery_attempts_session_created
                    ON push_delivery_attempts(session_id, created_at DESC);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 14)?;
        }

        // Migration 15: Persist session work mode
        if current_version < 15 {
            info!("Running migration 15: Session work mode");
            tx.execute_batch(
                r#"
                ALTER TABLE sessions ADD COLUMN work_mode TEXT NOT NULL DEFAULT 'build'
                    CHECK (work_mode IN ('build', 'plan'));

                CREATE INDEX IF NOT EXISTS idx_sessions_work_mode
                    ON sessions(work_mode);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 15)?;
        }

        // Migration 16: Optional target git branch on sessions
        if current_version < 16 {
            info!("Running migration 16: Session target branch");
            tx.execute_batch(
                r#"
                ALTER TABLE sessions ADD COLUMN target_branch TEXT;

                CREATE INDEX IF NOT EXISTS idx_sessions_target_branch
                    ON sessions(target_branch);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 16)?;
        }

        // Migration 17: Context ledger + continuation contract persistence
        if current_version < 17 {
            info!("Running migration 17: Context continuation state");
            tx.execute_batch(
                r#"
                ALTER TABLE sessions ADD COLUMN context_ledger_json TEXT;
                ALTER TABLE sessions ADD COLUMN continuation_json TEXT;
                "#,
            )?;
            self.set_schema_version_tx(&tx, 17)?;
        }

        // Migration 18: Typed interrupted-turn recovery state
        if current_version < 18 {
            info!("Running migration 18: Session recovery state");
            tx.execute_batch(
                r#"
                ALTER TABLE sessions ADD COLUMN recovery_json TEXT;
                "#,
            )?;
            self.set_schema_version_tx(&tx, 18)?;
        }

        // Migration 19: Structured runtime traces
        if current_version < 19 {
            info!("Running migration 19: Runtime traces");
            tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS runtime_traces (
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
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    UNIQUE(session_id, sequence)
                );

                CREATE INDEX IF NOT EXISTS idx_runtime_traces_session_sequence
                    ON runtime_traces(session_id, sequence);

                CREATE INDEX IF NOT EXISTS idx_runtime_traces_session_run
                    ON runtime_traces(session_id, run_id, sequence);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 19)?;
        }

        // Migration 20: Autonomous tasks for Mako agent coordination
        if current_version < 20 {
            info!("Running migration 20: Autonomous tasks");
            tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS autonomous_tasks (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    owner TEXT,
                    blocked_by TEXT DEFAULT '[]',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    completed_at TEXT,
                    result TEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_autonomous_tasks_session
                    ON autonomous_tasks(session_id);
                CREATE INDEX IF NOT EXISTS idx_autonomous_tasks_status
                    ON autonomous_tasks(session_id, status);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 20)?;
        }

        // Migration 21: Research reports
        if current_version < 21 {
            info!("Running migration 21: Reports");
            tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS reports (
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

                CREATE INDEX IF NOT EXISTS idx_reports_session
                    ON reports(session_id);
                CREATE INDEX IF NOT EXISTS idx_reports_project
                    ON reports(project_dir);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 21)?;
        }

        // Migration 22: Explicit workspace context on sessions
        if current_version < 22 {
            info!("Running migration 22: Session workspace context");
            if !Self::column_exists(&tx, "sessions", "project_dir") {
                tx.execute_batch("ALTER TABLE sessions ADD COLUMN project_dir TEXT;")?;
            }
            if !Self::column_exists(&tx, "sessions", "workspace_mode") {
                tx.execute_batch(
                    r#"ALTER TABLE sessions ADD COLUMN workspace_mode TEXT NOT NULL DEFAULT 'neutral'
                        CHECK (workspace_mode IN ('neutral', 'selected', 'created'));"#,
                )?;
            }
            tx.execute_batch(
                r#"

                UPDATE sessions
                   SET project_dir = COALESCE(project_dir, working_dir),
                       workspace_mode = CASE
                           WHEN COALESCE(project_dir, working_dir) IS NULL THEN 'neutral'
                           ELSE 'selected'
                       END;

                CREATE INDEX IF NOT EXISTS idx_sessions_project_dir
                    ON sessions(project_dir);
                CREATE INDEX IF NOT EXISTS idx_sessions_workspace_mode
                    ON sessions(workspace_mode);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 22)?;
        }

        // Migration 23: First-class delegated run persistence
        if current_version < 23 {
            info!("Running migration 23: Delegated runs");
            tx.execute_batch(
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
            )?;
            self.set_schema_version_tx(&tx, 23)?;
        }

        // Migration 24: First-class session types
        if current_version < 24 {
            info!("Running migration 24: Session types");
            if current_version > 0 && !Self::column_exists(&tx, "sessions", "session_type") {
                tx.execute_batch(
                    r#"
                    ALTER TABLE sessions ADD COLUMN session_type TEXT NOT NULL DEFAULT 'code'
                        CHECK (session_type IN ('chat', 'code', 'hive'));
                    "#,
                )?;
            }
            tx.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_sessions_session_type
                    ON sessions(session_type);
                "#,
            )?;
            self.set_schema_version_tx(&tx, 24)?;
        }

        // Migration 25: Autonomous tasks + reports tables (datetime defaults)
        if current_version < 25 {
            info!("Running migration 25: autonomous_tasks + reports tables");
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS autonomous_tasks (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    owner TEXT,
                    blocked_by TEXT DEFAULT '[]',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    completed_at TEXT,
                    result TEXT,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_autonomous_tasks_session ON autonomous_tasks(session_id);
                CREATE INDEX IF NOT EXISTS idx_autonomous_tasks_status ON autonomous_tasks(session_id, status);

                CREATE TABLE IF NOT EXISTS reports (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    project_dir TEXT,
                    content TEXT NOT NULL,
                    summary TEXT NOT NULL DEFAULT '',
                    tags TEXT NOT NULL DEFAULT '[]',
                    sources TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE INDEX IF NOT EXISTS idx_reports_session ON reports(session_id);
                CREATE INDEX IF NOT EXISTS idx_reports_project ON reports(project_dir);",
            )
            .context("Migration 25: autonomous_tasks + reports tables")?;
            self.set_schema_version_tx(&tx, 25)?;
        }

        // Migration 26: APNs device tokens for iOS push notifications
        if current_version < 26 {
            info!("Running migration 26: APNs device tokens");
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS apns_devices (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    device_token TEXT NOT NULL UNIQUE,
                    bundle_id TEXT NOT NULL DEFAULT 'io.krusty.mobile',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    last_used_at TEXT,
                    last_success_at TEXT,
                    last_failure_at TEXT,
                    last_failure_reason TEXT,
                    failure_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_apns_devices_user ON apns_devices(user_id);",
            )
            .context("Migration 26: APNs device tokens")?;
            self.set_schema_version_tx(&tx, 26)?;
        }

        // Migration 27: Persisted Mako runtime state
        if current_version < 27 {
            info!("Running migration 27: Mako runtime state");
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS mako_runtime_state (
                    session_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL
                        CHECK (status IN ('idle', 'running', 'sleeping', 'awaiting_input', 'paused', 'error', 'cancelled')),
                    next_wake_at TEXT,
                    sleep_reason TEXT,
                    last_error TEXT,
                    current_run_id TEXT,
                    last_wake_reason TEXT,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_mako_runtime_state_status
                    ON mako_runtime_state(status);
                CREATE INDEX IF NOT EXISTS idx_mako_runtime_state_next_wake
                    ON mako_runtime_state(next_wake_at);",
            )
            .context("Migration 27: Mako runtime state")?;
            self.set_schema_version_tx(&tx, 27)?;
        }

        // Migration 28: Persisted Mako run priority
        if current_version < 28 {
            info!("Running migration 28: Mako run priority");
            if !Self::column_exists(&tx, "mako_runtime_state", "priority") {
                tx.execute_batch(
                    "ALTER TABLE mako_runtime_state
                     ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal';",
                )?;
            }
            self.set_schema_version_tx(&tx, 28)?;
        }

        // Migration 29: Persisted Mako crew assignment
        if current_version < 29 {
            info!("Running migration 29: Mako crew assignment");
            if !Self::column_exists(&tx, "mako_runtime_state", "crew_slug") {
                tx.execute_batch(
                    "ALTER TABLE mako_runtime_state
                     ADD COLUMN crew_slug TEXT;",
                )?;
            }
            self.set_schema_version_tx(&tx, 29)?;
        }

        // Migration 30: Persisted Mako attention item state
        if current_version < 30 {
            info!("Running migration 30: Mako attention state");
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS mako_attention_state (
                    user_scope TEXT NOT NULL DEFAULT '',
                    item_id TEXT NOT NULL,
                    read INTEGER NOT NULL DEFAULT 0,
                    cleared INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (user_scope, item_id)
                );
                CREATE INDEX IF NOT EXISTS idx_mako_attention_state_user_scope
                    ON mako_attention_state(user_scope);",
            )
            .context("Migration 30: Mako attention state")?;
            self.set_schema_version_tx(&tx, 30)?;
        }

        // Migration 31: Persist session permission mode
        if current_version < 31 {
            info!("Running migration 31: Session permission mode");
            if !Self::column_exists(&tx, "sessions", "permission_mode") {
                tx.execute_batch(
                    "ALTER TABLE sessions
                     ADD COLUMN permission_mode TEXT NOT NULL DEFAULT 'autonomous'
                     CHECK (permission_mode IN ('supervised', 'autonomous'));",
                )?;
            }
            self.set_schema_version_tx(&tx, 31)?;
        }

        // Migration 32: Compaction checkpoints and transcript segments
        if current_version < 32 {
            info!("Running migration 32: Compaction checkpoints and segments");
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS compaction_checkpoints (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    prompt_index_at_compaction INTEGER NOT NULL,
                    pre_compact_message_ids_json TEXT NOT NULL,
                    compacted_history_json TEXT NOT NULL,
                    original_user_info TEXT,
                    reread_file_paths_json TEXT NOT NULL DEFAULT '[]',
                    schema_version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_compaction_checkpoints_session
                    ON compaction_checkpoints(session_id);
                CREATE TABLE IF NOT EXISTS compaction_segments (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    checkpoint_id TEXT NOT NULL,
                    message_id_start INTEGER NOT NULL,
                    message_id_end INTEGER NOT NULL,
                    segment_markdown TEXT NOT NULL,
                    token_estimate INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (checkpoint_id) REFERENCES compaction_checkpoints(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_compaction_segments_session
                    ON compaction_segments(session_id);
                CREATE INDEX IF NOT EXISTS idx_compaction_segments_checkpoint
                    ON compaction_segments(checkpoint_id);",
            )
            .context("Migration 32: Compaction checkpoints and segments")?;
            self.set_schema_version_tx(&tx, 32)?;
        }

        // Migration 33: Remove legacy compaction memory leaks and duplicate checkpoint history.
        if current_version < 33 {
            info!("Running migration 33: Compaction memory cleanup");
            if Self::table_exists(&tx, "agent_memories") {
                tx.execute(
                    "DELETE FROM agent_memories
                     WHERE memory_type = 'project'
                       AND title LIKE 'Compaction flush #%';",
                    [],
                )
                .context("Migration 33: delete legacy compaction flush memories")?;
            }
            tx.execute(
                "UPDATE compaction_checkpoints
                 SET compacted_history_json = '[]'
                 WHERE compacted_history_json <> '[]';",
                [],
            )
            .context("Migration 33: redact duplicate checkpoint history")?;
            self.set_schema_version_tx(&tx, 33)?;
        }

        // Migration 34: Queryable provider-call classification.
        if current_version < 34 {
            info!("Running migration 34: Provider-call trace classification");
            if Self::table_exists(&tx, "runtime_traces") {
                if !Self::column_exists(&tx, "runtime_traces", "call_kind") {
                    tx.execute_batch("ALTER TABLE runtime_traces ADD COLUMN call_kind TEXT;")?;
                }
                if !Self::column_exists(&tx, "runtime_traces", "operation") {
                    tx.execute_batch("ALTER TABLE runtime_traces ADD COLUMN operation TEXT;")?;
                }
                tx.execute_batch(
                    "UPDATE runtime_traces
                     SET call_kind = json_extract(payload_json, '$.call_kind')
                     WHERE call_kind IS NULL
                       AND event_type = 'provider_call';
                     UPDATE runtime_traces
                     SET operation = json_extract(payload_json, '$.operation')
                     WHERE operation IS NULL
                       AND event_type = 'provider_call';
                     CREATE INDEX IF NOT EXISTS idx_runtime_traces_provider_operation
                       ON runtime_traces(session_id, call_kind, operation, sequence);",
                )
                .context("Migration 34: classify provider-call traces")?;
            }
            self.set_schema_version_tx(&tx, 34)?;
        }

        // Migration 35: Database-owned, revisioned Mako identity profiles.
        if current_version < 35 {
            info!("Running migration 35: Mako identity profiles");
            tx.execute_batch(
                r#"
                CREATE TABLE mako_profiles (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE UNIQUE INDEX idx_mako_profiles_user
                    ON mako_profiles(user_id) WHERE user_id IS NOT NULL;

                CREATE TABLE mako_profile_documents (
                    profile_id TEXT NOT NULL,
                    kind TEXT NOT NULL
                        CHECK (kind IN ('soul', 'identity', 'user', 'heartbeat', 'channels')),
                    content TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (profile_id, kind),
                    FOREIGN KEY (profile_id) REFERENCES mako_profiles(id) ON DELETE CASCADE
                );

                CREATE TABLE mako_crew_profiles (
                    profile_id TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (profile_id, slug),
                    FOREIGN KEY (profile_id) REFERENCES mako_profiles(id) ON DELETE CASCADE
                );

                CREATE TABLE mako_crew_documents (
                    profile_id TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK (kind IN ('identity', 'soul')),
                    content TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (profile_id, slug, kind),
                    FOREIGN KEY (profile_id, slug)
                        REFERENCES mako_crew_profiles(profile_id, slug) ON DELETE CASCADE
                );
                "#,
            )
            .context("Migration 35: Mako identity profiles")?;
            self.set_schema_version_tx(&tx, 35)?;
        }

        // Migration 36: Durable Mako controllers, schedules, runs, leases, and event journal.
        if current_version < 36 {
            info!("Running migration 36: Durable Mako scheduler");
            tx.execute_batch(
                r#"
                CREATE TABLE mako_controllers (
                    id TEXT PRIMARY KEY,
                    scope_key TEXT NOT NULL UNIQUE,
                    user_id TEXT,
                    session_id TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'disabled')),
                    timezone TEXT NOT NULL,
                    max_concurrent_runs INTEGER NOT NULL CHECK (max_concurrent_runs > 0),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );
                CREATE INDEX idx_mako_controllers_user ON mako_controllers(user_id);
                CREATE INDEX idx_mako_controllers_status ON mako_controllers(status);

                CREATE TABLE mako_schedules (
                    id TEXT PRIMARY KEY,
                    controller_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    objective TEXT NOT NULL,
                    recurrence_kind TEXT NOT NULL,
                    recurrence_json TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    gap_policy TEXT NOT NULL CHECK (gap_policy IN ('shift_forward', 'skip')),
                    fold_policy TEXT NOT NULL CHECK (fold_policy IN ('first', 'second')),
                    next_fire_at TEXT,
                    last_scheduled_for TEXT,
                    status TEXT NOT NULL
                        CHECK (status IN ('enabled', 'paused', 'completed', 'cancelled')),
                    priority INTEGER NOT NULL DEFAULT 0,
                    project_dir TEXT,
                    model TEXT,
                    model_key_json TEXT,
                    model_catalog_revision TEXT,
                    crew_slug TEXT,
                    misfire_policy TEXT NOT NULL
                        CHECK (misfire_policy IN ('fire_once', 'skip', 'catch_up')),
                    misfire_grace_secs INTEGER NOT NULL CHECK (misfire_grace_secs >= 0),
                    catch_up_limit INTEGER NOT NULL CHECK (catch_up_limit >= 0),
                    overlap_policy TEXT NOT NULL CHECK (overlap_policy IN ('skip', 'queue_one', 'allow')),
                    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
                    retry_base_secs INTEGER NOT NULL CHECK (retry_base_secs >= 0),
                    retry_max_secs INTEGER NOT NULL CHECK (retry_max_secs >= 0),
                    retry_jitter TEXT NOT NULL CHECK (retry_jitter IN ('none', 'full')),
                    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (controller_id) REFERENCES mako_controllers(id) ON DELETE CASCADE
                );
                CREATE INDEX idx_mako_schedules_due
                    ON mako_schedules(status, next_fire_at);
                CREATE INDEX idx_mako_schedules_controller
                    ON mako_schedules(controller_id, status);

                CREATE TABLE mako_schedule_occurrences (
                    id TEXT PRIMARY KEY,
                    schedule_id TEXT NOT NULL,
                    scheduled_for TEXT NOT NULL,
                    run_id TEXT,
                    status TEXT NOT NULL
                        CHECK (status IN ('pending', 'queued', 'skipped', 'coalesced', 'running', 'succeeded', 'failed', 'cancelled')),
                    decision_reason TEXT,
                    coalesced_count INTEGER NOT NULL DEFAULT 0 CHECK (coalesced_count >= 0),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (schedule_id, scheduled_for),
                    FOREIGN KEY (schedule_id) REFERENCES mako_schedules(id) ON DELETE CASCADE
                );

                CREATE TABLE mako_runs (
                    id TEXT PRIMARY KEY,
                    controller_id TEXT NOT NULL,
                    session_id TEXT,
                    schedule_id TEXT,
                    occurrence_id TEXT,
                    kind TEXT NOT NULL
                        CHECK (kind IN ('dispatch', 'scheduled', 'controller_child', 'legacy_resume')),
                    objective TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK (status IN ('queued', 'leased', 'running', 'sleeping', 'retry_wait', 'awaiting_input', 'recovery_required', 'succeeded', 'failed', 'cancelled', 'dead_letter')),
                    priority INTEGER NOT NULL DEFAULT 0,
                    concurrency_key TEXT,
                    scheduled_for TEXT,
                    available_at TEXT NOT NULL,
                    wake_at TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
                    lease_owner TEXT,
                    lease_token TEXT,
                    lease_epoch INTEGER CHECK (lease_epoch IS NULL OR lease_epoch >= 0),
                    lease_expires_at TEXT,
                    heartbeat_at TEXT,
                    last_stop_reason TEXT,
                    last_error TEXT,
                    outcome_json TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (controller_id) REFERENCES mako_controllers(id) ON DELETE CASCADE,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE SET NULL,
                    FOREIGN KEY (schedule_id) REFERENCES mako_schedules(id) ON DELETE SET NULL,
                    FOREIGN KEY (occurrence_id) REFERENCES mako_schedule_occurrences(id) ON DELETE SET NULL
                );
                CREATE INDEX idx_mako_runs_claim
                    ON mako_runs(status, available_at, priority DESC, created_at);
                CREATE INDEX idx_mako_runs_controller_status
                    ON mako_runs(controller_id, status);
                CREATE INDEX idx_mako_runs_concurrency
                    ON mako_runs(concurrency_key, status) WHERE concurrency_key IS NOT NULL;
                CREATE INDEX idx_mako_runs_lease_expiry
                    ON mako_runs(lease_expires_at) WHERE lease_expires_at IS NOT NULL;
                CREATE INDEX idx_mako_runs_session ON mako_runs(session_id);

                CREATE TABLE mako_run_attempts (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
                    worker_id TEXT NOT NULL,
                    lease_token TEXT NOT NULL,
                    lease_epoch INTEGER NOT NULL CHECK (lease_epoch >= 0),
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    outcome TEXT NOT NULL
                        CHECK (outcome IN ('leased', 'succeeded', 'failed', 'retry_scheduled', 'sleeping', 'awaiting_input', 'recovery_required', 'cancelled', 'abandoned', 'dead_letter')),
                    stop_reason TEXT,
                    error TEXT,
                    retry_at TEXT,
                    trace_sequence_start INTEGER,
                    trace_sequence_end INTEGER,
                    UNIQUE (run_id, attempt_no),
                    FOREIGN KEY (run_id) REFERENCES mako_runs(id) ON DELETE CASCADE
                );
                CREATE INDEX idx_mako_run_attempts_run
                    ON mako_run_attempts(run_id, attempt_no);

                CREATE TABLE mako_daemon_leases (
                    lease_name TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL CHECK (fencing_token >= 0),
                    acquired_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );

                CREATE TABLE mako_idempotency_keys (
                    scope_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    resource_id TEXT,
                    response_json TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    PRIMARY KEY (scope_key, operation, idempotency_key)
                );
                CREATE INDEX idx_mako_idempotency_expiry ON mako_idempotency_keys(expires_at);

                CREATE TABLE mako_controller_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    controller_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence > 0),
                    event_type TEXT NOT NULL,
                    run_id TEXT,
                    schedule_id TEXT,
                    dedupe_key TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (controller_id, sequence),
                    FOREIGN KEY (controller_id) REFERENCES mako_controllers(id) ON DELETE CASCADE,
                    FOREIGN KEY (run_id) REFERENCES mako_runs(id) ON DELETE SET NULL,
                    FOREIGN KEY (schedule_id) REFERENCES mako_schedules(id) ON DELETE SET NULL
                );
                CREATE UNIQUE INDEX idx_mako_controller_events_dedupe
                    ON mako_controller_events(controller_id, dedupe_key)
                    WHERE dedupe_key IS NOT NULL;
                CREATE INDEX idx_mako_controller_events_replay
                    ON mako_controller_events(controller_id, sequence);
                "#,
            )
            .context("Migration 36: Durable Mako scheduler")?;
            self.set_schema_version_tx(&tx, 36)?;
        }

        // Migration 37: Owned cross-session episodic recall with a bounded FTS index.
        if current_version < 37 {
            info!("Running migration 37: Mako episodic recall");
            tx.execute_batch(
                r#"
                CREATE TABLE conversation_episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
                    body TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    UNIQUE (session_id, source_message_id),
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (source_message_id) REFERENCES messages(id) ON DELETE CASCADE
                );
                CREATE INDEX idx_conversation_episodes_session_time
                    ON conversation_episodes(session_id, occurred_at DESC);
                CREATE INDEX idx_conversation_episodes_hash
                    ON conversation_episodes(content_hash);

                CREATE VIRTUAL TABLE conversation_episodes_fts USING fts5(
                    body,
                    content = 'conversation_episodes',
                    content_rowid = 'id',
                    tokenize = 'porter unicode61'
                );
                CREATE TRIGGER conversation_episodes_ai AFTER INSERT ON conversation_episodes BEGIN
                    INSERT INTO conversation_episodes_fts(rowid, body) VALUES (new.id, new.body);
                END;
                CREATE TRIGGER conversation_episodes_ad AFTER DELETE ON conversation_episodes BEGIN
                    INSERT INTO conversation_episodes_fts(conversation_episodes_fts, rowid, body)
                    VALUES ('delete', old.id, old.body);
                END;
                CREATE TRIGGER conversation_episodes_au AFTER UPDATE ON conversation_episodes BEGIN
                    INSERT INTO conversation_episodes_fts(conversation_episodes_fts, rowid, body)
                    VALUES ('delete', old.id, old.body);
                    INSERT INTO conversation_episodes_fts(rowid, body) VALUES (new.id, new.body);
                END;
                "#,
            )
            .context("Migration 37: Mako episodic recall")?;
            self.set_schema_version_tx(&tx, 37)?;
        }

        // Migration 38: Governed post-turn learning proposals and reviewer checkpoints.
        if current_version < 38 {
            info!("Running migration 38: Governed Mako learning");
            tx.execute_batch(
                r#"
                CREATE TABLE mako_learning_runs (
                    session_id TEXT NOT NULL,
                    through_message_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
                    model TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    PRIMARY KEY (session_id, through_message_id),
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (through_message_id) REFERENCES messages(id) ON DELETE CASCADE
                );

                CREATE TABLE mako_learning_candidates (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    project_dir TEXT,
                    canonical_key TEXT NOT NULL,
                    kind TEXT NOT NULL
                        CHECK (kind IN ('user_preference', 'user_correction', 'project_fact', 'procedure', 'relationship_context', 'forget')),
                    proposed_content TEXT NOT NULL,
                    evidence_session_id TEXT NOT NULL,
                    evidence_message_id INTEGER NOT NULL,
                    evidence_excerpt TEXT NOT NULL,
                    explicit INTEGER NOT NULL CHECK (explicit IN (0, 1)),
                    confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
                    sensitivity TEXT NOT NULL CHECK (sensitivity IN ('normal', 'sensitive', 'prohibited')),
                    status TEXT NOT NULL
                        CHECK (status IN ('pending', 'accepted', 'auto_accepted', 'rejected', 'tombstoned')),
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    reviewed_at TEXT,
                    UNIQUE (evidence_session_id, evidence_message_id, canonical_key),
                    FOREIGN KEY (evidence_session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (evidence_message_id) REFERENCES messages(id) ON DELETE CASCADE
                );
                CREATE INDEX idx_mako_learning_candidates_owner_status
                    ON mako_learning_candidates(user_id, status, created_at DESC);
                CREATE INDEX idx_mako_learning_candidates_project
                    ON mako_learning_candidates(project_dir, status);
                "#,
            )
            .context("Migration 38: Governed Mako learning")?;
            self.set_schema_version_tx(&tx, 38)?;
        }

        // Migration 39: Canonical, provenance-aware memory and derived knowledge snapshots.
        if current_version < 39 {
            info!("Running migration 39: Canonical Mako memory");

            // Memory storage historically initialized lazily. Creating the
            // legacy columns first keeps this migration valid for databases
            // that have never opened the memory API.
            tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS agent_memories (
                    id TEXT PRIMARY KEY,
                    memory_type TEXT NOT NULL
                        CHECK (memory_type IN ('user', 'feedback', 'project', 'reference')),
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    project_dir TEXT,
                    user_id TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    canonical_key TEXT,
                    namespace TEXT NOT NULL DEFAULT 'shared'
                        CHECK (namespace IN ('shared', 'hive', 'crew')),
                    namespace_id TEXT,
                    status TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'superseded', 'deleted')),
                    source TEXT NOT NULL DEFAULT 'legacy'
                        CHECK (source IN ('legacy', 'user', 'agent', 'tool', 'import', 'compaction', 'system')),
                    source_session_id TEXT,
                    source_message_id TEXT,
                    confidence REAL NOT NULL DEFAULT 1.0
                        CHECK (confidence >= 0.0 AND confidence <= 1.0),
                    sensitivity TEXT NOT NULL DEFAULT 'normal'
                        CHECK (sensitivity IN ('normal', 'sensitive', 'secret')),
                    pinned INTEGER NOT NULL DEFAULT 0 CHECK (pinned IN (0, 1)),
                    supersedes_id TEXT REFERENCES agent_memories(id),
                    last_accessed_at TEXT,
                    access_count INTEGER NOT NULL DEFAULT 0 CHECK (access_count >= 0)
                );
                "#,
            )
            .context("Migration 39: ensure memory base table")?;

            if !Self::column_exists(&tx, "agent_memories", "canonical_key") {
                tx.execute_batch("ALTER TABLE agent_memories ADD COLUMN canonical_key TEXT;")?;
            }
            if !Self::column_exists(&tx, "agent_memories", "namespace") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN namespace TEXT NOT NULL DEFAULT 'shared' CHECK (namespace IN ('shared', 'hive', 'crew'));",
                )?;
            }
            if !Self::column_exists(&tx, "agent_memories", "namespace_id") {
                tx.execute_batch("ALTER TABLE agent_memories ADD COLUMN namespace_id TEXT;")?;
            }
            if !Self::column_exists(&tx, "agent_memories", "status") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'superseded', 'deleted'));",
                )?;
            }
            if !Self::column_exists(&tx, "agent_memories", "source") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN source TEXT NOT NULL DEFAULT 'legacy' CHECK (source IN ('legacy', 'user', 'agent', 'tool', 'import', 'compaction', 'system'));",
                )?;
            }
            if !Self::column_exists(&tx, "agent_memories", "source_session_id") {
                tx.execute_batch("ALTER TABLE agent_memories ADD COLUMN source_session_id TEXT;")?;
            }
            if !Self::column_exists(&tx, "agent_memories", "source_message_id") {
                tx.execute_batch("ALTER TABLE agent_memories ADD COLUMN source_message_id TEXT;")?;
            }
            if !Self::column_exists(&tx, "agent_memories", "confidence") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0);",
                )?;
            }
            if !Self::column_exists(&tx, "agent_memories", "sensitivity") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN sensitivity TEXT NOT NULL DEFAULT 'normal' CHECK (sensitivity IN ('normal', 'sensitive', 'secret'));",
                )?;
            }
            if !Self::column_exists(&tx, "agent_memories", "pinned") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0 CHECK (pinned IN (0, 1));",
                )?;
            }
            if !Self::column_exists(&tx, "agent_memories", "supersedes_id") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN supersedes_id TEXT REFERENCES agent_memories(id);",
                )?;
            }
            if !Self::column_exists(&tx, "agent_memories", "last_accessed_at") {
                tx.execute_batch("ALTER TABLE agent_memories ADD COLUMN last_accessed_at TEXT;")?;
            }
            if !Self::column_exists(&tx, "agent_memories", "access_count") {
                tx.execute_batch(
                    "ALTER TABLE agent_memories ADD COLUMN access_count INTEGER NOT NULL DEFAULT 0 CHECK (access_count >= 0);",
                )?;
            }

            tx.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_agent_memories_type
                    ON agent_memories(memory_type);
                CREATE INDEX IF NOT EXISTS idx_agent_memories_project
                    ON agent_memories(project_dir);
                CREATE INDEX IF NOT EXISTS idx_agent_memories_user
                    ON agent_memories(user_id);
                CREATE INDEX idx_agent_memories_active_scope
                    ON agent_memories(status, user_id, project_dir, namespace, namespace_id);
                CREATE INDEX idx_agent_memories_canonical_key
                    ON agent_memories(canonical_key, status);
                CREATE UNIQUE INDEX idx_agent_memories_active_canonical
                    ON agent_memories(
                        COALESCE(user_id, ''), COALESCE(project_dir, ''), namespace,
                        COALESCE(namespace_id, ''), canonical_key
                    )
                    WHERE status = 'active' AND canonical_key IS NOT NULL;

                CREATE TABLE agent_memory_revisions (
                    id TEXT PRIMARY KEY,
                    memory_id TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision > 0),
                    event TEXT NOT NULL
                        CHECK (event IN ('created', 'updated', 'superseded', 'deleted')),
                    snapshot_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE (memory_id, revision),
                    FOREIGN KEY (memory_id) REFERENCES agent_memories(id) ON DELETE CASCADE
                );
                CREATE INDEX idx_agent_memory_revisions_memory
                    ON agent_memory_revisions(memory_id, revision);

                INSERT OR IGNORE INTO agent_memory_revisions
                    (id, memory_id, revision, event, snapshot_json, created_at)
                SELECT
                    'migration39:' || id,
                    id,
                    1,
                    'created',
                    json_object(
                        'id', id,
                        'memory_type', memory_type,
                        'title', title,
                        'content', content,
                        'project_dir', project_dir,
                        'user_id', user_id,
                        'created_at', created_at,
                        'updated_at', updated_at,
                        'canonical_key', canonical_key,
                        'namespace', namespace,
                        'namespace_id', namespace_id,
                        'status', status,
                        'source', source,
                        'source_session_id', source_session_id,
                        'source_message_id', source_message_id,
                        'confidence', confidence,
                        'sensitivity', sensitivity,
                        'pinned', CASE pinned WHEN 0 THEN json('false') ELSE json('true') END,
                        'supersedes_id', supersedes_id,
                        'last_accessed_at', last_accessed_at,
                        'access_count', access_count
                    ),
                    created_at
                FROM agent_memories;

                CREATE TABLE knowledge_snapshots (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    project_dir TEXT,
                    user_id TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE UNIQUE INDEX idx_knowledge_snapshots_exact_scope
                    ON knowledge_snapshots(COALESCE(project_dir, ''), COALESCE(user_id, ''));

                INSERT OR IGNORE INTO knowledge_snapshots
                    (id, title, content, project_dir, user_id, created_at, updated_at)
                SELECT id, title, content, project_dir, user_id, created_at, updated_at
                FROM agent_memories
                WHERE memory_type = 'project' AND title = 'Current Snapshot';
                DELETE FROM agent_memories
                WHERE memory_type = 'project' AND title = 'Current Snapshot';
                "#,
            )
            .context("Migration 39: canonical memory and knowledge tables")?;

            self.set_schema_version_tx(&tx, 39)?;
        }

        // Migration 40: Make every durable Mako run transition produce an
        // authoritative replay event in the same SQLite transaction. Runtime
        // publishers may crash after commit; subscribers can still recover the
        // event from this journal.
        if current_version < 40 {
            info!("Running migration 40: Atomic Mako run transition journal");
            // Some legacy/specialized databases advance the shared schema
            // version without materializing the optional Mako tables. Keep
            // their upgrade valid; any database with the Mako contract gets
            // the trigger atomically with the version bump.
            if Self::table_exists(&tx, "mako_runs")
                && Self::table_exists(&tx, "mako_controller_events")
            {
                tx.execute_batch(
                    r#"
                CREATE TRIGGER mako_runs_transition_event
                AFTER UPDATE OF status ON mako_runs
                WHEN OLD.status <> NEW.status
                BEGIN
                    INSERT OR IGNORE INTO mako_controller_events (
                        controller_id, sequence, event_type, run_id, schedule_id,
                        dedupe_key, payload_json, created_at
                    )
                    SELECT
                        NEW.controller_id,
                        COALESCE(MAX(sequence), 0) + 1,
                        CASE
                            WHEN NEW.status = 'queued' AND OLD.status = 'leased'
                                THEN 'run_lease_requeued'
                            WHEN NEW.status = 'queued' THEN 'run_requeued'
                            WHEN NEW.status = 'leased' THEN 'run_leased'
                            WHEN NEW.status = 'running' THEN 'run_started'
                            WHEN NEW.status = 'sleeping' THEN 'run_sleeping'
                            WHEN NEW.status = 'retry_wait' THEN 'run_retry_scheduled'
                            WHEN NEW.status = 'awaiting_input' THEN 'run_awaiting_input'
                            WHEN NEW.status = 'recovery_required' THEN 'recovery_required'
                            WHEN NEW.status = 'succeeded' THEN 'run_completed'
                            WHEN NEW.status = 'failed' THEN 'run_failed'
                            WHEN NEW.status = 'cancelled' THEN 'run_cancelled'
                            WHEN NEW.status = 'dead_letter' THEN 'run_dead_lettered'
                            ELSE 'run_state_changed'
                        END,
                        NEW.id,
                        NEW.schedule_id,
                        'transition:' || NEW.id || ':' || NEW.attempt_count || ':' || NEW.status,
                        json_object(
                            'run_id', NEW.id,
                            'status', NEW.status,
                            'previous_status', OLD.status,
                            'attempt', NEW.attempt_count,
                            'stop_reason', NEW.last_stop_reason,
                            'error', NEW.last_error
                        ),
                        NEW.updated_at
                    FROM mako_controller_events
                    WHERE controller_id = NEW.controller_id;
                END;
                "#,
                )
                .context("Migration 40: atomic Mako run transition journal")?;
            }
            self.set_schema_version_tx(&tx, 40)?;
        }

        // Migration 41: Durable controller-to-runner controls and exact-once
        // scheduled objective delivery. Tool approval decisions must survive
        // daemon restarts and host registration races; the scheduler delivers
        // this outbox only while holding its current process-generation fence.
        if current_version < 41 {
            info!("Running migration 41: Durable Mako control outbox");
            if Self::table_exists(&tx, "mako_controllers")
                && Self::table_exists(&tx, "sessions")
                && Self::table_exists(&tx, "mako_runs")
            {
                tx.execute_batch(
                    r#"
                ALTER TABLE mako_runs
                    ADD COLUMN objective_message_id INTEGER
                    REFERENCES messages(id) ON DELETE SET NULL;

                CREATE TABLE mako_control_outbox (
                    id TEXT PRIMARY KEY,
                    controller_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    control_kind TEXT NOT NULL
                        CHECK (control_kind IN ('tool_approval')),
                    dedupe_key TEXT NOT NULL,
                    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'delivered', 'discarded')),
                    attempt_count INTEGER NOT NULL DEFAULT 0
                        CHECK (attempt_count >= 0),
                    available_at TEXT NOT NULL,
                    delivered_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (controller_id, control_kind, dedupe_key),
                    FOREIGN KEY (controller_id) REFERENCES mako_controllers(id) ON DELETE CASCADE,
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (run_id) REFERENCES mako_runs(id) ON DELETE CASCADE
                );
                CREATE INDEX idx_mako_control_outbox_pending
                    ON mako_control_outbox(status, available_at, created_at);
                "#,
                )
                .context("Migration 41: durable Mako control outbox")?;
            }
            self.set_schema_version_tx(&tx, 41)?;
        }

        // Migration 42: Repair the atomic Mako transition journal for
        // controllers whose event stream is still empty. The original
        // trigger selected its next sequence from existing event rows, which
        // produced no INSERT candidate at all when there was no prior row.
        // Use a scalar subquery instead so the first status transition is
        // journaled in the same transaction as the run update.
        if current_version < 42 {
            info!("Running migration 42: Complete atomic Mako transition journal");
            if Self::table_exists(&tx, "mako_runs")
                && Self::table_exists(&tx, "mako_controller_events")
            {
                tx.execute_batch(
                    r#"
                DROP TRIGGER IF EXISTS mako_runs_transition_event;
                CREATE TRIGGER mako_runs_transition_event
                AFTER UPDATE OF status ON mako_runs
                WHEN OLD.status <> NEW.status
                BEGIN
                    INSERT OR IGNORE INTO mako_controller_events (
                        controller_id, sequence, event_type, run_id, schedule_id,
                        dedupe_key, payload_json, created_at
                    ) VALUES (
                        NEW.controller_id,
                        (SELECT COALESCE(MAX(sequence), 0) + 1
                         FROM mako_controller_events
                         WHERE controller_id = NEW.controller_id),
                        CASE
                            WHEN NEW.status = 'queued' AND OLD.status = 'leased'
                                THEN 'run_lease_requeued'
                            WHEN NEW.status = 'queued' THEN 'run_requeued'
                            WHEN NEW.status = 'leased' THEN 'run_leased'
                            WHEN NEW.status = 'running' THEN 'run_started'
                            WHEN NEW.status = 'sleeping' THEN 'run_sleeping'
                            WHEN NEW.status = 'retry_wait' THEN 'run_retry_scheduled'
                            WHEN NEW.status = 'awaiting_input' THEN 'run_awaiting_input'
                            WHEN NEW.status = 'recovery_required' THEN 'recovery_required'
                            WHEN NEW.status = 'succeeded' THEN 'run_completed'
                            WHEN NEW.status = 'failed' THEN 'run_failed'
                            WHEN NEW.status = 'cancelled' THEN 'run_cancelled'
                            WHEN NEW.status = 'dead_letter' THEN 'run_dead_lettered'
                            ELSE 'run_state_changed'
                        END,
                        NEW.id,
                        NEW.schedule_id,
                        'transition:' || NEW.id || ':' || NEW.attempt_count || ':' || NEW.status,
                        json_object(
                            'run_id', NEW.id,
                            'status', NEW.status,
                            'previous_status', OLD.status,
                            'attempt', NEW.attempt_count,
                            'stop_reason', NEW.last_stop_reason,
                            'error', NEW.last_error
                        ),
                        NEW.updated_at
                    );
                END;
                "#,
                )
                .context("Migration 42: complete atomic Mako transition journal")?;
            }
            self.set_schema_version_tx(&tx, 42)?;
        }

        // Migration 43: Replace legacy Mako execution payloads with a
        // minimal allow-listed replay form. Earlier builds journaled raw
        // reasoning, provider signatures, tool arguments/results, web
        // bodies, and error copies. Those values are not a durable contract.
        if current_version < 43 {
            info!("Running migration 43: Redact legacy Mako execution journal");
            if Self::table_exists(&tx, "mako_controller_events")
                && Self::column_exists(&tx, "mako_controller_events", "payload_json")
            {
                tx.execute_batch(
                    r#"
                    UPDATE mako_controller_events
                       SET payload_json = CASE
                         WHEN NOT json_valid(payload_json) THEN
                           json_object(
                             'type', 'redacted_invalid_legacy_event',
                             'redacted', json('true')
                           )
                         WHEN event_type = 'agentic_event' THEN
                           json_object(
                             'type', CASE
                               WHEN json_type(payload_json, '$.type') = 'text'
                                 THEN json_extract(payload_json, '$.type')
                               ELSE 'redacted_legacy_event'
                             END,
                             'id', CASE
                               WHEN json_type(payload_json, '$.id') = 'text'
                                 THEN json_extract(payload_json, '$.id')
                             END,
                             'name', CASE
                               WHEN json_type(payload_json, '$.name') = 'text'
                                 THEN json_extract(payload_json, '$.name')
                             END,
                             'tool_call_id', CASE
                               WHEN json_type(payload_json, '$.tool_call_id') = 'text'
                                 THEN json_extract(payload_json, '$.tool_call_id')
                             END,
                             'tool_name', CASE
                               WHEN json_type(payload_json, '$.tool_name') = 'text'
                                 THEN json_extract(payload_json, '$.tool_name')
                             END,
                             'session_id', CASE
                               WHEN json_type(payload_json, '$.session_id') = 'text'
                                 THEN json_extract(payload_json, '$.session_id')
                             END,
                             'status', CASE
                               WHEN json_type(payload_json, '$.status') = 'text'
                                 THEN json_extract(payload_json, '$.status')
                             END,
                             'is_error', CASE
                               WHEN json_type(payload_json, '$.is_error') IN (
                                 'true', 'false', 'integer'
                               ) THEN json_extract(payload_json, '$.is_error')
                             END,
                             'arguments', CASE
                               WHEN json_type(payload_json, '$.arguments') IS NOT NULL THEN
                                 json_object(
                                   'type', json_type(payload_json, '$.arguments'),
                                   'redacted', json('true')
                                 )
                             END,
                             'arguments_redacted', CASE
                               WHEN json_type(payload_json, '$.arguments') IS NOT NULL
                                 THEN json('true')
                               ELSE json('false')
                             END,
                             'redacted', json('true')
                           )
                         ELSE
                           json_object(
                             'run_id', COALESCE(
                               run_id,
                               CASE WHEN json_type(payload_json, '$.run_id') = 'text'
                                 THEN json_extract(payload_json, '$.run_id') END
                             ),
                             'schedule_id', COALESCE(
                               schedule_id,
                               CASE WHEN json_type(payload_json, '$.schedule_id') = 'text'
                                 THEN json_extract(payload_json, '$.schedule_id') END
                             ),
                             'tool_call_id', CASE
                               WHEN json_type(payload_json, '$.tool_call_id') = 'text'
                                 THEN json_extract(payload_json, '$.tool_call_id')
                             END,
                             'pending_id', CASE
                               WHEN json_type(payload_json, '$.pending_id') = 'text'
                                 THEN json_extract(payload_json, '$.pending_id')
                             END,
                             'kind', CASE
                               WHEN json_type(payload_json, '$.kind') = 'text'
                                 THEN json_extract(payload_json, '$.kind')
                             END,
                             'status', CASE
                               WHEN json_type(payload_json, '$.status') = 'text'
                                 THEN json_extract(payload_json, '$.status')
                             END,
                             'previous_status', CASE
                               WHEN json_type(payload_json, '$.previous_status') = 'text'
                                 THEN json_extract(payload_json, '$.previous_status')
                             END,
                             'previous', CASE
                               WHEN json_type(payload_json, '$.previous') = 'text'
                                 THEN json_extract(payload_json, '$.previous')
                             END,
                             'current', CASE
                               WHEN json_type(payload_json, '$.current') = 'text'
                                 THEN json_extract(payload_json, '$.current')
                             END,
                             'attempt', CASE
                               WHEN json_type(payload_json, '$.attempt') = 'integer'
                                 THEN json_extract(payload_json, '$.attempt')
                             END,
                             'attempt_no', CASE
                               WHEN json_type(payload_json, '$.attempt_no') = 'integer'
                                 THEN json_extract(payload_json, '$.attempt_no')
                             END,
                             'revision', CASE
                               WHEN json_type(payload_json, '$.revision') = 'integer'
                                 THEN json_extract(payload_json, '$.revision')
                             END,
                             'approved', CASE
                               WHEN json_type(payload_json, '$.approved') IN (
                                 'true', 'false', 'integer'
                               ) THEN json_extract(payload_json, '$.approved')
                             END,
                             'redacted', json('true')
                           )
                       END;
                    "#,
                )
                .context("Migration 43: redact legacy Mako controller events")?;
            }

            if Self::table_exists(&tx, "mako_runs") {
                if Self::column_exists(&tx, "mako_runs", "last_error") {
                    tx.execute(
                        "UPDATE mako_runs
                            SET last_error = '[redacted legacy execution error]'
                          WHERE last_error IS NOT NULL",
                        [],
                    )?;
                }
                if Self::column_exists(&tx, "mako_runs", "last_stop_reason") {
                    tx.execute(
                        "UPDATE mako_runs
                            SET last_stop_reason = CASE
                              WHEN last_stop_reason IN (
                                'completed', 'failed', 'transient_failure',
                                'invalid_retry_policy', 'retry_schedule_unavailable',
                                'awaiting_input', 'recovery_required',
                                'execution cancelled'
                              ) THEN last_stop_reason
                              ELSE 'redacted_legacy'
                            END
                          WHERE last_stop_reason IS NOT NULL",
                        [],
                    )?;
                }
                if Self::column_exists(&tx, "mako_runs", "outcome_json") {
                    tx.execute_batch(
                        r#"
                        UPDATE mako_runs
                           SET outcome_json = json_object(
                             'kind', CASE
                               WHEN json_valid(outcome_json)
                                AND json_extract(outcome_json, '$.kind') IN (
                                  'succeeded', 'failed', 'retry_scheduled',
                                  'sleeping', 'awaiting_input',
                                  'recovery_required', 'cancelled'
                                ) THEN json_extract(outcome_json, '$.kind')
                               ELSE 'redacted_legacy'
                             END,
                             'redacted', json('true')
                           )
                         WHERE outcome_json IS NOT NULL;
                        "#,
                    )?;
                }
            }
            if Self::table_exists(&tx, "mako_run_attempts") {
                if Self::column_exists(&tx, "mako_run_attempts", "error") {
                    tx.execute(
                        "UPDATE mako_run_attempts
                            SET error = '[redacted legacy execution error]'
                          WHERE error IS NOT NULL",
                        [],
                    )?;
                }
                if Self::column_exists(&tx, "mako_run_attempts", "stop_reason") {
                    tx.execute(
                        "UPDATE mako_run_attempts
                            SET stop_reason = CASE
                              WHEN stop_reason IN (
                                'completed', 'failed', 'transient_failure',
                                'invalid_retry_policy', 'retry_schedule_unavailable',
                                'awaiting_input', 'recovery_required',
                                'execution cancelled'
                              ) THEN stop_reason
                              ELSE 'redacted_legacy'
                            END
                          WHERE stop_reason IS NOT NULL",
                        [],
                    )?;
                }
            }
            if Self::table_exists(&tx, "mako_runtime_state")
                && Self::column_exists(&tx, "mako_runtime_state", "last_error")
            {
                tx.execute(
                    "UPDATE mako_runtime_state
                        SET last_error = '[redacted legacy execution error]'
                      WHERE last_error IS NOT NULL",
                    [],
                )?;
            }
            if Self::table_exists(&tx, "mako_control_outbox") {
                if Self::column_exists(&tx, "mako_control_outbox", "last_error") {
                    tx.execute(
                        "UPDATE mako_control_outbox
                            SET last_error = '[redacted legacy delivery error]'
                          WHERE last_error IS NOT NULL",
                        [],
                    )?;
                }
                if Self::column_exists(&tx, "mako_control_outbox", "payload_json") {
                    tx.execute_batch(
                        r#"
                        UPDATE mako_control_outbox
                           SET payload_json = CASE
                             WHEN json_valid(payload_json) THEN json_object(
                               'tool_call_id', CASE
                                 WHEN json_type(payload_json, '$.tool_call_id') = 'text'
                                   THEN json_extract(payload_json, '$.tool_call_id')
                               END,
                               'approved', CASE
                                 WHEN json_type(payload_json, '$.approved') IN (
                                   'true', 'false', 'integer'
                                 ) THEN json_extract(payload_json, '$.approved')
                               END,
                               'redacted', json('true')
                             )
                             ELSE json_object('redacted', json('true'))
                           END;
                        "#,
                    )?;
                }
            }

            // Keep every future transition event on the same privacy contract
            // as the explicit append boundary. Raw stop reasons and errors
            // remain on the run projection only in their already-redacted
            // scheduler form and never enter replay payloads.
            if Self::table_exists(&tx, "mako_runs")
                && Self::table_exists(&tx, "mako_controller_events")
            {
                tx.execute_batch(
                    r#"
                    DROP TRIGGER IF EXISTS mako_runs_transition_event;
                    CREATE TRIGGER mako_runs_transition_event
                    AFTER UPDATE OF status ON mako_runs
                    WHEN OLD.status <> NEW.status
                    BEGIN
                        INSERT OR IGNORE INTO mako_controller_events (
                            controller_id, sequence, event_type, run_id, schedule_id,
                            dedupe_key, payload_json, created_at
                        ) VALUES (
                            NEW.controller_id,
                            (SELECT COALESCE(MAX(sequence), 0) + 1
                             FROM mako_controller_events
                             WHERE controller_id = NEW.controller_id),
                            CASE
                                WHEN NEW.status = 'queued' AND OLD.status = 'leased'
                                    THEN 'run_lease_requeued'
                                WHEN NEW.status = 'queued' THEN 'run_requeued'
                                WHEN NEW.status = 'leased' THEN 'run_leased'
                                WHEN NEW.status = 'running' THEN 'run_started'
                                WHEN NEW.status = 'sleeping' THEN 'run_sleeping'
                                WHEN NEW.status = 'retry_wait' THEN 'run_retry_scheduled'
                                WHEN NEW.status = 'awaiting_input' THEN 'run_awaiting_input'
                                WHEN NEW.status = 'recovery_required' THEN 'recovery_required'
                                WHEN NEW.status = 'succeeded' THEN 'run_completed'
                                WHEN NEW.status = 'failed' THEN 'run_failed'
                                WHEN NEW.status = 'cancelled' THEN 'run_cancelled'
                                WHEN NEW.status = 'dead_letter' THEN 'run_dead_lettered'
                                ELSE 'run_state_changed'
                            END,
                            NEW.id,
                            NEW.schedule_id,
                            'transition:' || NEW.id || ':' || NEW.attempt_count || ':' || NEW.status,
                            json_object(
                                'run_id', NEW.id,
                                'status', NEW.status,
                                'previous_status', OLD.status,
                                'attempt', NEW.attempt_count,
                                'has_stop_reason', NEW.last_stop_reason IS NOT NULL,
                                'has_error', NEW.last_error IS NOT NULL,
                                'redacted', json('true')
                            ),
                            NEW.updated_at
                        );
                    END;
                    "#,
                )
                .context("Migration 43: install privacy-safe Mako transition journal")?;
            }
            self.set_schema_version_tx(&tx, 43)?;
        }

        tx.commit()?;

        if privacy_cleanup_required {
            // UPDATE redaction alone does not remove prior values from WAL or
            // free pages. Checkpoint, rebuild, and checkpoint again before
            // publishing schema 44 as the durable cleanup-complete marker.
            self.checkpoint_wal_without_busy_readers("pre-VACUUM")?;
            self.conn
                .execute_batch("VACUUM;")
                .context("physically erasing legacy Mako journal payloads")?;
            self.checkpoint_wal_without_busy_readers("post-VACUUM")?;
        }

        // Migration 44: crash-safe physical privacy-cleanup checkpoint. A
        // process that dies after migration 43 commits but before this insert
        // leaves the database at 43; the next opener repeats the idempotent
        // checkpoint/VACUUM and only then advances to 44.
        let finalize_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .context("acquiring Mako privacy-cleanup checkpoint lock")?;
        finalize_tx
            .execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (44)",
                [],
            )
            .context("recording completed Mako privacy cleanup")?;
        finalize_tx.commit()?;

        // Migration 45: provider-aware model identity on sessions.
        //
        // Keep the legacy `model` slug for older clients while storing the
        // exact provider/auth/transport key and catalog revision alongside it.
        // Existing rows intentionally remain NULL: guessing a provider from a
        // bare slug would recreate the ambiguity this migration removes.
        if current_version < 45 {
            info!("Running migration 45: provider-aware session model identity");
            let model_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring provider-aware model migration lock")?;
            if Self::table_exists(&model_tx, "sessions") {
                if !Self::column_exists(&model_tx, "sessions", "model_key_json") {
                    model_tx
                        .execute_batch("ALTER TABLE sessions ADD COLUMN model_key_json TEXT;")
                        .context("Migration 45: add session model key")?;
                }
                if !Self::column_exists(&model_tx, "sessions", "model_catalog_revision") {
                    model_tx
                        .execute_batch(
                            "ALTER TABLE sessions ADD COLUMN model_catalog_revision TEXT;",
                        )
                        .context("Migration 45: add session model catalog revision")?;
                }
            }
            model_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (45)",
                [],
            )?;
            model_tx.commit()?;
        }

        // Migration 46: provider-aware model identity on durable Mako schedules.
        //
        // Scheduled occurrences must retain the same provider/auth/transport
        // selection as the request that created them. Existing bare-model rows
        // remain NULL and use the ambiguity-rejecting legacy execution path.
        if current_version < 46 {
            info!("Running migration 46: provider-aware Mako schedule model identity");
            let mako_model_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Mako model identity migration lock")?;
            if Self::table_exists(&mako_model_tx, "mako_schedules") {
                if !Self::column_exists(&mako_model_tx, "mako_schedules", "model_key_json") {
                    mako_model_tx
                        .execute_batch("ALTER TABLE mako_schedules ADD COLUMN model_key_json TEXT;")
                        .context("Migration 46: add Mako schedule model key")?;
                }
                if !Self::column_exists(&mako_model_tx, "mako_schedules", "model_catalog_revision")
                {
                    mako_model_tx
                        .execute_batch(
                            "ALTER TABLE mako_schedules ADD COLUMN model_catalog_revision TEXT;",
                        )
                        .context("Migration 46: add Mako schedule model catalog revision")?;
                }
            }
            mako_model_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (46)",
                [],
            )?;
            mako_model_tx.commit()?;
        }

        // Migration 47: Canonical Goal/Plan workflow state.
        //
        // The legacy `plans` table remains intact as an import/export and
        // rollback surface. Executable workflow state is normalized, revisioned,
        // and append-journaled so reconnects, concurrent clients, and automatic
        // continuation all observe the same durable contract.
        if current_version < 47 {
            info!("Running migration 47: canonical Goal and Plan workflow");
            let workflow_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring workflow migration lock")?;
            workflow_tx
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS workflow_goals (
                        id TEXT PRIMARY KEY,
                        session_id TEXT NOT NULL,
                        title TEXT NOT NULL,
                        objective TEXT NOT NULL,
                        constraints_json TEXT NOT NULL DEFAULT '[]'
                            CHECK (json_valid(constraints_json)),
                        status TEXT NOT NULL DEFAULT 'draft'
                            CHECK (status IN (
                                'draft', 'active', 'paused', 'blocked',
                                'completed', 'cancelled'
                            )),
                        status_reason TEXT,
                        needs_definition INTEGER NOT NULL DEFAULT 0
                            CHECK (needs_definition IN (0, 1)),
                        revision INTEGER NOT NULL DEFAULT 1
                            CHECK (revision >= 1),
                        token_budget INTEGER CHECK (token_budget IS NULL OR token_budget > 0),
                        tokens_used INTEGER NOT NULL DEFAULT 0
                            CHECK (tokens_used >= 0),
                        source TEXT NOT NULL DEFAULT 'user'
                            CHECK (source IN ('user', 'legacy_import', 'system')),
                        legacy_plan_id TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        activated_at TEXT,
                        completed_at TEXT,
                        cancelled_at TEXT,
                        FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_goals_one_unfinished
                        ON workflow_goals(session_id)
                        WHERE status IN ('draft', 'active', 'paused', 'blocked');
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_goals_legacy_plan
                        ON workflow_goals(legacy_plan_id)
                        WHERE legacy_plan_id IS NOT NULL;
                    CREATE INDEX IF NOT EXISTS idx_workflow_goals_session_updated
                        ON workflow_goals(session_id, updated_at DESC);

                    CREATE TABLE IF NOT EXISTS workflow_goal_criteria (
                        id TEXT PRIMARY KEY,
                        goal_id TEXT NOT NULL,
                        position INTEGER NOT NULL CHECK (position >= 0),
                        description TEXT NOT NULL,
                        required INTEGER NOT NULL DEFAULT 1
                            CHECK (required IN (0, 1)),
                        status TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'passed', 'failed', 'waived')),
                        evidence_json TEXT NOT NULL DEFAULT '[]'
                            CHECK (json_valid(evidence_json)),
                        verifier TEXT,
                        verified_at TEXT,
                        FOREIGN KEY (goal_id) REFERENCES workflow_goals(id) ON DELETE CASCADE,
                        UNIQUE (goal_id, position)
                    );

                    CREATE TABLE IF NOT EXISTS workflow_plan_revisions (
                        id TEXT PRIMARY KEY,
                        goal_id TEXT NOT NULL,
                        revision_number INTEGER NOT NULL CHECK (revision_number >= 1),
                        status TEXT NOT NULL DEFAULT 'proposed'
                            CHECK (status IN (
                                'proposed', 'approved', 'active', 'superseded',
                                'completed', 'cancelled'
                            )),
                        title TEXT NOT NULL,
                        rationale TEXT,
                        source_message_id INTEGER,
                        predecessor_id TEXT,
                        legacy_markdown TEXT,
                        created_at TEXT NOT NULL,
                        approved_at TEXT,
                        completed_at TEXT,
                        FOREIGN KEY (goal_id) REFERENCES workflow_goals(id) ON DELETE CASCADE,
                        FOREIGN KEY (source_message_id) REFERENCES messages(id) ON DELETE SET NULL,
                        FOREIGN KEY (predecessor_id) REFERENCES workflow_plan_revisions(id)
                            ON DELETE SET NULL,
                        UNIQUE (goal_id, revision_number)
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_plan_one_active
                        ON workflow_plan_revisions(goal_id)
                        WHERE status = 'active';
                    CREATE INDEX IF NOT EXISTS idx_workflow_plan_goal_revision
                        ON workflow_plan_revisions(goal_id, revision_number DESC);

                    CREATE TABLE IF NOT EXISTS workflow_plan_steps (
                        id TEXT PRIMARY KEY,
                        plan_revision_id TEXT NOT NULL,
                        parent_step_id TEXT,
                        display_key TEXT NOT NULL,
                        position INTEGER NOT NULL CHECK (position >= 0),
                        description TEXT NOT NULL,
                        context TEXT,
                        acceptance_criteria_json TEXT NOT NULL DEFAULT '[]'
                            CHECK (json_valid(acceptance_criteria_json)),
                        required INTEGER NOT NULL DEFAULT 1
                            CHECK (required IN (0, 1)),
                        status TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN (
                                'pending', 'in_progress', 'blocked', 'completed',
                                'failed', 'skipped', 'cancelled'
                            )),
                        outcome TEXT,
                        evidence_json TEXT NOT NULL DEFAULT '[]'
                            CHECK (json_valid(evidence_json)),
                        claimed_attempt_id TEXT,
                        revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        completed_at TEXT,
                        FOREIGN KEY (plan_revision_id)
                            REFERENCES workflow_plan_revisions(id) ON DELETE CASCADE,
                        FOREIGN KEY (parent_step_id)
                            REFERENCES workflow_plan_steps(id) ON DELETE CASCADE,
                        UNIQUE (plan_revision_id, display_key)
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_one_serial_step
                        ON workflow_plan_steps(plan_revision_id)
                        WHERE status = 'in_progress' AND parent_step_id IS NULL;
                    CREATE INDEX IF NOT EXISTS idx_workflow_steps_plan_position
                        ON workflow_plan_steps(plan_revision_id, position);

                    CREATE TABLE IF NOT EXISTS workflow_step_dependencies (
                        step_id TEXT NOT NULL,
                        depends_on_step_id TEXT NOT NULL,
                        PRIMARY KEY (step_id, depends_on_step_id),
                        CHECK (step_id <> depends_on_step_id),
                        FOREIGN KEY (step_id) REFERENCES workflow_plan_steps(id) ON DELETE CASCADE,
                        FOREIGN KEY (depends_on_step_id)
                            REFERENCES workflow_plan_steps(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS workflow_execution_attempts (
                        id TEXT PRIMARY KEY,
                        goal_id TEXT NOT NULL,
                        plan_revision_id TEXT,
                        step_id TEXT,
                        status TEXT NOT NULL DEFAULT 'running'
                            CHECK (status IN (
                                'running', 'paused', 'succeeded', 'failed', 'cancelled'
                            )),
                        stop_reason TEXT,
                        permission_mode TEXT NOT NULL
                            CHECK (permission_mode IN ('supervised', 'autonomous')),
                        goal_revision_at_start INTEGER NOT NULL,
                        max_turns INTEGER NOT NULL CHECK (max_turns > 0),
                        max_tool_calls INTEGER NOT NULL CHECK (max_tool_calls > 0),
                        max_wall_time_secs INTEGER NOT NULL CHECK (max_wall_time_secs > 0),
                        max_research_actions INTEGER NOT NULL CHECK (max_research_actions > 0),
                        turn_count INTEGER NOT NULL DEFAULT 0 CHECK (turn_count >= 0),
                        tool_call_count INTEGER NOT NULL DEFAULT 0 CHECK (tool_call_count >= 0),
                        research_action_count INTEGER NOT NULL DEFAULT 0
                            CHECK (research_action_count >= 0),
                        progress_revision INTEGER NOT NULL DEFAULT 0
                            CHECK (progress_revision >= 0),
                        blocker_fingerprint TEXT,
                        blocker_streak INTEGER NOT NULL DEFAULT 0
                            CHECK (blocker_streak >= 0),
                        started_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        ended_at TEXT,
                        FOREIGN KEY (goal_id) REFERENCES workflow_goals(id) ON DELETE CASCADE,
                        FOREIGN KEY (plan_revision_id)
                            REFERENCES workflow_plan_revisions(id) ON DELETE SET NULL,
                        FOREIGN KEY (step_id) REFERENCES workflow_plan_steps(id) ON DELETE SET NULL
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_one_running_attempt
                        ON workflow_execution_attempts(goal_id)
                        WHERE status = 'running';
                    CREATE INDEX IF NOT EXISTS idx_workflow_attempt_goal_started
                        ON workflow_execution_attempts(goal_id, started_at DESC);

                    CREATE TABLE IF NOT EXISTS workflow_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        goal_id TEXT NOT NULL,
                        aggregate_revision INTEGER NOT NULL CHECK (aggregate_revision >= 1),
                        operation_id TEXT NOT NULL UNIQUE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        attempt_id TEXT,
                        payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
                        created_at TEXT NOT NULL,
                        FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                        FOREIGN KEY (goal_id) REFERENCES workflow_goals(id) ON DELETE CASCADE,
                        FOREIGN KEY (attempt_id)
                            REFERENCES workflow_execution_attempts(id) ON DELETE SET NULL,
                        UNIQUE (goal_id, aggregate_revision)
                    );
                    CREATE INDEX IF NOT EXISTS idx_workflow_events_session_revision
                        ON workflow_events(session_id, aggregate_revision);

                    CREATE TABLE IF NOT EXISTS workflow_idempotency (
                        operation_id TEXT PRIMARY KEY,
                        session_id TEXT NOT NULL,
                        action TEXT NOT NULL,
                        result_json TEXT NOT NULL CHECK (json_valid(result_json)),
                        created_at TEXT NOT NULL,
                        FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                    );
                    "#,
                )
                .context("Migration 47: create canonical workflow schema")?;
            workflow_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (47)",
                [],
            )?;
            workflow_tx.commit()?;
        }

        // Migration 48: canonical mobile notification lifecycle.
        //
        // Device identity and policy are persisted independently from provider
        // acceptance. Notification intents form a durable outbox, Expo tokens
        // cover Android delivery, and ActivityKit tokens are scoped to the
        // session whose Live Activity they update.
        if current_version < 48 {
            info!("Running migration 48: mobile notification lifecycle");
            let notification_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring notification lifecycle migration lock")?;

            if Self::table_exists(&notification_tx, "apns_devices") {
                if !Self::column_exists(&notification_tx, "apns_devices", "notification_level") {
                    notification_tx.execute_batch(
                        "ALTER TABLE apns_devices
                         ADD COLUMN notification_level TEXT NOT NULL DEFAULT 'important'
                         CHECK (notification_level IN ('all', 'important', 'silent'));",
                    )?;
                }
                if !Self::column_exists(&notification_tx, "apns_devices", "environment") {
                    notification_tx.execute_batch(
                        "ALTER TABLE apns_devices
                         ADD COLUMN environment TEXT NOT NULL DEFAULT 'production'
                         CHECK (environment IN ('sandbox', 'production'));",
                    )?;
                }
                if !Self::column_exists(&notification_tx, "apns_devices", "last_registered_at") {
                    notification_tx.execute_batch(
                        "ALTER TABLE apns_devices ADD COLUMN last_registered_at TEXT;",
                    )?;
                }
                if !Self::column_exists(&notification_tx, "apns_devices", "enabled") {
                    notification_tx.execute_batch(
                        "ALTER TABLE apns_devices
                         ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1
                         CHECK (enabled IN (0, 1));",
                    )?;
                }
            }

            notification_tx
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS expo_push_devices (
                        id TEXT PRIMARY KEY,
                        user_id TEXT,
                        expo_push_token TEXT NOT NULL UNIQUE,
                        platform TEXT NOT NULL,
                        notification_level TEXT NOT NULL DEFAULT 'important'
                            CHECK (notification_level IN ('all', 'important', 'silent')),
                        enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
                        created_at TEXT NOT NULL,
                        last_registered_at TEXT NOT NULL,
                        last_success_at TEXT,
                        last_failure_at TEXT,
                        last_failure_reason TEXT,
                        failure_count INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE INDEX IF NOT EXISTS idx_expo_push_devices_user
                        ON expo_push_devices(user_id);

                    CREATE TABLE IF NOT EXISTS live_activity_tokens (
                        id TEXT PRIMARY KEY,
                        user_id TEXT,
                        session_id TEXT NOT NULL,
                        push_token TEXT NOT NULL UNIQUE,
                        bundle_id TEXT NOT NULL DEFAULT 'io.krusty.mobile',
                        environment TEXT NOT NULL DEFAULT 'production'
                            CHECK (environment IN ('sandbox', 'production')),
                        content_state_json TEXT NOT NULL DEFAULT '{}'
                            CHECK (json_valid(content_state_json)),
                        started_at_ms INTEGER NOT NULL,
                        active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        ended_at TEXT,
                        FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_live_activity_tokens_session_active
                        ON live_activity_tokens(session_id, active);
                    CREATE INDEX IF NOT EXISTS idx_live_activity_tokens_user
                        ON live_activity_tokens(user_id);

                    CREATE TABLE IF NOT EXISTS notification_intents (
                        id TEXT PRIMARY KEY,
                        operation_id TEXT NOT NULL UNIQUE,
                        user_id TEXT,
                        session_id TEXT,
                        event_type TEXT NOT NULL,
                        payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
                        status TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN (
                                'pending', 'dispatching', 'accepted', 'failed',
                                'expired', 'cancelled'
                            )),
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        available_at TEXT NOT NULL,
                        expires_at TEXT,
                        last_error TEXT,
                        provider_message_id TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        accepted_at TEXT,
                        FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                    );
                    CREATE INDEX IF NOT EXISTS idx_notification_intents_pending
                        ON notification_intents(status, available_at);
                    CREATE INDEX IF NOT EXISTS idx_notification_intents_session
                        ON notification_intents(session_id, created_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_notification_intents_user
                        ON notification_intents(user_id, created_at DESC);
                    "#,
                )
                .context("Migration 48: create notification lifecycle schema")?;
            notification_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (48)",
                [],
            )?;
            notification_tx.commit()?;
        }

        // Migration 49: bounded, content-free mobile performance diagnostics.
        // Payloads are operational metadata only; prompts, responses, file
        // contents, terminal output, credentials, and raw URLs are forbidden
        // by the HTTP contract before these rows are written.
        if current_version < 49 {
            info!("Running migration 49: mobile performance diagnostics");
            let diagnostics_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring mobile diagnostics migration lock")?;
            diagnostics_tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS mobile_diagnostic_runs (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    installation_id TEXT NOT NULL,
                    app_version TEXT NOT NULL,
                    build_number TEXT NOT NULL,
                    platform TEXT NOT NULL CHECK (platform IN ('ios', 'android', 'web')),
                    os_version TEXT NOT NULL,
                    device_class TEXT NOT NULL,
                    capture_level TEXT NOT NULL CHECK (capture_level IN ('baseline', 'stress')),
                    started_at_ms INTEGER NOT NULL,
                    ended_at_ms INTEGER,
                    status TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'completed')),
                    event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0),
                    dropped_event_count INTEGER NOT NULL DEFAULT 0 CHECK (dropped_event_count >= 0),
                    byte_count INTEGER NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_mobile_diagnostic_runs_user_updated
                    ON mobile_diagnostic_runs(user_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS mobile_diagnostic_events (
                    run_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL CHECK (sequence >= 0),
                    occurred_at_ms INTEGER NOT NULL,
                    monotonic_ms REAL NOT NULL CHECK (monotonic_ms >= 0),
                    category TEXT NOT NULL,
                    name TEXT NOT NULL,
                    duration_ms REAL CHECK (duration_ms IS NULL OR duration_ms >= 0),
                    severity TEXT NOT NULL DEFAULT 'info'
                        CHECK (severity IN ('debug', 'info', 'warning', 'error', 'fatal')),
                    attributes_json TEXT NOT NULL DEFAULT '{}'
                        CHECK (json_valid(attributes_json)),
                    PRIMARY KEY (run_id, sequence),
                    FOREIGN KEY (run_id) REFERENCES mobile_diagnostic_runs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_mobile_diagnostic_events_run_category
                    ON mobile_diagnostic_events(run_id, category, name);

                CREATE TABLE IF NOT EXISTS mobile_diagnostic_native_payloads (
                    run_id TEXT NOT NULL,
                    payload_id TEXT NOT NULL,
                    kind TEXT NOT NULL CHECK (kind IN ('metric', 'diagnostic')),
                    received_at_ms INTEGER NOT NULL,
                    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
                    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
                    PRIMARY KEY (run_id, payload_id),
                    FOREIGN KEY (run_id) REFERENCES mobile_diagnostic_runs(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_mobile_diagnostic_native_run_time
                    ON mobile_diagnostic_native_payloads(run_id, received_at_ms ASC);
                "#,
            )?;
            diagnostics_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (49)",
                [],
            )?;
            diagnostics_tx.commit()?;
        }

        // Migration 50: durable steering idempotency receipts.
        //
        // Pending steering rows are deleted when promoted into canonical
        // history, so the messages table alone cannot reject a repeated
        // completion event after promotion. Keep a compact session-scoped
        // receipt for enqueue-once callers; ordinary interactive and process
        // steering retain their existing queue semantics.
        if current_version < 50 {
            info!("Running migration 50: durable steering idempotency receipts");
            let steering_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring steering idempotency migration lock")?;
            steering_tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS steering_idempotency (
                    session_id TEXT NOT NULL,
                    pending_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (session_id, pending_id),
                    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_steering_idempotency_created
                    ON steering_idempotency(created_at);
                "#,
            )?;
            steering_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (50)",
                [],
            )?;
            steering_tx.commit()?;
        }

        // Migration 51: persist the parent-chosen identity and exact child
        // capability contract so durable resume does not reconstruct access
        // from a presentation-oriented legacy role.
        if current_version < 51 {
            info!("Running migration 51: delegated child contracts");
            let delegated_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring delegated child contract migration lock")?;
            if !Self::column_exists(&delegated_tx, "delegated_runs", "child_name") {
                delegated_tx
                    .execute("ALTER TABLE delegated_runs ADD COLUMN child_name TEXT", [])?;
            }
            if !Self::column_exists(&delegated_tx, "delegated_runs", "capabilities_json") {
                delegated_tx.execute(
                    "ALTER TABLE delegated_runs ADD COLUMN capabilities_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(capabilities_json))",
                    [],
                )?;
            }
            delegated_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (51)",
                [],
            )?;
            delegated_tx.commit()?;
        }

        // Migration 52: one durable descendant may claim a delegated run as
        // its continuation origin. A separate claim table lets older databases
        // retain historical duplicate rows while making every new claim
        // atomic across concurrent SQLite connections.
        if current_version < 52 {
            info!("Running migration 52: unique delegated continuation claims");
            let continuation_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring delegated continuation migration lock")?;
            continuation_tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS delegated_run_continuations (
                    resumed_from_run_id TEXT PRIMARY KEY,
                    delegated_run_id TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (delegated_run_id) REFERENCES delegated_runs(delegated_run_id)
                        ON DELETE CASCADE
                );

                INSERT OR IGNORE INTO delegated_run_continuations (
                    resumed_from_run_id,
                    delegated_run_id,
                    created_at
                )
                SELECT resumed_from_run_id, delegated_run_id, created_at
                  FROM delegated_runs
                 WHERE resumed_from_run_id IS NOT NULL
                 ORDER BY created_at ASC, delegated_run_id ASC;
                "#,
            )?;
            continuation_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (52)",
                [],
            )?;
            continuation_tx.commit()?;
        }

        // Migration 53: persist background parent-wake intent before a child
        // begins. Terminal artifacts and pending steering are separate durable
        // writes; this flag lets server startup reconcile the crash window
        // between them without mistaking foreground delegated runs for work
        // that promised an autonomous parent continuation.
        if current_version < 53 {
            info!("Running migration 53: delegated background wake intent");
            let wake_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring delegated wake-intent migration lock")?;
            if !Self::column_exists(&wake_tx, "delegated_runs", "wake_parent") {
                wake_tx.execute(
                    "ALTER TABLE delegated_runs ADD COLUMN wake_parent INTEGER NOT NULL DEFAULT 0 CHECK (wake_parent IN (0, 1))",
                    [],
                )?;
            }
            wake_tx.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_delegated_runs_unqueued_wake
                    ON delegated_runs(wake_parent, stage, completed_at);",
            )?;
            wake_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (53)",
                [],
            )?;
            wake_tx.commit()?;
        }

        // Migration 54: fence server-hosted background Agent execution with a
        // renewable process-owner lease. New launches persist both fields
        // before execution. Existing non-terminal rows remain NULL on purpose:
        // a mixed-version peer may still own them and cannot renew this new
        // contract, so inventing an expiry during migration would be unsafe.
        if current_version < 54 {
            info!("Running migration 54: delegated background host leases");
            let host_lease_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring delegated host-lease migration lock")?;
            if !Self::column_exists(&host_lease_tx, "delegated_runs", "host_owner_id") {
                host_lease_tx.execute(
                    "ALTER TABLE delegated_runs ADD COLUMN host_owner_id TEXT",
                    [],
                )?;
            }
            if !Self::column_exists(&host_lease_tx, "delegated_runs", "host_lease_expires_at_ms") {
                host_lease_tx.execute(
                    "ALTER TABLE delegated_runs ADD COLUMN host_lease_expires_at_ms INTEGER",
                    [],
                )?;
            }
            host_lease_tx.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_delegated_runs_expired_host_lease
                    ON delegated_runs(wake_parent, stage, host_lease_expires_at_ms);",
            )?;
            host_lease_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (54)",
                [],
            )?;
            host_lease_tx.commit()?;
        }

        // Migration 55: finish Mitsuro/Hive identity in durable schema.
        // - Canonical session_type / memory namespace values are "hive"
        // - Backend tables created as mako_* are renamed to hive_*
        // - CHECK constraints are rewritten to accept the canonical values
        if current_version < 55 {
            info!("Running migration 55: hive table renames and CHECK rewrites");
            // foreign_keys cannot be toggled mid-transaction; disable before
            // opening the rewrite transaction so sessions/agent_memories can
            // be rebuilt without "database table is locked" from child FKs.
            self.conn
                .pragma_update(None, "foreign_keys", "OFF")
                .context("disabling foreign_keys for hive identity rewrite")?;
            let hive_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring hive identity schema migration lock")?;

            // Prefer renaming physical tables over dual-writing. Indexes that
            // still reference old names are recreated with the table rebuild
            // path below when CHECK SQL must change.
            let renames = [
                ("mako_runtime_state", "hive_runtime_state"),
                ("mako_attention_state", "hive_attention_state"),
                ("mako_profiles", "hive_profiles"),
                ("mako_profile_documents", "hive_profile_documents"),
                ("mako_crew_profiles", "hive_crew_profiles"),
                ("mako_crew_documents", "hive_crew_documents"),
                ("mako_controllers", "hive_controllers"),
                ("mako_schedules", "hive_schedules"),
                ("mako_schedule_occurrences", "hive_schedule_occurrences"),
                ("mako_runs", "hive_runs"),
                ("mako_run_attempts", "hive_run_attempts"),
                ("mako_daemon_leases", "hive_daemon_leases"),
                ("mako_idempotency_keys", "hive_idempotency_keys"),
                ("mako_controller_events", "hive_controller_events"),
                ("mako_learning_runs", "hive_learning_runs"),
                ("mako_learning_candidates", "hive_learning_candidates"),
                ("mako_control_outbox", "hive_control_outbox"),
            ];
            for (from, to) in renames {
                if Self::table_exists(&hive_tx, from) && !Self::table_exists(&hive_tx, to) {
                    hive_tx
                        .execute_batch(&format!("ALTER TABLE \"{from}\" RENAME TO \"{to}\";"))
                        .with_context(|| format!("rename {from} -> {to}"))?;
                }
            }

            // CHECK constraints still only accept the legacy discriminator, so
            // UPDATE-before-rebuild is impossible. Remap values during the
            // table rewrite copy instead.
            Self::rebuild_table_with_sql_rewrite_and_values(
                &hive_tx,
                "sessions",
                &["'mako'"],
                &["'hive'"],
                &[("session_type", "mako", "hive")],
            )
            .context("Migration 55: rebuild sessions CHECK for hive")?;
            if Self::table_exists(&hive_tx, "agent_memories") {
                Self::rebuild_table_with_sql_rewrite_and_values(
                    &hive_tx,
                    "agent_memories",
                    &["'mako'"],
                    &["'hive'"],
                    &[("namespace", "mako", "hive")],
                )
                .context("Migration 55: rebuild agent_memories CHECK for hive")?;
            }

            hive_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (55)",
                [],
            )?;
            hive_tx.commit()?;
            self.conn
                .pragma_update(None, "foreign_keys", "ON")
                .context("re-enabling foreign_keys after hive identity rewrite")?;
        }

        // Migration 56: add the session-level delegation authority. Groups
        // own completion/failure policy, tasks own logical objectives, and an
        // append-preserved attempt ledger owns each execution epoch. The
        // existing delegated_runs row remains the compatibility aggregate.
        if current_version < 56 {
            info!("Running migration 56: delegation groups and tasks");
            let delegation_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring delegation coordinator migration lock")?;
            delegation_tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS delegation_groups (
                    delegation_group_id TEXT PRIMARY KEY,
                    parent_session_id TEXT NOT NULL,
                    parent_tool_call_id TEXT,
                    state TEXT NOT NULL
                        CHECK (state IN (
                            'created', 'queued', 'running', 'ready_for_parent',
                            'synthesizing', 'complete', 'degraded', 'failed', 'cancelled'
                        )),
                    contract_json TEXT NOT NULL CHECK (json_valid(contract_json)),
                    parent_continuation_state TEXT NOT NULL
                        CHECK (parent_continuation_state IN (
                            'not_requested', 'pending', 'queued', 'promoted'
                        )),
                    parent_continuation_id TEXT UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    FOREIGN KEY (parent_session_id) REFERENCES sessions(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS delegation_tasks (
                    delegation_task_id TEXT PRIMARY KEY,
                    delegation_group_id TEXT NOT NULL,
                    task_key TEXT NOT NULL,
                    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
                    role TEXT NOT NULL
                        CHECK (role IN ('explore', 'build', 'planner', 'verifier')),
                    state TEXT NOT NULL
                        CHECK (state IN (
                            'created', 'queued', 'leased', 'running', 'retrying',
                            'complete', 'degraded', 'failed', 'cancelled'
                        )),
                    specification_json TEXT NOT NULL CHECK (json_valid(specification_json)),
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
                    error_summary TEXT,
                    lease_owner_id TEXT,
                    lease_expires_at_ms INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    FOREIGN KEY (delegation_group_id)
                        REFERENCES delegation_groups(delegation_group_id) ON DELETE CASCADE,
                    UNIQUE (delegation_group_id, task_key),
                    UNIQUE (delegation_group_id, ordinal)
                );

                CREATE TABLE IF NOT EXISTS delegation_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_session_id TEXT NOT NULL,
                    delegation_group_id TEXT NOT NULL,
                    delegation_task_id TEXT,
                    event_type TEXT NOT NULL CHECK (event_type IN (
                        'group_created', 'group_queued', 'group_state_changed',
                        'task_claimed', 'task_running', 'task_state_changed',
                        'parent_continuation_queued', 'parent_continuation_promoted'
                    )),
                    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (parent_session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (delegation_group_id)
                        REFERENCES delegation_groups(delegation_group_id) ON DELETE CASCADE,
                    FOREIGN KEY (delegation_task_id)
                        REFERENCES delegation_tasks(delegation_task_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS delegation_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    delegation_group_id TEXT NOT NULL,
                    delegation_task_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
                    lease_owner_id TEXT NOT NULL,
                    runtime_key TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN (
                        'running', 'complete', 'degraded', 'failed',
                        'cancelled', 'expired'
                    )),
                    artifact_json TEXT CHECK (
                        artifact_json IS NULL OR json_valid(artifact_json)
                    ),
                    error_summary TEXT,
                    started_at TEXT NOT NULL,
                    last_heartbeat_at TEXT NOT NULL,
                    completed_at TEXT,
                    FOREIGN KEY (delegation_group_id)
                        REFERENCES delegation_groups(delegation_group_id) ON DELETE CASCADE,
                    FOREIGN KEY (delegation_task_id)
                        REFERENCES delegation_tasks(delegation_task_id) ON DELETE CASCADE,
                    UNIQUE (delegation_task_id, attempt_number)
                );

                CREATE INDEX IF NOT EXISTS idx_delegation_groups_session_updated
                    ON delegation_groups(parent_session_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_delegation_groups_parent_tool
                    ON delegation_groups(parent_tool_call_id);
                CREATE INDEX IF NOT EXISTS idx_delegation_groups_state
                    ON delegation_groups(state, updated_at ASC);
                CREATE INDEX IF NOT EXISTS idx_delegation_groups_continuation
                    ON delegation_groups(parent_continuation_state, updated_at ASC);
                CREATE INDEX IF NOT EXISTS idx_delegation_tasks_group_ordinal
                    ON delegation_tasks(delegation_group_id, ordinal ASC);
                CREATE INDEX IF NOT EXISTS idx_delegation_tasks_schedulable
                    ON delegation_tasks(state, lease_expires_at_ms, updated_at ASC);
                CREATE INDEX IF NOT EXISTS idx_delegation_events_session_cursor
                    ON delegation_events(parent_session_id, event_id ASC);
                CREATE INDEX IF NOT EXISTS idx_delegation_events_group_cursor
                    ON delegation_events(delegation_group_id, event_id ASC);
                CREATE INDEX IF NOT EXISTS idx_delegation_attempts_task_number
                    ON delegation_attempts(delegation_task_id, attempt_number ASC);
                CREATE INDEX IF NOT EXISTS idx_delegation_attempts_group_state
                    ON delegation_attempts(delegation_group_id, state, started_at ASC);
                "#,
            )?;
            if !Self::column_exists(&delegation_tx, "delegated_runs", "delegation_group_id") {
                delegation_tx.execute(
                    "ALTER TABLE delegated_runs ADD COLUMN delegation_group_id TEXT",
                    [],
                )?;
            }
            if !Self::column_exists(&delegation_tx, "delegated_runs", "delegation_task_id") {
                delegation_tx.execute(
                    "ALTER TABLE delegated_runs ADD COLUMN delegation_task_id TEXT",
                    [],
                )?;
            }
            if !Self::column_exists(&delegation_tx, "delegated_runs", "attempt_number") {
                delegation_tx.execute(
                    "ALTER TABLE delegated_runs ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 1 CHECK (attempt_number >= 1)",
                    [],
                )?;
            }
            delegation_tx.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_delegated_runs_group_updated
                    ON delegated_runs(delegation_group_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_delegated_runs_task_attempt
                    ON delegated_runs(delegation_task_id, attempt_number DESC);
                "#,
            )?;
            delegation_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (56)",
                [],
            )?;
            delegation_tx.commit()?;
        }

        // Migration 57: fence aggregate synthesis with the same durable lease
        // discipline as task execution. A process crash in ReadyForParent or
        // Synthesizing can now be reclaimed without allowing two parents to
        // integrate patches or publish the aggregate result concurrently.
        if current_version < 57 {
            info!("Running migration 57: delegation synthesis leases");
            let synthesis_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring delegation synthesis migration lock")?;
            if !Self::column_exists(&synthesis_tx, "delegation_groups", "synthesis_owner_id") {
                synthesis_tx.execute(
                    "ALTER TABLE delegation_groups ADD COLUMN synthesis_owner_id TEXT",
                    [],
                )?;
            }
            if !Self::column_exists(
                &synthesis_tx,
                "delegation_groups",
                "synthesis_lease_expires_at_ms",
            ) {
                synthesis_tx.execute(
                    "ALTER TABLE delegation_groups ADD COLUMN synthesis_lease_expires_at_ms INTEGER",
                    [],
                )?;
            }
            if !Self::column_exists(
                &synthesis_tx,
                "delegation_groups",
                "synthesis_attempt_count",
            ) {
                synthesis_tx.execute(
                    "ALTER TABLE delegation_groups ADD COLUMN synthesis_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (synthesis_attempt_count >= 0)",
                    [],
                )?;
            }
            synthesis_tx.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_delegation_groups_synthesis_lease
                    ON delegation_groups(state, synthesis_lease_expires_at_ms);",
            )?;
            synthesis_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (57)",
                [],
            )?;
            synthesis_tx.commit()?;
        }

        // Migration 58: make capacity admission a database authority rather
        // than a process-local assumption. The in-process scheduler remains
        // the fast fairness layer, while these leases provide the hard host,
        // provider-domain, and writer-contention ceiling across every process
        // sharing this database.
        if current_version < 58 {
            info!("Running migration 58: durable delegation capacity authority");
            let capacity_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring delegation capacity migration lock")?;
            capacity_tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS delegation_capacity_hosts (
                    authority_key TEXT PRIMARY KEY,
                    target_limit INTEGER NOT NULL CHECK (target_limit > 0),
                    minimum_limit INTEGER NOT NULL CHECK (minimum_limit > 0),
                    maximum_limit INTEGER NOT NULL CHECK (maximum_limit >= minimum_limit),
                    ramp_step INTEGER NOT NULL CHECK (ramp_step > 0),
                    healthy_threshold INTEGER NOT NULL CHECK (healthy_threshold > 0),
                    healthy_streak INTEGER NOT NULL DEFAULT 0 CHECK (healthy_streak >= 0),
                    demand_observed INTEGER NOT NULL DEFAULT 0 CHECK (demand_observed IN (0, 1)),
                    default_cooldown_ms INTEGER NOT NULL CHECK (default_cooldown_ms > 0),
                    updated_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS delegation_capacity_domains (
                    authority_key TEXT NOT NULL,
                    domain_key TEXT NOT NULL,
                    target_limit INTEGER NOT NULL CHECK (target_limit > 0),
                    healthy_streak INTEGER NOT NULL DEFAULT 0 CHECK (healthy_streak >= 0),
                    demand_observed INTEGER NOT NULL DEFAULT 0 CHECK (demand_observed IN (0, 1)),
                    cooldown_until_ms INTEGER,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (authority_key, domain_key),
                    FOREIGN KEY (authority_key)
                        REFERENCES delegation_capacity_hosts(authority_key) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS delegation_capacity_waiters (
                    waiter_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    delegation_task_id TEXT NOT NULL UNIQUE,
                    lease_owner_id TEXT NOT NULL,
                    authority_key TEXT NOT NULL,
                    domain_key TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    scheduling_class TEXT NOT NULL CHECK (scheduling_class IN (
                        'read_only', 'write_shared', 'write_isolated', 'verification'
                    )),
                    isolation_group TEXT,
                    lease_expires_at_ms INTEGER NOT NULL,
                    enqueued_at_ms INTEGER NOT NULL,
                    FOREIGN KEY (delegation_task_id)
                        REFERENCES delegation_tasks(delegation_task_id) ON DELETE CASCADE,
                    FOREIGN KEY (authority_key)
                        REFERENCES delegation_capacity_hosts(authority_key) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS delegation_capacity_leases (
                    delegation_task_id TEXT PRIMARY KEY,
                    lease_owner_id TEXT NOT NULL,
                    authority_key TEXT NOT NULL,
                    domain_key TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    scheduling_class TEXT NOT NULL CHECK (scheduling_class IN (
                        'read_only', 'write_shared', 'write_isolated', 'verification'
                    )),
                    isolation_group TEXT,
                    waiter_sequence INTEGER NOT NULL,
                    lease_expires_at_ms INTEGER NOT NULL,
                    admitted_at_ms INTEGER NOT NULL,
                    FOREIGN KEY (delegation_task_id)
                        REFERENCES delegation_tasks(delegation_task_id) ON DELETE CASCADE,
                    FOREIGN KEY (authority_key)
                        REFERENCES delegation_capacity_hosts(authority_key) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_delegation_capacity_waiters_order
                    ON delegation_capacity_waiters(authority_key, domain_key, waiter_sequence);
                CREATE INDEX IF NOT EXISTS idx_delegation_capacity_waiters_expiry
                    ON delegation_capacity_waiters(lease_expires_at_ms);
                CREATE INDEX IF NOT EXISTS idx_delegation_capacity_leases_host
                    ON delegation_capacity_leases(authority_key, lease_expires_at_ms);
                CREATE INDEX IF NOT EXISTS idx_delegation_capacity_leases_domain
                    ON delegation_capacity_leases(authority_key, domain_key, lease_expires_at_ms);
                CREATE INDEX IF NOT EXISTS idx_delegation_capacity_leases_writer
                    ON delegation_capacity_leases(authority_key, partition_key, scheduling_class);
                ",
            )?;
            capacity_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (58)",
                [],
            )?;
            capacity_tx.commit()?;
        }

        // Migration 59: persist an immutable, versioned executor envelope for
        // detached Chat/Code tasks. The envelope contains only reconstruction
        // metadata and a digest of the already-bounded task objective; it does
        // not duplicate parent transcripts, raw prompts, or tool outputs.
        if current_version < 59 {
            info!("Running migration 59: detached delegation executor envelopes");
            let envelope_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring delegation executor envelope migration lock")?;
            if !Self::column_exists(
                &envelope_tx,
                "delegation_tasks",
                "executor_envelope_version",
            ) {
                envelope_tx.execute(
                    "ALTER TABLE delegation_tasks ADD COLUMN executor_envelope_version INTEGER",
                    [],
                )?;
            }
            if !Self::column_exists(&envelope_tx, "delegation_tasks", "executor_envelope_json") {
                envelope_tx.execute(
                    "ALTER TABLE delegation_tasks ADD COLUMN executor_envelope_json TEXT",
                    [],
                )?;
            }
            envelope_tx.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_delegation_tasks_replayable
                    ON delegation_tasks(executor_envelope_version, state, delegation_group_id);",
            )?;
            envelope_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (59)",
                [],
            )?;
            envelope_tx.commit()?;
        }

        // Migration 60: elect exactly one recovery host for a replayable
        // detached group across startup and periodic reconciliation scans.
        if current_version < 60 {
            info!("Running migration 60: delegation replay owner leases");
            let replay_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring delegation replay lease migration lock")?;
            if !Self::column_exists(&replay_tx, "delegation_groups", "replay_owner_id") {
                replay_tx.execute(
                    "ALTER TABLE delegation_groups ADD COLUMN replay_owner_id TEXT",
                    [],
                )?;
            }
            if !Self::column_exists(
                &replay_tx,
                "delegation_groups",
                "replay_lease_expires_at_ms",
            ) {
                replay_tx.execute(
                    "ALTER TABLE delegation_groups ADD COLUMN replay_lease_expires_at_ms INTEGER",
                    [],
                )?;
            }
            if !Self::column_exists(&replay_tx, "delegation_groups", "replay_attempt_count") {
                replay_tx.execute(
                    "ALTER TABLE delegation_groups ADD COLUMN replay_attempt_count INTEGER NOT NULL DEFAULT 0",
                    [],
                )?;
            }
            replay_tx.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_delegation_groups_replay_lease
                    ON delegation_groups(state, replay_lease_expires_at_ms, delegation_group_id);",
            )?;
            replay_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (60)",
                [],
            )?;
            replay_tx.commit()?;
        }

        // Migration 61: event kinds are an append-only protocol surface, not a
        // closed lifecycle state machine. Keep group/task state CHECKs closed,
        // but allow newer servers to persist event kinds that older clients
        // safely project as `Other`/an opaque string.
        if current_version < 61 {
            info!("Running migration 61: extensible delegation event kinds");
            let event_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring delegation event compatibility migration lock")?;
            if Self::table_exists(&event_tx, "delegation_events") {
                Self::rebuild_table_with_sql_rewrite_and_values(
                    &event_tx,
                    "delegation_events",
                    &[r#"event_type TEXT NOT NULL CHECK (event_type IN (
                        'group_created', 'group_queued', 'group_state_changed',
                        'task_claimed', 'task_running', 'task_state_changed',
                        'parent_continuation_queued', 'parent_continuation_promoted'
                    ))"#],
                    &["event_type TEXT NOT NULL"],
                    &[],
                )
                .context("Migration 61: remove delegation event kind CHECK")?;

                let create_sql: String = event_tx.query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'delegation_events'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    !create_sql.contains("event_type IN"),
                    "Migration 61 could not remove the delegation event kind CHECK"
                );
            }
            event_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (61)",
                [],
            )?;
            event_tx.commit()?;
        }

        // Migration 62: durable conversation-list organization. Pinning is an
        // ordering preference, while archiving is a reversible visibility
        // state; neither mutates conversation history or project files.
        if current_version < 62 {
            info!("Running migration 62: session pin and archive metadata");
            let session_list_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring session list metadata migration lock")?;
            if Self::table_exists(&session_list_tx, "sessions") {
                if !Self::column_exists(&session_list_tx, "sessions", "pinned_at") {
                    session_list_tx
                        .execute_batch("ALTER TABLE sessions ADD COLUMN pinned_at TEXT;")?;
                }
                if !Self::column_exists(&session_list_tx, "sessions", "archived_at") {
                    session_list_tx
                        .execute_batch("ALTER TABLE sessions ADD COLUMN archived_at TEXT;")?;
                }
                if Self::column_exists(&session_list_tx, "sessions", "updated_at") {
                    session_list_tx.execute_batch(
                        "CREATE INDEX IF NOT EXISTS idx_sessions_archive_pin_updated
                            ON sessions(archived_at, pinned_at, updated_at DESC);",
                    )?;
                }
            }
            session_list_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (62)",
                [],
            )?;
            session_list_tx.commit()?;
        }

        // Migration 63: first-class Hive Worker identities.
        //
        // A Worker is a durable product identity (persona documents, frozen
        // provider/model choice, autonomy policy, private DM session) layered
        // over the existing controller/run machinery. Existing crew profiles
        // and the durable Hive companion session are backfilled as Workers,
        // and the daemon lease claimant on hive_run_attempts is renamed to
        // executor_id so "worker" refers only to the product concept.
        if current_version < 63 {
            info!("Running migration 63: Hive Worker identities");
            let worker_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring Hive worker identity migration lock")?;

            worker_tx
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS hive_workers (
                        id TEXT PRIMARY KEY,
                        user_id TEXT,
                        slug TEXT NOT NULL,
                        display_name TEXT NOT NULL,
                        avatar_color TEXT,
                        model TEXT,
                        model_key_json TEXT
                            CHECK (model_key_json IS NULL OR json_valid(model_key_json)),
                        model_catalog_revision TEXT,
                        permission_mode TEXT NOT NULL DEFAULT 'autonomous'
                            CHECK (permission_mode IN ('supervised', 'autonomous')),
                        autonomy TEXT NOT NULL DEFAULT 'manual'
                            CHECK (autonomy IN ('manual', 'scheduled', 'always_on')),
                        heartbeat_interval_secs INTEGER
                            CHECK (
                                heartbeat_interval_secs IS NULL
                                OR heartbeat_interval_secs > 0
                            ),
                        status TEXT NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'paused', 'archived')),
                        dm_session_id TEXT UNIQUE
                            REFERENCES sessions(id) ON DELETE SET NULL,
                        memory_namespace_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_workers_active_slug
                        ON hive_workers(COALESCE(user_id, ''), slug)
                        WHERE status <> 'archived';
                    CREATE INDEX IF NOT EXISTS idx_hive_workers_owner_status
                        ON hive_workers(user_id, status);

                    CREATE TABLE IF NOT EXISTS hive_worker_documents (
                        worker_id TEXT NOT NULL,
                        kind TEXT NOT NULL CHECK (kind IN ('identity', 'soul')),
                        content TEXT NOT NULL,
                        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                        PRIMARY KEY (worker_id, kind),
                        FOREIGN KEY (worker_id)
                            REFERENCES hive_workers(id) ON DELETE CASCADE
                    );
                    "#,
                )
                .context("Migration 63: create Hive worker tables")?;

            // Nullable linkage columns. Existing rows keep crew_slug as the
            // transitional identity; new call sites write worker_id.
            for table in [
                "hive_controllers",
                "hive_runs",
                "hive_runtime_state",
                "hive_schedules",
            ] {
                if Self::table_exists(&worker_tx, table)
                    && !Self::column_exists(&worker_tx, table, "worker_id")
                {
                    worker_tx
                        .execute_batch(&format!(
                            "ALTER TABLE {table} ADD COLUMN worker_id TEXT
                             REFERENCES hive_workers(id) ON DELETE SET NULL;"
                        ))
                        .with_context(|| format!("Migration 63: add {table}.worker_id"))?;
                }
                if Self::table_exists(&worker_tx, table) {
                    worker_tx.execute_batch(&format!(
                        "CREATE INDEX IF NOT EXISTS idx_{table}_worker
                            ON {table}(worker_id) WHERE worker_id IS NOT NULL;"
                    ))?;
                }
            }

            // Free "worker" for the product concept: the run-attempt claimant
            // is the daemon executor instance, not a Hive Worker.
            if Self::table_exists(&worker_tx, "hive_run_attempts")
                && Self::column_exists(&worker_tx, "hive_run_attempts", "worker_id")
                && !Self::column_exists(&worker_tx, "hive_run_attempts", "executor_id")
            {
                worker_tx
                    .execute_batch(
                        "ALTER TABLE hive_run_attempts RENAME COLUMN worker_id TO executor_id;",
                    )
                    .context("Migration 63: rename attempt claimant to executor_id")?;
            }

            let now = chrono::Utc::now().to_rfc3339();

            // Backfill: every crew profile becomes a Worker owned by the
            // profile's user (NULL user_id = local), keeping the crew slug as
            // the memory namespace so existing crew memories stay reachable.
            if Self::table_exists(&worker_tx, "hive_crew_profiles")
                && Self::table_exists(&worker_tx, "hive_profiles")
            {
                let mut crew_rows: Vec<(Option<String>, String)> = Vec::new();
                {
                    let mut statement = worker_tx.prepare(
                        "SELECT p.user_id, cp.slug
                         FROM hive_crew_profiles cp
                         JOIN hive_profiles p ON p.id = cp.profile_id
                         ORDER BY cp.slug",
                    )?;
                    let rows = statement.query_map([], |row| {
                        Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
                    })?;
                    for row in rows {
                        crew_rows.push(row?);
                    }
                }
                for (user_id, slug) in &crew_rows {
                    worker_tx.execute(
                        "INSERT OR IGNORE INTO hive_workers (
                             id, user_id, slug, display_name, permission_mode,
                             autonomy, status, memory_namespace_id,
                             created_at, updated_at
                         ) VALUES (
                             ?1, ?2, ?3, ?4, 'autonomous',
                             'manual', 'active', ?3, ?5, ?5
                         )",
                        rusqlite::params![
                            uuid::Uuid::new_v4().to_string(),
                            user_id,
                            slug,
                            crate::storage::hive_workers::display_name_from_slug(slug),
                            now,
                        ],
                    )?;
                }
                if Self::table_exists(&worker_tx, "hive_crew_documents") {
                    worker_tx
                        .execute(
                            "INSERT OR IGNORE INTO hive_worker_documents
                                 (worker_id, kind, content, updated_at)
                             SELECT w.id, cd.kind, cd.content, cd.updated_at
                             FROM hive_crew_documents cd
                             JOIN hive_profiles p ON p.id = cd.profile_id
                             JOIN hive_workers w
                               ON w.slug = cd.slug
                              AND COALESCE(w.user_id, '') = COALESCE(p.user_id, '')
                              AND w.status <> 'archived'
                             WHERE cd.kind IN ('identity', 'soul')",
                            [],
                        )
                        .context("Migration 63: copy crew documents to workers")?;
                }
            }

            // Backfill: the durable Hive companion chat becomes the default
            // "assistant" Worker with the companion as its DM session.
            let sessions_ready = Self::table_exists(&worker_tx, "sessions")
                && [
                    "title",
                    "session_type",
                    "parent_session_id",
                    "project_dir",
                    "user_id",
                    "updated_at",
                ]
                .iter()
                .all(|column| Self::column_exists(&worker_tx, "sessions", column));
            if sessions_ready {
                let mut companions: Vec<(String, Option<String>)> = Vec::new();
                {
                    let mut statement = worker_tx.prepare(
                        "SELECT id, user_id FROM sessions
                         WHERE title = 'Hive' AND session_type = 'hive'
                           AND parent_session_id IS NULL AND project_dir IS NULL
                         ORDER BY updated_at ASC",
                    )?;
                    let rows = statement.query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                    })?;
                    for row in rows {
                        companions.push(row?);
                    }
                }
                let mut seen_owners = std::collections::HashSet::new();
                for (session_id, user_id) in &companions {
                    // Mirror ensure_hive_main_session: one companion per
                    // owner, preferring the oldest matching session.
                    if !seen_owners.insert(user_id.clone().unwrap_or_default()) {
                        continue;
                    }
                    worker_tx.execute(
                        "INSERT OR IGNORE INTO hive_workers (
                             id, user_id, slug, display_name, permission_mode,
                             autonomy, status, dm_session_id,
                             memory_namespace_id, created_at, updated_at
                         ) VALUES (
                             ?1, ?2, 'assistant', 'Assistant', 'autonomous',
                             'manual', 'active', ?3,
                             'assistant', ?4, ?4
                         )",
                        rusqlite::params![
                            uuid::Uuid::new_v4().to_string(),
                            user_id,
                            session_id,
                            now,
                        ],
                    )?;
                    // If an "assistant" Worker already existed for this owner
                    // (e.g. a crew member with that slug), bind the companion
                    // as its DM session instead of leaving it dangling.
                    worker_tx.execute(
                        "UPDATE hive_workers
                         SET dm_session_id = ?1, updated_at = ?2
                         WHERE slug = 'assistant'
                           AND COALESCE(user_id, '') = COALESCE(?3, '')
                           AND status <> 'archived'
                           AND dm_session_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1 FROM hive_workers bound
                               WHERE bound.dm_session_id = ?1
                           )",
                        rusqlite::params![session_id, now, user_id],
                    )?;
                }
            }

            // Link controllers whose session is a Worker's DM session (the
            // Worker's serialized execution lane).
            if Self::table_exists(&worker_tx, "hive_controllers")
                && Self::column_exists(&worker_tx, "hive_controllers", "worker_id")
            {
                worker_tx.execute(
                    "UPDATE hive_controllers
                     SET worker_id = (
                         SELECT w.id FROM hive_workers w
                         WHERE w.dm_session_id = hive_controllers.session_id
                           AND w.status <> 'archived'
                     )
                     WHERE worker_id IS NULL
                       AND EXISTS (
                           SELECT 1 FROM hive_workers w
                           WHERE w.dm_session_id = hive_controllers.session_id
                             AND w.status <> 'archived'
                       )",
                    [],
                )?;
            }

            // Dual-read transition: map persisted crew assignments onto the
            // backfilled Workers where the slugs match within one owner.
            if Self::table_exists(&worker_tx, "hive_runtime_state")
                && Self::column_exists(&worker_tx, "hive_runtime_state", "crew_slug")
                && Self::column_exists(&worker_tx, "hive_runtime_state", "worker_id")
                && Self::table_exists(&worker_tx, "sessions")
                && Self::column_exists(&worker_tx, "sessions", "user_id")
            {
                worker_tx.execute(
                    "UPDATE hive_runtime_state
                     SET worker_id = (
                         SELECT w.id FROM hive_workers w
                         JOIN sessions s ON s.id = hive_runtime_state.session_id
                         WHERE w.slug = hive_runtime_state.crew_slug
                           AND COALESCE(w.user_id, '') = COALESCE(s.user_id, '')
                           AND w.status <> 'archived'
                     )
                     WHERE crew_slug IS NOT NULL
                       AND worker_id IS NULL
                       AND EXISTS (
                           SELECT 1 FROM hive_workers w
                           JOIN sessions s ON s.id = hive_runtime_state.session_id
                           WHERE w.slug = hive_runtime_state.crew_slug
                             AND COALESCE(w.user_id, '') = COALESCE(s.user_id, '')
                             AND w.status <> 'archived'
                       )",
                    [],
                )?;
            }
            if Self::table_exists(&worker_tx, "hive_schedules")
                && Self::column_exists(&worker_tx, "hive_schedules", "crew_slug")
                && Self::column_exists(&worker_tx, "hive_schedules", "worker_id")
                && Self::table_exists(&worker_tx, "hive_controllers")
            {
                worker_tx.execute(
                    "UPDATE hive_schedules
                     SET worker_id = (
                         SELECT w.id FROM hive_workers w
                         JOIN hive_controllers c ON c.id = hive_schedules.controller_id
                         WHERE w.slug = hive_schedules.crew_slug
                           AND COALESCE(w.user_id, '') = COALESCE(c.user_id, '')
                           AND w.status <> 'archived'
                     )
                     WHERE crew_slug IS NOT NULL
                       AND worker_id IS NULL
                       AND EXISTS (
                           SELECT 1 FROM hive_workers w
                           JOIN hive_controllers c ON c.id = hive_schedules.controller_id
                           WHERE w.slug = hive_schedules.crew_slug
                             AND COALESCE(w.user_id, '') = COALESCE(c.user_id, '')
                             AND w.status <> 'archived'
                       )",
                    [],
                )?;
            }

            worker_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (63)",
                [],
            )?;
            worker_tx.commit()?;
        }

        // Migration 64: Group rooms where Workers collaborate.
        //
        // A group references Workers (never owns them; deleting a group
        // cascades membership but leaves Workers intact), stores an
        // append-only per-group-sequenced message timeline, and records each
        // user-triggered fan-out as a durable hive_group_turns aggregate with
        // per-member outcomes. hive_runs gains nullable group linkage so one
        // member run can be traced back to its turn and trigger message.
        if current_version < 64 {
            info!("Running migration 64: Hive group rooms");

            // hive_runs.kind gains 'group_turn'. The table is a cascade
            // parent (attempts, occurrences, control outbox), so a
            // drop-and-rebuild would destroy child rows; edit the CHECK in
            // place instead. Any DDL later in this migration bumps the schema
            // cookie, so other connections reparse the edited definition.
            let runs_table_exists: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'
                     )",
                    [],
                    |row| row.get(0),
                )
                .context("Migration 64: checking for hive_runs")?;
            if runs_table_exists {
                const LEGACY_KINDS: &str =
                    "('dispatch', 'scheduled', 'controller_child', 'legacy_resume')";
                const GROUP_KINDS: &str =
                    "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 64: enabling writable_schema")?;
                let rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [LEGACY_KINDS, GROUP_KINDS],
                    )
                    .context("Migration 64: extend hive_runs kind CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 64: reloading schema after CHECK edit");
                rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("group_turn") || !runs_sql.contains("kind IN"),
                    "Migration 64 could not extend the hive_runs kind CHECK"
                );
            }

            let group_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring Hive group migration lock")?;

            group_tx
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS hive_groups (
                        id TEXT PRIMARY KEY,
                        user_id TEXT,
                        title TEXT NOT NULL,
                        execution_mode TEXT NOT NULL DEFAULT 'workbench'
                            CHECK (execution_mode IN ('workbench', 'roundtable', 'direct')),
                        max_rounds INTEGER NOT NULL DEFAULT 3
                            CHECK (max_rounds > 0),
                        max_member_messages_per_turn INTEGER NOT NULL DEFAULT 2
                            CHECK (max_member_messages_per_turn > 0),
                        parallelism INTEGER NOT NULL DEFAULT 3
                            CHECK (parallelism > 0),
                        context_window_messages INTEGER NOT NULL DEFAULT 24
                            CHECK (context_window_messages > 0),
                        status TEXT NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'archived')),
                        default_assignee_worker_id TEXT
                            REFERENCES hive_workers(id) ON DELETE SET NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_groups_owner_status
                        ON hive_groups(user_id, status);

                    CREATE TABLE IF NOT EXISTS hive_group_members (
                        group_id TEXT NOT NULL
                            REFERENCES hive_groups(id) ON DELETE CASCADE,
                        worker_id TEXT NOT NULL
                            REFERENCES hive_workers(id),
                        position INTEGER NOT NULL,
                        added_at TEXT NOT NULL,
                        PRIMARY KEY (group_id, worker_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_group_members_worker
                        ON hive_group_members(worker_id);

                    -- Append-only room timeline. seq is allocated
                    -- transactionally per group (like hive_controller_events
                    -- sequences); message rows are never updated or deleted.
                    -- turn_id has no FK because the trigger message and its
                    -- turn reference each other and are inserted in one
                    -- transaction, message first.
                    CREATE TABLE IF NOT EXISTS hive_group_messages (
                        id TEXT PRIMARY KEY,
                        group_id TEXT NOT NULL
                            REFERENCES hive_groups(id) ON DELETE CASCADE,
                        seq INTEGER NOT NULL,
                        sender_kind TEXT NOT NULL
                            CHECK (sender_kind IN ('user', 'worker', 'system')),
                        sender_worker_id TEXT
                            REFERENCES hive_workers(id) ON DELETE SET NULL,
                        sender_run_id TEXT,
                        content TEXT NOT NULL,
                        -- SET NULL keeps whole-group cascade deletes safe under
                        -- enforced foreign keys; the store API itself never
                        -- updates or deletes message rows.
                        reply_to_message_id TEXT
                            REFERENCES hive_group_messages(id) ON DELETE SET NULL,
                        turn_id TEXT,
                        idempotency_key TEXT,
                        created_at TEXT NOT NULL,
                        UNIQUE (group_id, seq),
                        UNIQUE (group_id, idempotency_key)
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_group_messages_turn
                        ON hive_group_messages(turn_id)
                        WHERE turn_id IS NOT NULL;

                    CREATE TABLE IF NOT EXISTS hive_group_turns (
                        id TEXT PRIMARY KEY,
                        group_id TEXT NOT NULL
                            REFERENCES hive_groups(id) ON DELETE CASCADE,
                        trigger_message_id TEXT NOT NULL
                            REFERENCES hive_group_messages(id) ON DELETE CASCADE,
                        execution_mode TEXT NOT NULL
                            CHECK (execution_mode IN ('workbench', 'roundtable', 'direct')),
                        policy_json TEXT NOT NULL
                            CHECK (json_valid(policy_json)),
                        speaker_plan_json TEXT NOT NULL
                            CHECK (json_valid(speaker_plan_json)),
                        next_speaker_index INTEGER NOT NULL DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'running'
                            CHECK (status IN (
                                'running', 'completed', 'partial', 'failed', 'cancelled'
                            )),
                        member_outcomes_json TEXT
                            CHECK (
                                member_outcomes_json IS NULL
                                OR json_valid(member_outcomes_json)
                            ),
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_group_turns_group
                        ON hive_group_turns(group_id, started_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_hive_group_turns_running
                        ON hive_group_turns(group_id)
                        WHERE status = 'running';

                    CREATE TABLE IF NOT EXISTS hive_member_cursors (
                        group_id TEXT NOT NULL
                            REFERENCES hive_groups(id) ON DELETE CASCADE,
                        worker_id TEXT NOT NULL,
                        last_seen_seq INTEGER NOT NULL DEFAULT 0,
                        last_spoke_seq INTEGER NOT NULL DEFAULT 0,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (group_id, worker_id)
                    );
                    "#,
                )
                .context("Migration 64: create Hive group tables")?;

            // Nullable group linkage for member runs of a group turn.
            if Self::table_exists(&group_tx, "hive_runs") {
                for column in ["group_id", "group_turn_id", "trigger_message_id"] {
                    if !Self::column_exists(&group_tx, "hive_runs", column) {
                        group_tx
                            .execute_batch(&format!(
                                "ALTER TABLE hive_runs ADD COLUMN {column} TEXT;"
                            ))
                            .with_context(|| format!("Migration 64: add hive_runs.{column}"))?;
                    }
                }
                group_tx.execute_batch(
                    "CREATE INDEX IF NOT EXISTS idx_hive_runs_group_turn
                        ON hive_runs(group_turn_id) WHERE group_turn_id IS NOT NULL;",
                )?;
            }

            group_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (64)",
                [],
            )?;
            group_tx.commit()?;
        }

        // Migration 65: Durable Worker-to-Worker delivery ledger.
        //
        // hive_deliveries generalizes hive_control_outbox (dedupe key, status,
        // attempts, available_at) into one row per message-per-recipient. The
        // daemon pump claims due rows and delivers by enqueueing a run on the
        // recipient Worker's DM lane or by steering its active run. hive_runs
        // gains kind 'worker_message' for those wake runs.
        if current_version < 65 {
            info!("Running migration 65: Hive delivery ledger");

            let runs_table_exists: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'
                     )",
                    [],
                    |row| row.get(0),
                )
                .context("Migration 65: checking for hive_runs")?;
            if runs_table_exists {
                const GROUP_KINDS: &str =
                    "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn')";
                const DELIVERY_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 65: enabling writable_schema")?;
                let rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [GROUP_KINDS, DELIVERY_KINDS],
                    )
                    .context("Migration 65: extend hive_runs kind CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 65: reloading schema after CHECK edit");
                rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("worker_message") || !runs_sql.contains("kind IN"),
                    "Migration 65 could not extend the hive_runs kind CHECK"
                );
            }

            let delivery_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Hive delivery migration lock")?;
            delivery_tx
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS hive_deliveries (
                        id TEXT PRIMARY KEY,
                        kind TEXT NOT NULL DEFAULT 'worker_message'
                            CHECK (kind IN ('worker_message')),
                        from_worker_id TEXT
                            REFERENCES hive_workers(id) ON DELETE SET NULL,
                        to_worker_id TEXT NOT NULL
                            REFERENCES hive_workers(id),
                        group_id TEXT
                            REFERENCES hive_groups(id) ON DELETE SET NULL,
                        body TEXT NOT NULL,
                        priority TEXT NOT NULL DEFAULT 'normal'
                            CHECK (priority IN ('normal', 'high')),
                        dedupe_key TEXT,
                        status TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN (
                                'pending', 'delivering', 'delivered', 'acked', 'dead_letter'
                            )),
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        max_attempts INTEGER NOT NULL DEFAULT 5
                            CHECK (max_attempts > 0),
                        available_at TEXT NOT NULL,
                        delivered_at TEXT,
                        acked_at TEXT,
                        last_error TEXT,
                        run_id TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE (dedupe_key)
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_deliveries_due
                        ON hive_deliveries(status, available_at)
                        WHERE status IN ('pending', 'delivering');
                    CREATE INDEX IF NOT EXISTS idx_hive_deliveries_to_worker
                        ON hive_deliveries(to_worker_id, created_at);
                    CREATE INDEX IF NOT EXISTS idx_hive_deliveries_from_worker
                        ON hive_deliveries(from_worker_id, created_at)
                        WHERE from_worker_id IS NOT NULL;
                    CREATE INDEX IF NOT EXISTS idx_hive_deliveries_run
                        ON hive_deliveries(run_id)
                        WHERE run_id IS NOT NULL;
                    "#,
                )
                .context("Migration 65: create hive_deliveries")?;
            delivery_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (65)",
                [],
            )?;
            delivery_tx.commit()?;
        }

        // Migration 66: Memory ACL scopes and conversation isolation.
        //
        // Worker-private facts stay on acl_scope='worker' so a group member
        // run cannot inherit another Worker's crew namespace. conversation_id
        // is the opt-in key for group-shared and conversation-private rows.
        if current_version < 66 {
            info!("Running migration 66: Hive memory ACL scopes");
            let memory_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring memory ACL migration lock")?;
            if Self::table_exists(&memory_tx, "agent_memories") {
                if !Self::column_exists(&memory_tx, "agent_memories", "acl_scope") {
                    memory_tx
                        .execute_batch(
                            "ALTER TABLE agent_memories ADD COLUMN acl_scope TEXT NOT NULL DEFAULT 'owner';",
                        )
                        .context("Migration 66: add agent_memories.acl_scope")?;
                }
                if !Self::column_exists(&memory_tx, "agent_memories", "conversation_id") {
                    memory_tx
                        .execute_batch(
                            "ALTER TABLE agent_memories ADD COLUMN conversation_id TEXT;",
                        )
                        .context("Migration 66: add agent_memories.conversation_id")?;
                }
                memory_tx.execute(
                    "UPDATE agent_memories SET acl_scope = 'worker'
                     WHERE namespace = 'crew' AND (acl_scope IS NULL OR acl_scope = 'owner')",
                    [],
                )?;
                memory_tx.execute_batch(
                    "CREATE INDEX IF NOT EXISTS idx_agent_memories_acl
                        ON agent_memories(status, user_id, namespace, namespace_id, acl_scope, conversation_id);",
                )?;
            }
            memory_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (66)",
                [],
            )?;
            memory_tx.commit()?;
        }

        // Migration 67: Always-on Worker heartbeat run kind.
        //
        // hive_runs.kind gains worker_heartbeat so the pump can wake an
        // AlwaysOn Worker's DM on its interval without colliding with
        // scheduled or worker_message rows.
        if current_version < 67 {
            info!("Running migration 67: Hive worker heartbeat run kind");
            let runs_table_exists: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'
                     )",
                    [],
                    |row| row.get(0),
                )
                .context("Migration 67: checking for hive_runs")?;
            if runs_table_exists {
                const DELIVERY_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message')";
                const HEARTBEAT_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 67: enabling writable_schema")?;
                let rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [DELIVERY_KINDS, HEARTBEAT_KINDS],
                    )
                    .context("Migration 67: extend hive_runs kind CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 67: reloading schema after CHECK edit");
                rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("worker_heartbeat") || !runs_sql.contains("kind IN"),
                    "Migration 67 could not extend the hive_runs kind CHECK"
                );
            }
            self.conn.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (67)",
                [],
            )?;
        }

        // Migration 68: Calendar schedules may target a Group room.
        //
        // hive_schedules.group_id is optional and exclusive with worker_id.
        // Occurrences enqueue a group turn through the same delivery path
        // as a user room message.
        if current_version < 68 {
            info!("Running migration 68: Hive schedule group targeting");
            let schedules_exist: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hive_schedules'
                     )",
                    [],
                    |row| row.get(0),
                )
                .context("Migration 68: checking for hive_schedules")?;
            if schedules_exist {
                let has_group_id: bool = self
                    .conn
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1 FROM pragma_table_info('hive_schedules')
                             WHERE name = 'group_id'
                         )",
                        [],
                        |row| row.get(0),
                    )
                    .context("Migration 68: checking hive_schedules.group_id")?;
                if !has_group_id {
                    self.conn
                        .execute_batch(
                            "ALTER TABLE hive_schedules ADD COLUMN group_id TEXT
                                 REFERENCES hive_groups(id) ON DELETE SET NULL;
                             CREATE INDEX IF NOT EXISTS idx_hive_schedules_group
                                 ON hive_schedules(group_id) WHERE group_id IS NOT NULL;",
                        )
                        .context("Migration 68: adding hive_schedules.group_id")?;
                }
            }
            self.conn.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (68)",
                [],
            )?;
        }

        // Migration 69: isolated group Worker lanes and crash-safe Worker
        // introductions.
        //
        // Group member runs use a dedicated session per `(group, Worker)` so
        // private DM history can never become their model transcript. Message
        // idempotency lets an introduction persist its assistant opening
        // exactly once across controller retries. The introduction ledger is
        // separate from the public transcript and records the durable
        // lifecycle without manufacturing a user message.
        if current_version < 69 {
            info!(
                "Running migration 69: Hive Worker conversation isolation and introduction ledger"
            );

            // Adding an enum member to a SQLite CHECK has no ALTER TABLE
            // syntax. Match the established migrations 64/65/67 approach:
            // edit only the table's CREATE SQL so rows, columns, foreign keys,
            // and every index remain physically untouched.
            let runs_table_exists: bool = self
                .conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'
                     )",
                    [],
                    |row| row.get(0),
                )
                .context("Migration 69: checking for hive_runs")?;
            if runs_table_exists {
                const BASE_KINDS: &str =
                    "('dispatch', 'scheduled', 'controller_child', 'legacy_resume')";
                const GROUP_KINDS: &str =
                    "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn')";
                const DELIVERY_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message')";
                const HEARTBEAT_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat')";
                const INTRODUCTION_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 69: enabling writable_schema")?;
                let rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql =
                             replace(
                                 replace(
                                     replace(
                                         replace(sql, ?1, ?5),
                                         ?2, ?5
                                     ),
                                     ?3, ?5
                                 ),
                                 ?4, ?5
                             )
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND (
                               instr(sql, ?1) > 0 OR instr(sql, ?2) > 0
                               OR instr(sql, ?3) > 0 OR instr(sql, ?4) > 0
                           )",
                        [
                            BASE_KINDS,
                            GROUP_KINDS,
                            DELIVERY_KINDS,
                            HEARTBEAT_KINDS,
                            INTRODUCTION_KINDS,
                        ],
                    )
                    .context("Migration 69: extend hive_runs kind CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 69: reloading schema after CHECK edit");
                rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("worker_introduction") || !runs_sql.contains("kind IN"),
                    "Migration 69 could not extend the hive_runs kind CHECK"
                );
            }

            let introduction_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Hive Worker introduction migration lock")?;

            if Self::table_exists(&introduction_tx, "messages") {
                if !Self::column_exists(&introduction_tx, "messages", "idempotency_key") {
                    introduction_tx
                        .execute_batch("ALTER TABLE messages ADD COLUMN idempotency_key TEXT;")
                        .context("Migration 69: add messages.idempotency_key")?;
                }
                introduction_tx.execute_batch(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_session_idempotency
                         ON messages(session_id, idempotency_key)
                         WHERE idempotency_key IS NOT NULL;",
                )?;
            }

            introduction_tx
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS hive_group_worker_lanes (
                        group_id TEXT NOT NULL
                            REFERENCES hive_groups(id) ON DELETE RESTRICT,
                        worker_id TEXT NOT NULL
                            REFERENCES hive_workers(id) ON DELETE RESTRICT,
                        session_id TEXT NOT NULL UNIQUE
                            REFERENCES sessions(id) ON DELETE CASCADE,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (group_id, worker_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_group_worker_lanes_worker
                        ON hive_group_worker_lanes(worker_id, group_id);

                    CREATE TABLE IF NOT EXISTS hive_worker_introductions (
                        worker_id TEXT PRIMARY KEY
                            REFERENCES hive_workers(id) ON DELETE CASCADE,
                        run_id TEXT UNIQUE,
                        status TEXT NOT NULL
                            CHECK (status IN (
                                'queued', 'running', 'awaiting_context', 'review_ready',
                                'confirmed', 'skipped', 'failed', 'needs_recovery'
                            )),
                        prompt_version INTEGER NOT NULL,
                        opening_message_id INTEGER
                            REFERENCES messages(id) ON DELETE SET NULL,
                        proposal_json TEXT
                            CHECK (proposal_json IS NULL OR json_valid(proposal_json)),
                        last_error TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        completed_at TEXT
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_worker_introductions_status
                        ON hive_worker_introductions(status, updated_at);
                    CREATE INDEX IF NOT EXISTS idx_hive_worker_introductions_opening_message
                        ON hive_worker_introductions(opening_message_id)
                        WHERE opening_message_id IS NOT NULL;
                    "#,
                )
                .context("Migration 69: create Hive Worker lane and introduction tables")?;
            introduction_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (69)",
                [],
            )?;
            introduction_tx.commit()?;
        }

        // Migration 70: Freeze the memory namespace selected for each Hive
        // learning candidate at evidence-ingest time.
        //
        // Without this snapshot, a candidate produced in a Worker's private
        // DM/group lane could later be promoted as owner-Shared after a DM
        // rebind. Existing candidates are migrated only when their durable
        // Worker binding can be proven. Unresolved pending legacy candidates
        // are retained in a blocked terminal state instead of defaulting to
        // Shared memory.
        if current_version < 70 {
            info!("Running migration 70: Hive learning memory scopes");
            let learning_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Hive learning memory-scope migration lock")?;
            if Self::table_exists(&learning_tx, "hive_learning_candidates") {
                if !Self::column_exists(
                    &learning_tx,
                    "hive_learning_candidates",
                    "memory_namespace",
                ) {
                    learning_tx.execute_batch(
                        "ALTER TABLE hive_learning_candidates
                         ADD COLUMN memory_namespace TEXT NOT NULL DEFAULT 'shared'
                         CHECK(memory_namespace IN ('shared', 'hive', 'crew'));",
                    )?;
                }
                if !Self::column_exists(
                    &learning_tx,
                    "hive_learning_candidates",
                    "memory_namespace_id",
                ) {
                    learning_tx.execute_batch(
                        "ALTER TABLE hive_learning_candidates
                         ADD COLUMN memory_namespace_id TEXT;",
                    )?;
                }
                if !Self::column_exists(
                    &learning_tx,
                    "hive_learning_candidates",
                    "memory_acl_scope",
                ) {
                    learning_tx.execute_batch(
                        "ALTER TABLE hive_learning_candidates
                         ADD COLUMN memory_acl_scope TEXT NOT NULL DEFAULT 'owner'
                         CHECK(memory_acl_scope IN ('owner', 'worker', 'group', 'conversation'));",
                    )?;
                }
                if !Self::column_exists(
                    &learning_tx,
                    "hive_learning_candidates",
                    "memory_scope_resolved",
                ) {
                    learning_tx.execute_batch(
                        "ALTER TABLE hive_learning_candidates
                         ADD COLUMN memory_scope_resolved INTEGER NOT NULL DEFAULT 0
                         CHECK(memory_scope_resolved IN (0, 1));",
                    )?;
                }

                learning_tx.execute_batch(
                    r#"
                    UPDATE hive_learning_candidates
                    SET memory_namespace = 'crew',
                        memory_namespace_id = COALESCE(
                            (
                                SELECT worker.memory_namespace_id
                                FROM hive_workers worker
                                WHERE worker.dm_session_id =
                                    hive_learning_candidates.evidence_session_id
                                  AND worker.user_id IS
                                    hive_learning_candidates.user_id
                                LIMIT 1
                            ),
                            (
                                SELECT worker.memory_namespace_id
                                FROM hive_group_worker_lanes lane
                                JOIN hive_workers worker ON worker.id = lane.worker_id
                                WHERE lane.session_id =
                                    hive_learning_candidates.evidence_session_id
                                  AND worker.user_id IS
                                    hive_learning_candidates.user_id
                                LIMIT 1
                            )
                        ),
                        memory_acl_scope = 'worker',
                        memory_scope_resolved = 1
                    WHERE EXISTS (
                        SELECT 1 FROM hive_workers worker
                        WHERE worker.dm_session_id =
                            hive_learning_candidates.evidence_session_id
                          AND worker.user_id IS hive_learning_candidates.user_id
                    ) OR EXISTS (
                        SELECT 1
                        FROM hive_group_worker_lanes lane
                        JOIN hive_workers worker ON worker.id = lane.worker_id
                        WHERE lane.session_id =
                            hive_learning_candidates.evidence_session_id
                          AND worker.user_id IS hive_learning_candidates.user_id
                    );

                    UPDATE hive_learning_candidates
                    SET status = 'rejected',
                        reason = reason || '; blocked because the legacy memory scope could not be proven',
                        reviewed_at = COALESCE(reviewed_at, datetime('now'))
                    WHERE memory_scope_resolved = 0
                      AND status = 'pending';

                    CREATE INDEX IF NOT EXISTS idx_hive_learning_candidates_memory_scope
                        ON hive_learning_candidates(
                            user_id, memory_namespace, memory_namespace_id,
                            memory_acl_scope, status
                        );
                    "#,
                )?;
            }
            learning_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (70)",
                [],
            )?;
            learning_tx.commit()?;
        }

        // Migration 71: Reviewed, provenance-fenced Hive Worker Introduction
        // proposals. The proposal itself remains on the one-row lifecycle
        // ledger for efficient UI reads. Every provider review attempt is
        // separately retained in an append-only claim/audit table so a crash,
        // retry, rejection, or superseded transcript cannot silently rewrite
        // the evidence that was shown to the user.
        if current_version < 71 {
            info!("Running migration 71: reviewed Hive Worker Introductions");
            let introduction_review_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Hive Worker Introduction review migration lock")?;

            if Self::table_exists(&introduction_review_tx, "hive_worker_introductions") {
                if !Self::column_exists(
                    &introduction_review_tx,
                    "hive_worker_introductions",
                    "proposal_revision",
                ) {
                    introduction_review_tx.execute_batch(
                        "ALTER TABLE hive_worker_introductions
                         ADD COLUMN proposal_revision INTEGER NOT NULL DEFAULT 0
                         CHECK(proposal_revision >= 0);",
                    )?;
                }
                if !Self::column_exists(
                    &introduction_review_tx,
                    "hive_worker_introductions",
                    "decision_json",
                ) {
                    introduction_review_tx.execute_batch(
                        "ALTER TABLE hive_worker_introductions
                         ADD COLUMN decision_json TEXT
                         CHECK(decision_json IS NULL OR json_valid(decision_json));",
                    )?;
                }

                introduction_review_tx.execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS hive_worker_introduction_reviews (
                        id TEXT PRIMARY KEY,
                        worker_id TEXT NOT NULL
                            REFERENCES hive_workers(id) ON DELETE CASCADE,
                        session_id TEXT NOT NULL
                            REFERENCES sessions(id) ON DELETE CASCADE,
                        status TEXT NOT NULL
                            CHECK(status IN (
                                'claimed', 'gather_more', 'review_ready',
                                'confirmed', 'rejected', 'keep_talking',
                                'failed', 'stale'
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
                        model_key_json TEXT NOT NULL
                            CHECK(json_valid(model_key_json)),
                        model_catalog_revision TEXT,
                        provider_id TEXT NOT NULL,
                        trace_run_id TEXT NOT NULL,
                        provider_call_id TEXT UNIQUE,
                        usage_json TEXT
                            CHECK(usage_json IS NULL OR json_valid(usage_json)),
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
                    CREATE INDEX IF NOT EXISTS idx_hive_worker_introduction_reviews_worker
                        ON hive_worker_introduction_reviews(
                            worker_id, through_message_id, status, updated_at
                        );
                    CREATE INDEX IF NOT EXISTS idx_hive_worker_introduction_reviews_claim
                        ON hive_worker_introduction_reviews(status, claim_expires_at);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_worker_introduction_reviews_trace_run
                        ON hive_worker_introduction_reviews(trace_run_id);
                    "#,
                )?;
            }

            introduction_review_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (71)",
                [],
            )?;
            introduction_review_tx.commit()?;
        }

        // Migration 72: freeze report ownership and Worker-private scope.
        //
        // Reports historically inherited access from the source session's
        // *current* DM/group binding. Rebinding a Worker therefore changed
        // the visibility of already-written reports. Snapshot exact owner,
        // namespace, ACL, and source Worker on the report itself. Legacy rows
        // use every typed Worker link we can prove and abort the migration on
        // conflicting or unresolved Worker evidence rather than silently
        // widening it to owner-shared access.
        if current_version < 72 {
            info!("Running migration 72: immutable report memory scopes");
            let report_scope_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring report memory-scope migration lock")?;
            if Self::table_exists(&report_scope_tx, "reports") {
                let scope_already_immutable: bool = report_scope_tx.query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master
                         WHERE type = 'trigger' AND name = 'reports_scope_immutable'
                     )",
                    [],
                    |row| row.get(0),
                )?;
                if !scope_already_immutable {
                    if !Self::column_exists(&report_scope_tx, "reports", "owner_user_id") {
                        report_scope_tx
                            .execute_batch("ALTER TABLE reports ADD COLUMN owner_user_id TEXT;")?;
                    }
                    if !Self::column_exists(&report_scope_tx, "reports", "memory_namespace") {
                        report_scope_tx.execute_batch(
                            "ALTER TABLE reports
                         ADD COLUMN memory_namespace TEXT NOT NULL DEFAULT 'shared'
                         CHECK(memory_namespace IN ('shared', 'crew'));",
                        )?;
                    }
                    if !Self::column_exists(&report_scope_tx, "reports", "namespace_id") {
                        report_scope_tx.execute_batch(
                            "ALTER TABLE reports
                         ADD COLUMN namespace_id TEXT
                         CHECK(namespace_id IS NULL OR length(trim(namespace_id)) > 0);",
                        )?;
                    }
                    if !Self::column_exists(&report_scope_tx, "reports", "source_worker_id") {
                        let worker_parent_ready =
                            Self::table_exists(&report_scope_tx, "hive_workers")
                                && Self::column_exists(&report_scope_tx, "hive_workers", "id");
                        // SQLite accepts a foreign key declaration whose
                        // parent table is absent, but every later INSERT then
                        // fails while foreign keys are enabled -- even when
                        // the new value is NULL. Preserve the FK in the real
                        // Hive schema and keep legacy ordinary-only fixtures
                        // usable with a fail-closed insert trigger instead.
                        let source_worker_column = if worker_parent_ready {
                            "ALTER TABLE reports
                             ADD COLUMN source_worker_id TEXT
                             REFERENCES hive_workers(id) ON DELETE RESTRICT;"
                        } else {
                            "ALTER TABLE reports
                             ADD COLUMN source_worker_id TEXT;"
                        };
                        report_scope_tx.execute_batch(source_worker_column)?;
                    }
                    if !Self::column_exists(&report_scope_tx, "reports", "acl_scope") {
                        // SQLite stores a column CHECK as part of the table's
                        // overall row constraint, so later updates to any of the
                        // referenced scope columns remain guarded as well.
                        report_scope_tx.execute_batch(
                            "ALTER TABLE reports
                         ADD COLUMN acl_scope TEXT NOT NULL DEFAULT 'owner'
                         CHECK(
                             (acl_scope = 'owner'
                              AND memory_namespace = 'shared'
                              AND namespace_id IS NULL
                              AND source_worker_id IS NULL)
                             OR
                             (acl_scope = 'worker'
                              AND memory_namespace = 'crew'
                              AND namespace_id IS NOT NULL
                              AND length(trim(namespace_id)) > 0
                              AND source_worker_id IS NOT NULL
                              AND length(trim(source_worker_id)) > 0)
                         );",
                        )?;
                    }

                    let report_count: i64 =
                        report_scope_tx
                            .query_row("SELECT COUNT(*) FROM reports", [], |row| row.get(0))?;
                    let sessions_ready = Self::table_exists(&report_scope_tx, "sessions")
                        && ["id", "user_id", "session_type"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "sessions", column)
                        });
                    ensure!(
                        report_count == 0 || sessions_ready,
                        "Migration 72 cannot freeze report ownership without typed source sessions"
                    );
                    if report_count > 0 {
                        let orphan_count: i64 = report_scope_tx.query_row(
                            "SELECT COUNT(*)
                         FROM reports report
                         LEFT JOIN sessions session ON session.id = report.session_id
                         WHERE session.id IS NULL",
                            [],
                            |row| row.get(0),
                        )?;
                        ensure!(
                            orphan_count == 0,
                            "Migration 72 found {orphan_count} reports with no source session"
                        );
                    }

                    report_scope_tx.execute_batch(
                        "DROP TABLE IF EXISTS report_scope_candidates__migration72;
                     CREATE TABLE report_scope_candidates__migration72 (
                         report_id TEXT NOT NULL,
                         worker_id TEXT NOT NULL,
                         evidence_kind TEXT NOT NULL,
                         UNIQUE(report_id, worker_id, evidence_kind)
                     );",
                    )?;
                    if Self::table_exists(&report_scope_tx, "hive_workers")
                        && ["id", "dm_session_id"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_workers", column)
                        })
                    {
                        report_scope_tx.execute(
                            "INSERT OR IGNORE INTO report_scope_candidates__migration72
                             (report_id, worker_id, evidence_kind)
                         SELECT report.id, worker.id, 'current_dm'
                         FROM reports report
                         JOIN hive_workers worker
                           ON worker.dm_session_id = report.session_id",
                            [],
                        )?;
                    }
                    if Self::table_exists(&report_scope_tx, "hive_group_worker_lanes")
                        && ["worker_id", "session_id"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_group_worker_lanes", column)
                        })
                    {
                        report_scope_tx.execute(
                            "INSERT OR IGNORE INTO report_scope_candidates__migration72
                             (report_id, worker_id, evidence_kind)
                         SELECT report.id, lane.worker_id, 'group_lane'
                         FROM reports report
                         JOIN hive_group_worker_lanes lane
                           ON lane.session_id = report.session_id",
                            [],
                        )?;
                    }
                    if Self::table_exists(&report_scope_tx, "hive_controllers")
                        && ["session_id", "worker_id"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_controllers", column)
                        })
                    {
                        report_scope_tx.execute(
                            "INSERT OR IGNORE INTO report_scope_candidates__migration72
                             (report_id, worker_id, evidence_kind)
                         SELECT report.id, controller.worker_id, 'controller'
                         FROM reports report
                         JOIN hive_controllers controller
                           ON controller.session_id = report.session_id
                         WHERE controller.worker_id IS NOT NULL",
                            [],
                        )?;
                    }
                    if Self::table_exists(&report_scope_tx, "hive_runtime_state")
                        && ["session_id", "worker_id"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_runtime_state", column)
                        })
                    {
                        report_scope_tx.execute(
                            "INSERT OR IGNORE INTO report_scope_candidates__migration72
                             (report_id, worker_id, evidence_kind)
                         SELECT report.id, runtime.worker_id, 'runtime_state'
                         FROM reports report
                         JOIN hive_runtime_state runtime
                           ON runtime.session_id = report.session_id
                         WHERE runtime.worker_id IS NOT NULL",
                            [],
                        )?;
                    }
                    let runs_ready = Self::table_exists(&report_scope_tx, "hive_runs")
                        && ["session_id", "worker_id"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_runs", column)
                        });
                    if runs_ready {
                        report_scope_tx.execute(
                            "INSERT OR IGNORE INTO report_scope_candidates__migration72
                             (report_id, worker_id, evidence_kind)
                         SELECT report.id, run.worker_id, 'run'
                         FROM reports report
                         JOIN hive_runs run ON run.session_id = report.session_id
                         WHERE run.worker_id IS NOT NULL",
                            [],
                        )?;
                    }
                    if runs_ready
                        && Self::column_exists(&report_scope_tx, "hive_runs", "controller_id")
                        && Self::table_exists(&report_scope_tx, "hive_controllers")
                        && ["id", "worker_id"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_controllers", column)
                        })
                    {
                        report_scope_tx.execute(
                            "INSERT OR IGNORE INTO report_scope_candidates__migration72
                             (report_id, worker_id, evidence_kind)
                         SELECT report.id, controller.worker_id, 'run_controller'
                         FROM reports report
                         JOIN hive_runs run ON run.session_id = report.session_id
                         JOIN hive_controllers controller ON controller.id = run.controller_id
                         WHERE controller.worker_id IS NOT NULL",
                            [],
                        )?;
                    }

                    let conflict_count: i64 = report_scope_tx.query_row(
                        "SELECT COUNT(*)
                     FROM (
                         SELECT report_id
                         FROM report_scope_candidates__migration72
                         GROUP BY report_id
                         HAVING COUNT(DISTINCT worker_id) > 1
                     )",
                        [],
                        |row| row.get(0),
                    )?;
                    ensure!(
                    conflict_count == 0,
                    "Migration 72 found {conflict_count} reports with conflicting Worker scope evidence"
                );

                    let candidate_count: i64 = report_scope_tx.query_row(
                        "SELECT COUNT(*) FROM report_scope_candidates__migration72",
                        [],
                        |row| row.get(0),
                    )?;
                    let workers_ready = Self::table_exists(&report_scope_tx, "hive_workers")
                        && ["id", "user_id", "memory_namespace_id"]
                            .iter()
                            .all(|column| {
                                Self::column_exists(&report_scope_tx, "hive_workers", column)
                            });
                    ensure!(
                    candidate_count == 0 || workers_ready,
                    "Migration 72 found Worker-authored reports without a resolvable Worker table"
                );
                    if candidate_count > 0 {
                        let invalid_candidate_count: i64 = report_scope_tx.query_row(
                            "SELECT COUNT(*)
                         FROM report_scope_candidates__migration72 candidate
                         JOIN reports report ON report.id = candidate.report_id
                         JOIN sessions session ON session.id = report.session_id
                         LEFT JOIN hive_workers worker ON worker.id = candidate.worker_id
                         WHERE worker.id IS NULL
                            OR session.session_type <> 'hive'
                            OR NOT (worker.user_id IS session.user_id)
                            OR length(trim(worker.memory_namespace_id)) = 0",
                            [],
                            |row| row.get(0),
                        )?;
                        ensure!(
                        invalid_candidate_count == 0,
                        "Migration 72 found {invalid_candidate_count} unresolved Worker report scope claims"
                    );
                    }

                    if report_count > 0 && workers_ready {
                        report_scope_tx.execute(
                            "UPDATE reports
                         SET owner_user_id = (
                                 SELECT session.user_id
                                 FROM sessions session
                                 WHERE session.id = reports.session_id
                             ),
                             memory_namespace = CASE
                                 WHEN EXISTS (
                                     SELECT 1
                                     FROM report_scope_candidates__migration72 candidate
                                     WHERE candidate.report_id = reports.id
                                 ) THEN 'crew'
                                 ELSE 'shared'
                             END,
                             namespace_id = (
                                 SELECT worker.memory_namespace_id
                                 FROM report_scope_candidates__migration72 candidate
                                 JOIN hive_workers worker ON worker.id = candidate.worker_id
                                 WHERE candidate.report_id = reports.id
                                 LIMIT 1
                             ),
                             acl_scope = CASE
                                 WHEN EXISTS (
                                     SELECT 1
                                     FROM report_scope_candidates__migration72 candidate
                                     WHERE candidate.report_id = reports.id
                                 ) THEN 'worker'
                                 ELSE 'owner'
                             END,
                             source_worker_id = (
                                 SELECT candidate.worker_id
                                 FROM report_scope_candidates__migration72 candidate
                                 WHERE candidate.report_id = reports.id
                                 LIMIT 1
                             )",
                            [],
                        )?;
                    } else if report_count > 0 {
                        // A synthetic/old ordinary-only schema may legitimately
                        // have no Hive tables. With no Worker candidates proven,
                        // freeze those reports as exact-owner shared without
                        // compiling a query against a missing parent table.
                        report_scope_tx.execute(
                            "UPDATE reports
                         SET owner_user_id = (
                                 SELECT session.user_id
                                 FROM sessions session
                                 WHERE session.id = reports.session_id
                             ),
                             memory_namespace = 'shared',
                             namespace_id = NULL,
                             acl_scope = 'owner',
                             source_worker_id = NULL",
                            [],
                        )?;
                    }
                    report_scope_tx.execute_batch(
                        r#"
                    DROP TABLE report_scope_candidates__migration72;
                    CREATE INDEX IF NOT EXISTS idx_reports_frozen_reader_scope
                        ON reports(
                            owner_user_id, acl_scope, source_worker_id,
                            project_dir, created_at DESC
                        );
                    CREATE TRIGGER IF NOT EXISTS reports_scope_immutable
                    BEFORE UPDATE OF
                        owner_user_id, memory_namespace, namespace_id,
                        acl_scope, source_worker_id
                    ON reports
                    WHEN NOT (OLD.owner_user_id IS NEW.owner_user_id)
                      OR NOT (OLD.memory_namespace IS NEW.memory_namespace)
                      OR NOT (OLD.namespace_id IS NEW.namespace_id)
                      OR NOT (OLD.acl_scope IS NEW.acl_scope)
                      OR NOT (OLD.source_worker_id IS NEW.source_worker_id)
                    BEGIN
                        SELECT RAISE(ABORT, 'report scope is immutable');
                    END;
                    "#,
                    )?;
                } else {
                    ensure!(
                        [
                            "owner_user_id",
                            "memory_namespace",
                            "namespace_id",
                            "acl_scope",
                            "source_worker_id",
                        ]
                        .iter()
                        .all(|column| Self::column_exists(
                            &report_scope_tx,
                            "reports",
                            column
                        )),
                        "Migration 72 found an immutable report trigger without its scope columns"
                    );
                }
                report_scope_tx.execute_batch(
                    "CREATE INDEX IF NOT EXISTS idx_reports_frozen_reader_scope
                         ON reports(
                             owner_user_id, acl_scope, source_worker_id,
                             project_dir, created_at DESC
                         );
                     DROP TRIGGER IF EXISTS reports_scope_insert_guard;",
                )?;
                let insert_sessions_ready = Self::table_exists(&report_scope_tx, "sessions")
                    && ["id", "user_id", "session_type"]
                        .iter()
                        .all(|column| Self::column_exists(&report_scope_tx, "sessions", column));
                let insert_workers_ready = Self::table_exists(&report_scope_tx, "hive_workers")
                    && ["id", "user_id", "dm_session_id", "memory_namespace_id"]
                        .iter()
                        .all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_workers", column)
                        });
                let insert_lanes_ready =
                    Self::table_exists(&report_scope_tx, "hive_group_worker_lanes")
                        && ["worker_id", "session_id"].iter().all(|column| {
                            Self::column_exists(&report_scope_tx, "hive_group_worker_lanes", column)
                        });
                if insert_sessions_ready && insert_workers_ready && insert_lanes_ready {
                    report_scope_tx.execute_batch(
                        r#"
                        CREATE TRIGGER reports_scope_insert_guard
                        BEFORE INSERT ON reports
                        WHEN NOT EXISTS (
                                 SELECT 1 FROM sessions session
                                 WHERE session.id = NEW.session_id
                                   AND session.user_id IS NEW.owner_user_id
                             )
                          OR (
                              NEW.acl_scope = 'owner'
                              AND (
                                  EXISTS (
                                      SELECT 1 FROM hive_workers worker
                                      WHERE worker.dm_session_id = NEW.session_id
                                  )
                                  OR EXISTS (
                                      SELECT 1 FROM hive_group_worker_lanes lane
                                      WHERE lane.session_id = NEW.session_id
                                  )
                              )
                          )
                          OR (
                              NEW.acl_scope = 'worker'
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM sessions session
                                  JOIN hive_workers worker
                                    ON worker.id = NEW.source_worker_id
                                  WHERE session.id = NEW.session_id
                                    AND session.session_type = 'hive'
                                    AND session.user_id IS NEW.owner_user_id
                                    AND worker.user_id IS NEW.owner_user_id
                                    AND worker.memory_namespace_id = NEW.namespace_id
                                    AND (
                                        worker.dm_session_id = NEW.session_id
                                        OR EXISTS (
                                            SELECT 1
                                            FROM hive_group_worker_lanes lane
                                            WHERE lane.session_id = NEW.session_id
                                              AND lane.worker_id = worker.id
                                        )
                                    )
                              )
                          )
                        BEGIN
                            SELECT RAISE(ABORT, 'report scope does not match source session');
                        END;
                        "#,
                    )?;
                } else if insert_sessions_ready {
                    // Old ordinary-only schemas have no Hive parent tables.
                    // Allow exact-owner shared reports and reject every
                    // Worker-private claim without compiling a trigger that
                    // references tables which do not exist.
                    report_scope_tx.execute_batch(
                        r#"
                        CREATE TRIGGER reports_scope_insert_guard
                        BEFORE INSERT ON reports
                        WHEN NEW.acl_scope <> 'owner'
                          OR NOT EXISTS (
                              SELECT 1 FROM sessions session
                              WHERE session.id = NEW.session_id
                                AND session.user_id IS NEW.owner_user_id
                          )
                        BEGIN
                            SELECT RAISE(ABORT, 'report scope does not match source session');
                        END;
                        "#,
                    )?;
                } else {
                    // With no typed session authority there is no safe scope
                    // to freeze. Existing rows remain readable, but no future
                    // insert may guess at ownership.
                    report_scope_tx.execute_batch(
                        r#"
                        CREATE TRIGGER reports_scope_insert_guard
                        BEFORE INSERT ON reports
                        BEGIN
                            SELECT RAISE(ABORT, 'report scope has no typed source session');
                        END;
                        "#,
                    )?;
                }
            }
            report_scope_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (72)",
                [],
            )?;
            report_scope_tx.commit()?;
        }

        // Migration 73: execution/profile provenance for every first-class
        // Hive Worker. Existing Workers begin at revision 1; profile,
        // document, model, permission, and autonomy changes advance it in the
        // same IMMEDIATE transaction. Status-only pause/resume/archive is a
        // scheduling lifecycle and must not invalidate frozen runnable work.
        if current_version < 73 {
            info!("Running migration 73: revisioned Hive Workers");
            let worker_revision_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Hive Worker revision migration lock")?;
            if Self::table_exists(&worker_revision_tx, "hive_workers")
                && !Self::column_exists(&worker_revision_tx, "hive_workers", "revision")
            {
                worker_revision_tx.execute_batch(
                    "ALTER TABLE hive_workers
                     ADD COLUMN revision INTEGER NOT NULL DEFAULT 1
                     CHECK(revision >= 1);",
                )?;
            }
            worker_revision_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (73)",
                [],
            )?;
            worker_revision_tx.commit()?;
        }

        // Migration 74: authoritative Hive Worker spend and wake governor.
        //
        // Provider calls are a two-phase append-only ledger: a Started row is
        // synchronously committed before the network boundary and exactly one
        // immutable Completed/Unknown outcome may follow. Runtime traces are
        // intentionally not backfilled because they are best-effort and
        // prunable, while session token_count remains conversation accounting.
        if current_version < 74 {
            info!("Running migration 74: Hive Worker spend and wake governor");
            let governor_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Hive Worker governor migration lock")?;
            let default_calls =
                crate::storage::hive_worker_governor::DEFAULT_WORKER_DAILY_CALL_LIMIT;
            let default_tokens =
                crate::storage::hive_worker_governor::DEFAULT_WORKER_DAILY_TOKEN_LIMIT;
            let default_idle_base =
                crate::storage::hive_worker_governor::DEFAULT_WORKER_IDLE_BASE_SECS;
            let default_idle_max =
                crate::storage::hive_worker_governor::DEFAULT_WORKER_IDLE_MAX_SECS;
            let max_calls = crate::storage::hive_worker_governor::MAX_WORKER_DAILY_CALL_LIMIT;
            let max_tokens = crate::storage::hive_worker_governor::MAX_WORKER_DAILY_TOKEN_LIMIT;
            let max_idle = crate::storage::hive_worker_governor::MAX_WORKER_IDLE_SECS;
            let migration_sql = format!(
                r#"
                CREATE TABLE IF NOT EXISTS hive_worker_governor_policies (
                    worker_id TEXT PRIMARY KEY
                        REFERENCES hive_workers(id) ON DELETE CASCADE,
                    revision INTEGER NOT NULL DEFAULT 1
                        CHECK (revision >= 1),
                    daily_call_limit INTEGER NOT NULL DEFAULT {default_calls}
                        CHECK (daily_call_limit > 0 AND daily_call_limit <= {max_calls}),
                    daily_token_limit INTEGER NOT NULL DEFAULT {default_tokens}
                        CHECK (daily_token_limit > 0 AND daily_token_limit <= {max_tokens}),
                    timezone TEXT NOT NULL DEFAULT 'UTC'
                        CHECK (length(timezone) BETWEEN 1 AND 128),
                    quiet_start_minute INTEGER
                        CHECK (quiet_start_minute IS NULL
                               OR quiet_start_minute BETWEEN 0 AND 1439),
                    quiet_end_minute INTEGER
                        CHECK (quiet_end_minute IS NULL
                               OR quiet_end_minute BETWEEN 0 AND 1439),
                    quiet_gap_policy TEXT NOT NULL DEFAULT 'shift_forward'
                        CHECK (quiet_gap_policy IN ('shift_forward', 'skip')),
                    quiet_fold_policy TEXT NOT NULL DEFAULT 'first'
                        CHECK (quiet_fold_policy IN ('first', 'second')),
                    idle_base_secs INTEGER NOT NULL DEFAULT {default_idle_base}
                        CHECK (idle_base_secs > 0 AND idle_base_secs <= {max_idle}),
                    idle_max_secs INTEGER NOT NULL DEFAULT {default_idle_max}
                        CHECK (idle_max_secs >= idle_base_secs
                               AND idle_max_secs <= {max_idle}),
                    tracking_started_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    CHECK (
                        (quiet_start_minute IS NULL AND quiet_end_minute IS NULL)
                        OR (
                            quiet_start_minute IS NOT NULL
                            AND quiet_end_minute IS NOT NULL
                            AND quiet_start_minute <> quiet_end_minute
                        )
                    )
                );

                CREATE TABLE IF NOT EXISTS hive_worker_governor_override_grants (
                    id TEXT PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 256),
                    operation_id TEXT NOT NULL
                        CHECK (length(operation_id) BETWEEN 1 AND 256),
                    worker_id TEXT NOT NULL
                        REFERENCES hive_workers(id) ON DELETE RESTRICT,
                    owner_user_id TEXT,
                    bypass_unresolved_provider_call INTEGER NOT NULL DEFAULT 0
                        CHECK (bypass_unresolved_provider_call IN (0, 1)),
                    bypass_daily_call_cap INTEGER NOT NULL DEFAULT 0
                        CHECK (bypass_daily_call_cap IN (0, 1)),
                    bypass_daily_token_cap INTEGER NOT NULL DEFAULT 0
                        CHECK (bypass_daily_token_cap IN (0, 1)),
                    bypass_quiet_hours INTEGER NOT NULL DEFAULT 0
                        CHECK (bypass_quiet_hours IN (0, 1)),
                    bypass_idle_backoff INTEGER NOT NULL DEFAULT 0
                        CHECK (bypass_idle_backoff IN (0, 1)),
                    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 2048),
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL CHECK (expires_at > created_at),
                    UNIQUE (worker_id, operation_id),
                    CHECK (
                        bypass_unresolved_provider_call = 1
                        OR bypass_daily_call_cap = 1
                        OR bypass_daily_token_cap = 1
                        OR bypass_quiet_hours = 1
                        OR bypass_idle_backoff = 1
                    )
                );

                CREATE TABLE IF NOT EXISTS hive_worker_provider_calls (
                    provider_call_id TEXT PRIMARY KEY
                        CHECK (length(provider_call_id) BETWEEN 1 AND 256),
                    worker_id TEXT NOT NULL
                        REFERENCES hive_workers(id) ON DELETE RESTRICT,
                    worker_revision INTEGER NOT NULL CHECK (worker_revision >= 1),
                    owner_user_id TEXT,
                    session_id TEXT NOT NULL
                        CHECK (length(session_id) BETWEEN 1 AND 256),
                    group_id TEXT,
                    run_id TEXT NOT NULL CHECK (length(run_id) BETWEEN 1 AND 256),
                    run_lease_token TEXT NOT NULL
                        CHECK (length(run_lease_token) BETWEEN 1 AND 256),
                    run_lease_epoch INTEGER NOT NULL CHECK (run_lease_epoch >= 0),
                    run_lease_expires_at TEXT NOT NULL,
                    workflow_goal_id TEXT,
                    workflow_attempt_id TEXT,
                    origin TEXT NOT NULL CHECK (origin IN (
                        'user_dm', 'user_group', 'user_lifecycle_action',
                        'user_workflow_activation', 'manual_run_now', 'scheduled',
                        'heartbeat', 'worker_peer', 'scheduled_group',
                        'workflow_rollover', 'lifecycle_sweep'
                    )),
                    lane_key TEXT NOT NULL CHECK (length(lane_key) BETWEEN 1 AND 512),
                    call_kind TEXT NOT NULL CHECK (length(call_kind) BETWEEN 1 AND 256),
                    provider_id TEXT NOT NULL
                        CHECK (length(provider_id) BETWEEN 1 AND 256),
                    model_id TEXT NOT NULL CHECK (length(model_id) BETWEEN 1 AND 512),
                    model_key_json TEXT NOT NULL CHECK (json_valid(model_key_json)),
                    model_key_fingerprint TEXT NOT NULL
                        CHECK (length(model_key_fingerprint) = 64),
                    model_catalog_revision TEXT,
                    permission_mode TEXT NOT NULL
                        CHECK (permission_mode IN ('supervised', 'autonomous')),
                    pricing_snapshot_json TEXT
                        CHECK (pricing_snapshot_json IS NULL
                               OR json_valid(pricing_snapshot_json)),
                    policy_revision INTEGER NOT NULL CHECK (policy_revision >= 1),
                    timezone TEXT NOT NULL CHECK (length(timezone) BETWEEN 1 AND 128),
                    local_day TEXT NOT NULL CHECK (local_day GLOB '????-??-??'),
                    reserved_tokens INTEGER NOT NULL
                        CHECK (reserved_tokens > 0 AND reserved_tokens <= {max_tokens}),
                    override_grant_id TEXT
                        REFERENCES hive_worker_governor_override_grants(id)
                        ON DELETE RESTRICT,
                    started_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_hive_worker_provider_calls_worker_started
                    ON hive_worker_provider_calls(worker_id, started_at);
                CREATE INDEX IF NOT EXISTS idx_hive_worker_provider_calls_worker_day
                    ON hive_worker_provider_calls(worker_id, local_day);
                CREATE INDEX IF NOT EXISTS idx_hive_worker_provider_calls_run
                    ON hive_worker_provider_calls(run_id);

                CREATE TABLE IF NOT EXISTS hive_worker_provider_call_outcomes (
                    provider_call_id TEXT PRIMARY KEY
                        REFERENCES hive_worker_provider_calls(provider_call_id)
                        ON DELETE RESTRICT,
                    state TEXT NOT NULL CHECK (state IN ('completed', 'unknown')),
                    outcome TEXT NOT NULL CHECK (length(outcome) BETWEEN 1 AND 2048),
                    remote_acceptance TEXT NOT NULL
                        CHECK (remote_acceptance IN (
                            'not_sent', 'possibly_sent', 'acknowledged'
                        )),
                    usage_json TEXT CHECK (usage_json IS NULL OR json_valid(usage_json)),
                    usage_total_tokens INTEGER
                        CHECK (usage_total_tokens IS NULL OR usage_total_tokens >= 0),
                    estimated_cost_microunits INTEGER
                        CHECK (estimated_cost_microunits IS NULL
                               OR estimated_cost_microunits >= 0),
                    unknown_reason TEXT,
                    finished_at TEXT NOT NULL,
                    CHECK (
                        (state = 'unknown'
                         AND usage_json IS NULL
                         AND usage_total_tokens IS NULL
                         AND unknown_reason IS NOT NULL
                         AND length(unknown_reason) BETWEEN 1 AND 2048)
                        OR
                        (state = 'completed' AND unknown_reason IS NULL)
                    )
                );

                CREATE TABLE IF NOT EXISTS hive_worker_governor_override_consumptions (
                    grant_id TEXT PRIMARY KEY
                        REFERENCES hive_worker_governor_override_grants(id)
                        ON DELETE RESTRICT,
                    provider_call_id TEXT NOT NULL UNIQUE
                        REFERENCES hive_worker_provider_calls(provider_call_id)
                        ON DELETE RESTRICT,
                    consumed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS hive_worker_idle_state (
                    worker_id TEXT NOT NULL
                        REFERENCES hive_workers(id) ON DELETE CASCADE,
                    lane_key TEXT NOT NULL CHECK (length(lane_key) BETWEEN 1 AND 512),
                    idle_streak INTEGER NOT NULL DEFAULT 0 CHECK (idle_streak >= 0),
                    not_before TEXT,
                    last_material_at TEXT,
                    last_outcome_run_id TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (worker_id, lane_key)
                );

                CREATE TRIGGER IF NOT EXISTS hive_worker_governor_policy_identity_immutable
                BEFORE UPDATE OF worker_id, tracking_started_at, created_at
                ON hive_worker_governor_policies
                BEGIN
                    SELECT RAISE(ABORT, 'Worker governor policy identity is immutable');
                END;

                CREATE TRIGGER IF NOT EXISTS hive_worker_provider_calls_no_update
                BEFORE UPDATE ON hive_worker_provider_calls
                BEGIN
                    SELECT RAISE(ABORT, 'Worker provider-call Started rows are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS hive_worker_provider_calls_no_delete
                BEFORE DELETE ON hive_worker_provider_calls
                BEGIN
                    SELECT RAISE(ABORT, 'Worker provider-call Started rows are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS hive_worker_provider_call_outcomes_no_update
                BEFORE UPDATE ON hive_worker_provider_call_outcomes
                BEGIN
                    SELECT RAISE(ABORT, 'Worker provider-call outcomes are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS hive_worker_provider_call_outcomes_no_delete
                BEFORE DELETE ON hive_worker_provider_call_outcomes
                BEGIN
                    SELECT RAISE(ABORT, 'Worker provider-call outcomes are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS hive_worker_governor_override_grants_no_update
                BEFORE UPDATE ON hive_worker_governor_override_grants
                BEGIN
                    SELECT RAISE(ABORT, 'Worker governor override grants are immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS hive_worker_governor_override_grants_no_delete
                BEFORE DELETE ON hive_worker_governor_override_grants
                BEGIN
                    SELECT RAISE(ABORT, 'Worker governor override grants are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS hive_worker_governor_override_consumptions_no_update
                BEFORE UPDATE ON hive_worker_governor_override_consumptions
                BEGIN
                    SELECT RAISE(ABORT, 'Worker governor override consumption is immutable');
                END;
                CREATE TRIGGER IF NOT EXISTS hive_worker_governor_override_consumptions_no_delete
                BEFORE DELETE ON hive_worker_governor_override_consumptions
                BEGIN
                    SELECT RAISE(ABORT, 'Worker governor override consumption is append-only');
                END;
                "#,
            );
            governor_tx
                .execute_batch(&migration_sql)
                .context("Migration 74: create Worker governor ledgers")?;
            if Self::table_exists(&governor_tx, "hive_worker_provider_calls")
                && !Self::column_exists(
                    &governor_tx,
                    "hive_worker_provider_calls",
                    "worker_revision",
                )
            {
                governor_tx.execute_batch(
                    "ALTER TABLE hive_worker_provider_calls
                     ADD COLUMN worker_revision INTEGER
                     CHECK(worker_revision IS NULL OR worker_revision >= 1);",
                )?;
            }

            let migration_cutoff = "strftime('%Y-%m-%dT%H:%M:%f000Z', 'now')";
            if Self::table_exists(&governor_tx, "hive_workers") {
                governor_tx.execute_batch(&format!(
                    "INSERT OR IGNORE INTO hive_worker_governor_policies (
                         worker_id, revision, daily_call_limit, daily_token_limit,
                         timezone, quiet_start_minute, quiet_end_minute,
                         quiet_gap_policy, quiet_fold_policy, idle_base_secs,
                         idle_max_secs, tracking_started_at, created_at, updated_at
                     )
                     SELECT id, 1, {default_calls}, {default_tokens}, 'UTC', NULL, NULL,
                            'shift_forward', 'first', {default_idle_base},
                            {default_idle_max}, {migration_cutoff},
                            {migration_cutoff}, {migration_cutoff}
                     FROM hive_workers;
                     CREATE TRIGGER IF NOT EXISTS hive_workers_governor_policy_after_insert
                     AFTER INSERT ON hive_workers
                     BEGIN
                         INSERT INTO hive_worker_governor_policies (
                             worker_id, revision, daily_call_limit, daily_token_limit,
                             timezone, quiet_start_minute, quiet_end_minute,
                             quiet_gap_policy, quiet_fold_policy, idle_base_secs,
                             idle_max_secs, tracking_started_at, created_at, updated_at
                         ) VALUES (
                             NEW.id, 1, {default_calls}, {default_tokens}, 'UTC', NULL,
                             NULL, 'shift_forward', 'first', {default_idle_base},
                             {default_idle_max}, {migration_cutoff},
                             {migration_cutoff}, {migration_cutoff}
                         );
                     END;"
                ))?;
            }

            let override_guard_ready = Self::table_exists(&governor_tx, "hive_workers")
                && ["id", "user_id", "status"]
                    .iter()
                    .all(|column| Self::column_exists(&governor_tx, "hive_workers", column));
            if override_guard_ready {
                governor_tx.execute_batch(
                    r#"
                    CREATE TRIGGER IF NOT EXISTS hive_worker_governor_override_owner_guard
                    BEFORE INSERT ON hive_worker_governor_override_grants
                    WHEN NOT EXISTS (
                        SELECT 1 FROM hive_workers worker
                        WHERE worker.id = NEW.worker_id
                          AND worker.user_id IS NEW.owner_user_id
                          AND worker.status = 'active'
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker governor override identity mismatch');
                    END;
                    "#,
                )?;
            }

            let provider_guard_ready = [
                "hive_workers",
                "hive_runs",
                "hive_controllers",
                "hive_group_worker_lanes",
                "hive_groups",
                "hive_group_members",
            ]
            .iter()
            .all(|table| Self::table_exists(&governor_tx, table))
                && [
                    "id",
                    "user_id",
                    "status",
                    "revision",
                    "dm_session_id",
                    "model",
                    "model_key_json",
                    "model_catalog_revision",
                    "permission_mode",
                ]
                .iter()
                .all(|column| Self::column_exists(&governor_tx, "hive_workers", column))
                && [
                    "id",
                    "controller_id",
                    "session_id",
                    "worker_id",
                    "status",
                    "lease_token",
                    "lease_epoch",
                    "lease_expires_at",
                ]
                .iter()
                .all(|column| Self::column_exists(&governor_tx, "hive_runs", column));
            let provider_guard_ready = provider_guard_ready
                && Self::column_exists(
                    &governor_tx,
                    "hive_worker_provider_calls",
                    "worker_revision",
                );

            if Self::table_exists(&governor_tx, "hive_runs") {
                for (column, definition) in [
                    (
                        "governor_origin",
                        "TEXT CHECK (governor_origin IS NULL OR governor_origin IN (
                            'user_dm', 'user_group', 'user_lifecycle_action',
                            'user_workflow_activation', 'manual_run_now', 'scheduled',
                            'heartbeat', 'worker_peer', 'scheduled_group',
                            'workflow_rollover', 'lifecycle_sweep', 'controller_child'
                        ))",
                    ),
                    (
                        "governor_lane_key",
                        "TEXT CHECK (governor_lane_key IS NULL
                                     OR length(governor_lane_key) BETWEEN 1 AND 512)",
                    ),
                    (
                        "governor_gate_reason",
                        "TEXT CHECK (governor_gate_reason IS NULL OR governor_gate_reason IN (
                            'policy_unavailable', 'unresolved_provider_call',
                            'daily_call_cap_reached', 'daily_token_cap_reached',
                            'quiet_hours', 'idle_backoff'
                        ))",
                    ),
                    ("governor_next_eligible_at", "TEXT"),
                    (
                        "governor_policy_revision",
                        "INTEGER CHECK (governor_policy_revision IS NULL
                                        OR governor_policy_revision >= 0)",
                    ),
                    (
                        "governor_override_id",
                        "TEXT REFERENCES hive_worker_governor_override_grants(id)
                              ON DELETE SET NULL",
                    ),
                ] {
                    if !Self::column_exists(&governor_tx, "hive_runs", column) {
                        governor_tx.execute_batch(&format!(
                            "ALTER TABLE hive_runs ADD COLUMN {column} {definition};"
                        ))?;
                    }
                }
                governor_tx.execute_batch(
                    "CREATE INDEX IF NOT EXISTS idx_hive_runs_governor_gate
                         ON hive_runs(governor_gate_reason, governor_next_eligible_at)
                         WHERE governor_gate_reason IS NOT NULL;",
                )?;
            }

            if provider_guard_ready
                && ["governor_origin", "governor_lane_key"]
                    .iter()
                    .all(|column| Self::column_exists(&governor_tx, "hive_runs", column))
            {
                governor_tx.execute_batch(
                    r#"
                    CREATE TRIGGER IF NOT EXISTS hive_worker_provider_calls_binding_guard
                    BEFORE INSERT ON hive_worker_provider_calls
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM hive_workers worker
                        WHERE worker.id = NEW.worker_id
                          AND worker.user_id IS NEW.owner_user_id
                          AND worker.status = 'active'
                          AND worker.revision = NEW.worker_revision
                          AND worker.model = NEW.model_id
                          AND worker.model_key_json = NEW.model_key_json
                          AND worker.model_catalog_revision IS NEW.model_catalog_revision
                          AND worker.permission_mode = NEW.permission_mode
                          AND (
                              (
                                  NEW.group_id IS NULL
                                  AND worker.dm_session_id = NEW.session_id
                              )
                              OR (
                                  NEW.group_id IS NOT NULL
                                  AND EXISTS (
                                      SELECT 1
                                      FROM hive_group_worker_lanes lane
                                      JOIN hive_groups group_room
                                        ON group_room.id = lane.group_id
                                      JOIN hive_group_members member
                                        ON member.group_id = lane.group_id
                                       AND member.worker_id = lane.worker_id
                                      WHERE lane.group_id = NEW.group_id
                                        AND lane.worker_id = NEW.worker_id
                                        AND lane.session_id = NEW.session_id
                                        AND group_room.status = 'active'
                                  )
                              )
                          )
                          AND EXISTS (
                              SELECT 1
                              FROM hive_runs run
                              JOIN hive_controllers controller
                                ON controller.id = run.controller_id
                              WHERE run.id = NEW.run_id
                                AND COALESCE(run.worker_id, controller.worker_id)
                                    = NEW.worker_id
                                AND run.session_id = NEW.session_id
                                AND run.status = 'running'
                                AND run.lease_token = NEW.run_lease_token
                                AND run.lease_epoch = NEW.run_lease_epoch
                                AND run.lease_expires_at > NEW.started_at
                                AND run.governor_origin = NEW.origin
                                AND run.governor_lane_key = NEW.lane_key
                          )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker provider-call binding mismatch');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_governor_override_consumption_guard
                    BEFORE INSERT ON hive_worker_governor_override_consumptions
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM hive_worker_governor_override_grants grant_row
                        JOIN hive_worker_provider_calls call
                          ON call.provider_call_id = NEW.provider_call_id
                        WHERE grant_row.id = NEW.grant_id
                          AND call.override_grant_id = grant_row.id
                          AND call.worker_id = grant_row.worker_id
                          AND call.owner_user_id IS grant_row.owner_user_id
                          AND grant_row.created_at <= call.started_at
                          AND grant_row.expires_at > call.started_at
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker governor override consumption mismatch');
                    END;
                    "#,
                )?;
            }

            governor_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (74)",
                [],
            )?;
            governor_tx.commit()?;
        }

        // Migration 75: typed, workspace-neutral Worker conversation runs.
        //
        // A Worker run freezes its least-privilege execution context rather
        // than allowing the execution host to substitute its own cwd. Final
        // responses are linked to deterministic canonical rows, and user
        // input accepted during an active response stays durable outside the
        // canonical transcript until that response commits.
        if current_version < 75 {
            info!("Running migration 75: typed Worker conversation execution");

            if Self::table_exists(&self.conn, "hive_runs") {
                const INTRODUCTION_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction')";
                const CONVERSATION_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 75: enabling writable_schema")?;
                let rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [INTRODUCTION_KINDS, CONVERSATION_KINDS],
                    )
                    .context("Migration 75: extend hive_runs kind CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 75: reloading schema after CHECK edit");
                rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master
                     WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("worker_conversation") || !runs_sql.contains("kind IN"),
                    "Migration 75 could not extend the hive_runs kind CHECK"
                );
            }

            let conversation_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Worker conversation migration lock")?;

            if Self::table_exists(&conversation_tx, "hive_runs") {
                for (column, definition) in [
                    (
                        "execution_context_json",
                        "TEXT CHECK (
                            execution_context_json IS NULL OR (
                                json_valid(execution_context_json)
                                AND length(execution_context_json) BETWEEN 2 AND 16384
                            )
                        )",
                    ),
                    (
                        "conversation_through_message_id",
                        "INTEGER REFERENCES messages(id) ON DELETE SET NULL",
                    ),
                    (
                        "response_message_id",
                        "INTEGER REFERENCES messages(id) ON DELETE SET NULL",
                    ),
                    (
                        "response_group_message_id",
                        "TEXT REFERENCES hive_group_messages(id) ON DELETE SET NULL",
                    ),
                    ("response_provider_call_id", "TEXT"),
                ] {
                    if !Self::column_exists(&conversation_tx, "hive_runs", column) {
                        conversation_tx.execute_batch(&format!(
                            "ALTER TABLE hive_runs ADD COLUMN {column} {definition};"
                        ))?;
                    }
                }
                if ["kind", "objective_message_id"]
                    .iter()
                    .all(|column| Self::column_exists(&conversation_tx, "hive_runs", column))
                {
                    conversation_tx.execute_batch(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_worker_conversation_objective
                             ON hive_runs(objective_message_id)
                             WHERE kind = 'worker_conversation'
                               AND objective_message_id IS NOT NULL;",
                    )?;
                }
                conversation_tx.execute_batch(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_runs_response_message
                         ON hive_runs(response_message_id)
                         WHERE response_message_id IS NOT NULL;
                     CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_runs_response_group_message
                         ON hive_runs(response_group_message_id)
                         WHERE response_group_message_id IS NOT NULL;
                     CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_runs_response_provider_call
                         ON hive_runs(response_provider_call_id)
                         WHERE response_provider_call_id IS NOT NULL;",
                )?;
                if ["kind", "status", "worker_id", "available_at"]
                    .iter()
                    .all(|column| Self::column_exists(&conversation_tx, "hive_runs", column))
                {
                    conversation_tx.execute_batch(
                        "CREATE INDEX IF NOT EXISTS idx_hive_worker_conversation_recovery
                             ON hive_runs(kind, status, worker_id, available_at)
                             WHERE kind = 'worker_conversation';",
                    )?;
                }
            }

            // Schema 74 was developed immediately before this migration. A
            // pre-75 preview database may therefore have the provider ledger
            // without the exact Worker revision column. Preserve old rows as
            // provenance-incomplete (NULL, which loading rejects) rather than
            // fabricating the historical revision from mutable current state.
            if Self::table_exists(&conversation_tx, "hive_worker_provider_calls")
                && !Self::column_exists(
                    &conversation_tx,
                    "hive_worker_provider_calls",
                    "worker_revision",
                )
            {
                conversation_tx.execute_batch(
                    "ALTER TABLE hive_worker_provider_calls
                     ADD COLUMN worker_revision INTEGER
                     CHECK(worker_revision IS NULL OR worker_revision >= 1);",
                )?;
            }

            let provider_revision_guard_ready = [
                "hive_worker_provider_calls",
                "hive_workers",
                "hive_runs",
                "hive_controllers",
                "hive_group_worker_lanes",
                "hive_groups",
                "hive_group_members",
            ]
            .iter()
            .all(|table| Self::table_exists(&conversation_tx, table))
                && [
                    "id",
                    "user_id",
                    "status",
                    "revision",
                    "dm_session_id",
                    "model",
                    "model_key_json",
                    "model_catalog_revision",
                    "permission_mode",
                ]
                .iter()
                .all(|column| Self::column_exists(&conversation_tx, "hive_workers", column))
                && [
                    "worker_revision",
                    "owner_user_id",
                    "session_id",
                    "group_id",
                    "run_id",
                    "run_lease_token",
                    "run_lease_epoch",
                    "run_lease_expires_at",
                    "origin",
                    "lane_key",
                    "model_id",
                    "model_key_json",
                    "model_catalog_revision",
                    "permission_mode",
                ]
                .iter()
                .all(|column| {
                    Self::column_exists(&conversation_tx, "hive_worker_provider_calls", column)
                })
                && [
                    "id",
                    "controller_id",
                    "session_id",
                    "worker_id",
                    "status",
                    "lease_token",
                    "lease_epoch",
                    "lease_expires_at",
                    "governor_origin",
                    "governor_lane_key",
                    "execution_context_json",
                ]
                .iter()
                .all(|column| Self::column_exists(&conversation_tx, "hive_runs", column));
            if provider_revision_guard_ready {
                conversation_tx.execute_batch(
                    r#"
                    DROP TRIGGER IF EXISTS hive_worker_provider_calls_binding_guard;
                    CREATE TRIGGER hive_worker_provider_calls_binding_guard
                    BEFORE INSERT ON hive_worker_provider_calls
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM hive_workers worker
                        WHERE worker.id = NEW.worker_id
                          AND worker.user_id IS NEW.owner_user_id
                          AND worker.status = 'active'
                          AND worker.revision = NEW.worker_revision
                          AND worker.model = NEW.model_id
                          AND worker.model_key_json = NEW.model_key_json
                          AND worker.model_catalog_revision IS NEW.model_catalog_revision
                          AND worker.permission_mode = NEW.permission_mode
                          AND (
                              (
                                  NEW.group_id IS NULL
                                  AND worker.dm_session_id = NEW.session_id
                              )
                              OR (
                                  NEW.group_id IS NOT NULL
                                  AND EXISTS (
                                      SELECT 1
                                      FROM hive_group_worker_lanes lane
                                      JOIN hive_groups group_room
                                        ON group_room.id = lane.group_id
                                      JOIN hive_group_members member
                                        ON member.group_id = lane.group_id
                                       AND member.worker_id = lane.worker_id
                                      WHERE lane.group_id = NEW.group_id
                                        AND lane.worker_id = NEW.worker_id
                                        AND lane.session_id = NEW.session_id
                                        AND group_room.status = 'active'
                                  )
                              )
                          )
                          AND EXISTS (
                              SELECT 1
                              FROM hive_runs run
                              JOIN hive_controllers controller
                                ON controller.id = run.controller_id
                              WHERE run.id = NEW.run_id
                                AND COALESCE(run.worker_id, controller.worker_id)
                                    = NEW.worker_id
                                AND run.session_id = NEW.session_id
                                AND run.status = 'running'
                                AND run.lease_token = NEW.run_lease_token
                                AND run.lease_epoch = NEW.run_lease_epoch
                                AND run.lease_expires_at > NEW.started_at
                                AND run.governor_origin = NEW.origin
                                AND run.governor_lane_key = NEW.lane_key
                                AND CAST(json_extract(
                                    run.execution_context_json,
                                    '$.mode.worker_revision'
                                ) AS INTEGER) = NEW.worker_revision
                          )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker provider-call binding mismatch');
                    END;
                    "#,
                )?;
            }

            let input_dependencies_ready = [
                "hive_workers",
                "hive_controllers",
                "sessions",
                "hive_runs",
                "messages",
            ]
            .iter()
            .all(|table| Self::table_exists(&conversation_tx, table));
            if input_dependencies_ready {
                conversation_tx.execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS hive_worker_conversation_inputs (
                        id TEXT PRIMARY KEY CHECK(length(id) BETWEEN 1 AND 256),
                        worker_id TEXT NOT NULL
                            REFERENCES hive_workers(id) ON DELETE CASCADE,
                        owner_user_id TEXT,
                        session_id TEXT NOT NULL
                            REFERENCES sessions(id) ON DELETE CASCADE,
                        request_id TEXT NOT NULL
                            CHECK(length(request_id) BETWEEN 1 AND 256),
                        accepted_while_run_id TEXT NOT NULL
                            REFERENCES hive_runs(id) ON DELETE RESTRICT,
                        content_json TEXT NOT NULL CHECK(
                            json_valid(content_json)
                            AND length(content_json) BETWEEN 2 AND 262144
                        ),
                        state TEXT NOT NULL
                            CHECK(state IN ('staged', 'materialized')),
                        canonical_message_id INTEGER UNIQUE
                            REFERENCES messages(id) ON DELETE RESTRICT,
                        assigned_run_id TEXT
                            REFERENCES hive_runs(id) ON DELETE RESTRICT,
                        accepted_at TEXT NOT NULL,
                        materialized_at TEXT,
                        UNIQUE(session_id, request_id),
                        CHECK(
                            (state = 'staged'
                             AND canonical_message_id IS NULL
                             AND assigned_run_id IS NULL
                             AND materialized_at IS NULL)
                            OR
                            (state = 'materialized'
                             AND canonical_message_id IS NOT NULL
                             AND assigned_run_id IS NOT NULL
                             AND materialized_at IS NOT NULL)
                        )
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_worker_conversation_inputs_staged
                        ON hive_worker_conversation_inputs(session_id, accepted_at, id)
                        WHERE state = 'staged';

                    CREATE TRIGGER IF NOT EXISTS hive_worker_conversation_inputs_insert_guard
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

                    CREATE TRIGGER IF NOT EXISTS hive_worker_conversation_inputs_identity_immutable
                    BEFORE UPDATE OF id, worker_id, owner_user_id, session_id, request_id,
                                     accepted_while_run_id, content_json, accepted_at
                    ON hive_worker_conversation_inputs
                    WHEN OLD.id IS NOT NEW.id
                      OR OLD.worker_id IS NOT NEW.worker_id
                      OR OLD.owner_user_id IS NOT NEW.owner_user_id
                      OR OLD.session_id IS NOT NEW.session_id
                      OR OLD.request_id IS NOT NEW.request_id
                      OR OLD.accepted_while_run_id IS NOT NEW.accepted_while_run_id
                      OR OLD.content_json IS NOT NEW.content_json
                      OR OLD.accepted_at IS NOT NEW.accepted_at
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker conversation input identity is immutable');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_conversation_inputs_no_delete
                    BEFORE DELETE ON hive_worker_conversation_inputs
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker conversation input ledger is append-only');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_conversation_inputs_transition_guard
                    BEFORE UPDATE OF state, canonical_message_id, assigned_run_id, materialized_at
                    ON hive_worker_conversation_inputs
                    WHEN NOT (
                        OLD.state = 'staged'
                        AND NEW.state = 'materialized'
                        AND OLD.canonical_message_id IS NULL
                        AND NEW.canonical_message_id IS NOT NULL
                        AND OLD.assigned_run_id IS NULL
                        AND NEW.assigned_run_id IS NOT NULL
                        AND OLD.materialized_at IS NULL
                        AND NEW.materialized_at IS NOT NULL
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker conversation input transition');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_conversation_inputs_materialize_guard
                    BEFORE UPDATE OF state, canonical_message_id, assigned_run_id, materialized_at
                    ON hive_worker_conversation_inputs
                    WHEN NEW.state = 'materialized' AND NOT EXISTS (
                        SELECT 1
                        FROM messages message
                        JOIN hive_runs assigned ON assigned.id = NEW.assigned_run_id
                        JOIN hive_runs completed
                          ON completed.id = NEW.accepted_while_run_id
                        WHERE message.id = NEW.canonical_message_id
                          AND message.session_id = NEW.session_id
                          AND message.role = 'user'
                          AND assigned.kind = 'worker_conversation'
                          AND assigned.worker_id = NEW.worker_id
                          AND assigned.session_id = NEW.session_id
                          AND assigned.objective_message_id = NEW.canonical_message_id
                          AND assigned.conversation_through_message_id
                              = NEW.canonical_message_id
                          AND completed.worker_id = NEW.worker_id
                          AND completed.session_id = NEW.session_id
                          AND completed.response_message_id IS NOT NULL
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid materialized Worker input binding');
                    END;
                    "#,
                )?;
            }

            let run_guard_ready = [
                "hive_runs",
                "hive_workers",
                "hive_controllers",
                "sessions",
                "messages",
                "hive_group_worker_lanes",
            ]
            .iter()
            .all(|table| Self::table_exists(&conversation_tx, table))
                && [
                    "worker_id",
                    "objective_message_id",
                    "group_id",
                    "group_turn_id",
                    "trigger_message_id",
                    "governor_origin",
                    "governor_lane_key",
                    "execution_context_json",
                    "conversation_through_message_id",
                    "response_message_id",
                    "response_group_message_id",
                    "response_provider_call_id",
                ]
                .iter()
                .all(|column| Self::column_exists(&conversation_tx, "hive_runs", column));
            if run_guard_ready {
                conversation_tx.execute_batch(
                    r#"
                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_context_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.worker_id IS NOT NULL AND (
                        NEW.execution_context_json IS NULL
                        OR json_extract(NEW.execution_context_json, '$.schema_version') <> 1
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            NOT IN (
                                'worker_conversation_neutral',
                                'worker_workspace_attached'
                            )
                        OR json_extract(NEW.execution_context_json, '$.mode.worker_id')
                            IS NOT NEW.worker_id
                        OR CAST(json_extract(
                            NEW.execution_context_json, '$.mode.worker_revision'
                        ) AS INTEGER) <> (
                            SELECT worker.revision FROM hive_workers worker
                            WHERE worker.id = NEW.worker_id
                        )
                        OR NEW.governor_origin IS NULL
                        OR NEW.governor_lane_key IS NULL
                        OR (
                            json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                                = 'direct_message'
                            AND NEW.governor_lane_key <> 'dm'
                        )
                        OR (
                            json_extract(NEW.execution_context_json, '$.mode.lane.kind') = 'group'
                            AND NEW.governor_lane_key <> 'group:' || json_extract(
                                NEW.execution_context_json, '$.mode.lane.group_id'
                            )
                        )
                        OR json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                            NOT IN ('direct_message', 'group')
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker run has no exact typed execution binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_response_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.response_message_id IS NOT NULL
                      OR NEW.response_group_message_id IS NOT NULL
                      OR NEW.response_provider_call_id IS NOT NULL
                    BEGIN
                        SELECT RAISE(ABORT, 'new Worker run cannot have a response linkage');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_legacy_resume_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.kind = 'legacy_resume' AND NEW.worker_id IS NOT NULL
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker conversations cannot use legacy_resume');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_conversation_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.kind = 'worker_conversation' AND (
                        NEW.worker_id IS NULL
                        OR NEW.session_id IS NULL
                        OR NEW.objective_message_id IS NULL
                        OR NEW.conversation_through_message_id IS NULL
                        OR NEW.objective_message_id <> NEW.conversation_through_message_id
                        OR NEW.group_id IS NOT NULL
                        OR NEW.group_turn_id IS NOT NULL
                        OR NEW.trigger_message_id IS NOT NULL
                        OR NEW.governor_origin <> 'user_dm'
                        OR NEW.governor_lane_key <> 'dm'
                        OR json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                            <> 'direct_message'
                        OR NOT EXISTS (
                            SELECT 1
                            FROM hive_workers worker
                            JOIN hive_controllers controller
                              ON controller.id = NEW.controller_id
                            JOIN sessions session ON session.id = NEW.session_id
                            JOIN messages objective
                              ON objective.id = NEW.objective_message_id
                            WHERE worker.id = NEW.worker_id
                              AND worker.status = 'active'
                              AND worker.dm_session_id = NEW.session_id
                              AND controller.worker_id = worker.id
                              AND controller.session_id = session.id
                              AND controller.user_id IS worker.user_id
                              AND controller.status = 'active'
                              AND session.user_id IS worker.user_id
                              AND session.session_type = 'hive'
                              AND objective.session_id = session.id
                              AND objective.role = 'user'
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker conversation run binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_binding_immutable
                    BEFORE UPDATE OF worker_id, session_id, objective_message_id,
                                     execution_context_json,
                                     conversation_through_message_id,
                                     governor_origin, governor_lane_key
                    ON hive_runs
                    WHEN OLD.worker_id IS NOT NEW.worker_id
                      OR (
                          OLD.worker_id IS NOT NULL AND (
                              OLD.session_id IS NOT NEW.session_id
                              OR OLD.objective_message_id IS NOT NEW.objective_message_id
                              OR OLD.execution_context_json IS NOT NEW.execution_context_json
                              OR OLD.conversation_through_message_id
                                  IS NOT NEW.conversation_through_message_id
                              OR OLD.governor_origin IS NOT NEW.governor_origin
                              OR OLD.governor_lane_key IS NOT NEW.governor_lane_key
                          )
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker run execution binding is immutable');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_response_message_link_guard
                    BEFORE UPDATE OF response_message_id ON hive_runs
                    WHEN NEW.response_message_id IS NOT NULL AND (
                        NEW.response_provider_call_id IS NULL OR NOT EXISTS (
                        SELECT 1 FROM messages message
                        WHERE message.id = NEW.response_message_id
                          AND message.session_id = NEW.session_id
                          AND message.role = 'assistant'
                          AND (
                              message.idempotency_key =
                                  'worker-run:' || NEW.id || ':assistant:final'
                              OR (
                                  NEW.kind = 'worker_conversation'
                                  AND NEW.worker_id IS NOT NULL
                                  AND NEW.objective_message_id IS NOT NULL
                                  AND message.idempotency_key =
                                      'introduction:' || NEW.worker_id || ':user:'
                                      || NEW.objective_message_id || ':context-response'
                              )
                          )
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker run response message');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_response_tuple_guard
                    BEFORE UPDATE OF response_message_id, response_provider_call_id ON hive_runs
                    WHEN (NEW.response_message_id IS NULL)
                         <> (NEW.response_provider_call_id IS NULL)
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker response message and provider provenance must link atomically');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_response_message_immutable
                    BEFORE UPDATE OF response_message_id, response_group_message_id,
                                     response_provider_call_id ON hive_runs
                    WHEN (OLD.response_message_id IS NOT NULL
                          AND OLD.response_message_id IS NOT NEW.response_message_id)
                      OR (OLD.response_group_message_id IS NOT NULL
                          AND OLD.response_group_message_id
                              IS NOT NEW.response_group_message_id)
                      OR (OLD.response_provider_call_id IS NOT NULL
                          AND OLD.response_provider_call_id
                              IS NOT NEW.response_provider_call_id)
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker run response linkage is immutable');
                    END;
                    "#,
                )?;
            }

            let response_provider_guard_ready = run_guard_ready
                && [
                    "hive_worker_provider_calls",
                    "hive_worker_provider_call_outcomes",
                    "hive_worker_introductions",
                ]
                .iter()
                .all(|table| Self::table_exists(&conversation_tx, table));
            if response_provider_guard_ready {
                conversation_tx.execute_batch(
                    r#"
                    CREATE TRIGGER IF NOT EXISTS hive_runs_response_provider_call_guard
                    BEFORE UPDATE OF response_provider_call_id ON hive_runs
                    WHEN NEW.response_provider_call_id IS NOT NULL AND NOT EXISTS (
                        SELECT 1
                        FROM hive_worker_provider_calls call
                        LEFT JOIN hive_worker_provider_call_outcomes outcome
                          ON outcome.provider_call_id = call.provider_call_id
                        WHERE call.provider_call_id = NEW.response_provider_call_id
                          AND call.worker_id = NEW.worker_id
                          AND call.session_id = NEW.session_id
                          AND call.run_id = NEW.id
                          AND call.run_lease_token = NEW.lease_token
                          AND call.run_lease_epoch = NEW.lease_epoch
                          AND call.call_kind IN (
                              'agent_turn', 'worker_introduction_onboarding'
                          )
                          AND (
                              outcome.provider_call_id IS NULL
                              OR (
                                  outcome.state = 'completed'
                                  AND outcome.outcome = 'completed'
                                  AND outcome.remote_acceptance = 'acknowledged'
                              )
                              OR (
                                  outcome.state = 'completed'
                                  AND outcome.outcome = 'semantic_invalid'
                                  AND outcome.remote_acceptance = 'acknowledged'
                                  AND NEW.kind = 'worker_conversation'
                                  AND NEW.group_id IS NULL
                                  AND NEW.objective_message_id IS NOT NULL
                                  AND EXISTS (
                                      SELECT 1
                                      FROM hive_worker_introductions introduction
                                      JOIN messages objective
                                        ON objective.id = NEW.objective_message_id
                                      WHERE introduction.worker_id = NEW.worker_id
                                        AND introduction.status = 'awaiting_context'
                                        AND introduction.opening_message_id IS NOT NULL
                                        AND objective.session_id = NEW.session_id
                                        AND objective.role = 'user'
                                  )
                              )
                          )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker response provider provenance');
                    END;
                    "#,
                )?;
            }

            let group_response_guard_ready =
                run_guard_ready && Self::table_exists(&conversation_tx, "hive_group_messages");
            if group_response_guard_ready {
                conversation_tx.execute_batch(
                    r#"
                    CREATE TRIGGER IF NOT EXISTS hive_runs_group_response_link_guard
                    BEFORE UPDATE OF response_group_message_id ON hive_runs
                    WHEN NEW.response_group_message_id IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM hive_group_messages message
                        WHERE message.id = NEW.response_group_message_id
                          AND message.group_id = NEW.group_id
                          AND message.turn_id = NEW.group_turn_id
                          AND message.sender_kind = 'worker'
                          AND message.sender_worker_id = NEW.worker_id
                          AND message.sender_run_id = NEW.id
                          AND message.idempotency_key =
                              'group-turn:' || NEW.group_turn_id
                              || ':worker:' || NEW.worker_id
                              || ':run:' || NEW.id || ':final'
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker group response message');
                    END;
                    "#,
                )?;
            }

            conversation_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (75)",
                [],
            )?;
            conversation_tx.commit()?;
        }

        // Migration 76: durable Hive Worker Workflow Goal attempts.
        //
        // A Workflow run is bound to exactly one canonical Goal attempt and
        // an attached Worker workspace.  Its terminal authority is a bounded,
        // append-only outcome record, never an ordinary assistant message and
        // never a model-selected Goal/plan/step identifier.
        //
        // This migration also repairs preview databases which were stamped 75
        // while the response-provider provenance column and its guards were
        // still being developed.  Version stamps are not treated as proof
        // that those safety objects exist.
        if current_version < 76 {
            info!("Running migration 76: durable Hive Worker Workflow Goals");

            if Self::table_exists(&self.conn, "hive_runs") {
                const CONVERSATION_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation')";
                const WORKFLOW_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 76: enabling writable_schema")?;
                let rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [CONVERSATION_KINDS, WORKFLOW_KINDS],
                    )
                    .context("Migration 76: extend hive_runs kind CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 76: reloading schema after CHECK edit");
                rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master
                     WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("worker_workflow") || !runs_sql.contains("kind IN"),
                    "Migration 76 could not extend the hive_runs kind CHECK"
                );
            }

            let workflow_worker_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Worker Workflow migration lock")?;

            if Self::table_exists(&workflow_worker_tx, "hive_runs") {
                // Compatibility catch-up for schema-75 preview databases.
                if !Self::column_exists(
                    &workflow_worker_tx,
                    "hive_runs",
                    "response_provider_call_id",
                ) {
                    workflow_worker_tx.execute_batch(
                        "ALTER TABLE hive_runs ADD COLUMN response_provider_call_id TEXT;",
                    )?;
                }
                if !Self::column_exists(&workflow_worker_tx, "hive_runs", "workflow_goal_id") {
                    workflow_worker_tx.execute_batch(
                        "ALTER TABLE hive_runs ADD COLUMN workflow_goal_id TEXT
                             REFERENCES workflow_goals(id) ON DELETE RESTRICT;",
                    )?;
                }
                if !Self::column_exists(&workflow_worker_tx, "hive_runs", "workflow_attempt_id") {
                    workflow_worker_tx.execute_batch(
                        "ALTER TABLE hive_runs ADD COLUMN workflow_attempt_id TEXT
                             REFERENCES workflow_execution_attempts(id) ON DELETE RESTRICT;",
                    )?;
                }
                workflow_worker_tx.execute_batch(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_runs_response_provider_call
                         ON hive_runs(response_provider_call_id)
                         WHERE response_provider_call_id IS NOT NULL;
                     CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_worker_workflow_attempt
                         ON hive_runs(workflow_attempt_id)
                         WHERE workflow_attempt_id IS NOT NULL;
                     CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_worker_workflow_one_nonterminal
                         ON hive_runs(workflow_goal_id)
                         WHERE kind = 'worker_workflow'
                           AND status IN (
                               'queued', 'leased', 'running', 'sleeping',
                               'awaiting_input', 'retry_wait', 'recovery_required'
                           );
                     CREATE INDEX IF NOT EXISTS idx_hive_worker_workflow_recovery
                         ON hive_runs(kind, status, workflow_goal_id, available_at)
                         WHERE kind = 'worker_workflow';",
                )?;
            }

            let workflow_dependencies_ready = [
                "hive_runs",
                "hive_workers",
                "hive_controllers",
                "hive_worker_introductions",
                "hive_worker_provider_calls",
                "hive_worker_provider_call_outcomes",
                "sessions",
                "workflow_goals",
                "workflow_plan_revisions",
                "workflow_plan_steps",
                "workflow_step_dependencies",
                "workflow_execution_attempts",
            ]
            .iter()
            .all(|table| Self::table_exists(&workflow_worker_tx, table));

            if workflow_dependencies_ready {
                workflow_worker_tx.execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS hive_worker_goal_outcomes (
                        run_id TEXT PRIMARY KEY
                            REFERENCES hive_runs(id) ON DELETE RESTRICT,
                        worker_id TEXT NOT NULL
                            REFERENCES hive_workers(id) ON DELETE RESTRICT,
                        owner_user_id TEXT,
                        session_id TEXT NOT NULL
                            REFERENCES sessions(id) ON DELETE RESTRICT,
                        workflow_goal_id TEXT NOT NULL
                            REFERENCES workflow_goals(id) ON DELETE RESTRICT,
                        workflow_attempt_id TEXT NOT NULL UNIQUE
                            REFERENCES workflow_execution_attempts(id) ON DELETE RESTRICT,
                        plan_revision_id TEXT NOT NULL
                            REFERENCES workflow_plan_revisions(id) ON DELETE RESTRICT,
                        step_id TEXT NOT NULL
                            REFERENCES workflow_plan_steps(id) ON DELETE RESTRICT,
                        workspace_dir TEXT NOT NULL
                            CHECK(length(workspace_dir) BETWEEN 1 AND 16384),
                        provider_call_ids_json TEXT NOT NULL CHECK(
                            json_valid(provider_call_ids_json)
                            AND json_type(provider_call_ids_json) = 'array'
                            AND json_array_length(provider_call_ids_json) BETWEEN 1 AND 256
                        ),
                        outcome TEXT NOT NULL CHECK(outcome IN (
                            'progressed', 'blocked', 'failed',
                            'cancelled', 'budget_exhausted', 'needs_attention'
                        )),
                        evidence_json TEXT NOT NULL CHECK(
                            json_valid(evidence_json)
                            AND json_type(evidence_json) = 'array'
                            AND json_array_length(evidence_json) <= 32
                            AND length(evidence_json) <= 131072
                        ),
                        effect_json TEXT NOT NULL CHECK(
                            json_valid(effect_json) AND length(effect_json) <= 16384
                        ),
                        counters_json TEXT NOT NULL CHECK(
                            json_valid(counters_json) AND length(counters_json) <= 4096
                        ),
                        no_progress_fingerprint TEXT,
                        no_progress_streak INTEGER NOT NULL DEFAULT 0
                            CHECK(no_progress_streak BETWEEN 0 AND 3),
                        committed_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_worker_goal_outcomes_goal
                        ON hive_worker_goal_outcomes(
                            workflow_goal_id, committed_at DESC, run_id
                        );

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_outcomes_no_update
                    BEFORE UPDATE ON hive_worker_goal_outcomes
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Goal outcomes are immutable');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_outcomes_no_delete
                    BEFORE DELETE ON hive_worker_goal_outcomes
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Goal outcomes are append-only');
                    END;

                    DROP TRIGGER IF EXISTS hive_runs_worker_context_insert_guard;
                    CREATE TRIGGER hive_runs_worker_context_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.worker_id IS NOT NULL AND (
                        NEW.execution_context_json IS NULL
                        OR json_extract(NEW.execution_context_json, '$.schema_version') <> 1
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            NOT IN (
                                'worker_conversation_neutral',
                                'worker_workspace_attached',
                                'worker_goal'
                            )
                        OR json_extract(NEW.execution_context_json, '$.mode.worker_id')
                            IS NOT NEW.worker_id
                        OR CAST(json_extract(
                            NEW.execution_context_json, '$.mode.worker_revision'
                        ) AS INTEGER) <> (
                            SELECT worker.revision FROM hive_workers worker
                            WHERE worker.id = NEW.worker_id
                        )
                        OR NEW.governor_origin IS NULL
                        OR NEW.governor_lane_key IS NULL
                        OR (
                            json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                                = 'direct_message'
                            AND NEW.governor_lane_key <> 'dm'
                        )
                        OR (
                            json_extract(NEW.execution_context_json, '$.mode.lane.kind') = 'group'
                            AND NEW.governor_lane_key <> 'group:' || json_extract(
                                NEW.execution_context_json, '$.mode.lane.group_id'
                            )
                        )
                        OR json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                            NOT IN ('direct_message', 'group')
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker run has no exact typed execution binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_non_workflow_link_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.kind <> 'worker_workflow' AND (
                        NEW.workflow_goal_id IS NOT NULL
                        OR NEW.workflow_attempt_id IS NOT NULL
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            = 'worker_goal'
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'non-Workflow run cannot carry Worker Goal authority');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_non_workflow_link_update_guard
                    BEFORE UPDATE OF kind, workflow_goal_id, workflow_attempt_id,
                                     execution_context_json ON hive_runs
                    WHEN NEW.kind <> 'worker_workflow' AND (
                        NEW.workflow_goal_id IS NOT NULL
                        OR NEW.workflow_attempt_id IS NOT NULL
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            = 'worker_goal'
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'non-Workflow run cannot carry Worker Goal authority');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_workflow_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.kind = 'worker_workflow' AND (
                        NEW.worker_id IS NULL
                        OR NEW.session_id IS NULL
                        OR NEW.workflow_goal_id IS NULL
                        OR NEW.workflow_attempt_id IS NULL
                        OR NEW.objective_message_id IS NOT NULL
                        OR NEW.conversation_through_message_id IS NOT NULL
                        OR NEW.group_id IS NOT NULL
                        OR NEW.group_turn_id IS NOT NULL
                        OR NEW.trigger_message_id IS NOT NULL
                        OR NEW.governor_origin NOT IN (
                            'user_workflow_activation', 'workflow_rollover'
                        )
                        OR NEW.governor_lane_key <> 'dm'
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            <> 'worker_goal'
                        OR json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                            <> 'direct_message'
                        OR json_extract(NEW.execution_context_json, '$.mode.goal_id')
                            IS NOT NEW.workflow_goal_id
                        OR json_extract(NEW.execution_context_json, '$.mode.attempt_id')
                            IS NOT NEW.workflow_attempt_id
                        OR json_extract(NEW.execution_context_json, '$.mode.workspace_mode')
                            NOT IN ('selected', 'created')
                        OR json_extract(NEW.execution_context_json, '$.mode.working_dir')
                            NOT GLOB '/*'
                        OR json_extract(NEW.execution_context_json, '$.mode.project_dir')
                            IS NOT json_extract(
                                NEW.execution_context_json, '$.mode.working_dir'
                            )
                        OR NOT EXISTS (
                            SELECT 1
                            FROM hive_workers worker
                            JOIN hive_controllers controller
                              ON controller.id = NEW.controller_id
                            JOIN hive_worker_introductions introduction
                              ON introduction.worker_id = worker.id
                            JOIN sessions session ON session.id = NEW.session_id
                            JOIN workflow_goals goal
                              ON goal.id = NEW.workflow_goal_id
                            JOIN workflow_execution_attempts attempt
                              ON attempt.id = NEW.workflow_attempt_id
                            JOIN workflow_plan_revisions plan
                              ON plan.id = attempt.plan_revision_id
                            JOIN workflow_plan_steps step
                              ON step.id = attempt.step_id
                            WHERE worker.id = NEW.worker_id
                              AND worker.status = 'active'
                              AND worker.dm_session_id = NEW.session_id
                              AND worker.user_id IS session.user_id
                              AND controller.worker_id = worker.id
                              AND controller.session_id = session.id
                              AND controller.user_id IS worker.user_id
                              AND controller.status = 'active'
                              AND introduction.status IN ('confirmed', 'skipped')
                              AND session.session_type = 'hive'
                              AND session.workspace_mode IN ('selected', 'created')
                              AND session.working_dir GLOB '/*'
                              AND session.project_dir = session.working_dir
                              AND json_extract(
                                  NEW.execution_context_json, '$.mode.working_dir'
                              ) = session.working_dir
                              AND goal.session_id = session.id
                              AND goal.status = 'active'
                              AND attempt.goal_id = goal.id
                              AND attempt.status = 'running'
                              AND attempt.goal_revision_at_start = goal.revision
                              AND plan.goal_id = goal.id
                              AND plan.status = 'active'
                              AND step.plan_revision_id = plan.id
                              AND step.status = 'in_progress'
                              AND step.claimed_attempt_id = attempt.id
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM workflow_step_dependencies dependency
                                  JOIN workflow_plan_steps prerequisite
                                    ON prerequisite.id = dependency.depends_on_step_id
                                  WHERE dependency.step_id = step.id
                                    AND prerequisite.plan_revision_id = plan.id
                                    AND prerequisite.status NOT IN ('completed', 'skipped')
                              )
                              AND CAST(json_extract(
                                  NEW.execution_context_json, '$.mode.goal_revision'
                              ) AS INTEGER) = goal.revision
                              AND CAST(json_extract(
                                  NEW.execution_context_json,
                                  '$.mode.workflow_aggregate_revision'
                              ) AS INTEGER) = goal.revision
                              AND json_extract(
                                  NEW.execution_context_json, '$.mode.plan_revision_id'
                              ) = plan.id
                              AND CAST(json_extract(
                                  NEW.execution_context_json,
                                  '$.mode.plan_revision_number'
                              ) AS INTEGER) = plan.revision_number
                              AND json_extract(
                                  NEW.execution_context_json, '$.mode.step_id'
                              ) = step.id
                              AND CAST(json_extract(
                                  NEW.execution_context_json, '$.mode.step_revision'
                              ) AS INTEGER) = step.revision
                              AND json_extract(NEW.config_json, '$.model') = worker.model
                              AND json_extract(NEW.config_json, '$.working_dir')
                                  = session.working_dir
                              AND json_extract(NEW.config_json, '$.project_dir')
                                  = session.project_dir
                              AND NOT EXISTS (
                                  SELECT configured.fullkey,
                                         configured.type,
                                         configured.atom,
                                         COUNT(*)
                                  FROM json_tree(json_extract(
                                      NEW.config_json, '$.model_key'
                                  )) configured
                                  GROUP BY configured.fullkey,
                                           configured.type,
                                           configured.atom
                                  EXCEPT
                                  SELECT persisted.fullkey,
                                         persisted.type,
                                         persisted.atom,
                                         COUNT(*)
                                  FROM json_tree(worker.model_key_json) persisted
                                  GROUP BY persisted.fullkey,
                                           persisted.type,
                                           persisted.atom
                              )
                              AND NOT EXISTS (
                                  SELECT persisted.fullkey,
                                         persisted.type,
                                         persisted.atom,
                                         COUNT(*)
                                  FROM json_tree(worker.model_key_json) persisted
                                  GROUP BY persisted.fullkey,
                                           persisted.type,
                                           persisted.atom
                                  EXCEPT
                                  SELECT configured.fullkey,
                                         configured.type,
                                         configured.atom,
                                         COUNT(*)
                                  FROM json_tree(json_extract(
                                      NEW.config_json, '$.model_key'
                                  )) configured
                                  GROUP BY configured.fullkey,
                                           configured.type,
                                           configured.atom
                              )
                              AND json_extract(
                                  NEW.config_json, '$.model_catalog_revision'
                              ) IS worker.model_catalog_revision
                              AND json_extract(
                                  NEW.config_json, '$.permission_mode'
                              ) = worker.permission_mode
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker Workflow run binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_workflow_link_immutable
                    BEFORE UPDATE OF workflow_goal_id, workflow_attempt_id ON hive_runs
                    WHEN OLD.workflow_goal_id IS NOT NEW.workflow_goal_id
                      OR OLD.workflow_attempt_id IS NOT NEW.workflow_attempt_id
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Workflow linkage is immutable');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_provider_calls_workflow_guard
                    BEFORE INSERT ON hive_worker_provider_calls
                    WHEN (
                        EXISTS (
                            SELECT 1 FROM hive_runs run
                            WHERE run.id = NEW.run_id
                              AND run.kind = 'worker_workflow'
                              AND (
                                  NEW.workflow_goal_id IS NOT run.workflow_goal_id
                                  OR NEW.workflow_attempt_id IS NOT run.workflow_attempt_id
                              )
                        )
                        OR (
                            (NEW.workflow_goal_id IS NOT NULL
                             OR NEW.workflow_attempt_id IS NOT NULL)
                            AND NOT EXISTS (
                                SELECT 1 FROM hive_runs run
                                WHERE run.id = NEW.run_id
                                  AND run.kind = 'worker_workflow'
                            )
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker provider call Workflow binding mismatch');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_outcomes_insert_guard
                    BEFORE INSERT ON hive_worker_goal_outcomes
                    WHEN NOT EXISTS (
                        SELECT 1
                        FROM hive_runs run
                        JOIN workflow_goals goal
                          ON goal.id = NEW.workflow_goal_id
                        JOIN workflow_execution_attempts attempt
                          ON attempt.id = NEW.workflow_attempt_id
                        JOIN workflow_plan_revisions plan
                          ON plan.id = NEW.plan_revision_id
                        JOIN workflow_plan_steps step ON step.id = NEW.step_id
                        JOIN sessions session ON session.id = NEW.session_id
                        JOIN hive_workers worker ON worker.id = NEW.worker_id
                        WHERE run.id = NEW.run_id
                          AND run.kind = 'worker_workflow'
                          AND run.worker_id = worker.id
                          AND run.session_id = session.id
                          AND run.workflow_goal_id = goal.id
                          AND run.workflow_attempt_id = attempt.id
                          AND goal.session_id = session.id
                          AND attempt.goal_id = goal.id
                          AND attempt.plan_revision_id = plan.id
                          AND attempt.step_id = step.id
                          AND step.plan_revision_id = plan.id
                          AND worker.user_id IS NEW.owner_user_id
                          AND session.user_id IS NEW.owner_user_id
                          AND session.working_dir = NEW.workspace_dir
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker Goal outcome binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_workflow_success_guard
                    BEFORE UPDATE OF status ON hive_runs
                    WHEN NEW.kind = 'worker_workflow'
                      AND NEW.status = 'succeeded'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM hive_worker_goal_outcomes outcome
                          WHERE outcome.run_id = NEW.id
                            AND outcome.worker_id = NEW.worker_id
                            AND outcome.session_id = NEW.session_id
                            AND outcome.workflow_goal_id = NEW.workflow_goal_id
                            AND outcome.workflow_attempt_id = NEW.workflow_attempt_id
                            AND NOT EXISTS (
                                SELECT 1
                                FROM json_each(outcome.provider_call_ids_json) call_id
                                LEFT JOIN hive_worker_provider_calls call
                                  ON call.provider_call_id = call_id.value
                                LEFT JOIN hive_worker_provider_call_outcomes call_outcome
                                  ON call_outcome.provider_call_id = call.provider_call_id
                                WHERE call.provider_call_id IS NULL
                                   OR call.run_id <> NEW.id
                                   OR call.worker_id <> NEW.worker_id
                                   OR call.session_id <> NEW.session_id
                                   OR call.workflow_goal_id IS NOT NEW.workflow_goal_id
                                   OR call.workflow_attempt_id IS NOT NEW.workflow_attempt_id
                                   OR call.run_lease_token IS NOT OLD.lease_token
                                   OR call.run_lease_epoch IS NOT OLD.lease_epoch
                                   OR call.call_kind <> 'agent_turn'
                                   OR call_outcome.state IS NOT 'completed'
                                   OR call_outcome.outcome IS NOT 'completed'
                                   OR call_outcome.remote_acceptance IS NOT 'acknowledged'
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                FROM hive_worker_provider_calls call
                                LEFT JOIN hive_worker_provider_call_outcomes call_outcome
                                  ON call_outcome.provider_call_id = call.provider_call_id
                                WHERE call.run_id = NEW.id
                                  AND (
                                      call.worker_id <> NEW.worker_id
                                      OR call.session_id <> NEW.session_id
                                      OR call.workflow_goal_id IS NOT NEW.workflow_goal_id
                                      OR call.workflow_attempt_id IS NOT NEW.workflow_attempt_id
                                      OR call.run_lease_token IS NOT OLD.lease_token
                                      OR call.run_lease_epoch IS NOT OLD.lease_epoch
                                      OR call_outcome.state IS NOT 'completed'
                                      OR call_outcome.remote_acceptance IS NOT 'acknowledged'
                                      OR (
                                          call.call_kind = 'agent_turn'
                                          AND NOT EXISTS (
                                              SELECT 1
                                              FROM json_each(
                                                  outcome.provider_call_ids_json
                                              ) listed
                                              WHERE listed.value = call.provider_call_id
                                          )
                                      )
                                  )
                            )
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Workflow cannot succeed without an exact committed outcome');
                    END;

                    -- Schema-75 compatibility objects.  These are deliberately
                    -- recreated after adding response_provider_call_id so a
                    -- preview database cannot bypass atomic response linkage.
                    DROP TRIGGER IF EXISTS hive_runs_response_insert_guard;
                    CREATE TRIGGER hive_runs_response_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.response_message_id IS NOT NULL
                      OR NEW.response_group_message_id IS NOT NULL
                      OR NEW.response_provider_call_id IS NOT NULL
                    BEGIN
                        SELECT RAISE(ABORT, 'new Worker run cannot have a response linkage');
                    END;

                    DROP TRIGGER IF EXISTS hive_runs_response_message_link_guard;
                    CREATE TRIGGER hive_runs_response_message_link_guard
                    BEFORE UPDATE OF response_message_id ON hive_runs
                    WHEN NEW.response_message_id IS NOT NULL AND (
                        NEW.response_provider_call_id IS NULL OR NOT EXISTS (
                        SELECT 1 FROM messages message
                        WHERE message.id = NEW.response_message_id
                          AND message.session_id = NEW.session_id
                          AND message.role = 'assistant'
                          AND (
                              message.idempotency_key =
                                  'worker-run:' || NEW.id || ':assistant:final'
                              OR (
                                  NEW.kind = 'worker_conversation'
                                  AND NEW.worker_id IS NOT NULL
                                  AND NEW.objective_message_id IS NOT NULL
                                  AND message.idempotency_key =
                                      'introduction:' || NEW.worker_id || ':user:'
                                      || NEW.objective_message_id || ':context-response'
                              )
                          )
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker run response message');
                    END;

                    DROP TRIGGER IF EXISTS hive_runs_response_tuple_guard;
                    CREATE TRIGGER hive_runs_response_tuple_guard
                    BEFORE UPDATE OF response_message_id, response_provider_call_id ON hive_runs
                    WHEN (NEW.response_message_id IS NULL)
                         <> (NEW.response_provider_call_id IS NULL)
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker response message and provider provenance must link atomically');
                    END;

                    DROP TRIGGER IF EXISTS hive_runs_response_provider_call_guard;
                    CREATE TRIGGER hive_runs_response_provider_call_guard
                    BEFORE UPDATE OF response_provider_call_id ON hive_runs
                    WHEN NEW.response_provider_call_id IS NOT NULL AND NOT EXISTS (
                        SELECT 1
                        FROM hive_worker_provider_calls call
                        LEFT JOIN hive_worker_provider_call_outcomes outcome
                          ON outcome.provider_call_id = call.provider_call_id
                        WHERE call.provider_call_id = NEW.response_provider_call_id
                          AND call.worker_id = NEW.worker_id
                          AND call.session_id = NEW.session_id
                          AND call.run_id = NEW.id
                          AND call.run_lease_token = NEW.lease_token
                          AND call.run_lease_epoch = NEW.lease_epoch
                          AND call.call_kind IN (
                              'agent_turn', 'worker_introduction_onboarding'
                          )
                          AND (
                              outcome.provider_call_id IS NULL
                              OR (
                                  outcome.state = 'completed'
                                  AND outcome.outcome = 'completed'
                                  AND outcome.remote_acceptance = 'acknowledged'
                              )
                              OR (
                                  outcome.state = 'completed'
                                  AND outcome.outcome = 'semantic_invalid'
                                  AND outcome.remote_acceptance = 'acknowledged'
                                  AND NEW.kind = 'worker_conversation'
                                  AND NEW.group_id IS NULL
                                  AND NEW.objective_message_id IS NOT NULL
                                  AND EXISTS (
                                      SELECT 1
                                      FROM hive_worker_introductions introduction
                                      JOIN messages objective
                                        ON objective.id = NEW.objective_message_id
                                      WHERE introduction.worker_id = NEW.worker_id
                                        AND introduction.status = 'awaiting_context'
                                        AND introduction.opening_message_id IS NOT NULL
                                        AND objective.session_id = NEW.session_id
                                        AND objective.role = 'user'
                                  )
                              )
                          )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker response provider provenance');
                    END;

                    DROP TRIGGER IF EXISTS hive_runs_response_message_immutable;
                    CREATE TRIGGER hive_runs_response_message_immutable
                    BEFORE UPDATE OF response_message_id, response_group_message_id,
                                     response_provider_call_id ON hive_runs
                    WHEN (OLD.response_message_id IS NOT NULL
                          AND OLD.response_message_id IS NOT NEW.response_message_id)
                      OR (OLD.response_group_message_id IS NOT NULL
                          AND OLD.response_group_message_id
                              IS NOT NEW.response_group_message_id)
                      OR (OLD.response_provider_call_id IS NOT NULL
                          AND OLD.response_provider_call_id
                              IS NOT NEW.response_provider_call_id)
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker run response linkage is immutable');
                    END;

                    "#,
                )?;

                // A schema-75 preview may contain the response provenance
                // column without the group tables. Keep that partial-schema
                // repairable without weakening full installations.
                if Self::table_exists(&workflow_worker_tx, "hive_group_messages") {
                    workflow_worker_tx.execute_batch(
                        r#"
                        DROP TRIGGER IF EXISTS hive_runs_group_response_link_guard;
                        CREATE TRIGGER hive_runs_group_response_link_guard
                        BEFORE UPDATE OF response_group_message_id ON hive_runs
                        WHEN NEW.response_group_message_id IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM hive_group_messages message
                            WHERE message.id = NEW.response_group_message_id
                              AND message.group_id = NEW.group_id
                              AND message.turn_id = NEW.group_turn_id
                              AND message.sender_kind = 'worker'
                              AND message.sender_worker_id = NEW.worker_id
                              AND message.sender_run_id = NEW.id
                              AND message.idempotency_key =
                                  'group-turn:' || NEW.group_turn_id
                                  || ':worker:' || NEW.worker_id
                                  || ':run:' || NEW.id || ':final'
                        )
                        BEGIN
                            SELECT RAISE(ABORT, 'invalid Worker group response message');
                        END;
                        "#,
                    )?;
                }
            }

            workflow_worker_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (76)",
                [],
            )?;
            workflow_worker_tx.commit()?;
        }

        // Migration 77: claimed, governed Hive Worker Introduction reviews.
        //
        // The reviewer used to be launched inline after an onboarding reply
        // and rediscovered by a process-local host sweep.  A review is now a
        // first-class Hive run linked to one immutable transcript snapshot.
        // Its audit row is created in `queued` before the run is visible, and
        // the exact provider Started row is the only provenance that can
        // authorize a terminal review or a successful run.
        if current_version < 77 {
            info!("Running migration 77: durable Hive Worker Introduction review runs");

            let has_hive_runs = Self::table_exists(&self.conn, "hive_runs");
            let has_typed_sessions = Self::table_exists(&self.conn, "sessions");
            let review_run_dependencies_ready = [
                "hive_worker_introduction_reviews",
                "hive_runs",
                "hive_workers",
                "hive_controllers",
                "hive_worker_provider_calls",
                "hive_worker_provider_call_outcomes",
            ]
            .into_iter()
            .all(|table| Self::table_exists(&self.conn, table))
                && [
                    "id",
                    "controller_id",
                    "session_id",
                    "kind",
                    "status",
                    "worker_id",
                    "conversation_through_message_id",
                    "objective_message_id",
                    "response_message_id",
                    "response_provider_call_id",
                    "workflow_goal_id",
                    "workflow_attempt_id",
                    "governor_origin",
                    "governor_lane_key",
                    "lease_token",
                    "lease_epoch",
                ]
                .into_iter()
                .all(|column| Self::column_exists(&self.conn, "hive_runs", column))
                && ["id", "user_id"]
                    .into_iter()
                    .all(|column| Self::column_exists(&self.conn, "hive_workers", column))
                && ["id", "worker_id", "session_id", "user_id"]
                    .into_iter()
                    .all(|column| Self::column_exists(&self.conn, "hive_controllers", column))
                && [
                    "provider_call_id",
                    "call_kind",
                    "run_id",
                    "worker_id",
                    "session_id",
                    "run_lease_token",
                    "run_lease_epoch",
                ]
                .into_iter()
                .all(|column| {
                    Self::column_exists(&self.conn, "hive_worker_provider_calls", column)
                })
                && ["provider_call_id", "state", "outcome", "remote_acceptance"]
                    .into_iter()
                    .all(|column| {
                        Self::column_exists(
                            &self.conn,
                            "hive_worker_provider_call_outcomes",
                            column,
                        )
                    })
                && [
                    "id",
                    "worker_id",
                    "session_id",
                    "status",
                    "trace_run_id",
                    "through_message_id",
                    "provider_call_id",
                    "last_error",
                ]
                .into_iter()
                .all(|column| {
                    Self::column_exists(&self.conn, "hive_worker_introduction_reviews", column)
                });

            // Specialized historical schemas may intentionally omit the Hive
            // execution tables.  Do not partially activate the durable review
            // surface in those databases: every CHECK extension, column, index,
            // and trigger below is one atomic compatibility contract.  A schema
            // that has both typed sessions and hive_runs but is missing one of
            // the coupled authorities is corrupt/incomplete and must fail
            // closed instead of accepting review runs with unenforceable
            // provenance. Older specialized schemas are explicitly allowed to
            // omit sessions while retaining scheduler tables; they receive the
            // version stamp but no durable review surface.
            ensure!(
                !has_hive_runs || !has_typed_sessions || review_run_dependencies_ready,
                "Migration 77 requires the complete Hive Introduction review-run dependency set"
            );

            if review_run_dependencies_ready {
                const WORKFLOW_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow')";
                const REVIEW_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow', 'worker_introduction_review')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 77: enabling writable_schema for Hive run kind")?;
                let rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [WORKFLOW_KINDS, REVIEW_KINDS],
                    )
                    .context("Migration 77: extend hive_runs kind CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 77: reloading Hive run schema");
                rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master
                     WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("worker_introduction_review")
                        || !runs_sql.contains("kind IN"),
                    "Migration 77 could not extend the hive_runs kind CHECK"
                );
            }

            if review_run_dependencies_ready {
                // SQLite cannot add a new value to an inline CHECK. Preserve
                // the existing table and append only the queued value used by
                // the durable materializer.
                const CLAIMED_REVIEW_STATUSES: &str = "'claimed', 'gather_more', 'review_ready'";
                const QUEUED_REVIEW_STATUSES: &str =
                    "'queued', 'claimed', 'gather_more', 'review_ready'";
                const REVIEW_WITHOUT_PROPOSAL_STATUSES: &str =
                    "OR status IN ('claimed', 'gather_more', 'failed', 'stale')";
                const QUEUED_REVIEW_WITHOUT_PROPOSAL_STATUSES: &str =
                    "OR status IN ('queued', 'claimed', 'gather_more', 'failed', 'stale')";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 77: enabling writable_schema for review status")?;
                let first = self.conn.execute(
                    "UPDATE sqlite_master
                     SET sql = replace(sql, ?1, ?2)
                     WHERE type = 'table'
                       AND name = 'hive_worker_introduction_reviews'
                       AND instr(sql, ?1) > 0
                       AND instr(sql, ?2) = 0",
                    [CLAIMED_REVIEW_STATUSES, QUEUED_REVIEW_STATUSES],
                );
                let second = self.conn.execute(
                    "UPDATE sqlite_master
                     SET sql = replace(sql, ?1, ?2)
                     WHERE type = 'table'
                       AND name = 'hive_worker_introduction_reviews'",
                    [
                        REVIEW_WITHOUT_PROPOSAL_STATUSES,
                        QUEUED_REVIEW_WITHOUT_PROPOSAL_STATUSES,
                    ],
                );
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 77: reloading review schema");
                first.context("Migration 77: add queued review status")?;
                second.context("Migration 77: allow queued review without proposal")?;
                restore?;
                let review_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master
                     WHERE type = 'table'
                       AND name = 'hive_worker_introduction_reviews'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    review_sql.contains("'queued', 'claimed', 'gather_more'"),
                    "Migration 77 could not extend the Introduction review status CHECK"
                );
            }

            let review_run_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Introduction review-run migration lock")?;
            if review_run_dependencies_ready {
                if !Self::column_exists(
                    &review_run_tx,
                    "hive_worker_introduction_reviews",
                    "run_id",
                ) {
                    review_run_tx.execute_batch(
                        "ALTER TABLE hive_worker_introduction_reviews
                         ADD COLUMN run_id TEXT
                         REFERENCES hive_runs(id) ON DELETE RESTRICT;",
                    )?;
                }
                if !Self::column_exists(
                    &review_run_tx,
                    "hive_worker_introduction_reviews",
                    "attempt_no",
                ) {
                    review_run_tx.execute_batch(
                        "ALTER TABLE hive_worker_introduction_reviews
                         ADD COLUMN attempt_no INTEGER
                         CHECK(attempt_no IS NULL OR attempt_no >= 1);",
                    )?;
                }
                review_run_tx.execute_batch(
                    r#"
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_worker_introduction_review_run
                        ON hive_worker_introduction_reviews(run_id)
                        WHERE run_id IS NOT NULL;
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_hive_worker_introduction_review_attempt
                        ON hive_worker_introduction_reviews(
                            worker_id, through_message_id, attempt_no
                        ) WHERE run_id IS NOT NULL;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_introduction_review_run_insert_guard
                    BEFORE INSERT ON hive_worker_introduction_reviews
                    WHEN NEW.run_id IS NOT NULL AND (
                        NEW.attempt_no IS NULL
                        OR NEW.status <> 'queued'
                        OR NEW.trace_run_id <> NEW.run_id
                        OR NOT EXISTS (
                            SELECT 1
                            FROM hive_runs run
                            JOIN hive_workers worker ON worker.id = NEW.worker_id
                            JOIN hive_controllers controller
                              ON controller.id = run.controller_id
                            WHERE run.id = NEW.run_id
                              AND run.kind = 'worker_introduction_review'
                              AND run.status = 'queued'
                              AND run.worker_id = NEW.worker_id
                              AND run.session_id = NEW.session_id
                              AND run.conversation_through_message_id = NEW.through_message_id
                              AND run.objective_message_id IS NULL
                              AND run.response_message_id IS NULL
                              AND run.response_provider_call_id IS NULL
                              AND run.workflow_goal_id IS NULL
                              AND run.workflow_attempt_id IS NULL
                              AND run.governor_origin = 'user_lifecycle_action'
                              AND run.governor_lane_key = 'dm'
                              AND controller.worker_id = worker.id
                              AND controller.session_id = NEW.session_id
                              AND controller.user_id IS worker.user_id
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker Introduction review-run binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_introduction_review_run_immutable
                    BEFORE UPDATE OF run_id, attempt_no ON hive_worker_introduction_reviews
                    WHEN OLD.run_id IS NOT NEW.run_id
                      OR OLD.attempt_no IS NOT NEW.attempt_no
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Introduction review-run binding is immutable');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_introduction_review_provider_guard
                    BEFORE UPDATE OF provider_call_id ON hive_worker_introduction_reviews
                    WHEN NEW.run_id IS NOT NULL AND NEW.provider_call_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM hive_worker_provider_calls call
                          JOIN hive_runs run ON run.id = NEW.run_id
                          WHERE call.provider_call_id = NEW.provider_call_id
                            AND call.call_kind = 'worker_introduction_review'
                            AND call.run_id = run.id
                            AND call.worker_id = NEW.worker_id
                            AND call.session_id = NEW.session_id
                            AND call.run_lease_token = run.lease_token
                            AND call.run_lease_epoch = run.lease_epoch
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Introduction review provider provenance');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_introduction_review_call_kind_guard
                    BEFORE INSERT ON hive_worker_provider_calls
                    WHEN (
                        NEW.call_kind = 'worker_introduction_review'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM hive_runs run
                            JOIN hive_worker_introduction_reviews review
                              ON review.run_id = run.id
                            WHERE run.id = NEW.run_id
                              AND run.kind = 'worker_introduction_review'
                              AND run.status = 'running'
                              AND run.lease_token = NEW.run_lease_token
                              AND run.lease_epoch = NEW.run_lease_epoch
                              AND review.status = 'claimed'
                              AND review.provider_call_id IS NULL
                        )
                    ) OR (
                        NEW.call_kind <> 'worker_introduction_review'
                        AND EXISTS (
                            SELECT 1 FROM hive_runs run
                            WHERE run.id = NEW.run_id
                              AND run.kind = 'worker_introduction_review'
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Introduction review provider-call kind');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_introduction_review_success_guard
                    BEFORE UPDATE OF status ON hive_runs
                    WHEN NEW.kind = 'worker_introduction_review'
                      AND NEW.status = 'succeeded'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM hive_worker_introduction_reviews review
                          LEFT JOIN hive_worker_provider_calls call
                            ON call.provider_call_id = review.provider_call_id
                          LEFT JOIN hive_worker_provider_call_outcomes outcome
                            ON outcome.provider_call_id = call.provider_call_id
                          WHERE review.run_id = NEW.id
                            AND review.worker_id = NEW.worker_id
                            AND review.session_id = NEW.session_id
                            AND review.through_message_id = NEW.conversation_through_message_id
                            AND (
                                (
                                    review.status = 'stale'
                                    AND review.provider_call_id IS NULL
                                    AND review.last_error IS NOT NULL
                                    AND NOT EXISTS (
                                        SELECT 1
                                        FROM hive_worker_provider_calls pre_provider_call
                                        WHERE pre_provider_call.run_id = NEW.id
                                          AND pre_provider_call.run_lease_token = OLD.lease_token
                                          AND pre_provider_call.run_lease_epoch = OLD.lease_epoch
                                    )
                                )
                                OR (
                                    review.status IN (
                                        'gather_more', 'review_ready', 'confirmed',
                                        'rejected', 'keep_talking', 'stale'
                                    )
                                    AND call.run_id = NEW.id
                                    AND call.run_lease_token = OLD.lease_token
                                    AND call.run_lease_epoch = OLD.lease_epoch
                                    AND call.call_kind = 'worker_introduction_review'
                                    AND outcome.state = 'completed'
                                    AND outcome.remote_acceptance = 'acknowledged'
                                    AND outcome.outcome IN (
                                        'completed', 'semantic_invalid',
                                        'canonical_commit_stale'
                                    )
                                )
                            )
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'Introduction review cannot succeed without exact committed audit provenance');
                    END;
                    "#,
                )?;
            }
            review_run_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (77)",
                [],
            )?;
            review_run_tx.commit()?;
        }

        // Migration 78: immutable Worker Workflow acceptance authority.
        //
        // A `Progressed` Worker Goal outcome stages one provider-free,
        // unclaimable acceptance run while its exact step remains claimed.
        // Only an exact owner decision or a coupled lifecycle invalidation can
        // terminalize that run. Automatic structural execution remains gated
        // off until a separate network/process/workspace sandbox exists.
        if current_version < 78 {
            info!("Running migration 78: durable Worker Workflow acceptance authority");

            // The acceptance tables are installed transactionally, but the
            // hive_runs CHECK rewrite must be reloaded outside that
            // transaction. Detect either form of prior authority so a
            // damaged/half-present schema cannot be silently stamped 78 when
            // its coupled dependencies are no longer complete.
            let has_acceptance_run_schema: bool = self.conn.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master
                     WHERE type = 'table' AND name = 'hive_runs'
                       AND (
                           instr(sql, 'worker_workflow_acceptance') > 0
                           OR instr(sql, '''workflow_acceptance''') > 0
                       )
                 )",
                [],
                |row| row.get(0),
            )?;
            let has_worker_goal_or_acceptance_authority = [
                "hive_worker_goal_outcomes",
                "hive_worker_goal_acceptance_candidates",
                "hive_worker_goal_acceptance_results",
            ]
            .into_iter()
            .any(|table| Self::table_exists(&self.conn, table))
                || has_acceptance_run_schema;
            let acceptance_dependencies_ready = [
                "hive_runs",
                "hive_workers",
                "hive_controllers",
                "hive_worker_governor_policies",
                "hive_worker_provider_calls",
                "hive_worker_goal_outcomes",
                "sessions",
                "workflow_goals",
                "workflow_goal_criteria",
                "workflow_plan_revisions",
                "workflow_plan_steps",
                "workflow_execution_attempts",
            ]
            .into_iter()
            .all(|table| Self::table_exists(&self.conn, table));
            ensure!(
                !has_worker_goal_or_acceptance_authority || acceptance_dependencies_ready,
                "Migration 78 requires the complete Worker Workflow acceptance dependency set"
            );

            if acceptance_dependencies_ready {
                const REVIEW_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow', 'worker_introduction_review')";
                const ACCEPTANCE_KINDS: &str = "('dispatch', 'scheduled', 'controller_child', 'legacy_resume', 'group_turn', 'worker_message', 'worker_heartbeat', 'worker_introduction', 'worker_conversation', 'worker_workflow', 'worker_introduction_review', 'worker_workflow_acceptance')";
                const OLD_ORIGIN_TAIL: &str =
                    "'workflow_rollover', 'lifecycle_sweep', 'controller_child'";
                const ACCEPTANCE_ORIGIN_TAIL: &str = "'workflow_rollover', 'workflow_acceptance', 'lifecycle_sweep', 'controller_child'";
                self.conn
                    .pragma_update(None, "writable_schema", "ON")
                    .context("Migration 78: enabling writable_schema for acceptance authority")?;
                let kind_rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [REVIEW_KINDS, ACCEPTANCE_KINDS],
                    )
                    .context("Migration 78: extend hive_runs kind CHECK");
                let origin_rewrite = self
                    .conn
                    .execute(
                        "UPDATE sqlite_master SET sql = replace(sql, ?1, ?2)
                         WHERE type = 'table' AND name = 'hive_runs'
                           AND instr(sql, ?1) > 0",
                        [OLD_ORIGIN_TAIL, ACCEPTANCE_ORIGIN_TAIL],
                    )
                    .context("Migration 78: extend hive_runs governor-origin CHECK");
                let restore = self
                    .conn
                    .pragma_update(None, "writable_schema", "RESET")
                    .context("Migration 78: reloading acceptance run schema");
                kind_rewrite?;
                origin_rewrite?;
                restore?;
                let runs_sql: String = self.conn.query_row(
                    "SELECT sql FROM sqlite_master
                     WHERE type = 'table' AND name = 'hive_runs'",
                    [],
                    |row| row.get(0),
                )?;
                ensure!(
                    runs_sql.contains("worker_workflow_acceptance")
                        || !runs_sql.contains("kind IN"),
                    "Migration 78 could not extend the hive_runs kind CHECK"
                );
                ensure!(
                    runs_sql.contains("'workflow_acceptance'")
                        || !runs_sql.contains("governor_origin IN"),
                    "Migration 78 could not extend the governor-origin CHECK"
                );
            }

            let acceptance_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring Worker Workflow acceptance migration lock")?;

            if acceptance_dependencies_ready {
                acceptance_tx.execute_batch(
                    r#"
                    DROP INDEX IF EXISTS idx_hive_worker_workflow_attempt;
                    CREATE UNIQUE INDEX idx_hive_worker_workflow_attempt
                        ON hive_runs(workflow_attempt_id)
                        WHERE kind = 'worker_workflow'
                          AND workflow_attempt_id IS NOT NULL;

                    CREATE TABLE IF NOT EXISTS hive_worker_goal_acceptance_candidates (
                        acceptance_run_id TEXT PRIMARY KEY
                            REFERENCES hive_runs(id) ON DELETE RESTRICT,
                        source_run_id TEXT NOT NULL UNIQUE
                            REFERENCES hive_runs(id) ON DELETE RESTRICT,
                        worker_id TEXT NOT NULL
                            REFERENCES hive_workers(id) ON DELETE RESTRICT,
                        worker_revision INTEGER NOT NULL CHECK(worker_revision >= 1),
                        owner_user_id TEXT,
                        session_id TEXT NOT NULL
                            REFERENCES sessions(id) ON DELETE RESTRICT,
                        workflow_goal_id TEXT NOT NULL
                            REFERENCES workflow_goals(id) ON DELETE RESTRICT,
                        source_attempt_id TEXT NOT NULL UNIQUE
                            REFERENCES workflow_execution_attempts(id) ON DELETE RESTRICT,
                        plan_revision_id TEXT NOT NULL
                            REFERENCES workflow_plan_revisions(id) ON DELETE RESTRICT,
                        plan_revision_number INTEGER NOT NULL
                            CHECK(plan_revision_number >= 1),
                        step_id TEXT NOT NULL
                            REFERENCES workflow_plan_steps(id) ON DELETE RESTRICT,
                        goal_revision INTEGER NOT NULL CHECK(goal_revision >= 1),
                        workflow_aggregate_revision INTEGER NOT NULL
                            CHECK(workflow_aggregate_revision = goal_revision),
                        step_revision INTEGER NOT NULL CHECK(step_revision >= 1),
                        workspace_dir TEXT NOT NULL CHECK(
                            length(workspace_dir) BETWEEN 1 AND 16384
                            AND workspace_dir GLOB '/*'
                        ),
                        acceptance_contract_json TEXT NOT NULL CHECK(
                            json_valid(acceptance_contract_json)
                            AND json_type(acceptance_contract_json) = 'object'
                            AND json_extract(
                                acceptance_contract_json, '$.schema_version'
                            ) = 1
                            AND json_type(
                                acceptance_contract_json, '$.step_specs'
                            ) = 'array'
                            AND json_array_length(
                                acceptance_contract_json, '$.step_specs'
                            ) BETWEEN 1 AND 32
                            AND json_type(
                                acceptance_contract_json, '$.goal_specs'
                            ) = 'array'
                            AND json_array_length(
                                acceptance_contract_json, '$.goal_specs'
                            ) BETWEEN 0 AND 32
                            AND length(acceptance_contract_json) <= 131072
                        ),
                        acceptance_contract_sha256 TEXT NOT NULL CHECK(
                            length(acceptance_contract_sha256) = 64
                            AND acceptance_contract_sha256
                                NOT GLOB '*[^0-9a-f]*'
                        ),
                        source_outcome_sha256 TEXT NOT NULL CHECK(
                            length(source_outcome_sha256) = 64
                            AND source_outcome_sha256 NOT GLOB '*[^0-9a-f]*'
                        ),
                        state TEXT NOT NULL CHECK(state IN (
                            'awaiting_user', 'verifying', 'needs_user',
                            'accepted', 'rejected', 'stale'
                        )),
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        resolved_at TEXT
                    );
                    CREATE INDEX IF NOT EXISTS idx_worker_goal_acceptance_pending
                        ON hive_worker_goal_acceptance_candidates(
                            worker_id, state, created_at, acceptance_run_id
                        ) WHERE state IN (
                            'awaiting_user', 'verifying', 'needs_user'
                        );
                    CREATE INDEX IF NOT EXISTS idx_worker_goal_acceptance_goal
                        ON hive_worker_goal_acceptance_candidates(
                            workflow_goal_id, state, created_at
                        );

                    CREATE TABLE IF NOT EXISTS hive_worker_goal_acceptance_results (
                        acceptance_run_id TEXT PRIMARY KEY
                            REFERENCES hive_worker_goal_acceptance_candidates(
                                acceptance_run_id
                            ) ON DELETE RESTRICT,
                        source_run_id TEXT NOT NULL UNIQUE
                            REFERENCES hive_runs(id) ON DELETE RESTRICT,
                        authority TEXT NOT NULL CHECK(authority IN (
                            'user', 'lifecycle'
                        )),
                        decision TEXT NOT NULL CHECK(decision IN ('accept', 'reject')),
                        reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 4096),
                        criteria_json TEXT NOT NULL CHECK(
                            json_valid(criteria_json)
                            AND json_type(criteria_json) = 'array'
                            AND json_array_length(criteria_json) BETWEEN 0 AND 32
                            AND length(criteria_json) <= 131072
                        ),
                        receipts_json TEXT NOT NULL CHECK(
                            json_valid(receipts_json)
                            AND json_type(receipts_json) = 'array'
                            AND json_array_length(receipts_json) BETWEEN 0 AND 32
                            AND length(receipts_json) <= 131072
                        ),
                        provider_call_ids_json TEXT NOT NULL CHECK(
                            json_valid(provider_call_ids_json)
                            AND json_type(provider_call_ids_json) = 'array'
                            AND json_array_length(provider_call_ids_json) = 0
                        ),
                        resulting_goal_revision INTEGER,
                        resulting_goal_status TEXT,
                        resulting_step_status TEXT,
                        committed_at TEXT NOT NULL,
                        CHECK(authority <> 'lifecycle' OR decision = 'reject'),
                        CHECK(authority <> 'lifecycle' OR json_array_length(criteria_json) = 0),
                        CHECK(json_array_length(receipts_json) = 0),
                        CHECK(
                            (
                                authority = 'user'
                                AND resulting_goal_revision IS NOT NULL
                                AND resulting_goal_revision >= 1
                                AND resulting_goal_status IS NOT NULL
                                AND resulting_goal_status IN (
                                    'active', 'paused', 'completed'
                                )
                                AND resulting_step_status IS NOT NULL
                                AND resulting_step_status IN (
                                    'pending', 'completed'
                                )
                            )
                            OR (
                                authority = 'lifecycle'
                                AND resulting_goal_revision IS NULL
                                AND resulting_goal_status IS NULL
                                AND resulting_step_status IS NULL
                            )
                        )
                    );

                    DROP TRIGGER IF EXISTS hive_runs_worker_context_insert_guard;
                    CREATE TRIGGER hive_runs_worker_context_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.worker_id IS NOT NULL AND (
                        NEW.execution_context_json IS NULL
                        OR json_extract(NEW.execution_context_json, '$.schema_version') <> 1
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            NOT IN (
                                'worker_conversation_neutral',
                                'worker_workspace_attached',
                                'worker_goal',
                                'worker_goal_acceptance'
                            )
                        OR json_extract(NEW.execution_context_json, '$.mode.worker_id')
                            IS NOT NEW.worker_id
                        OR CAST(json_extract(
                            NEW.execution_context_json, '$.mode.worker_revision'
                        ) AS INTEGER) <> (
                            SELECT worker.revision FROM hive_workers worker
                            WHERE worker.id = NEW.worker_id
                        )
                        OR NEW.governor_origin IS NULL
                        OR NEW.governor_lane_key IS NULL
                        OR (
                            json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                                = 'direct_message'
                            AND NEW.governor_lane_key <> 'dm'
                        )
                        OR (
                            json_extract(NEW.execution_context_json, '$.mode.lane.kind') = 'group'
                            AND NEW.governor_lane_key <> 'group:' || json_extract(
                                NEW.execution_context_json, '$.mode.lane.group_id'
                            )
                        )
                        OR json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                            NOT IN ('direct_message', 'group')
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker run has no exact typed execution binding');
                    END;

                    DROP TRIGGER IF EXISTS hive_runs_non_workflow_link_insert_guard;
                    CREATE TRIGGER hive_runs_non_workflow_link_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.kind NOT IN (
                        'worker_workflow', 'worker_workflow_acceptance'
                    ) AND (
                        NEW.workflow_goal_id IS NOT NULL
                        OR NEW.workflow_attempt_id IS NOT NULL
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            IN ('worker_goal', 'worker_goal_acceptance')
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'non-Workflow run cannot carry Worker Goal authority');
                    END;

                    DROP TRIGGER IF EXISTS hive_runs_non_workflow_link_update_guard;
                    CREATE TRIGGER hive_runs_non_workflow_link_update_guard
                    BEFORE UPDATE OF kind, workflow_goal_id, workflow_attempt_id,
                                     execution_context_json ON hive_runs
                    WHEN NEW.kind NOT IN (
                        'worker_workflow', 'worker_workflow_acceptance'
                    ) AND (
                        NEW.workflow_goal_id IS NOT NULL
                        OR NEW.workflow_attempt_id IS NOT NULL
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            IN ('worker_goal', 'worker_goal_acceptance')
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'non-Workflow run cannot carry Worker Goal authority');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_acceptance_insert_guard
                    BEFORE INSERT ON hive_runs
                    WHEN NEW.kind = 'worker_workflow_acceptance' AND (
                        NEW.worker_id IS NULL
                        OR NEW.session_id IS NULL
                        OR NEW.workflow_goal_id IS NULL
                        OR NEW.workflow_attempt_id IS NULL
                        OR NEW.status <> 'awaiting_input'
                        OR NEW.attempt_count <> 0
                        OR NEW.max_attempts <> 1
                        OR NEW.lease_owner IS NOT NULL
                        OR NEW.lease_token IS NOT NULL
                        OR NEW.lease_epoch IS NOT NULL
                        OR NEW.lease_expires_at IS NOT NULL
                        OR NEW.objective_message_id IS NOT NULL
                        OR NEW.conversation_through_message_id IS NOT NULL
                        OR NEW.response_message_id IS NOT NULL
                        OR NEW.response_group_message_id IS NOT NULL
                        OR NEW.response_provider_call_id IS NOT NULL
                        OR NEW.group_id IS NOT NULL
                        OR NEW.group_turn_id IS NOT NULL
                        OR NEW.trigger_message_id IS NOT NULL
                        OR NEW.governor_origin <> 'workflow_acceptance'
                        OR NEW.governor_lane_key <> 'dm'
                        OR json_extract(NEW.execution_context_json, '$.mode.kind')
                            <> 'worker_goal_acceptance'
                        OR json_extract(NEW.execution_context_json, '$.mode.lane.kind')
                            <> 'direct_message'
                        OR json_extract(NEW.execution_context_json, '$.mode.goal_id')
                            IS NOT NEW.workflow_goal_id
                        OR json_extract(
                            NEW.execution_context_json, '$.mode.source_attempt_id'
                        ) IS NOT NEW.workflow_attempt_id
                        OR json_extract(
                            NEW.execution_context_json, '$.mode.workspace_mode'
                        ) NOT IN ('selected', 'created')
                        OR json_extract(NEW.execution_context_json, '$.mode.working_dir')
                            NOT GLOB '/*'
                        OR json_extract(NEW.execution_context_json, '$.mode.project_dir')
                            IS NOT json_extract(
                                NEW.execution_context_json, '$.mode.working_dir'
                            )
                        OR json_type(
                            NEW.execution_context_json, '$.mode.tool_allowlist'
                        ) <> 'array'
                        OR json_array_length(
                            NEW.execution_context_json, '$.mode.tool_allowlist'
                        ) <> 0
                        OR length(json_extract(
                            NEW.execution_context_json,
                            '$.mode.acceptance_contract_sha256'
                        )) <> 64
                        OR json_extract(
                            NEW.execution_context_json,
                            '$.mode.acceptance_contract_sha256'
                        ) GLOB '*[^0-9a-f]*'
                        OR length(json_extract(
                            NEW.execution_context_json, '$.mode.source_outcome_sha256'
                        )) <> 64
                        OR json_extract(
                            NEW.execution_context_json, '$.mode.source_outcome_sha256'
                        ) GLOB '*[^0-9a-f]*'
                        OR NOT EXISTS (
                            SELECT 1
                            FROM hive_runs source
                            JOIN hive_worker_goal_outcomes outcome
                              ON outcome.run_id = source.id
                            JOIN hive_workers worker ON worker.id = source.worker_id
                            JOIN hive_controllers controller
                              ON controller.id = NEW.controller_id
                            JOIN sessions session ON session.id = source.session_id
                            JOIN workflow_goals goal
                              ON goal.id = source.workflow_goal_id
                            JOIN workflow_execution_attempts attempt
                              ON attempt.id = source.workflow_attempt_id
                            JOIN workflow_plan_revisions plan
                              ON plan.id = attempt.plan_revision_id
                            JOIN workflow_plan_steps step ON step.id = attempt.step_id
                            JOIN hive_worker_governor_policies policy
                              ON policy.worker_id = worker.id
                            WHERE source.id = json_extract(
                                      NEW.execution_context_json, '$.mode.source_run_id'
                                  )
                              AND source.kind = 'worker_workflow'
                              AND source.status = 'running'
                              AND outcome.outcome = 'progressed'
                              AND source.worker_id = NEW.worker_id
                              AND source.session_id = NEW.session_id
                              AND source.workflow_goal_id = NEW.workflow_goal_id
                              AND source.workflow_attempt_id = NEW.workflow_attempt_id
                              AND worker.status = 'active'
                              AND worker.dm_session_id = session.id
                              AND worker.user_id IS session.user_id
                              AND worker.revision = CAST(json_extract(
                                  NEW.execution_context_json, '$.mode.worker_revision'
                              ) AS INTEGER)
                              AND controller.worker_id = worker.id
                              AND controller.session_id = session.id
                              AND controller.user_id IS worker.user_id
                              AND controller.status = 'active'
                              AND NEW.governor_policy_revision = policy.revision
                              AND session.session_type = 'hive'
                              AND session.workspace_mode IN ('selected', 'created')
                              AND session.working_dir = session.project_dir
                              AND session.working_dir = json_extract(
                                  NEW.execution_context_json, '$.mode.working_dir'
                              )
                              AND goal.session_id = session.id
                              AND goal.status = 'active'
                              AND goal.revision = CAST(json_extract(
                                  NEW.execution_context_json, '$.mode.goal_revision'
                              ) AS INTEGER)
                              AND goal.revision = CAST(json_extract(
                                  NEW.execution_context_json,
                                  '$.mode.workflow_aggregate_revision'
                              ) AS INTEGER)
                              AND goal.revision = CAST(json_extract(
                                  source.execution_context_json,
                                  '$.mode.goal_revision'
                              ) AS INTEGER) + 1
                              AND attempt.goal_id = goal.id
                              AND attempt.status = 'paused'
                              AND attempt.stop_reason = 'awaiting_acceptance'
                              AND plan.id = json_extract(
                                  NEW.execution_context_json, '$.mode.plan_revision_id'
                              )
                              AND plan.goal_id = goal.id
                              AND plan.status = 'active'
                              AND plan.revision_number = CAST(json_extract(
                                  NEW.execution_context_json,
                                  '$.mode.plan_revision_number'
                              ) AS INTEGER)
                              AND step.id = json_extract(
                                  NEW.execution_context_json, '$.mode.step_id'
                              )
                              AND step.plan_revision_id = plan.id
                              AND step.status = 'in_progress'
                              AND step.claimed_attempt_id = attempt.id
                              AND step.revision = CAST(json_extract(
                                  NEW.execution_context_json, '$.mode.step_revision'
                              ) AS INTEGER)
                        )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker Workflow acceptance run binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_acceptance_immutable
                    BEFORE UPDATE OF kind, controller_id, session_id, worker_id,
                                     workflow_goal_id, workflow_attempt_id,
                                     governor_origin, governor_lane_key,
                                     governor_policy_revision, execution_context_json,
                                     objective_message_id,
                                     conversation_through_message_id,
                                     response_message_id, response_group_message_id,
                                     response_provider_call_id, group_id, group_turn_id,
                                     trigger_message_id, attempt_count, max_attempts,
                                     lease_owner, lease_token, lease_epoch,
                                     lease_expires_at ON hive_runs
                    WHEN OLD.kind = 'worker_workflow_acceptance' AND (
                        OLD.kind IS NOT NEW.kind
                        OR OLD.controller_id IS NOT NEW.controller_id
                        OR OLD.session_id IS NOT NEW.session_id
                        OR OLD.worker_id IS NOT NEW.worker_id
                        OR OLD.workflow_goal_id IS NOT NEW.workflow_goal_id
                        OR OLD.workflow_attempt_id IS NOT NEW.workflow_attempt_id
                        OR OLD.governor_origin IS NOT NEW.governor_origin
                        OR OLD.governor_lane_key IS NOT NEW.governor_lane_key
                        OR OLD.governor_policy_revision
                            IS NOT NEW.governor_policy_revision
                        OR OLD.execution_context_json IS NOT NEW.execution_context_json
                        OR OLD.objective_message_id IS NOT NEW.objective_message_id
                        OR OLD.conversation_through_message_id
                            IS NOT NEW.conversation_through_message_id
                        OR OLD.response_message_id IS NOT NEW.response_message_id
                        OR OLD.response_group_message_id
                            IS NOT NEW.response_group_message_id
                        OR OLD.response_provider_call_id
                            IS NOT NEW.response_provider_call_id
                        OR OLD.group_id IS NOT NEW.group_id
                        OR OLD.group_turn_id IS NOT NEW.group_turn_id
                        OR OLD.trigger_message_id IS NOT NEW.trigger_message_id
                        OR OLD.attempt_count IS NOT NEW.attempt_count
                        OR OLD.max_attempts IS NOT NEW.max_attempts
                        OR OLD.lease_owner IS NOT NEW.lease_owner
                        OR OLD.lease_token IS NOT NEW.lease_token
                        OR OLD.lease_epoch IS NOT NEW.lease_epoch
                        OR OLD.lease_expires_at IS NOT NEW.lease_expires_at
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Workflow acceptance run is immutable and unclaimable');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_acceptance_candidate_insert_guard
                    BEFORE INSERT ON hive_worker_goal_acceptance_candidates
                    WHEN NEW.state <> 'awaiting_user'
                      OR NEW.resolved_at IS NOT NULL
                      OR NOT EXISTS (
                          SELECT 1
                          FROM hive_runs acceptance_run
                          JOIN hive_runs source
                            ON source.id = NEW.source_run_id
                          JOIN hive_worker_goal_outcomes outcome
                            ON outcome.run_id = source.id
                          JOIN hive_workers worker ON worker.id = NEW.worker_id
                          JOIN sessions session ON session.id = NEW.session_id
                          JOIN workflow_goals goal ON goal.id = NEW.workflow_goal_id
                          JOIN workflow_execution_attempts attempt
                            ON attempt.id = NEW.source_attempt_id
                          JOIN workflow_plan_revisions plan
                            ON plan.id = NEW.plan_revision_id
                          JOIN workflow_plan_steps step ON step.id = NEW.step_id
                          WHERE acceptance_run.id = NEW.acceptance_run_id
                            AND acceptance_run.kind = 'worker_workflow_acceptance'
                            AND acceptance_run.status = 'awaiting_input'
                            AND acceptance_run.worker_id = NEW.worker_id
                            AND acceptance_run.session_id = NEW.session_id
                            AND acceptance_run.workflow_goal_id = NEW.workflow_goal_id
                            AND acceptance_run.workflow_attempt_id = NEW.source_attempt_id
                            AND source.kind = 'worker_workflow'
                            AND source.status = 'running'
                            AND source.worker_id = NEW.worker_id
                            AND source.session_id = NEW.session_id
                            AND source.workflow_goal_id = NEW.workflow_goal_id
                            AND source.workflow_attempt_id = NEW.source_attempt_id
                            AND outcome.outcome = 'progressed'
                            AND outcome.worker_id = NEW.worker_id
                            AND outcome.owner_user_id IS NEW.owner_user_id
                            AND outcome.session_id = NEW.session_id
                            AND outcome.workflow_goal_id = NEW.workflow_goal_id
                            AND outcome.workflow_attempt_id = NEW.source_attempt_id
                            AND outcome.plan_revision_id = NEW.plan_revision_id
                            AND outcome.step_id = NEW.step_id
                            AND outcome.workspace_dir = NEW.workspace_dir
                            AND worker.user_id IS NEW.owner_user_id
                            AND worker.revision = NEW.worker_revision
                            AND worker.dm_session_id = session.id
                            AND worker.status = 'active'
                            AND session.user_id IS NEW.owner_user_id
                            AND session.session_type = 'hive'
                            AND session.working_dir = NEW.workspace_dir
                            AND session.project_dir = NEW.workspace_dir
                            AND goal.session_id = session.id
                            AND goal.status = 'active'
                            AND goal.revision = NEW.goal_revision
                            AND NEW.workflow_aggregate_revision = goal.revision
                            AND attempt.goal_id = goal.id
                            AND attempt.plan_revision_id = plan.id
                            AND attempt.step_id = step.id
                            AND attempt.status = 'paused'
                            AND attempt.stop_reason = 'awaiting_acceptance'
                            AND plan.goal_id = goal.id
                            AND plan.revision_number = NEW.plan_revision_number
                            AND plan.status = 'active'
                            AND step.plan_revision_id = plan.id
                            AND step.status = 'in_progress'
                            AND step.claimed_attempt_id = attempt.id
                            AND step.revision = NEW.step_revision
                            AND json_extract(
                                acceptance_run.execution_context_json,
                                '$.mode.source_run_id'
                            ) = source.id
                            AND json_extract(
                                acceptance_run.execution_context_json,
                                '$.mode.acceptance_contract_sha256'
                            ) = NEW.acceptance_contract_sha256
                            AND json_extract(
                                acceptance_run.execution_context_json,
                                '$.mode.source_outcome_sha256'
                            ) = NEW.source_outcome_sha256
                            AND json_extract(
                                acceptance_run.execution_context_json,
                                '$.mode.plan_revision_id'
                            ) = NEW.plan_revision_id
                            AND json_extract(
                                acceptance_run.execution_context_json,
                                '$.mode.step_id'
                            ) = NEW.step_id
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker Goal acceptance candidate binding');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_acceptance_candidate_update_guard
                    BEFORE UPDATE ON hive_worker_goal_acceptance_candidates
                    WHEN OLD.acceptance_run_id IS NOT NEW.acceptance_run_id
                      OR OLD.source_run_id IS NOT NEW.source_run_id
                      OR OLD.worker_id IS NOT NEW.worker_id
                      OR OLD.worker_revision IS NOT NEW.worker_revision
                      OR OLD.owner_user_id IS NOT NEW.owner_user_id
                      OR OLD.session_id IS NOT NEW.session_id
                      OR OLD.workflow_goal_id IS NOT NEW.workflow_goal_id
                      OR OLD.source_attempt_id IS NOT NEW.source_attempt_id
                      OR OLD.plan_revision_id IS NOT NEW.plan_revision_id
                      OR OLD.plan_revision_number IS NOT NEW.plan_revision_number
                      OR OLD.step_id IS NOT NEW.step_id
                      OR OLD.goal_revision IS NOT NEW.goal_revision
                      OR OLD.workflow_aggregate_revision
                          IS NOT NEW.workflow_aggregate_revision
                      OR OLD.step_revision IS NOT NEW.step_revision
                      OR OLD.workspace_dir IS NOT NEW.workspace_dir
                      OR OLD.acceptance_contract_json
                          IS NOT NEW.acceptance_contract_json
                      OR OLD.acceptance_contract_sha256
                          IS NOT NEW.acceptance_contract_sha256
                      OR OLD.source_outcome_sha256 IS NOT NEW.source_outcome_sha256
                      OR OLD.created_at IS NOT NEW.created_at
                      OR OLD.resolved_at IS NOT NULL
                      OR OLD.state NOT IN ('awaiting_user', 'needs_user', 'verifying')
                      OR NEW.state NOT IN ('accepted', 'rejected', 'stale')
                      OR NEW.resolved_at IS NULL
                      OR NEW.updated_at IS NOT NEW.resolved_at
                      OR NOT EXISTS (
                          SELECT 1
                          FROM hive_worker_goal_acceptance_results result
                          WHERE result.acceptance_run_id = OLD.acceptance_run_id
                            AND result.source_run_id = OLD.source_run_id
                            AND (
                                (NEW.state = 'accepted'
                                 AND result.authority = 'user'
                                 AND result.decision = 'accept')
                                OR (NEW.state = 'rejected'
                                    AND result.authority = 'user'
                                    AND result.decision = 'reject')
                                OR (NEW.state = 'stale'
                                    AND result.authority = 'lifecycle'
                                    AND result.decision = 'reject')
                            )
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker Goal acceptance candidate transition');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_acceptance_candidate_no_delete
                    BEFORE DELETE ON hive_worker_goal_acceptance_candidates
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Goal acceptance candidates are append-only');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_acceptance_result_insert_guard
                    BEFORE INSERT ON hive_worker_goal_acceptance_results
                    WHEN json_array_length(NEW.receipts_json) <> 0
                      OR json_array_length(NEW.provider_call_ids_json) <> 0
                      OR (NEW.decision = 'reject'
                          AND json_array_length(NEW.criteria_json) <> 0)
                      OR (NEW.authority = 'lifecycle' AND (
                          NEW.decision <> 'reject'
                          OR json_array_length(NEW.criteria_json) <> 0
                          OR NEW.reason NOT IN (
                              'workflow_goal_cancelled', 'worker_archived'
                          )
                      ))
                      OR EXISTS (
                          SELECT 1 FROM json_each(NEW.criteria_json) criterion
                          WHERE json_type(criterion.value) <> 'object'
                             OR length(json_extract(
                                 criterion.value, '$.criterion_id'
                             )) NOT BETWEEN 1 AND 256
                             OR json_extract(criterion.value, '$.decision')
                                 NOT IN ('passed', 'failed', 'waived')
                             OR json_type(criterion.value, '$.evidence') <> 'array'
                             OR json_array_length(
                                 criterion.value, '$.evidence'
                             ) > 16
                      )
                      OR NOT EXISTS (
                          SELECT 1
                          FROM hive_worker_goal_acceptance_candidates candidate
                          JOIN hive_runs acceptance_run
                            ON acceptance_run.id = candidate.acceptance_run_id
                          WHERE candidate.acceptance_run_id = NEW.acceptance_run_id
                            AND candidate.source_run_id = NEW.source_run_id
                            AND candidate.state IN (
                                'awaiting_user', 'needs_user', 'verifying'
                            )
                            AND (
                                NEW.authority = 'lifecycle'
                                OR candidate.state IN ('awaiting_user', 'needs_user')
                            )
                            AND acceptance_run.kind = 'worker_workflow_acceptance'
                            AND acceptance_run.status = 'awaiting_input'
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker Goal acceptance result authority');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_acceptance_result_no_update
                    BEFORE UPDATE ON hive_worker_goal_acceptance_results
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Goal acceptance results are immutable');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_goal_acceptance_result_no_delete
                    BEFORE DELETE ON hive_worker_goal_acceptance_results
                    BEGIN
                        SELECT RAISE(ABORT, 'Worker Goal acceptance results are append-only');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_acceptance_status_guard
                    BEFORE UPDATE OF status ON hive_runs
                    WHEN OLD.kind = 'worker_workflow_acceptance'
                      AND NEW.status IS NOT OLD.status
                      AND NOT (
                          OLD.status = 'awaiting_input'
                          AND NEW.status = 'succeeded'
                          AND EXISTS (
                              SELECT 1
                              FROM hive_worker_goal_acceptance_candidates candidate
                              JOIN hive_worker_goal_acceptance_results result
                                ON result.acceptance_run_id
                                    = candidate.acceptance_run_id
                              WHERE candidate.acceptance_run_id = OLD.id
                                AND candidate.source_run_id = result.source_run_id
                                AND result.authority = 'user'
                                AND (
                                    (candidate.state = 'accepted'
                                     AND result.decision = 'accept')
                                    OR (candidate.state = 'rejected'
                                        AND result.decision = 'reject')
                                )
                          )
                      )
                      AND NOT (
                          OLD.status = 'awaiting_input'
                          AND NEW.status = 'cancelled'
                          AND EXISTS (
                              SELECT 1
                              FROM hive_worker_goal_acceptance_candidates candidate
                              JOIN hive_worker_goal_acceptance_results result
                                ON result.acceptance_run_id
                                    = candidate.acceptance_run_id
                              WHERE candidate.acceptance_run_id = OLD.id
                                AND candidate.state = 'stale'
                                AND candidate.source_run_id = result.source_run_id
                                AND result.authority = 'lifecycle'
                                AND result.decision = 'reject'
                          )
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'acceptance run requires an exact committed result');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_worker_provider_calls_acceptance_disabled
                    BEFORE INSERT ON hive_worker_provider_calls
                    WHEN EXISTS (
                        SELECT 1 FROM hive_runs run
                        WHERE run.id = NEW.run_id
                          AND run.kind = 'worker_workflow_acceptance'
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'automatic Workflow acceptance is disabled');
                    END;

                    CREATE TRIGGER IF NOT EXISTS hive_runs_worker_workflow_progressed_acceptance_guard
                    BEFORE UPDATE OF status ON hive_runs
                    WHEN NEW.kind = 'worker_workflow'
                      AND NEW.status = 'succeeded'
                      AND EXISTS (
                          SELECT 1 FROM hive_worker_goal_outcomes outcome
                          WHERE outcome.run_id = NEW.id
                            AND outcome.outcome = 'progressed'
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM hive_worker_goal_outcomes outcome
                          JOIN hive_worker_goal_acceptance_candidates candidate
                            ON candidate.source_run_id = outcome.run_id
                          JOIN hive_runs acceptance_run
                            ON acceptance_run.id = candidate.acceptance_run_id
                          WHERE outcome.run_id = NEW.id
                            AND outcome.outcome = 'progressed'
                            AND candidate.worker_id = outcome.worker_id
                            AND candidate.owner_user_id IS outcome.owner_user_id
                            AND candidate.session_id = outcome.session_id
                            AND candidate.workflow_goal_id = outcome.workflow_goal_id
                            AND candidate.source_attempt_id
                                = outcome.workflow_attempt_id
                            AND candidate.plan_revision_id = outcome.plan_revision_id
                            AND candidate.step_id = outcome.step_id
                            AND candidate.workspace_dir = outcome.workspace_dir
                            AND acceptance_run.kind
                                = 'worker_workflow_acceptance'
                            AND acceptance_run.status = 'awaiting_input'
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'Progressed Workflow outcome has no atomic acceptance authority');
                    END;
                    "#,
                )?;
            }
            let terminal_conversation_promotion_ready = [
                "hive_worker_conversation_inputs",
                "hive_runs",
                "hive_workers",
                "hive_controllers",
                "hive_worker_introduction_reviews",
                "hive_worker_governor_override_grants",
                "hive_worker_governor_override_consumptions",
                "hive_worker_provider_calls",
                "hive_worker_provider_call_outcomes",
                "messages",
                "sessions",
            ]
            .iter()
            .all(|table| Self::table_exists(&acceptance_tx, table));
            if terminal_conversation_promotion_ready {
                acceptance_tx.execute_batch(
                    r#"
                    DROP TRIGGER IF EXISTS hive_worker_conversation_inputs_insert_guard;
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
                            AND active.schedule_id IS NULL
                            AND active.occurrence_id IS NULL
                            AND active.group_id IS NULL
                            AND active.workflow_goal_id IS NULL
                            AND active.workflow_attempt_id IS NULL
                            AND controller.worker_id = worker.id
                            AND controller.session_id = session.id
                            AND controller.user_id IS worker.user_id
                            AND controller.status = 'active'
                            AND json_valid(active.execution_context_json)
                            AND json_extract(
                                active.execution_context_json, '$.mode.kind'
                            ) IN (
                                'worker_conversation_neutral',
                                'worker_workspace_attached'
                            )
                            AND json_extract(
                                active.execution_context_json, '$.mode.lane.kind'
                            ) = 'direct_message'
                            AND json_extract(
                                active.execution_context_json, '$.mode.worker_id'
                            ) = active.worker_id
                            AND CAST(json_extract(
                                active.execution_context_json,
                                '$.mode.worker_revision'
                            ) AS INTEGER) = worker.revision
                            AND (
                                (
                                    json_extract(
                                        active.execution_context_json,
                                        '$.mode.kind'
                                    ) = 'worker_conversation_neutral'
                                    AND session.workspace_mode = 'neutral'
                                    AND (
                                        session.working_dir IS NULL
                                        OR session.working_dir = ''
                                    )
                                    AND (
                                        session.project_dir IS NULL
                                        OR session.project_dir = ''
                                    )
                                )
                                OR (
                                    session.workspace_mode = json_extract(
                                        active.execution_context_json,
                                        '$.mode.workspace_mode'
                                    )
                                    AND session.working_dir = json_extract(
                                        active.execution_context_json,
                                        '$.mode.working_dir'
                                    )
                                    AND session.project_dir IS json_extract(
                                        active.execution_context_json,
                                        '$.mode.project_dir'
                                    )
                                )
                            )
                            AND (
                                (
                                    active.kind = 'worker_conversation'
                                    AND active.governor_origin = 'user_dm'
                                    AND active.governor_lane_key = 'dm'
                                    AND active.objective_message_id IS NOT NULL
                                    AND active.conversation_through_message_id
                                        = active.objective_message_id
                                    AND EXISTS (
                                        SELECT 1 FROM messages objective
                                        WHERE objective.id
                                            = active.objective_message_id
                                          AND objective.session_id = session.id
                                          AND objective.role = 'user'
                                    )
                                )
                                OR (
                                    active.kind = 'worker_introduction_review'
                                    AND active.status IN ('leased', 'running')
                                    AND active.governor_origin
                                        = 'user_lifecycle_action'
                                    AND active.governor_lane_key = 'dm'
                                    AND active.objective_message_id IS NULL
                                    AND active.response_message_id IS NULL
                                    AND active.response_group_message_id IS NULL
                                    AND active.response_provider_call_id IS NULL
                                    AND EXISTS (
                                        SELECT 1
                                        FROM hive_worker_introduction_reviews review
                                        WHERE review.run_id = active.id
                                          AND review.worker_id = worker.id
                                          AND review.session_id = session.id
                                          AND review.through_message_id
                                              = active.conversation_through_message_id
                                          AND review.status = 'stale'
                                          AND review.provider_call_id IS NULL
                                          AND review.last_error
                                              = 'pre-provider stale: superseded by newer accepted user input'
                                    )
                                    AND NOT EXISTS (
                                        SELECT 1
                                        FROM hive_worker_provider_calls provider_call
                                        WHERE provider_call.run_id = active.id
                                    )
                                )
                            )
                      )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid Worker conversation input binding');
                    END;

                    DROP TRIGGER IF EXISTS hive_worker_conversation_inputs_materialize_guard;
                    CREATE TRIGGER hive_worker_conversation_inputs_materialize_guard
                    BEFORE UPDATE OF state, canonical_message_id, assigned_run_id, materialized_at
                    ON hive_worker_conversation_inputs
                    WHEN NEW.state = 'materialized' AND NOT EXISTS (
                        WITH RECURSIVE conversation_chain(run_id) AS (
                            SELECT NEW.accepted_while_run_id
                            UNION
                            SELECT ledger.accepted_while_run_id
                            FROM hive_worker_conversation_inputs ledger
                            JOIN conversation_chain chain
                              ON ledger.assigned_run_id = chain.run_id
                            WHERE ledger.id <> NEW.id
                              AND ledger.state = 'materialized'
                              AND ledger.worker_id = NEW.worker_id
                              AND ledger.owner_user_id IS NEW.owner_user_id
                              AND ledger.session_id = NEW.session_id
                            UNION
                            SELECT ledger.assigned_run_id
                            FROM hive_worker_conversation_inputs ledger
                            JOIN conversation_chain chain
                              ON ledger.accepted_while_run_id = chain.run_id
                            WHERE ledger.id <> NEW.id
                              AND ledger.state = 'materialized'
                              AND ledger.worker_id = NEW.worker_id
                              AND ledger.owner_user_id IS NEW.owner_user_id
                              AND ledger.session_id = NEW.session_id
                              AND ledger.assigned_run_id IS NOT NULL
                        ),
                        component_tail(run_id) AS (
                            SELECT COALESCE(
                                (
                                    SELECT ledger.assigned_run_id
                                    FROM hive_worker_conversation_inputs ledger
                                    JOIN conversation_chain component
                                      ON component.run_id
                                         = ledger.accepted_while_run_id
                                    WHERE ledger.id <> NEW.id
                                      AND ledger.state = 'materialized'
                                      AND ledger.worker_id = NEW.worker_id
                                      AND ledger.owner_user_id IS NEW.owner_user_id
                                      AND ledger.session_id = NEW.session_id
                                      AND ledger.assigned_run_id IS NOT NULL
                                    ORDER BY ledger.canonical_message_id DESC
                                    LIMIT 1
                                ),
                                NEW.accepted_while_run_id
                            )
                        )
                        SELECT 1
                        FROM component_tail tail
                        JOIN messages message
                          ON message.id = NEW.canonical_message_id
                        JOIN hive_runs assigned ON assigned.id = NEW.assigned_run_id
                        JOIN hive_runs predecessor
                          ON predecessor.id = tail.run_id
                        JOIN hive_workers worker ON worker.id = predecessor.worker_id
                        JOIN sessions predecessor_session
                          ON predecessor_session.id = predecessor.session_id
                        JOIN hive_controllers predecessor_controller
                          ON predecessor_controller.id = predecessor.controller_id
                        WHERE message.session_id = NEW.session_id
                          AND message.role = 'user'
                          AND assigned.kind = 'worker_conversation'
                          AND assigned.worker_id = NEW.worker_id
                          AND assigned.session_id = NEW.session_id
                          AND assigned.controller_id = predecessor.controller_id
                          AND assigned.schedule_id IS NULL
                          AND assigned.occurrence_id IS NULL
                          AND assigned.group_id IS NULL
                          AND assigned.workflow_goal_id IS NULL
                          AND assigned.workflow_attempt_id IS NULL
                          AND assigned.governor_origin = 'user_dm'
                          AND assigned.governor_lane_key = 'dm'
                          AND assigned.objective_message_id = NEW.canonical_message_id
                          AND assigned.conversation_through_message_id
                              = NEW.canonical_message_id
                          AND worker.user_id IS NEW.owner_user_id
                          AND worker.status = 'active'
                          AND predecessor_session.user_id IS worker.user_id
                          AND predecessor_session.session_type = 'hive'
                          AND predecessor_controller.worker_id = worker.id
                          AND predecessor_controller.session_id
                              = predecessor_session.id
                          AND predecessor_controller.user_id IS worker.user_id
                          AND predecessor_controller.status = 'active'
                          AND predecessor.worker_id = NEW.worker_id
                          AND predecessor.session_id = NEW.session_id
                          AND (
                              predecessor.kind = 'worker_introduction_review'
                              OR assigned.execution_context_json
                                 = predecessor.execution_context_json
                          )
                          AND (
                              (
                                  predecessor.status = 'succeeded'
                                  AND (
                                      predecessor.response_message_id IS NOT NULL
                                      OR predecessor.kind
                                          = 'worker_introduction_review'
                                  )
                              )
                              OR (
                                  predecessor.status = 'cancelled'
                                  AND predecessor.kind = 'worker_conversation'
                                  AND predecessor.schedule_id IS NULL
                                  AND predecessor.occurrence_id IS NULL
                                  AND predecessor.group_id IS NULL
                                  AND predecessor.workflow_goal_id IS NULL
                                  AND predecessor.workflow_attempt_id IS NULL
                                  AND predecessor.governor_origin = 'user_dm'
                                  AND predecessor.governor_lane_key = 'dm'
                                  AND predecessor.response_message_id IS NULL
                                  AND predecessor.response_group_message_id IS NULL
                                  AND predecessor.response_provider_call_id IS NULL
                                  AND predecessor.last_stop_reason
                                      = 'owner acknowledged unresolved provider accounting for one direct-message recovery call'
                                  AND json_valid(predecessor.outcome_json)
                                  AND json_extract(
                                      predecessor.outcome_json, '$.kind'
                                  ) = 'cancelled'
                                  AND json_extract(
                                      predecessor.outcome_json, '$.reason'
                                  ) = 'owner_acknowledged_governor_recovery'
                                  AND EXISTS (
                                      SELECT 1
                                      FROM hive_worker_governor_override_grants recovery_grant
                                      WHERE recovery_grant.id = json_extract(
                                          predecessor.outcome_json,
                                          '$.governor_recovery_grant_id'
                                      )
                                        AND recovery_grant.worker_id = worker.id
                                        AND recovery_grant.owner_user_id IS worker.user_id
                                        AND recovery_grant.bypass_unresolved_provider_call = 1
                                        AND recovery_grant.bypass_daily_call_cap = 0
                                        AND recovery_grant.bypass_daily_token_cap = 0
                                        AND recovery_grant.bypass_quiet_hours = 0
                                        AND recovery_grant.bypass_idle_backoff = 0
                                        AND recovery_grant.created_at
                                            <= predecessor.finished_at
                                        AND recovery_grant.expires_at
                                            > NEW.materialized_at
                                        AND assigned.governor_override_id
                                            = recovery_grant.id
                                        AND NOT EXISTS (
                                            SELECT 1
                                            FROM hive_worker_governor_override_consumptions recovery_consumption
                                            WHERE recovery_consumption.grant_id
                                                = recovery_grant.id
                                        )
                                        AND (
                                            SELECT COUNT(*)
                                            FROM hive_runs recovery_reference
                                            WHERE recovery_reference.governor_override_id
                                                = recovery_grant.id
                                        ) = 1
                                        AND EXISTS (
                                            SELECT 1
                                            FROM hive_worker_provider_calls recovery_call
                                            LEFT JOIN hive_worker_provider_call_outcomes recovery_outcome
                                              ON recovery_outcome.provider_call_id
                                                 = recovery_call.provider_call_id
                                            WHERE recovery_call.run_id = predecessor.id
                                              AND (
                                                  recovery_outcome.provider_call_id IS NULL
                                                  OR recovery_outcome.state = 'unknown'
                                              )
                                        )
                                        AND NOT EXISTS (
                                            SELECT 1
                                            FROM hive_worker_provider_calls late_recovery_call
                                            LEFT JOIN hive_worker_provider_call_outcomes late_recovery_outcome
                                              ON late_recovery_outcome.provider_call_id
                                                 = late_recovery_call.provider_call_id
                                            WHERE late_recovery_call.run_id = predecessor.id
                                              AND (
                                                  late_recovery_outcome.provider_call_id IS NULL
                                                  OR late_recovery_outcome.state = 'unknown'
                                              )
                                              AND late_recovery_call.started_at
                                                  >= recovery_grant.created_at
                                        )
                                  )
                                  AND json_valid(
                                      predecessor.execution_context_json
                                  )
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.kind'
                                  ) IN (
                                      'worker_conversation_neutral',
                                      'worker_workspace_attached'
                                  )
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.lane.kind'
                                  ) = 'direct_message'
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.worker_id'
                                  ) = predecessor.worker_id
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.worker_revision'
                                  ) = worker.revision
                                  AND worker.dm_session_id = predecessor.session_id
                                  AND (
                                      (
                                          json_extract(
                                              predecessor.execution_context_json,
                                              '$.mode.kind'
                                          ) = 'worker_conversation_neutral'
                                          AND predecessor_session.workspace_mode
                                              = 'neutral'
                                          AND (
                                              predecessor_session.working_dir IS NULL
                                              OR predecessor_session.working_dir = ''
                                          )
                                          AND (
                                              predecessor_session.project_dir IS NULL
                                              OR predecessor_session.project_dir = ''
                                          )
                                      )
                                      OR (
                                          predecessor_session.workspace_mode
                                              = json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.workspace_mode'
                                              )
                                          AND predecessor_session.working_dir
                                              = json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.working_dir'
                                              )
                                          AND predecessor_session.project_dir
                                              IS json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.project_dir'
                                              )
                                      )
                                  )
                              )
                              OR (
                                  predecessor.status = 'cancelled'
                                  AND predecessor.kind = 'worker_conversation'
                                  AND predecessor.schedule_id IS NULL
                                  AND predecessor.occurrence_id IS NULL
                                  AND predecessor.group_id IS NULL
                                  AND predecessor.workflow_goal_id IS NULL
                                  AND predecessor.workflow_attempt_id IS NULL
                                  AND predecessor.governor_origin = 'user_dm'
                                  AND predecessor.governor_lane_key = 'dm'
                                  AND predecessor.response_message_id IS NULL
                                  AND predecessor.response_group_message_id IS NULL
                                  AND predecessor.response_provider_call_id IS NULL
                                  AND predecessor.last_stop_reason
                                      = 'owner acknowledged completed provider response loss for one direct-message recovery call'
                                  AND json_valid(predecessor.outcome_json)
                                  AND json_extract(
                                      predecessor.outcome_json, '$.kind'
                                  ) = 'cancelled'
                                  AND json_extract(
                                      predecessor.outcome_json, '$.reason'
                                  ) = 'owner_acknowledged_provider_response_loss'
                                  AND (
                                      (
                                          json_type(
                                              predecessor.outcome_json,
                                              '$.governor_recovery_grant_id'
                                          ) IS NULL
                                          AND assigned.governor_override_id IS NULL
                                      )
                                      OR EXISTS (
                                          SELECT 1
                                          FROM hive_worker_governor_override_grants response_loss_grant
                                          WHERE response_loss_grant.id = json_extract(
                                              predecessor.outcome_json,
                                              '$.governor_recovery_grant_id'
                                          )
                                            AND assigned.governor_override_id
                                                = response_loss_grant.id
                                            AND response_loss_grant.worker_id = worker.id
                                            AND response_loss_grant.owner_user_id
                                                IS worker.user_id
                                            AND response_loss_grant.bypass_unresolved_provider_call = 1
                                            AND response_loss_grant.bypass_daily_call_cap = 0
                                            AND response_loss_grant.bypass_daily_token_cap = 0
                                            AND response_loss_grant.bypass_quiet_hours = 0
                                            AND response_loss_grant.bypass_idle_backoff = 0
                                            AND response_loss_grant.created_at
                                                <= predecessor.finished_at
                                            AND response_loss_grant.expires_at
                                                > NEW.materialized_at
                                            AND NOT EXISTS (
                                                SELECT 1
                                                FROM hive_worker_governor_override_consumptions response_loss_consumption
                                                WHERE response_loss_consumption.grant_id
                                                    = response_loss_grant.id
                                            )
                                            AND (
                                                SELECT COUNT(*)
                                                FROM hive_runs response_loss_reference
                                                WHERE response_loss_reference.governor_override_id
                                                    = response_loss_grant.id
                                            ) = 1
                                      )
                                  )
                                  AND (
                                      SELECT COUNT(*)
                                      FROM hive_worker_provider_calls response_loss_call
                                      WHERE response_loss_call.run_id = predecessor.id
                                  ) = 1
                                  AND EXISTS (
                                      SELECT 1
                                      FROM hive_worker_provider_calls response_loss_call
                                      JOIN hive_worker_provider_call_outcomes response_loss_outcome
                                        ON response_loss_outcome.provider_call_id
                                           = response_loss_call.provider_call_id
                                      WHERE response_loss_call.run_id = predecessor.id
                                        AND response_loss_call.worker_id = worker.id
                                        AND response_loss_call.worker_revision = worker.revision
                                        AND response_loss_call.owner_user_id IS worker.user_id
                                        AND response_loss_call.session_id
                                            = predecessor.session_id
                                        AND response_loss_call.group_id IS NULL
                                        AND response_loss_call.workflow_goal_id IS NULL
                                        AND response_loss_call.workflow_attempt_id IS NULL
                                        AND response_loss_call.origin = 'user_dm'
                                        AND response_loss_call.lane_key = 'dm'
                                        AND response_loss_call.call_kind = 'agent_turn'
                                        AND response_loss_outcome.state = 'completed'
                                        AND response_loss_outcome.outcome = 'completed'
                                        AND response_loss_outcome.remote_acceptance
                                            = 'acknowledged'
                                  )
                                  AND json_valid(
                                      predecessor.execution_context_json
                                  )
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.kind'
                                  ) IN (
                                      'worker_conversation_neutral',
                                      'worker_workspace_attached'
                                  )
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.lane.kind'
                                  ) = 'direct_message'
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.worker_id'
                                  ) = predecessor.worker_id
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.worker_revision'
                                  ) = worker.revision
                                  AND worker.status = 'active'
                                  AND worker.dm_session_id = predecessor.session_id
                                  AND (
                                      (
                                          json_extract(
                                              predecessor.execution_context_json,
                                              '$.mode.kind'
                                          ) = 'worker_conversation_neutral'
                                          AND predecessor_session.workspace_mode
                                              = 'neutral'
                                          AND (
                                              predecessor_session.working_dir IS NULL
                                              OR predecessor_session.working_dir = ''
                                          )
                                          AND (
                                              predecessor_session.project_dir IS NULL
                                              OR predecessor_session.project_dir = ''
                                          )
                                      )
                                      OR (
                                          predecessor_session.workspace_mode
                                              = json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.workspace_mode'
                                              )
                                          AND predecessor_session.working_dir
                                              = json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.working_dir'
                                              )
                                          AND predecessor_session.project_dir
                                              IS json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.project_dir'
                                              )
                                      )
                                  )
                              )
                              OR (
                                  predecessor.status IN (
                                      'failed', 'dead_letter', 'cancelled'
                                  )
                                  AND predecessor.kind = 'worker_conversation'
                                  AND predecessor.schedule_id IS NULL
                                  AND predecessor.occurrence_id IS NULL
                                  AND predecessor.group_id IS NULL
                                  AND predecessor.workflow_goal_id IS NULL
                                  AND predecessor.workflow_attempt_id IS NULL
                                  AND predecessor.governor_origin = 'user_dm'
                                  AND predecessor.governor_lane_key = 'dm'
                                  AND predecessor.response_message_id IS NULL
                                  AND predecessor.response_group_message_id IS NULL
                                  AND predecessor.response_provider_call_id IS NULL
                                  AND NOT EXISTS (
                                      SELECT 1
                                      FROM hive_worker_provider_calls call
                                      LEFT JOIN hive_worker_provider_call_outcomes outcome
                                        ON outcome.provider_call_id
                                           = call.provider_call_id
                                      WHERE call.run_id = predecessor.id
                                        AND (
                                            outcome.provider_call_id IS NULL
                                            OR outcome.state = 'unknown'
                                            OR (
                                                call.call_kind IN (
                                                    'agent_turn',
                                                    'worker_introduction_opening',
                                                    'worker_introduction_onboarding'
                                                )
                                                AND outcome.state = 'completed'
                                                AND outcome.outcome = 'completed'
                                                AND outcome.remote_acceptance
                                                    = 'acknowledged'
                                            )
                                        )
                                  )
                                  AND json_valid(
                                      predecessor.execution_context_json
                                  )
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.kind'
                                  ) IN (
                                      'worker_conversation_neutral',
                                      'worker_workspace_attached'
                                  )
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.lane.kind'
                                  ) = 'direct_message'
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.worker_id'
                                  ) = predecessor.worker_id
                                  AND json_extract(
                                      predecessor.execution_context_json,
                                      '$.mode.worker_revision'
                                  ) = worker.revision
                                  AND worker.dm_session_id = predecessor.session_id
                                  AND (
                                      (
                                          json_extract(
                                              predecessor.execution_context_json,
                                              '$.mode.kind'
                                          ) = 'worker_conversation_neutral'
                                          AND predecessor_session.workspace_mode
                                              = 'neutral'
                                          AND (
                                              predecessor_session.working_dir IS NULL
                                              OR predecessor_session.working_dir = ''
                                          )
                                          AND (
                                              predecessor_session.project_dir IS NULL
                                              OR predecessor_session.project_dir = ''
                                          )
                                      )
                                      OR (
                                          predecessor_session.workspace_mode
                                              = json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.workspace_mode'
                                              )
                                          AND predecessor_session.working_dir
                                              = json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.working_dir'
                                              )
                                          AND predecessor_session.project_dir
                                              IS json_extract(
                                                  predecessor.execution_context_json,
                                                  '$.mode.project_dir'
                                              )
                                      )
                                  )
                              )
                          )
                    )
                    BEGIN
                        SELECT RAISE(ABORT, 'invalid materialized Worker input binding');
                    END;
                    "#,
                )?;
            }
            acceptance_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (78)",
                [],
            )?;
            acceptance_tx.commit()?;
        }

        // Migration 79: exact-owner, project-bound spatial canvases.
        //
        // The materialized snapshot makes startup bounded, while the compact
        // append-only event ledger supplies reconnect replay and idempotency.
        // NULL is the exact local owner, never a wildcard across tenant rows.
        if current_version < 79 {
            info!("Running migration 79: spatial canvas snapshots and replay ledger");
            let canvas_tx = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                .context("acquiring spatial canvas migration lock")?;
            canvas_tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS spatial_canvases (
                    id TEXT PRIMARY KEY,
                    owner_user_id TEXT,
                    project_dir TEXT NOT NULL CHECK(
                        length(project_dir) BETWEEN 1 AND 16384
                        AND project_dir GLOB '/*'
                    ),
                    surface TEXT NOT NULL CHECK(surface IN ('code', 'hive')),
                    name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 512),
                    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
                    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
                    shared_theme_id TEXT NOT NULL DEFAULT 'graphite'
                        CHECK(length(shared_theme_id) BETWEEN 1 AND 128),
                    state_json TEXT NOT NULL CHECK(
                        json_valid(state_json)
                        AND json_type(state_json) = 'object'
                        AND json_extract(state_json, '$.schema_version') = 1
                        AND json_type(state_json, '$.nodes') = 'array'
                        AND json_array_length(state_json, '$.nodes') <= 256
                        AND length(state_json) <= 2097152
                    ),
                    create_op_id TEXT NOT NULL
                        CHECK(length(create_op_id) BETWEEN 1 AND 256),
                    create_request_json TEXT NOT NULL CHECK(
                        json_valid(create_request_json)
                        AND json_type(create_request_json) = 'object'
                        AND length(create_request_json) <= 32768
                    ),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    archived_at TEXT
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_spatial_canvases_local_name
                    ON spatial_canvases(project_dir, surface, name)
                    WHERE owner_user_id IS NULL AND archived_at IS NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_spatial_canvases_tenant_name
                    ON spatial_canvases(owner_user_id, project_dir, surface, name)
                    WHERE owner_user_id IS NOT NULL AND archived_at IS NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_spatial_canvases_local_create_op
                    ON spatial_canvases(create_op_id)
                    WHERE owner_user_id IS NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_spatial_canvases_tenant_create_op
                    ON spatial_canvases(owner_user_id, create_op_id)
                    WHERE owner_user_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_spatial_canvases_owner_recent
                    ON spatial_canvases(owner_user_id, surface, updated_at DESC)
                    WHERE archived_at IS NULL;

                CREATE TABLE IF NOT EXISTS spatial_canvas_events (
                    canvas_id TEXT NOT NULL
                        REFERENCES spatial_canvases(id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL CHECK(sequence >= 0),
                    op_id TEXT NOT NULL CHECK(length(op_id) BETWEEN 1 AND 256),
                    client_id TEXT NOT NULL CHECK(length(client_id) BETWEEN 1 AND 256),
                    operations_json TEXT NOT NULL CHECK(
                        json_valid(operations_json)
                        AND json_type(operations_json) = 'array'
                        AND length(operations_json) <= 2097152
                    ),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(canvas_id, sequence),
                    UNIQUE(canvas_id, op_id)
                );
                CREATE INDEX IF NOT EXISTS idx_spatial_canvas_events_replay
                    ON spatial_canvas_events(canvas_id, sequence ASC);
                "#,
            )?;
            canvas_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (79)",
                [],
            )?;
            canvas_tx.commit()?;
        }

        // Migration 80: crash-reconcilable Canvas resource creation intents,
        // including detach-generation CAS for Bound resources whose external
        // runtime cannot be proven after restart.
        if current_version < 80 {
            info!("Running migration 80: canvas resource create intent ledger");
            let resource_tx =
                Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
                    .context("acquiring canvas resource intent migration lock")?;
            resource_tx.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS spatial_canvas_resource_creates (
                    canvas_id TEXT NOT NULL
                        REFERENCES spatial_canvases(id) ON DELETE CASCADE,
                    owner_user_id TEXT,
                    create_op_id TEXT NOT NULL
                        CHECK(length(create_op_id) BETWEEN 1 AND 192),
                    bind_op_id TEXT NOT NULL
                        CHECK(length(bind_op_id) BETWEEN 1 AND 256),
                    client_id TEXT NOT NULL
                        CHECK(length(client_id) BETWEEN 1 AND 256),
                    node_id TEXT NOT NULL
                        CHECK(length(node_id) BETWEEN 1 AND 64),
                    resource_kind TEXT NOT NULL
                        CHECK(resource_kind IN ('agent', 'terminal', 'browser')),
                    request_json TEXT NOT NULL CHECK(
                        json_valid(request_json)
                        AND json_type(request_json) = 'object'
                        AND length(request_json) <= 65536
                    ),
                    request_fingerprint TEXT NOT NULL
                        CHECK(length(request_fingerprint) BETWEEN 1 AND 128),
                    status TEXT NOT NULL CHECK(status IN (
                        'reserved', 'provisioning', 'provisioned', 'bound',
                        'failed', 'ambiguous', 'cleaning', 'cleaned', 'abandoned'
                    )),
                    executor_id TEXT CHECK(
                        executor_id IS NULL OR length(executor_id) BETWEEN 1 AND 128
                    ),
                    claim_token TEXT CHECK(
                        claim_token IS NULL OR length(claim_token) BETWEEN 1 AND 128
                    ),
                    result_json TEXT CHECK(
                        result_json IS NULL OR (
                            json_valid(result_json)
                            AND json_type(result_json) = 'object'
                            AND length(result_json) <= 65536
                        )
                    ),
                    last_error TEXT CHECK(
                        last_error IS NULL OR length(last_error) <= 8192
                    ),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    detach_generation INTEGER NOT NULL DEFAULT 0
                        CHECK(detach_generation >= 0),
                    PRIMARY KEY(canvas_id, create_op_id),
                    UNIQUE(canvas_id, bind_op_id)
                );
                CREATE INDEX IF NOT EXISTS idx_spatial_canvas_resource_creates_pending
                    ON spatial_canvas_resource_creates(canvas_id, status, updated_at ASC)
                    WHERE status NOT IN ('bound', 'cleaned', 'abandoned');
                "#,
            )?;
            resource_tx.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (80)",
                [],
            )?;
            resource_tx.commit()?;
        }

        if privacy_cleanup_requested {
            self.restore_normal_locking_after_privacy_migration()?;
        }

        info!("Migrations complete");
        Ok(())
    }
}

#[cfg(test)]
mod privacy_checkpoint_tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    #[test]
    fn privacy_checkpoint_rejects_a_pinned_wal_reader() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("privacy-checkpoint-busy.db");
        let writer = Connection::open(&db_path).expect("open writer");
        writer
            .pragma_update(None, "journal_mode", "WAL")
            .expect("enable WAL");
        writer
            .execute_batch(
                "CREATE TABLE probe (value INTEGER NOT NULL); INSERT INTO probe VALUES (1);",
            )
            .expect("seed probe");
        let database = Database { conn: writer };
        database
            .checkpoint_wal_without_busy_readers("fixture")
            .expect("clear fixture WAL");

        let reader = Connection::open(&db_path).expect("open reader");
        reader.execute_batch("BEGIN").expect("begin read snapshot");
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM probe", [], |row| row.get(0))
            .expect("pin read snapshot");
        assert_eq!(count, 1);

        database
            .conn
            .execute("INSERT INTO probe VALUES (2)", [])
            .expect("append newer WAL frame");
        let error = database
            .checkpoint_wal_without_busy_readers("pinned-reader")
            .expect_err("privacy checkpoint must fail while a reader pins the WAL");
        assert!(
            error.to_string().contains("was busy"),
            "unexpected checkpoint error: {error:#}"
        );

        reader.execute_batch("ROLLBACK").expect("release snapshot");
        drop(reader);
        database
            .checkpoint_wal_without_busy_readers("released-reader")
            .expect("checkpoint should succeed after reader release");
    }
}

#[cfg(test)]
mod delegation_event_migration_tests {
    use super::*;
    use rusqlite::{params, Connection};
    use tempfile::TempDir;

    #[test]
    fn migration_61_preserves_preview_events_and_accepts_future_kinds() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("delegation-event-compatibility.db");
        let fixture = Connection::open(&db_path).expect("open preview fixture");
        fixture
            .execute_batch(
                r#"
                CREATE TABLE schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO schema_version (version) VALUES (60);

                CREATE TABLE sessions (id TEXT PRIMARY KEY);
                CREATE TABLE delegation_groups (delegation_group_id TEXT PRIMARY KEY);
                CREATE TABLE delegation_tasks (delegation_task_id TEXT PRIMARY KEY);
                INSERT INTO sessions (id) VALUES ('session-1');
                INSERT INTO delegation_groups (delegation_group_id) VALUES ('group-1');
                INSERT INTO delegation_tasks (delegation_task_id) VALUES ('task-1');

                CREATE TABLE delegation_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_session_id TEXT NOT NULL,
                    delegation_group_id TEXT NOT NULL,
                    delegation_task_id TEXT,
                    event_type TEXT NOT NULL CHECK (event_type IN (
                        'group_created', 'group_queued', 'group_state_changed',
                        'task_claimed', 'task_running', 'task_state_changed',
                        'parent_continuation_queued', 'parent_continuation_promoted'
                    )),
                    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (parent_session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (delegation_group_id)
                        REFERENCES delegation_groups(delegation_group_id) ON DELETE CASCADE,
                    FOREIGN KEY (delegation_task_id)
                        REFERENCES delegation_tasks(delegation_task_id) ON DELETE CASCADE
                );
                CREATE INDEX idx_delegation_events_session_cursor
                    ON delegation_events(parent_session_id, event_id ASC);
                CREATE INDEX idx_delegation_events_group_cursor
                    ON delegation_events(delegation_group_id, event_id ASC);
                INSERT INTO delegation_events (
                    parent_session_id, delegation_group_id, delegation_task_id,
                    event_type, payload_json, created_at
                ) VALUES (
                    'session-1', 'group-1', 'task-1', 'task_running', '{}',
                    '2026-08-08T00:00:00Z'
                );
                "#,
            )
            .expect("seed schema-60 preview database");
        drop(fixture);

        let database = Database::new(&db_path).expect("migrate preview database");
        assert_eq!(database.get_schema_version(), 80);
        database
            .conn()
            .execute(
                "INSERT INTO delegation_events (
                    parent_session_id, delegation_group_id, delegation_task_id,
                    event_type, payload_json, created_at
                 ) VALUES (?1, ?2, NULL, ?3, ?4, ?5)",
                params![
                    "session-1",
                    "group-1",
                    "future_scheduler_event",
                    r#"{"domain":"workspace"}"#,
                    "2026-08-08T00:00:01Z"
                ],
            )
            .expect("persist a future event kind");

        let event_kinds = database
            .conn()
            .prepare("SELECT event_type FROM delegation_events ORDER BY event_id ASC")
            .expect("prepare event query")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query event kinds")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect event kinds");
        assert_eq!(event_kinds, ["task_running", "future_scheduler_event"]);

        let index_count: i64 = database
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index'
                   AND name IN (
                       'idx_delegation_events_session_cursor',
                       'idx_delegation_events_group_cursor'
                   )",
                [],
                |row| row.get(0),
            )
            .expect("count restored event indexes");
        assert_eq!(index_count, 2);
    }

    #[test]
    fn migration_61_handles_an_empty_synthetic_event_table_without_parent_tables() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("synthetic-delegation-events.db");
        let fixture = Connection::open(&db_path).expect("open synthetic fixture");
        fixture
            .execute_batch(
                r#"
                CREATE TABLE schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO schema_version (version) VALUES (60);
                CREATE TABLE delegation_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_session_id TEXT NOT NULL,
                    delegation_group_id TEXT NOT NULL,
                    delegation_task_id TEXT,
                    event_type TEXT NOT NULL CHECK (event_type IN (
                        'group_created', 'group_queued', 'group_state_changed',
                        'task_claimed', 'task_running', 'task_state_changed',
                        'parent_continuation_queued', 'parent_continuation_promoted'
                    )),
                    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (parent_session_id) REFERENCES sessions(id) ON DELETE CASCADE,
                    FOREIGN KEY (delegation_group_id)
                        REFERENCES delegation_groups(delegation_group_id) ON DELETE CASCADE,
                    FOREIGN KEY (delegation_task_id)
                        REFERENCES delegation_tasks(delegation_task_id) ON DELETE CASCADE
                );
                "#,
            )
            .expect("seed synthetic schema-60 database");
        drop(fixture);

        let database = Database::new(&db_path).expect("migrate synthetic database");
        assert_eq!(database.get_schema_version(), 80);
        let create_sql: String = database
            .conn()
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type = 'table' AND name = 'delegation_events'",
                [],
                |row| row.get(0),
            )
            .expect("read migrated event schema");
        assert!(!create_sql.contains("event_type IN"));
        let foreign_key_count: i64 = database
            .conn()
            .prepare("PRAGMA foreign_key_list(delegation_events)")
            .expect("prepare event foreign keys")
            .query_map([], |_| Ok(()))
            .expect("query event foreign keys")
            .count() as i64;
        assert_eq!(
            foreign_key_count, 3,
            "migration must preserve production FKs"
        );
    }
}
