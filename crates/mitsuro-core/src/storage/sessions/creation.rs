use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};

use crate::agent::PinchContext;
use crate::tools::registry::PermissionMode;

use super::{SessionManager, SessionType, WorkspaceMode};

struct LinkedSessionContract {
    user_id: Option<String>,
    session_type: SessionType,
    working_dir: Option<String>,
    project_dir: Option<String>,
    workspace_mode: WorkspaceMode,
    target_branch: Option<String>,
}

impl SessionManager {
    fn has_legacy_required_provider_column(&self) -> Result<bool> {
        let count: i64 = self.db.conn().query_row(
            "SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name = 'provider' AND \"notnull\" = 1",
            [],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn create_session(
        &self,
        title: &str,
        model: Option<&str>,
        working_dir: Option<&str>,
    ) -> Result<String> {
        self.create_session_with_target_branch(title, model, working_dir, None)
    }

    /// Create a new session with optional target branch metadata.
    pub fn create_session_with_target_branch(
        &self,
        title: &str,
        model: Option<&str>,
        working_dir: Option<&str>,
        target_branch: Option<&str>,
    ) -> Result<String> {
        self.create_session_for_user_with_target_branch(
            title,
            model,
            working_dir,
            None,
            target_branch,
        )
    }

    /// Create a new session with user ownership (multi-tenant)
    pub fn create_session_for_user(
        &self,
        title: &str,
        model: Option<&str>,
        working_dir: Option<&str>,
        user_id: Option<&str>,
    ) -> Result<String> {
        self.create_session_for_user_with_target_branch(title, model, working_dir, user_id, None)
    }

    /// Create a new session with explicit workspace and surface metadata.
    pub fn create_session_for_user_with_config(
        &self,
        title: &str,
        model: Option<&str>,
        working_dir: Option<&str>,
        project_dir: Option<&str>,
        workspace_mode: WorkspaceMode,
        user_id: Option<&str>,
        target_branch: Option<&str>,
        session_type: SessionType,
    ) -> Result<String> {
        self.create_session_for_user_with_config_and_permission(
            title,
            model,
            working_dir,
            project_dir,
            workspace_mode,
            user_id,
            target_branch,
            session_type,
            PermissionMode::default(),
        )
    }

    /// Create a new session with explicit workspace, surface, and permission metadata.
    pub fn create_session_for_user_with_config_and_permission(
        &self,
        title: &str,
        model: Option<&str>,
        working_dir: Option<&str>,
        project_dir: Option<&str>,
        workspace_mode: WorkspaceMode,
        user_id: Option<&str>,
        target_branch: Option<&str>,
        session_type: SessionType,
        permission_mode: PermissionMode,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        if self.has_legacy_required_provider_column()? {
            let legacy_provider = model
                .and_then(|value| value.split_once(':').map(|(provider, _)| provider))
                .unwrap_or("mitsuro");
            self.db.conn().execute(
                "INSERT INTO sessions (id, title, created_at, updated_at, provider, model, metadata, working_dir, project_dir, workspace_mode, session_type, user_id, target_branch, permission_mode)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}', ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    id,
                    title,
                    now,
                    now,
                    legacy_provider,
                    model.unwrap_or_default(),
                    working_dir,
                    project_dir,
                    workspace_mode.to_string(),
                    session_type.to_string(),
                    user_id,
                    target_branch,
                    permission_mode.as_str()
                ],
            )?;
        } else {
            self.db.conn().execute(
                "INSERT INTO sessions (id, title, created_at, updated_at, model, working_dir, project_dir, workspace_mode, session_type, user_id, target_branch, permission_mode)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    id,
                    title,
                    now,
                    now,
                    model,
                    working_dir,
                    project_dir,
                    workspace_mode.to_string(),
                    session_type.to_string(),
                    user_id,
                    target_branch,
            permission_mode.as_str()
                ],
            )?;
        }

        Ok(id)
    }

    /// Create a session with an exact caller-chosen id for Canvas resource bind.
    pub fn create_session_for_user_with_id_config_and_permission(
        &self,
        id: &str,
        title: &str,
        model: Option<&str>,
        working_dir: Option<&str>,
        project_dir: Option<&str>,
        workspace_mode: WorkspaceMode,
        user_id: Option<&str>,
        target_branch: Option<&str>,
        session_type: SessionType,
        permission_mode: PermissionMode,
    ) -> Result<String> {
        if self.get_session(id)?.is_some() {
            return Ok(id.to_string());
        }
        let now = Utc::now().to_rfc3339();
        if self.has_legacy_required_provider_column()? {
            let legacy_provider = model
                .and_then(|value| value.split_once(':').map(|(provider, _)| provider))
                .unwrap_or("mitsuro");
            self.db.conn().execute(
                "INSERT INTO sessions (id, title, created_at, updated_at, provider, model, metadata, working_dir, project_dir, workspace_mode, session_type, user_id, target_branch, permission_mode)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}', ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    id,
                    title,
                    now,
                    now,
                    legacy_provider,
                    model.unwrap_or_default(),
                    working_dir,
                    project_dir,
                    workspace_mode.to_string(),
                    session_type.to_string(),
                    user_id,
                    target_branch,
                    permission_mode.as_str()
                ],
            )?;
        } else {
            self.db.conn().execute(
                "INSERT INTO sessions (id, title, created_at, updated_at, model, working_dir, project_dir, workspace_mode, session_type, user_id, target_branch, permission_mode)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    id,
                    title,
                    now,
                    now,
                    model,
                    working_dir,
                    project_dir,
                    workspace_mode.to_string(),
                    session_type.to_string(),
                    user_id,
                    target_branch,
                    permission_mode.as_str()
                ],
            )?;
        }
        Ok(id.to_string())
    }

    /// Create a new session with user ownership and optional target branch.
    pub fn create_session_for_user_with_target_branch(
        &self,
        title: &str,
        model: Option<&str>,
        working_dir: Option<&str>,
        user_id: Option<&str>,
        target_branch: Option<&str>,
    ) -> Result<String> {
        let project_dir = working_dir;
        let workspace_mode = if working_dir.is_some() {
            WorkspaceMode::Selected
        } else {
            WorkspaceMode::Neutral
        };
        self.create_session_for_user_with_config(
            title,
            model,
            working_dir,
            project_dir,
            workspace_mode,
            user_id,
            target_branch,
            SessionType::Code,
        )
    }

    /// Ensure the durable singleton Hive companion chat for a user exists.
    ///
    /// The companion thread is global (not project-bound), always autonomous, and
    /// shared across every client surface for that user. Job/run sessions created
    /// by Hive dispatch remain separate work units under the hood.
    pub fn ensure_hive_main_session(&self, user_id: Option<&str>) -> Result<super::SessionInfo> {
        const MAIN_TITLE: &str = "Hive";

        let mut sessions = self.list_sessions_for_user_by_type(None, user_id, SessionType::Hive)?;
        // Prefer the oldest matching companion candidate so repeated opens stay
        // pinned to one relationship thread even if later job sessions exist.
        sessions.sort_by_key(|session| session.updated_at);

        let companion = sessions.into_iter().find(|session| {
            session.parent_session_id.is_none()
                && session.project_dir.is_none()
                && matches!(session.workspace_mode, WorkspaceMode::Neutral)
                && (session.title == MAIN_TITLE
                    || session
                        .working_dir
                        .as_deref()
                        .is_none_or(|value| value.trim().is_empty()))
        });

        if let Some(mut session) = companion {
            if session.permission_mode != PermissionMode::Autonomous {
                self.update_session_permission_mode(&session.id, PermissionMode::Autonomous)?;
                session.permission_mode = PermissionMode::Autonomous;
            }
            if session.title != MAIN_TITLE {
                self.update_session_title(&session.id, MAIN_TITLE)?;
                session.title = MAIN_TITLE.to_string();
            }
            return Ok(session);
        }

        let session_id = self.create_session_for_user_with_config_and_permission(
            MAIN_TITLE,
            None,
            None,
            None,
            WorkspaceMode::Neutral,
            user_id,
            None,
            SessionType::Hive,
            PermissionMode::Autonomous,
        )?;

        self.get_session(&session_id)?
            .ok_or_else(|| anyhow::anyhow!("failed to load newly created Hive main session"))
    }

    /// Create a linked child session that inherits the parent's ownership metadata.
    pub fn create_linked_session(
        &self,
        title: &str,
        parent_session_id: &str,
        pinch_ctx: &PinchContext,
        model: Option<&str>,
        working_dir: Option<&str>,
        target_branch: Option<&str>,
        permission_mode: PermissionMode,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let parent_contract = self
            .db
            .conn()
            .query_row(
                "SELECT user_id, session_type, working_dir, project_dir, workspace_mode, target_branch
                 FROM sessions WHERE id = ?1",
                [parent_session_id],
                |row| {
                    let session_type_raw: String = row.get(1)?;
                    let workspace_mode_raw: String = row.get(4)?;
                    Ok(LinkedSessionContract {
                        user_id: row.get(0)?,
                        session_type: session_type_raw.parse().unwrap_or(SessionType::Code),
                        working_dir: row.get(2)?,
                        project_dir: row.get(3)?,
                        workspace_mode: workspace_mode_raw.parse().unwrap_or_else(|_| {
                            let project_dir: Option<String> = row.get(3).ok().flatten();
                            let working_dir: Option<String> = row.get(2).ok().flatten();
                            if project_dir.is_some() || working_dir.is_some() {
                                WorkspaceMode::Selected
                            } else {
                                WorkspaceMode::Neutral
                            }
                        }),
                        target_branch: row.get(5)?,
                    })
                },
            )
            .optional()?;

        let fallback_workspace_mode = if working_dir.is_some() {
            WorkspaceMode::Selected
        } else {
            WorkspaceMode::Neutral
        };
        let contract = parent_contract.unwrap_or_else(|| LinkedSessionContract {
            user_id: None,
            session_type: SessionType::Code,
            working_dir: working_dir.map(ToOwned::to_owned),
            project_dir: working_dir.map(ToOwned::to_owned),
            workspace_mode: fallback_workspace_mode,
            target_branch: target_branch.map(ToOwned::to_owned),
        });

        // Create new session with parent reference.
        if self.has_legacy_required_provider_column()? {
            let legacy_provider = model
                .and_then(|value| value.split_once(':').map(|(provider, _)| provider))
                .unwrap_or("mitsuro");
            self.db.conn().execute(
                "INSERT INTO sessions (id, title, created_at, updated_at, provider, model, metadata, working_dir, project_dir, workspace_mode, session_type, user_id, parent_session_id, target_branch, permission_mode)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}', ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    id,
                    title,
                    now,
                    now,
                    legacy_provider,
                    model.unwrap_or_default(),
                    contract.working_dir,
                    contract.project_dir,
                    contract.workspace_mode.to_string(),
                    contract.session_type.to_string(),
                    contract.user_id,
                    parent_session_id,
                    contract.target_branch,
                    permission_mode.as_str()
                ],
            )?;
        } else {
            self.db.conn().execute(
                "INSERT INTO sessions (id, title, created_at, updated_at, model, working_dir, project_dir, workspace_mode, session_type, user_id, parent_session_id, target_branch, permission_mode)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    id,
                    title,
                    now,
                    now,
                    model,
                    contract.working_dir,
                    contract.project_dir,
                    contract.workspace_mode.to_string(),
                    contract.session_type.to_string(),
                    contract.user_id,
                    parent_session_id,
                    contract.target_branch,
                    permission_mode.as_str()
                ],
            )?;
        }

        // Store pinch metadata
        let pinch_id = uuid::Uuid::new_v4().to_string();
        let key_files_json = serde_json::to_string(&pinch_ctx.ranked_files)?;

        self.db.conn().execute(
            "INSERT INTO pinch_metadata (id, source_session_id, target_session_id, summary, key_files, user_preservation_hints, user_direction, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                pinch_id,
                parent_session_id,
                id,
                &pinch_ctx.work_summary,
                key_files_json,
                &pinch_ctx.preservation_hints,
                &pinch_ctx.direction,
                now
            ],
        )?;

        Ok(id)
    }
}
