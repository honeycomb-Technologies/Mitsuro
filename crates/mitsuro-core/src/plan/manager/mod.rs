//! Plan manager.
//!
//! Keeps the public session-plan API concentrated in one place while separating
//! legacy file migration helpers and summary models into focused submodules.

mod legacy;
mod summary;
#[cfg(test)]
mod tests;

use anyhow::Result;
use chrono::Utc;
use std::path::PathBuf;

use super::file::PlanFile;
use super::lifecycle::{is_active_plan, PlanLifecycleState};
use crate::paths;
use crate::storage::{Database, PlanStore, SharedDatabase, WorkMode};

pub use summary::{LegacyPlanSummary, PlanSummary};

/// Manages plans with SQLite storage.
pub struct PlanManager {
    /// Directory where legacy plan files are stored (for migration).
    plans_dir: PathBuf,
    /// Shared database connection for plan storage.
    db: SharedDatabase,
}

impl PlanManager {
    fn with_store<T>(&self, f: impl FnOnce(&PlanStore<'_>) -> Result<T>) -> Result<T> {
        let db = self
            .db
            .lock()
            .map_err(|e| anyhow::anyhow!("Lock error: {}", e))?;
        let store = PlanStore::new(&db);
        f(&store)
    }

    fn parse_created_at_or_now(created_at: &str) -> chrono::DateTime<chrono::Utc> {
        created_at.parse().unwrap_or_else(|_| Utc::now())
    }

    /// Create a new plan manager with shared database.
    pub fn with_shared_db(db: SharedDatabase) -> Result<Self> {
        let plans_dir = paths::ensure_plans_dir()?;
        Ok(Self { plans_dir, db })
    }

    /// Create a new plan manager with database path (creates new connection).
    pub fn new(db_path: PathBuf) -> Result<Self> {
        let db = Database::shared(&db_path)?;
        Self::with_shared_db(db)
    }

    /// Get plan for a session (database-backed, no working_dir fallback).
    pub fn get_plan(&self, session_id: &str) -> Result<Option<PlanFile>> {
        self.with_store(|store| store.get_plan_for_session(session_id))
    }

    /// Get the active in-progress plan for a session, filtering out archived plans.
    pub fn get_active_plan(&self, session_id: &str) -> Result<Option<PlanFile>> {
        Ok(self.get_plan(session_id)?.filter(is_active_plan))
    }

    /// Resolve canonical plan lifecycle state for a session.
    pub fn get_lifecycle_state(
        &self,
        session_id: &str,
        work_mode: WorkMode,
    ) -> Result<PlanLifecycleState> {
        Ok(PlanLifecycleState::from_session_mode(
            work_mode,
            self.get_plan(session_id)?,
        ))
    }

    /// Save a plan (creates or updates in database).
    pub fn save_plan_for_session(&self, session_id: &str, plan: &PlanFile) -> Result<()> {
        self.with_store(|store| {
            store.upsert_plan(session_id, plan)?;
            Ok(())
        })
    }

    /// Abandon plan for a session (deletes from database).
    pub fn abandon_plan(&self, session_id: &str) -> Result<bool> {
        self.with_store(|store| store.abandon_plan(session_id))
    }

    /// Check if session has an active plan.
    pub fn has_plan(&self, session_id: &str) -> bool {
        self.with_store(|store| Ok(store.has_plan(session_id)))
            .unwrap_or(false)
    }

    /// Create a new plan for a session.
    pub fn create_plan(
        &self,
        title: &str,
        session_id: &str,
        working_dir: Option<&str>,
    ) -> Result<PlanFile> {
        let mut plan = PlanFile::new(title);
        plan.session_id = Some(session_id.to_string());
        plan.working_dir = working_dir.map(|s| s.to_string());
        self.save_plan_for_session(session_id, &plan)?;
        Ok(plan)
    }

    /// Save a plan to database (legacy API wrapper).
    pub fn save_plan(&self, plan: &PlanFile) -> Result<()> {
        let session_id = plan
            .session_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Plan has no session_id"))?;
        self.save_plan_for_session(session_id, plan)
    }

    /// List completed plans for a working directory (for history).
    pub fn list_completed_for_dir(&self, working_dir: &str) -> Result<Vec<PlanSummary>> {
        self.with_store(|store| {
            Ok(store
                .list_completed_for_working_dir(working_dir)?
                .into_iter()
                .map(|p| PlanSummary {
                    id: p.id,
                    session_id: Some(p.session_id),
                    title: p.title,
                    status: p.status,
                    progress: (0, 0),
                    created_at: Self::parse_created_at_or_now(&p.created_at),
                    working_dir: p.working_dir,
                })
                .collect())
        })
    }
}
