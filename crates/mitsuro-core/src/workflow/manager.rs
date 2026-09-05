use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;

use chrono::Utc;
use rusqlite::types::Type;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use thiserror::Error;

use crate::plan::{PlanFile, TaskStatus};
use crate::storage::{Database, SharedDatabase};

use super::model::{
    AttemptProgressInput, AttemptStatus, CollaborationMode, CompleteStepInput, CreateGoalInput,
    CriterionStatus, EditGoalInput, ExecutionAttempt, Goal, GoalCriterion, GoalStatus,
    PlanProposalInput, PlanRevision, PlanRevisionStatus, SetCriterionInput, StartAttemptInput,
    StepDependency, StepProposalInput, WorkflowMutation, WorkflowSnapshot, WorkflowStep,
    WorkflowStepStatus,
};

const WORKFLOW_SCHEMA_VERSION: u32 = 1;
const MAX_GOAL_OBJECTIVE_CHARS: usize = 4_000;

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("workflow not found: {0}")]
    NotFound(String),
    #[error("workflow conflict: {0}")]
    Conflict(String),
    #[error("invalid workflow transition: {0}")]
    InvalidTransition(String),
    #[error("invalid workflow input: {0}")]
    Validation(String),
    #[error("Worker Workflow requires an attached absolute workspace: {0}")]
    WorkspaceRequired(String),
    #[error("workflow database error: {0}")]
    Database(String),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Canonical transaction boundary for Goal, plan, step, and attempt state.
pub struct WorkflowManager {
    pub(crate) db: SharedDatabase,
}

impl WorkflowManager {
    pub fn new(db_path: PathBuf) -> Result<Self, WorkflowError> {
        let db = Database::shared(&db_path)
            .map_err(|error| WorkflowError::Database(error.to_string()))?;
        Ok(Self { db })
    }

    pub fn with_shared_db(db: SharedDatabase) -> Self {
        Self { db }
    }

    pub fn get_snapshot(
        &self,
        session_id: &str,
    ) -> Result<Option<WorkflowSnapshot>, WorkflowError> {
        let db = self
            .db
            .lock()
            .map_err(|error| WorkflowError::Database(error.to_string()))?;
        load_snapshot(db.conn(), session_id)
    }

    pub fn create_goal(
        &self,
        session_id: &str,
        input: CreateGoalInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        validate_goal_input(&input)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "create_goal")? {
                return Ok(previous);
            }
            ensure_session_exists(tx, session_id)?;
            let unfinished: Option<String> = tx
                .query_row(
                    "SELECT id FROM workflow_goals
                     WHERE session_id = ?1
                       AND status IN ('draft', 'active', 'paused', 'blocked')
                     LIMIT 1",
                    [session_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(goal_id) = unfinished {
                return Err(WorkflowError::Conflict(format!(
                    "session already has unfinished goal {goal_id}"
                )));
            }

            let now = now();
            let goal_id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO workflow_goals (
                    id, session_id, title, objective, constraints_json, status,
                    needs_definition, revision, token_budget, source, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'draft', 0, 1, ?6, 'user', ?7, ?7)",
                params![
                    goal_id,
                    session_id,
                    input.title.trim(),
                    input.objective.trim(),
                    serde_json::to_string(&normalize_strings(&input.constraints))?,
                    input.token_budget.map(to_i64).transpose()?,
                    now
                ],
            )?;
            insert_criteria(tx, &goal_id, &input.criteria)?;
            finish_changed(
                tx,
                "create_goal",
                session_id,
                &goal_id,
                1,
                operation_id,
                "goal_created",
                actor,
                None,
            )
        })
    }

    /// Atomically creates a Goal and its first proposed plan.
    ///
    /// Worker-native authoring cannot repair a draft that is stranded before
    /// its first plan, so validation, both inserts, the event, and the
    /// idempotency receipt intentionally share one transaction.
    pub fn create_goal_with_plan(
        &self,
        session_id: &str,
        goal_input: CreateGoalInput,
        plan_input: PlanProposalInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        validate_goal_input(&goal_input)?;
        validate_plan_input(&plan_input)?;
        self.with_transaction(|tx| {
            if let Some(previous) =
                load_idempotent(tx, session_id, operation_id, "create_goal_with_plan")?
            {
                return Ok(previous);
            }
            ensure_session_exists(tx, session_id)?;
            let unfinished: Option<String> = tx
                .query_row(
                    "SELECT id FROM workflow_goals
                     WHERE session_id = ?1
                       AND status IN ('draft', 'active', 'paused', 'blocked')
                     LIMIT 1",
                    [session_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(goal_id) = unfinished {
                return Err(WorkflowError::Conflict(format!(
                    "session already has unfinished goal {goal_id}"
                )));
            }

            let timestamp = now();
            let goal_id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO workflow_goals (
                    id, session_id, title, objective, constraints_json, status,
                    needs_definition, revision, token_budget, source, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'draft', 0, 1, ?6, 'user', ?7, ?7)",
                params![
                    goal_id,
                    session_id,
                    goal_input.title.trim(),
                    goal_input.objective.trim(),
                    serde_json::to_string(&normalize_strings(&goal_input.constraints))?,
                    goal_input.token_budget.map(to_i64).transpose()?,
                    timestamp
                ],
            )?;
            insert_criteria(tx, &goal_id, &goal_input.criteria)?;

            let plan_id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO workflow_plan_revisions (
                    id, goal_id, revision_number, status, title, rationale,
                    source_message_id, predecessor_id, legacy_markdown, created_at
                 ) VALUES (?1, ?2, 1, 'proposed', ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    plan_id,
                    goal_id,
                    plan_input.title.trim(),
                    clean_optional(plan_input.rationale.as_deref()),
                    plan_input.source_message_id,
                    plan_input.predecessor_id,
                    plan_input.legacy_markdown,
                    timestamp
                ],
            )?;
            insert_steps(tx, &plan_id, &plan_input)?;
            finish_changed(
                tx,
                "create_goal_with_plan",
                session_id,
                &goal_id,
                1,
                operation_id,
                "goal_created_with_plan",
                actor,
                None,
            )
        })
    }

    /// Explicitly adopt the session's legacy Markdown plan into Workflow v2.
    ///
    /// Import never approves or activates execution. A user-supplied Goal
    /// definition is required, legacy task IDs become stable display keys, and
    /// completed evidence is retained. Stale in-progress tasks return to
    /// pending because no Workflow-v2 attempt owns them.
    pub fn import_legacy_plan(
        &self,
        session_id: &str,
        goal_input: CreateGoalInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        validate_goal_input(&goal_input)?;
        self.with_transaction(|tx| {
            if let Some(previous) =
                load_idempotent(tx, session_id, operation_id, "import_legacy_plan")?
            {
                return Ok(previous);
            }
            ensure_session_exists(tx, session_id)?;
            let unfinished: Option<String> = tx
                .query_row(
                    "SELECT id FROM workflow_goals
                     WHERE session_id = ?1
                       AND status IN ('draft', 'active', 'paused', 'blocked')
                     LIMIT 1",
                    [session_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(goal_id) = unfinished {
                return Err(WorkflowError::Conflict(format!(
                    "session already has unfinished goal {goal_id}"
                )));
            }
            let (legacy_plan_id, legacy_markdown): (String, String) = tx
                .query_row(
                    "SELECT id, content FROM plans WHERE session_id = ?1",
                    [session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| {
                    WorkflowError::NotFound(format!("legacy plan for session {session_id}"))
                })?;
            let legacy = PlanFile::from_markdown(&legacy_markdown)
                .map_err(|error| WorkflowError::Validation(format!("legacy plan: {error}")))?;
            let steps = legacy
                .phases
                .iter()
                .flat_map(|phase| phase.tasks.iter())
                .map(|task| StepProposalInput {
                    display_key: task.id.clone(),
                    description: task.description.clone(),
                    context: task.context.clone(),
                    parent_display_key: task.parent_id.clone(),
                    dependencies: task.blocked_by.clone(),
                    acceptance_criteria: Vec::new(),
                    required: true,
                })
                .collect::<Vec<_>>();
            let plan_input = PlanProposalInput {
                title: legacy.title.clone(),
                rationale: Some(
                    "Explicitly imported from the legacy Mitsuro plan store".to_string(),
                ),
                source_message_id: None,
                predecessor_id: None,
                legacy_markdown: Some(legacy_markdown),
                steps,
            };
            validate_plan_input(&plan_input)?;

            let timestamp = now();
            let goal_id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO workflow_goals (
                    id, session_id, title, objective, constraints_json, status,
                    needs_definition, revision, token_budget, source,
                    legacy_plan_id, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 'draft', 0, 1, ?6,
                           'legacy_import', ?7, ?8, ?8)",
                params![
                    goal_id,
                    session_id,
                    goal_input.title.trim(),
                    goal_input.objective.trim(),
                    serde_json::to_string(&normalize_strings(&goal_input.constraints))?,
                    goal_input.token_budget.map(to_i64).transpose()?,
                    legacy_plan_id,
                    timestamp
                ],
            )?;
            insert_criteria(tx, &goal_id, &goal_input.criteria)?;

            let plan_id = uuid::Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO workflow_plan_revisions (
                    id, goal_id, revision_number, status, title, rationale,
                    legacy_markdown, created_at
                 ) VALUES (?1, ?2, 1, 'proposed', ?3, ?4, ?5, ?6)",
                params![
                    plan_id,
                    goal_id,
                    plan_input.title,
                    plan_input.rationale,
                    plan_input.legacy_markdown,
                    timestamp
                ],
            )?;
            insert_steps(tx, &plan_id, &plan_input)?;

            for task in legacy.phases.iter().flat_map(|phase| phase.tasks.iter()) {
                let status = if task.completed || task.status == TaskStatus::Completed {
                    WorkflowStepStatus::Completed
                } else if task.status == TaskStatus::Blocked {
                    WorkflowStepStatus::Blocked
                } else {
                    WorkflowStepStatus::Pending
                };
                let evidence = task
                    .result
                    .as_ref()
                    .map(|result| vec![result.clone()])
                    .unwrap_or_default();
                tx.execute(
                    "UPDATE workflow_plan_steps
                        SET status = ?1, outcome = ?2, evidence_json = ?3,
                            completed_at = CASE WHEN ?1 = 'completed' THEN ?4 ELSE NULL END
                      WHERE plan_revision_id = ?5 AND display_key = ?6",
                    params![
                        status.as_str(),
                        task.result,
                        serde_json::to_string(&evidence)?,
                        task.completed_at.map(|value| value.to_rfc3339()),
                        plan_id,
                        task.id
                    ],
                )?;
            }

            finish_changed(
                tx,
                "import_legacy_plan",
                session_id,
                &goal_id,
                1,
                operation_id,
                "legacy_plan_imported",
                actor,
                None,
            )
        })
    }

    pub fn edit_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        input: EditGoalInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "edit_goal")? {
                return Ok(previous);
            }
            let current = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if !current.status.is_unfinished() {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot edit {} goal",
                    current.status
                )));
            }

            let title = input
                .title
                .as_deref()
                .unwrap_or(&current.title)
                .trim()
                .to_string();
            let objective = input
                .objective
                .as_deref()
                .unwrap_or(&current.objective)
                .trim()
                .to_string();
            validate_title_and_objective(&title, &objective)?;
            let constraints = input
                .constraints
                .as_ref()
                .map(|items| normalize_strings(items))
                .unwrap_or_else(|| current.constraints.clone());
            let token_budget = match input.token_budget {
                Some(value) => value,
                None => current.token_budget,
            };
            if token_budget == Some(0) {
                return Err(WorkflowError::Validation(
                    "token budget must be greater than zero".to_string(),
                ));
            }

            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let edited_while_active = current.status == GoalStatus::Active;
            tx.execute(
                "UPDATE workflow_goals
                    SET title = ?1,
                        objective = ?2,
                        constraints_json = ?3,
                        token_budget = ?4,
                        needs_definition = 0,
                        status = CASE WHEN status = 'active' THEN 'paused' ELSE status END,
                        status_reason = CASE
                            WHEN status = 'active' THEN 'goal_edited_replan_required'
                            ELSE status_reason
                        END,
                        updated_at = ?5
                  WHERE id = ?6 AND session_id = ?7",
                params![
                    title,
                    objective,
                    serde_json::to_string(&constraints)?,
                    token_budget.map(to_i64).transpose()?,
                    now(),
                    goal_id,
                    session_id
                ],
            )?;
            if let Some(criteria) = &input.criteria {
                if criteria.is_empty() {
                    return Err(WorkflowError::Validation(
                        "a durable goal requires at least one verification criterion".to_string(),
                    ));
                }
                tx.execute(
                    "DELETE FROM workflow_goal_criteria WHERE goal_id = ?1",
                    [goal_id],
                )?;
                insert_criteria(tx, goal_id, criteria)?;
            }
            if edited_while_active {
                pause_running_attempt(tx, goal_id, "goal_edited_replan_required")?;
            }
            finish_changed(
                tx,
                "edit_goal",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "goal_edited",
                actor,
                None,
            )
        })
    }

    pub fn propose_plan(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        input: PlanProposalInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        validate_plan_input(&input)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "propose_plan")? {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if !goal.status.is_unfinished() {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot propose a plan for {} goal",
                    goal.status
                )));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let revision_number: i64 = tx.query_row(
                "SELECT COALESCE(MAX(revision_number), 0) + 1
                   FROM workflow_plan_revisions WHERE goal_id = ?1",
                [goal_id],
                |row| row.get(0),
            )?;
            let predecessor_id = input.predecessor_id.clone().or_else(|| {
                tx.query_row(
                    "SELECT id FROM workflow_plan_revisions
                     WHERE goal_id = ?1
                     ORDER BY revision_number DESC LIMIT 1",
                    [goal_id],
                    |row| row.get(0),
                )
                .optional()
                .ok()
                .flatten()
            });
            tx.execute(
                "UPDATE workflow_plan_revisions
                    SET status = 'superseded'
                  WHERE goal_id = ?1 AND status = 'proposed'",
                [goal_id],
            )?;
            let plan_id = uuid::Uuid::new_v4().to_string();
            let created_at = now();
            tx.execute(
                "INSERT INTO workflow_plan_revisions (
                    id, goal_id, revision_number, status, title, rationale,
                    source_message_id, predecessor_id, legacy_markdown, created_at
                 ) VALUES (?1, ?2, ?3, 'proposed', ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    plan_id,
                    goal_id,
                    revision_number,
                    input.title.trim(),
                    clean_optional(input.rationale.as_deref()),
                    input.source_message_id,
                    predecessor_id,
                    input.legacy_markdown,
                    created_at
                ],
            )?;
            insert_steps(tx, &plan_id, &input)?;
            if goal.status == GoalStatus::Active {
                tx.execute(
                    "UPDATE workflow_goals
                        SET status = 'paused',
                            status_reason = 'plan_revision_pending_approval',
                            updated_at = ?1
                      WHERE id = ?2 AND session_id = ?3",
                    params![now(), goal_id, session_id],
                )?;
                pause_running_attempt(tx, goal_id, "plan_revision_pending_approval")?;
            }
            finish_changed(
                tx,
                "propose_plan",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "plan_proposed",
                actor,
                None,
            )
        })
    }

    pub fn approve_plan(
        &self,
        session_id: &str,
        goal_id: &str,
        plan_revision_id: &str,
        expected_revision: u64,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "approve_plan")? {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if !goal.status.is_unfinished() {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot approve a plan for {} goal",
                    goal.status
                )));
            }
            let plan_status = load_plan_status(tx, goal_id, plan_revision_id)?;
            if plan_status == PlanRevisionStatus::Active {
                return finish_noop(tx, session_id, operation_id, "approve_plan");
            }
            if plan_status != PlanRevisionStatus::Proposed {
                return Err(WorkflowError::InvalidTransition(format!(
                    "only a proposed plan can be approved, found {plan_status}"
                )));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            tx.execute(
                "UPDATE workflow_plan_revisions
                    SET status = 'superseded'
                  WHERE goal_id = ?1 AND status IN ('active', 'approved')",
                [goal_id],
            )?;
            tx.execute(
                "UPDATE workflow_plan_revisions
                    SET status = 'active', approved_at = ?1
                  WHERE id = ?2 AND goal_id = ?3",
                params![now(), plan_revision_id, goal_id],
            )?;
            finish_changed(
                tx,
                "approve_plan",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "plan_approved",
                actor,
                None,
            )
        })
    }

    pub fn activate_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "activate_goal")?
            {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status == GoalStatus::Active {
                return finish_noop(tx, session_id, operation_id, "activate_goal");
            }
            if !matches!(
                goal.status,
                GoalStatus::Draft | GoalStatus::Paused | GoalStatus::Blocked
            ) {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot activate {} goal",
                    goal.status
                )));
            }
            if goal.needs_definition || goal.objective.trim().is_empty() {
                return Err(WorkflowError::Validation(
                    "goal must be explicitly defined before activation".to_string(),
                ));
            }
            let criteria_count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM workflow_goal_criteria WHERE goal_id = ?1",
                [goal_id],
                |row| row.get(0),
            )?;
            if criteria_count == 0 {
                return Err(WorkflowError::Validation(
                    "goal requires verification criteria before activation".to_string(),
                ));
            }
            let active_plan_count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM workflow_plan_revisions
                 WHERE goal_id = ?1 AND status = 'active'",
                [goal_id],
                |row| row.get(0),
            )?;
            if active_plan_count != 1 {
                return Err(WorkflowError::Validation(
                    "exactly one approved plan revision is required before activation".to_string(),
                ));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            tx.execute(
                "UPDATE workflow_goals
                    SET status = 'active', status_reason = NULL,
                        activated_at = COALESCE(activated_at, ?1), updated_at = ?1
                  WHERE id = ?2 AND session_id = ?3",
                params![now(), goal_id, session_id],
            )?;
            finish_changed(
                tx,
                "activate_goal",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "goal_activated",
                actor,
                None,
            )
        })
    }

    pub fn pause_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        reason: Option<&str>,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        self.transition_goal(
            session_id,
            goal_id,
            expected_revision,
            GoalStatus::Active,
            GoalStatus::Paused,
            reason.unwrap_or("paused_by_user"),
            operation_id,
            actor,
            "goal_paused",
        )
    }

    pub fn resume_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "resume_goal")? {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status == GoalStatus::Active {
                return finish_noop(tx, session_id, operation_id, "resume_goal");
            }
            if !matches!(goal.status, GoalStatus::Paused | GoalStatus::Blocked) {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot resume {} goal",
                    goal.status
                )));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            tx.execute(
                "UPDATE workflow_goals
                    SET status = 'active', status_reason = NULL, updated_at = ?1
                  WHERE id = ?2 AND session_id = ?3",
                params![now(), goal_id, session_id],
            )?;
            finish_changed(
                tx,
                "resume_goal",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "goal_resumed",
                actor,
                None,
            )
        })
    }

    /// Persist a controller-observed blocker. Waiting for one approval or a
    /// transient failure should use an attempt stop instead; this transition is
    /// for a repeated blocker that prevents meaningful progress.
    pub fn block_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        reason: &str,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        if reason.trim().is_empty() {
            return Err(WorkflowError::Validation(
                "blocked goals require a concrete reason".to_string(),
            ));
        }
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "block_goal")? {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status == GoalStatus::Blocked
                && goal.status_reason.as_deref() == Some(reason.trim())
            {
                return finish_noop(tx, session_id, operation_id, "block_goal");
            }
            if goal.status != GoalStatus::Active {
                return Err(WorkflowError::InvalidTransition(format!(
                    "only an active goal can become blocked, found {}",
                    goal.status
                )));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let timestamp = now();
            tx.execute(
                "UPDATE workflow_goals
                    SET status = 'blocked', status_reason = ?1, updated_at = ?2
                  WHERE id = ?3 AND session_id = ?4",
                params![reason.trim(), timestamp, goal_id, session_id],
            )?;
            let attempt_id: Option<String> = tx
                .query_row(
                    "SELECT id FROM workflow_execution_attempts
                     WHERE goal_id = ?1 AND status = 'running'",
                    [goal_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(attempt_id) = attempt_id.as_deref() {
                tx.execute(
                    "UPDATE workflow_execution_attempts
                        SET status = 'paused', stop_reason = ?1,
                            ended_at = ?2, updated_at = ?2
                      WHERE id = ?3",
                    params![reason.trim(), timestamp, attempt_id],
                )?;
                release_attempt_step(tx, attempt_id, WorkflowStepStatus::Blocked)?;
            }
            finish_changed(
                tx,
                "block_goal",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "goal_blocked",
                actor,
                attempt_id.as_deref(),
            )
        })
    }

    pub fn cancel_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        reason: Option<&str>,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "cancel_goal")? {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status == GoalStatus::Cancelled {
                return finish_noop(tx, session_id, operation_id, "cancel_goal");
            }
            if !goal.status.is_unfinished() {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot cancel {} goal",
                    goal.status
                )));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let timestamp = now();
            tx.execute(
                "UPDATE workflow_goals
                    SET status = 'cancelled', status_reason = ?1,
                        cancelled_at = ?2, updated_at = ?2
                  WHERE id = ?3 AND session_id = ?4",
                params![
                    reason.unwrap_or("cancelled_by_user"),
                    timestamp,
                    goal_id,
                    session_id
                ],
            )?;
            tx.execute(
                "UPDATE workflow_execution_attempts
                    SET status = 'cancelled', stop_reason = 'goal_cancelled',
                        ended_at = ?1, updated_at = ?1
                  WHERE goal_id = ?2 AND status = 'running'",
                params![timestamp, goal_id],
            )?;
            tx.execute(
                "UPDATE workflow_plan_steps
                    SET status = 'cancelled', claimed_attempt_id = NULL,
                        revision = revision + 1
                  WHERE plan_revision_id IN (
                    SELECT id FROM workflow_plan_revisions WHERE goal_id = ?1
                  ) AND status IN ('pending', 'in_progress', 'blocked')",
                [goal_id],
            )?;
            tx.execute(
                "UPDATE workflow_plan_revisions
                    SET status = 'cancelled'
                  WHERE goal_id = ?1 AND status IN ('proposed', 'approved', 'active')",
                [goal_id],
            )?;
            finish_changed(
                tx,
                "cancel_goal",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "goal_cancelled",
                actor,
                None,
            )
        })
    }

    pub fn start_attempt(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        input: StartAttemptInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        validate_attempt_input(&input)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "start_attempt")?
            {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status != GoalStatus::Active {
                return Err(WorkflowError::InvalidTransition(format!(
                    "attempts require an active goal, found {}",
                    goal.status
                )));
            }
            let running_attempt: Option<String> = tx
                .query_row(
                    "SELECT id FROM workflow_execution_attempts
                     WHERE goal_id = ?1 AND status = 'running' LIMIT 1",
                    [goal_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(attempt_id) = running_attempt {
                return Err(WorkflowError::Conflict(format!(
                    "goal already has running attempt {attempt_id}"
                )));
            }
            let plan_id: String = tx
                .query_row(
                    "SELECT id FROM workflow_plan_revisions
                     WHERE goal_id = ?1
                       AND (status = 'active' OR (?2 = 1 AND status = 'completed'))
                     ORDER BY CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                              revision_number DESC
                     LIMIT 1",
                    params![goal_id, input.step_id.is_none()],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| {
                    WorkflowError::InvalidTransition(
                        "active goal has no approved plan revision".to_string(),
                    )
                })?;
            if let Some(step_id) = input.step_id.as_deref() {
                validate_step_claim(tx, &plan_id, step_id)?;
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let attempt_id = uuid::Uuid::new_v4().to_string();
            let timestamp = now();
            tx.execute(
                "INSERT INTO workflow_execution_attempts (
                    id, goal_id, plan_revision_id, step_id, status, permission_mode,
                    goal_revision_at_start, max_turns, max_tool_calls,
                    max_wall_time_secs, max_research_actions, started_at, updated_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, 'running', ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11
                 )",
                params![
                    attempt_id,
                    goal_id,
                    plan_id,
                    input.step_id,
                    input.permission_mode,
                    to_i64(new_revision)?,
                    input.max_turns,
                    input.max_tool_calls,
                    to_i64(input.max_wall_time_secs)?,
                    input.max_research_actions,
                    timestamp
                ],
            )?;
            if let Some(step_id) = input.step_id.as_deref() {
                tx.execute(
                    "UPDATE workflow_plan_steps
                        SET status = 'in_progress', claimed_attempt_id = ?1,
                            revision = revision + 1, started_at = COALESCE(started_at, ?2)
                      WHERE id = ?3 AND plan_revision_id = ?4 AND status = 'pending'",
                    params![attempt_id, timestamp, step_id, plan_id],
                )?;
            }
            finish_changed(
                tx,
                "start_attempt",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "attempt_started",
                actor,
                Some(&attempt_id),
            )
        })
    }

    /// Atomically claim one step for an already-running attempt.
    ///
    /// Repeating the same claim for the same attempt is a semantic no-op:
    /// it writes no state, emits no workflow event, and cannot earn progress.
    pub fn claim_step(
        &self,
        session_id: &str,
        goal_id: &str,
        attempt_id: &str,
        step_id: &str,
        expected_revision: u64,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "claim_step")? {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status != GoalStatus::Active {
                return Err(WorkflowError::InvalidTransition(format!(
                    "step claims require an active goal, found {}",
                    goal.status
                )));
            }
            let attempt = load_attempt(tx, goal_id, attempt_id)?;
            if attempt.status != AttemptStatus::Running {
                return Err(WorkflowError::InvalidTransition(
                    "step claims require a running attempt".to_string(),
                ));
            }
            let (status, claimed_attempt_id, plan_id): (
                WorkflowStepStatus,
                Option<String>,
                String,
            ) = tx
                .query_row(
                    "SELECT status, claimed_attempt_id, plan_revision_id
                       FROM workflow_plan_steps WHERE id = ?1",
                    [step_id],
                    |row| {
                        Ok((
                            parse_sql_enum(row.get::<_, String>(0)?, 0)?,
                            row.get(1)?,
                            row.get(2)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| WorkflowError::NotFound(format!("step {step_id}")))?;
            if status == WorkflowStepStatus::InProgress
                && claimed_attempt_id.as_deref() == Some(attempt_id)
            {
                return finish_noop(tx, session_id, operation_id, "claim_step");
            }
            if status == WorkflowStepStatus::InProgress {
                return Err(WorkflowError::Conflict(
                    "step is claimed by another running attempt".to_string(),
                ));
            }
            if let Some(existing_step_id) = attempt.step_id.as_deref() {
                if existing_step_id != step_id {
                    return Err(WorkflowError::Conflict(format!(
                        "attempt already owns step {existing_step_id}"
                    )));
                }
            }
            validate_step_claim(tx, &plan_id, step_id)?;
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let timestamp = now();
            tx.execute(
                "UPDATE workflow_plan_steps
                    SET status = 'in_progress', claimed_attempt_id = ?1,
                        revision = revision + 1, started_at = COALESCE(started_at, ?2)
                  WHERE id = ?3 AND status = 'pending'",
                params![attempt_id, timestamp, step_id],
            )?;
            tx.execute(
                "UPDATE workflow_execution_attempts
                    SET step_id = ?1, plan_revision_id = ?2, updated_at = ?3
                  WHERE id = ?4 AND status = 'running'",
                params![step_id, plan_id, timestamp, attempt_id],
            )?;
            finish_changed(
                tx,
                "claim_step",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "step_claimed",
                actor,
                Some(attempt_id),
            )
        })
    }

    /// Account the exact attempt admitted for a provider batch. Running
    /// attempts evaluate progress and limits. Already terminal attempts accept
    /// only monotonic counter settlement; their lifecycle, blocker history,
    /// and any successor attempt remain unchanged.
    pub fn record_attempt_progress(
        &self,
        session_id: &str,
        goal_id: &str,
        attempt_id: &str,
        expected_revision: u64,
        input: AttemptProgressInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) =
                load_idempotent(tx, session_id, operation_id, "record_attempt_progress")?
            {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            let attempt = load_attempt(tx, goal_id, attempt_id)?;
            if goal.status != GoalStatus::Active && attempt.status == AttemptStatus::Running {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot record running execution against {} goal",
                    goal.status
                )));
            }
            if input.turn_count < attempt.turn_count
                || input.tool_call_count < attempt.tool_call_count
                || input.research_action_count < attempt.research_action_count
            {
                return Err(WorkflowError::Validation(
                    "attempt counters must be monotonic".to_string(),
                ));
            }
            // A lifecycle tool may finish this attempt (and even start its
            // successor) inside the provider batch being accounted. Settle
            // usage on the admitted attempt without reopening it or applying
            // its limits/blocker state to the successor.
            if attempt.status != AttemptStatus::Running {
                if input.turn_count == attempt.turn_count
                    && input.tool_call_count == attempt.tool_call_count
                    && input.research_action_count == attempt.research_action_count
                {
                    return finish_noop(tx, session_id, operation_id, "settle_attempt_usage");
                }
                let revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
                tx.execute(
                    "UPDATE workflow_execution_attempts
                        SET turn_count = ?1, tool_call_count = ?2, research_action_count = ?3,
                            updated_at = ?4
                      WHERE id = ?5 AND goal_id = ?6",
                    params![
                        input.turn_count,
                        input.tool_call_count,
                        input.research_action_count,
                        now(),
                        attempt_id,
                        goal_id
                    ],
                )?;
                return finish_changed(
                    tx,
                    "record_attempt_progress",
                    session_id,
                    goal_id,
                    revision,
                    operation_id,
                    "terminal_attempt_usage_settled",
                    actor,
                    Some(attempt_id),
                );
            }
            let blocker_streak = match (
                input.blocker_fingerprint.as_deref(),
                attempt.blocker_fingerprint.as_deref(),
            ) {
                (Some(current), Some(previous)) if current == previous => {
                    attempt.blocker_streak.saturating_add(1)
                }
                (Some(_), _) => 1,
                (None, _) => 0,
            };
            let elapsed_secs = chrono::DateTime::parse_from_rfc3339(&attempt.started_at)
                .ok()
                .map(|started| {
                    Utc::now()
                        .signed_duration_since(started.with_timezone(&Utc))
                        .num_seconds()
                        .max(0) as u64
                })
                .unwrap_or(0);
            let stop_reason = if elapsed_secs >= attempt.max_wall_time_secs {
                Some("wall_time_budget_exhausted")
            } else if input.turn_count >= attempt.max_turns {
                Some("turn_budget_exhausted")
            } else if input.tool_call_count >= attempt.max_tool_calls {
                Some("tool_budget_exhausted")
            } else if input.research_action_count >= attempt.max_research_actions {
                Some("research_budget_exhausted")
            } else if blocker_streak >= 3 {
                Some("repeated_blocker")
            } else {
                None
            };
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            tx.execute(
                "UPDATE workflow_execution_attempts
                    SET turn_count = ?1, tool_call_count = ?2,
                        research_action_count = ?3,
                        progress_revision = progress_revision + ?4,
                        blocker_fingerprint = ?5, blocker_streak = ?6,
                        updated_at = ?7
                  WHERE id = ?8 AND goal_id = ?9 AND status = 'running'",
                params![
                    input.turn_count,
                    input.tool_call_count,
                    input.research_action_count,
                    if input.material_progress { 1 } else { 0 },
                    input.blocker_fingerprint,
                    blocker_streak,
                    now(),
                    attempt_id,
                    goal_id
                ],
            )?;
            let event_type = if let Some(reason) = stop_reason {
                let timestamp = now();
                tx.execute(
                    "UPDATE workflow_execution_attempts
                        SET status = 'paused', stop_reason = ?1,
                            ended_at = ?2, updated_at = ?2
                      WHERE id = ?3",
                    params![reason, timestamp, attempt_id],
                )?;
                if reason == "repeated_blocker" {
                    tx.execute(
                        "UPDATE workflow_goals
                            SET status = 'blocked', status_reason = ?1, updated_at = ?2
                          WHERE id = ?3",
                        params![reason, timestamp, goal_id],
                    )?;
                    release_attempt_step(tx, attempt_id, WorkflowStepStatus::Blocked)?;
                    "goal_blocked"
                } else {
                    // A bounded attempt is a resumable execution slice, not a
                    // terminal budget for the approved Goal. Keep the Goal
                    // active and release the step so the same parent loop (or
                    // a later continuation) can claim a fresh attempt.
                    release_attempt_step(tx, attempt_id, WorkflowStepStatus::Pending)?;
                    "attempt_budget_exhausted"
                }
            } else if input.material_progress {
                "attempt_progressed"
            } else {
                "attempt_observed"
            };
            finish_changed(
                tx,
                "record_attempt_progress",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                event_type,
                actor,
                Some(attempt_id),
            )
        })
    }

    pub fn complete_step(
        &self,
        session_id: &str,
        goal_id: &str,
        step_id: &str,
        expected_revision: u64,
        input: CompleteStepInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        if input.outcome.trim().is_empty() || normalize_strings(&input.evidence).is_empty() {
            return Err(WorkflowError::Validation(
                "step completion requires an outcome and concrete evidence".to_string(),
            ));
        }
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "complete_step")?
            {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status != GoalStatus::Active {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot complete a step for {} goal",
                    goal.status
                )));
            }
            let (status, claimed_attempt_id, plan_id): (
                WorkflowStepStatus,
                Option<String>,
                String,
            ) = tx
                .query_row(
                    "SELECT status, claimed_attempt_id, plan_revision_id
                       FROM workflow_plan_steps WHERE id = ?1",
                    [step_id],
                    |row| {
                        Ok((
                            parse_sql_enum(row.get::<_, String>(0)?, 0)?,
                            row.get(1)?,
                            row.get(2)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| WorkflowError::NotFound(format!("step {step_id}")))?;
            if status == WorkflowStepStatus::Completed {
                return finish_noop(tx, session_id, operation_id, "complete_step");
            }
            if status != WorkflowStepStatus::InProgress
                || claimed_attempt_id.as_deref() != Some(input.attempt_id.as_str())
            {
                return Err(WorkflowError::Conflict(
                    "step is not claimed by the supplied running attempt".to_string(),
                ));
            }
            let attempt = load_attempt(tx, goal_id, &input.attempt_id)?;
            if attempt.status != AttemptStatus::Running {
                return Err(WorkflowError::InvalidTransition(
                    "step attempt is no longer running".to_string(),
                ));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let timestamp = now();
            tx.execute(
                "UPDATE workflow_plan_steps
                    SET status = 'completed', outcome = ?1, evidence_json = ?2,
                        claimed_attempt_id = NULL, revision = revision + 1,
                        completed_at = ?3
                  WHERE id = ?4",
                params![
                    input.outcome.trim(),
                    serde_json::to_string(&normalize_strings(&input.evidence))?,
                    timestamp,
                    step_id
                ],
            )?;
            tx.execute(
                "UPDATE workflow_execution_attempts
                    SET status = 'succeeded', stop_reason = 'step_completed',
                        progress_revision = progress_revision + 1,
                        ended_at = ?1, updated_at = ?1
                  WHERE id = ?2",
                params![timestamp, input.attempt_id],
            )?;
            let incomplete_required: i64 = tx.query_row(
                "SELECT COUNT(*) FROM workflow_plan_steps
                 WHERE plan_revision_id = ?1 AND required = 1
                   AND status NOT IN ('completed', 'skipped')",
                [plan_id.as_str()],
                |row| row.get(0),
            )?;
            if incomplete_required == 0 {
                tx.execute(
                    "UPDATE workflow_plan_revisions
                        SET status = 'completed', completed_at = ?1
                      WHERE id = ?2 AND status = 'active'",
                    params![timestamp, plan_id],
                )?;
            }
            finish_changed(
                tx,
                "complete_step",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "step_completed",
                actor,
                Some(&input.attempt_id),
            )
        })
    }

    pub fn finish_attempt(
        &self,
        session_id: &str,
        goal_id: &str,
        attempt_id: &str,
        expected_revision: u64,
        status: AttemptStatus,
        stop_reason: &str,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        if status == AttemptStatus::Running || stop_reason.trim().is_empty() {
            return Err(WorkflowError::Validation(
                "finishing an attempt requires a terminal status and stop reason".to_string(),
            ));
        }
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "finish_attempt")?
            {
                return Ok(previous);
            }
            let _goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            let attempt = load_attempt(tx, goal_id, attempt_id)?;
            if attempt.status != AttemptStatus::Running {
                return finish_noop(tx, session_id, operation_id, "finish_attempt");
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let timestamp = now();
            tx.execute(
                "UPDATE workflow_execution_attempts
                    SET status = ?1, stop_reason = ?2, ended_at = ?3, updated_at = ?3
                  WHERE id = ?4 AND goal_id = ?5",
                params![
                    status.as_str(),
                    stop_reason.trim(),
                    timestamp,
                    attempt_id,
                    goal_id
                ],
            )?;
            release_attempt_step(tx, attempt_id, WorkflowStepStatus::Pending)?;
            finish_changed(
                tx,
                "finish_attempt",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "attempt_finished",
                actor,
                Some(attempt_id),
            )
        })
    }

    pub fn set_criterion(
        &self,
        session_id: &str,
        goal_id: &str,
        criterion_id: &str,
        expected_revision: u64,
        input: SetCriterionInput,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        if input.status == CriterionStatus::Pending {
            return Err(WorkflowError::Validation(
                "criterion result cannot be pending".to_string(),
            ));
        }
        if input.status == CriterionStatus::Waived && actor != "user" {
            return Err(WorkflowError::InvalidTransition(
                "only the user may waive a required criterion".to_string(),
            ));
        }
        let evidence = normalize_strings(&input.evidence);
        if matches!(
            input.status,
            CriterionStatus::Passed | CriterionStatus::Failed
        ) && evidence.is_empty()
        {
            return Err(WorkflowError::Validation(
                "passed or failed criteria require evidence".to_string(),
            ));
        }
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "set_criterion")?
            {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if !goal.status.is_unfinished() {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot verify {} goal",
                    goal.status
                )));
            }
            let existing: Option<String> = tx
                .query_row(
                    "SELECT status FROM workflow_goal_criteria
                     WHERE id = ?1 AND goal_id = ?2",
                    params![criterion_id, goal_id],
                    |row| row.get(0),
                )
                .optional()?;
            let existing = existing
                .ok_or_else(|| WorkflowError::NotFound(format!("criterion {criterion_id}")))?;
            if existing == input.status.as_str() {
                return finish_noop(tx, session_id, operation_id, "set_criterion");
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            tx.execute(
                "UPDATE workflow_goal_criteria
                    SET status = ?1, evidence_json = ?2, verifier = ?3, verified_at = ?4
                  WHERE id = ?5 AND goal_id = ?6",
                params![
                    input.status.as_str(),
                    serde_json::to_string(&evidence)?,
                    input.verifier.trim(),
                    now(),
                    criterion_id,
                    goal_id
                ],
            )?;
            finish_changed(
                tx,
                "set_criterion",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "criterion_updated",
                actor,
                None,
            )
        })
    }

    pub fn complete_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        operation_id: &str,
        actor: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "complete_goal")?
            {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status == GoalStatus::Completed {
                return finish_noop(tx, session_id, operation_id, "complete_goal");
            }
            if !matches!(
                goal.status,
                GoalStatus::Active | GoalStatus::Paused | GoalStatus::Blocked
            ) {
                return Err(WorkflowError::InvalidTransition(format!(
                    "cannot complete {} goal",
                    goal.status
                )));
            }
            let running_step_attempts: i64 = tx.query_row(
                "SELECT COUNT(*) FROM workflow_execution_attempts
                 WHERE goal_id = ?1 AND status = 'running' AND step_id IS NOT NULL",
                [goal_id],
                |row| row.get(0),
            )?;
            if running_step_attempts > 0 {
                return Err(WorkflowError::InvalidTransition(
                    "cannot complete a goal while an attempt is running".to_string(),
                ));
            }
            let unmet_criteria: i64 = tx.query_row(
                "SELECT COUNT(*) FROM workflow_goal_criteria
                 WHERE goal_id = ?1 AND required = 1
                   AND status NOT IN ('passed', 'waived')",
                [goal_id],
                |row| row.get(0),
            )?;
            if unmet_criteria > 0 {
                return Err(WorkflowError::InvalidTransition(format!(
                    "{unmet_criteria} required verification criteria remain unmet"
                )));
            }
            let incomplete_steps: i64 = tx.query_row(
                "SELECT COUNT(*) FROM workflow_plan_steps
                 WHERE plan_revision_id IN (
                    SELECT id FROM workflow_plan_revisions
                    WHERE goal_id = ?1 AND status IN ('active', 'completed')
                 )
                   AND required = 1
                   AND status NOT IN ('completed', 'skipped')",
                [goal_id],
                |row| row.get(0),
            )?;
            if incomplete_steps > 0 {
                return Err(WorkflowError::InvalidTransition(format!(
                    "{incomplete_steps} required plan steps remain incomplete"
                )));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            let timestamp = now();
            // Final verification is a bounded step-less attempt. Once all
            // required work and evidence pass, settle that attempt in the same
            // transaction as the Goal; it cannot block its own completion.
            tx.execute(
                "UPDATE workflow_execution_attempts
                    SET status = 'succeeded', stop_reason = 'goal_verified',
                        ended_at = ?1, updated_at = ?1
                  WHERE goal_id = ?2 AND status = 'running' AND step_id IS NULL",
                params![timestamp, goal_id],
            )?;
            tx.execute(
                "UPDATE workflow_goals
                    SET status = 'completed', status_reason = 'verified',
                        completed_at = ?1, updated_at = ?1
                  WHERE id = ?2 AND session_id = ?3",
                params![timestamp, goal_id, session_id],
            )?;
            finish_changed(
                tx,
                "complete_goal",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                "goal_completed",
                actor,
                None,
            )
        })
    }

    /// Reconcile attempts that cannot still be running after a server restart.
    ///
    /// This is intentionally called by the owning runtime at startup, not on
    /// every database open, so a read-only client cannot pause another live
    /// process's Goal.
    pub fn recover_interrupted_attempts(&self) -> Result<usize, WorkflowError> {
        self.with_transaction(|tx| {
            let interrupted = {
                let mut statement = tx.prepare(
                    "SELECT attempt.id, goal.id, goal.session_id, goal.revision
                       FROM workflow_execution_attempts attempt
                       JOIN workflow_goals goal ON goal.id = attempt.goal_id
                      WHERE attempt.status = 'running'
                        AND NOT EXISTS (
                            SELECT 1 FROM hive_runs run
                            WHERE run.kind = 'worker_workflow'
                              AND run.workflow_attempt_id = attempt.id
                        )",
                )?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            from_i64(row.get(3)?, 3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            };
            let timestamp = now();
            for (attempt_id, goal_id, session_id, revision) in &interrupted {
                tx.execute(
                    "UPDATE workflow_execution_attempts
                        SET status = 'paused', stop_reason = 'runtime_restarted',
                            ended_at = ?1, updated_at = ?1
                      WHERE id = ?2 AND status = 'running'",
                    params![timestamp, attempt_id],
                )?;
                release_attempt_step(tx, attempt_id, WorkflowStepStatus::Pending)?;
                let new_revision = revision.checked_add(1).ok_or_else(|| {
                    WorkflowError::Validation("workflow revision overflow".to_string())
                })?;
                tx.execute(
                    "UPDATE workflow_goals
                        SET status = CASE WHEN status = 'active' THEN 'paused' ELSE status END,
                            status_reason = CASE
                                WHEN status = 'active' THEN 'runtime_restarted'
                                ELSE status_reason
                            END,
                            revision = ?1, updated_at = ?2
                      WHERE id = ?3 AND session_id = ?4 AND revision = ?5",
                    params![
                        to_i64(new_revision)?,
                        timestamp,
                        goal_id,
                        session_id,
                        to_i64(*revision)?
                    ],
                )?;
                tx.execute(
                    "INSERT INTO workflow_events (
                        session_id, goal_id, aggregate_revision, operation_id,
                        event_type, actor, attempt_id, payload_json, created_at
                     ) VALUES (?1, ?2, ?3, ?4, 'attempt_recovered',
                               'runtime', ?5, ?6, ?7)",
                    params![
                        session_id,
                        goal_id,
                        to_i64(new_revision)?,
                        format!("startup-recovery-{}", uuid::Uuid::new_v4()),
                        attempt_id,
                        serde_json::json!({
                            "changed": true,
                            "goal_status": "paused",
                            "stop_reason": "runtime_restarted",
                        })
                        .to_string(),
                        timestamp
                    ],
                )?;
            }
            Ok(interrupted.len())
        })
    }

    /// Account provider usage against an active Goal. Ordinary accounting does
    /// not churn the semantic aggregate revision; crossing the optional budget
    /// is a lifecycle transition and therefore does.
    pub fn record_token_usage(
        &self,
        session_id: &str,
        token_delta: u64,
    ) -> Result<Option<WorkflowMutation>, WorkflowError> {
        self.record_token_usage_scoped(session_id, None, token_delta)
    }

    /// Bind provider usage to the Goal that admitted the request. A replaced
    /// Goal cannot inherit charges from an older in-flight provider call.
    /// Matching inactive Goals accept late usage without changing lifecycle
    /// state or reapplying a budget transition.
    pub fn record_token_usage_for_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        token_delta: u64,
    ) -> Result<Option<WorkflowMutation>, WorkflowError> {
        self.record_token_usage_scoped(session_id, Some(goal_id), token_delta)
    }

    fn record_token_usage_scoped(
        &self,
        session_id: &str,
        admitted_goal_id: Option<&str>,
        token_delta: u64,
    ) -> Result<Option<WorkflowMutation>, WorkflowError> {
        if token_delta == 0 {
            return Ok(None);
        }
        self.with_transaction(|tx| {
            let Some(goal) = load_current_goal(tx, session_id)? else {
                return Ok(None);
            };
            if admitted_goal_id.is_some_and(|goal_id| goal_id != goal.id) {
                return Err(WorkflowError::Conflict(
                    "the admitted Goal changed before provider usage could be settled".into(),
                ));
            }
            if goal.status != GoalStatus::Active && admitted_goal_id.is_none() {
                return Ok(None);
            }
            let tokens_used = goal.tokens_used.saturating_add(token_delta);
            tx.execute(
                "UPDATE workflow_goals
                    SET tokens_used = ?1, updated_at = ?2
                  WHERE id = ?3 AND session_id = ?4",
                params![to_i64(tokens_used)?, now(), goal.id, session_id],
            )?;
            if goal.status != GoalStatus::Active
                || goal
                    .token_budget
                    .is_none_or(|token_budget| tokens_used < token_budget)
            {
                return Ok(None);
            }

            let new_revision = goal.revision.checked_add(1).ok_or_else(|| {
                WorkflowError::Validation("workflow revision overflow".to_string())
            })?;
            tx.execute(
                "UPDATE workflow_goals
                    SET status = 'paused', status_reason = 'token_budget_exhausted',
                        revision = ?1, updated_at = ?2
                  WHERE id = ?3 AND session_id = ?4 AND revision = ?5",
                params![
                    to_i64(new_revision)?,
                    now(),
                    goal.id,
                    session_id,
                    to_i64(goal.revision)?
                ],
            )?;
            pause_running_attempt(tx, &goal.id, "token_budget_exhausted")?;
            let operation_id = format!("token-budget-{}", uuid::Uuid::new_v4());
            finish_changed(
                tx,
                "record_token_usage",
                session_id,
                &goal.id,
                new_revision,
                &operation_id,
                "goal_token_budget_exhausted",
                "runtime",
                None,
            )
            .map(Some)
        })
    }

    fn transition_goal(
        &self,
        session_id: &str,
        goal_id: &str,
        expected_revision: u64,
        from: GoalStatus,
        to: GoalStatus,
        reason: &str,
        operation_id: &str,
        actor: &str,
        event_type: &str,
    ) -> Result<WorkflowMutation, WorkflowError> {
        validate_operation(operation_id, actor)?;
        self.with_transaction(|tx| {
            if let Some(previous) = load_idempotent(tx, session_id, operation_id, "pause_goal")? {
                return Ok(previous);
            }
            let goal = load_goal_for_update(tx, session_id, goal_id, expected_revision)?;
            if goal.status == to {
                return finish_noop(tx, session_id, operation_id, event_type);
            }
            if goal.status != from {
                return Err(WorkflowError::InvalidTransition(format!(
                    "expected {from} goal, found {}",
                    goal.status
                )));
            }
            let new_revision = bump_goal_revision(tx, session_id, goal_id, expected_revision)?;
            tx.execute(
                "UPDATE workflow_goals
                    SET status = ?1, status_reason = ?2, updated_at = ?3
                  WHERE id = ?4 AND session_id = ?5",
                params![to.as_str(), reason, now(), goal_id, session_id],
            )?;
            if to == GoalStatus::Paused {
                pause_running_attempt(tx, goal_id, reason)?;
            }
            finish_changed(
                tx,
                "pause_goal",
                session_id,
                goal_id,
                new_revision,
                operation_id,
                event_type,
                actor,
                None,
            )
        })
    }

    fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, WorkflowError>,
    ) -> Result<T, WorkflowError> {
        let db = self
            .db
            .lock()
            .map_err(|error| WorkflowError::Database(error.to_string()))?;
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
        let output = operation(&tx)?;
        tx.commit()?;
        Ok(output)
    }
}

/// Pause every active Goal bound to a Worker's private or group execution
/// lanes as part of the caller's Worker-archive transaction. Keeping this
/// transaction-bearing prevents an archived Worker from retaining an active
/// Goal if the process dies between independent commits.
pub fn pause_worker_goals_for_archive_in_transaction(
    tx: &Transaction<'_>,
    worker_id: &str,
    actor: &str,
) -> Result<usize, WorkflowError> {
    validate_operation("worker-archive", actor)?;
    let mut statement = tx.prepare(
        "SELECT goal.id, goal.session_id, goal.revision
         FROM workflow_goals goal
         JOIN hive_workers worker ON worker.id = ?1
         WHERE goal.status = 'active'
           AND (
               goal.session_id = worker.dm_session_id
               OR EXISTS (
                   SELECT 1 FROM hive_group_worker_lanes lane
                   WHERE lane.worker_id = worker.id
                     AND lane.session_id = goal.session_id
               )
           )
         ORDER BY goal.session_id ASC, goal.id ASC",
    )?;
    let goals = statement
        .query_map([worker_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for (goal_id, session_id, revision) in &goals {
        let timestamp = now();
        tx.execute(
            "INSERT OR IGNORE INTO hive_worker_provider_call_outcomes (
                 provider_call_id, state, outcome, remote_acceptance,
                 unknown_reason, finished_at
             )
             SELECT call.provider_call_id, 'unknown', 'worker_archived',
                    'possibly_sent',
                    'Worker was archived while its Workflow provider call was unresolved',
                    ?2
             FROM hive_worker_provider_calls call
             JOIN hive_runs run ON run.id = call.run_id
             WHERE run.workflow_goal_id = ?1 AND run.kind = 'worker_workflow'
               AND NOT EXISTS (
                   SELECT 1 FROM hive_worker_provider_call_outcomes terminal
                   WHERE terminal.provider_call_id = call.provider_call_id
               )",
            params![goal_id, timestamp],
        )?;
        tx.execute(
            "UPDATE hive_run_attempts
             SET finished_at = COALESCE(finished_at, ?2), outcome = 'cancelled',
                 stop_reason = 'worker_archived', error = NULL
             WHERE run_id IN (
                 SELECT id FROM hive_runs
                 WHERE workflow_goal_id = ?1 AND kind = 'worker_workflow'
                   AND status IN (
                       'queued', 'leased', 'running', 'sleeping', 'awaiting_input',
                       'retry_wait', 'recovery_required'
                   )
             ) AND finished_at IS NULL",
            params![goal_id, timestamp],
        )?;
        tx.execute(
            "UPDATE hive_runs
             SET status = 'cancelled', lease_owner = NULL, lease_token = NULL,
                 lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                 last_stop_reason = 'worker_archived', last_error = NULL,
                 finished_at = COALESCE(finished_at, ?2), updated_at = ?2
             WHERE workflow_goal_id = ?1 AND kind = 'worker_workflow'
               AND status IN (
                   'queued', 'leased', 'running', 'sleeping', 'awaiting_input',
                   'retry_wait', 'recovery_required'
               )",
            params![goal_id, timestamp],
        )?;
        let next_revision = bump_goal_revision(tx, session_id, goal_id, *revision)?;
        tx.execute(
            "UPDATE workflow_goals
             SET status = 'paused', status_reason = 'worker_archived', updated_at = ?1
             WHERE id = ?2 AND session_id = ?3 AND status = 'active'",
            params![now(), goal_id, session_id],
        )?;
        pause_running_attempt(tx, goal_id, "worker_archived")?;
        let operation_id = format!("worker-archive:{worker_id}:{goal_id}:{next_revision}");
        finish_changed(
            tx,
            "pause_worker_goals_for_archive",
            session_id,
            goal_id,
            next_revision,
            &operation_id,
            "goal_paused",
            actor,
            None,
        )?;
    }
    Ok(goals.len())
}

fn validate_operation(operation_id: &str, actor: &str) -> Result<(), WorkflowError> {
    if operation_id.trim().is_empty() {
        return Err(WorkflowError::Validation(
            "operation_id is required".to_string(),
        ));
    }
    if actor.trim().is_empty() {
        return Err(WorkflowError::Validation("actor is required".to_string()));
    }
    Ok(())
}

fn validate_goal_input(input: &CreateGoalInput) -> Result<(), WorkflowError> {
    validate_title_and_objective(input.title.trim(), input.objective.trim())?;
    if input.criteria.is_empty() {
        return Err(WorkflowError::Validation(
            "a durable goal requires at least one verification criterion".to_string(),
        ));
    }
    if input.token_budget == Some(0) {
        return Err(WorkflowError::Validation(
            "token budget must be greater than zero".to_string(),
        ));
    }
    for criterion in &input.criteria {
        if criterion.description.trim().is_empty() {
            return Err(WorkflowError::Validation(
                "criterion descriptions cannot be empty".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_title_and_objective(title: &str, objective: &str) -> Result<(), WorkflowError> {
    if title.is_empty() {
        return Err(WorkflowError::Validation(
            "goal title cannot be empty".to_string(),
        ));
    }
    let objective_chars = objective.chars().count();
    if objective_chars == 0 || objective_chars > MAX_GOAL_OBJECTIVE_CHARS {
        return Err(WorkflowError::Validation(format!(
            "goal objective must contain 1 to {MAX_GOAL_OBJECTIVE_CHARS} characters"
        )));
    }
    Ok(())
}

fn validate_plan_input(input: &PlanProposalInput) -> Result<(), WorkflowError> {
    if input.title.trim().is_empty() {
        return Err(WorkflowError::Validation(
            "plan title cannot be empty".to_string(),
        ));
    }
    if input.steps.is_empty() {
        return Err(WorkflowError::Validation(
            "plan must contain at least one step".to_string(),
        ));
    }
    let mut keys = HashSet::new();
    for step in &input.steps {
        let key = step.display_key.trim();
        if key.is_empty() || step.description.trim().is_empty() {
            return Err(WorkflowError::Validation(
                "step keys and descriptions cannot be empty".to_string(),
            ));
        }
        if !keys.insert(key.to_string()) {
            return Err(WorkflowError::Validation(format!(
                "duplicate step key {key}"
            )));
        }
    }
    for step in &input.steps {
        if let Some(parent) = step.parent_display_key.as_deref() {
            if !keys.contains(parent) || parent == step.display_key {
                return Err(WorkflowError::Validation(format!(
                    "invalid parent {parent} for step {}",
                    step.display_key
                )));
            }
        }
        for dependency in &step.dependencies {
            if !keys.contains(dependency) || dependency == &step.display_key {
                return Err(WorkflowError::Validation(format!(
                    "invalid dependency {dependency} for step {}",
                    step.display_key
                )));
            }
        }
    }
    ensure_acyclic(input)?;
    Ok(())
}

fn ensure_acyclic(input: &PlanProposalInput) -> Result<(), WorkflowError> {
    let graph: HashMap<&str, Vec<&str>> = input
        .steps
        .iter()
        .map(|step| {
            (
                step.display_key.as_str(),
                step.dependencies.iter().map(String::as_str).collect(),
            )
        })
        .collect();
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    fn visit<'a>(
        key: &'a str,
        graph: &HashMap<&'a str, Vec<&'a str>>,
        visiting: &mut HashSet<&'a str>,
        visited: &mut HashSet<&'a str>,
    ) -> bool {
        if visited.contains(key) {
            return true;
        }
        if !visiting.insert(key) {
            return false;
        }
        if let Some(dependencies) = graph.get(key) {
            for dependency in dependencies {
                if !visit(dependency, graph, visiting, visited) {
                    return false;
                }
            }
        }
        visiting.remove(key);
        visited.insert(key);
        true
    }
    for key in graph.keys().copied() {
        if !visit(key, &graph, &mut visiting, &mut visited) {
            return Err(WorkflowError::Validation(
                "plan dependencies must be acyclic".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_attempt_input(input: &StartAttemptInput) -> Result<(), WorkflowError> {
    if !matches!(input.permission_mode.as_str(), "supervised" | "autonomous") {
        return Err(WorkflowError::Validation(
            "permission_mode must be supervised or autonomous".to_string(),
        ));
    }
    if input.max_turns == 0
        || input.max_tool_calls == 0
        || input.max_wall_time_secs == 0
        || input.max_research_actions == 0
    {
        return Err(WorkflowError::Validation(
            "all attempt safety limits must be finite and greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn insert_criteria(
    tx: &Transaction<'_>,
    goal_id: &str,
    criteria: &[super::model::CriterionInput],
) -> Result<(), WorkflowError> {
    for (position, criterion) in criteria.iter().enumerate() {
        let description = criterion.description.trim();
        if description.is_empty() {
            return Err(WorkflowError::Validation(
                "criterion descriptions cannot be empty".to_string(),
            ));
        }
        tx.execute(
            "INSERT INTO workflow_goal_criteria (
                id, goal_id, position, description, required, status, evidence_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', '[]')",
            params![
                uuid::Uuid::new_v4().to_string(),
                goal_id,
                position as i64,
                description,
                criterion.required
            ],
        )?;
    }
    Ok(())
}

fn insert_steps(
    tx: &Transaction<'_>,
    plan_id: &str,
    input: &PlanProposalInput,
) -> Result<(), WorkflowError> {
    let ids: HashMap<&str, String> = input
        .steps
        .iter()
        .map(|step| (step.display_key.as_str(), uuid::Uuid::new_v4().to_string()))
        .collect();
    let timestamp = now();
    for (position, step) in input.steps.iter().enumerate() {
        let step_id = ids
            .get(step.display_key.as_str())
            .expect("validated step key");
        let parent_id = step
            .parent_display_key
            .as_deref()
            .and_then(|key| ids.get(key))
            .cloned();
        tx.execute(
            "INSERT INTO workflow_plan_steps (
                id, plan_revision_id, parent_step_id, display_key, position,
                description, context, acceptance_criteria_json, required,
                status, evidence_json, revision, created_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending', '[]', 1, ?10
             )",
            params![
                step_id,
                plan_id,
                parent_id,
                step.display_key.trim(),
                position as i64,
                step.description.trim(),
                clean_optional(step.context.as_deref()),
                serde_json::to_string(&normalize_strings(&step.acceptance_criteria))?,
                step.required,
                timestamp
            ],
        )?;
    }
    for step in &input.steps {
        let step_id = ids
            .get(step.display_key.as_str())
            .expect("validated step key");
        for dependency in &step.dependencies {
            let dependency_id = ids.get(dependency.as_str()).expect("validated dependency");
            tx.execute(
                "INSERT INTO workflow_step_dependencies (step_id, depends_on_step_id)
                 VALUES (?1, ?2)",
                params![step_id, dependency_id],
            )?;
        }
    }
    Ok(())
}

fn validate_step_claim(
    tx: &Transaction<'_>,
    plan_id: &str,
    step_id: &str,
) -> Result<(), WorkflowError> {
    let (status, parent_step_id): (WorkflowStepStatus, Option<String>) = tx
        .query_row(
            "SELECT status, parent_step_id FROM workflow_plan_steps
             WHERE id = ?1 AND plan_revision_id = ?2",
            params![step_id, plan_id],
            |row| Ok((parse_sql_enum(row.get::<_, String>(0)?, 0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or_else(|| WorkflowError::NotFound(format!("step {step_id}")))?;
    if status == WorkflowStepStatus::InProgress {
        return Err(WorkflowError::Conflict(
            "step is already claimed by another running attempt".to_string(),
        ));
    }
    if status != WorkflowStepStatus::Pending {
        return Err(WorkflowError::InvalidTransition(format!(
            "cannot claim {status} step"
        )));
    }
    let unresolved: i64 = tx.query_row(
        "SELECT COUNT(*)
           FROM workflow_step_dependencies dependency
           JOIN workflow_plan_steps blocker
             ON blocker.id = dependency.depends_on_step_id
          WHERE dependency.step_id = ?1
            AND blocker.status NOT IN ('completed', 'skipped')",
        [step_id],
        |row| row.get(0),
    )?;
    if unresolved > 0 {
        return Err(WorkflowError::InvalidTransition(format!(
            "step has {unresolved} unresolved dependencies"
        )));
    }
    if parent_step_id.is_none() {
        let running_root: i64 = tx.query_row(
            "SELECT COUNT(*) FROM workflow_plan_steps
             WHERE plan_revision_id = ?1 AND parent_step_id IS NULL
               AND status = 'in_progress'",
            [plan_id],
            |row| row.get(0),
        )?;
        if running_root > 0 {
            return Err(WorkflowError::Conflict(
                "another serial plan step is already in progress".to_string(),
            ));
        }
    }
    Ok(())
}

fn load_idempotent(
    connection: &Connection,
    session_id: &str,
    operation_id: &str,
    expected_action: &str,
) -> Result<Option<WorkflowMutation>, WorkflowError> {
    let result: Option<(String, String, String)> = connection
        .query_row(
            "SELECT session_id, action, result_json FROM workflow_idempotency WHERE operation_id = ?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    result
        .map(|(stored_session, action, json)| {
            if stored_session != session_id
                || canonical_operation_action(&action) != Some(expected_action)
            {
                return Err(WorkflowError::Conflict(
                    "workflow operation ID belongs to another session or action".into(),
                ));
            }
            serde_json::from_str(&json).map_err(WorkflowError::from)
        })
        .transpose()
}

/// Read historical event-labelled receipts, and write exact command identity
/// going forward. `goal_paused` and `goal_blocked` were each emitted by more
/// than one command, so those legacy receipts cannot safely authorize replay.
fn canonical_operation_action(action: &str) -> Option<&str> {
    if let Some(action) = action.strip_prefix("command:") {
        return Some(action);
    }
    Some(match action {
        "goal_created" => "create_goal",
        "goal_created_with_plan" => "create_goal_with_plan",
        "legacy_plan_imported" => "import_legacy_plan",
        "goal_edited" => "edit_goal",
        "plan_proposed" => "propose_plan",
        "plan_approved" => "approve_plan",
        "goal_activated" => "activate_goal",
        "goal_resumed" => "resume_goal",
        "goal_paused" | "goal_blocked" => return None,
        "goal_cancelled" => "cancel_goal",
        "attempt_started" => "start_attempt",
        "step_claimed" => "claim_step",
        "settle_attempt_usage"
        | "terminal_attempt_usage_settled"
        | "attempt_budget_exhausted"
        | "attempt_progressed"
        | "attempt_observed" => "record_attempt_progress",
        "step_completed" => "complete_step",
        "attempt_finished" => "finish_attempt",
        "criterion_updated" => "set_criterion",
        "goal_completed" => "complete_goal",
        action => action,
    })
}

fn finish_changed(
    tx: &Transaction<'_>,
    operation_action: &str,
    session_id: &str,
    goal_id: &str,
    revision: u64,
    operation_id: &str,
    event_type: &str,
    actor: &str,
    attempt_id: Option<&str>,
) -> Result<WorkflowMutation, WorkflowError> {
    let snapshot = load_snapshot(tx, session_id)?
        .ok_or_else(|| WorkflowError::NotFound(format!("workflow for session {session_id}")))?;
    if snapshot.goal.id != goal_id || snapshot.aggregate_revision != revision {
        return Err(WorkflowError::Database(
            "workflow snapshot revision diverged inside transaction".to_string(),
        ));
    }
    let mutation = WorkflowMutation {
        changed: true,
        operation_id: operation_id.to_string(),
        snapshot,
    };
    let timestamp = now();
    tx.execute(
        "INSERT INTO workflow_events (
            session_id, goal_id, aggregate_revision, operation_id, event_type,
            actor, attempt_id, payload_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            session_id,
            goal_id,
            to_i64(revision)?,
            operation_id,
            event_type,
            actor,
            attempt_id,
            serde_json::json!({
                "changed": true,
                "goal_status": mutation.snapshot.goal.status,
                "plan_revision_id": mutation
                    .snapshot
                    .plan_revision
                    .as_ref()
                    .map(|plan| plan.id.as_str()),
            })
            .to_string(),
            timestamp
        ],
    )?;
    store_idempotent(tx, session_id, operation_action, &mutation, &timestamp)?;
    Ok(mutation)
}

fn finish_noop(
    tx: &Transaction<'_>,
    session_id: &str,
    operation_id: &str,
    action: &str,
) -> Result<WorkflowMutation, WorkflowError> {
    let snapshot = load_snapshot(tx, session_id)?
        .ok_or_else(|| WorkflowError::NotFound(format!("workflow for session {session_id}")))?;
    let mutation = WorkflowMutation {
        changed: false,
        operation_id: operation_id.to_string(),
        snapshot,
    };
    store_idempotent(
        tx,
        session_id,
        canonical_operation_action(action).unwrap_or(action),
        &mutation,
        &now(),
    )?;
    Ok(mutation)
}

fn store_idempotent(
    tx: &Transaction<'_>,
    session_id: &str,
    action: &str,
    mutation: &WorkflowMutation,
    created_at: &str,
) -> Result<(), WorkflowError> {
    tx.execute(
        "INSERT INTO workflow_idempotency (
            operation_id, session_id, action, result_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            mutation.operation_id,
            session_id,
            format!("command:{action}"),
            serde_json::to_string(mutation)?,
            created_at
        ],
    )?;
    Ok(())
}

fn bump_goal_revision(
    tx: &Transaction<'_>,
    session_id: &str,
    goal_id: &str,
    expected_revision: u64,
) -> Result<u64, WorkflowError> {
    let changed = tx.execute(
        "UPDATE workflow_goals
            SET revision = revision + 1, updated_at = ?1
          WHERE id = ?2 AND session_id = ?3 AND revision = ?4",
        params![now(), goal_id, session_id, to_i64(expected_revision)?],
    )?;
    if changed != 1 {
        return Err(WorkflowError::Conflict(format!(
            "expected workflow revision {expected_revision}"
        )));
    }
    expected_revision
        .checked_add(1)
        .ok_or_else(|| WorkflowError::Validation("workflow revision overflow".to_string()))
}

fn load_goal_for_update(
    tx: &Transaction<'_>,
    session_id: &str,
    goal_id: &str,
    expected_revision: u64,
) -> Result<Goal, WorkflowError> {
    let goal = load_goal_by_id(tx, session_id, goal_id)?
        .ok_or_else(|| WorkflowError::NotFound(format!("goal {goal_id}")))?;
    if goal.revision != expected_revision {
        return Err(WorkflowError::Conflict(format!(
            "expected workflow revision {expected_revision}, current revision is {}",
            goal.revision
        )));
    }
    Ok(goal)
}

fn load_plan_status(
    connection: &Connection,
    goal_id: &str,
    plan_id: &str,
) -> Result<PlanRevisionStatus, WorkflowError> {
    let status: Option<String> = connection
        .query_row(
            "SELECT status FROM workflow_plan_revisions
             WHERE id = ?1 AND goal_id = ?2",
            params![plan_id, goal_id],
            |row| row.get(0),
        )
        .optional()?;
    status
        .ok_or_else(|| WorkflowError::NotFound(format!("plan revision {plan_id}")))?
        .parse()
        .map_err(WorkflowError::Database)
}

fn pause_running_attempt(
    tx: &Transaction<'_>,
    goal_id: &str,
    reason: &str,
) -> Result<(), WorkflowError> {
    let attempt_id: Option<String> = tx
        .query_row(
            "SELECT id FROM workflow_execution_attempts
             WHERE goal_id = ?1 AND status = 'running'",
            [goal_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(attempt_id) = attempt_id {
        let timestamp = now();
        tx.execute(
            "UPDATE workflow_execution_attempts
                SET status = 'paused', stop_reason = ?1,
                    ended_at = ?2, updated_at = ?2
              WHERE id = ?3",
            params![reason, timestamp, attempt_id],
        )?;
        release_attempt_step(tx, &attempt_id, WorkflowStepStatus::Pending)?;
    }
    Ok(())
}

fn release_attempt_step(
    tx: &Transaction<'_>,
    attempt_id: &str,
    next_status: WorkflowStepStatus,
) -> Result<(), WorkflowError> {
    tx.execute(
        "UPDATE workflow_plan_steps
            SET status = ?1, claimed_attempt_id = NULL, revision = revision + 1
          WHERE claimed_attempt_id = ?2 AND status = 'in_progress'",
        params![next_status.as_str(), attempt_id],
    )?;
    Ok(())
}

fn load_snapshot(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<WorkflowSnapshot>, WorkflowError> {
    let session_runtime: Option<(String, String)> = connection
        .query_row(
            "SELECT work_mode, permission_mode FROM sessions WHERE id = ?1",
            [session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((work_mode, permission_mode)) = session_runtime else {
        return Err(WorkflowError::NotFound(format!("session {session_id}")));
    };
    let goal = load_current_goal(connection, session_id)?;
    let Some(goal) = goal else {
        return Ok(None);
    };
    let criteria = load_criteria(connection, &goal.id)?;
    let plan_revision = load_current_plan(connection, &goal.id)?;
    let (steps, dependencies) = if let Some(plan) = plan_revision.as_ref() {
        (
            load_steps(connection, &plan.id)?,
            load_dependencies(connection, &plan.id)?,
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let latest_attempt = load_latest_attempt(connection, &goal.id)?;
    let collaboration_mode = if work_mode == "plan" {
        CollaborationMode::Plan
    } else {
        CollaborationMode::Default
    };
    let allowed_actions = allowed_actions(&goal, plan_revision.as_ref(), &latest_attempt);
    Ok(Some(WorkflowSnapshot {
        schema_version: WORKFLOW_SCHEMA_VERSION,
        aggregate_revision: goal.revision,
        collaboration_mode,
        permission_mode,
        goal,
        criteria,
        plan_revision,
        steps,
        dependencies,
        latest_attempt,
        allowed_actions,
    }))
}

fn load_current_goal(
    connection: &Connection,
    session_id: &str,
) -> Result<Option<Goal>, WorkflowError> {
    connection
        .query_row(
            "SELECT id, session_id, title, objective, constraints_json, status,
                    status_reason, needs_definition, revision, token_budget,
                    tokens_used, source, legacy_plan_id, created_at, updated_at,
                    activated_at, completed_at, cancelled_at
               FROM workflow_goals
              WHERE session_id = ?1
              ORDER BY CASE
                  WHEN status IN ('draft', 'active', 'paused', 'blocked') THEN 0
                  ELSE 1
              END, updated_at DESC
              LIMIT 1",
            [session_id],
            goal_from_row,
        )
        .optional()
        .map_err(WorkflowError::from)
}

fn load_goal_by_id(
    connection: &Connection,
    session_id: &str,
    goal_id: &str,
) -> Result<Option<Goal>, WorkflowError> {
    connection
        .query_row(
            "SELECT id, session_id, title, objective, constraints_json, status,
                    status_reason, needs_definition, revision, token_budget,
                    tokens_used, source, legacy_plan_id, created_at, updated_at,
                    activated_at, completed_at, cancelled_at
               FROM workflow_goals
              WHERE session_id = ?1 AND id = ?2",
            params![session_id, goal_id],
            goal_from_row,
        )
        .optional()
        .map_err(WorkflowError::from)
}

fn goal_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Goal> {
    Ok(Goal {
        id: row.get(0)?,
        session_id: row.get(1)?,
        title: row.get(2)?,
        objective: row.get(3)?,
        constraints: parse_sql_json(row.get::<_, String>(4)?, 4)?,
        status: parse_sql_enum(row.get::<_, String>(5)?, 5)?,
        status_reason: row.get(6)?,
        needs_definition: row.get(7)?,
        revision: from_i64(row.get(8)?, 8)?,
        token_budget: row
            .get::<_, Option<i64>>(9)?
            .map(|value| from_i64(value, 9))
            .transpose()?,
        tokens_used: from_i64(row.get(10)?, 10)?,
        source: row.get(11)?,
        legacy_plan_id: row.get(12)?,
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
        activated_at: row.get(15)?,
        completed_at: row.get(16)?,
        cancelled_at: row.get(17)?,
    })
}

fn load_criteria(
    connection: &Connection,
    goal_id: &str,
) -> Result<Vec<GoalCriterion>, WorkflowError> {
    let mut statement = connection.prepare(
        "SELECT id, goal_id, position, description, required, status,
                evidence_json, verifier, verified_at
           FROM workflow_goal_criteria
          WHERE goal_id = ?1 ORDER BY position",
    )?;
    let rows = statement.query_map([goal_id], |row| {
        Ok(GoalCriterion {
            id: row.get(0)?,
            goal_id: row.get(1)?,
            position: from_i64(row.get(2)?, 2)? as u32,
            description: row.get(3)?,
            required: row.get(4)?,
            status: parse_sql_enum(row.get::<_, String>(5)?, 5)?,
            evidence: parse_sql_json(row.get::<_, String>(6)?, 6)?,
            verifier: row.get(7)?,
            verified_at: row.get(8)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(WorkflowError::from)
}

fn load_current_plan(
    connection: &Connection,
    goal_id: &str,
) -> Result<Option<PlanRevision>, WorkflowError> {
    connection
        .query_row(
            "SELECT id, goal_id, revision_number, status, title, rationale,
                    source_message_id, predecessor_id, legacy_markdown,
                    created_at, approved_at, completed_at
               FROM workflow_plan_revisions
              WHERE goal_id = ?1
              ORDER BY CASE status
                  WHEN 'proposed' THEN 0
                  WHEN 'active' THEN 1
                  WHEN 'approved' THEN 2
                  WHEN 'completed' THEN 3
                  ELSE 4
              END, revision_number DESC
              LIMIT 1",
            [goal_id],
            |row| {
                Ok(PlanRevision {
                    id: row.get(0)?,
                    goal_id: row.get(1)?,
                    revision_number: from_i64(row.get(2)?, 2)?,
                    status: parse_sql_enum(row.get::<_, String>(3)?, 3)?,
                    title: row.get(4)?,
                    rationale: row.get(5)?,
                    source_message_id: row.get(6)?,
                    predecessor_id: row.get(7)?,
                    legacy_markdown: row.get(8)?,
                    created_at: row.get(9)?,
                    approved_at: row.get(10)?,
                    completed_at: row.get(11)?,
                })
            },
        )
        .optional()
        .map_err(WorkflowError::from)
}

fn load_steps(connection: &Connection, plan_id: &str) -> Result<Vec<WorkflowStep>, WorkflowError> {
    let mut statement = connection.prepare(
        "SELECT id, plan_revision_id, parent_step_id, display_key, position,
                description, context, acceptance_criteria_json, required, status,
                outcome, evidence_json, claimed_attempt_id, revision, created_at,
                started_at, completed_at
           FROM workflow_plan_steps
          WHERE plan_revision_id = ?1 ORDER BY position",
    )?;
    let rows = statement.query_map([plan_id], |row| {
        Ok(WorkflowStep {
            id: row.get(0)?,
            plan_revision_id: row.get(1)?,
            parent_step_id: row.get(2)?,
            display_key: row.get(3)?,
            position: from_i64(row.get(4)?, 4)? as u32,
            description: row.get(5)?,
            context: row.get(6)?,
            acceptance_criteria: parse_sql_json(row.get::<_, String>(7)?, 7)?,
            required: row.get(8)?,
            status: parse_sql_enum(row.get::<_, String>(9)?, 9)?,
            outcome: row.get(10)?,
            evidence: parse_sql_json(row.get::<_, String>(11)?, 11)?,
            claimed_attempt_id: row.get(12)?,
            revision: from_i64(row.get(13)?, 13)?,
            created_at: row.get(14)?,
            started_at: row.get(15)?,
            completed_at: row.get(16)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(WorkflowError::from)
}

fn load_dependencies(
    connection: &Connection,
    plan_id: &str,
) -> Result<Vec<StepDependency>, WorkflowError> {
    let mut statement = connection.prepare(
        "SELECT dependency.step_id, dependency.depends_on_step_id
           FROM workflow_step_dependencies dependency
           JOIN workflow_plan_steps step ON step.id = dependency.step_id
          WHERE step.plan_revision_id = ?1
          ORDER BY step.position",
    )?;
    let rows = statement.query_map([plan_id], |row| {
        Ok(StepDependency {
            step_id: row.get(0)?,
            depends_on_step_id: row.get(1)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(WorkflowError::from)
}

fn load_latest_attempt(
    connection: &Connection,
    goal_id: &str,
) -> Result<Option<ExecutionAttempt>, WorkflowError> {
    connection
        .query_row(
            "SELECT id, goal_id, plan_revision_id, step_id, status, stop_reason,
                    permission_mode, goal_revision_at_start, max_turns,
                    max_tool_calls, max_wall_time_secs, max_research_actions,
                    turn_count, tool_call_count, research_action_count,
                    progress_revision, blocker_fingerprint, blocker_streak,
                    started_at, updated_at, ended_at
               FROM workflow_execution_attempts
              WHERE goal_id = ?1
              ORDER BY started_at DESC LIMIT 1",
            [goal_id],
            attempt_from_row,
        )
        .optional()
        .map_err(WorkflowError::from)
}

fn load_attempt(
    connection: &Connection,
    goal_id: &str,
    attempt_id: &str,
) -> Result<ExecutionAttempt, WorkflowError> {
    connection
        .query_row(
            "SELECT id, goal_id, plan_revision_id, step_id, status, stop_reason,
                    permission_mode, goal_revision_at_start, max_turns,
                    max_tool_calls, max_wall_time_secs, max_research_actions,
                    turn_count, tool_call_count, research_action_count,
                    progress_revision, blocker_fingerprint, blocker_streak,
                    started_at, updated_at, ended_at
               FROM workflow_execution_attempts
              WHERE goal_id = ?1 AND id = ?2",
            params![goal_id, attempt_id],
            attempt_from_row,
        )
        .optional()?
        .ok_or_else(|| WorkflowError::NotFound(format!("attempt {attempt_id}")))
}

fn attempt_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExecutionAttempt> {
    Ok(ExecutionAttempt {
        id: row.get(0)?,
        goal_id: row.get(1)?,
        plan_revision_id: row.get(2)?,
        step_id: row.get(3)?,
        status: parse_sql_enum(row.get::<_, String>(4)?, 4)?,
        stop_reason: row.get(5)?,
        permission_mode: row.get(6)?,
        goal_revision_at_start: from_i64(row.get(7)?, 7)?,
        max_turns: from_i64(row.get(8)?, 8)? as u32,
        max_tool_calls: from_i64(row.get(9)?, 9)? as u32,
        max_wall_time_secs: from_i64(row.get(10)?, 10)?,
        max_research_actions: from_i64(row.get(11)?, 11)? as u32,
        turn_count: from_i64(row.get(12)?, 12)? as u32,
        tool_call_count: from_i64(row.get(13)?, 13)? as u32,
        research_action_count: from_i64(row.get(14)?, 14)? as u32,
        progress_revision: from_i64(row.get(15)?, 15)?,
        blocker_fingerprint: row.get(16)?,
        blocker_streak: from_i64(row.get(17)?, 17)? as u32,
        started_at: row.get(18)?,
        updated_at: row.get(19)?,
        ended_at: row.get(20)?,
    })
}

fn allowed_actions(
    goal: &Goal,
    plan: Option<&PlanRevision>,
    attempt: &Option<ExecutionAttempt>,
) -> Vec<String> {
    let mut actions = Vec::new();
    match goal.status {
        GoalStatus::Draft => {
            actions.extend(["edit_goal", "propose_plan", "cancel_goal"]);
            if plan.is_some_and(|plan| plan.status == PlanRevisionStatus::Proposed) {
                actions.push("approve_plan");
            }
            if !goal.needs_definition
                && plan.is_some_and(|plan| plan.status == PlanRevisionStatus::Active)
            {
                actions.push("activate_goal");
            }
        }
        GoalStatus::Active => {
            actions.extend(["pause_goal", "edit_goal", "propose_plan", "cancel_goal"]);
            if !attempt
                .as_ref()
                .is_some_and(|attempt| attempt.status == AttemptStatus::Running)
            {
                actions.push("start_attempt");
            }
        }
        GoalStatus::Paused | GoalStatus::Blocked => {
            actions.extend(["resume_goal", "edit_goal", "propose_plan", "cancel_goal"]);
            if plan.is_some_and(|plan| plan.status == PlanRevisionStatus::Proposed) {
                actions.push("approve_plan");
            }
        }
        GoalStatus::Completed | GoalStatus::Cancelled => {}
    }
    actions.into_iter().map(str::to_string).collect()
}

fn ensure_session_exists(connection: &Connection, session_id: &str) -> Result<(), WorkflowError> {
    let exists = connection
        .query_row("SELECT 1 FROM sessions WHERE id = ?1", [session_id], |_| {
            Ok(())
        })
        .optional()?
        .is_some();
    if !exists {
        return Err(WorkflowError::NotFound(format!("session {session_id}")));
    }
    Ok(())
}

fn parse_sql_enum<T>(value: String, column: usize) -> rusqlite::Result<T>
where
    T: FromStr<Err = String>,
{
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        )
    })
}

fn parse_sql_json<T>(value: String, column: usize) -> rusqlite::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

fn from_i64(value: i64, column: usize) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
    })
}

fn to_i64(value: u64) -> Result<i64, WorkflowError> {
    i64::try_from(value)
        .map_err(|_| WorkflowError::Validation("numeric value exceeds SQLite range".to_string()))
}

fn normalize_strings(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn clean_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn now() -> String {
    Utc::now().to_rfc3339()
}
