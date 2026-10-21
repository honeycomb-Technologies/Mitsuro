use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use std::time::Duration;

use mitsuro_core::agent::LoopInput;
use mitsuro_core::storage::{Database, HiveWorkerStore, SessionType};
use mitsuro_core::workflow::{
    AttemptProgressInput, AttemptStatus, CompleteStepInput, CreateGoalInput, EditGoalInput,
    PlanProposalInput, SetCriterionInput, StartAttemptInput, WorkflowError, WorkflowManager,
    WorkflowMutation, WorkflowSnapshot,
};

use super::{load_owned_session, open_session_manager};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::AppState;

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum WorkflowCommand {
    CreateGoal {
        operation_id: String,
        goal: CreateGoalInput,
    },
    ImportLegacyPlan {
        operation_id: String,
        goal: CreateGoalInput,
    },
    EditGoal {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
        goal: EditGoalInput,
    },
    ProposePlan {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
        plan: PlanProposalInput,
    },
    ApprovePlan {
        operation_id: String,
        goal_id: String,
        plan_revision_id: String,
        expected_revision: u64,
    },
    ActivateGoal {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
    },
    PauseGoal {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
        reason: String,
    },
    ResumeGoal {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
    },
    BlockGoal {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
        reason: String,
    },
    CancelGoal {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
        reason: Option<String>,
    },
    StartAttempt {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
        attempt: StartAttemptInput,
    },
    ClaimStep {
        operation_id: String,
        goal_id: String,
        attempt_id: String,
        step_id: String,
        expected_revision: u64,
    },
    RecordAttemptProgress {
        operation_id: String,
        goal_id: String,
        attempt_id: String,
        expected_revision: u64,
        progress: AttemptProgressInput,
    },
    CompleteStep {
        operation_id: String,
        goal_id: String,
        step_id: String,
        expected_revision: u64,
        completion: CompleteStepInput,
    },
    FinishAttempt {
        operation_id: String,
        goal_id: String,
        attempt_id: String,
        expected_revision: u64,
        status: AttemptStatus,
        reason: String,
    },
    SetCriterion {
        operation_id: String,
        goal_id: String,
        criterion_id: String,
        expected_revision: u64,
        criterion: SetCriterionInput,
    },
    CompleteGoal {
        operation_id: String,
        goal_id: String,
        expected_revision: u64,
    },
}

pub(super) async fn get_workflow(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(session_id): Path<String>,
) -> Result<Json<Option<WorkflowSnapshot>>, AppError> {
    let session_manager = open_session_manager(&state)?;
    load_owned_session(&session_manager, &session_id, user.as_ref())?;
    let manager =
        WorkflowManager::new(state.db_path.as_ref().clone()).map_err(map_workflow_error)?;
    Ok(Json(
        manager
            .get_snapshot(&session_id)
            .map_err(map_workflow_error)?,
    ))
}

pub(super) async fn execute_workflow_command(
    State(state): State<AppState>,
    user: Option<CurrentUser>,
    Path(session_id): Path<String>,
    Json(command): Json<WorkflowCommand>,
) -> Result<Json<WorkflowMutation>, AppError> {
    let session_manager = open_session_manager(&state)?;
    let session = load_owned_session(&session_manager, &session_id, user.as_ref())?;
    ensure_not_worker_dm_workflow_command(&state, &session)?;
    let manager =
        WorkflowManager::new(state.db_path.as_ref().clone()).map_err(map_workflow_error)?;
    let starts_execution = matches!(
        &command,
        WorkflowCommand::ActivateGoal { .. } | WorkflowCommand::ResumeGoal { .. }
    );
    let stops_execution = matches!(
        &command,
        WorkflowCommand::PauseGoal { .. }
            | WorkflowCommand::BlockGoal { .. }
            | WorkflowCommand::CancelGoal { .. }
            | WorkflowCommand::CompleteGoal { .. }
    );
    let mut execution_guard = if starts_execution {
        state.try_lock_session(&session_id).await
    } else {
        None
    };

    let mutation = match command {
        WorkflowCommand::CreateGoal { operation_id, goal } => {
            manager.create_goal(&session_id, goal, &operation_id, "user")
        }
        WorkflowCommand::ImportLegacyPlan { operation_id, goal } => {
            manager.import_legacy_plan(&session_id, goal, &operation_id, "user")
        }
        WorkflowCommand::EditGoal {
            operation_id,
            goal_id,
            expected_revision,
            goal,
        } => manager.edit_goal(
            &session_id,
            &goal_id,
            expected_revision,
            goal,
            &operation_id,
            "user",
        ),
        WorkflowCommand::ProposePlan {
            operation_id,
            goal_id,
            expected_revision,
            plan,
        } => manager.propose_plan(
            &session_id,
            &goal_id,
            expected_revision,
            plan,
            &operation_id,
            "user",
        ),
        WorkflowCommand::ApprovePlan {
            operation_id,
            goal_id,
            plan_revision_id,
            expected_revision,
        } => manager.approve_plan(
            &session_id,
            &goal_id,
            &plan_revision_id,
            expected_revision,
            &operation_id,
            "user",
        ),
        WorkflowCommand::ActivateGoal {
            operation_id,
            goal_id,
            expected_revision,
        } => manager.activate_goal(
            &session_id,
            &goal_id,
            expected_revision,
            &operation_id,
            "user",
        ),
        WorkflowCommand::PauseGoal {
            operation_id,
            goal_id,
            expected_revision,
            reason,
        } => manager.pause_goal(
            &session_id,
            &goal_id,
            expected_revision,
            Some(&reason),
            &operation_id,
            "user",
        ),
        WorkflowCommand::ResumeGoal {
            operation_id,
            goal_id,
            expected_revision,
        } => manager.resume_goal(
            &session_id,
            &goal_id,
            expected_revision,
            &operation_id,
            "user",
        ),
        WorkflowCommand::BlockGoal {
            operation_id,
            goal_id,
            expected_revision,
            reason,
        } => manager.block_goal(
            &session_id,
            &goal_id,
            expected_revision,
            &reason,
            &operation_id,
            "user",
        ),
        WorkflowCommand::CancelGoal {
            operation_id,
            goal_id,
            expected_revision,
            reason,
        } => manager.cancel_goal(
            &session_id,
            &goal_id,
            expected_revision,
            reason.as_deref(),
            &operation_id,
            "user",
        ),
        WorkflowCommand::StartAttempt {
            operation_id,
            goal_id,
            expected_revision,
            attempt,
        } => manager.start_attempt(
            &session_id,
            &goal_id,
            expected_revision,
            attempt,
            &operation_id,
            "user",
        ),
        WorkflowCommand::ClaimStep {
            operation_id,
            goal_id,
            attempt_id,
            step_id,
            expected_revision,
        } => manager.claim_step(
            &session_id,
            &goal_id,
            &attempt_id,
            &step_id,
            expected_revision,
            &operation_id,
            "user",
        ),
        WorkflowCommand::RecordAttemptProgress {
            operation_id,
            goal_id,
            attempt_id,
            expected_revision,
            progress,
        } => manager.record_attempt_progress(
            &session_id,
            &goal_id,
            &attempt_id,
            expected_revision,
            progress,
            &operation_id,
            "user",
        ),
        WorkflowCommand::CompleteStep {
            operation_id,
            goal_id,
            step_id,
            expected_revision,
            completion,
        } => manager.complete_step(
            &session_id,
            &goal_id,
            &step_id,
            expected_revision,
            completion,
            &operation_id,
            "user",
        ),
        WorkflowCommand::FinishAttempt {
            operation_id,
            goal_id,
            attempt_id,
            expected_revision,
            status,
            reason,
        } => manager.finish_attempt(
            &session_id,
            &goal_id,
            &attempt_id,
            expected_revision,
            status,
            &reason,
            &operation_id,
            "user",
        ),
        WorkflowCommand::SetCriterion {
            operation_id,
            goal_id,
            criterion_id,
            expected_revision,
            criterion,
        } => manager.set_criterion(
            &session_id,
            &goal_id,
            &criterion_id,
            expected_revision,
            criterion,
            &operation_id,
            "user",
        ),
        WorkflowCommand::CompleteGoal {
            operation_id,
            goal_id,
            expected_revision,
        } => manager.complete_goal(
            &session_id,
            &goal_id,
            expected_revision,
            &operation_id,
            "user",
        ),
    }
    .map_err(map_workflow_error)?;

    let live_sender = state.session_inputs.read().await.get(&session_id).cloned();
    let current = manager
        .get_snapshot(&session_id)
        .map_err(map_workflow_error)?;
    if !current.as_ref().is_some_and(|snapshot| {
        snapshot.goal.id == mutation.snapshot.goal.id
            && snapshot.aggregate_revision == mutation.snapshot.aggregate_revision
            && snapshot.goal.status == mutation.snapshot.goal.status
    }) {
        // Idempotency returns the original receipt. It must not repeat stale
        // control effects after a later command or a replacement Goal.
        return Ok(Json(mutation));
    }

    // Collaboration mode remains a conversational posture. Only the explicit
    // user activation command switches the session into executable Build mode.
    if matches!(
        mutation.snapshot.goal.status,
        mitsuro_core::workflow::GoalStatus::Active
    ) {
        session_manager
            .update_session_work_mode(&session_id, mitsuro_core::storage::WorkMode::Build)?;
    }

    // Existing runs observe every committed workflow change. Explicit stop
    // controls also interrupt an in-flight provider request; they never wake
    // an idle session merely to stop it again.
    let workflow_input = LoopInput::WorkflowUpdated {
        goal_id: mutation.snapshot.goal.id.clone(),
        aggregate_revision: mutation.snapshot.aggregate_revision,
        operation_id: mutation.operation_id.clone(),
    };
    let delivered_to_live_run = live_sender
        .as_ref()
        .is_some_and(|sender| sender.send(workflow_input).is_ok());
    if stops_execution {
        if let Some(sender) = &live_sender {
            let _ = sender.send(LoopInput::Cancel);
        }
    }

    if starts_execution
        && matches!(
            mutation.snapshot.goal.status,
            mitsuro_core::workflow::GoalStatus::Active
        )
    {
        if delivered_to_live_run {
            drop(execution_guard.take());
        } else {
            if execution_guard.is_none() {
                execution_guard = tokio::time::timeout(
                    Duration::from_millis(250),
                    state.lock_session(&session_id),
                )
                .await
                .ok();
            }

            let guard = execution_guard.take().ok_or_else(|| {
                AppError::Conflict(format!(
                    "Session {session_id} is busy and its workflow wake channel is not ready; retry the command"
                ))
            })?;
            if let Err(error) = super::super::chat::start_workflow_session_with_guard(
                &state,
                user.as_ref(),
                &session_id,
                guard,
            )
            .await
            {
                let _ = manager.pause_goal(
                    &session_id,
                    &mutation.snapshot.goal.id,
                    mutation.snapshot.aggregate_revision,
                    Some("activation_start_failed"),
                    &format!("runtime-activation-failed-{}", uuid::Uuid::new_v4()),
                    "runtime",
                );
                return Err(error);
            }
        }
    }

    Ok(Json(mutation))
}

/// Exact Worker DMs expose Goal authoring and lifecycle only through the
/// Worker-aware HTTP surface. Keep this fence ahead of every WorkflowManager
/// mutation, session lock, work-mode update, and live LoopInput delivery so a
/// generic Agent route cannot bypass Worker state or revision authority.
fn ensure_not_worker_dm_workflow_command(
    state: &AppState,
    session: &mitsuro_core::storage::SessionInfo,
) -> Result<(), AppError> {
    if session.session_type != SessionType::Hive {
        return Ok(());
    }

    let worker_store = HiveWorkerStore::new(Database::new(&state.db_path)?);
    let Some(worker) = worker_store.get_by_dm_session(&session.id)? else {
        // Primary Hive remains on its existing compatibility contract.
        return Ok(());
    };
    if worker.user_id != session.user_id
        || worker.dm_session_id.as_deref() != Some(session.id.as_str())
    {
        return Err(AppError::NotFound(format!(
            "Hive session {} not found",
            session.id
        )));
    }
    Err(AppError::Conflict(format!(
        "Worker Goal commands for {} must use /hive/workers/{}/workflow",
        session.id, worker.id
    )))
}

fn map_workflow_error(error: WorkflowError) -> AppError {
    match error {
        WorkflowError::NotFound(message) => AppError::NotFound(message),
        WorkflowError::Conflict(message)
        | WorkflowError::InvalidTransition(message)
        | WorkflowError::WorkspaceRequired(message) => AppError::Conflict(message),
        WorkflowError::Validation(message) => AppError::BadRequest(message),
        WorkflowError::Database(message) => AppError::Internal(message),
        WorkflowError::Sql(error) => AppError::Internal(error.to_string()),
        WorkflowError::Json(error) => AppError::BadRequest(error.to_string()),
    }
}
