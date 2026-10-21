//! Durable workflow boundaries for ordinary Agent runs. Hive Worker hosts own
//! their frozen attempts and never enter this lifecycle.

use std::collections::HashMap;
use std::path::Path;

use crate::storage::{Database, DelegatedRunStore};
use crate::tools::registry::PermissionMode;
use crate::workflow::{
    AttemptProgressInput, AttemptStatus, ExecutionAttempt, GoalStatus, PlanRevisionStatus,
    StartAttemptInput, WorkflowManager, WorkflowMutation, WorkflowSnapshot, WorkflowStepStatus,
    DEFAULT_GOAL_ATTEMPT_MAX_RESEARCH_ACTIONS, DEFAULT_GOAL_ATTEMPT_MAX_TOOL_CALLS,
    DEFAULT_GOAL_ATTEMPT_MAX_TURNS, DEFAULT_GOAL_ATTEMPT_MAX_WALL_TIME_SECS,
};

pub(super) fn active_goal_for_run(db_path: &Path, session_id: &str) -> bool {
    WorkflowManager::new(db_path.to_path_buf())
        .and_then(|manager| manager.get_snapshot(session_id))
        .map(|snapshot| snapshot.is_some_and(|snapshot| snapshot.goal.status == GoalStatus::Active))
        .unwrap_or_else(|error| {
            tracing::warn!(
                session_id,
                %error,
                "Failed to resolve durable Goal before agent run"
            );
            false
        })
}

/// Claim the next dependency-ready step when an explicitly active Goal enters
/// the orchestrator. Lifecycle correctness must not depend on the model
/// remembering to call `task_start`; a duplicate model call remains a no-op.
pub(super) fn ensure_active_goal_attempt_for_run(
    db_path: &Path,
    session_id: &str,
    permission_mode: PermissionMode,
) -> Result<Option<WorkflowMutation>, crate::workflow::WorkflowError> {
    let manager = WorkflowManager::new(db_path.to_path_buf())?;
    let Some(mut snapshot) = manager.get_snapshot(session_id)? else {
        return Ok(None);
    };
    if snapshot.goal.status != GoalStatus::Active {
        return Ok(None);
    }
    if let Some(attempt) = snapshot
        .latest_attempt
        .as_ref()
        .filter(|attempt| attempt.status == AttemptStatus::Running)
    {
        let elapsed = chrono::DateTime::parse_from_rfc3339(&attempt.started_at)
            .ok()
            .map(|started| {
                chrono::Utc::now()
                    .signed_duration_since(started)
                    .num_seconds()
                    .max(0) as u64
            })
            .unwrap_or(0);
        let exhausted = attempt.turn_count >= attempt.max_turns
            || attempt.tool_call_count >= attempt.max_tool_calls
            || attempt.research_action_count >= attempt.max_research_actions
            || elapsed >= attempt.max_wall_time_secs;
        if !exhausted {
            return Ok(None);
        }
        snapshot = manager
            .record_attempt_progress(
                session_id,
                &snapshot.goal.id,
                &attempt.id,
                snapshot.aggregate_revision,
                AttemptProgressInput {
                    turn_count: attempt.turn_count,
                    tool_call_count: attempt.tool_call_count,
                    research_action_count: attempt.research_action_count,
                    material_progress: false,
                    blocker_fingerprint: None,
                },
                &format!("runtime-admission-budget-{}", uuid::Uuid::new_v4()),
                "runtime",
            )?
            .snapshot;
    }

    let status_by_id = snapshot
        .steps
        .iter()
        .map(|step| (step.id.as_str(), step.status))
        .collect::<HashMap<_, _>>();
    let plan_completed = snapshot
        .plan_revision
        .as_ref()
        .is_some_and(|plan| plan.status == PlanRevisionStatus::Completed);
    let step = snapshot.steps.iter().find(|step| {
        !plan_completed
            && step.status == WorkflowStepStatus::Pending
            && snapshot
                .dependencies
                .iter()
                .filter(|dependency| dependency.step_id == step.id)
                .all(|dependency| {
                    status_by_id
                        .get(dependency.depends_on_step_id.as_str())
                        .is_some_and(|status| {
                            matches!(
                                status,
                                WorkflowStepStatus::Completed | WorkflowStepStatus::Skipped
                            )
                        })
                })
    });
    // Verification of the Goal after its final step is also governed work.
    // Keep it inside a bounded attempt even though no step remains to claim.
    if step.is_none()
        && snapshot.steps.iter().any(|step| {
            step.required
                && !matches!(
                    step.status,
                    WorkflowStepStatus::Completed | WorkflowStepStatus::Skipped
                )
        })
    {
        return Ok(None);
    }

    manager
        .start_attempt(
            session_id,
            &snapshot.goal.id,
            snapshot.aggregate_revision,
            StartAttemptInput {
                step_id: step.map(|step| step.id.clone()),
                permission_mode: permission_mode.as_str().to_string(),
                max_turns: DEFAULT_GOAL_ATTEMPT_MAX_TURNS,
                max_tool_calls: DEFAULT_GOAL_ATTEMPT_MAX_TOOL_CALLS,
                max_wall_time_secs: DEFAULT_GOAL_ATTEMPT_MAX_WALL_TIME_SECS,
                max_research_actions: DEFAULT_GOAL_ATTEMPT_MAX_RESEARCH_ACTIONS,
            },
            &format!("runtime-auto-attempt-{}", uuid::Uuid::new_v4()),
            "runtime",
        )
        .map(Some)
}

pub(super) fn pause_active_goal_for_stop(
    db_path: &Path,
    session_id: &str,
    reason: &str,
) -> Result<Option<WorkflowMutation>, crate::workflow::WorkflowError> {
    let manager = WorkflowManager::new(db_path.to_path_buf())?;
    let Some(snapshot) = manager.get_snapshot(session_id)? else {
        return Ok(None);
    };
    if snapshot.goal.status != GoalStatus::Active {
        return if snapshot.goal.status == GoalStatus::Paused {
            Ok(None)
        } else {
            Err(crate::workflow::WorkflowError::InvalidTransition(format!(
                "Goal is {}; pause was not applied",
                snapshot.goal.status
            )))
        };
    }
    manager
        .pause_goal(
            session_id,
            &snapshot.goal.id,
            snapshot.aggregate_revision,
            Some(reason),
            &format!("runtime-pause-{}", uuid::Uuid::new_v4()),
            "runtime",
        )
        .map(Some)
}

pub(super) fn block_active_goal_for_stop(
    db_path: &Path,
    session_id: &str,
    reason: &str,
) -> Result<Option<WorkflowMutation>, crate::workflow::WorkflowError> {
    let manager = WorkflowManager::new(db_path.to_path_buf())?;
    let Some(snapshot) = manager.get_snapshot(session_id)? else {
        return Ok(None);
    };
    if snapshot.goal.status != GoalStatus::Active {
        return Ok(None);
    }
    manager
        .block_goal(
            session_id,
            &snapshot.goal.id,
            snapshot.aggregate_revision,
            reason,
            &format!("runtime-block-{}", uuid::Uuid::new_v4()),
            "runtime",
        )
        .map(Some)
}

pub(super) fn finish_active_attempt_for_stop(
    db_path: &Path,
    session_id: &str,
    admitted_goal_id: &str,
    reason: &str,
) -> Result<Option<WorkflowMutation>, crate::workflow::WorkflowError> {
    let manager = WorkflowManager::new(db_path.to_path_buf())?;
    let Some(snapshot) = manager.get_snapshot(session_id)? else {
        return Ok(None);
    };
    if snapshot.goal.id != admitted_goal_id {
        // A replacement Goal is owned by its own run. This run may settle
        // successors within its admitted Goal, but cannot pause another Goal.
        return Ok(None);
    }
    let Some(attempt) = snapshot
        .latest_attempt
        .as_ref()
        .filter(|attempt| attempt.status == AttemptStatus::Running)
    else {
        return Ok(None);
    };
    manager
        .finish_attempt(
            session_id,
            &snapshot.goal.id,
            &attempt.id,
            snapshot.aggregate_revision,
            AttemptStatus::Paused,
            reason,
            &format!("runtime-attempt-stop-{}", uuid::Uuid::new_v4()),
            "runtime",
        )
        .map(Some)
}

#[derive(Debug, Default)]
pub(super) struct AttemptProgressTracker {
    attempt_id: Option<String>,
    goal_id: Option<String>,
    turn_baseline: usize,
    tool_call_baseline: usize,
    research_action_baseline: usize,
    persisted_counts: (u32, u32, u32),
}

impl AttemptProgressTracker {
    pub(super) fn goal_id(&self) -> Option<&str> {
        self.goal_id.as_deref()
    }

    /// Bind before provider admission, so cancellation/restart and attempt
    /// rollover never rebase durable counters onto zero or earlier run work.
    pub(super) fn bind(
        &mut self,
        attempt: &ExecutionAttempt,
        turns: usize,
        tools: usize,
        research: usize,
    ) {
        if self.attempt_id.as_deref() == Some(&attempt.id) {
            return;
        }
        self.attempt_id = Some(attempt.id.clone());
        self.goal_id = Some(attempt.goal_id.clone());
        self.turn_baseline = turns;
        self.tool_call_baseline = tools;
        self.research_action_baseline = research;
        self.persisted_counts = (
            attempt.turn_count,
            attempt.tool_call_count,
            attempt.research_action_count,
        );
    }

    pub(super) fn local_counts(
        &mut self,
        attempt_id: &str,
        run_turn_count: usize,
        run_tool_call_count: usize,
        run_research_action_count: usize,
        current_turn_tool_calls: usize,
        current_turn_research_actions: usize,
    ) -> (u32, u32, u32) {
        if self.attempt_id.as_deref() != Some(attempt_id) {
            self.persisted_counts = (0, 0, 0);
            self.attempt_id = Some(attempt_id.to_string());
            self.turn_baseline = run_turn_count.saturating_sub(1);
            self.tool_call_baseline = run_tool_call_count.saturating_sub(current_turn_tool_calls);
            self.research_action_baseline =
                run_research_action_count.saturating_sub(current_turn_research_actions);
        }

        (
            self.persisted_counts.0.saturating_add(
                run_turn_count
                    .saturating_sub(self.turn_baseline)
                    .min(u32::MAX as usize) as u32,
            ),
            self.persisted_counts.1.saturating_add(
                run_tool_call_count
                    .saturating_sub(self.tool_call_baseline)
                    .min(u32::MAX as usize) as u32,
            ),
            self.persisted_counts.2.saturating_add(
                run_research_action_count
                    .saturating_sub(self.research_action_baseline)
                    .min(u32::MAX as usize) as u32,
            ),
        )
    }
}

pub(super) fn record_active_attempt_progress(
    db_path: &Path,
    session_id: &str,
    tracker: &mut AttemptProgressTracker,
    turn_count: usize,
    tool_call_count: usize,
    research_action_count: usize,
    current_turn_tool_calls: usize,
    current_turn_research_actions: usize,
    material_progress: bool,
    blocker_fingerprint: Option<String>,
) -> Result<Option<(GoalStatus, Option<String>)>, crate::workflow::WorkflowError> {
    let manager = WorkflowManager::new(db_path.to_path_buf())?;
    let Some(snapshot) = manager.get_snapshot(session_id)? else {
        return Ok(None);
    };
    let Some(attempt_id) = tracker.attempt_id.clone() else {
        return Ok(None);
    };
    if tracker.goal_id.as_deref() != Some(snapshot.goal.id.as_str()) {
        return Err(crate::workflow::WorkflowError::Conflict(
            "the admitted Goal changed before attempt usage could be settled".into(),
        ));
    }
    let (turn_count, tool_call_count, research_action_count) = tracker.local_counts(
        &attempt_id,
        turn_count,
        tool_call_count,
        research_action_count,
        current_turn_tool_calls,
        current_turn_research_actions,
    );
    let mutation = manager.record_attempt_progress(
        session_id,
        &snapshot.goal.id,
        &attempt_id,
        snapshot.aggregate_revision,
        AttemptProgressInput {
            turn_count,
            tool_call_count,
            research_action_count,
            material_progress,
            blocker_fingerprint,
        },
        &format!("runtime-progress-{}", uuid::Uuid::new_v4()),
        "runtime",
    )?;
    Ok(matches!(
        mutation.snapshot.goal.status,
        GoalStatus::Paused | GoalStatus::Blocked
    )
    .then_some((
        mutation.snapshot.goal.status,
        mutation.snapshot.goal.status_reason,
    )))
}

pub(super) fn record_active_goal_tokens(
    db_path: &Path,
    session_id: &str,
    goal_id: Option<&str>,
    token_delta: usize,
) -> Result<Option<(GoalStatus, Option<String>)>, crate::workflow::WorkflowError> {
    let Some(goal_id) = goal_id else {
        return Ok(None);
    };
    let manager = WorkflowManager::new(db_path.to_path_buf())?;
    Ok(manager
        .record_token_usage_for_goal(session_id, goal_id, token_delta as u64)?
        .map(|mutation| {
            (
                mutation.snapshot.goal.status,
                mutation.snapshot.goal.status_reason,
            )
        }))
}

/// A model final is a proposal to end the foreground turn, not a workflow
/// transition. Only persisted state and bounded recovery select its disposition.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum GoalFinalDisposition {
    Complete,
    AwaitDelegation,
    Continue,
    Pause,
}

pub(super) const GOAL_FINAL_CONTINUATION: &str = "[GOAL CONTINUATION]
The Goal is still active. Continue the next concrete authorized action in this same run. If all work is done, verify every required criterion with evidence and use workflow_update to complete the Goal. If user input is required, use AskUserQuestion. A final answer alone cannot settle an active Goal.";

pub(super) fn goal_final_disposition(
    snapshot: Option<&WorkflowSnapshot>,
    awaiting_delegation: bool,
    continuation_attempted: bool,
) -> GoalFinalDisposition {
    if !snapshot.is_some_and(|snapshot| snapshot.goal.status == GoalStatus::Active) {
        return GoalFinalDisposition::Complete;
    }
    if awaiting_delegation {
        GoalFinalDisposition::AwaitDelegation
    } else if continuation_attempted {
        GoalFinalDisposition::Pause
    } else {
        GoalFinalDisposition::Continue
    }
}

/// Only nonterminal children that own a durable parent wake justify waiting.
/// A completed report must be consumed by the parent, never treated as a wait.
pub(super) fn awaiting_background_delegation(
    db_path: &Path,
    session_id: &str,
) -> anyhow::Result<bool> {
    let store = DelegatedRunStore::new(Database::new(db_path)?);
    store.has_active_parent_wake(session_id)
}
