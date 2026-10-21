use std::path::Path;

use tracing::warn;

use crate::plan::PlanManager;
use crate::storage::WorkMode;
use crate::workflow::{GoalStatus, WorkflowManager, WorkflowSnapshot, WorkflowStepStatus};

use super::{truncate_utf8, truncate_utf8_bytes};

const MAX_VISIBLE_READY_TASKS: usize = 12;
const MAX_VISIBLE_BLOCKED_TASKS: usize = 6;
const MAX_PLAN_TASK_DESCRIPTION_CHARS: usize = 300;

/// Build plan context from the active plan for this session.
pub fn build_plan_context(db_path: &Path, session_id: &str, work_mode: WorkMode) -> String {
    match WorkflowManager::new(db_path.to_path_buf())
        .and_then(|manager| manager.get_snapshot(session_id))
    {
        Ok(Some(snapshot)) => return build_workflow_context(&snapshot, work_mode),
        Ok(None) => {}
        Err(error) => {
            warn!(
                session_id = %session_id,
                error = %error,
                "Failed to load canonical workflow context"
            );
        }
    }

    let plan_manager = match PlanManager::new(db_path.to_path_buf()) {
        Ok(pm) => pm,
        Err(error) => {
            warn!(session_id = %session_id, db_path = %db_path.display(), error = %error, "Failed to open plan manager for context");
            return if work_mode == WorkMode::Plan {
                plan_mode_default_context()
            } else {
                String::new()
            };
        }
    };

    let plan = match plan_manager.get_active_plan(session_id) {
        Ok(Some(p)) => p,
        Err(error) => {
            warn!(session_id = %session_id, error = %error, "Failed to load active plan for context");
            return if work_mode == WorkMode::Plan {
                plan_mode_default_context()
            } else {
                String::new()
            };
        }
        _ => {
            return if work_mode == WorkMode::Plan {
                plan_mode_default_context()
            } else {
                String::new()
            };
        }
    };

    let (completed, total) = plan.progress();

    if work_mode == WorkMode::Plan {
        let markdown = plan.to_context();
        let title = plan.title.replace(['`', '"'], "'");
        format!(
            "[PLAN MODE ACTIVE - Plan: \"{}\"]\n\n\
             Progress: {}/{} tasks completed\n\n\
             {}\n\n\
             Plan mode is read-only: inspect and refine the plan, but do not modify project files.",
            title, completed, total, markdown
        )
    } else {
        let ready_tasks = plan.get_ready_tasks();
        let blocked_tasks = plan.get_blocked_tasks();

        let ready_list = if ready_tasks.is_empty() {
            "  (none)".to_string()
        } else {
            ready_tasks
                .iter()
                .take(MAX_VISIBLE_READY_TASKS)
                .map(|t| {
                    format!(
                        "  - Task {}: {}",
                        t.id,
                        truncate_utf8(&t.description, MAX_PLAN_TASK_DESCRIPTION_CHARS)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };

        let blocked_list = if blocked_tasks.is_empty() {
            "  (none)".to_string()
        } else {
            blocked_tasks
                .iter()
                .take(MAX_VISIBLE_BLOCKED_TASKS)
                .map(|t| {
                    format!(
                        "  - Task {}: {} (waiting on: {})",
                        t.id,
                        truncate_utf8(&t.description, MAX_PLAN_TASK_DESCRIPTION_CHARS),
                        t.blocked_by.join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };

        let ready_omitted = ready_tasks.len().saturating_sub(MAX_VISIBLE_READY_TASKS);
        let blocked_omitted = blocked_tasks
            .len()
            .saturating_sub(MAX_VISIBLE_BLOCKED_TASKS);
        let omitted = match (ready_omitted, blocked_omitted) {
            (0, 0) => String::new(),
            (ready, blocked) => format!(
                "\n\nAdditional tasks omitted from prompt: {ready} ready/active, {blocked} blocked.",
            ),
        };

        let title = plan.title.replace(['`', '"'], "'");
        format!(
            "[ACTIVE PLAN - \"{}\"]\n\n\
             Progress: {}/{} tasks completed\n\n\
             Ready or active:\n{}\n\n\
             Blocked:\n{}{}\n\n\
             Own one canonical plan task at a time. Within that task, delegate substantial independent subtasks as one structured Agent graph when it improves throughput; keep shared-file work dependency-ordered. Call `task_start` before work and `task_complete` only after integration and concrete verification.",
            title, completed, total, ready_list, blocked_list, omitted
        )
    }
}

/// One independently bounded section keeps canonical execution obligations
/// intact before the aggregate allocator considers project/retrieval material.
// Goal creation permits 4,000 Unicode characters (up to 16 KiB). Reserve
// enough for that exact objective plus current-step and verification context.
const MAX_WORKFLOW_CONTEXT_BYTES: usize = 28 * 1024;
const WORKFLOW_READ_GUIDANCE: &str = "This is a bounded view of the canonical Goal. Excerpts and omitted items do not narrow its scope. Use `workflow_read` (discover through `tool_search` if needed) to read full objective, constraints, criteria, or steps before acting on omitted requirements or claiming completion.";

pub(super) fn build_workflow_context(snapshot: &WorkflowSnapshot, work_mode: WorkMode) -> String {
    let goal = &snapshot.goal;
    let current_step = snapshot
        .steps
        .iter()
        .find(|step| step.status == WorkflowStepStatus::InProgress)
        .or_else(|| {
            snapshot.steps.iter().find(|step| {
                step.status == WorkflowStepStatus::Pending
                    && snapshot
                        .dependencies
                        .iter()
                        .filter(|dependency| dependency.step_id == step.id)
                        .all(|dependency| {
                            snapshot.steps.iter().any(|candidate| {
                                candidate.id == dependency.depends_on_step_id
                                    && matches!(
                                        candidate.status,
                                        WorkflowStepStatus::Completed | WorkflowStepStatus::Skipped
                                    )
                            })
                        })
            })
        });
    let execution_instruction = match goal.status {
        GoalStatus::Active => current_step.map_or_else(
            || {
                if snapshot.steps.iter().all(|step| step.status.is_terminal()) {
                    "All plan steps are terminal. Run the required verification, call \
                     `workflow_update` with `verify_criterion` and concrete evidence for each \
                     criterion, then call it with `complete_goal`. Do not claim completion in \
                     prose."
                        .to_string()
                } else {
                    "No executable step is currently available. Do not invent progress; report the \
                     dependency or verification state."
                        .to_string()
                }
            },
            |step| {
                if step.status == WorkflowStepStatus::InProgress {
                    format!(
                        "Continue only Step {}: {}. Complete it with `task_complete` and concrete \
                         evidence. Do not start another canonical step. You may delegate bounded, \
                         independent work inside this step as one structured Agent graph, then \
                         integrate and verify it before completion.",
                        bounded_field(&step.display_key, 128),
                        bounded_field(&step.description, 2_048)
                    )
                } else {
                    format!(
                        "Next Step {}: {}. Call `task_start` once before implementation, then \
                         complete it with concrete evidence. If the step contains substantial \
                         separable work, use one structured Agent graph with explicit write intent \
                         and dependency-order any shared files.",
                        bounded_field(&step.display_key, 128),
                        bounded_field(&step.description, 2_048)
                    )
                }
            },
        ),
        GoalStatus::Draft => {
            "This Goal is a draft. Do not implement it until the user approves the exact plan \
             revision and activates the Goal."
                .to_string()
        }
        GoalStatus::Paused => {
            "This Goal is paused. Do not continue implementation until the user resumes it."
                .to_string()
        }
        GoalStatus::Blocked => {
            "This Goal is blocked. Report the persisted blocker and wait for an explicit user \
             edit or resume."
                .to_string()
        }
        GoalStatus::Completed | GoalStatus::Cancelled => {
            "This Goal is terminal. Do not continue its plan.".to_string()
        }
    };

    let title = bounded_field(&goal.title.replace(['`', '\n', '\r'], " "), 256);
    let objective = bounded_field(&goal.objective, 16 * 1024);
    let heading = if work_mode == WorkMode::Plan {
        "[PLAN MODE ACTIVE - CANONICAL WORKFLOW]".to_string()
    } else {
        format!("[DURABLE GOAL - {title}]")
    };
    let obligation = if work_mode == WorkMode::Plan {
        "Plan mode is read-only for project state. Investigate, ask questions, and call \
         `workflow_propose` with typed Goal/plan data to create or revise a draft. Assistant \
         prose is never authoritative. Only the user can approve a plan revision and activate \
         or resume the Goal; permission mode does not change that boundary."
    } else {
        &execution_instruction
    };
    let mut context = format!(
        "{heading}\n\nGoal ID: {goal_id}\nRevision: {revision}\nStatus: {status}\n\n\
         {obligation}\n\n\
         Follow-up messages steer this Goal; they do not replace its outcome. Finishing an \
         assistant response or all plan steps does not complete the Goal. Goal completion requires \
         verification criteria to pass.\n\n\
         {WORKFLOW_READ_GUIDANCE}\n\nOutcome: {objective}",
        goal_id = bounded_field(&goal.id, 128),
        revision = snapshot.aggregate_revision,
        status = goal.status,
    );
    if let Some(reason) = goal.status_reason.as_deref() {
        context.push_str(&format!("\nStatus reason: {}", bounded_field(reason, 512)));
    }
    if let Some(step) = current_step {
        context.push_str(&format!(
            "\nCurrent step ID: {}\nCurrent step acceptance:\n{}",
            bounded_field(&step.id, 128),
            bounded_list(
                step.acceptance_criteria
                    .iter()
                    .map(|value| format!("  - {}", bounded_field(value, 384))),
                1_024
            )
        ));
    }
    let constraints = bounded_list(
        goal.constraints
            .iter()
            .map(|value| format!("  - {}", bounded_field(value, 512))),
        1_536,
    );
    // Surface unmet verification before passed history so a large Goal can
    // advance without repeatedly receiving the same completed criteria.
    let mut criteria = snapshot.criteria.iter().collect::<Vec<_>>();
    criteria.sort_by_key(|criterion| criterion.status == crate::workflow::CriterionStatus::Passed);
    let criteria = bounded_list(
        criteria.into_iter().map(|criterion| {
            format!(
                "  - [{}] {} (criterion_id: {}, required: {})",
                criterion.status,
                bounded_field(&criterion.description, 640),
                bounded_field(&criterion.id, 128),
                criterion.required
            )
        }),
        2_048,
    );
    let plan = snapshot
        .plan_revision
        .as_ref()
        .map(|plan| {
            format!(
                "{} (revision {}, {})",
                bounded_field(&plan.title, 256),
                plan.revision_number,
                plan.status
            )
        })
        .unwrap_or_else(|| "(no plan revision proposed)".to_string());
    let mut steps = snapshot.steps.iter().collect::<Vec<_>>();
    steps.sort_by_key(|step| step.status.is_terminal());
    let steps = bounded_list(
        steps.into_iter().map(|step| {
            format!(
                "  - [{}] {}: {} (step_id: {})",
                step.status,
                bounded_field(&step.display_key, 64),
                bounded_field(&step.description, 512),
                bounded_field(&step.id, 128)
            )
        }),
        2_048,
    );
    context.push_str(&format!("\n\nConstraints:\n{constraints}\n\nVerification:\n{criteria}\n\nCurrent plan: {plan}\nProgress:\n{steps}"));
    // The local field limits above normally keep the complete section below
    // this ceiling. Preserve an explicit marker if a future field grows it.
    bounded_field(&context, MAX_WORKFLOW_CONTEXT_BYTES)
}

fn bounded_field(text: &str, bytes: usize) -> String {
    const MARKER: &str = " [EXCERPT; read full value with workflow_read]";
    if text.len() <= bytes {
        return text.to_string();
    }
    format!(
        "{}{}",
        truncate_utf8_bytes(text, bytes.saturating_sub(MARKER.len())),
        MARKER
    )
}

fn bounded_list(items: impl Iterator<Item = String>, max_bytes: usize) -> String {
    const MARKER: &str = "\n  [ADDITIONAL ITEMS OMITTED; read with workflow_read]";
    let mut output = String::new();
    for item in items {
        if output.len() + item.len() + 1 + MARKER.len() > max_bytes {
            output.push_str(MARKER);
            break;
        }
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&item);
    }
    if output.is_empty() {
        "  (none)".to_string()
    } else {
        output
    }
}

fn plan_mode_default_context() -> String {
    "[PLAN MODE ACTIVE]\n\n\
     You are in PLAN MODE. The user wants a plan before implementing.\n\
     - You can READ files, search code, and explore the codebase\n\
     - You CANNOT write, edit, or create files\n\
     - Use the AskUserQuestion tool for clarifications\n\
     - Use `workflow_propose` to submit a typed draft Goal and plan\n\n\
     Define the desired outcome, constraints, measurable verification criteria, and ordered plan \
     steps. Assistant Markdown is presentation only and never creates execution state. The user \
     must explicitly approve the exact plan revision and activate the Goal before implementation."
        .to_string()
}
