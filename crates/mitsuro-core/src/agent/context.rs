//! Context injection for the agentic loop.
//!
//! Builds plan, skills, and project context strings that get injected as
//! system messages at the head of the conversation before each AI call.
//! This ensures the AI is always aware of the active plan, available skills,
//! and project-specific instructions.

mod episodes;
mod hive;
mod memory;
mod plan;
mod project;
mod reports;
mod runtime_state;
mod skills;
mod workspace;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::path::Path;

use thiserror::Error;
use tokio::sync::RwLock;
use tracing::warn;

use crate::ai::model_profile::ContextKind;
use crate::ai::types::{Content, ModelMessage, Role};
use crate::skills::SkillsManager;
use crate::storage::{
    Database, DelegationMode, HiveGroupRunContext, HiveProfileSnapshot, ProjectSettings, WorkMode,
};

use super::run_spec::WorkerGoalExecutionContext;

pub use plan::build_plan_context;
pub use project::{build_project_context, build_subagent_project_instructions};
pub use skills::build_skills_context;
pub use workspace::build_subagent_project_context;

/// Aggregate ceiling for request-time system context assembled by this module.
/// Individual sources also have smaller limits, but the aggregate guard keeps
/// several simultaneously-large sources from bypassing those local budgets.
pub(super) const MAX_DYNAMIC_CONTEXT_BYTES: usize = 64 * 1024;

const WORKER_CONVERSATION_CAPABILITY_CONTEXT: &str = "[WORKER CONVERSATION CAPABILITY]\n\nThis is a conversation-only Hive Worker response. You have no workspace, project, tools, web access, processes, extensions, skills, delegation, or global Hive identity in this run. Reply as the exact Worker described below using only the supplied conversation and bounded private continuity. Never claim an action or external observation you could not perform.\n\n[/WORKER CONVERSATION CAPABILITY]";
const WORKER_GROUP_RESPONSE_CAPABILITY_CONTEXT: &str = "[WORKER GROUP RESPONSE CAPABILITY]\n\nThis neutral run has no post_to_group tool. Your one final assistant response is the group contribution and will be projected into the room atomically by the runtime. Do not request or emit a tool call.\n\n[/WORKER GROUP RESPONSE CAPABILITY]";
const WORKER_GOAL_WORKFLOW_CONTEXT_BYTES: usize = 16 * 1024;
const WORKER_GOAL_FIELD_CHARS: usize = 640;
const WORKER_GOAL_LIST_ITEMS: usize = 16;

#[derive(Debug, Error)]
pub(crate) enum WorkerConversationContextError {
    #[error("session is not durably bound to a Hive Worker")]
    MissingBinding,
    #[error("Hive Worker conversation binding was denied")]
    DeniedBinding,
    #[error("Hive Worker conversation binding resolved to '{actual}', expected '{expected}'")]
    WorkerMismatch { expected: String, actual: String },
    #[error("Hive Worker group-room context could not be loaded")]
    GroupRoomUnavailable,
    #[error("Hive Worker revision resolved to '{actual}', expected '{expected}'")]
    WorkerRevisionMismatch { expected: u64, actual: u64 },
    #[error("Hive Worker Goal Workflow binding is unavailable or inconsistent")]
    WorkflowBindingMismatch,
}

/// Build a conversation clone with context system messages prepended.
///
/// Injects plan, skills, and project context in the same order as the TUI:
/// project → plan → skills → original conversation.
pub fn inject_context(
    conversation: &[ModelMessage],
    db_path: &Path,
    session_id: &str,
    working_dir: &Path,
    project_dir: Option<&Path>,
    work_mode: WorkMode,
    skills_manager: &RwLock<SkillsManager>,
    model_id: Option<&str>,
    session_type: Option<&str>,
    hive_crew_slug: Option<&str>,
    user_id: Option<&str>,
) -> Vec<ModelMessage> {
    inject_context_with_hive_profile_and_group(
        conversation,
        db_path,
        session_id,
        working_dir,
        project_dir,
        work_mode,
        skills_manager,
        model_id,
        session_type,
        hive_crew_slug,
        user_id,
        None,
        None,
    )
}

/// Build the least-privilege prompt for one neutral Worker conversation run.
///
/// This path intentionally has no `working_dir`, `project_dir`, model, skills,
/// global Hive profile, or work-mode arguments, so it cannot accidentally
/// fall back to repository or process context. The caller must have already
/// validated the same Worker/lane against its claimed provider governor.
pub(crate) fn inject_worker_conversation_context(
    conversation: &[ModelMessage],
    db_path: &Path,
    session_id: &str,
    expected_worker_id: &str,
    user_id: Option<&str>,
    hive_group_run: Option<&HiveGroupRunContext>,
) -> Result<Vec<ModelMessage>, WorkerConversationContextError> {
    let persona = match hive::resolve_worker_conversation_persona(
        db_path,
        session_id,
        user_id,
        hive_group_run,
    ) {
        hive::HiveWorkerConversationLookup::Primary => {
            return Err(WorkerConversationContextError::MissingBinding)
        }
        hive::HiveWorkerConversationLookup::Denied => {
            return Err(WorkerConversationContextError::DeniedBinding)
        }
        hive::HiveWorkerConversationLookup::Worker(persona) => persona,
    };
    if persona.worker_id != expected_worker_id {
        return Err(WorkerConversationContextError::WorkerMismatch {
            expected: expected_worker_id.to_string(),
            actual: persona.worker_id,
        });
    }

    let knowledge = reports::build_hive_knowledge_context(
        db_path,
        None,
        user_id,
        Some(&persona.memory_namespace_id),
        Some(&persona.worker_id),
        session_id,
        hive_group_run.map(|run| run.group_id.as_str()),
        conversation,
    );
    let episodes = episodes::build_episode_context(
        db_path,
        None,
        user_id,
        Some(&persona.worker_id),
        session_id,
        conversation,
    );
    let group_room = match hive_group_run {
        Some(group_run) => Some(
            hive::build_group_room_section(db_path, group_run)
                .ok_or(WorkerConversationContextError::GroupRoomUnavailable)?,
        ),
        None => None,
    };

    let mut injected = Vec::with_capacity(conversation.len() + persona.sections.len() + 4);
    injected.push(ModelMessage {
        role: Role::System,
        content: vec![Content::text(
            WORKER_CONVERSATION_CAPABILITY_CONTEXT.to_string(),
        )],
    });
    for text in persona.sections {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(text)],
        });
    }
    if !knowledge.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(knowledge)],
        });
    }
    if !episodes.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(episodes)],
        });
    }
    if let Some(text) = group_room {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(text)],
        });
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(
                WORKER_GROUP_RESPONSE_CAPABILITY_CONTEXT.to_string(),
            )],
        });
    }
    bound_dynamic_context_messages(&mut injected);
    injected.extend_from_slice(conversation);
    Ok(injected)
}

/// Build the least-privilege prompt for one bounded durable Worker Goal run.
///
/// The initial typed trigger is supplied ephemerally by the Hive runtime. This
/// function only prepends system context and never creates or persists a fake
/// user message. It deliberately omits global Hive identity, ordinary chat or
/// group transcripts, episodes, project instruction files, skills, extension
/// context, and ambient Workflow lookup.
pub(crate) fn inject_worker_goal_context(
    conversation: &[ModelMessage],
    db_path: &Path,
    session_id: &str,
    user_id: Option<&str>,
    goal_context: &WorkerGoalExecutionContext,
    execution_tool_allowlist: &HashSet<String>,
) -> Result<Vec<ModelMessage>, WorkerConversationContextError> {
    let binding = goal_context.binding();
    let persona = match hive::resolve_worker_goal_persona(db_path, session_id, user_id) {
        hive::HiveWorkerConversationLookup::Worker(persona) => persona,
        hive::HiveWorkerConversationLookup::Primary => {
            return Err(WorkerConversationContextError::MissingBinding)
        }
        hive::HiveWorkerConversationLookup::Denied => {
            return Err(WorkerConversationContextError::DeniedBinding)
        }
    };
    if persona.worker_id != binding.worker_id {
        return Err(WorkerConversationContextError::WorkerMismatch {
            expected: binding.worker_id.clone(),
            actual: persona.worker_id,
        });
    }
    if persona.worker_revision != binding.worker_revision {
        return Err(WorkerConversationContextError::WorkerRevisionMismatch {
            expected: binding.worker_revision,
            actual: persona.worker_revision,
        });
    }

    let project_dir = binding.workspace_dir.to_string_lossy();
    let knowledge = reports::build_hive_worker_goal_knowledge_context(
        db_path,
        project_dir.as_ref(),
        user_id,
        &persona.memory_namespace_id,
        &persona.worker_id,
        session_id,
        conversation,
    );
    let workflow = render_worker_goal_workflow_context(goal_context)?;
    let workspace = format!(
        "[WORKER GOAL WORKSPACE]\n\nExact workspace root: {}\n- This absolute path is the only workspace attached to this attempt.\n- Keep every file operation inside this root and use paths rooted here.\n- Do not discover, switch to, or infer another project or workspace.\n\n[/WORKER GOAL WORKSPACE]",
        binding.workspace_dir.display()
    );
    let mut allowed_tools = execution_tool_allowlist.iter().cloned().collect::<Vec<_>>();
    allowed_tools.sort();
    let tool_summary = if allowed_tools.is_empty() {
        "none".to_string()
    } else {
        allowed_tools.join(", ")
    };
    let capability = format!(
        "[WORKER GOAL CAPABILITY]\n\nThis is one bounded durable Hive Worker Goal attempt, not an ordinary chat, group-room turn, heartbeat, or open-ended autonomous loop. Work only on the exact frozen Goal, plan revision, step, attempt, Worker identity, and workspace shown below. Use only these advertised tools: {tool_summary}. Every tool remains governed by the run's normal permission mode and workspace sandbox. You have no web, tool discovery, subagents, delegation, MCP, extensions, skills, cross-Worker messaging, group history, global Hive identity, or unseen conversation history. Never claim work or evidence that the tools did not establish. A final response reports this attempt's outcome; it does not by itself prove the Goal complete.\n\n[/WORKER GOAL CAPABILITY]"
    );

    let mut injected = Vec::with_capacity(conversation.len() + persona.sections.len() + 5);
    for text in [capability, workspace, workflow] {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(text)],
        });
    }
    for text in persona.sections {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(text)],
        });
    }
    if !knowledge.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(knowledge)],
        });
    }
    bound_dynamic_context_messages(&mut injected);
    injected.extend_from_slice(conversation);
    Ok(injected)
}

fn render_worker_goal_workflow_context(
    goal_context: &WorkerGoalExecutionContext,
) -> Result<String, WorkerConversationContextError> {
    let binding = goal_context.binding();
    let snapshot = goal_context.workflow_snapshot();
    let plan = snapshot
        .plan_revision
        .as_ref()
        .filter(|plan| plan.id == binding.plan_revision_id)
        .ok_or(WorkerConversationContextError::WorkflowBindingMismatch)?;
    let step = snapshot
        .steps
        .iter()
        .find(|step| step.id == binding.step_id)
        .ok_or(WorkerConversationContextError::WorkflowBindingMismatch)?;
    let attempt = snapshot
        .latest_attempt
        .as_ref()
        .filter(|attempt| attempt.id == binding.attempt_id)
        .ok_or(WorkerConversationContextError::WorkflowBindingMismatch)?;

    let mut lines = vec![
        "[CANONICAL WORKER GOAL ATTEMPT]".to_string(),
        format!("Goal id: {}", binding.goal_id),
        format!("Goal revision: {}", binding.goal_revision),
        format!(
            "Workflow aggregate revision: {}",
            binding.workflow_aggregate_revision
        ),
        format!("Title: {}", truncate_utf8(&snapshot.goal.title, WORKER_GOAL_FIELD_CHARS)),
        format!(
            "Objective: {}",
            truncate_utf8(&snapshot.goal.objective, WORKER_GOAL_FIELD_CHARS)
        ),
        format!(
            "Plan: {} (id {}, revision {})",
            truncate_utf8(&plan.title, WORKER_GOAL_FIELD_CHARS),
            binding.plan_revision_id,
            binding.plan_revision_number
        ),
        format!("Attempt id: {}", binding.attempt_id),
        format!(
            "Attempt limits: max_turns={}, max_tool_calls={}, max_wall_time_secs={}, max_research_actions={}",
            attempt.max_turns,
            attempt.max_tool_calls,
            attempt.max_wall_time_secs,
            attempt.max_research_actions
        ),
        format!(
            "Exact step: {} - {} (id {}, revision {}, status {})",
            step.display_key,
            truncate_utf8(&step.description, WORKER_GOAL_FIELD_CHARS),
            binding.step_id,
            binding.step_revision,
            step.status
        ),
    ];
    if let Some(context) = step.context.as_deref() {
        lines.push(format!(
            "Step context: {}",
            truncate_utf8(context, WORKER_GOAL_FIELD_CHARS)
        ));
    }
    append_bounded_list(&mut lines, "Goal constraints", &snapshot.goal.constraints);
    append_bounded_list(
        &mut lines,
        "Step acceptance criteria",
        &step.acceptance_criteria,
    );

    if !snapshot.criteria.is_empty() {
        lines.push("Goal verification criteria:".to_string());
        for criterion in snapshot.criteria.iter().take(WORKER_GOAL_LIST_ITEMS) {
            lines.push(format!(
                "- [{}] {} (id {}, required={})",
                criterion.status,
                truncate_utf8(&criterion.description, WORKER_GOAL_FIELD_CHARS),
                criterion.id,
                criterion.required
            ));
        }
        let omitted = snapshot
            .criteria
            .len()
            .saturating_sub(WORKER_GOAL_LIST_ITEMS);
        if omitted > 0 {
            lines.push(format!(
                "- {omitted} additional criteria omitted by prompt budget"
            ));
        }
    }

    let dependency_ids = snapshot
        .dependencies
        .iter()
        .filter(|dependency| dependency.step_id == binding.step_id)
        .map(|dependency| dependency.depends_on_step_id.as_str())
        .collect::<HashSet<_>>();
    if !dependency_ids.is_empty() {
        lines.push("Exact step dependencies:".to_string());
        for dependency in snapshot
            .steps
            .iter()
            .filter(|candidate| dependency_ids.contains(candidate.id.as_str()))
            .take(WORKER_GOAL_LIST_ITEMS)
        {
            lines.push(format!(
                "- [{}] {}: {} (id {})",
                dependency.status,
                dependency.display_key,
                truncate_utf8(&dependency.description, WORKER_GOAL_FIELD_CHARS),
                dependency.id
            ));
        }
    }
    lines.push("[/CANONICAL WORKER GOAL ATTEMPT]".to_string());

    let rendered = lines.join("\n");
    if rendered.len() <= WORKER_GOAL_WORKFLOW_CONTEXT_BYTES {
        return Ok(rendered);
    }
    const END: &str =
        "\n[WORKER GOAL SNAPSHOT TRUNCATED AT REQUEST BUDGET]\n[/CANONICAL WORKER GOAL ATTEMPT]";
    let mut bounded = truncate_utf8_bytes(
        &rendered,
        WORKER_GOAL_WORKFLOW_CONTEXT_BYTES.saturating_sub(END.len()),
    );
    bounded.push_str(END);
    Ok(bounded)
}

fn append_bounded_list(lines: &mut Vec<String>, title: &str, values: &[String]) {
    if values.is_empty() {
        return;
    }
    lines.push(format!("{title}:"));
    for value in values.iter().take(WORKER_GOAL_LIST_ITEMS) {
        lines.push(format!(
            "- {}",
            truncate_utf8(value, WORKER_GOAL_FIELD_CHARS)
        ));
    }
    let omitted = values.len().saturating_sub(WORKER_GOAL_LIST_ITEMS);
    if omitted > 0 {
        lines.push(format!(
            "- {omitted} additional items omitted by prompt budget"
        ));
    }
}

#[allow(clippy::too_many_arguments)]
pub fn inject_context_with_hive_profile_and_group(
    conversation: &[ModelMessage],
    db_path: &Path,
    session_id: &str,
    working_dir: &Path,
    project_dir: Option<&Path>,
    work_mode: WorkMode,
    skills_manager: &RwLock<SkillsManager>,
    model_id: Option<&str>,
    session_type: Option<&str>,
    hive_crew_slug: Option<&str>,
    user_id: Option<&str>,
    hive_profile: Option<&HiveProfileSnapshot>,
    hive_group_run: Option<&HiveGroupRunContext>,
) -> Vec<ModelMessage> {
    let is_chat = session_type == Some("chat");

    if is_chat {
        // The caller owns the Chat capability prompt because it also owns the
        // request-time tool filter (for example, research mode). Injecting a
        // second capability policy here can contradict the tools that are
        // actually present on the request or an explicit custom prompt.
        let mut injected = Vec::with_capacity(conversation.len() + 1);
        let memory_ctx = memory::build_memory_context(db_path, None, user_id, conversation);
        if !memory_ctx.is_empty() {
            injected.push(ModelMessage {
                role: Role::System,
                content: vec![Content::text(memory_ctx)],
            });
        }
        injected.extend_from_slice(conversation);
        return injected;
    }

    let workspace_ctx = workspace::build_workspace_context(working_dir, project_dir);
    let env_ctx = workspace::build_environment_context(working_dir, model_id);
    let context_project_dir = project_dir.map(|p| p.to_string_lossy().to_string());
    let is_hive = session_type == Some("hive");
    // When the hive session is a Worker's private DM lane, the Worker's own
    // persona replaces the generic crew treatment and its memory namespace
    // scopes retrieval. Sessions without a Worker binding keep the primary
    // companion behavior unchanged.
    let worker_conversation = if is_hive {
        Some(hive::resolve_worker_conversation_persona(
            db_path,
            session_id,
            user_id,
            hive_group_run,
        ))
    } else {
        None
    };
    let worker_persona = worker_conversation
        .as_ref()
        .and_then(|lookup| match lookup {
            hive::HiveWorkerConversationLookup::Worker(persona) => Some(persona),
            hive::HiveWorkerConversationLookup::Primary
            | hive::HiveWorkerConversationLookup::Denied => None,
        });
    let worker_scope_denied = matches!(
        worker_conversation.as_ref(),
        Some(hive::HiveWorkerConversationLookup::Denied)
    );
    let hive_memory_namespace = worker_persona
        .map(|persona| persona.memory_namespace_id.as_str())
        .or_else(|| (!worker_scope_denied).then_some(hive_crew_slug).flatten());
    let hive_worker_id = worker_persona.map(|persona| persona.worker_id.as_str());
    let worker_persona_sections = worker_persona
        .map(|persona| persona.sections.as_slice())
        .unwrap_or_default();
    let memory_ctx = if is_hive {
        String::new()
    } else {
        memory::build_memory_context(
            db_path,
            context_project_dir.as_deref(),
            user_id,
            conversation,
        )
    };
    let plan_ctx = build_plan_context(db_path, session_id, work_mode);
    let delegated_ctx = runtime_state::build_delegated_context(db_path, session_id);
    let task_ctx = runtime_state::build_autonomous_task_context(db_path, session_id);
    let report_ctx = if is_hive {
        String::new()
    } else {
        reports::build_report_context(
            db_path,
            context_project_dir.as_deref(),
            user_id,
            conversation,
        )
    };
    let hive_knowledge_ctx = if is_hive && !worker_scope_denied {
        reports::build_hive_knowledge_context(
            db_path,
            context_project_dir.as_deref(),
            user_id,
            hive_memory_namespace,
            hive_worker_id,
            session_id,
            hive_group_run.map(|run| run.group_id.as_str()),
            conversation,
        )
    } else {
        String::new()
    };
    // Primary Hive recall excludes every Worker lane. A validated Worker gets
    // continuity only from that same Worker's DM/group lanes; denied group
    // claims get no episodic fallback at all.
    let hive_episode_ctx = if is_hive && !worker_scope_denied {
        episodes::build_episode_context(
            db_path,
            context_project_dir.as_deref(),
            user_id,
            hive_worker_id,
            session_id,
            conversation,
        )
    } else {
        String::new()
    };
    let skills_ctx = build_skills_context(skills_manager, project_dir.is_some());
    let project_settings = project_dir.map(ProjectSettings::load).unwrap_or_default();
    let project_context =
        project::ProjectContext::load(working_dir, project_dir, &project_settings);
    let mut hive_ctx_sections = if is_hive {
        if let Some(profile) = hive_profile {
            hive::build_hive_context_sections_with_profile(
                project_dir.unwrap_or(working_dir),
                profile,
                hive_crew_slug,
                worker_persona_sections,
            )
        } else {
            hive::build_hive_context_sections(
                project_dir.unwrap_or(working_dir),
                hive_crew_slug,
                worker_persona_sections,
            )
        }
    } else {
        Vec::new()
    };
    if is_hive {
        hive_ctx_sections.insert(
            0,
            crate::agent::autonomy::coordinator_prompt::hive_coordinator_system_prompt(),
        );
    }
    // A group member run sees the room right after its persona: title,
    // roster, and the bounded recent timeline. Rebuilt per provider call so
    // parallel members observe each other's posts as they land.
    if is_hive && !worker_scope_denied {
        if let Some(group_run) = hive_group_run {
            if let Some(section) = hive::build_group_room_section(db_path, group_run) {
                hive_ctx_sections.push(section);
            }
        }
    }
    let mut injected = Vec::with_capacity(conversation.len() + 7);

    if !workspace_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(workspace_ctx)],
        });
    }
    if !env_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(env_ctx)],
        });
    }
    if !memory_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(memory_ctx)],
        });
    }
    if !hive_knowledge_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(hive_knowledge_ctx)],
        });
    }
    if !hive_episode_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(hive_episode_ctx)],
        });
    }
    if !project_context.instructions.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(project_context.instructions)],
        });
    }
    for text in hive_ctx_sections {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(text)],
        });
    }
    let delegation_mode = project_settings.delegation_mode.unwrap_or(if is_hive {
        DelegationMode::Proactive
    } else {
        // Code mode is the coordinating parent by product contract. The
        // Orchestrator prompt still keeps simple and tightly-coupled work in
        // the parent, while making substantial decomposable work delegate
        // early instead of relying on a weak best-effort suggestion.
        DelegationMode::Orchestrator
    });
    injected.push(ModelMessage {
        role: Role::System,
        content: vec![Content::text(delegation_mode.prompt_contract())],
    });
    if !project_context.settings.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(project_context.settings)],
        });
    }
    if !plan_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(plan_ctx)],
        });
    }
    if !delegated_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(delegated_ctx)],
        });
    }
    if !task_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(task_ctx)],
        });
    }
    if !report_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(report_ctx)],
        });
    }
    if !skills_ctx.is_empty() {
        injected.push(ModelMessage {
            role: Role::System,
            content: vec![Content::text(skills_ctx)],
        });
    }

    bound_dynamic_context_messages(&mut injected);
    injected.extend_from_slice(conversation);
    injected
}

fn bound_dynamic_context_messages(messages: &mut Vec<ModelMessage>) {
    let total_bytes = messages
        .iter()
        .filter_map(context_message_text)
        .map(str::len)
        .sum::<usize>();
    if total_bytes <= MAX_DYNAMIC_CONTEXT_BYTES {
        return;
    }

    let mut allocation_order = (0..messages.len()).collect::<Vec<_>>();
    allocation_order.sort_by(|left, right| {
        let left_text = context_message_text(&messages[*left]).unwrap_or_default();
        let right_text = context_message_text(&messages[*right]).unwrap_or_default();
        dynamic_context_priority(right_text)
            .cmp(&dynamic_context_priority(left_text))
            .then_with(|| left.cmp(right))
    });

    let mut remaining = MAX_DYNAMIC_CONTEXT_BYTES;
    let mut retained = vec![None; messages.len()];
    let mut omitted = Vec::new();
    let mut truncated = Vec::new();
    for index in allocation_order {
        let Some(text) = context_message_text(&messages[index]) else {
            continue;
        };
        let label = context_section_label(text);
        if text.len() <= remaining {
            retained[index] = Some(text.to_string());
            remaining -= text.len();
            continue;
        }

        let marker = format!("\n[{} TRUNCATED AT AGGREGATE CONTEXT BUDGET]", label);
        if remaining > marker.len() + 256 {
            let mut bounded = truncate_utf8_bytes(text, remaining - marker.len());
            bounded.push_str(&marker);
            retained[index] = Some(bounded);
            truncated.push(label);
            remaining = 0;
        } else {
            omitted.push(label);
        }
    }

    let mut bounded_messages = Vec::with_capacity(messages.len());
    for (index, mut message) in std::mem::take(messages).into_iter().enumerate() {
        if context_message_text(&message).is_none() {
            bounded_messages.push(message);
            continue;
        }
        let Some(text) = retained[index].take() else {
            continue;
        };
        message.content = vec![Content::text(text)];
        bounded_messages.push(message);
    }
    *messages = bounded_messages;

    warn!(
        total_bytes,
        retained_bytes = MAX_DYNAMIC_CONTEXT_BYTES - remaining,
        max_bytes = MAX_DYNAMIC_CONTEXT_BYTES,
        truncated_sections = ?truncated,
        omitted_sections = ?omitted,
        "Dynamic request context exceeded its aggregate budget"
    );
}

fn context_message_text(message: &ModelMessage) -> Option<&str> {
    if message.role != Role::System || message.content.len() != 1 {
        return None;
    }
    match &message.content[0] {
        Content::Text { text, .. } => Some(text),
        _ => None,
    }
}

fn dynamic_context_priority(text: &str) -> u8 {
    ContextKind::from_text(text).budget_priority()
}

fn context_section_label(text: &str) -> String {
    text.lines()
        .next()
        .unwrap_or("CONTEXT SECTION")
        .trim_matches(['[', ']'])
        .chars()
        .take(80)
        .collect()
}

/// Truncate a string to at most `max_chars` characters on a valid UTF-8
/// boundary, appending "..." when truncation occurs.
pub(super) fn truncate_utf8(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let truncated: String = s.chars().take(max_chars).collect();
    format!("{}...", truncated)
}

/// Truncate a string to at most `max_bytes` without splitting a UTF-8 code
/// point. This is used for request-context budgets, which are byte based so a
/// multi-byte document cannot silently exceed the configured cap.
pub(super) fn truncate_utf8_bytes(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }

    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

pub(super) fn open_context_database(db_path: &Path, context: &'static str) -> Option<Database> {
    match Database::new(db_path) {
        Ok(db) => Some(db),
        Err(error) => {
            warn!(context, db_path = %db_path.display(), error = %error, "Failed to open context database");
            None
        }
    }
}
