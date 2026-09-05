use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::RwLock;

use super::hive::{
    build_group_room_section, build_hive_context_sections, build_hive_context_sections_with_home,
    load_worker_persona,
};
use super::reports::build_hive_knowledge_context;
use super::workspace::{build_environment_context, summarize_git_status};
use super::{
    bound_dynamic_context_messages, build_plan_context, build_project_context,
    build_skills_context, inject_context, inject_context_with_hive_profile_and_group,
    inject_worker_conversation_context, inject_worker_goal_context, MAX_DYNAMIC_CONTEXT_BYTES,
};

use crate::agent::{DelegatedRunStage, WorkerGoalExecutionBinding, WorkerGoalExecutionContext};
use crate::ai::types::{Content, ModelMessage, Role};
use crate::paths;
use crate::plan::PlanManager;
use crate::skills::SkillsManager;
use crate::storage::reports::{CreateReportInput, ReportScope};
use crate::storage::{
    AutonomousTaskStore, CanonicalMemoryInput, Database, DelegatedRunRole, DelegatedRunScope,
    DelegatedRunStartInput, DelegatedRunStore, EpisodeStore, HiveGroupRunContext, HiveGroupStore,
    HiveGroupWorkerLaneStore, HiveWorkerDocumentKind, HiveWorkerStore, MemoryAclScope,
    MemoryNamespace, MemoryStore, MemoryType, NewHiveGroup, NewHiveGroupMessage,
    NewHiveGroupWorkerLane, NewHiveWorker, ReportStore, SessionManager, WorkMode,
};
use crate::workflow::{
    AttemptStatus, CollaborationMode, ExecutionAttempt, Goal, GoalCriterion, GoalStatus,
    PlanRevision, PlanRevisionStatus, WorkflowSnapshot, WorkflowStep, WorkflowStepStatus,
};

fn test_worker_goal_context(
    workspace: &std::path::Path,
    worker_id: &str,
    worker_revision: u64,
) -> WorkerGoalExecutionContext {
    let timestamp = "2026-08-25T00:00:00.000000Z".to_string();
    let snapshot = WorkflowSnapshot {
        schema_version: 1,
        aggregate_revision: 2,
        collaboration_mode: CollaborationMode::Default,
        permission_mode: "supervised".into(),
        goal: Goal {
            id: "goal-context-1".into(),
            session_id: "worker-goal-session".into(),
            title: "Context isolation goal".into(),
            objective: "Verify the boundary-context marker without ambient data".into(),
            constraints: vec!["Keep the exact Worker scope".into()],
            status: GoalStatus::Active,
            status_reason: None,
            needs_definition: false,
            revision: 2,
            token_budget: None,
            tokens_used: 0,
            source: "hive_worker".into(),
            legacy_plan_id: None,
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
            activated_at: Some(timestamp.clone()),
            completed_at: None,
            cancelled_at: None,
        },
        criteria: vec![GoalCriterion {
            id: "criterion-context-1".into(),
            goal_id: "goal-context-1".into(),
            position: 0,
            description: "boundary-context marker is isolated".into(),
            required: true,
            status: crate::workflow::CriterionStatus::Pending,
            evidence: Vec::new(),
            verifier: None,
            verified_at: None,
        }],
        plan_revision: Some(PlanRevision {
            id: "plan-context-1".into(),
            goal_id: "goal-context-1".into(),
            revision_number: 1,
            status: PlanRevisionStatus::Active,
            title: "Exact context plan".into(),
            rationale: None,
            source_message_id: None,
            predecessor_id: None,
            legacy_markdown: None,
            created_at: timestamp.clone(),
            approved_at: Some(timestamp.clone()),
            completed_at: None,
        }),
        steps: vec![WorkflowStep {
            id: "step-context-1".into(),
            plan_revision_id: "plan-context-1".into(),
            parent_step_id: None,
            display_key: "1".into(),
            position: 0,
            description: "Inspect the exact boundary-context marker".into(),
            context: None,
            acceptance_criteria: vec!["No unrelated context appears".into()],
            required: true,
            status: WorkflowStepStatus::InProgress,
            outcome: None,
            evidence: Vec::new(),
            claimed_attempt_id: Some("attempt-context-1".into()),
            revision: 3,
            created_at: timestamp.clone(),
            started_at: Some(timestamp.clone()),
            completed_at: None,
        }],
        dependencies: Vec::new(),
        latest_attempt: Some(ExecutionAttempt {
            id: "attempt-context-1".into(),
            goal_id: "goal-context-1".into(),
            plan_revision_id: Some("plan-context-1".into()),
            step_id: Some("step-context-1".into()),
            status: AttemptStatus::Running,
            stop_reason: None,
            permission_mode: "supervised".into(),
            goal_revision_at_start: 2,
            max_turns: 3,
            max_tool_calls: 8,
            max_wall_time_secs: 300,
            max_research_actions: 4,
            turn_count: 0,
            tool_call_count: 0,
            research_action_count: 0,
            progress_revision: 0,
            blocker_fingerprint: None,
            blocker_streak: 0,
            started_at: timestamp.clone(),
            updated_at: timestamp,
            ended_at: None,
        }),
        allowed_actions: vec!["pause_goal".into()],
    };
    WorkerGoalExecutionContext::new(
        WorkerGoalExecutionBinding {
            worker_id: worker_id.into(),
            worker_revision,
            owner_user_id: None,
            session_id: "worker-goal-session".into(),
            run_id: "worker-goal-run-context-1".into(),
            run_lease_token: "worker-goal-lease-context-1".into(),
            run_lease_epoch: 1,
            run_origin: crate::storage::WorkerRunOrigin::UserWorkflowActivation,
            goal_id: "goal-context-1".into(),
            goal_revision: 2,
            workflow_aggregate_revision: 2,
            attempt_id: "attempt-context-1".into(),
            plan_revision_id: "plan-context-1".into(),
            plan_revision_number: 1,
            step_id: "step-context-1".into(),
            step_revision: 3,
            workspace_dir: workspace.to_path_buf(),
        },
        Arc::new(snapshot),
    )
}

#[test]
fn project_context_loads_hierarchical_instruction_files() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    let nested = repo.join("a").join("b");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join("AGENTS.md"), "root instructions").unwrap();
    fs::write(repo.join("a").join("CLAUDE.md"), "nested instructions").unwrap();

    let context = build_project_context(&nested);

    assert!(context.contains("root instructions"));
    assert!(context.contains("nested instructions"));
}

#[test]
fn build_hive_context_loads_global_home_files_and_project_overlay() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    let hive_home = temp.path().join("hive-home");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&hive_home).unwrap();

    fs::write(hive_home.join(paths::HIVE_SOUL_FILE), "Keep moving.").unwrap();
    fs::write(hive_home.join(paths::HIVE_IDENTITY_FILE), "Name: Hive").unwrap();
    fs::write(
        hive_home.join(paths::HIVE_HEARTBEAT_FILE),
        "Check queued work.",
    )
    .unwrap();
    fs::write(repo.join("HIVE.md"), "Project-specific operating notes.").unwrap();

    let context = build_hive_context_sections_with_home(&repo, &hive_home, None, &[]).join("\n\n");

    assert!(context.contains("[HIVE SOUL - HIVE_SOUL.md]"));
    assert!(context.contains("Keep moving."));
    assert!(context.contains("[HIVE IDENTITY - HIVE_IDENTITY.md]"));
    assert!(context.contains("Name: Hive"));
    assert!(context.contains("[HIVE HEARTBEAT - HIVE_HEARTBEAT.md]"));
    assert!(context.contains("Check queued work."));
    assert!(context.contains("[HIVE PROJECT OVERLAY - HIVE.md]"));
    assert!(context.contains("Project-specific operating notes."));
}

#[test]
fn build_hive_context_falls_back_to_project_overlay_when_global_home_is_empty() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    let hive_home = temp.path().join("hive-home");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&hive_home).unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();

    let context = build_hive_context_sections_with_home(&repo, &hive_home, None, &[]).join("\n\n");

    assert!(context.contains("[HIVE PROJECT OVERLAY - HIVE.md]"));
    assert!(context.contains("Always Swimming."));
}

#[test]
fn build_hive_context_reads_deprecated_project_overlay_but_prefers_canonical_name() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    let hive_home = temp.path().join("hive-home");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&hive_home).unwrap();
    fs::write(
        repo.join(crate::identity::legacy::HIVE_PROJECT_OVERLAY_FILE_NAME),
        "Deprecated overlay.",
    )
    .unwrap();

    let deprecated =
        build_hive_context_sections_with_home(&repo, &hive_home, None, &[]).join("\n\n");
    assert!(deprecated.contains("Deprecated overlay."));

    fs::write(repo.join("HIVE.md"), "Canonical overlay.").unwrap();
    let canonical =
        build_hive_context_sections_with_home(&repo, &hive_home, None, &[]).join("\n\n");
    assert!(canonical.contains("Canonical overlay."));
    assert!(!canonical.contains("Deprecated overlay."));
}

#[test]
fn build_hive_context_accepts_legacy_generic_home_file_names() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    let hive_home = temp.path().join("hive-home");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&hive_home).unwrap();

    fs::write(hive_home.join("SOUL.md"), "Legacy soul.").unwrap();
    fs::write(hive_home.join("IDENTITY.md"), "Legacy identity.").unwrap();

    let context = build_hive_context_sections_with_home(&repo, &hive_home, None, &[]).join("\n\n");

    assert!(context.contains("[HIVE SOUL - SOUL.md]"));
    assert!(context.contains("Legacy soul."));
    assert!(context.contains("[HIVE IDENTITY - IDENTITY.md]"));
    assert!(context.contains("Legacy identity."));
}

#[test]
fn build_hive_context_never_activates_legacy_crew_memory_as_instructions() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    let hive_home = temp.path().join("hive-home");
    let crew = hive_home.join("crew").join("reviewer");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&crew).unwrap();
    fs::write(crew.join("IDENTITY.md"), "Reviewer identity.").unwrap();
    fs::write(crew.join("SOUL.md"), "Evidence first.").unwrap();
    fs::write(crew.join("MEMORY.md"), "legacy-secret-memory-marker").unwrap();

    let context = build_hive_context_sections_with_home(&repo, &hive_home, Some("reviewer"), &[])
        .join("\n\n");

    assert!(context.contains("Reviewer identity."));
    assert!(context.contains("Evidence first."));
    assert!(!context.contains("legacy-secret-memory-marker"));
    assert!(!context.contains("CREW MEMORY"));
}

#[test]
fn build_hive_context_uses_global_home_path_helper_without_panic() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();

    let context = build_hive_context_sections(&repo, None, &[]).join("\n\n");

    assert!(context.contains("Always Swimming."));
}

#[test]
fn build_hive_context_sections_preserve_layer_order() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    let hive_home = temp.path().join("hive-home");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&hive_home).unwrap();

    fs::write(hive_home.join(paths::HIVE_SOUL_FILE), "Soul.").unwrap();
    fs::write(hive_home.join(paths::HIVE_IDENTITY_FILE), "Identity.").unwrap();
    fs::write(hive_home.join(paths::HIVE_USER_FILE), "User.").unwrap();
    fs::write(hive_home.join(paths::HIVE_HEARTBEAT_FILE), "Heartbeat.").unwrap();
    fs::write(hive_home.join(paths::HIVE_MEMORY_FILE), "Memory.").unwrap();
    fs::write(hive_home.join(paths::HIVE_CHANNELS_FILE), "Channels.").unwrap();
    fs::write(repo.join("HIVE.md"), "Overlay.").unwrap();

    let sections = build_hive_context_sections_with_home(&repo, &hive_home, None, &[]);
    let labels = sections
        .iter()
        .map(|section| {
            section
                .lines()
                .next()
                .unwrap_or_default()
                .trim()
                .to_string()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        labels,
        vec![
            "[HIVE SOUL - HIVE_SOUL.md]".to_string(),
            "[HIVE IDENTITY - HIVE_IDENTITY.md]".to_string(),
            "[HIVE USER - HIVE_USER.md]".to_string(),
            "[HIVE HEARTBEAT - HIVE_HEARTBEAT.md]".to_string(),
            "[HIVE CHANNELS - HIVE_CHANNELS.md]".to_string(),
            "[HIVE PROJECT OVERLAY - HIVE.md]".to_string(),
        ]
    );
    assert!(sections.iter().all(|section| !section.contains("Memory.")));
}

#[test]
fn build_plan_context_falls_back_to_generic_plan_mode_when_store_unavailable() {
    let temp = TempDir::new().unwrap();
    let missing_db_path = temp.path().join("missing").join("mitsuro.db");

    let context = build_plan_context(&missing_db_path, "session-id", WorkMode::Plan);

    assert!(context.contains("[PLAN MODE ACTIVE]"));
    assert!(context.contains("You CANNOT write, edit, or create files"));
}

#[test]
fn build_plan_context_returns_empty_when_store_unavailable_in_build_mode() {
    let temp = TempDir::new().unwrap();
    let missing_db_path = temp.path().join("missing").join("mitsuro.db");

    let context = build_plan_context(&missing_db_path, "session-id", WorkMode::Build);

    assert!(context.is_empty());
}

#[test]
fn build_plan_context_uses_compact_active_task_view_in_build_mode() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("plans.db");
    let session_id = SessionManager::new(Database::new(&db_path).unwrap())
        .create_session("plan context test", None, None)
        .unwrap();
    let manager = PlanManager::new(db_path.clone()).unwrap();
    let mut plan = manager
        .create_plan("Token efficient plan", &session_id, None)
        .unwrap();
    let phase = plan.add_phase("Implementation");
    for index in 0..18 {
        phase.add_task(format!("Implement bounded item {index}"));
    }
    manager.save_plan(&plan).unwrap();

    let context = build_plan_context(&db_path, &session_id, WorkMode::Build);

    assert!(context.contains("[ACTIVE PLAN"));
    assert!(context.contains("Additional tasks omitted from prompt"));
    assert!(!context.contains("## Current Plan"));
    assert!(!context.contains("Task Workflow Protocol"));
}

#[test]
fn build_skills_context_returns_empty_when_manager_is_busy() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let _guard = skills
        .try_write()
        .unwrap_or_else(|_| panic!("test should acquire write lock"));

    let context = build_skills_context(&skills, true);

    assert!(context.is_empty());
}

#[test]
fn build_environment_context_does_not_execute_repo_local_git_commands() {
    let temp = TempDir::new().unwrap();
    let repo_path = temp.path().join("repo");
    fs::create_dir_all(&repo_path).unwrap();
    git2::Repository::init(&repo_path).unwrap();

    let payload_path = repo_path.join("fsmonitor-payload.sh");
    let marker_path = repo_path.join("fsmonitor-marker");
    fs::write(
        &payload_path,
        format!("#!/bin/sh\necho vulnerable > {}\n", marker_path.display()),
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&payload_path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&payload_path, permissions).unwrap();
    }

    fs::OpenOptions::new()
        .append(true)
        .open(repo_path.join(".git").join("config"))
        .unwrap()
        .write_all(format!("\n[core]\n	fsmonitor = {}\n", payload_path.display()).as_bytes())
        .unwrap();

    let context = build_environment_context(&repo_path, Some("test-model"));

    assert!(context.contains("Git repository: yes"));
    assert!(context.contains("Model: test-model"));
    assert!(
        !marker_path.exists(),
        "environment context must not execute repo-local git helpers"
    );
}

#[test]
fn summarize_git_status_counts_modified_staged_and_untracked() {
    let summary = summarize_git_status(" M src/lib.rs\nA  Cargo.toml\n?? scratch.txt\n");

    assert_eq!(
        summary.as_deref(),
        Some("1 modified, 1 staged, 1 untracked")
    );
}

#[test]
fn summarize_git_status_returns_none_for_clean_status() {
    let summary = summarize_git_status("");

    assert!(summary.is_none());
}

#[test]
fn inject_context_skips_project_instructions_without_explicit_project_dir() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join("AGENTS.md"), "repo instructions").unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("hello".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "session-id",
        repo,
        None,
        WorkMode::Build,
        &skills,
        None,
        None,
        None,
        None,
    );

    assert!(injected.len() >= 3);
    assert_eq!(injected[0].role, Role::System);
    assert!(matches!(
        &injected[0].content[0],
        Content::Text { text, .. } if text.contains("[WORKSPACE MODE: NEUTRAL]")
    ));
    assert_eq!(injected[1].role, Role::System);
    assert!(matches!(
        &injected[1].content[0],
        Content::Text { text, .. } if text.contains("[ENVIRONMENT]")
    ));
    assert!(matches!(
        injected.last(),
        Some(message) if message.role == Role::User
    ));
    assert!(injected.iter().all(|message| {
        message.content.iter().all(|content| {
            !matches!(content, Content::Text { text, .. } if text.contains("repo instructions"))
        })
    }));
}

#[test]
fn chat_context_does_not_inject_a_conflicting_tool_policy() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("research the current release".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "chat-session",
        repo,
        None,
        WorkMode::Build,
        &skills,
        None,
        Some("chat"),
        None,
        None,
    );

    assert!(injected.iter().all(|message| {
        message.content.iter().all(|content| match content {
            Content::Text { text, .. } => !text.contains("do NOT have access to any tools"),
            _ => true,
        })
    }));
    assert_eq!(injected.last().unwrap().role, Role::User);
}

#[test]
fn aggregate_dynamic_context_budget_preserves_high_priority_sections() {
    let system_message = |text: String| ModelMessage {
        role: Role::System,
        content: vec![Content::text(text)],
    };
    let mut messages = vec![
        system_message(format!("[PERSISTENT MEMORY]\n{}", "memory ".repeat(8_000))),
        system_message(format!(
            "[PROJECT INSTRUCTIONS - AGENTS.md]\n{}",
            "instructions ".repeat(4_000)
        )),
        system_message("[ACTIVE PLAN - audit]\nFinish the selected task.".to_string()),
    ];

    bound_dynamic_context_messages(&mut messages);

    let retained_bytes = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text.len()),
            _ => None,
        })
        .sum::<usize>();
    assert!(retained_bytes <= MAX_DYNAMIC_CONTEXT_BYTES);
    assert!(messages.iter().any(|message| {
        matches!(&message.content[0], Content::Text { text, .. } if text.starts_with("[ACTIVE PLAN"))
    }));
    assert!(messages.iter().any(|message| {
        matches!(&message.content[0], Content::Text { text, .. } if text.starts_with("[PROJECT INSTRUCTIONS"))
    }));
}

#[test]
fn aggregate_context_pressure_preserves_complete_hive_identity_before_optional_context() {
    let system_message = |text: String| ModelMessage {
        role: Role::System,
        content: vec![Content::text(text)],
    };
    let mut messages = vec![
        system_message(format!(
            "[PERSISTENT MEMORY]\n{}",
            "optional memory ".repeat(4_000)
        )),
        system_message(format!(
            "[PROJECT INSTRUCTIONS - AGENTS.md]\n{}",
            "project instruction ".repeat(3_000)
        )),
        system_message("[HIVE COORDINATOR]\nCoordinate deliberately.".to_string()),
        system_message(format!(
            "[HIVE SOUL - profile:local]\n{}",
            "soul ".repeat(1_000)
        )),
        system_message(format!(
            "[HIVE IDENTITY - profile:local]\n{}",
            "identity ".repeat(800)
        )),
        system_message(format!(
            "[HIVE USER - profile:local]\n{}",
            "user preference ".repeat(600)
        )),
        system_message(format!(
            "[HIVE HEARTBEAT - profile:local]\n{}",
            "heartbeat ".repeat(3_000)
        )),
        system_message(format!(
            "[HIVE CHANNELS - profile:local]\n{}",
            "channels ".repeat(3_000)
        )),
    ];

    bound_dynamic_context_messages(&mut messages);

    let texts = messages
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for prefix in [
        "[HIVE COORDINATOR]",
        "[HIVE SOUL",
        "[HIVE IDENTITY",
        "[HIVE USER",
    ] {
        let retained = texts
            .iter()
            .find(|text| text.starts_with(prefix))
            .unwrap_or_else(|| panic!("missing stable Hive section {prefix}"));
        assert!(!retained.contains("TRUNCATED AT AGGREGATE CONTEXT BUDGET"));
    }
    assert!(texts.iter().all(|text| {
        !text.starts_with("[PERSISTENT MEMORY]")
            || text.contains("TRUNCATED AT AGGREGATE CONTEXT BUDGET")
    }));
    let retained_bytes = texts.iter().map(|text| text.len()).sum::<usize>();
    assert!(retained_bytes <= MAX_DYNAMIC_CONTEXT_BYTES);
}

#[test]
fn inject_context_filters_persistent_memory_by_user_id() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    let db_path = repo.join("mitsuro.db");
    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    memory_store
        .save(
            MemoryType::User,
            "Alice secret",
            "alice-only instruction",
            None,
            Some("alice"),
        )
        .unwrap();
    memory_store
        .save(
            MemoryType::User,
            "Bob preference",
            "bob-only preference",
            None,
            Some("bob"),
        )
        .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("please use bob preference".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        db_path.as_path(),
        "session-id",
        repo,
        None,
        WorkMode::Build,
        &skills,
        None,
        None,
        None,
        Some("bob"),
    );

    let context = injected
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(context.contains("Bob preference"));
    assert!(context.contains("bob-only preference"));
    assert!(!context.contains("Alice secret"));
    assert!(!context.contains("alice-only instruction"));
}

#[test]
fn inject_context_uses_latest_user_memory_relevance_and_hides_compaction_flushes() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    let db_path = repo.join("mitsuro.db");
    let project_dir = repo.to_string_lossy();
    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    memory_store
        .save(
            MemoryType::Project,
            "Compaction archive",
            "Prior compaction cleanup notes that should not leak into unrelated trivia.",
            Some(project_dir.as_ref()),
            None,
        )
        .unwrap();
    memory_store
        .save(
            MemoryType::Project,
            "Space notes",
            "Use space telescope examples when the user asks about space facts.",
            Some(project_dir.as_ref()),
            None,
        )
        .unwrap();
    memory_store
        .save(
            MemoryType::Project,
            &format!("{}1", crate::storage::COMPACTION_FLUSH_TITLE_PREFIX),
            "Full old conversation transcript should never be injected as memory.",
            Some(project_dir.as_ref()),
            None,
        )
        .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![
        ModelMessage {
            role: Role::User,
            content: vec![Content::text(
                "we were discussing compaction cleanup".to_string(),
            )],
        },
        ModelMessage {
            role: Role::Assistant,
            content: vec![Content::text("Understood.".to_string())],
        },
        ModelMessage {
            role: Role::User,
            content: vec![Content::text("tell me facts about space".to_string())],
        },
    ];

    let injected = inject_context(
        &conversation,
        db_path.as_path(),
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );

    let context = injected
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(context.contains("Space notes"));
    assert!(!context.contains("Compaction archive"));
    assert!(!context.contains("Full old conversation transcript"));
}

#[test]
fn generic_project_words_do_not_trigger_persistent_memory_injection() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    let db_path = repo.join("mitsuro.db");
    let project_dir = repo.to_string_lossy();
    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    for index in 0..12 {
        memory_store
            .save(
                MemoryType::Project,
                &format!("Project note {index}"),
                "Generic project system context and coding work notes.",
                Some(project_dir.as_ref()),
                None,
            )
            .unwrap();
    }

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Tell me about this project and its code.".to_string(),
        )],
    }];
    let injected = inject_context(
        &conversation,
        &db_path,
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );

    assert!(injected.iter().all(|message| {
        !matches!(&message.content[0], Content::Text { text, .. } if text.contains("[PERSISTENT MEMORY]"))
    }));
}

#[test]
fn inject_context_includes_hive_identity_only_for_hive_sessions() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join("AGENTS.md"), "repo instructions").unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("hello".to_string())],
    }];

    let hive_injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
    );
    let code_injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );

    assert!(hive_injected.iter().any(|message| {
        matches!(
            &message.content[0],
            Content::Text { text, .. } if text.contains("[HIVE PROJECT OVERLAY - HIVE.md]") && text.contains("Always Swimming.")
        )
    }));
    assert!(hive_injected.iter().all(|message| {
        !matches!(
            &message.content[0],
            Content::Text { text, .. } if text.contains("[HIVE HOME ")
        )
    }));
    assert!(!code_injected.iter().any(|message| {
        matches!(
            &message.content[0],
            Content::Text { text, .. } if text.contains("[HIVE PROJECT OVERLAY - HIVE.md]")
        )
    }));
}

#[test]
fn inject_context_places_all_hive_layers_before_project_settings() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::create_dir_all(repo.join(".mitsuro")).unwrap();
    fs::write(repo.join("AGENTS.md"), "repo instructions").unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();
    fs::write(
        repo.join(".mitsuro").join("settings.json"),
        r#"{ "system_prompt_append": "Project append." }"#,
    )
    .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("hello".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
    );

    let texts = injected
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let settings_index = texts
        .iter()
        .position(|text| text.contains("[PROJECT SETTINGS]"))
        .unwrap();
    let hive_indices = texts
        .iter()
        .enumerate()
        .filter_map(|(index, text)| text.contains("[HIVE ").then_some(index))
        .collect::<Vec<_>>();

    assert!(!hive_indices.is_empty());
    assert!(hive_indices.iter().all(|index| *index < settings_index));
}

#[test]
fn inject_context_includes_hive_coordinator_prompt_for_hive_sessions() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("hello".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
    );

    assert!(injected.iter().any(|message| {
        matches!(
            &message.content[0],
            Content::Text { text, .. } if text.contains("[HIVE COORDINATOR]")
        )
    }));
}

#[test]
fn inject_context_includes_hive_knowledge_from_memory_and_reports() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();

    let db_path = repo.join("mitsuro.db");
    let session_manager = SessionManager::new(Database::new(&db_path).unwrap());
    let session_id = session_manager
        .create_session("test", None, Some(repo.to_string_lossy().as_ref()))
        .unwrap();

    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    memory_store
        .save(
            MemoryType::Project,
            "Auth decision",
            "Use the daemon loop as the canonical wake path.",
            Some(repo.to_string_lossy().as_ref()),
            None,
        )
        .unwrap();
    memory_store
        .save(
            MemoryType::Project,
            &format!("{}1", crate::storage::COMPACTION_FLUSH_TITLE_PREFIX),
            "Full compacted transcript should not appear in Hive knowledge.",
            Some(repo.to_string_lossy().as_ref()),
            None,
        )
        .unwrap();

    let report_store = ReportStore::new(Database::new(&db_path).unwrap());
    report_store
        .create_report(CreateReportInput {
            title: "Wake pipeline check",
            session_id: &session_id,
            project_dir: Some(repo.to_string_lossy().as_ref()),
            report_root: Some(repo),
            content: "The wake pipeline is healthy.",
            summary: "Validated the wake pipeline end to end.",
            tags: &[],
            sources: &[],
            scope: ReportScope::owner_shared(),
        })
        .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("Check the wake pipeline health.".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        db_path.as_path(),
        &session_id,
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
    );

    let context = injected
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(context.contains("[HIVE KNOWLEDGE]"));
    assert!(context.contains("## Carry Forward"));
    assert!(context.contains("Auth decision"));
    assert!(context.contains("## Relevant Reports"));
    assert!(context.contains("Wake pipeline check"));
    assert!(!context.contains("Full compacted transcript"));
}

#[test]
fn hive_knowledge_prompt_is_exact_owner_for_alice_bob_and_local() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("hive-owner-knowledge.db");
    let project = "/shared/project";
    let db = Database::new(&db_path).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO users (id, email, license_tier)
                 VALUES ('alice', 'alice@knowledge.test', 'free');
             INSERT INTO users (id, email, license_tier)
                 VALUES ('bob', 'bob@knowledge.test', 'free');",
        )
        .unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    for (session_id, title, user_id) in [
        ("hive-local", "Local Hive", None),
        ("hive-alice", "Alice Hive", Some("alice")),
        ("hive-bob", "Bob Hive", Some("bob")),
    ] {
        db.conn()
            .execute(
                "INSERT INTO sessions (
                    id, title, created_at, updated_at, working_dir, project_dir,
                    workspace_mode, user_id, session_type
                 ) VALUES (?1, ?2, ?3, ?3, ?4, ?4, 'selected', ?5, 'hive')",
                rusqlite::params![session_id, title, now, project, user_id],
            )
            .unwrap();
    }

    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    for (title, content, user_id) in [
        ("Local queue ownership", "local-memory-marker", None),
        (
            "Alice queue ownership",
            "alice-memory-marker",
            Some("alice"),
        ),
        ("Bob queue ownership", "bob-memory-marker", Some("bob")),
    ] {
        memory_store
            .save(MemoryType::Project, title, content, Some(project), user_id)
            .unwrap();
    }

    let report_store = ReportStore::new(Database::new(&db_path).unwrap());
    for (session_id, title, summary) in [
        ("hive-local", "Local queue ownership", "local-report-marker"),
        ("hive-alice", "Alice queue ownership", "alice-report-marker"),
        ("hive-bob", "Bob queue ownership", "bob-report-marker"),
    ] {
        report_store
            .create_report(CreateReportInput {
                title,
                session_id,
                project_dir: Some(project),
                report_root: None,
                content: summary,
                summary,
                tags: &["queue".to_string(), "ownership".to_string()],
                sources: &[],
                scope: ReportScope::owner_shared(),
            })
            .unwrap();
    }

    let task_store = AutonomousTaskStore::new(Database::new(&db_path).unwrap());
    for (session_id, subject) in [
        ("hive-local", "local-task-marker"),
        ("hive-alice", "alice-task-marker"),
        ("hive-bob", "bob-task-marker"),
    ] {
        task_store
            .create_task(session_id, subject, "", &[])
            .unwrap();
    }

    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Review queue ownership evidence.".to_string(),
        )],
    }];
    let alice = build_hive_knowledge_context(
        &db_path,
        Some(project),
        Some("alice"),
        None,
        None,
        "hive-alice",
        None,
        &conversation,
    );
    let bob = build_hive_knowledge_context(
        &db_path,
        Some(project),
        Some("bob"),
        None,
        None,
        "hive-bob",
        None,
        &conversation,
    );
    let local = build_hive_knowledge_context(
        &db_path,
        Some(project),
        None,
        None,
        None,
        "hive-local",
        None,
        &conversation,
    );

    for (context, owned, foreign_a, foreign_b) in [
        (
            alice.as_str(),
            "alice-memory-marker",
            "bob-memory-marker",
            "local-memory-marker",
        ),
        (
            bob.as_str(),
            "bob-memory-marker",
            "alice-memory-marker",
            "local-memory-marker",
        ),
        (
            local.as_str(),
            "local-memory-marker",
            "alice-memory-marker",
            "bob-memory-marker",
        ),
    ] {
        assert!(context.contains(owned));
        assert!(!context.contains(foreign_a));
        assert!(!context.contains(foreign_b));
    }
    assert!(alice.contains("alice-report-marker"));
    assert!(alice.contains("alice-task-marker"));
    assert!(!alice.contains("bob-report-marker"));
    assert!(!alice.contains("bob-task-marker"));
    assert!(!alice.contains("local-report-marker"));
    assert!(!alice.contains("local-task-marker"));
    assert!(bob.contains("bob-report-marker"));
    assert!(bob.contains("bob-task-marker"));
    assert!(!bob.contains("alice-report-marker"));
    assert!(!bob.contains("alice-task-marker"));
    assert!(!bob.contains("local-report-marker"));
    assert!(!bob.contains("local-task-marker"));
    assert!(local.contains("local-report-marker"));
    assert!(local.contains("local-task-marker"));
    assert!(!local.contains("alice-report-marker"));
    assert!(!local.contains("alice-task-marker"));
    assert!(!local.contains("bob-report-marker"));
    assert!(!local.contains("bob-task-marker"));
}

#[test]
fn hive_knowledge_prompt_isolated_by_primary_and_named_crew_namespace() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("hive-crew-knowledge.db");
    let project = "/crew/project";
    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());

    for (canonical_key, title, content, namespace, namespace_id) in [
        (
            "shared-style",
            "Shared style",
            "shared-memory-marker",
            MemoryNamespace::Shared,
            None,
        ),
        (
            "primary-style",
            "Primary style",
            "primary-hive-marker",
            MemoryNamespace::Hive,
            None,
        ),
        (
            "reviewer-style",
            "Reviewer style",
            "reviewer-crew-marker",
            MemoryNamespace::Crew,
            Some("reviewer"),
        ),
        (
            "researcher-style",
            "Researcher style",
            "researcher-crew-marker",
            MemoryNamespace::Crew,
            Some("researcher"),
        ),
    ] {
        let mut input =
            CanonicalMemoryInput::new(MemoryType::Project, canonical_key, title, content);
        input.project_dir = Some(project.to_string());
        input.namespace = namespace;
        input.namespace_id = namespace_id.map(str::to_string);
        memory_store.save_canonical(&input).unwrap();
    }

    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Recall the correct working style.".to_string(),
        )],
    }];
    let primary = build_hive_knowledge_context(
        &db_path,
        Some(project),
        None,
        None,
        None,
        "hive-primary",
        None,
        &conversation,
    );
    let reviewer = build_hive_knowledge_context(
        &db_path,
        Some(project),
        None,
        Some("reviewer"),
        None,
        "hive-reviewer",
        None,
        &conversation,
    );

    assert!(primary.contains("shared-memory-marker"));
    assert!(primary.contains("primary-hive-marker"));
    assert!(!primary.contains("reviewer-crew-marker"));
    assert!(!primary.contains("researcher-crew-marker"));

    assert!(reviewer.contains("shared-memory-marker"));
    assert!(reviewer.contains("reviewer-crew-marker"));
    assert!(!reviewer.contains("primary-hive-marker"));
    assert!(!reviewer.contains("researcher-crew-marker"));
    assert!(!reviewer.contains("## Current Snapshot"));
}

#[test]
fn worker_persona_sections_replace_generic_crew_treatment() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path().join("repo");
    let hive_home = temp.path().join("hive-home");
    let crew_reviewer = hive_home.join("crew").join("reviewer");
    fs::create_dir_all(&repo).unwrap();
    fs::create_dir_all(&crew_reviewer).unwrap();
    fs::write(hive_home.join(paths::HIVE_SOUL_FILE), "Home soul.").unwrap();
    fs::write(crew_reviewer.join("IDENTITY.md"), "Crew reviewer identity.").unwrap();

    let worker_sections = vec![
        "[HIVE WORKER - reviewer]\n\nYou are Reviewer.\n\n[END HIVE WORKER]".to_string(),
        "[HIVE WORKER SOUL - reviewer]\n\nWorker soul marker.\n\n[END HIVE WORKER SOUL]"
            .to_string(),
    ];
    let bound = build_hive_context_sections_with_home(
        &repo,
        &hive_home,
        Some("reviewer"),
        &worker_sections,
    )
    .join("\n\n");

    assert!(bound.contains("Home soul."));
    assert!(bound.contains("[HIVE WORKER - reviewer]"));
    assert!(bound.contains("Worker soul marker."));
    assert!(!bound.contains("[HIVE CREW IDENTITY"));
    assert!(!bound.contains("Crew reviewer identity."));

    let unbound = build_hive_context_sections_with_home(&repo, &hive_home, Some("reviewer"), &[])
        .join("\n\n");
    assert!(unbound.contains("[HIVE CREW IDENTITY - reviewer - IDENTITY.md]"));
    assert!(unbound.contains("Crew reviewer identity."));
    assert!(!unbound.contains("[HIVE WORKER"));
}

#[test]
fn inject_context_scopes_worker_dm_to_its_own_persona_and_namespace() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    let db_path = repo.join("mitsuro.db");
    let project = repo.to_string_lossy().to_string();
    let db = Database::new(&db_path).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO sessions (id, title, created_at, updated_at, session_type)
             VALUES ('worker-dm', 'Analyst', '2026-08-01T00:00:00.000000Z',
                     '2026-08-01T00:00:00.000000Z', 'hive');
             INSERT INTO sessions (id, title, created_at, updated_at, session_type)
             VALUES ('companion', 'Hive', '2026-08-01T00:00:00.000000Z',
                     '2026-08-01T00:00:00.000000Z', 'hive');",
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO sessions (
                 id, title, created_at, updated_at, working_dir, project_dir,
                 workspace_mode, session_type
             ) VALUES ('other-chat', 'Other chat',
                 '2026-08-01T00:00:00.000000Z', '2026-08-01T00:00:00.000000Z',
                 ?1, ?1, 'selected', 'hive')",
            [&project],
        )
        .unwrap();
    let episode_content = serde_json::json!([{
        "type": "text",
        "text": "Recall the correct working style OWNER-WIDE-EPISODE-CANARY"
    }])
    .to_string();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES ('other-chat', 'user', ?1, '2026-08-01T00:00:00.000000Z')",
            [&episode_content],
        )
        .unwrap();
    let episode_message_id = db.conn().last_insert_rowid();
    EpisodeStore::new(&db)
        .record_message(
            "other-chat",
            episode_message_id,
            "user",
            &episode_content,
            "2026-08-01T00:00:00.000000Z",
        )
        .unwrap();

    let worker_store = HiveWorkerStore::new(Database::new(&db_path).unwrap());
    let worker = worker_store.create(&NewHiveWorker::new("analyst")).unwrap();
    worker_store
        .upsert_document(
            &worker.id,
            HiveWorkerDocumentKind::Identity,
            "Worker identity marker.",
        )
        .unwrap();
    worker_store
        .upsert_document(
            &worker.id,
            HiveWorkerDocumentKind::Soul,
            "Worker soul marker.",
        )
        .unwrap();
    worker_store
        .bind_dm_session(&worker.id, Some("worker-dm"))
        .unwrap();

    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    for (canonical_key, title, content, namespace, namespace_id) in [
        (
            "shared-style",
            "Shared style",
            "shared-memory-marker",
            MemoryNamespace::Shared,
            None,
        ),
        (
            "primary-style",
            "Primary style",
            "primary-hive-marker",
            MemoryNamespace::Hive,
            None,
        ),
        (
            "analyst-style",
            "Analyst style",
            "analyst-namespace-marker",
            MemoryNamespace::Crew,
            Some("analyst"),
        ),
        (
            "other-style",
            "Other style",
            "other-namespace-marker",
            MemoryNamespace::Crew,
            Some("researcher"),
        ),
    ] {
        let mut input =
            CanonicalMemoryInput::new(MemoryType::Project, canonical_key, title, content);
        input.project_dir = Some(project.clone());
        input.namespace = namespace;
        input.namespace_id = namespace_id.map(str::to_string);
        memory_store.save_canonical(&input).unwrap();
    }

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Recall the correct working style.".to_string(),
        )],
    }];
    let render = |session_id: &str| {
        inject_context(
            &conversation,
            &db_path,
            session_id,
            repo,
            Some(repo),
            WorkMode::Build,
            &skills,
            None,
            Some("hive"),
            None,
            None,
        )
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
    };

    let dm = render("worker-dm");
    assert!(dm.contains("[HIVE WORKER - analyst]"));
    assert!(dm.contains("[HIVE WORKER IDENTITY - analyst]"));
    assert!(dm.contains("Worker identity marker."));
    assert!(dm.contains("Worker soul marker."));
    assert!(dm.contains("shared-memory-marker"));
    assert!(dm.contains("analyst-namespace-marker"));
    assert!(!dm.contains("primary-hive-marker"));
    assert!(!dm.contains("other-namespace-marker"));
    assert!(
        !dm.contains("OWNER-WIDE-EPISODE-CANARY"),
        "Worker DMs must not search raw episodes from other owned conversations"
    );

    // A hive session without a Worker binding keeps the primary companion
    // treatment: no Worker persona, primary namespace memories intact.
    let companion = render("companion");
    assert!(!companion.contains("[HIVE WORKER"));
    assert!(companion.contains("shared-memory-marker"));
    assert!(companion.contains("primary-hive-marker"));
    assert!(!companion.contains("analyst-namespace-marker"));
    assert!(
        companion.contains("OWNER-WIDE-EPISODE-CANARY"),
        "the primary companion keeps bounded owner-wide episodic continuity"
    );
}

#[test]
fn worker_persona_requires_matching_session_owner() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("mitsuro.db");
    let db = Database::new(&db_path).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO users (id, email, license_tier)
             VALUES ('alice', 'alice@example.com', 'free');
             INSERT INTO sessions (id, title, created_at, updated_at, session_type, user_id)
             VALUES ('alice-dm', 'Analyst', '2026-08-01T00:00:00.000000Z',
                     '2026-08-01T00:00:00.000000Z', 'hive', 'alice');",
        )
        .unwrap();
    let worker_store = HiveWorkerStore::new(Database::new(&db_path).unwrap());
    let worker = worker_store
        .create(&NewHiveWorker {
            user_id: Some("alice".into()),
            ..NewHiveWorker::new("analyst")
        })
        .unwrap();
    worker_store
        .bind_dm_session(&worker.id, Some("alice-dm"))
        .unwrap();

    let owned = load_worker_persona(&db_path, "alice-dm", Some("alice"))
        .expect("owner resolves their worker persona");
    assert_eq!(owned.memory_namespace_id, "analyst");
    assert!(owned
        .sections
        .iter()
        .any(|section| section.starts_with("[HIVE WORKER - analyst]")));

    assert!(load_worker_persona(&db_path, "alice-dm", None).is_none());
    assert!(load_worker_persona(&db_path, "alice-dm", Some("bob")).is_none());
    assert!(load_worker_persona(&db_path, "unbound-session", Some("alice")).is_none());
}

#[test]
fn neutral_worker_context_has_no_workspace_or_global_hive_fallback() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("mitsuro.db");
    fs::write(temp.path().join("AGENTS.md"), "WORKSPACE-CONTEXT-CANARY").unwrap();
    fs::write(temp.path().join("HIVE.md"), "GLOBAL-HIVE-CANARY").unwrap();
    let db = Database::new(&db_path).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO sessions (id, title, created_at, updated_at, session_type)
             VALUES ('worker-dm', 'Analyst', '2026-08-01T00:00:00.000000Z',
                     '2026-08-01T00:00:00.000000Z', 'hive');",
        )
        .unwrap();
    let worker_store = HiveWorkerStore::new(Database::new(&db_path).unwrap());
    let worker = worker_store.create(&NewHiveWorker::new("analyst")).unwrap();
    worker_store
        .upsert_document(
            &worker.id,
            HiveWorkerDocumentKind::Identity,
            "EXACT-WORKER-PERSONA",
        )
        .unwrap();
    worker_store
        .bind_dm_session(&worker.id, Some("worker-dm"))
        .unwrap();
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("Who are you?".into())],
    }];

    let injected = inject_worker_conversation_context(
        &conversation,
        &db_path,
        "worker-dm",
        &worker.id,
        None,
        None,
    )
    .unwrap();
    let rendered = injected
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("[WORKER CONVERSATION CAPABILITY]"));
    assert!(rendered.contains("EXACT-WORKER-PERSONA"));
    for denied in [
        "WORKSPACE-CONTEXT-CANARY",
        "GLOBAL-HIVE-CANARY",
        "[HIVE COORDINATOR]",
        "[DELEGATION MODE:",
        "[AVAILABLE SKILLS]",
        "[ACTIVE PLAN",
    ] {
        assert!(!rendered.contains(denied), "leaked {denied}");
    }
    assert!(inject_worker_conversation_context(
        &conversation,
        &db_path,
        "worker-dm",
        "different-worker",
        None,
        None,
    )
    .is_err());
}

#[test]
fn worker_goal_context_isolates_workflow_worker_knowledge_and_ephemeral_trigger() {
    let temp = TempDir::new().unwrap();
    let workspace = temp.path().canonicalize().unwrap();
    let project = workspace.to_string_lossy().to_string();
    let db_path = workspace.join("mitsuro.db");
    fs::write(
        workspace.join("AGENTS.md"),
        "PROJECT-INSTRUCTION-LEAK-CANARY",
    )
    .unwrap();
    fs::write(workspace.join("HIVE.md"), "GLOBAL-HIVE-LEAK-CANARY").unwrap();
    let db = Database::new(&db_path).unwrap();
    for (id, title, session_type) in [
        ("worker-goal-session", "Worker Goal", "hive"),
        ("worker-goal-group-lane", "Hidden group lane", "hive"),
        ("other-worker-dm", "Other Worker", "hive"),
        ("ordinary-chat", "Ordinary Chat", "chat"),
    ] {
        db.conn()
            .execute(
                "INSERT INTO sessions (
                     id, title, created_at, updated_at, working_dir, project_dir,
                     workspace_mode, session_type
                 ) VALUES (?1, ?2,
                     '2026-08-25T00:00:00.000000Z', '2026-08-25T00:00:00.000000Z',
                     ?3, ?3, 'selected', ?4)",
                rusqlite::params![id, title, project, session_type],
            )
            .unwrap();
    }
    let worker_store = HiveWorkerStore::new(Database::new(&db_path).unwrap());
    let worker = worker_store
        .create(&NewHiveWorker {
            dm_session_id: Some("worker-goal-session".into()),
            memory_namespace_id: Some("goal-worker-private".into()),
            ..NewHiveWorker::new("goal-worker")
        })
        .unwrap();
    let other = worker_store
        .create(&NewHiveWorker {
            dm_session_id: Some("other-worker-dm".into()),
            memory_namespace_id: Some("other-worker-private".into()),
            ..NewHiveWorker::new("other-worker")
        })
        .unwrap();
    worker_store
        .upsert_document(
            &worker.id,
            HiveWorkerDocumentKind::Identity,
            "EXACT-WORKER-GOAL-PERSONA",
        )
        .unwrap();
    let group = HiveGroupStore::new(Database::new(&db_path).unwrap())
        .create(&NewHiveGroup {
            title: "Hidden room".into(),
            member_worker_ids: vec![worker.id.clone(), other.id.clone()],
            ..NewHiveGroup::default()
        })
        .unwrap();
    HiveGroupWorkerLaneStore::new(Database::new(&db_path).unwrap())
        .upsert(&NewHiveGroupWorkerLane::new(
            group.id,
            worker.id.clone(),
            "worker-goal-group-lane",
        ))
        .unwrap();
    worker_store
        .upsert_document(
            &other.id,
            HiveWorkerDocumentKind::Identity,
            "OTHER-WORKER-PERSONA-LEAK-CANARY",
        )
        .unwrap();

    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    for (key, marker, namespace, namespace_id, acl_scope, conversation_id) in [
        (
            "goal-shared",
            "SHARED-GOAL-MEMORY-LEAK-CANARY",
            MemoryNamespace::Shared,
            None,
            MemoryAclScope::Owner,
            None,
        ),
        (
            "goal-private",
            "EXACT-WORKER-PRIVATE-MEMORY",
            MemoryNamespace::Crew,
            Some("goal-worker-private"),
            MemoryAclScope::Worker,
            None,
        ),
        (
            "other-private",
            "OTHER-WORKER-MEMORY-LEAK-CANARY",
            MemoryNamespace::Crew,
            Some("other-worker-private"),
            MemoryAclScope::Worker,
            None,
        ),
        (
            "primary-hive",
            "PRIMARY-HIVE-MEMORY-LEAK-CANARY",
            MemoryNamespace::Hive,
            None,
            MemoryAclScope::Owner,
            None,
        ),
        (
            "goal-conversation",
            "WORKER-DM-CONVERSATION-MEMORY-LEAK-CANARY",
            MemoryNamespace::Shared,
            None,
            MemoryAclScope::Conversation,
            Some("worker-goal-session"),
        ),
    ] {
        let mut input =
            CanonicalMemoryInput::new(MemoryType::Project, key, "Boundary context memory", marker);
        input.project_dir = Some(project.clone());
        input.namespace = namespace;
        input.namespace_id = namespace_id.map(str::to_string);
        input.acl_scope = acl_scope;
        input.conversation_id = conversation_id.map(str::to_string);
        memory_store.save_canonical(&input).unwrap();
    }

    let report_store = ReportStore::new(Database::new(&db_path).unwrap());
    for (session_id, marker, scope) in [
        (
            "worker-goal-session",
            "EXACT-WORKER-PRIVATE-REPORT",
            ReportScope::worker_private(worker.id.clone(), worker.memory_namespace_id.clone())
                .unwrap(),
        ),
        (
            "other-worker-dm",
            "OTHER-WORKER-REPORT-LEAK-CANARY",
            ReportScope::worker_private(other.id, other.memory_namespace_id).unwrap(),
        ),
        (
            "ordinary-chat",
            "ORDINARY-CHAT-REPORT-LEAK-CANARY",
            ReportScope::owner_shared(),
        ),
    ] {
        report_store
            .create_report(CreateReportInput {
                title: "Boundary context evidence",
                session_id,
                project_dir: Some(&project),
                report_root: None,
                content: marker,
                summary: marker,
                tags: &["boundary".into(), "context".into()],
                sources: &[],
                scope,
            })
            .unwrap();
    }

    let ordinary_content = serde_json::json!([{
        "type": "text",
        "text": "ORDINARY-CHAT-EPISODE-LEAK-CANARY boundary context"
    }])
    .to_string();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES ('ordinary-chat', 'user', ?1, '2026-08-25T00:00:00.000000Z')",
            [&ordinary_content],
        )
        .unwrap();
    EpisodeStore::new(&db)
        .record_message(
            "ordinary-chat",
            db.conn().last_insert_rowid(),
            "user",
            &ordinary_content,
            "2026-08-25T00:00:00.000000Z",
        )
        .unwrap();

    let context = test_worker_goal_context(&workspace, &worker.id, worker.revision);
    let ephemeral_trigger = context.ephemeral_trigger_message();
    let conversation = vec![ephemeral_trigger];
    let injected = inject_worker_goal_context(
        &conversation,
        &db_path,
        "worker-goal-session",
        None,
        &context,
        &HashSet::from(["read".to_string(), "apply_patch".to_string()]),
    )
    .unwrap();
    let rendered = injected
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let workspace_text = workspace.to_string_lossy().to_string();

    for required in [
        "[WORKER GOAL CAPABILITY]",
        "[CANONICAL WORKER GOAL ATTEMPT]",
        "goal-context-1",
        "attempt-context-1",
        "step-context-1",
        "EXACT-WORKER-GOAL-PERSONA",
        "EXACT-WORKER-PRIVATE-MEMORY",
        "EXACT-WORKER-PRIVATE-REPORT",
        workspace_text.as_str(),
    ] {
        assert!(rendered.contains(required), "missing {required}");
    }
    for denied in [
        "PROJECT-INSTRUCTION-LEAK-CANARY",
        "GLOBAL-HIVE-LEAK-CANARY",
        "OTHER-WORKER-PERSONA-LEAK-CANARY",
        "OTHER-WORKER-MEMORY-LEAK-CANARY",
        "OTHER-WORKER-REPORT-LEAK-CANARY",
        "SHARED-GOAL-MEMORY-LEAK-CANARY",
        "PRIMARY-HIVE-MEMORY-LEAK-CANARY",
        "WORKER-DM-CONVERSATION-MEMORY-LEAK-CANARY",
        "ORDINARY-CHAT-REPORT-LEAK-CANARY",
        "ORDINARY-CHAT-EPISODE-LEAK-CANARY",
        "[GROUP ROOM]",
        "[AVAILABLE SKILLS]",
        "tool_search",
    ] {
        assert!(!rendered.contains(denied), "leaked {denied}");
    }
    assert_eq!(
        injected
            .iter()
            .filter(|message| message.role == Role::User)
            .count(),
        1,
        "context injection must not invent a persisted-looking user turn"
    );
    assert!(matches!(
        injected.last(),
        Some(ModelMessage {
            role: Role::User,
            content,
        }) if matches!(
            content.as_slice(),
            [Content::Text { text, .. }]
                if text.contains("[WORKER GOAL TRIGGER v1]")
                    && text.contains("goal-context-1")
                    && text.contains("attempt-context-1")
        )
    ));

    let stale_context = test_worker_goal_context(&workspace, &worker.id, worker.revision + 1);
    assert!(matches!(
        inject_worker_goal_context(
            &conversation,
            &db_path,
            "worker-goal-session",
            None,
            &stale_context,
            &HashSet::from(["read".to_string()]),
        ),
        Err(super::WorkerConversationContextError::WorkerRevisionMismatch { .. })
    ));
    assert!(matches!(
        inject_worker_goal_context(
            &conversation,
            &db_path,
            "worker-goal-group-lane",
            None,
            &context,
            &HashSet::from(["read".to_string()]),
        ),
        Err(super::WorkerConversationContextError::DeniedBinding)
    ));
}

#[test]
fn inject_context_prioritizes_relevant_reports_over_recent_reports() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();

    let db_path = repo.join("mitsuro.db");
    let session_manager = SessionManager::new(Database::new(&db_path).unwrap());
    let session_id = session_manager
        .create_session("test", None, Some(repo.to_string_lossy().as_ref()))
        .unwrap();

    let report_store = ReportStore::new(Database::new(&db_path).unwrap());
    report_store
        .create_report(CreateReportInput {
            title: "Queue Scheduling Audit",
            session_id: &session_id,
            project_dir: Some(repo.to_string_lossy().as_ref()),
            report_root: Some(repo),
            content: "Investigated overdue runs and wake cadence.",
            summary: "Queue scheduling and overdue run analysis.",
            tags: &["queue".into(), "scheduling".into()],
            sources: &[],
            scope: ReportScope::owner_shared(),
        })
        .unwrap();
    for index in 0..5 {
        report_store
            .create_report(CreateReportInput {
                title: &format!("Unrelated report {index}"),
                session_id: &session_id,
                project_dir: Some(repo.to_string_lossy().as_ref()),
                report_root: Some(repo),
                content: "Miscellaneous notes.",
                summary: "General project notes.",
                tags: &["misc".into()],
                sources: &[],
                scope: ReportScope::owner_shared(),
            })
            .unwrap();
    }

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Please stabilize queue scheduling and overdue runs.".to_string(),
        )],
    }];

    let injected = inject_context(
        &conversation,
        db_path.as_path(),
        &session_id,
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );

    assert!(injected.iter().any(|message| {
        matches!(
            &message.content[0],
            Content::Text { text, .. }
                if text.contains("[RELEVANT REPORTS]")
                    && text.contains("Queue Scheduling Audit")
        )
    }));

    let joined = injected
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!joined.contains("Unrelated report"));
}

#[test]
fn inject_context_omits_reports_when_none_are_relevant() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    let db_path = repo.join("mitsuro.db");
    let session_manager = SessionManager::new(Database::new(&db_path).unwrap());
    let session_id = session_manager
        .create_session("test", None, Some(repo.to_string_lossy().as_ref()))
        .unwrap();
    ReportStore::new(Database::new(&db_path).unwrap())
        .create_report(CreateReportInput {
            title: "Database migration retrospective",
            session_id: &session_id,
            project_dir: Some(repo.to_string_lossy().as_ref()),
            report_root: Some(repo),
            content: "Postgres migration notes.",
            summary: "Schema rollout findings.",
            tags: &["database".into()],
            sources: &[],
            scope: ReportScope::owner_shared(),
        })
        .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Tune the mobile animation easing.".to_string(),
        )],
    }];
    let injected = inject_context(
        &conversation,
        &db_path,
        &session_id,
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );

    assert!(injected.iter().all(|message| {
        message.content.iter().all(|content| {
            !matches!(content, Content::Text { text, .. } if text.contains("[RELEVANT REPORTS]"))
        })
    }));
}

#[test]
fn inject_context_uses_active_hive_tasks_for_report_relevance() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();

    let db_path = repo.join("mitsuro.db");
    let session_manager = SessionManager::new(Database::new(&db_path).unwrap());
    let session_id = session_manager
        .create_session("test", None, Some(repo.to_string_lossy().as_ref()))
        .unwrap();

    let report_store = ReportStore::new(Database::new(&db_path).unwrap());
    report_store
        .create_report(CreateReportInput {
            title: "Scheduler Drift Runbook",
            session_id: &session_id,
            project_dir: Some(repo.to_string_lossy().as_ref()),
            report_root: Some(repo),
            content: "Drift handling and wake diagnostics.",
            summary: "How to investigate scheduler drift safely.",
            tags: &["scheduler".into(), "drift".into()],
            sources: &[],
            scope: ReportScope::owner_shared(),
        })
        .unwrap();
    for index in 0..5 {
        report_store
            .create_report(CreateReportInput {
                title: &format!("Background note {index}"),
                session_id: &session_id,
                project_dir: Some(repo.to_string_lossy().as_ref()),
                report_root: Some(repo),
                content: "Background knowledge.",
                summary: "General notes.",
                tags: &["misc".into()],
                sources: &[],
                scope: ReportScope::owner_shared(),
            })
            .unwrap();
    }

    let task_store = AutonomousTaskStore::new(Database::new(&db_path).unwrap());
    task_store
        .create_task(&session_id, "Investigate scheduler drift", "", &[])
        .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("Keep watch and continue.".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        db_path.as_path(),
        &session_id,
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
    );

    assert!(injected.iter().any(|message| {
        matches!(
            &message.content[0],
            Content::Text { text, .. }
                if text.contains("[HIVE KNOWLEDGE]")
                    && text.contains("## Relevant Reports")
                    && text.contains("Scheduler Drift Runbook")
        )
    }));
}

#[test]
fn inject_context_does_not_duplicate_generic_memory_and_report_blocks_for_hive() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::write(repo.join("HIVE.md"), "Always Swimming.").unwrap();

    let db_path = repo.join("mitsuro.db");
    let session_manager = SessionManager::new(Database::new(&db_path).unwrap());
    let session_id = session_manager
        .create_session("test", None, Some(repo.to_string_lossy().as_ref()))
        .unwrap();

    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    memory_store
        .save(
            MemoryType::Feedback,
            "Status preference",
            "Show upcoming wakes before aggregate counters.",
            Some(repo.to_string_lossy().as_ref()),
            None,
        )
        .unwrap();

    let report_store = ReportStore::new(Database::new(&db_path).unwrap());
    report_store
        .create_report(CreateReportInput {
            title: "Queue audit",
            session_id: &session_id,
            project_dir: Some(repo.to_string_lossy().as_ref()),
            report_root: Some(repo),
            content: "Queue ordering is stable.",
            summary: "Queue ordering remains stable.",
            tags: &[],
            sources: &[],
            scope: ReportScope::owner_shared(),
        })
        .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("hello".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        db_path.as_path(),
        &session_id,
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
    );

    let texts = injected
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(texts.iter().any(|text| text.contains("[HIVE KNOWLEDGE]")));
    assert!(!texts
        .iter()
        .any(|text| text.contains("[PERSISTENT MEMORY]")));
    assert!(!texts.iter().any(|text| text.contains("[RECENT REPORTS]")));
}

#[test]
fn inject_context_includes_recent_delegated_run_guidance() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    let db_path = repo.join("mitsuro.db");
    let db = Database::new(&db_path).unwrap();
    let session_manager = SessionManager::new(Database::new(&db_path).unwrap());
    let session_id = session_manager
        .create_session("test", None, Some(repo.to_string_lossy().as_ref()))
        .unwrap();
    let store = DelegatedRunStore::new(db);
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-1".to_string(),
            parent_session_id: session_id.clone(),
            parent_tool_call_id: Some("tool-1".to_string()),
            role: DelegatedRunRole::Explore,
            stage: DelegatedRunStage::Created,
            provider: Some("MiniMax".to_string()),
            model: Some("MiniMax-M2.5".to_string()),
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![DelegatedRunScope {
                label: "src/storage".to_string(),
                path: "crates/mitsuro-core/src/storage".to_string(),
                kind: "directory".to_string(),
            }],
        })
        .unwrap();
    store
        .finalize_run(
            "run-1",
            DelegatedRunStage::Complete,
            &serde_json::json!({"outcome":"success"}),
            Some("Architecture review completed across 1 targets."),
            true,
        )
        .unwrap();

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Review the existing delegated findings".to_string(),
        )],
    }];

    let injected = inject_context(
        &conversation,
        db_path.as_path(),
        &session_id,
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        None,
        None,
        None,
    );

    let guidance = injected
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|content| match content {
            Content::Text { text, .. } if text.contains("[RECENT DELEGATED RUNS]") => Some(text),
            _ => None,
        })
        .expect("recent delegated evidence should be included");
    for expected in [
        "run-1",
        "src/storage",
        "Architecture review completed across 1 targets.",
        "Reuse relevant completed evidence",
        "`agent` tool's status action reads terminal evidence",
        "do not poll running children",
        "message action steers live work",
        "followup/resume starts new work",
        "ExplicitOnly's user-request requirement",
        "Continue useful independent work directly",
        "Prior delegation does not require delegating again",
    ] {
        assert!(guidance.contains(expected), "missing guidance: {expected}");
    }
    assert!(!guidance.contains("prefer calling `explore` again"));
    assert!(!guidance.contains("Only fall back to manual probing"));
}

#[test]
fn inject_context_adds_orchestrator_contract_for_code_sessions() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("hello".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );

    assert!(injected.iter().any(|message| {
        matches!(
            &message.content[0],
            Content::Text { text, .. }
                if text.contains("[DELEGATION MODE: ORCHESTRATOR]")
                    && text.contains("coordinate through focused agents early")
                    && text.contains("do not delegate trivial actions")
        )
    }));
}

#[test]
fn inject_context_honors_explicit_only_delegation_setting() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    fs::create_dir_all(repo.join(".git")).unwrap();
    fs::create_dir_all(repo.join(".mitsuro")).unwrap();
    fs::write(
        repo.join(".mitsuro").join("settings.json"),
        r#"{ "delegation_mode": "explicit_only" }"#,
    )
    .unwrap();
    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text("hello".to_string())],
    }];

    let injected = inject_context(
        &conversation,
        repo.join("mitsuro.db").as_path(),
        "session-id",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );

    assert!(injected.iter().any(|message| {
        matches!(
            &message.content[0],
            Content::Text { text, .. }
                if text.contains("[DELEGATION MODE: EXPLICIT_ONLY]")
                    && text.contains("only when the user explicitly requests")
        )
    }));
}

#[test]
fn group_room_section_carries_roster_timeline_and_posting_contract() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("group-context.db");
    let worker_store = HiveWorkerStore::new(Database::new(&db_path).unwrap());
    let researcher = worker_store
        .create(&NewHiveWorker {
            display_name: Some("Deep Researcher".into()),
            model: Some("grok-code-fast-1".into()),
            ..NewHiveWorker::new("researcher")
        })
        .unwrap();
    let builder = worker_store.create(&NewHiveWorker::new("builder")).unwrap();
    let group_store = HiveGroupStore::new(Database::new(&db_path).unwrap());
    let group = group_store
        .create(&NewHiveGroup {
            user_id: None,
            title: "Release Room".into(),
            member_worker_ids: vec![researcher.id.clone(), builder.id.clone()],
            ..NewHiveGroup::default()
        })
        .unwrap();
    group_store
        .append_message(&NewHiveGroupMessage::user(&group.id, "ship the release"))
        .unwrap();
    group_store
        .append_message(&NewHiveGroupMessage::worker(
            &group.id,
            &researcher.id,
            "auditing the diff now",
        ))
        .unwrap();

    let section = build_group_room_section(
        &db_path,
        &HiveGroupRunContext {
            group_id: group.id.clone(),
            group_turn_id: "turn-1".into(),
            run_id: "run-1".into(),
            worker_id: builder.id.clone(),
            max_member_messages_per_turn: 2,
            context_window_messages: 24,
        },
    )
    .expect("group room section should build");

    assert!(section.starts_with("[GROUP ROOM - Release Room]"));
    assert!(section.contains("@researcher (Deep Researcher, grok"));
    assert!(section.contains("<- you"));
    assert!(section.contains("post_to_group"));
    assert!(section.contains("at most 2 message(s)"));
    assert!(section.contains("ship the release"));
    assert!(section.contains("auditing the diff now"));
    assert!(section.ends_with("[END GROUP ROOM]"));

    // The context window bounds how much room history is replayed.
    let bounded = build_group_room_section(
        &db_path,
        &HiveGroupRunContext {
            group_id: group.id,
            group_turn_id: "turn-1".into(),
            run_id: "run-1".into(),
            worker_id: builder.id,
            max_member_messages_per_turn: 2,
            context_window_messages: 1,
        },
    )
    .unwrap();
    assert!(!bounded.contains("ship the release"));
    assert!(bounded.contains("auditing the diff now"));

    // An unknown group degrades to no section instead of failing the run.
    assert!(build_group_room_section(
        &db_path,
        &HiveGroupRunContext {
            group_id: "missing".into(),
            group_turn_id: "turn-1".into(),
            run_id: "run-1".into(),
            worker_id: "nobody".into(),
            max_member_messages_per_turn: 2,
            context_window_messages: 24,
        },
    )
    .is_none());
}

#[test]
fn group_member_run_isolates_worker_private_memories() {
    let temp = TempDir::new().unwrap();
    let repo = temp.path();
    let db_path = repo.join("mitsuro.db");
    let project = repo.to_string_lossy().to_string();
    let db = Database::new(&db_path).unwrap();
    for (id, title, session_type) in [
        ("group-run-a", "Researcher group lane", "hive"),
        ("researcher-dm", "Researcher DM", "hive"),
        ("builder-dm", "Builder DM", "hive"),
        ("primary-hive", "Primary Hive", "hive"),
        ("ordinary-hive-history", "Ordinary Hive history", "hive"),
        ("ordinary-chat", "Ordinary Chat", "chat"),
        ("ordinary-code", "Ordinary Code", "code"),
    ] {
        db.conn()
            .execute(
                "INSERT INTO sessions (
                     id, title, created_at, updated_at, working_dir, project_dir,
                     workspace_mode, session_type
                 ) VALUES (?1, ?2,
                     '2026-08-16T00:00:00.000000Z', '2026-08-16T00:00:00.000000Z',
                     ?3, ?3, 'selected', ?4)",
                rusqlite::params![id, title, project, session_type],
            )
            .unwrap();
    }
    let episode_content = serde_json::json!([{
        "type": "text",
        "text": "Recall the correct working style GROUP-EPISODE-LEAK-CANARY"
    }])
    .to_string();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES ('ordinary-hive-history', 'user', ?1,
                     '2026-08-16T00:00:00.000000Z')",
            [&episode_content],
        )
        .unwrap();
    EpisodeStore::new(&db)
        .record_message(
            "ordinary-hive-history",
            db.conn().last_insert_rowid(),
            "user",
            &episode_content,
            "2026-08-16T00:00:00.000000Z",
        )
        .unwrap();

    let worker_store = HiveWorkerStore::new(Database::new(&db_path).unwrap());
    let researcher = worker_store
        .create(&NewHiveWorker {
            dm_session_id: Some("researcher-dm".into()),
            memory_namespace_id: Some("stable-researcher".into()),
            ..NewHiveWorker::new("researcher")
        })
        .unwrap();
    let builder = worker_store
        .create(&NewHiveWorker {
            dm_session_id: Some("builder-dm".into()),
            memory_namespace_id: Some("stable-builder".into()),
            ..NewHiveWorker::new("builder")
        })
        .unwrap();
    worker_store
        .upsert_document(
            &researcher.id,
            HiveWorkerDocumentKind::Identity,
            "GROUP-WORKER-PERSONA-MARKER",
        )
        .unwrap();
    let group_store = HiveGroupStore::new(Database::new(&db_path).unwrap());
    let group = group_store
        .create(&NewHiveGroup {
            title: "Release Room".into(),
            member_worker_ids: vec![researcher.id.clone(), builder.id.clone()],
            ..NewHiveGroup::default()
        })
        .unwrap();
    HiveGroupWorkerLaneStore::new(Database::new(&db_path).unwrap())
        .upsert(&NewHiveGroupWorkerLane::new(
            group.id.clone(),
            researcher.id.clone(),
            "group-run-a",
        ))
        .unwrap();

    let memory_store = MemoryStore::new(Database::new(&db_path).unwrap());
    for (canonical_key, title, content, namespace, namespace_id) in [
        (
            "shared-style",
            "Shared working style",
            "shared-memory-marker working style",
            MemoryNamespace::Shared,
            None,
        ),
        (
            "primary-style",
            "Primary Hive working style",
            "primary-hive-marker working style",
            MemoryNamespace::Hive,
            None,
        ),
        (
            "researcher-private",
            "Researcher working style",
            "researcher-private-marker working style",
            MemoryNamespace::Crew,
            Some("stable-researcher"),
        ),
        (
            "builder-private",
            "Builder working style",
            "builder-private-marker working style",
            MemoryNamespace::Crew,
            Some("stable-builder"),
        ),
    ] {
        let mut input =
            CanonicalMemoryInput::new(MemoryType::Project, canonical_key, title, content);
        input.namespace = namespace;
        input.namespace_id = namespace_id.map(str::to_string);
        memory_store.save_canonical(&input).unwrap();
    }

    let mut group_shared = CanonicalMemoryInput::new(
        MemoryType::Project,
        "group-shared",
        "Group shared",
        "group-shared-marker",
    );
    group_shared.project_dir = Some(project.clone());
    group_shared.acl_scope = crate::storage::MemoryAclScope::Group;
    group_shared.conversation_id = Some(group.id.clone());
    memory_store.save_canonical(&group_shared).unwrap();

    for (session_id, marker) in [
        ("researcher-dm", "WORKER-A-DM-EPISODE-CANARY"),
        ("group-run-a", "WORKER-A-GROUP-EPISODE-CANARY"),
        ("builder-dm", "WORKER-B-DM-EPISODE-CANARY"),
    ] {
        let content = serde_json::json!([{
            "type": "text",
            "text": format!("Recall the correct working style {marker}")
        }])
        .to_string();
        db.conn()
            .execute(
                "INSERT INTO messages (session_id, role, content, created_at)
                 VALUES (?1, 'user', ?2, '2026-08-16T00:00:00.000000Z')",
                rusqlite::params![session_id, content],
            )
            .unwrap();
        EpisodeStore::new(&db)
            .record_message(
                session_id,
                db.conn().last_insert_rowid(),
                "user",
                &content,
                "2026-08-16T00:00:00.000000Z",
            )
            .unwrap();
    }

    let report_store = ReportStore::new(Database::new(&db_path).unwrap());
    for (session_id, title, marker) in [
        (
            "researcher-dm",
            "Researcher DM working style",
            "REPORT-A-DM",
        ),
        (
            "group-run-a",
            "Researcher group working style",
            "REPORT-A-GROUP",
        ),
        ("builder-dm", "Builder working style", "REPORT-B-DM"),
        ("primary-hive", "Primary working style", "REPORT-PRIMARY"),
    ] {
        let scope = match session_id {
            "researcher-dm" | "group-run-a" => ReportScope::worker_private(
                researcher.id.clone(),
                researcher.memory_namespace_id.clone(),
            )
            .unwrap(),
            "builder-dm" => {
                ReportScope::worker_private(builder.id.clone(), builder.memory_namespace_id.clone())
                    .unwrap()
            }
            _ => ReportScope::owner_shared(),
        };
        report_store
            .create_report(CreateReportInput {
                title,
                session_id,
                project_dir: Some(&project),
                report_root: None,
                content: marker,
                summary: marker,
                tags: &["working-style".into()],
                sources: &[],
                scope,
            })
            .unwrap();
    }

    let skills = RwLock::new(SkillsManager::with_defaults(repo));
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(
            "Recall the correct working style.".to_string(),
        )],
    }];
    let group_run = HiveGroupRunContext {
        group_id: group.id,
        group_turn_id: "turn-1".into(),
        run_id: "run-a".into(),
        worker_id: researcher.id,
        max_member_messages_per_turn: 1,
        context_window_messages: 24,
    };
    let injected = inject_context_with_hive_profile_and_group(
        &conversation,
        &db_path,
        "group-run-a",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
        None,
        Some(&group_run),
    );
    let context = injected
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    assert!(context.contains("[GROUP ROOM - Release Room]"));
    assert!(context.contains("[HIVE WORKER - researcher]"));
    assert!(context.contains("GROUP-WORKER-PERSONA-MARKER"));
    assert!(context.contains("private working lane for a group room"));
    assert!(
        !context.contains("GROUP-EPISODE-LEAK-CANARY"),
        "group Worker runs must not search owner-wide conversation episodes"
    );
    assert!(context.contains("shared-memory-marker"));
    assert!(context.contains("researcher-private-marker"));
    assert!(context.contains("group-shared-marker"));
    assert!(context.contains("REPORT-A-DM"));
    assert!(context.contains("REPORT-A-GROUP"));
    assert!(context.contains("REPORT-PRIMARY"));
    assert!(context.contains("WORKER-A-DM-EPISODE-CANARY"));
    assert!(!context.contains("WORKER-A-GROUP-EPISODE-CANARY"));
    assert!(!context.contains("WORKER-B-DM-EPISODE-CANARY"));
    assert!(
        !context.contains("builder-private-marker"),
        "a group member run must not inherit another Worker's private memories"
    );
    assert!(!context.contains("REPORT-B-DM"));

    let render_direct = |session_id: &str, session_type: &str| {
        inject_context(
            &conversation,
            &db_path,
            session_id,
            repo,
            Some(repo),
            WorkMode::Build,
            &skills,
            None,
            Some(session_type),
            None,
            None,
        )
        .iter()
        .filter_map(|message| match &message.content[0] {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
    };

    let researcher_dm = render_direct("researcher-dm", "hive");
    assert!(researcher_dm.contains("[HIVE WORKER - researcher]"));
    assert!(researcher_dm.contains("shared-memory-marker"));
    assert!(researcher_dm.contains("researcher-private-marker"));
    assert!(researcher_dm.contains("REPORT-A-DM"));
    assert!(researcher_dm.contains("REPORT-A-GROUP"));
    assert!(researcher_dm.contains("WORKER-A-GROUP-EPISODE-CANARY"));
    assert!(!researcher_dm.contains("WORKER-A-DM-EPISODE-CANARY"));
    assert!(!researcher_dm.contains("WORKER-B-DM-EPISODE-CANARY"));
    assert!(!researcher_dm.contains("builder-private-marker"));
    assert!(!researcher_dm.contains("group-shared-marker"));
    assert!(!researcher_dm.contains("REPORT-B-DM"));

    let builder_dm = render_direct("builder-dm", "hive");
    assert!(builder_dm.contains("[HIVE WORKER - builder]"));
    assert!(builder_dm.contains("shared-memory-marker"));
    assert!(builder_dm.contains("builder-private-marker"));
    assert!(builder_dm.contains("REPORT-B-DM"));
    assert!(!builder_dm.contains("WORKER-A-DM-EPISODE-CANARY"));
    assert!(!builder_dm.contains("WORKER-A-GROUP-EPISODE-CANARY"));
    assert!(!builder_dm.contains("researcher-private-marker"));
    assert!(!builder_dm.contains("REPORT-A-DM"));
    assert!(!builder_dm.contains("REPORT-A-GROUP"));

    let primary = render_direct("primary-hive", "hive");
    assert!(primary.contains("shared-memory-marker"));
    assert!(primary.contains("primary-hive-marker"));
    assert!(primary.contains("REPORT-PRIMARY"));
    assert!(primary.contains("GROUP-EPISODE-LEAK-CANARY"));
    for private_canary in [
        "researcher-private-marker",
        "builder-private-marker",
        "group-shared-marker",
        "REPORT-A-DM",
        "REPORT-A-GROUP",
        "REPORT-B-DM",
        "WORKER-A-DM-EPISODE-CANARY",
        "WORKER-A-GROUP-EPISODE-CANARY",
        "WORKER-B-DM-EPISODE-CANARY",
    ] {
        assert!(
            !primary.contains(private_canary),
            "primary Hive leaked Worker-private canary {private_canary}"
        );
    }

    for ordinary in [
        render_direct("ordinary-chat", "chat"),
        render_direct("ordinary-code", "code"),
    ] {
        assert!(ordinary.contains("shared-memory-marker"));
        for private_canary in [
            "primary-hive-marker",
            "researcher-private-marker",
            "builder-private-marker",
            "group-shared-marker",
            "REPORT-A-DM",
            "REPORT-A-GROUP",
            "REPORT-B-DM",
            "WORKER-A-DM-EPISODE-CANARY",
            "WORKER-A-GROUP-EPISODE-CANARY",
            "WORKER-B-DM-EPISODE-CANARY",
        ] {
            assert!(
                !ordinary.contains(private_canary),
                "ordinary session leaked private canary {private_canary}"
            );
        }
    }

    let unresolved_group = inject_context_with_hive_profile_and_group(
        &conversation,
        &db_path,
        "ordinary-hive-history",
        repo,
        Some(repo),
        WorkMode::Build,
        &skills,
        None,
        Some("hive"),
        None,
        None,
        None,
        Some(&group_run),
    )
    .iter()
    .filter_map(|message| match &message.content[0] {
        Content::Text { text, .. } => Some(text.as_str()),
        _ => None,
    })
    .collect::<Vec<_>>()
    .join("\n\n");
    for denied_canary in [
        "[HIVE WORKER - researcher]",
        "[GROUP ROOM - Release Room]",
        "shared-memory-marker",
        "primary-hive-marker",
        "researcher-private-marker",
        "group-shared-marker",
        "REPORT-A-DM",
        "REPORT-PRIMARY",
    ] {
        assert!(
            !unresolved_group.contains(denied_canary),
            "unresolved group binding failed open on {denied_canary}"
        );
    }
}

#[test]
fn canonical_goal_assembly_preserves_obligations_and_full_unicode_outcome_under_pressure() {
    use crate::ai::model_profile::build_system_prompt_sections;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;

    let temp = TempDir::new().unwrap();
    let binding = test_worker_goal_context(temp.path(), "worker", 1);
    let mut snapshot = binding.workflow_snapshot().clone();
    snapshot.goal.objective = "🦀".repeat(4_000);
    snapshot.goal.constraints = vec!["constraint ".repeat(200); 100];
    snapshot.criteria = (0..100)
        .map(|index| {
            let mut criterion = snapshot.criteria[0].clone();
            criterion.id = format!("criterion-{index}");
            criterion.description = "required verification ".repeat(100);
            criterion.status = crate::workflow::CriterionStatus::Passed;
            criterion
        })
        .collect();
    snapshot.criteria.last_mut().unwrap().status = crate::workflow::CriterionStatus::Pending;
    let step = snapshot.steps[0].clone();
    snapshot.steps = (0..100)
        .map(|index| {
            let mut step = step.clone();
            step.id = format!("step-{index}");
            step.status = WorkflowStepStatus::Completed;
            step.description = "historic task ".repeat(100);
            step
        })
        .collect();
    snapshot.steps.push(step);

    for mode in [WorkMode::Build, WorkMode::Plan] {
        let goal = super::plan::build_workflow_context(&snapshot, mode);
        assert!(goal.len() <= 28 * 1024);
        assert!(goal.contains(&snapshot.goal.objective));
        assert!(goal.contains("ADDITIONAL ITEMS OMITTED"));
        assert!(goal.contains("workflow_read"));
        assert!(goal.contains("criterion-99"));
        let system = |text| ModelMessage {
            role: Role::System,
            content: vec![Content::text(text)],
        };
        let mut messages = vec![
            system(format!(
                "[PROJECT INSTRUCTIONS - AGENTS.md]\n{}",
                "project ".repeat(7_000)
            )),
            system(format!("[AVAILABLE SKILLS]\n{}", "skill ".repeat(7_000))),
            system(format!("[PERSISTENT MEMORY]\n{}", "memory ".repeat(7_000))),
            system(goal.clone()),
        ];
        bound_dynamic_context_messages(&mut messages);
        assert!(messages.iter().any(
            |message| matches!(&message.content[0], Content::Text { text, .. } if text == &goal)
        ));
        let sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.5",
            &messages,
            None,
            &[],
        );
        assert!(sections.session_context.contains(&snapshot.goal.objective));
        assert!(sections
            .session_context
            .contains("Goal completion requires verification criteria to pass"));
        if mode == WorkMode::Build {
            assert!(sections.session_context.contains("Continue only Step 1:"));
            assert!(
                sections
                    .session_context
                    .find("Continue only Step 1:")
                    .unwrap()
                    < sections.session_context.find("Constraints:").unwrap()
            );
        } else {
            assert!(sections
                .session_context
                .contains("Plan mode is read-only for project state"));
            assert!(sections
                .session_context
                .contains("Only the user can approve"));
        }
    }
}

#[test]
fn canonical_goal_completion_and_pause_obligations_precede_optional_history() {
    let temp = TempDir::new().unwrap();
    let binding = test_worker_goal_context(temp.path(), "worker", 1);
    let mut snapshot = binding.workflow_snapshot().clone();
    snapshot.steps[0].status = WorkflowStepStatus::Completed;
    let context = super::plan::build_workflow_context(&snapshot, WorkMode::Build);
    assert!(context.contains("All plan steps are terminal"));
    assert!(context.contains("verify_criterion"));
    assert!(context.contains("complete_goal"));
    snapshot.goal.status = GoalStatus::Paused;
    let paused = super::plan::build_workflow_context(&snapshot, WorkMode::Build);
    assert!(paused.contains("Do not continue implementation until the user resumes"));
    assert!(!paused.contains("All plan steps are terminal"));
}

#[test]
fn assembled_code_and_child_prompts_share_bounded_nested_project_instructions() {
    use crate::ai::model_profile::build_system_prompt_sections;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    let nested = root.join("a").join("b");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(root.join(".mitsuro")).unwrap();
    // No Git metadata: the selected project root still owns ancestor instructions.
    fs::write(root.join("AGENTS.md"), "ROOT-INSTRUCTIONS").unwrap();
    fs::write(root.join("a").join("AGENTS.md"), "ANCESTOR-INSTRUCTIONS").unwrap();
    fs::write(nested.join("AGENTS.md"), "WORKING-DIRECTORY-INSTRUCTIONS").unwrap();
    fs::write(
        temp.path().join("AGENTS.md"),
        "OUTSIDE-PROJECT-INSTRUCTIONS",
    )
    .unwrap();
    fs::write(
        root.join(".mitsuro").join("settings.json"),
        serde_json::to_string(&serde_json::json!({
            "system_prompt_append": "设置".repeat(10_000), "delegation_mode": "explicit_only"
        }))
        .unwrap(),
    )
    .unwrap();
    let db_path = temp.path().join("context.sqlite");
    Database::new(&db_path).unwrap();
    let skills = RwLock::new(SkillsManager::with_defaults(&root));
    let messages = inject_context(
        &[],
        &db_path,
        "test-session",
        &nested,
        Some(&root),
        WorkMode::Build,
        &skills,
        None,
        Some("code"),
        None,
        None,
    );
    let parent = build_system_prompt_sections(
        ProviderId::Anthropic,
        ApiFormat::Anthropic,
        "claude-sonnet-4-5",
        &messages,
        None,
        &[],
    );
    let child = super::build_subagent_project_context(&nested, Some(&root));
    for marker in [
        "ROOT-INSTRUCTIONS",
        "ANCESTOR-INSTRUCTIONS",
        "WORKING-DIRECTORY-INSTRUCTIONS",
        "PROJECT SETTINGS TRUNCATED",
    ] {
        assert!(
            parent.project_context.contains(marker),
            "parent missing {marker}"
        );
        assert!(child.contains(marker), "child missing {marker}");
    }
    assert!(!parent.combined().contains("OUTSIDE-PROJECT-INSTRUCTIONS"));
    assert!(!child.contains("OUTSIDE-PROJECT-INSTRUCTIONS"));
    let instructions = super::build_subagent_project_instructions(&nested, Some(&root));
    assert!(parent
        .project_context
        .contains(instructions.split("\n\n[PROJECT SETTINGS]").next().unwrap()));
    assert!(instructions.len() < 41 * 1024);
    assert!(parent
        .base_prompt
        .contains("ExplicitOnly requires a user request"));
}

#[test]
fn environment_discovers_git_from_a_nested_working_directory() {
    let temp = TempDir::new().unwrap();
    git2::Repository::init(temp.path()).unwrap();
    let nested = temp.path().join("src").join("nested");
    fs::create_dir_all(&nested).unwrap();
    fs::write(nested.join("file.rs"), "untracked").unwrap();
    let context = build_environment_context(&nested, None);
    assert!(context.contains("Git repository: yes"));
    assert!(context.contains("Git root:"));
    assert!(context.contains("untracked"));
    assert!(context.contains(&format!("Working directory: {}", nested.display())));
}
