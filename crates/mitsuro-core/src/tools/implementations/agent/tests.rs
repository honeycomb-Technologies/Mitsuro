use super::build::{build_execution_waves, build_isolation_batches};
use super::{
    agent_progress_for_terminal_stage, build_parent_context_brief, build_single_agent_artifact,
    build_single_agent_warnings, compose_child_objective, concise_target_label,
    delegated_persistence_error, emit_single_agent_completion, has_explicit_empty_capabilities,
    normalize_structured_tasks, notify_child_completion, open_delegated_run_store,
    persist_delegated_artifact, persist_single_agent_artifact, resolve_explore_target,
    should_use_parallel_component_pool, truncate_utf8, validate_background_wake_host,
    AcceptanceCheckParams, AgentTool,
};
use crate::agent::subagent::{
    AgentExecutionProfile, AgentProgressStatus, AgentRuntimeManager, DelegatedEvidenceKind,
    SubAgentResult, SubAgentTermination,
};
use crate::agent::DelegatedRunStage;
use crate::ai::types::{Content, ModelMessage, Role};
use crate::storage::{
    DelegatedRunRole, DelegatedRunScope, DelegatedRunStartInput, DelegatedRunStore,
};
use crate::storage::{SessionType, WorkspaceMode};
use crate::tools::registry::{DelegationPolicy, PermissionMode};
use crate::tools::ToolContext;
use crate::Database;
use crate::SessionManager;
use std::fs;
use tempfile::TempDir;
use tokio::sync::mpsc;

fn sample_result(success: bool, error: Option<&str>) -> SubAgentResult {
    SubAgentResult {
        task_id: "agent-1".to_string(),
        agent_name: "planner".to_string(),
        delegated_run_id: Some("run-123".to_string()),
        success,
        output: "Found the relevant code paths".to_string(),
        files_examined: vec!["src/lib.rs".to_string(), "src/main.rs".to_string()],
        duration_ms: 1200,
        turns_used: 3,
        error: error.map(ToString::to_string),
        termination: if success {
            SubAgentTermination::Completed
        } else {
            SubAgentTermination::Failed
        },
        policy_violations: vec![],
        evidence: Default::default(),
        background_processes: vec![],
    }
}

fn seed_terminal_child(
    db_path: &std::path::Path,
    session_id: &str,
    delegated_run_id: &str,
    stage: DelegatedRunStage,
    summary: &str,
) {
    let store = seed_running_child(
        db_path,
        session_id,
        delegated_run_id,
        DelegatedRunRole::Explore,
    );
    store
        .finalize_run(
            delegated_run_id,
            stage,
            &serde_json::json!({"delegated_run_id": delegated_run_id}),
            Some(summary),
            true,
        )
        .expect("delegated run should finalize");
}

fn seed_running_child(
    db_path: &std::path::Path,
    session_id: &str,
    delegated_run_id: &str,
    role: DelegatedRunRole,
) -> DelegatedRunStore {
    let store = DelegatedRunStore::new(Database::new(db_path).expect("delegated database"));
    store
        .create_background_run(&DelegatedRunStartInput {
            delegated_run_id: delegated_run_id.to_string(),
            parent_session_id: session_id.to_string(),
            parent_tool_call_id: Some("tool-1".to_string()),
            role,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![
                DelegatedRunScope {
                    label: "launch workspace".to_string(),
                    path: db_path
                        .parent()
                        .expect("database parent")
                        .canonicalize()
                        .expect("canonical workspace")
                        .display()
                        .to_string(),
                    kind: "workspace".to_string(),
                },
                DelegatedRunScope {
                    label: "project".to_string(),
                    path: ".".to_string(),
                    kind: "project".to_string(),
                },
            ],
        })
        .expect("delegated run should create");
    store
}

#[test]
fn concise_target_label_uses_stable_readable_segments() {
    assert_eq!(
        concise_target_label("crates/mitsuro-core/src", 0),
        "mitsuro-core/src"
    );
    assert_eq!(concise_target_label("README.md", 1), "README.md");
    assert_eq!(concise_target_label("", 2), "target-3");
}

#[test]
fn only_multiple_explicit_write_components_use_the_legacy_pool() {
    let single = vec!["one component".to_string()];
    let parallel = vec!["component a".to_string(), "component b".to_string()];

    assert!(!should_use_parallel_component_pool(
        AgentExecutionProfile::Build,
        None,
    ));
    assert!(!should_use_parallel_component_pool(
        AgentExecutionProfile::Build,
        Some(&single),
    ));
    assert!(should_use_parallel_component_pool(
        AgentExecutionProfile::Build,
        Some(&parallel),
    ));
    assert!(!should_use_parallel_component_pool(
        AgentExecutionProfile::Explore,
        Some(&parallel),
    ));
}

#[test]
fn explicit_empty_capabilities_are_rejected_instead_of_widened() {
    assert!(has_explicit_empty_capabilities(&serde_json::json!({
        "profile": "build",
        "capabilities": []
    })));
    assert!(!has_explicit_empty_capabilities(&serde_json::json!({
        "profile": "build"
    })));
}

#[test]
fn structured_task_graph_is_topologically_ordered_and_builds_union_ceiling() {
    let mut params: super::Params = serde_json::from_value(serde_json::json!({
        "name": "feature-team",
        "tasks": [
            {
                "id": "verify",
                "instructions": "Run focused validation",
                "capabilities": ["read", "execute"],
                "depends_on": ["backend", "frontend"]
            },
            {
                "id": "frontend",
                "instructions": "Implement the client projection",
                "capabilities": ["read", "write"]
            },
            {
                "id": "backend",
                "instructions": "Implement the server contract",
                "capabilities": ["read", "write"]
            }
        ]
    }))
    .expect("structured params");

    assert!(normalize_structured_tasks(&mut params).expect("valid task graph"));
    let tasks = params.tasks.expect("normalized tasks");
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.id.as_str())
            .collect::<Vec<_>>(),
        vec!["frontend", "backend", "verify"]
    );
    assert_eq!(
        params.capabilities,
        vec![
            "execute".to_string(),
            "read".to_string(),
            "write".to_string()
        ]
    );
    assert!(params.prompt.contains("frontend"));
}

#[test]
fn structured_task_graph_gets_a_stable_name_when_provider_omits_one() {
    let mut params: super::Params = serde_json::from_value(serde_json::json!({
        "tasks": [
            {"id": "game-core", "instructions": "Build the game", "capabilities": ["write"]},
            {"id": "pwa-shell", "name": "PWA shell", "instructions": "Build the shell", "capabilities": ["write"]}
        ]
    }))
    .expect("structured params");

    normalize_structured_tasks(&mut params).expect("valid task graph");
    assert_eq!(super::derived_spawn_name(&params), "game-core + PWA shell");
}

#[test]
fn structured_task_graph_rejects_cycles_before_workspace_materialization() {
    let mut params: super::Params = serde_json::from_value(serde_json::json!({
        "name": "cycle",
        "instructions": "This must not start",
        "tasks": [
            {"id": "a", "instructions": "A", "depends_on": ["b"]},
            {"id": "b", "instructions": "B", "depends_on": ["a"]}
        ]
    }))
    .expect("structured params");
    let error = normalize_structured_tasks(&mut params).expect_err("cycle must fail");
    assert!(error.contains("dependency cycle"));
}

#[test]
fn browser_acceptance_requires_execute_capability_before_any_child_starts() {
    let mut invalid: super::Params = serde_json::from_value(serde_json::json!({
        "tasks": [{
            "id": "interface",
            "instructions": "Implement keyboard and touch controls",
            "capabilities": ["read", "write"],
            "acceptance_checks": [{
                "id": "interface-playable",
                "criterion": "Controls are implemented",
                "evidence_kind": "browser_runtime"
            }]
        }]
    }))
    .expect("structured params");

    let error = normalize_structured_tasks(&mut invalid).expect_err("invalid browser authority");

    assert!(error.contains("lacks execute capability"));
    assert!(error.contains("execute-capable verifier"));
}

#[test]
fn structured_runtime_waves_keep_independent_roots_together() {
    let mut params: super::Params = serde_json::from_value(serde_json::json!({
        "name": "waves",
        "tasks": [
            {"id": "verify", "instructions": "Verify", "depends_on": ["api", "ui"]},
            {"id": "ui", "instructions": "Build UI", "capabilities": ["read", "write"]},
            {"id": "api", "instructions": "Build API", "capabilities": ["read", "write"]},
            {"id": "release", "instructions": "Release proof", "depends_on": ["verify"]}
        ]
    }))
    .expect("structured params");
    normalize_structured_tasks(&mut params).expect("normalize graph");
    let runtime = params
        .tasks
        .as_ref()
        .expect("tasks")
        .iter()
        .map(|task| crate::agent::subagent::SubAgentTask::new(&task.id, task.objective()))
        .collect::<Vec<_>>();
    let waves = build_execution_waves(&runtime, params.tasks.as_deref());
    assert_eq!(
        waves
            .iter()
            .map(|wave| wave.iter().map(|task| task.id.as_str()).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec!["ui", "api"], vec!["verify"], vec!["release"]]
    );
}

#[test]
fn dependent_parallel_writer_wave_gets_its_own_isolation_batch() {
    let mut params: super::Params = serde_json::from_value(serde_json::json!({
        "name": "tetris-waves",
        "tasks": [
            {"id": "scaffold", "instructions": "Scaffold", "capabilities": ["read", "write"]},
            {"id": "engine", "instructions": "Engine", "capabilities": ["read", "write"], "depends_on": ["scaffold"]},
            {"id": "interface", "instructions": "Interface", "capabilities": ["read", "write"], "depends_on": ["scaffold"]},
            {"id": "verify", "instructions": "Verify", "capabilities": ["read", "execute"], "depends_on": ["engine", "interface"]}
        ]
    }))
    .expect("structured params");
    normalize_structured_tasks(&mut params).expect("normalize graph");
    let runtime = params
        .tasks
        .as_ref()
        .expect("tasks")
        .iter()
        .map(|task| {
            crate::agent::subagent::SubAgentTask::new(&task.id, task.objective())
                .with_delegation_policy(DelegationPolicy::for_subagent_child(
                    PermissionMode::Autonomous,
                    None,
                    true,
                    task.capabilities
                        .iter()
                        .any(|capability| capability == "write"),
                    task.capabilities
                        .iter()
                        .any(|capability| capability == "execute"),
                ))
        })
        .collect::<Vec<_>>();

    let batches = build_isolation_batches(&runtime, params.tasks.as_deref(), "run-123");
    assert_eq!(
        batches,
        vec![(
            1,
            "run-123-wave-0001".to_string(),
            vec!["engine".to_string(), "interface".to_string()],
        )]
    );
}

#[test]
fn structured_graph_requires_ordering_for_declared_overlapping_writes() {
    let mut unordered: super::Params = serde_json::from_value(serde_json::json!({
        "name": "overlap",
        "tasks": [
            {"id": "a", "instructions": "A", "capabilities": ["write"], "write_intent": ["src"]},
            {"id": "b", "instructions": "B", "capabilities": ["write"], "write_intent": ["src/app.ts"]}
        ]
    }))
    .expect("unordered params");
    let error = normalize_structured_tasks(&mut unordered).expect_err("overlap must fail");
    assert!(error.contains("overlapping write_intent"));

    let mut ordered: super::Params = serde_json::from_value(serde_json::json!({
        "name": "ordered-overlap",
        "tasks": [
            {"id": "a", "instructions": "A", "capabilities": ["write"], "write_intent": ["src"]},
            {"id": "b", "instructions": "B", "capabilities": ["write"], "write_intent": ["src/app.ts"], "depends_on": ["a"]}
        ]
    }))
    .expect("ordered params");
    normalize_structured_tasks(&mut ordered).expect("dependency makes overlap explicit");
}

#[test]
fn resolve_explore_target_rejects_missing_or_outside_paths() {
    let temp = tempfile::tempdir().expect("tempdir");
    let project = temp.path().join("project");
    fs::create_dir_all(project.join("src")).expect("project dirs");

    let inside = resolve_explore_target("src", &project, "directory").expect("inside target");
    assert!(inside.ends_with("src"));

    let missing = resolve_explore_target("missing", &project, "directory")
        .expect_err("missing target should fail");
    assert!(missing.contains("Missing explore target"));

    let outside = resolve_explore_target("../outside", &project, "directory")
        .expect_err("outside target should fail");
    assert!(outside.contains("outside project root") || outside.contains("Missing explore target"));
}

#[test]
fn parent_context_brief_extracts_last_turns() {
    let conversation = vec![
        ModelMessage {
            role: Role::User,
            content: vec![Content::text("First question".to_string())],
        },
        ModelMessage {
            role: Role::Assistant,
            content: vec![Content::text("First answer".to_string())],
        },
        ModelMessage {
            role: Role::User,
            content: vec![Content::text("Second question".to_string())],
        },
    ];

    let brief = build_parent_context_brief(&conversation, 2);
    assert!(brief.contains("[PARENT CONTEXT]"));
    assert!(brief.contains("[/PARENT CONTEXT]"));
    assert!(brief.contains("First answer"));
    assert!(brief.contains("Second question"));
}

#[test]
fn parent_context_brief_truncates_long_messages() {
    let long_text = "x".repeat(300);
    let conversation = vec![ModelMessage {
        role: Role::Assistant,
        content: vec![Content::text(long_text.clone())],
    }];

    let brief = build_parent_context_brief(&conversation, 10);
    assert!(brief.contains("..."));
    assert!(brief.len() < long_text.len());
}

#[test]
fn parent_context_brief_empty_on_no_messages() {
    let brief = build_parent_context_brief(&[], 10);
    assert!(brief.is_empty());
}

#[test]
fn open_delegated_run_store_returns_none_without_db_path() {
    let ctx = ToolContext::default();

    assert!(open_delegated_run_store(&ctx).is_none());
}

#[test]
fn open_delegated_run_store_opens_valid_database() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("delegated.db");
    Database::new(&db_path).expect("database");

    let ctx = ToolContext {
        working_dir: temp_dir.path().to_path_buf(),
        workspace_mode: WorkspaceMode::Neutral,
        db_path: Some(db_path),
        ..Default::default()
    };

    assert!(open_delegated_run_store(&ctx).is_some());
}

#[test]
fn open_delegated_run_store_returns_none_for_unopenable_database() {
    let temp_dir = TempDir::new().expect("tempdir");
    let invalid_db_path = temp_dir.path().join("not-a-db-file");
    std::fs::create_dir_all(&invalid_db_path).expect("directory path should exist");
    let ctx = ToolContext {
        working_dir: temp_dir.path().to_path_buf(),
        workspace_mode: WorkspaceMode::Neutral,
        db_path: Some(invalid_db_path),
        session_id: Some("session-1".to_string()),
        tool_use_id: Some("tool-1".to_string()),
        ..Default::default()
    };

    assert!(open_delegated_run_store(&ctx).is_none());
}

#[test]
fn background_agent_requires_a_live_completion_host() {
    let runtime = AgentRuntimeManager::default();
    let error = validate_background_wake_host(&runtime, &ToolContext::default())
        .expect_err("CLI/TUI-style runtime must fail closed");
    assert!(error.output.contains("background_wake_unsupported"));

    let (sender, receiver) = mpsc::unbounded_channel();
    runtime.set_completion_sender(sender);
    drop(receiver);
    let error = validate_background_wake_host(&runtime, &ToolContext::default())
        .expect_err("closed completion host must fail closed");
    assert!(error.output.contains("background_wake_unsupported"));
}

#[test]
fn background_agent_requires_chat_or_code_parent_session() {
    let temp = TempDir::new().expect("tempdir");
    let db_path = temp.path().join("background-host.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let code_session = manager
        .create_session("code", None, Some(temp.path().to_string_lossy().as_ref()))
        .expect("code session");
    let hive_session = manager
        .create_session_for_user_with_config(
            "hive",
            None,
            Some(temp.path().to_string_lossy().as_ref()),
            Some(temp.path().to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            None,
            None,
            SessionType::Hive,
        )
        .expect("hive session");
    let runtime = AgentRuntimeManager::default();
    let (sender, _receiver) = mpsc::unbounded_channel();
    runtime.set_completion_sender(sender);
    let (reconciliation_sender, _reconciliation_receiver) = mpsc::unbounded_channel();
    runtime.set_completion_reconciliation_sender(reconciliation_sender);

    let code_ctx = ToolContext {
        db_path: Some(db_path.clone()),
        session_id: Some(code_session),
        ..Default::default()
    };
    validate_background_wake_host(&runtime, &code_ctx)
        .expect("server-hosted Code session supports background wake");

    let hive_ctx = ToolContext {
        db_path: Some(db_path),
        session_id: Some(hive_session),
        ..Default::default()
    };
    let error = validate_background_wake_host(&runtime, &hive_ctx)
        .expect_err("Hive parent cannot be woken by the chat continuation host");
    assert!(error.output.contains("background_wake_unsupported"));
}

#[tokio::test]
async fn child_completion_queues_and_notifies_once_with_one_stable_id() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("completion.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let session_id = manager
        .create_session(
            "parent",
            None,
            Some(temp_dir.path().to_string_lossy().as_ref()),
        )
        .expect("session should create");
    seed_terminal_child(
        &db_path,
        &session_id,
        "run-stable",
        DelegatedRunStage::Complete,
        "done",
    );
    let runtime = AgentRuntimeManager::default();
    let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
    runtime.set_completion_sender(completion_tx);

    assert!(notify_child_completion(
        &runtime,
        Some(&db_path),
        Some(&session_id),
        None,
        Some(temp_dir.path()),
        "run-stable",
        "research",
        true,
        "done",
    )
    .expect("first completion should queue"));
    let event = completion_rx.recv().await.expect("completion event");
    assert_eq!(event.pending_id, "child-wake-run-stable");
    assert_eq!(event.session_id.as_deref(), Some(session_id.as_str()));
    assert_eq!(event.terminal_stage, DelegatedRunStage::Complete);
    assert_eq!(event.outcome, "complete");
    assert_eq!(event.usable_agents, 1);
    assert!(manager
        .has_pending_steering(&session_id, &event.pending_id)
        .expect("pending completion should exist"));

    assert!(notify_child_completion(
        &runtime,
        Some(&db_path),
        Some(&session_id),
        None,
        Some(temp_dir.path()),
        "run-stable",
        "research",
        true,
        "done",
    )
    .expect("pending completion should be re-emitted"));
    let retried = completion_rx
        .try_recv()
        .expect("duplicate receipt must not suppress an unacknowledged wake");
    assert_eq!(retried.pending_id, event.pending_id);
}

#[tokio::test]
async fn degraded_child_completion_preserves_partial_outcome_for_parent_integration() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("degraded-completion.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let session_id = manager
        .create_session(
            "parent",
            None,
            Some(temp_dir.path().to_string_lossy().as_ref()),
        )
        .expect("session should create");
    let store = seed_running_child(
        &db_path,
        &session_id,
        "run-degraded",
        DelegatedRunRole::Build,
    );
    store
        .finalize_run(
            "run-degraded",
            DelegatedRunStage::Degraded,
            &serde_json::json!({
                "outcome": "partial",
                "usable_agents": 1,
                "failed_agents": 1,
            }),
            Some("One builder completed and one failed."),
            true,
        )
        .expect("degraded run should finalize");
    let runtime = AgentRuntimeManager::default();
    let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
    runtime.set_completion_sender(completion_tx);

    assert!(notify_child_completion(
        &runtime,
        Some(&db_path),
        Some(&session_id),
        None,
        Some(temp_dir.path()),
        "run-degraded",
        "parallel repair",
        false,
        "One builder completed and one failed.",
    )
    .expect("degraded completion should queue"));

    let event = completion_rx.recv().await.expect("degraded completion");
    assert_eq!(event.terminal_stage, DelegatedRunStage::Degraded);
    assert_eq!(event.outcome, "partial");
    assert_eq!(event.usable_agents, 1);
    assert!(!event.success);
    let Content::Text { text, .. } = &event.content[0] else {
        panic!("completion wake should be text");
    };
    assert!(text.contains("terminal_stage: degraded"));
    assert!(text.contains("outcome: partial"));
    assert!(text.contains("usable_agents: 1"));
}

#[tokio::test]
async fn child_completion_can_be_reemitted_after_listener_was_missing() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("completion.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let session_id = manager
        .create_session(
            "parent",
            None,
            Some(temp_dir.path().to_string_lossy().as_ref()),
        )
        .expect("session should create");
    seed_terminal_child(
        &db_path,
        &session_id,
        "run-retry",
        DelegatedRunStage::Complete,
        "done",
    );
    let runtime = AgentRuntimeManager::default();

    assert!(notify_child_completion(
        &runtime,
        Some(&db_path),
        Some(&session_id),
        None,
        Some(temp_dir.path()),
        "run-retry",
        "research",
        true,
        "done",
    )
    .expect("completion should become durable without a listener"));

    let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
    runtime.set_completion_sender(completion_tx);
    assert!(notify_child_completion(
        &runtime,
        Some(&db_path),
        Some(&session_id),
        None,
        Some(temp_dir.path()),
        "run-retry",
        "research",
        true,
        "done",
    )
    .expect("pending completion should be reclaimed"));
    assert_eq!(
        completion_rx
            .recv()
            .await
            .expect("retried event")
            .pending_id,
        "child-wake-run-retry"
    );
}

#[tokio::test]
async fn closed_completion_receiver_falls_back_to_durable_reconciliation() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("completion-reconciliation.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let session_id = manager
        .create_session(
            "parent",
            None,
            Some(temp_dir.path().to_string_lossy().as_ref()),
        )
        .expect("session should create");
    seed_terminal_child(
        &db_path,
        &session_id,
        "run-reconcile",
        DelegatedRunStage::Complete,
        "done",
    );
    let runtime = AgentRuntimeManager::default();
    let (completion_tx, completion_rx) = mpsc::unbounded_channel();
    runtime.set_completion_sender(completion_tx);
    drop(completion_rx);
    let (reconciliation_tx, mut reconciliation_rx) = mpsc::unbounded_channel();
    runtime.set_completion_reconciliation_sender(reconciliation_tx);

    assert!(notify_child_completion(
        &runtime,
        Some(&db_path),
        Some(&session_id),
        None,
        Some(temp_dir.path()),
        "run-reconcile",
        "research",
        true,
        "done",
    )
    .expect("completion should remain durably queued"));
    assert_eq!(
        reconciliation_rx.recv().await.as_deref(),
        Some("run-reconcile")
    );
    assert!(manager
        .has_pending_steering(&session_id, "child-wake-run-reconcile")
        .expect("durable completion should remain pending"));
}

#[test]
fn child_completion_queue_failure_is_returned_before_live_notification() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("completion.db");
    Database::new(&db_path).expect("database");
    let runtime = AgentRuntimeManager::default();
    let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
    runtime.set_completion_sender(completion_tx);

    let error = notify_child_completion(
        &runtime,
        Some(&db_path),
        Some("missing-session"),
        None,
        Some(temp_dir.path()),
        "run-missing-parent",
        "research",
        false,
        "failed",
    )
    .expect_err("foreign-key queue failure must reach the caller");
    assert!(!error.to_string().is_empty());
    assert!(completion_rx.try_recv().is_err());
}

#[test]
fn single_agent_artifact_keeps_payload_shape() {
    let mut result = sample_result(true, None);
    result.output = r#"Found the relevant code paths
<delegated_handoff>{"status":"complete","summary":"done","acceptance_checks":[{"id":"paths","status":"passed","evidence":"src/lib.rs and src/main.rs"}],"remaining_work":[],"blockers":[]}</delegated_handoff>"#.to_string();
    result
        .evidence
        .record_success(DelegatedEvidenceKind::Observation);
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-123", &result, &policy);

    assert!(artifact
        .review_summary
        .contains("Found the relevant code paths"));
    assert_eq!(artifact.final_stage, DelegatedRunStage::Complete);
    assert_eq!(artifact.payload["delegated_run_id"], "run-123");
    assert!(artifact.payload["findings"]
        .as_str()
        .is_some_and(|findings| findings.contains("Found the relevant code paths")));
    assert_eq!(artifact.payload["files_examined_count"], 2);
    assert_eq!(artifact.payload["success"], true);
    assert_eq!(artifact.payload["outcome"], "success");
    assert!(artifact.payload["next_action_hint"]
        .as_str()
        .is_some_and(|hint| hint.contains("Synthesize")));
    assert_eq!(artifact.payload["agent_count"], 1);
    assert_eq!(artifact.payload["usable_agents"], 1);
    assert_eq!(artifact.payload["failed_agents"], 0);
    assert!(artifact.payload["agents"][0]["summary"]
        .as_str()
        .is_some_and(|summary| summary.contains("Found the relevant code paths")));
    assert_eq!(
        artifact.payload["delegation_policy"]["surface"],
        "subagent_plan"
    );
}

#[test]
fn single_agent_artifact_degrades_success_without_complete_objective() {
    let result = sample_result(true, None);
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-no-handoff", &result, &policy);

    assert_eq!(artifact.final_stage, DelegatedRunStage::Degraded);
    assert_eq!(artifact.payload["success"], false);
    assert_eq!(artifact.payload["outcome"], "partial");
    assert_eq!(artifact.payload["degraded_agents"], 1);
    assert_eq!(artifact.payload["failed_agents"], 0);
}

#[test]
fn single_agent_artifact_completes_only_with_passed_handoff() {
    let mut result = sample_result(true, None);
    result.output = r#"Found the relevant code paths
<delegated_handoff>{"status":"complete","summary":"done","acceptance_checks":[{"id":"tests","status":"passed","evidence":"focused tests passed"}],"remaining_work":[],"blockers":[]}</delegated_handoff>"#.to_string();
    result
        .evidence
        .record_success(DelegatedEvidenceKind::Mutation);
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-complete-handoff", &result, &policy);

    assert_eq!(artifact.final_stage, DelegatedRunStage::Complete);
    assert_eq!(artifact.payload["success"], true);
    assert_eq!(artifact.payload["outcome"], "success");
}

#[test]
fn single_agent_artifact_degrades_when_browser_contract_lacks_canonical_proof() {
    let mut result = sample_result(true, None);
    result.output = r#"Implemented the app.
<delegated_handoff>{"status":"complete","summary":"tests and build passed","acceptance_checks":[{"id":"browser_runtime","status":"passed","evidence":"looked good"}],"remaining_work":[],"blockers":[]}</delegated_handoff>"#.to_string();
    result
        .evidence
        .record_success(DelegatedEvidenceKind::Mutation);
    result.enforce_acceptance_contract(
        r#"Build the app.
<delegated_acceptance_contract>{"required":[{"id":"browser_runtime","criterion":"primary interaction works without console errors"}]}</delegated_acceptance_contract>"#,
    );
    let policy = DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-false-browser", &result, &policy);

    assert_eq!(artifact.final_stage, DelegatedRunStage::Degraded);
    assert_eq!(artifact.payload["success"], false);
    assert_eq!(artifact.payload["outcome"], "partial");
}

#[test]
fn child_objective_injects_typed_acceptance_contract() {
    let objective = compose_child_objective(
        "Implement the game",
        &[AcceptanceCheckParams {
            id: "browser_runtime".to_string(),
            criterion: "phone and desktop interaction has zero console errors".to_string(),
            evidence_kind: Some("browser_runtime".to_string()),
        }],
    );

    assert!(objective.contains("Implement the game"));
    assert!(objective.contains("<delegated_acceptance_contract>"));
    assert!(objective.contains("browser_runtime"));
    assert!(objective.contains("phone and desktop"));
}

#[test]
fn agent_schema_advertises_top_level_acceptance_checks() {
    use crate::agent::AgentCancellation;
    use crate::ai::client::{AiClient, AiClientConfig};
    use crate::tools::registry::Tool;
    use std::sync::Arc;

    let tool = AgentTool::new(
        Arc::new(AiClient::new(
            AiClientConfig::for_grok("grok-4.6"),
            "test".to_string(),
        )),
        AgentCancellation::new(),
        AgentRuntimeManager::default(),
    );
    let schema = tool.parameters_schema();

    assert!(schema["properties"]["acceptance_checks"].is_object());
    assert_eq!(
        schema["properties"]["acceptance_checks"]["items"]["required"],
        serde_json::json!(["id", "criterion"])
    );
    assert_eq!(
        schema["properties"]["acceptance_checks"]["items"]["properties"]["evidence_kind"]["enum"],
        serde_json::json!([
            "handoff",
            "browser_runtime",
            "browser_keyboard",
            "browser_touch"
        ])
    );
    assert!(schema["properties"]["tasks"]["items"]["properties"]["acceptance_checks"].is_object());
    assert!(tool.description().contains("browser_check"));
    assert!(!tool
        .description()
        .contains("dependent execute-capable browser verifier"));
}

#[test]
fn single_agent_artifact_rejects_tool_free_prose_as_success() {
    let mut result = sample_result(true, None);
    result.output = "The task appears complete.".to_string();
    result.files_examined.clear();
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-no-evidence", &result, &policy);

    assert_eq!(artifact.final_stage, DelegatedRunStage::Failed);
    assert_eq!(artifact.payload["success"], false);
    assert_eq!(artifact.payload["outcome"], "failed");
    assert_eq!(artifact.payload["outcome_reason"], "no_usable_evidence");
    assert_eq!(artifact.payload["usable_agents"], 0);
    assert_eq!(artifact.payload["failed_agents"], 1);
    assert_eq!(artifact.payload["agents"][0]["success"], false);
}

#[test]
fn single_agent_artifact_accepts_compact_canonical_tool_evidence() {
    let mut result = sample_result(true, None);
    result.files_examined.clear();
    result
        .evidence
        .record_success(DelegatedEvidenceKind::Execution);
    let policy = DelegationPolicy::for_subagent_verify(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-evidence", &result, &policy);

    assert_eq!(artifact.final_stage, DelegatedRunStage::Degraded);
    assert_eq!(artifact.payload["success"], false);
    assert_eq!(artifact.payload["outcome"], "partial");
    assert_eq!(artifact.payload["usable_agents"], 1);
    assert_eq!(artifact.payload["agents"][0]["evidence"]["executions"], 1);
}

#[test]
fn interrupted_provider_result_with_evidence_is_durable_partial_not_complete() {
    let policy = DelegationPolicy::for_subagent_verify(PermissionMode::Autonomous, Some(8));

    for (termination, reason) in [
        (
            SubAgentTermination::ProviderMaxTokens,
            "provider_max_tokens",
        ),
        (SubAgentTermination::ProviderTimeout, "provider_timeout"),
        (SubAgentTermination::LoopGuard, "loop_guard"),
    ] {
        let mut result = sample_result(false, Some("provider response interrupted"));
        result.files_examined.clear();
        result.termination = termination;
        result
            .evidence
            .record_success(DelegatedEvidenceKind::Execution);

        let artifact = build_single_agent_artifact("run-partial", &result, &policy);

        assert_eq!(artifact.final_stage, DelegatedRunStage::Degraded);
        assert_eq!(artifact.payload["success"], false);
        assert_eq!(artifact.payload["outcome"], "partial");
        assert_eq!(artifact.payload["outcome_reason"], reason);
        assert_eq!(artifact.payload["usable_agents"], 1);
        assert_eq!(artifact.payload["degraded_agents"], 1);
        assert_eq!(artifact.payload["failed_agents"], 0);
        assert_eq!(artifact.payload["agents"][0]["degraded_success"], true);
    }
}

#[test]
fn interrupted_provider_result_without_evidence_is_durable_failure() {
    let mut result = sample_result(false, Some("provider response interrupted"));
    result.output.clear();
    result.files_examined.clear();
    result.termination = SubAgentTermination::ProviderMaxTokens;
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-empty-truncation", &result, &policy);

    assert_eq!(artifact.final_stage, DelegatedRunStage::Failed);
    assert_eq!(artifact.payload["success"], false);
    assert_eq!(artifact.payload["outcome"], "failed");
    assert_eq!(artifact.payload["outcome_reason"], "provider_max_tokens");
    assert_eq!(artifact.payload["usable_agents"], 0);
    assert_eq!(artifact.payload["degraded_agents"], 0);
    assert_eq!(artifact.payload["failed_agents"], 1);
}

#[test]
fn acknowledged_cancellation_remains_distinct_from_failure() {
    let mut result = sample_result(false, Some("Cancelled"));
    result.termination = SubAgentTermination::Cancelled;
    result
        .evidence
        .record_success(DelegatedEvidenceKind::Mutation);
    let policy = DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-cancelled", &result, &policy);

    assert_eq!(artifact.final_stage, DelegatedRunStage::Cancelled);
    assert_eq!(artifact.payload["success"], false);
    assert_eq!(artifact.payload["outcome"], "cancelled");
    assert_eq!(artifact.payload["outcome_reason"], "cancelled");
    assert_eq!(artifact.payload["failed_agents"], 0);
    assert_eq!(artifact.payload["agents"][0]["evidence"]["mutations"], 1);
}

#[test]
fn interrupted_provider_evidence_persists_as_degraded_terminal_row() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("provider-partial.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let session_id = manager
        .create_session(
            "parent",
            None,
            Some(temp_dir.path().to_string_lossy().as_ref()),
        )
        .expect("session should create");
    let store = seed_running_child(
        &db_path,
        &session_id,
        "run-provider-partial",
        DelegatedRunRole::Explore,
    );
    let mut result = sample_result(false, Some("provider call timed out"));
    result.files_examined.clear();
    result.termination = SubAgentTermination::ProviderTimeout;
    result
        .evidence
        .record_success(DelegatedEvidenceKind::Observation);
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));
    let artifact = build_single_agent_artifact("run-provider-partial", &result, &policy);

    let authoritative = persist_single_agent_artifact(
        &store,
        "run-provider-partial",
        &artifact,
        true,
        "persist provider partial",
    )
    .expect("partial result should persist");

    assert_eq!(authoritative.stage, DelegatedRunStage::Degraded);
    assert_eq!(
        authoritative.artifact.as_ref().unwrap()["outcome"],
        "partial"
    );
    assert_eq!(
        authoritative.artifact.as_ref().unwrap()["outcome_reason"],
        "provider_timeout"
    );
}

#[test]
fn long_review_summary_keeps_the_final_outcome_tail() {
    let mut result = sample_result(true, None);
    result.output = format!(
        "Initial investigation. {}\nFINAL OUTCOME: validation passed; no blockers remain.",
        "detail ".repeat(300)
    );
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));

    let artifact = build_single_agent_artifact("run-long-summary", &result, &policy);

    assert!(artifact
        .review_summary
        .starts_with("Initial investigation."));
    assert!(artifact
        .review_summary
        .ends_with("FINAL OUTCOME: validation passed; no blockers remain."));
    assert!(artifact.review_summary.len() <= 1_204);
}

#[test]
fn successful_build_finalization_stays_resumable() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("build-finalization.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let session_id = manager
        .create_session(
            "parent",
            None,
            Some(temp_dir.path().to_string_lossy().as_ref()),
        )
        .expect("session should create");
    let store = seed_running_child(&db_path, &session_id, "build-run", DelegatedRunRole::Build);
    let payload = serde_json::json!({"outcome": "success", "files_modified": 2});

    let authoritative = persist_delegated_artifact(
        &store,
        "build-run",
        DelegatedRunStage::Complete,
        &payload,
        "build complete",
        true,
    )
    .expect("successful build should finalize");

    assert_eq!(authoritative.stage, DelegatedRunStage::Complete);
    assert_eq!(authoritative.role, DelegatedRunRole::Build);
    assert!(authoritative.resumable);
    assert_eq!(authoritative.artifact.as_ref(), Some(&payload));
}

#[test]
fn single_agent_finalization_rejects_a_stale_result_after_cancellation_won() {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("cancelled-finalization.db");
    let manager = SessionManager::new(Database::new(&db_path).expect("database"));
    let session_id = manager
        .create_session(
            "parent",
            None,
            Some(temp_dir.path().to_string_lossy().as_ref()),
        )
        .expect("session should create");
    seed_terminal_child(
        &db_path,
        &session_id,
        "cancelled-run",
        DelegatedRunStage::Cancelled,
        "cancelled by parent",
    );
    let policy = DelegationPolicy::for_subagent_plan(PermissionMode::Autonomous, Some(8));
    let artifact =
        build_single_agent_artifact("cancelled-run", &sample_result(true, None), &policy);
    let store = DelegatedRunStore::new(Database::new(&db_path).expect("delegated database"));

    let error = persist_single_agent_artifact(
        &store,
        "cancelled-run",
        &artifact,
        true,
        "persist child result",
    )
    .expect_err("stale child result must not look durable");
    assert!(
        error.chain().any(|cause| cause
            .to_string()
            .contains("authoritative stage is Cancelled")),
        "unexpected persistence error: {error:#}"
    );

    let authoritative = store
        .get_run("cancelled-run")
        .expect("load cancelled run")
        .expect("cancelled run");
    assert_eq!(authoritative.stage, DelegatedRunStage::Cancelled);
    assert_eq!(
        authoritative.human_review.as_deref(),
        Some("cancelled by parent")
    );
}

#[test]
fn persistence_error_preserves_the_unpersisted_result() {
    let payload = serde_json::json!({"findings": "usable result", "success": true});
    let result = delegated_persistence_error(
        "run-unpersisted",
        payload.clone(),
        &anyhow::anyhow!("disk full"),
    );

    assert!(result.is_error);
    let envelope: serde_json::Value =
        serde_json::from_str(&result.output).expect("structured persistence error");
    assert_eq!(envelope["error"]["code"], "agent_persistence_error");
    assert_eq!(envelope["data"]["delegated_run_id"], "run-unpersisted");
    assert_eq!(envelope["data"]["unpersisted_result"], payload);
}

#[test]
fn single_agent_warnings_preserve_failure_wording() {
    let errored = sample_result(false, Some("tool timeout"));
    assert_eq!(
        build_single_agent_warnings(&errored, "Verification"),
        vec!["Verification failed: tool timeout".to_string()]
    );

    let empty = sample_result(false, None);
    assert_eq!(
        build_single_agent_warnings(&empty, "Planning"),
        vec!["Planning completed without usable results.".to_string()]
    );

    let ok = sample_result(true, Some("ignored"));
    assert!(build_single_agent_warnings(&ok, "Exploration").is_empty());
}

#[test]
fn emit_single_agent_completion_reports_summary_and_status() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let result = sample_result(false, Some("lint failed"));

    emit_single_agent_completion(
        &Some(tx),
        "run-999",
        "verify",
        &result,
        DelegatedRunStage::Failed,
        "short summary",
    );

    let progress = rx.try_recv().expect("completion event");
    assert_eq!(progress.delegated_run_id.as_deref(), Some("run-999"));
    assert_eq!(progress.task_id, "agent-1");
    assert_eq!(progress.name, "verify");
    assert_eq!(progress.status, AgentProgressStatus::Failed);
    assert_eq!(
        progress.completion_summary.as_deref(),
        Some("short summary")
    );
}

#[test]
fn single_agent_completion_uses_authoritative_terminal_stage() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let raw_success_without_authoritative_evidence = sample_result(true, None);

    emit_single_agent_completion(
        &Some(tx),
        "run-authoritative-failure",
        "explore",
        &raw_success_without_authoritative_evidence,
        DelegatedRunStage::Failed,
        "No usable evidence was retained.",
    );

    let progress = rx.try_recv().expect("completion event");
    assert_eq!(progress.status, AgentProgressStatus::Failed);
    assert_eq!(progress.current_action, None);

    assert_eq!(
        agent_progress_for_terminal_stage(DelegatedRunStage::Degraded),
        (AgentProgressStatus::Degraded, Some("degraded".to_string()))
    );
    assert_eq!(
        agent_progress_for_terminal_stage(DelegatedRunStage::Cancelled),
        (
            AgentProgressStatus::Cancelled,
            Some("cancelled".to_string())
        )
    );
}

#[test]
fn truncate_utf8_respects_char_boundaries() {
    // ASCII: truncation at exact byte offset is fine
    assert_eq!(truncate_utf8("hello world", 5), "hello");
    // Multi-byte: U+00E9 (e-acute) is 2 bytes in UTF-8
    let s = "caf\u{00e9}!"; // "cafe!" with accented e — 6 bytes total
    assert_eq!(truncate_utf8(s, 10), s); // within budget
    assert_eq!(truncate_utf8(s, 4), "caf"); // byte 4 is mid-char, backs up to 3
    assert_eq!(truncate_utf8(s, 5), "caf\u{00e9}"); // byte 5 is right after the char
                                                    // Empty and zero budget
    assert_eq!(truncate_utf8("", 0), "");
    assert_eq!(truncate_utf8("abc", 0), "");
}
