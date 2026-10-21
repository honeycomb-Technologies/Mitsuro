use chrono::Utc;
use rusqlite::params;
use serde_json::Value;
use std::collections::BTreeSet;
use tempfile::TempDir;

use super::model::MAX_DELEGATION_TASK_OBJECTIVE_BYTES;
use super::*;
use crate::agent::subagent::{AgentConversationEvent, AgentConversationToolCall};
use crate::ai::models::{ApiFormat, ModelKey};
use crate::ai::providers::{ProviderId, ReasoningEffort};
use crate::storage::{Database, DelegatedRunRole, DelegatedRunScope};
use crate::tools::registry::PermissionMode;

fn create_store() -> (DelegationStore, TempDir) {
    let temp_dir = TempDir::new().expect("temp dir");
    let db = Database::new(&temp_dir.path().join("delegation.db")).expect("db");
    let now = Utc::now().to_rfc3339();
    db.conn()
        .execute(
            "INSERT INTO sessions (id, title, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params!["session-1", "Delegation Test", now, now],
        )
        .expect("seed session");
    (DelegationStore::new(db), temp_dir)
}

fn input() -> DelegationGroupStartInput {
    DelegationGroupStartInput {
        delegation_group_id: "group-1".to_string(),
        parent_session_id: "session-1".to_string(),
        parent_tool_call_id: Some("tool-1".to_string()),
        contract: DelegationGroupContract {
            execution_mode: DelegationExecutionMode::Detached,
            completion_policy: DelegationCompletionPolicy::AllSettled,
            failure_policy: DelegationFailurePolicy::Continue,
            governance: DelegationGovernance {
                permission_mode: PermissionMode::Supervised,
                reasoning_effort: None,
                delegated_turn_budget: Some(12),
                max_parallelism: 2,
                execution_tool_allowlist: Some(BTreeSet::from(["read".to_string()])),
                delegation_policy: crate::tools::registry::DelegationPolicy::for_subagent_explore(
                    PermissionMode::Supervised,
                    Some(12),
                ),
            },
        },
        tasks: vec![
            DelegationTaskSpec {
                delegation_task_id: "task-storage".to_string(),
                task_key: "storage".to_string(),
                objective: "Map durable delegation state".to_string(),
                role: DelegatedRunRole::Explore,
                target_scope: vec![DelegatedRunScope {
                    label: "storage".to_string(),
                    path: "crates/mitsuro-core/src/storage".to_string(),
                    kind: "directory".to_string(),
                }],
                max_attempts: 2,
                depends_on: Vec::new(),
                write_intent: Vec::new(),
                task_policy: None,
                writer_mode: DelegationWriterMode::Shared,
                attempt_workspace: None,
                workspace_baseline: None,
                executor_envelope: None,
            },
            DelegationTaskSpec {
                delegation_task_id: "task-ui".to_string(),
                task_key: "ui".to_string(),
                objective: "Map cross-client projections".to_string(),
                role: DelegatedRunRole::Planner,
                target_scope: Vec::new(),
                max_attempts: 1,
                depends_on: Vec::new(),
                write_intent: Vec::new(),
                task_policy: None,
                writer_mode: DelegationWriterMode::Shared,
                attempt_workspace: None,
                workspace_baseline: None,
                executor_envelope: None,
            },
        ],
    }
}

fn input_for(group_id: &str) -> DelegationGroupStartInput {
    let mut input = input();
    input.delegation_group_id = group_id.to_string();
    for (index, task) in input.tasks.iter_mut().enumerate() {
        task.delegation_task_id = format!("{group_id}-task-{index}");
    }
    input
}

#[test]
fn delegated_turn_budget_is_optional_and_old_integer_contracts_remain_readable() {
    let mut encoded = serde_json::to_value(input().contract).expect("serialize contract");
    assert_eq!(encoded["governance"]["delegated_turn_budget"], 12);

    encoded["governance"]["reasoning_effort"] =
        serde_json::to_value(ReasoningEffort::Medium).unwrap();
    let typed: DelegationGroupContract =
        serde_json::from_value(encoded.clone()).expect("typed reasoning contract remains readable");
    assert_eq!(
        typed.governance.reasoning_effort,
        Some(ReasoningEffort::Medium)
    );

    let old_contract: DelegationGroupContract =
        serde_json::from_value(encoded.clone()).expect("old integer contract remains readable");
    assert_eq!(old_contract.governance.delegated_turn_budget, Some(12));

    encoded["governance"]
        .as_object_mut()
        .expect("governance object")
        .remove("delegated_turn_budget");
    encoded["governance"]
        .as_object_mut()
        .expect("governance object")
        .remove("reasoning_effort");
    let unlimited: DelegationGroupContract =
        serde_json::from_value(encoded).expect("missing budget means no turn ceiling");
    assert_eq!(unlimited.governance.delegated_turn_budget, None);
    assert_eq!(unlimited.governance.reasoning_effort, None);
    unlimited
        .validate(2)
        .expect("unlimited governance remains valid");
}

#[test]
fn durable_objective_budget_keeps_project_context_headroom_but_stays_bounded() {
    let (store, _temp_dir) = create_store();
    let mut group = input();
    group.tasks.truncate(1);
    group.contract.governance.max_parallelism = 1;
    group.tasks[0].objective = "x".repeat(40 * 1024);
    store
        .create_group(&group)
        .expect("bounded project context plus coordinator wrapper should persist");

    let mut oversized = input_for("oversized-objective");
    oversized.tasks.truncate(1);
    oversized.contract.governance.max_parallelism = 1;
    oversized.tasks[0].objective = "x".repeat(MAX_DELEGATION_TASK_OBJECTIVE_BYTES + 1);
    assert!(store
        .create_group(&oversized)
        .expect_err("oversized durable objective must fail closed")
        .to_string()
        .contains("objective exceeds"));
}

fn replayable_input_for(group_id: &str, workspace: &std::path::Path) -> DelegationGroupStartInput {
    let mut group = input_for(group_id);
    let workspace = workspace.display().to_string();
    for task in &mut group.tasks {
        let kind = match &task.role {
            DelegatedRunRole::Planner => DelegationExecutorKind::Plan,
            _ => DelegationExecutorKind::Explore,
        };
        task.executor_envelope = Some(DelegationExecutorEnvelopeV1 {
            version: DELEGATION_EXECUTOR_ENVELOPE_VERSION,
            session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-1".to_string()),
            session_type: DelegationExecutorSessionType::Code,
            user_id: None,
            task_id: task.delegation_task_id.clone(),
            task_name: task.task_key.clone(),
            kind,
            role: task.role.clone(),
            provider_id: ProviderId::OpenAI.to_string(),
            model_key: ModelKey::new(ProviderId::OpenAI, "test:model", ApiFormat::OpenAIResponses),
            resolved_model: "test:model".to_string(),
            working_dir: workspace.clone(),
            project_dir: Some(workspace.clone()),
            sandbox_root: workspace.clone(),
            objective_sha256: DelegationExecutorEnvelopeV1::objective_digest(&task.objective),
        });
    }
    group
}

fn capacity_policy(limit: usize) -> DelegationCapacityPolicy {
    DelegationCapacityPolicy {
        initial_limit: limit,
        minimum_limit: 1,
        maximum_limit: limit.max(4),
        ramp_step: 1,
        healthy_completions_before_ramp: 2,
        default_cooldown_ms: 1_000,
    }
}

fn capacity_request(
    domain: &str,
    partition: &str,
    scheduling_class: DelegationCapacityClass,
    isolation_group: Option<&str>,
) -> DelegationCapacityRequest {
    DelegationCapacityRequest {
        authority_key: "test-host".to_string(),
        domain_key: domain.to_string(),
        partition_key: partition.to_string(),
        scheduling_class,
        isolation_group: isolation_group.map(str::to_string),
    }
}

#[test]
fn group_and_tasks_are_created_atomically_with_immutable_contract() {
    let (store, _temp_dir) = create_store();
    let expected_contract = input().contract;
    let group = store.create_group(&input()).expect("create group");

    assert_eq!(group.state, DelegationGroupState::Created);
    assert_eq!(group.contract, expected_contract);
    assert_eq!(
        group.parent_continuation_state,
        DelegationParentContinuationState::Pending
    );
    assert_eq!(
        group.parent_continuation_id.as_deref(),
        Some("child-wake-group-1")
    );
    assert_eq!(group.tasks.len(), 2);
    assert_eq!(group.tasks[0].ordinal, 0);
    assert_eq!(group.tasks[1].specification.task_key, "ui");
    assert!(group.tasks.iter().all(|task| task.attempt_count == 0));
}

#[test]
fn executor_envelope_is_versioned_bounded_and_separate_from_task_projection() {
    let (store, temp_dir) = create_store();
    let mut group = input();
    let objective = group.tasks[0].objective.clone();
    let task_id = group.tasks[0].delegation_task_id.clone();
    let workspace = temp_dir.path().display().to_string();
    group.tasks[0].executor_envelope = Some(DelegationExecutorEnvelopeV1 {
        version: DELEGATION_EXECUTOR_ENVELOPE_VERSION,
        session_id: "session-1".to_string(),
        parent_tool_call_id: Some("tool-1".to_string()),
        session_type: DelegationExecutorSessionType::Code,
        user_id: None,
        task_id: task_id.clone(),
        task_name: "storage recovery".to_string(),
        kind: DelegationExecutorKind::Explore,
        role: DelegatedRunRole::Explore,
        provider_id: ProviderId::OpenAI.to_string(),
        model_key: ModelKey::new(ProviderId::OpenAI, "test:model", ApiFormat::OpenAIResponses),
        resolved_model: "test:model".to_string(),
        working_dir: workspace.clone(),
        project_dir: Some(workspace.clone()),
        sandbox_root: workspace,
        objective_sha256: DelegationExecutorEnvelopeV1::objective_digest(&objective),
    });
    store.create_group(&group).expect("create replayable group");
    let restored = store
        .get_task(&task_id)
        .expect("task lookup")
        .expect("task");
    assert_eq!(
        restored.specification.executor_envelope,
        group.tasks[0].executor_envelope
    );

    let db = Database::new(&temp_dir.path().join("delegation.db")).expect("inspect db");
    let (specification_json, version, envelope_json): (String, i64, String) = db
        .conn()
        .query_row(
            "SELECT specification_json, executor_envelope_version, executor_envelope_json
               FROM delegation_tasks WHERE delegation_task_id = ?1",
            params![task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("executor columns");
    assert_eq!(version, i64::from(DELEGATION_EXECUTOR_ENVELOPE_VERSION));
    assert!(!specification_json.contains("executor_envelope"));
    assert!(!envelope_json.contains(&objective));
    assert!(!envelope_json.contains("tool_output"));
    assert!(envelope_json.len() < 32 * 1024);

    db.conn()
        .execute(
            "UPDATE delegation_tasks SET executor_envelope_json = '{' WHERE delegation_task_id = ?1",
            params![task_id],
        )
        .expect("corrupt executor envelope");
    let corrupt = store
        .get_task(&task_id)
        .expect("corrupt task remains readable for fail-closed recovery")
        .expect("corrupt task");
    assert_eq!(
        corrupt
            .specification
            .executor_envelope
            .as_ref()
            .expect("corrupt envelope sentinel")
            .version,
        0
    );
    assert!(corrupt.specification.validate().is_err());
}

#[test]
fn detached_parent_continuation_has_one_durable_owner() {
    let (store, _temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .transition_group("group-1", DelegationGroupState::Failed)
        .expect("fail queued group");
    assert!(store
        .authorize_parent_continuation("group-1", "child-wake-group-1")
        .expect("authorize"));
    assert!(store
        .mark_parent_continuation_queued("group-1", "child-wake-group-1")
        .expect("queue continuation"));
    assert!(store
        .mark_parent_continuation_promoted("group-1", "child-wake-group-1")
        .expect("promote continuation"));
    let group = store.get_group("group-1").expect("read").expect("group");
    assert_eq!(
        group.parent_continuation_state,
        DelegationParentContinuationState::Promoted
    );
    assert!(!store
        .authorize_parent_continuation("group-1", "child-wake-other")
        .expect("reject sibling"));
}

#[test]
fn session_cancel_suppresses_detached_wakes_and_discards_pending_steering() {
    let (store, temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .transition_group("group-1", DelegationGroupState::Failed)
        .expect("fail queued group");
    let database = Database::new(&temp_dir.path().join("delegation.db")).expect("open database");
    crate::storage::SessionManager::new(database)
        .queue_pending_steering_once(
            "session-1",
            "child-wake-group-1",
            r#"[{"type":"text","text":"child finished"}]"#,
        )
        .expect("queue pending wake");
    assert!(store
        .mark_parent_continuation_queued("group-1", "child-wake-group-1")
        .expect("mark queued"));

    assert_eq!(
        store
            .suppress_parent_continuations_for_session("session-1")
            .expect("suppress wakes"),
        1
    );
    assert_eq!(
        store
            .suppress_parent_continuations_for_session("session-1")
            .expect("repeat suppression"),
        0,
        "session cancellation must remain idempotent"
    );
    let group = store.get_group("group-1").expect("read").expect("group");
    assert_eq!(
        group.parent_continuation_state,
        DelegationParentContinuationState::NotRequested
    );
    let database = Database::new(&temp_dir.path().join("delegation.db")).expect("reopen database");
    let manager = crate::storage::SessionManager::new(database);
    assert!(manager
        .load_pending_steering("session-1", "child-wake-group-1")
        .expect("load pending wake")
        .is_none());
    assert!(
        !manager
            .queue_pending_steering_once(
                "session-1",
                "child-wake-group-1",
                r#"[{"type":"text","text":"late child"}]"#,
            )
            .expect("late wake remains fenced"),
        "the retained idempotency receipt must fence a racing finalizer"
    );
}

#[test]
fn session_events_replay_from_a_monotonic_cursor_without_content_payloads() {
    let (store, _temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .transition_group("group-1", DelegationGroupState::Failed)
        .expect("fail group");

    let events = store
        .list_session_events_after("session-1", 0, 100)
        .expect("replay events");
    assert_eq!(events.len(), 3);
    assert!(events
        .windows(2)
        .all(|pair| pair[0].event_id < pair[1].event_id));
    assert_eq!(events[0].event_type, DelegationEventType::GroupCreated);
    let serialized = serde_json::to_string(&events).expect("serialize events");
    assert!(!serialized.contains("Map durable delegation state"));

    let tail = store
        .list_session_events_after("session-1", events[1].event_id, 100)
        .expect("replay tail");
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].event_id, events[2].event_id);
}

#[test]
fn task_activity_is_bounded_typed_and_group_authorized() {
    let (store, _temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    let activity = DelegationTaskActivity {
        agent_name: "Storage Agent".to_string(),
        status: "running".to_string(),
        tool_count: 3,
        tokens: 1_024,
        current_action: Some("Reviewing durable event replay".to_string()),
        completion_summary: None,
        lines_added: 12,
        lines_removed: 4,
        completed_plan_task: None,
    };

    store
        .record_task_activity("group-1", "task-storage", &activity)
        .expect("record task activity");
    let events = store
        .list_session_events_after("session-1", 0, 100)
        .expect("list task activity");
    let event = events
        .iter()
        .find(|event| event.event_type == DelegationEventType::TaskActivity)
        .expect("task activity event");
    assert_eq!(event.delegation_task_id.as_deref(), Some("task-storage"));
    assert_eq!(
        serde_json::from_value::<DelegationTaskActivity>(event.payload.clone())
            .expect("typed activity payload"),
        activity
    );

    let foreign = input_for("group-2");
    store.create_group(&foreign).expect("create foreign group");
    let error = store
        .record_task_activity("group-1", "group-2-task-0", &activity)
        .expect_err("foreign task must be rejected");
    assert!(error.to_string().contains("lost its group authority"));

    let mut oversized = activity;
    oversized.current_action = Some("x".repeat(513));
    let error = store
        .record_task_activity("group-1", "task-storage", &oversized)
        .expect_err("oversized activity must be rejected");
    assert!(error.to_string().contains("exceeds its size limit"));
}

#[test]
fn task_conversation_replays_semantic_chat_boundaries() {
    let (store, _temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    let assistant = AgentConversationEvent::AssistantTurn {
        message_id: "task-storage:turn:1".to_string(),
        turn: 1,
        content: "I am checking the durable store.".to_string(),
        tool_calls: vec![AgentConversationToolCall {
            id: "read-1".to_string(),
            name: "read".to_string(),
            arguments: serde_json::json!({"file_path": "src/storage.rs"}),
        }],
    };
    let result = AgentConversationEvent::ToolResult {
        message_id: "task-storage:turn:1".to_string(),
        tool_call_id: "read-1".to_string(),
        name: "read".to_string(),
        output: "bounded result".to_string(),
        is_error: false,
    };
    store
        .record_task_conversation("group-1", "task-storage", &assistant)
        .expect("record assistant turn");
    store
        .record_task_conversation("group-1", "task-storage", &result)
        .expect("record tool result");

    let replayed = store
        .list_session_events_after("session-1", 0, 100)
        .expect("replay conversation")
        .into_iter()
        .filter(|event| event.event_type == DelegationEventType::TaskConversation)
        .map(|event| {
            serde_json::from_value::<AgentConversationEvent>(event.payload)
                .expect("typed conversation event")
        })
        .collect::<Vec<_>>();
    assert_eq!(replayed, vec![assistant, result]);

    let foreign = input_for("group-2");
    store.create_group(&foreign).expect("create foreign group");
    let error = store
        .record_task_conversation("group-1", "group-2-task-0", &replayed[0])
        .expect_err("foreign conversation must be rejected");
    assert!(error.to_string().contains("lost its delegation authority"));
}

#[test]
fn delegation_event_type_round_trips_unknown_protocol_kinds() {
    let event_type: DelegationEventType =
        serde_json::from_str(r#""future_scheduler_event""#).expect("decode future event kind");
    assert_eq!(
        event_type,
        DelegationEventType::Other("future_scheduler_event".to_owned())
    );
    assert_eq!(
        serde_json::to_string(&event_type).expect("encode future event kind"),
        r#""future_scheduler_event""#
    );
}

#[test]
fn invalid_task_set_rolls_back_the_entire_group() {
    let (store, _temp_dir) = create_store();
    let mut invalid = input();
    invalid.tasks[1].task_key = invalid.tasks[0].task_key.clone();

    store
        .create_group(&invalid)
        .expect_err("duplicate key must fail");
    assert!(store.get_group("group-1").expect("read group").is_none());
}

#[test]
fn dependency_graph_admits_only_ready_tasks_and_releases_dependents() {
    let (store, _temp_dir) = create_store();
    let mut graph = input();
    graph.tasks[1].depends_on = vec!["storage".to_string()];
    store.create_group(&graph).expect("create dependency graph");
    store
        .queue_group("group-1")
        .expect("queue dependency graph");

    let first = store
        .claim_tasks("group-1", "owner-first", 2, 10_000)
        .expect("claim ready roots");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].task.specification.task_key, "storage");
    assert!(store
        .complete_task(
            "task-storage",
            "owner-first",
            DelegationTaskState::Complete,
            Some(&serde_json::json!({"summary": "mapped storage"})),
            None,
        )
        .expect("complete prerequisite"));

    let second = store
        .claim_tasks("group-1", "owner-second", 2, 10_000)
        .expect("claim released dependent");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].task.specification.task_key, "ui");
}

#[test]
fn isolated_dependency_waits_for_durable_integration_barrier() {
    let (store, _temp_dir) = create_store();
    let mut graph = input();
    graph.tasks[0].writer_mode = DelegationWriterMode::Isolated;
    graph.tasks[0].attempt_workspace = Some("/tmp/isolated-task".to_string());
    graph.tasks[0].workspace_baseline = Some("baseline".to_string());
    graph.tasks[1].depends_on = vec!["storage".to_string()];
    store.create_group(&graph).expect("create dependency graph");
    store.queue_group("group-1").expect("queue graph");
    store
        .claim_task("task-storage", "owner-first", 10_000)
        .expect("claim prerequisite")
        .expect("prerequisite lease");
    assert!(store
        .complete_task(
            "task-storage",
            "owner-first",
            DelegationTaskState::Complete,
            Some(&serde_json::json!({"integration_state": "pending"})),
            None,
        )
        .expect("complete child loop"));
    assert!(store
        .claim_task("task-ui", "owner-second", 10_000)
        .expect("check blocked dependent")
        .is_none());
    assert_eq!(
        store
            .get_group("group-1")
            .expect("read group")
            .expect("group")
            .state,
        DelegationGroupState::Running
    );

    assert!(store
        .complete_task_integration("task-storage", true, None)
        .expect("publish integration"));
    assert!(store
        .claim_task("task-ui", "owner-second", 10_000)
        .expect("claim integrated dependent")
        .is_some());
}

#[test]
fn isolated_baseline_refresh_is_fenced_before_task_admission() {
    let (store, _temp_dir) = create_store();
    let mut graph = input();
    graph.tasks.truncate(1);
    graph.contract.governance.max_parallelism = 1;
    graph.tasks[0].writer_mode = DelegationWriterMode::Isolated;
    graph.tasks[0].attempt_workspace = Some("/tmp/group-1-wave-0001/task-0000".to_string());
    graph.tasks[0].workspace_baseline = Some("old-baseline".to_string());
    store.create_group(&graph).expect("create isolated graph");
    store.queue_group("group-1").expect("queue isolated graph");

    assert!(store
        .refresh_isolated_task_baseline(
            "task-storage",
            "/tmp/group-1-wave-0001/task-0000",
            "dependency-aware-baseline",
        )
        .expect("refresh queued baseline"));
    assert_eq!(
        store
            .get_task("task-storage")
            .expect("read task")
            .expect("task")
            .specification
            .workspace_baseline
            .as_deref(),
        Some("dependency-aware-baseline")
    );

    store
        .claim_task("task-storage", "owner", 10_000)
        .expect("claim isolated task")
        .expect("lease");
    let error = store
        .refresh_isolated_task_baseline(
            "task-storage",
            "/tmp/group-1-wave-0001/task-0000",
            "too-late",
        )
        .expect_err("leased task must fence baseline refresh");
    assert!(error.to_string().contains("before task admission"));
}

#[test]
fn pre_admission_wave_failure_settles_tasks_and_all_transitive_dependents() {
    let (store, _temp_dir) = create_store();
    let mut graph = input();
    graph.tasks[1].depends_on = vec!["storage".to_string()];
    let mut final_task = graph.tasks[1].clone();
    final_task.delegation_task_id = "task-final".to_string();
    final_task.task_key = "final".to_string();
    final_task.depends_on = vec!["ui".to_string()];
    graph.tasks.push(final_task);
    store.create_group(&graph).expect("create dependency graph");
    store
        .queue_group("group-1")
        .expect("queue dependency graph");

    assert_eq!(
        store
            .fail_unstarted_tasks(
                "group-1",
                &["task-storage".to_string()],
                "dependency snapshot could not be materialized",
            )
            .expect("persist pre-admission failure"),
        1
    );
    assert_eq!(
        store
            .get_task("task-storage")
            .expect("read failed task")
            .expect("failed task")
            .state,
        DelegationTaskState::Failed
    );
    for task_id in ["task-ui", "task-final"] {
        assert_eq!(
            store
                .get_task(task_id)
                .expect("read dependent")
                .expect("dependent")
                .state,
            DelegationTaskState::Cancelled,
            "all dependency depths must settle in one transaction"
        );
    }
    assert_eq!(
        store
            .get_group("group-1")
            .expect("read group")
            .expect("group")
            .state,
        DelegationGroupState::Failed
    );
}

#[test]
fn degraded_dependency_does_not_cancel_downstream_task() {
    let (store, _temp_dir) = create_store();
    let mut graph = input();
    graph.tasks[1].depends_on = vec!["storage".to_string()];
    store.create_group(&graph).expect("create dependency graph");
    store
        .queue_group("group-1")
        .expect("queue dependency graph");
    store
        .claim_task("task-storage", "owner-first", 10_000)
        .expect("claim prerequisite")
        .expect("prerequisite lease");
    assert!(store
        .complete_task(
            "task-storage",
            "owner-first",
            DelegationTaskState::Degraded,
            Some(&serde_json::json!({
                "integration_state": "ready",
                "termination": "provider_timeout"
            })),
            Some("API call timed out after 180s on turn 3"),
        )
        .expect("degrade prerequisite"));

    let claimed = store
        .claim_tasks("group-1", "owner-second", 2, 10_000)
        .expect("claim dependents after degraded prerequisite");
    assert_eq!(
        claimed.len(),
        1,
        "degraded usable prerequisite should unblock dependents"
    );
    let dependent = store
        .get_task("task-ui")
        .expect("read dependent")
        .expect("dependent task");
    assert_ne!(dependent.state, DelegationTaskState::Cancelled);
}

#[test]
fn failed_dependency_cancels_downstream_task_and_settles_group() {
    let (store, _temp_dir) = create_store();
    let mut graph = input();
    graph.tasks[1].depends_on = vec!["storage".to_string()];
    store.create_group(&graph).expect("create dependency graph");
    store
        .queue_group("group-1")
        .expect("queue dependency graph");
    store
        .claim_task("task-storage", "owner-first", 10_000)
        .expect("claim prerequisite")
        .expect("prerequisite lease");
    assert!(store
        .complete_task(
            "task-storage",
            "owner-first",
            DelegationTaskState::Failed,
            None,
            Some("prerequisite failed"),
        )
        .expect("fail prerequisite"));

    assert!(store
        .claim_tasks("group-1", "owner-second", 2, 10_000)
        .expect("reconcile blocked tasks")
        .is_empty());
    let dependent = store
        .get_task("task-ui")
        .expect("read dependent")
        .expect("dependent task");
    assert_eq!(dependent.state, DelegationTaskState::Cancelled);
    assert_eq!(
        store
            .get_group("group-1")
            .expect("read group")
            .expect("group")
            .state,
        DelegationGroupState::Failed
    );
}

#[test]
fn invalid_dependency_graphs_are_rejected_atomically() {
    for (group_id, dependency, expected) in [
        ("unknown-dependency", "missing", "unknown dependency"),
        ("self-dependency", "ui", "cannot depend on itself"),
    ] {
        let (store, _temp_dir) = create_store();
        let mut graph = input_for(group_id);
        graph.tasks[1].depends_on = vec![dependency.to_string()];
        let error = store
            .create_group(&graph)
            .expect_err("invalid dependency graph must fail");
        assert!(error.to_string().contains(expected), "{error:#}");
        assert!(store.get_group(group_id).expect("read group").is_none());
    }

    let (store, _temp_dir) = create_store();
    let mut cyclic = input_for("cyclic-dependency");
    cyclic.tasks[0].depends_on = vec!["ui".to_string()];
    cyclic.tasks[1].depends_on = vec!["storage".to_string()];
    let error = store
        .create_group(&cyclic)
        .expect_err("cyclic dependency graph must fail");
    assert!(error.to_string().contains("cycle"), "{error:#}");
    assert!(store
        .get_group("cyclic-dependency")
        .expect("read group")
        .is_none());
}

#[test]
fn task_governance_may_narrow_but_not_expand_the_group_ceiling() {
    let (store, _temp_dir) = create_store();
    let mut narrower = input_for("narrow-task-policy");
    let mut task_policy = crate::tools::registry::DelegationPolicy::for_subagent_explore(
        PermissionMode::Supervised,
        Some(4),
    );
    task_policy.execution_tool_allowlist = Some(BTreeSet::from(["read".to_string()]));
    narrower.tasks[0].task_policy = Some(task_policy);
    store
        .create_group(&narrower)
        .expect("narrower task policy should be accepted");

    let (store, _temp_dir) = create_store();
    let mut broader = input_for("broad-task-policy");
    broader.tasks[0].task_policy = Some(
        crate::tools::registry::DelegationPolicy::for_subagent_build(
            PermissionMode::Supervised,
            Some(12),
        ),
    );
    let error = store
        .create_group(&broader)
        .expect_err("broader task policy must fail");
    assert!(
        error
            .to_string()
            .contains("exceeds its immutable group governance"),
        "{error:#}"
    );
    assert!(store
        .get_group("broad-task-policy")
        .expect("read group")
        .is_none());
}

#[test]
fn state_machines_reject_terminal_rewrites() {
    let (store, _temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store
        .transition_group("group-1", DelegationGroupState::Running)
        .expect("start group");
    store
        .transition_task("task-storage", DelegationTaskState::Queued)
        .expect("queue task");
    store
        .transition_task("task-storage", DelegationTaskState::Leased)
        .expect("lease task");
    store
        .transition_task("task-storage", DelegationTaskState::Running)
        .expect("run task");
    store
        .transition_task("task-storage", DelegationTaskState::Complete)
        .expect("complete task");

    store
        .transition_task("task-storage", DelegationTaskState::Failed)
        .expect_err("terminal task must be first-writer-wins");
    let task = store
        .get_task("task-storage")
        .expect("read task")
        .expect("task");
    assert_eq!(task.state, DelegationTaskState::Complete);
}

#[test]
fn quorum_cannot_exceed_logical_task_count() {
    let (store, _temp_dir) = create_store();
    let mut invalid = input();
    invalid.contract.completion_policy = DelegationCompletionPolicy::Quorum { required: 3 };
    store
        .create_group(&invalid)
        .expect_err("oversized quorum must fail");
}

#[test]
fn replay_group_has_exactly_one_live_owner_across_scans() {
    let (store, temp_dir) = create_store();
    store
        .create_group(&replayable_input_for("replay-live", temp_dir.path()))
        .expect("create replayable group");
    store
        .queue_group("replay-live")
        .expect("queue replayable group");
    let peer = DelegationStore::new(
        Database::new(&temp_dir.path().join("delegation.db")).expect("peer db"),
    );
    let owner = store
        .try_claim_replay_owner("replay-live")
        .expect("claim replay owner")
        .expect("first scan owns replay");
    assert!(store
        .replay_owner_is_current("replay-live", &owner)
        .expect("live owner lookup"));
    assert!(peer
        .try_claim_replay_owner("replay-live")
        .expect("duplicate scan")
        .is_none());
}

#[test]
fn replay_group_heartbeat_keeps_duplicate_scans_fenced() {
    let (store, temp_dir) = create_store();
    store
        .create_group(&replayable_input_for("replay-heartbeat", temp_dir.path()))
        .expect("create replayable group");
    store
        .queue_group("replay-heartbeat")
        .expect("queue replayable group");
    let owner = store
        .try_claim_replay_owner("replay-heartbeat")
        .expect("claim replay owner")
        .expect("replay owner");
    assert!(store
        .renew_replay_owner("replay-heartbeat", &owner)
        .expect("renew replay owner"));
    let peer = DelegationStore::new(
        Database::new(&temp_dir.path().join("delegation.db")).expect("peer db"),
    );
    assert!(peer
        .try_claim_replay_owner("replay-heartbeat")
        .expect("duplicate periodic scan")
        .is_none());
}

#[test]
fn expired_replay_group_owner_is_adopted_and_stale_owner_is_fenced() {
    let (store, temp_dir) = create_store();
    store
        .create_group(&replayable_input_for("replay-expired", temp_dir.path()))
        .expect("create replayable group");
    store
        .queue_group("replay-expired")
        .expect("queue replayable group");
    let stale_owner = store
        .try_claim_replay_owner("replay-expired")
        .expect("claim replay owner")
        .expect("initial replay owner");
    Database::new(&temp_dir.path().join("delegation.db"))
        .expect("expiry db")
        .conn()
        .execute(
            "UPDATE delegation_groups SET replay_lease_expires_at_ms = 0
              WHERE delegation_group_id = 'replay-expired'",
            [],
        )
        .expect("expire replay owner");
    let peer = DelegationStore::new(
        Database::new(&temp_dir.path().join("delegation.db")).expect("peer db"),
    );
    let adopted_owner = peer
        .try_claim_replay_owner("replay-expired")
        .expect("adopt expired replay")
        .expect("expired replay must be adopted");
    assert_ne!(adopted_owner, stale_owner);
    assert!(!store
        .renew_replay_owner("replay-expired", &stale_owner)
        .expect("stale renewal is fenced"));
    assert!(!store
        .release_replay_owner("replay-expired", &stale_owner)
        .expect("stale release is fenced"));
    assert!(peer
        .replay_owner_is_current("replay-expired", &adopted_owner)
        .expect("adopted owner lookup"));
}

#[test]
fn durable_capacity_is_a_cross_connection_hard_ceiling_and_fifo_queue() {
    let (store, temp_dir) = create_store();
    let mut group = input();
    group.tasks.push(DelegationTaskSpec {
        delegation_task_id: "task-third".to_string(),
        task_key: "third".to_string(),
        objective: "Third capacity task".to_string(),
        role: DelegatedRunRole::Explore,
        target_scope: Vec::new(),
        max_attempts: 1,
        depends_on: Vec::new(),
        write_intent: Vec::new(),
        task_policy: None,
        writer_mode: DelegationWriterMode::Shared,
        attempt_workspace: None,
        workspace_baseline: None,
        executor_envelope: None,
    });
    group.contract.governance.max_parallelism = 3;
    store.create_group(&group).expect("create group");
    store.queue_group("group-1").expect("queue group");
    for (task, owner) in [
        ("task-storage", "owner-1"),
        ("task-ui", "owner-2"),
        ("task-third", "owner-3"),
    ] {
        store
            .claim_task(task, owner, 10_000)
            .expect("claim")
            .expect("lease");
    }
    let request = capacity_request(
        "resolved-model",
        "read-partition",
        DelegationCapacityClass::ReadOnly,
        None,
    );
    assert!(store
        .try_admit_and_start_task(
            "task-storage",
            "owner-1",
            "resolved-model",
            &request,
            capacity_policy(1),
        )
        .expect("admit first"));

    // A separate connection observes the same ceiling and establishes the
    // durable waiter order.
    let other = DelegationStore::new(
        Database::new(&temp_dir.path().join("delegation.db")).expect("second connection"),
    );
    assert!(!other
        .try_admit_and_start_task(
            "task-ui",
            "owner-2",
            "resolved-model",
            &request,
            capacity_policy(4),
        )
        .expect("queue second"));
    assert!(!store
        .try_admit_and_start_task(
            "task-third",
            "owner-3",
            "resolved-model",
            &request,
            capacity_policy(4),
        )
        .expect("queue third"));
    assert!(store
        .complete_task_with_capacity_feedback(
            "task-storage",
            "owner-1",
            DelegationTaskState::Complete,
            Some(&serde_json::json!({"ok": 1})),
            None,
            DelegationCapacityFeedback::Healthy,
        )
        .expect("complete first"));
    assert!(!store
        .try_admit_and_start_task(
            "task-third",
            "owner-3",
            "resolved-model",
            &request,
            capacity_policy(4),
        )
        .expect("third cannot bypass"));
    assert!(other
        .try_admit_and_start_task(
            "task-ui",
            "owner-2",
            "resolved-model",
            &request,
            capacity_policy(4),
        )
        .expect("second admits first"));
}

#[test]
fn durable_writer_fence_and_domain_cooldown_cross_connections() {
    let (store, temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .claim_task("task-storage", "writer-1", 10_000)
        .expect("claim writer")
        .expect("writer lease");
    store
        .claim_task("task-ui", "writer-2", 10_000)
        .expect("claim contender")
        .expect("contender lease");
    let shared_writer = capacity_request(
        "resolved-model",
        "/workspace",
        DelegationCapacityClass::WriteShared,
        Some("group-1"),
    );
    assert!(store
        .try_admit_and_start_task(
            "task-storage",
            "writer-1",
            "resolved-model",
            &shared_writer,
            capacity_policy(4),
        )
        .expect("admit writer"));
    let other = DelegationStore::new(
        Database::new(&temp_dir.path().join("delegation.db")).expect("second connection"),
    );
    assert!(!other
        .try_admit_and_start_task(
            "task-ui",
            "writer-2",
            "resolved-model",
            &shared_writer,
            capacity_policy(4),
        )
        .expect("writer fenced"));
    assert!(store
        .complete_task_with_capacity_feedback(
            "task-storage",
            "writer-1",
            DelegationTaskState::Failed,
            None,
            Some("429 rate limit"),
            DelegationCapacityFeedback::RateLimited {
                retry_after_ms: Some(10_000),
            },
        )
        .expect("release with cooldown"));
    assert!(!other
        .try_admit_and_start_task(
            "task-ui",
            "writer-2",
            "resolved-model",
            &shared_writer,
            capacity_policy(4),
        )
        .expect("shared cooldown"));
}

#[test]
fn expired_capacity_slot_is_reconciled_and_renewal_tracks_task_lease() {
    let (store, temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .claim_task("task-storage", "owner-expire", 10_000)
        .expect("claim first")
        .expect("first lease");
    store
        .claim_task("task-ui", "owner-next", 10_000)
        .expect("claim second")
        .expect("second lease");
    let request = capacity_request(
        "resolved-model",
        "read-partition",
        DelegationCapacityClass::ReadOnly,
        None,
    );
    assert!(store
        .try_admit_and_start_task(
            "task-storage",
            "owner-expire",
            "resolved-model",
            &request,
            capacity_policy(1),
        )
        .expect("admit first"));
    assert!(store
        .renew_task_lease("task-storage", "owner-expire", 20_000)
        .expect("renew"));
    let db = Database::new(&temp_dir.path().join("delegation.db")).expect("inspect db");
    let expiries: (i64, i64) = db
        .conn()
        .query_row(
            "SELECT tasks.lease_expires_at_ms, capacity.lease_expires_at_ms
               FROM delegation_tasks AS tasks
               JOIN delegation_capacity_leases AS capacity
                 ON capacity.delegation_task_id = tasks.delegation_task_id
              WHERE tasks.delegation_task_id = 'task-storage'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("paired expiries");
    assert_eq!(expiries.0, expiries.1);
    db.conn()
        .execute(
            "UPDATE delegation_tasks SET lease_expires_at_ms = 0
              WHERE delegation_task_id = 'task-storage'",
            [],
        )
        .expect("simulate crash expiry");
    assert!(store
        .try_admit_and_start_task(
            "task-ui",
            "owner-next",
            "resolved-model",
            &request,
            capacity_policy(4),
        )
        .expect("reclaim expired capacity"));
}

#[test]
fn early_success_fences_siblings_and_releases_durable_capacity() {
    let (store, temp_dir) = create_store();
    let mut group = input();
    group.contract.completion_policy = DelegationCompletionPolicy::AnySuccess;
    store.create_group(&group).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .claim_task("task-storage", "winner", 10_000)
        .expect("claim winner")
        .expect("winner lease");
    store
        .claim_task("task-ui", "sibling", 10_000)
        .expect("claim sibling")
        .expect("sibling lease");
    let request = capacity_request(
        "resolved-model",
        "read-partition",
        DelegationCapacityClass::ReadOnly,
        None,
    );
    assert!(store
        .try_admit_and_start_task(
            "task-storage",
            "winner",
            "resolved-model",
            &request,
            capacity_policy(4),
        )
        .expect("start winner"));
    assert!(store
        .try_admit_and_start_task(
            "task-ui",
            "sibling",
            "resolved-model",
            &request,
            capacity_policy(4),
        )
        .expect("start sibling"));

    assert!(store
        .complete_task_with_capacity_feedback(
            "task-storage",
            "winner",
            DelegationTaskState::Complete,
            Some(&serde_json::json!({"ok": true})),
            None,
            DelegationCapacityFeedback::Healthy,
        )
        .expect("complete winner"));
    let settled = store
        .get_group("group-1")
        .expect("group lookup")
        .expect("group");
    assert_eq!(settled.state, DelegationGroupState::ReadyForParent);
    assert_eq!(settled.tasks[1].state, DelegationTaskState::Cancelled);
    assert!(!store
        .renew_task_lease("task-ui", "sibling", 10_000)
        .expect("sibling fenced"));

    let db = Database::new(&temp_dir.path().join("delegation.db")).expect("inspect db");
    let remaining_capacity: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM delegation_capacity_leases
              WHERE delegation_task_id IN ('task-storage', 'task-ui')",
            [],
            |row| row.get(0),
        )
        .expect("capacity count");
    assert_eq!(remaining_capacity, 0);
}

#[test]
fn scheduler_admission_release_does_not_consume_an_execution_attempt() {
    let (store, _temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    let lease = store
        .claim_task("task-storage", "owner-1", 10_000)
        .expect("claim")
        .expect("lease");
    assert_eq!(lease.task.attempt_count, 0);
    assert!(store
        .release_task_claim("task-storage", "owner-1")
        .expect("release admission"));
    assert_eq!(
        store
            .get_task("task-storage")
            .expect("task")
            .expect("task exists")
            .attempt_count,
        0
    );
}

#[test]
fn replayable_task_has_exactly_one_live_execution_owner() {
    let (store, temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    assert!(store
        .claim_task("task-storage", "recovery-owner-a", 10_000)
        .expect("first claim")
        .is_some());
    let competing = DelegationStore::new(
        Database::new(&temp_dir.path().join("delegation.db")).expect("competing connection"),
    );
    assert!(competing
        .claim_task("task-storage", "recovery-owner-b", 10_000)
        .expect("competing claim")
        .is_none());
}

#[test]
fn expired_owner_is_fenced_and_retry_attempts_remain_in_the_ledger() {
    let (store, temp_dir) = create_store();
    let mut retry_input = input();
    retry_input.tasks.truncate(1);
    retry_input.contract.governance.max_parallelism = 1;
    store.create_group(&retry_input).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .claim_task("task-storage", "owner-1", 100)
        .expect("claim first")
        .expect("first lease");
    assert!(store
        .mark_task_running("task-storage", "owner-1", "provider/model")
        .expect("start first"));
    std::thread::sleep(std::time::Duration::from_millis(125));
    assert!(!store
        .renew_task_lease("task-storage", "owner-1", 10_000)
        .expect("stale renew"));
    assert!(!store
        .complete_task(
            "task-storage",
            "owner-1",
            DelegationTaskState::Complete,
            Some(&serde_json::json!({"stale": true})),
            None,
        )
        .expect("stale completion"));

    store
        .claim_task("task-storage", "owner-2", 10_000)
        .expect("claim retry")
        .expect("retry lease");
    assert!(store
        .mark_task_running("task-storage", "owner-2", "provider/model")
        .expect("start retry"));
    assert!(store
        .complete_task(
            "task-storage",
            "owner-2",
            DelegationTaskState::Complete,
            Some(&serde_json::json!({"ok": true})),
            None,
        )
        .expect("complete retry"));

    let db = Database::new(&temp_dir.path().join("delegation.db")).expect("reopen db");
    let attempts = db
        .conn()
        .prepare(
            "SELECT attempt_number, state FROM delegation_attempts
              WHERE delegation_task_id = 'task-storage' ORDER BY attempt_number",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .expect("attempt ledger");
    assert_eq!(
        attempts,
        vec![(1, "expired".to_string()), (2, "complete".to_string())]
    );
}

#[test]
fn final_attempt_lease_expiry_reconciles_the_group() {
    let (store, _temp_dir) = create_store();
    let mut one_attempt = input();
    one_attempt.tasks.truncate(1);
    one_attempt.contract.governance.max_parallelism = 1;
    one_attempt.tasks[0].max_attempts = 1;
    store.create_group(&one_attempt).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .claim_task("task-storage", "owner-final", 100)
        .expect("claim")
        .expect("lease");
    assert!(store
        .mark_task_running("task-storage", "owner-final", "provider/model")
        .expect("start"));
    std::thread::sleep(std::time::Duration::from_millis(125));
    assert!(store
        .claim_task("task-storage", "owner-recovery", 10_000)
        .expect("reconcile expired")
        .is_none());
    assert_eq!(
        store
            .get_group("group-1")
            .expect("group")
            .expect("group exists")
            .state,
        DelegationGroupState::Failed
    );
}

#[test]
fn active_groups_are_retained_ahead_of_the_terminal_snapshot_window() {
    let (store, _temp_dir) = create_store();
    let mut active = input();
    active.delegation_group_id = "group-active".to_string();
    for (index, task) in active.tasks.iter_mut().enumerate() {
        task.delegation_task_id = format!("active-task-{index}");
    }
    store.create_group(&active).expect("create active");
    store.queue_group("group-active").expect("queue active");

    for index in 0..55 {
        let mut terminal = input();
        terminal.delegation_group_id = format!("terminal-{index:02}");
        for (task_index, task) in terminal.tasks.iter_mut().enumerate() {
            task.delegation_task_id = format!("terminal-{index:02}-task-{task_index}");
        }
        store.create_group(&terminal).expect("create terminal");
        store
            .queue_group(&terminal.delegation_group_id)
            .expect("queue terminal");
        store
            .transition_group(&terminal.delegation_group_id, DelegationGroupState::Failed)
            .expect("terminalize group");
    }

    let groups = store
        .list_groups_for_session("session-1", 50)
        .expect("bounded groups");
    assert_eq!(groups.len(), 50);
    assert_eq!(groups[0].delegation_group_id, "group-active");
    assert!(!groups[0].state.is_terminal());
}

#[test]
fn recoverable_groups_include_queued_running_and_ready_but_exclude_terminal_states() {
    let (store, _temp_dir) = create_store();

    let queued = input_for("recover-queued");
    store.create_group(&queued).expect("create queued");
    store.queue_group("recover-queued").expect("queue queued");

    let running = input_for("recover-running");
    store.create_group(&running).expect("create running");
    store.queue_group("recover-running").expect("queue running");
    store
        .transition_group("recover-running", DelegationGroupState::Running)
        .expect("start running");

    let ready = input_for("recover-ready");
    store.create_group(&ready).expect("create ready");
    store.queue_group("recover-ready").expect("queue ready");
    store
        .transition_group("recover-ready", DelegationGroupState::Running)
        .expect("start ready");
    store
        .transition_group("recover-ready", DelegationGroupState::ReadyForParent)
        .expect("mark ready");

    for (group_id, terminal_state) in [
        ("terminal-complete", DelegationGroupState::Complete),
        ("terminal-degraded", DelegationGroupState::Degraded),
        ("terminal-failed", DelegationGroupState::Failed),
        ("terminal-cancelled", DelegationGroupState::Cancelled),
    ] {
        let terminal = input_for(group_id);
        store.create_group(&terminal).expect("create terminal");
        store.queue_group(group_id).expect("queue terminal");
        match terminal_state {
            DelegationGroupState::Complete => {
                store
                    .transition_group(group_id, DelegationGroupState::Running)
                    .expect("start complete");
                store
                    .transition_group(group_id, DelegationGroupState::ReadyForParent)
                    .expect("ready complete");
            }
            DelegationGroupState::Degraded => {
                store
                    .transition_group(group_id, DelegationGroupState::Running)
                    .expect("start degraded");
            }
            DelegationGroupState::Failed | DelegationGroupState::Cancelled => {}
            _ => unreachable!("test only supplies terminal states"),
        }
        store
            .transition_group(group_id, terminal_state)
            .expect("terminalize group");
    }

    let groups = store
        .list_recoverable_groups(100)
        .expect("recoverable inventory");
    let states = groups
        .into_iter()
        .map(|group| (group.delegation_group_id, group.state))
        .collect::<Vec<_>>();
    assert_eq!(
        states,
        vec![
            ("recover-queued".to_string(), DelegationGroupState::Queued),
            ("recover-running".to_string(), DelegationGroupState::Running),
            (
                "recover-ready".to_string(),
                DelegationGroupState::ReadyForParent
            ),
        ]
    );
}

#[test]
fn recoverable_group_order_is_stable_when_timestamps_match() {
    let (store, temp_dir) = create_store();
    for group_id in ["recover-z", "recover-a", "recover-m"] {
        let group = input_for(group_id);
        store.create_group(&group).expect("create group");
        store.queue_group(group_id).expect("queue group");
    }
    let db = Database::new(&temp_dir.path().join("delegation.db")).expect("reopen db");
    db.conn()
        .execute(
            "UPDATE delegation_groups
                SET created_at = '2026-08-08T00:00:00Z',
                    updated_at = CASE delegation_group_id
                        WHEN 'recover-z' THEN '2026-08-08T03:00:00Z'
                        WHEN 'recover-a' THEN '2026-08-08T02:00:00Z'
                        ELSE '2026-08-08T01:00:00Z'
                    END",
            [],
        )
        .expect("normalize timestamps");

    let first = store
        .list_recoverable_groups(100)
        .expect("first inventory")
        .into_iter()
        .map(|group| group.delegation_group_id)
        .collect::<Vec<_>>();
    let second = store
        .list_recoverable_groups(100)
        .expect("second inventory")
        .into_iter()
        .map(|group| group.delegation_group_id)
        .collect::<Vec<_>>();
    assert_eq!(first, vec!["recover-a", "recover-m", "recover-z"]);
    assert_eq!(second, first);
}

#[test]
fn synthesis_lease_reclaims_expired_owner_and_fences_stale_publication() {
    let (store, temp_dir) = create_store();
    store.create_group(&input()).expect("create group");
    store.queue_group("group-1").expect("queue group");
    store
        .transition_group("group-1", DelegationGroupState::Running)
        .expect("start group");
    store
        .transition_group("group-1", DelegationGroupState::ReadyForParent)
        .expect("settle tasks");

    let first = store
        .claim_synthesis("group-1", "synth-owner-1", 10_000)
        .expect("claim synthesis")
        .expect("first synthesis lease");
    assert_eq!(first.group.state, DelegationGroupState::Synthesizing);
    assert_eq!(first.group.synthesis_attempt_count, 1);
    assert!(store
        .claim_synthesis("group-1", "synth-owner-2", 10_000)
        .expect("competing claim")
        .is_none());

    Database::new(&temp_dir.path().join("delegation.db"))
        .expect("reopen db")
        .conn()
        .execute(
            "UPDATE delegation_groups SET synthesis_lease_expires_at_ms = 0
             WHERE delegation_group_id = 'group-1'",
            [],
        )
        .expect("expire synthesis lease deterministically");
    let replacement = store
        .claim_synthesis("group-1", "synth-owner-2", 10_000)
        .expect("reclaim synthesis")
        .expect("replacement synthesis lease");
    assert_eq!(replacement.group.synthesis_attempt_count, 2);
    assert!(!store
        .complete_synthesis("group-1", "synth-owner-1", DelegationGroupState::Complete,)
        .expect("stale completion is fenced"));
    assert!(store
        .renew_synthesis_lease("group-1", "synth-owner-2", 10_000)
        .expect("renew replacement"));
    assert!(store
        .complete_synthesis("group-1", "synth-owner-2", DelegationGroupState::Complete,)
        .expect("complete replacement"));
    let terminal = store
        .get_group("group-1")
        .expect("read group")
        .expect("group exists");
    assert_eq!(terminal.state, DelegationGroupState::Complete);
    assert!(terminal.synthesis_owner_id.is_none());
    assert!(terminal.synthesis_lease_expires_at_ms.is_none());

    let events = store
        .list_session_events_after("session-1", 0, 100)
        .expect("events");
    assert!(events.iter().any(|event| {
        event.payload.get("reason").and_then(Value::as_str) == Some("synthesis_lease_reclaimed")
    }));
}

#[test]
fn recoverable_inventory_reconciles_expired_attempts_before_returning() {
    let (store, _temp_dir) = create_store();

    let mut retry = input_for("recover-retry");
    retry.tasks.truncate(1);
    retry.contract.governance.max_parallelism = 1;
    let retry_task_id = retry.tasks[0].delegation_task_id.clone();
    store.create_group(&retry).expect("create retry");
    store.queue_group("recover-retry").expect("queue retry");
    store
        .claim_task(&retry_task_id, "owner-retry", 100)
        .expect("claim retry")
        .expect("retry lease");
    assert!(store
        .mark_task_running(&retry_task_id, "owner-retry", "provider/model")
        .expect("start retry"));

    let mut exhausted = input_for("recover-exhausted");
    exhausted.tasks.truncate(1);
    exhausted.contract.governance.max_parallelism = 1;
    exhausted.tasks[0].max_attempts = 1;
    let exhausted_task_id = exhausted.tasks[0].delegation_task_id.clone();
    store.create_group(&exhausted).expect("create exhausted");
    store
        .queue_group("recover-exhausted")
        .expect("queue exhausted");
    store
        .claim_task(&exhausted_task_id, "owner-exhausted", 100)
        .expect("claim exhausted")
        .expect("exhausted lease");
    assert!(store
        .mark_task_running(&exhausted_task_id, "owner-exhausted", "provider/model")
        .expect("start exhausted"));

    std::thread::sleep(std::time::Duration::from_millis(125));
    let groups = store
        .list_recoverable_groups(100)
        .expect("recoverable inventory");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].delegation_group_id, "recover-retry");
    assert_eq!(groups[0].state, DelegationGroupState::Running);
    assert_eq!(groups[0].tasks[0].state, DelegationTaskState::Queued);
    assert_eq!(groups[0].tasks[0].attempt_count, 1);
    assert_eq!(
        store
            .get_group("recover-exhausted")
            .expect("read exhausted")
            .expect("exhausted exists")
            .state,
        DelegationGroupState::Failed
    );
}
