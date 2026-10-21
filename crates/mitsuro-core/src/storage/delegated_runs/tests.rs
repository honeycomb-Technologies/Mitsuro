use chrono::Utc;
use rusqlite::params;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

use super::*;
use crate::agent::subagent::AgentCapability;
use crate::agent::DelegatedRunStage;
use crate::storage::Database;
use crate::tools::registry::{DelegationPolicy, PermissionMode};

fn create_store() -> (DelegatedRunStore, TempDir) {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("delegated-runs.db");
    let db = Database::new(&db_path).expect("db");
    let now = Utc::now().to_rfc3339();
    db.conn()
        .execute(
            "INSERT INTO sessions (id, title, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params!["session-1", "Delegated Run Test", now, now],
        )
        .expect("seed session");
    (DelegatedRunStore::new(db), temp_dir)
}

fn scope(label: &str, path: &str, kind: &str) -> DelegatedRunScope {
    DelegatedRunScope {
        label: label.to_string(),
        path: path.to_string(),
        kind: kind.to_string(),
    }
}

#[test]
fn active_workspace_writer_fences_duplicate_graph_until_terminal() {
    let (store, _tmp) = create_store();
    let capabilities = std::collections::BTreeSet::from([
        AgentCapability::Read,
        AgentCapability::Write,
        AgentCapability::Execute,
    ]);
    let start = |run_id: &str, tool_id: &str, task_label: &str| DelegatedRunStartInput {
        delegated_run_id: run_id.to_string(),
        parent_session_id: "session-1".to_string(),
        parent_tool_call_id: Some(tool_id.to_string()),
        role: DelegatedRunRole::Build,
        stage: DelegatedRunStage::Created,
        provider: Some("test".to_string()),
        model: Some("test-model".to_string()),
        resumable: true,
        resumed_from_run_id: None,
        target_scope: vec![
            scope("workspace", "/tmp/project", "workspace"),
            scope(task_label, ".", "task"),
        ],
    };

    assert_eq!(
        store
            .create_run_with_child_contract(
                &start("writer-1", "tool-1", "first graph"),
                Some("first"),
                &capabilities,
            )
            .expect("create first writer"),
        DelegatedRunCreateOutcome::Created
    );
    assert_eq!(
        store
            .create_run_with_child_contract(
                &start("writer-2", "tool-2", "restated graph"),
                Some("second"),
                &capabilities,
            )
            .expect("fence duplicate writer"),
        DelegatedRunCreateOutcome::ExistingActiveWorkspaceWriter {
            delegated_run_id: "writer-1".to_string(),
            workspace_path: "/tmp/project".to_string(),
        }
    );
    assert!(store
        .get_run("writer-2")
        .expect("read rejected writer")
        .is_none());

    let read_only = DelegatedRunStartInput {
        delegated_run_id: "reader-1".to_string(),
        role: DelegatedRunRole::Explore,
        ..start("unused-reader-template", "tool-reader", "reader")
    };
    assert_eq!(
        store
            .create_run_with_child_contract(
                &read_only,
                Some("reader"),
                &std::collections::BTreeSet::from([AgentCapability::Read]),
            )
            .expect("allow concurrent reader"),
        DelegatedRunCreateOutcome::Created
    );
    assert_eq!(
        store
            .create_run_with_child_contract(
                &DelegatedRunStartInput {
                    delegated_run_id: "writer-other-workspace".to_string(),
                    target_scope: vec![scope("workspace", "/tmp/other", "workspace")],
                    ..start("unused-writer-template", "tool-other", "other workspace")
                },
                Some("other workspace"),
                &capabilities,
            )
            .expect("allow a writer for a distinct workspace"),
        DelegatedRunCreateOutcome::Created
    );

    store
        .finalize_run(
            "writer-1",
            DelegatedRunStage::Complete,
            &serde_json::json!({"summary": "done"}),
            None,
            false,
        )
        .expect("finish first writer");
    assert_eq!(
        store
            .create_run_with_child_contract(
                &start("writer-3", "tool-3", "follow-up graph"),
                Some("follow-up"),
                &capabilities,
            )
            .expect("create terminal follow-up writer"),
        DelegatedRunCreateOutcome::Created
    );
}

#[test]
fn unadmitted_compatibility_run_is_deleted_without_emitting_a_cancelled_wake() {
    let (store, _tmp) = create_store();
    let mut lease = DelegatedRunLease::new(store);
    let run_id = "unadmitted-background";
    let outcome = lease
        .create_background_run(&DelegatedRunStartInput {
            delegated_run_id: run_id.to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-unadmitted".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Created,
            provider: Some("test".to_string()),
            model: Some("test-model".to_string()),
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("workspace", ".", "workspace")],
        })
        .expect("prepare compatibility run");
    assert_eq!(outcome, DelegatedRunCreateOutcome::Created);

    assert!(lease
        .discard_unadmitted_run(run_id)
        .expect("discard unadmitted run"));
    assert!(lease.get_run(run_id).expect("read discarded run").is_none());
    drop(lease);

    let db = Database::new(&_tmp.path().join("delegated-runs.db")).expect("reopen db");
    let pending_wakes: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM delegated_runs WHERE wake_parent = 1",
            [],
            |row| row.get(0),
        )
        .expect("count pending wakes");
    assert_eq!(pending_wakes, 0);
}

#[test]
fn round_trip_persisted_delegated_run() {
    let (store, _tmp) = create_store();
    let delegated_run_id = "run-1".to_string();
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: delegated_run_id.clone(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-1".to_string()),
            role: DelegatedRunRole::Explore,
            stage: DelegatedRunStage::Created,
            provider: Some("minimax".to_string()),
            model: Some("MiniMax-M2.5".to_string()),
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope(
                "storage",
                "crates/mitsuro-core/src/storage",
                "directory",
            )],
        })
        .expect("create run");

    store
        .update_snapshot(
            &delegated_run_id,
            DelegatedRunStage::Running,
            &DelegatedRunSnapshot {
                stage: DelegatedRunStage::Running,
                agents: vec![DelegatedRunAgentSnapshot {
                    task_id: "dir-0".to_string(),
                    agent_name: "storage".to_string(),
                    status: "running".to_string(),
                    tool_count: 3,
                    tokens: 120,
                    current_action: Some("reading mod.rs".to_string()),
                    completion_summary: None,
                    lines_added: 0,
                    lines_removed: 0,
                    completed_plan_task: None,
                }],
            },
        )
        .expect("update snapshot");

    store
        .finalize_run(
            &delegated_run_id,
            DelegatedRunStage::Complete,
            &serde_json::json!({
                "human_review": "Storage owns session persistence and runtime traces."
            }),
            Some("Storage owns session persistence and runtime traces."),
            true,
        )
        .expect("finalize run");

    let record = store
        .get_run(&delegated_run_id)
        .expect("get run")
        .expect("record exists");
    assert_eq!(record.stage, DelegatedRunStage::Complete);
    assert!(!record.wake_parent);
    assert_eq!(record.role, DelegatedRunRole::Explore);
    assert!(record.resumable);
    assert_eq!(record.target_scope.len(), 1);
    assert_eq!(
        record.snapshot.as_ref().unwrap().agents[0].agent_name,
        "storage"
    );
    assert_eq!(
        record.human_review.as_deref(),
        Some("Storage owns session persistence and runtime traces.")
    );
}

#[test]
fn background_round_trip_preserves_name_capabilities_and_wake_intent() {
    let (store, _tmp) = create_store();
    let capabilities = [AgentCapability::Execute].into_iter().collect();
    store
        .create_background_run_with_child_contract(
            &DelegatedRunStartInput {
                delegated_run_id: "run-execute-only".to_string(),
                parent_session_id: "session-1".to_string(),
                parent_tool_call_id: Some("tool-execute".to_string()),
                role: DelegatedRunRole::Explore,
                stage: DelegatedRunStage::Created,
                provider: None,
                model: None,
                resumable: true,
                resumed_from_run_id: None,
                target_scope: vec![scope("project", ".", "project")],
            },
            Some("focused validation"),
            &capabilities,
        )
        .expect("create contracted run");

    let record = store
        .get_run("run-execute-only")
        .expect("get run")
        .expect("record exists");
    assert_eq!(record.stage, DelegatedRunStage::Created);
    assert_eq!(record.child_name.as_deref(), Some("focused validation"));
    assert_eq!(record.capabilities, capabilities);
    assert_eq!(record.effective_capabilities(), capabilities);
    assert!(record.wake_parent);
}

#[test]
fn second_continuation_returns_the_existing_durable_descendant() {
    let (store, _tmp) = create_store();
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "origin".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-origin".to_string()),
            role: DelegatedRunRole::Explore,
            stage: DelegatedRunStage::Complete,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("project", ".", "project")],
        })
        .expect("create origin");
    let first = DelegatedRunStartInput {
        delegated_run_id: "continuation-1".to_string(),
        parent_session_id: "session-1".to_string(),
        parent_tool_call_id: Some("tool-continuation-1".to_string()),
        role: DelegatedRunRole::Explore,
        stage: DelegatedRunStage::Created,
        provider: None,
        model: None,
        resumable: true,
        resumed_from_run_id: Some("origin".to_string()),
        target_scope: vec![scope("project", ".", "project")],
    };
    assert_eq!(
        store.create_run(&first).expect("create first continuation"),
        DelegatedRunCreateOutcome::Created
    );

    let second = DelegatedRunStartInput {
        delegated_run_id: "continuation-2".to_string(),
        parent_tool_call_id: Some("tool-continuation-2".to_string()),
        ..first
    };
    assert_eq!(
        store
            .create_run(&second)
            .expect("duplicate continuation should resolve"),
        DelegatedRunCreateOutcome::ExistingContinuation {
            delegated_run_id: "continuation-1".to_string(),
            resumed_from_run_id: "origin".to_string(),
        }
    );
    assert!(store
        .get_run("continuation-2")
        .expect("load rejected continuation")
        .is_none());
}

#[test]
fn concurrent_continuation_creation_launches_exactly_one_descendant() {
    let (store, temp) = create_store();
    let db_path = temp.path().join("delegated-runs.db");
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "concurrent-origin".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-origin".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Complete,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("project", ".", "project")],
        })
        .expect("create origin");

    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for index in 0..2 {
        let db_path = db_path.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let store = DelegatedRunStore::new(Database::new(&db_path).expect("continuation db"));
            barrier.wait();
            store.create_run(&DelegatedRunStartInput {
                delegated_run_id: format!("concurrent-child-{index}"),
                parent_session_id: "session-1".to_string(),
                parent_tool_call_id: Some(format!("tool-child-{index}")),
                role: DelegatedRunRole::Build,
                stage: DelegatedRunStage::Created,
                provider: None,
                model: None,
                resumable: true,
                resumed_from_run_id: Some("concurrent-origin".to_string()),
                target_scope: vec![scope("project", ".", "project")],
            })
        }));
    }
    barrier.wait();
    let outcomes = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .expect("continuation thread")
                .expect("continuation result")
        })
        .collect::<Vec<_>>();

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, DelegatedRunCreateOutcome::Created))
            .count(),
        1
    );
    let winner = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            DelegatedRunCreateOutcome::ExistingContinuation {
                delegated_run_id, ..
            } => Some(delegated_run_id.as_str()),
            DelegatedRunCreateOutcome::Created => None,
            DelegatedRunCreateOutcome::ExistingActiveWorkspaceWriter { .. } => {
                panic!("read-only continuation unexpectedly hit the writer fence")
            }
        })
        .expect("one caller should observe the winner");
    let descendants = store
        .list_runs_for_session("session-1", 10)
        .expect("list descendants")
        .into_iter()
        .filter(|run| run.resumed_from_run_id.as_deref() == Some("concurrent-origin"))
        .collect::<Vec<_>>();
    assert_eq!(descendants.len(), 1);
    assert_eq!(descendants[0].delegated_run_id, winner);
}

#[test]
fn hydration_summaries_keep_only_the_newest_run_per_parent_tool() {
    let (store, _tmp) = create_store();
    for (run_id, tool_call_id, role) in [
        ("run-old", "tool-shared", DelegatedRunRole::Explore),
        ("run-new", "tool-shared", DelegatedRunRole::Build),
        ("run-other", "tool-other", DelegatedRunRole::Verifier),
    ] {
        store
            .create_run(&DelegatedRunStartInput {
                delegated_run_id: run_id.to_string(),
                parent_session_id: "session-1".to_string(),
                parent_tool_call_id: Some(tool_call_id.to_string()),
                role,
                stage: DelegatedRunStage::Created,
                provider: None,
                model: None,
                resumable: true,
                resumed_from_run_id: None,
                target_scope: vec![scope("project", ".", "project")],
            })
            .expect("create hydration run");
    }
    store
        .db
        .conn()
        .execute(
            "UPDATE delegated_runs SET updated_at = CASE delegated_run_id
                WHEN 'run-old' THEN '2026-01-01T00:00:00Z'
                WHEN 'run-new' THEN '2026-01-03T00:00:00Z'
                ELSE '2026-01-02T00:00:00Z' END",
            [],
        )
        .expect("order hydration runs");

    let summaries = store
        .list_run_summaries_for_session("session-1", 10)
        .expect("list hydration summaries");
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].delegated_run_id, "run-new");
    assert_eq!(summaries[0].parent_tool_call_id, "tool-shared");
    assert_eq!(summaries[0].role, DelegatedRunRole::Build);
    assert_eq!(
        summaries[0].effective_capabilities(),
        [
            AgentCapability::Read,
            AgentCapability::Write,
            AgentCapability::Execute,
        ]
        .into_iter()
        .collect()
    );
    assert!(summaries
        .iter()
        .all(|summary| summary.delegated_run_id != "run-old"));
}

#[test]
fn empty_contract_uses_legacy_role_fallback_only() {
    let (store, _tmp) = create_store();
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "legacy-build".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: None,
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Complete,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("project", ".", "project")],
        })
        .expect("create legacy row");

    let record = store
        .get_run("legacy-build")
        .expect("get run")
        .expect("record exists");
    assert!(record.capabilities.is_empty());
    assert_eq!(
        record.effective_capabilities(),
        [
            AgentCapability::Read,
            AgentCapability::Write,
            AgentCapability::Execute,
        ]
        .into_iter()
        .collect()
    );
}

#[test]
fn finalized_background_artifact_preserves_delegation_policy_metadata() {
    let (store, _tmp) = create_store();
    let delegated_run_id = "run-policy".to_string();
    let policy = DelegationPolicy::for_subagent_build(PermissionMode::Supervised, Some(12));

    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: delegated_run_id.clone(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-1".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Created,
            provider: Some("openai".to_string()),
            model: Some("gpt-test".to_string()),
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("main", ".", "project")],
        })
        .expect("create run");

    store
        .finalize_run(
            &delegated_run_id,
            DelegatedRunStage::Complete,
            &serde_json::json!({
                "delegated_run_id": delegated_run_id,
                "outcome": "success",
                "delegation_policy": policy.audit_json(),
            }),
            Some("Build complete"),
            false,
        )
        .expect("finalize run");

    let record = store
        .get_run("run-policy")
        .expect("get run")
        .expect("record exists");
    let artifact = record.artifact.expect("artifact persisted");
    assert_eq!(artifact["delegation_policy"]["surface"], "subagent_build");
    assert_eq!(
        artifact["delegation_policy"]["permission_mode"],
        "supervised"
    );
    assert_eq!(artifact["delegation_policy"]["max_turns"], 12);
    assert_eq!(artifact["delegation_policy"]["read_only_only"], false);
}

#[test]
fn find_related_run_matches_scope_key() {
    let (store, _tmp) = create_store();
    let scope = vec![scope("agent", "crates/mitsuro-core/src/agent", "directory")];
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-1".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-1".to_string()),
            role: DelegatedRunRole::Explore,
            stage: DelegatedRunStage::Complete,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: scope.clone(),
        })
        .expect("create run");

    let found = store
        .find_related_run("session-1", DelegatedRunRole::Explore, &scope)
        .expect("query related")
        .expect("related run");
    assert_eq!(found.delegated_run_id, "run-1");
}

#[test]
fn cancelled_run_cannot_be_overwritten_by_late_child_finalization() {
    let (store, _tmp) = create_store();
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-cancel".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-1".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("main", ".", "project")],
        })
        .expect("create run");

    store
        .finalize_run(
            "run-cancel",
            DelegatedRunStage::Cancelled,
            &serde_json::json!({"outcome": "cancelled"}),
            Some("parent interrupt"),
            true,
        )
        .expect("cancel run");
    store
        .finalize_run(
            "run-cancel",
            DelegatedRunStage::Failed,
            &serde_json::json!({"outcome": "failed"}),
            Some("late child result"),
            true,
        )
        .expect("late finalization is ignored");

    let record = store
        .get_run("run-cancel")
        .expect("load run")
        .expect("record");
    assert_eq!(record.stage, DelegatedRunStage::Cancelled);
    assert_eq!(record.artifact.unwrap()["outcome"], "cancelled");
}

#[test]
fn late_progress_snapshot_cannot_reopen_a_terminal_run() {
    let (store, _temp) = create_store();
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-terminal-snapshot".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-terminal-snapshot".to_string()),
            role: DelegatedRunRole::Explore,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("main", ".", "project")],
        })
        .expect("create run");
    store
        .finalize_run(
            "run-terminal-snapshot",
            DelegatedRunStage::Complete,
            &serde_json::json!({"outcome": "success"}),
            Some("complete"),
            true,
        )
        .expect("terminal result should persist");

    store
        .update_snapshot(
            "run-terminal-snapshot",
            DelegatedRunStage::Running,
            &DelegatedRunSnapshot {
                stage: DelegatedRunStage::Running,
                agents: Vec::new(),
            },
        )
        .expect("stale progress should be a harmless terminal loser");

    let record = store
        .get_run("run-terminal-snapshot")
        .expect("read run")
        .expect("run exists");
    assert_eq!(record.stage, DelegatedRunStage::Complete);
    assert_eq!(
        record.artifact,
        Some(serde_json::json!({"outcome": "success"}))
    );
}

#[test]
fn concurrent_terminal_writers_preserve_the_first_durable_winner() {
    let (store, temp) = create_store();
    let db_path = temp.path().join("delegated-runs.db");

    for index in 0..12 {
        let run_id = format!("run-race-{index}");
        store
            .create_run(&DelegatedRunStartInput {
                delegated_run_id: run_id.clone(),
                parent_session_id: "session-1".to_string(),
                parent_tool_call_id: Some(format!("tool-{index}")),
                role: DelegatedRunRole::Build,
                stage: DelegatedRunStage::Running,
                provider: None,
                model: None,
                resumable: true,
                resumed_from_run_id: None,
                target_scope: vec![scope("main", ".", "project")],
            })
            .expect("create raced run");

        let barrier = Arc::new(Barrier::new(3));
        let completion_path = db_path.clone();
        let completion_id = run_id.clone();
        let completion_barrier = Arc::clone(&barrier);
        let completion = std::thread::spawn(move || {
            let store = DelegatedRunStore::new(
                Database::new(&completion_path).expect("completion database"),
            );
            completion_barrier.wait();
            store.finalize_run(
                &completion_id,
                DelegatedRunStage::Complete,
                &serde_json::json!({"outcome": "complete"}),
                Some("child completed"),
                true,
            )
        });

        let cancel_path = db_path.clone();
        let cancel_id = run_id.clone();
        let cancel_barrier = Arc::clone(&barrier);
        let cancel = std::thread::spawn(move || {
            let store =
                DelegatedRunStore::new(Database::new(&cancel_path).expect("cancel database"));
            cancel_barrier.wait();
            store.finalize_run(
                &cancel_id,
                DelegatedRunStage::Cancelled,
                &serde_json::json!({"outcome": "cancelled"}),
                Some("parent interrupt"),
                true,
            )
        });

        barrier.wait();
        completion
            .join()
            .expect("completion thread")
            .expect("completion finalization");
        cancel
            .join()
            .expect("cancel thread")
            .expect("cancel finalization");

        let record = store
            .get_run(&run_id)
            .expect("load raced run")
            .expect("raced run exists");
        let artifact = record.artifact.expect("winning artifact");
        match record.stage {
            DelegatedRunStage::Complete => {
                assert_eq!(artifact["outcome"], "complete");
                assert_eq!(record.human_review.as_deref(), Some("child completed"));
            }
            DelegatedRunStage::Cancelled => {
                assert_eq!(artifact["outcome"], "cancelled");
                assert_eq!(record.human_review.as_deref(), Some("parent interrupt"));
            }
            stage => panic!("unexpected terminal winner: {stage:?}"),
        }
        assert!(record.completed_at.is_some());
    }
}

#[test]
fn completed_run_cannot_be_overwritten_by_late_interrupt() {
    let (store, _tmp) = create_store();
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-complete".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-complete".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("main", ".", "project")],
        })
        .expect("create run");

    store
        .finalize_run(
            "run-complete",
            DelegatedRunStage::Complete,
            &serde_json::json!({"outcome": "complete"}),
            Some("child completed"),
            true,
        )
        .expect("complete run");
    store
        .finalize_run(
            "run-complete",
            DelegatedRunStage::Cancelled,
            &serde_json::json!({"outcome": "cancelled"}),
            Some("late parent interrupt"),
            true,
        )
        .expect("late interrupt observes terminal winner");

    let record = store
        .get_run("run-complete")
        .expect("load run")
        .expect("record");
    assert_eq!(record.stage, DelegatedRunStage::Complete);
    assert_eq!(record.artifact.unwrap()["outcome"], "complete");
    assert_eq!(record.human_review.as_deref(), Some("child completed"));
}

#[test]
fn finalizing_unknown_run_is_an_error() {
    let (store, _tmp) = create_store();
    let error = store
        .finalize_run(
            "missing-run",
            DelegatedRunStage::Failed,
            &serde_json::json!({"outcome": "failed"}),
            Some("missing"),
            false,
        )
        .expect_err("unknown run must not look finalized");
    assert!(error.to_string().contains("does not exist"));
}

#[test]
fn dropping_armed_lease_cancels_nonterminal_run() {
    let (store, temp) = create_store();
    let db_path = temp.path().join("delegated-runs.db");
    let mut lease = DelegatedRunLease::new(store);
    lease
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-abandoned".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-abandoned".to_string()),
            role: DelegatedRunRole::Explore,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("main", ".", "project")],
        })
        .expect("create run");
    drop(lease);

    let store = DelegatedRunStore::new(Database::new(&db_path).expect("reopen database"));
    let record = store
        .get_run("run-abandoned")
        .expect("load abandoned run")
        .expect("abandoned run exists");
    assert_eq!(record.stage, DelegatedRunStage::Cancelled);
    assert!(record.resumable);
    assert_eq!(
        record.artifact.as_ref().unwrap()["outcome_reason"],
        "caller_aborted_before_terminal"
    );
    assert_eq!(record.artifact.as_ref().unwrap()["quiescent"], false);
    assert_eq!(
        record.artifact.as_ref().unwrap()["side_effects_may_have_occurred"],
        true
    );
    assert!(record.completed_at.is_some());
}

#[test]
fn disarmed_lease_preserves_authoritative_completion() {
    let (store, temp) = create_store();
    let db_path = temp.path().join("delegated-runs.db");
    let mut lease = DelegatedRunLease::new(store);
    lease
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-disarmed".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-disarmed".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("main", ".", "project")],
        })
        .expect("create run");
    lease
        .finalize_run(
            "run-disarmed",
            DelegatedRunStage::Complete,
            &serde_json::json!({"outcome": "complete"}),
            Some("child completed"),
            true,
        )
        .expect("finalize run");
    assert!(lease.disarm("run-disarmed"));

    drop(lease);

    let store = DelegatedRunStore::new(Database::new(&db_path).expect("reopen database"));
    let record = store
        .get_run("run-disarmed")
        .expect("load completed run")
        .expect("completed run exists");
    assert_eq!(record.stage, DelegatedRunStage::Complete);
    assert_eq!(record.artifact.unwrap()["outcome"], "complete");
}

#[test]
fn armed_lease_cannot_overwrite_terminal_winner() {
    let (store, temp) = create_store();
    let db_path = temp.path().join("delegated-runs.db");
    let mut lease = DelegatedRunLease::new(store);
    lease
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "run-terminal-winner".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-terminal-winner".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope("main", ".", "project")],
        })
        .expect("create run");
    lease
        .finalize_run(
            "run-terminal-winner",
            DelegatedRunStage::Failed,
            &serde_json::json!({"outcome": "failed", "outcome_reason": "provider_error"}),
            Some("provider failed"),
            true,
        )
        .expect("finalize run");

    // Intentionally leave the lease armed. Its Drop finalizer must observe,
    // rather than overwrite, the first terminal writer.
    drop(lease);

    let store = DelegatedRunStore::new(Database::new(&db_path).expect("reopen database"));
    let record = store
        .get_run("run-terminal-winner")
        .expect("load terminal winner")
        .expect("terminal winner exists");
    assert_eq!(record.stage, DelegatedRunStage::Failed);
    assert_eq!(record.artifact.unwrap()["outcome_reason"], "provider_error");
    assert_eq!(record.human_review.as_deref(), Some("provider failed"));
}

#[test]
fn wake_scan_includes_background_completion_and_abnormal_drop_only() {
    let (store, temp) = create_store();
    let background = DelegatedRunStartInput {
        delegated_run_id: "background-failed".to_string(),
        parent_session_id: "session-1".to_string(),
        parent_tool_call_id: Some("tool-background".to_string()),
        role: DelegatedRunRole::Explore,
        stage: DelegatedRunStage::Running,
        provider: None,
        model: None,
        resumable: true,
        resumed_from_run_id: None,
        target_scope: vec![scope(
            "workspace",
            temp.path().to_string_lossy().as_ref(),
            "workspace",
        )],
    };
    store
        .create_background_run(&background)
        .expect("create background run");
    store
        .finalize_run(
            "background-failed",
            DelegatedRunStage::Failed,
            &serde_json::json!({"outcome": "failed"}),
            Some("background failed"),
            true,
        )
        .expect("finalize background run");

    let mut explicit_cancel = background.clone();
    explicit_cancel.delegated_run_id = "background-explicit-cancel".to_string();
    store
        .create_background_run(&explicit_cancel)
        .expect("create explicit cancellation fixture");
    store
        .finalize_run(
            "background-explicit-cancel",
            DelegatedRunStage::Cancelled,
            &serde_json::json!({"outcome": "cancelled", "outcome_reason": "cancelled"}),
            Some("cancelled by user"),
            true,
        )
        .expect("finalize explicit cancellation");

    let mut foreground = background.clone();
    foreground.delegated_run_id = "foreground-failed".to_string();
    store
        .create_run(&foreground)
        .expect("create foreground run");
    store
        .finalize_run(
            "foreground-failed",
            DelegatedRunStage::Failed,
            &serde_json::json!({"outcome": "failed"}),
            Some("foreground failed"),
            true,
        )
        .expect("finalize foreground run");

    let db_path = temp.path().join("delegated-runs.db");
    let mut lease = DelegatedRunLease::new(store);
    let mut abnormal = background;
    abnormal.delegated_run_id = "background-abnormal".to_string();
    lease
        .create_background_run(&abnormal)
        .expect("create abnormal fixture");
    drop(lease);

    let store = DelegatedRunStore::new(Database::new(&db_path).expect("reopen database"));
    let wake_ids = store
        .list_unqueued_parent_wakes()
        .expect("list wakeable completions")
        .into_iter()
        .map(|record| record.delegated_run_id)
        .collect::<Vec<_>>();
    assert_eq!(
        wake_ids,
        vec![
            "background-failed".to_string(),
            "background-abnormal".to_string(),
        ]
    );
}

#[test]
fn expired_background_host_lease_terminalizes_once_and_wakes_parent() {
    let (store, temp) = create_store();
    let run = DelegatedRunStartInput {
        delegated_run_id: "expired-host".to_string(),
        parent_session_id: "session-1".to_string(),
        parent_tool_call_id: Some("tool-expired-host".to_string()),
        role: DelegatedRunRole::Build,
        stage: DelegatedRunStage::Running,
        provider: None,
        model: None,
        resumable: true,
        resumed_from_run_id: None,
        target_scope: vec![scope(
            "workspace",
            temp.path().to_string_lossy().as_ref(),
            "workspace",
        )],
    };
    store
        .create_background_run(&run)
        .expect("create leased background run");
    let (owner, initial_expiry): (Option<String>, Option<i64>) = store
        .db
        .conn()
        .query_row(
            "SELECT host_owner_id, host_lease_expires_at_ms FROM delegated_runs WHERE delegated_run_id = ?1",
            params![run.delegated_run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read host lease");
    assert!(owner.is_some());
    assert!(initial_expiry.is_some_and(|expiry| expiry > Utc::now().timestamp_millis()));

    store
        .db
        .conn()
        .execute(
            "UPDATE delegated_runs SET host_lease_expires_at_ms = ?2 WHERE delegated_run_id = ?1",
            params![run.delegated_run_id, Utc::now().timestamp_millis() - 5_000],
        )
        .expect("expire host lease");
    assert_eq!(
        store
            .expire_stale_background_host_leases()
            .expect("recover expired host lease"),
        vec!["expired-host".to_string()]
    );
    assert!(store
        .expire_stale_background_host_leases()
        .expect("repeat recovery is idempotent")
        .is_empty());

    let record = store
        .get_run("expired-host")
        .expect("load recovered run")
        .expect("recovered run exists");
    assert_eq!(record.stage, DelegatedRunStage::Cancelled);
    assert_eq!(
        record.artifact.as_ref().unwrap()["outcome_reason"],
        "background_host_lease_expired"
    );
    assert!(record.should_wake_parent());
}

#[test]
fn fresh_or_legacy_unowned_background_rows_are_never_stolen() {
    let (store, temp) = create_store();
    let run = DelegatedRunStartInput {
        delegated_run_id: "fresh-host".to_string(),
        parent_session_id: "session-1".to_string(),
        parent_tool_call_id: None,
        role: DelegatedRunRole::Explore,
        stage: DelegatedRunStage::Running,
        provider: None,
        model: None,
        resumable: true,
        resumed_from_run_id: None,
        target_scope: vec![scope(
            "workspace",
            temp.path().to_string_lossy().as_ref(),
            "workspace",
        )],
    };
    store
        .create_background_run(&run)
        .expect("create fresh leased run");
    let mut legacy = run;
    legacy.delegated_run_id = "legacy-unowned".to_string();
    store
        .create_background_run(&legacy)
        .expect("create legacy fixture");
    store
        .db
        .conn()
        .execute(
            "UPDATE delegated_runs SET host_owner_id = NULL, host_lease_expires_at_ms = NULL WHERE delegated_run_id = ?1",
            params![legacy.delegated_run_id],
        )
        .expect("simulate migration-era unowned row");

    assert!(store
        .expire_stale_background_host_leases()
        .expect("scan active and legacy rows")
        .is_empty());
    assert_eq!(
        store.get_run("fresh-host").unwrap().unwrap().stage,
        DelegatedRunStage::Running
    );
    assert_eq!(
        store.get_run("legacy-unowned").unwrap().unwrap().stage,
        DelegatedRunStage::Running
    );
}

#[test]
fn expired_owner_cannot_publish_completion_or_drop_overwrite_recovery() {
    let (store, temp) = create_store();
    let db_path = temp.path().join("delegated-runs.db");
    let mut lease = DelegatedRunLease::new(store);
    lease
        .create_background_run(&DelegatedRunStartInput {
            delegated_run_id: "expired-owner-writer".to_string(),
            parent_session_id: "session-1".to_string(),
            parent_tool_call_id: Some("tool-expired-owner".to_string()),
            role: DelegatedRunRole::Build,
            stage: DelegatedRunStage::Running,
            provider: None,
            model: None,
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![scope(
                "workspace",
                temp.path().to_string_lossy().as_ref(),
                "workspace",
            )],
        })
        .expect("create leased run");
    lease
        .db
        .conn()
        .execute(
            "UPDATE delegated_runs SET host_lease_expires_at_ms = 0 WHERE delegated_run_id = 'expired-owner-writer'",
            [],
        )
        .expect("expire owner lease");

    let stale_completion = serde_json::json!({"outcome": "success"});
    let error = lease
        .finalize_background_run(
            "expired-owner-writer",
            DelegatedRunStage::Complete,
            &stale_completion,
            Some("stale success"),
            true,
        )
        .expect_err("expired owner must not publish completion");
    assert!(error.to_string().contains("lost its background host lease"));
    assert_eq!(
        lease
            .get_run("expired-owner-writer")
            .unwrap()
            .unwrap()
            .stage,
        DelegatedRunStage::Running
    );

    assert_eq!(
        lease
            .expire_stale_background_host_leases()
            .expect("recovery should win terminal CAS"),
        vec!["expired-owner-writer".to_string()]
    );
    // The armed lease's Drop fallback is owner-fenced and must observe rather
    // than overwrite the recovery terminal artifact.
    drop(lease);

    let record = DelegatedRunStore::new(Database::new(&db_path).expect("reopen database"))
        .get_run("expired-owner-writer")
        .expect("load recovered row")
        .expect("recovered row exists");
    assert_eq!(record.stage, DelegatedRunStage::Cancelled);
    assert_eq!(
        record.artifact.as_ref().unwrap()["outcome_reason"],
        "background_host_lease_expired"
    );
}

#[test]
fn active_parent_wake_is_scoped_and_survives_long_terminal_history() {
    let (store, temp) = create_store();
    let start = DelegatedRunStartInput {
        delegated_run_id: "old-live-child".into(),
        parent_session_id: "session-1".into(),
        parent_tool_call_id: None,
        role: DelegatedRunRole::Explore,
        stage: DelegatedRunStage::Running,
        provider: None,
        model: None,
        resumable: false,
        resumed_from_run_id: None,
        target_scope: vec![],
    };
    store.create_background_run(&start).unwrap();
    let db = Database::new(&temp.path().join("delegated-runs.db")).unwrap();
    db.conn()
        .execute_batch(
            "UPDATE delegated_runs SET updated_at = '2000-01-01T00:00:00Z';
         WITH RECURSIVE history(n) AS (
             SELECT 1 UNION ALL SELECT n + 1 FROM history WHERE n < 1001
         )
         INSERT INTO delegated_runs (
             delegated_run_id, parent_session_id, role, stage, target_scope_key,
             target_scope_json, created_at, updated_at, completed_at, wake_parent
         ) SELECT 'terminal-' || n, 'session-1', 'explore', 'complete', '', '[]',
             '2001-01-01T00:00:00Z', '2001-01-01T00:00:00Z',
             '2001-01-01T00:00:00Z', 1 FROM history;",
        )
        .unwrap();
    assert!(store
        .list_runs_for_session("session-1", 1000)
        .unwrap()
        .iter()
        .all(|run| run.delegated_run_id != start.delegated_run_id));
    assert!(store.has_active_parent_wake("session-1").unwrap());
    assert!(!store.has_active_parent_wake("another-session").unwrap());
    store
        .finalize_run(
            &start.delegated_run_id,
            DelegatedRunStage::Complete,
            &serde_json::json!({"summary":"finished"}),
            None,
            false,
        )
        .unwrap();
    assert!(!store.has_active_parent_wake("session-1").unwrap());
    store
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: "foreground-child".into(),
            ..start
        })
        .unwrap();
    assert!(!store.has_active_parent_wake("session-1").unwrap());
}
