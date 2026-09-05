use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use tempfile::TempDir;

use crate::ai::models::{ApiFormat, ModelKey};
use crate::ai::providers::ProviderId;
use crate::hive::HiveRunStatus;
use crate::storage::{
    grant_worker_governor_recovery_in_transaction,
    refresh_worker_governor_recovery_run_binding_in_transaction, BeginWorkerProviderCall,
    BeginWorkerProviderCallResult, ClaimRunRequest, ClaimedHiveRun, DaemonFence,
    DaemonLeaseAcquire, Database, FinishWorkerProviderCall, HiveDaemonLeaseStore,
    HiveRunExecutionContextV1, HiveRunKind, HiveRunStore, HiveWorkerGovernorStore,
    ProviderCallRemoteAcceptance, ProviderCallTerminalState, ReconcileUnknownProviderCall,
    RunCompletion, WorkerConversationLane, WorkerGovernorRecoveryRunBinding, WorkerRunOrigin,
    WORKER_CONVERSATION_STOP_REQUESTED_REASON,
};
use crate::tools::registry::PermissionMode;
use crate::Content;

use super::*;

const OWNER: &str = "alice";
const WORKER_ID: &str = "worker-1";
const SESSION_ID: &str = "worker-dm";
const CONTROLLER_ID: &str = "controller-1";
const DAEMON_ID: &str = "daemon-1";

fn model_key() -> ModelKey {
    ModelKey::new(
        ProviderId::Grok,
        "grok-worker-test",
        ApiFormat::OpenAIResponses,
    )
}

fn now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 25, 4, 0, 0).single().unwrap()
}

fn fixture() -> (Database, TempDir) {
    let temp = TempDir::new().unwrap();
    let db = Database::new(&temp.path().join("worker-conversation.db")).unwrap();
    db.conn()
        .execute_batch(
            r#"
            INSERT INTO users (id, email)
            VALUES ('alice', 'alice@example.test');
            INSERT INTO sessions (
                id, user_id, title, created_at, updated_at, session_type,
                workspace_mode, working_dir, project_dir
            ) VALUES
            (
                'worker-dm', 'alice', 'Worker DM',
                '2026-08-25T04:00:00.000000Z',
                '2026-08-25T04:00:00.000000Z', 'hive',
                'neutral', NULL, NULL
            ),
            (
                'worker-group-lane', 'alice', 'Worker group lane',
                '2026-08-25T04:00:00.000000Z',
                '2026-08-25T04:00:00.000000Z', 'hive',
                'neutral', NULL, NULL
            );
            INSERT INTO hive_workers (
                id, user_id, slug, display_name, model, model_key_json,
                model_catalog_revision, permission_mode, autonomy, status,
                dm_session_id, memory_namespace_id, created_at, updated_at
            ) VALUES (
                'worker-1', 'alice', 'worker-1', 'Worker 1',
                'grok-worker-test',
                '{"provider":"grok","model_id":"grok-worker-test","api_format":"open_ai_responses"}',
                'catalog-v1', 'autonomous', 'always_on', 'active',
                'worker-dm', 'worker-1',
                '2026-08-25T04:00:00.000000Z',
                '2026-08-25T04:00:00.000000Z'
            );
            INSERT INTO hive_controllers (
                id, scope_key, user_id, session_id, status, timezone,
                max_concurrent_runs, worker_id, created_at, updated_at
            ) VALUES (
                'controller-1', 'worker:worker-1', 'alice', 'worker-dm',
                'active', 'UTC', 1, 'worker-1',
                '2026-08-25T04:00:00.000000Z',
                '2026-08-25T04:00:00.000000Z'
            );
            INSERT INTO hive_groups (
                id, user_id, title, execution_mode, max_rounds,
                max_member_messages_per_turn, parallelism,
                context_window_messages, status, created_at, updated_at
            ) VALUES (
                'group-1', 'alice', 'Group 1', 'roundtable', 3, 2, 1, 24,
                'active', '2026-08-25T04:00:00.000000Z',
                '2026-08-25T04:00:00.000000Z'
            );
            INSERT INTO hive_group_members (group_id, worker_id, position, added_at)
            VALUES ('group-1', 'worker-1', 0, '2026-08-25T04:00:00.000000Z');
            INSERT INTO hive_group_worker_lanes (
                group_id, worker_id, session_id, created_at, updated_at
            ) VALUES (
                'group-1', 'worker-1', 'worker-group-lane',
                '2026-08-25T04:00:00.000000Z',
                '2026-08-25T04:00:00.000000Z'
            );
            INSERT INTO hive_controllers (
                id, scope_key, user_id, session_id, status, timezone,
                max_concurrent_runs, worker_id, created_at, updated_at
            ) VALUES (
                'group-controller-1', 'worker:worker-1:group:group-1', 'alice',
                'worker-group-lane', 'active', 'UTC', 1, 'worker-1',
                '2026-08-25T04:00:00.000000Z',
                '2026-08-25T04:00:00.000000Z'
            );
            "#,
        )
        .unwrap();
    (db, temp)
}

fn acceptance(
    input_id: &str,
    request_id: &str,
    run_id: &str,
    body: &str,
) -> AcceptWorkerConversationInput {
    AcceptWorkerConversationInput {
        input_id: input_id.to_string(),
        request_id: request_id.to_string(),
        worker_id: WORKER_ID.to_string(),
        owner_user_id: Some(OWNER.to_string()),
        session_id: SESSION_ID.to_string(),
        controller_id: CONTROLLER_ID.to_string(),
        body: body.to_string(),
        accepted_at: now(),
        new_run_id: run_id.to_string(),
        run_config: serde_json::json!({
            "model": "grok-worker-test",
            "model_key": {
                "provider": "grok",
                "model_id": "grok-worker-test",
                "api_format": "open_ai_responses"
            },
            "model_catalog_revision": "catalog-v1",
            "permission_mode": "autonomous",
            "working_dir": null,
            "project_dir": null
        }),
        execution_context: HiveRunExecutionContextV1::worker_conversation_neutral(
            WORKER_ID,
            1,
            WorkerConversationLane::DirectMessage,
        )
        .unwrap(),
        priority: 10,
        concurrency_key: Some(format!("worker-dm:{WORKER_ID}")),
        max_attempts: 2,
    }
}

fn accept(
    db: &Database,
    input: &AcceptWorkerConversationInput,
) -> anyhow::Result<AcceptWorkerConversationInputResult> {
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let result = accept_worker_conversation_input_in_transaction(&tx, input)?;
    tx.commit()?;
    Ok(result)
}

fn database_path(temp: &TempDir) -> PathBuf {
    temp.path().join("worker-conversation.db")
}

fn acquire_fence(path: &Path, at: DateTime<Utc>) -> DaemonFence {
    let lease = match HiveDaemonLeaseStore::new(Database::new(path).unwrap())
        .acquire("hive-scheduler", DAEMON_ID, at, Duration::from_secs(3_600))
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        other => panic!("unexpected daemon lease result: {other:?}"),
    };
    DaemonFence {
        lease_name: lease.lease_name,
        owner_id: lease.owner_id,
        fencing_token: lease.fencing_token,
    }
}

fn claim_running(path: &Path, fence: &DaemonFence, lease_seconds: u64) -> ClaimedHiveRun {
    claim_running_at(
        path,
        fence,
        lease_seconds,
        now() + chrono::Duration::seconds(1),
    )
}

fn claim_running_at(
    path: &Path,
    fence: &DaemonFence,
    lease_seconds: u64,
    claim_at: DateTime<Utc>,
) -> ClaimedHiveRun {
    let store = HiveRunStore::new(Database::new(path).unwrap());
    let claim = store
        .claim_next_fenced(
            &ClaimRunRequest {
                executor_id: DAEMON_ID.to_string(),
                lease_epoch: fence.fencing_token,
                now: claim_at,
                lease_duration: Duration::from_secs(lease_seconds),
                global_concurrency_limit: 4,
            },
            fence,
        )
        .unwrap()
        .expect("queued Worker run");
    assert!(store
        .mark_running_fenced(
            &claim.run.id,
            &claim.lease_token,
            fence.fencing_token,
            claim_at + chrono::Duration::seconds(1),
            fence,
        )
        .unwrap());
    claim
}

fn begin_agent_call(
    path: &Path,
    claim: &ClaimedHiveRun,
    call_id: &str,
) -> crate::storage::WorkerProviderCall {
    begin_agent_call_at(path, claim, call_id, now() + chrono::Duration::seconds(3))
}

fn begin_agent_call_at(
    path: &Path,
    claim: &ClaimedHiveRun,
    call_id: &str,
    started_at: DateTime<Utc>,
) -> crate::storage::WorkerProviderCall {
    begin_provider_call_at(path, claim, call_id, "agent_turn", started_at)
}

fn begin_provider_call_at(
    path: &Path,
    claim: &ClaimedHiveRun,
    call_id: &str,
    call_kind: &str,
    started_at: DateTime<Utc>,
) -> crate::storage::WorkerProviderCall {
    begin_provider_call_with_override_at(path, claim, call_id, call_kind, None, started_at)
}

fn begin_provider_call_with_override_at(
    path: &Path,
    claim: &ClaimedHiveRun,
    call_id: &str,
    call_kind: &str,
    override_grant_id: Option<&str>,
    started_at: DateTime<Utc>,
) -> crate::storage::WorkerProviderCall {
    let input = BeginWorkerProviderCall {
        provider_call_id: call_id.to_string(),
        worker_id: WORKER_ID.to_string(),
        expected_worker_revision: 1,
        owner_user_id: Some(OWNER.to_string()),
        session_id: SESSION_ID.to_string(),
        conversation_lane: WorkerConversationLane::DirectMessage,
        run_id: claim.run.id.clone(),
        run_lease_token: claim.lease_token.clone(),
        run_lease_epoch: claim.run.lease_epoch.unwrap(),
        expected_model_key: model_key(),
        expected_model_catalog_revision: Some("catalog-v1".to_string()),
        expected_permission_mode: PermissionMode::Autonomous,
        origin: WorkerRunOrigin::UserDm,
        lane_key: "dm".to_string(),
        call_kind: call_kind.to_string(),
        workflow_goal_id: None,
        workflow_attempt_id: None,
        reserved_tokens: 128,
        pricing: None,
        override_grant_id: override_grant_id.map(str::to_string),
        started_at,
    };
    match HiveWorkerGovernorStore::new(Database::new(path).unwrap())
        .begin_provider_call(&input)
        .unwrap()
    {
        BeginWorkerProviderCallResult::Started(call) => call,
        other => panic!("unexpected provider admission: {other:?}"),
    }
}

fn response_input(
    claim: &ClaimedHiveRun,
    provider_call_id: &str,
    text: &str,
) -> CommitWorkerConversationResponse {
    response_input_at(
        claim,
        provider_call_id,
        text,
        now() + chrono::Duration::seconds(4),
    )
}

fn response_input_at(
    claim: &ClaimedHiveRun,
    provider_call_id: &str,
    text: &str,
    committed_at: DateTime<Utc>,
) -> CommitWorkerConversationResponse {
    CommitWorkerConversationResponse {
        worker_id: WORKER_ID.to_string(),
        worker_revision: 1,
        owner_user_id: Some(OWNER.to_string()),
        session_id: SESSION_ID.to_string(),
        lane: WorkerConversationLane::DirectMessage,
        run_id: claim.run.id.clone(),
        run_lease_token: claim.lease_token.clone(),
        run_lease_epoch: claim.run.lease_epoch.unwrap(),
        provider_call_id: provider_call_id.to_string(),
        response_text: text.to_string(),
        committed_at,
    }
}

fn completion(target_status: HiveRunStatus, at: DateTime<Utc>) -> RunCompletion {
    RunCompletion {
        target_status,
        now: at,
        available_at: (target_status == HiveRunStatus::RetryWait)
            .then_some(at + chrono::Duration::minutes(1)),
        wake_at: None,
        stop_reason: Some("caller completion".to_string()),
        error: (target_status != HiveRunStatus::Succeeded).then_some("caller failure".to_string()),
        outcome: Some(serde_json::json!({ "kind": target_status.as_str() })),
        trace_sequence_end: None,
    }
}

fn provider_outcome(path: &Path, call_id: &str) -> Option<(String, String, String)> {
    Database::new(path)
        .unwrap()
        .conn()
        .query_row(
            "SELECT state, outcome, remote_acceptance
             FROM hive_worker_provider_call_outcomes
             WHERE provider_call_id = ?1",
            [call_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .unwrap()
}

#[test]
fn idle_accept_materializes_one_canonical_user_row_and_exact_run() {
    let (db, temp) = fixture();
    let input = acceptance("input-1", "request-1", "run-1", "Help me plan releases.");
    let result = accept(&db, &input).unwrap();
    let message_id = match result {
        AcceptWorkerConversationInputResult::Queued { run_id, message_id } => {
            assert_eq!(run_id, "run-1");
            message_id
        }
        other => panic!("unexpected acceptance result: {other:?}"),
    };
    assert_eq!(
        accept(&db, &input).unwrap(),
        AcceptWorkerConversationInputResult::Queued {
            run_id: "run-1".to_string(),
            message_id,
        }
    );
    let mut conflicting_replay = input;
    conflicting_replay.input_id = "another-input".to_string();
    conflicting_replay.new_run_id = "another-run".to_string();
    assert!(accept(&db, &conflicting_replay).is_err());

    let run =
        HiveRunStore::new(Database::new(&temp.path().join("worker-conversation.db")).unwrap())
            .get_run("run-1")
            .unwrap()
            .unwrap();
    assert_eq!(run.kind, HiveRunKind::WorkerConversation);
    assert_eq!(run.worker_id.as_deref(), Some(WORKER_ID));
    assert_eq!(run.objective_message_id, Some(message_id));
    assert_eq!(run.conversation_through_message_id, Some(message_id));
    assert_eq!(run.execution_context.unwrap().worker_revision(), 1);
    assert_eq!(run.governor.unwrap().lane_key.as_deref(), Some("dm"));
    assert!(run.response_message_id.is_none());

    let episode_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM conversation_episodes
             WHERE session_id = ?1 AND source_message_id = ?2 AND role = 'user'",
            params![SESSION_ID, message_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(episode_count, 1);
}

#[test]
fn input_during_an_unfinished_run_is_staged_and_replay_is_exact() {
    let (db, _temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let staged_input = acceptance("input-2", "request-2", "run-2", "Second message");
    let first = accept(&db, &staged_input).unwrap();
    let second = accept(&db, &staged_input).unwrap();
    let staged = match first {
        AcceptWorkerConversationInputResult::Staged {
            active_run_id,
            input,
        } => {
            assert_eq!(active_run_id, "run-1");
            input
        }
        other => panic!("unexpected acceptance result: {other:?}"),
    };
    assert_eq!(staged.state, WorkerConversationInputState::Staged);
    assert_eq!(
        second,
        AcceptWorkerConversationInputResult::Staged {
            active_run_id: "run-1".to_string(),
            input: staged,
        }
    );
    let canonical_second: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM messages
             WHERE session_id = ?1 AND idempotency_key = ?2",
            params![SESSION_ID, "worker-request:request-2:canonical"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(canonical_second, 0);
    assert!(db
        .conn()
        .execute(
            "DELETE FROM hive_worker_conversation_inputs WHERE id = 'input-2'",
            [],
        )
        .is_err());
}

#[test]
fn awaiting_input_is_not_treated_as_an_idle_or_stageable_lane() {
    let (db, _temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs SET status = 'awaiting_input' WHERE id = 'run-1'",
            [],
        )
        .unwrap();

    let error = accept(
        &db,
        &acceptance("input-2", "request-2", "run-2", "Explicit response"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("explicit UserResponse"));
    let run_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_runs WHERE kind = 'worker_conversation'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(run_count, 1);
}

#[test]
fn specialized_lane_occupants_reject_direct_messages_before_any_mutation() {
    for (kind, status) in [
        ("worker_workflow", "queued"),
        ("worker_workflow", "running"),
        ("worker_workflow", "recovery_required"),
        ("worker_workflow_acceptance", "awaiting_input"),
        ("worker_introduction", "queued"),
        ("scheduled", "queued"),
        ("worker_message", "queued"),
        ("worker_heartbeat", "queued"),
        ("group_turn", "queued"),
    ] {
        let (db, _temp) = fixture();
        accept(
            &db,
            &acceptance("input-1", "request-1", "run-1", "Workflow message"),
        )
        .unwrap();
        db.conn()
            .execute(
                "UPDATE hive_runs
                 SET kind = ?2, status = ?3
                 WHERE id = ?1",
                params!["run-1", kind, status],
            )
            .unwrap();
        if status == "recovery_required" {
            db.conn()
                .execute(
                    "UPDATE hive_controllers SET status = 'paused'
                     WHERE id = ?1",
                    [CONTROLLER_ID],
                )
                .unwrap();
        }

        let before_run: (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            u32,
        ) = db
            .conn()
            .query_row(
                "SELECT kind, status, outcome_json, last_error, last_stop_reason,
                        lease_owner, lease_token, attempt_count
                 FROM hive_runs WHERE id = 'run-1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .unwrap();
        let before_counts: (i64, i64, i64, i64) = db
            .conn()
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM hive_runs),
                     (SELECT COUNT(*) FROM messages),
                     (SELECT COUNT(*) FROM conversation_episodes),
                     (SELECT COUNT(*) FROM hive_worker_conversation_inputs)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        let before_controller: String = db
            .conn()
            .query_row(
                "SELECT status FROM hive_controllers WHERE id = ?1",
                [CONTROLLER_ID],
                |row| row.get(0),
            )
            .unwrap();

        let error = accept(
            &db,
            &acceptance("input-2", "request-2", "run-2", "Do not strand me"),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("Worker direct message is blocked by non-conversation run run-1 ({kind})")
        );
        assert!(db
            .conn()
            .execute(
                "INSERT INTO hive_worker_conversation_inputs (
                     id, worker_id, owner_user_id, session_id, request_id,
                     accepted_while_run_id, content_json, state, accepted_at
                 ) VALUES (
                     ?1, ?2, ?3, ?4, ?5, 'run-1',
                     '[{\"type\":\"text\",\"text\":\"forged\"}]',
                     'staged', ?6
                 )",
                params![
                    format!("forged-{kind}-{status}"),
                    WORKER_ID,
                    OWNER,
                    SESSION_ID,
                    format!("forged-{kind}-{status}"),
                    crate::hive::canonical_timestamp(now()),
                ],
            )
            .is_err());

        let after_run: (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            u32,
        ) = db
            .conn()
            .query_row(
                "SELECT kind, status, outcome_json, last_error, last_stop_reason,
                        lease_owner, lease_token, attempt_count
                 FROM hive_runs WHERE id = 'run-1'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .unwrap();
        let after_counts: (i64, i64, i64, i64) = db
            .conn()
            .query_row(
                "SELECT
                     (SELECT COUNT(*) FROM hive_runs),
                     (SELECT COUNT(*) FROM messages),
                     (SELECT COUNT(*) FROM conversation_episodes),
                     (SELECT COUNT(*) FROM hive_worker_conversation_inputs)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        let after_controller: String = db
            .conn()
            .query_row(
                "SELECT status FROM hive_controllers WHERE id = ?1",
                [CONTROLLER_ID],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(after_run, before_run, "run mutated for {kind}/{status}");
        assert_eq!(
            after_counts, before_counts,
            "rows mutated for {kind}/{status}"
        );
        assert_eq!(
            after_controller, before_controller,
            "controller mutated for {kind}/{status}"
        );
    }
}

#[test]
fn staged_input_replay_requires_its_immutable_predecessor_authority() {
    let (db, _temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    let original = accept(&db, &second).unwrap();
    assert!(matches!(
        &original,
        AcceptWorkerConversationInputResult::Staged { .. }
    ));

    db.conn()
        .execute(
            "UPDATE hive_runs SET status = 'failed' WHERE id = 'run-1'",
            [],
        )
        .unwrap();
    assert_eq!(
        accept(&db, &second).unwrap(),
        original,
        "an ordinary accepted input must replay after predecessor terminality"
    );
    let counts: (i64, i64, i64) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM hive_worker_conversation_inputs),
                 (SELECT COUNT(*) FROM messages),
                 (SELECT COUNT(*) FROM hive_runs)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(counts, (1, 1, 1));

    db.conn()
        .execute(
            "UPDATE hive_runs SET kind = 'worker_workflow' WHERE id = 'run-1'",
            [],
        )
        .unwrap();
    let error = accept(&db, &second).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Worker direct message is blocked by non-conversation run run-1 (worker_workflow)"
    );
    let counts_after: (i64, i64, i64) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM hive_worker_conversation_inputs),
                 (SELECT COUNT(*) FROM messages),
                 (SELECT COUNT(*) FROM hive_runs)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(counts_after, counts);
}

#[test]
fn database_guards_reject_cross_lane_staging_and_preforged_responses() {
    let (db, _temp) = fixture();
    let queued = accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let _message_id = match queued {
        AcceptWorkerConversationInputResult::Queued { message_id, .. } => message_id,
        other => panic!("unexpected acceptance result: {other:?}"),
    };
    assert!(db
        .conn()
        .execute(
            "INSERT INTO hive_worker_conversation_inputs (
                 id, worker_id, owner_user_id, session_id, request_id,
                 accepted_while_run_id, content_json, state, accepted_at
             ) VALUES (
                 'forged-input', 'worker-1', 'mallory', 'worker-dm',
                 'forged-request', 'run-1',
                 '[{\"type\":\"text\",\"text\":\"forged\"}]',
                 'staged', '2026-08-25T04:00:00.000000Z'
             )",
            [],
        )
        .is_err());

    db.conn()
        .execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES (
                 'worker-dm', 'user',
                 '[{\"type\":\"text\",\"text\":\"forged objective\"}]',
                 '2026-08-25T04:00:01.000000Z',
                 'worker-request:forged-objective:canonical'
             )",
            [],
        )
        .unwrap();
    let forged_objective_id = db.conn().last_insert_rowid();
    db.conn()
        .execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES (
                 'worker-dm', 'assistant',
                 '[{\"type\":\"text\",\"text\":\"forged response\"}]',
                 '2026-08-25T04:00:01.000000Z',
                 'worker-run:forged-run:assistant:final'
             )",
            [],
        )
        .unwrap();
    let response_message_id = db.conn().last_insert_rowid();
    let context = serde_json::to_string(
        &HiveRunExecutionContextV1::worker_conversation_neutral(
            WORKER_ID,
            1,
            WorkerConversationLane::DirectMessage,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(db
        .conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, available_at, max_attempts, created_at, updated_at,
                 worker_id, objective_message_id, governor_origin,
                 governor_lane_key, execution_context_json,
                 conversation_through_message_id, response_message_id
             ) VALUES (
                 'forged-run', 'controller-1', 'worker-dm',
                 'worker_conversation', 'forged', '{}', 'queued',
                 '2026-08-25T04:00:01.000000Z', 1,
                 '2026-08-25T04:00:01.000000Z',
                 '2026-08-25T04:00:01.000000Z', 'worker-1', ?1,
                 'user_dm', 'dm', ?2, ?1, ?3
             )",
            params![forged_objective_id, context, response_message_id],
        )
        .is_err());
}

#[test]
fn run_kind_parser_and_recovery_policy_keep_worker_conversation_non_replayable() {
    assert_eq!(
        HiveRunKind::WorkerConversation.as_str(),
        "worker_conversation"
    );
    assert_eq!(
        HiveRunKind::parse("worker_conversation"),
        Some(HiveRunKind::WorkerConversation)
    );
    assert!(!HiveRunKind::WorkerConversation.replays_after_expired_running());
    assert_eq!(HiveRunStatus::Queued.as_str(), "queued");
}

#[test]
fn fenced_response_commit_inserts_adopts_and_rejects_conflicting_content() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    let store = SqliteWorkerConversationResponseStore::new(&path, fence);
    let input = response_input(&claim, "call-1", "A bounded Worker reply.");

    let inserted = store.commit_response(&input).unwrap();
    assert_eq!(
        inserted.disposition,
        WorkerConversationResponseCommitDisposition::Inserted
    );
    let adopted = store.commit_response(&input).unwrap();
    assert_eq!(
        adopted.disposition,
        WorkerConversationResponseCommitDisposition::AdoptedIdentical
    );
    assert_eq!(adopted.response_message_id, inserted.response_message_id);
    let run = HiveRunStore::new(Database::new(&path).unwrap())
        .get_run("run-1")
        .unwrap()
        .unwrap();
    assert_eq!(run.response_provider_call_id.as_deref(), Some("call-1"));
    assert!(db
        .conn()
        .execute(
            "UPDATE hive_runs SET response_provider_call_id = 'forged-call'
             WHERE id = 'run-1'",
            [],
        )
        .is_err());

    let mut conflict = input;
    conflict.response_text = "Different reply under the same run key.".to_string();
    assert!(matches!(
        store.commit_response(&conflict),
        Err(WorkerConversationResponseCommitError::ConflictOrCorrupt(_))
    ));
    let rows: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM messages
             WHERE idempotency_key = 'worker-run:run-1:assistant:final'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rows, 1);
}

#[test]
fn onboarding_fallback_commits_through_the_standard_response_boundary() {
    let (db, temp) = fixture();
    db.conn()
        .execute(
            "INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES (
                 ?1, 'assistant', ?2, ?3, 'introduction:worker-1:opening'
             )",
            params![
                SESSION_ID,
                serde_json::to_string(&vec![crate::ai::types::Content::text(
                    "What should I help with?".to_string()
                )])
                .unwrap(),
                crate::hive::canonical_timestamp(now()),
            ],
        )
        .unwrap();
    let opening_message_id = db.conn().last_insert_rowid();
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introductions (
                 worker_id, status, prompt_version, opening_message_id,
                 created_at, updated_at
             ) VALUES (?1, 'awaiting_context', 1, ?2, ?3, ?3)",
            params![
                WORKER_ID,
                opening_message_id,
                crate::hive::canonical_timestamp(now()),
            ],
        )
        .unwrap();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "Help with releases."),
    )
    .unwrap();

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    HiveWorkerGovernorStore::new(Database::new(&path).unwrap())
        .finish_provider_call(&FinishWorkerProviderCall {
            provider_call_id: "call-1".to_string(),
            worker_id: WORKER_ID.to_string(),
            run_id: "run-1".to_string(),
            state: ProviderCallTerminalState::Completed,
            outcome: "semantic_invalid".to_string(),
            remote_acceptance: ProviderCallRemoteAcceptance::Acknowledged,
            usage: None,
            estimated_cost_microunits: None,
            unknown_reason: None,
            finished_at: now() + chrono::Duration::seconds(4),
        })
        .unwrap();

    let commit = SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input_at(
            &claim,
            "call-1",
            "I can start with release planning. What cadence should I use?",
            now() + chrono::Duration::seconds(5),
        ))
        .unwrap();
    let result = HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            "run-1",
            &claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::Succeeded,
                now() + chrono::Duration::seconds(6),
            ),
            &fence,
        )
        .unwrap();

    assert_eq!(result, Some(HiveRunStatus::Succeeded));
    let key: String = db
        .conn()
        .query_row(
            "SELECT idempotency_key FROM messages WHERE id = ?1",
            [commit.response_message_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(key, "worker-run:run-1:assistant:final");
    assert_eq!(
        provider_outcome(&path, "call-1"),
        Some((
            "completed".to_string(),
            "semantic_invalid".to_string(),
            "acknowledged".to_string(),
        ))
    );
}

#[test]
fn late_response_is_stale_after_worker_controller_model_or_daemon_drift() {
    for mutation in [
        "UPDATE hive_workers SET status = 'paused' WHERE id = 'worker-1'",
        "UPDATE hive_workers SET status = 'archived' WHERE id = 'worker-1'",
        "UPDATE hive_workers SET revision = revision + 1 WHERE id = 'worker-1'",
        "UPDATE hive_workers SET model = 'different-model' WHERE id = 'worker-1'",
        "UPDATE hive_controllers SET status = 'disabled' WHERE id = 'controller-1'",
        "UPDATE hive_runs SET status = 'cancelled' WHERE id = 'run-1'",
        "UPDATE hive_runs SET last_stop_reason = 'Worker conversation stop requested by user' WHERE id = 'run-1'",
        "UPDATE hive_daemon_leases SET owner_id = 'daemon-2', fencing_token = fencing_token + 1 WHERE lease_name = 'hive-scheduler'",
    ] {
        let (db, temp) = fixture();
        accept(
            &db,
            &acceptance("input-1", "request-1", "run-1", "First message"),
        )
        .unwrap();
        let path = database_path(&temp);
        let fence = acquire_fence(&path, now());
        let claim = claim_running(&path, &fence, 120);
        begin_agent_call(&path, &claim, "call-1");
        db.conn().execute(mutation, []).unwrap();

        let error = SqliteWorkerConversationResponseStore::new(&path, fence)
            .commit_response(&response_input(&claim, "call-1", "Late response."))
            .unwrap_err();
        assert!(matches!(
            error,
            WorkerConversationResponseCommitError::StaleRejected(_)
        ));
        let response_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE idempotency_key = 'worker-run:run-1:assistant:final'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(response_count, 0);
    }
}

#[test]
fn canonical_response_committed_before_stop_wins_terminal_race() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    let committed = SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input(
            &claim,
            "call-1",
            "This response committed before Stop.",
        ))
        .unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs SET last_stop_reason = ?2 WHERE id = ?1",
            params!["run-1", WORKER_CONVERSATION_STOP_REQUESTED_REASON],
        )
        .unwrap();

    let result = HiveRunStore::new(Database::new(&path).unwrap())
        .finish_stopped_worker_conversation_claim_fenced(
            "run-1",
            &claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::Cancelled,
                now() + chrono::Duration::seconds(5),
            ),
            &fence,
        )
        .unwrap();
    assert_eq!(result, Some(HiveRunStatus::Succeeded));
    let projection: (String, i64) = db
        .conn()
        .query_row(
            "SELECT status, response_message_id FROM hive_runs WHERE id = 'run-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        projection,
        ("succeeded".into(), committed.response_message_id)
    );
}

#[test]
fn group_response_projects_atomically_with_a_run_scoped_room_key() {
    let (db, temp) = fixture();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let context = HiveRunExecutionContextV1::worker_conversation_neutral(
        WORKER_ID,
        1,
        WorkerConversationLane::Group {
            group_id: "group-1".to_string(),
        },
    )
    .unwrap();
    let config = acceptance("unused", "unused", "unused", "unused").run_config;
    let lease_expires = crate::hive::canonical_timestamp(now() + chrono::Duration::seconds(120));
    db.conn()
        .execute_batch(
            "INSERT INTO hive_group_messages (
                 id, group_id, seq, sender_kind, sender_worker_id,
                 sender_run_id, content, reply_to_message_id, turn_id,
                 idempotency_key, created_at
             ) VALUES (
                 'trigger-1', 'group-1', 1, 'user', NULL, NULL,
                 'Please compare the plans.', NULL, 'turn-1', 'trigger-key',
                 '2026-08-25T04:00:00.000000Z'
             );
             INSERT INTO hive_group_turns (
                 id, group_id, trigger_message_id, execution_mode, policy_json,
                 speaker_plan_json, next_speaker_index, status,
                 member_outcomes_json, started_at, finished_at, created_at, updated_at
             ) VALUES (
                 'turn-1', 'group-1', 'trigger-1', 'roundtable', '{}',
                 '[\"worker-1\",\"worker-1\"]', 0, 'running', NULL,
                 '2026-08-25T04:00:00.000000Z', NULL,
                 '2026-08-25T04:00:00.000000Z',
                 '2026-08-25T04:00:00.000000Z'
             );",
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, priority, available_at, attempt_count, max_attempts,
                 lease_owner, lease_token, lease_epoch, lease_expires_at,
                 created_at, started_at, updated_at, worker_id, group_id,
                 group_turn_id, trigger_message_id, governor_origin,
                 governor_lane_key, execution_context_json
             ) VALUES (
                 'group-run-1', 'group-controller-1', 'worker-group-lane',
                 'group_turn', 'Please compare the plans.', ?1, 'running', 10,
                 '2026-08-25T04:00:00.000000Z', 1, 2, 'daemon-1', 'group-lease-1',
                 ?2, ?3, '2026-08-25T04:00:00.000000Z',
                 '2026-08-25T04:00:01.000000Z', '2026-08-25T04:00:01.000000Z',
                 'worker-1', 'group-1', 'turn-1', 'trigger-1', 'user_group',
                 'group:group-1', ?4
             )",
            params![
                serde_json::to_string(&config).unwrap(),
                fence.fencing_token,
                lease_expires,
                serde_json::to_string(&context).unwrap(),
            ],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_run_attempts (
                 id, run_id, attempt_no, executor_id, lease_token, lease_epoch,
                 started_at, finished_at, outcome
             ) VALUES (
                 'group-attempt-1', 'group-run-1', 1, 'daemon-1',
                 'group-lease-1', ?1, '2026-08-25T04:00:01.000000Z', NULL, 'leased'
             )",
            [fence.fencing_token],
        )
        .unwrap();
    let begin = BeginWorkerProviderCall {
        provider_call_id: "group-call-1".to_string(),
        worker_id: WORKER_ID.to_string(),
        expected_worker_revision: 1,
        owner_user_id: Some(OWNER.to_string()),
        session_id: "worker-group-lane".to_string(),
        conversation_lane: WorkerConversationLane::Group {
            group_id: "group-1".to_string(),
        },
        run_id: "group-run-1".to_string(),
        run_lease_token: "group-lease-1".to_string(),
        run_lease_epoch: fence.fencing_token,
        expected_model_key: model_key(),
        expected_model_catalog_revision: Some("catalog-v1".to_string()),
        expected_permission_mode: PermissionMode::Autonomous,
        origin: WorkerRunOrigin::UserGroup,
        lane_key: "group:group-1".to_string(),
        call_kind: "agent_turn".to_string(),
        workflow_goal_id: None,
        workflow_attempt_id: None,
        reserved_tokens: 128,
        pricing: None,
        override_grant_id: None,
        started_at: now() + chrono::Duration::seconds(2),
    };
    assert!(matches!(
        HiveWorkerGovernorStore::new(Database::new(&path).unwrap())
            .begin_provider_call(&begin)
            .unwrap(),
        BeginWorkerProviderCallResult::Started(_)
    ));
    let commit = SqliteWorkerConversationResponseStore::new(&path, fence)
        .commit_response(&CommitWorkerConversationResponse {
            worker_id: WORKER_ID.to_string(),
            worker_revision: 1,
            owner_user_id: Some(OWNER.to_string()),
            session_id: "worker-group-lane".to_string(),
            lane: WorkerConversationLane::Group {
                group_id: "group-1".to_string(),
            },
            run_id: "group-run-1".to_string(),
            run_lease_token: "group-lease-1".to_string(),
            run_lease_epoch: begin.run_lease_epoch,
            provider_call_id: "group-call-1".to_string(),
            response_text: "The first plan is safer.".to_string(),
            committed_at: now() + chrono::Duration::seconds(3),
        })
        .unwrap();
    let group_message_id = commit.response_group_message_id.unwrap();
    let room_binding: (String, String) = db
        .conn()
        .query_row(
            "SELECT sender_run_id, idempotency_key
             FROM hive_group_messages WHERE id = ?1",
            [&group_message_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(room_binding.0, "group-run-1");
    assert_eq!(
        room_binding.1,
        "group-turn:turn-1:worker:worker-1:run:group-run-1:final"
    );
}

#[test]
fn succeeded_finish_adopts_the_exact_unresolved_final_provider_call() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input(&claim, "call-1", "Committed response."))
        .unwrap();

    assert_eq!(provider_outcome(&path, "call-1"), None);
    let result = HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            "run-1",
            &claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::Succeeded,
                now() + chrono::Duration::seconds(5),
            ),
            &fence,
        )
        .unwrap();
    assert_eq!(result, Some(HiveRunStatus::Succeeded));
    assert_eq!(
        provider_outcome(&path, "call-1"),
        Some((
            "completed".to_string(),
            "canonical_response_adopted".to_string(),
            "acknowledged".to_string(),
        ))
    );
    let run = HiveRunStore::new(Database::new(&path).unwrap())
        .get_run("run-1")
        .unwrap()
        .unwrap();
    assert_eq!(run.status, HiveRunStatus::Succeeded);
    assert!(run.last_error.is_none());
}

#[test]
fn recovery_finish_terminalizes_unresolved_call_and_never_strands_started() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");

    let result = HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            "run-1",
            &claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::RecoveryRequired,
                now() + chrono::Duration::seconds(5),
            ),
            &fence,
        )
        .unwrap();
    assert_eq!(result, Some(HiveRunStatus::RecoveryRequired));
    assert_eq!(
        provider_outcome(&path, "call-1"),
        Some((
            "unknown".to_string(),
            "response_missing".to_string(),
            "possibly_sent".to_string(),
        ))
    );
    let run = HiveRunStore::new(Database::new(&path).unwrap())
        .get_run("run-1")
        .unwrap()
        .unwrap();
    assert_eq!(run.status, HiveRunStatus::RecoveryRequired);
    assert!(run
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("explicit recovery")));
}

#[test]
fn acknowledged_provider_success_without_a_response_requires_recovery() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    let third = acceptance("input-3", "request-3", "run-3", "Third message");
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    HiveWorkerGovernorStore::new(Database::new(&path).unwrap())
        .finish_provider_call(&FinishWorkerProviderCall {
            provider_call_id: "call-1".to_string(),
            worker_id: WORKER_ID.to_string(),
            run_id: "run-1".to_string(),
            state: ProviderCallTerminalState::Completed,
            outcome: "completed".to_string(),
            remote_acceptance: ProviderCallRemoteAcceptance::Acknowledged,
            usage: None,
            estimated_cost_microunits: None,
            unknown_reason: None,
            finished_at: now() + chrono::Duration::seconds(4),
        })
        .unwrap();

    let result = HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            "run-1",
            &claim.lease_token,
            fence.fencing_token,
            &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(5)),
            &fence,
        )
        .unwrap();

    assert_eq!(result, Some(HiveRunStatus::RecoveryRequired));
    let settled_at = crate::hive::canonical_timestamp(now() + chrono::Duration::seconds(6));
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let settled = acknowledge_worker_conversation_response_loss_in_transaction(
        &tx,
        WORKER_ID,
        Some(OWNER),
        None,
        &settled_at,
    )
    .unwrap();
    let WorkerConversationGovernorRecovery::Recovered {
        predecessor_run_id,
        session_id,
        materialized_run_id: Some(second_run_id),
    } = settled
    else {
        panic!("acknowledged response loss did not promote its oldest successor")
    };
    assert_eq!(predecessor_run_id, "run-1");
    assert_eq!(session_id, SESSION_ID);
    tx.commit().unwrap();
    assert_eq!(
        provider_outcome(&path, "call-1"),
        Some((
            "completed".to_string(),
            "completed".to_string(),
            "acknowledged".to_string(),
        ))
    );
    let settlement: (String, Option<String>, String, Option<String>, String, i64) = db
        .conn()
        .query_row(
            "SELECT predecessor.status, predecessor.governor_override_id,
                    json_extract(predecessor.outcome_json, '$.reason'),
                    successor.governor_override_id, controller.status,
                    (SELECT COUNT(*)
                     FROM hive_worker_governor_override_grants
                     WHERE worker_id = ?3)
             FROM hive_runs predecessor
             JOIN hive_runs successor ON successor.id = ?2
             JOIN hive_controllers controller
               ON controller.id = predecessor.controller_id
             WHERE predecessor.id = ?1",
            params!["run-1", second_run_id, WORKER_ID],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        settlement,
        (
            "cancelled".to_string(),
            None,
            "owner_acknowledged_provider_response_loss".to_string(),
            None,
            "active".to_string(),
            0,
        )
    );
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if run_id == &second_run_id
    ));

    let second_claim = claim_running_at(&path, &fence, 120, now() + chrono::Duration::seconds(7));
    assert_eq!(second_claim.run.id, second_run_id);
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &second_run_id,
                &second_claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(9)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Failed)
    );
    let third_after: (String, Option<String>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT input.state, input.assigned_run_id, run.governor_override_id
             FROM hive_worker_conversation_inputs input
             LEFT JOIN hive_runs run ON run.id = input.assigned_run_id
             WHERE input.id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(third_after.0, "materialized");
    assert!(third_after.2.is_none());
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if third_after.1.as_deref() == Some(run_id.as_str())
    ));
}

#[test]
fn response_loss_with_older_terminal_unknown_requires_and_binds_real_grant() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "Older message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let older_claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &older_claim, "call-older");
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                "run-1",
                &older_claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(5)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );

    let older_recovery_at = now() + chrono::Duration::seconds(6);
    let older_recovery_text = crate::hive::canonical_timestamp(older_recovery_at);
    let older_tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (older_grant, _) = grant_worker_governor_recovery_in_transaction(
        &older_tx,
        WORKER_ID,
        Some(OWNER),
        "response-loss-older-recovery",
        older_recovery_at,
    )
    .unwrap();
    assert_eq!(
        acknowledge_worker_conversation_governor_recovery_in_transaction(
            &older_tx,
            WORKER_ID,
            Some(OWNER),
            &older_grant.id,
            &older_recovery_text,
        )
        .unwrap(),
        WorkerConversationGovernorRecovery::Recovered {
            predecessor_run_id: "run-1".to_string(),
            session_id: SESSION_ID.to_string(),
            materialized_run_id: None,
        }
    );
    older_tx.commit().unwrap();
    let mut current = acceptance("input-2", "request-2", "run-2", "Current message");
    current.accepted_at = now() + chrono::Duration::seconds(7);
    let current_run_id = match accept(&db, &current).unwrap() {
        AcceptWorkerConversationInputResult::Queued { run_id, .. } => run_id,
        other => panic!("current message did not queue: {other:?}"),
    };
    let current_claim = claim_running_at(&path, &fence, 120, now() + chrono::Duration::seconds(7));
    assert_eq!(current_claim.run.id, current_run_id);
    assert_eq!(
        current_claim
            .run
            .governor
            .as_ref()
            .and_then(|projection| projection.override_grant_id.as_deref()),
        Some(older_grant.id.as_str())
    );
    let mut successor = acceptance("input-3", "request-3", "run-3", "Successor message");
    successor.accepted_at = now() + chrono::Duration::seconds(8);
    assert!(matches!(
        accept(&db, &successor).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    begin_provider_call_with_override_at(
        &path,
        &current_claim,
        "call-current",
        "agent_turn",
        Some(&older_grant.id),
        now() + chrono::Duration::seconds(9),
    );

    // A specialized provider call can cross its boundary concurrently after
    // the first recovery grant is consumed. Terminalizing it as Unknown makes
    // it a genuinely newer, still-unacknowledged boundary; the current DM's
    // completed provider response is then lost independently.
    let group_context = HiveRunExecutionContextV1::worker_conversation_neutral(
        WORKER_ID,
        1,
        WorkerConversationLane::Group {
            group_id: "group-1".to_string(),
        },
    )
    .unwrap();
    let group_config = acceptance("unused", "unused", "unused", "unused").run_config;
    let group_lease_expires =
        crate::hive::canonical_timestamp(now() + chrono::Duration::seconds(120));
    db.conn()
        .execute_batch(
            "INSERT INTO hive_group_messages (
                 id, group_id, seq, sender_kind, sender_worker_id,
                 sender_run_id, content, reply_to_message_id, turn_id,
                 idempotency_key, created_at
             ) VALUES (
                 'response-loss-trigger', 'group-1', 1, 'user', NULL, NULL,
                 'Concurrent specialized work.', NULL, 'response-loss-turn',
                 'response-loss-trigger-key', '2026-08-25T04:00:09.000000Z'
             );
             INSERT INTO hive_group_turns (
                 id, group_id, trigger_message_id, execution_mode, policy_json,
                 speaker_plan_json, next_speaker_index, status,
                 member_outcomes_json, started_at, finished_at, created_at, updated_at
             ) VALUES (
                 'response-loss-turn', 'group-1', 'response-loss-trigger',
                 'roundtable', '{}', '[\"worker-1\"]', 0, 'running', NULL,
                 '2026-08-25T04:00:09.000000Z', NULL,
                 '2026-08-25T04:00:09.000000Z',
                 '2026-08-25T04:00:09.000000Z'
             );",
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, priority, available_at, attempt_count, max_attempts,
                 lease_owner, lease_token, lease_epoch, lease_expires_at,
                 created_at, started_at, updated_at, worker_id, group_id,
                 group_turn_id, trigger_message_id, governor_origin,
                 governor_lane_key, execution_context_json
             ) VALUES (
                 'response-loss-group-run', 'group-controller-1',
                 'worker-group-lane', 'group_turn', 'Concurrent specialized work.',
                 ?1, 'running', 10, '2026-08-25T04:00:09.000000Z', 1, 2,
                 'daemon-1', 'response-loss-group-lease', ?2, ?3,
                 '2026-08-25T04:00:09.000000Z',
                 '2026-08-25T04:00:09.000000Z',
                 '2026-08-25T04:00:09.000000Z', 'worker-1', 'group-1',
                 'response-loss-turn', 'response-loss-trigger', 'user_group',
                 'group:group-1', ?4
             )",
            params![
                serde_json::to_string(&group_config).unwrap(),
                fence.fencing_token,
                group_lease_expires,
                serde_json::to_string(&group_context).unwrap(),
            ],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_run_attempts (
                 id, run_id, attempt_no, executor_id, lease_token, lease_epoch,
                 started_at, finished_at, outcome
             ) VALUES (
                 'response-loss-group-attempt', 'response-loss-group-run', 1,
                 'daemon-1', 'response-loss-group-lease', ?1,
                 '2026-08-25T04:00:09.000000Z', NULL, 'leased'
             )",
            [fence.fencing_token],
        )
        .unwrap();
    let group_begin = BeginWorkerProviderCall {
        provider_call_id: "response-loss-group-call".to_string(),
        worker_id: WORKER_ID.to_string(),
        expected_worker_revision: 1,
        owner_user_id: Some(OWNER.to_string()),
        session_id: "worker-group-lane".to_string(),
        conversation_lane: WorkerConversationLane::Group {
            group_id: "group-1".to_string(),
        },
        run_id: "response-loss-group-run".to_string(),
        run_lease_token: "response-loss-group-lease".to_string(),
        run_lease_epoch: fence.fencing_token,
        expected_model_key: model_key(),
        expected_model_catalog_revision: Some("catalog-v1".to_string()),
        expected_permission_mode: PermissionMode::Autonomous,
        origin: WorkerRunOrigin::UserGroup,
        lane_key: "group:group-1".to_string(),
        call_kind: "agent_turn".to_string(),
        workflow_goal_id: None,
        workflow_attempt_id: None,
        reserved_tokens: 128,
        pricing: None,
        override_grant_id: None,
        started_at: now() + chrono::Duration::seconds(10),
    };
    assert!(matches!(
        HiveWorkerGovernorStore::new(Database::new(&path).unwrap())
            .begin_provider_call(&group_begin)
            .unwrap(),
        BeginWorkerProviderCallResult::Started(_)
    ));
    db.conn()
        .execute(
            "UPDATE hive_controllers SET status = 'disabled'
             WHERE id = 'group-controller-1' AND status = 'active'",
            [],
        )
        .unwrap();
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_cancelled_claim_fenced(
                "response-loss-group-run",
                "response-loss-group-lease",
                fence.fencing_token,
                &completion(
                    HiveRunStatus::Cancelled,
                    now() + chrono::Duration::seconds(11),
                ),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Cancelled)
    );
    HiveWorkerGovernorStore::new(Database::new(&path).unwrap())
        .reconcile_unknown_provider_call(&ReconcileUnknownProviderCall {
            provider_call_id: "response-loss-group-call".to_string(),
            worker_id: WORKER_ID.to_string(),
            run_id: "response-loss-group-run".to_string(),
            daemon_lease_name: fence.lease_name.clone(),
            daemon_owner_id: fence.owner_id.clone(),
            daemon_fencing_token: fence.fencing_token,
            reason: "specialized executor stopped after provider admission".to_string(),
            reconciled_at: now() + chrono::Duration::seconds(12),
        })
        .unwrap();
    assert_eq!(
        provider_outcome(&path, "response-loss-group-call"),
        Some((
            "unknown".to_string(),
            "executor_lost".to_string(),
            "possibly_sent".to_string(),
        ))
    );
    HiveWorkerGovernorStore::new(Database::new(&path).unwrap())
        .finish_provider_call(&FinishWorkerProviderCall {
            provider_call_id: "call-current".to_string(),
            worker_id: WORKER_ID.to_string(),
            run_id: current_run_id.clone(),
            state: ProviderCallTerminalState::Completed,
            outcome: "completed".to_string(),
            remote_acceptance: ProviderCallRemoteAcceptance::Acknowledged,
            usage: None,
            estimated_cost_microunits: None,
            unknown_reason: None,
            finished_at: now() + chrono::Duration::seconds(13),
        })
        .unwrap();
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &current_run_id,
                &current_claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(14)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );

    let response_loss_at = crate::hive::canonical_timestamp(now() + chrono::Duration::seconds(15));
    let no_grant_tx =
        Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let no_grant_error = acknowledge_worker_conversation_response_loss_in_transaction(
        &no_grant_tx,
        WORKER_ID,
        Some(OWNER),
        None,
        &response_loss_at,
    )
    .unwrap_err();
    assert!(no_grant_error
        .to_string()
        .contains("older unresolved provider boundary"));
    let unchanged: (String, String, Option<String>) = no_grant_tx
        .query_row(
            "SELECT run.status, controller.status, input.assigned_run_id
             FROM hive_runs run
             JOIN hive_controllers controller ON controller.id = run.controller_id
             JOIN hive_worker_conversation_inputs input ON input.id = 'input-3'
             WHERE run.id = ?1",
            [&current_run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        unchanged,
        ("recovery_required".to_string(), "paused".to_string(), None)
    );
    no_grant_tx.rollback().unwrap();

    let with_grant_tx =
        Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let new_recovery_at = now() + chrono::Duration::seconds(16);
    let new_recovery_text = crate::hive::canonical_timestamp(new_recovery_at);
    let (response_loss_grant, created) = grant_worker_governor_recovery_in_transaction(
        &with_grant_tx,
        WORKER_ID,
        Some(OWNER),
        "response-loss-newer-specialized-recovery",
        new_recovery_at,
    )
    .unwrap();
    assert!(created);
    let settled = acknowledge_worker_conversation_response_loss_in_transaction(
        &with_grant_tx,
        WORKER_ID,
        Some(OWNER),
        Some(&response_loss_grant.id),
        &new_recovery_text,
    )
    .unwrap();
    let WorkerConversationGovernorRecovery::Recovered {
        materialized_run_id: Some(successor_run_id),
        ..
    } = settled
    else {
        panic!("combined response-loss recovery did not promote its successor")
    };
    with_grant_tx.commit().unwrap();
    let successor_grant: String = db
        .conn()
        .query_row(
            "SELECT governor_override_id FROM hive_runs WHERE id = ?1",
            [&successor_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(successor_grant, response_loss_grant.id);
    assert_eq!(
        provider_outcome(&path, "call-older"),
        Some((
            "unknown".to_string(),
            "response_missing".to_string(),
            "possibly_sent".to_string(),
        ))
    );
}

#[test]
fn owner_response_loss_settlement_rejects_non_agent_turn_provider_success() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_provider_call_at(
        &path,
        &claim,
        "call-1",
        "worker_introduction_opening",
        now() + chrono::Duration::seconds(3),
    );
    HiveWorkerGovernorStore::new(Database::new(&path).unwrap())
        .finish_provider_call(&FinishWorkerProviderCall {
            provider_call_id: "call-1".to_string(),
            worker_id: WORKER_ID.to_string(),
            run_id: "run-1".to_string(),
            state: ProviderCallTerminalState::Completed,
            outcome: "completed".to_string(),
            remote_acceptance: ProviderCallRemoteAcceptance::Acknowledged,
            usage: None,
            estimated_cost_microunits: None,
            unknown_reason: None,
            finished_at: now() + chrono::Duration::seconds(4),
        })
        .unwrap();
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                "run-1",
                &claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(5)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );

    let settled_at = crate::hive::canonical_timestamp(now() + chrono::Duration::seconds(6));
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    assert_eq!(
        acknowledge_worker_conversation_response_loss_in_transaction(
            &tx,
            WORKER_ID,
            Some(OWNER),
            None,
            &settled_at,
        )
        .unwrap(),
        WorkerConversationGovernorRecovery::UnsupportedBoundary {
            run_id: "run-1".to_string(),
            kind: "worker_conversation".to_string(),
        }
    );
    let unchanged: (String, String) = tx
        .query_row(
            "SELECT run.status, controller.status
             FROM hive_runs run
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE run.id = 'run-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        unchanged,
        ("recovery_required".to_string(), "paused".to_string())
    );
    tx.rollback().unwrap();
}

#[test]
fn recovery_finish_adopts_committed_response_as_clean_success() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input(&claim, "call-1", "Committed response."))
        .unwrap();

    let result = HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            "run-1",
            &claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::RecoveryRequired,
                now() + chrono::Duration::seconds(5),
            ),
            &fence,
        )
        .unwrap();
    assert_eq!(result, Some(HiveRunStatus::Succeeded));
    let run = HiveRunStore::new(Database::new(&path).unwrap())
        .get_run("run-1")
        .unwrap()
        .unwrap();
    assert_eq!(run.status, HiveRunStatus::Succeeded);
    assert!(run.last_error.is_none());
    assert!(run
        .last_stop_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("canonical Worker response adopted")));
    assert_eq!(
        run.outcome
            .as_ref()
            .and_then(|value| value.get("recovered"))
            .and_then(serde_json::Value::as_str),
        Some("canonical_worker_response")
    );
}

#[test]
fn expired_worker_attempts_choose_response_adoption_unknown_or_safe_requeue() {
    // Exact canonical response: adopt success and close its unresolved call.
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 5);
    begin_agent_call(&path, &claim, "call-1");
    SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input(&claim, "call-1", "Committed before crash."))
        .unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs SET last_stop_reason = ?2 WHERE id = ?1",
            params!["run-1", WORKER_CONVERSATION_STOP_REQUESTED_REASON],
        )
        .unwrap();
    let adopted = HiveRunStore::new(Database::new(&path).unwrap())
        .reconcile_expired_leases_fenced(now() + chrono::Duration::seconds(7), &fence)
        .unwrap();
    assert_eq!(adopted.recovered_succeeded, 1);
    assert_eq!(
        provider_outcome(&path, "call-1").unwrap().1,
        "canonical_response_adopted"
    );

    // Started without a response: never replay the backend.
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 5);
    begin_agent_call(&path, &claim, "call-1");
    let uncertain = HiveRunStore::new(Database::new(&path).unwrap())
        .reconcile_expired_leases_fenced(now() + chrono::Duration::seconds(7), &fence)
        .unwrap();
    assert_eq!(uncertain.recovery_required, 1);
    assert_eq!(provider_outcome(&path, "call-1").unwrap().0, "unknown");

    // No provider Started row: the attempt is provably pre-boundary.
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    claim_running(&path, &fence, 5);
    let safe = HiveRunStore::new(Database::new(&path).unwrap())
        .reconcile_expired_leases_fenced(now() + chrono::Duration::seconds(7), &fence)
        .unwrap();
    assert_eq!(safe.requeued_unstarted, 1);
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .get_run("run-1")
            .unwrap()
            .unwrap()
            .status,
        HiveRunStatus::Queued
    );

    // Exact Stop plus an unresolved Started row: account the permit and
    // terminalize instead of requeueing a response-fenced run.
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 5);
    begin_agent_call(&path, &claim, "call-1");
    db.conn()
        .execute(
            "UPDATE hive_runs SET last_stop_reason = ?2 WHERE id = ?1",
            params!["run-1", WORKER_CONVERSATION_STOP_REQUESTED_REASON],
        )
        .unwrap();
    let stopped = HiveRunStore::new(Database::new(&path).unwrap())
        .reconcile_expired_leases_fenced(now() + chrono::Duration::seconds(7), &fence)
        .unwrap();
    assert_eq!(stopped.recovered_cancelled, 1);
    assert_eq!(
        provider_outcome(&path, "call-1"),
        Some((
            "completed".into(),
            "cancelled_by_user".into(),
            "possibly_sent".into(),
        ))
    );

    // Exact Stop before a provider Started row is equally terminal and must
    // never be reclaimed as a fresh attempt.
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    claim_running(&path, &fence, 5);
    db.conn()
        .execute(
            "UPDATE hive_runs SET last_stop_reason = ?2 WHERE id = ?1",
            params!["run-1", WORKER_CONVERSATION_STOP_REQUESTED_REASON],
        )
        .unwrap();
    let stopped = HiveRunStore::new(Database::new(&path).unwrap())
        .reconcile_expired_leases_fenced(now() + chrono::Duration::seconds(7), &fence)
        .unwrap();
    assert_eq!(stopped.recovered_cancelled, 1);
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .get_run("run-1")
            .unwrap()
            .unwrap()
            .status,
        HiveRunStatus::Cancelled
    );
}

#[test]
fn staged_inputs_materialize_one_at_a_time_and_replay_the_assigned_successor() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    let third = acceptance("input-3", "request-3", "run-3", "Third message");
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    let staged_canonical_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM messages
             WHERE idempotency_key IN (
                 'worker-request:request-2:canonical',
                 'worker-request:request-3:canonical'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(staged_canonical_count, 0);

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let first_claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &first_claim, "call-1");
    SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input(&first_claim, "call-1", "First response."))
        .unwrap();
    HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            "run-1",
            &first_claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::Succeeded,
                now() + chrono::Duration::seconds(5),
            ),
            &fence,
        )
        .unwrap();

    let (assigned_second_run, second_message_id) = match accept(&db, &second).unwrap() {
        AcceptWorkerConversationInputResult::Queued { run_id, message_id } => (run_id, message_id),
        other => panic!("materialized input replay was not queued: {other:?}"),
    };
    assert_ne!(assigned_second_run, second.new_run_id);
    let third_state: (String, Option<i64>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, canonical_message_id, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(third_state, ("staged".to_string(), None, None));

    let second_claim = claim_running_at(&path, &fence, 120, now() + chrono::Duration::seconds(6));
    assert_eq!(second_claim.run.id, assigned_second_run);
    assert_eq!(
        second_claim.run.objective_message_id,
        Some(second_message_id)
    );
    begin_agent_call_at(
        &path,
        &second_claim,
        "call-2",
        now() + chrono::Duration::seconds(8),
    );
    SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input_at(
            &second_claim,
            "call-2",
            "Second response.",
            now() + chrono::Duration::seconds(9),
        ))
        .unwrap();
    HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            &assigned_second_run,
            &second_claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::Succeeded,
                now() + chrono::Duration::seconds(10),
            ),
            &fence,
        )
        .unwrap();

    let third_state: (String, Option<i64>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, canonical_message_id, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(third_state.0, "materialized");
    assert!(third_state.1.is_some());
    assert!(third_state.2.is_some());
    let canonical_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM messages
             WHERE idempotency_key IN (
                 'worker-request:request-2:canonical',
                 'worker-request:request-3:canonical'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(canonical_count, 2);
}

#[test]
fn stopped_predecessor_materializes_the_entire_staged_chain_one_at_a_time() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    let third = acceptance("input-3", "request-3", "run-3", "Third message");
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let first_claim = claim_running(&path, &fence, 120);
    db.conn()
        .execute(
            "UPDATE hive_runs SET last_stop_reason = ?2 WHERE id = ?1",
            params!["run-1", WORKER_CONVERSATION_STOP_REQUESTED_REASON],
        )
        .unwrap();
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_stopped_worker_conversation_claim_fenced(
                "run-1",
                &first_claim.lease_token,
                fence.fencing_token,
                &completion(
                    HiveRunStatus::Cancelled,
                    now() + chrono::Duration::seconds(5),
                ),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Cancelled)
    );

    let second_state: (String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(second_state.0, "materialized");
    let second_run_id = second_state.1.expect("second input successor run");
    let third_before: (String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(third_before, ("staged".to_string(), None));

    let second_claim = claim_running_at(&path, &fence, 120, now() + chrono::Duration::seconds(6));
    assert_eq!(second_claim.run.id, second_run_id);
    begin_agent_call_at(
        &path,
        &second_claim,
        "call-2",
        now() + chrono::Duration::seconds(8),
    );
    SqliteWorkerConversationResponseStore::new(&path, fence.clone())
        .commit_response(&response_input_at(
            &second_claim,
            "call-2",
            "Second response.",
            now() + chrono::Duration::seconds(9),
        ))
        .unwrap();
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &second_run_id,
                &second_claim.lease_token,
                fence.fencing_token,
                &completion(
                    HiveRunStatus::Succeeded,
                    now() + chrono::Duration::seconds(10),
                ),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Succeeded)
    );

    let third_after: (String, Option<i64>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, canonical_message_id, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(third_after.0, "materialized");
    assert!(third_after.1.is_some());
    assert!(third_after.2.is_some());
    let queued_successors: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_runs
             WHERE worker_id = ?1 AND status = 'queued'",
            [WORKER_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(queued_successors, 1);
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if third_after.2.as_deref() == Some(run_id.as_str())
    ));
}

fn assert_terminal_predecessor_without_response_materializes_staged_chain(
    terminal_status: HiveRunStatus,
) {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    let third = acceptance("input-3", "request-3", "run-3", "Third message");
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let first_claim = claim_running(&path, &fence, 120);
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                "run-1",
                &first_claim.lease_token,
                fence.fencing_token,
                &completion(terminal_status, now() + chrono::Duration::seconds(5),),
                &fence,
            )
            .unwrap(),
        Some(terminal_status)
    );

    let second_state: (String, Option<i64>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, canonical_message_id, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(second_state.0, "materialized");
    assert!(second_state.1.is_some());
    let second_run_id = second_state.2.expect("second input successor run");
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if run_id == &second_run_id
    ));
    let third_before: (String, Option<i64>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, canonical_message_id, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(third_before, ("staged".to_string(), None, None));

    let second_claim = claim_running_at(&path, &fence, 120, now() + chrono::Duration::seconds(6));
    assert_eq!(second_claim.run.id, second_run_id);
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &second_run_id,
                &second_claim.lease_token,
                fence.fencing_token,
                &completion(terminal_status, now() + chrono::Duration::seconds(10),),
                &fence,
            )
            .unwrap(),
        Some(terminal_status)
    );

    let third_after: (String, Option<i64>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, canonical_message_id, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(third_after.0, "materialized");
    assert!(third_after.1.is_some());
    let third_run_id = third_after.2.expect("third input successor run");
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if run_id == &third_run_id
    ));
    let canonical_users: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM messages
             WHERE session_id = ?1 AND role = 'user'",
            [SESSION_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(canonical_users, 3);
    let queued_successors: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_runs
             WHERE worker_id = ?1 AND status = 'queued'",
            [WORKER_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(queued_successors, 1);
}

#[test]
fn failed_and_dead_letter_predecessors_materialize_staged_chain_one_at_a_time() {
    for terminal_status in [HiveRunStatus::Failed, HiveRunStatus::DeadLetter] {
        assert_terminal_predecessor_without_response_materializes_staged_chain(terminal_status);
    }
}

#[test]
fn ordinary_cancelled_predecessor_materializes_staged_chain_one_at_a_time() {
    assert_terminal_predecessor_without_response_materializes_staged_chain(
        HiveRunStatus::Cancelled,
    );
}

#[test]
fn ordinary_cancel_with_ambiguous_provider_boundary_does_not_promote_staged_input() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                "run-1",
                &claim.lease_token,
                fence.fencing_token,
                &completion(
                    HiveRunStatus::Cancelled,
                    now() + chrono::Duration::seconds(5),
                ),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );
    assert_eq!(
        provider_outcome(&path, "call-1"),
        Some((
            "unknown".to_string(),
            "response_missing".to_string(),
            "possibly_sent".to_string(),
        ))
    );
    let staged: (String, Option<i64>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT state, canonical_message_id, assigned_run_id
             FROM hive_worker_conversation_inputs WHERE id = 'input-2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(staged, ("staged".to_string(), None, None));
}

#[test]
fn owner_governor_recovery_settles_exact_dm_and_promotes_one_staged_successor() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    let third = acceptance("input-3", "request-3", "run-3", "Third message");
    let fourth = acceptance("input-4", "request-4", "run-4", "Fourth message");
    assert!(matches!(
        accept(&db, &second).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));
    assert!(matches!(
        accept(&db, &fourth).unwrap(),
        AcceptWorkerConversationInputResult::Staged { .. }
    ));

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &claim, "call-1");
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                "run-1",
                &claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(5)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );

    let recovery_at = now() + chrono::Duration::seconds(6);
    let recovery_at_text = crate::hive::canonical_timestamp(recovery_at);
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (grant, created) = grant_worker_governor_recovery_in_transaction(
        &tx,
        WORKER_ID,
        Some(OWNER),
        "recover-op-1",
        recovery_at,
    )
    .unwrap();
    assert!(created);
    tx.execute(
        "INSERT INTO hive_control_outbox (
             id, controller_id, session_id, run_id, control_kind, dedupe_key,
             payload_json, status, available_at, created_at, updated_at
         ) VALUES (
             'outbox-recovery-1', ?1, ?2, 'run-1', 'tool_approval',
             'approval-recovery-1', '{}', 'pending', ?3, ?3, ?3
         )",
        params![CONTROLLER_ID, SESSION_ID, recovery_at_text],
    )
    .unwrap();
    let recovered = acknowledge_worker_conversation_governor_recovery_in_transaction(
        &tx,
        WORKER_ID,
        Some(OWNER),
        &grant.id,
        &recovery_at_text,
    )
    .unwrap();
    let WorkerConversationGovernorRecovery::Recovered {
        predecessor_run_id,
        session_id,
        materialized_run_id: Some(second_run_id),
    } = recovered
    else {
        panic!("ordinary DM recovery did not materialize its oldest successor")
    };
    assert_eq!(predecessor_run_id, "run-1");
    assert_eq!(session_id, SESSION_ID);
    tx.commit().unwrap();

    let predecessor: (String, Option<String>, Option<String>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT status, finished_at, governor_override_id,
                    json_extract(outcome_json, '$.governor_recovery_grant_id')
             FROM hive_runs WHERE id = 'run-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(predecessor.0, "cancelled");
    assert!(predecessor.1.is_some());
    assert!(predecessor.2.is_none());
    assert_eq!(predecessor.3.as_deref(), Some(grant.id.as_str()));
    let recovery_cleanup: (i64, i64, String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM hive_run_attempts
                  WHERE run_id = 'run-1' AND finished_at IS NULL),
                 (SELECT COUNT(*) FROM hive_control_outbox
                  WHERE run_id = 'run-1' AND status = 'pending'),
                 outbox.status, outbox.last_error
             FROM hive_control_outbox outbox
             WHERE outbox.id = 'outbox-recovery-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(recovery_cleanup.0, 0);
    assert_eq!(recovery_cleanup.1, 0);
    assert_eq!(recovery_cleanup.2, "discarded");
    assert_eq!(
        recovery_cleanup.3.as_deref(),
        Some("Worker conversation recovery acknowledged before control delivery")
    );
    assert_eq!(
        provider_outcome(&path, "call-1"),
        Some((
            "unknown".to_string(),
            "response_missing".to_string(),
            "possibly_sent".to_string(),
        ))
    );
    let successor: (String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT status, governor_override_id FROM hive_runs WHERE id = ?1",
            [&second_run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(successor, ("queued".to_string(), Some(grant.id.clone())));
    let controller_and_runtime: (String, String, Option<String>) = db
        .conn()
        .query_row(
            "SELECT controller.status, runtime.status, runtime.current_run_id
             FROM hive_controllers controller
             JOIN hive_runtime_state runtime ON runtime.session_id = controller.session_id
             WHERE controller.id = ?1",
            [CONTROLLER_ID],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        controller_and_runtime,
        (
            "active".to_string(),
            "idle".to_string(),
            Some(second_run_id.clone()),
        )
    );
    let remaining_staged: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM hive_worker_conversation_inputs
             WHERE id IN ('input-3', 'input-4')
               AND state = 'staged' AND assigned_run_id IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining_staged, 2);

    let renewal_at =
        crate::hive::parse_utc_timestamp(&grant.expires_at).unwrap() + chrono::Duration::seconds(1);
    let renewal_at_text = crate::hive::canonical_timestamp(renewal_at);
    let renewal_tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (renewed_grant, renewed_created) = grant_worker_governor_recovery_in_transaction(
        &renewal_tx,
        WORKER_ID,
        Some(OWNER),
        "recover-op-2",
        renewal_at,
    )
    .unwrap();
    assert!(renewed_created);
    assert_ne!(renewed_grant.id, grant.id);
    assert_eq!(
        refresh_worker_governor_recovery_run_binding_in_transaction(
            &renewal_tx,
            WORKER_ID,
            Some(OWNER),
            &renewed_grant.id,
            &renewal_at_text,
        )
        .unwrap(),
        WorkerGovernorRecoveryRunBinding::Rebound {
            run_id: second_run_id.clone(),
            replaced_grant_id: grant.id,
        }
    );
    renewal_tx.commit().unwrap();
    let rebound_grant: String = db
        .conn()
        .query_row(
            "SELECT governor_override_id FROM hive_runs WHERE id = ?1",
            [&second_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rebound_grant, renewed_grant.id);

    let renewed_grant_expires_at =
        crate::hive::parse_utc_timestamp(&renewed_grant.expires_at).unwrap();
    let second_claim = claim_running_at(
        &path,
        &fence,
        120,
        renewed_grant_expires_at - chrono::Duration::seconds(2),
    );
    assert_eq!(second_claim.run.id, second_run_id);
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &second_run_id,
                &second_claim.lease_token,
                fence.fencing_token,
                &completion(
                    HiveRunStatus::Failed,
                    renewed_grant_expires_at + chrono::Duration::seconds(1),
                ),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Failed)
    );
    let third_after: (String, Option<String>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT input.state, input.assigned_run_id, run.governor_override_id
             FROM hive_worker_conversation_inputs input
             LEFT JOIN hive_runs run ON run.id = input.assigned_run_id
             WHERE input.id = 'input-3'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(third_after.0, "materialized");
    let third_run_id = third_after.1.expect("third input successor run");
    assert_eq!(third_after.2.as_deref(), Some(renewed_grant.id.as_str()));
    let second_terminal_grant: Option<String> = db
        .conn()
        .query_row(
            "SELECT governor_override_id FROM hive_runs WHERE id = ?1",
            [&second_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(second_terminal_grant.is_none());
    assert!(matches!(
        accept(&db, &third).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if run_id == &third_run_id
    ));

    let third_rebind_at = renewed_grant_expires_at + chrono::Duration::seconds(2);
    let third_rebind_at_text = crate::hive::canonical_timestamp(third_rebind_at);
    let third_rebind_tx =
        Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (third_grant, third_grant_created) = grant_worker_governor_recovery_in_transaction(
        &third_rebind_tx,
        WORKER_ID,
        Some(OWNER),
        "recover-op-3",
        third_rebind_at,
    )
    .unwrap();
    assert!(third_grant_created);
    assert_eq!(
        refresh_worker_governor_recovery_run_binding_in_transaction(
            &third_rebind_tx,
            WORKER_ID,
            Some(OWNER),
            &third_grant.id,
            &third_rebind_at_text,
        )
        .unwrap(),
        WorkerGovernorRecoveryRunBinding::Rebound {
            run_id: third_run_id.clone(),
            replaced_grant_id: renewed_grant.id,
        }
    );
    third_rebind_tx.commit().unwrap();

    let third_grant_expires_at = crate::hive::parse_utc_timestamp(&third_grant.expires_at).unwrap();
    let third_claim = claim_running_at(
        &path,
        &fence,
        120,
        third_grant_expires_at - chrono::Duration::seconds(2),
    );
    assert_eq!(third_claim.run.id, third_run_id);
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &third_run_id,
                &third_claim.lease_token,
                fence.fencing_token,
                &completion(
                    HiveRunStatus::Failed,
                    third_grant_expires_at + chrono::Duration::seconds(1),
                ),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Failed)
    );
    let fourth_after: (String, Option<String>, Option<String>) = db
        .conn()
        .query_row(
            "SELECT input.state, input.assigned_run_id, run.governor_override_id
             FROM hive_worker_conversation_inputs input
             LEFT JOIN hive_runs run ON run.id = input.assigned_run_id
             WHERE input.id = 'input-4'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(fourth_after.0, "materialized");
    let fourth_run_id = fourth_after.1.expect("fourth input successor run");
    assert_eq!(fourth_after.2.as_deref(), Some(third_grant.id.as_str()));
    let third_terminal_grant: Option<String> = db
        .conn()
        .query_row(
            "SELECT governor_override_id FROM hive_runs WHERE id = ?1",
            [&third_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(third_terminal_grant.is_none());
    assert!(matches!(
        accept(&db, &fourth).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if run_id == &fourth_run_id
    ));

    let fourth_rebind_at = third_grant_expires_at + chrono::Duration::seconds(2);
    let fourth_rebind_at_text = crate::hive::canonical_timestamp(fourth_rebind_at);
    let fourth_rebind_tx =
        Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (fourth_grant, fourth_grant_created) = grant_worker_governor_recovery_in_transaction(
        &fourth_rebind_tx,
        WORKER_ID,
        Some(OWNER),
        "recover-op-4",
        fourth_rebind_at,
    )
    .unwrap();
    assert!(fourth_grant_created);
    assert_eq!(
        refresh_worker_governor_recovery_run_binding_in_transaction(
            &fourth_rebind_tx,
            WORKER_ID,
            Some(OWNER),
            &fourth_grant.id,
            &fourth_rebind_at_text,
        )
        .unwrap(),
        WorkerGovernorRecoveryRunBinding::Rebound {
            run_id: fourth_run_id.clone(),
            replaced_grant_id: third_grant.id,
        }
    );
    fourth_rebind_tx.commit().unwrap();
    let fourth_bound_grant: String = db
        .conn()
        .query_row(
            "SELECT governor_override_id FROM hive_runs WHERE id = ?1",
            [&fourth_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(fourth_bound_grant, fourth_grant.id);
}

#[test]
fn nested_owner_governor_recovery_uses_recursive_immutable_conversation_tail() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    let second = acceptance("input-2", "request-2", "run-2", "Second message");
    let third = acceptance("input-3", "request-3", "run-3", "Third message");
    let fourth = acceptance("input-4", "request-4", "run-4", "Fourth message");
    for input in [&second, &third, &fourth] {
        assert!(matches!(
            accept(&db, input).unwrap(),
            AcceptWorkerConversationInputResult::Staged { .. }
        ));
    }

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let first_claim = claim_running(&path, &fence, 120);
    begin_agent_call(&path, &first_claim, "call-1");
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                "run-1",
                &first_claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(5)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );

    let first_recovery_at = now() + chrono::Duration::seconds(6);
    let first_recovery_text = crate::hive::canonical_timestamp(first_recovery_at);
    let first_tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (first_grant, _) = grant_worker_governor_recovery_in_transaction(
        &first_tx,
        WORKER_ID,
        Some(OWNER),
        "nested-recover-1",
        first_recovery_at,
    )
    .unwrap();
    let first_recovered = acknowledge_worker_conversation_governor_recovery_in_transaction(
        &first_tx,
        WORKER_ID,
        Some(OWNER),
        &first_grant.id,
        &first_recovery_text,
    )
    .unwrap();
    let WorkerConversationGovernorRecovery::Recovered {
        materialized_run_id: Some(second_run_id),
        ..
    } = first_recovered
    else {
        panic!("first recovery did not materialize B")
    };
    first_tx.commit().unwrap();

    let second_claim = claim_running_at(&path, &fence, 120, now() + chrono::Duration::seconds(7));
    assert_eq!(second_claim.run.id, second_run_id);
    begin_provider_call_with_override_at(
        &path,
        &second_claim,
        "call-2",
        "agent_turn",
        Some(&first_grant.id),
        now() + chrono::Duration::seconds(9),
    );
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &second_run_id,
                &second_claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(10)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );

    let second_recovery_at = now() + chrono::Duration::seconds(11);
    let second_recovery_text = crate::hive::canonical_timestamp(second_recovery_at);
    let second_tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (second_grant, _) = grant_worker_governor_recovery_in_transaction(
        &second_tx,
        WORKER_ID,
        Some(OWNER),
        "nested-recover-2",
        second_recovery_at,
    )
    .unwrap();
    let second_recovered = acknowledge_worker_conversation_governor_recovery_in_transaction(
        &second_tx,
        WORKER_ID,
        Some(OWNER),
        &second_grant.id,
        &second_recovery_text,
    )
    .unwrap();
    let WorkerConversationGovernorRecovery::Recovered {
        materialized_run_id: Some(third_run_id),
        ..
    } = second_recovered
    else {
        panic!("nested recovery did not materialize C")
    };
    second_tx.commit().unwrap();
    let third_binding: String = db
        .conn()
        .query_row(
            "SELECT governor_override_id FROM hive_runs WHERE id = ?1",
            [&third_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(third_binding, second_grant.id);

    let third_claim = claim_running_at(&path, &fence, 120, now() + chrono::Duration::seconds(12));
    assert_eq!(third_claim.run.id, third_run_id);
    begin_provider_call_with_override_at(
        &path,
        &third_claim,
        "call-3",
        "agent_turn",
        Some(&second_grant.id),
        now() + chrono::Duration::seconds(14),
    );
    assert_eq!(
        HiveRunStore::new(Database::new(&path).unwrap())
            .finish_claimed_fenced(
                &third_run_id,
                &third_claim.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(15)),
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::RecoveryRequired)
    );

    let third_recovery_at = now() + chrono::Duration::seconds(16);
    let third_recovery_text = crate::hive::canonical_timestamp(third_recovery_at);
    let third_tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
    let (third_grant, _) = grant_worker_governor_recovery_in_transaction(
        &third_tx,
        WORKER_ID,
        Some(OWNER),
        "nested-recover-3",
        third_recovery_at,
    )
    .unwrap();
    let third_recovered = acknowledge_worker_conversation_governor_recovery_in_transaction(
        &third_tx,
        WORKER_ID,
        Some(OWNER),
        &third_grant.id,
        &third_recovery_text,
    )
    .unwrap();
    let WorkerConversationGovernorRecovery::Recovered {
        materialized_run_id: Some(fourth_run_id),
        ..
    } = third_recovered
    else {
        panic!("deep nested recovery did not materialize D")
    };
    third_tx.commit().unwrap();
    let fourth_binding: String = db
        .conn()
        .query_row(
            "SELECT governor_override_id FROM hive_runs WHERE id = ?1",
            [&fourth_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(fourth_binding, third_grant.id);
    assert!(matches!(
        accept(&db, &fourth).unwrap(),
        AcceptWorkerConversationInputResult::Queued { ref run_id, .. }
            if run_id == &fourth_run_id
    ));
}

#[test]
fn owner_governor_recovery_rejects_active_specialized_boundaries_without_mutation() {
    for specialized_kind in [
        "worker_introduction",
        "worker_introduction_review",
        "worker_workflow",
    ] {
        let (db, temp) = fixture();
        accept(
            &db,
            &acceptance("input-1", "request-1", "run-1", "First message"),
        )
        .unwrap();
        let path = database_path(&temp);
        let fence = acquire_fence(&path, now());
        let claim = claim_running(&path, &fence, 120);
        begin_agent_call(&path, &claim, "call-1");
        assert_eq!(
            HiveRunStore::new(Database::new(&path).unwrap())
                .finish_claimed_fenced(
                    "run-1",
                    &claim.lease_token,
                    fence.fencing_token,
                    &completion(HiveRunStatus::Failed, now() + chrono::Duration::seconds(5)),
                    &fence,
                )
                .unwrap(),
            Some(HiveRunStatus::RecoveryRequired)
        );

        let recovery_at = now() + chrono::Duration::seconds(6);
        let recovery_at_text = crate::hive::canonical_timestamp(recovery_at);
        let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate).unwrap();
        let (grant, _) = grant_worker_governor_recovery_in_transaction(
            &tx,
            WORKER_ID,
            Some(OWNER),
            &format!("recover-{specialized_kind}"),
            recovery_at,
        )
        .unwrap();
        tx.execute(
            "UPDATE hive_runs SET kind = ?2 WHERE id = ?1",
            params!["run-1", specialized_kind],
        )
        .unwrap();
        let snapshot = |tx: &Transaction<'_>| {
            tx.query_row(
                "SELECT run.status, run.kind, run.last_stop_reason,
                        run.last_error, run.outcome_json, run.finished_at,
                        run.updated_at, controller.status,
                        (SELECT COUNT(*) FROM hive_runs
                         WHERE governor_override_id = ?2),
                        (SELECT COUNT(*)
                         FROM hive_worker_governor_override_consumptions
                         WHERE grant_id = ?2)
                 FROM hive_runs run
                 JOIN hive_controllers controller ON controller.id = run.controller_id
                 WHERE run.id = ?1",
                params!["run-1", grant.id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, i64>(9)?,
                    ))
                },
            )
            .unwrap()
        };
        let before = snapshot(&tx);
        assert_eq!(before.0, "recovery_required");
        assert_eq!(before.1, specialized_kind);
        assert_eq!(before.7, "paused");
        assert_eq!((before.8, before.9), (0, 0));
        assert_eq!(
            acknowledge_worker_conversation_governor_recovery_in_transaction(
                &tx,
                WORKER_ID,
                Some(OWNER),
                &grant.id,
                &recovery_at_text,
            )
            .unwrap(),
            WorkerConversationGovernorRecovery::UnsupportedBoundary {
                run_id: "run-1".to_string(),
                kind: specialized_kind.to_string(),
            }
        );
        assert_eq!(snapshot(&tx), before);
        let response_loss_error = acknowledge_worker_conversation_response_loss_in_transaction(
            &tx,
            WORKER_ID,
            Some(OWNER),
            Some(&grant.id),
            &recovery_at_text,
        )
        .unwrap_err();
        assert!(
            response_loss_error
                .to_string()
                .contains("does not cover the older unresolved provider boundary"),
            "unexpected response-loss rejection: {response_loss_error:#}"
        );
        assert_eq!(snapshot(&tx), before);
        tx.rollback().unwrap();
    }
}

#[test]
fn repeatedly_stopped_materialized_successors_keep_the_original_staged_chain_live() {
    let (db, temp) = fixture();
    accept(
        &db,
        &acceptance("input-1", "request-1", "run-1", "First message"),
    )
    .unwrap();
    for input in [
        acceptance("input-2", "request-2", "run-2", "Second message"),
        acceptance("input-3", "request-3", "run-3", "Third message"),
    ] {
        assert!(matches!(
            accept(&db, &input).unwrap(),
            AcceptWorkerConversationInputResult::Staged { .. }
        ));
    }

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let mut expected_run_id = "run-1".to_string();
    for (input_id, offset) in [("input-2", 1), ("input-3", 5), ("input-4", 9)] {
        let claim = claim_running_at(
            &path,
            &fence,
            120,
            now() + chrono::Duration::seconds(offset),
        );
        assert_eq!(claim.run.id, expected_run_id);
        db.conn()
            .execute(
                "UPDATE hive_runs SET last_stop_reason = ?2 WHERE id = ?1",
                params![claim.run.id, WORKER_CONVERSATION_STOP_REQUESTED_REASON],
            )
            .unwrap();
        assert_eq!(
            HiveRunStore::new(Database::new(&path).unwrap())
                .finish_stopped_worker_conversation_claim_fenced(
                    &claim.run.id,
                    &claim.lease_token,
                    fence.fencing_token,
                    &completion(
                        HiveRunStatus::Cancelled,
                        now() + chrono::Duration::seconds(offset + 2),
                    ),
                    &fence,
                )
                .unwrap(),
            Some(HiveRunStatus::Cancelled)
        );
        let promoted: (String, Option<String>) = db
            .conn()
            .query_row(
                "SELECT state, assigned_run_id
                 FROM hive_worker_conversation_inputs WHERE id = ?1",
                [input_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(promoted.0, "materialized");
        expected_run_id = promoted.1.expect("next staged successor run");
        if input_id == "input-2" {
            let mut fourth = acceptance("input-4", "request-4", "run-4", "Fourth message");
            fourth.accepted_at = now() + chrono::Duration::seconds(4);
            let staged = accept(&db, &fourth).unwrap();
            assert!(matches!(
                staged,
                AcceptWorkerConversationInputResult::Staged {
                    active_run_id,
                    ..
                } if active_run_id == expected_run_id
            ));
        } else if input_id == "input-3" {
            let fourth_state: (String, String) = db
                .conn()
                .query_row(
                    "SELECT state, accepted_while_run_id
                     FROM hive_worker_conversation_inputs WHERE id = 'input-4'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(fourth_state.0, "staged");
            assert_eq!(
                fourth_state.1, claim.run.id,
                "the newer direct successor must retain its exact stopped predecessor"
            );
        }
    }

    let states: Vec<(String, String)> = db
        .conn()
        .prepare(
            "SELECT id, state FROM hive_worker_conversation_inputs
             WHERE id IN ('input-2', 'input-3', 'input-4') ORDER BY id",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        states,
        vec![
            ("input-2".into(), "materialized".into()),
            ("input-3".into(), "materialized".into()),
            ("input-4".into(), "materialized".into()),
        ]
    );
    let queued: Vec<String> = db
        .conn()
        .prepare(
            "SELECT id FROM hive_runs
             WHERE worker_id = ?1 AND status = 'queued' ORDER BY id",
        )
        .unwrap()
        .query_map([WORKER_ID], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(queued, vec![expected_run_id]);
}

#[test]
fn user_input_supersedes_a_running_pre_provider_introduction_review_and_materializes() {
    let (db, temp) = fixture();
    let at = crate::hive::canonical_timestamp(now());
    let content =
        |text: &str| serde_json::to_string(&vec![Content::text(text.to_string())]).unwrap();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES (?1, 'assistant', ?2, ?3)",
            params![SESSION_ID, content("What should I help with?"), at],
        )
        .unwrap();
    let opening_message_id = db.conn().last_insert_rowid();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES (?1, 'user', ?2, ?3)",
            params![SESSION_ID, content("Help with reliability."), at],
        )
        .unwrap();
    let evidence_message_id = db.conn().last_insert_rowid();
    db.conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES (?1, 'assistant', ?2, ?3)",
            params![SESSION_ID, content("What should I investigate first?"), at],
        )
        .unwrap();
    let through_message_id = db.conn().last_insert_rowid();
    let context = HiveRunExecutionContextV1::worker_conversation_neutral(
        WORKER_ID,
        1,
        WorkerConversationLane::DirectMessage,
    )
    .unwrap();
    let config = acceptance("unused", "unused", "unused", "unused").run_config;
    db.conn()
        .execute(
            "INSERT INTO hive_runs (
                 id, controller_id, session_id, kind, objective, config_json,
                 status, priority, concurrency_key, available_at, attempt_count,
                 max_attempts, created_at, updated_at, worker_id,
                 governor_origin, governor_lane_key, governor_policy_revision,
                 execution_context_json, conversation_through_message_id
             ) VALUES (
                 'review-run', ?1, ?2, 'worker_introduction_review',
                 'Review Introduction context', ?3, 'queued', 60,
                 'worker:worker-1', ?4, 0, 1, ?4, ?4, ?5,
                 'user_lifecycle_action', 'dm', 1, ?6, ?7
             )",
            params![
                CONTROLLER_ID,
                SESSION_ID,
                serde_json::to_string(&config).unwrap(),
                at,
                WORKER_ID,
                serde_json::to_string(&context).unwrap(),
                through_message_id,
            ],
        )
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_worker_introduction_reviews (
                 id, worker_id, session_id, status, claim_token,
                 claim_expires_at, opening_message_id, through_message_id,
                 user_message_ids_json, transcript_digest,
                 base_identity_digest, base_soul_digest, worker_user_id,
                 model, model_key_json, model_catalog_revision, provider_id,
                 trace_run_id, claimed_at, created_at, updated_at,
                 run_id, attempt_no
             ) VALUES (
                 'review-audit', ?1, ?2, 'queued', 'queued:review-run',
                 ?3, ?4, ?5, json_array(?6), 'transcript',
                 'identity', 'soul', ?7, 'grok-worker-test',
                 ?8, 'catalog-v1', 'grok', 'review-run', ?3, ?3, ?3,
                 'review-run', 1
             )",
            params![
                WORKER_ID,
                SESSION_ID,
                at,
                opening_message_id,
                through_message_id,
                evidence_message_id,
                OWNER,
                serde_json::to_string(&model_key()).unwrap(),
            ],
        )
        .unwrap();

    let path = database_path(&temp);
    let fence = acquire_fence(&path, now());
    let claim = claim_running(&path, &fence, 120);
    assert_eq!(claim.run.id, "review-run");
    let reply = acceptance(
        "input-during-review",
        "request-during-review",
        "caller-run-id-is-not-used",
        "Start with the scheduler race.",
    );
    assert!(matches!(
        accept(&db, &reply).unwrap(),
        AcceptWorkerConversationInputResult::Staged { active_run_id, .. }
            if active_run_id == "review-run"
    ));
    HiveRunStore::new(Database::new(&path).unwrap())
        .finish_claimed_fenced(
            "review-run",
            &claim.lease_token,
            fence.fencing_token,
            &completion(
                HiveRunStatus::Succeeded,
                now() + chrono::Duration::seconds(5),
            ),
            &fence,
        )
        .unwrap();

    let (review_run_status, review_status, input_state, assigned_run_id): (
        String,
        String,
        String,
        Option<String>,
    ) = db
        .conn()
        .query_row(
            "SELECT run.status, review.status, input.state, input.assigned_run_id
             FROM hive_runs run
             JOIN hive_worker_introduction_reviews review ON review.run_id = run.id
             JOIN hive_worker_conversation_inputs input
               ON input.accepted_while_run_id = run.id
             WHERE run.id = 'review-run'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(review_run_status, "succeeded");
    assert_eq!(review_status, "stale");
    assert_eq!(input_state, "materialized");
    let assigned_run_id = assigned_run_id.expect("staged reply successor");
    let successor: (String, String, i64) = db
        .conn()
        .query_row(
            "SELECT status, kind,
                    json_extract(execution_context_json, '$.mode.worker_revision')
             FROM hive_runs WHERE id = ?1",
            [&assigned_run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        successor,
        ("queued".into(), "worker_conversation".into(), 1)
    );
}
