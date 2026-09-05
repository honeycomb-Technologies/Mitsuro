use std::time::Duration;

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use tempfile::TempDir;

use crate::hive::HiveRunStatus;
use crate::storage::{
    Database, HiveRunExecutionContextV1, WorkerConversationLane, WorkerRunGovernorProjection,
    WorkerRunOrigin,
};

use super::{
    ClaimRunRequest, DaemonFence, HiveRun, HiveRunAttemptOutcome, HiveRunKind, HiveRunStore,
    RunCompletion,
};
use crate::storage::{DaemonLeaseAcquire, HiveDaemonLeaseStore};

fn instant(second: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, second)
        .single()
        .unwrap()
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn store() -> (HiveRunStore, TempDir) {
    let temp = TempDir::new().unwrap();
    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO sessions (id, title, created_at, updated_at, session_type)
             VALUES ('session-1', 'Hive controller', '2026-07-01T00:00:00.000000Z',
                     '2026-07-01T00:00:00.000000Z', 'hive');
             INSERT INTO hive_controllers (
                 id, scope_key, user_id, session_id, status, timezone,
                 max_concurrent_runs, created_at, updated_at
             ) VALUES (
                 'controller-1', 'local:test', NULL, 'session-1', 'active', 'UTC', 1,
                 '2026-07-01T00:00:00.000000Z', '2026-07-01T00:00:00.000000Z'
             );",
        )
        .unwrap();
    (HiveRunStore::new(db), temp)
}

fn run(id: &str, priority: i32, max_attempts: u32) -> HiveRun {
    let now = instant(0);
    HiveRun {
        id: id.into(),
        controller_id: "controller-1".into(),
        session_id: None,
        schedule_id: None,
        occurrence_id: None,
        worker_id: None,
        objective_message_id: None,
        execution_context: None,
        conversation_through_message_id: None,
        response_message_id: None,
        response_provider_call_id: None,
        response_group_message_id: None,
        workflow_goal_id: None,
        workflow_attempt_id: None,
        governor: None,
        kind: HiveRunKind::Dispatch,
        objective: format!("execute {id}"),
        config: serde_json::json!({"permission_mode": "ask"}),
        status: HiveRunStatus::Queued,
        priority,
        concurrency_key: None,
        scheduled_for: None,
        available_at: timestamp(now),
        wake_at: None,
        attempt_count: 0,
        max_attempts,
        lease_owner: None,
        lease_token: None,
        lease_epoch: None,
        lease_expires_at: None,
        heartbeat_at: None,
        last_stop_reason: None,
        last_error: None,
        outcome: None,
        created_at: timestamp(now),
        started_at: None,
        finished_at: None,
        updated_at: timestamp(now),
    }
}

fn running_worker_introduction(opening_key: Option<&str>) -> (HiveRunStore, TempDir, Option<i64>) {
    let (store, temp) = store();
    let path = temp.path().join("runs.db");
    let db = Database::new(&path).unwrap();
    db.conn()
        .execute_batch(
            "UPDATE sessions
             SET model = 'test:model',
                 model_key_json = '{\"provider\":\"grok\",\"model_id\":\"test:model\",\"api_format\":\"open_ai_responses\"}',
                 model_catalog_revision = 'catalog-1',
                 permission_mode = 'autonomous'
             WHERE id = 'session-1';
             INSERT INTO hive_workers (
                 id, slug, display_name, model, model_key_json,
                 model_catalog_revision, permission_mode, autonomy, status,
                 dm_session_id, memory_namespace_id, created_at, updated_at
             ) VALUES (
                 'worker-1', 'worker-1', 'Worker 1', 'test:model',
                 '{\"provider\":\"grok\",\"model_id\":\"test:model\",\"api_format\":\"open_ai_responses\"}',
                 'catalog-1', 'autonomous', 'manual', 'active',
                 'session-1', 'worker-1',
                 '2026-07-01T00:00:00.000000Z',
                 '2026-07-01T00:00:00.000000Z'
             );
             UPDATE hive_controllers SET worker_id = 'worker-1'
             WHERE id = 'controller-1';",
        )
        .unwrap();
    drop(db);

    let mut introduction = run("introduction-1", 0, 2);
    introduction.kind = HiveRunKind::WorkerIntroduction;
    introduction.session_id = Some("session-1".into());
    introduction.worker_id = Some("worker-1".into());
    introduction.execution_context = Some(
        HiveRunExecutionContextV1::worker_conversation_neutral(
            "worker-1",
            1,
            WorkerConversationLane::DirectMessage,
        )
        .unwrap(),
    );
    introduction.governor = Some(WorkerRunGovernorProjection {
        run_id: introduction.id.clone(),
        origin: Some(WorkerRunOrigin::UserLifecycleAction),
        lane_key: Some("dm".into()),
        gate_reason: None,
        next_eligible_at: None,
        policy_revision: None,
        override_grant_id: None,
    });
    introduction.config = serde_json::json!({
        "worker_id": "worker-1",
        "model": "test:model",
        "model_key": {
            "provider": "grok",
            "model_id": "test:model",
            "api_format": "open_ai_responses"
        },
        "model_catalog_revision": "catalog-1",
        "permission_mode": "autonomous"
    });
    store.insert_run(&introduction).unwrap();
    let db = Database::new(&path).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO hive_worker_introductions (
                 worker_id, run_id, status, prompt_version, created_at, updated_at
             ) VALUES (
                 'worker-1', 'introduction-1', 'queued', 1,
                 '2026-07-01T00:00:00.000000Z',
                 '2026-07-01T00:00:00.000000Z'
             );",
        )
        .unwrap();
    drop(db);

    let claimed = store
        .claim_next(&claim_request(instant(0), 1))
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running("introduction-1", &claimed.lease_token, 1, instant(1))
        .unwrap());
    let db = Database::new(&path).unwrap();
    db.conn()
        .execute(
            "UPDATE hive_worker_introductions
             SET status = 'running' WHERE run_id = 'introduction-1'",
            [],
        )
        .unwrap();
    let opening_message_id = opening_key.map(|key| {
        db.conn()
            .execute(
                "INSERT INTO messages (
                     session_id, role, content, created_at, idempotency_key
                 ) VALUES (
                     'session-1', 'assistant',
                     '[{\"type\":\"text\",\"text\":\"What should we build together?\"}]',
                     '2026-07-01T00:00:02.000000Z', ?1
                 )",
                [key],
            )
            .unwrap();
        db.conn().last_insert_rowid()
    });
    drop(db);
    (store, temp, opening_message_id)
}

fn claim_request(now: DateTime<Utc>, epoch: u64) -> ClaimRunRequest {
    ClaimRunRequest {
        executor_id: "executor-1".into(),
        lease_epoch: epoch,
        now,
        lease_duration: Duration::from_secs(10),
        global_concurrency_limit: 8,
    }
}

fn completion(target_status: HiveRunStatus, now: DateTime<Utc>) -> RunCompletion {
    RunCompletion {
        target_status,
        now,
        available_at: None,
        wake_at: None,
        stop_reason: Some("completed".into()),
        error: None,
        outcome: Some(serde_json::json!({"ok": true})),
        trace_sequence_end: Some(42),
    }
}

#[test]
fn claim_is_priority_ordered_and_honors_controller_concurrency() {
    let (store, _temp) = store();
    store.insert_run(&run("low", 1, 3)).unwrap();
    store.insert_run(&run("high", 50, 3)).unwrap();

    let claimed = store
        .claim_next(&claim_request(instant(0), 7))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run.id, "high");
    assert_eq!(claimed.attempt_no, 1);
    assert!(store
        .claim_next(&claim_request(instant(1), 7))
        .unwrap()
        .is_none());

    assert!(store
        .mark_running("high", &claimed.lease_token, 7, instant(1))
        .unwrap());
    assert_eq!(
        store
            .finish_claimed(
                "high",
                &claimed.lease_token,
                7,
                &completion(HiveRunStatus::Succeeded, instant(2)),
            )
            .unwrap(),
        Some(HiveRunStatus::Succeeded)
    );
    assert_eq!(
        store
            .claim_next(&claim_request(instant(3), 7))
            .unwrap()
            .unwrap()
            .run
            .id,
        "low"
    );
}

#[test]
fn claim_skips_unconsumed_expired_unresolved_only_recovery_grant() {
    let (store, temp) = store();
    let path = temp.path().join("runs.db");
    let db = Database::new(&path).unwrap();
    db.conn()
        .execute_batch(
            "UPDATE sessions
             SET model = 'test:model',
                 model_key_json = '{\"provider\":\"grok\",\"model_id\":\"test:model\",\"api_format\":\"open_ai_responses\"}',
                 model_catalog_revision = 'catalog-1',
                 permission_mode = 'autonomous'
             WHERE id = 'session-1';
             INSERT INTO hive_workers (
                 id, slug, display_name, model, model_key_json,
                 model_catalog_revision, permission_mode, autonomy, status,
                 dm_session_id, memory_namespace_id, created_at, updated_at
             ) VALUES (
                 'worker-1', 'worker-1', 'Worker 1', 'test:model',
                 '{\"provider\":\"grok\",\"model_id\":\"test:model\",\"api_format\":\"open_ai_responses\"}',
                 'catalog-1', 'autonomous', 'manual', 'active',
                 'session-1', 'worker-1',
                 '2026-07-01T00:00:00.000000Z',
                 '2026-07-01T00:00:00.000000Z'
             );
             UPDATE hive_controllers SET worker_id = 'worker-1'
             WHERE id = 'controller-1';
             INSERT INTO hive_worker_governor_override_grants (
                 id, operation_id, worker_id, owner_user_id,
                 bypass_unresolved_provider_call, bypass_daily_call_cap,
                 bypass_daily_token_cap, bypass_quiet_hours, bypass_idle_backoff,
                 reason, created_at, expires_at
             ) VALUES (
                 'expired-recovery-grant', 'expired-recovery-operation',
                 'worker-1', NULL, 1, 0, 0, 0, 0,
                 'test exact expired recovery claim fence',
                 '2026-07-01T00:00:00.000000Z',
                 '2026-07-01T00:00:01.000000Z'
             );
             INSERT INTO messages (
                 session_id, role, content, created_at, idempotency_key
             ) VALUES (
                 'session-1', 'user',
                 '[{\"type\":\"text\",\"text\":\"Resume this exact recovery turn\"}]',
                 '2026-07-01T00:00:00.000000Z',
                 'expired-recovery-objective'
             );",
        )
        .unwrap();
    let initiating_message_id = db.conn().last_insert_rowid();
    drop(db);

    let mut recovery = run("expired-recovery", 50, 2);
    recovery.kind = HiveRunKind::WorkerConversation;
    recovery.session_id = Some("session-1".into());
    recovery.worker_id = Some("worker-1".into());
    recovery.objective_message_id = Some(initiating_message_id);
    recovery.conversation_through_message_id = Some(initiating_message_id);
    recovery.execution_context = Some(
        HiveRunExecutionContextV1::worker_conversation_neutral(
            "worker-1",
            1,
            WorkerConversationLane::DirectMessage,
        )
        .unwrap(),
    );
    recovery.governor = Some(WorkerRunGovernorProjection {
        run_id: recovery.id.clone(),
        origin: Some(WorkerRunOrigin::UserDm),
        lane_key: Some("dm".into()),
        gate_reason: None,
        next_eligible_at: None,
        policy_revision: None,
        override_grant_id: Some("expired-recovery-grant".into()),
    });
    recovery.config = serde_json::json!({
        "worker_id": "worker-1",
        "model": "test:model",
        "model_key": {
            "provider": "grok",
            "model_id": "test:model",
            "api_format": "open_ai_responses"
        },
        "model_catalog_revision": "catalog-1",
        "permission_mode": "autonomous"
    });
    store.insert_run(&recovery).unwrap();
    store.insert_run(&run("ordinary", 1, 2)).unwrap();

    let claimed = store
        .claim_next(&claim_request(instant(2), 1))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run.id, "ordinary");
    let db = Database::new(&path).unwrap();
    let recovery_state: (String, i64, i64) = db
        .conn()
        .query_row(
            "SELECT run.status, run.attempt_count,
                    (SELECT COUNT(*) FROM hive_run_attempts attempt
                     WHERE attempt.run_id = run.id)
             FROM hive_runs run WHERE run.id = 'expired-recovery'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(recovery_state, ("queued".to_string(), 0, 0));
}

#[test]
fn worker_pause_fences_claim_start_heartbeat_and_finish() {
    let (store, temp) = store();
    let path = temp.path().join("runs.db");
    let db = Database::new(&path).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO hive_workers (
                 id, slug, display_name, model, permission_mode, autonomy, status,
                 dm_session_id, memory_namespace_id, created_at, updated_at
             ) VALUES (
                 'worker-fence', 'worker-fence', 'Worker Fence', 'test:model',
                 'supervised', 'manual', 'paused', 'session-1', 'worker-fence',
                 '2026-07-01T00:00:00.000000Z',
                 '2026-07-01T00:00:00.000000Z'
             );
             UPDATE hive_controllers SET worker_id = 'worker-fence'
             WHERE id = 'controller-1';",
        )
        .unwrap();
    drop(db);

    let mut queued = run("worker-queued", 10, 3);
    queued.session_id = Some("session-1".into());
    queued.kind = HiveRunKind::WorkerHeartbeat;
    queued.worker_id = Some("worker-fence".into());
    queued.execution_context = Some(
        HiveRunExecutionContextV1::worker_conversation_neutral(
            "worker-fence",
            1,
            WorkerConversationLane::DirectMessage,
        )
        .unwrap(),
    );
    queued.governor = Some(WorkerRunGovernorProjection {
        run_id: queued.id.clone(),
        origin: Some(WorkerRunOrigin::Heartbeat),
        lane_key: Some("dm".into()),
        gate_reason: None,
        next_eligible_at: None,
        policy_revision: None,
        override_grant_id: None,
    });
    queued.config = serde_json::json!({
        "worker_id": "worker-fence",
        "model": "test:model",
        "model_key": null,
        "model_catalog_revision": null,
        "permission_mode": "supervised",
    });
    store.insert_run(&queued).unwrap();
    assert!(store
        .claim_next(&claim_request(instant(0), 7))
        .unwrap()
        .is_none());

    Database::new(&path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_workers SET status = 'active' WHERE id = 'worker-fence'",
            [],
        )
        .unwrap();
    let claimed_before_pause = store
        .claim_next(&claim_request(instant(0), 7))
        .unwrap()
        .unwrap();
    Database::new(&path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_workers SET status = 'paused' WHERE id = 'worker-fence'",
            [],
        )
        .unwrap();
    assert!(!store
        .mark_running(
            &claimed_before_pause.run.id,
            &claimed_before_pause.lease_token,
            7,
            instant(1),
        )
        .unwrap());

    Database::new(&path)
        .unwrap()
        .conn()
        .execute_batch(
            "UPDATE hive_workers SET status = 'active' WHERE id = 'worker-fence';
             UPDATE hive_runs
             SET status = 'queued', lease_owner = NULL, lease_token = NULL,
                 lease_epoch = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
                 attempt_count = 0
             WHERE id = 'worker-queued';
             DELETE FROM hive_run_attempts WHERE run_id = 'worker-queued';",
        )
        .unwrap();
    let running = store
        .claim_next(&claim_request(instant(2), 8))
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running(&running.run.id, &running.lease_token, 8, instant(3))
        .unwrap());
    Database::new(&path)
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_workers SET status = 'archived' WHERE id = 'worker-fence'",
            [],
        )
        .unwrap();
    assert!(!store
        .heartbeat(
            &running.run.id,
            &running.lease_token,
            8,
            instant(4),
            Duration::from_secs(10),
        )
        .unwrap());
    assert_eq!(
        store
            .finish_claimed(
                &running.run.id,
                &running.lease_token,
                8,
                &completion(HiveRunStatus::Succeeded, instant(5)),
            )
            .unwrap(),
        None,
        "a late provider result cannot finish after the Worker becomes inactive"
    );
}

#[test]
fn first_transition_is_journaled_for_an_empty_controller_event_stream() {
    let (store, temp) = store();
    store.insert_run(&run("run-1", 0, 3)).unwrap();

    store
        .claim_next(&claim_request(instant(0), 7))
        .unwrap()
        .expect("run should be claimed");

    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    let event: (i64, String, String, String) = db
        .conn()
        .query_row(
            "SELECT sequence, event_type, dedupe_key, payload_json
             FROM hive_controller_events WHERE controller_id = 'controller-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("the first transition must create the first controller event");
    assert_eq!(event.0, 1);
    assert_eq!(event.1, "run_leased");
    assert_eq!(event.2, "transition:run-1:1:leased");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&event.3).unwrap()["status"],
        "leased"
    );
}

#[test]
fn transition_rolls_back_if_its_authoritative_event_cannot_be_written() {
    let (store, temp) = store();
    store.insert_run(&run("run-1", 0, 3)).unwrap();
    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    db.conn()
        .execute_batch(
            "CREATE TRIGGER reject_hive_event
             BEFORE INSERT ON hive_controller_events
             BEGIN
                 SELECT RAISE(ABORT, 'simulated journal failure');
             END;",
        )
        .unwrap();
    drop(db);

    let error = store
        .claim_next(&claim_request(instant(0), 7))
        .expect_err("claim must fail when its transition event cannot commit");
    assert!(error.to_string().contains("simulated journal failure"));
    let persisted = store.get_run("run-1").unwrap().unwrap();
    assert_eq!(persisted.status, HiveRunStatus::Queued);
    assert_eq!(persisted.attempt_count, 0);
    assert!(store.list_attempts("run-1").unwrap().is_empty());
}

#[test]
fn lease_token_and_epoch_fence_heartbeats_and_completion() {
    let (store, _temp) = store();
    store.insert_run(&run("run-1", 0, 3)).unwrap();
    let claimed = store
        .claim_next(&claim_request(instant(0), 11))
        .unwrap()
        .unwrap();

    assert!(!store
        .heartbeat(
            "run-1",
            "stale-token",
            11,
            instant(1),
            Duration::from_secs(10)
        )
        .unwrap());
    assert!(!store
        .mark_running("run-1", &claimed.lease_token, 10, instant(1))
        .unwrap());
    assert!(store
        .mark_running_with_trace("run-1", &claimed.lease_token, 11, instant(1), Some(8))
        .unwrap());
    assert!(store
        .heartbeat(
            "run-1",
            &claimed.lease_token,
            11,
            instant(2),
            Duration::from_secs(10),
        )
        .unwrap());
    assert_eq!(
        store
            .finish_claimed(
                "run-1",
                &claimed.lease_token,
                10,
                &completion(HiveRunStatus::Succeeded, instant(3)),
            )
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .finish_claimed(
                "run-1",
                &claimed.lease_token,
                11,
                &completion(HiveRunStatus::Succeeded, instant(3)),
            )
            .unwrap(),
        Some(HiveRunStatus::Succeeded)
    );

    let attempts = store.list_attempts("run-1").unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].outcome, HiveRunAttemptOutcome::Succeeded);
    assert_eq!(attempts[0].trace_sequence_start, Some(8));
    assert_eq!(attempts[0].trace_sequence_end, Some(42));
}

#[test]
fn disabled_controller_fences_mark_running_before_backend_side_effects() {
    let (store, temp) = store();
    store.insert_run(&run("run-1", 0, 3)).unwrap();
    let claimed = store
        .claim_next(&claim_request(instant(0), 11))
        .unwrap()
        .unwrap();
    Database::new(&temp.path().join("runs.db"))
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_controllers SET status = 'disabled' WHERE id = 'controller-1'",
            [],
        )
        .unwrap();

    assert!(!store
        .mark_running("run-1", &claimed.lease_token, 11, instant(1))
        .unwrap());
    assert!(!store
        .heartbeat(
            "run-1",
            &claimed.lease_token,
            11,
            instant(1),
            Duration::from_secs(10),
        )
        .unwrap());
    assert_eq!(
        store.get_run("run-1").unwrap().unwrap().status,
        HiveRunStatus::Leased
    );
}

#[test]
fn expired_unstarted_lease_is_requeued() {
    let (store, _temp) = store();
    store.insert_run(&run("run-1", 0, 3)).unwrap();

    store
        .claim_next(&claim_request(instant(0), 1))
        .unwrap()
        .unwrap();
    let reconciled = store.reconcile_expired_leases(instant(11)).unwrap();
    assert_eq!(reconciled.requeued_unstarted, 1);
    assert_eq!(reconciled.recovery_required, 0);
    assert_eq!(
        store.get_run("run-1").unwrap().unwrap().status,
        HiveRunStatus::Queued
    );
    assert_eq!(
        store.list_attempts("run-1").unwrap()[0].outcome,
        HiveRunAttemptOutcome::Abandoned
    );
}

#[test]
fn expired_running_delivery_requires_recovery() {
    let (store, _temp) = store();
    store.insert_run(&run("run-1", 0, 3)).unwrap();
    let claimed = store
        .claim_next(&claim_request(instant(0), 1))
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running("run-1", &claimed.lease_token, 1, instant(1))
        .unwrap());
    store.insert_run(&run("queued-sibling", 0, 3)).unwrap();

    let reconciled = store.reconcile_expired_leases(instant(11)).unwrap();
    assert_eq!(reconciled.requeued_unstarted, 0);
    assert_eq!(reconciled.recovery_required, 1);
    assert_eq!(
        store.get_run("run-1").unwrap().unwrap().status,
        HiveRunStatus::RecoveryRequired
    );
    assert!(store
        .claim_next(&claim_request(instant(12), 2))
        .unwrap()
        .is_none());
}

#[test]
fn expired_running_group_turn_is_requeued() {
    let (store, _temp) = store();
    let mut group_turn = run("group-turn-1", 0, 3);
    group_turn.kind = HiveRunKind::GroupTurn;
    store.insert_run(&group_turn).unwrap();
    let claimed = store
        .claim_next(&claim_request(instant(0), 1))
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running("group-turn-1", &claimed.lease_token, 1, instant(1))
        .unwrap());

    let reconciled = store.reconcile_expired_leases(instant(11)).unwrap();
    assert_eq!(reconciled.requeued_unstarted, 1);
    assert_eq!(reconciled.recovery_required, 0);
    assert_eq!(
        store.get_run("group-turn-1").unwrap().unwrap().status,
        HiveRunStatus::Queued
    );
    assert_eq!(
        store
            .claim_next(&claim_request(instant(12), 2))
            .unwrap()
            .unwrap()
            .run
            .id,
        "group-turn-1"
    );
}

#[test]
fn expired_running_introduction_adopts_committed_opening_without_replay() {
    let (store, temp, opening_message_id) =
        running_worker_introduction(Some("introduction:introduction-1:opening"));
    let opening_message_id = opening_message_id.unwrap();

    let reconciled = store.reconcile_expired_leases(instant(11)).unwrap();
    assert_eq!(reconciled.recovered_succeeded, 1);
    assert_eq!(reconciled.requeued_unstarted, 0);
    assert_eq!(reconciled.recovery_required, 0);
    assert_eq!(
        reconciled.recovered_succeeded_runs[0].run_id,
        "introduction-1"
    );

    let run = store.get_run("introduction-1").unwrap().unwrap();
    assert_eq!(run.status, HiveRunStatus::Succeeded);
    assert!(run.finished_at.is_some());
    assert!(run.last_error.is_none());
    assert_eq!(
        run.outcome.as_ref().unwrap()["recovered"],
        "committed_introduction_opening"
    );
    assert_eq!(
        store.list_attempts("introduction-1").unwrap()[0].outcome,
        HiveRunAttemptOutcome::Succeeded
    );

    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    let (status, persisted_opening, runtime_status, controller_status): (
        String,
        Option<i64>,
        String,
        String,
    ) = db
        .conn()
        .query_row(
            "SELECT introduction.status, introduction.opening_message_id,
                    runtime.status, controller.status
             FROM hive_worker_introductions introduction
             JOIN hive_runs run ON run.id = introduction.run_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             JOIN hive_runtime_state runtime ON runtime.session_id = run.session_id
             WHERE introduction.run_id = 'introduction-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(status, "awaiting_context");
    assert_eq!(persisted_opening, Some(opening_message_id));
    assert_eq!(runtime_status, "idle");
    assert_eq!(controller_status, "active");

    let second = store.reconcile_expired_leases(instant(12)).unwrap();
    assert_eq!(second, Default::default());
}

#[test]
fn expired_running_introduction_without_exact_opening_requires_explicit_recovery() {
    let (store, temp, _) = running_worker_introduction(Some("unrelated:assistant-row"));

    let reconciled = store.reconcile_expired_leases(instant(11)).unwrap();
    assert_eq!(reconciled.recovered_succeeded, 0);
    assert_eq!(reconciled.requeued_unstarted, 0);
    assert_eq!(reconciled.recovery_required, 1);
    assert_eq!(
        store.get_run("introduction-1").unwrap().unwrap().status,
        HiveRunStatus::RecoveryRequired
    );
    assert_eq!(
        store.list_attempts("introduction-1").unwrap()[0].outcome,
        HiveRunAttemptOutcome::RecoveryRequired
    );

    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    let (status, opening_message_id, error, controller_status): (
        String,
        Option<i64>,
        String,
        String,
    ) = db
        .conn()
        .query_row(
            "SELECT introduction.status, introduction.opening_message_id,
                    introduction.last_error, controller.status
             FROM hive_worker_introductions introduction
             JOIN hive_runs run ON run.id = introduction.run_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE introduction.run_id = 'introduction-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(status, "needs_recovery");
    assert_eq!(opening_message_id, None);
    assert!(error.contains("explicit retry or skip"));
    assert_eq!(controller_status, "paused");
    assert!(store
        .claim_next(&claim_request(instant(12), 2))
        .unwrap()
        .is_none());
}

#[test]
fn fenced_expired_introduction_without_exact_opening_requires_explicit_recovery() {
    let (store, temp, _) = running_worker_introduction(None);
    let path = temp.path().join("runs.db");
    let db = Database::new(&path).unwrap();
    let (introduction_status, opening_message_id, provider_calls): (String, Option<i64>, i64) = db
        .conn()
        .query_row(
            "SELECT introduction.status, introduction.opening_message_id,
                    (SELECT COUNT(*) FROM hive_worker_provider_calls
                     WHERE run_id = introduction.run_id)
             FROM hive_worker_introductions introduction
             WHERE introduction.run_id = 'introduction-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(introduction_status, "running");
    assert_eq!(opening_message_id, None);
    assert_eq!(provider_calls, 0);
    drop(db);

    let lease = match HiveDaemonLeaseStore::new(Database::new(&path).unwrap())
        .acquire(
            "hive-scheduler",
            "daemon-a",
            instant(10),
            Duration::from_secs(10),
        )
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        held => panic!("unexpected lease result: {held:?}"),
    };
    let fence = DaemonFence {
        lease_name: lease.lease_name,
        owner_id: lease.owner_id,
        fencing_token: lease.fencing_token,
    };

    let reconciled = store
        .reconcile_expired_leases_fenced(instant(11), &fence)
        .unwrap();
    assert_eq!(reconciled.requeued_unstarted, 0);
    assert_eq!(reconciled.recovery_required, 1);
    assert_eq!(
        store.get_run("introduction-1").unwrap().unwrap().status,
        HiveRunStatus::RecoveryRequired
    );
    assert_eq!(
        store.list_attempts("introduction-1").unwrap()[0].outcome,
        HiveRunAttemptOutcome::RecoveryRequired
    );

    let db = Database::new(&path).unwrap();
    let (introduction_status, controller_status): (String, String) = db
        .conn()
        .query_row(
            "SELECT introduction.status, controller.status
             FROM hive_worker_introductions introduction
             JOIN hive_runs run ON run.id = introduction.run_id
             JOIN hive_controllers controller ON controller.id = run.controller_id
             WHERE introduction.run_id = 'introduction-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(introduction_status, "needs_recovery");
    assert_eq!(controller_status, "paused");
}

#[test]
fn second_daemon_takeover_rejects_stale_completion() {
    let (store, temp) = store();
    let lease_store = HiveDaemonLeaseStore::new(
        Database::new(&temp.path().join("runs.db")).expect("daemon lease database"),
    );
    let first = match lease_store
        .acquire(
            "hive-scheduler",
            "daemon-a",
            instant(0),
            Duration::from_secs(10),
        )
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        held => panic!("unexpected lease result: {held:?}"),
    };
    let first_fence = DaemonFence {
        lease_name: first.lease_name,
        owner_id: first.owner_id,
        fencing_token: first.fencing_token,
    };
    store.insert_run(&run("run-1", 0, 3)).unwrap();
    let mut request = claim_request(instant(0), first_fence.fencing_token);
    request.lease_duration = Duration::from_secs(100);
    let claimed = store
        .claim_next_fenced(&request, &first_fence)
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running_fenced(
            "run-1",
            &claimed.lease_token,
            first_fence.fencing_token,
            instant(1),
            &first_fence,
        )
        .unwrap());

    let second = match lease_store
        .acquire(
            "hive-scheduler",
            "daemon-b",
            instant(11),
            Duration::from_secs(10),
        )
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        held => panic!("unexpected lease result: {held:?}"),
    };
    assert!(second.fencing_token > first_fence.fencing_token);

    assert_eq!(
        store
            .finish_claimed_fenced(
                "run-1",
                &claimed.lease_token,
                first_fence.fencing_token,
                &completion(HiveRunStatus::Succeeded, instant(12)),
                &first_fence,
            )
            .unwrap(),
        None
    );
    assert_eq!(
        store.get_run("run-1").unwrap().unwrap().status,
        HiveRunStatus::Running
    );
    let journal_db = Database::new(&temp.path().join("runs.db")).unwrap();
    let journal = journal_db
        .conn()
        .prepare(
            "SELECT event_type FROM hive_controller_events
             WHERE run_id = 'run-1' ORDER BY sequence",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(journal, vec!["run_leased", "run_started"]);
}

#[test]
fn execution_host_revalidates_exact_claim_and_immutable_inputs() {
    let (store, temp) = store();
    let lease_store = HiveDaemonLeaseStore::new(
        Database::new(&temp.path().join("runs.db")).expect("daemon lease database"),
    );
    let lease = match lease_store
        .acquire(
            "hive-scheduler",
            "executor-1",
            instant(0),
            Duration::from_secs(10),
        )
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        held => panic!("unexpected lease result: {held:?}"),
    };
    let fence = DaemonFence {
        lease_name: lease.lease_name,
        owner_id: lease.owner_id,
        fencing_token: lease.fencing_token,
    };
    let mut scheduled = run("fenced", 0, 3);
    scheduled.session_id = Some("session-1".into());
    scheduled.kind = HiveRunKind::Scheduled;
    scheduled.objective = "Inspect the immutable deployment target".into();
    scheduled.config = serde_json::json!({
        "working_dir": "/work/original",
        "project_dir": "/work/original/project",
        "model": "provider:claimed-model",
        "crew_slug": "release",
    });
    let objective_db = Database::new(&temp.path().join("runs.db")).unwrap();
    let objective_content = serde_json::to_string(&vec![crate::ai::types::Content::text(format!(
        "Hive scheduled objective:\n{}",
        scheduled.objective
    ))])
    .unwrap();
    objective_db
        .conn()
        .execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES ('session-1', 'user', ?1, ?2)",
            rusqlite::params![objective_content, timestamp(instant(0))],
        )
        .unwrap();
    scheduled.objective_message_id = Some(objective_db.conn().last_insert_rowid());
    drop(objective_db);
    store.insert_run(&scheduled).unwrap();
    let mut request = claim_request(instant(0), fence.fencing_token);
    request.lease_duration = Duration::from_secs(20);
    let claim = store.claim_next_fenced(&request, &fence).unwrap().unwrap();
    assert!(store
        .mark_running_fenced(
            &claim.run.id,
            &claim.lease_token,
            fence.fencing_token,
            instant(1),
            &fence,
        )
        .unwrap());
    assert!(store
        .validate_claimed_execution_fenced(&claim, &fence, instant(2))
        .unwrap());

    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs SET config_json = json_set(config_json, '$.model', 'provider:mutated')
             WHERE id = ?1",
            [&claim.run.id],
        )
        .unwrap();
    assert!(!store
        .validate_claimed_execution_fenced(&claim, &fence, instant(2))
        .unwrap());
}

#[test]
fn exhausted_retry_budget_dead_letters_the_run() {
    let (store, _temp) = store();
    store.insert_run(&run("run-1", 0, 1)).unwrap();
    let claimed = store
        .claim_next(&claim_request(instant(0), 1))
        .unwrap()
        .unwrap();
    store
        .mark_running("run-1", &claimed.lease_token, 1, instant(1))
        .unwrap();
    let mut retry = completion(HiveRunStatus::RetryWait, instant(2));
    retry.available_at = Some(instant(20));

    assert_eq!(
        store
            .finish_claimed("run-1", &claimed.lease_token, 1, &retry)
            .unwrap(),
        Some(HiveRunStatus::DeadLetter)
    );
    let attempts = store.list_attempts("run-1").unwrap();
    assert_eq!(attempts[0].outcome, HiveRunAttemptOutcome::DeadLetter);
    assert_eq!(attempts[0].retry_at, None);
}

#[test]
fn cancellation_fences_an_active_worker_and_closes_its_attempt() {
    let (store, _temp) = store();
    store.insert_run(&run("run-1", 0, 3)).unwrap();
    let claimed = store
        .claim_next(&claim_request(instant(0), 4))
        .unwrap()
        .unwrap();
    store
        .mark_running("run-1", &claimed.lease_token, 4, instant(1))
        .unwrap();

    assert!(store
        .cancel("run-1", instant(2), "cancelled by user")
        .unwrap());
    assert_eq!(
        store.get_run("run-1").unwrap().unwrap().status,
        HiveRunStatus::Cancelled
    );
    assert_eq!(
        store.list_attempts("run-1").unwrap()[0].outcome,
        HiveRunAttemptOutcome::Cancelled
    );
    assert_eq!(
        store
            .finish_claimed(
                "run-1",
                &claimed.lease_token,
                4,
                &completion(HiveRunStatus::Succeeded, instant(3)),
            )
            .unwrap(),
        None
    );
}

#[test]
fn committed_cancellation_requires_exact_live_claim_and_daemon_fence() {
    let (store, temp) = store();
    let lease = match HiveDaemonLeaseStore::new(
        Database::new(&temp.path().join("runs.db")).expect("daemon lease database"),
    )
    .acquire(
        "hive-scheduler",
        "executor-1",
        instant(0),
        Duration::from_secs(10),
    )
    .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        held => panic!("unexpected lease result: {held:?}"),
    };
    let fence = DaemonFence {
        lease_name: lease.lease_name,
        owner_id: lease.owner_id,
        fencing_token: lease.fencing_token,
    };
    store.insert_run(&run("run-1", 0, 3)).unwrap();
    let claimed = store
        .claim_next_fenced(&claim_request(instant(0), fence.fencing_token), &fence)
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running_fenced(
            "run-1",
            &claimed.lease_token,
            fence.fencing_token,
            instant(1),
            &fence,
        )
        .unwrap());
    let mut cancelled = completion(HiveRunStatus::Cancelled, instant(2));
    cancelled.stop_reason = Some("cancellation grace elapsed".into());
    cancelled.error = Some("side effects may be uncertain".into());
    cancelled.outcome = Some(serde_json::json!({
        "kind": "cancelled",
        "forced": true,
    }));

    assert_eq!(
        store
            .finish_cancelled_claim_fenced(
                "run-1",
                &claimed.lease_token,
                fence.fencing_token,
                &cancelled,
                &fence,
            )
            .unwrap(),
        None,
        "an active controller is not a committed CancelSession"
    );
    Database::new(&temp.path().join("runs.db"))
        .unwrap()
        .conn()
        .execute(
            "UPDATE hive_controllers SET status = 'disabled' WHERE id = 'controller-1'",
            [],
        )
        .unwrap();
    assert_eq!(
        store
            .finish_cancelled_claim_fenced(
                "run-1",
                "wrong-token",
                fence.fencing_token,
                &cancelled,
                &fence,
            )
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .finish_cancelled_claim_fenced(
                "run-1",
                &claimed.lease_token,
                fence.fencing_token,
                &cancelled,
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Cancelled)
    );
    assert_eq!(
        store.list_attempts("run-1").unwrap()[0].outcome,
        HiveRunAttemptOutcome::Cancelled
    );
    assert_eq!(
        store
            .finish_claimed_fenced(
                "run-1",
                &claimed.lease_token,
                fence.fencing_token,
                &completion(HiveRunStatus::Succeeded, instant(3)),
                &fence,
            )
            .unwrap(),
        None,
        "late worker completion must not overwrite authoritative cancellation"
    );
}

#[test]
fn stopped_worker_conversation_finishes_with_active_controller_and_accounts_provider_permit() {
    let (store, temp) = store();
    let path = temp.path().join("runs.db");
    let db = Database::new(&path).unwrap();
    db.conn()
        .execute_batch(
            r#"INSERT INTO hive_workers (
                   id, slug, display_name, model, model_key_json,
                   model_catalog_revision, permission_mode, autonomy, status,
                   dm_session_id, memory_namespace_id, created_at, updated_at
               ) VALUES (
                   'worker-stop', 'worker-stop', 'Worker Stop', 'test-model',
                   '{"provider":"grok","model_id":"test-model","api_format":"open_ai_responses"}',
                   'catalog-1', 'autonomous', 'manual', 'active',
                   'session-1', 'worker-stop',
                   '2026-07-01T00:00:00.000000Z',
                   '2026-07-01T00:00:00.000000Z'
               );
               UPDATE hive_controllers SET worker_id = 'worker-stop'
               WHERE id = 'controller-1';
               INSERT INTO hive_worker_introductions (
                   worker_id, run_id, status, prompt_version,
                   created_at, updated_at, completed_at
               ) VALUES (
                   'worker-stop', NULL, 'confirmed', 1,
                   '2026-07-01T00:00:00.000000Z',
                   '2026-07-01T00:00:00.000000Z',
                   '2026-07-01T00:00:00.000000Z'
               );
               INSERT INTO messages (
                   session_id, role, content, created_at, idempotency_key
               ) VALUES (
                   'session-1', 'user',
                   '[{"type":"text","text":"Please stop this turn"}]',
                   '2026-07-01T00:00:00.000000Z', 'worker-stop-objective'
               );"#,
        )
        .unwrap();
    let objective_message_id = db.conn().last_insert_rowid();
    drop(db);

    let mut conversation = run("worker-stop-run", 50, 3);
    conversation.session_id = Some("session-1".into());
    conversation.worker_id = Some("worker-stop".into());
    conversation.kind = HiveRunKind::WorkerConversation;
    conversation.objective_message_id = Some(objective_message_id);
    conversation.conversation_through_message_id = Some(objective_message_id);
    conversation.execution_context = Some(
        HiveRunExecutionContextV1::worker_conversation_neutral(
            "worker-stop",
            1,
            WorkerConversationLane::DirectMessage,
        )
        .unwrap(),
    );
    conversation.governor = Some(WorkerRunGovernorProjection {
        run_id: conversation.id.clone(),
        origin: Some(WorkerRunOrigin::UserDm),
        lane_key: Some("dm".into()),
        gate_reason: None,
        next_eligible_at: None,
        policy_revision: None,
        override_grant_id: None,
    });
    conversation.config = serde_json::json!({
        "worker_id": "worker-stop",
        "worker_revision": 1,
        "model": "test-model",
        "model_key": {
            "provider": "grok",
            "model_id": "test-model",
            "api_format": "open_ai_responses"
        },
        "model_catalog_revision": "catalog-1",
        "permission_mode": "autonomous"
    });
    store.insert_run(&conversation).unwrap();

    let lease = match HiveDaemonLeaseStore::new(Database::new(&path).unwrap())
        .acquire(
            "hive-scheduler",
            "executor-1",
            instant(0),
            Duration::from_secs(10),
        )
        .unwrap()
    {
        DaemonLeaseAcquire::Acquired(lease) => lease,
        held => panic!("unexpected lease result: {held:?}"),
    };
    let fence = DaemonFence {
        lease_name: lease.lease_name,
        owner_id: lease.owner_id,
        fencing_token: lease.fencing_token,
    };
    let claimed = store
        .claim_next_fenced(&claim_request(instant(0), fence.fencing_token), &fence)
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running_fenced(
            &conversation.id,
            &claimed.lease_token,
            fence.fencing_token,
            instant(1),
            &fence,
        )
        .unwrap());
    let db = Database::new(&path).unwrap();
    db.conn()
        .execute(
            r#"INSERT INTO hive_worker_provider_calls (
                   provider_call_id, worker_id, worker_revision, owner_user_id,
                   session_id, run_id, run_lease_token, run_lease_epoch,
                   run_lease_expires_at, origin, lane_key, call_kind,
                   provider_id, model_id, model_key_json, model_key_fingerprint,
                   model_catalog_revision, permission_mode, policy_revision,
                   timezone, local_day, reserved_tokens, started_at
               ) VALUES (
                   'worker-stop-call', 'worker-stop', 1, NULL,
                   'session-1', 'worker-stop-run', ?1, ?2,
                   '2026-07-01T00:00:10.000000Z', 'user_dm', 'dm', 'agent_turn',
                   'grok', 'test-model',
                   '{"provider":"grok","model_id":"test-model","api_format":"open_ai_responses"}',
                   ?3, 'catalog-1', 'autonomous', 1, 'UTC', '2026-07-01', 64,
                   '2026-07-01T00:00:01.000000Z'
               )"#,
            rusqlite::params![claimed.lease_token, fence.fencing_token, "a".repeat(64)],
        )
        .unwrap();
    db.conn()
        .execute_batch(
            r#"INSERT INTO hive_worker_provider_calls (
                   provider_call_id, worker_id, worker_revision, owner_user_id,
                   session_id, group_id, run_id, run_lease_token, run_lease_epoch,
                   run_lease_expires_at, workflow_goal_id, workflow_attempt_id,
                   origin, lane_key, call_kind, provider_id, model_id,
                   model_key_json, model_key_fingerprint, model_catalog_revision,
                   permission_mode, pricing_snapshot_json, policy_revision,
                   timezone, local_day, reserved_tokens, override_grant_id, started_at
               )
               SELECT 'worker-stop-completed-call', worker_id, worker_revision, owner_user_id,
                      session_id, group_id, run_id, run_lease_token, run_lease_epoch,
                      run_lease_expires_at, workflow_goal_id, workflow_attempt_id,
                      origin, lane_key, call_kind, provider_id, model_id,
                      model_key_json, model_key_fingerprint, model_catalog_revision,
                      permission_mode, pricing_snapshot_json, policy_revision,
                      timezone, local_day, reserved_tokens, override_grant_id,
                      '2026-07-01T00:00:01.500000Z'
               FROM hive_worker_provider_calls
               WHERE provider_call_id = 'worker-stop-call';
               INSERT INTO hive_worker_provider_call_outcomes (
                   provider_call_id, state, outcome, remote_acceptance,
                   usage_json, usage_total_tokens, estimated_cost_microunits,
                   unknown_reason, finished_at
               ) VALUES (
                   'worker-stop-completed-call', 'completed',
                   'cancelled_after_acceptance', 'acknowledged',
                   '{"input_tokens":3,"output_tokens":2,"total_tokens":5}',
                   5, 77, NULL, '2026-07-01T00:00:01.750000Z'
               );"#,
        )
        .unwrap();
    db.conn()
        .execute(
            "UPDATE hive_runs SET last_stop_reason = ?2 WHERE id = ?1",
            rusqlite::params![
                conversation.id,
                super::WORKER_CONVERSATION_STOP_REQUESTED_REASON
            ],
        )
        .unwrap();
    drop(db);

    let mut cancelled = completion(HiveRunStatus::Cancelled, instant(2));
    cancelled.stop_reason = Some("backend acknowledged cancellation".into());
    assert_eq!(
        store
            .finish_stopped_worker_conversation_claim_fenced(
                &conversation.id,
                &claimed.lease_token,
                fence.fencing_token,
                &cancelled,
                &fence,
            )
            .unwrap(),
        Some(HiveRunStatus::Cancelled)
    );
    let db = Database::new(&path).unwrap();
    let projection: (String, String, String, String, i64) = db
        .conn()
        .query_row(
            "SELECT run.status, controller.status, outcome.state,
                    outcome.outcome,
                    (SELECT COUNT(*)
                     FROM hive_worker_provider_calls call
                     LEFT JOIN hive_worker_provider_call_outcomes terminal
                       ON terminal.provider_call_id = call.provider_call_id
                     WHERE call.worker_id = 'worker-stop'
                       AND (terminal.state IS NULL OR terminal.state = 'unknown'))
             FROM hive_runs run
             JOIN hive_controllers controller ON controller.id = run.controller_id
             JOIN hive_worker_provider_call_outcomes outcome
               ON outcome.provider_call_id = 'worker-stop-call'
             WHERE run.id = 'worker-stop-run'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        projection,
        (
            "cancelled".into(),
            "active".into(),
            "completed".into(),
            "cancelled_by_user".into(),
            0,
        )
    );
    let preserved: (String, String, i64, i64) = db
        .conn()
        .query_row(
            "SELECT outcome, remote_acceptance, usage_total_tokens,
                    estimated_cost_microunits
             FROM hive_worker_provider_call_outcomes
             WHERE provider_call_id = 'worker-stop-completed-call'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        preserved,
        (
            "cancelled_after_acceptance".into(),
            "acknowledged".into(),
            5,
            77
        )
    );
}

#[test]
fn due_run_projection_does_not_clobber_an_active_sibling() {
    let (store, temp) = store();
    let mut active = run("active", 50, 3);
    active.session_id = Some("session-1".into());
    store.insert_run(&active).unwrap();

    let claimed = store
        .claim_next(&claim_request(instant(0), 1))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run.id, "active");
    assert!(store
        .mark_running("active", &claimed.lease_token, 1, instant(1))
        .unwrap());

    let mut sleeping = run("sleeping", 0, 3);
    sleeping.session_id = Some("session-1".into());
    store.insert_run(&sleeping).unwrap();
    let projection_db = Database::new(&temp.path().join("runs.db")).unwrap();
    projection_db
        .conn()
        .execute(
            "UPDATE hive_runs SET status = 'sleeping', wake_at = ?2 WHERE id = ?1",
            rusqlite::params![sleeping.id, timestamp(instant(2))],
        )
        .unwrap();
    drop(projection_db);
    assert_eq!(store.promote_due_runs(instant(2)).unwrap(), 1);

    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    let (runtime_status, current_run_id): (String, String) = db
        .conn()
        .query_row(
            "SELECT status, current_run_id FROM hive_runtime_state
             WHERE session_id = 'session-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(runtime_status, "running");
    assert_eq!(current_run_id, "active");
}

#[test]
fn scheduled_objective_is_materialized_exactly_once_across_retry() {
    let (store, temp) = store();
    let mut scheduled = run("scheduled", 0, 3);
    scheduled.session_id = Some("session-1".into());
    scheduled.kind = HiveRunKind::Scheduled;
    scheduled.objective = "Inspect the deployment health".into();
    store.insert_run(&scheduled).unwrap();

    let first = store
        .claim_next(&claim_request(instant(0), 1))
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running("scheduled", &first.lease_token, 1, instant(1))
        .unwrap());
    let mut retry = completion(HiveRunStatus::RetryWait, instant(2));
    retry.available_at = Some(instant(3));
    assert_eq!(
        store
            .finish_claimed("scheduled", &first.lease_token, 1, &retry)
            .unwrap(),
        Some(HiveRunStatus::RetryWait)
    );
    assert_eq!(store.promote_due_runs(instant(3)).unwrap(), 1);
    let second = store
        .claim_next(&claim_request(instant(3), 1))
        .unwrap()
        .unwrap();
    assert!(store
        .mark_running("scheduled", &second.lease_token, 1, instant(4))
        .unwrap());

    let db = Database::new(&temp.path().join("runs.db")).unwrap();
    let (messages, episodes, objective_message_id): (i64, i64, Option<i64>) = db
        .conn()
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM messages
                  WHERE session_id = 'session-1' AND role = 'user'
                    AND content LIKE '%deployment health%'),
                 (SELECT COUNT(*) FROM conversation_episodes
                  WHERE session_id = 'session-1' AND role = 'user'
                    AND body LIKE '%deployment health%'),
                 objective_message_id
             FROM hive_runs WHERE id = 'scheduled'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(messages, 1);
    assert_eq!(episodes, 1);
    assert!(objective_message_id.is_some());
}
