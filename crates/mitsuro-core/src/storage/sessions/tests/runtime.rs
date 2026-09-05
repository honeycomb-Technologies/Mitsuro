use super::create_test_db;
use crate::storage::sessions::SessionManager;
use crate::storage::Database;

#[test]
fn test_agent_state_management() {
    // Test agent state tracking and updates
    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);

    let session_id = manager
        .create_session("Test Session", Some("claude-3-5-sonnet"), Some("/tmp"))
        .expect("Failed to create session");

    // Initially idle
    let state = manager.get_agent_state(&session_id);
    assert!(state.is_some(), "Should have agent state");
    assert_eq!(state.unwrap().state, "idle", "Initial state should be idle");

    // Update to streaming
    manager
        .set_agent_state(&session_id, "streaming")
        .expect("Failed to update agent state");

    let state = manager.get_agent_state(&session_id).unwrap();
    assert_eq!(state.state, "streaming");
    assert!(state.started_at.is_some(), "Should have started_at");
    assert!(state.last_event_at.is_some(), "Should have last_event_at");

    // Update back to idle
    manager
        .set_agent_state(&session_id, "idle")
        .expect("Failed to update agent state");

    let state = manager.get_agent_state(&session_id).unwrap();
    assert_eq!(state.state, "idle");
    assert!(state.started_at.is_none(), "Idle should clear started_at");
}

#[test]
fn test_list_active_sessions() {
    // Test filtering sessions by agent activity
    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);

    let session1 = manager
        .create_session("Active 1", Some("claude-3-5-sonnet"), Some("/tmp"))
        .expect("Failed to create session");
    let session2 = manager
        .create_session("Active 2", Some("claude-3-5-sonnet"), Some("/tmp"))
        .expect("Failed to create session");
    let _session3 = manager
        .create_session("Idle", Some("claude-3-5-sonnet"), Some("/tmp"))
        .expect("Failed to create session");

    // Set two sessions to active states
    manager
        .set_agent_state(&session1, "streaming")
        .expect("Failed to update state");
    manager
        .set_agent_state(&session2, "tool_executing")
        .expect("Failed to update state");

    // List active sessions
    let active = manager
        .list_active_sessions()
        .expect("Failed to list active sessions");

    assert_eq!(active.len(), 2, "Should have 2 active sessions");

    let active_ids: Vec<&str> = active.iter().map(|(id, _)| id.as_str()).collect();
    assert!(active_ids.contains(&session1.as_str()));
    assert!(active_ids.contains(&session2.as_str()));
}

#[test]
fn test_touch_agent_event() {
    // Test updating agent activity timestamp
    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);

    let session_id = manager
        .create_session("Test Session", Some("claude-3-5-sonnet"), Some("/tmp"))
        .expect("Failed to create session");

    manager
        .set_agent_state(&session_id, "streaming")
        .expect("Failed to set streaming");

    // Touch the event
    manager
        .touch_agent_event(&session_id)
        .expect("Failed to touch event");

    let state = manager.get_agent_state(&session_id).unwrap();
    assert!(state.last_event_at.is_some(), "Should have last_event_at");
}

#[test]
fn test_context_continuation_state_round_trip() {
    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);
    let session_id = manager
        .create_session("Test Session", Some("gpt-5"), Some("/tmp"))
        .expect("Failed to create session");

    manager
        .update_context_continuation_state(
            &session_id,
            r#"{"schema_version":1,"canonical_messages":2}"#,
            r#"{"schema_version":1,"decision":{"kind":"resumable","latest_user_objective":"ship fix"}}"#,
        )
        .expect("Failed to persist context continuation state");

    let loaded = manager
        .load_context_continuation_state(&session_id)
        .expect("Failed to load context continuation state");

    let (ledger, continuation) = loaded.expect("Expected persisted state");
    assert!(ledger.contains("\"schema_version\":1"));
    assert!(continuation.contains("\"kind\":\"resumable\""));
}

#[test]
fn test_recovery_state_round_trip() {
    use crate::agent::loop_events::LoopStopReason;
    use crate::storage::{
        PartialAssistantState, RecoveryDecision, RecoveryStatus, RecoveryToolCall,
        SessionRecoveryState,
    };

    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);
    let session_id = manager
        .create_session("Interrupted Session", Some("gpt-5"), Some("/tmp"))
        .expect("Failed to create session");

    let recovery = SessionRecoveryState::new(
        RecoveryStatus::Interrupted,
        Some(LoopStopReason::StreamIdleTimeout),
        Some("stream timeout".to_string()),
        PartialAssistantState {
            citations: Vec::new(),
            text: "partial answer".to_string(),
            thinking: String::new(),
            tool_calls: vec![RecoveryToolCall::summary("call-1", "read")],
        },
        RecoveryDecision::NonResumable {
            reason: crate::storage::RecoveryNonResumableReason::PendingToolCall,
        },
    );

    manager
        .update_recovery_state(&session_id, &recovery)
        .expect("Failed to persist recovery state");

    let loaded = manager
        .load_recovery_state(&session_id)
        .expect("Failed to load recovery state")
        .expect("Expected persisted recovery state");

    assert_eq!(loaded, recovery);

    manager
        .clear_recovery_state(&session_id)
        .expect("Failed to clear recovery state");
    assert!(
        manager
            .load_recovery_state(&session_id)
            .expect("Failed to reload recovery state")
            .is_none(),
        "Expected recovery state to be cleared"
    );
}

#[test]
fn awaiting_interaction_claim_is_single_use_and_survives_idle_restart_state() {
    use crate::agent::loop_events::LoopStopReason;
    use crate::storage::{
        PartialAssistantState, PendingInteractionSnapshot, RecoveryDecision,
        RecoveryNonResumableReason, RecoveryStatus, SessionRecoveryState,
    };
    use serde_json::json;
    use std::sync::{Arc, Barrier};

    let (db, temp) = create_test_db();
    let manager = SessionManager::new(db);
    let session_id = manager
        .create_session("Awaiting answer", Some("gpt-5"), Some("/tmp"))
        .expect("session should be created");
    let recovery = SessionRecoveryState::new_with_pending_interactions(
        RecoveryStatus::AwaitingInput,
        Some(LoopStopReason::AwaitingInput),
        None,
        PartialAssistantState::default(),
        vec![PendingInteractionSnapshot::ask_user_from_call(
            "ask-1",
            &json!({"questions": [{"header": "Choice", "question": "Continue?"}]}),
        )],
        RecoveryDecision::NonResumable {
            reason: RecoveryNonResumableReason::AwaitingHumanInput,
        },
    );
    manager
        .update_recovery_state(&session_id, &recovery)
        .expect("recovery should persist");
    // This is the state produced by HTTP startup repair. Durable recovery,
    // rather than transient runtime state, authorizes the continuation.
    manager
        .set_agent_state(&session_id, "idle")
        .expect("runtime state should reset to idle");

    let db_path = temp.path().join("test.db");
    let manager_a = SessionManager::new(Database::new(&db_path).expect("first connection"));
    let manager_b = SessionManager::new(Database::new(&db_path).expect("second connection"));
    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let first_id = session_id.clone();
    let second_id = session_id.clone();

    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(move || {
            first_barrier.wait();
            manager_a.claim_awaiting_interaction(&first_id, "ask-1", "yes")
        });
        let second = scope.spawn(move || {
            second_barrier.wait();
            manager_b.claim_awaiting_interaction(&second_id, "ask-1", "yes")
        });
        (
            first.join().expect("first claim thread should not panic"),
            second.join().expect("second claim thread should not panic"),
        )
    });

    let accepted = [first, second]
        .into_iter()
        .map(|claim| claim.expect("claim should not fail"))
        .filter(Option::is_some)
        .count();
    assert_eq!(accepted, 1, "exactly one competing submission may claim");
    let claimed_recovery = manager
        .load_recovery_state(&session_id)
        .expect("recovery should load")
        .expect("accepted response must remain durable before run start");
    let accepted_claim = claimed_recovery
        .continuation_claim
        .as_ref()
        .expect("accepted response should be recorded");
    assert_eq!(accepted_claim.interaction_id, "ask-1");
    assert_eq!(accepted_claim.accepted_response, "yes");
    assert_eq!(
        manager
            .get_agent_state(&session_id)
            .expect("agent state should exist")
            .state,
        "resuming_input"
    );

    // Failpoint: the process exits after claim acceptance but before the run
    // starts. Startup repair yields only transient state; the accepted answer
    // and prompt remain durable and a different answer cannot replace it.
    assert_eq!(
        manager
            .reset_transient_agent_states()
            .expect("startup repair should reset transient state"),
        1
    );
    assert!(manager
        .claim_awaiting_interaction(&session_id, "ask-1", "different")
        .expect("different response should be rejected normally")
        .is_none());
    assert!(manager
        .claim_awaiting_interaction(&session_id, "ask-1", "yes")
        .expect("same accepted response should reclaim after restart")
        .is_some());
    assert!(manager
        .yield_awaiting_interaction_claim(&session_id, "ask-1", "yes")
        .expect("failed run start should yield its lease"));
    assert_eq!(
        manager
            .get_agent_state(&session_id)
            .expect("agent state should exist")
            .state,
        "idle"
    );
    assert_eq!(
        manager
            .load_recovery_state(&session_id)
            .expect("recovery should remain durable"),
        Some(claimed_recovery)
    );
}

#[test]
fn awaiting_interaction_claim_mismatch_and_multi_pending_state_fail_without_consuming() {
    use crate::agent::loop_events::LoopStopReason;
    use crate::storage::{
        PartialAssistantState, PendingInteractionSnapshot, RecoveryDecision,
        RecoveryNonResumableReason, RecoveryStatus, SessionRecoveryState,
    };
    use serde_json::json;

    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);
    let session_id = manager
        .create_session("Awaiting answers", Some("gpt-5"), Some("/tmp"))
        .expect("session should be created");
    let one_pending = SessionRecoveryState::new_with_pending_interactions(
        RecoveryStatus::AwaitingInput,
        Some(LoopStopReason::AwaitingInput),
        None,
        PartialAssistantState::default(),
        vec![PendingInteractionSnapshot::ask_user_from_call(
            "ask-1",
            &json!({}),
        )],
        RecoveryDecision::NonResumable {
            reason: RecoveryNonResumableReason::AwaitingHumanInput,
        },
    );
    manager
        .update_recovery_state(&session_id, &one_pending)
        .expect("recovery should persist");

    assert!(manager
        .claim_awaiting_interaction(&session_id, "ask-other", "answer")
        .expect("mismatch should be a normal rejection")
        .is_none());
    assert_eq!(
        manager
            .load_recovery_state(&session_id)
            .expect("recovery should load"),
        Some(one_pending)
    );

    let multi_pending = SessionRecoveryState::new_with_pending_interactions(
        RecoveryStatus::AwaitingInput,
        Some(LoopStopReason::AwaitingInput),
        None,
        PartialAssistantState::default(),
        vec![
            PendingInteractionSnapshot::ask_user_from_call("ask-1", &json!({})),
            PendingInteractionSnapshot::ask_user_from_call("ask-2", &json!({})),
        ],
        RecoveryDecision::NonResumable {
            reason: RecoveryNonResumableReason::AwaitingHumanInput,
        },
    );
    manager
        .update_recovery_state(&session_id, &multi_pending)
        .expect("multi-pending recovery should persist for the fixture");

    assert!(manager
        .claim_awaiting_interaction(&session_id, "ask-1", "answer")
        .is_err());
    assert_eq!(
        manager
            .load_recovery_state(&session_id)
            .expect("invalid recovery should remain inspectable"),
        Some(multi_pending)
    );
}

#[test]
fn test_clear_stale_transient_recovery_states_preserves_pending_interactions() {
    use crate::agent::loop_events::LoopStopReason;
    use crate::storage::{
        PartialAssistantState, PendingInteractionSnapshot, RecoveryDecision,
        RecoveryNonResumableReason, RecoveryStatus, SessionRecoveryState,
    };
    use serde_json::json;

    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);
    let stale_session = manager
        .create_session("Stale", Some("gpt-5"), Some("/tmp"))
        .expect("Failed to create stale session");
    let pending_session = manager
        .create_session("Pending", Some("gpt-5"), Some("/tmp"))
        .expect("Failed to create pending session");

    manager
        .update_recovery_state(
            &stale_session,
            &SessionRecoveryState::new(
                RecoveryStatus::Interrupted,
                Some(LoopStopReason::ProviderError),
                None,
                PartialAssistantState::default(),
                RecoveryDecision::NonResumable {
                    reason: RecoveryNonResumableReason::EmptyConversation,
                },
            ),
        )
        .expect("Failed to persist stale recovery");
    manager
        .update_recovery_state(
            &pending_session,
            &SessionRecoveryState::new_with_pending_interactions(
                RecoveryStatus::AwaitingInput,
                Some(LoopStopReason::AwaitingInput),
                None,
                PartialAssistantState::default(),
                vec![PendingInteractionSnapshot::tool_approval_from_call(
                    "call-edit",
                    "edit",
                    &json!({"file_path": "src/lib.rs"}),
                )],
                RecoveryDecision::NonResumable {
                    reason: RecoveryNonResumableReason::AwaitingHumanInput,
                },
            ),
        )
        .expect("Failed to persist pending recovery");

    let cleared = manager
        .clear_stale_transient_recovery_states()
        .expect("Failed to clear stale recovery states");

    assert_eq!(cleared, 1);
    assert!(manager
        .load_recovery_state(&stale_session)
        .expect("Failed to load stale session")
        .is_none());
    assert!(manager
        .load_recovery_state(&pending_session)
        .expect("Failed to load pending session")
        .is_some());
}

#[test]
fn startup_repair_preserves_daemon_owned_hive_state() {
    use crate::agent::loop_events::LoopStopReason;
    use crate::storage::{
        PartialAssistantState, RecoveryDecision, RecoveryNonResumableReason, RecoveryStatus,
        SessionRecoveryState, SessionType, WorkspaceMode,
    };

    let (db, _temp) = create_test_db();
    let manager = SessionManager::new(db);
    let code_session = manager
        .create_session("Code", Some("gpt-5"), Some("/tmp"))
        .expect("create code session");
    let chat_session = manager
        .create_session_for_user_with_config(
            "Chat",
            Some("gpt-5"),
            Some("/tmp"),
            Some("/tmp"),
            WorkspaceMode::Selected,
            None,
            None,
            SessionType::Chat,
        )
        .expect("create chat session");
    let hive_session = manager
        .create_session_for_user_with_config(
            "Hive",
            Some("gpt-5"),
            Some("/tmp"),
            Some("/tmp"),
            WorkspaceMode::Selected,
            None,
            None,
            SessionType::Hive,
        )
        .expect("create Hive session");

    let stale_recovery = SessionRecoveryState::new(
        RecoveryStatus::Interrupted,
        Some(LoopStopReason::ProviderError),
        None,
        PartialAssistantState::default(),
        RecoveryDecision::NonResumable {
            reason: RecoveryNonResumableReason::EmptyConversation,
        },
    );
    for session_id in [&code_session, &chat_session, &hive_session] {
        manager
            .set_agent_state(session_id, "streaming")
            .expect("set active state");
        manager
            .update_recovery_state(session_id, &stale_recovery)
            .expect("persist stale recovery");
    }

    assert_eq!(
        manager
            .reset_transient_agent_states()
            .expect("repair transient states"),
        2
    );
    assert_eq!(
        manager
            .clear_stale_transient_recovery_states()
            .expect("clear stale recovery"),
        2
    );

    for session_id in [&code_session, &chat_session] {
        assert_eq!(manager.get_agent_state(session_id).unwrap().state, "idle");
        assert!(manager
            .load_recovery_state(session_id)
            .expect("load repaired recovery")
            .is_none());
    }
    assert_eq!(
        manager.get_agent_state(&hive_session).unwrap().state,
        "streaming"
    );
    assert_eq!(
        manager
            .load_recovery_state(&hive_session)
            .expect("load Hive recovery"),
        Some(stale_recovery)
    );
}
