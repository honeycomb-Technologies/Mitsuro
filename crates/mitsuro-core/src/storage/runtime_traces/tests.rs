use rusqlite::params;
use tempfile::TempDir;

use super::{ReplayExpectations, RuntimeTraceEvent, RuntimeTraceStore, TraceFailureCategory};
use crate::agent::loop_events::{LoopEvent, LoopStopReason};
use crate::ai::client::{AiClient, AiClientConfig, CallOptions};
use crate::ai::types::{AiTool, Content, ModelMessage, Role};
use crate::storage::database::Database;

fn create_test_db() -> (Database, TempDir, String) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let db = Database::new(&db_path).expect("Failed to create database");
    let session_id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    db.conn()
        .execute(
            "INSERT INTO sessions (id, title, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![session_id, "Trace Test", now, now],
        )
        .expect("Failed to create session");
    (db, temp_dir, session_id)
}

#[test]
fn runtime_trace_store_round_trip() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);

    let event = RuntimeTraceEvent::from_loop_event(
        "run-1",
        1,
        1,
        &LoopEvent::ToolCallComplete {
            id: "tool-1".to_string(),
            name: "read_file".to_string(),
            arguments: serde_json::json!({ "path": "src/main.rs" }),
        },
    );
    store
        .append_event(&session_id, &event)
        .expect("trace append should succeed");

    let events = store
        .list_events(&session_id, None)
        .expect("trace list should succeed");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "tool_call_complete");
    assert_eq!(events[0].payload["arguments"]["type"], "object");
}

#[test]
fn runtime_trace_store_limit_returns_most_recent_events_in_order() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);

    for sequence in 1..=3 {
        let event = RuntimeTraceEvent::from_loop_event(
            "run-1",
            sequence,
            1,
            &LoopEvent::TurnComplete {
                turn: sequence as usize,
                has_more: sequence < 3,
            },
        );
        store
            .append_event(&session_id, &event)
            .expect("trace append should succeed");
    }

    let events = store
        .list_events(&session_id, Some(2))
        .expect("trace list should succeed");
    let sequences: Vec<i64> = events.iter().map(|event| event.sequence).collect();
    assert_eq!(sequences, vec![2, 3]);
}

#[test]
fn runtime_trace_batch_allocates_monotonic_sequences_and_prunes_old_rows() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);
    let events = (0..5)
        .map(|index| {
            RuntimeTraceEvent::from_loop_event(
                "run-batch",
                0,
                1,
                &LoopEvent::TextDelta {
                    delta: format!("chunk-{index}"),
                },
            )
        })
        .collect::<Vec<_>>();

    let sequences = store
        .append_events_with_next_sequences(&session_id, &events)
        .expect("batch append should succeed");
    assert_eq!(sequences, vec![1, 2, 3, 4, 5]);

    let deleted = store
        .prune_session_to_latest(&session_id, 2)
        .expect("prune should succeed");
    assert_eq!(deleted, 3);
    let retained = store
        .list_events(&session_id, None)
        .expect("retained traces should load");
    assert_eq!(
        retained
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
}

#[test]
fn runtime_trace_usage_keeps_logical_and_cache_buckets() {
    let event = RuntimeTraceEvent::from_loop_event(
        "run-usage",
        1,
        1,
        &LoopEvent::Usage {
            prompt_tokens: 100,
            input_tokens: 1_000,
            completion_tokens: 50,
            reasoning_tokens: 40,
            cache_creation_input_tokens: 200,
            cache_read_input_tokens: 700,
            total_tokens: 1_050,
        },
    );

    assert_eq!(event.payload["prompt_tokens"], 100);
    assert_eq!(event.payload["input_tokens"], 1_000);
    assert_eq!(event.payload["cache_creation_input_tokens"], 200);
    assert_eq!(event.payload["cache_read_input_tokens"], 700);
    assert_eq!(event.payload["completion_tokens"], 50);
    assert_eq!(event.payload["reasoning_tokens"], 40);
    assert_eq!(event.payload["total_tokens"], 1_050);
}

#[test]
fn provider_request_trace_keeps_contract_metadata_but_redacts_request_contents() {
    const SYSTEM_SECRET: &str = "never-persist-system-prompt";
    const USER_SECRET: &str = "never-persist-user-message";
    const TOOL_SECRET: &str = "never-persist-tool-schema";
    const CREDENTIAL_SECRET: &str = "never-persist-provider-credential";
    const CACHE_KEY_SECRET: &str = "never-persist-cache-key-source";

    let client = AiClient::new(
        AiClientConfig::for_grok("grok-4.6"),
        CREDENTIAL_SECRET.to_string(),
    );
    let messages = vec![
        ModelMessage {
            role: Role::System,
            content: vec![Content::text(SYSTEM_SECRET.to_string())],
        },
        ModelMessage {
            role: Role::User,
            content: vec![Content::text(USER_SECRET.to_string())],
        },
    ];
    let options = CallOptions {
        system_prompt: Some(SYSTEM_SECRET.to_string()),
        tools: Some(vec![AiTool {
            name: "secret_tool".to_string(),
            description: TOOL_SECRET.to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "value": { "type": "string" } },
                "sentinel": TOOL_SECRET,
            }),
            prompt: Some(TOOL_SECRET.to_string()),
        }]),
        session_id: Some(CACHE_KEY_SECRET.to_string()),
        ..CallOptions::default()
    };
    let diagnostics = client.request_diagnostics(&messages, &options);
    let loop_event = LoopEvent::ProviderRequestPrepared {
        turn: 7,
        diagnostics: Box::new(diagnostics.into()),
    };
    let trace = RuntimeTraceEvent::from_loop_event("run-request", 1, 7, &loop_event);

    assert_eq!(trace.event_type, "provider_request_prepared");
    assert_eq!(trace.payload["turn"], 7);
    let diagnostics = &trace.payload["diagnostics"];
    assert_eq!(diagnostics["model_key"]["provider"], "grok");
    assert_eq!(diagnostics["model_key"]["model_id"], "grok-4.6");
    assert!(diagnostics["catalog_source"].is_string());
    assert_eq!(diagnostics["effective_request"]["tool_count"], 1);
    assert_eq!(
        diagnostics["tool_names"],
        serde_json::json!(["secret_tool"])
    );
    assert_eq!(diagnostics["message_count"], 2);
    assert_eq!(diagnostics["system_message_count"], 1);
    assert_eq!(diagnostics["user_message_count"], 1);
    assert_eq!(
        diagnostics["prompt_manifest"]["prompt_hash"]
            .as_str()
            .map(str::len),
        Some(64)
    );
    assert_eq!(
        diagnostics["stable_request_fingerprint"]
            .as_str()
            .map(str::len),
        Some(64)
    );
    assert_eq!(diagnostics["cache_key_present"], true);
    assert_eq!(diagnostics["cache_mode"], "session_key");
    assert!(
        diagnostics["stable_instruction_bytes"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert!(diagnostics["tool_schema_bytes"].as_u64().unwrap_or(0) > 0);

    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);
    store
        .append_event(&session_id, &trace)
        .expect("redacted request trace should persist");
    let persisted = store
        .list_events(&session_id, None)
        .expect("redacted request trace should load");
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].payload, trace.payload);

    let loop_json = serde_json::to_string(&loop_event).expect("loop event should serialize");
    let trace_json = serde_json::to_string(&persisted[0].payload).expect("trace should serialize");
    for secret in [
        SYSTEM_SECRET,
        USER_SECRET,
        TOOL_SECRET,
        CREDENTIAL_SECRET,
        CACHE_KEY_SECRET,
    ] {
        assert!(!loop_json.contains(secret));
        assert!(!trace_json.contains(secret));
    }
}

#[test]
fn runtime_trace_summary_classifies_failures_and_pinches() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);

    let traces = [
        RuntimeTraceEvent::from_loop_event(
            "run-1",
            1,
            1,
            &LoopEvent::SessionPinched {
                reason: "pressure".to_string(),
                source_session_id: session_id.clone(),
                new_session_id: "child-session".to_string(),
                estimated_tokens_before: 100_000,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-1",
            2,
            1,
            &LoopEvent::ToolResult {
                id: "tool-1".to_string(),
                output: "permission denied".to_string(),
                is_error: true,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-1",
            3,
            1,
            &LoopEvent::Finished {
                session_id: session_id.clone(),
                stop_reason: LoopStopReason::ProviderError,
            },
        ),
    ];

    for event in traces {
        store
            .append_event(&session_id, &event)
            .expect("trace append should succeed");
    }

    let summary = store
        .summarize_session(&session_id)
        .expect("summary should succeed");
    assert_eq!(summary.total_events, 3);
    assert_eq!(summary.total_runs, 1);
    assert_eq!(summary.total_turns, 1);
    assert_eq!(summary.tool_errors, 1);
    assert_eq!(summary.provider_failures, 1);
    assert_eq!(summary.session_pinches, 1);
    assert!(summary
        .failure_categories
        .contains(&TraceFailureCategory::ToolExecutionError));
    assert!(summary
        .failure_categories
        .contains(&TraceFailureCategory::ProviderError));
}

#[test]
fn replay_gate_accepts_long_session_workload_with_pinch() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);

    let traces = [
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            1,
            1,
            &LoopEvent::ToolCallComplete {
                id: "tool-1".to_string(),
                name: "read".to_string(),
                arguments: serde_json::json!({ "file_path": "src/main.rs" }),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            2,
            1,
            &LoopEvent::ToolResult {
                id: "tool-1".to_string(),
                output: "read ok".to_string(),
                is_error: false,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            3,
            1,
            &LoopEvent::TurnComplete {
                turn: 1,
                has_more: true,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            4,
            2,
            &LoopEvent::SessionPinched {
                reason: "context_pressure".to_string(),
                source_session_id: session_id.clone(),
                new_session_id: "child-session".to_string(),
                estimated_tokens_before: 140_000,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            5,
            2,
            &LoopEvent::ToolCallComplete {
                id: "tool-2".to_string(),
                name: "write".to_string(),
                arguments: serde_json::json!({ "file_path": "src/lib.rs" }),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            6,
            2,
            &LoopEvent::ToolResult {
                id: "tool-2".to_string(),
                output: "write ok".to_string(),
                is_error: false,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            7,
            2,
            &LoopEvent::TurnComplete {
                turn: 2,
                has_more: true,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-long",
            8,
            3,
            &LoopEvent::Finished {
                session_id: session_id.clone(),
                stop_reason: LoopStopReason::Completed,
            },
        ),
    ];

    for event in traces {
        store
            .append_event(&session_id, &event)
            .expect("trace append should succeed");
    }

    let summary = store
        .summarize_session(&session_id)
        .expect("summary should succeed");
    let expectations = ReplayExpectations {
        min_total_turns: 3,
        min_session_pinches: 1,
        required_event_types: vec![
            "tool_call_complete".to_string(),
            "tool_result".to_string(),
            "session_pinched".to_string(),
        ],
        ..ReplayExpectations::strict()
    };

    let result = expectations.evaluate(&summary);
    assert!(
        result.passed,
        "unexpected violations: {:?}",
        result.violations
    );
    assert_eq!(summary.session_pinches, 1);
    assert_eq!(summary.tool_calls, 2);
}

#[test]
fn replay_gate_accepts_approval_pause_and_resume_workload() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);

    let traces = [
        RuntimeTraceEvent::from_loop_event(
            "run-awaiting-input",
            1,
            1,
            &LoopEvent::ToolApprovalRequired {
                id: "tool-1".to_string(),
                name: "write".to_string(),
                arguments: serde_json::json!({ "file_path": "src/main.rs" }),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-awaiting-input",
            2,
            1,
            &LoopEvent::AwaitingInput {
                tool_call_id: "tool-1".to_string(),
                tool_name: "write".to_string(),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-awaiting-input",
            3,
            1,
            &LoopEvent::Finished {
                session_id: session_id.clone(),
                stop_reason: LoopStopReason::AwaitingInput,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-approved",
            4,
            2,
            &LoopEvent::ToolApproved {
                id: "tool-1".to_string(),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-approved",
            5,
            2,
            &LoopEvent::ToolExecuting {
                id: "tool-1".to_string(),
                name: "write".to_string(),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-approved",
            6,
            2,
            &LoopEvent::ToolResult {
                id: "tool-1".to_string(),
                output: "write ok".to_string(),
                is_error: false,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-approved",
            7,
            2,
            &LoopEvent::Finished {
                session_id: session_id.clone(),
                stop_reason: LoopStopReason::Completed,
            },
        ),
    ];

    for event in traces {
        store
            .append_event(&session_id, &event)
            .expect("trace append should succeed");
    }

    let summary = store
        .summarize_session(&session_id)
        .expect("summary should succeed");
    let expectations = ReplayExpectations {
        min_total_runs: 2,
        min_total_turns: 2,
        min_awaiting_input_events: 1,
        required_event_types: vec![
            "tool_approval_required".to_string(),
            "awaiting_input".to_string(),
            "tool_approved".to_string(),
            "tool_result".to_string(),
        ],
        ..ReplayExpectations::strict()
    };

    let result = expectations.evaluate(&summary);
    assert!(
        result.passed,
        "unexpected violations: {:?}",
        result.violations
    );
    assert_eq!(summary.awaiting_input_events, 1);
    assert_eq!(summary.total_runs, 2);
}

#[test]
fn summarize_latest_run_ignores_prior_provider_interruption_after_recovery() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);

    let traces = [
        RuntimeTraceEvent::from_loop_event(
            "run-interrupted",
            1,
            1,
            &LoopEvent::Error {
                error: "provider disconnected".to_string(),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-interrupted",
            2,
            1,
            &LoopEvent::Finished {
                session_id: session_id.clone(),
                stop_reason: LoopStopReason::ProviderError,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-recovered",
            3,
            2,
            &LoopEvent::ToolCallComplete {
                id: "tool-2".to_string(),
                name: "read".to_string(),
                arguments: serde_json::json!({ "file_path": "src/lib.rs" }),
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-recovered",
            4,
            2,
            &LoopEvent::ToolResult {
                id: "tool-2".to_string(),
                output: "read ok".to_string(),
                is_error: false,
            },
        ),
        RuntimeTraceEvent::from_loop_event(
            "run-recovered",
            5,
            2,
            &LoopEvent::Finished {
                session_id: session_id.clone(),
                stop_reason: LoopStopReason::Completed,
            },
        ),
    ];

    for event in traces {
        store
            .append_event(&session_id, &event)
            .expect("trace append should succeed");
    }

    let whole_session = store
        .summarize_session(&session_id)
        .expect("summary should succeed");
    assert_eq!(whole_session.provider_failures, 1);

    let latest_run = store
        .summarize_latest_run(&session_id)
        .expect("latest run summary should succeed");
    assert_eq!(latest_run.total_runs, 1);
    assert_eq!(latest_run.provider_failures, 0);
    assert_eq!(latest_run.last_stop_reason, Some(LoopStopReason::Completed));

    let result = ReplayExpectations::strict().evaluate(&latest_run);
    assert!(
        result.passed,
        "unexpected violations: {:?}",
        result.violations
    );
}

#[test]
fn replay_gate_rejects_loop_guard_workload() {
    let failing_summary = super::RuntimeTraceSummary {
        total_events: 3,
        total_runs: 1,
        total_turns: 4,
        last_stop_reason: Some(LoopStopReason::LoopGuardTriggered),
        failure_categories: vec![TraceFailureCategory::LoopGuardTriggered],
        event_counts: vec![
            super::TraceEventCount {
                event_type: "tool_call_complete".to_string(),
                count: 3,
            },
            super::TraceEventCount {
                event_type: "finished".to_string(),
                count: 1,
            },
        ],
        ..Default::default()
    };

    let expectations = ReplayExpectations {
        min_total_turns: 2,
        required_event_types: vec!["tool_call_complete".to_string()],
        ..ReplayExpectations::strict()
    };
    let result = expectations.evaluate(&failing_summary);

    assert!(!result.passed);
    assert!(result
        .violations
        .iter()
        .any(|violation| violation.contains("terminal reason")));
}

#[test]
fn replay_gate_rejects_provider_failures() {
    let failing_summary = super::RuntimeTraceSummary {
        total_events: 2,
        total_runs: 1,
        total_turns: 1,
        provider_failures: 1,
        last_stop_reason: Some(LoopStopReason::ProviderError),
        failure_categories: vec![TraceFailureCategory::ProviderError],
        ..Default::default()
    };

    let result = ReplayExpectations::strict().evaluate(&failing_summary);
    assert!(!result.passed);
    assert!(result
        .violations
        .iter()
        .any(|violation| violation.contains("terminal reason")));
}

#[test]
fn latest_sequence_and_after_filter_follow_monotonic_trace_order() {
    let (db, _temp_dir, session_id) = create_test_db();
    let store = RuntimeTraceStore::new(&db);

    store
        .append_event(
            &session_id,
            &RuntimeTraceEvent::from_loop_event(
                "run-1".to_string(),
                1,
                1,
                &LoopEvent::TextDelta {
                    delta: "one".to_string(),
                },
            ),
        )
        .expect("first event should persist");
    store
        .append_event(
            &session_id,
            &RuntimeTraceEvent::from_loop_event(
                "run-1".to_string(),
                2,
                1,
                &LoopEvent::TurnComplete {
                    turn: 1,
                    has_more: true,
                },
            ),
        )
        .expect("second event should persist");
    store
        .append_event(
            &session_id,
            &RuntimeTraceEvent::from_loop_event(
                "run-1".to_string(),
                3,
                2,
                &LoopEvent::Finished {
                    session_id: session_id.clone(),
                    stop_reason: LoopStopReason::Completed,
                },
            ),
        )
        .expect("third event should persist");

    assert_eq!(
        store
            .latest_sequence(&session_id)
            .expect("latest sequence should load"),
        Some(3)
    );

    let filtered = store
        .list_events_after(&session_id, 1, Some(10))
        .expect("filtered events should load");
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].sequence, 2);
    assert_eq!(filtered[1].sequence, 3);
}

#[test]
fn concurrent_trace_writers_allocate_unique_monotonic_sequences() {
    use std::sync::{Arc, Barrier};

    let (db, temp_dir, session_id) = create_test_db();
    drop(db);
    let db_path = temp_dir.path().join("test.db");
    let barrier = Arc::new(Barrier::new(3));
    let mut writers = Vec::new();

    for writer_index in 0..2 {
        let db_path = db_path.clone();
        let session_id = session_id.clone();
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || {
            let db = Database::new(&db_path).expect("writer database should open");
            let store = RuntimeTraceStore::new(&db);
            barrier.wait();
            for event_index in 0..50 {
                let event = RuntimeTraceEvent::from_loop_event(
                    format!("run-{writer_index}"),
                    0,
                    1,
                    &LoopEvent::TextDelta {
                        delta: format!("writer {writer_index} event {event_index}"),
                    },
                );
                store
                    .append_event_with_next_sequence(&session_id, &event)
                    .expect("concurrent event should persist");
            }
        }));
    }

    barrier.wait();
    for writer in writers {
        writer.join().expect("writer should not panic");
    }

    let db = Database::new(&db_path).expect("verification database should open");
    let events = RuntimeTraceStore::new(&db)
        .list_events(&session_id, None)
        .expect("events should load");
    assert_eq!(events.len(), 100);
    assert!(events
        .iter()
        .enumerate()
        .all(|(index, event)| event.sequence == index as i64 + 1));
}
