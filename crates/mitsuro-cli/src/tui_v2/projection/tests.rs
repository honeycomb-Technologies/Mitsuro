use chrono::Utc;
use mitsuro_core::{
    agent::{
        loop_events::LoopStopReason,
        subagent::{AgentProgress, AgentProgressStatus},
        DelegatedProgressEvent, DelegatedRunStage, DelegatedToolKind, LoopEvent,
    },
    ai::types::{Content, ModelMessage, Role},
    storage::{
        DelegatedRunRole, DelegationCompletionPolicy, DelegationExecutionMode,
        DelegationFailurePolicy, DelegationGovernance, DelegationGroupContract,
        DelegationGroupRecord, DelegationGroupState, DelegationParentContinuationState,
        DelegationTaskRecord, DelegationTaskSpec, DelegationTaskState, DelegationWriterMode,
    },
    tools::registry::{DelegationPolicy, PermissionMode},
};
use serde_json::json;

use crate::tui_v2::model::{
    artifact::ArtifactContent,
    conversation::{PendingInteraction, TimelinePart, ToolStatus, TurnState},
};

use super::{tool_output::LIVE_ARTIFACT_BYTES, ConversationProjection, PersistedMessage};

#[test]
fn live_and_persisted_text_tool_text_have_identical_turns() {
    let mut live = ConversationProjection::new("session");
    live.push_user_prompt("u1", "Inspect this.".to_owned(), Vec::new(), false);
    for event in [
        LoopEvent::TextDelta {
            delta: "I’ll inspect the current state.".to_owned(),
        },
        LoopEvent::ToolCallStart {
            id: "read-1".to_owned(),
            name: "read".to_owned(),
        },
        LoopEvent::ToolCallComplete {
            id: "read-1".to_owned(),
            name: "read".to_owned(),
            arguments: json!({"path": "src/main.rs"}),
        },
        LoopEvent::ToolExecuting {
            id: "read-1".to_owned(),
            name: "read".to_owned(),
        },
        LoopEvent::ToolResult {
            id: "read-1".to_owned(),
            output: json!({"ok": true, "data": "42"}).to_string(),
            is_error: false,
        },
        LoopEvent::TextDelta {
            delta: "The state is stored in parallel vectors.".to_owned(),
        },
        LoopEvent::TurnComplete {
            turn: 1,
            has_more: false,
        },
        LoopEvent::Finished {
            session_id: "session".to_owned(),
            stop_reason: LoopStopReason::Completed,
        },
    ] {
        live.apply_event(event);
    }

    let persisted = ConversationProjection::from_persisted(
        "session",
        &[
            PersistedMessage::new(
                "u1",
                ModelMessage {
                    role: Role::User,
                    content: vec![Content::text("Inspect this.".to_owned())],
                },
            ),
            PersistedMessage::without_id(ModelMessage {
                role: Role::Assistant,
                content: vec![
                    Content::text("I’ll inspect the current state.".to_owned()),
                    Content::ToolUse {
                        id: "read-1".to_owned(),
                        name: "read".to_owned(),
                        input: json!({"path": "src/main.rs"}),
                    },
                ],
            }),
            PersistedMessage::without_id(ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "read-1".to_owned(),
                    output: json!({"ok": true, "data": "42"}),
                    is_error: Some(false),
                }],
            }),
            PersistedMessage::without_id(ModelMessage {
                role: Role::Assistant,
                content: vec![Content::text(
                    "The state is stored in parallel vectors.".to_owned(),
                )],
            }),
        ],
    );

    assert_eq!(live.presentation().turns, persisted.presentation().turns);
    let parts = &live.presentation().turns[0].parts;
    assert!(matches!(
        parts.as_slice(),
        [
            TimelinePart::AgentText(_),
            TimelinePart::Tool(_),
            TimelinePart::AgentText(_)
        ]
    ));
}

#[test]
fn a_thousand_bash_deltas_update_one_bounded_artifact() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Run tests.".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::ToolCallComplete {
        id: "bash-1".to_owned(),
        name: "bash".to_owned(),
        arguments: json!({"command": "cargo test"}),
    });
    projection.apply_event(LoopEvent::ToolExecuting {
        id: "bash-1".to_owned(),
        name: "bash".to_owned(),
    });
    for index in 0..1_000 {
        projection.apply_event(LoopEvent::ToolOutputDelta {
            id: "bash-1".to_owned(),
            delta: format!("\rcompiling crate {index}: {}\n", "x".repeat(160)),
        });
    }
    let TimelinePart::Tool(running_tool) = &projection.presentation().turns[0].parts[0] else {
        panic!("tool");
    };
    assert_eq!(running_tool.status, ToolStatus::Running);
    assert!(matches!(
        &running_tool.artifact.content,
        ArtifactContent::Text(text)
            if text.text.len() <= LIVE_ARTIFACT_BYTES && text.omitted_bytes > 0
    ));

    projection.apply_event(LoopEvent::ToolResult {
        id: "bash-1".to_owned(),
        output: "finished".to_owned(),
        is_error: false,
    });

    let tool_parts = projection.presentation().turns[0]
        .parts
        .iter()
        .filter_map(|part| match part {
            TimelinePart::Tool(tool) => Some(tool),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tool_parts.len(), 1);
    assert_eq!(tool_parts[0].status, ToolStatus::Succeeded);
    assert!(matches!(
        &tool_parts[0].artifact.content,
        ArtifactContent::Text(text)
            if text.text.contains("compiling crate") && text.text.contains("finished")
    ));
}

#[test]
fn tool_events_do_not_split_a_streaming_word_across_agent_parts() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Inspect this.".to_owned(), Vec::new(), false);
    // Stream an incomplete trailing token, interrupt with a tool, then finish it.
    projection.apply_event(LoopEvent::TextDelta {
        delta: "Checking Hive/M".to_owned(),
    });
    projection.apply_event(LoopEvent::ToolCallStart {
        id: "read-1".to_owned(),
        name: "read".to_owned(),
    });
    projection.apply_event(LoopEvent::TextDelta {
        delta: "ako next.".to_owned(),
    });

    let parts = &projection.presentation().turns[0].parts;
    let agent_text: Vec<&str> = parts
        .iter()
        .filter_map(|part| match part {
            TimelinePart::AgentText(part) => Some(part.text.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        parts
            .iter()
            .any(|part| matches!(part, TimelinePart::Tool(_))),
        "tool part should be present: {parts:?}"
    );
    // Reassembly must produce the full stream without a permanent dangling token part.
    assert_eq!(agent_text.join(""), "Checking Hive/Mako next.");
    assert!(
        !agent_text.contains(&"Checking Hive/M"),
        "incomplete stream token should not remain as its own final AgentText part: {agent_text:?}"
    );
}

#[test]
fn usage_title_and_mode_update_metadata_without_transcript_noise() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Hello".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::TitleGenerated {
        title: "Clean TUI".to_owned(),
    });
    projection.apply_event(LoopEvent::ModeChange {
        mode: "build".to_owned(),
        reason: None,
    });
    projection.apply_event(LoopEvent::Usage {
        prompt_tokens: 10,
        input_tokens: 30,
        completion_tokens: 5,
        reasoning_tokens: 2,
        cache_creation_input_tokens: 10,
        cache_read_input_tokens: 10,
        total_tokens: 35,
    });

    assert!(projection.presentation().turns[0].parts.is_empty());
    assert_eq!(
        projection.presentation().metadata.title.as_deref(),
        Some("Clean TUI")
    );
    assert_eq!(
        projection.presentation().metadata.mode.as_deref(),
        Some("build")
    );
    assert_eq!(
        projection
            .presentation()
            .metadata
            .usage
            .as_ref()
            .map(|usage| usage.total_tokens),
        Some(35)
    );
}

#[test]
fn canonical_steering_ack_deduplicates_an_optimistic_turn() {
    let mut projection = ConversationProjection::new("session");
    projection.apply_event(LoopEvent::SteeringInjected {
        pending_id: Some("pending-1".to_owned()),
        message: "Keep the layout compact.".to_owned(),
    });
    projection.apply_event(LoopEvent::SteeringInjected {
        pending_id: Some("pending-1".to_owned()),
        message: "Keep the layout compact.".to_owned(),
    });

    assert_eq!(projection.presentation().turns.len(), 1);
    assert!(projection.presentation().turns[0]
        .user
        .as_ref()
        .is_some_and(|user| user.steering));
}

#[test]
fn in_place_compaction_and_linked_continuation_stay_distinct() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Continue".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::ContextCompactionStarted {
        reason: "threshold".to_owned(),
    });
    projection.apply_event(LoopEvent::ContextCompacted {
        reason: "threshold".to_owned(),
        estimated_tokens_before: 100_000,
        estimated_tokens_after: 40_000,
        replaced_messages: 20,
        checkpoint_id: "cp-1".to_owned(),
        compaction_count: 1,
    });
    projection.apply_event(LoopEvent::SessionPinched {
        reason: "explicit continuation".to_owned(),
        source_session_id: "session".to_owned(),
        new_session_id: "session-2".to_owned(),
        estimated_tokens_before: 50_000,
    });

    assert!(matches!(
        projection.presentation().turns[0].parts[0],
        TimelinePart::Compaction(ref part) if part.in_place
    ));
    assert!(matches!(
        projection.presentation().turns[0].parts[1],
        TimelinePart::Notice(ref part) if part.message.contains("session-2")
    ));
}

#[test]
fn approvals_and_questions_target_exact_tool_and_session_once() {
    let mut projection = ConversationProjection::new("session-42");
    projection.push_user_prompt("u1", "Proceed carefully.".to_owned(), Vec::new(), false);

    for _ in 0..2 {
        projection.apply_event(LoopEvent::ToolApprovalRequired {
            id: "bash-1".to_owned(),
            name: "bash".to_owned(),
            arguments: json!({"command": "git push", "token": "secret"}),
        });
    }
    assert_eq!(projection.presentation().pending_interactions.len(), 1);
    assert!(matches!(
        &projection.presentation().pending_interactions[0],
        PendingInteraction::ToolApproval(approval)
            if approval.session_id == "session-42"
                && approval.tool_call_id == "bash-1"
                && approval.arguments.redacted_fields == 1
    ));
    projection.apply_event(LoopEvent::ToolDenied {
        id: "bash-1".to_owned(),
    });
    assert!(projection.presentation().pending_interactions.is_empty());

    projection.apply_event(LoopEvent::ToolCallComplete {
        id: "question-1".to_owned(),
        name: "AskUserQuestion".to_owned(),
        arguments: json!({
            "questions": [{
                "header": "Style",
                "question": "Which density?",
                "options": [{"label": "Compact", "description": "One-line tools"}],
                "multi_select": false
            }]
        }),
    });
    projection.apply_event(LoopEvent::AwaitingInput {
        tool_call_id: "question-1".to_owned(),
        tool_name: "AskUserQuestion".to_owned(),
    });
    assert!(matches!(
        &projection.presentation().pending_interactions[0],
        PendingInteraction::Questions(questions)
            if questions.session_id == "session-42"
                && questions.tool_call_id == "question-1"
                && questions.questions[0].question == "Which density?"
    ));
    projection.apply_event(LoopEvent::ToolResult {
        id: "question-1".to_owned(),
        output: "Compact".to_owned(),
        is_error: false,
    });
    assert!(projection.presentation().pending_interactions.is_empty());
}

#[test]
fn server_tool_events_update_one_ordered_part() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Research Ratatui.".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::ServerToolStart {
        id: "web-1".to_owned(),
        name: "web_search".to_owned(),
    });
    projection.apply_event(LoopEvent::WebSearchResults {
        tool_use_id: "web-1".to_owned(),
        results: vec![mitsuro_core::ai::types::WebSearchResult {
            url: "https://ratatui.rs".to_owned(),
            title: "Ratatui".to_owned(),
            encrypted_content: None,
            page_age: Some("recent".to_owned()),
        }],
    });
    projection.apply_event(LoopEvent::ServerToolComplete {
        id: "web-1".to_owned(),
        name: "web_search".to_owned(),
    });

    assert_eq!(projection.presentation().turns[0].parts.len(), 1);
    assert!(matches!(
        &projection.presentation().turns[0].parts[0],
        TimelinePart::Tool(tool)
            if tool.server_side
                && tool.status == ToolStatus::Succeeded
                && matches!(tool.artifact.content, ArtifactContent::WebResults(ref results) if results.len() == 1)
    ));
}

#[test]
fn interrupt_settles_every_live_visual_state_without_dropping_evidence() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Run it.".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::ThinkingDelta {
        thinking: "Checking".to_owned(),
    });
    projection.apply_event(LoopEvent::ToolExecuting {
        id: "bash-1".to_owned(),
        name: "bash".to_owned(),
    });
    projection.apply_event(LoopEvent::ToolOutputDelta {
        id: "bash-1".to_owned(),
        delta: "partial evidence".to_owned(),
    });
    projection.apply_event(LoopEvent::Finished {
        session_id: "session".to_owned(),
        stop_reason: LoopStopReason::UserAbort,
    });

    assert_eq!(
        projection.presentation().turns[0].state,
        TurnState::Interrupted
    );
    assert!(projection.presentation().live_turn_id.is_none());
    assert!(matches!(
        &projection.presentation().turns[0].parts[0],
        TimelinePart::Thinking(thinking) if !thinking.streaming
    ));
    assert!(matches!(
        &projection.presentation().turns[0].parts[1],
        TimelinePart::Tool(tool)
            if tool.status == ToolStatus::Interrupted
                && matches!(tool.artifact.content, ArtifactContent::Text(ref text) if text.text.contains("partial evidence"))
    ));
}

#[test]
fn canonical_steering_event_opens_a_new_user_turn() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Start here.".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::TextDelta {
        delta: "Working".to_owned(),
    });
    projection.apply_event(LoopEvent::SteeringInjected {
        pending_id: Some("steer-1".to_owned()),
        message: "Also update the tests.".to_owned(),
    });

    assert_eq!(projection.presentation().turns.len(), 2);
    let steering = projection.presentation().turns[1]
        .user
        .as_ref()
        .expect("canonical steering prompt");
    assert!(steering.steering);
    assert_eq!(steering.text, "Also update the tests.");
}

#[test]
fn delegated_task_lifecycle_is_visible_without_settling_the_aggregate() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Build it.".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::AgentBackgroundStarted {
        delegated_run_id: "run-1".to_owned(),
        agent_type: "build".to_owned(),
        description: "parallel implementation".to_owned(),
    });

    for status in [
        AgentProgressStatus::Queued,
        AgentProgressStatus::Leased,
        AgentProgressStatus::Running,
        AgentProgressStatus::Retrying,
        AgentProgressStatus::Running,
        AgentProgressStatus::Complete,
    ] {
        projection.apply_delegated_progress(&delegated_progress("builder-a", status));
    }

    let TimelinePart::Tool(tool) = &projection.presentation().turns[0].parts[0] else {
        panic!("agent tool");
    };
    assert_eq!(tool.status, ToolStatus::Running);
    let ArtifactContent::Text(text) = &tool.artifact.content else {
        panic!("agent lifecycle text");
    };
    for expected in [
        "· status  queued",
        "· status  waiting for provider",
        "· status  running",
        "· status  retrying",
        "· status  complete",
    ] {
        assert!(
            text.text.contains(expected),
            "missing {expected}: {}",
            text.text
        );
    }

    projection.apply_event(LoopEvent::AgentBackgroundCompleted {
        delegated_run_id: "run-1".to_owned(),
        agent_type: "build".to_owned(),
        success: true,
        summary: "Integrated both patches.".to_owned(),
    });
    projection.apply_delegated_progress(&delegated_progress(
        "builder-a",
        AgentProgressStatus::Failed,
    ));

    let TimelinePart::Tool(tool) = &projection.presentation().turns[0].parts[0] else {
        panic!("settled agent tool");
    };
    assert_eq!(tool.status, ToolStatus::Succeeded);
    assert!(matches!(
        &tool.artifact.content,
        ArtifactContent::Text(text) if text.text.contains("Integrated both patches.")
            && !text.text.contains("· status  failed")
    ));
}

#[test]
fn delegated_terminal_task_states_do_not_impersonate_group_failure() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Review it.".to_owned(), Vec::new(), false);

    for (name, status) in [
        ("partial", AgentProgressStatus::Degraded),
        ("broken", AgentProgressStatus::Failed),
        ("stopped", AgentProgressStatus::Cancelled),
    ] {
        projection.apply_delegated_progress(&delegated_progress(name, status));
    }

    let TimelinePart::Tool(tool) = &projection.presentation().turns[0].parts[0] else {
        panic!("agent tool");
    };
    assert_eq!(tool.status, ToolStatus::Running);
    let ArtifactContent::Text(text) = &tool.artifact.content else {
        panic!("agent lifecycle text");
    };
    for expected in [
        "· status  degraded",
        "· status  failed",
        "· status  cancelled",
    ] {
        assert!(
            text.text.contains(expected),
            "missing {expected}: {}",
            text.text
        );
    }

    projection.apply_event(LoopEvent::AgentBackgroundCompleted {
        delegated_run_id: "run-1".to_owned(),
        agent_type: "build".to_owned(),
        success: false,
        summary: "The delegation group failed.".to_owned(),
    });
    let TimelinePart::Tool(tool) = &projection.presentation().turns[0].parts[0] else {
        panic!("failed aggregate tool");
    };
    assert_eq!(tool.status, ToolStatus::Failed);
}

#[test]
fn parallel_child_updates_reopen_the_correct_named_section() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Inspect in parallel.".to_owned(), Vec::new(), false);

    projection
        .apply_delegated_progress(&delegated_progress("reader-a", AgentProgressStatus::Queued));
    projection
        .apply_delegated_progress(&delegated_progress("reader-b", AgentProgressStatus::Queued));
    projection.apply_delegated_progress(&delegated_progress(
        "reader-a",
        AgentProgressStatus::Running,
    ));

    let TimelinePart::Tool(tool) = &projection.presentation().turns[0].parts[0] else {
        panic!("agent tool");
    };
    let ArtifactContent::Text(text) = &tool.artifact.content else {
        panic!("agent lifecycle text");
    };
    assert!(text
        .text
        .contains("── reader-a ──\n· status  queued\n\n── reader-b ──\n· status  queued"));
    assert!(text.text.contains("── reader-a ──\n· status  running"));
}

#[test]
fn reopened_session_restores_canonical_delegation_into_existing_agent_card() {
    let mut projection = ConversationProjection::new("session");
    projection.push_user_prompt("u1", "Build it.".to_owned(), Vec::new(), false);
    projection.apply_event(LoopEvent::ToolCallComplete {
        id: "agent-call".to_owned(),
        name: "agent".to_owned(),
        arguments: json!({"role": "build"}),
    });
    projection.apply_event(LoopEvent::ToolResult {
        id: "agent-call".to_owned(),
        output: "Compatibility aggregate persisted.".to_owned(),
        is_error: false,
    });

    projection.restore_delegation_groups(&[delegation_group(
        DelegationGroupState::Complete,
        DelegationTaskState::Complete,
    )]);

    let tools = projection.presentation().turns[0]
        .parts
        .iter()
        .filter_map(|part| match part {
            TimelinePart::Tool(tool) => Some(tool),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tools.len(),
        1,
        "durable replay must reuse the transcript card"
    );
    assert_eq!(tools[0].tool_call_id, "agent-call");
    assert_eq!(tools[0].status, ToolStatus::Succeeded);
    assert!(matches!(
        &tools[0].artifact.content,
        ArtifactContent::Text(text)
            if text.text.contains("· status  complete")
                && text.text.contains("· group  complete")
                && text.text.contains("Implemented the focused change.")
    ));
}

fn delegation_group(
    group_state: DelegationGroupState,
    task_state: DelegationTaskState,
) -> DelegationGroupRecord {
    let now = Utc::now();
    DelegationGroupRecord {
        delegation_group_id: "group-1".to_owned(),
        parent_session_id: "session".to_owned(),
        parent_tool_call_id: Some("agent-call".to_owned()),
        contract: DelegationGroupContract {
            execution_mode: DelegationExecutionMode::Detached,
            completion_policy: DelegationCompletionPolicy::AllSettled,
            failure_policy: DelegationFailurePolicy::Continue,
            governance: DelegationGovernance {
                permission_mode: PermissionMode::Supervised,
                reasoning_effort: None,
                delegated_turn_budget: Some(12),
                max_parallelism: 1,
                execution_tool_allowlist: None,
                delegation_policy: DelegationPolicy::for_subagent_build(
                    PermissionMode::Supervised,
                    Some(12),
                ),
            },
        },
        state: group_state,
        parent_continuation_state: DelegationParentContinuationState::NotRequested,
        parent_continuation_id: None,
        synthesis_owner_id: None,
        synthesis_lease_expires_at_ms: None,
        synthesis_attempt_count: 1,
        tasks: vec![DelegationTaskRecord {
            delegation_group_id: "group-1".to_owned(),
            ordinal: 0,
            specification: DelegationTaskSpec {
                delegation_task_id: "task-1".to_owned(),
                task_key: "builder-a".to_owned(),
                objective: "Implement the focused change".to_owned(),
                role: DelegatedRunRole::Build,
                target_scope: Vec::new(),
                max_attempts: 2,
                depends_on: Vec::new(),
                write_intent: Vec::new(),
                task_policy: None,
                writer_mode: DelegationWriterMode::Isolated,
                attempt_workspace: None,
                workspace_baseline: None,
                executor_envelope: None,
            },
            state: task_state,
            attempt_count: 1,
            result: Some(json!({"summary": "Implemented the focused change."})),
            error_summary: None,
            created_at: now,
            updated_at: now,
            completed_at: task_state.is_terminal().then_some(now),
        }],
        created_at: now,
        updated_at: now,
        completed_at: group_state.is_terminal().then_some(now),
    }
}

fn delegated_progress(name: &str, status: AgentProgressStatus) -> DelegatedProgressEvent {
    let stage = match status {
        AgentProgressStatus::Created
        | AgentProgressStatus::Queued
        | AgentProgressStatus::Leased => DelegatedRunStage::Created,
        AgentProgressStatus::Running | AgentProgressStatus::Retrying => DelegatedRunStage::Running,
        AgentProgressStatus::Complete => DelegatedRunStage::Complete,
        AgentProgressStatus::Degraded => DelegatedRunStage::Degraded,
        AgentProgressStatus::Failed => DelegatedRunStage::Failed,
        AgentProgressStatus::Cancelled => DelegatedRunStage::Cancelled,
    };
    DelegatedProgressEvent {
        delegated_run_id: "run-1".to_owned(),
        parent_session_id: "session".to_owned(),
        tool_call_id: "agent-call".to_owned(),
        kind: DelegatedToolKind::Build,
        stage,
        progress: AgentProgress {
            delegated_run_id: Some("run-1".to_owned()),
            task_id: name.to_owned(),
            name: name.to_owned(),
            status,
            ..AgentProgress::default()
        },
    }
}

#[test]
fn cited_text_keeps_sources_across_live_persisted_and_recovery_projection() {
    use mitsuro_core::ai::types::Citation;
    use mitsuro_core::storage::{
        PartialAssistantState, RecoveryDecision, RecoveryStatus, SessionRecoveryState,
    };
    let first = Citation {
        url: "https://example.org/source".into(),
        title: "Source".into(),
        cited_text: "first finding".into(),
    };
    let second = Citation {
        cited_text: "second finding".into(),
        ..first.clone()
    };
    let citations = vec![first, second];
    let mut live = ConversationProjection::new("citation-session");
    live.apply_event(LoopEvent::TextDelta {
        delta: "Two findings.".into(),
    });
    live.apply_event(LoopEvent::TextDeltaWithCitations {
        delta: String::new(),
        citations: citations.clone(),
    });
    live.apply_event(LoopEvent::TextDeltaWithCitations {
        delta: String::new(),
        citations: citations.clone(),
    });
    let persisted = ConversationProjection::from_model_messages(
        "citation-session",
        &[ModelMessage {
            role: Role::Assistant,
            content: vec![Content::Text {
                text: "Two findings.".into(),
                citations: citations.clone(),
            }],
        }],
    );
    let recovery = SessionRecoveryState::new(
        RecoveryStatus::Streaming,
        None,
        None,
        PartialAssistantState {
            text: "Two findings.".into(),
            citations,
            ..Default::default()
        },
        RecoveryDecision::Resumable {
            latest_user_objective: "Cite evidence".into(),
        },
    );
    let mut restored = ConversationProjection::new("citation-session");
    restored.merge_recovery(&recovery);
    restored.merge_recovery(&recovery);
    let sources = |projection: &ConversationProjection| {
        projection
            .presentation()
            .turns
            .iter()
            .flat_map(|turn| &turn.parts)
            .filter_map(|part| {
                if let TimelinePart::AgentText(text) = part {
                    Some((text.text.clone(), text.citations.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(sources(&live), sources(&persisted));
    assert_eq!(sources(&live), sources(&restored));
    assert_eq!(sources(&restored)[0].1.len(), 2);
}
