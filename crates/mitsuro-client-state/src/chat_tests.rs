use mitsuro_client::{
    ChatStreamEvent, Citation, PartialAssistantState, PermissionMode, RecoveryToolCall,
    SessionRecoveryState, SessionStateResponse, WorkMode,
};
use serde_json::json;

use crate::{
    pending_approval_from_state, AttachmentDraft, AttachmentKind, ChatMessage, ChatStore,
    MessageRole, ToolStatus, TranscriptNode,
};

#[test]
fn recovery_partial_restores_assistant_thinking_and_tools() {
    let mut store = ChatStore::default();
    let snapshot = SessionStateResponse {
        id: "s1".to_owned(),
        agent_state: "streaming".to_owned(),
        started_at: None,
        last_event_at: None,
        mode: WorkMode::Plan,
        permission_mode: PermissionMode::Supervised,
        recovery: Some(SessionRecoveryState {
            schema_version: 1,
            status: "recoverable".to_owned(),
            stop_reason: Some("disconnect".to_owned()),
            last_error: Some("socket closed".to_owned()),
            partial_assistant: PartialAssistantState {
                citations: vec![mitsuro_client::Citation {
                    url: "https://example.test/source".to_owned(),
                    title: "Source".to_owned(),
                    cited_text: "partial answer".to_owned(),
                }],
                text: "partial answer".to_owned(),
                thinking: Some("working it out".to_owned()),
                tool_calls: vec![RecoveryToolCall {
                    id: "tool-1".to_owned(),
                    name: "read".to_owned(),
                    arguments: None,
                }],
            },
            pending_interactions: Vec::new(),
            decision: serde_json::Value::Null,
        }),
        pending_interactions: Vec::new(),
        live_partial_assistant: None,
        delegated_tools: Vec::new(),
        recent_delegated_runs: Vec::new(),
        delegated_run_summaries: Vec::new(),
        delegation_groups: Vec::new(),
        delegation_events: Vec::new(),
        delegation_event_cursor: None,
        last_event_sequence: Some(7),
        agent_stream_generation: 0,
        agent_stream_sequence: 0,
    };

    store.apply_session_state_snapshot(&snapshot);

    assert!(store.state.is_streaming);
    assert_eq!(
        store.state.controls.permission_mode,
        PermissionMode::Supervised
    );
    assert_eq!(store.state.controls.work_mode, WorkMode::Plan);
    assert_eq!(store.state.last_error.as_deref(), Some("socket closed"));
    assert!(store.state.transcript.iter().any(|node| matches!(
        node,
        TranscriptNode::Message(message)
            if message.role == MessageRole::Assistant && message.content == "partial answer"
                && message.citations.len() == 1
                && message.citations[0].url == "https://example.test/source"
    )));
    assert!(store.state.transcript.iter().any(|node| matches!(
        node,
        TranscriptNode::Thinking(thinking) if thinking.content == "working it out"
    )));
    assert!(store.state.transcript.iter().any(|node| matches!(
        node,
        TranscriptNode::Tool(tool)
            if tool.id == "tool-1" && tool.name == "read" && tool.status == ToolStatus::Pending
    )));
}

#[test]
fn pending_interaction_recovery_skips_unknown_kinds() {
    let state: SessionStateResponse = serde_json::from_value(json!({
        "id": "s1",
        "agent_state": "awaiting_input",
        "pending_interactions": [
            { "kind": "future_pending_kind", "id": "ignored" },
            {
                "kind": "plan_confirm",
                "tool_call_id": "plan-1",
                "title": "Mobile plan",
                "task_count": 2
            }
        ]
    }))
    .expect("state should deserialize with unknown pending interaction");

    let approval = pending_approval_from_state(&state).expect("plan approval");
    assert_eq!(approval.tool_call_id, "plan-1");
    assert_eq!(approval.tool_name, "PlanConfirm");
}

fn assistants(store: &ChatStore) -> Vec<&ChatMessage> {
    store
        .state
        .transcript
        .iter()
        .filter_map(|node| match node {
            TranscriptNode::Message(message) if message.role == MessageRole::Assistant => {
                Some(message)
            }
            _ => None,
        })
        .collect()
}

fn citation(turn: usize) -> Citation {
    Citation {
        url: format!("https://example.test/turn-{turn}"),
        title: format!("Turn {turn} source"),
        cited_text: format!("Answer {turn}"),
    }
}

#[test]
fn turn_complete_separates_assistants_and_citations_around_tool_execution() {
    let mut store = ChatStore::default();
    store.submit_user_message("Inspect, then answer.".to_owned());
    store.apply_stream_event(ChatStreamEvent::ThinkingDelta {
        thinking: "Inspecting.".to_owned(),
    });
    store.apply_stream_event(ChatStreamEvent::TextDeltaWithCitations {
        delta: "Answer 1".to_owned(),
        citations: vec![citation(1)],
    });
    store.apply_stream_event(ChatStreamEvent::ToolCallStart {
        id: "read-1".to_owned(),
        name: "read".to_owned(),
    });
    store.apply_stream_event(ChatStreamEvent::ToolResult {
        id: "read-1".to_owned(),
        output: "File contents".to_owned(),
        is_error: false,
    });
    store.apply_stream_event(ChatStreamEvent::TurnComplete {
        turn: 1,
        has_more: true,
    });

    assert!(
        store.state.is_streaming,
        "a provider-turn boundary must not end the run"
    );
    let first = assistants(&store);
    assert_eq!(
        first.len(),
        1,
        "the next assistant is allocated only on content"
    );
    assert!(!first[0].streaming);
    assert_eq!(first[0].content, "Answer 1");
    assert_eq!(first[0].citations, vec![citation(1)]);
    let finalized_first = first[0].clone();
    assert!(store.state.transcript.iter().any(|node| matches!(node,
        TranscriptNode::Thinking(thinking) if thinking.content == "Inspecting." && !thinking.streaming
    )));

    store.apply_stream_event(ChatStreamEvent::TextDeltaWithCitations {
        delta: "Answer 2".to_owned(),
        citations: vec![citation(2)],
    });
    store.apply_stream_event(ChatStreamEvent::TextDelta {
        delta: " continues.".to_owned(),
    });
    let messages = assistants(&store);
    assert_eq!(messages.len(), 2);
    assert_eq!(
        *messages[0], finalized_first,
        "later content must not mutate the finalized turn"
    );
    assert_ne!(messages[0].id, messages[1].id);
    assert_eq!(messages[1].content, "Answer 2 continues.");
    assert_eq!(messages[1].citations, vec![citation(2)]);
    assert!(messages[1].streaming);
    let first_index = store
        .state
        .transcript
        .iter()
        .position(|node| node.id() == messages[0].id)
        .unwrap();
    let tool_index = store
        .state
        .transcript
        .iter()
        .position(|node| node.id() == "read-1")
        .unwrap();
    let second_index = store
        .state
        .transcript
        .iter()
        .position(|node| node.id() == messages[1].id)
        .unwrap();
    assert!(first_index < tool_index && tool_index < second_index);

    store.apply_stream_event(ChatStreamEvent::Finish {
        session_id: "s1".to_owned(),
        stop_reason: "completed".to_owned(),
    });
    assert!(!store.state.is_streaming);
    assert!(assistants(&store).iter().all(|message| !message.streaming));
}

#[test]
fn turn_complete_tool_only_and_repeated_boundaries_remove_only_empty_placeholder() {
    let mut store = ChatStore::default();
    store.submit_user_message("Read the file.".to_owned());
    store.apply_stream_event(ChatStreamEvent::ToolExecuting {
        id: "read-1".to_owned(),
        name: "read".to_owned(),
    });
    assert_eq!(assistants(&store).len(), 1);
    store.apply_stream_event(ChatStreamEvent::TurnComplete {
        turn: 1,
        has_more: true,
    });
    assert!(assistants(&store).is_empty());
    assert!(store.state.is_streaming);
    let settled = store.state.transcript.clone();
    for _ in 0..3 {
        store.apply_stream_event(ChatStreamEvent::TurnComplete {
            turn: 1,
            has_more: true,
        });
        assert_eq!(
            store.state.transcript, settled,
            "repeated boundaries must not create nodes or prose"
        );
    }
    assert!(store.state.transcript.iter().any(|node| matches!(node,
        TranscriptNode::Tool(tool) if tool.id == "read-1" && tool.status == ToolStatus::Running
    )));
    store.apply_stream_event(ChatStreamEvent::Finish {
        session_id: "s1".to_owned(),
        stop_reason: "completed".to_owned(),
    });
    assert!(!store.state.is_streaming);
    assert_eq!(store.state.transcript, settled);
}

#[test]
fn turn_complete_preserves_citation_only_turn_and_allocates_next_on_annotation() {
    let mut store = ChatStore::default();
    store.submit_user_message("Keep the references.".to_owned());
    store.apply_stream_event(ChatStreamEvent::TextDeltaWithCitations {
        delta: String::new(),
        citations: vec![citation(1)],
    });
    store.apply_stream_event(ChatStreamEvent::TurnComplete {
        turn: 1,
        has_more: true,
    });
    let first = assistants(&store);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].content, "");
    assert_eq!(first[0].citations, vec![citation(1)]);
    assert!(!first[0].streaming);
    let first_id = first[0].id.clone();
    store.apply_stream_event(ChatStreamEvent::TurnComplete {
        turn: 1,
        has_more: true,
    });
    assert_eq!(assistants(&store).len(), 1);
    assert!(store.state.is_streaming);
    store.apply_stream_event(ChatStreamEvent::TextDeltaWithCitations {
        delta: String::new(),
        citations: vec![citation(2)],
    });
    store.apply_stream_event(ChatStreamEvent::Finish {
        session_id: "s1".to_owned(),
        stop_reason: "completed".to_owned(),
    });
    let messages = assistants(&store);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].id, first_id);
    assert_ne!(messages[1].id, first_id);
    assert_eq!(messages[1].citations, vec![citation(2)]);
    assert!(messages
        .iter()
        .all(|message| message.content.is_empty() && !message.streaming));
    assert!(!store.state.is_streaming);
}

#[test]
fn turn_complete_preserves_attachment_only_assistant() {
    let mut store = ChatStore::default();
    store.submit_user_message("Keep the attachment.".to_owned());
    let attachment = AttachmentDraft {
        id: "attachment-1".to_owned(),
        kind: AttachmentKind::File,
        name: "result.txt".to_owned(),
        uri: Some("file:///fixture/result.txt".to_owned()),
        mime_type: Some("text/plain".to_owned()),
        base64: None,
    };
    let assistant = store
        .state
        .transcript
        .iter_mut()
        .find_map(|node| match node {
            TranscriptNode::Message(message) if message.role == MessageRole::Assistant => {
                Some(message)
            }
            _ => None,
        })
        .unwrap();
    assistant.attachments.push(attachment.clone());
    store.apply_stream_event(ChatStreamEvent::TurnComplete {
        turn: 1,
        has_more: true,
    });
    let messages = assistants(&store);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].attachments, vec![attachment]);
    assert!(messages[0].content.is_empty());
    assert!(!messages[0].streaming);
    assert!(store.state.is_streaming);
}

#[test]
fn turn_complete_without_more_waits_for_finish_and_keeps_active_assistant() {
    let mut store = ChatStore::default();
    store.submit_user_message("Answer.".to_owned());
    store.apply_stream_event(ChatStreamEvent::TextDelta {
        delta: "Answer".to_owned(),
    });
    let first_id = assistants(&store)[0].id.clone();
    store.apply_stream_event(ChatStreamEvent::TurnComplete {
        turn: 1,
        has_more: false,
    });
    assert!(store.state.is_streaming);
    assert!(assistants(&store)[0].streaming);
    store.apply_stream_event(ChatStreamEvent::TextDeltaWithCitations {
        delta: String::new(),
        citations: vec![citation(1)],
    });
    assert_eq!(assistants(&store).len(), 1);
    assert_eq!(assistants(&store)[0].id, first_id);
    store.apply_stream_event(ChatStreamEvent::Finish {
        session_id: "s1".to_owned(),
        stop_reason: "completed".to_owned(),
    });
    assert!(!store.state.is_streaming);
    assert_eq!(assistants(&store)[0].content, "Answer");
    assert_eq!(assistants(&store)[0].citations, vec![citation(1)]);
    assert!(!assistants(&store)[0].streaming);
}
