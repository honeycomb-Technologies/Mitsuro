use std::collections::HashMap;

use mitsuro_client::{
    citations_from_value, MessageResponse, PendingInteractionSnapshot, SessionStateResponse,
    SessionWithMessages,
};
use serde_json::Value;

use crate::{
    AttachmentDraft, AttachmentKind, ChatMessage, MessageRole, PendingToolApproval, ThinkingBlock,
    ToolBlock, ToolStatus, TranscriptNode,
};

pub fn transcript_from_session(snapshot: &SessionWithMessages) -> Vec<TranscriptNode> {
    let tool_results = collect_tool_results(&snapshot.messages);
    let mut nodes = Vec::new();

    for (message_index, message) in snapshot.messages.iter().enumerate() {
        append_message_nodes(message_index, message, &tool_results, &mut nodes);
    }

    nodes
}

pub fn pending_approval_from_state(state: &SessionStateResponse) -> Option<PendingToolApproval> {
    state
        .pending_interactions
        .iter()
        .find_map(pending_approval_from_interaction)
        .or_else(|| {
            state
                .recovery
                .as_ref()?
                .pending_interactions
                .iter()
                .find_map(pending_approval_from_interaction)
        })
}

fn pending_approval_from_interaction(
    interaction: &PendingInteractionSnapshot,
) -> Option<PendingToolApproval> {
    match interaction {
        PendingInteractionSnapshot::ToolApproval { tool_call } => Some(PendingToolApproval {
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.name.clone(),
        }),
        PendingInteractionSnapshot::AskUserQuestion { tool_call_id, .. } => {
            Some(PendingToolApproval {
                tool_call_id: tool_call_id.clone(),
                tool_name: "AskUserQuestion".to_owned(),
            })
        }
        PendingInteractionSnapshot::PlanConfirm { tool_call_id, .. } => Some(PendingToolApproval {
            tool_call_id: tool_call_id.clone(),
            tool_name: "PlanConfirm".to_owned(),
        }),
        PendingInteractionSnapshot::Unknown => None,
    }
}

fn append_message_nodes(
    message_index: usize,
    message: &MessageResponse,
    tool_results: &HashMap<String, ToolResultSnapshot>,
    nodes: &mut Vec<TranscriptNode>,
) {
    let role = if message.role == "user" {
        MessageRole::User
    } else {
        MessageRole::Assistant
    };
    let content_array = message.content.as_array();
    let mut text = String::new();
    let mut citations = Vec::new();
    let mut attachments = Vec::new();
    let mut image_index = 0usize;

    if let Some(blocks) = content_array {
        for block in blocks {
            let block_type = block
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match block_type {
                "text" => {
                    append_text(&mut text, block.get("text"));
                    for citation in citations_from_value(block.get("citations")) {
                        if !citations.contains(&citation) {
                            citations.push(citation);
                        }
                    }
                }
                "image" => {
                    if let Some(attachment) = image_attachment(block, image_index) {
                        attachments.push(attachment);
                        image_index += 1;
                    }
                }
                "thinking" => {
                    if let Some(thinking) = block.get("thinking").and_then(Value::as_str) {
                        nodes.push(TranscriptNode::Thinking(ThinkingBlock {
                            id: format!("stored-thinking-{message_index}"),
                            content: thinking.to_owned(),
                            streaming: false,
                            expanded: false,
                        }));
                    }
                }
                "tool_use" => {
                    if let (Some(id), Some(name)) = (
                        block.get("id").and_then(Value::as_str),
                        block.get("name").and_then(Value::as_str),
                    ) {
                        let result = tool_results.get(id);
                        nodes.push(TranscriptNode::Tool(ToolBlock {
                            id: id.to_owned(),
                            name: name.to_owned(),
                            status: result
                                .map(|result| {
                                    if result.is_error {
                                        ToolStatus::Error
                                    } else {
                                        ToolStatus::Success
                                    }
                                })
                                .unwrap_or(ToolStatus::Pending),
                            output: result
                                .map(|result| result.output.clone())
                                .unwrap_or_default(),
                        }));
                    }
                }
                _ => {}
            }
        }
    } else if let Some(raw) = message.content.as_str() {
        text.push_str(raw);
    } else if let Some(raw) = message.content.get("text").and_then(Value::as_str) {
        text.push_str(raw);
    }

    if !text.trim().is_empty() || !attachments.is_empty() || !citations.is_empty() {
        nodes.push(TranscriptNode::Message(ChatMessage {
            id: format!("stored-message-{message_index}"),
            role,
            content: text,
            citations,
            streaming: false,
            attachments,
        }));
    }
}

fn append_text(text: &mut String, value: Option<&Value>) {
    let Some(value) = value.and_then(Value::as_str) else {
        return;
    };
    text.push_str(value);
}

fn image_attachment(block: &Value, image_index: usize) -> Option<AttachmentDraft> {
    let source = block.get("source")?;
    let source_type = source.get("type").and_then(Value::as_str)?;
    match source_type {
        "base64" => {
            let mime_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .unwrap_or("image/png")
                .to_owned();
            Some(AttachmentDraft {
                id: format!("stored-image-{image_index}"),
                kind: AttachmentKind::Image,
                name: format!(
                    "image-{}.{}",
                    image_index + 1,
                    extension_for_mime(&mime_type)
                ),
                uri: None,
                mime_type: Some(mime_type),
                base64: source
                    .get("data")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            })
        }
        "url" => Some(AttachmentDraft {
            id: format!("stored-image-{image_index}"),
            kind: AttachmentKind::Image,
            name: format!("image-{}", image_index + 1),
            uri: source
                .get("url")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            mime_type: None,
            base64: None,
        }),
        _ => None,
    }
}

fn extension_for_mime(mime_type: &str) -> &'static str {
    match mime_type.trim().to_ascii_lowercase().as_str() {
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        _ => "jpg",
    }
}

#[derive(Clone, Debug)]
struct ToolResultSnapshot {
    output: String,
    is_error: bool,
}

fn collect_tool_results(messages: &[MessageResponse]) -> HashMap<String, ToolResultSnapshot> {
    let mut results = HashMap::new();
    for message in messages {
        let Some(blocks) = message.content.as_array() else {
            continue;
        };
        for block in blocks {
            let block_type = block
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if block_type != "tool_result" && block.get("tool_use_id").is_none() {
                continue;
            }
            let Some(tool_use_id) = block.get("tool_use_id").and_then(Value::as_str) else {
                continue;
            };
            let output = block
                .get("output")
                .or_else(|| block.get("content"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| {
                    block
                        .get("output")
                        .or_else(|| block.get("content"))
                        .cloned()
                        .unwrap_or(Value::Null)
                        .to_string()
                });
            results.insert(
                tool_use_id.to_owned(),
                ToolResultSnapshot {
                    output,
                    is_error: block
                        .get("is_error")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                },
            );
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use mitsuro_client::{MessageResponse, SessionInfo, SessionWithMessages};
    use serde_json::json;

    use super::*;

    #[test]
    fn converts_stored_text_messages() {
        let snapshot = SessionWithMessages {
            session: SessionInfo {
                id: "s1".to_owned(),
                title: "demo".to_owned(),
                updated_at: String::new(),
                token_count: None,
                parent_session_id: None,
                working_dir: None,
                project_dir: None,
                workspace_mode: mitsuro_client::WorkspaceMode::Neutral,
                session_type: mitsuro_client::SessionType::Chat,
                mode: mitsuro_client::WorkMode::Build,
                model: None,
                model_key: None,
                model_catalog_revision: None,
                target_branch: None,
                permission_mode: mitsuro_client::PermissionMode::Autonomous,
            },
            messages: vec![MessageResponse {
                role: "user".to_owned(),
                content: json!([{ "type": "text", "text": "hello" }]),
            }],
        };

        let nodes = transcript_from_session(&snapshot);
        assert!(
            matches!(nodes.first(), Some(TranscriptNode::Message(message)) if message.content == "hello")
        );
    }

    #[test]
    fn joins_tool_use_with_later_tool_result() {
        let messages = vec![
            MessageResponse {
                role: "assistant".to_owned(),
                content: json!([{ "type": "tool_use", "id": "t1", "name": "bash", "input": {} }]),
            },
            MessageResponse {
                role: "user".to_owned(),
                content: json!([{ "type": "tool_result", "tool_use_id": "t1", "content": "ok" }]),
            },
        ];
        let results = collect_tool_results(&messages);
        assert_eq!(
            results.get("t1").map(|result| result.output.as_str()),
            Some("ok")
        );
    }

    #[test]
    fn stored_citations_preserve_plain_text_spacing_and_distinct_quotes() {
        let message = MessageResponse {
            role: "assistant".to_owned(),
            content: json!([
                { "type": "text", "text": "Sourced ", "citations": [{ "url": "https://example.test/source", "title": "Source", "cited_text": "Sourced" }] },
                { "type": "text", "text": "answer", "citations": [{ "url": "https://example.test/source", "title": "Source", "cited_text": "answer" }] }
            ]),
        };
        let mut nodes = Vec::new();
        append_message_nodes(0, &message, &HashMap::new(), &mut nodes);
        let Some(TranscriptNode::Message(assistant)) = nodes.first() else {
            panic!("expected stored assistant");
        };
        assert_eq!(assistant.content, "Sourced answer");
        assert_eq!(assistant.citations.len(), 2);
        assert_eq!(assistant.citations[0].cited_text, "Sourced");
        assert_eq!(assistant.citations[1].cited_text, "answer");
    }
}
