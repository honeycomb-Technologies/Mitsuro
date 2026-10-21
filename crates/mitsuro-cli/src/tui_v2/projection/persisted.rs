//! Persisted `ModelMessage` projection.

use mitsuro_core::ai::types::{Content, ModelMessage, Role};

use crate::tui_v2::model::{
    artifact::PartId,
    conversation::{AttachmentKind, AttachmentPart, TimelinePart, ToolStatus, TurnState},
};

use super::{
    tool_output::{artifact_from_tool_value, parse_tool_arguments, LIVE_ARTIFACT_BYTES},
    ConversationProjection,
};

#[derive(Clone, Debug)]
pub struct PersistedMessage {
    pub id: Option<String>,
    pub message: ModelMessage,
}

impl PersistedMessage {
    #[cfg(test)]
    pub fn new(id: impl Into<String>, message: ModelMessage) -> Self {
        Self {
            id: Some(id.into()),
            message,
        }
    }

    pub fn without_id(message: ModelMessage) -> Self {
        Self { id: None, message }
    }
}

pub(super) fn project_model_messages(
    session_id: String,
    messages: &[ModelMessage],
) -> ConversationProjection {
    let messages = messages
        .iter()
        .cloned()
        .map(PersistedMessage::without_id)
        .collect::<Vec<_>>();
    project_persisted_messages(session_id, &messages)
}

pub(super) fn project_persisted_messages(
    session_id: String,
    messages: &[PersistedMessage],
) -> ConversationProjection {
    let mut projection = ConversationProjection::new(session_id);

    for (message_index, envelope) in messages.iter().enumerate() {
        match envelope.message.role {
            Role::System => {}
            Role::Tool => project_tool_results(&mut projection, &envelope.message.content),
            Role::User => {
                project_tool_results(&mut projection, &envelope.message.content);
                let (text, attachments) = user_content(&envelope.message.content, message_index);
                if text.is_empty() && attachments.is_empty() {
                    continue;
                }
                let message_id = envelope
                    .id
                    .clone()
                    .unwrap_or_else(|| format!("message:{message_index}"));
                projection.push_user_prompt(&message_id, text, attachments, false);
            }
            Role::Assistant => {
                project_assistant_content(
                    &mut projection,
                    &envelope.message.content,
                    message_index,
                );
            }
        }
    }

    for turn in &mut projection.presentation.turns {
        turn.state = TurnState::Completed;
        for part in &mut turn.parts {
            match part {
                TimelinePart::AgentText(part) => part.streaming = false,
                TimelinePart::Thinking(part) => part.streaming = false,
                _ => {}
            }
        }
    }
    projection.presentation.live_turn_id = None;
    projection.active_text = None;
    projection.active_thinking = None;
    crate::tui_v2::presentation::retention::apply_historical_retention(
        &mut projection.presentation,
    );
    projection
}

fn project_assistant_content(
    projection: &mut ConversationProjection,
    contents: &[Content],
    message_index: usize,
) {
    let mut attachment_ordinal = 0;
    for content in contents {
        match content {
            Content::Text { text, citations } => projection.append_agent_text(
                text,
                citations
                    .iter()
                    .cloned()
                    .map(super::live::map_citation)
                    .collect(),
            ),
            Content::Thinking {
                thinking,
                signature,
            } => projection.complete_thinking(thinking, Some(signature.clone())),
            Content::RedactedThinking { .. } => projection.push_redacted_thinking(),
            Content::ToolUse { id, name, input } => {
                let tool = projection.upsert_tool(id, name, ToolStatus::Pending, false);
                tool.arguments = parse_tool_arguments(input);
            }
            Content::ToolResult {
                tool_use_id,
                output,
                is_error,
            } => apply_persisted_tool_result(
                projection,
                tool_use_id,
                output,
                is_error.unwrap_or(false),
            ),
            Content::Image { image, detail: _ } => {
                projection.push_part(TimelinePart::Attachment(AttachmentPart {
                    id: PartId::from_semantic(format!(
                        "assistant:{message_index}/attachment:{attachment_ordinal}"
                    )),
                    kind: AttachmentKind::Image,
                    label: format!("Image {}", attachment_ordinal + 1),
                    media_type: image.media_type.clone(),
                    url: image.url.clone(),
                    embedded: image.base64.is_some(),
                }));
                attachment_ordinal += 1;
            }
            Content::Document { source } => {
                projection.push_part(TimelinePart::Attachment(AttachmentPart {
                    id: PartId::from_semantic(format!(
                        "assistant:{message_index}/attachment:{attachment_ordinal}"
                    )),
                    kind: AttachmentKind::Document,
                    label: format!("Document {}", attachment_ordinal + 1),
                    media_type: Some(source.media_type.clone()),
                    url: source.url.clone(),
                    embedded: source.data.is_some(),
                }));
                attachment_ordinal += 1;
            }
        }
    }
}

fn project_tool_results(projection: &mut ConversationProjection, contents: &[Content]) {
    for content in contents {
        if let Content::ToolResult {
            tool_use_id,
            output,
            is_error,
        } = content
        {
            apply_persisted_tool_result(projection, tool_use_id, output, is_error.unwrap_or(false));
        }
    }
}

fn apply_persisted_tool_result(
    projection: &mut ConversationProjection,
    tool_use_id: &str,
    output: &serde_json::Value,
    is_error: bool,
) {
    let tool = projection.upsert_tool(
        tool_use_id,
        "",
        if is_error {
            ToolStatus::Failed
        } else {
            ToolStatus::Succeeded
        },
        false,
    );
    if tool.name.is_empty() {
        tool.name = "tool".to_owned();
    }
    // Same envelope unwrap as live finalize — keep live/persisted panels aligned.
    let name = tool.name.clone();
    tool.artifact = artifact_from_tool_value(&name, output, LIVE_ARTIFACT_BYTES, false);
}

fn user_content(contents: &[Content], message_index: usize) -> (String, Vec<AttachmentPart>) {
    let mut text = String::new();
    let mut attachments = Vec::new();

    for content in contents {
        match content {
            Content::Text { text: value, .. } => {
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(value);
            }
            Content::Image { image, detail: _ } => attachments.push(AttachmentPart {
                id: PartId::from_semantic(format!(
                    "message:{message_index}/attachment:{}",
                    attachments.len()
                )),
                kind: AttachmentKind::Image,
                label: format!("Image {}", attachments.len() + 1),
                media_type: image.media_type.clone(),
                url: image.url.clone(),
                embedded: image.base64.is_some(),
            }),
            Content::Document { source } => attachments.push(AttachmentPart {
                id: PartId::from_semantic(format!(
                    "message:{message_index}/attachment:{}",
                    attachments.len()
                )),
                kind: AttachmentKind::Document,
                label: format!("Document {}", attachments.len() + 1),
                media_type: Some(source.media_type.clone()),
                url: source.url.clone(),
                embedded: source.data.is_some(),
            }),
            Content::ToolUse { .. }
            | Content::ToolResult { .. }
            | Content::Thinking { .. }
            | Content::RedactedThinking { .. } => {}
        }
    }

    (text, attachments)
}

#[cfg(test)]
mod tests {
    use mitsuro_core::ai::types::Content;
    use serde_json::json;

    use super::*;

    #[test]
    fn tool_result_messages_do_not_create_false_user_turns() {
        let projection = ConversationProjection::from_model_messages(
            "session",
            &[
                ModelMessage {
                    role: Role::User,
                    content: vec![Content::text("inspect".to_owned())],
                },
                ModelMessage {
                    role: Role::Assistant,
                    content: vec![Content::ToolUse {
                        id: "read-1".to_owned(),
                        name: "read".to_owned(),
                        input: json!({"path": "src/main.rs"}),
                    }],
                },
                ModelMessage {
                    role: Role::User,
                    content: vec![Content::ToolResult {
                        tool_use_id: "read-1".to_owned(),
                        output: json!("content"),
                        is_error: Some(false),
                    }],
                },
            ],
        );

        assert_eq!(projection.presentation().turns.len(), 1);
        assert!(matches!(
            projection.presentation().turns[0].parts.first(),
            Some(TimelinePart::Tool(tool)) if tool.status == ToolStatus::Succeeded
        ));
    }

    #[test]
    fn embedded_attachment_payload_never_enters_the_render_model() {
        let projection = ConversationProjection::from_model_messages(
            "session",
            &[ModelMessage {
                role: Role::User,
                content: vec![Content::Image {
                    image: mitsuro_core::ai::types::ImageContent {
                        url: None,
                        base64: Some("secret-binary-body".to_owned()),
                        media_type: Some("image/png".to_owned()),
                    },
                    detail: None,
                }],
            }],
        );

        let attachment = &projection.presentation().turns[0]
            .user
            .as_ref()
            .expect("user")
            .attachments[0];
        assert!(attachment.embedded);
        assert!(!format!("{attachment:?}").contains("secret-binary-body"));
    }
}
