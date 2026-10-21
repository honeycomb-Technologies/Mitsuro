mod parts;

use std::collections::HashSet;

use serde_json::Value;
use tracing::debug;

use super::OpenAIFormat;
use crate::ai::format::needs_role_alternation_filler;
use crate::ai::providers::ProviderId;
use crate::ai::types::{Content, ModelMessage, Role};

impl OpenAIFormat {
    pub(super) fn convert_messages_impl(
        &self,
        messages: &[ModelMessage],
        _provider_id: Option<ProviderId>,
    ) -> Vec<Value> {
        let mut tool_use_ids: HashSet<String> = HashSet::new();
        let mut tool_result_ids: HashSet<String> = HashSet::new();

        for msg in messages {
            for content in &msg.content {
                match content {
                    Content::ToolUse { id, .. } => {
                        tool_use_ids.insert(id.clone());
                    }
                    Content::ToolResult { tool_use_id, .. } => {
                        tool_result_ids.insert(tool_use_id.clone());
                    }
                    _ => {}
                }
            }
        }

        let orphaned_ids: HashSet<&String> = tool_use_ids.difference(&tool_result_ids).collect();

        if !orphaned_ids.is_empty() {
            debug!(
                "Found {} orphaned tool calls without results: {:?}",
                orphaned_ids.len(),
                orphaned_ids
            );
        }

        let mut result: Vec<Value> = Vec::new();
        let mut last_role: Option<&str> = None;

        for msg in messages.iter().filter(|m| m.role != Role::System) {
            let role = match msg.role {
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "tool",
                Role::System => continue,
            };

            let has_tool_results = msg
                .content
                .iter()
                .any(|c| matches!(c, Content::ToolResult { .. }));

            if has_tool_results {
                for content in &msg.content {
                    if let Content::ToolResult {
                        tool_use_id,
                        output,
                        ..
                    } = content
                    {
                        let output_str = match output {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        result.push(serde_json::json!({
                            "role": "tool",
                            "tool_call_id": tool_use_id,
                            "content": output_str
                        }));
                    }
                }
                last_role = Some("tool");
                continue;
            }

            if let Some(filler_role) = needs_role_alternation_filler(last_role, role, &["tool"]) {
                debug!(
                    "Inserting filler {} message to maintain alternation",
                    filler_role
                );
                result.push(serde_json::json!({
                    "role": filler_role,
                    "content": "."
                }));
            }

            let has_tool_use = msg
                .content
                .iter()
                .any(|c| matches!(c, Content::ToolUse { .. }));

            if has_tool_use && role == "assistant" {
                let mut tool_calls = Vec::new();
                let mut text_content = String::new();
                let mut orphaned_tool_ids: Vec<String> = Vec::new();

                for content in &msg.content {
                    match content {
                        Content::Text { text, citations } => text_content
                            .push_str(&crate::ai::format::provider_text(text, citations)),
                        Content::ToolUse { id, name, input } => {
                            if orphaned_ids.contains(&id) {
                                orphaned_tool_ids.push(id.clone());
                            }
                            tool_calls.push(serde_json::json!({
                                "id": id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": input.to_string()
                                }
                            }));
                        }
                        // Persisted reasoning is not portable OpenAI assistant text. Keep it in
                        // canonical history, but do not replay it as visible model-facing content.
                        Content::Thinking { .. } | Content::RedactedThinking { .. } => {}
                        _ => {}
                    }
                }

                let mut msg_obj = serde_json::json!({
                    "role": "assistant",
                    "tool_calls": tool_calls
                });
                if !text_content.is_empty() {
                    msg_obj["content"] = serde_json::json!(text_content);
                }
                result.push(msg_obj);

                for orphan_id in orphaned_tool_ids {
                    debug!(
                        "Adding placeholder result for orphaned tool call: {}",
                        orphan_id
                    );
                    result.push(serde_json::json!({
                        "role": "tool",
                        "tool_call_id": orphan_id,
                        "content": "[Tool execution was interrupted - session resumed]"
                    }));
                }

                last_role = Some(role);
                continue;
            }

            let mut text_parts: Vec<String> = Vec::new();
            let mut user_parts: Vec<Value> = Vec::new();
            let mut has_user_images = false;

            for content in &msg.content {
                match content {
                    Content::Text { text, citations }
                        if !text.is_empty() || !citations.is_empty() =>
                    {
                        let text = crate::ai::format::provider_text(text, citations);
                        text_parts.push(text.to_string());
                        if role == "user" {
                            user_parts.push(parts::user_text_part(self, &text));
                        }
                    }
                    // OpenAI-compatible APIs do not accept our durable thinking blocks. In
                    // particular, never flatten them into assistant plaintext for later turns.
                    Content::Thinking { .. } | Content::RedactedThinking { .. } => {}
                    Content::Image { image, detail } if role == "user" => {
                        if let Some(part) = parts::user_image_part(self, image, detail.as_deref()) {
                            user_parts.push(part);
                            has_user_images = true;
                        }
                    }
                    _ => {}
                }
            }

            let text = text_parts.join("\n\n");

            if role == "user" && has_user_images && !user_parts.is_empty() {
                result.push(serde_json::json!({
                    "role": role,
                    "content": user_parts
                }));
                last_role = Some(role);
            } else if !text.is_empty() {
                result.push(serde_json::json!({
                    "role": role,
                    "content": text
                }));
                last_role = Some(role);
            }
        }

        result
    }
}
