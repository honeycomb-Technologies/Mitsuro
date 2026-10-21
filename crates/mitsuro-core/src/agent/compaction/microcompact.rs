//! Layer 0: cheap per-turn pressure relief before full compaction.

use std::collections::HashSet;

use crate::agent::history_policy::ToolRetention;
use crate::ai::types::{Content, ModelMessage, Role};
use serde_json::Value;

const KEEP_RECENT_MESSAGES: usize = 6;
const KEEP_RECENT_THINKING_ASSISTANTS: usize = 2;
const MAX_COMPLETED_TOOL_INPUT_CHARS: usize = 4_000;
/// Batch rolling history rewrites so a warm prefix is not invalidated on every
/// turn as one more message crosses the retention boundary.
pub(crate) const MICROCOMPACT_HISTORY_BATCH_MESSAGES: usize = 12;

pub(crate) struct MicrocompactResult {
    pub messages: Vec<ModelMessage>,
    pub changed: bool,
    pub history_rewritten: bool,
    pub tool_inputs_rewritten: bool,
}

pub(crate) fn microcompact_messages(conversation: &[ModelMessage]) -> MicrocompactResult {
    microcompact_messages_cache_aware(conversation, true)
}

pub(crate) fn should_rewrite_microcompact_history(
    message_count: usize,
    last_rewrite_message_count: usize,
) -> bool {
    message_count > KEEP_RECENT_MESSAGES
        && message_count.saturating_sub(last_rewrite_message_count)
            >= MICROCOMPACT_HISTORY_BATCH_MESSAGES
}

pub(crate) fn microcompact_messages_cache_aware(
    conversation: &[ModelMessage],
    rewrite_history: bool,
) -> MicrocompactResult {
    if conversation.is_empty() {
        return MicrocompactResult {
            messages: Vec::new(),
            changed: false,
            history_rewritten: false,
            tool_inputs_rewritten: false,
        };
    }

    let mut messages = conversation.to_vec();
    let mut history_rewritten = false;
    if rewrite_history {
        let thinking_changed = strip_old_thinking(&mut messages);
        let tool_results_changed = compact_old_tool_results(&mut messages);
        history_rewritten = thinking_changed || tool_results_changed;
    }
    let tool_inputs_rewritten = compact_large_tool_inputs(&mut messages);
    let changed = history_rewritten || tool_inputs_rewritten;

    MicrocompactResult {
        messages,
        changed,
        history_rewritten,
        tool_inputs_rewritten,
    }
}

fn compact_large_tool_inputs(messages: &mut [ModelMessage]) -> bool {
    let completed_tool_ids = messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|content| match content {
            Content::ToolResult { tool_use_id, .. } => Some(tool_use_id.clone()),
            _ => None,
        })
        .collect::<HashSet<_>>();

    let mut changed = false;
    for message in messages {
        if message.role != Role::Assistant {
            continue;
        }
        for content in &mut message.content {
            let Content::ToolUse { id, name, input } = content else {
                continue;
            };
            // Never rewrite an interrupted/in-flight call. Once its matching
            // result exists, filesystem artifacts and the structured result
            // are canonical; retaining a full generated file body in the
            // provider transcript only spends context repeatedly.
            if completed_tool_ids.contains(id) && compact_completed_tool_input(name, input) {
                changed = true;
            }
        }
    }
    changed
}

fn compact_completed_tool_input(tool_name: &str, input: &mut Value) -> bool {
    if tool_name == "write" {
        return compact_artifact_field(input, "content", "write content");
    }

    compact_large_strings(input)
}

fn compact_artifact_field(input: &mut Value, field: &str, label: &str) -> bool {
    let Some(value) = input.get_mut(field) else {
        return false;
    };
    let Value::String(text) = value else {
        return false;
    };
    if text.len() <= MAX_COMPLETED_TOOL_INPUT_CHARS {
        return false;
    }

    let original_chars = text.chars().count();
    *text = format!(
        "[completed {label} omitted: {original_chars} chars; read the artifact for current contents]"
    );
    true
}

fn compact_large_strings(value: &mut Value) -> bool {
    match value {
        Value::String(text)
            if text.len() > MAX_COMPLETED_TOOL_INPUT_CHARS
                && !text.contains("[completed tool input truncated from ") =>
        {
            let original_chars = text.chars().count();
            *text = format!(
                "{}\n\n[completed tool input truncated from {original_chars} chars]",
                truncate_chars(text, MAX_COMPLETED_TOOL_INPUT_CHARS)
            );
            true
        }
        Value::Array(items) => {
            let mut changed = false;
            for item in items {
                changed |= compact_large_strings(item);
            }
            changed
        }
        Value::Object(map) => {
            let mut changed = false;
            for item in map.values_mut() {
                changed |= compact_large_strings(item);
            }
            changed
        }
        _ => false,
    }
}

fn strip_old_thinking(messages: &mut [ModelMessage]) -> bool {
    let mut assistant_with_thinking = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        if message.role == Role::Assistant
            && message
                .content
                .iter()
                .any(|content| matches!(content, Content::Thinking { .. }))
        {
            assistant_with_thinking.push(index);
        }
    }

    if assistant_with_thinking.len() <= KEEP_RECENT_THINKING_ASSISTANTS {
        return false;
    }

    let strip_count = assistant_with_thinking.len() - KEEP_RECENT_THINKING_ASSISTANTS;
    let mut changed = false;
    for index in assistant_with_thinking.into_iter().take(strip_count) {
        let message = &mut messages[index];
        let original_len = message.content.len();
        message
            .content
            .retain(|content| !matches!(content, Content::Thinking { .. }));
        if message.content.len() != original_len {
            changed = true;
        }
    }
    changed
}

fn compact_old_tool_results(messages: &mut [ModelMessage]) -> bool {
    if messages.len() <= KEEP_RECENT_MESSAGES {
        return false;
    }

    let cutoff = messages.len().saturating_sub(KEEP_RECENT_MESSAGES);
    let mut changed = false;

    for message in &mut messages[..cutoff] {
        if message.role != Role::User {
            continue;
        }
        for content in &mut message.content {
            let Content::ToolResult { output, .. } = content else {
                continue;
            };
            let retention = output
                .get("retention")
                .and_then(|value| value.as_str())
                .unwrap_or("retain_full")
                .to_owned();
            let summary = output
                .get("summary")
                .and_then(|value| value.as_str())
                .unwrap_or("tool result cleared")
                .to_owned();

            match retention.as_str() {
                "retain_full"
                    if output.get("tool").and_then(Value::as_str) == Some("read")
                        && compact_old_read_result(output) =>
                {
                    changed = true;
                }
                "drop_after_compaction" => {
                    *output = serde_json::json!({
                        "retention": ToolRetention::DropAfterCompaction.as_str(),
                        "summary": summary,
                        "result": "[Old tool result content cleared during microcompact — re-run tool if needed]",
                    });
                    changed = true;
                }
                "summarize_after_turn" => {
                    if let Some(result) = output.get("result") {
                        if result.as_str().is_some_and(|value| {
                            value.contains("[microcompact truncated]")
                                || value.contains(
                                    "[Old tool result content cleared during microcompact",
                                )
                        }) {
                            continue;
                        }
                        let compact = serde_json::to_string(result).unwrap_or_default();
                        if compact.chars().count() > 500 {
                            output["result"] = serde_json::json!(format!(
                                "{}...[microcompact truncated]",
                                truncate_chars(&compact, 500)
                            ));
                            changed = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    changed
}

fn compact_old_read_result(output: &mut Value) -> bool {
    let Some(result) = output.get_mut("result") else {
        return false;
    };
    let payload = if result.get("data").is_some() {
        result.get_mut("data").expect("data checked above")
    } else {
        result
    };
    let Some(content) = payload.get_mut("content") else {
        return false;
    };
    if content
        .as_str()
        .is_some_and(|text| text.contains("[Old read content cleared during microcompact"))
    {
        return false;
    }

    *content = Value::String(
        "[Old read content cleared during microcompact — re-read the file if needed]".to_string(),
    );
    true
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn microcompact_bounds_large_completed_tool_arguments() {
        let content = "x".repeat(MAX_COMPLETED_TOOL_INPUT_CHARS + 2_000);
        let messages = vec![
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::ToolUse {
                    id: "tool-1".to_string(),
                    name: "write".to_string(),
                    input: json!({"file_path": "site/index.html", "content": content}),
                }],
            },
            ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "tool-1".to_string(),
                    output: json!({"summary": "wrote site/index.html"}),
                    is_error: None,
                }],
            },
        ];

        let compacted = microcompact_messages(&messages);
        assert!(compacted.changed);
        let Content::ToolUse { input, .. } = &compacted.messages[0].content[0] else {
            panic!("expected tool use");
        };
        let compact_content = input["content"]
            .as_str()
            .expect("content receipt should remain text");
        assert!(compact_content.len() < 150);
        assert!(compact_content.contains("completed write content omitted"));
        assert_eq!(input["file_path"], "site/index.html");
    }

    #[test]
    fn microcompact_large_tool_input_is_idempotent() {
        let messages = vec![
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::ToolUse {
                    id: "tool-1".to_string(),
                    name: "apply_patch".to_string(),
                    input: json!({"patch": "x".repeat(MAX_COMPLETED_TOOL_INPUT_CHARS + 2_000)}),
                }],
            },
            ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "tool-1".to_string(),
                    output: json!({"summary": "applied patch"}),
                    is_error: None,
                }],
            },
        ];

        let once = microcompact_messages(&messages);
        assert!(once.changed);
        let twice = microcompact_messages(&once.messages);
        assert!(!twice.tool_inputs_rewritten);
        assert_eq!(
            serde_json::to_value(&once.messages).expect("serialize once"),
            serde_json::to_value(&twice.messages).expect("serialize twice")
        );
    }

    #[test]
    fn microcompact_preserves_in_flight_tool_arguments() {
        let content = "x".repeat(MAX_COMPLETED_TOOL_INPUT_CHARS + 2_000);
        let messages = vec![ModelMessage {
            role: Role::Assistant,
            content: vec![Content::ToolUse {
                id: "tool-1".to_string(),
                name: "write".to_string(),
                input: json!({"file_path": "site/index.html", "content": content}),
            }],
        }];

        let compacted = microcompact_messages(&messages);
        assert!(!compacted.changed);
        let Content::ToolUse { input, .. } = &compacted.messages[0].content[0] else {
            panic!("expected tool use");
        };
        assert_eq!(input["content"], content);
    }

    #[test]
    fn microcompact_clears_old_read_body_but_keeps_read_coordinates() {
        let mut messages = vec![
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::ToolUse {
                    id: "read-1".to_string(),
                    name: "read".to_string(),
                    input: json!({"file_path": "src/main.rs", "offset": 20, "limit": 50}),
                }],
            },
            ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "read-1".to_string(),
                    output: json!({
                        "tool": "read",
                        "retention": "retain_full",
                        "summary": "read returned 50 lines starting at line 20",
                        "result": {
                            "ok": true,
                            "data": {
                                "content": "fn main() {}".repeat(1_000),
                                "total_lines": 200,
                                "lines_returned": 50,
                                "start_line": 20
                            }
                        }
                    }),
                    is_error: None,
                }],
            },
        ];
        messages.extend((0..KEEP_RECENT_MESSAGES).map(|index| ModelMessage {
            role: Role::Assistant,
            content: vec![Content::text(format!("later message {index}"))],
        }));

        let compacted = microcompact_messages(&messages);
        assert!(compacted.changed);
        let Content::ToolUse { input, .. } = &compacted.messages[0].content[0] else {
            panic!("expected read tool use");
        };
        assert_eq!(input["file_path"], "src/main.rs");
        assert_eq!(input["offset"], 20);

        let Content::ToolResult { output, .. } = &compacted.messages[1].content[0] else {
            panic!("expected read result");
        };
        assert_eq!(output["result"]["data"]["lines_returned"], 50);
        assert!(output["result"]["data"]["content"]
            .as_str()
            .is_some_and(|content| content.contains("Old read content cleared")));
    }
    #[test]
    fn rolling_history_rewrites_are_batched() {
        let baseline = KEEP_RECENT_MESSAGES;
        assert!(!should_rewrite_microcompact_history(
            baseline + MICROCOMPACT_HISTORY_BATCH_MESSAGES - 1,
            baseline
        ));
        assert!(should_rewrite_microcompact_history(
            baseline + MICROCOMPACT_HISTORY_BATCH_MESSAGES,
            baseline
        ));
    }

    #[test]
    fn cache_aware_microcompact_defers_old_history_rewrites() {
        let mut messages = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::ToolResult {
                tool_use_id: "old".to_string(),
                output: json!({
                    "retention": "drop_after_compaction",
                    "summary": "old result",
                    "result": "large old result"
                }),
                is_error: None,
            }],
        }];
        messages.extend((0..KEEP_RECENT_MESSAGES).map(|index| ModelMessage {
            role: Role::Assistant,
            content: vec![Content::text(format!("later {index}"))],
        }));

        let deferred = microcompact_messages_cache_aware(&messages, false);
        assert!(!deferred.changed);
        let rewritten = microcompact_messages_cache_aware(&messages, true);
        assert!(rewritten.history_rewritten);
    }
}
