use serde_json::Value;
use sha2::{Digest, Sha256};

use super::build_codex_input_messages;
use crate::ai::client::streaming::codex::session::CodexContinuation;
use crate::ai::types::{Content, ModelMessage, Role};

pub(in crate::ai::client::streaming::codex) struct PreparedCodexRequest {
    pub full_body: Value,
    pub websocket_body: Value,
    pub request_fingerprint: String,
    pub message_fingerprints: Vec<String>,
    pub volatile_context_fingerprint: Option<String>,
    pub used_continuation: bool,
}

pub(in crate::ai::client::streaming::codex) fn prepare_codex_ws_request(
    full_body: Value,
    messages: &[ModelMessage],
    volatile_context: Option<&str>,
    continuation: Option<&CodexContinuation>,
) -> PreparedCodexRequest {
    let request_fingerprint = request_fingerprint(&full_body);
    // System messages are rendered into the layered instruction/context fields,
    // not into Responses `input`; comparing them here would make harmless
    // runtime-context refreshes invalidate an otherwise safe delta chain.
    let conversation_messages = messages
        .iter()
        .filter(|message| message.role != Role::System)
        .cloned()
        .collect::<Vec<_>>();
    let message_fingerprints = conversation_messages
        .iter()
        .map(stable_fingerprint)
        .collect::<Vec<_>>();
    let volatile_context_fingerprint = volatile_context
        .map(str::trim)
        .filter(|context| !context.is_empty())
        .map(stable_fingerprint);

    let mut websocket_body = full_body.clone();
    let mut used_continuation = false;

    if let Some(previous) = continuation {
        let prefix_len = previous.message_fingerprints.len();
        let prefix_matches = message_fingerprints.len() > prefix_len
            && message_fingerprints
                .get(..prefix_len)
                .is_some_and(|prefix| prefix == previous.message_fingerprints.as_slice());
        let new_messages = conversation_messages.get(prefix_len..).unwrap_or_default();
        let prior_assistant_matches = new_messages
            .first()
            .filter(|message| message.role == Role::Assistant)
            .and_then(assistant_fingerprint_from_message)
            .zip(previous.assistant_fingerprint.as_ref())
            .is_some_and(|(current, prior)| &current == prior);
        let can_preserve_volatile_context = !matches!(
            (
                previous.volatile_context_fingerprint.as_ref(),
                volatile_context_fingerprint.as_ref(),
            ),
            (Some(_), None)
        );

        if previous.request_fingerprint == request_fingerprint
            && prefix_matches
            && prior_assistant_matches
            && can_preserve_volatile_context
        {
            let context_changed =
                previous.volatile_context_fingerprint != volatile_context_fingerprint;
            let delta_context = context_changed.then_some(volatile_context).flatten();
            let delta_input = build_codex_input_messages(&new_messages[1..], delta_context);

            if !delta_input.is_empty() {
                websocket_body["input"] = Value::Array(delta_input);
                websocket_body["previous_response_id"] =
                    Value::String(previous.response_id.clone());
                used_continuation = true;
            }
        }
    }

    PreparedCodexRequest {
        full_body,
        websocket_body,
        request_fingerprint,
        message_fingerprints,
        volatile_context_fingerprint,
        used_continuation,
    }
}

pub(in crate::ai::client::streaming::codex) fn assistant_fingerprint_from_response(
    response: &Value,
) -> Option<String> {
    let output = response.get("output")?.as_array()?;
    let mut semantic_output = Vec::new();

    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                for content in item
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if matches!(
                        content.get("type").and_then(Value::as_str),
                        Some("output_text" | "text")
                    ) {
                        if let Some(text) = content.get("text").and_then(Value::as_str) {
                            semantic_output.push(serde_json::json!({
                                "type": "text",
                                "text": text,
                            }));
                        }
                    }
                }
            }
            Some("function_call") => {
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .and_then(|arguments| serde_json::from_str::<Value>(arguments).ok())
                    .or_else(|| item.get("arguments").cloned())
                    .unwrap_or_else(|| serde_json::json!({}));
                semantic_output.push(serde_json::json!({
                    "type": "tool_use",
                    "id": item
                        .get("call_id")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    "name": item.get("name").and_then(Value::as_str).unwrap_or_default(),
                    "input": arguments,
                }));
            }
            _ => {}
        }
    }

    (!semantic_output.is_empty()).then(|| stable_fingerprint(&semantic_output))
}

fn assistant_fingerprint_from_message(message: &ModelMessage) -> Option<String> {
    let semantic_output = message
        .content
        .iter()
        .filter_map(|content| match content {
            Content::Text { text, citations } => Some(serde_json::json!({
                "type": "text",
                "text": crate::ai::format::provider_text(text, citations),
            })),
            Content::ToolUse { id, name, input } => Some(serde_json::json!({
                "type": "tool_use",
                "id": id,
                "name": name,
                "input": input,
            })),
            _ => None,
        })
        .collect::<Vec<_>>();

    (!semantic_output.is_empty()).then(|| stable_fingerprint(&semantic_output))
}

fn request_fingerprint(body: &Value) -> String {
    let mut stable_body = body.clone();
    if let Some(object) = stable_body.as_object_mut() {
        for field in [
            "input",
            "previous_response_id",
            "stream",
            "background",
            "type",
        ] {
            object.remove(field);
        }
    }
    stable_fingerprint(&stable_body)
}

fn stable_fingerprint<T: serde::Serialize + ?Sized>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn citation_context_invalidates_plain_assistant_continuation_signature() {
        let response = serde_json::json!({"output":[{"type":"message","content":[{"type":"output_text","text":"Answer"}]}]});
        let plain = message(Role::Assistant, vec![Content::text("Answer".into())]);
        let annotated = message(
            Role::Assistant,
            vec![Content::Text {
                text: "Answer".into(),
                citations: vec![crate::ai::types::Citation {
                    url: "https://example.com/docs".into(),
                    title: "Docs".into(),
                    cited_text: "Answer".into(),
                }],
            }],
        );
        assert_eq!(
            assistant_fingerprint_from_message(&plain),
            assistant_fingerprint_from_response(&response)
        );
        assert_ne!(
            assistant_fingerprint_from_message(&annotated),
            assistant_fingerprint_from_response(&response)
        );
    }

    fn message(role: Role, content: Vec<Content>) -> ModelMessage {
        ModelMessage { role, content }
    }

    #[test]
    fn continuation_sends_only_new_items_when_history_and_request_match() {
        let prior_user = message(Role::User, vec![Content::text("inspect".into())]);
        let assistant = message(
            Role::Assistant,
            vec![Content::ToolUse {
                id: "call_1".into(),
                name: "read".into(),
                input: serde_json::json!({"path": "src/lib.rs"}),
            }],
        );
        let tool_result = message(
            Role::Tool,
            vec![Content::ToolResult {
                tool_use_id: "call_1".into(),
                output: Value::String("contents".into()),
                is_error: None,
            }],
        );
        let body = serde_json::json!({
            "model": "gpt-5.6",
            "instructions": "stable",
            "input": build_codex_input_messages(std::slice::from_ref(&prior_user), None),
            "tools": [],
            "stream": true,
        });
        let initial = prepare_codex_ws_request(body, std::slice::from_ref(&prior_user), None, None);
        let assistant_fingerprint = assistant_fingerprint_from_message(&assistant);
        let continuation = CodexContinuation {
            response_id: "resp_1".into(),
            request_fingerprint: initial.request_fingerprint,
            message_fingerprints: initial.message_fingerprints,
            assistant_fingerprint,
            volatile_context_fingerprint: None,
        };
        let messages = vec![prior_user, assistant, tool_result];
        let full_body = serde_json::json!({
            "model": "gpt-5.6",
            "instructions": "stable",
            "input": build_codex_input_messages(&messages, None),
            "tools": [],
            "stream": true,
        });

        let prepared = prepare_codex_ws_request(full_body, &messages, None, Some(&continuation));

        assert!(prepared.used_continuation);
        assert_eq!(prepared.websocket_body["previous_response_id"], "resp_1");
        assert_eq!(
            prepared.websocket_body["input"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            prepared.websocket_body["input"][0]["type"],
            "function_call_output"
        );
    }

    #[test]
    fn warm_continuation_wire_payload_is_under_ten_percent_of_cold_history() {
        let prior_user = message(
            Role::User,
            vec![Content::text(
                "inspect this large repository snapshot\n".repeat(1_000),
            )],
        );
        let assistant = message(
            Role::Assistant,
            vec![Content::ToolUse {
                id: "call_1".into(),
                name: "read".into(),
                input: serde_json::json!({"path": "src/lib.rs"}),
            }],
        );
        let tool_result = message(
            Role::Tool,
            vec![Content::ToolResult {
                tool_use_id: "call_1".into(),
                output: Value::String("small delta".into()),
                is_error: None,
            }],
        );
        let initial_body = serde_json::json!({
            "model": "gpt-5.6",
            "instructions": "stable",
            "input": build_codex_input_messages(std::slice::from_ref(&prior_user), None),
            "tools": [],
            "stream": true,
        });
        let initial =
            prepare_codex_ws_request(initial_body, std::slice::from_ref(&prior_user), None, None);
        let continuation = CodexContinuation {
            response_id: "resp_1".into(),
            request_fingerprint: initial.request_fingerprint,
            message_fingerprints: initial.message_fingerprints,
            assistant_fingerprint: assistant_fingerprint_from_message(&assistant),
            volatile_context_fingerprint: None,
        };
        let messages = vec![prior_user, assistant, tool_result];
        let full_body = serde_json::json!({
            "model": "gpt-5.6",
            "instructions": "stable",
            "input": build_codex_input_messages(&messages, None),
            "tools": [],
            "stream": true,
        });

        let prepared = prepare_codex_ws_request(full_body, &messages, None, Some(&continuation));
        let cold_bytes = serde_json::to_vec(&prepared.full_body).unwrap().len();
        let warm_bytes = serde_json::to_vec(&prepared.websocket_body).unwrap().len();
        println!(
            "codex_continuation cold_bytes={cold_bytes} warm_bytes={warm_bytes} reduction_percent={:.2}",
            100.0 * (cold_bytes - warm_bytes) as f64 / cold_bytes as f64
        );

        assert!(prepared.used_continuation);
        assert!(
            warm_bytes * 10 < cold_bytes,
            "warm continuation should reuse over 90% of the cold request: cold={cold_bytes}B warm={warm_bytes}B"
        );
    }

    #[test]
    fn continuation_resets_when_tools_or_instructions_change() {
        let user = message(Role::User, vec![Content::text("inspect".into())]);
        let assistant = message(Role::Assistant, vec![Content::text("done".into())]);
        let next_user = message(Role::User, vec![Content::text("continue".into())]);
        let initial_body = serde_json::json!({
            "model": "gpt-5.6",
            "instructions": "stable",
            "input": build_codex_input_messages(std::slice::from_ref(&user), None),
            "tools": [{"type":"function","name":"read"}],
            "stream": true,
        });
        let initial =
            prepare_codex_ws_request(initial_body, std::slice::from_ref(&user), None, None);
        let continuation = CodexContinuation {
            response_id: "resp_1".into(),
            request_fingerprint: initial.request_fingerprint,
            message_fingerprints: initial.message_fingerprints,
            assistant_fingerprint: assistant_fingerprint_from_message(&assistant),
            volatile_context_fingerprint: None,
        };
        let messages = vec![user, assistant, next_user];
        let changed_body = serde_json::json!({
            "model": "gpt-5.6",
            "instructions": "changed",
            "input": build_codex_input_messages(&messages, None),
            "tools": [{"type":"function","name":"write"}],
            "stream": true,
        });

        let prepared = prepare_codex_ws_request(changed_body, &messages, None, Some(&continuation));

        assert!(!prepared.used_continuation);
        assert!(prepared
            .websocket_body
            .get("previous_response_id")
            .is_none());
    }

    #[test]
    fn response_and_model_assistant_signatures_match() {
        let response = serde_json::json!({
            "output": [{
                "type": "function_call",
                "call_id": "call_1",
                "name": "read",
                "arguments": "{\"path\":\"src/lib.rs\"}"
            }]
        });
        let message = message(
            Role::Assistant,
            vec![Content::ToolUse {
                id: "call_1".into(),
                name: "read".into(),
                input: serde_json::json!({"path": "src/lib.rs"}),
            }],
        );

        assert_eq!(
            assistant_fingerprint_from_response(&response),
            assistant_fingerprint_from_message(&message)
        );
    }
}
