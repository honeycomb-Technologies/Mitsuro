//! OpenAI-compatible SSE parser for chat/completions format

mod chat;
mod responses;
mod state;

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde_json::Value;

use crate::ai::retry::safe_provider_event_error;
use crate::ai::sse::{SseEvent, SseParser, ToolCallAccumulator};

/// OpenAI-compatible SSE parser for chat/completions format
pub struct OpenAIParser {
    /// Track tool calls being accumulated
    tool_accumulators: std::sync::Mutex<HashMap<String, ToolCallAccumulator>>,
    /// Preserve tool call ordering for deterministic completion
    tool_order: std::sync::Mutex<Vec<String>>,
    /// Map Responses API item ids to call ids for interleaved argument deltas
    response_item_to_call: std::sync::Mutex<HashMap<String, String>>,
    /// Responses-compatible providers often emit both text-done and part-done
    /// events for one reasoning block. Only surface one completion.
    reasoning_completions_emitted: std::sync::Mutex<HashSet<String>>,
    /// Exact response text already emitted from streaming deltas. Responses
    /// final snapshots can repeat the whole message, so retain the prefix to
    /// recover only genuinely missing text without duplicating streamed output.
    emitted_response_text: std::sync::Mutex<String>,
}

impl OpenAIParser {
    pub fn new() -> Self {
        Self {
            tool_accumulators: std::sync::Mutex::new(HashMap::new()),
            tool_order: std::sync::Mutex::new(Vec::new()),
            response_item_to_call: std::sync::Mutex::new(HashMap::new()),
            reasoning_completions_emitted: std::sync::Mutex::new(HashSet::new()),
            emitted_response_text: std::sync::Mutex::new(String::new()),
        }
    }
}

impl Default for OpenAIParser {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SseParser for OpenAIParser {
    async fn parse_event(&self, json: &Value) -> Result<SseEvent> {
        if let Some(error) = json.get("error") {
            let message = error
                .get("message")
                .and_then(|m| m.as_str())
                .or_else(|| error.as_str());
            let error_type = error.get("type").and_then(|t| t.as_str());
            let error_code = error.get("code").and_then(Value::as_str);
            return Err(anyhow::Error::msg(safe_provider_event_error(
                "OpenAI API error",
                error_code,
                error_type,
                message,
            )));
        }

        if let Some(event_type) = json.get("type").and_then(|t| t.as_str()) {
            if event_type == "error" || event_type.contains("error") {
                let message = json
                    .get("message")
                    .and_then(|m| m.as_str())
                    .or_else(|| json.get("error").and_then(|e| e.as_str()))
                    .or_else(|| json.pointer("/error/message").and_then(Value::as_str));
                let error_code = json.pointer("/error/code").and_then(Value::as_str);
                let error_type = json
                    .pointer("/error/type")
                    .and_then(Value::as_str)
                    .or(Some(event_type));
                return Err(anyhow::Error::msg(safe_provider_event_error(
                    "OpenAI Responses API error",
                    error_code,
                    error_type,
                    message,
                )));
            }
            return self.parse_responses_api_event(json, event_type);
        }

        self.parse_chat_completions_event(json)
    }

    async fn parse_events(&self, json: &Value) -> Result<Vec<SseEvent>> {
        if json.get("type").and_then(Value::as_str).is_none() && json.get("error").is_none() {
            return self.parse_chat_completions_events(json);
        }
        let is_responses_finish = matches!(
            json.get("type").and_then(Value::as_str),
            Some("response.done" | "response.completed")
        );
        let event = self.parse_event(json).await?;
        let citations = Self::responses_snapshot_citations(json);
        let annotation = (!citations.is_empty()).then_some(SseEvent::TextDeltaWithCitations {
            text: String::new(),
            citations,
        });

        if !is_responses_finish {
            let mut events = vec![event];
            events.extend(annotation);
            return Ok(events);
        }

        let mut events = Vec::new();
        if let Some(delta) = self.final_response_snapshot_delta(json)? {
            events.push(SseEvent::TextDelta(delta));
        }
        events.extend(annotation);
        events.push(event);
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::OpenAIParser;
    use crate::ai::sse::{SseEvent, SseParser};

    #[tokio::test]
    async fn provider_error_event_never_reflects_message_type_or_code() {
        const MESSAGE_SENTINEL: &str = "OPENAI_MESSAGE_SENTINEL_624a";
        const TYPE_SENTINEL: &str = "OPENAI_TYPE_SENTINEL_288d";
        const CODE_SENTINEL: &str = "OPENAI_CODE_SENTINEL_99fe";
        let result = OpenAIParser::new()
            .parse_event(&json!({
                "error": {
                    "message": MESSAGE_SENTINEL,
                    "type": TYPE_SENTINEL,
                    "code": CODE_SENTINEL
                }
            }))
            .await;
        let error = match result {
            Ok(_) => panic!("provider error event should fail"),
            Err(error) => error.to_string(),
        };
        for sentinel in [MESSAGE_SENTINEL, TYPE_SENTINEL, CODE_SENTINEL] {
            assert!(!error.contains(sentinel));
        }
        assert!(error.contains("message_fingerprint=sha256:"));
        assert!(error.contains("category_fingerprint=sha256:"));
        assert!(error.contains("code_fingerprint=sha256:"));
    }

    #[tokio::test]
    async fn responses_final_snapshot_recovers_text_when_deltas_are_absent() {
        let parser = OpenAIParser::new();
        let completed = json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {"type": "output_text", "text": "Recovered "},
                        {"type": "output_text", "text": "answer"}
                    ]
                }]
            }
        });

        let events = parser
            .parse_events(&completed)
            .await
            .expect("completed response should parse");

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            SseEvent::TextDelta(text) if text == "Recovered answer"
        ));
        assert!(matches!(&events[1], SseEvent::Finish { .. }));
    }

    #[tokio::test]
    async fn responses_final_snapshot_does_not_duplicate_streamed_text() {
        let parser = OpenAIParser::new();
        let delta = json!({
            "type": "response.output_text.delta",
            "delta": "Already streamed"
        });
        assert!(matches!(
            parser
                .parse_event(&delta)
                .await
                .expect("delta should parse"),
            SseEvent::TextDelta(text) if text == "Already streamed"
        ));

        let completed = json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": "Already streamed"}]
                }]
            }
        });
        let events = parser
            .parse_events(&completed)
            .await
            .expect("completed response should parse");

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], SseEvent::Finish { .. }));
    }

    #[tokio::test]
    async fn responses_output_text_done_recovers_once_and_completed_deduplicates_it() {
        let parser = OpenAIParser::new();
        let text_done = json!({
            "type": "response.output_text.done",
            "text": "Done snapshot"
        });
        assert!(matches!(
            parser
                .parse_event(&text_done)
                .await
                .expect("text done should parse"),
            SseEvent::TextDelta(text) if text == "Done snapshot"
        ));

        let completed = json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output_text": "Done snapshot"
            }
        });
        let events = parser
            .parse_events(&completed)
            .await
            .expect("completed response should parse");

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], SseEvent::Finish { .. }));
    }

    #[tokio::test]
    async fn responses_final_snapshot_recovers_only_exact_missing_suffix() {
        let parser = OpenAIParser::new();
        let delta = json!({
            "type": "response.output_text.delta",
            "delta": "Partial"
        });
        parser
            .parse_event(&delta)
            .await
            .expect("delta should parse");

        let completed = json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output_text": "Partial completion"
            }
        });
        let events = parser
            .parse_events(&completed)
            .await
            .expect("completed response should parse");

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            SseEvent::TextDelta(text) if text == " completion"
        ));
        assert!(matches!(&events[1], SseEvent::Finish { .. }));
    }

    #[tokio::test]
    async fn responses_divergent_final_snapshot_never_corrupts_streamed_text() {
        let parser = OpenAIParser::new();
        let delta = json!({
            "type": "response.output_text.delta",
            "delta": "Authoritative stream"
        });
        parser
            .parse_event(&delta)
            .await
            .expect("delta should parse");

        let completed = json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output_text": "Divergent snapshot"
            }
        });
        let events = parser
            .parse_events(&completed)
            .await
            .expect("completed response should parse");

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], SseEvent::Finish { .. }));
    }

    #[tokio::test]
    async fn responses_reasoning_only_final_snapshot_remains_non_visible() {
        let parser = OpenAIParser::new();
        let completed = json!({
            "type": "response.completed",
            "response": {
                "status": "completed",
                "output": [{
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": "Internal reasoning"}]
                }],
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 18,
                    "output_tokens_details": {"reasoning_tokens": 18}
                }
            }
        });
        let events = parser
            .parse_events(&completed)
            .await
            .expect("reasoning-only response should parse");

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            SseEvent::Finish {
                usage: Some(usage),
                ..
            } if usage.completion_tokens == 18 && usage.reasoning_tokens == 18
        ));
    }

    #[test]
    fn responses_web_search_call_emits_server_tool_events() {
        let parser = OpenAIParser::new();
        let added = json!({
            "type": "response.output_item.added",
            "item": {
                "type": "web_search_call",
                "id": "ws_123",
                "status": "in_progress"
            }
        });
        let event = parser
            .parse_responses_api_event(&added, "response.output_item.added")
            .expect("web search start should parse");
        assert!(
            matches!(event, SseEvent::ServerToolStart { id, name } if id == "ws_123" && name == "web_search")
        );

        let done = json!({
            "type": "response.output_item.done",
            "item": {
                "type": "web_search_call",
                "id": "ws_123",
                "status": "completed",
                "action": {"type": "search", "query": "mitsuro web fetch"}
            }
        });
        let event = parser
            .parse_responses_api_event(&done, "response.output_item.done")
            .expect("web search completion should parse");
        assert!(
            matches!(event, SseEvent::ServerToolComplete { id, name, .. } if id == "ws_123" && name == "web_search")
        );
    }

    #[test]
    fn responses_reasoning_completion_is_emitted_once() {
        let parser = OpenAIParser::new();
        let done = json!({"type": "response.reasoning_summary_text.done"});
        let part_done = json!({"type": "response.reasoning_summary_part.done"});

        let first = parser
            .parse_responses_api_event(&done, "response.reasoning_summary_text.done")
            .expect("first reasoning completion should parse");
        let duplicate = parser
            .parse_responses_api_event(&part_done, "response.reasoning_summary_part.done")
            .expect("duplicate reasoning completion should parse");

        assert!(matches!(first, SseEvent::ThinkingComplete { .. }));
        assert!(matches!(duplicate, SseEvent::Skip));
    }

    #[test]
    fn responses_reasoning_completion_deduplicates_per_summary_block() {
        let parser = OpenAIParser::new();
        let first_text_done = json!({
            "type": "response.reasoning_summary_text.done",
            "item_id": "reasoning-1",
            "output_index": 0,
            "summary_index": 0
        });
        let first_part_done = json!({
            "type": "response.reasoning_summary_part.done",
            "item_id": "reasoning-1",
            "output_index": 0,
            "summary_index": 0
        });
        let second_done = json!({
            "type": "response.reasoning_summary_text.done",
            "item_id": "reasoning-1",
            "output_index": 0,
            "summary_index": 1
        });

        let first = parser
            .parse_responses_api_event(&first_text_done, "response.reasoning_summary_text.done")
            .expect("first reasoning block should complete");
        let duplicate = parser
            .parse_responses_api_event(&first_part_done, "response.reasoning_summary_part.done")
            .expect("duplicate completion should parse");
        let second = parser
            .parse_responses_api_event(&second_done, "response.reasoning_summary_text.done")
            .expect("second reasoning block should complete");

        assert!(matches!(first, SseEvent::ThinkingComplete { .. }));
        assert!(matches!(duplicate, SseEvent::Skip));
        assert!(matches!(second, SseEvent::ThinkingComplete { .. }));
    }

    #[test]
    fn responses_output_item_done_does_not_duplicate_arguments() {
        let parser = OpenAIParser::new();
        let args = "{\"pattern\":\"**/*prompt*\",\"path\":\".\"}";

        let start_event = json!({
            "type": "response.output_item.added",
            "item": {
                "type": "function_call",
                "id": "item_1",
                "call_id": "call_1",
                "name": "glob",
                "arguments": args
            }
        });
        let event = parser
            .parse_responses_api_event(&start_event, "response.output_item.added")
            .expect("start event should parse");
        assert!(matches!(event, SseEvent::ToolCallStart { .. }));

        let done_event = json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "id": "item_1",
                "call_id": "call_1",
                "name": "glob",
                "arguments": args
            }
        });
        let event = parser
            .parse_responses_api_event(&done_event, "response.output_item.done")
            .expect("done event should parse");
        assert!(matches!(event, SseEvent::Skip));

        let finish_event = json!({
            "type": "response.done",
            "response": {
                "status": "incomplete",
                "incomplete_details": {
                    "reason": "tool_calls"
                }
            }
        });
        let event = parser
            .parse_responses_api_event(&finish_event, "response.done")
            .expect("finish event should parse");

        let SseEvent::FinishWithToolCalls { tool_calls, .. } = event else {
            panic!("expected FinishWithToolCalls event");
        };
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].arguments["pattern"], "**/*prompt*");
        assert_eq!(tool_calls[0].arguments["path"], ".");
        assert!(tool_calls[0].arguments.get("raw").is_none());
    }

    #[test]
    fn responses_output_item_done_replaces_partial_snapshot() {
        let parser = OpenAIParser::new();

        let start_event = json!({
            "type": "response.output_item.added",
            "item": {
                "type": "function_call",
                "id": "item_1",
                "call_id": "call_1",
                "name": "glob",
                "arguments": "{\"pattern\":\"**/*prompt*\""
            }
        });
        parser
            .parse_responses_api_event(&start_event, "response.output_item.added")
            .expect("start event should parse");

        let done_event = json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "id": "item_1",
                "call_id": "call_1",
                "name": "glob",
                "arguments": "{\"pattern\":\"**/*prompt*\",\"path\":\".\"}"
            }
        });
        parser
            .parse_responses_api_event(&done_event, "response.output_item.done")
            .expect("done event should parse");

        let finish_event = json!({
            "type": "response.done",
            "response": {
                "status": "incomplete",
                "incomplete_details": {
                    "reason": "tool_calls"
                }
            }
        });
        let event = parser
            .parse_responses_api_event(&finish_event, "response.done")
            .expect("finish event should parse");

        let SseEvent::FinishWithToolCalls { tool_calls, .. } = event else {
            panic!("expected FinishWithToolCalls event");
        };
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].arguments["pattern"], "**/*prompt*");
        assert_eq!(tool_calls[0].arguments["path"], ".");
        assert!(tool_calls[0].arguments.get("raw").is_none());
    }

    #[test]
    fn responses_usage_keeps_cached_tokens_separate() {
        let parser = OpenAIParser::new();
        let done_event = json!({
            "type": "response.done",
            "response": {
                "status": "completed",
                "usage": {
                    "input_tokens": 40747,
                    "input_tokens_details": {"cached_tokens": 40704},
                    "output_tokens": 244,
                    "output_tokens_details": {"reasoning_tokens": 200},
                    "total_tokens": 40991
                }
            }
        });

        let event = parser
            .parse_responses_api_event(&done_event, "response.done")
            .expect("usage event should parse");

        let SseEvent::Finish {
            usage: Some(usage), ..
        } = event
        else {
            panic!("expected finish event with usage");
        };
        assert_eq!(usage.prompt_tokens, 43);
        assert_eq!(usage.cache_read_input_tokens, 40_704);
        assert_eq!(usage.completion_tokens, 244);
        assert_eq!(usage.reasoning_tokens, 200);
        assert_eq!(usage.total_tokens, 40_991);
        assert_eq!(usage.logical_total_tokens(), 40_991);
    }

    #[test]
    fn responses_usage_keeps_gpt_5_6_cache_writes_separate() {
        let parser = OpenAIParser::new();
        let done_event = json!({
            "type": "response.done",
            "response": {
                "status": "completed",
                "usage": {
                    "input_tokens": 1000,
                    "input_tokens_details": {
                        "cached_tokens": 400,
                        "cache_write_tokens": 500
                    },
                    "output_tokens": 50,
                    "total_tokens": 1050
                }
            }
        });

        let event = parser
            .parse_responses_api_event(&done_event, "response.done")
            .expect("usage event should parse");
        let SseEvent::Finish {
            usage: Some(usage), ..
        } = event
        else {
            panic!("expected finish event with usage");
        };

        assert_eq!(usage.prompt_tokens, 100);
        assert_eq!(usage.cache_read_input_tokens, 400);
        assert_eq!(usage.cache_creation_input_tokens, 500);
        assert_eq!(usage.input_tokens(), 1_000);
        assert_eq!(usage.logical_total_tokens(), 1_050);
    }

    #[test]
    fn chat_usage_keeps_cached_tokens_separate() {
        let parser = OpenAIParser::new();
        let event = parser
            .parse_chat_completions_event(&json!({
                "choices": [],
                "usage": {
                    "prompt_tokens": 1000,
                    "prompt_tokens_details": {"cached_tokens": 700},
                    "completion_tokens": 50,
                    "completion_tokens_details": {"reasoning_tokens": 40},
                    "total_tokens": 1050
                }
            }))
            .expect("chat usage should parse");

        let SseEvent::Usage(usage) = event else {
            panic!("expected usage event");
        };
        assert_eq!(usage.prompt_tokens, 300);
        assert_eq!(usage.cache_read_input_tokens, 700);
        assert_eq!(usage.input_tokens(), 1_000);
        assert_eq!(usage.completion_tokens, 50);
        assert_eq!(usage.reasoning_tokens, 40);
        assert_eq!(usage.total_tokens, 1_050);
        assert_eq!(usage.logical_total_tokens(), 1_050);
    }

    #[test]
    fn chat_usage_keeps_gpt_5_6_cache_writes_separate() {
        let parser = OpenAIParser::new();
        let event = parser
            .parse_chat_completions_event(&json!({
                "choices": [],
                "usage": {
                    "prompt_tokens": 1000,
                    "prompt_tokens_details": {
                        "cached_tokens": 400,
                        "cache_write_tokens": 500
                    },
                    "completion_tokens": 50,
                    "total_tokens": 1050
                }
            }))
            .expect("chat usage should parse");

        let SseEvent::Usage(usage) = event else {
            panic!("expected usage event");
        };
        assert_eq!(usage.prompt_tokens, 100);
        assert_eq!(usage.cache_read_input_tokens, 400);
        assert_eq!(usage.cache_creation_input_tokens, 500);
        assert_eq!(usage.input_tokens(), 1_000);
    }

    #[test]
    fn chat_finish_preserves_usage_and_filter_reason() {
        let event = OpenAIParser::new()
            .parse_chat_completions_event(&json!({
                "choices": [{"finish_reason": "content_filter", "delta": {}}],
                "usage": {
                    "prompt_tokens": 20,
                    "completion_tokens": 3,
                    "total_tokens": 23
                }
            }))
            .expect("filtered finish should parse");

        assert!(matches!(
            event,
            SseEvent::Finish {
                reason: crate::ai::types::FinishReason::ContentFilter,
                usage: Some(crate::ai::types::Usage {
                    total_tokens: 23,
                    ..
                })
            }
        ));
    }

    #[test]
    fn responses_argument_deltas_still_accumulate() {
        let parser = OpenAIParser::new();

        let start_event = json!({
            "type": "response.output_item.added",
            "item": {
                "type": "function_call",
                "id": "item_1",
                "call_id": "call_1",
                "name": "glob"
            }
        });
        let event = parser
            .parse_responses_api_event(&start_event, "response.output_item.added")
            .expect("start event should parse");
        assert!(matches!(event, SseEvent::ToolCallStart { .. }));

        let delta_one = json!({
            "type": "response.function_call_arguments.delta",
            "call_id": "call_1",
            "delta": "{\"pattern\":\"**/*"
        });
        parser
            .parse_responses_api_event(&delta_one, "response.function_call_arguments.delta")
            .expect("first delta should parse");

        let delta_two = json!({
            "type": "response.function_call_arguments.delta",
            "call_id": "call_1",
            "delta": "prompt*\",\"path\":\".\"}"
        });
        parser
            .parse_responses_api_event(&delta_two, "response.function_call_arguments.delta")
            .expect("second delta should parse");

        let finish_event = json!({
            "type": "response.done",
            "response": {
                "status": "incomplete",
                "incomplete_details": {
                    "reason": "tool_calls"
                }
            }
        });
        let event = parser
            .parse_responses_api_event(&finish_event, "response.done")
            .expect("finish event should parse");

        let SseEvent::FinishWithToolCalls { tool_calls, .. } = event else {
            panic!("expected FinishWithToolCalls event");
        };
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].arguments["pattern"], "**/*prompt*");
        assert_eq!(tool_calls[0].arguments["path"], ".");
    }
}

#[cfg(test)]
mod mixed_chat_tests {
    use super::*;
    use crate::ai::sse::SseStreamProcessor;
    use crate::ai::streaming::StreamPart;
    use serde_json::json;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn chat_mixed_text_reasoning_and_multiple_tools_preserve_all_updates() {
        let parser = OpenAIParser::new();
        let events = parser
            .parse_events(&json!({"choices":[{"delta":{
                "content":"Inspecting.", "reasoning_content":"Two independent reads.",
                "tool_calls":[
                    {"index":0,"id":"first","function":{"name":"read","arguments":"{\"path\":"}},
                    {"index":1,"id":"second","function":{"name":"grep","arguments":"{\"pattern\":"}}
                ]
            }}]}))
            .await
            .unwrap();
        assert_eq!(events.len(), 6);
        assert!(matches!(&events[0], SseEvent::TextDelta(text) if text == "Inspecting."));
        assert!(
            matches!(&events[1], SseEvent::ThinkingDelta { thinking, .. } if thinking == "Two independent reads.")
        );
        assert!(matches!(&events[2], SseEvent::ToolCallStart { id, .. } if id == "first"));
        assert!(matches!(&events[3], SseEvent::ToolCallDelta { id, .. } if id == "first"));
        assert!(matches!(&events[4], SseEvent::ToolCallStart { id, .. } if id == "second"));
        assert!(matches!(&events[5], SseEvent::ToolCallDelta { id, .. } if id == "second"));
        let finished = parser
            .parse_events(&json!({"choices":[{"delta":{
            "content":" Ready.", "tool_calls":[
                {"index":0,"function":{"arguments":"\"a.rs\"}"}},
                {"index":1,"function":{"arguments":"\"needle\"}"}}
            ]
        },"finish_reason":"tool_calls"}]}))
            .await
            .unwrap();
        assert!(matches!(&finished[0], SseEvent::TextDelta(text) if text == " Ready."));
        let Some(SseEvent::FinishWithToolCalls { tool_calls, .. }) = finished.last() else {
            panic!("missing tools");
        };
        assert_eq!(tool_calls.len(), 2);
        assert_eq!(tool_calls[0].id, "first");
        assert_eq!(tool_calls[0].arguments["path"], "a.rs");
        assert_eq!(tool_calls[1].id, "second");
        assert_eq!(tool_calls[1].arguments["pattern"], "needle");
        let repeated_finish = parser
            .parse_events(&json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}))
            .await
            .unwrap();
        assert!(repeated_finish.iter().all(|event| !matches!(
            event,
            SseEvent::FinishWithToolCalls { .. } | SseEvent::ToolCallComplete(_)
        )));
    }

    #[tokio::test]
    async fn mixed_chat_wire_frames_dispatch_text_before_tools_and_finish_each_call_once() {
        let parser = OpenAIParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut processor = SseStreamProcessor::new(tx);
        let first = json!({"choices":[{"delta":{
            "content":"First.", "tool_calls":[
                {"index":0,"id":"one","function":{"name":"read","arguments":"{\"path\":"}},
                {"index":1,"id":"two","function":{"name":"read","arguments":"{\"path\":"}}
            ]
        }}]});
        let second = json!({"choices":[{"delta":{
            "content":"Tail.", "reasoning_content":"Both selected.", "tool_calls":[
                {"index":0,"function":{"arguments":"\"a\"}"}},
                {"index":1,"function":{"arguments":"\"b\"}"}}
            ]
        },"finish_reason":"tool_calls"}]});
        let wire = format!("data: {first}\n\ndata: {second}\n\ndata: [DONE]\n\n");
        processor
            .process_chunk(bytes::Bytes::from(wire), &parser)
            .await
            .unwrap();
        processor.finish().await;
        drop(processor);
        let mut parts = Vec::new();
        while let Some(part) = rx.recv().await {
            parts.push(part);
        }
        let mut observed = Vec::new();
        let mut completed = Vec::new();
        for part in parts {
            match part {
                StreamPart::TextDelta { delta } => observed.push(format!("text:{delta}")),
                StreamPart::ThinkingDelta { thinking, .. } => {
                    observed.push(format!("thinking:{thinking}"))
                }
                StreamPart::ToolCallStart { id, .. } => observed.push(format!("start:{id}")),
                StreamPart::ToolCallDelta { id, .. } => observed.push(format!("delta:{id}")),
                StreamPart::ToolCallComplete { tool_call } => completed.push(tool_call),
                _ => {}
            }
        }
        assert_eq!(
            observed,
            [
                "text:First.",
                "start:one",
                "delta:one",
                "start:two",
                "delta:two",
                "text:Tail.",
                "thinking:Both selected.",
                "delta:one",
                "delta:two"
            ]
        );
        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].arguments["path"], "a");
        assert_eq!(completed[1].arguments["path"], "b");
    }
}
