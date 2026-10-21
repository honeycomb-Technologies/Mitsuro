use serde_json::Value;

use super::OpenAIParser;
use crate::ai::retry::safe_provider_code;
use crate::ai::sse::SseEvent;
use crate::ai::types::FinishReason;
use crate::ai::usage::parse_openai_chat_usage;

impl OpenAIParser {
    /// Compatibility for callers consuming a single logical event. Streaming
    /// dispatch uses the plural method so compound provider frames stay intact.
    pub(super) fn parse_chat_completions_event(&self, json: &Value) -> anyhow::Result<SseEvent> {
        Ok(self
            .parse_chat_completions_events(json)?
            .into_iter()
            .next()
            .unwrap_or(SseEvent::Skip))
    }

    pub(super) fn parse_chat_completions_events(
        &self,
        json: &Value,
    ) -> anyhow::Result<Vec<SseEvent>> {
        let usage = parse_openai_chat_usage(json);
        let mut events = Vec::new();
        let choice = json
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first());
        if let Some(choice) = choice {
            if let Some(delta) = choice.get("delta") {
                if let Some(content) = delta
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    events.push(SseEvent::TextDelta(content.to_string()));
                }
                if let Some(reasoning) = delta
                    .get("reasoning_content")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    events.push(SseEvent::ThinkingDelta {
                        index: 0,
                        thinking: reasoning.to_string(),
                    });
                }
                // A delta may carry both text and several tool updates. Process
                // every entry before finish_reason drains the accumulated calls.
                if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                    for tool_call in tool_calls {
                        let index = tool_call.get("index").and_then(Value::as_u64).unwrap_or(0);
                        let key = format!("chat-index:{index}");
                        let Some(function) = tool_call.get("function") else {
                            continue;
                        };
                        let tool_id = tool_call
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .unwrap_or(&key);
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            if self.register_tool_call(key.clone(), tool_id, name, None)? {
                                events.push(SseEvent::ToolCallStart {
                                    id: tool_id.to_string(),
                                    name: name.to_string(),
                                });
                            }
                        }
                        if let Some(args) = function.get("arguments").and_then(Value::as_str) {
                            if let Some(id) = self.append_tool_arguments(&key, args)? {
                                if !args.is_empty() {
                                    events.push(SseEvent::ToolCallDelta {
                                        id,
                                        delta: args.to_string(),
                                    });
                                }
                            }
                        }
                    }
                }
            }
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                if reason == "tool_calls" {
                    let tool_calls = self.drain_tool_calls()?;
                    events.push(if tool_calls.is_empty() {
                        SseEvent::Finish {
                            reason: FinishReason::ToolCalls,
                            usage,
                        }
                    } else {
                        SseEvent::FinishWithToolCalls { tool_calls, usage }
                    });
                } else {
                    let reason = match reason {
                        "stop" | "end_turn" => FinishReason::Stop,
                        "length" | "max_tokens" => FinishReason::Length,
                        "content_filter" => FinishReason::ContentFilter,
                        other => FinishReason::Other(safe_provider_code(other)),
                    };
                    events.push(SseEvent::Finish { reason, usage });
                }
                return Ok(events);
            }
        }
        if let Some(usage) = usage {
            events.push(SseEvent::Usage(usage));
        }
        if events.is_empty() {
            events.push(SseEvent::Skip);
        }
        Ok(events)
    }
}
