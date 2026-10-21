use serde_json::Value;
use tracing::{debug, info, warn};

use super::super::events::{SseEvent, SseParser};
use super::SseStreamProcessor;
use crate::ai::retry::{safe_provider_code, safe_provider_event_error};
use crate::ai::streaming::StreamPart;
use crate::ai::types::FinishReason;

impl SseStreamProcessor {
    /// Process SSE data using the provider-specific parser.
    pub async fn process_sse_data<P: SseParser>(
        &mut self,
        data: &str,
        parser: &P,
    ) -> anyhow::Result<()> {
        self.event_count += 1;
        let elapsed = self.stream_start.elapsed();

        if data == "[DONE]" {
            info!(
                "SSE stream [DONE] marker received after {:?}, {} events, {} bytes",
                elapsed, self.event_count, self.bytes_received
            );
            self.stream_buffer.flush().await;
            self.dispatch_part(StreamPart::Finish {
                reason: FinishReason::Stop,
            });
            return Ok(());
        }

        if let Ok(json) = serde_json::from_str::<Value>(data) {
            let event_type_bytes = json
                .get("type")
                .and_then(|t| t.as_str())
                .map_or(0, str::len);
            debug!(
                event_number = self.event_count,
                ?elapsed,
                event_type_bytes,
                "SSE event received"
            );

            for event in parser.parse_events(&json).await? {
                // Smoothing may retain a short text tail. Preserve its order
                // before tool/thinking/citation events from the same frame or
                // a later one, including deltas for already-started tools.
                if !matches!(
                    &event,
                    SseEvent::TextDelta(_) | SseEvent::Usage(_) | SseEvent::Skip
                ) {
                    self.stream_buffer.flush().await;
                }
                match event {
                    SseEvent::TextDelta(text) => {
                        debug!("  -> TextDelta: {} chars", text.len());
                        self.stream_buffer.process_chunk(text).await;
                    }
                    SseEvent::TextDeltaWithCitations { text, citations } => {
                        debug!(
                            "  -> TextDeltaWithCitations: {} chars, {} citations",
                            text.len(),
                            citations.len()
                        );
                        self.dispatch_part(StreamPart::TextDeltaWithCitations {
                            delta: text,
                            citations,
                        });
                    }
                    SseEvent::ToolCallStart { id, name } => {
                        info!(
                            tool_call_id_bytes = id.len(),
                            tool_name_bytes = name.len(),
                            ?elapsed,
                            "SSE tool call started"
                        );
                        self.dispatch_part(StreamPart::ToolCallStart { id, name });
                    }
                    SseEvent::ToolCallDelta { id, delta } => {
                        debug!(
                            tool_call_id_bytes = id.len(),
                            delta_bytes = delta.len(),
                            "SSE tool call delta received"
                        );
                        self.dispatch_part(StreamPart::ToolCallDelta { id, delta });
                    }
                    SseEvent::ToolCallComplete(tool_call) => {
                        info!(
                            tool_call_id_bytes = tool_call.id.len(),
                            tool_name_bytes = tool_call.name.len(),
                            ?elapsed,
                            "SSE tool call completed"
                        );
                        self.dispatch_part(StreamPart::ToolCallComplete { tool_call });
                    }
                    SseEvent::ServerToolStart { id, name } => {
                        info!(
                            tool_call_id_bytes = id.len(),
                            tool_name_bytes = name.len(),
                            ?elapsed,
                            "SSE server tool started"
                        );
                        self.dispatch_part(StreamPart::ServerToolStart { id, name });
                    }
                    SseEvent::ServerToolDelta { id, delta } => {
                        debug!(
                            tool_call_id_bytes = id.len(),
                            delta_bytes = delta.len(),
                            "SSE server tool delta received"
                        );
                        self.dispatch_part(StreamPart::ServerToolDelta { id, delta });
                    }
                    SseEvent::ServerToolComplete { id, name, input } => {
                        info!(
                            tool_call_id_bytes = id.len(),
                            tool_name_bytes = name.len(),
                            ?elapsed,
                            "SSE server tool completed"
                        );
                        self.dispatch_part(StreamPart::ServerToolComplete { id, name, input });
                    }
                    SseEvent::WebSearchResults {
                        tool_use_id,
                        results,
                    } => {
                        info!(
                            result_count = results.len(),
                            tool_call_id_bytes = tool_use_id.len(),
                            ?elapsed,
                            "SSE web search results received"
                        );
                        self.dispatch_part(StreamPart::WebSearchResults {
                            tool_use_id,
                            results,
                        });
                    }
                    SseEvent::WebFetchResult {
                        tool_use_id,
                        content,
                    } => {
                        info!(
                            url_bytes = content.url.len(),
                            tool_call_id_bytes = tool_use_id.len(),
                            ?elapsed,
                            "SSE web fetch result received"
                        );
                        self.dispatch_part(StreamPart::WebFetchResult {
                            tool_use_id,
                            content,
                        });
                    }
                    SseEvent::ServerToolError {
                        tool_use_id,
                        error_code,
                    } => {
                        warn!(
                            error_code_bytes = error_code.len(),
                            tool_call_id_bytes = tool_use_id.len(),
                            ?elapsed,
                            "SSE server tool error received"
                        );
                        self.dispatch_part(StreamPart::ServerToolError {
                            tool_use_id,
                            error_code: safe_provider_code(&error_code),
                        });
                    }
                    SseEvent::ThinkingStart { index } => {
                        info!("SSE ThinkingStart: index={} at {:?}", index, elapsed);
                        self.dispatch_part(StreamPart::ThinkingStart { index });
                    }
                    SseEvent::ThinkingDelta { index, thinking } => {
                        debug!(
                            "  -> ThinkingDelta: index={}, {} chars",
                            index,
                            thinking.len()
                        );
                        self.dispatch_part(StreamPart::ThinkingDelta { index, thinking });
                    }
                    SseEvent::SignatureDelta { index, signature } => {
                        debug!(
                            "  -> SignatureDelta: index={}, {} chars",
                            index,
                            signature.len()
                        );
                        self.dispatch_part(StreamPart::SignatureDelta { index, signature });
                    }
                    SseEvent::ThinkingComplete {
                        index,
                        thinking,
                        signature,
                    } => {
                        info!(
                        "SSE ThinkingComplete: index={}, thinking={} chars, sig={} chars at {:?}",
                        index,
                        thinking.len(),
                        signature.len(),
                        elapsed
                    );
                        self.dispatch_part(StreamPart::ThinkingComplete {
                            index,
                            thinking,
                            signature,
                        });
                    }
                    SseEvent::Finish { reason, usage } => {
                        let reason_kind = match &reason {
                            FinishReason::Stop => "stop",
                            FinishReason::Length => "length",
                            FinishReason::ToolCalls => "tool_calls",
                            FinishReason::ContentFilter => "content_filter",
                            FinishReason::Other(_) => "other",
                        };
                        info!(
                            reason_kind,
                            ?elapsed,
                            event_count = self.event_count,
                            bytes_received = self.bytes_received,
                            "SSE stream finished"
                        );
                        self.stream_buffer.flush().await;
                        if let Some(usage) = usage {
                            self.emit_usage(usage, "from finish");
                        }
                        self.dispatch_part(StreamPart::Finish { reason });
                    }
                    SseEvent::FinishWithToolCalls { tool_calls, usage } => {
                        info!(
                            "SSE FinishWithToolCalls: {} tool calls at {:?} ({} events, {} bytes)",
                            tool_calls.len(),
                            elapsed,
                            self.event_count,
                            self.bytes_received
                        );
                        self.stream_buffer.flush().await;
                        for tool_call in tool_calls {
                            info!(
                                tool_call_id_bytes = tool_call.id.len(),
                                tool_name_bytes = tool_call.name.len(),
                                "Completing SSE tool call"
                            );
                            self.dispatch_part(StreamPart::ToolCallComplete { tool_call });
                        }
                        if let Some(usage) = usage {
                            self.emit_usage(usage, "from finish");
                        }
                        self.dispatch_part(StreamPart::Finish {
                            reason: FinishReason::ToolCalls,
                        });
                    }
                    SseEvent::Usage(usage) => {
                        self.emit_usage(usage, "event");
                    }
                    SseEvent::ContextEdited(metrics) => {
                        info!(
                        "SSE ContextEdited: cleared {} tokens ({} tool uses, {} thinking turns)",
                        metrics.cleared_input_tokens,
                        metrics.cleared_tool_uses,
                        metrics.cleared_thinking_turns
                    );
                        self.dispatch_part(StreamPart::ContextEdited { metrics });
                    }
                    SseEvent::Skip => {
                        debug!("  -> Skip event");
                    }
                }
            }
        } else if !data.is_empty() && !data.trim().is_empty() {
            let safe_error = safe_provider_event_error(
                "Failed to parse SSE JSON",
                None,
                Some("invalid_request_error"),
                Some(data),
            );
            warn!(
                event_number = self.event_count,
                data_bytes = data.len(),
                error = %safe_error,
                "Failed to parse SSE JSON"
            );
        }

        Ok(())
    }
}
