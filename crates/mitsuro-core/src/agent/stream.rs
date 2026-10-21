//! Stream processing for the agentic loop.
//!
//! Consumes `StreamPart` events from `AiClient::call_streaming()` and:
//! - Accumulates text, thinking blocks, and tool calls
//! - Emits `LoopEvent`s for each meaningful state change
//! - Handles stream timeout (configurable idle timeout)

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::ai::retry::is_retryable_error_message;
use crate::ai::streaming::StreamPart;
use crate::ai::types::{AiToolCall, Citation, FinishReason, Usage};
use serde_json::Value;

use super::compaction::is_context_overflow_error;
use super::loop_events::{LoopEvent, LoopStopReason};

const RECOVERY_CHECKPOINT_CHAR_INTERVAL: usize = 256;
const TOOL_ARGUMENT_PROGRESS_INTERVAL_BYTES: usize = 4 * 1024;

/// Typed classification of the stream failure that terminated a turn.
///
/// Classification happens once, at the point of receipt, so the orchestrator
/// never re-parses provider error prose downstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamFailure {
    /// Provider reported the request exceeded the model context window; the
    /// sanctioned recovery is one in-place compaction and retry.
    ContextOverflow(String),
    /// Transport-level failure that a fresh attempt may survive.
    RetryableTransport(String),
    /// Terminal failure; the same request is not retried automatically.
    Terminal(String),
    /// No data arrived within the configured idle timeout.
    IdleTimeout(String),
}

impl StreamFailure {
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::ContextOverflow(message)
            | Self::RetryableTransport(message)
            | Self::Terminal(message)
            | Self::IdleTimeout(message) => message,
        }
    }
}

/// Accumulated thinking block from the AI response.
pub(crate) struct ThinkingBlock {
    pub thinking: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamToolCallSummary {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamCheckpoint {
    pub text: String,
    pub citations: Vec<Citation>,
    pub thinking: String,
    pub tool_calls: Vec<StreamToolCallSummary>,
}

/// Result of processing a complete AI stream.
pub(crate) struct StreamResult {
    pub text: String,
    pub citations: Vec<Citation>,
    pub thinking_blocks: Vec<ThinkingBlock>,
    pub tool_calls: Vec<AiToolCall>,
    pub recovery_checkpoint: StreamCheckpoint,
    /// Typed failure classification; `None` when the stream completed cleanly.
    pub last_error: Option<StreamFailure>,
    /// Logical input plus output for durable session accounting.
    pub total_tokens: usize,
    /// Logical input including uncached, cache-write, and cache-read buckets.
    pub prompt_tokens: usize,
    /// Final normalized usage for this single provider call. Live consumers
    /// still receive every cumulative `LoopEvent::Usage` snapshot; this copy
    /// lets observability persist exactly one terminal accounting record.
    pub usage: Usage,
    /// Distinguishes an omitted provider usage frame from a real zero value.
    pub usage_available: bool,
    pub stop_reason: Option<LoopStopReason>,
    /// Whether the provider emitted content or began work that must not be
    /// duplicated by an automatic retry.
    pub produced_output: bool,
    /// Whether a provider-hosted tool (for example web search/fetch) began or
    /// returned work that a semantic continuation must not replay blindly.
    pub had_server_tool_activity: bool,
}

/// Process an AI streaming response, emitting LoopEvents as chunks arrive.
///
/// Returns the accumulated result once the stream completes or times out.
pub(crate) async fn process_stream(
    mut api_rx: mpsc::UnboundedReceiver<StreamPart>,
    event_tx: &mpsc::UnboundedSender<LoopEvent>,
    idle_timeout: Duration,
    mut on_checkpoint: impl FnMut(&StreamCheckpoint),
) -> StreamResult {
    let mut text_buffer = String::new();
    let mut text_citations = Vec::new();
    let mut thinking_buffer = String::new();
    let mut current_thinking_buffer = String::new();
    let mut thinking_blocks = Vec::new();
    let mut tool_calls = Vec::new();
    let mut recovery_tool_calls = Vec::new();
    let mut last_error = None;
    let mut usage = Usage::default();
    let mut usage_available = false;
    let mut stop_reason = None;
    let mut received_finish = false;
    let mut produced_output = false;
    let mut had_server_tool_activity = false;
    let mut last_checkpoint_text_len = 0usize;
    let mut tool_argument_progress = HashMap::<String, (String, usize, usize)>::new();

    loop {
        let receive_timeout = if received_finish {
            // OpenAI-compatible streams can emit their usage-only frame after
            // the finish reason. Drain already queued telemetry without making
            // every provider keep the turn open for the full idle timeout.
            Duration::from_millis(250)
        } else {
            idle_timeout
        };
        let part = match tokio::time::timeout(receive_timeout, api_rx.recv()).await {
            Ok(Some(part)) => part,
            Ok(None) if received_finish => break,
            Ok(None) => {
                // This exact wording is a recognized retryable transport
                // marker; a fresh attempt may receive a well-formed stream.
                last_error = Some(StreamFailure::RetryableTransport(
                    "AI stream ended without a finish signal".to_string(),
                ));
                stop_reason = Some(LoopStopReason::ProviderError);
                break;
            }
            Err(_) if received_finish => break,
            Err(_) => {
                last_error = Some(StreamFailure::IdleTimeout(format!(
                    "AI stream timeout: no data received for {} seconds",
                    idle_timeout.as_secs()
                )));
                stop_reason = Some(LoopStopReason::StreamIdleTimeout);
                break;
            }
        };

        if received_finish && !matches!(&part, StreamPart::Usage { .. } | StreamPart::Finish { .. })
        {
            // A finish signal commits the response. Some compatible providers
            // append duplicate bookkeeping (and older Mitsuro buffering could
            // also race a final text chunk) after that boundary. Never turn an
            // already-complete answer into a failed turn; ignore the malformed
            // tail while continuing to drain usage telemetry.
            tracing::warn!("Ignoring provider stream event received after finish");
            continue;
        }

        match &part {
            StreamPart::TextDelta { delta } => {
                produced_output = true;
                text_buffer.push_str(delta);
                let _ = event_tx.send(LoopEvent::TextDelta {
                    delta: delta.clone(),
                });
                maybe_checkpoint(
                    &text_buffer,
                    &text_citations,
                    &thinking_buffer,
                    &recovery_tool_calls,
                    &mut last_checkpoint_text_len,
                    &mut on_checkpoint,
                    false,
                );
            }
            StreamPart::ThinkingDelta { thinking, .. } => {
                produced_output = true;
                thinking_buffer.push_str(thinking);
                current_thinking_buffer.push_str(thinking);
                let _ = event_tx.send(LoopEvent::ThinkingDelta {
                    thinking: thinking.clone(),
                });
                maybe_checkpoint(
                    &text_buffer,
                    &text_citations,
                    &thinking_buffer,
                    &recovery_tool_calls,
                    &mut last_checkpoint_text_len,
                    &mut on_checkpoint,
                    false,
                );
            }
            StreamPart::ThinkingComplete {
                thinking,
                signature,
                ..
            } => {
                produced_output = true;
                let completed_thinking = if thinking.is_empty() {
                    std::mem::take(&mut current_thinking_buffer)
                } else {
                    if current_thinking_buffer.is_empty() {
                        thinking_buffer.push_str(thinking);
                    } else {
                        current_thinking_buffer.clear();
                    }
                    thinking.clone()
                };
                if !completed_thinking.is_empty() || !signature.is_empty() {
                    thinking_blocks.push(ThinkingBlock {
                        thinking: completed_thinking.clone(),
                        signature: signature.clone(),
                    });
                }
                let _ = event_tx.send(LoopEvent::ThinkingComplete {
                    thinking: completed_thinking,
                    signature: signature.clone(),
                });
            }
            StreamPart::ToolCallStart { id, name } => {
                produced_output = true;
                if !recovery_tool_calls.iter().any(|call| call.id == *id) {
                    recovery_tool_calls.push(StreamToolCallSummary {
                        id: id.clone(),
                        name: name.clone(),
                        arguments: Value::Null,
                    });
                }
                let _ = event_tx.send(LoopEvent::ToolCallStart {
                    id: id.clone(),
                    name: name.clone(),
                });
                tool_argument_progress.insert(
                    id.clone(),
                    (name.clone(), 0, TOOL_ARGUMENT_PROGRESS_INTERVAL_BYTES),
                );
                maybe_checkpoint(
                    &text_buffer,
                    &text_citations,
                    &thinking_buffer,
                    &recovery_tool_calls,
                    &mut last_checkpoint_text_len,
                    &mut on_checkpoint,
                    true,
                );
            }
            StreamPart::ToolCallComplete { tool_call } => {
                produced_output = true;
                tool_calls.push(tool_call.clone());
                if let Some(existing) = recovery_tool_calls
                    .iter_mut()
                    .find(|call| call.id == tool_call.id)
                {
                    existing.name = tool_call.name.clone();
                    existing.arguments = tool_call.arguments.clone();
                } else {
                    recovery_tool_calls.push(StreamToolCallSummary {
                        id: tool_call.id.clone(),
                        name: tool_call.name.clone(),
                        arguments: tool_call.arguments.clone(),
                    });
                }
                let _ = event_tx.send(LoopEvent::ToolCallComplete {
                    id: tool_call.id.clone(),
                    name: tool_call.name.clone(),
                    arguments: tool_call.arguments.clone(),
                });
                tool_argument_progress.remove(&tool_call.id);
                maybe_checkpoint(
                    &text_buffer,
                    &text_citations,
                    &thinking_buffer,
                    &recovery_tool_calls,
                    &mut last_checkpoint_text_len,
                    &mut on_checkpoint,
                    true,
                );
            }
            StreamPart::Finish { reason } => {
                let incomplete_tool_calls = recovery_tool_calls
                    .iter()
                    .filter(|pending| !tool_calls.iter().any(|call| call.id == pending.id))
                    .count();
                match reason {
                    FinishReason::Stop | FinishReason::ToolCalls if incomplete_tool_calls > 0 => {
                        last_error = Some(StreamFailure::Terminal(format!(
                            "AI response ended with {incomplete_tool_calls} incomplete tool call(s); none were executed"
                        )));
                        stop_reason = Some(LoopStopReason::ProviderError);
                    }
                    FinishReason::ToolCalls if tool_calls.is_empty() => {
                        last_error = Some(StreamFailure::Terminal(
                            "AI provider reported tool calls but supplied no complete calls; none were executed".to_string(),
                        ));
                        stop_reason = Some(LoopStopReason::ProviderError);
                    }
                    FinishReason::Stop | FinishReason::ToolCalls => {}
                    FinishReason::Length => {
                        last_error = Some(StreamFailure::Terminal(
                            "AI response reached its output-token limit; incomplete tool calls were not executed".to_string(),
                        ));
                        stop_reason = Some(LoopStopReason::ProviderError);
                    }
                    FinishReason::ContentFilter => {
                        last_error = Some(StreamFailure::Terminal(
                            "AI response was blocked by the provider content filter".to_string(),
                        ));
                        stop_reason = Some(LoopStopReason::ProviderError);
                    }
                    FinishReason::Other(reason) => {
                        last_error = Some(StreamFailure::Terminal(format!(
                            "AI response ended unexpectedly: {reason}"
                        )));
                        stop_reason = Some(LoopStopReason::ProviderError);
                    }
                }
                if stop_reason.is_some() {
                    break;
                }
                received_finish = true;
            }
            StreamPart::Usage { usage: snapshot } => {
                usage_available = true;
                // Usage snapshots can split input and output across events.
                // Merge them before publishing so downstream cache telemetry and
                // context budgeting always see the complete turn so far.
                usage.merge_snapshot(snapshot);
                let _ = event_tx.send(LoopEvent::Usage {
                    prompt_tokens: usage.prompt_tokens,
                    input_tokens: usage.input_tokens(),
                    completion_tokens: usage.completion_tokens,
                    reasoning_tokens: usage.reasoning_tokens,
                    cache_creation_input_tokens: usage.cache_creation_input_tokens,
                    cache_read_input_tokens: usage.cache_read_input_tokens,
                    total_tokens: usage.logical_total_tokens(),
                });
            }
            StreamPart::TextDeltaWithCitations { delta, citations } => {
                produced_output = true;
                text_buffer.push_str(delta);
                let previous_citation_count = text_citations.len();
                for citation in citations {
                    if !text_citations.contains(citation) {
                        text_citations.push(citation.clone());
                    }
                }
                let _ = event_tx.send(LoopEvent::TextDeltaWithCitations {
                    delta: delta.clone(),
                    citations: citations.clone(),
                });
                maybe_checkpoint(
                    &text_buffer,
                    &text_citations,
                    &thinking_buffer,
                    &recovery_tool_calls,
                    &mut last_checkpoint_text_len,
                    &mut on_checkpoint,
                    text_citations.len() != previous_citation_count,
                );
            }
            StreamPart::ServerToolStart { id, name } => {
                produced_output = true;
                had_server_tool_activity = true;
                let _ = event_tx.send(LoopEvent::ServerToolStart {
                    id: id.clone(),
                    name: name.clone(),
                });
            }
            StreamPart::ServerToolComplete { id, name, .. } => {
                produced_output = true;
                had_server_tool_activity = true;
                let _ = event_tx.send(LoopEvent::ServerToolComplete {
                    id: id.clone(),
                    name: name.clone(),
                });
            }
            StreamPart::WebSearchResults {
                tool_use_id,
                results,
            } => {
                produced_output = true;
                had_server_tool_activity = true;
                let _ = event_tx.send(LoopEvent::WebSearchResults {
                    tool_use_id: tool_use_id.clone(),
                    results: results.clone(),
                });
            }
            StreamPart::WebFetchResult {
                tool_use_id,
                content,
            } => {
                produced_output = true;
                had_server_tool_activity = true;
                let _ = event_tx.send(LoopEvent::WebFetchResult {
                    tool_use_id: tool_use_id.clone(),
                    content: content.clone(),
                });
            }
            StreamPart::ServerToolError {
                tool_use_id,
                error_code,
            } => {
                produced_output = true;
                had_server_tool_activity = true;
                let _ = event_tx.send(LoopEvent::ServerToolError {
                    tool_use_id: tool_use_id.clone(),
                    error_code: error_code.clone(),
                });
            }
            StreamPart::Error { error } => {
                last_error = Some(if is_context_overflow_error(error) {
                    StreamFailure::ContextOverflow(error.clone())
                } else if is_retryable_error_message(error) {
                    StreamFailure::RetryableTransport(error.clone())
                } else {
                    StreamFailure::Terminal(error.clone())
                });
                stop_reason = Some(LoopStopReason::ProviderError);
                break;
            }
            StreamPart::ThinkingStart { .. } => {
                produced_output = true;
                current_thinking_buffer.clear();
            }
            StreamPart::ServerToolDelta { .. } => {
                produced_output = true;
                had_server_tool_activity = true;
            }
            StreamPart::ToolCallDelta { id, delta } => {
                produced_output = true;
                let entry = tool_argument_progress.entry(id.clone()).or_insert_with(|| {
                    ("tool".to_string(), 0, TOOL_ARGUMENT_PROGRESS_INTERVAL_BYTES)
                });
                entry.1 = entry.1.saturating_add(delta.len());
                if entry.1 >= entry.2 {
                    let _ = event_tx.send(LoopEvent::ToolCallPreparing {
                        id: id.clone(),
                        name: entry.0.clone(),
                        received_bytes: entry.1,
                    });
                    entry.2 = entry
                        .1
                        .saturating_add(TOOL_ARGUMENT_PROGRESS_INTERVAL_BYTES);
                }
            }
            StreamPart::SignatureDelta { .. } | StreamPart::ContextEdited { .. } => {
                produced_output = true;
            }
            StreamPart::Start { .. } => {}
        }
    }

    let recovery_checkpoint = StreamCheckpoint {
        text: text_buffer.clone(),
        citations: text_citations.clone(),
        thinking: thinking_buffer,
        tool_calls: recovery_tool_calls,
    };
    on_checkpoint(&recovery_checkpoint);

    StreamResult {
        text: text_buffer,
        citations: text_citations,
        thinking_blocks,
        tool_calls,
        recovery_checkpoint,
        last_error,
        total_tokens: usage.logical_total_tokens(),
        prompt_tokens: usage.input_tokens(),
        usage,
        usage_available,
        stop_reason,
        produced_output,
        had_server_tool_activity,
    }
}

fn maybe_checkpoint(
    text: &str,
    citations: &[Citation],
    thinking: &str,
    tool_calls: &[StreamToolCallSummary],
    last_checkpoint_text_len: &mut usize,
    on_checkpoint: &mut impl FnMut(&StreamCheckpoint),
    force: bool,
) {
    if !force
        && text.len().saturating_sub(*last_checkpoint_text_len) < RECOVERY_CHECKPOINT_CHAR_INTERVAL
    {
        return;
    }

    *last_checkpoint_text_len = text.len();
    on_checkpoint(&StreamCheckpoint {
        text: text.to_string(),
        citations: citations.to_vec(),
        thinking: thinking.to_string(),
        tool_calls: tool_calls.to_vec(),
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::process_stream;
    use super::StreamFailure;
    use crate::agent::loop_events::{LoopEvent, LoopStopReason};
    use crate::ai::streaming::StreamPart;
    use crate::ai::types::{AiToolCall, Usage};

    #[tokio::test]
    async fn annotation_only_delta_survives_stream_checkpoint_and_durable_message() {
        use crate::ai::types::{Citation, Content};
        let citation = Citation {
            url: "https://example.org/source".into(),
            title: "Source".into(),
            cited_text: "Supported fact".into(),
        };
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        api_tx
            .send(StreamPart::TextDelta {
                delta: "Supported fact.".into(),
            })
            .unwrap();
        for _ in 0..2 {
            api_tx
                .send(StreamPart::TextDeltaWithCitations {
                    delta: String::new(),
                    citations: vec![citation.clone()],
                })
                .unwrap();
        }
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .unwrap();
        drop(api_tx);
        let mut checkpoints = Vec::new();
        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |checkpoint| {
            checkpoints.push(checkpoint.clone())
        })
        .await;
        assert_eq!(result.text, "Supported fact.");
        assert_eq!(result.citations, vec![citation.clone()]);
        assert_eq!(result.recovery_checkpoint.citations, result.citations);
        assert!(checkpoints
            .iter()
            .any(|checkpoint| checkpoint.citations == result.citations));
        assert_eq!(
            checkpoints.len(),
            2,
            "duplicate annotations must not trigger another checkpoint write"
        );
        let mut streamed = Vec::new();
        while let Ok(event) = event_rx.try_recv() {
            streamed.push(event);
        }
        assert!(
            matches!(&streamed[0], LoopEvent::TextDelta { delta } if delta == "Supported fact.")
        );
        assert!(
            matches!(&streamed[1], LoopEvent::TextDeltaWithCitations { delta, citations } if delta.is_empty() && citations == &result.citations)
        );
        let content = Content::Text {
            text: result.text,
            citations: result.citations,
        };
        let reloaded: Content =
            serde_json::from_str(&serde_json::to_string(&content).unwrap()).unwrap();
        assert!(
            matches!(reloaded, Content::Text { text, citations } if text == "Supported fact." && citations == vec![citation])
        );
    }

    #[tokio::test]
    async fn usage_event_preserves_prompt_completion_split() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::Usage {
                usage: Usage {
                    prompt_tokens: 120,
                    completion_tokens: 45,
                    reasoning_tokens: 40,
                    total_tokens: 165,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                },
            })
            .expect("usage send should succeed");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert_eq!(result.total_tokens, 165);
        assert!(!result.produced_output);

        let usage_event = event_rx
            .recv()
            .await
            .expect("usage event should be emitted");
        match usage_event {
            LoopEvent::Usage {
                prompt_tokens: 120,
                input_tokens: 120,
                completion_tokens: 45,
                reasoning_tokens: 40,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                total_tokens: 165,
            } => {}
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn recovery_checkpoint_captures_partial_thinking() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ThinkingDelta {
                index: 0,
                thinking: "step one".to_string(),
            })
            .expect("thinking delta should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert_eq!(result.recovery_checkpoint.thinking, "step one");
        assert!(result.text.trim().is_empty());
        assert!(result.tool_calls.is_empty());
        assert!(result.stop_reason.is_none());
        assert!(result.produced_output);
        assert!(!result.had_server_tool_activity);
    }

    #[tokio::test]
    async fn thinking_completion_uses_accumulated_deltas() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ThinkingDelta {
                index: 0,
                thinking: "step one".to_string(),
            })
            .expect("thinking delta should send");
        api_tx
            .send(StreamPart::ThinkingDelta {
                index: 0,
                thinking: " then step two".to_string(),
            })
            .expect("second thinking delta should send");
        api_tx
            .send(StreamPart::ThinkingComplete {
                index: 0,
                thinking: String::new(),
                signature: String::new(),
            })
            .expect("thinking completion should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;

        assert_eq!(result.thinking_blocks.len(), 1);
        assert_eq!(result.thinking_blocks[0].thinking, "step one then step two");
        assert_eq!(
            result.recovery_checkpoint.thinking,
            "step one then step two"
        );
        let mut completion = None;
        while let Ok(event) = event_rx.try_recv() {
            if matches!(event, LoopEvent::ThinkingComplete { .. }) {
                completion = Some(event);
            }
        }
        assert!(matches!(
            completion,
            Some(LoopEvent::ThinkingComplete { thinking, signature })
                if thinking == "step one then step two" && signature.is_empty()
        ));
    }

    #[tokio::test]
    async fn thinking_completion_preserves_signed_empty_block() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ThinkingComplete {
                index: 0,
                thinking: String::new(),
                signature: "opaque-signature".to_string(),
            })
            .expect("thinking completion should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;

        assert_eq!(result.thinking_blocks.len(), 1);
        assert!(result.thinking_blocks[0].thinking.is_empty());
        assert_eq!(result.thinking_blocks[0].signature, "opaque-signature");
    }

    #[tokio::test]
    async fn thinking_completion_drops_truly_empty_unsigned_block() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ThinkingComplete {
                index: 0,
                thinking: String::new(),
                signature: String::new(),
            })
            .expect("thinking completion should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;

        assert!(result.thinking_blocks.is_empty());
    }

    #[tokio::test]
    async fn usage_snapshots_merge_input_cache_and_output() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::Usage {
                usage: Usage {
                    prompt_tokens: 100,
                    completion_tokens: 0,
                    reasoning_tokens: 0,
                    total_tokens: 1_000,
                    cache_creation_input_tokens: 200,
                    cache_read_input_tokens: 700,
                },
            })
            .expect("start usage should send");
        api_tx
            .send(StreamPart::Usage {
                usage: Usage {
                    prompt_tokens: 0,
                    completion_tokens: 50,
                    reasoning_tokens: 40,
                    total_tokens: 50,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                },
            })
            .expect("delta usage should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert_eq!(result.prompt_tokens, 1_000);
        assert_eq!(result.total_tokens, 1_050);
        assert_eq!(result.usage.input_tokens(), 1_000);
        assert_eq!(result.usage.completion_tokens, 50);

        assert!(matches!(
            event_rx.recv().await,
            Some(LoopEvent::Usage {
                input_tokens: 1_000,
                completion_tokens: 0,
                total_tokens: 1_000,
                ..
            })
        ));
        assert!(matches!(
            event_rx.recv().await,
            Some(LoopEvent::Usage {
                input_tokens: 1_000,
                completion_tokens: 50,
                total_tokens: 1_050,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn usage_frame_after_finish_is_drained() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        api_tx
            .send(StreamPart::Usage {
                usage: Usage {
                    prompt_tokens: 25,
                    completion_tokens: 10,
                    reasoning_tokens: 5,
                    total_tokens: 100,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 65,
                },
            })
            .expect("trailing usage should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert_eq!(result.prompt_tokens, 90);
        assert_eq!(result.total_tokens, 100);
        assert!(result.stop_reason.is_none());
    }

    #[tokio::test]
    async fn content_after_finish_does_not_invalidate_completed_response() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::TextDelta {
                delta: "complete answer".to_string(),
            })
            .expect("text should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        api_tx
            .send(StreamPart::TextDelta {
                delta: " malformed tail".to_string(),
            })
            .expect("late text should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;

        assert_eq!(result.text, "complete answer");
        assert!(result.last_error.is_none());
        assert!(result.stop_reason.is_none());
        assert!(matches!(
            event_rx.recv().await,
            Some(LoopEvent::TextDelta { delta }) if delta == "complete answer"
        ));
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn length_finish_is_terminal_and_never_executes_partial_tools() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ToolCallStart {
                id: "partial".to_string(),
                name: "bash".to_string(),
            })
            .expect("tool start should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Length,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert!(result.tool_calls.is_empty());
        assert_eq!(result.stop_reason, Some(LoopStopReason::ProviderError));
        assert!(result
            .last_error
            .as_ref()
            .is_some_and(|failure| failure.message().contains("output-token limit")));
    }

    #[tokio::test]
    async fn large_tool_arguments_emit_bounded_preparation_progress() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ToolCallStart {
                id: "large-patch".to_string(),
                name: "apply_patch".to_string(),
            })
            .expect("tool start");
        api_tx
            .send(StreamPart::ToolCallDelta {
                id: "large-patch".to_string(),
                delta: "x".repeat(3_000),
            })
            .expect("first delta");
        api_tx
            .send(StreamPart::ToolCallDelta {
                id: "large-patch".to_string(),
                delta: "x".repeat(2_000),
            })
            .expect("second delta");
        api_tx
            .send(StreamPart::ToolCallComplete {
                tool_call: AiToolCall {
                    id: "large-patch".to_string(),
                    name: "apply_patch".to_string(),
                    arguments: serde_json::json!({"patch": "bounded"}),
                },
            })
            .expect("tool complete");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::ToolCalls,
            })
            .expect("finish");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert_eq!(result.tool_calls.len(), 1);
        let events = std::iter::from_fn(|| event_rx.try_recv().ok()).collect::<Vec<_>>();
        let preparation = events
            .iter()
            .filter_map(|event| match event {
                LoopEvent::ToolCallPreparing {
                    id,
                    name,
                    received_bytes,
                } => Some((id, name, *received_bytes)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(preparation.len(), 1, "progress must be threshold-bounded");
        assert_eq!(preparation[0].0, "large-patch");
        assert_eq!(preparation[0].1, "apply_patch");
        assert_eq!(preparation[0].2, 5_000);
    }

    #[tokio::test]
    async fn nominal_finish_rejects_incomplete_tool_calls() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ToolCallStart {
                id: "partial".to_string(),
                name: "edit".to_string(),
            })
            .expect("tool start should send");
        api_tx
            .send(StreamPart::Finish {
                reason: crate::ai::types::FinishReason::Stop,
            })
            .expect("finish should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert!(result.tool_calls.is_empty());
        assert_eq!(result.stop_reason, Some(LoopStopReason::ProviderError));
        assert!(result
            .last_error
            .as_ref()
            .is_some_and(|failure| failure.message().contains("incomplete tool call")));
    }

    #[tokio::test]
    async fn channel_close_without_finish_is_provider_error() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::TextDelta {
                delta: "partial".to_string(),
            })
            .expect("text should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert_eq!(result.stop_reason, Some(LoopStopReason::ProviderError));
        assert_eq!(
            result.last_error.as_ref().map(StreamFailure::message),
            Some("AI stream ended without a finish signal")
        );
        assert!(result.produced_output);
    }

    #[tokio::test]
    async fn server_tool_start_prevents_automatic_replay() {
        let (api_tx, api_rx) = mpsc::unbounded_channel();
        let (event_tx, _event_rx) = mpsc::unbounded_channel();

        api_tx
            .send(StreamPart::ServerToolStart {
                id: "search-1".to_string(),
                name: "web_search".to_string(),
            })
            .expect("server tool start should send");
        api_tx
            .send(StreamPart::Error {
                error: "API error: 503 Service Unavailable".to_string(),
            })
            .expect("provider error should send");
        drop(api_tx);

        let result = process_stream(api_rx, &event_tx, Duration::from_secs(1), |_| {}).await;
        assert_eq!(result.stop_reason, Some(LoopStopReason::ProviderError));
        assert!(result.produced_output);
        assert!(result.had_server_tool_activity);
    }
}
