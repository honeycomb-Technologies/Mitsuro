use anyhow::Result;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::ai::model_profile::SystemPromptSections;
use crate::ai::providers::ProviderId;
use crate::ai::sse::{create_streaming_channels, SseParser, SseStreamProcessor};
use crate::ai::streaming::StreamPart;
use crate::ai::types::{AiTool, ModelMessage, Role};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestComponentMetrics {
    pub base_bytes: usize,
    pub identity_bytes: usize,
    pub project_bytes: usize,
    pub session_bytes: usize,
    pub tool_schema_bytes: usize,
    pub history_bytes: usize,
    pub system_message_count: usize,
    pub history_message_count: usize,
    pub tool_count: usize,
    pub request_content_bytes: usize,
    pub estimated_tokens: usize,
    pub request_shape_fingerprint: String,
}

fn estimate_tokens(bytes: usize) -> usize {
    bytes.saturating_add(3) / 4
}

/// Count the exact serialized size without allocating a JSON copy of history.
#[derive(Default)]
struct JsonByteCounter(usize);

impl std::io::Write for JsonByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn request_component_metrics(
    sections: &SystemPromptSections,
    messages: &[ModelMessage],
    tools: Option<&[AiTool]>,
) -> RequestComponentMetrics {
    let base_bytes = sections.base_prompt.len();
    let identity_bytes = sections.identity_context.len();
    let project_bytes = sections.project_context.len();
    let session_bytes = sections.session_context.len();
    let mut system_message_count = 0;
    let mut history_message_count = 0;
    let mut history_bytes = 0;
    for message in messages {
        if message.role == Role::System {
            system_message_count += 1;
        } else {
            history_message_count += 1;
            let mut size = JsonByteCounter::default();
            if serde_json::to_writer(&mut size, message).is_ok() {
                history_bytes += size.0;
            }
        }
    }

    let mut sorted_tools = tools.unwrap_or_default().iter().collect::<Vec<_>>();
    sorted_tools.sort_by(|a, b| a.name.cmp(&b.name));
    let serialized_tools = sorted_tools
        .iter()
        .map(|tool| serde_json::to_vec(tool).unwrap_or_default())
        .collect::<Vec<_>>();
    let tool_schema_bytes = serialized_tools.iter().map(Vec::len).sum();

    // This hash identifies the stable request contract without exposing its
    // prompt or schemas in logs. Dynamic conversation history and session
    // context are deliberately excluded so callers can detect stable-prefix
    // drift across warm turns.
    let mut hasher = Sha256::new();
    for component in [
        sections.base_prompt.as_bytes(),
        sections.identity_context.as_bytes(),
        sections.project_context.as_bytes(),
    ] {
        hasher.update(component.len().to_le_bytes());
        hasher.update(component);
    }
    for tool in &serialized_tools {
        hasher.update(tool.len().to_le_bytes());
        hasher.update(tool);
    }
    let request_shape_fingerprint = format!("{:x}", hasher.finalize());

    let request_content_bytes = base_bytes
        .saturating_add(identity_bytes)
        .saturating_add(project_bytes)
        .saturating_add(session_bytes)
        .saturating_add(tool_schema_bytes)
        .saturating_add(history_bytes);

    RequestComponentMetrics {
        base_bytes,
        identity_bytes,
        project_bytes,
        session_bytes,
        tool_schema_bytes,
        history_bytes,
        system_message_count,
        history_message_count,
        tool_count: sorted_tools.len(),
        request_content_bytes,
        estimated_tokens: estimate_tokens(request_content_bytes),
        request_shape_fingerprint,
    }
}
/// Spawn a stream processing task for an HTTP SSE response.
///
/// Handles the common pattern of reading bytes from a response stream,
/// parsing SSE events, and forwarding them through channels. Sends an
/// explicit error signal if the stream fails, ensuring the receiver
/// never waits on a silently-dead channel.
fn spawn_sse_stream_task<S, P>(
    stream: S,
    mut processor: SseStreamProcessor,
    parser: P,
    tx_err: mpsc::UnboundedSender<StreamPart>,
    label: &'static str,
) where
    S: futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static,
    P: SseParser + 'static,
{
    tokio::spawn(async move {
        tokio::pin!(stream);
        let mut chunk_count: u64 = 0;
        let mut had_error = false;

        loop {
            let chunk = tokio::select! {
                biased;
                _ = tx_err.closed() => {
                    info!("{} stream consumer disconnected; closing upstream response", label);
                    break;
                }
                chunk = stream.next() => chunk,
            };
            let Some(chunk) = chunk else {
                break;
            };
            chunk_count += 1;
            match chunk {
                Ok(bytes) => {
                    if let Err(e) = processor.process_chunk(bytes, &parser).await {
                        warn!("{} chunk #{} parse error: {}", label, chunk_count, e);
                        let _ = tx_err.send(StreamPart::Error {
                            error: format!("{} parse error: {}", label, e),
                        });
                        had_error = true;
                        break;
                    }
                }
                Err(e) => {
                    error!("{} read error at chunk #{}: {}", label, chunk_count, e);
                    let _ = tx_err.send(StreamPart::Error {
                        error: format!("{} read error: {}", label, e),
                    });
                    had_error = true;
                    break;
                }
            }
        }

        if !had_error {
            info!("{} stream ended after {} chunks", label, chunk_count);
        }
        processor.finish().await;
    });
}

#[allow(clippy::too_many_arguments)]
pub(super) fn log_request_metrics(
    label: &str,
    sections: &SystemPromptSections,
    messages: &[ModelMessage],
    tools: Option<&[AiTool]>,
    custom_prompt: bool,
    cache_mode: &'static str,
    cache_key_present: bool,
    wire_body_bytes: usize,
) {
    let metrics = request_component_metrics(sections, messages, tools);
    info!(
        stream_kind = label,
        prompt_family = ?sections.profile.prompt_family,
        base_bytes = metrics.base_bytes,
        base_estimated_tokens = estimate_tokens(metrics.base_bytes),
        identity_bytes = metrics.identity_bytes,
        identity_estimated_tokens = estimate_tokens(metrics.identity_bytes),
        project_bytes = metrics.project_bytes,
        project_estimated_tokens = estimate_tokens(metrics.project_bytes),
        session_bytes = metrics.session_bytes,
        session_estimated_tokens = estimate_tokens(metrics.session_bytes),
        tool_schema_bytes = metrics.tool_schema_bytes,
        tool_schema_estimated_tokens = estimate_tokens(metrics.tool_schema_bytes),
        history_bytes = metrics.history_bytes,
        history_estimated_tokens = estimate_tokens(metrics.history_bytes),
        system_message_count = metrics.system_message_count,
        history_message_count = metrics.history_message_count,
        tool_count = metrics.tool_count,
        request_content_bytes = metrics.request_content_bytes,
        request_estimated_tokens = metrics.estimated_tokens,
        wire_body_bytes,
        wire_body_estimated_tokens = estimate_tokens(wire_body_bytes),
        request_shape_fingerprint = %metrics.request_shape_fingerprint,
        custom_prompt,
        cache_mode,
        cache_key_present,
        "AI request component metrics"
    );
}

pub(super) async fn ensure_success_stream_response(
    response: reqwest::Response,
    call_start: Instant,
    response_label: &str,
    error_label: &str,
) -> Result<reqwest::Response> {
    let status = response.status();
    info!(
        "{}: {} in {:?}",
        response_label,
        status,
        call_start.elapsed()
    );

    if status.is_success() {
        return Ok(response);
    }

    let error = crate::ai::retry::provider_http_error(response, error_label).await;
    error.log();
    Err(error.into())
}

pub(super) fn start_sse_stream<P>(
    response: reqwest::Response,
    parser: P,
    label: &'static str,
    provider_id: ProviderId,
    api_format: crate::ai::models::ApiFormat,
    model_id: &str,
) -> mpsc::UnboundedReceiver<StreamPart>
where
    P: SseParser + 'static,
{
    let (tx, rx) = create_streaming_channels();

    let processor = SseStreamProcessor::new(tx.clone()).with_transform_context(
        provider_id,
        api_format,
        model_id.to_string(),
    );
    spawn_sse_stream_task(response.bytes_stream(), processor, parser, tx, label);

    rx
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use serde_json::json;
    use tokio::sync::oneshot;

    use super::*;
    use crate::ai::model_profile::build_system_prompt_sections;
    use crate::ai::models::ApiFormat;
    use crate::ai::types::Content;

    struct PendingResponseStream {
        dropped: Option<oneshot::Sender<()>>,
    }

    impl futures::Stream for PendingResponseStream {
        type Item = reqwest::Result<bytes::Bytes>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for PendingResponseStream {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    fn message(role: Role, text: &str) -> ModelMessage {
        ModelMessage {
            role,
            content: vec![Content::text(text.to_string())],
        }
    }

    fn tool(name: &str) -> AiTool {
        AiTool {
            name: name.to_string(),
            description: format!("{name} description"),
            input_schema: json!({"type": "object"}),
            prompt: None,
        }
    }

    #[test]
    fn request_metrics_are_stable_across_tool_order() {
        let messages = vec![
            message(Role::System, "[PROJECT INSTRUCTIONS]\nUse Rust."),
            message(Role::User, "secret user text"),
        ];
        let sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.4",
            &messages,
            Some("small base"),
            &[],
        );

        let forward =
            request_component_metrics(&sections, &messages, Some(&[tool("read"), tool("bash")]));
        let reverse =
            request_component_metrics(&sections, &messages, Some(&[tool("bash"), tool("read")]));

        assert_eq!(forward, reverse);
        assert_eq!(forward.system_message_count, 1);
        assert_eq!(forward.history_message_count, 1);
        assert_eq!(forward.tool_count, 2);
        assert_eq!(forward.request_shape_fingerprint.len(), 64);
    }

    #[test]
    fn dynamic_history_changes_size_but_not_request_shape_fingerprint() {
        let first_messages = vec![message(Role::User, "first")];
        let second_messages = vec![message(Role::User, "a much longer second message")];
        let first_sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.4",
            &first_messages,
            Some("small base"),
            &[],
        );
        let second_sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.4",
            &second_messages,
            Some("small base"),
            &[],
        );

        let first = request_component_metrics(&first_sections, &first_messages, None);
        let second = request_component_metrics(&second_sections, &second_messages, None);

        assert_ne!(first.history_bytes, second.history_bytes);
        assert_eq!(
            first.request_shape_fingerprint,
            second.request_shape_fingerprint
        );
    }

    #[test]
    fn volatile_session_context_does_not_change_stable_shape_fingerprint() {
        let first_messages = vec![
            message(Role::System, "[ACTIVE PLAN]\nstep one"),
            message(Role::User, "continue"),
        ];
        let second_messages = vec![
            message(
                Role::System,
                "[ACTIVE PLAN]\nstep one complete; step two active",
            ),
            message(Role::User, "continue"),
        ];
        let first_sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.6",
            &first_messages,
            Some("small base"),
            &[],
        );
        let second_sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.6",
            &second_messages,
            Some("small base"),
            &[],
        );

        let first = request_component_metrics(&first_sections, &first_messages, None);
        let second = request_component_metrics(&second_sections, &second_messages, None);

        assert_ne!(first.session_bytes, second.session_bytes);
        assert_eq!(
            first.request_shape_fingerprint,
            second.request_shape_fingerprint
        );
    }

    #[tokio::test]
    async fn dropping_stream_receiver_closes_pending_upstream_response() {
        let (tx, rx) = create_streaming_channels();
        let processor = SseStreamProcessor::new(tx.clone());
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let stream = PendingResponseStream {
            dropped: Some(dropped_tx),
        };

        spawn_sse_stream_task(
            stream,
            processor,
            crate::ai::parsers::OpenAIParser::new(),
            tx,
            "test",
        );
        drop(rx);

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("upstream stream should close promptly")
            .expect("drop signal should be delivered");
    }
}

#[cfg(test)]
#[path = "metrics_benchmark.rs"]
mod metrics_benchmark;
