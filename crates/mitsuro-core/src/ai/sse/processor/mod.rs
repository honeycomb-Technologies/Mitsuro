use std::time::Instant;

use tokio::sync::mpsc;
use tracing::info;

use crate::ai::models::ApiFormat;
use crate::ai::providers::ProviderId;
use crate::ai::stream_smoothing::StreamBuffer;
use crate::ai::streaming::StreamPart;
use crate::ai::transform::apply_stream_part_transform;
use crate::ai::types::Usage;

mod chunking;
mod dispatch;

/// Maximum partial line buffer size to prevent unbounded growth.
/// SSE lines should never exceed this in practice (1MB).
const MAX_PARTIAL_LINE_SIZE: usize = 1024 * 1024;

/// Common SSE stream processor that handles partial lines and buffering.
pub struct SseStreamProcessor {
    partial_line: String,
    stream_buffer: StreamBuffer,
    tx: mpsc::UnboundedSender<StreamPart>,
    stream_start: Instant,
    event_count: usize,
    bytes_received: usize,
    transform_context: Option<(ProviderId, ApiFormat, String)>,
}

impl SseStreamProcessor {
    /// Create a new SSE stream processor.
    pub fn new(tx: mpsc::UnboundedSender<StreamPart>) -> Self {
        info!("SSE stream processor created");
        Self {
            partial_line: String::new(),
            stream_buffer: StreamBuffer::new(tx.clone()),
            tx,
            stream_start: Instant::now(),
            event_count: 0,
            bytes_received: 0,
            transform_context: None,
        }
    }

    /// Attach provider/model context so post-parse transforms can normalize
    /// stream parts before they reach the orchestrator.
    pub fn with_transform_context(
        mut self,
        provider_id: ProviderId,
        api_format: ApiFormat,
        model_id: impl Into<String>,
    ) -> Self {
        self.transform_context = Some((provider_id, api_format, model_id.into()));
        self
    }

    fn emit_usage(&self, usage: Usage, source: &str) {
        let total_input = usage.input_tokens();
        let cache_hit_rate = if total_input > 0 {
            (usage.cache_read_input_tokens as f64 / total_input as f64) * 100.0
        } else {
            0.0
        };

        info!(
            usage_source = source,
            uncached_input_tokens = usage.prompt_tokens,
            input_tokens = total_input,
            output_tokens = usage.completion_tokens,
            reasoning_tokens = usage.reasoning_tokens,
            cache_read_input_tokens = usage.cache_read_input_tokens,
            cache_creation_input_tokens = usage.cache_creation_input_tokens,
            logical_total_tokens = usage.logical_total_tokens(),
            provider_reported_total_tokens = usage.total_tokens,
            cache_hit_rate_percent = cache_hit_rate,
            "AI stream usage"
        );
        self.dispatch_part(StreamPart::Usage { usage });
    }

    /// Finish processing and ensure all buffers are flushed.
    pub async fn finish(&mut self) {
        let elapsed = self.stream_start.elapsed();
        info!(
            "SSE stream processor finishing: {:?} elapsed, {} events, {} bytes total",
            elapsed, self.event_count, self.bytes_received
        );
        self.stream_buffer.finish().await;
    }

    fn dispatch_part(&self, part: StreamPart) {
        let part = if let Some((provider_id, api_format, model_id)) = &self.transform_context {
            apply_stream_part_transform(part, *provider_id, *api_format, model_id)
        } else {
            part
        };
        let _ = self.tx.send(part);
    }
}
