//! Stream buffer for smooth text streaming
//!
//! Breaks text into smaller chunks for smoother UI rendering

use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::ai::streaming::StreamPart;

/// A buffer that smoothly streams text by breaking it into smaller chunks
/// and sending them at regular intervals for smoother UI rendering
pub struct StreamBuffer {
    /// Internal buffer for accumulating text
    buffer: String,
    /// Ordered stream channel shared with finish/tool/usage events.
    ///
    /// Text used to pass through a second forwarding task. That allowed a
    /// finish event to overtake the final buffered text chunk and made a valid
    /// response look like provider content emitted after finish.
    tx: mpsc::UnboundedSender<StreamPart>,
    /// Target size for each chunk (in characters)
    chunk_size: usize,
    /// Maximum time to wait before flushing buffer
    flush_interval: Duration,
    /// Last time we flushed
    last_flush: Instant,
    /// Total characters processed
    total_chars: usize,
}

impl StreamBuffer {
    /// Create a new StreamBuffer with default settings
    pub fn new(tx: mpsc::UnboundedSender<StreamPart>) -> Self {
        Self {
            // Pre-allocate 256 bytes to reduce reallocations during streaming
            buffer: String::with_capacity(256),
            tx,
            chunk_size: 64, // Send 64 characters at a time for smooth streaming
            flush_interval: Duration::from_millis(16), // Flush every 16ms (~60fps)
            last_flush: Instant::now(),
            total_chars: 0,
        }
    }

    /// Process a chunk of text, breaking it into smaller pieces for smooth streaming
    pub async fn process_chunk(&mut self, chunk: String) {
        trace!("StreamBuffer: Processing chunk of {} bytes", chunk.len());

        // Bulk append to buffer
        self.buffer.push_str(&chunk);
        self.total_chars += chunk.chars().count();

        // Flush in chunks until buffer is below threshold
        while self.buffer.len() >= self.chunk_size {
            // Take chunk_size characters (respecting UTF-8 boundaries)
            let drain_to = self
                .buffer
                .char_indices()
                .nth(self.chunk_size)
                .map(|(i, _)| i)
                .unwrap_or(self.buffer.len());
            let to_send: String = self.buffer.drain(..drain_to).collect();
            let _ = self.tx.send(StreamPart::TextDelta { delta: to_send });
            self.last_flush = Instant::now();
        }

        // Check if we should flush remaining based on time
        if self.last_flush.elapsed() >= self.flush_interval && !self.buffer.is_empty() {
            self.flush().await;
        }
    }

    /// Flush the current buffer contents
    pub async fn flush(&mut self) {
        if !self.buffer.is_empty() {
            trace!("StreamBuffer: Flushing {} bytes", self.buffer.len());
            let content = std::mem::take(&mut self.buffer);
            let _ = self.tx.send(StreamPart::TextDelta { delta: content });
            self.last_flush = Instant::now();
        }
    }

    /// Force flush any remaining content
    pub async fn finish(&mut self) {
        self.flush().await;
        debug!(
            "StreamBuffer: Finished, processed {} total characters",
            self.total_chars
        );
    }
}
