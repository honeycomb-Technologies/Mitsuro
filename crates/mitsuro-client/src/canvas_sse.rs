use std::pin::Pin;

use anyhow::{Context as _, Result};
use futures_core::Stream;
use futures_util::StreamExt as _;
use reqwest::Response;

use crate::CanvasStreamEnvelope;

pub type CanvasEventStream =
    Pin<Box<dyn Stream<Item = Result<CanvasStreamEnvelope>> + Send + 'static>>;

pub fn canvas_stream_from_response(response: Response) -> CanvasEventStream {
    let mut chunks = response.bytes_stream();
    Box::pin(async_stream::try_stream! {
        let mut line_buffer = Vec::<u8>::new();
        let mut data = String::new();

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.context("reading canvas SSE response chunk")?;
            line_buffer.extend_from_slice(&chunk);
            while let Some(line_end) = line_buffer.iter().position(|byte| *byte == b'\n') {
                let line = line_buffer.drain(..=line_end).collect::<Vec<_>>();
                let line = std::str::from_utf8(line.strip_suffix(b"\n").unwrap_or(&line))
                    .context("decoding canvas SSE line as UTF-8")?;
                if let Some(envelope) = push_line(&mut data, line)? {
                    yield envelope;
                }
            }
        }

        if !line_buffer.is_empty() {
            let line = std::str::from_utf8(&line_buffer)
                .context("decoding canvas SSE tail as UTF-8")?;
            if let Some(envelope) = push_line(&mut data, line)? {
                yield envelope;
            }
        }
        if let Some(envelope) = dispatch(&mut data)? {
            yield envelope;
        }
    })
}

fn push_line(data: &mut String, line: &str) -> Result<Option<CanvasStreamEnvelope>> {
    let line = line.strip_suffix('\r').unwrap_or(line);
    if line.is_empty() {
        return dispatch(data);
    }
    if line.starts_with(':') {
        return Ok(None);
    }
    let Some(value) = line.strip_prefix("data:") else {
        return Ok(None);
    };
    if !data.is_empty() {
        data.push('\n');
    }
    data.push_str(value.strip_prefix(' ').unwrap_or(value));
    Ok(None)
}

fn dispatch(data: &mut String) -> Result<Option<CanvasStreamEnvelope>> {
    let payload = std::mem::take(data);
    if payload.trim().is_empty() {
        return Ok(None);
    }
    serde_json::from_str(payload.trim())
        .context("parsing canvas SSE envelope")
        .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_canvas_snapshot_envelope() {
        let mut data = String::new();
        let payload = r#"data: {"type":"snapshot","canvas":{"id":"c","owner_user_id":null,"project_dir":"/tmp","surface":"code","name":"Main","schema_version":1,"revision":0,"shared_theme_id":"graphite","state":{"schema_version":1,"nodes":[]},"created_at":"now","updated_at":"now","archived_at":null}}"#;
        assert!(push_line(&mut data, payload).expect("data line").is_none());
        assert!(matches!(
            push_line(&mut data, "").expect("dispatch"),
            Some(CanvasStreamEnvelope::Snapshot { .. })
        ));
    }

    #[test]
    fn decodes_collaboration_snapshot_and_event_envelopes() {
        let mut data = String::new();
        let snapshot = r#"data: {"type":"collaboration_snapshot","collaboration":{"canvas_id":"c","sequence":2,"clients":[{"client_id":"other","display_name":"Other","cursor":{"x":-3.0,"y":4.0},"last_seen_at":"now","stale":false}],"geometry_previews":[]}}"#;
        assert!(push_line(&mut data, snapshot)
            .expect("snapshot line")
            .is_none());
        assert!(matches!(
            push_line(&mut data, "").expect("snapshot dispatch"),
            Some(CanvasStreamEnvelope::CollaborationSnapshot { .. })
        ));

        let events = r#"data: {"type":"collaboration_events","page":{"events":[{"type":"presence_stale","canvas_id":"c","sequence":3,"client_id":"other"}],"earliest_sequence":1,"latest_sequence":3,"gap":false}}"#;
        assert!(push_line(&mut data, events).expect("event line").is_none());
        assert!(matches!(
            push_line(&mut data, "").expect("event dispatch"),
            Some(CanvasStreamEnvelope::CollaborationEvents { .. })
        ));
    }
}
