use std::pin::Pin;

use anyhow::{Context as _, Result};
use futures_core::Stream;
use futures_util::StreamExt as _;
use reqwest::Response;
use serde::Deserialize;
use serde_json::Value;

use crate::ChatStreamEvent;

#[derive(Clone, Debug, PartialEq)]
pub struct AgentStreamEnvelope {
    pub generation: u64,
    pub sequence: u64,
    pub event: ChatStreamEvent,
}

pub type AgentEventStream =
    Pin<Box<dyn Stream<Item = Result<AgentStreamEnvelope>> + Send + 'static>>;

#[derive(Deserialize)]
struct WireEnvelope {
    generation: u64,
    sequence: u64,
    event: Value,
}

pub fn agent_stream_from_response(response: Response) -> AgentEventStream {
    let mut chunks = response.bytes_stream();
    Box::pin(async_stream::try_stream! {
        let mut line_buffer = Vec::<u8>::new();
        let mut data = String::new();

        while let Some(chunk) = chunks.next().await {
            line_buffer.extend_from_slice(&chunk.context("reading Agent observer SSE chunk")?);
            while let Some(line_end) = line_buffer.iter().position(|byte| *byte == b'\n') {
                let line = line_buffer.drain(..=line_end).collect::<Vec<_>>();
                let line = decode_line(&line)?;
                if let Some(envelope) = push_line(&mut data, &line)? {
                    yield envelope;
                }
            }
        }
        if !line_buffer.is_empty() {
            let line = decode_line(&line_buffer)?;
            if let Some(envelope) = push_line(&mut data, &line)? {
                yield envelope;
            }
        }
        if let Some(envelope) = dispatch(&mut data)? {
            yield envelope;
        }
    })
}

fn push_line(data: &mut String, line: &str) -> Result<Option<AgentStreamEnvelope>> {
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

fn dispatch(data: &mut String) -> Result<Option<AgentStreamEnvelope>> {
    let data = std::mem::take(data);
    let data = data.trim();
    if data.is_empty() || data == "[DONE]" {
        return Ok(None);
    }
    let wire: WireEnvelope =
        serde_json::from_str(data).context("parsing Agent observer SSE envelope")?;
    Ok(Some(AgentStreamEnvelope {
        generation: wire.generation,
        sequence: wire.sequence,
        event: ChatStreamEvent::from_json_value(wire.event),
    }))
}

fn decode_line(line: &[u8]) -> Result<String> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    Ok(std::str::from_utf8(line)
        .context("decoding Agent observer SSE line as UTF-8")?
        .to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_generation_sequence_and_nested_agent_event() {
        let mut data = String::new();
        assert!(push_line(
            &mut data,
            r#"data: {"generation":7,"sequence":9,"event":{"type":"text_delta","delta":"hi"}}"#
        )
        .expect("data line")
        .is_none());
        let event = push_line(&mut data, "").expect("dispatch").expect("event");
        assert_eq!((event.generation, event.sequence), (7, 9));
        assert_eq!(
            event.event,
            ChatStreamEvent::TextDelta { delta: "hi".into() }
        );
    }
}
