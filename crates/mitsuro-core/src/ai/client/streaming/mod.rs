//! Streaming API calls
//!
//! Handles SSE streaming responses from different providers.

mod anthropic;
pub(crate) mod codex;
mod google;
mod openai;
mod request_options;
pub(crate) mod shared;

use anyhow::Result;
use std::fmt;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::info;

use super::config::CallOptions;
use super::core::AiClient;
use super::RemoteAttemptPolicy;
use crate::ai::retry::{
    is_retryable_interactive_stream_error, with_retry, IsRetryable, RetryConfig,
};
use crate::ai::streaming::StreamPart;
use crate::ai::types::ModelMessage;

#[derive(Debug)]
struct InteractiveStreamSetupError(anyhow::Error);

impl fmt::Display for InteractiveStreamSetupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for InteractiveStreamSetupError {}

impl IsRetryable for InteractiveStreamSetupError {
    fn is_retryable(&self) -> bool {
        is_retryable_interactive_stream_error(&self.0)
    }

    fn retry_after(&self) -> Option<std::time::Duration> {
        self.0.retry_after()
    }
}

impl AiClient {
    /// Call the API with streaming response
    pub async fn call_streaming(
        &self,
        messages: Vec<ModelMessage>,
        options: &CallOptions,
    ) -> Result<mpsc::UnboundedReceiver<StreamPart>> {
        self.call_streaming_with_attempt_policy(
            messages,
            options,
            RemoteAttemptPolicy::ConfiguredRetries,
        )
        .await
    }

    /// Call a streaming provider with an explicit remote-attempt policy.
    ///
    /// Hive Worker callers must pass `GovernedSingleAttempt` only after their
    /// exact durable provider-call slot has been Started.
    pub async fn call_streaming_with_attempt_policy(
        &self,
        messages: Vec<ModelMessage>,
        options: &CallOptions,
        attempt_policy: RemoteAttemptPolicy,
    ) -> Result<mpsc::UnboundedReceiver<StreamPart>> {
        let canonical_options = self.canonical_call_options(&self.config().model, options);

        if !attempt_policy.allows_retry() {
            return self
                .call_streaming_once(messages, &canonical_options, attempt_policy)
                .await;
        }

        // A receiver is returned only after the provider accepts the request,
        // so typed transient HTTP failures and definite connect failures can be
        // retried here without duplicating visible deltas or local tool work.
        let retry_config = RetryConfig::interactive_stream();
        with_retry(&retry_config, || async {
            self.call_streaming_once(messages.clone(), &canonical_options, attempt_policy)
                .await
                .map_err(InteractiveStreamSetupError)
        })
        .await
        .map_err(|error| error.0)
    }

    async fn call_streaming_once(
        &self,
        messages: Vec<ModelMessage>,
        canonical_options: &CallOptions,
        attempt_policy: RemoteAttemptPolicy,
    ) -> Result<mpsc::UnboundedReceiver<StreamPart>> {
        let call_start = Instant::now();
        info!("=== API CALL START ===");
        info!(
            "Model: {}, Messages: {}, Tools: {}, Thinking: {}, Format: {:?}",
            self.config().model,
            messages.len(),
            canonical_options
                .tools
                .as_ref()
                .map(|t| t.len())
                .unwrap_or(0),
            canonical_options.thinking.is_some(),
            self.config().api_format
        );

        if self.config().uses_openai_format() {
            return self
                .call_streaming_openai(messages, canonical_options, call_start, attempt_policy)
                .await;
        }

        if self.config().uses_google_format() {
            return self
                .call_streaming_google(messages, canonical_options, call_start)
                .await;
        }

        self.call_streaming_anthropic(messages, canonical_options, call_start)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Duration;

    use tiny_http::{Header, Response, Server};

    use super::*;
    use crate::ai::client::AiClientConfig;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::{AuthHeader, ProviderId};
    use crate::ai::types::{Content, Role};

    fn openai_test_client(url: String) -> AiClient {
        AiClient::new(
            AiClientConfig {
                model: "test-model".to_string(),
                max_tokens: 128,
                base_url: Some(url),
                auth_header: AuthHeader::Bearer,
                provider_id: ProviderId::OpenAI,
                api_format: ApiFormat::OpenAI,
                custom_headers: Default::default(),
            },
            "test-key".to_string(),
        )
    }

    fn user_message() -> ModelMessage {
        ModelMessage {
            role: Role::User,
            content: vec![Content::text("reply with ok".to_string())],
        }
    }

    #[tokio::test]
    async fn responses_parallel_policy_matches_dispatched_request_and_diagnostics() {
        for (provider, model) in [
            (ProviderId::Grok, "grok-4.6"),
            (ProviderId::OpenAI, "gpt-5.5"),
        ] {
            for requested in [None, Some(false), Some(true)] {
                let server = Server::http("127.0.0.1:0").unwrap();
                let url = format!("http://{}", server.server_addr());
                let (body_tx, body_rx) = std_mpsc::channel();
                let server_thread = thread::spawn(move || {
                    let mut request = server
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap()
                        .unwrap();
                    let mut body = String::new();
                    request.as_reader().read_to_string(&mut body).unwrap();
                    body_tx
                        .send(serde_json::from_str::<serde_json::Value>(&body).unwrap())
                        .unwrap();
                    let payload = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
                    request
                        .respond(Response::from_string(payload).with_header(
                            Header::from_bytes("Content-Type", "text/event-stream").unwrap(),
                        ))
                        .unwrap();
                });
                let client = AiClient::new(
                    AiClientConfig {
                        model: model.into(),
                        provider_id: provider,
                        api_format: ApiFormat::OpenAIResponses,
                        base_url: Some(url),
                        ..Default::default()
                    },
                    "fixture-key".into(),
                );
                let options = CallOptions {
                    parallel_tool_calls: requested,
                    tools: Some(vec![crate::ai::types::AiTool {
                        name: "read".into(),
                        description: "Read a fixture".into(),
                        input_schema: serde_json::json!({"type":"object","properties":{}}),
                        prompt: None,
                    }]),
                    ..Default::default()
                };
                let messages = vec![user_message()];
                let diagnostic = client.request_diagnostics(&messages, &options);
                let mut stream = tokio::time::timeout(
                    Duration::from_secs(5),
                    client.call_streaming_with_attempt_policy(
                        messages,
                        &options,
                        RemoteAttemptPolicy::GovernedSingleAttempt,
                    ),
                )
                .await
                .unwrap()
                .unwrap();
                while tokio::time::timeout(Duration::from_secs(5), stream.recv())
                    .await
                    .unwrap()
                    .is_some()
                {}
                let body = body_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let expected = requested.unwrap_or(true);
                assert_eq!(body["parallel_tool_calls"], expected);
                assert_eq!(diagnostic.effective_request.parallel_tool_calls, expected);
                assert!(body.get("tool_choice").is_none());
                server_thread.join().unwrap();
            }
        }
    }

    fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout should be set");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let mut expected_len = None;

        loop {
            let read = stream
                .read(&mut buffer)
                .expect("request should be readable");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);

            if expected_len.is_none() {
                if let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or_default();
                    expected_len = Some(header_end + 4 + content_length);
                }
            }

            if expected_len.is_some_and(|length| request.len() >= length) {
                break;
            }
        }

        request
    }

    #[tokio::test]
    async fn transient_http_failure_retries_before_exposing_stream() {
        let server = Server::http("127.0.0.1:0").expect("test server should bind");
        let url = format!("http://{}", server.server_addr());
        let (body_tx, body_rx) = std_mpsc::channel();
        let server_thread = thread::spawn(move || {
            for attempt in 0..2 {
                let mut request = server.recv().expect("request should arrive");
                let mut body = String::new();
                request
                    .as_reader()
                    .read_to_string(&mut body)
                    .expect("request body should be readable");
                body_tx.send(body).expect("body should be recorded");

                if attempt == 0 {
                    let response = Response::from_string("temporarily at capacity")
                        .with_status_code(429)
                        .with_header(
                            Header::from_bytes("Retry-After", "0")
                                .expect("retry header should be valid"),
                        );
                    request.respond(response).expect("429 should be sent");
                } else {
                    let response = Response::from_string(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n\
                         data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
                         data: [DONE]\n\n",
                    )
                    .with_header(
                        Header::from_bytes("Content-Type", "text/event-stream")
                            .expect("content type should be valid"),
                    );
                    request.respond(response).expect("SSE should be sent");
                }
            }
        });

        let client = openai_test_client(url);
        let mut stream = client
            .call_streaming(vec![user_message()], &CallOptions::default())
            .await
            .expect("second request should open a stream");

        let mut text = String::new();
        let mut finished = false;
        while let Some(part) = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("stream should not stall")
        {
            match part {
                StreamPart::TextDelta { delta } => text.push_str(&delta),
                StreamPart::Finish { .. } => {
                    finished = true;
                    break;
                }
                StreamPart::Error { error } => panic!("unexpected stream error: {error}"),
                _ => {}
            }
        }

        server_thread.join().expect("server thread should finish");
        let first_body = body_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first body should be recorded");
        let second_body = body_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second body should be recorded");
        assert_eq!(first_body, second_body);
        assert_eq!(text, "ok");
        assert!(finished);
    }

    #[tokio::test]
    async fn dropped_request_connection_retries_before_exposing_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("address should resolve")
        );
        let (request_tx, request_rx) = std_mpsc::channel();
        let server_thread = thread::spawn(move || {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().expect("connection should arrive");
                let request = read_http_request(&mut stream);
                request_tx
                    .send(request)
                    .expect("request should be recorded");

                if attempt == 0 {
                    // Simulate an edge/proxy closing the connection after the
                    // request was dispatched but before any response arrived.
                    continue;
                }

                let payload = concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                );
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    payload.len(),
                    payload
                )
                .expect("response should be written");
                stream.flush().expect("response should be flushed");
            }
        });

        let client = openai_test_client(url);
        let mut stream = client
            .call_streaming(vec![user_message()], &CallOptions::default())
            .await
            .expect("dropped setup request should be retried");

        let mut text = String::new();
        let mut finished = false;
        while let Some(part) = tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("stream should not stall")
        {
            match part {
                StreamPart::TextDelta { delta } => text.push_str(&delta),
                StreamPart::Finish { .. } => {
                    finished = true;
                    break;
                }
                StreamPart::Error { error } => panic!("unexpected stream error: {error}"),
                _ => {}
            }
        }

        server_thread.join().expect("server thread should finish");
        let first_request = request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first request should be recorded");
        let second_request = request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second request should be recorded");
        let first_body = first_request
            .split(|byte| *byte == b'\n')
            .next_back()
            .expect("request should contain a body");
        let second_body = second_request
            .split(|byte| *byte == b'\n')
            .next_back()
            .expect("request should contain a body");
        assert_eq!(first_body, second_body);
        assert_eq!(text, "ok");
        assert!(finished);
    }

    #[tokio::test]
    async fn governed_ambiguous_setup_failure_uses_one_remote_attempt() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("address should resolve")
        );
        let (request_tx, request_rx) = std_mpsc::channel();
        let server_thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("one connection should arrive");
            let request = read_http_request(&mut stream);
            request_tx
                .send(request)
                .expect("request should be recorded");
            // Closing after the complete request is intentionally ambiguous:
            // the peer may have accepted work before its response disappeared.
            drop(stream);

            listener
                .set_nonblocking(true)
                .expect("listener should become nonblocking");
            let deadline = std::time::Instant::now() + Duration::from_millis(350);
            while std::time::Instant::now() < deadline {
                match listener.accept() {
                    Ok(_) => panic!("governed call attempted a second remote request"),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("unexpected listener error: {error}"),
                }
            }
        });

        let client = openai_test_client(url);
        client
            .call_streaming_with_attempt_policy(
                vec![user_message()],
                &CallOptions::default(),
                RemoteAttemptPolicy::GovernedSingleAttempt,
            )
            .await
            .expect_err("ambiguous governed setup failure must surface without retry");

        server_thread.join().expect("server thread should finish");
        request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("exactly one request should be recorded");
        assert!(request_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn payment_failure_is_terminal_without_retry() {
        let server = Server::http("127.0.0.1:0").expect("test server should bind");
        let url = format!("http://{}", server.server_addr());
        let (request_tx, request_rx) = std_mpsc::channel();
        let server_thread = thread::spawn(move || {
            let request = server.recv().expect("request should arrive");
            request_tx.send(()).expect("request should be counted");
            request
                .respond(Response::from_string("limit reached").with_status_code(402))
                .expect("402 should be sent");
        });

        let client = openai_test_client(url);
        let error = client
            .call_streaming(vec![user_message()], &CallOptions::default())
            .await
            .expect_err("payment failure must be terminal");

        server_thread.join().expect("server thread should finish");
        request_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("one request should be recorded");
        assert!(request_rx.try_recv().is_err());
        assert!(error.to_string().contains("402 Payment Required"));
    }
}
