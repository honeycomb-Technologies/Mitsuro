//! WebSocket terminal handler with PTY support.
//!
//! A terminal session necessarily runs with the server's OS privileges; that
//! is the single-host self-host trust model (one server user, one credential
//! store). Tenancy scoping therefore applies to registry accounting: the PTY
//! is registered under the authenticated principal's process bucket so
//! tenant-scoped process listing and termination cover it.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use base64ct::{Base64, Encoding};
use futures::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::auth::CurrentUser;
use crate::AppState;

const MAX_INPUT_SIZE: usize = 64 * 1024;
const MAX_TERMINAL_COLS: u16 = 500;
const MAX_TERMINAL_ROWS: u16 = 500;
const MAX_OUTPUT_BATCH_BYTES: usize = 64 * 1024;
const OUTPUT_COALESCE_WINDOW: Duration = Duration::from_millis(4);

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Hello {
        binary_output: Option<bool>,
        output_encoding: Option<OutputEncoding>,
    },
    Input {
        data: String,
    },
    InputBase64 {
        data: String,
    },
    Resize {
        cols: u16,
        rows: u16,
    },
    Ping,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OutputEncoding {
    Text,
    Binary,
    Base64,
}

impl OutputEncoding {
    const fn as_u8(self) -> u8 {
        match self {
            Self::Text => 0,
            Self::Binary => 1,
            Self::Base64 => 2,
        }
    }

    const fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Binary,
            2 => Self::Base64,
            _ => Self::Text,
        }
    }
}

pub async fn handler(
    ws: WebSocketUpgrade,
    user: Option<CurrentUser>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let user_id = user.and_then(|current_user| current_user.0.user_id);
    ws.on_upgrade(move |socket| handle_socket(socket, state, user_id))
}

async fn send_ws_error(sink: &mut futures::stream::SplitSink<WebSocket, Message>, msg: &str) {
    let error = serde_json::json!({ "type": "error", "error": msg });
    let _ = sink.send(Message::Text(error.to_string())).await;
}

fn clamp_terminal_size(cols: u16, rows: u16) -> PtySize {
    PtySize {
        rows: rows.clamp(1, MAX_TERMINAL_ROWS),
        cols: cols.clamp(1, MAX_TERMINAL_COLS),
        pixel_width: 0,
        pixel_height: 0,
    }
}

fn terminal_command(shell: &str, working_dir: &Path) -> CommandBuilder {
    let mut cmd = CommandBuilder::new(shell);
    cmd.cwd(working_dir);
    cmd.env("TERM", "xterm-256color");
    cmd.env("COLORTERM", "truecolor");
    cmd
}

async fn handle_socket(socket: WebSocket, state: AppState, user_id: Option<String>) {
    let (mut ws_sink, mut ws_stream) = socket.split();
    let pty_system = native_pty_system();

    let pair = match pty_system.openpty(clamp_terminal_size(80, 24)) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::error!("Failed to open PTY: {}", e);
            send_ws_error(&mut ws_sink, &format!("Failed to open PTY: {}", e)).await;
            return;
        }
    };

    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
    let cmd = terminal_command(&shell, &state.working_dir);

    let child = match pair.slave.spawn_command(cmd) {
        Ok(child) => child,
        Err(e) => {
            tracing::error!("Failed to spawn shell: {}", e);
            send_ws_error(&mut ws_sink, &format!("Failed to spawn shell: {}", e)).await;
            return;
        }
    };

    let process_id = uuid::Uuid::new_v4().to_string();
    let registered = if let Some(user_id) = user_id.as_deref() {
        state
            .process_registry
            .register_external_for_user(
                user_id,
                process_id.clone(),
                shell,
                Some("Terminal session".to_string()),
                child.process_id(),
                (*state.working_dir).clone(),
            )
            .await
    } else {
        state
            .process_registry
            .register_external(
                process_id.clone(),
                shell,
                Some("Terminal session".to_string()),
                child.process_id(),
                (*state.working_dir).clone(),
            )
            .await
    };
    if let Err(error) = registered {
        tracing::warn!(%error, "Terminal process rejected by registry");
        send_ws_error(&mut ws_sink, &format!("Terminal unavailable: {error}")).await;
        return;
    }

    let reader = match pair.master.try_clone_reader() {
        Ok(reader) => reader,
        Err(e) => {
            tracing::error!("Failed to clone PTY reader: {}", e);
            send_ws_error(&mut ws_sink, &format!("Failed to clone PTY reader: {}", e)).await;
            return;
        }
    };
    let writer = match pair.master.take_writer() {
        Ok(writer) => writer,
        Err(e) => {
            tracing::error!("Failed to take PTY writer: {}", e);
            send_ws_error(&mut ws_sink, &format!("Failed to take PTY writer: {}", e)).await;
            return;
        }
    };

    let (output_tx, mut output_rx) = mpsc::channel::<Vec<u8>>(256);

    let reader_handle = {
        let tx = output_tx.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut reader = reader;
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if tx.blocking_send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        })
    };

    let ws_sink = Arc::new(tokio::sync::Mutex::new(ws_sink));
    let output_encoding = Arc::new(AtomicU8::new(OutputEncoding::Text.as_u8()));
    let ws_sender_handle = {
        let ws_sink = Arc::clone(&ws_sink);
        let output_encoding = Arc::clone(&output_encoding);
        tokio::spawn(async move {
            let mut pending_output: Option<Vec<u8>> = None;
            loop {
                let mut batch = if let Some(pending) = pending_output.take() {
                    pending
                } else {
                    match output_rx.recv().await {
                        Some(data) => data,
                        None => break,
                    }
                };

                let deadline = tokio::time::Instant::now() + OUTPUT_COALESCE_WINDOW;
                while batch.len() < MAX_OUTPUT_BATCH_BYTES {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        break;
                    }

                    match tokio::time::timeout(deadline - now, output_rx.recv()).await {
                        Ok(Some(next)) => {
                            if batch.len() + next.len() > MAX_OUTPUT_BATCH_BYTES {
                                pending_output = Some(next);
                                break;
                            }
                            batch.extend_from_slice(&next);
                        }
                        Ok(None) | Err(_) => break,
                    }
                }

                let send_result =
                    match OutputEncoding::from_u8(output_encoding.load(Ordering::Relaxed)) {
                        OutputEncoding::Binary => {
                            ws_sink.lock().await.send(Message::Binary(batch)).await
                        }
                        OutputEncoding::Base64 => {
                            let msg = serde_json::json!({
                                "type": "output_base64",
                                "data": Base64::encode_string(&batch),
                            });
                            ws_sink
                                .lock()
                                .await
                                .send(Message::Text(msg.to_string()))
                                .await
                        }
                        OutputEncoding::Text => {
                            let msg = serde_json::json!({
                                "type": "output",
                                "data": String::from_utf8_lossy(&batch),
                            });
                            ws_sink
                                .lock()
                                .await
                                .send(Message::Text(msg.to_string()))
                                .await
                        }
                    };

                if send_result.is_err() {
                    break;
                }
            }
        })
    };

    let master = Arc::new(tokio::sync::Mutex::new(pair.master));
    {
        let master = Arc::clone(&master);
        let ws_sink = Arc::clone(&ws_sink);
        let output_encoding = Arc::clone(&output_encoding);
        let mut writer = writer;
        while let Some(Ok(msg)) = ws_stream.next().await {
            match msg {
                Message::Text(ref text) if text.len() > MAX_INPUT_SIZE => {
                    tracing::warn!(
                        "Rejected oversized WebSocket message ({} bytes)",
                        text.len()
                    );
                }
                Message::Text(text) => {
                    if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                        match client_msg {
                            ClientMessage::Hello {
                                binary_output: flag,
                                output_encoding: requested_encoding,
                            } => {
                                let encoding = requested_encoding.unwrap_or_else(|| {
                                    if flag.unwrap_or(false) {
                                        OutputEncoding::Binary
                                    } else {
                                        OutputEncoding::Text
                                    }
                                });
                                output_encoding.store(encoding.as_u8(), Ordering::Relaxed);
                            }
                            ClientMessage::Input { data } => {
                                if data.len() > MAX_INPUT_SIZE {
                                    tracing::warn!(
                                        "Rejected oversized terminal input ({} bytes)",
                                        data.len()
                                    );
                                    continue;
                                }
                                use std::io::Write;
                                let _ = writer.write_all(data.as_bytes());
                                let _ = writer.flush();
                            }
                            ClientMessage::InputBase64 { data } => {
                                if data.len() > MAX_INPUT_SIZE * 2 {
                                    tracing::warn!(
                                        "Rejected oversized base64 terminal input ({} bytes)",
                                        data.len()
                                    );
                                    continue;
                                }
                                let Ok(decoded) = Base64::decode_vec(&data) else {
                                    tracing::warn!("Rejected malformed base64 terminal input");
                                    continue;
                                };
                                if decoded.len() > MAX_INPUT_SIZE {
                                    tracing::warn!(
                                        "Rejected oversized decoded terminal input ({} bytes)",
                                        decoded.len()
                                    );
                                    continue;
                                }
                                use std::io::Write;
                                let _ = writer.write_all(&decoded);
                                let _ = writer.flush();
                            }
                            ClientMessage::Resize { cols, rows } => {
                                let m = master.lock().await;
                                let _ = m.resize(clamp_terminal_size(cols, rows));
                            }
                            ClientMessage::Ping => {
                                let mut sink = ws_sink.lock().await;
                                if sink
                                    .send(Message::Text(r#"{"type":"pong"}"#.to_string()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
                Message::Binary(data) => {
                    if data.len() > MAX_INPUT_SIZE {
                        tracing::warn!(
                            "Rejected oversized binary terminal input ({} bytes)",
                            data.len()
                        );
                        continue;
                    }
                    use std::io::Write;
                    let _ = writer.write_all(&data);
                    let _ = writer.flush();
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    }

    drop(output_tx);
    // PTY reader runs blocking I/O that cannot be gracefully cancelled, so abort is appropriate.
    reader_handle.abort();
    let _ = ws_sender_handle.await;

    {
        let m = master.lock().await;
        drop(m);
    }
    state.process_registry.unregister(&process_id).await;
    tracing::debug!(process_id = %process_id, "Terminal session closed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_terminal_size_bounds() {
        let clamped = clamp_terminal_size(0, 900);
        assert_eq!(clamped.cols, 1);
        assert_eq!(clamped.rows, MAX_TERMINAL_ROWS);
    }

    #[test]
    fn parses_hello_and_ping_messages() {
        let hello: ClientMessage =
            serde_json::from_str(r#"{"type":"hello","binary_output":true}"#).unwrap();
        assert!(matches!(
            hello,
            ClientMessage::Hello {
                binary_output: Some(true),
                output_encoding: None,
            }
        ));

        let base64_hello: ClientMessage =
            serde_json::from_str(r#"{"type":"hello","output_encoding":"base64"}"#).unwrap();
        assert!(matches!(
            base64_hello,
            ClientMessage::Hello {
                output_encoding: Some(OutputEncoding::Base64),
                ..
            }
        ));

        let ping: ClientMessage = serde_json::from_str(r#"{"type":"ping"}"#).unwrap();
        assert!(matches!(ping, ClientMessage::Ping));
    }

    #[test]
    fn terminal_command_advertises_color_capabilities() {
        let command = terminal_command("/bin/sh", Path::new("/tmp"));
        assert_eq!(
            command.get_env("TERM"),
            Some(std::ffi::OsStr::new("xterm-256color"))
        );
        assert_eq!(
            command.get_env("COLORTERM"),
            Some(std::ffi::OsStr::new("truecolor"))
        );
    }
}
