//! Native server-backed terminal surface for the spatial Canvas.
//!
//! Mitsuro owns the PTY on the server. This module owns only an
//! `alacritty_terminal` emulator, a GPUI renderer, and bounded WebSocket
//! transport. Keeping the live `Entity<TerminalSurface>` transferable means
//! Canvas and Stage never mount a second emulator or terminal subscription.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alacritty_terminal::event::{Event as AlacrittyEvent, EventListener, WindowSize};
use alacritty_terminal::grid::{Dimensions, Scroll};
use alacritty_terminal::index::{Column, Line, Point, Side};
use alacritty_terminal::selection::{Selection, SelectionType};
use alacritty_terminal::term::cell::Flags;
use alacritty_terminal::term::{Config as TerminalConfig, Osc52, TermMode};
use alacritty_terminal::vte::ansi::{self, Color, NamedColor, Rgb};
use alacritty_terminal::Term;
use futures_util::{SinkExt as _, StreamExt as _};
use gpui::prelude::FluentBuilder as _;
use gpui::{
    div, px, rgb, Bounds, ClipboardItem, Context, FocusHandle, Focusable, FontWeight,
    InteractiveElement as _, IntoElement, KeyDownEvent, Modifiers, MouseButton, MouseDownEvent,
    MouseMoveEvent, MouseUpEvent, ParentElement as _, Pixels, Render, ScrollWheelEvent,
    SharedString, Styled as _, Task, Timer, Window,
};
use gpui_component::button::Button;
use gpui_component::Disableable as _;
use mitsuro_client::{
    ControlLease, ControlLeaseSummary, MitsuroClient, TakeTerminalControlRequest,
    TerminalDescriptor, TerminalSize, TerminalStatus, TerminalTransportCommand,
    TerminalTransportEvent,
};
use tokio::runtime::Runtime;
use tokio_tungstenite::tungstenite::Message;

use crate::theme::CanvasPalette;

const TRANSPORT_EVENT_CAPACITY: usize = 256;
const TRANSPORT_COMMAND_CAPACITY: usize = 128;
const EMULATOR_EVENT_CAPACITY: usize = 64;
const MAX_EVENTS_PER_FRAME: usize = 96;
const MAX_OUTPUT_BYTES_PER_FRAME: usize = 256 * 1024;
const FRAME_COALESCE_DELAY: Duration = Duration::from_millis(8);
const RECONNECT_MIN_DELAY: Duration = Duration::from_millis(250);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(2);
const LEASE_KEEPALIVE: Duration = Duration::from_secs(5);
const CELL_WIDTH: f32 = 8.2;
const LINE_HEIGHT: f32 = 18.0;
const TERMINAL_FONT_SIZE: f32 = 13.0;
const SCROLLBACK_LINES: usize = 10_000;
const MAX_TERMINAL_INPUT_BYTES: usize = 64 * 1024;
const BRACKETED_PASTE_WRAPPER_BYTES: usize = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalSurfaceConnection {
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
    Exited,
}

/// A compact, renderer-neutral status snapshot for Pulse and Stage chrome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalSurfaceSnapshot {
    pub terminal_id: String,
    pub title: String,
    pub connection: TerminalSurfaceConnection,
    pub controller: Option<String>,
    pub has_control: bool,
    pub next_offset: u64,
    pub replay_gap: Option<(u64, u64)>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EmulatorSize {
    cols: usize,
    rows: usize,
}

impl EmulatorSize {
    fn from_wire(size: TerminalSize) -> Self {
        Self {
            cols: usize::from(size.cols.max(2)),
            rows: usize::from(size.rows.max(1)),
        }
    }
}

impl Dimensions for EmulatorSize {
    fn total_lines(&self) -> usize {
        self.rows
    }

    fn screen_lines(&self) -> usize {
        self.rows
    }

    fn columns(&self) -> usize {
        self.cols
    }
}

enum EmulatorSideEffect {
    Title(String),
    PtyWrite(Vec<u8>),
    ColorRequest(usize, Arc<dyn Fn(Rgb) -> String + Sync + Send + 'static>),
    TextAreaSize(Arc<dyn Fn(WindowSize) -> String + Sync + Send + 'static>),
    Bell,
}

#[derive(Clone)]
struct EmulatorEventProxy {
    pending: Arc<Mutex<VecDeque<EmulatorSideEffect>>>,
}

impl EventListener for EmulatorEventProxy {
    fn send_event(&self, event: AlacrittyEvent) {
        let side_effect = match event {
            AlacrittyEvent::Title(title) => Some(EmulatorSideEffect::Title(title)),
            AlacrittyEvent::ResetTitle => Some(EmulatorSideEffect::Title(String::new())),
            AlacrittyEvent::PtyWrite(text) => Some(EmulatorSideEffect::PtyWrite(text.into_bytes())),
            AlacrittyEvent::ColorRequest(index, formatter) => {
                Some(EmulatorSideEffect::ColorRequest(index, formatter))
            }
            AlacrittyEvent::TextAreaSizeRequest(formatter) => {
                Some(EmulatorSideEffect::TextAreaSize(formatter))
            }
            AlacrittyEvent::Bell => Some(EmulatorSideEffect::Bell),
            AlacrittyEvent::ClipboardStore(_, _)
            | AlacrittyEvent::ClipboardLoad(_, _)
            | AlacrittyEvent::MouseCursorDirty
            | AlacrittyEvent::CursorBlinkingChange
            | AlacrittyEvent::Wakeup
            | AlacrittyEvent::Exit
            | AlacrittyEvent::ChildExit(_) => None,
        };
        let Some(side_effect) = side_effect else {
            return;
        };
        if let Ok(mut pending) = self.pending.lock() {
            if pending.len() == EMULATOR_EVENT_CAPACITY {
                // Window-title and bell events are expendable. Never grow an
                // unbounded side-effect queue when a hostile PTY floods OSC.
                if matches!(
                    side_effect,
                    EmulatorSideEffect::Title(_) | EmulatorSideEffect::Bell
                ) {
                    return;
                }
                pending.pop_front();
            }
            pending.push_back(side_effect);
        }
    }
}

struct TerminalEmulator {
    term: Term<EmulatorEventProxy>,
    parser: ansi::Processor,
    side_effects: Arc<Mutex<VecDeque<EmulatorSideEffect>>>,
}

impl TerminalEmulator {
    fn new(size: TerminalSize) -> Self {
        let side_effects = Arc::new(Mutex::new(VecDeque::new()));
        let listener = EmulatorEventProxy {
            pending: Arc::clone(&side_effects),
        };
        let config = TerminalConfig {
            scrolling_history: SCROLLBACK_LINES,
            osc52: Osc52::Disabled,
            ..TerminalConfig::default()
        };
        let term = Term::new(config, &EmulatorSize::from_wire(size), listener);
        Self {
            term,
            parser: ansi::Processor::new(),
            side_effects,
        }
    }

    fn advance(&mut self, bytes: &[u8]) {
        self.parser.advance(&mut self.term, bytes);
    }

    fn resize(&mut self, size: TerminalSize) {
        self.term.resize(EmulatorSize::from_wire(size));
    }

    fn take_side_effects(&self) -> VecDeque<EmulatorSideEffect> {
        self.side_effects
            .lock()
            .map(|mut effects| std::mem::take(&mut *effects))
            .unwrap_or_default()
    }
}

enum SurfaceEvent {
    TransportConnected,
    TransportReconnecting(String),
    TransportClosed(String),
    Wire(TerminalTransportEvent),
    Lease(Result<ControlLease, String>),
    Viewport(TerminalSize),
}

enum TransportCommand {
    Wire(TerminalTransportCommand),
    Shutdown,
}

struct TerminalTransport {
    commands: flume::Sender<TransportCommand>,
    task: tokio::task::JoinHandle<()>,
}

impl TerminalTransport {
    fn spawn(
        client: MitsuroClient,
        runtime: &Runtime,
        terminal_id: String,
        initial_offset: u64,
        lease_token: Arc<Mutex<Option<String>>>,
        events: flume::Sender<SurfaceEvent>,
    ) -> Self {
        let (commands, command_rx) = flume::bounded(TRANSPORT_COMMAND_CAPACITY);
        let task = runtime.spawn(run_transport(
            client,
            terminal_id,
            initial_offset,
            lease_token,
            command_rx,
            events,
        ));
        Self { commands, task }
    }

    fn send(&self, command: TerminalTransportCommand) -> Result<(), String> {
        self.commands
            .try_send(TransportCommand::Wire(command))
            .map_err(|_| "Terminal input queue is busy; try again".to_owned())
    }
}

impl Drop for TerminalTransport {
    fn drop(&mut self) {
        let _ = self
            .commands
            .try_send(TransportCommand::Wire(TerminalTransportCommand::Detach));
        let _ = self.commands.try_send(TransportCommand::Shutdown);
        // The server owns the PTY and keeps it alive. Aborting only tears down
        // this socket task, so no local reader thread or child can leak.
        self.task.abort();
    }
}

async fn run_transport(
    client: MitsuroClient,
    terminal_id: String,
    mut cursor: u64,
    lease_token: Arc<Mutex<Option<String>>>,
    commands: flume::Receiver<TransportCommand>,
    events: flume::Sender<SurfaceEvent>,
) {
    let mut reconnect_delay = RECONNECT_MIN_DELAY;
    loop {
        let stream_url = match client.terminal_stream_url(&terminal_id, Some(cursor)) {
            Ok(url) => url,
            Err(error) => {
                let _ = events
                    .send_async(SurfaceEvent::TransportClosed(format!("{error:#}")))
                    .await;
                return;
            }
        };
        let request = match client.websocket_request(&stream_url) {
            Ok(request) => request,
            Err(error) => {
                let _ = events
                    .send_async(SurfaceEvent::TransportClosed(format!("{error:#}")))
                    .await;
                return;
            }
        };

        let socket = match tokio_tungstenite::connect_async(request).await {
            Ok((socket, _)) => socket,
            Err(error) => {
                if events
                    .send_async(SurfaceEvent::TransportReconnecting(format!(
                        "Terminal stream unavailable: {error}"
                    )))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_DELAY);
                continue;
            }
        };

        reconnect_delay = RECONNECT_MIN_DELAY;
        if events
            .send_async(SurfaceEvent::TransportConnected)
            .await
            .is_err()
        {
            return;
        }
        let (mut sink, mut source) = socket.split();
        let mut keepalive = tokio::time::interval(LEASE_KEEPALIVE);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut should_reconnect = true;

        loop {
            tokio::select! {
                incoming = source.next() => {
                    let Some(incoming) = incoming else { break };
                    let message = match incoming {
                        Ok(message) => message,
                        Err(error) => {
                            let _ = events.send_async(SurfaceEvent::TransportReconnecting(
                                format!("Terminal stream disconnected: {error}")
                            )).await;
                            break;
                        }
                    };
                    match message {
                        Message::Text(text) => {
                            match serde_json::from_str::<TerminalTransportEvent>(&text) {
                                Ok(event) => {
                                    match &event {
                                        TerminalTransportEvent::Output { offset, bytes } => {
                                            cursor = cursor.max(offset.saturating_add(bytes.len() as u64));
                                        }
                                        TerminalTransportEvent::Gap { available_from, .. } => {
                                            cursor = cursor.max(*available_from);
                                        }
                                        TerminalTransportEvent::Exited { next_offset, .. } => {
                                            cursor = cursor.max(*next_offset);
                                            should_reconnect = false;
                                        }
                                        _ => {}
                                    }
                                    let terminal = matches!(event, TerminalTransportEvent::Exited { .. });
                                    if events.send_async(SurfaceEvent::Wire(event)).await.is_err() {
                                        return;
                                    }
                                    if terminal { break; }
                                }
                                Err(error) => {
                                    if events.send_async(SurfaceEvent::TransportReconnecting(
                                        format!("Malformed terminal stream event: {error}")
                                    )).await.is_err() {
                                        return;
                                    }
                                    break;
                                }
                            }
                        }
                        Message::Ping(payload) => {
                            if sink.send(Message::Pong(payload)).await.is_err() { break; }
                        }
                        Message::Close(_) => break,
                        Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                    }
                }
                command = commands.recv_async() => {
                    let Ok(command) = command else { return };
                    match command {
                        TransportCommand::Wire(command) => {
                            let detach = matches!(command, TerminalTransportCommand::Detach);
                            let payload = match serde_json::to_string(&command) {
                                Ok(payload) => payload,
                                Err(_) => continue,
                            };
                            if sink.send(Message::Text(payload)).await.is_err() { break; }
                            if detach {
                                should_reconnect = false;
                                break;
                            }
                        }
                        TransportCommand::Shutdown => return,
                    }
                }
                _ = keepalive.tick() => {
                    let token = lease_token.lock().ok().and_then(|token| token.clone());
                    let command = TerminalTransportCommand::Ping { lease_token: token };
                    if let Ok(payload) = serde_json::to_string(&command) {
                        if sink.send(Message::Text(payload)).await.is_err() { break; }
                    }
                }
            }
        }

        if !should_reconnect {
            return;
        }
        if events
            .send_async(SurfaceEvent::TransportReconnecting(
                "Reattaching to the same server terminal…".to_owned(),
            ))
            .await
            .is_err()
        {
            return;
        }
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

/// The single live GPUI entity mounted either in Canvas or Stage.
pub struct TerminalSurface {
    client: MitsuroClient,
    runtime: Arc<Runtime>,
    palette: CanvasPalette,
    descriptor: TerminalDescriptor,
    client_id: String,
    title: String,
    focus: FocusHandle,
    connection: TerminalSurfaceConnection,
    controller: Option<ControlLeaseSummary>,
    lease: Option<ControlLease>,
    lease_token: Arc<Mutex<Option<String>>>,
    emulator: TerminalEmulator,
    expected_offset: u64,
    replay_gap: Option<(u64, u64)>,
    exit_code: Option<i32>,
    error: Option<String>,
    bell: bool,
    confirm_terminate: bool,
    transport: TerminalTransport,
    event_tx: flume::Sender<SurfaceEvent>,
    _event_listener: Task<()>,
    body_bounds: Arc<Mutex<Option<Bounds<Pixels>>>>,
    last_layout: Arc<Mutex<TerminalSize>>,
}

impl TerminalSurface {
    pub fn new(
        client: MitsuroClient,
        runtime: Arc<Runtime>,
        descriptor: TerminalDescriptor,
        client_id: impl Into<String>,
        palette: CanvasPalette,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let client_id = client_id.into();
        let focus = cx.focus_handle();
        let (event_tx, event_rx) = flume::bounded(TRANSPORT_EVENT_CAPACITY);
        let listener = cx.spawn(async move |this, cx| {
            while let Ok(first) = event_rx.recv_async().await {
                Timer::after(FRAME_COALESCE_DELAY).await;
                let mut events = Vec::with_capacity(32);
                let mut output_bytes = surface_event_bytes(&first);
                events.push(first);
                while events.len() < MAX_EVENTS_PER_FRAME
                    && output_bytes < MAX_OUTPUT_BYTES_PER_FRAME
                {
                    let Ok(event) = event_rx.try_recv() else {
                        break;
                    };
                    output_bytes = output_bytes.saturating_add(surface_event_bytes(&event));
                    events.push(event);
                }
                if this
                    .update(cx, |surface, cx| {
                        for event in events {
                            surface.reduce(event, cx);
                        }
                        surface.drain_emulator_side_effects(cx);
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        let lease_token = Arc::new(Mutex::new(None));
        let initial_offset = descriptor.available_from;
        let transport = TerminalTransport::spawn(
            client.clone(),
            runtime.as_ref(),
            descriptor.id.clone(),
            initial_offset,
            Arc::clone(&lease_token),
            event_tx.clone(),
        );
        let controller = descriptor.controller.clone();
        let should_take_control = controller
            .as_ref()
            .is_none_or(|controller| controller.client_id == client_id);
        let size = descriptor.size;
        let mut surface = Self {
            client,
            runtime,
            palette,
            title: terminal_title(&descriptor),
            descriptor,
            client_id,
            focus,
            connection: TerminalSurfaceConnection::Connecting,
            controller,
            lease: None,
            lease_token,
            emulator: TerminalEmulator::new(size),
            expected_offset: initial_offset,
            replay_gap: None,
            exit_code: None,
            error: None,
            bell: false,
            confirm_terminate: false,
            transport,
            event_tx,
            _event_listener: listener,
            body_bounds: Arc::new(Mutex::new(None)),
            last_layout: Arc::new(Mutex::new(size)),
        };
        if should_take_control && surface.descriptor.status == TerminalStatus::Running {
            surface.take_control(cx);
        }
        surface
    }

    pub fn snapshot(&self) -> TerminalSurfaceSnapshot {
        TerminalSurfaceSnapshot {
            terminal_id: self.descriptor.id.clone(),
            title: self.title.clone(),
            connection: self.connection,
            controller: self
                .controller
                .as_ref()
                .map(|lease| lease.client_id.clone()),
            has_control: self.has_control(),
            next_offset: self.expected_offset,
            replay_gap: self.replay_gap,
            exit_code: self.exit_code,
            error: self.error.clone(),
        }
    }

    pub fn set_palette(&mut self, palette: CanvasPalette, cx: &mut Context<Self>) {
        if self.palette != palette {
            self.palette = palette;
            cx.notify();
        }
    }

    pub fn compact_summary(&self) -> SharedString {
        let snapshot = self.snapshot();
        if let Some(error) = snapshot.error.as_ref() {
            return error.clone().into();
        }
        match snapshot.connection {
            TerminalSurfaceConnection::Connecting => "Connecting…".into(),
            TerminalSurfaceConnection::Connected if self.has_control() => "Live terminal".into(),
            TerminalSurfaceConnection::Connected => "Observing terminal".into(),
            TerminalSurfaceConnection::Reconnecting => "Reattaching…".into(),
            TerminalSurfaceConnection::Disconnected => "Disconnected".into(),
            TerminalSurfaceConnection::Exited => match snapshot.exit_code {
                Some(code) => format!("Exited with {code}").into(),
                None => "Terminal ended".into(),
            },
        }
    }

    pub fn needs_attention(&self) -> bool {
        self.replay_gap.is_some()
            || self.error.is_some()
            || matches!(
                self.connection,
                TerminalSurfaceConnection::Disconnected | TerminalSurfaceConnection::Exited
            )
    }

    pub fn is_ended(&self) -> bool {
        self.connection == TerminalSurfaceConnection::Exited
            || self.descriptor.status == TerminalStatus::Exited
    }

    pub fn has_control(&self) -> bool {
        self.lease
            .as_ref()
            .is_some_and(|lease| lease.client_id == self.client_id)
    }

    pub fn take_control(&mut self, cx: &mut Context<Self>) {
        self.confirm_terminate = false;
        let client = self.client.clone();
        let terminal_id = self.descriptor.id.clone();
        let client_id = self.client_id.clone();
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            let result = client
                .take_terminal_control(&terminal_id, &TakeTerminalControlRequest { client_id })
                .await
                .map(|response| response.lease)
                .map_err(|error| format!("Could not take terminal control: {error:#}"));
            let _ = tx.send_async(SurfaceEvent::Lease(result)).await;
        });
        self.error = None;
        cx.notify();
    }

    /// Request process termination after the caller has presented its own
    /// confirmation UI. Closing a Canvas node should not call this method.
    pub fn request_terminate(&mut self, cx: &mut Context<Self>) {
        self.confirm_terminate = false;
        let Some(token) = self.current_lease_token() else {
            self.error = Some("Take control before ending this terminal".to_owned());
            cx.notify();
            return;
        };
        self.send_command(
            TerminalTransportCommand::Terminate { lease_token: token },
            cx,
        );
        self.error = Some("Ending terminal process…".to_owned());
        cx.notify();
    }

    fn confirm_or_terminate(&mut self, cx: &mut Context<Self>) {
        if self.confirm_terminate {
            self.request_terminate(cx);
        } else {
            self.confirm_terminate = true;
            self.error = Some(
                "End terminates the server process. Press Confirm end to continue.".to_owned(),
            );
            cx.notify();
        }
    }

    pub fn paste_text(&mut self, text: &str, cx: &mut Context<Self>) {
        if text.is_empty() {
            return;
        }
        let mode = *self.emulator.term.mode();
        let bytes = match encode_paste_payload(text, mode) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.error = Some(error.to_owned());
                cx.notify();
                return;
            }
        };
        self.send_input(bytes, cx);
    }

    pub fn copy_selection(&mut self, cx: &mut Context<Self>) -> Option<String> {
        let text = self.emulator.term.selection_to_string()?;
        cx.write_to_clipboard(ClipboardItem::new_string(text.clone()));
        Some(text)
    }

    fn current_lease_token(&self) -> Option<String> {
        self.lease.as_ref().map(|lease| lease.lease_token.clone())
    }

    fn send_input(&mut self, bytes: Vec<u8>, cx: &mut Context<Self>) {
        if bytes.is_empty() {
            return;
        }
        let Some(lease_token) = self.current_lease_token() else {
            self.error = Some("This terminal is read-only. Take control to type.".to_owned());
            cx.notify();
            return;
        };
        self.send_command(TerminalTransportCommand::Input { lease_token, bytes }, cx);
    }

    fn send_command(&mut self, command: TerminalTransportCommand, cx: &mut Context<Self>) {
        if let Err(error) = self.transport.send(command) {
            self.error = Some(error);
            cx.notify();
        }
    }

    fn reduce(&mut self, event: SurfaceEvent, cx: &mut Context<Self>) {
        match event {
            SurfaceEvent::TransportConnected => {
                self.connection = TerminalSurfaceConnection::Connected;
                self.error = None;
            }
            SurfaceEvent::TransportReconnecting(reason) => {
                self.connection = TerminalSurfaceConnection::Reconnecting;
                self.error = Some(reason);
            }
            SurfaceEvent::TransportClosed(reason) => {
                self.connection = TerminalSurfaceConnection::Disconnected;
                self.error = Some(reason);
            }
            SurfaceEvent::Lease(result) => match result {
                Ok(lease) => {
                    self.controller = Some(ControlLeaseSummary {
                        client_id: lease.client_id.clone(),
                        expires_at: lease.expires_at.clone(),
                    });
                    if let Ok(mut token) = self.lease_token.lock() {
                        *token = Some(lease.lease_token.clone());
                    }
                    self.lease = Some(lease);
                    self.error = None;
                }
                Err(error) => self.error = Some(error),
            },
            SurfaceEvent::Viewport(size) => self.apply_resize(size, cx),
            SurfaceEvent::Wire(event) => self.reduce_wire(event),
        }
    }

    fn reduce_wire(&mut self, event: TerminalTransportEvent) {
        match event {
            TerminalTransportEvent::Ready {
                terminal,
                replay_from,
                ..
            } => {
                self.descriptor = terminal;
                self.controller = self.descriptor.controller.clone();
                if replay_from > self.expected_offset {
                    self.replay_gap = Some((self.expected_offset, replay_from));
                }
                self.expected_offset = self.expected_offset.max(replay_from);
                self.connection = TerminalSurfaceConnection::Connected;
            }
            TerminalTransportEvent::Output { offset, bytes } => {
                let end = offset.saturating_add(bytes.len() as u64);
                if end <= self.expected_offset {
                    return;
                }
                if offset > self.expected_offset {
                    self.replay_gap = Some((self.expected_offset, offset));
                }
                let trim = usize::try_from(self.expected_offset.saturating_sub(offset))
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                self.emulator.advance(&bytes[trim..]);
                self.expected_offset = end;
            }
            TerminalTransportEvent::Gap {
                requested_offset,
                available_from,
                next_offset,
            } => {
                self.replay_gap = Some((requested_offset, available_from));
                self.expected_offset = available_from.min(next_offset);
            }
            TerminalTransportEvent::Disconnected { reason } => {
                self.connection = TerminalSurfaceConnection::Disconnected;
                self.error = Some(reason);
            }
            TerminalTransportEvent::ControlChanged { controller } => {
                let still_ours = controller
                    .as_ref()
                    .is_some_and(|controller| controller.client_id == self.client_id);
                self.controller = controller;
                if !still_ours {
                    self.lease = None;
                    if let Ok(mut token) = self.lease_token.lock() {
                        *token = None;
                    }
                }
            }
            TerminalTransportEvent::CommandRejected { code, message } => {
                if code.contains("conflict") || message.contains("control lease") {
                    self.lease = None;
                    if let Ok(mut token) = self.lease_token.lock() {
                        *token = None;
                    }
                }
                self.error = Some(message);
            }
            TerminalTransportEvent::Exited {
                exit_code,
                next_offset,
            } => {
                self.expected_offset = self.expected_offset.max(next_offset);
                self.exit_code = exit_code;
                self.connection = TerminalSurfaceConnection::Exited;
                self.descriptor.status = TerminalStatus::Exited;
                self.descriptor.exit_code = exit_code;
            }
            TerminalTransportEvent::Pong => {}
        }
    }

    fn apply_resize(&mut self, size: TerminalSize, cx: &mut Context<Self>) {
        if self.descriptor.size == size {
            return;
        }
        self.descriptor.size = size;
        self.emulator.resize(size);
        if let Some(lease_token) = self.current_lease_token() {
            self.send_command(
                TerminalTransportCommand::Resize {
                    lease_token,
                    cols: size.cols,
                    rows: size.rows,
                },
                cx,
            );
        }
    }

    fn drain_emulator_side_effects(&mut self, cx: &mut Context<Self>) {
        for effect in self.emulator.take_side_effects() {
            match effect {
                EmulatorSideEffect::Title(title) => {
                    self.title = if title.trim().is_empty() {
                        terminal_title(&self.descriptor)
                    } else {
                        title
                    };
                }
                EmulatorSideEffect::PtyWrite(bytes) => self.send_input(bytes, cx),
                EmulatorSideEffect::ColorRequest(index, formatter) => {
                    let color = named_or_indexed_rgb(index, self.palette);
                    self.send_input(formatter(color).into_bytes(), cx);
                }
                EmulatorSideEffect::TextAreaSize(formatter) => {
                    let size = WindowSize {
                        num_lines: self.descriptor.size.rows,
                        num_cols: self.descriptor.size.cols,
                        cell_width: CELL_WIDTH.round() as u16,
                        cell_height: LINE_HEIGHT.round() as u16,
                    };
                    self.send_input(formatter(size).into_bytes(), cx);
                }
                EmulatorSideEffect::Bell => self.bell = true,
            }
        }
    }

    fn handle_key(&mut self, event: &KeyDownEvent, cx: &mut Context<Self>) {
        let key = event.keystroke.key.as_str();
        let modifiers = event.keystroke.modifiers;
        if modifiers.secondary() && modifiers.shift && key.eq_ignore_ascii_case("c") {
            self.copy_selection(cx);
            cx.stop_propagation();
            return;
        }
        if modifiers.secondary() && modifiers.shift && key.eq_ignore_ascii_case("v") {
            if let Some(text) = cx.read_from_clipboard().and_then(|item| item.text()) {
                self.paste_text(&text, cx);
            }
            cx.stop_propagation();
            return;
        }
        if modifiers.shift && key == "pageup" {
            self.emulator.term.scroll_display(Scroll::PageUp);
            cx.stop_propagation();
            return;
        }
        if modifiers.shift && key == "pagedown" {
            self.emulator.term.scroll_display(Scroll::PageDown);
            cx.stop_propagation();
            return;
        }

        let mode = *self.emulator.term.mode();
        if let Some(bytes) = encode_key(event, mode) {
            self.send_input(bytes, cx);
            cx.stop_propagation();
        }
    }

    fn handle_scroll(&mut self, event: &ScrollWheelEvent, cx: &mut Context<Self>) {
        let delta = event.delta.pixel_delta(px(LINE_HEIGHT));
        let lines = (f32::from(delta.y).abs() / LINE_HEIGHT).ceil().max(1.0) as usize;
        let upward = f32::from(delta.y) > 0.0;
        let mode = *self.emulator.term.mode();
        if mode.intersects(TermMode::MOUSE_MODE) {
            if let Some(point) = self.point_for_position(event.position) {
                let code = if upward { 64 } else { 65 };
                if let Some(bytes) = encode_mouse(mode, code, point, true, event.modifiers) {
                    self.send_input(bytes.repeat(lines), cx);
                }
            }
        } else if mode.contains(TermMode::ALT_SCREEN | TermMode::ALTERNATE_SCROLL) {
            let sequence = if upward {
                b"\x1bOA".as_slice()
            } else {
                b"\x1bOB".as_slice()
            };
            self.send_input(sequence.repeat(lines), cx);
        } else {
            self.emulator.term.scroll_display(if upward {
                Scroll::Delta(lines as i32)
            } else {
                Scroll::Delta(-(lines as i32))
            });
        }
        cx.stop_propagation();
    }

    fn handle_mouse_down(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        window.focus(&self.focus);
        self.bell = false;
        let Some(point) = self.point_for_position(event.position) else {
            return;
        };
        let mode = *self.emulator.term.mode();
        if mode.intersects(TermMode::MOUSE_MODE) && !event.modifiers.shift {
            if let Some(code) = mouse_button_code(event.button) {
                if let Some(bytes) = encode_mouse(mode, code, point, true, event.modifiers) {
                    self.send_input(bytes, cx);
                }
            }
        } else if event.button == MouseButton::Left {
            let selection_type = if event.modifiers.alt {
                SelectionType::Block
            } else if event.click_count >= 3 {
                SelectionType::Lines
            } else if event.click_count == 2 {
                SelectionType::Semantic
            } else {
                SelectionType::Simple
            };
            self.emulator.term.selection = Some(Selection::new(selection_type, point, Side::Left));
        }
        cx.stop_propagation();
    }

    fn handle_mouse_move(&mut self, event: &MouseMoveEvent, cx: &mut Context<Self>) {
        let Some(point) = self.point_for_position(event.position) else {
            return;
        };
        let mode = *self.emulator.term.mode();
        if mode.intersects(TermMode::MOUSE_MODE) && !event.modifiers.shift {
            let report_motion = mode.contains(TermMode::MOUSE_MOTION)
                || (event.dragging() && mode.contains(TermMode::MOUSE_DRAG));
            if report_motion {
                let button = event
                    .pressed_button
                    .and_then(mouse_button_code)
                    .unwrap_or(3)
                    | 32;
                if let Some(bytes) = encode_mouse(mode, button, point, true, event.modifiers) {
                    self.send_input(bytes, cx);
                }
            }
        } else if event.dragging() {
            if let Some(selection) = self.emulator.term.selection.as_mut() {
                selection.update(point, Side::Right);
            }
        }
    }

    fn handle_mouse_up(&mut self, event: &MouseUpEvent, cx: &mut Context<Self>) {
        let Some(point) = self.point_for_position(event.position) else {
            return;
        };
        let mode = *self.emulator.term.mode();
        if mode.intersects(TermMode::MOUSE_MODE) && !event.modifiers.shift {
            if let Some(code) = mouse_button_code(event.button) {
                if let Some(bytes) = encode_mouse(mode, code, point, false, event.modifiers) {
                    self.send_input(bytes, cx);
                }
            }
        } else if event.button == MouseButton::Left {
            if let Some(selection) = self.emulator.term.selection.as_mut() {
                selection.update(point, Side::Right);
            }
            #[cfg(any(target_os = "linux", target_os = "freebsd"))]
            if let Some(text) = self.emulator.term.selection_to_string() {
                cx.write_to_primary(ClipboardItem::new_string(text));
            }
        }
        cx.stop_propagation();
    }

    fn point_for_position(&self, position: gpui::Point<Pixels>) -> Option<Point> {
        let bounds = self.body_bounds.lock().ok().and_then(|bounds| *bounds)?;
        let relative_x = f32::from(position.x - bounds.origin.x).max(0.0);
        let relative_y = f32::from(position.y - bounds.origin.y).max(0.0);
        let cols = usize::from(self.descriptor.size.cols.max(2));
        let rows = usize::from(self.descriptor.size.rows.max(1));
        let column = ((relative_x / f32::from(bounds.size.width).max(1.0)) * cols as f32)
            .floor()
            .clamp(0.0, (cols - 1) as f32) as usize;
        let viewport_line = ((relative_y / f32::from(bounds.size.height).max(1.0)) * rows as f32)
            .floor()
            .clamp(0.0, (rows - 1) as f32) as usize;
        let display_offset = self.emulator.term.grid().display_offset();
        Some(Point::new(
            Line(viewport_line as i32 - display_offset as i32),
            Column(column),
        ))
    }

    fn render_rows(&self) -> Vec<Vec<RenderRun>> {
        render_runs(&self.emulator.term, self.palette)
    }
}

impl Focusable for TerminalSurface {
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus.clone()
    }
}

impl Render for TerminalSurface {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.palette;
        let rows = self.render_rows();
        let body_bounds = Arc::clone(&self.body_bounds);
        let last_layout = Arc::clone(&self.last_layout);
        let layout_tx = self.event_tx.clone();
        let focus = self.focus.clone();
        let connection = self.compact_summary();
        let controller = self
            .controller
            .as_ref()
            .map(|controller| controller.client_id.clone())
            .unwrap_or_else(|| "No controller".to_owned());
        let can_take = !self.has_control()
            && self.descriptor.status == TerminalStatus::Running
            && self.connection != TerminalSurfaceConnection::Exited;

        div()
            .id(SharedString::from(format!(
                "terminal-surface-{}",
                self.descriptor.id
            )))
            .key_context("MitsuroTerminal")
            .track_focus(&focus)
            .flex()
            .flex_col()
            .size_full()
            .min_h_0()
            .overflow_hidden()
            .bg(palette.canvas)
            .on_key_down(cx.listener(|surface, event: &KeyDownEvent, _, cx| {
                surface.handle_key(event, cx)
            }))
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(px(34.0))
                    .px(px(10.0))
                    .border_b_1()
                    .border_color(palette.edge)
                    .bg(palette.surface)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(px(8.0))
                            .min_w_0()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(palette.text)
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(self.title.clone()),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(if self.bell { palette.brass } else { palette.text_muted })
                                    .child(connection),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(palette.text_muted)
                                    .child(controller),
                            ),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(px(6.0))
                            .when(can_take, |actions| {
                                actions.child(
                                    Button::new(SharedString::from(format!(
                                        "terminal-take-{}",
                                        self.descriptor.id
                                    )))
                                        .label("Take control")
                                        .on_click(cx.listener(|surface, _, _, cx| {
                                            surface.take_control(cx)
                                        })),
                                )
                            })
                            .child(
                                Button::new(SharedString::from(format!(
                                    "terminal-copy-{}",
                                    self.descriptor.id
                                )))
                                    .label("Copy")
                                    .on_click(cx.listener(|surface, _, _, cx| {
                                        surface.copy_selection(cx);
                                    })),
                            )
                            .child(
                                Button::new(SharedString::from(format!(
                                    "terminal-paste-{}",
                                    self.descriptor.id
                                )))
                                    .label("Paste")
                                    .disabled(!self.has_control())
                                    .on_click(cx.listener(|surface, _, _, cx| {
                                        if let Some(text) = cx.read_from_clipboard().and_then(|item| item.text()) {
                                            surface.paste_text(&text, cx);
                                        }
                                    })),
                            )
                            .child(
                                Button::new(SharedString::from(format!(
                                    "terminal-end-{}",
                                    self.descriptor.id
                                )))
                                .label(if self.confirm_terminate {
                                    "Confirm end"
                                } else {
                                    "End"
                                })
                                .disabled(
                                    !self.has_control()
                                        || self.descriptor.status != TerminalStatus::Running,
                                )
                                .on_click(cx.listener(|surface, _, _, cx| {
                                    surface.confirm_or_terminate(cx)
                                })),
                            ),
                    ),
            )
            .when_some(self.replay_gap, |surface, (requested, available)| {
                surface.child(
                    div()
                        .px(px(10.0))
                        .py(px(4.0))
                        .bg(palette.brass.opacity(0.12))
                        .text_xs()
                        .text_color(palette.brass)
                        .child(format!(
                            "Earlier terminal output expired (missing offsets {requested}–{available})."
                        )),
                )
            })
            .when_some(self.error.clone(), |surface, error| {
                surface.child(
                    div()
                        .px(px(10.0))
                        .py(px(4.0))
                        .bg(palette.surface)
                        .text_xs()
                        .text_color(palette.brass)
                        .child(error),
                )
            })
            .child(
                div()
                    .on_children_prepainted(move |children, _, _| {
                        let Some(bounds) = children.first().copied() else { return };
                        if let Ok(mut stored) = body_bounds.lock() {
                            *stored = Some(bounds);
                        }
                        let size = TerminalSize {
                            cols: ((f32::from(bounds.size.width) / CELL_WIDTH).floor() as u16)
                                .clamp(2, 500),
                            rows: ((f32::from(bounds.size.height) / LINE_HEIGHT).floor() as u16)
                                .clamp(1, 500),
                        };
                        if let Ok(mut last) = last_layout.lock() {
                            if *last != size
                                && layout_tx.try_send(SurfaceEvent::Viewport(size)).is_ok()
                            {
                                *last = size;
                            }
                        }
                    })
                    .id(SharedString::from(format!(
                        "terminal-body-{}",
                        self.descriptor.id
                    )))
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_hidden()
                    .cursor_text()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|surface, event: &MouseDownEvent, window, cx| {
                            surface.handle_mouse_down(event, window, cx)
                        }),
                    )
                    .on_mouse_down(
                        MouseButton::Middle,
                        cx.listener(|surface, event: &MouseDownEvent, window, cx| {
                            surface.handle_mouse_down(event, window, cx)
                        }),
                    )
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(|surface, event: &MouseDownEvent, window, cx| {
                            surface.handle_mouse_down(event, window, cx)
                        }),
                    )
                    .on_mouse_move(cx.listener(|surface, event: &MouseMoveEvent, _, cx| {
                        surface.handle_mouse_move(event, cx)
                    }))
                    .on_mouse_up(
                        MouseButton::Left,
                        cx.listener(|surface, event: &MouseUpEvent, _, cx| {
                            surface.handle_mouse_up(event, cx)
                        }),
                    )
                    .on_mouse_up(
                        MouseButton::Middle,
                        cx.listener(|surface, event: &MouseUpEvent, _, cx| {
                            surface.handle_mouse_up(event, cx)
                        }),
                    )
                    .on_mouse_up(
                        MouseButton::Right,
                        cx.listener(|surface, event: &MouseUpEvent, _, cx| {
                            surface.handle_mouse_up(event, cx)
                        }),
                    )
                    .on_scroll_wheel(cx.listener(
                        |surface, event: &ScrollWheelEvent, _, cx| {
                            surface.handle_scroll(event, cx)
                        },
                    ))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .w_full()
                            .h_full()
                            .bg(palette.canvas)
                            .children(rows.into_iter().map(render_row)),
                    ),
            )
    }
}

fn surface_event_bytes(event: &SurfaceEvent) -> usize {
    match event {
        SurfaceEvent::Wire(TerminalTransportEvent::Output { bytes, .. }) => bytes.len(),
        _ => 0,
    }
}

fn terminal_title(descriptor: &TerminalDescriptor) -> String {
    descriptor
        .working_dir
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(|directory| format!("Terminal · {directory}"))
        .unwrap_or_else(|| "Terminal".to_owned())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RenderStyle {
    fg: u32,
    bg: u32,
    bold: bool,
    italic: bool,
    underline: bool,
    strike: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RenderRun {
    text: String,
    cells: usize,
    style: RenderStyle,
}

fn render_runs<T: EventListener>(term: &Term<T>, palette: CanvasPalette) -> Vec<Vec<RenderRun>> {
    let content = term.renderable_content();
    let selection = content.selection;
    let cursor = content.cursor.point;
    let colors = content.colors;
    let mut rows: Vec<Vec<RenderRun>> = Vec::with_capacity(term.screen_lines());
    let mut current_line = None;

    for indexed in content.display_iter {
        if current_line != Some(indexed.point.line) {
            rows.push(Vec::new());
            current_line = Some(indexed.point.line);
        }
        let selected = selection.is_some_and(|selection| selection.contains(indexed.point));
        let cursor = indexed.point == cursor && content.mode.contains(TermMode::SHOW_CURSOR);
        let mut fg = resolve_color(indexed.fg, colors, true, indexed.flags, palette);
        let mut bg = resolve_color(indexed.bg, colors, false, indexed.flags, palette);
        if indexed.flags.contains(Flags::INVERSE) {
            std::mem::swap(&mut fg, &mut bg);
        }
        if selected {
            bg = hsla_over_rgb24(palette.pulse_soft, palette.canvas);
            fg = hsla_to_rgb24(palette.text);
        }
        if cursor {
            std::mem::swap(&mut fg, &mut bg);
            if fg == bg {
                bg = hsla_to_rgb24(palette.pulse);
                fg = hsla_to_rgb24(palette.canvas);
            }
        }
        let style = RenderStyle {
            fg,
            bg,
            bold: indexed.flags.contains(Flags::BOLD),
            italic: indexed.flags.contains(Flags::ITALIC),
            underline: indexed.flags.intersects(Flags::ALL_UNDERLINES),
            strike: indexed.flags.contains(Flags::STRIKEOUT),
        };
        let text = if indexed
            .flags
            .intersects(Flags::WIDE_CHAR_SPACER | Flags::LEADING_WIDE_CHAR_SPACER)
        {
            String::new()
        } else if indexed.flags.contains(Flags::HIDDEN) {
            " ".to_owned()
        } else {
            let mut text = String::new();
            text.push(if indexed.c == '\t' { ' ' } else { indexed.c });
            if let Some(zerowidth) = indexed.zerowidth() {
                text.extend(zerowidth.iter());
            }
            text
        };
        let row = rows.last_mut().expect("row exists after line transition");
        if let Some(run) = row.last_mut().filter(|run| run.style == style) {
            run.text.push_str(&text);
            run.cells += 1;
        } else {
            row.push(RenderRun {
                text,
                cells: 1,
                style,
            });
        }
    }
    rows
}

fn render_row(runs: Vec<RenderRun>) -> impl IntoElement {
    div()
        .flex()
        .flex_none()
        .h(px(LINE_HEIGHT))
        .w_full()
        .overflow_hidden()
        .children(runs.into_iter().map(|run| {
            div()
                .flex_none()
                .w(px(run.cells as f32 * CELL_WIDTH))
                .h(px(LINE_HEIGHT))
                .overflow_hidden()
                .whitespace_nowrap()
                .font_family("monospace")
                .text_size(px(TERMINAL_FONT_SIZE))
                .line_height(px(LINE_HEIGHT))
                .text_color(rgb(run.style.fg))
                .bg(rgb(run.style.bg))
                .when(run.style.bold, |cell| cell.font_weight(FontWeight::BOLD))
                .when(run.style.italic, |cell| cell.italic())
                .when(run.style.underline, |cell| cell.underline())
                .when(run.style.strike, |cell| cell.line_through())
                .child(run.text)
        }))
}

fn resolve_color(
    color: Color,
    dynamic: &alacritty_terminal::term::color::Colors,
    foreground: bool,
    flags: Flags,
    palette: CanvasPalette,
) -> u32 {
    let color = if flags.contains(Flags::BOLD) {
        match color {
            Color::Named(named) => Color::Named(named.to_bright()),
            color => color,
        }
    } else if flags.contains(Flags::DIM) {
        match color {
            Color::Named(named) => Color::Named(named.to_dim()),
            color => color,
        }
    } else {
        color
    };
    let rgb = match color {
        Color::Spec(rgb) => rgb,
        Color::Indexed(index) => {
            dynamic[index as usize].unwrap_or_else(|| indexed_rgb(index, palette))
        }
        Color::Named(named) => {
            dynamic[named].unwrap_or_else(|| named_rgb(named, foreground, palette))
        }
    };
    (u32::from(rgb.r) << 16) | (u32::from(rgb.g) << 8) | u32::from(rgb.b)
}

fn named_rgb(color: NamedColor, foreground: bool, palette: CanvasPalette) -> Rgb {
    let value = match color {
        NamedColor::Black => 0x1b1d23,
        NamedColor::Red => 0xe06c75,
        NamedColor::Green => 0x98c379,
        NamedColor::Yellow => 0xd7ba7d,
        NamedColor::Blue => 0x61afef,
        NamedColor::Magenta => 0xc678dd,
        NamedColor::Cyan => 0x56b6c2,
        NamedColor::White => 0xabb2bf,
        NamedColor::BrightBlack | NamedColor::DimBlack => 0x5c6370,
        NamedColor::BrightRed | NamedColor::DimRed => 0xff7a83,
        NamedColor::BrightGreen | NamedColor::DimGreen => 0xb4e08d,
        NamedColor::BrightYellow | NamedColor::DimYellow => 0xf1ca88,
        NamedColor::BrightBlue | NamedColor::DimBlue => 0x79c0ff,
        NamedColor::BrightMagenta | NamedColor::DimMagenta => 0xd896ed,
        NamedColor::BrightCyan | NamedColor::DimCyan => 0x6fd3df,
        NamedColor::BrightWhite | NamedColor::DimWhite => 0xf3f0f4,
        NamedColor::Background => hsla_to_rgb24(palette.canvas),
        NamedColor::DimForeground => hsla_over_rgb24(palette.text_muted, palette.canvas),
        NamedColor::Cursor => hsla_to_rgb24(palette.pulse),
        NamedColor::Foreground | NamedColor::BrightForeground => {
            if foreground {
                hsla_to_rgb24(palette.text)
            } else {
                hsla_to_rgb24(palette.canvas)
            }
        }
    };
    Rgb {
        r: (value >> 16) as u8,
        g: (value >> 8) as u8,
        b: value as u8,
    }
}

fn hsla_to_rgb24(color: gpui::Hsla) -> u32 {
    let color = color.to_rgb();
    let channel = |value: f32| ((value.clamp(0.0, 1.0) * 255.0).round() as u32).min(255);
    (channel(color.r) << 16) | (channel(color.g) << 8) | channel(color.b)
}

fn hsla_over_rgb24(foreground: gpui::Hsla, background: gpui::Hsla) -> u32 {
    let foreground = foreground.to_rgb();
    let background = background.to_rgb();
    let alpha = foreground.a.clamp(0.0, 1.0);
    let channel = |foreground: f32, background: f32| {
        ((foreground * alpha + background * (1.0 - alpha)).clamp(0.0, 1.0) * 255.0).round() as u32
    };
    (channel(foreground.r, background.r) << 16)
        | (channel(foreground.g, background.g) << 8)
        | channel(foreground.b, background.b)
}

fn indexed_rgb(index: u8, palette: CanvasPalette) -> Rgb {
    match index {
        0..=15 => named_rgb(
            [
                NamedColor::Black,
                NamedColor::Red,
                NamedColor::Green,
                NamedColor::Yellow,
                NamedColor::Blue,
                NamedColor::Magenta,
                NamedColor::Cyan,
                NamedColor::White,
                NamedColor::BrightBlack,
                NamedColor::BrightRed,
                NamedColor::BrightGreen,
                NamedColor::BrightYellow,
                NamedColor::BrightBlue,
                NamedColor::BrightMagenta,
                NamedColor::BrightCyan,
                NamedColor::BrightWhite,
            ][usize::from(index)],
            true,
            palette,
        ),
        16..=231 => {
            let index = index - 16;
            let component = |value: u8| if value == 0 { 0 } else { 55 + value * 40 };
            Rgb {
                r: component(index / 36),
                g: component((index % 36) / 6),
                b: component(index % 6),
            }
        }
        232..=255 => {
            let gray = 8 + (index - 232) * 10;
            Rgb {
                r: gray,
                g: gray,
                b: gray,
            }
        }
    }
}

fn named_or_indexed_rgb(index: usize, palette: CanvasPalette) -> Rgb {
    match u8::try_from(index) {
        Ok(index) => indexed_rgb(index, palette),
        Err(_) if index == NamedColor::Foreground as usize => {
            named_rgb(NamedColor::Foreground, true, palette)
        }
        Err(_) if index == NamedColor::Background as usize => {
            named_rgb(NamedColor::Background, false, palette)
        }
        _ => named_rgb(NamedColor::Foreground, true, palette),
    }
}

fn encode_key(event: &KeyDownEvent, mode: TermMode) -> Option<Vec<u8>> {
    let key = event.keystroke.key.to_ascii_lowercase();
    let modifiers = event.keystroke.modifiers;
    let modifier_parameter = xterm_modifier_parameter(modifiers);
    let app_cursor = mode.contains(TermMode::APP_CURSOR);
    let special = match key.as_str() {
        "enter" | "return" => Some("\r".to_owned()),
        "tab" if modifiers.shift => Some("\x1b[Z".to_owned()),
        "tab" => Some("\t".to_owned()),
        "backspace" => Some("\x7f".to_owned()),
        "escape" => Some("\x1b".to_owned()),
        "up" | "arrowup" => Some(cursor_sequence('A', app_cursor, modifier_parameter)),
        "down" | "arrowdown" => Some(cursor_sequence('B', app_cursor, modifier_parameter)),
        "right" | "arrowright" => Some(cursor_sequence('C', app_cursor, modifier_parameter)),
        "left" | "arrowleft" => Some(cursor_sequence('D', app_cursor, modifier_parameter)),
        "home" => Some(csi_tilde_or_final('H', 1, modifier_parameter)),
        "end" => Some(csi_tilde_or_final('F', 1, modifier_parameter)),
        "insert" => Some(csi_tilde(2, modifier_parameter)),
        "delete" => Some(csi_tilde(3, modifier_parameter)),
        "pageup" => Some(csi_tilde(5, modifier_parameter)),
        "pagedown" => Some(csi_tilde(6, modifier_parameter)),
        "f1" => Some(function_ss3('P', modifier_parameter)),
        "f2" => Some(function_ss3('Q', modifier_parameter)),
        "f3" => Some(function_ss3('R', modifier_parameter)),
        "f4" => Some(function_ss3('S', modifier_parameter)),
        "f5" => Some(csi_tilde(15, modifier_parameter)),
        "f6" => Some(csi_tilde(17, modifier_parameter)),
        "f7" => Some(csi_tilde(18, modifier_parameter)),
        "f8" => Some(csi_tilde(19, modifier_parameter)),
        "f9" => Some(csi_tilde(20, modifier_parameter)),
        "f10" => Some(csi_tilde(21, modifier_parameter)),
        "f11" => Some(csi_tilde(23, modifier_parameter)),
        "f12" => Some(csi_tilde(24, modifier_parameter)),
        _ => None,
    };
    if let Some(special) = special {
        return Some(prefix_alt(special.into_bytes(), modifiers.alt));
    }

    let text = event
        .keystroke
        .key_char
        .as_deref()
        .unwrap_or(event.keystroke.key.as_str());
    if modifiers.control {
        let mut chars = text.chars();
        let character = chars.next()?;
        if chars.next().is_none() {
            let byte = match character.to_ascii_uppercase() {
                '@' | ' ' => 0,
                'A'..='Z' => character.to_ascii_uppercase() as u8 - b'@',
                '[' => 27,
                '\\' => 28,
                ']' => 29,
                '^' => 30,
                '_' => 31,
                '?' => 127,
                _ => return None,
            };
            return Some(prefix_alt(vec![byte], modifiers.alt));
        }
    }
    if text.is_empty() || modifiers.platform {
        return None;
    }
    Some(prefix_alt(text.as_bytes().to_vec(), modifiers.alt))
}

fn xterm_modifier_parameter(modifiers: Modifiers) -> u8 {
    1 + u8::from(modifiers.shift)
        + (u8::from(modifiers.alt) * 2)
        + (u8::from(modifiers.control) * 4)
        + (u8::from(modifiers.platform) * 8)
}

fn cursor_sequence(final_byte: char, app_cursor: bool, modifier: u8) -> String {
    if modifier == 1 {
        format!("\x1b{}{}", if app_cursor { 'O' } else { '[' }, final_byte)
    } else {
        format!("\x1b[1;{modifier}{final_byte}")
    }
}

fn csi_tilde(number: u8, modifier: u8) -> String {
    if modifier == 1 {
        format!("\x1b[{number}~")
    } else {
        format!("\x1b[{number};{modifier}~")
    }
}

fn csi_tilde_or_final(final_byte: char, number: u8, modifier: u8) -> String {
    if modifier == 1 {
        format!("\x1b[{final_byte}")
    } else {
        format!("\x1b[{number};{modifier}{final_byte}")
    }
}

fn function_ss3(final_byte: char, modifier: u8) -> String {
    if modifier == 1 {
        format!("\x1bO{final_byte}")
    } else {
        format!("\x1b[1;{modifier}{final_byte}")
    }
}

fn prefix_alt(mut bytes: Vec<u8>, alt: bool) -> Vec<u8> {
    if alt && !bytes.starts_with(b"\x1b") {
        bytes.insert(0, 0x1b);
    }
    bytes
}

fn mouse_button_code(button: MouseButton) -> Option<u8> {
    match button {
        MouseButton::Left => Some(0),
        MouseButton::Middle => Some(1),
        MouseButton::Right => Some(2),
        MouseButton::Navigate(_) => None,
    }
}

fn encode_mouse(
    mode: TermMode,
    mut button: u8,
    point: Point,
    pressed: bool,
    modifiers: Modifiers,
) -> Option<Vec<u8>> {
    button |= u8::from(modifiers.shift) * 4;
    button |= u8::from(modifiers.alt) * 8;
    button |= u8::from(modifiers.control) * 16;
    let column = point.column.0.saturating_add(1);
    let row = usize::try_from(point.line.0).ok()?.saturating_add(1);
    if mode.contains(TermMode::SGR_MOUSE) {
        return Some(
            format!(
                "\x1b[<{button};{column};{row}{}",
                if pressed { 'M' } else { 'm' }
            )
            .into_bytes(),
        );
    }
    if !pressed {
        button = 3
            | (u8::from(modifiers.shift) * 4)
            | (u8::from(modifiers.alt) * 8)
            | (u8::from(modifiers.control) * 16);
    }
    let column = u8::try_from(column.min(223)).ok()?.saturating_add(32);
    let row = u8::try_from(row.min(223)).ok()?.saturating_add(32);
    Some(vec![
        0x1b,
        b'[',
        b'M',
        button.saturating_add(32),
        column,
        row,
    ])
}

fn encode_paste_payload(text: &str, mode: TermMode) -> Result<Vec<u8>, &'static str> {
    if !mode.contains(TermMode::BRACKETED_PASTE) {
        if text.len() > MAX_TERMINAL_INPUT_BYTES {
            return Err("Paste exceeds the terminal's 64 KiB input limit");
        }
        return Ok(text.as_bytes().to_vec());
    }
    if text.len() > MAX_TERMINAL_INPUT_BYTES.saturating_sub(BRACKETED_PASTE_WRAPPER_BYTES) {
        return Err("Paste exceeds the terminal's 64 KiB input limit");
    }

    let mut bytes = Vec::with_capacity(text.len() + BRACKETED_PASTE_WRAPPER_BYTES);
    bytes.extend_from_slice(b"\x1b[200~");
    // Never allow pasted text to terminate the bracket early and append
    // unbracketed input. This mirrors the explicit user paste path only; PTY
    // OSC clipboard requests remain disabled independently.
    bytes.extend_from_slice(text.replace("\x1b[201~", "").as_bytes());
    bytes.extend_from_slice(b"\x1b[201~");
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alacritty_terminal::event::VoidListener;
    use gpui::Keystroke;

    fn key(source: &str) -> KeyDownEvent {
        KeyDownEvent {
            keystroke: Keystroke::parse(source).expect("valid key"),
            is_held: false,
        }
    }

    #[test]
    fn key_encoding_covers_modifiers_functions_and_app_cursor() {
        assert_eq!(encode_key(&key("ctrl-c"), TermMode::empty()), Some(vec![3]));
        assert_eq!(
            encode_key(&key("alt-x"), TermMode::empty()),
            Some(b"\x1bx".to_vec())
        );
        assert_eq!(
            encode_key(&key("up"), TermMode::APP_CURSOR),
            Some(b"\x1bOA".to_vec())
        );
        assert_eq!(
            encode_key(&key("ctrl-f5"), TermMode::empty()),
            Some(b"\x1b[15;5~".to_vec())
        );
    }

    #[test]
    fn offset_reducer_trims_duplicate_output_and_marks_forward_gaps() {
        let mut expected = 5_u64;
        let mut emulator = TerminalEmulator::new(TerminalSize { cols: 20, rows: 2 });
        let apply = |expected: &mut u64, emulator: &mut TerminalEmulator, offset, bytes: &[u8]| {
            let end = offset + bytes.len() as u64;
            if end <= *expected {
                return false;
            }
            let gap = offset > *expected;
            let trim = usize::try_from(expected.saturating_sub(offset))
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            emulator.advance(&bytes[trim..]);
            *expected = end;
            gap
        };
        assert!(!apply(&mut expected, &mut emulator, 3, b"cdef"));
        assert_eq!(expected, 7);
        assert!(apply(&mut expected, &mut emulator, 9, b"x"));
        assert_eq!(expected, 10);
    }

    #[test]
    fn alacritty_emulator_keeps_truecolor_and_ten_thousand_lines() {
        let mut emulator = TerminalEmulator::new(TerminalSize { cols: 20, rows: 2 });
        emulator.advance(b"\x1b[38;2;12;34;56mRGB\x1b[0m");
        let content = emulator.term.renderable_content();
        let rgb_cell = content
            .display_iter
            .into_iter()
            .find(|indexed| indexed.c == 'R')
            .expect("RGB cell");
        assert_eq!(
            rgb_cell.fg,
            Color::Spec(Rgb {
                r: 12,
                g: 34,
                b: 56
            })
        );
        emulator.advance("\r\n".repeat(SCROLLBACK_LINES + 8).as_bytes());
        assert_eq!(emulator.term.grid().history_size(), SCROLLBACK_LINES);
    }

    #[test]
    fn emulator_preserves_wide_glyph_cells_and_selection_text() {
        let mut emulator = TerminalEmulator::new(TerminalSize { cols: 12, rows: 2 });
        emulator.advance("A界B".as_bytes());

        let grid = emulator.term.grid();
        assert_eq!(grid[Line(0)][Column(1)].c, '界');
        assert!(grid[Line(0)][Column(1)].flags.contains(Flags::WIDE_CHAR));
        assert!(grid[Line(0)][Column(2)]
            .flags
            .contains(Flags::WIDE_CHAR_SPACER));

        emulator.term.selection = Some(Selection::new(
            SelectionType::Simple,
            Point::new(Line(0), Column(0)),
            Side::Left,
        ));
        emulator
            .term
            .selection
            .as_mut()
            .expect("selection")
            .update(Point::new(Line(0), Column(3)), Side::Right);
        assert_eq!(emulator.term.selection_to_string().as_deref(), Some("A界B"));
    }

    #[test]
    fn emulator_scrollback_navigation_and_alternate_screen_restore() {
        let mut emulator = TerminalEmulator::new(TerminalSize { cols: 12, rows: 3 });
        emulator.advance(b"primary");
        for _ in 0..8 {
            emulator.advance(b"\r\nline");
        }
        assert!(emulator.term.grid().history_size() > 0);
        assert_eq!(emulator.term.grid().display_offset(), 0);

        emulator.term.scroll_display(Scroll::PageUp);
        assert!(emulator.term.grid().display_offset() > 0);
        emulator.term.scroll_display(Scroll::Bottom);
        assert_eq!(emulator.term.grid().display_offset(), 0);

        emulator.advance(b"\x1b[2J\x1b[Hprimary\x1b[?1049h\x1b[2J\x1b[Halternate");
        assert!(emulator.term.mode().contains(TermMode::ALT_SCREEN));
        assert_eq!(emulator.term.grid()[Line(0)][Column(0)].c, 'a');
        emulator.advance(b"\x1b[?1049l");
        assert!(!emulator.term.mode().contains(TermMode::ALT_SCREEN));
        assert_eq!(emulator.term.grid()[Line(0)][Column(0)].c, 'p');
    }

    #[test]
    fn bracketed_paste_mode_is_detected_and_payload_cannot_escape_early() {
        let mut emulator = TerminalEmulator::new(TerminalSize { cols: 12, rows: 2 });
        emulator.advance(b"\x1b[?2004h");
        assert!(emulator.term.mode().contains(TermMode::BRACKETED_PASTE));
        assert_eq!(
            encode_paste_payload("hello\x1b[201~world", *emulator.term.mode()),
            Ok(b"\x1b[200~helloworld\x1b[201~".to_vec())
        );
        emulator.advance(b"\x1b[?2004l");
        assert_eq!(
            encode_paste_payload("plain", *emulator.term.mode()),
            Ok(b"plain".to_vec())
        );
    }

    #[test]
    fn paste_is_rejected_before_allocating_beyond_the_server_input_limit() {
        let oversized = "x".repeat(MAX_TERMINAL_INPUT_BYTES + 1);
        assert!(encode_paste_payload(&oversized, TermMode::empty()).is_err());

        let bracketed = "x".repeat(MAX_TERMINAL_INPUT_BYTES - BRACKETED_PASTE_WRAPPER_BYTES + 1);
        assert!(encode_paste_payload(&bracketed, TermMode::BRACKETED_PASTE).is_err());
    }

    #[test]
    fn transport_event_queue_is_bounded_under_output_flood() {
        let (tx, _rx) = flume::bounded(2);
        let output = || {
            SurfaceEvent::Wire(TerminalTransportEvent::Output {
                offset: 0,
                bytes: vec![0; 4096],
            })
        };
        assert!(tx.try_send(output()).is_ok());
        assert!(tx.try_send(output()).is_ok());
        assert!(tx.try_send(output()).is_err());
    }

    #[test]
    fn osc52_cannot_enqueue_system_clipboard_side_effects() {
        let mut emulator = TerminalEmulator::new(TerminalSize { cols: 20, rows: 2 });

        emulator.advance(b"\x1b]52;c;c2VjcmV0\x07");
        emulator.advance(b"\x1b]52;c;?\x1b\\");

        assert!(emulator.take_side_effects().is_empty());
    }

    #[test]
    fn sgr_mouse_encoding_preserves_coordinates_and_release() {
        let point = Point::new(Line(4), Column(9));
        assert_eq!(
            encode_mouse(TermMode::SGR_MOUSE, 0, point, true, Modifiers::default()),
            Some(b"\x1b[<0;10;5M".to_vec())
        );
        assert_eq!(
            encode_mouse(TermMode::SGR_MOUSE, 0, point, false, Modifiers::default()),
            Some(b"\x1b[<0;10;5m".to_vec())
        );
    }

    #[test]
    fn renderer_groups_cells_without_flattening_ansi_color() {
        let size = EmulatorSize { cols: 8, rows: 1 };
        let mut term = Term::new(TerminalConfig::default(), &size, VoidListener);
        let mut parser: ansi::Processor = ansi::Processor::new();
        parser.advance(&mut term, b"a\x1b[38;2;1;2;3mb");
        let runs = render_runs(&term, CanvasPalette::default());
        assert_eq!(runs.len(), 1);
        assert!(runs[0].iter().any(|run| run.style.fg == 0x010203));
    }

    #[test]
    fn renderer_uses_the_active_palette_for_default_terminal_colors() {
        let size = EmulatorSize { cols: 4, rows: 1 };
        let mut term = Term::new(TerminalConfig::default(), &size, VoidListener);
        let mut parser: ansi::Processor = ansi::Processor::new();
        parser.advance(&mut term, b"a");
        let palette = CanvasPalette {
            canvas: crate::theme::hex(0x102030),
            text: crate::theme::hex(0xf1e2d3),
            pulse: crate::theme::hex(0xab54ef),
            ..CanvasPalette::default()
        };

        let runs = render_runs(&term, palette);
        let first = runs[0].first().expect("default terminal run");
        assert_eq!(first.style.bg, 0x102030);
        assert_eq!(first.style.fg, 0xf1e2d3);
    }
}
