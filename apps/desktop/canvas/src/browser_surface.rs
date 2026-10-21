use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use futures_util::{SinkExt as _, StreamExt as _};
use gpui::prelude::FluentBuilder as _;
use gpui::{
    div, img, px, AppContext as _, Bounds, Context, Entity, FocusHandle, Image, ImageFormat,
    InteractiveElement as _, IntoElement, ParentElement as _, Pixels, Render, SharedString,
    Styled as _, StyledImage as _, Task, Window,
};
use gpui_component::button::Button;
use gpui_component::input::{Input, InputEvent, InputState};
use mitsuro_client::{
    BrowserAction, BrowserControlLease, BrowserPresenceCapability, BrowserSession,
    BrowserSessionStatus, MitsuroClient, TakeBrowserControlRequest,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::runtime::Runtime;
use tokio::sync::{watch, Notify};
use tokio_tungstenite::tungstenite::Message;

use crate::theme::CanvasPalette;

const EVENT_QUEUE_CAPACITY: usize = 32;
const INPUT_QUEUE_CAPACITY: usize = 64;
const MAX_FRAME_BASE64_BYTES: usize = 24 * 1024 * 1024;
const MAX_RECONNECT_ATTEMPTS: usize = 8;
const HEALTHY_CONNECTION_RESET_AFTER: Duration = Duration::from_secs(30);

fn browser_session_is_ended(session: &BrowserSession) -> bool {
    matches!(
        session.status,
        BrowserSessionStatus::Stopped | BrowserSessionStatus::Error
    )
}

#[derive(Debug)]
struct SequencedBrowserInput {
    sequence: u64,
    value: Value,
}

#[derive(Default)]
struct BrowserInputQueueState {
    ordered: VecDeque<SequencedBrowserInput>,
    latest_move: Option<SequencedBrowserInput>,
    next_sequence: u64,
}

#[derive(Clone)]
struct BrowserInputSender {
    state: Arc<Mutex<BrowserInputQueueState>>,
    wake: Arc<Notify>,
    ordered_capacity: usize,
}

struct BrowserInputReceiver {
    state: Arc<Mutex<BrowserInputQueueState>>,
    wake: Arc<Notify>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct BrowserInputQueueFull;

fn browser_input_queue(ordered_capacity: usize) -> (BrowserInputSender, BrowserInputReceiver) {
    let state = Arc::new(Mutex::new(BrowserInputQueueState::default()));
    let wake = Arc::new(Notify::new());
    (
        BrowserInputSender {
            state: Arc::clone(&state),
            wake: Arc::clone(&wake),
            ordered_capacity,
        },
        BrowserInputReceiver { state, wake },
    )
}

impl BrowserInputSender {
    fn try_send(&self, value: Value) -> Result<(), BrowserInputQueueFull> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !is_pointer_move(&value) && state.ordered.len() >= self.ordered_capacity {
            return Err(BrowserInputQueueFull);
        }
        let input = SequencedBrowserInput {
            sequence: state.next_sequence,
            value,
        };
        state.next_sequence = state.next_sequence.wrapping_add(1);
        if is_pointer_move(&input.value) {
            state.latest_move = Some(input);
        } else {
            state.ordered.push_back(input);
        }
        drop(state);
        self.wake.notify_one();
        Ok(())
    }

    #[cfg(test)]
    fn pending_counts(&self) -> (usize, usize) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.ordered.len(),
            usize::from(state.latest_move.is_some()),
        )
    }
}

impl BrowserInputReceiver {
    async fn recv(&self) -> Value {
        loop {
            let notified = self.wake.notified();
            if let Some(value) = self.try_recv() {
                return value;
            }
            notified.await;
        }
    }

    fn try_recv(&self) -> Option<Value> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let take_move = match (state.latest_move.as_ref(), state.ordered.front()) {
            (Some(latest_move), Some(ordered)) => latest_move.sequence < ordered.sequence,
            (Some(_), None) => true,
            (None, _) => false,
        };
        if take_move {
            state.latest_move.take().map(|input| input.value)
        } else {
            state.ordered.pop_front().map(|input| input.value)
        }
    }
}

fn is_pointer_move(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("input_mouse")
        && value.get("eventType").and_then(Value::as_str) == Some("mouseMoved")
}

#[derive(Clone)]
struct BrowserFrame {
    image: Arc<Image>,
    seq: u64,
    device_width: f32,
    device_height: f32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FrameMetadata {
    #[serde(default)]
    device_width: f32,
    #[serde(default)]
    device_height: f32,
}

#[derive(Debug, Deserialize)]
struct StreamEnvelope {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    data: String,
    metadata: Option<FrameMetadata>,
}

struct BrowserAttachment {
    session: BrowserSession,
    control_lease: Option<BrowserControlLease>,
    notice: Option<String>,
}

enum BrowserEvent {
    Attached(std::result::Result<BrowserAttachment, String>),
    Session(std::result::Result<BrowserSession, String>),
    ControlTaken(std::result::Result<(BrowserSession, BrowserControlLease), String>),
    ControlLost(String),
    FrameReady,
    Connected,
    Disconnected(String),
    Ended(std::result::Result<BrowserSession, String>),
}

/// Native pixels and input for one server-owned Browser session.
///
/// This entity owns one bounded connection and exactly one latest frame. Moving
/// the entity into Stage therefore never duplicates a browser subscription.
pub struct BrowserSurface {
    client: MitsuroClient,
    runtime: Arc<Runtime>,
    palette: CanvasPalette,
    project_dir: String,
    session_id: String,
    client_id: String,
    session: Option<BrowserSession>,
    control_lease: Option<BrowserControlLease>,
    taking_control: bool,
    address: Entity<InputState>,
    focus: FocusHandle,
    frame: Option<BrowserFrame>,
    frame_slot: Arc<Mutex<Option<BrowserFrame>>>,
    content_bounds: Arc<Mutex<Option<Bounds<Pixels>>>>,
    connected: bool,
    error: Option<String>,
    confirm_end: bool,
    event_tx: flume::Sender<BrowserEvent>,
    input_tx: Option<BrowserInputSender>,
    cancel_tx: Option<watch::Sender<bool>>,
    _listener: Task<()>,
}

impl Drop for BrowserSurface {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel_tx.take() {
            let _ = cancel.send(true);
        }
    }
}

impl BrowserSurface {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: MitsuroClient,
        runtime: Arc<Runtime>,
        project_dir: String,
        session_id: String,
        client_id: String,
        palette: CanvasPalette,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let address = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Address or local URL")
                .clean_on_escape()
        });
        cx.subscribe_in(
            &address,
            window,
            |surface, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::PressEnter { secondary: false }) {
                    surface.navigate(input.read(cx).value().to_string(), cx);
                }
            },
        )
        .detach();

        let (event_tx, event_rx) = flume::bounded(EVENT_QUEUE_CAPACITY);
        let listener = cx.spawn(async move |this, cx| {
            while let Ok(first) = event_rx.recv_async().await {
                let mut batch = Vec::with_capacity(8);
                batch.push(first);
                while batch.len() < EVENT_QUEUE_CAPACITY {
                    match event_rx.try_recv() {
                        Ok(event) => batch.push(event),
                        Err(_) => break,
                    }
                }
                if this
                    .update(cx, |surface, cx| {
                        for event in batch {
                            surface.reduce(event, cx);
                        }
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        let mut surface = Self {
            client,
            runtime,
            palette,
            project_dir,
            session_id,
            client_id,
            session: None,
            control_lease: None,
            taking_control: false,
            address,
            focus: cx.focus_handle(),
            frame: None,
            frame_slot: Arc::new(Mutex::new(None)),
            content_bounds: Arc::new(Mutex::new(None)),
            connected: false,
            error: None,
            confirm_end: false,
            event_tx,
            input_tx: None,
            cancel_tx: None,
            _listener: listener,
        };
        surface.load_session();
        surface
    }

    pub fn compact_summary(&self) -> SharedString {
        if let Some(error) = &self.error {
            return format!("Disconnected · {error}").into();
        }
        match &self.session {
            Some(session) => session
                .url
                .clone()
                .unwrap_or_else(|| session.title.clone())
                .into(),
            None => "Connecting to server Browser…".into(),
        }
    }

    pub fn set_palette(&mut self, palette: CanvasPalette, cx: &mut Context<Self>) {
        if self.palette != palette {
            self.palette = palette;
            cx.notify();
        }
    }

    pub fn is_ended(&self) -> bool {
        self.session.as_ref().is_some_and(browser_session_is_ended)
    }

    pub fn is_working(&self) -> bool {
        self.taking_control
            || self.session.as_ref().is_none_or(|session| {
                matches!(
                    session.status,
                    BrowserSessionStatus::Starting | BrowserSessionStatus::Running
                )
            })
    }

    pub fn is_disconnected(&self) -> bool {
        !self.connected && self.error.is_some() && !self.is_ended()
    }

    fn load_session(&mut self) {
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let project_dir = self.project_dir.clone();
        let client_id = self.client_id.clone();
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            let result = async {
                let mut session = client.get_browser_session(&session_id).await?;
                if session.project_dir != project_dir {
                    anyhow::bail!("Browser session belongs to a different project");
                }

                if browser_session_is_ended(&session) {
                    return Ok(BrowserAttachment {
                        session,
                        control_lease: None,
                        notice: Some("Browser session ended".to_owned()),
                    });
                }

                let may_claim = session
                    .controller
                    .as_ref()
                    .map(|controller| controller.client_id == client_id)
                    .unwrap_or(true);
                if !may_claim {
                    let controller = session
                        .controller
                        .as_ref()
                        .map(|controller| controller.client_id.clone())
                        .unwrap_or_else(|| "another client".to_owned());
                    return Ok(BrowserAttachment {
                        session,
                        control_lease: None,
                        notice: Some(format!("Observer · controlled by {controller}")),
                    });
                }

                match client
                    .take_browser_control(
                        &session_id,
                        &TakeBrowserControlRequest {
                            client_id: client_id.clone(),
                        },
                    )
                    .await
                {
                    Ok(response) => {
                        session = client.get_browser_session(&session_id).await?;
                        Ok(BrowserAttachment {
                            session,
                            control_lease: Some(response.lease),
                            notice: None,
                        })
                    }
                    Err(error) => {
                        // A concurrent controller or temporarily unavailable
                        // lease endpoint must not make the visible Browser
                        // disappear. Attach read-only and let the user retry.
                        session = client.get_browser_session(&session_id).await?;
                        Ok(BrowserAttachment {
                            session,
                            control_lease: None,
                            notice: Some(format!(
                                "Observer · could not take control automatically: {error:#}"
                            )),
                        })
                    }
                }
            }
            .await
            .map_err(|error| format!("Could not attach Browser: {error:#}"));
            let _ = tx.send_async(BrowserEvent::Attached(result)).await;
        });
    }

    fn reduce(&mut self, event: BrowserEvent, cx: &mut Context<Self>) {
        match event {
            BrowserEvent::Attached(Ok(attachment)) => {
                self.control_lease = attachment.control_lease;
                self.error = attachment
                    .notice
                    .or_else(|| attachment.session.last_error.clone());
                self.session = Some(attachment.session.clone());
                if browser_session_is_ended(&attachment.session) {
                    self.stop_stream();
                } else {
                    self.start_stream(attachment.session);
                }
            }
            BrowserEvent::Attached(Err(error)) => self.error = Some(error),
            BrowserEvent::Session(Ok(session)) => {
                self.error = session.last_error.clone();
                if browser_session_is_ended(&session) {
                    self.stop_stream();
                }
                self.session = Some(session);
            }
            BrowserEvent::Session(Err(error)) => self.error = Some(error),
            BrowserEvent::ControlTaken(Ok((session, lease))) => {
                self.taking_control = false;
                self.control_lease = Some(lease);
                self.error = session.last_error.clone();
                self.session = Some(session.clone());
                self.start_stream(session);
            }
            BrowserEvent::ControlTaken(Err(error)) => {
                self.taking_control = false;
                self.control_lease = None;
                self.error = Some(error);
            }
            BrowserEvent::ControlLost(reason) => {
                self.taking_control = false;
                self.control_lease = None;
                self.error = Some(reason);
                if let Some(session) = self.session.clone() {
                    self.start_stream(session);
                }
            }
            BrowserEvent::FrameReady => {
                let frame = self.frame_slot.lock().ok().and_then(|mut slot| slot.take());
                if let Some(frame) = frame {
                    self.send_protocol(json!({"type": "ack", "seq": frame.seq}));
                    self.frame = Some(frame);
                }
            }
            BrowserEvent::Connected => {
                self.connected = true;
                self.error = None;
            }
            BrowserEvent::Disconnected(reason) => {
                self.connected = false;
                self.error = Some(reason);
            }
            BrowserEvent::Ended(Ok(session)) => {
                self.stop_stream();
                self.confirm_end = false;
                self.control_lease = None;
                self.session = Some(session);
                self.error = Some("Browser session ended".to_owned());
            }
            BrowserEvent::Ended(Err(error)) => {
                self.confirm_end = false;
                self.error = Some(error);
            }
        }
        cx.notify();
    }

    fn start_stream(&mut self, session: BrowserSession) {
        self.stop_stream();
        let Some(stream_path) = session.stream_url else {
            self.error = Some(
                session
                    .last_error
                    .unwrap_or_else(|| "Browser session has no live frame stream".to_owned()),
            );
            return;
        };
        let lease_token = self.control_lease.as_ref().and_then(|lease| {
            (lease.resource_id == self.session_id && lease.client_id == self.client_id)
                .then(|| lease.lease_token.clone())
        });
        if self.control_lease.is_some() && lease_token.is_none() {
            self.control_lease = None;
        }
        let capability = if lease_token.is_some() {
            BrowserPresenceCapability::Controller
        } else {
            BrowserPresenceCapability::Viewer
        };
        let url = match self.client.browser_stream_url(
            &stream_path,
            capability,
            &self.client_id,
            lease_token.as_deref(),
        ) {
            Ok(url) => url,
            Err(error) => {
                self.error = Some(format!("Invalid Browser stream: {error:#}"));
                return;
            }
        };
        let (input_tx, input_rx) = browser_input_queue(INPUT_QUEUE_CAPACITY);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.input_tx = Some(input_tx);
        self.cancel_tx = Some(cancel_tx);
        let client = self.client.clone();
        let event_tx = self.event_tx.clone();
        let frame_slot = Arc::clone(&self.frame_slot);
        let session_id = self.session_id.clone();
        let client_id = self.client_id.clone();
        self.runtime.spawn(async move {
            run_stream(
                client,
                session_id,
                client_id,
                capability,
                lease_token,
                url,
                input_rx,
                cancel_rx,
                event_tx,
                frame_slot,
            )
            .await;
        });
    }

    fn stop_stream(&mut self) {
        if let Some(cancel) = self.cancel_tx.take() {
            let _ = cancel.send(true);
        }
        self.input_tx = None;
        self.connected = false;
        self.frame = None;
        if let Ok(mut slot) = self.frame_slot.lock() {
            slot.take();
        }
    }

    fn send_protocol(&mut self, value: Value) {
        let Some(tx) = &self.input_tx else {
            return;
        };
        if tx.try_send(value).is_err() {
            self.error = Some("Browser stream queue is busy; retry shortly".to_owned());
        }
    }

    fn send_input(&mut self, value: Value) {
        if self.control_lease.is_none() {
            self.error =
                Some("Observer mode · choose Take control before sending Browser input".to_owned());
            return;
        }
        self.send_protocol(value);
    }

    fn navigate(&mut self, raw: String, cx: &mut Context<Self>) {
        let url = normalize_address(&raw);
        if !url.is_empty() {
            self.run_action(BrowserAction::Navigate { url }, cx);
        }
    }

    fn run_action(&mut self, action: BrowserAction, cx: &mut Context<Self>) {
        let Some(lease_token) = self
            .control_lease
            .as_ref()
            .map(|lease| lease.lease_token.clone())
        else {
            self.error =
                Some("Observer mode · choose Take control before changing this Browser".to_owned());
            cx.notify();
            return;
        };
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let tx = self.event_tx.clone();
        self.error = None;
        self.runtime.spawn(async move {
            let result = async {
                client
                    .run_browser_actions(&session_id, &lease_token, &[action])
                    .await?;
                client.get_browser_session(&session_id).await
            }
            .await
            .map_err(|error| format!("Browser action failed: {error:#}"));
            let _ = tx.send_async(BrowserEvent::Session(result)).await;
        });
        cx.notify();
    }

    fn take_control(&mut self, cx: &mut Context<Self>) {
        if self.taking_control {
            return;
        }
        self.taking_control = true;
        self.error = None;
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let client_id = self.client_id.clone();
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            let result = async {
                let response = client
                    .take_browser_control(&session_id, &TakeBrowserControlRequest { client_id })
                    .await?;
                let session = client.get_browser_session(&session_id).await?;
                Ok((session, response.lease))
            }
            .await
            .map_err(|error: anyhow::Error| format!("Could not take Browser control: {error:#}"));
            let _ = tx.send_async(BrowserEvent::ControlTaken(result)).await;
        });
        cx.notify();
    }

    fn end_session(&mut self, cx: &mut Context<Self>) {
        let Some(lease_token) = self
            .control_lease
            .as_ref()
            .map(|lease| lease.lease_token.clone())
        else {
            self.confirm_end = false;
            self.error =
                Some("Observer mode · take control before ending this Browser session".to_owned());
            cx.notify();
            return;
        };
        if !self.confirm_end {
            self.confirm_end = true;
            self.error = Some("Press End again to terminate this Browser session".to_owned());
            cx.notify();
            return;
        }
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            let result = client
                .stop_browser_session(&session_id, &lease_token)
                .await
                .map_err(|error| format!("Could not end Browser: {error:#}"));
            let _ = tx.send_async(BrowserEvent::Ended(result)).await;
        });
    }

    fn pointer_position(&self, position: gpui::Point<Pixels>) -> Option<(f32, f32)> {
        let bounds = self.content_bounds.lock().ok()?.as_ref().copied()?;
        let frame = self.frame.as_ref()?;
        map_contained_point(
            f32::from(bounds.size.width),
            f32::from(bounds.size.height),
            frame.device_width,
            frame.device_height,
            f32::from(position.x - bounds.origin.x),
            f32::from(position.y - bounds.origin.y),
        )
    }
}

impl Render for BrowserSurface {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.palette;
        let frame = self.frame.clone();
        let content_bounds = Arc::clone(&self.content_bounds);
        let click_focus = self.focus.clone();
        let key_focus = self.focus.clone();
        let has_control = self.control_lease.is_some();
        let status = self.error.clone().unwrap_or_else(|| match &self.session {
            Some(session) if session.status == BrowserSessionStatus::Starting => {
                "Starting server Browser…".to_owned()
            }
            Some(_) if self.connected && has_control => "Live · controller".to_owned(),
            Some(session) if self.connected => session
                .controller
                .as_ref()
                .map(|controller| format!("Live · observer · {} controls", controller.client_id))
                .unwrap_or_else(|| "Live · observer · control available".to_owned()),
            Some(_) => "Connecting Browser stream…".to_owned(),
            None => "Loading Browser session…".to_owned(),
        });

        div()
            .flex()
            .flex_col()
            .size_full()
            .min_h_0()
            .bg(palette.surface)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(5.0))
                    .h(px(38.0))
                    .px(px(7.0))
                    .border_b_1()
                    .border_color(palette.edge)
                    .child(
                        Button::new("browser-back")
                            .label("←")
                            .compact()
                            .on_click(cx.listener(|surface, _, _, cx| {
                                surface.run_action(BrowserAction::Back, cx);
                            })),
                    )
                    .child(
                        Button::new("browser-forward")
                            .label("→")
                            .compact()
                            .on_click(cx.listener(|surface, _, _, cx| {
                                surface.run_action(BrowserAction::Forward, cx);
                            })),
                    )
                    .child(Button::new("browser-reload").label("↻").compact().on_click(
                        cx.listener(|surface, _, _, cx| {
                            surface.run_action(BrowserAction::Reload, cx);
                        }),
                    ))
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .h(px(28.0))
                            .px(px(6.0))
                            .border_1()
                            .border_color(palette.edge)
                            .rounded(px(4.0))
                            .child(Input::new(&self.address).appearance(false)),
                    )
                    .when(!has_control, |bar| {
                        bar.child(
                            Button::new("browser-take-control")
                                .label(if self.taking_control {
                                    "Taking…"
                                } else {
                                    "Take control"
                                })
                                .compact()
                                .on_click(
                                    cx.listener(|surface, _, _, cx| surface.take_control(cx)),
                                ),
                        )
                    })
                    .child(
                        Button::new("browser-end")
                            .label(if self.confirm_end { "Confirm" } else { "End" })
                            .compact()
                            .on_click(cx.listener(|surface, _, _, cx| surface.end_session(cx))),
                    ),
            )
            .child(
                div()
                    .on_children_prepainted(move |bounds, _, _| {
                        if let Some(bounds) = bounds.first().copied() {
                            if let Ok(mut stored) = content_bounds.lock() {
                                *stored = Some(bounds);
                            }
                        }
                    })
                    .id("browser-frame")
                    .key_context("MitsuroBrowser")
                    .track_focus(&self.focus)
                    .flex()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_hidden()
                    .bg(palette.canvas)
                    .on_mouse_down(
                        gpui::MouseButton::Left,
                        cx.listener(move |surface, event: &gpui::MouseDownEvent, window, cx| {
                            let Some((x, y)) = surface.pointer_position(event.position) else {
                                return;
                            };
                            window.focus(&click_focus);
                            surface.send_input(json!({
                                "type": "input_mouse", "eventType": "mousePressed",
                                "x": x, "y": y, "button": "left", "clickCount": event.click_count
                            }));
                            cx.stop_propagation();
                        }),
                    )
                    .on_mouse_down(
                        gpui::MouseButton::Right,
                        cx.listener(|surface, event: &gpui::MouseDownEvent, _, cx| {
                            let Some((x, y)) = surface.pointer_position(event.position) else {
                                return;
                            };
                            surface.send_input(json!({
                                "type": "input_mouse", "eventType": "mousePressed",
                                "x": x, "y": y, "button": "right", "clickCount": event.click_count
                            }));
                            cx.stop_propagation();
                        }),
                    )
                    .on_mouse_up(
                        gpui::MouseButton::Left,
                        cx.listener(|surface, event: &gpui::MouseUpEvent, _, cx| {
                            let Some((x, y)) = surface.pointer_position(event.position) else {
                                return;
                            };
                            surface.send_input(json!({
                                "type": "input_mouse", "eventType": "mouseReleased",
                                "x": x, "y": y, "button": "left", "clickCount": event.click_count
                            }));
                            cx.stop_propagation();
                        }),
                    )
                    .on_mouse_up(
                        gpui::MouseButton::Right,
                        cx.listener(|surface, event: &gpui::MouseUpEvent, _, cx| {
                            let Some((x, y)) = surface.pointer_position(event.position) else {
                                return;
                            };
                            surface.send_input(json!({
                                "type": "input_mouse", "eventType": "mouseReleased",
                                "x": x, "y": y, "button": "right", "clickCount": event.click_count
                            }));
                            cx.stop_propagation();
                        }),
                    )
                    .on_mouse_move(cx.listener(|surface, event: &gpui::MouseMoveEvent, _, _| {
                        if surface.control_lease.is_none() {
                            return;
                        }
                        let Some((x, y)) = surface.pointer_position(event.position) else {
                            return;
                        };
                        let button = match event.pressed_button {
                            Some(gpui::MouseButton::Left) => "left",
                            Some(gpui::MouseButton::Right) => "right",
                            Some(gpui::MouseButton::Middle) => "middle",
                            _ => "none",
                        };
                        surface.send_input(json!({
                            "type": "input_mouse", "eventType": "mouseMoved",
                            "x": x, "y": y, "button": button
                        }));
                    }))
                    .on_scroll_wheel(cx.listener(
                        |surface, event: &gpui::ScrollWheelEvent, _, cx| {
                            let Some((x, y)) = surface.pointer_position(event.position) else {
                                return;
                            };
                            let delta = event.delta.pixel_delta(px(20.0));
                            surface.send_input(json!({
                                "type": "input_mouse", "eventType": "mouseWheel",
                                "x": x, "y": y,
                                "deltaX": f32::from(delta.x), "deltaY": f32::from(delta.y)
                            }));
                            cx.stop_propagation();
                        },
                    ))
                    .on_key_down(cx.listener(
                        move |surface, event: &gpui::KeyDownEvent, window, cx| {
                            if !key_focus.is_focused(window) {
                                return;
                            }
                            let key = event.keystroke.key.clone();
                            surface.send_input(json!({
                                "type": "input_keyboard", "eventType": "keyDown",
                                "key": key, "text": event.keystroke.key_char,
                                "modifiers": {
                                    "control": event.keystroke.modifiers.control,
                                    "alt": event.keystroke.modifiers.alt,
                                    "shift": event.keystroke.modifiers.shift,
                                    "meta": event.keystroke.modifiers.platform
                                }
                            }));
                            surface.send_input(json!({
                                "type": "input_keyboard", "eventType": "keyUp",
                                "key": event.keystroke.key
                            }));
                            cx.stop_propagation();
                        },
                    ))
                    .when_some(frame, |surface, frame| {
                        surface.child(
                            img(frame.image)
                                .size_full()
                                .object_fit(gpui::ObjectFit::Contain),
                        )
                    })
                    .when(self.frame.is_none(), |surface| {
                        surface.child(
                            div()
                                .flex()
                                .size_full()
                                .items_center()
                                .justify_center()
                                .text_sm()
                                .text_color(palette.text_muted)
                                .child(status.clone()),
                        )
                    }),
            )
            .child(
                div()
                    .h(px(24.0))
                    .px(px(8.0))
                    .flex()
                    .items_center()
                    .border_t_1()
                    .border_color(palette.edge)
                    .text_xs()
                    .text_color(if self.error.is_some() {
                        palette.brass
                    } else {
                        palette.text_muted
                    })
                    .child(status),
            )
    }
}

async fn run_stream(
    client: MitsuroClient,
    session_id: String,
    client_id: String,
    capability: BrowserPresenceCapability,
    lease_token: Option<String>,
    url: String,
    input_rx: BrowserInputReceiver,
    mut cancel_rx: watch::Receiver<bool>,
    event_tx: flume::Sender<BrowserEvent>,
    frame_slot: Arc<Mutex<Option<BrowserFrame>>>,
) {
    let heartbeat_client = client.clone();
    let heartbeat_session_id = session_id;
    let heartbeat_client_id = client_id;
    let heartbeat_lease_token = lease_token.clone();
    let stream_lease_token = lease_token;
    let heartbeat_events = event_tx.clone();
    let _heartbeat = AbortTask(tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let heartbeat = heartbeat_client
                .heartbeat_browser_session(
                    &heartbeat_session_id,
                    capability,
                    heartbeat_client_id.clone(),
                    heartbeat_lease_token.as_deref(),
                )
                .await;
            match heartbeat {
                Ok(session) => {
                    let _ = heartbeat_events.try_send(BrowserEvent::Session(Ok(session)));
                }
                Err(error) if capability == BrowserPresenceCapability::Controller => {
                    let _ = heartbeat_events
                        .send_async(BrowserEvent::ControlLost(format!(
                            "Browser control lease ended: {error:#}"
                        )))
                        .await;
                    return;
                }
                Err(_) => {}
            }
        }
    }));

    let mut consecutive_failures = 0_u32;
    loop {
        if *cancel_rx.borrow() {
            return;
        }
        let request = match client.browser_websocket_request(&url, stream_lease_token.as_deref()) {
            Ok(request) => request,
            Err(error) => {
                let _ = event_tx
                    .send_async(BrowserEvent::Disconnected(format!(
                        "Browser authentication failed: {error:#}"
                    )))
                    .await;
                return;
            }
        };
        match tokio_tungstenite::connect_async(request).await {
            Ok((socket, _)) => {
                let connected_at = Instant::now();
                let _ = event_tx.send_async(BrowserEvent::Connected).await;
                let (mut sink, mut stream) = socket.split();
                let _ = sink
                    .send(Message::Text(
                        json!({"type": "config", "pacing": "ack", "maxFps": 24}).to_string(),
                    ))
                    .await;
                let disconnect_reason = loop {
                    tokio::select! {
                        _ = cancel_rx.changed() => {
                            let _ = sink.send(Message::Close(None)).await;
                            return;
                        }
                        outgoing = input_rx.recv() => {
                            if sink.send(Message::Text(outgoing.to_string())).await.is_err() {
                                break "Browser stream write failed".to_owned();
                            }
                        }
                        incoming = stream.next() => {
                            let incoming = match incoming {
                                Some(Ok(incoming)) => incoming,
                                Some(Err(error)) => break format!("Browser stream read failed: {error}"),
                                None => break "Browser stream ended by the server".to_owned(),
                            };
                            if let Message::Text(text) = incoming {
                                if let Some(frame) = decode_frame(&text) {
                                    if let Ok(mut slot) = frame_slot.lock() {
                                        *slot = Some(frame);
                                    }
                                    let _ = event_tx.try_send(BrowserEvent::FrameReady);
                                }
                            }
                        }
                    }
                };
                let _ = event_tx
                    .send_async(BrowserEvent::Disconnected(disconnect_reason))
                    .await;
                consecutive_failures = next_browser_reconnect_failure(
                    consecutive_failures,
                    Some(connected_at.elapsed()),
                );
            }
            Err(error) => {
                consecutive_failures = next_browser_reconnect_failure(consecutive_failures, None);
                let _ = event_tx
                    .send_async(BrowserEvent::Disconnected(format!(
                        "Browser stream disconnected: {error}"
                    )))
                    .await;
            }
        }
        if consecutive_failures >= MAX_RECONNECT_ATTEMPTS as u32 {
            let _ = event_tx
                .send_async(BrowserEvent::Disconnected(
                    "Browser stream reconnect limit reached".to_owned(),
                ))
                .await;
            return;
        }
        let delay = browser_reconnect_delay(consecutive_failures);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel_rx.changed() => return,
        }
    }
}

fn next_browser_reconnect_failure(current: u32, connected_for: Option<Duration>) -> u32 {
    let base = if connected_for.is_some_and(|uptime| uptime >= HEALTHY_CONNECTION_RESET_AFTER) {
        0
    } else {
        current
    };
    base.saturating_add(1)
}

fn browser_reconnect_delay(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(5);
    Duration::from_millis((250_u64.saturating_mul(1_u64 << exponent)).min(5_000))
}

struct AbortTask(tokio::task::JoinHandle<()>);

impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn decode_frame(text: &str) -> Option<BrowserFrame> {
    let envelope: StreamEnvelope = serde_json::from_str(text).ok()?;
    if envelope.kind != "frame" || envelope.data.len() > MAX_FRAME_BASE64_BYTES {
        return None;
    }
    let metadata = envelope.metadata?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(envelope.data)
        .ok()?;
    Some(BrowserFrame {
        image: Arc::new(Image::from_bytes(ImageFormat::Jpeg, bytes)),
        seq: envelope.seq,
        device_width: metadata.device_width.max(1.0),
        device_height: metadata.device_height.max(1.0),
    })
}

fn normalize_address(raw: &str) -> String {
    let value = raw.trim();
    if value.is_empty() {
        String::new()
    } else if value.contains("://") || value.starts_with("about:") {
        value.to_owned()
    } else if value.contains('.') || value.starts_with("localhost") || value.starts_with("127.") {
        format!("http://{value}")
    } else {
        format!(
            "https://www.google.com/search?q={}",
            value.replace(' ', "+")
        )
    }
}

fn map_contained_point(
    container_width: f32,
    container_height: f32,
    content_width: f32,
    content_height: f32,
    local_x: f32,
    local_y: f32,
) -> Option<(f32, f32)> {
    if ![
        container_width,
        container_height,
        content_width,
        content_height,
        local_x,
        local_y,
    ]
    .into_iter()
    .all(f32::is_finite)
        || container_width <= 0.0
        || container_height <= 0.0
        || content_width <= 0.0
        || content_height <= 0.0
    {
        return None;
    }
    let scale = (container_width / content_width).min(container_height / content_height);
    let display_width = content_width * scale;
    let display_height = content_height * scale;
    let offset_x = (container_width - display_width) * 0.5;
    let offset_y = (container_height - display_height) * 0.5;
    let x = local_x - offset_x;
    let y = local_y - offset_y;
    if x < 0.0 || y < 0.0 || x > display_width || y > display_height {
        return None;
    }
    Some((x / scale, y / scale))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitsuro_client::{BrowserSessionKind, BrowserViewportMode};

    fn browser_session(status: BrowserSessionStatus) -> BrowserSession {
        BrowserSession {
            id: "browser-1".to_owned(),
            title: "Browser".to_owned(),
            project_dir: "/workspace".to_owned(),
            kind: BrowserSessionKind::Interactive,
            status,
            url: None,
            cdp_url: None,
            debug_port: None,
            stream_url: None,
            viewers: 0,
            controllers: 0,
            controller: None,
            last_error: None,
            created_at: "2026-08-31T00:00:00Z".to_owned(),
            updated_at: "2026-08-31T00:00:00Z".to_owned(),
            viewport_mode: BrowserViewportMode::Desktop,
        }
    }

    #[test]
    fn frame_decoder_rejects_non_frames_and_decodes_metadata() {
        assert!(decode_frame(r#"{"type":"status"}"#).is_none());
        let jpeg = base64::engine::general_purpose::STANDARD.encode([0xff, 0xd8, 0xff, 0xd9]);
        let frame = decode_frame(
            &json!({
                "type": "frame", "seq": 7, "data": jpeg,
                "metadata": {"deviceWidth": 390, "deviceHeight": 700}
            })
            .to_string(),
        )
        .expect("frame should decode");
        assert_eq!(frame.seq, 7);
        assert_eq!(frame.device_width, 390.0);
    }

    #[test]
    fn address_normalization_preserves_urls_and_searches_plain_text() {
        assert_eq!(normalize_address("localhost:5173"), "http://localhost:5173");
        assert_eq!(
            normalize_address("mitsuro canvas"),
            "https://www.google.com/search?q=mitsuro+canvas"
        );
    }

    #[test]
    fn contain_mapping_excludes_letterbox_and_maps_device_coordinates() {
        assert_eq!(
            map_contained_point(100.0, 100.0, 200.0, 100.0, 50.0, 50.0),
            Some((100.0, 50.0))
        );
        assert_eq!(
            map_contained_point(100.0, 100.0, 200.0, 100.0, 50.0, 10.0),
            None
        );
        assert_eq!(
            map_contained_point(100.0, 200.0, 100.0, 200.0, 100.0, 200.0),
            Some((100.0, 200.0))
        );
    }

    #[test]
    fn reconnect_backoff_and_flap_budget_are_bounded() {
        assert_eq!(browser_reconnect_delay(0), Duration::from_millis(250));
        assert_eq!(browser_reconnect_delay(1), Duration::from_millis(250));
        assert_eq!(browser_reconnect_delay(2), Duration::from_millis(500));
        assert_eq!(browser_reconnect_delay(99), Duration::from_secs(5));
        assert_eq!(next_browser_reconnect_failure(0, None), 1);
        assert_eq!(
            next_browser_reconnect_failure(7, Some(Duration::from_secs(1))),
            8
        );
        assert_eq!(
            next_browser_reconnect_failure(7, Some(HEALTHY_CONNECTION_RESET_AFTER)),
            1
        );
    }

    #[test]
    fn browser_error_is_ended_only_when_termination_is_certain() {
        assert!(browser_session_is_ended(&browser_session(
            BrowserSessionStatus::Error,
        )));
        assert!(browser_session_is_ended(&browser_session(
            BrowserSessionStatus::Error,
        )));
        assert!(browser_session_is_ended(&browser_session(
            BrowserSessionStatus::Stopped,
        )));
    }

    #[test]
    fn pointer_flood_uses_one_latest_slot_and_preserves_bounded_control_order() {
        let (sender, receiver) = browser_input_queue(3);
        let config = json!({"type": "config", "pacing": "ack"});
        let click = json!({
            "type": "input_mouse", "eventType": "mousePressed", "x": 10, "y": 20
        });
        let key = json!({
            "type": "input_keyboard", "eventType": "keyDown", "key": "Enter"
        });
        sender.try_send(config.clone()).expect("config fits");
        sender
            .try_send(json!({
                "type": "input_mouse", "eventType": "mouseMoved", "x": 0, "y": 0
            }))
            .expect("initial move fits");
        sender.try_send(click.clone()).expect("click fits");
        for coordinate in 1..=10_000 {
            sender
                .try_send(json!({
                    "type": "input_mouse", "eventType": "mouseMoved",
                    "x": coordinate, "y": coordinate
                }))
                .expect("pointer moves never overflow the ordered queue");
        }
        sender.try_send(key.clone()).expect("key fits");
        assert_eq!(sender.pending_counts(), (3, 1));
        assert!(sender
            .try_send(json!({
                "type": "input_mouse", "eventType": "mouseWheel", "deltaY": 1
            }))
            .is_err());

        assert_eq!(receiver.try_recv(), Some(config));
        assert_eq!(receiver.try_recv(), Some(click));
        let latest_move = receiver.try_recv().expect("latest move");
        assert!(is_pointer_move(&latest_move));
        assert_eq!(latest_move.get("x").and_then(Value::as_i64), Some(10_000));
        assert_eq!(receiver.try_recv(), Some(key));
        assert!(receiver.try_recv().is_none());
    }
}
