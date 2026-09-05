use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use gpui::prelude::FluentBuilder as _;
use gpui::{
    actions, div, img, linear_color_stop, linear_gradient, px, size, AnyElement, App,
    AppContext as _, Application, AssetSource, Background, Bounds, Context, Entity, FocusHandle,
    Focusable, Image, InteractiveElement as _, IntoElement, KeyBinding, MouseButton,
    MouseDownEvent, MouseMoveEvent, ObjectFit, ParentElement as _, Render, ScrollWheelEvent,
    SharedString, StatefulInteractiveElement as _, Styled as _, StyledImage as _, Task, Timer,
    Window, WindowBounds, WindowOptions,
};
use gpui_component::button::Button;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::popover::Popover;
use gpui_component::scroll::ScrollableElement as _;
use gpui_component::Disableable as _;
use mitsuro_client::{
    BrowseResponse, BrowserSession, BrowserSessionStatus, CanvasCollaborationApply,
    CanvasCollaborationResponse, CanvasCollaborationSnapshot, CanvasCollaborationState,
    CanvasCursor, CanvasGeometryPreview, CanvasGeometryPreviewRequest,
    CanvasNode as WireCanvasNode, CanvasNodeBinding, CanvasOperation, CanvasPresenceClient,
    CanvasPresenceHeartbeatRequest, CanvasPublishThrottle, CanvasResourceIntentResponse,
    CanvasResourceNodeRequest, CanvasResourceSpec, CanvasStreamEnvelope,
    CleanupCanvasResourceResponse, CreateCanvasRequest, CreateCanvasResourceRequest,
    CreateCanvasResourceResponse, DetachCanvasResourceRequest, DetachCanvasResourceResponse,
    MitsuroClient, PatchCanvasRequest, ProjectSummary, SessionInfo, SpatialCanvas,
    SpatialCanvasSurface, TerminalDescriptor, TerminalSize, WorldRect as WireWorldRect,
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use uuid::Uuid;

use crate::acceptance;
use crate::agent::{AgentSurface, AgentSurfaceEvent};
use crate::browser_surface::BrowserSurface;
use crate::preferences::{CanvasPersonalState, DesktopPreferences};
use crate::resource_journal::{
    self, ResourceCreateJournalEntry, ResourceJournalRemovalMode, ResourceJournalRemoveOutcome,
};
use crate::resources::{ChangesSurface, FilesSurface};
use crate::server_supervisor::{
    BootstrapFailure, BootstrapFailureKind, BootstrapOutcome, ServerConnection, ServerSupervisor,
};
use crate::spatial::{
    semantic_level, snap_world, Camera, NodeKind, NodeStatus, SemanticLevel, SpatialNode,
    StageLayout, WorldPoint, WorldRect, WorldSize,
};
use crate::terminal_surface::{TerminalSurface, TerminalSurfaceConnection};
use crate::theme::{
    parse_theme_color, BackgroundPreferencesV1, BackgroundSpecV1, CanvasPalette, GradientStopV1,
    MotionPreference, StaticBackgroundV1, ThemeCatalog, ValidatedThemeImage, VisualThemeV1,
    DEFAULT_THEME_ID, PULSE_EFFECT_ID,
};

const TITLE_HEIGHT: f32 = 34.0;
const PULSE_RAIL_WIDTH: f32 = 46.0;
const PULSE_RAIL_EXPANDED_WIDTH: f32 = 196.0;
const STAGE_RAIL_WIDTH: f32 = 48.0;
const COMMAND_SHELF_WIDTH: f32 = 820.0;
const CANVAS_OVERLAY_TOP: f32 = 8.0;
const CANVAS_OVERLAY_BOTTOM: f32 = 72.0;
const GRID_WORLD: f64 = 20.0;
const MAX_QUEUED_PATCH_OPERATIONS: usize = 256;
const MAX_NATIVE_BROWSER_SESSIONS: usize = 4;
const DRAFT_PERSIST_DELAY: Duration = Duration::from_millis(250);
const MAX_RESOURCE_CREATE_ATTEMPTS: u8 = 8;
const MAX_RESOURCE_POLL_ATTEMPTS: u8 = 8;

actions!(
    mitsuro_canvas,
    [
        FitAll,
        ResetZoom,
        FocusCommandShelf,
        NewAgent,
        NewTerminal,
        NewBrowser,
        NewFiles,
        NewChanges,
        StageOne,
        StageTwo,
        StageFour
    ]
);

pub fn run() {
    Application::new()
        .with_assets(CanvasAssets)
        .run(|cx: &mut App| {
            gpui_component::init(cx);
            gpui_component::Theme::change(gpui_component::ThemeMode::Dark, None, cx);
            apply_component_theme(CanvasPalette::default(), cx);
            cx.bind_keys([
                KeyBinding::new("ctrl-shift-f", FitAll, Some("Canvas")),
                KeyBinding::new("ctrl-0", ResetZoom, Some("Canvas")),
                KeyBinding::new("ctrl-l", FocusCommandShelf, Some("Canvas")),
                KeyBinding::new("ctrl-1", StageOne, Some("Canvas")),
                KeyBinding::new("ctrl-2", StageTwo, Some("Canvas")),
                KeyBinding::new("ctrl-4", StageFour, Some("Canvas")),
            ]);

            let bounds = Bounds::centered(None, size(px(1280.0), px(840.0)), cx);
            let window = cx.open_window(
                WindowOptions {
                    window_bounds: Some(WindowBounds::Windowed(bounds)),
                    titlebar: Some(gpui_component::TitleBar::title_bar_options()),
                    app_id: Some("io.mitsuro.canvas".to_owned()),
                    ..Default::default()
                },
                |window, cx| {
                    window.set_window_title("Mitsuro Canvas");
                    window.set_rem_size(px(16.0));
                    let app = cx.new(|cx| CanvasApp::new(window, cx));
                    cx.new(|cx| gpui_component::Root::new(app, window, cx))
                },
            );
            if let Err(error) = window {
                eprintln!("failed to open Mitsuro Canvas: {error}");
            }
        });
}

fn apply_component_theme(palette: CanvasPalette, cx: &mut App) {
    let theme = gpui_component::Theme::global_mut(cx);
    theme.font_size = px(15.0);
    theme.mono_font_size = px(13.5);
    theme.radius = px(4.0);
    theme.radius_lg = px(4.0);
    theme.shadow = false;
    theme.background = palette.canvas;
    theme.foreground = palette.text;
    theme.border = palette.edge;
    theme.input = palette.surface;
    theme.ring = palette.pulse;
    theme.selection = palette.pulse_soft;
    theme.muted = palette.surface;
    theme.muted_foreground = palette.text_muted;
    theme.popover = palette.surface_high;
    theme.popover_foreground = palette.text;
    theme.primary = palette.pulse;
    theme.primary_foreground = palette.canvas;
    theme.secondary = palette.surface_high;
    theme.secondary_foreground = palette.text;
    theme.danger = palette.danger;
}

struct CanvasAssets;

impl AssetSource for CanvasAssets {
    fn load(&self, path: &str) -> gpui::Result<Option<Cow<'static, [u8]>>> {
        if path.is_empty() {
            return Ok(None);
        }
        for candidate in asset_candidates(path) {
            if candidate.is_file() {
                return std::fs::read(candidate)
                    .map(|bytes| Some(Cow::Owned(bytes)))
                    .map_err(Into::into);
            }
        }
        Ok(None)
    }

    fn list(&self, _path: &str) -> gpui::Result<Vec<SharedString>> {
        Ok(Vec::new())
    }
}

fn asset_candidates(path: &str) -> Vec<PathBuf> {
    let relative = PathBuf::from(path);
    if relative.is_absolute() {
        return vec![relative];
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut candidates = vec![manifest.join("assets").join(&relative)];
    if let Ok(executable) = std::env::current_exe() {
        if let Some(bin) = executable.parent() {
            candidates.push(bin.join("assets").join(&relative));
            if let Some(prefix) = bin.parent() {
                candidates.push(prefix.join("share/mitsuro-canvas/assets").join(&relative));
            }
        }
    }
    candidates
}

enum ConnectionState {
    Connecting,
    Ready(ServerConnection),
    Blocked(BootstrapFailure),
    Harness,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanvasAsyncScope {
    generation: u64,
    canvas_id: String,
}

impl CanvasAsyncScope {
    fn is_current(&self, generation: u64, canvas_id: Option<&str>) -> bool {
        self.generation == generation && canvas_id == Some(self.canvas_id.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DraftStoreResult {
    Stale,
    Stored,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScrollStoreResult {
    Stale,
    Stored,
    Full,
}

fn store_agent_draft_if_current(
    state: &mut CanvasPersonalState,
    scope: &CanvasAsyncScope,
    generation: u64,
    canvas_id: Option<&str>,
    node_id: Uuid,
    draft: Option<&str>,
) -> DraftStoreResult {
    if !scope.is_current(generation, canvas_id) {
        return DraftStoreResult::Stale;
    }
    if state.set_draft(node_id, draft.unwrap_or_default()) {
        DraftStoreResult::Stored
    } else {
        DraftStoreResult::Full
    }
}

fn store_agent_scroll_if_current(
    state: &mut CanvasPersonalState,
    scope: &CanvasAsyncScope,
    generation: u64,
    canvas_id: Option<&str>,
    node_id: Uuid,
    offset: f32,
) -> ScrollStoreResult {
    if !scope.is_current(generation, canvas_id) {
        return ScrollStoreResult::Stale;
    }
    if state.set_scroll_offset(node_id, offset) {
        ScrollStoreResult::Stored
    } else {
        ScrollStoreResult::Full
    }
}

fn reserve_browser_create(active: usize, in_flight: &mut usize) -> bool {
    if active.saturating_add(*in_flight) >= MAX_NATIVE_BROWSER_SESSIONS {
        return false;
    }
    *in_flight = in_flight.saturating_add(1);
    true
}

fn complete_browser_create_if_current(
    in_flight: &mut usize,
    scope: &CanvasAsyncScope,
    generation: u64,
    canvas_id: Option<&str>,
) -> bool {
    if !scope.is_current(generation, canvas_id) {
        return false;
    }
    *in_flight = in_flight.saturating_sub(1);
    true
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ResourceTaskKey {
    canvas_id: String,
    create_op_id: String,
}

impl ResourceTaskKey {
    fn new(canvas_id: impl Into<String>, create_op_id: impl Into<String>) -> Self {
        Self {
            canvas_id: canvas_id.into(),
            create_op_id: create_op_id.into(),
        }
    }
}

#[derive(Debug)]
struct ResourceCreateTaskResult {
    response: std::result::Result<CreateCanvasResourceResponse, String>,
    request_attempted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResourceJournalRemovalPurpose {
    Normal,
    ConfirmedCleanup,
    ConfirmedDetach,
}

impl ResourceJournalRemovalPurpose {
    fn mode(self) -> ResourceJournalRemovalMode {
        match self {
            Self::Normal => ResourceJournalRemovalMode::Normal,
            Self::ConfirmedCleanup => ResourceJournalRemovalMode::ConfirmedCleanup,
            Self::ConfirmedDetach => ResourceJournalRemovalMode::ConfirmedDetach,
        }
    }
}

fn resource_poll_delay(attempt: u8) -> Duration {
    let exponent = attempt.saturating_sub(1).min(4) as u32;
    Duration::from_millis((250_u64 << exponent).min(4_000))
}

fn resource_status_is_terminal(status: &str) -> bool {
    matches!(status, "cleaned" | "abandoned")
}

fn resource_status_needs_poll(status: &str) -> bool {
    matches!(
        status,
        "reserved" | "provisioning" | "provisioned" | "failed" | "ambiguous" | "cleaning"
    )
}

fn resource_status_is_bound(status: &str) -> bool {
    status == "bound"
}

fn shelf_restore_is_safe(current_value: &str) -> bool {
    current_value.trim().is_empty()
}

fn placeholder_resource_intent(
    request: CreateCanvasResourceRequest,
    status: &str,
    last_error: Option<String>,
    detach_generation: u64,
) -> CanvasResourceIntentResponse {
    CanvasResourceIntentResponse {
        request,
        status: status.to_owned(),
        last_error,
        retry_requires_confirmation: status == "ambiguous",
        initial_prompt_completed: false,
        detach_generation,
    }
}

fn resource_spec_kind(resource: &CanvasResourceSpec) -> NodeKind {
    match resource {
        CanvasResourceSpec::Agent => NodeKind::Agent,
        CanvasResourceSpec::Terminal { .. } => NodeKind::Terminal,
        CanvasResourceSpec::Browser { .. } => NodeKind::Browser,
    }
}

fn resource_spec_title(resource: &CanvasResourceSpec) -> &'static str {
    match resource {
        CanvasResourceSpec::Agent => "Agent",
        CanvasResourceSpec::Terminal { .. } => "Terminal",
        CanvasResourceSpec::Browser { .. } => "Browser",
    }
}

fn abort_join_tasks(tasks: impl IntoIterator<Item = tokio::task::JoinHandle<()>>) {
    for task in tasks {
        task.abort();
    }
}

enum UiEvent {
    Bootstrap(std::result::Result<ServerConnection, BootstrapFailure>),
    PreferencesLoaded(std::result::Result<DesktopPreferences, String>),
    PreferencesSaved {
        generation: u64,
        result: std::result::Result<(), String>,
    },
    ThemeCatalogReloaded(ThemeCatalog),
    BackgroundImageLoaded {
        generation: u64,
        theme_id: String,
        asset: String,
        result: std::result::Result<Option<ValidatedThemeImage>, String>,
    },
    StartupLoaded(std::result::Result<StartupData, String>),
    CanvasOpened {
        request_generation: u64,
        result: std::result::Result<SpatialCanvas, String>,
    },
    CanvasStream {
        scope: CanvasAsyncScope,
        envelope: CanvasStreamEnvelope,
    },
    CanvasStreamFailed {
        scope: CanvasAsyncScope,
        error: String,
    },
    CanvasStreamConnected {
        scope: CanvasAsyncScope,
        resumed_after: u64,
    },
    CanvasPatched {
        scope: CanvasAsyncScope,
        result: std::result::Result<SpatialCanvas, String>,
        attempted: Vec<CanvasOperation>,
    },
    CollaborationSnapshot {
        scope: CanvasAsyncScope,
        result: std::result::Result<CanvasCollaborationResponse, String>,
    },
    CursorPublished {
        scope: CanvasAsyncScope,
        result: std::result::Result<CanvasCollaborationResponse, String>,
    },
    GeometryPreviewPublished {
        scope: CanvasAsyncScope,
        node_id: Uuid,
        result: std::result::Result<CanvasCollaborationResponse, String>,
    },
    GeometryPreviewReleased {
        scope: CanvasAsyncScope,
        node_id: Uuid,
        result: std::result::Result<CanvasCollaborationResponse, String>,
    },
    BrowseLoaded {
        request_generation: u64,
        result: std::result::Result<BrowseResponse, String>,
    },
    #[allow(dead_code)]
    TerminalCreated {
        scope: CanvasAsyncScope,
        result: std::result::Result<TerminalDescriptor, String>,
    },
    TerminalLoaded {
        scope: CanvasAsyncScope,
        node_id: Uuid,
        terminal_id: String,
        result: std::result::Result<TerminalDescriptor, String>,
    },
    #[allow(dead_code)]
    BrowserCreated {
        scope: CanvasAsyncScope,
        result: std::result::Result<BrowserSession, String>,
    },
    #[allow(dead_code)]
    SessionCreated {
        scope: CanvasAsyncScope,
        result: std::result::Result<SessionInfo, String>,
        prompt: Option<String>,
    },
    ResourceJournalPersisted {
        key: ResourceTaskKey,
        request: CreateCanvasResourceRequest,
        result: std::result::Result<ResourceCreateJournalEntry, String>,
    },
    ResourceJournalRemoved {
        key: ResourceTaskKey,
        purpose: ResourceJournalRemovalPurpose,
        result: std::result::Result<ResourceJournalRemoveOutcome, String>,
    },
    ResourceJournalCancellationPersisted {
        key: ResourceTaskKey,
        result: std::result::Result<ResourceCreateJournalEntry, String>,
    },
    ResourceCreateFinished {
        key: ResourceTaskKey,
        request: CreateCanvasResourceRequest,
        result: ResourceCreateTaskResult,
    },
    ResourceCleanupFinished {
        key: ResourceTaskKey,
        result: std::result::Result<CleanupCanvasResourceResponse, String>,
    },
    ResourceDetachFinished {
        key: ResourceTaskKey,
        result: std::result::Result<DetachCanvasResourceResponse, String>,
    },
    ResourceIntentsListed {
        canvas_id: String,
        result: std::result::Result<Vec<CanvasResourceIntentResponse>, String>,
    },
}

struct StartupData {
    projects: Vec<ProjectSummary>,
    canvases: Vec<SpatialCanvas>,
    selected: Option<SpatialCanvas>,
    journal_entries: Vec<ResourceCreateJournalEntry>,
    journal_error: Option<String>,
    list_error: Option<String>,
}

struct PendingAgentMount {
    node_id: Uuid,
    session_id: String,
    project_dir: String,
    title: String,
    prompt: Option<String>,
    add_node: bool,
}

#[derive(Clone)]
struct LoadedBackgroundImage {
    theme_id: String,
    asset: String,
    image: Arc<Image>,
}

enum PointerGesture {
    Pan {
        last: WorldPoint,
    },
    Move {
        node_id: Uuid,
        grab_offset: WorldPoint,
        initial: WorldRect,
        initial_z: i64,
    },
    Resize {
        node_id: Uuid,
        anchor: WorldPoint,
        initial: WorldRect,
        initial_z: i64,
    },
}

pub struct CanvasApp {
    focus: FocusHandle,
    runtime: Arc<Runtime>,
    connection: ConnectionState,
    command: Entity<InputState>,
    canvas_name_input: Entity<InputState>,
    nodes: Vec<SpatialNode>,
    agents: BTreeMap<Uuid, Entity<AgentSurface>>,
    files: BTreeMap<Uuid, Entity<FilesSurface>>,
    changes: BTreeMap<Uuid, Entity<ChangesSurface>>,
    browsers: BTreeMap<Uuid, Entity<BrowserSurface>>,
    terminals: BTreeMap<Uuid, Entity<TerminalSurface>>,
    terminal_loads: BTreeMap<Uuid, String>,
    browser_mount_waiting: BTreeSet<Uuid>,
    browser_mount_retired: BTreeSet<Uuid>,
    pending_terminals: Vec<(Uuid, TerminalDescriptor)>,
    pending_agents: Vec<PendingAgentMount>,
    projects: Vec<ProjectSummary>,
    canvases: Vec<SpatialCanvas>,
    browse: Option<BrowseResponse>,
    project_dir: Option<String>,
    project_name: String,
    canvas_id: Option<String>,
    canvas_name: String,
    shared_theme_id: String,
    canvas_revision: u64,
    canvas_generation: u64,
    client_id: String,
    browser_creates_in_flight: usize,
    browser_create_gate: Arc<tokio::sync::Mutex<()>>,
    patch_queue: VecDeque<Vec<CanvasOperation>>,
    patch_in_flight: bool,
    patch_task: Option<tokio::task::JoinHandle<()>>,
    reconciliation_task: Option<tokio::task::JoinHandle<()>>,
    canvas_open_task: Option<tokio::task::JoinHandle<()>>,
    canvas_bound_tasks: Vec<tokio::task::JoinHandle<()>>,
    canvas_stream_task: Option<tokio::task::JoinHandle<()>>,
    presence_task: Option<tokio::task::JoinHandle<()>>,
    presence_token: Option<String>,
    presence_token_updates: Option<tokio::sync::watch::Sender<Option<String>>>,
    collaboration: CanvasCollaborationState,
    collaboration_started_at: Instant,
    cursor_throttle: CanvasPublishThrottle,
    cursor_in_flight: bool,
    pending_cursor: Option<CanvasCursor>,
    preview_throttle: CanvasPublishThrottle,
    preview_in_flight: BTreeSet<Uuid>,
    preview_releases_in_flight: BTreeSet<Uuid>,
    preview_acquired: BTreeSet<Uuid>,
    pending_previews: BTreeMap<Uuid, (WireWorldRect, i64)>,
    pending_geometry_commits: BTreeMap<Uuid, (WireWorldRect, i64)>,
    geometry_commits_persisted: BTreeSet<Uuid>,
    pending_preview_releases: BTreeMap<Uuid, bool>,
    camera: Camera,
    selected_node: Option<Uuid>,
    gesture: Option<PointerGesture>,
    gesture_active: bool,
    wheel_settle_task: Option<Task<()>>,
    stage_layout: StageLayout,
    stage_slots: Vec<Uuid>,
    pulse_open: bool,
    pulse_hovered: bool,
    personal_persist_task: Option<Task<()>>,
    status: SharedString,
    preferences: DesktopPreferences,
    preferences_loaded: bool,
    pending_startup_data: Option<StartupData>,
    preferences_load_task: Option<tokio::task::JoinHandle<()>>,
    preferences_write_generation: u64,
    preferences_write_task: Option<tokio::task::JoinHandle<()>>,
    pending_preferences_write: Option<DesktopPreferences>,
    themes: ThemeCatalog,
    background_preferences: BackgroundPreferencesV1,
    background_image: Option<LoadedBackgroundImage>,
    background_image_generation: u64,
    background_image_task: Option<tokio::task::JoinHandle<()>>,
    motion: MotionPreference,
    switcher_open: bool,
    aura_phase: f32,
    window_active: bool,
    animation_tick: u64,
    last_input_activity: Instant,
    journaled_resource_requests: BTreeMap<ResourceTaskKey, ResourceCreateJournalEntry>,
    resource_intents: BTreeMap<String, BTreeMap<String, CanvasResourceIntentResponse>>,
    resource_journal_write_tasks: BTreeMap<ResourceTaskKey, tokio::task::JoinHandle<()>>,
    resource_journal_remove_tasks: BTreeMap<ResourceTaskKey, tokio::task::JoinHandle<()>>,
    resource_journal_cancel_tasks: BTreeMap<ResourceTaskKey, tokio::task::JoinHandle<()>>,
    resource_journal_remove_attempts: BTreeMap<ResourceTaskKey, u8>,
    resource_create_tasks: BTreeMap<ResourceTaskKey, tokio::task::JoinHandle<()>>,
    resource_cleanup_tasks: BTreeMap<ResourceTaskKey, tokio::task::JoinHandle<()>>,
    resource_detach_tasks: BTreeMap<ResourceTaskKey, tokio::task::JoinHandle<()>>,
    resource_list_tasks: BTreeMap<String, tokio::task::JoinHandle<()>>,
    resource_local_cancellations: BTreeSet<ResourceTaskKey>,
    resource_durable_cancellations: BTreeSet<ResourceTaskKey>,
    resource_network_may_have_started: BTreeSet<ResourceTaskKey>,
    server_owned_resource_intents: BTreeSet<ResourceTaskKey>,
    resource_journal_available: bool,
    resource_journal_gate: Arc<tokio::sync::Mutex<()>>,
    resource_cleanup_attempts: BTreeMap<ResourceTaskKey, u8>,
    resource_create_attempts: BTreeMap<ResourceTaskKey, u8>,
    resource_detach_attempts: BTreeMap<ResourceTaskKey, u8>,
    resource_poll_attempts: BTreeMap<String, u8>,
    pending_shelf_restore: Option<String>,
    event_tx: flume::Sender<UiEvent>,
    _event_listener: Task<()>,
    _animation_task: Task<()>,
    _theme_reload_task: tokio::task::JoinHandle<()>,
}

impl Drop for CanvasApp {
    fn drop(&mut self) {
        let _ = self.personal_persist_task.take();
        if let Some(task) = self.preferences_load_task.take() {
            task.abort();
        }
        if !self.preferences_loaded {
            self.preferences = DesktopPreferences::load_default().unwrap_or_default();
            self.preferences_loaded = true;
        }
        self.capture_personal_state();
        if let Some(task) = self.preferences_write_task.take() {
            let _ = self.runtime.block_on(task);
        }
        let _ = self.preferences.save_default();
        self._theme_reload_task.abort();
        if let Some(task) = self.background_image_task.take() {
            task.abort();
        }
        if let Some(task) = self.canvas_stream_task.take() {
            task.abort();
        }
        if let Some(task) = self.presence_task.take() {
            task.abort();
        }
        if let Some(task) = self.patch_task.take() {
            task.abort();
        }
        if let Some(task) = self.reconciliation_task.take() {
            task.abort();
        }
        if let Some(task) = self.canvas_open_task.take() {
            task.abort();
        }
        for task in self.canvas_bound_tasks.drain(..) {
            task.abort();
        }
        abort_join_tasks(std::mem::take(&mut self.resource_journal_write_tasks).into_values());
        abort_join_tasks(std::mem::take(&mut self.resource_journal_remove_tasks).into_values());
        abort_join_tasks(std::mem::take(&mut self.resource_journal_cancel_tasks).into_values());
        abort_join_tasks(std::mem::take(&mut self.resource_create_tasks).into_values());
        abort_join_tasks(std::mem::take(&mut self.resource_cleanup_tasks).into_values());
        abort_join_tasks(std::mem::take(&mut self.resource_detach_tasks).into_values());
        abort_join_tasks(std::mem::take(&mut self.resource_list_tasks).into_values());
        let Some(client) = self.client().cloned() else {
            return;
        };
        let Some(canvas_id) = self.canvas_id.clone() else {
            return;
        };
        let Some(presence_token) = self.presence_token.clone() else {
            return;
        };
        let client_id = self.client_id.clone();
        let _ = std::thread::Builder::new()
            .name("mitsuro-canvas-leave".to_owned())
            .spawn(move || {
                let runtime = RuntimeBuilder::new_current_thread().enable_all().build();
                if let Ok(runtime) = runtime {
                    let _ = runtime.block_on(client.leave_canvas_presence(
                        &canvas_id,
                        &client_id,
                        &presence_token,
                    ));
                }
            });
    }
}

impl CanvasApp {
    fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let runtime = Arc::new(
            RuntimeBuilder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("mitsuro-canvas")
                .build()
                .expect("building Mitsuro Canvas runtime"),
        );
        let command = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Ask the focused Agent, or start something new")
                .clean_on_escape()
        });
        let canvas_name_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("New canvas name")
                .clean_on_escape()
        });
        cx.subscribe_in(
            &command,
            window,
            |app, input, event: &InputEvent, window, cx| {
                app.last_input_activity = Instant::now();
                if matches!(event, InputEvent::PressEnter { secondary: false }) {
                    app.submit_shelf(input, window, cx);
                }
            },
        )
        .detach();
        cx.subscribe_in(
            &canvas_name_input,
            window,
            |app, _, event: &InputEvent, window, cx| {
                app.last_input_activity = Instant::now();
                if matches!(event, InputEvent::PressEnter { secondary: false }) {
                    app.create_named_canvas_from_input(window, cx);
                }
            },
        )
        .detach();

        let (event_tx, event_rx) = flume::bounded(64);
        let event_listener = cx.spawn_in(window, async move |this, cx| {
            while let Ok(event) = event_rx.recv_async().await {
                let update = cx.update(|window, cx| {
                    this.update(cx, |app, cx| {
                        let previous_palette = app.active_palette();
                        app.reduce_event(event);
                        app.restore_pending_shelf_command(window, cx);
                        app.materialize_pending_agents(window, cx);
                        app.materialize_pending_terminals(window, cx);
                        app.materialize_resource_surfaces(window, cx);
                        app.sync_agent_selection(cx);
                        if let Some(palette) =
                            changed_palette(previous_palette, app.active_palette())
                        {
                            app.sync_surface_palettes(palette, cx);
                        }
                        cx.notify();
                    })
                });
                if !matches!(update, Ok(Ok(()))) {
                    break;
                }
            }
        });

        #[cfg(feature = "component-harness")]
        let harness = true;
        #[cfg(not(feature = "component-harness"))]
        let harness = false;
        if !harness {
            let tx = event_tx.clone();
            runtime.spawn(async move {
                let _ = tx
                    .send_async(UiEvent::Bootstrap(ServerSupervisor::connect().await))
                    .await;
            });
        }

        let animation_task = cx.spawn(async move |this, cx| loop {
            Timer::after(Duration::from_millis(33)).await;
            if this
                .update(cx, |app, cx| {
                    let fps = app.motion.frames_per_second();
                    app.animation_tick = app.animation_tick.wrapping_add(1);
                    let economy_skip = fps == 15 && app.animation_tick % 2 == 1;
                    if fps > 0
                        && !economy_skip
                        && app.window_active
                        && app.active_effect_speed().is_some()
                        && !app.background_should_yield(cx)
                    {
                        let speed = app.active_effect_speed().unwrap_or_default();
                        app.aura_phase = (app.aura_phase + 0.0025 + speed * 0.01) % 1.0;
                        cx.notify();
                    }
                })
                .is_err()
            {
                break;
            }
        });

        // Compiled-in themes are pure data and safe to install synchronously.
        // User packs may contain large validated images, so their catalog and
        // assets are always loaded on the blocking runtime.
        let themes = ThemeCatalog::builtins();
        let theme_tx = event_tx.clone();
        let theme_reload_task = {
            let mut last_catalog = themes.clone();
            let theme_root = ThemeCatalog::root();
            runtime.spawn(async move {
                let initial_root = theme_root.clone();
                if let Ok(next_catalog) =
                    tokio::task::spawn_blocking(move || ThemeCatalog::load(&initial_root)).await
                {
                    if next_catalog != last_catalog {
                        last_catalog = next_catalog.clone();
                        if theme_tx
                            .send_async(UiEvent::ThemeCatalogReloaded(next_catalog))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                let initial_fingerprint_root = theme_root.clone();
                let mut last_fingerprint = tokio::task::spawn_blocking(move || {
                    ThemeCatalog::watch_fingerprint(&initial_fingerprint_root)
                })
                .await
                .unwrap_or_default();
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let fingerprint_root = theme_root.clone();
                    let Ok(next_fingerprint) = tokio::task::spawn_blocking(move || {
                        ThemeCatalog::watch_fingerprint(&fingerprint_root)
                    })
                    .await
                    else {
                        continue;
                    };
                    if next_fingerprint == last_fingerprint {
                        continue;
                    }
                    last_fingerprint = next_fingerprint;
                    let catalog_root = theme_root.clone();
                    let loaded =
                        tokio::task::spawn_blocking(move || ThemeCatalog::load(&catalog_root))
                            .await;
                    let Ok(next_catalog) = loaded else {
                        continue;
                    };
                    if next_catalog == last_catalog {
                        continue;
                    }
                    last_catalog = next_catalog.clone();
                    if theme_tx
                        .send_async(UiEvent::ThemeCatalogReloaded(next_catalog))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            })
        };

        let preferences = DesktopPreferences::default();
        let preferences_loaded = harness;
        let preferences_load_task = (!harness).then(|| {
            let tx = event_tx.clone();
            runtime.spawn(async move {
                let result = tokio::task::spawn_blocking(DesktopPreferences::load_default)
                    .await
                    .map_err(|error| format!("personal state task failed: {error}"))
                    .and_then(|result| result.map_err(|error| format!("{error:#}")));
                let _ = tx.send_async(UiEvent::PreferencesLoaded(result)).await;
            })
        });
        let background_preferences = BackgroundPreferencesV1::default();
        let motion = background_preferences.motion;
        let app = Self {
            focus: cx.focus_handle(),
            runtime,
            connection: if harness {
                ConnectionState::Harness
            } else {
                ConnectionState::Connecting
            },
            command,
            canvas_name_input,
            nodes: Vec::new(),
            agents: BTreeMap::new(),
            files: BTreeMap::new(),
            changes: BTreeMap::new(),
            browsers: BTreeMap::new(),
            terminals: BTreeMap::new(),
            terminal_loads: BTreeMap::new(),
            browser_mount_waiting: BTreeSet::new(),
            browser_mount_retired: BTreeSet::new(),
            pending_terminals: Vec::new(),
            pending_agents: Vec::new(),
            projects: Vec::new(),
            canvases: Vec::new(),
            browse: None,
            project_dir: None,
            project_name: "No project".to_owned(),
            canvas_id: None,
            canvas_name: "Main".to_owned(),
            shared_theme_id: DEFAULT_THEME_ID.to_owned(),
            canvas_revision: 0,
            canvas_generation: 0,
            client_id: Uuid::new_v4().to_string(),
            browser_creates_in_flight: 0,
            browser_create_gate: Arc::new(tokio::sync::Mutex::new(())),
            patch_queue: VecDeque::new(),
            patch_in_flight: false,
            patch_task: None,
            reconciliation_task: None,
            canvas_open_task: None,
            canvas_bound_tasks: Vec::new(),
            canvas_stream_task: None,
            presence_task: None,
            presence_token: None,
            presence_token_updates: None,
            collaboration: CanvasCollaborationState::default(),
            collaboration_started_at: Instant::now(),
            cursor_throttle: CanvasPublishThrottle::new(Duration::from_millis(50)),
            cursor_in_flight: false,
            pending_cursor: None,
            preview_throttle: CanvasPublishThrottle::new(Duration::from_millis(50)),
            preview_in_flight: BTreeSet::new(),
            preview_releases_in_flight: BTreeSet::new(),
            preview_acquired: BTreeSet::new(),
            pending_previews: BTreeMap::new(),
            pending_geometry_commits: BTreeMap::new(),
            geometry_commits_persisted: BTreeSet::new(),
            pending_preview_releases: BTreeMap::new(),
            camera: Camera::default(),
            selected_node: None,
            gesture: None,
            gesture_active: false,
            wheel_settle_task: None,
            stage_layout: StageLayout::One,
            stage_slots: Vec::new(),
            pulse_open: false,
            pulse_hovered: false,
            personal_persist_task: None,
            status: if harness {
                "Component harness · no server data".into()
            } else {
                "Finding the canonical Mitsuro server…".into()
            },
            preferences,
            preferences_loaded,
            pending_startup_data: None,
            preferences_load_task,
            preferences_write_generation: 0,
            preferences_write_task: None,
            pending_preferences_write: None,
            themes,
            background_preferences,
            background_image: None,
            background_image_generation: 0,
            background_image_task: None,
            motion,
            switcher_open: false,
            aura_phase: 0.0,
            window_active: true,
            animation_tick: 0,
            last_input_activity: Instant::now(),
            journaled_resource_requests: BTreeMap::new(),
            resource_intents: BTreeMap::new(),
            resource_journal_write_tasks: BTreeMap::new(),
            resource_journal_remove_tasks: BTreeMap::new(),
            resource_journal_cancel_tasks: BTreeMap::new(),
            resource_journal_remove_attempts: BTreeMap::new(),
            resource_create_tasks: BTreeMap::new(),
            resource_cleanup_tasks: BTreeMap::new(),
            resource_detach_tasks: BTreeMap::new(),
            resource_list_tasks: BTreeMap::new(),
            resource_local_cancellations: BTreeSet::new(),
            resource_durable_cancellations: BTreeSet::new(),
            resource_network_may_have_started: BTreeSet::new(),
            server_owned_resource_intents: BTreeSet::new(),
            resource_journal_available: true,
            resource_journal_gate: Arc::new(tokio::sync::Mutex::new(())),
            resource_cleanup_attempts: BTreeMap::new(),
            resource_create_attempts: BTreeMap::new(),
            resource_detach_attempts: BTreeMap::new(),
            resource_poll_attempts: BTreeMap::new(),
            pending_shelf_restore: None,
            event_tx,
            _event_listener: event_listener,
            _animation_task: animation_task,
            _theme_reload_task: theme_reload_task,
        };
        #[cfg(feature = "component-harness")]
        let app = {
            let mut app = app;
            app.seed_component_harness();
            app
        };
        app
    }

    fn reduce_event(&mut self, event: UiEvent) {
        match event {
            UiEvent::Bootstrap(Ok(connection)) => {
                let outcome = match connection.outcome {
                    BootstrapOutcome::Reused => "Reused",
                    BootstrapOutcome::StartedSystemService => "Started system service",
                    BootstrapOutcome::SpawnedCompanion => "Started companion",
                };
                self.status = format!(
                    "{outcome} · server {} · protocol {} · pid {} · :{}",
                    connection.health.version,
                    connection.health.api_protocol,
                    connection.pid,
                    connection.port,
                )
                .into();
                self.connection = ConnectionState::Ready(connection);
                if self.preferences_loaded {
                    self.load_startup();
                }
            }
            UiEvent::Bootstrap(Err(error)) => {
                self.status = error.message.clone().into();
                self.connection = ConnectionState::Blocked(error);
            }
            UiEvent::PreferencesLoaded(result) => {
                let _ = self.preferences_load_task.take();
                self.preferences_loaded = true;
                match result {
                    Ok(preferences) => self.preferences = preferences,
                    Err(error) => {
                        self.preferences = DesktopPreferences::default();
                        self.status =
                            format!("Could not load personal Canvas state: {error}").into();
                    }
                }
                if let Some(data) = self.pending_startup_data.take() {
                    self.apply_startup_data(data);
                } else if matches!(self.connection, ConnectionState::Ready(_)) {
                    self.load_startup();
                }
            }
            UiEvent::PreferencesSaved { generation, result }
                if generation == self.preferences_write_generation =>
            {
                let _ = self.preferences_write_task.take();
                if let Err(error) = result {
                    self.status = format!("Could not save personal Canvas state: {error}").into();
                }
                self.kick_preferences_write();
            }
            UiEvent::PreferencesSaved { .. } => {}
            UiEvent::ThemeCatalogReloaded(themes) => {
                if self.themes != themes {
                    self.themes = themes;
                    self.background_image = None;
                    self.request_active_background_image();
                }
            }
            UiEvent::BackgroundImageLoaded {
                generation,
                theme_id,
                asset,
                result,
            } if generation == self.background_image_generation
                && self.active_theme_id() == theme_id
                && matches!(
                    &self.themes.get(&theme_id).background,
                    BackgroundSpecV1::Image {
                        asset: current_asset,
                        ..
                    } if current_asset == &asset
                ) =>
            {
                let _ = self.background_image_task.take();
                match result {
                    Ok(Some(validated)) => {
                        self.background_image = Some(LoadedBackgroundImage {
                            theme_id,
                            asset,
                            image: Arc::new(Image::from_bytes(validated.format, validated.bytes)),
                        });
                    }
                    Ok(None) => self.background_image = None,
                    Err(error) => {
                        self.background_image = None;
                        self.status =
                            format!("Theme image rejected; using Graphite: {error}").into();
                    }
                }
            }
            UiEvent::BackgroundImageLoaded { .. } => {}
            UiEvent::StartupLoaded(Ok(data)) => {
                if self.preferences_loaded {
                    self.apply_startup_data(data);
                } else {
                    self.pending_startup_data = Some(data);
                    self.status = "Loading personal Canvas state…".into();
                }
            }
            UiEvent::StartupLoaded(Err(error)) => self.status = error.into(),
            UiEvent::CanvasOpened {
                request_generation,
                result,
            } if request_generation == self.canvas_generation => {
                self.canvas_open_task.take();
                match result {
                    Ok(canvas) => self.open_canvas_model(canvas),
                    Err(error) => self.status = error.into(),
                }
            }
            UiEvent::CanvasOpened { .. } => {}
            UiEvent::CanvasStream { scope, envelope }
                if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) =>
            {
                self.reduce_canvas_stream(envelope)
            }
            UiEvent::CanvasStream { .. } => {}
            UiEvent::CanvasStreamFailed { scope, error }
                if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) =>
            {
                self.status = format!("Canvas live updates disconnected: {error}").into();
            }
            UiEvent::CanvasStreamFailed { .. } => {}
            UiEvent::CanvasStreamConnected {
                scope,
                resumed_after,
            } if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) => {
                self.status =
                    format!("{} · live from revision {resumed_after}", self.canvas_name).into();
            }
            UiEvent::CanvasStreamConnected { .. } => {}
            UiEvent::CanvasPatched {
                scope,
                result,
                attempted,
            } if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) => {
                self.patch_task.take();
                match result {
                    Ok(canvas) => {
                        self.canvas_revision = self.canvas_revision.max(canvas.revision);
                        self.patch_in_flight = false;
                        let committed_nodes = attempted
                            .iter()
                            .filter_map(|operation| match operation {
                                CanvasOperation::SetGeometry { node_id, .. } => {
                                    Uuid::parse_str(node_id).ok()
                                }
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        self.kick_patch_queue();
                        for node_id in committed_nodes {
                            self.geometry_commits_persisted.insert(node_id);
                            if self.pending_preview_releases.get(&node_id) == Some(&true) {
                                self.kick_preview_release(node_id);
                            }
                        }
                    }
                    Err(error) => {
                        self.patch_in_flight = false;
                        let retry_fits = queued_patch_operation_count(&self.patch_queue)
                            .saturating_add(attempted.len())
                            <= MAX_QUEUED_PATCH_OPERATIONS;
                        if retry_fits {
                            self.patch_queue.push_front(attempted);
                            self.status =
                                format!("Canvas change needs reconciliation: {error}").into();
                        } else {
                            self.status = format!(
                                "Canvas change failed and the bounded retry queue is full: {error}"
                            )
                            .into();
                        }
                        self.request_canvas_reconciliation();
                    }
                }
            }
            UiEvent::CanvasPatched { .. } => {}
            UiEvent::CollaborationSnapshot { scope, result }
                if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) =>
            {
                match result {
                    Ok(response) => {
                        self.apply_collaboration_response(&scope, response);
                        self.drain_pending_presence_mutations();
                    }
                    Err(error) => {
                        self.status =
                            format!("Canvas collaboration heartbeat failed: {error}").into()
                    }
                }
            }
            UiEvent::CollaborationSnapshot { .. } => {}
            UiEvent::CursorPublished { scope, result }
                if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) =>
            {
                self.cursor_in_flight = false;
                if let Ok(response) = result {
                    self.apply_collaboration_response(&scope, response);
                }
                self.drain_pending_presence_mutations();
            }
            UiEvent::CursorPublished { .. } => {}
            UiEvent::GeometryPreviewPublished {
                scope,
                node_id,
                result,
            } if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) => {
                self.preview_in_flight.remove(&node_id);
                match result {
                    Ok(response) => {
                        self.apply_collaboration_response(&scope, response);
                        self.preview_acquired.insert(node_id);
                        self.kick_geometry_commit(node_id);
                    }
                    Err(error) => {
                        self.cancel_geometry_gesture(node_id);
                        self.pending_previews.remove(&node_id);
                        self.pending_geometry_commits.remove(&node_id);
                        let held_lock = self.preview_acquired.remove(&node_id);
                        self.pending_preview_releases.remove(&node_id);
                        if held_lock {
                            self.pending_preview_releases.insert(node_id, false);
                        }
                        self.status =
                            format!("Move blocked by another Canvas client: {error}").into();
                        self.request_canvas_reconciliation();
                    }
                }
                if self.pending_preview_releases.get(&node_id) == Some(&false)
                    || (self.pending_preview_releases.get(&node_id) == Some(&true)
                        && self.geometry_commits_persisted.contains(&node_id))
                {
                    self.kick_preview_release(node_id);
                } else if !self.pending_geometry_commits.contains_key(&node_id)
                    && !self.pending_preview_releases.contains_key(&node_id)
                {
                    self.kick_preview_publish(node_id);
                }
                self.drain_pending_presence_mutations();
            }
            UiEvent::GeometryPreviewPublished { .. } => {}
            UiEvent::GeometryPreviewReleased {
                scope,
                node_id,
                result,
            } if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) => {
                self.preview_releases_in_flight.remove(&node_id);
                match result {
                    Ok(response) => {
                        self.apply_collaboration_response(&scope, response);
                        self.preview_acquired.remove(&node_id);
                        self.geometry_commits_persisted.remove(&node_id);
                    }
                    Err(error) => {
                        self.status = format!("Canvas soft lock release failed: {error}").into();
                    }
                }
                self.drain_pending_presence_mutations();
            }
            UiEvent::GeometryPreviewReleased { .. } => {}
            UiEvent::BrowseLoaded {
                request_generation,
                result,
            } if request_generation == self.canvas_generation => match result {
                Ok(browse) => {
                    self.status = format!("Browsing {}", browse.current).into();
                    self.browse = Some(browse);
                }
                Err(error) => self.status = error.into(),
            },
            UiEvent::BrowseLoaded { .. } => {}
            UiEvent::TerminalCreated { scope, result }
                if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) =>
            {
                match result {
                    Ok(terminal) => {
                        let mut node = SpatialNode::new(
                            NodeKind::Terminal,
                            "Terminal",
                            self.next_node_rect(NodeKind::Terminal),
                            self.next_z(),
                        );
                        node.binding_id = Some(terminal.id.clone());
                        self.selected_node = Some(node.id);
                        if let Some(node_wire) = spatial_to_wire(&node) {
                            self.enqueue_patch(vec![CanvasOperation::UpsertNode {
                                node: node_wire,
                            }]);
                        }
                        self.pending_terminals.push((node.id, terminal));
                        self.nodes.push(node);
                        self.status = "Server terminal added".into();
                    }
                    Err(error) => self.status = error.into(),
                }
            }
            UiEvent::TerminalCreated { .. } => {}
            UiEvent::TerminalLoaded {
                scope,
                node_id,
                terminal_id,
                result,
            } if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) => {
                if self.terminal_loads.get(&node_id) != Some(&terminal_id) {
                    return;
                }
                self.terminal_loads.remove(&node_id);
                let still_bound = self.nodes.iter().any(|node| {
                    node.id == node_id
                        && node.kind == NodeKind::Terminal
                        && node.binding_id.as_deref() == Some(terminal_id.as_str())
                });
                if !still_bound {
                    return;
                }
                match result {
                    Ok(descriptor) => self.pending_terminals.push((node_id, descriptor)),
                    Err(error) => self.status = error.into(),
                }
            }
            UiEvent::TerminalLoaded { .. } => {}
            UiEvent::BrowserCreated { scope, result } => {
                if !complete_browser_create_if_current(
                    &mut self.browser_creates_in_flight,
                    &scope,
                    self.canvas_generation,
                    self.canvas_id.as_deref(),
                ) {
                    return;
                }
                match result {
                    Ok(browser) => {
                        let mut node = SpatialNode::new(
                            NodeKind::Browser,
                            if browser.title.trim().is_empty() {
                                "Browser"
                            } else {
                                &browser.title
                            },
                            self.next_node_rect(NodeKind::Browser),
                            self.next_z(),
                        );
                        node.binding_id = Some(browser.id);
                        self.selected_node = Some(node.id);
                        if let Some(node_wire) = spatial_to_wire(&node) {
                            self.enqueue_patch(vec![CanvasOperation::UpsertNode {
                                node: node_wire,
                            }]);
                        }
                        self.nodes.push(node);
                        self.status = "Server Browser added".into();
                    }
                    Err(error) => self.status = error.into(),
                }
            }
            UiEvent::SessionCreated {
                scope,
                result,
                prompt,
            } if scope.is_current(self.canvas_generation, self.canvas_id.as_deref()) => {
                match result {
                    Ok(session) => {
                        let node_id = Uuid::new_v4();
                        let project_dir = session
                            .project_dir
                            .clone()
                            .or(session.working_dir.clone())
                            .or_else(|| self.project_dir.clone())
                            .unwrap_or_default();
                        self.pending_agents.push(PendingAgentMount {
                            node_id,
                            session_id: session.id,
                            project_dir,
                            title: if session.title.trim().is_empty() {
                                "Agent".to_owned()
                            } else {
                                session.title
                            },
                            prompt,
                            add_node: true,
                        });
                    }
                    Err(error) => self.status = error.into(),
                }
            }
            UiEvent::SessionCreated { .. } => {}
            UiEvent::ResourceJournalPersisted {
                key,
                request,
                result,
            } => self.handle_resource_journal_persisted(key, request, result),
            UiEvent::ResourceJournalRemoved {
                key,
                purpose,
                result,
            } => self.handle_resource_journal_removed(key, purpose, result),
            UiEvent::ResourceJournalCancellationPersisted { key, result } => {
                self.handle_resource_journal_cancellation_persisted(key, result)
            }
            UiEvent::ResourceCreateFinished {
                key,
                request,
                result,
            } => self.handle_resource_create_finished(key, request, result),
            UiEvent::ResourceCleanupFinished { key, result } => {
                self.handle_resource_cleanup_finished(key, result)
            }
            UiEvent::ResourceDetachFinished { key, result } => {
                self.handle_resource_detach_finished(key, result)
            }
            UiEvent::ResourceIntentsListed { canvas_id, result } => {
                self.handle_resource_intents_listed(canvas_id, result)
            }
        }
    }

    fn apply_startup_data(&mut self, data: StartupData) {
        self.projects = data.projects;
        self.canvases = data.canvases;
        let mut recovered_cleanup = Vec::new();
        let mut recovered_detach = Vec::new();
        for entry in data.journal_entries {
            let key = ResourceTaskKey::new(&entry.canvas_id, &entry.request.create_op_id);
            if entry.cleanup_requested {
                self.resource_local_cancellations.insert(key.clone());
                self.resource_durable_cancellations.insert(key.clone());
                if entry.detach_generation.is_some()
                    && entry.request.resource != CanvasResourceSpec::Agent
                {
                    recovered_detach.push(key.clone());
                } else {
                    recovered_cleanup.push(key.clone());
                }
            }
            self.journaled_resource_requests
                .insert(key.clone(), entry.clone());
            self.resource_intents
                .entry(entry.canvas_id)
                .or_default()
                .entry(entry.request.create_op_id.clone())
                .or_insert(placeholder_resource_intent(
                    entry.request,
                    "reserved",
                    Some(
                        "Recovered a crash-safe create request; checking server ownership"
                            .to_owned(),
                    ),
                    entry.detach_generation.unwrap_or(0),
                ));
        }
        self.resource_journal_available = data.journal_error.is_none();
        if let Some(canvas) = data.selected {
            self.open_canvas_model(canvas);
        } else {
            self.status = "Choose a recent project or browse the server filesystem.".into();
        }
        self.refresh_all_canvas_resource_intents();
        for key in recovered_cleanup {
            self.start_resource_cleanup_request(key);
        }
        for key in recovered_detach {
            self.start_resource_detach_request(key);
        }
        if let Some(error) = data.journal_error {
            self.status = format!(
                "Crash-safe resource journal is unavailable; new live resources are disabled: {error}"
            )
            .into();
        } else if let Some(error) = data.list_error {
            self.status = error.into();
        }
    }

    fn load_startup(&mut self) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let last_canvas = self
            .preferences
            .last_canvas_by_server
            .get(client.base_url())
            .cloned();
        let origin = client.base_url().to_owned();
        let tx = self.event_tx.clone();
        self.runtime.spawn(async move {
            let journal = tokio::task::spawn_blocking({
                let origin = origin.clone();
                move || resource_journal::load_default_for_origin(&origin)
            })
            .await
            .map_err(|error| format!("resource journal task failed: {error}"))
            .and_then(|result| result.map_err(|error| format!("{error:#}")));
            let (journal_entries, journal_error) = match journal {
                Ok(entries) => (entries, None),
                Err(error) => (Vec::new(), Some(error)),
            };
            let (projects, canvases) = tokio::join!(
                client.list_projects(SpatialCanvasSurface::Code),
                client.list_canvases(Some(SpatialCanvasSurface::Code))
            );
            let (project_list, project_error) = match projects {
                Ok(projects) => (projects.projects, None),
                Err(error) => (
                    Vec::new(),
                    Some(format!("Could not load recent projects: {error:#}")),
                ),
            };
            let (canvas_list, canvas_error) = match canvases {
                Ok(canvases) => (canvases.canvases, None),
                Err(error) => (
                    Vec::new(),
                    Some(format!("Could not load canvases: {error:#}")),
                ),
            };
            let selected = last_canvas
                .as_deref()
                .and_then(|id| canvas_list.iter().find(|canvas| canvas.id == id))
                .cloned()
                .or_else(|| {
                    (canvas_list.len() == 1)
                        .then(|| canvas_list.first().cloned())
                        .flatten()
                });
            let _ = tx
                .send_async(UiEvent::StartupLoaded(Ok(StartupData {
                    projects: project_list,
                    canvases: canvas_list,
                    selected,
                    journal_entries,
                    journal_error,
                    list_error: project_error.or(canvas_error),
                })))
                .await;
        });
    }

    fn open_project(&mut self, project_dir: String, cx: &mut Context<Self>) {
        if let Some(canvas) = self
            .canvases
            .iter()
            .find(|canvas| canvas.project_dir == project_dir && canvas.archived_at.is_none())
            .cloned()
        {
            self.open_canvas_model_and_sync_palette(canvas, cx);
            return;
        }
        self.request_canvas_creation(project_dir, "Main".to_owned());
    }

    fn request_canvas_creation(&mut self, project_dir: String, name: String) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let tx = self.event_tx.clone();
        let request_generation = self.canvas_generation;
        if let Some(task) = self.canvas_open_task.take() {
            task.abort();
        }
        self.status = format!("Creating {name} canvas…").into();
        self.canvas_open_task = Some(self.runtime.spawn(async move {
            let result = client
                .create_canvas(&CreateCanvasRequest {
                    create_op_id: Uuid::new_v4().to_string(),
                    project_dir,
                    surface: SpatialCanvasSurface::Code,
                    name: name.clone(),
                    shared_theme_id: DEFAULT_THEME_ID.to_owned(),
                })
                .await
                .map(|response| response.canvas)
                .map_err(|error| format!("Could not create {name} canvas: {error:#}"));
            let _ = tx
                .send_async(UiEvent::CanvasOpened {
                    request_generation,
                    result,
                })
                .await;
        }));
    }

    fn create_named_canvas_from_input(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let raw = self.canvas_name_input.read(cx).value().to_string();
        let Ok(name) = normalize_canvas_name(&raw) else {
            self.status = "Canvas names must be 1–80 visible characters.".into();
            cx.notify();
            return;
        };
        let Some(project_dir) = self
            .project_dir
            .clone()
            .or_else(|| self.browse.as_ref().map(|browse| browse.current.clone()))
        else {
            self.status = "Choose or browse to a project before creating a canvas.".into();
            cx.notify();
            return;
        };
        self.canvas_name_input
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.switcher_open = false;
        self.request_canvas_creation(project_dir, name);
        cx.notify();
    }

    fn open_canvas_by_id(&mut self, canvas_id: &str, cx: &mut Context<Self>) {
        if self.canvas_id.as_deref() == Some(canvas_id) {
            self.switcher_open = false;
            return;
        }
        let Some(canvas) = self
            .canvases
            .iter()
            .find(|canvas| canvas.id == canvas_id && canvas.archived_at.is_none())
            .cloned()
        else {
            self.status = "That canvas is no longer available.".into();
            return;
        };
        self.switcher_open = false;
        self.open_canvas_model_and_sync_palette(canvas, cx);
    }

    fn browse_directory(&mut self, path: Option<String>) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let tx = self.event_tx.clone();
        let request_generation = self.canvas_generation;
        self.status = "Browsing server projects…".into();
        self.runtime.spawn(async move {
            let result = client
                .browse_directories(path.as_deref())
                .await
                .map_err(|error| format!("Could not browse server directories: {error:#}"));
            let _ = tx
                .send_async(UiEvent::BrowseLoaded {
                    request_generation,
                    result,
                })
                .await;
        });
    }

    fn canvas_async_scope(&self) -> Option<CanvasAsyncScope> {
        Some(CanvasAsyncScope {
            generation: self.canvas_generation,
            canvas_id: self.canvas_id.clone()?,
        })
    }

    fn track_canvas_bound_task(&mut self, task: tokio::task::JoinHandle<()>) {
        self.canvas_bound_tasks.retain(|task| !task.is_finished());
        self.canvas_bound_tasks.push(task);
    }

    fn resource_intent_mut(
        &mut self,
        key: &ResourceTaskKey,
    ) -> Option<&mut CanvasResourceIntentResponse> {
        self.resource_intents
            .get_mut(&key.canvas_id)?
            .get_mut(&key.create_op_id)
    }

    fn remove_resource_intent(&mut self, key: &ResourceTaskKey) {
        let remove_canvas = self
            .resource_intents
            .get_mut(&key.canvas_id)
            .is_some_and(|intents| {
                intents.remove(&key.create_op_id);
                intents.is_empty()
            });
        if remove_canvas {
            self.resource_intents.remove(&key.canvas_id);
            self.resource_poll_attempts.remove(&key.canvas_id);
        }
    }

    fn upsert_resource_intent(&mut self, canvas_id: &str, intent: CanvasResourceIntentResponse) {
        self.resource_intents
            .entry(canvas_id.to_owned())
            .or_default()
            .insert(intent.request.create_op_id.clone(), intent);
    }

    fn resource_key_for_node(&self, node_id: Uuid) -> Option<ResourceTaskKey> {
        let node_id = node_id.to_string();
        for (canvas_id, intents) in &self.resource_intents {
            for (create_op_id, intent) in intents {
                if intent.request.node.id == node_id {
                    return Some(ResourceTaskKey::new(canvas_id, create_op_id));
                }
            }
        }
        for (key, entry) in &self.journaled_resource_requests {
            if entry.request.node.id == node_id {
                return Some(key.clone());
            }
        }
        None
    }

    fn restore_pending_shelf_command(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(text) = self.pending_shelf_restore.take() else {
            return;
        };
        let current = self.command.read(cx).value().to_string();
        if !shelf_restore_is_safe(&current) {
            return;
        }
        self.command
            .update(cx, |state, cx| state.set_value(text, window, cx));
    }

    fn complete_browser_reservation_for_request(&mut self, request: &CreateCanvasResourceRequest) {
        if matches!(request.resource, CanvasResourceSpec::Browser { .. }) {
            self.browser_creates_in_flight = self.browser_creates_in_flight.saturating_sub(1);
        }
    }

    fn apply_resource_canvas(&mut self, canvas: SpatialCanvas) {
        if let Some(existing) = self.canvases.iter_mut().find(|item| item.id == canvas.id) {
            *existing = canvas.clone();
        } else {
            self.canvases.push(canvas.clone());
        }
        if self.canvas_id.as_deref() == Some(canvas.id.as_str()) {
            self.reduce_canvas_stream(CanvasStreamEnvelope::Snapshot { canvas });
        }
    }

    fn persist_resource_request_before_network(
        &mut self,
        canvas_id: String,
        request: CreateCanvasResourceRequest,
    ) {
        let key = ResourceTaskKey::new(&canvas_id, &request.create_op_id);
        if self.journaled_resource_requests.contains_key(&key) {
            self.start_resource_create_request(canvas_id, request);
            return;
        }
        if self
            .resource_journal_write_tasks
            .get(&key)
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        self.resource_journal_write_tasks.remove(&key);
        let Some(server_origin) = self.client().map(|client| client.base_url().to_owned()) else {
            return;
        };
        let journal_gate = Arc::clone(&self.resource_journal_gate);
        let tx = self.event_tx.clone();
        let task_key = key.clone();
        let io_request = request.clone();
        let task_request = request;
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = self.runtime.spawn(async move {
            let _guard = journal_gate.lock().await;
            let entry = ResourceCreateJournalEntry {
                server_origin,
                canvas_id,
                request: io_request,
                cleanup_requested: false,
                detach_generation: None,
            };
            let result =
                tokio::task::spawn_blocking(move || resource_journal::persist_default(entry))
                    .await
                    .map_err(|error| format!("resource journal task failed: {error}"))
                    .and_then(|result| result.map_err(|error| format!("{error:#}")));
            let _ = result_tx.send(result);
        });
        // Queue delivery is intentionally untracked. Drop waits on neither a
        // full bounded UI queue nor its receiver; the tracked task ends at the
        // durable IO result, and restart recovery owns a persisted entry if
        // the UI event cannot be delivered.
        drop(self.runtime.spawn(async move {
            let result = result_rx.await.unwrap_or_else(|_| {
                Err("resource journal IO task ended before reporting its result".to_owned())
            });
            let _ = tx
                .send_async(UiEvent::ResourceJournalPersisted {
                    key: task_key,
                    request: task_request,
                    result,
                })
                .await;
        }));
        self.resource_journal_write_tasks.insert(key, task);
    }

    fn clear_journaled_resource_request(
        &mut self,
        key: ResourceTaskKey,
        purpose: ResourceJournalRemovalPurpose,
    ) {
        let Some(expected) = self.journaled_resource_requests.get(&key).cloned() else {
            return;
        };
        if *self
            .resource_journal_remove_attempts
            .get(&key)
            .unwrap_or(&0)
            >= MAX_RESOURCE_CREATE_ATTEMPTS
            || self
                .resource_journal_remove_tasks
                .get(&key)
                .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        self.resource_journal_remove_tasks.remove(&key);
        let journal_gate = Arc::clone(&self.resource_journal_gate);
        let tx = self.event_tx.clone();
        let task_key = key.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = self.runtime.spawn(async move {
            let _guard = journal_gate.lock().await;
            let result = tokio::task::spawn_blocking(move || {
                resource_journal::remove_default(&expected, purpose.mode())
            })
            .await
            .map_err(|error| format!("resource journal task failed: {error}"))
            .and_then(|result| result.map_err(|error| format!("{error:#}")));
            let _ = result_tx.send(result);
        });
        drop(self.runtime.spawn(async move {
            let result = result_rx.await.unwrap_or_else(|_| {
                Err("resource journal IO task ended before reporting its result".to_owned())
            });
            let _ = tx
                .send_async(UiEvent::ResourceJournalRemoved {
                    key: task_key,
                    purpose,
                    result,
                })
                .await;
        }));
        self.resource_journal_remove_tasks.insert(key, task);
    }

    fn persist_resource_cancellation(
        &mut self,
        key: ResourceTaskKey,
        request: CreateCanvasResourceRequest,
        detach_generation: Option<u64>,
    ) {
        if self
            .resource_journal_cancel_tasks
            .get(&key)
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        self.resource_journal_cancel_tasks.remove(&key);
        let Some(server_origin) = self.client().map(|client| client.base_url().to_owned()) else {
            return;
        };
        let journal_gate = Arc::clone(&self.resource_journal_gate);
        let tx = self.event_tx.clone();
        let task_key = key.clone();
        let io_key = task_key.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = self.runtime.spawn(async move {
            let _guard = journal_gate.lock().await;
            let entry = ResourceCreateJournalEntry {
                server_origin,
                canvas_id: io_key.canvas_id,
                request,
                cleanup_requested: true,
                detach_generation,
            };
            let result =
                tokio::task::spawn_blocking(move || resource_journal::persist_default(entry))
                    .await
                    .map_err(|error| format!("resource journal task failed: {error}"))
                    .and_then(|result| result.map_err(|error| format!("{error:#}")));
            let _ = result_tx.send(result);
        });
        drop(self.runtime.spawn(async move {
            let result = result_rx.await.unwrap_or_else(|_| {
                Err("resource journal IO task ended before reporting its result".to_owned())
            });
            let _ = tx
                .send_async(UiEvent::ResourceJournalCancellationPersisted {
                    key: task_key,
                    result,
                })
                .await;
        }));
        self.resource_journal_cancel_tasks.insert(key, task);
    }

    fn schedule_resource_intent_list(&mut self, canvas_id: String, delay: Duration) {
        if self
            .resource_list_tasks
            .get(&canvas_id)
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        self.resource_list_tasks.remove(&canvas_id);
        let Some(client) = self.client().cloned() else {
            return;
        };
        let tx = self.event_tx.clone();
        let task_canvas_id = canvas_id.clone();
        let task = self.runtime.spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            // Listing is passive: GET /resources must not spawn jobs.
            let result = client
                .list_canvas_resource_intents(&task_canvas_id)
                .await
                .map(|response| response.intents)
                .map_err(|error| format!("Could not list Canvas resource intents: {error:#}"));
            let _ = tx
                .send_async(UiEvent::ResourceIntentsListed {
                    canvas_id: task_canvas_id,
                    result,
                })
                .await;
        });
        self.resource_list_tasks.insert(canvas_id, task);
    }

    fn schedule_resource_intent_poll(&mut self, canvas_id: &str) {
        let attempt = self
            .resource_poll_attempts
            .entry(canvas_id.to_owned())
            .or_default();
        if *attempt >= MAX_RESOURCE_POLL_ATTEMPTS {
            return;
        }
        *attempt = attempt.saturating_add(1);
        let delay = resource_poll_delay(*attempt);
        self.schedule_resource_intent_list(canvas_id.to_owned(), delay);
    }

    fn refresh_all_canvas_resource_intents(&mut self) {
        let mut canvas_ids = self
            .canvases
            .iter()
            .map(|canvas| canvas.id.clone())
            .collect::<BTreeSet<_>>();
        canvas_ids.extend(self.resource_intents.keys().cloned());
        canvas_ids.extend(
            self.journaled_resource_requests
                .keys()
                .map(|key| key.canvas_id.clone()),
        );
        if let Some(canvas_id) = self.canvas_id.clone() {
            canvas_ids.insert(canvas_id);
        }
        for canvas_id in canvas_ids {
            self.schedule_resource_intent_list(canvas_id, Duration::ZERO);
        }
    }

    fn merge_resource_intents(
        &mut self,
        canvas_id: &str,
        intents: Vec<CanvasResourceIntentResponse>,
    ) {
        let previous = self.resource_intents.remove(canvas_id).unwrap_or_default();
        let mut merged = intents
            .into_iter()
            .map(|intent| (intent.request.create_op_id.clone(), intent))
            .collect::<BTreeMap<_, _>>();
        let server_owned = merged
            .keys()
            .map(|create_op_id| ResourceTaskKey::new(canvas_id, create_op_id))
            .collect::<Vec<_>>();
        let semantic_progress = merged.iter().any(|(create_op_id, intent)| {
            previous.get(create_op_id).is_none_or(|old| {
                old.status != intent.status
                    || old.initial_prompt_completed != intent.initial_prompt_completed
                    || old.detach_generation != intent.detach_generation
            })
        }) || previous.keys().any(|key| !merged.contains_key(key));
        for (create_op_id, intent) in previous {
            let key = ResourceTaskKey::new(canvas_id, &create_op_id);
            let locally_active = self.resource_create_tasks.contains_key(&key)
                || self.resource_cleanup_tasks.contains_key(&key)
                || self.resource_detach_tasks.contains_key(&key)
                || self.resource_journal_write_tasks.contains_key(&key)
                || self.journaled_resource_requests.contains_key(&key)
                || self.resource_local_cancellations.contains(&key)
                || resource_status_is_bound(&intent.status);
            if locally_active {
                merged.entry(create_op_id).or_insert(intent);
            }
        }
        if semantic_progress {
            self.resource_poll_attempts.remove(canvas_id);
        }
        if merged.is_empty() {
            self.resource_poll_attempts.remove(canvas_id);
        } else {
            self.resource_intents.insert(canvas_id.to_owned(), merged);
        }
        for key in server_owned {
            self.server_owned_resource_intents.insert(key.clone());
            if !self.resource_local_cancellations.contains(&key)
                && self
                    .journaled_resource_requests
                    .get(&key)
                    .is_some_and(|entry| !entry.cleanup_requested)
            {
                self.clear_journaled_resource_request(key, ResourceJournalRemovalPurpose::Normal);
            }
        }
    }

    fn reconcile_canvas_resource_intents(&mut self, canvas_id: &str) {
        let Some(intents) = self.resource_intents.get(canvas_id).cloned() else {
            return;
        };
        let mut remove = Vec::new();
        let mut should_poll = false;
        for (create_op_id, intent) in intents {
            let key = ResourceTaskKey::new(canvas_id, &create_op_id);
            if self.resource_local_cancellations.contains(&key) {
                if !self.resource_durable_cancellations.contains(&key) {
                    let detach_generation = self
                        .journaled_resource_requests
                        .get(&key)
                        .and_then(|entry| entry.detach_generation)
                        .filter(|_| intent.request.resource != CanvasResourceSpec::Agent);
                    self.persist_resource_cancellation(key, intent.request, detach_generation);
                } else if self
                    .journaled_resource_requests
                    .get(&key)
                    .and_then(|entry| entry.detach_generation)
                    .is_some()
                    && intent.request.resource != CanvasResourceSpec::Agent
                {
                    self.start_resource_detach_request(key);
                } else if self.resource_network_may_have_started.contains(&key)
                    || self.server_owned_resource_intents.contains(&key)
                    || resource_status_is_bound(&intent.status)
                {
                    self.start_resource_cleanup_request(key);
                } else {
                    self.clear_journaled_resource_request(
                        key,
                        ResourceJournalRemovalPurpose::Normal,
                    );
                }
                continue;
            }
            if resource_status_is_terminal(&intent.status) {
                remove.push(key);
                continue;
            }
            if resource_status_is_bound(&intent.status) {
                if self.journaled_resource_requests.contains_key(&key) {
                    self.clear_journaled_resource_request(
                        key,
                        ResourceJournalRemovalPurpose::Normal,
                    );
                }
                continue;
            }
            if intent.status == "cleaning" || intent.status == "ambiguous" {
                should_poll = true;
                continue;
            }
            if resource_status_needs_poll(&intent.status) {
                if *self.resource_create_attempts.get(&key).unwrap_or(&0)
                    < MAX_RESOURCE_CREATE_ATTEMPTS
                {
                    let mut request = intent.request;
                    request.retry_ambiguous = false;
                    self.start_resource_create_request(key.canvas_id, request);
                } else {
                    should_poll = true;
                }
            }
        }
        for key in remove {
            if self.journaled_resource_requests.contains_key(&key) {
                self.clear_journaled_resource_request(
                    key.clone(),
                    ResourceJournalRemovalPurpose::ConfirmedCleanup,
                );
            }
            self.remove_resource_intent(&key);
        }
        if should_poll {
            self.schedule_resource_intent_poll(canvas_id);
        }
    }

    fn start_resource_create_request(
        &mut self,
        canvas_id: String,
        request: CreateCanvasResourceRequest,
    ) {
        let key = ResourceTaskKey::new(&canvas_id, &request.create_op_id);
        if self.resource_local_cancellations.contains(&key) {
            self.cleanup_resource_intent(key);
            return;
        }
        if self
            .resource_create_tasks
            .get(&key)
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        self.resource_create_tasks.remove(&key);
        let Some(client) = self.client().cloned() else {
            return;
        };
        let tx = self.event_tx.clone();
        let task_key = key.clone();
        let task_request = request.clone();
        let browser_create_gate = Arc::clone(&self.browser_create_gate);
        let is_browser = matches!(request.resource, CanvasResourceSpec::Browser { .. });
        let task = self.runtime.spawn(async move {
            let mut request_attempted = false;
            let response = if is_browser {
                let _guard = browser_create_gate.lock().await;
                match client.list_browser_sessions().await {
                    Ok(sessions) => {
                        let count = sessions
                            .sessions
                            .iter()
                            .filter(|session| {
                                !matches!(
                                    session.status,
                                    BrowserSessionStatus::Stopped | BrowserSessionStatus::Error
                                )
                            })
                            .count();
                        if count >= MAX_NATIVE_BROWSER_SESSIONS {
                            Err(format!(
                                "Native Canvas supports at most four live Browser sessions ({count} found)"
                            ))
                        } else {
                            request_attempted = true;
                            client
                                .create_canvas_resource(&canvas_id, &task_request)
                                .await
                                .map_err(|error| {
                                    format!("Could not create Canvas Browser: {error:#}")
                                })
                        }
                    }
                    Err(error) => Err(format!(
                        "Could not verify the native Browser admission limit: {error:#}"
                    )),
                }
            } else {
                request_attempted = true;
                client
                    .create_canvas_resource(&canvas_id, &task_request)
                    .await
                    .map_err(|error| format!("Could not create Canvas resource: {error:#}"))
            };
            let _ = tx
                .send_async(UiEvent::ResourceCreateFinished {
                    key: task_key,
                    request: task_request,
                    result: ResourceCreateTaskResult {
                        response,
                        request_attempted,
                    },
                })
                .await;
        });
        self.resource_create_tasks.insert(key, task);
    }

    fn cleanup_resource_intent(&mut self, key: ResourceTaskKey) {
        let Some(request) = self
            .journaled_resource_requests
            .get(&key)
            .map(|entry| entry.request.clone())
            .or_else(|| {
                self.resource_intents
                    .get(&key.canvas_id)
                    .and_then(|intents| intents.get(&key.create_op_id))
                    .map(|intent| intent.request.clone())
            })
        else {
            return;
        };
        self.resource_local_cancellations.insert(key.clone());
        if self
            .resource_journal_write_tasks
            .get(&key)
            .is_some_and(|task| !task.is_finished())
        {
            self.status = "Securing resource cleanup request…".into();
            return;
        }
        let detach_generation = self
            .journaled_resource_requests
            .get(&key)
            .and_then(|entry| entry.detach_generation);
        self.persist_resource_cancellation(key, request, detach_generation);
        self.status = "Securing resource cleanup request…".into();
    }

    fn detach_recovery_resource_intent(&mut self, key: ResourceTaskKey) {
        let is_agent = self
            .resource_intents
            .get(&key.canvas_id)
            .and_then(|intents| intents.get(&key.create_op_id))
            .is_some_and(|intent| intent.request.resource == CanvasResourceSpec::Agent)
            || self
                .journaled_resource_requests
                .get(&key)
                .is_some_and(|entry| entry.request.resource == CanvasResourceSpec::Agent);
        if is_agent {
            self.cleanup_resource_intent(key);
            return;
        }
        let Some(request) = self
            .journaled_resource_requests
            .get(&key)
            .map(|entry| entry.request.clone())
            .or_else(|| {
                self.resource_intents
                    .get(&key.canvas_id)
                    .and_then(|intents| intents.get(&key.create_op_id))
                    .map(|intent| intent.request.clone())
            })
        else {
            return;
        };
        let expected_detach_generation = self
            .resource_intents
            .get(&key.canvas_id)
            .and_then(|intents| intents.get(&key.create_op_id))
            .map(|intent| intent.detach_generation)
            .or_else(|| {
                self.journaled_resource_requests
                    .get(&key)
                    .and_then(|entry| entry.detach_generation)
            })
            .unwrap_or(0);
        self.resource_local_cancellations.insert(key.clone());
        self.persist_resource_cancellation(key, request, Some(expected_detach_generation));
        self.status = "Runtime end could not be proven; detaching the Canvas binding…".into();
    }

    fn start_resource_cleanup_request(&mut self, key: ResourceTaskKey) {
        if *self.resource_cleanup_attempts.get(&key).unwrap_or(&0) >= MAX_RESOURCE_CREATE_ATTEMPTS {
            if let Some(intent) = self.resource_intent_mut(&key) {
                intent.last_error.get_or_insert_with(|| {
                    "Cleanup is durably pending but paused after bounded retries; it will resume after restart"
                        .to_owned()
                });
            }
            return;
        }
        if self
            .resource_cleanup_tasks
            .get(&key)
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        self.resource_cleanup_tasks.remove(&key);
        let Some(client) = self.client().cloned() else {
            return;
        };
        let tx = self.event_tx.clone();
        let task_key = key.clone();
        let task = self.runtime.spawn(async move {
            let result = client
                .cleanup_canvas_resource(&task_key.canvas_id, &task_key.create_op_id)
                .await
                .map_err(|error| format!("Resource cleanup needs attention: {error:#}"));
            let _ = tx
                .send_async(UiEvent::ResourceCleanupFinished {
                    key: task_key,
                    result,
                })
                .await;
        });
        self.resource_cleanup_tasks.insert(key, task);
    }

    fn start_resource_detach_request(&mut self, key: ResourceTaskKey) {
        if *self.resource_detach_attempts.get(&key).unwrap_or(&0) >= MAX_RESOURCE_CREATE_ATTEMPTS {
            return;
        }
        if self
            .resource_detach_tasks
            .get(&key)
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let Some(request) = self
            .journaled_resource_requests
            .get(&key)
            .map(|entry| entry.request.clone())
            .or_else(|| {
                self.resource_intents
                    .get(&key.canvas_id)
                    .and_then(|intents| intents.get(&key.create_op_id))
                    .map(|intent| intent.request.clone())
            })
        else {
            return;
        };
        if request.resource == CanvasResourceSpec::Agent {
            self.start_resource_cleanup_request(key);
            return;
        }
        self.resource_detach_tasks.remove(&key);
        let Some(client) = self.client().cloned() else {
            return;
        };
        let expected_detach_generation = self
            .journaled_resource_requests
            .get(&key)
            .and_then(|entry| entry.detach_generation)
            .or_else(|| {
                self.resource_intents
                    .get(&key.canvas_id)
                    .and_then(|intents| intents.get(&key.create_op_id))
                    .map(|intent| intent.detach_generation)
            })
            .unwrap_or(0);
        let tx = self.event_tx.clone();
        let task_key = key.clone();
        let task = self.runtime.spawn(async move {
            let result = client
                .detach_canvas_resource(
                    &task_key.canvas_id,
                    &task_key.create_op_id,
                    &DetachCanvasResourceRequest {
                        request,
                        expected_detach_generation,
                    },
                )
                .await
                .map_err(|error| format!("Resource detach needs attention: {error:#}"));
            let _ = tx
                .send_async(UiEvent::ResourceDetachFinished {
                    key: task_key,
                    result,
                })
                .await;
        });
        self.resource_detach_tasks.insert(key, task);
    }

    fn create_bound_resource(
        &mut self,
        resource: CanvasResourceSpec,
        initial_prompt: Option<String>,
        original_shelf_text: Option<String>,
    ) -> bool {
        if self.client().is_none() {
            self.status =
                "A compatible Mitsuro server is required before creating a resource.".into();
            return false;
        }
        if !self.resource_journal_available {
            self.status =
                "Crash-safe resource storage is unavailable; live resource creation is disabled."
                    .into();
            if let Some(text) = original_shelf_text {
                self.pending_shelf_restore = Some(text);
            }
            return false;
        }
        if self.project_dir.is_none() {
            self.status = "Open a project before creating a resource.".into();
            return false;
        }
        let Some(canvas_id) = self.canvas_id.clone() else {
            return false;
        };
        let title = resource_spec_title(&resource);
        let kind = resource_spec_kind(&resource);
        let request = CreateCanvasResourceRequest {
            create_op_id: Uuid::new_v4().to_string(),
            client_id: self.client_id.clone(),
            node: CanvasResourceNodeRequest {
                id: Uuid::new_v4().to_string(),
                title: title.to_owned(),
                rect: world_rect_to_wire(self.next_node_rect(kind)),
                z_index: self.next_z(),
            },
            resource,
            initial_prompt,
            retry_ambiguous: false,
        };
        self.pending_shelf_restore = original_shelf_text;
        self.upsert_resource_intent(
            &canvas_id,
            placeholder_resource_intent(request.clone(), "reserved", None, 0),
        );
        // Every resource request is fsynced locally before any network side
        // effect begins. Browser admission is decided only under the global
        // gate after a current server list.
        self.persist_resource_request_before_network(canvas_id, request);
        self.status = format!("Securing {title} create request…").into();
        true
    }

    fn handle_resource_journal_persisted(
        &mut self,
        key: ResourceTaskKey,
        request: CreateCanvasResourceRequest,
        result: std::result::Result<ResourceCreateJournalEntry, String>,
    ) {
        self.resource_journal_write_tasks.remove(&key);
        match result {
            Ok(entry) => {
                self.journaled_resource_requests
                    .insert(key.clone(), entry.clone());
                if entry.cleanup_requested || self.resource_local_cancellations.contains(&key) {
                    self.complete_browser_reservation_for_request(&request);
                    self.resource_local_cancellations.insert(key.clone());
                    if entry.detach_generation.is_some()
                        && entry.request.resource != CanvasResourceSpec::Agent
                    {
                        self.start_resource_detach_request(key);
                    } else {
                        self.persist_resource_cancellation(
                            key,
                            entry.request,
                            entry.detach_generation,
                        );
                    }
                } else {
                    self.pending_shelf_restore = None;
                    self.start_resource_create_request(key.canvas_id, request);
                }
            }
            Err(error) => {
                self.resource_journal_available = false;
                self.complete_browser_reservation_for_request(&request);
                if !self.resource_local_cancellations.contains(&key) {
                    if self.pending_shelf_restore.is_none() {
                        self.pending_shelf_restore = request.initial_prompt.clone().or_else(|| {
                            Some(format!(
                                "+ {}",
                                resource_spec_title(&request.resource).to_lowercase()
                            ))
                        });
                    }
                } else {
                    self.pending_shelf_restore = None;
                }
                self.remove_resource_intent(&key);
                self.status = format!(
                    "Resource was not started because its crash-safe journal write failed; the command was restored: {error}"
                )
                .into();
            }
        }
    }

    fn handle_resource_journal_removed(
        &mut self,
        key: ResourceTaskKey,
        purpose: ResourceJournalRemovalPurpose,
        result: std::result::Result<ResourceJournalRemoveOutcome, String>,
    ) {
        let _ = purpose;
        self.resource_journal_remove_tasks.remove(&key);
        match result {
            Ok(ResourceJournalRemoveOutcome::Removed)
            | Ok(ResourceJournalRemoveOutcome::Missing) => {
                self.journaled_resource_requests.remove(&key);
                self.resource_journal_remove_attempts.remove(&key);
                self.resource_local_cancellations.remove(&key);
                self.resource_durable_cancellations.remove(&key);
                self.resource_network_may_have_started.remove(&key);
                if self
                    .resource_intents
                    .get(&key.canvas_id)
                    .and_then(|intents| intents.get(&key.create_op_id))
                    .is_some_and(|intent| resource_status_is_terminal(&intent.status))
                {
                    self.remove_resource_intent(&key);
                }
            }
            Ok(ResourceJournalRemoveOutcome::RetainedCleanupRequested) => {
                self.resource_local_cancellations.insert(key.clone());
                if let Some(entry) = self.journaled_resource_requests.get_mut(&key) {
                    entry.cleanup_requested = true;
                }
                self.start_resource_cleanup_request(key);
            }
            Err(error) => {
                let attempt = self
                    .resource_journal_remove_attempts
                    .entry(key.clone())
                    .or_default();
                *attempt = attempt.saturating_add(1);
                if let Some(intent) = self.resource_intent_mut(&key) {
                    intent.last_error =
                        Some(format!("Local recovery receipt cleanup failed: {error}"));
                }
                if self.canvas_id.as_deref() == Some(key.canvas_id.as_str()) {
                    self.status = format!(
                        "Server owns the resource, but its local recovery receipt could not be cleared: {error}"
                    )
                    .into();
                }
                if *self
                    .resource_journal_remove_attempts
                    .get(&key)
                    .unwrap_or(&0)
                    < MAX_RESOURCE_CREATE_ATTEMPTS
                {
                    self.schedule_resource_intent_poll(&key.canvas_id);
                }
            }
        }
    }

    fn handle_resource_journal_cancellation_persisted(
        &mut self,
        key: ResourceTaskKey,
        result: std::result::Result<ResourceCreateJournalEntry, String>,
    ) {
        self.resource_journal_cancel_tasks.remove(&key);
        match result {
            Ok(entry) => {
                self.journaled_resource_requests
                    .insert(key.clone(), entry.clone());
                self.resource_durable_cancellations.insert(key.clone());
                let externally_uncertain = self.resource_network_may_have_started.contains(&key)
                    || self.server_owned_resource_intents.contains(&key)
                    || self
                        .resource_intents
                        .get(&key.canvas_id)
                        .and_then(|intents| intents.get(&key.create_op_id))
                        .is_some_and(|intent| resource_status_is_bound(&intent.status));
                if entry.detach_generation.is_some()
                    && entry.request.resource != CanvasResourceSpec::Agent
                {
                    self.start_resource_detach_request(key);
                } else if externally_uncertain {
                    self.start_resource_cleanup_request(key);
                } else {
                    self.clear_journaled_resource_request(
                        key,
                        ResourceJournalRemovalPurpose::Normal,
                    );
                }
            }
            Err(error) => {
                if let Some(intent) = self.resource_intent_mut(&key) {
                    intent.last_error = Some(format!(
                        "Cleanup is paused because its durable cancellation could not be saved: {error}"
                    ));
                }
                if self.canvas_id.as_deref() == Some(key.canvas_id.as_str()) {
                    self.status =
                        "Cleanup was not started because its durable cancellation could not be saved"
                            .into();
                }
            }
        }
    }

    fn handle_resource_create_finished(
        &mut self,
        key: ResourceTaskKey,
        request: CreateCanvasResourceRequest,
        result: ResourceCreateTaskResult,
    ) {
        self.resource_create_tasks.remove(&key);
        self.complete_browser_reservation_for_request(&request);
        if result.request_attempted {
            self.resource_network_may_have_started.insert(key.clone());
        }
        if self.resource_local_cancellations.contains(&key) {
            if let Ok(response) = result.response {
                self.server_owned_resource_intents.insert(key.clone());
                self.apply_resource_canvas(response.canvas);
            }
            self.start_resource_cleanup_request(key);
            return;
        }
        match result.response {
            Ok(response) => {
                self.server_owned_resource_intents.insert(key.clone());
                let detach_generation = self
                    .resource_intents
                    .get(&key.canvas_id)
                    .and_then(|intents| intents.get(&key.create_op_id))
                    .map(|intent| intent.detach_generation)
                    .unwrap_or(0);
                self.upsert_resource_intent(
                    &key.canvas_id,
                    placeholder_resource_intent(request.clone(), "bound", None, detach_generation),
                );
                if let Ok(node_id) = Uuid::parse_str(&request.node.id) {
                    self.selected_node = Some(node_id);
                }
                self.apply_resource_canvas(response.canvas);
                self.clear_journaled_resource_request(key, ResourceJournalRemovalPurpose::Normal);
                self.status =
                    format!("Server {} added", resource_spec_title(&request.resource)).into();
            }
            Err(error) => {
                let attempt = self
                    .resource_create_attempts
                    .entry(key.clone())
                    .or_default();
                *attempt = attempt.saturating_add(1);
                if let Some(intent) = self.resource_intent_mut(&key) {
                    intent.last_error = Some(error.clone());
                    if !result.request_attempted {
                        intent.status = "failed".to_owned();
                    }
                }
                if self.canvas_id.as_deref() == Some(key.canvas_id.as_str()) {
                    self.status = error.into();
                }
                if result.request_attempted
                    && *self.resource_create_attempts.get(&key).unwrap_or(&0)
                        < MAX_RESOURCE_CREATE_ATTEMPTS
                {
                    self.schedule_resource_intent_poll(&key.canvas_id);
                } else if !result.request_attempted {
                    self.clear_journaled_resource_request(
                        key,
                        ResourceJournalRemovalPurpose::Normal,
                    );
                }
            }
        }
    }

    fn handle_resource_cleanup_finished(
        &mut self,
        key: ResourceTaskKey,
        result: std::result::Result<CleanupCanvasResourceResponse, String>,
    ) {
        self.resource_cleanup_tasks.remove(&key);
        match result {
            Ok(response) => {
                self.apply_resource_canvas(response.canvas);
                if resource_status_is_terminal(&response.status) {
                    self.resource_cleanup_attempts.remove(&key);
                    self.clear_journaled_resource_request(
                        key.clone(),
                        ResourceJournalRemovalPurpose::ConfirmedCleanup,
                    );
                    self.remove_resource_intent(&key);
                    if self.canvas_id.as_deref() == Some(key.canvas_id.as_str()) {
                        self.status = "Resource cleanup confirmed".into();
                    }
                } else if response.status == "cleaning" {
                    self.schedule_resource_intent_poll(&key.canvas_id);
                } else {
                    let attempt = self
                        .resource_cleanup_attempts
                        .entry(key.clone())
                        .or_default();
                    *attempt = attempt.saturating_add(1);
                    if *self.resource_cleanup_attempts.get(&key).unwrap_or(&0)
                        < MAX_RESOURCE_CREATE_ATTEMPTS
                    {
                        self.schedule_resource_intent_poll(&key.canvas_id);
                    }
                }
            }
            Err(error) => {
                let attempt = self
                    .resource_cleanup_attempts
                    .entry(key.clone())
                    .or_default();
                *attempt = attempt.saturating_add(1);
                if let Some(intent) = self.resource_intent_mut(&key) {
                    intent.last_error = Some(error.clone());
                }
                let is_agent = self
                    .resource_intents
                    .get(&key.canvas_id)
                    .and_then(|intents| intents.get(&key.create_op_id))
                    .is_some_and(|intent| intent.request.resource == CanvasResourceSpec::Agent)
                    || self
                        .journaled_resource_requests
                        .get(&key)
                        .is_some_and(|entry| entry.request.resource == CanvasResourceSpec::Agent);
                let missing = error.to_lowercase().contains("not found");
                if missing {
                    self.clear_journaled_resource_request(
                        key.clone(),
                        ResourceJournalRemovalPurpose::ConfirmedCleanup,
                    );
                    self.remove_resource_intent(&key);
                    return;
                }
                if !is_agent && *self.resource_cleanup_attempts.get(&key).unwrap_or(&0) >= 1 {
                    // Bound Terminal/Browser cleanup requires an authoritative
                    // end proof. If that cannot be established, Detach is the
                    // monotonic accepted action.
                    self.detach_recovery_resource_intent(key);
                    return;
                }
                if self.canvas_id.as_deref() == Some(key.canvas_id.as_str()) {
                    self.status = error.into();
                }
                if *self.resource_cleanup_attempts.get(&key).unwrap_or(&0)
                    < MAX_RESOURCE_CREATE_ATTEMPTS
                {
                    self.schedule_resource_intent_poll(&key.canvas_id);
                }
            }
        }
    }

    fn handle_resource_detach_finished(
        &mut self,
        key: ResourceTaskKey,
        result: std::result::Result<DetachCanvasResourceResponse, String>,
    ) {
        self.resource_detach_tasks.remove(&key);
        match result {
            Ok(response) => {
                if let Some(intent) = self.resource_intent_mut(&key) {
                    intent.status = response.status.clone();
                    intent.detach_generation = response.detach_generation;
                }
                if let Some(entry) = self.journaled_resource_requests.get_mut(&key) {
                    entry.cleanup_requested = true;
                    entry.detach_generation = Some(response.detach_generation);
                }
                self.apply_resource_canvas(response.canvas);
                if response.detach_committed || response.binding_absent {
                    self.clear_journaled_resource_request(
                        key.clone(),
                        ResourceJournalRemovalPurpose::ConfirmedDetach,
                    );
                    if self.canvas_id.as_deref() == Some(key.canvas_id.as_str()) {
                        self.status =
                            "Window detached; the durable runtime was left running or uncertain"
                                .into();
                    }
                } else {
                    let attempt = self
                        .resource_detach_attempts
                        .entry(key.clone())
                        .or_default();
                    *attempt = attempt.saturating_add(1);
                    self.schedule_resource_intent_poll(&key.canvas_id);
                }
            }
            Err(error) => {
                let attempt = self
                    .resource_detach_attempts
                    .entry(key.clone())
                    .or_default();
                *attempt = attempt.saturating_add(1);
                if let Some(intent) = self.resource_intent_mut(&key) {
                    intent.last_error = Some(error.clone());
                }
                if error.to_lowercase().contains("not found") {
                    self.clear_journaled_resource_request(
                        key.clone(),
                        ResourceJournalRemovalPurpose::ConfirmedDetach,
                    );
                    self.remove_resource_intent(&key);
                    return;
                }
                if self.canvas_id.as_deref() == Some(key.canvas_id.as_str()) {
                    self.status = error.into();
                }
                if *self.resource_detach_attempts.get(&key).unwrap_or(&0)
                    < MAX_RESOURCE_CREATE_ATTEMPTS
                {
                    self.schedule_resource_intent_poll(&key.canvas_id);
                }
            }
        }
    }

    fn handle_resource_intents_listed(
        &mut self,
        canvas_id: String,
        result: std::result::Result<Vec<CanvasResourceIntentResponse>, String>,
    ) {
        self.resource_list_tasks.remove(&canvas_id);
        match result {
            Ok(intents) => {
                self.merge_resource_intents(&canvas_id, intents);
                self.reconcile_canvas_resource_intents(&canvas_id);
            }
            Err(error) => {
                if self.canvas_id.as_deref() == Some(canvas_id.as_str()) {
                    self.status = error.into();
                }
                self.schedule_resource_intent_poll(&canvas_id);
            }
        }
    }

    fn begin_canvas_generation(&mut self) {
        self.canvas_generation = self
            .canvas_generation
            .checked_add(1)
            .expect("canvas generation exhausted");
        self.browser_creates_in_flight = 0;
        if let Some(task) = self.patch_task.take() {
            task.abort();
        }
        if let Some(task) = self.reconciliation_task.take() {
            task.abort();
        }
        if let Some(task) = self.canvas_open_task.take() {
            task.abort();
        }
        for task in self.canvas_bound_tasks.drain(..) {
            task.abort();
        }
    }

    fn open_canvas_model(&mut self, canvas: SpatialCanvas) {
        // Draft callbacks update the in-memory preferences immediately. Flush
        // that old canvas state before changing the generation/key.
        self.personal_persist_task.take();
        self.persist_personal_state();
        self.begin_canvas_generation();
        self.leave_current_canvas();
        if let Some(task) = self.canvas_stream_task.take() {
            task.abort();
        }
        self.canvas_id = Some(canvas.id.clone());
        self.canvas_name = canvas.name.clone();
        self.shared_theme_id = canvas.shared_theme_id.clone();
        self.canvas_revision = canvas.revision;
        self.project_dir = Some(canvas.project_dir.clone());
        self.project_name = Path::new(&canvas.project_dir)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&canvas.project_dir)
            .to_owned();
        self.nodes = canvas
            .state
            .nodes
            .iter()
            .filter_map(wire_to_spatial)
            .collect();
        self.agents.clear();
        self.files.clear();
        self.changes.clear();
        self.browsers.clear();
        self.terminals.clear();
        self.terminal_loads.clear();
        self.pending_terminals.clear();
        self.pending_agents = self
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Agent)
            .filter_map(|node| {
                Some(PendingAgentMount {
                    node_id: node.id,
                    session_id: node.binding_id.clone()?,
                    project_dir: canvas.project_dir.clone(),
                    title: node.title.clone(),
                    prompt: None,
                    add_node: false,
                })
            })
            .collect();
        self.patch_queue.clear();
        self.patch_in_flight = false;
        self.browse = None;
        self.presence_token = None;
        self.collaboration = CanvasCollaborationState::default();
        self.collaboration_started_at = Instant::now();
        self.cursor_throttle.reset();
        self.cursor_in_flight = false;
        self.pending_cursor = None;
        self.preview_throttle.reset();
        self.preview_in_flight.clear();
        self.preview_releases_in_flight.clear();
        self.preview_acquired.clear();
        self.pending_previews.clear();
        self.pending_geometry_commits.clear();
        self.geometry_commits_persisted.clear();
        self.pending_preview_releases.clear();

        if let Some(origin) = self.client().map(|client| client.base_url().to_owned()) {
            let key = DesktopPreferences::canvas_key(&origin, &canvas.id);
            if let Some(personal) = self.preferences.canvases.get(&key) {
                self.camera = personal.camera;
                self.selected_node = personal.selected_node;
                self.stage_layout = personal.stage_layout;
                self.stage_slots = personal
                    .stage_slots
                    .iter()
                    .copied()
                    .filter(|id| self.nodes.iter().any(|node| node.id == *id))
                    .take(personal.stage_layout.capacity())
                    .collect();
                self.background_preferences =
                    sanitize_background_preferences(personal.background.clone());
                self.motion = self.background_preferences.motion;
                self.pulse_open = personal.pulse_open;
                self.pulse_hovered = false;
            } else {
                self.camera = Camera::default();
                self.selected_node = None;
                self.stage_layout = StageLayout::One;
                self.stage_slots.clear();
                self.background_preferences = BackgroundPreferencesV1::default();
                self.motion = self.background_preferences.motion;
                self.pulse_open = false;
                self.pulse_hovered = false;
            }
        } else {
            self.background_preferences = BackgroundPreferencesV1::default();
            self.motion = self.background_preferences.motion;
            self.pulse_open = false;
            self.pulse_hovered = false;
        }
        if let Some(existing) = self.canvases.iter_mut().find(|item| item.id == canvas.id) {
            *existing = canvas.clone();
        } else {
            self.canvases.push(canvas.clone());
        }
        self.request_active_background_image();
        self.persist_personal_state();
        let scope = self
            .canvas_async_scope()
            .expect("opening a canvas establishes an async scope");
        self.start_canvas_stream(scope.clone(), canvas.revision);
        self.start_canvas_presence(scope);
        self.schedule_resource_intent_list(canvas.id, Duration::ZERO);
        self.status = format!("{} · live", self.canvas_name).into();
    }

    fn open_canvas_model_and_sync_palette(
        &mut self,
        canvas: SpatialCanvas,
        cx: &mut Context<Self>,
    ) {
        let previous_palette = self.active_palette();
        self.open_canvas_model(canvas);
        if let Some(palette) = changed_palette(previous_palette, self.active_palette()) {
            self.sync_surface_palettes(palette, cx);
        }
    }

    fn leave_current_canvas(&mut self) {
        if let Some(task) = self.presence_task.take() {
            task.abort();
        }
        self.presence_token_updates = None;
        let Some(presence_token) = self.presence_token.take() else {
            return;
        };
        let Some(client) = self.client().cloned() else {
            return;
        };
        let Some(canvas_id) = self.canvas_id.clone() else {
            return;
        };
        let client_id = self.client_id.clone();
        self.runtime.spawn(async move {
            let _ = client
                .leave_canvas_presence(&canvas_id, &client_id, &presence_token)
                .await;
        });
    }

    fn start_canvas_presence(&mut self, scope: CanvasAsyncScope) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let client_id = self.client_id.clone();
        let tx = self.event_tx.clone();
        let (presence_token_tx, presence_token_rx) = tokio::sync::watch::channel(None);
        self.presence_token_updates = Some(presence_token_tx.clone());
        self.presence_task = Some(self.runtime.spawn(async move {
            loop {
                let presence_token = presence_token_rx.borrow().clone();
                let result = client
                    .heartbeat_canvas_presence(
                        &canvas_id,
                        &CanvasPresenceHeartbeatRequest {
                            client_id: client_id.clone(),
                            presence_token: presence_token.clone(),
                            display_name: Some("Mitsuro Canvas".to_owned()),
                            cursor: None,
                        },
                    )
                    .await
                    .map_err(|error| format!("{error:#}"));
                if let Ok(response) = &result {
                    if let Some(next_token) = response.presence_token.clone() {
                        presence_token_tx.send_replace(Some(next_token));
                    }
                }
                if tx
                    .send_async(UiEvent::CollaborationSnapshot {
                        scope: scope.clone(),
                        result,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }));
    }

    fn start_canvas_stream(&mut self, scope: CanvasAsyncScope, after_sequence: u64) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let tx = self.event_tx.clone();
        self.canvas_stream_task = Some(self.runtime.spawn(async move {
            let mut cursor = after_sequence;
            let mut retry = 0_u32;
            loop {
                let mut stream = match client.canvas_stream(&canvas_id, Some(cursor)).await {
                    Ok(stream) => {
                        if tx
                            .send_async(UiEvent::CanvasStreamConnected {
                                scope: scope.clone(),
                                resumed_after: cursor,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                        stream
                    }
                    Err(error) => {
                        let _ = tx
                            .send_async(UiEvent::CanvasStreamFailed {
                                scope: scope.clone(),
                                error: format!("{error:#}"),
                            })
                            .await;
                        retry = retry.saturating_add(1);
                        tokio::time::sleep(canvas_reconnect_delay(retry)).await;
                        continue;
                    }
                };
                let disconnect = loop {
                    match stream.next().await {
                        Some(Ok(envelope)) => {
                            retry = 0;
                            match &envelope {
                                CanvasStreamEnvelope::Snapshot { canvas } => {
                                    cursor = cursor.max(canvas.revision);
                                }
                                CanvasStreamEnvelope::Events { page } => {
                                    if let Some(sequence) =
                                        page.events.last().map(|event| event.sequence)
                                    {
                                        cursor = cursor.max(sequence);
                                    }
                                }
                                CanvasStreamEnvelope::CollaborationSnapshot { .. }
                                | CanvasStreamEnvelope::CollaborationEvents { .. } => {}
                            }
                            if tx
                                .send_async(UiEvent::CanvasStream {
                                    scope: scope.clone(),
                                    envelope,
                                })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Some(Err(error)) => break format!("{error:#}"),
                        None => break "server ended the Canvas event stream".to_owned(),
                    }
                };
                if tx
                    .send_async(UiEvent::CanvasStreamFailed {
                        scope: scope.clone(),
                        error: disconnect,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                retry = retry.saturating_add(1);
                tokio::time::sleep(canvas_reconnect_delay(retry)).await;
            }
        }));
    }

    fn reduce_canvas_stream(&mut self, envelope: CanvasStreamEnvelope) {
        match envelope {
            CanvasStreamEnvelope::Snapshot { canvas } => {
                // HTTP resource/create responses and the SSE stream race by
                // design. Never let an older (or already-applied) snapshot
                // roll the live model backwards or prune a node delivered by
                // a newer event.
                if !canvas_snapshot_is_newer(self.canvas_revision, canvas.revision) {
                    return;
                }
                self.canvas_revision = canvas.revision;
                self.canvas_name = canvas.name;
                self.shared_theme_id = canvas.shared_theme_id;
                self.request_active_background_image();
                let live_ids = canvas
                    .state
                    .nodes
                    .iter()
                    .filter_map(|node| Uuid::parse_str(&node.id).ok())
                    .collect::<Vec<_>>();
                let removed_ids = self
                    .nodes
                    .iter()
                    .filter(|node| !live_ids.contains(&node.id))
                    .map(|node| node.id)
                    .collect::<Vec<_>>();
                self.nodes.retain(|node| live_ids.contains(&node.id));
                self.agents.retain(|id, _| live_ids.contains(id));
                self.files.retain(|id, _| live_ids.contains(id));
                self.changes.retain(|id, _| live_ids.contains(id));
                self.browsers.retain(|id, _| live_ids.contains(id));
                self.terminals.retain(|id, _| live_ids.contains(id));
                self.terminal_loads.retain(|id, _| live_ids.contains(id));
                self.pending_terminals
                    .retain(|(id, _)| live_ids.contains(id));
                self.pending_agents
                    .retain(|mount| live_ids.contains(&mount.node_id));
                self.stage_slots.retain(|id| live_ids.contains(id));
                if self.selected_node.is_some_and(|id| !live_ids.contains(&id)) {
                    self.selected_node = None;
                }
                let mut personal_state_changed = false;
                for node_id in removed_ids {
                    personal_state_changed |= self.clear_personal_node_state(node_id);
                }
                for node in canvas.state.nodes {
                    self.apply_canvas_operation(CanvasOperation::UpsertNode { node });
                }
                if personal_state_changed {
                    self.persist_personal_state();
                }
                self.kick_patch_queue();
            }
            CanvasStreamEnvelope::Events { page } => {
                let latest_sequence = page.latest_sequence;
                for event in page.events {
                    for operation in event.operations {
                        self.apply_canvas_operation(operation);
                    }
                    self.canvas_revision = self.canvas_revision.max(event.sequence);
                }
                if self.canvas_revision >= latest_sequence {
                    self.kick_patch_queue();
                } else {
                    self.status = format!(
                        "Catching up Canvas revisions {}–{latest_sequence}…",
                        self.canvas_revision.saturating_add(1)
                    )
                    .into();
                }
            }
            CanvasStreamEnvelope::CollaborationSnapshot { collaboration } => {
                self.apply_collaboration_snapshot(collaboration);
            }
            CanvasStreamEnvelope::CollaborationEvents { page } => {
                match self.collaboration.apply_page(page) {
                    CanvasCollaborationApply::Applied => self.reconcile_local_collaboration(),
                    CanvasCollaborationApply::Gap => {
                        self.status = "Recovering Canvas collaboration state…".into();
                        self.request_collaboration_snapshot();
                    }
                    CanvasCollaborationApply::Ignored => {}
                }
            }
        }
    }

    fn apply_collaboration_snapshot(&mut self, snapshot: CanvasCollaborationSnapshot) {
        if !self.collaboration.replace(snapshot) {
            return;
        }
        self.reconcile_local_collaboration();
    }

    fn apply_collaboration_response(
        &mut self,
        scope: &CanvasAsyncScope,
        response: CanvasCollaborationResponse,
    ) {
        if !store_presence_token_if_current(
            &mut self.presence_token,
            scope,
            self.canvas_generation,
            self.canvas_id.as_deref(),
            response.presence_token,
        ) {
            return;
        }
        if let Some(token_updates) = &self.presence_token_updates {
            token_updates.send_replace(self.presence_token.clone());
        }
        self.apply_collaboration_snapshot(response.snapshot);
    }

    fn drain_pending_presence_mutations(&mut self) {
        if self.presence_token.is_none() {
            return;
        }
        self.kick_cursor_publish();

        let preview_ids = self.pending_previews.keys().copied().collect::<Vec<_>>();
        for node_id in preview_ids {
            self.kick_preview_publish(node_id);
        }

        let release_ids = self
            .pending_preview_releases
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for node_id in release_ids {
            self.kick_preview_release(node_id);
        }
    }

    fn reconcile_local_collaboration(&mut self) {
        acceptance::observe_collaboration(
            &self.client_id,
            self.collaboration
                .clients
                .values()
                .filter(|client| !client.stale)
                .map(|client| client.client_id.as_str()),
            self.collaboration
                .geometry_previews
                .values()
                .map(|preview| preview.client_id.as_str()),
        );
        let owned_locks = self
            .collaboration
            .geometry_previews
            .values()
            .filter(|preview| preview.client_id == self.client_id)
            .filter_map(|preview| Uuid::parse_str(&preview.node_id).ok())
            .collect::<BTreeSet<_>>();
        self.preview_acquired.retain(|node_id| {
            owned_locks.contains(node_id)
                || self.preview_in_flight.contains(node_id)
                || self.preview_releases_in_flight.contains(node_id)
                || self.pending_geometry_commits.contains_key(node_id)
                || self.pending_preview_releases.contains_key(node_id)
        });
        self.geometry_commits_persisted.retain(|node_id| {
            self.preview_acquired.contains(node_id)
                || self.pending_preview_releases.contains_key(node_id)
        });
    }

    fn request_collaboration_snapshot(&mut self) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let Some(presence_token) = self.presence_token.clone() else {
            // The canvas heartbeat owns initial registration. Its response
            // will install the token and a full snapshot, so do not race it
            // with a second tokenless heartbeat.
            return;
        };
        let Some(scope) = self.canvas_async_scope() else {
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let client_id = self.client_id.clone();
        let tx = self.event_tx.clone();
        let task = self.runtime.spawn(async move {
            let result = client
                .heartbeat_canvas_presence(
                    &canvas_id,
                    &CanvasPresenceHeartbeatRequest {
                        client_id,
                        presence_token: Some(presence_token),
                        display_name: Some("Mitsuro Canvas".to_owned()),
                        cursor: None,
                    },
                )
                .await
                .map_err(|error| format!("{error:#}"));
            let _ = tx
                .send_async(UiEvent::CollaborationSnapshot { scope, result })
                .await;
        });
        self.track_canvas_bound_task(task);
    }

    fn request_canvas_reconciliation(&mut self) {
        if self
            .reconciliation_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let Some(client) = self.client().cloned() else {
            return;
        };
        let Some(scope) = self.canvas_async_scope() else {
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let tx = self.event_tx.clone();
        self.reconciliation_task = Some(self.runtime.spawn(async move {
            match client.get_canvas(&canvas_id).await {
                Ok(canvas) => {
                    let _ = tx
                        .send_async(UiEvent::CanvasStream {
                            scope,
                            envelope: CanvasStreamEnvelope::Snapshot { canvas },
                        })
                        .await;
                }
                Err(error) => {
                    let _ = tx
                        .send_async(UiEvent::CanvasStreamFailed {
                            scope,
                            error: format!("Could not reconcile Canvas geometry: {error:#}"),
                        })
                        .await;
                }
            }
        }));
    }

    fn apply_canvas_operation(&mut self, operation: CanvasOperation) {
        match operation {
            CanvasOperation::UpsertNode { node } => {
                let Some(spatial) = wire_to_spatial(&node) else {
                    return;
                };
                let identity_changed = self
                    .nodes
                    .iter()
                    .find(|item| item.id == spatial.id)
                    .is_some_and(|existing| resource_identity_changed(existing, &spatial));
                if identity_changed {
                    self.release_node_surface_for_rebind(spatial.id);
                }
                if let Some(existing) = self.nodes.iter_mut().find(|item| item.id == spatial.id) {
                    *existing = spatial.clone();
                    if identity_changed {
                        self.queue_remote_agent_mount(&spatial);
                        if self.clear_personal_node_state(spatial.id) {
                            self.persist_personal_state();
                        }
                    }
                } else {
                    self.queue_remote_agent_mount(&spatial);
                    self.nodes.push(spatial);
                }
            }
            CanvasOperation::RemoveNode { node_id } => {
                let Ok(id) = Uuid::parse_str(&node_id) else {
                    return;
                };
                self.nodes.retain(|node| node.id != id);
                self.agents.remove(&id);
                self.files.remove(&id);
                self.changes.remove(&id);
                self.browsers.remove(&id);
                self.terminals.remove(&id);
                self.terminal_loads.remove(&id);
                self.pending_terminals.retain(|(node_id, _)| *node_id != id);
                self.pending_agents.retain(|mount| mount.node_id != id);
                self.stage_slots.retain(|slot| *slot != id);
                if self.selected_node == Some(id) {
                    self.selected_node = None;
                }
                if self.clear_personal_node_state(id) {
                    self.persist_personal_state();
                }
            }
            CanvasOperation::SetGeometry {
                node_id,
                rect,
                z_index,
            } => {
                let Ok(id) = Uuid::parse_str(&node_id) else {
                    return;
                };
                if let Some(node) = self.nodes.iter_mut().find(|node| node.id == id) {
                    node.world_bounds = wire_rect_to_world(rect);
                    node.z_order = z_index;
                }
            }
            CanvasOperation::SetTitle { node_id, title } => {
                let Ok(id) = Uuid::parse_str(&node_id) else {
                    return;
                };
                if let Some(node) = self.nodes.iter_mut().find(|node| node.id == id) {
                    node.title = title;
                }
            }
            CanvasOperation::RenameCanvas { name } => self.canvas_name = name,
            CanvasOperation::SetSharedTheme { theme_id } => {
                self.shared_theme_id = theme_id;
                self.request_active_background_image();
            }
            CanvasOperation::SetArchived { archived: true } => {
                self.status = "This canvas was archived by another client.".into();
            }
            CanvasOperation::SetArchived { archived: false } => {}
        }
    }

    fn queue_remote_agent_mount(&mut self, node: &SpatialNode) {
        if node.kind != NodeKind::Agent {
            return;
        }
        let Some(session_id) = node.binding_id.clone() else {
            return;
        };
        self.pending_agents.push(PendingAgentMount {
            node_id: node.id,
            session_id,
            project_dir: self.project_dir.clone().unwrap_or_default(),
            title: node.title.clone(),
            prompt: None,
            add_node: false,
        });
    }

    fn release_node_surface_for_rebind(&mut self, node_id: Uuid) {
        self.agents.remove(&node_id);
        self.files.remove(&node_id);
        self.changes.remove(&node_id);
        self.browsers.remove(&node_id);
        self.browser_mount_waiting.remove(&node_id);
        self.browser_mount_retired.remove(&node_id);
        self.terminals.remove(&node_id);
        self.terminal_loads.remove(&node_id);
        self.pending_terminals.retain(|(id, _)| *id != node_id);
        self.pending_agents.retain(|mount| mount.node_id != node_id);
    }

    fn enqueue_patch(&mut self, operations: Vec<CanvasOperation>) {
        if operations.is_empty() || matches!(self.connection, ConnectionState::Harness) {
            return;
        }
        let Some(queue) =
            bounded_patch_queue(&self.patch_queue, operations, MAX_QUEUED_PATCH_OPERATIONS)
        else {
            self.status = "Canvas change queue is full; reconciling with the server.".into();
            self.request_canvas_reconciliation();
            return;
        };
        self.patch_queue = queue;
        self.kick_patch_queue();
    }

    fn kick_patch_queue(&mut self) {
        if self.patch_in_flight {
            return;
        }
        let Some(operations) = self.patch_queue.pop_front() else {
            return;
        };
        let Some(client) = self.client().cloned() else {
            self.patch_queue.push_front(operations);
            return;
        };
        let Some(scope) = self.canvas_async_scope() else {
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let request = PatchCanvasRequest {
            op_id: Uuid::new_v4().to_string(),
            client_id: self.client_id.clone(),
            expected_revision: self.canvas_revision,
            operations: operations.clone(),
        };
        let tx = self.event_tx.clone();
        self.patch_in_flight = true;
        self.patch_task = Some(self.runtime.spawn(async move {
            let result = client
                .patch_canvas(&canvas_id, &request)
                .await
                .map(|response| response.canvas)
                .map_err(|error| format!("{error:#}"));
            let _ = tx
                .send_async(UiEvent::CanvasPatched {
                    scope,
                    result,
                    attempted: operations,
                })
                .await;
        }));
    }

    fn materialize_pending_agents(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(client) = self.client().cloned() else {
            self.pending_agents.clear();
            return;
        };
        let pending = std::mem::take(&mut self.pending_agents);
        for mount in pending {
            if self.agents.contains_key(&mount.node_id) {
                continue;
            }
            let runtime = Arc::clone(&self.runtime);
            let session_id = mount.session_id.clone();
            let project_dir = mount.project_dir.clone();
            let title = mount.title.clone();
            let client_id = self.client_id.clone();
            let initial_draft = self.personal_agent_draft(mount.node_id);
            let initial_scroll_offset = self.personal_agent_scroll_offset(mount.node_id);
            let palette = self.active_palette();
            let scope = self.canvas_async_scope();
            let entity = cx.new(|cx| {
                AgentSurface::new(
                    client.clone(),
                    runtime,
                    session_id.clone(),
                    project_dir,
                    title.clone(),
                    client_id,
                    initial_draft,
                    palette,
                    window,
                    cx,
                )
            });
            if let Some(scope) = scope {
                let node_id = mount.node_id;
                cx.subscribe(&entity, move |app, _, event, cx| {
                    app.handle_agent_surface_event(&scope, node_id, event, cx);
                })
                .detach();
            }
            entity.update(cx, |agent, _| {
                agent.restore_scroll_offset(initial_scroll_offset)
            });
            if mount.add_node {
                let mut node = SpatialNode::new(
                    NodeKind::Agent,
                    mount.title,
                    self.next_node_rect(NodeKind::Agent),
                    self.next_z(),
                );
                node.id = mount.node_id;
                node.binding_id = Some(mount.session_id);
                node.status = NodeStatus::Idle;
                let operation =
                    spatial_to_wire(&node).map(|node| CanvasOperation::UpsertNode { node });
                self.nodes.push(node);
                if let Some(operation) = operation {
                    self.enqueue_patch(vec![operation]);
                }
            }
            self.agents.insert(mount.node_id, entity.clone());
            self.selected_node = Some(mount.node_id);
            if let Some(prompt) = mount.prompt {
                entity.update(cx, |agent, cx| agent.send(prompt, cx));
            }
            self.status = "Agent added to Main".into();
        }
    }

    fn materialize_pending_terminals(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let pending = std::mem::take(&mut self.pending_terminals);
        for (node_id, descriptor) in pending {
            if self.terminals.contains_key(&node_id)
                || !self.nodes.iter().any(|node| {
                    node.id == node_id
                        && node.kind == NodeKind::Terminal
                        && node.binding_id.as_deref() == Some(descriptor.id.as_str())
                })
            {
                continue;
            }
            let runtime = Arc::clone(&self.runtime);
            let client_id = self.client_id.clone();
            let palette = self.active_palette();
            let entity = cx.new(|cx| {
                TerminalSurface::new(
                    client.clone(),
                    runtime,
                    descriptor,
                    client_id,
                    palette,
                    window,
                    cx,
                )
            });
            self.terminals.insert(node_id, entity);
        }
    }

    fn materialize_resource_surfaces(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(client) = self.client().cloned() else {
            return;
        };
        let Some(scope) = self.canvas_async_scope() else {
            return;
        };
        let Some(project_dir) = self.project_dir.clone() else {
            return;
        };
        let resources = self
            .nodes
            .iter()
            .map(|node| (node.id, node.kind, node.binding_id.clone()))
            .collect::<Vec<_>>();
        let palette = self.active_palette();
        for (id, kind, binding_id) in resources {
            match kind {
                NodeKind::Files if !self.files.contains_key(&id) => {
                    let runtime = Arc::clone(&self.runtime);
                    let client = client.clone();
                    let canvas_id = scope.canvas_id.clone();
                    let entity =
                        cx.new(|cx| FilesSurface::new(client, runtime, canvas_id, palette, cx));
                    self.files.insert(id, entity);
                }
                NodeKind::Changes if !self.changes.contains_key(&id) => {
                    let runtime = Arc::clone(&self.runtime);
                    let client = client.clone();
                    let project_dir = project_dir.clone();
                    let entity =
                        cx.new(|cx| ChangesSurface::new(client, runtime, project_dir, palette, cx));
                    self.changes.insert(id, entity);
                }
                NodeKind::Browser if !self.browsers.contains_key(&id) => {
                    let Some(session_id) = binding_id else {
                        continue;
                    };
                    let runtime = Arc::clone(&self.runtime);
                    let client = client.clone();
                    let project_dir = project_dir.clone();
                    let client_id = self.client_id.clone();
                    let entity = cx.new(|cx| {
                        BrowserSurface::new(
                            client,
                            runtime,
                            project_dir,
                            session_id,
                            client_id,
                            palette,
                            window,
                            cx,
                        )
                    });
                    self.browsers.insert(id, entity);
                }
                NodeKind::Terminal
                    if !self.terminals.contains_key(&id)
                        && !self.terminal_loads.contains_key(&id) =>
                {
                    let Some(terminal_id) = binding_id else {
                        continue;
                    };
                    self.terminal_loads.insert(id, terminal_id.clone());
                    let client = client.clone();
                    let tx = self.event_tx.clone();
                    let scope = scope.clone();
                    let requested_terminal_id = terminal_id.clone();
                    let task = self.runtime.spawn(async move {
                        let result = client.get_terminal(&terminal_id).await.map_err(|error| {
                            format!("Could not attach terminal {terminal_id}: {error:#}")
                        });
                        let _ = tx
                            .send_async(UiEvent::TerminalLoaded {
                                scope,
                                node_id: id,
                                terminal_id: requested_terminal_id,
                                result,
                            })
                            .await;
                    });
                    self.track_canvas_bound_task(task);
                }
                _ => {}
            }
        }
    }

    fn client(&self) -> Option<&MitsuroClient> {
        match &self.connection {
            ConnectionState::Ready(connection) => Some(&connection.client),
            _ => None,
        }
    }

    fn submit_shelf(
        &mut self,
        input: &Entity<InputState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let command = input.read(cx).value().to_string();
        if command.trim().is_empty() {
            return;
        }
        input.update(cx, |state, cx| state.set_value("", window, cx));
        self.execute_command(command, cx);
    }

    fn execute_command(&mut self, raw: String, cx: &mut Context<Self>) {
        let command = raw.trim();
        if command.is_empty() {
            return;
        }
        if !command.starts_with('+') {
            if let Some(agent) = self
                .selected_node
                .and_then(|id| self.agents.get(&id))
                .cloned()
            {
                agent.update(cx, |agent, cx| agent.send(command.to_owned(), cx));
                return;
            }
            self.create_agent(Some(command.to_owned()));
            return;
        }

        match shelf_resource_command(command) {
            Some("agent") => self.create_agent(None),
            Some("terminal") => self.create_terminal(),
            Some("browser") => self.create_browser(),
            Some("files") => self.add_unbound_node(NodeKind::Files),
            Some("changes") => self.add_unbound_node(NodeKind::Changes),
            _ => {
                self.status =
                    "Unknown command. Try + agent, + terminal, + browser, + files, or + changes."
                        .into()
            }
        }
        self.sync_agent_selection(cx);
        cx.notify();
    }

    fn create_agent(&mut self, prompt: Option<String>) {
        let restore = prompt.clone().or_else(|| Some("+ agent".to_owned()));
        let _ = self.create_bound_resource(CanvasResourceSpec::Agent, prompt, restore);
    }

    fn create_terminal(&mut self) {
        let _ = self.create_bound_resource(
            CanvasResourceSpec::Terminal {
                size: TerminalSize::default(),
            },
            None,
            Some("+ terminal".to_owned()),
        );
    }

    fn create_browser(&mut self) {
        let active = self
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Browser)
            .count();
        if !reserve_browser_create(active, &mut self.browser_creates_in_flight) {
            self.status =
                "Native Canvas supports at most four live or pending Browser sessions.".into();
            return;
        }
        if !self.create_bound_resource(
            CanvasResourceSpec::Browser {
                url: Some("about:blank".to_owned()),
            },
            None,
            Some("+ browser".to_owned()),
        ) {
            self.browser_creates_in_flight = self.browser_creates_in_flight.saturating_sub(1);
        }
    }

    fn add_unbound_node(&mut self, kind: NodeKind) {
        if self.project_dir.is_none() && !matches!(self.connection, ConnectionState::Harness) {
            self.status = "Open a project before adding a window.".into();
            return;
        }
        let title = match kind {
            NodeKind::Agent => "Agent",
            NodeKind::Terminal => "Terminal",
            NodeKind::Browser => "Browser",
            NodeKind::Files => "Files",
            NodeKind::Changes => "Changes",
        };
        let node = SpatialNode::new(kind, title, self.next_node_rect(kind), self.next_z());
        self.selected_node = Some(node.id);
        if let Some(node_wire) = spatial_to_wire(&node) {
            self.enqueue_patch(vec![CanvasOperation::UpsertNode { node: node_wire }]);
        }
        self.nodes.push(node);
        self.status = format!("{title} window added · awaiting server binding").into();
    }

    fn next_node_rect(&self, kind: NodeKind) -> WorldRect {
        let cascade = self.nodes.len() as f64 * 28.0;
        let (width, height) = match kind {
            NodeKind::Agent => (660.0, 520.0),
            NodeKind::Terminal => (720.0, 420.0),
            NodeKind::Browser => (820.0, 560.0),
            NodeKind::Files => (420.0, 540.0),
            NodeKind::Changes => (760.0, 540.0),
        };
        WorldRect::new(
            self.camera.world_origin.x + 120.0 + cascade,
            self.camera.world_origin.y + 90.0 + cascade,
            width,
            height,
        )
    }

    fn next_z(&self) -> i64 {
        self.nodes
            .iter()
            .map(|node| node.z_order)
            .max()
            .unwrap_or(0)
            + 1
    }

    fn collaboration_elapsed(&self) -> Duration {
        self.collaboration_started_at.elapsed()
    }

    fn publish_cursor(&mut self, world: WorldPoint) {
        if self.canvas_id.is_none() || matches!(self.connection, ConnectionState::Harness) {
            return;
        }
        let now = self.collaboration_elapsed();
        if !self.cursor_throttle.should_publish(now) {
            return;
        }
        self.pending_cursor = Some(CanvasCursor {
            x: world.x,
            y: world.y,
        });
        self.kick_cursor_publish();
    }

    fn kick_cursor_publish(&mut self) {
        if !presence_mutation_ready(
            self.presence_token.as_deref(),
            self.cursor_in_flight,
            self.pending_cursor.is_some(),
        ) {
            return;
        }
        let cursor = self
            .pending_cursor
            .take()
            .expect("presence mutation readiness requires a pending cursor");
        let presence_token = self
            .presence_token
            .clone()
            .expect("presence mutation readiness requires a token");
        let Some(client) = self.client().cloned() else {
            self.pending_cursor = Some(cursor);
            return;
        };
        let Some(scope) = self.canvas_async_scope() else {
            self.pending_cursor = Some(cursor);
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let tx = self.event_tx.clone();
        let client_id = self.client_id.clone();
        self.cursor_in_flight = true;
        let task = self.runtime.spawn(async move {
            let result = client
                .heartbeat_canvas_presence(
                    &canvas_id,
                    &CanvasPresenceHeartbeatRequest {
                        client_id,
                        presence_token: Some(presence_token),
                        display_name: Some("Mitsuro Canvas".to_owned()),
                        cursor: Some(cursor),
                    },
                )
                .await
                .map_err(|error| format!("{error:#}"));
            let _ = tx
                .send_async(UiEvent::CursorPublished { scope, result })
                .await;
        });
        self.track_canvas_bound_task(task);
    }

    fn node_lock_owner(&self, node_id: Uuid) -> Option<String> {
        let owner = self
            .collaboration
            .locked_by_other(&node_id.to_string(), &self.client_id)?;
        Some(
            self.collaboration
                .clients
                .get(owner)
                .and_then(|client| client.display_name.clone())
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| "another Canvas client".to_owned()),
        )
    }

    fn queue_geometry_preview(&mut self, node_id: Uuid, force: bool) {
        if matches!(self.connection, ConnectionState::Harness) {
            return;
        }
        let now = self.collaboration_elapsed();
        if force {
            let _ = self.preview_throttle.should_publish(now);
        } else if !self.preview_throttle.should_publish(now) {
            return;
        }
        let Some(node) = self.nodes.iter().find(|node| node.id == node_id) else {
            return;
        };
        self.pending_previews.insert(
            node_id,
            (world_rect_to_wire(node.world_bounds), node.z_order),
        );
        self.kick_preview_publish(node_id);
    }

    fn kick_preview_publish(&mut self, node_id: Uuid) {
        let blocked = self.preview_in_flight.contains(&node_id)
            || self.preview_releases_in_flight.contains(&node_id)
            || self.pending_preview_releases.contains_key(&node_id);
        if !presence_mutation_ready(
            self.presence_token.as_deref(),
            blocked,
            self.pending_previews.contains_key(&node_id),
        ) {
            return;
        }
        let (rect, z_index) = self
            .pending_previews
            .remove(&node_id)
            .expect("presence mutation readiness requires a pending preview");
        let presence_token = self
            .presence_token
            .clone()
            .expect("presence mutation readiness requires a token");
        let Some(client) = self.client().cloned() else {
            self.pending_previews.insert(node_id, (rect, z_index));
            return;
        };
        let Some(scope) = self.canvas_async_scope() else {
            self.pending_previews.insert(node_id, (rect, z_index));
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let client_id = self.client_id.clone();
        let tx = self.event_tx.clone();
        self.preview_in_flight.insert(node_id);
        let task = self.runtime.spawn(async move {
            let result = client
                .update_canvas_geometry_preview(
                    &canvas_id,
                    &node_id.to_string(),
                    &CanvasGeometryPreviewRequest {
                        client_id,
                        presence_token,
                        rect,
                        z_index,
                    },
                )
                .await
                .map_err(|error| format!("{error:#}"));
            let _ = tx
                .send_async(UiEvent::GeometryPreviewPublished {
                    scope,
                    node_id,
                    result,
                })
                .await;
        });
        self.track_canvas_bound_task(task);
    }

    fn kick_geometry_commit(&mut self, node_id: Uuid) {
        if self.preview_in_flight.contains(&node_id) || !self.preview_acquired.contains(&node_id) {
            return;
        }
        // Drain the latest coalesced preview before persisting/releasing so
        // peers see the final pointer position, not only the lock-acquisition
        // frame. `pending_previews` is a one-entry-per-node latest-only map.
        if self.pending_previews.contains_key(&node_id) {
            self.kick_preview_publish(node_id);
            return;
        }
        let Some((rect, z_index)) = self.pending_geometry_commits.remove(&node_id) else {
            return;
        };
        self.pending_preview_releases.insert(node_id, true);
        self.enqueue_patch(vec![CanvasOperation::SetGeometry {
            node_id: node_id.to_string(),
            rect,
            z_index,
        }]);
    }

    fn kick_preview_release(&mut self, node_id: Uuid) {
        if self.preview_in_flight.contains(&node_id)
            || self.preview_releases_in_flight.contains(&node_id)
        {
            return;
        }
        let Some(presence_token) = self.presence_token.clone() else {
            return;
        };
        let Some(committed) = self.pending_preview_releases.get(&node_id).copied() else {
            return;
        };
        if !geometry_release_ready(
            committed,
            self.geometry_commits_persisted.contains(&node_id),
            false,
        ) {
            return;
        }
        self.pending_preview_releases.remove(&node_id);
        let Some(client) = self.client().cloned() else {
            return;
        };
        let Some(scope) = self.canvas_async_scope() else {
            return;
        };
        let canvas_id = scope.canvas_id.clone();
        let tx = self.event_tx.clone();
        let client_id = self.client_id.clone();
        self.preview_releases_in_flight.insert(node_id);
        let task = self.runtime.spawn(async move {
            let mut attempts = 0_u8;
            let result = loop {
                attempts = attempts.saturating_add(1);
                match client
                    .release_canvas_geometry_preview(
                        &canvas_id,
                        &node_id.to_string(),
                        &client_id,
                        &presence_token,
                        committed,
                    )
                    .await
                {
                    Ok(response) => break Ok(response),
                    Err(_) if attempts < 5 => {
                        tokio::time::sleep(Duration::from_millis(
                            150_u64.saturating_mul(u64::from(attempts)),
                        ))
                        .await;
                    }
                    Err(error) => break Err(format!("{error:#}")),
                }
            };
            let _ = tx
                .send_async(UiEvent::GeometryPreviewReleased {
                    scope,
                    node_id,
                    result,
                })
                .await;
        });
        self.track_canvas_bound_task(task);
    }

    fn cancel_geometry_gesture(&mut self, node_id: Uuid) {
        let restore = match self.gesture.as_ref() {
            Some(PointerGesture::Move {
                node_id: active,
                initial,
                initial_z,
                ..
            }) if *active == node_id => Some((*initial, *initial_z)),
            Some(PointerGesture::Resize {
                node_id: active,
                initial,
                initial_z,
                ..
            }) if *active == node_id => Some((*initial, *initial_z)),
            _ => None,
        };
        if let Some((rect, z_index)) = restore {
            if let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) {
                node.world_bounds = rect;
                node.z_order = z_index;
            }
            self.gesture = None;
            self.gesture_active = false;
        }
    }

    fn begin_move(&mut self, node_id: Uuid, event: &MouseDownEvent, cx: &mut Context<Self>) {
        self.last_input_activity = Instant::now();
        if let Some(owner) = self.node_lock_owner(node_id) {
            self.status = format!("{owner} is already moving this window").into();
            cx.notify();
            return;
        }
        if self.preview_in_flight.contains(&node_id)
            || self.preview_releases_in_flight.contains(&node_id)
            || self.preview_acquired.contains(&node_id)
            || self.pending_geometry_commits.contains_key(&node_id)
            || self.pending_preview_releases.contains_key(&node_id)
        {
            self.status = "Finishing the previous window move…".into();
            cx.notify();
            return;
        }
        let screen = canvas_point(event.position.x.into(), event.position.y.into());
        let world = self.camera.screen_to_world(screen, WorldPoint::default());
        if let Some(node) = self.nodes.iter().find(|node| node.id == node_id) {
            let initial = node.world_bounds;
            let initial_z = node.z_order;
            self.gesture = Some(PointerGesture::Move {
                node_id,
                grab_offset: WorldPoint::new(
                    world.x - node.world_bounds.origin.x,
                    world.y - node.world_bounds.origin.y,
                ),
                initial,
                initial_z,
            });
            self.selected_node = Some(node_id);
            self.sync_agent_selection(cx);
            self.gesture_active = true;
            let next_z = self.next_z();
            if let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) {
                node.z_order = next_z;
            }
            self.preview_throttle.reset();
            self.queue_geometry_preview(node_id, true);
            cx.notify();
        }
    }

    fn begin_resize(&mut self, node_id: Uuid, event: &MouseDownEvent, cx: &mut Context<Self>) {
        self.last_input_activity = Instant::now();
        if let Some(owner) = self.node_lock_owner(node_id) {
            self.status = format!("{owner} is already resizing this window").into();
            cx.notify();
            return;
        }
        if self.preview_in_flight.contains(&node_id)
            || self.preview_releases_in_flight.contains(&node_id)
            || self.pending_preview_releases.contains_key(&node_id)
        {
            self.status = "Finishing the previous window resize…".into();
            cx.notify();
            return;
        }
        let screen = canvas_point(event.position.x.into(), event.position.y.into());
        let world = self.camera.screen_to_world(screen, WorldPoint::default());
        if let Some(node) = self.nodes.iter().find(|node| node.id == node_id) {
            let initial_z = node.z_order;
            self.gesture = Some(PointerGesture::Resize {
                node_id,
                anchor: world,
                initial: node.world_bounds,
                initial_z,
            });
            self.selected_node = Some(node_id);
            self.sync_agent_selection(cx);
            self.gesture_active = true;
            let next_z = self.next_z();
            if let Some(node) = self.nodes.iter_mut().find(|node| node.id == node_id) {
                node.z_order = next_z;
            }
            self.preview_throttle.reset();
            self.queue_geometry_preview(node_id, true);
            cx.notify();
        }
    }

    fn handle_mouse_move(&mut self, event: &MouseMoveEvent, cx: &mut Context<Self>) {
        self.last_input_activity = Instant::now();
        let current = canvas_point(event.position.x.into(), event.position.y.into());
        let world = self.camera.screen_to_world(current, WorldPoint::default());
        self.publish_cursor(world);
        if self.gesture.is_none() {
            return;
        }
        let preview_node = match &mut self.gesture {
            Some(PointerGesture::Pan { last }) => {
                self.camera
                    .pan_screen_delta(current.x - last.x, current.y - last.y);
                *last = current;
                None
            }
            Some(PointerGesture::Move {
                node_id,
                grab_offset,
                ..
            }) => {
                if let Some(node) = self.nodes.iter_mut().find(|node| node.id == *node_id) {
                    node.world_bounds.origin.x =
                        snap_world(world.x - grab_offset.x, GRID_WORLD, self.camera.zoom);
                    node.world_bounds.origin.y =
                        snap_world(world.y - grab_offset.y, GRID_WORLD, self.camera.zoom);
                }
                Some(*node_id)
            }
            Some(PointerGesture::Resize {
                node_id,
                anchor,
                initial,
                ..
            }) => {
                if let Some(node) = self.nodes.iter_mut().find(|node| node.id == *node_id) {
                    node.world_bounds.size.width = snap_world(
                        (initial.size.width + world.x - anchor.x).max(260.0),
                        GRID_WORLD,
                        self.camera.zoom,
                    );
                    node.world_bounds.size.height = snap_world(
                        (initial.size.height + world.y - anchor.y).max(180.0),
                        GRID_WORLD,
                        self.camera.zoom,
                    );
                }
                Some(*node_id)
            }
            None => None,
        };
        if let Some(node_id) = preview_node {
            self.queue_geometry_preview(node_id, false);
        }
        cx.notify();
    }

    fn end_gesture(&mut self, cx: &mut Context<Self>) {
        if let Some(gesture) = self.gesture.take() {
            let changed_node = match gesture {
                PointerGesture::Move { node_id, .. } | PointerGesture::Resize { node_id, .. } => {
                    Some(node_id)
                }
                PointerGesture::Pan { .. } => None,
            };
            let geometry = changed_node
                .and_then(|id| self.nodes.iter().find(|node| node.id == id))
                .map(|node| (node.id, world_rect_to_wire(node.world_bounds), node.z_order));
            if let Some((node_id, rect, z_index)) = geometry {
                if matches!(self.connection, ConnectionState::Harness) {
                    self.enqueue_patch(vec![CanvasOperation::SetGeometry {
                        node_id: node_id.to_string(),
                        rect,
                        z_index,
                    }]);
                } else {
                    self.preview_throttle.reset();
                    self.pending_geometry_commits
                        .insert(node_id, (rect, z_index));
                    self.pending_previews.remove(&node_id);
                    self.kick_geometry_commit(node_id);
                }
            }
            self.gesture_active = false;
            self.persist_personal_state();
            cx.notify();
        }
    }

    fn handle_scroll(
        &mut self,
        event: &ScrollWheelEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.last_input_activity = Instant::now();
        let delta = event.delta.pixel_delta(window.line_height());
        if event.modifiers.control || event.modifiers.platform {
            let factor = (-f32::from(delta.y) * 0.0025).exp() as f64;
            let cursor = canvas_point(event.position.x.into(), event.position.y.into());
            self.camera
                .zoom_at(self.camera.zoom * factor, cursor, WorldPoint::default());
        } else {
            self.camera
                .pan_screen_delta(-f32::from(delta.x) as f64, -f32::from(delta.y) as f64);
        }
        let cursor = canvas_point(event.position.x.into(), event.position.y.into());
        self.publish_cursor(self.camera.screen_to_world(cursor, WorldPoint::default()));
        self.gesture_active = true;
        self.wheel_settle_task = Some(cx.spawn(async move |this, cx| {
            Timer::after(Duration::from_millis(120)).await;
            let _ = this.update(cx, |app, cx| {
                app.gesture_active = false;
                app.persist_personal_state();
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn fit_all(&mut self, window: &Window, cx: &mut Context<Self>) {
        let viewport = viewport_size(window);
        if let Some(camera) = Camera::fit_all(&self.nodes, viewport, 76.0) {
            self.camera = camera;
            self.persist_personal_state();
            cx.notify();
        }
    }

    fn reset_zoom(&mut self, window: &Window, cx: &mut Context<Self>) {
        let center = WorldPoint::new(
            f32::from(window.viewport_size().width) as f64 / 2.0,
            (f32::from(window.viewport_size().height) - TITLE_HEIGHT) as f64 / 2.0,
        );
        self.camera.zoom_at(1.0, center, WorldPoint::default());
        self.persist_personal_state();
        cx.notify();
    }

    fn set_stage_layout(&mut self, layout: StageLayout, cx: &mut Context<Self>) {
        self.stage_layout = layout;
        self.stage_slots.truncate(layout.capacity());
        self.persist_personal_state();
        cx.notify();
    }

    fn stage_selected(&mut self, cx: &mut Context<Self>) {
        let Some(id) = self.selected_node else {
            self.status = "Select a window before adding it to Stage.".into();
            cx.notify();
            return;
        };
        if let Some(index) = self.stage_slots.iter().position(|slot| *slot == id) {
            self.stage_slots.remove(index);
        } else if self.stage_slots.len() < self.stage_layout.capacity() {
            self.stage_slots.push(id);
        } else if let Some(last) = self.stage_slots.last_mut() {
            *last = id;
        }
        self.persist_personal_state();
        cx.notify();
    }

    fn select_node(&mut self, node_id: Uuid, cx: &mut Context<Self>) {
        self.selected_node = Some(node_id);
        self.sync_agent_selection(cx);
        if let Some(agent) = self.agents.get(&node_id) {
            agent.update(cx, |agent, cx| agent.mark_read(cx));
        }
        self.persist_personal_state();
        cx.notify();
    }

    fn close_node(&mut self, node_id: Uuid, cx: &mut Context<Self>) {
        let kind = self
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .map(|node| node.kind);
        let Some(kind) = kind else {
            return;
        };
        if let Some(key) = self.resource_key_for_node(node_id) {
            match kind {
                NodeKind::Agent => {
                    self.cleanup_resource_intent(key);
                    self.sync_agent_selection(cx);
                    self.persist_personal_state();
                    cx.notify();
                    return;
                }
                NodeKind::Terminal | NodeKind::Browser => {
                    let ended = match kind {
                        NodeKind::Terminal => self
                            .terminals
                            .get(&node_id)
                            .map(|terminal| terminal.read(cx).is_ended()),
                        NodeKind::Browser => self
                            .browsers
                            .get(&node_id)
                            .map(|browser| browser.read(cx).is_ended()),
                        _ => None,
                    };
                    if ended == Some(false) {
                        self.selected_node = Some(node_id);
                        self.sync_agent_selection(cx);
                        self.status = if kind == NodeKind::Terminal {
                            "This terminal is still live. Use End terminal and confirm inside the terminal window before removing it."
                        } else {
                            "This browser session is still live. Use End browser and confirm inside the browser window before removing it."
                        }
                        .into();
                        cx.notify();
                        return;
                    }
                    if ended == Some(true) {
                        self.cleanup_resource_intent(key);
                    } else {
                        self.detach_recovery_resource_intent(key);
                    }
                    self.sync_agent_selection(cx);
                    self.persist_personal_state();
                    cx.notify();
                    return;
                }
                NodeKind::Files | NodeKind::Changes => {}
            }
        }
        let terminal_ended = self
            .terminals
            .get(&node_id)
            .map(|terminal| terminal.read(cx).is_ended());
        if live_resource_blocks_close(kind, terminal_ended) && kind == NodeKind::Terminal {
            self.selected_node = Some(node_id);
            self.sync_agent_selection(cx);
            self.status =
                "This terminal is still live. Use End terminal and confirm inside the terminal window before removing it."
                    .into();
            cx.notify();
            return;
        }
        let browser_ended = self
            .browsers
            .get(&node_id)
            .map(|browser| browser.read(cx).is_ended());
        if live_resource_blocks_close(kind, browser_ended) && kind == NodeKind::Browser {
            self.selected_node = Some(node_id);
            self.sync_agent_selection(cx);
            self.status =
                "This browser session is still live. Use End browser and confirm inside the browser window before removing it."
                    .into();
            cx.notify();
            return;
        }
        self.enqueue_patch(vec![CanvasOperation::RemoveNode {
            node_id: node_id.to_string(),
        }]);
        self.nodes.retain(|node| node.id != node_id);
        self.agents.remove(&node_id);
        self.files.remove(&node_id);
        self.changes.remove(&node_id);
        self.browsers.remove(&node_id);
        self.terminals.remove(&node_id);
        self.terminal_loads.remove(&node_id);
        self.pending_terminals.retain(|(id, _)| *id != node_id);
        self.stage_slots.retain(|id| *id != node_id);
        self.clear_personal_node_state(node_id);
        if self.selected_node == Some(node_id) {
            self.selected_node = None;
        }
        self.sync_agent_selection(cx);
        self.status = "Window removed; durable resources were left running.".into();
        self.persist_personal_state();
        cx.notify();
    }

    fn active_personal_state_key(&self) -> Option<(String, String, String)> {
        let canvas_id = self.canvas_id.clone()?;
        let origin = self
            .client()
            .map(|client| client.base_url().to_owned())
            .unwrap_or_else(|| "component-harness".to_owned());
        let key = DesktopPreferences::canvas_key(&origin, &canvas_id);
        Some((origin, canvas_id, key))
    }

    fn sync_agent_selection(&mut self, cx: &mut Context<Self>) {
        let selected_node = self.selected_node;
        let agents = self
            .agents
            .iter()
            .map(|(node_id, agent)| (*node_id, agent.clone()))
            .collect::<Vec<_>>();
        for (node_id, agent) in agents {
            agent.update(cx, |agent, cx| {
                agent.set_selected(selected_node == Some(node_id), cx)
            });
        }
    }

    fn personal_agent_draft(&self, node_id: Uuid) -> String {
        let Some((_, _, key)) = self.active_personal_state_key() else {
            return String::new();
        };
        self.preferences
            .canvases
            .get(&key)
            .map(|state| state.draft(node_id))
            .unwrap_or_default()
    }

    fn personal_agent_scroll_offset(&self, node_id: Uuid) -> f32 {
        let Some((_, _, key)) = self.active_personal_state_key() else {
            return 0.0;
        };
        self.preferences
            .canvases
            .get(&key)
            .map(|state| state.scroll_offset(node_id))
            .unwrap_or_default()
    }

    fn handle_agent_surface_event(
        &mut self,
        scope: &CanvasAsyncScope,
        node_id: Uuid,
        event: &AgentSurfaceEvent,
        cx: &mut Context<Self>,
    ) {
        if !self.agents.contains_key(&node_id)
            || !self
                .nodes
                .iter()
                .any(|node| node.id == node_id && node.kind == NodeKind::Agent)
        {
            return;
        }
        let Some((_, _, key)) = self.active_personal_state_key() else {
            return;
        };
        let state = self.preferences.canvases.entry(key).or_default();
        match event {
            AgentSurfaceEvent::DraftChanged(draft) => match store_agent_draft_if_current(
                state,
                scope,
                self.canvas_generation,
                self.canvas_id.as_deref(),
                node_id,
                Some(draft),
            ) {
                DraftStoreResult::Stale => {}
                DraftStoreResult::Full => {
                    self.status =
                        "Draft was not saved because this Canvas already has 64 Agent drafts."
                            .into();
                    cx.notify();
                }
                DraftStoreResult::Stored => self.schedule_personal_persist(cx),
            },
            AgentSurfaceEvent::DraftCleared => match store_agent_draft_if_current(
                state,
                scope,
                self.canvas_generation,
                self.canvas_id.as_deref(),
                node_id,
                None,
            ) {
                DraftStoreResult::Stale => {}
                DraftStoreResult::Full => unreachable!("clearing a draft cannot fill storage"),
                DraftStoreResult::Stored => {
                    self.personal_persist_task.take();
                    self.persist_personal_state();
                }
            },
            AgentSurfaceEvent::ScrollChanged(offset) => match store_agent_scroll_if_current(
                state,
                scope,
                self.canvas_generation,
                self.canvas_id.as_deref(),
                node_id,
                *offset,
            ) {
                ScrollStoreResult::Stale => {}
                ScrollStoreResult::Full => {
                    self.status =
                        "Scroll position was not saved because this Canvas already has 128 saved Agent positions."
                            .into();
                    cx.notify();
                }
                ScrollStoreResult::Stored => self.schedule_personal_persist(cx),
            },
        }
    }

    fn schedule_personal_persist(&mut self, cx: &mut Context<Self>) {
        self.personal_persist_task = Some(cx.spawn(async move |this, cx| {
            Timer::after(DRAFT_PERSIST_DELAY).await;
            let _ = this.update(cx, |app, _| {
                app.persist_personal_state();
                app.personal_persist_task = None;
            });
        }));
    }

    fn clear_personal_node_state(&mut self, node_id: Uuid) -> bool {
        let Some((_, _, key)) = self.active_personal_state_key() else {
            return false;
        };
        self.preferences
            .canvases
            .get_mut(&key)
            .is_some_and(|state| state.clear_node_state(node_id))
    }

    fn persist_personal_state(&mut self) {
        if !self.preferences_loaded {
            return;
        }
        self.capture_personal_state();
        self.pending_preferences_write = Some(self.preferences.clone());
        self.kick_preferences_write();
    }

    fn capture_personal_state(&mut self) {
        let Some((origin, canvas_id, key)) = self.active_personal_state_key() else {
            return;
        };
        let state = CanvasPersonalState {
            camera: self.camera,
            selected_node: self.selected_node,
            stage_layout: self.stage_layout,
            stage_slots: self.stage_slots.clone(),
            pulse_open: self.pulse_open,
            background: self.background_preferences.clone(),
            ..self
                .preferences
                .canvases
                .get(&key)
                .cloned()
                .unwrap_or_default()
        };
        self.preferences.canvases.insert(key.clone(), state);
        self.preferences.touch_canvas(&key);
        self.preferences
            .last_canvas_by_server
            .insert(origin, canvas_id);
    }

    fn kick_preferences_write(&mut self) {
        if self
            .preferences_write_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        let _ = self.preferences_write_task.take();
        let Some(snapshot) = self.pending_preferences_write.take() else {
            return;
        };
        self.preferences_write_generation = self.preferences_write_generation.wrapping_add(1);
        let generation = self.preferences_write_generation;
        let tx = self.event_tx.clone();
        self.preferences_write_task = Some(self.runtime.spawn(async move {
            let result = tokio::task::spawn_blocking(move || snapshot.save_default())
                .await
                .map_err(|error| format!("personal state task failed: {error}"))
                .and_then(|result| result.map_err(|error| format!("{error:#}")));
            let _ = tx.try_send(UiEvent::PreferencesSaved { generation, result });
        }));
    }

    fn active_theme_id(&self) -> &str {
        self.themes.resolve_id(
            self.background_preferences.per_canvas_override.as_deref(),
            Some(&self.shared_theme_id),
        )
    }

    fn active_theme(&self) -> &VisualThemeV1 {
        self.themes.get(self.active_theme_id())
    }

    fn active_palette(&self) -> CanvasPalette {
        CanvasPalette::from_theme(self.active_theme())
    }

    fn sync_surface_palettes(&self, palette: CanvasPalette, cx: &mut Context<Self>) {
        apply_component_theme(palette, cx);
        for surface in self.agents.values() {
            surface.update(cx, |surface, cx| surface.set_palette(palette, cx));
        }
        for surface in self.terminals.values() {
            surface.update(cx, |surface, cx| surface.set_palette(palette, cx));
        }
        for surface in self.browsers.values() {
            surface.update(cx, |surface, cx| surface.set_palette(palette, cx));
        }
        for surface in self.files.values() {
            surface.update(cx, |surface, cx| surface.set_palette(palette, cx));
        }
        for surface in self.changes.values() {
            surface.update(cx, |surface, cx| surface.set_palette(palette, cx));
        }
    }

    fn request_active_background_image(&mut self) {
        let theme_id = self.active_theme_id().to_owned();
        let asset = match &self.themes.get(&theme_id).background {
            BackgroundSpecV1::Image { asset, .. } => asset.clone(),
            _ => {
                self.background_image_generation = self.background_image_generation.wrapping_add(1);
                if let Some(task) = self.background_image_task.take() {
                    task.abort();
                }
                self.background_image = None;
                return;
            }
        };
        if self
            .background_image
            .as_ref()
            .is_some_and(|loaded| loaded.theme_id == theme_id && loaded.asset == asset)
        {
            return;
        }
        self.background_image_generation = self.background_image_generation.wrapping_add(1);
        let generation = self.background_image_generation;
        if let Some(task) = self.background_image_task.take() {
            task.abort();
        }
        self.background_image = None;
        let themes = self.themes.clone();
        let load_theme_id = theme_id.clone();
        let tx = self.event_tx.clone();
        self.background_image_task = Some(self.runtime.spawn(async move {
            let result = tokio::task::spawn_blocking(move || themes.load_image(&load_theme_id))
                .await
                .map_err(|error| format!("theme image task failed: {error}"))
                .and_then(|result| result.map_err(|error| format!("{error:#}")));
            let _ = tx
                .send_async(UiEvent::BackgroundImageLoaded {
                    generation,
                    theme_id,
                    asset,
                    result,
                })
                .await;
        }));
    }

    fn set_personal_theme_override(&mut self, theme_id: Option<String>, cx: &mut Context<Self>) {
        if theme_id
            .as_deref()
            .is_some_and(|theme_id| !self.themes.contains(theme_id))
        {
            self.status = "That local theme is unavailable.".into();
            return;
        }
        let previous_palette = self.active_palette();
        self.background_preferences.per_canvas_override = theme_id;
        self.request_active_background_image();
        self.persist_personal_state();
        if let Some(palette) = changed_palette(previous_palette, self.active_palette()) {
            self.sync_surface_palettes(palette, cx);
        }
    }

    fn set_shared_theme(&mut self, theme_id: String, cx: &mut Context<Self>) {
        if !self.themes.contains(&theme_id) || !self.themes.get(&theme_id).shareable {
            self.status = "That theme cannot be shared on this canvas.".into();
            return;
        }
        let previous_palette = self.active_palette();
        self.shared_theme_id = theme_id.clone();
        self.request_active_background_image();
        self.enqueue_patch(vec![CanvasOperation::SetSharedTheme { theme_id }]);
        if let Some(palette) = changed_palette(previous_palette, self.active_palette()) {
            self.sync_surface_palettes(palette, cx);
        }
    }

    fn set_motion_preference(&mut self, motion: MotionPreference) {
        self.motion = motion;
        self.background_preferences.motion = motion;
        self.persist_personal_state();
    }

    fn active_effect_speed(&self) -> Option<f32> {
        match &self.active_theme().background {
            BackgroundSpecV1::NativeEffect { effect, speed, .. }
                if effect == PULSE_EFFECT_ID && self.motion.frames_per_second() > 0 =>
            {
                Some(*speed)
            }
            _ => None,
        }
    }

    fn background_should_yield(&self, cx: &App) -> bool {
        self.gesture_active
            || self.last_input_activity.elapsed() < Duration::from_millis(180)
            || self.any_agent_streaming(cx)
            || self
                .terminals
                .values()
                .any(|terminal| !terminal.read(cx).is_ended())
            || self
                .browsers
                .values()
                .any(|browser| !browser.read(cx).is_ended())
    }

    fn any_agent_streaming(&self, cx: &App) -> bool {
        self.agents
            .values()
            .any(|agent| agent.read(cx).is_streaming())
    }

    #[cfg(feature = "component-harness")]
    fn seed_component_harness(&mut self) {
        self.project_dir = Some("/component-harness/project".to_owned());
        self.project_name = "Component Harness".to_owned();
        self.canvas_id = Some("component-harness-main".to_owned());
        self.nodes = vec![
            SpatialNode::new(
                NodeKind::Agent,
                "Build Agent",
                WorldRect::new(-160.0, -100.0, 660.0, 520.0),
                1,
            ),
            SpatialNode::new(
                NodeKind::Terminal,
                "Dev Server",
                WorldRect::new(560.0, -30.0, 720.0, 420.0),
                2,
            ),
            SpatialNode::new(
                NodeKind::Changes,
                "Changes",
                WorldRect::new(180.0, 470.0, 760.0, 540.0),
                3,
            ),
        ];
        self.nodes[0].status = NodeStatus::Working;
        self.nodes[1].status = NodeStatus::NeedsYou;
    }

    fn render_background(&self) -> AnyElement {
        let palette = self.active_palette();
        let dim = self.background_preferences.dim.clamp(0.0, 1.0);
        let background = match &self.active_theme().background {
            BackgroundSpecV1::Solid { color } => div()
                .absolute()
                .inset_0()
                .bg(parse_theme_color(color).unwrap_or(palette.canvas))
                .into_any_element(),
            BackgroundSpecV1::Gradient { angle_deg, stops } => div()
                .absolute()
                .inset_0()
                .bg(theme_gradient(*angle_deg, stops, palette.canvas))
                .into_any_element(),
            BackgroundSpecV1::Image {
                asset,
                opacity,
                scrim,
            } => {
                let image = self.background_image.as_ref().filter(|loaded| {
                    loaded.theme_id == self.active_theme_id() && loaded.asset == *asset
                });
                div()
                    .absolute()
                    .inset_0()
                    .bg(if image.is_some() {
                        palette.canvas
                    } else {
                        CanvasPalette::default().canvas
                    })
                    .when_some(image, |layer, loaded| {
                        layer.child(
                            img(loaded.image.clone())
                                .absolute()
                                .inset_0()
                                .size_full()
                                .object_fit(ObjectFit::Cover)
                                .opacity(opacity.clamp(0.0, 1.0)),
                        )
                    })
                    .when_some(
                        scrim.as_deref().and_then(parse_theme_color),
                        |layer, scrim| layer.child(div().absolute().inset_0().bg(scrim)),
                    )
                    .into_any_element()
            }
            BackgroundSpecV1::NativeEffect {
                effect,
                seed,
                intensity,
                parallax,
                fallback,
                ..
            } => {
                let animated = effect == PULSE_EFFECT_ID && self.motion.frames_per_second() > 0;
                let intensity = self
                    .background_preferences
                    .intensity_override
                    .unwrap_or(*intensity)
                    .clamp(0.0, 1.0);
                let seed_phase = (*seed % 360) as f32 * std::f32::consts::PI / 180.0;
                let phase = self.aura_phase * std::f32::consts::TAU + seed_phase;
                let parallax_x =
                    (self.camera.world_origin.x * f64::from(*parallax)).clamp(-180.0, 180.0) as f32;
                let parallax_y =
                    (self.camera.world_origin.y * f64::from(*parallax)).clamp(-140.0, 140.0) as f32;
                let x = if animated { phase.sin() * 70.0 } else { 0.0 } - parallax_x;
                let y = if animated { phase.cos() * 45.0 } else { 0.0 } - parallax_y;
                div()
                    .absolute()
                    .inset_0()
                    .overflow_hidden()
                    .bg(static_theme_background(fallback, palette.canvas))
                    .when(animated, |layer| {
                        layer
                            .child(
                                div()
                                    .absolute()
                                    .left(px(-300.0 + x))
                                    .top(px(-380.0 + y))
                                    .w(px(900.0))
                                    .h(px(760.0))
                                    .rounded(px(999.0))
                                    .bg(palette.pulse)
                                    .opacity(0.084 * intensity),
                            )
                            .child(
                                div()
                                    .absolute()
                                    .right(px(-360.0 - x * 0.6))
                                    .bottom(px(-420.0 - y * 0.4))
                                    .w(px(1050.0))
                                    .h(px(820.0))
                                    .rounded(px(999.0))
                                    .bg(palette.pulse)
                                    .opacity(0.052 * intensity),
                            )
                    })
                    .into_any_element()
            }
        };
        div()
            .absolute()
            .inset_0()
            .child(background)
            .when(dim > 0.0, |layer| {
                layer.child(
                    div()
                        .absolute()
                        .inset_0()
                        .bg(CanvasPalette::default().canvas.opacity(dim)),
                )
            })
            .into_any_element()
    }

    fn render_titlebar(&self) -> impl IntoElement {
        let palette = self.active_palette();
        div()
            .flex()
            .items_center()
            .justify_between()
            .h(px(TITLE_HEIGHT))
            .pl(px(10.0))
            .bg(palette.canvas)
            .border_b_1()
            .border_color(palette.edge)
            .on_mouse_down(MouseButton::Left, |_, window, _| window.start_window_move())
            .child(
                div()
                    .text_xs()
                    .text_color(palette.text_muted)
                    .child("Mitsuro Canvas"),
            )
            .child(
                div()
                    .flex()
                    .h_full()
                    .items_center()
                    .child(
                        Button::new("window-minimize")
                            .label("—")
                            .compact()
                            .on_click(|_, window, _| window.minimize_window()),
                    )
                    .child(
                        Button::new("window-maximize")
                            .label("□")
                            .compact()
                            .on_click(|_, window, _| window.zoom_window()),
                    )
                    .child(
                        Button::new("window-close")
                            .label("×")
                            .compact()
                            .on_click(|_, window, _| window.remove_window()),
                    ),
            )
    }

    fn render_project_switcher(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.active_palette();
        let weak = cx.weak_entity();
        let open_weak = weak.clone();
        let canvases = self
            .canvases
            .iter()
            .filter(|canvas| canvas.archived_at.is_none())
            .cloned()
            .collect::<Vec<_>>();
        let projects = self.projects.clone();
        let browse = self.browse.clone();
        let browse_start = self.project_dir.clone();
        let canvas_name_input = self.canvas_name_input.clone();
        let current_canvas = self.canvas_id.clone();
        let active_theme_id = self.active_theme_id().to_owned();
        let shared_theme_id = self.shared_theme_id.clone();
        let personal_theme_id = self.background_preferences.per_canvas_override.clone();
        let motion = self.motion;
        let themes = self
            .themes
            .iter()
            .map(|(id, theme)| (id.to_owned(), theme.display_name.clone(), theme.shareable))
            .collect::<Vec<_>>();
        let trigger = Button::new("project-canvas-switcher-trigger")
            .label(format!("{}  /  {}  ▾", self.project_name, self.canvas_name))
            .compact();
        div().absolute().top(px(10.0)).left(px(12.0)).child(
            Popover::new("project-canvas-switcher")
                .open(self.switcher_open)
                .on_open_change(move |open, _, cx| {
                    let _ = open_weak.update(cx, |app, cx| {
                        app.switcher_open = *open;
                        cx.notify();
                    });
                })
                .trigger(trigger)
                .content(move |_, _window, _| {
                    let canvas_rows = canvases.clone();
                    let project_rows = projects.clone();
                    let browse = browse.clone();
                    let themes = themes.clone();
                    let current_canvas = current_canvas.clone();
                    let active_theme_id = active_theme_id.clone();
                    let shared_theme_id = shared_theme_id.clone();
                    let personal_theme_id = personal_theme_id.clone();
                    let weak_for_browse = weak.clone();
                    let browse_start = browse_start.clone();
                    let name_input = canvas_name_input.clone();

                    div()
                        .flex()
                        .flex_col()
                        .w(px(420.0))
                        .max_h(px(620.0))
                        .overflow_y_scrollbar()
                        .p(px(12.0))
                        .gap(px(12.0))
                        .bg(palette.surface_high)
                        .text_color(palette.text)
                        .child(switcher_section_label("Canvases", palette))
                        .child(div().flex().flex_col().gap(px(3.0)).children(
                            canvas_rows.into_iter().map(|canvas| {
                                let canvas_id = canvas.id.clone();
                                let path_label = Path::new(&canvas.project_dir)
                                    .file_name()
                                    .and_then(|name| name.to_str())
                                    .unwrap_or(&canvas.project_dir)
                                    .to_owned();
                                let selected =
                                    current_canvas.as_deref() == Some(canvas.id.as_str());
                                let weak = weak.clone();
                                Button::new(SharedString::from(format!(
                                    "switch-canvas-{}",
                                    canvas.id
                                )))
                                .label(format!(
                                    "{}{}  /  {}",
                                    if selected { "• " } else { "" },
                                    path_label,
                                    canvas.name
                                ))
                                .compact()
                                .on_click(move |_, _, cx| {
                                    let _ = weak.update(cx, |app, cx| {
                                        app.open_canvas_by_id(&canvas_id, cx);
                                        cx.notify();
                                    });
                                })
                            }),
                        ))
                        .child(switcher_section_label("Projects", palette))
                        .child(div().flex().flex_col().gap(px(3.0)).children(
                            project_rows.into_iter().map(|project| {
                                let project_dir = project.project_dir.clone();
                                let weak = weak.clone();
                                Button::new(SharedString::from(format!(
                                    "switch-project-{}",
                                    project.project_dir
                                )))
                                .label(format!(
                                    "{}  ·  {} canvas{}",
                                    project.name,
                                    project.canvas_count,
                                    if project.canvas_count == 1 { "" } else { "es" }
                                ))
                                .compact()
                                .disabled(!project.available)
                                .on_click(move |_, _, cx| {
                                    let _ = weak.update(cx, |app, cx| {
                                        app.switcher_open = false;
                                        app.open_project(project_dir.clone(), cx);
                                        cx.notify();
                                    });
                                })
                            }),
                        ))
                        .child(
                            Button::new("browse-server-projects")
                                .label("Browse server folders")
                                .compact()
                                .on_click(move |_, _, cx| {
                                    let _ = weak_for_browse.update(cx, |app, cx| {
                                        app.browse_directory(browse_start.clone());
                                        cx.notify();
                                    });
                                }),
                        )
                        .when_some(browse, |panel, browse| {
                            let current = browse.current.clone();
                            let parent = browse.parent.clone();
                            let weak_open = weak.clone();
                            panel.child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap(px(4.0))
                                    .border_t_1()
                                    .border_color(palette.edge)
                                    .pt(px(9.0))
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(palette.text_muted)
                                            .child(current.clone()),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .gap(px(5.0))
                                            .when_some(parent, |row, parent| {
                                                let weak = weak.clone();
                                                row.child(
                                                    Button::new("browse-server-parent")
                                                        .label("Up")
                                                        .compact()
                                                        .on_click(move |_, _, cx| {
                                                            let _ = weak.update(cx, |app, cx| {
                                                                app.browse_directory(Some(
                                                                    parent.clone(),
                                                                ));
                                                                cx.notify();
                                                            });
                                                        }),
                                                )
                                            })
                                            .child(
                                                Button::new("open-browsed-project")
                                                    .label("Open project")
                                                    .compact()
                                                    .on_click(move |_, _, cx| {
                                                        let _ = weak_open.update(cx, |app, cx| {
                                                            app.switcher_open = false;
                                                            app.open_project(current.clone(), cx);
                                                            cx.notify();
                                                        });
                                                    }),
                                            ),
                                    )
                                    .children(browse.directories.into_iter().map(|directory| {
                                        let path = directory.path.clone();
                                        let weak = weak.clone();
                                        Button::new(SharedString::from(format!(
                                            "browse-directory-{}",
                                            directory.path
                                        )))
                                        .label(directory.name)
                                        .compact()
                                        .on_click(
                                            move |_, _, cx| {
                                                let _ = weak.update(cx, |app, cx| {
                                                    app.browse_directory(Some(path.clone()));
                                                    cx.notify();
                                                });
                                            },
                                        )
                                    })),
                            )
                        })
                        .child(switcher_section_label("New canvas", palette))
                        .child(
                            div()
                                .flex()
                                .gap(px(6.0))
                                .child(
                                    div()
                                        .flex_1()
                                        .h(px(30.0))
                                        .child(Input::new(&name_input).appearance(false)),
                                )
                                .child({
                                    let weak = weak.clone();
                                    Button::new("create-named-canvas")
                                        .label("Create")
                                        .compact()
                                        .on_click(move |_, window, cx| {
                                            let _ = weak.update(cx, |app, app_cx| {
                                                app.create_named_canvas_from_input(window, app_cx);
                                            });
                                        })
                                }),
                        )
                        .child(switcher_section_label("Appearance", palette))
                        .child(
                            div()
                                .text_xs()
                                .text_color(palette.text_muted)
                                .child(format!(
                                    "Active: {active_theme_id}  ·  Shared: {shared_theme_id}"
                                )),
                        )
                        .child({
                            let weak = weak.clone();
                            Button::new("theme-use-shared")
                                .label(if personal_theme_id.is_none() {
                                    "• Follow shared theme"
                                } else {
                                    "Follow shared theme"
                                })
                                .compact()
                                .on_click(move |_, _, cx| {
                                    let _ = weak.update(cx, |app, cx| {
                                        app.set_personal_theme_override(None, cx);
                                        cx.notify();
                                    });
                                })
                        })
                        .children(themes.into_iter().map(|(id, name, shareable)| {
                            let selected = personal_theme_id.as_deref() == Some(id.as_str());
                            let personal_id = id.clone();
                            let weak_personal = weak.clone();
                            let weak_shared = weak.clone();
                            div()
                                .flex()
                                .gap(px(5.0))
                                .child(
                                    Button::new(SharedString::from(format!("theme-personal-{id}")))
                                        .label(format!(
                                            "{}{}",
                                            if selected { "• " } else { "" },
                                            name
                                        ))
                                        .compact()
                                        .on_click(move |_, _, cx| {
                                            let _ = weak_personal.update(cx, |app, cx| {
                                                app.set_personal_theme_override(
                                                    Some(personal_id.clone()),
                                                    cx,
                                                );
                                                cx.notify();
                                            });
                                        }),
                                )
                                .when(shareable, |row| {
                                    row.child(
                                        Button::new(SharedString::from(format!(
                                            "theme-shared-{id}"
                                        )))
                                        .label("Share")
                                        .compact()
                                        .on_click(
                                            move |_, _, cx| {
                                                let _ = weak_shared.update(cx, |app, cx| {
                                                    app.set_shared_theme(id.clone(), cx);
                                                    cx.notify();
                                                });
                                            },
                                        ),
                                    )
                                })
                        }))
                        .child(div().flex().gap(px(4.0)).children([
                            motion_button(
                                "motion-full",
                                "30 fps",
                                MotionPreference::Full,
                                motion,
                                weak.clone(),
                            ),
                            motion_button(
                                "motion-economy",
                                "15 fps",
                                MotionPreference::Economy,
                                motion,
                                weak.clone(),
                            ),
                            motion_button(
                                "motion-reduced",
                                "Static",
                                MotionPreference::Reduced,
                                motion,
                                weak.clone(),
                            ),
                            motion_button(
                                "motion-off",
                                "Off",
                                MotionPreference::Off,
                                motion,
                                weak.clone(),
                            ),
                        ]))
                }),
        )
    }

    fn render_node(
        &self,
        node: &SpatialNode,
        level: SemanticLevel,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let palette = self.active_palette();
        let live_status = self.live_node_status(node, cx);
        let screen = self
            .camera
            .world_to_screen(node.world_bounds.origin, WorldPoint::default());
        let width = node.world_bounds.size.width * self.camera.zoom;
        let height = node.world_bounds.size.height * self.camera.zoom;
        let node_id = node.id;
        let close_id = node.id;
        let resize_id = node.id;
        let selected = self.selected_node == Some(node.id);
        let staged = self.stage_slots.contains(&node.id);
        let summary: SharedString = self
            .agents
            .get(&node.id)
            .map(|agent| agent.read(cx).compact_summary())
            .or_else(|| {
                self.terminals
                    .get(&node.id)
                    .map(|terminal| terminal.read(cx).compact_summary())
            })
            .or_else(|| {
                self.browsers
                    .get(&node.id)
                    .map(|browser| browser.read(cx).compact_summary())
            })
            .unwrap_or_else(|| generic_summary(node, live_status).into());
        let detail = self
            .agents
            .get(&node.id)
            .cloned()
            .map(|entity| div().size_full().child(entity).into_any_element())
            .or_else(|| {
                self.files
                    .get(&node.id)
                    .cloned()
                    .map(|entity| div().size_full().child(entity).into_any_element())
            })
            .or_else(|| {
                self.changes
                    .get(&node.id)
                    .cloned()
                    .map(|entity| div().size_full().child(entity).into_any_element())
            })
            .or_else(|| {
                self.browsers
                    .get(&node.id)
                    .cloned()
                    .map(|entity| div().size_full().child(entity).into_any_element())
            })
            .or_else(|| {
                self.terminals
                    .get(&node.id)
                    .cloned()
                    .map(|entity| div().size_full().child(entity).into_any_element())
            });

        div()
            .id(SharedString::from(format!("node-{}", node.id)))
            .absolute()
            .flex()
            .flex_col()
            .left(px(screen.x as f32))
            .top(px(screen.y as f32))
            .w(px(width.max(120.0) as f32))
            .h(px(height.max(80.0) as f32))
            .min_w(px(120.0))
            .min_h(px(80.0))
            .overflow_hidden()
            .rounded(px(4.0))
            .border_1()
            .border_color(if selected {
                palette.pulse
            } else {
                palette.edge_strong
            })
            .bg(palette.surface)
            .occlude()
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(px(34.0))
                    .px(px(10.0))
                    .border_b_1()
                    .border_color(palette.edge)
                    .cursor_move()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |app, event, _, cx| {
                            app.begin_move(node_id, event, cx);
                            cx.stop_propagation();
                        }),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(px(7.0))
                            .child(
                                div()
                                    .size(px(6.0))
                                    .rounded(px(99.0))
                                    .bg(status_color(live_status, palette)),
                            )
                            .child(node.title.clone()),
                    )
                    .child(
                        Button::new(SharedString::from(format!("close-{close_id}")))
                            .label(if staged { "Stage · ×" } else { "×" })
                            .compact()
                            .on_click(
                                cx.listener(move |app, _, _, cx| app.close_node(close_id, cx)),
                            ),
                    ),
            )
            .child(match level {
                SemanticLevel::Glance => div()
                    .flex()
                    .flex_col()
                    .justify_center()
                    .p(px(12.0))
                    .text_sm()
                    .text_color(palette.text_muted)
                    .child(node.kind.label())
                    .child(summary)
                    .into_any_element(),
                SemanticLevel::Summary => div()
                    .flex()
                    .flex_col()
                    .p(px(14.0))
                    .gap(px(8.0))
                    .child(
                        div()
                            .text_xs()
                            .text_color(palette.pulse)
                            .child(node.kind.label()),
                    )
                    .child(summary)
                    .into_any_element(),
                SemanticLevel::Detail => div()
                    .flex_1()
                    .min_h_0()
                    .child(detail.unwrap_or_else(|| render_generic_detail(node, palette)))
                    .into_any_element(),
            })
            .child(
                div()
                    .absolute()
                    .right(px(0.0))
                    .bottom(px(0.0))
                    .size(px(16.0))
                    .cursor_e_resize()
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |app, event, _, cx| {
                            app.begin_resize(resize_id, event, cx);
                            cx.stop_propagation();
                        }),
                    ),
            )
            .into_any_element()
    }

    fn render_geometry_preview(&self, preview: &CanvasGeometryPreview) -> AnyElement {
        let palette = self.active_palette();
        let origin = self.camera.world_to_screen(
            WorldPoint::new(preview.rect.x, preview.rect.y),
            WorldPoint::default(),
        );
        let owner = self
            .collaboration
            .clients
            .get(&preview.client_id)
            .and_then(|client| client.display_name.as_deref())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("Another client");
        div()
            .absolute()
            .left(px(origin.x as f32))
            .top(px(origin.y as f32))
            .w(px((preview.rect.width * self.camera.zoom).max(120.0) as f32))
            .h(px((preview.rect.height * self.camera.zoom).max(80.0) as f32))
            .border_1()
            .border_color(palette.pulse.opacity(0.82))
            .bg(palette.pulse.opacity(0.035))
            .child(
                div()
                    .absolute()
                    .top(px(5.0))
                    .left(px(7.0))
                    .px(px(6.0))
                    .py(px(2.0))
                    .rounded(px(3.0))
                    .bg(palette.surface_high)
                    .text_xs()
                    .text_color(palette.pulse)
                    .child(format!("{owner} · moving")),
            )
            .into_any_element()
    }

    fn render_remote_cursor(&self, client: &CanvasPresenceClient) -> Option<AnyElement> {
        if client.client_id == self.client_id || client.stale {
            return None;
        }
        let cursor = client.cursor?;
        let palette = self.active_palette();
        let position = self
            .camera
            .world_to_screen(WorldPoint::new(cursor.x, cursor.y), WorldPoint::default());
        let label = client
            .display_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("Canvas client");
        Some(
            div()
                .absolute()
                .left(px(position.x as f32))
                .top(px(position.y as f32))
                .flex()
                .items_start()
                .gap(px(5.0))
                .child(
                    div()
                        .mt(px(2.0))
                        .size(px(8.0))
                        .rounded(px(2.0))
                        .bg(palette.pulse),
                )
                .child(
                    div()
                        .px(px(6.0))
                        .py(px(2.0))
                        .rounded(px(3.0))
                        .bg(palette.surface_high)
                        .text_xs()
                        .text_color(palette.text)
                        .child(label.to_owned()),
                )
                .into_any_element(),
        )
    }

    fn render_pulse_rail(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.active_palette();
        let expanded = self.pulse_open || self.pulse_hovered;
        let width = if expanded { 210.0 } else { PULSE_RAIL_WIDTH };
        let needs = self
            .nodes
            .iter()
            .filter(|node| {
                matches!(
                    self.live_node_status(node, cx),
                    NodeStatus::NeedsYou | NodeStatus::Disconnected | NodeStatus::Ended
                )
            })
            .collect::<Vec<_>>();
        let agents = self
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Agent)
            .collect::<Vec<_>>();
        let windows = self
            .nodes
            .iter()
            .filter(|node| node.kind != NodeKind::Agent)
            .collect::<Vec<_>>();

        div()
            .id("pulse-rail")
            .absolute()
            .left(px(0.0))
            .top(px(CANVAS_OVERLAY_TOP))
            .bottom(px(CANVAS_OVERLAY_BOTTOM))
            .w(px(width))
            .overflow_hidden()
            .border_r_1()
            .border_color(palette.edge)
            .bg(palette.surface)
            .on_hover(cx.listener(|app, hovering, _, cx| {
                app.pulse_hovered = *hovering;
                cx.notify();
            }))
            .child(
                Button::new("pulse-pin")
                    .label(if expanded { "PULSE" } else { "P" })
                    .compact()
                    .on_click(cx.listener(|app, _, _, cx| {
                        app.pulse_open = !app.pulse_open;
                        app.persist_personal_state();
                        cx.notify();
                    })),
            )
            .when(expanded, |rail| {
                rail.child(self.render_pulse_group("Needs You", needs, cx))
                    .child(self.render_pulse_group("Agents", agents, cx))
                    .child(self.render_pulse_group("Windows", windows, cx))
            })
    }

    fn render_pulse_group(
        &self,
        label: &'static str,
        nodes: Vec<&SpatialNode>,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let palette = self.active_palette();
        div()
            .flex()
            .flex_col()
            .px(px(8.0))
            .pb(px(10.0))
            .child(
                div()
                    .px(px(6.0))
                    .pb(px(4.0))
                    .text_xs()
                    .text_color(palette.text_muted)
                    .child(label),
            )
            .children(nodes.into_iter().map(|node| {
                let id = node.id;
                let status = self.live_node_status(node, cx);
                let label = if status == NodeStatus::Idle {
                    node.title.clone()
                } else {
                    format!("{} · {}", node.title, status_label(status))
                };
                Button::new(SharedString::from(format!("pulse-{label}-{id}")))
                    .label(label)
                    .compact()
                    .on_click(cx.listener(move |app, _, _, cx| {
                        app.select_node(id, cx);
                    }))
            }))
    }

    fn live_node_status(&self, node: &SpatialNode, cx: &App) -> NodeStatus {
        match node.kind {
            NodeKind::Agent => self
                .agents
                .get(&node.id)
                .map(|agent| {
                    let agent = agent.read(cx);
                    agent_node_status(agent.is_streaming(), agent.needs_attention())
                })
                .unwrap_or(node.status),
            NodeKind::Terminal => self
                .terminals
                .get(&node.id)
                .map(|terminal| {
                    let terminal = terminal.read(cx);
                    terminal_node_status(terminal.snapshot().connection, terminal.needs_attention())
                })
                .unwrap_or(node.status),
            NodeKind::Browser => self
                .browsers
                .get(&node.id)
                .map(|browser| {
                    let browser = browser.read(cx);
                    browser_node_status(
                        browser.is_ended(),
                        browser.is_disconnected(),
                        browser.is_working(),
                    )
                })
                .unwrap_or(node.status),
            NodeKind::Files => self
                .files
                .get(&node.id)
                .map(|files| {
                    let files = files.read(cx);
                    activity_node_status(files.is_loading(), files.needs_attention())
                })
                .unwrap_or(node.status),
            NodeKind::Changes => self
                .changes
                .get(&node.id)
                .map(|changes| {
                    let changes = changes.read(cx);
                    activity_node_status(changes.is_loading(), changes.needs_attention())
                })
                .unwrap_or(node.status),
        }
    }

    fn render_stage_rail(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.active_palette();
        div()
            .absolute()
            .right(px(0.0))
            .top(px(62.0))
            .bottom(px(76.0))
            .w(px(STAGE_RAIL_WIDTH))
            .flex()
            .flex_col()
            .items_center()
            .gap(px(6.0))
            .pt(px(8.0))
            .border_l_1()
            .border_color(palette.edge)
            .bg(palette.surface)
            .child(div().text_xs().text_color(palette.text_muted).child("STG"))
            .child(
                Button::new("stage-selected")
                    .label("+")
                    .compact()
                    .on_click(cx.listener(|app, _, _, cx| app.stage_selected(cx))),
            )
            .child(
                Button::new("stage-one").label("1").compact().on_click(
                    cx.listener(|app, _, _, cx| app.set_stage_layout(StageLayout::One, cx)),
                ),
            )
            .child(
                Button::new("stage-two").label("2").compact().on_click(
                    cx.listener(|app, _, _, cx| app.set_stage_layout(StageLayout::Two, cx)),
                ),
            )
            .child(
                Button::new("stage-four").label("4").compact().on_click(
                    cx.listener(|app, _, _, cx| app.set_stage_layout(StageLayout::Four, cx)),
                ),
            )
    }

    fn render_stage(&self, window: &Window, _cx: &mut Context<Self>) -> Option<AnyElement> {
        if self.stage_slots.is_empty() {
            return None;
        }
        let palette = self.active_palette();
        let landscape =
            f32::from(window.viewport_size().width) >= f32::from(window.viewport_size().height);
        let columns = match self.stage_layout {
            StageLayout::One => 1,
            StageLayout::Two if landscape => 2,
            StageLayout::Two => 1,
            StageLayout::Four => 2,
        };
        // Stage mounts the same live Entity handles used on the canvas, so
        // transcripts, subscriptions, and scroll state transfer without a
        // duplicate subscription. Agent scroll offsets are captured by the
        // surface's explicit user-scroll event, never sampled during render.
        let slots =
            self.stage_slots
                .iter()
                .filter_map(|id| self.nodes.iter().find(|node| node.id == *id))
                .map(|node| {
                    let content =
                        self.agents
                            .get(&node.id)
                            .cloned()
                            .map(|agent| div().size_full().child(agent).into_any_element())
                            .or_else(|| {
                                self.files
                                    .get(&node.id)
                                    .cloned()
                                    .map(|files| div().size_full().child(files).into_any_element())
                            })
                            .or_else(|| {
                                self.changes.get(&node.id).cloned().map(|changes| {
                                    div().size_full().child(changes).into_any_element()
                                })
                            })
                            .or_else(|| {
                                self.browsers.get(&node.id).cloned().map(|browser| {
                                    div().size_full().child(browser).into_any_element()
                                })
                            })
                            .or_else(|| {
                                self.terminals.get(&node.id).cloned().map(|terminal| {
                                    div().size_full().child(terminal).into_any_element()
                                })
                            })
                            .unwrap_or_else(|| render_generic_detail(node, palette));
                    div()
                        .min_h_0()
                        .min_w_0()
                        .overflow_hidden()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(palette.edge_strong)
                        .bg(palette.surface)
                        .child(content)
                })
                .collect::<Vec<_>>();
        Some(
            div()
                .absolute()
                .left(px(66.0))
                .right(px(60.0))
                .top(px(52.0))
                .bottom(px(76.0))
                .grid()
                .grid_cols(columns)
                .gap(px(8.0))
                .p(px(8.0))
                .bg(palette.canvas)
                .border_1()
                .border_color(palette.edge)
                .children(slots)
                .into_any_element(),
        )
    }

    fn render_command_shelf(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.active_palette();
        div()
            .absolute()
            .bottom(px(14.0))
            .left_1_2()
            .ml(px(-COMMAND_SHELF_WIDTH / 2.0))
            .w(px(COMMAND_SHELF_WIDTH))
            .p(px(6.0))
            .rounded(px(4.0))
            .border_1()
            .border_color(palette.edge_strong)
            .bg(palette.surface)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(4.0))
                    .child(
                        Button::new("new-agent")
                            .label("+ agent")
                            .compact()
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.execute_command("+agent".to_owned(), cx)
                            })),
                    )
                    .child(
                        Button::new("new-terminal")
                            .label("+ terminal")
                            .compact()
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.execute_command("+terminal".to_owned(), cx)
                            })),
                    )
                    .child(
                        Button::new("new-browser")
                            .label("+ browser")
                            .compact()
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.execute_command("+browser".to_owned(), cx)
                            })),
                    )
                    .child(
                        Button::new("new-files")
                            .label("+ files")
                            .compact()
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.execute_command("+files".to_owned(), cx)
                            })),
                    )
                    .child(
                        Button::new("new-changes")
                            .label("+ changes")
                            .compact()
                            .on_click(cx.listener(|app, _, _, cx| {
                                app.execute_command("+changes".to_owned(), cx)
                            })),
                    )
                    .child(
                        Input::new(&self.command)
                            .appearance(false)
                            .bordered(false)
                            .focus_bordered(false)
                            .flex_1(),
                    ),
            )
            .child(
                div()
                    .px(px(8.0))
                    .pt(px(3.0))
                    .text_xs()
                    .text_color(palette.text_muted)
                    .child(self.status.clone()),
            )
    }

    fn render_empty_state(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.active_palette();
        let (title, body, diagnostic) = match &self.connection {
            ConnectionState::Connecting => (
                "Finding Mitsuro",
                "Checking the canonical server before opening your last canvas.",
                None,
            ),
            ConnectionState::Ready(_) => (
                "Open a project",
                "Recent Projects and the server filesystem chooser will appear here. Product mode never invents local fixture data.",
                None,
            ),
            ConnectionState::Blocked(error) => {
                let title = match error.kind {
                    BootstrapFailureKind::Occupied => "Existing server in the way",
                    BootstrapFailureKind::Incompatible => "Server update required",
                    BootstrapFailureKind::ServiceFailed => "Could not start Mitsuro",
                    BootstrapFailureKind::CompanionMissing => "Mitsuro companion missing",
                    BootstrapFailureKind::CompanionFailed => "Could not start Mitsuro",
                };
                (title, error.message.as_str(), error.diagnostics.as_deref())
            }
            ConnectionState::Harness => (
                "Component harness",
                "Test-only visual data is enabled.",
                None,
            ),
        };
        div()
            .absolute()
            .inset_0()
            .flex()
            .items_center()
            .justify_center()
            .child(
                div()
                    .w(px(520.0))
                    .max_h(px(720.0))
                    .overflow_y_scrollbar()
                    .px(px(28.0))
                    .py(px(24.0))
                    .border_l_1()
                    .border_color(palette.pulse)
                    .child(div().text_2xl().child(title))
                    .child(
                        div()
                            .mt(px(10.0))
                            .text_color(palette.text_muted)
                            .child(body.to_owned()),
                    )
                    .when_some(diagnostic.map(str::to_owned), |panel, diagnostic| {
                        panel.child(
                            div()
                                .mt(px(12.0))
                                .text_xs()
                                .text_color(palette.brass)
                                .child(diagnostic),
                        )
                    })
                    .when(
                        matches!(self.connection, ConnectionState::Ready(_)),
                        |panel| {
                            panel
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap(px(5.0))
                                        .mt(px(18.0))
                                        .children(self.projects.iter().take(8).map(|project| {
                                            let path = project.project_dir.clone();
                                            Button::new(SharedString::from(format!(
                                                "recent-project-{path}"
                                            )))
                                            .label(format!(
                                                "{}{}",
                                                project.name,
                                                if project.available {
                                                    ""
                                                } else {
                                                    " · unavailable"
                                                }
                                            ))
                                            .disabled(!project.available)
                                            .on_click(cx.listener(move |app, _, _, cx| {
                                                app.open_project(path.clone(), cx)
                                            }))
                                        }))
                                        .child(
                                            Button::new("browse-project")
                                                .label("Browse server folders…")
                                                .on_click(cx.listener(|app, _, _, _| {
                                                    app.browse_directory(None)
                                                })),
                                        ),
                                )
                                .when_some(self.browse.as_ref(), |panel, browse| {
                                    let current = browse.current.clone();
                                    panel.child(
                                        div()
                                            .mt(px(12.0))
                                            .p(px(8.0))
                                            .border_t_1()
                                            .border_color(palette.edge)
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(palette.text_muted)
                                                    .child(current.clone()),
                                            )
                                            .child(
                                                div()
                                                    .flex()
                                                    .gap(px(5.0))
                                                    .mt(px(6.0))
                                                    .when_some(
                                                        browse.parent.clone(),
                                                        |row, parent| {
                                                            row.child(
                                                                Button::new("browse-parent")
                                                                    .label("Up")
                                                                    .compact()
                                                                    .on_click(cx.listener(
                                                                        move |app, _, _, _| {
                                                                            app.browse_directory(
                                                                                Some(
                                                                                    parent.clone(),
                                                                                ),
                                                                            )
                                                                        },
                                                                    )),
                                                            )
                                                        },
                                                    )
                                                    .child(
                                                        Button::new("open-current-project")
                                                            .label("Open this project")
                                                            .compact()
                                                            .on_click(cx.listener(
                                                                move |app, _, _, cx| {
                                                                    app.open_project(
                                                                        current.clone(),
                                                                        cx,
                                                                    )
                                                                },
                                                            )),
                                                    ),
                                            )
                                            .child(div().flex().flex_col().mt(px(6.0)).children(
                                                browse.directories.iter().map(|entry| {
                                                    let path = entry.path.clone();
                                                    Button::new(SharedString::from(format!(
                                                        "browse-{path}"
                                                    )))
                                                    .label(entry.name.clone())
                                                    .compact()
                                                    .on_click(cx.listener(move |app, _, _, _| {
                                                        app.browse_directory(Some(path.clone()))
                                                    }))
                                                }),
                                            )),
                                    )
                                })
                        },
                    ),
            )
    }
}

fn switcher_section_label(label: &'static str, palette: CanvasPalette) -> AnyElement {
    div()
        .border_t_1()
        .border_color(palette.edge)
        .pt(px(8.0))
        .text_xs()
        .text_color(palette.text_muted)
        .child(label)
        .into_any_element()
}

fn motion_button(
    id: &'static str,
    label: &'static str,
    preference: MotionPreference,
    current: MotionPreference,
    weak: gpui::WeakEntity<CanvasApp>,
) -> Button {
    Button::new(id)
        .label(format!(
            "{}{}",
            if preference == current { "• " } else { "" },
            label
        ))
        .compact()
        .on_click(move |_, _, cx| {
            let _ = weak.update(cx, |app, cx| {
                app.set_motion_preference(preference);
                cx.notify();
            });
        })
}

fn shelf_resource_command(command: &str) -> Option<&str> {
    command.strip_prefix('+')?.split_whitespace().next()
}

fn normalize_canvas_name(raw: &str) -> std::result::Result<String, ()> {
    let name = raw.trim();
    if name.is_empty() || name.chars().count() > 80 || name.chars().any(char::is_control) {
        return Err(());
    }
    Ok(name.to_owned())
}

fn live_resource_blocks_close(kind: NodeKind, mounted_ended: Option<bool>) -> bool {
    // Absence is not proof that a server-owned process ended. A node can be
    // pending materialization or temporarily disconnected; removing it in
    // that state would orphan a live Terminal/Browser without confirmation.
    matches!(kind, NodeKind::Terminal | NodeKind::Browser) && mounted_ended != Some(true)
}

fn store_presence_token_if_current(
    current_token: &mut Option<String>,
    scope: &CanvasAsyncScope,
    current_generation: u64,
    current_canvas_id: Option<&str>,
    response_token: Option<String>,
) -> bool {
    if !scope.is_current(current_generation, current_canvas_id) {
        return false;
    }
    if let Some(response_token) = response_token {
        *current_token = Some(response_token);
    }
    true
}

fn presence_mutation_ready(
    presence_token: Option<&str>,
    in_flight_or_blocked: bool,
    has_pending_value: bool,
) -> bool {
    presence_token.is_some() && !in_flight_or_blocked && has_pending_value
}

fn sanitize_background_preferences(
    mut preferences: BackgroundPreferencesV1,
) -> BackgroundPreferencesV1 {
    preferences.dim = if preferences.dim.is_finite() {
        preferences.dim.clamp(0.0, 1.0)
    } else {
        BackgroundPreferencesV1::default().dim
    };
    preferences.intensity_override = preferences
        .intensity_override
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 1.0));
    preferences
}

fn theme_gradient(angle_deg: f32, stops: &[GradientStopV1], fallback: gpui::Hsla) -> Background {
    let Some(first) = stops.first() else {
        return fallback.into();
    };
    let Some(last) = stops.last() else {
        return fallback.into();
    };
    let from = parse_theme_color(&first.color).unwrap_or(fallback);
    let to = parse_theme_color(&last.color).unwrap_or(fallback);
    linear_gradient(
        angle_deg.rem_euclid(360.0),
        linear_color_stop(from, first.offset.clamp(0.0, 1.0)),
        linear_color_stop(to, last.offset.clamp(0.0, 1.0)),
    )
}

fn static_theme_background(spec: &StaticBackgroundV1, fallback: gpui::Hsla) -> Background {
    match spec {
        StaticBackgroundV1::Solid { color } => parse_theme_color(color).unwrap_or(fallback).into(),
        StaticBackgroundV1::Gradient { angle_deg, stops } => {
            theme_gradient(*angle_deg, stops, fallback)
        }
    }
}

fn queued_patch_operation_count(queue: &VecDeque<Vec<CanvasOperation>>) -> usize {
    queue.iter().map(Vec::len).sum()
}

fn patch_operation_coalesces(queued: &CanvasOperation, incoming: &CanvasOperation) -> bool {
    match (queued, incoming) {
        (
            CanvasOperation::SetGeometry {
                node_id: queued, ..
            },
            CanvasOperation::SetGeometry {
                node_id: incoming, ..
            },
        )
        | (
            CanvasOperation::SetTitle {
                node_id: queued, ..
            },
            CanvasOperation::SetTitle {
                node_id: incoming, ..
            },
        ) => queued == incoming,
        (CanvasOperation::RenameCanvas { .. }, CanvasOperation::RenameCanvas { .. })
        | (CanvasOperation::SetSharedTheme { .. }, CanvasOperation::SetSharedTheme { .. })
        | (CanvasOperation::SetArchived { .. }, CanvasOperation::SetArchived { .. }) => true,
        _ => false,
    }
}

fn patch_operation_is_barrier(queued: &CanvasOperation, incoming: &CanvasOperation) -> bool {
    let incoming_node = match incoming {
        CanvasOperation::SetGeometry { node_id, .. }
        | CanvasOperation::SetTitle { node_id, .. } => node_id,
        _ => return false,
    };
    match queued {
        CanvasOperation::UpsertNode { node } => &node.id == incoming_node,
        CanvasOperation::RemoveNode { node_id } => node_id == incoming_node,
        _ => false,
    }
}

fn changed_palette(previous: CanvasPalette, current: CanvasPalette) -> Option<CanvasPalette> {
    (previous != current).then_some(current)
}

fn bounded_patch_queue(
    current: &VecDeque<Vec<CanvasOperation>>,
    incoming: Vec<CanvasOperation>,
    max_operations: usize,
) -> Option<VecDeque<Vec<CanvasOperation>>> {
    if incoming.is_empty() {
        return Some(current.clone());
    }

    let mut candidate = current.clone();
    if incoming.len() == 1 {
        let operation = &incoming[0];
        'batches: for batch in candidate.iter_mut().rev() {
            for queued in batch.iter_mut().rev() {
                if patch_operation_coalesces(queued, operation) {
                    *queued = operation.clone();
                    return Some(candidate);
                }
                if patch_operation_is_barrier(queued, operation) {
                    break 'batches;
                }
            }
        }
    }

    if queued_patch_operation_count(&candidate).saturating_add(incoming.len()) > max_operations {
        return None;
    }
    candidate.push_back(incoming);
    Some(candidate)
}

impl Focusable for CanvasApp {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus.clone()
    }
}

impl Render for CanvasApp {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.window_active = window.is_window_active();
        let palette = self.active_palette();
        let viewport = viewport_size(window);
        let visible = self.camera.visible_world_rect(viewport);
        let level = semantic_level(self.camera.zoom, self.gesture_active);
        let mut nodes = self
            .nodes
            .iter()
            .filter(|node| {
                !self.stage_slots.contains(&node.id) && visible.intersects(node.world_bounds)
            })
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| node.z_order);
        let has_canvas = self.canvas_id.is_some();
        let stage = self.render_stage(window, cx);

        div()
            .key_context("Canvas")
            .track_focus(&self.focus)
            .flex()
            .flex_col()
            .size_full()
            .bg(palette.canvas)
            .text_color(palette.text)
            .on_action(cx.listener(|app, _: &FitAll, window, cx| app.fit_all(window, cx)))
            .on_action(cx.listener(|app, _: &ResetZoom, window, cx| app.reset_zoom(window, cx)))
            .on_action(cx.listener(|app, _: &FocusCommandShelf, window, cx| {
                window.focus(&app.command.read(cx).focus_handle(cx));
            }))
            .on_action(
                cx.listener(|app, _: &NewAgent, _, cx| {
                    app.execute_command("+agent".to_owned(), cx)
                }),
            )
            .on_action(cx.listener(|app, _: &NewTerminal, _, cx| {
                app.execute_command("+terminal".to_owned(), cx)
            }))
            .on_action(cx.listener(|app, _: &NewBrowser, _, cx| {
                app.execute_command("+browser".to_owned(), cx)
            }))
            .on_action(
                cx.listener(|app, _: &NewFiles, _, cx| {
                    app.execute_command("+files".to_owned(), cx)
                }),
            )
            .on_action(cx.listener(|app, _: &NewChanges, _, cx| {
                app.execute_command("+changes".to_owned(), cx)
            }))
            .on_action(
                cx.listener(|app, _: &StageOne, _, cx| app.set_stage_layout(StageLayout::One, cx)),
            )
            .on_action(
                cx.listener(|app, _: &StageTwo, _, cx| app.set_stage_layout(StageLayout::Two, cx)),
            )
            .on_action(
                cx.listener(|app, _: &StageFour, _, cx| {
                    app.set_stage_layout(StageLayout::Four, cx)
                }),
            )
            .child(self.render_titlebar())
            .child(
                div()
                    .id("spatial-viewport")
                    .relative()
                    .flex_1()
                    .min_h_0()
                    .overflow_hidden()
                    .on_mouse_down(
                        MouseButton::Middle,
                        cx.listener(|app, event: &MouseDownEvent, _, cx| {
                            app.gesture = Some(PointerGesture::Pan {
                                last: canvas_point(
                                    event.position.x.into(),
                                    event.position.y.into(),
                                ),
                            });
                            app.gesture_active = true;
                            cx.stop_propagation();
                            cx.notify();
                        }),
                    )
                    .on_mouse_move(
                        cx.listener(|app, event, _, cx| app.handle_mouse_move(event, cx)),
                    )
                    .on_mouse_up(
                        MouseButton::Left,
                        cx.listener(|app, _, _, cx| app.end_gesture(cx)),
                    )
                    .on_mouse_up(
                        MouseButton::Middle,
                        cx.listener(|app, _, _, cx| app.end_gesture(cx)),
                    )
                    .on_scroll_wheel(
                        cx.listener(|app, event, window, cx| app.handle_scroll(event, window, cx)),
                    )
                    .child(self.render_background())
                    .when(!has_canvas, |viewport| {
                        viewport.child(self.render_empty_state(cx))
                    })
                    .children(
                        nodes
                            .into_iter()
                            .map(|node| self.render_node(node, level, cx)),
                    )
                    .children(
                        self.collaboration
                            .geometry_previews
                            .values()
                            .filter(|preview| preview.client_id != self.client_id)
                            .map(|preview| self.render_geometry_preview(preview)),
                    )
                    .children(
                        self.collaboration
                            .clients
                            .values()
                            .filter_map(|client| self.render_remote_cursor(client)),
                    )
                    .when(has_canvas, |viewport| {
                        viewport
                            .child(self.render_project_switcher(cx))
                            .child(self.render_pulse_rail(cx))
                            .child(self.render_stage_rail(cx))
                    })
                    .when_some(stage, |viewport, stage| viewport.child(stage))
                    .child(self.render_command_shelf(cx)),
            )
    }
}

fn canvas_point(x: f32, y: f32) -> WorldPoint {
    WorldPoint::new(x as f64, (y - TITLE_HEIGHT).max(0.0) as f64)
}

fn canvas_reconnect_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(5);
    Duration::from_millis((250_u64 << exponent).min(5_000))
}

fn resource_identity_changed(existing: &SpatialNode, incoming: &SpatialNode) -> bool {
    existing.kind != incoming.kind || existing.binding_id != incoming.binding_id
}

fn canvas_snapshot_is_newer(current_revision: u64, snapshot_revision: u64) -> bool {
    snapshot_revision > current_revision
}

fn geometry_release_ready(committed: bool, persisted: bool, preview_in_flight: bool) -> bool {
    !preview_in_flight && (!committed || persisted)
}

fn viewport_size(window: &Window) -> WorldSize {
    WorldSize {
        width: f32::from(window.viewport_size().width) as f64,
        height: (f32::from(window.viewport_size().height) - TITLE_HEIGHT).max(1.0) as f64,
    }
}

#[allow(dead_code)]
fn usable_canvas_view(viewport: WorldSize, pulse_expanded: bool) -> (WorldPoint, WorldSize) {
    let left = if pulse_expanded {
        PULSE_RAIL_EXPANDED_WIDTH
    } else {
        PULSE_RAIL_WIDTH
    } as f64;
    let right = STAGE_RAIL_WIDTH as f64;
    let top = CANVAS_OVERLAY_TOP as f64;
    let bottom = CANVAS_OVERLAY_BOTTOM as f64;
    (
        WorldPoint::new(left, top),
        WorldSize {
            width: (viewport.width - left - right).max(1.0),
            height: (viewport.height - top - bottom).max(1.0),
        },
    )
}

fn wire_rect_to_world(rect: WireWorldRect) -> WorldRect {
    WorldRect::new(rect.x, rect.y, rect.width, rect.height)
}

fn world_rect_to_wire(rect: WorldRect) -> WireWorldRect {
    WireWorldRect {
        x: rect.origin.x,
        y: rect.origin.y,
        width: rect.size.width,
        height: rect.size.height,
    }
}

fn wire_to_spatial(node: &WireCanvasNode) -> Option<SpatialNode> {
    let (kind, binding_id) = match &node.binding {
        CanvasNodeBinding::Agent { session_id } => (NodeKind::Agent, Some(session_id.clone())),
        CanvasNodeBinding::Terminal { terminal_id } => {
            (NodeKind::Terminal, Some(terminal_id.clone()))
        }
        CanvasNodeBinding::Browser {
            browser_session_id, ..
        } => (NodeKind::Browser, Some(browser_session_id.clone())),
        CanvasNodeBinding::Files { path } => (NodeKind::Files, path.clone()),
        CanvasNodeBinding::Changes => (NodeKind::Changes, None),
    };
    Some(SpatialNode {
        id: Uuid::parse_str(&node.id).ok()?,
        kind,
        title: node.title.clone(),
        world_bounds: wire_rect_to_world(node.rect),
        z_order: node.z_index,
        status: NodeStatus::Idle,
        binding_id,
    })
}

fn spatial_to_wire(node: &SpatialNode) -> Option<WireCanvasNode> {
    let binding = match node.kind {
        NodeKind::Agent => CanvasNodeBinding::Agent {
            session_id: node.binding_id.clone()?,
        },
        NodeKind::Terminal => CanvasNodeBinding::Terminal {
            terminal_id: node.binding_id.clone()?,
        },
        NodeKind::Browser => CanvasNodeBinding::Browser {
            browser_session_id: node.binding_id.clone()?,
            last_url: None,
        },
        NodeKind::Files => CanvasNodeBinding::Files {
            path: node.binding_id.clone(),
        },
        NodeKind::Changes => CanvasNodeBinding::Changes,
    };
    Some(WireCanvasNode {
        id: node.id.to_string(),
        title: node.title.clone(),
        rect: world_rect_to_wire(node.world_bounds),
        z_index: node.z_order,
        binding,
        revision: 0,
    })
}

fn status_color(status: NodeStatus, palette: CanvasPalette) -> gpui::Hsla {
    match status {
        NodeStatus::Working => palette.pulse,
        NodeStatus::NeedsYou => palette.brass,
        NodeStatus::Disconnected | NodeStatus::Ended => palette.danger,
        NodeStatus::Idle => palette.text_muted,
    }
}

fn status_label(status: NodeStatus) -> &'static str {
    match status {
        NodeStatus::Working => "Working",
        NodeStatus::NeedsYou => "Needs you",
        NodeStatus::Disconnected => "Disconnected",
        NodeStatus::Ended => "Ended",
        NodeStatus::Idle => "Idle",
    }
}

fn agent_node_status(streaming: bool, needs_attention: bool) -> NodeStatus {
    if needs_attention {
        NodeStatus::NeedsYou
    } else if streaming {
        NodeStatus::Working
    } else {
        NodeStatus::Idle
    }
}

fn terminal_node_status(
    connection: TerminalSurfaceConnection,
    needs_attention: bool,
) -> NodeStatus {
    match connection {
        TerminalSurfaceConnection::Exited => NodeStatus::Ended,
        TerminalSurfaceConnection::Disconnected => NodeStatus::Disconnected,
        TerminalSurfaceConnection::Connecting | TerminalSurfaceConnection::Reconnecting => {
            NodeStatus::Working
        }
        TerminalSurfaceConnection::Connected if needs_attention => NodeStatus::NeedsYou,
        TerminalSurfaceConnection::Connected => NodeStatus::Idle,
    }
}

fn browser_node_status(ended: bool, disconnected: bool, working: bool) -> NodeStatus {
    if ended {
        NodeStatus::Ended
    } else if disconnected {
        NodeStatus::Disconnected
    } else if working {
        NodeStatus::Working
    } else {
        NodeStatus::Idle
    }
}

fn activity_node_status(loading: bool, needs_attention: bool) -> NodeStatus {
    if needs_attention {
        NodeStatus::NeedsYou
    } else if loading {
        NodeStatus::Working
    } else {
        NodeStatus::Idle
    }
}

fn generic_summary(node: &SpatialNode, status: NodeStatus) -> &'static str {
    match (node.kind, status) {
        (_, NodeStatus::Disconnected) => "Disconnected",
        (_, NodeStatus::Ended) => "Ended",
        (NodeKind::Terminal, _) => "Server-owned shell",
        (NodeKind::Browser, _) => "Server-owned browser session",
        (NodeKind::Files, _) => "Project files · read only",
        (NodeKind::Changes, _) => "Working tree changes · read only",
        (NodeKind::Agent, _) => "Agent session",
    }
}

fn render_generic_detail(node: &SpatialNode, palette: CanvasPalette) -> AnyElement {
    let (headline, detail) = match node.kind {
        NodeKind::Terminal => (
            "Terminal session not attached",
            "The native emulator only connects to a server-owned terminal. There is no local-shell fallback.",
        ),
        NodeKind::Browser => (
            "Browser session not attached",
            "Frames and input come from the Mitsuro server; local WebKit is intentionally excluded.",
        ),
        NodeKind::Files => (
            "Project files",
            "Read-only browse and file content are loaded from the authoritative server.",
        ),
        NodeKind::Changes => (
            "Working tree changes",
            "Unified and split diffs are read-only in this alpha slice.",
        ),
        NodeKind::Agent => (
            "Agent session not attached",
            "This node has no durable Agent binding yet.",
        ),
    };
    div()
        .flex()
        .flex_col()
        .size_full()
        .justify_center()
        .px(px(22.0))
        .child(div().text_lg().child(headline))
        .child(
            div()
                .mt(px(8.0))
                .text_sm()
                .text_color(palette.text_muted)
                .child(detail),
        )
        .into_any_element()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_lookup_is_scoped_to_canvas_pack() {
        let candidates = asset_candidates("icons/window-close.svg");
        assert!(candidates
            .first()
            .is_some_and(|path| path.starts_with(Path::new(env!("CARGO_MANIFEST_DIR")))));
    }

    #[test]
    fn canvas_point_removes_only_native_title_strip() {
        assert_eq!(canvas_point(10.0, 44.0), WorldPoint::new(10.0, 10.0));
        assert_eq!(canvas_point(10.0, 12.0), WorldPoint::new(10.0, 0.0));
    }

    #[test]
    fn committed_geometry_keeps_lock_until_patch_is_persisted() {
        assert!(!geometry_release_ready(true, false, false));
        assert!(geometry_release_ready(true, true, false));
        assert!(!geometry_release_ready(true, true, true));
        assert!(geometry_release_ready(false, false, false));
    }

    #[test]
    fn canvas_reconnect_backoff_is_bounded() {
        assert_eq!(canvas_reconnect_delay(1), Duration::from_millis(250));
        assert_eq!(canvas_reconnect_delay(2), Duration::from_millis(500));
        assert_eq!(canvas_reconnect_delay(99), Duration::from_secs(5));
    }

    #[test]
    fn streamed_bind_event_cannot_be_rolled_back_by_an_older_resource_snapshot() {
        assert!(!canvas_snapshot_is_newer(42, 41));
        assert!(!canvas_snapshot_is_newer(42, 42));
    }

    #[test]
    fn resource_snapshot_applies_before_its_newer_stream_event() {
        assert!(canvas_snapshot_is_newer(41, 42));
    }

    #[test]
    fn advertised_spaced_resource_commands_are_recognized() {
        for command in ["agent", "terminal", "browser", "files", "changes"] {
            assert_eq!(
                shelf_resource_command(&format!("+ {command}")),
                Some(command)
            );
            assert_eq!(
                shelf_resource_command(&format!("+{command}")),
                Some(command)
            );
        }
        assert_eq!(shelf_resource_command("+ unknown"), Some("unknown"));
        assert_eq!(shelf_resource_command("plain language"), None);
    }

    #[test]
    fn canvas_name_validation_trims_and_rejects_unsafe_names() {
        assert_eq!(normalize_canvas_name("  Focus  "), Ok("Focus".to_owned()));
        assert!(normalize_canvas_name("\n").is_err());
        assert!(normalize_canvas_name("bad\0name").is_err());
        assert!(normalize_canvas_name(&"x".repeat(81)).is_err());
    }

    #[test]
    fn live_terminal_and_browser_require_surface_end_confirmation() {
        assert!(live_resource_blocks_close(NodeKind::Terminal, Some(false)));
        assert!(live_resource_blocks_close(NodeKind::Browser, Some(false)));
        assert!(!live_resource_blocks_close(NodeKind::Terminal, Some(true)));
        assert!(!live_resource_blocks_close(NodeKind::Agent, Some(false)));
        assert!(live_resource_blocks_close(NodeKind::Terminal, None));
        assert!(live_resource_blocks_close(NodeKind::Browser, None));
    }

    #[test]
    fn corrupt_background_preferences_are_bounded_before_rendering() {
        let preferences = BackgroundPreferencesV1 {
            dim: f32::NAN,
            intensity_override: Some(f32::INFINITY),
            ..BackgroundPreferencesV1::default()
        };
        let sanitized = sanitize_background_preferences(preferences);
        assert_eq!(sanitized.dim, BackgroundPreferencesV1::default().dim);
        assert_eq!(sanitized.intensity_override, None);

        let preferences = BackgroundPreferencesV1 {
            dim: -4.0,
            intensity_override: Some(9.0),
            ..BackgroundPreferencesV1::default()
        };
        let sanitized = sanitize_background_preferences(preferences);
        assert_eq!(sanitized.dim, 0.0);
        assert_eq!(sanitized.intensity_override, Some(1.0));
    }

    #[test]
    fn palette_propagation_triggers_only_for_a_resolved_color_change() {
        let graphite = CanvasPalette::default();
        assert_eq!(changed_palette(graphite, graphite), None);

        let custom = CanvasPalette {
            canvas: crate::theme::hex(0x101522),
            surface: crate::theme::hex(0x172033),
            pulse: crate::theme::hex(0xff8a3d),
            ..graphite
        };
        assert_eq!(changed_palette(graphite, custom), Some(custom));
    }

    #[test]
    fn delayed_a_patch_completion_is_rejected_after_b_opens() {
        let delayed_a = CanvasAsyncScope {
            generation: 1,
            canvas_id: "canvas-a".to_owned(),
        };

        assert!(!delayed_a.is_current(2, Some("canvas-b")));
        assert!(!delayed_a.is_current(2, Some("canvas-a")));
    }

    #[test]
    fn delayed_a_resource_completion_is_rejected_after_b_opens() {
        let delayed_agent_terminal_or_browser = CanvasAsyncScope {
            generation: 7,
            canvas_id: "canvas-a".to_owned(),
        };
        let current_b = CanvasAsyncScope {
            generation: 8,
            canvas_id: "canvas-b".to_owned(),
        };

        assert!(!delayed_agent_terminal_or_browser
            .is_current(current_b.generation, Some(&current_b.canvas_id)));
        assert!(current_b.is_current(current_b.generation, Some(&current_b.canvas_id)));
    }

    #[test]
    fn delayed_a_draft_callback_cannot_write_canvas_b_state() {
        let node_id = Uuid::new_v4();
        let stale_a = CanvasAsyncScope {
            generation: 7,
            canvas_id: "canvas-a".to_owned(),
        };
        let mut canvas_b = CanvasPersonalState::default();
        assert!(canvas_b.set_draft(node_id, "current B draft"));

        assert_eq!(
            store_agent_draft_if_current(
                &mut canvas_b,
                &stale_a,
                8,
                Some("canvas-b"),
                node_id,
                Some("late A draft"),
            ),
            DraftStoreResult::Stale
        );
        assert_eq!(canvas_b.draft(node_id), "current B draft");
    }

    #[test]
    fn delayed_a_scroll_callback_cannot_write_canvas_b_state() {
        let node_id = Uuid::new_v4();
        let stale_a = CanvasAsyncScope {
            generation: 7,
            canvas_id: "canvas-a".to_owned(),
        };
        let mut canvas_b = CanvasPersonalState::default();
        assert!(canvas_b.set_scroll_offset(node_id, 120.0));

        assert_eq!(
            store_agent_scroll_if_current(
                &mut canvas_b,
                &stale_a,
                8,
                Some("canvas-b"),
                node_id,
                900.0,
            ),
            ScrollStoreResult::Stale
        );
        assert_eq!(canvas_b.scroll_offset(node_id), 120.0);
    }

    #[test]
    fn live_surface_status_prioritizes_attention_and_terminal_lifecycle() {
        assert_eq!(agent_node_status(true, true), NodeStatus::NeedsYou);
        assert_eq!(agent_node_status(true, false), NodeStatus::Working);
        assert_eq!(activity_node_status(true, true), NodeStatus::NeedsYou);
        assert_eq!(
            terminal_node_status(TerminalSurfaceConnection::Reconnecting, false),
            NodeStatus::Working
        );
        assert_eq!(
            terminal_node_status(TerminalSurfaceConnection::Disconnected, true),
            NodeStatus::Disconnected
        );
        assert_eq!(
            terminal_node_status(TerminalSurfaceConnection::Exited, true),
            NodeStatus::Ended
        );
        assert_eq!(browser_node_status(true, true, true), NodeStatus::Ended);
        assert_eq!(
            browser_node_status(false, true, true),
            NodeStatus::Disconnected
        );
    }

    #[test]
    fn rapid_browser_creates_reserve_only_the_remaining_native_capacity() {
        let mut in_flight = 0;
        assert!(reserve_browser_create(1, &mut in_flight));
        assert!(reserve_browser_create(1, &mut in_flight));
        assert!(reserve_browser_create(1, &mut in_flight));
        assert_eq!(in_flight, 3);
        assert!(!reserve_browser_create(1, &mut in_flight));
        assert_eq!(in_flight, 3);

        let stale = CanvasAsyncScope {
            generation: 4,
            canvas_id: "canvas-a".to_owned(),
        };
        assert!(!complete_browser_create_if_current(
            &mut in_flight,
            &stale,
            5,
            Some("canvas-b"),
        ));
        assert_eq!(in_flight, 3);

        let current = CanvasAsyncScope {
            generation: 5,
            canvas_id: "canvas-b".to_owned(),
        };
        assert!(complete_browser_create_if_current(
            &mut in_flight,
            &current,
            5,
            Some("canvas-b"),
        ));
        assert_eq!(in_flight, 2);
    }

    #[test]
    fn presence_token_is_stored_only_for_the_current_canvas_generation() {
        let current = CanvasAsyncScope {
            generation: 12,
            canvas_id: "canvas-current".to_owned(),
        };
        let mut token = None;
        assert!(store_presence_token_if_current(
            &mut token,
            &current,
            12,
            Some("canvas-current"),
            Some("token-current".to_owned()),
        ));
        assert_eq!(token.as_deref(), Some("token-current"));

        // Mutation responses intentionally omit the token; they must not
        // clear the identity installed by a heartbeat response.
        assert!(store_presence_token_if_current(
            &mut token,
            &current,
            12,
            Some("canvas-current"),
            None,
        ));
        assert_eq!(token.as_deref(), Some("token-current"));
    }

    #[test]
    fn stale_presence_response_cannot_replace_the_current_canvas_token() {
        let stale = CanvasAsyncScope {
            generation: 11,
            canvas_id: "canvas-old".to_owned(),
        };
        let mut token = Some("token-current".to_owned());
        assert!(!store_presence_token_if_current(
            &mut token,
            &stale,
            12,
            Some("canvas-current"),
            Some("token-stale".to_owned()),
        ));
        assert_eq!(token.as_deref(), Some("token-current"));
    }

    #[test]
    fn pending_presence_mutations_wait_for_registration_then_drain_latest() {
        let mut pending_cursor = Some(1_u64);
        assert!(!presence_mutation_ready(
            None,
            false,
            pending_cursor.is_some()
        ));
        assert_eq!(pending_cursor, Some(1));

        // Coalescing while registration is outstanding retains only the
        // latest cursor/preview value instead of discarding it.
        pending_cursor = Some(2);
        assert!(presence_mutation_ready(
            Some("presence-token"),
            false,
            pending_cursor.is_some()
        ));
        assert_eq!(pending_cursor.take(), Some(2));

        let node_id = Uuid::new_v4();
        let mut pending_previews = BTreeMap::new();
        pending_previews.insert(node_id, 10_u64);
        pending_previews.insert(node_id, 20_u64);
        assert!(!presence_mutation_ready(
            None,
            false,
            pending_previews.contains_key(&node_id),
        ));
        assert_eq!(pending_previews.get(&node_id), Some(&20));
        assert!(presence_mutation_ready(
            Some("presence-token"),
            false,
            pending_previews.contains_key(&node_id),
        ));
        assert_eq!(pending_previews.remove(&node_id), Some(20));
    }

    #[test]
    fn patch_queue_coalesces_latest_value_and_rejects_overflow() {
        let queue = VecDeque::from([vec![CanvasOperation::SetTitle {
            node_id: "node-a".to_owned(),
            title: "old".to_owned(),
        }]]);
        let coalesced = bounded_patch_queue(
            &queue,
            vec![CanvasOperation::SetTitle {
                node_id: "node-a".to_owned(),
                title: "latest".to_owned(),
            }],
            1,
        )
        .expect("a superseding title should fit without growing the queue");
        assert_eq!(queued_patch_operation_count(&coalesced), 1);
        assert!(matches!(
            &coalesced[0][0],
            CanvasOperation::SetTitle { title, .. } if title == "latest"
        ));

        let overflow = bounded_patch_queue(
            &coalesced,
            vec![CanvasOperation::RemoveNode {
                node_id: "node-b".to_owned(),
            }],
            1,
        );
        assert!(overflow.is_none());
    }

    #[test]
    fn resource_status_strings_classify_lifecycle() {
        assert!(resource_status_is_terminal("cleaned"));
        assert!(resource_status_is_terminal("abandoned"));
        assert!(!resource_status_is_terminal("bound"));
        assert!(resource_status_is_bound("bound"));
        assert!(resource_status_needs_poll("reserved"));
        assert!(resource_status_needs_poll("cleaning"));
        assert!(!resource_status_needs_poll("bound"));
        assert_eq!(resource_poll_delay(1), Duration::from_millis(250));
        assert_eq!(resource_poll_delay(99), Duration::from_secs(4));
    }

    #[test]
    fn resource_journal_removal_purpose_matches_crash_safe_modes() {
        assert_eq!(
            ResourceJournalRemovalPurpose::Normal.mode(),
            ResourceJournalRemovalMode::Normal
        );
        assert_eq!(
            ResourceJournalRemovalPurpose::ConfirmedCleanup.mode(),
            ResourceJournalRemovalMode::ConfirmedCleanup
        );
        assert_eq!(
            ResourceJournalRemovalPurpose::ConfirmedDetach.mode(),
            ResourceJournalRemovalMode::ConfirmedDetach
        );
    }

    #[test]
    fn shelf_restore_only_replaces_an_empty_command() {
        assert!(shelf_restore_is_safe(""));
        assert!(shelf_restore_is_safe("   "));
        assert!(!shelf_restore_is_safe("+ terminal"));
    }
}
