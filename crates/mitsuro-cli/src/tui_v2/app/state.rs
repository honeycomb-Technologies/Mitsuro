//! TUI-owned application and interaction state.

use std::collections::BTreeMap;

use mitsuro_core::auth::AuthMethod;

use crate::tui_v2::{
    components::splash::SplashState,
    layout::anchor::TranscriptAnchor,
    model::{
        artifact::{ArtifactUiState, PartId},
        capability::CapabilityProfile,
        focus::FocusTarget,
        overlay::OverlayState,
    },
    motion::{preference::MotionPreference, MotionState},
    presentation::theme::ThemeKind,
};

use super::route::AppRoute;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AppLifecycle {
    #[default]
    Running,
    ExitRequested,
    ApplyUpdateRequested,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateNotice {
    pub current_version: String,
    pub new_version: String,
    pub can_apply: bool,
    pub hint: String,
}

impl UpdateNotice {
    pub fn banner(&self) -> String {
        format!(
            "Update {} → {}  ·  {}",
            self.current_version, self.new_version, self.hint
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AgentRunState {
    #[default]
    Idle,
    Running,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DecisionAction {
    Approve,
    Deny,
    #[default]
    Inspect,
}

impl DecisionAction {
    pub const fn previous(self) -> Self {
        match self {
            Self::Approve => Self::Inspect,
            Self::Deny => Self::Approve,
            Self::Inspect => Self::Deny,
        }
    }

    pub const fn next(self) -> Self {
        match self {
            Self::Approve => Self::Deny,
            Self::Deny => Self::Inspect,
            Self::Inspect => Self::Approve,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DecisionDockUiState {
    pub focused_action: DecisionAction,
    pub current_question: usize,
    pub selected_option: usize,
    pub toggled_options: Vec<usize>,
    pub answers: Vec<QuestionAnswer>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuestionAnswer {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SetupStep {
    #[default]
    Provider,
    AuthMethod,
    Credential,
    OAuthWaiting,
    OAuthPasteCode,
    CatalogLoading,
    Model,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SetupUiState {
    pub step: SetupStep,
    pub provider_index: usize,
    pub auth_method_index: usize,
    pub selected_auth_method: Option<AuthMethod>,
    pub model_index: usize,
    pub oauth_message: Option<String>,
    pub oauth_url: Option<String>,
    pub device_code: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PickerUiState {
    pub query: String,
    pub selected: usize,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptUiState {
    pub follow_live: bool,
    pub unseen_parts: usize,
    pub scroll_rows: u32,
    pub selected_part: Option<PartId>,
    pub pending_anchor: Option<TranscriptAnchor>,
}

impl Default for TranscriptUiState {
    fn default() -> Self {
        Self {
            follow_live: true,
            unseen_parts: 0,
            scroll_rows: 0,
            selected_part: None,
            pending_anchor: None,
        }
    }
}

/// Which surface is auto-scrolling while a selection drag is at its edge.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EdgeScrollArea {
    #[default]
    Transcript,
    Composer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EdgeScrollDirection {
    Up,
    Down,
}

/// Continuous edge-scroll while the pointer is held in the edge zone during drag-select.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EdgeScrollState {
    pub direction: Option<EdgeScrollDirection>,
    pub area: EdgeScrollArea,
    pub last_x: u16,
}

impl EdgeScrollState {
    pub fn clear(&mut self) {
        self.direction = None;
    }

    pub const fn is_active(&self) -> bool {
        self.direction.is_some()
    }

    pub fn arm(&mut self, direction: EdgeScrollDirection, area: EdgeScrollArea, last_x: u16) {
        self.direction = Some(direction);
        self.area = area;
        self.last_x = last_x;
    }
}

/// Live mouse interaction state (selection + hover + scrollbar drag).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MouseUiState {
    pub position: Option<(u16, u16)>,
    pub hover_link: Option<String>,
    /// Active drag selection across transcript source offsets.
    pub selection: Option<MouseTextSelection>,
    pub selecting: bool,
    /// Composer byte-range selection (start, end), inclusive-oriented endpoints.
    pub composer_selection: Option<(usize, usize)>,
    pub selecting_composer: bool,
    /// Scrollbar currently being dragged, if any.
    pub scrollbar_drag: Option<crate::tui_v2::layout::snapshot::ScrollRegionId>,
    /// Auto-scroll while drag-selecting near a surface edge (legacy TUI parity).
    pub edge_scroll: EdgeScrollState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MouseTextSelection {
    pub start: crate::tui_v2::layout::snapshot::SelectionPoint,
    pub end: crate::tui_v2::layout::snapshot::SelectionPoint,
}

/// Attachment preview opened from composer brackets (files / clipboard chips).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentPreview {
    pub title: String,
    pub kind_label: String,
    pub detail: String,
    /// Text body for non-image previews (or fallback when graphics unavailable).
    pub body: String,
    /// When set, the overlay renders the image at this path (PNG/JPEG/etc.).
    pub image_path: Option<std::path::PathBuf>,
}

impl MouseTextSelection {
    pub fn is_empty_range(&self) -> bool {
        self.start.part_id == self.end.part_id && self.start.source_offset == self.end.source_offset
    }
}

impl MouseUiState {
    pub fn clear_selection(&mut self) {
        self.selection = None;
        self.selecting = false;
        self.composer_selection = None;
        self.selecting_composer = false;
        self.edge_scroll.clear();
    }

    pub fn begin_selection(&mut self, point: crate::tui_v2::layout::snapshot::SelectionPoint) {
        self.scrollbar_drag = None;
        self.selecting_composer = false;
        self.composer_selection = None;
        self.edge_scroll.clear();
        self.selecting = true;
        self.selection = Some(MouseTextSelection {
            start: point.clone(),
            end: point,
        });
    }

    pub fn drag_selection(&mut self, point: crate::tui_v2::layout::snapshot::SelectionPoint) {
        if !self.selecting {
            return;
        }
        if let Some(selection) = &mut self.selection {
            selection.end = point;
        }
    }

    pub fn begin_composer_selection(&mut self, byte: usize) {
        self.scrollbar_drag = None;
        self.selecting = false;
        self.selection = None;
        self.edge_scroll.clear();
        self.selecting_composer = true;
        self.composer_selection = Some((byte, byte));
    }

    pub fn drag_composer_selection(&mut self, byte: usize) {
        if !self.selecting_composer {
            return;
        }
        if let Some((start, _)) = self.composer_selection {
            self.composer_selection = Some((start, byte));
        }
    }

    pub fn composer_selection_ordered(&self) -> Option<(usize, usize)> {
        let (a, b) = self.composer_selection?;
        Some(if a <= b { (a, b) } else { (b, a) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComposerUiState {
    pub fullscreen: bool,
    pub autocomplete_open: bool,
    pub autocomplete_selected: usize,
    pub file_search_open: bool,
    pub file_search_selected: usize,
    pub buffer: crate::tui_v2::input::composer_buffer::ComposerBuffer,
    /// When true, the viewport pans so the caret stays inside the field.
    /// Cleared by scrollbar/wheel scroll so the user can look away; any edit,
    /// arrow motion, or click turns it back on.
    pub follow_cursor: bool,
    /// Last painted content width × visible rows (inner surface, post-border).
    /// Zero until the first layout pass.
    pub field_width: usize,
    pub field_rows: usize,
}

impl Default for ComposerUiState {
    fn default() -> Self {
        Self {
            fullscreen: false,
            autocomplete_open: false,
            autocomplete_selected: 0,
            file_search_open: false,
            file_search_selected: 0,
            buffer: crate::tui_v2::input::composer_buffer::ComposerBuffer::default(),
            follow_cursor: true,
            field_width: 0,
            field_rows: 0,
        }
    }
}

impl ComposerUiState {
    pub fn text(&self) -> &str {
        self.buffer.content()
    }

    pub fn cursor_byte(&self) -> usize {
        self.buffer.cursor()
    }

    pub fn set_cursor_byte(&mut self, byte: usize) {
        let (width, rows) = self.active_metrics(80, 6);
        self.follow_cursor = true;
        self.buffer.set_cursor(byte);
        self.buffer.ensure_cursor_visible(width, rows);
    }

    pub fn viewport_offset(&self) -> usize {
        self.buffer.viewport_offset()
    }

    pub fn selection(&self) -> Option<(usize, usize)> {
        self.buffer.selection()
    }

    pub fn set_selection(&mut self, start: usize, end: usize) {
        self.buffer.set_selection(start, end);
    }

    pub fn clear_selection(&mut self) {
        self.buffer.clear_selection();
    }

    /// Soft-wrap / navigation metrics for the live composer field.
    pub fn layout_metrics(viewport_cols: u16, viewport_rows: u16) -> (usize, usize) {
        // Before the first frame, viewport may still be (0,0). Fall back to a
        // comfortable Word-like default so Home/arrows don't soft-wrap at width 1.
        let cols = if viewport_cols < 8 { 80 } else { viewport_cols };
        let rows = if viewport_rows < 4 { 24 } else { viewport_rows };
        // Borders (2) + optional scrollbar gutter (1) ≈ content width.
        let width = usize::from(cols.saturating_sub(4).max(1));
        // Composer height is 3–6 cells; inner content is height − 2 for borders.
        let height = usize::from(rows.saturating_sub(2).clamp(1, 4));
        (width, height)
    }

    /// Prefer the last painted field size so soft-wrap matches what the user sees.
    pub fn active_metrics(&self, viewport_cols: u16, viewport_rows: u16) -> (usize, usize) {
        if self.field_width > 0 && self.field_rows > 0 {
            (self.field_width, self.field_rows)
        } else {
            Self::layout_metrics(viewport_cols, viewport_rows)
        }
    }

    /// Record the real content box from layout and pan the caret into view when following.
    pub fn sync_field_metrics(&mut self, width: usize, visible_rows: usize) {
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        if self.follow_cursor {
            self.buffer
                .ensure_cursor_visible(self.field_width, self.field_rows);
        } else {
            // Keep offset legal after resize / reflow even when scrolled away.
            let max_off = self
                .buffer
                .visual_row_count(self.field_width)
                .saturating_sub(self.field_rows);
            if self.buffer.viewport_offset() > max_off {
                self.buffer
                    .set_viewport_offset(max_off, self.field_width, self.field_rows);
            }
        }
    }

    pub fn insert(&mut self, value: &str) {
        let (width, rows) = self.active_metrics(80, 6);
        self.follow_cursor = true;
        self.buffer.insert_str(value);
        self.buffer.ensure_cursor_visible(width, rows);
        self.refresh_assist();
    }

    pub fn insert_with_layout(&mut self, value: &str, width: usize, visible_rows: usize) {
        self.follow_cursor = true;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer.insert_str(value);
        self.buffer
            .ensure_cursor_visible(self.field_width, self.field_rows);
        self.refresh_assist();
    }

    pub fn backspace(&mut self) {
        let (width, rows) = self.active_metrics(80, 6);
        self.follow_cursor = true;
        self.buffer.backspace();
        self.buffer.ensure_cursor_visible(width, rows);
        self.refresh_assist();
    }

    pub fn backspace_with_layout(&mut self, width: usize, visible_rows: usize) {
        self.follow_cursor = true;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer.backspace();
        self.buffer
            .ensure_cursor_visible(self.field_width, self.field_rows);
        self.refresh_assist();
    }

    pub fn delete_previous_word(&mut self) {
        let (width, rows) = self.active_metrics(80, 6);
        self.follow_cursor = true;
        self.buffer.delete_previous_word();
        self.buffer.ensure_cursor_visible(width, rows);
        self.refresh_assist();
    }

    pub fn clear_to_line_start(&mut self) {
        let (width, rows) = self.active_metrics(80, 6);
        self.follow_cursor = true;
        self.buffer.clear_to_line_start(width);
        self.buffer.ensure_cursor_visible(width, rows);
        self.refresh_assist();
    }

    /// Empty the input bar entirely (Ctrl+C): text, selection, assist, viewport.
    pub fn clear_all(&mut self) {
        self.fullscreen = false;
        self.autocomplete_open = false;
        self.autocomplete_selected = 0;
        self.file_search_open = false;
        self.file_search_selected = 0;
        self.follow_cursor = true;
        self.buffer.clear();
        self.refresh_assist();
    }

    pub fn move_left_width(&mut self, width: usize, visible_rows: usize) {
        self.follow_cursor = true;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer.move_left(self.field_width);
        self.buffer
            .ensure_cursor_visible(self.field_width, self.field_rows);
        self.refresh_assist();
    }

    pub fn move_right_width(&mut self, width: usize, visible_rows: usize) {
        self.follow_cursor = true;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer.move_right(self.field_width);
        self.buffer
            .ensure_cursor_visible(self.field_width, self.field_rows);
        self.refresh_assist();
    }

    pub fn move_to_line_start_width(&mut self, width: usize, visible_rows: usize) {
        self.follow_cursor = true;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer.move_line_start(self.field_width);
        self.buffer
            .ensure_cursor_visible(self.field_width, self.field_rows);
        self.refresh_assist();
    }

    pub fn move_to_line_end_width(&mut self, width: usize, visible_rows: usize) {
        self.follow_cursor = true;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer.move_line_end(self.field_width);
        self.buffer
            .ensure_cursor_visible(self.field_width, self.field_rows);
        self.refresh_assist();
    }

    pub fn move_visual_line(&mut self, forward: bool, width: usize, visible_rows: usize) {
        self.follow_cursor = true;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        if forward {
            self.buffer.move_down(self.field_width, self.field_rows);
        } else {
            self.buffer.move_up(self.field_width, self.field_rows);
        }
        self.refresh_assist();
    }

    pub fn scroll_viewport(&mut self, delta: isize, width: usize, visible_rows: usize) {
        // Manual scroll: allow caret to leave the frame until the next edit/click.
        self.follow_cursor = false;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer
            .scroll_viewport(delta, self.field_width, self.field_rows);
    }

    pub fn set_viewport_offset(&mut self, offset: usize, width: usize, visible_rows: usize) {
        self.follow_cursor = false;
        self.field_width = width.max(1);
        self.field_rows = visible_rows.max(1);
        self.buffer
            .set_viewport_offset(offset, self.field_width, self.field_rows);
    }

    pub fn visual_row_count(&self, width: usize) -> usize {
        self.buffer.visual_row_count(width.max(1))
    }

    pub fn take_text(&mut self) -> String {
        self.fullscreen = false;
        self.autocomplete_open = false;
        self.autocomplete_selected = 0;
        self.file_search_open = false;
        self.file_search_selected = 0;
        self.follow_cursor = true;
        self.buffer.take_content()
    }

    pub fn complete_slash(&mut self, completion: &str) {
        let (width, rows) = self.active_metrics(80, 6);
        self.follow_cursor = true;
        self.buffer.clear();
        self.buffer.insert_str(completion);
        self.buffer.ensure_cursor_visible(width, rows);
        self.autocomplete_open = false;
        self.autocomplete_selected = 0;
        self.file_search_open = false;
        self.file_search_selected = 0;
    }

    pub fn complete_project_entry(&mut self, entry: &crate::tui_v2::services::ProjectEntry) {
        let Some(completion) = crate::tui_v2::input::file_search::complete_active_query(
            self.text(),
            self.cursor_byte(),
            entry,
        ) else {
            return;
        };
        let (width, rows) = self.active_metrics(80, 6);
        self.follow_cursor = true;
        self.buffer.clear();
        self.buffer.insert_str(&completion.text);
        self.buffer.set_cursor(completion.cursor_byte);
        self.buffer.ensure_cursor_visible(width, rows);
        self.autocomplete_open = false;
        self.autocomplete_selected = 0;
        self.file_search_selected = 0;
        self.file_search_open = completion.keep_open;
    }

    pub fn byte_from_visual(
        &self,
        column: usize,
        row: usize,
        width: usize,
        visible_rows: usize,
    ) -> usize {
        self.buffer
            .byte_from_click(column, row, width.max(1), visible_rows.max(1))
    }

    fn refresh_assist(&mut self) {
        self.autocomplete_open = !crate::tui_v2::input::slash::suggestions(self.text()).is_empty();
        self.autocomplete_selected = 0;
        self.file_search_open = !self.autocomplete_open
            && crate::tui_v2::input::file_search::active_query(self.text(), self.cursor_byte())
                .is_some();
        self.file_search_selected = 0;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppearanceState {
    pub theme: ThemeKind,
    pub motion: MotionState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UiState {
    pub route: AppRoute,
    pub overlay: Option<OverlayState>,
    pub focus: FocusTarget,
    pub transcript: TranscriptUiState,
    pub artifacts: BTreeMap<PartId, ArtifactUiState>,
    pub composer: ComposerUiState,
    pub decision_dock: DecisionDockUiState,
    pub setup: SetupUiState,
    pub picker: PickerUiState,
    pub appearance: AppearanceState,
    pub splash: SplashState,
    pub capability: CapabilityProfile,
    pub lifecycle: AppLifecycle,
    pub update: Option<UpdateNotice>,
    pub agent_run: AgentRunState,
    pub sidebar_visible: bool,
    pub dock: DockUiState,
    pub mouse: MouseUiState,
    /// Active attachment preview payload (shown via AttachmentPreview overlay).
    pub attachment_preview: Option<AttachmentPreview>,
    /// In-place session title editor (context bar click → type → Enter).
    pub title_edit: TitleEditState,
    /// Workspace chrome for the context bar (git diff + agent context fill).
    pub workspace: WorkspaceChromeState,
    pub viewport: (u16, u16),
    next_overlay_sequence: u64,
}

/// Live git + model-context summary painted beside the session title.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkspaceChromeState {
    /// Uncommitted additions (`git` worktree).
    pub git_additions: u32,
    /// Uncommitted deletions.
    pub git_deletions: u32,
    /// Tokens currently in the agent context window.
    pub context_used: usize,
    /// Model context window size (0 = unknown).
    pub context_max: usize,
}

impl WorkspaceChromeState {
    pub fn has_git_diff(&self) -> bool {
        self.git_additions > 0 || self.git_deletions > 0
    }

    pub fn has_context(&self) -> bool {
        self.context_max > 0 && self.context_used > 0
    }
}

/// Click-to-rename state for the conversation title in the context bar.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TitleEditState {
    pub active: bool,
    pub buffer: String,
}

impl TitleEditState {
    pub fn start(&mut self, current: Option<&str>) {
        self.active = true;
        self.buffer = current
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("New conversation")
            .to_owned();
    }

    pub fn cancel(&mut self) {
        self.active = false;
        self.buffer.clear();
    }

    /// Finish editing. Returns `Some(title)` when the buffer is non-empty.
    pub fn finish(&mut self) -> Option<String> {
        self.active = false;
        let trimmed = self.buffer.trim().to_owned();
        self.buffer.clear();
        (!trimmed.is_empty()).then_some(trimmed)
    }

    pub fn insert_char(&mut self, ch: char) {
        if self.buffer.chars().count() < 80 {
            self.buffer.push(ch);
        }
    }

    pub fn backspace(&mut self) {
        let _ = self.buffer.pop();
    }
}

/// Wide workspace dock: plan band over plugin well.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DockUiState {
    /// Percent of dock height for the plan panel (20–80).
    pub plan_ratio_percent: u8,
    /// Plugin well is the active interaction target.
    pub plugin_focused: bool,
    /// Vertical scroll offset (rows) inside the plan dock body.
    pub plan_scroll: u16,
}

impl Default for DockUiState {
    fn default() -> Self {
        Self {
            plan_ratio_percent: 42,
            plugin_focused: false,
            plan_scroll: 0,
        }
    }
}

impl DockUiState {
    pub fn plan_ratio(&self) -> f32 {
        f32::from(self.plan_ratio_percent.clamp(20, 80)) / 100.0
    }

    pub fn scroll_plan(&mut self, delta: i32, content_rows: u16, visible_rows: u16) {
        let max = content_rows.saturating_sub(visible_rows.max(1));
        let next = i32::from(self.plan_scroll).saturating_add(delta);
        self.plan_scroll = next.clamp(0, i32::from(max)) as u16;
    }
}

impl UiState {
    pub fn preview(capability: CapabilityProfile) -> Self {
        Self {
            route: AppRoute::Home,
            overlay: None,
            focus: FocusTarget::Composer,
            transcript: TranscriptUiState::default(),
            artifacts: BTreeMap::new(),
            composer: ComposerUiState::default(),
            decision_dock: DecisionDockUiState::default(),
            setup: SetupUiState::default(),
            picker: PickerUiState::default(),
            appearance: AppearanceState {
                theme: ThemeKind::MitsuroDark,
                motion: MotionState::new(MotionPreference::default_for(capability)),
            },
            splash: SplashState::default(),
            capability,
            lifecycle: AppLifecycle::Running,
            update: None,
            agent_run: AgentRunState::Idle,
            sidebar_visible: true,
            dock: DockUiState::default(),
            mouse: MouseUiState::default(),
            attachment_preview: None,
            title_edit: TitleEditState::default(),
            workspace: WorkspaceChromeState::default(),
            viewport: (0, 0),
            next_overlay_sequence: 1,
        }
    }

    pub const fn should_exit(&self) -> bool {
        matches!(
            self.lifecycle,
            AppLifecycle::ExitRequested | AppLifecycle::ApplyUpdateRequested
        )
    }

    pub fn apply_update_version(&self) -> Option<String> {
        match self.lifecycle {
            AppLifecycle::ApplyUpdateRequested => self
                .update
                .as_ref()
                .map(|notice| notice.new_version.clone()),
            _ => None,
        }
    }

    pub(crate) fn take_overlay_sequence(&mut self) -> u64 {
        let sequence = self.next_overlay_sequence;
        self.next_overlay_sequence = self.next_overlay_sequence.saturating_add(1);
        sequence
    }
}
