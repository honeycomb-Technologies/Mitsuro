//! Preview application loop and typed application boundary.

pub mod effect;
pub mod reducer;
pub mod route;
pub mod state;
#[cfg(test)]
mod stream_burst;

use anyhow::{bail, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use mitsuro_core::agent::{DelegatedProgressEvent, LoopEvent, LoopInput};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{wrappers::UnboundedReceiverStream, StreamMap};

use self::{
    effect::{
        DecisionTarget, DecisionTargetKind, FocusDirection, PersistedUiPreference, ScrollAmount,
        ScrollDirection, ScrollTarget, UiEffect,
    },
    route::{AppRoute, SessionId},
};
use super::{
    components::conversation::ConversationRenderData,
    input::{
        action::{ActionId, ActionRegistry},
        active_context, file_search,
        mouse::{resolve_mouse_with_drag, MouseResolution},
        slash::{self, SlashCommand, SlashInput},
    },
    layout::{
        anchor::AnchorMode,
        engine::{LayoutEngine, LayoutRequest, TranscriptRequest},
        measure::MeasurementCache,
        snapshot::LayoutSnapshot,
    },
    model::{
        artifact::PartId,
        capability::CapabilityProfile,
        conversation::{PendingInteraction, TimelinePart},
        focus::FocusTarget,
        overlay::OverlayKind,
    },
    presentation::{theme::SemanticTheme, transcript::ConversationDisplayList},
    projection::ConversationProjection,
    render::frame::render_preview,
    services::{
        ControlSnapshot, ExtensionRow, HomeSnapshot, LoadedSession, OAuthStart, PlanSnapshot,
        PreparedInput, ProcessRow, ProjectEntry, RecentSession, RuntimeServices,
        SetupServiceUpdate, SetupSnapshot,
    },
    terminal::TerminalSession,
};
use reducer::{reduce, UiAction};
use state::{AttachmentPreview, DecisionAction, QuestionAnswer, SetupStep, UiState};

/// How expensive a mouse/input update is for the main loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Redraw {
    None,
    /// Reuse last layout/measurements; only re-paint (selection drag).
    Light,
    Full,
}

type DelegatedProgressReceivers = StreamMap<u64, UnboundedReceiverStream<DelegatedProgressEvent>>;

const LOOP_BATCH_MAX_EVENTS: usize = 64;
const LOOP_BATCH_MAX_DURATION: Duration = Duration::from_millis(2);

impl Redraw {
    fn handled(self) -> bool {
        !matches!(self, Self::None)
    }
}

pub struct PreviewApp {
    state: UiState,
    layout_engine: LayoutEngine,
    measurements: MeasurementCache,
    last_layout: Option<LayoutSnapshot>,
    /// Cached presentation for light repaints during selection drag.
    last_display: Option<ConversationDisplayList>,
    last_measured: Option<Vec<std::sync::Arc<crate::tui_v2::layout::measure::MeasuredPart>>>,
    conversation: ConversationProjection,
    next_message_id: u64,
    runtime: Option<RuntimeServices>,
    loop_events: Option<mpsc::UnboundedReceiver<LoopEvent>>,
    loop_input: Option<mpsc::UnboundedSender<LoopInput>>,
    /// Live explore/build/plan child progress. Detached streams outlive their
    /// parent turn and remain attached until their senders close.
    delegated_progress: DelegatedProgressReceivers,
    next_delegated_progress_id: u64,
    compaction: Option<oneshot::Receiver<Result<(), String>>>,
    extension_command: Option<(String, oneshot::Receiver<Result<String, String>>)>,
    extension_toggle: Option<oneshot::Receiver<Result<(), String>>>,
    auth_events: Option<mpsc::UnboundedReceiver<crate::tui_support::utils::OAuthStatusUpdate>>,
    setup_events: Option<mpsc::UnboundedReceiver<SetupServiceUpdate>>,
    update_events: Option<mpsc::UnboundedReceiver<mitsuro_core::updater::UpdateStatus>>,
    home: Option<HomeSnapshot>,
    setup: Option<SetupSnapshot>,
    sessions: Vec<RecentSession>,
    project_entries: Vec<ProjectEntry>,
    processes: Vec<ProcessRow>,
    plan: Option<PlanSnapshot>,
    extensions: Vec<ExtensionRow>,
    controls: ControlSnapshot,
    pending_clipboard_images: std::collections::HashMap<String, (usize, usize, Vec<u8>)>,
    /// Live ratatui-image protocol for the attachment preview overlay.
    attachment_image: Option<ratatui_image::protocol::StatefulProtocol>,
    /// Key (path or clipboard id) currently loaded into `attachment_image`.
    attachment_image_key: Option<String>,
    graphics: crate::tui_support::graphics::GraphicsContext,
    /// Throttle for git status polling (context-bar diff chrome).
    last_git_poll: std::time::Instant,
}

impl PreviewApp {
    fn preview() -> Self {
        Self {
            state: UiState::preview(CapabilityProfile::detect()),
            layout_engine: LayoutEngine::default(),
            measurements: MeasurementCache::default(),
            last_layout: None,
            last_display: None,
            last_measured: None,
            conversation: ConversationProjection::new("preview-session"),
            next_message_id: 1,
            runtime: None,
            loop_events: None,
            loop_input: None,
            delegated_progress: DelegatedProgressReceivers::new(),
            next_delegated_progress_id: 1,
            compaction: None,
            extension_command: None,
            extension_toggle: None,
            auth_events: None,
            setup_events: None,
            update_events: None,
            home: None,
            setup: None,
            sessions: Vec::new(),
            project_entries: Vec::new(),
            processes: Vec::new(),
            plan: None,
            extensions: Vec::new(),
            controls: ControlSnapshot::default(),
            pending_clipboard_images: std::collections::HashMap::new(),
            attachment_image: None,
            attachment_image_key: None,
            // Lazy: avoid stdio query in unit tests / headless; detect on first use.
            graphics: crate::tui_support::graphics::GraphicsContext { picker: None },
            // Force a first poll shortly after startup.
            last_git_poll: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(60))
                .unwrap_or_else(std::time::Instant::now),
        }
    }

    async fn initialize() -> Result<Self> {
        let (runtime, auth_events, setup_events) = RuntimeServices::initialize().await?;
        let ready = runtime.is_ready();
        let home = runtime.home_snapshot();
        let setup = runtime.setup_snapshot().await;
        let sessions = runtime.session_snapshot();
        let project_entries = runtime.project_entry_snapshot();
        let processes = runtime.process_snapshot();
        let plan = runtime.plan_snapshot();
        let extensions = runtime.extension_snapshot().await;
        let controls = runtime.controls_snapshot();
        let appearance = runtime.appearance_snapshot();
        let (update_tx, update_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            match mitsuro_core::updater::check_for_updates().await {
                Ok(None) => {
                    let _ = update_tx.send(mitsuro_core::updater::UpdateStatus::UpToDate);
                }
                Ok(Some(info)) => {
                    let _ = update_tx.send(mitsuro_core::updater::UpdateStatus::Available(info));
                }
                Err(error) => {
                    tracing::debug!("Update check failed: {error}");
                    let _ = update_tx.send(mitsuro_core::updater::UpdateStatus::Error(
                        error.to_string(),
                    ));
                }
            }
        });
        let mut app = Self {
            runtime: Some(runtime),
            auth_events: Some(auth_events),
            setup_events: Some(setup_events),
            update_events: Some(update_rx),
            home: Some(home),
            setup: Some(setup),
            sessions,
            project_entries,
            processes,
            plan,
            extensions,
            controls,
            ..Self::preview()
        };
        app.state.route = if ready {
            AppRoute::Home
        } else {
            AppRoute::Setup
        };
        if let Some(theme) = appearance.theme {
            app.state.appearance.theme = theme;
        }
        if let Some(motion) = appearance.motion {
            app.state.appearance.motion.preference = motion;
        }
        if ready {
            app.state.appearance.motion.set_active_regions(1);
        }
        Ok(app)
    }

    fn handle_event(&mut self, event: Event) -> Redraw {
        if matches!(self.state.route, AppRoute::Home)
            && self.state.appearance.motion.active_regions() > 0
            && matches!(event, Event::Key(_) | Event::Paste(_))
            && !self.state.splash.settled
        {
            // Skip wordmark stroke-in.
            self.state.splash.settled = true;
            self.state.appearance.motion.set_active_regions(0);
        }
        // Session title edit captures keys until Enter/Esc.
        if self.state.title_edit.active {
            if let Event::Key(key) = &event {
                if key.kind == KeyEventKind::Press || key.kind == KeyEventKind::Repeat {
                    return self.handle_title_edit_key(key.code, key.modifiers);
                }
            }
            // Drop mouse/paste noise while renaming.
            return Redraw::None;
        }
        if let Some(handled) = self.handle_question_event(&event) {
            return if handled { Redraw::Full } else { Redraw::None };
        }
        if self.setup_flow_active() {
            if let Some(handled) = self.handle_setup_event(&event) {
                return if handled { Redraw::Full } else { Redraw::None };
            }
        }
        if let Some(handled) = self.handle_artifact_inspector_event(&event) {
            return if handled { Redraw::Full } else { Redraw::None };
        }
        if self.picker_overlay_active() {
            if let Some(handled) = self.handle_picker_event(&event) {
                return if handled { Redraw::Full } else { Redraw::None };
            }
        }
        if let Some(handled) = self.handle_file_search_event(&event) {
            return if handled { Redraw::Full } else { Redraw::None };
        }
        if let Some(handled) = self.handle_slash_autocomplete_event(&event) {
            return if handled { Redraw::Full } else { Redraw::None };
        }
        if matches!(
            &event,
            Event::Key(key)
                if key.kind == KeyEventKind::Press
                    && key.code == KeyCode::Tab
                    && key.modifiers.is_empty()
                    && self.state.focus.is_composer()
                    && self.state.composer.text().is_empty()
                    && !matches!(self.state.route, AppRoute::Setup)
        ) {
            self.dispatch(UiAction::Invoke(ActionId::CycleReasoning));
            return Redraw::Full;
        }
        if matches!(
            &event,
            Event::Paste(value)
                if self.state.focus.is_composer()
                    && !matches!(self.state.route, AppRoute::Setup)
                    && crate::tui_support::utils::clipboard::looks_like_non_text_paste(value)
        ) {
            // Binary/garbage paste payload: only fall back to image when there is
            // no usable text (release-copy of stream text must not be shadowed).
            if crate::tui_support::utils::clipboard::read_clipboard_text().is_none() {
                if let Some(image) = crate::tui_support::utils::clipboard::read_clipboard_image() {
                    let id = uuid::Uuid::new_v4().to_string();
                    self.pending_clipboard_images
                        .insert(id.clone(), (image.width, image.height, image.rgba_bytes));
                    self.dispatch(UiAction::ComposerInserted(format!("[clipboard:{id}]")));
                    return Redraw::Full;
                }
            }
            // Drop non-text paste noise rather than inserting replacement chars.
            return Redraw::None;
        }
        // Ctrl+U installs a ready release. Composer kill-to-start stays when
        // no applyable update is showing.
        if matches!(
            &event,
            Event::Key(key)
                if key.kind == KeyEventKind::Press
                    && key.modifiers == KeyModifiers::CONTROL
                    && matches!(key.code, KeyCode::Char('u') | KeyCode::Char('U'))
                    && self.state.update.as_ref().is_some_and(|notice| notice.can_apply)
        ) {
            self.dispatch(UiAction::Invoke(ActionId::ApplyUpdate));
            return Redraw::Full;
        }
        // Ctrl+V: system clipboard (text or image), legacy input-bar parity.
        if matches!(
            &event,
            Event::Key(key)
                if key.kind == KeyEventKind::Press
                    && key.modifiers == KeyModifiers::CONTROL
                    && matches!(key.code, KeyCode::Char('v') | KeyCode::Char('V'))
                    && self.state.focus.is_composer()
                    && !matches!(self.state.route, AppRoute::Setup)
        ) {
            return self.paste_from_system_clipboard();
        }
        let action = match event {
            Event::Mouse(mouse) => return self.handle_mouse(mouse),
            Event::Key(key) if key.kind == KeyEventKind::Press => {
                ActionRegistry::action_for_key(active_context(&self.state), key)
                    .and_then(|action| self.registered_ui_action(action))
                    .or_else(|| composer_key_action(&self.state, key.code, key.modifiers))
            }
            Event::Paste(value)
                if self.state.focus.is_composer()
                    && !matches!(self.state.route, AppRoute::Setup) =>
            {
                Some(UiAction::ComposerInserted(value))
            }
            Event::Resize(width, height) => Some(UiAction::TerminalResized { width, height }),
            Event::FocusGained => Some(UiAction::TerminalFocusChanged(true)),
            Event::FocusLost => Some(UiAction::TerminalFocusChanged(false)),
            _ => None,
        };

        let Some(action) = action else {
            return Redraw::None;
        };

        self.dispatch(action);
        Redraw::Full
    }

    fn paste_from_system_clipboard(&mut self) -> Redraw {
        // Prefer text when present. Image-first caused stale clipboard images to
        // win over freshly release-copied transcript/composer text (macOS often
        // keeps both representations until text fully replaces the pasteboard).
        if let Some(text) = crate::tui_support::utils::clipboard::read_clipboard_text() {
            let prepared = prepare_pasted_composer_text(&text);
            self.dispatch(UiAction::ComposerInserted(prepared));
            return Redraw::Full;
        }
        if let Some(image) = crate::tui_support::utils::clipboard::read_clipboard_image() {
            let id = uuid::Uuid::new_v4().to_string();
            self.pending_clipboard_images
                .insert(id.clone(), (image.width, image.height, image.rgba_bytes));
            self.dispatch(UiAction::ComposerInserted(format!("[clipboard:{id}]")));
            return Redraw::Full;
        }
        Redraw::None
    }

    fn setup_flow_active(&self) -> bool {
        matches!(self.state.route, AppRoute::Setup)
            || self.state.overlay.as_ref().is_some_and(|overlay| {
                matches!(
                    overlay.kind,
                    crate::tui_v2::model::overlay::OverlayKind::Connections
                )
            })
    }

    fn handle_slash_autocomplete_event(&mut self, event: &Event) -> Option<bool> {
        if !self.state.composer.autocomplete_open
            || !self.state.focus.is_composer()
            || self.state.overlay.is_some()
        {
            return None;
        }
        let Event::Key(key) = event else {
            return None;
        };
        if key.kind != KeyEventKind::Press || !key.modifiers.is_empty() {
            return None;
        }
        let suggestions = slash::suggestions(self.state.composer.text());
        match key.code {
            KeyCode::Esc => {
                self.state.composer.autocomplete_open = false;
                self.state.composer.autocomplete_selected = 0;
                Some(true)
            }
            KeyCode::Up | KeyCode::Down => {
                self.state.composer.autocomplete_selected = cycle_index(
                    self.state.composer.autocomplete_selected,
                    suggestions.len(),
                    key.code == KeyCode::Down,
                );
                Some(true)
            }
            KeyCode::Tab | KeyCode::Enter => {
                if let Some(suggestion) = suggestions.get(self.state.composer.autocomplete_selected)
                {
                    self.state.composer.complete_slash(suggestion.primary);
                }
                Some(true)
            }
            _ => None,
        }
    }

    fn handle_file_search_event(&mut self, event: &Event) -> Option<bool> {
        if !self.state.composer.file_search_open
            || !self.state.focus.is_composer()
            || self.state.overlay.is_some()
        {
            return None;
        }
        let Event::Key(key) = event else {
            return None;
        };
        if key.kind != KeyEventKind::Press || !key.modifiers.is_empty() {
            return None;
        }
        let suggestions = file_search::suggestions(
            &self.project_entries,
            self.state.composer.text(),
            self.state.composer.cursor_byte(),
        );
        match key.code {
            KeyCode::Esc => {
                self.state.composer.file_search_open = false;
                self.state.composer.file_search_selected = 0;
                Some(true)
            }
            KeyCode::Up | KeyCode::Down => {
                self.state.composer.file_search_selected = cycle_index(
                    self.state.composer.file_search_selected,
                    suggestions.len(),
                    key.code == KeyCode::Down,
                );
                Some(true)
            }
            KeyCode::Tab | KeyCode::Enter => {
                if let Some(file) = suggestions.get(self.state.composer.file_search_selected) {
                    self.state.composer.complete_project_entry(file);
                }
                Some(true)
            }
            _ => None,
        }
    }

    fn handle_artifact_inspector_event(&mut self, event: &Event) -> Option<bool> {
        let part_id = self
            .state
            .overlay
            .as_ref()
            .and_then(|overlay| match &overlay.kind {
                OverlayKind::FileArtifactInspector { part_id } => Some(part_id.clone()),
                _ => None,
            })?;
        let viewport = self
            .last_layout
            .as_ref()
            .and_then(|layout| {
                layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::Overlay)
            })
            .map_or(1, |area| u32::from(area.height.saturating_sub(2).max(1)));
        match event {
            Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
                KeyCode::PageUp => {
                    self.scroll_artifact(&part_id, ScrollDirection::Backward, viewport);
                    Some(true)
                }
                KeyCode::PageDown => {
                    self.scroll_artifact(&part_id, ScrollDirection::Forward, viewport);
                    Some(true)
                }
                KeyCode::Home => {
                    self.scroll_artifact(&part_id, ScrollDirection::Start, u32::MAX);
                    Some(true)
                }
                KeyCode::End => {
                    self.scroll_artifact(&part_id, ScrollDirection::End, u32::MAX);
                    Some(true)
                }
                _ => None,
            },
            Event::Mouse(mouse) => match mouse.kind {
                crossterm::event::MouseEventKind::ScrollUp => {
                    self.scroll_artifact(&part_id, ScrollDirection::Backward, 3);
                    Some(true)
                }
                crossterm::event::MouseEventKind::ScrollDown => {
                    self.scroll_artifact(&part_id, ScrollDirection::Forward, 3);
                    Some(true)
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn handle_question_event(&mut self, event: &Event) -> Option<bool> {
        // Capture answer keys whenever a question is pending — not only when
        // DecisionDock still has focus. Clicking the transcript/composer used to
        // strand Enter on the wrong surface so "submit" appeared to do nothing.
        if self.state.overlay.is_some() || self.picker_overlay_active() {
            return None;
        }
        let questions = self
            .conversation
            .presentation()
            .pending_interactions
            .first()
            .and_then(|pending| match pending {
                PendingInteraction::Questions(value) => Some(value.clone()),
                _ => None,
            })?;
        let Event::Key(key) = event else {
            return None;
        };
        if key.kind != KeyEventKind::Press || !key.modifiers.is_empty() {
            return None;
        }
        let Some(question) = questions
            .questions
            .get(self.state.decision_dock.current_question)
        else {
            return Some(false);
        };
        let option_count = question.options.len();
        // Ensure chrome reflects where input is going.
        self.state.focus = crate::tui_v2::model::focus::FocusTarget::DecisionDock;
        match key.code {
            KeyCode::Up | KeyCode::Left => {
                self.state.decision_dock.selected_option = cycle_index(
                    self.state.decision_dock.selected_option,
                    option_count,
                    false,
                );
                Some(true)
            }
            KeyCode::Down | KeyCode::Right => {
                self.state.decision_dock.selected_option =
                    cycle_index(self.state.decision_dock.selected_option, option_count, true);
                Some(true)
            }
            KeyCode::Char(character @ '1'..='9') => {
                let index = character.to_digit(10).unwrap_or(1) as usize - 1;
                if index >= option_count {
                    return Some(true);
                }
                self.state.decision_dock.selected_option = index;
                if question.multi_select {
                    // Digit toggles multi options (legacy parity for Space still holds).
                    if self.state.decision_dock.toggled_options.contains(&index) {
                        self.state
                            .decision_dock
                            .toggled_options
                            .retain(|item| *item != index);
                    } else {
                        self.state.decision_dock.toggled_options.push(index);
                    }
                } else {
                    // Single-select: digit commits immediately (legacy parity).
                    self.confirm_question_answer(&questions);
                }
                Some(true)
            }
            KeyCode::Char(' ') if question.multi_select => {
                let selected = self.state.decision_dock.selected_option;
                if self.state.decision_dock.toggled_options.contains(&selected) {
                    self.state
                        .decision_dock
                        .toggled_options
                        .retain(|index| *index != selected);
                } else {
                    self.state.decision_dock.toggled_options.push(selected);
                }
                Some(true)
            }
            KeyCode::Enter => {
                self.confirm_question_answer(&questions);
                Some(true)
            }
            // Swallow bare typing so it does not leak into the composer mid-prompt.
            KeyCode::Char(_) => Some(true),
            _ => None,
        }
    }

    fn confirm_question_answer(
        &mut self,
        pending: &crate::tui_v2::model::conversation::PendingQuestions,
    ) {
        let Some(question) = pending
            .questions
            .get(self.state.decision_dock.current_question)
        else {
            return;
        };
        let answer = if question.multi_select {
            // Enter with nothing toggled: take the focused row so multi-select
            // does not submit an empty answers array and look "broken".
            let indices = if self.state.decision_dock.toggled_options.is_empty() {
                vec![self.state.decision_dock.selected_option]
            } else {
                self.state.decision_dock.toggled_options.clone()
            };
            let labels: Vec<String> = indices
                .iter()
                .filter_map(|index| question.options.get(*index))
                .map(|option| option.label.clone())
                .collect();
            if labels.is_empty() {
                return;
            }
            QuestionAnswer::Multiple(labels)
        } else {
            let Some(option) = question
                .options
                .get(self.state.decision_dock.selected_option)
            else {
                return;
            };
            QuestionAnswer::Single(option.label.clone())
        };
        self.state.decision_dock.answers.push(answer);
        self.state.decision_dock.current_question += 1;
        self.state.decision_dock.selected_option = 0;
        self.state.decision_dock.toggled_options.clear();
        if self.state.decision_dock.current_question < pending.questions.len() {
            return;
        }

        let response = serialize_question_answers(pending, &self.state.decision_dock.answers);
        self.continue_pending_interaction(&pending.tool_call_id, &response);
    }

    fn continue_pending_interaction(&mut self, tool_call_id: &str, response: &str) {
        if let Some(runtime) = &mut self.runtime {
            match runtime.continue_interaction(tool_call_id, response) {
                Ok(started) => {
                    self.conversation.apply_event(LoopEvent::ToolResult {
                        id: tool_call_id.to_owned(),
                        output: response.to_owned(),
                        is_error: false,
                    });
                    self.loop_events = Some(started.events);
                    self.loop_input = Some(started.input);
                    self.attach_delegated_progress(started.delegated_progress);
                    let _ = reduce(
                        &mut self.state,
                        UiAction::AgentRunChanged(state::AgentRunState::Running),
                    );
                    self.state.decision_dock = Default::default();
                    self.state.focus = crate::tui_v2::model::focus::FocusTarget::Composer;
                }
                Err(error) => {
                    // Rewind the dock so a failed submit does not leave a blank
                    // bordered shell with current_question past the end.
                    if !self.state.decision_dock.answers.is_empty() {
                        self.state.decision_dock.answers.pop();
                        self.state.decision_dock.current_question =
                            self.state.decision_dock.answers.len();
                        self.state.decision_dock.selected_option = 0;
                        self.state.decision_dock.toggled_options.clear();
                    }
                    self.state.focus = crate::tui_v2::model::focus::FocusTarget::DecisionDock;
                    self.conversation.apply_event(LoopEvent::Error {
                        error: error.to_string(),
                    });
                }
            }
        } else {
            self.conversation.apply_event(LoopEvent::ToolResult {
                id: tool_call_id.to_owned(),
                output: response.to_owned(),
                is_error: false,
            });
            self.state.decision_dock = Default::default();
            self.state.focus = crate::tui_v2::model::focus::FocusTarget::Composer;
        }
    }

    fn picker_overlay_active(&self) -> bool {
        self.state.overlay.as_ref().is_some_and(|overlay| {
            matches!(
                overlay.kind,
                OverlayKind::SessionPicker
                    | OverlayKind::CommandPalette
                    | OverlayKind::ModelPicker
                    | OverlayKind::Help
                    | OverlayKind::Processes
                    | OverlayKind::ExtensionsCenter
                    | OverlayKind::ThemeAppearance
            )
        })
    }

    fn handle_picker_event(&mut self, event: &Event) -> Option<bool> {
        let kind = self.state.overlay.as_ref()?.kind.clone();
        if matches!(kind, OverlayKind::Help) {
            return None;
        }
        let filterable = matches!(
            kind,
            OverlayKind::SessionPicker | OverlayKind::CommandPalette | OverlayKind::ModelPicker
        );
        if let Event::Paste(value) = event {
            if filterable {
                self.state.picker.query.push_str(value);
                self.state.picker.selected = 0;
                return Some(true);
            }
            return None;
        }
        let Event::Key(key) = event else {
            return None;
        };
        if key.kind != KeyEventKind::Press || !key.modifiers.is_empty() {
            return None;
        }
        match key.code {
            KeyCode::Up | KeyCode::Down => {
                let count = self.picker_item_count(&kind);
                self.state.picker.selected =
                    cycle_index(self.state.picker.selected, count, key.code == KeyCode::Down);
                Some(true)
            }
            KeyCode::Backspace if filterable => {
                self.state.picker.query.pop();
                self.state.picker.selected = 0;
                self.state.picker.error = None;
                Some(true)
            }
            KeyCode::Char(character) if filterable => {
                self.state.picker.query.push(character);
                self.state.picker.selected = 0;
                self.state.picker.error = None;
                Some(true)
            }
            KeyCode::Enter => {
                match kind {
                    OverlayKind::SessionPicker => self.open_selected_session(),
                    OverlayKind::CommandPalette => self.run_selected_command(),
                    OverlayKind::ModelPicker => self.select_picker_model(),
                    OverlayKind::ThemeAppearance => self.apply_selected_appearance(),
                    OverlayKind::Processes => self.stop_selected_process(),
                    OverlayKind::ExtensionsCenter => self.toggle_selected_extension(),
                    _ => {}
                }
                Some(true)
            }
            _ => None,
        }
    }

    fn picker_item_count(&self, kind: &OverlayKind) -> usize {
        match kind {
            OverlayKind::SessionPicker => crate::tui_v2::components::session_picker::filtered(
                &self.sessions,
                &self.state.picker.query,
            )
            .count(),
            OverlayKind::CommandPalette => {
                crate::tui_v2::components::command_palette::filtered(&self.state.picker.query).len()
            }
            OverlayKind::ModelPicker => self.setup.as_ref().map_or(0, |setup| {
                crate::tui_v2::components::model_picker::filtered(setup, &self.state.picker.query)
                    .len()
            }),
            OverlayKind::Processes => self.processes.len(),
            OverlayKind::ExtensionsCenter => self.extensions.len(),
            OverlayKind::ThemeAppearance => {
                crate::tui_v2::components::service_inspector::THEMES.len()
                    + crate::tui_v2::components::service_inspector::MOTION.len()
            }
            _ => 0,
        }
    }

    fn stop_selected_process(&mut self) {
        let selected = self.processes.get(self.state.picker.selected).cloned();
        let Some(process) = selected else {
            return;
        };
        if !process.active {
            self.state.picker.error = Some(format!(
                "{} is already {}.",
                process.command, process.status
            ));
            return;
        }
        let result = self
            .runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
            .and_then(|runtime| runtime.stop_process(&process.id));
        match result {
            Ok(()) => {
                if let Some(runtime) = &self.runtime {
                    self.processes = runtime.process_snapshot();
                }
                self.state.picker.error = None;
            }
            Err(error) => self.state.picker.error = Some(error.to_string()),
        }
    }

    fn toggle_selected_extension(&mut self) {
        let Some(extension) = self.extensions.get(self.state.picker.selected).cloned() else {
            return;
        };
        if self.extension_toggle.is_some() {
            self.state.picker.error = Some("Wait for the current extension action.".to_owned());
            return;
        }
        if extension.category == "MCP" {
            let result = self
                .runtime
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
                .and_then(|runtime| runtime.begin_mcp_toggle(&extension));
            match result {
                Ok(receiver) => {
                    if let Some(row) = self.extensions.get_mut(self.state.picker.selected) {
                        row.status = if extension.enabled {
                            "disconnecting"
                        } else {
                            "connecting"
                        }
                        .to_owned();
                    }
                    self.extension_toggle = Some(receiver);
                    self.state.picker.error = None;
                }
                Err(error) => self.state.picker.error = Some(error.to_string()),
            }
            return;
        }
        let result = self
            .runtime
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
            .and_then(|runtime| runtime.toggle_extension(&extension));
        match result {
            Ok(()) => {
                if let Some(runtime) = &self.runtime {
                    self.extensions = futures::executor::block_on(runtime.extension_snapshot());
                }
                self.state.picker.error = None;
                self.state.picker.selected = self
                    .state
                    .picker
                    .selected
                    .min(self.extensions.len().saturating_sub(1));
            }
            Err(error) => self.state.picker.error = Some(error.to_string()),
        }
    }

    async fn finish_extension_toggle(&mut self, result: Result<(), String>) {
        if let Some(runtime) = &mut self.runtime {
            self.extensions = runtime.refresh_extension_runtime().await;
        }
        self.state.picker.selected = self
            .state
            .picker
            .selected
            .min(self.extensions.len().saturating_sub(1));
        self.state.picker.error = result.err();
    }

    fn run_selected_command(&mut self) {
        let action = crate::tui_v2::components::command_palette::filtered(&self.state.picker.query)
            .get(self.state.picker.selected)
            .map(|definition| definition.id);
        let Some(action) = action else {
            return;
        };
        if let Some(overlay_id) = self.state.overlay.as_ref().map(|overlay| overlay.id) {
            let _ = reduce(&mut self.state, UiAction::OverlayClosed(overlay_id));
        }
        self.dispatch(UiAction::Invoke(action));
    }

    fn select_picker_model(&mut self) {
        let choice = self.setup.as_ref().and_then(|setup| {
            crate::tui_v2::components::model_picker::filtered(setup, &self.state.picker.query)
                .get(self.state.picker.selected)
                .map(|choice| {
                    (
                        choice.provider.id,
                        choice.model.key.clone(),
                        choice.model.label.clone(),
                    )
                })
        });
        let Some((provider, key, label)) = choice else {
            return;
        };
        let result = self
            .runtime
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
            .and_then(|runtime| {
                runtime.activate_provider(provider);
                runtime.select_model(&key)
            });
        match result {
            Ok(()) => {
                if let Some(setup) = &mut self.setup {
                    setup.selected_model = Some(label);
                }
                if let Some(runtime) = &self.runtime {
                    self.home = Some(runtime.home_snapshot());
                    self.controls = runtime.controls_snapshot();
                }
                if let Some(overlay_id) = self.state.overlay.as_ref().map(|overlay| overlay.id) {
                    let _ = reduce(&mut self.state, UiAction::OverlayClosed(overlay_id));
                }
            }
            Err(error) => self.state.picker.error = Some(error.to_string()),
        }
    }

    fn apply_selected_appearance(&mut self) {
        let theme_count = crate::tui_v2::components::service_inspector::THEMES.len();
        if let Some(theme) = crate::tui_v2::components::service_inspector::THEMES
            .get(self.state.picker.selected)
            .copied()
        {
            self.dispatch(UiAction::ThemeChanged(theme));
            return;
        }
        if let Some(preference) = crate::tui_v2::components::service_inspector::MOTION
            .get(self.state.picker.selected.saturating_sub(theme_count))
            .copied()
        {
            self.dispatch(UiAction::MotionPreferenceChanged(preference));
        }
    }

    fn open_selected_session(&mut self) {
        let selected = crate::tui_v2::components::session_picker::filtered(
            &self.sessions,
            &self.state.picker.query,
        )
        .nth(self.state.picker.selected)
        .map(|session| session.session_id.clone());
        let Some(session_id) = selected else {
            return;
        };
        let result = self
            .runtime
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
            .and_then(|runtime| runtime.open_session(&session_id));
        match result {
            Ok(loaded) => self.apply_loaded_session(loaded),
            Err(error) => self.state.picker.error = Some(error.to_string()),
        }
    }

    fn apply_loaded_session(&mut self, loaded: LoadedSession) {
        if let Some(runtime) = &self.runtime {
            self.controls = runtime.controls_snapshot();
            // open_session already set current_session_id (+ working_dir) —
            // reload goal/plan and project index for the dock / chrome.
            self.plan = runtime.plan_snapshot();
            self.project_entries = runtime.project_entry_snapshot();
        } else {
            self.plan = None;
        }
        self.conversation =
            ConversationProjection::from_model_messages(&loaded.session_id, &loaded.messages);
        if let Some(recovery) = &loaded.recovery {
            self.conversation.merge_recovery(recovery);
        }
        self.conversation
            .restore_delegation_groups(&loaded.delegation_groups);
        self.conversation.set_title(Some(loaded.title));
        // Context-used bar is driven by metadata.usage; rebuild from sessions.token_count
        // since history load does not re-emit Usage loop events.
        self.conversation
            .set_usage_from_token_count(loaded.token_count);
        let has_pending = !self
            .conversation
            .presentation()
            .pending_interactions
            .is_empty();
        let _ = reduce(
            &mut self.state,
            UiAction::RouteChanged(AppRoute::Conversation {
                session_id: SessionId::from_canonical(&loaded.session_id),
            }),
        );
        self.state.decision_dock = Default::default();
        self.state.dock.plan_scroll = 0;
        // Force git + context chrome refresh for the reopened session's cwd.
        self.last_git_poll = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(60))
            .unwrap_or_else(std::time::Instant::now);
        self.refresh_workspace_chrome();
        self.state.focus = if has_pending {
            crate::tui_v2::model::focus::FocusTarget::DecisionDock
        } else {
            crate::tui_v2::model::focus::FocusTarget::Composer
        };
    }

    fn handle_setup_event(&mut self, event: &Event) -> Option<bool> {
        if let Event::Paste(value) = event {
            if matches!(
                self.state.setup.step,
                SetupStep::Credential | SetupStep::OAuthPasteCode
            ) {
                self.state.composer.insert(value);
                return Some(true);
            }
            return Some(false);
        }
        let Event::Key(key) = event else {
            return None;
        };
        if key.kind != KeyEventKind::Press || !key.modifiers.is_empty() {
            return None;
        }
        let setup = self.setup.as_ref()?;
        match key.code {
            KeyCode::Up | KeyCode::Down => {
                let forward = key.code == KeyCode::Down;
                match self.state.setup.step {
                    SetupStep::Provider => {
                        self.state.setup.provider_index = cycle_index(
                            self.state.setup.provider_index,
                            setup.providers.len().min(8),
                            forward,
                        );
                    }
                    SetupStep::AuthMethod => {
                        let count = setup
                            .providers
                            .get(self.state.setup.provider_index)
                            .map_or(0, |provider| provider.auth_methods.len());
                        self.state.setup.auth_method_index =
                            cycle_index(self.state.setup.auth_method_index, count, forward);
                    }
                    SetupStep::Model => {
                        let count = setup
                            .providers
                            .get(self.state.setup.provider_index)
                            .map_or(0, |provider| provider.models.len().min(8));
                        self.state.setup.model_index =
                            cycle_index(self.state.setup.model_index, count, forward);
                    }
                    SetupStep::Credential
                    | SetupStep::OAuthWaiting
                    | SetupStep::OAuthPasteCode
                    | SetupStep::CatalogLoading => {}
                }
                Some(true)
            }
            KeyCode::Esc if self.state.setup.step != SetupStep::Provider => {
                self.state.setup.step = match self.state.setup.step {
                    SetupStep::AuthMethod | SetupStep::Model => SetupStep::Provider,
                    SetupStep::Credential
                    | SetupStep::OAuthWaiting
                    | SetupStep::OAuthPasteCode
                    | SetupStep::CatalogLoading => SetupStep::AuthMethod,
                    SetupStep::Provider => SetupStep::Provider,
                };
                self.state.setup.error = None;
                self.state.setup.oauth_message = None;
                self.state.setup.oauth_url = None;
                self.state.setup.device_code = None;
                self.state.composer.take_text();
                Some(true)
            }
            KeyCode::Backspace
                if matches!(
                    self.state.setup.step,
                    SetupStep::Credential | SetupStep::OAuthPasteCode
                ) =>
            {
                self.state.composer.backspace();
                Some(true)
            }
            KeyCode::Char(character)
                if matches!(
                    self.state.setup.step,
                    SetupStep::Credential | SetupStep::OAuthPasteCode
                ) =>
            {
                self.state.composer.insert(&character.to_string());
                Some(true)
            }
            KeyCode::Enter => {
                self.advance_setup_flow();
                Some(true)
            }
            KeyCode::Char(_) | KeyCode::Backspace => Some(true),
            _ => None,
        }
    }

    fn advance_setup_flow(&mut self) {
        let Some(setup) = self.setup.as_mut() else {
            return;
        };
        let Some(provider) = setup
            .providers
            .get(self.state.setup.provider_index)
            .cloned()
        else {
            return;
        };
        self.state.setup.error = None;
        match self.state.setup.step {
            SetupStep::Provider => {
                if provider.connected {
                    if let Some(runtime) = &mut self.runtime {
                        runtime.activate_provider(provider.id);
                    }
                }
                self.state.setup.step = if provider.connected {
                    SetupStep::Model
                } else {
                    SetupStep::AuthMethod
                };
                self.state.setup.auth_method_index = 0;
                self.state.setup.model_index = 0;
            }
            SetupStep::AuthMethod => {
                let Some(method) = provider
                    .auth_methods
                    .get(self.state.setup.auth_method_index)
                    .copied()
                else {
                    self.state.setup.error =
                        Some("No authentication methods are available.".to_owned());
                    return;
                };
                self.state.setup.selected_auth_method = Some(method);
                if method == mitsuro_core::auth::AuthMethod::ApiKey {
                    self.state.setup.step = SetupStep::Credential;
                    return;
                }
                let result = self
                    .runtime
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
                    .and_then(|runtime| runtime.start_oauth_flow(provider.id, method));
                match result {
                    Ok(OAuthStart::Waiting) => {
                        self.state.setup.step = SetupStep::OAuthWaiting;
                        self.state.setup.oauth_message =
                            Some("Waiting for secure authorization…".to_owned());
                    }
                    Ok(OAuthStart::PasteCode { authorization_url }) => {
                        self.state.setup.step = SetupStep::OAuthPasteCode;
                        self.state.setup.oauth_message = Some(
                            "Authorize in the browser, then paste the returned code.".to_owned(),
                        );
                        self.state.setup.oauth_url = Some(authorization_url);
                    }
                    Err(error) => self.state.setup.error = Some(error.to_string()),
                }
            }
            SetupStep::Credential => {
                let credential = self.state.composer.take_text();
                let result = self
                    .runtime
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
                    .and_then(|runtime| runtime.connect_provider(provider.id, credential));
                match result {
                    Ok(()) => {
                        if let Some(provider) =
                            setup.providers.get_mut(self.state.setup.provider_index)
                        {
                            provider.connected = true;
                        }
                        let loading = self
                            .runtime
                            .as_ref()
                            .is_some_and(|runtime| runtime.begin_catalog_refresh(provider.id));
                        self.state.setup.step = if loading {
                            self.state.setup.oauth_message =
                                Some("Loading models for this account…".to_owned());
                            SetupStep::CatalogLoading
                        } else {
                            SetupStep::Model
                        };
                    }
                    Err(error) => self.state.setup.error = Some(error.to_string()),
                }
            }
            SetupStep::OAuthPasteCode => {
                let code = self.state.composer.take_text();
                let result = self
                    .runtime
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
                    .and_then(|runtime| runtime.submit_anthropic_oauth_code(provider.id, code));
                match result {
                    Ok(()) => {
                        self.state.setup.step = SetupStep::OAuthWaiting;
                        self.state.setup.oauth_message =
                            Some("Exchanging the authorization code…".to_owned());
                    }
                    Err(error) => self.state.setup.error = Some(error.to_string()),
                }
            }
            SetupStep::OAuthWaiting => {}
            SetupStep::CatalogLoading => {}
            SetupStep::Model => {
                let Some(model) = provider.models.get(self.state.setup.model_index) else {
                    self.state.setup.error =
                        Some("No models are available for this provider.".to_owned());
                    return;
                };
                let result = self
                    .runtime
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
                    .and_then(|runtime| runtime.select_model(&model.key));
                match result {
                    Ok(()) => {
                        setup.selected_model = Some(model.label.clone());
                        if let Some(runtime) = &self.runtime {
                            self.home = Some(runtime.home_snapshot());
                        }
                        self.state.setup = Default::default();
                        if let Some(overlay) = self.state.overlay.as_ref() {
                            let overlay_id = overlay.id;
                            let _ = reduce(&mut self.state, UiAction::OverlayClosed(overlay_id));
                        }
                        if matches!(self.state.route, AppRoute::Setup) {
                            let _ = reduce(&mut self.state, UiAction::RouteChanged(AppRoute::Home));
                            self.state.appearance.motion.set_active_regions(1);
                        }
                    }
                    Err(error) => self.state.setup.error = Some(error.to_string()),
                }
            }
        }
    }

    fn registered_ui_action(&self, action: ActionId) -> Option<UiAction> {
        if matches!(self.state.route, AppRoute::Setup)
            && matches!(action, ActionId::Submit | ActionId::InsertNewline)
        {
            return None;
        }
        let decision = match action {
            ActionId::ApproveDecision => Some(DecisionAction::Approve),
            ActionId::DenyDecision => Some(DecisionAction::Deny),
            ActionId::InspectDecision => Some(DecisionAction::Inspect),
            ActionId::ActivateDecision => Some(self.state.decision_dock.focused_action),
            _ => None,
        };
        match decision {
            Some(action) => self
                .conversation
                .presentation()
                .pending_interactions
                .first()
                .map(decision_target)
                .map(|target| UiAction::DecisionRequested { target, action }),
            None => Some(UiAction::Invoke(action)),
        }
    }

    fn handle_mouse(&mut self, event: crossterm::event::MouseEvent) -> Redraw {
        let Some(layout) = self.last_layout.as_ref() else {
            return Redraw::None;
        };
        let drag = self.state.mouse.scrollbar_drag.clone();
        let selecting_composer = self.state.mouse.selecting_composer;
        let resolution = resolve_mouse_with_drag(layout, event, drag.as_ref(), selecting_composer);
        match resolution {
            Some(MouseResolution::Action(action)) => {
                self.state.mouse.clear_selection();
                self.state.mouse.scrollbar_drag = None;
                let action = match action {
                    UiAction::Invoke(action) => self.registered_ui_action(action),
                    action => Some(action),
                };
                let Some(action) = action else {
                    return Redraw::None;
                };
                self.dispatch(action);
                Redraw::Full
            }
            Some(MouseResolution::Scroll { region, rows }) => {
                let direction = if rows < 0 {
                    ScrollDirection::Backward
                } else {
                    ScrollDirection::Forward
                };
                let amount = rows.unsigned_abs();
                match region {
                    crate::tui_v2::layout::snapshot::ScrollRegionId::Transcript => {
                        self.scroll_transcript(direction, amount);
                    }
                    crate::tui_v2::layout::snapshot::ScrollRegionId::Artifact(part_id) => {
                        self.scroll_artifact(&part_id, direction, amount);
                    }
                    crate::tui_v2::layout::snapshot::ScrollRegionId::Overlay(_) => {
                        self.scroll_overlay_picker(direction);
                    }
                    crate::tui_v2::layout::snapshot::ScrollRegionId::PlanDock => {
                        self.state.focus = FocusTarget::PlanDock;
                        self.state.dock.plugin_focused = false;
                        let (content, visible) = self.plan_dock_scroll_metrics();
                        let delta = if matches!(direction, ScrollDirection::Backward) {
                            -(amount as i32)
                        } else {
                            amount as i32
                        };
                        self.state.dock.scroll_plan(delta, content, visible);
                    }
                    crate::tui_v2::layout::snapshot::ScrollRegionId::PluginDock => {
                        self.state.focus = FocusTarget::PluginDock;
                        self.state.dock.plugin_focused = true;
                    }
                    crate::tui_v2::layout::snapshot::ScrollRegionId::Composer => {
                        self.state.focus = FocusTarget::Composer;
                        let (width, visible) = self.composer_layout_metrics();
                        let delta = if matches!(direction, ScrollDirection::Backward) {
                            -(amount as isize)
                        } else {
                            amount as isize
                        };
                        self.state.composer.scroll_viewport(delta, width, visible);
                    }
                    crate::tui_v2::layout::snapshot::ScrollRegionId::ComposerAssist => {
                        self.state.focus = FocusTarget::Composer;
                        let steps = amount.max(1) as usize;
                        let forward = matches!(direction, ScrollDirection::Forward);
                        self.nudge_assist_selection(forward, steps);
                    }
                }
                Redraw::Full
            }
            Some(MouseResolution::OpenLink(url)) => {
                self.state.mouse.clear_selection();
                let _ = webbrowser::open(&url);
                Redraw::None
            }
            Some(MouseResolution::SelectionStart(point)) => {
                // Click-drag selection: do not steal layout-heavy focus changes.
                self.state.mouse.begin_selection(point);
                self.state.mouse.edge_scroll.clear();
                Redraw::Light
            }
            Some(MouseResolution::SelectionDrag { point, column, row }) => {
                if !self.state.mouse.selecting {
                    return Redraw::None;
                }
                self.state.mouse.position = Some((column, row));
                let changed = self
                    .state
                    .mouse
                    .selection
                    .as_ref()
                    .is_none_or(|selection| selection.end != point);
                self.state.mouse.drag_selection(point);
                // Edge zone while drag-selecting the stream (legacy TUI parity).
                let scrolled = self.nudge_transcript_edge_scroll_for_selection(column, row);
                if scrolled || changed {
                    if scrolled {
                        Redraw::Full
                    } else {
                        Redraw::Light
                    }
                } else {
                    Redraw::None
                }
            }
            Some(MouseResolution::SelectionEnd) => {
                self.state.mouse.edge_scroll.clear();
                if self.state.mouse.scrollbar_drag.take().is_some() {
                    return Redraw::None;
                }
                if self.state.mouse.selecting_composer {
                    self.state.mouse.selecting_composer = false;
                    if let Some((lo, hi)) = self.state.mouse.composer_selection_ordered() {
                        if lo != hi {
                            let end = inclusive_end_boundary(self.state.composer.text(), hi);
                            let text = self.state.composer.text();
                            let start = text.floor_char_boundary(lo.min(text.len()));
                            if start < end {
                                let selected = text[start..end].to_owned();
                                let _ = crate::tui_support::utils::clipboard::write_clipboard_text(
                                    &selected,
                                );
                                // Keep buffer selection so typing replaces the range.
                                self.state.composer.set_selection(start, end);
                            }
                        } else {
                            self.state.composer.clear_selection();
                        }
                    }
                    self.state.mouse.composer_selection = None;
                    return Redraw::Light;
                }
                if !self.state.mouse.selecting {
                    return Redraw::None;
                }
                self.state.mouse.selecting = false;
                // Release-to-copy: write visible selected stream text, then clear.
                if let Some(text) = self.selected_transcript_text() {
                    let _ = crate::tui_support::utils::clipboard::write_clipboard_text(&text);
                }
                // Drop highlight after copy so the next click is clean.
                self.state.mouse.clear_selection();
                Redraw::Light
            }
            Some(MouseResolution::ComposerClick { column, row }) => {
                self.state.mouse.selection = None;
                self.state.mouse.selecting = false;
                self.state.mouse.scrollbar_drag = None;
                self.state.focus = FocusTarget::Composer;
                // Prefer opening a bracket attachment preview when clicking a chip.
                if self.try_open_composer_attachment_at(column, row) {
                    return Redraw::Full;
                }
                let byte = self.composer_byte_from_click(column, row);
                self.state.composer.clear_selection();
                self.state.composer.set_cursor_byte(byte);
                self.state.mouse.begin_composer_selection(byte);
                Redraw::Full
            }
            Some(MouseResolution::ComposerSelectionDrag { column, row }) => {
                if !self.state.mouse.selecting_composer {
                    return Redraw::None;
                }
                self.state.mouse.position = Some((column, row));
                let scrolled = self.nudge_composer_edge_scroll_for_selection(column, row);
                // After an edge pan, re-hit with the (possibly) scrolled viewport.
                let byte = self.composer_byte_from_click(column, row);
                let changed = self
                    .state
                    .mouse
                    .composer_selection
                    .is_none_or(|(_, end)| end != byte);
                // Selection drag must not force follow-cursor pan (that fights edge scroll).
                self.state.composer.buffer.set_cursor(byte);
                self.state.mouse.drag_composer_selection(byte);
                if let Some((start, end)) = self.state.mouse.composer_selection {
                    self.state.composer.set_selection(start, end);
                }
                if scrolled || changed {
                    if scrolled {
                        Redraw::Full
                    } else {
                        Redraw::Light
                    }
                } else {
                    Redraw::None
                }
            }
            Some(MouseResolution::ScrollbarJump { region, y }) => {
                self.state.mouse.clear_selection();
                self.state.mouse.scrollbar_drag = Some(region.clone());
                self.jump_scrollbar(&region, y);
                Redraw::Full
            }
            Some(MouseResolution::ScrollbarDrag { region, y }) => {
                self.jump_scrollbar(&region, y);
                Redraw::Full
            }
            Some(MouseResolution::ScrollbarEnd) => {
                self.state.mouse.scrollbar_drag = None;
                Redraw::None
            }
            Some(MouseResolution::Hover { position, link }) => {
                let changed = self.state.mouse.position != Some(position)
                    || self.state.mouse.hover_link != link;
                self.state.mouse.position = Some(position);
                self.state.mouse.hover_link = link;
                // Hover never forces a layout pass.
                let _ = changed;
                Redraw::None
            }
            Some(MouseResolution::EditSessionTitle) => {
                self.start_title_edit();
                Redraw::Full
            }
            Some(MouseResolution::ComposerAssistClick { column: _, row }) => {
                self.state.focus = FocusTarget::Composer;
                self.click_assist_row(row);
                Redraw::Full
            }
            None => Redraw::None,
        }
    }

    fn nudge_assist_selection(&mut self, forward: bool, steps: usize) {
        if self.state.composer.autocomplete_open {
            let total = slash::suggestions(self.state.composer.text()).len();
            if total == 0 {
                return;
            }
            let mut idx = self.state.composer.autocomplete_selected;
            for _ in 0..steps {
                idx = cycle_index(idx, total, forward);
            }
            self.state.composer.autocomplete_selected = idx;
            return;
        }
        if self.state.composer.file_search_open {
            let total = file_search::suggestions(
                &self.project_entries,
                self.state.composer.text(),
                self.state.composer.cursor_byte(),
            )
            .len();
            if total == 0 {
                return;
            }
            let mut idx = self.state.composer.file_search_selected;
            for _ in 0..steps {
                idx = cycle_index(idx, total, forward);
            }
            self.state.composer.file_search_selected = idx;
        }
    }

    fn click_assist_row(&mut self, screen_y: u16) {
        let Some(area) = self.last_layout.as_ref().and_then(|layout| {
            layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerAutocomplete)
        }) else {
            return;
        };
        if self.state.composer.autocomplete_open {
            let suggestions = slash::suggestions(self.state.composer.text());
            let selected = self.state.composer.autocomplete_selected;
            if let Some(index) = crate::tui_v2::components::slash_autocomplete::index_at_y(
                area,
                screen_y,
                suggestions.len(),
                selected,
            ) {
                self.state.composer.autocomplete_selected = index;
                if let Some(suggestion) = suggestions.get(index) {
                    self.state.composer.complete_slash(suggestion.primary);
                }
            }
            return;
        }
        if self.state.composer.file_search_open {
            let matches = file_search::suggestions(
                &self.project_entries,
                self.state.composer.text(),
                self.state.composer.cursor_byte(),
            );
            let selected = self.state.composer.file_search_selected;
            if let Some(index) = crate::tui_v2::components::file_search::index_at_y(
                area,
                screen_y,
                matches.len(),
                selected,
            ) {
                self.state.composer.file_search_selected = index;
                if let Some(entry) = matches.get(index) {
                    self.state.composer.complete_project_entry(entry);
                }
            }
        }
    }

    fn start_title_edit(&mut self) {
        if !matches!(self.state.route, AppRoute::Conversation { .. }) {
            return;
        }
        let current = self.conversation.presentation().metadata.title.clone();
        self.state.title_edit.start(current.as_deref());
    }

    fn handle_title_edit_key(&mut self, code: KeyCode, modifiers: KeyModifiers) -> Redraw {
        match code {
            KeyCode::Enter => {
                self.save_title_edit();
                Redraw::Full
            }
            KeyCode::Esc => {
                self.state.title_edit.cancel();
                Redraw::Full
            }
            KeyCode::Backspace => {
                self.state.title_edit.backspace();
                Redraw::Full
            }
            KeyCode::Char(ch)
                if !modifiers.contains(KeyModifiers::CONTROL)
                    && !modifiers.contains(KeyModifiers::ALT) =>
            {
                self.state.title_edit.insert_char(ch);
                Redraw::Full
            }
            _ => Redraw::None,
        }
    }

    fn save_title_edit(&mut self) {
        let Some(title) = self.state.title_edit.finish() else {
            return;
        };
        self.conversation.set_title(Some(title.clone()));
        if let (Some(runtime), AppRoute::Conversation { session_id }) =
            (self.runtime.as_ref(), &self.state.route)
        {
            let _ = runtime.update_session_title(session_id.as_str(), &title);
        }
    }

    /// Refresh git diff + agent context chrome for the top context bar.
    fn refresh_workspace_chrome(&mut self) {
        // Agent context fill from last usage event + selected model window.
        let used = self
            .conversation
            .presentation()
            .metadata
            .usage
            .as_ref()
            .map(|usage| {
                usage
                    .total_tokens
                    .max(usage.prompt_tokens)
                    .max(usage.input_tokens)
            })
            .unwrap_or(0);
        let max = self
            .runtime
            .as_ref()
            .map(|runtime| runtime.context_window())
            .unwrap_or(0);
        self.state.workspace.context_used = used;
        self.state.workspace.context_max = max;

        // Throttle git status (can be relatively expensive on large trees).
        const GIT_POLL: std::time::Duration = std::time::Duration::from_secs(2);
        if self.last_git_poll.elapsed() < GIT_POLL {
            return;
        }
        self.last_git_poll = std::time::Instant::now();
        let cwd = self
            .runtime
            .as_ref()
            .map(|runtime| runtime.working_dir().to_path_buf())
            .or_else(|| std::env::current_dir().ok());
        let Some(cwd) = cwd else {
            return;
        };
        match mitsuro_core::git::status(&cwd) {
            Ok(Some(status)) => {
                self.state.workspace.git_additions =
                    u32::try_from(status.worktree_additions).unwrap_or(u32::MAX);
                self.state.workspace.git_deletions =
                    u32::try_from(status.worktree_deletions).unwrap_or(u32::MAX);
                if let Some(home) = self.home.as_mut() {
                    if home.branch.as_deref() != status.branch.as_deref() {
                        home.branch = status.branch;
                    }
                }
            }
            Ok(None) | Err(_) => {
                self.state.workspace.git_additions = 0;
                self.state.workspace.git_deletions = 0;
            }
        }
    }

    fn set_composer_cursor_from_click(&mut self, column: u16, row: u16) {
        let byte = self.composer_byte_from_click(column, row);
        self.state.composer.set_cursor_byte(byte);
        self.state.composer.refresh_assist_public();
    }

    fn composer_field_width(&self) -> usize {
        self.composer_layout_metrics().0
    }

    /// Content width and visible rows inside the composer surface (post-border).
    fn composer_layout_metrics(&self) -> (usize, usize) {
        if let Some(field) = self.last_layout.as_ref().and_then(|layout| {
            layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
        }) {
            let width = usize::from(field.width.saturating_sub(2).max(1));
            let visible = usize::from(field.height.saturating_sub(2).max(1));
            return (width, visible);
        }
        crate::tui_v2::app::state::ComposerUiState::layout_metrics(
            self.state.viewport.0,
            self.state.viewport.1,
        )
    }

    /// Content rows and visible rows for the plan dock (for wheel scroll clamp).
    fn plan_dock_scroll_metrics(&self) -> (u16, u16) {
        let Some(area) = self.last_layout.as_ref().and_then(|layout| {
            layout
                .region(crate::tui_v2::layout::snapshot::LayoutRegionId::PlanDock)
                .or_else(|| {
                    layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::Inspector)
                })
        }) else {
            return (0, 1);
        };
        // paint_dock_panel Full border consumes 2 rows / 2 cols.
        let inner_w = area.width.saturating_sub(2).max(1);
        let visible = area.height.saturating_sub(2).max(1);
        let theme = crate::tui_v2::presentation::theme::SemanticTheme::resolve(
            self.state.appearance.theme,
            self.state.capability.color_depth,
        );
        // If content overflows, a scrollbar column is reserved — match render.
        let provisional = crate::tui_v2::components::service_inspector::plan_content_rows(
            self.plan.as_ref(),
            self.state.capability.glyph_mode,
            theme,
            inner_w,
        );
        let width = if provisional > visible {
            inner_w.saturating_sub(1).max(1)
        } else {
            inner_w
        };
        let content = crate::tui_v2::components::service_inspector::plan_content_rows(
            self.plan.as_ref(),
            self.state.capability.glyph_mode,
            theme,
            width,
        );
        (content, visible)
    }

    fn composer_byte_from_click(&self, column: u16, row: u16) -> usize {
        let Some(field) = self.last_layout.as_ref().and_then(|layout| {
            layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
        }) else {
            return self.state.composer.cursor_byte();
        };
        // Content sits inside the surface border.
        let content_x = field.x.saturating_add(1);
        let content_y = field.y.saturating_add(1);
        if column < content_x || row < content_y {
            return self.state.composer.cursor_byte();
        }
        let rel_col = usize::from(column.saturating_sub(content_x));
        let rel_row = usize::from(row.saturating_sub(content_y));
        let (width, visible) = self.composer_layout_metrics();
        self.state
            .composer
            .byte_from_visual(rel_col, rel_row, width, visible)
    }

    fn try_open_composer_attachment_at(&mut self, column: u16, row: u16) -> bool {
        let byte = self.composer_byte_from_click(column, row);
        let text = self.state.composer.text().to_owned();
        let Some((start, end, inner)) = bracket_ref_at_byte(&text, byte) else {
            return false;
        };
        let _ = (start, end);
        let preview = if let Some(id) = inner.strip_prefix("clipboard:") {
            if let Some((width, height, rgba)) = self.pending_clipboard_images.get(id) {
                let (width, height, rgba) = (*width, *height, rgba.clone());
                match crate::tui_support::utils::clipboard::save_clipboard_image_preview(
                    width, height, &rgba, id,
                ) {
                    Ok(path) => AttachmentPreview {
                        title: "Clipboard image".to_owned(),
                        kind_label: "Clipboard image".to_owned(),
                        detail: format!("{width}×{height} px · {} bytes", rgba.len()),
                        body: format!(
                            "Pasted image ready for the next send.\nSaved preview: {}",
                            path.display()
                        ),
                        image_path: Some(path),
                    },
                    Err(error) => AttachmentPreview {
                        title: format!("clipboard:{id}"),
                        kind_label: "Clipboard image".to_owned(),
                        detail: format!("{width}×{height} px"),
                        body: format!(
                            "Image is attached for the next send.\nPreview failed: {error}"
                        ),
                        image_path: None,
                    },
                }
            } else {
                AttachmentPreview {
                    title: format!("clipboard:{id}"),
                    kind_label: "Clipboard image".to_owned(),
                    detail: "missing".to_owned(),
                    body: "This clipboard attachment is no longer in memory.".to_owned(),
                    image_path: None,
                }
            }
        } else {
            let path = if inner.starts_with("~/") {
                dirs_next_home().map(|home| home.join(inner.trim_start_matches("~/")))
            } else {
                None
            }
            .unwrap_or_else(|| std::path::PathBuf::from(&inner));
            attachment_preview_for_path(&path, &inner)
        };
        self.load_attachment_image(&preview);
        self.state.attachment_preview = Some(preview);
        self.dispatch(UiAction::OverlayOpened(
            crate::tui_v2::model::overlay::OverlayKind::AttachmentPreview,
        ));
        true
    }

    fn ensure_graphics(&mut self) {
        if self.graphics.picker.is_none() {
            self.graphics = crate::tui_support::graphics::GraphicsContext::detect();
        }
    }

    fn load_attachment_image(&mut self, preview: &AttachmentPreview) {
        let Some(path) = preview.image_path.as_ref() else {
            self.attachment_image = None;
            self.attachment_image_key = None;
            return;
        };
        let key = path.display().to_string();
        if self.attachment_image_key.as_deref() == Some(key.as_str())
            && self.attachment_image.is_some()
        {
            return;
        }
        self.ensure_graphics();
        let Some(picker) = self.graphics.picker.as_ref() else {
            self.attachment_image = None;
            self.attachment_image_key = None;
            return;
        };
        match image::open(path) {
            Ok(img) => {
                self.attachment_image = Some(picker.new_resize_protocol(img));
                self.attachment_image_key = Some(key);
            }
            Err(_) => {
                self.attachment_image = None;
                self.attachment_image_key = None;
            }
        }
    }

    fn jump_scrollbar(&mut self, region: &crate::tui_v2::layout::snapshot::ScrollRegionId, y: u16) {
        let Some(layout) = self.last_layout.as_ref() else {
            return;
        };
        match region {
            crate::tui_v2::layout::snapshot::ScrollRegionId::Transcript => {
                let Some(area) = layout
                    .region(crate::tui_v2::layout::snapshot::LayoutRegionId::TranscriptScrollbar)
                else {
                    return;
                };
                let total = layout.transcript.total_height;
                let visible = u32::from(layout.transcript.viewport.height);
                let offset = crate::tui_v2::components::scrollbars::offset_from_track_y(
                    area, y, total, visible,
                );
                let maximum = total.saturating_sub(visible);
                self.state.transcript.scroll_rows = offset.min(maximum);
                self.state.transcript.follow_live = offset >= maximum;
                if self.state.transcript.follow_live {
                    self.state.transcript.unseen_parts = 0;
                }
            }
            crate::tui_v2::layout::snapshot::ScrollRegionId::Composer => {
                self.state.focus = FocusTarget::Composer;
                let Some(area) = layout
                    .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerScrollbar)
                else {
                    return;
                };
                let (width, visible) = self.composer_layout_metrics();
                let total = self.state.composer.visual_row_count(width) as u32;
                let visible_u32 = visible as u32;
                let offset = crate::tui_v2::components::scrollbars::offset_from_track_y(
                    area,
                    y,
                    total,
                    visible_u32,
                );
                self.state
                    .composer
                    .set_viewport_offset(offset as usize, width, visible);
            }
            crate::tui_v2::layout::snapshot::ScrollRegionId::ComposerAssist => {
                self.state.focus = FocusTarget::Composer;
                let Some(area) = layout
                    .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerAutocomplete)
                else {
                    return;
                };
                // Jump selection proportionally along the panel body.
                let body = ratatui::layout::Rect {
                    x: area.x,
                    y: area.y.saturating_add(1),
                    width: area.width,
                    height: area.height.saturating_sub(4).max(1),
                };
                let total = if self.state.composer.autocomplete_open {
                    slash::suggestions(self.state.composer.text()).len()
                } else if self.state.composer.file_search_open {
                    file_search::suggestions(
                        &self.project_entries,
                        self.state.composer.text(),
                        self.state.composer.cursor_byte(),
                    )
                    .len()
                } else {
                    0
                };
                if total == 0 {
                    return;
                }
                let offset = crate::tui_v2::components::scrollbars::offset_from_track_y(
                    body,
                    y,
                    total as u32,
                    u32::from(body.height.max(1)),
                ) as usize;
                let index = offset.min(total.saturating_sub(1));
                if self.state.composer.autocomplete_open {
                    self.state.composer.autocomplete_selected = index;
                } else if self.state.composer.file_search_open {
                    self.state.composer.file_search_selected = index;
                }
            }
            crate::tui_v2::layout::snapshot::ScrollRegionId::Artifact(part_id) => {
                let part_id = part_id.clone();
                let Some(area) = layout
                    .region(crate::tui_v2::layout::snapshot::LayoutRegionId::FullScreenArtifact)
                else {
                    return;
                };
                let total = self
                    .state
                    .artifacts
                    .get(&part_id)
                    .map(|artifact| artifact.inner_scroll.saturating_add(u32::from(area.height)))
                    .unwrap_or(u32::from(area.height));
                let offset = crate::tui_v2::components::scrollbars::offset_from_track_y(
                    area,
                    y,
                    total.max(u32::from(area.height)),
                    u32::from(area.height),
                );
                if let Some(artifact) = self.state.artifacts.get_mut(&part_id) {
                    artifact.inner_scroll = offset;
                    artifact.follow_live = false;
                }
            }
            _ => {}
        }
    }

    fn scroll_overlay_picker(&mut self, direction: ScrollDirection) {
        if self.state.overlay.is_none() {
            return;
        }
        let delta: isize = match direction {
            ScrollDirection::Backward | ScrollDirection::Start => -1,
            ScrollDirection::Forward | ScrollDirection::End => 1,
        };
        self.state.picker.selected = self.state.picker.selected.saturating_add_signed(delta);
    }

    fn selected_transcript_text(&self) -> Option<String> {
        let selection = self.state.mouse.selection.as_ref()?;
        if selection.is_empty_range() {
            return None;
        }
        let start = &selection.start;
        let end = &selection.end;

        // Prefer measured rows — their source offsets match selection hit-testing
        // (including agent markdown, which is *not* raw measurement_text).
        if let Some(measured) = self.last_measured.as_ref() {
            if start.part_id == end.part_id {
                let part = measured
                    .iter()
                    .find(|part| part.key.part_id == start.part_id)?;
                return Some(slice_measured_inclusive(
                    part,
                    start.source_offset,
                    end.source_offset,
                ))
                .filter(|text| !text.is_empty());
            }

            let start_idx = measured
                .iter()
                .position(|part| part.key.part_id == start.part_id)?;
            let end_idx = measured
                .iter()
                .position(|part| part.key.part_id == end.part_id)?;
            let (lo_idx, hi_idx, lo_off, hi_off) = if start_idx <= end_idx {
                (start_idx, end_idx, start.source_offset, end.source_offset)
            } else {
                (end_idx, start_idx, end.source_offset, start.source_offset)
            };
            let mut out = String::new();
            for (index, part) in measured.iter().enumerate() {
                if index < lo_idx || index > hi_idx {
                    continue;
                }
                if !out.is_empty() {
                    out.push('\n');
                }
                if index == lo_idx && index == hi_idx {
                    out.push_str(&slice_measured_inclusive(part, lo_off, hi_off));
                } else if index == lo_idx {
                    out.push_str(&slice_measured_from(part, lo_off));
                } else if index == hi_idx {
                    out.push_str(&slice_measured_until_inclusive(part, hi_off));
                } else {
                    out.push_str(&measured_plain_text(part));
                }
            }
            return (!out.is_empty()).then_some(out);
        }

        // Fallback when measurements aren't cached yet (rare — pre-first paint).
        let display = self.last_display.clone().unwrap_or_else(|| {
            ConversationDisplayList::build(
                self.conversation.presentation(),
                &self.state.artifacts,
                self.state.viewport.1.max(16),
            )
        });
        if start.part_id == end.part_id {
            let part = display.parts.iter().find(|part| part.id == start.part_id)?;
            return Some(slice_inclusive(
                &part.measurement_text,
                start.source_offset,
                end.source_offset,
            ))
            .filter(|text| !text.is_empty());
        }
        let start_idx = display
            .parts
            .iter()
            .position(|part| part.id == start.part_id)?;
        let end_idx = display
            .parts
            .iter()
            .position(|part| part.id == end.part_id)?;
        let (lo_idx, hi_idx, lo_off, hi_off) = if start_idx <= end_idx {
            (start_idx, end_idx, start.source_offset, end.source_offset)
        } else {
            (end_idx, start_idx, end.source_offset, start.source_offset)
        };
        let mut out = String::new();
        for (index, part) in display.parts.iter().enumerate() {
            if index < lo_idx || index > hi_idx {
                continue;
            }
            if !out.is_empty() {
                out.push('\n');
            }
            if index == lo_idx && index == hi_idx {
                out.push_str(&slice_inclusive(&part.measurement_text, lo_off, hi_off));
            } else if index == lo_idx {
                let start = part.measurement_text.floor_char_boundary(lo_off);
                out.push_str(&part.measurement_text[start..]);
            } else if index == hi_idx {
                let end = inclusive_end_boundary(&part.measurement_text, hi_off);
                out.push_str(&part.measurement_text[..end]);
            } else {
                out.push_str(&part.measurement_text);
            }
        }
        (!out.is_empty()).then_some(out)
    }

    fn dispatch(&mut self, action: UiAction) {
        self.preserve_trigger_row(&action);
        let effects = reduce(&mut self.state, action);
        self.apply_effects(effects);
    }

    fn preserve_trigger_row(&mut self, action: &UiAction) {
        let part_id = match action {
            UiAction::ArtifactActivated(part_id)
            | UiAction::ArtifactToggled(part_id)
            | UiAction::ArtifactFullscreenChanged { part_id, .. } => Some(part_id),
            UiAction::Invoke(ActionId::ActivateFocused | ActionId::ToggleFullscreen) => {
                self.state.transcript.selected_part.as_ref()
            }
            _ => None,
        };
        let Some((transcript, part)) = self.last_layout.as_ref().and_then(|layout| {
            layout
                .transcript
                .parts
                .iter()
                .find(|part| Some(&part.part_id) == part_id)
                .map(|part| (&layout.transcript, part))
        }) else {
            return;
        };
        self.state.transcript.pending_anchor =
            Some(crate::tui_v2::layout::anchor::TranscriptAnchor::new(
                part.part_id.clone(),
                part.source_rows.start,
                part.visible_rect.y.saturating_sub(transcript.viewport.y),
            ));
        self.state.transcript.follow_live = false;
    }

    fn apply_effects(&mut self, effects: Vec<UiEffect>) {
        for effect in effects {
            match effect {
                UiEffect::SubmitComposer => {
                    let text = self.state.composer.take_text();
                    if text.trim().is_empty() {
                        continue;
                    }
                    self.submit_composer(text);
                }
                UiEffect::InsertComposerNewline => self.state.composer.insert("\n"),
                UiEffect::Scroll {
                    target: ScrollTarget::Transcript,
                    direction,
                    amount,
                } => {
                    let rows = match amount {
                        ScrollAmount::Line => 1,
                        ScrollAmount::Page => self.last_layout.as_ref().map_or(1, |layout| {
                            u32::from(layout.transcript.viewport.height.max(1))
                        }),
                        ScrollAmount::Edge => u32::MAX,
                    };
                    self.scroll_transcript(direction, rows);
                }
                UiEffect::Scroll {
                    target: ScrollTarget::FocusedArtifact,
                    direction,
                    amount,
                } => {
                    let Some(part_id) = self.state.transcript.selected_part.clone() else {
                        continue;
                    };
                    let rows = match amount {
                        ScrollAmount::Line => 1,
                        ScrollAmount::Page => self.last_layout.as_ref().map_or(1, |layout| {
                            u32::from(
                                layout
                                    .region(crate::tui_v2::layout::snapshot::LayoutRegionId::FullScreenArtifact)
                                    .map_or(1, |area| area.height.saturating_sub(2).max(1)),
                            )
                        }),
                        ScrollAmount::Edge => u32::MAX,
                    };
                    self.scroll_artifact(&part_id, direction, rows);
                }
                UiEffect::ResolveDecision { target, action } => {
                    self.resolve_decision(target, action);
                }
                UiEffect::PrepareOverlay { id, kind } => {
                    if matches!(kind, OverlayKind::SessionPicker) {
                        if let Some(runtime) = &self.runtime {
                            self.sessions = runtime.session_snapshot();
                        }
                    }
                    if matches!(kind, OverlayKind::Processes) {
                        if let Some(runtime) = &self.runtime {
                            self.processes = runtime.process_snapshot();
                        }
                    }
                    if matches!(kind, OverlayKind::PlanGoal) {
                        if let Some(runtime) = &self.runtime {
                            self.plan = runtime.plan_snapshot();
                        }
                    }
                    let _ = reduce(
                        &mut self.state,
                        UiAction::OverlayPhaseChanged {
                            id,
                            phase: crate::tui_v2::model::overlay::OverlayPhase::Ready,
                        },
                    );
                }
                UiEffect::PersistPreference(preference) => {
                    let result = self
                        .runtime
                        .as_ref()
                        .map_or(Ok(()), |runtime| match preference {
                            PersistedUiPreference::Motion(motion) => runtime.persist_motion(motion),
                            PersistedUiPreference::Theme(theme) => runtime.persist_theme(theme),
                        });
                    if let Err(error) = result {
                        self.conversation.apply_event(LoopEvent::Error {
                            error: format!("Could not save appearance preference: {error}"),
                        });
                    }
                }
                UiEffect::MoveInteractiveFocus(direction) => {
                    self.move_interactive_focus(direction);
                }
                UiEffect::CopyFocused => self.copy_focused(),
                UiEffect::InterruptAgentRun => {
                    self.send_loop_input(LoopInput::Cancel);
                }
                UiEffect::ToggleCanonicalWorkMode => {
                    if let Some(runtime) = &mut self.runtime {
                        let mode = runtime.toggle_work_mode();
                        self.conversation.apply_event(LoopEvent::ModeChange {
                            mode: mode.to_string(),
                            reason: Some("user".to_owned()),
                        });
                    }
                }
                UiEffect::CycleCanonicalReasoning => {
                    if let Some(runtime) = &mut self.runtime {
                        self.controls = runtime.cycle_reasoning();
                    }
                }
                UiEffect::ToggleCanonicalFastMode => {
                    if let Some(runtime) = &mut self.runtime {
                        self.controls = runtime.toggle_fast_mode();
                    }
                }
                UiEffect::ToggleCanonicalPermissionMode => {
                    if let Some(runtime) = &mut self.runtime {
                        self.controls = runtime.toggle_permission_mode();
                    }
                }
            }
        }
    }

    fn submit_composer(&mut self, text: String) {
        match slash::parse(&text) {
            SlashInput::NotCommand => {}
            SlashInput::Known { command, arguments } => {
                self.execute_slash_command(command, arguments);
                return;
            }
            SlashInput::Unknown { name, arguments } => {
                self.start_extension_command(name, arguments);
                return;
            }
        }
        self.submit_prompt(text, None);
    }

    fn execute_slash_command(&mut self, command: SlashCommand, arguments: &str) {
        let action = match command {
            SlashCommand::Sessions => Some(ActionId::OpenSessionPicker),
            SlashCommand::Model => Some(ActionId::OpenModelPicker),
            SlashCommand::Fast => Some(ActionId::ToggleFastMode),
            SlashCommand::Connections => Some(ActionId::OpenConnections),
            SlashCommand::Appearance => Some(ActionId::OpenThemeAppearance),
            SlashCommand::Help => Some(ActionId::OpenHelp),
            SlashCommand::Processes => Some(ActionId::OpenProcesses),
            SlashCommand::Extensions => Some(ActionId::OpenExtensions),
            SlashCommand::PlanGoal => Some(ActionId::OpenPlanGoal),
            SlashCommand::Permissions => Some(ActionId::TogglePermissionMode),
            SlashCommand::NewConversation => {
                // /home, /new, and /clear share this path.
                self.begin_fresh_home_conversation();
                None
            }
            SlashCommand::InitializeProject => {
                let scope = if arguments.is_empty() {
                    "the current repository"
                } else {
                    arguments
                };
                self.submit_prompt(
                    format!(
                        "Analyze {scope} and create or improve KRAB.md with concise architecture, \
                         conventions, key files, and verified build and test commands."
                    ),
                    Some("/init".to_owned()),
                );
                None
            }
            SlashCommand::Compact => {
                self.start_manual_compaction(arguments);
                None
            }
            SlashCommand::Update => Some(ActionId::ApplyUpdate),
        };
        if let Some(action) = action {
            self.dispatch(UiAction::Invoke(action));
        }
    }

    /// Leave the current session draft and land on Home ready for a new chat.
    /// Used by `/home`, `/new`, and `/clear`.
    fn begin_fresh_home_conversation(&mut self) {
        if matches!(self.state.agent_run, state::AgentRunState::Running) {
            self.conversation.apply_event(LoopEvent::Error {
                error: "Interrupt the active response before starting a new conversation."
                    .to_owned(),
            });
            return;
        }
        if let Some(runtime) = &mut self.runtime {
            runtime.begin_new_conversation();
        }
        self.conversation = ConversationProjection::new("new-conversation");
        self.plan = None;
        self.state.workspace = Default::default();
        self.state.dock.plan_scroll = 0;
        self.next_message_id = 1;
        self.loop_events = None;
        self.loop_input = None;
        self.state.decision_dock = Default::default();
        self.state.composer.take_text();
        self.state.mouse.clear_selection();
        let _ = reduce(
            &mut self.state,
            UiAction::AgentRunChanged(state::AgentRunState::Idle),
        );
        self.dispatch(UiAction::RouteChanged(AppRoute::Home));
    }

    fn start_manual_compaction(&mut self, preservation_hints: &str) {
        if matches!(self.state.agent_run, state::AgentRunState::Running) {
            self.conversation.apply_event(LoopEvent::Error {
                error: "Wait for the active response to finish before compacting.".to_owned(),
            });
            return;
        }
        if self.compaction.is_some() {
            self.conversation.apply_event(LoopEvent::Error {
                error: "Conversation compaction is already running.".to_owned(),
            });
            return;
        }
        let hints = (!preservation_hints.is_empty()).then(|| preservation_hints.to_owned());
        let result = self
            .runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
            .and_then(|runtime| runtime.begin_compaction(hints));
        match result {
            Ok(receiver) => {
                self.compaction = Some(receiver);
                self.state.appearance.motion.set_active_regions(1);
                self.conversation.apply_event(LoopEvent::ToolExecuting {
                    id: "manual-compaction".to_owned(),
                    name: "compaction".to_owned(),
                });
            }
            Err(error) => self.conversation.apply_event(LoopEvent::Error {
                error: error.to_string(),
            }),
        }
    }

    fn finish_manual_compaction(&mut self, result: Result<(), String>) {
        self.state
            .appearance
            .motion
            .set_active_regions(u8::from(matches!(
                self.state.agent_run,
                state::AgentRunState::Running
            )));
        match result {
            Ok(()) => {
                let loaded = self
                    .runtime
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("Runtime services are unavailable."))
                    .and_then(RuntimeServices::reload_current_session);
                match loaded {
                    Ok(loaded) => self.apply_loaded_session(loaded),
                    Err(error) => self.conversation.apply_event(LoopEvent::Error {
                        error: format!("Conversation compacted, but reload failed: {error}"),
                    }),
                }
            }
            Err(error) => self.conversation.apply_event(LoopEvent::ToolResult {
                id: "manual-compaction".to_owned(),
                output: format!("Compaction failed: {error}"),
                is_error: true,
            }),
        }
    }

    fn start_extension_command(&mut self, name: &str, arguments: &str) {
        if self.extension_command.is_some() {
            self.conversation.apply_event(LoopEvent::Error {
                error: "Wait for the active extension command to finish.".to_owned(),
            });
            return;
        }
        let command = name.trim_start_matches('/').to_owned();
        let result = self
            .runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Unknown command: {name}"))
            .and_then(|runtime| {
                runtime.begin_extension_command(command.clone(), arguments.to_owned())
            });
        match result {
            Ok(receiver) => {
                let id = format!("extension-command-{}", self.next_message_id);
                self.next_message_id = self.next_message_id.saturating_add(1);
                self.conversation.apply_event(LoopEvent::ToolExecuting {
                    id: id.clone(),
                    name: format!("extension /{command}"),
                });
                self.extension_command = Some((id, receiver));
                self.state.appearance.motion.set_active_regions(1);
            }
            Err(error) => self.conversation.apply_event(LoopEvent::Error {
                error: format!("{error}. Open /cmd for available commands."),
            }),
        }
    }

    fn finish_extension_command(&mut self, id: String, result: Result<String, String>) {
        self.state
            .appearance
            .motion
            .set_active_regions(u8::from(matches!(
                self.state.agent_run,
                state::AgentRunState::Running
            )));
        let (output, is_error) = match result {
            Ok(output) => (output, false),
            Err(error) => (format!("Extension command failed: {error}"), true),
        };
        self.conversation.apply_event(LoopEvent::ToolResult {
            id,
            output,
            is_error,
        });
    }

    fn submit_prompt(&mut self, text: String, display_override: Option<String>) {
        if self.runtime.is_some() {
            let message_id = format!("local-{}", self.next_message_id);
            let prepared = self.runtime.as_ref().expect("checked above").prepare_input(
                &text,
                &message_id,
                &mut self.pending_clipboard_images,
            );
            let PreparedInput {
                content,
                display_text,
                attachments,
                consumed_clipboard_ids,
            } = match prepared {
                Ok(prepared) => prepared,
                Err(error) => {
                    self.state.composer.insert(&text);
                    self.conversation.apply_event(LoopEvent::Error {
                        error: error.to_string(),
                    });
                    return;
                }
            };

            if matches!(self.state.agent_run, state::AgentRunState::Running) {
                let pending_id = format!("tui-v2-{message_id}");
                let sent = self.send_loop_input(LoopInput::Steer {
                    pending_id: Some(pending_id.clone()),
                    content,
                });
                if sent {
                    self.conversation.push_user_prompt(
                        &format!("steering:{pending_id}"),
                        display_override.unwrap_or(display_text),
                        attachments,
                        true,
                    );
                    self.next_message_id = self.next_message_id.saturating_add(1);
                    for id in consumed_clipboard_ids {
                        self.pending_clipboard_images.remove(&id);
                    }
                } else {
                    self.state.composer.insert(&text);
                }
                return;
            }

            match self
                .runtime
                .as_mut()
                .expect("checked above")
                .start_run(&text, content)
            {
                Ok(started) => {
                    if self.conversation.presentation().metadata.session_id != started.session_id {
                        self.conversation = ConversationProjection::new(&started.session_id);
                    }
                    self.conversation.set_title(Some(started.title));
                    self.next_message_id = self.next_message_id.saturating_add(1);
                    self.conversation.push_user_prompt(
                        &message_id,
                        display_override.unwrap_or(display_text),
                        attachments,
                        false,
                    );
                    for id in consumed_clipboard_ids {
                        self.pending_clipboard_images.remove(&id);
                    }
                    let _ = reduce(
                        &mut self.state,
                        UiAction::RouteChanged(AppRoute::Conversation {
                            session_id: SessionId::from_canonical(&started.session_id),
                        }),
                    );
                    self.loop_events = Some(started.events);
                    self.loop_input = Some(started.input);
                    self.attach_delegated_progress(started.delegated_progress);
                    let _ = reduce(
                        &mut self.state,
                        UiAction::AgentRunChanged(state::AgentRunState::Running),
                    );
                }
                Err(error) => {
                    self.state.composer.insert(&text);
                    self.conversation.apply_event(LoopEvent::Error {
                        error: error.to_string(),
                    });
                    if matches!(self.state.route, AppRoute::Home) {
                        let _ = reduce(
                            &mut self.state,
                            UiAction::RouteChanged(AppRoute::Conversation {
                                session_id: SessionId::from_canonical("configuration-error"),
                            }),
                        );
                    }
                }
            }
            return;
        }

        if matches!(self.state.route, AppRoute::Home) {
            let _ = reduce(
                &mut self.state,
                UiAction::RouteChanged(AppRoute::Conversation {
                    session_id: SessionId::from_canonical("preview-session"),
                }),
            );
        }
        let message_id = format!("preview-{}", self.next_message_id);
        self.next_message_id = self.next_message_id.saturating_add(1);
        self.conversation.push_user_prompt(
            &message_id,
            display_override.unwrap_or(text),
            Vec::new(),
            false,
        );
    }

    fn resolve_decision(&mut self, target: DecisionTarget, action: DecisionAction) {
        // Questions: any non-deny action confirms the focused option. Works in
        // preview and live. Runs before Inspect early-out — ActivateDecision
        // defaults focused_action to Inspect.
        if target.kind == DecisionTargetKind::Questions {
            if action == DecisionAction::Deny {
                return;
            }
            let Some(pending) = self
                .conversation
                .presentation()
                .pending_interactions
                .first()
                .and_then(|pending| match pending {
                    PendingInteraction::Questions(value) => Some(value.clone()),
                    _ => None,
                })
            else {
                return;
            };
            self.confirm_question_answer(&pending);
            return;
        }
        if self.runtime.is_none() {
            self.resolve_preview_decision(target, action);
            return;
        }
        if action == DecisionAction::Inspect {
            self.resolve_preview_decision(target, action);
            return;
        }
        match target.kind {
            DecisionTargetKind::ToolApproval => {
                self.send_loop_input(LoopInput::ToolApproval {
                    tool_call_id: target.tool_call_id,
                    approved: action == DecisionAction::Approve,
                });
            }
            DecisionTargetKind::PlanConfirmation => match action {
                DecisionAction::Approve => {
                    self.continue_pending_interaction(&target.tool_call_id, "execute")
                }
                DecisionAction::Deny => {
                    self.continue_pending_interaction(&target.tool_call_id, "abandon")
                }
                DecisionAction::Inspect => {}
            },
            DecisionTargetKind::Questions => {}
        }
    }

    fn send_loop_input(&mut self, input: LoopInput) -> bool {
        let sent = self
            .loop_input
            .as_ref()
            .is_some_and(|sender| sender.send(input).is_ok());
        if !sent {
            self.conversation.apply_event(LoopEvent::Error {
                error: "The active agent run is no longer accepting input.".to_owned(),
            });
        }
        sent
    }

    /// Apply a bounded burst before rendering and returning to terminal-first
    /// selection. The time limit is checked between handlers, not within one.
    fn handle_loop_event_batch(
        &mut self,
        first: LoopEvent,
        mut before_event: impl FnMut(),
    ) -> usize {
        let started = Instant::now();
        let mut event = first;
        let mut processed = 0;
        loop {
            let boundary = matches!(
                &event,
                LoopEvent::Finished { .. }
                    | LoopEvent::Error { .. }
                    | LoopEvent::AwaitingInput { .. }
            );
            before_event();
            self.handle_loop_event(event);
            processed += 1;
            // Some tool calls create an interaction before AwaitingInput arrives.
            // Publish that state before consuming any following loop event.
            if boundary
                || !self
                    .conversation
                    .presentation()
                    .pending_interactions
                    .is_empty()
                || processed >= LOOP_BATCH_MAX_EVENTS
                || started.elapsed() >= LOOP_BATCH_MAX_DURATION
            {
                break;
            }
            let Some(next) = self
                .loop_events
                .as_mut()
                .and_then(|receiver| receiver.try_recv().ok())
            else {
                // Leave disconnection cleanup to the main loop's Loop(None).
                break;
            };
            event = next;
        }
        processed
    }

    fn handle_loop_event(&mut self, event: LoopEvent) {
        let prior_parts = part_count(self.conversation.presentation());
        let updates_visible_content = matches!(
            &event,
            LoopEvent::TextDelta { .. }
                | LoopEvent::TextDeltaWithCitations { .. }
                | LoopEvent::ThinkingDelta { .. }
                | LoopEvent::ThinkingComplete { .. }
                | LoopEvent::ToolOutputDelta { .. }
                | LoopEvent::WebSearchResults { .. }
                | LoopEvent::WebFetchResult { .. }
        );
        let had_pending = !self
            .conversation
            .presentation()
            .pending_interactions
            .is_empty();
        // workflow_propose / workflow_update emit WorkflowUpdated (not PlanUpdate).
        // Refresh the dock from durable storage on all plan/goal lifecycle signals.
        let refresh_plan = matches!(
            &event,
            LoopEvent::PlanUpdate { .. }
                | LoopEvent::PlanComplete { .. }
                | LoopEvent::WorkflowUpdated { .. }
                | LoopEvent::Finished { .. }
                | LoopEvent::ModeChange { .. }
        );
        let finished = matches!(event, LoopEvent::Finished { .. });
        self.conversation.apply_event(event);
        if refresh_plan {
            if let Some(runtime) = &self.runtime {
                self.plan = runtime.plan_snapshot();
            }
        }
        let next_parts = part_count(self.conversation.presentation());
        if !self.state.transcript.follow_live
            && (next_parts > prior_parts || updates_visible_content)
        {
            self.state.transcript.unseen_parts = self
                .state
                .transcript
                .unseen_parts
                .saturating_add((next_parts - prior_parts).max(1));
        }
        let has_pending = !self
            .conversation
            .presentation()
            .pending_interactions
            .is_empty();
        if !had_pending && has_pending {
            self.state.decision_dock = Default::default();
            self.state.focus = crate::tui_v2::model::focus::FocusTarget::DecisionDock;
        } else if had_pending
            && !has_pending
            && matches!(
                self.state.focus,
                crate::tui_v2::model::focus::FocusTarget::DecisionDock
            )
        {
            self.state.focus = crate::tui_v2::model::focus::FocusTarget::Composer;
        }
        if finished {
            self.loop_input = None;
            let _ = reduce(
                &mut self.state,
                UiAction::AgentRunChanged(state::AgentRunState::Idle),
            );
        }
    }

    fn handle_delegated_progress(&mut self, event: DelegatedProgressEvent) {
        if event.parent_session_id != self.conversation.presentation().metadata.session_id {
            return;
        }
        self.conversation.apply_delegated_progress(&event);
        // Keep spinners / edge alive while children report.
        if matches!(self.state.agent_run, state::AgentRunState::Running) {
            self.state.appearance.motion.set_active_regions(1);
        }
        // If the parent agent row is expanded, follow the live tail like bash.
        let part_id = self
            .conversation
            .presentation()
            .turns
            .iter()
            .flat_map(|turn| turn.parts.iter())
            .find_map(|part| match part {
                crate::tui_v2::model::conversation::TimelinePart::Tool(tool)
                    if tool.tool_call_id == format!("delegated:{}", event.delegated_run_id)
                        || tool.tool_call_id == event.tool_call_id
                        || tool.tool_call_id == format!("delegated:{}", event.tool_call_id) =>
                {
                    Some(tool.id.clone())
                }
                _ => None,
            });
        if let Some(part_id) = part_id {
            if let Some(artifact) = self.state.artifacts.get_mut(&part_id) {
                if artifact.expanded && artifact.follow_live {
                    // Force follow-live refresh on next layout measure.
                    artifact.follow_live = true;
                }
            }
        }
    }

    fn attach_delegated_progress(
        &mut self,
        receiver: mpsc::UnboundedReceiver<DelegatedProgressEvent>,
    ) {
        let stream_id = self.next_delegated_progress_id;
        self.next_delegated_progress_id = self.next_delegated_progress_id.saturating_add(1);
        self.delegated_progress
            .insert(stream_id, UnboundedReceiverStream::new(receiver));
    }

    async fn handle_oauth_update(&mut self, update: crate::tui_support::utils::OAuthStatusUpdate) {
        if let Some(device) = update.device_code {
            self.state.setup.oauth_message = Some(update.message);
            self.state.setup.oauth_url = Some(device.verification_uri);
            self.state.setup.device_code = Some(device.user_code);
            self.state.setup.error = None;
            return;
        }
        if !update.success {
            self.state.setup.error = Some(update.message);
            self.state.setup.step = SetupStep::AuthMethod;
            return;
        }
        let Some(token) = update.token else {
            self.state.setup.oauth_message = Some(update.message);
            return;
        };
        let result = match &mut self.runtime {
            Some(runtime) => runtime.complete_oauth(update.provider, token),
            None => Err(anyhow::anyhow!("Runtime services are unavailable.")),
        };
        match result {
            Ok(loading) => {
                if let Some(setup) = &mut self.setup {
                    if let Some((index, provider)) = setup
                        .providers
                        .iter_mut()
                        .enumerate()
                        .find(|(_, provider)| provider.id == update.provider)
                    {
                        provider.connected = true;
                        self.state.setup.provider_index = index;
                    }
                }
                self.state.setup.step = if loading {
                    SetupStep::CatalogLoading
                } else {
                    SetupStep::Model
                };
                self.state.setup.model_index = 0;
                self.state.setup.oauth_message = Some(if loading {
                    "Connected securely. Loading models for this account…".to_owned()
                } else {
                    "Connected securely.".to_owned()
                });
                self.state.setup.oauth_url = None;
                self.state.setup.device_code = None;
                self.state.setup.error = None;
            }
            Err(error) => {
                self.state.setup.error = Some(error.to_string());
                self.state.setup.step = SetupStep::AuthMethod;
            }
        }
    }

    async fn handle_setup_service_update(&mut self, update: SetupServiceUpdate) {
        let SetupServiceUpdate::CatalogRefresh { provider, result } = update;
        let selected_provider = self.setup.as_ref().and_then(|setup| {
            setup
                .providers
                .get(self.state.setup.provider_index)
                .map(|provider| provider.id)
        });
        let error = match result {
            Ok(models) => match &mut self.runtime {
                Some(runtime) => runtime
                    .apply_catalog_refresh(provider, models)
                    .await
                    .err()
                    .map(|error| error.to_string()),
                None => Some("Runtime services are unavailable.".to_owned()),
            },
            Err(error) => Some(format!(
                "Connected, but the live model catalog could not refresh: {error}"
            )),
        };
        if let Some(runtime) = &self.runtime {
            self.setup = Some(runtime.setup_snapshot().await);
        }
        if let Some(setup) = &self.setup {
            if let Some(index) = setup
                .providers
                .iter()
                .position(|candidate| candidate.id == provider)
            {
                self.state.setup.provider_index = index;
            }
        }
        if selected_provider == Some(provider) && self.state.setup.step == SetupStep::CatalogLoading
        {
            self.state.setup.step = SetupStep::Model;
            self.state.setup.model_index = 0;
            self.state.setup.oauth_message = Some("Model catalog ready.".to_owned());
            self.state.setup.error = error;
        }
    }

    fn handle_update_status(&mut self, status: mitsuro_core::updater::UpdateStatus) {
        match status {
            mitsuro_core::updater::UpdateStatus::Available(info) => {
                self.state.update = Some(state::UpdateNotice {
                    current_version: info.current_version,
                    new_version: info.new_version,
                    can_apply: info.apply.can_apply(),
                    hint: if info.apply.can_apply() {
                        "Ctrl+U to install".to_owned()
                    } else {
                        info.apply.guidance()
                    },
                });
            }
            mitsuro_core::updater::UpdateStatus::UpToDate => {
                self.state.update = None;
            }
            mitsuro_core::updater::UpdateStatus::Error(error) => {
                tracing::debug!("Update check failed: {error}");
            }
            _ => {}
        }
    }

    fn resolve_preview_decision(&mut self, target: DecisionTarget, action: DecisionAction) {
        if self.conversation.presentation().metadata.session_id != target.session_id {
            return;
        }
        match action {
            DecisionAction::Approve if target.kind == DecisionTargetKind::ToolApproval => {
                self.conversation.apply_event(LoopEvent::ToolApproved {
                    id: target.tool_call_id,
                });
                self.state.focus = crate::tui_v2::model::focus::FocusTarget::Composer;
            }
            DecisionAction::Deny if target.kind == DecisionTargetKind::ToolApproval => {
                self.conversation.apply_event(LoopEvent::ToolDenied {
                    id: target.tool_call_id,
                });
                self.state.focus = crate::tui_v2::model::focus::FocusTarget::Composer;
            }
            DecisionAction::Inspect => {
                let part_id = self
                    .conversation
                    .presentation()
                    .turns
                    .iter()
                    .flat_map(|turn| &turn.parts)
                    .find_map(|part| match part {
                        TimelinePart::Tool(tool) if tool.tool_call_id == target.tool_call_id => {
                            Some(tool.id.clone())
                        }
                        _ => None,
                    });
                if let Some(part_id) = part_id {
                    let effects = reduce(
                        &mut self.state,
                        UiAction::OverlayOpened(OverlayKind::FileArtifactInspector { part_id }),
                    );
                    debug_assert!(matches!(
                        effects.as_slice(),
                        [UiEffect::PrepareOverlay { .. }]
                    ));
                }
            }
            _ => {}
        }
    }

    fn scroll_transcript(&mut self, direction: ScrollDirection, rows: u32) {
        let Some(layout) = self.last_layout.as_ref() else {
            return;
        };
        let maximum = layout
            .transcript
            .total_height
            .saturating_sub(u32::from(layout.transcript.viewport.height));
        let current = layout.transcript.scroll_top;
        let next = match direction {
            ScrollDirection::Backward => current.saturating_sub(rows),
            ScrollDirection::Forward => current.saturating_add(rows).min(maximum),
            ScrollDirection::Start => 0,
            ScrollDirection::End => maximum,
        };
        self.state.transcript.scroll_rows = next;
        self.state.transcript.follow_live = next == maximum;
        if self.state.transcript.follow_live {
            self.state.transcript.unseen_parts = 0;
        }
    }

    /// Rows from the top/bottom of a surface that arm continuous edge-scroll.
    const TRANSCRIPT_EDGE_ZONE: u16 = 2;
    const COMPOSER_EDGE_ZONE: u16 = 1;

    /// While drag-selecting the stream, scroll when the pointer sits in the edge band.
    /// Returns true if a scroll step was taken.
    fn nudge_transcript_edge_scroll_for_selection(&mut self, column: u16, row: u16) -> bool {
        let (vp_y, vp_bottom, vp_height, maximum, current) = {
            let Some(layout) = self.last_layout.as_ref() else {
                self.state.mouse.edge_scroll.clear();
                return false;
            };
            let vp = layout.transcript.viewport;
            if vp.height == 0 {
                self.state.mouse.edge_scroll.clear();
                return false;
            }
            let maximum = layout
                .transcript
                .total_height
                .saturating_sub(u32::from(vp.height));
            (
                vp.y,
                vp.bottom(),
                vp.height,
                maximum,
                layout.transcript.scroll_top,
            )
        };
        let edge = Self::TRANSCRIPT_EDGE_ZONE.min(vp_height.saturating_sub(1).max(1));
        let at_top = row <= vp_y.saturating_add(edge);
        let at_bottom = row >= vp_bottom.saturating_sub(edge.max(1));
        if at_top && current > 0 {
            self.scroll_transcript(ScrollDirection::Backward, 1);
            self.state.mouse.edge_scroll.arm(
                state::EdgeScrollDirection::Up,
                state::EdgeScrollArea::Transcript,
                column,
            );
            return true;
        }
        if at_bottom && current < maximum {
            self.scroll_transcript(ScrollDirection::Forward, 1);
            self.state.mouse.edge_scroll.arm(
                state::EdgeScrollDirection::Down,
                state::EdgeScrollArea::Transcript,
                column,
            );
            return true;
        }
        self.state.mouse.edge_scroll.clear();
        false
    }

    /// While drag-selecting in the composer, pan the input viewport at its edges.
    fn nudge_composer_edge_scroll_for_selection(&mut self, column: u16, row: u16) -> bool {
        let (field_y, field_bottom, field_height) = {
            let Some(field) = self.last_layout.as_ref().and_then(|layout| {
                layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            }) else {
                self.state.mouse.edge_scroll.clear();
                return false;
            };
            (field.y, field.bottom(), field.height)
        };
        let (width, visible) = self.composer_layout_metrics();
        let total = self.state.composer.visual_row_count(width);
        if total <= visible || field_height == 0 {
            self.state.mouse.edge_scroll.clear();
            return false;
        }
        let edge = Self::COMPOSER_EDGE_ZONE.min(field_height.saturating_sub(1).max(1));
        let at_top = row <= field_y.saturating_add(edge);
        let at_bottom = row >= field_bottom.saturating_sub(edge.max(1));
        let offset = self.state.composer.viewport_offset();
        let max_off = total.saturating_sub(visible);
        if at_top && offset > 0 {
            // Selection edge-scroll: pan without re-enabling follow_cursor.
            self.state.composer.follow_cursor = false;
            self.state.composer.scroll_viewport(-1, width, visible);
            self.state.mouse.edge_scroll.arm(
                state::EdgeScrollDirection::Up,
                state::EdgeScrollArea::Composer,
                column,
            );
            return true;
        }
        if at_bottom && offset < max_off {
            self.state.composer.follow_cursor = false;
            self.state.composer.scroll_viewport(1, width, visible);
            self.state.mouse.edge_scroll.arm(
                state::EdgeScrollDirection::Down,
                state::EdgeScrollArea::Composer,
                column,
            );
            return true;
        }
        self.state.mouse.edge_scroll.clear();
        false
    }

    /// Continuous edge-scroll while the pointer stays in the edge band (motion tick).
    fn process_selection_edge_scroll(&mut self) -> bool {
        if !self.state.mouse.edge_scroll.is_active() {
            return false;
        }
        if !self.state.mouse.selecting && !self.state.mouse.selecting_composer {
            self.state.mouse.edge_scroll.clear();
            return false;
        }
        let direction = self.state.mouse.edge_scroll.direction;
        let area = self.state.mouse.edge_scroll.area;
        let last_x = self.state.mouse.edge_scroll.last_x;
        let Some(direction) = direction else {
            return false;
        };
        match area {
            state::EdgeScrollArea::Transcript => {
                let (vp_y, vp_bottom, maximum, current) = {
                    let Some(layout) = self.last_layout.as_ref() else {
                        return false;
                    };
                    let vp = layout.transcript.viewport;
                    let maximum = layout
                        .transcript
                        .total_height
                        .saturating_sub(u32::from(vp.height));
                    (vp.y, vp.bottom(), maximum, layout.transcript.scroll_top)
                };
                let can = match direction {
                    state::EdgeScrollDirection::Up => current > 0,
                    state::EdgeScrollDirection::Down => current < maximum,
                };
                if !can {
                    self.state.mouse.edge_scroll.clear();
                    return false;
                }
                match direction {
                    state::EdgeScrollDirection::Up => {
                        self.scroll_transcript(ScrollDirection::Backward, 1);
                    }
                    state::EdgeScrollDirection::Down => {
                        self.scroll_transcript(ScrollDirection::Forward, 1);
                    }
                }
                // Extend selection to the edge row under last_x (pre-reflow best-effort).
                let y = match direction {
                    state::EdgeScrollDirection::Up => vp_y.saturating_add(1),
                    state::EdgeScrollDirection::Down => vp_bottom.saturating_sub(1),
                };
                if let Some(layout) = self.last_layout.as_ref() {
                    let pos = ratatui::layout::Position::new(last_x, y);
                    if let Some(point) = layout.transcript.selection_at_clamped(pos) {
                        self.state.mouse.drag_selection(point);
                    }
                }
                true
            }
            state::EdgeScrollArea::Composer => {
                let (width, visible) = self.composer_layout_metrics();
                let total = self.state.composer.visual_row_count(width);
                let max_off = total.saturating_sub(visible);
                let offset = self.state.composer.viewport_offset();
                let delta: isize = match direction {
                    state::EdgeScrollDirection::Up if offset > 0 => -1,
                    state::EdgeScrollDirection::Down if offset < max_off => 1,
                    _ => {
                        self.state.mouse.edge_scroll.clear();
                        return false;
                    }
                };
                self.state.composer.follow_cursor = false;
                self.state.composer.scroll_viewport(delta, width, visible);
                let field_y_bottom = self.last_layout.as_ref().and_then(|layout| {
                    layout
                        .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
                        .map(|field| (field.y, field.bottom()))
                });
                let Some((field_y, field_bottom)) = field_y_bottom else {
                    return true;
                };
                let y = match direction {
                    state::EdgeScrollDirection::Up => field_y.saturating_add(1),
                    state::EdgeScrollDirection::Down => field_bottom.saturating_sub(1),
                };
                let byte = self.composer_byte_from_click(last_x, y);
                self.state.composer.buffer.set_cursor(byte);
                self.state.mouse.drag_composer_selection(byte);
                if let Some((start, end)) = self.state.mouse.composer_selection {
                    self.state.composer.set_selection(start, end);
                }
                true
            }
        }
    }

    fn scroll_artifact(&mut self, part_id: &PartId, direction: ScrollDirection, rows: u32) {
        let viewport_height = self
            .last_layout
            .as_ref()
            .and_then(|layout| {
                let region = if self.state.overlay.as_ref().is_some_and(|overlay| {
                    matches!(
                        &overlay.kind,
                        OverlayKind::FileArtifactInspector { part_id: inspected }
                            if inspected == part_id
                    )
                }) {
                    crate::tui_v2::layout::snapshot::LayoutRegionId::Overlay
                } else {
                    crate::tui_v2::layout::snapshot::LayoutRegionId::FullScreenArtifact
                };
                layout.region(region)
            })
            .map_or(1, |area| u32::from(area.height.saturating_sub(2).max(1)));
        let display = ConversationDisplayList::build_with_materialize(
            self.conversation.presentation(),
            &self.state.artifacts,
            self.state.viewport.1,
            Some(part_id),
        );
        let total = display
            .parts
            .iter()
            .find(|part| &part.id == part_id)
            .map_or(0_u32, |part| match &part.kind {
                crate::tui_v2::presentation::transcript::DisplayPartKind::Tool(tool) => {
                    tool.artifact_lines.len().try_into().unwrap_or(u32::MAX)
                }
                crate::tui_v2::presentation::transcript::DisplayPartKind::Thinking {
                    lines,
                    ..
                } => lines.len().try_into().unwrap_or(u32::MAX),
                _ => 0,
            });
        let maximum = total.saturating_sub(viewport_height);
        let artifact = self.state.artifacts.entry(part_id.clone()).or_default();
        artifact.inner_scroll = match direction {
            ScrollDirection::Backward => artifact.inner_scroll.saturating_sub(rows),
            ScrollDirection::Forward => artifact.inner_scroll.saturating_add(rows).min(maximum),
            ScrollDirection::Start => 0,
            ScrollDirection::End => maximum,
        };
        // Terminal live-tail: follow only when parked at the end.
        artifact.follow_live = artifact.inner_scroll >= maximum;
    }

    fn copy_focused(&mut self) {
        let Some(part_id) = self.state.transcript.selected_part.as_ref() else {
            return;
        };
        // Force full body so copy keeps complete quality even if the row is collapsed.
        let display = ConversationDisplayList::build_with_materialize(
            self.conversation.presentation(),
            &self.state.artifacts,
            self.state.viewport.1,
            Some(part_id),
        );
        let Some(part) = display.parts.iter().find(|part| &part.id == part_id) else {
            return;
        };
        let text = match &part.kind {
            crate::tui_v2::presentation::transcript::DisplayPartKind::Tool(tool) => {
                if tool.artifact_lines.is_empty() {
                    format!("{} · {}", tool.label, tool.summary)
                } else {
                    tool.plain_lines().join("\n")
                }
            }
            crate::tui_v2::presentation::transcript::DisplayPartKind::Thinking {
                lines, ..
            } => lines.join("\n"),
            _ => part.measurement_text.clone(),
        };
        if !crate::tui_support::utils::clipboard::write_clipboard_text(&text) {
            self.conversation.apply_event(LoopEvent::Error {
                error: "Could not write the focused content to the clipboard.".to_owned(),
            });
        }
    }

    fn move_interactive_focus(&mut self, direction: FocusDirection) {
        let viewport_height = self
            .last_layout
            .as_ref()
            .map_or(24, |layout| layout.viewport.height);
        let display = ConversationDisplayList::build(
            self.conversation.presentation(),
            &self.state.artifacts,
            viewport_height,
        );
        let ids = display.expandable_ids();
        let Some(next) = next_interactive_id(
            &ids,
            self.state.transcript.selected_part.as_ref(),
            direction,
        ) else {
            return;
        };
        self.state.transcript.selected_part = Some(next.clone());
        self.state.transcript.pending_anchor =
            Some(crate::tui_v2::layout::anchor::TranscriptAnchor::new(
                next.clone(),
                0,
                viewport_height.saturating_sub(1) / 2,
            ));
        self.state.transcript.follow_live = false;
        self.state.focus = crate::tui_v2::model::focus::FocusTarget::Transcript { part_id: next };
    }

    fn render(&mut self, frame: &mut ratatui::Frame) {
        self.refresh_workspace_chrome();
        let theme = SemanticTheme::resolve(
            self.state.appearance.theme,
            self.state.capability.color_depth,
        );
        let inspector_requested =
            self.state.sidebar_visible && matches!(self.state.route, AppRoute::Conversation { .. });
        let wrap_width = usize::from(frame.area().width.saturating_sub(4).max(1));
        let composer_total_rows = self
            .state
            .composer
            .visual_row_count(wrap_width)
            .try_into()
            .unwrap_or(u16::MAX);
        // Field grows with content up to 4 content rows (+ borders in layout).
        let composer_content_rows = composer_total_rows.clamp(1, 4);
        let transcript_width = crate::tui_v2::layout::responsive::compose_route(
            frame.area(),
            inspector_requested,
            composer_content_rows,
        )
        .map(|geometry| {
            crate::tui_v2::layout::responsive::transcript_column_with_dock(
                geometry.primary,
                geometry.inspector.is_some(),
            )
            .width
        })
        .unwrap_or(frame.area().width.max(1));
        let inspect_part = self
            .state
            .overlay
            .as_ref()
            .and_then(|overlay| match &overlay.kind {
                crate::tui_v2::model::overlay::OverlayKind::FileArtifactInspector { part_id } => {
                    Some(part_id)
                }
                _ => None,
            });
        let display = matches!(self.state.route, AppRoute::Conversation { .. }).then(|| {
            ConversationDisplayList::build_with_materialize(
                self.conversation.presentation(),
                &self.state.artifacts,
                frame.area().height,
                inspect_part,
            )
        });
        let measured = display.as_ref().map(|display| {
            display.measure(
                &mut self.measurements,
                transcript_width,
                &self.state.artifacts,
                self.state.appearance.theme,
                self.state.capability,
            )
        });
        let expandable = display
            .as_ref()
            .map_or_else(Vec::new, |display| display.expandable_ids());
        let transcript_spacing = display
            .as_ref()
            .map_or_else(Vec::new, |display| display.spacing_before());
        let fullscreen_artifact = self
            .state
            .artifacts
            .iter()
            .find_map(|(part_id, artifact)| artifact.fullscreen.then_some(part_id));
        let requested_anchor = self.state.transcript.pending_anchor.clone();
        let pass = self.layout_engine.layout(LayoutRequest {
            viewport: frame.area(),
            route: &self.state.route,
            overlay: self.state.overlay.as_ref(),
            focus: &self.state.focus,
            inspector_requested,
            dock_plan_ratio: self.state.dock.plan_ratio(),
            plugin_focused: self.state.dock.plugin_focused
                || matches!(self.state.focus, FocusTarget::PluginDock),
            fullscreen_artifact,
            decision_dock_height: decision_dock_height(
                self.conversation
                    .presentation()
                    .pending_interactions
                    .first(),
            ),
            composer_content_rows,
            composer_total_rows,
            composer_fullscreen: self.state.composer.fullscreen,
            composer_autocomplete_rows: if self.state.composer.autocomplete_open {
                crate::tui_v2::input::slash::suggestions(self.state.composer.text())
                    .len()
                    .try_into()
                    .unwrap_or(u16::MAX)
            } else if self.state.composer.file_search_open {
                file_search::suggestions(
                    &self.project_entries,
                    self.state.composer.text(),
                    self.state.composer.cursor_byte(),
                )
                .len()
                .max(1)
                .try_into()
                .unwrap_or(u16::MAX)
            } else {
                0
            },
            transcript: measured.as_ref().map(|items| TranscriptRequest {
                items,
                spacing_before: &transcript_spacing,
                expandable: &expandable,
                anchor: if let Some(anchor) = requested_anchor.clone() {
                    AnchorMode::Fixed(anchor)
                } else if self.state.transcript.follow_live {
                    AnchorMode::FollowLive
                } else {
                    AnchorMode::ScrollTop(self.state.transcript.scroll_rows)
                },
                new_content_count: self.state.transcript.unseen_parts,
            }),
        });
        let conversation = display
            .as_ref()
            .zip(measured.as_ref())
            .map(|(display, measured)| ConversationRenderData {
                display,
                measured,
                metadata: &self.conversation.presentation().metadata,
                pending: &self.conversation.presentation().pending_interactions,
            });
        // Lock soft-wrap + viewport pan to the real composer content box so the
        // caret never paints outside the field when follow_cursor is on.
        if let Some(field) = pass
            .snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
        {
            let width = usize::from(field.width.saturating_sub(2).max(1));
            let rows = usize::from(field.height.saturating_sub(2).max(1));
            self.state.composer.sync_field_metrics(width, rows);
        }
        self.state.viewport = (frame.area().width, frame.area().height);
        render_preview(
            frame,
            &self.state,
            theme,
            &pass.snapshot,
            conversation,
            self.home.as_ref(),
            self.setup.as_ref(),
            &self.sessions,
            &self.project_entries,
            &self.processes,
            self.plan.as_ref(),
            &self.extensions,
            &self.controls,
            self.attachment_image.as_mut(),
        );
        if requested_anchor.is_some() {
            self.state.transcript.scroll_rows = pass.snapshot.transcript.scroll_top;
            self.state.transcript.pending_anchor = None;
        }
        self.last_layout = Some(pass.snapshot);
        self.last_display = display;
        self.last_measured = measured;
    }

    /// Fast path during text-selection drag: reuse last layout and measurements.
    fn render_light(&mut self, frame: &mut ratatui::Frame<'_>) {
        let Some(layout) = self.last_layout.as_ref() else {
            self.render(frame);
            return;
        };
        let theme = SemanticTheme::resolve(
            self.state.appearance.theme,
            self.state.capability.color_depth,
        );
        let conversation = self
            .last_display
            .as_ref()
            .zip(self.last_measured.as_ref())
            .map(|(display, measured)| ConversationRenderData {
                display,
                measured,
                metadata: &self.conversation.presentation().metadata,
                pending: &self.conversation.presentation().pending_interactions,
            });
        render_preview(
            frame,
            &self.state,
            theme,
            layout,
            conversation,
            self.home.as_ref(),
            self.setup.as_ref(),
            &self.sessions,
            &self.project_entries,
            &self.processes,
            self.plan.as_ref(),
            &self.extensions,
            &self.controls,
            self.attachment_image.as_mut(),
        );
    }
}

pub async fn run() -> Result<crate::tui_v2::TuiOutcome> {
    let mut app = PreviewApp::initialize().await?;
    let mut terminal = TerminalSession::enter()?;
    let mut events = EventStream::new();
    let motion_started = std::time::Instant::now();
    // Prefer the splash cadence while Home is alive; falls back fine for
    // conversation spinners (they key off elapsed_ms, not tick count).
    // Splash uses a faster cadence; agent spinners use the slower shared clock.
    // Rebuild the interval when the mode changes so we never leave a 16ms timer
    // armed after the wordmark settles (that starves input under load).
    let mut splash_motion = true;
    let mut motion_tick =
        tokio::time::interval(crate::tui_v2::motion::clock::SPLASH_FRAME_INTERVAL);
    motion_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    terminal.draw(|frame| app.render(frame))?;

    while !app.state.should_exit() {
        enum NextEvent {
            Terminal(Option<std::io::Result<Event>>),
            Loop(Option<LoopEvent>),
            Delegated(Option<Box<DelegatedProgressEvent>>),
            Auth(Option<crate::tui_support::utils::OAuthStatusUpdate>),
            Setup(Option<SetupServiceUpdate>),
            Compaction(Option<Result<(), String>>),
            ExtensionCommand(Option<Result<String, String>>),
            ExtensionToggle(Option<Result<(), String>>),
            Update(Option<mitsuro_core::updater::UpdateStatus>),
            Motion(u64),
        }
        let edge_scrolling = app.state.mouse.edge_scroll.is_active();
        let wants_motion = edge_scrolling
            || (app.state.appearance.motion.wants_tick() && app.state.overlay.is_none());
        let home_splash = matches!(app.state.route, AppRoute::Home) && !app.state.splash.settled;
        // Edge-scroll wants a snappy cadence (~60fps); splash keeps its own clock.
        let want_splash_cadence = home_splash && !edge_scrolling;
        if want_splash_cadence != splash_motion {
            splash_motion = want_splash_cadence;
            let period = if splash_motion {
                crate::tui_v2::motion::clock::SPLASH_FRAME_INTERVAL
            } else {
                crate::tui_v2::motion::clock::MOTION_FRAME_INTERVAL
            };
            motion_tick = tokio::time::interval(period);
            motion_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Consume the immediate first tick so we don't redraw twice in a row.
            motion_tick.reset();
        }
        let next = {
            let loop_events = &mut app.loop_events;
            let delegated_progress = &mut app.delegated_progress;
            let auth_events = &mut app.auth_events;
            let setup_events = &mut app.setup_events;
            let compaction = &mut app.compaction;
            let extension_command = &mut app.extension_command;
            let extension_toggle = &mut app.extension_toggle;
            let update_events = &mut app.update_events;
            // Prefer terminal input over motion so splash/spinner ticks cannot
            // monopolize the loop and make typing feel frozen.
            tokio::select! {
                biased;
                event = events.next() => NextEvent::Terminal(event),
                event = async {
                    match loop_events.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending::<Option<LoopEvent>>().await,
                    }
                } => NextEvent::Loop(event),
                event = async {
                    if delegated_progress.is_empty() {
                        std::future::pending::<Option<DelegatedProgressEvent>>().await
                    } else {
                        delegated_progress.next().await.map(|(_, event)| event)
                    }
                } => NextEvent::Delegated(event.map(Box::new)),
                event = async {
                    match auth_events.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending::<Option<crate::tui_support::utils::OAuthStatusUpdate>>().await,
                    }
                } => NextEvent::Auth(event),
                event = async {
                    match setup_events.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending::<Option<SetupServiceUpdate>>().await,
                    }
                } => NextEvent::Setup(event),
                event = async {
                    match compaction.as_mut() {
                        Some(receiver) => receiver.await.ok(),
                        None => std::future::pending::<Option<Result<(), String>>>().await,
                    }
                } => NextEvent::Compaction(event),
                event = async {
                    match extension_command.as_mut() {
                        Some((_, receiver)) => receiver.await.ok(),
                        None => std::future::pending::<Option<Result<String, String>>>().await,
                    }
                } => NextEvent::ExtensionCommand(event),
                event = async {
                    match extension_toggle.as_mut() {
                        Some(receiver) => receiver.await.ok(),
                        None => std::future::pending::<Option<Result<(), String>>>().await,
                    }
                } => NextEvent::ExtensionToggle(event),
                event = async {
                    match update_events.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending::<Option<mitsuro_core::updater::UpdateStatus>>().await,
                    }
                } => NextEvent::Update(event),
                _ = motion_tick.tick(), if wants_motion => {
                    NextEvent::Motion(motion_started.elapsed().as_millis().try_into().unwrap_or(u64::MAX))
                }
            }
        };
        let redraw = match next {
            // Never poll EventStream with now_or_never: Pending arms crossterm's
            // wake thread with a disposable waker and can leave input permanently
            // unwakeable once motion ticks stop after the splash settles.
            NextEvent::Terminal(Some(event)) => app.handle_event(event?),
            NextEvent::Terminal(None) => bail!("terminal event stream ended unexpectedly"),
            NextEvent::Loop(Some(event)) => {
                app.handle_loop_event_batch(event, || {});
                Redraw::Full
            }
            NextEvent::Loop(None) => {
                app.loop_events = None;
                app.loop_input = None;
                let _ = reduce(
                    &mut app.state,
                    UiAction::AgentRunChanged(state::AgentRunState::Idle),
                );
                Redraw::Full
            }
            NextEvent::Delegated(Some(event)) => {
                app.handle_delegated_progress(*event);
                Redraw::Full
            }
            NextEvent::Delegated(None) => Redraw::None,
            NextEvent::Auth(Some(update)) => {
                app.handle_oauth_update(update).await;
                Redraw::Full
            }
            NextEvent::Auth(None) => {
                app.auth_events = None;
                if matches!(
                    app.state.setup.step,
                    SetupStep::OAuthWaiting | SetupStep::OAuthPasteCode
                ) {
                    app.state.setup.error =
                        Some("Authentication ended before it completed.".to_owned());
                    app.state.setup.step = SetupStep::AuthMethod;
                }
                Redraw::Full
            }
            NextEvent::Setup(Some(update)) => {
                app.handle_setup_service_update(update).await;
                Redraw::Full
            }
            NextEvent::Setup(None) => {
                app.setup_events = None;
                if app.state.setup.step == SetupStep::CatalogLoading {
                    app.state.setup.error =
                        Some("The model catalog refresh ended unexpectedly.".to_owned());
                    app.state.setup.step = SetupStep::Model;
                }
                Redraw::Full
            }
            NextEvent::Compaction(Some(result)) => {
                app.compaction = None;
                app.finish_manual_compaction(result);
                Redraw::Full
            }
            NextEvent::Compaction(None) => {
                app.compaction = None;
                app.finish_manual_compaction(Err(
                    "Compaction task ended before returning a result.".to_owned(),
                ));
                Redraw::Full
            }
            NextEvent::ExtensionCommand(result) => {
                let id = app
                    .extension_command
                    .take()
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| "extension-command".to_owned());
                app.finish_extension_command(
                    id,
                    result.unwrap_or_else(|| {
                        Err("Extension command ended before returning a result.".to_owned())
                    }),
                );
                Redraw::Full
            }
            NextEvent::ExtensionToggle(result) => {
                app.extension_toggle = None;
                app.finish_extension_toggle(result.unwrap_or_else(|| {
                    Err("Extension action ended before returning a result.".to_owned())
                }))
                .await;
                Redraw::Full
            }
            NextEvent::Update(Some(status)) => {
                app.handle_update_status(status);
                Redraw::Full
            }
            NextEvent::Update(None) => {
                app.update_events = None;
                Redraw::None
            }
            NextEvent::Motion(elapsed_ms) => {
                let mut need = Redraw::None;
                if app.process_selection_edge_scroll() {
                    need = Redraw::Full;
                }
                if app.state.appearance.motion.wants_tick() {
                    let _ = reduce(&mut app.state, UiAction::MotionAdvancedTo(elapsed_ms));
                    if matches!(app.state.route, AppRoute::Home) {
                        let wall = app.state.appearance.motion.clock.elapsed_ms();
                        app.state
                            .splash
                            .mark_settled_if_ready(wall, app.state.appearance.motion.preference);
                        // Stroke-in only — stop ticking once the wordmark is settled.
                        app.state
                            .appearance
                            .motion
                            .set_active_regions(u8::from(!app.state.splash.settled));
                    }
                    need = Redraw::Full;
                }
                need
            }
        };
        match redraw {
            Redraw::None => {}
            Redraw::Light => {
                terminal.draw(|frame| app.render_light(frame))?;
            }
            Redraw::Full => {
                terminal.draw(|frame| app.render(frame))?;
            }
        }
    }

    let outcome = if let Some(version) = app.state.apply_update_version() {
        crate::tui_v2::TuiOutcome::ApplyUpdate { version }
    } else {
        crate::tui_v2::TuiOutcome::Quit
    };

    if let Some(runtime) = &app.runtime {
        runtime.shutdown().await;
    }

    Ok(outcome)
}

/// Char-safe inclusive slice for selection copy.
fn slice_inclusive(text: &str, a: usize, b: usize) -> String {
    let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
    let start = text.floor_char_boundary(lo.min(text.len()));
    let end = inclusive_end_boundary(text, hi);
    if start >= end {
        String::new()
    } else {
        text[start..end].to_owned()
    }
}

fn inclusive_end_boundary(text: &str, offset: usize) -> usize {
    if text.is_empty() {
        return 0;
    }
    let offset = text.floor_char_boundary(offset.min(text.len()));
    if offset >= text.len() {
        return text.len();
    }
    // Include the character that starts at `offset`.
    text[offset..]
        .chars()
        .next()
        .map(|ch| offset + ch.len_utf8())
        .unwrap_or(text.len())
}

/// Reconstruct selectable plain text from measured rows (matches hit-test offsets).
fn measured_plain_text(part: &crate::tui_v2::layout::measure::MeasuredPart) -> String {
    let mut out = String::new();
    for (index, row) in part.rows.iter().enumerate() {
        if index > 0 {
            // measure_from_markdown_lines inserts a synthetic gap of 1 between rows.
            while out.len() < row.source_start {
                out.push('\n');
            }
        } else {
            while out.len() < row.source_start {
                out.push('\n');
            }
        }
        out.push_str(&row.text);
    }
    out
}

fn slice_measured_inclusive(
    part: &crate::tui_v2::layout::measure::MeasuredPart,
    a: usize,
    b: usize,
) -> String {
    let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
    let plain = measured_plain_text(part);
    if plain.is_empty() {
        // Empty chrome rows (user bubble borders) — fall back to joining row text.
        return part
            .rows
            .iter()
            .map(|row| row.text.as_str())
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
    }
    slice_inclusive(&plain, lo, hi)
}

fn slice_measured_from(
    part: &crate::tui_v2::layout::measure::MeasuredPart,
    start: usize,
) -> String {
    let plain = measured_plain_text(part);
    let start = plain.floor_char_boundary(start.min(plain.len()));
    plain[start..].to_owned()
}

fn slice_measured_until_inclusive(
    part: &crate::tui_v2::layout::measure::MeasuredPart,
    end: usize,
) -> String {
    let plain = measured_plain_text(part);
    let end = inclusive_end_boundary(&plain, end);
    plain[..end].to_owned()
}

#[cfg(test)]
mod measured_selection_copy_tests {
    use super::{measured_plain_text, slice_measured_inclusive};
    use crate::tui_v2::{
        layout::measure::{ExpansionMode, MeasuredPart, MeasuredRow, MeasurementKey, ThemeMetrics},
        model::{
            artifact::PartId,
            capability::{CapabilityProfile, ColorDepth, GlyphMode},
        },
        presentation::theme::ThemeKind,
    };

    fn part_with_rows(rows: Vec<MeasuredRow>) -> MeasuredPart {
        MeasuredPart {
            key: MeasurementKey {
                part_id: PartId::from_semantic("p1"),
                revision: 1,
                width: 40,
                expansion: ExpansionMode::Collapsed,
                theme_metrics: ThemeMetrics::new(ThemeKind::MitsuroDark),
                capability: CapabilityProfile {
                    glyph_mode: GlyphMode::Unicode,
                    color_depth: ColorDepth::TrueColor,
                },
            },
            rows,
            markdown: None,
            weight: 32,
        }
    }

    #[test]
    fn measured_copy_uses_rendered_row_offsets_not_raw_markdown() {
        // Mirrors measure_from_markdown_lines: row0 0..5, gap, row1 6..11
        let part = part_with_rows(vec![
            MeasuredRow {
                text: "hello".into(),
                source_start: 0,
                source_end: 5,
                column_offsets: vec![0, 1, 2, 3, 4, 5],
            },
            MeasuredRow {
                text: "world".into(),
                source_start: 6,
                source_end: 11,
                column_offsets: vec![6, 7, 8, 9, 10, 11],
            },
        ]);
        assert_eq!(measured_plain_text(&part), "hello\nworld");
        // Select "wor" on second line (offsets 6,7,8)
        assert_eq!(slice_measured_inclusive(&part, 6, 8), "wor");
        // Cross-line selection
        assert_eq!(slice_measured_inclusive(&part, 3, 8), "lo\nwor");
    }
}

fn bracket_ref_at_byte(text: &str, byte: usize) -> Option<(usize, usize, String)> {
    let byte = byte.min(text.len());
    let mut depth_start = None;
    for (idx, ch) in text.char_indices() {
        if ch == '[' {
            depth_start = Some(idx);
        } else if ch == ']' {
            if let Some(start) = depth_start {
                let end = idx + ch.len_utf8();
                if byte >= start && byte < end {
                    let inner = text[start + 1..idx].to_owned();
                    if !inner.is_empty() {
                        return Some((start, end, inner));
                    }
                }
                depth_start = None;
            }
        }
    }
    None
}

fn dirs_next_home() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(std::path::PathBuf::from)
}

fn attachment_preview_for_path(path: &std::path::Path, label: &str) -> AttachmentPreview {
    let title = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(label)
        .to_owned();
    if !path.exists() {
        return AttachmentPreview {
            title,
            kind_label: "Missing file".to_owned(),
            detail: path.display().to_string(),
            body: "This path does not exist on disk.".to_owned(),
            image_path: None,
        };
    }
    let meta = std::fs::metadata(path).ok();
    let size = meta.as_ref().map(|meta| meta.len()).unwrap_or(0);
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let kind_label = match ext.as_str() {
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" => "Image file",
        "pdf" => "PDF",
        "rs" | "ts" | "tsx" | "js" | "py" | "md" | "txt" | "toml" | "json" | "yaml" | "yml" => {
            "Text file"
        }
        _ => "File",
    }
    .to_owned();
    let body = if matches!(
        ext.as_str(),
        "rs" | "ts" | "tsx" | "js" | "py" | "md" | "txt" | "toml" | "json" | "yaml" | "yml"
    ) {
        std::fs::read_to_string(path)
            .map(|content| content.lines().take(40).collect::<Vec<_>>().join("\n"))
            .unwrap_or_else(|error| format!("Could not read file: {error}"))
    } else if matches!(
        ext.as_str(),
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp" | "pdf"
    ) {
        format!(
            "Binary {} attachment.\nOpen in an external viewer for full graphics.\n\n{}",
            kind_label.to_ascii_lowercase(),
            path.display()
        )
    } else {
        format!("Attached path:\n{}", path.display())
    };
    let image_path = matches!(
        ext.as_str(),
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp"
    )
    .then(|| path.to_path_buf());
    AttachmentPreview {
        title,
        kind_label,
        detail: format!("{} · {size} bytes", path.display()),
        body,
        image_path,
    }
}

/// Bracket file-looking tokens on paste (legacy input-bar behavior, light).
fn prepare_pasted_composer_text(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return text.to_owned();
    }
    // Single path-like token → wrap for attachment preview.
    if !trimmed.contains(|c: char| c.is_whitespace())
        && !trimmed.starts_with('[')
        && (trimmed.starts_with('/')
            || trimmed.starts_with("~/")
            || trimmed.contains('/')
                && trimmed
                    .rsplit_once('.')
                    .is_some_and(|(_, ext)| !ext.is_empty() && ext.len() <= 8))
    {
        return format!("[{trimmed}]");
    }
    text.to_owned()
}

fn composer_key_action(
    state: &UiState,
    code: KeyCode,
    modifiers: KeyModifiers,
) -> Option<UiAction> {
    if matches!(state.route, AppRoute::Setup) || !state.focus.is_composer() {
        return None;
    }
    if modifiers == KeyModifiers::CONTROL
        || modifiers == (KeyModifiers::CONTROL | KeyModifiers::SHIFT)
    {
        return match code {
            KeyCode::Char('w') | KeyCode::Char('W') => Some(UiAction::ComposerDeletePreviousWord),
            KeyCode::Char('u') | KeyCode::Char('U') => Some(UiAction::ComposerClearToLineStart),
            // Full clear of the input bar (not kill-to-start).
            KeyCode::Char('c') | KeyCode::Char('C') => Some(UiAction::ComposerClear),
            // Ctrl+V is handled before this via system clipboard paste.
            _ => None,
        };
    }
    if modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) {
        return None;
    }
    match code {
        KeyCode::Char(character) => Some(UiAction::ComposerInserted(character.to_string())),
        KeyCode::Backspace => Some(UiAction::ComposerBackspace),
        KeyCode::Left => Some(UiAction::ComposerMoveLeft),
        KeyCode::Right => Some(UiAction::ComposerMoveRight),
        KeyCode::Home => Some(UiAction::ComposerMoveLineStart),
        KeyCode::End => Some(UiAction::ComposerMoveLineEnd),
        KeyCode::Up => Some(UiAction::ComposerMoveVisualLine { forward: false }),
        KeyCode::Down => Some(UiAction::ComposerMoveVisualLine { forward: true }),
        _ => None,
    }
}

fn decision_target(pending: &PendingInteraction) -> DecisionTarget {
    match pending {
        PendingInteraction::ToolApproval(value) => DecisionTarget {
            session_id: value.session_id.clone(),
            tool_call_id: value.tool_call_id.clone(),
            kind: DecisionTargetKind::ToolApproval,
        },
        PendingInteraction::Questions(value) => DecisionTarget {
            session_id: value.session_id.clone(),
            tool_call_id: value.tool_call_id.clone(),
            kind: DecisionTargetKind::Questions,
        },
        PendingInteraction::PlanConfirm(value) => DecisionTarget {
            session_id: value.session_id.clone(),
            tool_call_id: value.tool_call_id.clone(),
            kind: DecisionTargetKind::PlanConfirmation,
        },
    }
}

fn decision_dock_height(pending: Option<&PendingInteraction>) -> u16 {
    match pending {
        Some(PendingInteraction::ToolApproval(_)) => 3,
        Some(PendingInteraction::PlanConfirm(_)) => 4,
        // Outer dock height (includes Full border ×2). Inner body is:
        // question + options + footer. No title strip.
        Some(PendingInteraction::Questions(value)) => {
            let max_options = value
                .questions
                .iter()
                .map(|question| question.options.len())
                .max()
                .unwrap_or(0);
            u16::try_from(max_options)
                .unwrap_or(u16::MAX)
                .saturating_add(4) // border×2 + question + footer
                .clamp(5, 12)
        }
        None => 0,
    }
}

fn next_interactive_id(
    ids: &[PartId],
    selected: Option<&PartId>,
    direction: FocusDirection,
) -> Option<PartId> {
    if ids.is_empty() {
        return None;
    }
    let current = selected.and_then(|selected| ids.iter().position(|id| id == selected));
    let index = match (current, direction) {
        (Some(index), FocusDirection::Previous) => {
            index.checked_sub(1).unwrap_or(ids.len().saturating_sub(1))
        }
        (Some(index), FocusDirection::Next) => (index + 1) % ids.len(),
        (None, FocusDirection::Previous) => ids.len().saturating_sub(1),
        (None, FocusDirection::Next) => 0,
    };
    ids.get(index).cloned()
}

fn part_count(
    presentation: &crate::tui_v2::model::conversation::ConversationPresentation,
) -> usize {
    presentation
        .turns
        .iter()
        .map(|turn| turn.parts.len() + usize::from(turn.user.is_some()))
        .sum()
}

fn serialize_question_answers(
    pending: &crate::tui_v2::model::conversation::PendingQuestions,
    selected_answers: &[QuestionAnswer],
) -> String {
    let answers = pending
        .questions
        .iter()
        .zip(selected_answers)
        .map(|(question, answer)| {
            let value = match answer {
                QuestionAnswer::Single(label) => serde_json::Value::String(label.clone()),
                QuestionAnswer::Multiple(labels) => serde_json::Value::Array(
                    labels
                        .iter()
                        .cloned()
                        .map(serde_json::Value::String)
                        .collect(),
                ),
            };
            (question.header.clone(), value)
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::json!({ "answers": answers }).to_string()
}

fn cycle_index(current: usize, count: usize, forward: bool) -> usize {
    if count == 0 {
        return 0;
    }
    if forward {
        (current + 1) % count
    } else {
        current.checked_sub(1).unwrap_or(count - 1)
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{
        KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    };
    use mitsuro_core::{
        agent::loop_events::LoopStopReason,
        ai::{
            models::{ApiFormat, ModelKey},
            providers::ProviderId,
        },
        storage::{
            DelegationGroupRecord, PartialAssistantState, PendingInteractionSnapshot,
            RecoveryDecision, RecoveryNonResumableReason, RecoveryStatus, SessionRecoveryState,
        },
    };
    use ratatui::{backend::TestBackend, Terminal};
    use serde_json::json;

    use super::*;
    use crate::tui_v2::{
        motion::preference::MotionPreference,
        services::{ExtensionRow, SetupModel, SetupProvider},
    };

    fn unicode_capability() -> CapabilityProfile {
        CapabilityProfile::from_environment(Some("xterm-256color"), Some("truecolor"), false, false)
    }

    #[test]
    fn quit_is_registered_and_plain_input_is_preserved() {
        let mut app = PreviewApp {
            state: UiState::preview(CapabilityProfile::from_environment(
                Some("xterm-256color"),
                Some("truecolor"),
                false,
                false,
            )),
            layout_engine: LayoutEngine::default(),
            measurements: MeasurementCache::default(),
            last_layout: None,
            last_display: None,
            last_measured: None,
            conversation: ConversationProjection::new("preview-session"),
            next_message_id: 1,
            runtime: None,
            loop_events: None,
            loop_input: None,
            delegated_progress: DelegatedProgressReceivers::new(),
            next_delegated_progress_id: 1,
            compaction: None,
            extension_command: None,
            extension_toggle: None,
            auth_events: None,
            setup_events: None,
            update_events: None,
            home: None,
            setup: None,
            sessions: Vec::new(),
            project_entries: Vec::new(),
            processes: Vec::new(),
            plan: None,
            extensions: Vec::new(),
            controls: ControlSnapshot::default(),
            pending_clipboard_images: std::collections::HashMap::new(),
            attachment_image: None,
            attachment_image_key: None,
            graphics: crate::tui_support::graphics::GraphicsContext { picker: None },
            last_git_poll: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(60))
                .unwrap_or_else(std::time::Instant::now),
        };

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('q'),
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(!app.state.should_exit());
        assert_eq!(app.state.composer.text(), "q");

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('q'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        assert!(app.state.should_exit());
    }

    #[test]
    fn ctrl_u_installs_an_available_update_and_otherwise_kills_the_composer_line() {
        let mut app = PreviewApp::preview();
        app.dispatch(UiAction::ComposerInserted("keep this".to_owned()));
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('u'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        assert_eq!(app.state.composer.text(), "");
        assert!(!matches!(
            app.state.lifecycle,
            state::AppLifecycle::ApplyUpdateRequested
        ));

        app.state.update = Some(state::UpdateNotice {
            current_version: "0.9.22".to_owned(),
            new_version: "0.9.23".to_owned(),
            can_apply: true,
            hint: "Ctrl+U to install".to_owned(),
        });
        app.dispatch(UiAction::ComposerInserted("draft".to_owned()));
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('u'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        assert_eq!(app.state.apply_update_version().as_deref(), Some("0.9.23"));
    }

    #[test]
    fn home_submit_routes_to_a_typed_conversation_and_clears_composer() {
        let mut app = PreviewApp::preview();
        assert!(app
            .handle_event(Event::Paste("Build the clean TUI.".to_owned()))
            .handled());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());

        assert!(matches!(app.state.route, AppRoute::Conversation { .. }));
        assert!(app.state.composer.text().is_empty());
        assert_eq!(
            app.conversation.presentation().turns[0]
                .user
                .as_ref()
                .expect("user prompt")
                .text,
            "Build the clean TUI."
        );
    }

    #[test]
    fn composer_supports_multiline_cursor_and_editing_contract() {
        let mut app = PreviewApp::preview();
        assert!(app
            .handle_event(Event::Paste("alpha beta".to_owned()))
            .handled());
        for _ in 0..4 {
            assert!(app
                .handle_event(Event::Key(
                    KeyEvent::new(KeyCode::Left, KeyModifiers::NONE,)
                ))
                .handled());
        }
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('w'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        assert_eq!(app.state.composer.text(), "beta");
        assert_eq!(app.state.composer.cursor_byte(), 0);

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('j'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        assert!(app
            .handle_event(Event::Paste("second".to_owned()))
            .handled());
        assert_eq!(app.state.composer.text(), "\nsecondbeta");
        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Home, KeyModifiers::NONE,)
            ))
            .handled());
        assert_eq!(app.state.composer.cursor_byte(), 1);
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('u'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        assert_eq!(app.state.composer.text(), "\nsecondbeta");
    }

    #[test]
    fn ctrl_c_clears_the_entire_composer_input() {
        let mut app = PreviewApp::preview();
        assert!(app
            .handle_event(Event::Paste("line one\nline two with more".to_owned()))
            .handled());
        assert!(!app.state.composer.text().is_empty());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('c'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        assert!(app.state.composer.text().is_empty());
        assert_eq!(app.state.composer.cursor_byte(), 0);
        assert!(!app.state.composer.autocomplete_open);
        assert!(!app.state.composer.file_search_open);
    }

    #[test]
    fn full_screen_composer_preserves_the_draft_and_restores_the_exact_buffer() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        assert!(app
            .handle_event(Event::Paste(
                "A long prompt\nwith implementation detail\nand validation notes.".to_owned(),
            ))
            .handled());
        let cursor = app.state.composer.cursor_byte();
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let before = terminal.backend().buffer().clone();

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('e'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        terminal.draw(|frame| app.render(frame)).expect("render");
        let editor = app
            .last_layout
            .as_ref()
            .and_then(|layout| {
                layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            })
            .expect("full-screen composer");
        assert!(app.state.composer.fullscreen);
        assert!(editor.height > 10);

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE,)))
            .handled());
        terminal.draw(|frame| app.render(frame)).expect("render");
        assert!(!app.state.composer.fullscreen);
        assert_eq!(app.state.composer.cursor_byte(), cursor);
        assert_eq!(terminal.backend().buffer(), &before);
    }

    #[test]
    fn home_animation_skip_preserves_the_triggering_key() {
        let mut app = PreviewApp::preview();
        app.state.appearance.motion.preference = MotionPreference::Full;
        app.state.appearance.motion.set_active_regions(1);

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))
            .handled());

        assert_eq!(app.state.composer.text(), "x");
        // Entrance is skipped; no ambient fireflies — motion stops after settle.
        assert!(app.state.splash.settled);
        assert_eq!(app.state.appearance.motion.active_regions(), 0);
        assert!(!app.state.appearance.motion.wants_tick());
    }

    #[test]
    fn agent_activity_uses_one_shared_clock_and_idle_conversation_stops_it() {
        let mut app = PreviewApp::preview();
        app.state.capability = CapabilityProfile::from_environment(
            Some("xterm-256color"),
            Some("truecolor"),
            false,
            false,
        );
        app.state.appearance.motion.preference = MotionPreference::Full;
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("motion-session"),
        };
        app.dispatch(UiAction::AgentRunChanged(state::AgentRunState::Running));
        app.dispatch(UiAction::MotionAdvancedTo(280));
        assert!(app.state.appearance.motion.wants_tick());

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(
            rendered.contains("• working")
                || rendered.contains("working")
                || rendered.contains("…")
                || rendered.to_lowercase().contains("running"),
            "expected activity chrome, got: {rendered:?}"
        );

        app.dispatch(UiAction::AgentRunChanged(state::AgentRunState::Idle));
        assert!(!app.state.appearance.motion.wants_tick());
    }

    #[test]
    fn conversation_chrome_prioritizes_session_and_project_without_product_label() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("context-session"),
        };
        app.home = Some(HomeSnapshot {
            project: "workspace".to_owned(),
            branch: Some("feature/tui-v2".to_owned()),
            model: Some("GPT Codex".to_owned()),
            provider: "OpenAI".to_owned(),
            recent_sessions: Vec::new(),
        });
        let mut terminal = Terminal::new(TestBackend::new(120, 36)).expect("terminal");

        terminal.draw(|frame| app.render(frame)).expect("render");

        let buffer = terminal.backend().buffer();
        let rendered = buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        let top_row = buffer
            .content()
            .iter()
            .take(usize::from(buffer.area.width))
            .map(|cell| cell.symbol())
            .collect::<String>();
        // Right: session title (original placement). Left is git/context chrome.
        assert!(
            top_row.contains("New conversation") && top_row.contains("workspace"),
            "expected right-side title/project chrome: {top_row:?}"
        );
        let title_at = top_row.find("New conversation").expect("title");
        let mid = top_row.len() / 2;
        assert!(
            title_at >= mid.saturating_sub(8),
            "title should sit toward the right half, got col {title_at} in {top_row:?}"
        );
        assert!(!top_row.contains("mitsuro"));
        assert!(!top_row.contains("plan"));
        // Bottom status: model · reasoning · permission only (no build mode / branch / tokens).
        assert!(
            rendered.contains("GPT Codex"),
            "status should show model: {rendered}"
        );
        assert!(
            rendered.contains("autonomous") || rendered.contains("supervised"),
            "status should show permission mode"
        );
        assert!(
            !rendered.contains("build ·"),
            "status should not lead with build/plan mode"
        );
        assert!(!rendered.contains("foundation"));
    }

    #[test]
    fn session_title_is_editable_via_click_and_enter() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("rename-me"),
        };
        app.conversation.set_title(Some("Old title".to_owned()));
        let mut terminal = Terminal::new(TestBackend::new(100, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");

        // Click the right context meta region (title) to start editing.
        let meta = app
            .last_layout
            .as_ref()
            .and_then(|layout| {
                layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::ContextMeta)
            })
            .expect("context meta");
        assert!(app
            .handle_event(Event::Mouse(crossterm::event::MouseEvent {
                kind: crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Left),
                column: meta.x.saturating_add(meta.width.saturating_sub(4)),
                row: meta.y,
                modifiers: KeyModifiers::NONE,
            }))
            .handled());
        assert!(app.state.title_edit.active);
        assert_eq!(app.state.title_edit.buffer, "Old title");

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Backspace,
                KeyModifiers::NONE
            )))
            .handled());
        // Clear remaining and type a new title.
        while !app.state.title_edit.buffer.is_empty() {
            assert!(app
                .handle_event(Event::Key(KeyEvent::new(
                    KeyCode::Backspace,
                    KeyModifiers::NONE
                )))
                .handled());
        }
        for ch in "Fresh name".chars() {
            assert!(app
                .handle_event(Event::Key(KeyEvent::new(
                    KeyCode::Char(ch),
                    KeyModifiers::NONE
                )))
                .handled());
        }
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE
            )))
            .handled());
        assert!(!app.state.title_edit.active);
        assert_eq!(
            app.conversation.presentation().metadata.title.as_deref(),
            Some("Fresh name")
        );
    }

    #[test]
    fn wide_conversation_restores_goal_and_plugin_sidebar_with_a_single_toggle() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("sidebar-session"),
        };
        app.plan = Some(PlanSnapshot {
            title: "Polish the TUI".to_owned(),
            objective: "Finish the interaction and layout system.".to_owned(),
            status: "active".to_owned(),
            completed_steps: 2,
            total_steps: 4,
            current_step: Some("Refine the workspace sidebar".to_owned()),
            steps: vec![
                crate::tui_v2::services::PlanStepRow {
                    description: "Layout system".to_owned(),
                    done: true,
                    active: false,
                },
                crate::tui_v2::services::PlanStepRow {
                    description: "Refine the workspace sidebar".to_owned(),
                    done: false,
                    active: true,
                },
                crate::tui_v2::services::PlanStepRow {
                    description: "Interaction polish".to_owned(),
                    done: false,
                    active: false,
                },
            ],
        });
        app.extensions = vec![ExtensionRow {
            category: "Plugin".to_owned(),
            id: "design-tools".to_owned(),
            name: "Design tools".to_owned(),
            status: "1.0.0".to_owned(),
            enabled: true,
            toggleable: true,
        }];
        let mut terminal = Terminal::new(TestBackend::new(160, 36)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("sidebar");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!rendered.contains("GOAL / PLAN"));
        assert!(rendered.contains("GOAL"));
        assert!(rendered.contains("PLAN"));
        assert!(rendered.contains("Finish the interaction and layout system."));
        assert!(rendered.contains("Refine the workspace sidebar"));
        // Open-circle checklist under PLAN; no criteria / next footer.
        assert!(
            rendered.contains("○") || rendered.contains("●") || rendered.contains("o "),
            "plan dock should use circle markers for steps"
        );
        assert!(!rendered.contains("Done when"));
        assert!(!rendered.contains("No step in progress"));
        // Title-less purple dock: plan content, plugin well empty-state (no border titles).
        assert!(
            rendered.contains("No plugin loaded") || rendered.contains("Plugins ready"),
            "plugin well should render under the plan band"
        );
        let layout = app.last_layout.as_ref().expect("wide layout");
        let transcript = layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::Transcript)
            .expect("transcript");
        let inspector = layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::Inspector)
            .expect("inspector");
        let plan_dock = layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::PlanDock)
            .expect("plan dock");
        let plugin_dock = layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::PluginDock)
            .expect("plugin dock");
        assert!(plan_dock.bottom() <= plugin_dock.y);
        assert_eq!(plan_dock.x, inspector.x);
        assert_eq!(plugin_dock.x, inspector.x);
        let composer = layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            .expect("composer");
        assert!(transcript.right() < inspector.x);
        // Dock open: left gutter only; dock channel owns stream→panel separation.
        let primary = layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::Primary)
            .expect("primary");
        assert_eq!(
            transcript.width,
            primary
                .width
                .saturating_sub(crate::tui_v2::layout::responsive::TRANSCRIPT_SIDE_GUTTER)
        );
        assert_eq!(
            transcript.x,
            crate::tui_v2::layout::responsive::TRANSCRIPT_SIDE_GUTTER
        );
        assert_eq!(transcript.right(), primary.right());
        // Scrollbar sits centered in the primary→inspector channel.
        if let Some(sb) =
            layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::TranscriptScrollbar)
        {
            let left_pad = sb.x.saturating_sub(primary.right());
            let right_pad = inspector.x.saturating_sub(sb.right());
            assert_eq!(
                left_pad, right_pad,
                "scrollbar not centered in dock channel"
            );
        }
        // Composer shares the panel's outer right edge (full route width).
        assert_eq!(composer.x, 0);
        assert_eq!(composer.width, 160);
        assert_eq!(composer.right(), inspector.right());

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('t'),
                KeyModifiers::CONTROL,
            )))
            .handled());
        terminal
            .draw(|frame| app.render(frame))
            .expect("sidebar hidden");
        assert!(app.last_layout.as_ref().is_some_and(|layout| {
            layout
                .region(crate::tui_v2::layout::snapshot::LayoutRegionId::Inspector)
                .is_none()
        }));
    }

    #[test]
    fn composer_exposes_compact_controls_without_claiming_typed_tab() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.controls = ControlSnapshot {
            reasoning: Some("reasoning high".to_owned()),
            fast_available: true,
            fast_enabled: true,
            permission: "supervised".to_owned(),
        };
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(
            rendered.contains("reasoning")
                || rendered.contains("supervised")
                || rendered.contains("fast")
                || rendered.contains("autonomous"),
            "expected compact controls, got: {rendered:?}"
        );

        app.state.composer.buffer.clear();
        app.state.composer.insert("draft");
        assert!(!app
            .handle_event(Event::Key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE,)))
            .handled());
        assert_eq!(app.state.composer.text(), "draft");
    }

    #[test]
    fn extensions_center_exposes_state_and_keeps_toggle_failures_local() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.extensions = vec![
            ExtensionRow {
                category: "Skill".to_owned(),
                id: "release-check".to_owned(),
                name: "release-check".to_owned(),
                status: "disabled".to_owned(),
                enabled: false,
                toggleable: true,
            },
            ExtensionRow {
                category: "MCP".to_owned(),
                id: "filesystem".to_owned(),
                name: "filesystem".to_owned(),
                status: "connected".to_owned(),
                enabled: true,
                toggleable: false,
            },
        ];
        app.dispatch(UiAction::Invoke(ActionId::OpenExtensions));

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Skill · release-check"));
        assert!(rendered.contains("MCP · filesystem"));
        assert!(rendered.contains("Enter toggle"));

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(
            app.state.picker.error.as_deref(),
            Some("Runtime services are unavailable.")
        );
        assert!(matches!(
            app.state.overlay.as_ref().map(|overlay| &overlay.kind),
            Some(OverlayKind::ExtensionsCenter)
        ));
    }

    #[test]
    fn slash_commands_dispatch_locally_and_clear_starts_a_fresh_draft() {
        let mut app = PreviewApp::preview();

        app.submit_composer("/model".to_owned());
        assert!(matches!(
            app.state.overlay.as_ref().map(|overlay| &overlay.kind),
            Some(OverlayKind::ModelPicker)
        ));
        assert!(app.conversation.presentation().turns.is_empty());

        if let Some(overlay_id) = app.state.overlay.as_ref().map(|overlay| overlay.id) {
            app.dispatch(UiAction::OverlayClosed(overlay_id));
        }
        // /home, /new, and /clear are the same product action.
        for command in ["/home", "/new", "/clear"] {
            app.submit_composer("Keep this first conversation.".to_owned());
            assert_eq!(
                app.conversation.presentation().turns.len(),
                1,
                "seed turn before {command}"
            );
            app.submit_composer(command.to_owned());
            assert!(
                matches!(app.state.route, AppRoute::Home),
                "{command} should route Home"
            );
            assert!(
                app.conversation.presentation().turns.is_empty(),
                "{command} should clear the draft"
            );
            assert_eq!(
                app.conversation.presentation().metadata.session_id,
                "new-conversation"
            );
        }
    }

    #[test]
    fn slash_autocomplete_is_compact_layout_owned_and_completes_without_submitting() {
        let mut app = PreviewApp::preview();
        app.state.capability = CapabilityProfile {
            glyph_mode: crate::tui_v2::model::capability::GlyphMode::Ascii,
            color_depth: crate::tui_v2::model::capability::ColorDepth::Monochrome,
        };
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('/'),
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app.state.composer.autocomplete_open);

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal
            .draw(|frame| app.render(frame))
            .expect("autocomplete");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        // Titleless popup chrome: list + footer hints, no "Commands" title band.
        assert!(rendered.contains("/home"));
        assert!(
            rendered.contains("/home")
                || rendered.contains("complete")
                || rendered.contains("Tab")
                || rendered.contains("enter"),
            "expected slash popup chrome, got: {rendered:?}"
        );
        assert!(rendered.is_ascii());
        assert!(app.last_layout.as_ref().is_some_and(|layout| layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerAutocomplete)
            .is_some()));

        for _ in 1..crate::tui_v2::input::slash::DEFINITIONS.len() {
            assert!(app
                .handle_event(Event::Key(
                    KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
                ))
                .handled());
        }
        let mut compact = Terminal::new(TestBackend::new(50, 16)).expect("compact terminal");
        compact
            .draw(|frame| app.render(frame))
            .expect("scrolled command catalog");
        let scrolled = compact
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        // Selection follows keyboard into the last entry; window scrolls.
        assert!(scrolled.contains("/permissions"));
        assert_eq!(
            app.state.composer.autocomplete_selected,
            crate::tui_v2::input::slash::DEFINITIONS.len() - 1
        );

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE,)))
            .handled());
        terminal
            .draw(|frame| app.render(frame))
            .expect("closed autocomplete");
        let closed =
            crate::tui_v2::test_support::BufferSnapshot::capture(terminal.backend().buffer(), None);
        // Semantic close contract: catalog gone and layout drops the popup region.
        assert!(!closed.text().contains("/permissions"));
        assert!(!closed.text().contains("complete"));
        assert!(app.last_layout.as_ref().is_some_and(|layout| layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerAutocomplete)
            .is_none()));
        assert!(closed.text().contains('/'));

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Backspace,
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('/'),
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app.state.composer.autocomplete_open);

        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE,)))
            .handled());
        assert_eq!(app.state.composer.text(), "/load");
        assert!(!app.state.composer.autocomplete_open);
        assert!(app.conversation.presentation().turns.is_empty());

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(matches!(
            app.state.overlay.as_ref().map(|overlay| &overlay.kind),
            Some(OverlayKind::SessionPicker)
        ));
    }

    #[test]
    fn project_tree_search_is_compact_ascii_safe_and_artifact_clean() {
        let mut app = PreviewApp::preview();
        app.state.capability = CapabilityProfile {
            glyph_mode: crate::tui_v2::model::capability::GlyphMode::Ascii,
            color_depth: crate::tui_v2::model::capability::ColorDepth::Monochrome,
        };
        app.project_entries = vec![ProjectEntry {
            path: "src".to_owned(),
            name: "src".to_owned(),
            parent: String::new(),
            kind: crate::tui_v2::services::ProjectEntryKind::Directory,
            search_path: "src".to_owned(),
            search_name: "src".to_owned(),
        }];
        app.project_entries
            .extend(
                ["src/main.rs", "src/model.rs"]
                    .into_iter()
                    .map(|path| ProjectEntry {
                        path: path.to_owned(),
                        name: path.rsplit('/').next().unwrap_or(path).to_owned(),
                        parent: "src".to_owned(),
                        kind: crate::tui_v2::services::ProjectEntryKind::File,
                        search_path: path.to_owned(),
                        search_name: path.rsplit('/').next().unwrap_or(path).to_owned(),
                    }),
            );
        assert!(app
            .handle_event(Event::Paste("Review @".to_owned()))
            .handled());
        assert!(app.state.composer.file_search_open);
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(app.state.composer.text(), "Review @src/");
        assert!(app.state.composer.file_search_open);

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal
            .draw(|frame| app.render(frame))
            .expect("file search");
        let open =
            crate::tui_v2::test_support::BufferSnapshot::capture(terminal.backend().buffer(), None);
        // Titleless popup chrome — list + footer, no "Project N/M" title.
        open.assert_contains("src/main.rs");
        open.assert_contains("src/model.rs");
        assert!(
            open.text().contains("Enter") || open.text().contains("complete"),
            "expected footer hints, got {}",
            open.text()
        );
        assert!(open.text().is_ascii());
        assert!(app.last_layout.as_ref().is_some_and(|layout| layout
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerAutocomplete)
            .is_some()));

        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE,)))
            .handled());
        assert_eq!(app.state.composer.text(), "Review [src/model.rs] ");
        assert!(!app.state.composer.file_search_open);
        assert!(app.conversation.presentation().turns.is_empty());

        terminal
            .draw(|frame| app.render(frame))
            .expect("closed file search");
        let closed =
            crate::tui_v2::test_support::BufferSnapshot::capture(terminal.backend().buffer(), None);
        assert!(closed.text().contains("[src/model.rs]"));
        assert!(!closed.text().contains("Enter open") && !closed.text().contains("scroll wheel"));
        let mut reference = PreviewApp::preview();
        reference.state.capability = app.state.capability;
        reference.state.composer.buffer.clear();
        reference.state.composer.insert(app.state.composer.text());
        reference
            .state
            .composer
            .set_cursor_byte(app.state.composer.cursor_byte());
        let mut reference_terminal =
            Terminal::new(TestBackend::new(80, 24)).expect("reference terminal");
        reference_terminal
            .draw(|frame| reference.render(frame))
            .expect("reference frame");
        let expected = crate::tui_v2::test_support::BufferSnapshot::capture(
            reference_terminal.backend().buffer(),
            None,
        );
        assert_eq!(closed, expected, "file search left stale cells or styles");
    }

    #[test]
    fn init_command_uses_agent_runtime_but_keeps_a_compact_user_prompt() {
        let mut app = PreviewApp::preview();
        app.submit_composer("/init crates/mitsuro-cli".to_owned());

        let user = app.conversation.presentation().turns[0]
            .user
            .as_ref()
            .expect("user prompt");
        assert_eq!(user.text, "/init");
        assert!(matches!(app.state.route, AppRoute::Conversation { .. }));
    }

    #[test]
    fn setup_navigation_is_typed_and_credentials_are_never_rendered() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.state.route = AppRoute::Setup;
        app.setup = Some(setup_fixture());

        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert_eq!(app.state.setup.provider_index, 1);
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(app.state.setup.step, SetupStep::AuthMethod);
        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(app.state.setup.step, SetupStep::Credential);

        let secret = "sk-test-secret-that-must-not-render";
        assert!(app.handle_event(Event::Paste(secret.to_owned())).handled());
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered =
            terminal
                .backend()
                .buffer()
                .content()
                .iter()
                .fold(String::new(), |mut text, cell| {
                    text.push_str(cell.symbol());
                    text
                });

        assert!(!rendered.contains(secret));
        assert!(rendered.contains("••••"));
        assert_eq!(app.state.composer.text(), secret);
    }

    #[test]
    fn connected_setup_provider_advances_directly_to_its_model_catalog() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Setup;
        app.setup = Some(setup_fixture());

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(app.state.setup.step, SetupStep::Model);
        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert_eq!(app.state.setup.model_index, 1);
    }

    #[test]
    fn setup_renders_every_provider_advertised_auth_method() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Setup;
        app.setup = Some(setup_fixture());
        app.state.setup.provider_index = 1;
        app.state.setup.step = SetupStep::AuthMethod;
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");

        terminal.draw(|frame| app.render(frame)).expect("render");

        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("OAuth (Browser)"));
        assert!(rendered.contains("API Key"));
        assert!(rendered.contains("Choose how to authenticate"));
        assert!(
            !rendered.contains("selected model"),
            "selected model line is chrome-noise on Connections/setup"
        );
        assert!(
            !rendered.contains("connect mitsuro"),
            "overlay title already names the surface"
        );
    }

    #[test]
    fn every_setup_step_renders_cleanly_in_unicode_and_ascii_modes() {
        for glyph_mode in [
            crate::tui_v2::model::capability::GlyphMode::Unicode,
            crate::tui_v2::model::capability::GlyphMode::Ascii,
        ] {
            for step in [
                SetupStep::Provider,
                SetupStep::AuthMethod,
                SetupStep::Credential,
                SetupStep::OAuthWaiting,
                SetupStep::OAuthPasteCode,
                SetupStep::CatalogLoading,
                SetupStep::Model,
            ] {
                let mut app = PreviewApp::preview();
                app.state.capability = CapabilityProfile {
                    glyph_mode,
                    color_depth: if glyph_mode == crate::tui_v2::model::capability::GlyphMode::Ascii
                    {
                        crate::tui_v2::model::capability::ColorDepth::Monochrome
                    } else {
                        crate::tui_v2::model::capability::ColorDepth::TrueColor
                    },
                };
                app.state.route = AppRoute::Setup;
                app.state.setup.step = step;
                app.state.setup.oauth_message = Some("Waiting securely…".to_owned());
                app.state.setup.device_code = Some("ABCD-EFGH".to_owned());
                app.state.setup.oauth_url = Some("https://example.test/auth".to_owned());
                if matches!(step, SetupStep::Credential | SetupStep::OAuthPasteCode) {
                    app.state.composer.buffer.clear();
                    app.state.composer.insert("never-render-this-secret");
                }
                app.setup = Some(setup_fixture());
                let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
                terminal.draw(|frame| app.render(frame)).expect("setup");
                let rendered = terminal
                    .backend()
                    .buffer()
                    .content()
                    .iter()
                    .map(|cell| cell.symbol())
                    .collect::<String>();
                assert!(!rendered.contains("never-render-this-secret"));
                if glyph_mode == crate::tui_v2::model::capability::GlyphMode::Ascii {
                    assert!(
                        rendered.is_ascii(),
                        "{step:?} leaked a Unicode UI glyph in ASCII mode: {rendered}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn oauth_device_code_update_enters_a_stable_waiting_state() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Setup;
        app.setup = Some(setup_fixture());
        app.state.setup.step = SetupStep::OAuthWaiting;

        app.handle_oauth_update(crate::tui_support::utils::OAuthStatusUpdate {
            provider: ProviderId::OpenAI,
            success: true,
            message: "Enter the code in your browser".to_owned(),
            device_code: Some(crate::tui_support::utils::DeviceCodeInfo {
                user_code: "ABCD-EFGH".to_owned(),
                verification_uri: "https://example.test/device".to_owned(),
            }),
            token: None,
        })
        .await;

        assert_eq!(app.state.setup.step, SetupStep::OAuthWaiting);
        assert_eq!(app.state.setup.device_code.as_deref(), Some("ABCD-EFGH"));
        assert_eq!(
            app.state.setup.oauth_url.as_deref(),
            Some("https://example.test/device")
        );
        assert!(app.state.setup.error.is_none());
    }

    #[test]
    fn session_picker_filters_canonical_rows_and_escape_restores_composer() {
        let mut app = PreviewApp::preview();
        app.sessions = vec![
            RecentSession {
                session_id: "one".to_owned(),
                title: "Setup polish".to_owned(),
                model: Some("GPT Alpha".to_owned()),
            },
            RecentSession {
                session_id: "two".to_owned(),
                title: "Artifact cleanup".to_owned(),
                model: Some("Claude Beta".to_owned()),
            },
        ];
        app.dispatch(UiAction::Invoke(ActionId::OpenSessionPicker));
        assert!(app.picker_overlay_active());
        assert_eq!(
            app.state.overlay.as_ref().map(|overlay| overlay.phase),
            Some(crate::tui_v2::model::overlay::OverlayPhase::Ready)
        );

        for character in "claude".chars() {
            assert!(app
                .handle_event(Event::Key(KeyEvent::new(
                    KeyCode::Char(character),
                    KeyModifiers::NONE,
                )))
                .handled());
        }
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Artifact cleanup"));
        assert!(!rendered.contains("Setup polish"));

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE,)))
            .handled());
        assert!(app.state.overlay.is_none());
        assert!(app.state.focus.is_composer());
    }

    #[test]
    fn opening_session_restores_the_exact_pending_question_dock() {
        let recovery = SessionRecoveryState::new_with_pending_interactions(
            RecoveryStatus::AwaitingInput,
            Some(LoopStopReason::AwaitingInput),
            None,
            PartialAssistantState::default(),
            vec![PendingInteractionSnapshot::ask_user_from_call(
                "ask-recovered",
                &json!({
                    "questions": [{
                        "header": "Scope",
                        "question": "Which scope should continue?",
                        "options": [{"label": "Focused"}, {"label": "Complete"}],
                        "multiSelect": false
                    }]
                }),
            )],
            RecoveryDecision::NonResumable {
                reason: RecoveryNonResumableReason::AwaitingHumanInput,
            },
        );
        let mut app = PreviewApp::preview();

        app.apply_loaded_session(LoadedSession {
            session_id: "recovered-session".to_owned(),
            title: "Recovered work".to_owned(),
            messages: Vec::new(),
            recovery: Some(recovery),
            token_count: Some(12_000),
            delegation_groups: Vec::new(),
        });
        assert_eq!(
            app.conversation
                .presentation()
                .metadata
                .usage
                .as_ref()
                .map(|usage| usage.total_tokens),
            Some(12_000)
        );

        assert_eq!(
            app.state.route,
            AppRoute::Conversation {
                session_id: SessionId::from_canonical("recovered-session")
            }
        );
        assert!(matches!(
            app.state.focus,
            crate::tui_v2::model::focus::FocusTarget::DecisionDock
        ));
        assert!(matches!(
            app.conversation
                .presentation()
                .pending_interactions
                .first(),
            Some(PendingInteraction::Questions(questions))
                if questions.tool_call_id == "ask-recovered"
                    && questions.questions[0].options[1].label == "Complete"
        ));
    }

    #[tokio::test]
    async fn detached_progress_receiver_survives_parent_finish_until_sender_closes() {
        let mut app = PreviewApp::preview();
        let (sender, receiver) = mpsc::unbounded_channel();
        app.attach_delegated_progress(receiver);

        app.handle_loop_event(LoopEvent::Finished {
            session_id: "preview-session".to_owned(),
            stop_reason: LoopStopReason::Completed,
        });

        assert_eq!(app.delegated_progress.len(), 1);
        drop(sender);
        assert!(app.delegated_progress.next().await.is_none());
        assert!(app.delegated_progress.is_empty());
    }

    #[test]
    fn opening_session_projects_its_durable_delegation_group() {
        let group: DelegationGroupRecord = serde_json::from_value(json!({
            "delegation_group_id": "group-reopen",
            "parent_session_id": "delegated-session",
            "parent_tool_call_id": null,
            "contract": {
                "execution_mode": "detached",
                "completion_policy": {"kind": "all_settled"},
                "failure_policy": "continue",
                "governance": {
                    "permission_mode": "supervised",
                    "delegated_turn_budget": 8,
                    "max_parallelism": 1,
                    "execution_tool_allowlist": null,
                    "delegation_policy": {
                        "surface": "subagent_build",
                        "inherited_permission_mode": "supervised",
                        "supervised_approval_granted": false,
                        "max_turns": 8,
                        "read_only_only": false,
                        "bash_allowed": false
                    }
                }
            },
            "state": "running",
            "parent_continuation_state": "not_requested",
            "parent_continuation_id": null,
            "synthesis_owner_id": null,
            "synthesis_lease_expires_at_ms": null,
            "synthesis_attempt_count": 0,
            "tasks": [{
                "delegation_group_id": "group-reopen",
                "ordinal": 0,
                "specification": {
                    "delegation_task_id": "task-reopen",
                    "task_key": "builder-a",
                    "objective": "Continue the durable build",
                    "role": "build",
                    "target_scope": [],
                    "max_attempts": 2,
                    "writer_mode": "isolated",
                    "attempt_workspace": null,
                    "workspace_baseline": null
                },
                "state": "leased",
                "attempt_count": 1,
                "result": null,
                "error_summary": null,
                "created_at": "2026-08-08T12:00:00Z",
                "updated_at": "2026-08-08T12:00:01Z",
                "completed_at": null
            }],
            "created_at": "2026-08-08T12:00:00Z",
            "updated_at": "2026-08-08T12:00:01Z",
            "completed_at": null
        }))
        .expect("delegation fixture");
        let mut app = PreviewApp::preview();

        app.apply_loaded_session(LoadedSession {
            session_id: "delegated-session".to_owned(),
            title: "Delegated work".to_owned(),
            messages: Vec::new(),
            recovery: None,
            token_count: None,
            delegation_groups: vec![group],
        });

        assert!(matches!(
            &app.conversation.presentation().turns[0].parts[0],
            TimelinePart::Tool(tool)
                if tool.status == crate::tui_v2::model::conversation::ToolStatus::Running
                    && matches!(&tool.artifact.content,
                        crate::tui_v2::model::artifact::ArtifactContent::Text(text)
                            if text.text.contains("· status  waiting for provider")
                                && text.text.contains("· group  running"))
        ));
    }

    #[test]
    fn command_palette_runs_the_registered_model_picker_action() {
        let mut app = PreviewApp::preview();
        app.setup = Some(setup_fixture());
        app.dispatch(UiAction::Invoke(ActionId::OpenCommandPalette));
        for character in "choose model".chars() {
            assert!(app
                .handle_event(Event::Key(KeyEvent::new(
                    KeyCode::Char(character),
                    KeyModifiers::NONE,
                )))
                .handled());
        }
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app
            .state
            .overlay
            .as_ref()
            .is_some_and(|overlay| matches!(overlay.kind, OverlayKind::ModelPicker)));

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("GPT Alpha"));
        assert!(!rendered.contains("Claude Alpha"));
    }

    #[test]
    fn appearance_picker_applies_theme_and_motion_without_a_component_timer() {
        let mut app = PreviewApp::preview();
        app.dispatch(UiAction::Invoke(ActionId::OpenThemeAppearance));
        app.state.picker.selected = 1;
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(
            app.state.appearance.theme,
            crate::tui_v2::presentation::theme::ThemeKind::MitsuroLight
        );

        app.state.picker.selected = crate::tui_v2::components::service_inspector::THEMES.len() + 1;
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(
            app.state.appearance.motion.preference,
            MotionPreference::Reduced
        );
        assert!(!app.state.appearance.motion.wants_tick());
    }

    #[test]
    fn agent_markdown_is_quietly_styled_without_exposing_fence_artifacts() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Show the shape.".to_owned(), Vec::new(), false);
        app.handle_loop_event(LoopEvent::TextDelta {
            delta: "# Result\n- compact\n```rust\nlet polished = true;\n```\nUse `cargo test`."
                .to_owned(),
        });
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Result"));
        assert!(rendered.contains("compact"));
        assert!(rendered.contains("let polished = true;"));
        assert!(rendered.contains("cargo test"));
        assert!(!rendered.contains("```"));
    }

    #[test]
    fn agent_markdown_links_share_osc8_and_mouse_geometry() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Show the guide.".to_owned(), Vec::new(), false);
        app.handle_loop_event(LoopEvent::TextDelta {
            delta: "Read [the guide](https://example.com/guide).".to_owned(),
        });
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();

        assert!(rendered.contains("\x1b]8;;https://example.com/guide\x07"));
        assert!(app.last_layout.as_ref().is_some_and(|layout| {
            layout.interactions.iter().any(|region| {
                matches!(
                    &region.intent,
                    crate::tui_v2::layout::snapshot::InteractionIntent::OpenLink(url)
                        if url == "https://example.com/guide"
                )
            })
        }));
    }

    #[test]
    fn artifact_inspector_replaces_placeholder_and_scrolls_exact_content() {
        let (mut app, part_id) = app_with_streaming_tool();
        app.dispatch(UiAction::OverlayOpened(
            OverlayKind::FileArtifactInspector {
                part_id: part_id.clone(),
            },
        ));
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let first = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(first.contains("check 0"));
        assert!(!first.contains("ready for its feature adapter"));

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::PageDown,
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app
            .state
            .artifacts
            .get(&part_id)
            .is_some_and(|artifact| artifact.inner_scroll > 0));
    }

    #[test]
    fn process_inspector_renders_service_rows_and_does_not_capture_plain_input() {
        let mut app = PreviewApp::preview();
        app.state.capability = unicode_capability();
        app.processes = vec![ProcessRow {
            id: "process-1".to_owned(),
            command: "cargo test -p mitsuro".to_owned(),
            status: "running".to_owned(),
            elapsed_seconds: 7,
            active: true,
        }];
        app.dispatch(UiAction::Invoke(ActionId::OpenProcesses));
        assert!(!app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))
            .handled());
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("cargo test -p mitsuro"));
        assert!(rendered.contains("running · 7s"));
    }

    #[test]
    fn agent_questions_render_options_and_submit_all_answers_to_the_exact_tool() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Choose the scope.".to_owned(), Vec::new(), false);
        app.handle_loop_event(LoopEvent::ToolCallComplete {
            id: "ask-42".to_owned(),
            name: "AskUserQuestion".to_owned(),
            arguments: json!({
                "questions": [
                    {
                        "header": "Scope",
                        "question": "How broad should this be?",
                        "options": [
                            {"label": "Focused", "description": "One surface"},
                            {"label": "Complete", "description": "All surfaces"}
                        ],
                        "multiSelect": false
                    },
                    {
                        "header": "Proof",
                        "question": "Which proof should run?",
                        "options": [
                            {"label": "Tests"},
                            {"label": "Clippy"}
                        ],
                        "multiSelect": true
                    }
                ]
            }),
        });
        // Finished(AwaitingInput) must not turn Question into an "approval" row.
        app.handle_loop_event(LoopEvent::Finished {
            session_id: "preview-session".to_owned(),
            stop_reason: LoopStopReason::AwaitingInput,
        });
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let first = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(first.contains("How broad should this be?"));
        assert!(first.contains("Focused"));
        // Titleless dock: header label is not painted as a top strip.
        assert!(
            !first.contains("Scope  1/") && !first.contains("Scope 1/"),
            "header strip should be gone: {first}"
        );
        // Quiet tool row — no structured-arg dump.
        assert!(!first.contains("structured value omitted"));
        assert!(!first.contains("approval"));

        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(app.state.decision_dock.current_question, 1);
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char(' '),
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char(' '),
                KeyModifiers::NONE,
            )))
            .handled());
        assert_eq!(app.state.decision_dock.toggled_options, vec![0, 1]);
        let pending = app
            .conversation
            .presentation()
            .pending_interactions
            .first()
            .and_then(|pending| match pending {
                PendingInteraction::Questions(value) => Some(value),
                _ => None,
            })
            .expect("pending questions");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&serialize_question_answers(
                pending,
                &[
                    QuestionAnswer::Single("Complete".to_owned()),
                    QuestionAnswer::Multiple(vec!["Tests".to_owned(), "Clippy".to_owned()]),
                ],
            ))
            .expect("question response"),
            json!({
                "answers": {
                    "Scope": "Complete",
                    "Proof": ["Tests", "Clippy"]
                }
            })
        );
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());

        assert!(app
            .conversation
            .presentation()
            .pending_interactions
            .is_empty());
        assert!(app.state.focus.is_composer());
        let tool = app.conversation.presentation().turns[0]
            .parts
            .iter()
            .find_map(|part| match part {
                TimelinePart::Tool(tool) if tool.tool_call_id == "ask-42" => Some(tool),
                _ => None,
            })
            .expect("question tool");
        let output = match &tool.artifact.content {
            crate::tui_v2::model::artifact::ArtifactContent::Text(value) => value.text.clone(),
            crate::tui_v2::model::artifact::ArtifactContent::Fields(fields) => fields
                .iter()
                .map(|field| format!("{}={}", field.key, field.value))
                .collect::<Vec<_>>()
                .join("\n"),
            other => panic!("unexpected question artifact: {other:?}"),
        };
        assert!(output.contains("Complete"));
    }

    #[test]
    fn multi_select_enter_without_toggle_uses_focused_option() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Pick areas.".to_owned(), Vec::new(), false);
        app.handle_loop_event(LoopEvent::ToolCallComplete {
            id: "ask-multi".to_owned(),
            name: "AskUserQuestion".to_owned(),
            arguments: json!({
                "questions": [{
                    "header": "Focus",
                    "question": "Which areas?",
                    "options": [
                        {"label": "Gameplay"},
                        {"label": "Polish"},
                        {"label": "UX"}
                    ],
                    "multiSelect": true
                }]
            }),
        });
        // Composer focus must not strand answers (this is what looked broken).
        app.state.focus = crate::tui_v2::model::focus::FocusTarget::Composer;
        assert!(app
            .handle_event(Event::Key(
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE,)
            ))
            .handled());
        assert_eq!(app.state.decision_dock.selected_option, 1);
        assert!(app.state.decision_dock.toggled_options.is_empty());
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )))
            .handled());
        // Empty multi toggles + Enter must still submit (focused row) and clear pending.
        assert!(
            app.conversation
                .presentation()
                .pending_interactions
                .is_empty(),
            "Enter without Space should still answer multi-select"
        );
        assert!(app.state.focus.is_composer());
        let tool = app.conversation.presentation().turns[0]
            .parts
            .iter()
            .find_map(|part| match part {
                TimelinePart::Tool(tool) if tool.tool_call_id == "ask-multi" => Some(tool),
                _ => None,
            })
            .expect("question tool");
        assert!(
            matches!(
                tool.status,
                crate::tui_v2::model::conversation::ToolStatus::Succeeded
            ),
            "multi answer should mark tool succeeded"
        );
    }

    #[test]
    fn single_select_digit_commits_immediately() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Pick one.".to_owned(), Vec::new(), false);
        app.handle_loop_event(LoopEvent::ToolCallComplete {
            id: "ask-digit".to_owned(),
            name: "AskUserQuestion".to_owned(),
            arguments: json!({
                "questions": [{
                    "header": "Choice",
                    "question": "Which one?",
                    "options": [
                        {"label": "Alpha"},
                        {"label": "Beta"}
                    ],
                    "multiSelect": false
                }]
            }),
        });
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('2'),
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app
            .conversation
            .presentation()
            .pending_interactions
            .is_empty());
        let tool = app.conversation.presentation().turns[0]
            .parts
            .iter()
            .find_map(|part| match part {
                TimelinePart::Tool(tool) if tool.tool_call_id == "ask-digit" => Some(tool),
                _ => None,
            })
            .expect("question tool");
        let output = match &tool.artifact.content {
            crate::tui_v2::model::artifact::ArtifactContent::Text(value) => value.text.clone(),
            crate::tui_v2::model::artifact::ArtifactContent::Fields(fields) => fields
                .iter()
                .map(|field| format!("{}={}", field.key, field.value))
                .collect::<Vec<_>>()
                .join("\n"),
            other => panic!("unexpected question artifact: {other:?}"),
        };
        assert!(output.contains("Beta"), "digit should commit: {output}");
        assert!(matches!(
            tool.status,
            crate::tui_v2::model::conversation::ToolStatus::Succeeded
        ));
    }

    #[test]
    fn approval_shortcut_resolves_the_exact_pending_tool() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Write the file.".to_owned(), Vec::new(), false);
        app.conversation
            .apply_event(LoopEvent::ToolApprovalRequired {
                id: "write-42".to_owned(),
                name: "write".to_owned(),
                arguments: json!({"path": "src/main.rs"}),
            });
        app.state.focus = crate::tui_v2::model::focus::FocusTarget::DecisionDock;

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('a'),
                KeyModifiers::NONE,
            )))
            .handled());

        assert!(app
            .conversation
            .presentation()
            .pending_interactions
            .is_empty());
        assert!(app.conversation.presentation().turns[0]
            .parts
            .iter()
            .any(|part| matches!(
                part,
                TimelinePart::Tool(tool)
                    if tool.tool_call_id == "write-42"
                        && tool.status == crate::tui_v2::model::conversation::ToolStatus::Approved
            )));
        assert!(app.state.focus.is_composer());
    }

    #[test]
    fn approval_click_resolves_through_the_last_layout_snapshot() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Write the file.".to_owned(), Vec::new(), false);
        app.conversation
            .apply_event(LoopEvent::ToolApprovalRequired {
                id: "write-mouse".to_owned(),
                name: "write".to_owned(),
                arguments: json!({"path": "src/main.rs"}),
            });
        app.state.focus = crate::tui_v2::model::focus::FocusTarget::DecisionDock;
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let approve = app
            .last_layout
            .as_ref()
            .and_then(|layout| {
                layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::DecisionApprove)
            })
            .expect("approve region");

        assert!(app
            .handle_event(Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: approve.x,
                row: approve.y,
                modifiers: KeyModifiers::NONE,
            }))
            .handled());
        assert!(app
            .conversation
            .presentation()
            .pending_interactions
            .is_empty());
    }

    #[test]
    fn artifact_expansion_preserves_the_triggering_screen_row() {
        let (mut app, part_id) = app_with_streaming_tool();
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let before = visible_part_y(&app, &part_id);

        app.dispatch(UiAction::ArtifactToggled(part_id.clone()));
        terminal.draw(|frame| app.render(frame)).expect("render");

        assert_eq!(visible_part_y(&app, &part_id), before);
        assert!(app.state.artifacts[&part_id].expanded);
    }

    #[test]
    fn fullscreen_artifact_scrolls_and_escape_unwinds_it_first() {
        let (mut app, part_id) = app_with_streaming_tool();
        app.state.transcript.selected_part = Some(part_id.clone());
        app.state.focus = crate::tui_v2::model::focus::FocusTarget::Transcript {
            part_id: part_id.clone(),
        };
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::Char('f'),
                KeyModifiers::NONE,
            )))
            .handled());
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        assert!(app
            .last_layout
            .as_ref()
            .and_then(|layout| layout
                .region(crate::tui_v2::layout::snapshot::LayoutRegionId::FullScreenArtifact))
            .is_some());

        assert!(app
            .handle_event(Event::Key(KeyEvent::new(
                KeyCode::PageDown,
                KeyModifiers::NONE,
            )))
            .handled());
        assert!(app.state.artifacts[&part_id].inner_scroll > 0);
        assert!(app
            .handle_event(Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE,)))
            .handled());
        assert!(!app.state.artifacts[&part_id].fullscreen);
        assert!(matches!(
            app.state.focus,
            crate::tui_v2::model::focus::FocusTarget::Artifact { .. }
        ));
    }

    #[test]
    fn streaming_away_from_live_edge_accumulates_and_click_clears_indicator() {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Explain it.".to_owned(), Vec::new(), false);
        app.handle_loop_event(LoopEvent::TextDelta {
            delta: "First".to_owned(),
        });
        app.state.transcript.follow_live = false;
        app.handle_loop_event(LoopEvent::TextDelta {
            delta: " second".to_owned(),
        });
        assert_eq!(app.state.transcript.unseen_parts, 1);

        let mut terminal = Terminal::new(TestBackend::new(80, 24)).expect("terminal");
        terminal.draw(|frame| app.render(frame)).expect("render");
        let indicator = app
            .last_layout
            .as_ref()
            .and_then(|layout| {
                layout.region(crate::tui_v2::layout::snapshot::LayoutRegionId::NewContentIndicator)
            })
            .expect("indicator");
        assert!(app
            .handle_event(Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: indicator.x,
                row: indicator.y,
                modifiers: KeyModifiers::NONE,
            }))
            .handled());
        assert!(app.state.transcript.follow_live);
        assert_eq!(app.state.transcript.unseen_parts, 0);
    }

    fn app_with_streaming_tool() -> (PreviewApp, PartId) {
        let mut app = PreviewApp::preview();
        app.state.route = AppRoute::Conversation {
            session_id: SessionId::from_canonical("preview-session"),
        };
        app.conversation
            .push_user_prompt("u1", "Run the checks.".to_owned(), Vec::new(), false);
        app.conversation.apply_event(LoopEvent::ToolExecuting {
            id: "bash-live".to_owned(),
            name: "bash".to_owned(),
        });
        app.conversation.apply_event(LoopEvent::ToolOutputDelta {
            id: "bash-live".to_owned(),
            delta: (0..80)
                .map(|line| format!("check {line}"))
                .collect::<Vec<_>>()
                .join("\n"),
        });
        let part_id = app.conversation.presentation().turns[0]
            .parts
            .iter()
            .find_map(|part| match part {
                TimelinePart::Tool(tool) => Some(tool.id.clone()),
                _ => None,
            })
            .expect("tool part");
        (app, part_id)
    }

    fn visible_part_y(app: &PreviewApp, part_id: &PartId) -> u16 {
        app.last_layout
            .as_ref()
            .and_then(|layout| {
                layout
                    .transcript
                    .parts
                    .iter()
                    .find(|part| &part.part_id == part_id)
            })
            .map(|part| part.visible_rect.y)
            .expect("visible part")
    }

    fn setup_fixture() -> SetupSnapshot {
        SetupSnapshot {
            providers: vec![
                SetupProvider {
                    id: ProviderId::OpenAI,
                    label: "OpenAI".to_owned(),
                    connected: true,
                    auth_methods: ProviderId::OpenAI.auth_methods(),
                    models: vec![
                        SetupModel {
                            key: ModelKey::new(
                                ProviderId::OpenAI,
                                "gpt-alpha",
                                ApiFormat::OpenAIResponses,
                            ),
                            label: "GPT Alpha".to_owned(),
                        },
                        SetupModel {
                            key: ModelKey::new(
                                ProviderId::OpenAI,
                                "gpt-beta",
                                ApiFormat::OpenAIResponses,
                            ),
                            label: "GPT Beta".to_owned(),
                        },
                    ],
                },
                SetupProvider {
                    id: ProviderId::Anthropic,
                    label: "Anthropic".to_owned(),
                    connected: false,
                    auth_methods: ProviderId::Anthropic.auth_methods(),
                    models: vec![SetupModel {
                        key: ModelKey::new(
                            ProviderId::Anthropic,
                            "claude-alpha",
                            ApiFormat::Anthropic,
                        ),
                        label: "Claude Alpha".to_owned(),
                    }],
                },
            ],
            selected_model: None,
        }
    }
}
