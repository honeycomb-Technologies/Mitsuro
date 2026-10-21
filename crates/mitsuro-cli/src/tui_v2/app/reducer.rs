//! Pure, deterministic UI state transitions.

use crate::tui_v2::{
    input::action::ActionId,
    model::{
        artifact::PartId,
        focus::{ControlId, FocusTarget},
        overlay::{OverlayId, OverlayKind, OverlayPhase, OverlayState},
    },
    motion::preference::MotionPreference,
};

use super::{
    effect::{
        DecisionTarget, FocusDirection, PersistedUiPreference, ScrollAmount, ScrollDirection,
        ScrollTarget, UiEffect,
    },
    route::AppRoute,
    state::{AgentRunState, AppLifecycle, DecisionAction, UiState},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UiAction {
    Invoke(ActionId),
    RouteChanged(AppRoute),
    OverlayOpened(OverlayKind),
    OverlayClosed(OverlayId),
    OverlayPhaseChanged {
        id: OverlayId,
        phase: OverlayPhase,
    },
    FocusChanged(FocusTarget),
    ComposerInserted(String),
    ComposerBackspace,
    ComposerDeletePreviousWord,
    ComposerClearToLineStart,
    /// Clear the entire composer input (Ctrl+C).
    ComposerClear,
    ComposerMoveLeft,
    ComposerMoveRight,
    ComposerMoveLineStart,
    ComposerMoveLineEnd,
    ComposerMoveVisualLine {
        forward: bool,
    },
    DecisionRequested {
        target: DecisionTarget,
        action: DecisionAction,
    },
    ArtifactActivated(PartId),
    MotionPreferenceChanged(MotionPreference),
    ThemeChanged(crate::tui_v2::presentation::theme::ThemeKind),
    MotionAdvancedTo(u64),
    TerminalResized {
        width: u16,
        height: u16,
    },
    TerminalFocusChanged(bool),
    AgentRunChanged(AgentRunState),
}

pub fn reduce(state: &mut UiState, action: UiAction) -> Vec<UiEffect> {
    match action {
        UiAction::Invoke(action) => invoke(state, action),
        UiAction::RouteChanged(route) => {
            state.route = route;
            if matches!(state.route, AppRoute::Home) {
                // Fresh load screen when returning Home (scene-local restart).
                let now = state.appearance.motion.clock.elapsed_ms();
                state.splash.restart_at(now);
                state.appearance.motion.set_active_regions(1);
            } else {
                state.appearance.motion.set_active_regions(0);
            }
            state.overlay = None;
            state.focus = FocusTarget::Composer;
            state.transcript = Default::default();
            state.artifacts.clear();
            Vec::new()
        }
        UiAction::OverlayOpened(kind) => open_overlay(state, kind),
        UiAction::OverlayClosed(id) => {
            close_overlay(state, id);
            Vec::new()
        }
        UiAction::OverlayPhaseChanged { id, phase } => {
            if let Some(overlay) = &mut state.overlay {
                if overlay.id == id {
                    overlay.phase = phase;
                }
            }
            Vec::new()
        }
        UiAction::FocusChanged(target) => {
            if focus_change_is_allowed(state, &target) {
                if let FocusTarget::Transcript { part_id } | FocusTarget::Artifact { part_id } =
                    &target
                {
                    state.transcript.selected_part = Some(part_id.clone());
                }
                state.dock.plugin_focused = matches!(target, FocusTarget::PluginDock);
                state.focus = target;
            }
            Vec::new()
        }
        UiAction::ComposerInserted(value) => {
            if state.focus.is_composer() {
                let (width, rows) = composer_nav_metrics(state);
                state.composer.insert_with_layout(&value, width, rows);
            }
            Vec::new()
        }
        UiAction::ComposerBackspace => {
            if state.focus.is_composer() {
                let (width, rows) = composer_nav_metrics(state);
                state.composer.backspace_with_layout(width, rows);
            }
            Vec::new()
        }
        UiAction::ComposerDeletePreviousWord => {
            if state.focus.is_composer() {
                let (width, rows) = composer_nav_metrics(state);
                state.composer.follow_cursor = true;
                state.composer.field_width = width;
                state.composer.field_rows = rows;
                state.composer.delete_previous_word();
            }
            Vec::new()
        }
        UiAction::ComposerClearToLineStart => {
            if state.focus.is_composer() {
                let (width, rows) = composer_nav_metrics(state);
                state.composer.follow_cursor = true;
                state.composer.field_width = width;
                state.composer.field_rows = rows;
                state.composer.clear_to_line_start();
            }
            Vec::new()
        }
        UiAction::ComposerClear => {
            if state.focus.is_composer() {
                state.composer.clear_all();
            }
            Vec::new()
        }
        UiAction::ComposerMoveLeft => {
            let (width, visible) = composer_nav_metrics(state);
            state.composer.move_left_width(width, visible);
            Vec::new()
        }
        UiAction::ComposerMoveRight => {
            let (width, visible) = composer_nav_metrics(state);
            state.composer.move_right_width(width, visible);
            Vec::new()
        }
        UiAction::ComposerMoveLineStart => {
            let (width, visible) = composer_nav_metrics(state);
            state.composer.move_to_line_start_width(width, visible);
            Vec::new()
        }
        UiAction::ComposerMoveLineEnd => {
            let (width, visible) = composer_nav_metrics(state);
            state.composer.move_to_line_end_width(width, visible);
            Vec::new()
        }
        UiAction::ComposerMoveVisualLine { forward } => {
            // Soft-wrap width matches the live field so Up/Down feel Word-like.
            let (width, visible_rows) = composer_nav_metrics(state);
            state
                .composer
                .move_visual_line(forward, width, visible_rows);
            Vec::new()
        }
        UiAction::DecisionRequested { target, action } => {
            vec![UiEffect::ResolveDecision { target, action }]
        }
        UiAction::ArtifactActivated(part_id) => {
            state.transcript.selected_part = Some(part_id.clone());
            state.focus = FocusTarget::Artifact {
                part_id: part_id.clone(),
            };
            state
                .artifacts
                .entry(part_id)
                .or_default()
                .toggle_expanded();
            Vec::new()
        }
        UiAction::MotionPreferenceChanged(preference) => {
            if state.appearance.motion.preference == preference {
                Vec::new()
            } else {
                state.appearance.motion.preference = preference;
                vec![UiEffect::PersistPreference(PersistedUiPreference::Motion(
                    preference,
                ))]
            }
        }
        UiAction::ThemeChanged(theme) => {
            if state.appearance.theme == theme {
                Vec::new()
            } else {
                state.appearance.theme = theme;
                vec![UiEffect::PersistPreference(PersistedUiPreference::Theme(
                    theme,
                ))]
            }
        }
        UiAction::MotionAdvancedTo(elapsed_ms) => {
            if state.appearance.motion.wants_tick() {
                state.appearance.motion.clock.advance_to(elapsed_ms);
            }
            Vec::new()
        }
        UiAction::TerminalResized { width, height } => {
            state.viewport = (width, height);
            // Reflow soft-wrap after resize; keep caret framed when following.
            let (w, r) = state.composer.active_metrics(width, height);
            state.composer.sync_field_metrics(w, r);
            Vec::new()
        }
        UiAction::TerminalFocusChanged(focused) => {
            state.appearance.motion.terminal_focused = focused;
            Vec::new()
        }
        UiAction::AgentRunChanged(run_state) => {
            state.agent_run = run_state;
            state
                .appearance
                .motion
                .set_active_regions(u8::from(matches!(run_state, AgentRunState::Running)));
            Vec::new()
        }
    }
}

fn invoke(state: &mut UiState, action: ActionId) -> Vec<UiEffect> {
    match action {
        ActionId::Quit => {
            state.lifecycle = AppLifecycle::ExitRequested;
            Vec::new()
        }
        ActionId::ApplyUpdate => {
            if state.update.as_ref().is_some_and(|notice| notice.can_apply) {
                state.lifecycle = AppLifecycle::ApplyUpdateRequested;
            }
            Vec::new()
        }
        ActionId::Escape => unwind_nearest(state),
        ActionId::OpenCommandPalette => open_overlay(state, OverlayKind::CommandPalette),
        ActionId::OpenSessionPicker => open_overlay(state, OverlayKind::SessionPicker),
        ActionId::OpenProcesses => open_overlay(state, OverlayKind::Processes),
        ActionId::OpenPlanGoal => open_overlay(state, OverlayKind::PlanGoal),
        ActionId::ToggleSidebar => {
            state.sidebar_visible = !state.sidebar_visible;
            Vec::new()
        }
        ActionId::OpenExtensions => open_overlay(state, OverlayKind::ExtensionsCenter),
        ActionId::OpenConnections => open_overlay(state, OverlayKind::Connections),
        ActionId::OpenModelPicker => open_overlay(state, OverlayKind::ModelPicker),
        ActionId::OpenThemeAppearance => open_overlay(state, OverlayKind::ThemeAppearance),
        ActionId::OpenHelp => open_overlay(state, OverlayKind::Help),
        ActionId::ToggleWorkMode => vec![UiEffect::ToggleCanonicalWorkMode],
        ActionId::CycleReasoning => vec![UiEffect::CycleCanonicalReasoning],
        ActionId::ToggleFastMode => vec![UiEffect::ToggleCanonicalFastMode],
        ActionId::TogglePermissionMode => vec![UiEffect::ToggleCanonicalPermissionMode],
        ActionId::ToggleComposerEditor => {
            state.composer.fullscreen = !state.composer.fullscreen;
            state.composer.autocomplete_open = false;
            state.composer.file_search_open = false;
            Vec::new()
        }
        ActionId::ScrollPageUp => vec![scroll_effect(
            state,
            ScrollDirection::Backward,
            ScrollAmount::Page,
        )],
        ActionId::ScrollPageDown => vec![scroll_effect(
            state,
            ScrollDirection::Forward,
            ScrollAmount::Page,
        )],
        ActionId::PreviousInteractivePart => {
            vec![UiEffect::MoveInteractiveFocus(FocusDirection::Previous)]
        }
        ActionId::NextInteractivePart => {
            vec![UiEffect::MoveInteractiveFocus(FocusDirection::Next)]
        }
        ActionId::PreviousDecision => {
            state.decision_dock.focused_action = state.decision_dock.focused_action.previous();
            Vec::new()
        }
        ActionId::NextDecision => {
            state.decision_dock.focused_action = state.decision_dock.focused_action.next();
            Vec::new()
        }
        ActionId::ActivateDecision
        | ActionId::ApproveDecision
        | ActionId::DenyDecision
        | ActionId::InspectDecision => Vec::new(),
        ActionId::ActivateFocused => activate_focused(state),
        ActionId::ToggleFullscreen => toggle_focused_fullscreen(state),
        ActionId::CopyFocused => vec![UiEffect::CopyFocused],
        ActionId::ToggleFollowLive => toggle_focused_follow_live(state),
        ActionId::StopProcess => open_overlay(state, OverlayKind::Processes),
        ActionId::JumpStart => vec![scroll_effect(
            state,
            ScrollDirection::Start,
            ScrollAmount::Edge,
        )],
        ActionId::JumpEnd => vec![scroll_effect(
            state,
            ScrollDirection::End,
            ScrollAmount::Edge,
        )],
        ActionId::Submit => vec![UiEffect::SubmitComposer],
        ActionId::InsertNewline => vec![UiEffect::InsertComposerNewline],
    }
}

fn open_overlay(state: &mut UiState, kind: OverlayKind) -> Vec<UiEffect> {
    let return_focus = state.overlay.as_ref().map_or_else(
        || state.focus.clone(),
        |overlay| overlay.return_focus.clone(),
    );
    let id = OverlayId::from_sequence(state.take_overlay_sequence());
    state.picker = Default::default();
    state.overlay = Some(OverlayState {
        id,
        kind: kind.clone(),
        phase: OverlayPhase::Opening,
        return_focus,
    });
    state.focus = FocusTarget::Overlay {
        overlay_id: id,
        control_id: ControlId::new("root"),
    };

    vec![UiEffect::PrepareOverlay { id, kind }]
}

fn close_overlay(state: &mut UiState, id: OverlayId) {
    let Some(overlay) = state.overlay.take() else {
        return;
    };

    if overlay.id == id {
        state.focus = restorable_focus(state, overlay.return_focus);
    } else {
        state.overlay = Some(overlay);
    }
}

fn unwind_nearest(state: &mut UiState) -> Vec<UiEffect> {
    if state.composer.autocomplete_open {
        state.composer.autocomplete_open = false;
        return Vec::new();
    }
    if state.composer.file_search_open {
        state.composer.file_search_open = false;
        return Vec::new();
    }
    if state.composer.fullscreen {
        state.composer.fullscreen = false;
        return Vec::new();
    }

    if let FocusTarget::Artifact { part_id } = &state.focus {
        if let Some(artifact) = state.artifacts.get_mut(part_id) {
            if artifact.fullscreen {
                artifact.fullscreen = false;
                return Vec::new();
            }
        }
    }

    if let Some(overlay) = &mut state.overlay {
        let id = overlay.id;
        close_overlay(state, id);
        return Vec::new();
    }

    if matches!(
        state.focus,
        FocusTarget::Artifact { .. } | FocusTarget::Transcript { .. }
    ) {
        state.focus = FocusTarget::Composer;
        return Vec::new();
    }

    if matches!(state.agent_run, AgentRunState::Running) {
        return vec![UiEffect::InterruptAgentRun];
    }

    Vec::new()
}

fn focus_change_is_allowed(state: &UiState, target: &FocusTarget) -> bool {
    match &state.overlay {
        Some(overlay) => matches!(
            target,
            FocusTarget::Overlay { overlay_id, .. } if *overlay_id == overlay.id
        ),
        None => !matches!(target, FocusTarget::Overlay { .. }),
    }
}

fn restorable_focus(state: &UiState, target: FocusTarget) -> FocusTarget {
    match &target {
        FocusTarget::Artifact { part_id } if !state.artifacts.contains_key(part_id) => {
            FocusTarget::Composer
        }
        FocusTarget::Transcript { part_id }
            if state.transcript.selected_part.as_ref() != Some(part_id) =>
        {
            FocusTarget::Composer
        }
        FocusTarget::Overlay { .. } => FocusTarget::Composer,
        _ => target,
    }
}

fn scroll_effect(state: &UiState, direction: ScrollDirection, amount: ScrollAmount) -> UiEffect {
    let target = if matches!(state.focus, FocusTarget::Artifact { .. }) {
        ScrollTarget::FocusedArtifact
    } else {
        ScrollTarget::Transcript
    };
    UiEffect::Scroll {
        target,
        direction,
        amount,
    }
}

/// Soft-wrap / viewport metrics for Word-like composer navigation.
fn composer_nav_metrics(state: &UiState) -> (usize, usize) {
    state
        .composer
        .active_metrics(state.viewport.0, state.viewport.1)
}

fn activate_focused(state: &mut UiState) -> Vec<UiEffect> {
    let part_id = match &state.focus {
        FocusTarget::Artifact { part_id } | FocusTarget::Transcript { part_id } => part_id.clone(),
        _ => return Vec::new(),
    };
    state
        .artifacts
        .entry(part_id)
        .or_default()
        .toggle_expanded();
    Vec::new()
}

fn toggle_focused_fullscreen(state: &mut UiState) -> Vec<UiEffect> {
    let part_id = match &state.focus {
        FocusTarget::Artifact { part_id } | FocusTarget::Transcript { part_id } => part_id.clone(),
        _ => return Vec::new(),
    };
    let artifact = state.artifacts.entry(part_id.clone()).or_default();
    artifact.set_fullscreen(!artifact.fullscreen);
    state.focus = FocusTarget::Artifact { part_id };
    Vec::new()
}

fn toggle_focused_follow_live(state: &mut UiState) -> Vec<UiEffect> {
    let FocusTarget::Artifact { part_id } = &state.focus else {
        return Vec::new();
    };
    let artifact = state.artifacts.entry(part_id.clone()).or_default();
    artifact.follow_live = !artifact.follow_live;
    Vec::new()
}

#[cfg(test)]
mod tests {
    use crate::tui_v2::model::capability::{ColorDepth, GlyphMode};

    use super::*;

    fn state() -> UiState {
        UiState::preview(crate::tui_v2::model::capability::CapabilityProfile {
            glyph_mode: GlyphMode::Unicode,
            color_depth: ColorDepth::TrueColor,
        })
    }

    #[test]
    fn apply_update_exits_only_when_a_managed_release_is_ready() {
        let mut state = state();
        reduce(&mut state, UiAction::Invoke(ActionId::ApplyUpdate));
        assert_eq!(state.lifecycle, AppLifecycle::Running);

        state.update = Some(crate::tui_v2::app::state::UpdateNotice {
            current_version: "0.9.22".to_owned(),
            new_version: "0.9.23".to_owned(),
            can_apply: false,
            hint: "brew upgrade".to_owned(),
        });
        reduce(&mut state, UiAction::Invoke(ActionId::ApplyUpdate));
        assert_eq!(state.lifecycle, AppLifecycle::Running);

        state.update.as_mut().expect("notice").can_apply = true;
        reduce(&mut state, UiAction::Invoke(ActionId::ApplyUpdate));
        assert_eq!(state.lifecycle, AppLifecycle::ApplyUpdateRequested);
        assert_eq!(state.apply_update_version().as_deref(), Some("0.9.23"));
    }

    #[test]
    fn replay_is_deterministic() {
        let part_id = PartId::from_semantic("turn:1/tool:read");
        let actions = vec![
            UiAction::TerminalResized {
                width: 120,
                height: 36,
            },
            UiAction::ArtifactActivated(part_id.clone()),
            UiAction::FocusChanged(FocusTarget::Artifact { part_id }),
            UiAction::Invoke(ActionId::ToggleFullscreen),
            UiAction::MotionPreferenceChanged(MotionPreference::Off),
            UiAction::Invoke(ActionId::OpenCommandPalette),
            UiAction::Invoke(ActionId::Escape),
        ];

        let replay = |mut state: UiState| {
            let effects = actions
                .clone()
                .into_iter()
                .flat_map(|action| reduce(&mut state, action))
                .collect::<Vec<_>>();
            (state, effects)
        };

        assert_eq!(replay(state()), replay(state()));
    }

    #[test]
    fn overlay_close_restores_focus_or_falls_back_to_composer() {
        let mut state = state();
        let part_id = PartId::from_semantic("part");
        state.artifacts.insert(part_id.clone(), Default::default());
        state.focus = FocusTarget::Artifact {
            part_id: part_id.clone(),
        };

        reduce(
            &mut state,
            UiAction::OverlayOpened(OverlayKind::CommandPalette),
        );
        let overlay_id = state.overlay.as_ref().expect("overlay").id;
        reduce(&mut state, UiAction::OverlayClosed(overlay_id));
        assert_eq!(state.focus, FocusTarget::Artifact { part_id });

        reduce(
            &mut state,
            UiAction::OverlayOpened(OverlayKind::CommandPalette),
        );
        let overlay_id = state.overlay.as_ref().expect("overlay").id;
        state.artifacts.clear();
        reduce(&mut state, UiAction::OverlayClosed(overlay_id));
        assert_eq!(state.focus, FocusTarget::Composer);
    }

    #[test]
    fn escape_unwinds_nearest_layer_before_interrupting_agent() {
        let mut state = state();
        state.agent_run = AgentRunState::Running;
        state.composer.autocomplete_open = true;

        assert!(reduce(&mut state, UiAction::Invoke(ActionId::Escape)).is_empty());
        assert!(!state.composer.autocomplete_open);
        assert_eq!(
            reduce(&mut state, UiAction::Invoke(ActionId::Escape)),
            vec![UiEffect::InterruptAgentRun]
        );
    }

    #[test]
    fn terminal_focus_events_pause_and_resume_the_shared_motion_clock() {
        let mut state = state();
        state.appearance.motion.set_active_regions(1);
        assert!(state.appearance.motion.wants_tick());

        reduce(&mut state, UiAction::TerminalFocusChanged(false));
        assert!(!state.appearance.motion.wants_tick());
        reduce(&mut state, UiAction::TerminalFocusChanged(true));
        assert!(state.appearance.motion.wants_tick());
    }

    #[test]
    fn stale_overlay_completion_cannot_close_the_current_overlay() {
        let mut state = state();
        reduce(
            &mut state,
            UiAction::OverlayOpened(OverlayKind::SessionPicker),
        );
        let stale_id = state.overlay.as_ref().expect("overlay").id;
        reduce(
            &mut state,
            UiAction::OverlayOpened(OverlayKind::CommandPalette),
        );

        reduce(&mut state, UiAction::OverlayClosed(stale_id));

        assert!(matches!(
            state.overlay.as_ref().map(|overlay| &overlay.kind),
            Some(OverlayKind::CommandPalette)
        ));
    }

    #[test]
    fn stop_process_never_signals_an_implicit_background_target() {
        let mut state = state();
        let effects = reduce(&mut state, UiAction::Invoke(ActionId::StopProcess));

        assert!(matches!(
            state.overlay.as_ref().map(|overlay| &overlay.kind),
            Some(OverlayKind::Processes)
        ));
        assert!(matches!(
            effects.as_slice(),
            [UiEffect::PrepareOverlay {
                kind: OverlayKind::Processes,
                ..
            }]
        ));
    }
}
