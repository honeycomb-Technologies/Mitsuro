//! Deterministic preview renderer.

use ratatui::{backend::TestBackend, Terminal};

use crate::tui_v2::{
    app::state::UiState,
    components::conversation::ConversationRenderData,
    layout::{
        anchor::AnchorMode,
        engine::{LayoutEngine, LayoutRequest},
        measure::{MeasurementCache, MeasurementCacheStats},
        snapshot::{InvalidationPlan, LayoutSnapshot},
    },
    model::{capability::CapabilityProfile, conversation::ConversationPresentation},
    presentation::{theme::SemanticTheme, transcript::ConversationDisplayList},
    render::frame::render_preview,
    services::ControlSnapshot,
};

use super::BufferSnapshot;

pub struct RenderHarness {
    terminal: Terminal<TestBackend>,
    layout_engine: LayoutEngine,
    measurements: MeasurementCache,
}

pub struct RenderedFrame {
    pub buffer: BufferSnapshot,
    pub layout: LayoutSnapshot,
    pub invalidation: InvalidationPlan,
}

impl RenderHarness {
    pub fn new(width: u16, height: u16) -> Self {
        let backend = TestBackend::new(width, height);
        let terminal = Terminal::new(backend).expect("test terminal should initialize");

        Self {
            terminal,
            layout_engine: LayoutEngine::default(),
            measurements: MeasurementCache::default(),
        }
    }

    pub fn resize(&mut self, width: u16, height: u16) {
        self.terminal.backend_mut().resize(width, height);
    }

    pub fn measurement_cache_stats(&self) -> MeasurementCacheStats {
        self.measurements.stats()
    }

    pub fn render_with_layout(mut self, capability: CapabilityProfile) -> RenderedFrame {
        let state = UiState::preview(capability);
        self.draw(&state)
    }

    pub fn draw(&mut self, state: &UiState) -> RenderedFrame {
        let theme = SemanticTheme::resolve(state.appearance.theme, state.capability.color_depth);
        let viewport = self.terminal.backend().buffer().area;
        let pass = self.layout_engine.layout(LayoutRequest {
            viewport,
            route: &state.route,
            overlay: state.overlay.as_ref(),
            focus: &state.focus,
            inspector_requested: false,
            dock_plan_ratio: 0.42,
            plugin_focused: false,
            fullscreen_artifact: None,
            decision_dock_height: 0,
            composer_content_rows: 1,
            composer_total_rows: 1,
            composer_fullscreen: false,
            composer_autocomplete_rows: 0,
            transcript: None,
        });

        self.terminal
            .draw(|frame| {
                render_preview(
                    frame,
                    state,
                    theme,
                    &pass.snapshot,
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    &[],
                    None,
                    &[],
                    &ControlSnapshot::default(),
                    None,
                )
            })
            .expect("preview should render");

        RenderedFrame {
            buffer: BufferSnapshot::capture(self.terminal.backend().buffer(), None),
            layout: pass.snapshot,
            invalidation: pass.invalidation,
        }
    }

    pub fn draw_conversation(
        &mut self,
        state: &UiState,
        conversation: &ConversationPresentation,
    ) -> RenderedFrame {
        let theme = SemanticTheme::resolve(state.appearance.theme, state.capability.color_depth);
        let viewport = self.terminal.backend().buffer().area;
        let display =
            ConversationDisplayList::build(conversation, &state.artifacts, viewport.height);
        let transcript_width = crate::tui_v2::layout::responsive::compose_route(viewport, false, 1)
            .map(|geometry| {
                crate::tui_v2::layout::responsive::transcript_column_with_dock(
                    geometry.primary,
                    false,
                )
                .width
            })
            .unwrap_or(viewport.width.max(1));
        let measured = display.measure(
            &mut self.measurements,
            transcript_width,
            &state.artifacts,
            state.appearance.theme,
            state.capability,
        );
        let expandable = display.expandable_ids();
        let spacing_before = display.spacing_before();
        let pass = self.layout_engine.layout(LayoutRequest {
            viewport,
            route: &state.route,
            overlay: state.overlay.as_ref(),
            focus: &state.focus,
            inspector_requested: false,
            dock_plan_ratio: 0.42,
            plugin_focused: false,
            fullscreen_artifact: None,
            decision_dock_height: conversation.pending_interactions.first().map_or(0, |_| 3),
            composer_content_rows: 1,
            composer_total_rows: 1,
            composer_fullscreen: false,
            composer_autocomplete_rows: 0,
            transcript: Some(crate::tui_v2::layout::engine::TranscriptRequest {
                items: &measured,
                spacing_before: &spacing_before,
                expandable: &expandable,
                anchor: if state.transcript.follow_live {
                    AnchorMode::FollowLive
                } else if let Some(anchor) = state.transcript.pending_anchor.clone() {
                    AnchorMode::Fixed(anchor)
                } else {
                    AnchorMode::ScrollTop(state.transcript.scroll_rows)
                },
                new_content_count: state.transcript.unseen_parts,
            }),
        });
        let data = ConversationRenderData {
            display: &display,
            measured: &measured,
            metadata: &conversation.metadata,
            pending: &conversation.pending_interactions,
        };
        self.terminal
            .draw(|frame| {
                render_preview(
                    frame,
                    state,
                    theme,
                    &pass.snapshot,
                    Some(data),
                    None,
                    None,
                    &[],
                    &[],
                    &[],
                    None,
                    &[],
                    &ControlSnapshot::default(),
                    None,
                )
            })
            .expect("conversation should render");

        RenderedFrame {
            buffer: BufferSnapshot::capture(self.terminal.backend().buffer(), None),
            layout: pass.snapshot,
            invalidation: pass.invalidation,
        }
    }
}
