use std::sync::Arc;

use ratatui::layout::{Position, Rect};

use crate::tui_v2::{
    app::route::{AppRoute, SessionId},
    input::action::ActionId,
    layout::{
        anchor::{AnchorMode, TranscriptAnchor},
        engine::{LayoutEngine, LayoutRequest, TranscriptRequest},
        measure::{
            ExpansionMode, MeasureRequest, MeasuredPart, MeasurementCache, MeasurementKey,
            ThemeMetrics,
        },
        snapshot::{InteractionIntent, LayoutRegionId},
    },
    model::{
        artifact::PartId,
        capability::{CapabilityProfile, ColorDepth, GlyphMode},
        focus::FocusTarget,
        overlay::{OverlayId, OverlayKind, OverlayPhase, OverlayState},
    },
    presentation::theme::{SemanticTheme, ThemeKind},
};

fn capability() -> CapabilityProfile {
    CapabilityProfile {
        glyph_mode: GlyphMode::Unicode,
        color_depth: ColorDepth::TrueColor,
    }
}

#[test]
fn new_content_indicator_owns_the_exact_follow_live_hit_target() {
    let route = AppRoute::Conversation {
        session_id: SessionId::from_canonical("session"),
    };
    let focus = FocusTarget::Composer;
    let mut cache = MeasurementCache::default();
    let items = measured_items(&mut cache, 80);
    let snapshot = LayoutEngine::default()
        .layout(LayoutRequest {
            viewport: Rect::new(0, 0, 80, 24),
            route: &route,
            overlay: None,
            focus: &focus,
            inspector_requested: false,
            dock_plan_ratio: 0.42,
            plugin_focused: false,
            fullscreen_artifact: None,
            decision_dock_height: 0,
            composer_content_rows: 1,
            composer_total_rows: 1,
            composer_fullscreen: false,
            composer_autocomplete_rows: 0,
            transcript: Some(TranscriptRequest {
                items: &items,
                spacing_before: &[],
                expandable: &[],
                anchor: AnchorMode::ScrollTop(0),
                new_content_count: 3,
            }),
        })
        .snapshot;

    let indicator = snapshot
        .region(LayoutRegionId::NewContentIndicator)
        .expect("new content indicator");
    let hit = snapshot
        .hit_test(Position::new(indicator.x, indicator.y))
        .expect("indicator hit target");
    assert_eq!(hit.intent, InteractionIntent::Invoke(ActionId::JumpEnd));
}

#[test]
fn markdown_links_own_precise_visible_hit_targets() {
    let route = AppRoute::Conversation {
        session_id: SessionId::from_canonical("session"),
    };
    let focus = FocusTarget::Composer;
    let mut cache = MeasurementCache::default();
    let item = cache.measure_markdown(
        MeasureRequest {
            key: MeasurementKey {
                part_id: PartId::from_semantic("linked-agent"),
                revision: 1,
                width: 80,
                expansion: ExpansionMode::Collapsed,
                theme_metrics: ThemeMetrics::new(ThemeKind::MitsuroDark),
                capability: capability(),
            },
            text: "Read [the guide](https://example.com/guide).",
        },
        SemanticTheme::resolve(ThemeKind::MitsuroDark, ColorDepth::TrueColor),
    );
    let items = vec![item];
    let snapshot = LayoutEngine::default()
        .layout(LayoutRequest {
            viewport: Rect::new(0, 0, 80, 24),
            route: &route,
            overlay: None,
            focus: &focus,
            inspector_requested: false,
            dock_plan_ratio: 0.42,
            plugin_focused: false,
            fullscreen_artifact: None,
            decision_dock_height: 0,
            composer_content_rows: 1,
            composer_total_rows: 1,
            composer_fullscreen: false,
            composer_autocomplete_rows: 0,
            transcript: Some(TranscriptRequest {
                items: &items,
                spacing_before: &[],
                expandable: &[],
                anchor: AnchorMode::ScrollTop(0),
                new_content_count: 0,
            }),
        })
        .snapshot;
    let link = snapshot
        .interactions
        .iter()
        .find(|region| matches!(region.intent, InteractionIntent::OpenLink(_)))
        .expect("link interaction");

    assert_eq!(
        snapshot
            .hit_test(Position::new(link.bounds.x, link.bounds.y))
            .map(|region| &region.intent),
        Some(&InteractionIntent::OpenLink(
            "https://example.com/guide".to_owned()
        ))
    );
}

fn measured_items(cache: &mut MeasurementCache, width: u16) -> Vec<Arc<MeasuredPart>> {
    [
        ("before", "before ".repeat(300)),
        (
            "anchor",
            "zero one two three four five six seven ".repeat(30),
        ),
        ("after", "after ".repeat(3_000)),
    ]
    .into_iter()
    .map(|(id, text)| {
        cache.measure(MeasureRequest {
            key: MeasurementKey {
                part_id: PartId::from_semantic(id),
                revision: 1,
                width,
                expansion: ExpansionMode::Collapsed,
                theme_metrics: ThemeMetrics::new(ThemeKind::MitsuroDark),
                capability: capability(),
            },
            text: &text,
        })
    })
    .collect()
}

#[test]
fn resize_sequence_preserves_semantic_anchor() {
    let route = AppRoute::Conversation {
        session_id: SessionId::from_canonical("session"),
    };
    let focus = FocusTarget::Transcript {
        part_id: PartId::from_semantic("anchor"),
    };
    let anchor = TranscriptAnchor::new(PartId::from_semantic("anchor"), 100, 5);
    let mut cache = MeasurementCache::default();
    let mut engine = LayoutEngine::default();

    for viewport in [
        Rect::new(0, 0, 160, 48),
        Rect::new(0, 0, 80, 24),
        Rect::new(0, 0, 50, 16),
        Rect::new(0, 0, 120, 36),
    ] {
        let items = measured_items(&mut cache, viewport.width);
        let pass = engine.layout(LayoutRequest {
            viewport,
            route: &route,
            overlay: None,
            focus: &focus,
            inspector_requested: false,
            dock_plan_ratio: 0.42,
            plugin_focused: false,
            fullscreen_artifact: None,
            decision_dock_height: 0,
            composer_content_rows: 1,
            composer_total_rows: 1,
            composer_fullscreen: false,
            composer_autocomplete_rows: 0,
            transcript: Some(TranscriptRequest {
                items: &items,
                spacing_before: &[],
                expandable: &[],
                anchor: AnchorMode::Fixed(anchor.clone()),
                new_content_count: 0,
            }),
        });
        let resolved = pass
            .snapshot
            .transcript
            .anchor
            .as_ref()
            .expect("anchor should resolve");
        assert_eq!(resolved.part_id, anchor.part_id);
        assert_eq!(resolved.source_offset, 100);
        assert_eq!(resolved.screen_row, 5);
        assert!(pass.snapshot.focus_rect.is_some());
        assert!(pass.invalidation.full);
    }
}

#[test]
fn hit_testing_and_selection_use_snapshot_geometry() {
    let route = AppRoute::Conversation {
        session_id: SessionId::from_canonical("session"),
    };
    let focus = FocusTarget::Composer;
    let mut cache = MeasurementCache::default();
    let items = measured_items(&mut cache, 80);
    let mut engine = LayoutEngine::default();
    let snapshot = engine
        .layout(LayoutRequest {
            viewport: Rect::new(0, 0, 80, 24),
            route: &route,
            overlay: None,
            focus: &focus,
            inspector_requested: false,
            dock_plan_ratio: 0.42,
            plugin_focused: false,
            fullscreen_artifact: None,
            decision_dock_height: 0,
            composer_content_rows: 1,
            composer_total_rows: 1,
            composer_fullscreen: false,
            composer_autocomplete_rows: 0,
            transcript: Some(TranscriptRequest {
                items: &items,
                spacing_before: &[],
                expandable: &[],
                anchor: AnchorMode::ScrollTop(0),
                new_content_count: 0,
            }),
        })
        .snapshot;
    let first = snapshot.transcript.selection_rows[0].clone();
    let position = Position::new(snapshot.transcript.viewport.x, first.screen_y);

    assert!(snapshot.hit_test(position).is_some());
    assert_eq!(
        snapshot.selection_at(position).expect("selection").part_id,
        first.part_id
    );
}

#[test]
fn question_dock_has_no_invisible_approval_hit_targets() {
    let route = AppRoute::Conversation {
        session_id: SessionId::from_canonical("session"),
    };
    let mut engine = LayoutEngine::default();
    let snapshot = engine
        .layout(LayoutRequest {
            viewport: Rect::new(0, 0, 80, 24),
            route: &route,
            overlay: None,
            focus: &FocusTarget::DecisionDock,
            inspector_requested: false,
            dock_plan_ratio: 0.42,
            plugin_focused: false,
            fullscreen_artifact: None,
            decision_dock_height: 7,
            composer_content_rows: 1,
            composer_total_rows: 1,
            composer_fullscreen: false,
            composer_autocomplete_rows: 0,
            transcript: None,
        })
        .snapshot;

    assert!(snapshot.region(LayoutRegionId::DecisionDock).is_some());
    assert!(snapshot.region(LayoutRegionId::DecisionApprove).is_none());
    assert!(snapshot.region(LayoutRegionId::DecisionDeny).is_none());
    assert!(snapshot.region(LayoutRegionId::DecisionInspect).is_none());
}

#[test]
fn overlay_close_invalidates_exact_old_rectangle() {
    let route = AppRoute::Home;
    let focus = FocusTarget::Composer;
    let overlay = OverlayState {
        id: OverlayId::from_sequence(7),
        kind: OverlayKind::CommandPalette,
        phase: OverlayPhase::Ready,
        return_focus: FocusTarget::Composer,
    };
    let mut engine = LayoutEngine::default();
    let open = engine.layout(LayoutRequest {
        viewport: Rect::new(0, 0, 80, 24),
        route: &route,
        overlay: Some(&overlay),
        focus: &focus,
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
    let overlay_rect = open
        .snapshot
        .region(LayoutRegionId::Overlay)
        .expect("overlay");
    let closed = engine.layout(LayoutRequest {
        viewport: Rect::new(0, 0, 80, 24),
        route: &route,
        overlay: None,
        focus: &focus,
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

    assert!(!closed.invalidation.full);
    assert_eq!(closed.invalidation.clear_regions, vec![overlay_rect]);
}

#[test]
fn composer_autocomplete_is_layout_owned_above_the_composer() {
    let route = AppRoute::Home;
    let focus = FocusTarget::Composer;
    let mut engine = LayoutEngine::default();
    let pass = engine.layout(LayoutRequest {
        viewport: Rect::new(0, 0, 80, 24),
        route: &route,
        overlay: None,
        focus: &focus,
        inspector_requested: false,
        dock_plan_ratio: 0.42,
        plugin_focused: false,
        fullscreen_artifact: None,
        decision_dock_height: 0,
        composer_content_rows: 1,
        composer_total_rows: 1,
        composer_fullscreen: false,
        composer_autocomplete_rows: 6,
        transcript: None,
    });
    let primary = pass
        .snapshot
        .region(LayoutRegionId::Primary)
        .expect("primary");
    let composer = pass
        .snapshot
        .region(LayoutRegionId::ComposerField)
        .expect("composer");
    let autocomplete = pass
        .snapshot
        .region(LayoutRegionId::ComposerAutocomplete)
        .expect("autocomplete");

    assert!(primary.contains(Position::new(autocomplete.x, autocomplete.y)));
    assert!(primary.contains(Position::new(
        autocomplete.right().saturating_sub(1),
        autocomplete.bottom().saturating_sub(1),
    )));
    assert!(autocomplete.bottom() <= composer.y);
    assert_eq!(autocomplete.x, composer.x);
    assert_eq!(autocomplete.width, composer.width);
}

#[test]
fn wide_inspector_splits_into_titleless_plan_and_plugin_docks() {
    let route = AppRoute::Conversation {
        session_id: crate::tui_v2::app::route::SessionId::from_canonical("dock"),
    };
    let focus = FocusTarget::Composer;
    let pass = LayoutEngine::default().layout(LayoutRequest {
        viewport: Rect::new(0, 0, 160, 40),
        route: &route,
        overlay: None,
        focus: &focus,
        inspector_requested: true,
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
    let inspector = pass
        .snapshot
        .region(LayoutRegionId::Inspector)
        .expect("inspector");
    let plan = pass
        .snapshot
        .region(LayoutRegionId::PlanDock)
        .expect("plan dock");
    let plugin = pass
        .snapshot
        .region(LayoutRegionId::PluginDock)
        .expect("plugin dock");
    assert_eq!(plan.x, inspector.x);
    assert_eq!(plugin.x, inspector.x);
    assert!(plan.bottom() <= plugin.y);
    assert_eq!(plugin.bottom(), inspector.bottom());
    // Click routing skips pure Scroll intents and focuses the plan dock.
    let click = crate::tui_v2::input::mouse::resolve_mouse(
        &pass.snapshot,
        crossterm::event::MouseEvent {
            kind: crossterm::event::MouseEventKind::Down(crossterm::event::MouseButton::Left),
            column: plan.x.saturating_add(1),
            row: plan.y.saturating_add(1),
            modifiers: crossterm::event::KeyModifiers::NONE,
        },
    );
    assert!(matches!(
        click,
        Some(crate::tui_v2::input::mouse::MouseResolution::Action(
            crate::tui_v2::app::reducer::UiAction::FocusChanged(FocusTarget::PlanDock)
        ))
    ));
}

#[test]
fn multiline_composer_expands_to_a_bounded_height() {
    let mut engine = LayoutEngine::default();
    let route = AppRoute::Home;
    let focus = FocusTarget::Composer;
    let pass = engine.layout(LayoutRequest {
        viewport: Rect::new(0, 0, 80, 24),
        route: &route,
        overlay: None,
        focus: &focus,
        inspector_requested: false,
        dock_plan_ratio: 0.42,
        plugin_focused: false,
        fullscreen_artifact: None,
        decision_dock_height: 0,
        composer_content_rows: 4,
        composer_total_rows: 4,
        composer_fullscreen: false,
        composer_autocomplete_rows: 0,
        transcript: None,
    });
    let composer = pass
        .snapshot
        .region(LayoutRegionId::ComposerField)
        .expect("composer");
    assert_eq!(composer.height, 6);
    assert_eq!(
        composer.bottom(),
        pass.snapshot.region(LayoutRegionId::StatusLine).unwrap().y
    );
}
