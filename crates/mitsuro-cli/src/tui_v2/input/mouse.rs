//! Mouse resolution against the last immutable layout snapshot.
//!
//! Classic TUI mouse model, snapshot-first:
//! click routing, wheel scroll, text selection, hover links, scrollbar drag,
//! and composer click-to-caret.

use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::Position;

use crate::tui_v2::{
    app::reducer::UiAction,
    layout::snapshot::{InteractionIntent, LayoutSnapshot, ScrollRegionId, SelectionPoint},
    model::focus::FocusTarget,
};

/// Result of resolving a mouse event against the current frame snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MouseResolution {
    Action(UiAction),
    Scroll {
        region: ScrollRegionId,
        rows: i32,
    },
    OpenLink(String),
    SelectionStart(SelectionPoint),
    /// Drag over the transcript; includes pointer for edge-scroll detection.
    SelectionDrag {
        point: SelectionPoint,
        column: u16,
        row: u16,
    },
    SelectionEnd,
    /// Absolute screen coordinates over the composer field (pointer down).
    ComposerClick {
        column: u16,
        row: u16,
    },
    /// Drag while selecting inside the composer field.
    ComposerSelectionDrag {
        column: u16,
        row: u16,
    },
    /// Jump/drag a scrollbar track to a new offset.
    ScrollbarJump {
        region: ScrollRegionId,
        y: u16,
    },
    ScrollbarDrag {
        region: ScrollRegionId,
        y: u16,
    },
    ScrollbarEnd,
    Hover {
        position: (u16, u16),
        link: Option<String>,
    },
    /// Start in-place session title edit (context bar).
    EditSessionTitle,
    /// Click inside slash / `@` assist panel (screen coordinates).
    ComposerAssistClick {
        column: u16,
        row: u16,
    },
}

pub fn resolve_mouse(snapshot: &LayoutSnapshot, event: MouseEvent) -> Option<MouseResolution> {
    let position = Position::new(event.column, event.row);
    match event.kind {
        MouseEventKind::Down(MouseButton::Left) => resolve_left_down(snapshot, position),
        MouseEventKind::Drag(MouseButton::Left) => resolve_left_drag(snapshot, position),
        MouseEventKind::Up(MouseButton::Left) => Some(MouseResolution::SelectionEnd),
        MouseEventKind::Moved => Some(MouseResolution::Hover {
            position: (event.column, event.row),
            link: hover_link(snapshot, position),
        }),
        MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
            resolve_scroll(snapshot, position, event.kind)
        }
        _ => None,
    }
}

/// Variant used when a scrollbar or composer selection drag is already active.
pub fn resolve_mouse_with_drag(
    snapshot: &LayoutSnapshot,
    event: MouseEvent,
    scrollbar_drag: Option<&ScrollRegionId>,
    selecting_composer: bool,
) -> Option<MouseResolution> {
    if let Some(region) = scrollbar_drag {
        match event.kind {
            MouseEventKind::Drag(MouseButton::Left) => {
                return Some(MouseResolution::ScrollbarDrag {
                    region: region.clone(),
                    y: event.row,
                });
            }
            MouseEventKind::Up(MouseButton::Left) => {
                return Some(MouseResolution::ScrollbarEnd);
            }
            _ => {}
        }
    }
    if selecting_composer {
        match event.kind {
            MouseEventKind::Drag(MouseButton::Left) => {
                return Some(MouseResolution::ComposerSelectionDrag {
                    column: event.column,
                    row: event.row,
                });
            }
            MouseEventKind::Up(MouseButton::Left) => {
                return Some(MouseResolution::SelectionEnd);
            }
            _ => {}
        }
    }
    resolve_mouse(snapshot, event)
}

fn resolve_left_down(snapshot: &LayoutSnapshot, position: Position) -> Option<MouseResolution> {
    for hit in snapshot.interactions.iter().rev() {
        if !hit.contains(position) {
            continue;
        }
        match &hit.intent {
            InteractionIntent::OpenLink(url) => {
                return Some(MouseResolution::OpenLink(url.clone()));
            }
            InteractionIntent::EditSessionTitle => {
                return Some(MouseResolution::EditSessionTitle);
            }
            InteractionIntent::ComposerAssist => {
                return Some(MouseResolution::ComposerAssistClick {
                    column: position.x,
                    row: position.y,
                });
            }
            InteractionIntent::Scrollbar(region) => {
                return Some(MouseResolution::ScrollbarJump {
                    region: region.clone(),
                    y: position.y,
                });
            }
            InteractionIntent::Invoke(action) => {
                return Some(MouseResolution::Action(UiAction::Invoke(*action)));
            }
            InteractionIntent::ToggleArtifact(part_id) => {
                return Some(MouseResolution::Action(UiAction::ArtifactActivated(
                    part_id.clone(),
                )));
            }
            InteractionIntent::Focus(FocusTarget::Composer) => {
                return Some(MouseResolution::ComposerClick {
                    column: position.x,
                    row: position.y,
                });
            }
            InteractionIntent::Focus(FocusTarget::Transcript { .. })
            | InteractionIntent::Scroll(ScrollRegionId::Transcript) => {
                // Prefer text selection on the reading surface.
                if let Some(point) = snapshot.selection_at(position) {
                    return Some(MouseResolution::SelectionStart(point));
                }
                if let InteractionIntent::Focus(target) = &hit.intent {
                    return Some(MouseResolution::Action(UiAction::FocusChanged(
                        target.clone(),
                    )));
                }
            }
            InteractionIntent::Focus(target) => {
                return Some(MouseResolution::Action(UiAction::FocusChanged(
                    target.clone(),
                )));
            }
            InteractionIntent::Scroll(_) => continue,
        }
    }

    if let Some(point) = snapshot.selection_at(position) {
        return Some(MouseResolution::SelectionStart(point));
    }
    None
}

fn resolve_left_drag(snapshot: &LayoutSnapshot, position: Position) -> Option<MouseResolution> {
    if let Some(field) =
        snapshot.region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
    {
        // Composer is typically full-width under the stream. Only treat the drag
        // as composer selection when the pointer is *vertically* over the field.
        // X-only matching stole every transcript drag (same columns as composer).
        // Vertical edge-scroll while already selecting the composer is handled by
        // `resolve_mouse_with_drag` when `selecting_composer` is true.
        if position.x >= field.x
            && position.x < field.right()
            && position.y >= field.y
            && position.y < field.bottom()
        {
            return Some(MouseResolution::ComposerSelectionDrag {
                column: position.x.clamp(field.x, field.right().saturating_sub(1)),
                row: position.y.clamp(field.y, field.bottom().saturating_sub(1)),
            });
        }
    }
    // Transcript: clamp Y so drag past top/bottom still updates selection + edge-scroll.
    snapshot
        .transcript
        .selection_at_clamped(position)
        .map(|point| MouseResolution::SelectionDrag {
            point,
            column: position.x,
            row: position.y,
        })
}

fn resolve_scroll(
    snapshot: &LayoutSnapshot,
    position: Position,
    kind: MouseEventKind,
) -> Option<MouseResolution> {
    let region = snapshot
        .interactions
        .iter()
        .rev()
        .find(|region| {
            region.contains(position)
                && matches!(
                    region.intent,
                    InteractionIntent::Scroll(_) | InteractionIntent::Scrollbar(_)
                )
        })
        .and_then(|region| match &region.intent {
            InteractionIntent::Scroll(id) | InteractionIntent::Scrollbar(id) => Some(id.clone()),
            _ => None,
        })?;

    let height = scroll_region_height(snapshot, &region).max(1);
    let amount = (u32::from(height) / 10).clamp(3, 10) as i32;
    let rows = if matches!(kind, MouseEventKind::ScrollUp) {
        -amount
    } else {
        amount
    };
    Some(MouseResolution::Scroll { region, rows })
}

fn scroll_region_height(snapshot: &LayoutSnapshot, region: &ScrollRegionId) -> u16 {
    match region {
        ScrollRegionId::Transcript => snapshot.transcript.viewport.height,
        ScrollRegionId::Artifact(_) => snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::FullScreenArtifact)
            .or_else(|| snapshot.region(crate::tui_v2::layout::snapshot::LayoutRegionId::Primary))
            .map(|rect| rect.height)
            .unwrap_or(snapshot.viewport.height),
        ScrollRegionId::Overlay(_) => snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::Overlay)
            .map(|rect| rect.height)
            .unwrap_or(12),
        ScrollRegionId::PlanDock => snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::PlanDock)
            .map(|rect| rect.height)
            .unwrap_or(8),
        ScrollRegionId::PluginDock => snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::PluginDock)
            .map(|rect| rect.height)
            .unwrap_or(8),
        ScrollRegionId::Composer => snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            .map(|rect| rect.height)
            .unwrap_or(4),
        ScrollRegionId::ComposerAssist => snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerAutocomplete)
            .map(|rect| {
                rect.height
                    .saturating_sub(
                        crate::tui_v2::components::primitive::assist_chrome::ASSIST_CHROME_ROWS,
                    )
                    .max(1)
            })
            .unwrap_or(8),
    }
}

fn hover_link(snapshot: &LayoutSnapshot, position: Position) -> Option<String> {
    snapshot
        .hit_test(position)
        .and_then(|hit| match &hit.intent {
            InteractionIntent::OpenLink(url) => Some(url.clone()),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use crossterm::event::KeyModifiers;
    use ratatui::layout::Rect;

    use crate::tui_v2::{
        app::route::AppRoute,
        layout::engine::{LayoutEngine, LayoutRequest},
        model::focus::FocusTarget,
    };

    use super::*;

    fn home_snapshot() -> LayoutSnapshot {
        LayoutEngine::default()
            .layout(LayoutRequest {
                viewport: Rect::new(0, 0, 80, 24),
                route: &AppRoute::Home,
                overlay: None,
                focus: &FocusTarget::Composer,
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
            })
            .snapshot
    }

    #[test]
    fn click_composer_is_click_to_caret_not_bare_focus() {
        let snapshot = home_snapshot();
        let composer = snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            .expect("composer");
        let resolution = resolve_mouse(
            &snapshot,
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: composer.x.saturating_add(2),
                row: composer.y,
                modifiers: KeyModifiers::NONE,
            },
        );
        assert!(matches!(
            resolution,
            Some(MouseResolution::ComposerClick { .. })
        ));
    }

    #[test]
    fn wheel_over_composer_targets_composer_scroll_region() {
        let snapshot = home_snapshot();
        let composer = snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            .expect("composer field");
        let resolution = resolve_mouse(
            &snapshot,
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: composer.x.saturating_add(1),
                row: composer.y.saturating_add(1),
                modifiers: KeyModifiers::NONE,
            },
        );
        assert!(matches!(
            resolution,
            Some(MouseResolution::Scroll {
                region: ScrollRegionId::Composer,
                rows
            }) if rows > 0
        ));
    }

    #[test]
    fn move_reports_hover_without_action() {
        let snapshot = home_snapshot();
        let resolution = resolve_mouse(
            &snapshot,
            MouseEvent {
                kind: MouseEventKind::Moved,
                column: 10,
                row: 10,
                modifiers: KeyModifiers::NONE,
            },
        );
        assert!(matches!(
            resolution,
            Some(MouseResolution::Hover {
                position: (10, 10),
                link: None
            })
        ));
    }

    #[test]
    fn mouse_up_ends_selection() {
        let snapshot = home_snapshot();
        assert_eq!(
            resolve_mouse(
                &snapshot,
                MouseEvent {
                    kind: MouseEventKind::Up(MouseButton::Left),
                    column: 0,
                    row: 0,
                    modifiers: KeyModifiers::NONE,
                },
            ),
            Some(MouseResolution::SelectionEnd)
        );
    }

    #[test]
    fn composer_drag_clamps_when_pointer_leaves_the_field_vertically() {
        let snapshot = home_snapshot();
        let composer = snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            .expect("composer field");
        // Outside the field vertically: only an *active* composer selection keeps
        // the drag (edge-scroll). Bare resolve_mouse must not steal stream drags.
        let resolution = resolve_mouse_with_drag(
            &snapshot,
            MouseEvent {
                kind: MouseEventKind::Drag(MouseButton::Left),
                column: composer.x.saturating_add(2),
                row: composer.y.saturating_sub(3),
                modifiers: KeyModifiers::NONE,
            },
            None,
            true, // selecting_composer
        );
        assert!(matches!(
            resolution,
            Some(MouseResolution::ComposerSelectionDrag { row, .. }) if row == composer.y.saturating_sub(3)
                || row == composer.y
        ));
        // Without an active composer selection, a drag above the field is not composer.
        let bare = resolve_mouse(
            &snapshot,
            MouseEvent {
                kind: MouseEventKind::Drag(MouseButton::Left),
                column: composer.x.saturating_add(2),
                row: composer.y.saturating_sub(3),
                modifiers: KeyModifiers::NONE,
            },
        );
        assert!(
            !matches!(bare, Some(MouseResolution::ComposerSelectionDrag { .. })),
            "transcript-column drags must not be claimed by the composer field: {bare:?}"
        );
    }

    #[test]
    fn stream_drag_is_not_stolen_by_full_width_composer() {
        use crate::tui_v2::layout::snapshot::{SelectionPoint, SelectionRow};
        use crate::tui_v2::model::artifact::PartId;

        let mut snapshot = home_snapshot();
        let composer = snapshot
            .region(crate::tui_v2::layout::snapshot::LayoutRegionId::ComposerField)
            .expect("composer field");
        // Synthetic stream row above the composer, same X span (full-width layout).
        let stream_y = composer.y.saturating_sub(4).max(1);
        snapshot.transcript.selection_rows = vec![SelectionRow {
            screen_y: stream_y,
            part_id: PartId::from_semantic("agent:1"),
            source: 0..12,
            column_offsets: (0..=12).collect(),
        }];
        snapshot.transcript.viewport =
            Rect::new(composer.x, 0, composer.width.max(40), composer.y.max(8));

        let resolution = resolve_mouse(
            &snapshot,
            MouseEvent {
                kind: MouseEventKind::Drag(MouseButton::Left),
                column: composer.x.saturating_add(5),
                row: stream_y,
                modifiers: KeyModifiers::NONE,
            },
        );
        assert!(
            matches!(
                resolution,
                Some(MouseResolution::SelectionDrag {
                    point: SelectionPoint { .. },
                    row,
                    ..
                }) if row == stream_y
            ),
            "expected transcript SelectionDrag, got {resolution:?}"
        );
    }
}
