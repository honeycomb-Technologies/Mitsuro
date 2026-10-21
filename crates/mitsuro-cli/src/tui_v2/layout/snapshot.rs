//! Immutable geometry for one terminal frame.

use std::ops::Range;

use ratatui::layout::{Position, Rect};

use crate::tui_v2::{
    input::action::ActionId,
    model::{
        artifact::PartId,
        focus::FocusTarget,
        overlay::{OverlayId, OverlayKind},
    },
};

use super::{anchor::TranscriptAnchor, responsive::ResponsiveClass};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RouteLayoutKind {
    Setup,
    Home,
    Conversation,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LayoutRegionId {
    Canvas,
    ContextBar,
    /// Left: git diff + agent context fill.
    ContextIdentity,
    /// Center: reserved band (working lives on the bottom edge rail).
    ContextStatus,
    /// Right: session title · project.
    ContextMeta,
    TopDivider,
    Primary,
    Transcript,
    NewContentIndicator,
    FullScreenArtifact,
    DecisionDock,
    DecisionApprove,
    DecisionDeny,
    DecisionInspect,
    Inspector,
    /// Plan / goal band in the wide workspace dock.
    PlanDock,
    /// Drag / breath gap between plan and plugin panels.
    DockDivider,
    /// Plugin / game well under the plan band.
    PluginDock,
    Composer,
    ComposerField,
    ComposerAutocomplete,
    /// 1-cell transcript scrollbar track.
    TranscriptScrollbar,
    /// 1-cell composer scrollbar track.
    ComposerScrollbar,
    StatusLine,
    StatusMeta,
    ActionFooter,
    /// Bottom border row — purple comet working edge while the agent runs.
    BottomDivider,
    Overlay,
    ResizeMessage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutRegion {
    pub id: LayoutRegionId,
    pub rect: Rect,
    pub clip: Rect,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InteractionId(String);

impl InteractionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[cfg(test)]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScrollRegionId {
    Transcript,
    Artifact(PartId),
    Overlay(OverlayId),
    PlanDock,
    PluginDock,
    Composer,
    /// Slash / `@` assist panel above the composer.
    ComposerAssist,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InteractionIntent {
    Focus(FocusTarget),
    Invoke(ActionId),
    ToggleArtifact(PartId),
    OpenLink(String),
    Scroll(ScrollRegionId),
    /// Click/drag the scrollbar thumb track for a scroll region.
    Scrollbar(ScrollRegionId),
    /// Click the context-bar session title to rename it.
    EditSessionTitle,
    /// Click a row in the slash / `@` assist panel.
    ComposerAssist,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InteractionRegion {
    pub id: InteractionId,
    pub bounds: Rect,
    pub clip: Rect,
    pub intent: InteractionIntent,
}

impl InteractionRegion {
    pub fn contains(&self, position: Position) -> bool {
        contains(self.bounds, position) && contains(self.clip, position)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectionRow {
    pub screen_y: u16,
    pub part_id: PartId,
    pub source: Range<usize>,
    pub column_offsets: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartLayout {
    pub part_id: PartId,
    pub revision: u64,
    pub full_height: u32,
    pub visible_rect: Rect,
    pub clip_rows: Range<u32>,
    pub source_rows: Range<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TranscriptLayout {
    pub viewport: Rect,
    pub total_height: u32,
    pub scroll_top: u32,
    pub parts: Vec<PartLayout>,
    pub selection_rows: Vec<SelectionRow>,
    pub anchor: Option<TranscriptAnchor>,
    pub at_live_edge: bool,
}

impl TranscriptLayout {
    pub fn selection_at(&self, position: Position) -> Option<SelectionPoint> {
        if !contains(self.viewport, position) {
            return None;
        }
        let row = self
            .selection_rows
            .iter()
            .find(|row| row.screen_y == position.y)?;
        Some(SelectionPoint {
            part_id: row.part_id.clone(),
            source_offset: source_offset_for_column(row, position.x, self.viewport.x),
        })
    }

    /// Like `selection_at`, but clamps Y to the nearest selection row so drag
    /// stays alive when the cursor briefly leaves a text row.
    pub fn selection_at_clamped(&self, position: Position) -> Option<SelectionPoint> {
        if self.selection_rows.is_empty() {
            return None;
        }
        if let Some(point) = self.selection_at(position) {
            return Some(point);
        }
        // Horizontal must still be over the transcript column.
        if position.x < self.viewport.x || position.x >= self.viewport.right() {
            return None;
        }
        let row = self
            .selection_rows
            .iter()
            .min_by_key(|row| row.screen_y.abs_diff(position.y))?;
        let x = position
            .x
            .clamp(self.viewport.x, self.viewport.right().saturating_sub(1));
        Some(SelectionPoint {
            part_id: row.part_id.clone(),
            source_offset: source_offset_for_column(row, x, self.viewport.x),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectionPoint {
    pub part_id: PartId,
    pub source_offset: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InvalidationPlan {
    pub full: bool,
    pub clear_regions: Vec<Rect>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutSnapshot {
    pub generation: u64,
    pub viewport: Rect,
    pub class: ResponsiveClass,
    pub route: RouteLayoutKind,
    pub overlay: Option<(OverlayId, OverlayKind)>,
    pub regions: Vec<LayoutRegion>,
    pub interactions: Vec<InteractionRegion>,
    pub transcript: TranscriptLayout,
    pub focus_rect: Option<Rect>,
}

impl LayoutSnapshot {
    pub fn region(&self, id: LayoutRegionId) -> Option<Rect> {
        self.regions
            .iter()
            .find(|region| region.id == id)
            .map(|region| region.rect)
    }

    pub fn hit_test(&self, position: Position) -> Option<&InteractionRegion> {
        self.interactions
            .iter()
            .rev()
            .find(|region| region.contains(position))
    }

    pub fn selection_at(&self, position: Position) -> Option<SelectionPoint> {
        self.transcript.selection_at(position)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self
            .regions
            .iter()
            .any(|region| intersect(region.rect, self.viewport) != region.clip)
        {
            return Err("layout region escaped its viewport clip");
        }
        if self
            .interactions
            .iter()
            .any(|region| intersect(region.bounds, self.viewport) != region.clip)
        {
            return Err("interaction region escaped its viewport clip");
        }
        Ok(())
    }
}

pub fn transition_between(
    previous: Option<&LayoutSnapshot>,
    current: &LayoutSnapshot,
) -> InvalidationPlan {
    let Some(previous) = previous else {
        return InvalidationPlan {
            full: true,
            clear_regions: vec![current.viewport],
        };
    };
    if previous.viewport != current.viewport || previous.route != current.route {
        return InvalidationPlan {
            full: true,
            clear_regions: vec![current.viewport],
        };
    }

    let previous_overlay = previous.region(LayoutRegionId::Overlay);
    let current_overlay = current.region(LayoutRegionId::Overlay);
    let clear_regions = match (previous_overlay, current_overlay) {
        (Some(old), Some(new)) if old != new => vec![union(old, new)],
        (Some(old), None) => vec![old],
        (None, Some(new)) => vec![new],
        _ => Vec::new(),
    };

    InvalidationPlan {
        full: false,
        clear_regions,
    }
}

pub(crate) fn intersect(left: Rect, right: Rect) -> Rect {
    let x = left.x.max(right.x);
    let y = left.y.max(right.y);
    let right_edge = left.right().min(right.right());
    let bottom_edge = left.bottom().min(right.bottom());
    Rect::new(
        x,
        y,
        right_edge.saturating_sub(x),
        bottom_edge.saturating_sub(y),
    )
}

fn union(left: Rect, right: Rect) -> Rect {
    let x = left.x.min(right.x);
    let y = left.y.min(right.y);
    Rect::new(
        x,
        y,
        left.right().max(right.right()).saturating_sub(x),
        left.bottom().max(right.bottom()).saturating_sub(y),
    )
}

fn contains(rect: Rect, position: Position) -> bool {
    position.x >= rect.x
        && position.x < rect.right()
        && position.y >= rect.y
        && position.y < rect.bottom()
}

fn source_offset_for_column(row: &SelectionRow, x: u16, viewport_x: u16) -> usize {
    let column = usize::from(x.saturating_sub(viewport_x));
    row.column_offsets
        .get(column)
        .copied()
        .unwrap_or(row.source.end)
}
