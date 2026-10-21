//! Responsive breakpoints and route-region composition.

use ratatui::layout::Rect;

pub const MINIMUM_WIDTH: u16 = 50;
pub const MINIMUM_HEIGHT: u16 = 16;
/// Side breath for the transcript stream (each side). Even left/right padding
/// when there is no workspace dock.
pub const TRANSCRIPT_SIDE_GUTTER: u16 = 1;
/// Channel between the primary stream and the wide workspace dock.
/// Scrollbar is centered in this channel: pad · track · pad (2+1+2).
pub const INSPECTOR_GAP: u16 = 5;
/// Cells between content and the transcript scrollbar when no dock is open
/// (mirrored on the outer side so the track sits centered in its gutter).
pub const SCROLLBAR_CONTENT_GAP: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ResponsiveClass {
    TooSmall,
    Compact,
    Standard,
    Wide,
}

impl ResponsiveClass {
    pub const fn resolve(viewport: Rect) -> Self {
        if viewport.width < MINIMUM_WIDTH || viewport.height < MINIMUM_HEIGHT {
            Self::TooSmall
        } else if viewport.width < 80 || viewport.height < 24 {
            Self::Compact
        } else if viewport.width < 120 {
            Self::Standard
        } else {
            Self::Wide
        }
    }

    pub const fn supports_inspector(self) -> bool {
        matches!(self, Self::Wide)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteGeometry {
    pub context_bar: Rect,
    pub top_divider: Rect,
    pub primary: Rect,
    pub inspector: Option<Rect>,
    pub composer: Rect,
    pub status_line: Rect,
    pub bottom_divider: Rect,
}

pub fn compose_route(
    viewport: Rect,
    inspector_requested: bool,
    composer_content_rows: u16,
) -> Option<RouteGeometry> {
    let class = ResponsiveClass::resolve(viewport);
    if matches!(class, ResponsiveClass::TooSmall) {
        return None;
    }

    let context_bar = Rect::new(viewport.x, viewport.y, viewport.width, 1);
    let top_divider = Rect::new(viewport.x, viewport.y.saturating_add(1), viewport.width, 0);
    // Bottom row reserved for the purple working edge (comet line) when the
    // agent is running; idle frames leave it as canvas.
    let bottom_divider = Rect::new(
        viewport.x,
        viewport.bottom().saturating_sub(1),
        viewport.width,
        1,
    );
    let status_line = Rect::new(
        viewport.x,
        bottom_divider.y.saturating_sub(1),
        viewport.width,
        1,
    );
    let compact_height = if matches!(class, ResponsiveClass::Compact) {
        3
    } else {
        4
    };
    let composer_height = compact_height.max(composer_content_rows.clamp(1, 4).saturating_add(2));
    let composer = Rect::new(
        viewport.x,
        status_line.y.saturating_sub(composer_height),
        viewport.width,
        composer_height,
    );
    let body = Rect::new(
        viewport.x,
        top_divider.bottom(),
        viewport.width,
        composer.y.saturating_sub(top_divider.bottom()),
    );

    let (primary, inspector) = if inspector_requested && class.supports_inspector() {
        // Keep the dock width stable, then leave a dedicated channel so the
        // transcript scrollbar can sit centered between stream and panels.
        let inspector_width = (viewport.width / 3).clamp(32, 46);
        let channel = INSPECTOR_GAP;
        let primary_width = body
            .width
            .saturating_sub(inspector_width)
            .saturating_sub(channel);
        (
            Rect::new(body.x, body.y, primary_width, body.height),
            Some(Rect::new(
                body.x.saturating_add(primary_width).saturating_add(channel),
                body.y,
                inspector_width,
                body.height,
            )),
        )
    } else {
        (body, None)
    };

    Some(RouteGeometry {
        context_bar,
        top_divider,
        primary,
        inspector,
        composer,
        status_line,
        bottom_divider,
    })
}

/// Vertical inset when the workspace dock is open so stream top/bottom line up
/// with the bordered plan/plugin panels (1-cell frame on each end).
pub const TRANSCRIPT_DOCK_VERTICAL_INSET: u16 = 1;

/// Full-bleed transcript stream inside the primary pane.
///
/// Content uses the primary width with equal side gutters. When a workspace
/// dock is present the right gutter is omitted so [`INSPECTOR_GAP`] and the
/// centered scrollbar own the separation to the panels.
///
/// With the dock open, also insets top/bottom by [`TRANSCRIPT_DOCK_VERTICAL_INSET`]
/// so the message stream aligns with the plan/plugin panel chrome (inline ends).
pub fn transcript_column_with_dock(primary: Rect, dock_open: bool) -> Rect {
    let left = TRANSCRIPT_SIDE_GUTTER;
    let right = if dock_open { 0 } else { TRANSCRIPT_SIDE_GUTTER };
    let v_inset = if dock_open {
        TRANSCRIPT_DOCK_VERTICAL_INSET.min(primary.height.saturating_sub(2) / 2)
    } else {
        0
    };
    let width = primary
        .width
        .saturating_sub(left.saturating_add(right))
        .max(1);
    let height = primary
        .height
        .saturating_sub(v_inset.saturating_mul(2))
        .max(1);
    Rect::new(
        primary.x.saturating_add(left),
        primary.y.saturating_add(v_inset),
        width,
        height,
    )
}

/// Place a 1-cell scrollbar centered in `[left, right)`.
pub fn centered_scrollbar_x(left: u16, right: u16) -> Option<u16> {
    let width = right.saturating_sub(left);
    if width == 0 {
        return None;
    }
    Some(left.saturating_add(width.saturating_sub(1) / 2))
}

pub fn centered_overlay(viewport: Rect, class: ResponsiveClass) -> Rect {
    let horizontal_margin = if matches!(class, ResponsiveClass::Compact) {
        2
    } else {
        4
    };
    let vertical_margin = 2;
    let maximum_width = viewport.width.saturating_sub(horizontal_margin * 2);
    let maximum_height = viewport.height.saturating_sub(vertical_margin * 2);
    let preferred_width = if matches!(class, ResponsiveClass::Wide) {
        92
    } else {
        72
    };
    let preferred_height = if matches!(class, ResponsiveClass::Compact) {
        12
    } else {
        22
    };
    let width = maximum_width.min(preferred_width);
    let height = maximum_height.min(preferred_height);

    Rect::new(
        viewport
            .x
            .saturating_add(viewport.width.saturating_sub(width) / 2),
        viewport
            .y
            .saturating_add(viewport.height.saturating_sub(height) / 2),
        width,
        height,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responsive_composition_never_overlaps_persistent_regions() {
        for viewport in [
            Rect::new(0, 0, 50, 16),
            Rect::new(0, 0, 80, 24),
            Rect::new(0, 0, 120, 36),
            Rect::new(0, 0, 160, 48),
        ] {
            let geometry = compose_route(viewport, true, 1).expect("supported viewport");
            assert_eq!(geometry.context_bar.bottom(), geometry.top_divider.y);
            assert_eq!(geometry.top_divider.bottom(), geometry.primary.y);
            assert_eq!(geometry.primary.bottom(), geometry.composer.y);
            assert_eq!(geometry.composer.bottom(), geometry.status_line.y);
            assert_eq!(geometry.status_line.bottom(), geometry.bottom_divider.y);
            assert_eq!(geometry.bottom_divider.bottom(), viewport.bottom());
        }
    }

    #[test]
    fn transcript_column_is_full_bleed_and_never_reaches_the_sidebar() {
        let geometry =
            compose_route(Rect::new(0, 0, 160, 36), true, 1).expect("supported viewport");
        let transcript = transcript_column_with_dock(geometry.primary, true);
        let inspector = geometry.inspector.expect("wide inspector");

        assert_eq!(transcript.x, geometry.primary.x + TRANSCRIPT_SIDE_GUTTER);
        // Dock open: only left gutter; stream runs to the primary edge so the
        // dock channel owns the separation.
        assert_eq!(
            transcript.width,
            geometry
                .primary
                .width
                .saturating_sub(TRANSCRIPT_SIDE_GUTTER)
        );
        assert_eq!(transcript.right(), geometry.primary.right());
        // Top/bottom match dock panel frame inset so stream and plan/plugin
        // chrome read as one horizontal band.
        assert_eq!(
            transcript.y,
            geometry
                .primary
                .y
                .saturating_add(TRANSCRIPT_DOCK_VERTICAL_INSET)
        );
        assert_eq!(
            transcript.height,
            geometry
                .primary
                .height
                .saturating_sub(TRANSCRIPT_DOCK_VERTICAL_INSET.saturating_mul(2))
        );
        assert_eq!(
            transcript.bottom(),
            inspector
                .bottom()
                .saturating_sub(TRANSCRIPT_DOCK_VERTICAL_INSET)
        );
        assert!(transcript.right() < inspector.x);
        assert_eq!(
            inspector.x.saturating_sub(geometry.primary.right()),
            INSPECTOR_GAP
        );
        // Scrollbar is centered in the dock channel (2+1+2).
        let sb = centered_scrollbar_x(geometry.primary.right(), inspector.x).expect("channel");
        assert_eq!(sb.saturating_sub(geometry.primary.right()), 2);
        assert_eq!(inspector.x.saturating_sub(sb.saturating_add(1)), 2);
    }

    #[test]
    fn transcript_without_dock_stays_full_height() {
        let geometry =
            compose_route(Rect::new(0, 0, 100, 30), false, 1).expect("supported viewport");
        let transcript = transcript_column_with_dock(geometry.primary, false);
        assert_eq!(transcript.y, geometry.primary.y);
        assert_eq!(transcript.height, geometry.primary.height);
    }
}
