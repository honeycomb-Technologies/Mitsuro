use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MIN_ZOOM: f64 = 0.25;
pub const MAX_ZOOM: f64 = 2.5;
pub const DETAIL_ZOOM: f64 = 0.85;
pub const SUMMARY_ZOOM: f64 = 0.55;
pub const SNAP_SCREEN_PX: f64 = 8.0;
pub const MAX_CAMERA_WORLD_ORIGIN: f64 = 10_000_000.0;

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WorldPoint {
    pub x: f64,
    pub y: f64,
}

impl WorldPoint {
    pub const fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WorldSize {
    pub width: f64,
    pub height: f64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WorldRect {
    pub origin: WorldPoint,
    pub size: WorldSize,
}

impl WorldRect {
    pub const fn new(x: f64, y: f64, width: f64, height: f64) -> Self {
        Self {
            origin: WorldPoint { x, y },
            size: WorldSize { width, height },
        }
    }

    pub fn right(self) -> f64 {
        self.origin.x + self.size.width
    }

    pub fn bottom(self) -> f64 {
        self.origin.y + self.size.height
    }

    pub fn intersects(self, other: Self) -> bool {
        self.origin.x < other.right()
            && self.right() > other.origin.x
            && self.origin.y < other.bottom()
            && self.bottom() > other.origin.y
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Camera {
    pub world_origin: WorldPoint,
    pub zoom: f64,
}

impl Default for Camera {
    fn default() -> Self {
        Self {
            world_origin: WorldPoint::default(),
            zoom: 1.0,
        }
    }
}

impl Camera {
    pub fn normalized(self) -> Self {
        let x = if self.world_origin.x.is_finite() {
            self.world_origin
                .x
                .clamp(-MAX_CAMERA_WORLD_ORIGIN, MAX_CAMERA_WORLD_ORIGIN)
        } else {
            0.0
        };
        let y = if self.world_origin.y.is_finite() {
            self.world_origin
                .y
                .clamp(-MAX_CAMERA_WORLD_ORIGIN, MAX_CAMERA_WORLD_ORIGIN)
        } else {
            0.0
        };
        let zoom = if self.zoom.is_finite() {
            self.zoom.clamp(MIN_ZOOM, MAX_ZOOM)
        } else {
            1.0
        };
        Self {
            world_origin: WorldPoint::new(x, y),
            zoom,
        }
    }

    pub fn world_to_screen(self, world: WorldPoint, viewport: WorldPoint) -> WorldPoint {
        WorldPoint::new(
            viewport.x + (world.x - self.world_origin.x) * self.zoom,
            viewport.y + (world.y - self.world_origin.y) * self.zoom,
        )
    }

    pub fn screen_to_world(self, screen: WorldPoint, viewport: WorldPoint) -> WorldPoint {
        WorldPoint::new(
            self.world_origin.x + (screen.x - viewport.x) / self.zoom,
            self.world_origin.y + (screen.y - viewport.y) / self.zoom,
        )
    }

    pub fn pan_screen_delta(&mut self, dx: f64, dy: f64) {
        self.world_origin.x -= dx / self.zoom;
        self.world_origin.y -= dy / self.zoom;
    }

    pub fn zoom_at(&mut self, requested_zoom: f64, cursor: WorldPoint, viewport: WorldPoint) {
        let anchored_world = self.screen_to_world(cursor, viewport);
        self.zoom = requested_zoom.clamp(MIN_ZOOM, MAX_ZOOM);
        self.world_origin = WorldPoint::new(
            anchored_world.x - (cursor.x - viewport.x) / self.zoom,
            anchored_world.y - (cursor.y - viewport.y) / self.zoom,
        );
    }

    pub fn fit_all(nodes: &[SpatialNode], viewport: WorldSize, padding: f64) -> Option<Self> {
        Self::fit_all_in_view(nodes, WorldPoint::default(), viewport, padding)
    }

    pub fn fit_all_in_view(
        nodes: &[SpatialNode],
        view_origin: WorldPoint,
        view_size: WorldSize,
        padding: f64,
    ) -> Option<Self> {
        let bounds = world_bounds(nodes)?;
        let usable_width = (view_size.width - padding * 2.0).max(1.0);
        let usable_height = (view_size.height - padding * 2.0).max(1.0);
        let zoom = (usable_width / bounds.size.width.max(1.0))
            .min(usable_height / bounds.size.height.max(1.0))
            .clamp(MIN_ZOOM, MAX_ZOOM);
        let center = WorldPoint::new(
            bounds.origin.x + bounds.size.width / 2.0,
            bounds.origin.y + bounds.size.height / 2.0,
        );
        let screen_center = WorldPoint::new(
            view_origin.x + view_size.width / 2.0,
            view_origin.y + view_size.height / 2.0,
        );
        Some(Self {
            world_origin: WorldPoint::new(
                center.x - screen_center.x / zoom,
                center.y - screen_center.y / zoom,
            ),
            zoom,
        })
    }

    pub fn visible_world_rect(self, viewport: WorldSize) -> WorldRect {
        WorldRect::new(
            self.world_origin.x,
            self.world_origin.y,
            viewport.width / self.zoom,
            viewport.height / self.zoom,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Agent,
    Terminal,
    Browser,
    Files,
    Changes,
}

impl NodeKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Agent => "Agent",
            Self::Terminal => "Terminal",
            Self::Browser => "Browser",
            Self::Files => "Files",
            Self::Changes => "Changes",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    #[default]
    Idle,
    Working,
    NeedsYou,
    Disconnected,
    Ended,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpatialNode {
    pub id: Uuid,
    pub kind: NodeKind,
    pub title: String,
    pub world_bounds: WorldRect,
    pub z_order: i64,
    pub status: NodeStatus,
    pub binding_id: Option<String>,
}

impl SpatialNode {
    pub fn new(kind: NodeKind, title: impl Into<String>, world_bounds: WorldRect, z: i64) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind,
            title: title.into(),
            world_bounds,
            z_order: z,
            status: NodeStatus::Idle,
            binding_id: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticLevel {
    Glance,
    Summary,
    Detail,
}

pub fn semantic_level(zoom: f64, gesture_active: bool) -> SemanticLevel {
    if gesture_active || zoom < SUMMARY_ZOOM {
        SemanticLevel::Glance
    } else if zoom < DETAIL_ZOOM {
        SemanticLevel::Summary
    } else {
        SemanticLevel::Detail
    }
}

pub fn snap_world(value: f64, grid_world: f64, zoom: f64) -> f64 {
    if grid_world <= 0.0 {
        return value;
    }
    let snapped = (value / grid_world).round() * grid_world;
    let threshold_world = SNAP_SCREEN_PX / zoom.max(MIN_ZOOM);
    if (snapped - value).abs() <= threshold_world {
        snapped
    } else {
        value
    }
}

pub fn world_bounds(nodes: &[SpatialNode]) -> Option<WorldRect> {
    let first = nodes.first()?.world_bounds;
    let mut left = first.origin.x;
    let mut top = first.origin.y;
    let mut right = first.right();
    let mut bottom = first.bottom();
    for node in &nodes[1..] {
        left = left.min(node.world_bounds.origin.x);
        top = top.min(node.world_bounds.origin.y);
        right = right.max(node.world_bounds.right());
        bottom = bottom.max(node.world_bounds.bottom());
    }
    Some(WorldRect::new(left, top, right - left, bottom - top))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageLayout {
    #[default]
    One,
    Two,
    Four,
}

impl StageLayout {
    pub const fn capacity(self) -> usize {
        match self {
            Self::One => 1,
            Self::Two => 2,
            Self::Four => 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_anchored_zoom_preserves_world_point() {
        let mut camera = Camera {
            world_origin: WorldPoint::new(-120.0, 40.0),
            zoom: 0.75,
        };
        let cursor = WorldPoint::new(612.0, 318.0);
        let before = camera.screen_to_world(cursor, WorldPoint::default());
        camera.zoom_at(1.4, cursor, WorldPoint::default());
        let after = camera.screen_to_world(cursor, WorldPoint::default());
        assert!((before.x - after.x).abs() < 0.001);
        assert!((before.y - after.y).abs() < 0.001);
    }

    #[test]
    fn persisted_camera_values_are_finite_and_product_bounded() {
        let camera = Camera {
            world_origin: WorldPoint::new(f64::INFINITY, -1.0e100),
            zoom: 0.0,
        }
        .normalized();
        assert_eq!(camera.world_origin.x, 0.0);
        assert_eq!(camera.world_origin.y, -MAX_CAMERA_WORLD_ORIGIN);
        assert_eq!(camera.zoom, MIN_ZOOM);

        let camera = Camera {
            world_origin: WorldPoint::new(f64::NAN, f64::NEG_INFINITY),
            zoom: f64::NAN,
        }
        .normalized();
        assert_eq!(camera.world_origin, WorldPoint::default());
        assert_eq!(camera.zoom, 1.0);
    }

    #[test]
    fn fit_all_supports_negative_coordinates() {
        let nodes = vec![
            SpatialNode::new(
                NodeKind::Agent,
                "A",
                WorldRect::new(-800.0, -200.0, 500.0, 400.0),
                1,
            ),
            SpatialNode::new(
                NodeKind::Terminal,
                "T",
                WorldRect::new(200.0, 300.0, 600.0, 300.0),
                2,
            ),
        ];
        let camera = Camera::fit_all(
            &nodes,
            WorldSize {
                width: 1200.0,
                height: 800.0,
            },
            48.0,
        )
        .expect("camera");
        let visible = camera.visible_world_rect(WorldSize {
            width: 1200.0,
            height: 800.0,
        });
        for node in nodes {
            assert!(visible.intersects(node.world_bounds));
        }
    }

    #[test]
    fn fit_all_in_view_centers_nodes_inside_overlay_insets() {
        let nodes = vec![SpatialNode::new(
            NodeKind::Agent,
            "A",
            WorldRect::new(-200.0, 50.0, 600.0, 300.0),
            1,
        )];
        let origin = WorldPoint::new(210.0, 62.0);
        let size = WorldSize {
            width: 1_200.0 - 210.0 - 48.0,
            height: 800.0 - 62.0 - 76.0,
        };
        let padding = 24.0;
        let camera = Camera::fit_all_in_view(&nodes, origin, size, padding).expect("camera");
        let bounds = nodes[0].world_bounds;
        let top_left = camera.world_to_screen(bounds.origin, WorldPoint::default());
        let bottom_right = camera.world_to_screen(
            WorldPoint::new(bounds.right(), bounds.bottom()),
            WorldPoint::default(),
        );
        assert!(top_left.x >= origin.x + padding - 0.001);
        assert!(top_left.y >= origin.y + padding - 0.001);
        assert!(bottom_right.x <= origin.x + size.width - padding + 0.001);
        assert!(bottom_right.y <= origin.y + size.height - padding + 0.001);
    }

    #[test]
    fn semantic_zoom_has_three_authored_levels() {
        assert_eq!(semantic_level(0.4, false), SemanticLevel::Glance);
        assert_eq!(semantic_level(0.7, false), SemanticLevel::Summary);
        assert_eq!(semantic_level(1.0, false), SemanticLevel::Detail);
        assert_eq!(semantic_level(1.0, true), SemanticLevel::Glance);
    }

    #[test]
    fn snap_threshold_stays_screen_constant() {
        assert_eq!(snap_world(19.0, 20.0, 1.0), 20.0);
        assert_eq!(snap_world(35.0, 20.0, 2.0), 35.0);
        assert_eq!(snap_world(-19.0, 20.0, 1.0), -20.0);
    }
}
