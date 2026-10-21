//! Explicit keyboard and mouse focus ownership.

use super::{artifact::PartId, overlay::OverlayId};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ControlId(String);

impl ControlId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum FocusTarget {
    #[default]
    Composer,
    Transcript {
        part_id: PartId,
    },
    Artifact {
        part_id: PartId,
    },
    DecisionDock,
    /// Plan band inside the wide workspace dock.
    PlanDock,
    /// Plugin / game well inside the wide workspace dock.
    PluginDock,
    Overlay {
        overlay_id: OverlayId,
        control_id: ControlId,
    },
}

impl FocusTarget {
    pub const fn is_composer(&self) -> bool {
        matches!(self, Self::Composer)
    }
}
