//! Typed overlay families and lifecycle.

use super::{artifact::PartId, focus::FocusTarget};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OverlayId(u64);

impl OverlayId {
    pub(crate) const fn from_sequence(sequence: u64) -> Self {
        Self(sequence)
    }

    pub(crate) const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OverlayKind {
    CommandPalette,
    Help,
    SessionPicker,
    ModelPicker,
    Connections,
    ThemeAppearance,
    PlanGoal,
    Processes,
    ExtensionsCenter,
    FileArtifactInspector {
        part_id: PartId,
    },
    /// Composer bracket attachment preview (file path, clipboard image, etc.).
    AttachmentPreview,
}

impl OverlayKind {
    pub const fn label(&self) -> &'static str {
        match self {
            Self::CommandPalette => "Command Palette",
            Self::Help => "Help",
            Self::SessionPicker => "Sessions",
            Self::ModelPicker => "Models",
            Self::Connections => "Connections",
            Self::ThemeAppearance => "Appearance",
            Self::PlanGoal => "Plan & Goal",
            Self::Processes => "Processes",
            Self::ExtensionsCenter => "Extensions",
            Self::FileArtifactInspector { .. } => "Artifact Inspector",
            Self::AttachmentPreview => "Attachment",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OverlayPhase {
    Opening,
    #[default]
    Ready,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OverlayState {
    pub id: OverlayId,
    pub kind: OverlayKind,
    pub phase: OverlayPhase,
    pub return_focus: FocusTarget,
}
