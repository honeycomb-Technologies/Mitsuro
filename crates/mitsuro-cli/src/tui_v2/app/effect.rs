//! Typed requests for work outside the pure reducer.

use crate::tui_v2::{
    app::state::DecisionAction,
    model::overlay::{OverlayId, OverlayKind},
    motion::preference::MotionPreference,
    presentation::theme::ThemeKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScrollDirection {
    Backward,
    Forward,
    Start,
    End,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScrollAmount {
    Page,
    Edge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScrollTarget {
    Transcript,
    FocusedArtifact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FocusDirection {
    Previous,
    Next,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistedUiPreference {
    Motion(MotionPreference),
    Theme(ThemeKind),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionTargetKind {
    ToolApproval,
    Questions,
    PlanConfirmation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionTarget {
    pub session_id: String,
    pub tool_call_id: String,
    pub kind: DecisionTargetKind,
}

/// Effects describe intent; service adapters remain responsible for invoking
/// canonical core behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UiEffect {
    PrepareOverlay {
        id: OverlayId,
        kind: OverlayKind,
    },
    PersistPreference(PersistedUiPreference),
    Scroll {
        target: ScrollTarget,
        direction: ScrollDirection,
        amount: ScrollAmount,
    },
    MoveInteractiveFocus(FocusDirection),
    SubmitComposer,
    InsertComposerNewline,
    ToggleCanonicalWorkMode,
    CycleCanonicalReasoning,
    ToggleCanonicalFastMode,
    ToggleCanonicalPermissionMode,
    CopyFocused,
    InterruptAgentRun,
    ResolveDecision {
        target: DecisionTarget,
        action: DecisionAction,
    },
}
