//! One action vocabulary for dispatch, help, palette, and footer hints.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use unicode_width::UnicodeWidthStr;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ActionId {
    Quit,
    Escape,
    OpenCommandPalette,
    OpenSessionPicker,
    OpenProcesses,
    OpenPlanGoal,
    ToggleSidebar,
    OpenExtensions,
    OpenConnections,
    OpenModelPicker,
    OpenThemeAppearance,
    OpenHelp,
    ToggleWorkMode,
    CycleReasoning,
    ToggleFastMode,
    TogglePermissionMode,
    ToggleComposerEditor,
    ScrollPageUp,
    ScrollPageDown,
    PreviousInteractivePart,
    NextInteractivePart,
    PreviousDecision,
    NextDecision,
    ActivateDecision,
    ApproveDecision,
    DenyDecision,
    InspectDecision,
    ActivateFocused,
    ToggleFullscreen,
    CopyFocused,
    ToggleFollowLive,
    StopProcess,
    JumpStart,
    JumpEnd,
    Submit,
    InsertNewline,
    ApplyUpdate,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ActionContext {
    Composer,
    Transcript,
    Artifact,
    DecisionDock,
    Overlay,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum KeySymbol {
    Char(char),
    Esc,
    Enter,
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct KeyChord {
    pub symbol: KeySymbol,
    pub control: bool,
    pub alt: bool,
    pub shift: bool,
}

impl KeyChord {
    pub const fn plain(symbol: KeySymbol) -> Self {
        Self {
            symbol,
            control: false,
            alt: false,
            shift: false,
        }
    }

    pub const fn control(symbol: KeySymbol) -> Self {
        Self {
            symbol,
            control: true,
            alt: false,
            shift: false,
        }
    }

    pub const fn alt(symbol: KeySymbol) -> Self {
        Self {
            symbol,
            control: false,
            alt: true,
            shift: false,
        }
    }

    pub const fn shift(symbol: KeySymbol) -> Self {
        Self {
            symbol,
            control: false,
            alt: false,
            shift: true,
        }
    }

    pub fn matches(self, event: KeyEvent) -> bool {
        let modifiers = event.modifiers;
        self.symbol.matches(event.code)
            && self.control == modifiers.contains(KeyModifiers::CONTROL)
            && self.alt == modifiers.contains(KeyModifiers::ALT)
            && self.shift == modifiers.contains(KeyModifiers::SHIFT)
    }

    pub fn label(self) -> String {
        let mut label = String::new();
        if self.control {
            label.push_str("Ctrl+");
        }
        if self.alt {
            label.push_str("Alt+");
        }
        if self.shift {
            label.push_str("Shift+");
        }
        self.symbol.push_label(&mut label);
        label
    }
}

impl KeySymbol {
    const fn matches(self, code: KeyCode) -> bool {
        matches!(
            (self, code),
            (Self::Char(expected), KeyCode::Char(actual)) if expected == actual
        ) || matches!(
            (self, code),
            (Self::Esc, KeyCode::Esc)
                | (Self::Enter, KeyCode::Enter)
                | (Self::Up, KeyCode::Up)
                | (Self::Down, KeyCode::Down)
                | (Self::Left, KeyCode::Left)
                | (Self::Right, KeyCode::Right)
                | (Self::PageUp, KeyCode::PageUp)
                | (Self::PageDown, KeyCode::PageDown)
        )
    }

    fn push_label(self, output: &mut String) {
        match self {
            Self::Char(' ') => output.push_str("Space"),
            Self::Char(character) => output.push(character.to_ascii_uppercase()),
            Self::Esc => output.push_str("Esc"),
            Self::Enter => output.push_str("Enter"),
            Self::Up => output.push_str("Up"),
            Self::Down => output.push_str("Down"),
            Self::Left => output.push_str("Left"),
            Self::Right => output.push_str("Right"),
            Self::PageUp => output.push_str("PgUp"),
            Self::PageDown => output.push_str("PgDn"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionDefinition {
    pub id: ActionId,
    pub label: &'static str,
    pub bindings: &'static [KeyChord],
    pub contexts: &'static [ActionContext],
    pub global: bool,
    pub footer_priority: Option<u8>,
}

impl ActionDefinition {
    pub fn active_in(self, context: ActionContext) -> bool {
        self.global || self.contexts.contains(&context)
    }

    pub fn primary_binding(self) -> Option<KeyChord> {
        self.bindings.first().copied()
    }
}

const ALL_CONTEXTS: &[ActionContext] = &[
    ActionContext::Composer,
    ActionContext::Transcript,
    ActionContext::Artifact,
    ActionContext::DecisionDock,
    ActionContext::Overlay,
];
const NON_TERMINAL: &[ActionContext] = &[
    ActionContext::Composer,
    ActionContext::Transcript,
    ActionContext::Artifact,
    ActionContext::DecisionDock,
    ActionContext::Overlay,
];
const TRANSCRIPT_NAV: &[ActionContext] = &[
    ActionContext::Composer,
    ActionContext::Transcript,
    ActionContext::Artifact,
];
const INTERACTIVE_PART: &[ActionContext] = &[ActionContext::Transcript, ActionContext::Artifact];
const ARTIFACT: &[ActionContext] = &[ActionContext::Artifact];
const COMPOSER: &[ActionContext] = &[ActionContext::Composer];
const DECISION_DOCK: &[ActionContext] = &[ActionContext::DecisionDock];
const NO_KEYS: &[KeyChord] = &[];

const CTRL_Q: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('q'))];
const ESC: &[KeyChord] = &[KeyChord::plain(KeySymbol::Esc)];
const CTRL_K: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('k'))];
const CTRL_O: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('o'))];
const CTRL_B: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('b'))];
const CTRL_T: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('t'))];
const CTRL_P: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('p'))];
const CTRL_COMMA: &[KeyChord] = &[KeyChord::control(KeySymbol::Char(','))];
const CTRL_G: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('g'))];
const CTRL_E: &[KeyChord] = &[KeyChord::control(KeySymbol::Char('e'))];
const PAGE_UP: &[KeyChord] = &[KeyChord::plain(KeySymbol::PageUp)];
const PAGE_DOWN: &[KeyChord] = &[KeyChord::plain(KeySymbol::PageDown)];
const ALT_UP: &[KeyChord] = &[KeyChord::alt(KeySymbol::Up)];
const ALT_DOWN: &[KeyChord] = &[KeyChord::alt(KeySymbol::Down)];
const ENTER: &[KeyChord] = &[KeyChord::plain(KeySymbol::Enter)];
const LEFT: &[KeyChord] = &[KeyChord::plain(KeySymbol::Left)];
const RIGHT: &[KeyChord] = &[KeyChord::plain(KeySymbol::Right)];
const A: &[KeyChord] = &[KeyChord::plain(KeySymbol::Char('a'))];
const D: &[KeyChord] = &[KeyChord::plain(KeySymbol::Char('d'))];
const I: &[KeyChord] = &[KeyChord::plain(KeySymbol::Char('i'))];
const F: &[KeyChord] = &[KeyChord::plain(KeySymbol::Char('f'))];
const C: &[KeyChord] = &[KeyChord::plain(KeySymbol::Char('c'))];
const SHIFT_F: &[KeyChord] = &[KeyChord::shift(KeySymbol::Char('F'))];
const S: &[KeyChord] = &[KeyChord::plain(KeySymbol::Char('s'))];
const G: &[KeyChord] = &[KeyChord::plain(KeySymbol::Char('g'))];
const SHIFT_G: &[KeyChord] = &[KeyChord::shift(KeySymbol::Char('G'))];
const NEWLINE: &[KeyChord] = &[
    KeyChord::shift(KeySymbol::Enter),
    KeyChord::alt(KeySymbol::Enter),
    KeyChord::control(KeySymbol::Char('j')),
];

pub const ACTIONS: &[ActionDefinition] = &[
    ActionDefinition {
        id: ActionId::Quit,
        label: "Quit",
        bindings: CTRL_Q,
        contexts: ALL_CONTEXTS,
        global: true,
        footer_priority: Some(0),
    },
    ActionDefinition {
        id: ActionId::Escape,
        label: "Back / interrupt",
        bindings: ESC,
        contexts: ALL_CONTEXTS,
        global: true,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::OpenCommandPalette,
        label: "Command palette",
        bindings: CTRL_K,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: Some(1),
    },
    ActionDefinition {
        id: ActionId::OpenSessionPicker,
        label: "Open sessions",
        bindings: CTRL_O,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: Some(2),
    },
    ActionDefinition {
        id: ActionId::OpenProcesses,
        label: "Processes",
        bindings: CTRL_B,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: Some(5),
    },
    ActionDefinition {
        id: ActionId::OpenPlanGoal,
        label: "Plan & Goal",
        bindings: NO_KEYS,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ToggleSidebar,
        label: "Toggle workspace sidebar",
        bindings: CTRL_T,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: Some(4),
    },
    ActionDefinition {
        id: ActionId::OpenExtensions,
        label: "Extensions",
        bindings: CTRL_P,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: Some(6),
    },
    ActionDefinition {
        id: ActionId::OpenConnections,
        label: "Connections",
        bindings: CTRL_COMMA,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: Some(4),
    },
    ActionDefinition {
        id: ActionId::OpenModelPicker,
        label: "Choose model",
        bindings: NO_KEYS,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::OpenThemeAppearance,
        label: "Theme & appearance",
        bindings: NO_KEYS,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::OpenHelp,
        label: "Keyboard help",
        bindings: NO_KEYS,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ToggleWorkMode,
        label: "Toggle Build / Plan",
        bindings: CTRL_G,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: Some(3),
    },
    ActionDefinition {
        id: ActionId::CycleReasoning,
        label: "Cycle reasoning",
        bindings: NO_KEYS,
        contexts: COMPOSER,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ToggleFastMode,
        label: "Toggle fast mode",
        bindings: NO_KEYS,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::TogglePermissionMode,
        label: "Toggle permission mode",
        bindings: NO_KEYS,
        contexts: NON_TERMINAL,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ToggleComposerEditor,
        label: "Full-screen editor",
        bindings: CTRL_E,
        contexts: COMPOSER,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ScrollPageUp,
        label: "Page up",
        bindings: PAGE_UP,
        contexts: TRANSCRIPT_NAV,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ScrollPageDown,
        label: "Page down",
        bindings: PAGE_DOWN,
        contexts: TRANSCRIPT_NAV,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::PreviousInteractivePart,
        label: "Previous artifact",
        bindings: ALT_UP,
        contexts: TRANSCRIPT_NAV,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::NextInteractivePart,
        label: "Next artifact",
        bindings: ALT_DOWN,
        contexts: TRANSCRIPT_NAV,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::PreviousDecision,
        label: "Previous choice",
        bindings: LEFT,
        contexts: DECISION_DOCK,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::NextDecision,
        label: "Next choice",
        bindings: RIGHT,
        contexts: DECISION_DOCK,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ActivateDecision,
        label: "Choose",
        bindings: ENTER,
        contexts: DECISION_DOCK,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ApproveDecision,
        label: "Approve",
        bindings: A,
        contexts: DECISION_DOCK,
        global: false,
        footer_priority: Some(1),
    },
    ActionDefinition {
        id: ActionId::DenyDecision,
        label: "Deny",
        bindings: D,
        contexts: DECISION_DOCK,
        global: false,
        footer_priority: Some(2),
    },
    ActionDefinition {
        id: ActionId::InspectDecision,
        label: "Inspect",
        bindings: I,
        contexts: DECISION_DOCK,
        global: false,
        footer_priority: Some(3),
    },
    ActionDefinition {
        id: ActionId::ActivateFocused,
        label: "Expand / collapse",
        bindings: ENTER,
        contexts: INTERACTIVE_PART,
        global: false,
        footer_priority: Some(1),
    },
    ActionDefinition {
        id: ActionId::ToggleFullscreen,
        label: "Full screen",
        bindings: F,
        contexts: INTERACTIVE_PART,
        global: false,
        footer_priority: Some(2),
    },
    ActionDefinition {
        id: ActionId::CopyFocused,
        label: "Copy",
        bindings: C,
        contexts: INTERACTIVE_PART,
        global: false,
        footer_priority: Some(3),
    },
    ActionDefinition {
        id: ActionId::ToggleFollowLive,
        label: "Follow live",
        bindings: SHIFT_F,
        contexts: ARTIFACT,
        global: false,
        footer_priority: Some(4),
    },
    ActionDefinition {
        id: ActionId::StopProcess,
        label: "Manage processes",
        bindings: S,
        contexts: ARTIFACT,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::JumpStart,
        label: "Jump to start",
        bindings: G,
        contexts: ARTIFACT,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::JumpEnd,
        label: "Jump to end",
        bindings: SHIFT_G,
        contexts: ARTIFACT,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::Submit,
        label: "Send",
        bindings: ENTER,
        contexts: COMPOSER,
        global: false,
        footer_priority: Some(1),
    },
    ActionDefinition {
        id: ActionId::InsertNewline,
        label: "New line",
        bindings: NEWLINE,
        contexts: COMPOSER,
        global: false,
        footer_priority: None,
    },
    ActionDefinition {
        id: ActionId::ApplyUpdate,
        label: "Install update",
        bindings: NO_KEYS,
        contexts: ALL_CONTEXTS,
        global: true,
        footer_priority: None,
    },
];

pub struct ActionRegistry;

impl ActionRegistry {
    pub fn definition(id: ActionId) -> Option<&'static ActionDefinition> {
        ACTIONS.iter().find(|definition| definition.id == id)
    }

    pub fn action_for_key(context: ActionContext, key: KeyEvent) -> Option<ActionId> {
        ACTIONS
            .iter()
            .find(|definition| {
                definition.active_in(context)
                    && definition
                        .bindings
                        .iter()
                        .any(|binding| binding.matches(key))
            })
            .map(|definition| definition.id)
    }

    pub fn active(context: ActionContext) -> impl Iterator<Item = &'static ActionDefinition> {
        ACTIONS
            .iter()
            .filter(move |definition| definition.active_in(context))
    }

    pub fn footer_hints_with_separator(
        context: ActionContext,
        max_width: usize,
        separator: &str,
    ) -> String {
        let mut definitions = Self::active(context)
            .filter_map(|definition| {
                definition
                    .footer_priority
                    .map(|priority| (priority, definition))
            })
            .collect::<Vec<_>>();
        definitions.sort_by_key(|(priority, _)| *priority);

        let mut output = String::new();
        for (_, definition) in definitions {
            let Some(binding) = definition.primary_binding() else {
                continue;
            };
            let hint = format!(
                "{} {}",
                binding.label(),
                definition.label.to_ascii_lowercase()
            );
            let separator = if output.is_empty() { "" } else { separator };
            if UnicodeWidthStr::width(output.as_str())
                + UnicodeWidthStr::width(separator)
                + UnicodeWidthStr::width(hint.as_str())
                > max_width
            {
                continue;
            }
            output.push_str(separator);
            output.push_str(&hint);
        }
        output
    }

    #[cfg(test)]
    fn conflicts() -> Vec<(ActionId, ActionId, KeyChord)> {
        let mut conflicts = Vec::new();
        for (index, left) in ACTIONS.iter().enumerate() {
            for right in &ACTIONS[index + 1..] {
                if !contexts_overlap(left, right) {
                    continue;
                }
                for binding in left.bindings {
                    if right.bindings.contains(binding) {
                        conflicts.push((left.id, right.id, *binding));
                    }
                }
            }
        }
        conflicts
    }
}

#[cfg(test)]
fn contexts_overlap(left: &ActionDefinition, right: &ActionDefinition) -> bool {
    left.global
        || right.global
        || left
            .contexts
            .iter()
            .any(|context| right.contexts.contains(context))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_keymap_has_no_contextual_conflicts() {
        assert_eq!(ActionRegistry::conflicts(), Vec::new());
    }

    #[test]
    fn dispatch_help_and_footer_share_the_same_definition() {
        let event = KeyEvent::new(KeyCode::Char('k'), KeyModifiers::CONTROL);
        assert_eq!(
            ActionRegistry::action_for_key(ActionContext::Composer, event),
            Some(ActionId::OpenCommandPalette)
        );

        let definition = ActionRegistry::active(ActionContext::Composer)
            .find(|definition| definition.id == ActionId::OpenCommandPalette)
            .expect("palette action should be available");
        assert_eq!(definition.label, "Command palette");
        assert!(
            ActionRegistry::footer_hints_with_separator(ActionContext::Composer, 80, " · ")
                .contains("Ctrl+K command palette")
        );
    }

    #[test]
    fn plain_typing_is_not_claimed_by_global_shortcuts() {
        let event = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
        assert_eq!(
            ActionRegistry::action_for_key(ActionContext::Composer, event),
            None
        );
    }

    #[test]
    fn footer_uses_only_complete_hints_that_fit() {
        assert_eq!(
            ActionRegistry::footer_hints_with_separator(ActionContext::Composer, 12, " · "),
            "Ctrl+Q quit"
        );
        assert!(
            !ActionRegistry::footer_hints_with_separator(ActionContext::Composer, 20, " · ")
                .ends_with('·')
        );
    }
}
