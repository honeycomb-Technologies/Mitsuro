//! One-cell, capability-safe status presentation.

use ratatui::{style::Style, text::Span};

use crate::tui_v2::{
    model::capability::CapabilityProfile,
    presentation::{symbols::Symbols, theme::SemanticTheme},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusKind {
    Idle,
    Running,
    Success,
    Failed,
    AwaitingAuthority,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatusGlyph {
    pub kind: StatusKind,
    pub phase: usize,
}

impl StatusGlyph {
    pub fn span(self, capability: CapabilityProfile, theme: SemanticTheme) -> Span<'static> {
        let symbols = Symbols::for_mode(capability.glyph_mode);
        let (symbol, color) = match self.kind {
            StatusKind::Idle => (symbols.field, theme.foreground_muted),
            StatusKind::Running => (
                symbols.pulse_frames[self.phase % symbols.pulse_frames.len()],
                theme.thinking,
            ),
            StatusKind::Success => (symbols.success, theme.success),
            StatusKind::Failed => (symbols.failure, theme.error),
            StatusKind::AwaitingAuthority => (symbols.warning, theme.warning),
            StatusKind::Cancelled => (symbols.failure, theme.foreground_muted),
        };
        Span::styled(symbol, Style::default().fg(color))
    }
}
