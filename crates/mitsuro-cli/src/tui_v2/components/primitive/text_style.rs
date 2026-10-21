//! Semantic text roles and width-safe terminal truncation.

use ratatui::style::{Modifier, Style};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::tui_v2::presentation::theme::SemanticTheme;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextRole {
    Body,
    Muted,
    Label,
    Error,
    Selection,
}

impl TextRole {
    pub fn style(self, theme: SemanticTheme) -> Style {
        match self {
            Self::Body => Style::default().fg(theme.foreground),
            Self::Muted => Style::default().fg(theme.foreground_muted),
            Self::Label => Style::default()
                .fg(theme.foreground_muted)
                .add_modifier(Modifier::BOLD),
            Self::Error => Style::default().fg(theme.error),
            Self::Selection => Style::default()
                .fg(theme.foreground)
                .bg(theme.selection_surface),
        }
    }
}

pub fn truncate_to_width(value: &str, width: usize, marker: &str) -> String {
    if UnicodeWidthStr::width(value) <= width {
        return value.to_owned();
    }
    let marker_width = UnicodeWidthStr::width(marker);
    if width <= marker_width {
        return marker.chars().take(width).collect();
    }
    let target = width - marker_width;
    let mut output = String::new();
    let mut used = 0_usize;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used.saturating_add(character_width) > target {
            break;
        }
        output.push(character);
        used = used.saturating_add(character_width);
    }
    output.push_str(marker);
    output
}

pub fn window_to_width(value: &str, offset: usize, width: usize) -> String {
    let mut output = String::new();
    let mut skipped = 0_usize;
    let mut used = 0_usize;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0).max(1);
        if skipped.saturating_add(character_width) <= offset {
            skipped = skipped.saturating_add(character_width);
            continue;
        }
        if used.saturating_add(character_width) > width {
            break;
        }
        output.push(character);
        used = used.saturating_add(character_width);
    }
    output
}
