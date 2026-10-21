//! Clip-safe expanded artifact rows (diff / code / terminal / plain).

use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};
use unicode_width::UnicodeWidthStr;

use crate::tui_v2::{
    components::primitive::text_style::truncate_to_width,
    model::capability::{CapabilityProfile, GlyphMode},
    presentation::{
        syntax::SyntaxRole,
        theme::SemanticTheme,
        tool::{ArtifactLine, ArtifactLineKind, ArtifactPanelKind},
    },
};

pub fn render_panel_row(
    frame: &mut Frame,
    area: Rect,
    panel_row: usize,
    panel_height: usize,
    content: &[ArtifactLine],
    panel_kind: ArtifactPanelKind,
    // Window start into `content` for live-tail terminals (0 for full panels).
    content_offset: usize,
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    if area.is_empty() {
        return;
    }

    match panel_kind {
        ArtifactPanelKind::Terminal => render_terminal_row(
            frame,
            area,
            panel_row,
            panel_height,
            content,
            content_offset,
            capability,
            theme,
        ),
        ArtifactPanelKind::AgentChat => render_agent_chat_row(
            frame,
            area,
            panel_row,
            panel_height,
            content,
            content_offset,
            theme,
        ),
        ArtifactPanelKind::Diff => render_diff_row(
            frame,
            area,
            panel_row,
            panel_height,
            content,
            capability,
            theme,
        ),
        ArtifactPanelKind::Code => render_code_row(
            frame,
            area,
            panel_row,
            panel_height,
            content,
            capability,
            theme,
        ),
        ArtifactPanelKind::Generic => render_generic_row(
            frame,
            area,
            panel_row,
            panel_height,
            content,
            capability,
            theme,
        ),
    }
}

/// Scrollable mini-chat for a sub-agent (dialog + tool use), main-stream feel.
fn render_agent_chat_row(
    frame: &mut Frame,
    area: Rect,
    panel_row: usize,
    panel_height: usize,
    content: &[ArtifactLine],
    content_offset: usize,
    theme: SemanticTheme,
) {
    let bg = theme.canvas;
    // Top/bottom breath — soft divider, not a heavy terminal frame.
    if panel_row == 0 || panel_row + 1 == panel_height {
        let bar = "─".repeat(usize::from(area.width));
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().fg(theme.border).bg(bg)),
            area,
        );
        return;
    }
    let body_index = content_offset + panel_row.saturating_sub(1);
    let line = content.get(body_index);
    let (fg, style_mod) = match line.map(|line| line.kind) {
        Some(ArtifactLineKind::Add) => (theme.success, Modifier::empty()),
        Some(ArtifactLineKind::Remove) => (theme.error, Modifier::empty()),
        Some(ArtifactLineKind::Header) => (theme.thinking, Modifier::empty()),
        Some(ArtifactLineKind::Meta) => (theme.foreground_muted, Modifier::ITALIC),
        _ => (theme.foreground, Modifier::empty()),
    };
    let text = line.map(|line| line.text.as_str()).unwrap_or("");
    let body = truncate_to_width(text, usize::from(area.width), "…");
    frame.render_widget(
        Paragraph::new(body).style(Style::default().fg(fg).bg(bg).add_modifier(style_mod)),
        area,
    );
}

/// Thinking body: full plain text, no tool chrome — just readable stream.
pub fn render_thinking_row(frame: &mut Frame, area: Rect, line: &str, theme: SemanticTheme) {
    if area.is_empty() {
        return;
    }
    let text = truncate_to_width(line, usize::from(area.width), "…");
    frame.render_widget(
        Paragraph::new(text).style(
            Style::default()
                .fg(theme.foreground_muted)
                .bg(theme.canvas)
                .add_modifier(Modifier::ITALIC),
        ),
        area,
    );
}

fn render_terminal_row(
    frame: &mut Frame,
    area: Rect,
    panel_row: usize,
    panel_height: usize,
    content: &[ArtifactLine],
    content_offset: usize,
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    let (left, right, horizontal) = frame_chars(capability);
    let bg = theme.code_surface;
    if panel_row == 0 || panel_row + 1 == panel_height {
        let bar = horizontal.repeat(usize::from(area.width));
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().fg(theme.border).bg(bg)),
            area,
        );
        return;
    }
    let body_index = content_offset + panel_row.saturating_sub(1);
    let line = content.get(body_index);
    let (fg, text) = match line.map(|line| line.kind) {
        Some(ArtifactLineKind::Meta) | Some(ArtifactLineKind::Header) => (
            theme.identity,
            line.map(|line| line.text.as_str()).unwrap_or(""),
        ),
        _ => (
            theme.foreground,
            line.map(|line| line.text.as_str()).unwrap_or(""),
        ),
    };
    let inner_width = usize::from(area.width.saturating_sub(2));
    let body = truncate_to_width(text, inner_width, "…");
    let padding = " ".repeat(inner_width.saturating_sub(UnicodeWidthStr::width(body.as_str())));
    let symbol = format!("{left}{body}{padding}{right}");
    frame.render_widget(
        Paragraph::new(symbol).style(Style::default().fg(fg).bg(bg)),
        area,
    );
}

fn render_diff_row(
    frame: &mut Frame,
    area: Rect,
    panel_row: usize,
    panel_height: usize,
    content: &[ArtifactLine],
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    let (left, right, horizontal) = frame_chars(capability);
    if panel_row == 0 || panel_row + 1 == panel_height {
        let bar = horizontal.repeat(usize::from(area.width));
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().fg(theme.border).bg(theme.canvas)),
            area,
        );
        return;
    }
    let line = content.get(panel_row.saturating_sub(1));
    let (fallback_fg, bg) = match line.map(|line| line.kind) {
        Some(ArtifactLineKind::Add) => (theme.diff_add, theme.diff_add_surface),
        Some(ArtifactLineKind::Remove) => (theme.diff_remove, theme.diff_remove_surface),
        Some(ArtifactLineKind::Header) => (theme.identity, theme.canvas),
        Some(ArtifactLineKind::Meta) => (theme.foreground_muted, theme.canvas),
        _ => (theme.foreground, theme.canvas),
    };
    paint_framed_line(
        frame,
        area,
        line,
        left,
        right,
        fallback_fg,
        bg,
        theme,
        /* prefer_syntax */ true,
    );
}

fn render_code_row(
    frame: &mut Frame,
    area: Rect,
    panel_row: usize,
    panel_height: usize,
    content: &[ArtifactLine],
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    let (left, right, horizontal) = frame_chars(capability);
    let bg = theme.code_surface;
    if panel_row == 0 || panel_row + 1 == panel_height {
        let bar = horizontal.repeat(usize::from(area.width));
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().fg(theme.border).bg(bg)),
            area,
        );
        return;
    }
    let line = content.get(panel_row.saturating_sub(1));
    let (fallback_fg, modifier) = match line.map(|line| line.kind) {
        Some(ArtifactLineKind::Header) => (theme.identity, Modifier::BOLD),
        Some(ArtifactLineKind::Meta) => (theme.foreground_muted, Modifier::empty()),
        _ => (theme.foreground, Modifier::empty()),
    };
    let use_syntax = matches!(line.map(|line| line.kind), Some(ArtifactLineKind::Plain));
    if use_syntax {
        paint_framed_line(frame, area, line, left, right, fallback_fg, bg, theme, true);
    } else {
        let text = line.map(|line| line.text.as_str()).unwrap_or("");
        let inner_width = usize::from(area.width.saturating_sub(2));
        let body = truncate_to_width(text, inner_width, "…");
        let padding = " ".repeat(inner_width.saturating_sub(UnicodeWidthStr::width(body.as_str())));
        let symbol = format!("{left}{body}{padding}{right}");
        frame.render_widget(
            Paragraph::new(symbol).style(
                Style::default()
                    .fg(fallback_fg)
                    .bg(bg)
                    .add_modifier(modifier),
            ),
            area,
        );
    }
}

fn render_generic_row(
    frame: &mut Frame,
    area: Rect,
    panel_row: usize,
    panel_height: usize,
    content: &[ArtifactLine],
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    let (left, right, horizontal) = frame_chars(capability);
    if panel_row == 0 || panel_row + 1 == panel_height {
        let bar = horizontal.repeat(usize::from(area.width));
        frame.render_widget(
            Paragraph::new(bar).style(Style::default().fg(theme.border).bg(theme.canvas)),
            area,
        );
        return;
    }
    let text = content
        .get(panel_row.saturating_sub(1))
        .map(|line| line.text.as_str())
        .unwrap_or("");
    let inner_width = usize::from(area.width.saturating_sub(2));
    let body = truncate_to_width(text, inner_width, "…");
    let padding = " ".repeat(inner_width.saturating_sub(UnicodeWidthStr::width(body.as_str())));
    let symbol = format!("{left}{body}{padding}{right}");
    frame.render_widget(
        Paragraph::new(symbol).style(Style::default().fg(theme.foreground).bg(theme.code_surface)),
        area,
    );
}

fn paint_framed_line(
    frame: &mut Frame,
    area: Rect,
    line: Option<&ArtifactLine>,
    left: &str,
    right: &str,
    fallback_fg: ratatui::style::Color,
    bg: ratatui::style::Color,
    theme: SemanticTheme,
    prefer_syntax: bool,
) {
    let inner_width = usize::from(area.width.saturating_sub(2));
    let Some(line) = line else {
        let padding = " ".repeat(inner_width);
        let symbol = format!("{left}{padding}{right}");
        frame.render_widget(
            Paragraph::new(symbol).style(Style::default().fg(fallback_fg).bg(bg)),
            area,
        );
        return;
    };

    let mut spans: Vec<Span> = vec![Span::styled(
        left.to_owned(),
        Style::default().fg(theme.border).bg(bg),
    )];
    let mut used = 0usize;

    if !line.gutter.is_empty() {
        let gutter = truncate_to_width(&line.gutter, inner_width, "…");
        used = UnicodeWidthStr::width(gutter.as_str());
        let gutter_fg = match line.kind {
            ArtifactLineKind::Add => theme.diff_add,
            ArtifactLineKind::Remove => theme.diff_remove,
            _ => theme.foreground_muted,
        };
        spans.push(Span::styled(gutter, Style::default().fg(gutter_fg).bg(bg)));
    }

    let remaining = inner_width.saturating_sub(used);
    if prefer_syntax && !line.chunks.is_empty() && remaining > 0 {
        let mut room = remaining;
        for chunk in &line.chunks {
            if room == 0 {
                break;
            }
            let piece = truncate_to_width(&chunk.text, room, "…");
            let width = UnicodeWidthStr::width(piece.as_str());
            if piece.is_empty() {
                break;
            }
            spans.push(Span::styled(
                piece,
                Style::default()
                    .fg(role_color(chunk.role, theme, fallback_fg))
                    .bg(bg),
            ));
            room = room.saturating_sub(width);
            used = used.saturating_add(width);
            if width < UnicodeWidthStr::width(chunk.text.as_str()) {
                break;
            }
        }
    } else {
        let body_start = line.gutter.len().min(line.text.len());
        let body = &line.text[body_start..];
        let painted = truncate_to_width(body, remaining, "…");
        used = used.saturating_add(UnicodeWidthStr::width(painted.as_str()));
        spans.push(Span::styled(
            painted,
            Style::default().fg(fallback_fg).bg(bg),
        ));
    }

    let pad = " ".repeat(inner_width.saturating_sub(used));
    spans.push(Span::styled(pad, Style::default().bg(bg)));
    spans.push(Span::styled(
        right.to_owned(),
        Style::default().fg(theme.border).bg(bg),
    ));

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn role_color(
    role: SyntaxRole,
    theme: SemanticTheme,
    fallback: ratatui::style::Color,
) -> ratatui::style::Color {
    match role {
        SyntaxRole::Plain => fallback,
        SyntaxRole::Keyword => theme.accent,
        SyntaxRole::Function => theme.success,
        SyntaxRole::String => theme.identity,
        SyntaxRole::Number => theme.thinking,
        SyntaxRole::Comment => theme.foreground_muted,
        SyntaxRole::Type => theme.link,
        SyntaxRole::Variable => theme.foreground,
        SyntaxRole::Punctuation => theme.foreground_muted,
    }
}

fn frame_chars(capability: CapabilityProfile) -> (&'static str, &'static str, &'static str) {
    match capability.glyph_mode {
        GlyphMode::Unicode => ("│", "│", "─"),
        GlyphMode::Ascii => ("|", "|", "-"),
    }
}

/// Visible body rows inside a framed panel (excludes top/bottom frame).
pub fn panel_body_rows(panel_height: usize) -> usize {
    panel_height.saturating_sub(2)
}

/// Content window start for a live-tailing terminal panel.
pub fn terminal_content_offset(
    total_lines: usize,
    body_rows: usize,
    follow_live: bool,
    inner_scroll: u32,
) -> usize {
    if total_lines <= body_rows {
        return 0;
    }
    let max_offset = total_lines.saturating_sub(body_rows);
    if follow_live {
        max_offset
    } else {
        usize::try_from(inner_scroll)
            .unwrap_or(max_offset)
            .min(max_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_follow_live_tails() {
        assert_eq!(terminal_content_offset(100, 10, true, 0), 90);
        assert_eq!(terminal_content_offset(100, 10, false, 3), 3);
        assert_eq!(terminal_content_offset(5, 10, true, 0), 0);
    }
}
