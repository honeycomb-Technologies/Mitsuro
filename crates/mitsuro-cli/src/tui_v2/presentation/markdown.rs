//! Width-stable Markdown presentation shared by measurement and rendering.

use std::borrow::Cow;

use ratatui::{
    style::{Color, Modifier},
    text::Line,
};
use url::Url;

use crate::tui_support::markdown::{self, MarkdownTheme, RenderedMarkdown};

use super::theme::SemanticTheme;
use crate::tui_v2::model::capability::GlyphMode;

/// Render assistant Markdown once for a specific layout width.
///
/// The terminal Markdown parser uses the active semantic palette, with external
/// links restricted to HTTP and HTTPS.
pub fn render(
    text: &str,
    width: u16,
    theme: SemanticTheme,
    glyph_mode: GlyphMode,
) -> RenderedMarkdown {
    let mut rendered =
        markdown::render_with_links(text, usize::from(width.max(1)), &markdown_theme(theme));
    rendered.links = rendered
        .links
        .into_iter()
        .filter_map(|mut link| {
            link.url = safe_external_url(&link.url)?;
            Some(link)
        })
        .collect();
    normalize_vertical_rhythm(&mut rendered, theme.identity);
    if matches!(glyph_mode, GlyphMode::Ascii) {
        use_ascii_borders(&mut rendered);
    }
    rendered
}

fn normalize_vertical_rhythm(rendered: &mut RenderedMarkdown, heading_color: Color) {
    let original = std::mem::take(&mut rendered.lines);
    let mut line_map = vec![None; original.len()];
    let mut lines = Vec::with_capacity(original.len());
    // Allow up to three blank rows between display blocks (tables/code need more
    // breath than a single tight gap). Never stack more than that.
    let mut pending_blanks: u8 = 0;
    let mut blank_source_indices: Vec<usize> = Vec::new();

    for (old_index, line) in original.into_iter().enumerate() {
        let blank = line.spans.iter().all(|span| span.content.trim().is_empty());
        if blank {
            if !lines.is_empty() {
                pending_blanks = pending_blanks.saturating_add(1).min(3);
                if blank_source_indices.len() < 3 {
                    blank_source_indices.push(old_index);
                }
            }
            continue;
        }
        for (i, _) in (0..pending_blanks).enumerate() {
            if let Some(source) = blank_source_indices.get(i).copied() {
                line_map[source] = Some(lines.len());
            }
            lines.push(Line::default());
        }
        if pending_blanks == 0
            && lines
                .last()
                .is_some_and(|line| is_heading_line(line, heading_color))
        {
            // Ensure at least one blank after a heading when the next block
            // arrived without spacing.
            lines.push(Line::default());
        }
        pending_blanks = 0;
        blank_source_indices.clear();
        line_map[old_index] = Some(lines.len());
        lines.push(line);
    }

    // Drop trailing blanks so the transcript does not grow a dead tail.
    while lines
        .last()
        .is_some_and(|line| line.spans.iter().all(|s| s.content.trim().is_empty()))
    {
        lines.pop();
    }

    rendered.lines = lines;
    rendered.links = std::mem::take(&mut rendered.links)
        .into_iter()
        .filter_map(|mut link| {
            link.line = line_map.get(link.line).copied().flatten()?;
            Some(link)
        })
        .collect();
}

fn is_heading_line(line: &Line<'_>, heading_color: Color) -> bool {
    !line.spans.is_empty()
        && line.spans.iter().all(|span| {
            span.style.fg == Some(heading_color) && span.style.add_modifier.contains(Modifier::BOLD)
        })
}

/// Plain, readable text with exactly the same rows as the styled rendering.
#[cfg(test)]
pub fn measurement_text(lines: &[Line<'_>]) -> String {
    lines
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn safe_external_url(value: &str) -> Option<String> {
    let parsed = Url::parse(value).ok()?;
    matches!(parsed.scheme(), "http" | "https").then(|| parsed.to_string())
}

fn use_ascii_borders(rendered: &mut RenderedMarkdown) {
    for line in &mut rendered.lines {
        for span in &mut line.spans {
            span.content = Cow::Owned(
                span.content
                    .chars()
                    .map(|character| match character {
                        '─' => '-',
                        '│' => '|',
                        '╭' | '╮' | '╰' | '╯' | '┬' | '┴' | '├' | '┤' | '┼' => {
                            '+'
                        }
                        '•' | '◦' | '▪' | '▫' => '-',
                        '☑' => 'x',
                        '☐' => ' ',
                        other => other,
                    })
                    .collect(),
            );
        }
    }
}

fn markdown_theme(theme: SemanticTheme) -> MarkdownTheme {
    MarkdownTheme {
        title_color: theme.identity,
        accent_color: theme.accent,
        text_color: theme.foreground,
        dim_color: theme.foreground_muted,
        code_bg_color: theme.code_surface,
        link_color: theme.link,
        syntax_keyword_color: theme.accent,
        syntax_function_color: theme.success,
        syntax_string_color: theme.identity,
        syntax_number_color: theme.thinking,
        syntax_comment_color: theme.foreground_muted,
        syntax_type_color: theme.link,
        syntax_variable_color: theme.foreground,
        syntax_punctuation_color: theme.foreground,
    }
}

#[cfg(test)]
mod tests {
    use crate::tui_v2::{
        model::capability::ColorDepth,
        presentation::theme::{SemanticTheme, ThemeKind},
    };

    use super::*;

    #[test]
    fn renders_tables_and_tracks_only_safe_external_links() {
        let theme = SemanticTheme::resolve(ThemeKind::MitsuroDark, ColorDepth::TrueColor);
        let rendered = render(
            "| Name | State |\n| --- | --- |\n| crab | ready |\n\n[docs](https://example.com/a) [local](file:///tmp/a)",
            48,
            theme,
            GlyphMode::Unicode,
        );
        let text = measurement_text(&rendered.lines);

        assert!(text.contains("Name"));
        assert!(text.contains("crab"));
        assert_eq!(rendered.links.len(), 1);
        assert_eq!(rendered.links[0].url, "https://example.com/a");
    }

    #[test]
    fn ascii_capability_replaces_every_decorative_markdown_glyph() {
        let theme = SemanticTheme::resolve(ThemeKind::MitsuroDark, ColorDepth::Ansi16);
        let rendered = render(
            "```rust\nlet crab = true;\n```\n\n- ready",
            32,
            theme,
            GlyphMode::Ascii,
        );
        let text = measurement_text(&rendered.lines);

        assert!(text.contains("let crab = true;"));
        assert!(!text
            .chars()
            .any(|character| matches!(character, '─' | '│' | '╭' | '╮' | '╰' | '╯' | '•')));
    }

    #[test]
    fn markdown_uses_intentional_gaps_without_outer_padding() {
        let theme = SemanticTheme::resolve(ThemeKind::MitsuroDark, ColorDepth::TrueColor);
        let rendered = render(
            "First paragraph.\n\n## Heading\n\nSecond paragraph.",
            48,
            theme,
            GlyphMode::Unicode,
        );
        let text = measurement_text(&rendered.lines);

        assert!(!text.starts_with('\n'));
        assert!(!text.ends_with('\n'));
        // Major block transitions may use a couple blanks; never stack four empties.
        assert!(!text.contains("\n\n\n\n\n"));
        assert!(
            text.contains("First paragraph.")
                && text.contains("Heading")
                && text.contains("Second paragraph."),
            "rendered markdown: {text:?}"
        );
        // At least one blank separates prose from the heading.
        assert!(
            text.contains("First paragraph.\n\n") || text.contains("First paragraph.\n\n\n"),
            "expected vertical breath before heading: {text:?}"
        );
    }

    #[test]
    fn code_blocks_omit_language_labels() {
        let theme = SemanticTheme::resolve(ThemeKind::MitsuroDark, ColorDepth::TrueColor);
        let rendered = render("```text\ndiagram\n```", 40, theme, GlyphMode::Unicode);
        let text = measurement_text(&rendered.lines);
        assert!(text.contains("diagram"));
        assert!(
            !text.lines().any(|line| line.trim() == "text"),
            "fence language must not render: {text:?}"
        );
    }

    #[test]
    fn diagrams_strip_orphan_pipes_without_reflowing_connectors() {
        let theme = SemanticTheme::resolve(ThemeKind::MitsuroDark, ColorDepth::TrueColor);
        // Trailing ascii pipe after unicode corner, plus a short connector row
        // that must stay short (not padded to box width).
        let rendered = render(
            "```\n┌──────┐|\n│ hi   │\n└──────┘\n   │\n```",
            40,
            theme,
            GlyphMode::Unicode,
        );
        let text = measurement_text(&rendered.lines);
        assert!(
            !text.contains("│|") && !text.contains("┐|"),
            "trailing orphan pipe must be stripped: {text:?}"
        );
        let connector = text
            .lines()
            .map(str::trim_start)
            .find(|line| *line == "│" || line.chars().all(|c| c == '│' || c == ' '))
            .expect("short connector row should remain");
        // Connector must not be expanded into a full-width padded bar.
        assert!(
            connector.trim().chars().count() <= 2,
            "connector must stay short, got {connector:?}"
        );
    }

    #[test]
    fn tables_and_diagrams_center_code_frames_left() {
        let theme = SemanticTheme::resolve(ThemeKind::MitsuroDark, ColorDepth::TrueColor);
        let width = 60_u16;
        let rendered = render(
            "| A | B |\n| - | - |\n| 1 | 2 |\n\n```bash\necho hi\n```\n\n```text\n→ flow line\n→ second\n```\n\n```\n┌───┐\n│ X │\n└───┘\n```",
            width,
            theme,
            GlyphMode::Unicode,
        );
        let text = measurement_text(&rendered.lines);
        let table_line = text
            .lines()
            .find(|line| line.contains('A') && line.contains('B') && line.contains('│'))
            .expect("table header row");
        let bash_line = text
            .lines()
            .find(|line| line.contains("echo hi"))
            .expect("bash line");
        let flow_line = text
            .lines()
            .find(|line| line.contains("flow line"))
            .expect("flow text line");
        let diagram_line = text
            .lines()
            .find(|line| line.contains('X') && (line.contains('│') || line.contains('┌')))
            .expect("diagram line");

        let table_pad = table_line.len() - table_line.trim_start().len();
        let bash_pad = bash_line.len() - bash_line.trim_start().len();
        let flow_pad = flow_line.len() - flow_line.trim_start().len();
        let diagram_pad = diagram_line.len() - diagram_line.trim_start().len();

        assert!(table_pad > 0, "table should center: {table_line:?}");
        assert!(
            diagram_pad > 0,
            "box-drawing diagrams should center: {diagram_line:?}"
        );
        // Arrow lists must not be treated as diagrams (→ alone is not box art).
        assert_eq!(
            flow_pad, 0,
            "arrow flow fences should stay left: {flow_line:?}"
        );
        assert_eq!(bash_pad, 0, "code frames stay left: {bash_line:?}");
        // Code fences get a contrast frame again.
        assert!(
            text.contains('╭') && text.contains('╰'),
            "code/flow fences should render with a border frame: {text:?}"
        );
        assert!(
            bash_line.contains('│') || bash_line.contains('|'),
            "bash body should sit inside framed rows: {bash_line:?}"
        );
    }
}
