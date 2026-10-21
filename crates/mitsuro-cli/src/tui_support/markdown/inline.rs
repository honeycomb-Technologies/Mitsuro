//! Inline content rendering to Ratatui Spans

use ratatui::style::{Modifier, Style};
use ratatui::text::Span;
use unicode_width::UnicodeWidthStr;

use super::elements::InlineContent;
use super::links::LinkSpan;
use super::MarkdownTheme;

/// Context for tracking positions during inline rendering
struct RenderContext {
    /// Current column position in display width units
    current_col: usize,
    /// Collected link spans
    links: Vec<LinkSpan>,
    /// Base line offset for this content (set by caller)
    base_line: usize,
}

/// Convert inline content to styled spans
pub fn render_inline(content: &[InlineContent], theme: &MarkdownTheme) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for item in content {
        render_inline_item(
            item,
            theme,
            Style::default().fg(theme.text_color),
            &mut spans,
        );
    }
    spans
}

fn render_inline_item(
    item: &InlineContent,
    theme: &MarkdownTheme,
    base_style: Style,
    spans: &mut Vec<Span<'static>>,
) {
    match item {
        InlineContent::Text(text) => {
            spans.push(Span::styled(text.clone(), base_style));
        }
        InlineContent::Bold(content) => {
            let style = base_style.add_modifier(Modifier::BOLD);
            for inner in content {
                render_inline_item(inner, theme, style, spans);
            }
        }
        InlineContent::Italic(content) => {
            let style = base_style.add_modifier(Modifier::ITALIC);
            for inner in content {
                render_inline_item(inner, theme, style, spans);
            }
        }
        InlineContent::Code(code) => {
            let style = Style::default()
                .fg(theme.accent_color)
                .bg(theme.code_bg_color);
            spans.push(Span::styled(format!(" {} ", code), style));
        }
        InlineContent::Link { text, url: _ } => {
            // Links are styled with underline and link color
            // OSC 8 hyperlinks are applied via apply_hyperlinks() after buffer rendering
            let style = base_style
                .fg(theme.link_color)
                .add_modifier(Modifier::UNDERLINED);

            for inner in text {
                render_inline_item(inner, theme, style, spans);
            }
        }
        InlineContent::Strikethrough(content) => {
            let style = base_style.add_modifier(Modifier::CROSSED_OUT);
            for inner in content {
                render_inline_item(inner, theme, style, spans);
            }
        }
        InlineContent::SoftBreak => {
            spans.push(Span::raw(" "));
        }
        InlineContent::HardBreak => {
            // Hard breaks are handled at the line level
            spans.push(Span::raw(" "));
        }
    }
}

/// Convert inline content to styled spans WITH link tracking
///
/// Returns both the spans and a list of link positions for OSC 8 post-processing.
/// The base_line parameter is added to all link line indices.
pub fn render_inline_with_links(
    content: &[InlineContent],
    theme: &MarkdownTheme,
    base_line: usize,
) -> (Vec<Span<'static>>, Vec<LinkSpan>) {
    let mut ctx = RenderContext {
        current_col: 0,
        links: Vec::new(),
        base_line,
    };
    let mut spans = Vec::new();

    for item in content {
        render_inline_item_tracked(
            item,
            theme,
            Style::default().fg(theme.text_color),
            &mut spans,
            &mut ctx,
        );
    }

    (spans, ctx.links)
}

fn render_inline_item_tracked(
    item: &InlineContent,
    theme: &MarkdownTheme,
    base_style: Style,
    spans: &mut Vec<Span<'static>>,
    ctx: &mut RenderContext,
) {
    match item {
        InlineContent::Text(text) => {
            let width = UnicodeWidthStr::width(text.as_str());
            ctx.current_col += width;
            spans.push(Span::styled(text.clone(), base_style));
        }
        InlineContent::Bold(content) => {
            let style = base_style.add_modifier(Modifier::BOLD);
            for inner in content {
                render_inline_item_tracked(inner, theme, style, spans, ctx);
            }
        }
        InlineContent::Italic(content) => {
            let style = base_style.add_modifier(Modifier::ITALIC);
            for inner in content {
                render_inline_item_tracked(inner, theme, style, spans, ctx);
            }
        }
        InlineContent::Code(code) => {
            let width = UnicodeWidthStr::width(code.as_str()) + 2; // spaces
            ctx.current_col += width;
            let style = Style::default()
                .fg(theme.accent_color)
                .bg(theme.code_bg_color);
            spans.push(Span::styled(format!(" {} ", code), style));
        }
        InlineContent::Link { text, url } => {
            let start_col = ctx.current_col;
            let style = base_style
                .fg(theme.link_color)
                .add_modifier(Modifier::UNDERLINED);

            // Render link text, tracking position
            for inner in text {
                render_inline_item_tracked(inner, theme, style, spans, ctx);
            }

            // Record link span (only if it has content)
            if ctx.current_col > start_col {
                ctx.links.push(LinkSpan {
                    url: url.clone(),
                    line: ctx.base_line,
                    start_col,
                    end_col: ctx.current_col,
                });
            }
        }
        InlineContent::Strikethrough(content) => {
            let style = base_style.add_modifier(Modifier::CROSSED_OUT);
            for inner in content {
                render_inline_item_tracked(inner, theme, style, spans, ctx);
            }
        }
        InlineContent::SoftBreak => {
            ctx.current_col += 1;
            spans.push(Span::raw(" "));
        }
        InlineContent::HardBreak => {
            ctx.current_col += 1;
            spans.push(Span::raw(" "));
        }
    }
}

/// Get the display width of inline content
pub fn inline_width(content: &[InlineContent]) -> usize {
    content.iter().map(item_width).sum()
}

fn item_width(item: &InlineContent) -> usize {
    match item {
        InlineContent::Text(text) => unicode_width::UnicodeWidthStr::width(text.as_str()),
        InlineContent::Bold(content)
        | InlineContent::Italic(content)
        | InlineContent::Strikethrough(content) => inline_width(content),
        InlineContent::Code(code) => unicode_width::UnicodeWidthStr::width(code.as_str()) + 2, // spaces
        InlineContent::Link { text, .. } => inline_width(text),
        InlineContent::SoftBreak | InlineContent::HardBreak => 1,
    }
}
