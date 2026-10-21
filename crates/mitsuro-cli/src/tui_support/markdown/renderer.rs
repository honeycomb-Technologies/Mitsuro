//! Render markdown elements to Ratatui Lines

use once_cell::sync::Lazy;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::elements::{InlineContent, ListItem, MarkdownElement, TableCell};
use super::inline::{inline_width, render_inline, render_inline_with_links};
use super::links::{LinkSpan, RenderedMarkdown};
use super::MarkdownTheme;
use crate::tui_support::utils::highlight_code;

/// Border characters for code blocks and tables
const TOP_LEFT: char = '╭';
const TOP_RIGHT: char = '╮';
const BOTTOM_LEFT: char = '╰';
const BOTTOM_RIGHT: char = '╯';
const HORIZONTAL: char = '─';
const VERTICAL: char = '│';

/// Table junction characters
const T_DOWN: char = '┬';
const T_UP: char = '┴';
const T_RIGHT: char = '├';
const T_LEFT: char = '┤';
const CROSS: char = '┼';

/// Pre-computed horizontal border string (avoids repeated allocations)
/// Maximum expected border width is 500 chars which covers most terminals
const MAX_BORDER_WIDTH: usize = 500;
static HORIZONTAL_BORDER: Lazy<String> =
    Lazy::new(|| HORIZONTAL.to_string().repeat(MAX_BORDER_WIDTH));

/// Get a horizontal border string of the given width (sliced from cache)
#[inline]
fn horizontal_border(width: usize) -> &'static str {
    let chars = width.min(MAX_BORDER_WIDTH);
    // HORIZONTAL is a 3-byte UTF-8 char (─), so multiply by 3 for byte index
    &HORIZONTAL_BORDER[..chars * 3]
}

/// Center a fixed-width block (tables, code/diagram boxes) in the available column.
///
/// Leading spaces are pure layout — they keep measurement/render rows aligned so
/// v2 transcript clipping stays correct.
fn center_block_lines(
    block_lines: Vec<Line<'static>>,
    block_width: usize,
    available_width: usize,
) -> Vec<Line<'static>> {
    let pad = available_width.saturating_sub(block_width) / 2;
    if pad == 0 || block_lines.is_empty() {
        return block_lines;
    }
    let prefix = " ".repeat(pad);
    block_lines
        .into_iter()
        .map(|line| {
            // Keep intentional blank separators empty so vertical rhythm is unchanged.
            if line.spans.is_empty()
                || line
                    .spans
                    .iter()
                    .all(|span| span.content.chars().all(char::is_whitespace))
            {
                return line;
            }
            let mut spans = Vec::with_capacity(line.spans.len().saturating_add(1));
            spans.push(Span::raw(prefix.clone()));
            spans.extend(line.spans);
            Line::from(spans)
        })
        .collect()
}

/// Convert markdown elements to styled lines with link tracking
pub fn render_elements_with_links(
    elements: &[MarkdownElement],
    width: usize,
    theme: &MarkdownTheme,
) -> RenderedMarkdown {
    let mut lines = Vec::new();
    let mut all_links = Vec::new();

    for element in elements {
        match element {
            MarkdownElement::Paragraph(content) => {
                let base_line = lines.len();
                let (para_lines, para_links) =
                    render_paragraph_with_links(content, width, theme, base_line);
                lines.extend(para_lines);
                all_links.extend(para_links);
            }
            MarkdownElement::Heading { level, content } => {
                // Headings can have links too
                let base_line = lines.len();
                let (heading_lines, heading_links) =
                    render_heading_with_links(*level, content, width, theme, base_line);
                lines.extend(heading_lines);
                all_links.extend(heading_links);
            }
            MarkdownElement::CodeBlock { lang, code } => {
                render_code_block(&mut lines, lang.as_deref(), code, width, theme);
            }
            MarkdownElement::BlockQuote(nested) => {
                // Blockquotes need special handling for link column offsets
                let base_line = lines.len();
                let inner = render_elements_with_links(nested, width.saturating_sub(2), theme);

                for line in inner.lines {
                    let bar_style = Style::default().fg(theme.dim_color);
                    let mut new_spans = vec![Span::styled("│ ", bar_style)];
                    new_spans.extend(line.spans);
                    lines.push(Line::from(new_spans));
                }

                // Adjust link positions for the "│ " prefix (2 columns)
                for mut link in inner.links {
                    link.line += base_line;
                    link.start_col += 2;
                    link.end_col += 2;
                    all_links.push(link);
                }
            }
            MarkdownElement::List {
                ordered,
                start,
                items,
            } => {
                let base_line = lines.len();
                let (list_lines, list_links) =
                    render_list_with_links(*ordered, *start, items, width, theme, 0, base_line);
                lines.extend(list_lines);
                all_links.extend(list_links);
            }
            MarkdownElement::Table { headers, rows, .. } => {
                render_table(&mut lines, headers, rows, width, theme);
                // Note: Table cells could have links but we skip tracking for now
            }
            MarkdownElement::ThematicBreak => {
                render_thematic_break(&mut lines, width, theme);
            }
        }
    }

    RenderedMarkdown::with_links(lines, all_links)
}

fn render_paragraph_with_links(
    content: &[InlineContent],
    width: usize,
    theme: &MarkdownTheme,
    base_line: usize,
) -> (Vec<Line<'static>>, Vec<LinkSpan>) {
    // Get spans and link positions (pre-wrap positions)
    let (spans, pre_wrap_links) = render_inline_with_links(content, theme, 0);

    // Wrap spans and track how positions map to wrapped lines
    let (wrapped_lines, wrapped_links) =
        wrap_spans_with_links(spans, width, &pre_wrap_links, base_line);

    // Add blank line after paragraph
    let mut lines = wrapped_lines;
    lines.push(Line::from(""));

    (lines, wrapped_links)
}

fn render_heading_with_links(
    level: u8,
    content: &[InlineContent],
    width: usize,
    theme: &MarkdownTheme,
    base_line: usize,
) -> (Vec<Line<'static>>, Vec<LinkSpan>) {
    // Get spans and link positions
    let (mut spans, links) = render_inline_with_links(content, theme, 0);

    // Apply heading styles
    let style = match level {
        1 => Style::default()
            .fg(theme.accent_color)
            .add_modifier(Modifier::BOLD),
        2 => Style::default()
            .fg(theme.title_color)
            .add_modifier(Modifier::BOLD),
        _ => Style::default()
            .fg(theme.text_color)
            .add_modifier(Modifier::BOLD),
    };

    for span in &mut spans {
        span.style = span.style.patch(style);
    }

    // Wrap heading content to prevent overflow (base_line accounts for lead-in blanks).
    let (wrapped_lines, wrapped_links) = wrap_spans_with_links(spans, width, &links, base_line + 2);

    // Extra breath before headings so section changes read clearly.
    let mut lines = vec![Line::from(""), Line::from("")];
    lines.extend(wrapped_lines);
    lines.push(Line::from(""));

    (lines, wrapped_links)
}

fn render_code_block(
    lines: &mut Vec<Line<'static>>,
    lang: Option<&str>,
    code: &str,
    width: usize,
    theme: &MarkdownTheme,
) {
    let lang_label = lang.unwrap_or("");
    // True structural diagrams only (box drawing). Arrow lists are NOT diagrams.
    let is_diagram = looks_like_diagram(code);

    if is_diagram {
        render_diagram_block(lines, code, width, theme);
    } else {
        // Code / flow fences: left-aligned framed block for contrast.
        render_framed_code_block(lines, lang_label, code, width, theme);
    }
}

/// Structural ascii/box diagrams: centered, no outer frame (they draw their own).
fn render_diagram_block(
    lines: &mut Vec<Line<'static>>,
    code: &str,
    width: usize,
    theme: &MarkdownTheme,
) {
    let source_lines: Vec<String> = code
        .lines()
        .map(|line| clean_diagram_line(line.trim_end()))
        .collect();
    let content_width = source_lines
        .iter()
        .map(|line| line.width())
        .max()
        .unwrap_or(0)
        .max(1);
    let plain_style = Style::default().fg(theme.text_color);
    let mut block: Vec<Line<'static>> = source_lines
        .into_iter()
        .map(|line| {
            if line.is_empty() {
                Line::from("")
            } else {
                Line::from(Span::styled(line, plain_style))
            }
        })
        .collect();
    if block.is_empty() {
        block.push(Line::from(""));
    }
    ensure_block_lead_in(lines);
    lines.extend(center_block_lines(block, content_width, width));
    push_block_trail(lines);
}

/// Copyable code / arrow lists / ```text fences: left-aligned with a frame.
fn render_framed_code_block(
    lines: &mut Vec<Line<'static>>,
    lang_label: &str,
    code: &str,
    width: usize,
    theme: &MarkdownTheme,
) {
    let border_style = Style::default().fg(theme.dim_color);
    let highlighted_lines = highlight_code(code, lang_label, theme);

    let longest_line = code.lines().map(|l| l.width()).max().unwrap_or(0);
    let box_inner_width = longest_line.max(10).min(width.saturating_sub(4)).max(1);
    // Total width: │ + space + content + space + │
    let total_width = box_inner_width.saturating_add(4);

    let mut block: Vec<Line<'static>> = Vec::new();

    // Header without language label (label was noisy); plain top border only.
    let header = format!(
        "{}{}{}",
        TOP_LEFT,
        horizontal_border(total_width.saturating_sub(2)),
        TOP_RIGHT
    );
    block.push(Line::from(Span::styled(header, border_style)));

    for (raw_line, highlighted_spans) in code.lines().zip(highlighted_lines.iter()) {
        let line_width = raw_line.width();
        let mut line_spans = vec![Span::styled(format!("{} ", VERTICAL), border_style)];

        if line_width > box_inner_width {
            line_spans.push(Span::styled(
                truncate_to_display_width(raw_line, box_inner_width),
                Style::default().fg(theme.text_color),
            ));
            line_spans.push(Span::styled(format!(" {VERTICAL}"), border_style));
        } else {
            let padding = box_inner_width.saturating_sub(line_width);
            if raw_line.is_empty() {
                // empty body row
            } else {
                line_spans.extend(highlighted_spans.clone());
            }
            line_spans.push(Span::styled(
                format!("{} {VERTICAL}", " ".repeat(padding)),
                border_style,
            ));
        }
        block.push(Line::from(line_spans));
    }

    if code.lines().next().is_none() {
        block.push(Line::from(Span::styled(
            format!("{} {}{}", VERTICAL, " ".repeat(box_inner_width), VERTICAL),
            border_style,
        )));
    }

    let footer = format!(
        "{}{}{}",
        BOTTOM_LEFT,
        horizontal_border(total_width.saturating_sub(2)),
        BOTTOM_RIGHT
    );
    block.push(Line::from(Span::styled(footer, border_style)));

    ensure_block_lead_in(lines);
    // Framed code stays left-aligned with prose.
    lines.extend(block);
    push_block_trail(lines);
}

/// True box-drawing / +---+ figures only. Arrow lists (→) are regular text fences.
fn looks_like_diagram(code: &str) -> bool {
    let mut box_glyphs = 0usize;
    for c in code.chars() {
        if is_box_drawing_glyph(c) {
            box_glyphs = box_glyphs.saturating_add(1);
            if box_glyphs >= 4 {
                return true;
            }
        }
    }
    // Classic ASCII box diagrams: +---+ / |   |
    let pluses = code.bytes().filter(|b| *b == b'+').count();
    let pipes = code.bytes().filter(|b| *b == b'|').count();
    let dashes = code.bytes().filter(|b| *b == b'-').count();
    pluses >= 2 && pipes >= 2 && dashes >= 4
}

fn is_box_drawing_glyph(c: char) -> bool {
    matches!(
        c,
        '─' | '│'
            | '┌'
            | '┐'
            | '└'
            | '┘'
            | '├'
            | '┤'
            | '┬'
            | '┴'
            | '┼'
            | '╭'
            | '╮'
            | '╯'
            | '╰'
            | '═'
            | '║'
            | '╔'
            | '╗'
            | '╚'
            | '╝'
    )
}

fn is_vertical_border(c: char) -> bool {
    matches!(c, '│' | '║' | '|')
}

fn is_right_corner(c: char) -> bool {
    matches!(c, '┐' | '┘' | '┤' | '╮' | '╯' | '╗' | '╝' | '╣' | '+')
}

/// Strip common LLM double-border glitches only. Do not reflow line lengths —
/// tree diagrams need short connector rows left as-authored.
fn clean_diagram_line(line: &str) -> String {
    let mut out = line.to_owned();
    loop {
        let before = out.clone();
        if out.ends_with("│|") || out.ends_with("|│") || out.ends_with("||") {
            out.pop();
            continue;
        }
        // Trailing orphan ascii pipe after a closed unicode box edge.
        if let Some(stripped) = out.strip_suffix('|') {
            let rest = stripped.trim_end();
            if rest
                .chars()
                .last()
                .is_some_and(|c| is_vertical_border(c) || is_right_corner(c))
            {
                out = rest.to_owned();
                continue;
            }
        }
        if out == before {
            break;
        }
    }
    out
}

fn truncate_to_display_width(text: &str, target_width: usize) -> String {
    let mut result = String::new();
    let mut w = 0usize;
    for c in text.chars() {
        let cw = c.width().unwrap_or(1);
        if w + cw > target_width {
            break;
        }
        result.push(c);
        w += cw;
    }
    result
}

/// Render list with link tracking
fn render_list_with_links(
    ordered: bool,
    start: Option<u64>,
    items: &[ListItem],
    width: usize,
    theme: &MarkdownTheme,
    depth: usize,
    base_line: usize,
) -> (Vec<Line<'static>>, Vec<LinkSpan>) {
    let mut lines = Vec::new();
    let mut all_links = Vec::new();

    let bullets = ['•', '◦', '▪', '▫'];
    let bullet = bullets[depth % bullets.len()];
    let indent = "  ".repeat(depth);
    let bullet_style = Style::default().fg(theme.dim_color);

    for (i, item) in items.iter().enumerate() {
        let marker = if ordered {
            let num = start.unwrap_or(1) + i as u64;
            format!("{}{}. ", indent, num)
        } else if let Some(checked) = item.checked {
            let check = if checked { "☑" } else { "☐" };
            format!("{}{} ", indent, check)
        } else {
            format!("{}{} ", indent, bullet)
        };

        let marker_width = marker.width();

        // Render item content with link tracking
        let inner =
            render_elements_with_links(&item.content, width.saturating_sub(marker_width), theme);

        // Strip trailing empty lines (paragraphs add blank lines we don't want in lists)
        let mut inner_lines = inner.lines;
        while inner_lines
            .last()
            .map(|l| l.spans.is_empty())
            .unwrap_or(false)
        {
            inner_lines.pop();
        }

        // If no content, still render the bullet
        if inner_lines.is_empty() {
            lines.push(Line::from(Span::styled(marker.clone(), bullet_style)));
            continue;
        }

        let item_base_line = base_line + lines.len();

        for (j, line) in inner_lines.iter().enumerate() {
            if j == 0 {
                let mut spans = vec![Span::styled(marker.clone(), bullet_style)];
                spans.extend(line.spans.clone());
                lines.push(Line::from(spans));
            } else {
                let prefix = " ".repeat(marker_width);
                let mut spans = vec![Span::raw(prefix)];
                spans.extend(line.spans.clone());
                lines.push(Line::from(spans));
            }
        }

        // Adjust link positions: add marker_width to columns and adjust line numbers
        for mut link in inner.links {
            link.line += item_base_line;
            link.start_col += marker_width;
            link.end_col += marker_width;
            all_links.push(link);
        }
    }

    lines.push(Line::from("")); // blank after list
    (lines, all_links)
}

fn render_table(
    lines: &mut Vec<Line<'static>>,
    headers: &[TableCell],
    rows: &[Vec<TableCell>],
    width: usize,
    theme: &MarkdownTheme,
) {
    if headers.is_empty() && rows.is_empty() {
        return;
    }

    let border_style = Style::default().fg(theme.dim_color);
    let header_style = Style::default()
        .fg(theme.text_color)
        .add_modifier(Modifier::BOLD);
    let cell_style = Style::default().fg(theme.text_color);

    // Calculate column count
    let col_count = headers
        .len()
        .max(rows.iter().map(|r| r.len()).max().unwrap_or(0));
    if col_count == 0 {
        return;
    }

    // Calculate column widths (max of header and all row cells)
    let mut col_widths: Vec<usize> = vec![0; col_count];

    for (i, cell) in headers.iter().enumerate() {
        col_widths[i] = col_widths[i].max(inline_width(&cell.content));
    }

    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_count {
                col_widths[i] = col_widths[i].max(inline_width(&cell.content));
            }
        }
    }

    // Add padding (1 space each side) and enforce minimum width
    for w in col_widths.iter_mut() {
        *w = (*w).max(3); // minimum 3 chars content width
    }

    // Check if table fits in available width, shrink if needed
    // Total width = │ + (content + 2 padding) * cols + (│ between cols) + │
    // = 1 + sum(w+2) + (cols-1) + 1 = sum(w) + 2*cols + cols
    let total_content_width: usize = col_widths.iter().sum::<usize>() + col_count * 3;
    let max_width = width.saturating_sub(2); // borders

    if total_content_width > max_width && col_count > 0 {
        // Proportionally shrink columns
        let scale = max_width as f64 / total_content_width as f64;
        for w in col_widths.iter_mut() {
            *w = ((*w as f64 * scale) as usize).max(2);
        }
    }

    // Pre-render all cells to spans
    let header_spans: Vec<Vec<Span<'static>>> = headers
        .iter()
        .map(|c| render_inline(&c.content, theme))
        .collect();

    let row_spans: Vec<Vec<Vec<Span<'static>>>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|c| render_inline(&c.content, theme))
                .collect()
        })
        .collect();

    // Build border strings
    let build_border = |left: char, mid: char, right: char| -> String {
        let mut s = String::new();
        s.push(left);
        for (i, w) in col_widths.iter().enumerate() {
            s.push_str(horizontal_border(*w + 2));
            if i < col_widths.len() - 1 {
                s.push(mid);
            }
        }
        s.push(right);
        s
    };

    let top_border = build_border(TOP_LEFT, T_DOWN, TOP_RIGHT);
    let sep_border = build_border(T_RIGHT, CROSS, T_LEFT);
    let bottom_border = build_border(BOTTOM_LEFT, T_UP, BOTTOM_RIGHT);
    let table_width = top_border.width();

    // Helper to render a data row
    // Structure: │ content  │ content  │
    let render_row = |spans_row: &[Vec<Span<'static>>], style: Style| -> Line<'static> {
        let mut line_spans: Vec<Span<'static>> = Vec::new();
        line_spans.push(Span::styled(VERTICAL.to_string(), border_style));

        for (col_idx, col_width) in col_widths.iter().enumerate() {
            let cell_spans = spans_row.get(col_idx).cloned().unwrap_or_default();

            // Calculate actual content width
            let content_width: usize = cell_spans.iter().map(|s| s.content.width()).sum();

            // Space before content
            line_spans.push(Span::raw(" "));

            // Truncate content if wider than column
            if content_width > *col_width {
                let mut remaining_width = *col_width;
                for mut span in cell_spans {
                    let span_width = span.content.width();
                    if remaining_width == 0 {
                        break;
                    }
                    if span_width <= remaining_width {
                        span.style = span.style.patch(style);
                        line_spans.push(span);
                        remaining_width -= span_width;
                    } else {
                        // Truncate this span
                        let mut truncated = String::new();
                        let mut w = 0;
                        for c in span.content.chars() {
                            let cw = c.width().unwrap_or(0);
                            if w + cw > remaining_width {
                                break;
                            }
                            truncated.push(c);
                            w += cw;
                        }
                        line_spans.push(Span::styled(truncated, span.style.patch(style)));
                        remaining_width = 0;
                    }
                }
                // No padding, just border
                line_spans.push(Span::styled(format!(" {}", VERTICAL), border_style));
            } else {
                let padding = col_width.saturating_sub(content_width);
                // Content
                for mut span in cell_spans {
                    span.style = span.style.patch(style);
                    line_spans.push(span);
                }
                // Padding after content + space + border
                line_spans.push(Span::styled(
                    format!("{} {}", " ".repeat(padding), VERTICAL),
                    border_style,
                ));
            }
        }

        Line::from(line_spans)
    };

    let mut block: Vec<Line<'static>> = Vec::new();

    // Top border
    block.push(Line::from(Span::styled(top_border, border_style)));

    // Header row
    if !headers.is_empty() {
        block.push(render_row(&header_spans, header_style));
        // Separator between header and data
        if !rows.is_empty() {
            block.push(Line::from(Span::styled(sep_border.clone(), border_style)));
        }
    }

    // Data rows with separators between them
    for (i, row) in row_spans.iter().enumerate() {
        block.push(render_row(row, cell_style));
        // Add separator between data rows (not after the last one)
        if i < row_spans.len() - 1 {
            block.push(Line::from(Span::styled(sep_border.clone(), border_style)));
        }
    }

    // Bottom border
    block.push(Line::from(Span::styled(bottom_border, border_style)));

    ensure_block_lead_in(lines);
    lines.extend(center_block_lines(block, table_width, width));
    push_block_trail(lines);
}

fn ensure_block_lead_in(lines: &mut Vec<Line<'static>>) {
    if lines.is_empty() {
        return;
    }
    // Want two blank rows before centered display blocks when possible.
    let trailing_blanks = lines
        .iter()
        .rev()
        .take_while(|line| line.spans.iter().all(|s| s.content.trim().is_empty()))
        .count();
    for _ in trailing_blanks..2 {
        lines.push(Line::from(""));
    }
}

fn push_block_trail(lines: &mut Vec<Line<'static>>) {
    // Three blank rows after tables/code so they breathe from following prose.
    lines.push(Line::from(""));
    lines.push(Line::from(""));
    lines.push(Line::from(""));
}

fn render_thematic_break(lines: &mut Vec<Line<'static>>, _width: usize, _theme: &MarkdownTheme) {
    // Just add a blank line instead of a horizontal rule
    lines.push(Line::from(""));
}

/// Wrap spans and track link positions through wrapping
///
/// Takes pre-wrap link positions and maps them to post-wrap positions,
/// splitting links that span multiple wrapped lines.
fn wrap_spans_with_links(
    spans: Vec<Span<'static>>,
    width: usize,
    pre_wrap_links: &[LinkSpan],
    base_line: usize,
) -> (Vec<Line<'static>>, Vec<LinkSpan>) {
    if width == 0 {
        // No wrapping - just adjust line numbers
        let adjusted_links: Vec<LinkSpan> = pre_wrap_links
            .iter()
            .map(|link| LinkSpan {
                url: link.url.clone(),
                line: base_line,
                start_col: link.start_col,
                end_col: link.end_col,
            })
            .collect();
        return (vec![Line::from(spans)], adjusted_links);
    }

    // Track position mapping: (pre_wrap_col) -> (line_idx, post_wrap_col)
    let mut position_map: Vec<(usize, usize)> = Vec::new();
    let mut result = Vec::new();
    let mut current_line: Vec<Span<'static>> = Vec::new();
    let mut current_width = 0;
    let mut _pre_wrap_col = 0;
    let mut line_idx = 0;

    for span in spans {
        let span_text = span.content.to_string();
        let span_width = span_text.width();

        if current_width + span_width <= width {
            // Span fits on current line
            // Map each column in this span
            for (i, c) in span_text.chars().enumerate() {
                let char_width = c.width().unwrap_or(0);
                for _ in 0..char_width {
                    position_map.push((
                        line_idx,
                        current_width
                            + span_text[..]
                                .chars()
                                .take(i)
                                .map(|ch| ch.width().unwrap_or(0))
                                .sum::<usize>(),
                    ));
                }
                _pre_wrap_col += char_width;
            }
            current_line.push(span);
            current_width += span_width;
        } else if span_width <= width {
            // Span fits on a new line
            if !current_line.is_empty() {
                result.push(Line::from(current_line));
                current_line = Vec::new();
                line_idx += 1;
            }
            // Map columns for this span on new line
            let mut col = 0;
            for c in span_text.chars() {
                let char_width = c.width().unwrap_or(0);
                for _ in 0..char_width {
                    position_map.push((line_idx, col));
                }
                col += char_width;
                _pre_wrap_col += char_width;
            }
            current_line.push(span);
            current_width = span_width;
        } else {
            // Span is too wide - simplified handling
            // For now, just track roughly where things end up
            let style = span.style;

            // Break at word boundaries
            let mut words: Vec<&str> = Vec::new();
            let mut last_end = 0;
            for (i, c) in span_text.char_indices() {
                if c.is_whitespace() {
                    if i > last_end {
                        words.push(&span_text[last_end..i]);
                    }
                    words.push(&span_text[i..i + c.len_utf8()]);
                    last_end = i + c.len_utf8();
                }
            }
            if last_end < span_text.len() {
                words.push(&span_text[last_end..]);
            }

            for word in words {
                let word_width = word.width();

                if word_width == 0 {
                    continue;
                }

                if current_width + word_width <= width {
                    // Track columns
                    for c in word.chars() {
                        let cw = c.width().unwrap_or(0);
                        for _ in 0..cw {
                            position_map.push((line_idx, current_width));
                        }
                        current_width += cw;
                        _pre_wrap_col += cw;
                    }
                    current_line.push(Span::styled(word.to_string(), style));
                } else if word_width <= width {
                    if !current_line.is_empty() {
                        result.push(Line::from(current_line));
                        current_line = Vec::new();
                        line_idx += 1;
                    }
                    current_width = 0;
                    for c in word.chars() {
                        let cw = c.width().unwrap_or(0);
                        for _ in 0..cw {
                            position_map.push((line_idx, current_width));
                        }
                        current_width += cw;
                        _pre_wrap_col += cw;
                    }
                    current_line.push(Span::styled(word.to_string(), style));
                } else {
                    // Word longer than line - character breaking
                    if !current_line.is_empty() {
                        result.push(Line::from(current_line));
                        current_line = Vec::new();
                        line_idx += 1;
                        current_width = 0;
                    }

                    let mut remaining = word;
                    while !remaining.is_empty() {
                        let available = width.saturating_sub(current_width);
                        if available == 0 {
                            if !current_line.is_empty() {
                                result.push(Line::from(current_line));
                                current_line = Vec::new();
                                line_idx += 1;
                            }
                            current_width = 0;
                            continue;
                        }

                        let mut fit_len = 0;
                        let mut fit_width = 0;
                        for c in remaining.chars() {
                            let cw = c.width().unwrap_or(0);
                            if fit_width + cw > available {
                                break;
                            }
                            fit_len += c.len_utf8();
                            fit_width += cw;
                        }

                        if fit_len == 0 && !remaining.is_empty() {
                            let c = remaining.chars().next().unwrap();
                            fit_len = c.len_utf8();
                            let _ = c.width(); // Width is tracked in loop below
                        }

                        let chunk = &remaining[..fit_len];
                        remaining = &remaining[fit_len..];

                        for c in chunk.chars() {
                            let cw = c.width().unwrap_or(0);
                            for _ in 0..cw {
                                position_map.push((line_idx, current_width));
                            }
                            current_width += cw;
                            _pre_wrap_col += cw;
                        }
                        current_line.push(Span::styled(chunk.to_string(), style));

                        if !remaining.is_empty() {
                            result.push(Line::from(current_line));
                            current_line = Vec::new();
                            line_idx += 1;
                            current_width = 0;
                        }
                    }
                }
            }
        }
    }

    if !current_line.is_empty() {
        result.push(Line::from(current_line));
    }

    if result.is_empty() {
        result.push(Line::from(""));
    }

    // Now map pre-wrap link positions to post-wrap positions
    let mut wrapped_links = Vec::new();

    for link in pre_wrap_links {
        // Find start and end positions in wrapped output
        let start_mapped = position_map.get(link.start_col);
        let end_mapped = if link.end_col > 0 {
            position_map.get(link.end_col.saturating_sub(1))
        } else {
            start_mapped
        };

        if let (Some(&(start_line, start_col)), Some(&(end_line, end_col))) =
            (start_mapped, end_mapped)
        {
            if start_line == end_line {
                // Link fits on one line
                wrapped_links.push(LinkSpan {
                    url: link.url.clone(),
                    line: base_line + start_line,
                    start_col,
                    end_col: end_col + 1, // +1 because end is inclusive in map
                });
            } else {
                // Link spans multiple lines - create multiple LinkSpans
                // First line: from start_col to end of line
                wrapped_links.push(LinkSpan {
                    url: link.url.clone(),
                    line: base_line + start_line,
                    start_col,
                    end_col: width, // to end of line
                });

                // Middle lines: full width
                for mid_line in (start_line + 1)..end_line {
                    wrapped_links.push(LinkSpan {
                        url: link.url.clone(),
                        line: base_line + mid_line,
                        start_col: 0,
                        end_col: width,
                    });
                }

                // Last line: from start to end_col
                wrapped_links.push(LinkSpan {
                    url: link.url.clone(),
                    line: base_line + end_line,
                    start_col: 0,
                    end_col: end_col + 1,
                });
            }
        }
    }

    (result, wrapped_links)
}
