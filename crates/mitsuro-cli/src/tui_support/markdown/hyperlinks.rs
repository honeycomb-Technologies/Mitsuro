//! OSC 8 hyperlink buffer post-processing
//!
//! Applies OSC 8 escape sequences to buffer cells after normal rendering.
//! This avoids width calculation issues since the layout is already complete.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;

use super::links::LinkSpan;

/// OSC 8 hyperlink start sequence
/// Format: ESC ] 8 ; ; URL BEL
#[inline]
fn osc8_start(url: &str) -> String {
    format!("\x1b]8;;{}\x07", url)
}

/// OSC 8 hyperlink end sequence
/// Format: ESC ] 8 ; ; BEL
const OSC8_END: &str = "\x1b]8;;\x07";

/// Apply OSC 8 hyperlink sequences to buffer cells
///
/// This function should be called AFTER the Paragraph widget has rendered
/// to the buffer. Uses 2-character chunks written to alternating cells
/// as a workaround for ratatui issue #902.
///
/// # Arguments
/// * `buf` - The buffer to modify
/// * `area` - The area where markdown content was rendered
/// * `links` - Link spans with their positions (line-relative to the content)
/// * `scroll_offset` - How many lines have been scrolled (for viewport calculation)
/// * `base_line` - The starting line offset for this content block
pub fn apply_hyperlinks(
    buf: &mut Buffer,
    area: Rect,
    links: &[LinkSpan],
    scroll_offset: usize,
    base_line: usize,
) {
    for link in links {
        // Calculate the absolute line position
        let absolute_line = base_line + link.line;

        // Skip links that are scrolled past
        let visible_line = match absolute_line.checked_sub(scroll_offset) {
            Some(l) if l < area.height as usize => l,
            _ => continue,
        };

        let y = area.y + visible_line as u16;

        // Collect characters and their positions from the link range
        let max_link_width = link
            .end_col
            .saturating_sub(link.start_col)
            .min(area.width as usize);
        let mut chars: Vec<(u16, char)> = Vec::with_capacity(max_link_width);
        for col in link.start_col..link.end_col {
            if col >= area.width as usize {
                break;
            }
            let x = area.x + col as u16;
            if let Some(cell) = buf.cell((x, y)) {
                let symbol = cell.symbol();
                // Get first char of symbol (usually single char per cell)
                if let Some(c) = symbol.chars().next() {
                    if !c.is_whitespace() {
                        chars.push((x, c));
                    }
                }
            }
        }

        if chars.is_empty() {
            continue;
        }

        // Apply OSC 8 in 2-character chunks to alternating cells
        // This matches the ratatui hyperlink example workaround
        let osc_start = osc8_start(&link.url);
        let mut i = 0;
        while i < chars.len() {
            let (x, c1) = chars[i];
            let mut text = String::with_capacity(2);
            text.push(c1);
            if i + 1 < chars.len() {
                // Combine two characters
                text.push(chars[i + 1].1);
            }

            let mut hyperlink =
                String::with_capacity(osc_start.len() + text.len() + OSC8_END.len());
            hyperlink.push_str(&osc_start);
            hyperlink.push_str(&text);
            hyperlink.push_str(OSC8_END);
            if let Some(cell) = buf.cell_mut((x, y)) {
                cell.set_symbol(&hyperlink);
            }

            i += 2;
        }
    }
}
