//! Word-like multi-line composer buffer.
//!
//! Ports the classic `MultiLineInput` capabilities into a pure, testable model:
//! soft-wrap layout, sticky-column up/down, viewport scrolling, click mapping,
//! and byte-range selection. Width is supplied by layout each frame.

use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

/// Multi-line editor buffer for the conversation composer.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ComposerBuffer {
    content: String,
    cursor: usize,
    /// First visible wrapped line.
    viewport_offset: usize,
    /// Sticky visual column for up/down (Unicode display cells).
    preferred_column: usize,
    /// Optional selection as absolute byte offsets (unordered endpoints).
    selection: Option<(usize, usize)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrappedLine {
    pub text: String,
    /// Absolute byte start in `content` for the first char of this visual line.
    pub src_start: usize,
    /// Absolute byte end (exclusive) for this visual line's content.
    pub src_end: usize,
    /// True when this line ended because of an explicit `\n`.
    pub hard_break: bool,
}

impl ComposerBuffer {
    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn cursor(&self) -> usize {
        self.cursor.min(self.content.len())
    }

    pub fn set_cursor(&mut self, byte: usize) {
        self.cursor = self
            .content
            .floor_char_boundary(byte.min(self.content.len()));
        self.sync_preferred_column(self.wrap_width_hint());
    }

    pub fn viewport_offset(&self) -> usize {
        self.viewport_offset
    }

    pub fn set_viewport_offset(&mut self, offset: usize, width: usize, visible_rows: usize) {
        let lines = self.wrapped_lines(width.max(1));
        let max_off = lines.len().saturating_sub(visible_rows.max(1));
        self.viewport_offset = offset.min(max_off);
    }

    pub fn selection(&self) -> Option<(usize, usize)> {
        self.selection
            .map(|(a, b)| if a <= b { (a, b) } else { (b, a) })
    }

    pub fn set_selection(&mut self, start: usize, end: usize) {
        let start = self
            .content
            .floor_char_boundary(start.min(self.content.len()));
        let end = self
            .content
            .floor_char_boundary(end.min(self.content.len()));
        self.selection = Some((start, end));
        self.cursor = end;
    }

    pub fn clear_selection(&mut self) {
        self.selection = None;
    }

    pub fn take_content(&mut self) -> String {
        self.cursor = 0;
        self.viewport_offset = 0;
        self.preferred_column = 0;
        self.selection = None;
        std::mem::take(&mut self.content)
    }

    pub fn clear(&mut self) {
        self.content.clear();
        self.cursor = 0;
        self.viewport_offset = 0;
        self.preferred_column = 0;
        self.selection = None;
    }

    pub fn insert_str(&mut self, text: &str) {
        self.delete_selection_if_any();
        let at = self.cursor();
        self.content.insert_str(at, text);
        self.cursor = at.saturating_add(text.len());
        self.sync_preferred_column(self.wrap_width_hint());
    }

    pub fn backspace(&mut self) {
        if self.delete_selection_if_any() {
            return;
        }
        let cursor = self.cursor();
        if cursor == 0 {
            return;
        }
        let prev = self.content[..cursor]
            .char_indices()
            .next_back()
            .map_or(0, |(i, _)| i);
        self.content.drain(prev..cursor);
        self.cursor = prev;
        self.sync_preferred_column(self.wrap_width_hint());
    }

    pub fn delete_previous_word(&mut self) {
        if self.delete_selection_if_any() {
            return;
        }
        let mut start = self.cursor();
        while let Some((index, ch)) = self.content[..start].char_indices().next_back() {
            if !ch.is_whitespace() {
                break;
            }
            start = index;
        }
        while let Some((index, ch)) = self.content[..start].char_indices().next_back() {
            if ch.is_whitespace() {
                break;
            }
            start = index;
        }
        let cursor = self.cursor();
        if start < cursor {
            self.content.drain(start..cursor);
            self.cursor = start;
            self.sync_preferred_column(self.wrap_width_hint());
        }
    }

    pub fn clear_to_line_start(&mut self, width: usize) {
        let lines = self.wrapped_lines(width);
        let (line, _) = self.visual_cursor(&lines);
        let start = lines.get(line).map(|l| l.src_start).unwrap_or(0);
        let cursor = self.cursor();
        if start < cursor {
            self.content.drain(start..cursor);
            self.cursor = start;
            self.sync_preferred_column(width);
        }
    }

    pub fn move_left(&mut self, width: usize) {
        self.clear_selection();
        let cursor = self.cursor();
        if cursor == 0 {
            return;
        }
        let prev = self.content[..cursor]
            .char_indices()
            .next_back()
            .map_or(0, |(i, _)| i);
        self.cursor = prev;
        self.sync_preferred_column(width);
        self.ensure_cursor_visible(width, 4);
    }

    pub fn move_right(&mut self, width: usize) {
        self.clear_selection();
        let cursor = self.cursor();
        if let Some(ch) = self.content[cursor..].chars().next() {
            self.cursor = cursor + ch.len_utf8();
            self.sync_preferred_column(width);
            self.ensure_cursor_visible(width, 4);
        }
    }

    pub fn move_up(&mut self, width: usize, visible_rows: usize) {
        self.clear_selection();
        let lines = self.wrapped_lines(width);
        let (line, _) = self.visual_cursor(&lines);
        if line == 0 {
            return;
        }
        self.set_cursor_visual_line(&lines, line - 1, self.preferred_column);
        self.ensure_cursor_visible(width, visible_rows);
    }

    pub fn move_down(&mut self, width: usize, visible_rows: usize) {
        self.clear_selection();
        let lines = self.wrapped_lines(width);
        let (line, _) = self.visual_cursor(&lines);
        if line + 1 >= lines.len() {
            return;
        }
        self.set_cursor_visual_line(&lines, line + 1, self.preferred_column);
        self.ensure_cursor_visible(width, visible_rows);
    }

    pub fn move_line_start(&mut self, width: usize) {
        self.clear_selection();
        let lines = self.wrapped_lines(width);
        let (line, _) = self.visual_cursor(&lines);
        if let Some(row) = lines.get(line) {
            self.cursor = row.src_start;
            self.preferred_column = 0;
            self.ensure_cursor_visible(width, 4);
        }
    }

    pub fn move_line_end(&mut self, width: usize) {
        self.clear_selection();
        let lines = self.wrapped_lines(width);
        let (line, _) = self.visual_cursor(&lines);
        if let Some(row) = lines.get(line) {
            // End of visual line content (before hard newline if any).
            self.cursor = row.src_end;
            if row.hard_break && self.cursor > row.src_start {
                // src_end points at newline; sit before it.
                self.cursor = self.content[..self.cursor]
                    .char_indices()
                    .next_back()
                    .map_or(
                        row.src_start,
                        |(i, ch)| {
                            if ch == '\n' {
                                i
                            } else {
                                self.cursor
                            }
                        },
                    );
            }
            self.sync_preferred_column(width);
            self.ensure_cursor_visible(width, 4);
        }
    }

    /// Soft-wrapped layout for the given content width.
    pub fn wrapped_lines(&self, width: usize) -> Vec<WrappedLine> {
        let width = width.max(1);
        if self.content.is_empty() {
            return vec![WrappedLine {
                text: String::new(),
                src_start: 0,
                src_end: 0,
                hard_break: false,
            }];
        }
        let mut lines = Vec::new();
        let mut text = String::new();
        let mut src_start = 0usize;
        let mut width_now = 0usize;
        let mut byte = 0usize;

        for ch in self.content.chars() {
            let ch_len = ch.len_utf8();
            if ch == '\n' {
                lines.push(WrappedLine {
                    text: std::mem::take(&mut text),
                    src_start,
                    src_end: byte + ch_len,
                    hard_break: true,
                });
                byte += ch_len;
                src_start = byte;
                width_now = 0;
                continue;
            }
            let ch_w = UnicodeWidthChar::width(ch).unwrap_or(0).max(1);
            if width_now > 0 && width_now + ch_w > width {
                lines.push(WrappedLine {
                    text: std::mem::take(&mut text),
                    src_start,
                    src_end: byte,
                    hard_break: false,
                });
                src_start = byte;
                width_now = 0;
            }
            text.push(ch);
            width_now += ch_w;
            byte += ch_len;
        }
        lines.push(WrappedLine {
            text,
            src_start,
            src_end: byte,
            hard_break: false,
        });
        if lines.is_empty() {
            lines.push(WrappedLine {
                text: String::new(),
                src_start: 0,
                src_end: 0,
                hard_break: false,
            });
        }
        lines
    }

    pub fn visual_row_count(&self, width: usize) -> usize {
        self.wrapped_lines(width).len().max(1)
    }

    /// Map a click in the visible window to an absolute byte offset.
    pub fn byte_from_click(
        &self,
        rel_col: usize,
        rel_row: usize,
        width: usize,
        visible_rows: usize,
    ) -> usize {
        let lines = self.wrapped_lines(width);
        let line_idx = self.viewport_offset.saturating_add(rel_row);
        let Some(line) = lines.get(line_idx) else {
            return self.content.len();
        };
        let mut visual = 0usize;
        let mut byte = line.src_start;
        for ch in line.text.chars() {
            let w = UnicodeWidthChar::width(ch).unwrap_or(0).max(1);
            if visual + w > rel_col {
                // Snap to nearer edge.
                if rel_col > visual + w / 2 {
                    byte += ch.len_utf8();
                }
                return byte.min(self.content.len());
            }
            visual += w;
            byte += ch.len_utf8();
        }
        let _ = visible_rows;
        line.src_end.min(self.content.len())
    }

    pub fn ensure_cursor_visible(&mut self, width: usize, visible_rows: usize) {
        let visible_rows = visible_rows.max(1);
        let lines = self.wrapped_lines(width);
        let (line, _) = self.visual_cursor(&lines);
        if line < self.viewport_offset {
            self.viewport_offset = line;
        } else if line >= self.viewport_offset + visible_rows {
            self.viewport_offset = line + 1 - visible_rows;
        }
        let max_off = lines.len().saturating_sub(visible_rows);
        self.viewport_offset = self.viewport_offset.min(max_off);
    }

    pub fn scroll_viewport(&mut self, delta: isize, width: usize, visible_rows: usize) {
        let lines = self.wrapped_lines(width);
        let max_off = lines.len().saturating_sub(visible_rows.max(1));
        let next = if delta < 0 {
            self.viewport_offset.saturating_sub(delta.unsigned_abs())
        } else {
            self.viewport_offset.saturating_add(delta as usize)
        };
        self.viewport_offset = next.min(max_off);
    }

    fn visual_cursor(&self, lines: &[WrappedLine]) -> (usize, usize) {
        let cursor = self.cursor();
        for (idx, line) in lines.iter().enumerate() {
            let at_line_end = cursor == line.src_end;
            let is_last = idx + 1 == lines.len();
            // A cursor sitting on the byte after a hard newline belongs to the
            // *next* line (column 0), not the end of the previous row. Only
            // claim the line end when this is the final row.
            if cursor < line.src_end || (at_line_end && is_last) {
                let mut col = 0usize;
                let mut b = line.src_start;
                for ch in line.text.chars() {
                    if b >= cursor {
                        break;
                    }
                    col += UnicodeWidthChar::width(ch).unwrap_or(0).max(1);
                    b += ch.len_utf8();
                }
                return (idx, col);
            }
            if at_line_end && line.hard_break && !is_last {
                continue;
            }
        }
        let last = lines.len().saturating_sub(1);
        (
            last,
            lines
                .last()
                .map(|l| UnicodeWidthStr::width(l.text.as_str()))
                .unwrap_or(0),
        )
    }

    fn set_cursor_visual_line(&mut self, lines: &[WrappedLine], line: usize, target_col: usize) {
        let Some(row) = lines.get(line) else {
            return;
        };
        let mut col = 0usize;
        let mut byte = row.src_start;
        for ch in row.text.chars() {
            let w = UnicodeWidthChar::width(ch).unwrap_or(0).max(1);
            if col + w > target_col {
                break;
            }
            col += w;
            byte += ch.len_utf8();
        }
        self.cursor = byte.min(self.content.len());
    }

    fn sync_preferred_column(&mut self, width: usize) {
        let lines = self.wrapped_lines(width.max(1));
        let (_, col) = self.visual_cursor(&lines);
        self.preferred_column = col;
    }

    fn wrap_width_hint(&self) -> usize {
        // Used only when width is not yet known; prefer a comfortable default.
        80
    }

    fn delete_selection_if_any(&mut self) -> bool {
        let Some((a, b)) = self.selection.take() else {
            return false;
        };
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        if lo >= hi {
            return false;
        }
        let lo = self.content.floor_char_boundary(lo);
        let hi = self.content.ceil_char_boundary(hi.min(self.content.len()));
        if lo < hi {
            self.content.drain(lo..hi);
            self.cursor = lo;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn up_down_keeps_sticky_column_like_word() {
        let mut buf = ComposerBuffer::default();
        buf.insert_str("hello world\nxy");
        buf.set_cursor(buf.content().len()); // end of "xy"
                                             // preferred col ~ 2
        buf.move_up(80, 4);
        // should land near column 2 of first line ("ll" area)
        assert!(buf.cursor() < "hello world".len());
        buf.move_down(80, 4);
        assert_eq!(buf.cursor(), buf.content().len());
    }

    #[test]
    fn soft_wrap_up_down_crosses_wrapped_rows() {
        let mut buf = ComposerBuffer::default();
        buf.insert_str("abcdefghijklmnopqrst");
        let width = 5;
        assert!(buf.visual_row_count(width) > 1);
        buf.move_up(width, 3);
        assert!(buf.cursor() < buf.content().len());
    }

    #[test]
    fn click_maps_into_content_bytes() {
        let mut buf = ComposerBuffer::default();
        buf.insert_str("abc\ndef");
        let byte = buf.byte_from_click(1, 1, 80, 4);
        assert_eq!(&buf.content()[byte..byte + 1], "e");
    }

    #[test]
    fn ensure_cursor_visible_pans_when_caret_pushes_past_the_window() {
        let mut buf = ComposerBuffer::default();
        let width = 20;
        let visible = 3;
        // Many hard lines so the caret leaves a 3-row window.
        for i in 0..12 {
            buf.insert_str(&format!("line-{i}\n"));
            buf.ensure_cursor_visible(width, visible);
        }
        let lines = buf.wrapped_lines(width);
        let (row, _) = buf.visual_cursor(&lines);
        assert!(
            row >= buf.viewport_offset() && row < buf.viewport_offset() + visible,
            "caret row {row} outside viewport {}..{}",
            buf.viewport_offset(),
            buf.viewport_offset() + visible
        );
    }

    #[test]
    fn manual_scroll_can_leave_caret_offscreen_until_reframe() {
        let mut buf = ComposerBuffer::default();
        for i in 0..10 {
            buf.insert_str(&format!("row {i}\n"));
        }
        buf.ensure_cursor_visible(40, 3);
        // Scroll to top while caret stays at end.
        buf.set_viewport_offset(0, 40, 3);
        let lines = buf.wrapped_lines(40);
        let (row, _) = buf.visual_cursor(&lines);
        assert!(row >= 3, "caret should still be below the window");
        buf.ensure_cursor_visible(40, 3);
        let (row, _) = buf.visual_cursor(&lines);
        assert!(row >= buf.viewport_offset() && row < buf.viewport_offset() + 3);
    }
}
