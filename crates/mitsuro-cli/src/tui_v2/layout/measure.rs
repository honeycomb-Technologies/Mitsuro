//! Pure text measurement with an explicitly bounded cache.

use std::{collections::HashMap, sync::Arc};

use unicode_width::UnicodeWidthChar;

use crate::{
    tui_support::markdown::RenderedMarkdown,
    tui_v2::{
        model::{artifact::PartId, capability::CapabilityProfile},
        presentation::{markdown, theme::SemanticTheme, theme::ThemeKind},
    },
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExpansionMode {
    Collapsed,
    Expanded,
    Fullscreen,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ThemeMetrics {
    pub theme: ThemeKind,
    pub horizontal_padding: u16,
    pub schema_version: u16,
}

impl ThemeMetrics {
    pub const fn new(theme: ThemeKind) -> Self {
        Self {
            theme,
            horizontal_padding: 0,
            // v6: user bubbles use 1-cell side pad + canvas fill + ~½ wrap.
            schema_version: 6,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MeasurementKey {
    pub part_id: PartId,
    pub revision: u64,
    pub width: u16,
    pub expansion: ExpansionMode,
    pub theme_metrics: ThemeMetrics,
    pub capability: CapabilityProfile,
}

pub struct MeasureRequest<'a> {
    pub key: MeasurementKey,
    pub text: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeasuredRow {
    pub text: String,
    pub source_start: usize,
    pub source_end: usize,
    /// Source byte offset for each terminal cell boundary.
    pub column_offsets: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct MeasuredPart {
    pub key: MeasurementKey,
    pub rows: Vec<MeasuredRow>,
    pub markdown: Option<RenderedMarkdown>,
    pub weight: usize,
}

impl PartialEq for MeasuredPart {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.rows == other.rows
            && self.weight == other.weight
            && self.markdown.is_some() == other.markdown.is_some()
    }
}

impl MeasuredPart {
    pub fn height(&self) -> u32 {
        self.rows.len().try_into().unwrap_or(u32::MAX)
    }

    pub fn row_for_source(&self, source_offset: usize) -> usize {
        self.rows
            .iter()
            .position(|row| {
                source_offset >= row.source_start
                    && (source_offset < row.source_end
                        || (row.source_start == row.source_end
                            && source_offset == row.source_start))
            })
            .unwrap_or_else(|| self.rows.len().saturating_sub(1))
    }
}

#[derive(Debug)]
pub struct MeasurementCache {
    entries: HashMap<MeasurementKey, CacheEntry>,
    weight: usize,
    max_entries: usize,
    max_weight: usize,
    clock: u64,
}

#[derive(Debug)]
struct CacheEntry {
    measured: Arc<MeasuredPart>,
    last_used: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MeasurementCacheStats {
    pub entries: usize,
    pub weight: usize,
    pub max_entries: usize,
    pub max_weight: usize,
}

impl MeasurementCache {
    pub fn new(max_entries: usize, max_weight: usize) -> Self {
        Self {
            entries: HashMap::new(),
            weight: 0,
            max_entries,
            max_weight,
            clock: 0,
        }
    }

    pub fn measure(&mut self, request: MeasureRequest<'_>) -> Arc<MeasuredPart> {
        let tick = self.next_tick();
        if let Some(measured) = self.cached(&request.key, tick) {
            return measured;
        }

        self.store(Arc::new(measure_uncached(request, None)), tick)
    }

    pub fn measure_markdown(
        &mut self,
        request: MeasureRequest<'_>,
        theme: SemanticTheme,
    ) -> Arc<MeasuredPart> {
        let tick = self.next_tick();
        if let Some(measured) = self.cached(&request.key, tick) {
            return measured;
        }

        let rendered = markdown::render(
            request.text,
            request.key.width,
            theme,
            request.key.capability.glyph_mode,
        );
        // Keep measured rows 1:1 with styled markdown lines. Re-wrapping
        // measurement_text can produce extra rows and panic when clip_rows
        // indexes into markdown.lines during scroll.
        self.store(
            Arc::new(measure_from_markdown_lines(request.key, rendered)),
            tick,
        )
    }

    fn cached(&mut self, key: &MeasurementKey, tick: u64) -> Option<Arc<MeasuredPart>> {
        let entry = self.entries.get_mut(key)?;
        entry.last_used = tick;
        Some(entry.measured.clone())
    }

    fn store(&mut self, measured: Arc<MeasuredPart>, tick: u64) -> Arc<MeasuredPart> {
        if self.max_entries == 0 || measured.weight > self.max_weight {
            return measured;
        }

        while self.entries.len() >= self.max_entries
            || self.weight.saturating_add(measured.weight) > self.max_weight
        {
            if !self.evict_oldest() {
                break;
            }
        }

        self.weight = self.weight.saturating_add(measured.weight);
        self.entries.insert(
            measured.key.clone(),
            CacheEntry {
                measured: measured.clone(),
                last_used: tick,
            },
        );
        measured
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub fn weight(&self) -> usize {
        self.weight
    }

    #[cfg(test)]
    pub fn stats(&self) -> MeasurementCacheStats {
        MeasurementCacheStats {
            entries: self.entries.len(),
            weight: self.weight,
            max_entries: self.max_entries,
            max_weight: self.max_weight,
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.clock = self.clock.wrapping_add(1);
        self.clock
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            return false;
        };
        if let Some(entry) = self.entries.remove(&key) {
            self.weight = self.weight.saturating_sub(entry.measured.weight);
        }
        true
    }
}

impl Default for MeasurementCache {
    fn default() -> Self {
        Self::new(8_192, 8 * 1024 * 1024)
    }
}

fn measure_from_markdown_lines(key: MeasurementKey, rendered: RenderedMarkdown) -> MeasuredPart {
    let mut rows = Vec::with_capacity(rendered.lines.len().max(1));
    let mut cursor = 0_usize;
    for line in &rendered.lines {
        let text = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();
        let end = cursor.saturating_add(text.len());
        push_row_owned(&mut rows, text, cursor, end);
        // Synthetic newline separator so source offsets stay ordered.
        cursor = end.saturating_add(1);
    }
    if rows.is_empty() {
        push_row_owned(&mut rows, String::new(), 0, 0);
    }
    let weight = rows
        .iter()
        .map(|row| row.text.len().saturating_add(row.column_offsets.len() * 8))
        .sum();
    MeasuredPart {
        key,
        rows,
        markdown: Some(rendered),
        weight,
    }
}

fn measure_uncached(
    request: MeasureRequest<'_>,
    markdown: Option<RenderedMarkdown>,
) -> MeasuredPart {
    let width = usize::from(request.key.width.max(1));
    let mut rows = Vec::new();
    let mut row_start = 0;
    let mut row_width = 0_usize;

    for (byte_index, character) in request.text.char_indices() {
        if character == '\n' {
            push_row(&mut rows, request.text, row_start, byte_index);
            row_start = byte_index + character.len_utf8();
            row_width = 0;
            continue;
        }

        let character_width = terminal_width(character);
        if row_width > 0 && row_width.saturating_add(character_width) > width {
            push_row(&mut rows, request.text, row_start, byte_index);
            row_start = byte_index;
            row_width = 0;
        }
        row_width = row_width.saturating_add(character_width);
        if row_width >= width {
            let end = byte_index + character.len_utf8();
            push_row(&mut rows, request.text, row_start, end);
            row_start = end;
            row_width = 0;
        }
    }
    if row_start < request.text.len() || request.text.ends_with('\n') || rows.is_empty() {
        push_row(&mut rows, request.text, row_start, request.text.len());
    }

    let weight = request
        .text
        .len()
        .saturating_add(rows.iter().map(|row| row.column_offsets.len() * 8).sum());
    MeasuredPart {
        key: request.key,
        rows,
        markdown,
        weight,
    }
}

fn push_row(rows: &mut Vec<MeasuredRow>, source: &str, start: usize, end: usize) {
    let end = end.min(source.len());
    let start = start.min(end);
    push_row_owned(rows, source[start..end].to_owned(), start, end);
}

fn push_row_owned(rows: &mut Vec<MeasuredRow>, text: String, start: usize, end: usize) {
    let mut column_offsets = vec![start];
    for (relative, character) in text.char_indices() {
        let character_start = start + relative;
        let character_end = character_start + character.len_utf8();
        let width = terminal_width(character);
        for _ in 1..width {
            column_offsets.push(character_start);
        }
        column_offsets.push(character_end);
    }
    rows.push(MeasuredRow {
        text,
        source_start: start,
        source_end: end,
        column_offsets,
    });
}

fn terminal_width(character: char) -> usize {
    if character == '\t' {
        4
    } else {
        UnicodeWidthChar::width(character).unwrap_or(0).max(1)
    }
}

#[cfg(test)]
mod tests {
    use crate::tui_v2::model::capability::{ColorDepth, GlyphMode};

    use super::*;

    fn request<'a>(part: &str, revision: u64, width: u16, text: &'a str) -> MeasureRequest<'a> {
        MeasureRequest {
            key: MeasurementKey {
                part_id: PartId::from_semantic(part),
                revision,
                width,
                expansion: ExpansionMode::Collapsed,
                theme_metrics: ThemeMetrics::new(ThemeKind::MitsuroDark),
                capability: CapabilityProfile {
                    glyph_mode: GlyphMode::Unicode,
                    color_depth: ColorDepth::TrueColor,
                },
            },
            text,
        }
    }

    #[test]
    fn unicode_rows_map_terminal_columns_back_to_source_bytes() {
        let mut cache = MeasurementCache::default();
        let measured = cache.measure(request("one", 1, 4, "a蟹bcd"));

        assert_eq!(measured.rows[0].text, "a蟹b");
        assert_eq!(measured.rows[0].column_offsets, vec![0, 1, 1, 4, 5]);
        assert_eq!(measured.rows[1].text, "cd");
        assert_eq!(measured.row_for_source(4), 0);
    }

    #[test]
    fn cache_is_bounded_by_entry_count_and_weight() {
        let mut cache = MeasurementCache::new(2, 100);
        cache.measure(request("one", 1, 10, "one"));
        cache.measure(request("two", 1, 10, "two"));
        cache.measure(request("three", 1, 10, "three"));

        assert_eq!(cache.len(), 2);
        assert!(cache.weight() <= 100);
    }

    #[test]
    fn cache_hits_are_constant_time_and_refresh_lru_recency() {
        let mut cache = MeasurementCache::new(2, 1_024);
        let one = cache.measure(request("one", 1, 10, "one"));
        let two = cache.measure(request("two", 1, 10, "two"));
        assert!(Arc::ptr_eq(
            &one,
            &cache.measure(request("one", 1, 10, "one"))
        ));

        cache.measure(request("three", 1, 10, "three"));
        assert!(Arc::ptr_eq(
            &one,
            &cache.measure(request("one", 1, 10, "one"))
        ));
        assert!(!Arc::ptr_eq(
            &two,
            &cache.measure(request("two", 1, 10, "two"))
        ));
        assert_eq!(
            cache.stats(),
            MeasurementCacheStats {
                entries: 2,
                weight: cache.weight(),
                max_entries: 2,
                max_weight: 1_024,
            }
        );
    }
}
