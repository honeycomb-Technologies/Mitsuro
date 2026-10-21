//! Transcript viewport geometry resolved from measured semantic rows.

use std::sync::Arc;

use ratatui::layout::Rect;

use super::{
    anchor::AnchorMode,
    measure::MeasuredPart,
    snapshot::{PartLayout, SelectionRow, TranscriptLayout},
};

pub(crate) fn layout_transcript(
    viewport: Rect,
    items: &[Arc<MeasuredPart>],
    spacing_before: &[u16],
    anchor_mode: &AnchorMode,
) -> TranscriptLayout {
    let starts = item_starts(items, spacing_before);
    let total_height = items
        .last()
        .zip(starts.last())
        .map_or(0, |(item, start)| start.saturating_add(item.height()));
    let maximum_scroll = total_height.saturating_sub(u32::from(viewport.height));
    let (scroll_top, requested_anchor) =
        resolve_scroll(items, &starts, anchor_mode, maximum_scroll);
    let visible_end = scroll_top.saturating_add(u32::from(viewport.height));
    let mut parts = Vec::new();
    let mut selection_rows = Vec::new();

    for (item, start) in items.iter().zip(starts) {
        let end = start.saturating_add(item.height());
        let visible_start = start.max(scroll_top);
        let visible_part_end = end.min(visible_end);
        if visible_start >= visible_part_end {
            continue;
        }

        let first_row = visible_start.saturating_sub(start);
        let last_row = visible_part_end.saturating_sub(start);
        let first_index = usize::try_from(first_row).unwrap_or(usize::MAX);
        let last_index = usize::try_from(last_row).unwrap_or(usize::MAX);
        append_selection_rows(
            &mut selection_rows,
            item,
            first_index..last_index.min(item.rows.len()),
            start,
            scroll_top,
            viewport.y,
        );
        parts.push(part_layout(
            item,
            viewport,
            visible_start..visible_part_end,
            scroll_top,
            first_row..last_row,
        ));
    }

    let anchor = requested_anchor.map(|(mut anchor, absolute_row)| {
        anchor.screen_row = absolute_row
            .saturating_sub(scroll_top)
            .try_into()
            .unwrap_or(u16::MAX);
        anchor
    });

    TranscriptLayout {
        viewport,
        total_height,
        scroll_top,
        parts,
        selection_rows,
        anchor,
        at_live_edge: scroll_top == maximum_scroll,
    }
}

fn resolve_scroll(
    items: &[Arc<MeasuredPart>],
    starts: &[u32],
    anchor_mode: &AnchorMode,
    maximum_scroll: u32,
) -> (u32, Option<(super::anchor::TranscriptAnchor, u32)>) {
    match anchor_mode {
        AnchorMode::FollowLive => (maximum_scroll, None),
        AnchorMode::ScrollTop(offset) => ((*offset).min(maximum_scroll), None),
        AnchorMode::Fixed(anchor) => {
            let absolute_row = items
                .iter()
                .zip(starts)
                .find(|(item, _)| item.key.part_id == anchor.part_id)
                .map(|(item, start)| {
                    start.saturating_add(
                        item.row_for_source(anchor.source_offset)
                            .try_into()
                            .unwrap_or(u32::MAX),
                    )
                })
                .unwrap_or(0);
            (
                absolute_row
                    .saturating_sub(u32::from(anchor.screen_row))
                    .min(maximum_scroll),
                Some((anchor.clone(), absolute_row)),
            )
        }
    }
}

fn append_selection_rows(
    output: &mut Vec<SelectionRow>,
    item: &MeasuredPart,
    rows: std::ops::Range<usize>,
    item_start: u32,
    scroll_top: u32,
    viewport_y: u16,
) {
    for visual_row in rows {
        let Some(row) = item.rows.get(visual_row) else {
            continue;
        };
        let absolute = item_start.saturating_add(visual_row.try_into().unwrap_or(u32::MAX));
        output.push(SelectionRow {
            screen_y: viewport_y.saturating_add(
                absolute
                    .saturating_sub(scroll_top)
                    .try_into()
                    .unwrap_or(u16::MAX),
            ),
            part_id: item.key.part_id.clone(),
            source: row.source_start..row.source_end,
            column_offsets: row.column_offsets.clone(),
        });
    }
}

fn part_layout(
    item: &MeasuredPart,
    viewport: Rect,
    visible: std::ops::Range<u32>,
    scroll_top: u32,
    clip_rows: std::ops::Range<u32>,
) -> PartLayout {
    let first_index = usize::try_from(clip_rows.start).unwrap_or(usize::MAX);
    let last_index = usize::try_from(clip_rows.end).unwrap_or(usize::MAX);
    let source_start = item.rows.get(first_index).map_or(0, |row| row.source_start);
    let source_end = item
        .rows
        .get(last_index.saturating_sub(1))
        .map_or(source_start, |row| row.source_end);
    let screen_y = viewport.y.saturating_add(
        visible
            .start
            .saturating_sub(scroll_top)
            .try_into()
            .unwrap_or(u16::MAX),
    );
    let visible_height = visible
        .end
        .saturating_sub(visible.start)
        .try_into()
        .unwrap_or(u16::MAX);

    PartLayout {
        part_id: item.key.part_id.clone(),
        revision: item.key.revision,
        full_height: item.height(),
        visible_rect: Rect::new(viewport.x, screen_y, viewport.width, visible_height),
        clip_rows,
        source_rows: source_start..source_end,
    }
}

fn item_starts(items: &[Arc<MeasuredPart>], spacing_before: &[u16]) -> Vec<u32> {
    let mut cursor = 0_u32;
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            cursor = cursor.saturating_add(u32::from(
                spacing_before.get(index).copied().unwrap_or_default(),
            ));
            let start = cursor;
            cursor = cursor.saturating_add(item.height());
            start
        })
        .collect()
}
