//! Visible-only transcript renderer.

use std::{collections::HashSet, sync::Arc};

use ratatui::{
    layout::{Alignment, Rect},
    style::Style,
    text::Line,
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};
use unicode_width::UnicodeWidthStr;

use crate::tui_v2::{
    app::state::UiState,
    components::primitive::{
        expandable_row::ExpandableRow,
        status_glyph::{StatusGlyph, StatusKind},
    },
    layout::{
        measure::MeasuredPart,
        snapshot::{LayoutRegionId, LayoutSnapshot, PartLayout},
    },
    model::conversation::NoticeLevel,
    motion::preference::MotionPreference,
    presentation::{
        symbols::ASCII_BORDER,
        theme::SemanticTheme,
        transcript::{ConversationDisplayList, DisplayPart, DisplayPartKind},
    },
};

use super::artifact_panel::{
    panel_body_rows, render_panel_row, render_thinking_row, terminal_content_offset,
};

pub fn render_transcript(
    frame: &mut Frame,
    layout: &LayoutSnapshot,
    state: &UiState,
    display: &ConversationDisplayList,
    measured: &[Arc<MeasuredPart>],
    theme: SemanticTheme,
) {
    // Animate every live/running row — parallel agent tools must all spin,
    // not just the most recently started one.
    let animated = display
        .parts
        .iter()
        .filter(|part| match &part.kind {
            DisplayPartKind::Tool(tool) => tool.status == StatusKind::Running,
            DisplayPartKind::Thinking { status, .. } => *status == StatusKind::Running,
            _ => false,
        })
        .map(|part| part.id.clone())
        .collect::<HashSet<_>>();
    for part_layout in &layout.transcript.parts {
        let Some(display_part) = display
            .parts
            .iter()
            .find(|part| part.id == part_layout.part_id)
        else {
            continue;
        };
        let Some(measured_part) = measured
            .iter()
            .find(|part| part.key.part_id == part_layout.part_id)
        else {
            continue;
        };
        render_part(
            frame,
            part_layout,
            display_part,
            measured_part,
            state,
            theme,
            animated.contains(&part_layout.part_id),
        );
    }
    if state.transcript.unseen_parts > 0 {
        if let Some(area) = layout.region(LayoutRegionId::NewContentIndicator) {
            let label = format!(" {} new · End to follow ", state.transcript.unseen_parts);
            frame.render_widget(
                Paragraph::new(label)
                    .alignment(ratatui::layout::Alignment::Center)
                    .style(
                        Style::default()
                            .fg(theme.foreground)
                            .bg(theme.selection_surface),
                    ),
                area,
            );
        }
    }
    apply_selection_highlight(frame, layout, state, theme);
    if let Some(area) = layout.region(LayoutRegionId::TranscriptScrollbar) {
        crate::tui_v2::components::scrollbars::render_scrollbar(
            frame,
            area,
            layout.transcript.scroll_top,
            layout.transcript.total_height,
            u32::from(layout.transcript.viewport.height),
            theme,
            state.mouse.scrollbar_drag.as_ref().is_some_and(|region| {
                matches!(
                    region,
                    crate::tui_v2::layout::snapshot::ScrollRegionId::Transcript
                )
            }),
        );
    }
}

/// Paint selection background over transcript cells using snapshot selection rows.
fn apply_selection_highlight(
    frame: &mut Frame,
    layout: &LayoutSnapshot,
    state: &UiState,
    theme: SemanticTheme,
) {
    let Some(selection) = state.mouse.selection.as_ref() else {
        return;
    };
    if selection.is_empty_range() {
        return;
    }
    let rows = &layout.transcript.selection_rows;
    let start_idx = rows.iter().position(|row| {
        row.part_id == selection.start.part_id
            && selection.start.source_offset >= row.source.start
            && selection.start.source_offset <= row.source.end
    });
    let end_idx = rows.iter().position(|row| {
        row.part_id == selection.end.part_id
            && selection.end.source_offset >= row.source.start
            && selection.end.source_offset <= row.source.end
    });
    let (mut a, mut b, start_off, end_off) = match (start_idx, end_idx) {
        (Some(s), Some(e)) => {
            if s <= e {
                (
                    s,
                    e,
                    selection.start.source_offset,
                    selection.end.source_offset,
                )
            } else {
                (
                    e,
                    s,
                    selection.end.source_offset,
                    selection.start.source_offset,
                )
            }
        }
        _ => return,
    };
    let buffer = frame.buffer_mut();
    for (index, row) in rows.iter().enumerate() {
        if index < a || index > b {
            continue;
        }
        let lo = if index == a {
            start_off.max(row.source.start)
        } else {
            row.source.start
        };
        let hi = if index == b {
            end_off.saturating_add(1).min(row.source.end).max(lo)
        } else {
            row.source.end
        };
        if lo >= hi && index == b && start_off == end_off {
            continue;
        }
        let start_col = column_for_offset(row, lo);
        let end_col = column_for_offset(row, hi).max(start_col.saturating_add(1));
        let x0 = layout
            .transcript
            .viewport
            .x
            .saturating_add(start_col as u16);
        let x1 = layout
            .transcript
            .viewport
            .x
            .saturating_add(end_col as u16)
            .min(layout.transcript.viewport.right());
        for x in x0..x1 {
            if let Some(cell) = buffer.cell_mut((x, row.screen_y)) {
                cell.set_bg(theme.selection_surface);
            }
        }
    }
    let _ = (&mut a, &mut b);
}

fn column_for_offset(row: &crate::tui_v2::layout::snapshot::SelectionRow, offset: usize) -> usize {
    if row.column_offsets.is_empty() {
        return offset.saturating_sub(row.source.start);
    }
    row.column_offsets
        .iter()
        .position(|boundary| *boundary >= offset)
        .unwrap_or(row.column_offsets.len().saturating_sub(1))
}

fn render_part(
    frame: &mut Frame,
    layout: &PartLayout,
    display: &DisplayPart,
    measured: &MeasuredPart,
    state: &UiState,
    theme: SemanticTheme,
    animate: bool,
) {
    match &display.kind {
        DisplayPartKind::Tool(tool) => render_expandable(
            frame,
            layout,
            measured,
            ExpandableContent {
                family: &tool.label,
                summary: &tool.summary,
                metadata: &tool.metadata,
                status: tool.status,
                expandable: tool.expandable,
                expanded: tool.expanded,
                panel_kind: tool.panel_kind,
                lines: &tool.artifact_lines,
                thinking_body: false,
            },
            state,
            theme,
            animate,
        ),
        DisplayPartKind::Thinking {
            status,
            expanded,
            lines,
        } => {
            let thinking_lines: Vec<crate::tui_v2::presentation::tool::ArtifactLine> = lines
                .iter()
                .map(|line| crate::tui_v2::presentation::tool::ArtifactLine {
                    kind: crate::tui_v2::presentation::tool::ArtifactLineKind::Plain,
                    text: line.clone(),
                    chunks: Vec::new(),
                    gutter: String::new(),
                })
                .collect();
            render_expandable(
                frame,
                layout,
                measured,
                ExpandableContent {
                    family: "",
                    summary: "thinking",
                    metadata: "",
                    status: *status,
                    expandable: !lines.is_empty(),
                    expanded: *expanded,
                    panel_kind: crate::tui_v2::presentation::tool::ArtifactPanelKind::Generic,
                    lines: &thinking_lines,
                    thinking_body: true,
                },
                state,
                theme,
                animate,
            )
        }
        DisplayPartKind::User { .. } => render_user_bubble(frame, layout, measured, state, theme),
        DisplayPartKind::Agent { .. } => render_agent_markdown(frame, layout, measured, theme),
        DisplayPartKind::Notice { level } => {
            let color = match level {
                NoticeLevel::Neutral => theme.foreground_muted,
                NoticeLevel::Warning => theme.warning,
                NoticeLevel::Success => theme.success,
            };
            render_text(
                frame,
                layout,
                measured,
                color,
                theme.canvas,
                Alignment::Left,
            );
        }
        DisplayPartKind::Error => {
            render_text(
                frame,
                layout,
                measured,
                theme.error,
                theme.canvas,
                Alignment::Left,
            );
        }
    }
}

fn clip_row_range(clip_rows: &std::ops::Range<u32>, len: usize) -> std::ops::Range<usize> {
    let end = usize::try_from(clip_rows.end).unwrap_or(len).min(len);
    let start = usize::try_from(clip_rows.start).unwrap_or(end).min(end);
    start..end
}

fn render_agent_markdown(
    frame: &mut Frame,
    layout: &PartLayout,
    measured: &MeasuredPart,
    theme: SemanticTheme,
) {
    let Some(markdown) = measured.markdown.as_ref() else {
        render_text(
            frame,
            layout,
            measured,
            theme.foreground,
            theme.canvas,
            Alignment::Left,
        );
        return;
    };
    let range = clip_row_range(&layout.clip_rows, markdown.lines.len());
    let lines = markdown.lines[range.clone()].to_vec();
    frame.render_widget(
        Paragraph::new(lines).style(Style::default().fg(theme.foreground).bg(theme.canvas)),
        layout.visible_rect,
    );
    crate::tui_support::markdown::apply_hyperlinks(
        frame.buffer_mut(),
        layout.visible_rect,
        &markdown.links,
        range.start,
        0,
    );
}

struct ExpandableContent<'a> {
    family: &'a str,
    summary: &'a str,
    metadata: &'a str,
    status: StatusKind,
    expandable: bool,
    expanded: bool,
    panel_kind: crate::tui_v2::presentation::tool::ArtifactPanelKind,
    lines: &'a [crate::tui_v2::presentation::tool::ArtifactLine],
    /// Thinking body uses a plain full-text panel (no tool chrome).
    thinking_body: bool,
}

fn render_expandable(
    frame: &mut Frame,
    layout: &PartLayout,
    measured: &MeasuredPart,
    content: ExpandableContent<'_>,
    state: &UiState,
    theme: SemanticTheme,
    animate: bool,
) {
    let range = clip_row_range(&layout.clip_rows, measured.rows.len());
    let first = range.start;
    let last = range.end;
    let panel_height = measured.rows.len().saturating_sub(1);
    for row_index in first..last {
        let y = layout
            .visible_rect
            .y
            .saturating_add(u16::try_from(row_index.saturating_sub(first)).unwrap_or(u16::MAX));
        let area = Rect::new(layout.visible_rect.x, y, layout.visible_rect.width, 1);
        if row_index == 0 {
            ExpandableRow {
                indent: 1,
                status: StatusGlyph {
                    kind: content.status,
                    phase: if animate
                        && matches!(state.appearance.motion.preference, MotionPreference::Full)
                    {
                        let frames = crate::tui_v2::presentation::symbols::Symbols::for_mode(
                            state.capability.glyph_mode,
                        )
                        .pulse_frames
                        .len()
                        .max(1);
                        // Per-row phase offset so parallel agents don't look locked together.
                        let base = state.appearance.motion.clock.frame(
                            frames,
                            crate::tui_v2::presentation::symbols::RUNNING_FRAME_INTERVAL_MS,
                        );
                        let salt = layout.part_id.as_str().bytes().fold(0usize, |acc, b| {
                            acc.wrapping_mul(31).wrapping_add(b as usize)
                        });
                        base.wrapping_add(salt) % frames
                    } else {
                        0
                    },
                },
                family: content.family,
                summary: content.summary,
                metadata: (!content.metadata.is_empty()).then_some(content.metadata),
                expandable: content.expandable,
                expanded: content.expanded,
                focused: state.transcript.selected_part.as_ref() == Some(&layout.part_id),
            }
            .render(frame, area, state.capability, theme);
        } else if !content.expanded {
            // Collapsed rows are header-only. Extra measured rows (legacy wrap
            // of long summaries) must never paint panel chrome.
        } else if content.thinking_body {
            let artifact = state
                .artifacts
                .get(&layout.part_id)
                .cloned()
                .unwrap_or_default();
            let body_rows = panel_height.max(1);
            let offset = terminal_content_offset(
                content.lines.len(),
                body_rows,
                artifact.follow_live,
                artifact.inner_scroll,
            );
            let line = content
                .lines
                .get(offset.saturating_add(row_index.saturating_sub(1)))
                .map(|line| line.text.as_str())
                .unwrap_or("");
            render_thinking_row(frame, area, line, theme);
        } else {
            let content_offset = if matches!(
                content.panel_kind,
                crate::tui_v2::presentation::tool::ArtifactPanelKind::Terminal
                    | crate::tui_v2::presentation::tool::ArtifactPanelKind::AgentChat
            ) {
                let artifact = state
                    .artifacts
                    .get(&layout.part_id)
                    .cloned()
                    .unwrap_or_default();
                terminal_content_offset(
                    content.lines.len(),
                    panel_body_rows(panel_height),
                    artifact.follow_live,
                    artifact.inner_scroll,
                )
            } else {
                0
            };
            render_panel_row(
                frame,
                area,
                row_index - 1,
                panel_height,
                content.lines,
                content.panel_kind,
                content_offset,
                state.capability,
                theme,
            );
        }
    }
}

fn render_text(
    frame: &mut Frame,
    layout: &PartLayout,
    measured: &MeasuredPart,
    foreground: ratatui::style::Color,
    background: ratatui::style::Color,
    alignment: Alignment,
) {
    let range = clip_row_range(&layout.clip_rows, measured.rows.len());
    let lines = measured.rows[range]
        .iter()
        .map(|row| {
            Line::styled(
                row.text.clone(),
                Style::default().fg(foreground).bg(background),
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(alignment)
            .style(Style::default().fg(foreground).bg(background)),
        layout.visible_rect,
    );
}

/// Right-aligned chat bubble: tight rounded frame, light side pad, page fill.
fn render_user_bubble(
    frame: &mut Frame,
    layout: &PartLayout,
    measured: &MeasuredPart,
    state: &UiState,
    theme: SemanticTheme,
) {
    use crate::tui_v2::presentation::transcript::USER_BUBBLE_SIDE_PAD;

    let area = layout.visible_rect;
    if area.is_empty() || measured.rows.len() < 3 {
        // Need border + content + border (chrome always adds 2).
        return;
    }

    // Measured chrome: [border][content…][border]
    let content_end = measured.rows.len().saturating_sub(1);
    let content_rows = &measured.rows[1..content_end.max(1)];
    let content_width = content_rows
        .iter()
        .map(|row| UnicodeWidthStr::width(row.text.as_str()))
        .max()
        .unwrap_or(0)
        .max(1);
    // Border (1 each side) + light inner pad (USER_BUBBLE_SIDE_PAD each side).
    let horizontal_chrome = 2u16.saturating_add(USER_BUBBLE_SIDE_PAD.saturating_mul(2));
    let bubble_width = u16::try_from(content_width.saturating_add(usize::from(horizontal_chrome)))
        .unwrap_or(area.width)
        .clamp(
            horizontal_chrome.saturating_add(1),
            area.width.max(horizontal_chrome.saturating_add(1)),
        );
    let bubble_x = area
        .x
        .saturating_add(area.width.saturating_sub(bubble_width));
    let bubble = Rect::new(bubble_x, area.y, bubble_width, area.height);

    let border_set = if state.capability.supports_rounded_borders() {
        ratatui::symbols::border::ROUNDED
    } else {
        ASCII_BORDER
    };
    // Page continuity: canvas fill (same as surface). Border + text share ink.
    // Explicit spaces implement side pad so it cannot collapse to zero width.
    let ink = theme.border_focused;
    let fill = theme.canvas;
    let side_pad = " ".repeat(usize::from(USER_BUBBLE_SIDE_PAD));
    let block = Block::default()
        .borders(Borders::ALL)
        .border_set(border_set)
        .border_style(Style::default().fg(ink).bg(fill))
        .style(Style::default().fg(ink).bg(fill));
    let inner = block.inner(bubble);
    frame.render_widget(Clear, bubble);
    // Paint the whole plate first so pad columns match the page, not a leftover plate.
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(fill).fg(ink)),
        bubble,
    );
    frame.render_widget(block, bubble);

    if inner.is_empty() {
        return;
    }

    // Content rows are measured[1..len-1]; map clip_rows onto them.
    let range = clip_row_range(&layout.clip_rows, measured.rows.len());
    let visible = content_rows
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let measured_index = index + 1;
            measured_index >= range.start && measured_index < range.end
        })
        .map(|(_, row)| {
            Line::styled(
                format!("{side_pad}{}{side_pad}", row.text),
                Style::default().fg(ink).bg(fill),
            )
        })
        .collect::<Vec<_>>();
    if visible.is_empty() {
        return;
    }
    let text_area = Rect::new(
        inner.x,
        inner.y,
        inner.width,
        u16::try_from(visible.len())
            .unwrap_or(inner.height)
            .min(inner.height),
    );
    frame.render_widget(
        Paragraph::new(visible).style(Style::default().fg(ink).bg(fill)),
        text_area,
    );
}
