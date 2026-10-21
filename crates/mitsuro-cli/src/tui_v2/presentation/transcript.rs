//! Conversation projection transformed into measurable semantic rows.

use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use crate::tui_v2::{
    components::primitive::status_glyph::StatusKind,
    layout::measure::{
        ExpansionMode, MeasureRequest, MeasuredPart, MeasurementCache, MeasurementKey, ThemeMetrics,
    },
    model::{
        artifact::{ArtifactUiState, PartId},
        capability::CapabilityProfile,
        conversation::{AttachmentKind, ConversationPresentation, NoticeLevel, TimelinePart},
    },
    presentation::theme::ThemeKind,
};

use super::tool::ToolDisplay;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DisplayPartKind {
    User {
        steering: bool,
    },
    Agent {
        streaming: bool,
    },
    Thinking {
        status: StatusKind,
        expanded: bool,
        lines: Vec<String>,
    },
    Tool(ToolDisplay),
    Notice {
        level: NoticeLevel,
    },
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplayPart {
    pub id: PartId,
    pub revision: u64,
    pub measurement_text: String,
    pub kind: DisplayPartKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConversationDisplayList {
    pub parts: Vec<DisplayPart>,
}

impl ConversationDisplayList {
    pub fn build(
        conversation: &ConversationPresentation,
        artifact_state: &BTreeMap<PartId, ArtifactUiState>,
        viewport_height: u16,
    ) -> Self {
        Self::build_with_materialize(conversation, artifact_state, viewport_height, None)
    }

    /// Same as [`build`], but always materializes full quality body for `force_materialize`
    /// (inspector / copy targets) even when the inline row is collapsed.
    pub fn build_with_materialize(
        conversation: &ConversationPresentation,
        artifact_state: &BTreeMap<PartId, ArtifactUiState>,
        viewport_height: u16,
        force_materialize: Option<&PartId>,
    ) -> Self {
        let mut parts = Vec::new();
        for turn in &conversation.turns {
            if let Some(user) = &turn.user {
                let mut text = user.text.clone();
                for attachment in &user.attachments {
                    text.push_str(&format!("\n[{}]", attachment.label));
                }
                parts.push(display(
                    user.id.clone(),
                    text,
                    DisplayPartKind::User {
                        steering: user.steering,
                    },
                ));
            }
            parts.extend(turn.parts.iter().map(|part| {
                display_part(part, artifact_state, viewport_height, force_materialize)
            }));
        }
        Self { parts }
    }

    pub fn expandable_ids(&self) -> Vec<PartId> {
        self.parts
            .iter()
            .filter_map(|part| match &part.kind {
                DisplayPartKind::Tool(tool) if tool.expandable => Some(part.id.clone()),
                DisplayPartKind::Thinking { lines, .. } if !lines.is_empty() => {
                    Some(part.id.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn spacing_before(&self) -> Vec<u16> {
        self.parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let Some(previous) = index.checked_sub(1).and_then(|index| self.parts.get(index))
                else {
                    return 0;
                };
                if is_activity(&previous.kind) && is_activity(&part.kind) {
                    0
                } else {
                    1
                }
            })
            .collect()
    }

    pub fn measure(
        &self,
        cache: &mut MeasurementCache,
        width: u16,
        artifacts: &BTreeMap<PartId, ArtifactUiState>,
        theme: ThemeKind,
        capability: CapabilityProfile,
    ) -> Vec<Arc<MeasuredPart>> {
        let semantic_theme = super::theme::SemanticTheme::resolve(theme, capability.color_depth);
        self.parts
            .iter()
            .map(|part| {
                let expansion =
                    artifacts
                        .get(&part.id)
                        .map_or(ExpansionMode::Collapsed, |artifact| {
                            if artifact.fullscreen {
                                ExpansionMode::Fullscreen
                            } else if artifact.expanded {
                                ExpansionMode::Expanded
                            } else {
                                ExpansionMode::Collapsed
                            }
                        });
                let is_user = matches!(part.kind, DisplayPartKind::User { .. });
                // User bubbles hug the right edge and expand left, with light
                // horizontal chrome (border + a couple cols of side pad).
                let measure_width = if is_user {
                    user_wrap_width(width)
                } else {
                    width.max(1)
                };
                let request = MeasureRequest {
                    key: MeasurementKey {
                        part_id: part.id.clone(),
                        revision: part.revision,
                        width: measure_width,
                        expansion,
                        theme_metrics: ThemeMetrics::new(theme),
                        capability,
                    },
                    text: &part.measurement_text,
                };
                let measured = if matches!(part.kind, DisplayPartKind::Agent { .. }) {
                    cache.measure_markdown(request, semantic_theme)
                } else {
                    cache.measure(request)
                };
                if is_user {
                    Arc::new(with_user_bubble_chrome(measured.as_ref()))
                } else {
                    measured
                }
            })
            .collect()
    }
}

/// Horizontal padding inside the bubble border (each side), in terminal cells.
pub const USER_BUBBLE_SIDE_PAD: u16 = 1;

/// Content wrap width for a right-aligned user bubble inside `column_width`.
///
/// Reserves border (2) + side pad (2×[`USER_BUBBLE_SIDE_PAD`]) and caps long
/// prompts around half the column so bubbles stay chatty, not wall-to-wall.
/// Short prompts still size to content at paint time.
pub fn user_wrap_width(column_width: u16) -> u16 {
    let chrome = 2u16.saturating_add(USER_BUBBLE_SIDE_PAD.saturating_mul(2));
    let available = column_width.saturating_sub(chrome).max(8);
    // ~half the transcript column — compact chat bubble, not a full-width bar.
    let preferred = (column_width / 2).max(8);
    preferred.min(available).max(8)
}

/// Adds top/bottom border rows only (horizontal pad is paint-time).
fn with_user_bubble_chrome(measured: &MeasuredPart) -> MeasuredPart {
    use crate::tui_v2::layout::measure::MeasuredRow;

    let empty = |source_start: usize| MeasuredRow {
        text: String::new(),
        source_start,
        source_end: source_start,
        column_offsets: vec![source_start],
    };
    let mut rows = Vec::with_capacity(measured.rows.len().saturating_add(2));
    // top border, content…, bottom border
    rows.push(empty(0));
    rows.extend(measured.rows.iter().cloned());
    let tail = measured.rows.last().map(|row| row.source_end).unwrap_or(0);
    rows.push(empty(tail));
    MeasuredPart {
        key: measured.key.clone(),
        rows,
        markdown: None,
        weight: measured.weight.saturating_add(2 * 8),
    }
}

fn is_activity(kind: &DisplayPartKind) -> bool {
    matches!(
        kind,
        DisplayPartKind::Tool(_) | DisplayPartKind::Thinking { .. }
    )
}

fn display_part(
    part: &TimelinePart,
    artifact_state: &BTreeMap<PartId, ArtifactUiState>,
    viewport_height: u16,
    force_materialize: Option<&PartId>,
) -> DisplayPart {
    // Fullscreen / inspector / force targets need the full body even if collapsed inline.
    let expanded = force_materialize.is_some_and(|id| id == part.id())
        || artifact_state
            .get(part.id())
            .is_some_and(|state| state.expanded || state.fullscreen);
    match part {
        TimelinePart::AgentText(agent) => display(
            agent.id.clone(),
            agent.text.clone(),
            DisplayPartKind::Agent {
                streaming: agent.streaming,
            },
        ),
        TimelinePart::Thinking(thinking) => {
            let lines = thinking
                .content
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            // Collapsed by default; expand only via explicit UI state (click/toggle).
            let expanded = force_materialize.is_some_and(|id| id == &thinking.id)
                || artifact_state
                    .get(&thinking.id)
                    .is_some_and(|state| state.expanded || state.fullscreen);
            // Cap tall thoughts to a scrollable window (same spirit as bash).
            let rows = if expanded {
                thinking_panel_rows(lines.len().max(1), viewport_height)
            } else {
                0
            };
            display(
                thinking.id.clone(),
                expandable_measurement("thinking", expanded, rows),
                DisplayPartKind::Thinking {
                    status: if thinking.streaming {
                        StatusKind::Running
                    } else {
                        StatusKind::Idle
                    },
                    expanded,
                    lines,
                },
            )
        }
        TimelinePart::Tool(tool) => {
            let tool_display = ToolDisplay::from_part(tool, expanded);
            // Diff/code: full packed body when expanded.
            // Bash: fixed terminal window that live-tails (does not grow unboundedly).
            let rows = match tool_display.panel_kind {
                crate::tui_v2::presentation::tool::ArtifactPanelKind::Terminal
                | crate::tui_v2::presentation::tool::ArtifactPanelKind::AgentChat => {
                    terminal_panel_rows(
                        tool_display.artifact_lines.len(),
                        viewport_height,
                        expanded,
                    )
                }
                crate::tui_v2::presentation::tool::ArtifactPanelKind::Diff
                | crate::tui_v2::presentation::tool::ArtifactPanelKind::Code => {
                    panel_rows(tool_display.artifact_lines.len(), viewport_height, expanded)
                }
                crate::tui_v2::presentation::tool::ArtifactPanelKind::Generic => {
                    panel_rows(tool_display.artifact_lines.len(), viewport_height, false)
                }
            };
            let measurement =
                expandable_measurement(&tool_display.summary, tool_display.expanded, rows);
            display(
                tool.id.clone(),
                measurement,
                DisplayPartKind::Tool(tool_display),
            )
        }
        TimelinePart::Notice(notice) => display(
            notice.id.clone(),
            notice.message.clone(),
            DisplayPartKind::Notice {
                level: notice.level,
            },
        ),
        TimelinePart::Attachment(attachment) => display(
            attachment.id.clone(),
            format!(
                "[{}] {}",
                match attachment.kind {
                    AttachmentKind::Image => "image",
                    AttachmentKind::Document => "file",
                },
                attachment.label
            ),
            DisplayPartKind::Notice {
                level: NoticeLevel::Neutral,
            },
        ),
        TimelinePart::Compaction(compaction) => display(
            compaction.id.clone(),
            format!(
                "Context compacted · {} → {} tokens",
                compaction.estimated_tokens_before,
                compaction.estimated_tokens_after.unwrap_or_default()
            ),
            DisplayPartKind::Notice {
                level: NoticeLevel::Neutral,
            },
        ),
        TimelinePart::Error(error) => display(
            error.id.clone(),
            error.message.clone(),
            DisplayPartKind::Error,
        ),
    }
}

fn display(id: PartId, measurement_text: String, kind: DisplayPartKind) -> DisplayPart {
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    measurement_text.hash(&mut hasher);
    std::mem::discriminant(&kind).hash(&mut hasher);
    DisplayPart {
        id,
        revision: hasher.finish(),
        measurement_text,
        kind,
    }
}

fn panel_rows(content_rows: usize, viewport_height: u16, show_full: bool) -> usize {
    let height = content_rows.saturating_add(2).max(3);
    if show_full {
        // Cap extreme dumps so a single expand cannot monopolize the transcript.
        let hard_cap = usize::from(viewport_height.saturating_mul(3).max(24));
        return height.min(hard_cap.saturating_add(2));
    }
    // Tools can stream huge output; keep the inline panel bounded.
    let cap = usize::from((viewport_height.saturating_mul(2) / 5).clamp(3, 14));
    height.min(cap)
}

/// Bash expands as a fixed terminal viewport that tails while `follow_live`.
fn terminal_panel_rows(content_rows: usize, viewport_height: u16, expanded: bool) -> usize {
    if !expanded {
        return 0;
    }
    // ~1/3 of the viewport, clamped so small/large terminals still feel terminal-like.
    let window = usize::from((viewport_height / 3).clamp(8, 18));
    let body = content_rows.min(window);
    body.saturating_add(2).max(3)
}

/// Thinking body: plain text rows, capped so long chains stay scrollable.
fn thinking_panel_rows(content_rows: usize, viewport_height: u16) -> usize {
    let window = usize::from((viewport_height / 3).clamp(6, 20));
    content_rows.min(window).max(1)
}

fn expandable_measurement(_label: &str, expanded: bool, panel_rows: usize) -> String {
    if expanded {
        // Height is encoded as trailing newlines; the header label itself must
        // never wrap or collapsed/expanded tools grow phantom panel chrome rows.
        format!("·{}", "\n".repeat(panel_rows))
    } else {
        // Collapsed height is always one row. Do not measure the real summary —
        // long bash/write paths wrap and used to paint full-width ─ bars under
        // the header via render_panel_row.
        "·".to_owned()
    }
}
