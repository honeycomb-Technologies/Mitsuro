//! Inline authority surface that preserves the exact pending target.

use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::tui_v2::{
    app::state::{DecisionAction, UiState},
    components::primitive::{surface::Surface, text_style::TextRole},
    input::action::{ActionId, ActionRegistry},
    layout::snapshot::{LayoutRegionId, LayoutSnapshot},
    model::capability::GlyphMode,
    model::{conversation::PendingInteraction, focus::FocusTarget},
    presentation::theme::SemanticTheme,
};

pub fn render_decision_dock(
    frame: &mut Frame,
    layout: &LayoutSnapshot,
    state: &UiState,
    pending: &[PendingInteraction],
    theme: SemanticTheme,
) {
    let (Some(area), Some(pending)) =
        (layout.region(LayoutRegionId::DecisionDock), pending.first())
    else {
        return;
    };
    let inner = Surface {
        focused: matches!(state.focus, FocusTarget::DecisionDock),
    }
    .render(frame, area, theme, state.capability);
    if let PendingInteraction::Questions(value) = pending {
        render_questions(frame, inner, state, value, theme);
        return;
    }
    let (label, target) = match pending {
        PendingInteraction::ToolApproval(value) => ("Approval required", value.tool_name.as_str()),
        PendingInteraction::Questions(_) => ("Agent question", "response needed"),
        PendingInteraction::PlanConfirm(value) => ("Confirm plan", value.title.as_str()),
    };
    frame.render_widget(
        Paragraph::new(Line::styled(
            format!(
                "{label}{}{target}",
                if state.capability.glyph_mode == GlyphMode::Ascii {
                    " | "
                } else {
                    " · "
                }
            ),
            Style::default().fg(theme.warning),
        )),
        inner,
    );
    for (region, action_id, action) in [
        (
            LayoutRegionId::DecisionApprove,
            ActionId::ApproveDecision,
            DecisionAction::Approve,
        ),
        (
            LayoutRegionId::DecisionDeny,
            ActionId::DenyDecision,
            DecisionAction::Deny,
        ),
        (
            LayoutRegionId::DecisionInspect,
            ActionId::InspectDecision,
            DecisionAction::Inspect,
        ),
    ] {
        let (Some(area), Some(definition)) =
            (layout.region(region), ActionRegistry::definition(action_id))
        else {
            continue;
        };
        let binding = definition
            .primary_binding()
            .map_or_else(String::new, |binding| binding.label());
        let style = if state.decision_dock.focused_action == action {
            TextRole::Selection.style(theme)
        } else {
            TextRole::Muted.style(theme)
        };
        frame.render_widget(
            Paragraph::new(format!(
                " {binding} {} ",
                definition.label.to_ascii_lowercase()
            ))
            .style(style),
            area,
        );
    }
}

fn render_questions(
    frame: &mut Frame,
    area: ratatui::layout::Rect,
    state: &UiState,
    pending: &crate::tui_v2::model::conversation::PendingQuestions,
    theme: SemanticTheme,
) {
    let Some(question) = pending.questions.get(state.decision_dock.current_question) else {
        // Never leave a titleless empty border after a failed submit or OOB index.
        frame.render_widget(
            Paragraph::new(Line::styled(
                "Question unavailable — try again or start a new conversation.",
                Style::default().fg(theme.warning),
            )),
            area,
        );
        return;
    };
    // Titleless like the rest of tui-v2: lead with the question body.
    // Multi-question progress is a muted suffix, never a header strip.
    let mut question_spans = vec![Span::styled(
        question.question.as_str(),
        Style::default().fg(theme.foreground),
    )];
    if pending.questions.len() > 1 {
        question_spans.push(Span::styled(
            format!(
                "  ·  {}/{}",
                state.decision_dock.current_question + 1,
                pending.questions.len()
            ),
            Style::default().fg(theme.foreground_muted),
        ));
    }
    let mut lines = vec![Line::from(question_spans)];
    // Reserve one row for the question and one for the footer hints.
    let available = usize::from(area.height).saturating_sub(2);
    let ascii = state.capability.glyph_mode == GlyphMode::Ascii;
    for (index, option) in question.options.iter().enumerate().take(available) {
        let focused = index == state.decision_dock.selected_option;
        let checked = state.decision_dock.toggled_options.contains(&index);
        let marker = if question.multi_select {
            if checked {
                if ascii {
                    "[x]"
                } else {
                    "[×]"
                }
            } else {
                "[ ]"
            }
        } else if focused {
            if ascii {
                "*"
            } else {
                "●"
            }
        } else if ascii {
            "o"
        } else {
            "○"
        };
        lines.push(Line::from(vec![
            Span::styled(
                if focused {
                    if ascii {
                        "> "
                    } else {
                        "› "
                    }
                } else {
                    "  "
                },
                Style::default().fg(theme.identity),
            ),
            Span::styled(
                format!("{} {marker} {}", index + 1, option.label),
                Style::default()
                    .fg(if focused {
                        theme.foreground
                    } else {
                        theme.foreground_muted
                    })
                    .add_modifier(if focused {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    }),
            ),
            Span::styled(
                option
                    .description
                    .as_ref()
                    .map(|description| format!("  {description}"))
                    .unwrap_or_default(),
                Style::default().fg(theme.foreground_muted),
            ),
        ]));
    }
    lines.push(Line::styled(
        if ascii && question.multi_select {
            "Up/Down · Space toggle · Enter continue"
        } else if ascii {
            "Up/Down · 1-9 select · Enter continue"
        } else if question.multi_select {
            "↑/↓ · Space toggle · Enter continue"
        } else {
            "↑/↓ · 1–9 select · Enter continue"
        },
        Style::default().fg(theme.foreground_muted),
    ));
    frame.render_widget(Paragraph::new(lines), area);
}
