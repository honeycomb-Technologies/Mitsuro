//! Exact provider/auth/transport model picker.

use ratatui::{
    layout::Rect,
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};

use crate::tui_v2::{
    app::state::PickerUiState,
    components::primitive::overlay_chrome::{paint_crossbar, OverlayChromeLayout},
    model::capability::{CapabilityProfile, GlyphMode},
    presentation::theme::SemanticTheme,
    services::{SetupModel, SetupProvider, SetupSnapshot},
};

#[derive(Clone, Copy)]
pub struct ModelChoice<'a> {
    pub provider_index: usize,
    pub provider: &'a SetupProvider,
    pub model: &'a SetupModel,
}

#[derive(Clone, Copy)]
enum DisplayRow<'a> {
    Provider(&'a str),
    Model {
        index: usize,
        choice: ModelChoice<'a>,
    },
}

pub fn filtered<'a>(setup: &'a SetupSnapshot, query: &str) -> Vec<ModelChoice<'a>> {
    let query = query.trim().to_lowercase();
    setup
        .providers
        .iter()
        .enumerate()
        .filter(|(_, provider)| provider.connected)
        .flat_map(|(provider_index, provider)| {
            let query = query.clone();
            provider
                .models
                .iter()
                .filter(move |model| {
                    query.is_empty()
                        || model.label.to_lowercase().contains(&query)
                        || provider.label.to_lowercase().contains(&query)
                })
                .map(move |model| ModelChoice {
                    provider_index,
                    provider,
                    model,
                })
        })
        .collect()
}

fn display_rows<'a>(choices: &'a [ModelChoice<'a>]) -> Vec<DisplayRow<'a>> {
    let mut rows = Vec::with_capacity(choices.len().saturating_mul(2));
    let mut last_provider = None;
    for (index, choice) in choices.iter().enumerate() {
        if last_provider != Some(choice.provider_index) {
            rows.push(DisplayRow::Provider(choice.provider.label.as_str()));
            last_provider = Some(choice.provider_index);
        }
        rows.push(DisplayRow::Model {
            index,
            choice: *choice,
        });
    }
    rows
}

/// Window of display rows that keeps the selected model visible.
fn visible_rows<'a>(
    rows: &'a [DisplayRow<'a>],
    selected: usize,
    capacity: usize,
) -> &'a [DisplayRow<'a>] {
    if rows.is_empty() || capacity == 0 {
        return &[];
    }
    let focus = rows
        .iter()
        .position(|row| matches!(row, DisplayRow::Model { index, .. } if *index == selected))
        .unwrap_or(0);
    if rows.len() <= capacity {
        return rows;
    }
    let start = focus
        .saturating_sub(capacity / 2)
        .min(rows.len().saturating_sub(capacity));
    &rows[start..start.saturating_add(capacity)]
}

pub fn render(
    frame: &mut Frame,
    chrome: OverlayChromeLayout,
    setup: Option<&SetupSnapshot>,
    picker: &PickerUiState,
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    let glyph_mode = capability.glyph_mode;
    let pointer = if glyph_mode == GlyphMode::Ascii {
        "> "
    } else {
        "› "
    };
    let (search_area, crossbar_y, list_area) = chrome.search_list().unwrap_or((
        Rect::new(chrome.body.x, chrome.body.y, chrome.body.width, 0),
        chrome.body.y,
        chrome.body,
    ));

    if search_area.height > 0 {
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled("search  ", Style::default().fg(theme.foreground_muted)),
                Span::styled(
                    if picker.query.is_empty() {
                        "provider or model"
                    } else {
                        picker.query.as_str()
                    },
                    Style::default().fg(if picker.query.is_empty() {
                        theme.foreground_muted
                    } else {
                        theme.foreground
                    }),
                ),
            ]))
            .style(Style::default().bg(theme.surface)),
            search_area,
        );
        paint_crossbar(frame, chrome.outer, crossbar_y, theme, capability);
    }

    let choices = setup
        .map(|setup| filtered(setup, &picker.query))
        .unwrap_or_default();
    let mut lines = Vec::new();
    if choices.is_empty() {
        lines.push(Line::styled(
            "No authenticated model matches this filter.",
            Style::default().fg(theme.foreground_muted),
        ));
    } else {
        let reserved = usize::from(picker.error.is_some());
        let available = usize::from(list_area.height)
            .saturating_sub(reserved)
            .max(1);
        let selected = picker.selected.min(choices.len().saturating_sub(1));
        let rows = display_rows(&choices);
        let window = visible_rows(&rows, selected, available);
        for row in window {
            match *row {
                DisplayRow::Provider(label) => {
                    lines.push(Line::styled(
                        label.to_owned(),
                        Style::default()
                            .fg(theme.identity)
                            .add_modifier(Modifier::BOLD),
                    ));
                }
                DisplayRow::Model { index, choice } => {
                    let active = index == selected;
                    lines.push(Line::from(vec![
                        Span::styled(
                            if active { pointer } else { "  " },
                            Style::default().fg(theme.identity),
                        ),
                        Span::styled(
                            choice.model.label.clone(),
                            Style::default()
                                .fg(theme.foreground)
                                .add_modifier(if active {
                                    Modifier::BOLD
                                } else {
                                    Modifier::empty()
                                }),
                        ),
                    ]));
                }
            }
        }
    }
    if let Some(error) = &picker.error {
        lines.push(Line::styled(
            error.clone(),
            Style::default().fg(theme.error),
        ));
    }
    if list_area.height > 0 {
        frame.render_widget(
            Paragraph::new(lines).style(Style::default().fg(theme.foreground).bg(theme.surface)),
            list_area,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitsuro_core::ai::{models::ApiFormat, models::ModelKey, providers::ProviderId};

    use crate::tui_v2::services::{SetupModel, SetupProvider, SetupSnapshot};

    fn setup() -> SetupSnapshot {
        SetupSnapshot {
            providers: vec![
                SetupProvider {
                    id: ProviderId::Grok,
                    label: "xAI".to_owned(),
                    connected: true,
                    auth_methods: ProviderId::Grok.auth_methods(),
                    models: vec![
                        SetupModel {
                            key: ModelKey::new(
                                ProviderId::Grok,
                                "grok-4.5",
                                ApiFormat::OpenAIResponses,
                            ),
                            label: "grok-4.5".to_owned(),
                        },
                        SetupModel {
                            key: ModelKey::new(
                                ProviderId::Grok,
                                "grok-3",
                                ApiFormat::OpenAIResponses,
                            ),
                            label: "grok-3".to_owned(),
                        },
                    ],
                },
                SetupProvider {
                    id: ProviderId::OpenAI,
                    label: "OpenAI".to_owned(),
                    connected: true,
                    auth_methods: ProviderId::OpenAI.auth_methods(),
                    models: vec![SetupModel {
                        key: ModelKey::new(
                            ProviderId::OpenAI,
                            "gpt-5.6",
                            ApiFormat::OpenAIResponses,
                        ),
                        label: "gpt-5.6".to_owned(),
                    }],
                },
            ],
            selected_model: None,
        }
    }

    #[test]
    fn filtered_models_group_under_provider_headers() {
        let setup = setup();
        let choices = filtered(&setup, "");
        let rows = display_rows(&choices);
        assert!(matches!(rows[0], DisplayRow::Provider("xAI")));
        assert!(matches!(rows[1], DisplayRow::Model { index: 0, .. }));
        assert!(matches!(rows[3], DisplayRow::Provider("OpenAI")));
        assert_eq!(choices.len(), 3);
    }

    #[test]
    fn filtering_by_provider_keeps_group_headers() {
        let setup = setup();
        let choices = filtered(&setup, "openai");
        let rows = display_rows(&choices);
        assert_eq!(rows.len(), 2);
        assert!(matches!(rows[0], DisplayRow::Provider("OpenAI")));
        assert!(matches!(rows[1], DisplayRow::Model { index: 0, .. }));
    }
}
