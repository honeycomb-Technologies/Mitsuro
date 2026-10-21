//! Quiet service-backed inspectors and the wide workspace dock.

use ratatui::{
    layout::{Alignment, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::tui_v2::{
    app::state::{PickerUiState, UiState},
    components::{primitive::dock_chrome::paint_dock_panel, scrollbars::render_scrollbar_glyphs},
    layout::snapshot::{LayoutRegionId, LayoutSnapshot},
    model::{
        capability::{CapabilityProfile, GlyphMode},
        focus::FocusTarget,
    },
    motion::preference::MotionPreference,
    presentation::{
        symbols::Symbols,
        theme::{SemanticTheme, ThemeKind},
    },
    services::{ExtensionRow, PlanSnapshot, ProcessRow},
};

fn row<'a>(
    active: bool,
    label: impl Into<Span<'a>>,
    meta: impl Into<Span<'a>>,
    glyph_mode: GlyphMode,
    theme: SemanticTheme,
) -> Line<'a> {
    Line::from(vec![
        Span::styled(
            if active {
                if glyph_mode == GlyphMode::Ascii {
                    "> "
                } else {
                    "› "
                }
            } else {
                "  "
            },
            Style::default().fg(theme.identity),
        ),
        label.into().style(
            Style::default()
                .fg(theme.foreground)
                .add_modifier(if active {
                    Modifier::BOLD
                } else {
                    Modifier::empty()
                }),
        ),
        Span::raw("  "),
        meta.into()
            .style(Style::default().fg(theme.foreground_muted)),
    ])
}

pub fn render_processes(
    frame: &mut Frame,
    area: Rect,
    processes: &[ProcessRow],
    picker: &PickerUiState,
    glyph_mode: GlyphMode,
    theme: SemanticTheme,
) {
    let separator = if glyph_mode == GlyphMode::Ascii {
        " | "
    } else {
        " · "
    };
    let mut lines = Vec::new();
    if processes.is_empty() {
        lines.push(Line::styled(
            "No running or recent background processes.",
            Style::default().fg(theme.foreground_muted),
        ));
    } else {
        let available = usize::from(area.height)
            .saturating_sub(usize::from(picker.error.is_some()))
            .max(1);
        let selected = picker.selected.min(processes.len().saturating_sub(1));
        for (index, process) in processes.iter().enumerate().take(available) {
            lines.push(row(
                index == selected,
                process.command.clone(),
                format!("{}{separator}{}s", process.status, process.elapsed_seconds),
                glyph_mode,
                theme,
            ));
        }
    }
    if let Some(error) = picker.error.as_deref() {
        lines.push(Line::styled(error, Style::default().fg(theme.error)));
    }
    paint(frame, area, lines, theme);
}

/// Sectioned Goal / Plan dock using product section lines (`─` / `-`).
///
/// ```text
/// GOAL
/// ────
/// Ship a disposable classic Tetris…
/// ────
/// PLAN  0/5
/// ────
///   ○  Board + piece spawn
///   ○  Controls + gravity
/// ```
///
/// Long body text is width-wrapped. Callers page with `scroll` + viewport height.
fn plan_lines(
    plan: Option<&PlanSnapshot>,
    glyph_mode: GlyphMode,
    theme: SemanticTheme,
    width: u16,
) -> Vec<Line<'static>> {
    let symbols = Symbols::for_mode(glyph_mode);
    let content_width = usize::from(width.max(1));
    let rule = section_rule(symbols.divider, width);
    let rule_style = Style::default().fg(theme.border);
    let label_style = Style::default()
        .fg(theme.foreground_muted)
        .add_modifier(Modifier::BOLD);

    let Some(plan) = plan else {
        return vec![
            Line::styled("GOAL", label_style),
            Line::styled(rule.clone(), rule_style),
            Line::styled(
                "No active goal",
                Style::default().fg(theme.foreground_muted),
            ),
            Line::styled(rule, rule_style),
            Line::styled(
                "/plan to start",
                Style::default().fg(theme.foreground_muted),
            ),
        ];
    };

    let mut lines = Vec::new();

    // ── GOAL ──────────────────────────────────────────────
    lines.push(Line::styled("GOAL", label_style));
    lines.push(Line::styled(rule.clone(), rule_style));

    let goal_body = {
        let objective = plan.objective.trim();
        if !objective.is_empty() && objective != plan.title.as_str() {
            objective.to_owned()
        } else {
            plan.title.clone()
        }
    };
    let body_style = Style::default().fg(theme.foreground);
    for chunk in wrap_text(&goal_body, content_width) {
        lines.push(Line::styled(chunk, body_style));
    }

    // ── PLAN ──────────────────────────────────────────────
    lines.push(Line::styled(rule.clone(), rule_style));
    lines.push(Line::styled(
        format!("PLAN  {}/{}", plan.completed_steps, plan.total_steps),
        label_style,
    ));
    lines.push(Line::styled(rule, rule_style));

    if plan.steps.is_empty() {
        lines.push(Line::styled(
            "  No steps yet",
            Style::default().fg(theme.foreground_muted),
        ));
    } else {
        for step in &plan.steps {
            let marker = if step.done {
                symbols.success
            } else if step.active {
                if glyph_mode == GlyphMode::Ascii {
                    "*"
                } else {
                    "●"
                }
            } else if glyph_mode == GlyphMode::Ascii {
                "o"
            } else {
                "○"
            };
            let style = if step.active {
                Style::default()
                    .fg(theme.foreground)
                    .add_modifier(Modifier::BOLD)
            } else if step.done {
                Style::default().fg(theme.foreground_muted)
            } else {
                Style::default().fg(theme.foreground)
            };
            let prefix = format!("  {marker}  ");
            let prefix_w = UnicodeWidthStr::width(prefix.as_str());
            let hang = " ".repeat(prefix_w);
            let desc_width = content_width.saturating_sub(prefix_w).max(1);
            let wrapped = wrap_text(&step.description, desc_width);
            if wrapped.is_empty() {
                lines.push(Line::styled(prefix, style));
            } else {
                for (index, chunk) in wrapped.into_iter().enumerate() {
                    let text = if index == 0 {
                        format!("{prefix}{chunk}")
                    } else {
                        format!("{hang}{chunk}")
                    };
                    lines.push(Line::styled(text, style));
                }
            }
        }
    }

    lines
}

fn section_rule(divider: &str, width: u16) -> String {
    let cell = if divider.is_empty() { "─" } else { divider };
    cell.repeat(usize::from(width.max(1)))
}

/// Width-aware wrap that breaks on spaces when possible.
fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    for raw in text.split('\n') {
        if raw.is_empty() {
            lines.push(String::new());
            continue;
        }
        let mut current = String::new();
        let mut current_w = 0usize;
        for word in raw.split_whitespace() {
            let word_w = UnicodeWidthStr::width(word);
            if current.is_empty() {
                if word_w <= width {
                    current.push_str(word);
                    current_w = word_w;
                } else {
                    // Hard-break overlong tokens.
                    for ch in word.chars() {
                        let ch_w = UnicodeWidthChar::width(ch).unwrap_or(0);
                        if current_w + ch_w > width && !current.is_empty() {
                            lines.push(std::mem::take(&mut current));
                            current_w = 0;
                        }
                        current.push(ch);
                        current_w = current_w.saturating_add(ch_w);
                    }
                }
                continue;
            }
            if current_w + 1 + word_w <= width {
                current.push(' ');
                current.push_str(word);
                current_w = current_w.saturating_add(1 + word_w);
            } else {
                lines.push(std::mem::take(&mut current));
                if word_w <= width {
                    current.push_str(word);
                    current_w = word_w;
                } else {
                    current_w = 0;
                    for ch in word.chars() {
                        let ch_w = UnicodeWidthChar::width(ch).unwrap_or(0);
                        if current_w + ch_w > width && !current.is_empty() {
                            lines.push(std::mem::take(&mut current));
                            current_w = 0;
                        }
                        current.push(ch);
                        current_w = current_w.saturating_add(ch_w);
                    }
                }
            }
        }
        if !current.is_empty() {
            lines.push(current);
        }
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

/// Total plan-dock content rows for a given width (for scroll clamping).
pub fn plan_content_rows(
    plan: Option<&PlanSnapshot>,
    glyph_mode: GlyphMode,
    theme: SemanticTheme,
    width: u16,
) -> u16 {
    let lines = plan_lines(plan, glyph_mode, theme, width);
    u16::try_from(lines.len()).unwrap_or(u16::MAX)
}

pub fn render_plan(
    frame: &mut Frame,
    area: Rect,
    plan: Option<&PlanSnapshot>,
    glyph_mode: GlyphMode,
    theme: SemanticTheme,
) {
    let lines = plan_lines(plan, glyph_mode, theme, area.width);
    paint(frame, area, lines, theme);
}

/// Wide workspace dock: title-less purple twin panels (plan over plugin).
pub fn render_workspace_sidebar(
    frame: &mut Frame,
    layout: &LayoutSnapshot,
    state: &UiState,
    plan: Option<&PlanSnapshot>,
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    let Some(inspector) = layout.region(LayoutRegionId::Inspector) else {
        return;
    };
    if inspector.is_empty() {
        return;
    }

    let plan_area = layout.region(LayoutRegionId::PlanDock).unwrap_or(inspector);
    let plugin_area = layout.region(LayoutRegionId::PluginDock);
    let plugin_focused =
        state.dock.plugin_focused || matches!(state.focus, FocusTarget::PluginDock);

    render_plan_dock(
        frame,
        plan_area,
        plan,
        state.dock.plan_scroll,
        capability,
        theme,
        matches!(state.focus, FocusTarget::PlanDock) || !plugin_focused,
    );

    if let Some(plugin) = plugin_area.filter(|area| area.height > 0) {
        render_plugin_dock(frame, plugin, plugin_focused, capability, theme);
    }
}

fn render_plan_dock(
    frame: &mut Frame,
    area: Rect,
    plan: Option<&PlanSnapshot>,
    scroll: u16,
    capability: CapabilityProfile,
    theme: SemanticTheme,
    focused: bool,
) {
    if area.is_empty() {
        return;
    }
    let inner = paint_dock_panel(frame, area, focused, theme, capability);
    if inner.is_empty() {
        return;
    }

    // Measure with full width first; reserve a scrollbar column when overflowing.
    let full_lines = plan_lines(plan, capability.glyph_mode, theme, inner.width);
    let visible = usize::from(inner.height).max(1);
    let needs_scroll = full_lines.len() > visible;
    let content_width = if needs_scroll {
        inner.width.saturating_sub(1).max(1)
    } else {
        inner.width
    };
    let lines = if content_width == inner.width {
        full_lines
    } else {
        plan_lines(plan, capability.glyph_mode, theme, content_width)
    };
    let total = lines.len();
    let max_scroll = total.saturating_sub(visible);
    let offset = usize::from(scroll).min(max_scroll);
    let window: Vec<Line<'static>> = lines.into_iter().skip(offset).take(visible).collect();

    let text_area = if needs_scroll {
        Rect::new(inner.x, inner.y, content_width, inner.height)
    } else {
        inner
    };
    frame.render_widget(
        Paragraph::new(window).style(Style::default().fg(theme.foreground).bg(theme.surface)),
        text_area,
    );

    if needs_scroll {
        let sb = Rect::new(inner.right().saturating_sub(1), inner.y, 1, inner.height);
        render_scrollbar_glyphs(
            frame,
            sb,
            offset as u32,
            total as u32,
            visible as u32,
            theme,
            focused,
            capability.glyph_mode == GlyphMode::Ascii,
        );
    }
}

fn render_plugin_dock(
    frame: &mut Frame,
    area: Rect,
    focused: bool,
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    if area.is_empty() {
        return;
    }
    let inner = paint_dock_panel(frame, area, focused, theme, capability);
    if inner.is_empty() {
        return;
    }

    let lines = vec![
        Line::styled(
            "No plugin loaded",
            Style::default().fg(theme.foreground_muted),
        ),
        Line::default(),
        Line::styled("/extensions", Style::default().fg(theme.foreground_muted)),
    ];

    let content_height = u16::try_from(lines.len())
        .unwrap_or(u16::MAX)
        .min(inner.height);
    let content = Rect::new(
        inner.x,
        inner
            .y
            .saturating_add(inner.height.saturating_sub(content_height) / 2),
        inner.width,
        content_height,
    );
    frame.render_widget(
        Paragraph::new(lines)
            .alignment(Alignment::Center)
            .style(Style::default().fg(theme.foreground).bg(theme.surface)),
        content,
    );
}

pub fn render_extensions(
    frame: &mut Frame,
    area: Rect,
    extensions: &[ExtensionRow],
    picker: &PickerUiState,
    glyph_mode: GlyphMode,
    theme: SemanticTheme,
) {
    let separator = if glyph_mode == GlyphMode::Ascii {
        " | "
    } else {
        " · "
    };
    let mut lines = Vec::new();
    if extensions.is_empty() {
        lines.push(Line::styled(
            "No MCP servers, skills, plugins, or hooks are configured.",
            Style::default().fg(theme.foreground_muted),
        ));
    } else {
        let available = usize::from(area.height)
            .saturating_sub(usize::from(picker.error.is_some()))
            .max(1);
        let selected = picker.selected.min(extensions.len().saturating_sub(1));
        for (index, extension) in extensions.iter().enumerate().take(available) {
            lines.push(row(
                index == selected,
                format!("{}{separator}{}", extension.category, extension.name),
                extension.status.clone(),
                glyph_mode,
                theme,
            ));
        }
    }
    if let Some(error) = picker.error.as_deref() {
        lines.push(Line::styled(error, Style::default().fg(theme.error)));
    }
    paint(frame, area, lines, theme);
}

pub const THEMES: [ThemeKind; 4] = [
    ThemeKind::MitsuroDark,
    ThemeKind::MitsuroLight,
    ThemeKind::TerminalAdaptive,
    ThemeKind::HighContrast,
];
pub const MOTION: [MotionPreference; 3] = [
    MotionPreference::Full,
    MotionPreference::Reduced,
    MotionPreference::Off,
];

pub fn render_appearance(
    frame: &mut Frame,
    area: Rect,
    picker: &PickerUiState,
    selected_theme: ThemeKind,
    selected_motion: MotionPreference,
    glyph_mode: GlyphMode,
    theme: SemanticTheme,
) {
    let mut lines = vec![Line::styled(
        "theme",
        Style::default().fg(theme.foreground_muted),
    )];
    for (index, value) in THEMES.iter().enumerate() {
        lines.push(row(
            picker.selected == index,
            format!("{value:?}"),
            if *value == selected_theme {
                "selected"
            } else {
                ""
            }
            .to_owned(),
            glyph_mode,
            theme,
        ));
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "motion",
        Style::default().fg(theme.foreground_muted),
    ));
    for (index, value) in MOTION.iter().enumerate() {
        lines.push(row(
            picker.selected == THEMES.len() + index,
            format!("{value:?}"),
            if *value == selected_motion {
                "selected"
            } else {
                ""
            }
            .to_owned(),
            glyph_mode,
            theme,
        ));
    }
    paint(frame, area, lines, theme);
}

fn paint(frame: &mut Frame, area: Rect, lines: Vec<Line<'_>>, theme: SemanticTheme) {
    frame.render_widget(
        Paragraph::new(lines).style(Style::default().fg(theme.foreground).bg(theme.surface)),
        area,
    );
}
