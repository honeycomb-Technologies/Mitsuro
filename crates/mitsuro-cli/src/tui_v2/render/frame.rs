//! Developer-preview frame rendered exclusively from `LayoutSnapshot`.

use ratatui::{
    layout::{Alignment, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Clear, Paragraph},
    Frame,
};

use crate::tui_v2::{
    app::state::{AgentRunState, UiState},
    components::{
        attachment_preview::render as render_attachment_preview,
        command_palette::render as render_command_palette,
        conversation::{
            render_context_bar as render_conversation_context, render_decision_dock,
            render_transcript, ConversationRenderData,
        },
        file_search::render as render_file_search,
        home::render as render_home,
        model_picker::render as render_model_picker,
        primitive::{
            action_footer::ActionFooter, input_field::InputField, overlay_chrome::OverlayChrome,
            surface::Surface, working_edge::render_working_edge,
        },
        service_inspector::{
            render_appearance, render_extensions, render_plan, render_processes,
            render_workspace_sidebar,
        },
        session_picker::render as render_session_picker,
        setup::render as render_setup,
        slash_autocomplete::render as render_slash_autocomplete,
    },
    input::active_context,
    layout::{
        responsive::{MINIMUM_HEIGHT, MINIMUM_WIDTH},
        snapshot::{LayoutRegionId, LayoutSnapshot},
    },
    model::capability::CapabilityProfile,
    presentation::transcript::DisplayPartKind,
    presentation::{symbols::Symbols, theme::SemanticTheme},
    services::{
        ControlSnapshot, ExtensionRow, HomeSnapshot, PlanSnapshot, ProcessRow, ProjectEntry,
        RecentSession, SetupSnapshot,
    },
};

pub fn render_preview(
    frame: &mut Frame,
    state: &UiState,
    theme: SemanticTheme,
    layout: &LayoutSnapshot,
    conversation: Option<ConversationRenderData<'_>>,
    home: Option<&HomeSnapshot>,
    setup: Option<&SetupSnapshot>,
    sessions: &[RecentSession],
    project_entries: &[ProjectEntry],
    processes: &[ProcessRow],
    plan: Option<&PlanSnapshot>,
    extensions: &[ExtensionRow],
    controls: &ControlSnapshot,
    attachment_image: Option<&mut ratatui_image::protocol::StatefulProtocol>,
) {
    let capability = state.capability;
    let area = layout.viewport;
    frame.render_widget(Clear, area);
    frame.render_widget(
        Block::default().style(Style::default().bg(theme.canvas).fg(theme.foreground)),
        area,
    );

    if let Some(resize) = layout.region(LayoutRegionId::ResizeMessage) {
        render_resize_message(frame, resize, theme);
        return;
    }

    // Top chrome only in conversation; Home stays brand-first with no title strip.
    if let Some(conversation) = conversation.as_ref() {
        render_conversation_context(frame, layout, state, conversation.metadata, home, theme);
    }
    render_divider(
        frame,
        required_region(layout, LayoutRegionId::TopDivider),
        capability,
        theme,
    );
    if let Some(conversation) = conversation.as_ref() {
        render_transcript(
            frame,
            layout,
            state,
            conversation.display,
            conversation.measured,
            theme,
        );
        render_decision_dock(frame, layout, state, conversation.pending, theme);
    } else if matches!(state.route, crate::tui_v2::app::route::AppRoute::Setup) {
        render_setup(
            frame,
            required_region(layout, LayoutRegionId::Primary),
            state,
            setup,
            theme,
        );
    } else {
        render_home(
            frame,
            required_region(layout, LayoutRegionId::Primary),
            state,
            home,
            theme,
        );
    }
    render_composer(
        frame,
        required_region(layout, LayoutRegionId::ComposerField),
        state,
        capability,
        theme,
        layout,
    );
    render_status_line(
        frame,
        layout,
        state,
        home,
        conversation.as_ref().map(|value| value.metadata),
        controls,
        theme,
    );
    if layout.region(LayoutRegionId::Inspector).is_some() {
        render_workspace_sidebar(frame, layout, state, plan, state.capability, theme);
    }
    // Bottom purple comet edge while the agent is running (Toad-style thin line).
    // Idle: leave the reserved row as canvas (no divider paint).
    if matches!(state.agent_run, AgentRunState::Running) {
        if let Some(edge) = layout.region(LayoutRegionId::BottomDivider) {
            if !edge.is_empty() {
                render_working_edge(
                    frame,
                    edge,
                    state.appearance.motion.clock,
                    theme,
                    capability.glyph_mode,
                    capability.color_depth,
                );
            }
        }
    }
    if let Some(autocomplete) = layout.region(LayoutRegionId::ComposerAutocomplete) {
        if state.composer.autocomplete_open {
            render_slash_autocomplete(frame, autocomplete, state, theme);
        } else if state.composer.file_search_open {
            render_file_search(frame, autocomplete, state, project_entries, theme);
        }
    }
    render_fullscreen_artifact(frame, layout, state, conversation.as_ref(), theme);
    render_overlay(
        frame,
        layout,
        state,
        setup,
        sessions,
        processes,
        plan,
        extensions,
        conversation.as_ref(),
        theme,
        attachment_image,
    );
}

fn render_fullscreen_artifact(
    frame: &mut Frame,
    layout: &LayoutSnapshot,
    state: &UiState,
    conversation: Option<&ConversationRenderData<'_>>,
    theme: SemanticTheme,
) {
    let Some((part_id, artifact)) = state
        .artifacts
        .iter()
        .find(|(_, artifact)| artifact.fullscreen)
    else {
        return;
    };
    let (Some(conversation), Some(area)) = (
        conversation,
        layout.region(LayoutRegionId::FullScreenArtifact),
    ) else {
        return;
    };
    let Some(part) = conversation
        .display
        .parts
        .iter()
        .find(|part| &part.id == part_id)
    else {
        return;
    };
    let (title, lines): (&str, Vec<String>) = match &part.kind {
        DisplayPartKind::Tool(tool) => (tool.label.as_str(), tool.plain_lines()),
        DisplayPartKind::Thinking { lines, .. } => ("thinking", lines.clone()),
        _ => return,
    };
    let hints = if state.capability.glyph_mode == crate::tui_v2::model::capability::GlyphMode::Ascii
    {
        "PgUp/PgDn scroll | c copy | Esc close"
    } else {
        "PgUp/PgDn scroll  ·  c copy  ·  Esc close"
    };
    let chrome = OverlayChrome { title, hints }.render(frame, area, theme, state.capability);
    let offset = usize::try_from(artifact.inner_scroll).unwrap_or(usize::MAX);
    let height = usize::from(chrome.body.height);
    let visible = lines
        .iter()
        .skip(offset)
        .take(height)
        .map(|line| Line::styled(line.clone(), Style::default().fg(theme.foreground)))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(visible).style(Style::default().fg(theme.foreground).bg(theme.surface)),
        chrome.body,
    );
}

fn render_divider(
    frame: &mut Frame,
    area: Rect,
    capability: CapabilityProfile,
    theme: SemanticTheme,
) {
    if area.is_empty() {
        return;
    }
    let divider = Symbols::for_mode(capability.glyph_mode)
        .divider
        .repeat(area.width.into());
    frame.render_widget(
        Paragraph::new(divider).style(Style::default().fg(theme.border)),
        area,
    );
}

fn render_composer(
    frame: &mut Frame,
    area: Rect,
    state: &UiState,
    capability: CapabilityProfile,
    theme: SemanticTheme,
    layout: &LayoutSnapshot,
) {
    let setup = matches!(state.route, crate::tui_v2::app::route::AppRoute::Setup)
        || state.overlay.as_ref().is_some_and(|overlay| {
            matches!(
                overlay.kind,
                crate::tui_v2::model::overlay::OverlayKind::Connections
            )
        });
    let credential = setup
        && matches!(
            state.setup.step,
            crate::tui_v2::app::state::SetupStep::Credential
                | crate::tui_v2::app::state::SetupStep::OAuthPasteCode
        );
    // Filled field like Grok Build: subtle surface + border (not hollow on canvas).
    // `area` is ComposerField (already narrowed by 1 when a scrollbar is reserved).
    let inner = Surface {
        focused: state.focus.is_composer() && (!setup || credential),
    }
    .render(frame, area, theme, capability);

    let content_width = usize::from(inner.width.max(1));
    let _content_rows = usize::from(inner.height.max(1));

    let selection = state
        .mouse
        .composer_selection_ordered()
        .or_else(|| state.composer.selection());
    let focused = state.focus.is_composer() && (!setup || credential);
    InputField {
        value: state.composer.text(),
        placeholder: if state.setup.step == crate::tui_v2::app::state::SetupStep::OAuthPasteCode {
            " Paste the returned authorization code, then press Enter"
        } else if credential {
            " Paste credential, then press Enter"
        } else if setup {
            if capability.glyph_mode == crate::tui_v2::model::capability::GlyphMode::Ascii {
                " Use Up/Down and Enter to continue setup"
            } else {
                " Use ↑/↓ and Enter to continue setup"
            }
        } else if capability.glyph_mode == crate::tui_v2::model::capability::GlyphMode::Ascii {
            " Ask Agent..."
        } else {
            " Ask Agent…"
        },
        masked: credential,
        mask_symbol: if capability.glyph_mode == crate::tui_v2::model::capability::GlyphMode::Ascii
        {
            "*"
        } else {
            "•"
        },
        horizontal_offset: 0,
        cursor_byte: state.composer.cursor_byte(),
        focused,
        error: None,
        fill_background: true,
        fill_color: Some(theme.surface),
        selection,
        // Word-like: pin viewport from ComposerBuffer (scrollbar / multi-line).
        // When following, buffer.offset already keeps the caret in range.
        viewport_offset: if credential {
            None
        } else {
            Some(state.composer.viewport_offset())
        },
    }
    .render(frame, inner, theme);

    if let Some(sb) = layout.region(LayoutRegionId::ComposerScrollbar) {
        // Track height already matches the input's inner content box; `visible`
        // is that same row count so the thumb scale cannot overshoot the field.
        let width = content_width;
        let visible = u32::from(sb.height.max(1));
        let total = state.composer.visual_row_count(width) as u32;
        if total > visible {
            crate::tui_v2::components::scrollbars::render_scrollbar(
                frame,
                sb,
                state.composer.viewport_offset() as u32,
                total,
                visible,
                theme,
                state.mouse.scrollbar_drag.as_ref().is_some_and(|region| {
                    matches!(
                        region,
                        crate::tui_v2::layout::snapshot::ScrollRegionId::Composer
                    )
                }),
            );
        }
    }
}

fn render_status_line(
    frame: &mut Frame,
    layout: &LayoutSnapshot,
    state: &UiState,
    home: Option<&HomeSnapshot>,
    metadata: Option<&crate::tui_v2::model::conversation::ConversationMetadata>,
    controls: &ControlSnapshot,
    theme: SemanticTheme,
) {
    let style = Style::default()
        .fg(theme.foreground_muted)
        .bg(theme.surface);
    let separator =
        if state.capability.glyph_mode == crate::tui_v2::model::capability::GlyphMode::Ascii {
            " | "
        } else {
            " · "
        };

    let status_meta = required_region(layout, LayoutRegionId::StatusMeta);
    // Quiet control strip: model · reasoning · permission only.
    // (No build/plan mode, branch, tokens, fast, or attention noise.)
    let mut parts = Vec::with_capacity(3);
    if !matches!(state.route, crate::tui_v2::app::route::AppRoute::Setup) {
        if let Some(model) = home.and_then(|home| home.model.as_deref()) {
            parts.push(model.to_owned());
        }
        if let Some(reasoning) = controls
            .reasoning
            .as_deref()
            .map(status_reasoning_word)
            .filter(|word| !word.is_empty())
        {
            parts.push(reasoning);
        }
        let permission = status_permission_word(&controls.permission);
        if !permission.is_empty() {
            parts.push(permission);
        }
    }
    let _ = metadata;
    let status = if let Some(notice) = &state.update {
        format!(" {}", notice.banner())
    } else if parts.is_empty() {
        String::new()
    } else {
        format!(" {}", parts.join(separator))
    };
    let status_style = if state.update.as_ref().is_some_and(|notice| notice.can_apply) {
        Style::default().fg(theme.success).bg(theme.surface)
    } else if state.update.is_some() {
        Style::default().fg(theme.warning).bg(theme.surface)
    } else {
        style
    };
    frame.render_widget(Paragraph::new(status).style(status_style), status_meta);
    let footer = required_region(layout, LayoutRegionId::ActionFooter);
    frame.render_widget(Block::default().style(style), footer);
    ActionFooter::render(
        frame,
        Rect::new(
            footer.x,
            footer.y,
            footer.width.saturating_sub(1),
            footer.height,
        ),
        active_context(state),
        state.capability.glyph_mode,
        theme,
    );
}

/// Status-line reasoning token: short word only (`high`, not `reasoning high`).
fn status_reasoning_word(label: &str) -> String {
    let trimmed = label.trim();
    let word = trimmed.strip_prefix("reasoning ").unwrap_or(trimmed).trim();
    if word.eq_ignore_ascii_case("off") || word.eq_ignore_ascii_case("none") {
        return String::new();
    }
    word.to_ascii_lowercase()
}

fn status_permission_word(permission: &str) -> String {
    match permission.trim().to_ascii_lowercase().as_str() {
        "autonomous" | "auto" => "autonomous".to_owned(),
        "supervised" | "manual" => "supervised".to_owned(),
        "" => String::new(),
        other => other.to_owned(),
    }
}

fn render_resize_message(frame: &mut Frame, area: Rect, theme: SemanticTheme) {
    let message = format!(
        "Mitsuro needs at least {MINIMUM_WIDTH}x{MINIMUM_HEIGHT}\ncurrent: {}x{}",
        area.width, area.height
    );
    frame.render_widget(
        Paragraph::new(message)
            .alignment(Alignment::Center)
            .style(Style::default().fg(theme.foreground)),
        vertically_centered(area, 2),
    );
}

fn render_overlay(
    frame: &mut Frame,
    layout: &LayoutSnapshot,
    state: &UiState,
    setup: Option<&SetupSnapshot>,
    sessions: &[RecentSession],
    processes: &[ProcessRow],
    plan: Option<&PlanSnapshot>,
    extensions: &[ExtensionRow],
    conversation: Option<&ConversationRenderData<'_>>,
    theme: SemanticTheme,
    attachment_image: Option<&mut ratatui_image::protocol::StatefulProtocol>,
) {
    let (Some(overlay), Some(area)) = (
        state.overlay.as_ref(),
        layout.region(LayoutRegionId::Overlay),
    ) else {
        return;
    };
    let chrome = OverlayChrome::for_overlay(&overlay.kind, state.capability).render(
        frame,
        area,
        theme,
        state.capability,
    );
    if matches!(
        overlay.kind,
        crate::tui_v2::model::overlay::OverlayKind::Connections
    ) {
        render_setup(frame, chrome.body, state, setup, theme);
        return;
    }
    if matches!(
        overlay.kind,
        crate::tui_v2::model::overlay::OverlayKind::SessionPicker
    ) {
        render_session_picker(
            frame,
            chrome,
            sessions,
            &state.picker,
            state.capability,
            theme,
        );
        return;
    }
    if matches!(
        overlay.kind,
        crate::tui_v2::model::overlay::OverlayKind::CommandPalette
    ) {
        render_command_palette(frame, chrome, &state.picker, false, state.capability, theme);
        return;
    }
    if matches!(
        overlay.kind,
        crate::tui_v2::model::overlay::OverlayKind::Help
    ) {
        render_command_palette(frame, chrome, &state.picker, true, state.capability, theme);
        return;
    }
    if matches!(
        overlay.kind,
        crate::tui_v2::model::overlay::OverlayKind::ModelPicker
    ) {
        render_model_picker(frame, chrome, setup, &state.picker, state.capability, theme);
        return;
    }
    match overlay.kind {
        crate::tui_v2::model::overlay::OverlayKind::Processes => {
            render_processes(
                frame,
                chrome.body,
                processes,
                &state.picker,
                state.capability.glyph_mode,
                theme,
            );
            return;
        }
        crate::tui_v2::model::overlay::OverlayKind::PlanGoal => {
            render_plan(frame, chrome.body, plan, state.capability.glyph_mode, theme);
            return;
        }
        crate::tui_v2::model::overlay::OverlayKind::ExtensionsCenter => {
            render_extensions(
                frame,
                chrome.body,
                extensions,
                &state.picker,
                state.capability.glyph_mode,
                theme,
            );
            return;
        }
        crate::tui_v2::model::overlay::OverlayKind::ThemeAppearance => {
            render_appearance(
                frame,
                chrome.body,
                &state.picker,
                state.appearance.theme,
                state.appearance.motion.preference,
                state.capability.glyph_mode,
                theme,
            );
            return;
        }
        crate::tui_v2::model::overlay::OverlayKind::FileArtifactInspector { ref part_id } => {
            render_artifact_inspector(frame, chrome.body, state, conversation, part_id, theme);
            return;
        }
        crate::tui_v2::model::overlay::OverlayKind::AttachmentPreview => {
            if let Some(preview) = state.attachment_preview.as_ref() {
                render_attachment_preview(frame, chrome.body, preview, theme, attachment_image);
            }
            return;
        }
        _ => {}
    }
    debug_assert!(false, "unhandled TUI v2 overlay: {:?}", overlay.kind);
}

fn render_artifact_inspector(
    frame: &mut Frame,
    area: Rect,
    state: &UiState,
    conversation: Option<&ConversationRenderData<'_>>,
    part_id: &crate::tui_v2::model::artifact::PartId,
    theme: SemanticTheme,
) {
    let Some(part) = conversation.and_then(|conversation| {
        conversation
            .display
            .parts
            .iter()
            .find(|part| &part.id == part_id)
    }) else {
        frame.render_widget(
            Paragraph::new("This artifact is no longer available.")
                .style(Style::default().fg(theme.foreground_muted)),
            area,
        );
        return;
    };
    let mut lines = match &part.kind {
        DisplayPartKind::Tool(tool) => {
            let mut lines = vec![
                Line::from(vec![
                    Span::styled(
                        tool.label.clone(),
                        Style::default()
                            .fg(theme.identity)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("  {} · {}", tool.summary, tool.metadata),
                        Style::default().fg(theme.foreground_muted),
                    ),
                ]),
                Line::default(),
            ];
            lines.extend(tool.plain_lines().into_iter().map(Line::raw));
            lines
        }
        DisplayPartKind::Thinking { lines, .. } => {
            let mut rendered = vec![
                Line::styled(
                    "Reasoning",
                    Style::default()
                        .fg(theme.identity)
                        .add_modifier(Modifier::BOLD),
                ),
                Line::default(),
            ];
            rendered.extend(lines.iter().cloned().map(Line::raw));
            rendered
        }
        _ => part.measurement_text.lines().map(Line::raw).collect(),
    };
    if lines.is_empty() {
        lines.push(Line::styled(
            "No artifact content was returned.",
            Style::default().fg(theme.foreground_muted),
        ));
    }
    let offset = state
        .artifacts
        .get(part_id)
        .map_or(0, |artifact| artifact.inner_scroll)
        .min(u32::from(u16::MAX)) as u16;
    frame.render_widget(
        Paragraph::new(lines)
            .scroll((offset, 0))
            .style(Style::default().fg(theme.foreground).bg(theme.surface)),
        area,
    );
}

fn vertically_centered(area: Rect, height: u16) -> Rect {
    let padding = area.height.saturating_sub(height);
    Rect::new(
        area.x,
        area.y.saturating_add(padding / 2),
        area.width,
        height.min(area.height),
    )
}

fn required_region(layout: &LayoutSnapshot, id: LayoutRegionId) -> Rect {
    layout
        .region(id)
        .unwrap_or_else(|| panic!("layout snapshot omitted required region {id:?}"))
}
