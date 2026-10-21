//! Shared containment surface with clip-safe borders.

use ratatui::{
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Clear},
    Frame,
};

use crate::tui_v2::{
    layout::snapshot::intersect,
    model::capability::CapabilityProfile,
    presentation::{symbols::ASCII_BORDER, theme::SemanticTheme},
};

#[derive(Clone, Copy, Debug)]
pub struct Surface {
    pub focused: bool,
}

impl Surface {
    pub fn render(
        self,
        frame: &mut Frame,
        area: Rect,
        theme: SemanticTheme,
        capability: CapabilityProfile,
    ) -> Rect {
        let area = intersect(area, frame.area());
        if area.is_empty() {
            return area;
        }
        let border_set = if capability.supports_rounded_borders() {
            ratatui::symbols::border::ROUNDED
        } else {
            ASCII_BORDER
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .border_set(border_set)
            .border_style(Style::default().fg(if self.focused {
                theme.border_focused
            } else {
                theme.border
            }));
        let block = block.style(Style::default().bg(theme.surface).fg(theme.foreground));
        let inner = block.inner(area);
        frame.render_widget(Clear, area);
        frame.render_widget(block, area);
        inner
    }
}

#[cfg(test)]
mod tests {
    use ratatui::{backend::TestBackend, layout::Rect, Terminal};

    use crate::tui_v2::{
        model::capability::{CapabilityProfile, ColorDepth, GlyphMode},
        presentation::theme::{SemanticTheme, ThemeKind},
    };

    use super::*;

    #[test]
    fn partially_offscreen_surface_is_clipped_before_border_rendering() {
        let mut terminal = Terminal::new(TestBackend::new(20, 8)).expect("terminal");
        let capability = CapabilityProfile {
            glyph_mode: GlyphMode::Ascii,
            color_depth: ColorDepth::Monochrome,
        };
        let theme = SemanticTheme::resolve(ThemeKind::MitsuroDark, capability.color_depth);

        terminal
            .draw(|frame| {
                Surface { focused: true }.render(
                    frame,
                    Rect::new(18, 6, 10, 10),
                    theme,
                    capability,
                );
            })
            .expect("clipped render");

        let buffer = terminal.backend().buffer();
        assert_eq!(buffer.area, Rect::new(0, 0, 20, 8));
        assert_eq!(buffer.cell((18, 6)).expect("top-left").symbol(), "+");
        assert_eq!(buffer.cell((19, 7)).expect("bottom-right").symbol(), "+");
    }
}
