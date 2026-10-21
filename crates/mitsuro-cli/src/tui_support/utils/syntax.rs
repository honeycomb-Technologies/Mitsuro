//! Syntax highlighting using syntect

use once_cell::sync::Lazy;
use ratatui::style::{Color, Style};
use ratatui::text::Span;
use syntect::easy::HighlightLines;
use syntect::highlighting::{FontStyle, ThemeSet};
use syntect::parsing::SyntaxSet;
use syntect::util::LinesWithEndings;

use crate::tui_support::markdown::MarkdownTheme;

/// Global syntax set - loaded once at startup
static SYNTAX_SET: Lazy<SyntaxSet> = Lazy::new(SyntaxSet::load_defaults_newlines);

/// Global theme set for syntect (we'll use our own theme mapping instead)
static THEME_SET: Lazy<ThemeSet> = Lazy::new(ThemeSet::load_defaults);

/// Highlight a code block and return styled spans for each line
pub fn highlight_code(code: &str, lang: &str, theme: &MarkdownTheme) -> Vec<Vec<Span<'static>>> {
    // Try to find syntax by extension or name
    let syntax = SYNTAX_SET
        .find_syntax_by_token(lang)
        .or_else(|| SYNTAX_SET.find_syntax_by_extension(lang))
        .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text());

    // Use base16-ocean.dark as a reasonable default for scope detection
    let syntect_theme = &THEME_SET.themes["base16-ocean.dark"];
    let mut highlighter = HighlightLines::new(syntax, syntect_theme);

    let mut result = Vec::new();

    for line in LinesWithEndings::from(code) {
        let ranges = highlighter
            .highlight_line(line, &SYNTAX_SET)
            .unwrap_or_default();

        let spans: Vec<Span<'static>> = ranges
            .into_iter()
            .map(|(style, text)| {
                // Map syntect style to our theme colors
                let color = map_syntect_color_to_theme(style.foreground, theme);
                let mut ratatui_style = Style::default().fg(color);

                if style.font_style.contains(FontStyle::BOLD) {
                    ratatui_style = ratatui_style.add_modifier(ratatui::style::Modifier::BOLD);
                }
                if style.font_style.contains(FontStyle::ITALIC) {
                    ratatui_style = ratatui_style.add_modifier(ratatui::style::Modifier::ITALIC);
                }

                // Remove trailing newline from text for cleaner output
                let clean_text = text.trim_end_matches('\n').to_string();
                Span::styled(clean_text, ratatui_style)
            })
            .collect();

        result.push(spans);
    }

    // Handle empty code
    if result.is_empty() {
        result.push(vec![Span::raw("")]);
    }

    result
}

/// Map syntect's color to our theme colors based on the base16-ocean palette
/// This gives us semantic highlighting that respects the user's theme
fn map_syntect_color_to_theme(
    syntect_color: syntect::highlighting::Color,
    theme: &MarkdownTheme,
) -> Color {
    // base16-ocean.dark palette colors (what syntect will output):
    // base00 = #2b303b (background)
    // base01 = #343d46 (lighter bg)
    // base02 = #4f5b66 (selection)
    // base03 = #65737e (comments)
    // base04 = #a7adba (dark fg)
    // base05 = #c0c5ce (default fg)
    // base06 = #dfe1e8 (light fg)
    // base07 = #eff1f5 (lightest fg)
    // base08 = #bf616a (red - errors, variables)
    // base09 = #d08770 (orange - numbers)
    // base0A = #ebcb8b (yellow - classes)
    // base0B = #a3be8c (green - strings)
    // base0C = #96b5b4 (cyan - support)
    // base0D = #8fa1b3 (blue - functions)
    // base0E = #b48ead (purple - keywords)
    // base0F = #ab7967 (brown - deprecated)

    let (r, g, b) = (syntect_color.r, syntect_color.g, syntect_color.b);

    // Match against known base16-ocean colors and map to our theme
    match (r, g, b) {
        // Comments (gray)
        (101, 115, 126) => theme.syntax_comment_color,
        // Strings (green)
        (163, 190, 140) => theme.syntax_string_color,
        // Numbers (orange)
        (208, 135, 112) => theme.syntax_number_color,
        // Keywords (purple)
        (180, 142, 173) => theme.syntax_keyword_color,
        // Functions (blue)
        (143, 161, 179) => theme.syntax_function_color,
        // Types/Classes (yellow)
        (235, 203, 139) => theme.syntax_type_color,
        // Variables (red)
        (191, 97, 106) => theme.syntax_variable_color,
        // Cyan (support/constants)
        (150, 181, 180) => theme.syntax_type_color,
        // Operators and punctuation (light gray)
        (192, 197, 206) | (167, 173, 186) => theme.syntax_punctuation_color,
        // Default foreground colors
        (223, 225, 232) | (239, 241, 245) => theme.text_color,
        // For any other color, use text color to maintain theme consistency
        _ => theme.text_color,
    }
}
