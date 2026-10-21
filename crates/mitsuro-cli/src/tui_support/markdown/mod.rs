//! Markdown rendering for assistant messages
//!
//! Only affects assistant text - does not touch ThinkingBlock,
//! BashBlock, ToolResultBlock, or user/system messages.

mod elements;
mod hyperlinks;
mod inline;
mod links;
mod parser;
mod renderer;

pub use hyperlinks::apply_hyperlinks;
pub use links::RenderedMarkdown;

/// Render markdown text to styled lines with link tracking
///
/// Returns both the rendered lines and metadata about hyperlink positions.
/// Use `apply_hyperlinks()` to apply OSC 8 sequences after rendering to buffer.
pub fn render_with_links(text: &str, width: usize, theme: &MarkdownTheme) -> RenderedMarkdown {
    let elements = parser::parse(text);
    renderer::render_elements_with_links(&elements, width, theme)
}

/// Colors consumed by Markdown and its embedded code blocks.
pub struct MarkdownTheme {
    pub title_color: ratatui::style::Color,
    pub accent_color: ratatui::style::Color,
    pub text_color: ratatui::style::Color,
    pub dim_color: ratatui::style::Color,
    pub code_bg_color: ratatui::style::Color,
    pub link_color: ratatui::style::Color,
    pub syntax_keyword_color: ratatui::style::Color,
    pub syntax_function_color: ratatui::style::Color,
    pub syntax_string_color: ratatui::style::Color,
    pub syntax_number_color: ratatui::style::Color,
    pub syntax_comment_color: ratatui::style::Color,
    pub syntax_type_color: ratatui::style::Color,
    pub syntax_variable_color: ratatui::style::Color,
    pub syntax_punctuation_color: ratatui::style::Color,
}
