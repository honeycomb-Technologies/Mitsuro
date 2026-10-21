//! Link tracking for OSC 8 hyperlinks
//!
//! Tracks hyperlink positions during markdown rendering so they can be
//! applied to buffer cells after normal text rendering is complete.

use ratatui::text::Line;

/// Tracks a hyperlink's position in rendered output
#[derive(Debug, Clone)]
pub struct LinkSpan {
    /// The URL this link points to
    pub url: String,
    /// Line index in rendered output (0-based)
    pub line: usize,
    /// Start column in display width units (0-based)
    pub start_col: usize,
    /// End column in display width units (exclusive)
    pub end_col: usize,
}

/// Rendered markdown with link tracking
#[derive(Debug, Clone)]
pub struct RenderedMarkdown {
    /// The rendered lines of text
    pub lines: Vec<Line<'static>>,
    /// All hyperlinks with their positions
    pub links: Vec<LinkSpan>,
}

impl RenderedMarkdown {
    /// Create a new RenderedMarkdown with links
    pub fn with_links(lines: Vec<Line<'static>>, links: Vec<LinkSpan>) -> Self {
        Self { lines, links }
    }
}
