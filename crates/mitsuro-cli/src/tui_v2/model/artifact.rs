//! Stable artifact identity and local interaction state.

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartId(String);

impl PartId {
    pub fn from_semantic(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn scoped(scope: &str, family: &str, ordinal: usize) -> Self {
        Self(format!("{scope}/{family}:{ordinal}"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionLevel {
    Full,
    Preview,
    Summary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactWarning {
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactField {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebResultArtifact {
    pub title: String,
    pub url: String,
    pub age: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebDocumentArtifact {
    pub title: Option<String>,
    pub url: String,
    pub media_type: String,
    pub content: BoundedText,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BoundedText {
    pub text: String,
    pub omitted_bytes: usize,
}

impl BoundedText {
    pub const fn truncated(&self) -> bool {
        self.omitted_bytes > 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArtifactContent {
    Empty,
    Text(BoundedText),
    Fields(Vec<ArtifactField>),
    WebResults(Vec<WebResultArtifact>),
    WebDocument(WebDocumentArtifact),
}

/// File-oriented presentation hints preserved from tool envelopes.
///
/// Used so Read/Write/Edit panels share uniform headers and accurate line
/// numbers (e.g. read `start_line` instead of always renumbering from 1).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactProvenance {
    pub path: Option<String>,
    /// 1-based first line of the content body in the real file.
    pub start_line: Option<u32>,
    pub total_lines: Option<u32>,
    pub lines_returned: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactModel {
    pub content: ArtifactContent,
    pub warning: Option<ArtifactWarning>,
    pub retention: RetentionLevel,
    pub provenance: ArtifactProvenance,
}

impl Default for ArtifactModel {
    fn default() -> Self {
        Self {
            content: ArtifactContent::Empty,
            warning: None,
            retention: RetentionLevel::Full,
            provenance: ArtifactProvenance::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactUiState {
    pub expanded: bool,
    pub fullscreen: bool,
    pub inner_scroll: u32,
    pub follow_live: bool,
}

impl Default for ArtifactUiState {
    fn default() -> Self {
        Self {
            expanded: false,
            fullscreen: false,
            inner_scroll: 0,
            follow_live: true,
        }
    }
}

impl ArtifactUiState {
    pub fn toggle_expanded(&mut self) {
        self.expanded = !self.expanded;
        if !self.expanded {
            self.fullscreen = false;
            self.inner_scroll = 0;
        }
    }

    pub fn set_fullscreen(&mut self, fullscreen: bool) {
        self.expanded |= fullscreen;
        self.fullscreen = fullscreen;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collapse_clears_geometry_that_cannot_remain_visible() {
        let mut state = ArtifactUiState {
            expanded: true,
            fullscreen: true,
            inner_scroll: 24,
            follow_live: false,
        };

        state.toggle_expanded();

        assert!(!state.expanded);
        assert!(!state.fullscreen);
        assert_eq!(state.inner_scroll, 0);
        assert!(!state.follow_live);
    }
}
