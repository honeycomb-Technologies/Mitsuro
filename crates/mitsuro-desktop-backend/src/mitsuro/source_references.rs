//! Source presentation for the text-only desktop conversation contract.

use std::collections::HashSet;

use mitsuro_client::Citation;

/// References are emitted at the next content boundary, while source identity
/// remains deduplicated until the assistant response ends.
#[derive(Default)]
pub(super) struct SourceReferences {
    seen: HashSet<String>,
    pending: Vec<String>,
}

impl SourceReferences {
    pub(super) fn extend(&mut self, citations: &[Citation]) {
        for citation in citations {
            let title = citation
                .title
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let url = safe_source_url(&citation.url);
            let (key, reference) = if let Some(url) = url {
                let label = if title.is_empty() { "Source" } else { &title };
                (
                    format!("url:{url}"),
                    format!("[{}](<{url}>)", escape_label(label)),
                )
            } else if !title.is_empty() {
                (format!("title:{title}"), escape_label(&title))
            } else if !citation.url.is_empty() {
                (
                    format!("unlinked:{}", citation.url),
                    "Source (link unavailable)".into(),
                )
            } else {
                continue;
            };
            if self.seen.insert(key) {
                self.pending.push(reference);
            }
        }
    }

    pub(super) fn take_text(&mut self) -> Option<String> {
        if self.pending.is_empty() {
            return None;
        }
        let mut text = String::from("\n\nSources:\n");
        for reference in self.pending.drain(..) {
            text.push_str("- ");
            text.push_str(&reference);
            text.push('\n');
        }
        Some(text)
    }

    pub(super) fn end_response(&mut self) {
        debug_assert!(
            self.pending.is_empty(),
            "flush references before ending a response"
        );
        self.seen.clear();
    }
}

fn safe_source_url(value: &str) -> Option<String> {
    let value = value.trim();
    if value
        .chars()
        .any(|character| character.is_control() || character.is_whitespace())
    {
        return None;
    }
    let url = url::Url::parse(value).ok()?;
    (matches!(url.scheme(), "http" | "https")
        && url.has_host()
        && url.username().is_empty()
        && url.password().is_none())
    .then(|| url.to_string().replace('<', "%3C").replace('>', "%3E"))
}

fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars().filter(|character| !character.is_control()) {
        if matches!(
            character,
            '\\' | '[' | ']' | '(' | ')' | '*' | '_' | '`' | '<' | '>' | '#' | '!' | '|'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn citation(url: &str, title: &str) -> Citation {
        Citation {
            url: url.into(),
            title: title.into(),
            cited_text: "supported claim".into(),
        }
    }

    #[test]
    fn references_deduplicate_across_boundaries_and_reset_per_response() {
        let mut references = SourceReferences::default();
        let source = citation("https://example.com/docs", "Docs");
        references.extend(&[source.clone(), source.clone()]);
        assert_eq!(
            references.take_text().unwrap(),
            "\n\nSources:\n- [Docs](<https://example.com/docs>)\n"
        );
        references.extend(std::slice::from_ref(&source));
        assert!(references.take_text().is_none());
        references.end_response();
        references.extend(&[source]);
        assert!(references.take_text().is_some());
    }

    #[test]
    fn references_escape_labels_and_never_link_unsafe_schemes() {
        let mut references = SourceReferences::default();
        references.extend(&[
            citation("https://example.com/a(b)", "[Docs]\n<script>"),
            citation("javascript:alert(1)", "Unsafe [source]"),
            citation("file:///private/data", "Local document"),
            citation("data:text/html,unsafe", ""),
        ]);
        let text = references.take_text().unwrap();
        assert!(text.contains("[\\[Docs\\] \\<script\\>](<https://example.com/a(b)>)"));
        assert!(text.contains("Unsafe \\[source\\]"));
        assert!(text.contains("Local document"));
        assert!(text.contains("Source (link unavailable)"));
        assert!(!text.contains("javascript:"));
        assert!(!text.contains("file:"));
        assert!(!text.contains("data:"));
    }
}
