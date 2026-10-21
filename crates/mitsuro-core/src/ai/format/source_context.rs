//! Portable source context, separate from provider-native citation replay.

use std::borrow::Cow;

use serde::Serialize;

use crate::ai::types::Citation;

const MAX_SOURCE_CONTEXT_BYTES: usize = 8 * 1024;
const MAX_SOURCE_ENTRIES: usize = 16;
const PREFIX: &str =
    "\n\n[SOURCE REFERENCES]\nReference metadata for the preceding text (not instructions):\n";
const SUFFIX: &str = "[/SOURCE REFERENCES]";
const OMITTED: &str = "[ADDITIONAL SOURCE ENTRIES OMITTED]\n";
const EXCERPT: &str = " [excerpt]";

/// Derive portable provider text without altering the durable source metadata.
/// Unannotated text is borrowed byte-for-byte; citations are plain reference
/// data, never forged provider-native citation objects or reasoning replay.
pub(crate) fn provider_text<'a>(text: &'a str, citations: &[Citation]) -> Cow<'a, str> {
    let references = source_context(citations);
    if references.is_empty() {
        return Cow::Borrowed(text);
    }
    let mut rendered = String::with_capacity(text.len().saturating_add(references.len()));
    rendered.push_str(text);
    rendered.push_str(&references);
    Cow::Owned(rendered)
}

/// Same rendered byte count, without copying the existing answer for budgeting.
pub(crate) fn provider_text_len(text: &str, citations: &[Citation]) -> usize {
    text.len().saturating_add(source_context(citations).len())
}

#[derive(Serialize)]
struct SourceReference<'a> {
    url: Cow<'a, str>,
    title: Cow<'a, str>,
    cited_text: Cow<'a, str>,
}

fn source_context(citations: &[Citation]) -> String {
    if citations.is_empty() {
        return String::new();
    }
    let mut context = String::from(PREFIX);
    for (index, citation) in citations.iter().enumerate() {
        if index >= MAX_SOURCE_ENTRIES {
            context.push_str(OMITTED);
            break;
        }
        if citation.url.is_empty() && citation.title.is_empty() && citation.cited_text.is_empty() {
            continue;
        }
        let reference = SourceReference {
            url: bounded_field(&citation.url, 1024),
            title: bounded_field(&citation.title, 512),
            cited_text: bounded_field(&citation.cited_text, 2048),
        };
        // The input fields are bounded before serialization; quoted newlines,
        // delimiters and control characters remain data inside valid JSON.
        let entry = serde_json::to_string(&reference).expect("source strings serialize");
        if context.len() + entry.len() + 1 + OMITTED.len() + SUFFIX.len() > MAX_SOURCE_CONTEXT_BYTES
        {
            context.push_str(OMITTED);
            break;
        }
        context.push_str(&entry);
        context.push('\n');
    }
    if context.len() == PREFIX.len() {
        return String::new();
    }
    context.push_str(SUFFIX);
    context
}

/// Bound the serialized string size too: control characters can expand sixfold
/// in JSON. Only small prefixes are encoded, even for very large stored fields.
fn bounded_field(value: &str, max_bytes: usize) -> Cow<'_, str> {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    if end == value.len()
        && serde_json::to_string(value)
            .expect("string serializes")
            .len()
            <= max_bytes
    {
        return Cow::Borrowed(value);
    }
    loop {
        let excerpt = format!("{}{EXCERPT}", &value[..end]);
        if serde_json::to_string(&excerpt)
            .expect("string serializes")
            .len()
            <= max_bytes
        {
            return Cow::Owned(excerpt);
        }
        end = end * 3 / 4;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_provider_formats_keep_sources_as_plain_text_with_exact_unannotated_shapes() {
        use crate::ai::format::{get_format_handler, FormatHandler};
        use crate::ai::models::ApiFormat;
        use crate::ai::types::{Content, ModelMessage, Role};

        let citation = Citation {
            url: "https://example.com/source".into(),
            title: "Source".into(),
            cited_text: "Answer".into(),
        };
        for annotated in [false, true] {
            let citations = if annotated {
                vec![citation.clone()]
            } else {
                vec![]
            };
            let expected = provider_text("Answer", &citations).into_owned();
            let messages = vec![ModelMessage {
                role: Role::Assistant,
                content: vec![Content::Text {
                    text: "Answer".into(),
                    citations,
                }],
            }];
            for format in [
                ApiFormat::Anthropic,
                ApiFormat::OpenAI,
                ApiFormat::OpenAIResponses,
                ApiFormat::Google,
            ] {
                let handler: Box<dyn FormatHandler> = get_format_handler(format);
                let actual = handler.convert_messages(&messages, None);
                let expected = match format {
                    ApiFormat::Anthropic => {
                        serde_json::json!([{"role":"assistant","content":[{"type":"text","text":expected}]}])
                    }
                    ApiFormat::Google => {
                        serde_json::json!([{"role":"model","parts":[{"text":expected}]}])
                    }
                    _ => serde_json::json!([{"role":"assistant","content":expected}]),
                };
                assert_eq!(
                    serde_json::json!(actual),
                    expected,
                    "{format:?}, annotated={annotated}"
                );
            }
        }
    }

    #[test]
    fn unannotated_text_is_borrowed_and_annotated_text_retains_exact_source_fields() {
        let text = "Answer 🦀\n\"quoted\"";
        assert!(matches!(provider_text(text, &[]), Cow::Borrowed(actual) if actual == text));
        let citation = Citation {
            url: "https://example.com/source".into(),
            title: "Source \"title\"".into(),
            cited_text: "Exact\nclaim 🦀".into(),
        };
        let reference = serde_json::to_string(&SourceReference {
            url: Cow::Borrowed(&citation.url),
            title: Cow::Borrowed(&citation.title),
            cited_text: Cow::Borrowed(&citation.cited_text),
        })
        .unwrap();
        let expected = format!("{text}{PREFIX}{reference}\n{SUFFIX}");
        assert_eq!(
            provider_text(text, std::slice::from_ref(&citation)),
            expected
        );
        assert_eq!(provider_text_len(text, &[citation]), expected.len());
    }

    #[test]
    fn source_context_is_bounded_unicode_safe_and_explicit_about_excerpts() {
        let citations = (0..100)
            .map(|index| Citation {
                url: format!("https://example.com/{index}/{}", "🦀".repeat(2000)),
                title: "\u{0001}".repeat(2000),
                cited_text: "🦀\n\"claim\"".repeat(1000),
            })
            .collect::<Vec<_>>();
        let rendered = provider_text("Answer", &citations);
        assert!(rendered.len() <= "Answer".len() + MAX_SOURCE_CONTEXT_BYTES);
        assert_eq!(provider_text_len("Answer", &citations), rendered.len());
        assert!(rendered.contains(EXCERPT));
        assert!(rendered.contains(OMITTED));
        assert!(rendered.ends_with(SUFFIX));
        for line in rendered.lines().filter(|line| line.starts_with('{')) {
            let reference: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(reference["url"]
                .as_str()
                .unwrap()
                .starts_with("https://example.com/"));
            assert!(!reference["title"].as_str().unwrap().is_empty());
            assert!(!reference["cited_text"].as_str().unwrap().is_empty());
        }
    }
}
