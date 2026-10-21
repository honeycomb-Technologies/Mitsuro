//! Packed syntax highlighting for tool artifacts (read / write / edit).
//!
//! Uses syntect for quality multi-language highlighting. Token roles are
//! theme-independent so SemanticTheme can map colors at paint time.
//!
//! Results are cached by (language, content hash) so stable expanded tools do
//! not re-parse on every frame. Output quality is identical to uncached runs.

use std::{
    collections::{HashMap, VecDeque},
    hash::{Hash, Hasher},
    sync::Arc,
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::SyntaxSet;
use syntect::util::LinesWithEndings;

/// Semantic token roles for tool code panels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SyntaxRole {
    #[default]
    Plain,
    Keyword,
    Function,
    String,
    Number,
    Comment,
    Type,
    Variable,
    Punctuation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyntaxChunk {
    pub role: SyntaxRole,
    pub text: String,
}

static SYNTAX_SET: Lazy<SyntaxSet> = Lazy::new(SyntaxSet::load_defaults_newlines);
static THEME_SET: Lazy<ThemeSet> = Lazy::new(ThemeSet::load_defaults);

/// Bounded highlight cache — quality-preserving; only avoids recompute.
const HIGHLIGHT_CACHE_CAP: usize = 64;

static HIGHLIGHT_CACHE: Lazy<Mutex<HighlightCache>> =
    Lazy::new(|| Mutex::new(HighlightCache::new(HIGHLIGHT_CACHE_CAP)));

struct HighlightCache {
    map: HashMap<u64, Arc<Vec<Vec<SyntaxChunk>>>>,
    order: VecDeque<u64>,
    cap: usize,
}

impl HighlightCache {
    fn new(cap: usize) -> Self {
        Self {
            map: HashMap::with_capacity(cap),
            order: VecDeque::with_capacity(cap),
            cap: cap.max(1),
        }
    }

    fn get(&mut self, key: u64) -> Option<Arc<Vec<Vec<SyntaxChunk>>>> {
        if let Some(value) = self.map.get(&key) {
            // Refresh LRU order.
            if let Some(pos) = self.order.iter().position(|k| *k == key) {
                self.order.remove(pos);
                self.order.push_back(key);
            }
            return Some(Arc::clone(value));
        }
        None
    }

    fn insert(&mut self, key: u64, value: Arc<Vec<Vec<SyntaxChunk>>>) {
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.map.entry(key) {
            e.insert(value);
            return;
        }
        while self.map.len() >= self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            } else {
                break;
            }
        }
        self.map.insert(key, value);
        self.order.push_back(key);
    }
}

/// Infer a syntect language token from a file path or explicit language hint.
pub fn language_from_path(path: &str) -> String {
    let file = path.rsplit('/').next().unwrap_or(path);
    if let Some((_, ext)) = file.rsplit_once('.') {
        return match ext.to_ascii_lowercase().as_str() {
            "rs" => "Rust".to_owned(),
            "ts" | "tsx" | "mts" | "cts" => "TypeScript".to_owned(),
            "js" | "jsx" | "mjs" | "cjs" => "JavaScript".to_owned(),
            "py" | "pyi" => "Python".to_owned(),
            "go" => "Go".to_owned(),
            "rb" => "Ruby".to_owned(),
            "java" => "Java".to_owned(),
            "kt" | "kts" => "Kotlin".to_owned(),
            "swift" => "Swift".to_owned(),
            "c" | "h" => "C".to_owned(),
            "cpp" | "cc" | "cxx" | "hpp" | "hh" | "hxx" => "C++".to_owned(),
            "cs" => "C#".to_owned(),
            "php" => "PHP".to_owned(),
            "sh" | "bash" | "zsh" => "Bash".to_owned(),
            "toml" => "TOML".to_owned(),
            "yaml" | "yml" => "YAML".to_owned(),
            "json" | "jsonc" => "JSON".to_owned(),
            "md" | "mdx" => "Markdown".to_owned(),
            "html" | "htm" => "HTML".to_owned(),
            "css" | "scss" => "CSS".to_owned(),
            "sql" => "SQL".to_owned(),
            "xml" | "svg" => "XML".to_owned(),
            "lua" => "Lua".to_owned(),
            "r" => "R".to_owned(),
            "scala" => "Scala".to_owned(),
            "hs" => "Haskell".to_owned(),
            "ex" | "exs" => "Elixir".to_owned(),
            "erl" => "Erlang".to_owned(),
            "zig" => "Zig".to_owned(),
            "nim" => "Nim".to_owned(),
            "dart" => "Dart".to_owned(),
            "vue" => "Vue".to_owned(),
            "svelte" => "Svelte".to_owned(),
            "dockerfile" => "Dockerfile".to_owned(),
            "makefile" | "mk" => "Makefile".to_owned(),
            other => other.to_owned(),
        };
    }
    match file.to_ascii_lowercase().as_str() {
        "dockerfile" => "Dockerfile".to_owned(),
        "makefile" | "gnumakefile" => "Makefile".to_owned(),
        "cargo.toml" | "pyproject.toml" => "TOML".to_owned(),
        "package.json" | "tsconfig.json" => "JSON".to_owned(),
        _ => "Plain Text".to_owned(),
    }
}

/// Highlight `code` into per-line role chunks. Empty input yields one empty line.
///
/// Cached: same (language, code) always returns equivalent tokens.
pub fn highlight_roles(code: &str, language: &str) -> Arc<Vec<Vec<SyntaxChunk>>> {
    let key = {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        language.hash(&mut hasher);
        code.hash(&mut hasher);
        hasher.finish()
    };
    {
        let mut cache = HIGHLIGHT_CACHE.lock();
        if let Some(hit) = cache.get(key) {
            return hit;
        }
    }
    let computed = Arc::new(highlight_roles_uncached(code, language));
    HIGHLIGHT_CACHE.lock().insert(key, Arc::clone(&computed));
    computed
}

fn highlight_roles_uncached(code: &str, language: &str) -> Vec<Vec<SyntaxChunk>> {
    let syntax = SYNTAX_SET
        .find_syntax_by_token(language)
        .or_else(|| SYNTAX_SET.find_syntax_by_name(language))
        .or_else(|| SYNTAX_SET.find_syntax_by_extension(language))
        .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text());

    let theme = &THEME_SET.themes["base16-ocean.dark"];
    let mut highlighter = HighlightLines::new(syntax, theme);
    let mut lines = Vec::new();

    for line in LinesWithEndings::from(code) {
        let ranges = highlighter
            .highlight_line(line, &SYNTAX_SET)
            .unwrap_or_default();
        let chunks: Vec<SyntaxChunk> = ranges
            .into_iter()
            .map(|(style, text)| SyntaxChunk {
                role: role_from_base16(style.foreground),
                text: text.trim_end_matches('\n').to_owned(),
            })
            .filter(|chunk| !chunk.text.is_empty() || lines.is_empty())
            .collect();
        lines.push(if chunks.is_empty() {
            vec![SyntaxChunk {
                role: SyntaxRole::Plain,
                text: String::new(),
            }]
        } else {
            chunks
        });
    }

    if lines.is_empty() {
        lines.push(vec![SyntaxChunk {
            role: SyntaxRole::Plain,
            text: String::new(),
        }]);
    }
    lines
}

fn role_from_base16(color: syntect::highlighting::Color) -> SyntaxRole {
    // base16-ocean.dark token colors (same mapping as tui/utils/syntax.rs).
    match (color.r, color.g, color.b) {
        (101, 115, 126) => SyntaxRole::Comment,
        (163, 190, 140) => SyntaxRole::String,
        (208, 135, 112) => SyntaxRole::Number,
        (180, 142, 173) => SyntaxRole::Keyword,
        (143, 161, 179) => SyntaxRole::Function,
        (235, 203, 139) => SyntaxRole::Type,
        (191, 97, 106) => SyntaxRole::Variable,
        (150, 181, 180) => SyntaxRole::Type,
        (192, 197, 206) | (167, 173, 186) => SyntaxRole::Punctuation,
        _ => SyntaxRole::Plain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_from_rust_path() {
        assert_eq!(language_from_path("crates/foo/src/main.rs"), "Rust");
        assert_eq!(language_from_path("app.tsx"), "TypeScript");
        assert_eq!(language_from_path("Dockerfile"), "Dockerfile");
    }

    #[test]
    fn highlight_rust_keywords() {
        let lines = highlight_roles("fn main() {}\n", "Rust");
        assert!(!lines.is_empty());
        let joined: String = lines[0].iter().map(|c| c.text.as_str()).collect();
        assert!(joined.contains("fn"));
        assert!(lines[0].iter().any(|c| c.role == SyntaxRole::Keyword));
        // Second call hits cache — same quality.
        let again = highlight_roles("fn main() {}\n", "Rust");
        assert_eq!(lines, again);
    }
}
