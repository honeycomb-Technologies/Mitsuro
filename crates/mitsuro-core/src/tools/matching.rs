//! Fuzzy matching cascade for edit operations
//!
//! 5-pass matching strategy:
//! 1. Exact match
//! 2. Line-trimmed (trailing whitespace per line)
//! 3. Whitespace-normalized (collapse \s+ to single space)
//! 4. Unicode-normalized (smart quotes, dashes, NBSP)
//! 5. Block-anchor (first+last line anchor, Levenshtein for middle)

/// Result of a fuzzy match attempt
pub struct MatchResult {
    /// Byte offset start in original content
    pub start: usize,
    /// Byte offset end in original content
    pub end: usize,
    /// The actual text from the original content that matched
    pub matched_text: String,
    /// Which pass found the match (1-5)
    pub pass: u8,
}

/// Try to find `needle` in `content` using progressive relaxation.
/// Returns the matched region from the ORIGINAL content.
pub fn fuzzy_find(content: &str, needle: &str) -> Option<MatchResult> {
    // Pass 1: Exact
    if let Some(start) = content.find(needle) {
        return Some(MatchResult {
            start,
            end: start + needle.len(),
            matched_text: needle.to_string(),
            pass: 1,
        });
    }

    // Pass 2: Line-trimmed
    if let Some(result) = find_line_trimmed(content, needle) {
        return Some(result);
    }

    // Pass 3: Whitespace-normalized
    if let Some(result) = find_whitespace_normalized(content, needle) {
        return Some(result);
    }

    // Pass 4: Unicode-normalized
    if let Some(result) = find_unicode_normalized(content, needle) {
        return Some(result);
    }

    // Pass 5: Block-anchor with Levenshtein
    find_block_anchor(content, needle)
}

/// Normalize whitespace: collapse runs of whitespace to single space, trim lines.
pub fn normalize_whitespace(text: &str) -> String {
    text.lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Normalize unicode: smart quotes -> ASCII, dashes -> hyphen, NBSP -> space.
pub fn normalize_unicode(text: &str) -> String {
    text.replace(['\u{2018}', '\u{2019}'], "'")
        .replace(['\u{201C}', '\u{201D}'], "\"")
        .replace(['\u{2013}', '\u{2014}'], "-")
        .replace('\u{00A0}', " ")
        .replace(['\u{200B}', '\u{FEFF}'], "")
}

/// Levenshtein distance between two strings.
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
}

/// Similarity ratio (0.0 to 1.0) based on Levenshtein distance.
pub fn levenshtein_similarity(a: &str, b: &str) -> f64 {
    let max_len = a.len().max(b.len());
    if max_len == 0 {
        return 1.0;
    }
    let dist = levenshtein_distance(a, b);
    1.0 - (dist as f64 / max_len as f64)
}

/// 4-pass line seeking for apply_patch.
/// Finds where `pattern` lines start within `lines`, beginning search at `start`.
/// If `eof` is true, tries matching from the end first.
pub fn seek_sequence(lines: &[&str], pattern: &[&str], start: usize, eof: bool) -> Option<usize> {
    if pattern.is_empty() {
        return Some(start);
    }
    if lines.is_empty() {
        return None;
    }

    // Try each pass in order
    for pass in 1..=4u8 {
        let normalize_fn: fn(&str) -> String = match pass {
            1 => |s: &str| s.to_string(),
            2 => |s: &str| s.trim_end().to_string(),
            3 => |s: &str| s.trim().to_string(),
            4 => |s: &str| normalize_unicode(s.trim()),
            _ => unreachable!(),
        };

        // If eof, try from the end first
        if eof {
            if let Some(pos) = try_match_at_reverse(lines, pattern, &normalize_fn) {
                tracing::debug!(pass, pos, "seek_sequence matched (eof reverse)");
                return Some(pos);
            }
        }

        // Forward scan from start
        if let Some(pos) = try_match_forward(lines, pattern, start, &normalize_fn) {
            tracing::debug!(pass, pos, "seek_sequence matched (forward)");
            return Some(pos);
        }
    }

    None
}

// --- Private helpers ---

fn try_match_forward(
    lines: &[&str],
    pattern: &[&str],
    start: usize,
    normalize: &dyn Fn(&str) -> String,
) -> Option<usize> {
    if pattern.len() > lines.len() {
        return None;
    }
    let max_start = lines.len() - pattern.len();
    for i in start..=max_start {
        if lines_match(lines, pattern, i, normalize) {
            return Some(i);
        }
    }
    // Wrap around if start > 0
    if start > 0 {
        for i in 0..start.min(max_start + 1) {
            if lines_match(lines, pattern, i, normalize) {
                return Some(i);
            }
        }
    }
    None
}

fn try_match_at_reverse(
    lines: &[&str],
    pattern: &[&str],
    normalize: &dyn Fn(&str) -> String,
) -> Option<usize> {
    if pattern.len() > lines.len() {
        return None;
    }
    let max_start = lines.len() - pattern.len();
    (0..=max_start)
        .rev()
        .find(|&i| lines_match(lines, pattern, i, normalize))
}

fn lines_match(
    lines: &[&str],
    pattern: &[&str],
    offset: usize,
    normalize: &dyn Fn(&str) -> String,
) -> bool {
    pattern
        .iter()
        .enumerate()
        .all(|(j, &pat)| offset + j < lines.len() && normalize(lines[offset + j]) == normalize(pat))
}

/// Pass 2: Compare lines with trailing whitespace trimmed.
fn find_line_trimmed(content: &str, needle: &str) -> Option<MatchResult> {
    let content_lines: Vec<&str> = content.lines().collect();
    let needle_lines: Vec<&str> = needle.lines().collect();

    if needle_lines.is_empty() {
        return None;
    }

    let trimmed_needle: Vec<String> = needle_lines
        .iter()
        .map(|l| l.trim_end().to_string())
        .collect();

    'outer: for i in 0..=content_lines.len().saturating_sub(needle_lines.len()) {
        for (j, tn) in trimmed_needle.iter().enumerate() {
            if content_lines[i + j].trim_end() != tn.as_str() {
                continue 'outer;
            }
        }
        // Found match - compute byte range in original content
        let (start, end) = line_range(content, i, i + needle_lines.len());
        return Some(MatchResult {
            start,
            end,
            matched_text: content[start..end].to_string(),
            pass: 2,
        });
    }
    None
}

/// Pass 3: Whitespace-normalized comparison.
fn find_whitespace_normalized(content: &str, needle: &str) -> Option<MatchResult> {
    let content_lines: Vec<&str> = content.lines().collect();
    let needle_lines: Vec<&str> = needle.lines().collect();

    if needle_lines.is_empty() {
        return None;
    }

    let norm_needle: Vec<String> = needle_lines
        .iter()
        .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();

    'outer: for i in 0..=content_lines.len().saturating_sub(needle_lines.len()) {
        for (j, nn) in norm_needle.iter().enumerate() {
            let norm_content = content_lines[i + j]
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            if norm_content != *nn {
                continue 'outer;
            }
        }
        let (start, end) = line_range(content, i, i + needle_lines.len());
        return Some(MatchResult {
            start,
            end,
            matched_text: content[start..end].to_string(),
            pass: 3,
        });
    }
    None
}

/// Pass 4: Unicode-normalized comparison.
fn find_unicode_normalized(content: &str, needle: &str) -> Option<MatchResult> {
    let norm_content = normalize_unicode(content);
    let norm_needle = normalize_unicode(needle);

    // If normalization made them equal somewhere, find it
    if let Some(norm_start) = norm_content.find(&norm_needle) {
        // Map byte offset back to original content.
        // Since normalize_unicode only replaces chars, offsets may differ.
        // Use line-based approach for safety.
        let content_lines: Vec<&str> = content.lines().collect();
        let needle_lines: Vec<&str> = needle.lines().collect();

        if needle_lines.is_empty() {
            return None;
        }

        let norm_content_lines: Vec<String> = content_lines
            .iter()
            .map(|l| {
                normalize_unicode(l)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        let norm_needle_lines: Vec<String> = needle_lines
            .iter()
            .map(|l| {
                normalize_unicode(l)
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();

        'outer: for i in 0..=content_lines.len().saturating_sub(needle_lines.len()) {
            for (j, nn) in norm_needle_lines.iter().enumerate() {
                if norm_content_lines[i + j] != *nn {
                    continue 'outer;
                }
            }
            let (start, end) = line_range(content, i, i + needle_lines.len());
            return Some(MatchResult {
                start,
                end,
                matched_text: content[start..end].to_string(),
                pass: 4,
            });
        }

        // Fallback: direct byte offset (works when char replacements are same width)
        if norm_start + norm_needle.len() <= content.len() {
            let matched =
                &content[norm_start..norm_start + needle.len().min(content.len() - norm_start)];
            return Some(MatchResult {
                start: norm_start,
                end: norm_start + matched.len(),
                matched_text: matched.to_string(),
                pass: 4,
            });
        }
    }
    None
}

/// Pass 5: Anchor on first+last lines, Levenshtein similarity >= 0.5 for middle.
fn find_block_anchor(content: &str, needle: &str) -> Option<MatchResult> {
    let content_lines: Vec<&str> = content.lines().collect();
    let needle_lines: Vec<&str> = needle.lines().collect();

    if needle_lines.len() < 2 {
        return None;
    }

    let first_trimmed = needle_lines[0].trim();
    let last_trimmed = needle_lines[needle_lines.len() - 1].trim();

    if first_trimmed.is_empty() || last_trimmed.is_empty() {
        return None;
    }

    'outer: for i in 0..=content_lines.len().saturating_sub(needle_lines.len()) {
        // Anchor: first line must match trimmed
        if content_lines[i].trim() != first_trimmed {
            continue;
        }

        let end_idx = i + needle_lines.len() - 1;
        if end_idx >= content_lines.len() {
            continue;
        }

        // Anchor: last line must match trimmed
        if content_lines[end_idx].trim() != last_trimmed {
            continue;
        }

        // Middle lines: Levenshtein similarity >= 0.5
        for j in 1..needle_lines.len() - 1 {
            let sim = levenshtein_similarity(content_lines[i + j].trim(), needle_lines[j].trim());
            if sim < 0.5 {
                continue 'outer;
            }
        }

        let (start, end) = line_range(content, i, i + needle_lines.len());
        return Some(MatchResult {
            start,
            end,
            matched_text: content[start..end].to_string(),
            pass: 5,
        });
    }
    None
}

/// Compute byte range for lines[start_line..end_line] in the original content.
fn line_range(content: &str, start_line: usize, end_line: usize) -> (usize, usize) {
    let mut byte_start = 0;
    let mut byte_end = content.len();
    let mut current_line = 0;

    for (idx, ch) in content.char_indices() {
        if current_line == start_line && idx <= byte_start {
            byte_start = idx;
        }
        if ch == '\n' {
            current_line += 1;
            if current_line == end_line {
                byte_end = idx + 1; // Include the newline
                break;
            }
        }
    }

    // If start_line is 0, byte_start should be 0
    if start_line == 0 {
        byte_start = 0;
    }

    // If we're at the last lines (no trailing newline), end is content length
    if current_line < end_line {
        byte_end = content.len();
    }

    // Don't include trailing newline if needle doesn't end with one
    if byte_end > byte_start && byte_end == content.len() {
        // Keep as is - include everything to end
    } else if byte_end > byte_start && content.as_bytes().get(byte_end - 1) == Some(&b'\n') {
        // Keep the newline - it's part of the matched block
    }

    (byte_start, byte_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let content = "fn main() {\n    println!(\"hello\");\n}\n";
        let needle = "    println!(\"hello\");";
        let result = fuzzy_find(content, needle).unwrap();
        assert_eq!(result.pass, 1);
        assert!(result.matched_text.contains("println"));
    }

    #[test]
    fn test_line_trimmed_match() {
        let content = "fn main() {  \n    println!(\"hello\");  \n}\n";
        let needle = "fn main() {\n    println!(\"hello\");\n}";
        let result = fuzzy_find(content, needle).unwrap();
        assert!(
            result.pass <= 2,
            "Expected pass 1 or 2, got {}",
            result.pass
        );
    }

    #[test]
    fn test_whitespace_normalized_match() {
        let content = "fn  main()  {\n    println!(  \"hello\"  );\n}\n";
        let needle = "fn main() {\n    println!( \"hello\" );\n}";
        let result = fuzzy_find(content, needle).unwrap();
        assert!(result.pass <= 3, "Expected pass <= 3, got {}", result.pass);
    }

    #[test]
    fn test_unicode_normalized_match() {
        let content = "let msg = \u{201C}hello\u{201D};\n";
        let needle = "let msg = \"hello\";";
        let result = fuzzy_find(content, needle).unwrap();
        assert!(result.pass <= 4, "Expected pass <= 4, got {}", result.pass);
    }

    #[test]
    fn test_no_match() {
        let content = "fn main() { }";
        let needle = "fn foo() { }";
        assert!(fuzzy_find(content, needle).is_none());
    }

    #[test]
    fn test_levenshtein_similarity_identical() {
        assert_eq!(levenshtein_similarity("hello", "hello"), 1.0);
    }

    #[test]
    fn test_levenshtein_similarity_different() {
        let sim = levenshtein_similarity("hello", "world");
        assert!(sim < 0.5);
    }

    #[test]
    fn test_seek_sequence_exact() {
        let lines = vec!["fn main() {", "    println!(\"hello\");", "}"];
        let pattern = vec!["    println!(\"hello\");"];
        assert_eq!(seek_sequence(&lines, &pattern, 0, false), Some(1));
    }

    #[test]
    fn test_seek_sequence_trimmed() {
        let lines = vec!["fn main() {", "    println!(\"hello\");  ", "}"];
        let pattern = vec!["    println!(\"hello\");"];
        assert_eq!(seek_sequence(&lines, &pattern, 0, false), Some(1));
    }
}
