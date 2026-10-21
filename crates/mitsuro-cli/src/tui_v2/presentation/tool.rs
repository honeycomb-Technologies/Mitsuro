//! Tool models reduced to compact, renderer-ready semantics.
//!
//! Performance: collapsed tools never highlight or materialize panel lines.
//! Expanded code/diff panels share a content-keyed cache so stable tools do not
//! re-diff / re-highlight every frame. Visual quality is unchanged.

use std::{
    collections::{HashMap, VecDeque},
    hash::{Hash, Hasher},
    sync::Arc,
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use similar::{ChangeTag, TextDiff};

use crate::tui_v2::{
    components::primitive::status_glyph::StatusKind,
    model::{
        artifact::{ArtifactContent, ArtifactModel},
        conversation::{ToolPart, ToolStatus},
    },
    presentation::syntax::{highlight_roles, language_from_path, SyntaxChunk, SyntaxRole},
};

/// Bounded cache of fully built expanded code/diff panels (quality-preserving).
const ARTIFACT_VIEW_CACHE_CAP: usize = 48;

static ARTIFACT_VIEW_CACHE: Lazy<Mutex<ArtifactViewCache>> =
    Lazy::new(|| Mutex::new(ArtifactViewCache::new(ARTIFACT_VIEW_CACHE_CAP)));

struct CachedArtifactView {
    panel_kind: ArtifactPanelKind,
    lines: Arc<Vec<ArtifactLine>>,
    language: Option<String>,
}

struct ArtifactViewCache {
    map: HashMap<u64, CachedArtifactView>,
    order: VecDeque<u64>,
    cap: usize,
}

impl ArtifactViewCache {
    fn new(cap: usize) -> Self {
        Self {
            map: HashMap::with_capacity(cap),
            order: VecDeque::with_capacity(cap),
            cap: cap.max(1),
        }
    }

    fn get(&mut self, key: u64) -> Option<CachedArtifactView> {
        let value = self.map.get(&key)?;
        if let Some(pos) = self.order.iter().position(|k| *k == key) {
            self.order.remove(pos);
            self.order.push_back(key);
        }
        Some(CachedArtifactView {
            panel_kind: value.panel_kind,
            lines: Arc::clone(&value.lines),
            language: value.language.clone(),
        })
    }

    fn insert(&mut self, key: u64, value: CachedArtifactView) {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactLineKind {
    /// Neutral body text.
    Plain,
    /// File/path header or section label.
    Header,
    /// Unified-diff context / meta.
    Meta,
    /// Added line (`+`).
    Add,
    /// Removed line (`-`).
    Remove,
    /// Shell/terminal stream line.
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactLine {
    pub kind: ArtifactLineKind,
    /// Full display text (gutter + content). Used for copy and plain paint.
    pub text: String,
    /// Packed syntax tokens for the code body (no gutter / no diff marker).
    pub chunks: Vec<SyntaxChunk>,
    /// Leading gutter painted in muted style (line number, `$`, `+`/`-`).
    pub gutter: String,
}

impl ArtifactLine {
    fn plain(kind: ArtifactLineKind, text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            kind,
            text,
            chunks: Vec::new(),
            gutter: String::new(),
        }
    }

    fn with_syntax(
        kind: ArtifactLineKind,
        gutter: impl Into<String>,
        body: impl Into<String>,
        chunks: Vec<SyntaxChunk>,
    ) -> Self {
        let gutter = gutter.into();
        let body = body.into();
        let text = format!("{gutter}{body}");
        Self {
            kind,
            text,
            chunks,
            gutter,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArtifactPanelKind {
    /// Generic expandable body.
    Generic,
    /// Syntax-highlighted code or file read.
    Code,
    /// Edit/write unified diff (syntax on body).
    Diff,
    /// Live shell transcript.
    Terminal,
    /// Sub-agent live chat (dialog + tool use), scrollable like main stream.
    AgentChat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolDisplay {
    pub label: String,
    pub summary: String,
    pub metadata: String,
    pub status: StatusKind,
    pub expandable: bool,
    pub expanded: bool,
    pub panel_kind: ArtifactPanelKind,
    /// Shared across frames when content is stable (Arc clone is O(1)).
    pub artifact_lines: Arc<Vec<ArtifactLine>>,
    /// Language token used for highlighting (e.g. "Rust").
    pub language: Option<String>,
}

impl ToolDisplay {
    pub fn from_part(part: &ToolPart, expanded: bool) -> Self {
        let (classified_kind, classified_lang) = classify_tool(part);
        let expandable = tool_has_expandable_body(part);
        // Collapsed: summary row only — no syntect, no line materialization.
        // Expanded: full quality panel, content-keyed cache for code/diff.
        let (panel_kind, artifact_lines, language) = if expanded {
            resolve_artifact_view(part, classified_kind, classified_lang)
        } else {
            (classified_kind, Arc::new(Vec::new()), classified_lang)
        };
        Self {
            label: tool_label(&part.name).to_owned(),
            summary: tool_summary(part),
            metadata: status_label(part).to_owned(),
            status: status_kind(part),
            expandable,
            expanded,
            panel_kind,
            artifact_lines,
            language,
        }
    }

    pub fn plain_lines(&self) -> Vec<String> {
        self.artifact_lines
            .iter()
            .map(|line| line.text.clone())
            .collect()
    }
}

fn classify_tool(part: &ToolPart) -> (ArtifactPanelKind, Option<String>) {
    let name = part.name.to_ascii_lowercase();
    match name.as_str() {
        "bash" | "shell" | "terminal" => (ArtifactPanelKind::Terminal, None),
        // Delegated sub-agents: scrollable chat of dialog + tool use.
        "agent" | "subagent" => (ArtifactPanelKind::AgentChat, None),
        "read" | "read_file" => {
            let path = field(part, "path")
                .or_else(|| field(part, "file_path"))
                .unwrap_or("");
            let language = (!path.is_empty()).then(|| language_from_path(path));
            (ArtifactPanelKind::Code, language)
        }
        "write" | "write_file" | "edit" | "str_replace" | "apply_patch" | "multiedit" => {
            let path = field(part, "path")
                .or_else(|| field(part, "file_path"))
                .unwrap_or("file");
            // Prefer Diff when we can form one; fall back at resolve time.
            (ArtifactPanelKind::Diff, Some(language_from_path(path)))
        }
        _ => (ArtifactPanelKind::Generic, None),
    }
}

fn tool_has_expandable_body(part: &ToolPart) -> bool {
    if field(part, "command").is_some()
        || field(part, "old_string").is_some()
        || field(part, "old_str").is_some()
        || field(part, "new_string").is_some()
        || field(part, "new_str").is_some()
        || field(part, "content").is_some()
    {
        return true;
    }
    match &part.artifact.content {
        ArtifactContent::Empty => part.artifact.warning.is_some(),
        ArtifactContent::Text(text) => !text.text.is_empty() || text.truncated(),
        ArtifactContent::Fields(fields) => !fields.is_empty(),
        ArtifactContent::WebResults(results) => !results.is_empty(),
        ArtifactContent::WebDocument(document) => {
            !document.content.text.is_empty() || document.content.truncated()
        }
    }
}

fn resolve_artifact_view(
    part: &ToolPart,
    classified: ArtifactPanelKind,
    classified_lang: Option<String>,
) -> (ArtifactPanelKind, Arc<Vec<ArtifactLine>>, Option<String>) {
    // Terminal / agent chat / generic stream often; caching would thrash.
    if matches!(
        classified,
        ArtifactPanelKind::Terminal | ArtifactPanelKind::AgentChat | ArtifactPanelKind::Generic
    ) {
        let (kind, lines, language) = build_artifact_view(part);
        return (kind, Arc::new(lines), language.or(classified_lang));
    }

    let key = artifact_content_key(part);
    {
        let mut cache = ARTIFACT_VIEW_CACHE.lock();
        if let Some(hit) = cache.get(key) {
            return (hit.panel_kind, hit.lines, hit.language.or(classified_lang));
        }
    }

    let (panel_kind, lines, language_owned) = build_artifact_view(part);
    let language = language_owned.or(classified_lang);
    let lines = Arc::new(lines);
    // Only cache expensive code/diff materializations.
    if matches!(
        panel_kind,
        ArtifactPanelKind::Code | ArtifactPanelKind::Diff
    ) {
        ARTIFACT_VIEW_CACHE.lock().insert(
            key,
            CachedArtifactView {
                panel_kind,
                lines: Arc::clone(&lines),
                language: language.clone(),
            },
        );
    }
    (panel_kind, lines, language)
}

fn artifact_content_key(part: &ToolPart) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    part.name.hash(&mut hasher);
    for field in &part.arguments.fields {
        field.key.hash(&mut hasher);
        field.value.hash(&mut hasher);
    }
    match &part.artifact.content {
        ArtifactContent::Empty => 0u8.hash(&mut hasher),
        ArtifactContent::Text(text) => {
            1u8.hash(&mut hasher);
            text.text.hash(&mut hasher);
            text.omitted_bytes.hash(&mut hasher);
        }
        ArtifactContent::Fields(fields) => {
            2u8.hash(&mut hasher);
            for field in fields {
                field.key.hash(&mut hasher);
                field.value.hash(&mut hasher);
            }
        }
        ArtifactContent::WebResults(results) => {
            3u8.hash(&mut hasher);
            for result in results {
                result.title.hash(&mut hasher);
                result.url.hash(&mut hasher);
            }
        }
        ArtifactContent::WebDocument(document) => {
            4u8.hash(&mut hasher);
            document.url.hash(&mut hasher);
            document.content.text.hash(&mut hasher);
            document.content.omitted_bytes.hash(&mut hasher);
        }
    }
    if let Some(warning) = &part.artifact.warning {
        warning.message.hash(&mut hasher);
    }
    let prov = &part.artifact.provenance;
    prov.path.hash(&mut hasher);
    prov.start_line.hash(&mut hasher);
    prov.total_lines.hash(&mut hasher);
    prov.lines_returned.hash(&mut hasher);
    hasher.finish()
}

fn tool_label(name: &str) -> &str {
    match name.to_ascii_lowercase().as_str() {
        "bash" | "shell" | "terminal" => "Bash",
        "read" | "read_file" => "Read",
        "write" | "write_file" => "Write",
        "edit" | "str_replace" | "apply_patch" | "multiedit" => "Patch",
        "grep" | "search" => "Search",
        "glob" | "list" | "list_files" => "Files",
        "web_search" => "Web",
        "web_fetch" | "fetch" => "Fetch",
        "agent" | "subagent" => "Agent",
        "askuserquestion" => "Question",
        "planconfirm" => "Plan",
        _ if name.is_empty() => "Tool",
        _ => name,
    }
}

fn tool_summary(part: &ToolPart) -> String {
    let name = part.name.to_ascii_lowercase();
    // Structured multi-question payloads render as junk (`questions[0] …`).
    // The decision dock owns the real prompt — keep the tool row quiet.
    if matches!(
        name.as_str(),
        "askuserquestion" | "planconfirm" | "plan_confirm"
    ) {
        return String::new();
    }
    const PRIORITY: &[&str] = &[
        "command",
        "path",
        "file_path",
        "pattern",
        "query",
        "description",
        "agent_type",
        "url",
    ];
    for preferred in PRIORITY {
        if let Some(field) = part
            .arguments
            .fields
            .iter()
            .find(|field| field.key.eq_ignore_ascii_case(preferred))
        {
            let value = field.value.lines().next().unwrap_or_default().trim();
            if !value.is_empty() {
                return value.to_owned();
            }
        }
    }
    if let Some(field) = part.arguments.fields.first() {
        let value = field.value.lines().next().unwrap_or_default().trim();
        if !value.is_empty() {
            return format!("{} {value}", field.key);
        }
    }
    // No useful args yet — stay quiet. Status glyph + metadata already convey
    // progress (e.g. `• Write receiving`), so avoid "receiving arguments receiving".
    String::new()
}

fn status_label(part: &ToolPart) -> &'static str {
    let interactive = is_interactive_tool(&part.name);
    let agent = is_agent_tool(&part.name);
    match part.status {
        // Agent tools: Pending is usually "accepted, about to run / already in flight"
        // under parallel tool calls — never "queued" (that reads like a backlog).
        ToolStatus::Receiving if agent => "starting",
        ToolStatus::Pending if agent => "starting",
        ToolStatus::Running if agent => "running",
        ToolStatus::Receiving => "receiving",
        ToolStatus::Pending if interactive => "waiting",
        ToolStatus::Pending => "queued",
        // AskUser / plan confirm can be mis-settled to AwaitingApproval; still say waiting.
        ToolStatus::AwaitingApproval if interactive => "waiting",
        ToolStatus::AwaitingApproval => "approval",
        ToolStatus::Approved => "approved",
        ToolStatus::Running => "running",
        ToolStatus::Succeeded => "done",
        ToolStatus::Failed => "failed",
        ToolStatus::Denied => "denied",
        ToolStatus::Interrupted => "stopped",
    }
}

fn status_kind(part: &ToolPart) -> StatusKind {
    let interactive = is_interactive_tool(&part.name);
    let agent = is_agent_tool(&part.name);
    match part.status {
        ToolStatus::Receiving | ToolStatus::Running => StatusKind::Running,
        // Interactive prompts need attention, not a spinner.
        ToolStatus::Pending if interactive => StatusKind::AwaitingAuthority,
        // Agents + ordinary tools: Pending still spins — parallel batches sit here
        // until each tool hits ToolExecuting / background_started.
        ToolStatus::Pending if agent => StatusKind::Running,
        ToolStatus::Pending => StatusKind::Running,
        ToolStatus::AwaitingApproval => StatusKind::AwaitingAuthority,
        ToolStatus::Approved => StatusKind::Idle,
        ToolStatus::Succeeded => StatusKind::Success,
        ToolStatus::Failed => StatusKind::Failed,
        ToolStatus::Denied | ToolStatus::Interrupted => StatusKind::Cancelled,
    }
}

fn is_agent_tool(name: &str) -> bool {
    matches!(name.to_ascii_lowercase().as_str(), "agent" | "subagent")
}

fn is_interactive_tool(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "askuserquestion" | "planconfirm" | "plan_confirm"
    )
}

fn field<'a>(part: &'a ToolPart, key: &str) -> Option<&'a str> {
    part.arguments
        .fields
        .iter()
        .find(|field| field.key.eq_ignore_ascii_case(key))
        .map(|field| field.value.as_str())
        .or_else(|| field_suffix(part, key))
}

/// Match nested flattened keys (`edits[0].old_string`, `data.content`, …).
fn field_suffix<'a>(part: &'a ToolPart, key: &str) -> Option<&'a str> {
    let key_lower = key.to_ascii_lowercase();
    part.arguments.fields.iter().find_map(|field| {
        let fk = field.key.to_ascii_lowercase();
        let leaf = fk
            .rsplit(['.', '[', ']'])
            .find(|s| !s.is_empty())
            .unwrap_or(fk.as_str());
        (leaf == key_lower).then_some(field.value.as_str())
    })
}

fn artifact_text(part: &ToolPart) -> Option<&str> {
    match &part.artifact.content {
        ArtifactContent::Text(text) if !text.text.is_empty() => Some(text.text.as_str()),
        _ => None,
    }
}

fn build_artifact_view(part: &ToolPart) -> (ArtifactPanelKind, Vec<ArtifactLine>, Option<String>) {
    let name = part.name.to_ascii_lowercase();
    match name.as_str() {
        "bash" | "shell" | "terminal" => (ArtifactPanelKind::Terminal, terminal_lines(part), None),
        "agent" | "subagent" => (ArtifactPanelKind::AgentChat, agent_chat_lines(part), None),
        "read" | "read_file" => {
            let path = field(part, "path")
                .or_else(|| field(part, "file_path"))
                .unwrap_or("");
            let language = if path.is_empty() {
                None
            } else {
                Some(language_from_path(path))
            };
            (
                ArtifactPanelKind::Code,
                code_read_lines(part, language.as_deref()),
                language,
            )
        }
        "write" | "write_file" | "edit" | "str_replace" | "apply_patch" | "multiedit" => {
            let path = field(part, "path")
                .or_else(|| field(part, "file_path"))
                .unwrap_or("file");
            let language = Some(language_from_path(path));
            let lines = diff_lines(part, language.as_deref());
            if lines.is_empty() {
                (
                    ArtifactPanelKind::Generic,
                    generic_lines(&part.artifact),
                    language,
                )
            } else {
                (ArtifactPanelKind::Diff, lines, language)
            }
        }
        _ => (
            ArtifactPanelKind::Generic,
            generic_lines(&part.artifact),
            None,
        ),
    }
}

/// Chat-style lines for an expanded sub-agent stream.
fn agent_chat_lines(part: &ToolPart) -> Vec<ArtifactLine> {
    let text = artifact_text(part).unwrap_or("");
    if text.is_empty() {
        return vec![ArtifactLine::plain(
            ArtifactLineKind::Meta,
            "  (no stream yet — expand while the agent runs)",
        )];
    }
    text.lines()
        .map(|line| {
            let kind = if line.starts_with("assistant") {
                ArtifactLineKind::Plain
            } else if line.starts_with('✓') || line.starts_with("+") {
                ArtifactLineKind::Add
            } else if line.starts_with('×') || line.starts_with('x') {
                ArtifactLineKind::Remove
            } else if line.starts_with("──") || line.starts_with("  think") {
                ArtifactLineKind::Meta
            } else if line.starts_with('·') || line.starts_with("tool") {
                ArtifactLineKind::Header
            } else {
                ArtifactLineKind::Plain
            };
            ArtifactLine::plain(kind, line)
        })
        .collect()
}

fn terminal_lines(part: &ToolPart) -> Vec<ArtifactLine> {
    let mut lines = Vec::new();
    if let Some(command) = field(part, "command") {
        lines.push(ArtifactLine::with_syntax(
            ArtifactLineKind::Meta,
            "$ ",
            command,
            vec![SyntaxChunk {
                role: SyntaxRole::Plain,
                text: command.to_owned(),
            }],
        ));
    }
    match &part.artifact.content {
        ArtifactContent::Text(text) => {
            for line in text.text.lines() {
                lines.push(ArtifactLine::plain(ArtifactLineKind::Terminal, line));
            }
            if text.truncated() {
                lines.push(ArtifactLine::plain(
                    ArtifactLineKind::Meta,
                    format!("… {} bytes omitted …", text.omitted_bytes),
                ));
            }
        }
        ArtifactContent::Empty => {}
        _ => lines.extend(generic_lines(&part.artifact)),
    }
    if let Some(warning) = &part.artifact.warning {
        lines.push(ArtifactLine::plain(
            ArtifactLineKind::Meta,
            format!("warning: {}", warning.message),
        ));
    }
    lines
}

fn code_read_lines(part: &ToolPart, language: Option<&str>) -> Vec<ArtifactLine> {
    let mut lines = Vec::new();
    let path = part
        .artifact
        .provenance
        .path
        .as_deref()
        .or_else(|| field(part, "path"))
        .or_else(|| field(part, "file_path"))
        .unwrap_or("");
    let start_line = resolve_read_start_line(part);
    match &part.artifact.content {
        ArtifactContent::Text(text) => {
            let body = &text.text;
            let source_lines: Vec<&str> = body.lines().collect();
            let body_len = source_lines.len() as u32;
            let end_line = start_line.saturating_add(body_len.saturating_sub(1));
            lines.push(ArtifactLine::plain(
                ArtifactLineKind::Header,
                file_scope_header(
                    path,
                    start_line,
                    end_line,
                    part.artifact.provenance.total_lines,
                ),
            ));
            let highlighted = language.map(|lang| highlight_roles(body, lang));
            let count = source_lines
                .len()
                .max(highlighted.as_ref().map(|h| h.len()).unwrap_or(0));
            for index in 0..count {
                let body_line = source_lines.get(index).copied().unwrap_or("");
                let chunks = highlighted
                    .as_ref()
                    .and_then(|h| h.get(index))
                    .cloned()
                    .unwrap_or_else(|| {
                        vec![SyntaxChunk {
                            role: SyntaxRole::Plain,
                            text: body_line.to_owned(),
                        }]
                    });
                let line_no = start_line.saturating_add(index as u32);
                let gutter = format!("{:>4} │ ", line_no);
                lines.push(ArtifactLine::with_syntax(
                    ArtifactLineKind::Plain,
                    gutter,
                    body_line,
                    chunks,
                ));
            }
            if text.truncated() {
                lines.push(ArtifactLine::plain(
                    ArtifactLineKind::Meta,
                    format!("… {} bytes omitted …", text.omitted_bytes),
                ));
            }
        }
        ArtifactContent::Empty => {
            if !path.is_empty() {
                lines.push(ArtifactLine::plain(ArtifactLineKind::Header, path));
            }
        }
        _ => lines.extend(generic_lines(&part.artifact)),
    }
    if let Some(warning) = &part.artifact.warning {
        lines.push(ArtifactLine::plain(
            ArtifactLineKind::Meta,
            format!("warning: {}", warning.message),
        ));
    }
    lines
}

/// 1-based file line for the first body row of a read.
fn resolve_read_start_line(part: &ToolPart) -> u32 {
    if let Some(start) = part.artifact.provenance.start_line.filter(|n| *n > 0) {
        return start;
    }
    // Tool arg `offset` is 1-indexed start line.
    field(part, "offset")
        .and_then(|s| s.trim().parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

fn file_scope_header(path: &str, start: u32, end: u32, total: Option<u32>) -> String {
    let path = if path.is_empty() { "file" } else { path };
    let range = if end >= start && start > 0 {
        if end == start {
            format!("L{start}")
        } else {
            format!("L{start}–{end}")
        }
    } else {
        String::new()
    };
    match (range.is_empty(), total) {
        (true, Some(t)) => format!("{path}  ·  {t} lines"),
        (true, None) => path.to_owned(),
        (false, Some(t)) => format!("{path}  ·  {range} of {t}"),
        (false, None) => format!("{path}  ·  {range}"),
    }
}

fn diff_lines(part: &ToolPart, language: Option<&str>) -> Vec<ArtifactLine> {
    let path = part
        .artifact
        .provenance
        .path
        .as_deref()
        .or_else(|| field(part, "path"))
        .or_else(|| field(part, "file_path"))
        .unwrap_or("file");
    let tool = part.name.to_ascii_lowercase();

    // 1) Prefer tool-result unified diff (envelope `diff` unwrapped to Text).
    if let Some(text) = artifact_text(part) {
        if looks_like_unified_diff(text) {
            return decorate_diff_panel(path, parse_unified_diff_lines(text, language));
        }
        // apply_patch often stores the patch body as the primary text payload.
        if tool == "apply_patch" && looks_like_apply_patch(text) {
            return decorate_diff_panel(path, parse_unified_diff_lines(text, language));
        }
    }

    // 2) apply_patch argument body.
    if tool == "apply_patch" {
        if let Some(patch) = field(part, "patch") {
            if !patch.is_empty() {
                return decorate_diff_panel(path, parse_unified_diff_lines(patch, language));
            }
        }
    }

    // 3) Rebuild from edit/write arguments (including nested multiedit keys).
    let old = field(part, "old_string")
        .or_else(|| field(part, "old_str"))
        .unwrap_or("");
    let new = field(part, "new_string")
        .or_else(|| field(part, "new_str"))
        .or_else(|| field(part, "content"))
        .unwrap_or("");

    if !old.is_empty()
        || (!new.is_empty()
            && matches!(
                tool.as_str(),
                "edit" | "str_replace" | "multiedit" | "write" | "write_file"
            ))
    {
        let old = if old.is_empty() && matches!(tool.as_str(), "write" | "write_file") {
            ""
        } else {
            old
        };
        if old != new {
            return decorate_diff_panel(path, unified_diff_lines(path, old, new, language));
        }
    }

    // 4) Write result that is pure new content (no unified markers).
    if matches!(tool.as_str(), "write" | "write_file") {
        if let Some(text) = artifact_text(part).filter(|t| !t.is_empty()) {
            return decorate_diff_panel(path, unified_diff_lines(path, "", text, language));
        }
    }

    Vec::new()
}

/// Shared header + stats so Read/Write/Edit feel like one family.
fn decorate_diff_panel(path: &str, mut body: Vec<ArtifactLine>) -> Vec<ArtifactLine> {
    let mut adds = 0u32;
    let mut removes = 0u32;
    for line in &body {
        match line.kind {
            ArtifactLineKind::Add => adds += 1,
            ArtifactLineKind::Remove => removes += 1,
            _ => {}
        }
    }
    // Drop raw ---/+++ file headers; we paint a uniform scope header instead.
    body.retain(|line| {
        !(line.kind == ArtifactLineKind::Header
            && (line.text.starts_with("---") || line.text.starts_with("+++")))
    });
    let mut out = vec![ArtifactLine::plain(
        ArtifactLineKind::Header,
        diff_scope_header(path, adds, removes),
    )];
    out.append(&mut body);
    out
}

fn diff_scope_header(path: &str, adds: u32, removes: u32) -> String {
    let path = if path.is_empty() { "file" } else { path };
    format!("{path}  ·  +{adds} −{removes}")
}

fn looks_like_apply_patch(text: &str) -> bool {
    text.contains("*** Begin Patch")
        || text.contains("*** Update File:")
        || text.contains("*** Add File:")
        || text.contains("*** Delete File:")
}

fn looks_like_unified_diff(text: &str) -> bool {
    text.lines().any(|line| {
        line.starts_with("@@")
            || line.starts_with("---")
            || line.starts_with("+++")
            || (line.starts_with('+') && !line.starts_with("+++"))
            || (line.starts_with('-') && !line.starts_with("---"))
    })
}

fn parse_unified_diff_lines(text: &str, language: Option<&str>) -> Vec<ArtifactLine> {
    let mut lines = Vec::new();
    let mut old_line = 1u32;
    let mut new_line = 1u32;
    for line in text.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            lines.push(ArtifactLine::plain(ArtifactLineKind::Header, line));
            continue;
        }
        if line.starts_with("@@") {
            if let Some((o, n)) = parse_hunk_header(line) {
                old_line = o;
                new_line = n;
            }
            lines.push(ArtifactLine::plain(ArtifactLineKind::Header, line));
            continue;
        }
        let (kind, marker, body, line_no) = if let Some(rest) = line.strip_prefix('+') {
            let no = new_line;
            new_line = new_line.saturating_add(1);
            (ArtifactLineKind::Add, "+", rest, no)
        } else if let Some(rest) = line.strip_prefix('-') {
            let no = old_line;
            old_line = old_line.saturating_add(1);
            (ArtifactLineKind::Remove, "-", rest, no)
        } else if let Some(rest) = line.strip_prefix(' ') {
            let no = new_line;
            old_line = old_line.saturating_add(1);
            new_line = new_line.saturating_add(1);
            (ArtifactLineKind::Meta, " ", rest, no)
        } else {
            lines.push(ArtifactLine::plain(ArtifactLineKind::Meta, line));
            continue;
        };
        let chunks = highlight_body(body, language);
        // Uniform with Read: `  42 │ ±body`
        let gutter = format!("{:>4} │ {marker}", line_no);
        lines.push(ArtifactLine::with_syntax(kind, gutter, body, chunks));
    }
    lines
}

/// Parse `@@ -10,6 +12,8 @@` / `@@ -1 +1 @@` → (old_start, new_start).
fn parse_hunk_header(line: &str) -> Option<(u32, u32)> {
    let mut old = None;
    let mut new = None;
    for token in line.split_whitespace() {
        if let Some(rest) = token.strip_prefix('-') {
            if rest.starts_with(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']) {
                old = Some(hunk_side_start(rest));
            }
        } else if let Some(rest) = token.strip_prefix('+') {
            if rest.starts_with(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']) {
                new = Some(hunk_side_start(rest));
            }
        }
    }
    Some((old?, new?))
}

fn hunk_side_start(side: &str) -> u32 {
    side.split(',')
        .next()
        .and_then(|n| n.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

fn unified_diff_lines(
    path: &str,
    old: &str,
    new: &str,
    language: Option<&str>,
) -> Vec<ArtifactLine> {
    let _ = path;
    let mut lines = Vec::new();
    let old_hl = language.map(|lang| highlight_roles(old, lang));
    let new_hl = language.map(|lang| highlight_roles(new, lang));
    let mut old_idx = 0usize;
    let mut new_idx = 0usize;
    // Rebuild as if from line 1 of the replaced span (string replace is local).
    let mut old_line = 1u32;
    let mut new_line = 1u32;

    let diff = TextDiff::from_lines(old, new);
    for op in diff.ops() {
        for change in diff.iter_changes(op) {
            let value = change.value().trim_end_matches('\n');
            let (kind, marker, line_no) = match change.tag() {
                ChangeTag::Delete => {
                    let no = old_line;
                    old_line = old_line.saturating_add(1);
                    (ArtifactLineKind::Remove, "-", no)
                }
                ChangeTag::Insert => {
                    let no = new_line;
                    new_line = new_line.saturating_add(1);
                    (ArtifactLineKind::Add, "+", no)
                }
                ChangeTag::Equal => {
                    let no = new_line;
                    old_line = old_line.saturating_add(1);
                    new_line = new_line.saturating_add(1);
                    (ArtifactLineKind::Meta, " ", no)
                }
            };
            let chunks = match change.tag() {
                ChangeTag::Delete => {
                    let chunks = take_line_chunks(old_hl.as_ref(), old_idx, value)
                        .unwrap_or_else(|| plain_chunks(value));
                    old_idx += 1;
                    chunks
                }
                ChangeTag::Insert => {
                    let chunks = take_line_chunks(new_hl.as_ref(), new_idx, value)
                        .unwrap_or_else(|| plain_chunks(value));
                    new_idx += 1;
                    chunks
                }
                ChangeTag::Equal => {
                    let chunks = take_line_chunks(old_hl.as_ref(), old_idx, value)
                        .or_else(|| take_line_chunks(new_hl.as_ref(), new_idx, value))
                        .unwrap_or_else(|| plain_chunks(value));
                    old_idx += 1;
                    new_idx += 1;
                    chunks
                }
            };
            let gutter = format!("{:>4} │ {marker}", line_no);
            lines.push(ArtifactLine::with_syntax(kind, gutter, value, chunks));
        }
    }
    if lines.len() > 400 {
        lines.truncate(400);
        lines.push(ArtifactLine::plain(
            ArtifactLineKind::Meta,
            "… diff truncated for terminal …",
        ));
    }
    lines
}

fn take_line_chunks(
    highlighted: Option<&Arc<Vec<Vec<SyntaxChunk>>>>,
    index: usize,
    fallback: &str,
) -> Option<Vec<SyntaxChunk>> {
    highlighted.map(|lines| {
        lines
            .get(index)
            .cloned()
            .unwrap_or_else(|| plain_chunks(fallback))
    })
}

fn highlight_body(body: &str, language: Option<&str>) -> Vec<SyntaxChunk> {
    language
        .map(|lang| {
            highlight_roles(body, lang)
                .first()
                .cloned()
                .unwrap_or_else(|| plain_chunks(body))
        })
        .unwrap_or_else(|| plain_chunks(body))
}

fn plain_chunks(text: &str) -> Vec<SyntaxChunk> {
    vec![SyntaxChunk {
        role: SyntaxRole::Plain,
        text: text.to_owned(),
    }]
}

fn generic_lines(artifact: &ArtifactModel) -> Vec<ArtifactLine> {
    let mut lines = match &artifact.content {
        ArtifactContent::Empty => Vec::new(),
        ArtifactContent::Text(text) => text
            .text
            .lines()
            .map(|line| ArtifactLine::plain(ArtifactLineKind::Plain, line))
            .collect(),
        ArtifactContent::Fields(fields) => fields
            .iter()
            .map(|field| {
                ArtifactLine::plain(
                    ArtifactLineKind::Plain,
                    format!("{}: {}", field.key, field.value),
                )
            })
            .collect(),
        ArtifactContent::WebResults(results) => results
            .iter()
            .map(|result| {
                ArtifactLine::plain(
                    ArtifactLineKind::Plain,
                    format!("{} — {}", result.title, result.url),
                )
            })
            .collect(),
        ArtifactContent::WebDocument(document) => {
            let mut lines = vec![ArtifactLine::plain(
                ArtifactLineKind::Header,
                document
                    .title
                    .clone()
                    .unwrap_or_else(|| document.url.clone()),
            )];
            lines.extend(
                document
                    .content
                    .text
                    .lines()
                    .map(|line| ArtifactLine::plain(ArtifactLineKind::Plain, line)),
            );
            lines
        }
    };
    if let Some(warning) = &artifact.warning {
        lines.push(ArtifactLine::plain(
            ArtifactLineKind::Meta,
            format!("warning: {}", warning.message),
        ));
    }
    lines
}

#[cfg(test)]
mod tests {
    use crate::tui_v2::model::{
        artifact::{
            ArtifactContent, ArtifactField, ArtifactModel, BoundedText, PartId, RetentionLevel,
        },
        conversation::{ToolArguments, ToolPart},
    };

    use super::*;

    #[test]
    fn receiving_write_stays_quiet_until_path_arrives() {
        let bare = ToolPart {
            id: PartId::from_semantic("tool:write-recv"),
            tool_call_id: "write-recv".to_owned(),
            name: "write".to_owned(),
            status: ToolStatus::Receiving,
            arguments: ToolArguments {
                fields: Vec::new(),
                redacted_fields: 0,
            },
            artifact: ArtifactModel::default(),
            server_side: false,
        };
        let display = ToolDisplay::from_part(&bare, false);
        assert_eq!(display.label, "Write");
        assert_eq!(display.summary, "");
        assert_eq!(display.metadata, "receiving");

        let with_path = ToolPart {
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "path".to_owned(),
                    value: "src/main.rs".to_owned(),
                }],
                redacted_fields: 0,
            },
            ..bare
        };
        let display = ToolDisplay::from_part(&with_path, false);
        assert_eq!(display.summary, "src/main.rs");
        assert_eq!(display.metadata, "receiving");
    }

    #[test]
    fn ask_user_question_row_stays_quiet_while_waiting() {
        let part = ToolPart {
            id: PartId::from_semantic("tool:ask"),
            tool_call_id: "ask".to_owned(),
            name: "AskUserQuestion".to_owned(),
            status: ToolStatus::Pending,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "questions[0]".to_owned(),
                    value: "{structured value omitted}".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact: ArtifactModel::default(),
            server_side: false,
        };
        let display = ToolDisplay::from_part(&part, false);
        assert_eq!(display.label, "Question");
        assert_eq!(display.summary, "");
        assert_eq!(display.metadata, "waiting");
    }

    #[test]
    fn running_bash_stays_one_semantic_row_with_latest_progress() {
        let part = ToolPart {
            id: PartId::from_semantic("tool:bash-1"),
            tool_call_id: "bash-1".to_owned(),
            name: "bash".to_owned(),
            status: ToolStatus::Running,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "command".to_owned(),
                    value: "cargo test --workspace".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact: ArtifactModel {
                content: ArtifactContent::Text(BoundedText {
                    text: "Compiling core\n42 tests passed".to_owned(),
                    omitted_bytes: 0,
                }),
                warning: None,
                retention: RetentionLevel::Full,
                provenance: Default::default(),
            },
            server_side: false,
        };

        let collapsed = ToolDisplay::from_part(&part, false);
        assert_eq!(collapsed.label, "Bash");
        assert_eq!(collapsed.summary, "cargo test --workspace");
        assert_eq!(collapsed.metadata, "running");
        assert_eq!(collapsed.panel_kind, ArtifactPanelKind::Terminal);
        assert!(collapsed.expandable);
        assert!(collapsed.artifact_lines.is_empty()); // lazy until expanded
        assert!(!collapsed.expanded);

        let expanded = ToolDisplay::from_part(&part, true);
        assert!(expanded
            .artifact_lines
            .iter()
            .any(|line| line.text.contains("cargo test")));
        assert_eq!(expanded.artifact_lines.len(), 3); // $ cmd + 2 output lines
    }

    #[test]
    fn expanded_code_panel_is_cached_across_frames() {
        let part = ToolPart {
            id: PartId::from_semantic("tool:read-cache"),
            tool_call_id: "read-cache".to_owned(),
            name: "read".to_owned(),
            status: ToolStatus::Succeeded,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "path".to_owned(),
                    value: "src/cache.rs".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact: ArtifactModel {
                content: ArtifactContent::Text(BoundedText {
                    text: "fn cache() {}\n".to_owned(),
                    omitted_bytes: 0,
                }),
                warning: None,
                retention: RetentionLevel::Full,
                provenance: Default::default(),
            },
            server_side: false,
        };
        let a = ToolDisplay::from_part(&part, true);
        let b = ToolDisplay::from_part(&part, true);
        assert!(Arc::ptr_eq(&a.artifact_lines, &b.artifact_lines));
    }

    #[test]
    fn edit_tool_builds_packed_diff_lines() {
        let part = ToolPart {
            id: PartId::from_semantic("tool:edit-1"),
            tool_call_id: "edit-1".to_owned(),
            name: "edit".to_owned(),
            status: ToolStatus::Succeeded,
            arguments: ToolArguments {
                fields: vec![
                    ArtifactField {
                        key: "path".to_owned(),
                        value: "src/main.rs".to_owned(),
                    },
                    ArtifactField {
                        key: "old_string".to_owned(),
                        value: "fn main() {}\n".to_owned(),
                    },
                    ArtifactField {
                        key: "new_string".to_owned(),
                        value: "fn main() {\n    println!(\"hi\");\n}\n".to_owned(),
                    },
                ],
                redacted_fields: 0,
            },
            artifact: ArtifactModel::default(),
            server_side: false,
        };
        let display = ToolDisplay::from_part(&part, true);
        assert_eq!(display.panel_kind, ArtifactPanelKind::Diff);
        assert_eq!(display.language.as_deref(), Some("Rust"));
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.kind == ArtifactLineKind::Add));
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.kind == ArtifactLineKind::Remove));
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| !line.chunks.is_empty() && line.kind == ArtifactLineKind::Add));
    }

    #[test]
    fn read_tool_highlights_rust_body() {
        let part = ToolPart {
            id: PartId::from_semantic("tool:read-1"),
            tool_call_id: "read-1".to_owned(),
            name: "read".to_owned(),
            status: ToolStatus::Succeeded,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "path".to_owned(),
                    value: "src/lib.rs".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact: ArtifactModel {
                content: ArtifactContent::Text(BoundedText {
                    text: "pub fn ready() -> bool {\n    true\n}\n".to_owned(),
                    omitted_bytes: 0,
                }),
                warning: None,
                retention: RetentionLevel::Full,
                provenance: crate::tui_v2::model::artifact::ArtifactProvenance {
                    path: Some("src/lib.rs".to_owned()),
                    start_line: Some(10),
                    total_lines: Some(40),
                    lines_returned: Some(3),
                },
            },
            server_side: false,
        };
        let display = ToolDisplay::from_part(&part, true);
        assert_eq!(display.panel_kind, ArtifactPanelKind::Code);
        assert_eq!(display.language.as_deref(), Some("Rust"));
        let header = display
            .artifact_lines
            .iter()
            .find(|line| line.kind == ArtifactLineKind::Header)
            .expect("header");
        assert!(header.text.contains("L10"));
        assert!(header.text.contains("of 40"));
        let code_line = display
            .artifact_lines
            .iter()
            .find(|line| line.kind == ArtifactLineKind::Plain)
            .expect("code line");
        assert!(code_line.gutter.contains("10"));
        assert!(code_line
            .chunks
            .iter()
            .any(|chunk| chunk.role == SyntaxRole::Keyword));
    }

    #[test]
    fn edit_result_diff_text_becomes_diff_panel() {
        let part = ToolPart {
            id: PartId::from_semantic("tool:edit-diff"),
            tool_call_id: "edit-diff".to_owned(),
            name: "edit".to_owned(),
            status: ToolStatus::Succeeded,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "file_path".to_owned(),
                    value: "src/main.rs".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact: ArtifactModel {
                content: ArtifactContent::Text(BoundedText {
                    text: "--- a/src/main.rs\n+++ b/src/main.rs\n-old\n+new\n".to_owned(),
                    omitted_bytes: 0,
                }),
                warning: None,
                retention: RetentionLevel::Full,
                provenance: Default::default(),
            },
            server_side: false,
        };
        let display = ToolDisplay::from_part(&part, true);
        assert_eq!(display.panel_kind, ArtifactPanelKind::Diff);
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.kind == ArtifactLineKind::Header && line.text.contains('+')));
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.kind == ArtifactLineKind::Add && line.text.contains("new")));
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.kind == ArtifactLineKind::Remove && line.text.contains("old")));
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.kind == ArtifactLineKind::Add && line.gutter.contains('│')));
    }

    #[test]
    fn bash_text_artifact_stays_terminal_panel() {
        let part = ToolPart {
            id: PartId::from_semantic("tool:bash-done"),
            tool_call_id: "bash-done".to_owned(),
            name: "bash".to_owned(),
            status: ToolStatus::Succeeded,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "command".to_owned(),
                    value: "cargo test".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact: ArtifactModel {
                content: ArtifactContent::Text(BoundedText {
                    text: "running 3 tests\nok\n".to_owned(),
                    omitted_bytes: 0,
                }),
                warning: None,
                retention: RetentionLevel::Full,
                provenance: Default::default(),
            },
            server_side: false,
        };
        let display = ToolDisplay::from_part(&part, true);
        assert_eq!(display.panel_kind, ArtifactPanelKind::Terminal);
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.text.contains("running 3 tests")));
    }

    #[test]
    fn history_shaped_read_parse_feeds_code_panel() {
        use crate::tui_v2::projection::tool_output::parse_tool_output;
        use serde_json::json;

        let history = json!({
            "tool": "read",
            "retention": "retain_full",
            "summary": "read returned 2 lines",
            "is_error": false,
            "result": {
                "ok": true,
                "data": {
                    "content": "export function play() {\n  return true;\n}\n",
                    "total_lines": 3,
                    "lines_returned": 3,
                    "start_line": 1
                }
            }
        })
        .to_string();
        let artifact = parse_tool_output("read", &history, false);
        let part = ToolPart {
            id: PartId::from_semantic("tool:read-history"),
            tool_call_id: "read-history".to_owned(),
            name: "read".to_owned(),
            status: ToolStatus::Succeeded,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "file_path".to_owned(),
                    value: "tests/snake-tetris/game.js".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact,
            server_side: false,
        };
        let display = ToolDisplay::from_part(&part, true);
        assert_eq!(display.panel_kind, ArtifactPanelKind::Code);
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.text.contains("export function") || line.text.contains("play")));
        assert!(!display
            .artifact_lines
            .iter()
            .any(|line| line.text.contains("retention") || line.text.contains("is_error")));
    }

    #[test]
    fn history_shaped_write_parse_feeds_diff_panel() {
        use crate::tui_v2::projection::tool_output::parse_tool_output;
        use serde_json::json;

        let history = json!({
            "tool": "write",
            "retention": "summarize_after_turn",
            "summary": "Created new file",
            "is_error": false,
            "result": {
                "file_path": "README.md",
                "diff_preview": "--- README.md\n+++ README.md\n@@\n+# Hello\n"
            }
        })
        .to_string();
        let artifact = parse_tool_output("write", &history, false);
        let part = ToolPart {
            id: PartId::from_semantic("tool:write-history"),
            tool_call_id: "write-history".to_owned(),
            name: "write".to_owned(),
            status: ToolStatus::Succeeded,
            arguments: ToolArguments {
                fields: vec![ArtifactField {
                    key: "file_path".to_owned(),
                    value: "README.md".to_owned(),
                }],
                redacted_fields: 0,
            },
            artifact,
            server_side: false,
        };
        let display = ToolDisplay::from_part(&part, true);
        assert_eq!(display.panel_kind, ArtifactPanelKind::Diff);
        assert!(display
            .artifact_lines
            .iter()
            .any(|line| line.kind == ArtifactLineKind::Add));
    }
}
