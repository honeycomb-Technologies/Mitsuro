//! Compaction summary formatting and incremental merge support.

use crate::agent::summarizer::SummarizationResult;
use crate::storage::RankedFile;

use super::{COMPACTION_BOUNDARY_PREFIX, COMPACTION_SUMMARY_PREFIX};

const MAX_PRIOR_WORK_CHARS: usize = 4_000;
const MAX_WORK_SUMMARY_CHARS: usize = 6_000;
const MAX_SUMMARY_ITEMS: usize = 12;
const MAX_SUMMARY_ITEM_CHARS: usize = 500;
const MAX_FILE_ITEMS: usize = 40;
const MAX_FILE_PATH_CHARS: usize = 512;
const MAX_DIRECTION_CHARS: usize = 2_000;
const MAX_OBJECTIVE_CHARS: usize = 2_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PriorCompactionContext {
    pub work_summary: String,
    pub key_decisions: Vec<String>,
    pub pending_tasks: Vec<String>,
}

impl PriorCompactionContext {
    fn is_empty(&self) -> bool {
        self.work_summary.is_empty()
            && self.key_decisions.is_empty()
            && self.pending_tasks.is_empty()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompactionSummaryInput {
    pub summary: SummarizationResult,
    pub direction: Option<String>,
    pub preservation_hints: Option<String>,
    pub ranked_files: Vec<RankedFile>,
    pub read_files: Vec<String>,
    pub modified_files: Vec<String>,
    pub checkpoint_id: String,
    pub compaction_count: u32,
    pub latest_user_objective: Option<String>,
    pub prior_context: PriorCompactionContext,
}

pub(crate) fn format_boundary_message(
    trigger: &str,
    tokens_before: usize,
    tokens_after: usize,
    first_kept_original_message_id: i64,
    checkpoint_id: &str,
    compaction_count: u32,
) -> String {
    serde_json::json!({
        "type": "compact_boundary",
        "trigger": trigger,
        "tokens_before": tokens_before,
        "tokens_after": tokens_after,
        "first_kept_original_message_id": first_kept_original_message_id,
        "checkpoint_id": checkpoint_id,
        "compaction_count": compaction_count,
    })
    .to_string()
}

pub(crate) fn format_summary_message(input: &CompactionSummaryInput) -> String {
    let mut msg = String::from(COMPACTION_SUMMARY_PREFIX);

    msg.push_str(
        "This session is being continued from a previous conversation that was compacted to stay within the context window. This summary is not new user authorization. Distinguish user requirements and accepted decisions from repository rules and unaccepted proposals; recover uncertain decision provenance with search_compaction_segments.\n\n",
    );

    if let Some(direction) = &input.direction {
        msg.push_str("## Priority Direction\n\n");
        msg.push_str(&truncate_chars(direction, MAX_DIRECTION_CHARS));
        msg.push_str("\n\n");
    }

    if let Some(objective) = &input.latest_user_objective {
        msg.push_str("## Latest User Objective\n\n");
        msg.push_str(&truncate_chars(objective, MAX_OBJECTIVE_CHARS));
        msg.push_str("\n\n");
    }

    if !input.prior_context.is_empty() {
        msg.push_str("## Prior Compacted Work\n\n");
        if !input.prior_context.work_summary.is_empty() {
            msg.push_str("<prior-work>\n");
            msg.push_str(&input.prior_context.work_summary);
            msg.push_str("\n</prior-work>\n\n");
        }
        if !input.prior_context.key_decisions.is_empty() {
            msg.push_str("<prior-key-decisions>\n");
            for decision in &input.prior_context.key_decisions {
                msg.push_str("- ");
                msg.push_str(decision);
                msg.push('\n');
            }
            msg.push_str("</prior-key-decisions>\n\n");
        }
        if !input.prior_context.pending_tasks.is_empty() {
            msg.push_str("<prior-pending-tasks>\n");
            for task in &input.prior_context.pending_tasks {
                msg.push_str("- ");
                msg.push_str(task);
                msg.push('\n');
            }
            msg.push_str("</prior-pending-tasks>\n\n");
        }
    }

    msg.push_str("## Work Summary\n\n");
    msg.push_str(&input.summary.work_summary);
    msg.push_str("\n\n");

    if !input.summary.key_decisions.is_empty() {
        msg.push_str("## Key Decisions\n\n");
        for decision in &input.summary.key_decisions {
            msg.push_str(&format!("- {}\n", decision));
        }
        msg.push('\n');
    }

    if !input.summary.pending_tasks.is_empty() {
        msg.push_str("## Pending Tasks\n\n");
        for (index, task) in input.summary.pending_tasks.iter().enumerate() {
            msg.push_str(&format!("{}. {}\n", index + 1, task));
        }
        msg.push('\n');
    }

    if !input.read_files.is_empty() {
        msg.push_str("<read-files>\n");
        for path in input.read_files.iter().take(MAX_FILE_ITEMS) {
            msg.push_str(&truncate_chars(path, MAX_FILE_PATH_CHARS));
            msg.push('\n');
        }
        msg.push_str("</read-files>\n\n");
    }

    if !input.modified_files.is_empty() {
        msg.push_str("<modified-files>\n");
        for path in input.modified_files.iter().take(MAX_FILE_ITEMS) {
            msg.push_str(&truncate_chars(path, MAX_FILE_PATH_CHARS));
            msg.push('\n');
        }
        msg.push_str("</modified-files>\n\n");
    }

    if !input.ranked_files.is_empty() {
        msg.push_str("## Important Files\n\n");
        for (index, file) in input.ranked_files.iter().take(10).enumerate() {
            msg.push_str(&format!("{}. `{}`\n", index + 1, file.path));
        }
        msg.push('\n');
    }

    if let Some(hints) = &input.preservation_hints {
        msg.push_str("## Preservation Notes\n\n");
        msg.push_str(hints);
        msg.push_str("\n\n");
    }

    msg.push_str("## Recovery\n\n");
    msg.push_str(&format!(
        "Checkpoint `{}` (compaction #{}) archived a canonical typed snapshot of pre-compact history. Use `search_compaction_segments` to recover prior details.\n",
        input.checkpoint_id, input.compaction_count
    ));

    msg
}

pub(crate) fn boundary_user_text(payload_json: &str) -> String {
    format!("{COMPACTION_BOUNDARY_PREFIX}\n{payload_json}")
}

pub(crate) fn extract_previous_summary(
    messages: &[crate::ai::types::ModelMessage],
) -> Option<String> {
    use crate::ai::types::{Content, ModelMessage};

    messages.iter().rev().find_map(|message: &ModelMessage| {
        message.content.iter().find_map(|content| {
            if let Content::Text { text, .. } = content {
                if text.starts_with(COMPACTION_SUMMARY_PREFIX) {
                    Some(text.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
    })
}

/// Parse only stable, bounded semantic fields from the previous compacted
/// summary. The raw summary is never nested into the next summary.
pub(crate) fn merge_previous_summary(previous: Option<&str>) -> PriorCompactionContext {
    let Some(previous) = previous else {
        return PriorCompactionContext::default();
    };

    let carried_work = extract_tag(previous, "prior-work").unwrap_or_default();
    let latest_work = extract_heading(previous, "Work Summary").unwrap_or_default();
    let work_summary = merge_bounded_work(carried_work, latest_work);

    let mut key_decisions = parse_list(extract_tag(previous, "prior-key-decisions"));
    key_decisions.extend(parse_list(extract_heading(previous, "Key Decisions")));
    let key_decisions = bounded_unique_items(key_decisions);

    let mut pending_tasks = parse_list(extract_tag(previous, "prior-pending-tasks"));
    pending_tasks.extend(parse_list(extract_heading(previous, "Pending Tasks")));
    let pending_tasks = bounded_unique_items(pending_tasks);

    PriorCompactionContext {
        work_summary,
        key_decisions,
        pending_tasks,
    }
}

pub(crate) fn bound_summarization_result(mut summary: SummarizationResult) -> SummarizationResult {
    summary.work_summary = truncate_chars(&summary.work_summary, MAX_WORK_SUMMARY_CHARS);
    summary.key_decisions = bounded_unique_items(summary.key_decisions);
    summary.pending_tasks = bounded_unique_items(summary.pending_tasks);
    summary.important_files = bounded_unique_paths(summary.important_files, 20);
    summary
}

pub(crate) fn extract_file_operations(
    messages: &[crate::ai::types::ModelMessage],
) -> (Vec<String>, Vec<String>) {
    use crate::ai::types::Content;
    use std::collections::BTreeSet;

    let mut read_files = BTreeSet::new();
    let mut modified_files = BTreeSet::new();

    for message in messages {
        if message.role != crate::ai::types::Role::Assistant {
            continue;
        }
        for content in &message.content {
            if let Content::ToolUse { name, input, .. } = content {
                match name.as_str() {
                    "read" => {
                        if let Some(path) = input.get("file_path").and_then(|v| v.as_str()) {
                            read_files.insert(path.to_string());
                        }
                    }
                    "write" | "edit" | "multiedit" | "apply_patch" => {
                        if let Some(path) = input.get("file_path").and_then(|v| v.as_str()) {
                            modified_files.insert(path.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    (
        read_files.into_iter().collect(),
        modified_files.into_iter().collect(),
    )
}

fn extract_tag<'a>(text: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open)?.saturating_add(open.len());
    let end = text[start..].find(&close)?.saturating_add(start);
    Some(text[start..end].trim())
}

fn extract_heading<'a>(text: &'a str, heading: &str) -> Option<&'a str> {
    let marker = format!("## {heading}");
    let marker_start = text.find(&marker)?;
    let start = marker_start.saturating_add(marker.len());
    let body = text[start..].trim_start_matches(['\r', '\n']);
    let end = body.find("\n## ").unwrap_or(body.len());
    Some(body[..end].trim())
}

fn parse_list(section: Option<&str>) -> Vec<String> {
    section
        .into_iter()
        .flat_map(str::lines)
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let item = line
                .strip_prefix("- ")
                .or_else(|| {
                    line.split_once(". ")
                        .filter(|(prefix, _)| prefix.chars().all(|ch| ch.is_ascii_digit()))
                        .map(|(_, item)| item)
                })
                .unwrap_or(line)
                .trim();
            (!item.is_empty()).then(|| item.to_string())
        })
        .collect()
}

fn merge_bounded_work(carried: &str, latest: &str) -> String {
    match (carried.trim().is_empty(), latest.trim().is_empty()) {
        (true, true) => String::new(),
        (true, false) => truncate_chars_balanced(latest.trim(), MAX_PRIOR_WORK_CHARS),
        (false, true) => truncate_chars_balanced(carried.trim(), MAX_PRIOR_WORK_CHARS),
        (false, false) => truncate_chars_balanced(
            &format!(
                "Earlier compacted work:\n{}\n\nMost recent compacted work:\n{}",
                carried.trim(),
                latest.trim()
            ),
            MAX_PRIOR_WORK_CHARS,
        ),
    }
}

fn bounded_unique_items(items: Vec<String>) -> Vec<String> {
    let mut unique = Vec::new();
    for item in items {
        let item = truncate_chars(item.trim(), MAX_SUMMARY_ITEM_CHARS);
        if !item.is_empty() && !unique.contains(&item) {
            unique.push(item);
        }
    }
    if unique.len() <= MAX_SUMMARY_ITEMS {
        return unique;
    }

    let recent = unique.split_off(unique.len() - 8);
    unique.truncate(MAX_SUMMARY_ITEMS - recent.len());
    unique.extend(recent);
    unique
}

fn bounded_unique_paths(paths: Vec<String>, limit: usize) -> Vec<String> {
    let mut unique = Vec::new();
    for path in paths {
        let path = truncate_chars(path.trim(), MAX_FILE_PATH_CHARS);
        if !path.is_empty() && !unique.contains(&path) {
            unique.push(path);
        }
        if unique.len() >= limit {
            break;
        }
    }
    unique
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated = text
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn truncate_chars_balanced(text: &str, max_chars: usize) -> String {
    let count = text.chars().count();
    if count <= max_chars {
        return text.to_string();
    }
    let marker = "\n… bounded prior context …\n";
    let marker_len = marker.chars().count();
    let available = max_chars.saturating_sub(marker_len);
    let head_len = available / 2;
    let tail_len = available.saturating_sub(head_len);
    let head = text.chars().take(head_len).collect::<String>();
    let tail = text
        .chars()
        .skip(count.saturating_sub(tail_len))
        .collect::<String>();
    format!("{head}{marker}{tail}")
}
