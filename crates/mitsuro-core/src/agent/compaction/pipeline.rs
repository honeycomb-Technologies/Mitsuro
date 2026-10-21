//! Compaction pipeline orchestration.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use rusqlite::{params, Transaction, TransactionBehavior};
use uuid::Uuid;

use crate::agent::build_project_context;
use crate::agent::context_ledger::ContextLedger;
use crate::agent::summarizer::{generate_summary, generate_summary_observed, SummarizationResult};
use crate::agent::ProviderCallTraceContext;
use crate::ai::client::AiClient;
use crate::ai::types::{Content, ModelMessage, Role};
use crate::storage::{
    CanonicalMemoryInput, CompactionStore, Database, FileActivityTracker, MemoryAclScope,
    MemorySource, MemoryStore, MemoryType, MessageStore, RankedFile, StoredMessageRecord,
    COMPACTION_FLUSH_TITLE_PREFIX,
};

use super::apply::build_compacted_conversation;
use super::budget::{estimate_tokens, CompactionManager, CompactionRequestBudget};
use super::cut_point::{
    find_aggressive_cut_point, find_cut_point, find_last_compaction_index, IndexedMessage,
};
use super::microcompact::microcompact_messages;
use super::summarize::{
    bound_summarization_result, extract_file_operations, extract_previous_summary,
    merge_previous_summary, CompactionSummaryInput,
};
use super::DEFAULT_RESERVE_TOKENS;

const PINCH_RANKED_FILE_LIMIT: usize = 20;
const PINCH_SUMMARY_FILE_CONTENT_LIMIT: usize = 10;

#[derive(Debug, Clone)]
pub enum CompactionTrigger {
    Auto,
    Manual {
        preservation_hints: Option<String>,
        direction: Option<String>,
    },
    Reactive,
    Overflow,
}

impl CompactionTrigger {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Manual { .. } => "manual",
            Self::Reactive => "reactive",
            Self::Overflow => "overflow",
        }
    }
}

pub struct CompactionRequest<'a> {
    pub db_path: &'a Path,
    pub session_id: &'a str,
    pub conversation: &'a [ModelMessage],
    pub working_dir: &'a Path,
    pub ai_client: Option<&'a AiClient>,
    pub model: Option<&'a str>,
    pub trigger: CompactionTrigger,
    pub compaction_manager: CompactionManager,
    /// Optional rendered request pressure with explicit irreducible overhead.
    pub request_budget: Option<CompactionRequestBudget>,
    pub last_usage_prompt_tokens: Option<usize>,
    pub messages_after_usage: usize,
    /// When set, skips the summarization LLM call (manual `/pinch` after preview).
    pub summary_override: Option<SummarizationResult>,
    pub project_dir: Option<&'a str>,
    pub user_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq)]
struct PersistedMessageSnapshot {
    id: i64,
    role: String,
    content: serde_json::Value,
}

pub struct CompactionResult {
    pub compacted_conversation: Vec<ModelMessage>,
    pub estimated_tokens_before: usize,
    pub estimated_tokens_after: usize,
    pub replaced_messages: usize,
    pub checkpoint_id: String,
    pub compaction_count: u32,
    pub summary: SummarizationResult,
}

pub async fn run_compaction_pipeline(request: CompactionRequest<'_>) -> Result<CompactionResult> {
    run_compaction_pipeline_inner(request, None).await
}

pub(crate) async fn run_compaction_pipeline_observed(
    request: CompactionRequest<'_>,
    trace: &ProviderCallTraceContext,
) -> Result<CompactionResult> {
    run_compaction_pipeline_inner(request, Some(trace)).await
}

async fn run_compaction_pipeline_inner(
    request: CompactionRequest<'_>,
    trace: Option<&ProviderCallTraceContext>,
) -> Result<CompactionResult> {
    if request.conversation.is_empty() {
        bail!("Cannot compact session with no messages");
    }

    let micro = microcompact_messages(request.conversation);
    let working_conversation = if micro.changed {
        micro.messages
    } else {
        request.conversation.to_vec()
    };

    let raw_conversation_tokens = estimate_tokens(&working_conversation);
    let fallback_total = super::budget::estimate_with_usage(
        &working_conversation,
        request.last_usage_prompt_tokens,
        request.messages_after_usage,
    );
    let request_budget = request.request_budget.unwrap_or(CompactionRequestBudget {
        total_tokens: fallback_total,
        fixed_overhead_tokens: 0,
    });
    let fixed_request_overhead = request_budget.fixed_overhead_tokens;
    let estimated_tokens_before = request_budget
        .total_tokens
        .max(raw_conversation_tokens.saturating_add(fixed_request_overhead));

    if matches!(request.trigger, CompactionTrigger::Auto)
        && !request
            .compaction_manager
            .should_compact(estimated_tokens_before)
    {
        bail!("Compaction trigger threshold not reached");
    }

    let (indexed_messages, persisted_snapshot) = load_indexed_messages(
        request.db_path,
        request.session_id,
        request.conversation,
        &working_conversation,
    )?;
    if indexed_messages.is_empty() {
        bail!("Cannot compact session with no persisted messages");
    }

    let compaction_window_start = find_last_compaction_index(&working_conversation);
    let ranked_files = ranked_files_for_session(request.db_path, request.session_id);
    let file_contents = load_key_file_contents(
        request.working_dir,
        &ranked_files,
        PINCH_SUMMARY_FILE_CONTENT_LIMIT,
    );
    let project_context = load_project_context(request.working_dir);
    let latest_user_objective =
        ContextLedger::from_conversation(&working_conversation).latest_user_objective;
    let previous_summary = extract_previous_summary(&working_conversation);
    let prior_context = merge_previous_summary(previous_summary.as_deref());

    let (preservation_hints, direction) = match &request.trigger {
        CompactionTrigger::Manual {
            preservation_hints,
            direction,
        } => (preservation_hints.clone(), direction.clone()),
        _ => (None, None),
    };

    let compaction_count = {
        let db = Database::new(request.db_path)?;
        CompactionStore::new(&db)
            .count_checkpoints(request.session_id)?
            .saturating_add(1)
    };

    let requires_pressure_relief = !matches!(request.trigger, CompactionTrigger::Manual { .. })
        || request
            .compaction_manager
            .should_compact(estimated_tokens_before);
    if requires_pressure_relief
        && fixed_request_overhead >= request.compaction_manager.target_tokens()
    {
        bail!(
            "Compaction cannot reach target: irreducible fixed request overhead ({fixed_request_overhead} tokens) is at or above the target ({} tokens)",
            request.compaction_manager.target_tokens()
        );
    }

    let mut cut = None;
    let mut best_projected_tokens = usize::MAX;
    for attempt in 0..4 {
        let keep_recent_tokens = request.compaction_manager.keep_recent_tokens_for_attempt(
            estimated_tokens_before,
            raw_conversation_tokens,
            fixed_request_overhead,
            attempt,
        );
        if let Some(candidate) = find_cut_point(
            &indexed_messages,
            compaction_window_start,
            keep_recent_tokens,
        ) {
            let projected_tokens =
                projected_tokens_after_cut(fixed_request_overhead, &candidate.kept_messages);
            if projected_tokens < best_projected_tokens {
                best_projected_tokens = projected_tokens;
                cut = Some(candidate);
            }
            if projected_tokens <= request.compaction_manager.target_tokens() {
                break;
            }
        }
    }
    if cut.is_none()
        || (requires_pressure_relief
            && best_projected_tokens > request.compaction_manager.target_tokens())
    {
        if let Some(aggressive) =
            find_aggressive_cut_point(&indexed_messages, compaction_window_start)
        {
            let aggressive_projected =
                projected_tokens_after_cut(fixed_request_overhead, &aggressive.kept_messages);
            if aggressive_projected < best_projected_tokens {
                cut = Some(aggressive);
            }
        }
    }
    let cut = cut.ok_or_else(|| anyhow::anyhow!("No valid compaction cut point found"))?;

    let summarize_messages: Vec<ModelMessage> = cut
        .messages_to_summarize
        .iter()
        .map(|indexed| indexed.message.clone())
        .collect();

    if let Err(error) = flush_compaction_memory(&request, &summarize_messages, compaction_count) {
        tracing::warn!(%error, "Failed to flush durable notes before compaction summarization");
    }

    let summary = if let Some(summary) = request.summary_override.clone() {
        summary
    } else {
        summarize_for_compaction(
            request.ai_client,
            &summarize_messages,
            preservation_hints.as_deref(),
            &ranked_files,
            &file_contents,
            project_context.as_deref(),
            request.model,
            trace,
        )
        .await
    };
    let summary = bound_summarization_result(summary);

    let (mut read_files, mut modified_files) = extract_file_operations(&summarize_messages);
    if let Some(previous) = &previous_summary {
        for path in extract_paths_from_tag(previous, "read-files") {
            if !read_files.contains(&path) {
                read_files.push(path);
            }
        }
        for path in extract_paths_from_tag(previous, "modified-files") {
            if !modified_files.contains(&path) {
                modified_files.push(path);
            }
        }
    }

    let pre_compact_ids: Vec<i64> = cut
        .messages_to_summarize
        .iter()
        .map(|message| message.id)
        .collect();
    // Keep exact pre-compact transcript content only in compaction_segments.
    // Checkpoints retain metadata and message ids, not a second full-history copy.
    let compacted_history_json = "[]";
    let segment_markdown = build_segment_markdown(&cut.messages_to_summarize);
    let segment_token_estimate = estimate_tokens(&summarize_messages);
    let checkpoint_id = Uuid::new_v4().to_string();

    let summary_objective = latest_user_objective
        .as_ref()
        .filter(|objective| !kept_tail_contains_objective(&cut.kept_messages, objective))
        .cloned();
    let summary_input = CompactionSummaryInput {
        summary: summary.clone(),
        direction,
        preservation_hints,
        ranked_files: ranked_files.clone(),
        read_files,
        modified_files,
        checkpoint_id: checkpoint_id.clone(),
        compaction_count,
        latest_user_objective: summary_objective,
        prior_context,
    };

    let mut compacted_conversation = build_compacted_conversation(
        &summary_input,
        request.trigger.as_str(),
        estimated_tokens_before,
        0,
        cut.first_kept_message_id,
        &cut.kept_messages,
    );

    let estimated_tokens_after =
        estimate_tokens(&compacted_conversation).saturating_add(fixed_request_overhead);
    if requires_pressure_relief && estimated_tokens_after >= estimated_tokens_before {
        bail!(
            "Compaction made no token progress: request would remain at {estimated_tokens_after} tokens (before: {estimated_tokens_before})"
        );
    }
    if requires_pressure_relief
        && estimated_tokens_after > request.compaction_manager.target_tokens()
    {
        bail!(
            "Compaction could not reach the {} token target: projected request is {estimated_tokens_after} tokens, including {fixed_request_overhead} tokens of irreducible fixed request overhead",
            request.compaction_manager.target_tokens()
        );
    }
    if let Some(boundary) = compacted_conversation.first_mut() {
        if let Some(text) = boundary.content.first_mut().and_then(|content| {
            if let crate::ai::types::Content::Text { text, .. } = content {
                Some(text)
            } else {
                None
            }
        }) {
            if text.contains("\"tokens_after\":0") {
                *text = text.replace(
                    "\"tokens_after\":0",
                    &format!("\"tokens_after\":{estimated_tokens_after}"),
                );
            }
        }
    }

    persist_compaction_atomically(
        request.db_path,
        request.session_id,
        &checkpoint_id,
        cut.first_kept_index,
        &pre_compact_ids,
        compacted_history_json,
        latest_user_objective.as_deref(),
        &summary.important_files,
        pre_compact_ids.first().copied().unwrap_or(0),
        pre_compact_ids.last().copied().unwrap_or(0),
        &segment_markdown,
        segment_token_estimate,
        &compacted_conversation,
        &persisted_snapshot,
    )?;

    let replaced_messages = cut.messages_to_summarize.len();

    Ok(CompactionResult {
        compacted_conversation,
        estimated_tokens_before,
        estimated_tokens_after,
        replaced_messages,
        checkpoint_id,
        compaction_count,
        summary,
    })
}

#[allow(clippy::too_many_arguments)]
fn persist_compaction_atomically(
    db_path: &Path,
    session_id: &str,
    checkpoint_id: &str,
    prompt_index_at_compaction: usize,
    pre_compact_message_ids: &[i64],
    compacted_history_json: &str,
    original_user_info: Option<&str>,
    reread_file_paths: &[String],
    message_id_start: i64,
    message_id_end: i64,
    segment_markdown: &str,
    segment_token_estimate: usize,
    compacted_conversation: &[ModelMessage],
    expected_snapshot: &[PersistedMessageSnapshot],
) -> Result<()> {
    let db = Database::new(db_path)?;
    // IMMEDIATE prevents a writer from changing the transcript after the
    // optimistic snapshot check but before replacement.
    let tx = Transaction::new_unchecked(db.conn(), TransactionBehavior::Immediate)?;
    let current_snapshot = load_persisted_snapshot(&tx, session_id)?;
    if current_snapshot != expected_snapshot {
        bail!(
            "Session changed while compaction was running; refusing to replace a stale transcript snapshot"
        );
    }
    let now = Utc::now().to_rfc3339();
    let pre_compact_message_ids_json = serde_json::to_string(pre_compact_message_ids)?;
    let reread_file_paths_json = serde_json::to_string(reread_file_paths)?;

    tx.execute(
        "INSERT INTO compaction_checkpoints (
            id, session_id, prompt_index_at_compaction,
            pre_compact_message_ids_json, compacted_history_json,
            original_user_info, reread_file_paths_json,
            schema_version, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            checkpoint_id,
            session_id,
            prompt_index_at_compaction,
            pre_compact_message_ids_json,
            compacted_history_json,
            original_user_info,
            reread_file_paths_json,
            1_i32,
            now,
        ],
    )
    .context("save compaction checkpoint")?;

    let segment_id = Uuid::new_v4().to_string();
    tx.execute(
        "INSERT INTO compaction_segments (
            id, session_id, checkpoint_id,
            message_id_start, message_id_end,
            segment_markdown, token_estimate, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            segment_id,
            session_id,
            checkpoint_id,
            message_id_start,
            message_id_end,
            segment_markdown,
            segment_token_estimate,
            now,
        ],
    )
    .context("save compaction segment")?;

    tx.execute("DELETE FROM messages WHERE session_id = ?1", [session_id])
        .context("delete pre-compaction messages")?;

    for message in compacted_conversation {
        let role = match message.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            _ => continue,
        };
        let content_json = serde_json::to_string(&message.content)?;
        tx.execute(
            "INSERT INTO messages (session_id, role, content, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![session_id, role, content_json, now],
        )
        .context("insert compacted message")?;
    }

    tx.execute(
        "UPDATE sessions SET updated_at = ?1 WHERE id = ?2",
        params![now, session_id],
    )
    .context("touch compacted session")?;
    tx.commit().context("commit compaction transaction")?;

    Ok(())
}

async fn summarize_for_compaction(
    ai_client: Option<&AiClient>,
    conversation: &[ModelMessage],
    preservation_hints: Option<&str>,
    ranked_files: &[RankedFile],
    file_contents: &[(String, String)],
    project_context: Option<&str>,
    model: Option<&str>,
    trace: Option<&ProviderCallTraceContext>,
) -> SummarizationResult {
    if let Some(client) = ai_client {
        let generated = if let Some(trace) = trace {
            generate_summary_observed(
                client,
                conversation,
                preservation_hints,
                ranked_files,
                file_contents,
                project_context,
                model,
                (trace, "compaction_summary"),
            )
            .await
        } else {
            generate_summary(
                client,
                conversation,
                preservation_hints,
                ranked_files,
                file_contents,
                project_context,
                model,
            )
            .await
        };
        match generated {
            Ok(summary) if !summary.work_summary.trim().is_empty() => return summary,
            Ok(_) => tracing::warn!(
                "Compaction summarizer returned an empty summary; using deterministic fallback"
            ),
            Err(error) => {
                tracing::warn!(%error, "Compaction summarizer failed; using deterministic fallback")
            }
        }
    }

    deterministic_summary(conversation, preservation_hints, ranked_files)
}

fn deterministic_summary(
    conversation: &[ModelMessage],
    preservation_hints: Option<&str>,
    ranked_files: &[RankedFile],
) -> SummarizationResult {
    let mut excerpts = Vec::new();
    for message in conversation.iter().rev() {
        let role = match message.role {
            Role::User => "User",
            Role::Assistant => "Assistant",
            _ => continue,
        };
        let Some(text) = first_text(&message.content) else {
            continue;
        };
        let trimmed = text.trim();
        if trimmed.is_empty() {
            continue;
        }
        excerpts.push(format!("- {role}: {}", truncate_chars(trimmed, 700)));
        if excerpts.len() >= 8 {
            break;
        }
    }
    excerpts.reverse();

    let (read_files, modified_files) = extract_file_operations(conversation);
    let mut important_files: Vec<String> = ranked_files
        .iter()
        .map(|file| file.path.clone())
        .take(10)
        .collect();
    for path in read_files.iter().chain(modified_files.iter()) {
        if !important_files.contains(path) {
            important_files.push(path.clone());
        }
        if important_files.len() >= 10 {
            break;
        }
    }

    let mut work_summary = String::from(
        "A deterministic compaction summary was generated because AI summarization was unavailable or unusable. The archived checkpoint stores a canonical typed snapshot of the compacted transcript segment; use search_compaction_segments to recover prior details.\n\n",
    );
    if let Some(hints) = preservation_hints.filter(|value| !value.trim().is_empty()) {
        work_summary.push_str("Preservation hints supplied by the user:\n");
        work_summary.push_str(&truncate_chars(hints.trim(), 1_000));
        work_summary.push_str("\n\n");
    }
    if excerpts.is_empty() {
        work_summary.push_str("The compacted segment contained tool-only or non-text messages.");
    } else {
        work_summary.push_str("Recent compacted transcript excerpts:\n");
        work_summary.push_str(&excerpts.join("\n"));
    }

    let pending_tasks = latest_user_text(conversation).into_iter().collect();

    SummarizationResult {
        work_summary,
        key_decisions: Vec::new(),
        pending_tasks,
        important_files,
    }
}

fn flush_compaction_memory(
    request: &CompactionRequest<'_>,
    summarize_messages: &[ModelMessage],
    compaction_count: u32,
) -> Result<()> {
    let content = compaction_flush_content(summarize_messages);
    if content.trim().is_empty() {
        return Ok(());
    }
    let db = Database::new(request.db_path)?;
    let mut input = CanonicalMemoryInput::new(
        MemoryType::Project,
        format!("compaction-flush:{}:{compaction_count}", request.session_id),
        format!("{COMPACTION_FLUSH_TITLE_PREFIX}{compaction_count}"),
        content,
    );
    input.project_dir = request.project_dir.map(str::to_string);
    input.user_id = request.user_id.map(str::to_string);
    input.source = MemorySource::Compaction;
    input.source_session_id = Some(request.session_id.to_string());
    input.acl_scope = MemoryAclScope::Conversation;
    input.conversation_id = Some(request.session_id.to_string());
    MemoryStore::new(db).save_canonical(&input)?;
    Ok(())
}

fn compaction_flush_content(messages: &[ModelMessage]) -> String {
    let mut parts = Vec::new();
    for message in messages {
        if !matches!(message.role, Role::User | Role::Assistant) {
            continue;
        }
        let Some(text) = first_text(&message.content) else {
            continue;
        };
        let trimmed = text.trim();
        if trimmed.is_empty() {
            continue;
        }
        parts.push(trimmed.to_string());
    }
    truncate_chars(&parts.join("\n\n"), 4_000)
}

fn first_text(content: &[Content]) -> Option<&str> {
    content.iter().find_map(|content| match content {
        Content::Text { text, .. } => Some(text.as_str()),
        _ => None,
    })
}

fn latest_user_text(conversation: &[ModelMessage]) -> Option<String> {
    conversation.iter().rev().find_map(|message| {
        if message.role != Role::User {
            return None;
        }
        first_text(&message.content)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(|text| truncate_chars(text, 500))
    })
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated: String = text.chars().take(max_chars).collect();
    truncated.push('…');
    truncated
}

fn load_indexed_messages(
    db_path: &Path,
    session_id: &str,
    original_conversation: &[ModelMessage],
    working_conversation: &[ModelMessage],
) -> Result<(Vec<IndexedMessage>, Vec<PersistedMessageSnapshot>)> {
    let db = Database::new(db_path)?;
    let store = MessageStore::new(&db);
    let records = store.load_session_message_records(session_id)?;
    let snapshot = snapshot_from_records(&records)?;
    let original = persisted_roles_only(original_conversation)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let working = persisted_roles_only(working_conversation)
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let normalized_original = microcompact_messages(&original).messages;
    let persisted = snapshot
        .iter()
        .map(message_from_snapshot)
        .collect::<Result<Vec<_>>>()?;
    let normalized_persisted = microcompact_messages(&persisted).messages;

    if normalized_original.len() != working.len() || snapshot.len() != working.len() {
        bail!(
            "Session transcript changed before compaction started; persisted and in-memory message counts differ"
        );
    }
    if !same_messages(&normalized_original, &working)? {
        bail!("Compaction working transcript does not match its normalized in-memory source");
    }
    if !same_messages(&normalized_persisted, &working)? {
        bail!(
            "Session transcript changed before compaction started; refusing a stale in-memory snapshot"
        );
    }

    let mut indexed = Vec::with_capacity(snapshot.len());
    for (persisted, working_message) in snapshot.iter().zip(working) {
        indexed.push(IndexedMessage {
            id: persisted.id,
            // Use the microcompacted copy. Reloading the raw DB value here
            // would silently restore oversized thinking/tool-result tails.
            message: working_message.clone(),
        });
    }

    Ok((indexed, snapshot))
}

fn message_from_snapshot(snapshot: &PersistedMessageSnapshot) -> Result<ModelMessage> {
    let role = match snapshot.role.as_str() {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        _ => bail!("Unsupported persisted message role '{}'", snapshot.role),
    };
    let content = serde_json::from_value(snapshot.content.clone())?;
    Ok(ModelMessage { role, content })
}

fn same_messages(left: &[ModelMessage], right: &[ModelMessage]) -> Result<bool> {
    Ok(serde_json::to_value(left)? == serde_json::to_value(right)?)
}

fn snapshot_from_records(records: &[StoredMessageRecord]) -> Result<Vec<PersistedMessageSnapshot>> {
    records
        .iter()
        .map(|record| {
            if !matches!(record.role.as_str(), "user" | "assistant") {
                bail!(
                    "Unsupported persisted message role '{}' in compaction transcript",
                    record.role
                );
            }
            let content = serde_json::from_str(&record.content_json).with_context(|| {
                format!(
                    "failed to parse message {} while preparing compaction",
                    record.id
                )
            })?;
            Ok(PersistedMessageSnapshot {
                id: record.id,
                role: record.role.clone(),
                content,
            })
        })
        .collect()
}

fn load_persisted_snapshot(
    conn: &rusqlite::Connection,
    session_id: &str,
) -> Result<Vec<PersistedMessageSnapshot>> {
    let mut stmt =
        conn.prepare("SELECT id, role, content FROM messages WHERE session_id = ?1 ORDER BY id")?;
    let rows = stmt.query_map([session_id], |row| {
        Ok(StoredMessageRecord {
            id: row.get(0)?,
            role: row.get(1)?,
            content_json: row.get(2)?,
            created_at: String::new(),
        })
    })?;
    let records = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    snapshot_from_records(&records)
}

fn persisted_roles_only(messages: &[ModelMessage]) -> Vec<&ModelMessage> {
    messages
        .iter()
        .filter(|message| matches!(message.role, Role::User | Role::Assistant))
        .collect()
}

#[cfg(test)]
fn role_name(role: &Role) -> Option<&'static str> {
    match role {
        Role::User => Some("user"),
        Role::Assistant => Some("assistant"),
        _ => None,
    }
}

fn ranked_files_for_session(db_path: &Path, session_id: &str) -> Vec<RankedFile> {
    let Ok(db) = Database::new(db_path) else {
        return Vec::new();
    };
    let tracker = FileActivityTracker::new(&db, session_id.to_string());
    tracker
        .get_ranked_files(PINCH_RANKED_FILE_LIMIT)
        .unwrap_or_default()
}

fn load_key_file_contents(
    working_dir: &Path,
    ranked_files: &[RankedFile],
    limit: usize,
) -> Vec<(String, String)> {
    ranked_files
        .iter()
        .take(limit)
        .filter_map(|file| {
            let path = if Path::new(&file.path).is_absolute() {
                PathBuf::from(&file.path)
            } else {
                working_dir.join(&file.path)
            };
            std::fs::read_to_string(path)
                .ok()
                .map(|content| (file.path.clone(), content))
        })
        .collect()
}

fn load_project_context(working_dir: &Path) -> Option<String> {
    let context = build_project_context(working_dir);
    (!context.trim().is_empty()).then_some(context)
}

fn build_segment_markdown(messages: &[IndexedMessage]) -> String {
    let messages = messages
        .iter()
        .map(|indexed| {
            serde_json::json!({
                "id": indexed.id,
                "message": &indexed.message,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&serde_json::json!({
        "schema": "mitsuro.compaction_segment.v1",
        "messages": messages,
    }))
    .unwrap_or_else(|error| {
        serde_json::json!({
            "schema": "mitsuro.compaction_segment.v1",
            "serialization_error": error.to_string(),
        })
        .to_string()
    })
}

fn projected_tokens_after_cut(
    fixed_request_overhead: usize,
    kept_messages: &[IndexedMessage],
) -> usize {
    let kept = kept_messages
        .iter()
        .map(|message| message.message.clone())
        .collect::<Vec<_>>();
    fixed_request_overhead
        .saturating_add(estimate_tokens(&kept))
        .saturating_add(DEFAULT_RESERVE_TOKENS)
}

fn kept_tail_contains_objective(messages: &[IndexedMessage], objective: &str) -> bool {
    let normalized_objective = normalize_text(objective);
    messages.iter().any(|indexed| {
        indexed.message.role == Role::User
            && indexed.message.content.iter().any(|content| {
                matches!(content, Content::Text { text, .. } if normalize_text(text) == normalized_objective)
            })
    })
}

fn normalize_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_paths_from_tag(previous_summary: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let Some(start) = previous_summary.find(&open) else {
        return Vec::new();
    };
    let content_start = start + open.len();
    let Some(relative_end) = previous_summary[content_start..].find(&close) else {
        return Vec::new();
    };
    let end = content_start + relative_end;
    previous_summary[content_start..end]
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::storage::SessionManager;
    use tempfile::TempDir;

    fn save_conversation(db_path: &Path, session_id: &str, conversation: &[ModelMessage]) {
        let manager = SessionManager::new(Database::new(db_path).expect("db"));
        for message in conversation {
            let role = role_name(&message.role).expect("persisted role");
            manager
                .save_message(
                    session_id,
                    role,
                    &serde_json::to_string(&message.content).expect("content"),
                )
                .expect("save message");
        }
    }

    #[test]
    fn path_tag_extraction_pairs_close_after_open() {
        let summary = "</modified_files>\nquoted old content\n<modified_files>\nalpha.rs\nbeta.ts\n</modified_files>";

        assert_eq!(
            extract_paths_from_tag(summary, "modified_files"),
            vec!["alpha.rs", "beta.ts"]
        );
    }

    #[test]
    fn path_tag_extraction_ignores_unmatched_prior_close() {
        let summary = "</modified_files>\nquoted old content\n<modified_files>\nalpha.rs";

        assert!(extract_paths_from_tag(summary, "modified_files").is_empty());
    }

    #[test]
    fn indexed_kept_tail_uses_microcompacted_content() {
        let temp = TempDir::new().expect("temp");
        let db_path = temp.path().join("micro-tail.db");
        let manager = SessionManager::new(Database::new(&db_path).expect("db"));
        let session_id = manager
            .create_session("micro tail", None, None)
            .expect("session");
        drop(manager);

        let mut conversation = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::ToolResult {
                tool_use_id: "tool-1".to_string(),
                output: serde_json::json!({
                    "retention": "summarize_after_turn",
                    "summary": "large result",
                    "result": "x".repeat(5_000),
                }),
                is_error: None,
            }],
        }];
        for index in 0..7 {
            conversation.push(ModelMessage {
                role: Role::Assistant,
                content: vec![Content::text(format!("assistant {index}"))],
            });
        }
        save_conversation(&db_path, &session_id, &conversation);
        let micro = microcompact_messages(&conversation);
        assert!(micro.changed);

        let (indexed, _) =
            load_indexed_messages(&db_path, &session_id, &conversation, &micro.messages)
                .expect("indexed");
        let Content::ToolResult { output, .. } = &indexed[0].message.content[0] else {
            panic!("tool result");
        };
        assert!(output["result"]
            .as_str()
            .is_some_and(|result| result.contains("[microcompact truncated]")));

        // The orchestrator may have already normalized its in-memory copy
        // while SQLite intentionally retains the raw transcript.
        load_indexed_messages(&db_path, &session_id, &micro.messages, &micro.messages)
            .expect("pre-microcompacted orchestrator transcript must remain valid");
    }

    #[test]
    fn atomic_persistence_rejects_stale_snapshot() {
        let temp = TempDir::new().expect("temp");
        let db_path = temp.path().join("stale.db");
        let manager = SessionManager::new(Database::new(&db_path).expect("db"));
        let session_id = manager
            .create_session("stale", None, None)
            .expect("session");
        drop(manager);
        let conversation = vec![
            ModelMessage {
                role: Role::User,
                content: vec![Content::text("before".to_string())],
            },
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::text("reply".to_string())],
            },
        ];
        save_conversation(&db_path, &session_id, &conversation);
        let (_, snapshot) =
            load_indexed_messages(&db_path, &session_id, &conversation, &conversation)
                .expect("snapshot");

        let manager = SessionManager::new(Database::new(&db_path).expect("db"));
        manager
            .save_message(
                &session_id,
                "user",
                r#"[{"type":"text","text":"concurrent mutation"}]"#,
            )
            .expect("mutate");
        drop(manager);

        let result = persist_compaction_atomically(
            &db_path,
            &session_id,
            "checkpoint",
            1,
            &[snapshot[0].id],
            "[]",
            None,
            &[],
            snapshot[0].id,
            snapshot[0].id,
            "{}",
            1,
            &conversation,
            &snapshot,
        );
        assert!(result
            .expect_err("stale snapshot")
            .to_string()
            .contains("stale transcript snapshot"));

        let db = Database::new(&db_path).expect("db");
        assert_eq!(
            CompactionStore::new(&db)
                .count_checkpoints(&session_id)
                .expect("checkpoints"),
            0
        );
        assert_eq!(
            MessageStore::new(&db)
                .load_session_message_records(&session_id)
                .expect("messages")
                .len(),
            3
        );
    }
}
