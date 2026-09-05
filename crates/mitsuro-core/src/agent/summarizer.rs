//! AI-powered conversation summarization for in-place compaction.
//!
//! Uses the user's current model to produce a structured summary
//! for the continued session. Summarization intentionally uses its own short,
//! stable system prefix: the parent request has layered project/tool
//! instructions that are not reproduced here, so claiming parent-prefix cache
//! reuse would be incorrect.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::ai::client::{AiClient, RemoteAttemptPolicy};
use crate::ai::types::{Content, ModelMessage, Role};
use crate::storage::RankedFile;

/// Truncate a string to at most `max_bytes` without splitting a UTF-8 char.
fn truncate_str(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Max tokens for summarization calls
///
/// 4000 tokens is sufficient for summary output. The output is structured JSON,
/// so it doesn't need to be verbose.
const SUMMARIZATION_MAX_TOKENS: usize = 4000;

/// Summarization instructions live in the final user message so the dedicated
/// system prefix stays small and reusable across compaction requests.
const SUMMARIZATION_INSTRUCTIONS: &str = r#"Analyze the conversation above and produce a structured summary for continuing this work after in-place compaction.

You MUST respond with a valid JSON object (no markdown code blocks, no extra text):
{
  "work_summary": "2-3 paragraph summary of what was accomplished, focusing on the WHY and WHAT",
  "key_decisions": ["Important architectural or design decisions made"],
  "pending_tasks": ["Incomplete work or clearly identified next steps"],
  "important_files": ["Top 10 most relevant file paths for continuing work"]
}

Guidelines:
1. Work Summary: Focus on accomplishments, not mechanics. What was built? What problems were solved? What's the current state?
2. Key Decisions: Capture architectural choices, patterns adopted, trade-offs made.
3. Pending Tasks: Identify explicitly mentioned TODOs, incomplete work, or logical next steps. Be specific.
4. Important Files: List files most critical for continuing the work. Prioritize files that were modified or are central to the work."#;

/// Dedicated stable prefix shared by both summary paths. Detailed instructions
/// live in the final user message so the static prefix stays small and cacheable.
const SUMMARIZATION_SYSTEM_PROMPT: &str =
    "Summarize coding-agent transcripts for in-place context continuation. Return exactly one valid JSON object and no prose.";

/// Result from the summarization AI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummarizationResult {
    pub work_summary: String,
    pub key_decisions: Vec<String>,
    pub pending_tasks: Vec<String>,
    pub important_files: Vec<String>,
}

impl Default for SummarizationResult {
    fn default() -> Self {
        Self {
            work_summary: "No summary available.".to_string(),
            key_decisions: Vec::new(),
            pending_tasks: Vec::new(),
            important_files: Vec::new(),
        }
    }
}

/// Build the user message for summarization
///
/// Includes:
/// - Full conversation history (condensed)
/// - Key file contents
/// - Bounded project instructions
/// - User's preservation hints
pub fn build_summarization_prompt(
    conversation: &[ModelMessage],
    preservation_hints: Option<&str>,
    ranked_files: &[RankedFile],
    file_contents: &[(String, String)], // (path, content)
    project_context: Option<&str>,      // Project instruction bundle
) -> String {
    let mut prompt = String::new();

    // User's preservation hints (highest priority)
    if let Some(hints) = preservation_hints {
        prompt.push_str("## USER'S PRESERVATION PRIORITIES (IMPORTANT)\n\n");
        prompt.push_str(hints);
        prompt.push_str("\n\nWeight these areas heavily in your summary.\n\n");
    }

    // Project context
    if let Some(ctx) = project_context {
        prompt.push_str("## PROJECT INSTRUCTIONS\n\n");
        // Truncate if too long
        if ctx.len() > 5000 {
            prompt.push_str(truncate_str(ctx, 5000));
            prompt.push_str("\n...[truncated]\n");
        } else {
            prompt.push_str(ctx);
        }
        prompt.push_str("\n\n");
    }

    // Ranked files for reference
    if !ranked_files.is_empty() {
        prompt.push_str("## FILES BY ACTIVITY\n\n");
        for (i, file) in ranked_files.iter().take(20).enumerate() {
            let reasons = if file.reasons.is_empty() {
                String::new()
            } else {
                format!(" ({})", file.reasons.join(", "))
            };
            prompt.push_str(&format!("{}. {}{}\n", i + 1, file.path, reasons));
        }
        prompt.push('\n');
    }

    // Key file contents
    if !file_contents.is_empty() {
        prompt.push_str("## KEY FILE CONTENTS\n\n");
        for (path, content) in file_contents.iter().take(10) {
            prompt.push_str(&format!("### {}\n```\n", path));
            // Truncate long files
            if content.len() > 3000 {
                prompt.push_str(truncate_str(content, 3000));
                prompt.push_str("\n...[truncated]\n");
            } else {
                prompt.push_str(content);
            }
            prompt.push_str("\n```\n\n");
        }
    }

    // Conversation history
    prompt.push_str("## CONVERSATION HISTORY\n\n");
    for msg in conversation {
        let role_str = match msg.role {
            Role::User => "USER",
            Role::Assistant => "ASSISTANT",
            Role::System => continue, // Skip system messages
            Role::Tool => continue,   // Skip tool role messages
        };

        for content in &msg.content {
            match content {
                Content::Text { text, .. } => {
                    // Truncate very long messages
                    let preview = if text.len() > 2000 {
                        format!(
                            "{}...[truncated, {} chars total]",
                            truncate_str(text, 2000),
                            text.len()
                        )
                    } else {
                        text.clone()
                    };
                    prompt.push_str(&format!("{}: {}\n\n", role_str, preview));
                }
                Content::ToolUse { name, input, .. } => {
                    let input_preview = summarize_tool_input(name, input);
                    prompt.push_str(&format!(
                        "{}: [Tool: {} - {}]\n\n",
                        role_str, name, input_preview
                    ));
                }
                Content::ToolResult {
                    output, is_error, ..
                } => {
                    let status = if is_error.unwrap_or(false) {
                        "ERROR"
                    } else {
                        "OK"
                    };
                    // Brief preview of result from the JSON output
                    let preview = output
                        .as_str()
                        .map(|s| {
                            if s.len() > 200 {
                                format!("{}...", truncate_str(s, 200))
                            } else {
                                s.to_string()
                            }
                        })
                        .unwrap_or_else(|| "[output]".to_string());
                    prompt.push_str(&format!(
                        "{}: [Result: {} - {}]\n\n",
                        role_str, status, preview
                    ));
                }
                Content::Thinking { thinking, .. } if thinking.len() > 500 => {
                    // Brief thinking preview
                    prompt.push_str(&format!(
                        "{}: [Thinking: {}...]\n\n",
                        role_str,
                        truncate_str(thinking, 500)
                    ));
                }
                _ => {}
            }
        }
    }

    prompt
}

/// Summarize tool input for the prompt
fn summarize_tool_input(tool_name: &str, input: &serde_json::Value) -> String {
    match tool_name.to_lowercase().as_str() {
        "read" => input
            .get("file_path")
            .and_then(|v| v.as_str())
            .map(|p| format!("reading {}", p))
            .unwrap_or_else(|| "reading file".to_string()),
        "write" => input
            .get("file_path")
            .and_then(|v| v.as_str())
            .map(|p| format!("writing {}", p))
            .unwrap_or_else(|| "writing file".to_string()),
        "edit" => input
            .get("file_path")
            .and_then(|v| v.as_str())
            .map(|p| format!("editing {}", p))
            .unwrap_or_else(|| "editing file".to_string()),
        "bash" => input
            .get("command")
            .and_then(|v| v.as_str())
            .map(|c| {
                if c.len() > 50 {
                    format!("{}...", truncate_str(c, 50))
                } else {
                    c.to_string()
                }
            })
            .unwrap_or_else(|| "running command".to_string()),
        "glob" => input
            .get("pattern")
            .and_then(|v| v.as_str())
            .map(|p| format!("searching for {}", p))
            .unwrap_or_else(|| "glob search".to_string()),
        "grep" => input
            .get("pattern")
            .and_then(|v| v.as_str())
            .map(|p| format!("searching for '{}'", p))
            .unwrap_or_else(|| "grep search".to_string()),
        _ => format!("{} operation", tool_name),
    }
}

/// Generate a summary using the user's current model.
///
/// When conversation messages are available they remain structured rather
/// than being flattened into a second copy. This reduces the summary request,
/// but it does not claim cache identity with the parent's differently layered
/// system prompt.
///
/// Falls back to the legacy `call_simple` path when no conversation context
/// is available (e.g., server-side API with no message history).
pub async fn generate_summary(
    client: &AiClient,
    conversation: &[ModelMessage],
    preservation_hints: Option<&str>,
    ranked_files: &[RankedFile],
    file_contents: &[(String, String)],
    project_context: Option<&str>,
    current_model: Option<&str>, // Deprecated: model comes from client.config()
) -> Result<SummarizationResult> {
    generate_summary_inner(
        client,
        conversation,
        preservation_hints,
        ranked_files,
        file_contents,
        project_context,
        current_model,
        None,
    )
    .await
}

pub(crate) async fn generate_summary_observed(
    client: &AiClient,
    conversation: &[ModelMessage],
    preservation_hints: Option<&str>,
    ranked_files: &[RankedFile],
    file_contents: &[(String, String)],
    project_context: Option<&str>,
    current_model: Option<&str>,
    trace: (&super::ProviderCallTraceContext, &'static str),
) -> Result<SummarizationResult> {
    generate_summary_inner(
        client,
        conversation,
        preservation_hints,
        ranked_files,
        file_contents,
        project_context,
        current_model,
        Some(trace),
    )
    .await
}

async fn generate_summary_inner(
    client: &AiClient,
    conversation: &[ModelMessage],
    preservation_hints: Option<&str>,
    ranked_files: &[RankedFile],
    file_contents: &[(String, String)],
    project_context: Option<&str>,
    _current_model: Option<&str>,
    trace: Option<(&super::ProviderCallTraceContext, &'static str)>,
) -> Result<SummarizationResult> {
    tracing::info!(
        "Starting summarization with {} conversation messages, {} file contents",
        conversation.len(),
        file_contents.len()
    );

    let model = client.config().model.as_str();
    let provider = client.provider_id();

    // Check if we have actual conversation messages to reuse as prefix
    let has_conversation = conversation
        .iter()
        .any(|m| m.role == Role::User || m.role == Role::Assistant);

    let governed_permit = if let Some((trace, operation)) = trace {
        if let Some(governor) = trace.provider_governor() {
            let conversation_bytes = serde_json::to_vec(conversation)
                .map(|value| value.len())
                .unwrap_or_default();
            let auxiliary_bytes = preservation_hints
                .map(str::len)
                .unwrap_or_default()
                .saturating_add(project_context.map(str::len).unwrap_or_default())
                .saturating_add(file_contents.iter().fold(0usize, |total, (path, content)| {
                    total
                        .saturating_add(path.len())
                        .saturating_add(content.len())
                }))
                .saturating_add(ranked_files.len().saturating_mul(256));
            let reservation = super::bounded_reservation(
                conversation_bytes
                    .saturating_add(SUMMARIZATION_SYSTEM_PROMPT.len())
                    .saturating_add(auxiliary_bytes)
                    .saturating_add(2)
                    .saturating_div(3)
                    .saturating_add(128),
                SUMMARIZATION_MAX_TOKENS,
            );
            let slot = super::WorkerProviderCallSlot::child(
                super::WorkerProviderCallKind::CompactionSummary,
                u32::try_from(trace.turn()).unwrap_or(u32::MAX),
                0,
                operation,
            )?;
            match governor.admit(slot, reservation)? {
                super::WorkerProviderAdmission::Allowed(permit) => Some(permit),
                super::WorkerProviderAdmission::Gated(decision) => {
                    anyhow::bail!(
                        "Hive Worker compaction provider call gated: {}",
                        serde_json::to_string(&decision)?
                    )
                }
                super::WorkerProviderAdmission::AlreadyStarted(call) => anyhow::bail!(
                    "Hive Worker compaction provider call {} was already Started and was not replayed",
                    call.provider_call_id
                ),
            }
        } else {
            None
        }
    } else {
        None
    };
    let provider_call_id = governed_permit
        .as_ref()
        .map(|permit| permit.provider_call_id().to_string());
    let attempt_policy = if governed_permit.is_some() {
        RemoteAttemptPolicy::GovernedSingleAttempt
    } else {
        RemoteAttemptPolicy::ConfiguredRetries
    };
    let started_at = std::time::Instant::now();
    let response = if has_conversation {
        let appended_message = build_summarization_user_message(
            preservation_hints,
            ranked_files,
            file_contents,
            project_context,
        );

        tracing::info!(
            "Structured summarization: sending {} conversation messages with dedicated prefix for {:?}",
            conversation.len(),
            provider,
        );

        client
            .call_with_conversation_with_usage_and_attempt_policy(
                model,
                SUMMARIZATION_SYSTEM_PROMPT,
                conversation,
                &appended_message,
                SUMMARIZATION_MAX_TOKENS,
                attempt_policy,
            )
            .await
    } else {
        let prompt = build_summarization_prompt(
            conversation,
            preservation_hints,
            ranked_files,
            file_contents,
            project_context,
        );

        tracing::info!(
            "Flattened summarization (no conversation messages) with model {} for {:?}",
            model,
            provider,
        );

        client
            .call_simple_with_usage_and_attempt_policy(
                model,
                SUMMARIZATION_SYSTEM_PROMPT,
                &prompt,
                SUMMARIZATION_MAX_TOKENS,
                attempt_policy,
            )
            .await
    };

    if let Some((trace, operation)) = trace {
        if let Some(provider_call_id) = provider_call_id.clone() {
            trace
                .record_simple_call_with_id(
                    provider_call_id,
                    operation,
                    client.provider_id(),
                    model,
                    started_at,
                    &response,
                )
                .await;
        } else {
            trace
                .record_simple_call(
                    operation,
                    client.provider_id(),
                    model,
                    started_at,
                    &response,
                )
                .await;
        }
    }
    if let (Some(permit), Ok(result)) = (governed_permit.as_ref(), response.as_ref()) {
        permit.complete(super::WorkerProviderCompletion::acknowledged(
            super::WorkerProviderTerminalOutcome::Completed,
            result.usage.clone(),
        ))?;
    }

    parse_summary_response(&response?.text)
}

/// Build the user message for the cache-safe summarization path.
///
/// Contains the summarization instructions plus any extra context
/// (preservation hints, file rankings, file contents). The conversation
/// history is already in the API messages, so we don't flatten it here.
fn build_summarization_user_message(
    preservation_hints: Option<&str>,
    ranked_files: &[RankedFile],
    file_contents: &[(String, String)],
    project_context: Option<&str>,
) -> String {
    let mut message = String::new();

    message.push_str(SUMMARIZATION_INSTRUCTIONS);

    if let Some(context) = project_context.filter(|value| !value.trim().is_empty()) {
        message.push_str("\n\n## PROJECT CONTEXT\n\n");
        message.push_str(truncate_str(context, 5_000));
        if context.len() > 5_000 {
            message.push_str("\n...[truncated]");
        }
    }

    if let Some(hints) = preservation_hints {
        message.push_str("\n\n## USER'S PRESERVATION PRIORITIES (IMPORTANT)\n\n");
        message.push_str(hints);
        message.push_str("\n\nWeight these areas heavily in your summary.");
    }

    if !ranked_files.is_empty() {
        message.push_str("\n\n## FILES BY ACTIVITY\n\n");
        for (i, file) in ranked_files.iter().take(20).enumerate() {
            let reasons = if file.reasons.is_empty() {
                String::new()
            } else {
                format!(" ({})", file.reasons.join(", "))
            };
            message.push_str(&format!("{}. {}{}\n", i + 1, file.path, reasons));
        }
    }

    if !file_contents.is_empty() {
        message.push_str("\n\n## KEY FILE CONTENTS\n\n");
        for (path, content) in file_contents.iter().take(10) {
            message.push_str(&format!("### {}\n```\n", path));
            if content.len() > 3000 {
                message.push_str(truncate_str(content, 3000));
                message.push_str("\n...[truncated]\n");
            } else {
                message.push_str(content);
            }
            message.push_str("\n```\n\n");
        }
    }

    message
}

/// Parse the JSON response from the summarization AI
fn parse_summary_response(response: &str) -> Result<SummarizationResult> {
    // Try to extract JSON from the response
    let json_str = extract_json(response);

    serde_json::from_str(&json_str).map_err(|e| {
        tracing::warn!(
            "Failed to parse summary JSON: {}. Response: {}",
            e,
            response
        );
        anyhow::anyhow!("Failed to parse summarization response: {}", e)
    })
}

/// Extract JSON from response, handling potential markdown wrapping
fn extract_json(response: &str) -> String {
    let trimmed = response.trim();

    // If it starts with {, assume it's raw JSON
    if trimmed.starts_with('{') {
        return trimmed.to_string();
    }

    // Try to extract from markdown code block
    if let Some(start) = trimmed.find("```json") {
        let after_marker = &trimmed[start + 7..];
        if let Some(end) = after_marker.find("```") {
            return after_marker[..end].trim().to_string();
        }
    }

    // Try plain code block
    if let Some(start) = trimmed.find("```") {
        let after_marker = &trimmed[start + 3..];
        let content_start = after_marker.find('\n').map(|i| i + 1).unwrap_or(0);
        let content = &after_marker[content_start..];
        if let Some(end) = content.find("```") {
            return content[..end].trim().to_string();
        }
    }

    // Last resort: find first { and last }
    if let (Some(start), Some(end)) = (trimmed.find('{'), trimmed.rfind('}')) {
        if end > start {
            return trimmed[start..=end].to_string();
        }
    }

    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_raw() {
        let input = r#"{"work_summary": "test", "key_decisions": [], "pending_tasks": [], "important_files": []}"#;
        let result = extract_json(input);
        assert!(result.starts_with('{'));
        assert!(result.ends_with('}'));
    }

    #[test]
    fn test_extract_json_markdown() {
        let input = r#"Here's the summary:

```json
{"work_summary": "test", "key_decisions": [], "pending_tasks": [], "important_files": []}
```

Done!"#;
        let result = extract_json(input);
        assert!(result.starts_with('{'));
    }

    #[test]
    fn test_parse_summary() {
        let input = r#"{"work_summary": "Built a feature", "key_decisions": ["Used Rust"], "pending_tasks": ["Add tests"], "important_files": ["src/main.rs"]}"#;
        let result = parse_summary_response(input).unwrap();
        assert_eq!(result.work_summary, "Built a feature");
        assert_eq!(result.key_decisions, vec!["Used Rust"]);
    }
}
