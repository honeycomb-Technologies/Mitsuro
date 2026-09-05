//! Context ledger for deterministic continuation semantics.
//!
//! Tracks high-level context state transitions (canonical/summarized/dropped/pinned/replayed)
//! so compaction and resume decisions can be explicit and auditable.

use crate::ai::types::{Content, ModelMessage, Role};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonResumableReason {
    MissingUserObjective,
    EmptyConversation,
}

impl NonResumableReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::MissingUserObjective => "missing_user_objective",
            Self::EmptyConversation => "empty_conversation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContinuationDecision {
    Resumable { latest_user_objective: String },
    NonResumable { reason: NonResumableReason },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum ContinuationContractDecision {
    Resumable { latest_user_objective: String },
    NonResumable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContinuationContract {
    pub(crate) schema_version: u8,
    pub(crate) decision: ContinuationContractDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContextLedgerRecord {
    pub(crate) schema_version: u8,
    pub(crate) canonical_messages: usize,
    pub(crate) summarized_messages: usize,
    pub(crate) dropped_messages: usize,
    pub(crate) pinned_messages: usize,
    pub(crate) replayed_messages: usize,
    pub(crate) latest_user_objective: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ContextLedger {
    pub(crate) canonical_messages: usize,
    pub(crate) summarized_messages: usize,
    pub(crate) dropped_messages: usize,
    pub(crate) pinned_messages: usize,
    pub(crate) replayed_messages: usize,
    pub(crate) latest_user_objective: Option<String>,
}

impl ContextLedger {
    pub(crate) fn from_conversation(conversation: &[ModelMessage]) -> Self {
        let mut ledger = Self::default();
        ledger.recompute(conversation);
        ledger
    }

    pub(crate) fn update_from_conversation(&mut self, conversation: &[ModelMessage]) {
        self.recompute(conversation);
    }

    pub(crate) fn continuation_decision(&self) -> ContinuationDecision {
        if self.canonical_messages == 0 {
            return ContinuationDecision::NonResumable {
                reason: NonResumableReason::EmptyConversation,
            };
        }

        match self.latest_user_objective.as_ref().map(|s| s.trim()) {
            Some(text) if !text.is_empty() => ContinuationDecision::Resumable {
                latest_user_objective: text.to_string(),
            },
            _ => ContinuationDecision::NonResumable {
                reason: NonResumableReason::MissingUserObjective,
            },
        }
    }

    pub(crate) fn persistence_record(&self) -> ContextLedgerRecord {
        ContextLedgerRecord {
            schema_version: 1,
            canonical_messages: self.canonical_messages,
            summarized_messages: self.summarized_messages,
            dropped_messages: self.dropped_messages,
            pinned_messages: self.pinned_messages,
            replayed_messages: self.replayed_messages,
            latest_user_objective: self.latest_user_objective.clone(),
        }
    }

    pub(crate) fn continuation_contract(&self) -> ContinuationContract {
        let decision = match self.continuation_decision() {
            ContinuationDecision::Resumable {
                latest_user_objective,
            } => ContinuationContractDecision::Resumable {
                latest_user_objective,
            },
            ContinuationDecision::NonResumable { reason } => {
                ContinuationContractDecision::NonResumable {
                    reason: reason.as_str().to_string(),
                }
            }
        };

        ContinuationContract {
            schema_version: 1,
            decision,
        }
    }

    fn recompute(&mut self, conversation: &[ModelMessage]) {
        self.canonical_messages = conversation.len();
        self.summarized_messages = 0;
        self.dropped_messages = 0;
        self.pinned_messages = 0;
        self.latest_user_objective = None;

        for message in conversation {
            if message.role == Role::System
                && first_text(&message.content)
                    .is_some_and(|text| text.starts_with("[PROJECT INSTRUCTIONS"))
            {
                self.pinned_messages += 1;
            }

            if message.role == Role::User {
                for content in &message.content {
                    if let Content::ToolResult { output, .. } = content {
                        if output
                            .get("retention")
                            .and_then(|value| value.as_str())
                            .is_some_and(|value| value == "summarize_after_turn")
                        {
                            self.summarized_messages += 1;
                        }
                        if output
                            .get("retention")
                            .and_then(|value| value.as_str())
                            .is_some_and(|value| value == "drop_after_compaction")
                        {
                            self.dropped_messages += 1;
                        }
                    }
                }
            }
        }

        self.latest_user_objective = extract_latest_user_objective(conversation);
    }
}

fn first_text(content: &[Content]) -> Option<&str> {
    content.iter().find_map(|c| {
        if let Content::Text { text, .. } = c {
            Some(text.as_str())
        } else {
            None
        }
    })
}

fn extract_latest_user_objective(conversation: &[ModelMessage]) -> Option<String> {
    conversation.iter().rev().find_map(|message| {
        if message.role != Role::User {
            return None;
        }

        message.content.iter().find_map(|content| {
            if let Content::Text { text, .. } = content {
                let trimmed = text.trim();
                if trimmed.is_empty()
                    || trimmed.starts_with(super::compaction::COMPACTION_BOUNDARY_PREFIX)
                {
                    None
                } else if trimmed.starts_with(super::compaction::COMPACTION_SUMMARY_PREFIX.trim()) {
                    extract_objective_from_compaction_summary(trimmed)
                        .and_then(|objective| normalize_objective(&objective))
                } else {
                    normalize_objective(trimmed)
                }
            } else {
                None
            }
        })
    })
}

fn normalize_objective(objective: &str) -> Option<String> {
    const MAX_OBJECTIVE_CHARS: usize = 2_000;

    let normalized = objective.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return None;
    }
    if normalized.chars().count() <= MAX_OBJECTIVE_CHARS {
        return Some(normalized);
    }

    let mut bounded = normalized
        .chars()
        .take(MAX_OBJECTIVE_CHARS.saturating_sub(1))
        .collect::<String>();
    bounded.push('…');
    Some(bounded)
}

fn extract_objective_from_compaction_summary(summary: &str) -> Option<String> {
    const HEADING: &str = "## Latest User Objective\n\n";
    let objective = summary.split_once(HEADING)?.1;
    let objective = objective
        .split_once("\n\n## ")
        .map_or(objective, |(value, _)| value)
        .trim();
    (!objective.is_empty()).then(|| objective.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn text_message(role: Role, text: &str) -> ModelMessage {
        ModelMessage {
            role,
            content: vec![Content::text(text.to_string())],
        }
    }

    #[test]
    fn ledger_extracts_latest_user_objective() {
        let conversation = vec![
            text_message(Role::User, "first request"),
            text_message(Role::Assistant, "thinking"),
            text_message(Role::User, "final request"),
        ];

        let ledger = ContextLedger::from_conversation(&conversation);
        assert_eq!(
            ledger.continuation_decision(),
            ContinuationDecision::Resumable {
                latest_user_objective: "final request".to_string()
            }
        );
    }

    #[test]
    fn ledger_uses_objective_embedded_in_compaction_summary() {
        let conversation = vec![
            text_message(
                Role::User,
                "[COMPACTION_BOUNDARY]\n{\"type\":\"compact_boundary\"}",
            ),
            text_message(
                Role::User,
                "# Conversation Compacted\n\n## Latest User Objective\n\nFinish the cache benchmark\n\n## Work Summary\n\nPrior work.",
            ),
            text_message(Role::Assistant, "continuing"),
        ];

        let ledger = ContextLedger::from_conversation(&conversation);

        assert_eq!(
            ledger.latest_user_objective.as_deref(),
            Some("Finish the cache benchmark")
        );
    }

    #[test]
    fn ledger_normalizes_and_bounds_latest_objective() {
        let objective = format!("  finish\n\t the   task {}", "x".repeat(2_500));
        let ledger = ContextLedger::from_conversation(&[text_message(Role::User, &objective)]);
        let objective = ledger.latest_user_objective.expect("objective");

        assert!(objective.starts_with("finish the task "));
        assert!(!objective.contains('\n'));
        assert_eq!(objective.chars().count(), 2_000);
        assert!(objective.ends_with('…'));
    }

    #[test]
    fn ledger_never_uses_compaction_boundary_as_objective() {
        let conversation = vec![text_message(
            Role::User,
            "[COMPACTION_BOUNDARY]\n{\"type\":\"compact_boundary\"}",
        )];

        let ledger = ContextLedger::from_conversation(&conversation);

        assert!(ledger.latest_user_objective.is_none());
    }

    #[test]
    fn ledger_detects_missing_objective() {
        let conversation = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::ToolResult {
                tool_use_id: "call_1".to_string(),
                output: json!({"retention":"summarize_after_turn"}),
                is_error: None,
            }],
        }];

        let ledger = ContextLedger::from_conversation(&conversation);
        assert_eq!(
            ledger.continuation_decision(),
            ContinuationDecision::NonResumable {
                reason: NonResumableReason::MissingUserObjective
            }
        );
    }

    #[test]
    fn ledger_tracks_retention_and_pinned_counts() {
        let conversation = vec![
            text_message(
                Role::System,
                "[PROJECT INSTRUCTIONS - /repo/AGENTS.md]\nUse Rust",
            ),
            ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "call_1".to_string(),
                    output: json!({"retention":"drop_after_compaction"}),
                    is_error: None,
                }],
            },
        ];

        let ledger = ContextLedger::from_conversation(&conversation);
        assert_eq!(ledger.pinned_messages, 1);
        assert_eq!(ledger.dropped_messages, 1);
    }

    #[test]
    fn ledger_serializes_persistence_contract() {
        let conversation = vec![text_message(Role::User, "stabilize streaming resumes")];
        let ledger = ContextLedger::from_conversation(&conversation);

        let record = ledger.persistence_record();
        assert_eq!(record.schema_version, 1);
        assert_eq!(
            record.latest_user_objective.as_deref(),
            Some("stabilize streaming resumes")
        );
        let contract = ledger.continuation_contract();
        assert_eq!(contract.schema_version, 1);
        match contract.decision {
            ContinuationContractDecision::Resumable {
                latest_user_objective,
            } => assert_eq!(latest_user_objective, "stabilize streaming resumes"),
            ContinuationContractDecision::NonResumable { reason } => {
                panic!("unexpected non-resumable decision: {reason}")
            }
        }
    }
}
