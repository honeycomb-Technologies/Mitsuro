//! Session recovery state persisted separately from canonical conversation history.
//!
//! This captures interrupted in-flight work without mutating the durable thread.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::agent::loop_events::LoopStopReason;
use crate::tools::registry::PermissionMode;

pub const REDACTED_ARGUMENT_VALUE: &str = "[REDACTED]";
const MAX_ARGUMENT_STRING_CHARS: usize = 2_048;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStatus {
    Streaming,
    ToolExecuting,
    AwaitingInput,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryNonResumableReason {
    MissingUserObjective,
    EmptyConversation,
    PendingToolCall,
    ToolExecutionInProgress,
    AwaitingHumanInput,
}

impl RecoveryNonResumableReason {
    pub fn message(&self) -> &'static str {
        match self {
            Self::MissingUserObjective => {
                "Mitsuro did not auto-resume because no stable user objective was preserved."
            }
            Self::EmptyConversation => {
                "Mitsuro did not auto-resume because the conversation has no recoverable context."
            }
            Self::PendingToolCall => {
                "Mitsuro did not auto-resume because a tool call was only partially received."
            }
            Self::ToolExecutionInProgress => {
                "Mitsuro did not auto-resume because tools may already have run."
            }
            Self::AwaitingHumanInput => {
                "Mitsuro did not auto-resume because it is waiting for human input."
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum RecoveryDecision {
    Resumable { latest_user_objective: String },
    NonResumable { reason: RecoveryNonResumableReason },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryToolArguments {
    #[serde(default)]
    pub value: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redacted_paths: Vec<String>,
}

impl Default for RecoveryToolArguments {
    fn default() -> Self {
        Self {
            value: Value::Null,
            redacted_paths: Vec::new(),
        }
    }
}

impl RecoveryToolArguments {
    pub fn redacted(arguments: &Value) -> Self {
        let mut redacted_paths = Vec::new();
        let value = redact_argument_value(arguments, "$", None, &mut redacted_paths);
        Self {
            value,
            redacted_paths,
        }
    }

    pub fn was_redacted(&self) -> bool {
        !self.redacted_paths.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryToolCall {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub arguments: RecoveryToolArguments,
}

impl RecoveryToolCall {
    pub fn summary(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            arguments: RecoveryToolArguments::default(),
        }
    }

    pub fn from_call_parts(id: &str, name: &str, arguments: &Value) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            arguments: RecoveryToolArguments::redacted(arguments),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingQuestionOptionSnapshot {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingQuestionSnapshot {
    pub header: String,
    pub question: String,
    #[serde(default)]
    pub options: Vec<PendingQuestionOptionSnapshot>,
    #[serde(default)]
    pub multi_select: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingPlanTaskSnapshot {
    pub description: String,
    pub completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PendingInteractionSnapshot {
    ToolApproval {
        tool_call: RecoveryToolCall,
    },
    AskUserQuestion {
        tool_call_id: String,
        questions: Vec<PendingQuestionSnapshot>,
    },
    PlanConfirm {
        tool_call_id: String,
        title: String,
        task_count: usize,
        #[serde(default)]
        tasks: Vec<PendingPlanTaskSnapshot>,
    },
}

impl PendingInteractionSnapshot {
    pub fn tool_approval_from_call(id: &str, name: &str, arguments: &Value) -> Self {
        Self::ToolApproval {
            tool_call: RecoveryToolCall::from_call_parts(id, name, arguments),
        }
    }

    pub fn ask_user_from_call(tool_call_id: &str, arguments: &Value) -> Self {
        let questions = arguments
            .get("questions")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(parse_pending_question_snapshot)
                    .collect::<Vec<_>>()
            })
            .filter(|questions| !questions.is_empty())
            .unwrap_or_else(|| {
                vec![PendingQuestionSnapshot {
                    header: "Question".to_string(),
                    question: "Mitsuro is awaiting user input, but the question payload could not be reconstructed safely.".to_string(),
                    options: Vec::new(),
                    multi_select: false,
                }]
            });

        Self::AskUserQuestion {
            tool_call_id: tool_call_id.to_string(),
            questions,
        }
    }

    pub fn plan_confirm(
        tool_call_id: impl Into<String>,
        title: impl Into<String>,
        task_count: usize,
        tasks: Vec<PendingPlanTaskSnapshot>,
    ) -> Self {
        let tasks = tasks
            .into_iter()
            .map(|task| PendingPlanTaskSnapshot {
                description: safe_prompt_string(task.description),
                completed: task.completed,
            })
            .collect();

        Self::PlanConfirm {
            tool_call_id: tool_call_id.into(),
            title: safe_prompt_string(title.into()),
            task_count,
            tasks,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::ToolApproval { .. } => "tool approval",
            Self::AskUserQuestion { .. } => "user input",
            Self::PlanConfirm { .. } => "plan confirmation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PartialAssistantState {
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub citations: Vec<crate::ai::types::Citation>,
    #[serde(default)]
    pub thinking: String,
    #[serde(default)]
    pub tool_calls: Vec<RecoveryToolCall>,
}

/// Durable acceptance record for a human-input continuation.
///
/// This remains attached to the awaiting-input recovery snapshot until the
/// resumed orchestrator starts and supersedes that snapshot. If the server
/// exits in between, startup resets only transient agent state and the same
/// accepted response can be reclaimed without reopening the prompt to a
/// different answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationClaimSnapshot {
    pub interaction_id: String,
    pub accepted_response: String,
    pub claimed_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecoveryState {
    pub schema_version: u8,
    pub status: RecoveryStatus,
    pub stop_reason: Option<LoopStopReason>,
    pub last_error: Option<String>,
    pub partial_assistant: PartialAssistantState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_interactions: Vec<PendingInteractionSnapshot>,
    pub decision: RecoveryDecision,
    /// Effective permission mode for the interrupted turn, used to resume
    /// interactive continuations without weakening tool approval policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<PermissionMode>,
    /// Exact tool capability for the interrupted turn. `None` is legacy or
    /// unrestricted; `Some([])` is an explicitly empty capability and must
    /// remain distinguishable across an interactive continuation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_tool_allowlist: Option<Vec<String>>,
    /// Accepted response retained across the narrow claim-to-run-start crash
    /// window. This is not canonical conversation history; the endpoint still
    /// persists the provider-facing ToolResult/user message before running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_claim: Option<ContinuationClaimSnapshot>,
}

impl SessionRecoveryState {
    pub fn new(
        status: RecoveryStatus,
        stop_reason: Option<LoopStopReason>,
        last_error: Option<String>,
        partial_assistant: PartialAssistantState,
        decision: RecoveryDecision,
    ) -> Self {
        Self::new_with_pending_interactions(
            status,
            stop_reason,
            last_error,
            partial_assistant,
            Vec::new(),
            decision,
        )
    }

    pub fn new_with_pending_interactions(
        status: RecoveryStatus,
        stop_reason: Option<LoopStopReason>,
        last_error: Option<String>,
        partial_assistant: PartialAssistantState,
        pending_interactions: Vec<PendingInteractionSnapshot>,
        decision: RecoveryDecision,
    ) -> Self {
        Self {
            schema_version: 1,
            status,
            stop_reason,
            last_error,
            partial_assistant,
            pending_interactions,
            decision,
            permission_mode: None,
            execution_tool_allowlist: None,
            continuation_claim: None,
        }
    }

    pub fn with_pending_interactions(
        mut self,
        pending_interactions: Vec<PendingInteractionSnapshot>,
    ) -> Self {
        self.pending_interactions = pending_interactions;
        self
    }

    pub fn with_permission_mode(mut self, permission_mode: PermissionMode) -> Self {
        self.permission_mode = Some(permission_mode);
        self
    }

    pub fn with_execution_tool_allowlist(
        mut self,
        execution_tool_allowlist: Option<&std::collections::HashSet<String>>,
    ) -> Self {
        self.execution_tool_allowlist = execution_tool_allowlist.map(|allowlist| {
            let mut names = allowlist.iter().cloned().collect::<Vec<_>>();
            names.sort_unstable();
            names
        });
        self
    }

    pub fn has_pending_interactions(&self) -> bool {
        !self.pending_interactions.is_empty()
    }

    pub fn is_resumable(&self) -> bool {
        matches!(self.decision, RecoveryDecision::Resumable { .. })
    }

    pub fn latest_user_objective(&self) -> Option<&str> {
        match &self.decision {
            RecoveryDecision::Resumable {
                latest_user_objective,
            } => Some(latest_user_objective.as_str()),
            RecoveryDecision::NonResumable { .. } => None,
        }
    }

    pub fn pending_tool_calls(&self) -> &[RecoveryToolCall] {
        &self.partial_assistant.tool_calls
    }

    pub fn notice(&self) -> String {
        let headline = match (&self.status, &self.stop_reason) {
            (RecoveryStatus::Streaming, _) => {
                "Previous turn ended while the assistant was still streaming."
            }
            (RecoveryStatus::ToolExecuting, _) => {
                "Previous turn ended while tool execution was in progress."
            }
            (RecoveryStatus::AwaitingInput, _) => "Previous turn is waiting for human input.",
            (_, Some(LoopStopReason::StreamIdleTimeout)) => {
                "Previous turn stopped after the provider stream went idle."
            }
            (_, Some(LoopStopReason::ProviderError)) => {
                "Previous turn stopped after a provider error."
            }
            (_, Some(LoopStopReason::UserAbort)) => {
                "Previous turn was interrupted by user cancellation."
            }
            (_, _) => "Previous turn ended before Mitsuro could safely finalize it.",
        };

        let continuation = match &self.decision {
            RecoveryDecision::Resumable {
                latest_user_objective,
            } => format!("Safe manual resume target: {}.", latest_user_objective),
            RecoveryDecision::NonResumable { reason } => reason.message().to_string(),
        };

        let tool_detail = if self.partial_assistant.tool_calls.is_empty() {
            String::new()
        } else {
            let tools = self
                .partial_assistant
                .tool_calls
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!(" Pending tool calls: {}.", tools)
        };

        let pending_detail = if self.pending_interactions.is_empty() {
            String::new()
        } else {
            let interactions = self
                .pending_interactions
                .iter()
                .map(PendingInteractionSnapshot::label)
                .collect::<Vec<_>>()
                .join(", ");
            format!(" Pending interactions: {}.", interactions)
        };

        format!("{headline} {continuation}{tool_detail}{pending_detail}")
    }
}

fn parse_pending_question_snapshot(value: &Value) -> Option<PendingQuestionSnapshot> {
    let header = value
        .get("header")
        .and_then(Value::as_str)
        .map(|text| safe_prompt_string(text.to_string()))?;
    let question = value
        .get("question")
        .and_then(Value::as_str)
        .map(|text| safe_prompt_string(text.to_string()))?;

    let options = value
        .get("options")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let label = item
                        .get("label")
                        .and_then(Value::as_str)
                        .map(|text| safe_prompt_string(text.to_string()))?;
                    let description = item
                        .get("description")
                        .and_then(Value::as_str)
                        .map(|text| safe_prompt_string(text.to_string()));
                    Some(PendingQuestionOptionSnapshot { label, description })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let multi_select = value
        .get("multiSelect")
        .or_else(|| value.get("multi_select"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    Some(PendingQuestionSnapshot {
        header,
        question,
        options,
        multi_select,
    })
}

fn redact_argument_value(
    value: &Value,
    path: &str,
    key: Option<&str>,
    redacted_paths: &mut Vec<String>,
) -> Value {
    if key.is_some_and(|key| is_sensitive_argument_key(key) || is_raw_content_argument_key(key)) {
        redacted_paths.push(path.to_string());
        return Value::String(REDACTED_ARGUMENT_VALUE.to_string());
    }

    match value {
        Value::Object(map) => {
            let mut redacted = Map::new();
            for (child_key, child_value) in map {
                let child_path = format!("{path}.{}", child_key.replace('.', "_"));
                redacted.insert(
                    child_key.clone(),
                    redact_argument_value(
                        child_value,
                        &child_path,
                        Some(child_key),
                        redacted_paths,
                    ),
                );
            }
            Value::Object(redacted)
        }
        Value::Array(items) => Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    redact_argument_value(item, &format!("{path}[{index}]"), None, redacted_paths)
                })
                .collect(),
        ),
        Value::String(text) if contains_sensitive_string_marker(text) => {
            redacted_paths.push(path.to_string());
            Value::String(REDACTED_ARGUMENT_VALUE.to_string())
        }
        Value::String(text) => Value::String(truncate_snapshot_string(text)),
        other => other.clone(),
    }
}

fn safe_prompt_string(text: String) -> String {
    if contains_sensitive_string_marker(&text) {
        REDACTED_ARGUMENT_VALUE.to_string()
    } else {
        truncate_snapshot_string(&text)
    }
}

fn truncate_snapshot_string(text: &str) -> String {
    if text.len() <= MAX_ARGUMENT_STRING_CHARS {
        return text.to_string();
    }

    let boundary = floor_char_boundary(text, MAX_ARGUMENT_STRING_CHARS);
    format!("{}…[truncated]", &text[..boundary])
}

fn floor_char_boundary(text: &str, index: usize) -> usize {
    let mut boundary = index.min(text.len());
    while boundary > 0 && !text.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

fn is_sensitive_argument_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', ' '], "_");
    normalized.contains("password")
        || normalized.contains("passwd")
        || normalized.contains("secret")
        || normalized.contains("api_key")
        || normalized.contains("apikey")
        || normalized.contains("api_token")
        || normalized == "token"
        || normalized.ends_with("_token")
        || normalized.contains("access_token")
        || normalized.contains("refresh_token")
        || normalized.contains("auth_token")
        || normalized.contains("authorization")
        || normalized.contains("credential")
        || normalized.contains("private_key")
        || normalized.contains("client_secret")
        || normalized.contains("cookie")
}

fn is_raw_content_argument_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "content" | "old_string" | "new_string" | "replacement" | "insert" | "patch" | "diff"
    )
}

fn contains_sensitive_string_marker(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("sk-")
        || lower.contains("ghp_")
        || lower.contains("bearer ")
        || lower.contains("api_key=")
        || lower.contains("apikey=")
        || lower.contains("token=")
        || lower.contains("password=")
        || lower.contains("secret=")
        || lower.contains("authorization:")
        || lower.contains("begin private key")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recovery_state_permission_mode_is_optional_for_legacy_snapshots() {
        let state: SessionRecoveryState = serde_json::from_value(json!({
            "schema_version": 1,
            "status": "awaiting_input",
            "stop_reason": "awaiting_input",
            "last_error": null,
            "partial_assistant": {"text": "", "thinking": "", "tool_calls": []},
            "pending_interactions": [],
            "decision": {"kind": "non_resumable", "reason": "awaiting_human_input"}
        }))
        .expect("legacy recovery state should deserialize");

        assert_eq!(state.permission_mode, None);
        assert_eq!(state.execution_tool_allowlist, None);
    }

    #[test]
    fn recovery_state_serializes_permission_mode_when_present() {
        let state = SessionRecoveryState::new(
            RecoveryStatus::AwaitingInput,
            Some(LoopStopReason::AwaitingInput),
            None,
            PartialAssistantState::default(),
            RecoveryDecision::NonResumable {
                reason: RecoveryNonResumableReason::AwaitingHumanInput,
            },
        )
        .with_permission_mode(PermissionMode::Supervised);

        let serialized = serde_json::to_value(&state).expect("state should serialize");
        assert_eq!(serialized["permission_mode"], "supervised");
    }

    #[test]
    fn recovery_state_preserves_exact_empty_and_nonempty_tool_scopes() {
        let base = || {
            SessionRecoveryState::new(
                RecoveryStatus::AwaitingInput,
                Some(LoopStopReason::AwaitingInput),
                None,
                PartialAssistantState::default(),
                RecoveryDecision::NonResumable {
                    reason: RecoveryNonResumableReason::AwaitingHumanInput,
                },
            )
        };
        let empty = std::collections::HashSet::new();
        let scoped =
            std::collections::HashSet::from(["tool_search".to_string(), "read".to_string()]);

        let empty_round_trip: SessionRecoveryState = serde_json::from_value(
            serde_json::to_value(base().with_execution_tool_allowlist(Some(&empty))).unwrap(),
        )
        .unwrap();
        assert_eq!(empty_round_trip.execution_tool_allowlist, Some(Vec::new()));

        let scoped_round_trip: SessionRecoveryState = serde_json::from_value(
            serde_json::to_value(base().with_execution_tool_allowlist(Some(&scoped))).unwrap(),
        )
        .unwrap();
        assert_eq!(
            scoped_round_trip.execution_tool_allowlist,
            Some(vec!["read".to_string(), "tool_search".to_string()])
        );
    }

    #[test]
    fn resumable_notice_mentions_objective() {
        let state = SessionRecoveryState::new(
            RecoveryStatus::Interrupted,
            Some(LoopStopReason::StreamIdleTimeout),
            None,
            PartialAssistantState {
                citations: Vec::new(),
                text: "partial answer".to_string(),
                thinking: "reasoning".to_string(),
                tool_calls: Vec::new(),
            },
            RecoveryDecision::Resumable {
                latest_user_objective: "finish the parser fix".to_string(),
            },
        );

        let notice = state.notice();
        assert!(notice.contains("provider stream went idle"));
        assert!(notice.contains("finish the parser fix"));
    }

    #[test]
    fn non_resumable_notice_mentions_pending_tools() {
        let state = SessionRecoveryState::new(
            RecoveryStatus::Interrupted,
            Some(LoopStopReason::ProviderError),
            Some("provider failed".to_string()),
            PartialAssistantState {
                citations: Vec::new(),
                text: String::new(),
                thinking: String::new(),
                tool_calls: vec![RecoveryToolCall::summary("tool-1", "bash")],
            },
            RecoveryDecision::NonResumable {
                reason: RecoveryNonResumableReason::PendingToolCall,
            },
        );

        let notice = state.notice();
        assert!(notice.contains("provider error"));
        assert!(notice.contains("Pending tool calls: bash."));
    }

    #[test]
    fn trace_payload_for_tool_approval_is_redacted_but_reconstructable_enough_for_ui() {
        let snapshot = PendingInteractionSnapshot::tool_approval_from_call(
            "call-edit",
            "edit",
            &json!({
                "file_path": "src/lib.rs",
                "old_string": "OPENAI_API_KEY=sk-live-secret",
                "new_string": "OPENAI_API_KEY=sk-live-secret-2",
                "metadata": {
                    "api_token": "secret-token-value"
                },
                "dry_run": false
            }),
        );

        let PendingInteractionSnapshot::ToolApproval { tool_call } = snapshot else {
            panic!("expected tool approval snapshot");
        };

        assert_eq!(tool_call.id, "call-edit");
        assert_eq!(tool_call.name, "edit");
        assert_eq!(tool_call.arguments.value["file_path"], "src/lib.rs");
        assert_eq!(tool_call.arguments.value["dry_run"], false);
        assert_eq!(
            tool_call.arguments.value["old_string"],
            REDACTED_ARGUMENT_VALUE
        );
        assert_eq!(
            tool_call.arguments.value["new_string"],
            REDACTED_ARGUMENT_VALUE
        );
        assert!(tool_call
            .arguments
            .redacted_paths
            .contains(&"$.metadata.api_token".to_string()));

        let serialized = serde_json::to_string(&tool_call).expect("tool call serializes");
        assert!(!serialized.contains("sk-live-secret"));
        assert!(!serialized.contains("secret-token-value"));
    }

    #[test]
    fn plan_confirm_snapshot_redacts_sensitive_title_and_task_descriptions() {
        let snapshot = PendingInteractionSnapshot::plan_confirm(
            "plan-1",
            "Fix auth bearer secret",
            2,
            vec![
                PendingPlanTaskSnapshot {
                    description: "Rotate token=secret-token-value".to_string(),
                    completed: false,
                },
                PendingPlanTaskSnapshot {
                    description: "Update docs".to_string(),
                    completed: true,
                },
            ],
        );

        let PendingInteractionSnapshot::PlanConfirm { title, tasks, .. } = snapshot else {
            panic!("expected plan confirmation snapshot");
        };

        assert_eq!(title, REDACTED_ARGUMENT_VALUE);
        assert_eq!(tasks[0].description, REDACTED_ARGUMENT_VALUE);
        assert_eq!(tasks[1].description, "Update docs");
        assert!(tasks[1].completed);

        let serialized = serde_json::to_string(&tasks).expect("tasks serialize");
        assert!(!serialized.contains("secret-token-value"));
    }
}
