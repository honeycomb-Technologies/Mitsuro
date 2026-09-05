use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::agent::subagent::AgentIdentity;
use crate::agent::subagent::AgentMailbox;
use crate::agent::ProviderCallTraceContext;
use crate::ai::providers::ReasoningEffort;
use crate::process::ProcessRegistry;
use crate::tools::registry::DelegationPolicy;
use sha2::{Digest, Sha256};

/// One display-safe tool call attached to a child Agent's assistant turn.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentConversationToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

/// A semantic child-conversation update. These events intentionally mirror
/// normal chat boundaries without exposing hidden reasoning or provider wire
/// events. Tool results are bounded before emission by the delegated runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentConversationEvent {
    AssistantTurn {
        message_id: String,
        turn: usize,
        content: String,
        tool_calls: Vec<AgentConversationToolCall>,
    },
    ToolResult {
        message_id: String,
        tool_call_id: String,
        name: String,
        output: String,
        is_error: bool,
    },
}

impl AgentConversationEvent {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::AssistantTurn {
                message_id,
                content,
                tool_calls,
                ..
            } => {
                ensure!(
                    !message_id.trim().is_empty() && message_id.len() <= 512,
                    "child conversation message id is invalid"
                );
                ensure!(
                    content.len() <= 128 * 1024,
                    "child conversation assistant content exceeds its size limit"
                );
                ensure!(
                    tool_calls.len() <= 32,
                    "child conversation turn exceeds its tool-call limit"
                );
                for call in tool_calls {
                    ensure!(
                        !call.id.trim().is_empty() && call.id.len() <= 512,
                        "child conversation tool-call id is invalid"
                    );
                    ensure!(
                        !call.name.trim().is_empty() && call.name.len() <= 128,
                        "child conversation tool name is invalid"
                    );
                }
                ensure!(
                    serde_json::to_vec(tool_calls)?.len() <= 128 * 1024,
                    "child conversation tool arguments exceed their size limit"
                );
            }
            Self::ToolResult {
                message_id,
                tool_call_id,
                name,
                output,
                ..
            } => {
                ensure!(
                    !message_id.trim().is_empty() && message_id.len() <= 512,
                    "child conversation message id is invalid"
                );
                ensure!(
                    !tool_call_id.trim().is_empty() && tool_call_id.len() <= 512,
                    "child conversation tool-call id is invalid"
                );
                ensure!(
                    !name.trim().is_empty() && name.len() <= 128,
                    "child conversation tool name is invalid"
                );
                ensure!(
                    output.len() <= 128 * 1024,
                    "child conversation tool output exceeds its size limit"
                );
            }
        }
        Ok(())
    }
}

/// Real-time progress update from a sub-agent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentProgress {
    /// First-class delegated runtime unit identifier.
    pub delegated_run_id: Option<String>,
    /// Agent task ID.
    pub task_id: String,
    /// Display name (derived from task context).
    pub name: String,
    /// Stable runtime identity. `name` remains the semantic task label while
    /// the identity carries the creature-themed display name.
    pub identity: Option<AgentIdentity>,
    /// Current status.
    pub status: AgentProgressStatus,
    /// Number of tool calls made.
    pub tool_count: usize,
    /// Approximate token usage.
    pub tokens: usize,
    /// Current action description (e.g. "reading app.rs").
    pub current_action: Option<String>,
    /// Short completion summary when the sub-agent finishes a delegated task.
    pub completion_summary: Option<String>,
    /// Lines added (for build agents).
    pub lines_added: usize,
    /// Lines removed (for build agents).
    pub lines_removed: usize,
    /// Plan task ID completed (for auto-marking tasks).
    pub completed_plan_task: Option<String>,
    /// Optional child chat update projected at a semantic conversation
    /// boundary. Summary-only consumers may safely ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_event: Option<AgentConversationEvent>,
}

/// Status of a sub-agent.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AgentProgressStatus {
    /// Logical task exists but has not entered scheduler admission yet.
    Created,
    /// Logical task is waiting for local/group admission.
    Queued,
    /// Durable task lease is held while provider capacity is pending.
    Leased,
    /// Agent is running.
    #[default]
    Running,
    /// A prior attempt ended and the logical task is eligible to run again.
    Retrying,
    /// Agent completed successfully.
    Complete,
    /// Agent completed with usable partial evidence or output.
    Degraded,
    /// Agent failed.
    Failed,
    /// Agent was cancelled before completing.
    Cancelled,
}

impl From<crate::storage::DelegationTaskState> for AgentProgressStatus {
    fn from(state: crate::storage::DelegationTaskState) -> Self {
        match state {
            crate::storage::DelegationTaskState::Created => Self::Created,
            crate::storage::DelegationTaskState::Queued => Self::Queued,
            crate::storage::DelegationTaskState::Leased => Self::Leased,
            crate::storage::DelegationTaskState::Running => Self::Running,
            crate::storage::DelegationTaskState::Retrying => Self::Retrying,
            crate::storage::DelegationTaskState::Complete => Self::Complete,
            crate::storage::DelegationTaskState::Degraded => Self::Degraded,
            crate::storage::DelegationTaskState::Failed => Self::Failed,
            crate::storage::DelegationTaskState::Cancelled => Self::Cancelled,
        }
    }
}

/// Configuration for a sub-agent task.
///
/// The model to use is determined by `SubAgentPool.override_model`, not by the task.
/// This provides a provider-agnostic experience where all sub-agents use the user's
/// current model.
#[derive(Debug, Clone)]
pub struct SubAgentTask {
    pub id: String,
    /// Display name for the agent (e.g. "tui", "agent", "main").
    pub name: String,
    /// Stable runtime identity kept separate from semantic task fields.
    pub identity: Option<AgentIdentity>,
    pub prompt: String,
    /// Bounded inherited project instructions, separate from user task text.
    /// Frozen before isolation; execution paths remain in working_dir.
    pub project_context: String,
    pub working_dir: PathBuf,
    /// Filesystem sandbox root inherited from the parent tool context.
    pub sandbox_root: Option<PathBuf>,
    /// Runtime-owned environment overrides for delegated commands. These are
    /// never provider-authored; isolation uses them to keep temporary files
    /// and dependency caches inside the attempt workspace.
    pub command_environment: BTreeMap<String, String>,
    /// Stable delegated runtime unit identifier shared by all tasks in one parent invocation.
    pub delegated_run_id: Option<String>,
    /// Durable logical group/task identity. These are separate from
    /// delegated_run_id because a logical task may have multiple attempts.
    pub delegation_group_id: Option<String>,
    pub delegation_task_id: Option<String>,
    /// Direct logical dependencies declared by the parent task graph. These
    /// are used to deliver bounded upstream handoff evidence only after the
    /// dependency wave has settled; sibling transcripts are never shared.
    pub depends_on: Vec<String>,
    /// Plan task ID this agent completes (for auto-marking).
    pub plan_task_id: Option<String>,
    /// Exact reasoning level inherited from the parent run. `None` keeps
    /// compatibility with older delegated records that did not persist it.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Inherited delegated execution policy from parent tool context.
    pub delegation_policy: Option<DelegationPolicy>,
    /// Optional per-task turn budget inherited from parent.
    pub max_turns_override: Option<usize>,
    /// Shared process registry inherited from the parent runtime. Delegated
    /// background commands must remain visible and controllable from the
    /// originating session instead of falling back to detached shell handles.
    pub process_registry: Option<Arc<ProcessRegistry>>,
    /// Parent owner key used to preserve multi-tenant process isolation.
    pub process_owner_id: Option<String>,
    /// Parent session used for tool-output scoping and delegated provenance.
    pub parent_session_id: Option<String>,
    /// Parent run's canonical provider-call accounting sink.
    pub provider_call_trace: Option<ProviderCallTraceContext>,
    /// Parent-to-child steering delivered between model turns.
    pub mailbox: Option<AgentMailbox>,
}

impl SubAgentTask {
    pub fn new(id: impl Into<String>, prompt: impl Into<String>) -> Self {
        let id = id.into();
        let name = id.clone();
        Self {
            id,
            name,
            identity: None,
            prompt: prompt.into(),
            project_context: String::new(),
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            sandbox_root: None,
            command_environment: BTreeMap::new(),
            delegated_run_id: None,
            delegation_group_id: None,
            delegation_task_id: None,
            depends_on: Vec::new(),
            plan_task_id: None,
            reasoning_effort: None,
            delegation_policy: None,
            max_turns_override: None,
            process_registry: None,
            process_owner_id: None,
            parent_session_id: None,
            provider_call_trace: None,
            mailbox: None,
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_identity(mut self, identity: AgentIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn ensure_identity(&mut self, parent_path: &str, role: &str, ordinal: usize) {
        if self.identity.is_none() {
            self.identity = Some(AgentIdentity::child(
                self.id.clone(),
                parent_path,
                self.name.clone(),
                role,
                ordinal,
            ));
        }
    }

    pub fn display_name(&self) -> String {
        self.identity
            .as_ref()
            .map_or_else(|| self.name.clone(), AgentIdentity::display_name)
    }

    pub fn with_project_context(mut self, context: String) -> Self {
        self.project_context = context;
        self
    }

    pub fn with_working_dir(mut self, dir: PathBuf) -> Self {
        self.working_dir = dir;
        self
    }

    pub fn with_sandbox_root(mut self, sandbox_root: PathBuf) -> Self {
        self.sandbox_root = Some(sandbox_root);
        self
    }

    pub fn with_command_environment(mut self, environment: BTreeMap<String, String>) -> Self {
        self.command_environment = environment;
        self
    }

    pub fn with_delegated_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.delegated_run_id = Some(run_id.into());
        self
    }

    pub fn with_delegation_task(
        mut self,
        group_id: impl Into<String>,
        task_id: impl Into<String>,
    ) -> Self {
        self.delegation_group_id = Some(group_id.into());
        self.delegation_task_id = Some(task_id.into());
        self
    }

    pub fn with_dependencies(mut self, dependencies: Vec<String>) -> Self {
        self.depends_on = dependencies;
        self
    }

    pub fn with_plan_task_id(mut self, task_id: impl Into<String>) -> Self {
        self.plan_task_id = Some(task_id.into());
        self
    }

    pub fn with_thinking(mut self, enabled: bool) -> Self {
        self.reasoning_effort = enabled.then_some(ReasoningEffort::Medium);
        self
    }

    pub fn with_reasoning_effort(mut self, effort: Option<ReasoningEffort>) -> Self {
        self.reasoning_effort = effort;
        self
    }

    pub fn with_delegation_policy(mut self, policy: DelegationPolicy) -> Self {
        self.delegation_policy = Some(policy);
        self
    }

    pub fn with_max_turns(mut self, max_turns: usize) -> Self {
        self.max_turns_override = Some(max_turns);
        self
    }

    pub fn with_mailbox(mut self, mailbox: AgentMailbox) -> Self {
        self.mailbox = Some(mailbox);
        self
    }

    pub fn with_provider_call_trace(
        mut self,
        provider_call_trace: Option<ProviderCallTraceContext>,
    ) -> Self {
        self.provider_call_trace = provider_call_trace;
        self
    }

    pub fn with_process_context(
        mut self,
        process_registry: Option<Arc<ProcessRegistry>>,
        process_owner_id: Option<String>,
        parent_session_id: Option<String>,
    ) -> Self {
        self.process_registry = process_registry;
        self.process_owner_id = process_owner_id;
        self.parent_session_id = parent_session_id;
        self
    }

    /// Produce a stable process-only owner for this exact delegated task. The
    /// parent tenant owner remains separate for reports, memory, and storage.
    pub fn delegated_process_owner_id(&self) -> String {
        // The server represents single-tenant mode as `None`, while the
        // process registry represents that same owner as `default`.
        let parent_owner = self.process_owner_id.as_deref().unwrap_or("default");
        let group = self
            .delegation_group_id
            .as_deref()
            .or(self.delegated_run_id.as_deref())
            .unwrap_or("local");
        let mut hasher = Sha256::new();
        hasher.update(group.as_bytes());
        hasher.update([0]);
        hasher.update(self.id.as_bytes());
        let digest = format!("{:x}", hasher.finalize());
        format!("{parent_owner}:hive:{}", &digest[..16])
    }
}
