use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};
use std::str::FromStr;
use std::time::Duration;

use crate::ai::providers::ProviderId;
use crate::ai::types::AiTool;
use crate::storage::WorkMode;

/// Default tool execution timeout (2 minutes)
pub(crate) const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(120);
/// Delegated audit/build tools can legitimately run much longer than generic reads.
pub(crate) const DELEGATED_TOOL_TIMEOUT: Duration = Duration::from_secs(900);

/// Maximum number of function tools in the default coding-agent request.
pub const DEFAULT_CODE_TOOL_LIMIT: usize = 10;

const STANDARD_CODE_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "agent",
    "apply_patch",
    "bash",
    "enter_plan_mode",
    "glob",
    "grep",
    "read",
    "tool_search",
];

const STANDARD_EDIT_WRITE_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "agent",
    "bash",
    "edit",
    "enter_plan_mode",
    "glob",
    "grep",
    "read",
    "tool_search",
    "write",
];

const PLAN_MODE_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "agent",
    "glob",
    "grep",
    "read",
    "tool_search",
    "workflow_propose",
];

const ACTIVE_PLAN_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "agent",
    "apply_patch",
    "bash",
    "read",
    "task_complete",
    "task_start",
    "tool_search",
    "workflow_update",
];

const ACTIVE_PLAN_EDIT_WRITE_TOOLS: &[&str] = &[
    "AskUserQuestion",
    "agent",
    "bash",
    "edit",
    "read",
    "task_complete",
    "task_start",
    "tool_search",
    "workflow_update",
    "write",
];

/// Active implementation plans add canonical lifecycle tools to the normal
/// coding surface. Keeping these direct is intentional: lifecycle and user
/// interaction tools are intercepted by the orchestrator and cannot be safely
/// emulated by nested registry dispatch.
const ACTIVE_PLAN_TOOL_LIMIT: usize = 9;
const ACTIVE_PLAN_EDIT_WRITE_TOOL_LIMIT: usize = 10;

/// Mutation grammar exposed directly to a model family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MutationToolSurface {
    /// GPT/Codex models reliably produce the structured multi-file patch grammar.
    #[default]
    ApplyPatch,
    /// Grok, Claude, Gemini, Kimi, and generic models are more reliable with
    /// exact replacement plus whole-file creation tools.
    EditWrite,
}

impl MutationToolSurface {
    pub fn for_model(provider: ProviderId, model_id: &str) -> Self {
        if provider == ProviderId::Grok {
            return Self::EditWrite;
        }

        let model = model_id.trim().to_ascii_lowercase();
        if provider == ProviderId::OpenAI
            || model.contains("codex")
            || model.starts_with("gpt-")
            || model.contains("/gpt-")
            || model.starts_with("o1")
            || model.starts_with("o3")
            || model.starts_with("o4")
        {
            Self::ApplyPatch
        } else {
            Self::EditWrite
        }
    }
}

/// Request-time exposure policy for the primary coding surface.
///
/// The registry may contain dozens of built-in, MCP, extension, and plugin
/// tools. Keeping only the high-frequency surface on the wire improves prompt
/// caching and tool choice. `tool_search` preserves governed access to regular
/// specialist tools without serializing every schema on every request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRequestPolicy {
    pub permission_mode: PermissionMode,
    pub plan_mode: bool,
    pub active_plan: bool,
    pub supervised_approval_available: bool,
    pub mutation_surface: MutationToolSurface,
    disabled_tools: HashSet<String>,
}

impl Default for ToolRequestPolicy {
    fn default() -> Self {
        Self::code(PermissionMode::Autonomous, false, false, true, &[])
    }
}

impl ToolRequestPolicy {
    pub fn code(
        permission_mode: PermissionMode,
        plan_mode: bool,
        active_plan: bool,
        supervised_approval_available: bool,
        disabled_tools: &[String],
    ) -> Self {
        Self {
            permission_mode,
            plan_mode,
            active_plan,
            supervised_approval_available,
            mutation_surface: MutationToolSurface::ApplyPatch,
            disabled_tools: disabled_tools.iter().cloned().collect(),
        }
    }

    pub fn with_mutation_surface(mut self, mutation_surface: MutationToolSurface) -> Self {
        self.mutation_surface = mutation_surface;
        self
    }

    pub fn filter(&self, tools: Vec<AiTool>) -> Vec<AiTool> {
        let (selected, limit): (&[&str], usize) = if self.plan_mode {
            (PLAN_MODE_TOOLS, DEFAULT_CODE_TOOL_LIMIT)
        } else if self.active_plan {
            match self.mutation_surface {
                MutationToolSurface::ApplyPatch => (ACTIVE_PLAN_TOOLS, ACTIVE_PLAN_TOOL_LIMIT),
                MutationToolSurface::EditWrite => (
                    ACTIVE_PLAN_EDIT_WRITE_TOOLS,
                    ACTIVE_PLAN_EDIT_WRITE_TOOL_LIMIT,
                ),
            }
        } else {
            match self.mutation_surface {
                MutationToolSurface::ApplyPatch => (STANDARD_CODE_TOOLS, DEFAULT_CODE_TOOL_LIMIT),
                MutationToolSurface::EditWrite => {
                    (STANDARD_EDIT_WRITE_TOOLS, DEFAULT_CODE_TOOL_LIMIT)
                }
            }
        };

        let mut filtered = tools
            .into_iter()
            .filter(|tool| selected.contains(&tool.name.as_str()))
            .filter(|tool| !self.disabled_tools.contains(&tool.name))
            // An autonomous Build turn is already authorized to execute. The
            // model must not convert it into a draft-plan approval stop; users
            // who want planning select Plan mode explicitly at the request
            // boundary. Supervised Build retains the voluntary switch.
            .filter(|tool| {
                self.permission_mode != PermissionMode::Autonomous
                    || self.plan_mode
                    || tool.name != "enter_plan_mode"
            })
            .filter(|tool| {
                let policy = tool_policy(&tool.name);
                !self.plan_mode || policy.allowed_in_plan_mode
            })
            .filter(|tool| {
                self.permission_mode != PermissionMode::Supervised
                    || self.supervised_approval_available
                    || (!tool_policy(&tool.name).requires_supervised_approval
                        && !matches!(tool.name.as_str(), "agent" | "tool_search"))
            })
            .collect::<Vec<_>>();

        filtered.sort_by(|left, right| left.name.cmp(&right.name));
        filtered.truncate(limit);
        filtered
    }

    pub fn is_disabled(&self, tool_name: &str) -> bool {
        self.disabled_tools.contains(tool_name)
    }
}

/// Tool category for permission checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCategory {
    /// Read-only tools that never modify state.
    ReadOnly,
    /// Write tools that modify files, execute commands, etc.
    Write,
    /// Interactive tools that require user input.
    Interactive,
}

/// Centralized policy contract for a tool name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolPolicy {
    pub category: ToolCategory,
    pub requires_supervised_approval: bool,
    pub retry_timeout_once: bool,
    pub allowed_in_plan_mode: bool,
    pub timeout_override: Option<Duration>,
}

/// Runtime authorization result for a concrete tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolAuthorization {
    /// The tool call may execute immediately.
    Execute,
    /// The tool call may execute only after user approval.
    RequiresApproval,
    /// The tool call is blocked because it is not allowed in plan mode.
    BlockedInPlanMode,
}

impl ToolAuthorization {
    pub fn requires_approval(self) -> bool {
        self == Self::RequiresApproval
    }

    pub fn is_blocked(self) -> bool {
        self == Self::BlockedInPlanMode
    }
}

impl ToolPolicy {
    const fn read_only() -> Self {
        Self {
            category: ToolCategory::ReadOnly,
            requires_supervised_approval: false,
            retry_timeout_once: true,
            allowed_in_plan_mode: true,
            timeout_override: None,
        }
    }

    const fn read_only_with_timeout(timeout_override: Duration) -> Self {
        Self {
            category: ToolCategory::ReadOnly,
            requires_supervised_approval: false,
            retry_timeout_once: true,
            allowed_in_plan_mode: true,
            timeout_override: Some(timeout_override),
        }
    }

    const fn read_only_without_retry_with_timeout(timeout_override: Duration) -> Self {
        Self {
            category: ToolCategory::ReadOnly,
            requires_supervised_approval: false,
            retry_timeout_once: false,
            allowed_in_plan_mode: true,
            timeout_override: Some(timeout_override),
        }
    }

    const fn interactive() -> Self {
        Self {
            category: ToolCategory::Interactive,
            requires_supervised_approval: false,
            retry_timeout_once: false,
            allowed_in_plan_mode: true,
            timeout_override: None,
        }
    }

    const fn interactive_with_supervised_approval() -> Self {
        Self {
            category: ToolCategory::Interactive,
            requires_supervised_approval: true,
            retry_timeout_once: false,
            allowed_in_plan_mode: true,
            timeout_override: None,
        }
    }

    const fn write() -> Self {
        Self {
            category: ToolCategory::Write,
            requires_supervised_approval: true,
            retry_timeout_once: false,
            allowed_in_plan_mode: false,
            timeout_override: None,
        }
    }

    const fn write_with_timeout(timeout_override: Duration) -> Self {
        Self {
            category: ToolCategory::Write,
            requires_supervised_approval: true,
            retry_timeout_once: false,
            allowed_in_plan_mode: false,
            timeout_override: Some(timeout_override),
        }
    }
}

/// Permission mode for tool execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PermissionMode {
    Supervised,
    #[default]
    Autonomous,
}

impl PermissionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Supervised => "supervised",
            Self::Autonomous => "autonomous",
        }
    }
}

impl FromStr for PermissionMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "supervised" => Ok(Self::Supervised),
            "autonomous" => Ok(Self::Autonomous),
            other => Err(format!("Unknown permission mode: {other}")),
        }
    }
}

/// Delegated execution surface type for governance/audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegationSurface {
    SubagentExplore,
    SubagentBuild,
    SubagentPlan,
    SubagentVerify,
    McpRemote,
    Skill,
    Extension,
}

/// Inherited delegated execution policy from the parent orchestrator/tool context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationPolicy {
    pub surface: DelegationSurface,
    pub inherited_permission_mode: PermissionMode,
    /// The parent explicitly approved the outer delegated run and its declared
    /// capability ceiling. This is deliberately run-scoped: it does not change
    /// the session's permission mode or authorize tools outside the immutable
    /// child allowlist.
    #[serde(default)]
    pub supervised_approval_granted: bool,
    pub max_turns: Option<usize>,
    pub read_only_only: bool,
    pub bash_allowed: bool,
    /// Immutable tool-capability ceiling inherited from an explicit parent
    /// execution scope. `None` preserves the ordinary governed delegation
    /// surface; `Some(empty)` deliberately creates a tool-free child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_tool_allowlist: Option<BTreeSet<String>>,
}

impl DelegationPolicy {
    /// Whether this policy is an equal-or-narrower executable subset of a
    /// parent/group ceiling. Surface labels describe intent and may differ;
    /// executable authority is defined by permission, turn, read/write/bash,
    /// approval, and exact tool allowlists.
    pub fn is_within(&self, ceiling: &Self) -> bool {
        if self.inherited_permission_mode != ceiling.inherited_permission_mode
            || (self.supervised_approval_granted && !ceiling.supervised_approval_granted)
            || (!self.read_only_only && ceiling.read_only_only)
            || (self.bash_allowed && !ceiling.bash_allowed)
        {
            return false;
        }

        match (self.max_turns, ceiling.max_turns) {
            (Some(requested), Some(limit)) if requested > limit => return false,
            (None, Some(_)) => return false,
            _ => {}
        }

        match (
            self.execution_tool_allowlist.as_ref(),
            ceiling.execution_tool_allowlist.as_ref(),
        ) {
            (Some(requested), Some(allowed)) => requested.is_subset(allowed),
            (None, Some(_)) => false,
            _ => true,
        }
    }

    pub fn for_subagent_explore(
        inherited_permission_mode: PermissionMode,
        max_turns: Option<usize>,
    ) -> Self {
        Self {
            surface: DelegationSurface::SubagentExplore,
            inherited_permission_mode,
            supervised_approval_granted: false,
            max_turns,
            read_only_only: true,
            bash_allowed: false,
            execution_tool_allowlist: None,
        }
    }

    pub fn for_subagent_build(
        inherited_permission_mode: PermissionMode,
        max_turns: Option<usize>,
    ) -> Self {
        Self {
            surface: DelegationSurface::SubagentBuild,
            inherited_permission_mode,
            supervised_approval_granted: false,
            max_turns,
            read_only_only: false,
            bash_allowed: false,
            execution_tool_allowlist: None,
        }
    }

    pub fn for_subagent_plan(
        inherited_permission_mode: PermissionMode,
        max_turns: Option<usize>,
    ) -> Self {
        Self {
            surface: DelegationSurface::SubagentPlan,
            inherited_permission_mode,
            supervised_approval_granted: false,
            max_turns,
            read_only_only: true,
            bash_allowed: false,
            execution_tool_allowlist: None,
        }
    }

    pub fn for_subagent_verify(
        inherited_permission_mode: PermissionMode,
        max_turns: Option<usize>,
    ) -> Self {
        Self {
            surface: DelegationSurface::SubagentVerify,
            inherited_permission_mode,
            supervised_approval_granted: false,
            max_turns,
            read_only_only: true,
            bash_allowed: true,
            execution_tool_allowlist: None,
        }
    }

    /// Agnostic child policy from parent-requested capabilities.
    ///
    /// Write → build surface. Execute without write → read tools + bash.
    /// Otherwise → read-only explore surface.
    pub fn for_subagent_child(
        inherited_permission_mode: PermissionMode,
        max_turns: Option<usize>,
        wants_read: bool,
        wants_write: bool,
        wants_execute: bool,
    ) -> Self {
        let mut policy = if wants_write {
            Self::for_subagent_build(inherited_permission_mode, max_turns)
        } else if wants_execute {
            Self::for_subagent_verify(inherited_permission_mode, max_turns)
        } else {
            Self::for_subagent_explore(inherited_permission_mode, max_turns)
        };

        // `bash_allowed` is part of the immutable policy lattice, not merely an
        // authorization shortcut for read-only verifier surfaces. Keep it in
        // lockstep with the exact execute capability so a mixed write+execute
        // group is a truthful ceiling for an execute-only verifier task.
        policy.bash_allowed = wants_execute;

        // Capability requests are an exact child-side ceiling. In particular,
        // execute-only must not silently acquire read access, and write does
        // not imply shell execution. The immutable parent ceiling is
        // intersected below.
        let mut allowlist = BTreeSet::new();
        if wants_read {
            allowlist.extend(
                [
                    "glob",
                    "grep",
                    "list",
                    "read",
                    "tool_search",
                    "web_fetch",
                    "web_search",
                ]
                .into_iter()
                .map(ToString::to_string),
            );
        }
        if wants_write {
            allowlist.extend(
                ["apply_patch", "edit", "multiedit", "write"]
                    .into_iter()
                    .map(ToString::to_string),
            );
        }
        if wants_execute {
            allowlist.insert("bash".to_string());
            allowlist.insert("browser_check".to_string());
        }
        policy.execution_tool_allowlist = Some(allowlist);
        policy
    }

    /// Intersect delegated execution with the parent's exact per-turn tool
    /// scope. The parent scope is already the narrowest run-level capability,
    /// so a child may retain only names present in it; it must never reconstruct
    /// the default build/explore surface.
    pub fn with_execution_tool_allowlist(
        mut self,
        execution_tool_allowlist: Option<&HashSet<String>>,
    ) -> Self {
        if let Some(parent_allowlist) = execution_tool_allowlist {
            self.execution_tool_allowlist = Some(match self.execution_tool_allowlist.take() {
                Some(child_allowlist) => child_allowlist
                    .into_iter()
                    .filter(|name| parent_allowlist.contains(name))
                    .collect(),
                None => parent_allowlist.iter().cloned().collect(),
            });
        }
        self
    }

    /// Carry the result of the parent Agent-tool approval into the child
    /// governance contract. The capability allowlist remains the hard ceiling.
    pub fn with_supervised_approval(mut self, granted: bool) -> Self {
        self.supervised_approval_granted = granted;
        self
    }

    pub fn authorize_tool(&self, tool_name: &str, plan_mode: bool) -> Result<(), String> {
        self.authorize_non_recursive_tool(tool_name)?;
        self.authorize_execution_scope(tool_name, tool_name)?;
        self.authorize_tool_policy(tool_name, tool_policy(tool_name), plan_mode)
    }

    pub fn authorize_tool_call(
        &self,
        tool_name: &str,
        params: &Value,
        plan_mode: bool,
    ) -> Result<(), String> {
        let (effective_name, effective_params) = effective_tool_call(tool_name, params);
        self.authorize_non_recursive_tool(tool_name)?;
        self.authorize_non_recursive_tool(effective_name)?;
        self.authorize_execution_scope(tool_name, effective_name)?;
        if self.is_subagent()
            && self
                .execution_tool_allowlist
                .as_ref()
                .is_some_and(|allowlist| allowlist.contains("browser_check"))
            && effective_name == "bash"
            && effective_params
                .get("command")
                .and_then(Value::as_str)
                .is_some_and(is_browser_runtime_install_command)
        {
            return Err(
                "Delegated policy blocked browser-runtime installation: use the governed browser_check tool with the installed browser instead of downloading Playwright, Puppeteer, or another browser runtime"
                    .to_string(),
            );
        }
        self.authorize_tool_policy(
            effective_name,
            tool_policy_for_call(effective_name, effective_params),
            plan_mode,
        )
    }

    fn authorize_execution_scope(
        &self,
        wrapper_name: &str,
        effective_name: &str,
    ) -> Result<(), String> {
        let Some(allowlist) = self.execution_tool_allowlist.as_ref() else {
            return Ok(());
        };

        if allowlist.contains(wrapper_name) && allowlist.contains(effective_name) {
            return Ok(());
        }

        Err(format!(
            "Delegated policy blocked tool '{}': it exceeds the parent run's explicit tool capability",
            effective_name
        ))
    }

    fn authorize_non_recursive_tool(&self, tool_name: &str) -> Result<(), String> {
        if self.is_subagent()
            && matches!(
                tool_name,
                "agent" | "skill" | "enter_plan_mode" | "set_work_mode" | "set_workspace_context"
            )
        {
            return Err(format!(
                "Delegated policy blocked tool '{}': delegated agents cannot recursively delegate or change parent runtime mode",
                tool_name
            ));
        }
        Ok(())
    }

    fn is_subagent(&self) -> bool {
        matches!(
            self.surface,
            DelegationSurface::SubagentExplore
                | DelegationSurface::SubagentBuild
                | DelegationSurface::SubagentPlan
                | DelegationSurface::SubagentVerify
        )
    }

    fn authorize_tool_policy(
        &self,
        tool_name: &str,
        policy: ToolPolicy,
        plan_mode: bool,
    ) -> Result<(), String> {
        if self.read_only_only
            && policy.category != ToolCategory::ReadOnly
            && !(self.bash_allowed && tool_name == "bash")
        {
            return Err(format!(
                "Delegated policy blocked tool '{}': {} only permits read-only tools",
                tool_name,
                self.surface_name()
            ));
        }
        if self.inherited_permission_mode == PermissionMode::Supervised
            && policy.requires_supervised_approval
            && !self.supervised_approval_granted
        {
            return Err(format!(
                "Delegated policy blocked tool '{}': supervised parent requires approval for write-capable tools",
                tool_name
            ));
        }
        if plan_mode && !policy.allowed_in_plan_mode {
            return Err(format!(
                "Delegated policy blocked tool '{}': tool is disallowed in plan mode",
                tool_name
            ));
        }
        Ok(())
    }

    pub fn surface_name(&self) -> &'static str {
        match self.surface {
            DelegationSurface::SubagentExplore => "subagent_explore",
            DelegationSurface::SubagentBuild => "subagent_build",
            DelegationSurface::SubagentPlan => "subagent_plan",
            DelegationSurface::SubagentVerify => "subagent_verify",
            DelegationSurface::McpRemote => "mcp_remote",
            DelegationSurface::Skill => "skill",
            DelegationSurface::Extension => "extension",
        }
    }

    pub fn audit_json(&self) -> Value {
        serde_json::json!({
            "surface": self.surface_name(),
            "permission_mode": self.inherited_permission_mode,
            "supervised_approval_granted": self.supervised_approval_granted,
            "max_turns": self.max_turns,
            "read_only_only": self.read_only_only,
            "bash_allowed": self.bash_allowed,
            "execution_tool_allowlist": self.execution_tool_allowlist,
        })
    }
}

fn is_browser_runtime_install_command(command: &str) -> bool {
    let normalized = command.to_ascii_lowercase();
    (normalized.contains("playwright")
        && (normalized.contains(" install") || normalized.contains("install-deps")))
        || (normalized.contains("puppeteer")
            && (normalized.contains(" install") || normalized.contains("browsers install")))
}

/// Authorize a concrete top-level tool call under the current runtime mode.
pub fn authorize_tool_call(
    name: &str,
    params: &Value,
    permission_mode: PermissionMode,
    plan_mode: bool,
) -> ToolAuthorization {
    let policy = tool_policy_for_call(name, params);
    if plan_mode && !policy.allowed_in_plan_mode {
        return ToolAuthorization::BlockedInPlanMode;
    }

    if permission_mode == PermissionMode::Supervised && policy.requires_supervised_approval {
        return ToolAuthorization::RequiresApproval;
    }

    ToolAuthorization::Execute
}

/// Categorize a tool by name.
pub fn tool_category(name: &str) -> ToolCategory {
    tool_policy(name).category
}

/// Resolve the canonical policy for a concrete tool call.
///
/// This extends the name-only policy with argument-aware intent detection for
/// polymorphic tools. In particular, `agent(agent_type = "build")` is
/// write-capable even though other agent subtypes are read-only delegations.
/// Canonical direct Code tool surface for an effective work mode.
///
/// Single source shared by the agent loop's mode transitions and the HTTP
/// session/mode-override routes so per-turn allowlist validation never runs
/// against stale or divergent schemas.
pub fn filter_code_tools_for_mode(
    tools: Vec<AiTool>,
    permission_mode: PermissionMode,
    work_mode: WorkMode,
    has_active_plan: bool,
    disabled_tools: &[String],
    provider: ProviderId,
    model: &str,
) -> Vec<AiTool> {
    ToolRequestPolicy::code(
        permission_mode,
        work_mode == WorkMode::Plan,
        has_active_plan,
        true,
        disabled_tools,
    )
    .with_mutation_surface(MutationToolSurface::for_model(provider, model))
    .filter(tools)
}

pub fn tool_policy_for_call(name: &str, params: &Value) -> ToolPolicy {
    match name {
        "agent" => agent_tool_policy(params),
        "tool_search" => tool_search_policy(params),
        _ => tool_policy(name),
    }
}

/// Resolve the user-visible operation represented by a wrapper call.
///
/// Approval UIs retain the wrapper call ID for protocol continuity, but must
/// show the effective target and target arguments so consent is informed.
pub fn effective_tool_call<'a>(name: &'a str, params: &'a Value) -> (&'a str, &'a Value) {
    if name == "tool_search" && params.get("action").and_then(Value::as_str) == Some("execute") {
        if let Some(target) = params.get("tool").and_then(Value::as_str) {
            if target != "tool_search" {
                return (target, params.get("arguments").unwrap_or(&Value::Null));
            }
        }
    }

    (name, params)
}

fn tool_search_policy(params: &Value) -> ToolPolicy {
    if params.get("action").and_then(Value::as_str) != Some("execute") {
        return ToolPolicy::read_only();
    }

    let Some(target) = params.get("tool").and_then(Value::as_str) else {
        return ToolPolicy::read_only();
    };
    if target == "tool_search" {
        return ToolPolicy::read_only();
    }

    let arguments = params.get("arguments").unwrap_or(&Value::Null);
    tool_policy_for_call(target, arguments)
}

fn agent_tool_policy(params: &Value) -> ToolPolicy {
    match agent_call_action(params) {
        "message" | "interrupt" => ToolPolicy::interactive(),
        // A followup steers a live child when one exists, but the same call
        // resumes a terminal child as a new run. Authorize the maximum side
        // effect of the wrapper so a write-capable durable contract cannot be
        // restarted through an approval-free interactive call.
        "followup" => ToolPolicy::write_with_timeout(DELEGATED_TOOL_TIMEOUT),
        "list" | "status" | "wait" => ToolPolicy::read_only_with_timeout(DELEGATED_TOOL_TIMEOUT),
        "resume" => ToolPolicy::write_with_timeout(DELEGATED_TOOL_TIMEOUT),
        _ if agent_call_requests_write(params) => {
            ToolPolicy::write_with_timeout(DELEGATED_TOOL_TIMEOUT)
        }
        // A read-capable spawn is read-only with respect to the workspace, but
        // it creates a durable run and starts provider work. Retrying it after
        // the outer timeout would create a second child after the first run's
        // lease has been cancelled, so run-start calls are non-retryable.
        _ => ToolPolicy::read_only_without_retry_with_timeout(DELEGATED_TOOL_TIMEOUT),
    }
}

pub fn agent_call_action(params: &Value) -> &str {
    params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("spawn")
}

pub fn agent_call_starts_run(params: &Value) -> bool {
    // A terminal followup may resume internally, but a live followup is only
    // mailbox steering. Keep definite run accounting separate from the
    // conservative write authorization applied to every followup.
    matches!(agent_call_action(params), "spawn" | "resume")
}

/// Whether an Agent call needs the delegated-progress execution bridge.
///
/// Unlike `agent_call_starts_run`, this is intentionally conservative: a
/// followup can be mailbox-only for a live child or become a new durable run
/// after the tool resolves the referenced record. Installing a progress sender
/// for both cases is harmless; the executor decides whether to detach that
/// sender from the returned result, once it knows whether a background run was
/// actually started.
pub fn agent_call_may_start_run(params: &Value) -> bool {
    matches!(agent_call_action(params), "spawn" | "resume" | "followup")
}

pub fn agent_call_requests_write(params: &Value) -> bool {
    if agent_call_action(params) == "resume" {
        return true;
    }
    let contract = resolved_agent_call_capabilities(params);
    if contract.explicit {
        return contract.values.contains("write") || contract.values.contains("execute");
    }
    ["profile", "agent_type"].iter().any(|field| {
        matches!(
            params.get(field).and_then(Value::as_str),
            Some("build" | "worker" | "verify")
        )
    })
}

pub fn agent_call_execution_profile(params: &Value) -> &'static str {
    let contract = resolved_agent_call_capabilities(params);
    if contract.explicit {
        return if contract.values.contains("write") {
            "build"
        } else {
            "explore"
        };
    }

    let profile = params
        .get("profile")
        .and_then(Value::as_str)
        .or_else(|| params.get("agent_type").and_then(Value::as_str));
    match profile {
        Some("plan") => "plan",
        Some("verify") => "verify",
        Some("build") => "build",
        Some("explore") => "explore",
        _ if agent_call_requests_write(params) => "build",
        _ => "explore",
    }
}

/// Whether a delegated Agent spawn is research work for loop accounting.
///
/// Explicit capabilities are authoritative: read/read+execute count, while
/// any write-capable child does not. Legacy profile labels remain a fallback
/// for older clients that do not send capabilities.
pub fn agent_call_is_research(params: &Value) -> bool {
    if !agent_call_starts_run(params) {
        return false;
    }

    let contract = resolved_agent_call_capabilities(params);
    if contract.explicit {
        return contract.values.contains("read") && !contract.values.contains("write");
    }

    // A bare durable resume restores capabilities from storage after top-level
    // authorization, so its research/write nature cannot be inferred here.
    if agent_call_action(params) == "resume" {
        return false;
    }

    !matches!(
        params
            .get("profile")
            .and_then(Value::as_str)
            .or_else(|| params.get("agent_type").and_then(Value::as_str)),
        Some("build" | "worker")
    )
}

#[derive(Debug, Default)]
struct ResolvedAgentCallCapabilities<'a> {
    explicit: bool,
    values: BTreeSet<&'a str>,
}

/// Resolve the capability ceiling from the complete raw Agent invocation.
///
/// Authorization and loop accounting run before the Agent tool deserializes
/// and normalizes a structured task graph. They must therefore observe nested
/// task capabilities too; otherwise the executor can correctly see a build
/// graph while the outer loop incorrectly treats the same call as research.
fn resolved_agent_call_capabilities(params: &Value) -> ResolvedAgentCallCapabilities<'_> {
    let mut resolved = ResolvedAgentCallCapabilities::default();
    if let Some(capabilities) = params.get("capabilities").and_then(Value::as_array) {
        resolved.explicit = true;
        resolved
            .values
            .extend(capabilities.iter().filter_map(Value::as_str));
    }

    if let Some(tasks) = params.get("tasks").and_then(Value::as_array) {
        // A structured task with omitted capabilities is normalized to read by
        // the executor. Record that default here so every pre-execution policy
        // consumer sees the same contract.
        resolved.explicit = true;
        for task in tasks {
            match task.get("capabilities").and_then(Value::as_array) {
                Some(capabilities) => resolved
                    .values
                    .extend(capabilities.iter().filter_map(Value::as_str)),
                None => {
                    resolved.values.insert("read");
                }
            }
        }
    }

    resolved
}

/// Resolve the canonical policy for a tool name.
pub fn tool_policy(name: &str) -> ToolPolicy {
    match name {
        "agent" => ToolPolicy::read_only_without_retry_with_timeout(DELEGATED_TOOL_TIMEOUT),
        "read"
        | "browser_check"
        | "glob"
        | "grep"
        | "list"
        | "web_search"
        | "web_fetch"
        | "skill"
        | "tool_search"
        | "workflow_read"
        | "mcp__list_resources"
        | "mcp__list_resource_templates"
        | "mcp__read_resource"
        | "mcp__list_prompts"
        | "mcp__get_prompt"
        | "mcp__list_tools" => ToolPolicy::read_only(),
        "AskUserQuestion" | "PlanConfirm" | "enter_plan_mode" | "memory" | "set_work_mode"
        | "task_start" | "task_complete" | "add_subtask" | "set_dependency"
        | "workflow_propose" | "workflow_update" | "send_user_message" | "sleep"
        | "autonomous_task" | "report" => ToolPolicy::interactive(),
        "set_workspace_context" => ToolPolicy::interactive_with_supervised_approval(),
        _ => ToolPolicy::write(),
    }
}
