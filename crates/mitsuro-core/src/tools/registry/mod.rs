//! Tool registry for managing available tools.
//!
//! Supports pre/post execution hooks for logging, validation, and safety.

mod context;
mod policy;
mod result;
mod runtime;

pub use context::{
    FileObservationTracker, FilesystemAccess, ShellIsolationPolicy, ToolContext, ToolOutputChunk,
};
pub use policy::{
    agent_call_action, agent_call_execution_profile, agent_call_is_research,
    agent_call_may_start_run, agent_call_requests_write, agent_call_starts_run,
    authorize_tool_call, effective_tool_call, filter_code_tools_for_mode, tool_category,
    tool_policy, tool_policy_for_call, DelegationPolicy, DelegationSurface, MutationToolSurface,
    PermissionMode, ToolAuthorization, ToolCategory, ToolPolicy, ToolRequestPolicy,
    DEFAULT_CODE_TOOL_LIMIT,
};
pub use result::{parse_params, progress_change_key_for_paths, trusted_changed, ToolResult};
pub use runtime::{Tool, ToolRegistry};

#[cfg(test)]
mod tests;
