//! Tool implementations.
//!
//! This module is the catalog/facade for concrete tool types. Registration
//! policy for CLI/ACP/Hive surfaces lives in `registration.rs` so the catalog
//! does not also own runtime exposure decisions.

pub mod add_subtask;
pub mod agent;
pub mod apply_patch;
pub mod ask_user;
pub mod autonomous_task;
pub mod bash;
pub mod browser_check;
pub mod edit;
pub mod glob;
pub mod grep;
pub mod list;
pub mod memory;
pub mod multiedit;
mod mutation_diagnostics;
pub mod plan_mode;
pub mod post_to_group;
pub mod processes;
pub mod read;
pub mod report;
pub mod search_compaction_segments;
pub mod send_to_worker;
pub mod send_user_message;
pub mod set_dependency;
pub mod set_work_mode;
pub mod set_workspace_context;
pub mod skill;
pub mod sleep;
pub mod task_complete;
pub mod task_start;
pub mod tool_search;
pub mod web_fetch;
pub mod web_search;
pub mod workflow_propose;
pub mod workflow_read;
pub mod workflow_update;
pub mod write;

mod registration;
mod web_utils;

pub use add_subtask::AddSubtaskTool;
pub use agent::AgentTool;
pub use apply_patch::ApplyPatchTool;
pub use ask_user::AskUserQuestionTool;
pub use autonomous_task::AutonomousTaskTool;
pub use bash::BashTool;
pub use browser_check::BrowserCheckTool;
pub use edit::EditTool;
pub use glob::GlobTool;
pub use grep::GrepTool;
pub use list::ListTool;
pub use memory::MemoryTool;
pub use multiedit::MultiEditTool;
pub use plan_mode::EnterPlanModeTool;
pub use post_to_group::PostToGroupTool;
pub use processes::ProcessesTool;
pub use read::ReadTool;
pub use registration::{
    register_acp_tools, register_agent_tool, register_all_tools, register_hive_tools,
};
pub use report::ReportTool;
pub use search_compaction_segments::SearchCompactionSegmentsTool;
pub use send_to_worker::SendToWorkerTool;
pub use send_user_message::SendUserMessageTool;
pub use set_dependency::SetDependencyTool;
pub use set_work_mode::SetWorkModeTool;
pub use set_workspace_context::SetWorkspaceContextTool;
pub use skill::SkillTool;
pub use sleep::SleepTool;
pub use task_complete::TaskCompleteTool;
pub use task_start::TaskStartTool;
pub use tool_search::ToolSearchTool;
pub use web_fetch::WebFetchTool;
pub use web_search::WebSearchTool;
pub use workflow_propose::WorkflowProposeTool;
pub use workflow_read::WorkflowReadTool;
pub use workflow_update::WorkflowUpdateTool;
pub use write::WriteTool;
