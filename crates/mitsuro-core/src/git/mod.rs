//! Lightweight git helpers shared by server and clients.

mod changes;
mod command;
mod model;
mod pr;
mod status;
#[cfg(test)]
mod tests;

pub use changes::{changes, file_diff, project_changes, project_file_diff};
pub use command::{checkout, resolve_repo_root, should_suppress_display};
pub use model::{
    GitBranchSummary, GitChangedFileSummary, GitChangesSummary, GitFileDiff, GitStatusSummary,
    GitWorktreeSummary,
};
pub use status::{branches, status, worktrees};
