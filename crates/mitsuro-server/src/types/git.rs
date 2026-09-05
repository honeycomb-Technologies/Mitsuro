use serde::{Deserialize, Serialize};

// ============================================================================
// Git Types
// ============================================================================

#[derive(Deserialize)]
pub struct GitQuery {
    /// Optional path to inspect. If omitted, defaults to current workspace path.
    pub path: Option<String>,
}

#[derive(Serialize)]
pub struct GitStatusResponse {
    pub in_repo: bool,
    pub repo_root: Option<String>,
    pub branch: Option<String>,
    pub head: Option<String>,
    pub upstream: Option<String>,
    pub branch_files: usize,
    pub branch_additions: usize,
    pub branch_deletions: usize,
    pub pr_number: Option<u64>,
    pub ahead: usize,
    pub behind: usize,
    pub staged: usize,
    pub modified: usize,
    pub untracked: usize,
    pub conflicted: usize,
    pub total_changes: usize,
}

#[derive(Serialize)]
pub struct GitChangedFileResponse {
    pub path: String,
    pub status: String,
    pub additions: usize,
    pub deletions: usize,
}

#[derive(Serialize)]
pub struct GitChangesResponse {
    pub in_repo: bool,
    pub repo_root: Option<String>,
    pub files: Vec<GitChangedFileResponse>,
    pub truncated: bool,
}

#[derive(Deserialize)]
pub struct GitDiffQuery {
    /// Optional repository path. If omitted, defaults to the current workspace.
    pub path: Option<String>,
    /// Repository-relative file path selected from `/git/changes`.
    pub file: String,
}

#[derive(Serialize)]
pub struct GitFileDiffResponse {
    pub path: String,
    pub patch: String,
    pub truncated: bool,
    pub binary: bool,
}

#[derive(Serialize)]
pub struct GitBranchResponse {
    pub name: String,
    pub is_current: bool,
    pub upstream: Option<String>,
    pub is_remote: bool,
}

#[derive(Serialize)]
pub struct GitBranchesResponse {
    pub repo_root: String,
    pub branches: Vec<GitBranchResponse>,
}

#[derive(Serialize)]
pub struct GitWorktreeResponse {
    pub path: String,
    pub branch: Option<String>,
    pub head: Option<String>,
    pub is_current: bool,
}

#[derive(Serialize)]
pub struct GitWorktreesResponse {
    pub repo_root: String,
    pub worktrees: Vec<GitWorktreeResponse>,
}

#[derive(Deserialize)]
pub struct GitCheckoutRequest {
    /// Optional path within a repository.
    pub path: Option<String>,
    /// Branch to switch to.
    pub branch: String,
    /// If true, creates a new branch (`git checkout -b`).
    #[serde(default)]
    pub create: bool,
    /// Optional start point used when creating a new branch.
    pub start_point: Option<String>,
}
