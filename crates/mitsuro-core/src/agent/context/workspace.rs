use std::path::Path;
use tracing::warn;

use git2::{Repository, Status, StatusOptions};

use super::project::build_subagent_project_instructions;

/// Build combined workspace + project context for a subagent system prompt.
pub fn build_subagent_project_context(working_dir: &Path, project_dir: Option<&Path>) -> String {
    let workspace = build_workspace_context(working_dir, project_dir);
    let project = build_subagent_project_instructions(working_dir, project_dir);
    [workspace, project]
        .into_iter()
        .filter(|section| !section.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

pub(super) fn build_workspace_context(working_dir: &Path, project_dir: Option<&Path>) -> String {
    let execution_dir = working_dir.display();

    if let Some(project_dir) = project_dir {
        let broad_root_guidance = dirs::home_dir()
            .filter(|home| home == project_dir)
            .map(|_| {
                "\n- This selected directory is the user's broad home directory, not a detected repository\n\
                 - Do not recursively scan it or search generated/cache directories\n\
                 - If the user refers ambiguously to 'this project', do not call any discovery tool: explain that no specific project is selected and ask which folder they mean\n\
                 - Only inspect immediate children with the list tool when the user explicitly asks you to enumerate or locate projects; never list the same directory twice in one turn"
            })
            .unwrap_or_default();
        return format!(
            "[WORKSPACE MODE: PROJECT]\n\n\
             Execution directory: {}\n\
             Project directory: {}\n\
             - Treat the project directory above as the canonical repository root for this session\n\
             - Use the workspace for orientation, not as an assumed filesystem security boundary\n\
             - Prefer absolute paths rooted in that project directory when referring to files\n\
             - Do not invent alternate workspace roots or mirror paths if tools already revealed the real filesystem layout{}",
            execution_dir,
            project_dir.display(),
            broad_root_guidance
        );
    }

    format!(
        "[WORKSPACE MODE: NEUTRAL]\n\n\
     Execution directory: {}\n\
     Project directory: none selected\n\n\
     No project directory is currently selected for this session.\n\
     - Do not assume the user wants help with any repository just because files are accessible\n\
     - Do not describe yourself as working on a named project unless the user explicitly selects or creates one\n\
     - Use the execution directory above when running tools, and treat paths returned by tools as canonical\n\
     - Do not invent alternate workspace roots, mount points, or mirrored directories when tools already returned absolute paths\n\
     - Read-only system inspection, brainstorming, and machine-level tasks are valid in this mode\n\
     - If the user chooses or creates a project directory, call `set_workspace_context` so future turns pivot into project-aware behavior",
        execution_dir
    )
}

fn open_git_repository(working_dir: &Path) -> Option<Repository> {
    match Repository::discover(working_dir) {
        Ok(repo) => Some(repo),
        Err(error) if error.code() == git2::ErrorCode::NotFound => None,
        Err(error) => {
            warn!(
                working_dir = %working_dir.display(),
                error = %error,
                "Failed to open git repository while building environment context"
            );
            None
        }
    }
}

fn current_git_branch(repo: &Repository) -> Option<String> {
    let head = repo.head().ok()?;
    let branch = head.shorthand()?.trim();
    if branch.is_empty() {
        None
    } else {
        Some(branch.to_string())
    }
}

fn summarize_git_statuses(repo: &Repository) -> Option<String> {
    let mut options = StatusOptions::new();
    options.include_untracked(true);

    let statuses = match repo.statuses(Some(&mut options)) {
        Ok(statuses) => statuses,
        Err(error) => {
            warn!(
                error = %error,
                "Failed to inspect git status while building environment context"
            );
            return None;
        }
    };

    let mut modified = 0usize;
    let mut staged = 0usize;
    let mut untracked = 0usize;

    for entry in statuses.iter() {
        let status = entry.status();
        if status.contains(Status::WT_NEW) {
            untracked += 1;
        }
        if status.intersects(
            Status::INDEX_NEW
                | Status::INDEX_MODIFIED
                | Status::INDEX_DELETED
                | Status::INDEX_RENAMED
                | Status::INDEX_TYPECHANGE,
        ) {
            staged += 1;
        }
        if status.intersects(
            Status::WT_MODIFIED | Status::WT_DELETED | Status::WT_RENAMED | Status::WT_TYPECHANGE,
        ) {
            modified += 1;
        }
    }

    format_git_status_summary(modified, staged, untracked)
}

fn format_git_status_summary(modified: usize, staged: usize, untracked: usize) -> Option<String> {
    let mut parts = Vec::new();
    if modified > 0 {
        parts.push(format!("{} modified", modified));
    }
    if staged > 0 {
        parts.push(format!("{} staged", staged));
    }
    if untracked > 0 {
        parts.push(format!("{} untracked", untracked));
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

#[cfg(test)]
pub(super) fn summarize_git_status(status_text: &str) -> Option<String> {
    let mut modified = 0usize;
    let mut staged = 0usize;
    let mut untracked = 0usize;

    for line in status_text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("??") {
            untracked += 1;
        } else if trimmed.starts_with('A') {
            staged += 1;
        } else if trimmed.starts_with('M') || trimmed.contains('M') {
            modified += 1;
        }
    }

    format_git_status_summary(modified, staged, untracked)
}

/// Build environment context with runtime information.
///
/// Gathers working directory, git status, platform, shell, date, and model
/// information. Repository metadata is read through libgit2 so context collection
/// does not spawn repo-configurable Git helper commands.
pub(super) fn build_environment_context(working_dir: &Path, model_id: Option<&str>) -> String {
    let mut lines = vec![
        "[ENVIRONMENT]".to_string(),
        format!("Working directory: {}", working_dir.display()),
    ];

    let repository = open_git_repository(working_dir);
    lines.push(format!(
        "Git repository: {}",
        if repository.is_some() { "yes" } else { "no" }
    ));

    if let Some(repo) = repository {
        if let Some(root) = repo.workdir() {
            lines.push(format!("Git root: {}", root.display()));
        }
        if let Some(branch) = current_git_branch(&repo) {
            lines.push(format!("Git branch: {}", branch));
        }
        if let Some(summary) = summarize_git_statuses(&repo) {
            lines.push(format!("Git status: {}", summary));
        }
    }

    lines.push(format!("Platform: {}", std::env::consts::OS));

    if let Ok(shell) = std::env::var("SHELL") {
        let shell_name = shell.rsplit('/').next().unwrap_or(&shell);
        lines.push(format!("Shell: {}", shell_name));
    }

    let date = chrono::Local::now().format("%Y-%m-%d").to_string();
    lines.push(format!("Date: {}", date));

    if let Some(model) = model_id {
        lines.push(format!("Model: {}", model));
    }

    lines.join("\n")
}

#[cfg(test)]
mod workspace_context_tests {
    use super::build_workspace_context;

    #[test]
    fn selected_home_directory_warns_against_recursive_scans() {
        let Some(home) = dirs::home_dir() else {
            return;
        };
        let context = build_workspace_context(&home, Some(&home));
        assert!(context.contains("broad home directory"));
        assert!(context.contains("Do not recursively scan it"));
        assert!(context.contains("do not call any discovery tool"));
        assert!(context.contains("never list the same directory twice"));
    }
}
