use std::path::{Path, PathBuf};

use tracing::warn;

use super::truncate_utf8_bytes;
use crate::storage::ProjectSettings;

const MAX_PROJECT_SETTINGS_APPEND_BYTES: usize = 8 * 1024;

/// Identical bounded repository instructions for parent and delegated runs.
/// Workspace orientation is kept separate: isolated children have a different
/// execution directory, but inherit the selected project's instruction bundle.
pub(super) struct ProjectContext {
    pub instructions: String,
    pub settings: String,
}

impl ProjectContext {
    pub fn load(
        working_dir: &Path,
        project_dir: Option<&Path>,
        settings: &ProjectSettings,
    ) -> Self {
        let instructions = project_dir
            .map(|root| build_project_context_for_workspace(working_dir, root))
            .unwrap_or_default();
        let settings = settings
            .system_prompt_append
            .as_deref()
            .filter(|append| !append.is_empty())
            .map(|append| {
                let header = "[PROJECT SETTINGS]\n";
                let marker = "\n[PROJECT SETTINGS TRUNCATED AT REQUEST BUDGET]";
                if header.len() + append.len() <= MAX_PROJECT_SETTINGS_APPEND_BYTES {
                    format!("{header}{append}")
                } else {
                    format!(
                        "{header}{}{marker}",
                        truncate_utf8_bytes(
                            append,
                            MAX_PROJECT_SETTINGS_APPEND_BYTES - header.len() - marker.len()
                        )
                    )
                }
            })
            .unwrap_or_default();
        Self {
            instructions,
            settings,
        }
    }

    pub fn combined(self) -> String {
        [self.instructions, self.settings]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n")
    }
}

/// The instruction-only bundle can be frozen before child workspace isolation
/// without freezing an obsolete execution directory into the system prompt.
pub fn build_subagent_project_instructions(
    working_dir: &Path,
    project_dir: Option<&Path>,
) -> String {
    let settings = project_dir.map(ProjectSettings::load).unwrap_or_default();
    ProjectContext::load(working_dir, project_dir, &settings).combined()
}

/// Match the documented Codex project-instruction ceiling. The cap applies to
/// the complete root-to-working-directory instruction bundle, including
/// section labels, so nested repositories cannot grow the request without
/// bound.
const DEFAULT_PROJECT_CONTEXT_MAX_BYTES: usize = 32 * 1024;

/// Instruction files to search for in the working directory (priority order).
const PROJECT_FILES: &[&str] = &[
    "KRAB.md",
    "krab.md",
    "AGENTS.md",
    "agents.md",
    "CLAUDE.md",
    "claude.md",
    ".cursorrules",
    ".windsurfrules",
    ".clinerules",
    ".github/copilot-instructions.md",
    "JULES.md",
    "gemini.md",
];

/// Build project context from instruction files in the working directory.
///
/// Searches from the project root down to the working directory and
/// concatenates the closest instruction file from each directory.
pub fn build_project_context(working_dir: &Path) -> String {
    build_project_context_with_limit(working_dir, DEFAULT_PROJECT_CONTEXT_MAX_BYTES)
}

fn build_project_context_with_limit(working_dir: &Path, max_bytes: usize) -> String {
    if max_bytes == 0 {
        return String::new();
    }

    let start = canonical_path(working_dir);
    let root = discover_project_root(&start);
    render_project_context(&start, root, max_bytes)
}

fn build_project_context_for_workspace(working_dir: &Path, project_dir: &Path) -> String {
    let root = canonical_path(project_dir);
    let working_dir = canonical_path(working_dir);
    // Selected project scope owns these instructions. If a caller temporarily
    // executes elsewhere, do not accidentally inherit an unrelated repository.
    let start = if working_dir.starts_with(&root) {
        &working_dir
    } else {
        &root
    };
    render_project_context(start, &root, DEFAULT_PROJECT_CONTEXT_MAX_BYTES)
}

fn render_project_context(working_dir: &Path, project_dir: &Path, max_bytes: usize) -> String {
    let instruction_files = discover_instruction_files(working_dir, project_dir);
    if instruction_files.is_empty() {
        return String::new();
    }

    let mut sections = Vec::new();
    for path in instruction_files {
        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            Err(error) => {
                warn!(path = %path.display(), error = %error, "Failed to read project instruction file");
                continue;
            }
        };
        if content.trim().is_empty() {
            continue;
        }

        let label = path
            .strip_prefix(project_dir)
            .ok()
            .map(|p| p.display().to_string())
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| path.display().to_string());

        sections.push(format!(
            "[PROJECT INSTRUCTIONS - {}]\n\n{}\n\n[END PROJECT INSTRUCTIONS]",
            label, content
        ));
    }

    let context = sections.join("\n\n");
    if context.len() <= max_bytes {
        return context;
    }

    const TRUNCATION_MARKER: &str = "\n\n[PROJECT INSTRUCTIONS TRUNCATED AT REQUEST BUDGET]";
    if max_bytes <= TRUNCATION_MARKER.len() {
        return truncate_utf8_bytes(TRUNCATION_MARKER, max_bytes);
    }

    let mut truncated = truncate_utf8_bytes(&context, max_bytes - TRUNCATION_MARKER.len());
    truncated.push_str(TRUNCATION_MARKER);
    truncated
}

fn canonical_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn discover_instruction_files(working_dir: &Path, root: &Path) -> Vec<PathBuf> {
    let start = working_dir;
    let mut dirs = Vec::new();
    let mut current = start;

    loop {
        dirs.push(current.to_path_buf());
        if current == root {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        current = parent;
    }

    dirs.reverse();

    let mut files = Vec::new();
    for dir in dirs {
        if let Some(path) = PROJECT_FILES
            .iter()
            .map(|name| dir.join(name))
            .find(|path| path.is_file())
        {
            files.push(path);
        }
    }

    files
}

pub(super) fn discover_named_file(base_dir: &Path, candidates: &[&str]) -> Option<PathBuf> {
    candidates
        .iter()
        .map(|name| base_dir.join(name))
        .find(|path| path.is_file())
}

fn discover_project_root(working_dir: &Path) -> &Path {
    for ancestor in working_dir.ancestors() {
        if ancestor.join(".git").exists() {
            return ancestor;
        }
    }
    working_dir
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::build_project_context_with_limit;

    #[test]
    fn aggregate_project_context_respects_byte_budget_and_utf8_boundaries() {
        let temp = TempDir::new().expect("temp dir");
        let repo = temp.path();
        fs::create_dir_all(repo.join(".git")).expect("git dir");
        fs::write(repo.join("AGENTS.md"), "🦀".repeat(20_000)).expect("instructions");

        let context = build_project_context_with_limit(repo, 1_024);

        assert!(context.len() <= 1_024);
        assert!(context.is_char_boundary(context.len()));
        assert!(context.contains("TRUNCATED"));
    }
}
