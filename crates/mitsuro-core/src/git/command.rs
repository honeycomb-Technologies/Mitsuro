use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{anyhow, bail, Context, Result};

const MAX_GIT_ERROR_BYTES: usize = 64 * 1024;

pub(super) struct BoundedGitOutput {
    pub(super) stdout: Vec<u8>,
    pub(super) stdout_truncated: bool,
}

/// Returns true if git display should be suppressed for this repo.
/// Suppresses when the repo root is the user's home directory (dotfiles repo).
pub fn should_suppress_display(repo_root: &Path) -> bool {
    dirs::home_dir().is_some_and(|home| repo_root == home)
}

/// Resolve the current worktree root for a path, or `None` if path is not inside a git repo.
pub fn resolve_repo_root(path: &Path) -> Result<Option<PathBuf>> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()
        .with_context(|| format!("Failed to run git in {}", path.display()))?;

    if output.status.success() {
        let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if root.is_empty() {
            return Ok(None);
        }
        return Ok(Some(PathBuf::from(root)));
    }

    let stderr = String::from_utf8_lossy(&output.stderr).to_lowercase();
    if stderr.contains("not a git repository") {
        return Ok(None);
    }

    let detail = command_error_detail(&output.stdout, &output.stderr);
    Err(anyhow!("git rev-parse failed: {}", detail))
}

/// Checkout or create a branch in the repository at `path`.
pub fn checkout(path: &Path, branch: &str, create: bool, start_point: Option<&str>) -> Result<()> {
    let repo_root = resolve_repo_root(path)?
        .ok_or_else(|| anyhow!("Path is not inside a git repository: {}", path.display()))?;

    let branch = branch.trim();
    if branch.is_empty() {
        bail!("Branch name cannot be empty");
    }

    let mut args = vec!["checkout"];
    if create {
        args.push("-b");
        args.push(branch);
        if let Some(start_point) = start_point.map(str::trim).filter(|s| !s.is_empty()) {
            args.push(start_point);
        }
    } else {
        args.push(branch);
    }

    let _ = run_git(&args, &repo_root)?;
    Ok(())
}

pub(super) fn run_git(args: &[&str], cwd: &Path) -> Result<std::process::Output> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| {
            format!(
                "Failed to execute git {} in {}",
                args.join(" "),
                cwd.display()
            )
        })?;

    if output.status.success() {
        Ok(output)
    } else {
        let detail = command_error_detail(&output.stdout, &output.stderr);
        Err(anyhow!("git {} failed: {}", args.join(" "), detail))
    }
}

pub(super) fn run_git_bounded(
    args: &[&str],
    cwd: &Path,
    max_stdout_bytes: usize,
) -> Result<BoundedGitOutput> {
    let mut child = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to execute git {} in {}",
                args.join(" "),
                cwd.display()
            )
        })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("git stdout pipe was unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("git stderr pipe was unavailable"))?;
    let stderr_reader = std::thread::spawn(move || read_bounded(stderr, MAX_GIT_ERROR_BYTES));
    let stdout_result = read_bounded(stdout, max_stdout_bytes);
    let status_result = child.wait();
    let stderr_result = stderr_reader
        .join()
        .map_err(|_| anyhow!("git stderr reader panicked"))?;

    let status = status_result.context("waiting for bounded git command")?;
    let (stdout, stdout_truncated) = stdout_result.context("reading bounded git stdout")?;
    let (stderr, stderr_truncated) = stderr_result.context("reading bounded git stderr")?;
    if !status.success() {
        let mut detail = command_error_detail(&stdout, &stderr);
        if stdout_truncated || stderr_truncated {
            detail.push_str(" (command output truncated)");
        }
        return Err(anyhow!("git {} failed: {}", args.join(" "), detail));
    }

    Ok(BoundedGitOutput {
        stdout,
        stdout_truncated,
    })
}

fn read_bounded(mut reader: impl std::io::Read, limit: usize) -> std::io::Result<(Vec<u8>, bool)> {
    let mut captured = Vec::with_capacity(limit.min(8 * 1024));
    let mut truncated = false;
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        let count = reader.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(captured.len());
        let keep = count.min(remaining);
        captured.extend_from_slice(&chunk[..keep]);
        truncated |= keep < count;
    }
    Ok((captured, truncated))
}

pub(super) fn command_error_detail(stdout: &[u8], stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }
    let stdout = String::from_utf8_lossy(stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }
    "unknown git error".to_string()
}

pub(super) fn short_sha(sha: &str) -> String {
    sha.chars().take(8).collect()
}

pub(super) fn ref_exists(repo_root: &Path, reference: &str) -> bool {
    Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", reference])
        .current_dir(repo_root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::read_bounded;

    #[test]
    fn bounded_reader_caps_capture_and_reports_drained_tail() {
        let input = vec![b'x'; 32 * 1024];
        let (captured, truncated) = read_bounded(Cursor::new(input), 1_024).expect("bounded read");
        assert_eq!(captured.len(), 1_024);
        assert!(truncated);

        let (captured, truncated) =
            read_bounded(Cursor::new(b"complete"), 1_024).expect("complete read");
        assert_eq!(captured, b"complete");
        assert!(!truncated);
    }
}
