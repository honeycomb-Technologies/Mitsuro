use std::collections::{HashMap, HashSet};
use std::io::Read as _;
use std::path::{Component, Path};

use anyhow::{anyhow, bail, Result};
use cap_fs_ext::{ambient_authority, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};

use super::command::{resolve_repo_root, run_git, run_git_bounded, should_suppress_display};
use super::model::{GitChangedFileSummary, GitChangesSummary, GitFileDiff};
use super::status::diff::resolve_base_ref;

const MAX_PATCH_BYTES: usize = 240_000;
const UTF8_BOUNDARY_LOOKAHEAD: usize = 3;
const UNTRACKED_INSPECTION_BYTES: usize = MAX_PATCH_BYTES + UTF8_BOUNDARY_LOOKAHEAD;
const MAX_CHANGE_METADATA_BYTES: usize = 4 * 1024 * 1024;
const MAX_CHANGED_FILES: usize = 2_000;

/// List files changed from the current branch base through the working tree.
pub fn changes(path: &Path) -> Result<Option<GitChangesSummary>> {
    changes_scoped(path, false)
}

/// List changes only inside the supplied project, with project-relative paths.
pub fn project_changes(path: &Path) -> Result<Option<GitChangesSummary>> {
    changes_scoped(path, true)
}

fn changes_scoped(path: &Path, project_scoped: bool) -> Result<Option<GitChangesSummary>> {
    let repo_root = match resolve_repo_root(path)? {
        Some(root) => root,
        None => return Ok(None),
    };
    if should_suppress_display(&repo_root) {
        return Ok(None);
    }
    let repo_root = if project_scoped {
        std::fs::canonicalize(path)?
    } else {
        repo_root
    };

    let base = diff_base(&repo_root)?;
    let base_arg = base.as_deref().unwrap_or("HEAD");
    let name_output = run_git_bounded(
        &[
            "diff",
            "--no-ext-diff",
            "--no-renames",
            "--relative",
            "--name-status",
            "-z",
            base_arg,
            "--",
            ".",
        ],
        &repo_root,
        MAX_CHANGE_METADATA_BYTES,
    )?;
    let numstat_output = run_git_bounded(
        &[
            "diff",
            "--no-ext-diff",
            "--no-renames",
            "--relative",
            "--numstat",
            "-z",
            base_arg,
            "--",
            ".",
        ],
        &repo_root,
        MAX_CHANGE_METADATA_BYTES,
    )?;

    let counts = parse_numstat(
        &numstat_output.stdout,
        numstat_output.stdout_truncated,
        MAX_CHANGED_FILES,
    );
    let names = parse_name_status(
        &name_output.stdout,
        name_output.stdout_truncated,
        MAX_CHANGED_FILES,
    );
    let mut truncated = counts.truncated || names.truncated;
    let mut files = names
        .value
        .into_iter()
        .map(|(path, status)| {
            let (additions, deletions) = counts.value.get(&path).copied().unwrap_or_default();
            GitChangedFileSummary {
                path,
                status,
                additions,
                deletions,
            }
        })
        .collect::<Vec<_>>();
    let mut seen = files
        .iter()
        .map(|file| file.path.clone())
        .collect::<HashSet<_>>();

    let untracked_output = run_git_bounded(
        &["ls-files", "--others", "--exclude-standard", "-z"],
        &repo_root,
        MAX_CHANGE_METADATA_BYTES,
    )?;
    let untracked = parse_nul_paths(
        &untracked_output.stdout,
        untracked_output.stdout_truncated,
        MAX_CHANGED_FILES.saturating_sub(files.len()),
    );
    truncated |= untracked.truncated;
    for path in untracked.value {
        if !seen.insert(path.clone()) {
            continue;
        }
        let additions = count_untracked_lines(&repo_root, &path).unwrap_or_default();
        files.push(GitChangedFileSummary {
            path,
            status: "untracked".to_string(),
            additions,
            deletions: 0,
        });
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Some(GitChangesSummary {
        repo_root,
        files,
        truncated,
    }))
}

/// Load a bounded unified patch for one changed file.
pub fn file_diff(path: &Path, file: &str) -> Result<Option<GitFileDiff>> {
    file_diff_scoped(path, file, false)
}

/// Load a patch confined to the supplied project, with project-relative paths.
pub fn project_file_diff(path: &Path, file: &str) -> Result<Option<GitFileDiff>> {
    file_diff_scoped(path, file, true)
}

fn file_diff_scoped(path: &Path, file: &str, project_scoped: bool) -> Result<Option<GitFileDiff>> {
    validate_relative_file(file)?;
    let repo_root = match resolve_repo_root(path)? {
        Some(root) => root,
        None => return Ok(None),
    };
    if should_suppress_display(&repo_root) {
        return Ok(None);
    }
    let repo_root = if project_scoped {
        std::fs::canonicalize(path)?
    } else {
        repo_root
    };

    let changed = changes_scoped(&repo_root, project_scoped)?
        .ok_or_else(|| anyhow!("Path is not inside a displayable git repository"))?;
    let entry = changed
        .files
        .iter()
        .find(|entry| entry.path == file)
        .ok_or_else(|| anyhow!("File is not part of the current repository changes"))?;

    if entry.status == "untracked" {
        return Ok(Some(untracked_diff(&repo_root, file)?));
    }

    let base = diff_base(&repo_root)?;
    let base_arg = base.as_deref().unwrap_or("HEAD");
    let output = run_git_bounded(
        &[
            "diff",
            "--no-ext-diff",
            "--no-renames",
            "--relative",
            "--unified=3",
            base_arg,
            "--",
            file,
        ],
        &repo_root,
        UNTRACKED_INSPECTION_BYTES,
    )?;
    let binary = output
        .stdout
        .windows(b"Binary files".len())
        .any(|window| window == b"Binary files");
    let (patch, patch_truncated) = bounded_utf8(&output.stdout);
    Ok(Some(GitFileDiff {
        path: file.to_string(),
        patch,
        truncated: output.stdout_truncated || patch_truncated,
        binary,
    }))
}

fn diff_base(repo_root: &Path) -> Result<Option<String>> {
    let upstream = run_git(
        &[
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{upstream}",
        ],
        repo_root,
    )
    .ok()
    .and_then(|output| {
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        (!value.is_empty()).then_some(value)
    });
    let Some(base_ref) = resolve_base_ref(repo_root, upstream.as_deref()) else {
        return Ok(None);
    };
    let output = run_git(&["merge-base", "HEAD", base_ref.as_str()], repo_root)?;
    let merge_base = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok((!merge_base.is_empty()).then_some(merge_base))
}

fn validate_relative_file(file: &str) -> Result<()> {
    let path = Path::new(file);
    if file.trim().is_empty() || path.is_absolute() {
        bail!("File path must be a non-empty repository-relative path");
    }
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        bail!("File path must stay inside the repository");
    }
    Ok(())
}

struct BoundedParse<T> {
    value: T,
    truncated: bool,
}

fn parse_name_status(
    bytes: &[u8],
    input_truncated: bool,
    max_files: usize,
) -> BoundedParse<Vec<(String, String)>> {
    let mut fields = complete_nul_prefix(bytes, input_truncated)
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty());
    let mut parsed = Vec::with_capacity(max_files.min(128));
    let mut incomplete_record = false;
    while parsed.len() < max_files {
        let Some(status) = fields.next() else {
            break;
        };
        let Some(path) = fields.next() else {
            incomplete_record = true;
            break;
        };
        let status = match status.first().copied().unwrap_or(b'M') {
            b'A' => "added",
            b'D' => "deleted",
            b'U' => "conflicted",
            b'T' => "type changed",
            _ => "modified",
        };
        parsed.push((
            String::from_utf8_lossy(path).into_owned(),
            status.to_owned(),
        ));
    }
    BoundedParse {
        value: parsed,
        truncated: input_truncated || incomplete_record || fields.next().is_some(),
    }
}

fn parse_numstat(
    bytes: &[u8],
    input_truncated: bool,
    max_files: usize,
) -> BoundedParse<HashMap<String, (usize, usize)>> {
    let mut fields = complete_nul_prefix(bytes, input_truncated)
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty());
    let mut parsed = HashMap::with_capacity(max_files.min(128));
    let mut records = 0;
    while records < max_files {
        let Some(field) = fields.next() else {
            break;
        };
        records += 1;
        let field = String::from_utf8_lossy(field);
        let mut parts = field.splitn(3, '\t');
        let Some(additions) = parts.next() else {
            continue;
        };
        let Some(deletions) = parts.next() else {
            continue;
        };
        let Some(path) = parts.next() else {
            continue;
        };
        parsed.insert(
            path.to_owned(),
            (
                additions.parse().unwrap_or(0),
                deletions.parse().unwrap_or(0),
            ),
        );
    }
    BoundedParse {
        value: parsed,
        truncated: input_truncated || fields.next().is_some(),
    }
}

fn parse_nul_paths(
    bytes: &[u8],
    input_truncated: bool,
    max_files: usize,
) -> BoundedParse<Vec<String>> {
    let mut fields = complete_nul_prefix(bytes, input_truncated)
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty());
    let value = fields
        .by_ref()
        .take(max_files)
        .map(|field| String::from_utf8_lossy(field).into_owned())
        .collect();
    BoundedParse {
        value,
        truncated: input_truncated || fields.next().is_some(),
    }
}

fn complete_nul_prefix(bytes: &[u8], input_truncated: bool) -> &[u8] {
    if !input_truncated || bytes.last() == Some(&0) {
        return bytes;
    }
    bytes
        .iter()
        .rposition(|byte| *byte == 0)
        .map_or(&[], |last_separator| &bytes[..=last_separator])
}

fn count_untracked_lines(repo_root: &Path, file: &str) -> Option<usize> {
    match read_untracked(repo_root, file).ok()? {
        UntrackedContent::Text { bytes, .. } => {
            let text = std::str::from_utf8(&bytes).ok()?;
            Some(text.lines().count())
        }
        UntrackedContent::Symlink(target) => Some(target.lines().count().max(1)),
        UntrackedContent::Binary | UntrackedContent::Special => None,
    }
}

fn untracked_diff(repo_root: &Path, file: &str) -> Result<GitFileDiff> {
    let (bytes, mode, source_truncated) = match read_untracked(repo_root, file)? {
        UntrackedContent::Text { bytes, truncated } => (bytes, "100644", truncated),
        UntrackedContent::Symlink(target) => (target.into_bytes(), "120000", false),
        UntrackedContent::Binary => {
            return Ok(GitFileDiff {
                path: file.to_string(),
                patch: String::new(),
                truncated: false,
                binary: true,
            });
        }
        UntrackedContent::Special => {
            bail!("Untracked path is not a regular file or symbolic link")
        }
    };
    let text = String::from_utf8_lossy(&bytes);
    let mut patch = format!(
        "diff --git a/{file} b/{file}\nnew file mode {mode}\n--- /dev/null\n+++ b/{file}\n@@ -0,0 +1,{} @@\n",
        text.lines().count().max(1)
    );
    let mut wrote_line = false;
    for line in text.lines() {
        wrote_line = true;
        patch.push('+');
        patch.push_str(line);
        patch.push('\n');
    }
    if !wrote_line {
        patch.push_str("+\n");
    }
    let (patch, patch_truncated) = bounded_utf8(patch.as_bytes());
    Ok(GitFileDiff {
        path: file.to_string(),
        patch,
        truncated: source_truncated || patch_truncated,
        binary: false,
    })
}

#[derive(Debug)]
enum UntrackedContent {
    Text { bytes: Vec<u8>, truncated: bool },
    Binary,
    Symlink(String),
    Special,
}

fn read_untracked(repo_root: &Path, file: &str) -> Result<UntrackedContent> {
    validate_relative_file(file)?;
    let mut directory = open_capability_root(repo_root)?;
    let relative = Path::new(file);
    let components = relative.components().collect::<Vec<_>>();
    let (file_name, parents) = components
        .split_last()
        .ok_or_else(|| anyhow!("File path must be a non-empty repository-relative path"))?;
    for component in parents {
        let Component::Normal(name) = component else {
            bail!("File path must stay inside the repository");
        };
        directory = open_child_directory(&directory, Path::new(name))?;
    }
    let Component::Normal(file_name) = file_name else {
        bail!("File path must stay inside the repository");
    };
    let file_name = Path::new(file_name);
    let metadata = directory.symlink_metadata(file_name)?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        let target = directory.read_link_contents(file_name)?;
        return Ok(UntrackedContent::Symlink(
            target.to_string_lossy().into_owned(),
        ));
    }
    if !file_type.is_file() {
        return Ok(UntrackedContent::Special);
    }

    let mut opened = directory.open_with(file_name, &nofollow_read_options())?;
    if !opened.metadata()?.is_file() {
        return Ok(UntrackedContent::Special);
    }
    let read_limit = UNTRACKED_INSPECTION_BYTES + 1;
    let mut bytes = Vec::with_capacity(read_limit.min(metadata.len() as usize));
    opened
        .by_ref()
        .take(read_limit as u64)
        .read_to_end(&mut bytes)?;

    // Classification is intentionally prefix-based. One additional byte only
    // detects truncation; it and later bytes are not classified. Such content
    // remains text when the inspected prefix is valid and is reported truncated.
    let has_more = bytes.len() > UNTRACKED_INSPECTION_BYTES;
    if has_more {
        bytes.truncate(UNTRACKED_INSPECTION_BYTES);
    }
    if bytes.contains(&0) {
        return Ok(UntrackedContent::Binary);
    }
    let inspected_len = bytes.len();
    let valid_len = match std::str::from_utf8(&bytes) {
        Ok(_) => inspected_len,
        Err(error) if error.error_len().is_none() && has_more => error.valid_up_to(),
        Err(_) => return Ok(UntrackedContent::Binary),
    };
    bytes.truncate(valid_len);

    let text = std::str::from_utf8(&bytes).expect("validated UTF-8 prefix");
    let mut output_end = bytes.len().min(MAX_PATCH_BYTES);
    while output_end > 0 && !text.is_char_boundary(output_end) {
        output_end -= 1;
    }
    let truncated = has_more || valid_len < inspected_len || output_end < bytes.len();
    bytes.truncate(output_end);
    Ok(UntrackedContent::Text { bytes, truncated })
}

fn open_capability_root(root: &Path) -> Result<Dir> {
    if !root.is_absolute() {
        bail!("Repository root must be absolute");
    }
    let mut directory = Dir::open_ambient_dir(Path::new("/"), ambient_authority())?;
    let mut saw_component = false;
    for component in root.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                saw_component = true;
                directory = open_child_directory(&directory, Path::new(name))?;
            }
            _ => bail!("Repository root must be an absolute canonical path"),
        }
    }
    if !saw_component {
        bail!("Repository root cannot be the filesystem root");
    }
    Ok(directory)
}

fn open_child_directory(parent: &Dir, name: &Path) -> Result<Dir> {
    let metadata = parent.symlink_metadata(name)?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() || !file_type.is_dir() {
        bail!("Repository path contains a symbolic link or non-directory component");
    }
    let opened = parent.open_with(name, &nofollow_read_options())?;
    if !opened.metadata()?.is_dir() {
        bail!("Repository path contains a symbolic link or non-directory component");
    }
    Ok(Dir::from_std_file(opened.into_std()))
}

fn nofollow_read_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NONBLOCK);
    }
    options
}

fn bounded_utf8(bytes: &[u8]) -> (String, bool) {
    if bytes.len() <= MAX_PATCH_BYTES {
        return (String::from_utf8_lossy(bytes).into_owned(), false);
    }
    let mut end = MAX_PATCH_BYTES;
    while end > 0 && std::str::from_utf8(&bytes[..end]).is_err() {
        end -= 1;
    }
    let mut patch = String::from_utf8_lossy(&bytes[..end]).into_owned();
    patch.push_str("\n... diff truncated ...\n");
    (patch, true)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Command;

    use super::{
        changes, file_diff, parse_name_status, parse_numstat, read_untracked,
        validate_relative_file, UntrackedContent, MAX_CHANGED_FILES, MAX_PATCH_BYTES,
        UNTRACKED_INSPECTION_BYTES,
    };

    fn run_git(repo: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(repo)
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    }

    fn initialized_repo() -> tempfile::TempDir {
        let temp = tempfile::tempdir().expect("temp repo");
        run_git(temp.path(), &["init", "-b", "main"]);
        run_git(temp.path(), &["config", "user.name", "Mitsuro Test"]);
        run_git(
            temp.path(),
            &["config", "user.email", "mitsuro-test@example.invalid"],
        );
        std::fs::write(temp.path().join("README.md"), "base\n").expect("base file");
        run_git(temp.path(), &["add", "README.md"]);
        run_git(temp.path(), &["commit", "-m", "base"]);
        temp
    }

    #[test]
    fn parses_nul_delimited_change_metadata() {
        let names = parse_name_status(b"M\0src/main.rs\0A\0new file.ts\0", false, 10);
        assert_eq!(
            names.value,
            vec![
                ("src/main.rs".to_string(), "modified".to_string()),
                ("new file.ts".to_string(), "added".to_string()),
            ]
        );
        assert!(!names.truncated);
        let counts = parse_numstat(b"4\t2\tsrc/main.rs\0-\t-\tasset.png\0", false, 10);
        assert_eq!(counts.value.get("src/main.rs"), Some(&(4, 2)));
        assert_eq!(counts.value.get("asset.png"), Some(&(0, 0)));
        assert!(!counts.truncated);
    }

    #[test]
    fn change_metadata_drops_partial_records_and_caps_parsed_files() {
        let partial = parse_name_status(b"M\0complete.rs\0A\0partial", true, 10);
        assert_eq!(
            partial.value,
            vec![("complete.rs".to_owned(), "modified".to_owned())]
        );
        assert!(partial.truncated);

        let mut many = Vec::new();
        for index in 0..=MAX_CHANGED_FILES {
            many.extend_from_slice(b"M\0");
            many.extend_from_slice(format!("file-{index}.rs").as_bytes());
            many.push(0);
        }
        let parsed = parse_name_status(&many, false, MAX_CHANGED_FILES);
        assert_eq!(parsed.value.len(), MAX_CHANGED_FILES);
        assert!(parsed.truncated);
    }

    #[test]
    fn rejects_paths_that_escape_the_repository() {
        assert!(validate_relative_file("../secret").is_err());
        assert!(validate_relative_file("/tmp/secret").is_err());
        assert!(validate_relative_file("src/main.rs").is_ok());
    }

    #[test]
    fn ordinary_untracked_text_keeps_counts_and_a_bounded_patch() {
        let repo = initialized_repo();
        std::fs::write(repo.path().join("notes.txt"), "alpha\nbeta\n").expect("notes");

        let summary = changes(repo.path()).expect("changes").expect("repository");
        assert!(!summary.truncated);
        let entry = summary
            .files
            .iter()
            .find(|entry| entry.path == "notes.txt")
            .expect("untracked entry");
        assert_eq!(entry.additions, 2);
        let diff = file_diff(repo.path(), "notes.txt")
            .expect("diff")
            .expect("repository");
        assert!(!diff.binary);
        assert!(!diff.truncated);
        assert!(diff.patch.contains("new file mode 100644"));
        assert!(diff.patch.contains("+alpha\n+beta\n"));
    }

    #[test]
    fn large_untracked_repository_is_capped_and_reports_truncation() {
        let repo = initialized_repo();
        for index in 0..=MAX_CHANGED_FILES {
            std::fs::write(repo.path().join(format!("untracked-{index:04}.txt")), "")
                .expect("untracked file");
        }

        let summary = changes(repo.path()).expect("changes").expect("repository");
        assert_eq!(summary.files.len(), MAX_CHANGED_FILES);
        assert!(summary.truncated);
        assert!(summary.files.iter().all(|file| file.status == "untracked"));
    }

    #[test]
    fn tracked_diff_capture_is_bounded_and_reports_truncation() {
        let repo = initialized_repo();
        let mut content = vec![b'x'; MAX_PATCH_BYTES + 32 * 1024];
        content.push(b'\n');
        std::fs::write(repo.path().join("README.md"), content).expect("large tracked file");

        let diff = file_diff(repo.path(), "README.md")
            .expect("tracked diff")
            .expect("repository");
        assert!(diff.truncated);
        assert!(diff.patch.len() <= MAX_PATCH_BYTES + 32);
        assert!(diff.patch.contains("diff --git"));
    }

    #[test]
    fn valid_multibyte_utf8_crossing_patch_boundary_remains_text_and_truncated() {
        let repo = initialized_repo();
        let mut content = vec![b'a'; MAX_PATCH_BYTES - 1];
        content.extend_from_slice("💜".as_bytes());
        content.extend_from_slice(b"tail");
        std::fs::write(repo.path().join("boundary.txt"), content).expect("boundary file");

        match read_untracked(repo.path(), "boundary.txt").expect("classify boundary file") {
            UntrackedContent::Text { bytes, truncated } => {
                assert!(truncated);
                assert!(bytes.len() <= MAX_PATCH_BYTES);
                assert!(std::str::from_utf8(&bytes).is_ok());
                assert_eq!(bytes.len(), MAX_PATCH_BYTES - 1);
            }
            other => panic!("expected bounded text, got {other:?}"),
        }

        let diff = file_diff(repo.path(), "boundary.txt")
            .expect("boundary diff")
            .expect("repository");
        assert!(!diff.binary);
        assert!(diff.truncated);
    }

    #[test]
    fn nul_and_invalid_utf8_inside_inspected_prefix_are_binary() {
        let repo = initialized_repo();
        let mut nul_content = vec![b'a'; MAX_PATCH_BYTES];
        nul_content.push(0);
        std::fs::write(repo.path().join("nul.bin"), nul_content).expect("NUL file");

        let mut invalid_content = vec![b'a'; MAX_PATCH_BYTES + 1];
        invalid_content.push(0xff);
        std::fs::write(repo.path().join("invalid.bin"), invalid_content)
            .expect("invalid UTF-8 file");

        for file in ["nul.bin", "invalid.bin"] {
            assert!(matches!(
                read_untracked(repo.path(), file).expect("classify binary file"),
                UntrackedContent::Binary
            ));
            let diff = file_diff(repo.path(), file)
                .expect("binary diff")
                .expect("repository");
            assert!(diff.binary);
            assert!(!diff.truncated);
        }
    }

    #[test]
    fn invalid_utf8_beyond_bounded_inspection_is_unobserved_truncated_text() {
        let repo = initialized_repo();
        let mut content = vec![b'a'; UNTRACKED_INSPECTION_BYTES + 1];
        content.push(0xff);
        std::fs::write(repo.path().join("beyond-prefix.txt"), content).expect("beyond-prefix file");

        match read_untracked(repo.path(), "beyond-prefix.txt").expect("classify bounded prefix") {
            UntrackedContent::Text { bytes, truncated } => {
                assert!(truncated);
                assert_eq!(bytes.len(), MAX_PATCH_BYTES);
                assert!(std::str::from_utf8(&bytes).is_ok());
            }
            other => panic!("expected prefix-classified text, got {other:?}"),
        }

        let diff = file_diff(repo.path(), "beyond-prefix.txt")
            .expect("bounded-prefix diff")
            .expect("repository");
        assert!(!diff.binary);
        assert!(diff.truncated);
    }

    #[cfg(unix)]
    #[test]
    fn untracked_symlink_diff_contains_only_link_text_never_target_bytes() {
        let repo = initialized_repo();
        let outside = tempfile::tempdir().expect("outside");
        let secret = outside.path().join("secret.txt");
        std::fs::write(&secret, "DO-NOT-DISCLOSE").expect("secret");
        std::os::unix::fs::symlink(&secret, repo.path().join("external-link"))
            .expect("external symlink");
        std::os::unix::fs::symlink("missing-target", repo.path().join("broken-link"))
            .expect("broken symlink");

        let summary = changes(repo.path()).expect("changes").expect("repository");
        assert_eq!(
            summary
                .files
                .iter()
                .find(|entry| entry.path == "external-link")
                .map(|entry| entry.additions),
            Some(1)
        );
        for (file, expected_target) in [
            ("external-link", secret.to_string_lossy().into_owned()),
            ("broken-link", "missing-target".to_string()),
        ] {
            let diff = file_diff(repo.path(), file)
                .expect("symlink diff")
                .expect("repository");
            assert!(!diff.binary);
            assert!(diff.patch.contains("new file mode 120000"));
            assert!(diff.patch.contains(&format!("+{expected_target}")));
            assert!(!diff.patch.contains("DO-NOT-DISCLOSE"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn untracked_special_file_is_listed_without_opening_and_diff_fails_closed() {
        use std::os::unix::net::UnixListener;

        let repo = initialized_repo();
        let _listener = UnixListener::bind(repo.path().join("special.sock")).expect("socket");
        assert!(matches!(
            read_untracked(repo.path(), "special.sock").expect("classify special file"),
            UntrackedContent::Special
        ));
        assert!(file_diff(repo.path(), "special.sock").is_err());
    }
}
