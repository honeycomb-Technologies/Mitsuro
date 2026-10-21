//! Deterministic, diagnostics-first skill filesystem loading.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use tracing::debug;

use super::skill::{validate_skill_name, Skill, SkillSource};

const MAX_DISCOVERY_DEPTH: usize = 8;
const MAX_SKILLS_PER_ROOT: usize = 2_048;
const MAX_DISCOVERY_ENTRIES_PER_ROOT: usize = 50_000;
const MAX_DEFINITION_BYTES_PER_ROOT: u64 = 32 * 1024 * 1024;
pub(crate) const MAX_SKILL_DEFINITION_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_SKILL_RESOURCE_BYTES: usize = 4 * 1024 * 1024;
const MAX_SKILL_RESOURCE_PATH_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillDiagnostic {
    pub severity: SkillDiagnosticSeverity,
    pub code: String,
    pub message: String,
    pub path: PathBuf,
    pub skill_name: Option<String>,
}

impl SkillDiagnostic {
    pub(crate) fn warning(
        code: impl Into<String>,
        message: impl Into<String>,
        path: PathBuf,
        skill_name: Option<String>,
    ) -> Self {
        Self {
            severity: SkillDiagnosticSeverity::Warning,
            code: code.into(),
            message: message.into(),
            path,
            skill_name,
        }
    }

    pub(crate) fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        path: PathBuf,
        skill_name: Option<String>,
    ) -> Self {
        Self {
            severity: SkillDiagnosticSeverity::Error,
            code: code.into(),
            message: message.into(),
            path,
            skill_name,
        }
    }
}

#[derive(Debug, Default)]
pub struct SkillLoadReport {
    pub skills: Vec<Skill>,
    pub diagnostics: Vec<SkillDiagnostic>,
}

/// Root-specific discovery behavior.
#[derive(Debug, Clone, Copy, Default)]
pub struct SkillLoadOptions {
    /// Find nested directories containing `SKILL.md`, as Pi/package roots do.
    pub recursive: bool,
    /// Accept direct `<name>.md` files at the root for Pi compatibility.
    pub direct_markdown: bool,
}

/// Load all valid skills from a directory, preserving the legacy API.
pub fn load_skills_from_dir(dir: &Path, source: SkillSource) -> Vec<Skill> {
    load_skills_from_root(dir, source, source.as_str(), SkillLoadOptions::default()).skills
}

/// Load skills and return actionable diagnostics for every rejected candidate.
pub fn load_skills_from_root(
    dir: &Path,
    source: SkillSource,
    origin: &str,
    options: SkillLoadOptions,
) -> SkillLoadReport {
    let mut report = SkillLoadReport::default();
    if dir.is_file() {
        let result = if dir.file_name().and_then(|name| name.to_str()) == Some("SKILL.md") {
            dir.parent()
                .ok_or_else(|| anyhow!("SKILL.md has no parent directory"))
                .and_then(|parent| load_skill_with_origin(parent, source, origin))
        } else if dir.extension().and_then(|extension| extension.to_str()) == Some("md") {
            load_flat_skill(dir, source, origin)
        } else {
            Err(anyhow!(
                "Explicit skill path must be SKILL.md, a Markdown skill, or a directory"
            ))
        };
        match result {
            Ok(skill) => report.skills.push(skill),
            Err(error) => report.diagnostics.push(SkillDiagnostic::error(
                "invalid_skill",
                error.to_string(),
                dir.to_path_buf(),
                None,
            )),
        }
        return report;
    }
    if !dir.is_dir() {
        return report;
    }

    let mut candidates = Vec::new();
    let mut visited_entries = 0usize;
    let mut entry_limit_reported = false;
    discover_candidates(
        dir,
        0,
        options,
        &mut candidates,
        &mut report.diagnostics,
        &mut visited_entries,
        &mut entry_limit_reported,
    );
    candidates.sort();
    candidates.dedup();

    let mut definition_bytes = 0u64;
    for definition in candidates.into_iter().take(MAX_SKILLS_PER_ROOT) {
        let candidate_bytes = fs::metadata(&definition)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if definition_bytes.saturating_add(candidate_bytes) > MAX_DEFINITION_BYTES_PER_ROOT {
            report.diagnostics.push(SkillDiagnostic::warning(
                "root_definition_bytes_limit",
                format!(
                    "Skill root definitions exceed the aggregate safety limit of {MAX_DEFINITION_BYTES_PER_ROOT} bytes"
                ),
                dir.to_path_buf(),
                None,
            ));
            break;
        }
        definition_bytes = definition_bytes.saturating_add(candidate_bytes);

        let result = if definition.file_name().and_then(|name| name.to_str()) == Some("SKILL.md") {
            let skill_dir = definition
                .parent()
                .expect("SKILL.md candidate always has a parent");
            load_skill_with_origin(skill_dir, source, origin)
        } else {
            load_flat_skill(&definition, source, origin)
        };

        match result {
            Ok(skill) => {
                debug!(skill = %skill.name, path = ?definition, origin, "loaded skill");
                report.skills.push(skill);
            }
            Err(error) => report.diagnostics.push(SkillDiagnostic::error(
                "invalid_skill",
                error.to_string(),
                definition,
                None,
            )),
        }
    }
    if report.skills.len() == MAX_SKILLS_PER_ROOT {
        report.diagnostics.push(SkillDiagnostic::warning(
            "root_skill_limit",
            format!("Skill root exceeded the safety limit of {MAX_SKILLS_PER_ROOT} definitions"),
            dir.to_path_buf(),
            None,
        ));
    }

    report.skills.sort_by(|a, b| a.name.cmp(&b.name));
    report
}

fn discover_candidates(
    current: &Path,
    depth: usize,
    options: SkillLoadOptions,
    candidates: &mut Vec<PathBuf>,
    diagnostics: &mut Vec<SkillDiagnostic>,
    visited_entries: &mut usize,
    entry_limit_reported: &mut bool,
) {
    if candidates.len() >= MAX_SKILLS_PER_ROOT {
        return;
    }
    if depth > MAX_DISCOVERY_DEPTH {
        diagnostics.push(SkillDiagnostic::warning(
            "discovery_depth_limit",
            format!("Nested skill discovery stops after {MAX_DISCOVERY_DEPTH} levels"),
            current.to_path_buf(),
            None,
        ));
        return;
    }

    let skill_file = current.join("SKILL.md");
    if skill_file.is_file() {
        candidates.push(skill_file);
        // Treat a skill directory as a package boundary. References/assets may
        // contain arbitrary nested folders that should not become extra skills.
        return;
    }

    if *visited_entries >= MAX_DISCOVERY_ENTRIES_PER_ROOT {
        if !*entry_limit_reported {
            diagnostics.push(SkillDiagnostic::warning(
                "discovery_entry_limit",
                format!(
                    "Skill discovery stopped after {MAX_DISCOVERY_ENTRIES_PER_ROOT} filesystem entries"
                ),
                current.to_path_buf(),
                None,
            ));
            *entry_limit_reported = true;
        }
        return;
    }

    let entries = match fs::read_dir(current) {
        Ok(entries) => entries,
        Err(error) => {
            diagnostics.push(SkillDiagnostic::error(
                "root_unreadable",
                format!("Failed to read skill directory: {error}"),
                current.to_path_buf(),
                None,
            ));
            return;
        }
    };
    let mut bounded_entries = Vec::new();
    for entry in entries.flatten() {
        if *visited_entries >= MAX_DISCOVERY_ENTRIES_PER_ROOT {
            if !*entry_limit_reported {
                diagnostics.push(SkillDiagnostic::warning(
                    "discovery_entry_limit",
                    format!(
                        "Skill discovery stopped after {MAX_DISCOVERY_ENTRIES_PER_ROOT} filesystem entries"
                    ),
                    current.to_path_buf(),
                    None,
                ));
                *entry_limit_reported = true;
            }
            break;
        }
        *visited_entries += 1;
        bounded_entries.push(entry);
    }
    let mut entries = bounded_entries;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        if candidates.len() >= MAX_SKILLS_PER_ROOT {
            break;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        // Never follow symlinks during discovery. This prevents loops and keeps
        // registered roots honest about their filesystem boundary.
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            if depth == 0 || options.recursive {
                discover_candidates(
                    &entry.path(),
                    depth + 1,
                    options,
                    candidates,
                    diagnostics,
                    visited_entries,
                    entry_limit_reported,
                );
            }
        } else if depth == 0
            && options.direct_markdown
            && entry.path().extension().and_then(|value| value.to_str()) == Some("md")
        {
            candidates.push(entry.path());
        }
    }
}

/// Load a single standard skill from its directory.
pub fn load_skill(path: &Path, source: SkillSource) -> Result<Skill> {
    load_skill_with_origin(path, source, source.as_str())
}

pub(crate) fn load_skill_with_origin(
    path: &Path,
    source: SkillSource,
    origin: &str,
) -> Result<Skill> {
    let skill_file = path.join("SKILL.md");
    if !skill_file.is_file() {
        return Err(anyhow!("SKILL.md not found in {}", path.display()));
    }

    let content =
        read_utf8_file_bounded(&skill_file, MAX_SKILL_DEFINITION_BYTES, "skill definition")?;
    let skill = Skill::parse_with_origin(&content, path.to_path_buf(), source, origin.to_string())?;
    let directory_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("Skill directory name is not valid UTF-8"))?;
    if directory_name != skill.name {
        return Err(anyhow!(
            "Skill name '{}' must match its directory name '{}'",
            skill.name,
            directory_name
        ));
    }
    Ok(skill)
}

fn load_flat_skill(path: &Path, source: SkillSource, origin: &str) -> Result<Skill> {
    let content = read_utf8_file_bounded(path, MAX_SKILL_DEFINITION_BYTES, "skill definition")?;
    let expected_name = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("Skill filename is not valid UTF-8"))?;
    validate_skill_name(expected_name)?;
    let skill_dir = path
        .parent()
        .ok_or_else(|| anyhow!("Flat skill file has no parent directory"))?;
    let skill = Skill::parse_definition_with_origin(
        &content,
        skill_dir.to_path_buf(),
        path.to_path_buf(),
        source,
        origin.to_string(),
    )?;
    if skill.name != expected_name {
        return Err(anyhow!(
            "Skill name '{}' must match its filename '{}'",
            skill.name,
            expected_name
        ));
    }
    Ok(skill)
}

/// Load a specific UTF-8 file from within a skill directory.
pub fn load_skill_file(skill_path: &Path, file_name: &str) -> Result<String> {
    if file_name.len() > MAX_SKILL_RESOURCE_PATH_BYTES {
        return Err(anyhow!(
            "Skill resource path exceeds {MAX_SKILL_RESOURCE_PATH_BYTES} bytes"
        ));
    }
    let requested = Path::new(file_name);
    if requested.as_os_str().is_empty()
        || requested.is_absolute()
        || requested.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(anyhow!("Invalid relative skill file path: {file_name}"));
    }

    let canonical_skill = skill_path
        .canonicalize()
        .with_context(|| format!("Skill directory is unavailable: {}", skill_path.display()))?;
    let file_path = skill_path.join(requested);
    let canonical_file = file_path
        .canonicalize()
        .with_context(|| format!("Skill file not found: {file_name}"))?;
    if !canonical_file.starts_with(&canonical_skill) {
        return Err(anyhow!("File path escapes skill directory"));
    }
    if !canonical_file.is_file() {
        return Err(anyhow!("Skill resource is not a file: {file_name}"));
    }

    read_utf8_file_bounded(&canonical_file, MAX_SKILL_RESOURCE_BYTES, "skill resource")
        .map_err(|error| anyhow!("Failed to read skill file {file_name}: {error:#}"))
}

pub(crate) fn read_utf8_file_bounded(path: &Path, max_bytes: usize, label: &str) -> Result<String> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("Failed to inspect {label} {}", path.display()))?;
    if !metadata.is_file() {
        return Err(anyhow!("{label} is not a file: {}", path.display()));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(anyhow!(
            "{label} exceeds the {max_bytes} byte limit: {}",
            path.display()
        ));
    }

    let file = fs::File::open(path)
        .with_context(|| format!("Failed to open {label} {}", path.display()))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take((max_bytes + 1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("Failed to read {label} {}", path.display()))?;
    if bytes.len() > max_bytes {
        return Err(anyhow!(
            "{label} exceeds the {max_bytes} byte limit: {}",
            path.display()
        ));
    }
    String::from_utf8(bytes)
        .with_context(|| format!("{label} is not valid UTF-8: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_skill(root: &Path, directory: &str, name: &str) {
        let skill_dir = root.join(directory);
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: Test skill\n---\nBody"),
        )
        .unwrap();
    }

    #[test]
    fn directory_name_must_match_frontmatter() {
        let temp = tempdir().unwrap();
        write_skill(temp.path(), "wrong-directory", "declared-name");
        let report = load_skills_from_root(
            temp.path(),
            SkillSource::Project,
            "agents",
            SkillLoadOptions::default(),
        );
        assert!(report.skills.is_empty());
        assert_eq!(report.diagnostics[0].code, "invalid_skill");
        assert!(report.diagnostics[0].message.contains("must match"));
    }

    #[test]
    fn recursive_and_flat_pi_discovery_are_deterministic() {
        let temp = tempdir().unwrap();
        write_skill(&temp.path().join("bundle"), "nested-skill", "nested-skill");
        fs::write(
            temp.path().join("flat-skill.md"),
            "---\nname: flat-skill\ndescription: Flat\n---\nBody",
        )
        .unwrap();
        let report = load_skills_from_root(
            temp.path(),
            SkillSource::Global,
            "pi",
            SkillLoadOptions {
                recursive: true,
                direct_markdown: true,
            },
        );
        assert_eq!(
            report
                .skills
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            vec!["flat-skill", "nested-skill"]
        );
    }

    #[test]
    fn an_exact_skill_directory_can_be_registered_as_a_root() {
        let temp = tempdir().unwrap();
        write_skill(temp.path(), "exact-skill", "exact-skill");
        let report = load_skills_from_root(
            &temp.path().join("exact-skill"),
            SkillSource::Package,
            "package:demo",
            SkillLoadOptions {
                recursive: true,
                direct_markdown: false,
            },
        );
        assert_eq!(report.skills.len(), 1);
        assert_eq!(report.skills[0].name, "exact-skill");

        let file_report = load_skills_from_root(
            &temp.path().join("exact-skill/SKILL.md"),
            SkillSource::Package,
            "package:demo",
            SkillLoadOptions::default(),
        );
        assert_eq!(file_report.skills.len(), 1);
        assert_eq!(file_report.skills[0].name, "exact-skill");
    }

    #[test]
    fn path_traversal_and_absolute_paths_are_blocked() {
        let temp = tempdir().unwrap();
        let skill_dir = temp.path().join("skill");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(skill_dir.join("resource.md"), "ok").unwrap();
        assert!(load_skill_file(&skill_dir, "../../../etc/passwd").is_err());
        assert!(load_skill_file(&skill_dir, "/etc/passwd").is_err());
        assert_eq!(load_skill_file(&skill_dir, "resource.md").unwrap(), "ok");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escape_is_blocked() {
        use std::os::unix::fs::symlink;
        let temp = tempdir().unwrap();
        let skill_dir = temp.path().join("skill");
        fs::create_dir_all(&skill_dir).unwrap();
        let outside = temp.path().join("outside.txt");
        fs::write(&outside, "secret").unwrap();
        symlink(&outside, skill_dir.join("escape.txt")).unwrap();
        assert!(load_skill_file(&skill_dir, "escape.txt").is_err());
    }

    #[test]
    fn oversized_skill_definition_is_rejected_before_parse() {
        let temp = tempdir().unwrap();
        let skill_dir = temp.path().join("oversized");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("SKILL.md"),
            vec![b' '; MAX_SKILL_DEFINITION_BYTES + 1],
        )
        .unwrap();

        let report = load_skills_from_root(
            temp.path(),
            SkillSource::Project,
            "project",
            SkillLoadOptions::default(),
        );

        assert!(report.skills.is_empty());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("byte limit")));
    }

    #[test]
    fn oversized_skill_resource_is_rejected_before_utf8_decode() {
        let temp = tempdir().unwrap();
        let skill_dir = temp.path().join("skill");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("resource.bin"),
            vec![0_u8; MAX_SKILL_RESOURCE_BYTES + 1],
        )
        .unwrap();

        let error = load_skill_file(&skill_dir, "resource.bin")
            .expect_err("oversized resource must be rejected");
        assert!(error.to_string().contains("byte limit"));
    }
}
