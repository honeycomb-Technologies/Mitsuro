//! Skills manager: compatible discovery, deterministic precedence, policy, and caching.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tracing::info;

use super::loader::{
    load_skill_file, load_skills_from_root, read_utf8_file_bounded, SkillDiagnostic,
    SkillLoadOptions, MAX_SKILL_DEFINITION_BYTES,
};
use super::skill::{validate_skill_name, Skill, SkillInfo, SkillPermission, SkillSource};

const GLOBAL_PRIORITY_BASE: i32 = 100;
// Installed packages are a fallback distribution source. User and project
// definitions must always be able to override package-provided instructions.
const PACKAGE_PRIORITY: i32 = 50;
const PROJECT_PRIORITY_BASE: i32 = 1_000;
const POLICY_FILE_NAME: &str = "skills-policy.json";
const MAX_DISCOVERY_ROOTS: usize = 256;
const MAX_POLICY_FILES: usize = 64;
const MAX_SKILL_POLICY_BYTES: usize = 256 * 1024;
const MAX_SKILL_POLICY_ENTRIES: usize = 4_096;
const MAX_CATALOG_SKILLS: usize = 4_096;
const MAX_CATALOG_CONTENT_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_DIAGNOSTICS: usize = 8_192;
const MAX_FINGERPRINT_ENTRIES: usize = 100_000;
const MAX_FINGERPRINT_BYTES: usize = MAX_CATALOG_CONTENT_BYTES;

/// One filesystem root participating in skill discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillRoot {
    pub path: PathBuf,
    pub source: SkillSource,
    pub origin: String,
    pub priority: i32,
    pub recursive: bool,
    pub direct_markdown: bool,
}

impl SkillRoot {
    pub fn new(
        path: PathBuf,
        source: SkillSource,
        origin: impl Into<String>,
        priority: i32,
    ) -> Self {
        Self {
            path,
            source,
            origin: origin.into(),
            priority,
            recursive: true,
            direct_markdown: false,
        }
    }

    /// A conventional package root. Project roots still override package skills.
    pub fn package(package: impl AsRef<str>, path: PathBuf) -> Self {
        Self::new(
            path,
            SkillSource::Package,
            format!("package:{}", package.as_ref()),
            PACKAGE_PRIORITY,
        )
    }

    pub fn with_direct_markdown(mut self, enabled: bool) -> Self {
        self.direct_markdown = enabled;
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SkillPolicyFile {
    #[serde(default)]
    skills: BTreeMap<String, SkillPolicyOverride>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SkillPolicyOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    permission: Option<SkillPermission>,
}

/// Manages skill discovery, loading, policy, and access.
pub struct SkillsManager {
    /// Native user root retained for compatibility/scaffolding.
    global_dir: PathBuf,
    /// Native nearest-project root retained for compatibility.
    project_dir: Option<PathBuf>,
    roots: Vec<SkillRoot>,
    policy_files: Vec<PathBuf>,
    policy_write_path: PathBuf,
    cache: HashMap<String, Skill>,
    diagnostics: Vec<SkillDiagnostic>,
    cache_fingerprint: Option<u64>,
}

impl SkillsManager {
    /// Create a manager with explicit native global/project roots.
    ///
    /// When the project root has the conventional `.mitsuro/skills` shape, the
    /// full compatibility discovery set is inferred automatically. Synthetic
    /// roots keep the historical two-root behavior, which is useful for tests
    /// and embedded callers.
    pub fn new(global_dir: PathBuf, project_dir: Option<PathBuf>) -> Self {
        if let Some(working_dir) = project_dir.as_deref().and_then(infer_working_dir) {
            let mut manager = Self::with_discovered_roots(working_dir, Some(global_dir.clone()));
            manager.global_dir = global_dir;
            manager.project_dir = project_dir;
            return manager;
        }

        let global_policy = global_dir
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(POLICY_FILE_NAME);
        let mut roots = vec![SkillRoot::new(
            global_dir.clone(),
            SkillSource::Global,
            "mitsuro",
            GLOBAL_PRIORITY_BASE + 60,
        )];
        if let Some(path) = project_dir.clone() {
            roots.push(SkillRoot::new(
                path,
                SkillSource::Project,
                "mitsuro",
                PROJECT_PRIORITY_BASE + 60,
            ));
        }
        Self {
            global_dir,
            project_dir,
            roots,
            policy_files: vec![global_policy.clone()],
            policy_write_path: global_policy,
            cache: HashMap::new(),
            diagnostics: Vec::new(),
            cache_fingerprint: None,
        }
    }

    /// Create a manager with Mitsuro, Agent Skills, OpenCode, Claude, Codex, and
    /// Pi-compatible roots from the current directory through the worktree root.
    pub fn with_defaults(working_dir: &Path) -> Self {
        Self::with_discovered_roots(working_dir, None)
    }

    fn with_discovered_roots(working_dir: &Path, native_global: Option<PathBuf>) -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let global_dir = native_global.unwrap_or_else(|| home.join(".mitsuro/skills"));
        let mut roots = vec![
            SkillRoot::new(
                home.join(".config/opencode/skills"),
                SkillSource::Global,
                "opencode",
                GLOBAL_PRIORITY_BASE + 10,
            ),
            SkillRoot::new(
                home.join(".claude/skills"),
                SkillSource::Global,
                "claude",
                GLOBAL_PRIORITY_BASE + 20,
            ),
            SkillRoot::new(
                home.join(".codex/skills"),
                SkillSource::Global,
                "codex",
                GLOBAL_PRIORITY_BASE + 30,
            ),
            SkillRoot::new(
                home.join(".agents/skills"),
                SkillSource::Global,
                "agents",
                GLOBAL_PRIORITY_BASE + 40,
            ),
            SkillRoot::new(
                home.join(".pi/agent/skills"),
                SkillSource::Global,
                "pi",
                GLOBAL_PRIORITY_BASE + 50,
            )
            .with_direct_markdown(true),
            SkillRoot::new(
                global_dir.clone(),
                SkillSource::Global,
                "mitsuro",
                GLOBAL_PRIORITY_BASE + 60,
            ),
        ];

        let ancestors = discovery_ancestors(working_dir);
        for (depth, directory) in ancestors.iter().enumerate() {
            let base = PROJECT_PRIORITY_BASE + (depth as i32 * 100);
            roots.extend([
                SkillRoot::new(
                    directory.join(".opencode/skills"),
                    SkillSource::Project,
                    "opencode",
                    base + 10,
                ),
                SkillRoot::new(
                    directory.join(".claude/skills"),
                    SkillSource::Project,
                    "claude",
                    base + 20,
                ),
                SkillRoot::new(
                    directory.join(".codex/skills"),
                    SkillSource::Project,
                    "codex",
                    base + 30,
                ),
                SkillRoot::new(
                    directory.join(".agents/skills"),
                    SkillSource::Project,
                    "agents",
                    base + 40,
                ),
                SkillRoot::new(
                    directory.join(".pi/skills"),
                    SkillSource::Project,
                    "pi",
                    base + 50,
                )
                .with_direct_markdown(true),
                SkillRoot::new(
                    crate::identity::legacy_project_state_dir(directory).join("skills"),
                    SkillSource::Project,
                    "mitsuro-deprecated-path",
                    base + 55,
                ),
                SkillRoot::new(
                    crate::paths::project_state_dir(directory).join("skills"),
                    SkillSource::Project,
                    "mitsuro",
                    base + 60,
                ),
            ]);
        }
        // Outside a git worktree, upward traversal can encounter a user-level
        // root again (for example ~/.agents/skills). Keep its original global
        // scope rather than loading the same definition twice as a project root.
        let mut seen_paths = HashSet::new();
        roots.retain(|root| seen_paths.insert(root.path.clone()));
        if roots.len() > MAX_DISCOVERY_ROOTS {
            tracing::warn!(
                roots = roots.len(),
                limit = MAX_DISCOVERY_ROOTS,
                "Truncating skill discovery roots at the safety limit"
            );
            roots.sort_by(|left, right| {
                right
                    .priority
                    .cmp(&left.priority)
                    .then_with(|| left.path.cmp(&right.path))
            });
            roots.truncate(MAX_DISCOVERY_ROOTS);
        }

        let global_policy = global_dir
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(POLICY_FILE_NAME);
        let mut policy_files = vec![global_policy];
        for directory in &ancestors {
            // The policy loader walks this list in reverse, so append the
            // canonical file after its deprecated fallback.
            policy_files
                .push(crate::identity::legacy_project_state_dir(directory).join(POLICY_FILE_NAME));
            policy_files.push(crate::paths::project_state_dir(directory).join(POLICY_FILE_NAME));
        }
        let mut seen_policy_paths = HashSet::new();
        policy_files.retain(|path| seen_policy_paths.insert(path.clone()));
        if policy_files.len() > MAX_POLICY_FILES {
            let global = policy_files.remove(0);
            let nearest_start = policy_files.len() - (MAX_POLICY_FILES - 1);
            let mut bounded = vec![global];
            bounded.extend(policy_files.into_iter().skip(nearest_start));
            policy_files = bounded;
        }
        let nearest = ancestors
            .last()
            .cloned()
            .unwrap_or_else(|| working_dir.to_path_buf());
        let project_dir = crate::paths::project_state_dir(&nearest).join("skills");
        let policy_write_path = crate::paths::project_state_dir(&nearest).join(POLICY_FILE_NAME);

        Self {
            global_dir,
            project_dir: Some(project_dir),
            roots,
            policy_files,
            policy_write_path,
            cache: HashMap::new(),
            diagnostics: Vec::new(),
            cache_fingerprint: None,
        }
    }

    /// Register a package or explicit skill root without coupling the skills
    /// subsystem to a particular plugin/package manager.
    pub fn register_root(&mut self, root: SkillRoot) {
        if let Some(existing) = self
            .roots
            .iter_mut()
            .find(|existing| existing.path == root.path && existing.origin == root.origin)
        {
            *existing = root;
        } else if self.roots.len() < MAX_DISCOVERY_ROOTS {
            self.roots.push(root);
        } else {
            tracing::warn!(
                limit = MAX_DISCOVERY_ROOTS,
                "Ignoring skill root beyond the safety limit"
            );
        }
        self.invalidate();
    }

    pub fn register_package_root(&mut self, package: &str, path: PathBuf) {
        self.register_root(SkillRoot::package(package, path));
    }

    /// Atomically replace every package-contributed root.
    ///
    /// Plugin enable/disable, update, and uninstall flows should call this with
    /// the complete enabled package snapshot so stale skills disappear on the
    /// same refresh. Input order does not affect precedence or diagnostics.
    pub fn set_package_roots(&mut self, mut roots: Vec<(String, PathBuf)>) {
        roots.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        roots.dedup();
        self.roots
            .retain(|root| root.source != SkillSource::Package);
        let available = MAX_DISCOVERY_ROOTS.saturating_sub(self.roots.len());
        if roots.len() > available {
            tracing::warn!(
                package_roots = roots.len(),
                accepted = available,
                "Truncating package skill roots at the safety limit"
            );
            roots.truncate(available);
        }
        self.roots.extend(
            roots
                .into_iter()
                .map(|(package, path)| SkillRoot::package(package, path)),
        );
        self.refresh();
    }

    pub fn discovery_roots(&self) -> &[SkillRoot] {
        &self.roots
    }

    /// Force a refresh. Normal reads also detect filesystem/config changes.
    pub fn refresh(&mut self) {
        self.cache_fingerprint = None;
        self.ensure_cache();
    }

    fn invalidate(&mut self) {
        self.cache_fingerprint = None;
    }

    fn ensure_cache(&mut self) {
        let fingerprint = self.discovery_fingerprint();
        if self.cache_fingerprint == Some(fingerprint) {
            return;
        }
        self.rebuild_cache(fingerprint);
    }

    fn rebuild_cache(&mut self, fingerprint: u64) {
        self.cache.clear();
        self.diagnostics.clear();
        let policy = self.load_policy();

        let mut roots = self.roots.clone();
        roots.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| right.path.cmp(&left.path))
                .then_with(|| right.origin.cmp(&left.origin))
        });

        let mut catalog_content_bytes = 0usize;
        'roots: for root in roots.into_iter().take(MAX_DISCOVERY_ROOTS) {
            let report = load_skills_from_root(
                &root.path,
                root.source,
                &root.origin,
                SkillLoadOptions {
                    recursive: root.recursive,
                    direct_markdown: root.direct_markdown,
                },
            );
            let diagnostic_capacity =
                MAX_CATALOG_DIAGNOSTICS.saturating_sub(self.diagnostics.len());
            if report.diagnostics.len() >= diagnostic_capacity {
                self.diagnostics.extend(
                    report
                        .diagnostics
                        .into_iter()
                        .take(diagnostic_capacity.saturating_sub(1)),
                );
                push_limit_diagnostic(
                    &mut self.diagnostics,
                    "catalog_diagnostic_limit",
                    format!("Skills catalog stopped after {MAX_CATALOG_DIAGNOSTICS} diagnostics"),
                    root.path,
                );
                break;
            }
            self.diagnostics.extend(report.diagnostics);
            for mut skill in report.skills {
                let definition_bytes = skill
                    .content
                    .len()
                    .saturating_add(skill.description.len())
                    .saturating_add(skill.name.len());
                let adds_new_skill = !self.cache.contains_key(&skill.name);
                if (adds_new_skill && self.cache.len() >= MAX_CATALOG_SKILLS)
                    || catalog_content_bytes.saturating_add(definition_bytes)
                        > MAX_CATALOG_CONTENT_BYTES
                {
                    push_limit_diagnostic(
                        &mut self.diagnostics,
                        "catalog_size_limit",
                        format!(
                            "Skills catalog stopped at {MAX_CATALOG_SKILLS} skills or {MAX_CATALOG_CONTENT_BYTES} content bytes"
                        ),
                        skill.definition_path,
                    );
                    break 'roots;
                }
                catalog_content_bytes = catalog_content_bytes.saturating_add(definition_bytes);
                if let Some(override_) = policy.get(&skill.name) {
                    skill.enabled = override_.enabled.unwrap_or(true);
                    skill.permission = override_.permission.unwrap_or_default();
                }
                if let Some(winner) = self.cache.get(&skill.name) {
                    if self.diagnostics.len() < MAX_CATALOG_DIAGNOSTICS {
                        self.diagnostics.push(SkillDiagnostic::warning(
                            "skill_shadowed",
                            format!(
                                "'{}' from {} shadows the lower-precedence definition at {}",
                                winner.name,
                                winner.definition_path.display(),
                                skill.definition_path.display()
                            ),
                            skill.definition_path,
                            Some(skill.name),
                        ));
                    }
                } else {
                    self.cache.insert(skill.name.clone(), skill);
                }
            }
        }

        for name in policy.keys() {
            if !self.cache.contains_key(name) && self.diagnostics.len() < MAX_CATALOG_DIAGNOSTICS {
                self.diagnostics.push(SkillDiagnostic::warning(
                    "policy_skill_missing",
                    format!("Policy references undiscovered skill '{name}'"),
                    self.policy_write_path.clone(),
                    Some(name.clone()),
                ));
            }
        }

        self.cache_fingerprint = Some(fingerprint);
        info!(
            skills = self.cache.len(),
            diagnostics = self.diagnostics.len(),
            roots = self.roots.len(),
            "refreshed skills catalog"
        );
    }

    /// List every discovered skill, including disabled/denied entries for UI.
    pub fn list_skills(&mut self) -> Vec<SkillInfo> {
        self.ensure_cache();
        sorted_info(self.cache.values())
    }

    pub fn list_global_skills(&mut self) -> Vec<SkillInfo> {
        self.ensure_cache();
        sorted_info(
            self.cache
                .values()
                .filter(|skill| skill.source == SkillSource::Global),
        )
    }

    /// Skills safe to advertise for model-driven progressive disclosure.
    pub fn list_model_skills(&mut self, include_project_skills: bool) -> Vec<SkillInfo> {
        self.ensure_cache();
        sorted_info(self.cache.values().filter(|skill| {
            (include_project_skills || skill.source == SkillSource::Global)
                && skill.is_model_invocable()
        }))
    }

    pub fn diagnostics(&mut self) -> Vec<SkillDiagnostic> {
        self.ensure_cache();
        self.diagnostics.clone()
    }

    pub fn get_skill(&mut self, name: &str) -> Option<&Skill> {
        self.ensure_cache();
        self.cache.get(name)
    }

    pub fn skill_exists(&mut self, name: &str) -> bool {
        self.ensure_cache();
        self.cache.contains_key(name)
    }

    pub fn load_skill_content(&mut self, name: &str) -> Result<String> {
        let skill = self.enabled_skill(name)?;
        if skill.permission == SkillPermission::Deny {
            return Err(anyhow!("Skill '{name}' is denied by local policy"));
        }
        Ok(skill.content.clone())
    }

    pub fn load_file_from_skill(&mut self, skill_name: &str, file: &str) -> Result<String> {
        let skill = self.enabled_skill(skill_name)?;
        if skill.permission == SkillPermission::Deny {
            return Err(anyhow!("Skill '{skill_name}' is denied by local policy"));
        }
        load_skill_file(&skill.path, file)
    }

    fn enabled_skill(&mut self, name: &str) -> Result<&Skill> {
        self.ensure_cache();
        let skill = self
            .cache
            .get(name)
            .ok_or_else(|| anyhow!("Skill '{name}' not found"))?;
        if !skill.enabled {
            return Err(anyhow!("Skill '{name}' is disabled by local policy"));
        }
        Ok(skill)
    }

    /// Persist an enable/disable override at the nearest project policy path.
    pub fn set_skill_enabled(&mut self, name: &str, enabled: bool) -> Result<()> {
        validate_skill_name(name)?;
        self.ensure_cache();
        if !self.cache.contains_key(name) {
            return Err(anyhow!("Skill '{name}' not found"));
        }
        self.update_policy(name, |entry| entry.enabled = Some(enabled))
    }

    /// Persist an allow/ask/deny override at the nearest project policy path.
    pub fn set_skill_permission(&mut self, name: &str, permission: SkillPermission) -> Result<()> {
        validate_skill_name(name)?;
        self.ensure_cache();
        if !self.cache.contains_key(name) {
            return Err(anyhow!("Skill '{name}' not found"));
        }
        self.update_policy(name, |entry| entry.permission = Some(permission))
    }

    fn update_policy(
        &mut self,
        name: &str,
        update: impl FnOnce(&mut SkillPolicyOverride),
    ) -> Result<()> {
        let mut file = read_policy_file(&self.policy_write_path)?;
        update(file.skills.entry(name.to_string()).or_default());
        write_policy_file(&self.policy_write_path, &file)?;
        self.invalidate();
        self.ensure_cache();
        Ok(())
    }

    fn load_policy(&mut self) -> BTreeMap<String, SkillPolicyOverride> {
        let mut global = BTreeMap::new();
        if let Some(path) = self.policy_files.first() {
            merge_policy_file(
                path,
                &mut global,
                &mut self.diagnostics,
                "Global skill policy",
            );
        }

        // Nearest project policy still wins field-by-field among project files.
        // It is combined with user policy below instead of replacing it: a
        // repository may narrow user policy, but must never loosen it.
        let mut project = BTreeMap::new();
        for path in self
            .policy_files
            .iter()
            .skip(1)
            .rev()
            .take(MAX_POLICY_FILES.saturating_sub(1))
        {
            merge_policy_file(
                path,
                &mut project,
                &mut self.diagnostics,
                "Project skill policy",
            );
        }

        let mut effective = global;
        for (name, project_override) in project {
            if let Some(global_override) = effective.get_mut(&name) {
                global_override.enabled =
                    restrictive_enabled(global_override.enabled, project_override.enabled);
                global_override.permission =
                    restrictive_permission(global_override.permission, project_override.permission);
            } else if effective.len() < MAX_SKILL_POLICY_ENTRIES {
                effective.insert(name, project_override);
            } else {
                let path = self
                    .policy_files
                    .last()
                    .cloned()
                    .unwrap_or_else(|| self.policy_write_path.clone());
                push_limit_diagnostic(
                    &mut self.diagnostics,
                    "policy_entry_limit",
                    format!("Skill policy stopped after {MAX_SKILL_POLICY_ENTRIES} entries"),
                    path,
                );
                break;
            }
        }
        effective
    }

    fn discovery_fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        let mut budget = FingerprintBudget {
            entries_remaining: MAX_FINGERPRINT_ENTRIES,
            bytes_remaining: MAX_FINGERPRINT_BYTES,
        };
        let mut roots = self.roots.iter().collect::<Vec<_>>();
        roots.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| right.path.cmp(&left.path))
                .then_with(|| right.origin.cmp(&left.origin))
        });
        for root in roots.into_iter().take(MAX_DISCOVERY_ROOTS) {
            root.path.hash(&mut hasher);
            root.origin.hash(&mut hasher);
            root.priority.hash(&mut hasher);
            hash_tree(&root.path, 0, &mut budget, &mut hasher);
        }
        for path in self.policy_files.iter().take(MAX_POLICY_FILES) {
            path.hash(&mut hasher);
            hash_bounded_file(path, MAX_SKILL_POLICY_BYTES, &mut budget, &mut hasher);
        }
        hasher.finish()
    }

    pub fn global_dir(&self) -> &PathBuf {
        &self.global_dir
    }

    pub fn project_dir(&self) -> Option<&PathBuf> {
        self.project_dir.as_ref()
    }

    pub fn policy_write_path(&self) -> &Path {
        &self.policy_write_path
    }
}

fn merge_policy_file(
    path: &Path,
    merged: &mut BTreeMap<String, SkillPolicyOverride>,
    diagnostics: &mut Vec<SkillDiagnostic>,
    scope: &str,
) {
    if !path.is_file() {
        return;
    }
    match read_policy_file(path) {
        Ok(file) => {
            for (name, override_) in file.skills {
                if let Err(error) = validate_skill_name(&name) {
                    if diagnostics.len() < MAX_CATALOG_DIAGNOSTICS - 1 {
                        diagnostics.push(SkillDiagnostic::error(
                            "invalid_policy_skill_name",
                            error.to_string(),
                            path.to_path_buf(),
                            Some(name),
                        ));
                    }
                    continue;
                }
                if !merged.contains_key(&name) && merged.len() >= MAX_SKILL_POLICY_ENTRIES {
                    push_limit_diagnostic(
                        diagnostics,
                        "policy_entry_limit",
                        format!("{scope} stopped after {MAX_SKILL_POLICY_ENTRIES} entries"),
                        path.to_path_buf(),
                    );
                    break;
                }
                let target = merged.entry(name).or_default();
                if target.enabled.is_none() && override_.enabled.is_some() {
                    target.enabled = override_.enabled;
                }
                if target.permission.is_none() && override_.permission.is_some() {
                    target.permission = override_.permission;
                }
            }
        }
        Err(error) => {
            if diagnostics.len() < MAX_CATALOG_DIAGNOSTICS - 1 {
                diagnostics.push(SkillDiagnostic::error(
                    "invalid_policy_file",
                    error.to_string(),
                    path.to_path_buf(),
                    None,
                ));
            }
        }
    }
}

fn restrictive_enabled(global: Option<bool>, project: Option<bool>) -> Option<bool> {
    match (global, project) {
        (Some(global), Some(project)) => Some(global && project),
        (Some(global), None) => Some(global),
        (None, Some(project)) => Some(project),
        (None, None) => None,
    }
}

fn restrictive_permission(
    global: Option<SkillPermission>,
    project: Option<SkillPermission>,
) -> Option<SkillPermission> {
    fn rank(permission: SkillPermission) -> u8 {
        match permission {
            SkillPermission::Allow => 0,
            SkillPermission::Ask => 1,
            SkillPermission::Deny => 2,
        }
    }

    match (global, project) {
        (Some(global), Some(project)) => Some(if rank(global) >= rank(project) {
            global
        } else {
            project
        }),
        (Some(global), None) => Some(global),
        (None, Some(project)) => Some(project),
        (None, None) => None,
    }
}

fn infer_working_dir(project_skills: &Path) -> Option<&Path> {
    if project_skills.file_name().and_then(|name| name.to_str()) != Some("skills") {
        return None;
    }
    let state = project_skills.parent()?;
    if state.file_name().and_then(|name| name.to_str()) != Some(".mitsuro") {
        return None;
    }
    state.parent()
}

/// Return directories from the worktree/filesystem boundary to `working_dir`,
/// so increasing depth yields increasing precedence.
fn discovery_ancestors(working_dir: &Path) -> Vec<PathBuf> {
    let start = working_dir
        .canonicalize()
        .unwrap_or_else(|_| working_dir.to_path_buf());
    let start = if start.is_file() {
        start.parent().unwrap_or(&start).to_path_buf()
    } else {
        start
    };
    let mut nearest_first = Vec::new();
    let mut current = Some(start.as_path());
    while let Some(directory) = current {
        nearest_first.push(directory.to_path_buf());
        if directory.join(".git").exists() {
            break;
        }
        current = directory.parent();
    }
    nearest_first.reverse();
    nearest_first
}

fn sorted_info<'a>(skills: impl Iterator<Item = &'a Skill>) -> Vec<SkillInfo> {
    let mut skills = skills.map(Skill::to_info).collect::<Vec<_>>();
    skills.sort_by(|left, right| left.name.cmp(&right.name));
    skills
}

fn push_limit_diagnostic(
    diagnostics: &mut Vec<SkillDiagnostic>,
    code: &str,
    message: String,
    path: PathBuf,
) {
    diagnostics.truncate(MAX_CATALOG_DIAGNOSTICS - 1);
    diagnostics.push(SkillDiagnostic::warning(code, message, path, None));
}

fn read_policy_file(path: &Path) -> Result<SkillPolicyFile> {
    if !path.exists() {
        return Ok(SkillPolicyFile::default());
    }
    let content = read_utf8_file_bounded(path, MAX_SKILL_POLICY_BYTES, "skill policy")?;
    serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse skill policy {}", path.display()))
}

fn write_policy_file(path: &Path, policy: &SkillPolicyFile) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("Skill policy path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".{POLICY_FILE_NAME}.tmp-{}", std::process::id()));
    let content = serde_json::to_string_pretty(policy)?;
    if content.len() > MAX_SKILL_POLICY_BYTES {
        return Err(anyhow!(
            "Skill policy exceeds the {MAX_SKILL_POLICY_BYTES} byte limit: {}",
            path.display()
        ));
    }
    fs::write(&temporary, format!("{content}\n"))?;
    fs::rename(&temporary, path).with_context(|| {
        format!(
            "Failed to atomically replace skill policy {}",
            path.display()
        )
    })?;
    Ok(())
}

struct FingerprintBudget {
    entries_remaining: usize,
    bytes_remaining: usize,
}

fn hash_bounded_file(
    path: &Path,
    max_bytes: usize,
    budget: &mut FingerprintBudget,
    hasher: &mut DefaultHasher,
) {
    let Ok(metadata) = fs::metadata(path) else {
        "unreadable-file".hash(hasher);
        return;
    };
    metadata.len().hash(hasher);
    metadata.modified().ok().hash(hasher);

    let read_limit = max_bytes.min(budget.bytes_remaining);
    if read_limit == 0 {
        "fingerprint-byte-limit".hash(hasher);
        return;
    }
    let Ok(file) = fs::File::open(path) else {
        "unreadable-file".hash(hasher);
        return;
    };
    use std::io::Read;
    let capacity = usize::try_from(metadata.len())
        .unwrap_or(read_limit)
        .min(read_limit);
    let mut bytes = Vec::with_capacity(capacity);
    match file.take((read_limit + 1) as u64).read_to_end(&mut bytes) {
        Ok(_) => {
            let retained = bytes.len().min(read_limit);
            bytes[..retained].hash(hasher);
            budget.bytes_remaining = budget.bytes_remaining.saturating_sub(retained);
            if bytes.len() > read_limit || metadata.len() > retained as u64 {
                "fingerprint-file-truncated".hash(hasher);
            }
        }
        Err(_) => "unreadable-file".hash(hasher),
    }
}

fn hash_tree(
    path: &Path,
    depth: usize,
    budget: &mut FingerprintBudget,
    hasher: &mut DefaultHasher,
) {
    if path.is_file() {
        hash_bounded_file(path, MAX_SKILL_DEFINITION_BYTES, budget, hasher);
        return;
    }
    if depth > 9 || !path.is_dir() {
        return;
    }
    let definition = path.join("SKILL.md");
    if definition.is_file() {
        "skill-boundary".hash(hasher);
        hash_bounded_file(&definition, MAX_SKILL_DEFINITION_BYTES, budget, hasher);
        return;
    }
    let Ok(entries) = fs::read_dir(path) else {
        "unreadable".hash(hasher);
        return;
    };
    let mut bounded_entries = Vec::new();
    for entry in entries.flatten() {
        if budget.entries_remaining == 0 {
            "fingerprint-entry-limit".hash(hasher);
            break;
        }
        budget.entries_remaining -= 1;
        bounded_entries.push(entry);
    }
    let mut entries = bounded_entries;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        entry.file_name().hash(hasher);
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            "symlink".hash(hasher);
        } else if file_type.is_dir() {
            hash_tree(&entry.path(), depth + 1, budget, hasher);
        } else if entry.file_name() == "SKILL.md"
            || (depth == 0
                && entry.path().extension().and_then(|value| value.to_str()) == Some("md"))
        {
            hash_bounded_file(&entry.path(), MAX_SKILL_DEFINITION_BYTES, budget, hasher);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_skill(root: &Path, name: &str, description: &str) {
        let skill_dir = root.join(name);
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {description}\n---\n# {name}"),
        )
        .unwrap();
    }

    #[test]
    fn project_overrides_global_with_diagnostic() {
        let temp = tempdir().unwrap();
        let global = temp.path().join("global");
        let project = temp.path().join("project");
        write_skill(&global, "shared-skill", "Global version");
        write_skill(&project, "shared-skill", "Project version");

        let mut manager = SkillsManager::new(global, Some(project));
        let skill = manager.get_skill("shared-skill").unwrap();
        assert_eq!(skill.description, "Project version");
        assert_eq!(skill.source, SkillSource::Project);
        assert!(manager
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code == "skill_shadowed"));
    }

    #[test]
    fn discovers_compatible_roots_upward_with_nearest_precedence() {
        let temp = tempdir().unwrap();
        fs::create_dir(temp.path().join(".git")).unwrap();
        let nested = temp.path().join("a/b");
        fs::create_dir_all(&nested).unwrap();
        write_skill(&temp.path().join(".agents/skills"), "shared-skill", "root");
        write_skill(
            &temp.path().join("a/.claude/skills"),
            "shared-skill",
            "nearest",
        );
        write_skill(&temp.path().join(".pi/skills"), "pi-skill", "pi compatible");

        let mut manager = SkillsManager::with_defaults(&nested);
        assert_eq!(
            manager.get_skill("shared-skill").unwrap().description,
            "nearest"
        );
        assert!(manager.skill_exists("pi-skill"));
    }

    #[test]
    fn deprecated_project_skill_root_is_read_but_canonical_root_wins() {
        let temp = tempdir().unwrap();
        fs::create_dir(temp.path().join(".git")).unwrap();
        let nested = temp.path().join("nested");
        fs::create_dir_all(&nested).unwrap();
        let deprecated = crate::identity::legacy_project_state_dir(temp.path()).join("skills");
        let canonical = crate::paths::project_state_dir(temp.path()).join("skills");

        write_skill(
            &deprecated,
            "identity-bridge-project-skill",
            "deprecated path",
        );
        let mut fallback_manager = SkillsManager::with_defaults(&nested);
        assert_eq!(
            fallback_manager
                .get_skill("identity-bridge-project-skill")
                .unwrap()
                .description,
            "deprecated path"
        );

        write_skill(
            &canonical,
            "identity-bridge-project-skill",
            "canonical path",
        );
        let mut canonical_manager = SkillsManager::with_defaults(&nested);
        assert_eq!(
            canonical_manager
                .get_skill("identity-bridge-project-skill")
                .unwrap()
                .description,
            "canonical path"
        );
    }

    #[test]
    fn cache_detects_content_changes_without_manual_refresh() {
        let temp = tempdir().unwrap();
        let global = temp.path().join("global");
        write_skill(&global, "live-skill", "before");
        let mut manager = SkillsManager::new(global.clone(), None);
        assert_eq!(
            manager.get_skill("live-skill").unwrap().description,
            "before"
        );

        write_skill(&global, "live-skill", "after");
        assert_eq!(
            manager.get_skill("live-skill").unwrap().description,
            "after"
        );
    }

    #[test]
    fn persisted_policy_controls_catalog_and_model_visibility() {
        let temp = tempdir().unwrap();
        let global = temp.path().join("global");
        write_skill(&global, "safe-skill", "safe");
        let mut manager = SkillsManager::new(global, None);

        manager
            .set_skill_permission("safe-skill", SkillPermission::Ask)
            .unwrap();
        assert_eq!(
            manager.get_skill("safe-skill").unwrap().permission,
            SkillPermission::Ask
        );
        manager.set_skill_enabled("safe-skill", false).unwrap();
        assert!(!manager.get_skill("safe-skill").unwrap().enabled);
        assert!(manager.list_model_skills(true).is_empty());
        assert!(manager.load_skill_content("safe-skill").is_err());

        // A fresh manager observes the persisted override.
        let global = temp.path().join("global");
        let mut reloaded = SkillsManager::new(global, None);
        assert!(!reloaded.get_skill("safe-skill").unwrap().enabled);
        assert_eq!(
            reloaded.get_skill("safe-skill").unwrap().permission,
            SkillPermission::Ask
        );
    }

    #[test]
    fn project_policy_can_narrow_but_not_loosen_global_policy() {
        let temp = tempdir().unwrap();
        let global_skills = temp.path().join("global/skills");
        for name in [
            "locked-off",
            "locked-deny",
            "supervised",
            "local-narrow",
            "nearest-wins",
        ] {
            write_skill(&global_skills, name, name);
        }

        let global_policy = temp.path().join("global/skills-policy.json");
        let root_policy = temp.path().join("repo/.mitsuro/skills-policy.json");
        let nearest_policy = temp.path().join("repo/nested/.mitsuro/skills-policy.json");
        write_policy_file(
            &global_policy,
            &SkillPolicyFile {
                skills: BTreeMap::from([
                    (
                        "locked-off".to_string(),
                        SkillPolicyOverride {
                            enabled: Some(false),
                            permission: Some(SkillPermission::Allow),
                        },
                    ),
                    (
                        "locked-deny".to_string(),
                        SkillPolicyOverride {
                            enabled: Some(true),
                            permission: Some(SkillPermission::Deny),
                        },
                    ),
                    (
                        "supervised".to_string(),
                        SkillPolicyOverride {
                            enabled: None,
                            permission: Some(SkillPermission::Ask),
                        },
                    ),
                    (
                        "local-narrow".to_string(),
                        SkillPolicyOverride {
                            enabled: Some(true),
                            permission: Some(SkillPermission::Allow),
                        },
                    ),
                    (
                        "nearest-wins".to_string(),
                        SkillPolicyOverride {
                            enabled: None,
                            permission: Some(SkillPermission::Allow),
                        },
                    ),
                ]),
            },
        )
        .unwrap();
        write_policy_file(
            &root_policy,
            &SkillPolicyFile {
                skills: BTreeMap::from([
                    (
                        "locked-off".to_string(),
                        SkillPolicyOverride {
                            enabled: Some(true),
                            permission: None,
                        },
                    ),
                    (
                        "locked-deny".to_string(),
                        SkillPolicyOverride {
                            enabled: None,
                            permission: Some(SkillPermission::Allow),
                        },
                    ),
                    (
                        "supervised".to_string(),
                        SkillPolicyOverride {
                            enabled: None,
                            permission: Some(SkillPermission::Deny),
                        },
                    ),
                    (
                        "local-narrow".to_string(),
                        SkillPolicyOverride {
                            enabled: Some(false),
                            permission: Some(SkillPermission::Deny),
                        },
                    ),
                    (
                        "nearest-wins".to_string(),
                        SkillPolicyOverride {
                            enabled: None,
                            permission: Some(SkillPermission::Deny),
                        },
                    ),
                ]),
            },
        )
        .unwrap();
        write_policy_file(
            &nearest_policy,
            &SkillPolicyFile {
                skills: BTreeMap::from([
                    (
                        "supervised".to_string(),
                        SkillPolicyOverride {
                            enabled: None,
                            permission: Some(SkillPermission::Allow),
                        },
                    ),
                    (
                        "nearest-wins".to_string(),
                        SkillPolicyOverride {
                            enabled: None,
                            permission: Some(SkillPermission::Allow),
                        },
                    ),
                ]),
            },
        )
        .unwrap();

        let mut manager = SkillsManager::new(global_skills, None);
        manager.policy_files = vec![global_policy, root_policy, nearest_policy.clone()];
        manager.policy_write_path = nearest_policy;
        manager.refresh();

        assert!(!manager.get_skill("locked-off").unwrap().enabled);
        assert_eq!(
            manager.get_skill("locked-deny").unwrap().permission,
            SkillPermission::Deny
        );
        assert_eq!(
            manager.get_skill("supervised").unwrap().permission,
            SkillPermission::Ask
        );
        let narrowed = manager.get_skill("local-narrow").unwrap();
        assert!(!narrowed.enabled);
        assert_eq!(narrowed.permission, SkillPermission::Deny);
        assert_eq!(
            manager.get_skill("nearest-wins").unwrap().permission,
            SkillPermission::Allow
        );
    }

    #[test]
    fn registered_package_root_participates_below_project_precedence() {
        let temp = tempdir().unwrap();
        let global = temp.path().join("global");
        let project = temp.path().join("project");
        let package = temp.path().join("package");
        write_skill(&package, "packaged-skill", "from package");
        write_skill(&package, "global-shadowed", "from package");
        write_skill(&global, "global-shadowed", "from global");
        write_skill(&project, "packaged-skill", "from project");
        let mut manager = SkillsManager::new(global, Some(project));
        manager.register_package_root("demo", package);
        let skill = manager.get_skill("packaged-skill").unwrap();
        assert_eq!(skill.description, "from project");
        assert_eq!(skill.source, SkillSource::Project);
        let global_skill = manager.get_skill("global-shadowed").unwrap();
        assert_eq!(global_skill.description, "from global");
        assert_eq!(global_skill.source, SkillSource::Global);
    }

    #[test]
    fn package_root_snapshot_removes_disabled_or_uninstalled_contributions() {
        let temp = tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        write_skill(&first, "first-skill", "first");
        write_skill(&second, "second-skill", "second");
        let mut manager = SkillsManager::new(temp.path().join("global"), None);

        manager.set_package_roots(vec![("first-package".to_string(), first)]);
        assert!(manager.skill_exists("first-skill"));
        manager.set_package_roots(vec![("second-package".to_string(), second)]);
        assert!(!manager.skill_exists("first-skill"));
        assert!(manager.skill_exists("second-skill"));
        manager.set_package_roots(Vec::new());
        assert!(!manager.skill_exists("second-skill"));
    }

    #[test]
    fn oversized_policy_is_rejected_before_parse_or_fingerprint_allocation() {
        let temp = tempdir().unwrap();
        let policy = temp.path().join(POLICY_FILE_NAME);
        fs::write(&policy, vec![b' '; MAX_SKILL_POLICY_BYTES + 1]).unwrap();

        let error = read_policy_file(&policy).expect_err("oversized policy must fail");
        assert!(error.to_string().contains("byte limit"));

        let mut budget = FingerprintBudget {
            entries_remaining: 1,
            bytes_remaining: 32,
        };
        let mut hasher = DefaultHasher::new();
        hash_bounded_file(&policy, MAX_SKILL_POLICY_BYTES, &mut budget, &mut hasher);
        assert_eq!(budget.bytes_remaining, 0);
    }

    #[test]
    fn package_skill_roots_are_capped_deterministically() {
        let temp = tempdir().unwrap();
        let mut manager = SkillsManager::new(temp.path().join("global"), None);
        let roots = (0..MAX_DISCOVERY_ROOTS + 10)
            .map(|index| {
                (
                    format!("package-{index:04}"),
                    temp.path().join(format!("missing-{index:04}")),
                )
            })
            .collect();

        manager.set_package_roots(roots);
        assert!(manager.discovery_roots().len() <= MAX_DISCOVERY_ROOTS);
    }
}
