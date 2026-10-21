use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};

use super::storage::load_lockfile;
use super::transaction::{commit_staged_install, create_staging_root, remove_manager_owned_root};
use super::validation::{parse_toml_or_json, validate_relative_path_for, MAX_MANIFEST_BYTES};
use super::PluginManager;
use crate::plugins::{
    InstalledPlugin, PluginInstallOptions, PluginLockEntry, PluginManifestV1, PluginSourceTrust,
};

pub(super) const MAX_PACKAGE_SNAPSHOT_ENTRIES: usize = 100_000;
pub(super) const MAX_PACKAGE_SNAPSHOT_BYTES: u64 = 512 * 1024 * 1024;
pub(super) const MAX_PACKAGE_SNAPSHOT_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PACKAGE_JSON_BYTES: usize = MAX_MANIFEST_BYTES;
const MAX_PACKAGE_PLUGIN_MANIFESTS: usize = 256;
const MAX_PACKAGE_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
const MAX_PACKAGE_COMMAND_STREAM_BYTES: usize = 32 * 1024;
const PACKAGE_COMMAND_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const PACKAGE_COMMAND_TERM_GRACE: Duration = Duration::from_millis(500);
const PACKAGE_COMMAND_PIPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const PACKAGE_COMMAND_SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, Debug)]
struct PackageSnapshotLimits {
    max_entries: usize,
    max_total_bytes: u64,
    max_file_bytes: u64,
}

const PACKAGE_SNAPSHOT_LIMITS: PackageSnapshotLimits = PackageSnapshotLimits {
    max_entries: MAX_PACKAGE_SNAPSHOT_ENTRIES,
    max_total_bytes: MAX_PACKAGE_SNAPSHOT_BYTES,
    max_file_bytes: MAX_PACKAGE_SNAPSHOT_FILE_BYTES,
};

#[derive(Debug)]
struct PackageCommandSnapshotBudget {
    root: PathBuf,
    limits: PackageSnapshotLimits,
}

impl PackageCommandSnapshotBudget {
    fn staged(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            limits: PACKAGE_SNAPSHOT_LIMITS,
        }
    }
}

#[derive(Debug, Default)]
struct BoundedCommandStream {
    bytes: Vec<u8>,
    dropped_bytes: usize,
    incomplete: bool,
}

impl BoundedCommandStream {
    fn push(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }

        if chunk.len() >= MAX_PACKAGE_COMMAND_STREAM_BYTES {
            self.dropped_bytes = self
                .dropped_bytes
                .saturating_add(self.bytes.len())
                .saturating_add(chunk.len() - MAX_PACKAGE_COMMAND_STREAM_BYTES);
            self.bytes.clear();
            self.bytes
                .extend_from_slice(&chunk[chunk.len() - MAX_PACKAGE_COMMAND_STREAM_BYTES..]);
            return;
        }

        let overflow = self
            .bytes
            .len()
            .saturating_add(chunk.len())
            .saturating_sub(MAX_PACKAGE_COMMAND_STREAM_BYTES);
        if overflow > 0 {
            self.bytes.drain(..overflow);
            self.dropped_bytes = self.dropped_bytes.saturating_add(overflow);
        }
        self.bytes.extend_from_slice(chunk);
    }
}

#[derive(Debug)]
struct PackageCommandOutput {
    status: Option<ExitStatus>,
    timed_out: bool,
    stdout: BoundedCommandStream,
    stderr: BoundedCommandStream,
}

#[cfg(unix)]
struct PackageProcessGroupGuard {
    leader_pid: u32,
    armed: bool,
}

#[cfg(unix)]
impl PackageProcessGroupGuard {
    fn new(leader_pid: u32) -> Self {
        Self {
            leader_pid,
            armed: true,
        }
    }

    fn terminate_remaining(&mut self) {
        match crate::process::signals::process_group_exists(self.leader_pid) {
            Ok(false) => self.armed = false,
            Ok(true) => {
                if let Err(error) = crate::process::signals::signal_process_group(
                    self.leader_pid,
                    libc::SIGKILL,
                    "SIGKILL",
                ) {
                    tracing::debug!(
                        pid = self.leader_pid,
                        %error,
                        "Failed to terminate remaining plugin package command descendants"
                    );
                }
            }
            Err(error) => {
                tracing::debug!(
                    pid = self.leader_pid,
                    %error,
                    "Failed to inspect plugin package command process group"
                );
            }
        }
    }

    fn disarm_if_gone(&mut self) {
        if matches!(
            crate::process::signals::process_group_exists(self.leader_pid),
            Ok(false)
        ) {
            self.armed = false;
        }
    }
}

#[cfg(unix)]
impl Drop for PackageProcessGroupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let _ = crate::process::signals::signal_process_group(
            self.leader_pid,
            libc::SIGKILL,
            "SIGKILL",
        );
    }
}

async fn read_bounded_command_stream<R>(mut reader: R) -> BoundedCommandStream
where
    R: AsyncRead + Unpin,
{
    let mut output = BoundedCommandStream::default();
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(count) => output.push(&chunk[..count]),
            Err(error) => {
                tracing::debug!(%error, "Failed to drain plugin package command output");
                output.incomplete = true;
                break;
            }
        }
    }
    output
}

async fn finish_command_reader(
    reader: Option<JoinHandle<BoundedCommandStream>>,
) -> BoundedCommandStream {
    let Some(mut reader) = reader else {
        return BoundedCommandStream::default();
    };

    match timeout(PACKAGE_COMMAND_PIPE_DRAIN_TIMEOUT, &mut reader).await {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => {
            tracing::debug!(%error, "Plugin package command output reader failed");
            BoundedCommandStream {
                incomplete: true,
                ..BoundedCommandStream::default()
            }
        }
        Err(_) => {
            reader.abort();
            let _ = reader.await;
            BoundedCommandStream {
                incomplete: true,
                ..BoundedCommandStream::default()
            }
        }
    }
}

async fn terminate_package_process_tree(child: &mut Child, pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid {
        if let Err(error) =
            crate::process::signals::signal_process_group(pid, libc::SIGTERM, "SIGTERM")
        {
            tracing::debug!(pid, %error, "Failed to terminate plugin package process group");
        }
        tokio::time::sleep(PACKAGE_COMMAND_TERM_GRACE).await;
        match crate::process::signals::process_group_exists(pid) {
            Ok(false) => {}
            Ok(true) | Err(_) => {
                if let Err(error) =
                    crate::process::signals::signal_process_group(pid, libc::SIGKILL, "SIGKILL")
                {
                    tracing::debug!(pid, %error, "Failed to kill plugin package process group");
                }
            }
        }
    }

    #[cfg(windows)]
    if let Some(pid) = pid {
        let _ = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }

    if timeout(PACKAGE_COMMAND_TERM_GRACE, child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

async fn run_bounded_package_command(
    mut command: Command,
    timeout_duration: Duration,
    snapshot_budget: Option<PackageCommandSnapshotBudget>,
) -> Result<PackageCommandOutput> {
    let snapshot_identity = if let Some(budget) = &snapshot_budget {
        let identity =
            snapshot_directory_identity(&budget.root, "live plugin package snapshot root").await?;
        validate_live_package_snapshot_budget(&budget.root, budget.limits, identity).await?;
        Some(identity)
    } else {
        None
    };

    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command.spawn()?;
    let pid = child.id();
    #[cfg(unix)]
    let mut process_group_guard = pid.map(PackageProcessGroupGuard::new);

    let stdout_reader = child
        .stdout
        .take()
        .map(|stdout| tokio::spawn(read_bounded_command_stream(stdout)));
    let stderr_reader = child
        .stderr
        .take()
        .map(|stderr| tokio::spawn(read_bounded_command_stream(stderr)));

    enum CommandWaitOutcome {
        Exited(std::io::Result<ExitStatus>),
        TimedOut,
        SnapshotBudgetExceeded(anyhow::Error),
    }

    let wait_outcome = {
        let wait = child.wait();
        tokio::pin!(wait);
        let deadline = tokio::time::sleep(timeout_duration);
        tokio::pin!(deadline);

        if let Some(budget) = &snapshot_budget {
            let expected_identity = snapshot_identity
                .context("package snapshot budget root identity was not captured")?;
            let violation = wait_for_live_package_snapshot_violation(budget, expected_identity);
            tokio::pin!(violation);
            tokio::select! {
                result = &mut wait => CommandWaitOutcome::Exited(result),
                _ = &mut deadline => CommandWaitOutcome::TimedOut,
                error = &mut violation => CommandWaitOutcome::SnapshotBudgetExceeded(error),
            }
        } else {
            tokio::select! {
                result = &mut wait => CommandWaitOutcome::Exited(result),
                _ = &mut deadline => CommandWaitOutcome::TimedOut,
            }
        }
    };

    let (status, timed_out, wait_error, snapshot_violation) = match wait_outcome {
        CommandWaitOutcome::Exited(Ok(status)) => (Some(status), false, None, None),
        CommandWaitOutcome::Exited(Err(error)) => (None, false, Some(error), None),
        CommandWaitOutcome::TimedOut => (None, true, None, None),
        CommandWaitOutcome::SnapshotBudgetExceeded(error) => (None, false, None, Some(error)),
    };

    if timed_out || wait_error.is_some() || snapshot_violation.is_some() {
        terminate_package_process_tree(&mut child, pid).await;
    } else {
        #[cfg(unix)]
        if let Some(guard) = &mut process_group_guard {
            // npm package commands are not background-process launchers. Kill
            // descendants that retain inherited pipes after the leader exits.
            guard.terminate_remaining();
        }
    }

    let (stdout, stderr) = tokio::join!(
        finish_command_reader(stdout_reader),
        finish_command_reader(stderr_reader)
    );

    #[cfg(unix)]
    if let Some(guard) = &mut process_group_guard {
        guard.disarm_if_gone();
    }

    let output = PackageCommandOutput {
        status,
        timed_out,
        stdout,
        stderr,
    };
    if let Some(error) = wait_error {
        bail!(
            "failed while waiting for plugin package command: {error}; {}",
            format_command_output(&output)
        );
    }
    if let Some(error) = snapshot_violation {
        bail!(
            "plugin package command exceeded its live snapshot budget: {error:#}; {}",
            format_command_output(&output)
        );
    }
    Ok(output)
}

#[derive(Debug, Deserialize)]
struct PackageJson {
    #[allow(dead_code)]
    name: Option<String>,
    #[allow(dead_code)]
    version: Option<String>,
    #[serde(default, alias = "krusty")]
    mitsuro: Option<MitsuroPackageManifest>,
    #[serde(default)]
    scripts: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct MitsuroPackageManifest {
    #[serde(default)]
    plugins: Vec<String>,
}

impl PluginManager {
    /// Install either a signed standalone plugin manifest, an npm package
    /// (`npm:<spec>`), or a local package directory. Arbitrary package scripts
    /// are disabled unless explicitly enabled through `PluginInstallOptions`.
    pub async fn install_from_ref(&self, plugin_ref: &str) -> Result<Vec<InstalledPlugin>> {
        self.install_from_ref_with_options(plugin_ref, PluginInstallOptions::default())
            .await
    }

    pub async fn install_from_ref_with_options(
        &self,
        plugin_ref: &str,
        options: PluginInstallOptions,
    ) -> Result<Vec<InstalledPlugin>> {
        let _guard = self.acquire_mutation().await?;
        self.install_from_ref_with_options_unlocked(plugin_ref, options)
            .await
    }

    pub(super) async fn install_from_ref_with_options_unlocked(
        &self,
        plugin_ref: &str,
        options: PluginInstallOptions,
    ) -> Result<Vec<InstalledPlugin>> {
        if plugin_ref.trim().starts_with("npm:") {
            return self
                .install_from_package_ref_with_options_unlocked(plugin_ref, options)
                .await;
        }

        let path = PathBuf::from(plugin_ref);
        if path.is_dir() {
            return self
                .install_from_package_ref_with_options_unlocked(plugin_ref, options)
                .await;
        }

        Ok(vec![
            self.install_from_manifest_ref_with_options_unlocked(plugin_ref, options)
                .await?,
        ])
    }

    pub async fn install_from_package_ref_with_options(
        &self,
        package_ref: &str,
        options: PluginInstallOptions,
    ) -> Result<Vec<InstalledPlugin>> {
        let _guard = self.acquire_mutation().await?;
        self.install_from_package_ref_with_options_unlocked(package_ref, options)
            .await
    }

    pub(super) async fn install_from_package_ref_with_options_unlocked(
        &self,
        package_ref: &str,
        options: PluginInstallOptions,
    ) -> Result<Vec<InstalledPlugin>> {
        ensure_secure_package_snapshot_platform()?;
        let staging_root = create_staging_root(self).await?;
        let staged_result = self
            .stage_and_commit_package(&staging_root, package_ref, options)
            .await;
        if staged_result.is_err() {
            let _ = remove_manager_owned_root(self, &staging_root).await;
        }
        staged_result
    }

    async fn stage_and_commit_package(
        &self,
        staging_root: &Path,
        package_ref: &str,
        options: PluginInstallOptions,
    ) -> Result<Vec<InstalledPlugin>> {
        let (package_root, source, source_trust, source_default_pinned) = if let Some(npm_spec) =
            package_ref.trim().strip_prefix("npm:")
        {
            let spec = npm_spec.trim();
            validate_npm_spec(spec)?;
            let npm_root = staging_root.join("npm");
            let package_root = self
                .install_npm_package(&npm_root, staging_root, spec, options.allow_package_scripts)
                .await?;
            (
                package_root,
                format!("npm:{spec}"),
                PluginSourceTrust::NpmUnsigned,
                npm_spec_is_pinned(spec),
            )
        } else {
            let source_root = PathBuf::from(package_ref);
            let source_root = if source_root.is_absolute() {
                source_root
            } else {
                std::env::current_dir()
                    .context("failed to resolve current directory for local plugin package")?
                    .join(source_root)
            };
            let source_metadata = fs::symlink_metadata(&source_root).await.with_context(|| {
                format!(
                    "failed to inspect plugin package path {}",
                    source_root.display()
                )
            })?;
            if source_metadata.file_type().is_symlink() || !source_metadata.is_dir() {
                bail!(
                        "plugin package path must be a real directory, not a symlink or other filesystem entry: {}",
                        source_root.display()
                    );
            }
            let source_identity =
                snapshot_directory_identity(&source_root, "local plugin package root").await?;
            let canonical_source_root =
                fs::canonicalize(&source_root).await.with_context(|| {
                    format!(
                        "failed to resolve verified plugin package root {}",
                        source_root.display()
                    )
                })?;
            ensure_snapshot_directory_identity(
                &source_root,
                "local plugin package root",
                source_identity,
            )
            .await?;
            let canonical_identity = snapshot_directory_identity(
                &canonical_source_root,
                "resolved local plugin package root",
            )
            .await?;
            if canonical_identity != source_identity {
                bail!(
                    "local plugin package root changed identity while being resolved: {}",
                    source_root.display()
                );
            }
            let source_root = canonical_source_root;
            let source = source_root.to_str().map(str::to_owned).with_context(|| {
                format!(
                    "canonical local package path cannot be recorded because it is not valid UTF-8: {}",
                    source_root.display()
                )
            })?;
            let package_root = staging_root.join("package");
            copy_package_snapshot(&source_root, &package_root).await?;
            (package_root, source, PluginSourceTrust::LocalUnsigned, true)
        };

        // npm may unpack dependencies outside `package_root`; validate the
        // complete transaction tree instead of only the selected package.
        validate_package_snapshot(staging_root).await?;

        let manifest_paths = self
            .discover_package_plugin_manifests(&package_root)
            .await?;
        if manifest_paths.is_empty() {
            bail!(
                "package {} does not declare any Mitsuro plugins (expected compatibility key package.json mitsuro.plugins or plugin.toml)",
                package_root.display()
            );
        }

        let mut manifests = self
            .read_package_manifests(&package_root, &manifest_paths)
            .await?;
        let missing_before_build = missing_components(&package_root, &manifests);
        if !missing_before_build.is_empty() && options.allow_package_scripts {
            self.try_build_package(&package_root, staging_root).await?;
            // Build scripts may update generated manifests as well as components.
            manifests = self
                .read_package_manifests(&package_root, &manifest_paths)
                .await?;
        }
        let missing = missing_components(&package_root, &manifests);
        if !missing.is_empty() {
            let hint = if options.allow_package_scripts {
                "the explicitly allowed build script did not produce every component"
            } else {
                "package scripts are disabled by default; build the package first or reinstall with explicit script consent"
            };
            bail!(
                "missing plugin bundle component(s): {} ({})",
                missing.join(", "),
                hint
            );
        }

        let previous_lock = load_lockfile(self).await?;
        let mut entries = Vec::with_capacity(manifests.len());
        let mut ids = BTreeSet::new();
        for (manifest_rel, manifest) in manifest_paths.into_iter().zip(manifests) {
            if !ids.insert(manifest.id.clone()) {
                bail!("package contains duplicate plugin id '{}'", manifest.id);
            }
            let existing = previous_lock
                .plugins
                .iter()
                .find(|entry| entry.id == manifest.id);
            let enabled = existing.map(|entry| entry.enabled).unwrap_or(true);
            let pinned = options
                .pinned
                .or_else(|| existing.map(|entry| entry.pinned))
                .unwrap_or(source_default_pinned);
            entries.push(PluginLockEntry {
                id: manifest.id,
                version: manifest.version,
                enabled,
                pinned,
                package_path: Some(package_root.clone()),
                manifest_path: Some(manifest_rel),
                source: Some(source.clone()),
                managed_root: Some(staging_root.to_path_buf()),
                source_trust,
                package_scripts_allowed: options.allow_package_scripts,
            });
        }

        // An explicitly allowed build can create new files after the initial
        // audit. Re-audit immediately before publication so generated
        // symlinks, special files, and quota overruns cannot enter the
        // immutable snapshot.
        validate_package_snapshot(staging_root).await?;
        commit_staged_install(self, staging_root, entries).await
    }

    async fn read_package_manifests(
        &self,
        package_root: &Path,
        manifest_paths: &[PathBuf],
    ) -> Result<Vec<PluginManifestV1>> {
        if manifest_paths.len() > MAX_PACKAGE_PLUGIN_MANIFESTS {
            bail!(
                "plugin package declares {} manifests; the limit is {}",
                manifest_paths.len(),
                MAX_PACKAGE_PLUGIN_MANIFESTS
            );
        }
        let mut manifests = Vec::with_capacity(manifest_paths.len());
        let mut aggregate_bytes = 0usize;
        for manifest_rel in manifest_paths {
            let manifest_path = package_root.join(manifest_rel);
            let aggregate_remaining = MAX_PACKAGE_MANIFEST_BYTES
                .checked_sub(aggregate_bytes)
                .context("aggregate plugin manifest size accounting underflowed")?;
            let bytes = read_package_manifest_bytes(&manifest_path, aggregate_remaining).await?;
            aggregate_bytes = aggregate_bytes
                .checked_add(bytes.len())
                .context("aggregate plugin manifest size overflowed")?;
            if aggregate_bytes > MAX_PACKAGE_MANIFEST_BYTES {
                bail!(
                    "plugin package manifests exceed aggregate size limit of {} bytes at {}",
                    MAX_PACKAGE_MANIFEST_BYTES,
                    manifest_path.display(),
                );
            }
            let manifest: PluginManifestV1 = parse_toml_or_json(&bytes)
                .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
            self.validate_manifest(&manifest, false)?;
            manifests.push(manifest);
        }
        Ok(manifests)
    }

    async fn try_build_package(&self, package_root: &Path, snapshot_root: &Path) -> Result<()> {
        let package_json = self
            .read_package_json(package_root)
            .await?
            .context("package is missing package.json and cannot run a build script")?;

        if !package_json.scripts.contains_key("build") {
            bail!(
                "plugin package {} has missing components and no build script",
                package_root.display()
            );
        }

        let mut command = Command::new("npm");
        command
            .args(["run", "build", "--ignore-scripts=false"])
            .current_dir(package_root);
        let output = run_bounded_package_command(
            command,
            PACKAGE_COMMAND_TIMEOUT,
            Some(PackageCommandSnapshotBudget::staged(snapshot_root)),
        )
        .await
        .with_context(|| {
            format!(
                "failed to execute explicitly allowed npm build script in {}",
                package_root.display()
            )
        })?;

        if output.timed_out {
            bail!(
                "npm run build timed out after {} seconds for plugin package {}: {}",
                PACKAGE_COMMAND_TIMEOUT.as_secs(),
                package_root.display(),
                format_command_output(&output)
            );
        }
        if !output.status.as_ref().is_some_and(ExitStatus::success) {
            bail!(
                "npm run build failed for plugin package {}: {}",
                package_root.display(),
                format_command_output(&output)
            );
        }

        Ok(())
    }

    async fn install_npm_package(
        &self,
        install_root: &Path,
        snapshot_root: &Path,
        spec: &str,
        allow_scripts: bool,
    ) -> Result<PathBuf> {
        self.ensure_npm_project(install_root).await?;

        let mut command = Command::new("npm");
        command.args([
            "install",
            spec,
            "--no-audit",
            "--no-fund",
            "--no-bin-links",
            "--package-lock=false",
            "--prefix",
        ]);
        command.arg(install_root);
        if !allow_scripts {
            command.arg("--ignore-scripts");
        }
        let output = run_bounded_package_command(
            command,
            PACKAGE_COMMAND_TIMEOUT,
            Some(PackageCommandSnapshotBudget::staged(snapshot_root)),
        )
        .await
        .with_context(|| "failed to execute npm; install npm or use a local plugin package")?;

        if output.timed_out {
            bail!(
                "npm install timed out after {} seconds for package spec '{}': {}",
                PACKAGE_COMMAND_TIMEOUT.as_secs(),
                spec,
                format_command_output(&output)
            );
        }
        if !output.status.as_ref().is_some_and(ExitStatus::success) {
            bail!(
                "npm install failed for package spec '{}': {}",
                spec,
                format_command_output(&output)
            );
        }

        let package_name = npm_package_name(spec);
        let package_path = install_root.join("node_modules").join(package_name);
        let metadata = fs::symlink_metadata(&package_path).await.with_context(|| {
            format!(
                "npm install completed but package directory was not found: {}",
                package_path.display()
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!(
                "npm package root must be a real directory, not a symlink or other filesystem entry: {}",
                package_path.display()
            );
        }
        Ok(package_path)
    }

    async fn ensure_npm_project(&self, install_root: &Path) -> Result<()> {
        fs::create_dir_all(install_root)
            .await
            .with_context(|| format!("failed to create {}", install_root.display()))?;
        let package_json = install_root.join("package.json");
        let content = serde_json::json!({
            "name": "mitsuro-plugin-transaction",
            "private": true
        });
        fs::write(&package_json, serde_json::to_vec_pretty(&content)?)
            .await
            .with_context(|| format!("failed to write {}", package_json.display()))?;
        Ok(())
    }

    async fn discover_package_plugin_manifests(&self, package_root: &Path) -> Result<Vec<PathBuf>> {
        if let Some(package_json) = self.read_package_json(package_root).await? {
            if let Some(mitsuro) = package_json.mitsuro {
                if mitsuro.plugins.len() > MAX_PACKAGE_PLUGIN_MANIFESTS {
                    bail!(
                        "package.json mitsuro.plugins declares {} manifests; the limit is {}",
                        mitsuro.plugins.len(),
                        MAX_PACKAGE_PLUGIN_MANIFESTS
                    );
                }
                let mut manifests = Vec::with_capacity(mitsuro.plugins.len());
                let mut seen = BTreeSet::new();
                for (index, plugin_path) in mitsuro.plugins.into_iter().enumerate() {
                    let rel = normalize_package_manifest_path(
                        &plugin_path,
                        &format!("package.json mitsuro.plugins[{}]", index),
                    )?;
                    if !seen.insert(rel.clone()) {
                        bail!(
                            "package.json mitsuro.plugins contains duplicate manifest path '{}'",
                            rel.display()
                        );
                    }
                    manifests.push(rel);
                }
                for rel in &manifests {
                    let manifest_path = package_root.join(rel);
                    let metadata = match fs::symlink_metadata(&manifest_path).await {
                        Ok(metadata) => metadata,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            bail!(
                                "declared Mitsuro plugin manifest does not exist: {}",
                                manifest_path.display()
                            )
                        }
                        Err(error) => {
                            return Err(error).with_context(|| {
                                format!(
                                    "failed to inspect declared Mitsuro plugin manifest {}",
                                    manifest_path.display()
                                )
                            })
                        }
                    };
                    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
                        bail!(
                            "declared Mitsuro plugin manifest must be a regular file: {}",
                            manifest_path.display()
                        );
                    }
                }
                if !manifests.is_empty() {
                    return Ok(manifests);
                }
            }
        }

        let default_manifest = PathBuf::from("plugin.toml");
        if package_root.join(&default_manifest).exists() {
            return Ok(vec![default_manifest]);
        }

        Ok(Vec::new())
    }

    async fn read_package_json(&self, package_root: &Path) -> Result<Option<PackageJson>> {
        let package_json_path = package_root.join("package.json");
        if !package_json_path.exists() {
            return Ok(None);
        }

        let metadata = fs::metadata(&package_json_path)
            .await
            .with_context(|| format!("failed to inspect {}", package_json_path.display()))?;
        if metadata.len() > MAX_PACKAGE_JSON_BYTES as u64 {
            bail!(
                "package.json exceeds size limit of {} bytes: {}",
                MAX_PACKAGE_JSON_BYTES,
                package_json_path.display()
            );
        }

        let file = fs::File::open(&package_json_path)
            .await
            .with_context(|| format!("failed to read {}", package_json_path.display()))?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take((MAX_PACKAGE_JSON_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .with_context(|| format!("failed to read {}", package_json_path.display()))?;
        if bytes.len() > MAX_PACKAGE_JSON_BYTES {
            bail!(
                "package.json exceeds size limit of {} bytes: {}",
                MAX_PACKAGE_JSON_BYTES,
                package_json_path.display()
            );
        }
        let package_json: PackageJson = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse {}", package_json_path.display()))?;
        Ok(Some(package_json))
    }
}

/// Local and npm packages are unsigned, so their security boundary is the
/// immutable filesystem snapshot itself. The current implementation relies on
/// Unix `O_NOFOLLOW`, stable device/inode identities, and link counts. Rust's
/// stable non-Unix filesystem API does not expose an equivalent complete set,
/// so package installs fail before staging instead of silently weakening the
/// snapshot contract. Signed standalone manifests use a separate authenticated
/// artifact path and remain available.
fn ensure_secure_package_snapshot_platform() -> Result<()> {
    if cfg!(unix) {
        Ok(())
    } else {
        bail!(
            "local and npm plugin package installation is unavailable on this platform because \
             Mitsuro cannot enforce its no-follow, stable-identity, and hard-link snapshot \
             guarantees; install a signed standalone plugin manifest instead"
        )
    }
}

fn normalize_package_manifest_path(path: &str, label: &str) -> Result<PathBuf> {
    let validated = validate_relative_path_for(path, label)?;
    let mut normalized = PathBuf::new();
    for component in validated.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => normalized.push(segment),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                unreachable!("validate_relative_path_for rejects escaping components")
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        bail!("{} must identify a manifest file", label);
    }
    Ok(normalized)
}

async fn read_package_manifest_bytes(path: &Path, aggregate_remaining: usize) -> Result<Vec<u8>> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .await
        .with_context(|| format!("failed to open plugin manifest {}", path.display()))?;
    let metadata = file
        .metadata()
        .await
        .with_context(|| format!("failed to inspect plugin manifest {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("plugin manifest is not a regular file: {}", path.display());
    }
    if metadata.len() > MAX_MANIFEST_BYTES as u64 {
        bail!(
            "plugin manifest exceeds size limit of {} bytes: {}",
            MAX_MANIFEST_BYTES,
            path.display()
        );
    }
    if metadata.len() > aggregate_remaining as u64 {
        bail!(
            "plugin package manifests exceed aggregate size limit of {} bytes at {}",
            MAX_PACKAGE_MANIFEST_BYTES,
            path.display()
        );
    }

    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    let read_limit = MAX_MANIFEST_BYTES
        .min(aggregate_remaining)
        .saturating_add(1);
    file.take(read_limit as u64)
        .read_to_end(&mut bytes)
        .await
        .with_context(|| format!("failed to read plugin manifest {}", path.display()))?;
    if bytes.len() > MAX_MANIFEST_BYTES {
        bail!(
            "plugin manifest exceeds size limit of {} bytes: {}",
            MAX_MANIFEST_BYTES,
            path.display()
        );
    }
    if bytes.len() > aggregate_remaining {
        bail!(
            "plugin package manifests exceed aggregate size limit of {} bytes at {}",
            MAX_PACKAGE_MANIFEST_BYTES,
            path.display()
        );
    }
    Ok(bytes)
}

fn manifest_component_paths(manifest: &PluginManifestV1) -> Vec<(&str, &str)> {
    let mut paths = Vec::new();
    if let Some(path) = manifest.entry_component.as_deref() {
        paths.push(("entry_component", path));
    }
    paths.extend(manifest.skills.iter().map(|path| ("skills", path.as_str())));
    paths.extend(
        manifest
            .agent_extensions
            .iter()
            .map(|path| ("agent_extensions", path.as_str())),
    );
    if let Some(path) = manifest.mcp_servers.as_deref() {
        paths.push(("mcp_servers", path));
    }
    paths.extend(manifest.hooks.iter().map(|path| ("hooks", path.as_str())));
    if let Some(path) = manifest.assets.as_deref() {
        paths.push(("assets", path));
    }
    paths
}

fn missing_components(package_root: &Path, manifests: &[PluginManifestV1]) -> Vec<String> {
    manifests
        .iter()
        .flat_map(|manifest| {
            manifest_component_paths(manifest)
                .into_iter()
                .filter(|(_, path)| !package_root.join(path).exists())
                .map(|(kind, path)| format!("{}:{}={}", manifest.id, kind, path))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SnapshotDirectoryIdentity {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SnapshotDirectoryIdentity;

async fn copy_package_snapshot(source_root: &Path, target_root: &Path) -> Result<()> {
    validate_package_snapshot_with_limits(source_root, PACKAGE_SNAPSHOT_LIMITS, "local").await?;
    let source_identity = snapshot_directory_identity(source_root, "local package root").await?;
    fs::create_dir(target_root)
        .await
        .with_context(|| format!("failed to create snapshot root {}", target_root.display()))?;
    let target_identity = snapshot_directory_identity(target_root, "staged package root").await?;
    let mut pending = vec![(
        source_root.to_path_buf(),
        target_root.to_path_buf(),
        source_identity,
        target_identity,
    )];
    let mut copied_entries = 1usize;
    let mut copied_bytes = 0u64;

    while let Some((source_dir, target_dir, source_identity, target_identity)) = pending.pop() {
        ensure_snapshot_directory_identity(&source_dir, "local package directory", source_identity)
            .await?;
        ensure_snapshot_directory_identity(
            &target_dir,
            "staged package directory",
            target_identity,
        )
        .await?;
        let entries =
            read_sorted_directory_entries(&source_dir, MAX_PACKAGE_SNAPSHOT_ENTRIES).await?;
        let mut child_directories = Vec::new();
        for entry in entries {
            let source_path = entry.path();
            let target_path = target_dir.join(entry.file_name());
            let file_type = entry.file_type().await?;
            if file_type.is_symlink() {
                bail!(
                    "local plugin packages may not contain symlinks: {}",
                    source_path.display()
                );
            }

            copied_entries = copied_entries
                .checked_add(1)
                .context("local plugin package entry count overflowed while creating snapshot")?;
            if copied_entries > MAX_PACKAGE_SNAPSHOT_ENTRIES {
                bail!(
                    "local plugin package exceeds snapshot entry limit of {} at {}",
                    MAX_PACKAGE_SNAPSHOT_ENTRIES,
                    source_path.display()
                );
            }
            if file_type.is_dir() {
                let source_child_identity =
                    snapshot_directory_identity(&source_path, "local package directory").await?;
                fs::create_dir(&target_path).await.with_context(|| {
                    format!(
                        "failed to create staged package directory {}",
                        target_path.display()
                    )
                })?;
                let target_child_identity =
                    snapshot_directory_identity(&target_path, "staged package directory").await?;
                child_directories.push((
                    source_path,
                    target_path,
                    source_child_identity,
                    target_child_identity,
                ));
                continue;
            }
            if !file_type.is_file() {
                bail!(
                    "local plugin package contains unsupported filesystem entry: {}",
                    source_path.display()
                );
            }

            let metadata = fs::symlink_metadata(&source_path).await?;
            if !metadata.file_type().is_file() {
                bail!(
                    "local plugin package entry changed type while being snapshotted: {}",
                    source_path.display()
                );
            }
            reject_multiply_linked_file(&metadata, &source_path, "local")?;
            if metadata.len() > MAX_PACKAGE_SNAPSHOT_FILE_BYTES {
                bail!(
                    "local plugin package file exceeds per-file snapshot limit of {} bytes: {}",
                    MAX_PACKAGE_SNAPSHOT_FILE_BYTES,
                    source_path.display()
                );
            }
            copied_bytes = copied_bytes
                .checked_add(metadata.len())
                .context("local plugin package byte count overflowed while creating snapshot")?;
            if copied_bytes > MAX_PACKAGE_SNAPSHOT_BYTES {
                bail!(
                    "local plugin package exceeds aggregate snapshot limit of {} bytes at {}",
                    MAX_PACKAGE_SNAPSHOT_BYTES,
                    source_path.display()
                );
            }
            copy_regular_file_nofollow(
                &source_path,
                &target_path,
                metadata,
                source_identity,
                target_identity,
            )
            .await?;
        }
        ensure_snapshot_directory_identity(&source_dir, "local package directory", source_identity)
            .await?;
        ensure_snapshot_directory_identity(
            &target_dir,
            "staged package directory",
            target_identity,
        )
        .await?;
        // The stack is LIFO, so reverse the sorted children to preserve a
        // deterministic lexical traversal.
        pending.extend(child_directories.into_iter().rev());
    }
    Ok(())
}

async fn snapshot_directory_identity(
    path: &Path,
    description: &str,
) -> Result<SnapshotDirectoryIdentity> {
    let metadata = fs::symlink_metadata(path)
        .await
        .with_context(|| format!("failed to inspect {description} {}", path.display()))?;
    snapshot_directory_identity_from_metadata(path, description, &metadata)
}

fn snapshot_directory_identity_sync(
    path: &Path,
    description: &str,
) -> Result<SnapshotDirectoryIdentity> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {description} {}", path.display()))?;
    snapshot_directory_identity_from_metadata(path, description, &metadata)
}

fn snapshot_directory_identity_from_metadata(
    path: &Path,
    description: &str,
    metadata: &std::fs::Metadata,
) -> Result<SnapshotDirectoryIdentity> {
    let file_type = metadata.file_type();
    if file_type.is_symlink() || !file_type.is_dir() {
        bail!(
            "{description} changed to a symlink or non-directory while creating a snapshot: {}",
            path.display()
        );
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok(SnapshotDirectoryIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(SnapshotDirectoryIdentity)
    }
}

async fn ensure_snapshot_directory_identity(
    path: &Path,
    description: &str,
    expected: SnapshotDirectoryIdentity,
) -> Result<()> {
    let actual = snapshot_directory_identity(path, description).await?;
    if actual != expected {
        bail!(
            "{description} changed identity while creating a snapshot: {}",
            path.display()
        );
    }
    Ok(())
}

fn ensure_snapshot_directory_identity_sync(
    path: &Path,
    description: &str,
    expected: SnapshotDirectoryIdentity,
) -> Result<()> {
    let actual = snapshot_directory_identity_sync(path, description)?;
    if actual != expected {
        bail!(
            "{description} changed identity while creating a snapshot: {}",
            path.display()
        );
    }
    Ok(())
}

async fn copy_regular_file_nofollow(
    source_path: &Path,
    target_path: &Path,
    expected_metadata: std::fs::Metadata,
    source_parent_identity: SnapshotDirectoryIdentity,
    target_parent_identity: SnapshotDirectoryIdentity,
) -> Result<()> {
    let source_path = source_path.to_path_buf();
    let target_path = target_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<()> {
            let source_parent = source_path
                .parent()
                .context("local package file has no parent directory")?;
            let target_parent = target_path
                .parent()
                .context("staged package file has no parent directory")?;
            ensure_snapshot_directory_identity_sync(
                source_parent,
                "local package directory",
                source_parent_identity,
            )?;
            ensure_snapshot_directory_identity_sync(
                target_parent,
                "staged package directory",
                target_parent_identity,
            )?;

            let path_metadata = std::fs::symlink_metadata(&source_path).with_context(|| {
                format!(
                    "failed to inspect local package file {}",
                    source_path.display()
                )
            })?;
            validate_snapshot_regular_file(&path_metadata, &source_path, "local")?;
            ensure_file_version_unchanged(
                &expected_metadata,
                &path_metadata,
                &source_path,
                "local plugin package file",
                "before opening",
            )?;

            let mut source_options = std::fs::OpenOptions::new();
            source_options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                source_options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
            }
            let mut source_file = source_options.open(&source_path).with_context(|| {
                format!(
                    "failed to open local package file without following links {}",
                    source_path.display()
                )
            })?;
            let opened_metadata = source_file.metadata().with_context(|| {
                format!(
                    "failed to inspect opened package file {}",
                    source_path.display()
                )
            })?;
            validate_snapshot_regular_file(&opened_metadata, &source_path, "local")?;
            ensure_file_version_unchanged(
                &expected_metadata,
                &opened_metadata,
                &source_path,
                "local plugin package file",
                "while opening",
            )?;

            // Recheck both parents immediately before creating the destination.
            // `create_new` rejects a final-component symlink; the parent identity
            // checks catch a directory replaced during traversal.
            ensure_snapshot_directory_identity_sync(
                source_parent,
                "local package directory",
                source_parent_identity,
            )?;
            ensure_snapshot_directory_identity_sync(
                target_parent,
                "staged package directory",
                target_parent_identity,
            )?;
            let mut target_options = std::fs::OpenOptions::new();
            target_options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                target_options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
            }
            let mut target_file = target_options.open(&target_path).with_context(|| {
                format!(
                    "failed to create staged package file {}",
                    target_path.display()
                )
            })?;

            use std::io::Read as _;
            let mut limited = (&mut source_file).take(expected_metadata.len().saturating_add(1));
            let copied = std::io::copy(&mut limited, &mut target_file).with_context(|| {
                format!(
                    "failed to snapshot plugin package file {}",
                    source_path.display()
                )
            })?;
            if copied != expected_metadata.len() {
                bail!(
                    "local plugin package file changed size while being snapshotted: {}",
                    source_path.display()
                );
            }
            #[cfg(unix)]
            let copied_permissions = {
                use std::os::unix::fs::PermissionsExt as _;
                // Preserve executable components while stripping special bits
                // that have no place in a user-owned plugin snapshot.
                std::fs::Permissions::from_mode(expected_metadata.permissions().mode() & 0o777)
            };
            #[cfg(not(unix))]
            let copied_permissions = expected_metadata.permissions();
            target_file
                .set_permissions(copied_permissions)
                .with_context(|| {
                    format!(
                        "failed to set staged package file permissions {}",
                        target_path.display()
                    )
                })?;
            target_file.sync_all().with_context(|| {
                format!(
                    "failed to sync staged package file {}",
                    target_path.display()
                )
            })?;

            let final_source_metadata = source_file.metadata().with_context(|| {
                format!(
                    "failed to re-inspect package file {}",
                    source_path.display()
                )
            })?;
            validate_snapshot_regular_file(&final_source_metadata, &source_path, "local")?;
            ensure_file_version_unchanged(
                &expected_metadata,
                &final_source_metadata,
                &source_path,
                "local plugin package file",
                "during copy",
            )?;
            let target_metadata = target_file.metadata().with_context(|| {
                format!(
                    "failed to inspect staged package file {}",
                    target_path.display()
                )
            })?;
            validate_snapshot_regular_file(&target_metadata, &target_path, "staged")?;
            if target_metadata.len() != expected_metadata.len() {
                bail!(
                    "staged plugin package file has an unexpected size after copy: {}",
                    target_path.display()
                );
            }
            let target_path_metadata =
                std::fs::symlink_metadata(&target_path).with_context(|| {
                    format!(
                        "failed to inspect staged package path {}",
                        target_path.display()
                    )
                })?;
            validate_snapshot_regular_file(&target_path_metadata, &target_path, "staged")?;
            ensure_file_version_unchanged(
                &target_metadata,
                &target_path_metadata,
                &target_path,
                "staged plugin package file",
                "after copy",
            )?;
            ensure_snapshot_directory_identity_sync(
                source_parent,
                "local package directory",
                source_parent_identity,
            )?;
            ensure_snapshot_directory_identity_sync(
                target_parent,
                "staged package directory",
                target_parent_identity,
            )?;
            Ok(())
        })();

        if result.is_err() {
            // Remove only the destination entry. `remove_file` does not follow
            // a symlink if an attacker replaces it while the copy is failing.
            // If the parent itself changed identity, leave the path untouched:
            // it may now name an unrelated external directory.
            if target_path.parent().is_some_and(|target_parent| {
                ensure_snapshot_directory_identity_sync(
                    target_parent,
                    "staged package directory",
                    target_parent_identity,
                )
                .is_ok()
            }) {
                let _ = std::fs::remove_file(&target_path);
            }
        }
        result
    })
    .await
    .context("local plugin package snapshot copy task failed")?
}

fn validate_snapshot_regular_file(
    metadata: &std::fs::Metadata,
    path: &Path,
    snapshot_kind: &str,
) -> Result<()> {
    if !metadata.file_type().is_file() {
        bail!(
            "{snapshot_kind} plugin package entry changed type while being inspected: {}",
            path.display()
        );
    }
    reject_multiply_linked_file(metadata, path, snapshot_kind)
}

fn reject_multiply_linked_file(
    metadata: &std::fs::Metadata,
    path: &Path,
    snapshot_kind: &str,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            bail!(
                "{snapshot_kind} plugin package snapshots may not contain multiply-linked regular files: {}",
                path.display()
            );
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (metadata, path, snapshot_kind);
    }
    Ok(())
}

fn ensure_file_version_unchanged(
    expected: &std::fs::Metadata,
    actual: &std::fs::Metadata,
    path: &Path,
    description: &str,
    phase: &str,
) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let unchanged = expected.dev() == actual.dev()
            && expected.ino() == actual.ino()
            && expected.len() == actual.len()
            && expected.nlink() == actual.nlink()
            && expected.mtime() == actual.mtime()
            && expected.mtime_nsec() == actual.mtime_nsec()
            && expected.ctime() == actual.ctime()
            && expected.ctime_nsec() == actual.ctime_nsec();
        if !unchanged {
            bail!(
                "{description} changed identity or contents {phase}: {}",
                path.display()
            );
        }
    }
    #[cfg(not(unix))]
    {
        if expected.len() != actual.len() || expected.modified().ok() != actual.modified().ok() {
            bail!(
                "{description} changed identity or contents {phase}: {}",
                path.display()
            );
        }
    }
    Ok(())
}

async fn read_sorted_directory_entries(
    directory: &Path,
    max_snapshot_entries: usize,
) -> Result<Vec<fs::DirEntry>> {
    let mut directory_entries = fs::read_dir(directory)
        .await
        .with_context(|| format!("failed to read package directory {}", directory.display()))?;
    let mut entries = Vec::new();
    while let Some(entry) = directory_entries.next_entry().await? {
        entries.push(entry);
        if entries.len() > max_snapshot_entries {
            bail!(
                "plugin package snapshot exceeds entry limit of {} while reading {}",
                max_snapshot_entries,
                directory.display()
            );
        }
    }
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

async fn validate_package_snapshot(root: &Path) -> Result<()> {
    validate_package_snapshot_with_limits(root, PACKAGE_SNAPSHOT_LIMITS, "staged").await
}

async fn wait_for_live_package_snapshot_violation(
    budget: &PackageCommandSnapshotBudget,
    expected_identity: SnapshotDirectoryIdentity,
) -> anyhow::Error {
    let first_poll = tokio::time::Instant::now() + PACKAGE_COMMAND_SNAPSHOT_POLL_INTERVAL;
    let mut poll = tokio::time::interval_at(first_poll, PACKAGE_COMMAND_SNAPSHOT_POLL_INTERVAL);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        poll.tick().await;
        if let Err(error) =
            validate_live_package_snapshot_budget(&budget.root, budget.limits, expected_identity)
                .await
        {
            return error;
        }
    }
}

/// Inspect a snapshot while npm is actively mutating it. Entries that vanish
/// between `read_dir` and `symlink_metadata` are normal npm rename/unlink
/// transients, so this pass ignores only `NotFound` races. The strict,
/// immutable snapshot validator still runs on the post-command publication
/// path.
async fn validate_live_package_snapshot_budget(
    root: &Path,
    limits: PackageSnapshotLimits,
    expected_identity: SnapshotDirectoryIdentity,
) -> Result<()> {
    let root_metadata = fs::symlink_metadata(root).await.with_context(|| {
        format!(
            "failed to inspect live plugin package snapshot {}",
            root.display()
        )
    })?;
    let root_type = root_metadata.file_type();
    if root_type.is_symlink() || !root_type.is_dir() {
        bail!(
            "live plugin package snapshot root must be a real directory: {}",
            root.display()
        );
    }
    let actual_identity = snapshot_directory_identity_from_metadata(
        root,
        "live plugin package snapshot root",
        &root_metadata,
    )?;
    if actual_identity != expected_identity {
        bail!(
            "live plugin package snapshot root changed identity while a package command was running: {}",
            root.display()
        );
    }
    if limits.max_entries == 0 {
        bail!(
            "live plugin package exceeds snapshot entry limit of 0 at {}",
            root.display()
        );
    }

    let mut inspected_entries = 1usize;
    let mut inspected_bytes = 0u64;
    let mut pending = vec![root.to_path_buf()];

    while let Some(directory) = pending.pop() {
        let mut entries = match fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to read live plugin package directory {}",
                        directory.display()
                    )
                })
            }
        };
        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to enumerate live plugin package directory {}",
                            directory.display()
                        )
                    })
                }
            };
            let path = entry.path();
            inspected_entries = inspected_entries.checked_add(1).with_context(|| {
                format!(
                    "live plugin package entry count overflowed at {}",
                    path.display()
                )
            })?;
            if inspected_entries > limits.max_entries {
                bail!(
                    "live plugin package exceeds snapshot entry limit of {} at {}",
                    limits.max_entries,
                    path.display()
                );
            }

            let metadata = match fs::symlink_metadata(&path).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to inspect live plugin package entry {}",
                            path.display()
                        )
                    })
                }
            };
            let file_type = metadata.file_type();
            if file_type.is_dir() {
                pending.push(path);
                continue;
            }
            if !file_type.is_file() {
                // This pass enforces only the live byte/entry budget. The
                // strict post-command audit rejects symlinks and special files.
                continue;
            }
            if metadata.len() > limits.max_file_bytes {
                bail!(
                    "live plugin package file exceeds per-file snapshot limit of {} bytes: {}",
                    limits.max_file_bytes,
                    path.display()
                );
            }
            inspected_bytes = inspected_bytes
                .checked_add(metadata.len())
                .with_context(|| {
                    format!(
                        "live plugin package byte count overflowed at {}",
                        path.display()
                    )
                })?;
            if inspected_bytes > limits.max_total_bytes {
                bail!(
                    "live plugin package exceeds aggregate snapshot limit of {} bytes at {}",
                    limits.max_total_bytes,
                    path.display()
                );
            }
        }
    }

    ensure_snapshot_directory_identity(
        root,
        "live plugin package snapshot root",
        expected_identity,
    )
    .await?;

    Ok(())
}

async fn validate_package_snapshot_with_limits(
    root: &Path,
    limits: PackageSnapshotLimits,
    snapshot_kind: &str,
) -> Result<()> {
    let root_metadata = fs::symlink_metadata(root).await.with_context(|| {
        format!(
            "failed to inspect plugin package snapshot {}",
            root.display()
        )
    })?;
    let root_type = root_metadata.file_type();
    if root_type.is_symlink() || !root_type.is_dir() {
        bail!(
            "{} plugin package snapshot root must be a real directory: {}",
            snapshot_kind,
            root.display()
        );
    }
    if limits.max_entries == 0 {
        bail!(
            "{} plugin package exceeds snapshot entry limit of 0 at {}",
            snapshot_kind,
            root.display()
        );
    }

    let mut inspected_entries = 1usize;
    let mut inspected_bytes = 0u64;
    let mut pending = vec![root.to_path_buf()];

    while let Some(directory) = pending.pop() {
        let entries = read_sorted_directory_entries(&directory, limits.max_entries).await?;
        let mut child_directories = Vec::new();
        for entry in entries {
            let path = entry.path();
            inspected_entries = inspected_entries.checked_add(1).with_context(|| {
                format!(
                    "{} plugin package entry count overflowed at {}",
                    snapshot_kind,
                    path.display()
                )
            })?;
            if inspected_entries > limits.max_entries {
                bail!(
                    "{} plugin package exceeds snapshot entry limit of {} at {}",
                    snapshot_kind,
                    limits.max_entries,
                    path.display()
                );
            }

            let file_type = entry.file_type().await.with_context(|| {
                format!("failed to inspect plugin package entry {}", path.display())
            })?;
            if file_type.is_symlink() {
                bail!(
                    "{} plugin package snapshots may not contain symlinks: {}",
                    snapshot_kind,
                    path.display()
                );
            }
            if file_type.is_dir() {
                child_directories.push(path);
                continue;
            }
            if !file_type.is_file() {
                bail!(
                    "{} plugin package snapshot contains unsupported filesystem entry: {}",
                    snapshot_kind,
                    path.display()
                );
            }

            let metadata = fs::symlink_metadata(&path).await.with_context(|| {
                format!("failed to inspect plugin package file {}", path.display())
            })?;
            if !metadata.file_type().is_file() {
                bail!(
                    "{} plugin package entry changed type while being inspected: {}",
                    snapshot_kind,
                    path.display()
                );
            }
            reject_multiply_linked_file(&metadata, &path, snapshot_kind)?;
            if metadata.len() > limits.max_file_bytes {
                bail!(
                    "{} plugin package file exceeds per-file snapshot limit of {} bytes: {}",
                    snapshot_kind,
                    limits.max_file_bytes,
                    path.display()
                );
            }
            inspected_bytes = inspected_bytes
                .checked_add(metadata.len())
                .with_context(|| {
                    format!(
                        "{} plugin package byte count overflowed at {}",
                        snapshot_kind,
                        path.display()
                    )
                })?;
            if inspected_bytes > limits.max_total_bytes {
                bail!(
                    "{} plugin package exceeds aggregate snapshot limit of {} bytes at {}",
                    snapshot_kind,
                    limits.max_total_bytes,
                    path.display()
                );
            }
        }
        pending.extend(child_directories.into_iter().rev());
    }

    Ok(())
}

fn validate_npm_spec(spec: &str) -> Result<()> {
    if spec.is_empty() {
        bail!("npm package spec cannot be empty");
    }
    if spec.starts_with('-') || spec.chars().any(|ch| ch.is_control() || ch.is_whitespace()) {
        bail!(
            "invalid npm package spec '{}': options and whitespace are not allowed",
            spec
        );
    }
    Ok(())
}

fn format_command_output(output: &PackageCommandOutput) -> String {
    let mut message = if output.timed_out {
        "timed out".to_string()
    } else if let Some(status) = output.status {
        format!("status {status}")
    } else {
        "status unavailable".to_string()
    };
    if !output.stdout.bytes.is_empty()
        || output.stdout.dropped_bytes > 0
        || output.stdout.incomplete
    {
        message.push_str("; stdout: ");
        message.push_str(&format_command_stream(&output.stdout));
    }
    if !output.stderr.bytes.is_empty()
        || output.stderr.dropped_bytes > 0
        || output.stderr.incomplete
    {
        message.push_str("; stderr: ");
        message.push_str(&format_command_stream(&output.stderr));
    }
    message
}

fn format_command_stream(stream: &BoundedCommandStream) -> String {
    let mut message = String::new();
    if stream.dropped_bytes > 0 {
        message.push_str(&format!(
            "[... omitted {} earlier byte(s) ...] ",
            stream.dropped_bytes
        ));
    }
    message.push_str(String::from_utf8_lossy(&stream.bytes).trim());
    if stream.incomplete {
        if !message.is_empty() {
            message.push(' ');
        }
        message.push_str("[output drain incomplete]");
    }
    message
}

fn npm_package_name(spec: &str) -> &str {
    if let Some(scoped) = spec.strip_prefix('@') {
        let mut parts = scoped.splitn(3, '@');
        let scope_and_name = parts.next().unwrap_or(scoped);
        return &spec[..scope_and_name.len() + 1];
    }

    spec.split('@').next().unwrap_or(spec)
}

fn npm_spec_version(spec: &str) -> Option<&str> {
    if let Some(scoped) = spec.strip_prefix('@') {
        scoped.split_once('@').map(|(_, version)| version)
    } else {
        spec.split_once('@').map(|(_, version)| version)
    }
}

fn npm_spec_is_pinned(spec: &str) -> bool {
    npm_spec_version(spec)
        .and_then(|version| semver::Version::parse(version).ok())
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn secure_package_snapshots_are_enabled_on_unix() {
        ensure_secure_package_snapshot_platform().expect("Unix snapshot primitives are available");
    }

    #[cfg(not(unix))]
    #[tokio::test]
    async fn unsigned_package_snapshots_fail_before_staging_on_unsupported_platforms() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let manager = PluginManager::new(reqwest::Client::new(), temp.path().join("plugins"));
        let error = manager
            .install_from_package_ref_with_options_unlocked(
                "npm:example@1.0.0",
                PluginInstallOptions::default(),
            )
            .await
            .expect_err("unsigned package snapshots must not use weaker filesystem semantics");

        let message = error.to_string();
        assert!(message.contains("local and npm plugin package installation is unavailable"));
        assert!(message.contains("signed standalone plugin manifest"));
        assert!(
            !manager.staging_root().exists(),
            "the unsupported package path must fail before staging"
        );
    }

    #[test]
    fn parses_npm_package_names() {
        assert_eq!(npm_package_name("left-pad"), "left-pad");
        assert_eq!(npm_package_name("left-pad@1.0.0"), "left-pad");
        assert_eq!(npm_package_name("@scope/plugin"), "@scope/plugin");
        assert_eq!(npm_package_name("@scope/plugin@1.0.0"), "@scope/plugin");
    }

    #[test]
    fn detects_only_exact_semver_npm_specs_as_pinned() {
        assert!(!npm_spec_is_pinned("left-pad"));
        assert!(npm_spec_is_pinned("left-pad@1.0.0"));
        assert!(!npm_spec_is_pinned("left-pad@latest"));
        assert!(!npm_spec_is_pinned("@scope/plugin"));
        assert!(npm_spec_is_pinned("@scope/plugin@1.0.0"));
    }

    #[test]
    fn bounded_command_stream_keeps_the_most_recent_bytes() {
        let mut stream = BoundedCommandStream::default();
        stream.push(&vec![b'a'; MAX_PACKAGE_COMMAND_STREAM_BYTES]);
        stream.push(b"useful-tail");

        assert_eq!(stream.bytes.len(), MAX_PACKAGE_COMMAND_STREAM_BYTES);
        assert_eq!(stream.dropped_bytes, b"useful-tail".len());
        assert!(stream.bytes.ends_with(b"useful-tail"));
        assert!(format_command_stream(&stream).contains("omitted 11 earlier byte(s)"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn package_command_runner_bounds_both_output_streams() {
        let mut command = Command::new("sh");
        command.arg("-c").arg(
            r#"awk 'BEGIN { for (i = 0; i < 50000; i++) printf "o"; printf "stdout-tail" }'; awk 'BEGIN { for (i = 0; i < 50000; i++) printf "e"; printf "stderr-tail" }' >&2; exit 7"#,
        );

        let output = run_bounded_package_command(command, Duration::from_secs(5), None)
            .await
            .expect("run bounded command");

        assert!(!output.timed_out);
        assert_eq!(output.status.and_then(|status| status.code()), Some(7));
        assert_eq!(output.stdout.bytes.len(), MAX_PACKAGE_COMMAND_STREAM_BYTES);
        assert_eq!(output.stderr.bytes.len(), MAX_PACKAGE_COMMAND_STREAM_BYTES);
        assert!(output.stdout.dropped_bytes > 0);
        assert!(output.stderr.dropped_bytes > 0);
        assert!(output.stdout.bytes.ends_with(b"stdout-tail"));
        assert!(output.stderr.bytes.ends_with(b"stderr-tail"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn package_command_timeout_kills_descendants() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("(sleep 1; touch survived) & wait")
            .current_dir(temp.path());

        let started = std::time::Instant::now();
        let output = run_bounded_package_command(command, Duration::from_millis(100), None)
            .await
            .expect("time out command");

        assert!(output.timed_out);
        assert!(started.elapsed() < Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(
            !temp.path().join("survived").exists(),
            "a timed-out package command left a descendant running"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn live_snapshot_budget_kills_the_command_and_its_descendants() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(
                "(sleep 1; touch survived) & \
                 i=0; while [ $i -lt 200 ]; do \
                   printf 1234567890 >> growing; \
                   i=$((i + 1)); sleep 0.01; \
                 done; wait",
            )
            .current_dir(temp.path());
        let budget = PackageCommandSnapshotBudget {
            root: temp.path().to_path_buf(),
            limits: snapshot_limits(8, 4_096, 100),
        };

        let started = std::time::Instant::now();
        let error = run_bounded_package_command(command, Duration::from_secs(5), Some(budget))
            .await
            .expect_err("the live per-file quota must terminate the command");

        let message = error.to_string();
        assert!(
            message.contains("exceeded its live snapshot budget"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("per-file snapshot limit of 100 bytes"),
            "unexpected error: {message}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(
            !temp.path().join("survived").exists(),
            "a quota-terminated package command left a descendant running"
        );
    }

    #[tokio::test]
    async fn package_json_is_rejected_before_unbounded_read_or_parse() {
        let temp = tempfile::tempdir().expect("temporary directory");
        fs::write(
            temp.path().join("package.json"),
            vec![b' '; MAX_PACKAGE_JSON_BYTES + 1],
        )
        .await
        .expect("write oversized package.json");
        let manager = PluginManager::new(reqwest::Client::new(), temp.path().join("plugins"));

        let error = manager
            .read_package_json(temp.path())
            .await
            .expect_err("oversized package.json must fail");

        assert!(error
            .to_string()
            .contains("package.json exceeds size limit"));
    }

    fn snapshot_limits(
        max_entries: usize,
        max_total_bytes: u64,
        max_file_bytes: u64,
    ) -> PackageSnapshotLimits {
        PackageSnapshotLimits {
            max_entries,
            max_total_bytes,
            max_file_bytes,
        }
    }

    #[tokio::test]
    async fn snapshot_entry_limit_counts_root_directories_and_files_deterministically() {
        let temp = tempfile::tempdir().expect("temporary directory");
        fs::create_dir(temp.path().join("a-directory"))
            .await
            .expect("create directory");
        fs::write(temp.path().join("b-file"), b"x")
            .await
            .expect("write file");

        validate_package_snapshot_with_limits(temp.path(), snapshot_limits(3, 16, 16), "test")
            .await
            .expect("root plus two entries should fit");

        let error =
            validate_package_snapshot_with_limits(temp.path(), snapshot_limits(2, 16, 16), "test")
                .await
                .expect_err("the third inode must exceed the entry limit");
        let message = error.to_string();
        assert!(message.contains("snapshot entry limit of 2"));
        assert!(message.contains("b-file"), "unexpected error: {message}");
    }

    #[tokio::test]
    async fn snapshot_rejects_aggregate_byte_limit() {
        let temp = tempfile::tempdir().expect("temporary directory");
        fs::write(temp.path().join("a"), b"123")
            .await
            .expect("write first file");
        fs::write(temp.path().join("b"), b"456")
            .await
            .expect("write second file");

        let error =
            validate_package_snapshot_with_limits(temp.path(), snapshot_limits(3, 5, 3), "test")
                .await
                .expect_err("aggregate quota must reject the second file");

        assert!(error
            .to_string()
            .contains("aggregate snapshot limit of 5 bytes"));
    }

    #[tokio::test]
    async fn snapshot_rejects_per_file_byte_limit() {
        let temp = tempfile::tempdir().expect("temporary directory");
        fs::write(temp.path().join("large"), b"1234")
            .await
            .expect("write file");

        let error =
            validate_package_snapshot_with_limits(temp.path(), snapshot_limits(2, 16, 3), "test")
                .await
                .expect_err("per-file quota must reject the file");

        assert!(error
            .to_string()
            .contains("per-file snapshot limit of 3 bytes"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn snapshot_rejects_symlinks_and_special_files() {
        use std::os::unix::{fs::symlink, net::UnixListener};

        let symlink_temp = tempfile::tempdir().expect("temporary directory");
        fs::write(symlink_temp.path().join("target"), b"content")
            .await
            .expect("write target");
        symlink("target", symlink_temp.path().join("link")).expect("create symlink");
        let symlink_error = validate_package_snapshot_with_limits(
            symlink_temp.path(),
            snapshot_limits(3, 16, 16),
            "test",
        )
        .await
        .expect_err("symlink must be rejected");
        assert!(symlink_error
            .to_string()
            .contains("may not contain symlinks"));

        let socket_temp = tempfile::tempdir().expect("temporary directory");
        let _listener =
            UnixListener::bind(socket_temp.path().join("socket")).expect("create unix socket");
        let socket_error = validate_package_snapshot_with_limits(
            socket_temp.path(),
            snapshot_limits(2, 16, 16),
            "test",
        )
        .await
        .expect_err("socket must be rejected");
        assert!(socket_error
            .to_string()
            .contains("unsupported filesystem entry"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn snapshot_rejects_multiply_linked_regular_files() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let original = temp.path().join("original");
        fs::write(&original, b"content")
            .await
            .expect("write original");
        std::fs::hard_link(&original, temp.path().join("alias")).expect("create hard link");

        let error =
            validate_package_snapshot_with_limits(temp.path(), snapshot_limits(3, 32, 16), "test")
                .await
                .expect_err("multiply-linked files must be rejected");

        assert!(error
            .to_string()
            .contains("may not contain multiply-linked regular files"));
    }
}
