use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock};

use super::environment::CommandEnvironment;
use super::model::{
    elapsed_millis_u64, ProcessCompletionEvent, ProcessEntry, ProcessId, ProcessInfo,
    ProcessOutputBuffer, ProcessStatus,
};
use super::signals::{resume_process_tree, suspend_process_tree, terminate_process_tree};

/// Default user ID for single-tenant mode
const DEFAULT_USER: &str = "default";
/// Hard resource boundary for one owner. Unlimited agent turns must never
/// imply unlimited child processes; completed/killed entries do not count.
pub(crate) const MAX_ACTIVE_PROCESSES_PER_OWNER: usize = 16;
/// Retain a bounded diagnostic tail for completed, failed, and killed
/// processes. Active entries are never evicted by history pruning.
pub(crate) const MAX_TERMINAL_PROCESSES_PER_OWNER: usize = 64;
const STDERR_TAIL_BYTES: usize = 8 * 1024;
const OUTPUT_TAIL_BYTES: usize = 64 * 1024;

fn append_stderr_tail(tail: &mut Vec<u8>, bytes: &[u8]) {
    tail.extend_from_slice(bytes);
    if tail.len() > STDERR_TAIL_BYTES {
        tail.drain(..tail.len() - STDERR_TAIL_BYTES);
    }
}

fn append_output_tail(tail: &mut ProcessOutputBuffer, bytes: &[u8]) {
    tail.bytes.extend_from_slice(bytes);
    if tail.bytes.len() > OUTPUT_TAIL_BYTES {
        tail.truncated = true;
        tail.bytes.drain(..tail.bytes.len() - OUTPUT_TAIL_BYTES);
    }
}

/// Registry for tracking background processes, scoped by user for multi-tenant isolation
#[derive(Clone)]
pub struct ProcessRegistry {
    /// Outer key: user_id, Inner key: process_id
    processes: Arc<RwLock<HashMap<String, HashMap<ProcessId, ProcessEntry>>>>,
    /// Per-owner launch gates make check-and-spawn decisions atomic across all
    /// clones without serializing launches belonging to unrelated owners.
    launch_gates: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// Optional completion sink for session wake (server/TUI wires this).
    completion_tx: Arc<RwLock<Option<tokio::sync::mpsc::UnboundedSender<ProcessCompletionEvent>>>>,
}

impl std::fmt::Debug for ProcessRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessRegistry")
            .finish_non_exhaustive()
    }
}

impl Default for ProcessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessRegistry {
    pub fn new() -> Self {
        Self {
            processes: Arc::new(RwLock::new(HashMap::new())),
            launch_gates: Arc::new(Mutex::new(HashMap::new())),
            completion_tx: Arc::new(RwLock::new(None)),
        }
    }

    /// Register a completion listener used to wake parent sessions when a
    /// background process reaches a terminal state. Replaces any prior sink.
    pub async fn set_completion_sender(
        &self,
        sender: tokio::sync::mpsc::UnboundedSender<ProcessCompletionEvent>,
    ) {
        *self.completion_tx.write().await = Some(sender);
    }

    fn ensure_user_map<'a>(
        map: &'a mut HashMap<String, HashMap<ProcessId, ProcessEntry>>,
        user_id: &str,
    ) -> &'a mut HashMap<ProcessId, ProcessEntry> {
        map.entry(user_id.to_string()).or_default()
    }

    fn prune_terminal_history(user_map: &mut HashMap<ProcessId, ProcessEntry>) {
        let mut terminal = user_map
            .iter()
            .filter(|(_, entry)| !entry.info.is_active())
            .map(|(id, entry)| (id.clone(), entry.info.started_at))
            .collect::<Vec<_>>();
        if terminal.len() <= MAX_TERMINAL_PROCESSES_PER_OWNER {
            return;
        }

        terminal.sort_by_key(|(_, started_at)| *started_at);
        let remove_count = terminal.len() - MAX_TERMINAL_PROCESSES_PER_OWNER;
        for (id, _) in terminal.into_iter().take(remove_count) {
            user_map.remove(&id);
        }
    }

    async fn launch_gate_for_user(&self, user_id: &str) -> Arc<Mutex<()>> {
        let mut gates = self.launch_gates.lock().await;
        Arc::clone(
            gates
                .entry(user_id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    pub async fn spawn(
        &self,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
    ) -> Result<ProcessId> {
        self.spawn_for_user(DEFAULT_USER, command, working_dir, description, None)
            .await
    }

    pub async fn spawn_for_user(
        &self,
        user_id: &str,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
        session_id: Option<String>,
    ) -> Result<ProcessId> {
        self.spawn_for_user_with_environment(
            user_id,
            command,
            working_dir,
            description,
            session_id,
            CommandEnvironment::inherited(),
        )
        .await
    }

    pub async fn spawn_for_user_with_environment(
        &self,
        user_id: &str,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
        session_id: Option<String>,
        environment: CommandEnvironment,
    ) -> Result<ProcessId> {
        let launch_gate = self.launch_gate_for_user(user_id).await;
        let _launch_guard = launch_gate.lock().await;
        self.spawn_for_user_with_launch_guard(
            user_id,
            command,
            working_dir,
            description,
            session_id,
            environment,
        )
        .await
    }

    /// Atomically reuse an equivalent active process or launch a new one for
    /// the default owner. The predicate runs only against that owner's tracked
    /// processes while the same launch gate used by [`Self::spawn`] is held.
    pub async fn spawn_or_reuse_matching<F>(
        &self,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
        session_id: Option<String>,
        is_equivalent: F,
    ) -> Result<(ProcessInfo, bool)>
    where
        F: Fn(&ProcessInfo) -> bool + Send,
    {
        self.spawn_or_reuse_matching_with_environment(
            command,
            working_dir,
            description,
            session_id,
            CommandEnvironment::inherited(),
            is_equivalent,
        )
        .await
    }

    pub async fn spawn_or_reuse_matching_with_environment<F>(
        &self,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
        session_id: Option<String>,
        environment: CommandEnvironment,
        is_equivalent: F,
    ) -> Result<(ProcessInfo, bool)>
    where
        F: Fn(&ProcessInfo) -> bool + Send,
    {
        self.spawn_or_reuse_matching_for_user_with_environment(
            DEFAULT_USER,
            command,
            working_dir,
            description,
            session_id,
            environment,
            is_equivalent,
        )
        .await
    }

    /// Atomically reuse an equivalent active process or launch a new one for a
    /// specific owner. The returned boolean is true when an existing process
    /// was reused and false when this call spawned the returned process.
    pub async fn spawn_or_reuse_matching_for_user<F>(
        &self,
        user_id: &str,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
        session_id: Option<String>,
        is_equivalent: F,
    ) -> Result<(ProcessInfo, bool)>
    where
        F: Fn(&ProcessInfo) -> bool + Send,
    {
        self.spawn_or_reuse_matching_for_user_with_environment(
            user_id,
            command,
            working_dir,
            description,
            session_id,
            CommandEnvironment::inherited(),
            is_equivalent,
        )
        .await
    }

    pub async fn spawn_or_reuse_matching_for_user_with_environment<F>(
        &self,
        user_id: &str,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
        session_id: Option<String>,
        environment: CommandEnvironment,
        is_equivalent: F,
    ) -> Result<(ProcessInfo, bool)>
    where
        F: Fn(&ProcessInfo) -> bool + Send,
    {
        let launch_gate = self.launch_gate_for_user(user_id).await;
        let _launch_guard = launch_gate.lock().await;
        let environment_fingerprint = environment.fingerprint();

        let existing = self
            .processes
            .read()
            .await
            .get(user_id)
            .and_then(|user_map| {
                user_map
                    .values()
                    .find(|entry| {
                        entry.environment_fingerprint == environment_fingerprint
                            && is_equivalent(&entry.info)
                    })
                    .map(|entry| entry.info.clone())
            });
        if let Some(process) = existing {
            tracing::info!(
                id = %process.id,
                user_id = %user_id,
                command = %command,
                "Equivalent process launch reused"
            );
            return Ok((process, true));
        }

        let process_id = self
            .spawn_for_user_with_launch_guard(
                user_id,
                command,
                working_dir,
                description,
                session_id,
                environment,
            )
            .await?;
        let process = self
            .processes
            .read()
            .await
            .get(user_id)
            .and_then(|user_map| user_map.get(&process_id))
            .map(|entry| entry.info.clone())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "spawned process {process_id} was not registered for user {user_id}"
                )
            })?;
        Ok((process, false))
    }

    /// Spawn after the caller has acquired this owner's launch gate.
    async fn spawn_for_user_with_launch_guard(
        &self,
        user_id: &str,
        command: String,
        working_dir: PathBuf,
        description: Option<String>,
        session_id: Option<String>,
        environment: CommandEnvironment,
    ) -> Result<ProcessId> {
        let active_count = self
            .processes
            .read()
            .await
            .get(user_id)
            .map(|user_map| {
                user_map
                    .values()
                    .filter(|entry| entry.info.is_active())
                    .count()
            })
            .unwrap_or_default();
        anyhow::ensure!(
            active_count < MAX_ACTIVE_PROCESSES_PER_OWNER,
            "active background process limit reached for owner {user_id} ({MAX_ACTIVE_PROCESSES_PER_OWNER}); stop or reuse a tracked process before starting another"
        );

        let id = uuid::Uuid::new_v4().to_string();

        let mut cmd = if cfg!(windows) {
            let mut c = Command::new("cmd");
            c.arg("/C").arg(&command);
            c
        } else {
            let mut c = Command::new("sh");
            c.arg("-c").arg(&command);
            #[cfg(unix)]
            {
                c.process_group(0);
            }
            c
        };

        cmd.current_dir(&working_dir);
        let environment_fingerprint = environment.fingerprint();
        environment.apply(&mut cmd);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;
        let pid = child.id();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let output_tail = Arc::new(Mutex::new(ProcessOutputBuffer::default()));
        let output_tail_for_stdout = Arc::clone(&output_tail);
        let stdout_handle = tokio::spawn(async move {
            let Some(mut stdout) = stdout else {
                return;
            };
            let mut chunk = [0_u8; 1024];
            loop {
                match stdout.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => {
                        let mut tail = output_tail_for_stdout.lock().await;
                        append_output_tail(&mut tail, &chunk[..read]);
                    }
                }
            }
        });
        let stderr_tail = Arc::new(Mutex::new(Vec::new()));
        let stderr_tail_for_reader = Arc::clone(&stderr_tail);
        let output_tail_for_stderr = Arc::clone(&output_tail);
        let stderr_handle = tokio::spawn(async move {
            let Some(mut stderr) = stderr else {
                return;
            };
            let mut chunk = [0_u8; 1024];
            loop {
                match stderr.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => {
                        let mut tail = stderr_tail_for_reader.lock().await;
                        append_stderr_tail(&mut tail, &chunk[..read]);
                        drop(tail);
                        let mut output = output_tail_for_stderr.lock().await;
                        append_output_tail(&mut output, &chunk[..read]);
                    }
                }
            }
        });

        let info = ProcessInfo {
            id: id.clone(),
            command: command.clone(),
            description,
            pid,
            started_at: Instant::now(),
            status: ProcessStatus::Running,
            _working_dir: working_dir,
            session_id,
            completion_notified: false,
        };

        tracing::info!(
            id = %id,
            user_id = %user_id,
            pid = ?pid,
            command = %command,
            session_id = ?info.session_id,
            "Process spawned"
        );

        // Insert before the monitor starts. Fast startup failures (for example,
        // a preview server binding an occupied port) must not race their status
        // update against registration and remain falsely marked as running.
        {
            let entry = ProcessEntry {
                info,
                environment_fingerprint,
                output: Arc::clone(&output_tail),
                _handle: None,
            };
            let mut processes = self.processes.write().await;
            let user_map = Self::ensure_user_map(&mut processes, user_id);
            Self::prune_terminal_history(user_map);
            user_map.insert(id.clone(), entry);
        }

        let registry = self.clone();
        let process_id = id.clone();
        let owner_id = user_id.to_string();
        let start_time = Instant::now();
        let handle = tokio::spawn(async move {
            let result = child.wait().await;
            let _ = stdout_handle.await;
            let _ = stderr_handle.await;
            let duration_ms = elapsed_millis_u64(start_time);
            let stderr = String::from_utf8_lossy(&stderr_tail.lock().await)
                .trim()
                .to_string();

            let status = match result {
                Ok(exit_status) => {
                    let code = exit_status.code().unwrap_or(-1);
                    if exit_status.success() {
                        ProcessStatus::Completed {
                            exit_code: code,
                            duration_ms,
                        }
                    } else {
                        ProcessStatus::Failed {
                            error: if stderr.is_empty() {
                                format!("Exit code: {}", code)
                            } else {
                                format!("Exit code: {}: {}", code, stderr)
                            },
                            duration_ms,
                        }
                    }
                }
                Err(e) => ProcessStatus::Failed {
                    error: e.to_string(),
                    duration_ms,
                },
            };

            registry
                .update_status_from_monitor_for_user(&owner_id, &process_id, status)
                .await;
        });

        let mut processes = self.processes.write().await;
        if let Some(entry) = Self::ensure_user_map(&mut processes, user_id).get_mut(&id) {
            entry._handle = Some(handle);
        }

        Ok(id)
    }

    pub async fn kill(&self, id: &str) -> Result<()> {
        self.kill_for_user(DEFAULT_USER, id).await
    }

    pub async fn kill_for_user(&self, user_id: &str, id: &str) -> Result<()> {
        let mut processes = self.processes.write().await;

        let user_map = processes
            .get_mut(user_id)
            .ok_or_else(|| anyhow::anyhow!("Process not found"))?;

        let entry = user_map
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("process {id} not found for user {user_id}"))?;
        anyhow::ensure!(
            entry.info.is_active(),
            "process {id} cannot be killed because it is {}",
            entry.info.display_status()
        );
        let pid = entry
            .info
            .pid
            .ok_or_else(|| anyhow::anyhow!("process {id} has no OS process ID to signal"))?;

        // A stopped process cannot act on SIGTERM until it receives SIGCONT.
        // Resume it first, and reflect that successful transition even if the
        // subsequent termination signal fails.
        if entry.info.is_suspended() {
            resume_process_tree(pid).map_err(|error| {
                anyhow::anyhow!(
                    "failed to resume suspended process {id} before termination: {error:#}"
                )
            })?;
            entry.info.status = ProcessStatus::Running;
        }

        terminate_process_tree(pid).map_err(|error| {
            anyhow::anyhow!("failed to terminate process {id} (OS pid {pid}): {error:#}")
        })?;

        let duration_ms = elapsed_millis_u64(entry.info.started_at);
        entry.info.status = ProcessStatus::Killed { duration_ms };
        let should_notify = !entry.info.completion_notified;
        if should_notify {
            entry.info.completion_notified = true;
        }
        let completion_event = should_notify.then(|| ProcessCompletionEvent {
            user_id: user_id.to_string(),
            process_id: entry.info.id.clone(),
            session_id: entry.info.session_id.clone(),
            command: entry.info.command.clone(),
            description: entry.info.description.clone(),
            status: entry.info.status.clone(),
            output_preview: None,
        });

        Self::prune_terminal_history(user_map);

        tracing::info!(id = %id, user_id = %user_id, pid, "Process killed");
        drop(processes);
        if let Some(event) = completion_event {
            self.emit_completion(event).await;
        }
        Ok(())
    }

    pub async fn suspend(&self, id: &str) -> Result<()> {
        self.suspend_for_user(DEFAULT_USER, id).await
    }

    pub async fn suspend_for_user(&self, user_id: &str, id: &str) -> Result<()> {
        let mut processes = self.processes.write().await;

        let user_map = processes
            .get_mut(user_id)
            .ok_or_else(|| anyhow::anyhow!("Process not found"))?;

        let entry = user_map
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("process {id} not found for user {user_id}"))?;
        anyhow::ensure!(
            entry.info.is_running(),
            "process {id} cannot be suspended because it is {}",
            entry.info.display_status()
        );
        let pid = entry
            .info
            .pid
            .ok_or_else(|| anyhow::anyhow!("process {id} has no OS process ID to signal"))?;

        suspend_process_tree(pid).map_err(|error| {
            anyhow::anyhow!("failed to suspend process {id} (OS pid {pid}): {error:#}")
        })?;
        entry.info.status = ProcessStatus::Suspended;

        tracing::info!(id = %id, user_id = %user_id, pid, "Process suspended");
        Ok(())
    }

    pub async fn resume(&self, id: &str) -> Result<()> {
        self.resume_for_user(DEFAULT_USER, id).await
    }

    pub async fn resume_for_user(&self, user_id: &str, id: &str) -> Result<()> {
        let mut processes = self.processes.write().await;

        let user_map = processes
            .get_mut(user_id)
            .ok_or_else(|| anyhow::anyhow!("Process not found"))?;

        let entry = user_map
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("process {id} not found for user {user_id}"))?;
        anyhow::ensure!(
            entry.info.is_suspended(),
            "process {id} cannot be resumed because it is {}",
            entry.info.display_status()
        );
        let pid = entry
            .info
            .pid
            .ok_or_else(|| anyhow::anyhow!("process {id} has no OS process ID to signal"))?;

        resume_process_tree(pid).map_err(|error| {
            anyhow::anyhow!("failed to resume process {id} (OS pid {pid}): {error:#}")
        })?;
        entry.info.status = ProcessStatus::Running;

        tracing::info!(id = %id, user_id = %user_id, pid, "Process resumed");
        Ok(())
    }

    pub async fn list(&self) -> Vec<ProcessInfo> {
        self.list_for_user(DEFAULT_USER).await
    }

    pub async fn list_for_user(&self, user_id: &str) -> Vec<ProcessInfo> {
        self.processes
            .read()
            .await
            .get(user_id)
            .map(|user_map| user_map.values().map(|e| e.info.clone()).collect())
            .unwrap_or_default()
    }

    pub fn try_running_count(&self) -> Option<usize> {
        self.processes.try_read().ok().map(|guard| {
            guard
                .get(DEFAULT_USER)
                .into_iter()
                .flat_map(|user_map| user_map.values())
                .filter(|e| e.info.is_running())
                .count()
        })
    }

    pub fn try_list(&self) -> Option<Vec<ProcessInfo>> {
        self.processes.try_read().ok().map(|guard| {
            guard
                .get(DEFAULT_USER)
                .into_iter()
                .flat_map(|user_map| user_map.values().map(|e| e.info.clone()))
                .collect()
        })
    }

    pub async fn get(&self, id: &str) -> Option<ProcessInfo> {
        self.get_for_user(DEFAULT_USER, id).await
    }

    pub async fn get_for_user(&self, user_id: &str, id: &str) -> Option<ProcessInfo> {
        self.processes
            .read()
            .await
            .get(user_id)
            .and_then(|user_map| user_map.get(id).map(|e| e.info.clone()))
    }

    /// Return the bounded combined stdout/stderr tail for a tracked process.
    /// The boolean indicates that older output was discarded.
    pub async fn output(&self, id: &str) -> Option<(String, bool)> {
        self.output_for_user(DEFAULT_USER, id).await
    }

    /// User-scoped variant of [`Self::output`].
    pub async fn output_for_user(&self, user_id: &str, id: &str) -> Option<(String, bool)> {
        let output = self
            .processes
            .read()
            .await
            .get(user_id)
            .and_then(|user_map| user_map.get(id))
            .map(|entry| Arc::clone(&entry.output))?;
        let output = output.lock().await;
        Some((
            String::from_utf8_lossy(&output.bytes).into_owned(),
            output.truncated,
        ))
    }

    pub async fn update_status(&self, id: &str, status: ProcessStatus) {
        self.update_status_for_user(DEFAULT_USER, id, status).await;
    }

    pub async fn update_status_for_user(&self, user_id: &str, id: &str, status: ProcessStatus) {
        let mut completion_event = None;
        {
            let mut processes = self.processes.write().await;
            if let Some(user_map) = processes.get_mut(user_id) {
                if let Some(entry) = user_map.get_mut(id) {
                    tracing::info!(id = %id, user_id = %user_id, status = ?status, "Process status updated");
                    entry.info.status = status;
                    if !entry.info.is_active() && !entry.info.completion_notified {
                        entry.info.completion_notified = true;
                        completion_event = Some(ProcessCompletionEvent {
                            user_id: user_id.to_string(),
                            process_id: entry.info.id.clone(),
                            session_id: entry.info.session_id.clone(),
                            command: entry.info.command.clone(),
                            description: entry.info.description.clone(),
                            status: entry.info.status.clone(),
                            output_preview: None,
                        });
                    }
                }
                Self::prune_terminal_history(user_map);
            }
        }
        if let Some(event) = completion_event {
            self.emit_completion(event).await;
        }
    }

    async fn update_status_from_monitor_for_user(
        &self,
        user_id: &str,
        id: &str,
        status: ProcessStatus,
    ) {
        let mut completion_event = None;
        {
            let mut processes = self.processes.write().await;
            if let Some(user_map) = processes.get_mut(user_id) {
                if let Some(entry) = user_map.get_mut(id) {
                    if matches!(entry.info.status, ProcessStatus::Killed { .. }) {
                        tracing::debug!(
                            id = %id,
                            user_id = %user_id,
                            observed_status = ?status,
                            "Ignoring process monitor status after intentional kill"
                        );
                        return;
                    }
                    tracing::info!(id = %id, user_id = %user_id, status = ?status, "Process status updated by monitor");
                    entry.info.status = status;
                    if !entry.info.is_active() && !entry.info.completion_notified {
                        entry.info.completion_notified = true;
                        let output_preview = entry.output.try_lock().ok().and_then(|output| {
                            let text = String::from_utf8_lossy(&output.bytes);
                            let trimmed = text.trim();
                            if trimmed.is_empty() {
                                None
                            } else {
                                let preview: String = trimmed.chars().take(2_000).collect();
                                Some(preview)
                            }
                        });
                        completion_event = Some(ProcessCompletionEvent {
                            user_id: user_id.to_string(),
                            process_id: entry.info.id.clone(),
                            session_id: entry.info.session_id.clone(),
                            command: entry.info.command.clone(),
                            description: entry.info.description.clone(),
                            status: entry.info.status.clone(),
                            output_preview,
                        });
                    }
                }
                Self::prune_terminal_history(user_map);
            }
        }
        if let Some(event) = completion_event {
            self.emit_completion(event).await;
        }
    }

    async fn emit_completion(&self, event: ProcessCompletionEvent) {
        let sender = self.completion_tx.read().await.clone();
        if let Some(sender) = sender {
            if sender.send(event).is_err() {
                tracing::debug!("Process completion sink dropped; no session wake delivered");
            }
        }
    }

    pub async fn kill_all(&self) {
        let processes = self.processes.read().await;
        let active: Vec<_> = processes
            .iter()
            .flat_map(|(user_id, user_map)| {
                user_map
                    .iter()
                    .filter(|(_, entry)| entry.info.is_active())
                    .map(|(id, _)| (user_id.clone(), id.clone()))
            })
            .collect();
        drop(processes);

        for (user_id, id) in active {
            if let Err(error) = self.kill_for_user(&user_id, &id).await {
                tracing::warn!(
                    id = %id,
                    user_id = %user_id,
                    error = %error,
                    "Failed to kill tracked process during shutdown"
                );
            }
        }
    }

    /// Stop every active process belonging to one exact owner. This is used
    /// for delegated-task lease cleanup and never crosses an owner boundary.
    pub async fn kill_all_for_user(&self, user_id: &str) -> Vec<(ProcessId, String)> {
        let active = self
            .list_for_user(user_id)
            .await
            .into_iter()
            .filter(ProcessInfo::is_active)
            .map(|process| process.id)
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for id in active {
            if let Err(error) = self.kill_for_user(user_id, &id).await {
                // A monitor may have observed natural completion between the
                // snapshot and signal. Only retain failures for still-active
                // processes.
                if self
                    .get_for_user(user_id, &id)
                    .await
                    .is_some_and(|process| process.is_active())
                {
                    failures.push((id, error.to_string()));
                }
            }
        }
        failures
    }

    pub async fn register_external(
        &self,
        id: ProcessId,
        command: String,
        description: Option<String>,
        pid: Option<u32>,
        working_dir: PathBuf,
    ) -> Result<()> {
        self.register_external_for_user(DEFAULT_USER, id, command, description, pid, working_dir)
            .await
    }

    pub async fn register_external_for_user(
        &self,
        user_id: &str,
        id: ProcessId,
        command: String,
        description: Option<String>,
        pid: Option<u32>,
        working_dir: PathBuf,
    ) -> Result<()> {
        let launch_gate = self.launch_gate_for_user(user_id).await;
        let _launch_guard = launch_gate.lock().await;
        let active_count = self
            .processes
            .read()
            .await
            .get(user_id)
            .map(|user_map| {
                user_map
                    .values()
                    .filter(|entry| entry.info.is_active())
                    .count()
            })
            .unwrap_or_default();
        if active_count >= MAX_ACTIVE_PROCESSES_PER_OWNER {
            if let Some(pid) = pid {
                // External registration transfers lifecycle ownership to the
                // registry. If the reservation is rejected, terminate the
                // just-created tree rather than leave an untracked process.
                if let Err(error) = terminate_process_tree(pid) {
                    tracing::error!(
                        user_id = %user_id,
                        pid,
                        %error,
                        "Failed to terminate external process rejected by owner cap"
                    );
                }
            }
            anyhow::bail!(
                "active background process limit reached for owner {user_id} ({MAX_ACTIVE_PROCESSES_PER_OWNER}); stop or reuse a tracked process before registering another"
            );
        }

        let info = ProcessInfo {
            id: id.clone(),
            command,
            description,
            pid,
            started_at: Instant::now(),
            status: ProcessStatus::Running,
            _working_dir: working_dir,
            session_id: None,
            completion_notified: false,
        };
        let entry = ProcessEntry {
            info,
            environment_fingerprint: CommandEnvironment::inherited().fingerprint(),
            output: Arc::new(Mutex::new(ProcessOutputBuffer::default())),
            _handle: None,
        };
        let mut processes = self.processes.write().await;
        let user_map = Self::ensure_user_map(&mut processes, user_id);
        Self::prune_terminal_history(user_map);
        user_map.insert(id.clone(), entry);
        tracing::info!(id = %id, user_id = %user_id, pid = ?pid, "External process registered");
        Ok(())
    }

    pub async fn unregister(&self, id: &str) {
        self.unregister_for_user(DEFAULT_USER, id).await;
    }

    pub async fn unregister_for_user(&self, user_id: &str, id: &str) {
        let mut processes = self.processes.write().await;
        if let Some(user_map) = processes.get_mut(user_id) {
            if let Some(entry) = user_map.remove(id) {
                tracing::info!(id = %id, user_id = %user_id, status = ?entry.info.status, "Process unregistered");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProcessRegistry, ProcessStatus, MAX_ACTIVE_PROCESSES_PER_OWNER,
        MAX_TERMINAL_PROCESSES_PER_OWNER,
    };
    use tempfile::TempDir;

    #[cfg(unix)]
    fn heartbeat_len(path: &std::path::Path) -> u64 {
        std::fs::metadata(path).map_or(0, |metadata| metadata.len())
    }

    #[cfg(unix)]
    async fn wait_for_heartbeat(path: &std::path::Path, minimum: u64) {
        for _ in 0..100 {
            if heartbeat_len(path) >= minimum {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("heartbeat {} did not reach {minimum} bytes", path.display());
    }

    #[cfg(unix)]
    async fn wait_for_process_exit(pid: u32) {
        let pid = libc::pid_t::try_from(pid).expect("test process ID fits pid_t");
        for _ in 0..100 {
            if unsafe { libc::kill(pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("process {pid} remained alive after termination");
    }

    #[cfg(unix)]
    async fn spawn_heartbeat(
        registry: &ProcessRegistry,
        directory: &TempDir,
        name: &str,
    ) -> (String, u32, std::path::PathBuf) {
        let script = directory.path().join(format!("{name}.sh"));
        let heartbeat = directory.path().join(format!("{name}.heartbeat"));
        std::fs::write(
            &script,
            "#!/bin/sh\nwhile :; do\n  printf x >> \"$1\"\n  sleep 0.02\ndone\n",
        )
        .expect("write heartbeat script");

        let command = format!(
            "sh {} {}",
            script
                .file_name()
                .expect("script filename")
                .to_string_lossy(),
            heartbeat
                .file_name()
                .expect("heartbeat filename")
                .to_string_lossy()
        );
        let id = registry
            .spawn(
                command,
                directory.path().to_path_buf(),
                Some(format!("{name} lifecycle test")),
            )
            .await
            .expect("spawn heartbeat process");
        let pid = registry
            .get(&id)
            .await
            .and_then(|process| process.pid)
            .expect("spawned process has PID");
        wait_for_heartbeat(&heartbeat, 3).await;
        (id, pid, heartbeat)
    }

    #[tokio::test]
    async fn fast_background_failure_is_registered_with_stderr() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        let id = registry
            .spawn(
                "echo 'bind failed: address already in use' >&2; exit 7".to_string(),
                directory.path().to_path_buf(),
                Some("failed preview".to_string()),
            )
            .await
            .expect("process should spawn");

        let mut observed = None;
        for _ in 0..20 {
            let process = registry.get(&id).await.expect("registered process");
            if !process.is_running() {
                observed = Some(process.status);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        match observed.expect("fast failure should become visible") {
            ProcessStatus::Failed { error, .. } => {
                assert!(error.contains("Exit code: 7"));
                assert!(error.contains("address already in use"));
            }
            status => panic!("expected failed status, got {status:?}"),
        }
    }

    #[tokio::test]
    async fn background_output_is_replayable_and_user_scoped() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        let command = if cfg!(windows) {
            "echo stdout line & echo stderr line 1>&2"
        } else {
            "printf 'stdout line\\n'; printf 'stderr line\\n' >&2"
        };
        let id = registry
            .spawn_for_user(
                "alice",
                command.to_string(),
                directory.path().to_path_buf(),
                Some("captured output".to_string()),
                Some("session-alice".to_string()),
            )
            .await
            .expect("process should spawn");

        for _ in 0..100 {
            let process = registry
                .get_for_user("alice", &id)
                .await
                .expect("registered process");
            if !process.is_running() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let (output, truncated) = registry
            .output_for_user("alice", &id)
            .await
            .expect("owner should read output");
        assert!(output.contains("stdout line"));
        assert!(output.contains("stderr line"));
        assert!(!truncated);
        assert_eq!(registry.output_for_user("bob", &id).await, None);
    }

    #[tokio::test]
    async fn unscoped_operations_only_access_the_default_owner() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        let shared_id = "same-id-in-two-owner-maps".to_string();

        registry
            .register_external(
                shared_id.clone(),
                "default command".to_string(),
                None,
                None,
                directory.path().to_path_buf(),
            )
            .await
            .expect("register default process");
        registry
            .register_external_for_user(
                "alice",
                shared_id.clone(),
                "alice command".to_string(),
                None,
                None,
                directory.path().to_path_buf(),
            )
            .await
            .expect("register alice process");

        let unscoped = registry.list().await;
        assert_eq!(unscoped.len(), 1);
        assert_eq!(unscoped[0].command, "default command");
        assert_eq!(registry.try_list().expect("registry lock").len(), 1);
        assert_eq!(registry.try_running_count(), Some(1));
        assert_eq!(
            registry
                .get(&shared_id)
                .await
                .expect("default process")
                .command,
            "default command"
        );

        registry
            .update_status(&shared_id, ProcessStatus::Suspended)
            .await;
        assert!(registry
            .get(&shared_id)
            .await
            .expect("default process")
            .is_suspended());
        assert!(registry
            .get_for_user("alice", &shared_id)
            .await
            .expect("alice process")
            .is_running());

        registry.unregister(&shared_id).await;
        assert!(registry.get(&shared_id).await.is_none());
        assert!(registry.output(&shared_id).await.is_none());
        assert!(registry.get_for_user("alice", &shared_id).await.is_some());
    }

    #[tokio::test]
    async fn active_process_cap_is_enforced_per_owner() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        for index in 0..MAX_ACTIVE_PROCESSES_PER_OWNER {
            registry
                .register_external_for_user(
                    "alice",
                    format!("active-{index}"),
                    "externally tracked".to_string(),
                    None,
                    None,
                    directory.path().to_path_buf(),
                )
                .await
                .expect("register active process");
        }

        let error = registry
            .spawn_for_user(
                "alice",
                "echo should-not-launch".to_string(),
                directory.path().to_path_buf(),
                None,
                None,
            )
            .await
            .expect_err("the seventeenth active process must be rejected");
        assert!(error
            .to_string()
            .contains("active background process limit"));

        let external_error = registry
            .register_external_for_user(
                "alice",
                "active-overflow".to_string(),
                "externally tracked overflow".to_string(),
                None,
                None,
                directory.path().to_path_buf(),
            )
            .await
            .expect_err("external registration must share the same owner cap");
        assert!(external_error
            .to_string()
            .contains("active background process limit"));

        let bob = registry
            .spawn_for_user(
                "bob",
                "echo independent-owner".to_string(),
                directory.path().to_path_buf(),
                None,
                None,
            )
            .await;
        assert!(bob.is_ok(), "one owner's cap must not block another owner");
    }

    #[tokio::test]
    async fn terminal_history_is_bounded_without_evicting_active_entries() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        registry
            .register_external_for_user(
                "alice",
                "active".to_string(),
                "still running".to_string(),
                None,
                None,
                directory.path().to_path_buf(),
            )
            .await
            .expect("register active history sentinel");

        for index in 0..(MAX_TERMINAL_PROCESSES_PER_OWNER + 12) {
            let id = format!("finished-{index:03}");
            registry
                .register_external_for_user(
                    "alice",
                    id.clone(),
                    "quick task".to_string(),
                    None,
                    None,
                    directory.path().to_path_buf(),
                )
                .await
                .expect("register quick process");
            registry
                .update_status_for_user(
                    "alice",
                    &id,
                    ProcessStatus::Completed {
                        exit_code: 0,
                        duration_ms: 1,
                    },
                )
                .await;
        }

        let entries = registry.list_for_user("alice").await;
        assert_eq!(
            entries.iter().filter(|entry| !entry.is_active()).count(),
            MAX_TERMINAL_PROCESSES_PER_OWNER
        );
        assert!(entries
            .iter()
            .any(|entry| entry.id == "active" && entry.is_active()));
        assert!(registry
            .get_for_user("alice", "finished-000")
            .await
            .is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn suspend_resume_and_kill_follow_the_real_process_lifecycle() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        let (id, pid, heartbeat) = spawn_heartbeat(&registry, &directory, "lifecycle").await;

        registry.suspend(&id).await.expect("deliver SIGSTOP");
        assert!(
            registry.get(&id).await.expect("process").is_suspended(),
            "status changes only after SIGSTOP succeeds"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let stopped_at = heartbeat_len(&heartbeat);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            heartbeat_len(&heartbeat),
            stopped_at,
            "SIGSTOP must pause the entire process group"
        );

        registry.resume(&id).await.expect("deliver SIGCONT");
        assert!(registry.get(&id).await.expect("process").is_running());
        wait_for_heartbeat(&heartbeat, stopped_at + 2).await;

        registry.kill(&id).await.expect("deliver SIGTERM");
        wait_for_process_exit(pid).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(matches!(
            registry.get(&id).await.expect("process").status,
            ProcessStatus::Killed { .. }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_resumes_and_terminates_a_suspended_process() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        let (id, pid, _) = spawn_heartbeat(&registry, &directory, "suspended-kill").await;

        registry.suspend(&id).await.expect("suspend process");
        registry
            .kill(&id)
            .await
            .expect("resume and terminate suspended process");
        wait_for_process_exit(pid).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(matches!(
            registry.get(&id).await.expect("process").status,
            ProcessStatus::Killed { .. }
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn kill_all_includes_running_and_suspended_processes() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        let (running_id, running_pid, _) =
            spawn_heartbeat(&registry, &directory, "kill-all-running").await;
        let (suspended_id, suspended_pid, _) =
            spawn_heartbeat(&registry, &directory, "kill-all-suspended").await;
        registry
            .suspend(&suspended_id)
            .await
            .expect("suspend second process");

        registry.kill_all().await;
        wait_for_process_exit(running_pid).await;
        wait_for_process_exit(suspended_pid).await;

        for id in [running_id, suspended_id] {
            assert!(matches!(
                registry.get(&id).await.expect("process").status,
                ProcessStatus::Killed { .. }
            ));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_signal_delivery_does_not_advance_registry_state() {
        let registry = ProcessRegistry::new();
        let directory = TempDir::new().expect("temp dir");
        let id = "invalid-signal-target".to_string();
        registry
            .register_external(
                id.clone(),
                "not actually running".to_string(),
                None,
                Some(u32::MAX),
                directory.path().to_path_buf(),
            )
            .await
            .expect("register invalid signal target");

        let suspend_error = registry
            .suspend(&id)
            .await
            .expect_err("invalid SIGSTOP target must fail");
        assert!(suspend_error.to_string().contains("failed to suspend"));
        assert!(suspend_error.to_string().contains("does not fit"));
        assert!(registry.get(&id).await.expect("process").is_running());

        registry.update_status(&id, ProcessStatus::Suspended).await;
        let resume_error = registry
            .resume(&id)
            .await
            .expect_err("invalid SIGCONT target must fail");
        assert!(resume_error.to_string().contains("failed to resume"));
        assert!(resume_error.to_string().contains("does not fit"));
        assert!(registry.get(&id).await.expect("process").is_suspended());

        let kill_error = registry
            .kill(&id)
            .await
            .expect_err("invalid suspended target must not be marked killed");
        assert!(kill_error
            .to_string()
            .contains("failed to resume suspended"));
        assert!(kill_error.to_string().contains("does not fit"));
        assert!(registry.get(&id).await.expect("process").is_suspended());
    }
}
