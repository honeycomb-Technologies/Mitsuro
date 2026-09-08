//! Persistent, authenticated PTY sessions for native desktop clients.
//!
//! Unlike the legacy `/ws/terminal` endpoint, a terminal here is owned by the
//! server rather than by one WebSocket. Attachments may come and go while the
//! PTY, replay buffer, and child waiter remain alive.

use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex, OnceLock,
    },
    time::{Duration, Instant},
};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path as AxumPath, Query, State, WebSocketUpgrade,
    },
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use portable_pty::{native_pty_system, Child, ChildKiller, CommandBuilder, MasterPty, PtySize};
use serde::Deserialize;
use tokio::sync::{broadcast, mpsc, Notify, RwLock};
use uuid::Uuid;

use crate::{
    auth::{AuthenticatedUser, CurrentUser},
    error::AppError,
    types::{
        ControlLease, ControlLeaseSummary, TakeTerminalControlRequest, TakeTerminalControlResponse,
        TerminalDescriptor, TerminalListResponse, TerminalSize, TerminalSpec, TerminalStatus,
        TerminalTransportCommand, TerminalTransportEvent,
    },
    utils::{
        paths::validate_path_within,
        workspace::{allowed_root, resolve_scoped_workspace_path, workspace_base},
    },
    AppState,
};

const MAX_TERMINALS_PER_OWNER: usize = 8;
const DEFAULT_REPLAY_BYTES: usize = 1024 * 1024;
const EVENT_CHANNEL_CAPACITY: usize = 256;
const READER_CHANNEL_CAPACITY: usize = 128;
const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_COMMAND_BYTES: usize = MAX_INPUT_BYTES * 2 + 1024;
const MAX_TERMINAL_COLS: u16 = 500;
const MAX_TERMINAL_ROWS: u16 = 500;
const TERMINATE_TIMEOUT: Duration = Duration::from_secs(3);
const READER_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
const CONTROL_LEASE_TTL: Duration = Duration::from_secs(15);
const CONTROL_LEASE_HEADER: &str = "x-mitsuro-control-lease";

static TERMINAL_MANAGER: OnceLock<TerminalManager> = OnceLock::new();

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_terminals).post(create_terminal))
        .route("/:id", get(get_terminal).delete(delete_terminal))
        .route("/:id/control", axum::routing::post(take_terminal_control))
        .route("/:id/stream", get(stream_terminal))
}

fn manager() -> &'static TerminalManager {
    TERMINAL_MANAGER.get_or_init(TerminalManager::production)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OwnerKey(Option<String>);

impl OwnerKey {
    fn from_user(user: &AuthenticatedUser) -> Self {
        Self(user.user_id.clone())
    }
}

#[derive(Debug, Clone)]
struct OutputFrame {
    offset: u64,
    bytes: Vec<u8>,
}

impl OutputFrame {
    fn end_offset(&self) -> u64 {
        self.offset
            .saturating_add(u64::try_from(self.bytes.len()).unwrap_or(u64::MAX))
    }
}

#[derive(Debug)]
struct ReplayBuffer {
    frames: VecDeque<OutputFrame>,
    retained_bytes: usize,
    max_bytes: usize,
    next_offset: u64,
}

impl ReplayBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            frames: VecDeque::new(),
            retained_bytes: 0,
            max_bytes,
            next_offset: 0,
        }
    }

    fn available_from(&self) -> u64 {
        self.frames
            .front()
            .map(|frame| frame.offset)
            .unwrap_or(self.next_offset)
    }

    fn append(&mut self, bytes: Vec<u8>) -> OutputFrame {
        let frame = OutputFrame {
            offset: self.next_offset,
            bytes,
        };
        self.next_offset = frame.end_offset();
        self.retained_bytes = self.retained_bytes.saturating_add(frame.bytes.len());
        self.frames.push_back(frame.clone());
        self.trim();
        frame
    }

    fn trim(&mut self) {
        while self.retained_bytes > self.max_bytes {
            let excess = self.retained_bytes - self.max_bytes;
            let Some(front) = self.frames.front_mut() else {
                self.retained_bytes = 0;
                break;
            };
            if front.bytes.len() <= excess {
                self.retained_bytes -= front.bytes.len();
                self.frames.pop_front();
            } else {
                front.bytes.drain(..excess);
                front.offset = front
                    .offset
                    .saturating_add(u64::try_from(excess).unwrap_or(u64::MAX));
                self.retained_bytes -= excess;
            }
        }
    }

    fn snapshot(&self, requested_offset: Option<u64>) -> ReplaySnapshot {
        let available_from = self.available_from();
        let requested = requested_offset.unwrap_or(available_from);
        let (cursor, gap) = if requested < available_from {
            (
                available_from,
                Some(TerminalTransportEvent::Gap {
                    requested_offset: requested,
                    available_from,
                    next_offset: self.next_offset,
                }),
            )
        } else if requested > self.next_offset {
            (
                self.next_offset,
                Some(TerminalTransportEvent::Gap {
                    requested_offset: requested,
                    available_from: self.next_offset,
                    next_offset: self.next_offset,
                }),
            )
        } else {
            (requested, None)
        };

        let mut output = Vec::new();
        for frame in &self.frames {
            if frame.end_offset() <= cursor {
                continue;
            }
            let trim = cursor.saturating_sub(frame.offset);
            let trim = usize::try_from(trim).unwrap_or(usize::MAX);
            if trim >= frame.bytes.len() {
                continue;
            }
            output.push(OutputFrame {
                offset: frame
                    .offset
                    .saturating_add(u64::try_from(trim).unwrap_or(u64::MAX)),
                bytes: frame.bytes[trim..].to_vec(),
            });
        }

        ReplaySnapshot {
            next_offset: self.next_offset,
            replay_from: cursor,
            gap,
            output,
        }
    }
}

#[derive(Debug)]
struct ReplaySnapshot {
    next_offset: u64,
    replay_from: u64,
    gap: Option<TerminalTransportEvent>,
    output: Vec<OutputFrame>,
}

struct TerminalRuntimeState {
    descriptor: TerminalDescriptor,
    replay: ReplayBuffer,
    control: Option<ControlLeaseState>,
}

#[derive(Debug, Clone)]
struct ControlLeaseState {
    client_id: String,
    lease_token: String,
    last_seen: Instant,
}

struct TerminalSession {
    owner: OwnerKey,
    state: RwLock<TerminalRuntimeState>,
    events: broadcast::Sender<TerminalTransportEvent>,
    master: Arc<StdMutex<Box<dyn MasterPty + Send>>>,
    writer: Arc<StdMutex<Option<Box<dyn Write + Send>>>>,
    killer: Arc<StdMutex<Option<Box<dyn ChildKiller + Send + Sync>>>>,
    terminating: AtomicBool,
    reader_finished: AtomicBool,
    wait_finished: AtomicBool,
    reader_finished_notify: Notify,
    exit_notify: Notify,
    control_lease_ttl: Duration,
}

impl TerminalSession {
    fn spawn(
        owner: OwnerKey,
        working_dir: PathBuf,
        size: TerminalSize,
        replay_bytes: usize,
        control_lease_ttl: Duration,
        shell: &str,
        id: Option<&str>,
    ) -> Result<Arc<Self>, AppError> {
        let size = clamp_terminal_size(size);
        let pty_system = native_pty_system();
        let pair = pty_system.openpty(to_pty_size(size)).map_err(|error| {
            AppError::ServiceUnavailable(format!("Failed to open PTY: {error}"))
        })?;

        // Acquire all fallible master handles before spawning. If one of these
        // operations fails there is no child process to orphan.
        let reader = pair.master.try_clone_reader().map_err(|error| {
            AppError::ServiceUnavailable(format!("Failed to open terminal output: {error}"))
        })?;
        let writer = pair.master.take_writer().map_err(|error| {
            AppError::ServiceUnavailable(format!("Failed to open terminal input: {error}"))
        })?;

        let mut command = CommandBuilder::new(shell);
        command.cwd(&working_dir);
        command.env("TERM", "xterm-256color");
        command.env("COLORTERM", "truecolor");
        let child = pair.slave.spawn_command(command).map_err(|error| {
            AppError::ServiceUnavailable(format!("Failed to spawn terminal shell: {error}"))
        })?;
        drop(pair.slave);

        let pid = child.process_id();
        let killer = child.clone_killer();
        let id = id
            .map(str::to_string)
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let now = chrono::Utc::now().to_rfc3339();
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let session = Arc::new(Self {
            owner,
            state: RwLock::new(TerminalRuntimeState {
                descriptor: TerminalDescriptor {
                    id: id.clone(),
                    working_dir: working_dir.to_string_lossy().into_owned(),
                    size,
                    status: TerminalStatus::Running,
                    pid,
                    stream_url: format!("/api/terminals/{id}/stream"),
                    available_from: 0,
                    next_offset: 0,
                    exit_code: None,
                    last_error: None,
                    controller: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
                replay: ReplayBuffer::new(replay_bytes),
                control: None,
            }),
            events,
            master: Arc::new(StdMutex::new(pair.master)),
            writer: Arc::new(StdMutex::new(Some(writer))),
            killer: Arc::new(StdMutex::new(Some(killer))),
            terminating: AtomicBool::new(false),
            reader_finished: AtomicBool::new(false),
            wait_finished: AtomicBool::new(false),
            reader_finished_notify: Notify::new(),
            exit_notify: Notify::new(),
            control_lease_ttl,
        });
        Self::spawn_runtime_tasks(Arc::clone(&session), reader, child);
        Ok(session)
    }

    fn spawn_runtime_tasks(
        session: Arc<Self>,
        mut reader: Box<dyn Read + Send>,
        mut child: Box<dyn Child + Send + Sync>,
    ) {
        enum ReaderMessage {
            Output(Vec<u8>),
            Closed(Option<String>),
        }

        let (reader_tx, mut reader_rx) = mpsc::channel(READER_CHANNEL_CAPACITY);
        tokio::task::spawn_blocking(move || {
            let mut buffer = [0_u8; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let _ = reader_tx.blocking_send(ReaderMessage::Closed(None));
                        break;
                    }
                    Ok(read) => {
                        if reader_tx
                            .blocking_send(ReaderMessage::Output(buffer[..read].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        let error = (!is_expected_pty_eof(&error)).then(|| error.to_string());
                        let _ = reader_tx.blocking_send(ReaderMessage::Closed(error));
                        break;
                    }
                }
            }
        });

        let output_session = Arc::clone(&session);
        tokio::spawn(async move {
            let mut terminate_after_reader_failure = false;
            while let Some(message) = reader_rx.recv().await {
                match message {
                    ReaderMessage::Output(bytes) => output_session.append_output(bytes).await,
                    ReaderMessage::Closed(error) => {
                        if let Some(error) = error {
                            terminate_after_reader_failure =
                                output_session.mark_reader_disconnected(error).await;
                        }
                        break;
                    }
                }
            }
            output_session
                .reader_finished
                .store(true, Ordering::Release);
            output_session.reader_finished_notify.notify_waiters();
            if terminate_after_reader_failure {
                let _ = output_session.terminate().await;
            }
        });

        tokio::spawn(async move {
            let wait_result = tokio::task::spawn_blocking(move || child.wait()).await;
            if !session.reader_finished.load(Ordering::Acquire) {
                let notified = session.reader_finished_notify.notified();
                if !session.reader_finished.load(Ordering::Acquire) {
                    let _ = tokio::time::timeout(READER_DRAIN_TIMEOUT, notified).await;
                }
            }
            let (exit_code, error) = match wait_result {
                Ok(Ok(status)) => (i32::try_from(status.exit_code()).ok(), None),
                Ok(Err(error)) => (None, Some(format!("Failed to wait for terminal: {error}"))),
                Err(error) => (None, Some(format!("Terminal waiter failed: {error}"))),
            };
            session.mark_exited(exit_code, error).await;
            session.wait_finished.store(true, Ordering::Release);
            session.exit_notify.notify_waiters();
        });
    }

    async fn descriptor(&self) -> TerminalDescriptor {
        let (descriptor, expired) = {
            let mut state = self.state.write().await;
            let expired = prune_expired_control(&mut state, self.control_lease_ttl);
            (state.descriptor.clone(), expired)
        };
        if expired {
            let _ = self
                .events
                .send(TerminalTransportEvent::ControlChanged { controller: None });
        }
        descriptor
    }

    async fn take_control(&self, client_id: String) -> Result<ControlLease, AppError> {
        let client_id = client_id.trim();
        if client_id.is_empty() || client_id.len() > 128 {
            return Err(AppError::BadRequest(
                "Terminal controller client_id must contain 1 to 128 bytes".to_string(),
            ));
        }
        let lease = {
            let mut state = self.state.write().await;
            if state.descriptor.status != TerminalStatus::Running {
                return Err(AppError::Conflict(
                    "Terminal is not available for control".to_string(),
                ));
            }
            let control = ControlLeaseState {
                client_id: client_id.to_string(),
                lease_token: Uuid::new_v4().to_string(),
                last_seen: Instant::now(),
            };
            let summary = control_summary(&control, self.control_lease_ttl);
            let lease = ControlLease {
                resource_id: state.descriptor.id.clone(),
                client_id: control.client_id.clone(),
                lease_token: control.lease_token.clone(),
                expires_at: summary.expires_at.clone(),
            };
            state.control = Some(control);
            state.descriptor.controller = Some(summary);
            state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
            lease
        };
        let _ = self.events.send(TerminalTransportEvent::ControlChanged {
            controller: Some(ControlLeaseSummary {
                client_id: lease.client_id.clone(),
                expires_at: lease.expires_at.clone(),
            }),
        });
        Ok(lease)
    }

    async fn authorize_control(&self, lease_token: &str) -> Result<(), AppError> {
        let (result, expired) = {
            let mut state = self.state.write().await;
            let expired = prune_expired_control(&mut state, self.control_lease_ttl);
            let result = match state.control.as_mut() {
                Some(control) if constant_time_token_eq(&control.lease_token, lease_token) => {
                    control.last_seen = Instant::now();
                    let summary = control_summary(control, self.control_lease_ttl);
                    state.descriptor.controller = Some(summary);
                    state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
                    Ok(())
                }
                Some(_) => Err(AppError::Conflict(
                    "Terminal control lease is held by another client".to_string(),
                )),
                None => Err(AppError::Conflict(
                    "Terminal has no active control lease; take control before mutating it"
                        .to_string(),
                )),
            };
            (result, expired)
        };
        if expired {
            let _ = self
                .events
                .send(TerminalTransportEvent::ControlChanged { controller: None });
        }
        result
    }

    async fn append_output(&self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        let event = {
            let mut state = self.state.write().await;
            let frame = state.replay.append(bytes);
            state.descriptor.available_from = state.replay.available_from();
            state.descriptor.next_offset = state.replay.next_offset;
            state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
            TerminalTransportEvent::Output {
                offset: frame.offset,
                bytes: frame.bytes,
            }
        };
        let _ = self.events.send(event);
    }

    async fn mark_reader_disconnected(&self, reason: String) -> bool {
        let should_publish = {
            let mut state = self.state.write().await;
            if state.descriptor.status != TerminalStatus::Running {
                false
            } else {
                state.descriptor.status = TerminalStatus::Disconnected;
                state.descriptor.last_error = Some(reason.clone());
                state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
                true
            }
        };
        if should_publish {
            let _ = self
                .events
                .send(TerminalTransportEvent::Disconnected { reason });
        }
        should_publish
    }

    async fn mark_exited(&self, exit_code: Option<i32>, error: Option<String>) {
        let (next_offset, released_control) = {
            let mut state = self.state.write().await;
            state.descriptor.status = TerminalStatus::Exited;
            state.descriptor.exit_code = exit_code;
            if error.is_some() {
                state.descriptor.last_error = error;
            }
            let released_control = state.control.take().is_some();
            state.descriptor.controller = None;
            state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
            (state.descriptor.next_offset, released_control)
        };
        if let Ok(mut writer) = self.writer.lock() {
            writer.take();
        }
        if let Ok(mut killer) = self.killer.lock() {
            killer.take();
        }
        if released_control {
            let _ = self
                .events
                .send(TerminalTransportEvent::ControlChanged { controller: None });
        }
        let _ = self.events.send(TerminalTransportEvent::Exited {
            exit_code,
            next_offset,
        });
    }

    async fn replay_snapshot(&self, requested_offset: Option<u64>) -> AttachSnapshot {
        let (snapshot, expired) = {
            let mut state = self.state.write().await;
            let expired = prune_expired_control(&mut state, self.control_lease_ttl);
            let replay = state.replay.snapshot(requested_offset);
            (
                AttachSnapshot {
                    descriptor: state.descriptor.clone(),
                    replay,
                },
                expired,
            )
        };
        if expired {
            let _ = self
                .events
                .send(TerminalTransportEvent::ControlChanged { controller: None });
        }
        snapshot
    }

    fn subscribe(&self) -> broadcast::Receiver<TerminalTransportEvent> {
        self.events.subscribe()
    }

    async fn write_input(&self, lease_token: &str, bytes: Vec<u8>) -> Result<(), AppError> {
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(AppError::BadRequest(format!(
                "Terminal input exceeds the {MAX_INPUT_BYTES}-byte limit"
            )));
        }
        self.authorize_control(lease_token).await?;
        if self.state.read().await.descriptor.status != TerminalStatus::Running {
            return Err(AppError::Conflict(
                "Terminal is not accepting input".to_string(),
            ));
        }
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || {
            let mut writer = writer
                .lock()
                .map_err(|_| "Terminal input lock is unavailable".to_string())?;
            let writer = writer
                .as_mut()
                .ok_or_else(|| "Terminal input is closed".to_string())?;
            writer
                .write_all(&bytes)
                .map_err(|error| error.to_string())?;
            writer.flush().map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| AppError::Internal(format!("Terminal input task failed: {error}")))?
        .map_err(AppError::Conflict)
    }

    async fn resize(
        &self,
        lease_token: &str,
        size: TerminalSize,
    ) -> Result<TerminalDescriptor, AppError> {
        let size = clamp_terminal_size(size);
        self.authorize_control(lease_token).await?;
        if self.state.read().await.descriptor.status != TerminalStatus::Running {
            return Err(AppError::Conflict(
                "Terminal is not available for resize".to_string(),
            ));
        }
        let master = Arc::clone(&self.master);
        tokio::task::spawn_blocking(move || {
            master
                .lock()
                .map_err(|_| "Terminal resize lock is unavailable".to_string())?
                .resize(to_pty_size(size))
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| AppError::Internal(format!("Terminal resize task failed: {error}")))?
        .map_err(|error| AppError::Conflict(format!("Terminal resize failed: {error}")))?;

        let mut state = self.state.write().await;
        state.descriptor.size = size;
        state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
        Ok(state.descriptor.clone())
    }

    async fn terminate(&self) -> Result<TerminalDescriptor, AppError> {
        if self.wait_finished.load(Ordering::Acquire) {
            return Ok(self.descriptor().await);
        }
        let mut kill_error = None;
        if !self.terminating.swap(true, Ordering::AcqRel) {
            {
                let mut state = self.state.write().await;
                if state.descriptor.status != TerminalStatus::Exited {
                    state.descriptor.status = TerminalStatus::Terminating;
                    state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
                }
            }
            if let Ok(mut writer) = self.writer.lock() {
                writer.take();
            }
            let killer = Arc::clone(&self.killer);
            kill_error = tokio::task::spawn_blocking(move || {
                let mut killer = killer
                    .lock()
                    .map_err(|_| "Terminal process lock is unavailable".to_string())?;
                match killer.as_mut() {
                    Some(killer) => killer.kill().map_err(|error| error.to_string()),
                    None => Ok(()),
                }
            })
            .await
            .map_or_else(
                |error| Some(format!("Terminal kill task failed: {error}")),
                Result::err,
            );
        }

        if !self.wait_finished.load(Ordering::Acquire) {
            let notified = self.exit_notify.notified();
            if !self.wait_finished.load(Ordering::Acquire) {
                tokio::time::timeout(TERMINATE_TIMEOUT, notified)
                    .await
                    .map_err(|_| {
                        AppError::ServiceUnavailable(kill_error.unwrap_or_else(|| {
                            "Timed out waiting for terminal process teardown".to_string()
                        }))
                    })?;
            }
        }
        Ok(self.descriptor().await)
    }
}

#[derive(Debug)]
struct AttachSnapshot {
    descriptor: TerminalDescriptor,
    replay: ReplaySnapshot,
}

#[derive(Debug, Clone, Copy)]
struct ReplayDelivery {
    cursor: u64,
    terminal: bool,
}

struct TerminalManager {
    sessions: RwLock<HashMap<String, Arc<TerminalSession>>>,
    max_per_owner: usize,
    replay_bytes: usize,
    control_lease_ttl: Duration,
}

impl TerminalManager {
    fn production() -> Self {
        Self::new(
            MAX_TERMINALS_PER_OWNER,
            DEFAULT_REPLAY_BYTES,
            CONTROL_LEASE_TTL,
        )
    }

    fn new(max_per_owner: usize, replay_bytes: usize, control_lease_ttl: Duration) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            max_per_owner,
            replay_bytes,
            control_lease_ttl,
        }
    }

    async fn create(
        &self,
        owner: OwnerKey,
        working_dir: PathBuf,
        size: TerminalSize,
    ) -> Result<TerminalDescriptor, AppError> {
        let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
        self.create_with_shell(owner, working_dir, size, &shell)
            .await
    }

    async fn create_with_id(
        &self,
        owner: OwnerKey,
        working_dir: PathBuf,
        size: TerminalSize,
        id: &str,
    ) -> Result<TerminalDescriptor, AppError> {
        let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
        self.create_with_shell_and_id(owner, working_dir, size, &shell, id)
            .await
    }

    async fn create_with_shell(
        &self,
        owner: OwnerKey,
        working_dir: PathBuf,
        size: TerminalSize,
        shell: &str,
    ) -> Result<TerminalDescriptor, AppError> {
        self.prune_exited_owned(&owner).await;
        let mut sessions = self.sessions.write().await;
        let owned_count = sessions
            .values()
            .filter(|session| session.owner == owner)
            .count();
        if owned_count >= self.max_per_owner {
            return Err(AppError::Conflict(format!(
                "Terminal limit reached; close an existing terminal first (maximum {})",
                self.max_per_owner
            )));
        }
        let session = TerminalSession::spawn(
            owner,
            working_dir,
            size,
            self.replay_bytes,
            self.control_lease_ttl,
            shell,
            None,
        )?;
        let descriptor = session.descriptor().await;
        sessions.insert(descriptor.id.clone(), session);
        Ok(descriptor)
    }

    async fn create_with_shell_and_id(
        &self,
        owner: OwnerKey,
        working_dir: PathBuf,
        size: TerminalSize,
        shell: &str,
        id: &str,
    ) -> Result<TerminalDescriptor, AppError> {
        self.prune_exited_owned(&owner).await;
        let mut sessions = self.sessions.write().await;
        if sessions.contains_key(id) {
            return Err(AppError::Conflict(
                "Terminal create operation was reused with a different live session".to_string(),
            ));
        }
        let owned_count = sessions
            .values()
            .filter(|session| session.owner == owner)
            .count();
        if owned_count >= self.max_per_owner {
            return Err(AppError::Conflict(format!(
                "Terminal limit reached; close an existing terminal first (maximum {})",
                self.max_per_owner
            )));
        }
        let session = TerminalSession::spawn(
            owner,
            working_dir,
            size,
            self.replay_bytes,
            self.control_lease_ttl,
            shell,
            Some(id),
        )?;
        let descriptor = session.descriptor().await;
        sessions.insert(descriptor.id.clone(), session);
        Ok(descriptor)
    }

    async fn list_owned(&self, owner: &OwnerKey) -> Vec<TerminalDescriptor> {
        let sessions = self.sessions.read().await;
        let owned = sessions
            .values()
            .filter(|session| &session.owner == owner)
            .cloned()
            .collect::<Vec<_>>();
        drop(sessions);
        let mut descriptors = Vec::with_capacity(owned.len());
        for session in owned {
            descriptors.push(session.descriptor().await);
        }
        descriptors.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        descriptors
    }

    async fn owned_session(
        &self,
        owner: &OwnerKey,
        id: &str,
    ) -> Result<Arc<TerminalSession>, AppError> {
        self.sessions
            .read()
            .await
            .get(id)
            .filter(|session| &session.owner == owner)
            .cloned()
            .ok_or_else(|| AppError::NotFound("Terminal not found".to_string()))
    }

    async fn descriptor_for_canvas_binding(
        &self,
        owner: &OwnerKey,
        id: &str,
        canonical_project_dir: &Path,
    ) -> Result<TerminalDescriptor, AppError> {
        let descriptor = self.owned_session(owner, id).await?.descriptor().await;
        if Path::new(&descriptor.working_dir) != canonical_project_dir {
            return Err(AppError::BadRequest(
                "Terminal belongs to a different canvas project".to_string(),
            ));
        }
        Ok(descriptor)
    }

    async fn delete_owned(
        &self,
        owner: &OwnerKey,
        id: &str,
        lease_token: Option<&str>,
    ) -> Result<TerminalDescriptor, AppError> {
        let session = self.owned_session(owner, id).await?;
        let descriptor = session.descriptor().await;
        if descriptor.status != TerminalStatus::Exited {
            let lease_token = lease_token
                .filter(|token| !token.trim().is_empty())
                .ok_or_else(|| {
                    AppError::Conflict(
                        "Terminal control lease is required before terminating the process"
                            .to_string(),
                    )
                })?;
            session.authorize_control(lease_token).await?;
        }
        let descriptor = session.terminate().await?;
        self.remove_if_same(id, &session).await;
        Ok(descriptor)
    }

    async fn prune_exited_owned(&self, owner: &OwnerKey) {
        let candidates = self
            .sessions
            .read()
            .await
            .iter()
            .filter(|(_, session)| &session.owner == owner)
            .map(|(id, session)| (id.clone(), Arc::clone(session)))
            .collect::<Vec<_>>();
        let mut exited = Vec::new();
        for (id, session) in candidates {
            if session.descriptor().await.status == TerminalStatus::Exited {
                exited.push((id, session));
            }
        }
        for (id, session) in exited {
            self.remove_if_same(&id, &session).await;
        }
    }

    async fn remove_if_same(&self, id: &str, session: &Arc<TerminalSession>) {
        let mut sessions = self.sessions.write().await;
        if sessions
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            sessions.remove(id);
        }
    }
}

/// Validate a terminal node binding against the authoritative live terminal
/// registry. Wrong-owner identifiers deliberately collapse to `NotFound`, and
/// the terminal's canonical working directory must exactly match the canvas's
/// canonical project directory.
pub(crate) async fn validate_terminal_canvas_binding(
    user: &AuthenticatedUser,
    terminal_id: &str,
    canonical_project_dir: &Path,
) -> Result<TerminalDescriptor, AppError> {
    manager()
        .descriptor_for_canvas_binding(
            &OwnerKey::from_user(user),
            terminal_id,
            canonical_project_dir,
        )
        .await
}

pub(crate) async fn ensure_terminal_for_canvas(
    user: &AuthenticatedUser,
    terminal_id: &str,
    canonical_project_dir: &Path,
    size: TerminalSize,
) -> Result<TerminalDescriptor, AppError> {
    match validate_terminal_canvas_binding(user, terminal_id, canonical_project_dir).await {
        Ok(descriptor) => Ok(descriptor),
        Err(AppError::NotFound(_)) => {
            manager()
                .create_with_id(
                    OwnerKey::from_user(user),
                    canonical_project_dir.to_path_buf(),
                    size,
                    terminal_id,
                )
                .await
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
pub(crate) async fn remove_canvas_test_terminal(
    user: &AuthenticatedUser,
    terminal_id: &str,
) -> Result<(), AppError> {
    let owner = OwnerKey::from_user(user);
    let session = manager().owned_session(&owner, terminal_id).await?;
    let lease = session
        .take_control("canvas-test-cleanup".to_owned())
        .await?;
    manager()
        .delete_owned(&owner, terminal_id, Some(&lease.lease_token))
        .await
        .map(|_| ())
}

pub(crate) async fn cleanup_terminal_for_canvas(
    user: &AuthenticatedUser,
    terminal_id: &str,
    canonical_project_dir: &Path,
    _size: TerminalSize,
    absence_is_authoritative: bool,
) -> Result<(), AppError> {
    match validate_terminal_canvas_binding(user, terminal_id, canonical_project_dir).await {
        Ok(_) => {
            let owner = OwnerKey::from_user(user);
            let session = manager().owned_session(&owner, terminal_id).await?;
            let lease = session
                .take_control("canvas-resource-cleanup".to_owned())
                .await?;
            manager()
                .delete_owned(&owner, terminal_id, Some(&lease.lease_token))
                .await
                .map(|_| ())
        }
        Err(AppError::NotFound(_)) if absence_is_authoritative => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) async fn classify_terminal_cleanup_for_canvas(
    user: &AuthenticatedUser,
    terminal_id: &str,
    canonical_project_dir: &Path,
    _size: TerminalSize,
    _lease_token: Option<&str>,
    absence_is_authoritative: bool,
) -> Result<super::canvas_resources::BoundCleanupProof, AppError> {
    match validate_terminal_canvas_binding(user, terminal_id, canonical_project_dir).await {
        Ok(_) => Ok(super::canvas_resources::BoundCleanupProof::LiveLease),
        Err(AppError::NotFound(_)) if absence_is_authoritative => {
            Ok(super::canvas_resources::BoundCleanupProof::AuthoritativelyEnded)
        }
        Err(error) => Err(error),
    }
}

#[derive(Debug, Deserialize)]
struct AttachQuery {
    after_offset: Option<u64>,
    #[allow(dead_code)]
    token: Option<String>,
}

async fn create_terminal(
    State(state): State<AppState>,
    CurrentUser(user): CurrentUser,
    Json(spec): Json<TerminalSpec>,
) -> Result<Json<TerminalDescriptor>, AppError> {
    let working_dir =
        resolve_terminal_working_dir(&user, &state.working_dir, spec.working_dir.as_deref())?;
    manager()
        .create(OwnerKey::from_user(&user), working_dir, spec.size)
        .await
        .map(Json)
}

async fn list_terminals(CurrentUser(user): CurrentUser) -> Json<TerminalListResponse> {
    Json(TerminalListResponse {
        terminals: manager().list_owned(&OwnerKey::from_user(&user)).await,
        max_sessions: MAX_TERMINALS_PER_OWNER,
    })
}

async fn get_terminal(
    CurrentUser(user): CurrentUser,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<TerminalDescriptor>, AppError> {
    let session = manager()
        .owned_session(&OwnerKey::from_user(&user), &id)
        .await?;
    Ok(Json(session.descriptor().await))
}

async fn take_terminal_control(
    CurrentUser(user): CurrentUser,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<TakeTerminalControlRequest>,
) -> Result<Json<TakeTerminalControlResponse>, AppError> {
    let session = manager()
        .owned_session(&OwnerKey::from_user(&user), &id)
        .await?;
    let lease = session.take_control(request.client_id).await?;
    Ok(Json(TakeTerminalControlResponse { lease }))
}

async fn delete_terminal(
    CurrentUser(user): CurrentUser,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<TerminalDescriptor>, AppError> {
    let lease_token = optional_control_lease_header(&headers)?;
    manager()
        .delete_owned(&OwnerKey::from_user(&user), &id, lease_token)
        .await
        .map(Json)
}

fn optional_control_lease_header(headers: &HeaderMap) -> Result<Option<&str>, AppError> {
    let Some(value) = headers.get(CONTROL_LEASE_HEADER) else {
        return Ok(None);
    };
    let lease_token = value
        .to_str()
        .map_err(|_| AppError::BadRequest("Terminal control lease header is invalid".to_string()))?
        .trim();
    Ok((!lease_token.is_empty()).then_some(lease_token))
}

async fn stream_terminal(
    State(state): State<AppState>,
    identity: Option<axum::Extension<crate::device_access::DeviceIdentity>>,
    CurrentUser(user): CurrentUser,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<AttachQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let session = manager()
        .owned_session(&OwnerKey::from_user(&user), &id)
        .await?;
    let guard = crate::device_access::DeviceConnectionGuard::new(&state, identity.map(|i| i.0));
    Ok(upgrade
        .on_upgrade(move |socket| {
            guard.run(handle_terminal_socket(socket, session, query.after_offset))
        })
        .into_response())
}

async fn handle_terminal_socket(
    socket: WebSocket,
    session: Arc<TerminalSession>,
    after_offset: Option<u64>,
) {
    let (mut sink, mut source) = socket.split();
    // Subscribe before taking the replay snapshot. Events that race with the
    // snapshot can be duplicated, but monotonic offsets let us discard them;
    // subscribing afterward could lose an event entirely.
    let mut events = session.subscribe();
    let Ok(initial) = send_replay(&mut sink, &session, after_offset, true).await else {
        return;
    };
    if initial.terminal {
        return;
    }
    let mut cursor = initial.cursor;

    loop {
        tokio::select! {
            event = events.recv() => {
                match event {
                    Ok(TerminalTransportEvent::Output { offset, bytes }) => {
                        let end = offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                        if end <= cursor {
                            continue;
                        }
                        if offset > cursor {
                            match send_replay(&mut sink, &session, Some(cursor), false).await {
                                Ok(delivery) if delivery.terminal => break,
                                Ok(delivery) => cursor = delivery.cursor,
                                Err(()) => break,
                            }
                            continue;
                        }
                        let trim = usize::try_from(cursor.saturating_sub(offset)).unwrap_or(usize::MAX);
                        if trim >= bytes.len() {
                            continue;
                        }
                        let event = TerminalTransportEvent::Output {
                            offset: offset.saturating_add(u64::try_from(trim).unwrap_or(u64::MAX)),
                            bytes: bytes[trim..].to_vec(),
                        };
                        cursor = end;
                        if send_event(&mut sink, &event).await.is_err() {
                            break;
                        }
                    }
                    Ok(event) => {
                        if send_event(&mut sink, &event).await.is_err() {
                            break;
                        }
                        if matches!(event, TerminalTransportEvent::Exited { .. }) {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        match send_replay(&mut sink, &session, Some(cursor), false).await {
                            Ok(delivery) if delivery.terminal => break,
                            Ok(delivery) => cursor = delivery.cursor,
                            Err(()) => break,
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            message = source.next() => {
                let Some(Ok(message)) = message else { break };
                match message {
                    Message::Binary(bytes) => {
                        // Raw binary frames cannot carry a control lease. V2
                        // input uses the base64-backed typed command instead.
                        tracing::warn!(bytes = bytes.len(), "Rejected unauthenticated raw terminal input frame");
                        if send_command_error(
                            &mut sink,
                            AppError::Conflict(
                                "Raw terminal input cannot carry a control lease; use a typed input command"
                                    .to_string(),
                            ),
                        )
                        .await
                        .is_err()
                        {
                            break;
                        }
                    }
                    Message::Text(text) => {
                        if text.len() > MAX_COMMAND_BYTES {
                            tracing::warn!(bytes = text.len(), "Rejected oversized terminal command");
                            let _ = send_command_error(
                                &mut sink,
                                AppError::BadRequest("Terminal command is too large".to_string()),
                            )
                            .await;
                            break;
                        }
                        let Ok(command) = serde_json::from_str::<TerminalTransportCommand>(&text) else {
                            let terminal_id = session.descriptor().await.id;
                            tracing::warn!(%terminal_id, "Rejected malformed terminal command");
                            if send_command_error(
                                &mut sink,
                                AppError::BadRequest("Malformed terminal command".to_string()),
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                            continue;
                        };
                        match command {
                            TerminalTransportCommand::Input { lease_token, bytes } => {
                                if let Err(error) = session.write_input(&lease_token, bytes).await {
                                    if send_command_error(&mut sink, error).await.is_err() { break; }
                                }
                            }
                            TerminalTransportCommand::InputText { lease_token, text } => {
                                if let Err(error) = session.write_input(&lease_token, text.into_bytes()).await {
                                    if send_command_error(&mut sink, error).await.is_err() { break; }
                                }
                            }
                            TerminalTransportCommand::Resize { lease_token, cols, rows } => {
                                if let Err(error) = session.resize(&lease_token, TerminalSize { cols, rows }).await {
                                    if send_command_error(&mut sink, error).await.is_err() { break; }
                                }
                            }
                            TerminalTransportCommand::Ping { lease_token } => {
                                if let Some(lease_token) = lease_token {
                                    if let Err(error) = session.authorize_control(&lease_token).await {
                                        if send_command_error(&mut sink, error).await.is_err() { break; }
                                        continue;
                                    }
                                }
                                if send_event(&mut sink, &TerminalTransportEvent::Pong).await.is_err() { break; }
                            }
                            TerminalTransportCommand::Detach => {
                                let _ = send_event(
                                    &mut sink,
                                    &TerminalTransportEvent::Disconnected {
                                        reason: "client_detached".to_string(),
                                    },
                                ).await;
                                break;
                            }
                            TerminalTransportCommand::Terminate { lease_token } => {
                                if let Err(error) = session.authorize_control(&lease_token).await {
                                    if send_command_error(&mut sink, error).await.is_err() { break; }
                                    continue;
                                }
                                if let Err(error) = session.terminate().await {
                                    if send_command_error(&mut sink, error).await.is_err() { break; }
                                } else {
                                    let id = session.descriptor().await.id;
                                    manager().remove_if_same(&id, &session).await;
                                }
                            }
                        }
                    }
                    Message::Ping(bytes) => {
                        if sink.send(Message::Pong(bytes)).await.is_err() { break; }
                    }
                    Message::Close(_) => break,
                    Message::Pong(_) => {}
                }
            }
        }
    }
}

async fn send_replay(
    sink: &mut futures::stream::SplitSink<WebSocket, Message>,
    session: &TerminalSession,
    requested_offset: Option<u64>,
    include_ready: bool,
) -> Result<ReplayDelivery, ()> {
    let snapshot = session.replay_snapshot(requested_offset).await;
    let replay = snapshot.replay;
    if include_ready {
        send_event(
            sink,
            &TerminalTransportEvent::Ready {
                terminal: snapshot.descriptor.clone(),
                replay_from: replay.replay_from,
                next_offset: replay.next_offset,
            },
        )
        .await?;
    }
    if let Some(gap) = replay.gap.as_ref() {
        send_event(sink, gap).await?;
    }
    for frame in replay.output {
        send_event(
            sink,
            &TerminalTransportEvent::Output {
                offset: frame.offset,
                bytes: frame.bytes,
            },
        )
        .await?;
    }
    let terminal = snapshot.descriptor.status == TerminalStatus::Exited;
    if terminal {
        send_event(
            sink,
            &TerminalTransportEvent::Exited {
                exit_code: snapshot.descriptor.exit_code,
                next_offset: replay.next_offset,
            },
        )
        .await?;
    } else if snapshot.descriptor.status == TerminalStatus::Disconnected {
        send_event(
            sink,
            &TerminalTransportEvent::Disconnected {
                reason: snapshot
                    .descriptor
                    .last_error
                    .unwrap_or_else(|| "Terminal transport disconnected".to_string()),
            },
        )
        .await?;
    }
    Ok(ReplayDelivery {
        cursor: replay.next_offset,
        terminal,
    })
}

async fn send_event(
    sink: &mut futures::stream::SplitSink<WebSocket, Message>,
    event: &TerminalTransportEvent,
) -> Result<(), ()> {
    let encoded = serde_json::to_string(event).map_err(|error| {
        tracing::error!(%error, "Failed to serialize terminal transport event");
    })?;
    sink.send(Message::Text(encoded)).await.map_err(|_| ())
}

async fn send_command_error(
    sink: &mut futures::stream::SplitSink<WebSocket, Message>,
    error: AppError,
) -> Result<(), ()> {
    let (code, message) = match error {
        AppError::NotFound(message) => ("not_found", message),
        AppError::BadRequest(message) => ("bad_request", message),
        AppError::Conflict(message) => ("conflict", message),
        AppError::ServiceUnavailable(message) => ("service_unavailable", message),
        AppError::BadGateway(message) => ("bad_gateway", message),
        AppError::Internal(message) => {
            tracing::error!(%message, "Terminal command failed internally");
            ("internal_error", "Internal terminal error".to_string())
        }
    };
    send_event(
        sink,
        &TerminalTransportEvent::CommandRejected {
            code: code.to_string(),
            message,
        },
    )
    .await
}

fn resolve_terminal_working_dir(
    user: &AuthenticatedUser,
    server_working_dir: &Path,
    requested: Option<&str>,
) -> Result<PathBuf, AppError> {
    let base = workspace_base(user.home_dir.as_deref(), server_working_dir);
    let root = allowed_root(user.home_dir.as_deref(), &base);
    let resolved = resolve_scoped_workspace_path(requested, &base, &root)?;
    if !resolved.is_dir() {
        return Err(AppError::BadRequest(
            "Terminal working directory must be an existing directory".to_string(),
        ));
    }
    let canonical = resolved.canonicalize().map_err(|error| {
        AppError::BadRequest(format!(
            "Could not resolve terminal working directory: {error}"
        ))
    })?;
    validate_path_within(&root, &canonical)?;
    Ok(canonical)
}

fn clamp_terminal_size(size: TerminalSize) -> TerminalSize {
    TerminalSize {
        cols: size.cols.clamp(1, MAX_TERMINAL_COLS),
        rows: size.rows.clamp(1, MAX_TERMINAL_ROWS),
    }
}

fn to_pty_size(size: TerminalSize) -> PtySize {
    PtySize {
        rows: size.rows,
        cols: size.cols,
        pixel_width: 0,
        pixel_height: 0,
    }
}

fn prune_expired_control(state: &mut TerminalRuntimeState, ttl: Duration) -> bool {
    let expired = state
        .control
        .as_ref()
        .is_some_and(|control| control.last_seen.elapsed() >= ttl);
    if expired {
        state.control = None;
        state.descriptor.controller = None;
        state.descriptor.updated_at = chrono::Utc::now().to_rfc3339();
    } else if let Some(control) = state.control.as_ref() {
        state.descriptor.controller = Some(control_summary(control, ttl));
    }
    expired
}

fn control_summary(control: &ControlLeaseState, ttl: Duration) -> ControlLeaseSummary {
    let remaining = ttl.saturating_sub(control.last_seen.elapsed());
    let expires_at = chrono::Utc::now()
        + chrono::Duration::from_std(remaining).unwrap_or_else(|_| chrono::Duration::zero());
    ControlLeaseSummary {
        client_id: control.client_id.clone(),
        expires_at: expires_at.to_rfc3339(),
    }
}

fn constant_time_token_eq(expected: &str, candidate: &str) -> bool {
    if expected.len() != candidate.len() {
        return false;
    }
    expected
        .bytes()
        .zip(candidate.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn is_expected_pty_eof(error: &std::io::Error) -> bool {
    // Unix PTY masters conventionally return EIO after the slave side closes.
    // It is the terminal equivalent of EOF, not a transport failure.
    #[cfg(unix)]
    if error.raw_os_error() == Some(5) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn owner(value: Option<&str>) -> OwnerKey {
        OwnerKey(value.map(ToOwned::to_owned))
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("condition should become true");
    }

    async fn wait_for_replay_text(
        session: &TerminalSession,
        needles: &[&str],
    ) -> (AttachSnapshot, String) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = session.replay_snapshot(Some(0)).await;
                let bytes = snapshot
                    .replay
                    .output
                    .iter()
                    .flat_map(|frame| frame.bytes.iter().copied())
                    .collect::<Vec<_>>();
                let text = String::from_utf8_lossy(&bytes).into_owned();
                if needles.iter().all(|needle| text.contains(needle)) {
                    break (snapshot, text);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("terminal replay should contain every expected marker")
    }

    #[test]
    fn replay_offsets_are_monotonic_and_report_trimmed_gaps() {
        let mut replay = ReplayBuffer::new(5);
        assert_eq!(replay.append(b"abc".to_vec()).offset, 0);
        assert_eq!(replay.append(b"defg".to_vec()).offset, 3);
        assert_eq!(replay.available_from(), 2);
        assert_eq!(replay.next_offset, 7);

        let snapshot = replay.snapshot(Some(0));
        assert!(matches!(
            snapshot.gap,
            Some(TerminalTransportEvent::Gap {
                requested_offset: 0,
                available_from: 2,
                next_offset: 7,
            })
        ));
        assert_eq!(snapshot.output[0].offset, 2);
        assert_eq!(snapshot.output[0].bytes, b"c");
        assert_eq!(snapshot.output[1].offset, 3);
        assert_eq!(snapshot.output[1].bytes, b"defg");

        let partial = replay.snapshot(Some(5));
        assert!(partial.gap.is_none());
        assert_eq!(partial.output.len(), 1);
        assert_eq!(partial.output[0].offset, 5);
        assert_eq!(partial.output[0].bytes, b"fg");
    }

    #[test]
    fn working_directory_is_canonical_and_symlink_scoped() {
        let root = TempDir::new().expect("root tempdir");
        let project = root.path().join("project");
        std::fs::create_dir(&project).expect("project should exist");
        let user = AuthenticatedUser {
            user_id: Some("owner".to_string()),
            home_dir: Some(root.path().to_path_buf()),
        };
        let resolved = resolve_terminal_working_dir(
            &user,
            root.path(),
            Some(project.to_str().expect("utf8 path")),
        )
        .expect("project should resolve");
        assert_eq!(resolved, project.canonicalize().unwrap());

        let outside = TempDir::new().expect("outside tempdir");
        assert!(resolve_terminal_working_dir(
            &user,
            root.path(),
            Some(outside.path().to_str().expect("utf8 path")),
        )
        .is_err());

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), root.path().join("escape"))
                .expect("symlink should be created");
            assert!(resolve_terminal_working_dir(&user, root.path(), Some("escape")).is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_is_exact_and_session_survives_transport_detach() {
        let temp = TempDir::new().expect("tempdir");
        let manager = TerminalManager::new(2, 4096, CONTROL_LEASE_TTL);
        let descriptor = manager
            .create_with_shell(
                owner(None),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/sh",
            )
            .await
            .expect("terminal should start");
        assert!(manager.list_owned(&owner(Some("local"))).await.is_empty());

        let session = manager
            .owned_session(&owner(None), &descriptor.id)
            .await
            .expect("owner should see terminal");
        assert!(manager
            .owned_session(&owner(Some("local")), &descriptor.id)
            .await
            .is_err());

        let canonical_project = temp.path().canonicalize().expect("canonical project");
        assert_eq!(
            manager
                .descriptor_for_canvas_binding(&owner(None), &descriptor.id, &canonical_project)
                .await
                .expect("matching owner and project should bind")
                .id,
            descriptor.id
        );
        assert!(manager
            .descriptor_for_canvas_binding(
                &owner(Some("local")),
                &descriptor.id,
                &canonical_project,
            )
            .await
            .is_err());
        let other_project = temp.path().join("other-project");
        std::fs::create_dir(&other_project).expect("other project should exist");
        assert!(manager
            .descriptor_for_canvas_binding(
                &owner(None),
                &descriptor.id,
                &other_project
                    .canonicalize()
                    .expect("canonical other project"),
            )
            .await
            .is_err());

        let attachment = session.subscribe();
        drop(attachment);
        let lease = session
            .take_control("detached-client".to_string())
            .await
            .expect("client should take control");
        session
            .write_input(&lease.lease_token, b"printf detach-survived\\n".to_vec())
            .await
            .expect("detached terminal should accept input");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = session.replay_snapshot(Some(0)).await;
                let bytes = snapshot
                    .replay
                    .output
                    .into_iter()
                    .flat_map(|frame| frame.bytes)
                    .collect::<Vec<_>>();
                if String::from_utf8_lossy(&bytes).contains("detach-survived") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("terminal output should be replayable after detach");

        manager
            .delete_owned(&owner(None), &descriptor.id, Some(&lease.lease_token))
            .await
            .expect("terminal should terminate");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pty_reports_exact_cwd_resized_stty_and_utf8_after_detach() {
        let temp = TempDir::new().expect("tempdir");
        let canonical_project = temp.path().canonicalize().expect("canonical project");
        let manager = TerminalManager::new(1, 64 * 1024, CONTROL_LEASE_TTL);
        let descriptor = manager
            .create_with_shell(
                owner(Some("pty-owner")),
                canonical_project.clone(),
                TerminalSize::default(),
                "/bin/sh",
            )
            .await
            .expect("terminal should start");
        let session = manager
            .owned_session(&owner(Some("pty-owner")), &descriptor.id)
            .await
            .expect("terminal should remain owned");
        let attachment = session.subscribe();
        drop(attachment);
        let lease = session
            .take_control("pty-client".to_owned())
            .await
            .expect("control lease");
        session
            .resize(
                &lease.lease_token,
                TerminalSize {
                    cols: 101,
                    rows: 33,
                },
            )
            .await
            .expect("resize");

        session
            .write_input(&lease.lease_token, b"stty -echo\n".to_vec())
            .await
            .expect("disable echo");
        tokio::time::sleep(Duration::from_millis(50)).await;
        session
            .write_input(
                &lease.lease_token,
                "printf '__CWD__'; pwd; printf '__SIZE__'; stty size; printf '__UTF__界🙂\\n'\n"
                    .as_bytes()
                    .to_vec(),
            )
            .await
            .expect("probe input");

        let (_, output) = wait_for_replay_text(
            &session,
            &[
                &format!("__CWD__{}", canonical_project.display()),
                "__SIZE__33 101",
                "__UTF__界🙂",
            ],
        )
        .await;
        assert!(output.contains("__UTF__界🙂"));
        let after = session.descriptor().await;
        assert_eq!(after.pid, descriptor.pid);
        assert_eq!(after.status, TerminalStatus::Running);

        manager
            .delete_owned(
                &owner(Some("pty-owner")),
                &descriptor.id,
                Some(&lease.lease_token),
            )
            .await
            .expect("terminate");
        wait_until(|| session.wait_finished.load(Ordering::Acquire)).await;
        wait_until(|| session.reader_finished.load(Ordering::Acquire)).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn output_flood_keeps_replay_bounded_and_input_responsive() {
        const REPLAY_LIMIT: usize = 8 * 1024;

        let temp = TempDir::new().expect("tempdir");
        let manager = TerminalManager::new(1, REPLAY_LIMIT, CONTROL_LEASE_TTL);
        let descriptor = manager
            .create_with_shell(
                owner(Some("flood-owner")),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/sh",
            )
            .await
            .expect("terminal should start");
        let session = manager
            .owned_session(&owner(Some("flood-owner")), &descriptor.id)
            .await
            .expect("terminal should exist");
        let lease = session
            .take_control("flood-client".to_owned())
            .await
            .expect("control lease");
        session
            .write_input(&lease.lease_token, b"stty -echo\n".to_vec())
            .await
            .expect("disable echo");
        tokio::time::sleep(Duration::from_millis(50)).await;
        session
            .write_input(
                &lease.lease_token,
                b"i=0; while [ \"$i\" -lt 12000 ]; do printf '0123456789abcdef'; i=$((i+1)); done; printf '\\n__FLOOD_DONE__\\n'; printf '__RESPONSIVE__\\n'\n".to_vec(),
            )
            .await
            .expect("flood command");

        let (snapshot, output) =
            wait_for_replay_text(&session, &["__FLOOD_DONE__", "__RESPONSIVE__"]).await;
        let retained_bytes = snapshot
            .replay
            .output
            .iter()
            .map(|frame| frame.bytes.len())
            .sum::<usize>();
        assert!(retained_bytes <= REPLAY_LIMIT);
        assert!(snapshot.replay.replay_from > 0);
        assert!(snapshot.replay.next_offset > REPLAY_LIMIT as u64);
        assert!(output.contains("__RESPONSIVE__"));

        manager
            .delete_owned(
                &owner(Some("flood-owner")),
                &descriptor.id,
                Some(&lease.lease_token),
            )
            .await
            .expect("terminate");
        wait_until(|| session.wait_finished.load(Ordering::Acquire)).await;
        wait_until(|| session.reader_finished.load(Ordering::Acquire)).await;
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resize_terminate_and_cleanup_are_bounded() {
        let temp = TempDir::new().expect("tempdir");
        let manager = TerminalManager::new(1, 4096, CONTROL_LEASE_TTL);
        let descriptor = manager
            .create_with_shell(
                owner(Some("a")),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/sh",
            )
            .await
            .expect("terminal should start");
        assert!(manager
            .create_with_shell(
                owner(Some("a")),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/sh"
            )
            .await
            .is_err());
        let session = manager
            .owned_session(&owner(Some("a")), &descriptor.id)
            .await
            .expect("terminal should exist");
        let lease = session
            .take_control("resize-client".to_string())
            .await
            .expect("client should take control");

        let resized = session
            .resize(
                &lease.lease_token,
                TerminalSize {
                    cols: 120,
                    rows: 44,
                },
            )
            .await
            .expect("resize should succeed");
        assert_eq!(
            resized.size,
            TerminalSize {
                cols: 120,
                rows: 44
            }
        );

        let terminated = manager
            .delete_owned(&owner(Some("a")), &descriptor.id, Some(&lease.lease_token))
            .await
            .expect("delete should reap the child");
        assert_eq!(terminated.status, TerminalStatus::Exited);
        assert!(manager
            .owned_session(&owner(Some("a")), &descriptor.id)
            .await
            .is_err());
        wait_until(|| session.wait_finished.load(Ordering::Acquire)).await;
        wait_until(|| session.reader_finished.load(Ordering::Acquire)).await;
    }

    #[test]
    fn delete_route_accepts_an_optional_control_lease_header() {
        let headers = HeaderMap::new();
        assert_eq!(optional_control_lease_header(&headers).unwrap(), None);

        let mut headers = HeaderMap::new();
        headers.insert(CONTROL_LEASE_HEADER, "  ".parse().expect("header"));
        assert_eq!(optional_control_lease_header(&headers).unwrap(), None);
        headers.insert(
            CONTROL_LEASE_HEADER,
            "opaque-lease".parse().expect("header"),
        );
        assert_eq!(
            optional_control_lease_header(&headers).expect("header"),
            Some("opaque-lease")
        );
    }

    #[test]
    fn production_control_lease_uses_canvas_stale_window() {
        assert_eq!(CONTROL_LEASE_TTL, Duration::from_secs(15));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn natural_exit_can_be_cleaned_without_a_lease_and_capacity_reused() {
        let temp = TempDir::new().expect("tempdir");
        let manager = TerminalManager::new(1, 4096, CONTROL_LEASE_TTL);
        let owner = owner(Some("natural-exit-owner"));
        let first = manager
            .create_with_shell(
                owner.clone(),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/true",
            )
            .await
            .expect("short-lived terminal should start");
        let session = manager
            .owned_session(&owner, &first.id)
            .await
            .expect("terminal should be registered");
        wait_until(|| session.wait_finished.load(Ordering::Acquire)).await;
        assert_eq!(session.descriptor().await.status, TerminalStatus::Exited);

        manager
            .delete_owned(&owner, &first.id, None)
            .await
            .expect("exact owner may clean an already-exited terminal without a lease");
        let replacement = manager
            .create_with_shell(
                owner.clone(),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/true",
            )
            .await
            .expect("cleaned capacity should be reusable");
        let replacement_session = manager
            .owned_session(&owner, &replacement.id)
            .await
            .expect("replacement should exist");
        wait_until(|| replacement_session.wait_finished.load(Ordering::Acquire)).await;
        manager
            .delete_owned(&owner, &replacement.id, None)
            .await
            .expect("replacement cleanup");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_requires_the_current_controller_lease() {
        let temp = TempDir::new().expect("tempdir");
        let manager = TerminalManager::new(1, 4096, CONTROL_LEASE_TTL);
        let descriptor = manager
            .create_with_shell(
                owner(Some("delete-owner")),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/sh",
            )
            .await
            .expect("terminal should start");

        assert!(manager
            .delete_owned(&owner(Some("delete-owner")), &descriptor.id, None)
            .await
            .is_err());
        let session = manager
            .owned_session(&owner(Some("delete-owner")), &descriptor.id)
            .await
            .expect("failed observer delete must preserve terminal");
        let first = session
            .take_control("first-delete-client".to_owned())
            .await
            .expect("first control lease");
        let second = session
            .take_control("second-delete-client".to_owned())
            .await
            .expect("replacement control lease");
        assert!(manager
            .delete_owned(
                &owner(Some("delete-owner")),
                &descriptor.id,
                Some(&first.lease_token),
            )
            .await
            .is_err());
        assert!(manager
            .owned_session(&owner(Some("delete-owner")), &descriptor.id)
            .await
            .is_ok());

        let deleted = manager
            .delete_owned(
                &owner(Some("delete-owner")),
                &descriptor.id,
                Some(&second.lease_token),
            )
            .await
            .expect("current controller may terminate");
        assert_eq!(deleted.status, TerminalStatus::Exited);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn observer_mutations_fail_and_explicit_take_control_transfers_authority() {
        let temp = TempDir::new().expect("tempdir");
        let manager = TerminalManager::new(1, 4096, Duration::from_millis(40));
        let descriptor = manager
            .create_with_shell(
                owner(Some("lease-owner")),
                temp.path().to_path_buf(),
                TerminalSize::default(),
                "/bin/sh",
            )
            .await
            .expect("terminal should start");
        let session = manager
            .owned_session(&owner(Some("lease-owner")), &descriptor.id)
            .await
            .expect("terminal should exist");

        assert!(session
            .write_input("observer-has-no-token", b"echo denied\\n".to_vec())
            .await
            .is_err());

        let first = session
            .take_control("first-client".to_string())
            .await
            .expect("first client should take control");
        session
            .authorize_control(&first.lease_token)
            .await
            .expect("first lease should authorize");

        let second = session
            .take_control("second-client".to_string())
            .await
            .expect("explicit take control should transfer authority");
        assert!(session.authorize_control(&first.lease_token).await.is_err());
        session
            .authorize_control(&second.lease_token)
            .await
            .expect("replacement lease should authorize");

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(session
            .authorize_control(&second.lease_token)
            .await
            .is_err());
        assert!(session.descriptor().await.controller.is_none());

        let cleanup = session
            .take_control("cleanup-client".to_string())
            .await
            .expect("cleanup client should take control");
        manager
            .delete_owned(
                &owner(Some("lease-owner")),
                &descriptor.id,
                Some(&cleanup.lease_token),
            )
            .await
            .expect("terminal should terminate");
    }
}
