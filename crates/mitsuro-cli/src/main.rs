//! Mitsuro - local-first AI coding assistant
//!
//! A terminal-based AI coding assistant with:
//! - Multi-provider AI with API key authentication
//! - Single-mode Chat UI with slash commands
//! - `mitsuro serve` — unified server + embedded web app + Tailscale
//! - Clean architecture from day one

use std::collections::HashMap;
use std::io::{self, Write};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use serde::{de::DeserializeOwned, Deserialize};

// Re-export core modules for TUI usage
// Crate-root re-exports so terminal modules can use `crate::agent`, etc.
pub(crate) use mitsuro_core::{
    acp, agent, ai, extensions, paths, plan, plugins, process, storage, tools,
};

mod serve;
mod tui_support;
mod tui_v2;
mod update_cmd;

/// Mitsuro - AI Coding Assistant
#[derive(Parser)]
#[command(name = "mitsuro")]
#[command(
    about = "Mitsuro, a local-first AI coding assistant",
    long_about = None,
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Copy legacy state into the canonical Mitsuro root while offline
    #[command(hide = true)]
    MigrateIdentity {
        /// Confirm that every CLI, TUI, desktop, server, and Hive process is stopped
        #[arg(long)]
        confirm_offline: bool,
    },

    /// Run as ACP (Agent Client Protocol) server
    ///
    /// Mitsuro runs as an ACP-compatible Agent that communicates
    /// via JSON-RPC over stdin/stdout. This mode is used when Mitsuro is
    /// spawned by an ACP-compatible editor (Zed, Neovim, etc.).
    ///
    /// Uses credentials from TUI configuration, or override with env vars:
    /// - MITSURO_PROVIDER + MITSURO_API_KEY (+ optional MITSURO_MODEL)
    /// - Or provider-specific: ANTHROPIC_API_KEY, OPENROUTER_API_KEY, etc.
    Acp,

    /// Start the Mitsuro web server with embedded web frontend
    ///
    /// Launches the API server with the web bundle embedded into the binary.
    /// On first run, prompts for provider and API key configuration.
    /// Automatically configures Tailscale for remote access if available.
    Serve {
        /// Port to listen on
        #[arg(short, long, default_value_t = 3000)]
        port: u16,
        /// Run as the persistent loopback companion for native desktop clients
        #[arg(long)]
        desktop_managed: bool,
    },

    /// Check for or apply a Mitsuro release update
    Update {
        /// Download the latest release and replace this install
        #[arg(long)]
        apply: bool,
        /// Install this version instead of GitHub latest (for example 0.9.23)
        #[arg(long)]
        version: Option<String>,
    },

    /// Hive autonomous agent system
    #[command(name = "hive", alias = "mako", args_conflicts_with_subcommands = true)]
    Hive {
        #[command(subcommand)]
        command: Option<HiveCommand>,
        /// Submit a task directly without specifying `run`
        #[arg(value_name = "TASK")]
        task: Option<String>,
        /// Project directory (shorthand task submission only)
        #[arg(long, requires = "task")]
        project_dir: Option<String>,
        /// Attach to the live event stream after shorthand task submission
        #[arg(long, requires = "task")]
        attach: bool,
    },
}

#[derive(Subcommand)]
enum HiveCommand {
    /// Submit a task to Hive
    Run {
        /// The task to perform
        task: String,
        /// Project directory (defaults to current)
        #[arg(long)]
        project_dir: Option<String>,
        /// Attach to the live event stream after dispatch
        #[arg(long)]
        attach: bool,
    },
    /// Show status for Hive sessions
    Status {
        /// Optional session id for detailed status
        session_id: Option<String>,
    },
    /// Attach to a running Hive session's live event stream
    Attach {
        /// Hive session id
        session_id: String,
    },
    /// Pause a running Hive session
    Pause {
        /// Hive session id
        session_id: String,
    },
    /// Resume a paused or idle Hive session
    Resume {
        /// Hive session id
        session_id: String,
    },
    /// Cancel and delete a Hive session
    Cancel {
        /// Hive session id
        session_id: String,
    },
    /// Send a follow-up message to an existing Hive session
    Send {
        /// Hive session id
        session_id: String,
        /// Follow-up message
        message: String,
    },
    /// List Hive Workers or open a Worker DM
    Workers {
        #[command(subcommand)]
        action: HiveWorkersAction,
    },
    /// List Groups, send a room message, or attach to a Group timeline
    Groups {
        #[command(subcommand)]
        action: HiveGroupsAction,
    },
}

#[derive(Subcommand)]
enum HiveWorkersAction {
    /// List durable Hive Workers
    List,
    /// Ensure a Worker DM exists and print its session id
    Dm {
        /// Worker id or slug
        worker_id: String,
    },
}

#[derive(Subcommand)]
enum HiveGroupsAction {
    /// List Groups
    List,
    /// Send a user message into a Group room
    Send {
        /// Group id
        group_id: String,
        /// Room message
        message: String,
    },
    /// Attach to a Group's live timeline
    Attach {
        /// Group id
        group_id: String,
    },
}

#[derive(Debug, Deserialize)]
struct HiveDispatchResponse {
    session_id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct HiveOkResponse {
    ok: bool,
}

#[derive(Debug, Deserialize)]
struct HiveRuntimeStateResponse {
    session_id: String,
    status: String,
    next_wake_at: Option<String>,
    sleep_reason: Option<String>,
    last_error: Option<String>,
    current_run_id: Option<String>,
    last_wake_reason: Option<String>,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
struct HiveSessionSummaryResponse {
    session_id: String,
    title: String,
    agent_state: String,
    runtime: Option<HiveRuntimeStateResponse>,
}

#[derive(Debug, Deserialize)]
struct HiveWorkersListResponse {
    workers: Vec<HiveWorkerListItem>,
}

#[derive(Debug, Deserialize)]
struct HiveWorkerListItem {
    id: String,
    slug: String,
    display_name: String,
    status: String,
    model: Option<String>,
    dm_session_id: Option<String>,
    dm_agent_state: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HiveWorkerDmCliResponse {
    worker_id: String,
    session_id: String,
    title: String,
    created: bool,
    agent_state: String,
}

#[derive(Debug, Deserialize)]
struct HiveGroupsListResponse {
    groups: Vec<HiveGroupListItem>,
}

#[derive(Debug, Deserialize)]
struct HiveGroupListItem {
    id: String,
    title: String,
    execution_mode: String,
    status: String,
    #[serde(default)]
    members: Vec<HiveGroupMemberListItem>,
    active_turn_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HiveGroupMemberListItem {
    slug: String,
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct HiveGroupSendResponse {
    group_id: String,
    turn_id: String,
    message_id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct HiveTaskResponse {
    id: String,
    subject: String,
    description: String,
    status: String,
    owner: Option<String>,
    blocked_by: Vec<String>,
    result: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HiveSessionStatusResponse {
    session_id: String,
    session_type: String,
    title: String,
    tasks: Vec<HiveTaskResponse>,
    agent_state: String,
    runtime: Option<HiveRuntimeStateResponse>,
}

#[derive(Debug, Default, Deserialize)]
struct DelegationSessionStateResponse {
    #[serde(default)]
    delegation_groups: Vec<CliDelegationGroup>,
    #[serde(default)]
    delegation_event_cursor: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CliDelegationGroup {
    delegation_group_id: String,
    state: String,
    #[serde(default)]
    tasks: Vec<CliDelegationTask>,
}

#[derive(Debug, Deserialize)]
struct CliDelegationTask {
    delegation_task_id: String,
    task_key: String,
    state: String,
    attempt_count: usize,
}

struct CliDelegationProjection {
    session_id: String,
    cursor: i64,
    group_tasks: HashMap<String, Vec<(String, String)>>,
}

impl CliDelegationProjection {
    fn new(session_id: &str) -> Self {
        Self {
            session_id: session_id.to_owned(),
            cursor: 0,
            group_tasks: HashMap::new(),
        }
    }

    fn hydrate(&mut self, state: &DelegationSessionStateResponse) {
        for group in &state.delegation_groups {
            self.group_tasks.insert(
                group.delegation_group_id.clone(),
                group
                    .tasks
                    .iter()
                    .map(|task| (task.delegation_task_id.clone(), task.task_key.clone()))
                    .collect(),
            );
            println!(
                "[delegation:group] {} {}",
                group.delegation_group_id, group.state
            );
            for task in &group.tasks {
                println!(
                    "[delegation:task] {} {} {} attempt={}",
                    task.delegation_task_id, task.task_key, task.state, task.attempt_count
                );
            }
        }
        self.cursor = self.cursor.max(state.delegation_event_cursor.unwrap_or(0));
    }

    fn print_event(&mut self, envelope: &serde_json::Value) {
        let event = envelope.get("event").unwrap_or(envelope);
        if event
            .get("parent_session_id")
            .and_then(serde_json::Value::as_str)
            != Some(self.session_id.as_str())
        {
            return;
        }
        let event_id = event
            .get("event_id")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if event_id > 0 && event_id <= self.cursor {
            return;
        }
        let kind = event
            .get("event_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let group_id = event
            .get("delegation_group_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let task_id = event
            .get("delegation_task_id")
            .and_then(serde_json::Value::as_str);
        let payload = event
            .get("payload")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        match kind {
            "group_created" => {
                println!("[delegation:group] {group_id} created");
                let tasks = payload
                    .get("tasks")
                    .and_then(serde_json::Value::as_array)
                    .map(|tasks| {
                        tasks
                            .iter()
                            .filter_map(|task| {
                                Some((
                                    task.get("delegation_task_id")?.as_str()?.to_owned(),
                                    task.get("task_key")?.as_str()?.to_owned(),
                                ))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for (task_id, task_key) in &tasks {
                    println!("[delegation:task] {task_id} {task_key} created attempt=0");
                }
                self.group_tasks.insert(group_id.to_owned(), tasks);
            }
            "group_queued" => {
                println!("[delegation:group] {group_id} queued");
                if let Some(tasks) = self.group_tasks.get(group_id) {
                    for (task_id, task_key) in tasks {
                        println!("[delegation:task] {task_id} {task_key} queued");
                    }
                }
            }
            "group_state_changed" => println!(
                "[delegation:group] {} {}",
                group_id,
                payload
                    .get("to")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
            ),
            "task_claimed" => println!("[delegation:task] {} leased", task_id.unwrap_or("unknown")),
            "task_running" => {
                println!("[delegation:task] {} running", task_id.unwrap_or("unknown"))
            }
            "task_state_changed" => println!(
                "[delegation:task] {} {}",
                task_id.unwrap_or("unknown"),
                payload
                    .get("state")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
            ),
            _ => println!("[delegation:event] {kind} {payload}"),
        }
        self.cursor = self.cursor.max(event_id);
    }
}

fn hive_server_url() -> String {
    mitsuro_core::identity::env_var("MITSURO_SERVER_URL")
        .unwrap_or_else(|_| "http://localhost:3001".to_string())
}

async fn run_hive_command(command: HiveCommand) -> Result<()> {
    let base = hive_server_url();
    let client = reqwest::Client::new();

    match command {
        HiveCommand::Run {
            task,
            project_dir,
            attach,
        } => {
            let mut body = serde_json::json!({ "task": task });
            if let Some(dir) = &project_dir {
                body["project_dir"] = serde_json::json!(dir);
            }

            let response: HiveDispatchResponse = request_json(
                client.post(format!("{base}/api/hive/dispatch")).json(&body),
                "Failed to dispatch Hive task",
            )
            .await?;

            println!("Hive task dispatched");
            println!("  Session: {}", response.session_id);
            println!("  Status: {}", response.status);
            println!("  Observe: mitsuro hive attach {}", response.session_id);
            println!("  Status: mitsuro hive status {}", response.session_id);

            if attach {
                attach_hive_session(&client, &base, &response.session_id).await?;
            }
        }
        HiveCommand::Status { session_id } => {
            if let Some(session_id) = session_id {
                let status: HiveSessionStatusResponse = request_json(
                    client.get(format!("{base}/api/hive/sessions/{session_id}/status")),
                    "Failed to fetch Hive session status",
                )
                .await?;
                print_hive_session_status(&status);
            } else {
                let sessions: Vec<HiveSessionSummaryResponse> = request_json(
                    client.get(format!("{base}/api/hive/sessions")),
                    "Failed to fetch Hive sessions",
                )
                .await?;
                print_hive_session_summaries(&sessions);
            }
        }
        HiveCommand::Attach { session_id } => {
            attach_hive_session(&client, &base, &session_id).await?;
        }
        HiveCommand::Pause { session_id } => {
            let response: HiveOkResponse = request_json(
                client.post(format!("{base}/api/hive/sessions/{session_id}/pause")),
                "Failed to pause Hive session",
            )
            .await?;
            if response.ok {
                println!("Paused Hive session {session_id}");
            }
        }
        HiveCommand::Resume { session_id } => {
            let response: HiveOkResponse = request_json(
                client.post(format!("{base}/api/hive/sessions/{session_id}/resume")),
                "Failed to resume Hive session",
            )
            .await?;
            if response.ok {
                println!("Resumed Hive session {session_id}");
            }
        }
        HiveCommand::Cancel { session_id } => {
            request_empty(
                client.delete(format!("{base}/api/hive/sessions/{session_id}")),
                "Failed to cancel Hive session",
            )
            .await?;
            println!("Cancelled Hive session {session_id}");
        }
        HiveCommand::Send {
            session_id,
            message,
        } => {
            let response: HiveOkResponse = request_json(
                client
                    .post(format!("{base}/api/hive/sessions/{session_id}/message"))
                    .json(&serde_json::json!({ "message": message })),
                "Failed to send message to Hive session",
            )
            .await?;
            if response.ok {
                println!("Queued message for Hive session {session_id}");
            }
        }
        HiveCommand::Workers { action } => match action {
            HiveWorkersAction::List => {
                let response: HiveWorkersListResponse = request_json(
                    client.get(format!("{base}/api/hive/workers")),
                    "Failed to list Hive Workers",
                )
                .await?;
                print_hive_workers(&response.workers);
            }
            HiveWorkersAction::Dm { worker_id } => {
                let resolved = resolve_hive_worker_id(&client, &base, &worker_id).await?;
                let response: HiveWorkerDmCliResponse = request_json(
                    client.post(format!("{base}/api/hive/workers/{resolved}/dm")),
                    "Failed to open Hive Worker DM",
                )
                .await?;
                println!("Hive Worker DM ready");
                println!("  Worker: {}", response.worker_id);
                println!("  Session: {}", response.session_id);
                println!("  Title: {}", response.title);
                println!(
                    "  {}",
                    if response.created {
                        "Created a new DM session"
                    } else {
                        "Reused the existing DM session"
                    }
                );
                println!("  Agent: {}", response.agent_state);
                println!("  Observe: mitsuro hive attach {}", response.session_id);
            }
        },
        HiveCommand::Groups { action } => match action {
            HiveGroupsAction::List => {
                let response: HiveGroupsListResponse = request_json(
                    client.get(format!("{base}/api/hive/groups")),
                    "Failed to list Hive Groups",
                )
                .await?;
                print_hive_groups(&response.groups);
            }
            HiveGroupsAction::Send { group_id, message } => {
                let response: HiveGroupSendResponse = request_json(
                    client
                        .post(format!("{base}/api/hive/groups/{group_id}/messages"))
                        .json(&serde_json::json!({ "message": message })),
                    "Failed to send Group message",
                )
                .await?;
                println!("Queued Group turn");
                println!("  Group: {}", response.group_id);
                println!("  Turn: {}", response.turn_id);
                println!("  Message: {}", response.message_id);
                println!("  Status: {}", response.status);
                println!(
                    "  Observe: mitsuro hive groups attach {}",
                    response.group_id
                );
            }
            HiveGroupsAction::Attach { group_id } => {
                attach_hive_group(&client, &base, &group_id).await?;
            }
        },
    }

    Ok(())
}

async fn request_json<T>(request: reqwest::RequestBuilder, context: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    let response = request.send().await.with_context(|| context.to_string())?;
    let response = ensure_success(response).await?;
    response
        .json::<T>()
        .await
        .with_context(|| format!("{context}: failed to parse response"))
}

async fn request_empty(request: reqwest::RequestBuilder, context: &str) -> Result<()> {
    let response = request.send().await.with_context(|| context.to_string())?;
    let _ = ensure_success(response).await?;
    Ok(())
}

async fn ensure_success(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    anyhow::bail!("Server returned {status}: {text}");
}

fn print_hive_session_summaries(sessions: &[HiveSessionSummaryResponse]) {
    if sessions.is_empty() {
        println!("No Hive sessions found.");
        return;
    }

    println!(
        "{:<38} {:<16} {:<14} {:<36}",
        "SESSION ID", "RUNTIME", "AGENT", "TITLE"
    );
    println!("{}", "-".repeat(110));

    for session in sessions {
        let runtime_status = session
            .runtime
            .as_ref()
            .map(|runtime| runtime.status.as_str())
            .unwrap_or(session.agent_state.as_str());
        println!(
            "{:<38} {:<16} {:<14} {:<36}",
            session.session_id,
            runtime_status,
            session.agent_state,
            truncate(&session.title, 36)
        );
    }
}

fn print_hive_workers(workers: &[HiveWorkerListItem]) {
    if workers.is_empty() {
        println!("No Hive Workers found.");
        return;
    }

    println!(
        "{:<38} {:<16} {:<12} {:<18} {:<36}",
        "WORKER ID", "SLUG", "STATUS", "MODEL", "NAME"
    );
    println!("{}", "-".repeat(124));
    for worker in workers {
        let presence = worker
            .dm_agent_state
            .as_deref()
            .unwrap_or(worker.status.as_str());
        println!(
            "{:<38} {:<16} {:<12} {:<18} {:<36}",
            worker.id,
            truncate(&worker.slug, 16),
            truncate(presence, 12),
            truncate(worker.model.as_deref().unwrap_or("-"), 18),
            truncate(&worker.display_name, 36)
        );
        if let Some(session_id) = &worker.dm_session_id {
            println!("  DM: {session_id}");
        }
    }
}

fn print_hive_groups(groups: &[HiveGroupListItem]) {
    if groups.is_empty() {
        println!("No Hive Groups found.");
        return;
    }

    println!(
        "{:<38} {:<12} {:<10} {:<24} {:<36}",
        "GROUP ID", "MODE", "STATUS", "MEMBERS", "TITLE"
    );
    println!("{}", "-".repeat(124));
    for group in groups {
        let members = group
            .members
            .iter()
            .map(|member| {
                if member.display_name.is_empty() || member.display_name == member.slug {
                    member.slug.clone()
                } else {
                    format!("{} ({})", member.slug, member.display_name)
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:<38} {:<12} {:<10} {:<24} {:<36}",
            group.id,
            truncate(&group.execution_mode, 12),
            truncate(&group.status, 10),
            truncate(&members, 24),
            truncate(&group.title, 36)
        );
        if let Some(turn_id) = &group.active_turn_id {
            println!("  Active turn: {turn_id}");
        }
    }
}

async fn resolve_hive_worker_id(
    client: &reqwest::Client,
    base: &str,
    worker_ref: &str,
) -> Result<String> {
    let listed: HiveWorkersListResponse = request_json(
        client.get(format!("{base}/api/hive/workers")),
        "Failed to list Hive Workers",
    )
    .await?;
    if listed.workers.iter().any(|worker| worker.id == worker_ref) {
        return Ok(worker_ref.to_string());
    }
    let matches: Vec<_> = listed
        .workers
        .iter()
        .filter(|worker| worker.slug == worker_ref)
        .collect();
    match matches.as_slice() {
        [worker] => Ok(worker.id.clone()),
        [] => anyhow::bail!("Hive Worker '{worker_ref}' was not found"),
        _ => anyhow::bail!("Hive Worker slug '{worker_ref}' is ambiguous"),
    }
}

async fn attach_hive_group(client: &reqwest::Client, base: &str, group_id: &str) -> Result<()> {
    let response = client
        .get(format!("{base}/api/hive/groups/{group_id}/events"))
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .send()
        .await
        .context("Failed to attach to Hive Group")?;
    let response = ensure_success(response).await?;

    println!("Attaching to Hive Group {group_id}");
    let mut stream = response.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Failed to read Hive Group event stream")?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(newline_idx) = buffer.find('\n') {
            let line = buffer[..newline_idx].trim_end_matches('\r').to_string();
            buffer.drain(..=newline_idx);

            if let Some(data) = line.strip_prefix("data: ") {
                if data.trim().is_empty() {
                    continue;
                }
                let event: serde_json::Value = serde_json::from_str(data)
                    .with_context(|| format!("Failed to parse Hive Group event: {data}"))?;
                print_hive_group_event(&event);
            }
        }
    }

    Ok(())
}

fn print_hive_group_event(event: &serde_json::Value) {
    let event_type = event
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("event");
    match event_type {
        "message" => {
            let message = event.get("message").unwrap_or(event);
            let seq = message
                .get("seq")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default();
            let sender = message
                .get("sender_kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            let worker = message
                .get("sender_worker_id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let content = message
                .get("content")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if worker.is_empty() {
                println!("[{seq}] {sender}: {content}");
            } else {
                println!("[{seq}] {sender} {worker}: {content}");
            }
        }
        "turn" => {
            let turn = event.get("turn").unwrap_or(event);
            let status = turn
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            let turn_id = turn
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            println!("[turn] {turn_id} {status}");
        }
        _ => println!("[{event_type}] {event}"),
    }
}

fn print_hive_session_status(status: &HiveSessionStatusResponse) {
    println!("Session: {}", status.session_id);
    println!("Type: {}", status.session_type);
    println!("Title: {}", status.title);
    println!("Agent State: {}", status.agent_state);

    if let Some(runtime) = &status.runtime {
        println!("Runtime: {}", runtime.status);
        println!("Runtime Session: {}", runtime.session_id);
        println!("Updated: {}", runtime.updated_at);
        if let Some(wake_reason) = &runtime.last_wake_reason {
            println!("Wake Reason: {}", wake_reason);
        }
        if let Some(run_id) = &runtime.current_run_id {
            println!("Run ID: {}", run_id);
        }
        if let Some(next_wake_at) = &runtime.next_wake_at {
            println!("Next Wake: {}", next_wake_at);
        }
        if let Some(reason) = &runtime.sleep_reason {
            println!("Sleep Reason: {}", reason);
        }
        if let Some(error) = &runtime.last_error {
            println!("Last Error: {}", error);
        }
    }

    if status.tasks.is_empty() {
        println!("Tasks: none");
        return;
    }

    println!("Tasks:");
    for task in &status.tasks {
        println!("- [{}] {} ({})", task.status, task.subject, task.id);
        if !task.description.is_empty() {
            println!("  {}", task.description);
        }
        if let Some(owner) = &task.owner {
            println!("  owner: {}", owner);
        }
        if !task.blocked_by.is_empty() {
            println!("  blocked_by: {}", task.blocked_by.join(", "));
        }
        if let Some(result) = &task.result {
            println!("  result: {}", truncate(result, 120));
        }
    }
}

async fn attach_hive_session(client: &reqwest::Client, base: &str, session_id: &str) -> Result<()> {
    let response = client
        .get(format!("{base}/api/hive/sessions/{session_id}/events"))
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .send()
        .await
        .context("Failed to attach to Hive session")?;
    let response = ensure_success(response).await?;

    println!("Attaching to Hive session {}", session_id);
    let mut delegation = CliDelegationProjection::new(session_id);
    match request_json::<DelegationSessionStateResponse>(
        client.get(format!(
            "{base}/api/sessions/{session_id}/state?include_delegated_history=true"
        )),
        "Failed to hydrate delegation state",
    )
    .await
    {
        Ok(state) => delegation.hydrate(&state),
        Err(error) => eprintln!("Delegation reconnect hydration unavailable: {error}"),
    }
    let mut stream = response.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Failed to read Hive event stream")?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(newline_idx) = buffer.find('\n') {
            let line = buffer[..newline_idx].trim_end_matches('\r').to_string();
            buffer.drain(..=newline_idx);

            if let Some(data) = line.strip_prefix("data: ") {
                if data.trim().is_empty() {
                    continue;
                }

                let event: serde_json::Value = serde_json::from_str(data)
                    .with_context(|| format!("Failed to parse Hive event: {data}"))?;
                if print_hive_event(&event, &mut delegation)? {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

fn print_hive_event(
    event: &serde_json::Value,
    delegation: &mut CliDelegationProjection,
) -> Result<bool> {
    let Some(event_type) = event.get("type").and_then(|value| value.as_str()) else {
        println!("[event] {}", event);
        return Ok(false);
    };

    match event_type {
        "text_delta" | "text_delta_with_citations" => {
            if let Some(delta) = event.get("delta").and_then(|value| value.as_str()) {
                print!("{}", delta);
                io::stdout().flush()?;
            }
        }
        "tool_output_delta" => {
            if let Some(delta) = event.get("delta").and_then(|value| value.as_str()) {
                print!("{}", delta);
                io::stdout().flush()?;
            }
        }
        "tool_call_start" => {
            let name = event
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            println!("\n[tool:start] {}", name);
        }
        "tool_call_complete" => {
            let name = event
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            println!("\n[tool:call] {}", name);
        }
        "tool_result" => {
            if let Some(output) = event.get("output").and_then(|value| value.as_str()) {
                let output = output.trim();
                if !output.is_empty() {
                    println!("\n[tool:result] {}", output);
                }
            }
        }
        "user_message" => {
            let level = event
                .get("level")
                .and_then(|value| value.as_str())
                .unwrap_or("info");
            let title = event
                .get("title")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let message = event
                .get("message")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if title.is_empty() {
                println!("\n[user:{}] {}", level, message);
            } else {
                println!("\n[user:{}] {}: {}", level, title, message);
            }
        }
        "agent_sleeping" => {
            let duration = event
                .get("duration_secs")
                .and_then(|value| value.as_u64())
                .unwrap_or_default();
            let reason = event
                .get("reason")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            println!("\n[sleep] {}s {}", duration, reason);
        }
        "tick_injected" => {
            let tick = event
                .get("tick_number")
                .and_then(|value| value.as_u64())
                .unwrap_or_default();
            println!("\n[tick] #{}", tick);
        }
        "classifier_decision" => {
            let tool_name = event
                .get("tool_name")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let decision = event
                .get("decision")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let reason = event
                .get("reason")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            println!("\n[classifier] {} => {} ({})", tool_name, decision, reason);
        }
        "delegated_progress" => {
            let agent_name = event
                .get("agent_name")
                .and_then(|value| value.as_str())
                .unwrap_or("agent");
            let stage = event
                .get("stage")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let status = event
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let action = event
                .get("current_action")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if action.is_empty() {
                println!("\n[delegated] {} {} {}", agent_name, stage, status);
            } else {
                println!(
                    "\n[delegated] {} {} {}: {}",
                    agent_name, stage, status, action
                );
            }
        }
        "delegation_event" => delegation.print_event(event),
        "plan_update" => {
            let count = event
                .get("items")
                .and_then(|value| value.as_array())
                .map(|items| items.len())
                .unwrap_or_default();
            println!("\n[plan] {} items", count);
        }
        "finish" => {
            let stop_reason = event
                .get("stop_reason")
                .and_then(|value| value.as_str())
                .unwrap_or("completed");
            println!("\n[finish] {}", stop_reason);
            return Ok(true);
        }
        "error" => {
            let error = event
                .get("error")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown error");
            println!("\n[error] {}", error);
            return Ok(true);
        }
        _ => {
            println!("\n[{}] {}", event_type, event);
        }
    }

    Ok(false)
}

fn truncate(value: &str, max_chars: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_string();
    }

    let truncated: String = value.chars().take(max_chars.saturating_sub(3)).collect();
    format!("{}...", truncated)
}

/// Restore terminal state - called on panic or unexpected exit
fn restore_terminal() {
    use std::io::Write;

    use crossterm::{
        event::DisableMouseCapture,
        execute,
        terminal::{disable_raw_mode, LeaveAlternateScreen},
    };
    let _ = disable_raw_mode();
    let _ = execute!(std::io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
    let _ = std::io::stdout().flush();
}

#[tokio::main]
async fn main() -> Result<()> {
    mitsuro_core::identity::import_legacy_environment();
    let cli = Cli::parse();

    if let Some(Commands::MigrateIdentity { confirm_offline }) = &cli.command {
        if !confirm_offline {
            anyhow::bail!(
                "stop every Mitsuro CLI, TUI, desktop, server, and Hive process, take a backup, then rerun with --confirm-offline"
            );
        }
        let receipt = mitsuro_core::identity::migrate_config_root_offline()
            .context("copying legacy Mitsuro configuration into the canonical root")?;
        println!("Mitsuro identity migration completed");
        println!("  Canonical: {}", receipt.canonical_root.display());
        println!("  Rollback:  {}", receipt.rollback_root.display());
        println!("  Receipt:   {}", receipt.receipt_path.display());
        eprintln!(
            "Rollback state is recovery-only. After cutover, never launch any previous-generation app or binary except through a coordinated rollback, even when canonical Mitsuro is stopped: directly invoked archived binaries are not continuously locked out, can mutate the preserved authority, and can cause split authority or make the next canonical start fail. Stop every Mitsuro process and use the coordinated installer/manual rollback procedure."
        );
        return Ok(());
    }

    if let Some(Commands::Update { apply, version }) = &cli.command {
        return update_cmd::run(*apply, version.clone()).await;
    }

    // Serve mode has its own logging (stdout), skip TUI logging setup
    if matches!(cli.command, Some(Commands::Serve { .. })) {
        if let Some(Commands::Serve {
            port,
            desktop_managed,
        }) = cli.command
        {
            mitsuro_core::identity::require_startup_identity()
                .context("validating Mitsuro configuration authority")?;
            return serve::run(port, desktop_managed).await;
        }
    }

    // Hive subcommand uses Hive-compatible HTTP routes and exits, no TUI needed.
    if let Some(Commands::Hive {
        command,
        task,
        project_dir,
        attach,
    }) = cli.command
    {
        let command = if let Some(command) = command {
            command
        } else if let Some(task) = task {
            HiveCommand::Run {
                task,
                project_dir,
                attach,
            }
        } else {
            anyhow::bail!("Provide a task or a Hive subcommand");
        };
        return run_hive_command(command).await;
    }

    mitsuro_core::identity::require_startup_identity()
        .context("validating Mitsuro configuration authority")?;

    // Set up panic hook to restore terminal state (TUI/ACP modes)
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        restore_terminal();
        original_hook(panic_info);
    }));

    // Initialize logging to file (not stdout/stderr which would mess up TUI)
    let log_dir = paths::logs_dir();
    if let Err(e) = std::fs::create_dir_all(&log_dir) {
        eprintln!("Failed to create log directory: {}", e);
    }

    #[cfg(unix)]
    let null_device = "/dev/null";
    #[cfg(windows)]
    let null_device = "NUL";

    let log_file = match std::fs::File::create(log_dir.join("mitsuro.log")) {
        Ok(file) => file,
        Err(e) => {
            eprintln!(
                "Failed to create log file: {}, falling back to null device",
                e
            );
            match std::fs::File::create(null_device) {
                Ok(file) => file,
                Err(e) => {
                    eprintln!(
                        "Failed to create null device {}: {}, logging disabled",
                        null_device, e
                    );
                    return Err(e.into());
                }
            }
        }
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::sync::Mutex::new(log_file))
        .with_ansi(false)
        .init();

    // Apply any pending update before starting TUI
    match mitsuro_core::updater::apply_pending_update() {
        Ok(Some(version)) => tracing::info!("Applied pending update to v{}", version),
        Ok(None) => {}
        Err(error) => tracing::warn!("Pending update was not applied: {}", error),
    }

    match cli.command {
        Some(Commands::Acp) => {
            tracing::info!("Starting Mitsuro in ACP server mode");
            let server = acp::AcpServer::new()?;
            server.run().await?;
        }
        Some(
            Commands::Serve { .. }
            | Commands::Hive { .. }
            | Commands::MigrateIdentity { .. }
            | Commands::Update { .. },
        ) => {
            unreachable!()
        }
        None => {
            // Full replace: Mitsuro TUI v2 is the default terminal surface.
            // Legacy v1 remains available as `mitsuro tui-legacy` if re-exposed later.
            match tui_v2::run().await? {
                tui_v2::TuiOutcome::Quit => {}
                tui_v2::TuiOutcome::ApplyUpdate { version } => {
                    update_cmd::apply_and_relaunch(Some(&version)).await?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod cli_contract_tests {
    use super::*;

    #[test]
    fn canonical_cli_exposes_the_package_version() {
        let version = match Cli::try_parse_from(["mitsuro", "--version"]) {
            Ok(_) => panic!("Clap must return DisplayVersion for --version"),
            Err(error) => error,
        };
        assert_eq!(version.kind(), clap::error::ErrorKind::DisplayVersion);
        assert_eq!(
            version.to_string().trim(),
            format!("mitsuro {}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn serve_accepts_explicit_desktop_managed_mode() {
        let parsed =
            Cli::try_parse_from(["mitsuro", "serve", "--desktop-managed", "--port", "4123"])
                .expect("desktop-managed serve arguments");

        assert!(matches!(
            parsed.command,
            Some(Commands::Serve {
                port: 4123,
                desktop_managed: true,
            })
        ));
    }

    #[test]
    fn delegation_projection_filters_session_and_replay_cursor() {
        let mut projection = CliDelegationProjection::new("session-1");
        projection.print_event(&serde_json::json!({
            "event": {
                "event_id": 8,
                "parent_session_id": "other-session",
                "delegation_group_id": "group-1",
                "event_type": "future_event",
                "payload": {"state": "future"}
            }
        }));
        assert_eq!(projection.cursor, 0);

        projection.print_event(&serde_json::json!({
            "event": {
                "event_id": 8,
                "parent_session_id": "session-1",
                "delegation_group_id": "group-1",
                "event_type": "future_event",
                "payload": {"state": "future"}
            }
        }));
        assert_eq!(projection.cursor, 8);
        projection.print_event(&serde_json::json!({
            "event": {
                "event_id": 7,
                "parent_session_id": "session-1",
                "delegation_group_id": "group-1",
                "event_type": "task_running",
                "delegation_task_id": "task-1",
                "payload": {}
            }
        }));
        assert_eq!(projection.cursor, 8);
    }
}
