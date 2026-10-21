use std::collections::{BTreeMap, VecDeque};
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use gpui::prelude::FluentBuilder as _;
use gpui::{
    div, point, px, size, AppContext as _, Context, Entity, EventEmitter, InteractiveElement as _,
    IntoElement, ParentElement as _, Pixels, Render, ScrollWheelEvent, SharedString, Styled as _,
    Task, Timer, Window,
};
use gpui_component::button::Button;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::scroll::ScrollableElement as _;
use gpui_component::text::TextView;
use gpui_component::{v_virtual_list, Disableable as _, VirtualListScrollHandle};
use mitsuro_client::{
    AgentControlLease, AgentControlState, AgentInputResponseRequest, AgentStreamEnvelope,
    ChatRequest, ChatStreamEvent, ContentBlock, DelegatedProgressEvent, DelegatedProgressStatus,
    DelegatedRunStage, DelegatedToolKind, DelegationEventKind, DelegationEventResponse,
    HeartbeatAgentControlRequest, MessageResponse, MitsuroClient, PendingInteractionSnapshot,
    SessionType, SteerRequest, TakeAgentControlRequest,
};
use tokio::runtime::Runtime;

use crate::preferences::bounded_agent_draft;
use crate::theme::CanvasPalette;

const EVENT_QUEUE_CAPACITY: usize = 256;
const STREAM_FRAME_DELAY: Duration = Duration::from_millis(16);
const MAX_MESSAGES: usize = 400;
const MAX_MESSAGE_CHARS: usize = 128 * 1024;
const MAX_TRANSCRIPT_CHARS: usize = 2 * 1024 * 1024;
const MAX_ACTIVITY: usize = 24;
const MAX_TOOL_OUTPUT_CHARS: usize = 16 * 1024;
const MAX_TOOL_ARGUMENT_CHARS: usize = 8 * 1024;
const MAX_TOOL_TIMELINE_CHARS: usize = 256 * 1024;
const MAX_TOOL_TIMELINE_ENTRIES: usize = 64;
const MAX_VISIBLE_TOOL_ENTRIES: usize = 6;
const MAX_RENDERED_TOOL_DETAIL_CHARS: usize = 6 * 1024;
const MAX_RENDERED_TOOL_DETAIL_LINES: usize = 80;
const MAX_SCROLL_OFFSET: f32 = 100_000_000.0;
const SCROLL_TAIL_TOLERANCE: f32 = 2.0;
const CONTROL_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentRole {
    User,
    Assistant,
    Notice,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentMessage {
    pub id: u64,
    pub role: AgentRole,
    pub markdown: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingApproval {
    pub tool_call_id: String,
    pub tool_name: String,
    pub summary: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolTimelineKind {
    Tool,
    Server,
    WebSearch,
    WebFetch,
    Delegation,
}

impl ToolTimelineKind {
    fn label(self) -> &'static str {
        match self {
            Self::Tool => "Tool",
            Self::Server => "Server",
            Self::WebSearch => "Search",
            Self::WebFetch => "Fetch",
            Self::Delegation => "Delegation",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolTimelineStatus {
    Starting,
    Prepared,
    Running,
    AwaitingApproval,
    AwaitingInput,
    Complete,
    Denied,
    Error,
}

impl ToolTimelineStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Starting => "Starting",
            Self::Prepared => "Prepared",
            Self::Running => "Running",
            Self::AwaitingApproval => "Needs approval",
            Self::AwaitingInput => "Needs input",
            Self::Complete => "Complete",
            Self::Denied => "Denied",
            Self::Error => "Error",
        }
    }

    fn is_error(self) -> bool {
        matches!(self, Self::Denied | Self::Error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ToolDetailKind {
    Text,
    Json,
    UnifiedDiff,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolDetail {
    kind: ToolDetailKind,
    body: String,
    truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolTimelineEntry {
    id: String,
    sequence: u64,
    name: String,
    kind: ToolTimelineKind,
    status: ToolTimelineStatus,
    detail: Option<ToolDetail>,
}

impl ToolTimelineEntry {
    fn retained_chars(&self) -> usize {
        self.id
            .chars()
            .count()
            .saturating_add(self.name.chars().count())
            .saturating_add(
                self.detail
                    .as_ref()
                    .map_or(0, |detail| detail.body.chars().count()),
            )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ToolTimelineUpdate {
    activity: String,
    attention: bool,
}

#[derive(Default)]
struct ToolTimeline {
    entries: BTreeMap<String, ToolTimelineEntry>,
    order: VecDeque<String>,
    next_sequence: u64,
}

impl ToolTimeline {
    #[cfg(test)]
    fn len(&self) -> usize {
        self.order.len()
    }

    fn recent(&self, limit: usize) -> Vec<ToolTimelineEntry> {
        self.order
            .iter()
            .rev()
            .take(limit)
            .rev()
            .filter_map(|id| self.entries.get(id).cloned())
            .collect()
    }

    #[cfg(test)]
    fn get(&self, id: &str) -> Option<&ToolTimelineEntry> {
        self.entries.get(id)
    }

    fn apply_event(&mut self, event: &ChatStreamEvent) -> Option<ToolTimelineUpdate> {
        let update = match event {
            ChatStreamEvent::ToolCallStart { id, name } => {
                self.update_entry(id, "tool", name, ToolTimelineKind::Tool, |entry| {
                    entry.status = ToolTimelineStatus::Starting;
                });
                ToolTimelineUpdate {
                    activity: format!("Starting {name}"),
                    attention: false,
                }
            }
            ChatStreamEvent::ToolCallComplete {
                id,
                name,
                arguments,
            } => {
                self.update_entry(id, "tool", name, ToolTimelineKind::Tool, |entry| {
                    entry.status = ToolTimelineStatus::Prepared;
                    entry.detail = Some(detail_from_json(arguments, MAX_TOOL_ARGUMENT_CHARS));
                });
                ToolTimelineUpdate {
                    activity: format!("Prepared {name}"),
                    attention: false,
                }
            }
            ChatStreamEvent::ToolExecuting { id, name } => {
                self.update_entry(id, "tool", name, ToolTimelineKind::Tool, |entry| {
                    entry.status = ToolTimelineStatus::Running;
                });
                ToolTimelineUpdate {
                    activity: format!("Running {name}"),
                    attention: false,
                }
            }
            ChatStreamEvent::ToolOutputDelta { id, delta } => {
                self.update_entry(id, "tool", "Tool", ToolTimelineKind::Tool, |entry| {
                    entry.status = ToolTimelineStatus::Running;
                    append_tool_output(entry, delta);
                });
                ToolTimelineUpdate {
                    activity: single_line_excerpt(delta, 180),
                    attention: false,
                }
            }
            ChatStreamEvent::ToolResult {
                id,
                output,
                is_error,
            } => {
                self.update_entry(id, "tool", "Tool", ToolTimelineKind::Tool, |entry| {
                    entry.status = if *is_error {
                        ToolTimelineStatus::Error
                    } else {
                        ToolTimelineStatus::Complete
                    };
                    if !output.is_empty() {
                        entry.detail = Some(detail_from_output(output, MAX_TOOL_OUTPUT_CHARS));
                    }
                });
                ToolTimelineUpdate {
                    activity: if *is_error {
                        "Tool failed".to_owned()
                    } else {
                        "Tool complete".to_owned()
                    },
                    attention: *is_error,
                }
            }
            ChatStreamEvent::ServerToolStart { id, name } => {
                self.update_entry(id, "server", name, ToolTimelineKind::Server, |entry| {
                    entry.status = ToolTimelineStatus::Running;
                });
                ToolTimelineUpdate {
                    activity: format!("Running {name}"),
                    attention: false,
                }
            }
            ChatStreamEvent::ServerToolComplete { id, name } => {
                self.update_entry(id, "server", name, ToolTimelineKind::Server, |entry| {
                    entry.status = ToolTimelineStatus::Complete;
                });
                ToolTimelineUpdate {
                    activity: format!("{name} complete"),
                    attention: false,
                }
            }
            ChatStreamEvent::ServerToolError {
                tool_use_id,
                error_code,
            } => {
                self.update_entry(
                    tool_use_id,
                    "server",
                    "Server tool",
                    ToolTimelineKind::Server,
                    |entry| {
                        entry.status = ToolTimelineStatus::Error;
                        entry.detail = Some(detail_from_text(error_code, MAX_TOOL_OUTPUT_CHARS));
                    },
                );
                ToolTimelineUpdate {
                    activity: format!("Server tool failed · {error_code}"),
                    attention: true,
                }
            }
            ChatStreamEvent::WebSearchResults {
                tool_use_id,
                payload,
            } => {
                self.update_entry(
                    tool_use_id,
                    "search",
                    "Web search",
                    ToolTimelineKind::WebSearch,
                    |entry| {
                        entry.status = ToolTimelineStatus::Complete;
                        entry.detail = Some(detail_from_json(payload, MAX_TOOL_OUTPUT_CHARS));
                    },
                );
                ToolTimelineUpdate {
                    activity: "Web search complete".to_owned(),
                    attention: false,
                }
            }
            ChatStreamEvent::WebFetchResult {
                tool_use_id,
                payload,
            } => {
                self.update_entry(
                    tool_use_id,
                    "fetch",
                    "Web fetch",
                    ToolTimelineKind::WebFetch,
                    |entry| {
                        entry.status = ToolTimelineStatus::Complete;
                        entry.detail = Some(detail_from_json(payload, MAX_TOOL_OUTPUT_CHARS));
                    },
                );
                ToolTimelineUpdate {
                    activity: "Web fetch complete".to_owned(),
                    attention: false,
                }
            }
            ChatStreamEvent::DelegatedProgress { payload } => {
                let status = delegated_timeline_status(payload.status);
                let name = delegated_progress_name(payload);
                let detail = delegated_progress_detail(payload);
                self.update_entry(
                    &payload.tool_call_id,
                    "delegation",
                    &name,
                    ToolTimelineKind::Delegation,
                    |entry| {
                        entry.status = status;
                        entry.detail = Some(detail);
                    },
                );
                ToolTimelineUpdate {
                    activity: delegated_progress_activity(payload),
                    attention: status.is_error(),
                }
            }
            ChatStreamEvent::DelegationEvent { event } => {
                let id = delegation_event_key(event);
                let status = delegation_event_status(event);
                let name = format!(
                    "Delegation · {}",
                    delegation_event_kind_label(&event.event_type)
                );
                self.update_entry(
                    &id,
                    "delegation",
                    &name,
                    ToolTimelineKind::Delegation,
                    |entry| {
                        entry.status = status;
                        entry.detail =
                            Some(detail_from_json(&event.payload, MAX_TOOL_OUTPUT_CHARS));
                    },
                );
                ToolTimelineUpdate {
                    activity: name,
                    attention: status.is_error(),
                }
            }
            ChatStreamEvent::ToolApprovalRequired {
                id,
                name,
                arguments,
            } => {
                self.update_entry(id, "tool", name, ToolTimelineKind::Tool, |entry| {
                    entry.status = ToolTimelineStatus::AwaitingApproval;
                    entry.detail = Some(detail_from_json(arguments, MAX_TOOL_ARGUMENT_CHARS));
                });
                ToolTimelineUpdate {
                    activity: format!("{name} needs approval"),
                    attention: true,
                }
            }
            ChatStreamEvent::ToolApproved { id } => {
                self.update_entry(id, "tool", "Tool", ToolTimelineKind::Tool, |entry| {
                    entry.status = ToolTimelineStatus::Running;
                });
                ToolTimelineUpdate {
                    activity: "Tool approved".to_owned(),
                    attention: false,
                }
            }
            ChatStreamEvent::ToolDenied { id } => {
                self.update_entry(id, "tool", "Tool", ToolTimelineKind::Tool, |entry| {
                    entry.status = ToolTimelineStatus::Denied;
                });
                ToolTimelineUpdate {
                    activity: "Tool denied".to_owned(),
                    attention: true,
                }
            }
            ChatStreamEvent::AwaitingInput {
                tool_call_id,
                tool_name,
            } => {
                self.update_entry(
                    tool_call_id,
                    "tool",
                    tool_name,
                    ToolTimelineKind::Tool,
                    |entry| entry.status = ToolTimelineStatus::AwaitingInput,
                );
                ToolTimelineUpdate {
                    activity: format!("{tool_name} needs input"),
                    attention: true,
                }
            }
            _ => return None,
        };
        self.enforce_bounds();
        Some(update)
    }

    fn update_entry(
        &mut self,
        raw_id: &str,
        fallback_prefix: &str,
        name: &str,
        kind: ToolTimelineKind,
        update: impl FnOnce(&mut ToolTimelineEntry),
    ) {
        let id = if raw_id.trim().is_empty() {
            format!("{fallback_prefix}:missing:{}", self.next_sequence)
        } else {
            bounded_identifier(raw_id, 256)
        };
        if !self.entries.contains_key(&id) {
            let sequence = self.next_sequence;
            self.next_sequence = self.next_sequence.saturating_add(1);
            self.order.push_back(id.clone());
            self.entries.insert(
                id.clone(),
                ToolTimelineEntry {
                    id: id.clone(),
                    sequence,
                    name: bounded_text(name, 120),
                    kind,
                    status: ToolTimelineStatus::Starting,
                    detail: None,
                },
            );
        }
        if let Some(entry) = self.entries.get_mut(&id) {
            if entry.name == "Tool" || entry.name == "Server tool" || entry.name.is_empty() {
                entry.name = bounded_text(name, 120);
            }
            entry.kind = kind;
            update(entry);
        }
    }

    fn enforce_bounds(&mut self) {
        while self.order.len() > MAX_TOOL_TIMELINE_ENTRIES
            || (self.retained_chars() > MAX_TOOL_TIMELINE_CHARS && self.order.len() > 1)
        {
            let Some(id) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&id);
        }
    }

    fn retained_chars(&self) -> usize {
        self.entries
            .values()
            .map(ToolTimelineEntry::retained_chars)
            .sum()
    }
}

fn bounded_identifier(value: &str, max_chars: usize) -> String {
    let value = value.trim();
    let count = value.chars().count();
    if count <= max_chars {
        return value.to_owned();
    }
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    let suffix = format!("~{:016x}", hasher.finish());
    let head_chars = max_chars.saturating_sub(suffix.chars().count());
    let mut result = value.chars().take(head_chars).collect::<String>();
    result.push_str(&suffix);
    result
}

fn detail_from_text(value: &str, max_chars: usize) -> ToolDetail {
    let (body, truncated) = bounded_tool_text(value, max_chars);
    ToolDetail {
        kind: classify_tool_detail(&body),
        body,
        truncated,
    }
}

fn detail_from_json(value: &serde_json::Value, max_chars: usize) -> ToolDetail {
    let rendered = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    let (body, truncated) = bounded_tool_text(&rendered, max_chars);
    ToolDetail {
        kind: ToolDetailKind::Json,
        body,
        truncated,
    }
}

fn detail_from_output(value: &str, max_chars: usize) -> ToolDetail {
    if is_unified_diff(value) {
        let (body, truncated) = bounded_tool_text(value, max_chars);
        return ToolDetail {
            kind: ToolDetailKind::UnifiedDiff,
            body,
            truncated,
        };
    }
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(value) {
        return detail_from_json(&value, max_chars);
    }
    detail_from_text(value, max_chars)
}

fn append_tool_output(entry: &mut ToolTimelineEntry, delta: &str) {
    let mut combined = entry
        .detail
        .as_ref()
        .map(|detail| detail.body.clone())
        .unwrap_or_default();
    combined.push_str(delta);
    let mut detail = detail_from_output(&combined, MAX_TOOL_OUTPUT_CHARS);
    if entry
        .detail
        .as_ref()
        .is_some_and(|existing| existing.truncated)
    {
        detail.truncated = true;
    }
    entry.detail = Some(detail);
}

fn classify_tool_detail(value: &str) -> ToolDetailKind {
    if is_unified_diff(value) {
        ToolDetailKind::UnifiedDiff
    } else {
        ToolDetailKind::Text
    }
}

fn is_unified_diff(value: &str) -> bool {
    value.starts_with("diff --git ")
        || (value.contains("\n--- ") && value.contains("\n+++ "))
        || (value.starts_with("--- ") && value.contains("\n+++ "))
        || value.contains("\n@@ ")
}

fn bounded_tool_text(value: &str, max_chars: usize) -> (String, bool) {
    let count = value.chars().count();
    if count <= max_chars {
        return (value.to_owned(), false);
    }
    if max_chars == 0 {
        return (String::new(), true);
    }
    const ELISION: &str = "\n… output elided …\n";
    let elision_chars = ELISION.chars().count();
    if max_chars <= elision_chars + 2 {
        return (value.chars().take(max_chars).collect(), true);
    }
    let retained = max_chars - elision_chars;
    let head_chars = retained.saturating_mul(2) / 3;
    let tail_chars = retained.saturating_sub(head_chars);
    let mut body = value.chars().take(head_chars).collect::<String>();
    body.push_str(ELISION);
    body.extend(value.chars().skip(count.saturating_sub(tail_chars)));
    (body, true)
}

fn delegated_timeline_status(status: DelegatedProgressStatus) -> ToolTimelineStatus {
    match status {
        DelegatedProgressStatus::Created
        | DelegatedProgressStatus::Queued
        | DelegatedProgressStatus::Leased => ToolTimelineStatus::Starting,
        DelegatedProgressStatus::Running | DelegatedProgressStatus::Retrying => {
            ToolTimelineStatus::Running
        }
        DelegatedProgressStatus::Complete => ToolTimelineStatus::Complete,
        DelegatedProgressStatus::Cancelled => ToolTimelineStatus::Denied,
        DelegatedProgressStatus::Degraded | DelegatedProgressStatus::Failed => {
            ToolTimelineStatus::Error
        }
    }
}

fn delegated_tool_kind_label(kind: DelegatedToolKind) -> &'static str {
    match kind {
        DelegatedToolKind::Explore => "Explore",
        DelegatedToolKind::Plan => "Plan",
        DelegatedToolKind::Verify => "Verify",
        DelegatedToolKind::Build => "Build",
    }
}

fn delegated_stage_label(stage: DelegatedRunStage) -> &'static str {
    match stage {
        DelegatedRunStage::Created => "created",
        DelegatedRunStage::Running => "running",
        DelegatedRunStage::Synthesizing => "synthesizing",
        DelegatedRunStage::Complete => "complete",
        DelegatedRunStage::Degraded => "degraded",
        DelegatedRunStage::Failed => "failed",
        DelegatedRunStage::Cancelled => "cancelled",
    }
}

fn delegated_progress_name(progress: &DelegatedProgressEvent) -> String {
    bounded_text(
        &format!(
            "{} · {}",
            delegated_tool_kind_label(progress.kind),
            progress.agent_name
        ),
        120,
    )
}

fn delegated_progress_detail(progress: &DelegatedProgressEvent) -> ToolDetail {
    let detail = serde_json::json!({
        "stage": delegated_stage_label(progress.stage),
        "task_id": progress.task_id.as_str(),
        "action": progress.current_action.as_deref(),
        "summary": progress.completion_summary.as_deref(),
        "tool_count": progress.tool_count,
        "tokens": progress.tokens,
        "lines_added": progress.lines_added,
        "lines_removed": progress.lines_removed,
        "completed_plan_task": progress.completed_plan_task.as_deref(),
    });
    detail_from_json(&detail, MAX_TOOL_OUTPUT_CHARS)
}

fn delegated_progress_activity(progress: &DelegatedProgressEvent) -> String {
    progress
        .current_action
        .as_deref()
        .or(progress.completion_summary.as_deref())
        .map(|value| single_line_excerpt(value, 180))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            format!(
                "{} · {}",
                progress.agent_name,
                delegated_stage_label(progress.stage)
            )
        })
}

fn delegation_event_key(event: &DelegationEventResponse) -> String {
    event
        .delegation_task_id
        .as_ref()
        .map(|task_id| format!("delegation-task:{task_id}"))
        .unwrap_or_else(|| format!("delegation-group:{}", event.delegation_group_id))
}

fn delegation_event_kind_label(kind: &DelegationEventKind) -> String {
    match kind {
        DelegationEventKind::GroupCreated => "Group created".to_owned(),
        DelegationEventKind::GroupQueued => "Group queued".to_owned(),
        DelegationEventKind::GroupStateChanged => "Group state".to_owned(),
        DelegationEventKind::TaskClaimed => "Task claimed".to_owned(),
        DelegationEventKind::TaskRunning => "Task running".to_owned(),
        DelegationEventKind::TaskStateChanged => "Task state".to_owned(),
        DelegationEventKind::ParentContinuationQueued => "Continuation queued".to_owned(),
        DelegationEventKind::ParentContinuationPromoted => "Continuation promoted".to_owned(),
        DelegationEventKind::Other(value) => single_line_excerpt(value, 80),
    }
}

fn delegation_event_status(event: &DelegationEventResponse) -> ToolTimelineStatus {
    let payload_status = event
        .payload
        .get("status")
        .or_else(|| event.payload.get("state"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if matches!(payload_status, "failed" | "degraded" | "error") {
        return ToolTimelineStatus::Error;
    }
    if matches!(payload_status, "cancelled" | "canceled") {
        return ToolTimelineStatus::Denied;
    }
    if matches!(payload_status, "complete" | "completed" | "succeeded") {
        return ToolTimelineStatus::Complete;
    }
    match &event.event_type {
        DelegationEventKind::GroupCreated | DelegationEventKind::GroupQueued => {
            ToolTimelineStatus::Starting
        }
        DelegationEventKind::TaskClaimed
        | DelegationEventKind::TaskRunning
        | DelegationEventKind::ParentContinuationQueued
        | DelegationEventKind::ParentContinuationPromoted => ToolTimelineStatus::Running,
        DelegationEventKind::GroupStateChanged
        | DelegationEventKind::TaskStateChanged
        | DelegationEventKind::Other(_) => ToolTimelineStatus::Running,
    }
}

fn rendered_tool_detail_markdown(detail: &ToolDetail) -> String {
    let (body, render_truncated) = bounded_rendered_tool_detail(&detail.body);
    let language = match detail.kind {
        ToolDetailKind::Text => "text",
        ToolDetailKind::Json => "json",
        ToolDetailKind::UnifiedDiff => "diff",
    };
    let fence = safe_markdown_fence(&body);
    let truncation_notice = if detail.truncated || render_truncated {
        "\n\n_Output truncated to keep the Agent surface responsive._"
    } else {
        ""
    };
    format!("{fence}{language}\n{body}\n{fence}{truncation_notice}")
}

fn safe_markdown_fence(value: &str) -> String {
    let longest_backtick_run = value
        .split(|character| character != '`')
        .map(str::len)
        .max()
        .unwrap_or_default();
    "`".repeat(longest_backtick_run.saturating_add(1).max(4))
}

fn bounded_rendered_tool_detail(value: &str) -> (String, bool) {
    let mut rendered = String::new();
    let mut chars = 0_usize;
    let mut lines = 0_usize;
    let mut truncated = false;
    for line in value.lines() {
        if lines >= MAX_RENDERED_TOOL_DETAIL_LINES {
            truncated = true;
            break;
        }
        let separator_chars = usize::from(lines > 0);
        let remaining = MAX_RENDERED_TOOL_DETAIL_CHARS.saturating_sub(chars + separator_chars);
        if remaining == 0 {
            truncated = true;
            break;
        }
        if lines > 0 {
            rendered.push('\n');
            chars += 1;
        }
        let line_count = line.chars().count();
        if line_count > remaining {
            rendered.extend(line.chars().take(remaining));
            truncated = true;
            break;
        }
        rendered.push_str(line);
        chars += line_count;
        lines += 1;
    }
    if value.is_empty() {
        return (String::new(), false);
    }
    if !truncated && lines < value.lines().count() {
        truncated = true;
    }
    (rendered, truncated)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TranscriptViewState {
    selected: bool,
    following_tail: bool,
    unread: bool,
}

impl Default for TranscriptViewState {
    fn default() -> Self {
        Self {
            selected: false,
            following_tail: true,
            unread: false,
        }
    }
}

impl TranscriptViewState {
    fn set_selected(&mut self, selected: bool) {
        self.selected = selected;
        if selected && self.following_tail {
            self.unread = false;
        }
    }

    fn note_incoming(&mut self, attention: bool) {
        if attention || !self.selected || !self.following_tail {
            self.unread = true;
        }
    }

    fn user_scrolled(&mut self, at_tail: bool) {
        self.following_tail = at_tail;
        if self.selected && at_tail {
            self.unread = false;
        }
    }

    fn mark_read(&mut self) {
        self.unread = false;
    }

    fn jump_to_latest(&mut self) {
        self.following_tail = true;
        self.unread = false;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AgentQuestionOption {
    label: String,
    description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AgentQuestion {
    header: String,
    question: String,
    options: Vec<AgentQuestionOption>,
    multi_select: bool,
}

#[derive(Clone, Debug)]
struct PendingAgentInput {
    tool_call_id: String,
    questions: Vec<AgentQuestion>,
    response_kind: AgentInputResponseKind,
    question_index: usize,
    selected: Vec<String>,
    answers: BTreeMap<String, serde_json::Value>,
    submitting: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AgentInputResponseKind {
    Questions,
    PlanConfirm,
}

impl PendingAgentInput {
    fn new(tool_call_id: String, questions: Vec<AgentQuestion>) -> Self {
        Self::with_kind(tool_call_id, questions, AgentInputResponseKind::Questions)
    }

    fn plan_confirm(tool_call_id: String, title: &str, task_count: usize) -> Self {
        Self::with_kind(
            tool_call_id,
            vec![plan_confirm_question(title, task_count)],
            AgentInputResponseKind::PlanConfirm,
        )
    }

    fn with_kind(
        tool_call_id: String,
        questions: Vec<AgentQuestion>,
        response_kind: AgentInputResponseKind,
    ) -> Self {
        Self {
            tool_call_id,
            questions: if questions.is_empty() {
                vec![fallback_agent_question("Question")]
            } else {
                questions
            },
            response_kind,
            question_index: 0,
            selected: Vec::new(),
            answers: BTreeMap::new(),
            submitting: false,
        }
    }
}

#[derive(Clone, Debug)]
enum SurfaceEvent {
    ControlAttached(Result<ControlAttachment, String>),
    ControlTaken(Result<ControlAttachment, String>),
    ControlHeartbeat(AgentControlState),
    ControlLost {
        state: Option<AgentControlState>,
        message: String,
    },
    Hydrated {
        messages: Vec<AgentMessage>,
        pending_approvals: Vec<PendingApproval>,
        pending_input: Option<PendingAgentInput>,
        thinking: String,
        agent_active: bool,
        observer_generation: u64,
        observer_sequence: u64,
    },
    HydrationFailed(String),
    StreamEnded {
        generation: u64,
    },
    Observed(AgentStreamEnvelope),
    ObservationInterrupted(String),
    Failure {
        generation: u64,
        message: String,
    },
    ApprovalResult {
        id: String,
        approved: bool,
        error: Option<String>,
    },
    InputAccepted {
        tool_call_id: String,
    },
    InputFailure {
        tool_call_id: String,
        message: String,
    },
    CancelResult(Option<String>),
}

#[derive(Clone, Debug)]
struct ControlAttachment {
    state: AgentControlState,
    lease: Option<AgentControlLease>,
    notice: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AgentSurfaceEvent {
    DraftChanged(String),
    DraftCleared,
    ScrollChanged(f32),
}

pub struct AgentSurface {
    client: MitsuroClient,
    runtime: Arc<Runtime>,
    palette: CanvasPalette,
    session_id: String,
    project_dir: String,
    title: String,
    client_id: String,
    control_state: Option<AgentControlState>,
    control_lease: Option<AgentControlLease>,
    control_notice: Option<String>,
    taking_control: bool,
    hydrated: bool,
    pending_outbound: Option<String>,
    composer: Entity<InputState>,
    last_emitted_draft: String,
    answer_composer: Entity<InputState>,
    messages: VecDeque<AgentMessage>,
    tool_timeline: ToolTimeline,
    activity: VecDeque<String>,
    thinking: String,
    pending_approvals: Vec<PendingApproval>,
    question_payloads: BTreeMap<String, (Vec<AgentQuestion>, AgentInputResponseKind)>,
    pending_input: Option<PendingAgentInput>,
    streaming: bool,
    transcript_view: TranscriptViewState,
    error: Option<String>,
    next_message_id: u64,
    stream_generation: u64,
    observed_generation: u64,
    observed_sequence: u64,
    wire_tx: flume::Sender<SurfaceEvent>,
    _wire_listener: Task<()>,
    stream_task: Option<tokio::task::JoinHandle<()>>,
    control_heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    observation_task: Option<tokio::task::JoinHandle<()>>,
    scroll: VirtualListScrollHandle,
    user_scroll_capture_pending: bool,
    last_emitted_scroll_offset: f32,
}

impl EventEmitter<AgentSurfaceEvent> for AgentSurface {}

impl Drop for AgentSurface {
    fn drop(&mut self) {
        if let Some(task) = self.stream_task.take() {
            task.abort();
        }
        if let Some(task) = self.control_heartbeat_task.take() {
            task.abort();
        }
        if let Some(task) = self.observation_task.take() {
            task.abort();
        }
    }
}

impl AgentSurface {
    pub fn new(
        client: MitsuroClient,
        runtime: Arc<Runtime>,
        session_id: impl Into<String>,
        project_dir: impl Into<String>,
        title: impl Into<String>,
        client_id: impl Into<String>,
        initial_draft: String,
        palette: CanvasPalette,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let initial_draft = bounded_agent_draft(&initial_draft);
        let composer = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Tell this Agent what to do")
                .multi_line(true)
                .default_value(initial_draft.clone())
        });
        cx.subscribe_in(
            &composer,
            window,
            |surface, input, event: &InputEvent, window, cx| match event {
                InputEvent::Change => surface.publish_composer_draft(input, cx),
                InputEvent::PressEnter { secondary: false } => {
                    surface.submit_from(input, window, cx)
                }
                _ => {}
            },
        )
        .detach();
        let answer_composer = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("Type another answer")
                .multi_line(true)
        });
        cx.subscribe_in(
            &answer_composer,
            window,
            |surface, _, event: &InputEvent, window, cx| {
                if matches!(event, InputEvent::PressEnter { secondary: false }) {
                    surface.submit_current_answer(window, cx);
                }
            },
        )
        .detach();

        let (wire_tx, wire_rx) = flume::bounded(EVENT_QUEUE_CAPACITY);
        let wire_listener = cx.spawn(async move |this, cx| {
            while let Ok(first) = wire_rx.recv_async().await {
                Timer::after(STREAM_FRAME_DELAY).await;
                let mut batch = Vec::with_capacity(32);
                batch.push(first);
                while batch.len() < 128 {
                    match wire_rx.try_recv() {
                        Ok(event) => batch.push(event),
                        Err(_) => break,
                    }
                }
                if this
                    .update(cx, |surface, cx| {
                        for event in batch {
                            surface.reduce(event, cx);
                        }
                        if surface.transcript_view.following_tail {
                            surface.scroll.scroll_to_bottom();
                        }
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        let mut surface = Self {
            client,
            runtime,
            palette,
            session_id: session_id.into(),
            project_dir: project_dir.into(),
            title: title.into(),
            client_id: client_id.into(),
            control_state: None,
            control_lease: None,
            control_notice: Some("Attaching control…".to_owned()),
            taking_control: true,
            hydrated: false,
            pending_outbound: None,
            composer,
            last_emitted_draft: initial_draft,
            answer_composer,
            messages: VecDeque::new(),
            tool_timeline: ToolTimeline::default(),
            activity: VecDeque::new(),
            thinking: String::new(),
            pending_approvals: Vec::new(),
            question_payloads: BTreeMap::new(),
            pending_input: None,
            streaming: false,
            transcript_view: TranscriptViewState::default(),
            error: None,
            next_message_id: 1,
            stream_generation: 0,
            observed_generation: 0,
            observed_sequence: 0,
            wire_tx,
            _wire_listener: wire_listener,
            stream_task: None,
            control_heartbeat_task: None,
            observation_task: None,
            scroll: VirtualListScrollHandle::new(),
            user_scroll_capture_pending: false,
            last_emitted_scroll_offset: 0.0,
        };
        surface.hydrate();
        surface.attach_control();
        surface
    }

    pub fn is_streaming(&self) -> bool {
        self.streaming
    }

    pub fn set_palette(&mut self, palette: CanvasPalette, cx: &mut Context<Self>) {
        if self.palette != palette {
            self.palette = palette;
            cx.notify();
        }
    }

    pub fn needs_attention(&self) -> bool {
        self.transcript_view.unread
            || !self.pending_approvals.is_empty()
            || self.pending_input.is_some()
    }

    pub fn compact_summary(&self) -> SharedString {
        if let Some(error) = &self.error {
            return error.clone().into();
        }
        if let Some(activity) = self.activity.back() {
            return activity.clone().into();
        }
        self.messages
            .back()
            .map(|message| single_line_excerpt(&message.markdown, 120).into())
            .unwrap_or_else(|| "Ready".into())
    }

    /// Acknowledge content only when it is actually visible at the live tail.
    /// Selecting a scrolled-back Agent must preserve its unread indicator until
    /// the user jumps or scrolls to the latest output.
    pub fn mark_read(&mut self, cx: &mut Context<Self>) {
        if self.transcript_view.unread && self.transcript_view.following_tail {
            self.transcript_view.mark_read();
            cx.notify();
        }
    }

    pub fn set_selected(&mut self, selected: bool, cx: &mut Context<Self>) {
        let previous = self.transcript_view;
        self.transcript_view.set_selected(selected);
        if self.transcript_view != previous {
            cx.notify();
        }
    }

    pub fn restore_scroll_offset(&mut self, offset: f32) {
        let offset = bounded_scroll_offset(offset);
        self.scroll.set_offset(point(px(0.0), px(-offset)));
        self.last_emitted_scroll_offset = offset;
        let max_offset = bounded_scroll_offset(f32::from(self.scroll.max_offset().height));
        self.transcript_view
            .user_scrolled(scroll_offset_is_at_tail(offset, max_offset));
    }

    pub fn scroll_offset(&self) -> f32 {
        bounded_scroll_offset(-f32::from(self.scroll.offset().y))
    }

    fn queue_user_scroll_capture(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.user_scroll_capture_pending {
            return;
        }
        self.user_scroll_capture_pending = true;
        cx.on_next_frame(window, |surface, _, cx| {
            surface.capture_user_scroll(cx);
        });
    }

    fn capture_user_scroll(&mut self, cx: &mut Context<Self>) {
        self.user_scroll_capture_pending = false;
        let offset = self.scroll_offset();
        let max_offset = bounded_scroll_offset(f32::from(self.scroll.max_offset().height));
        let previous = self.transcript_view;
        self.transcript_view
            .user_scrolled(scroll_offset_is_at_tail(offset, max_offset));
        self.emit_scroll_offset(offset, false, cx);
        if self.transcript_view != previous {
            cx.notify();
        }
    }

    fn jump_to_latest(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.transcript_view.jump_to_latest();
        self.scroll.scroll_to_bottom();
        let offset = bounded_scroll_offset(f32::from(self.scroll.max_offset().height));
        self.emit_scroll_offset(offset, true, cx);
        self.queue_user_scroll_capture(window, cx);
        cx.notify();
    }

    fn emit_scroll_offset(&mut self, offset: f32, force: bool, cx: &mut Context<Self>) {
        let offset = bounded_scroll_offset(offset);
        if force || (offset - self.last_emitted_scroll_offset).abs() >= 0.5 {
            self.last_emitted_scroll_offset = offset;
            cx.emit(AgentSurfaceEvent::ScrollChanged(offset));
        }
    }

    fn publish_composer_draft(&mut self, input: &Entity<InputState>, cx: &mut Context<Self>) {
        let value = input.read(cx).value();
        let draft = bounded_agent_draft(value.as_ref());
        if draft == self.last_emitted_draft {
            return;
        }
        self.last_emitted_draft = draft.clone();
        if draft.is_empty() {
            cx.emit(AgentSurfaceEvent::DraftCleared);
        } else {
            cx.emit(AgentSurfaceEvent::DraftChanged(draft));
        }
    }

    fn has_control(&self) -> bool {
        self.control_lease
            .as_ref()
            .is_some_and(|lease| lease_controls(lease, &self.session_id, &self.client_id))
    }

    fn attach_control(&mut self) {
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let client_id = self.client_id.clone();
        let tx = self.wire_tx.clone();
        self.runtime.spawn(async move {
            let result = attach_agent_control(&client, &session_id, &client_id)
                .await
                .map_err(|error| format!("Could not attach Agent control: {error:#}"));
            let _ = tx.send_async(SurfaceEvent::ControlAttached(result)).await;
        });
    }

    fn take_control(&mut self, cx: &mut Context<Self>) {
        if self.taking_control {
            return;
        }
        self.taking_control = true;
        self.control_notice = Some("Taking control…".to_owned());
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let client_id = self.client_id.clone();
        let tx = self.wire_tx.clone();
        self.runtime.spawn(async move {
            let result: anyhow::Result<ControlAttachment> = async {
                let response = client
                    .take_agent_control(
                        &session_id,
                        &TakeAgentControlRequest {
                            client_id,
                            force: true,
                        },
                    )
                    .await?;
                let state = client.get_agent_control(&session_id).await?;
                Ok(ControlAttachment {
                    state,
                    lease: Some(response.lease),
                    notice: None,
                })
            }
            .await;
            let result = result.map_err(|error| format!("Could not take Agent control: {error:#}"));
            let _ = tx.send_async(SurfaceEvent::ControlTaken(result)).await;
        });
        cx.notify();
    }

    fn start_control_heartbeat(&mut self) {
        if let Some(task) = self.control_heartbeat_task.take() {
            task.abort();
        }
        let Some(lease) = self.control_lease.as_ref() else {
            return;
        };
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let client_id = self.client_id.clone();
        let lease_token = lease.lease_token.clone();
        let tx = self.wire_tx.clone();
        self.control_heartbeat_task = Some(self.runtime.spawn(async move {
            loop {
                tokio::time::sleep(CONTROL_HEARTBEAT_INTERVAL).await;
                match client
                    .heartbeat_agent_control(
                        &session_id,
                        &HeartbeatAgentControlRequest {
                            client_id: client_id.clone(),
                            lease_token: lease_token.clone(),
                        },
                    )
                    .await
                {
                    Ok(state) => {
                        if tx
                            .send_async(SurfaceEvent::ControlHeartbeat(state))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(error) => {
                        let state = client.get_agent_control(&session_id).await.ok();
                        let _ = tx
                            .send_async(SurfaceEvent::ControlLost {
                                state,
                                message: format!("Agent control was released: {error:#}"),
                            })
                            .await;
                        return;
                    }
                }
            }
        }));
    }

    fn start_observation(&mut self, generation: u64, sequence: u64) {
        if let Some(task) = self.observation_task.take() {
            task.abort();
        }
        self.observed_generation = generation;
        self.observed_sequence = sequence;
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let tx = self.wire_tx.clone();
        self.observation_task = Some(self.runtime.spawn(async move {
            let mut generation = generation;
            let mut sequence = sequence;
            loop {
                let stream = client
                    .observe_agent_stream(&session_id, generation, sequence)
                    .await;
                let mut stream = match stream {
                    Ok(stream) => stream,
                    Err(error) => {
                        if tx
                            .send_async(SurfaceEvent::ObservationInterrupted(format!(
                                "Live Agent view disconnected; retrying: {error:#}"
                            )))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let mut interrupted = "Live Agent view ended; reconnecting".to_owned();
                while let Some(event) = stream.next().await {
                    match event {
                        Ok(envelope) => {
                            if observer_cursor_accepts(
                                generation,
                                sequence,
                                envelope.generation,
                                envelope.sequence,
                            ) {
                                generation = envelope.generation;
                                sequence = envelope.sequence;
                            }
                            if tx
                                .send_async(SurfaceEvent::Observed(envelope))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(error) => {
                            interrupted =
                                format!("Live Agent view interrupted; retrying: {error:#}");
                            break;
                        }
                    }
                }
                if tx
                    .send_async(SurfaceEvent::ObservationInterrupted(interrupted))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }));
    }

    pub fn send(&mut self, message: String, cx: &mut Context<Self>) {
        let message = message.trim().to_owned();
        if message.is_empty() {
            return;
        }
        if !self.hydrated {
            let may_eventually_control = self.taking_control || self.has_control();
            if queue_outbound_while_initializing(
                &mut self.pending_outbound,
                may_eventually_control,
                message,
            ) {
                self.control_notice = Some("Command queued until Agent history loads…".to_owned());
            } else {
                self.control_notice =
                    Some("Agent is still loading; one command is already queued".to_owned());
            }
            cx.notify();
            return;
        }
        if self.pending_input.is_some() {
            self.control_notice = Some("Answer the pending Agent question first".to_owned());
            cx.notify();
            return;
        }
        let Some(lease_token) = self
            .control_lease
            .as_ref()
            .filter(|lease| lease_controls(lease, &self.session_id, &self.client_id))
            .map(|lease| lease.lease_token.clone())
        else {
            if queue_outbound_while_initializing(
                &mut self.pending_outbound,
                self.taking_control,
                message,
            ) {
                self.control_notice = Some("Command queued until control attaches…".to_owned());
            } else {
                self.control_notice = Some("Observer mode · Take control to send".to_owned());
            }
            cx.notify();
            return;
        };
        self.push_message(AgentRole::User, message.clone());
        self.error = None;
        self.transcript_view.jump_to_latest();

        if self.streaming {
            let client = self.client.clone();
            let session_id = self.session_id.clone();
            let tx = self.wire_tx.clone();
            let generation = self.stream_generation;
            self.runtime.spawn(async move {
                let result = client
                    .steer_with_lease(
                        SteerRequest {
                            session_id,
                            message,
                            content: Vec::new(),
                        },
                        Some(&lease_token),
                    )
                    .await;
                if let Err(error) = result {
                    let _ = tx
                        .send_async(SurfaceEvent::Failure {
                            generation,
                            message: format!("Could not steer the active Agent: {error:#}"),
                        })
                        .await;
                }
            });
            self.push_activity("Direction queued for the active turn".to_owned());
            cx.notify();
            return;
        }

        self.stream_generation = self.stream_generation.wrapping_add(1);
        let generation = self.stream_generation;
        self.streaming = true;
        self.thinking.clear();
        self.push_message(AgentRole::Assistant, String::new());
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let project_dir = self.project_dir.clone();
        let tx = self.wire_tx.clone();
        let request = ChatRequest {
            session_id: Some(session_id),
            message,
            content: Vec::<ContentBlock>::new(),
            project_dir: Some(project_dir.clone()),
            working_dir: Some(project_dir),
            workspace_mode: None,
            target_branch: None,
            session_type: Some(SessionType::Code),
            model: None,
            model_key: None,
            thinking_enabled: None,
            fast_mode: None,
            permission_mode: None,
            mode: None,
            research_enabled: None,
        };
        self.stream_task = Some(self.runtime.spawn(async move {
            let stream = client
                .chat_stream_with_lease(request, Some(&lease_token))
                .await;
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(error) => {
                    let _ = tx
                        .send_async(SurfaceEvent::Failure {
                            generation,
                            message: format!("Agent request failed: {error:#}"),
                        })
                        .await;
                    return;
                }
            };
            while let Some(event) = stream.next().await {
                if let Err(error) = event {
                    if tx
                        .send_async(SurfaceEvent::Failure {
                            generation,
                            message: format!("Agent stream failed: {error:#}"),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            let _ = tx
                .send_async(SurfaceEvent::StreamEnded { generation })
                .await;
        }));
        self.scroll.scroll_to_bottom();
        cx.notify();
    }

    fn submit_from(
        &mut self,
        input: &Entity<InputState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let message = input.read(cx).value().to_string();
        if message.trim().is_empty() {
            return;
        }
        if !self.has_control() && !self.taking_control {
            self.control_notice = Some("Observer mode · Take control to send".to_owned());
            cx.notify();
            return;
        }
        self.last_emitted_draft.clear();
        cx.emit(AgentSurfaceEvent::DraftCleared);
        input.update(cx, |state, cx| state.set_value("", window, cx));
        self.send(message, cx);
    }

    fn hydrate(&mut self) {
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let tx = self.wire_tx.clone();
        self.runtime.spawn(async move {
            match client.get_session(&session_id).await {
                Ok(session) => {
                    let state = client.get_session_state(&session_id).await.ok();
                    let interactions = state
                        .as_ref()
                        .map(|state| {
                            hydrated_interactions_from_snapshots(&state.pending_interactions)
                        })
                        .unwrap_or_default();
                    let mut messages = session
                        .messages
                        .into_iter()
                        .enumerate()
                        .filter_map(|(index, message)| hydrated_message(index as u64 + 1, message))
                        .collect::<Vec<_>>();
                    let partial = state
                        .as_ref()
                        .and_then(|state| state.live_partial_assistant.as_ref());
                    if let Some(text) = partial
                        .map(|partial| partial.text.trim())
                        .filter(|text| !text.is_empty())
                    {
                        let mut markdown = text.to_owned();
                        truncate_front(&mut markdown, MAX_MESSAGE_CHARS);
                        messages.push(AgentMessage {
                            id: messages.len() as u64 + 1,
                            role: AgentRole::Assistant,
                            markdown,
                        });
                    }
                    let thinking = partial
                        .and_then(|partial| partial.thinking.clone())
                        .unwrap_or_default();
                    let agent_active = state
                        .as_ref()
                        .is_some_and(|state| agent_state_is_streaming(&state.agent_state))
                        && interactions.pending_input.is_none();
                    let (observer_generation, observer_sequence) = state
                        .as_ref()
                        .map(|state| (state.agent_stream_generation, state.agent_stream_sequence))
                        .unwrap_or_default();
                    let _ = tx
                        .send_async(SurfaceEvent::Hydrated {
                            messages,
                            pending_approvals: interactions.pending_approvals,
                            pending_input: interactions.pending_input,
                            thinking,
                            agent_active,
                            observer_generation,
                            observer_sequence,
                        })
                        .await;
                }
                Err(error) => {
                    let _ = tx
                        .send_async(SurfaceEvent::HydrationFailed(format!(
                            "Could not load Agent history: {error:#}"
                        )))
                        .await;
                }
            }
        });
    }

    fn reduce(&mut self, incoming: SurfaceEvent, cx: &mut Context<Self>) {
        let mut flush_pending = None;
        match incoming {
            SurfaceEvent::ControlAttached(result) | SurfaceEvent::ControlTaken(result) => {
                self.taking_control = false;
                match result {
                    Ok(attachment) => {
                        self.control_state = Some(attachment.state);
                        self.control_lease = attachment.lease;
                        self.control_notice = attachment.notice;
                        self.start_control_heartbeat();
                        let acquired_control = self.has_control();
                        flush_pending = finish_pending_initial_command(
                            &mut self.pending_outbound,
                            true,
                            acquired_control,
                            self.hydrated,
                        );
                    }
                    Err(error) => {
                        self.control_lease = None;
                        self.control_notice = Some(error);
                        if let Some(task) = self.control_heartbeat_task.take() {
                            task.abort();
                        }
                        self.pending_outbound = None;
                    }
                }
            }
            SurfaceEvent::ControlHeartbeat(state) => {
                self.control_state = Some(state);
                self.control_notice = None;
            }
            SurfaceEvent::ControlLost { state, message } => {
                self.control_state = state;
                self.control_lease = None;
                self.control_notice = Some(message);
                if let Some(task) = self.control_heartbeat_task.take() {
                    task.abort();
                }
                self.pending_outbound = None;
            }
            SurfaceEvent::Hydrated {
                messages,
                pending_approvals,
                pending_input,
                thinking,
                agent_active,
                observer_generation,
                observer_sequence,
            } => {
                if !self.streaming {
                    self.messages = messages.into_iter().collect();
                    self.pending_approvals = pending_approvals;
                    self.pending_input = pending_input;
                    self.thinking = thinking;
                    self.streaming = agent_active;
                    self.next_message_id = self
                        .messages
                        .back()
                        .map_or(1, |message| message.id.saturating_add(1));
                    self.trim_history();
                }
                self.hydrated = true;
                self.start_observation(observer_generation, observer_sequence);
                let acquired_control = self.has_control();
                flush_pending = finish_pending_initial_command(
                    &mut self.pending_outbound,
                    !self.taking_control,
                    acquired_control,
                    true,
                );
            }
            SurfaceEvent::HydrationFailed(message) => {
                self.hydrated = true;
                self.error = Some(message.clone());
                self.push_message(AgentRole::Notice, message);
                self.start_observation(0, 0);
                let acquired_control = self.has_control();
                flush_pending = finish_pending_initial_command(
                    &mut self.pending_outbound,
                    !self.taking_control,
                    acquired_control,
                    true,
                );
            }
            SurfaceEvent::StreamEnded { generation }
                if should_end_stream(self.stream_generation, generation, self.streaming) =>
            {
                self.streaming = false;
                self.thinking.clear();
                self.stream_task = None;
                self.push_activity("Stream ended".to_owned());
            }
            SurfaceEvent::StreamEnded { .. } => {}
            SurfaceEvent::Observed(envelope) => {
                let new_generation = envelope.generation > self.observed_generation;
                if observer_cursor_accepts(
                    self.observed_generation,
                    self.observed_sequence,
                    envelope.generation,
                    envelope.sequence,
                ) {
                    self.observed_generation = envelope.generation;
                    self.observed_sequence = envelope.sequence;
                    let marks_active = observed_event_marks_active(&envelope.event);
                    if new_generation && marks_active {
                        let has_local_placeholder = self.streaming
                            && self.messages.back().is_some_and(|message| {
                                message.role == AgentRole::Assistant && message.markdown.is_empty()
                            });
                        if !has_local_placeholder {
                            self.push_message(AgentRole::Assistant, String::new());
                        }
                    }
                    if marks_active {
                        self.streaming = true;
                    }
                    self.reduce_stream_event(envelope.event);
                }
            }
            SurfaceEvent::ObservationInterrupted(message) => {
                self.push_activity(single_line_excerpt(&message, 240));
            }
            SurfaceEvent::Failure {
                generation,
                message,
            } if generation == 0 || generation == self.stream_generation => {
                self.streaming = false;
                self.error = Some(message.clone());
                self.push_message(AgentRole::Notice, message);
                self.transcript_view.note_incoming(true);
            }
            SurfaceEvent::Failure { .. } => {}
            SurfaceEvent::ApprovalResult {
                id,
                approved,
                error,
            } => {
                self.pending_approvals
                    .retain(|approval| approval.tool_call_id != id);
                match error {
                    Some(error) => {
                        self.error = Some(error.clone());
                        self.push_message(AgentRole::Notice, error);
                        self.transcript_view.note_incoming(true);
                    }
                    None => self.push_activity(format!(
                        "Tool request {}",
                        if approved { "approved" } else { "denied" }
                    )),
                }
            }
            SurfaceEvent::InputAccepted { tool_call_id } => {
                if self
                    .pending_input
                    .as_ref()
                    .is_some_and(|input| input.tool_call_id == tool_call_id)
                {
                    self.pending_input = None;
                }
                self.question_payloads.remove(&tool_call_id);
                self.push_activity("Answer sent".to_owned());
            }
            SurfaceEvent::InputFailure {
                tool_call_id,
                message,
            } => {
                if let Some(input) = self
                    .pending_input
                    .as_mut()
                    .filter(|input| input.tool_call_id == tool_call_id)
                {
                    input.submitting = false;
                }
                self.streaming = false;
                self.error = Some(message.clone());
                self.push_message(AgentRole::Notice, message);
                self.transcript_view.note_incoming(true);
            }
            SurfaceEvent::CancelResult(error) => match error {
                Some(error) => self.error = Some(error),
                None => {
                    self.streaming = false;
                    self.push_activity("Turn interrupted".to_owned());
                }
            },
        }
        if let Some(message) = flush_pending {
            self.send(message, cx);
        }
        cx.notify();
    }

    fn reduce_stream_event(&mut self, event: ChatStreamEvent) {
        if let Some(update) = self.tool_timeline.apply_event(&event) {
            self.push_activity(update.activity);
            self.transcript_view.note_incoming(update.attention);
        }
        match event {
            ChatStreamEvent::TextDelta { delta }
            | ChatStreamEvent::TextDeltaWithCitations { delta, .. } => {
                self.append_assistant(delta);
                self.transcript_view.note_incoming(false);
            }
            ChatStreamEvent::ThinkingDelta { thinking } => {
                self.thinking.push_str(&thinking);
                truncate_front(&mut self.thinking, 4_000);
            }
            ChatStreamEvent::ThinkingComplete { .. } => self.thinking.clear(),
            ChatStreamEvent::ToolCallComplete {
                id,
                name,
                arguments,
            } => {
                if name == "AskUserQuestion" {
                    self.question_payloads.insert(
                        id,
                        (
                            parse_agent_questions(&arguments),
                            AgentInputResponseKind::Questions,
                        ),
                    );
                }
            }
            ChatStreamEvent::ToolApprovalRequired {
                id,
                name,
                arguments,
            } => {
                if let Some(approval) = self
                    .pending_approvals
                    .iter_mut()
                    .find(|approval| approval.tool_call_id == id)
                {
                    approval.tool_name = name;
                    approval.summary = single_line_excerpt(&arguments.to_string(), 240);
                } else {
                    self.pending_approvals.push(PendingApproval {
                        tool_call_id: id,
                        tool_name: name,
                        summary: single_line_excerpt(&arguments.to_string(), 240),
                    });
                }
            }
            ChatStreamEvent::ToolApproved { id } | ChatStreamEvent::ToolDenied { id } => {
                self.pending_approvals
                    .retain(|approval| approval.tool_call_id != id);
            }
            ChatStreamEvent::AwaitingInput {
                tool_name,
                tool_call_id,
            } => {
                let (questions, response_kind) = self
                    .question_payloads
                    .get(&tool_call_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        (
                            vec![fallback_agent_question(&tool_name)],
                            AgentInputResponseKind::Questions,
                        )
                    });
                self.pending_input = Some(PendingAgentInput::with_kind(
                    tool_call_id.clone(),
                    questions,
                    response_kind,
                ));
                self.streaming = false;
                self.push_message(
                    AgentRole::Notice,
                    format!("{tool_name} is waiting for your input (`{tool_call_id}`)."),
                );
            }
            ChatStreamEvent::PlanUpdate { items } => {
                let text = items
                    .into_iter()
                    .map(|item| {
                        format!(
                            "- [{}] {}",
                            if item.completed { "x" } else { " " },
                            item.content
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                self.push_message(AgentRole::Notice, text);
                self.transcript_view.note_incoming(false);
            }
            ChatStreamEvent::PlanComplete {
                tool_call_id,
                title,
                task_count,
            } => {
                self.question_payloads.insert(
                    tool_call_id,
                    (
                        vec![plan_confirm_question(&title, task_count)],
                        AgentInputResponseKind::PlanConfirm,
                    ),
                );
                self.push_activity(format!("Plan ready · {task_count} tasks"));
            }
            ChatStreamEvent::Lagged { skipped } => {
                self.push_message(
                    AgentRole::Notice,
                    format!(
                        "Live display skipped {skipped} events; durable history will reconcile."
                    ),
                );
                self.transcript_view.note_incoming(true);
            }
            ChatStreamEvent::TitleUpdate { title } => self.title = title,
            ChatStreamEvent::Finish { .. } => {
                self.streaming = false;
                self.thinking.clear();
                self.push_activity("Ready".to_owned());
            }
            ChatStreamEvent::Error { error } => {
                self.streaming = false;
                self.error = Some(error.clone());
                self.push_message(AgentRole::Notice, error);
                self.transcript_view.note_incoming(true);
            }
            ChatStreamEvent::UserMessage { message, .. } => {
                self.push_message(AgentRole::Notice, message);
                self.transcript_view.note_incoming(true);
            }
            ChatStreamEvent::AgentSleeping {
                duration_secs,
                reason,
            } => self.push_activity(format!("Pausing {duration_secs}s — {reason}")),
            ChatStreamEvent::TurnComplete { turn, has_more } => self.push_activity(format!(
                "Turn {turn} complete{}",
                if has_more { " · continuing" } else { "" }
            )),
            ChatStreamEvent::ContextCompactionStarted { .. } => {
                self.push_activity("Compacting context".to_owned())
            }
            ChatStreamEvent::ContextCompacted { .. } => {
                self.push_activity("Context compacted".to_owned())
            }
            ChatStreamEvent::SteeringInjected { .. } => {
                self.push_activity("Direction applied".to_owned())
            }
            ChatStreamEvent::ToolCallStart { .. }
            | ChatStreamEvent::ToolExecuting { .. }
            | ChatStreamEvent::ToolOutputDelta { .. }
            | ChatStreamEvent::ToolResult { .. }
            | ChatStreamEvent::ServerToolStart { .. }
            | ChatStreamEvent::ServerToolComplete { .. }
            | ChatStreamEvent::ServerToolError { .. }
            | ChatStreamEvent::WebSearchResults { .. }
            | ChatStreamEvent::WebFetchResult { .. }
            | ChatStreamEvent::DelegatedProgress { .. }
            | ChatStreamEvent::DelegationEvent { .. } => {}
            _ => {}
        }
        self.trim_history();
    }

    fn toggle_answer_option(&mut self, label: String, cx: &mut Context<Self>) {
        if !self.has_control() {
            self.control_notice = Some("Observer mode · Take control to answer".to_owned());
            cx.notify();
            return;
        }
        let Some(input) = self.pending_input.as_mut() else {
            return;
        };
        if input.submitting {
            return;
        }
        let multi_select = input
            .questions
            .get(input.question_index)
            .is_some_and(|question| question.multi_select);
        if multi_select {
            if let Some(index) = input.selected.iter().position(|value| value == &label) {
                input.selected.remove(index);
            } else {
                input.selected.push(label);
            }
        } else {
            input.selected.clear();
            input.selected.push(label);
        }
        cx.notify();
    }

    fn submit_current_answer(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.has_control() {
            self.control_notice = Some("Observer mode · Take control to answer".to_owned());
            cx.notify();
            return;
        }
        let custom = self.answer_composer.read(cx).value().trim().to_owned();
        let mut completed = None;
        let Some(input) = self.pending_input.as_mut() else {
            return;
        };
        if input.submitting {
            return;
        }
        let Some(question) = input.questions.get(input.question_index).cloned() else {
            self.pending_input = None;
            return;
        };
        let answer = if !custom.is_empty() {
            serde_json::Value::String(custom)
        } else if question.multi_select && !input.selected.is_empty() {
            serde_json::Value::Array(
                input
                    .selected
                    .iter()
                    .cloned()
                    .map(serde_json::Value::String)
                    .collect(),
            )
        } else if let Some(value) = input.selected.first() {
            serde_json::Value::String(value.clone())
        } else {
            self.control_notice = Some("Choose an option or type an answer".to_owned());
            cx.notify();
            return;
        };
        input.answers.insert(question.header, answer);
        input.selected.clear();
        self.answer_composer
            .update(cx, |state, cx| state.set_value("", window, cx));

        if input.question_index + 1 < input.questions.len() {
            input.question_index += 1;
            self.control_notice = None;
        } else {
            input.submitting = true;
            let result = encode_agent_input_response(input.response_kind, &input.answers);
            completed = Some((input.tool_call_id.clone(), result));
        }

        if let Some((tool_call_id, result)) = completed {
            self.submit_agent_input(tool_call_id, result, cx);
        } else {
            cx.notify();
        }
    }

    fn submit_agent_input(&mut self, tool_call_id: String, result: String, cx: &mut Context<Self>) {
        let Some(lease_token) = self
            .control_lease
            .as_ref()
            .filter(|lease| lease_controls(lease, &self.session_id, &self.client_id))
            .map(|lease| lease.lease_token.clone())
        else {
            if let Some(input) = self.pending_input.as_mut() {
                input.submitting = false;
            }
            self.control_notice = Some("Observer mode · Take control to answer".to_owned());
            cx.notify();
            return;
        };
        if let Some(task) = self.stream_task.take() {
            task.abort();
        }
        self.stream_generation = self.stream_generation.wrapping_add(1);
        let generation = self.stream_generation;
        self.streaming = true;
        self.error = None;
        self.push_activity("Sending answer".to_owned());
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let tx = self.wire_tx.clone();
        let request_tool_call_id = tool_call_id;
        self.stream_task = Some(self.runtime.spawn(async move {
            let stream = client
                .answer_agent_input_stream_with_lease(
                    AgentInputResponseRequest {
                        session_id,
                        tool_call_id: request_tool_call_id.clone(),
                        result,
                    },
                    &lease_token,
                )
                .await;
            let mut stream = match stream {
                Ok(stream) => {
                    if tx
                        .send_async(SurfaceEvent::InputAccepted {
                            tool_call_id: request_tool_call_id.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    stream
                }
                Err(error) => {
                    let _ = tx
                        .send_async(SurfaceEvent::InputFailure {
                            tool_call_id: request_tool_call_id,
                            message: format!("Could not submit Agent answer: {error:#}"),
                        })
                        .await;
                    return;
                }
            };
            while let Some(event) = stream.next().await {
                if let Err(error) = event {
                    if tx
                        .send_async(SurfaceEvent::Failure {
                            generation,
                            message: format!("Agent continuation failed: {error:#}"),
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            let _ = tx
                .send_async(SurfaceEvent::StreamEnded { generation })
                .await;
        }));
        cx.notify();
    }

    fn approve(&mut self, id: String, approved: bool, cx: &mut Context<Self>) {
        let Some(lease_token) = self
            .control_lease
            .as_ref()
            .filter(|lease| lease_controls(lease, &self.session_id, &self.client_id))
            .map(|lease| lease.lease_token.clone())
        else {
            self.control_notice = Some("Observer mode · Take control to respond".to_owned());
            cx.notify();
            return;
        };
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let tx = self.wire_tx.clone();
        self.runtime.spawn(async move {
            let result = client
                .approve_tool_with_lease(&session_id, &id, approved, Some(&lease_token))
                .await;
            let _ = tx
                .send_async(SurfaceEvent::ApprovalResult {
                    id,
                    approved,
                    error: result
                        .err()
                        .map(|error| format!("Approval failed: {error:#}")),
                })
                .await;
        });
        cx.notify();
    }

    fn cancel(&mut self, cx: &mut Context<Self>) {
        if !self.streaming {
            return;
        }
        let Some(lease_token) = self
            .control_lease
            .as_ref()
            .filter(|lease| lease_controls(lease, &self.session_id, &self.client_id))
            .map(|lease| lease.lease_token.clone())
        else {
            self.control_notice = Some("Observer mode · Take control to interrupt".to_owned());
            cx.notify();
            return;
        };
        let client = self.client.clone();
        let session_id = self.session_id.clone();
        let tx = self.wire_tx.clone();
        self.runtime.spawn(async move {
            let result = client
                .cancel_session_with_lease(&session_id, Some(&lease_token))
                .await;
            let _ = tx
                .send_async(SurfaceEvent::CancelResult(
                    result
                        .err()
                        .map(|error| format!("Interrupt failed: {error:#}")),
                ))
                .await;
        });
        cx.notify();
    }

    fn append_assistant(&mut self, delta: String) {
        if let Some(message) = self
            .messages
            .iter_mut()
            .rev()
            .find(|message| message.role == AgentRole::Assistant)
        {
            message.markdown.push_str(&delta);
            truncate_front(&mut message.markdown, MAX_MESSAGE_CHARS);
        } else {
            self.push_message(AgentRole::Assistant, delta);
        }
    }

    fn push_message(&mut self, role: AgentRole, mut markdown: String) {
        truncate_front(&mut markdown, MAX_MESSAGE_CHARS);
        let id = self.next_message_id;
        self.next_message_id = self.next_message_id.saturating_add(1);
        self.messages.push_back(AgentMessage { id, role, markdown });
        self.trim_history();
    }

    fn push_activity(&mut self, activity: String) {
        let activity = single_line_excerpt(&activity, 240);
        if self.activity.back() != Some(&activity) {
            self.activity.push_back(activity);
        }
        while self.activity.len() > MAX_ACTIVITY {
            self.activity.pop_front();
        }
    }

    fn trim_history(&mut self) {
        while self.messages.len() > MAX_MESSAGES {
            self.messages.pop_front();
        }
        let mut transcript_chars = self
            .messages
            .iter()
            .map(|message| message.markdown.chars().count())
            .sum::<usize>();
        while transcript_chars > MAX_TRANSCRIPT_CHARS && self.messages.len() > 1 {
            if let Some(message) = self.messages.pop_front() {
                transcript_chars =
                    transcript_chars.saturating_sub(message.markdown.chars().count());
            }
        }
    }

    fn render_message(
        &self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let palette = self.palette;
        let Some(message) = self.messages.get(index) else {
            return div().into_any_element();
        };
        let user = message.role == AgentRole::User;
        let notice = message.role == AgentRole::Notice;
        let content = if message.markdown.is_empty() && self.streaming {
            "Working…".to_owned()
        } else {
            message.markdown.clone()
        };
        div()
            .w_full()
            .px(px(14.0))
            .py(px(8.0))
            .child(
                div()
                    .w_full()
                    .px(if user { px(12.0) } else { px(0.0) })
                    .py(if user { px(9.0) } else { px(2.0) })
                    .rounded(px(4.0))
                    .when(user, |element| element.bg(palette.surface_high))
                    .when(notice, |element| {
                        element.text_color(palette.text_muted).text_sm()
                    })
                    .child(
                        TextView::markdown(("agent-message", message.id), content, window, cx)
                            .selectable(true),
                    ),
            )
            .into_any_element()
    }

    fn render_tool_entry(
        &self,
        entry: ToolTimelineEntry,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let palette = self.palette;
        let status_color = if entry.status.is_error() {
            palette.danger
        } else if matches!(
            entry.status,
            ToolTimelineStatus::AwaitingApproval | ToolTimelineStatus::AwaitingInput
        ) {
            palette.brass
        } else if matches!(
            entry.status,
            ToolTimelineStatus::Starting
                | ToolTimelineStatus::Prepared
                | ToolTimelineStatus::Running
        ) {
            palette.pulse
        } else {
            palette.text_muted
        };
        let detail = entry
            .detail
            .as_ref()
            .map(rendered_tool_detail_markdown)
            .filter(|detail| !detail.is_empty());

        div()
            .w_full()
            .px(px(12.0))
            .py(px(7.0))
            .border_t_1()
            .border_color(palette.edge)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(px(7.0))
                    .text_xs()
                    .child(div().size(px(5.0)).rounded(px(99.0)).bg(status_color))
                    .child(entry.kind.label())
                    .child(div().text_color(palette.text).child(entry.name.clone()))
                    .child(div().text_color(status_color).child(entry.status.label())),
            )
            .when_some(detail, |surface, detail| {
                surface.child(
                    div()
                        .mt(px(5.0))
                        .text_xs()
                        .text_color(palette.text_muted)
                        .child(
                            TextView::markdown(
                                ("agent-tool-detail", entry.sequence),
                                detail,
                                window,
                                cx,
                            )
                            .selectable(true),
                        ),
                )
            })
            .into_any_element()
    }
}

impl Render for AgentSurface {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.transcript_view.following_tail {
            self.queue_user_scroll_capture(window, cx);
        }
        let palette = self.palette;
        let item_sizes = Rc::new(
            self.messages
                .iter()
                .map(|message| size(px(1.0), estimate_message_height(message)))
                .collect::<Vec<_>>(),
        );
        let approvals = self.pending_approvals.clone();
        let activity = self.activity.back().cloned();
        let thinking =
            (!self.thinking.is_empty()).then(|| single_line_excerpt(&self.thinking, 260));
        let has_control = self.has_control();
        let control_notice = self.control_notice.clone().or_else(|| {
            (!has_control).then(|| {
                self.control_state
                    .as_ref()
                    .and_then(|state| state.controller.as_ref())
                    .map(|controller| format!("Observer · controlled by {}", controller.client_id))
                    .unwrap_or_else(|| "Observer · control is unclaimed".to_owned())
            })
        });
        let can_take = !has_control;
        let pending_question = self.pending_input.as_ref().and_then(|input| {
            input
                .questions
                .get(input.question_index)
                .cloned()
                .map(|question| {
                    (
                        input.tool_call_id.clone(),
                        input.question_index,
                        input.questions.len(),
                        question,
                        input.selected.clone(),
                        input.submitting,
                    )
                })
        });
        let has_typed_answer = !self.answer_composer.read(cx).value().trim().is_empty();
        let has_pending_input = self.pending_input.is_some();
        let show_jump_to_latest = !self.transcript_view.following_tail;
        let jump_label = if self.transcript_view.unread {
            "Jump to latest · new"
        } else {
            "Jump to latest"
        };
        let tool_entries = self
            .tool_timeline
            .recent(MAX_VISIBLE_TOOL_ENTRIES)
            .into_iter()
            .map(|entry| self.render_tool_entry(entry, window, cx).into_any_element())
            .collect::<Vec<_>>();

        div()
            .flex()
            .flex_col()
            .size_full()
            .min_h_0()
            .bg(palette.surface)
            .text_color(palette.text)
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(px(14.0))
                    .h(px(36.0))
                    .border_b_1()
                    .border_color(palette.edge)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(px(8.0))
                            .child(div().size(px(6.0)).rounded(px(99.0)).bg(if self.streaming {
                                palette.pulse
                            } else {
                                palette.text_muted
                            }))
                            .child(self.title.clone()),
                    )
                    .when(self.streaming && has_control, |header| {
                        header.child(
                            Button::new("interrupt-agent")
                                .label("Stop")
                                .compact()
                                .on_click(cx.listener(|surface, _, _, cx| surface.cancel(cx))),
                        )
                    })
                    .when(can_take, |header| {
                        header.child(
                            Button::new(SharedString::from(format!(
                                "take-agent-control-{}",
                                self.session_id
                            )))
                            .label(if self.taking_control {
                                "Attaching…"
                            } else {
                                "Take control"
                            })
                            .compact()
                            .disabled(self.taking_control)
                            .on_click(cx.listener(|surface, _, _, cx| surface.take_control(cx))),
                        )
                    }),
            )
            .when_some(control_notice, |surface, notice| {
                surface.child(
                    div()
                        .px(px(14.0))
                        .py(px(5.0))
                        .border_b_1()
                        .border_color(palette.edge)
                        .text_xs()
                        .text_color(palette.text_muted)
                        .child(notice),
                )
            })
            .when_some(thinking, |surface, thinking| {
                surface.child(
                    div()
                        .px(px(14.0))
                        .py(px(6.0))
                        .text_xs()
                        .text_color(palette.text_muted)
                        .child(thinking),
                )
            })
            .when_some(activity, |surface, activity| {
                surface.child(
                    div()
                        .px(px(14.0))
                        .pb(px(5.0))
                        .text_xs()
                        .text_color(palette.pulse)
                        .child(activity),
                )
            })
            .child(
                div()
                    .id("agent-transcript-shell")
                    .relative()
                    .flex()
                    .flex_1()
                    .min_h_0()
                    .on_scroll_wheel(cx.listener(
                        |surface, _: &ScrollWheelEvent, window, cx| {
                            surface.queue_user_scroll_capture(window, cx);
                            cx.stop_propagation();
                        },
                    ))
                    .child(
                        v_virtual_list(
                            cx.entity(),
                            "agent-transcript",
                            item_sizes,
                            |surface, range, window, cx| {
                                range
                                    .map(|index| surface.render_message(index, window, cx))
                                    .collect::<Vec<_>>()
                            },
                        )
                        .track_scroll(&self.scroll)
                        .flex_1()
                        .min_h_0(),
                    )
                    .when(show_jump_to_latest, |transcript| {
                        transcript.child(
                            div()
                                .absolute()
                                .right(px(12.0))
                                .bottom(px(10.0))
                                .child(
                                    Button::new("agent-jump-to-latest")
                                        .label(jump_label)
                                        .compact()
                                        .on_click(cx.listener(|surface, _, window, cx| {
                                            surface.jump_to_latest(window, cx)
                                        })),
                                ),
                        )
                    }),
            )
            .when(!tool_entries.is_empty(), |surface| {
                surface.child(
                    div()
                        .max_h(px(240.0))
                        .overflow_y_scrollbar()
                        .border_b_1()
                        .border_color(palette.edge)
                        .children(tool_entries),
                )
            })
            .when_some(
                pending_question,
                |surface,
                 (tool_call_id, question_index, question_count, question, selected, submitting)| {
                    let can_submit =
                        has_control && !submitting && (has_typed_answer || !selected.is_empty());
                    surface.child(
                        div()
                            .mx(px(12.0))
                            .mb(px(8.0))
                            .pt(px(10.0))
                            .border_t_1()
                            .border_color(palette.brass)
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_between()
                                    .text_xs()
                                    .text_color(palette.brass)
                                    .child(question.header.clone())
                                    .child(format!(
                                        "{} of {}",
                                        question_index + 1,
                                        question_count
                                    )),
                            )
                            .child(
                                div()
                                    .mt(px(5.0))
                                    .text_sm()
                                    .child(question.question.clone()),
                            )
                            .child(
                                div()
                                    .flex()
                                    .flex_wrap()
                                    .gap(px(6.0))
                                    .mt(px(8.0))
                                    .children(question.options.into_iter().enumerate().map(
                                        |(option_index, option)| {
                                            let selected_option =
                                                selected.iter().any(|value| value == &option.label);
                                            let label = option
                                                .description
                                                .as_ref()
                                                .map(|description| {
                                                    format!(
                                                        "{}{} — {}",
                                                        if selected_option { "✓ " } else { "" },
                                                        option.label,
                                                        description
                                                    )
                                                })
                                                .unwrap_or_else(|| {
                                                    format!(
                                                        "{}{}",
                                                        if selected_option { "✓ " } else { "" },
                                                        option.label
                                                    )
                                                });
                                            let value = option.label;
                                            Button::new(SharedString::from(format!(
                                                "agent-answer-{tool_call_id}-{question_index}-{option_index}"
                                            )))
                                            .label(label)
                                            .compact()
                                            .disabled(!has_control || submitting)
                                            .on_click(cx.listener(move |surface, _, _, cx| {
                                                surface.toggle_answer_option(value.clone(), cx)
                                            }))
                                        },
                                    )),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .gap(px(6.0))
                                    .mt(px(8.0))
                                    .child(
                                        div()
                                            .flex_1()
                                            .min_h(px(34.0))
                                            .border_1()
                                            .border_color(palette.edge_strong)
                                            .bg(palette.canvas)
                                            .child(
                                                Input::new(&self.answer_composer)
                                                    .appearance(false)
                                                    .bordered(false)
                                                    .focus_bordered(false)
                                                    .disabled(!has_control || submitting),
                                            ),
                                    )
                                    .child(
                                        Button::new(SharedString::from(format!(
                                            "agent-submit-answer-{tool_call_id}-{question_index}"
                                        )))
                                        .label(if submitting {
                                            "Sending…"
                                        } else if question_index + 1 < question_count {
                                            "Next"
                                        } else {
                                            "Send answer"
                                        })
                                        .compact()
                                        .disabled(!can_submit)
                                        .on_click(cx.listener(|surface, _, window, cx| {
                                            surface.submit_current_answer(window, cx)
                                        })),
                                    ),
                            ),
                    )
                },
            )
            .children(approvals.into_iter().map(|approval| {
                let approve_id = approval.tool_call_id.clone();
                let deny_id = approval.tool_call_id.clone();
                div()
                    .mx(px(12.0))
                    .mb(px(7.0))
                    .p(px(10.0))
                    .rounded(px(4.0))
                    .border_1()
                    .border_color(palette.brass)
                    .child(
                        div()
                            .text_sm()
                            .child(format!("{} needs approval", approval.tool_name)),
                    )
                    .child(
                        div()
                            .mt(px(4.0))
                            .text_xs()
                            .text_color(palette.text_muted)
                            .child(approval.summary),
                    )
                    .child(
                        div()
                            .flex()
                            .gap(px(6.0))
                            .mt(px(8.0))
                            .child(
                                Button::new(SharedString::from(format!("approve-{approve_id}")))
                                    .label("Approve")
                                    .compact()
                                    .disabled(!has_control)
                                    .on_click(cx.listener(move |surface, _, _, cx| {
                                        surface.approve(approve_id.clone(), true, cx)
                                    })),
                            )
                            .child(
                                Button::new(SharedString::from(format!("deny-{deny_id}")))
                                    .label("Deny")
                                    .compact()
                                    .disabled(!has_control)
                                    .on_click(cx.listener(move |surface, _, _, cx| {
                                        surface.approve(deny_id.clone(), false, cx)
                                    })),
                            ),
                    )
            }))
            .child(
                div()
                    .m(px(10.0))
                    .mt(px(4.0))
                    .min_h(px(38.0))
                    .rounded(px(4.0))
                    .border_1()
                    .border_color(palette.edge_strong)
                    .bg(palette.canvas)
                    .child(
                        Input::new(&self.composer)
                            .appearance(false)
                            .bordered(false)
                            .focus_bordered(false)
                            .disabled(!has_control || has_pending_input),
                    ),
            )
    }
}

async fn attach_agent_control(
    client: &MitsuroClient,
    session_id: &str,
    client_id: &str,
) -> anyhow::Result<ControlAttachment> {
    let state = client.get_agent_control(session_id).await?;
    let may_claim = may_auto_claim_control(&state, client_id);
    if !may_claim {
        let controller = state
            .controller
            .as_ref()
            .map(|controller| controller.client_id.clone())
            .unwrap_or_else(|| "another client".to_owned());
        return Ok(ControlAttachment {
            state,
            lease: None,
            notice: Some(format!("Observer · controlled by {controller}")),
        });
    }

    match client
        .take_agent_control(
            session_id,
            &TakeAgentControlRequest {
                client_id: client_id.to_owned(),
                force: false,
            },
        )
        .await
    {
        Ok(response) => Ok(ControlAttachment {
            state: client.get_agent_control(session_id).await?,
            lease: Some(response.lease),
            notice: None,
        }),
        Err(error) => {
            // A competing client may have claimed the unowned lease between
            // our summary and automatic claim. Refresh and remain an honest
            // observer; automatic attach never force-transfers authority.
            let state = client.get_agent_control(session_id).await?;
            Ok(ControlAttachment {
                state,
                lease: None,
                notice: Some(format!(
                    "Observer · automatic control claim lost a race: {error:#}"
                )),
            })
        }
    }
}

fn may_auto_claim_control(state: &AgentControlState, client_id: &str) -> bool {
    state
        .controller
        .as_ref()
        .map(|controller| controller.client_id == client_id)
        .unwrap_or(true)
}

fn lease_controls(lease: &AgentControlLease, session_id: &str, client_id: &str) -> bool {
    lease.resource_id == session_id && lease.client_id == client_id && !lease.lease_token.is_empty()
}

/// Preserve at most one command while the initial history/control barriers
/// are unresolved. A resolved observer never retains an outbound mutation.
fn queue_outbound_while_initializing(
    pending: &mut Option<String>,
    may_eventually_control: bool,
    message: String,
) -> bool {
    if !may_eventually_control || pending.is_some() {
        return false;
    }
    *pending = Some(message);
    true
}

fn finish_pending_initial_command(
    pending: &mut Option<String>,
    control_resolved: bool,
    acquired_control: bool,
    hydrated: bool,
) -> Option<String> {
    if !control_resolved || !hydrated {
        return None;
    }
    if acquired_control {
        pending.take()
    } else {
        *pending = None;
        None
    }
}

#[derive(Default)]
struct HydratedInteractions {
    pending_approvals: Vec<PendingApproval>,
    pending_input: Option<PendingAgentInput>,
}

fn hydrated_interactions_from_snapshots(
    snapshots: &[PendingInteractionSnapshot],
) -> HydratedInteractions {
    let mut hydrated = HydratedInteractions::default();
    for snapshot in snapshots {
        match snapshot {
            PendingInteractionSnapshot::ToolApproval { tool_call } => {
                hydrated.pending_approvals.push(PendingApproval {
                    tool_call_id: tool_call.id.clone(),
                    tool_name: tool_call.name.clone(),
                    summary: tool_call
                        .arguments
                        .as_ref()
                        .map(|arguments| single_line_excerpt(&arguments.value.to_string(), 240))
                        .filter(|summary| !summary.is_empty())
                        .unwrap_or_else(|| "Review required".to_owned()),
                });
            }
            PendingInteractionSnapshot::AskUserQuestion {
                tool_call_id,
                questions,
            } if hydrated.pending_input.is_none() => {
                hydrated.pending_input = Some(PendingAgentInput::new(
                    tool_call_id.clone(),
                    questions
                        .iter()
                        .filter_map(parse_agent_question)
                        .collect::<Vec<_>>(),
                ));
            }
            PendingInteractionSnapshot::PlanConfirm {
                tool_call_id,
                title,
                task_count,
                ..
            } if hydrated.pending_input.is_none() => {
                hydrated.pending_input = Some(PendingAgentInput::plan_confirm(
                    tool_call_id.clone(),
                    title,
                    *task_count,
                ));
            }
            _ => {}
        }
    }
    hydrated
}

fn parse_agent_questions(arguments: &serde_json::Value) -> Vec<AgentQuestion> {
    arguments
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .take(3)
        .filter_map(parse_agent_question)
        .collect()
}

fn parse_agent_question(value: &serde_json::Value) -> Option<AgentQuestion> {
    let header = bounded_text(value.get("header")?.as_str()?, 40);
    let question = bounded_text(value.get("question")?.as_str()?, 500);
    if header.is_empty() || question.is_empty() {
        return None;
    }
    let options = value
        .get("options")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .take(4)
        .filter_map(|option| {
            let label = bounded_text(option.get("label")?.as_str()?, 80);
            (!label.is_empty()).then(|| AgentQuestionOption {
                label,
                description: option
                    .get("description")
                    .and_then(serde_json::Value::as_str)
                    .map(|description| bounded_text(description, 180))
                    .filter(|description| !description.is_empty()),
            })
        })
        .collect();
    let multi_select = value
        .get("multiSelect")
        .or_else(|| value.get("multi_select"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Some(AgentQuestion {
        header,
        question,
        options,
        multi_select,
    })
}

fn fallback_agent_question(tool_name: &str) -> AgentQuestion {
    AgentQuestion {
        header: "Question".to_owned(),
        question: format!(
            "{} is waiting for an answer. Type the response below.",
            bounded_text(tool_name, 80)
        ),
        options: Vec::new(),
        multi_select: false,
    }
}

fn plan_confirm_question(title: &str, task_count: usize) -> AgentQuestion {
    AgentQuestion {
        header: "Plan".to_owned(),
        question: format!(
            "Approve “{}” ({} {})?",
            bounded_text(title, 240),
            task_count,
            if task_count == 1 { "task" } else { "tasks" }
        ),
        options: vec![
            AgentQuestionOption {
                label: "Execute".to_owned(),
                description: Some("Approve and begin implementation".to_owned()),
            },
            AgentQuestionOption {
                label: "Abandon".to_owned(),
                description: Some("Discard this plan".to_owned()),
            },
        ],
        multi_select: false,
    }
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    value.trim().chars().take(max_chars).collect()
}

fn encode_agent_answers(answers: &BTreeMap<String, serde_json::Value>) -> String {
    serde_json::to_string(&serde_json::json!({ "answers": answers }))
        .unwrap_or_else(|_| r#"{"answers":{}}"#.to_owned())
}

fn encode_agent_input_response(
    kind: AgentInputResponseKind,
    answers: &BTreeMap<String, serde_json::Value>,
) -> String {
    match kind {
        AgentInputResponseKind::Questions => encode_agent_answers(answers),
        AgentInputResponseKind::PlanConfirm => {
            let choice = answers
                .get("Plan")
                .and_then(serde_json::Value::as_str)
                .map(|choice| choice.trim().to_ascii_lowercase())
                .filter(|choice| choice == "execute" || choice == "abandon")
                .unwrap_or_else(|| "abandon".to_owned());
            serde_json::to_string(&serde_json::json!({ "choice": choice }))
                .unwrap_or_else(|_| r#"{"choice":"abandon"}"#.to_owned())
        }
    }
}

fn should_end_stream(current_generation: u64, ended_generation: u64, streaming: bool) -> bool {
    streaming && current_generation == ended_generation
}

fn observer_cursor_accepts(
    current_generation: u64,
    current_sequence: u64,
    incoming_generation: u64,
    incoming_sequence: u64,
) -> bool {
    incoming_generation > current_generation
        || (incoming_generation == current_generation && incoming_sequence > current_sequence)
}

fn observed_event_marks_active(event: &ChatStreamEvent) -> bool {
    !matches!(
        event,
        ChatStreamEvent::Finish { .. }
            | ChatStreamEvent::Error { .. }
            | ChatStreamEvent::AwaitingInput { .. }
    )
}

fn agent_state_is_streaming(state: &str) -> bool {
    matches!(
        state,
        "starting" | "streaming" | "thinking" | "tool_executing" | "resuming_input"
    )
}

fn hydrated_message(id: u64, message: MessageResponse) -> Option<AgentMessage> {
    let role = match message.role.as_str() {
        "user" => AgentRole::User,
        "assistant" => AgentRole::Assistant,
        "system" | "tool" => AgentRole::Notice,
        _ => return None,
    };
    let mut markdown = content_text(&message.content);
    truncate_front(&mut markdown, MAX_MESSAGE_CHARS);
    (!markdown.is_empty()).then_some(AgentMessage { id, role, markdown })
}

fn content_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| {
                block
                    .get("text")
                    .or_else(|| block.get("content"))
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
        serde_json::Value::Object(object) => object
            .get("text")
            .or_else(|| object.get("message"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        _ => String::new(),
    }
}

fn single_line_excerpt(value: &str, max_chars: usize) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let mut excerpt = normalized
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    excerpt.push('…');
    excerpt
}

fn truncate_front(value: &mut String, max_chars: usize) {
    let count = value.chars().count();
    if count > max_chars {
        *value = value.chars().skip(count - max_chars).collect();
    }
}

fn bounded_scroll_offset(offset: f32) -> f32 {
    if offset.is_finite() {
        offset.clamp(0.0, MAX_SCROLL_OFFSET)
    } else {
        0.0
    }
}

fn scroll_offset_is_at_tail(offset: f32, max_offset: f32) -> bool {
    let offset = bounded_scroll_offset(offset);
    let max_offset = bounded_scroll_offset(max_offset);
    if max_offset <= SCROLL_TAIL_TOLERANCE {
        offset <= SCROLL_TAIL_TOLERANCE
    } else {
        offset >= max_offset - SCROLL_TAIL_TOLERANCE
    }
}

fn estimate_message_height(message: &AgentMessage) -> Pixels {
    let width_hint = 52_usize;
    let lines = message
        .markdown
        .lines()
        .map(|line| (line.chars().count() / width_hint).saturating_add(1))
        .sum::<usize>()
        .clamp(1, 80);
    px(34.0 + lines as f32 * 19.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_structured_message_text() {
        assert_eq!(
            content_text(&serde_json::json!([
                {"type": "text", "text": "one"},
                {"type": "text", "text": "two"}
            ])),
            "one\n\ntwo"
        );
    }

    #[test]
    fn excerpts_are_unicode_safe() {
        assert_eq!(single_line_excerpt("wide 猫 text", 8), "wide 猫 …");
    }

    #[test]
    fn estimated_rows_are_bounded() {
        let message = AgentMessage {
            id: 1,
            role: AgentRole::Assistant,
            markdown: "x\n".repeat(10_000),
        };
        assert_eq!(estimate_message_height(&message), px(1554.0));
    }

    #[test]
    fn ask_user_payload_is_parsed_into_bounded_typed_questions() {
        let questions = parse_agent_questions(&serde_json::json!({
            "questions": [{
                "header": "Target",
                "question": "Where should this run?",
                "options": [
                    {"label": "Staging", "description": "Safe validation"},
                    {"label": "Local"}
                ],
                "multiSelect": true
            }]
        }));
        assert_eq!(questions.len(), 1);
        assert_eq!(questions[0].header, "Target");
        assert_eq!(questions[0].options[0].label, "Staging");
        assert!(questions[0].multi_select);
    }

    #[test]
    fn typed_answers_use_the_canonical_tool_result_envelope() {
        let mut answers = BTreeMap::new();
        answers.insert(
            "Target".to_owned(),
            serde_json::Value::String("Staging".to_owned()),
        );
        assert_eq!(
            encode_agent_answers(&answers),
            r#"{"answers":{"Target":"Staging"}}"#
        );
        answers.clear();
        answers.insert(
            "Plan".to_owned(),
            serde_json::Value::String("Execute".to_owned()),
        );
        assert_eq!(
            encode_agent_input_response(AgentInputResponseKind::PlanConfirm, &answers),
            r#"{"choice":"execute"}"#
        );
    }

    #[test]
    fn hydration_restores_tool_approval_and_plan_confirmation() {
        let snapshots = vec![
            PendingInteractionSnapshot::ToolApproval {
                tool_call: mitsuro_client::RecoveryToolCall {
                    id: "tool-1".into(),
                    name: "bash".into(),
                    arguments: Some(mitsuro_client::RecoveryToolArguments {
                        value: serde_json::json!({"command": "cargo test"}),
                        redacted_paths: Vec::new(),
                    }),
                },
            },
            PendingInteractionSnapshot::PlanConfirm {
                tool_call_id: "plan-1".into(),
                title: "Ship observer support".into(),
                task_count: 3,
                tasks: Vec::new(),
            },
        ];
        let restored = hydrated_interactions_from_snapshots(&snapshots);
        assert_eq!(restored.pending_approvals.len(), 1);
        assert_eq!(restored.pending_approvals[0].tool_call_id, "tool-1");
        let plan = restored.pending_input.expect("plan confirmation");
        assert_eq!(plan.tool_call_id, "plan-1");
        assert_eq!(plan.response_kind, AgentInputResponseKind::PlanConfirm);
        assert_eq!(plan.questions[0].options[0].label, "Execute");
    }

    #[test]
    fn streamed_text_and_frame_delivery_have_explicit_bounds() {
        let mut text = "x".repeat(MAX_MESSAGE_CHARS + 500);
        truncate_front(&mut text, MAX_MESSAGE_CHARS);
        assert_eq!(text.chars().count(), MAX_MESSAGE_CHARS);
        assert_eq!(STREAM_FRAME_DELAY, Duration::from_millis(16));
        assert_eq!(EVENT_QUEUE_CAPACITY, 256);
        assert_eq!(MAX_TRANSCRIPT_CHARS, 2 * 1024 * 1024);
    }

    #[test]
    fn automatic_control_claim_never_steals_from_an_observed_client() {
        let state = AgentControlState {
            session_id: "code-1".into(),
            controller: Some(mitsuro_client::AgentControlLeaseSummary {
                client_id: "canvas-a".into(),
                expires_at: "later".into(),
            }),
            ttl_seconds: 15,
        };
        assert!(may_auto_claim_control(&state, "canvas-a"));
        assert!(!may_auto_claim_control(&state, "canvas-b"));
    }

    #[test]
    fn mutation_authority_is_bound_to_exact_session_and_client() {
        let lease = AgentControlLease {
            resource_id: "code-1".into(),
            client_id: "canvas-a".into(),
            lease_token: "opaque".into(),
            expires_at: "later".into(),
        };
        assert!(lease_controls(&lease, "code-1", "canvas-a"));
        assert!(!lease_controls(&lease, "code-2", "canvas-a"));
        assert!(!lease_controls(&lease, "code-1", "canvas-b"));
    }

    #[test]
    fn delayed_hydration_preserves_history_before_fast_control_flushes_initial_prompt() {
        let mut pending = None;
        assert!(queue_outbound_while_initializing(
            &mut pending,
            true,
            "build the feature".into()
        ));
        assert!(finish_pending_initial_command(&mut pending, true, true, false).is_none());
        assert_eq!(pending.as_deref(), Some("build the feature"));

        let mut messages = VecDeque::from([AgentMessage {
            id: 1,
            role: AgentRole::Assistant,
            markdown: "old hydrated answer".into(),
        }]);
        let outbound = finish_pending_initial_command(&mut pending, true, true, true)
            .expect("flush after both barriers");
        messages.push_back(AgentMessage {
            id: 2,
            role: AgentRole::User,
            markdown: outbound,
        });
        assert_eq!(
            messages
                .iter()
                .map(|message| message.markdown.as_str())
                .collect::<Vec<_>>(),
            vec!["old hydrated answer", "build the feature"]
        );

        assert!(!queue_outbound_while_initializing(
            &mut pending,
            false,
            "observer mutation".into()
        ));
        assert!(pending.is_none());
    }

    #[test]
    fn clean_stream_end_and_observer_events_are_generation_scoped() {
        assert!(should_end_stream(9, 9, true));
        assert!(!should_end_stream(9, 8, true));
        assert!(!should_end_stream(9, 9, false));

        assert!(observer_cursor_accepts(4, 10, 4, 11));
        assert!(observer_cursor_accepts(4, 10, 5, 1));
        assert!(!observer_cursor_accepts(4, 10, 4, 10));
        assert!(!observer_cursor_accepts(4, 10, 3, 99));
    }

    #[test]
    fn selected_tail_reads_live_text_while_background_or_scrolled_views_accumulate_unread() {
        let mut view = TranscriptViewState::default();
        view.set_selected(true);
        view.note_incoming(false);
        assert!(!view.unread, "visible tail content is already read");

        view.set_selected(false);
        view.note_incoming(false);
        assert!(view.unread, "background Agent content is unread");

        view.set_selected(true);
        assert!(!view.unread, "selecting a followed tail acknowledges it");
        view.user_scrolled(false);
        view.note_incoming(false);
        assert!(
            view.unread,
            "new content below a scrolled viewport is unread"
        );
        assert!(!view.following_tail);

        view.jump_to_latest();
        assert!(view.following_tail);
        assert!(!view.unread);
    }

    #[test]
    fn attention_events_remain_visible_even_at_the_selected_tail() {
        let mut view = TranscriptViewState::default();
        view.set_selected(true);
        view.note_incoming(true);
        assert!(view.unread);
        view.mark_read();
        assert!(!view.unread);
    }

    #[test]
    fn scroll_offsets_are_finite_bounded_and_tail_aware() {
        assert_eq!(bounded_scroll_offset(f32::NAN), 0.0);
        assert_eq!(bounded_scroll_offset(f32::INFINITY), 0.0);
        assert_eq!(bounded_scroll_offset(-12.0), 0.0);
        assert_eq!(
            bounded_scroll_offset(MAX_SCROLL_OFFSET * 2.0),
            MAX_SCROLL_OFFSET
        );
        assert!(scroll_offset_is_at_tail(0.0, 0.0));
        assert!(!scroll_offset_is_at_tail(200.0, 0.0));
        assert!(scroll_offset_is_at_tail(998.5, 1_000.0));
        assert!(!scroll_offset_is_at_tail(900.0, 1_000.0));
    }

    #[test]
    fn tool_timeline_updates_one_call_in_place_and_preserves_call_order() {
        let mut timeline = ToolTimeline::default();
        timeline.apply_event(&ChatStreamEvent::ToolCallStart {
            id: "call-a".into(),
            name: "bash".into(),
        });
        timeline.apply_event(&ChatStreamEvent::ToolCallStart {
            id: "call-b".into(),
            name: "read".into(),
        });
        timeline.apply_event(&ChatStreamEvent::ToolOutputDelta {
            id: "call-a".into(),
            delta: "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1 +1 @@\n-old\n+new".into(),
        });
        timeline.apply_event(&ChatStreamEvent::ToolResult {
            id: "call-a".into(),
            output: "diff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1 +1 @@\n-old\n+new".into(),
            is_error: false,
        });

        assert_eq!(timeline.len(), 2);
        assert_eq!(
            timeline.order,
            VecDeque::from(["call-a".into(), "call-b".into()])
        );
        let entry = timeline.get("call-a").expect("updated tool call");
        assert_eq!(entry.name, "bash");
        assert_eq!(entry.status, ToolTimelineStatus::Complete);
        assert_eq!(
            entry.detail.as_ref().map(|detail| detail.kind),
            Some(ToolDetailKind::UnifiedDiff)
        );
    }

    #[test]
    fn tool_timeline_bounds_entries_ids_outputs_and_render_work() {
        let mut timeline = ToolTimeline::default();
        for index in 0..(MAX_TOOL_TIMELINE_ENTRIES + 20) {
            timeline.apply_event(&ChatStreamEvent::ToolCallStart {
                id: format!("call-{index}"),
                name: "x".repeat(500),
            });
        }
        assert_eq!(timeline.len(), MAX_TOOL_TIMELINE_ENTRIES);
        assert!(timeline.get("call-0").is_none());

        let long_id = "identifier".repeat(100);
        timeline.apply_event(&ChatStreamEvent::ToolResult {
            id: long_id,
            output: "line\n".repeat(MAX_TOOL_OUTPUT_CHARS),
            is_error: true,
        });
        let entry = timeline.recent(1).pop().expect("bounded long entry");
        assert!(entry.id.chars().count() <= 256);
        assert!(entry.name.chars().count() <= 120);
        let detail = entry.detail.expect("bounded output");
        assert!(detail.truncated);
        assert!(detail.body.chars().count() <= MAX_TOOL_OUTPUT_CHARS);

        let (rendered, render_truncated) = bounded_rendered_tool_detail(&detail.body);
        assert!(render_truncated);
        assert!(rendered.chars().count() <= MAX_RENDERED_TOOL_DETAIL_CHARS);
        assert!(rendered.lines().count() <= MAX_RENDERED_TOOL_DETAIL_LINES);
    }

    #[test]
    fn delegated_progress_and_web_results_keep_structured_details() {
        let mut timeline = ToolTimeline::default();
        let progress = DelegatedProgressEvent {
            delegated_run_id: "run-1".into(),
            tool_call_id: "delegate-1".into(),
            kind: DelegatedToolKind::Verify,
            stage: DelegatedRunStage::Running,
            parent_session_id: "session-1".into(),
            task_id: "task-1".into(),
            agent_name: "Verifier".into(),
            status: DelegatedProgressStatus::Running,
            tool_count: 3,
            tokens: 500,
            current_action: Some("Running focused tests".into()),
            completion_summary: None,
            lines_added: 2,
            lines_removed: 1,
            completed_plan_task: None,
        };
        timeline.apply_event(&ChatStreamEvent::DelegatedProgress { payload: progress });
        timeline.apply_event(&ChatStreamEvent::WebSearchResults {
            tool_use_id: "search-1".into(),
            payload: serde_json::json!({"results": [{"title": "Primary source"}]}),
        });

        let delegated = timeline.get("delegate-1").expect("delegated call");
        assert_eq!(delegated.kind, ToolTimelineKind::Delegation);
        assert_eq!(delegated.status, ToolTimelineStatus::Running);
        assert_eq!(
            delegated.detail.as_ref().map(|detail| detail.kind),
            Some(ToolDetailKind::Json)
        );
        let search = timeline.get("search-1").expect("web search call");
        assert_eq!(search.kind, ToolTimelineKind::WebSearch);
        assert_eq!(search.status, ToolTimelineStatus::Complete);
    }

    #[test]
    fn tool_detail_markdown_uses_a_fence_that_cannot_be_closed_by_output() {
        let detail = ToolDetail {
            kind: ToolDetailKind::Text,
            body: "before ```` injected after".into(),
            truncated: false,
        };
        let markdown = rendered_tool_detail_markdown(&detail);
        assert!(markdown.starts_with("`````text\n"));
        assert!(markdown.ends_with("\n`````"));
    }
}
