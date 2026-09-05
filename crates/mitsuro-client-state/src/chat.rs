use std::collections::VecDeque;

use mitsuro_client::{
    ChatRequest, ChatStreamEvent, Citation, ContentBlock, ImageSource, ModelInfo, ModelKey,
    PermissionMode, PlanItem, SessionStateResponse, SessionType, SessionWithMessages,
    ThinkingLevel, WorkMode, WorkspaceMode,
};

use crate::stored::{pending_approval_from_state, transcript_from_session};
use crate::ShellAction;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MobileSurface {
    #[default]
    Chat,
    Folder,
    Terminal,
    Browser,
}

impl MobileSurface {
    pub const ALL: [Self; 4] = [Self::Chat, Self::Folder, Self::Terminal, Self::Browser];

    pub fn label(self) -> &'static str {
        match self {
            Self::Chat => "Chat",
            Self::Folder => "Folder",
            Self::Terminal => "Terminal",
            Self::Browser => "Browser",
        }
    }

    pub fn session_type(self) -> SessionType {
        match self {
            Self::Folder => SessionType::Code,
            Self::Chat | Self::Terminal | Self::Browser => SessionType::Chat,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatControls {
    pub thinking_level: ThinkingLevel,
    pub permission_mode: PermissionMode,
    pub fast_mode: bool,
    pub work_mode: WorkMode,
    pub selected_model: Option<String>,
    pub selected_model_key: Option<ModelKey>,
}

impl Default for ChatControls {
    fn default() -> Self {
        Self {
            thinking_level: ThinkingLevel::Medium,
            permission_mode: PermissionMode::Autonomous,
            fast_mode: false,
            work_mode: WorkMode::Build,
            selected_model: None,
            selected_model_key: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceSelection {
    pub project_dir: Option<String>,
    pub workspace_mode: WorkspaceMode,
}

impl Default for WorkspaceSelection {
    fn default() -> Self {
        Self {
            project_dir: None,
            workspace_mode: WorkspaceMode::Neutral,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttachmentKind {
    Image,
    File,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentDraft {
    pub id: String,
    pub kind: AttachmentKind,
    pub name: String,
    pub uri: Option<String>,
    pub mime_type: Option<String>,
    pub base64: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatMessage {
    pub id: String,
    pub role: MessageRole,
    pub content: String,
    pub citations: Vec<Citation>,
    pub streaming: bool,
    pub attachments: Vec<AttachmentDraft>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SystemNotice {
    pub id: String,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThinkingBlock {
    pub id: String,
    pub content: String,
    pub streaming: bool,
    pub expanded: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolStatus {
    Pending,
    Running,
    Success,
    Error,
    AwaitingApproval,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolBlock {
    pub id: String,
    pub name: String,
    pub status: ToolStatus,
    pub output: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TranscriptNode {
    Message(ChatMessage),
    System(SystemNotice),
    Thinking(ThinkingBlock),
    Tool(ToolBlock),
}

impl TranscriptNode {
    pub fn id(&self) -> &str {
        match self {
            Self::Message(message) => &message.id,
            Self::System(notice) => &notice.id,
            Self::Thinking(block) => &block.id,
            Self::Tool(block) => &block.id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingToolApproval {
    pub tool_call_id: String,
    pub tool_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatState {
    pub session_id: Option<String>,
    pub surface: MobileSurface,
    pub controls: ChatControls,
    pub workspace: WorkspaceSelection,
    pub models: Vec<ModelInfo>,
    pub transcript: Vec<TranscriptNode>,
    pub attachments: Vec<AttachmentDraft>,
    pub plan_items: Vec<PlanItem>,
    pub pending_approval: Option<PendingToolApproval>,
    pub token_count: Option<usize>,
    /// Reasoning tokens from the latest usage snapshot. This is already a
    /// subset of completion tokens and must not be added to `token_count`.
    pub last_reasoning_tokens: Option<usize>,
    pub is_streaming: bool,
    pub last_error: Option<String>,
}

impl Default for ChatState {
    fn default() -> Self {
        Self {
            session_id: None,
            surface: MobileSurface::Chat,
            controls: ChatControls::default(),
            workspace: WorkspaceSelection::default(),
            models: Vec::new(),
            transcript: Vec::new(),
            attachments: Vec::new(),
            plan_items: Vec::new(),
            pending_approval: None,
            token_count: None,
            last_reasoning_tokens: None,
            is_streaming: false,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatStore {
    pub state: ChatState,
    next_id: u64,
    active_assistant_id: Option<String>,
    active_thinking_id: Option<String>,
    shell_actions: VecDeque<ShellAction>,
}

impl Default for ChatStore {
    fn default() -> Self {
        let mut store = Self {
            state: ChatState::default(),
            next_id: 1,
            active_assistant_id: None,
            active_thinking_id: None,
            shell_actions: VecDeque::new(),
        };
        store.push_system("Chat-first mobile shell ready. Hive is intentionally on hold.");
        store
    }
}

impl ChatStore {
    pub fn set_surface(&mut self, surface: MobileSurface) {
        if self.state.surface == surface {
            return;
        }
        self.state.surface = surface;
        match surface {
            MobileSurface::Terminal => self.shell_actions.push_back(ShellAction::OpenTerminal {
                session_id: self.state.session_id.clone(),
            }),
            MobileSurface::Browser => self.shell_actions.push_back(ShellAction::OpenBrowser {
                url: "http://127.0.0.1:3000".to_owned(),
            }),
            _ => {}
        }
    }

    pub fn set_models(
        &mut self,
        models: Vec<ModelInfo>,
        default_model: Option<String>,
        default_model_key: Option<ModelKey>,
    ) {
        self.state.models = models;
        let selection_is_available = self.selected_model_info().is_some();
        if !selection_is_available {
            let selected = default_model_key
                .as_ref()
                .and_then(|key| {
                    self.state
                        .models
                        .iter()
                        .find(|model| model.key.as_ref() == Some(key))
                })
                .or_else(|| {
                    default_model.as_ref().and_then(|default| {
                        self.state.models.iter().find(|model| model.id == *default)
                    })
                })
                .or_else(|| self.state.models.first());
            self.state.controls.selected_model = selected.map(|model| model.id.clone());
            self.state.controls.selected_model_key = selected.and_then(|model| model.key.clone());
        }
        self.normalize_model_controls();
    }

    pub fn select_model(&mut self, model_id: impl Into<String>) {
        let model_id = model_id.into();
        self.state.controls.selected_model_key = self
            .state
            .models
            .iter()
            .find(|model| model.id == model_id)
            .and_then(|model| model.key.clone());
        self.state.controls.selected_model = Some(model_id);
        self.normalize_model_controls();
    }

    pub fn select_model_key(&mut self, model_id: impl Into<String>, key: Option<ModelKey>) {
        self.state.controls.selected_model = Some(model_id.into());
        self.state.controls.selected_model_key = key;
        self.normalize_model_controls();
    }

    pub fn set_project_dir(&mut self, project_dir: Option<String>) {
        self.state.workspace.project_dir = project_dir;
        self.state.workspace.workspace_mode = if self.state.workspace.project_dir.is_some() {
            WorkspaceMode::Selected
        } else {
            WorkspaceMode::Neutral
        };
    }

    pub fn load_session_snapshot(&mut self, snapshot: &SessionWithMessages) {
        self.state.session_id = Some(snapshot.session.id.clone());
        self.state.controls.selected_model = snapshot.session.model.clone();
        self.state.controls.selected_model_key = snapshot.session.model_key.clone();
        self.state.controls.permission_mode = snapshot.session.permission_mode;
        self.state.controls.work_mode = snapshot.session.mode;
        self.state.workspace.project_dir = snapshot.session.project_dir.clone();
        self.state.workspace.workspace_mode = snapshot.session.workspace_mode;
        self.state.token_count = snapshot.session.token_count;
        self.state.last_reasoning_tokens = None;
        self.state.transcript = transcript_from_session(snapshot);
        self.state.pending_approval = None;
        self.state.is_streaming = false;
        self.state.last_error = None;
        self.active_assistant_id = None;
        self.active_thinking_id = None;
        self.next_id = self.state.transcript.len() as u64 + 1;
        if self.state.transcript.is_empty() {
            self.push_system(format!("Loaded empty session {}.", snapshot.session.title));
        }
    }

    pub fn apply_session_state_snapshot(&mut self, snapshot: &SessionStateResponse) {
        self.state.controls.permission_mode = snapshot.permission_mode;
        self.state.controls.work_mode = snapshot.mode;
        self.state.pending_approval = pending_approval_from_state(snapshot);
        self.state.is_streaming = matches!(
            snapshot.agent_state.as_str(),
            "streaming" | "tool_executing" | "awaiting_input"
        );
        self.state.last_error = snapshot
            .recovery
            .as_ref()
            .and_then(|recovery| recovery.last_error.clone())
            .or_else(|| self.state.last_error.clone());

        let partial = snapshot.live_partial_assistant.as_ref().or_else(|| {
            snapshot
                .recovery
                .as_ref()
                .map(|recovery| &recovery.partial_assistant)
        });
        if let Some(live) = partial {
            if !live.text.trim().is_empty() || !live.citations.is_empty() {
                self.ensure_live_partial_assistant(&live.text, &live.citations);
            }
            if let Some(thinking) = &live.thinking {
                if !thinking.trim().is_empty() {
                    self.append_thinking(thinking, self.state.is_streaming);
                }
            }
            for tool_call in &live.tool_calls {
                self.upsert_tool(
                    tool_call.id.clone(),
                    tool_call.name.clone(),
                    ToolStatus::Pending,
                    None,
                );
            }
        }
    }

    pub fn cycle_thinking(&mut self) {
        self.state.controls.thinking_level = self
            .selected_model_info()
            .map(|model| self.state.controls.thinking_level.cycle_for_model(model))
            .unwrap_or_else(|| self.state.controls.thinking_level.cycle());
    }

    pub fn supports_thinking_control(&self) -> bool {
        self.selected_model_info().is_some_and(|model| {
            model
                .selectable_thinking_levels()
                .iter()
                .any(|level| *level != ThinkingLevel::Off)
        })
    }

    pub fn toggle_permission_mode(&mut self) {
        self.state.controls.permission_mode = self.state.controls.permission_mode.toggle();
    }

    pub fn toggle_fast_mode(&mut self) {
        if self
            .selected_model_info()
            .is_some_and(|model| model.supports_fast_mode)
        {
            self.state.controls.fast_mode = !self.state.controls.fast_mode;
        } else {
            self.state.controls.fast_mode = false;
        }
    }

    pub fn toggle_work_mode(&mut self) {
        self.state.controls.work_mode = self.state.controls.work_mode.toggle();
    }

    fn selected_model_info(&self) -> Option<&ModelInfo> {
        if let Some(key) = self.state.controls.selected_model_key.as_ref() {
            return self
                .state
                .models
                .iter()
                .find(|model| model.key.as_ref() == Some(key));
        }
        let selected = self.state.controls.selected_model.as_deref()?;
        self.state.models.iter().find(|model| model.id == selected)
    }

    fn normalize_model_controls(&mut self) {
        let Some(model) = self.selected_model_info().cloned() else {
            self.state.controls.fast_mode = false;
            return;
        };
        self.state.controls.thinking_level =
            model.normalize_thinking_level(self.state.controls.thinking_level);
        self.state.controls.fast_mode &= model.supports_fast_mode;
    }

    pub fn queue_attachment_picker(&mut self) {
        self.shell_actions.push_back(ShellAction::PickAttachment);
    }

    pub fn add_attachment(&mut self, attachment: AttachmentDraft) {
        self.state.attachments.push(attachment);
    }

    pub fn remove_attachment(&mut self, id: &str) {
        self.state
            .attachments
            .retain(|attachment| attachment.id != id);
    }

    pub fn pop_shell_action(&mut self) -> Option<ShellAction> {
        self.shell_actions.pop_front()
    }

    pub fn shell_actions(&self) -> impl Iterator<Item = &ShellAction> {
        self.shell_actions.iter()
    }

    pub fn chat_request_for(&self, message: String) -> ChatRequest {
        let mut content = vec![ContentBlock::Text {
            text: message.clone(),
        }];
        for attachment in &self.state.attachments {
            if attachment.kind == AttachmentKind::Image {
                if let (Some(media_type), Some(data)) =
                    (attachment.mime_type.clone(), attachment.base64.clone())
                {
                    content.push(ContentBlock::Image {
                        source: ImageSource::Base64 { media_type, data },
                    });
                }
            }
        }

        let project_dir = self.state.workspace.project_dir.clone();
        let folder_surface = self.state.surface == MobileSurface::Folder;
        ChatRequest {
            session_id: self.state.session_id.clone(),
            message,
            content,
            project_dir: project_dir.clone(),
            working_dir: project_dir,
            workspace_mode: folder_surface.then_some(self.state.workspace.workspace_mode),
            target_branch: None,
            session_type: Some(self.state.surface.session_type()),
            model: self.state.controls.selected_model.clone(),
            model_key: self.state.controls.selected_model_key.clone(),
            thinking_enabled: self
                .state
                .controls
                .thinking_level
                .api_value()
                .map(str::to_owned),
            fast_mode: self.state.controls.fast_mode.then_some(true),
            permission_mode: Some(self.state.controls.permission_mode),
            mode: folder_surface.then_some(self.state.controls.work_mode),
            research_enabled: None,
        }
    }

    pub fn submit_user_message(&mut self, content: String) {
        self.state.last_error = None;
        self.state.pending_approval = None;
        self.active_thinking_id = None;
        let attachments = std::mem::take(&mut self.state.attachments);
        let user_id = self.next_node_id("user");
        self.state
            .transcript
            .push(TranscriptNode::Message(ChatMessage {
                id: user_id,
                role: MessageRole::User,
                content,
                citations: Vec::new(),
                streaming: false,
                attachments,
            }));

        let assistant_id = self.next_node_id("assistant");
        self.active_assistant_id = Some(assistant_id.clone());
        self.state
            .transcript
            .push(TranscriptNode::Message(ChatMessage {
                id: assistant_id,
                role: MessageRole::Assistant,
                content: String::new(),
                citations: Vec::new(),
                streaming: true,
                attachments: Vec::new(),
            }));
        self.state.is_streaming = true;
    }

    pub fn apply_stream_event(&mut self, event: ChatStreamEvent) {
        match event {
            ChatStreamEvent::TextDelta { delta } => {
                self.append_assistant_text(&delta);
            }
            ChatStreamEvent::TextDeltaWithCitations { delta, citations } => {
                self.append_assistant_text(&delta);
                if let Some(id) = self.active_assistant_id.clone() {
                    if let Some(message) = self.find_message_mut(&id) {
                        for citation in citations {
                            if !message.citations.contains(&citation) {
                                message.citations.push(citation);
                            }
                        }
                    }
                }
            }
            ChatStreamEvent::ThinkingDelta { thinking } => self.append_thinking(&thinking, true),
            ChatStreamEvent::ThinkingComplete { thinking, .. } => {
                if !thinking.is_empty() {
                    self.append_thinking(&thinking, false);
                }
                self.finish_thinking();
            }
            ChatStreamEvent::ToolCallStart { id, name } => {
                self.upsert_tool(id, name, ToolStatus::Pending, None);
            }
            ChatStreamEvent::ToolCallComplete { id, name, .. } => {
                self.upsert_tool(id, name, ToolStatus::Pending, None);
            }
            ChatStreamEvent::ToolExecuting { id, name } => {
                self.upsert_tool(id, name, ToolStatus::Running, None);
            }
            ChatStreamEvent::ToolOutputDelta { id, delta } => {
                self.upsert_tool(id, "tool".to_owned(), ToolStatus::Running, Some(delta));
            }
            ChatStreamEvent::ToolResult {
                id,
                output,
                is_error,
            } => {
                let status = if is_error {
                    ToolStatus::Error
                } else {
                    ToolStatus::Success
                };
                self.upsert_tool(id, "tool".to_owned(), status, Some(output));
            }
            ChatStreamEvent::ServerToolStart { id, name } => {
                self.upsert_tool(id, name, ToolStatus::Running, None);
            }
            ChatStreamEvent::ServerToolComplete { id, name } => {
                self.upsert_tool(id, name, ToolStatus::Success, None);
            }
            ChatStreamEvent::ServerToolError {
                tool_use_id,
                error_code,
            } => {
                self.upsert_tool(
                    tool_use_id,
                    "server tool".to_owned(),
                    ToolStatus::Error,
                    Some(error_code),
                );
            }
            ChatStreamEvent::AwaitingInput {
                tool_call_id,
                tool_name,
            } => {
                self.state.pending_approval = Some(PendingToolApproval {
                    tool_call_id: tool_call_id.clone(),
                    tool_name: tool_name.clone(),
                });
                self.upsert_tool(tool_call_id, tool_name, ToolStatus::AwaitingApproval, None);
            }
            ChatStreamEvent::PlanUpdate { items } => self.state.plan_items = items,
            ChatStreamEvent::PlanComplete {
                title, task_count, ..
            } => self.push_system(format!("Plan ready: {title} ({task_count} tasks).")),
            ChatStreamEvent::ModeChange { mode, reason } => {
                let suffix = reason.map(|value| format!(": {value}")).unwrap_or_default();
                self.push_system(format!("Mode changed to {mode}{suffix}."));
            }
            ChatStreamEvent::ToolApprovalRequired { id, name, .. } => {
                self.state.pending_approval = Some(PendingToolApproval {
                    tool_call_id: id.clone(),
                    tool_name: name.clone(),
                });
                self.upsert_tool(id, name, ToolStatus::AwaitingApproval, None);
            }
            ChatStreamEvent::ToolApproved { id } => {
                self.state.pending_approval = None;
                self.upsert_tool(id, "tool".to_owned(), ToolStatus::Running, None);
            }
            ChatStreamEvent::ToolDenied { id } => {
                self.state.pending_approval = None;
                self.upsert_tool(
                    id,
                    "tool".to_owned(),
                    ToolStatus::Error,
                    Some("Denied".to_owned()),
                );
            }
            ChatStreamEvent::SteeringInjected {
                pending_id,
                message,
            } => self.push_live_steering(pending_id, message),
            ChatStreamEvent::TitleUpdate { title } => {
                self.push_system(format!("Session title: {title}"));
            }
            ChatStreamEvent::Usage {
                prompt_tokens,
                input_tokens,
                completion_tokens,
                reasoning_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                total_tokens,
            } => {
                self.state.token_count = Some(if total_tokens > 0 {
                    total_tokens
                } else if input_tokens > 0 {
                    input_tokens + completion_tokens
                } else {
                    prompt_tokens
                        + completion_tokens
                        + cache_creation_input_tokens
                        + cache_read_input_tokens
                });
                self.state.last_reasoning_tokens = Some(reasoning_tokens);
            }
            ChatStreamEvent::Lagged { skipped } => {
                self.push_system(format!("Stream skipped {skipped} non-critical events."));
            }
            ChatStreamEvent::SessionPinched {
                new_session_id,
                reason,
                ..
            } => {
                self.state.session_id = Some(new_session_id);
                self.push_system(format!("Session compacted/continued: {reason}"));
            }
            ChatStreamEvent::Finish { session_id, .. } => {
                self.state.session_id = Some(session_id);
                self.finish_stream();
            }
            ChatStreamEvent::Error { error } => self.fail_stream(error),
            ChatStreamEvent::UserMessage {
                title,
                message,
                level,
            } => {
                let title = title.unwrap_or_else(|| level.to_uppercase());
                self.push_system(format!("{title}: {message}"));
            }
            ChatStreamEvent::AgentSleeping { reason, .. } => {
                self.push_system(format!("Agent sleeping: {reason}"));
            }
            ChatStreamEvent::WebSearchResults { tool_use_id, .. } => {
                self.upsert_tool(
                    tool_use_id,
                    "web_search".to_owned(),
                    ToolStatus::Success,
                    None,
                );
            }
            ChatStreamEvent::WebFetchResult { tool_use_id, .. } => {
                self.upsert_tool(
                    tool_use_id,
                    "web_fetch".to_owned(),
                    ToolStatus::Success,
                    None,
                );
            }
            ChatStreamEvent::ContextCompactionStarted { reason } => {
                self.push_system(format!("Compaction started: {reason}"));
            }
            ChatStreamEvent::TurnComplete { has_more: true, .. } => {
                self.settle_assistant_turn();
            }
            ChatStreamEvent::ContextCompacted { .. }
            | ChatStreamEvent::DelegatedProgress { .. }
            | ChatStreamEvent::DelegationEvent { .. }
            | ChatStreamEvent::TurnComplete {
                has_more: false, ..
            }
            | ChatStreamEvent::TickInjected { .. }
            | ChatStreamEvent::AgentBackgroundStarted { .. }
            | ChatStreamEvent::AgentBackgroundCompleted { .. }
            | ChatStreamEvent::ClassifierDecision { .. }
            | ChatStreamEvent::Other { .. } => {}
        }
    }

    pub fn fail_stream(&mut self, error: String) {
        self.state.last_error = Some(error.clone());
        self.append_assistant_text(&format!("\n\nError: {error}"));
        self.finish_stream();
    }

    pub fn finish_stream(&mut self) {
        self.state.is_streaming = false;
        self.finish_thinking();
        if let Some(active_id) = self.active_assistant_id.take() {
            if let Some(message) = self.find_message_mut(&active_id) {
                if message.content.trim().is_empty() && message.citations.is_empty() {
                    message.content = "Turn completed without assistant text.".to_owned();
                }
                message.streaming = false;
            }
        }
    }

    pub fn push_system(&mut self, content: impl Into<String>) {
        let id = self.next_node_id("system");
        self.state
            .transcript
            .push(TranscriptNode::System(SystemNotice {
                id,
                content: content.into(),
            }));
    }

    /// Close the provider turn without ending the run or inventing answer text.
    /// The next text/citation event allocates its own assistant node lazily.
    fn settle_assistant_turn(&mut self) {
        self.finish_thinking();
        if let Some(active_id) = self.active_assistant_id.take() {
            let remove_empty = self
                .find_message_mut(&active_id)
                .map(|active| {
                    active.streaming = false;
                    active.content.trim().is_empty()
                        && active.attachments.is_empty()
                        && active.citations.is_empty()
                })
                .unwrap_or(false);
            if remove_empty {
                self.state.transcript.retain(|node| node.id() != active_id);
            }
        }
    }

    fn push_live_steering(&mut self, pending_id: Option<String>, message: String) {
        self.settle_assistant_turn();
        let id = pending_id
            .map(|id| format!("user-steering-{id}"))
            .unwrap_or_else(|| self.next_node_id("user-steering"));
        if self.state.transcript.iter().any(|node| node.id() == id) {
            return;
        }
        self.state
            .transcript
            .push(TranscriptNode::Message(ChatMessage {
                id,
                role: MessageRole::User,
                content: message,
                citations: Vec::new(),
                streaming: false,
                attachments: Vec::new(),
            }));
    }

    fn append_assistant_text(&mut self, delta: &str) {
        let assistant_id = self.ensure_active_assistant();
        if let Some(message) = self.find_message_mut(&assistant_id) {
            message.content.push_str(delta);
        }
    }

    fn append_thinking(&mut self, delta: &str, streaming: bool) {
        let thinking_id = match &self.active_thinking_id {
            Some(id) => id.clone(),
            None => {
                let id = self.next_node_id("thinking");
                self.active_thinking_id = Some(id.clone());
                self.state
                    .transcript
                    .push(TranscriptNode::Thinking(ThinkingBlock {
                        id: id.clone(),
                        content: String::new(),
                        streaming,
                        expanded: false,
                    }));
                id
            }
        };
        if let Some(block) = self.find_thinking_mut(&thinking_id) {
            block.content.push_str(delta);
            block.streaming = streaming;
        }
    }

    fn finish_thinking(&mut self) {
        if let Some(id) = self.active_thinking_id.take() {
            if let Some(block) = self.find_thinking_mut(&id) {
                block.streaming = false;
            }
        }
    }

    fn upsert_tool(
        &mut self,
        id: String,
        name: String,
        status: ToolStatus,
        output_delta: Option<String>,
    ) {
        for node in &mut self.state.transcript {
            if let TranscriptNode::Tool(block) = node {
                if block.id == id {
                    let incoming_is_placeholder = name == "tool" || name == "server tool";
                    let existing_is_placeholder =
                        block.name == "tool" || block.name == "server tool";
                    if !name.is_empty() && (!incoming_is_placeholder || existing_is_placeholder) {
                        block.name = name;
                    }
                    block.status = status;
                    if let Some(delta) = output_delta {
                        block.output.push_str(&delta);
                    }
                    return;
                }
            }
        }

        self.state.transcript.push(TranscriptNode::Tool(ToolBlock {
            id,
            name,
            status,
            output: output_delta.unwrap_or_default(),
        }));
    }

    fn ensure_live_partial_assistant(&mut self, text: &str, citations: &[Citation]) {
        let id = self
            .active_assistant_id
            .clone()
            .unwrap_or_else(|| self.next_node_id("live-assistant"));
        let streaming = self.state.is_streaming;
        self.active_assistant_id = Some(id.clone());
        if let Some(message) = self.find_message_mut(&id) {
            message.content = text.to_owned();
            message.citations = citations.to_vec();
            message.streaming = streaming;
            return;
        }
        self.state
            .transcript
            .push(TranscriptNode::Message(ChatMessage {
                id,
                role: MessageRole::Assistant,
                content: text.to_owned(),
                citations: citations.to_vec(),
                streaming,
                attachments: Vec::new(),
            }));
    }

    fn ensure_active_assistant(&mut self) -> String {
        if let Some(id) = &self.active_assistant_id {
            return id.clone();
        }
        let id = self.next_node_id("assistant");
        self.active_assistant_id = Some(id.clone());
        self.state
            .transcript
            .push(TranscriptNode::Message(ChatMessage {
                id: id.clone(),
                role: MessageRole::Assistant,
                content: String::new(),
                citations: Vec::new(),
                streaming: true,
                attachments: Vec::new(),
            }));
        id
    }

    fn find_message_mut(&mut self, id: &str) -> Option<&mut ChatMessage> {
        self.state
            .transcript
            .iter_mut()
            .find_map(|node| match node {
                TranscriptNode::Message(message) if message.id == id => Some(message),
                _ => None,
            })
    }

    fn find_thinking_mut(&mut self, id: &str) -> Option<&mut ThinkingBlock> {
        self.state
            .transcript
            .iter_mut()
            .find_map(|node| match node {
                TranscriptNode::Thinking(block) if block.id == id => Some(block),
                _ => None,
            })
    }

    fn next_node_id(&mut self, prefix: &str) -> String {
        let id = format!("{prefix}-{}", self.next_id);
        self.next_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model(
        id: &str,
        levels: Vec<mitsuro_client::ReasoningEffort>,
        default: Option<mitsuro_client::ReasoningEffort>,
        mandatory: bool,
        supports_fast_mode: bool,
    ) -> ModelInfo {
        ModelInfo {
            key: None,
            id: id.to_owned(),
            display_name: id.to_owned(),
            provider: "test".to_owned(),
            context_window: 0,
            max_output: 0,
            supports_thinking: !levels.is_empty() || default.is_some(),
            reasoning_control: Some(mitsuro_client::ReasoningControl::OpenAiEffort),
            supported_reasoning_levels: levels,
            default_reasoning_level: default,
            reasoning_is_mandatory: mandatory,
            supports_fast_mode,
            fast_mode: supports_fast_mode.then_some(mitsuro_client::FastMode::Priority),
            supports_tools: false,
            supports_vision: false,
        }
    }

    fn text_messages(store: &ChatStore) -> Vec<String> {
        store
            .state
            .transcript
            .iter()
            .filter_map(|node| match node {
                TranscriptNode::Message(message) => Some(message.content.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn submit_adds_user_and_streaming_assistant() {
        let mut store = ChatStore::default();
        store.submit_user_message("hello".to_owned());

        assert!(store.state.is_streaming);
        assert_eq!(
            text_messages(&store),
            vec!["hello".to_owned(), String::new()]
        );
    }

    #[test]
    fn stream_text_delta_updates_active_assistant() {
        let mut store = ChatStore::default();
        store.submit_user_message("hello".to_owned());
        store.apply_stream_event(ChatStreamEvent::TextDelta {
            delta: "hi".to_owned(),
        });
        store.apply_stream_event(ChatStreamEvent::TextDelta {
            delta: " there".to_owned(),
        });

        assert_eq!(
            text_messages(&store).last().map(String::as_str),
            Some("hi there")
        );
    }

    #[test]
    fn stream_citations_and_annotation_only_updates_remain_on_the_assistant() {
        let mut store = ChatStore::default();
        store.submit_user_message("Find the source".to_owned());
        store.apply_stream_event(ChatStreamEvent::TextDelta {
            delta: "Sourced answer".to_owned(),
        });
        let cited = serde_json::json!({
            "type": "text_delta_with_citations", "delta": "",
            "citations": [{ "url": "https://example.test/source", "title": "Source", "cited_text": "Sourced answer" }, null]
        });
        store.apply_stream_event(ChatStreamEvent::from_json_value(cited.clone()));
        store.apply_stream_event(ChatStreamEvent::from_json_value(cited));
        store.finish_stream();
        let assistant = store
            .state
            .transcript
            .iter()
            .find_map(|node| match node {
                TranscriptNode::Message(message) if message.role == MessageRole::Assistant => {
                    Some(message)
                }
                _ => None,
            })
            .expect("assistant with source annotations");
        assert_eq!(assistant.content, "Sourced answer");
        assert_eq!(assistant.citations.len(), 1);
        assert_eq!(assistant.citations[0].url, "https://example.test/source");
        assert_eq!(assistant.citations[0].cited_text, "Sourced answer");
    }

    #[test]
    fn usage_keeps_reasoning_observable_without_double_counting() {
        let mut store = ChatStore::default();
        store.apply_stream_event(ChatStreamEvent::Usage {
            prompt_tokens: 1_000,
            input_tokens: 1_000,
            completion_tokens: 550,
            reasoning_tokens: 500,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            total_tokens: 1_550,
        });

        assert_eq!(store.state.token_count, Some(1_550));
        assert_eq!(store.state.last_reasoning_tokens, Some(500));
    }

    #[test]
    fn finish_sets_session_and_stops_streaming() {
        let mut store = ChatStore::default();
        store.submit_user_message("hello".to_owned());
        store.apply_stream_event(ChatStreamEvent::Finish {
            session_id: "s1".to_owned(),
            stop_reason: "done".to_owned(),
        });

        assert_eq!(store.state.session_id.as_deref(), Some("s1"));
        assert!(!store.state.is_streaming);
    }

    #[test]
    fn tool_approval_is_reloadable_state() {
        let mut store = ChatStore::default();
        store.apply_stream_event(ChatStreamEvent::ToolApprovalRequired {
            id: "tool-1".to_owned(),
            name: "bash".to_owned(),
            arguments: serde_json::json!({}),
        });

        assert_eq!(
            store.state.pending_approval,
            Some(PendingToolApproval {
                tool_call_id: "tool-1".to_owned(),
                tool_name: "bash".to_owned(),
            })
        );
    }

    #[test]
    fn request_includes_accordion_controls() {
        let mut store = ChatStore::default();
        store.state.controls.fast_mode = true;
        store.state.controls.thinking_level = ThinkingLevel::High;
        store.state.controls.permission_mode = PermissionMode::Supervised;
        store.state.controls.selected_model = Some("model-a".to_owned());
        store.set_surface(MobileSurface::Chat);

        let request = store.chat_request_for("research this".to_owned());

        assert_eq!(request.model.as_deref(), Some("model-a"));
        assert_eq!(request.thinking_enabled.as_deref(), Some("high"));
        assert_eq!(request.fast_mode, Some(true));
        assert_eq!(request.permission_mode, Some(PermissionMode::Supervised));
        assert_eq!(request.session_type, Some(SessionType::Chat));
        assert_eq!(request.mode, None);
        assert_eq!(request.research_enabled, None);
    }

    #[test]
    fn model_refresh_replaces_a_removed_selection_and_normalizes_controls() {
        let mut store = ChatStore::default();
        store.state.controls.selected_model = Some("removed-model".to_owned());
        store.state.controls.thinking_level = ThinkingLevel::Off;
        store.state.controls.fast_mode = true;

        store.set_models(
            vec![model(
                "current-model",
                vec![
                    mitsuro_client::ReasoningEffort::Low,
                    mitsuro_client::ReasoningEffort::High,
                ],
                Some(mitsuro_client::ReasoningEffort::High),
                true,
                false,
            )],
            Some("current-model".to_owned()),
            None,
        );

        assert_eq!(
            store.state.controls.selected_model.as_deref(),
            Some("current-model")
        );
        assert_eq!(store.state.controls.thinking_level, ThinkingLevel::High);
        assert!(!store.state.controls.fast_mode);
    }

    #[test]
    fn output_only_reasoning_is_not_exposed_as_a_mobile_control() {
        let mut output_only = model("grok-build", Vec::new(), None, false, false);
        output_only.supports_thinking = true;
        output_only.reasoning_control = Some(mitsuro_client::ReasoningControl::OutputOnly);
        let mut store = ChatStore::default();
        store.state.controls.thinking_level = ThinkingLevel::High;

        store.set_models(vec![output_only], Some("grok-build".to_owned()), None);

        assert_eq!(store.state.controls.thinking_level, ThinkingLevel::Off);
        assert!(!store.supports_thinking_control());
    }

    #[test]
    fn load_session_snapshot_replaces_transcript_and_controls() {
        let mut store = ChatStore::default();
        let snapshot = mitsuro_client::SessionWithMessages {
            session: mitsuro_client::SessionInfo {
                id: "s1".to_owned(),
                title: "Loaded".to_owned(),
                updated_at: String::new(),
                token_count: Some(42),
                parent_session_id: None,
                working_dir: None,
                project_dir: Some("/tmp/project".to_owned()),
                workspace_mode: WorkspaceMode::Selected,
                session_type: SessionType::Code,
                mode: WorkMode::Plan,
                model: Some("model-a".to_owned()),
                model_key: None,
                model_catalog_revision: None,
                target_branch: None,
                permission_mode: PermissionMode::Supervised,
            },
            messages: vec![mitsuro_client::MessageResponse {
                role: "assistant".to_owned(),
                content: serde_json::json!([{ "type": "text", "text": "stored" }]),
            }],
        };

        store.load_session_snapshot(&snapshot);

        assert_eq!(store.state.session_id.as_deref(), Some("s1"));
        assert_eq!(store.state.controls.work_mode, WorkMode::Plan);
        assert_eq!(
            store.state.controls.permission_mode,
            PermissionMode::Supervised
        );
        assert_eq!(
            store.state.workspace.project_dir.as_deref(),
            Some("/tmp/project")
        );
        assert_eq!(text_messages(&store), vec!["stored".to_owned()]);
    }

    #[test]
    fn session_state_snapshot_restores_pending_approval() {
        let mut store = ChatStore::default();
        let snapshot = mitsuro_client::SessionStateResponse {
            id: "s1".to_owned(),
            agent_state: "awaiting_input".to_owned(),
            started_at: None,
            last_event_at: None,
            mode: WorkMode::Build,
            permission_mode: PermissionMode::Supervised,
            recovery: None,
            pending_interactions: vec![mitsuro_client::PendingInteractionSnapshot::ToolApproval {
                tool_call: mitsuro_client::RecoveryToolCall {
                    id: "tool-1".to_owned(),
                    name: "bash".to_owned(),
                    arguments: None,
                },
            }],
            live_partial_assistant: Some(mitsuro_client::PartialAssistantState {
                text: "partial".to_owned(),
                citations: Vec::new(),
                thinking: None,
                tool_calls: Vec::new(),
            }),
            delegated_tools: Vec::new(),
            recent_delegated_runs: Vec::new(),
            delegated_run_summaries: Vec::new(),
            delegation_groups: Vec::new(),
            delegation_events: Vec::new(),
            delegation_event_cursor: None,
            last_event_sequence: Some(7),
            agent_stream_generation: 0,
            agent_stream_sequence: 0,
        };

        store.apply_session_state_snapshot(&snapshot);

        assert!(store.state.is_streaming);
        assert_eq!(
            store
                .state
                .pending_approval
                .as_ref()
                .map(|approval| approval.tool_name.as_str()),
            Some("bash")
        );
        assert_eq!(
            text_messages(&store).last().map(String::as_str),
            Some("partial")
        );
    }
}
