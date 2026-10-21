//! Canonical conversation presentation model.

use crate::tui_v2::model::artifact::{ArtifactModel, PartId};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TurnId(String);

impl TurnId {
    pub fn from_message(message_id: &str) -> Self {
        Self(format!("turn:{message_id}"))
    }

    pub fn derived(index: usize) -> Self {
        Self(format!("turn:message:{index}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TurnState {
    #[default]
    Live,
    AwaitingInput,
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConversationPresentation {
    pub turns: Vec<ConversationTurn>,
    pub live_turn_id: Option<TurnId>,
    pub pending_interactions: Vec<PendingInteraction>,
    pub metadata: ConversationMetadata,
}

impl ConversationPresentation {
    pub fn part(&self, id: &PartId) -> Option<&TimelinePart> {
        self.turns
            .iter()
            .flat_map(|turn| &turn.parts)
            .find(|part| part.id() == id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversationTurn {
    pub id: TurnId,
    pub user: Option<UserPrompt>,
    pub parts: Vec<TimelinePart>,
    pub state: TurnState,
    pub usage: Option<UsageSnapshot>,
}

impl ConversationTurn {
    pub fn new(id: TurnId) -> Self {
        Self {
            id,
            user: None,
            parts: Vec::new(),
            state: TurnState::Live,
            usage: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserPrompt {
    pub id: PartId,
    pub text: String,
    pub attachments: Vec<AttachmentPart>,
    pub steering: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimelinePart {
    AgentText(AgentTextPart),
    Thinking(ThinkingPart),
    Tool(ToolPart),
    Notice(NoticePart),
    Attachment(AttachmentPart),
    Compaction(CompactionPart),
    Error(ErrorPart),
}

impl TimelinePart {
    pub const fn id(&self) -> &PartId {
        match self {
            Self::AgentText(part) => &part.id,
            Self::Thinking(part) => &part.id,
            Self::Tool(part) => &part.id,
            Self::Notice(part) => &part.id,
            Self::Attachment(part) => &part.id,
            Self::Compaction(part) => &part.id,
            Self::Error(part) => &part.id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentTextPart {
    pub id: PartId,
    pub text: String,
    pub citations: Vec<CitationModel>,
    pub streaming: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CitationModel {
    pub url: String,
    pub title: String,
    pub cited_text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThinkingPart {
    pub id: PartId,
    pub content: String,
    pub signature: Option<String>,
    pub streaming: bool,
    pub provider_redacted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolStatus {
    Receiving,
    Pending,
    AwaitingApproval,
    Approved,
    Running,
    Succeeded,
    Failed,
    Denied,
    Interrupted,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolArguments {
    pub fields: Vec<crate::tui_v2::model::artifact::ArtifactField>,
    pub redacted_fields: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolPart {
    pub id: PartId,
    pub tool_call_id: String,
    pub name: String,
    pub status: ToolStatus,
    pub arguments: ToolArguments,
    pub artifact: ArtifactModel,
    pub server_side: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticeLevel {
    Neutral,
    Success,
    Warning,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NoticePart {
    pub id: PartId,
    pub message: String,
    pub level: NoticeLevel,
    pub expandable: Option<ArtifactModel>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachmentKind {
    Image,
    Document,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentPart {
    pub id: PartId,
    pub kind: AttachmentKind,
    pub label: String,
    pub media_type: Option<String>,
    pub url: Option<String>,
    pub embedded: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionPart {
    pub id: PartId,
    pub reason: String,
    pub estimated_tokens_before: usize,
    pub estimated_tokens_after: Option<usize>,
    pub replaced_messages: Option<usize>,
    pub checkpoint_id: Option<String>,
    pub in_place: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorPart {
    pub id: PartId,
    pub message: String,
    pub provider_request_failure: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PendingInteraction {
    ToolApproval(PendingToolApproval),
    Questions(PendingQuestions),
    PlanConfirm(PendingPlanConfirmation),
}

impl PendingInteraction {
    pub fn tool_call_id(&self) -> &str {
        match self {
            Self::ToolApproval(value) => &value.tool_call_id,
            Self::Questions(value) => &value.tool_call_id,
            Self::PlanConfirm(value) => &value.tool_call_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingToolApproval {
    pub session_id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub arguments: ToolArguments,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingQuestions {
    pub session_id: String,
    pub tool_call_id: String,
    pub questions: Vec<QuestionModel>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionModel {
    pub header: String,
    pub question: String,
    pub options: Vec<QuestionOptionModel>,
    pub multi_select: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionOptionModel {
    pub label: String,
    pub description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingPlanConfirmation {
    pub session_id: String,
    pub tool_call_id: String,
    pub title: String,
    pub task_count: usize,
    pub tasks: Vec<PlanTaskModel>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanTaskModel {
    pub description: String,
    pub completed: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConversationMetadata {
    pub session_id: String,
    pub title: Option<String>,
    pub mode: Option<String>,
    pub workflow: Option<WorkflowRevision>,
    pub plan_tasks: Vec<PlanTaskModel>,
    pub usage: Option<UsageSnapshot>,
    pub run_budget: Option<RunBudgetSnapshot>,
    pub stop_reason: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRevision {
    pub goal_id: String,
    pub aggregate_revision: u64,
    pub operation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageSnapshot {
    pub prompt_tokens: usize,
    pub input_tokens: usize,
    pub completion_tokens: usize,
    pub reasoning_tokens: usize,
    pub cache_creation_input_tokens: usize,
    pub cache_read_input_tokens: usize,
    pub total_tokens: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunBudgetSnapshot {
    pub max_turns: Option<usize>,
    pub source: String,
}
