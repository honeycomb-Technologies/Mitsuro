//! Canonical conversation projection.

mod live;
mod persisted;
mod recovery;
pub mod tool_output;

use std::collections::HashMap;

use crate::tui_v2::model::{
    artifact::{ArtifactModel, PartId},
    conversation::{
        AgentTextPart, AttachmentPart, CitationModel, ConversationPresentation, ConversationTurn,
        ThinkingPart, TimelinePart, ToolArguments, ToolPart, ToolStatus, TurnId, TurnState,
        UserPrompt,
    },
};

#[cfg(test)]
pub use persisted::PersistedMessage;

#[cfg(test)]
mod tests;

/// Stateful projection boundary shared by persisted replay, recovery, and live
/// loop events. The indexes are caches only; `presentation` remains canonical.
pub struct ConversationProjection {
    session_id: String,
    presentation: ConversationPresentation,
    current_turn: Option<usize>,
    active_text: Option<(usize, usize)>,
    active_thinking: Option<(usize, usize)>,
    tool_locations: HashMap<String, (usize, usize)>,
}

impl ConversationProjection {
    pub fn new(session_id: impl Into<String>) -> Self {
        let session_id = session_id.into();
        Self {
            presentation: ConversationPresentation {
                metadata: crate::tui_v2::model::conversation::ConversationMetadata {
                    session_id: session_id.clone(),
                    ..Default::default()
                },
                ..Default::default()
            },
            session_id,
            current_turn: None,
            active_text: None,
            active_thinking: None,
            tool_locations: HashMap::new(),
        }
    }

    pub fn from_model_messages(
        session_id: impl Into<String>,
        messages: &[mitsuro_core::ai::types::ModelMessage],
    ) -> Self {
        persisted::project_model_messages(session_id.into(), messages)
    }

    #[cfg(test)]
    pub fn from_persisted(session_id: impl Into<String>, messages: &[PersistedMessage]) -> Self {
        persisted::project_persisted_messages(session_id.into(), messages)
    }

    pub const fn presentation(&self) -> &ConversationPresentation {
        &self.presentation
    }

    pub fn set_title(&mut self, title: Option<String>) {
        self.presentation.metadata.title = title;
    }

    /// Live sub-agent / explore-build progress → parent agent tool stream panel.
    pub fn apply_delegated_progress(
        &mut self,
        event: &mitsuro_core::agent::DelegatedProgressEvent,
    ) {
        live::apply_delegated_progress(self, event);
    }

    /// Rebuild delegated agent cards from the canonical session-level ledger.
    pub fn restore_delegation_groups(
        &mut self,
        groups: &[mitsuro_core::storage::DelegationGroupRecord],
    ) {
        live::restore_delegation_groups(self, groups);
    }

    /// Restore context-window chrome after session open from durable token_count.
    pub fn set_usage_from_token_count(&mut self, token_count: Option<usize>) {
        let Some(tokens) = token_count.filter(|n| *n > 0) else {
            self.presentation.metadata.usage = None;
            return;
        };
        self.presentation.metadata.usage =
            Some(crate::tui_v2::model::conversation::UsageSnapshot {
                prompt_tokens: tokens,
                input_tokens: tokens,
                completion_tokens: 0,
                reasoning_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                total_tokens: tokens,
            });
    }

    pub fn push_user_prompt(
        &mut self,
        message_id: &str,
        text: String,
        attachments: Vec<AttachmentPart>,
        steering: bool,
    ) -> TurnId {
        if let Some(index) = self.current_turn {
            if matches!(
                self.presentation.turns[index].state,
                TurnState::Live | TurnState::AwaitingInput
            ) {
                self.presentation.turns[index].state = TurnState::Completed;
            }
        }

        let turn_id = TurnId::from_message(message_id);
        if self
            .presentation
            .turns
            .iter()
            .any(|turn| turn.id == turn_id)
        {
            return turn_id;
        }
        let mut turn = ConversationTurn::new(turn_id.clone());
        turn.user = Some(UserPrompt {
            id: PartId::from_semantic(format!("{}/user", turn_id.as_str())),
            text,
            attachments,
            steering,
        });
        self.presentation.turns.push(turn);
        self.current_turn = Some(self.presentation.turns.len() - 1);
        self.presentation.live_turn_id = Some(turn_id.clone());
        self.active_text = None;
        self.active_thinking = None;
        turn_id
    }

    fn ensure_turn(&mut self) -> usize {
        if let Some(index) = self.current_turn {
            return index;
        }

        let index = self.presentation.turns.len();
        let id = TurnId::derived(index);
        self.presentation
            .turns
            .push(ConversationTurn::new(id.clone()));
        self.presentation.live_turn_id = Some(id);
        self.current_turn = Some(index);
        index
    }

    fn append_agent_text(&mut self, delta: &str, citations: Vec<CitationModel>) {
        let turn_index = self.ensure_turn();
        if let Some((active_turn, part_index)) = self.active_text {
            if active_turn == turn_index {
                if let TimelinePart::AgentText(part) =
                    &mut self.presentation.turns[turn_index].parts[part_index]
                {
                    part.text.push_str(delta);
                    merge_citations(&mut part.citations, citations);
                    return;
                }
            }
        }

        self.active_thinking = None;
        let ordinal = self.presentation.turns[turn_index]
            .parts
            .iter()
            .filter(|part| matches!(part, TimelinePart::AgentText(_)))
            .count();
        let part = TimelinePart::AgentText(AgentTextPart {
            id: PartId::scoped(
                self.presentation.turns[turn_index].id.as_str(),
                "agent",
                ordinal,
            ),
            text: delta.to_owned(),
            citations,
            streaming: true,
        });
        let part_index = self.presentation.turns[turn_index].parts.len();
        self.presentation.turns[turn_index].parts.push(part);
        self.active_text = Some((turn_index, part_index));
    }

    fn append_thinking(&mut self, delta: &str) {
        let turn_index = self.ensure_turn();
        if let Some((active_turn, part_index)) = self.active_thinking {
            if active_turn == turn_index {
                if let TimelinePart::Thinking(part) =
                    &mut self.presentation.turns[turn_index].parts[part_index]
                {
                    part.content.push_str(delta);
                    return;
                }
            }
        }

        self.active_text = None;
        let ordinal = self.presentation.turns[turn_index]
            .parts
            .iter()
            .filter(|part| matches!(part, TimelinePart::Thinking(_)))
            .count();
        let part = TimelinePart::Thinking(ThinkingPart {
            id: PartId::scoped(
                self.presentation.turns[turn_index].id.as_str(),
                "thinking",
                ordinal,
            ),
            content: delta.to_owned(),
            signature: None,
            streaming: true,
            provider_redacted: false,
        });
        let part_index = self.presentation.turns[turn_index].parts.len();
        self.presentation.turns[turn_index].parts.push(part);
        self.active_thinking = Some((turn_index, part_index));
    }

    fn complete_thinking(&mut self, content: &str, signature: Option<String>) {
        if self.active_thinking.is_none() {
            self.append_thinking(content);
        }
        if let Some((turn_index, part_index)) = self.active_thinking {
            if let TimelinePart::Thinking(part) =
                &mut self.presentation.turns[turn_index].parts[part_index]
            {
                if !content.is_empty() {
                    part.content = content.to_owned();
                }
                part.signature = signature;
                part.streaming = false;
            }
        }
        self.active_thinking = None;
    }

    fn push_redacted_thinking(&mut self) {
        self.active_text = None;
        self.active_thinking = None;
        let turn_index = self.ensure_turn();
        let ordinal = self.presentation.turns[turn_index]
            .parts
            .iter()
            .filter(|part| matches!(part, TimelinePart::Thinking(_)))
            .count();
        let turn_id = self.presentation.turns[turn_index].id.clone();
        self.presentation.turns[turn_index]
            .parts
            .push(TimelinePart::Thinking(ThinkingPart {
                id: PartId::scoped(turn_id.as_str(), "thinking", ordinal),
                content: "Provider-redacted reasoning".to_owned(),
                signature: None,
                streaming: false,
                provider_redacted: true,
            }));
    }

    fn upsert_tool(
        &mut self,
        tool_call_id: &str,
        name: &str,
        status: ToolStatus,
        server_side: bool,
    ) -> &mut ToolPart {
        if !self.active_text_ends_open_token() {
            self.active_text = None;
        }
        self.active_thinking = None;

        if let Some((turn_index, part_index)) = self.tool_locations.get(tool_call_id).copied() {
            let TimelinePart::Tool(tool) =
                &mut self.presentation.turns[turn_index].parts[part_index]
            else {
                unreachable!("tool index must address a tool part");
            };
            if !name.is_empty() {
                tool.name = name.to_owned();
            }
            tool.server_side |= server_side;
            tool.status = status;
            return tool;
        }

        let turn_index = self.ensure_turn();
        let part_index = self.presentation.turns[turn_index].parts.len();
        self.presentation.turns[turn_index]
            .parts
            .push(TimelinePart::Tool(ToolPart {
                id: PartId::from_semantic(format!("tool:{tool_call_id}")),
                tool_call_id: tool_call_id.to_owned(),
                name: name.to_owned(),
                status,
                arguments: ToolArguments::default(),
                artifact: ArtifactModel::default(),
                server_side,
            }));
        self.tool_locations
            .insert(tool_call_id.to_owned(), (turn_index, part_index));
        let TimelinePart::Tool(tool) = &mut self.presentation.turns[turn_index].parts[part_index]
        else {
            unreachable!();
        };
        tool
    }

    fn active_text_ends_open_token(&self) -> bool {
        let Some((turn_index, part_index)) = self.active_text else {
            return false;
        };
        let Some(TimelinePart::AgentText(part)) = self
            .presentation
            .turns
            .get(turn_index)
            .and_then(|turn| turn.parts.get(part_index))
        else {
            return false;
        };
        part.text.chars().next_back().is_some_and(|character| {
            !character.is_whitespace()
                && !matches!(
                    character,
                    '.' | ',' | '!' | '?' | ':' | ';' | ')' | ']' | '}' | '"' | '\'' | '`' | '…'
                )
        })
    }

    fn tool_mut(&mut self, tool_call_id: &str) -> Option<&mut ToolPart> {
        let (turn_index, part_index) = self.tool_locations.get(tool_call_id).copied()?;
        match &mut self.presentation.turns[turn_index].parts[part_index] {
            TimelinePart::Tool(tool) => Some(tool),
            _ => None,
        }
    }

    fn current_turn_mut(&mut self) -> &mut ConversationTurn {
        let index = self.ensure_turn();
        &mut self.presentation.turns[index]
    }

    fn push_part(&mut self, part: TimelinePart) {
        self.active_text = None;
        self.active_thinking = None;
        self.current_turn_mut().parts.push(part);
    }

    fn settle_streaming_parts(&mut self, status: ToolStatus) {
        for turn in &mut self.presentation.turns {
            for part in &mut turn.parts {
                match part {
                    TimelinePart::AgentText(part) => part.streaming = false,
                    TimelinePart::Thinking(part) => part.streaming = false,
                    TimelinePart::Tool(part)
                        if matches!(
                            part.status,
                            ToolStatus::Receiving
                                | ToolStatus::Pending
                                | ToolStatus::Approved
                                | ToolStatus::Running
                        ) =>
                    {
                        // Keep AskUser / plan-confirm rows in Pending while the
                        // decision dock is open. Mapping them to AwaitingApproval
                        // made the row say "approval" and looked like a different
                        // interaction entirely.
                        let interactive = matches!(
                            part.name.to_ascii_lowercase().as_str(),
                            "askuserquestion" | "planconfirm" | "plan_confirm"
                        );
                        if interactive
                            && matches!(part.status, ToolStatus::Pending | ToolStatus::Receiving)
                            && matches!(
                                status,
                                ToolStatus::AwaitingApproval | ToolStatus::Interrupted
                            )
                        {
                            part.status = ToolStatus::Pending;
                            continue;
                        }
                        part.status = status;
                    }
                    _ => {}
                }
            }
        }
        self.active_text = None;
        self.active_thinking = None;
    }

    fn set_pending_interaction(
        &mut self,
        interaction: crate::tui_v2::model::conversation::PendingInteraction,
    ) {
        let tool_call_id = interaction.tool_call_id().to_owned();
        if let Some(index) = self
            .presentation
            .pending_interactions
            .iter()
            .position(|pending| pending.tool_call_id() == tool_call_id)
        {
            self.presentation.pending_interactions[index] = interaction;
        } else {
            self.presentation.pending_interactions.push(interaction);
        }
    }
}

fn merge_citations(existing: &mut Vec<CitationModel>, incoming: Vec<CitationModel>) {
    for citation in incoming {
        if !existing.contains(&citation) {
            existing.push(citation);
        }
    }
}
