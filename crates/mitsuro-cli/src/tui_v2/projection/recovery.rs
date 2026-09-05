//! Recovery projection and stable live/persisted deduplication.

use mitsuro_core::storage::{PendingInteractionSnapshot, RecoveryStatus, SessionRecoveryState};

use crate::tui_v2::model::{
    artifact::{ArtifactContent, ArtifactModel, BoundedText, PartId},
    conversation::{
        NoticeLevel, NoticePart, PendingInteraction, PendingPlanConfirmation, PendingQuestions,
        PendingToolApproval, PlanTaskModel, QuestionModel, QuestionOptionModel, TimelinePart,
        ToolStatus, TurnState,
    },
};

use super::{tool_output::parse_tool_arguments, ConversationProjection};

impl ConversationProjection {
    pub fn merge_recovery(&mut self, recovery: &SessionRecoveryState) {
        let turn_state = match recovery.status {
            RecoveryStatus::AwaitingInput => TurnState::AwaitingInput,
            RecoveryStatus::Interrupted => TurnState::Interrupted,
            RecoveryStatus::Streaming | RecoveryStatus::ToolExecuting => TurnState::Live,
        };
        self.current_turn_mut().state = turn_state;

        self.merge_partial_text(
            &recovery.partial_assistant.text,
            recovery
                .partial_assistant
                .citations
                .iter()
                .cloned()
                .map(super::live::map_citation)
                .collect(),
        );
        self.merge_partial_thinking(&recovery.partial_assistant.thinking);

        let tool_status = match recovery.status {
            RecoveryStatus::Streaming => ToolStatus::Receiving,
            RecoveryStatus::ToolExecuting => ToolStatus::Running,
            RecoveryStatus::AwaitingInput => ToolStatus::Pending,
            RecoveryStatus::Interrupted => ToolStatus::Interrupted,
        };
        for tool_call in &recovery.partial_assistant.tool_calls {
            let tool = self.upsert_tool(&tool_call.id, &tool_call.name, tool_status, false);
            tool.arguments = parse_tool_arguments(&tool_call.arguments.value);
        }

        for pending in &recovery.pending_interactions {
            self.merge_pending_interaction(pending);
        }

        self.presentation.metadata.last_error = recovery.last_error.clone();
        self.presentation.metadata.stop_reason = recovery
            .stop_reason
            .as_ref()
            .map(|reason| format!("{reason:?}"));
        self.push_recovery_notice_once(recovery);
        self.presentation.live_turn_id = Some(self.current_turn_mut().id.clone());
    }

    fn merge_partial_text(
        &mut self,
        partial: &str,
        citations: Vec<crate::tui_v2::model::conversation::CitationModel>,
    ) {
        if partial.trim().is_empty() && citations.is_empty() {
            return;
        }
        let turn_index = self.ensure_turn();
        if let Some((part_index, part)) = self.presentation.turns[turn_index]
            .parts
            .iter_mut()
            .enumerate()
            .rev()
            .find_map(|(index, part)| match part {
                TimelinePart::AgentText(part)
                    if partial.starts_with(&part.text) || part.text.starts_with(partial) =>
                {
                    Some((index, part))
                }
                _ => None,
            })
        {
            if partial.len() > part.text.len() {
                part.text = partial.to_owned();
            }
            part.streaming = true;
            super::merge_citations(&mut part.citations, citations);
            self.active_text = Some((turn_index, part_index));
            return;
        }

        self.append_agent_text(partial, citations);
    }

    fn merge_partial_thinking(&mut self, partial: &str) {
        if partial.trim().is_empty() {
            return;
        }
        let turn_index = self.ensure_turn();
        if let Some((part_index, part)) = self.presentation.turns[turn_index]
            .parts
            .iter_mut()
            .enumerate()
            .rev()
            .find_map(|(index, part)| match part {
                TimelinePart::Thinking(part)
                    if partial.starts_with(&part.content) || part.content.starts_with(partial) =>
                {
                    Some((index, part))
                }
                _ => None,
            })
        {
            if partial.len() > part.content.len() {
                part.content = partial.to_owned();
            }
            part.streaming = true;
            self.active_thinking = Some((turn_index, part_index));
            return;
        }

        self.append_thinking(partial);
    }

    fn merge_pending_interaction(&mut self, pending: &PendingInteractionSnapshot) {
        match pending {
            PendingInteractionSnapshot::ToolApproval { tool_call } => {
                let arguments = parse_tool_arguments(&tool_call.arguments.value);
                self.upsert_tool(
                    &tool_call.id,
                    &tool_call.name,
                    ToolStatus::AwaitingApproval,
                    false,
                )
                .arguments = arguments.clone();
                self.set_pending_interaction(PendingInteraction::ToolApproval(
                    PendingToolApproval {
                        session_id: self.session_id.clone(),
                        tool_call_id: tool_call.id.clone(),
                        tool_name: tool_call.name.clone(),
                        arguments,
                    },
                ));
            }
            PendingInteractionSnapshot::AskUserQuestion {
                tool_call_id,
                questions,
            } => {
                self.upsert_tool(tool_call_id, "AskUserQuestion", ToolStatus::Pending, false);
                self.set_pending_interaction(PendingInteraction::Questions(PendingQuestions {
                    session_id: self.session_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    questions: questions
                        .iter()
                        .map(|question| QuestionModel {
                            header: question.header.clone(),
                            question: question.question.clone(),
                            options: question
                                .options
                                .iter()
                                .map(|option| QuestionOptionModel {
                                    label: option.label.clone(),
                                    description: option.description.clone(),
                                })
                                .collect(),
                            multi_select: question.multi_select,
                        })
                        .collect(),
                }));
            }
            PendingInteractionSnapshot::PlanConfirm {
                tool_call_id,
                title,
                task_count,
                tasks,
            } => {
                self.upsert_tool(tool_call_id, "PlanConfirm", ToolStatus::Pending, false);
                self.set_pending_interaction(PendingInteraction::PlanConfirm(
                    PendingPlanConfirmation {
                        session_id: self.session_id.clone(),
                        tool_call_id: tool_call_id.clone(),
                        title: title.clone(),
                        task_count: *task_count,
                        tasks: tasks
                            .iter()
                            .map(|task| PlanTaskModel {
                                description: task.description.clone(),
                                completed: task.completed,
                            })
                            .collect(),
                    },
                ));
            }
        }
    }

    fn push_recovery_notice_once(&mut self, recovery: &SessionRecoveryState) {
        let session_id = self.session_id.clone();
        let turn_id = self.current_turn_mut().id.clone();
        let part_id =
            PartId::from_semantic(format!("recovery:{}/{}", session_id, turn_id.as_str()));
        if self.presentation.part(&part_id).is_some() {
            return;
        }
        let expandable = recovery.last_error.as_ref().map(|error| ArtifactModel {
            content: ArtifactContent::Text(BoundedText {
                text: error.clone(),
                omitted_bytes: 0,
            }),
            ..ArtifactModel::default()
        });
        self.push_part(TimelinePart::Notice(NoticePart {
            id: part_id,
            message: recovery.notice(),
            level: NoticeLevel::Warning,
            expandable,
        }));
    }
}

#[cfg(test)]
mod tests {
    use mitsuro_core::{
        agent::loop_events::LoopStopReason,
        ai::types::{Content, ModelMessage, Role},
        storage::{
            PartialAssistantState, PendingQuestionSnapshot, RecoveryDecision,
            RecoveryToolArguments, RecoveryToolCall,
        },
    };
    use serde_json::json;

    use super::*;

    fn recovery(pending_interactions: Vec<PendingInteractionSnapshot>) -> SessionRecoveryState {
        SessionRecoveryState::new_with_pending_interactions(
            RecoveryStatus::AwaitingInput,
            Some(LoopStopReason::AwaitingInput),
            None,
            PartialAssistantState {
                citations: Vec::new(),
                text: "I inspected it.".to_owned(),
                thinking: String::new(),
                tool_calls: vec![RecoveryToolCall {
                    id: "read-1".to_owned(),
                    name: "read".to_owned(),
                    arguments: RecoveryToolArguments {
                        value: json!({"path": "src/main.rs"}),
                        redacted_paths: Vec::new(),
                    },
                }],
            },
            pending_interactions,
            RecoveryDecision::NonResumable {
                reason: mitsuro_core::storage::RecoveryNonResumableReason::AwaitingHumanInput,
            },
        )
    }

    #[test]
    fn recovery_merge_deduplicates_persisted_tool_and_partial_text() {
        let mut projection = ConversationProjection::from_model_messages(
            "session",
            &[
                ModelMessage {
                    role: Role::User,
                    content: vec![Content::text("inspect".to_owned())],
                },
                ModelMessage {
                    role: Role::Assistant,
                    content: vec![
                        Content::text("I inspected".to_owned()),
                        Content::ToolUse {
                            id: "read-1".to_owned(),
                            name: "read".to_owned(),
                            input: json!({"path": "src/main.rs"}),
                        },
                    ],
                },
            ],
        );

        projection.merge_recovery(&recovery(vec![]));

        let parts = &projection.presentation().turns[0].parts;
        assert_eq!(
            parts
                .iter()
                .filter(|part| matches!(part, TimelinePart::Tool(_)))
                .count(),
            1
        );
        assert_eq!(
            parts
                .iter()
                .filter_map(|part| match part {
                    TimelinePart::AgentText(part) => Some(part.text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["I inspected it."]
        );
    }

    #[test]
    fn every_recovered_pending_interaction_is_preserved() {
        let mut projection = ConversationProjection::new("session");
        projection.merge_recovery(&recovery(vec![
            PendingInteractionSnapshot::AskUserQuestion {
                tool_call_id: "q1".to_owned(),
                questions: vec![PendingQuestionSnapshot {
                    header: "Choice".to_owned(),
                    question: "Which?".to_owned(),
                    options: Vec::new(),
                    multi_select: false,
                }],
            },
            PendingInteractionSnapshot::PlanConfirm {
                tool_call_id: "p1".to_owned(),
                title: "Plan".to_owned(),
                task_count: 2,
                tasks: Vec::new(),
            },
        ]));

        assert_eq!(projection.presentation().pending_interactions.len(), 2);
    }
}
