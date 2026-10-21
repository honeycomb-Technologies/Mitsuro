//! Live `LoopEvent` projection.

use mitsuro_core::{
    agent::{
        loop_events::LoopStopReason,
        subagent::{AgentProgress, AgentProgressStatus},
        DelegatedProgressEvent, DelegatedRunStage, DelegatedToolKind, LoopEvent,
    },
    ai::types::Citation,
    storage::{
        DelegatedRunRole, DelegationGroupRecord, DelegationGroupState, DelegationTaskRecord,
        DelegationTaskState,
    },
};

use crate::tui_v2::model::{
    artifact::{
        ArtifactContent, ArtifactModel, BoundedText, PartId, WebDocumentArtifact, WebResultArtifact,
    },
    conversation::{
        CitationModel, CompactionPart, ErrorPart, NoticeLevel, NoticePart, PendingInteraction,
        PendingPlanConfirmation, PendingQuestions, PendingToolApproval, PlanTaskModel,
        QuestionModel, QuestionOptionModel, RunBudgetSnapshot, TimelinePart, ToolStatus, TurnState,
        UsageSnapshot, WorkflowRevision,
    },
};

use super::{
    tool_output::{append_tool_delta, bound_text, finalize_tool_output, parse_tool_arguments},
    ConversationProjection,
};

impl ConversationProjection {
    pub fn apply_event(&mut self, event: LoopEvent) {
        match event {
            LoopEvent::TextDelta { delta } => self.append_agent_text(&delta, Vec::new()),
            LoopEvent::TextDeltaWithCitations { delta, citations } => {
                self.append_agent_text(&delta, citations.into_iter().map(map_citation).collect());
            }
            LoopEvent::ThinkingDelta { thinking } => self.append_thinking(&thinking),
            LoopEvent::ThinkingComplete {
                thinking,
                signature,
            } => self.complete_thinking(&thinking, Some(signature)),
            LoopEvent::ToolCallStart { id, name } => {
                self.upsert_tool(&id, &name, ToolStatus::Receiving, false);
            }
            LoopEvent::ToolCallPreparing { id, name, .. } => {
                self.upsert_tool(&id, &name, ToolStatus::Receiving, false);
            }
            LoopEvent::ToolCallComplete {
                id,
                name,
                arguments,
            } => {
                let parsed = parse_tool_arguments(&arguments);
                self.upsert_tool(&id, &name, ToolStatus::Pending, false)
                    .arguments = parsed;
                self.prepare_special_interaction(&id, &name, &arguments);
            }
            LoopEvent::ToolExecuting { id, name } => {
                self.upsert_tool(&id, &name, ToolStatus::Running, false);
            }
            LoopEvent::ToolOutputDelta { id, delta } => {
                let tool = self.upsert_tool(&id, "", ToolStatus::Running, false);
                append_tool_delta(&mut tool.artifact, &delta);
            }
            LoopEvent::ToolResult {
                id,
                output,
                is_error,
            } => {
                let tool = self.upsert_tool(
                    &id,
                    "",
                    if is_error {
                        ToolStatus::Failed
                    } else {
                        ToolStatus::Succeeded
                    },
                    false,
                );
                finalize_tool_output(&mut tool.artifact, &tool.name, &output);
                self.clear_pending_interaction(&id);
            }
            LoopEvent::AwaitingInput {
                tool_call_id,
                tool_name,
            } => {
                self.current_turn_mut().state = TurnState::AwaitingInput;
                let status = if tool_name.eq_ignore_ascii_case("askuserquestion")
                    || tool_name.eq_ignore_ascii_case("planconfirm")
                {
                    ToolStatus::Pending
                } else {
                    ToolStatus::AwaitingApproval
                };
                self.upsert_tool(&tool_call_id, &tool_name, status, false);
            }
            LoopEvent::ToolApprovalRequired {
                id,
                name,
                arguments,
            } => {
                let arguments = parse_tool_arguments(&arguments);
                let session_id = self.session_id.clone();
                self.upsert_tool(&id, &name, ToolStatus::AwaitingApproval, false)
                    .arguments = arguments.clone();
                self.set_pending_interaction(PendingInteraction::ToolApproval(
                    PendingToolApproval {
                        session_id,
                        tool_call_id: id,
                        tool_name: name,
                        arguments,
                    },
                ));
                self.current_turn_mut().state = TurnState::AwaitingInput;
            }
            LoopEvent::ToolApproved { id } => {
                if let Some(tool) = self.tool_mut(&id) {
                    tool.status = ToolStatus::Approved;
                }
                self.clear_pending_interaction(&id);
                self.current_turn_mut().state = TurnState::Live;
            }
            LoopEvent::ToolDenied { id } => {
                if let Some(tool) = self.tool_mut(&id) {
                    tool.status = ToolStatus::Denied;
                }
                self.clear_pending_interaction(&id);
                self.current_turn_mut().state = TurnState::Live;
            }
            LoopEvent::SteeringInjected {
                pending_id,
                message,
            } => {
                let ordinal = self.presentation().turns.len();
                let id = pending_id.unwrap_or_else(|| format!("derived:{ordinal}"));
                self.push_user_prompt(&format!("steering:{id}"), message, Vec::new(), true);
            }
            LoopEvent::ServerToolStart { id, name } => {
                self.upsert_tool(&id, &name, ToolStatus::Running, true);
            }
            LoopEvent::ServerToolComplete { id, name } => {
                if let Some(tool) = self.tool_mut(&id) {
                    if !name.is_empty() {
                        tool.name = name;
                    }
                    tool.server_side = true;
                    if !matches!(tool.status, ToolStatus::Failed) {
                        tool.status = ToolStatus::Succeeded;
                    }
                } else {
                    self.upsert_tool(&id, &name, ToolStatus::Succeeded, true);
                }
            }
            LoopEvent::WebSearchResults {
                tool_use_id,
                results,
            } => {
                let tool = self.upsert_tool(&tool_use_id, "web_search", ToolStatus::Running, true);
                tool.artifact = ArtifactModel {
                    content: ArtifactContent::WebResults(
                        results
                            .into_iter()
                            .take(100)
                            .map(|result| WebResultArtifact {
                                title: result.title,
                                url: result.url,
                                age: result.page_age,
                            })
                            .collect(),
                    ),
                    ..ArtifactModel::default()
                };
            }
            LoopEvent::WebFetchResult {
                tool_use_id,
                content,
            } => {
                let tool = self.upsert_tool(&tool_use_id, "web_fetch", ToolStatus::Running, true);
                tool.artifact = ArtifactModel {
                    content: ArtifactContent::WebDocument(WebDocumentArtifact {
                        title: content.title,
                        url: content.url,
                        media_type: content.media_type,
                        content: bound_text(
                            &super::tool_output::sanitize_terminal_text(&content.content),
                            super::tool_output::LIVE_ARTIFACT_BYTES,
                        ),
                    }),
                    ..ArtifactModel::default()
                };
            }
            LoopEvent::ServerToolError {
                tool_use_id,
                error_code,
            } => {
                let tool = self.upsert_tool(&tool_use_id, "", ToolStatus::Failed, true);
                tool.artifact = ArtifactModel {
                    content: ArtifactContent::Text(BoundedText {
                        text: format!("Server tool failed: {error_code}"),
                        omitted_bytes: 0,
                    }),
                    ..ArtifactModel::default()
                };
            }
            LoopEvent::ModeChange { mode, .. } => self.presentation.metadata.mode = Some(mode),
            LoopEvent::PlanUpdate { tasks } => {
                self.presentation.metadata.plan_tasks = tasks
                    .into_iter()
                    .map(|task| PlanTaskModel {
                        description: task.description,
                        completed: task.completed,
                    })
                    .collect();
            }
            LoopEvent::WorkflowUpdated {
                goal_id,
                aggregate_revision,
                operation_id,
            } => {
                let should_update = self
                    .presentation
                    .metadata
                    .workflow
                    .as_ref()
                    .is_none_or(|current| aggregate_revision > current.aggregate_revision);
                if should_update {
                    self.presentation.metadata.workflow = Some(WorkflowRevision {
                        goal_id,
                        aggregate_revision,
                        operation_id,
                    });
                }
            }
            LoopEvent::PlanComplete {
                tool_call_id,
                title,
                task_count,
            } => {
                self.set_pending_interaction(PendingInteraction::PlanConfirm(
                    PendingPlanConfirmation {
                        session_id: self.session_id.clone(),
                        tool_call_id,
                        title,
                        task_count,
                        tasks: self.presentation.metadata.plan_tasks.clone(),
                    },
                ));
                self.current_turn_mut().state = TurnState::AwaitingInput;
            }
            LoopEvent::AgentSleeping {
                duration_secs,
                reason,
            } => {
                let ordinal = self.current_turn_mut().parts.len();
                self.push_part(TimelinePart::Notice(NoticePart {
                    id: PartId::from_semantic(format!("sleep:{ordinal}")),
                    message: format!("Waiting {duration_secs}s — {reason}"),
                    level: NoticeLevel::Neutral,
                    expandable: None,
                }));
            }
            LoopEvent::TurnComplete { has_more, .. } => {
                self.settle_streaming_parts(ToolStatus::Interrupted);
                if !has_more {
                    self.current_turn_mut().state = TurnState::Completed;
                    crate::tui_v2::presentation::retention::apply_historical_retention(
                        &mut self.presentation,
                    );
                }
            }
            LoopEvent::RunBudgetResolved { max_turns, source } => {
                self.presentation.metadata.run_budget = Some(RunBudgetSnapshot {
                    max_turns,
                    source: format!("{source:?}"),
                });
            }
            LoopEvent::ProviderRequestPrepared { .. }
            | LoopEvent::MicrocompactionApplied { .. }
            | LoopEvent::TickInjected { .. }
            | LoopEvent::ClassifierDecision { .. } => {}
            LoopEvent::ProgressGuard { telemetry } => {
                if telemetry.triggered {
                    self.presentation.metadata.last_error = telemetry.diagnostic();
                }
            }
            LoopEvent::Usage {
                prompt_tokens,
                input_tokens,
                completion_tokens,
                reasoning_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                total_tokens,
            } => {
                let usage = UsageSnapshot {
                    prompt_tokens,
                    input_tokens,
                    completion_tokens,
                    reasoning_tokens,
                    cache_creation_input_tokens,
                    cache_read_input_tokens,
                    total_tokens,
                };
                self.presentation.metadata.usage = Some(usage.clone());
                self.current_turn_mut().usage = Some(usage);
            }
            LoopEvent::SessionPinched {
                reason,
                source_session_id,
                new_session_id,
                estimated_tokens_before,
            } => {
                self.push_part(TimelinePart::Notice(NoticePart {
                    id: PartId::from_semantic(format!(
                        "continuation:{source_session_id}:{new_session_id}"
                    )),
                    message: format!(
                        "Continued from {source_session_id} to {new_session_id}: {reason}"
                    ),
                    level: NoticeLevel::Neutral,
                    expandable: Some(ArtifactModel {
                        content: ArtifactContent::Fields(vec![
                            crate::tui_v2::model::artifact::ArtifactField {
                                key: "source_session".to_owned(),
                                value: source_session_id,
                            },
                            crate::tui_v2::model::artifact::ArtifactField {
                                key: "new_session".to_owned(),
                                value: new_session_id,
                            },
                            crate::tui_v2::model::artifact::ArtifactField {
                                key: "estimated_tokens_before".to_owned(),
                                value: estimated_tokens_before.to_string(),
                            },
                        ]),
                        ..ArtifactModel::default()
                    }),
                }));
            }
            LoopEvent::ContextCompactionStarted { .. } => {}
            LoopEvent::ContextCompacted {
                reason,
                estimated_tokens_before,
                estimated_tokens_after,
                replaced_messages,
                checkpoint_id,
                ..
            } => {
                self.push_part(TimelinePart::Compaction(CompactionPart {
                    id: PartId::from_semantic(format!("compaction:{checkpoint_id}")),
                    reason,
                    estimated_tokens_before,
                    estimated_tokens_after: Some(estimated_tokens_after),
                    replaced_messages: Some(replaced_messages),
                    checkpoint_id: Some(checkpoint_id),
                    in_place: true,
                }));
            }
            LoopEvent::TitleGenerated { title } => {
                self.presentation.metadata.title = Some(title);
            }
            LoopEvent::Finished {
                session_id: _,
                stop_reason,
            } => self.finish(stop_reason),
            LoopEvent::Error { error } => {
                self.presentation.metadata.last_error = Some(error.clone());
                let ordinal = self.current_turn_mut().parts.len();
                self.push_part(TimelinePart::Error(ErrorPart {
                    id: PartId::from_semantic(format!("error:{ordinal}")),
                    message: error,
                    provider_request_failure: true,
                }));
            }
            LoopEvent::AgentBackgroundStarted {
                delegated_run_id,
                agent_type,
                description,
            } => {
                let key = format!("delegated:{delegated_run_id}");
                let tool = self.upsert_tool(&key, "agent", ToolStatus::Running, false);
                tool.arguments.fields = vec![
                    crate::tui_v2::model::artifact::ArtifactField {
                        key: "agent_type".to_owned(),
                        value: agent_type.clone(),
                    },
                    crate::tui_v2::model::artifact::ArtifactField {
                        key: "description".to_owned(),
                        value: description.clone(),
                    },
                ];
                // Seed an expandable live stream panel (child lines fill in via progress).
                tool.artifact = ArtifactModel {
                    content: ArtifactContent::Text(BoundedText {
                        text: format!("· {agent_type}  {description}\n  waiting for children…"),
                        omitted_bytes: 0,
                    }),
                    ..ArtifactModel::default()
                };
            }
            LoopEvent::AgentBackgroundCompleted {
                delegated_run_id,
                agent_type,
                success,
                summary,
            } => {
                let key = format!("delegated:{delegated_run_id}");
                let tool = self.upsert_tool(
                    &key,
                    "agent",
                    if success {
                        ToolStatus::Succeeded
                    } else {
                        ToolStatus::Failed
                    },
                    false,
                );
                tool.arguments.fields = vec![
                    crate::tui_v2::model::artifact::ArtifactField {
                        key: "agent_type".to_owned(),
                        value: agent_type,
                    },
                    crate::tui_v2::model::artifact::ArtifactField {
                        key: "description".to_owned(),
                        value: summary.lines().next().unwrap_or("done").to_owned(),
                    },
                ];
                // Prefer keeping the live child stream if we already have one;
                // append the final summary as a footer.
                let prior = match &tool.artifact.content {
                    ArtifactContent::Text(text) if !text.text.trim().is_empty() => {
                        Some(text.text.clone())
                    }
                    _ => None,
                };
                let body = if let Some(prior) = prior {
                    if prior.contains(summary.trim()) {
                        prior
                    } else {
                        format!("{prior}\n──\n{summary}")
                    }
                } else {
                    summary
                };
                tool.artifact = ArtifactModel {
                    content: ArtifactContent::Text(bound_text(
                        &body,
                        super::tool_output::LIVE_ARTIFACT_BYTES,
                    )),
                    ..ArtifactModel::default()
                };
            }
            LoopEvent::UserMessage {
                title,
                message,
                level,
            } => {
                let ordinal = self.current_turn_mut().parts.len();
                self.push_part(TimelinePart::Notice(NoticePart {
                    id: PartId::from_semantic(format!("agent-message:{ordinal}")),
                    message: title.map_or(message.clone(), |title| format!("{title}: {message}")),
                    level: match level.as_str() {
                        "error" | "warning" => NoticeLevel::Warning,
                        "success" => NoticeLevel::Success,
                        _ => NoticeLevel::Neutral,
                    },
                    expandable: None,
                }));
            }
        }
    }

    fn prepare_special_interaction(
        &mut self,
        tool_call_id: &str,
        tool_name: &str,
        arguments: &serde_json::Value,
    ) {
        if tool_name.eq_ignore_ascii_case("askuserquestion") {
            self.set_pending_interaction(PendingInteraction::Questions(PendingQuestions {
                session_id: self.session_id.clone(),
                tool_call_id: tool_call_id.to_owned(),
                questions: parse_questions(arguments),
            }));
        }
    }

    fn clear_pending_interaction(&mut self, tool_call_id: &str) {
        self.presentation
            .pending_interactions
            .retain(|pending| pending.tool_call_id() != tool_call_id);
    }

    fn finish(&mut self, reason: LoopStopReason) {
        let (turn_state, tool_status) = match reason {
            LoopStopReason::Completed | LoopStopReason::Pinched => {
                (TurnState::Completed, ToolStatus::Interrupted)
            }
            LoopStopReason::AwaitingInput | LoopStopReason::Sleeping => {
                (TurnState::AwaitingInput, ToolStatus::AwaitingApproval)
            }
            LoopStopReason::UserAbort => (TurnState::Interrupted, ToolStatus::Interrupted),
            LoopStopReason::BudgetExhausted
            | LoopStopReason::ProviderError
            | LoopStopReason::LoopGuardTriggered
            | LoopStopReason::StreamIdleTimeout
            | LoopStopReason::PinchFailed => (TurnState::Failed, ToolStatus::Failed),
        };
        self.settle_streaming_parts(tool_status);
        self.current_turn_mut().state = turn_state;
        self.presentation.metadata.stop_reason = Some(format!("{reason:?}"));
        self.presentation.live_turn_id = None;
    }
}

/// Merge a live child-agent progress tick into the parent `agent` tool stream.
pub(super) fn apply_delegated_progress(
    projection: &mut ConversationProjection,
    event: &DelegatedProgressEvent,
) {
    let delegated_key = format!("delegated:{}", event.delegated_run_id);
    let keys = [
        delegated_key.clone(),
        event.tool_call_id.clone(),
        format!("delegated:{}", event.tool_call_id),
    ];
    let existing = keys
        .iter()
        .find(|key| projection.tool_locations.contains_key(key.as_str()))
        .cloned();
    let key = existing.unwrap_or(delegated_key);
    let aggregate_is_settled = projection.tool_mut(&key).is_some_and(|tool| {
        matches!(
            tool.status,
            ToolStatus::Succeeded
                | ToolStatus::Failed
                | ToolStatus::Denied
                | ToolStatus::Interrupted
        )
    });
    // Aggregate completion/tool-result is authoritative. A buffered child
    // tick arriving afterward must never reopen or relabel a settled run.
    if aggregate_is_settled {
        if let Some(loc) = projection.tool_locations.get(&key).copied() {
            projection
                .tool_locations
                .insert(format!("delegated:{}", event.delegated_run_id), loc);
            projection
                .tool_locations
                .insert(event.tool_call_id.clone(), loc);
        }
        return;
    }
    let tool = projection.upsert_tool(&key, "agent", ToolStatus::Running, false);
    update_agent_stream(tool, event);
    // Dual-index so completion events keyed either way still hit this row.
    if let Some(loc) = projection.tool_locations.get(&key).copied() {
        projection
            .tool_locations
            .insert(format!("delegated:{}", event.delegated_run_id), loc);
        projection
            .tool_locations
            .insert(event.tool_call_id.clone(), loc);
    }
}

/// Replay canonical delegation state after transcript projection. Transcript
/// tool calls provide the stable row when present; the durable group id is the
/// fallback for detached work that never reached canonical message history.
pub(super) fn restore_delegation_groups(
    projection: &mut ConversationProjection,
    groups: &[DelegationGroupRecord],
) {
    for group in groups.iter().rev() {
        let tool_call_id = group
            .parent_tool_call_id
            .as_deref()
            .unwrap_or(&group.delegation_group_id)
            .to_owned();
        for task in &group.tasks {
            apply_restored_delegated_progress(
                projection,
                &durable_progress_event(group, task, tool_call_id.clone()),
            );
        }
        restore_group_status(projection, group, &tool_call_id);
    }
}

fn apply_restored_delegated_progress(
    projection: &mut ConversationProjection,
    event: &DelegatedProgressEvent,
) {
    let delegated_key = format!("delegated:{}", event.delegated_run_id);
    let keys = [
        delegated_key.clone(),
        event.tool_call_id.clone(),
        format!("delegated:{}", event.tool_call_id),
    ];
    let key = keys
        .iter()
        .find(|key| projection.tool_locations.contains_key(key.as_str()))
        .cloned()
        .unwrap_or(delegated_key);
    let tool = projection.upsert_tool(&key, "agent", ToolStatus::Running, false);
    update_agent_stream(tool, event);
    if let Some(loc) = projection.tool_locations.get(&key).copied() {
        projection
            .tool_locations
            .insert(format!("delegated:{}", event.delegated_run_id), loc);
        projection
            .tool_locations
            .insert(event.tool_call_id.clone(), loc);
    }
}

fn durable_progress_event(
    group: &DelegationGroupRecord,
    task: &DelegationTaskRecord,
    tool_call_id: String,
) -> DelegatedProgressEvent {
    let status = AgentProgressStatus::from(task.state);
    let completion_summary = task
        .result
        .as_ref()
        .and_then(|result| result.get("summary"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| task.error_summary.clone());
    DelegatedProgressEvent {
        delegated_run_id: group.delegation_group_id.clone(),
        parent_session_id: group.parent_session_id.clone(),
        tool_call_id,
        kind: role_kind(&task.specification.role),
        stage: task_stage(task.state),
        progress: AgentProgress {
            delegated_run_id: Some(group.delegation_group_id.clone()),
            task_id: task.specification.delegation_task_id.clone(),
            name: task.specification.task_key.clone(),
            status,
            completion_summary,
            ..AgentProgress::default()
        },
    }
}

fn restore_group_status(
    projection: &mut ConversationProjection,
    group: &DelegationGroupRecord,
    tool_call_id: &str,
) {
    let delegated_key = format!("delegated:{}", group.delegation_group_id);
    let keys = [
        delegated_key.clone(),
        tool_call_id.to_owned(),
        format!("delegated:{tool_call_id}"),
    ];
    let key = keys
        .iter()
        .find(|key| projection.tool_locations.contains_key(key.as_str()))
        .cloned()
        .unwrap_or(delegated_key);
    let group_label = group_state_label(group.state);
    let role_label = role_label(group.tasks.first().map(|task| &task.specification.role));
    let tool = projection.upsert_tool(&key, "agent", group_tool_status(group.state), false);
    let prior = match &tool.artifact.content {
        ArtifactContent::Text(text) => text.text.clone(),
        _ => String::new(),
    };
    let marker = format!("· group  {group_label}");
    let body = if prior.lines().any(|line| line == marker) {
        prior
    } else if prior.trim().is_empty() {
        marker
    } else {
        format!("{prior}\n\n{marker}")
    };
    tool.artifact = ArtifactModel {
        content: ArtifactContent::Text(bound_text(&body, super::tool_output::LIVE_ARTIFACT_BYTES)),
        ..ArtifactModel::default()
    };
    tool.arguments
        .fields
        .retain(|field| field.key != "agent_type" && field.key != "description");
    tool.arguments.fields.extend([
        crate::tui_v2::model::artifact::ArtifactField {
            key: "agent_type".to_owned(),
            value: role_label.to_owned(),
        },
        crate::tui_v2::model::artifact::ArtifactField {
            key: "description".to_owned(),
            value: format!("delegation group: {group_label}"),
        },
    ]);
    if let Some(loc) = projection.tool_locations.get(&key).copied() {
        projection
            .tool_locations
            .insert(format!("delegated:{}", group.delegation_group_id), loc);
        projection
            .tool_locations
            .insert(tool_call_id.to_owned(), loc);
    }
}

const fn group_tool_status(state: DelegationGroupState) -> ToolStatus {
    match state {
        DelegationGroupState::Complete | DelegationGroupState::Degraded => ToolStatus::Succeeded,
        DelegationGroupState::Failed => ToolStatus::Failed,
        DelegationGroupState::Cancelled => ToolStatus::Interrupted,
        DelegationGroupState::Created
        | DelegationGroupState::Queued
        | DelegationGroupState::Running
        | DelegationGroupState::ReadyForParent
        | DelegationGroupState::Synthesizing => ToolStatus::Running,
    }
}

const fn group_state_label(state: DelegationGroupState) -> &'static str {
    match state {
        DelegationGroupState::Created => "created",
        DelegationGroupState::Queued => "queued",
        DelegationGroupState::Running => "running",
        DelegationGroupState::ReadyForParent => "ready for parent",
        DelegationGroupState::Synthesizing => "synthesizing",
        DelegationGroupState::Complete => "complete",
        DelegationGroupState::Degraded => "degraded",
        DelegationGroupState::Failed => "failed",
        DelegationGroupState::Cancelled => "cancelled",
    }
}

const fn role_kind(role: &DelegatedRunRole) -> DelegatedToolKind {
    match role {
        DelegatedRunRole::Explore => DelegatedToolKind::Explore,
        DelegatedRunRole::Build => DelegatedToolKind::Build,
        DelegatedRunRole::Planner => DelegatedToolKind::Plan,
        DelegatedRunRole::Verifier => DelegatedToolKind::Verify,
    }
}

const fn role_label(role: Option<&DelegatedRunRole>) -> &'static str {
    match role {
        Some(DelegatedRunRole::Explore) => "explore",
        Some(DelegatedRunRole::Build) => "build",
        Some(DelegatedRunRole::Planner) => "plan",
        Some(DelegatedRunRole::Verifier) => "verify",
        None => "agent",
    }
}

const fn task_stage(state: DelegationTaskState) -> DelegatedRunStage {
    match state {
        DelegationTaskState::Created
        | DelegationTaskState::Queued
        | DelegationTaskState::Leased => DelegatedRunStage::Created,
        DelegationTaskState::Running | DelegationTaskState::Retrying => DelegatedRunStage::Running,
        DelegationTaskState::Complete => DelegatedRunStage::Complete,
        DelegationTaskState::Degraded => DelegatedRunStage::Degraded,
        DelegationTaskState::Failed => DelegatedRunStage::Failed,
        DelegationTaskState::Cancelled => DelegatedRunStage::Cancelled,
    }
}

fn update_agent_stream(
    tool: &mut crate::tui_v2::model::conversation::ToolPart,
    event: &DelegatedProgressEvent,
) {
    let progress = &event.progress;
    let display_name = progress
        .identity
        .as_ref()
        .map(|id| id.display_name())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            if progress.name.is_empty() {
                progress.task_id.clone()
            } else {
                progress.name.clone()
            }
        });
    let action = progress
        .current_action
        .as_deref()
        .or(progress.completion_summary.as_deref())
        .unwrap_or_else(|| agent_progress_label(&progress.status));
    let lifecycle_label = agent_progress_label(&progress.status);

    // Growing chat log (dialog + tool use), not a single status line.
    let prior = match &tool.artifact.content {
        ArtifactContent::Text(text) => text.text.clone(),
        _ => String::new(),
    };
    let mut lines: Vec<String> = prior
        .lines()
        .filter(|line| {
            // Drop the seed placeholder once real stream starts.
            !line.contains("waiting for children")
        })
        .map(str::to_owned)
        .collect();

    // Re-open a child's section when parallel progress interleaves. Without
    // this, an update from child A after child B would appear under B's name.
    let section = format!("── {display_name} ──");
    let latest_section = lines
        .iter()
        .rev()
        .find(|line| line.starts_with("── ") && line.ends_with(" ──"));
    if latest_section != Some(&section) {
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(section.clone());
    }

    // Render lifecycle transitions from the typed progress status. This keeps
    // queue/provider/retry/terminal state visible without inspecting arbitrary
    // action prose, while suppressing repeated streaming ticks in one state.
    let section_start = lines.iter().rposition(|line| line == &section).unwrap_or(0);
    let latest_lifecycle = lines[section_start + 1..].iter().rev().find_map(|line| {
        line.strip_prefix("· status  ")
            .map(str::trim)
            .filter(|label| !label.is_empty())
    });
    if latest_lifecycle != Some(lifecycle_label) {
        lines.push(format!("· status  {lifecycle_label}"));
    }

    // Core AgentProgress no longer streams per-line agent prose on the progress
    // object; surface the latest action / completion summary instead.
    if let Some(action) = progress
        .completion_summary
        .as_deref()
        .or(progress.current_action.as_deref())
    {
        let formatted = action.trim();
        if !formatted.is_empty() && lines.last().map(String::as_str) != Some(formatted) {
            for chunk in formatted.lines() {
                lines.push(chunk.to_owned());
            }
        }
    }

    let body = lines.join("\n");
    tool.artifact = ArtifactModel {
        content: ArtifactContent::Text(bound_text(&body, super::tool_output::LIVE_ARTIFACT_BYTES)),
        ..ArtifactModel::default()
    };

    // Child completion is not aggregate completion. The card remains live
    // until ToolResult (foreground) or AgentBackgroundCompleted (detached)
    // settles the group exactly once.
    tool.status = ToolStatus::Running;

    // Collapsed-row summary tracks latest activity.
    if let Some(field) = tool
        .arguments
        .fields
        .iter_mut()
        .find(|field| field.key == "description")
    {
        field.value = format_agent_progress_summary(&display_name, lifecycle_label, action);
    } else {
        tool.arguments
            .fields
            .push(crate::tui_v2::model::artifact::ArtifactField {
                key: "description".to_owned(),
                value: format_agent_progress_summary(&display_name, lifecycle_label, action),
            });
    }
}

fn agent_progress_label(status: &AgentProgressStatus) -> &'static str {
    match status {
        AgentProgressStatus::Created => "created",
        AgentProgressStatus::Queued => "queued",
        AgentProgressStatus::Leased => "waiting for provider",
        AgentProgressStatus::Running => "running",
        AgentProgressStatus::Retrying => "retrying",
        AgentProgressStatus::Complete => "complete",
        AgentProgressStatus::Degraded => "degraded",
        AgentProgressStatus::Failed => "failed",
        AgentProgressStatus::Cancelled => "cancelled",
    }
}

fn format_agent_progress_summary(name: &str, status: &str, action: &str) -> String {
    let action = action.trim();
    if action.is_empty() || action == status {
        format!("{name}: {status}")
    } else {
        format!("{name}: {status} — {action}")
    }
}

pub(super) fn map_citation(citation: Citation) -> CitationModel {
    CitationModel {
        url: citation.url,
        title: citation.title,
        cited_text: citation.cited_text,
    }
}

fn parse_questions(arguments: &serde_json::Value) -> Vec<QuestionModel> {
    arguments
        .get("questions")
        .and_then(serde_json::Value::as_array)
        .map(|questions| {
            questions
                .iter()
                .filter_map(|question| {
                    Some(QuestionModel {
                        header: question.get("header")?.as_str()?.to_owned(),
                        question: question.get("question")?.as_str()?.to_owned(),
                        options: question
                            .get("options")
                            .and_then(serde_json::Value::as_array)
                            .map(|options| {
                                options
                                    .iter()
                                    .filter_map(|option| {
                                        Some(QuestionOptionModel {
                                            label: option.get("label")?.as_str()?.to_owned(),
                                            description: option
                                                .get("description")
                                                .and_then(serde_json::Value::as_str)
                                                .map(ToOwned::to_owned),
                                        })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                        multi_select: question
                            .get("multi_select")
                            .or_else(|| question.get("multiSelect"))
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false),
                    })
                })
                .collect()
        })
        .filter(|questions: &Vec<_>| !questions.is_empty())
        .unwrap_or_else(|| {
            vec![QuestionModel {
                header: "Question".to_owned(),
                question: "Mitsuro is waiting for your input.".to_owned(),
                options: Vec::new(),
                multi_select: false,
            }]
        })
}
