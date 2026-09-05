use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use mitsuro_core::agent::LoopInput;
use mitsuro_core::ai::types::Content;
use mitsuro_core::storage::{HiveRunExecutionModeV1, HiveRunKind};
use mitsuro_server::hive_execution_host::HiveExecutionHost;
use mitsuro_server::types::AgenticEvent;
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex};

use crate::{ExecutionBackend, ExecutionControl, ExecutionOutcome, ExecutionRequest};

pub struct MitsuroExecutionBackend {
    host: Arc<HiveExecutionHost>,
    approval_waiters: Mutex<HashMap<ApprovalKey, oneshot::Sender<bool>>>,
    approval_ack_timeout: Duration,
}

const DEFAULT_APPROVAL_ACK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ApprovalKey {
    session_id: String,
    run_id: String,
    tool_call_id: String,
}

impl MitsuroExecutionBackend {
    pub fn new(host: Arc<HiveExecutionHost>) -> Self {
        Self {
            host,
            approval_waiters: Mutex::new(HashMap::new()),
            approval_ack_timeout: DEFAULT_APPROVAL_ACK_TIMEOUT,
        }
    }

    async fn register_approval_waiter(&self, key: ApprovalKey) -> oneshot::Receiver<bool> {
        let (sender, receiver) = oneshot::channel();
        // Delivery is scheduler-serialized. Replacing a prior sender closes a
        // stale attempt rather than allowing two callers to claim one ack.
        self.approval_waiters.lock().await.insert(key, sender);
        receiver
    }

    async fn remove_approval_waiter(&self, key: &ApprovalKey) {
        self.approval_waiters.lock().await.remove(key);
    }

    async fn acknowledge_approval_event(
        &self,
        session_id: &str,
        run_id: &str,
        event: &AgenticEvent,
    ) {
        let (tool_call_id, approved) = match event {
            AgenticEvent::ToolApproved { id } => (id, true),
            AgenticEvent::ToolDenied { id } => (id, false),
            _ => return,
        };
        let key = ApprovalKey {
            session_id: session_id.to_string(),
            run_id: run_id.to_string(),
            tool_call_id: tool_call_id.clone(),
        };
        if let Some(waiter) = self.approval_waiters.lock().await.remove(&key) {
            let _ = waiter.send(approved);
        }
    }
}

#[derive(Default)]
struct TerminalState {
    finish_reason: Option<String>,
    error: Option<String>,
    sleeping: Option<(chrono::DateTime<Utc>, String)>,
    awaiting_input: Option<Value>,
    observed_event: bool,
}

impl TerminalState {
    fn observe(&mut self, event: &AgenticEvent) {
        self.observed_event = true;
        match event {
            AgenticEvent::AgentSleeping {
                duration_secs,
                reason,
            } => {
                let seconds = i64::try_from(*duration_secs).unwrap_or(i64::MAX);
                let wake_at = Utc::now()
                    .checked_add_signed(ChronoDuration::seconds(seconds))
                    .unwrap_or_else(Utc::now);
                self.sleeping = Some((wake_at, reason.clone()));
            }
            AgenticEvent::AwaitingInput {
                tool_call_id,
                tool_name,
            } => {
                self.awaiting_input = Some(json!({
                    "tool_call_id": tool_call_id,
                    "tool_name": tool_name,
                }));
            }
            AgenticEvent::Error { error } => self.error = Some(error.clone()),
            AgenticEvent::Finish { stop_reason, .. } => {
                self.finish_reason = Some(stop_reason.clone());
            }
            _ => {}
        }
    }

    fn into_outcome(
        self,
        completion: std::result::Result<(), String>,
        externally_committed_worker_run: bool,
    ) -> ExecutionOutcome {
        if self.finish_reason.as_deref() == Some("user_abort") {
            return ExecutionOutcome::Cancelled {
                reason: "agent execution was cancelled".to_string(),
            };
        }
        if let Err(error) = completion {
            if externally_committed_worker_run
                && self.finish_reason.is_none()
                && error.contains("requires fenced reconciliation")
            {
                return ExecutionOutcome::RecoveryRequired { reason: error };
            }
            if self.finish_reason.is_none()
                && (self.observed_event
                    || error.contains("event stream ended before LoopEvent::Finished"))
            {
                return ExecutionOutcome::RecoveryRequired {
                    reason: format!(
                        "agent execution ended without a terminal event; external side effects are uncertain: {error}"
                    ),
                };
            }
            let error = self.error.unwrap_or(error);
            return ExecutionOutcome::Failed {
                retryable: transient_execution_error(&error),
                error,
                retry_after: None,
            };
        }

        match self.finish_reason.as_deref() {
            Some("completed" | "pinched") => ExecutionOutcome::Succeeded {
                output: json!({"stop_reason": self.finish_reason}),
            },
            Some("sleeping") => match self.sleeping {
                Some((wake_at, reason)) => ExecutionOutcome::Sleeping {
                    wake_at,
                    reason: Some(reason),
                },
                None => ExecutionOutcome::RecoveryRequired {
                    reason: "agent entered sleeping state without durable wake metadata".into(),
                },
            },
            Some("awaiting_input") if externally_committed_worker_run => {
                ExecutionOutcome::RecoveryRequired {
                    reason: self.error.unwrap_or_else(|| {
                        "fenced Worker execution requires recovery; it has no live user-input boundary"
                            .to_string()
                    }),
                }
            }
            Some("awaiting_input") => ExecutionOutcome::AwaitingInput {
                details: self
                    .awaiting_input
                    .unwrap_or_else(|| json!({"reason": "agent requested user input"})),
            },
            Some("provider_error" | "stream_idle_timeout") => ExecutionOutcome::Failed {
                error: self
                    .error
                    .unwrap_or_else(|| format!("agent stopped: {:?}", self.finish_reason)),
                retryable: true,
                retry_after: None,
            },
            Some(reason) => ExecutionOutcome::Failed {
                error: self
                    .error
                    .unwrap_or_else(|| format!("agent stopped: {reason}")),
                retryable: false,
                retry_after: None,
            },
            None => ExecutionOutcome::RecoveryRequired {
                reason: self.error.unwrap_or_else(|| {
                    "agent event stream ended without a terminal event; external side effects are uncertain"
                        .into()
                }),
            },
        }
    }
}

fn transient_execution_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    const DETERMINISTIC: &[&str] = &[
        "no ai credentials configured",
        "session not found",
        "not a hive session",
        "event payload is",
        "invalid steering content",
        "invalid hive execution claim",
        "claimed hive run",
        "claimed hive workspace",
        "claimed hive permission_mode",
        "hive execution fence",
        "execution spec does not match",
        "credential snapshot could not be reloaded",
    ];
    if DETERMINISTIC
        .iter()
        .any(|marker| normalized.contains(marker))
    {
        return false;
    }

    // Unknown failures are not automatically retryable. Retrying only known
    // transient classes prevents deterministic configuration/invariant bugs
    // from silently consuming every durable attempt.
    const TRANSIENT: &[&str] = &[
        "session is busy",
        "already executing",
        "connection reset",
        "connection refused",
        "temporarily unavailable",
        "service unavailable",
        "transport error",
        "network error",
        "rate limit",
        "timed out",
        "timeout",
        "http 429",
        "http 502",
        "http 503",
        "http 504",
    ];
    TRANSIENT.iter().any(|marker| normalized.contains(marker))
}

fn worker_goal_acceptance_execution_forbidden(
    kind: HiveRunKind,
    mode: Option<&HiveRunExecutionModeV1>,
) -> bool {
    kind == HiveRunKind::WorkerWorkflowAcceptance
        || mode
            .is_some_and(|mode| matches!(mode, HiveRunExecutionModeV1::WorkerGoalAcceptance { .. }))
}

#[async_trait]
impl ExecutionBackend for MitsuroExecutionBackend {
    async fn execute(&self, request: ExecutionRequest) -> ExecutionOutcome {
        if worker_goal_acceptance_execution_forbidden(
            request.claim.run.kind,
            request
                .claim
                .run
                .execution_context
                .as_ref()
                .map(|context| &context.mode),
        ) {
            return ExecutionOutcome::Failed {
                error: "Worker Workflow acceptance is awaiting owner input and cannot execute"
                    .into(),
                retryable: false,
                retry_after: None,
            };
        }
        let Some(session_id) = request.claim.run.session_id.clone() else {
            return ExecutionOutcome::Failed {
                error: "claimed Hive run has no session".into(),
                retryable: false,
                retry_after: None,
            };
        };
        let run_id = request.claim.run.id.clone();
        let externally_committed_worker_run = request
            .claim
            .run
            .execution_context
            .as_ref()
            .is_some_and(|context| {
                matches!(
                    &context.mode,
                    HiveRunExecutionModeV1::WorkerConversationNeutral { .. }
                        | HiveRunExecutionModeV1::WorkerWorkspaceAttached { .. }
                        | HiveRunExecutionModeV1::WorkerGoal { .. }
                )
            });
        let wake_reason = request.claim.run.kind.to_string();
        let run = match self
            .host
            .start(
                request.claim.clone(),
                request.daemon_instance_id.clone(),
                wake_reason,
            )
            .await
        {
            Ok(run) => run,
            Err(error) => {
                let retryable = error.is_retryable();
                let error = error.to_string();
                return ExecutionOutcome::Failed {
                    retryable,
                    error,
                    retry_after: None,
                };
            }
        };

        let (mut events, mut completion, _execution_guard) = run.into_parts();
        let mut terminal = TerminalState::default();
        let mut completion_result = None;
        let mut events_open = true;
        while completion_result.is_none() || events_open {
            tokio::select! {
                event = events.recv(), if events_open => {
                    match event {
                        Some(event) => {
                            terminal.observe(&event);
                            // An approval outbox entry is acknowledged only
                            // after the agent's canonical inbox consumed the
                            // decision and emitted ToolApproved/ToolDenied.
                            // This happens before event persistence so the
                            // scheduler's mutation gate cannot deadlock the
                            // bounded event sink.
                            self.acknowledge_approval_event(&session_id, &run_id, &event).await;
                            let payload = match serde_json::to_value(event) {
                                Ok(payload) => payload,
                                Err(error) => {
                                    self.host.abort(&session_id, Some(&run_id)).await;
                                    return ExecutionOutcome::RecoveryRequired {
                                        reason: format!(
                                            "agent event could not be durably encoded; side effects are uncertain: {error}"
                                        ),
                                    };
                                }
                            };
                            if let Err(error) = request.events.agentic(payload).await {
                                self.host.abort(&session_id, Some(&run_id)).await;
                                return ExecutionOutcome::RecoveryRequired {
                                    reason: format!(
                                        "agent event could not be durably emitted; side effects are uncertain: {error}"
                                    ),
                                };
                            }
                        }
                        None => events_open = false,
                    }
                }
                result = &mut completion, if completion_result.is_none() => {
                    completion_result = Some(result.unwrap_or_else(|_| {
                        Err("Hive execution completion channel closed".to_string())
                    }));
                }
            }
        }
        terminal.into_outcome(
            completion_result
                .unwrap_or_else(|| Err("Hive execution completed without a result".to_string())),
            externally_committed_worker_run,
        )
    }

    async fn control(&self, session_id: &str, control: ExecutionControl) -> anyhow::Result<()> {
        match control {
            // Start is a scheduler wake-up hint. The durable pump owns actual
            // execution and will call `execute` after successfully claiming.
            ExecutionControl::Start => Ok(()),
            ExecutionControl::Message { message, persisted } => {
                let content = vec![Content::text(message)];
                let input = if persisted {
                    LoopInput::PersistedUserMessage { content }
                } else {
                    LoopInput::Steer {
                        pending_id: None,
                        content,
                    }
                };
                self.host.send_input(session_id, input).await
            }
            ExecutionControl::Steer {
                pending_id,
                content,
            } => {
                let content: Vec<Content> = serde_json::from_value(content)
                    .map_err(|error| anyhow::anyhow!("invalid steering content: {error}"))?;
                self.host
                    .send_input(
                        session_id,
                        LoopInput::Steer {
                            pending_id,
                            content,
                        },
                    )
                    .await
            }
            ExecutionControl::ToolApproval {
                run_id,
                tool_call_id,
                approved,
            } => {
                let key = ApprovalKey {
                    session_id: session_id.to_string(),
                    run_id: run_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                };
                let acknowledgement = self.register_approval_waiter(key.clone()).await;
                if let Err(error) = self
                    .host
                    .send_input_for_run(
                        session_id,
                        &run_id,
                        LoopInput::ToolApproval {
                            tool_call_id,
                            approved,
                        },
                    )
                    .await
                {
                    self.remove_approval_waiter(&key).await;
                    return Err(error);
                }
                match tokio::time::timeout(self.approval_ack_timeout, acknowledgement).await {
                    Ok(Ok(observed)) if observed == approved => Ok(()),
                    Ok(Ok(_)) => {
                        self.remove_approval_waiter(&key).await;
                        anyhow::bail!("Hive Worker consumed the opposite approval decision")
                    }
                    Ok(Err(_)) => anyhow::bail!("Hive approval acknowledgement channel closed"),
                    Err(_) => {
                        self.remove_approval_waiter(&key).await;
                        anyhow::bail!("Hive Worker did not consume the approval before timeout")
                    }
                }
            }
            ExecutionControl::UserResponse {
                run_id,
                pending_id,
                tool_call_id,
                response,
            } => {
                self.host
                    .send_input_for_run(
                        session_id,
                        &run_id,
                        LoopInput::Steer {
                            pending_id: Some(pending_id),
                            content: vec![Content::text(format!(
                                "Response to {tool_call_id}:\n{response}"
                            ))],
                        },
                    )
                    .await
            }
            ExecutionControl::Cancel { reason } => {
                tracing::info!(session_id, reason, "Cancelling hosted Hive execution");
                self.host.cancel(session_id, None).await
            }
            ExecutionControl::CancelRun { run_id, reason } => {
                tracing::info!(
                    session_id,
                    run_id,
                    reason,
                    "Cancelling exact hosted Hive execution"
                );
                self.host.cancel(session_id, Some(&run_id)).await
            }
            ExecutionControl::AbortRun { run_id, reason } => {
                tracing::warn!(
                    session_id,
                    run_id,
                    reason,
                    "Aborting exact hosted Hive execution after cancellation grace elapsed"
                );
                self.host.abort(session_id, Some(&run_id)).await;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mitsuro_core::storage::HiveRunKind;
    use mitsuro_server::types::AgenticEvent;

    use crate::ExecutionOutcome;

    use super::{
        transient_execution_error, worker_goal_acceptance_execution_forbidden, TerminalState,
    };

    #[test]
    fn worker_goal_acceptance_is_not_an_executable_backend_kind() {
        assert!(worker_goal_acceptance_execution_forbidden(
            HiveRunKind::WorkerWorkflowAcceptance,
            None,
        ));
        assert!(!worker_goal_acceptance_execution_forbidden(
            HiveRunKind::WorkerWorkflow,
            None,
        ));
    }

    #[test]
    fn deterministic_execution_errors_do_not_retry() {
        for error in [
            "invalid Hive execution claim: claimed Hive run has no explicit model",
            "invalid Hive execution claim: every claimed Hive workspace path must be absolute",
            "Hive execution fence rejected: Hive execution fence is no longer current",
            "invalid steering content: expected a sequence",
            "No AI credentials configured",
            "Hive credential snapshot could not be reloaded: malformed credentials file",
        ] {
            assert!(
                !transient_execution_error(error),
                "unexpected retry: {error}"
            );
        }
    }

    #[test]
    fn dropped_stream_after_tool_event_requires_manual_recovery() {
        let mut terminal = TerminalState::default();
        terminal.observe(&AgenticEvent::ToolResult {
            id: "tool-1".into(),
            output: "side effect may have committed".into(),
            is_error: false,
        });

        let outcome = terminal.into_outcome(
            Err(
                "agent event stream ended before LoopEvent::Finished; external side effects are uncertain"
                    .into(),
            ),
            false,
        );
        assert!(matches!(outcome, ExecutionOutcome::RecoveryRequired { .. }));
    }

    #[test]
    fn neutral_worker_attention_requires_recovery_instead_of_live_input() {
        let mut terminal = TerminalState::default();
        terminal.observe(&AgenticEvent::Error {
            error: "provider call remains Started without a canonical response".into(),
        });
        terminal.observe(&AgenticEvent::Finish {
            session_id: "worker-dm".into(),
            stop_reason: "awaiting_input".into(),
        });

        let outcome = terminal.into_outcome(Ok(()), true);
        assert!(matches!(
            outcome,
            ExecutionOutcome::RecoveryRequired { reason }
                if reason.contains("provider call remains Started")
        ));
    }

    #[test]
    fn worker_goal_wall_timeout_requires_fenced_recovery() {
        let terminal = TerminalState::default();
        let outcome = terminal.into_outcome(
            Err(
                "Worker Workflow exceeded its frozen wall-time budget; outcome requires fenced reconciliation"
                    .into(),
            ),
            true,
        );
        assert!(matches!(
            outcome,
            ExecutionOutcome::RecoveryRequired { reason }
                if reason.contains("requires fenced reconciliation")
        ));
    }

    #[test]
    fn recognized_transient_execution_errors_retry() {
        for error in [
            "provider connection reset by peer",
            "provider request timed out",
            "HTTP 503 service unavailable",
            "session is busy",
        ] {
            assert!(transient_execution_error(error), "expected retry: {error}");
        }
    }
}
