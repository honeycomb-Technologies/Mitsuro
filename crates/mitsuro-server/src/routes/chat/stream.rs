use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::{Event, Sse};
use tokio::sync::mpsc::WeakSender;
use tokio::sync::{mpsc, Mutex, OwnedMutexGuard, RwLock};
use tokio_stream::wrappers::ReceiverStream;

use mitsuro_core::agent::{
    DelegatedProgressEvent, DelegatedRunStage as CoreDelegatedRunStage, LoopEvent, LoopInput,
    OrchestratorServices, RunProvenance, RunSpecBuilder,
};
use mitsuro_core::ai::transport_policy::StreamTransportPolicy;
use mitsuro_core::storage::{
    Database, DelegatedRunAgentSnapshot, DelegatedRunSnapshot, DelegatedRunStore, DelegationStore,
    DelegationTaskActivity, SessionType, WorkMode,
};
use mitsuro_core::tools::registry::PermissionMode;
use mitsuro_core::SessionManager;

use super::stream_notify::ChatStreamRunOutcome;
use super::{ChatSessionContext, SSE_CHANNEL_BUFFER};
use crate::error::AppError;
use crate::routes::sse::event_to_sse;
use crate::types::{
    AgenticEvent, DelegatedAgentStateResponse, DelegatedRunStage, DelegatedToolStateResponse,
};
use crate::AppState;

const DELEGATION_EVENT_LIVE_PAGE_LIMIT: usize = 128;
// ── Orchestrator → SSE bridge ────────────────────────────────────────

fn loop_event_requires_delivery(event: &LoopEvent) -> bool {
    matches!(
        event,
        LoopEvent::AwaitingInput { .. }
            | LoopEvent::SteeringInjected { .. }
            | LoopEvent::ToolApprovalRequired { .. }
            | LoopEvent::PlanComplete { .. }
            | LoopEvent::AgentSleeping { .. }
            | LoopEvent::UserMessage { .. }
            | LoopEvent::ClassifierDecision { .. }
            | LoopEvent::RunBudgetResolved { .. }
            | LoopEvent::ProviderRequestPrepared { .. }
            | LoopEvent::MicrocompactionApplied { .. }
            | LoopEvent::ProgressGuard { .. }
            | LoopEvent::Usage { .. }
            | LoopEvent::Finished { .. }
            | LoopEvent::Error { .. }
    )
}

pub(super) async fn forward_loop_event(
    sse_tx: &mpsc::Sender<Result<Event, Infallible>>,
    session_id: &str,
    loop_event: LoopEvent,
    skipped_events: &mut usize,
) -> bool {
    let requires_delivery = loop_event_requires_delivery(&loop_event);
    crate::routes::sse::forward_sse_event(
        sse_tx,
        session_id,
        loop_event.into(),
        requires_delivery,
        skipped_events,
        "Dropping SSE event because client queue is full",
    )
    .await
}

pub(super) async fn start_orchestrator_sse(
    state: &AppState,
    ctx: ChatSessionContext,
    work_mode: WorkMode,
    permission_mode: PermissionMode,
    generate_title: bool,
) -> Result<Sse<ReceiverStream<Result<Event, Infallible>>>, AppError> {
    let (sse_tx, sse_rx) = mpsc::channel::<Result<Event, Infallible>>(SSE_CHANNEL_BUFFER);
    let run = start_chat_run(state, ctx, work_mode, permission_mode, generate_title)?;
    launch_chat_run_bridge(state, run, sse_tx).await;

    Ok(crate::routes::sse::sse_response(sse_rx))
}

/// Resume an idle Chat/Code session without an attached SSE client while
/// retaining the normal run registration, notification, trace, cleanup, and
/// session-lock lifecycle.
pub(super) async fn start_orchestrator_detached(
    state: &AppState,
    ctx: ChatSessionContext,
    work_mode: WorkMode,
    permission_mode: PermissionMode,
) -> Result<(), AppError> {
    let run = start_chat_run(state, ctx, work_mode, permission_mode, false)?;
    let (sse_tx, sse_rx) = mpsc::channel::<Result<Event, Infallible>>(1);
    drop(sse_rx);
    launch_chat_run_bridge(state, run, sse_tx).await;
    Ok(())
}

struct StartedChatRun {
    event_rx: mpsc::UnboundedReceiver<LoopEvent>,
    delegated_progress_rx: mpsc::UnboundedReceiver<DelegatedProgressEvent>,
    input_tx: mpsc::UnboundedSender<LoopInput>,
    session_id: String,
    /// Durable delegation watermark captured before the orchestrator starts.
    /// Events created by this run therefore have IDs strictly above it.
    delegation_event_cursor: Option<i64>,
    user_id: Option<String>,
    guard: OwnedMutexGuard<()>,
}

fn start_chat_run(
    state: &AppState,
    ctx: ChatSessionContext,
    work_mode: WorkMode,
    permission_mode: PermissionMode,
    generate_title: bool,
) -> Result<StartedChatRun, AppError> {
    if ctx.session_type == SessionType::Hive {
        return Err(AppError::BadGateway(
            "Hive execution is owned by its background service".to_string(),
        ));
    }

    let delegation_event_cursor =
        delegation_event_cursor_at_run_start(state.db_path.as_ref(), &ctx.session_id);
    let stream_idle_timeout = model_stream_idle_timeout(&ctx.ai_client);
    let mode_aware_code_tools =
        ctx.session_type == SessionType::Code && ctx.options.tools.is_some();
    let (delegated_progress_tx, delegated_progress_rx) = mpsc::unbounded_channel();

    let run_spec = RunSpecBuilder::new(
        RunProvenance::Server,
        ctx.session_id.clone(),
        ctx.working_dir,
        ctx.session_type,
    )
    .project_dir(ctx.project_dir)
    .hive_crew_slug(ctx.hive_crew_slug.clone())
    .permission_mode(permission_mode)
    .execution_tool_allowlist(ctx.execution_tool_allowlist)
    .user_id(ctx.user_id.clone())
    .initial_work_mode(work_mode)
    .mode_aware_code_tools(mode_aware_code_tools)
    .stream_idle_timeout(stream_idle_timeout)
    .generate_title(generate_title)
    .delegated_progress_tx(Some(delegated_progress_tx))
    .call_options(ctx.options)
    .build(ctx.ai_client.as_ref())
    .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let services = OrchestratorServices {
        ai_client: ctx.ai_client,
        tool_registry: Arc::clone(&state.tool_registry),
        process_registry: Arc::clone(&state.process_registry),
        db_path: (*state.db_path).clone(),
        skills_manager: Arc::clone(&state.skills_manager),
    };

    let (event_rx, input_tx) = run_spec.start(services, ctx.conversation);

    Ok(StartedChatRun {
        event_rx,
        delegated_progress_rx,
        input_tx,
        session_id: ctx.session_id,
        delegation_event_cursor,
        user_id: ctx.user_id,
        guard: ctx.guard,
    })
}

fn delegation_event_cursor_at_run_start(
    db_path: &std::path::Path,
    session_id: &str,
) -> Option<i64> {
    let result = (|| -> anyhow::Result<Option<i64>> {
        let store = DelegationStore::new(Database::new(db_path)?);
        Ok(Some(
            store
                .list_latest_session_events(session_id, 1)?
                .last()
                .map(|event| event.event_id)
                .unwrap_or(0),
        ))
    })();
    match result {
        Ok(cursor) => cursor,
        Err(error) => {
            tracing::warn!(
                session_id,
                %error,
                "Live delegation events disabled because the run-start watermark could not load"
            );
            None
        }
    }
}

async fn launch_chat_run_bridge(
    state: &AppState,
    run: StartedChatRun,
    sse_tx: mpsc::Sender<Result<Event, Infallible>>,
) {
    let StartedChatRun {
        event_rx,
        delegated_progress_rx,
        input_tx,
        session_id,
        delegation_event_cursor,
        user_id,
        guard,
    } = run;
    {
        let mut inputs = state.session_inputs.write().await;
        inputs.insert(session_id.clone(), input_tx);
    }

    let session_inputs = Arc::clone(&state.session_inputs);
    let active_agent_streams = Arc::clone(&state.active_agent_streams);
    let push_service = state.push_service.clone();
    let apns_service = state.apns_service.clone();
    let db_path = Arc::clone(&state.db_path);
    let delegated_state = Arc::clone(&state.delegated_state);
    let delegated_session_id = session_id.clone();
    let delegated_db_path = Arc::clone(&state.db_path);
    let delegated_sse_tx = sse_tx.downgrade();
    let delegated_sse_open = Arc::new(Mutex::new(true));
    let agent_stream_generation = state
        .agent_streams
        .write()
        .await
        .begin_generation(&session_id);
    let agent_streams = Arc::clone(&state.agent_streams);

    tokio::spawn(run_delegated_progress_bridge(
        delegated_progress_rx,
        delegated_sse_tx,
        Arc::clone(&delegated_sse_open),
        delegated_session_id,
        delegated_state,
        delegated_db_path,
        delegation_event_cursor,
    ));

    tokio::spawn(async move {
        active_agent_streams.fetch_add(1, Ordering::Relaxed);
        let _guard = guard;
        run_orchestrator_event_bridge(
            event_rx,
            sse_tx,
            session_id,
            session_inputs,
            push_service,
            apns_service,
            user_id,
            db_path,
            Some((agent_streams, agent_stream_generation)),
            delegated_sse_open,
        )
        .await;
        active_agent_streams.fetch_sub(1, Ordering::Relaxed);
    });
}

fn delegated_stage_is_terminal(stage: CoreDelegatedRunStage) -> bool {
    matches!(
        stage,
        CoreDelegatedRunStage::Complete
            | CoreDelegatedRunStage::Degraded
            | CoreDelegatedRunStage::Failed
            | CoreDelegatedRunStage::Cancelled
    )
}

/// A child-level terminal progress update is not necessarily terminal for its
/// parent delegated run. In particular, parallel builders each emit a terminal
/// status before the aggregate artifact is finalized. The durable run row is
/// the authority for whether the live aggregate can leave the active state.
fn normalize_delegated_terminal_stage(
    durable: Option<&DelegatedRunStore>,
    mut event: DelegatedProgressEvent,
) -> Option<DelegatedProgressEvent> {
    if !delegated_stage_is_terminal(event.stage) {
        return Some(event);
    }

    let Some(durable) = durable else {
        event.stage = CoreDelegatedRunStage::Running;
        return Some(event);
    };
    let record = match durable.get_run(&event.delegated_run_id) {
        Ok(record) => record,
        Err(error) => {
            tracing::warn!(
                delegated_run_id = %event.delegated_run_id,
                %error,
                "Could not load durable delegated run while normalizing live progress"
            );
            event.stage = CoreDelegatedRunStage::Running;
            return Some(event);
        }
    };

    let Some(record) = record else {
        // A server-orchestrated Agent normally has a durable row. If creation
        // failed or this is legacy traffic, keep the aggregate active until
        // the outer tool result or sender closure provides a safe boundary.
        event.stage = CoreDelegatedRunStage::Running;
        return Some(event);
    };
    let tool_call_matches = record
        .parent_tool_call_id
        .as_deref()
        .map(|tool_call_id| tool_call_id == event.tool_call_id)
        .unwrap_or(true);
    if record.parent_session_id != event.parent_session_id || !tool_call_matches {
        tracing::warn!(
            delegated_run_id = %event.delegated_run_id,
            event_session_id = %event.parent_session_id,
            durable_session_id = %record.parent_session_id,
            "Ignoring delegated progress whose durable ownership does not match"
        );
        return None;
    }

    if delegated_stage_is_terminal(record.stage) {
        event.stage = record.stage;
    } else {
        event.stage = CoreDelegatedRunStage::Running;
    }
    Some(event)
}

fn delegated_stage_rank(stage: DelegatedRunStage) -> u8 {
    match stage {
        DelegatedRunStage::Created => 0,
        DelegatedRunStage::Running => 1,
        DelegatedRunStage::Synthesizing => 2,
        DelegatedRunStage::Complete
        | DelegatedRunStage::Degraded
        | DelegatedRunStage::Failed
        | DelegatedRunStage::Cancelled => 3,
    }
}

fn core_delegated_stage(stage: DelegatedRunStage) -> CoreDelegatedRunStage {
    match stage {
        DelegatedRunStage::Created => CoreDelegatedRunStage::Created,
        DelegatedRunStage::Running => CoreDelegatedRunStage::Running,
        DelegatedRunStage::Synthesizing => CoreDelegatedRunStage::Synthesizing,
        DelegatedRunStage::Complete => CoreDelegatedRunStage::Complete,
        DelegatedRunStage::Degraded => CoreDelegatedRunStage::Degraded,
        DelegatedRunStage::Failed => CoreDelegatedRunStage::Failed,
        DelegatedRunStage::Cancelled => CoreDelegatedRunStage::Cancelled,
    }
}

fn remove_delegated_snapshot(
    state: &mut crate::DelegatedStateMap,
    session_id: &str,
    delegated_run_id: &str,
    tool_call_id: &str,
) {
    let remove_session = if let Some(tools) = state.get_mut(session_id) {
        tools.retain(|tool| {
            tool.delegated_run_id != delegated_run_id || tool.tool_call_id != tool_call_id
        });
        tools.is_empty()
    } else {
        false
    };
    if remove_session {
        state.remove(session_id);
    }
}

/// Apply one non-terminal progress update to the reconnect snapshot. Terminal
/// runs are removed instead: their artifact in `delegated_runs` is the reload
/// authority, while this map intentionally contains live work only.
fn apply_delegated_progress_snapshot(
    state: &mut crate::DelegatedStateMap,
    event: &DelegatedProgressEvent,
) -> Option<DelegatedToolStateResponse> {
    if delegated_stage_is_terminal(event.stage) {
        remove_delegated_snapshot(
            state,
            &event.parent_session_id,
            &event.delegated_run_id,
            &event.tool_call_id,
        );
        return None;
    }

    let stage = DelegatedRunStage::from(event.stage);
    let tools = state.entry(event.parent_session_id.clone()).or_default();
    let tool = if let Some(index) = tools.iter().position(|tool| {
        tool.delegated_run_id == event.delegated_run_id && tool.tool_call_id == event.tool_call_id
    }) {
        &mut tools[index]
    } else {
        tools.push(DelegatedToolStateResponse {
            delegated_run_id: event.delegated_run_id.clone(),
            tool_call_id: event.tool_call_id.clone(),
            kind: event.kind.into(),
            stage,
            parent_session_id: Some(event.parent_session_id.clone()),
            agents: Vec::new(),
        });
        tools
            .last_mut()
            .expect("delegated snapshot was just inserted")
    };

    if delegated_stage_rank(stage) >= delegated_stage_rank(tool.stage) {
        tool.stage = stage;
    }
    tool.kind = event.kind.into();
    tool.parent_session_id = Some(event.parent_session_id.clone());

    let agent = DelegatedAgentStateResponse {
        task_id: event.progress.task_id.clone(),
        agent_name: event.progress.name.clone(),
        status: crate::types::DelegatedProgressStatus::from_progress(
            &event.progress.status,
            event.stage,
        ),
        tool_count: event.progress.tool_count,
        tokens: event.progress.tokens,
        current_action: event.progress.current_action.clone(),
        completion_summary: event.progress.completion_summary.clone(),
        lines_added: event.progress.lines_added,
        lines_removed: event.progress.lines_removed,
        completed_plan_task: event.progress.completed_plan_task.clone(),
    };
    if let Some(index) = tool
        .agents
        .iter()
        .position(|existing| existing.task_id == agent.task_id)
    {
        tool.agents[index] = agent;
    } else {
        tool.agents.push(agent);
    }
    Some(tool.clone())
}

fn delegated_progress_status_label(status: crate::types::DelegatedProgressStatus) -> &'static str {
    match status {
        crate::types::DelegatedProgressStatus::Created => "created",
        crate::types::DelegatedProgressStatus::Queued => "queued",
        crate::types::DelegatedProgressStatus::Leased => "leased",
        crate::types::DelegatedProgressStatus::Running => "running",
        crate::types::DelegatedProgressStatus::Retrying => "retrying",
        crate::types::DelegatedProgressStatus::Complete => "complete",
        crate::types::DelegatedProgressStatus::Degraded => "degraded",
        crate::types::DelegatedProgressStatus::Cancelled => "cancelled",
        crate::types::DelegatedProgressStatus::Failed => "failed",
    }
}

fn persist_delegated_progress_snapshot(
    durable: Option<&DelegatedRunStore>,
    event: &DelegatedProgressEvent,
    tool: &DelegatedToolStateResponse,
) {
    let Some(durable) = durable else {
        return;
    };
    let stage = core_delegated_stage(tool.stage);
    let snapshot = DelegatedRunSnapshot {
        stage,
        agents: tool
            .agents
            .iter()
            .map(|agent| DelegatedRunAgentSnapshot {
                task_id: agent.task_id.clone(),
                agent_name: agent.agent_name.clone(),
                status: delegated_progress_status_label(agent.status).to_string(),
                tool_count: agent.tool_count,
                tokens: agent.tokens,
                current_action: agent.current_action.clone(),
                completion_summary: agent.completion_summary.clone(),
                lines_added: agent.lines_added,
                lines_removed: agent.lines_removed,
                completed_plan_task: agent.completed_plan_task.clone(),
            })
            .collect(),
    };
    if let Err(error) = durable.update_snapshot(&event.delegated_run_id, stage, &snapshot) {
        tracing::warn!(
            delegated_run_id = %event.delegated_run_id,
            %error,
            "Failed to persist delegated live-progress snapshot"
        );
    }
}

fn persist_delegated_task_activity(
    durable: Option<&DelegationStore>,
    event: &DelegatedProgressEvent,
) {
    let Some(durable) = durable else {
        return;
    };
    // Canonical task-graph children use group-owned task IDs. Compatibility
    // single-child runs have no delegation_tasks row and retain their existing
    // snapshot-only presentation.
    if !event
        .progress
        .task_id
        .starts_with(&format!("{}:task:", event.delegated_run_id))
    {
        return;
    }
    if let Some(conversation_event) = event.progress.conversation_event.as_ref() {
        if let Err(error) = durable.record_task_conversation(
            &event.delegated_run_id,
            &event.progress.task_id,
            conversation_event,
        ) {
            tracing::warn!(
                delegated_run_id = %event.delegated_run_id,
                delegation_task_id = %event.progress.task_id,
                %error,
                "Failed to persist child conversation event"
            );
        }
        return;
    }
    let status =
        crate::types::DelegatedProgressStatus::from_progress(&event.progress.status, event.stage);
    let activity = DelegationTaskActivity {
        agent_name: event.progress.name.clone(),
        status: delegated_progress_status_label(status).to_string(),
        tool_count: event.progress.tool_count,
        tokens: event.progress.tokens,
        current_action: event.progress.current_action.clone(),
        completion_summary: event.progress.completion_summary.clone(),
        lines_added: event.progress.lines_added,
        lines_removed: event.progress.lines_removed,
        completed_plan_task: event.progress.completed_plan_task.clone(),
    };
    if let Err(error) =
        durable.record_task_activity(&event.delegated_run_id, &event.progress.task_id, &activity)
    {
        tracing::warn!(
            delegated_run_id = %event.delegated_run_id,
            delegation_task_id = %event.progress.task_id,
            %error,
            "Failed to persist delegated task activity"
        );
    }
}

async fn forward_delegated_progress(
    sse_tx: &WeakSender<Result<Event, Infallible>>,
    sse_open: &Arc<Mutex<bool>>,
    session_id: &str,
    event: DelegatedProgressEvent,
    skipped_events: &mut usize,
) -> bool {
    // The gate is held across forwarding: delegated progress that acquires it
    // first is delivered before a concurrent Finish closes the boundary.
    let sse_open = sse_open.lock().await;
    if !*sse_open {
        return false;
    }
    let Some(sse_tx) = sse_tx.upgrade() else {
        return false;
    };
    let requires_delivery = delegated_stage_is_terminal(event.stage);
    let agentic_event = AgenticEvent::delegated_progress(event);
    crate::routes::sse::forward_sse_event(
        &sse_tx,
        session_id,
        agentic_event,
        requires_delivery,
        skipped_events,
        "Dropping delegated SSE progress because client queue is full",
    )
    .await
}

/// Forward a bounded page from the canonical append-only event stream.
///
/// These sends are deliberately best-effort: the event is already durable and
/// the session-state endpoint replays `event_id > cursor`. If the SSE queue is
/// full, a Lagged marker tells the client to retain its older replay cursor;
/// blocking the agent loop would invert that authority relationship.
async fn forward_durable_delegation_events(
    sse_tx: &WeakSender<Result<Event, Infallible>>,
    sse_open: &Arc<Mutex<bool>>,
    session_id: &str,
    durable: &mut DelegationStore,
    cursor: &mut i64,
    skipped_events: &mut usize,
) -> bool {
    let events = match durable.list_session_events_after(
        session_id,
        *cursor,
        DELEGATION_EVENT_LIVE_PAGE_LIMIT,
    ) {
        Ok(events) => events,
        Err(error) => {
            tracing::warn!(
                session_id,
                after_event_id = *cursor,
                %error,
                "Could not read durable delegation events for live delivery"
            );
            return true;
        }
    };
    if events.is_empty() {
        return true;
    }

    if !*sse_open.lock().await {
        return false;
    }
    let Some(sse_tx) = sse_tx.upgrade() else {
        return false;
    };

    for event in events {
        // Advance the bridge scan watermark even when the live optimization
        // drops an event. The client detects the gap and replays from its own
        // independently tracked contiguous cursor.
        *cursor = event.event_id;

        if *skipped_events > 0 {
            let lagged = AgenticEvent::Lagged {
                skipped: *skipped_events,
            };
            if let Some(sse_event) = event_to_sse(&lagged) {
                match sse_tx.try_send(Ok(sse_event)) {
                    Ok(()) => *skipped_events = 0,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        *skipped_events = skipped_events.saturating_add(1);
                        continue;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return false,
                }
            }
        }

        let Some(sse_event) = event_to_sse(&AgenticEvent::DelegationEvent { event }) else {
            continue;
        };
        match sse_tx.try_send(Ok(sse_event)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                *skipped_events = skipped_events.saturating_add(1);
                tracing::warn!(
                    session_id,
                    skipped = *skipped_events,
                    "Dropping live delegation event because the SSE queue is full"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
        }
    }
    true
}

pub(super) async fn run_delegated_progress_bridge(
    mut progress_rx: mpsc::UnboundedReceiver<DelegatedProgressEvent>,
    sse_tx: WeakSender<Result<Event, Infallible>>,
    sse_open: Arc<Mutex<bool>>,
    session_id: String,
    delegated_state: Arc<RwLock<crate::DelegatedStateMap>>,
    db_path: Arc<std::path::PathBuf>,
    delegation_event_cursor: Option<i64>,
) {
    let durable = match Database::new(db_path.as_ref()) {
        Ok(database) => Some(DelegatedRunStore::new(database)),
        Err(error) => {
            tracing::warn!(
                session_id = %session_id,
                %error,
                "Delegated progress will remain process-local because the durable store could not open"
            );
            None
        }
    };
    let activity = match Database::new(db_path.as_ref()) {
        Ok(database) => Some(DelegationStore::new(database)),
        Err(error) => {
            tracing::warn!(
                session_id = %session_id,
                %error,
                "Delegated task activity will remain process-local"
            );
            None
        }
    };
    let canonical = match (delegation_event_cursor, Database::new(db_path.as_ref())) {
        (Some(cursor), Ok(database)) => Some((DelegationStore::new(database), cursor)),
        (Some(_), Err(error)) => {
            tracing::warn!(
                session_id = %session_id,
                %error,
                "Live delegation events disabled because the durable store could not open"
            );
            None
        }
        (None, _) => None,
    };
    let mut canonical = canonical;
    let mut tracked_snapshots = HashSet::<(String, String)>::new();
    let mut skipped_events = 0usize;
    let mut sse_connected = true;

    while let Some(event) = progress_rx.recv().await {
        if event.parent_session_id != session_id {
            tracing::warn!(
                expected_session_id = %session_id,
                event_session_id = %event.parent_session_id,
                delegated_run_id = %event.delegated_run_id,
                "Ignoring delegated progress from a different parent session"
            );
            continue;
        }
        let Some(mut event) = normalize_delegated_terminal_stage(durable.as_ref(), event) else {
            continue;
        };
        let snapshot_key = (event.delegated_run_id.clone(), event.tool_call_id.clone());
        persist_delegated_task_activity(activity.as_ref(), &event);
        let retained_snapshot = {
            let mut state = delegated_state.write().await;
            apply_delegated_progress_snapshot(&mut state, &event)
        };
        if let Some(ref snapshot) = retained_snapshot {
            tracked_snapshots.insert(snapshot_key.clone());
            event.stage = core_delegated_stage(snapshot.stage);
            persist_delegated_progress_snapshot(durable.as_ref(), &event, snapshot);
        } else {
            tracked_snapshots.remove(&snapshot_key);
        }

        if sse_connected {
            if let Some((store, cursor)) = canonical.as_mut() {
                if !forward_durable_delegation_events(
                    &sse_tx,
                    &sse_open,
                    &session_id,
                    store,
                    cursor,
                    &mut skipped_events,
                )
                .await
                {
                    sse_connected = false;
                    skipped_events = 0;
                }
            }
        }

        if sse_connected
            && !forward_delegated_progress(
                &sse_tx,
                &sse_open,
                &session_id,
                event,
                &mut skipped_events,
            )
            .await
        {
            sse_connected = false;
            skipped_events = 0;
            tracing::info!(
                session_id = %session_id,
                "SSE client disconnected; retaining delegated live-state updates"
            );
        }
    }

    // Group synthesis/finalization follows the final child update. Flush once
    // more after every progress sender has closed so those durable transitions
    // can reach an attached client without waiting for its next state poll.
    if sse_connected {
        if let Some((store, cursor)) = canonical.as_mut() {
            let _ = forward_durable_delegation_events(
                &sse_tx,
                &sse_open,
                &session_id,
                store,
                cursor,
                &mut skipped_events,
            )
            .await;
        }
    }

    if !tracked_snapshots.is_empty() {
        let mut state = delegated_state.write().await;
        for (delegated_run_id, tool_call_id) in tracked_snapshots {
            remove_delegated_snapshot(&mut state, &session_id, &delegated_run_id, &tool_call_id);
        }
    }
}

fn model_stream_idle_timeout(ai_client: &Arc<mitsuro_core::ai::client::AiClient>) -> Duration {
    StreamTransportPolicy::resolve(ai_client.provider_id(), ai_client.config().api_format)
        .idle_timeout
}

pub(super) async fn run_orchestrator_event_bridge(
    mut event_rx: mpsc::UnboundedReceiver<LoopEvent>,
    sse_tx: mpsc::Sender<Result<Event, Infallible>>,
    session_id: String,
    session_inputs: Arc<RwLock<crate::SessionInputMap>>,
    push_service: Option<Arc<crate::push::PushService>>,
    apns_service: Option<Arc<crate::apns::ApnsService>>,
    user_id: Option<String>,
    db_path: Arc<std::path::PathBuf>,
    agent_stream: Option<(Arc<RwLock<crate::agent_streams::AgentStreamHub>>, u64)>,
    delegated_sse_open: Arc<Mutex<bool>>,
) {
    let mut outcome = ChatStreamRunOutcome::default();
    let mut skipped_events = 0usize;
    let mut sse_connected = true;

    while let Some(loop_event) = event_rx.recv().await {
        outcome.record_event(
            &push_service,
            &apns_service,
            user_id.as_deref(),
            &session_id,
            &loop_event,
        );
        let is_finished = matches!(loop_event, LoopEvent::Finished { .. });
        if let Some((stream, generation)) = agent_stream.as_ref() {
            stream.write().await.publish(
                &session_id,
                *generation,
                AgenticEvent::from(loop_event.clone()),
            );
        }

        if is_finished {
            // Serialize the final foreground boundary against delegated sends:
            // progress that acquired the gate first is delivered before Finish;
            // detached progress that arrives later becomes live-state-only.
            *delegated_sse_open.lock().await = false;
        }

        let delivered = if !sse_connected {
            true
        } else {
            forward_loop_event(&sse_tx, &session_id, loop_event, &mut skipped_events).await
        };
        if sse_connected && !delivered {
            sse_connected = false;
            skipped_events = 0;
            *delegated_sse_open.lock().await = false;
            tracing::info!(
                session_id = %session_id,
                "SSE client disconnected; continuing to drain orchestrator events"
            );
        }

        if is_finished {
            break;
        }
    }

    *delegated_sse_open.lock().await = false;

    outcome.finalize(
        &push_service,
        &apns_service,
        user_id.as_deref(),
        &session_id,
        db_path.as_ref(),
    );
    yield_orphaned_continuation_claim(db_path.as_ref(), &session_id);
    session_inputs.write().await.remove(&session_id);
}

/// `RunSpec::start` spawns the orchestrator before returning its event stream.
/// If that task exits before its initial recovery handoff, no event can clear
/// the durable `resuming_input` lease. Yield the transient lease here while
/// retaining the accepted response, allowing an exact retry instead of a
/// permanently busy session or a lost prompt.
fn yield_orphaned_continuation_claim(db_path: &std::path::Path, session_id: &str) {
    let result = (|| -> anyhow::Result<()> {
        let session_manager = SessionManager::new(Database::new(db_path)?);
        let Some(recovery) = session_manager.load_recovery_state(session_id)? else {
            return Ok(());
        };
        let Some(claim) = recovery.continuation_claim else {
            return Ok(());
        };
        session_manager.yield_awaiting_interaction_claim(
            session_id,
            &claim.interaction_id,
            &claim.accepted_response,
        )?;
        Ok(())
    })();
    if let Err(error) = result {
        tracing::error!(
            session_id,
            %error,
            "Failed to yield orphaned continuation execution lease"
        );
    }
}
