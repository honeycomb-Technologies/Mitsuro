//! Exercise the production orchestrator, provider serialization, tool executor,
//! and durable WorkflowManager together. All provider traffic stays on loopback.

use super::*;
use crate::ai::client::AiClientConfig;
use crate::ai::models::{ApiFormat, ModelMetadata};
use crate::ai::providers::{AuthHeader, ProviderId};
use crate::tools::{AskUserQuestionTool, ReadTool, WorkflowUpdateTool, WriteTool};
use crate::workflow::{
    CreateGoalInput, CriterionInput, PlanProposalInput, StartAttemptInput, StepProposalInput,
    WorkflowSnapshot,
};
use serde_json::{json, Value};
use std::time::Duration;
use tempfile::TempDir;
use tiny_http::{Header, Request, Response, Server};

struct RuntimeFixture {
    temp: TempDir,
    services: OrchestratorServices,
    session_id: String,
    server: Arc<Server>,
}

impl RuntimeFixture {
    async fn new() -> anyhow::Result<Self> {
        let temp = TempDir::new()?;
        std::fs::write(temp.path().join("evidence.txt"), "one\ntwo\nthree\n")?;
        let db_path = temp.path().join("runtime.db");
        let sessions = SessionManager::new(Database::new(&db_path)?);
        let session_id =
            sessions.create_session("Runtime regression", Some("fixture-model"), None)?;
        let server = Arc::new(Server::http("127.0.0.1:0").map_err(|e| anyhow::anyhow!(e))?);
        // Exercise request governance without depending on a changing bundled
        // catalog: unknown model names intentionally receive no tools.
        let mut model = ModelMetadata::new("fixture-model", "Fixture", ProviderId::ZAi)
            .with_transport(ApiFormat::OpenAI);
        model.supports_tools = true;
        let client = AiClient::new_with_resolved_model(
            AiClientConfig {
                model: "fixture-model".into(),
                max_tokens: 1024,
                base_url: Some(format!("http://{}", server.server_addr())),
                auth_header: AuthHeader::Bearer,
                provider_id: ProviderId::ZAi,
                api_format: ApiFormat::OpenAI,
                custom_headers: Default::default(),
            },
            "local-fixture-key".into(),
            model.resolve_runtime(),
        )?;
        let registry = Arc::new(ToolRegistry::new());
        registry.register(Arc::new(ReadTool)).await;
        registry.register(Arc::new(WriteTool)).await;
        registry.register(Arc::new(WorkflowUpdateTool)).await;
        registry.register(Arc::new(AskUserQuestionTool)).await;
        let services = OrchestratorServices {
            ai_client: Arc::new(client),
            tool_registry: registry,
            process_registry: Arc::new(ProcessRegistry::new()),
            db_path,
            skills_manager: Arc::new(RwLock::new(SkillsManager::new(
                temp.path().join("skills"),
                None,
            ))),
        };
        Ok(Self {
            temp,
            services,
            session_id,
            server,
        })
    }

    fn manager(&self) -> anyhow::Result<WorkflowManager> {
        Ok(WorkflowManager::new(self.services.db_path.clone())?)
    }

    fn snapshot(&self) -> anyhow::Result<WorkflowSnapshot> {
        self.manager()?
            .get_snapshot(&self.session_id)?
            .ok_or_else(|| anyhow::anyhow!("missing workflow"))
    }

    fn approved_goal(&self, activate: bool) -> anyhow::Result<WorkflowSnapshot> {
        self.approved_goal_steps(activate, 1)
    }

    fn approved_goal_steps(
        &self,
        activate: bool,
        step_count: usize,
    ) -> anyhow::Result<WorkflowSnapshot> {
        self.approved_plan(activate, &vec![true; step_count])
    }

    fn approved_plan(
        &self,
        activate: bool,
        required_steps: &[bool],
    ) -> anyhow::Result<WorkflowSnapshot> {
        let manager = self.manager()?;
        let operation_prefix = uuid::Uuid::new_v4();
        let created = manager.create_goal(
            &self.session_id,
            CreateGoalInput {
                title: "Verified change".into(),
                objective: "Read evidence and finish the approved change".into(),
                constraints: vec![],
                criteria: vec![CriterionInput {
                    description: "Evidence verified".into(),
                    required: true,
                }],
                token_budget: None,
            },
            &format!("{operation_prefix}-create"),
            "user",
        )?;
        let goal_id = &created.snapshot.goal.id;
        let proposed = manager.propose_plan(
            &self.session_id,
            goal_id,
            created.snapshot.aggregate_revision,
            PlanProposalInput {
                title: "Change plan".into(),
                rationale: None,
                source_message_id: None,
                predecessor_id: None,
                legacy_markdown: None,
                steps: (1..=required_steps.len())
                    .map(|step| StepProposalInput {
                        display_key: format!("1.{step}"),
                        description: format!("Inspect evidence area {step}"),
                        context: None,
                        parent_display_key: None,
                        dependencies: if step > 1 {
                            vec![format!("1.{}", step - 1)]
                        } else {
                            vec![]
                        },
                        acceptance_criteria: vec![],
                        required: required_steps[step - 1],
                    })
                    .collect(),
            },
            &format!("{operation_prefix}-propose"),
            "agent",
        )?;
        let plan_id = &proposed.snapshot.plan_revision.as_ref().unwrap().id;
        let approved = manager.approve_plan(
            &self.session_id,
            goal_id,
            plan_id,
            proposed.snapshot.aggregate_revision,
            &format!("{operation_prefix}-approve"),
            "user",
        )?;
        if activate {
            Ok(manager
                .activate_goal(
                    &self.session_id,
                    goal_id,
                    approved.snapshot.aggregate_revision,
                    &format!("{operation_prefix}-activate"),
                    "user",
                )?
                .snapshot)
        } else {
            Ok(approved.snapshot)
        }
    }

    async fn run(
        &self,
        mode: WorkMode,
    ) -> (
        mpsc::UnboundedReceiver<LoopEvent>,
        mpsc::UnboundedSender<LoopInput>,
    ) {
        self.run_with_budget(mode, 8).await
    }

    async fn run_with_budget(
        &self,
        mode: WorkMode,
        max_turns: usize,
    ) -> (
        mpsc::UnboundedReceiver<LoopEvent>,
        mpsc::UnboundedSender<LoopInput>,
    ) {
        let options = CallOptions {
            tools: Some(self.services.tool_registry.get_ai_tools_all().await),
            ..Default::default()
        };
        AgenticOrchestrator::new(
            self.services.clone(),
            OrchestratorConfig {
                session_id: self.session_id.clone(),
                working_dir: self.temp.path().to_path_buf(),
                initial_work_mode: mode,
                permission_mode: PermissionMode::Autonomous,
                refresh_code_tools_on_mode_change: true,
                run_budget: Some(RunBudget::with_max_turns(max_turns)),
                ..Default::default()
            },
        )
        .run(
            vec![ModelMessage {
                role: Role::User,
                content: vec![Content::text(
                    "Carry out the authorized task and verify it.".into(),
                )],
            }],
            options,
        )
    }

    async fn request(&self) -> anyhow::Result<(Request, Value)> {
        let server = self.server.clone();
        let response = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let mut request = server
                .recv_timeout(Duration::from_secs(10))?
                .ok_or_else(|| {
                    anyhow::anyhow!("orchestrator did not admit the next provider request")
                })?;
            let value = serde_json::from_reader(request.as_reader())?;
            Ok((request, value))
        })
        .await?;
        response.map_err(|error| {
            let snapshot = self.snapshot().ok();
            let messages = SessionManager::new(Database::new(&self.services.db_path).unwrap())
                .load_session_messages(&self.session_id)
                .unwrap_or_default()
                .into_iter()
                .rev()
                .take(3)
                .map(|(_, content)| content.chars().take(3000).collect::<String>())
                .collect::<Vec<_>>();
            anyhow::anyhow!("{error}; workflow={snapshot:?}; recent messages={messages:?}")
        })
    }
}

fn respond(request: Request, text: &str, tool: Option<(&str, &str, Value)>) {
    respond_tools(request, text, tool.into_iter().collect());
}

fn respond_tools(request: Request, text: &str, tools: Vec<(&str, &str, Value)>) {
    let mut body = format!(
        "data: {}\n\n",
        json!({"choices":[{"index":0,"delta":{"content":text},"finish_reason":null}]})
    );
    let reason = if tools.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };
    for (index, (id, name, args)) in tools.into_iter().enumerate() {
        body.push_str(&format!("data: {}\n\n", json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":index,"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}}]},"finish_reason":null}]})));
    }
    body.push_str(&format!("data: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"index":0,"delta":{},"finish_reason":reason}],"usage":{"prompt_tokens":120,"completion_tokens":12,"total_tokens":132}})));
    request
        .respond(
            Response::from_string(body)
                .with_header(Header::from_bytes("Content-Type", "text/event-stream").unwrap()),
        )
        .unwrap();
}

async fn finished(events: &mut mpsc::UnboundedReceiver<LoopEvent>) -> LoopStopReason {
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            if let LoopEvent::Finished { stop_reason, .. } = event {
                return stop_reason;
            }
        }
        panic!("orchestrator closed without Finished");
    })
    .await
    .expect("orchestrator should terminate")
}

fn has_tool(request: &Value, name: &str) -> bool {
    request["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tool| tool["function"]["name"] == name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_activation_refreshes_next_request_and_accounts_real_tool_turn() -> anyhow::Result<()>
{
    let fixture = RuntimeFixture::new().await?;
    let approved = fixture.approved_goal(false)?;
    let (mut events, inputs) = fixture.run(WorkMode::Plan).await;
    let (request, before) = fixture.request().await?;
    assert!(
        !has_tool(&before, "write"),
        "Plan request must exclude write"
    );
    let active = fixture.manager()?.activate_goal(
        &fixture.session_id,
        &approved.goal.id,
        approved.aggregate_revision,
        "activate-during-stream",
        "user",
    )?;
    inputs.send(LoopInput::WorkflowUpdated {
        goal_id: active.snapshot.goal.id,
        aggregate_revision: active.snapshot.aggregate_revision,
        operation_id: active.operation_id,
    })?;
    respond(request, "The plan is ready for activation.", None);
    let (request, after) = fixture.request().await?;
    assert!(
        has_tool(&after, "write"),
        "Activation must expose Build mutations on the next real request"
    );
    assert!(has_tool(&after, "workflow_update"));
    assert!(after
        .to_string()
        .contains("Read evidence and finish the approved change"));
    assert_eq!(
        fixture.snapshot()?.latest_attempt.unwrap().status,
        AttemptStatus::Running
    );
    respond(
        request,
        "The evidence file is the source of truth for the change.",
        Some(("read-live", "read", json!({"file_path":"evidence.txt"}))),
    );
    let (request, next) = fixture.request().await?;
    let serialized = next.to_string();
    assert!(
        serialized.contains("The evidence file is the source of truth for the change."),
        "Substantive assistant findings must survive into the next request"
    );
    assert!(serialized.contains("read-live"));
    assert!(serialized.contains("one"));
    let running = fixture.snapshot()?.latest_attempt.unwrap();
    assert_eq!(running.tool_call_count, 1);
    assert_eq!(running.turn_count, 1);
    inputs.send(LoopInput::Cancel)?;
    assert_eq!(finished(&mut events).await, LoopStopReason::UserAbort);
    drop(request);
    let stopped = fixture.snapshot()?;
    assert_eq!(stopped.goal.status, GoalStatus::Active);
    assert_eq!(
        stopped.latest_attempt.unwrap().status,
        AttemptStatus::Paused
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_and_same_process_resume_acquires_new_attempt_with_monotonic_counts(
) -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    for offset in 1..=2 {
        let (request, _) = fixture.request().await?;
        respond(
            request,
            "Checking the next evidence line.",
            Some((
                &format!("read-{offset}"),
                "read",
                json!({"file_path":"evidence.txt","offset":offset,"limit":1}),
            )),
        );
    }
    let (request, _) = fixture.request().await?;
    let first = fixture.snapshot()?.latest_attempt.unwrap();
    assert_eq!((first.turn_count, first.tool_call_count), (2, 2));
    inputs.send(LoopInput::Cancel)?;
    assert_eq!(finished(&mut events).await, LoopStopReason::UserAbort);
    drop(request);
    let settled = fixture.snapshot()?.latest_attempt.unwrap();
    assert_eq!(settled.status, AttemptStatus::Paused);
    assert!(settled.turn_count >= first.turn_count);
    assert_eq!(settled.tool_call_count, first.tool_call_count);

    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    let resumed = fixture.snapshot()?.latest_attempt.unwrap();
    assert_ne!(first.id, resumed.id);
    respond(
        request,
        "Continuing the evidence check.",
        Some(("resumed-read", "read", json!({"file_path":"evidence.txt"}))),
    );
    let (request, _) = fixture.request().await?;
    let resumed = fixture.snapshot()?.latest_attempt.unwrap();
    assert_eq!((resumed.turn_count, resumed.tool_call_count), (1, 1));
    let db = Database::new(&fixture.services.db_path)?;
    let old_counts: (u32, u32) = db.conn().query_row(
        "SELECT turn_count, tool_call_count FROM workflow_execution_attempts WHERE id = ?1",
        [&first.id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(old_counts, (settled.turn_count, settled.tool_call_count));
    inputs.send(LoopInput::Cancel)?;
    assert_eq!(finished(&mut events).await, LoopStopReason::UserAbort);
    drop(request);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_attempt_rolls_over_before_next_provider_request_without_task_start(
) -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    let active = fixture.approved_goal(true)?;
    let started = fixture.manager()?.start_attempt(
        &fixture.session_id,
        &active.goal.id,
        active.aggregate_revision,
        StartAttemptInput {
            step_id: Some(active.steps[0].id.clone()),
            permission_mode: "autonomous".into(),
            max_turns: 1,
            max_tool_calls: 100,
            max_research_actions: 100,
            max_wall_time_secs: 300,
        },
        "bounded-attempt",
        "runtime",
    )?;
    let first = started.snapshot.latest_attempt.unwrap();
    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "Inspecting the evidence.",
        Some(("budget-read", "read", json!({"file_path":"evidence.txt"}))),
    );
    let (request, _) = fixture.request().await?;
    let next = fixture.snapshot()?;
    assert_eq!(next.goal.status, GoalStatus::Active);
    let replacement = next.latest_attempt.unwrap();
    assert_ne!(replacement.id, first.id);
    assert_eq!(replacement.status, AttemptStatus::Running);
    assert_eq!(replacement.tool_call_count, 0);
    assert_eq!(
        next.steps[0].claimed_attempt_id.as_deref(),
        Some(replacement.id.as_str())
    );
    inputs.send(LoopInput::Cancel)?;
    assert_eq!(finished(&mut events).await, LoopStopReason::UserAbort);
    drop(request);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_premature_final_gets_one_correction_then_explicit_pause() -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(request, "I'll start implementing next.", None);
    let (request, retry) = fixture.request().await?;
    assert!(retry.to_string().contains("GOAL CONTINUATION"));
    respond(request, "I will continue later.", None);
    assert_eq!(finished(&mut events).await, LoopStopReason::AwaitingInput);
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.goal.status, GoalStatus::Paused);
    assert_eq!(
        snapshot.latest_attempt.unwrap().status,
        AttemptStatus::Paused
    );
    assert_eq!(
        snapshot.goal.status_reason.as_deref(),
        Some("model_completion_without_goal_verification")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_failure_settles_attempt_and_preserves_interrupted_recovery() -> anyhow::Result<()>
{
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "Checking evidence before the provider fails.",
        Some(("failure-read", "read", json!({"file_path":"evidence.txt"}))),
    );
    let (request, _) = fixture.request().await?;
    request.respond(
        Response::from_string(
            r#"{"error":{"message":"fixture invalid request","type":"invalid_request_error"}}"#,
        )
        .with_status_code(400),
    )?;
    assert_eq!(finished(&mut events).await, LoopStopReason::ProviderError);
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.goal.status, GoalStatus::Active);
    assert_eq!(
        snapshot.latest_attempt.unwrap().status,
        AttemptStatus::Paused
    );
    let recovery = SessionManager::new(Database::new(&fixture.services.db_path)?)
        .load_recovery_state(&fixture.session_id)?
        .unwrap();
    assert_eq!(recovery.status, RecoveryStatus::Interrupted);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_question_waits_without_goal_final_retry_and_settles_attempt() -> anyhow::Result<()>
{
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond_tools(
        request,
        "The release target needs your decision.",
        vec![
            (
                "question-evidence",
                "read",
                json!({"file_path":"evidence.txt"}),
            ),
            (
                "input-question",
                "AskUserQuestion",
                json!({"questions":[{"header":"Target","question":"Which release target is intended?","options":[{"label":"Preview"},{"label":"Production"}]}]}),
            ),
        ],
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::AwaitingInput);
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.goal.status, GoalStatus::Active);
    let attempt = snapshot.latest_attempt.unwrap();
    assert_eq!(attempt.status, AttemptStatus::Paused);
    assert_eq!(
        (
            attempt.turn_count,
            attempt.tool_call_count,
            attempt.research_action_count
        ),
        (1, 2, 1)
    );
    let recovery = SessionManager::new(Database::new(&fixture.services.db_path)?)
        .load_recovery_state(&fixture.session_id)?
        .unwrap();
    assert_eq!(recovery.status, RecoveryStatus::AwaitingInput);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopted_running_attempt_seeds_durable_counter_offsets() -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    let active = fixture.approved_goal(true)?;
    let started = fixture.manager()?.start_attempt(
        &fixture.session_id,
        &active.goal.id,
        active.aggregate_revision,
        StartAttemptInput {
            step_id: Some(active.steps[0].id.clone()),
            permission_mode: "autonomous".into(),
            max_turns: 24,
            max_tool_calls: 100,
            max_research_actions: 100,
            max_wall_time_secs: 300,
        },
        "existing-attempt",
        "runtime",
    )?;
    let attempt = started.snapshot.latest_attempt.unwrap();
    fixture.manager()?.record_attempt_progress(
        &fixture.session_id,
        &active.goal.id,
        &attempt.id,
        started.snapshot.aggregate_revision,
        crate::workflow::AttemptProgressInput {
            turn_count: 5,
            tool_call_count: 9,
            research_action_count: 4,
            material_progress: true,
            blocker_fingerprint: None,
        },
        "existing-progress",
        "runtime",
    )?;
    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "Continuing previously recorded work.",
        Some(("adopt-read", "read", json!({"file_path":"evidence.txt"}))),
    );
    let (request, _) = fixture.request().await?;
    let adopted = fixture.snapshot()?.latest_attempt.unwrap();
    assert_eq!(adopted.id, attempt.id);
    assert_eq!(
        (
            adopted.turn_count,
            adopted.tool_call_count,
            adopted.research_action_count
        ),
        (6, 10, 5)
    );
    inputs.send(LoopInput::Cancel)?;
    assert_eq!(finished(&mut events).await, LoopStopReason::UserAbort);
    drop(request);
    Ok(())
}

struct WaitingBashFixtureTool {
    started: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::tools::registry::Tool for WaitingBashFixtureTool {
    fn name(&self) -> &str {
        "bash"
    }
    fn description(&self) -> &str {
        "Wait until the test cancels this tool"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"command":{"type":"string"}},"required":["command"]})
    }
    async fn execute(
        &self,
        _params: Value,
        _ctx: &crate::tools::ToolContext,
    ) -> crate::tools::ToolResult {
        self.started.notify_one();
        std::future::pending().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_mixed_question_batch_preserves_completed_write_and_accounts_all_calls(
) -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    let started = Arc::new(tokio::sync::Notify::new());
    fixture
        .services
        .tool_registry
        .register(Arc::new(WaitingBashFixtureTool {
            started: started.clone(),
        }))
        .await;
    fixture.approved_goal(true)?;
    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond_tools(
        request,
        "I will save the completed work before checking the release target.",
        vec![
            (
                "completed-write",
                "write",
                json!({"file_path":"completed.txt","content":"preserved work"}),
            ),
            ("waiting-command", "bash", json!({"command":"fixture-wait"})),
            (
                "cancelled-question",
                "AskUserQuestion",
                json!({"questions":[{"header":"Target","question":"Which target?","options":[{"label":"Preview"},{"label":"Production"}]}]}),
            ),
        ],
    );
    tokio::time::timeout(Duration::from_secs(5), started.notified()).await?;
    assert_eq!(
        std::fs::read_to_string(fixture.temp.path().join("completed.txt"))?,
        "preserved work"
    );
    inputs.send(LoopInput::Cancel)?;
    let stop = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            match event {
                LoopEvent::AwaitingInput { .. } => {
                    panic!("cancelled question must not be presented")
                }
                LoopEvent::Finished { stop_reason, .. } => return stop_reason,
                _ => {}
            }
        }
        panic!("orchestrator closed without Finished");
    })
    .await?;
    assert_eq!(stop, LoopStopReason::UserAbort);
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.goal.status, GoalStatus::Active);
    assert_eq!(snapshot.goal.tokens_used, 132);
    let attempt = snapshot.latest_attempt.unwrap();
    assert_eq!(attempt.status, AttemptStatus::Paused);
    assert_eq!(
        (
            attempt.turn_count,
            attempt.tool_call_count,
            attempt.research_action_count
        ),
        (1, 3, 0)
    );
    let sessions = SessionManager::new(Database::new(&fixture.services.db_path)?);
    assert!(sessions.load_recovery_state(&fixture.session_id)?.is_none());
    let contents = sessions
        .load_session_messages(&fixture.session_id)?
        .into_iter()
        .flat_map(|(_, serialized)| serde_json::from_str::<Vec<Content>>(&serialized).unwrap())
        .collect::<Vec<_>>();
    for id in ["completed-write", "waiting-command", "cancelled-question"] {
        assert_eq!(contents.iter().filter(|content| matches!(content, Content::ToolUse { id: tool_id, .. } if tool_id == id)).count(), 1);
        assert_eq!(contents.iter().filter(|content| matches!(content, Content::ToolResult { tool_use_id, .. } if tool_use_id == id)).count(), 1);
    }
    assert!(contents.iter().any(|content| matches!(content, Content::ToolResult { tool_use_id, is_error, .. } if tool_use_id == "completed-write" && *is_error != Some(true))));
    assert!(contents.iter().any(|content| matches!(content, Content::ToolResult { tool_use_id, is_error: Some(true), .. } if tool_use_id == "cancelled-question")));
    Ok(())
}

struct BackgroundFixtureTool;

#[async_trait::async_trait]
impl crate::tools::registry::Tool for BackgroundFixtureTool {
    fn name(&self) -> &str {
        "agent"
    }
    fn description(&self) -> &str {
        "Create a durable background child fixture"
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object"})
    }
    async fn execute(
        &self,
        _params: Value,
        ctx: &crate::tools::ToolContext,
    ) -> crate::tools::ToolResult {
        let result = (|| -> anyhow::Result<_> {
            let store = DelegatedRunStore::new(Database::new(ctx.db_path.as_ref().unwrap())?);
            store.create_background_run(&crate::storage::DelegatedRunStartInput {
                delegated_run_id: "background-fixture".into(),
                parent_session_id: ctx.session_id.clone().unwrap(),
                parent_tool_call_id: ctx.tool_use_id.clone(),
                role: crate::storage::DelegatedRunRole::Explore,
                stage: super::super::DelegatedRunStage::Running,
                provider: None,
                model: None,
                resumable: false,
                resumed_from_run_id: None,
                target_scope: vec![],
            })?;
            Ok(())
        })();
        match result {
            Ok(()) => crate::tools::ToolResult::success_data(
                json!({"delegated_run_id":"background-fixture","status":"background_started"}),
            ),
            Err(error) => crate::tools::ToolResult::error(error.to_string()),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_child_allows_independent_parent_work_then_durable_wait() -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture
        .services
        .tool_registry
        .register(Arc::new(BackgroundFixtureTool))
        .await;
    fixture.approved_goal(true)?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "The child will inspect one independent area.",
        Some((
            "background-start",
            "agent",
            json!({"agent_type":"explore","prompt":"Inspect the isolated child area.","run_in_background":true}),
        )),
    );
    let (request, second) = fixture.request().await?;
    assert!(second
        .to_string()
        .contains("Continue useful independent work now"));
    respond(
        request,
        "I can write the independent parent artifact now.",
        Some((
            "independent-write",
            "write",
            json!({"file_path":"independent.txt","content":"parent work finished"}),
        )),
    );
    let (request, _) = fixture.request().await?;
    assert_eq!(
        std::fs::read_to_string(fixture.temp.path().join("independent.txt"))?,
        "parent work finished"
    );
    respond(request, "The independent change is written. The delegated inspection is still running; its completion will resume the Goal.", None);
    assert_eq!(finished(&mut events).await, LoopStopReason::Completed);
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.goal.status, GoalStatus::Active);
    assert_eq!(
        snapshot.latest_attempt.unwrap().status,
        AttemptStatus::Paused
    );
    assert!(awaiting_background_delegation(
        &fixture.services.db_path,
        &fixture.session_id
    )?);
    let store = DelegatedRunStore::new(Database::new(&fixture.services.db_path)?);
    store.finalize_run(
        "background-fixture",
        super::super::DelegatedRunStage::Complete,
        &json!({"summary":"inspection done"}),
        None,
        false,
    )?;
    assert!(
        !awaiting_background_delegation(&fixture.services.db_path, &fixture.session_id)?,
        "A terminal report must be synthesized instead of treated as an ongoing wait"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_goal_completion_uses_runtime_tools_and_has_no_extra_retry() -> anyhow::Result<()>
{
    let fixture = RuntimeFixture::new().await?;
    fixture
        .services
        .tool_registry
        .register(Arc::new(crate::tools::TaskCompleteTool))
        .await;
    fixture.approved_plan(true, &[true, false])?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "Checking the acceptance evidence.",
        Some(("success-read", "read", json!({"file_path":"evidence.txt"}))),
    );
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "The evidence file contains the required lines.",
        Some((
            "complete-step",
            "task_complete",
            json!({"task_id":"1.1","result":"Inspected the requested evidence file","evidence":["Read evidence.txt and verified its three expected lines"]}),
        )),
    );
    let (request, _) = fixture.request().await?;
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.steps[0].status, WorkflowStepStatus::Completed);
    assert_eq!(snapshot.steps[1].status, WorkflowStepStatus::Pending,
        "An optional followup is preserved, but cannot prevent legal Goal verification after the required plan completes");
    let db = Database::new(&fixture.services.db_path)?;
    let completed_counts: (u32,u32) = db.conn().query_row("SELECT turn_count, tool_call_count FROM workflow_execution_attempts WHERE goal_id = ?1 AND step_id = ?2 AND status = 'succeeded'", [&snapshot.goal.id, &snapshot.steps[0].id], |row| Ok((row.get(0)?, row.get(1)?)))?;
    assert_eq!(
        completed_counts,
        (2, 2),
        "The task_complete batch belongs to its admitted attempt even after that attempt settles"
    );
    let verification_attempt = snapshot.latest_attempt.as_ref().unwrap();
    assert_eq!(verification_attempt.status, AttemptStatus::Running);
    assert!(
        verification_attempt.step_id.is_none(),
        "Goal verification is governed even after the last step"
    );
    respond(
        request,
        "The required criterion has concrete evidence.",
        Some((
            "verify-goal",
            "workflow_update",
            json!({"action":"verify_criterion","goal_id":snapshot.goal.id,"expected_revision":snapshot.aggregate_revision,"criterion_id":snapshot.criteria[0].id,"status":"passed","evidence":["Read evidence.txt and verified one, two, and three"]}),
        )),
    );
    let (request, _) = fixture.request().await?;
    let snapshot = fixture.snapshot()?;
    assert_eq!(
        snapshot.criteria[0].status,
        crate::workflow::CriterionStatus::Passed
    );
    respond(
        request,
        "All required work and evidence are recorded.",
        Some((
            "complete-goal",
            "workflow_update",
            json!({"action":"complete_goal","goal_id":snapshot.goal.id,"expected_revision":snapshot.aggregate_revision}),
        )),
    );
    let (request, _) = fixture.request().await?;
    assert_eq!(fixture.snapshot()?.goal.status, GoalStatus::Completed);
    respond(
        request,
        "The requested inspection is complete and verified.",
        None,
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::Completed);
    assert_eq!(fixture.snapshot()?.goal.status, GoalStatus::Completed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completion_and_successor_start_batch_charges_only_the_admitted_attempt(
) -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture
        .services
        .tool_registry
        .register(Arc::new(crate::tools::TaskCompleteTool))
        .await;
    fixture
        .services
        .tool_registry
        .register(Arc::new(crate::tools::TaskStartTool))
        .await;
    fixture.approved_goal_steps(true, 2)?;
    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    let admitted = fixture.snapshot()?.latest_attempt.unwrap();
    respond_tools(
        request,
        "Area one is verified; proceed to the dependent area.",
        vec![
            (
                "finish-first",
                "task_complete",
                json!({"task_id":"1.1","result":"Verified area one","evidence":["The fixture's area-one acceptance evidence is available"]}),
            ),
            ("start-second", "task_start", json!({"task_id":"1.2"})),
        ],
    );
    let (request, _) = fixture.request().await?;
    let snapshot = fixture.snapshot()?;
    let successor = snapshot.latest_attempt.unwrap();
    assert_ne!(successor.id, admitted.id);
    assert_eq!(successor.status, AttemptStatus::Running);
    assert_eq!((successor.turn_count, successor.tool_call_count), (0, 0));
    let db = Database::new(&fixture.services.db_path)?;
    let admitted_counts: (u32, u32, String) = db.conn().query_row(
        "SELECT turn_count, tool_call_count, status FROM workflow_execution_attempts WHERE id = ?1",
        [&admitted.id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(admitted_counts, (1, 2, "succeeded".into()));
    inputs.send(LoopInput::Cancel)?;
    assert_eq!(finished(&mut events).await, LoopStopReason::UserAbort);
    drop(request);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_pause_prevents_pending_effects_and_closes_tool_protocol() -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    let snapshot = fixture.snapshot()?;
    let paused = fixture.manager()?.pause_goal(
        &fixture.session_id,
        &snapshot.goal.id,
        snapshot.aggregate_revision,
        Some("user pause"),
        "pause-during-provider",
        "user",
    )?;
    inputs.send(LoopInput::WorkflowUpdated {
        goal_id: paused.snapshot.goal.id,
        aggregate_revision: paused.snapshot.aggregate_revision,
        operation_id: paused.operation_id,
    })?;
    respond(
        request,
        "Writing the change.",
        Some((
            "paused-write",
            "write",
            json!({"file_path":"must-not-exist.txt","content":"stale action"}),
        )),
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::AwaitingInput);
    assert!(!fixture.temp.path().join("must-not-exist.txt").exists());
    assert_eq!(fixture.snapshot()?.goal.status, GoalStatus::Paused);
    let stored = SessionManager::new(Database::new(&fixture.services.db_path)?)
        .load_session_messages(&fixture.session_id)?;
    let messages = stored
        .into_iter()
        .flat_map(|(_, serialized)| serde_json::from_str::<Vec<Content>>(&serialized).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        messages
            .iter()
            .filter(
                |content| matches!(content, Content::ToolUse { id, .. } if id == "paused-write")
            )
            .count(),
        1
    );
    assert_eq!(messages.iter().filter(|content| matches!(content, Content::ToolResult { tool_use_id, is_error: Some(true), .. } if tool_use_id == "paused-write")).count(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_goal_token_budget_prevents_returned_tool_effects() -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    let active = fixture.approved_goal(false)?;
    let edited = fixture.manager()?.edit_goal(
        &fixture.session_id,
        &active.goal.id,
        active.aggregate_revision,
        crate::workflow::EditGoalInput {
            title: None,
            objective: None,
            constraints: None,
            criteria: None,
            token_budget: Some(Some(1)),
        },
        "tiny-token-budget",
        "user",
    )?;
    fixture.manager()?.activate_goal(
        &fixture.session_id,
        &edited.snapshot.goal.id,
        edited.snapshot.aggregate_revision,
        "activate-tiny-budget",
        "user",
    )?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "Writing the result.",
        Some((
            "budget-write",
            "write",
            json!({"file_path":"over-budget.txt","content":"must not run"}),
        )),
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::BudgetExhausted);
    assert!(!fixture.temp.path().join("over-budget.txt").exists());
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.goal.status, GoalStatus::Paused);
    assert_eq!(
        snapshot.goal.status_reason.as_deref(),
        Some("token_budget_exhausted")
    );
    let attempt = snapshot.latest_attempt.unwrap();
    assert_eq!(attempt.status, AttemptStatus::Paused);
    assert_eq!(attempt.turn_count, 1);
    assert_eq!(attempt.tool_call_count, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_pause_blocks_returned_effects_without_a_workflow_notification(
) -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    let snapshot = fixture.snapshot()?;
    fixture.manager()?.pause_goal(
        &fixture.session_id,
        &snapshot.goal.id,
        snapshot.aggregate_revision,
        Some("user pause while delivery is unavailable"),
        "pause-without-wake",
        "user",
    )?;
    respond(
        request,
        "Writing the change.",
        Some((
            "unnotified-write",
            "write",
            json!({"file_path":"unnotified.txt","content":"must not run"}),
        )),
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::AwaitingInput);
    assert!(!fixture.temp.path().join("unnotified.txt").exists());
    let snapshot = fixture.snapshot()?;
    assert_eq!(snapshot.goal.status, GoalStatus::Paused);
    assert_eq!(snapshot.goal.tokens_used, 132);
    assert_eq!(
        snapshot.goal.status_reason.as_deref(),
        Some("user pause while delivery is unavailable")
    );
    let attempt = snapshot.latest_attempt.unwrap();
    assert_eq!(attempt.status, AttemptStatus::Paused);
    assert_eq!((attempt.turn_count, attempt.tool_call_count), (1, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replaced_goal_cannot_inherit_inflight_effects_usage_or_run_settlement(
) -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    let original = fixture.snapshot()?;
    fixture.manager()?.cancel_goal(
        &fixture.session_id,
        &original.goal.id,
        original.aggregate_revision,
        Some("replace this Goal"),
        "replace-original-goal",
        "user",
    )?;
    let replacement = fixture.approved_goal(true)?;
    let started = fixture.manager()?.start_attempt(
        &fixture.session_id,
        &replacement.goal.id,
        replacement.aggregate_revision,
        StartAttemptInput {
            step_id: Some(replacement.steps[0].id.clone()),
            permission_mode: "autonomous".into(),
            max_turns: 24,
            max_tool_calls: 100,
            max_research_actions: 100,
            max_wall_time_secs: 300,
        },
        "replacement-attempt",
        "runtime",
    )?;
    respond(
        request,
        "Writing a result from the superseded provider request.",
        Some((
            "superseded-write",
            "write",
            json!({"file_path":"superseded.txt","content":"must not run"}),
        )),
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::ProviderError);
    assert!(!fixture.temp.path().join("superseded.txt").exists());
    let current = fixture.snapshot()?;
    assert_eq!(current.goal.id, replacement.goal.id);
    assert_eq!(current.goal.status, GoalStatus::Active);
    assert_eq!(current.goal.tokens_used, 0);
    assert_eq!(
        current.aggregate_revision,
        started.snapshot.aggregate_revision
    );
    let attempt = current.latest_attempt.unwrap();
    assert_eq!(attempt.status, AttemptStatus::Running);
    assert_eq!((attempt.turn_count, attempt.tool_call_count), (0, 0));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_steering_rearms_the_goal_final_correction_once() -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    fixture.approved_goal(true)?;
    let (mut events, inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(request, "I will inspect the evidence next.", None);
    let (request, correction) = fixture.request().await?;
    assert!(correction.to_string().contains("GOAL CONTINUATION"));
    inputs.send(LoopInput::Steer {
        pending_id: None,
        content: vec![Content::text(
            "First check the second evidence line.".into(),
        )],
    })?;
    respond(request, "I have received the new direction.", None);
    let (request, steered) = fixture.request().await?;
    assert!(steered
        .to_string()
        .contains("First check the second evidence line."));
    respond(request, "I will check that line next.", None);
    // New input gets one new correction; it must not immediately inherit the
    // old final's spent retry, nor permit another unbounded final-only loop.
    let (request, _) = fixture.request().await?;
    assert_eq!(fixture.snapshot()?.goal.status, GoalStatus::Active);
    respond(request, "I will continue later.", None);
    assert_eq!(finished(&mut events).await, LoopStopReason::AwaitingInput);
    assert_eq!(fixture.snapshot()?.goal.status, GoalStatus::Paused);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_edit_cue_is_consumed_by_readback_before_final_response() -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    let (request, _) = fixture.request().await?;
    respond(
        request,
        "",
        Some((
            "write-marker",
            "write",
            json!({"file_path":"marker.txt","content":"DONE\n"}),
        )),
    );
    let (request, after_write) = fixture.request().await?;
    assert!(after_write.to_string().contains(VALIDATION_REMINDER));
    respond(
        request,
        "",
        Some(("read-marker", "read", json!({"file_path":"marker.txt"}))),
    );
    let (request, after_read) = fixture.request().await?;
    assert!(!after_read.to_string().contains(VALIDATION_REMINDER));
    assert!(after_read
        .to_string()
        .contains("Carry out the authorized task and verify it."));
    assert_eq!(
        std::fs::read_to_string(fixture.temp.path().join("marker.txt"))?,
        "DONE\n"
    );
    respond(
        request,
        "Wrote marker.txt and read back its contents. No executable tests were run.",
        None,
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::Completed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finite_code_budget_reserves_final_response_without_admitting_more_tools(
) -> anyhow::Result<()> {
    let fixture = RuntimeFixture::new().await?;
    let (mut events, _inputs) = fixture.run_with_budget(WorkMode::Build, 2).await;
    let (request, first) = fixture.request().await?;
    assert!(has_tool(&first, "write"));
    respond(
        request,
        "",
        Some((
            "write-once",
            "write",
            json!({"file_path":"marker.txt","content":"DONE\n"}),
        )),
    );
    let (request, last) = fixture.request().await?;
    assert!(last
        .get("tools")
        .is_none_or(|tools| tools.as_array().is_none_or(Vec::is_empty)));
    assert!(last.to_string().contains("failed or unrun required checks"));
    // Even a provider that ignores the tool-free surface cannot perform a
    // new side effect during the reserved final response.
    respond(
        request,
        "Wrote marker.txt. Tests were not run; verification is unfinished.",
        Some((
            "forbidden-write",
            "write",
            json!({"file_path":"extra.txt","content":"must not execute"}),
        )),
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::Completed);
    assert_eq!(
        std::fs::read_to_string(fixture.temp.path().join("marker.txt"))?,
        "DONE\n"
    );
    assert!(!fixture.temp.path().join("extra.txt").exists());
    assert!(
        fixture.server.try_recv()?.is_none(),
        "must not exceed two provider requests"
    );
    let messages = SessionManager::new(Database::new(&fixture.services.db_path)?)
        .load_session_messages(&fixture.session_id)?;
    assert!(messages
        .last()
        .unwrap()
        .1
        .contains("verification is unfinished"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_read_feedback_reaches_provider_and_expires_after_a_change() -> anyhow::Result<()>
{
    let fixture = RuntimeFixture::new().await?;
    std::fs::write(fixture.temp.path().join("evidence.txt"), "original")?;
    let (mut events, _inputs) = fixture.run(WorkMode::Build).await;
    for index in 0..2 {
        let (request, body) = fixture.request().await?;
        assert!(!body.to_string().contains("[PROGRESS GUARD]"));
        respond(
            request,
            "",
            Some((
                if index == 0 {
                    "first-read"
                } else {
                    "repeat-read"
                },
                "read",
                json!({"file_path":"evidence.txt"}),
            )),
        );
    }
    let (request, warned) = fixture.request().await?;
    assert!(warned.to_string().contains("specific unresolved question"));
    assert!(warned.to_string().contains("does not establish success"));
    respond(
        request,
        "The requested correction is still needed.",
        Some((
            "change",
            "write",
            json!({"file_path":"evidence.txt","content":"corrected"}),
        )),
    );
    let (request, after_change) = fixture.request().await?;
    assert!(!after_change.to_string().contains("[PROGRESS GUARD]"));
    respond(
        request,
        "Correction written; acceptance tests remain unrun.",
        None,
    );
    assert_eq!(finished(&mut events).await, LoopStopReason::Completed);
    assert_eq!(
        std::fs::read_to_string(fixture.temp.path().join("evidence.txt"))?,
        "corrected"
    );
    Ok(())
}
