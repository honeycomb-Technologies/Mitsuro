//! Typed adapter between the v2 application and the canonical agent runtime.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use mitsuro_core::{
    agent::{
        plan_handler::parse_plan_confirm_choice, run_compaction_pipeline, AgentCancellation,
        AgentConfig, CompactionManager, CompactionRequest, CompactionTrigger,
        DelegatedProgressEvent, LoopEvent, LoopInput, OrchestratorServices, RunProvenance,
        RunSpecBuilder,
    },
    ai::{
        client::{config::AnthropicAdaptiveEffort, AiClient, CallOptions, CodexReasoningEffort},
        models::{ModelKey, ModelMetadata},
        providers::{ProviderId, ReasoningControl, ReasoningEffort},
        types::{Content, ContextManagement, ModelMessage, Role, ThinkingConfig},
    },
    auth::{
        anthropic_oauth_config, openai_oauth_config, AuthMethod, BrowserOAuthFlow, DeviceCodeFlow,
        OAuthTokenStore, PasteCodeOAuthFlow, PkceVerifier,
    },
    extensions::ExtensionCallContext,
    process::ProcessRegistry,
    storage::{
        Database, DelegationGroupRecord, DelegationStore, PendingInteractionSnapshot,
        ProjectSettings, SessionManager, SessionRecoveryState, SessionType, WorkMode,
    },
    tools::{
        load_from_clipboard_rgba, load_from_path, load_from_url, register_agent_tool,
        registry::PermissionMode,
    },
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    paths,
    tui_support::{
        has_image_references, parse_input,
        utils::{DeviceCodeInfo, OAuthStatusUpdate},
        AppServices, InputSegment,
    },
    tui_v2::model::{
        artifact::PartId,
        conversation::{AttachmentKind, AttachmentPart},
    },
    tui_v2::{motion::preference::MotionPreference, presentation::theme::ThemeKind},
};

const MAX_FILES_PER_MESSAGE: usize = 20;
const MAX_PROJECT_FILE_INDEX: usize = 20_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecentSession {
    pub session_id: String,
    pub title: String,
    pub model: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HomeSnapshot {
    pub project: String,
    pub branch: Option<String>,
    pub model: Option<String>,
    pub provider: String,
    pub recent_sessions: Vec<RecentSession>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetupProvider {
    pub id: ProviderId,
    pub label: String,
    pub connected: bool,
    pub auth_methods: Vec<AuthMethod>,
    pub models: Vec<SetupModel>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OAuthStart {
    Waiting,
    PasteCode { authorization_url: String },
}

#[derive(Debug)]
pub enum SetupServiceUpdate {
    CatalogRefresh {
        provider: ProviderId,
        result: Result<Vec<ModelMetadata>, String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetupModel {
    pub key: ModelKey,
    pub label: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetupSnapshot {
    pub providers: Vec<SetupProvider>,
    pub selected_model: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlSnapshot {
    pub reasoning: Option<String>,
    pub fast_available: bool,
    pub fast_enabled: bool,
    pub permission: String,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AppearanceSnapshot {
    pub theme: Option<ThemeKind>,
    pub motion: Option<MotionPreference>,
}

impl Default for ControlSnapshot {
    fn default() -> Self {
        Self {
            reasoning: None,
            fast_available: false,
            fast_enabled: false,
            permission: PermissionMode::default().as_str().to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoadedSession {
    pub session_id: String,
    pub title: String,
    pub messages: Vec<ModelMessage>,
    pub recovery: Option<SessionRecoveryState>,
    /// Last persisted agent context size (sessions.token_count).
    pub token_count: Option<usize>,
    /// Canonical delegated work projected independently from transcript replay.
    pub delegation_groups: Vec<DelegationGroupRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessRow {
    pub id: String,
    pub command: String,
    pub status: String,
    pub elapsed_seconds: u64,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanSnapshot {
    pub title: String,
    pub objective: String,
    pub status: String,
    pub completed_steps: usize,
    pub total_steps: usize,
    pub current_step: Option<String>,
    /// Ordered plan steps for the dock checklist (bullet list, not numbered).
    pub steps: Vec<PlanStepRow>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanStepRow {
    pub description: String,
    pub done: bool,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionRow {
    pub category: String,
    pub id: String,
    pub name: String,
    pub status: String,
    pub enabled: bool,
    pub toggleable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectEntryKind {
    Directory,
    File,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectEntry {
    pub path: String,
    pub name: String,
    pub parent: String,
    pub kind: ProjectEntryKind,
    pub search_path: String,
    pub search_name: String,
}

impl ProjectEntry {
    fn new(path: String, kind: ProjectEntryKind) -> Self {
        let name = Path::new(&path)
            .file_name()
            .map_or_else(|| path.clone(), |name| name.to_string_lossy().into_owned());
        let parent = Path::new(&path)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map_or_else(String::new, |parent| parent.to_string_lossy().into_owned());
        Self {
            search_path: path.to_lowercase(),
            search_name: name.to_lowercase(),
            path,
            name,
            parent,
            kind,
        }
    }

    pub const fn is_directory(&self) -> bool {
        matches!(self.kind, ProjectEntryKind::Directory)
    }
}

/// Channels and canonical identity for one running turn.
pub struct StartedRun {
    pub session_id: String,
    pub title: String,
    pub events: mpsc::UnboundedReceiver<LoopEvent>,
    pub input: mpsc::UnboundedSender<LoopInput>,
    /// Live sub-agent / explore-build progress while the parent turn runs.
    pub delegated_progress: mpsc::UnboundedReceiver<DelegatedProgressEvent>,
}

pub struct PreparedInput {
    pub content: Vec<Content>,
    pub display_text: String,
    pub attachments: Vec<AttachmentPart>,
    pub consumed_clipboard_ids: Vec<String>,
}

/// Runtime-only state. No legacy widget, focus, layout, or presentation state
/// crosses this boundary.
pub struct RuntimeServices {
    services: AppServices,
    process_registry: Arc<ProcessRegistry>,
    working_dir: PathBuf,
    project_entries: Vec<ProjectEntry>,
    current_model: String,
    current_model_key: Option<ModelKey>,
    active_provider: ProviderId,
    permission_mode: PermissionMode,
    work_mode: WorkMode,
    reasoning_effort: Option<ReasoningEffort>,
    fast_mode: bool,
    agent_config: AgentConfig,
    cancellation: AgentCancellation,
    current_session_id: Option<String>,
    anthropic_verifier: Option<PkceVerifier>,
    setup_updates: mpsc::UnboundedSender<SetupServiceUpdate>,
}

impl RuntimeServices {
    pub async fn initialize() -> Result<(
        Self,
        mpsc::UnboundedReceiver<OAuthStatusUpdate>,
        mpsc::UnboundedReceiver<SetupServiceUpdate>,
    )> {
        let working_dir = std::env::current_dir().context("resolve current working directory")?;
        let project_entries = index_project_entries(&working_dir);
        let (
            mut services,
            mut legacy_channels,
            process_registry,
            current_model,
            current_model_key,
            _legacy_theme,
            _legacy_theme_name,
            active_provider,
        ) = crate::tui_support::app_builder::init_services(&working_dir).await;

        if active_provider.supports_oauth() {
            if let Err(error) = mitsuro_core::auth::refresh_oauth_token(active_provider).await {
                tracing::debug!(%error, "OAuth refresh was not required for TUI v2");
            }
        }

        let cancellation = AgentCancellation::new();
        let initial_metadata = selected_metadata(
            &services,
            current_model_key.as_ref(),
            &current_model,
            active_provider,
        );
        if let Some(metadata) = initial_metadata.clone() {
            if let Some(client) = create_ai_client(&services, &metadata) {
                register_agent_tool(
                    &services.tool_registry,
                    Arc::new(client),
                    cancellation.clone(),
                )
                .await;
                services.cached_ai_tools = services.tool_registry.get_ai_tools_all().await;
            }
        }

        let oauth_events = legacy_channels
            .oauth_status
            .take()
            .ok_or_else(|| anyhow!("OAuth status channel was not initialized."))?;
        let (setup_updates, setup_events) = mpsc::unbounded_channel();
        Ok((
            Self {
                services,
                process_registry,
                working_dir,
                project_entries,
                current_model,
                current_model_key,
                active_provider,
                permission_mode: PermissionMode::default(),
                work_mode: WorkMode::Build,
                reasoning_effort: initial_metadata.as_ref().and_then(default_reasoning_effort),
                fast_mode: false,
                agent_config: AgentConfig::default(),
                cancellation,
                current_session_id: None,
                anthropic_verifier: None,
                setup_updates,
            },
            oauth_events,
            setup_events,
        ))
    }

    pub fn prepare_input(
        &self,
        text: &str,
        message_id: &str,
        pending_clipboard_images: &mut HashMap<String, (usize, usize, Vec<u8>)>,
    ) -> Result<PreparedInput> {
        prepare_user_input(
            text,
            &self.working_dir,
            message_id,
            pending_clipboard_images,
        )
    }

    pub fn start_run(&mut self, text: &str, content: Vec<Content>) -> Result<StartedRun> {
        let metadata = self
            .selected_metadata()
            .ok_or_else(|| anyhow!("No exact model is selected. Open setup to choose one."))?;
        let client = create_ai_client(&self.services, &metadata).ok_or_else(|| {
            anyhow!(
                "No valid authentication is available for {}.",
                metadata.display_name
            )
        })?;

        self.cancellation.reset();
        let (session_id, title) = self.ensure_session(text, &metadata)?;
        let user_message = ModelMessage {
            role: Role::User,
            content,
        };
        let mut conversation = self.load_conversation(&session_id)?;
        conversation.push(user_message.clone());

        self.start_resolved_run(
            session_id,
            title,
            metadata,
            client,
            conversation,
            Some(&user_message),
            None,
        )
    }

    pub fn continue_interaction(
        &mut self,
        tool_call_id: &str,
        response: &str,
    ) -> Result<StartedRun> {
        let session_id = self
            .current_session_id
            .clone()
            .ok_or_else(|| anyhow!("No conversation is active."))?;
        let metadata = self
            .selected_metadata()
            .ok_or_else(|| anyhow!("No exact model is selected."))?;
        let client = create_ai_client(&self.services, &metadata).ok_or_else(|| {
            anyhow!(
                "No valid authentication is available for {}.",
                metadata.display_name
            )
        })?;
        let manager = SessionManager::new(Database::new(&paths::config_dir().join("mitsuro.db"))?);
        let (recovery, pending) = manager
            .claim_awaiting_interaction(&session_id, tool_call_id, response)?
            .ok_or_else(|| anyhow!("This decision is no longer awaiting a response."))?;
        self.permission_mode = recovery.permission_mode.unwrap_or(self.permission_mode);
        let execution_tool_allowlist = recovery
            .execution_tool_allowlist
            .map(|names| names.into_iter().collect::<HashSet<_>>());

        let prepare = (|| -> Result<(String, Vec<ModelMessage>)> {
            let session = manager
                .get_session(&session_id)?
                .ok_or_else(|| anyhow!("The active session no longer exists."))?;
            match pending {
                PendingInteractionSnapshot::PlanConfirm { .. } => {
                    let choice = parse_plan_confirm_choice(response);
                    let text = if choice.as_deref() == Some("execute") {
                        self.work_mode = WorkMode::Build;
                        manager.update_session_work_mode(&session_id, self.work_mode)?;
                        "The plan has been approved. Begin executing the plan, starting with Task 1.1."
                    } else {
                        if let Some(plan_manager) = &self.services.plan_manager {
                            plan_manager.abandon_plan(&session_id)?;
                        }
                        "The plan has been abandoned. What would you like to do instead?"
                    };
                    self.save_user_text_once(&session_id, text)?;
                }
                PendingInteractionSnapshot::AskUserQuestion { .. } => {
                    let mut messages = self.load_conversation(&session_id)?;
                    let mut merged = false;
                    if let Some(last) = messages.last_mut() {
                        if let Some(output) =
                            last.content.iter_mut().find_map(|content| match content {
                                Content::ToolResult {
                                    tool_use_id,
                                    output,
                                    ..
                                } if tool_use_id == tool_call_id => Some(output),
                                _ => None,
                            })
                        {
                            *output = serde_json::Value::String(response.to_owned());
                            manager.update_last_message(
                                &session_id,
                                "user",
                                &serde_json::to_string(&last.content)?,
                            )?;
                            merged = true;
                        }
                    }
                    if !merged {
                        let message = ModelMessage {
                            role: Role::User,
                            content: vec![Content::ToolResult {
                                tool_use_id: tool_call_id.to_owned(),
                                output: serde_json::Value::String(response.to_owned()),
                                is_error: None,
                            }],
                        };
                        self.save_message(&session_id, &message)?;
                    }
                }
                PendingInteractionSnapshot::ToolApproval { .. } => {
                    anyhow::bail!("Tool approvals must resolve on the active run channel.");
                }
            }
            Ok((session.title, self.load_conversation(&session_id)?))
        })();

        let started = prepare.and_then(|(title, conversation)| {
            self.start_resolved_run(
                session_id.clone(),
                title,
                metadata,
                client,
                conversation,
                None,
                execution_tool_allowlist,
            )
        });
        if started.is_err() {
            let _ = manager.yield_awaiting_interaction_claim(&session_id, tool_call_id, response);
        }
        started
    }

    #[allow(clippy::too_many_arguments)]
    fn start_resolved_run(
        &mut self,
        session_id: String,
        title: String,
        metadata: ModelMetadata,
        client: AiClient,
        conversation: Vec<ModelMessage>,
        message_to_persist: Option<&ModelMessage>,
        execution_tool_allowlist: Option<HashSet<String>>,
    ) -> Result<StartedRun> {
        self.cancellation.reset();
        let project_settings = ProjectSettings::load(&self.working_dir);
        let has_active_plan =
            mitsuro_core::workflow::WorkflowManager::new(paths::config_dir().join("mitsuro.db"))
                .ok()
                .and_then(|manager| manager.get_snapshot(&session_id).ok().flatten())
                .is_some_and(|snapshot| {
                    snapshot.goal.status == mitsuro_core::workflow::GoalStatus::Active
                        && snapshot.plan_revision.is_some()
                })
                || self
                    .services
                    .plan_manager
                    .as_ref()
                    .and_then(|manager| manager.get_active_plan(&session_id).ok())
                    .flatten()
                    .is_some();
        let tools = mitsuro_core::tools::registry::ToolRequestPolicy::code(
            self.permission_mode,
            self.work_mode == WorkMode::Plan,
            has_active_plan,
            true,
            project_settings.disabled_tools.as_deref().unwrap_or(&[]),
        )
        .filter(self.services.cached_ai_tools.clone());
        let reasoning_enabled = self
            .reasoning_effort
            .is_some_and(|effort| effort != ReasoningEffort::None)
            && metadata.reasoning_control != Some(ReasoningControl::OutputOnly);
        let thinking = reasoning_enabled.then(ThinkingConfig::default);
        let codex_reasoning_effort = (metadata.reasoning_control
            == Some(ReasoningControl::OpenAiEffort))
        .then(|| self.reasoning_effort.and_then(codex_effort))
        .flatten();
        let anthropic_adaptive_effort = (metadata.reasoning_control
            == Some(ReasoningControl::AnthropicAdaptive))
        .then(|| self.reasoning_effort.and_then(anthropic_effort))
        .flatten();
        let context_management = match (reasoning_enabled, !tools.is_empty()) {
            (true, _) => Some(ContextManagement::default_for_thinking_and_tools()),
            (false, true) => Some(ContextManagement::default_tools_only()),
            (false, false) => None,
        };
        let options = CallOptions {
            tools: (!tools.is_empty()).then_some(tools),
            thinking,
            enable_caching: true,
            context_management,
            session_id: Some(session_id.clone()),
            codex_reasoning_effort,
            codex_parallel_tool_calls: true,
            anthropic_adaptive_effort,
            reasoning_format: metadata.reasoning_format,
            reasoning_control: metadata.reasoning_control,
            fast_mode: self.fast_mode && metadata.fast_mode.is_some(),
            fast_mode_format: metadata.fast_mode,
            ..Default::default()
        };
        let mode_aware_code_tools = options.tools.is_some();
        let db_path = paths::config_dir().join("mitsuro.db");
        let ai_client = Arc::new(client);
        let (delegated_progress_tx, delegated_progress_rx) =
            mpsc::unbounded_channel::<DelegatedProgressEvent>();
        let run_spec = RunSpecBuilder::new(
            RunProvenance::Tui,
            session_id.clone(),
            self.working_dir.clone(),
            SessionType::Code,
        )
        .project_dir(Some(self.working_dir.clone()))
        .permission_mode(self.permission_mode)
        .execution_tool_allowlist(execution_tool_allowlist)
        .run_budget(self.agent_config.primary_run_budget_override())
        .stream_idle_timeout(self.agent_config.stream_idle_timeout())
        .initial_work_mode(self.work_mode)
        .mode_aware_code_tools(mode_aware_code_tools)
        .generate_title(message_to_persist.is_some() && conversation.len() <= 1)
        .delegated_progress_tx(Some(delegated_progress_tx))
        .call_options(options)
        .build(ai_client.as_ref())?;
        let runtime = OrchestratorServices {
            ai_client,
            tool_registry: self.services.tool_registry.clone(),
            process_registry: self.process_registry.clone(),
            db_path,
            skills_manager: self.services.skills_manager.clone(),
        };
        // Persist only after every fallible run contract has resolved. A
        // rejected contract must never leave a duplicate user message behind.
        if let Some(message) = message_to_persist {
            self.save_message(&session_id, message)?;
        }
        let (events, input) = run_spec.start(runtime, conversation);

        Ok(StartedRun {
            session_id,
            title,
            events,
            input,
            delegated_progress: delegated_progress_rx,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.selected_metadata()
            .is_some_and(|metadata| create_ai_client(&self.services, &metadata).is_some())
    }

    /// Detach from the active session without deleting it. The next submitted
    /// prompt creates a fresh canonical code conversation.
    pub fn begin_new_conversation(&mut self) {
        self.current_session_id = None;
    }

    pub fn begin_compaction(
        &self,
        preservation_hints: Option<String>,
    ) -> Result<oneshot::Receiver<Result<(), String>>> {
        let session_id = self
            .current_session_id
            .clone()
            .ok_or_else(|| anyhow!("No active conversation to compact."))?;
        let conversation = self.load_conversation(&session_id)?;
        if conversation.is_empty() {
            anyhow::bail!("No conversation to compact.");
        }
        let metadata = self
            .selected_metadata()
            .ok_or_else(|| anyhow!("No exact model is selected."))?;
        let client = create_ai_client(&self.services, &metadata)
            .ok_or_else(|| anyhow!("No valid authentication is available for compaction."))?;
        let working_dir = self.working_dir.clone();
        let project_dir = working_dir.to_string_lossy().into_owned();
        let db_path = paths::config_dir().join("mitsuro.db");
        let manager = CompactionManager::for_model(
            metadata.provider,
            metadata.api_format,
            &metadata.id,
            metadata.context_window,
        );
        let model = metadata.id;
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = run_compaction_pipeline(CompactionRequest {
                db_path: &db_path,
                session_id: &session_id,
                conversation: &conversation,
                working_dir: &working_dir,
                ai_client: Some(&client),
                model: Some(&model),
                trigger: CompactionTrigger::Manual {
                    preservation_hints,
                    direction: None,
                },
                compaction_manager: manager,
                request_budget: None,
                last_usage_prompt_tokens: None,
                messages_after_usage: 0,
                summary_override: None,
                project_dir: Some(&project_dir),
                user_id: None,
            })
            .await
            .map(|_| ())
            .map_err(|error| error.to_string());
            let _ = sender.send(result);
        });
        Ok(receiver)
    }

    pub fn reload_current_session(&mut self) -> Result<LoadedSession> {
        let session_id = self
            .current_session_id
            .clone()
            .ok_or_else(|| anyhow!("No active conversation."))?;
        self.open_session(&session_id)
    }

    pub fn begin_extension_command(
        &self,
        command: String,
        arguments: String,
    ) -> Result<oneshot::Receiver<Result<String, String>>> {
        let manager = self
            .services
            .tool_registry
            .agent_extension_manager()
            .ok_or_else(|| anyhow!("Unknown command: /{command}"))?;
        let context = ExtensionCallContext::for_turn(
            self.working_dir.clone(),
            Some(self.working_dir.clone()),
            self.current_session_id.clone(),
            Some(self.current_model.clone()),
            format!("{:?}", self.permission_mode).to_ascii_lowercase(),
            self.work_mode == WorkMode::Plan,
        );
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = manager
                .execute_command(&command, &arguments, &context)
                .await
                .map(|value| match value {
                    serde_json::Value::String(output) => output,
                    output => {
                        serde_json::to_string_pretty(&output).unwrap_or_else(|_| output.to_string())
                    }
                })
                .map_err(|error| error.to_string());
            let _ = sender.send(result);
        });
        Ok(receiver)
    }

    pub fn home_snapshot(&self) -> HomeSnapshot {
        let working_dir = self.working_dir.to_string_lossy();
        let recent_sessions = self
            .services
            .session_manager
            .as_ref()
            .and_then(|manager| manager.list_sessions(Some(working_dir.as_ref())).ok())
            .unwrap_or_default()
            .into_iter()
            .filter(|session| session.session_type == SessionType::Code)
            .take(5)
            .map(|session| RecentSession {
                session_id: session.id,
                title: session.title,
                model: session.model,
            })
            .collect();
        let branch = std::process::Command::new("git")
            .args(["branch", "--show-current"])
            .current_dir(&self.working_dir)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|branch| branch.trim().to_owned())
            .filter(|branch| !branch.is_empty());
        HomeSnapshot {
            project: self
                .working_dir
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("workspace")
                .to_owned(),
            branch,
            model: self
                .selected_metadata()
                .map(|metadata| metadata.display_name),
            provider: self.active_provider.to_string(),
            recent_sessions,
        }
    }

    pub fn session_snapshot(&self) -> Vec<RecentSession> {
        let working_dir = self.working_dir.to_string_lossy();
        self.services
            .session_manager
            .as_ref()
            .and_then(|manager| manager.list_sessions(Some(working_dir.as_ref())).ok())
            .unwrap_or_default()
            .into_iter()
            .filter(|session| session.session_type == SessionType::Code)
            .take(200)
            .map(|session| RecentSession {
                session_id: session.id,
                title: session.title,
                model: session.model,
            })
            .collect()
    }

    pub fn process_snapshot(&self) -> Vec<ProcessRow> {
        let mut processes = self.process_registry.try_list().unwrap_or_default();
        processes.sort_by_key(|process| std::cmp::Reverse(process.started_at));
        processes
            .into_iter()
            .map(|process| {
                let status = process.display_status().to_owned();
                let elapsed_seconds = process.duration().as_secs();
                let active = process.is_active();
                ProcessRow {
                    id: process.id,
                    command: process.command,
                    status,
                    elapsed_seconds,
                    active,
                }
            })
            .collect()
    }

    pub fn stop_process(&self, process_id: &str) -> Result<()> {
        futures::executor::block_on(self.process_registry.kill(process_id))
    }

    pub fn project_entry_snapshot(&self) -> Vec<ProjectEntry> {
        self.project_entries.clone()
    }

    pub fn plan_snapshot(&self) -> Option<PlanSnapshot> {
        let session_id = self.current_session_id.as_deref()?;
        let db_path = paths::config_dir().join("mitsuro.db");

        // Prefer durable workflow Goal + steps (canonical plan surface).
        if let Ok(manager) = mitsuro_core::workflow::WorkflowManager::new(db_path) {
            if let Ok(Some(snapshot)) = manager.get_snapshot(session_id) {
                let completed_steps = snapshot
                    .steps
                    .iter()
                    .filter(|step| {
                        step.status == mitsuro_core::workflow::WorkflowStepStatus::Completed
                    })
                    .count();
                let current_step = snapshot
                    .steps
                    .iter()
                    .find(|step| {
                        step.status == mitsuro_core::workflow::WorkflowStepStatus::InProgress
                    })
                    .map(|step| step.description.clone());
                let steps = snapshot
                    .steps
                    .iter()
                    .map(|step| {
                        use mitsuro_core::workflow::WorkflowStepStatus;
                        PlanStepRow {
                            description: step.description.clone(),
                            done: matches!(step.status, WorkflowStepStatus::Completed),
                            active: matches!(step.status, WorkflowStepStatus::InProgress),
                        }
                    })
                    .collect();
                // Show even when steps are empty (goal created, plan proposed later).
                return Some(PlanSnapshot {
                    title: snapshot.goal.title,
                    objective: snapshot.goal.objective,
                    status: snapshot.goal.status.to_string(),
                    completed_steps,
                    total_steps: snapshot.steps.len(),
                    current_step,
                    steps,
                });
            }
        }

        // Fallback: legacy PlanManager session plan (task tools / older sessions).
        let plan = self
            .services
            .plan_manager
            .as_ref()
            .and_then(|manager| manager.get_active_plan(session_id).ok())
            .flatten()?;
        let tasks: Vec<_> = plan
            .phases
            .iter()
            .flat_map(|phase| phase.tasks.iter())
            .collect();
        let completed_steps = tasks.iter().filter(|task| task.completed).count();
        let current_step = tasks
            .iter()
            .find(|task| !task.completed)
            .map(|task| task.description.clone());
        let steps: Vec<PlanStepRow> = tasks
            .iter()
            .enumerate()
            .map(|(index, task)| PlanStepRow {
                description: task.description.clone(),
                done: task.completed,
                active: !task.completed
                    && tasks.iter().position(|candidate| !candidate.completed) == Some(index),
            })
            .collect();
        Some(PlanSnapshot {
            title: plan.title.clone(),
            objective: plan
                .phases
                .first()
                .map(|phase| phase.name.clone())
                .unwrap_or_default(),
            status: "active".to_owned(),
            completed_steps,
            total_steps: tasks.len(),
            current_step,
            steps,
        })
    }

    pub async fn extension_snapshot(&self) -> Vec<ExtensionRow> {
        let mut rows = Vec::new();
        if let Some(manager) = self.services.tool_registry.agent_extension_manager() {
            if let Ok(status) = manager.project_trust_status() {
                rows.push(ExtensionRow {
                    category: "Agent".to_owned(),
                    id: "project-agent-extensions".to_owned(),
                    name: "Project extensions".to_owned(),
                    status: if status.trusted {
                        "trusted"
                    } else {
                        "not trusted"
                    }
                    .to_owned(),
                    enabled: status.trusted,
                    toggleable: true,
                });
            }
        }
        for server in self.services.mcp_manager.list_servers().await {
            let connected = matches!(server.status, mitsuro_core::mcp::McpServerStatus::Connected);
            rows.push(ExtensionRow {
                category: "MCP".to_owned(),
                id: server.name.clone(),
                name: server.name,
                status: server.status.to_string(),
                enabled: connected,
                toggleable: server.enabled,
            });
        }
        let skills = self.services.skills_manager.write().await.list_skills();
        rows.extend(skills.into_iter().map(|skill| ExtensionRow {
            category: "Skill".to_owned(),
            id: skill.name.clone(),
            name: skill.name,
            status: if skill.enabled {
                skill.permission.to_string()
            } else {
                "disabled".to_owned()
            },
            enabled: skill.enabled,
            toggleable: true,
        }));
        if let Some(manager) = &self.services.plugin_manager {
            if let Ok(plugins) = manager.list_installed_plugins().await {
                rows.extend(plugins.into_iter().map(|plugin| ExtensionRow {
                    category: "Plugin".to_owned(),
                    id: plugin.id,
                    name: plugin.name,
                    status: if plugin.enabled {
                        plugin.version
                    } else {
                        "disabled".to_owned()
                    },
                    enabled: plugin.enabled,
                    toggleable: true,
                }));
            }
        }
        let hooks = self.services.user_hook_manager.read().await;
        rows.extend(hooks.hooks().iter().map(|hook| {
            ExtensionRow {
                category: "Hook".to_owned(),
                id: hook.id.clone(),
                name: format!("{} · {}", hook.hook_type, hook.tool_pattern),
                status: if hook.is_package_hook() {
                    if hook.enabled {
                        "enabled by package"
                    } else {
                        "disabled by package"
                    }
                } else if hook.enabled {
                    "enabled"
                } else {
                    "disabled"
                }
                .to_owned(),
                enabled: hook.enabled,
                toggleable: !hook.is_package_hook(),
            }
        }));
        rows.sort_by(|left, right| {
            left.category
                .cmp(&right.category)
                .then_with(|| left.name.cmp(&right.name))
        });
        rows
    }

    pub fn toggle_extension(&mut self, extension: &ExtensionRow) -> Result<()> {
        if !extension.toggleable {
            anyhow::bail!("{} is informational in this view.", extension.name);
        }
        match extension.category.as_str() {
            "Agent" => {
                let manager = self
                    .services
                    .tool_registry
                    .agent_extension_manager()
                    .ok_or_else(|| anyhow!("Agent extension host is unavailable."))?;
                futures::executor::block_on(manager.set_project_trusted_and_refresh(
                    !extension.enabled,
                    &self.services.tool_registry,
                ))?;
                self.services.cached_ai_tools =
                    futures::executor::block_on(self.services.tool_registry.get_ai_tools_all());
            }
            "Skill" => {
                futures::executor::block_on(self.services.skills_manager.write())
                    .set_skill_enabled(&extension.id, !extension.enabled)?;
            }
            "Plugin" => {
                let manager = self
                    .services
                    .plugin_manager
                    .as_ref()
                    .ok_or_else(|| anyhow!("Plugin manager is unavailable."))?;
                futures::executor::block_on(
                    manager.set_plugin_enabled(&extension.id, !extension.enabled),
                )?;
            }
            "Hook" => {
                let database = Database::new(&paths::config_dir().join("mitsuro.db"))?;
                futures::executor::block_on(self.services.user_hook_manager.write())
                    .toggle(&database, &extension.id)?;
            }
            _ => anyhow::bail!("{} cannot be toggled here.", extension.name),
        }
        Ok(())
    }

    pub fn begin_mcp_toggle(
        &self,
        extension: &ExtensionRow,
    ) -> Result<oneshot::Receiver<Result<(), String>>> {
        if extension.category != "MCP" || !extension.toggleable {
            anyhow::bail!("{} cannot be toggled here.", extension.name);
        }
        let manager = self.services.mcp_manager.clone();
        let registry = self.services.tool_registry.clone();
        let id = extension.id.clone();
        let disconnect = extension.enabled;
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = if disconnect {
                manager.disconnect(&id).await;
                Ok(())
            } else {
                match manager.connect_explicit(&id).await {
                    Ok(()) => {
                        mitsuro_core::mcp::tool::register_mcp_tools(manager, &registry).await;
                        Ok(())
                    }
                    Err(error) => Err(error.to_string()),
                }
            };
            let _ = sender.send(result);
        });
        Ok(receiver)
    }

    pub async fn refresh_extension_runtime(&mut self) -> Vec<ExtensionRow> {
        self.services.cached_ai_tools = self.services.tool_registry.get_ai_tools_all().await;
        self.extension_snapshot().await
    }

    pub fn open_session(&mut self, session_id: &str) -> Result<LoadedSession> {
        let manager = self
            .services
            .session_manager
            .as_ref()
            .ok_or_else(|| anyhow!("Session storage is unavailable."))?;
        let session = manager
            .get_session(session_id)?
            .ok_or_else(|| anyhow!("The selected session no longer exists."))?;
        if session.session_type != SessionType::Code {
            anyhow::bail!("Only code conversations can be opened in this TUI.");
        }
        if let Some(key) = session.model_key.as_ref() {
            if let Some(metadata) = self.services.model_registry.try_get_model_by_key(key) {
                self.reasoning_effort = default_reasoning_effort(&metadata);
                self.active_provider = metadata.provider;
                self.current_model = metadata.id;
                self.current_model_key = Some(key.clone());
                self.fast_mode = false;
            }
        }
        let messages = self.load_conversation(session_id)?;
        let recovery = manager.load_recovery_state(session_id)?;
        let delegation_groups =
            DelegationStore::new(Database::new(&paths::config_dir().join("mitsuro.db"))?)
                .list_groups_for_session(session_id, 100)?;
        self.current_session_id = Some(session.id.clone());
        self.work_mode = session.work_mode;
        self.permission_mode = session.permission_mode;
        // Session working_dir is runtime source-of-truth for git/project chrome.
        if let Some(dir) = session
            .working_dir
            .as_deref()
            .filter(|dir| !dir.is_empty())
            .map(std::path::PathBuf::from)
        {
            if dir != self.working_dir {
                self.working_dir = dir;
                self.project_entries = index_project_entries(&self.working_dir);
            }
        }
        Ok(LoadedSession {
            session_id: session.id,
            title: session.title,
            messages,
            recovery,
            token_count: session.token_count,
            delegation_groups,
        })
    }

    /// Persist a user-edited session title for the active conversation.
    pub fn update_session_title(&self, session_id: &str, title: &str) -> Result<()> {
        let manager = self
            .services
            .session_manager
            .as_ref()
            .ok_or_else(|| anyhow!("Session storage is unavailable."))?;
        manager.update_session_title(session_id, title)?;
        Ok(())
    }

    /// Selected model context window size (0 when unknown).
    pub fn context_window(&self) -> usize {
        self.selected_metadata()
            .map(|metadata| metadata.context_window)
            .unwrap_or(0)
    }

    /// Working directory used for git / project chrome.
    pub fn working_dir(&self) -> &std::path::Path {
        &self.working_dir
    }

    pub async fn setup_snapshot(&self) -> SetupSnapshot {
        let authenticated = self
            .services
            .credential_store
            .providers_with_auth()
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let (_, models_by_provider) = self
            .services
            .model_registry
            .get_organized_models(ProviderId::all())
            .await;
        SetupSnapshot {
            providers: ProviderId::all()
                .iter()
                .copied()
                .map(|provider| SetupProvider {
                    id: provider,
                    label: provider.to_string(),
                    connected: authenticated.contains(&provider),
                    auth_methods: provider.auth_methods(),
                    models: models_by_provider
                        .get(&provider)
                        .into_iter()
                        .flatten()
                        .map(|metadata| SetupModel {
                            key: metadata.key(),
                            label: metadata.display_name.clone(),
                        })
                        .collect(),
                })
                .collect(),
            selected_model: self
                .selected_metadata()
                .map(|metadata| metadata.display_name),
        }
    }

    pub fn connect_provider(&mut self, provider: ProviderId, credential: String) -> Result<()> {
        let credential = credential.trim();
        if credential.is_empty() {
            anyhow::bail!("Credential cannot be empty.");
        }
        self.services
            .credential_store
            .set(provider, credential.to_owned());
        self.services.credential_store.save()?;
        self.active_provider = provider;
        self.current_model.clear();
        self.current_model_key = None;
        Ok(())
    }

    pub fn start_oauth_flow(
        &mut self,
        provider: ProviderId,
        method: AuthMethod,
    ) -> Result<OAuthStart> {
        let status_tx = self.services.oauth_status_tx.clone();
        match method {
            AuthMethod::ApiKey => anyhow::bail!("API keys use the credential input flow."),
            AuthMethod::OAuthBrowser if provider == ProviderId::Anthropic => {
                let flow = PasteCodeOAuthFlow::new(anthropic_oauth_config());
                let (authorization_url, verifier, _state) = flow.get_auth_url()?;
                mitsuro_core::auth::open_browser(&authorization_url)
                    .context("open the Anthropic authorization page")?;
                self.anthropic_verifier = Some(verifier);
                Ok(OAuthStart::PasteCode { authorization_url })
            }
            AuthMethod::OAuthBrowser if provider == ProviderId::Grok => {
                tokio::spawn(async move {
                    let update = match mitsuro_core::auth::force_grok_browser_login().await {
                        Ok(token) => OAuthStatusUpdate {
                            provider,
                            success: true,
                            message: "Authentication successful".to_owned(),
                            device_code: None,
                            token: Some(mitsuro_core::auth::grok_auth_token_to_oauth_data(&token)),
                        },
                        Err(error) => OAuthStatusUpdate {
                            provider,
                            success: false,
                            message: format!("Grok OAuth failed: {error}"),
                            device_code: None,
                            token: None,
                        },
                    };
                    let _ = status_tx.send(update);
                });
                Ok(OAuthStart::Waiting)
            }
            AuthMethod::OAuthBrowser if provider == ProviderId::OpenAI => {
                tokio::spawn(async move {
                    let update = match BrowserOAuthFlow::new(openai_oauth_config()).run().await {
                        Ok(token) => OAuthStatusUpdate {
                            provider,
                            success: true,
                            message: "Authentication successful".to_owned(),
                            device_code: None,
                            token: Some(token),
                        },
                        Err(error) => OAuthStatusUpdate {
                            provider,
                            success: false,
                            message: format!("OAuth failed: {error}"),
                            device_code: None,
                            token: None,
                        },
                    };
                    let _ = status_tx.send(update);
                });
                Ok(OAuthStart::Waiting)
            }
            AuthMethod::OAuthDevice if provider == ProviderId::OpenAI => {
                tokio::spawn(async move {
                    let flow = DeviceCodeFlow::new(openai_oauth_config());
                    match flow.request_code().await {
                        Ok(code) => {
                            let _ = status_tx.send(OAuthStatusUpdate {
                                provider,
                                success: true,
                                message: "Enter the code in your browser".to_owned(),
                                device_code: Some(DeviceCodeInfo {
                                    user_code: code.user_code.clone(),
                                    verification_uri: code.verification_uri.clone(),
                                }),
                                token: None,
                            });
                            let update =
                                match flow.poll_for_token(&code.device_code, code.interval).await {
                                    Ok(token) => OAuthStatusUpdate {
                                        provider,
                                        success: true,
                                        message: "Authentication successful".to_owned(),
                                        device_code: None,
                                        token: Some(token),
                                    },
                                    Err(error) => OAuthStatusUpdate {
                                        provider,
                                        success: false,
                                        message: format!("Device authentication failed: {error}"),
                                        device_code: None,
                                        token: None,
                                    },
                                };
                            let _ = status_tx.send(update);
                        }
                        Err(error) => {
                            let _ = status_tx.send(OAuthStatusUpdate {
                                provider,
                                success: false,
                                message: format!("Failed to request a device code: {error}"),
                                device_code: None,
                                token: None,
                            });
                        }
                    }
                });
                Ok(OAuthStart::Waiting)
            }
            _ => anyhow::bail!("{provider} does not support {method}."),
        }
    }

    pub fn submit_anthropic_oauth_code(
        &mut self,
        provider: ProviderId,
        pasted_code: String,
    ) -> Result<()> {
        if provider != ProviderId::Anthropic {
            anyhow::bail!("Paste-code OAuth is only available for Anthropic.");
        }
        let verifier = self
            .anthropic_verifier
            .take()
            .ok_or_else(|| anyhow!("The authorization verifier expired. Start again."))?;
        let pasted_code = pasted_code.trim();
        if pasted_code.is_empty() {
            anyhow::bail!("Paste the authorization code before continuing.");
        }
        let (code, state) = pasted_code
            .split_once('#')
            .map_or((pasted_code.to_owned(), None), |(code, state)| {
                (code.to_owned(), Some(state.to_owned()))
            });
        let status_tx = self.services.oauth_status_tx.clone();
        tokio::spawn(async move {
            let flow = PasteCodeOAuthFlow::new(anthropic_oauth_config());
            let update = match flow.exchange_code(&code, state.as_deref(), &verifier).await {
                Ok(token) => OAuthStatusUpdate {
                    provider,
                    success: true,
                    message: "Authentication successful".to_owned(),
                    device_code: None,
                    token: Some(token),
                },
                Err(error) => OAuthStatusUpdate {
                    provider,
                    success: false,
                    message: format!("Token exchange failed: {error}"),
                    device_code: None,
                    token: None,
                },
            };
            let _ = status_tx.send(update);
        });
        Ok(())
    }

    pub fn complete_oauth(
        &mut self,
        provider: ProviderId,
        token: mitsuro_core::auth::OAuthTokenData,
    ) -> Result<bool> {
        OAuthTokenStore::set_persisted(provider, token)?;
        self.activate_provider(provider);
        Ok(self.begin_catalog_refresh(provider))
    }

    pub fn begin_catalog_refresh(&self, provider: ProviderId) -> bool {
        if !mitsuro_core::ai::catalog::supports_dynamic_models(provider) {
            return false;
        }
        let credentials = self.services.credential_store.clone();
        let sender = self.setup_updates.clone();
        tokio::spawn(async move {
            let result =
                mitsuro_core::ai::catalog::fetch_dynamic_models_for_store(provider, &credentials)
                    .await
                    .map_err(|error| error.to_string());
            let _ = sender.send(SetupServiceUpdate::CatalogRefresh { provider, result });
        });
        true
    }

    pub async fn apply_catalog_refresh(
        &mut self,
        provider: ProviderId,
        models: Vec<ModelMetadata>,
    ) -> Result<()> {
        self.services
            .model_registry
            .set_models(provider, models.clone())
            .await;
        if let Some(preferences) = &self.services.preferences {
            preferences.cache_models(provider, &models)?;
        }
        Ok(())
    }

    pub fn activate_provider(&mut self, provider: ProviderId) {
        self.active_provider = provider;
        if self
            .current_model_key
            .as_ref()
            .is_none_or(|key| key.provider != provider)
        {
            self.current_model.clear();
            self.current_model_key = None;
        }
    }

    pub fn select_model(&mut self, key: &ModelKey) -> Result<()> {
        let metadata = self
            .services
            .model_registry
            .try_get_model_by_key(key)
            .ok_or_else(|| anyhow!("The selected model is no longer in the catalog."))?;
        if metadata.provider != self.active_provider {
            anyhow::bail!("The selected model does not belong to the active provider.");
        }
        let client = create_ai_client(&self.services, &metadata).ok_or_else(|| {
            anyhow!("The selected model cannot authenticate with this connection.")
        })?;
        let registry = self.services.tool_registry.clone();
        let cancellation = self.cancellation.clone();
        // Model selection is a synchronous UI boundary, while registry locks
        // are async. These futures only acquire in-memory locks; completing
        // them here guarantees the first submitted turn cannot race an Agent
        // tool that still points at the prior model.
        futures::executor::block_on(register_agent_tool(
            &registry,
            Arc::new(client),
            cancellation,
        ));
        self.services.cached_ai_tools = futures::executor::block_on(registry.get_ai_tools_all());
        self.current_model = metadata.id.clone();
        self.current_model_key = Some(metadata.key());
        self.reasoning_effort = default_reasoning_effort(&metadata);
        self.fast_mode = false;
        if let Some(preferences) = &self.services.preferences {
            preferences.set_current_model_key(&metadata.key())?;
        }
        Ok(())
    }

    pub fn controls_snapshot(&self) -> ControlSnapshot {
        let metadata = self.selected_metadata();
        ControlSnapshot {
            reasoning: self.reasoning_effort.map(reasoning_effort_label),
            fast_available: metadata
                .as_ref()
                .is_some_and(|metadata| metadata.fast_mode.is_some()),
            fast_enabled: self.fast_mode,
            permission: self.permission_mode.as_str().to_owned(),
        }
    }

    pub fn appearance_snapshot(&self) -> AppearanceSnapshot {
        let Some(preferences) = &self.services.preferences else {
            return AppearanceSnapshot::default();
        };
        AppearanceSnapshot {
            theme: preferences
                .get("mitsuro_tui_v2_theme")
                .as_deref()
                .and_then(parse_theme_kind),
            motion: preferences
                .get("mitsuro_tui_v2_motion")
                .as_deref()
                .and_then(parse_motion_preference),
        }
    }

    pub fn persist_theme(&self, theme: ThemeKind) -> Result<()> {
        if let Some(preferences) = &self.services.preferences {
            preferences.set("mitsuro_tui_v2_theme", theme_kind_key(theme))?;
        }
        Ok(())
    }

    pub fn persist_motion(&self, motion: MotionPreference) -> Result<()> {
        if let Some(preferences) = &self.services.preferences {
            preferences.set("mitsuro_tui_v2_motion", motion_preference_key(motion))?;
        }
        Ok(())
    }

    pub fn cycle_reasoning(&mut self) -> ControlSnapshot {
        let Some(metadata) = self.selected_metadata() else {
            return self.controls_snapshot();
        };
        if metadata.reasoning_control == Some(ReasoningControl::OutputOnly) {
            self.reasoning_effort = None;
            return self.controls_snapshot();
        }
        let mut levels = metadata.supported_reasoning_levels;
        if !metadata.reasoning_is_mandatory && !levels.contains(&ReasoningEffort::None) {
            levels.insert(0, ReasoningEffort::None);
        }
        if levels.is_empty() {
            self.reasoning_effort = None;
            return self.controls_snapshot();
        }
        let current = self
            .reasoning_effort
            .and_then(|effort| levels.iter().position(|candidate| *candidate == effort));
        self.reasoning_effort = Some(levels[current.map_or(0, |index| (index + 1) % levels.len())]);
        self.controls_snapshot()
    }

    pub fn toggle_fast_mode(&mut self) -> ControlSnapshot {
        if self
            .selected_metadata()
            .is_some_and(|metadata| metadata.fast_mode.is_some())
        {
            self.fast_mode = !self.fast_mode;
        } else {
            self.fast_mode = false;
        }
        self.controls_snapshot()
    }

    pub fn toggle_permission_mode(&mut self) -> ControlSnapshot {
        self.permission_mode = match self.permission_mode {
            PermissionMode::Supervised => PermissionMode::Autonomous,
            PermissionMode::Autonomous => PermissionMode::Supervised,
        };
        if let (Some(manager), Some(session_id)) = (
            self.services.session_manager.as_ref(),
            self.current_session_id.as_deref(),
        ) {
            if let Err(error) =
                manager.update_session_permission_mode(session_id, self.permission_mode)
            {
                tracing::warn!(%error, %session_id, "Failed to persist TUI v2 permission mode");
            }
        }
        self.controls_snapshot()
    }

    pub fn toggle_work_mode(&mut self) -> WorkMode {
        self.work_mode = match self.work_mode {
            WorkMode::Plan => WorkMode::Build,
            WorkMode::Build => WorkMode::Plan,
        };
        if let (Some(manager), Some(session_id)) = (
            self.services.session_manager.as_ref(),
            self.current_session_id.as_deref(),
        ) {
            if let Err(error) = manager.update_session_work_mode(session_id, self.work_mode) {
                tracing::warn!(%error, %session_id, "Failed to persist TUI v2 work mode");
            }
        }
        self.work_mode
    }

    pub async fn shutdown(&self) {
        self.process_registry.kill_all().await;
    }

    fn selected_metadata(&self) -> Option<ModelMetadata> {
        selected_metadata(
            &self.services,
            self.current_model_key.as_ref(),
            &self.current_model,
            self.active_provider,
        )
    }

    fn ensure_session(
        &mut self,
        first_message: &str,
        metadata: &ModelMetadata,
    ) -> Result<(String, String)> {
        if let Some(session_id) = &self.current_session_id {
            let title = self
                .services
                .session_manager
                .as_ref()
                .and_then(|manager| manager.get_session(session_id).ok().flatten())
                .map_or_else(|| "Conversation".to_owned(), |session| session.title);
            return Ok((session_id.clone(), title));
        }

        let manager = self
            .services
            .session_manager
            .as_ref()
            .ok_or_else(|| anyhow!("Session storage is unavailable."))?;
        let title = SessionManager::generate_title_from_content(first_message);
        let working_dir = self.working_dir.to_string_lossy();
        let session_id =
            manager.create_session(&title, Some(&metadata.id), Some(working_dir.as_ref()))?;
        manager.update_session_model_selection(
            &session_id,
            Some(&metadata.key()),
            metadata.catalog_revision.as_deref(),
        )?;
        manager.update_session_permission_mode(&session_id, self.permission_mode)?;
        manager.update_session_work_mode(&session_id, self.work_mode)?;
        self.current_session_id = Some(session_id.clone());
        Ok((session_id, title))
    }

    fn save_message(&self, session_id: &str, message: &ModelMessage) -> Result<()> {
        let manager = self
            .services
            .session_manager
            .as_ref()
            .ok_or_else(|| anyhow!("Session storage is unavailable."))?;
        manager.save_message(
            session_id,
            "user",
            &serde_json::to_string(&message.content)?,
        )?;
        Ok(())
    }

    fn save_user_text_once(&self, session_id: &str, text: &str) -> Result<()> {
        let already_persisted = self
            .load_conversation(session_id)?
            .last()
            .is_some_and(|message| {
                message.role == Role::User
                    && matches!(
                        message.content.as_slice(),
                        [Content::Text { text: existing, .. }] if existing == text
                    )
            });
        if already_persisted {
            return Ok(());
        }

        self.save_message(
            session_id,
            &ModelMessage {
                role: Role::User,
                content: vec![Content::text(text.to_owned())],
            },
        )
    }

    fn load_conversation(&self, session_id: &str) -> Result<Vec<ModelMessage>> {
        let manager = self
            .services
            .session_manager
            .as_ref()
            .ok_or_else(|| anyhow!("Session storage is unavailable."))?;
        let mut messages = Vec::new();
        for (role, content) in manager.load_session_messages(session_id)? {
            if role != "user" && role != "assistant" {
                continue;
            }
            messages.push(ModelMessage {
                role: if role == "assistant" {
                    Role::Assistant
                } else {
                    Role::User
                },
                content: serde_json::from_str(&content)?,
            });
        }
        Ok(messages)
    }
}

fn index_project_entries(root: &Path) -> Vec<ProjectEntry> {
    let mut builder = ignore::WalkBuilder::new(root);
    builder
        .standard_filters(true)
        .hidden(true)
        .follow_links(false)
        .filter_entry(|entry| {
            entry.file_name().to_str().is_none_or(|name| {
                !matches!(
                    name,
                    ".git"
                        | "node_modules"
                        | "target"
                        | "dist"
                        | "build"
                        | "__pycache__"
                        | ".venv"
                        | "venv"
                )
            })
        });
    let mut files = builder
        .build()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let kind = entry.file_type().and_then(|kind| {
                if kind.is_dir() {
                    Some(ProjectEntryKind::Directory)
                } else if kind.is_file() {
                    Some(ProjectEntryKind::File)
                } else {
                    None
                }
            })?;
            let path = entry.path().strip_prefix(root).ok()?;
            (!path.as_os_str().is_empty())
                .then(|| ProjectEntry::new(path.to_string_lossy().into_owned(), kind))
        })
        .take(MAX_PROJECT_FILE_INDEX)
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files.dedup_by(|left, right| left.path == right.path);
    files
}

fn prepare_user_input(
    text: &str,
    working_dir: &Path,
    message_id: &str,
    pending_clipboard_images: &mut HashMap<String, (usize, usize, Vec<u8>)>,
) -> Result<PreparedInput> {
    if !has_image_references(text) {
        return Ok(PreparedInput {
            content: vec![Content::text(text.to_owned())],
            display_text: text.to_owned(),
            attachments: Vec::new(),
            consumed_clipboard_ids: Vec::new(),
        });
    }

    let mut content = Vec::new();
    let mut text_parts = Vec::new();
    let mut attachments = Vec::new();
    let mut consumed_clipboard_ids = Vec::new();
    let mut file_count = 0usize;
    for segment in parse_input(text, working_dir) {
        match segment {
            InputSegment::Text(value) => {
                if !value.is_empty() {
                    text_parts.push(value.clone());
                    content.push(Content::text(value));
                }
            }
            InputSegment::ImagePath(path) => {
                file_count = file_count.saturating_add(1);
                check_file_limit(file_count)?;
                let loaded = load_from_path(&path)?;
                attachments.push(attachment_from_content(
                    &loaded.content,
                    loaded.display_name,
                    message_id,
                    attachments.len(),
                ));
                content.push(loaded.content);
            }
            InputSegment::ImageUrl(url) => {
                file_count = file_count.saturating_add(1);
                check_file_limit(file_count)?;
                let loaded = load_from_url(&url)?;
                attachments.push(attachment_from_content(
                    &loaded.content,
                    loaded.display_name,
                    message_id,
                    attachments.len(),
                ));
                content.push(loaded.content);
            }
            InputSegment::ClipboardImage(reference) => {
                let clipboard_id = reference.strip_prefix("clipboard:").unwrap_or(&reference);
                if let Some((width, height, bytes)) = pending_clipboard_images.get(clipboard_id) {
                    file_count = file_count.saturating_add(1);
                    check_file_limit(file_count)?;
                    let loaded = load_from_clipboard_rgba(*width, *height, bytes)?;
                    let display_name = format!(
                        "clipboard-{}.png",
                        clipboard_id.chars().take(8).collect::<String>()
                    );
                    attachments.push(attachment_from_content(
                        &loaded.content,
                        display_name,
                        message_id,
                        attachments.len(),
                    ));
                    content.push(loaded.content);
                    consumed_clipboard_ids.push(clipboard_id.to_owned());
                } else {
                    let value = format!("[{reference}]");
                    text_parts.push(value.clone());
                    content.push(Content::text(value));
                }
            }
        }
    }

    if content.is_empty() {
        anyhow::bail!("The message did not contain any usable text or attachments.");
    }
    Ok(PreparedInput {
        content,
        display_text: text_parts.join(" "),
        attachments,
        consumed_clipboard_ids,
    })
}

fn check_file_limit(count: usize) -> Result<()> {
    if count > MAX_FILES_PER_MESSAGE {
        anyhow::bail!("Too many files (max {MAX_FILES_PER_MESSAGE} per message)");
    }
    Ok(())
}

fn attachment_from_content(
    content: &Content,
    label: String,
    message_id: &str,
    index: usize,
) -> AttachmentPart {
    let (kind, media_type, url, embedded) = match content {
        Content::Image { image, detail: _ } => (
            AttachmentKind::Image,
            image.media_type.clone(),
            image.url.clone(),
            image.base64.is_some(),
        ),
        Content::Document { source } => (
            AttachmentKind::Document,
            Some(source.media_type.clone()),
            source.url.clone(),
            source.data.is_some(),
        ),
        _ => unreachable!("attachment loader returned non-attachment content"),
    };
    AttachmentPart {
        id: PartId::from_semantic(format!("{message_id}/attachment:{index}")),
        kind,
        label,
        media_type,
        url,
        embedded,
    }
}

fn selected_metadata(
    services: &AppServices,
    key: Option<&ModelKey>,
    model: &str,
    provider: ProviderId,
) -> Option<ModelMetadata> {
    let key = key?;
    (key.model_id == model && key.provider == provider)
        .then(|| services.model_registry.try_get_model_by_key(key))
        .flatten()
}

fn create_ai_client(services: &AppServices, metadata: &ModelMetadata) -> Option<AiClient> {
    let credential = if metadata.provider == ProviderId::Anthropic {
        mitsuro_core::auth::resolve_anthropic_auth(&services.credential_store).credential
    } else if metadata.provider == ProviderId::OpenAI {
        crate::tui_support::auth::resolve_openai_auth_for_metadata(
            metadata,
            &services.credential_store,
        )
        .credential
    } else if metadata.provider == ProviderId::Grok {
        mitsuro_core::auth::resolve_grok_auth(&services.credential_store).credential
    } else {
        services.credential_store.get_auth(&metadata.provider)
    }?;

    let config =
        crate::tui_support::auth::create_client_config(metadata, &services.credential_store);
    AiClient::new_with_resolved_model(config, credential, metadata.resolve_runtime()).ok()
}

fn reasoning_effort_label(effort: ReasoningEffort) -> String {
    match effort {
        ReasoningEffort::None => "reasoning off",
        ReasoningEffort::Minimal => "reasoning minimal",
        ReasoningEffort::Low => "reasoning low",
        ReasoningEffort::Medium => "reasoning medium",
        ReasoningEffort::High => "reasoning high",
        ReasoningEffort::XHigh => "reasoning xhigh",
        ReasoningEffort::Max => "reasoning max",
        ReasoningEffort::Ultra => "reasoning ultra",
    }
    .to_owned()
}

const fn theme_kind_key(theme: ThemeKind) -> &'static str {
    match theme {
        ThemeKind::MitsuroDark => "mitsuro_dark",
        ThemeKind::MitsuroLight => "mitsuro_light",
        ThemeKind::TerminalAdaptive => "terminal_adaptive",
        ThemeKind::HighContrast => "high_contrast",
    }
}

fn parse_theme_kind(value: &str) -> Option<ThemeKind> {
    match value {
        "mitsuro_dark" => Some(ThemeKind::MitsuroDark),
        "mitsuro_light" => Some(ThemeKind::MitsuroLight),
        "terminal_adaptive" => Some(ThemeKind::TerminalAdaptive),
        "high_contrast" => Some(ThemeKind::HighContrast),
        _ => None,
    }
}

const fn motion_preference_key(motion: MotionPreference) -> &'static str {
    match motion {
        MotionPreference::Full => "full",
        MotionPreference::Reduced => "reduced",
        MotionPreference::Off => "off",
    }
}

fn parse_motion_preference(value: &str) -> Option<MotionPreference> {
    match value {
        "full" => Some(MotionPreference::Full),
        "reduced" => Some(MotionPreference::Reduced),
        "off" => Some(MotionPreference::Off),
        _ => None,
    }
}

fn default_reasoning_effort(metadata: &ModelMetadata) -> Option<ReasoningEffort> {
    metadata.default_reasoning_level.or_else(|| {
        metadata
            .reasoning_is_mandatory
            .then(|| metadata.supported_reasoning_levels.first().copied())
            .flatten()
    })
}

fn codex_effort(effort: ReasoningEffort) -> Option<CodexReasoningEffort> {
    match effort {
        ReasoningEffort::None => None,
        ReasoningEffort::Minimal => Some(CodexReasoningEffort::Minimal),
        ReasoningEffort::Low => Some(CodexReasoningEffort::Low),
        ReasoningEffort::Medium => Some(CodexReasoningEffort::Medium),
        ReasoningEffort::High => Some(CodexReasoningEffort::High),
        ReasoningEffort::XHigh => Some(CodexReasoningEffort::XHigh),
        ReasoningEffort::Max | ReasoningEffort::Ultra => Some(CodexReasoningEffort::Max),
    }
}

fn anthropic_effort(effort: ReasoningEffort) -> Option<AnthropicAdaptiveEffort> {
    match effort {
        ReasoningEffort::None => None,
        ReasoningEffort::Minimal | ReasoningEffort::Low => Some(AnthropicAdaptiveEffort::Low),
        ReasoningEffort::Medium => Some(AnthropicAdaptiveEffort::Medium),
        ReasoningEffort::High => Some(AnthropicAdaptiveEffort::High),
        ReasoningEffort::XHigh => Some(AnthropicAdaptiveEffort::XHigh),
        ReasoningEffort::Max | ReasoningEffort::Ultra => Some(AnthropicAdaptiveEffort::Max),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_entry_index_is_relative_bounded_and_respects_ignored_directories() {
        let root = tempfile::tempdir().expect("temporary project");
        std::fs::create_dir_all(root.path().join("src")).expect("src directory");
        std::fs::create_dir_all(root.path().join("target")).expect("target directory");
        std::fs::write(root.path().join("src/main.rs"), "fn main() {}").expect("source fixture");
        std::fs::write(root.path().join("target/generated.rs"), "generated")
            .expect("ignored fixture");
        std::fs::write(root.path().join(".secret"), "hidden").expect("hidden fixture");

        let files = index_project_entries(root.path());
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "src");
        assert!(files[0].is_directory());
        assert_eq!(files[1].path, "src/main.rs");
        assert_eq!(files[1].name, "main.rs");
        assert!(!Path::new(&files[1].path).is_absolute());
        assert!(files.len() <= MAX_PROJECT_FILE_INDEX);
    }

    #[test]
    fn started_run_is_the_only_live_channel_boundary() {
        fn assert_send<T: Send>() {}
        assert_send::<StartedRun>();
    }

    #[test]
    fn provider_effort_mappings_are_explicit_and_bounded() {
        assert_eq!(codex_effort(ReasoningEffort::None), None);
        assert_eq!(
            codex_effort(ReasoningEffort::Ultra),
            Some(CodexReasoningEffort::Max)
        );
        assert_eq!(anthropic_effort(ReasoningEffort::None), None);
        assert_eq!(
            anthropic_effort(ReasoningEffort::Ultra),
            Some(AnthropicAdaptiveEffort::Max)
        );
        assert_eq!(
            reasoning_effort_label(ReasoningEffort::XHigh),
            "reasoning xhigh"
        );
    }

    #[test]
    fn prepared_input_preserves_text_order_without_rendering_payloads() {
        let mut clipboard = HashMap::new();
        let prepared = prepare_user_input(
            "Review [https://example.com/screen.png] carefully",
            Path::new("/workspace"),
            "local-7",
            &mut clipboard,
        )
        .expect("URL attachments do not require network access");

        assert_eq!(prepared.display_text, "Review carefully");
        assert_eq!(prepared.content.len(), 3);
        assert_eq!(prepared.attachments.len(), 1);
        assert_eq!(prepared.attachments[0].kind, AttachmentKind::Image);
        assert_eq!(
            prepared.attachments[0].url.as_deref(),
            Some("https://example.com/screen.png")
        );
        assert!(!format!("{:?}", prepared.attachments).contains("base64"));
    }

    #[test]
    fn clipboard_attachment_is_consumed_only_after_the_caller_commits() {
        let mut clipboard = HashMap::from([("clip-1".to_owned(), (1, 1, vec![0, 0, 0, 255]))]);
        let prepared = prepare_user_input(
            "Describe [clipboard:clip-1]",
            Path::new("/workspace"),
            "local-8",
            &mut clipboard,
        )
        .expect("valid RGBA clipboard image");

        assert!(clipboard.contains_key("clip-1"));
        assert_eq!(prepared.consumed_clipboard_ids, ["clip-1"]);
        assert_eq!(prepared.attachments.len(), 1);
        assert!(prepared.attachments[0].embedded);
    }

    #[test]
    fn v2_appearance_keys_round_trip_without_legacy_theme_aliases() {
        for theme in [
            ThemeKind::MitsuroDark,
            ThemeKind::MitsuroLight,
            ThemeKind::TerminalAdaptive,
            ThemeKind::HighContrast,
        ] {
            assert_eq!(parse_theme_kind(theme_kind_key(theme)), Some(theme));
        }
        for motion in [
            MotionPreference::Full,
            MotionPreference::Reduced,
            MotionPreference::Off,
        ] {
            assert_eq!(
                parse_motion_preference(motion_preference_key(motion)),
                Some(motion)
            );
        }
        assert_eq!(parse_theme_kind("krusty"), None);
    }
}
