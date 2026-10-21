use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{timeout, Duration};

use mitsuro_core::agent::loop_events::LoopStopReason;
use mitsuro_core::agent::subagent::{AgentProgress, AgentProgressStatus};
use mitsuro_core::agent::{
    AgentCancellation, DelegatedProgressEvent, DelegatedRunStage as CoreDelegatedRunStage,
    DelegatedToolKind as CoreDelegatedToolKind, LoopEvent, LoopInput, UserHookManager,
};
use mitsuro_core::ai::models::{
    create_model_registry, ApiFormat, ModelAuthScope, ModelKey, ModelMetadata,
};
use mitsuro_core::ai::providers::ProviderId;
use mitsuro_core::ai::types::Content;
use mitsuro_core::mcp::McpManager;
use mitsuro_core::process::ProcessRegistry;
use mitsuro_core::skills::SkillsManager;
use mitsuro_core::storage::credentials::CredentialStore;
use mitsuro_core::storage::{
    Database, DelegatedRunRole, DelegatedRunScope, DelegatedRunStartInput, DelegatedRunStore,
    PartialAssistantState, PendingInteractionSnapshot, Preferences, RecoveryDecision,
    RecoveryNonResumableReason, RecoveryStatus, SessionRecoveryState, SessionType, WorkspaceMode,
};
use mitsuro_core::tools::registry::ToolRegistry;
use mitsuro_core::SessionManager;

use super::interactions::{resolve_pending_hive_run, PendingHiveInteraction};
use super::{
    build_user_content, chat, deliver_steering_with_rollover, forward_loop_event,
    prepare_chat_contract_for_test, run_delegated_progress_bridge, run_orchestrator_event_bridge,
    select_model_for_chat_request, setup_chat_session, steer, tool_approval, RequestedModel,
};
use crate::error::AppError;
use crate::routes::test_support::current_user;
use crate::types::{ChatRequest, ContentBlock, SteerRequest, ToolApprovalRequest};
use crate::AppState;

fn create_test_state() -> (AppState, PathBuf) {
    let temp_dir =
        std::env::temp_dir().join(format!("mitsuro-server-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).expect("temp dir should exist");
    let db_path = temp_dir.join("krusty.db");
    Database::new(&db_path).expect("database should initialize");
    let working_dir = temp_dir.join("workspace");
    std::fs::create_dir_all(&working_dir).expect("workspace should exist");

    (
        AppState {
            server_port: 3000,
            db_path: Arc::new(db_path),
            working_dir: Arc::new(working_dir.clone()),
            ai_client: None,
            tool_registry: Arc::new(ToolRegistry::new()),
            process_registry: Arc::new(ProcessRegistry::new()),
            model_registry: create_model_registry(),
            credential_store: Arc::new(RwLock::new(CredentialStore::default())),
            mcp_manager: Arc::new(McpManager::new(working_dir.clone())),
            hook_manager: Arc::new(RwLock::new(UserHookManager::new())),
            skills_manager: Arc::new(RwLock::new(SkillsManager::with_defaults(&working_dir))),
            cancellation: AgentCancellation::new(),
            session_locks: Arc::new(RwLock::new(HashMap::new())),
            session_inputs: Arc::new(RwLock::new(HashMap::new())),
            session_presence: Arc::new(RwLock::new(HashMap::new())),
            session_control_leases: Arc::new(RwLock::new(
                crate::control_leases::SessionControlLeaseRegistry::default(),
            )),
            agent_streams: Arc::new(RwLock::new(crate::agent_streams::AgentStreamHub::default())),
            delegated_state: Arc::new(RwLock::new(HashMap::new())),
            remote_access: Arc::new(RwLock::new(crate::remote_access::RemoteAccessConfig {
                enabled: true,
                token: String::new(),
            })),
            active_agent_streams: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_rss_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peak_virtual_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            push_service: None,
            apns_service: None,
            oauth_flows: Arc::new(Mutex::new(HashMap::new())),
            hive_runtime: crate::hive_runtime::HiveRuntimeManager::new(),
        },
        temp_dir,
    )
}

fn create_test_user(state: &AppState, user_id: &str) {
    let db = Database::new(&state.db_path).expect("database should open");
    db.conn()
        .execute(
            "INSERT INTO users (id, email, license_tier) VALUES (?1, ?2, ?3)",
            (user_id, format!("{user_id}@example.com"), "free"),
        )
        .expect("user should insert");
}

async fn code_control_headers(state: &AppState, session_id: &str) -> HeaderMap {
    let lease = state
        .session_control_leases
        .write()
        .await
        .take(session_id, "server-test-controller".to_owned(), false)
        .expect("test controller should acquire the exact Code session lease");
    let mut headers = HeaderMap::new();
    headers.insert(
        crate::control_leases::CONTROL_LEASE_HEADER,
        lease.lease_token.parse().expect("lease token header value"),
    );
    headers
}

fn test_delegated_progress(
    session_id: &str,
    delegated_run_id: &str,
    tool_call_id: &str,
    task_id: &str,
    stage: CoreDelegatedRunStage,
    status: AgentProgressStatus,
    tool_count: usize,
) -> DelegatedProgressEvent {
    DelegatedProgressEvent {
        delegated_run_id: delegated_run_id.to_string(),
        parent_session_id: session_id.to_string(),
        tool_call_id: tool_call_id.to_string(),
        kind: CoreDelegatedToolKind::Build,
        stage,
        progress: AgentProgress {
            delegated_run_id: Some(delegated_run_id.to_string()),
            task_id: task_id.to_string(),
            name: task_id.to_string(),
            status,
            tool_count,
            current_action: Some("working".to_string()),
            ..AgentProgress::default()
        },
    }
}

fn create_test_delegated_run(
    state: &AppState,
    session_id: &str,
    delegated_run_id: &str,
    tool_call_id: &str,
) {
    DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"))
        .create_run(&DelegatedRunStartInput {
            delegated_run_id: delegated_run_id.to_string(),
            parent_session_id: session_id.to_string(),
            parent_tool_call_id: Some(tool_call_id.to_string()),
            role: DelegatedRunRole::Build,
            stage: CoreDelegatedRunStage::Created,
            provider: Some("test".to_string()),
            model: Some("test:model".to_string()),
            resumable: true,
            resumed_from_run_id: None,
            target_scope: vec![DelegatedRunScope {
                label: "workspace".to_string(),
                path: ".".to_string(),
                kind: "workspace".to_string(),
            }],
        })
        .expect("delegated run should create");
}

#[tokio::test]
async fn pending_hive_resolution_uses_durable_run_ids_not_trace_run_ids() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Hive pending identity",
            Some("test:model"),
            Some("/work"),
            Some("/work"),
            WorkspaceMode::Selected,
            None,
            None,
            SessionType::Hive,
        )
        .expect("session should create");
    let now = chrono::Utc::now().to_rfc3339();
    let db = Database::new(&state.db_path).expect("database should open");
    db.conn()
        .execute_batch(&format!(
            "INSERT INTO hive_controllers (
                id, scope_key, session_id, status, timezone, max_concurrent_runs,
                created_at, updated_at
             ) VALUES (
                'controller-1', 'session:{session_id}', '{session_id}', 'active', 'UTC', 1,
                '{now}', '{now}'
             );
             INSERT INTO hive_runs (
                id, controller_id, session_id, kind, objective, config_json, status,
                priority, available_at, attempt_count, max_attempts, created_at, updated_at
             ) VALUES (
                'durable-run-1', 'controller-1', '{session_id}', 'dispatch', 'work',
                '{{}}', 'running',
                0, '{now}', 1, 3, '{now}', '{now}'
             );"
        ))
        .expect("durable run should insert");
    db.conn()
        .execute(
            "INSERT INTO hive_controller_events (
                controller_id, sequence, event_type, run_id, payload_json, created_at
             ) VALUES ('controller-1', 1, 'agentic_event', 'durable-run-1', ?1, ?2)",
            (
                serde_json::json!({
                    "type": "tool_approval_required",
                    "id": "tool-1",
                    "name": "edit",
                    "arguments": {},
                })
                .to_string(),
                now.as_str(),
            ),
        )
        .expect("durable approval event should insert");
    db.conn()
        .execute(
            "INSERT INTO runtime_traces (
                session_id, run_id, sequence, turn, event_type, payload_json, created_at
             ) VALUES (?1, 'trace-run-deliberately-different', 1, 1,
                       'tool_approval_required', ?2, ?3)",
            (
                session_id.as_str(),
                serde_json::json!({"id": "tool-1"}).to_string(),
                now.as_str(),
            ),
        )
        .expect("diagnostic trace should insert");

    assert!(matches!(
        resolve_pending_hive_run(
            &state,
            &session_id,
            "tool-1",
            None,
            PendingHiveInteraction::ToolApproval,
        ),
        Ok(run_id) if run_id == "durable-run-1"
    ));

    db.conn()
        .execute(
            "INSERT INTO hive_controller_events (
                controller_id, sequence, event_type, run_id, payload_json, created_at
             ) VALUES ('controller-1', 2, 'tool_approval_queued', 'durable-run-1', ?1, ?2)",
            (
                serde_json::json!({"tool_call_id": "tool-1"}).to_string(),
                now.as_str(),
            ),
        )
        .expect("durable settlement should insert");
    assert!(matches!(
        resolve_pending_hive_run(
            &state,
            &session_id,
            "tool-1",
            None,
            PendingHiveInteraction::ToolApproval,
        ),
        Err(AppError::Conflict(_))
    ));
}

#[tokio::test]
async fn chat_rejects_mako_creation_and_daemon_owned_metadata_overrides() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Daemon Mako",
            Some("test:model"),
            Some("/work"),
            Some("/work"),
            WorkspaceMode::Selected,
            None,
            None,
            SessionType::Hive,
        )
        .expect("session should create");

    let new_mako = chat(
        State(state.clone()),
        None,
        HeaderMap::new(),
        Json(ChatRequest {
            session_id: None,
            message: "start".into(),
            content: Vec::new(),
            project_dir: Some("/work".into()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Selected),
            target_branch: None,
            session_type: Some(SessionType::Hive),
            model: Some("test:model".into()),
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            mode: None,
            permission_mode: None,
            fast_mode: false,
            research_enabled: None,
            allowed_tools: None,
        }),
    )
    .await;
    assert!(matches!(new_mako, Err(AppError::Conflict(_))));

    let override_attempt = chat(
        State(state),
        None,
        HeaderMap::new(),
        Json(ChatRequest {
            session_id: Some(session_id.clone()),
            message: "continue".into(),
            content: Vec::new(),
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            model: Some("test:other-model".into()),
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            mode: None,
            permission_mode: None,
            fast_mode: false,
            research_enabled: None,
            allowed_tools: None,
        }),
    )
    .await;
    assert!(matches!(override_attempt, Err(AppError::Conflict(_))));
    assert_eq!(
        session_manager
            .get_session(&session_id)
            .expect("session should load")
            .expect("session should exist")
            .model
            .as_deref(),
        Some("test:model")
    );
}

#[tokio::test]
async fn chat_new_session_contract_includes_target_branch_intent() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let project_parent = user_root.join("projects");
    std::fs::create_dir_all(&project_parent).expect("project parent should exist");
    let project_dir = project_parent.join("fresh-contract-repo");

    let contract = prepare_chat_contract_for_test(
        &state,
        Some(current_user("alice", &user_root)),
        ChatRequest {
            session_id: None,
            message: "continue in this workspace".to_string(),
            content: Vec::new(),
            project_dir: Some(project_dir.to_string_lossy().to_string()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Created),
            target_branch: Some("feature/mobile-continuation".to_string()),
            session_type: Some(SessionType::Code),
            model: Some("openai/gpt-5.5".to_string()),
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            fast_mode: true,
            mode: None,
            permission_mode: Some(mitsuro_core::tools::registry::PermissionMode::default()),
            research_enabled: None,
            allowed_tools: None,
        },
    )
    .await
    .unwrap_or_else(|_| panic!("chat contract should prepare without a real AI client"));

    let expected_project = project_dir.to_string_lossy().to_string();
    assert!(contract.is_first_message);
    assert_eq!(contract.session_type, SessionType::Code);
    assert_eq!(contract.workspace_mode, WorkspaceMode::Created);
    assert_eq!(contract.working_dir.to_string_lossy(), expected_project);
    assert_eq!(
        contract
            .project_dir
            .as_ref()
            .map(|path| path.to_string_lossy().to_string())
            .as_deref(),
        Some(expected_project.as_str())
    );
    assert_eq!(
        contract.target_branch.as_deref(),
        Some("feature/mobile-continuation")
    );
    assert_eq!(contract.model.as_deref(), Some("openai/gpt-5.5"));
    assert!(contract.fast_mode);
    assert!(
        !contract
            .model
            .as_deref()
            .expect("model should be present")
            .contains("mini"),
        "fast mode must stay independent from mini model selection"
    );
}

#[tokio::test]
async fn chat_existing_session_uses_persisted_target_branch_intent_when_request_omits_override() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let persisted_project = user_root.join("repo");
    let ignored_project = user_root.join("ignored-repo");
    std::fs::create_dir_all(&persisted_project).expect("persisted project should exist");
    std::fs::create_dir_all(&ignored_project).expect("ignored project should exist");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config_and_permission(
            "Persisted Contract",
            Some("openai/gpt-5.5-mini"),
            Some(persisted_project.to_string_lossy().as_ref()),
            Some(persisted_project.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            Some("feature/contract"),
            SessionType::Code,
            mitsuro_core::tools::registry::PermissionMode::Supervised,
        )
        .expect("session should be created");

    let contract = prepare_chat_contract_for_test(
        &state,
        Some(current_user("alice", &user_root)),
        ChatRequest {
            session_id: Some(session_id.clone()),
            message: "continue the existing work".to_string(),
            content: Vec::new(),
            project_dir: Some(ignored_project.to_string_lossy().to_string()),
            working_dir: Some(ignored_project.to_string_lossy().to_string()),
            workspace_mode: Some(WorkspaceMode::Created),
            target_branch: None,
            session_type: Some(SessionType::Chat),
            model: None,
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            fast_mode: true,
            mode: None,
            permission_mode: None,
            research_enabled: None,
            allowed_tools: None,
        },
    )
    .await
    .unwrap_or_else(|_| {
        panic!("existing session contract should prepare without a real AI client")
    });

    let expected_project = persisted_project.to_string_lossy().to_string();
    assert!(contract.is_first_message);
    assert_eq!(contract.session_id, session_id);
    assert_eq!(contract.session_type, SessionType::Code);
    assert_eq!(contract.workspace_mode, WorkspaceMode::Selected);
    assert_eq!(contract.working_dir.to_string_lossy(), expected_project);
    assert_eq!(
        contract
            .project_dir
            .as_ref()
            .map(|path| path.to_string_lossy().to_string())
            .as_deref(),
        Some(expected_project.as_str())
    );
    assert_eq!(contract.model.as_deref(), Some("openai/gpt-5.5-mini"));
    assert_eq!(contract.target_branch.as_deref(), Some("feature/contract"));
    assert_eq!(
        contract.permission_mode,
        mitsuro_core::tools::registry::PermissionMode::Supervised
    );
    assert!(contract.fast_mode);
}

#[tokio::test]
async fn chat_existing_session_allows_explicit_target_branch_intent_override() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let persisted_project = user_root.join("repo");
    let ignored_project = user_root.join("ignored-repo");
    std::fs::create_dir_all(&persisted_project).expect("persisted project should exist");
    std::fs::create_dir_all(&ignored_project).expect("ignored project should exist");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Persisted Contract",
            Some("openai/gpt-5.5-mini"),
            Some(persisted_project.to_string_lossy().as_ref()),
            Some(persisted_project.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            Some("feature/persisted"),
            SessionType::Code,
        )
        .expect("session should be created");

    let contract = prepare_chat_contract_for_test(
        &state,
        Some(current_user("alice", &user_root)),
        ChatRequest {
            session_id: Some(session_id.clone()),
            message: "continue with requested target".to_string(),
            content: Vec::new(),
            project_dir: Some(ignored_project.to_string_lossy().to_string()),
            working_dir: Some(ignored_project.to_string_lossy().to_string()),
            workspace_mode: Some(WorkspaceMode::Created),
            target_branch: Some("  feature/request-override  ".to_string()),
            session_type: Some(SessionType::Chat),
            model: None,
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            fast_mode: false,
            mode: None,
            permission_mode: Some(mitsuro_core::tools::registry::PermissionMode::default()),
            research_enabled: None,
            allowed_tools: None,
        },
    )
    .await
    .unwrap_or_else(|_| {
        panic!("existing session contract should prepare without a real AI client")
    });

    let expected_project = persisted_project.to_string_lossy().to_string();
    assert_eq!(contract.session_id, session_id);
    assert_eq!(contract.session_type, SessionType::Code);
    assert_eq!(contract.workspace_mode, WorkspaceMode::Selected);
    assert_eq!(contract.working_dir.to_string_lossy(), expected_project);
    assert_eq!(
        contract.target_branch.as_deref(),
        Some("feature/request-override")
    );

    let reloaded = session_manager
        .get_session(&session_id)
        .expect("session lookup should succeed")
        .expect("session should exist");
    assert_eq!(
        reloaded.target_branch.as_deref(),
        Some("feature/request-override")
    );
}

fn model(
    id: &str,
    provider: ProviderId,
    api_format: ApiFormat,
    supports_vision: bool,
) -> ModelMetadata {
    let mut model = ModelMetadata::new(id, id, provider).with_context(200_000, 32_768);
    model.supports_tools = true;
    model.supports_vision = supports_vision;
    model.api_format = api_format;
    model
}

#[test]
fn requested_model_prefers_request_and_trims_input() {
    let requested_model = RequestedModel::from_request(Some("  openai/gpt-5  "));

    assert_eq!(
        requested_model.effective(Some("minimax/m2")),
        Some("openai/gpt-5")
    );
    assert_eq!(requested_model.persisted(), Some(Some("openai/gpt-5")));
}

#[test]
fn requested_model_falls_back_to_session_model_when_unspecified() {
    let requested_model = RequestedModel::from_request(None);

    assert_eq!(
        requested_model.effective(Some("  anthropic/claude-opus-4.6  ")),
        Some("anthropic/claude-opus-4.6")
    );
    assert_eq!(requested_model.persisted(), None);
}

#[test]
fn requested_model_unspecified_ignores_empty_session_values() {
    let requested_model = RequestedModel::from_request(None);

    assert_eq!(requested_model.effective(Some("   ")), None);
    assert_eq!(requested_model.persisted(), None);
}

#[test]
fn requested_model_clear_does_not_fall_back_to_session_model() {
    let requested_model = RequestedModel::from_request(Some("   "));

    assert_eq!(
        requested_model.effective(Some("anthropic/claude-opus-4.6")),
        None
    );
    assert_eq!(requested_model.persisted(), Some(None));
}

#[test]
fn requested_model_rejects_mismatched_legacy_slug_and_exact_key() {
    let key = ModelKey::new(ProviderId::Grok, "grok-4.5", ApiFormat::OpenAIResponses);
    let result = RequestedModel::from_request_parts(Some("grok-4.1"), Some(&key));

    assert!(matches!(
        result,
        Err(AppError::BadRequest(message)) if message.contains("does not match")
    ));
}

#[tokio::test]
async fn setup_chat_session_prefers_persisted_exact_key_over_ambiguous_slug() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    std::fs::create_dir_all(&user_root).expect("user workspace should exist");
    let user = current_user("alice", &user_root);
    let minimax = model(
        "shared-model",
        ProviderId::MiniMax,
        ApiFormat::Anthropic,
        false,
    );
    let openrouter = model(
        "shared-model",
        ProviderId::OpenRouter,
        ApiFormat::OpenAI,
        true,
    );
    let expected_key = openrouter.key();
    state
        .model_registry
        .set_models(ProviderId::MiniMax, vec![minimax])
        .await;
    state
        .model_registry
        .set_models(ProviderId::OpenRouter, vec![openrouter])
        .await;
    {
        let mut credentials = state.credential_store.write().await;
        credentials.set(ProviderId::MiniMax, "minimax-test-key".to_string());
        credentials.set(ProviderId::OpenRouter, "openrouter-test-key".to_string());
    }

    let manager = SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = manager
        .create_session_for_user(
            "Exact selection",
            Some("shared-model"),
            Some(user_root.to_string_lossy().as_ref()),
            Some("alice"),
        )
        .expect("session should create");
    manager
        .update_session_model_selection(&session_id, Some(&expected_key), Some("catalog-test"))
        .expect("exact model selection should persist");

    let context = setup_chat_session(
        &state,
        Some(&user),
        &session_id,
        RequestedModel::Unspecified,
        crate::types::ThinkingLevel::Off,
        false,
        true,
    )
    .await
    .unwrap_or_else(|_| panic!("exact model selection should bootstrap"));

    assert_eq!(context.ai_client.provider_id(), ProviderId::OpenRouter);
    assert_eq!(context.ai_client.resolved_model().key, expected_key);
}

#[tokio::test]
async fn app_state_model_bootstrap_rejects_ambiguous_slug_and_stale_exact_key() {
    let (state, _temp_dir) = create_test_state();
    let mut api = model(
        "shared-openai",
        ProviderId::OpenAI,
        ApiFormat::OpenAIResponses,
        true,
    );
    api.auth_scope = Some(ModelAuthScope::ApiKey);
    let mut oauth = api.clone();
    oauth.display_name = "Shared OAuth".to_string();
    oauth.auth_scope = Some(ModelAuthScope::OAuth);
    state
        .model_registry
        .set_models(ProviderId::OpenAI, vec![api, oauth])
        .await;

    assert!(state
        .resolve_ai_client_for_user(Some("shared-openai"), None)
        .await
        .is_none());

    let stale = ModelKey::new(
        ProviderId::OpenAI,
        "shared-openai",
        ApiFormat::OpenAIResponses,
    );
    assert!(state
        .resolve_ai_client_for_key_for_user(&stale, None)
        .await
        .is_none());
}

#[tokio::test]
async fn new_empty_session_uses_project_model_before_user_preference_and_persists_exact_row() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let project_dir = user_root.join("project");
    std::fs::create_dir_all(project_dir.join(".krusty"))
        .expect("project settings directory should exist");

    let mut preferred = model(
        "preferred-model",
        ProviderId::MiniMax,
        ApiFormat::Anthropic,
        false,
    );
    preferred.catalog_revision = Some("preferred-rev".to_string());
    let preferred_key = preferred.key();
    let mut project = model(
        "project-model",
        ProviderId::OpenRouter,
        ApiFormat::OpenAI,
        false,
    );
    project.catalog_revision = Some("project-rev".to_string());
    let project_key = project.key();
    state
        .model_registry
        .set_models(ProviderId::MiniMax, vec![preferred])
        .await;
    state
        .model_registry
        .set_models(ProviderId::OpenRouter, vec![project])
        .await;

    Preferences::for_user(
        Database::new(&state.db_path).expect("database should open"),
        "alice",
    )
    .set_current_model_key(&preferred_key)
    .expect("preference should persist");
    std::fs::write(
        project_dir.join(".krusty/settings.json"),
        serde_json::to_vec_pretty(&serde_json::json!({ "model": project_key.clone() }))
            .expect("settings should serialize"),
    )
    .expect("settings should write");

    let contract = prepare_chat_contract_for_test(
        &state,
        Some(current_user("alice", &user_root)),
        ChatRequest {
            session_id: None,
            message: "use the project model".to_string(),
            content: Vec::new(),
            project_dir: Some(project_dir.to_string_lossy().into_owned()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Selected),
            target_branch: None,
            session_type: Some(SessionType::Code),
            model: None,
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            fast_mode: false,
            mode: None,
            permission_mode: None,
            research_enabled: None,
            allowed_tools: None,
        },
    )
    .await
    .unwrap_or_else(|_| panic!("project model should create the session"));

    assert_eq!(contract.model.as_deref(), Some("project-model"));
    assert_eq!(contract.model_key, Some(project_key));
    assert_eq!(
        contract.model_catalog_revision.as_deref(),
        Some("project-rev")
    );
}

#[tokio::test]
async fn explicit_then_persisted_session_model_precede_project_model() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let project_dir = user_root.join("project");
    std::fs::create_dir_all(project_dir.join(".krusty"))
        .expect("project settings directory should exist");

    let persisted = model(
        "persisted-model",
        ProviderId::MiniMax,
        ApiFormat::Anthropic,
        false,
    );
    let persisted_key = persisted.key();
    let project = model(
        "project-model",
        ProviderId::OpenRouter,
        ApiFormat::OpenAI,
        false,
    );
    let project_key = project.key();
    let explicit = model(
        "explicit-model",
        ProviderId::OpenRouter,
        ApiFormat::OpenAI,
        false,
    );
    let explicit_key = explicit.key();
    state
        .model_registry
        .set_models(ProviderId::MiniMax, vec![persisted])
        .await;
    state
        .model_registry
        .set_models(ProviderId::OpenRouter, vec![project, explicit])
        .await;
    {
        let mut credentials = state.credential_store.write().await;
        credentials.set(ProviderId::MiniMax, "minimax-test-key".to_string());
        credentials.set(ProviderId::OpenRouter, "router-test-key".to_string());
    }
    std::fs::write(
        project_dir.join(".krusty/settings.json"),
        serde_json::to_vec_pretty(&serde_json::json!({ "model": project_key.clone() }))
            .expect("settings should serialize"),
    )
    .expect("settings should write");

    let manager = SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = manager
        .create_session_for_user_with_config(
            "Project precedence",
            Some("persisted-model"),
            Some(project_dir.to_string_lossy().as_ref()),
            Some(project_dir.to_string_lossy().as_ref()),
            WorkspaceMode::Selected,
            Some("alice"),
            None,
            SessionType::Code,
        )
        .expect("session should create");
    manager
        .update_session_model_selection(&session_id, Some(&persisted_key), None)
        .expect("persisted exact selection should save");
    let user = current_user("alice", &user_root);

    let persisted_context = setup_chat_session(
        &state,
        Some(&user),
        &session_id,
        RequestedModel::Unspecified,
        crate::types::ThinkingLevel::Off,
        false,
        false,
    )
    .await
    .unwrap_or_else(|_| panic!("persisted model should resolve"));
    assert_eq!(
        persisted_context.ai_client.resolved_model().key,
        persisted_key
    );
    drop(persisted_context);

    let explicit_context = setup_chat_session(
        &state,
        Some(&user),
        &session_id,
        RequestedModel::Exact(&explicit_key),
        crate::types::ThinkingLevel::Off,
        false,
        false,
    )
    .await
    .unwrap_or_else(|_| panic!("explicit model should resolve"));
    assert_eq!(
        explicit_context.ai_client.resolved_model().key,
        explicit_key
    );
}

#[test]
fn build_user_content_appends_message_when_only_images_are_provided() {
    let content = match build_user_content(
        "describe this",
        &[ContentBlock::Image {
            source: crate::types::ImageSource::Url {
                url: "https://example.com/image.png".to_string(),
            },
        }],
    ) {
        Ok(content) => content,
        Err(_) => panic!("image url content should build"),
    };

    assert!(matches!(content.first(), Some(Content::Image { .. })));
    assert!(matches!(
        content.last(),
        Some(Content::Text { text, .. }) if text == "describe this"
    ));
}

#[test]
fn build_user_content_does_not_duplicate_message_when_text_block_exists() {
    let content = match build_user_content(
        "fallback message",
        &[ContentBlock::Text {
            text: "block text".to_string(),
        }],
    ) {
        Ok(content) => content,
        Err(_) => panic!("text content should build"),
    };

    assert_eq!(content.len(), 1);
    assert!(matches!(
        content.first(),
        Some(Content::Text { text, .. }) if text == "block text"
    ));
}

#[test]
fn build_user_content_preserves_file_attachment_text_block() {
    let file_text = "Please review the attached file.\n\n--- notes.txt ---\nhello from file";
    let content = match build_user_content(
        "fallback message",
        &[ContentBlock::Text {
            text: file_text.to_string(),
        }],
    ) {
        Ok(content) => content,
        Err(_) => panic!("file attachment text block should build"),
    };

    assert_eq!(content.len(), 1);
    assert!(matches!(
        content.first(),
        Some(Content::Text { text, .. }) if text == file_text
    ));
}

#[test]
fn build_user_content_rejects_unsupported_image_media_type() {
    let result = build_user_content(
        "describe this",
        &[ContentBlock::Image {
            source: crate::types::ImageSource::Base64 {
                media_type: "image/heic".to_string(),
                data: "ZmFrZQ==".to_string(),
            },
        }],
    );

    assert!(matches!(
        result,
        Err(AppError::BadRequest(message))
            if message.contains("image/heic") && message.contains("Convert HEIC/HEIF")
    ));
}

#[tokio::test]
async fn select_model_for_chat_request_rejects_non_vision_session_model() {
    let (state, _temp_dir) = create_test_state();
    {
        let mut credentials = state.credential_store.write().await;
        credentials.set(ProviderId::MiniMax, "minimax-test-key".to_string());
        credentials.set(ProviderId::OpenAI, "openai-test-key".to_string());
    }

    state
        .model_registry
        .set_models(
            ProviderId::MiniMax,
            vec![model(
                "MiniMax-M2.5",
                ProviderId::MiniMax,
                ApiFormat::Anthropic,
                false,
            )],
        )
        .await;
    state
        .model_registry
        .set_models(
            ProviderId::OpenAI,
            vec![model(
                "gpt-4.1",
                ProviderId::OpenAI,
                ApiFormat::OpenAI,
                true,
            )],
        )
        .await;

    let selected = select_model_for_chat_request(
        &state,
        RequestedModel::Unspecified,
        Some("MiniMax-M2.5"),
        true,
    )
    .await;

    // Silent model substitution is a product rule violation: the request must
    // fail explicitly instead of switching to another vision model.
    match selected {
        Ok(_) => panic!("non-vision session model must not be silently substituted"),
        Err(AppError::BadRequest(detail)) => {
            assert!(detail.contains("does not support image input"));
        }
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn select_model_for_chat_request_rejects_explicit_non_vision_model() {
    let (state, _temp_dir) = create_test_state();
    {
        let mut credentials = state.credential_store.write().await;
        credentials.set(ProviderId::MiniMax, "minimax-test-key".to_string());
    }

    state
        .model_registry
        .set_models(
            ProviderId::MiniMax,
            vec![model(
                "MiniMax-M2.5",
                ProviderId::MiniMax,
                ApiFormat::Anthropic,
                false,
            )],
        )
        .await;

    let result =
        select_model_for_chat_request(&state, RequestedModel::Set("MiniMax-M2.5"), None, true)
            .await;

    assert!(matches!(
        result,
        Err(AppError::BadRequest(message))
            if message.contains("does not support image input")
    ));
}

#[tokio::test]
async fn tool_approval_rejects_foreign_owner() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Owned Session", None, None, Some("alice"))
        .expect("session creation should succeed");

    let (tx, mut rx) = mpsc::unbounded_channel::<LoopInput>();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), tx);

    let result = tool_approval(
        State(state),
        Some(current_user("bob", std::path::Path::new("/tmp"))),
        HeaderMap::new(),
        Json(ToolApprovalRequest {
            session_id,
            run_id: None,
            tool_call_id: "tool-1".to_string(),
            approved: true,
        }),
    )
    .await;

    assert!(matches!(result, Err(AppError::NotFound(_))));
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn live_steering_retries_the_replacement_run_sender_once() {
    let (state, _temp_dir) = create_test_state();
    let session_id = "rollover-session".to_string();
    let (stale_tx, stale_rx) = mpsc::unbounded_channel();
    drop(stale_rx);
    let (replacement_tx, mut replacement_rx) = mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), replacement_tx);
    let input = LoopInput::Steer {
        pending_id: Some("steer-rollover".into()),
        content: vec![Content::text("use the new run".into())],
    };

    assert!(
        deliver_steering_with_rollover(&state, &session_id, stale_tx, input).await,
        "replacement sender should accept the same durable steering input"
    );
    assert!(matches!(
        replacement_rx.recv().await,
        Some(LoopInput::Steer {
            pending_id: Some(pending_id),
            content,
        }) if pending_id == "steer-rollover"
            && matches!(content.first(), Some(Content::Text { text, .. }) if text == "use the new run")
    ));
}

#[tokio::test]
async fn live_steering_is_owner_checked_and_hidden_until_core_injection() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    create_test_user(&state, "bob");
    let manager = SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = manager
        .create_session_for_user("Owned Session", None, None, Some("alice"))
        .expect("session should be created");
    let (input_tx, mut input_rx) = mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);

    let foreign = steer(
        State(state.clone()),
        Some(current_user("bob", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(SteerRequest {
            session_id: session_id.clone(),
            message: "foreign".into(),
            content: Vec::new(),
        }),
    )
    .await;
    assert!(matches!(foreign, Err(AppError::NotFound(_))));
    assert!(input_rx.try_recv().is_err());

    let control_headers = code_control_headers(&state, &session_id).await;
    let accepted = steer(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        control_headers,
        Json(SteerRequest {
            session_id: session_id.clone(),
            message: "change direction".into(),
            content: Vec::new(),
        }),
    )
    .await;
    let accepted = match accepted {
        Ok(response) => response,
        Err(_) => panic!("owner steering should be accepted"),
    };
    assert_eq!(accepted.0["status"], "accepted");
    assert!(matches!(
        input_rx.recv().await,
        Some(LoopInput::Steer {
            pending_id: Some(_),
            content,
        }) if matches!(content.first(), Some(Content::Text { text, .. }) if text == "change direction")
    ));
    assert!(
        manager
            .load_session_messages(&session_id)
            .expect("canonical history should load")
            .is_empty(),
        "durable steering must stay hidden before the core reaches a safe boundary"
    );
}

#[tokio::test]
async fn tool_approval_rejects_closed_session_channel() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Owned Session", None, None, Some("alice"))
        .expect("session creation should succeed");

    let (tx, rx) = mpsc::unbounded_channel::<LoopInput>();
    drop(rx);
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), tx);

    let result = tool_approval(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        HeaderMap::new(),
        Json(ToolApprovalRequest {
            session_id,
            run_id: None,
            tool_call_id: "tool-1".to_string(),
            approved: true,
        }),
    )
    .await;

    assert!(matches!(result, Err(AppError::Conflict(_))));
}

#[tokio::test]
async fn submit_tool_approval_returns_recoverable_pending_approval_error_when_channel_missing() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Owned Session", None, None, Some("alice"))
        .expect("session creation should succeed");
    let pending_interaction = PendingInteractionSnapshot::tool_approval_from_call(
        "tool-1",
        "edit",
        &serde_json::json!({ "file_path": "src/lib.rs", "api_token": "hidden" }),
    );
    let recovery = SessionRecoveryState::new_with_pending_interactions(
        RecoveryStatus::AwaitingInput,
        Some(LoopStopReason::AwaitingInput),
        None,
        PartialAssistantState::default(),
        vec![pending_interaction],
        RecoveryDecision::NonResumable {
            reason: RecoveryNonResumableReason::AwaitingHumanInput,
        },
    );
    session_manager
        .update_recovery_state(&session_id, &recovery)
        .expect("pending approval recovery should persist");

    let control_headers = code_control_headers(&state, &session_id).await;
    let result = tool_approval(
        State(state),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        control_headers,
        Json(ToolApprovalRequest {
            session_id: session_id.clone(),
            run_id: None,
            tool_call_id: "tool-1".to_string(),
            approved: true,
        }),
    )
    .await;

    assert!(matches!(
        result,
        Err(AppError::Conflict(message))
            if message.contains(&session_id)
                && message.contains("tool-1")
                && message.contains("approval channel unavailable")
                && message.contains("recoverable")
    ));
}

#[tokio::test]
async fn tool_approval_survives_sse_disconnect_until_run_finishes() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Owned Session", None, None, Some("alice"))
        .expect("session creation should succeed");

    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<LoopInput>();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);

    let (event_tx, event_rx) = mpsc::unbounded_channel::<LoopEvent>();
    let (sse_tx, sse_rx) = mpsc::channel(1);
    drop(sse_rx);

    let bridge = tokio::spawn(run_orchestrator_event_bridge(
        event_rx,
        sse_tx,
        session_id.clone(),
        Arc::clone(&state.session_inputs),
        None,
        None,
        Some("alice".to_string()),
        Arc::clone(&state.db_path),
        None,
        Arc::new(Mutex::new(true)),
    ));

    event_tx
        .send(LoopEvent::ToolApprovalRequired {
            id: "tool-1".to_string(),
            name: "edit".to_string(),
            arguments: serde_json::json!({}),
        })
        .expect("tool approval event should send");

    timeout(Duration::from_secs(1), async {
        loop {
            if state.session_inputs.read().await.contains_key(&session_id) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("session input should stay registered after stream loss");

    let control_headers = code_control_headers(&state, &session_id).await;
    let approval = tool_approval(
        State(state.clone()),
        Some(current_user("alice", std::path::Path::new("/tmp"))),
        control_headers,
        Json(ToolApprovalRequest {
            session_id: session_id.clone(),
            run_id: None,
            tool_call_id: "tool-1".to_string(),
            approved: true,
        }),
    )
    .await;

    assert!(approval.is_ok());
    assert!(matches!(
        timeout(Duration::from_secs(1), input_rx.recv()).await,
        Ok(Some(LoopInput::ToolApproval {
            tool_call_id,
            approved: true,
        })) if tool_call_id == "tool-1"
    ));

    event_tx
        .send(LoopEvent::Finished {
            session_id: session_id.clone(),
            stop_reason: LoopStopReason::Completed,
        })
        .expect("finish event should send");

    bridge.await.expect("bridge should finish");
    assert!(state.session_inputs.read().await.get(&session_id).is_none());
}

#[tokio::test]
async fn delegated_progress_survives_sse_disconnect_until_durable_terminal_state() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Delegated progress", None, None, None)
        .expect("session should create");
    let delegated_run_id = "delegated-run-live";
    let tool_call_id = "tool-agent-live";
    create_test_delegated_run(&state, &session_id, delegated_run_id, tool_call_id);

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let (sse_tx, mut sse_rx) = mpsc::channel(8);
    let bridge = tokio::spawn(run_delegated_progress_bridge(
        progress_rx,
        sse_tx.downgrade(),
        Arc::new(Mutex::new(true)),
        session_id.clone(),
        Arc::clone(&state.delegated_state),
        Arc::clone(&state.db_path),
        None,
    ));

    // One builder completing is not terminal for a parallel build while the
    // durable aggregate remains active. The client must receive a running run
    // with a completed component, not a prematurely completed tool card.
    progress_tx
        .send(test_delegated_progress(
            &session_id,
            delegated_run_id,
            tool_call_id,
            "builder-a",
            CoreDelegatedRunStage::Complete,
            AgentProgressStatus::Complete,
            3,
        ))
        .expect("component progress should send");
    let first_sse = timeout(Duration::from_secs(1), sse_rx.recv())
        .await
        .expect("delegated progress should reach SSE")
        .expect("SSE sender should remain open")
        .expect("SSE event should serialize");
    let first_sse = format!("{first_sse:?}");
    assert!(first_sse.contains("delegated_progress"));
    assert!(first_sse.contains("running"));
    {
        let state_snapshot = state.delegated_state.read().await;
        let tool = state_snapshot
            .get(&session_id)
            .and_then(|tools| tools.first())
            .expect("live delegated snapshot should exist");
        assert!(matches!(
            tool.stage,
            crate::types::DelegatedRunStage::Running
        ));
        assert!(matches!(
            tool.agents.first().map(|agent| agent.status),
            Some(crate::types::DelegatedProgressStatus::Complete)
        ));
    }

    // The progress worker keeps only a weak SSE sender. Ending the parent SSE
    // therefore closes the response immediately without cancelling detached
    // child state tracking.
    drop(sse_tx);
    assert!(matches!(
        timeout(Duration::from_secs(1), sse_rx.recv()).await,
        Ok(None)
    ));
    progress_tx
        .send(test_delegated_progress(
            &session_id,
            delegated_run_id,
            tool_call_id,
            "builder-b",
            CoreDelegatedRunStage::Running,
            AgentProgressStatus::Running,
            4,
        ))
        .expect("detached progress should send");
    timeout(Duration::from_secs(1), async {
        loop {
            let updated = state
                .delegated_state
                .read()
                .await
                .get(&session_id)
                .and_then(|tools| tools.first())
                .is_some_and(|tool| {
                    tool.agents
                        .iter()
                        .any(|agent| agent.task_id == "builder-b" && agent.tool_count == 4)
                });
            if updated {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("live state should update after SSE disconnect");

    DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"))
        .finalize_run(
            delegated_run_id,
            CoreDelegatedRunStage::Degraded,
            &serde_json::json!({
                "delegated_run_id": delegated_run_id,
                "outcome": "partial",
            }),
            Some("Partial evidence retained"),
            true,
        )
        .expect("durable terminal state should persist");
    let mut terminal = test_delegated_progress(
        &session_id,
        delegated_run_id,
        tool_call_id,
        "builder-b",
        CoreDelegatedRunStage::Failed,
        AgentProgressStatus::Failed,
        4,
    );
    terminal.progress.current_action = Some("degraded".to_string());
    progress_tx
        .send(terminal)
        .expect("terminal delegated progress should send");
    timeout(Duration::from_secs(1), async {
        loop {
            if state
                .delegated_state
                .read()
                .await
                .get(&session_id)
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durably terminal run should leave live snapshots");

    drop(progress_tx);
    bridge
        .await
        .expect("delegated progress bridge should finish");
}

#[tokio::test]
async fn foreground_finish_closes_sse_while_detached_progress_keeps_updating_state() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Detached progress", None, None, None)
        .expect("session should create");
    create_test_delegated_run(&state, &session_id, "run-detached", "tool-detached");

    let (input_tx, _input_rx) = mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let (sse_tx, mut sse_rx) = mpsc::channel(8);
    let sse_open = Arc::new(Mutex::new(true));
    let progress_bridge = tokio::spawn(run_delegated_progress_bridge(
        progress_rx,
        sse_tx.downgrade(),
        Arc::clone(&sse_open),
        session_id.clone(),
        Arc::clone(&state.delegated_state),
        Arc::clone(&state.db_path),
        None,
    ));
    let foreground_bridge = tokio::spawn(run_orchestrator_event_bridge(
        event_rx,
        sse_tx,
        session_id.clone(),
        Arc::clone(&state.session_inputs),
        None,
        None,
        None,
        Arc::clone(&state.db_path),
        None,
        Arc::clone(&sse_open),
    ));

    progress_tx
        .send(test_delegated_progress(
            &session_id,
            "run-detached",
            "tool-detached",
            "builder",
            CoreDelegatedRunStage::Running,
            AgentProgressStatus::Running,
            1,
        ))
        .expect("initial delegated progress should send");
    let live = timeout(Duration::from_secs(1), sse_rx.recv())
        .await
        .expect("live progress should arrive")
        .expect("SSE should be open")
        .expect("SSE event should serialize");
    assert!(format!("{live:?}").contains("delegated_progress"));

    event_tx
        .send(LoopEvent::Finished {
            session_id: session_id.clone(),
            stop_reason: LoopStopReason::Completed,
        })
        .expect("foreground finish should send");
    let finish = timeout(Duration::from_secs(1), sse_rx.recv())
        .await
        .expect("finish should arrive")
        .expect("SSE should remain open through finish")
        .expect("finish event should serialize");
    assert!(format!("{finish:?}").contains("finish"));
    foreground_bridge
        .await
        .expect("foreground event bridge should finish");
    assert!(state.session_inputs.read().await.get(&session_id).is_none());
    assert!(matches!(
        timeout(Duration::from_secs(1), sse_rx.recv()).await,
        Ok(None)
    ));

    progress_tx
        .send(test_delegated_progress(
            &session_id,
            "run-detached",
            "tool-detached",
            "builder",
            CoreDelegatedRunStage::Running,
            AgentProgressStatus::Running,
            7,
        ))
        .expect("late detached progress should send");
    timeout(Duration::from_secs(1), async {
        loop {
            let updated = state
                .delegated_state
                .read()
                .await
                .get(&session_id)
                .and_then(|tools| tools.first())
                .and_then(|tool| tool.agents.first())
                .is_some_and(|agent| agent.tool_count == 7);
            if updated {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached progress should remain live after foreground finish");

    drop(progress_tx);
    progress_bridge
        .await
        .expect("detached progress bridge should finish");
}

#[tokio::test]
async fn terminal_delegated_progress_survives_full_sse_buffer_with_lag_signal() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Terminal progress", None, None, None)
        .expect("session should create");
    let delegated_run_id = "run-terminal-buffer";
    let tool_call_id = "tool-terminal-buffer";
    create_test_delegated_run(&state, &session_id, delegated_run_id, tool_call_id);

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let (sse_tx, mut sse_rx) = mpsc::channel(1);
    let bridge = tokio::spawn(run_delegated_progress_bridge(
        progress_rx,
        sse_tx.downgrade(),
        Arc::new(Mutex::new(true)),
        session_id.clone(),
        Arc::clone(&state.delegated_state),
        Arc::clone(&state.db_path),
        None,
    ));
    let mut loop_skipped = 0;
    assert!(
        forward_loop_event(
            &sse_tx,
            &session_id,
            LoopEvent::TextDelta {
                delta: "occupy queue".to_string(),
            },
            &mut loop_skipped,
        )
        .await
    );
    progress_tx
        .send(test_delegated_progress(
            &session_id,
            delegated_run_id,
            tool_call_id,
            "builder",
            CoreDelegatedRunStage::Running,
            AgentProgressStatus::Running,
            2,
        ))
        .expect("droppable progress should send");
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot_persisted = DelegatedRunStore::new(
                Database::new(&state.db_path).expect("database should open"),
            )
            .get_run(delegated_run_id)
            .expect("delegated run should load")
            .and_then(|run| run.snapshot)
            .is_some();
            if snapshot_persisted {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active progress snapshot should persist");

    DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"))
        .finalize_run(
            delegated_run_id,
            CoreDelegatedRunStage::Degraded,
            &serde_json::json!({"outcome": "partial"}),
            Some("Retained partial evidence"),
            true,
        )
        .expect("terminal state should persist");
    let mut terminal = test_delegated_progress(
        &session_id,
        delegated_run_id,
        tool_call_id,
        "builder",
        CoreDelegatedRunStage::Failed,
        AgentProgressStatus::Failed,
        2,
    );
    terminal.progress.current_action = Some("degraded".to_string());
    progress_tx
        .send(terminal)
        .expect("terminal progress should send");

    let occupied = sse_rx.recv().await.unwrap().unwrap();
    assert!(format!("{occupied:?}").contains("text_delta"));
    let lagged = sse_rx.recv().await.unwrap().unwrap();
    assert!(format!("{lagged:?}").contains("lagged"));
    let terminal = sse_rx.recv().await.unwrap().unwrap();
    let terminal = format!("{terminal:?}");
    assert!(terminal.contains("delegated_progress"));
    assert!(terminal.contains("degraded"));

    drop(progress_tx);
    drop(sse_tx);
    bridge
        .await
        .expect("delegated progress bridge should finish");
}

#[tokio::test]
async fn full_undrained_sse_does_not_hold_foreground_finish_or_session_input_open() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Bounded delegated SSE", None, None, None)
        .expect("session should create");
    create_test_delegated_run(&state, &session_id, "run-no-drain", "tool-no-drain");
    DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"))
        .finalize_run(
            "run-no-drain",
            CoreDelegatedRunStage::Complete,
            &serde_json::json!({"outcome": "success"}),
            Some("complete"),
            true,
        )
        .expect("terminal state should persist");

    let (input_tx, _input_rx) = mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let (sse_tx, mut sse_rx) = mpsc::channel(1);
    let mut loop_skipped = 0;
    assert!(
        forward_loop_event(
            &sse_tx,
            &session_id,
            LoopEvent::TextDelta {
                delta: "occupy without draining".to_string(),
            },
            &mut loop_skipped,
        )
        .await
    );
    let sse_open = Arc::new(Mutex::new(true));
    let progress_bridge = tokio::spawn(run_delegated_progress_bridge(
        progress_rx,
        sse_tx.downgrade(),
        Arc::clone(&sse_open),
        session_id.clone(),
        Arc::clone(&state.delegated_state),
        Arc::clone(&state.db_path),
        None,
    ));
    let foreground_bridge = tokio::spawn(run_orchestrator_event_bridge(
        event_rx,
        sse_tx,
        session_id.clone(),
        Arc::clone(&state.session_inputs),
        None,
        None,
        None,
        Arc::clone(&state.db_path),
        None,
        Arc::clone(&sse_open),
    ));

    progress_tx
        .send(test_delegated_progress(
            &session_id,
            "run-no-drain",
            "tool-no-drain",
            "builder",
            CoreDelegatedRunStage::Complete,
            AgentProgressStatus::Complete,
            1,
        ))
        .expect("terminal delegated progress should send");
    timeout(Duration::from_secs(1), async {
        loop {
            if sse_open.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal progress should enter bounded delivery");

    event_tx
        .send(LoopEvent::Finished {
            session_id: session_id.clone(),
            stop_reason: LoopStopReason::Completed,
        })
        .expect("finish should send");
    timeout(Duration::from_secs(1), foreground_bridge)
        .await
        .expect("foreground finish must not wait forever for an undrained client")
        .expect("foreground bridge should join");
    assert!(state.session_inputs.read().await.get(&session_id).is_none());

    let occupied = sse_rx.recv().await.unwrap().unwrap();
    assert!(format!("{occupied:?}").contains("text_delta"));
    assert!(matches!(
        timeout(Duration::from_secs(1), sse_rx.recv()).await,
        Ok(None)
    ));
    drop(progress_tx);
    progress_bridge
        .await
        .expect("progress bridge should finish");
}

#[tokio::test]
async fn required_error_then_finish_does_not_hold_undrained_sse_or_session_input_open() {
    let (state, _temp_dir) = create_test_state();
    let session_id = "session-error-no-drain".to_string();
    let (input_tx, _input_rx) = mpsc::unbounded_channel();
    state
        .session_inputs
        .write()
        .await
        .insert(session_id.clone(), input_tx);

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (sse_tx, mut sse_rx) = mpsc::channel(1);
    let mut skipped_events = 0;
    assert!(
        forward_loop_event(
            &sse_tx,
            &session_id,
            LoopEvent::TextDelta {
                delta: "occupy without draining".to_string(),
            },
            &mut skipped_events,
        )
        .await
    );

    let sse_open = Arc::new(Mutex::new(true));
    let bridge = tokio::spawn(run_orchestrator_event_bridge(
        event_rx,
        sse_tx,
        session_id.clone(),
        Arc::clone(&state.session_inputs),
        None,
        None,
        None,
        Arc::clone(&state.db_path),
        None,
        Arc::clone(&sse_open),
    ));

    event_tx
        .send(LoopEvent::Error {
            error: "provider failed".to_string(),
        })
        .expect("error should send");
    event_tx
        .send(LoopEvent::Finished {
            session_id: session_id.clone(),
            stop_reason: LoopStopReason::ProviderError,
        })
        .expect("finish should send");

    timeout(Duration::from_secs(1), bridge)
        .await
        .expect("required events must not wait forever for an undrained client")
        .expect("foreground bridge should join");
    assert!(state.session_inputs.read().await.get(&session_id).is_none());
    assert!(!*sse_open.lock().await);

    let occupied = sse_rx.recv().await.unwrap().unwrap();
    assert!(format!("{occupied:?}").contains("text_delta"));
    assert!(matches!(
        timeout(Duration::from_secs(1), sse_rx.recv()).await,
        Ok(None)
    ));
}

#[tokio::test]
async fn delegated_progress_channel_closure_cleans_only_its_live_snapshots() {
    let (state, _temp_dir) = create_test_state();
    let session_id = "session-progress-cleanup".to_string();
    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let (sse_tx, _sse_rx) = mpsc::channel(2);
    state.delegated_state.write().await.insert(
        session_id.clone(),
        vec![crate::types::DelegatedToolStateResponse {
            delegated_run_id: "unrelated-run".to_string(),
            tool_call_id: "unrelated-tool".to_string(),
            kind: crate::types::DelegatedToolKind::Explore,
            stage: crate::types::DelegatedRunStage::Running,
            parent_session_id: Some(session_id.clone()),
            agents: Vec::new(),
        }],
    );
    let bridge = tokio::spawn(run_delegated_progress_bridge(
        progress_rx,
        sse_tx.downgrade(),
        Arc::new(Mutex::new(true)),
        session_id.clone(),
        Arc::clone(&state.delegated_state),
        Arc::clone(&state.db_path),
        None,
    ));

    progress_tx
        .send(test_delegated_progress(
            &session_id,
            "run-to-clean",
            "tool-to-clean",
            "builder",
            CoreDelegatedRunStage::Running,
            AgentProgressStatus::Running,
            1,
        ))
        .expect("progress should send");
    timeout(Duration::from_secs(1), async {
        loop {
            if state
                .delegated_state
                .read()
                .await
                .get(&session_id)
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("live snapshot should be inserted");

    drop(progress_tx);
    bridge
        .await
        .expect("delegated progress bridge should finish");
    let retained = state.delegated_state.read().await;
    let retained = retained
        .get(&session_id)
        .expect("unrelated snapshot should remain");
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].delegated_run_id, "unrelated-run");
}

#[tokio::test]
async fn delegated_progress_rejects_foreign_session_and_durable_tool_ownership() {
    let (state, _temp_dir) = create_test_state();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user("Owned progress", None, None, None)
        .expect("session should create");
    create_test_delegated_run(&state, &session_id, "owned-run", "owned-tool");
    DelegatedRunStore::new(Database::new(&state.db_path).expect("database should open"))
        .finalize_run(
            "owned-run",
            CoreDelegatedRunStage::Complete,
            &serde_json::json!({"outcome": "success"}),
            Some("complete"),
            true,
        )
        .expect("terminal state should persist");

    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let (sse_tx, mut sse_rx) = mpsc::channel(4);
    let bridge = tokio::spawn(run_delegated_progress_bridge(
        progress_rx,
        sse_tx.downgrade(),
        Arc::new(Mutex::new(true)),
        session_id.clone(),
        Arc::clone(&state.delegated_state),
        Arc::clone(&state.db_path),
        None,
    ));

    let mut foreign_session = test_delegated_progress(
        &session_id,
        "foreign-run",
        "foreign-tool",
        "foreign",
        CoreDelegatedRunStage::Running,
        AgentProgressStatus::Running,
        1,
    );
    foreign_session.parent_session_id = "different-session".to_string();
    progress_tx
        .send(foreign_session)
        .expect("foreign-session event should enter bridge");
    progress_tx
        .send(test_delegated_progress(
            &session_id,
            "owned-run",
            "wrong-tool",
            "foreign-tool",
            CoreDelegatedRunStage::Complete,
            AgentProgressStatus::Complete,
            1,
        ))
        .expect("wrong-tool event should enter bridge");
    drop(progress_tx);
    bridge.await.expect("progress bridge should finish");

    assert!(state.delegated_state.read().await.is_empty());
    assert!(matches!(sse_rx.try_recv(), Err(TryRecvError::Empty)));
    drop(sse_tx);
}

#[tokio::test]
async fn chat_does_not_persist_model_override_when_setup_fails() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let session_id = session_manager
        .create_session_for_user_with_config(
            "Existing Session",
            Some("minimax/m2"),
            None,
            None,
            mitsuro_core::storage::WorkspaceMode::Neutral,
            Some("alice"),
            None,
            mitsuro_core::storage::SessionType::Code,
        )
        .expect("session should be created");

    let control_headers = code_control_headers(&state, &session_id).await;
    let result = chat(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        control_headers,
        Json(ChatRequest {
            session_id: Some(session_id.clone()),
            message: "hello".to_string(),
            content: Vec::new(),
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: None,
            model: Some("openai/gpt-5".to_string()),
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            fast_mode: false,
            mode: None,
            permission_mode: Some(mitsuro_core::tools::registry::PermissionMode::default()),
            research_enabled: None,
            allowed_tools: None,
        }),
    )
    .await;

    match result {
        Err(AppError::BadRequest(message)) => {
            assert_eq!(message, "No AI credentials configured");
        }
        Ok(_) => panic!("chat request should fail without configured AI credentials"),
        Err(_) => panic!("chat request should fail with bad request"),
    }

    let reloaded = session_manager
        .get_session(&session_id)
        .expect("session lookup should succeed")
        .expect("session should exist");
    assert_eq!(reloaded.model.as_deref(), Some("minimax/m2"));
}

#[tokio::test]
async fn chat_rejects_missing_model_before_creating_session() {
    let (state, temp_dir) = create_test_state();
    create_test_user(&state, "alice");
    let user_root = temp_dir.join("alice-home");
    let parent_dir = user_root.join("projects");
    std::fs::create_dir_all(&parent_dir).expect("parent dir should exist");
    let fresh_project_dir = parent_dir.join("fresh-chat-repo");

    let result = chat(
        State(state.clone()),
        Some(current_user("alice", &user_root)),
        HeaderMap::new(),
        Json(ChatRequest {
            session_id: None,
            message: "scan this workspace".to_string(),
            content: Vec::new(),
            project_dir: Some(fresh_project_dir.to_string_lossy().to_string()),
            working_dir: None,
            workspace_mode: Some(WorkspaceMode::Selected),
            target_branch: None,
            session_type: Some(SessionType::Code),
            model: None,
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            fast_mode: false,
            mode: None,
            permission_mode: Some(mitsuro_core::tools::registry::PermissionMode::default()),
            research_enabled: None,
            allowed_tools: None,
        }),
    )
    .await;

    match result {
        Err(AppError::BadRequest(message)) => {
            assert_eq!(message, "No model selected. Choose a model and try again.");
        }
        Ok(_) => panic!("chat request should fail without a selected model"),
        Err(_) => panic!("chat request should fail with bad request"),
    }

    let expected = fresh_project_dir.to_string_lossy().to_string();
    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let sessions = session_manager
        .list_sessions_for_user(Some(expected.as_str()), Some("alice"))
        .expect("session listing should succeed");

    assert!(sessions.is_empty());
}

#[tokio::test]
async fn chat_rejects_unsupported_image_before_creating_session() {
    let (state, _temp_dir) = create_test_state();
    create_test_user(&state, "alice");

    let result = chat(
        State(state.clone()),
        Some(current_user("alice", state.working_dir.as_ref())),
        HeaderMap::new(),
        Json(ChatRequest {
            session_id: None,
            message: "what is this?".to_string(),
            content: vec![ContentBlock::Image {
                source: crate::types::ImageSource::Base64 {
                    media_type: "image/heic".to_string(),
                    data: "ZmFrZQ==".to_string(),
                },
            }],
            project_dir: None,
            working_dir: None,
            workspace_mode: None,
            target_branch: None,
            session_type: Some(SessionType::Chat),
            model: None,
            model_key: None,
            thinking_enabled: crate::types::ThinkingLevel::Off,
            fast_mode: false,
            mode: None,
            permission_mode: Some(mitsuro_core::tools::registry::PermissionMode::default()),
            research_enabled: None,
            allowed_tools: None,
        }),
    )
    .await;

    assert!(matches!(
        result,
        Err(AppError::BadRequest(message))
            if message.contains("image/heic") && message.contains("Supported formats")
    ));

    let session_manager =
        SessionManager::new(Database::new(&state.db_path).expect("database should open"));
    let sessions = session_manager
        .list_sessions_for_user(None, Some("alice"))
        .expect("session listing should succeed");

    assert!(sessions.is_empty());
}

#[tokio::test]
async fn sse_critical_tool_approval_survives_full_buffer_with_lag_signal() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut skipped_events = 0usize;

    assert!(
        forward_loop_event(
            &tx,
            "session-1",
            LoopEvent::TextDelta {
                delta: "first".to_string(),
            },
            &mut skipped_events,
        )
        .await
    );
    assert!(
        forward_loop_event(
            &tx,
            "session-1",
            LoopEvent::TextDelta {
                delta: "second".to_string(),
            },
            &mut skipped_events,
        )
        .await
    );
    assert_eq!(skipped_events, 1);

    let approval_tx = tx.clone();
    let approval_handle = tokio::spawn(async move {
        forward_loop_event(
            &approval_tx,
            "session-1",
            LoopEvent::ToolApprovalRequired {
                id: "tool-1".to_string(),
                name: "edit".to_string(),
                arguments: serde_json::json!({"path": "src/lib.rs"}),
            },
            &mut skipped_events,
        )
        .await
    });

    let first = rx.recv().await.expect("first event should arrive");
    let first = first.expect("sse event should be ok");
    assert!(format!("{first:?}").contains("text_delta"));

    let lagged = rx.recv().await.expect("lag signal should arrive");
    let lagged = lagged.expect("lag signal should be ok");
    let lagged = format!("{lagged:?}");
    assert!(lagged.contains("lagged"));
    assert!(lagged.contains("skipped"));

    let approval = rx.recv().await.expect("critical approval should arrive");
    let approval = approval.expect("approval event should be ok");
    let approval = format!("{approval:?}");
    assert!(approval.contains("tool_approval_required"));
    assert!(approval.contains("tool-1"));

    assert!(approval_handle.await.expect("approval task should join"));
}

#[tokio::test]
async fn sse_usage_survives_full_buffer_before_terminal_delivery() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut skipped_events = 0usize;

    assert!(
        forward_loop_event(
            &tx,
            "session-usage",
            LoopEvent::TextDelta {
                delta: "first".to_string(),
            },
            &mut skipped_events,
        )
        .await
    );
    assert!(
        forward_loop_event(
            &tx,
            "session-usage",
            LoopEvent::TextDelta {
                delta: "dropped".to_string(),
            },
            &mut skipped_events,
        )
        .await
    );

    let usage_tx = tx.clone();
    let usage_handle = tokio::spawn(async move {
        forward_loop_event(
            &usage_tx,
            "session-usage",
            LoopEvent::Usage {
                prompt_tokens: 100,
                input_tokens: 1_000,
                completion_tokens: 50,
                reasoning_tokens: 40,
                cache_creation_input_tokens: 200,
                cache_read_input_tokens: 700,
                total_tokens: 1_050,
            },
            &mut skipped_events,
        )
        .await
    });

    assert!(format!("{:?}", rx.recv().await.unwrap().unwrap()).contains("text_delta"));
    assert!(format!("{:?}", rx.recv().await.unwrap().unwrap()).contains("lagged"));
    let usage = format!("{:?}", rx.recv().await.unwrap().unwrap());
    assert!(usage.contains("usage"));
    assert!(usage.contains("input_tokens"));
    assert!(usage.contains("reasoning_tokens"));
    assert!(usage_handle.await.expect("usage task should join"));
}

#[tokio::test]
async fn forward_loop_event_surfaces_lag_before_terminal_event() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut skipped_events = 0usize;

    assert!(
        forward_loop_event(
            &tx,
            "session-1",
            LoopEvent::TextDelta {
                delta: "first".to_string(),
            },
            &mut skipped_events,
        )
        .await
    );
    assert_eq!(skipped_events, 0);

    assert!(
        forward_loop_event(
            &tx,
            "session-1",
            LoopEvent::TextDelta {
                delta: "second".to_string(),
            },
            &mut skipped_events,
        )
        .await
    );
    assert_eq!(skipped_events, 1);

    let first = rx.recv().await.expect("first event should arrive");
    let first = first.expect("sse event should be ok");
    let first = format!("{first:?}");
    assert!(first.contains("text_delta"));

    let mut finish_skipped_events = skipped_events;
    let tx_clone = tx.clone();
    let finish_handle = tokio::spawn(async move {
        let delivered = forward_loop_event(
            &tx_clone,
            "session-1",
            LoopEvent::Finished {
                session_id: "session-1".to_string(),
                stop_reason: LoopStopReason::Completed,
            },
            &mut finish_skipped_events,
        )
        .await;
        (delivered, finish_skipped_events)
    });

    let lagged = rx.recv().await.expect("lagged event should arrive");
    let lagged = lagged.expect("lagged event should be ok");
    let lagged = format!("{lagged:?}");
    assert!(lagged.contains("lagged"));
    assert!(lagged.contains("skipped"));

    let finish = rx.recv().await.expect("finish event should arrive");
    let finish = finish.expect("finish event should be ok");
    let finish = format!("{finish:?}");
    assert!(finish.contains("finish"));

    let (delivered, skipped_events) = finish_handle.await.expect("finish task should join");
    assert!(delivered);
    assert_eq!(skipped_events, 0);

    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}
