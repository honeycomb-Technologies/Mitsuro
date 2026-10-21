use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::OwnedMutexGuard;

use mitsuro_core::agent::autonomy::coordinator_prompt::system_prompt_for_session;
use mitsuro_core::ai::client::{AiClient, CallOptions};
use mitsuro_core::ai::models::{ModelKey, ModelLookupError, ModelMetadata, ProjectModelRef};
use mitsuro_core::ai::types::{ModelMessage, WebFetchConfig, WebSearchConfig};
use mitsuro_core::plan::{has_active_workflow_or_plan, PlanManager};
use mitsuro_core::storage::{
    Database, HiveRuntimeStateStore, ProjectSettings, SessionInfo, SessionType, WorkMode,
    WorkspaceMode,
};
use mitsuro_core::tools::registry::PermissionMode;
use mitsuro_core::SessionManager;

use super::super::session_access::{current_user_id, load_owned_session, request_workspace_scope};
use super::tools::{
    apply_thinking_config, chat_system_prompt, filter_code_tools_for_mode,
    filter_tools_for_session_type,
};
use crate::ai_bootstrap::{
    persist_current_model_key_selection, persist_current_model_selection, resolve_preferred_model,
    resolve_preferred_model_key,
};
use crate::auth::CurrentUser;
use crate::error::AppError;
use crate::types::{ChatRequest, ThinkingLevel};
use crate::utils::messages::parse_stored_model_messages;
use crate::utils::text::trimmed_nonempty;
use crate::utils::workspace::{
    normalize_resolved_requested_workspace, resolve_optional_workspace_path,
    resolve_session_working_dir, WorkspaceNormalizationPolicy,
};
use crate::AppState;

pub(super) struct ChatSessionContext {
    pub(super) ai_client: Arc<AiClient>,
    pub(super) options: CallOptions,
    pub(super) conversation: Vec<ModelMessage>,
    pub(super) session_id: String,
    pub(super) session_manager: SessionManager,
    pub(super) working_dir: PathBuf,
    pub(super) project_dir: Option<PathBuf>,
    pub(super) work_mode: WorkMode,
    pub(super) session_type: SessionType,
    pub(super) permission_mode: PermissionMode,
    pub(super) execution_tool_allowlist: Option<std::collections::HashSet<String>>,
    pub(super) hive_crew_slug: Option<String>,
    pub(super) user_id: Option<String>,
    pub(super) guard: OwnedMutexGuard<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequestedModel<'a> {
    Unspecified,
    Clear,
    Set(&'a str),
    Exact(&'a ModelKey),
}

impl<'a> RequestedModel<'a> {
    pub(super) fn from_request(value: Option<&'a str>) -> Self {
        match value {
            Some(raw) => trimmed_nonempty(Some(raw))
                .map(Self::Set)
                .unwrap_or(Self::Clear),
            None => Self::Unspecified,
        }
    }

    pub(super) fn from_request_parts(
        value: Option<&'a str>,
        key: Option<&'a ModelKey>,
    ) -> Result<Self, AppError> {
        let legacy = trimmed_nonempty(value);
        if let Some(key) = key {
            if value.is_some() && legacy.is_none() {
                return Err(AppError::BadRequest(
                    "model_key cannot be combined with an empty model override".to_string(),
                ));
            }
            if legacy.is_some_and(|model| model != key.model_id.as_str()) {
                return Err(AppError::BadRequest(format!(
                    "Model '{}' does not match provider-aware key '{}'",
                    legacy.unwrap_or_default(),
                    key.model_id
                )));
            }
            return Ok(Self::Exact(key));
        }
        Ok(Self::from_request(value))
    }

    pub(super) fn effective(self, session_model: Option<&'a str>) -> Option<&'a str> {
        match self {
            Self::Unspecified => trimmed_nonempty(session_model),
            Self::Clear => None,
            Self::Set(model) => Some(model),
            Self::Exact(key) => Some(key.model_id.as_str()),
        }
    }

    pub(super) fn persisted(self) -> Option<Option<&'a str>> {
        match self {
            Self::Unspecified => None,
            Self::Clear => Some(None),
            Self::Set(model) => Some(Some(model)),
            Self::Exact(key) => Some(Some(key.model_id.as_str())),
        }
    }
}

pub(super) struct PreparedChatRouteSession {
    pub(super) session_id: String,
    pub(super) session_type: SessionType,
    pub(super) is_first_message: bool,
    pub(super) pending_model_update: Option<Option<String>>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ChatContinuationContract {
    pub(super) session_id: String,
    pub(super) is_first_message: bool,
    pub(super) working_dir: PathBuf,
    pub(super) project_dir: Option<PathBuf>,
    pub(super) workspace_mode: WorkspaceMode,
    pub(super) session_type: SessionType,
    pub(super) work_mode: WorkMode,
    pub(super) model: Option<String>,
    pub(super) model_key: Option<ModelKey>,
    pub(super) model_catalog_revision: Option<String>,
    pub(super) target_branch: Option<String>,
    pub(super) permission_mode: PermissionMode,
    pub(super) fast_mode: bool,
    pub(super) user_id: Option<String>,
}

pub(super) async fn prepare_chat_route_session(
    state: &AppState,
    user: Option<&CurrentUser>,
    req: &ChatRequest,
    requested_model: RequestedModel<'_>,
    requested_session_type: SessionType,
    requires_vision: bool,
) -> Result<PreparedChatRouteSession, AppError> {
    let user_id = current_user_id(user).map(ToOwned::to_owned);
    let workspace_scope = request_workspace_scope(state, user);

    match req.session_id.as_deref() {
        Some(id) => {
            let db = Database::new(&state.db_path)?;
            let sm = SessionManager::new(db);
            let session = load_owned_session(&sm, id, user)?;
            if session.session_type == SessionType::Hive && req.target_branch.is_some() {
                return Err(AppError::Conflict(
                    "Hive target branches are background-service-owned and cannot be changed through /chat"
                        .into(),
                ));
            }
            if let Some(target_branch) = req.target_branch.as_deref() {
                sm.update_session_target_branch(id, trimmed_nonempty(Some(target_branch)))?;
            }
            let messages = sm.load_session_messages(id)?;
            Ok(PreparedChatRouteSession {
                session_id: id.to_string(),
                session_type: session.session_type,
                is_first_message: messages.is_empty(),
                pending_model_update: (session.session_type != SessionType::Hive)
                    .then(|| {
                        requested_model
                            .persisted()
                            .map(|model| model.map(ToOwned::to_owned))
                    })
                    .flatten(),
            })
        }
        None => {
            let db = Database::new(&state.db_path)?;
            let sm = SessionManager::new(db);
            let title = SessionManager::generate_title_from_content(&req.message);
            let default_mode_without_paths = if requested_session_type == SessionType::Chat {
                WorkspaceMode::Neutral
            } else {
                WorkspaceMode::Selected
            };
            let default_workspace = workspace_scope.base_dir.to_string_lossy().to_string();
            let workspace = normalize_resolved_requested_workspace(
                req.working_dir.as_deref(),
                req.project_dir.as_deref(),
                req.workspace_mode,
                WorkspaceNormalizationPolicy {
                    default_mode_without_paths,
                    selected_fallback_dir: Some(default_workspace.as_str()),
                },
                &workspace_scope.base_dir,
                &workspace_scope.allowed_root,
            )?;
            let preferred_model =
                resolve_preferred_model(state.db_path.as_ref().as_path(), user_id.as_deref());
            let preferred_key =
                resolve_preferred_model_key(state.db_path.as_ref().as_path(), user_id.as_deref());
            let preferred_key = if let Some(key) = preferred_key {
                if state.model_registry.get_model_by_key(&key).await.is_some() {
                    Some(key)
                } else {
                    tracing::warn!(?key, "Ignoring unavailable preferred model key");
                    None
                }
            } else {
                None
            };
            let project_settings = workspace
                .project_dir
                .as_deref()
                .or(workspace.working_dir.as_deref())
                .map(Path::new)
                .map(ProjectSettings::load)
                .unwrap_or_default();
            let initial_selection = select_model_selection_for_chat_request(
                state,
                requested_model,
                None,
                None,
                project_settings.model.as_ref(),
                preferred_key.as_ref(),
                preferred_model.as_deref(),
                requires_vision,
            )
            .await?;
            let initial_model = initial_selection
                .as_ref()
                .map(|selection| selection.model_id.as_str());
            if initial_model.is_none() && preferred_key.is_none() && preferred_model.is_none() {
                return Err(AppError::BadRequest(
                    "No model selected. Choose a model and try again.".to_string(),
                ));
            }
            let initial_target_branch = trimmed_nonempty(req.target_branch.as_deref());
            let session_id = sm.create_session_for_user_with_config_and_permission(
                &title,
                initial_model,
                workspace.working_dir.as_deref(),
                workspace.project_dir.as_deref(),
                workspace.workspace_mode,
                user_id.as_deref(),
                initial_target_branch,
                requested_session_type,
                req.permission_mode.unwrap_or_default(),
            )?;
            if let Some(selection) = initial_selection.as_ref() {
                if let Some(key) = selection.key() {
                    sm.update_session_model_selection(
                        &session_id,
                        Some(&key),
                        selection.catalog_revision(),
                    )?;
                }
            }
            let should_persist_current_model = match requested_model {
                RequestedModel::Set(_) | RequestedModel::Exact(_) => true,
                RequestedModel::Unspecified => {
                    preferred_key.as_ref().is_some_and(|key| {
                        initial_selection.as_ref().and_then(|s| s.key()) == Some(key.clone())
                    }) || preferred_model.as_deref() == initial_model
                }
                RequestedModel::Clear => false,
            };
            if should_persist_current_model {
                if let Some(selection) = initial_selection.as_ref() {
                    if let Some(key) = selection.key() {
                        persist_current_model_key_selection(
                            &state.model_registry,
                            state.db_path.as_ref().as_path(),
                            user_id.as_deref(),
                            &key,
                        )
                        .await?;
                    } else {
                        persist_current_model_selection(
                            &state.model_registry,
                            state.db_path.as_ref().as_path(),
                            user_id.as_deref(),
                            &selection.model_id,
                        )
                        .await?;
                    }
                }
            }

            Ok(PreparedChatRouteSession {
                session_id,
                session_type: requested_session_type,
                is_first_message: true,
                pending_model_update: None,
            })
        }
    }
}

#[cfg(test)]
pub(super) fn build_chat_continuation_contract(
    session: &SessionInfo,
    working_dir: PathBuf,
    project_dir: Option<PathBuf>,
    work_mode: WorkMode,
    fast_mode: bool,
    is_first_message: bool,
) -> ChatContinuationContract {
    ChatContinuationContract {
        session_id: session.id.clone(),
        is_first_message,
        working_dir,
        project_dir,
        workspace_mode: session.workspace_mode,
        session_type: session.session_type,
        work_mode,
        model: session.model.clone(),
        model_key: session.model_key.clone(),
        model_catalog_revision: session.model_catalog_revision.clone(),
        target_branch: session.target_branch.clone(),
        permission_mode: session.permission_mode,
        fast_mode,
        user_id: session.user_id.clone(),
    }
}

#[cfg(test)]
pub(super) async fn load_chat_continuation_contract(
    state: &AppState,
    user: Option<&CurrentUser>,
    prepared: &PreparedChatRouteSession,
    fast_mode: bool,
) -> Result<ChatContinuationContract, AppError> {
    let workspace_scope = request_workspace_scope(state, user);
    let db = Database::new(&state.db_path)?;
    let session_manager = SessionManager::new(db);
    let session = load_owned_session(&session_manager, &prepared.session_id, user)?;
    let working_dir = resolve_session_working_dir(
        session.working_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?;
    let project_dir = resolve_optional_workspace_path(
        session.project_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?
    .map(PathBuf::from);
    let work_mode = effective_session_work_mode(state, &session);

    Ok(build_chat_continuation_contract(
        &session,
        working_dir,
        project_dir,
        work_mode,
        fast_mode,
        prepared.is_first_message,
    ))
}

#[cfg(test)]
pub(super) async fn prepare_chat_contract_for_test(
    state: &AppState,
    user: Option<CurrentUser>,
    req: ChatRequest,
) -> Result<ChatContinuationContract, AppError> {
    let requested_model =
        RequestedModel::from_request_parts(req.model.as_deref(), req.model_key.as_ref())?;
    let requested_session_type = req.session_type.unwrap_or(SessionType::Code);
    let requires_vision = false;
    let prepared = prepare_chat_route_session(
        state,
        user.as_ref(),
        &req,
        requested_model,
        requested_session_type,
        requires_vision,
    )
    .await?;

    load_chat_continuation_contract(state, user.as_ref(), &prepared, req.fast_mode).await
}

fn effective_session_work_mode(state: &AppState, session: &SessionInfo) -> WorkMode {
    PlanManager::new((*state.db_path).clone())
        .ok()
        .and_then(|pm| pm.get_lifecycle_state(&session.id, session.work_mode).ok())
        .map(|state| state.effective_work_mode)
        .unwrap_or(session.work_mode)
}

#[derive(Debug, Clone)]
struct ChatModelSelection {
    model_id: String,
    metadata: Option<ModelMetadata>,
}

impl ChatModelSelection {
    fn exact(metadata: ModelMetadata) -> Self {
        Self {
            model_id: metadata.id.clone(),
            metadata: Some(metadata),
        }
    }

    fn legacy(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
            metadata: None,
        }
    }

    fn key(&self) -> Option<ModelKey> {
        self.metadata.as_ref().map(ModelMetadata::key)
    }

    fn catalog_revision(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.catalog_revision.as_deref())
    }

    fn supports_vision(&self) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(|metadata| metadata.supports_vision)
    }
}

fn model_lookup_error(error: ModelLookupError) -> AppError {
    match error {
        ModelLookupError::NotFound { model_id } => {
            AppError::BadRequest(format!("Model '{model_id}' is not available"))
        }
        ModelLookupError::Ambiguous {
            model_id,
            candidates,
        } => AppError::BadRequest(format!(
            "Model '{model_id}' is ambiguous; submit one of these provider-aware keys: {candidates:?}"
        )),
    }
}

async fn exact_model_selection(
    state: &AppState,
    key: &ModelKey,
) -> Result<ChatModelSelection, AppError> {
    state
        .model_registry
        .get_model_by_key(key)
        .await
        .map(ChatModelSelection::exact)
        .ok_or_else(|| AppError::BadRequest(format!("Model key {key:?} is not available")))
}

async fn legacy_model_selection(
    state: &AppState,
    model_id: &str,
) -> Result<ChatModelSelection, AppError> {
    match state.model_registry.resolve_legacy_key(model_id).await {
        Ok(key) => exact_model_selection(state, &key).await,
        // Preserve the legacy KRUSTY_PROVIDER + custom-slug bootstrap path.
        // It remains conservative and cannot claim catalog capabilities.
        Err(ModelLookupError::NotFound { .. }) => {
            Ok(ChatModelSelection::legacy(model_id.to_string()))
        }
        Err(error @ ModelLookupError::Ambiguous { .. }) => Err(model_lookup_error(error)),
    }
}

async fn project_model_selection(
    state: &AppState,
    model_ref: &ProjectModelRef,
) -> Result<ChatModelSelection, AppError> {
    state
        .model_registry
        .resolve_project_model_ref(model_ref)
        .await
        .map(ChatModelSelection::exact)
        .map_err(model_lookup_error)
}

async fn optional_model_selection(
    state: &AppState,
    model_key: Option<&ModelKey>,
    model_id: Option<&str>,
) -> Result<Option<ChatModelSelection>, AppError> {
    if let Some(key) = model_key {
        Ok(Some(exact_model_selection(state, key).await?))
    } else if let Some(model_id) = trimmed_nonempty(model_id) {
        Ok(Some(legacy_model_selection(state, model_id).await?))
    } else {
        Ok(None)
    }
}

async fn select_model_selection_for_chat_request(
    state: &AppState,
    requested_model: RequestedModel<'_>,
    session_model_key: Option<&ModelKey>,
    session_model: Option<&str>,
    project_model: Option<&ProjectModelRef>,
    preferred_model_key: Option<&ModelKey>,
    preferred_model: Option<&str>,
    requires_vision: bool,
) -> Result<Option<ChatModelSelection>, AppError> {
    let effective = match requested_model {
        RequestedModel::Exact(key) => Some(exact_model_selection(state, key).await?),
        RequestedModel::Set(model) => Some(legacy_model_selection(state, model).await?),
        RequestedModel::Clear => {
            if let Some(model_ref) = project_model {
                Some(project_model_selection(state, model_ref).await?)
            } else {
                optional_model_selection(state, preferred_model_key, preferred_model).await?
            }
        }
        RequestedModel::Unspecified => {
            if let Some(selection) =
                optional_model_selection(state, session_model_key, session_model).await?
            {
                Some(selection)
            } else if let Some(model_ref) = project_model {
                Some(project_model_selection(state, model_ref).await?)
            } else {
                optional_model_selection(state, preferred_model_key, preferred_model).await?
            }
        }
    };

    if !requires_vision {
        return Ok(effective);
    }

    if matches!(
        requested_model,
        RequestedModel::Exact(_) | RequestedModel::Set(_)
    ) && !effective
        .as_ref()
        .is_some_and(ChatModelSelection::supports_vision)
    {
        let model = requested_model.effective(None).unwrap_or_default();
        return Err(AppError::BadRequest(format!(
            "Model '{}' does not support image input. Select a vision-capable model and try again.",
            model
        )));
    }

    if effective
        .as_ref()
        .is_some_and(ChatModelSelection::supports_vision)
    {
        return Ok(effective);
    }

    // Never silently substitute a different model to make the request
    // succeed. Name the resolved model so the user can change it.
    let detail = match effective.as_ref() {
        Some(selection) => format!(
            "The resolved model '{}' does not support image input. Select a vision-capable model before sending images.",
            selection.model_id
        ),
        None => {
            "No model is configured for this session. Select a vision-capable model before sending images.".to_string()
        }
    };
    Err(AppError::BadRequest(detail))
}

#[cfg(test)]
pub(super) async fn select_model_for_chat_request(
    state: &AppState,
    requested_model: RequestedModel<'_>,
    session_model: Option<&str>,
    requires_vision: bool,
) -> Result<Option<String>, AppError> {
    Ok(select_model_selection_for_chat_request(
        state,
        requested_model,
        None,
        session_model,
        None,
        None,
        None,
        requires_vision,
    )
    .await?
    .map(|selection| selection.model_id))
}

pub(super) async fn setup_chat_session(
    state: &AppState,
    user: Option<&CurrentUser>,
    session_id: &str,
    requested_model: RequestedModel<'_>,
    thinking_level: ThinkingLevel,
    fast_mode: bool,
    requires_vision: bool,
) -> Result<ChatSessionContext, AppError> {
    setup_chat_session_with_guard(
        state,
        user,
        session_id,
        requested_model,
        thinking_level,
        fast_mode,
        requires_vision,
        None,
    )
    .await
}

pub(super) async fn setup_chat_session_with_guard(
    state: &AppState,
    user: Option<&CurrentUser>,
    session_id: &str,
    requested_model: RequestedModel<'_>,
    thinking_level: ThinkingLevel,
    fast_mode: bool,
    requires_vision: bool,
    preacquired_guard: Option<OwnedMutexGuard<()>>,
) -> Result<ChatSessionContext, AppError> {
    let user_id = current_user_id(user).map(ToOwned::to_owned);
    let workspace_scope = request_workspace_scope(state, user);

    let db = Database::new(&state.db_path)?;
    let session_manager = SessionManager::new(db);
    let session = load_owned_session(&session_manager, session_id, user)?;

    let working_dir = resolve_session_working_dir(
        session.working_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?;
    let project_dir = resolve_optional_workspace_path(
        session.project_dir.as_deref(),
        &workspace_scope.base_dir,
        &workspace_scope.allowed_root,
    )?
    .map(PathBuf::from);
    let project_settings = ProjectSettings::load(project_dir.as_deref().unwrap_or(&working_dir));

    let session_model = trimmed_nonempty(session.model.as_deref());
    let preferred_model =
        resolve_preferred_model(state.db_path.as_ref().as_path(), session.user_id.as_deref());
    let preferred_model_key =
        resolve_preferred_model_key(state.db_path.as_ref().as_path(), session.user_id.as_deref());
    let preferred_model_key = if let Some(key) = preferred_model_key {
        if state.model_registry.get_model_by_key(&key).await.is_some() {
            Some(key)
        } else {
            tracing::warn!(?key, "Ignoring unavailable preferred model key");
            None
        }
    } else {
        None
    };
    let effective_selection = select_model_selection_for_chat_request(
        state,
        requested_model,
        session.model_key.as_ref(),
        session_model,
        project_settings.model.as_ref(),
        preferred_model_key.as_ref(),
        preferred_model.as_deref(),
        requires_vision,
    )
    .await?;
    if effective_selection.is_none() {
        return Err(AppError::BadRequest(
            "No model selected. Choose a model and try again.".to_string(),
        ));
    }

    let ai_client = if let Some(key) = effective_selection
        .as_ref()
        .and_then(ChatModelSelection::key)
    {
        state
            .resolve_ai_client_for_key_for_user(&key, session.user_id.as_deref())
            .await
    } else {
        state
            .resolve_ai_client_for_user(
                effective_selection
                    .as_ref()
                    .map(|selection| selection.model_id.as_str()),
                session.user_id.as_deref(),
            )
            .await
    }
    .ok_or_else(|| AppError::BadRequest("No AI credentials configured".to_string()))?;
    let resolved_model = ai_client.config().model.clone();
    let resolved_key = ai_client.resolved_model().key.clone();

    let session_matches_resolved = session.model_key.as_ref().map_or_else(
        || session_model == Some(resolved_model.as_str()),
        |key| key == &resolved_key,
    );
    let should_persist_model = (requires_vision && !session_matches_resolved)
        || (matches!(requested_model, RequestedModel::Unspecified)
            && session_model.is_none()
            && session.model_key.is_none());
    if should_persist_model {
        if let Some(metadata) = state.model_registry.get_model_by_key(&resolved_key).await {
            session_manager.update_session_model_selection(
                session_id,
                Some(&resolved_key),
                metadata.catalog_revision.as_deref(),
            )?;
        } else {
            session_manager.update_session_model(session_id, Some(resolved_model.as_str()))?;
        }
    }

    let effective_work_mode = effective_session_work_mode(state, &session);
    let has_active_plan = has_active_workflow_or_plan(state.db_path.as_path(), session_id);
    let guard = match preacquired_guard {
        Some(guard) => guard,
        None => state
            .try_lock_session(session_id)
            .await
            .ok_or_else(|| AppError::Conflict(format!("Session {} is busy", session_id)))?,
    };

    let recovered_steering = session_manager.promote_orphaned_pending_steering(session_id)?;
    if recovered_steering > 0 {
        tracing::info!(
            session_id,
            recovered_steering,
            "Recovered durable steering left by an interrupted active run"
        );
    }

    let raw_messages = session_manager.load_session_messages(session_id)?;
    let conversation = parse_stored_model_messages(session_id, raw_messages, "chat conversation");

    tracing::info!(
        session_type = ?session.session_type,
        session_id = %session_id,
        "Filtering tools for session type"
    );
    let all_tools = state.tool_registry.get_ai_tools_all().await;
    let disabled_tool_names = project_settings
        .disabled_tools
        .as_deref()
        .unwrap_or_default();
    let ai_tools = if session.session_type == SessionType::Code {
        filter_code_tools_for_mode(
            all_tools,
            session.permission_mode,
            effective_work_mode,
            has_active_plan,
            disabled_tool_names,
            ai_client.provider_id(),
            &ai_client.config().model,
        )
    } else {
        filter_tools_for_session_type(all_tools, session.session_type)
            .into_iter()
            .filter(|tool| !disabled_tool_names.iter().any(|name| name == &tool.name))
            .collect()
    };
    let hosted_web_tools = session.session_type != SessionType::Code;
    let hosted_web_search =
        hosted_web_tools && !disabled_tool_names.iter().any(|name| name == "web_search");
    let hosted_web_fetch =
        hosted_web_tools && !disabled_tool_names.iter().any(|name| name == "web_fetch");
    let model_metadata = state.model_registry.get_model_by_key(&resolved_key).await;
    let effective_thinking_level =
        normalize_thinking_level_for_model(thinking_level, model_metadata.as_ref());
    let reasoning_format = model_metadata
        .as_ref()
        .and_then(|model| model.reasoning_format);
    let reasoning_control = model_metadata
        .as_ref()
        .and_then(|model| model.reasoning_control);
    let fast_mode_format = model_metadata.as_ref().and_then(|model| model.fast_mode);
    let mut options = CallOptions {
        tools: if ai_tools.is_empty() {
            None
        } else {
            Some(ai_tools)
        },
        session_id: Some(session_id.to_string()),
        codex_parallel_tool_calls: true,
        web_search: hosted_web_search.then(WebSearchConfig::default),
        web_fetch: hosted_web_fetch.then(WebFetchConfig::default),
        reasoning_format,
        reasoning_control,
        fast_mode: fast_mode && fast_mode_format.is_some(),
        fast_mode_format,
        system_prompt: match session.session_type {
            SessionType::Chat => Some(chat_system_prompt()),
            SessionType::Hive => system_prompt_for_session(SessionType::Hive),
            SessionType::Code => None, // uses default Krusty coding assistant prompt
        },
        ..Default::default()
    };
    if effective_thinking_level.is_enabled() {
        apply_thinking_config(effective_thinking_level, &mut options);
    }

    let hive_runtime = if session.session_type == SessionType::Hive {
        HiveRuntimeStateStore::new(Database::new(&state.db_path)?).get_state(session_id)?
    } else {
        None
    };

    Ok(ChatSessionContext {
        ai_client,
        options,
        conversation,
        session_id: session_id.to_string(),
        session_manager,
        working_dir,
        project_dir,
        work_mode: effective_work_mode,
        session_type: session.session_type,
        permission_mode: session.permission_mode,
        execution_tool_allowlist: None,
        hive_crew_slug: hive_runtime.and_then(|runtime| runtime.crew_slug),
        user_id,
        guard,
    })
}

/// Rebuild a Code request's direct schemas after an HTTP-owned mode change.
/// Core repeats this derivation at run start and after in-loop transitions;
/// doing it here is also required so `allowed_tools` is validated against the
/// requested mode rather than the previously persisted mode.
pub(super) async fn refresh_chat_code_tool_surface(
    state: &AppState,
    ctx: &mut ChatSessionContext,
    work_mode: WorkMode,
    permission_mode: PermissionMode,
) {
    if ctx.session_type != SessionType::Code {
        return;
    }

    let project_settings = ctx
        .project_dir
        .as_deref()
        .or(Some(ctx.working_dir.as_path()))
        .map(ProjectSettings::load)
        .unwrap_or_default();
    let has_active_plan = has_active_workflow_or_plan(state.db_path.as_path(), &ctx.session_id);
    let tools = filter_code_tools_for_mode(
        state.tool_registry.get_ai_tools_all().await,
        permission_mode,
        work_mode,
        has_active_plan,
        project_settings
            .disabled_tools
            .as_deref()
            .unwrap_or_default(),
        ctx.ai_client.provider_id(),
        &ctx.ai_client.config().model,
    );
    ctx.options.tools = (!tools.is_empty()).then_some(tools);
    ctx.options.codex_parallel_tool_calls = ctx
        .options
        .tools
        .as_ref()
        .is_some_and(|tools| tools.len() > 1);
    if !ctx.options.codex_parallel_tool_calls {
        ctx.options.parallel_tool_calls = Some(false);
    }
}

fn normalize_thinking_level_for_model(
    requested: ThinkingLevel,
    metadata: Option<&mitsuro_core::ai::models::ModelMetadata>,
) -> ThinkingLevel {
    let Some(metadata) = metadata else {
        return requested;
    };
    if !metadata.supports_thinking
        || metadata.reasoning_control
            == Some(mitsuro_core::ai::providers::ReasoningControl::OutputOnly)
    {
        return ThinkingLevel::Off;
    }

    let mut levels = metadata
        .supported_reasoning_levels
        .iter()
        .copied()
        .map(ThinkingLevel::from_reasoning_effort)
        .filter(|level| *level != ThinkingLevel::Ultra)
        .collect::<Vec<_>>();
    levels.dedup();
    let fallback = metadata
        .default_reasoning_level
        .map(ThinkingLevel::from_reasoning_effort)
        .filter(|level| !matches!(level, ThinkingLevel::Off | ThinkingLevel::Ultra))
        .unwrap_or(ThinkingLevel::Medium);
    if levels.is_empty() {
        return if metadata.reasoning_is_mandatory {
            fallback
        } else if requested == ThinkingLevel::Ultra {
            ThinkingLevel::Max
        } else {
            requested
        };
    }
    if metadata.reasoning_is_mandatory {
        levels.retain(|level| *level != ThinkingLevel::Off);
        if levels.is_empty() {
            levels.push(fallback);
        }
    } else if !levels.contains(&ThinkingLevel::Off) {
        levels.insert(0, ThinkingLevel::Off);
    }
    let requested = if requested == ThinkingLevel::Ultra && levels.contains(&ThinkingLevel::Max) {
        ThinkingLevel::Max
    } else {
        requested
    };
    if levels.contains(&requested) {
        return requested;
    }
    metadata
        .default_reasoning_level
        .map(ThinkingLevel::from_reasoning_effort)
        .filter(|level| levels.contains(level))
        .unwrap_or(levels[0])
}

#[cfg(test)]
mod reasoning_level_tests {
    use super::normalize_thinking_level_for_model;
    use crate::types::ThinkingLevel;
    use mitsuro_core::ai::models::ModelMetadata;
    use mitsuro_core::ai::providers::{
        ProviderId, ReasoningControl, ReasoningEffort, ReasoningFormat,
    };

    #[test]
    fn mandatory_reasoning_never_normalizes_to_an_empty_or_off_cycle() {
        let model = ModelMetadata::new("future-model", "Future", ProviderId::OpenAI)
            .with_thinking(ReasoningFormat::OpenAI)
            .with_reasoning_levels(
                vec![ReasoningEffort::None, ReasoningEffort::Ultra],
                Some(ReasoningEffort::Ultra),
                true,
            )
            .with_reasoning_control(ReasoningControl::OpenAiEffort);

        assert_eq!(
            normalize_thinking_level_for_model(ThinkingLevel::Off, Some(&model)),
            ThinkingLevel::Medium
        );
    }

    #[test]
    fn legacy_ultra_requests_degrade_to_max_without_advertising_ultra() {
        let model = ModelMetadata::new("gpt-test", "GPT Test", ProviderId::OpenAI)
            .with_thinking(ReasoningFormat::OpenAI)
            .with_reasoning_levels(
                vec![
                    ReasoningEffort::Low,
                    ReasoningEffort::Max,
                    ReasoningEffort::Ultra,
                ],
                Some(ReasoningEffort::Low),
                true,
            )
            .with_reasoning_control(ReasoningControl::OpenAiEffort);

        assert_eq!(
            normalize_thinking_level_for_model(ThinkingLevel::Ultra, Some(&model)),
            ThinkingLevel::Max
        );
    }
}
