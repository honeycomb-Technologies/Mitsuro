use agent_client_protocol::{
    Agent, AuthenticateRequest, AuthenticateResponse, Client as AcpClient, ContentBlock,
    ContentChunk, Error as AcpSchemaError, ExtRequest, ExtResponse, InitializeRequest,
    InitializeResponse, LoadSessionRequest, LoadSessionResponse, ModelId,
    ModelInfo as AcpModelInfo, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, SessionMode, SessionModeState, SessionModelState, SessionNotification,
    SessionUpdate, SetSessionModeRequest, SetSessionModeResponse, SetSessionModelRequest,
    SetSessionModelResponse, TextContent, ToolCall, ToolCallId, ToolCallStatus,
};

use super::{acp_model_id_for_key, negotiate_protocol_version, AvailableModelRecord, MitsuroAgent};
use crate::acp::bridge::NotificationBridge;
use crate::acp::error::AcpError;
use crate::acp::session::{SessionModelSelection, SessionState};
use crate::acp::tools::{
    create_tool_call_complete, create_tool_call_failed, text_to_tool_content, tool_name_to_kind,
};
use crate::ai::types::{Content, Role};

#[async_trait::async_trait(?Send)]
impl Agent for MitsuroAgent {
    async fn initialize(
        &self,
        request: InitializeRequest,
    ) -> agent_client_protocol::Result<InitializeResponse> {
        tracing::info!(
            "ACP initialize: protocol_version={}, client={:?}",
            request.protocol_version,
            request.client_info.as_ref().map(|i| &i.name)
        );

        *self.client_capabilities.write().await = Some(request.client_capabilities);

        let requested_version = request.protocol_version;
        let protocol_version = negotiate_protocol_version(requested_version.clone());
        if protocol_version != requested_version {
            tracing::warn!(
                "ACP client requested unsupported protocol version {}, responding with {}",
                requested_version,
                protocol_version
            );
        }

        let mut response = InitializeResponse::new(protocol_version);
        response.agent_capabilities = self.agent_capabilities();
        response.agent_info = Some(self.agent_info());
        Ok(response)
    }

    async fn authenticate(
        &self,
        request: AuthenticateRequest,
    ) -> agent_client_protocol::Result<AuthenticateResponse> {
        tracing::info!("ACP authenticate: method={}", request.method_id);

        if request.method_id.to_string() != "api_key" {
            return Err(AcpSchemaError::invalid_params());
        }

        *self.api_key.write().await = Some("authenticated".to_string());
        tracing::info!("Authentication successful");
        Ok(AuthenticateResponse::new())
    }

    async fn new_session(
        &self,
        request: NewSessionRequest,
    ) -> agent_client_protocol::Result<NewSessionResponse> {
        let cwd = request.cwd;
        let mcp_servers = request.mcp_servers;

        tracing::info!(
            "ACP new_session: cwd={:?}, mcp_servers={}",
            cwd,
            mcp_servers.len()
        );

        let session = self
            .sessions
            .create_persisted_session(
                Some(cwd),
                if mcp_servers.is_empty() {
                    None
                } else {
                    Some(mcp_servers)
                },
            )
            .await
            .map_err(|error| {
                tracing::error!("Failed to create ACP session: {}", error);
                AcpSchemaError::internal_error()
            })?;
        session.set_mode(Some("code".to_string())).await;

        let detected_models = self.detect_available_models().await;
        let mut response = NewSessionResponse::new(session.id.clone());

        let available_modes = vec![
            SessionMode::new("code", "Code").description("Write and edit code directly"),
            SessionMode::new("plan", "Plan").description("Plan changes before implementing"),
        ];
        response = response.modes(SessionModeState::new("code", available_modes));

        if !detected_models.is_empty() {
            {
                let mut available = self.available_models.write().await;
                *available = detected_models.clone();
            }

            let model_infos = acp_model_infos(&detected_models);

            let default_model = self.current_model.read().await.clone();
            let current_model_id = default_model.as_ref().and_then(|selected| {
                detected_models
                    .iter()
                    .find(|record| record.key() == &selected.key)
                    .map(|record| record.acp_model_id.clone())
            });

            let selected_model_id = if default_model.is_some() {
                if current_model_id.is_none() {
                    tracing::warn!(
                        "Exact ACP default model is unavailable; refusing to rebind the session"
                    );
                }
                current_model_id
            } else {
                detected_models
                    .first()
                    .map(|record| record.acp_model_id.clone())
            };
            if let Some(current_model_id) = selected_model_id {
                self.set_model_for_session(&session, &current_model_id, true)
                    .await
                    .map_err(|error| {
                        tracing::error!("Failed to initialize ACP session model: {}", error);
                        AcpSchemaError::internal_error()
                    })?;
                response = response.models(SessionModelState::new(
                    ModelId::new(current_model_id.clone()),
                    model_infos,
                ));
                tracing::info!(
                    "Session created with {} available models; selected {}",
                    detected_models.len(),
                    current_model_id
                );
            }
        } else {
            let default_model = self.current_model.read().await.clone();
            let default_client = self.processor.read().await.default_ai_client();
            if let (Some(model), Some(client)) = (default_model, default_client) {
                if client.resolved_model().key == model.key {
                    let runtime = client.resolved_model();
                    session
                        .set_model_client(
                            SessionModelSelection {
                                key: runtime.key.clone(),
                                acp_model_id: acp_model_id_for_key(&runtime.key),
                                catalog_revision: runtime.catalog_revision.clone(),
                            },
                            client.clone(),
                        )
                        .await;
                    session.persist_model_selection().await;
                } else {
                    tracing::warn!(
                        "ACP default client does not match the exact persisted model key"
                    );
                }
            } else {
                tracing::warn!("No models detected - configure API keys to enable AI features");
            }
        }

        self.send_available_commands(&session.id).await;
        Ok(response)
    }

    async fn load_session(
        &self,
        request: LoadSessionRequest,
    ) -> agent_client_protocol::Result<LoadSessionResponse> {
        tracing::info!("ACP load_session: id={}", request.session_id);

        let session = if self.sessions.has_session(&request.session_id) {
            tracing::info!("Session {} found in memory", request.session_id);
            self.sessions
                .get_session(&request.session_id)
                .map_err(|_| AcpSchemaError::invalid_params())?
        } else {
            let session_id_str = request.session_id.to_string();
            tracing::info!("Loading session {} from storage", session_id_str);
            self.sessions
                .create_session_from_storage(
                    &session_id_str,
                    Some(request.cwd),
                    if request.mcp_servers.is_empty() {
                        None
                    } else {
                        Some(request.mcp_servers)
                    },
                )
                .await
                .map_err(|error| {
                    tracing::warn!("Failed to restore ACP session: {}", error);
                    AcpSchemaError::invalid_params()
                })?
        };

        let detected_models = self.detect_available_models().await;
        if !detected_models.is_empty() {
            *self.available_models.write().await = detected_models.clone();
        }

        let persisted_model_key = session.persisted_model_key().await;
        let persisted_model = session.persisted_model_id().await;
        let default_model = self.current_model.read().await.clone();
        let default_model_id = default_model.as_ref().and_then(|selected| {
            detected_models
                .iter()
                .find(|record| record.key() == &selected.key)
                .map(|record| record.acp_model_id.clone())
        });
        let selected_model_id = if let Some(key) = persisted_model_key.as_ref() {
            let selected = self.resolve_model_key(key).await;
            if selected.is_none() {
                tracing::warn!(
                    "Exact persisted ACP session model is unavailable; refusing to rebind it"
                );
            }
            selected
        } else if let Some(persisted) = persisted_model.as_deref() {
            let selected = self.resolve_persisted_model_id(persisted).await;
            if selected.is_none() {
                tracing::warn!(
                    "Legacy ACP session model is ambiguous or unavailable; refusing to guess"
                );
            }
            selected
        } else if default_model.is_some() {
            if default_model_id.is_none() {
                tracing::warn!(
                    "Exact ACP default model is unavailable; refusing to rebind the session"
                );
            }
            default_model_id
        } else {
            detected_models
                .first()
                .map(|record| record.acp_model_id.clone())
        };

        if let Some(model_id) = selected_model_id.as_deref() {
            self.set_model_for_session(&session, model_id, false)
                .await
                .map_err(|error| {
                    tracing::error!("Failed to restore ACP session model: {}", error);
                    AcpSchemaError::internal_error()
                })?;
        } else if session.ai_client().await.is_none() {
            let default_model = self.current_model.read().await.clone();
            let default_client = self.processor.read().await.default_ai_client();
            if let (Some(model), Some(client)) = (default_model, default_client) {
                if client.resolved_model().key == model.key {
                    let runtime = client.resolved_model();
                    session
                        .set_model_client(
                            SessionModelSelection {
                                key: runtime.key.clone(),
                                acp_model_id: acp_model_id_for_key(&runtime.key),
                                catalog_revision: runtime.catalog_revision.clone(),
                            },
                            client.clone(),
                        )
                        .await;
                }
            }
        }

        self.replay_session_history(&session)
            .await
            .map_err(|error| {
                tracing::error!("Failed to replay ACP session history: {}", error);
                AcpSchemaError::internal_error()
            })?;
        {
            let notification_tx = self.notification_tx.read().await;
            let tx = notification_tx
                .as_ref()
                .ok_or_else(AcpSchemaError::internal_error)?;
            let bridge = NotificationBridge::new(tx.clone());
            self.processor
                .read()
                .await
                .replay_delegation_state(&session, &bridge)
                .await
                .map_err(|error| {
                    tracing::error!(
                        session_id = %session.id,
                        "Failed to replay ACP delegation state: {error}"
                    );
                    AcpSchemaError::internal_error()
                })?;
        }

        let current_mode = session
            .get_mode()
            .await
            .unwrap_or_else(|| "code".to_string());
        let mut response = LoadSessionResponse::new().modes(SessionModeState::new(
            current_mode,
            available_session_modes(),
        ));
        if let Some(selection) = session.selected_model().await {
            let model_infos = acp_model_infos(&detected_models);
            if !model_infos.is_empty() {
                response = response.models(SessionModelState::new(
                    ModelId::new(selection.acp_model_id),
                    model_infos,
                ));
            }
        }

        tracing::info!(
            "Session {} restored with {} messages",
            session.id,
            session.get_messages().await.len()
        );
        Ok(response)
    }

    async fn prompt(
        &self,
        request: PromptRequest,
    ) -> agent_client_protocol::Result<PromptResponse> {
        tracing::info!(
            "ACP prompt: session={}, content_blocks={}",
            request.session_id,
            request.prompt.len()
        );

        let session = self
            .sessions
            .get_session(&request.session_id)
            .map_err(|_e| AcpSchemaError::invalid_params())?;

        let _prompt_guard = session.try_begin_prompt().map_err(|error| {
            tracing::warn!("Rejected overlapping ACP prompt: {}", error);
            AcpSchemaError::invalid_params()
        })?;

        session.reset_cancellation();

        if request.prompt.is_empty() {
            return Err(AcpSchemaError::invalid_params());
        }

        let notification_tx = self.notification_tx.read().await;
        let Some(tx) = notification_tx.as_ref() else {
            tracing::error!("No notification channel available");
            return Err(AcpSchemaError::internal_error());
        };

        let bridge = NotificationBridge::new(tx.clone());
        let processor = self.processor.read().await;
        let stop_reason = processor
            .process_prompt(&session, request.prompt, &bridge)
            .await
            .map_err(|e| {
                tracing::error!("Prompt processing error: {}", e);
                match e {
                    AcpError::NotAuthenticated(_) => AcpSchemaError::invalid_params(),
                    _ => AcpSchemaError::internal_error(),
                }
            })?;

        Ok(PromptResponse::new(stop_reason))
    }

    async fn cancel(
        &self,
        request: agent_client_protocol::CancelNotification,
    ) -> agent_client_protocol::Result<()> {
        tracing::info!("ACP cancel: session={}", request.session_id);
        if let Err(e) = self.sessions.cancel_session(&request.session_id) {
            tracing::warn!("Failed to cancel session: {}", e);
        }
        Ok(())
    }

    async fn set_session_mode(
        &self,
        request: SetSessionModeRequest,
    ) -> agent_client_protocol::Result<SetSessionModeResponse> {
        tracing::info!(
            "ACP set_session_mode: session={}, mode={:?}",
            request.session_id,
            request.mode_id
        );

        let session = self
            .sessions
            .get_session(&request.session_id)
            .map_err(|_e| AcpSchemaError::invalid_params())?;
        let mode = request.mode_id.to_string();
        if !matches!(mode.as_str(), "code" | "plan") {
            return Err(AcpSchemaError::invalid_params());
        }
        session.set_mode(Some(mode)).await;

        Ok(SetSessionModeResponse::new())
    }

    async fn ext_method(&self, request: ExtRequest) -> agent_client_protocol::Result<ExtResponse> {
        tracing::debug!("ACP ext_method: {}", request.method);
        Err(AcpSchemaError::method_not_found())
    }

    async fn ext_notification(
        &self,
        notification: agent_client_protocol::ExtNotification,
    ) -> agent_client_protocol::Result<()> {
        tracing::debug!("ACP ext_notification: {}", notification.method);
        Ok(())
    }

    async fn set_session_model(
        &self,
        request: SetSessionModelRequest,
    ) -> agent_client_protocol::Result<SetSessionModelResponse> {
        tracing::info!(
            "ACP set_session_model: session={}, model={:?}",
            request.session_id,
            request.model_id
        );

        let session = self
            .sessions
            .get_session(&request.session_id)
            .map_err(|_e| AcpSchemaError::invalid_params())?;

        let model_id_str = request.model_id.to_string();
        self.set_model_for_session(&session, &model_id_str, true)
            .await
            .map_err(|e| {
                tracing::error!("Failed to set model: {}", e);
                AcpSchemaError::invalid_params()
            })?;

        tracing::info!("Model switched to: {}", model_id_str);
        Ok(SetSessionModelResponse::new())
    }
}

impl MitsuroAgent {
    async fn replay_session_history(&self, session: &SessionState) -> Result<(), AcpError> {
        let notification_tx = self.notification_tx.read().await;
        let tx = notification_tx.as_ref().ok_or_else(|| {
            AcpError::ProtocolError("ACP notification channel is not connected".to_string())
        })?;
        let bridge = NotificationBridge::new(tx.clone());

        for message in session.get_messages().await {
            if message.role == Role::System {
                continue;
            }

            let mut references = crate::acp::source_references::SourceReferences::default();
            for content in message.content {
                let update = match content {
                    Content::Text {
                        mut text,
                        citations,
                    } => {
                        if message.role == Role::Assistant {
                            references.extend(&citations);
                            if let Some(sources) = references.take_text() {
                                text.push_str(&sources);
                            }
                        }
                        let content =
                            crate::acp::source_references::cited_text_content(text, &citations);
                        let chunk = ContentChunk::new(ContentBlock::Text(content));
                        match message.role {
                            Role::User => SessionUpdate::UserMessageChunk(chunk),
                            Role::Assistant | Role::Tool => SessionUpdate::AgentMessageChunk(chunk),
                            Role::System => continue,
                        }
                    }
                    Content::Thinking { thinking, .. } => {
                        let chunk =
                            ContentChunk::new(ContentBlock::Text(TextContent::new(thinking)));
                        SessionUpdate::AgentThoughtChunk(chunk)
                    }
                    Content::RedactedThinking { .. } => continue,
                    Content::ToolUse { id, name, input } => {
                        let call = ToolCall::new(ToolCallId::from(id), format!("Running {}", name))
                            .kind(tool_name_to_kind(&name))
                            .status(ToolCallStatus::InProgress)
                            .raw_input(input);
                        SessionUpdate::ToolCall(call)
                    }
                    Content::ToolResult {
                        tool_use_id,
                        output,
                        is_error,
                    } => {
                        let output_text = output
                            .as_str()
                            .map(ToOwned::to_owned)
                            .unwrap_or_else(|| output.to_string());
                        let update = if is_error.unwrap_or(false) {
                            create_tool_call_failed(&tool_use_id, &output_text)
                        } else {
                            create_tool_call_complete(
                                &tool_use_id,
                                vec![text_to_tool_content(&output_text)],
                            )
                        };
                        SessionUpdate::ToolCallUpdate(update)
                    }
                    Content::Image { .. } => {
                        replay_placeholder_update(message.role.clone(), "[image attachment]")
                    }
                    Content::Document { .. } => {
                        replay_placeholder_update(message.role.clone(), "[document attachment]")
                    }
                };

                bridge
                    .session_notification(SessionNotification::new(session.id.clone(), update))
                    .await
                    .map_err(|error| AcpError::ProtocolError(error.to_string()))?;
            }
        }

        Ok(())
    }
}

fn replay_placeholder_update(role: Role, text: &str) -> SessionUpdate {
    let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text)));
    if role == Role::User {
        SessionUpdate::UserMessageChunk(chunk)
    } else {
        SessionUpdate::AgentMessageChunk(chunk)
    }
}

fn available_session_modes() -> Vec<SessionMode> {
    vec![
        SessionMode::new("code", "Code").description("Write and edit code directly"),
        SessionMode::new("plan", "Plan").description("Plan changes before implementing"),
    ]
}

fn acp_model_infos(models: &[AvailableModelRecord]) -> Vec<AcpModelInfo> {
    models
        .iter()
        .map(|record| {
            let name = format!(
                "[{}] {}",
                record.key().provider,
                record.runtime.display_name
            );
            AcpModelInfo::new(ModelId::new(record.acp_model_id.clone()), name)
        })
        .collect()
}
