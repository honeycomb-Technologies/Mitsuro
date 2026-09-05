use std::sync::Arc;

use agent_client_protocol::{
    Agent, InitializeRequest, LoadSessionRequest, NewSessionRequest, ProtocolVersion,
};
use tempfile::tempdir;
use tokio::sync::{mpsc, Mutex, RwLock};

use super::{
    acp_model_id_for_key, decode_acp_model_id, negotiate_protocol_version,
    persist_current_model_preference, AvailableModelRecord, MitsuroAgent,
};
use crate::acp::processor::PromptProcessor;
use crate::acp::session::SessionManager;
use crate::agent::loop_events::LoopStopReason;
use crate::ai::models::{ApiFormat, ModelAuthScope, ModelCatalogSource, ModelKey, ModelMetadata};
use crate::ai::providers::ProviderId;
use crate::storage::{
    Database, PartialAssistantState, Preferences, RecoveryDecision, RecoveryStatus,
    SessionManager as StorageSessionManager, SessionRecoveryState,
};
use crate::tools::registry::ToolRegistry;

fn agent_with_storage(storage: Arc<Mutex<StorageSessionManager>>) -> MitsuroAgent {
    let tools = Arc::new(ToolRegistry::new());
    MitsuroAgent {
        sessions: Arc::new(SessionManager::with_storage(storage)),
        tools: tools.clone(),
        client_capabilities: RwLock::new(None),
        api_key: RwLock::new(None),
        processor: RwLock::new(PromptProcessor::new(tools)),
        notification_tx: RwLock::new(None),
        current_model: RwLock::new(None),
        available_models: RwLock::new(Vec::new()),
    }
}

#[tokio::test]
async fn test_agent_creation() {
    let agent = MitsuroAgent::new();
    assert_eq!(agent.sessions().session_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_new_session() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::new(&dir.path().join("test.db"))?;
    let agent = agent_with_storage(Arc::new(Mutex::new(StorageSessionManager::new(db))));

    let request = NewSessionRequest::new("/tmp");
    let response = agent.new_session(request).await?;

    assert!(agent.sessions().has_session(&response.session_id));
    Ok(())
}

#[test]
fn negotiate_protocol_version_rejects_legacy_and_future_versions() {
    let legacy: ProtocolVersion = serde_json::from_str("\"1.0.0\"").expect("legacy version");
    let future: ProtocolVersion = serde_json::from_str("2").expect("future version");

    assert_eq!(negotiate_protocol_version(legacy), ProtocolVersion::LATEST);
    assert_eq!(negotiate_protocol_version(future), ProtocolVersion::LATEST);
}

#[tokio::test]
async fn initialize_negotiates_to_supported_protocol_version() -> anyhow::Result<()> {
    let agent = MitsuroAgent::new();
    let future_version: ProtocolVersion = serde_json::from_str("2")?;

    let response = agent
        .initialize(InitializeRequest::new(future_version))
        .await?;

    assert_eq!(response.protocol_version, ProtocolVersion::LATEST);
    Ok(())
}

#[tokio::test]
async fn load_session_restores_recovery_only_storage_session() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    let db = Database::new(&db_path)?;
    let storage = Arc::new(Mutex::new(StorageSessionManager::new(db)));
    let storage_session_id = {
        let storage = storage.lock().await;
        storage.create_session("Recovered Session", None, Some("/tmp"))?
    };
    {
        let storage = storage.lock().await;
        storage.update_recovery_state(
            &storage_session_id,
            &SessionRecoveryState::new(
                RecoveryStatus::Interrupted,
                Some(LoopStopReason::ProviderError),
                Some("provider failed".to_string()),
                PartialAssistantState {
                    citations: Vec::new(),
                    text: "Partial answer".to_string(),
                    thinking: String::new(),
                    tool_calls: Vec::new(),
                },
                RecoveryDecision::Resumable {
                    latest_user_objective: "Finish the answer".to_string(),
                },
            ),
        )?;
    }

    let agent = agent_with_storage(storage);
    let (notification_tx, _notification_rx) = mpsc::channel(8);
    agent.set_notification_channel(notification_tx).await;
    let request = LoadSessionRequest::new(storage_session_id.clone(), "/tmp");
    agent.load_session(request).await?;

    assert_eq!(agent.sessions().session_count(), 1);
    assert!(agent
        .sessions()
        .has_session(&agent_client_protocol::SessionId::from(storage_session_id)));
    Ok(())
}

#[tokio::test]
async fn load_session_rejects_unknown_id_without_creating_replacement() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::new(&dir.path().join("test.db"))?;
    let storage = Arc::new(Mutex::new(StorageSessionManager::new(db)));
    let agent = agent_with_storage(storage);
    let (notification_tx, _notification_rx) = mpsc::channel(8);
    agent.set_notification_channel(notification_tx).await;

    let result = agent
        .load_session(LoadSessionRequest::new("does-not-exist", "/tmp"))
        .await;

    assert!(result.is_err());
    assert_eq!(agent.sessions().session_count(), 0);
    Ok(())
}

#[tokio::test]
async fn model_selection_is_isolated_per_acp_session() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::new(&dir.path().join("test.db"))?;
    let agent = agent_with_storage(Arc::new(Mutex::new(StorageSessionManager::new(db))));
    let model_a = ModelMetadata::new("model-a", "Model A", ProviderId::MiniMax)
        .with_transport(ApiFormat::Anthropic);
    let model_b = ModelMetadata::new("model-b", "Model B", ProviderId::MiniMax)
        .with_transport(ApiFormat::Anthropic);
    let model_a_id = acp_model_id_for_key(&model_a.key());
    let model_b_id = acp_model_id_for_key(&model_b.key());
    *agent.available_models.write().await = vec![
        AvailableModelRecord::new(model_a, "test-key-a".to_string(), None),
        AvailableModelRecord::new(model_b, "test-key-b".to_string(), None),
    ];
    let first = agent.sessions().create_session(Some("/tmp".into()), None);
    let second = agent.sessions().create_session(Some("/tmp".into()), None);

    agent
        .set_model_for_session(&first, &model_a_id, false)
        .await?;
    agent
        .set_model_for_session(&second, &model_b_id, false)
        .await?;

    assert_eq!(
        first
            .selected_model()
            .await
            .expect("first model")
            .key
            .model_id,
        "model-a"
    );
    assert_eq!(
        second
            .selected_model()
            .await
            .expect("second model")
            .key
            .model_id,
        "model-b"
    );
    assert!(agent.current_model_id().await.is_none());
    Ok(())
}

#[test]
fn acp_model_id_round_trips_complete_key() {
    let key = ModelKey::new(
        ProviderId::OpenAI,
        "shared-slug",
        ApiFormat::OpenAIResponses,
    )
    .with_auth_scope(ModelAuthScope::OAuth);

    let encoded = acp_model_id_for_key(&key);

    assert_eq!(decode_acp_model_id(&encoded), Some(key));
}

#[test]
fn legacy_acp_model_id_reads_without_changing_canonical_emission() {
    let key = ModelKey::new(ProviderId::OpenAI, "model-a", ApiFormat::OpenAIResponses);
    let canonical = acp_model_id_for_key(&key);
    assert!(canonical.starts_with("mitsuro:model-key:"));
    let legacy = canonical.replacen("mitsuro:model-key:", "krusty:model-key:", 1);
    assert_eq!(decode_acp_model_id(&legacy), Some(key));
}

#[test]
fn acp_shared_preference_write_preserves_exact_key() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let preferences = Preferences::new(Database::new(&dir.path().join("preferences.db"))?);
    let key = ModelKey::new(ProviderId::Grok, "grok-4.5", ApiFormat::OpenAIResponses);

    persist_current_model_preference(&preferences, &key)?;

    assert_eq!(preferences.get_current_model_key(), Some(key));
    assert_eq!(preferences.get_current_model().as_deref(), Some("grok-4.5"));
    Ok(())
}

#[tokio::test]
async fn same_slug_variants_require_exact_identity() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::new(&dir.path().join("test.db"))?;
    let agent = agent_with_storage(Arc::new(Mutex::new(StorageSessionManager::new(db))));

    let mut api = ModelMetadata::new("shared-slug", "API", ProviderId::OpenAI)
        .with_transport(ApiFormat::OpenAIResponses);
    api.auth_scope = Some(ModelAuthScope::ApiKey);
    let mut oauth = api.clone();
    oauth.display_name = "OAuth".to_string();
    oauth.auth_scope = Some(ModelAuthScope::OAuth);
    let oauth_id = acp_model_id_for_key(&oauth.key());
    *agent.available_models.write().await = vec![
        AvailableModelRecord::new(api, "sk-api".to_string(), None),
        AvailableModelRecord::new(
            oauth.clone(),
            "oauth-token".to_string(),
            Some("acct".into()),
        ),
    ];

    assert!(agent
        .resolve_persisted_model_id("shared-slug")
        .await
        .is_none());
    assert_eq!(
        agent.resolve_persisted_model_id(&oauth_id).await,
        Some(oauth_id)
    );
    Ok(())
}

#[tokio::test]
async fn session_selection_uses_and_persists_exact_runtime() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let db = Database::new(&dir.path().join("test.db"))?;
    let storage = Arc::new(Mutex::new(StorageSessionManager::new(db)));
    let agent = agent_with_storage(Arc::clone(&storage));
    let session = agent
        .sessions()
        .create_persisted_session(Some("/tmp".into()), None)
        .await?;

    let mut metadata = ModelMetadata::new("grok-4.5", "Grok Exact", ProviderId::Grok)
        .with_context(456_789, 12_345)
        .with_transport(ApiFormat::OpenAI);
    metadata.catalog_source = ModelCatalogSource::LiveDynamic;
    metadata.catalog_revision = Some("grok-catalog-42".to_string());
    metadata.supports_vision = true;
    let expected_runtime = metadata.resolve_runtime();
    let model_id = acp_model_id_for_key(&metadata.key());
    *agent.available_models.write().await = vec![AvailableModelRecord::new(
        metadata,
        "grok-token".to_string(),
        None,
    )];

    agent
        .set_model_for_session(&session, &model_id, true)
        .await?;

    assert_eq!(
        session.selected_model().await.expect("selection").key,
        expected_runtime.key
    );
    assert_eq!(
        session.ai_client().await.expect("client").resolved_model(),
        &expected_runtime
    );
    let stored = storage
        .lock()
        .await
        .get_session(&session.id.to_string())?
        .expect("stored ACP session");
    assert_eq!(stored.model_key, Some(expected_runtime.key));
    assert_eq!(stored.model.as_deref(), Some("grok-4.5"));
    assert_eq!(
        stored.model_catalog_revision.as_deref(),
        Some("grok-catalog-42")
    );
    Ok(())
}
