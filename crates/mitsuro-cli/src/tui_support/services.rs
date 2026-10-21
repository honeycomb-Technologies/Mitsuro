//! Shared runtime services for the terminal (used by tui_v2).

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::agent::UserHookManager;
use crate::ai::models::SharedModelRegistry;
use crate::ai::types::AiTool;
use crate::extensions::{WasmExtension, WasmHost};
use crate::plan::PlanManager;
use crate::plugins::PluginManager;
use crate::storage::{CredentialStore, Preferences, SessionManager};
use crate::tools::ToolRegistry;
use mitsuro_core::skills::SkillsManager;

use super::utils::OAuthStatusUpdate;

/// Application services and external systems shared by the terminal runtime.
pub struct AppServices {
    pub plan_manager: Option<PlanManager>,
    pub session_manager: Option<SessionManager>,
    pub preferences: Option<Preferences>,
    pub credential_store: CredentialStore,
    pub model_registry: SharedModelRegistry,
    pub tool_registry: Arc<ToolRegistry>,
    pub cached_ai_tools: Vec<AiTool>,
    pub user_hook_manager: Arc<RwLock<UserHookManager>>,
    pub _wasm_host: Option<Arc<WasmHost>>,
    pub _wasm_extensions: Vec<WasmExtension>,
    pub plugin_manager: Option<Arc<PluginManager>>,
    pub skills_manager: Arc<RwLock<SkillsManager>>,
    pub mcp_manager: Arc<mitsuro_core::mcp::McpManager>,
    pub oauth_status_tx: tokio::sync::mpsc::UnboundedSender<OAuthStatusUpdate>,
}
