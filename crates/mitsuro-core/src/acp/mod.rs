//! ACP (Agent Client Protocol) server implementation
//!
//! This module implements the Agent Client Protocol, enabling Mitsuro to act as
//! an ACP-compatible agent that can integrate with any ACP-supporting editor
//! (Zed, Neovim, JetBrains, Marimo).
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────┐     JSON-RPC 2.0      ┌─────────────────┐
//! │     Editor      │◄────── stdio ────────►│     Mitsuro      │
//! │  (ACP Client)   │                       │   (ACP Agent)   │
//! │                 │   initialize          │                 │
//! │  - Zed          │   session/new         │  MitsuroAgent    │
//! │  - Neovim       │   session/prompt      │  SessionManager │
//! │  - JetBrains    │   session/update      │  Tool Bridge    │
//! └─────────────────┘                       └─────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use mitsuro_core::acp::AcpServer;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let server = AcpServer::new()?;
//!     server.run().await
//! }
//! ```

mod agent;
mod bridge;
mod delegation;
mod error;
mod model_manager;
mod processor;
mod server;
mod session;
mod source_references;
mod tools;
mod workspace_context;

pub use agent::MitsuroAgent;
pub use bridge::{create_notification_channel, AcpOutbound, NotificationBridge};
pub use error::AcpError;
pub use model_manager::{CachedProviderInfo, ModelManager};
pub use processor::PromptProcessor;
pub use server::AcpServer;
pub use session::{SessionManager, SessionState};
pub use workspace_context::WorkspaceContextBuilder;
