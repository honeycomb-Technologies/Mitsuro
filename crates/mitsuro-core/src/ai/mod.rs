//! AI provider layer
//!
//! Handles communication with AI providers (MiniMax, OpenRouter, ZAi, OpenAI, etc.)
//! Supports multiple API formats: Anthropic, OpenAI, and Google.

// Modular architecture
pub mod anthropic_catalog;
pub mod catalog;
pub mod client;
pub mod context_policy;
pub mod format;
pub mod format_detection;
pub mod grok;
pub mod minimax_catalog;
pub mod retry;

// Provider-specific configuration
pub mod glm;
pub mod model_profile;
pub mod models;
pub mod openai;
pub mod openrouter;

// Shared infrastructure
pub mod parsers;
pub mod providers;
pub mod reasoning;
pub mod sse;
pub mod stream_smoothing;
pub mod streaming;
pub mod title;
pub mod transform;
pub mod transport_policy;
pub mod types;
pub(crate) mod usage;

// Re-export main types from new module
pub use client::{
    AiClient, AiClientConfig, CallOptions, PromptCacheRetention, RemoteAttemptPolicy,
    MITSURO_SYSTEM_PROMPT,
};

pub use title::{derive_pinch_title, derive_title, generate_pinch_title, generate_title};
