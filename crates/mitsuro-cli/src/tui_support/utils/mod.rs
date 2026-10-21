//! Utilities for the TUI
//!
//! Common helper functions and types used throughout the TUI.

mod channels;
pub mod clipboard;
mod syntax;

pub use channels::{DeviceCodeInfo, OAuthStatusUpdate};
pub use syntax::highlight_code;
