//! Mitsuro TUI v2.
//!
//! Terminal presentation and typed adapters to the shared agent runtime.

mod app;
mod components;
mod input;
mod layout;
mod model;
mod motion;
mod presentation;
mod projection;
mod render;
mod services;
mod terminal;

#[cfg(test)]
mod test_support;

use anyhow::Result;

/// How the TUI finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TuiOutcome {
    Quit,
    ApplyUpdate { version: String },
}

/// Run the terminal interface.
pub async fn run() -> Result<TuiOutcome> {
    app::run().await
}
