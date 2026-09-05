//! No-op acceptance probes so product-mode compiles without the recovered
//! telemetry crate. The full probe file remains in recovery artifacts.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptanceCollaborationRole {
    Primary,
    #[allow(dead_code)]
    Secondary,
}

pub fn configured_collaboration_role() -> Option<AcceptanceCollaborationRole> {
    None
}

pub fn observe_counter(_name: &str, _value: u64) {}

pub fn observe_collaboration<'a>(
    _client_id: &str,
    _clients: impl IntoIterator<Item = &'a str>,
    _previews: impl IntoIterator<Item = &'a str>,
) {
}
