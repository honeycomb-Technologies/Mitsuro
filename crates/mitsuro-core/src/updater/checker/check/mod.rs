use anyhow::Result;
use tracing::{debug, info};

use super::types::UpdateInfo;
use super::VERSION;

mod release;

pub(super) fn is_newer_version(new: &str, current: &str) -> bool {
    if let (Ok(new), Ok(current)) = (semver::Version::parse(new), semver::Version::parse(current)) {
        return new.cmp_precedence(&current) == std::cmp::Ordering::Greater;
    }

    let parse = |s: &str| -> (u32, u32, u32) {
        let mut parts = s.split('.');
        (
            parts.next().and_then(|p| p.parse().ok()).unwrap_or(0),
            parts.next().and_then(|p| p.parse().ok()).unwrap_or(0),
            parts.next().and_then(|p| p.parse().ok()).unwrap_or(0),
        )
    };

    parse(new) > parse(current)
}

pub async fn check_for_updates() -> Result<Option<UpdateInfo>> {
    info!("Checking for updates (current version: {})", VERSION);
    debug!("Checking GitHub releases");
    release::check_for_updates_release().await
}
