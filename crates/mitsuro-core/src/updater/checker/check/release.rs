use anyhow::{anyhow, Result};
use tracing::{debug, info};

use crate::updater::checker::types::UpdateInfo;
use crate::updater::checker::{GITHUB_REPO, VERSION};

use super::is_newer_version;

pub(super) async fn check_for_updates_release() -> Result<Option<UpdateInfo>> {
    let url = format!(
        "https://api.github.com/repos/{}/releases/latest",
        GITHUB_REPO
    );
    debug!("Fetching: {}", url);

    let client = reqwest::Client::builder()
        .user_agent("mitsuro-updater")
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(anyhow!("GitHub API returned: {}", response.status()));
    }

    let release: serde_json::Value = response.json().await?;

    let tag_name = release["tag_name"]
        .as_str()
        .ok_or_else(|| anyhow!("No tag_name in release"))?;
    let new_version = tag_name.strip_prefix('v').unwrap_or(tag_name);
    debug!("Latest release: {} (current: {})", new_version, VERSION);

    if new_version == VERSION {
        info!("Already up to date");
        return Ok(None);
    }

    if !is_newer_version(new_version, VERSION) {
        info!("Current version is newer than release");
        return Ok(None);
    }

    let release_notes = release["body"]
        .as_str()
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("New version available")
        .to_string();

    info!("Update available: {} -> {}", VERSION, new_version);

    Ok(Some(UpdateInfo {
        current_version: VERSION.to_string(),
        new_version: new_version.to_string(),
        release_notes,
        is_dev_mode: false,
        apply: crate::updater::UpdateChannel::detect().apply_policy(),
    }))
}
