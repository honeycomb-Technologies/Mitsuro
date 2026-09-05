use std::collections::BTreeMap;
use std::fs;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::spatial::{Camera, StageLayout};
use crate::theme::BackgroundPreferencesV1;

const PREFERENCES_VERSION: u16 = 1;
const MAX_PREFERENCES_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_AGENT_DRAFT_BYTES: usize = 32 * 1024;
pub const MAX_AGENT_DRAFTS_PER_CANVAS: usize = 64;
pub const MAX_SCROLL_OFFSETS_PER_CANVAS: usize = 128;
const MAX_SCROLL_OFFSET: f32 = 10_000_000.0;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CanvasPersonalState {
    pub camera: Camera,
    pub selected_node: Option<Uuid>,
    pub stage_layout: StageLayout,
    #[serde(default)]
    pub stage_slots: Vec<Uuid>,
    #[serde(default)]
    pub drafts: BTreeMap<Uuid, String>,
    #[serde(default)]
    pub scroll_offsets: BTreeMap<Uuid, f32>,
    #[serde(default)]
    pub pulse_open: bool,
    #[serde(default)]
    pub background: BackgroundPreferencesV1,
}

impl Default for CanvasPersonalState {
    fn default() -> Self {
        Self {
            camera: Camera::default(),
            selected_node: None,
            stage_layout: StageLayout::One,
            stage_slots: Vec::new(),
            drafts: BTreeMap::new(),
            scroll_offsets: BTreeMap::new(),
            pulse_open: false,
            background: BackgroundPreferencesV1::default(),
        }
    }
}

impl CanvasPersonalState {
    pub fn draft(&self, node_id: Uuid) -> String {
        self.drafts
            .get(&node_id)
            .map(|draft| bounded_agent_draft(draft))
            .unwrap_or_default()
    }

    pub fn set_draft(&mut self, node_id: Uuid, draft: &str) -> bool {
        let draft = bounded_agent_draft(draft);
        if draft.is_empty() {
            self.drafts.remove(&node_id);
            return true;
        }
        if !self.drafts.contains_key(&node_id) && self.drafts.len() >= MAX_AGENT_DRAFTS_PER_CANVAS {
            return false;
        }
        self.drafts.insert(node_id, draft);
        true
    }

    pub fn clear_node_state(&mut self, node_id: Uuid) -> bool {
        let draft_removed = self.drafts.remove(&node_id).is_some();
        let scroll_removed = self.scroll_offsets.remove(&node_id).is_some();
        draft_removed || scroll_removed
    }

    pub fn scroll_offset(&self, node_id: Uuid) -> f32 {
        self.scroll_offsets
            .get(&node_id)
            .copied()
            .filter(|offset| offset.is_finite())
            .unwrap_or_default()
            .clamp(-MAX_SCROLL_OFFSET, MAX_SCROLL_OFFSET)
    }

    pub fn set_scroll_offset(&mut self, node_id: Uuid, offset: f32) -> bool {
        if !offset.is_finite() {
            return false;
        }
        let offset = offset.clamp(-MAX_SCROLL_OFFSET, MAX_SCROLL_OFFSET);
        if offset.abs() < f32::EPSILON {
            self.scroll_offsets.remove(&node_id);
            return true;
        }
        if !self.scroll_offsets.contains_key(&node_id)
            && self.scroll_offsets.len() >= MAX_SCROLL_OFFSETS_PER_CANVAS
        {
            return false;
        }
        self.scroll_offsets.insert(node_id, offset);
        true
    }

    fn normalize(&mut self) {
        self.camera = self.camera.normalized();
        let mut drafts = BTreeMap::new();
        for (node_id, draft) in std::mem::take(&mut self.drafts)
            .into_iter()
            .take(MAX_AGENT_DRAFTS_PER_CANVAS)
        {
            let draft = bounded_agent_draft(&draft);
            if !draft.is_empty() {
                drafts.insert(node_id, draft);
            }
        }
        self.drafts = drafts;

        self.scroll_offsets = std::mem::take(&mut self.scroll_offsets)
            .into_iter()
            .filter_map(|(node_id, offset)| {
                if !offset.is_finite() || offset.abs() < f32::EPSILON {
                    None
                } else {
                    Some((node_id, offset.clamp(-MAX_SCROLL_OFFSET, MAX_SCROLL_OFFSET)))
                }
            })
            .take(MAX_SCROLL_OFFSETS_PER_CANVAS)
            .collect();
    }
}

pub fn bounded_agent_draft(draft: &str) -> String {
    if draft.len() <= MAX_AGENT_DRAFT_BYTES {
        return draft.to_owned();
    }
    let mut end = MAX_AGENT_DRAFT_BYTES;
    while !draft.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    draft[..end].to_owned()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DesktopPreferences {
    pub schema_version: u16,
    #[serde(default)]
    pub last_canvas_by_server: BTreeMap<String, String>,
    #[serde(default)]
    pub canvases: BTreeMap<String, CanvasPersonalState>,
}

impl Default for DesktopPreferences {
    fn default() -> Self {
        Self {
            schema_version: PREFERENCES_VERSION,
            last_canvas_by_server: BTreeMap::new(),
            canvases: BTreeMap::new(),
        }
    }
}

impl DesktopPreferences {
    pub fn load_default() -> Result<Self> {
        Self::load_from(&Self::default_path())
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        let file = match fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => return Err(error).context("opening native canvas preferences"),
        };
        let mut bytes = Vec::with_capacity(MAX_PREFERENCES_BYTES.min(64 * 1024) as usize);
        file.take(MAX_PREFERENCES_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("reading native canvas preferences")?;
        if bytes.len() as u64 > MAX_PREFERENCES_BYTES {
            anyhow::bail!("native canvas preferences exceed 4 MiB");
        }
        let mut parsed: Self =
            serde_json::from_slice(&bytes).context("parsing native canvas preferences")?;
        if parsed.schema_version != PREFERENCES_VERSION {
            return Ok(Self::default());
        }
        for state in parsed.canvases.values_mut() {
            state.normalize();
        }
        Ok(parsed)
    }

    pub fn save_default(&self) -> Result<()> {
        self.save_to(&Self::default_path())
    }

    pub fn touch_canvas(&mut self, _key: &str) {}

    pub fn save_to(&self, path: &Path) -> Result<()> {
        let parent = path.parent().context("preferences path has no parent")?;
        fs::create_dir_all(parent).context("creating native canvas state directory")?;
        let temporary = parent.join(format!(
            ".{}.{}.{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("state"),
            std::process::id(),
            Uuid::new_v4()
        ));
        let bytes =
            serde_json::to_vec_pretty(self).context("encoding native canvas preferences")?;
        if bytes.len() as u64 > MAX_PREFERENCES_BYTES {
            anyhow::bail!("native canvas preferences exceed 4 MiB");
        }
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .context("creating temporary native canvas preferences")?;
        file.write_all(&bytes)
            .context("writing native canvas preferences")?;
        file.sync_all()
            .context("syncing native canvas preferences")?;
        fs::rename(&temporary, path).context("publishing native canvas preferences")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .context("securing native canvas preferences")?;
            fs::File::open(parent)
                .context("opening native canvas state directory")?
                .sync_all()
                .context("syncing native canvas state directory")?;
        }
        Ok(())
    }

    pub fn canvas_key(server: &str, canvas_id: &str) -> String {
        format!("{}|{}", server.trim_end_matches('/'), canvas_id)
    }

    pub fn default_path() -> PathBuf {
        mitsuro_core::paths::config_dir()
            .join("desktop-native")
            .join("state.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preferences_round_trip_personal_state() {
        let temp = tempfile::tempdir().expect("temp");
        let path = temp.path().join("state.json");
        let mut preferences = DesktopPreferences::default();
        let node = Uuid::new_v4();
        let mut state = CanvasPersonalState {
            selected_node: Some(node),
            stage_layout: StageLayout::Four,
            pulse_open: true,
            ..CanvasPersonalState::default()
        };
        state.stage_slots.push(node);
        assert!(state.set_draft(node, "keep this thought"));
        assert!(state.set_scroll_offset(node, -420.0));
        preferences.canvases.insert(
            DesktopPreferences::canvas_key("http://localhost:3000/", "c1"),
            state,
        );
        preferences.save_to(&path).expect("save");
        let loaded = DesktopPreferences::load_from(&path).expect("load");
        assert_eq!(loaded, preferences);
    }

    #[test]
    fn drafts_are_utf8_safe_and_count_bounded() {
        let oversized = format!("{}é", "x".repeat(MAX_AGENT_DRAFT_BYTES - 1));
        let bounded = bounded_agent_draft(&oversized);
        assert!(bounded.len() <= MAX_AGENT_DRAFT_BYTES);
        assert!(bounded.is_char_boundary(bounded.len()));

        let mut state = CanvasPersonalState::default();
        for _ in 0..MAX_AGENT_DRAFTS_PER_CANVAS {
            assert!(state.set_draft(Uuid::new_v4(), "draft"));
        }
        assert!(!state.set_draft(Uuid::new_v4(), "overflow"));
        assert_eq!(state.drafts.len(), MAX_AGENT_DRAFTS_PER_CANVAS);
    }

    #[test]
    fn persisted_personal_state_is_normalized_on_load() {
        let temp = tempfile::tempdir().expect("temp");
        let path = temp.path().join("state.json");
        let node = Uuid::new_v4();
        let mut state = CanvasPersonalState::default();
        state
            .drafts
            .insert(node, "x".repeat(MAX_AGENT_DRAFT_BYTES + 200));
        state.scroll_offsets.insert(node, MAX_SCROLL_OFFSET * 2.0);
        let mut preferences = DesktopPreferences::default();
        preferences
            .canvases
            .insert("server|canvas".to_owned(), state);
        preferences.save_to(&path).expect("save");

        let loaded = DesktopPreferences::load_from(&path).expect("load");
        let state = loaded.canvases.get("server|canvas").expect("state");
        assert_eq!(state.draft(node).len(), MAX_AGENT_DRAFT_BYTES);
        assert_eq!(state.scroll_offset(node), MAX_SCROLL_OFFSET);
    }

    #[test]
    fn scroll_offsets_are_finite_bounded_and_removed_with_the_node() {
        let mut state = CanvasPersonalState::default();
        let node = Uuid::new_v4();
        assert!(!state.set_scroll_offset(node, f32::NAN));
        assert!(state.set_scroll_offset(node, -42.5));
        assert_eq!(state.scroll_offset(node), -42.5);
        assert!(state.clear_node_state(node));
        assert_eq!(state.scroll_offset(node), 0.0);

        for _ in 0..MAX_SCROLL_OFFSETS_PER_CANVAS {
            assert!(state.set_scroll_offset(Uuid::new_v4(), 1.0));
        }
        assert!(!state.set_scroll_offset(Uuid::new_v4(), 1.0));
    }

    #[test]
    fn oversized_preferences_file_is_rejected_before_parsing() {
        let temp = tempfile::tempdir().expect("temp");
        let path = temp.path().join("state.json");
        let file = fs::File::create(&path).expect("create");
        file.set_len(MAX_PREFERENCES_BYTES + 1).expect("extend");
        assert!(DesktopPreferences::load_from(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn preferences_are_private() {
        use std::os::unix::fs::PermissionsExt as _;
        let temp = tempfile::tempdir().expect("temp");
        let path = temp.path().join("state.json");
        DesktopPreferences::default().save_to(&path).expect("save");
        assert_eq!(
            fs::metadata(path).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
    }
}
