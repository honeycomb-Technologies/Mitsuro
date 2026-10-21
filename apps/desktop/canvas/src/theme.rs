use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{bail, Context as _, Result};
use gpui::{rgb, rgba, Hsla, ImageFormat as GpuiImageFormat};
use image::{ImageFormat as DecodedImageFormat, ImageReader};
use serde::{Deserialize, Serialize};

pub const THEME_SCHEMA_VERSION: u16 = 1;
pub const DEFAULT_THEME_ID: &str = "graphite";
pub const PULSE_EFFECT_ID: &str = "pulse_veil";
const MAX_THEME_IMAGE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_THEME_IMAGE_DIMENSION: u32 = 8_192;
const MAX_THEME_IMAGE_PIXELS: u64 = 32 * 1024 * 1024;
const MAX_THEME_MANIFEST_BYTES: u64 = 256 * 1024;
const MAX_THEME_PACKS: usize = 64;
const BUNDLED_STATIC_THEMES: &[(&str, &str)] = &[
    (
        "quiet-ink",
        include_str!("../assets/themes/quiet-ink/theme.toml"),
    ),
    (
        "violet-dusk",
        include_str!("../assets/themes/violet-dusk/theme.toml"),
    ),
];

pub fn hex(value: u32) -> Hsla {
    rgb(value).into()
}

pub fn hex_alpha(value: u32, alpha: f32) -> Hsla {
    let alpha = ((alpha.clamp(0.0, 1.0) * 255.0).round() as u32).min(255);
    rgba((value << 8) | alpha).into()
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CanvasPalette {
    pub canvas: Hsla,
    pub surface: Hsla,
    pub surface_high: Hsla,
    pub text: Hsla,
    pub text_muted: Hsla,
    pub edge: Hsla,
    pub edge_strong: Hsla,
    pub pulse: Hsla,
    pub pulse_soft: Hsla,
    pub brass: Hsla,
    pub danger: Hsla,
}

impl CanvasPalette {
    pub fn from_theme(theme: &VisualThemeV1) -> Self {
        let colors = &theme.colors;
        Self {
            canvas: parse_theme_color(&colors.canvas).unwrap_or_else(|| Self::default().canvas),
            surface: parse_theme_color(&colors.surface).unwrap_or_else(|| Self::default().surface),
            surface_high: parse_theme_color(&colors.surface_high)
                .unwrap_or_else(|| Self::default().surface_high),
            text: parse_theme_color(&colors.text).unwrap_or_else(|| Self::default().text),
            text_muted: parse_theme_color(&colors.text_muted)
                .unwrap_or_else(|| Self::default().text_muted),
            edge: parse_theme_color(&colors.edge).unwrap_or_else(|| Self::default().edge),
            edge_strong: parse_theme_color(&colors.edge_strong)
                .unwrap_or_else(|| Self::default().edge_strong),
            pulse: parse_theme_color(&colors.pulse).unwrap_or_else(|| Self::default().pulse),
            pulse_soft: parse_theme_color(&colors.pulse_soft)
                .unwrap_or_else(|| Self::default().pulse_soft),
            brass: parse_theme_color(&colors.brass).unwrap_or_else(|| Self::default().brass),
            danger: parse_theme_color(&colors.danger).unwrap_or_else(|| Self::default().danger),
        }
    }
}

impl Default for CanvasPalette {
    fn default() -> Self {
        Self {
            canvas: hex(0x0e0e11),
            surface: hex(0x141519),
            surface_high: hex(0x1a1b20),
            text: hex(0xe8e5ea),
            text_muted: hex_alpha(0xe8e5ea, 0.58),
            edge: hex_alpha(0xffffff, 0.075),
            edge_strong: hex_alpha(0xffffff, 0.14),
            pulse: hex(0x9d73ff),
            pulse_soft: hex_alpha(0x9d73ff, 0.16),
            brass: hex(0xb89a61),
            danger: hex(0xef6461),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ThemeColorsV1 {
    pub canvas: String,
    pub surface: String,
    pub surface_high: String,
    pub text: String,
    pub text_muted: String,
    pub edge: String,
    pub edge_strong: String,
    pub pulse: String,
    pub pulse_soft: String,
    pub brass: String,
    pub danger: String,
}

impl Default for ThemeColorsV1 {
    fn default() -> Self {
        Self {
            canvas: "#0e0e11".to_owned(),
            surface: "#141519".to_owned(),
            surface_high: "#1a1b20".to_owned(),
            text: "#e8e5ea".to_owned(),
            text_muted: "#e8e5ea94".to_owned(),
            edge: "#ffffff13".to_owned(),
            edge_strong: "#ffffff24".to_owned(),
            pulse: "#9d73ff".to_owned(),
            pulse_soft: "#9d73ff29".to_owned(),
            brass: "#b89a61".to_owned(),
            danger: "#ef6461".to_owned(),
        }
    }
}

impl ThemeColorsV1 {
    fn validate(&self) -> Result<()> {
        for color in [
            &self.canvas,
            &self.surface,
            &self.surface_high,
            &self.text,
            &self.text_muted,
            &self.edge,
            &self.edge_strong,
            &self.pulse,
            &self.pulse_soft,
            &self.brass,
            &self.danger,
        ] {
            validate_hex(color)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct VisualThemeV1 {
    pub schema_version: u16,
    pub id: String,
    pub display_name: String,
    #[serde(default = "default_shared")]
    pub shareable: bool,
    #[serde(default)]
    pub colors: ThemeColorsV1,
    pub background: BackgroundSpecV1,
}

fn default_shared() -> bool {
    true
}

impl VisualThemeV1 {
    pub fn graphite() -> Self {
        Self {
            schema_version: THEME_SCHEMA_VERSION,
            id: DEFAULT_THEME_ID.to_owned(),
            display_name: "Graphite".to_owned(),
            shareable: true,
            colors: ThemeColorsV1::default(),
            background: BackgroundSpecV1::NativeEffect {
                effect: PULSE_EFFECT_ID.to_owned(),
                seed: 19,
                intensity: 0.42,
                speed: 0.28,
                parallax: 0.03,
                fallback: StaticBackgroundV1::Gradient {
                    angle_deg: 145.0,
                    stops: vec![
                        GradientStopV1 {
                            offset: 0.0,
                            color: "#0e0e11".to_owned(),
                        },
                        GradientStopV1 {
                            offset: 1.0,
                            color: "#14121a".to_owned(),
                        },
                    ],
                },
            },
        }
    }

    pub fn validate(&self, root: &Path) -> Result<()> {
        if self.schema_version != THEME_SCHEMA_VERSION {
            bail!("unsupported theme schema {}", self.schema_version);
        }
        if self.id.is_empty()
            || self.id.len() > 64
            || !self
                .id
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '_')
        {
            bail!("theme id must be a lowercase slug");
        }
        if self.display_name.trim().is_empty()
            || self.display_name.chars().count() > 80
            || self.display_name.chars().any(char::is_control)
        {
            bail!("theme display name must be 1-80 visible characters");
        }
        self.colors.validate()?;
        self.background.validate(root)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackgroundSpecV1 {
    Solid {
        color: String,
    },
    Gradient {
        angle_deg: f32,
        stops: Vec<GradientStopV1>,
    },
    Image {
        asset: String,
        #[serde(default = "default_opacity")]
        opacity: f32,
        #[serde(default)]
        scrim: Option<String>,
    },
    NativeEffect {
        effect: String,
        seed: u64,
        intensity: f32,
        speed: f32,
        parallax: f32,
        fallback: StaticBackgroundV1,
    },
}

fn default_opacity() -> f32 {
    1.0
}

impl BackgroundSpecV1 {
    fn validate(&self, root: &Path) -> Result<()> {
        match self {
            Self::Solid { color } => validate_hex(color),
            Self::Gradient { angle_deg, stops } => validate_gradient(*angle_deg, stops),
            Self::Image {
                asset,
                opacity,
                scrim,
            } => {
                if !(0.0..=1.0).contains(opacity) {
                    bail!("image opacity must be between zero and one");
                }
                if let Some(scrim) = scrim {
                    validate_hex(scrim)?;
                }
                load_validated_local_asset(root, asset).map(|_| ())
            }
            Self::NativeEffect {
                effect,
                intensity,
                speed,
                parallax,
                fallback,
                ..
            } => {
                if effect != PULSE_EFFECT_ID {
                    bail!("theme selects an unknown native effect");
                }
                if !(0.0..=1.0).contains(intensity)
                    || !(0.0..=1.0).contains(speed)
                    || !(0.0..=0.08).contains(parallax)
                {
                    bail!("native effect parameters are outside safe bounds");
                }
                fallback.validate()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StaticBackgroundV1 {
    Solid {
        color: String,
    },
    Gradient {
        angle_deg: f32,
        stops: Vec<GradientStopV1>,
    },
}

impl StaticBackgroundV1 {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Solid { color } => validate_hex(color),
            Self::Gradient { angle_deg, stops } => validate_gradient(*angle_deg, stops),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GradientStopV1 {
    pub offset: f32,
    pub color: String,
}

fn validate_stops(stops: &[GradientStopV1]) -> Result<()> {
    if !(2..=4).contains(&stops.len()) {
        bail!("gradients require two to four stops");
    }
    let mut previous = None;
    for stop in stops {
        if !(0.0..=1.0).contains(&stop.offset) {
            bail!("gradient stop outside zero to one");
        }
        if previous.is_some_and(|offset| stop.offset < offset) {
            bail!("gradient stops must be ordered");
        }
        validate_hex(&stop.color)?;
        previous = Some(stop.offset);
    }
    Ok(())
}

fn validate_gradient(angle_deg: f32, stops: &[GradientStopV1]) -> Result<()> {
    if !angle_deg.is_finite() {
        bail!("gradient angle must be finite");
    }
    validate_stops(stops)
}

fn validate_hex(value: &str) -> Result<()> {
    if parse_theme_color(value).is_none() {
        bail!("invalid theme color {value}");
    }
    Ok(())
}

pub fn parse_theme_color(value: &str) -> Option<Hsla> {
    let digits = value.strip_prefix('#')?;
    if !matches!(digits.len(), 6 | 8) || !digits.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return None;
    }
    let value = u32::from_str_radix(digits, 16).ok()?;
    Some(if digits.len() == 6 {
        rgb(value).into()
    } else {
        rgba(value).into()
    })
}

#[derive(Clone, Debug)]
pub struct ValidatedThemeImage {
    pub format: GpuiImageFormat,
    pub bytes: Vec<u8>,
}

fn canonical_local_asset(root: &Path, asset: &str) -> Result<PathBuf> {
    if asset.contains("://") || asset.contains('\0') {
        bail!("theme assets must be local files");
    }
    let candidate = root.join(asset);
    let canonical_root = root.canonicalize().context("canonicalizing theme root")?;
    let canonical = candidate
        .canonicalize()
        .with_context(|| format!("canonicalizing theme asset {}", candidate.display()))?;
    if !canonical.starts_with(&canonical_root) || !canonical.is_file() {
        bail!("theme asset escapes its pack");
    }
    Ok(canonical)
}

fn load_validated_local_asset(root: &Path, asset: &str) -> Result<ValidatedThemeImage> {
    let canonical = canonical_local_asset(root, asset)?;
    let metadata = canonical
        .metadata()
        .context("reading theme asset metadata")?;
    if metadata.len() > MAX_THEME_IMAGE_BYTES {
        bail!("theme asset exceeds 32 MiB");
    }
    let bytes = fs::read(&canonical).context("reading theme image bytes")?;
    let decoded_format = image::guess_format(&bytes).context("detecting theme image MIME")?;
    let extension = canonical
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    let format = match decoded_format {
        DecodedImageFormat::Png if extension == "png" => GpuiImageFormat::Png,
        DecodedImageFormat::Jpeg if matches!(extension.as_str(), "jpg" | "jpeg") => {
            GpuiImageFormat::Jpeg
        }
        DecodedImageFormat::Png | DecodedImageFormat::Jpeg => {
            bail!("theme image extension does not match decoded MIME")
        }
        _ => bail!("theme images must be static PNG or JPEG files"),
    };
    let reader = ImageReader::with_format(Cursor::new(&bytes), decoded_format);
    let (width, height) = reader
        .into_dimensions()
        .context("reading theme image dimensions")?;
    let pixels = u64::from(width).saturating_mul(u64::from(height));
    if width == 0
        || height == 0
        || width > MAX_THEME_IMAGE_DIMENSION
        || height > MAX_THEME_IMAGE_DIMENSION
        || pixels > MAX_THEME_IMAGE_PIXELS
    {
        bail!("theme image dimensions exceed safe limits");
    }
    image::load_from_memory_with_format(&bytes, decoded_format).context("decoding theme image")?;
    Ok(ValidatedThemeImage { format, bytes })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MotionPreference {
    #[default]
    Full,
    Economy,
    Reduced,
    Off,
}

impl MotionPreference {
    pub const fn frames_per_second(self) -> u32 {
        match self {
            Self::Full => 30,
            Self::Economy => 15,
            Self::Reduced | Self::Off => 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BackgroundPreferencesV1 {
    pub global_theme_id: String,
    pub per_canvas_override: Option<String>,
    pub motion: MotionPreference,
    pub dim: f32,
    pub intensity_override: Option<f32>,
}

impl Default for BackgroundPreferencesV1 {
    fn default() -> Self {
        Self {
            global_theme_id: DEFAULT_THEME_ID.to_owned(),
            per_canvas_override: None,
            motion: MotionPreference::Full,
            dim: 0.12,
            intensity_override: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ThemeCatalog {
    themes: BTreeMap<String, VisualThemeV1>,
    pack_roots: BTreeMap<String, PathBuf>,
    fingerprints: BTreeMap<String, u64>,
}

impl ThemeCatalog {
    pub fn load(root: &Path) -> Self {
        let mut catalog = Self::builtins();
        catalog.load_pack_directory(root, false);
        catalog
    }

    /// Return the compiled-in fallback catalog without touching user files.
    ///
    /// Product startup uses this synchronously so Graphite and curated themes
    /// are immediately available; user pack validation and image decoding run
    /// on the blocking runtime before replacing this catalog.
    pub fn builtins() -> Self {
        let mut catalog = Self::default();
        let graphite = VisualThemeV1::graphite();
        catalog.fingerprints.insert(graphite.id.clone(), 0);
        catalog.themes.insert(graphite.id.clone(), graphite);
        catalog.load_bundled_static_themes();
        catalog
    }

    fn load_bundled_static_themes(&mut self) {
        for (pack_name, source) in BUNDLED_STATIC_THEMES {
            let pack_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("assets/themes")
                .join(pack_name);
            let Ok(theme) = toml::from_str::<VisualThemeV1>(source) else {
                continue;
            };
            if self.themes.contains_key(&theme.id)
                || theme.validate(&pack_root).is_err()
                || matches!(&theme.background, BackgroundSpecV1::Image { .. })
            {
                continue;
            }
            let fingerprint = fingerprint_bytes(0xcbf2_9ce4_8422_2325, source.as_bytes());
            self.fingerprints.insert(theme.id.clone(), fingerprint);
            self.themes.insert(theme.id.clone(), theme);
        }
    }

    fn load_pack_directory(&mut self, root: &Path, allow_shared: bool) {
        for pack_root in theme_pack_directories(root) {
            let manifest = pack_root.join("theme.toml");
            let Ok(source) = read_theme_source(&manifest) else {
                continue;
            };
            let Ok(mut theme) = toml::from_str::<VisualThemeV1>(&source) else {
                continue;
            };
            if self.themes.contains_key(&theme.id) || theme.validate(&pack_root).is_err() {
                continue;
            }
            theme.shareable &= allow_shared;
            let fingerprint = theme_pack_fingerprint(&source, &pack_root, &theme);
            self.fingerprints.insert(theme.id.clone(), fingerprint);
            self.pack_roots.insert(theme.id.clone(), pack_root);
            self.themes.insert(theme.id.clone(), theme);
        }
    }

    pub fn get(&self, id: &str) -> &VisualThemeV1 {
        self.themes
            .get(id)
            .or_else(|| self.themes.get(DEFAULT_THEME_ID))
            .expect("graphite theme always exists")
    }

    pub fn resolve_id(&self, personal_override: Option<&str>, shared_theme: Option<&str>) -> &str {
        for candidate in [personal_override, shared_theme].into_iter().flatten() {
            if let Some((id, _)) = self.themes.get_key_value(candidate) {
                return id;
            }
        }
        DEFAULT_THEME_ID
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &VisualThemeV1)> {
        self.themes.iter().map(|(id, theme)| (id.as_str(), theme))
    }

    pub fn contains(&self, id: &str) -> bool {
        self.themes.contains_key(id)
    }

    pub fn load_image(&self, theme_id: &str) -> Result<Option<ValidatedThemeImage>> {
        let resolved_id = self.resolve_id(Some(theme_id), None);
        let theme = self.get(resolved_id);
        let BackgroundSpecV1::Image { asset, .. } = &theme.background else {
            return Ok(None);
        };
        let root = self
            .pack_roots
            .get(resolved_id)
            .context("image theme has no canonical pack root")?;
        load_validated_local_asset(root, asset).map(Some)
    }

    pub fn root() -> PathBuf {
        mitsuro_core::paths::config_dir().join("themes")
    }

    pub fn watch_fingerprint(root: &Path) -> u64 {
        directory_watch_fingerprint(0xcbf2_9ce4_8422_2325, root)
    }
}

fn theme_pack_directories(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let mut directories = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            fs::symlink_metadata(path)
                .map(|metadata| metadata.file_type().is_dir())
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    directories.sort();
    directories.truncate(MAX_THEME_PACKS);
    directories
}

fn read_theme_source(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path).context("reading theme manifest metadata")?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_THEME_MANIFEST_BYTES {
        bail!("theme manifest exceeds safe limits");
    }
    fs::read_to_string(path).context("reading theme manifest")
}

fn directory_watch_fingerprint(mut fingerprint: u64, root: &Path) -> u64 {
    for pack_root in theme_pack_directories(root) {
        fingerprint = fingerprint_bytes(fingerprint, pack_root.as_os_str().as_encoded_bytes());
        let manifest = pack_root.join("theme.toml");
        let Ok(source) = read_theme_source(&manifest) else {
            continue;
        };
        fingerprint = fingerprint_bytes(fingerprint, source.as_bytes());
        let Ok(theme) = toml::from_str::<VisualThemeV1>(&source) else {
            continue;
        };
        let BackgroundSpecV1::Image { asset, .. } = theme.background else {
            continue;
        };
        let Ok(asset) = canonical_local_asset(&pack_root, &asset) else {
            continue;
        };
        let Ok(metadata) = asset.metadata() else {
            continue;
        };
        fingerprint = fingerprint_bytes(fingerprint, asset.as_os_str().as_encoded_bytes());
        fingerprint = fingerprint_bytes(fingerprint, &metadata.len().to_le_bytes());
        if let Ok(modified) = metadata.modified() {
            if let Ok(elapsed) = modified.duration_since(UNIX_EPOCH) {
                fingerprint = fingerprint_bytes(fingerprint, &elapsed.as_nanos().to_le_bytes());
            }
        }
    }
    fingerprint
}

fn fingerprint_bytes(mut fingerprint: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        fingerprint ^= u64::from(*byte);
        fingerprint = fingerprint.wrapping_mul(0x100_0000_01b3);
    }
    fingerprint
}

fn theme_pack_fingerprint(source: &str, root: &Path, theme: &VisualThemeV1) -> u64 {
    let mut fingerprint = fingerprint_bytes(0xcbf2_9ce4_8422_2325, source.as_bytes());
    if let BackgroundSpecV1::Image { asset, .. } = &theme.background {
        if let Ok(path) = canonical_local_asset(root, asset) {
            fingerprint = fingerprint_bytes(fingerprint, path.as_os_str().as_encoded_bytes());
            if let Ok(metadata) = path.metadata() {
                fingerprint = fingerprint_bytes(fingerprint, &metadata.len().to_le_bytes());
                if let Ok(modified) = metadata.modified() {
                    if let Ok(elapsed) = modified.duration_since(UNIX_EPOCH) {
                        fingerprint =
                            fingerprint_bytes(fingerprint, &elapsed.as_nanos().to_le_bytes());
                    }
                }
            }
        }
    }
    fingerprint
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_graphite_is_valid() {
        let temp = tempfile::tempdir().expect("temp");
        VisualThemeV1::graphite()
            .validate(temp.path())
            .expect("graphite");
        let catalog = ThemeCatalog::load(&temp.path().join("no-user-themes"));
        assert!(catalog.contains(DEFAULT_THEME_ID));
        assert!(catalog.contains("quiet-ink"));
        assert!(catalog.contains("violet-dusk"));
        assert!(catalog.get("quiet-ink").shareable);
    }

    #[test]
    fn theme_rejects_remote_images_and_unknown_effects() {
        let temp = tempfile::tempdir().expect("temp");
        let mut theme = VisualThemeV1::graphite();
        theme.background = BackgroundSpecV1::Image {
            asset: "https://example.test/wallpaper.png".to_owned(),
            opacity: 1.0,
            scrim: None,
        };
        assert!(theme.validate(temp.path()).is_err());

        theme.background = BackgroundSpecV1::NativeEffect {
            effect: "downloaded_shader".to_owned(),
            seed: 0,
            intensity: 0.5,
            speed: 0.5,
            parallax: 0.0,
            fallback: StaticBackgroundV1::Solid {
                color: "#0e0e11".to_owned(),
            },
        };
        assert!(theme.validate(temp.path()).is_err());
    }

    #[test]
    fn reduced_motion_schedules_no_frames() {
        assert_eq!(MotionPreference::Reduced.frames_per_second(), 0);
        assert_eq!(MotionPreference::Off.frames_per_second(), 0);
    }

    #[test]
    fn catalog_resolves_personal_then_shared_then_graphite() {
        let temp = tempfile::tempdir().expect("temp");
        let pack = temp.path().join("custom");
        fs::create_dir_all(&pack).expect("pack");
        let mut custom = VisualThemeV1::graphite();
        custom.id = "custom".to_owned();
        custom.display_name = "Custom".to_owned();
        custom.background = BackgroundSpecV1::Solid {
            color: "#121217".to_owned(),
        };
        fs::write(
            pack.join("theme.toml"),
            toml::to_string(&custom).expect("theme source"),
        )
        .expect("write theme");
        let catalog = ThemeCatalog::load(temp.path());
        let watch_fingerprint = ThemeCatalog::watch_fingerprint(temp.path());

        assert_eq!(
            catalog.resolve_id(Some("custom"), Some(DEFAULT_THEME_ID)),
            "custom"
        );
        assert_eq!(catalog.resolve_id(None, Some("custom")), "custom");
        assert!(!catalog.get("custom").shareable);
        assert_eq!(
            catalog.resolve_id(Some("missing"), Some("also-missing")),
            DEFAULT_THEME_ID
        );

        let unchanged = ThemeCatalog::load(temp.path());
        assert_eq!(catalog, unchanged);
        assert_eq!(
            watch_fingerprint,
            ThemeCatalog::watch_fingerprint(temp.path())
        );
        custom.colors.pulse = "#ba8cff".to_owned();
        fs::write(
            pack.join("theme.toml"),
            toml::to_string(&custom).expect("updated theme source"),
        )
        .expect("update theme");
        assert_ne!(catalog, ThemeCatalog::load(temp.path()));
        assert_ne!(
            watch_fingerprint,
            ThemeCatalog::watch_fingerprint(temp.path())
        );
    }

    #[test]
    fn image_theme_requires_matching_mime_and_safe_dimensions() {
        let temp = tempfile::tempdir().expect("temp");
        let image_path = temp.path().join("wall.png");
        image::DynamicImage::new_rgba8(4, 3)
            .save_with_format(&image_path, DecodedImageFormat::Png)
            .expect("write png");
        let mut theme = VisualThemeV1::graphite();
        theme.id = "image-theme".to_owned();
        theme.background = BackgroundSpecV1::Image {
            asset: "wall.png".to_owned(),
            opacity: 0.8,
            scrim: Some("#00000066".to_owned()),
        };
        theme.validate(temp.path()).expect("valid image theme");
        let loaded = load_validated_local_asset(temp.path(), "wall.png").expect("load image");
        assert_eq!(loaded.format, GpuiImageFormat::Png);
        assert!(!loaded.bytes.is_empty());

        fs::rename(&image_path, temp.path().join("wall.jpg")).expect("rename");
        theme.background = BackgroundSpecV1::Image {
            asset: "wall.jpg".to_owned(),
            opacity: 1.0,
            scrim: None,
        };
        assert!(theme.validate(temp.path()).is_err());
    }

    #[test]
    fn image_theme_rejects_oversized_dimensions() {
        let temp = tempfile::tempdir().expect("temp");
        let image_path = temp.path().join("wide.png");
        image::DynamicImage::new_rgba8(MAX_THEME_IMAGE_DIMENSION + 1, 1)
            .save_with_format(&image_path, DecodedImageFormat::Png)
            .expect("write png");
        assert!(load_validated_local_asset(temp.path(), "wide.png").is_err());
    }
}
