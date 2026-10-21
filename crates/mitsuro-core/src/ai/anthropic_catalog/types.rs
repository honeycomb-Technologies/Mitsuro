use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(super) struct ModelsResponse {
    #[serde(default)]
    pub(super) data: Vec<AnthropicModel>,
    #[serde(default)]
    pub(super) last_id: Option<String>,
    #[serde(default)]
    pub(super) has_more: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct AnthropicModel {
    #[serde(default)]
    pub(super) id: String,
    #[serde(default)]
    pub(super) display_name: Option<String>,
    #[serde(default)]
    pub(super) max_input_tokens: Option<usize>,
    #[serde(default)]
    pub(super) max_tokens: Option<usize>,
    #[serde(default)]
    pub(super) capabilities: Option<Capabilities>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Capabilities {
    #[serde(default)]
    pub(super) effort: Option<EffortCapability>,
    #[serde(default)]
    pub(super) image_input: Option<SupportFlag>,
    #[serde(default)]
    pub(super) thinking: Option<ThinkingCapability>,
}

#[derive(Debug, Deserialize)]
pub(super) struct EffortCapability {
    #[serde(default)]
    pub(super) supported: Option<bool>,
    #[serde(default)]
    pub(super) low: Option<SupportFlag>,
    #[serde(default)]
    pub(super) medium: Option<SupportFlag>,
    #[serde(default)]
    pub(super) high: Option<SupportFlag>,
    #[serde(default, alias = "x_high")]
    pub(super) xhigh: Option<SupportFlag>,
    #[serde(default)]
    pub(super) max: Option<SupportFlag>,
}

impl EffortCapability {
    pub(super) fn has_explicit_value(&self) -> bool {
        self.supported.is_some()
            || self.low.as_ref().is_some_and(SupportFlag::is_explicit)
            || self.medium.as_ref().is_some_and(SupportFlag::is_explicit)
            || self.high.as_ref().is_some_and(SupportFlag::is_explicit)
            || self.xhigh.as_ref().is_some_and(SupportFlag::is_explicit)
            || self.max.as_ref().is_some_and(SupportFlag::is_explicit)
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ThinkingCapability {
    #[serde(default)]
    pub(super) supported: Option<bool>,
    #[serde(default)]
    pub(super) types: Option<ThinkingTypes>,
}

impl ThinkingCapability {
    pub(super) fn has_explicit_value(&self) -> bool {
        self.supported.is_some()
            || self
                .types
                .as_ref()
                .is_some_and(ThinkingTypes::has_explicit_value)
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ThinkingTypes {
    #[serde(default)]
    pub(super) adaptive: Option<SupportFlag>,
    #[serde(default)]
    pub(super) enabled: Option<SupportFlag>,
}

impl ThinkingTypes {
    fn has_explicit_value(&self) -> bool {
        self.adaptive.as_ref().is_some_and(SupportFlag::is_explicit)
            || self.enabled.as_ref().is_some_and(SupportFlag::is_explicit)
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct SupportFlag {
    #[serde(default)]
    pub(super) supported: Option<bool>,
}

impl SupportFlag {
    pub(super) fn is_explicit(&self) -> bool {
        self.supported.is_some()
    }

    pub(super) fn is_supported(&self) -> bool {
        self.supported == Some(true)
    }
}

impl crate::ai::catalog::CatalogPage for ModelsResponse {
    type Entry = AnthropicModel;

    fn entries(self) -> Vec<Self::Entry> {
        self.data
    }

    fn has_more(&self) -> bool {
        self.has_more
    }

    fn last_id(&self) -> Option<&str> {
        self.last_id.as_deref()
    }
}
