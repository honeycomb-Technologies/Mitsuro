//! User-selectable motion policy.

use crate::tui_v2::model::capability::{CapabilityProfile, ColorDepth};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MotionPreference {
    Full,
    Reduced,
    Off,
}

impl MotionPreference {
    /// Low-capability terminals begin in Reduced mode. This is a default, not a
    /// forced override of an explicit user preference.
    pub const fn default_for(capability: CapabilityProfile) -> Self {
        if matches!(
            capability.color_depth,
            ColorDepth::Ansi16 | ColorDepth::Monochrome
        ) {
            Self::Reduced
        } else {
            Self::Full
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tui_v2::model::capability::{ColorDepth, GlyphMode};

    use super::*;

    #[test]
    fn low_color_depth_defaults_to_reduced_motion() {
        let capability = CapabilityProfile {
            glyph_mode: GlyphMode::Ascii,
            color_depth: ColorDepth::Ansi16,
        };

        assert_eq!(
            MotionPreference::default_for(capability),
            MotionPreference::Reduced
        );
    }
}
