//! Stable transcript anchors expressed in semantic source offsets.

use crate::tui_v2::model::artifact::PartId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptAnchor {
    pub part_id: PartId,
    pub source_offset: usize,
    pub screen_row: u16,
}

impl TranscriptAnchor {
    pub fn new(part_id: PartId, source_offset: usize, screen_row: u16) -> Self {
        Self {
            part_id,
            source_offset,
            screen_row,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum AnchorMode {
    #[default]
    FollowLive,
    Fixed(TranscriptAnchor),
    ScrollTop(u32),
}
