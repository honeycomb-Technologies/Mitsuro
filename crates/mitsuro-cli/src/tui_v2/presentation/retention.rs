//! Historical artifact retention policy.

use crate::tui_v2::{
    model::{
        artifact::{
            ArtifactContent, ArtifactModel, ArtifactWarning, RetentionLevel, WebDocumentArtifact,
        },
        conversation::{ConversationPresentation, TimelinePart},
    },
    projection::tool_output::bound_text,
};

const DEFAULT_FULL_TURNS: usize = 12;
const SUMMARY_AGE: usize = 100;
const PREVIEW_BYTES: usize = 8 * 1024;
const SUMMARY_BYTES: usize = 2 * 1024;

pub fn apply_historical_retention(presentation: &mut ConversationPresentation) {
    apply_historical_retention_with_window(presentation, DEFAULT_FULL_TURNS);
}

pub fn apply_historical_retention_with_window(
    presentation: &mut ConversationPresentation,
    full_turns: usize,
) {
    let total = presentation.turns.len();
    for (index, turn) in presentation.turns.iter_mut().enumerate() {
        let age = total.saturating_sub(index + 1);
        let level = if age < full_turns {
            RetentionLevel::Full
        } else if age >= SUMMARY_AGE {
            RetentionLevel::Summary
        } else {
            RetentionLevel::Preview
        };

        for part in &mut turn.parts {
            match part {
                TimelinePart::Tool(tool) => retain_artifact(&mut tool.artifact, level),
                TimelinePart::Notice(notice) => {
                    if let Some(artifact) = &mut notice.expandable {
                        retain_artifact(artifact, level);
                    }
                }
                _ => {}
            }
        }
    }
}

fn retain_artifact(artifact: &mut ArtifactModel, level: RetentionLevel) {
    artifact.retention = level;
    let limit = match level {
        RetentionLevel::Full => return,
        RetentionLevel::Preview => PREVIEW_BYTES,
        RetentionLevel::Summary => SUMMARY_BYTES,
    };

    match &mut artifact.content {
        ArtifactContent::Text(text) => {
            let previous_omitted = text.omitted_bytes;
            *text = bound_text(&text.text, limit);
            text.omitted_bytes = text.omitted_bytes.saturating_add(previous_omitted);
        }
        ArtifactContent::Fields(fields) => {
            let limit = if matches!(level, RetentionLevel::Summary) {
                8
            } else {
                32
            };
            fields.truncate(limit);
        }
        ArtifactContent::WebResults(results) => {
            let limit = if matches!(level, RetentionLevel::Summary) {
                5
            } else {
                20
            };
            results.truncate(limit);
        }
        ArtifactContent::WebDocument(document) => {
            let WebDocumentArtifact { content, .. } = document;
            let previous_omitted = content.omitted_bytes;
            *content = bound_text(&content.text, limit);
            content.omitted_bytes = content.omitted_bytes.saturating_add(previous_omitted);
        }
        ArtifactContent::Empty => {}
    }

    artifact.warning.get_or_insert_with(|| ArtifactWarning {
        message: "Older output collapsed; the conversation history remains canonical".to_owned(),
    });
}

#[cfg(test)]
mod tests {
    use crate::tui_v2::model::{
        artifact::{ArtifactModel, BoundedText, PartId},
        conversation::{
            ConversationTurn, TimelinePart, ToolArguments, ToolPart, ToolStatus, TurnId,
        },
    };

    use super::*;

    #[test]
    fn older_turns_shrink_presentation_weight_without_removing_rows() {
        let mut presentation = ConversationPresentation::default();
        for index in 0..120 {
            let mut turn = ConversationTurn::new(TurnId::derived(index));
            turn.parts.push(TimelinePart::Tool(ToolPart {
                id: PartId::from_semantic(format!("tool:{index}")),
                tool_call_id: index.to_string(),
                name: "bash".to_owned(),
                status: ToolStatus::Succeeded,
                arguments: ToolArguments::default(),
                artifact: ArtifactModel {
                    content: ArtifactContent::Text(BoundedText {
                        text: "x".repeat(20_000),
                        omitted_bytes: 0,
                    }),
                    ..ArtifactModel::default()
                },
                server_side: false,
            }));
            presentation.turns.push(turn);
        }

        apply_historical_retention(&mut presentation);

        assert_eq!(presentation.turns.len(), 120);
        let TimelinePart::Tool(oldest) = &presentation.turns[0].parts[0] else {
            panic!("tool");
        };
        let TimelinePart::Tool(newest) = &presentation.turns[119].parts[0] else {
            panic!("tool");
        };
        assert_eq!(oldest.artifact.retention, RetentionLevel::Summary);
        assert_eq!(newest.artifact.retention, RetentionLevel::Full);
        assert!(matches!(
            &oldest.artifact.content,
            ArtifactContent::Text(text) if text.text.len() <= SUMMARY_BYTES
        ));
        assert!(matches!(
            &newest.artifact.content,
            ArtifactContent::Text(text) if text.text.len() == 20_000
        ));
    }
}
