use crate::ai::models::ApiFormat;
use crate::ai::providers::ProviderId;
use crate::ai::types::{Content, ModelMessage, Role};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::context_kind::ContextKind;
use super::profile::ModelProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSectionKind {
    AgentContract,
    Identity,
    Project,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptStability {
    Stable,
    Dynamic,
}

/// One typed, inspectable model-visible instruction layer.
///
/// Provider adapters still receive the cache-oriented aggregate strings below,
/// but prompt construction and diagnostics no longer have to reverse-engineer
/// an opaque concatenated system prompt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSection {
    pub kind: PromptSectionKind,
    pub stability: PromptStability,
    pub source: String,
    pub content: String,
    pub sha256: String,
    pub bytes: usize,
}

impl PromptSection {
    fn new(
        kind: PromptSectionKind,
        stability: PromptStability,
        source: impl Into<String>,
        content: String,
    ) -> Option<Self> {
        if content.trim().is_empty() {
            return None;
        }
        let digest = Sha256::digest(content.as_bytes());
        Some(Self {
            kind,
            stability,
            source: source.into(),
            bytes: content.len(),
            sha256: format!("{digest:x}"),
            content,
        })
    }
}

pub struct SystemPromptSections {
    pub profile: ModelProfile,
    pub sections: Vec<PromptSection>,
    pub base_prompt: String,
    /// Hive coordinator/persona/user context frozen for the current run.
    /// This is stable within a run and belongs in the cached prefix.
    pub identity_context: String,
    pub project_context: String,
    pub session_context: String,
}

impl SystemPromptSections {
    pub fn combined(&self) -> String {
        let mut sections = Vec::new();

        if !self.base_prompt.is_empty() {
            sections.push(self.base_prompt.as_str());
        }
        if !self.identity_context.is_empty() {
            sections.push(self.identity_context.as_str());
        }
        if !self.project_context.is_empty() {
            sections.push(self.project_context.as_str());
        }
        if !self.session_context.is_empty() {
            sections.push(self.session_context.as_str());
        }

        sections.join("\n\n---\n\n")
    }

    /// Stable fingerprint for the exact model-visible instruction stack.
    pub fn prompt_hash(&self) -> String {
        let digest = Sha256::digest(self.combined().as_bytes());
        format!("{digest:x}")
    }

    /// Redacted diagnostics retain ordering, provenance, stability, size and
    /// hashes while omitting prompt contents that may contain user data.
    pub fn diagnostic_manifest(&self) -> serde_json::Value {
        serde_json::json!({
            "prompt_hash": self.prompt_hash(),
            "total_bytes": self.combined().len(),
            "sections": self.sections.iter().map(|section| serde_json::json!({
                "kind": section.kind,
                "stability": section.stability,
                "source": section.source,
                "bytes": section.bytes,
                "sha256": section.sha256,
            })).collect::<Vec<_>>(),
        })
    }
}

pub fn build_system_prompt_sections(
    provider: ProviderId,
    api_format: ApiFormat,
    model_id: &str,
    messages: &[ModelMessage],
    custom_system_prompt: Option<&str>,
    _tool_prompts: &[(String, String)],
) -> SystemPromptSections {
    let profile = ModelProfile::resolve(provider, api_format, model_id);
    let base = profile.layered_system_prompt(provider, api_format, model_id, custom_system_prompt);
    let (identity_context, project_context, session_context) =
        partition_system_messages_with_base(messages, Some(&base));

    let mut sections = Vec::new();
    if let Some(section) = PromptSection::new(
        PromptSectionKind::AgentContract,
        PromptStability::Stable,
        format!("model_profile:{provider}:{model_id}"),
        base.clone(),
    ) {
        sections.push(section);
    }
    if let Some(section) = PromptSection::new(
        PromptSectionKind::Identity,
        PromptStability::Stable,
        "runtime_identity",
        identity_context.clone(),
    ) {
        sections.push(section);
    }
    if let Some(section) = PromptSection::new(
        PromptSectionKind::Project,
        PromptStability::Stable,
        "project_context",
        project_context.clone(),
    ) {
        sections.push(section);
    }
    if let Some(section) = PromptSection::new(
        PromptSectionKind::Session,
        PromptStability::Dynamic,
        "session_context",
        session_context.clone(),
    ) {
        sections.push(section);
    }

    SystemPromptSections {
        profile,
        sections,
        base_prompt: base,
        identity_context,
        project_context,
        session_context,
    }
}

pub fn partition_system_messages(messages: &[ModelMessage]) -> (String, String) {
    let (identity, project, session) = partition_system_messages_by_stability(messages);
    let session = [identity.trim(), session.trim()]
        .into_iter()
        .filter(|section| !section.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    (project, session)
}

fn partition_system_messages_by_stability(messages: &[ModelMessage]) -> (String, String, String) {
    partition_system_messages_with_base(messages, None)
}

fn partition_system_messages_with_base(
    messages: &[ModelMessage],
    base: Option<&str>,
) -> (String, String, String) {
    let mut identity_context = String::new();
    let mut project_context = String::new();
    let mut session_context = String::new();

    for message in messages.iter().filter(|m| m.role == Role::System) {
        for text in message.content.iter().filter_map(|block| match block {
            Content::Text { text, citations } => {
                Some(crate::ai::format::provider_text(text, citations))
            }
            _ => None,
        }) {
            // Hosted Hive uses the coordinator as an explicit custom base;
            // local callers also inject it as identity context. Keep that
            // existing base policy, but include an exact coordinator only once.
            // Never deduplicate arbitrary user/project instructions by substring.
            if text.starts_with("[HIVE COORDINATOR]")
                && base.is_some_and(|base| base.trim() == text.trim())
            {
                continue;
            }
            let kind = ContextKind::from_text(&text);
            let target = if kind.is_stable_identity() {
                &mut identity_context
            } else if kind.is_stable_project() {
                &mut project_context
            } else {
                &mut session_context
            };
            append_context(target, &text);
        }
    }

    (identity_context, project_context, session_context)
}

fn append_context(target: &mut String, text: &str) {
    if !target.is_empty() {
        target.push_str("\n\n");
    }
    target.push_str(text);
}

#[cfg(test)]
mod tests {
    use super::build_system_prompt_sections;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;

    #[test]
    fn tool_schema_is_the_only_tool_contract_in_the_system_prompt() {
        let sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.5-codex",
            &[],
            None,
            &[(
                "bash".to_string(),
                "LEGACY EXTENDED TOOL MANUAL MUST NOT BE INJECTED".to_string(),
            )],
        );

        assert!(!sections.base_prompt.contains("LEGACY EXTENDED TOOL MANUAL"));
        assert!(sections.base_prompt.len() <= 5_000);
        assert_eq!(sections.sections.len(), 1);
        assert_eq!(sections.prompt_hash().len(), 64);
        let manifest = sections.diagnostic_manifest();
        assert_eq!(manifest["sections"][0]["content"], serde_json::Value::Null);
        assert_eq!(
            manifest["sections"][0]["sha256"].as_str().unwrap().len(),
            64
        );
    }

    #[test]
    fn hive_identity_is_frozen_into_its_own_stable_section() {
        use crate::ai::types::{Content, ModelMessage, Role};

        let messages = vec![
            ModelMessage {
                role: Role::System,
                content: vec![Content::text(
                    "[HIVE SOUL - HIVE_SOUL.md]\nWarm and exact.".to_string(),
                )],
            },
            ModelMessage {
                role: Role::System,
                content: vec![Content::text(
                    "[HIVE HEARTBEAT - HIVE_HEARTBEAT.md]\nCheck work.".to_string(),
                )],
            },
        ];
        let sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.5-codex",
            &messages,
            None,
            &[],
        );

        assert!(sections.identity_context.contains("Warm and exact"));
        assert!(!sections.identity_context.contains("Check work"));
        assert!(sections.session_context.contains("Check work"));
    }

    #[test]
    fn hive_prompt_tiers_keep_persona_and_project_stable_but_operations_volatile() {
        use crate::ai::types::{Content, ModelMessage, Role};

        let system = |text: &str| ModelMessage {
            role: Role::System,
            content: vec![Content::text(text.to_string())],
        };
        let first = vec![
            system("[HIVE COORDINATOR]\nCoordinate deliberately."),
            system("[HIVE SOUL - profile:local]\nWarm and candid."),
            system("[HIVE IDENTITY - profile:local]\nName: Hive"),
            system("[HIVE USER - profile:local]\nUse concise updates."),
            system("[HIVE CREW SOUL - reviewer]\nEvidence first."),
            system("[PROJECT INSTRUCTIONS - AGENTS.md]\nValidate changes."),
            system("[HIVE PROJECT OVERLAY - HIVE.md]\nProject cadence."),
            system("[PROJECT SETTINGS]\nAutonomous mode."),
            system("[HIVE HEARTBEAT - snapshot-revision:4]\nCheck queue A."),
            system("[HIVE CHANNELS - snapshot-revision:4]\nMain thread."),
            system("[RECALLED CONVERSATION EPISODES]\nFallible evidence."),
        ];
        let mut second = first.clone();
        second[8] = system("[HIVE HEARTBEAT - snapshot-revision:5]\nCheck queue B.");
        second[9] = system("[HIVE CHANNELS - snapshot-revision:5]\nMobile push.");

        let first = build_system_prompt_sections(
            ProviderId::Anthropic,
            ApiFormat::Anthropic,
            "claude-sonnet-4-5",
            &first,
            None,
            &[],
        );
        let second = build_system_prompt_sections(
            ProviderId::Anthropic,
            ApiFormat::Anthropic,
            "claude-sonnet-4-5",
            &second,
            None,
            &[],
        );

        assert_eq!(first.identity_context, second.identity_context);
        assert_eq!(first.project_context, second.project_context);
        assert_ne!(first.session_context, second.session_context);
        for marker in [
            "HIVE COORDINATOR",
            "HIVE SOUL",
            "HIVE IDENTITY",
            "HIVE USER",
            "HIVE CREW SOUL",
        ] {
            assert!(first.identity_context.contains(marker));
        }
        assert!(first.project_context.contains("PROJECT INSTRUCTIONS"));
        assert!(first.project_context.contains("HIVE PROJECT OVERLAY"));
        assert!(first.project_context.contains("PROJECT SETTINGS"));
        assert!(first.session_context.contains("HIVE HEARTBEAT"));
        assert!(first.session_context.contains("HIVE CHANNELS"));
        assert!(first
            .session_context
            .contains("RECALLED CONVERSATION EPISODES"));
        assert!(!first.identity_context.contains("HIVE HEARTBEAT"));
        assert!(!first.project_context.contains("HIVE CHANNELS"));
    }
}

#[cfg(test)]
mod assembly_regressions {
    use super::*;

    #[test]
    fn every_system_text_block_is_retained_in_its_cache_tier() {
        let messages = [ModelMessage {
            role: Role::System,
            content: vec![
                Content::text("[PROJECT INSTRUCTIONS]\nroot contract".into()),
                Content::text("[DURABLE GOAL - exact]\ncurrent obligation".into()),
                Content::text("[HIVE SOUL]\nidentity contract".into()),
                Content::text("[HIVE WORKER]\nworker contract".into()),
            ],
        }];
        let sections = build_system_prompt_sections(
            ProviderId::OpenAI,
            ApiFormat::OpenAIResponses,
            "gpt-5.5",
            &messages,
            None,
            &[],
        );
        assert!(sections.project_context.contains("root contract"));
        assert!(sections.session_context.contains("current obligation"));
        assert!(sections.session_context.contains("worker contract"));
        assert!(sections.identity_context.contains("identity contract"));
        assert!(!sections.identity_context.contains("worker contract"));
        for marker in [
            "root contract",
            "current obligation",
            "identity contract",
            "worker contract",
        ] {
            assert_eq!(sections.combined().matches(marker).count(), 1);
        }
    }

    #[test]
    fn hosted_hive_keeps_its_explicit_base_without_a_second_coordinator() {
        let coordinator =
            crate::agent::autonomy::coordinator_prompt::hive_coordinator_system_prompt();
        let messages = [ModelMessage {
            role: Role::System,
            content: vec![
                Content::text(coordinator.clone()),
                Content::text("[HIVE SOUL]\nWarm and precise.".into()),
            ],
        }];
        let hosted = build_system_prompt_sections(
            ProviderId::Anthropic,
            ApiFormat::Anthropic,
            "claude-sonnet-4-5",
            &messages,
            Some(&coordinator),
            &[],
        );
        assert_eq!(hosted.base_prompt, coordinator.trim());
        assert_eq!(hosted.combined().matches("[HIVE COORDINATOR]").count(), 1);
        assert!(hosted.identity_context.contains("Warm and precise"));
        let local = build_system_prompt_sections(
            ProviderId::Anthropic,
            ApiFormat::Anthropic,
            "claude-sonnet-4-5",
            &messages,
            None,
            &[],
        );
        assert_eq!(local.combined().matches("[HIVE COORDINATOR]").count(), 1);
        assert!(local.base_prompt.contains("You are Agent inside Mitsuro"));
        assert!(local.identity_context.contains(&coordinator));
    }
}
