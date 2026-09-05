use std::collections::HashSet;

use crate::ai::client::{AiClient, CallOptions};
use crate::ai::types::AiTool;
use crate::storage::WorkMode;
use crate::tools::registry::{PermissionMode, ToolRegistry};

use super::super::run_spec::apply_execution_tool_allowlist;

/// Immutable catalog and request controls used to rebuild the provider-facing
/// Code tool surface when the effective work mode changes during a run.
///
/// The catalog is frozen once at run start. Mode transitions may therefore
/// change which governed schemas are advertised without picking up unrelated
/// registry mutations halfway through a provider conversation.
pub(super) struct ModeAwareToolSurface {
    catalog: Option<Vec<AiTool>>,
    parallel_tool_calls: bool,
    parallel_tool_calls_override: Option<bool>,
}

impl ModeAwareToolSurface {
    pub(super) async fn capture(
        enabled: bool,
        options: &CallOptions,
        tool_registry: &ToolRegistry,
    ) -> Self {
        let catalog = if enabled {
            Some(tool_registry.get_ai_tools_all().await)
        } else {
            None
        };
        Self {
            catalog,
            parallel_tool_calls: options.codex_parallel_tool_calls,
            parallel_tool_calls_override: options.parallel_tool_calls,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn refresh(
        &self,
        options: &mut CallOptions,
        advertised_tool_names: &mut HashSet<String>,
        ai_client: &AiClient,
        permission_mode: PermissionMode,
        work_mode: WorkMode,
        has_active_plan: bool,
        disabled_tools: &[String],
        execution_tool_allowlist: Option<&HashSet<String>>,
    ) {
        let Some(catalog) = self.catalog.as_ref() else {
            *advertised_tool_names = advertised_names(options);
            return;
        };

        let tools = crate::tools::registry::filter_code_tools_for_mode(
            catalog.clone(),
            permission_mode,
            work_mode,
            has_active_plan,
            disabled_tools,
            ai_client.provider_id(),
            &ai_client.config().model,
        );
        options.tools = (!tools.is_empty()).then_some(tools);
        options.codex_parallel_tool_calls =
            self.parallel_tool_calls && options.tools.as_ref().is_some_and(|tools| tools.len() > 1);
        options.parallel_tool_calls = if options.tools.as_ref().is_some_and(|tools| tools.len() > 1)
        {
            self.parallel_tool_calls_override
        } else {
            Some(false)
        };
        apply_execution_tool_allowlist(options, execution_tool_allowlist);
        *options = ai_client.canonical_call_options(&ai_client.config().model, options);
        *advertised_tool_names = advertised_names(options);
    }
}

pub(super) fn advertised_names(options: &CallOptions) -> HashSet<String> {
    options
        .tools
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|tool| tool.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::ai::client::AiClientConfig;
    use crate::ai::models::ApiFormat;
    use crate::ai::providers::ProviderId;

    fn tool(name: &str) -> AiTool {
        AiTool {
            name: name.to_string(),
            description: name.to_string(),
            input_schema: json!({"type": "object"}),
            prompt: None,
        }
    }

    fn surface(names: &[&str]) -> ModeAwareToolSurface {
        ModeAwareToolSurface {
            catalog: Some(names.iter().map(|name| tool(name)).collect()),
            parallel_tool_calls: true,
            parallel_tool_calls_override: None,
        }
    }

    fn client(provider_id: ProviderId, model: &str) -> AiClient {
        AiClient::new(
            AiClientConfig {
                provider_id,
                model: model.to_string(),
                api_format: ApiFormat::OpenAI,
                ..Default::default()
            },
            String::new(),
        )
    }

    fn names(options: &CallOptions) -> HashSet<String> {
        advertised_names(options)
    }

    #[test]
    fn plan_to_build_refreshes_provider_family_mutation_schema() {
        let catalog = [
            "AskUserQuestion",
            "apply_patch",
            "bash",
            "edit",
            "read",
            "set_work_mode",
            "tool_search",
            "write",
        ];
        let surface = surface(&catalog);
        let grok = client(ProviderId::Grok, "grok-4.6");
        let mut options = CallOptions {
            tools: Some(vec![tool("read"), tool("set_work_mode")]),
            codex_parallel_tool_calls: true,
            ..Default::default()
        };
        let mut advertised = HashSet::new();

        surface.refresh(
            &mut options,
            &mut advertised,
            &grok,
            PermissionMode::Autonomous,
            WorkMode::Build,
            false,
            &[],
            None,
        );

        assert!(advertised.contains("edit"));
        assert!(advertised.contains("write"));
        assert!(!advertised.contains("apply_patch"));
        assert!(!advertised.contains("set_work_mode"));
    }

    #[test]
    fn build_to_plan_removes_mutations_and_preserves_exact_scope_and_disables() {
        let catalog = [
            "AskUserQuestion",
            "apply_patch",
            "bash",
            "grep",
            "read",
            "set_work_mode",
            "tool_search",
            "workflow_propose",
        ];
        let surface = surface(&catalog);
        let openai = client(ProviderId::OpenAI, "gpt-5.3-codex");
        let mut options = CallOptions {
            tools: Some(catalog.iter().map(|name| tool(name)).collect()),
            codex_parallel_tool_calls: true,
            ..Default::default()
        };
        let mut advertised = HashSet::new();
        let exact = HashSet::from([
            "read".to_string(),
            "apply_patch".to_string(),
            "workflow_propose".to_string(),
        ]);

        surface.refresh(
            &mut options,
            &mut advertised,
            &openai,
            PermissionMode::Supervised,
            WorkMode::Plan,
            false,
            &["read".to_string()],
            Some(&exact),
        );

        assert_eq!(advertised, HashSet::from(["workflow_propose".to_string()]));
        assert!(!options.codex_parallel_tool_calls);
    }

    #[test]
    fn disabled_surface_never_expands_a_caller_subset() {
        let surface = ModeAwareToolSurface {
            catalog: None,
            parallel_tool_calls: true,
            parallel_tool_calls_override: None,
        };
        let client = client(ProviderId::Grok, "grok-4.5");
        let mut options = CallOptions {
            tools: Some(vec![tool("read")]),
            ..Default::default()
        };
        let mut advertised = HashSet::from(["stale".to_string()]);

        surface.refresh(
            &mut options,
            &mut advertised,
            &client,
            PermissionMode::Autonomous,
            WorkMode::Build,
            false,
            &[],
            None,
        );

        assert_eq!(advertised, HashSet::from(["read".to_string()]));
        assert_eq!(names(&options), HashSet::from(["read".to_string()]));
    }

    #[test]
    fn parallel_override_survives_mode_refresh_with_multiple_tools() {
        let mut surface = surface(&["read", "grep", "edit", "write"]);
        surface.parallel_tool_calls_override = Some(false);
        let client = client(ProviderId::Grok, "grok-4.6");
        let mut options = CallOptions {
            codex_parallel_tool_calls: true,
            parallel_tool_calls: Some(false),
            ..Default::default()
        };
        let mut advertised = HashSet::new();
        for permission in [PermissionMode::Supervised, PermissionMode::Autonomous] {
            surface.refresh(
                &mut options,
                &mut advertised,
                &client,
                permission,
                WorkMode::Build,
                false,
                &[],
                None,
            );
            assert!(options.tools.as_ref().is_some_and(|tools| tools.len() > 1));
            assert_eq!(options.parallel_tool_calls, Some(false));
        }
    }
}
