use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::ai::types::AiTool;
use crate::tools::registry::{DelegationPolicy, ToolContext, ToolRegistry, ToolResult};

use super::super::build_context::SharedBuildContext;
use super::super::tools::BuilderTools;
use super::super::types::{AgentProgress, SubAgentTask};

const READ_ONLY_CHILD_CONTRACT: &str = "\nThis child has no workspace-write authority. Shell execution is for inspection and requested validation only; do not run export, generate, install, clean, format, fix, sync, or similar project targets that intentionally modify workspace files. Request a write-capable child if such work is required.\n";

const DELEGATED_HANDOFF_CONTRACT: &str = r#"End with a short human summary followed by exactly one machine-readable handoff. The handoff must be the final block in your response:
<delegated_handoff>{"status":"complete|degraded|blocked|failed","summary":"truthful outcome","acceptance_checks":[{"id":"check name","status":"passed|failed|not_applicable","evidence":"bounded evidence"}],"remaining_work":[],"blockers":[],"generated_artifacts":[{"path":"relative/output/path","purpose":"why a dependent task or final product needs it"}]}</delegated_handoff>
Use complete only when the assigned objective and every required validation actually passed. Put unfinished work in remaining_work and use degraded or blocked rather than claiming success.
Declare generated_artifacts only for bounded deliverables that must survive an isolated workspace, such as a distributable web bundle. Use paths relative to the workspace root. Never declare dependency trees, compiler/package caches, temporary files, target, node_modules, .cargo-home, or .mitsuro. Use an empty list when source changes alone are sufficient.
"#;

/// Configuration trait for agent behavior. This abstracts explorer vs builder differences.
#[async_trait::async_trait]
pub(crate) trait AgentConfig: Send + Sync {
    /// Get system prompt (can be static or dynamic per turn).
    fn system_prompt(&self, turn: usize) -> String;

    /// Append-only runtime context. The loop persists a new tail message only
    /// when this value changes, preserving every previously cached prefix.
    fn dynamic_context(&self) -> Option<String> {
        None
    }

    /// Tool timeout in seconds.
    fn timeout_secs(&self) -> u64;

    /// Per-turn API call timeout.
    fn api_call_timeout(&self) -> Duration {
        crate::agent::constants::timeouts::EXPLORER_API_CALL
    }

    /// Max tokens for API calls.
    fn max_tokens(&self) -> usize;

    /// Get tool definitions for AI.
    fn get_ai_tools(&self) -> Vec<AiTool>;

    /// Execute a tool call.
    async fn execute_tool(
        &self,
        name: &str,
        params: Value,
        ctx: &ToolContext,
    ) -> Option<ToolResult>;

    /// Format action description for progress reporting.
    fn format_action(&self, tool_name: &str, params: &Value) -> String {
        match tool_name {
            "read" => {
                let path = params
                    .get("file_path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let short_path = path.rsplit('/').next().unwrap_or(path);
                format!("read {}", short_path)
            }
            "glob" => {
                let pattern = params
                    .get("pattern")
                    .and_then(|v| v.as_str())
                    .unwrap_or("*");
                format!("glob {}", pattern)
            }
            "grep" => {
                let pattern = params
                    .get("pattern")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let short = if pattern.len() > 12 {
                    &pattern[..12]
                } else {
                    pattern
                };
                format!("grep {}", short)
            }
            "write" | "edit" => {
                let path = params
                    .get("file_path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let short_path = path.rsplit('/').next().unwrap_or(path);
                format!("{} {}", tool_name, short_path)
            }
            "bash" => {
                let cmd = params
                    .get("command")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let short = if cmd.len() > 15 { &cmd[..15] } else { cmd };
                format!("bash {}", short)
            }
            _ => tool_name.to_string(),
        }
    }

    /// Update progress with agent-specific metadata (e.g., line counts for builders).
    fn update_progress(&self, progress: &mut AgentProgress);

    /// Cleanup on exit (e.g., release locks for builders).
    fn cleanup(&self);

    /// Check if a file was read (for tracking files examined).
    fn is_read_tool(&self, name: &str) -> bool {
        name == "read"
    }

    /// Whether to apply explorer-specific heuristics (forced reports, stale cycle detection).
    /// The legacy multi-agent pool ExplorerConfig returns true; the unified SingleChildConfig
    /// returns false since the capable model manages its own completion.
    fn use_explorer_heuristics(&self) -> bool {
        true
    }
}

/// Builder configuration - read-write, coordinated.
pub(crate) struct BuilderConfig {
    task: SubAgentTask,
    tools: BuilderTools,
    context: Arc<SharedBuildContext>,
}

impl BuilderConfig {
    pub fn new(task: SubAgentTask, context: Arc<SharedBuildContext>) -> Self {
        let task_id = task.id.clone();
        Self {
            task,
            tools: BuilderTools::new(context.clone(), task_id),
            context,
        }
    }
}

#[async_trait::async_trait]
impl AgentConfig for BuilderConfig {
    fn system_prompt(&self, _turn: usize) -> String {
        let mut prompt = builder_system_prompt(&self.task.working_dir);
        prompt.push('\n');
        prompt.push_str(&super::delegated_execution_context(
            &self.task,
            self.task.project_context.clone(),
        ));
        if self
            .task
            .delegation_policy
            .as_ref()
            .is_none_or(|policy| policy.read_only_only)
        {
            prompt.push_str(READ_ONLY_CHILD_CONTRACT);
        }
        if self
            .get_ai_tools()
            .iter()
            .any(|tool| tool.name == "browser_check")
        {
            prompt.push_str(
                "\n## Governed Browser Acceptance\n\
The browser_check tool is present and authorized for this delegated task. Use it directly for every browser_runtime, browser_keyboard, or browser_touch acceptance requirement. Do not substitute HTTP 200 checks, curl, screenshots, or an ungoverned browser installation.\n",
            );
        }
        prompt
    }

    fn dynamic_context(&self) -> Option<String> {
        let context = self.context.generate_context_injection();
        let body = if context.trim().is_empty() {
            "No active shared coordination metadata.".to_string()
        } else {
            context
        };
        Some(format!(
            "[COORDINATION UPDATE]\n{body}\n[/COORDINATION UPDATE]"
        ))
    }

    fn timeout_secs(&self) -> u64 {
        120
    }

    fn api_call_timeout(&self) -> Duration {
        crate::agent::constants::timeouts::BUILDER_API_CALL
    }

    fn max_tokens(&self) -> usize {
        16384
    }

    fn get_ai_tools(&self) -> Vec<AiTool> {
        let Some(policy) = self.task.delegation_policy.as_ref() else {
            // Execution also fails closed without delegated policy metadata;
            // keep the provider schema aligned with that runtime boundary.
            return Vec::new();
        };

        self.tools
            .get_ai_tools()
            .into_iter()
            .filter(|tool| policy.authorize_tool(&tool.name, false).is_ok())
            .collect()
    }

    async fn execute_tool(
        &self,
        name: &str,
        params: Value,
        ctx: &ToolContext,
    ) -> Option<ToolResult> {
        self.tools.execute(name, params, ctx).await
    }

    fn update_progress(&self, progress: &mut AgentProgress) {
        let (lines_added, lines_removed) = self.context.get_line_diff();
        progress.lines_added = lines_added;
        progress.lines_removed = lines_removed;
    }

    fn cleanup(&self) {
        self.context.release_all_locks(&self.task.id);
    }
}

/// Unified single-child config. The parent's instructions define the work;
/// the delegated policy defines the exact read/write/execute tool surface.
pub(crate) struct SingleChildConfig {
    policy: DelegationPolicy,
    registry: Arc<ToolRegistry>,
    ai_tools: Vec<AiTool>,
    project_context: String,
}

impl SingleChildConfig {
    pub async fn new(
        registry: Arc<ToolRegistry>,
        policy: DelegationPolicy,
        project_context: String,
    ) -> Self {
        let ai_tools = registry.get_ai_tools_filtered(&policy).await;
        Self {
            policy,
            registry,
            ai_tools,
            project_context,
        }
    }
}

#[async_trait::async_trait]
impl AgentConfig for SingleChildConfig {
    fn system_prompt(&self, _turn: usize) -> String {
        let mut prompt = String::from(
            "You are a delegated child agent of the parent Agent in Mitsuro. \
Complete only the task the parent assigned. You are not a fixed specialty — \
follow the parent instructions exactly.\n\n\
## Non-Negotiable Rules\n\
1. You are NOT the main agent. Do NOT address the end user directly.\n\
2. Do NOT spawn further sub-agents or call the agent tool.\n\
3. Stay within the parent's objective and any stated scope.\n\
4. Use tools for evidence. Prefer dedicated read/search tools over bash when possible.\n\
5. Once the assigned objective and required checks pass, return the handoff immediately; do not expand into downstream integration or extra polish.\n\
6. Honor capability limits: if write tools are unavailable, do not attempt edits.\n",
        );

        prompt.push_str("\n## Report\n");
        prompt.push_str(DELEGATED_HANDOFF_CONTRACT);

        if self.policy.read_only_only {
            prompt.push_str(READ_ONLY_CHILD_CONTRACT);
        }

        if self
            .ai_tools
            .iter()
            .any(|tool| tool.name == "browser_check")
        {
            prompt.push_str(
                "\n## Governed Browser Acceptance\n\
The browser_check tool is authorized for this delegated task. Use it directly for any browser_runtime, browser_keyboard, or browser_touch acceptance requirement. If it is not visible on the initial compact tool surface, retrieve it through tool_search by its exact name. Do not substitute HTTP 200 checks, curl, screenshots, or an ungoverned browser installation.\n",
            );
        }

        if !self.project_context.is_empty() {
            prompt.push('\n');
            prompt.push_str(&self.project_context);
            prompt.push('\n');
        }

        prompt
    }

    fn timeout_secs(&self) -> u64 {
        120
    }

    fn api_call_timeout(&self) -> Duration {
        crate::agent::constants::timeouts::SINGLE_EXPLORER_API_CALL
    }

    fn max_tokens(&self) -> usize {
        16384
    }

    fn get_ai_tools(&self) -> Vec<AiTool> {
        self.ai_tools.clone()
    }

    async fn execute_tool(
        &self,
        name: &str,
        params: Value,
        ctx: &ToolContext,
    ) -> Option<ToolResult> {
        if let Err(reason) = self
            .policy
            .authorize_tool_call(name, &params, ctx.plan_mode)
        {
            return Some(ToolResult::error(reason));
        }
        self.registry.execute(name, params, ctx).await
    }

    fn update_progress(&self, _progress: &mut AgentProgress) {}

    fn cleanup(&self) {}

    fn use_explorer_heuristics(&self) -> bool {
        false
    }
}

/// Generate write-capable child system prompt with context injection.
fn builder_system_prompt(working_dir: &std::path::Path) -> String {
    format!(
        r#"You are a delegated child agent of the parent Agent in Mitsuro.
Your inherited capability policy and available tool schemas define your read, write, and execution authority.
Complete only the task the parent assigned. You are not a fixed specialty — follow the parent instructions exactly.

## Working Directory
{}

## Non-Negotiable Rules
1. You are NOT the main agent. Do NOT address the end user directly.
2. Do NOT spawn further sub-agents or call the agent tool.
3. Stay within the parent's objective and any stated file/component scope.
4. ALWAYS read files before editing — other children may have modified them.
5. Prefer targeted edits over wholesale rewrites.
6. Do NOT claim completion if edits failed or required checks are broken.
7. Follow repository instructions (AGENTS.md / project conventions) and any [CONVENTIONS] from the parent.
8. If a file lock wait exceeds 30 seconds, skip that file and note it in your report.
9. Once the assigned objective and required checks pass, return the handoff immediately; do not expand into downstream integration or extra polish.

## Process
1. Locate relevant files (glob/grep/read)
2. Perform the assigned investigation or authorized changes
3. Complete the validation required by the assignment and repository instructions
4. Return a concise report to the parent

## Report
{}
"#,
        working_dir.display(),
        DELEGATED_HANDOFF_CONTRACT
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::register_all_tools;
    use crate::tools::registry::PermissionMode;
    use std::collections::HashSet;

    #[tokio::test]
    async fn single_and_graph_children_assemble_the_same_project_contract_at_system_authority() {
        use crate::agent::context::{
            build_subagent_project_context, build_subagent_project_instructions,
        };
        use crate::ai::model_profile::build_system_prompt_sections;
        use crate::ai::models::ApiFormat;
        use crate::ai::providers::ProviderId;

        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("source");
        let nested = project.join("component");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(project.join("AGENTS.md"), "SHARED-ROOT-CONTRACT").unwrap();
        std::fs::write(nested.join("AGENTS.md"), "CHILD-SCOPE-CONTRACT").unwrap();
        let instructions = build_subagent_project_instructions(&nested, Some(&project));
        let isolated = temp.path().join("isolated").join("component");
        let policy = DelegationPolicy::for_subagent_verify(PermissionMode::Autonomous, Some(7));
        let task = SubAgentTask::new("child", "Inspect the component")
            .with_working_dir(isolated.clone())
            .with_sandbox_root(temp.path().join("isolated"))
            .with_project_context(instructions.clone())
            .with_delegation_policy(policy.clone());
        assert!(!task.prompt.contains("SHARED-ROOT-CONTRACT"));
        let builder = BuilderConfig::new(task, Arc::new(SharedBuildContext::new()));
        let single = SingleChildConfig::new(
            Arc::new(ToolRegistry::new()),
            policy,
            build_subagent_project_context(&nested, Some(&project)),
        )
        .await;
        for prompt in [builder.system_prompt(1), single.system_prompt(1)] {
            let sections = build_system_prompt_sections(
                ProviderId::OpenAI,
                ApiFormat::OpenAIResponses,
                "gpt-5.5",
                &[],
                Some(&prompt),
                &[],
            );
            assert!(sections.base_prompt.contains("SHARED-ROOT-CONTRACT"));
            assert!(sections.base_prompt.contains("CHILD-SCOPE-CONTRACT"));
            assert!(sections
                .base_prompt
                .contains("no workspace-write authority"));
            assert!(sections.base_prompt.contains("<delegated_handoff>"));
            assert!(sections
                .base_prompt
                .contains("Do NOT spawn further sub-agents"));
        }
        let graph_prompt = builder.system_prompt(1);
        assert!(graph_prompt.contains(&format!(
            "Current working directory: {}",
            isolated.display()
        )));
        assert!(!graph_prompt.contains(&format!("Execution directory: {}", nested.display())));
    }

    #[test]
    fn builder_schema_intersects_with_exact_parent_execution_scope() {
        let scope = HashSet::from(["agent".to_string(), "read".to_string()]);
        let policy = DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(7))
            .with_execution_tool_allowlist(Some(&scope));
        let task = SubAgentTask::new("builder", "work").with_delegation_policy(policy);
        let config = BuilderConfig::new(task, Arc::new(SharedBuildContext::new()));

        let names = config
            .get_ai_tools()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["read"]);
        assert!(!names.iter().any(|name| {
            matches!(
                name.as_str(),
                "bash" | "browser_check" | "write" | "edit" | "apply_patch"
            )
        }));
    }

    #[test]
    fn execute_capable_builder_gets_governed_browser_tool_and_prompt() {
        let scope = HashSet::from([
            "bash".to_string(),
            "browser_check".to_string(),
            "read".to_string(),
            "write".to_string(),
        ]);
        let policy = DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(10))
            .with_execution_tool_allowlist(Some(&scope));
        let task = SubAgentTask::new("browser-verifier", "verify the live page")
            .with_delegation_policy(policy);
        let config = BuilderConfig::new(task, Arc::new(SharedBuildContext::new()));

        let names = config
            .get_ai_tools()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert!(names.iter().any(|name| name == "browser_check"));
        assert!(config
            .system_prompt(1)
            .contains("browser_check tool is present and authorized"));
        assert!(config
            .system_prompt(1)
            .contains("Do not substitute HTTP 200 checks"));
    }

    #[tokio::test]
    async fn read_execute_child_prompt_forbids_workspace_mutating_project_targets() {
        let policy = DelegationPolicy::for_subagent_verify(PermissionMode::Autonomous, Some(7));
        let config =
            SingleChildConfig::new(Arc::new(ToolRegistry::new()), policy, String::new()).await;

        let prompt = config.system_prompt(1);
        assert!(prompt.contains("no workspace-write authority"));
        assert!(prompt.contains("do not run export, generate, install, clean"));
        assert!(prompt.contains("Request a write-capable child"));
        assert!(prompt.contains("return the handoff immediately"));
    }

    #[tokio::test]
    async fn execute_child_prompt_names_governed_browser_discovery_path() {
        let registry = Arc::new(ToolRegistry::new());
        register_all_tools(&registry).await;
        let policy = DelegationPolicy::for_subagent_child(
            PermissionMode::Autonomous,
            Some(7),
            true,
            false,
            true,
        );
        let config = SingleChildConfig::new(registry, policy, String::new()).await;

        assert!(config
            .get_ai_tools()
            .iter()
            .any(|tool| tool.name == "browser_check"));
        let prompt = config.system_prompt(1);
        assert!(prompt.contains("browser_check tool is authorized"));
        assert!(prompt.contains("tool_search by its exact name"));
        assert!(prompt.contains("Do not substitute HTTP 200 checks"));
    }

    #[test]
    fn builder_schema_is_empty_for_agent_only_parent_scope() {
        let scope = HashSet::from(["agent".to_string()]);
        let policy = DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(7))
            .with_execution_tool_allowlist(Some(&scope));
        let task = SubAgentTask::new("builder", "work").with_delegation_policy(policy);
        let config = BuilderConfig::new(task, Arc::new(SharedBuildContext::new()));

        assert!(config.get_ai_tools().is_empty());
    }

    #[test]
    fn builder_schema_preserves_explicit_empty_scope_as_tool_free() {
        let scope = HashSet::new();
        let policy = DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(7))
            .with_execution_tool_allowlist(Some(&scope));
        let task = SubAgentTask::new("builder", "work").with_delegation_policy(policy.clone());
        let config = BuilderConfig::new(task, Arc::new(SharedBuildContext::new()));

        assert_eq!(policy.execution_tool_allowlist, Some(Default::default()));
        assert!(config.get_ai_tools().is_empty());
    }
    #[test]
    fn builder_system_prompt_stays_stable_while_coordination_updates() {
        let context = Arc::new(SharedBuildContext::new());
        let task = SubAgentTask::new("builder", "work")
            .with_working_dir(std::path::PathBuf::from("/workspace"));
        let config = BuilderConfig::new(task, context.clone());

        let original_prompt = config.system_prompt(1);
        let original_context = config.dynamic_context();
        context.set_conventions(vec!["Keep provider prefixes stable".to_string()]);
        let updated_context = config.dynamic_context();

        assert_eq!(config.system_prompt(2), original_prompt);
        assert!(original_prompt.contains("<delegated_handoff>"));
        assert!(original_prompt.contains("return the handoff immediately"));
        assert_ne!(updated_context, original_context);
        assert!(updated_context
            .as_deref()
            .is_some_and(|value| value.contains("Keep provider prefixes stable")));
    }
}
