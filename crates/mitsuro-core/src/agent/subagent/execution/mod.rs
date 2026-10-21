//! Sub-agent execution loop
//!
//! Unified agentic loop for both explorer and builder agents.

mod api;
mod config;
mod explorer;
mod governance;
mod runtime;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::ai::client::AiClient;
use crate::tools::registry::{DelegationPolicy, ToolRegistry};

use self::config::{BuilderConfig, SingleChildConfig};
pub(crate) use self::runtime::execute_agent_loop;

use super::build_context::SharedBuildContext;
use super::types::{AgentProgress, SubAgentResult, SubAgentTask};

/// Execute one parent-directed child through the shared governed loop.
pub async fn execute_single_child(
    client: Arc<AiClient>,
    task: SubAgentTask,
    registry: Arc<ToolRegistry>,
    policy: DelegationPolicy,
    project_context: String,
    model: String,
    cancellation: CancellationToken,
    progress_tx: Option<mpsc::UnboundedSender<AgentProgress>>,
) -> SubAgentResult {
    let project_context = delegated_execution_context(&task, project_context);
    let config = SingleChildConfig::new(registry, policy, project_context).await;
    execute_agent_loop(&client, &task, &model, cancellation, &config, progress_tx).await
}

fn delegated_execution_context(task: &SubAgentTask, project_context: String) -> String {
    let execution_root = task
        .sandbox_root
        .as_deref()
        .unwrap_or(&task.working_dir)
        .display();
    let working_dir = task.working_dir.display();
    format!(
        "{project_context}\n\n\
[DELEGATED EXECUTION AUTHORITY]\n\
Writable execution root: {execution_root}\n\
Current working directory: {working_dir}\n\
- These paths are the canonical filesystem authority for this delegated attempt.\n\
- Use paths relative to the current working directory whenever possible.\n\
- Any source/project path shown earlier is parent-session context only and may be read-only inside this attempt. Do not cd to it, write to it, or use it as a Cargo/build output root.\n\
- Run validation from the current working directory. The parent will integrate accepted changes into the authoritative source workspace.\n\
[/DELEGATED EXECUTION AUTHORITY]"
    )
}

/// Execute a builder agent with progress reporting.
pub(crate) async fn execute_builder_with_progress(
    client: &AiClient,
    task: SubAgentTask,
    model: &str,
    cancellation: CancellationToken,
    context: Arc<SharedBuildContext>,
    progress_tx: mpsc::UnboundedSender<AgentProgress>,
) -> SubAgentResult {
    let config = BuilderConfig::new(task.clone(), context);
    execute_agent_loop(
        client,
        &task,
        model,
        cancellation,
        &config,
        Some(progress_tx),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::delegated_execution_context;
    use super::explorer::{
        collect_paths_from_tool_result, normalize_explorer_result, should_replace_forced_summary,
        synthesized_explorer_output, text_claims_tool_empty, timeout_partial_output,
        tool_result_has_positive_evidence,
    };
    use super::governance::{build_subagent_tool_context, delegated_turn_budget};
    use crate::agent::subagent::{
        SubAgentResult, SubAgentTask, SubAgentTermination, TaskObjectiveStatus,
    };
    use crate::process::ProcessRegistry;
    use crate::tools::registry::{DelegationPolicy, PermissionMode};
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn delegated_execution_authority_overrides_parent_workspace_context() {
        let task = SubAgentTask::new("writer", "work")
            .with_working_dir(PathBuf::from("/tmp/attempt/repo/component"))
            .with_sandbox_root(PathBuf::from("/tmp/attempt/repo"));

        let context =
            delegated_execution_context(&task, "Project directory: /home/user/source".to_string());

        assert!(context.contains("Writable execution root: /tmp/attempt/repo"));
        assert!(context.contains("Current working directory: /tmp/attempt/repo/component"));
        assert!(context.contains("parent-session context only"));
        assert!(context.contains("Do not cd to it, write to it"));
    }

    #[test]
    fn delegated_turn_budget_prefers_task_override_then_policy_then_unlimited_default() {
        let budget_from_task = delegated_turn_budget(
            &SubAgentTask::new("task", "prompt")
                .with_delegation_policy(DelegationPolicy::for_subagent_build(
                    PermissionMode::Autonomous,
                    Some(11),
                ))
                .with_max_turns(7),
        );
        assert_eq!(budget_from_task, Some(7));

        let budget_from_policy =
            delegated_turn_budget(&SubAgentTask::new("task", "prompt").with_delegation_policy(
                DelegationPolicy::for_subagent_explore(PermissionMode::Autonomous, Some(11)),
            ));
        assert_eq!(budget_from_policy, Some(11));

        assert_eq!(
            delegated_turn_budget(&SubAgentTask::new("task", "prompt")),
            None
        );
    }

    #[test]
    fn build_subagent_tool_context_inherits_delegated_policy_contract() {
        let working_dir = PathBuf::from("/tmp/mitsuro-subagent");
        let policy = DelegationPolicy::for_subagent_build(PermissionMode::Autonomous, Some(17));
        let task = SubAgentTask::new("task", "prompt")
            .with_working_dir(working_dir.clone())
            .with_delegation_policy(policy.clone());

        let ctx = build_subagent_tool_context(&task, 45);

        assert_eq!(ctx.working_dir, working_dir);
        assert_eq!(ctx.timeout.map(|timeout| timeout.as_secs()), Some(45));
        assert_eq!(ctx.permission_mode, PermissionMode::Autonomous);
        assert_eq!(ctx.subagent_max_turns, Some(17));
        assert_eq!(ctx.delegation_policy, Some(policy));
        assert_eq!(ctx.sandbox_root, None);
        assert!(matches!(
            ctx.filesystem_access,
            crate::tools::registry::FilesystemAccess::Unrestricted
        ));
    }

    #[test]
    fn build_subagent_tool_context_preserves_inherited_sandbox_root() {
        let working_dir = PathBuf::from("/tmp/mitsuro-subagent/project");
        let sandbox_root = PathBuf::from("/tmp/mitsuro-subagent");
        let task = SubAgentTask::new("task", "prompt")
            .with_working_dir(working_dir.clone())
            .with_sandbox_root(sandbox_root.clone())
            .with_delegation_policy(DelegationPolicy::for_subagent_build(
                PermissionMode::Autonomous,
                Some(17),
            ));

        let ctx = build_subagent_tool_context(&task, 45);

        assert_eq!(ctx.working_dir, working_dir);
        assert_eq!(ctx.sandbox_root, Some(sandbox_root));
    }

    #[test]
    fn build_subagent_tool_context_preserves_process_owner_and_registry() {
        let registry = Arc::new(ProcessRegistry::new());
        let task = SubAgentTask::new("task", "prompt")
            .with_delegation_policy(DelegationPolicy::for_subagent_build(
                PermissionMode::Autonomous,
                Some(17),
            ))
            .with_process_context(
                Some(Arc::clone(&registry)),
                Some("owner-a".to_string()),
                Some("session-a".to_string()),
            );

        let ctx = build_subagent_tool_context(&task, 45);

        assert!(ctx
            .process_registry
            .as_ref()
            .is_some_and(|inherited| Arc::ptr_eq(inherited, &registry)));
        assert_eq!(ctx.user_id.as_deref(), Some("owner-a"));
        assert!(ctx
            .process_owner_id
            .as_deref()
            .is_some_and(|owner| owner.starts_with("owner-a:hive:")));
        assert_ne!(ctx.process_owner_id, ctx.user_id);
        assert_eq!(ctx.session_id.as_deref(), Some("session-a"));
    }

    #[test]
    fn delegated_process_owners_are_stable_and_task_isolated() {
        let base = SubAgentTask::new("task-a", "prompt")
            .with_delegated_run_id("run-a")
            .with_process_context(None, Some("tenant-a".to_string()), None);
        let same = base.clone();
        let peer = SubAgentTask::new("task-b", "prompt")
            .with_delegated_run_id("run-a")
            .with_process_context(None, Some("tenant-a".to_string()), None);
        let other_run = SubAgentTask::new("task-a", "prompt")
            .with_delegated_run_id("run-b")
            .with_process_context(None, Some("tenant-a".to_string()), None);

        assert_eq!(
            base.delegated_process_owner_id(),
            same.delegated_process_owner_id()
        );
        assert_ne!(
            base.delegated_process_owner_id(),
            peer.delegated_process_owner_id()
        );
        assert_ne!(
            base.delegated_process_owner_id(),
            other_run.delegated_process_owner_id()
        );

        let single_tenant = SubAgentTask::new("task-a", "prompt")
            .with_delegated_run_id("run-a")
            .delegated_process_owner_id();
        assert!(single_tenant.starts_with("default:hive:"));
    }

    #[test]
    fn explore_subagent_tool_context_inherits_unrestricted_parent_access() {
        let working_dir = PathBuf::from("/tmp/mitsuro-explore-scope");
        let policy = DelegationPolicy::for_subagent_explore(PermissionMode::Autonomous, Some(9));
        let task = SubAgentTask::new("task", "prompt")
            .with_working_dir(working_dir.clone())
            .with_delegation_policy(policy);

        let ctx = build_subagent_tool_context(&task, 30);

        assert_eq!(ctx.working_dir, working_dir);
        assert_eq!(ctx.sandbox_root, None);
        assert!(matches!(
            ctx.filesystem_access,
            crate::tools::registry::FilesystemAccess::Unrestricted
        ));
    }

    #[test]
    fn text_claims_tool_empty_detects_known_misreads() {
        assert!(text_claims_tool_empty(
            "Every tool is returning empty results and nothing is working."
        ));
        assert!(!text_claims_tool_empty(
            "The glob returned 12 files and the read succeeded."
        ));
    }

    #[test]
    fn tool_result_positive_evidence_detects_real_read_and_glob_data() {
        let read_output = json!({
            "data": {
                "content": "fn main() {}"
            }
        })
        .to_string();
        let glob_output = json!({
            "count": 3,
            "matches": ["src/main.rs", "src/lib.rs", "src/app.rs"]
        })
        .to_string();

        assert!(tool_result_has_positive_evidence(
            "read",
            &read_output,
            false
        ));
        assert!(tool_result_has_positive_evidence(
            "glob",
            &glob_output,
            false
        ));
        assert!(!tool_result_has_positive_evidence(
            "glob",
            &glob_output,
            true
        ));
    }

    #[test]
    fn collect_paths_from_list_preserves_directory_markers() {
        let output = json!({
            "data": {
                "output": "agent/\nai/\nstorage/\nlib.rs",
                "total_entries": 4
            }
        })
        .to_string();

        let paths = collect_paths_from_tool_result("list", &output, PathBuf::from(".").as_path());
        assert_eq!(paths, vec!["agent/", "ai/", "storage/", "lib.rs"]);
    }

    #[test]
    fn collect_paths_from_empty_glob_retains_the_searched_directory() {
        let working_dir = PathBuf::from("/tmp/mitsuro-empty-workspace");
        let output = json!({
            "data": {
                "matches": [],
                "count": 0,
                "search_path": "/tmp/mitsuro-empty-workspace"
            }
        })
        .to_string();

        let paths = collect_paths_from_tool_result("glob", &output, &working_dir);
        assert_eq!(paths, vec!["."]);
    }

    #[test]
    fn placeholder_forced_summary_gets_replaced() {
        assert!(should_replace_forced_summary(
            "Let me try using glob to inspect this target."
        ));
        assert!(should_replace_forced_summary(
            "Let me read the main module file and get context on the architecture:"
        ));
        assert!(should_replace_forced_summary(
            "I'll start by exploring the directory structure and then read the key files to understand the architecture."
        ));
        assert!(should_replace_forced_summary(
            "<explore_report>{\"summary\":\"Let me inspect a few files first.\",\"paths_examined\":[],\"files_examined\":[],\"key_findings\":[],\"design_patterns\":[],\"concerns\":[],\"confidence\":\"low\"}</explore_report>"
        ));
    }

    #[test]
    fn substantive_forced_summary_is_preserved() {
        assert!(!should_replace_forced_summary(
            "<explore_report>{\"summary\":\"The module centers runtime orchestration in orchestrator.rs and failure containment in failure.rs.\",\"paths_examined\":[\"agent/\",\"orchestrator.rs\",\"failure.rs\"],\"files_examined\":[\"orchestrator.rs\",\"failure.rs\"],\"key_findings\":[\"The orchestrator owns the main loop and continuation handling.\"],\"design_patterns\":[\"Event-driven orchestration\"],\"concerns\":[\"Failure policy remains centralized.\"],\"confidence\":\"medium\"}</explore_report>"
        ));
    }

    #[test]
    fn synthesized_explorer_output_replaces_placeholder_with_report() {
        let files = vec!["src/lib.rs".to_string(), "src/main.rs".to_string()];

        let output =
            synthesized_explorer_output("explorer", "Let me inspect a few files first.", &files);

        assert!(output.contains("<explore_report>"));
        assert!(output.contains("src/lib.rs"));
        assert!(output.contains("src/main.rs"));
    }

    #[test]
    fn synthesized_explorer_output_preserves_substantive_summary() {
        let substantive = "<explore_report>{\"summary\":\"The module centers runtime orchestration in orchestrator.rs.\",\"paths_examined\":[\"orchestrator.rs\"],\"files_examined\":[\"orchestrator.rs\"],\"key_findings\":[\"The orchestrator owns the main loop.\"],\"design_patterns\":[\"Event-driven orchestration\"],\"concerns\":[],\"confidence\":\"medium\"}</explore_report>";

        assert_eq!(
            synthesized_explorer_output("explorer", substantive, &["orchestrator.rs".to_string()]),
            substantive
        );
    }

    #[test]
    fn explorer_normalization_preserves_valid_handoff_when_synthesizing_report() {
        let task = SubAgentTask::new("environment proof", "inspect marker").with_delegation_policy(
            DelegationPolicy::for_subagent_explore(PermissionMode::Autonomous, None),
        );
        let result = SubAgentResult {
            task_id: "environment-proof".to_string(),
            agent_name: "proof worker".to_string(),
            delegated_run_id: None,
            success: true,
            output: r#"<explore_report>{"status":"complete","objective":"inspect marker"}</explore_report>
<delegated_handoff>{"status":"complete","summary":"marker verified","acceptance_checks":[{"id":"marker","status":"passed","evidence":"marker read"}],"remaining_work":[],"blockers":[]}</delegated_handoff>"#
                .to_string(),
            files_examined: vec!["marker-a.txt".to_string()],
            duration_ms: 1,
            turns_used: 1,
            error: None,
            termination: SubAgentTermination::Completed,
            policy_violations: Vec::new(),
            evidence: Default::default(),
            background_processes: Vec::new(),
        };
        assert!(result.delegated_handoff().is_some());

        let normalized = normalize_explorer_result(result, &task);

        assert!(normalized.output.starts_with("<explore_report>"));
        assert!(normalized.delegated_handoff().is_some());
        assert_eq!(normalized.objective_status(), TaskObjectiveStatus::Complete);
    }

    #[test]
    fn timeout_partial_output_is_agent_neutral() {
        let output =
            timeout_partial_output("", &["src/lib.rs".to_string(), "src/main.rs".to_string()]);

        assert!(output.starts_with("Sub-agent timed out before producing final output."));
        assert!(output.contains("src/lib.rs"));
        assert!(!output.contains("Explorer timed out"));
    }
}
