use std::sync::Arc;

use crate::agent::AgentCancellation;
use crate::ai::client::AiClient;
use crate::tools::registry::ToolRegistry;

use super::{
    AddSubtaskTool, AgentTool, ApplyPatchTool, AskUserQuestionTool, AutonomousTaskTool, BashTool,
    BrowserCheckTool, EditTool, EnterPlanModeTool, GlobTool, GrepTool, ListTool, MemoryTool,
    MultiEditTool, PostToGroupTool, ProcessesTool, ReadTool, ReportTool,
    SearchCompactionSegmentsTool, SendToWorkerTool, SendUserMessageTool, SetDependencyTool,
    SetWorkModeTool, SetWorkspaceContextTool, SkillTool, SleepTool, TaskCompleteTool,
    TaskStartTool, ToolSearchTool, WebFetchTool, WebSearchTool, WorkflowProposeTool,
    WorkflowReadTool, WorkflowUpdateTool, WriteTool,
};

/// Register all built-in tools (except agent which needs client)
pub async fn register_all_tools(registry: &ToolRegistry) {
    registry.register(Arc::new(ReadTool)).await;
    registry.register(Arc::new(WriteTool)).await;
    registry.register(Arc::new(EditTool)).await;
    registry.register(Arc::new(MultiEditTool)).await;
    registry.register(Arc::new(BashTool)).await;
    registry.register(Arc::new(BrowserCheckTool)).await;
    registry.register(Arc::new(GrepTool)).await;
    registry.register(Arc::new(GlobTool)).await;
    registry.register(Arc::new(ListTool)).await;
    registry.register(Arc::new(WebSearchTool)).await;
    registry.register(Arc::new(WebFetchTool)).await;
    registry.register(Arc::new(ApplyPatchTool)).await;
    registry.register(Arc::new(ProcessesTool)).await;
    registry.register(Arc::new(SkillTool)).await;
    registry.register(Arc::new(MemoryTool)).await;
    registry
        .register(Arc::new(SearchCompactionSegmentsTool))
        .await;
    registry.register(Arc::new(AskUserQuestionTool)).await;
    registry.register(Arc::new(TaskCompleteTool)).await;
    registry.register(Arc::new(TaskStartTool)).await;
    registry.register(Arc::new(AddSubtaskTool)).await;
    registry.register(Arc::new(SetDependencyTool)).await;
    registry.register(Arc::new(SetWorkspaceContextTool)).await;
    registry.register(Arc::new(SetWorkModeTool)).await;
    registry.register(Arc::new(EnterPlanModeTool)).await;
    registry.register(Arc::new(WorkflowProposeTool)).await;
    registry.register(Arc::new(WorkflowUpdateTool)).await;
    registry.register(Arc::new(WorkflowReadTool)).await;
    registry.register(Arc::new(ToolSearchTool)).await;
    registry.register(Arc::new(SendUserMessageTool)).await;
    registry.register(Arc::new(SleepTool)).await;
}

/// Register tools for ACP.
///
/// ACP relays supervised approvals through the editor, but an approval dialog is
/// not an operating-system sandbox. Do not expose tools that can execute arbitrary
/// host commands or manage long-lived host processes until ACP can give them an
/// isolated execution boundary. File tools remain path-scoped to the canonical
/// session workspace. Read-only web tools are also exposed so ACP sessions can
/// answer current-information questions without shell access.
pub async fn register_acp_tools(registry: &ToolRegistry) {
    registry.register(Arc::new(ReadTool)).await;
    registry.register(Arc::new(WriteTool)).await;
    registry.register(Arc::new(EditTool)).await;
    registry.register(Arc::new(MultiEditTool)).await;
    registry.register(Arc::new(GrepTool)).await;
    registry.register(Arc::new(GlobTool)).await;
    registry.register(Arc::new(ListTool)).await;
    registry.register(Arc::new(WebSearchTool)).await;
    registry.register(Arc::new(WebFetchTool)).await;
    registry.register(Arc::new(ApplyPatchTool)).await;
    registry.register(Arc::new(ToolSearchTool)).await;
}

/// Register the unified agent tool (explore, plan, verify, build)
///
/// Call this after authentication when the AI client is available.
pub async fn register_agent_tool(
    registry: &ToolRegistry,
    client: Arc<AiClient>,
    cancellation: AgentCancellation,
) {
    let runtime = registry.agent_runtime_manager();
    registry
        .register(Arc::new(AgentTool::new(client, cancellation, runtime)))
        .await;
}

/// Register Hive-specific tools (autonomous tasks and reports).
///
/// These are additive — call after `register_all_tools` so Hive sessions
/// get both the standard Code tools and the Hive extensions. Hive delegates
/// through the `agent` tool; the legacy mailbox `send_message` path is not
/// part of the autonomous contract.
pub async fn register_hive_tools(registry: &ToolRegistry) {
    registry.register(Arc::new(AutonomousTaskTool)).await;
    registry.register(Arc::new(ReportTool)).await;
    // Group-room speech. Outside a group member run the tool returns a
    // structured not_a_group_run error, so registration stays hive-wide.
    registry.register(Arc::new(PostToGroupTool)).await;
    registry.register(Arc::new(SendToWorkerTool)).await;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::agent::AgentCancellation;
    use crate::ai::client::{AiClient, AiClientConfig, MITSURO_SYSTEM_PROMPT};
    use crate::ai::format::get_format_handler;
    use crate::ai::models::ApiFormat;
    use crate::tools::registry::ToolRegistry;

    #[tokio::test]
    async fn acp_tools_exclude_unsandboxable_host_execution_tools() {
        let registry = ToolRegistry::new();

        register_acp_tools(&registry).await;

        assert!(registry.get("read").await.is_some());
        assert!(registry.get("write").await.is_some());
        assert!(registry.get("edit").await.is_some());
        assert!(registry.get("grep").await.is_some());
        assert!(registry.get("glob").await.is_some());
        assert!(registry.get("web_search").await.is_some());
        assert!(registry.get("web_fetch").await.is_some());
        assert!(registry.get("tool_search").await.is_some());
        assert!(registry.get("bash").await.is_none());
        assert!(registry.get("processes").await.is_none());
    }

    #[tokio::test]
    async fn default_wire_surface_is_bounded_but_catalog_remains_reachable() {
        let registry = ToolRegistry::new();
        register_all_tools(&registry).await;
        register_agent_tool(
            &registry,
            Arc::new(AiClient::new(AiClientConfig::default(), String::new())),
            AgentCancellation::new(),
        )
        .await;

        let unhosted_catalog = registry.get_ai_tools_all().await;
        let unhosted_agent = unhosted_catalog
            .iter()
            .find(|tool| tool.name == "agent")
            .expect("agent tool should be registered");
        assert!(unhosted_agent.input_schema["properties"]
            .get("run_in_background")
            .is_none());
        assert!(unhosted_agent.input_schema["properties"]["action"]["enum"]
            .as_array()
            .is_some_and(|actions| actions.iter().any(|action| action == "wait")));
        assert!(unhosted_agent.input_schema["properties"]
            .get("wait_timeout_ms")
            .is_some());

        let (completion_tx, _completion_rx) = tokio::sync::mpsc::unbounded_channel();
        registry
            .agent_runtime_manager()
            .set_completion_sender(completion_tx);
        let (reconciliation_tx, _reconciliation_rx) = tokio::sync::mpsc::unbounded_channel();
        registry
            .agent_runtime_manager()
            .set_completion_reconciliation_sender(reconciliation_tx);

        let wire_tools = registry.get_ai_tools().await;
        let catalog = registry.get_ai_tools_all().await;

        assert!(wire_tools.len() <= crate::tools::registry::DEFAULT_CODE_TOOL_LIMIT);
        assert!(wire_tools.iter().any(|tool| tool.name == "tool_search"));
        assert!(catalog.len() > wire_tools.len());
        assert!(catalog.iter().any(|tool| tool.name == "memory"));
        assert!(catalog.iter().any(|tool| tool.name == "browser_check"));
        assert!(!wire_tools.iter().any(|tool| tool.name == "browser_check"));
        let agent = catalog
            .iter()
            .find(|tool| tool.name == "agent")
            .expect("agent tool should be registered");
        assert!(agent.description.contains("parallel"));
        assert!(agent.description.contains("not simple lookups"));
        for field in ["name", "instructions", "capabilities"] {
            assert!(
                agent.input_schema["properties"].get(field).is_some(),
                "current Agent contract must expose {field}"
            );
        }
        assert!(
            agent.input_schema["properties"]["run_in_background"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("do not poll"))
        );
        assert!(agent.input_schema["properties"]["action"]["enum"]
            .as_array()
            .is_some_and(|actions| actions.iter().all(|action| action != "wait")));
        assert!(agent.input_schema["properties"]
            .get("wait_timeout_ms")
            .is_none());
        assert!(agent.input_schema["properties"]["limit"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("not a turn budget")));
        assert!(agent.input_schema["properties"]["action"]["enum"]
            .as_array()
            .is_some_and(|actions| actions.iter().any(|action| action == "resume")));
        assert!(agent.input_schema.get("required").is_none());
        assert!(agent.input_schema["properties"].get("agent_type").is_none());
        assert_eq!(
            agent.input_schema["properties"]["task_ids"]["items"]["type"],
            "string"
        );
        assert!(agent.input_schema["properties"]["task_ids"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("corresponding to components")));
        let task_properties = &agent.input_schema["properties"]["tasks"]["items"]["properties"];
        assert!(task_properties.get("max_turns").is_none());
        assert!(task_properties["working_dir"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("scope remains human-readable")));
        assert!(
            agent.input_schema["properties"]["components"]["description"]
                .as_str()
                .is_some_and(|description| description.contains("when tasks is provided"))
        );

        let provider_tools =
            get_format_handler(ApiFormat::OpenAIResponses).convert_tools(&wire_tools);
        assert!(serde_json::to_string(&provider_tools)
            .expect("serialize provider tools")
            .contains("not simple lookups"));
        for (tool, provider_tool) in wire_tools.iter().zip(provider_tools.iter()) {
            println!(
                "tool_schema name={} bytes={}",
                tool.name,
                serde_json::to_vec(provider_tool).unwrap().len()
            );
        }
        let tool_bytes = serde_json::to_vec(&provider_tools).unwrap().len();
        let base_prompt_bytes = MITSURO_SYSTEM_PROMPT.len();
        let fixed_bytes = base_prompt_bytes + tool_bytes;
        let estimated_tokens = fixed_bytes.div_ceil(4);
        println!(
            "default_tool_count={} base_prompt_bytes={} tool_bytes={} fixed_bytes={} estimated_tokens={}",
            wire_tools.len(),
            base_prompt_bytes,
            tool_bytes,
            fixed_bytes,
            estimated_tokens
        );
        assert!(
            estimated_tokens <= 2_600,
            "fixed no-project budget exceeded: tool_count={} base_prompt_bytes={} tool_bytes={} fixed_bytes={} estimated_tokens={} ceiling=2600",
            wire_tools.len(),
            base_prompt_bytes,
            tool_bytes,
            fixed_bytes,
            estimated_tokens
        );
    }
}
