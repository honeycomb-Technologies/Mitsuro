//! Unified agent tool — spawns agnostic parent-directed child agents.
//!
//! Children are malleable workers: the parent supplies a name and instructions.
//! Capabilities select tool access (read vs write vs execute). Legacy
//! profile/agent_type labels map to capability defaults only.

mod build;
mod control;
mod helpers;
mod single;

#[cfg(test)]
mod tests;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tracing::{info, warn};

use crate::agent::subagent::{AgentExecutionProfile, AgentRuntimeManager, AgentSpec};
use crate::agent::AgentCancellation;
use crate::ai::client::AiClient;
use crate::storage::{Database, SessionType};
use crate::tools::registry::Tool;
use crate::tools::{parse_params, ToolContext, ToolResult};
use crate::SessionManager;

use helpers::*;

/// Unified agent tool — spawns and supervises agnostic child agents.
pub struct AgentTool {
    client: Arc<AiClient>,
    cancellation: AgentCancellation,
    runtime: AgentRuntimeManager,
}

impl AgentTool {
    pub fn new(
        client: Arc<AiClient>,
        cancellation: AgentCancellation,
        runtime: AgentRuntimeManager,
    ) -> Self {
        Self {
            client,
            cancellation,
            runtime,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AgentAction {
    #[default]
    Spawn,
    List,
    Status,
    Wait,
    Message,
    Followup,
    Interrupt,
    Resume,
}

#[derive(Deserialize, Default)]
struct Params {
    /// Lifecycle operation. Spawn is the default.
    #[serde(default)]
    action: AgentAction,

    /// Existing delegated run targeted by lifecycle operations.
    #[serde(default)]
    delegated_run_id: Option<String>,

    /// Parent steering for message/followup.
    #[serde(default)]
    message: Option<String>,

    /// Bounded wait duration.
    #[serde(default)]
    wait_timeout_ms: Option<u64>,

    /// Maximum records returned by list.
    #[serde(default)]
    limit: Option<usize>,

    /// Backward-compatible built-in profile.
    #[serde(default)]
    agent_type: Option<String>,

    /// Optional built-in or custom profile.
    #[serde(default)]
    profile: Option<String>,

    /// Parent instructions for the child (preferred product field).
    #[serde(default)]
    instructions: Option<String>,

    /// The main objective for the sub-agent (alias of instructions).
    #[serde(default)]
    prompt: String,

    /// Requested result shape or proof contract.
    #[serde(default)]
    expected_output: Option<String>,

    /// Canonical acceptance contract for a single task or legacy component set.
    #[serde(default)]
    acceptance_checks: Vec<AcceptanceCheckParams>,

    /// Concise reason delegation improves this task.
    #[serde(default)]
    delegation_reason: Option<String>,

    /// Requested capabilities, clamped by the parent policy.
    #[serde(default)]
    capabilities: Vec<String>,

    /// Parent context strategy: auto, project, brief, or full.
    #[serde(default)]
    context: Option<String>,

    /// Optional per-agent turn budget. Parent ceiling still wins.
    #[serde(default)]
    max_turns: Option<usize>,

    /// Internal marker preventing duplicate parent context injection.
    #[serde(skip)]
    parent_context_applied: bool,

    /// Exact durable run selected by lifecycle `resume`/terminal `followup`.
    /// Fresh spawns may still discover related evidence, but an explicit
    /// resume must never be rebound to a newer sibling with the same scope.
    #[serde(skip)]
    resumed_from_run_id: Option<String>,

    /// A single component is executed by the unified child rather than the
    /// legacy parallel pool. Keep the assignment after `components` is
    /// normalized away so it remains part of the durable resume contract.
    #[serde(skip)]
    assigned_component: Option<String>,

    /// Optional: path hint to scope exploration (explore only)
    #[serde(default)]
    scope: Option<String>,

    /// Components to build in parallel (build only, one agent per component)
    #[serde(default)]
    components: Option<Vec<String>>,

    /// A heterogeneous durable task graph. Ready tasks run concurrently while
    /// dependencies, task-scoped capabilities, and write intent remain
    /// explicit orchestration contracts.
    #[serde(default)]
    tasks: Option<Vec<StructuredAgentTaskParams>>,

    /// Coding conventions all builders must follow (build only)
    #[serde(default)]
    conventions: Option<Vec<String>>,

    /// Maximum concurrent builders (build only, defaults to component count)
    #[serde(default)]
    max_concurrency: Option<usize>,

    /// Plan task IDs corresponding to each component (build only, for auto-marking)
    #[serde(default)]
    task_ids: Option<Vec<String>>,

    /// Run the agent in the background. Returns immediately with delegated_run_id.
    #[serde(default)]
    run_in_background: Option<bool>,

    /// Parent-chosen child identity for spawn calls. This remains optional in
    /// deserialization for lifecycle actions and legacy compatibility; current
    /// spawn validation requires a non-empty name.
    #[serde(default)]
    name: Option<String>,

    /// Short description of what this agent does (for status display).
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct StructuredAgentTaskParams {
    #[serde(default, alias = "task_key")]
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    expected_output: Option<String>,
    #[serde(default)]
    acceptance_checks: Vec<AcceptanceCheckParams>,
    #[serde(default)]
    capabilities: Vec<String>,
    #[serde(default)]
    scope: Option<String>,
    /// Optional filesystem execution directory. Unlike `scope`, this is a
    /// concrete path and must resolve to an existing directory inside the
    /// selected project workspace.
    #[serde(default)]
    working_dir: Option<String>,
    #[serde(default)]
    write_intent: Vec<String>,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    max_turns: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct AcceptanceCheckParams {
    id: String,
    criterion: String,
    /// Optional canonical evidence tier. Browser tiers can only be satisfied
    /// by the governed browser_check tool; ordinary implementation checks use
    /// the structured delegated handoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    evidence_kind: Option<String>,
}

pub(super) fn compose_child_objective(prompt: &str, checks: &[AcceptanceCheckParams]) -> String {
    let prompt = prompt.trim_end();
    if checks.is_empty() {
        return prompt.to_string();
    }
    format!("{prompt}\n\n{}", acceptance_contract_block(checks))
}

fn acceptance_contract_block(checks: &[AcceptanceCheckParams]) -> String {
    if checks.is_empty() {
        return "ACCEPTANCE CONTRACT:\nReport concrete checks proportional to this bounded task. Do not expand into downstream integration or product-wide verification. Runtime interaction is required only when this task owns interactive behavior or verification."
            .to_string();
    }
    let payload = serde_json::to_string(&json!({"required": checks}))
        .expect("acceptance contract contains serializable strings");
    format!(
        "ACCEPTANCE CONTRACT:\nEvery required check below must appear in the final delegated_handoff with the exact id, passed status, and concrete evidence. A skipped, missing, or failed check means the objective is degraded, not complete. Browser evidence kinds require the execute capability and a successful governed browser_check call.\n<delegated_acceptance_contract>{payload}</delegated_acceptance_contract>"
    )
}

impl StructuredAgentTaskParams {
    fn objective(&self) -> &str {
        self.instructions
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| self.prompt.trim())
    }
}

#[async_trait]
impl Tool for AgentTool {
    fn name(&self) -> &str {
        "agent"
    }

    fn description(&self) -> &str {
        "Delegate substantial work, not simple lookups. One graph runs disjoint tasks in parallel; dependency-order overlaps. Interactive journeys need browser_check and typed acceptance evidence."
    }

    fn prompt(&self) -> Option<&str> {
        Some(
            "Delegate substantial work. In one tasks graph, run disjoint ready tasks concurrently and dependency-order shared-file, integration, or verification work. Declare acceptance_checks; set evidence_kind to browser_runtime, browser_keyboard, or browser_touch for governed browser proof. Interactive web journeys require browser_check at phone and desktop sizes without errors; builds and HTTP 200s are not substitutes. scope is prose; working_dir is an existing project-relative directory. Never combine tasks and components. In hosted Chat/Code, after run_in_background=true, continue independent work while the child runs. Do not call wait or status to poll. When only child-dependent work remains, finish the foreground response with an honest pending-work status and let durable completion wake the parent. Use wait only for explicitly synchronous monitoring. status reads terminal evidence; message steers live work; followup/resume starts new work.",
        )
    }

    fn parameters_schema(&self) -> Value {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["spawn", "list", "status", "wait", "message", "followup", "interrupt", "resume"],
                    "description": "Spawn/supervise; defaults to spawn. status reads evidence; followup/resume starts work."
                },
                "delegated_run_id": {
                    "type": "string"
                },
                "message": {
                    "type": "string"
                },
                "wait_timeout_ms": {
                    "type": "integer",
                    "description": "action=wait only; one synchronous wait",
                    "minimum": 0,
                    "maximum": 300000
                },
                "limit": {
                    "type": "integer",
                    "description": "action=list only; not a turn budget",
                    "minimum": 1,
                    "maximum": 100
                },
                "name": {
                    "type": "string"
                },
                "instructions": {
                    "type": "string"
                },
                "prompt": {
                    "type": "string"
                },
                "capabilities": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "string",
                        "enum": ["read", "write", "execute"]
                    }
                },
                "profile": {
                    "type": "string",
                    "description": "Optional legacy label (explore/build/plan/verify/custom). Prefer name + instructions + capabilities"
                },
                "expected_output": {
                    "type": "string"
                },
                "delegation_reason": {
                    "type": "string"
                },
                "context": {
                    "type": "string",
                    "enum": ["auto", "project", "brief", "full"]
                },
                "scope": {
                    "type": "string"
                },
                "components": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Legacy alternative to tasks (one child per component); omit or leave empty when tasks is provided"
                },
                "tasks": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 32,
                    "items": {
                        "type": "object",
                        "required": ["id"],
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "instructions": {"type": "string"},
                            "prompt": {"type": "string"},
                            "expected_output": {"type": "string"},
                            "capabilities": {
                                "type": "array",
                                "minItems": 1,
                                "items": {"type": "string", "enum": ["read", "write", "execute"]}
                            },
                            "scope": {"type": "string"},
                            "write_intent": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "depends_on": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                        },
                        "additionalProperties": false
                    }
                },
                "conventions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Shared conventions for parallel writers"
                },
                "max_concurrency": {
                    "type": "integer",
                    "description": "Optional parallel writer ceiling",
                    "minimum": 1
                },
                "task_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional plan task IDs corresponding to components"
                },
                "run_in_background": {
                    "type": "boolean",
                    "description": "Hosted only: return now; do not poll. Completion wakes the parent."
                },
                "description": {
                    "type": "string",
                    "description": "Short status label"
                }
            },
            "additionalProperties": false
        });
        schema["properties"]["tasks"]["items"]["properties"]["working_dir"] = json!({
            "type": "string",
            "description": "Optional existing workspace-relative directory for task execution; scope remains human-readable and is never interpreted as a path"
        });
        let acceptance_check_schema = json!({
            "type": "array",
            "maxItems": 16,
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "criterion": {"type": "string"},
                    "evidence_kind": {
                        "type": "string",
                        "enum": ["handoff", "browser_runtime", "browser_keyboard", "browser_touch"]
                    }
                },
                "required": ["id", "criterion"],
                "additionalProperties": false
            }
        });
        schema["properties"]["acceptance_checks"] = acceptance_check_schema.clone();
        schema["properties"]["tasks"]["items"]["properties"]["acceptance_checks"] =
            acceptance_check_schema;
        if self.runtime.has_completion_listener() {
            if let Some(actions) = schema["properties"]["action"]["enum"].as_array_mut() {
                actions.retain(|action| action.as_str() != Some("wait"));
            }
            schema["properties"]
                .as_object_mut()
                .expect("Agent schema properties")
                .remove("wait_timeout_ms");
        } else {
            schema["properties"]
                .as_object_mut()
                .expect("Agent schema properties")
                .remove("run_in_background");
        }
        schema
    }

    async fn execute(&self, params: Value, ctx: &ToolContext) -> ToolResult {
        info!("Agent tool execute called with params: {:?}", params);

        if has_explicit_empty_capabilities(&params) {
            return ToolResult::error_with_code(
                "invalid_agent_capabilities",
                "Agent capabilities cannot be an empty array. Omit the field for the default read capability, or request one or more of read, write, and execute.",
            );
        }

        let mut params = match parse_params::<Params>(params) {
            Ok(p) => p,
            Err(e) => {
                warn!("Agent tool parameter validation failed: {}", e.output);
                return e;
            }
        };

        params.normalize_instructions();

        if params.action == AgentAction::Spawn {
            self.execute_spawn(params, ctx).await
        } else {
            self.execute_control(params, ctx).await
        }
    }
}

fn has_explicit_empty_capabilities(params: &Value) -> bool {
    params
        .get("capabilities")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}

impl AgentTool {
    async fn execute_spawn(&self, mut params: Params, ctx: &ToolContext) -> ToolResult {
        if let Err(error) = normalize_acceptance_checks(&mut params.acceptance_checks) {
            return ToolResult::error_with_code("invalid_agent_acceptance_checks", error);
        }
        let structured_group = match normalize_structured_tasks(&mut params) {
            Ok(structured_group) => structured_group,
            Err(error) => {
                return ToolResult::error_with_code("invalid_agent_task_graph", error);
            }
        };
        if params
            .name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            params.name = Some(derived_spawn_name(&params));
        }
        let spec = match AgentSpec::resolve(
            params.profile.as_deref(),
            params.agent_type.as_deref(),
            params.name.as_deref(),
            &params.prompt,
            params.expected_output.as_deref(),
            params.delegation_reason.as_deref(),
            &params.capabilities,
            params.context.as_deref(),
            params.max_turns,
            ctx.subagent_max_turns,
        ) {
            Ok(spec) => spec,
            Err(error) => return ToolResult::error_with_code("invalid_agent_spec", error),
        };
        let execution_profile = spec.execution_profile();

        if params.run_in_background.unwrap_or(false) {
            if let Err(error) = validate_background_wake_host(&self.runtime, ctx) {
                return error;
            }
        }
        params.prompt = spec.rendered_objective();
        params.profile = Some(spec.profile.clone());
        // agent_type retained for internal compatibility as capability class only.
        params.agent_type = Some(execution_profile.as_str().to_string());
        params.max_turns = spec.max_turns;
        params.name = Some(spec.task_name.clone());
        if let Some(turns) = spec.parent_context_turns() {
            if let Some(parent_conversation) = ctx.parent_conversation.as_ref() {
                let brief = build_parent_context_brief(parent_conversation, turns);
                if !brief.is_empty() {
                    params.prompt = format!("{}\n\n{}", brief, params.prompt);
                }
            }
        }
        params.parent_context_applied = true;
        if let Some(components) = params.components.as_mut() {
            components.retain(|component| !component.trim().is_empty());
        }
        let parallel_components =
            should_use_parallel_component_pool(execution_profile, params.components.as_deref());
        if !parallel_components && !structured_group {
            if let Some(component) = params
                .components
                .as_ref()
                .and_then(|components| components.first())
            {
                params.assigned_component = Some(component.trim().to_string());
                params.prompt = format!(
                    "{}\n\nAssigned component: {}",
                    params.prompt,
                    component.trim()
                );
            }
            params.components = None;
        }

        // Stash capability hints for child policy selection in execute paths.
        params.capabilities = spec
            .capabilities
            .iter()
            .map(|cap| match cap {
                crate::agent::subagent::AgentCapability::Read => "read".to_string(),
                crate::agent::subagent::AgentCapability::Write => "write".to_string(),
                crate::agent::subagent::AgentCapability::Execute => "execute".to_string(),
            })
            .collect();

        info!(
            name = %spec.task_name,
            description = ?params.description,
            profile = %spec.profile,
            execution_profile = %execution_profile.as_str(),
            capabilities = ?spec.capabilities,
            max_turns = ?spec.max_turns,
            background = params.run_in_background.unwrap_or(false),
            "Resolved delegated AgentSpec (agnostic child)"
        );

        if parallel_components || structured_group {
            self.execute_build(params, ctx).await
        } else {
            self.execute_child(params, ctx).await
        }
    }
}

fn derived_spawn_name(params: &Params) -> String {
    let candidate = params
        .description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            params.tasks.as_ref().map(|tasks| {
                tasks
                    .iter()
                    .take(3)
                    .map(|task| {
                        task.name
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .unwrap_or(&task.id)
                    })
                    .collect::<Vec<_>>()
                    .join(" + ")
            })
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "delegated task".to_string());

    let mut end = candidate.len().min(96);
    while end > 0 && !candidate.is_char_boundary(end) {
        end -= 1;
    }
    candidate[..end].trim().to_string()
}

fn normalize_structured_tasks(params: &mut Params) -> Result<bool, String> {
    let Some(tasks) = params.tasks.as_mut() else {
        return Ok(false);
    };
    if tasks.is_empty() {
        return Err("Agent task graph requires at least one task".to_string());
    }
    if tasks.len() > 32 {
        return Err("Agent task graph cannot exceed 32 tasks".to_string());
    }
    if params
        .components
        .as_ref()
        .is_some_and(|items| !items.is_empty())
    {
        return Err("Use either tasks or legacy components, not both".to_string());
    }

    let mut positions = BTreeMap::new();
    let mut group_capabilities = BTreeSet::new();
    for (index, task) in tasks.iter_mut().enumerate() {
        task.id = task.id.trim().to_string();
        if task.id.is_empty() || task.id.len() > 128 {
            return Err("Every Agent task requires an id of 128 characters or fewer".to_string());
        }
        if positions.insert(task.id.clone(), index).is_some() {
            return Err(format!("Duplicate Agent task id '{}'", task.id));
        }
        if task.objective().is_empty() {
            return Err(format!("Agent task '{}' requires instructions", task.id));
        }
        normalize_acceptance_checks(&mut task.acceptance_checks)
            .map_err(|error| format!("Agent task '{}': {error}", task.id))?;
        if task.capabilities.is_empty() {
            task.capabilities.push("read".to_string());
        }
        for capability in &task.capabilities {
            let parsed = crate::agent::subagent::AgentCapability::parse(capability)?;
            group_capabilities.insert(match parsed {
                crate::agent::subagent::AgentCapability::Read => "read".to_string(),
                crate::agent::subagent::AgentCapability::Write => "write".to_string(),
                crate::agent::subagent::AgentCapability::Execute => "execute".to_string(),
            });
        }
        if !task
            .capabilities
            .iter()
            .any(|capability| capability == "execute")
        {
            if let Some(check) = task.acceptance_checks.iter().find(|check| {
                check
                    .evidence_kind
                    .as_deref()
                    .is_some_and(|kind| kind.starts_with("browser_"))
            }) {
                return Err(format!(
                    "Agent task '{}' acceptance check '{}' requires browser evidence but the task lacks execute capability. Add execute or move the browser check to an execute-capable verifier task",
                    task.id, check.id
                ));
            }
        }
        task.depends_on = task
            .depends_on
            .iter()
            .map(|dependency| dependency.trim().to_string())
            .collect();
        task.write_intent = task
            .write_intent
            .iter()
            .map(|path| path.trim().to_string())
            .filter(|path| !path.is_empty())
            .collect();
    }

    let mut indegree = vec![0usize; tasks.len()];
    let mut dependents = vec![Vec::new(); tasks.len()];
    for (index, task) in tasks.iter().enumerate() {
        let mut unique = BTreeSet::new();
        for dependency in &task.depends_on {
            let Some(&dependency_index) = positions.get(dependency) else {
                return Err(format!(
                    "Agent task '{}' references unknown dependency '{}'",
                    task.id, dependency
                ));
            };
            if dependency_index == index {
                return Err(format!("Agent task '{}' cannot depend on itself", task.id));
            }
            if !unique.insert(dependency.clone()) {
                return Err(format!(
                    "Agent task '{}' repeats dependency '{}'",
                    task.id, dependency
                ));
            }
            indegree[index] += 1;
            dependents[dependency_index].push(index);
        }
    }

    // Stable Kahn ordering places every ready wave before its dependents. This
    // prevents dependency waiters from occupying all local pool slots ahead
    // of runnable roots while the durable coordinator remains authoritative.
    let mut ready = (0..tasks.len())
        .filter(|index| indegree[*index] == 0)
        .collect::<BTreeSet<_>>();
    let mut order = Vec::with_capacity(tasks.len());
    while let Some(index) = ready.pop_first() {
        order.push(index);
        for dependent in &dependents[index] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                ready.insert(*dependent);
            }
        }
    }
    if order.len() != tasks.len() {
        return Err("Agent task graph contains a dependency cycle".to_string());
    }
    let original = tasks.clone();
    let ordered = order
        .into_iter()
        .map(|index| original[index].clone())
        .collect::<Vec<_>>();
    let mut ancestors = BTreeMap::<String, BTreeSet<String>>::new();
    for task in &ordered {
        let mut task_ancestors = BTreeSet::new();
        for dependency in &task.depends_on {
            task_ancestors.insert(dependency.clone());
            if let Some(inherited) = ancestors.get(dependency) {
                task_ancestors.extend(inherited.iter().cloned());
            }
        }
        ancestors.insert(task.id.clone(), task_ancestors);
    }
    for (index, left) in ordered.iter().enumerate() {
        if !left
            .capabilities
            .iter()
            .any(|capability| capability == "write")
            || left.write_intent.is_empty()
        {
            continue;
        }
        for right in ordered.iter().skip(index + 1) {
            if !right
                .capabilities
                .iter()
                .any(|capability| capability == "write")
                || right.write_intent.is_empty()
                || ancestors
                    .get(&left.id)
                    .is_some_and(|items| items.contains(&right.id))
                || ancestors
                    .get(&right.id)
                    .is_some_and(|items| items.contains(&left.id))
            {
                continue;
            }
            if left.write_intent.iter().any(|left_path| {
                right
                    .write_intent
                    .iter()
                    .any(|right_path| write_intents_overlap(left_path, right_path))
            }) {
                return Err(format!(
                    "Agent tasks '{}' and '{}' have overlapping write_intent but no dependency ordering. Add a depends_on edge between them or narrow their write_intent paths; no child was started.",
                    left.id, right.id
                ));
            }
        }
    }
    *tasks = ordered;

    params.capabilities = group_capabilities.into_iter().collect();
    if params.prompt.trim().is_empty()
        && params
            .instructions
            .as_deref()
            .is_none_or(|instructions| instructions.trim().is_empty())
    {
        params.prompt = format!(
            "Coordinate this structured task graph to completion: {}",
            tasks
                .iter()
                .map(|task| task.name.as_deref().unwrap_or(&task.id))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(true)
}

fn normalize_acceptance_checks(checks: &mut Vec<AcceptanceCheckParams>) -> Result<(), String> {
    if checks.len() > 16 {
        return Err("acceptance_checks cannot exceed 16 items".to_string());
    }
    let mut ids = BTreeSet::new();
    for check in checks {
        check.id = check.id.trim().to_ascii_lowercase();
        check.criterion = check.criterion.trim().to_string();
        if check.id.is_empty() || check.id.len() > 128 {
            return Err(
                "every acceptance check needs an id of 128 characters or fewer".to_string(),
            );
        }
        if check.criterion.is_empty() || check.criterion.len() > 1_200 {
            return Err(
                "every acceptance check needs a criterion of 1200 characters or fewer".to_string(),
            );
        }
        if check.evidence_kind.as_deref().is_some_and(|kind| {
            !matches!(
                kind,
                "handoff" | "browser_runtime" | "browser_keyboard" | "browser_touch"
            )
        }) {
            return Err(format!(
                "acceptance check '{}' has an unsupported evidence_kind",
                check.id
            ));
        }
        if !ids.insert(check.id.clone()) {
            return Err(format!("duplicate acceptance check id '{}'", check.id));
        }
    }
    Ok(())
}

fn write_intents_overlap(left: &str, right: &str) -> bool {
    fn normalized(value: &str) -> String {
        value
            .trim()
            .trim_start_matches("./")
            .trim_end_matches('/')
            .to_string()
    }
    let left = normalized(left);
    let right = normalized(right);
    !left.is_empty()
        && !right.is_empty()
        && (left == right
            || left
                .strip_prefix(&right)
                .is_some_and(|suffix| suffix.starts_with('/'))
            || right
                .strip_prefix(&left)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

fn validate_background_wake_host(
    runtime: &AgentRuntimeManager,
    ctx: &ToolContext,
) -> Result<(), ToolResult> {
    if !runtime.has_completion_listener() {
        return Err(ToolResult::error_with_code(
            "background_wake_unsupported",
            "Background Agent execution requires a live parent-wake host. Use a foreground Agent on this surface.",
        ));
    }
    let db_path = ctx.db_path.as_deref().ok_or_else(|| {
        ToolResult::error_with_code(
            "background_session_required",
            "Background Agent execution requires durable session storage.",
        )
    })?;
    let session_id = ctx.session_id.as_deref().ok_or_else(|| {
        ToolResult::error_with_code(
            "background_session_required",
            "Background Agent execution requires a persisted parent session.",
        )
    })?;
    let manager =
        SessionManager::new(Database::new(db_path).map_err(|error| {
            ToolResult::error_with_code("agent_store_error", error.to_string())
        })?);
    let session = manager
        .get_session(session_id)
        .map_err(|error| ToolResult::error_with_code("agent_store_error", error.to_string()))?
        .ok_or_else(|| {
            ToolResult::error_with_code(
                "background_session_required",
                format!("Parent session '{session_id}' was not found."),
            )
        })?;
    if !matches!(session.session_type, SessionType::Chat | SessionType::Code) {
        return Err(ToolResult::error_with_code(
            "background_wake_unsupported",
            "Background child completion cannot autonomously wake a Hive-owned session. Run this Agent in the foreground.",
        ));
    }
    Ok(())
}

fn should_use_parallel_component_pool(
    execution_profile: AgentExecutionProfile,
    components: Option<&[String]>,
) -> bool {
    execution_profile == AgentExecutionProfile::Build
        && components.is_some_and(|components| components.len() > 1)
}

impl Params {
    /// Normalize the preferred product field before either spawn or durable
    /// resume control paths inspect the objective.
    fn normalize_instructions(&mut self) {
        if self.prompt.trim().is_empty() {
            if let Some(instructions) = self.instructions.as_deref() {
                self.prompt = instructions.trim().to_string();
            }
        } else if let Some(instructions) = self.instructions.as_deref() {
            if !instructions.trim().is_empty() && instructions.trim() != self.prompt.trim() {
                self.prompt = format!("{}\n\n{}", instructions.trim(), self.prompt.trim());
            }
        }
    }
}

impl AgentTool {
    /// Resolve the session-scoped AI client, falling back to registration-time client.
    fn resolve_client(&self, ctx: &ToolContext) -> Arc<AiClient> {
        if let Some(ref session_client) = ctx.ai_client {
            if session_client.provider_id() != self.client.provider_id()
                || session_client.config().model != self.client.config().model
            {
                info!(
                    base_provider = %self.client.provider_id(),
                    session_provider = %session_client.provider_id(),
                    base_model = %self.client.config().model,
                    session_model = %session_client.config().model,
                    "Agent tool using session AI client instead of registration-time client"
                );
            }
            session_client.clone()
        } else {
            self.client.clone()
        }
    }

    /// Resolve the immutable model owned by the session client.
    ///
    /// `ToolContext::current_model` is retained as legacy UI metadata. It is a
    /// bare slug and therefore cannot safely override the exact provider/auth/
    /// transport identity frozen into `AiClient` for this run.
    fn resolve_model(&self, _ctx: &ToolContext, client: &AiClient) -> String {
        client.resolved_model().wire_model_id.clone()
    }
}
