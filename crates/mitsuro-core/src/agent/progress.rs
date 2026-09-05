//! Semantic progress accounting for the canonical parent-agent loop.
//!
//! Turn count is a resource budget, not a loop detector. This ledger tracks
//! whether tool work produces new evidence or successful state changes so
//! cosmetically different calls cannot keep an agent alive indefinitely.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::ai::types::{AiToolCall, Content};
use crate::storage::DelegationMode;
use crate::tools::registry::{
    agent_call_action, agent_call_requests_write, effective_tool_call, tool_policy_for_call,
    trusted_changed, ToolCategory,
};

use super::failure;
use super::hooks::shell_policy::{classify_bash_command, semantic_bash_signature};

/// Consecutive evidence-free passive turns allowed before stopping.
pub const NO_PROGRESS_TURN_THRESHOLD: usize = 3;
/// Inject a model-visible change-of-strategy instruction before the terminal
/// threshold, giving a capable model one clean opportunity to recover.
pub const NO_PROGRESS_REPLAN_THRESHOLD: usize = 2;
/// A single observation intent may reveal a few changing outcomes (for
/// example, a growing log), but raw stdout churn is not unbounded proof of
/// progress. This closes random/timestamp output loops without imposing a
/// global turn limit.
pub const MAX_OUTCOME_VARIANTS_PER_INTENT: usize = 6;
/// Likewise, one identical outcome may be discovered through a bounded number
/// of distinct semantic intents (for example, several equal files). Varying
/// arguments around unchanged output cannot buy unlimited progress.
pub const MAX_INTENTS_PER_IDENTICAL_OUTCOME: usize = 8;
/// Opaque shell effects do not provide a trustworthy state delta. Permit a
/// short sequence of distinct build steps, then require independent evidence
/// before more unverified effects can keep the run alive.
pub const MAX_CONSECUTIVE_UNVERIFIED_EFFECT_TURNS: usize = 6;
/// Offer delegation advice after sustained multi-area investigation. This is
/// not a stop threshold: productive observation may continue without a child.
pub const DELEGATION_NUDGE_TURN_THRESHOLD: usize = 3;
pub const DELEGATION_NUDGE_AREA_THRESHOLD: usize = 3;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ActionClass {
    Observe,
    Mutate,
    Validate,
    Delegate,
    Communicate,
    Control,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionFingerprint {
    pub class: ActionClass,
    /// Stable hash of normalized intent; raw commands and arguments are not
    /// emitted into runtime telemetry.
    pub intent: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProgressGuardAction {
    Warn,
    Replan,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgressGuardTelemetry {
    pub guard: String,
    pub no_progress_turns: usize,
    pub threshold: usize,
    pub mutation_epoch: u64,
    pub action_classes: Vec<ActionClass>,
    pub evidence_signature: String,
    pub action: ProgressGuardAction,
    pub triggered: bool,
}

impl ProgressGuardTelemetry {
    pub fn diagnostic(&self) -> Option<String> {
        self.triggered.then(|| {
            format!(
                "Stopping no-progress loop: repeated work produced no verified state change or bounded new evidence for {} consecutive turns. Synthesize the evidence already gathered, change strategy, or ask the user for direction.",
                self.no_progress_turns
            )
        })
    }

    pub fn replan_instruction(&self) -> Option<&'static str> {
        matches!(self.action, ProgressGuardAction::Replan).then_some(
            "[PROGRESS GUARD]\nThe last actions repeated existing evidence or produced no state change. Do not repeat the same command with cosmetic variations. Reassess the current evidence, choose a materially different action, finish with the answer if the task is already complete, or ask for missing input.",
        )
    }
}

#[derive(Debug)]
pub struct LoopGuardOutcome {
    pub repeated_failure: Option<String>,
    pub repeated_validation: Option<String>,
    /// Same pure-exploration tool batch repeated without mutation.
    pub repeated_read_only: Option<String>,
    pub progress: Option<ProgressGuardTelemetry>,
}

#[derive(Debug, Default)]
pub struct LoopGuard {
    progress: ProgressLedger,
    failure_signatures: HashMap<String, usize>,
    validation_signatures: HashMap<String, usize>,
    read_only_signatures: HashMap<String, usize>,
}

impl LoopGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset_for_steering(&mut self) {
        self.progress.reset_for_steering();
        self.failure_signatures.clear();
        self.validation_signatures.clear();
        self.read_only_signatures.clear();
    }

    pub fn evaluate(
        &mut self,
        tool_calls: &[AiToolCall],
        tool_results: &[Content],
    ) -> LoopGuardOutcome {
        // A producer-verified mutation creates a new program state. Failures
        // observed before that boundary are no longer consecutive evidence
        // about the current state, so a first validation rerun after a fix
        // must not trip the repeated-failure guard. Subsequent identical
        // failures still converge under the normal threshold.
        if turn_has_verified_mutation(tool_calls, tool_results) {
            self.failure_signatures.clear();
            self.validation_signatures.clear();
            self.read_only_signatures.clear();
        }
        LoopGuardOutcome {
            repeated_failure: failure::detect_repeated_failures(
                &mut self.failure_signatures,
                tool_calls,
                tool_results,
            ),
            repeated_validation: failure::detect_repeated_validation_sequence(
                &mut self.validation_signatures,
                tool_calls,
                tool_results,
            ),
            repeated_read_only: failure::detect_repeated_read_only_sequence(
                &mut self.read_only_signatures,
                tool_calls,
                tool_results,
            ),
            progress: self.progress.record_turn(tool_calls, tool_results),
        }
    }
}

fn turn_has_verified_mutation(tool_calls: &[AiToolCall], tool_results: &[Content]) -> bool {
    let results = results_by_call_id(tool_results);
    tool_calls.iter().any(|call| {
        action_fingerprint(call).class == ActionClass::Mutate
            && results
                .get(call.id.as_str())
                .is_some_and(|result| !result.is_error && result.changed == Some(true))
    })
}

/// One advisory nudge per observation sequence, when session policy and the
/// current tool surface permit delegation. Only the semantic progress guard
/// may stop evidence-free work; breadth alone never forces a final response.
#[derive(Debug, Default)]
pub(crate) struct DelegationNudgeTracker {
    consecutive_observation_turns: usize,
    observed_areas: HashSet<String>,
    nudged: bool,
}

impl DelegationNudgeTracker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reset_for_steering(&mut self) {
        self.reset_sequence();
    }

    pub(crate) fn record_turn(
        &mut self,
        tool_calls: &[AiToolCall],
        tool_results: &[Content],
        mode: DelegationMode,
        can_delegate: bool,
    ) -> Option<String> {
        if mode == DelegationMode::ExplicitOnly
            || !can_delegate
            || tool_calls.is_empty()
            || !tool_calls
                .iter()
                .all(|call| action_fingerprint(call).class == ActionClass::Observe)
        {
            self.reset_sequence();
            return None;
        }

        let results = results_by_call_id(tool_results);
        if tool_calls.iter().any(|call| {
            results
                .get(call.id.as_str())
                .is_none_or(|result| result.is_error)
        }) {
            self.reset_sequence();
            return None;
        }

        if self.nudged {
            return None;
        }

        self.consecutive_observation_turns = self.consecutive_observation_turns.saturating_add(1);
        for call in tool_calls {
            self.observed_areas.extend(observation_areas(call));
        }

        if self.consecutive_observation_turns < DELEGATION_NUDGE_TURN_THRESHOLD
            || self.observed_areas.len() < DELEGATION_NUDGE_AREA_THRESHOLD
        {
            return None;
        }

        self.nudged = true;
        Some(format!(
            "[BROAD OBSERVATION CHECKPOINT]\nThe parent has completed {} consecutive observation turns across {} repository areas. If the remaining investigation is substantial and separable, consider one named `agent` child with precise bounded instructions. Set `run_in_background=true` only when that field is available on the current Agent tool surface; in that case, continue independent work while the child runs. Otherwise run the named child in the foreground and use its result before continuing. Continue productive direct investigation when it best serves the user's scope. If the evidence is sufficient, synthesize or act directly. Do not delegate merely to satisfy this checkpoint.",
            self.consecutive_observation_turns,
            self.observed_areas.len(),
        ))
    }

    fn reset_sequence(&mut self) {
        self.consecutive_observation_turns = 0;
        self.observed_areas.clear();
        self.nudged = false;
    }
}

fn observation_areas(call: &AiToolCall) -> HashSet<String> {
    let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
    let mut candidates = Vec::new();

    if matches!(name, "bash" | "shell" | "execute") {
        if let Some(command) = arguments.get("command").and_then(Value::as_str) {
            candidates.extend(command.split_whitespace().map(ToString::to_string));
        }
    } else if let Some(object) = arguments.as_object() {
        for (key, value) in object {
            let path_like = matches!(
                key.as_str(),
                "path" | "file_path" | "directory" | "root" | "cwd" | "working_dir" | "include"
            ) || (name == "glob" && key == "pattern");
            if path_like {
                collect_string_values(value, &mut candidates);
            }
        }
    }

    candidates
        .iter()
        .filter_map(|candidate| normalize_observation_area(candidate))
        .collect()
}

fn collect_string_values(value: &Value, values: &mut Vec<String>) {
    match value {
        Value::String(value) => values.push(value.clone()),
        Value::Array(items) => {
            for item in items {
                collect_string_values(item, values);
            }
        }
        _ => {}
    }
}

fn normalize_observation_area(candidate: &str) -> Option<String> {
    let candidate = candidate
        .trim_matches(|character: char| {
            character.is_whitespace()
                || matches!(
                    character,
                    '\'' | '"' | '`' | ',' | ';' | '(' | ')' | '[' | ']'
                )
        })
        .trim_start_matches("./")
        .trim_end_matches([':', ',', ';']);
    if candidate.is_empty()
        || candidate.starts_with('-')
        || candidate.contains("://")
        || !candidate.contains('/')
    {
        return None;
    }

    let components = Path::new(candidate)
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .filter(|component| !component.is_empty() && *component != "/")
        .collect::<Vec<_>>();
    const REPOSITORY_AREAS: &[&str] = &[
        "apps", "crates", "packages", ".github", "docs", "scripts", "tests", "src",
    ];
    let (index, area) = components
        .iter()
        .enumerate()
        .find(|(_, component)| REPOSITORY_AREAS.contains(component))?;

    if matches!(*area, "apps" | "crates" | "packages") {
        let child = components.get(index + 1).copied().unwrap_or_default();
        if !child.is_empty() && !child.contains(['*', '?', '[', '{']) {
            return Some(format!("{area}/{child}"));
        }
    }
    Some((*area).to_string())
}

#[derive(Debug, Default)]
pub struct ProgressLedger {
    mutation_epoch: u64,
    seen_evidence: HashSet<String>,
    seen_changed_effects: HashSet<String>,
    seen_unverified_effects: HashSet<String>,
    outcome_variants: HashMap<String, HashSet<String>>,
    outcome_intents: HashMap<String, HashSet<String>>,
    consecutive_unverified_effect_turns: usize,
    no_progress_turns: usize,
}

impl ProgressLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset_for_steering(&mut self) {
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
        self.seen_evidence.clear();
        self.seen_changed_effects.clear();
        self.seen_unverified_effects.clear();
        self.outcome_variants.clear();
        self.outcome_intents.clear();
        self.consecutive_unverified_effect_turns = 0;
        self.no_progress_turns = 0;
    }

    /// Record one completed tool turn and return structured telemetry when a
    /// passive turn repeated existing evidence. Successful mutations and
    /// control actions are the only events that reset the evidence window.
    pub fn record_turn(
        &mut self,
        tool_calls: &[AiToolCall],
        tool_results: &[Content],
    ) -> Option<ProgressGuardTelemetry> {
        if tool_calls.is_empty() {
            return None;
        }

        let fingerprints = tool_calls
            .iter()
            .map(action_fingerprint)
            .collect::<Vec<_>>();
        let results = results_by_call_id(tool_results);

        let mut evidence_candidates = Vec::new();
        let mut action_classes = Vec::new();
        let mut discovered_new_changed_effect = false;
        let mut discovered_new_unverified_effect = false;

        for (call, fingerprint) in tool_calls.iter().zip(&fingerprints) {
            let Some(result) = results.get(call.id.as_str()) else {
                continue;
            };
            // Failed mutations and validations have dedicated outcome-aware
            // guards. Do not let this ledger race or double-count them.
            if result.is_error {
                continue;
            }

            let producer_declares_effect = result.changed.is_some();
            let effect_class = matches!(
                fingerprint.class,
                ActionClass::Mutate | ActionClass::Communicate | ActionClass::Control
            );
            let evidence_class = matches!(
                fingerprint.class,
                ActionClass::Observe | ActionClass::Delegate | ActionClass::Validate
            );
            if !producer_declares_effect && !effect_class && !evidence_class {
                continue;
            }

            action_classes.push(if result.changed == Some(true) {
                ActionClass::Mutate
            } else {
                fingerprint.class
            });

            // A physical delta is semantic progress only once for the same
            // normalized intent in the current steering window. This contains
            // timestamp churn, random rewrites, and repeated background starts
            // without reintroducing an arbitrary global turn cap.
            let effect_identity = producer_effect_identity(call, fingerprint, result);
            if result.changed == Some(true) && self.seen_changed_effects.insert(effect_identity) {
                discovered_new_changed_effect = true;
            } else if result.changed.is_none()
                && effect_class
                && self
                    .seen_unverified_effects
                    .insert(fingerprint.intent.clone())
            {
                discovered_new_unverified_effect = true;
            }

            // Intent spelling never proves progress by itself. Successful
            // observations, delegated work, and validations need a materially
            // new outcome. Opaque effects do not get outcome credit: a command
            // can manufacture arbitrary random stdout without advancing the
            // task, so it must be followed by independently observed evidence.
            if evidence_class && result.changed != Some(true) {
                let (effective_name, _) = effective_tool_call(&call.name, &call.arguments);
                evidence_candidates.push((
                    fingerprint.intent.clone(),
                    outcome_evidence_key(call, result),
                    matches!(effective_name, "bash" | "shell" | "execute"),
                ));
            }
        }

        if action_classes.is_empty() {
            return None;
        }

        evidence_candidates.sort();
        evidence_candidates.dedup();
        if discovered_new_changed_effect {
            self.mutation_epoch = self.mutation_epoch.saturating_add(1);
            self.seen_evidence.clear();
            self.outcome_variants.clear();
            self.outcome_intents.clear();
            self.seen_unverified_effects.clear();
            self.consecutive_unverified_effect_turns = 0;
        }

        let mut accepted_evidence = Vec::new();
        for (intent, evidence, bound_cross_intent_churn) in &evidence_candidates {
            let evidence_identity = format!("{intent}:{evidence}");
            if self.seen_evidence.contains(&evidence_identity) {
                continue;
            }
            let variants = self.outcome_variants.entry(intent.clone()).or_default();
            if variants.len() >= MAX_OUTCOME_VARIANTS_PER_INTENT {
                continue;
            }
            if *bound_cross_intent_churn {
                let intents = self.outcome_intents.entry(evidence.clone()).or_default();
                if intents.len() >= MAX_INTENTS_PER_IDENTICAL_OUTCOME {
                    continue;
                }
                intents.insert(evidence_identity.clone());
            }
            variants.insert(evidence.clone());
            self.seen_evidence.insert(evidence_identity.clone());
            accepted_evidence.push(evidence_identity);
        }

        if discovered_new_changed_effect {
            self.no_progress_turns = 0;
            return None;
        }

        if !accepted_evidence.is_empty() {
            self.seen_unverified_effects.clear();
            self.consecutive_unverified_effect_turns = 0;
            self.no_progress_turns = 0;
            return None;
        }

        if discovered_new_unverified_effect {
            self.consecutive_unverified_effect_turns =
                self.consecutive_unverified_effect_turns.saturating_add(1);
            if self.consecutive_unverified_effect_turns <= MAX_CONSECUTIVE_UNVERIFIED_EFFECT_TURNS {
                self.no_progress_turns = 0;
                return None;
            }
        }

        self.no_progress_turns = self.no_progress_turns.saturating_add(1);
        action_classes.sort_by_key(|class| action_class_order(*class));
        action_classes.dedup();

        // When nothing new was accepted, fingerprint the rejected candidates
        // and action classes so telemetry is diagnostic instead of the empty
        // SHA-256 that previously made every no-progress stop look identical.
        let diagnostic = if accepted_evidence.is_empty() {
            let mut parts = action_classes
                .iter()
                .map(|class| format!("{class:?}"))
                .collect::<Vec<_>>();
            for (intent, evidence, _) in &evidence_candidates {
                parts.push(format!("{intent}:{evidence}"));
            }
            parts.sort();
            parts.dedup();
            parts.join("|")
        } else {
            accepted_evidence.join("|")
        };

        Some(self.telemetry(action_classes, &diagnostic))
    }

    fn telemetry(
        &self,
        action_classes: Vec<ActionClass>,
        evidence: &str,
    ) -> ProgressGuardTelemetry {
        ProgressGuardTelemetry {
            guard: "semantic_no_progress".to_string(),
            no_progress_turns: self.no_progress_turns,
            threshold: NO_PROGRESS_TURN_THRESHOLD,
            mutation_epoch: self.mutation_epoch,
            action_classes,
            evidence_signature: hash_text(evidence),
            action: if self.no_progress_turns >= NO_PROGRESS_TURN_THRESHOLD {
                ProgressGuardAction::Stop
            } else if self.no_progress_turns >= NO_PROGRESS_REPLAN_THRESHOLD {
                ProgressGuardAction::Replan
            } else {
                ProgressGuardAction::Warn
            },
            triggered: self.no_progress_turns >= NO_PROGRESS_TURN_THRESHOLD,
        }
    }
}

fn producer_effect_identity(
    call: &AiToolCall,
    fingerprint: &ActionFingerprint,
    result: &ResultEvidence,
) -> String {
    let Some(change_key) = result.progress_change_key.as_deref() else {
        return fingerprint.intent.clone();
    };
    let (effective_name, _) = effective_tool_call(&call.name, &call.arguments);

    // Structured editors publish a trusted target-surface key, while their
    // normalized input identifies the exact requested state transition. A
    // second legitimate patch to the same file is therefore new progress,
    // but replaying the exact patch is not. Free-form shell keeps the narrower
    // target-only identity so timestamp/random-content churn cannot evade
    // convergence by spelling the command differently.
    if matches!(
        effective_name,
        "apply_patch" | "edit" | "multiedit" | "write"
    ) {
        format!("{change_key}:{}", fingerprint.intent)
    } else {
        change_key.to_string()
    }
}

#[derive(Debug, Clone)]
struct ResultEvidence {
    is_error: bool,
    output: Value,
    signature: String,
    status_signature: String,
    changed: Option<bool>,
    progress_change_key: Option<String>,
}

fn results_by_call_id(tool_results: &[Content]) -> HashMap<&str, ResultEvidence> {
    tool_results
        .iter()
        .filter_map(|result| match result {
            Content::ToolResult {
                tool_use_id,
                output,
                is_error,
            } => Some((
                tool_use_id.as_str(),
                ResultEvidence {
                    is_error: is_error.unwrap_or(false),
                    output: output.clone(),
                    signature: hash_value(output),
                    status_signature: stable_status_signature(output, is_error.unwrap_or(false)),
                    changed: changed_value(output),
                    progress_change_key: progress_change_key(output),
                },
            )),
            _ => None,
        })
        .collect()
}

fn outcome_evidence_key(call: &AiToolCall, result: &ResultEvidence) -> String {
    let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
    let signature = if matches!(name, "bash" | "shell" | "execute") {
        let background = arguments
            .get("run_in_background")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if background {
            result.status_signature.clone()
        } else {
            bash_output_signature(result)
        }
    } else {
        result.signature.clone()
    };
    format!("{name}:{signature}")
}

fn bash_output_signature(result: &ResultEvidence) -> String {
    let output = extract_output_text(&result.output).unwrap_or_default();
    hash_text(&format!("{}:{output}", result.status_signature))
}

fn extract_output_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|decoded| extract_output_text(&decoded))
            .or_else(|| Some(text.clone())),
        Value::Object(object) => {
            // History packaging for bash stores stdout under `output_preview`
            // (see history_policy/summaries). Prefer that over the constant
            // success summary so status polls with changing stdout remain
            // distinct evidence instead of collapsing to empty/no-progress.
            const PREFERRED_KEYS: &[&str] = &[
                "output",
                "output_preview",
                "data",
                "result",
                "stdout",
                "summary",
            ];
            PREFERRED_KEYS
                .iter()
                .filter_map(|key| object.get(*key))
                .find_map(|nested| {
                    // Prefer nested string/preview leaves over recursing into
                    // objects that only re-export a constant summary.
                    match nested {
                        Value::String(text) if !text.is_empty() => Some(text.clone()),
                        other => extract_output_text(other),
                    }
                })
        }
        Value::Array(values) => {
            let parts = values
                .iter()
                .filter_map(extract_output_text)
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join("\n"))
        }
        Value::Null => None,
        scalar => Some(scalar.to_string()),
    }
}

fn stable_status_signature(output: &Value, is_error: bool) -> String {
    fn find<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
        let object = value.as_object()?;
        object.get(name).or_else(|| {
            ["metadata", "result", "data", "summary"]
                .into_iter()
                .filter_map(|key| object.get(key))
                .find_map(|nested| find(nested, name))
        })
    }

    let stable = serde_json::json!({
        "is_error": is_error,
        "error_code": find(output, "error_code"),
        "exit_code": find(output, "exit_code"),
        "killed": find(output, "killed"),
        "status": find(output, "status"),
        "process_error": find(output, "process_error"),
        "reused_existing": find(output, "reused_existing"),
    });
    hash_value(&stable)
}

fn changed_value(output: &Value) -> Option<bool> {
    trusted_changed(output)
}

fn progress_change_key(output: &Value) -> Option<String> {
    match output {
        Value::Object(object) => object
            .get("progress_change_key")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        Value::String(serialized) => serde_json::from_str::<Value>(serialized)
            .ok()
            .as_ref()
            .and_then(progress_change_key),
        _ => None,
    }
}

pub fn action_fingerprint(call: &AiToolCall) -> ActionFingerprint {
    let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
    let class = action_class(name, arguments, call);
    let normalized = if matches!(name, "bash" | "shell" | "execute") {
        let command = arguments
            .get("command")
            .and_then(Value::as_str)
            .map(semantic_bash_signature)
            .unwrap_or_else(|| "missing-command".to_string());
        let mode = if arguments
            .get("run_in_background")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "background"
        } else {
            "foreground"
        };
        format!("{mode}:{command}")
    } else {
        normalized_arguments(name, arguments)
    };

    ActionFingerprint {
        class,
        intent: hash_text(&format!("{name}:{normalized}")),
    }
}

fn action_class(name: &str, arguments: &Value, call: &AiToolCall) -> ActionClass {
    if failure::is_validation_call(call) {
        return ActionClass::Validate;
    }

    match name {
        "bash" | "shell" | "execute" => {
            let background = arguments
                .get("run_in_background")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let mutates = background
                || arguments
                    .get("command")
                    .and_then(Value::as_str)
                    .is_some_and(|command| {
                        classify_bash_command(command).modifies_filesystem_or_process
                    });
            if mutates {
                ActionClass::Mutate
            } else {
                ActionClass::Observe
            }
        }
        "agent" => match agent_call_action(arguments) {
            "list" | "status" | "wait" => ActionClass::Observe,
            "message" | "followup" => ActionClass::Communicate,
            "interrupt" => ActionClass::Control,
            _ if agent_call_requests_write(arguments) => ActionClass::Mutate,
            _ => ActionClass::Delegate,
        },
        "send_user_message" | "report" => ActionClass::Communicate,
        "set_work_mode" | "enter_plan_mode" | "workflow_propose" | "workflow_update"
        | "task_start" | "task_complete" | "add_subtask" | "set_dependency" | "sleep" => {
            ActionClass::Control
        }
        _ => match tool_policy_for_call(name, arguments).category {
            ToolCategory::ReadOnly => ActionClass::Observe,
            ToolCategory::Write => ActionClass::Mutate,
            ToolCategory::Interactive => ActionClass::Control,
        },
    }
}

fn normalized_arguments(name: &str, arguments: &Value) -> String {
    let mut normalized = arguments.clone();
    if let Some(object) = normalized.as_object_mut() {
        // Output size/presentation changes can reveal more evidence, which is
        // captured by the result signature. They are not a new action intent.
        match name {
            "list" => {
                object.remove("limit");
            }
            "glob" => {
                object.remove("max_results");
            }
            _ => {}
        }
    }
    normalized.to_string()
}

fn hash_value(value: &Value) -> String {
    hash_text(&value.to_string())
}

fn hash_text(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

const fn action_class_order(class: ActionClass) -> u8 {
    match class {
        ActionClass::Observe => 0,
        ActionClass::Mutate => 1,
        ActionClass::Validate => 2,
        ActionClass::Delegate => 3,
        ActionClass::Communicate => 4,
        ActionClass::Control => 5,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn call(id: &str, name: &str, arguments: Value) -> AiToolCall {
        AiToolCall {
            id: id.to_string(),
            name: name.to_string(),
            arguments,
        }
    }

    fn result(id: &str, output: &str, is_error: bool) -> Content {
        Content::ToolResult {
            tool_use_id: id.to_string(),
            output: Value::String(output.to_string()),
            is_error: is_error.then_some(true),
        }
    }

    fn result_with_changed(id: &str, changed: bool) -> Content {
        Content::ToolResult {
            tool_use_id: id.to_string(),
            output: json!({ "ok": true, "changed": changed }),
            is_error: None,
        }
    }

    fn result_with_changed_key(id: &str, key: &str) -> Content {
        Content::ToolResult {
            tool_use_id: id.to_string(),
            output: json!({
                "ok": true,
                "changed": true,
                "progress_change_key": key
            }),
            is_error: None,
        }
    }

    #[test]
    fn nested_payload_cannot_spoof_producer_change_metadata() {
        assert_eq!(changed_value(&json!({"data": {"changed": true}})), None);
        assert_eq!(
            progress_change_key(&json!({
                "data": {"progress_change_key": "untrusted-nested-key"}
            })),
            None
        );
    }

    #[test]
    fn read_only_and_noop_bash_are_observations_not_mutations() {
        for command in [
            "pwd",
            "git status --short",
            "rg needle src",
            "true",
            "gh run view 123",
            "gh pr view 45 --json status",
        ] {
            let fingerprint =
                action_fingerprint(&call("bash-1", "bash", json!({ "command": command })));
            assert_eq!(fingerprint.class, ActionClass::Observe, "{command}");
        }
    }

    #[test]
    fn broad_multi_area_observation_gets_one_delegation_nudge() {
        let mut tracker = DelegationNudgeTracker::new();
        let turns = [
            ("read-1", "crates/mitsuro-core/src/agent/orchestrator.rs"),
            ("read-2", "apps/mobile/src/app.tsx"),
            ("read-3", ".github/workflows/ci.yml"),
        ];

        for (index, (id, path)) in turns.into_iter().enumerate() {
            let calls = [call(id, "read", json!({"file_path": path}))];
            let results = [result(id, "observed", false)];
            let nudge = tracker.record_turn(&calls, &results, DelegationMode::Balanced, true);
            if index + 1 < DELEGATION_NUDGE_TURN_THRESHOLD {
                assert!(nudge.is_none());
            } else {
                let nudge = nudge.expect("broad sustained observation should nudge");
                assert!(nudge.contains("named `agent` child"));
                assert!(
                    nudge.contains("`run_in_background=true` only when that field is available")
                );
                assert!(nudge.contains("run the named child in the foreground"));
            }
        }

        let calls = [call(
            "read-4",
            "read",
            json!({"file_path": "docs/architecture.md"}),
        )];
        assert!(
            tracker
                .record_turn(
                    &calls,
                    &[result("read-4", "new architectural evidence", false)],
                    DelegationMode::Balanced,
                    true,
                )
                .is_none(),
            "productive investigation must not force synthesis"
        );
    }

    #[test]
    fn local_observation_does_not_force_delegation_and_mutation_resets_it() {
        let mut tracker = DelegationNudgeTracker::new();
        for index in 0..5 {
            let id = format!("read-{index}");
            let calls = [call(
                &id,
                "read",
                json!({"file_path": format!("crates/mitsuro-core/src/agent/file_{index}.rs")}),
            )];
            assert!(tracker
                .record_turn(
                    &calls,
                    &[result(&id, "observed", false)],
                    DelegationMode::Balanced,
                    true
                )
                .is_none());
        }

        let edit = [call(
            "edit-1",
            "edit",
            json!({"file_path": "crates/mitsuro-core/src/agent/file.rs"}),
        )];
        assert!(tracker
            .record_turn(
                &edit,
                &[result("edit-1", "changed", false)],
                DelegationMode::Balanced,
                true
            )
            .is_none());

        let calls = [call(
            "read-new",
            "read",
            json!({"file_path": "apps/mobile/src/app.tsx"}),
        )];
        assert!(tracker
            .record_turn(
                &calls,
                &[result("read-new", "observed", false)],
                DelegationMode::Balanced,
                true
            )
            .is_none());
    }

    #[test]
    fn delegation_advice_respects_policy_and_current_capabilities() {
        for (mode, can_delegate) in [
            (DelegationMode::ExplicitOnly, true),
            (DelegationMode::Balanced, false),
            (DelegationMode::Proactive, false),
            (DelegationMode::Orchestrator, false),
        ] {
            let mut tracker = DelegationNudgeTracker::new();
            for index in 0..8 {
                let id = format!("read-{index}");
                let calls = [call(
                    &id,
                    "read",
                    json!({"file_path": format!("crates/area_{index}/src/lib.rs")}),
                )];
                assert!(
                    tracker
                        .record_turn(
                            &calls,
                            &[result(&id, &format!("new evidence {index}"), false)],
                            mode,
                            can_delegate,
                        )
                        .is_none(),
                    "no policy-inconsistent advice: {mode:?}/{can_delegate}"
                );
            }
        }
    }

    #[test]
    fn productive_multi_area_audit_remains_live_after_advisory_checkpoint() {
        let mut tracker = DelegationNudgeTracker::new();
        let mut guard = LoopGuard::new();
        let mut nudges = 0;
        for index in 0..20 {
            let id = format!("audit-{index}");
            let calls = [call(
                &id,
                "read",
                json!({"file_path": format!("crates/area_{index}/src/lib.rs")}),
            )];
            let results = [result(&id, &format!("distinct finding {index}"), false)];
            let outcome = guard.evaluate(&calls, &results);
            assert!(outcome.progress.is_none());
            assert!(outcome.repeated_failure.is_none());
            assert!(outcome.repeated_read_only.is_none());
            nudges += usize::from(
                tracker
                    .record_turn(&calls, &results, DelegationMode::Balanced, true)
                    .is_some(),
            );
        }
        assert_eq!(nudges, 1);

        // Explicit steering opens a fresh sequence; the advice itself never
        // consumes the user's permission or becomes a terminal loop decision.
        tracker.reset_for_steering();
        assert!(!tracker.nudged);
        assert!(tracker.observed_areas.is_empty());
    }

    #[test]
    fn history_envelope_output_preview_counts_as_bash_evidence() {
        let mut ledger = ProgressLedger::new();
        let first = call("b1", "bash", json!({ "command": "git status --short" }));
        let second = call("b2", "bash", json!({ "command": "git status --short" }));

        let envelope_a = json!({
            "summary": "bash completed successfully (exit 0)",
            "result": {
                "exit_code": 0,
                "output_preview": " M file_a.rs\n"
            }
        });
        let envelope_b = json!({
            "summary": "bash completed successfully (exit 0)",
            "result": {
                "exit_code": 0,
                "output_preview": " M file_b.rs\n?? new.rs\n"
            }
        });

        assert!(ledger
            .record_turn(
                std::slice::from_ref(&first),
                &[Content::ToolResult {
                    tool_use_id: "b1".into(),
                    output: envelope_a,
                    is_error: None,
                }],
            )
            .is_none());

        // Different porcelain content must count as new evidence, not no-progress.
        assert!(ledger
            .record_turn(
                &[second],
                &[Content::ToolResult {
                    tool_use_id: "b2".into(),
                    output: envelope_b,
                    is_error: None,
                }],
            )
            .is_none());
    }

    #[test]
    fn no_progress_telemetry_is_not_empty_hash_when_candidates_exist() {
        let mut ledger = ProgressLedger::new();
        let call_a = call("b1", "bash", json!({ "command": "git status --short" }));
        let envelope = json!({
            "summary": "bash completed successfully (exit 0)",
            "result": { "exit_code": 0, "output_preview": "clean\n" }
        });
        assert!(ledger
            .record_turn(
                std::slice::from_ref(&call_a),
                &[Content::ToolResult {
                    tool_use_id: "b1".into(),
                    output: envelope.clone(),
                    is_error: None,
                }],
            )
            .is_none());

        // Same evidence again → no-progress, but signature must not be empty SHA.
        let empty_sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        for index in 1..=NO_PROGRESS_TURN_THRESHOLD {
            let id = format!("b{index}");
            let telemetry = ledger
                .record_turn(
                    &[call(
                        &id,
                        "bash",
                        json!({ "command": "git status --short" }),
                    )],
                    &[Content::ToolResult {
                        tool_use_id: id,
                        output: envelope.clone(),
                        is_error: None,
                    }],
                )
                .expect("repeated identical status is no-progress");
            assert_ne!(telemetry.evidence_signature, empty_sha);
            assert_eq!(telemetry.no_progress_turns, index);
        }
    }

    #[test]
    fn cosmetic_bash_variants_have_the_same_intent() {
        let first = action_fingerprint(&call(
            "bash-1",
            "bash",
            json!({
                "command": "pwd && rg -n 'needle' src | head -n 20",
                "timeout": 30_000,
                "description": "first look"
            }),
        ));
        let second = action_fingerprint(&call(
            "bash-2",
            "bash",
            json!({
                "command": "rg --line-number 'needle' src | head -50",
                "timeout": 60_000,
                "description": "try again"
            }),
        ));

        assert_eq!(first, second);
    }

    #[test]
    fn path_and_invocation_cosmetics_do_not_evade_bash_intent() {
        let calls = [
            "rg -n TODO src",
            "rg --line-number 'TODO' ./src/",
            "cd . && command rg -n TODO src/",
        ];
        let fingerprints = calls
            .into_iter()
            .map(|command| action_fingerprint(&call("bash", "bash", json!({ "command": command }))))
            .collect::<Vec<_>>();

        assert!(fingerprints.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn effectful_shell_cosmetics_do_not_evade_bash_intent() {
        let calls = [
            "mkdir -p out",
            "mkdir  -p out",
            "mkdir -p ./out",
            "command mkdir -p out",
            "true && mkdir -p out",
        ];
        let fingerprints = calls
            .into_iter()
            .map(|command| action_fingerprint(&call("bash", "bash", json!({ "command": command }))))
            .collect::<Vec<_>>();

        assert!(fingerprints.windows(2).all(|pair| pair[0] == pair[1]));
        assert!(fingerprints
            .iter()
            .all(|fingerprint| fingerprint.class == ActionClass::Mutate));
    }

    #[test]
    fn interpreter_and_background_bash_calls_are_effectful() {
        for command in [
            "python3 -c 'open(\"out\", \"w\").write(\"x\")'",
            "node -e 'require(\"fs\").writeFileSync(\"out\", \"x\")'",
            "sh -c 'touch out'",
            "echo ok\ntouch out",
            "printf '%s' ok\nrm -f victim",
        ] {
            assert_eq!(
                action_fingerprint(&call("bash", "bash", json!({"command": command}))).class,
                ActionClass::Mutate,
                "{command}"
            );
        }

        let foreground = action_fingerprint(&call(
            "foreground",
            "bash",
            json!({"command": "ps -axo pid,command"}),
        ));
        let background = action_fingerprint(&call(
            "background",
            "bash",
            json!({"command": "ps -axo pid,command", "run_in_background": true}),
        ));
        assert_eq!(foreground.class, ActionClass::Observe);
        assert_eq!(background.class, ActionClass::Mutate);
        assert_ne!(foreground.intent, background.intent);
    }

    #[test]
    fn repeated_semantic_observations_converge_after_three_no_progress_turns() {
        let mut ledger = ProgressLedger::new();
        let commands = [
            "rg -n needle src | head -20",
            "pwd && rg --line-number needle src | head -50",
            "rg -n needle src | head -100",
            "rg --line-number needle src",
        ];

        for (index, command) in commands.iter().enumerate() {
            let id = format!("bash-{index}");
            let calls = vec![call(&id, "bash", json!({ "command": command }))];
            let results = vec![result(&id, "same evidence", false)];
            let telemetry = ledger.record_turn(&calls, &results);
            if index == 0 {
                assert!(telemetry.is_none());
            } else if index < 3 {
                let telemetry = telemetry.expect("repeat telemetry");
                assert!(!telemetry.triggered);
                assert_eq!(
                    telemetry.action,
                    if index == 2 {
                        ProgressGuardAction::Replan
                    } else {
                        ProgressGuardAction::Warn
                    }
                );
            } else {
                let telemetry = telemetry.expect("guard telemetry");
                assert!(telemetry.triggered);
                assert_eq!(telemetry.action, ProgressGuardAction::Stop);
                assert_eq!(telemetry.no_progress_turns, 3);
                assert!(telemetry.diagnostic().is_some());
            }
        }
    }

    #[test]
    fn changed_evidence_and_positive_mutation_reset_no_progress() {
        let mut ledger = ProgressLedger::new();
        let read = vec![call("read-1", "read", json!({ "file_path": "src/lib.rs" }))];
        assert!(ledger
            .record_turn(&read, &[result("read-1", "before", false)])
            .is_none());
        assert!(ledger
            .record_turn(&read, &[result("read-1", "before", false)])
            .is_some());
        assert!(ledger
            .record_turn(&read, &[result("read-1", "after", false)])
            .is_none());

        let write = vec![call(
            "write-1",
            "write",
            json!({ "file_path": "src/lib.rs", "content": "after" }),
        )];
        assert!(ledger
            .record_turn(&write, &[result_with_changed("write-1", true)])
            .is_none());
        assert!(ledger
            .record_turn(&read, &[result("read-1", "after", false)])
            .is_none());
    }

    #[test]
    fn verified_mutation_invalidates_pre_change_validation_failures() {
        let validation = |id: &str| {
            call(
                id,
                "bash",
                json!({ "command": "cargo test -p app focused_regression" }),
            )
        };
        let mut guard = LoopGuard::new();

        let first = validation("validate-before");
        let first_result = result("validate-before", "compile error E0308", true);
        assert!(guard
            .evaluate(std::slice::from_ref(&first), &[first_result])
            .repeated_failure
            .is_none());

        let mutation = call(
            "fix",
            "apply_patch",
            json!({ "patch": "*** Begin Patch\n*** End Patch" }),
        );
        assert!(guard
            .evaluate(
                std::slice::from_ref(&mutation),
                &[result_with_changed("fix", true)],
            )
            .repeated_failure
            .is_none());

        let rerun = validation("validate-after-1");
        let rerun_result = result("validate-after-1", "compile error E0308", true);
        assert!(guard
            .evaluate(std::slice::from_ref(&rerun), &[rerun_result])
            .repeated_failure
            .is_none());

        let repeated = validation("validate-after-2");
        let repeated_result = result("validate-after-2", "compile error E0308", true);
        assert!(guard
            .evaluate(std::slice::from_ref(&repeated), &[repeated_result])
            .repeated_failure
            .is_some());
    }

    #[test]
    fn explicit_noop_mutations_trigger_after_three_attempts() {
        let mut ledger = ProgressLedger::new();

        for attempt in 1..=3 {
            let id = format!("bash-{attempt}");
            let calls = vec![call(
                &id,
                "bash",
                json!({ "command": "mkdir -p already-present" }),
            )];
            let telemetry = ledger
                .record_turn(&calls, &[result_with_changed(&id, false)])
                .expect("explicit no-op must produce telemetry");
            assert_eq!(telemetry.no_progress_turns, attempt);
            assert_eq!(telemetry.triggered, attempt == 3);
            assert_eq!(
                telemetry.action,
                match attempt {
                    1 => ProgressGuardAction::Warn,
                    2 => ProgressGuardAction::Replan,
                    _ => ProgressGuardAction::Stop,
                }
            );
        }
    }

    #[test]
    fn distinct_producer_change_keys_progress_and_exact_repeats_converge() {
        let mut ledger = ProgressLedger::new();
        for (index, (command, change_key)) in [
            ("echo a > a.txt", "target-a"),
            ("echo b > b.txt", "target-b"),
            ("echo c > c.txt", "target-c"),
            ("echo d > d.txt", "target-d"),
        ]
        .into_iter()
        .enumerate()
        {
            let id = format!("write-{index}");
            assert!(
                ledger
                    .record_turn(
                        &[call(&id, "bash", json!({"command": command}))],
                        &[result_with_changed_key(&id, change_key)],
                    )
                    .is_none(),
                "distinct mutation must be progress: {command}"
            );
        }

        for (attempt, expected) in [
            ProgressGuardAction::Warn,
            ProgressGuardAction::Replan,
            ProgressGuardAction::Stop,
        ]
        .into_iter()
        .enumerate()
        {
            let id = format!("repeat-{attempt}");
            let telemetry = ledger
                .record_turn(
                    &[call(
                        &id,
                        "bash",
                        json!({"command": format!("echo repeat-{attempt} > d.txt")}),
                    )],
                    &[result_with_changed_key(&id, "target-d")],
                )
                .expect("a repeated producer change target must converge");
            assert_eq!(telemetry.action, expected);
            assert_eq!(telemetry.triggered, expected == ProgressGuardAction::Stop);
        }
    }

    #[test]
    fn distinct_structured_edits_to_same_target_are_progress() {
        let mut ledger = ProgressLedger::new();
        for (index, replacement) in ["first", "second", "third"].into_iter().enumerate() {
            let id = format!("edit-{index}");
            let calls = vec![call(
                &id,
                "edit",
                json!({
                    "file_path": "vite.config.ts",
                    "old_string": "before",
                    "new_string": replacement,
                }),
            )];
            assert!(
                ledger
                    .record_turn(
                        &calls,
                        &[result_with_changed_key(&id, "vite-config-target")],
                    )
                    .is_none(),
                "a different structured edit to the same file is a new state transition"
            );
        }
    }

    #[test]
    fn exact_structured_edit_replay_still_converges() {
        let mut ledger = ProgressLedger::new();
        for attempt in 1..=NO_PROGRESS_TURN_THRESHOLD + 1 {
            let id = format!("edit-{attempt}");
            let calls = vec![call(
                &id,
                "edit",
                json!({
                    "file_path": "vite.config.ts",
                    "old_string": "before",
                    "new_string": "after",
                }),
            )];
            let telemetry = ledger.record_turn(
                &calls,
                &[result_with_changed_key(&id, "vite-config-target")],
            );
            if attempt == 1 {
                assert!(telemetry.is_none());
            } else {
                let telemetry = telemetry.expect("an exact structured replay must converge");
                assert_eq!(telemetry.no_progress_turns, attempt - 1);
                assert_eq!(
                    telemetry.triggered,
                    attempt - 1 == NO_PROGRESS_TURN_THRESHOLD
                );
            }
        }
    }

    #[test]
    fn opaque_mutation_batches_do_not_gain_novelty_from_order_or_multiplicity() {
        let mut ledger = ProgressLedger::new();
        let batch = |prefix: &str, commands: &[&str]| {
            let calls = commands
                .iter()
                .enumerate()
                .map(|(index, command)| {
                    call(
                        &format!("{prefix}-{index}"),
                        "bash",
                        json!({"command": command}),
                    )
                })
                .collect::<Vec<_>>();
            let results = calls
                .iter()
                .map(|call| result(&call.id, "ok", false))
                .collect::<Vec<_>>();
            (calls, results)
        };

        let (first_calls, first_results) = batch("first", &["touch a", "touch b"]);
        assert!(ledger.record_turn(&first_calls, &first_results).is_none());

        let (reordered_calls, reordered_results) = batch("reordered", &["touch b", "touch a"]);
        let telemetry = ledger
            .record_turn(&reordered_calls, &reordered_results)
            .expect("reordering known opaque effects is not progress");
        assert_eq!(telemetry.action, ProgressGuardAction::Warn);

        let (repeated_calls, repeated_results) = batch("repeat", &["touch b", "touch a"]);
        let telemetry = ledger
            .record_turn(&repeated_calls, &repeated_results)
            .expect("repeating an ordered opaque effect sequence is not progress");
        assert_eq!(telemetry.action, ProgressGuardAction::Replan);

        let (duplicate_calls, duplicate_results) = batch("duplicate", &["touch a", "touch a"]);
        let telemetry = ledger
            .record_turn(&duplicate_calls, &duplicate_results)
            .expect("multiplicity alone must not create a new effect sequence");
        assert_eq!(telemetry.action, ProgressGuardAction::Stop);
        assert!(telemetry.triggered);
    }

    #[test]
    fn growing_single_effect_batches_cannot_evade_guard() {
        let mut ledger = ProgressLedger::new();

        for (turn, multiplicity) in (1..=4).enumerate() {
            let calls = (0..multiplicity)
                .map(|index| {
                    call(
                        &format!("turn-{turn}-{index}"),
                        "bash",
                        json!({"command": "touch same"}),
                    )
                })
                .collect::<Vec<_>>();
            let results = calls
                .iter()
                .map(|call| result(&call.id, "ok", false))
                .collect::<Vec<_>>();
            let telemetry = ledger.record_turn(&calls, &results);

            if turn == 0 {
                assert!(telemetry.is_none(), "the first opaque effect is novel");
            } else {
                let telemetry = telemetry.expect("multiplicity-only turn must be no-progress");
                assert_eq!(telemetry.no_progress_turns, turn);
                assert_eq!(telemetry.triggered, turn == 3);
            }
        }
    }

    #[test]
    fn explicit_no_change_overrides_novel_mutation_intent() {
        let mut ledger = ProgressLedger::new();
        for (index, command) in ["mkdir one", "mkdir two", "mkdir three"]
            .into_iter()
            .enumerate()
        {
            let id = format!("noop-{index}");
            let telemetry = ledger
                .record_turn(
                    &[call(&id, "bash", json!({"command": command}))],
                    &[result_with_changed(&id, false)],
                )
                .expect("producer-declared no-change must not count as progress");
            assert_eq!(telemetry.no_progress_turns, index + 1);
            assert_eq!(telemetry.triggered, index == 2);
        }
    }

    #[test]
    fn repeated_opaque_mutation_cannot_evade_guard_with_volatile_output() {
        let mut ledger = ProgressLedger::new();

        for attempt in 0..=3 {
            let id = format!("bash-{attempt}");
            let calls = vec![call(
                &id,
                "bash",
                json!({ "command": "mkdir -p already-present" }),
            )];
            let results = vec![result(
                &id,
                &format!("ok pid={} timestamp={attempt}", 10_000 + attempt),
                false,
            )];
            let telemetry = ledger.record_turn(&calls, &results);
            if attempt == 0 {
                assert!(telemetry.is_none());
            } else if attempt < 3 {
                assert!(!telemetry.expect("warning").triggered);
            } else {
                assert!(telemetry.expect("terminal guard").triggered);
            }
        }
    }

    #[test]
    fn shell_parameter_churn_is_unverified_and_converges() {
        let mut ledger = ProgressLedger::new();
        for attempt in 0..=NO_PROGRESS_TURN_THRESHOLD {
            let id = format!("random-{attempt}");
            let calls = vec![call(&id, "bash", json!({"command": "echo \"$RANDOM\""}))];
            assert_eq!(action_fingerprint(&calls[0]).class, ActionClass::Mutate);
            let telemetry =
                ledger.record_turn(&calls, &[result(&id, &format!("random-{attempt}"), false)]);
            if attempt == 0 {
                assert!(telemetry.is_none());
            } else {
                let telemetry = telemetry.expect("random stdout must not prove progress");
                assert_eq!(telemetry.no_progress_turns, attempt);
                assert_eq!(telemetry.triggered, attempt == NO_PROGRESS_TURN_THRESHOLD);
            }
        }
    }

    #[test]
    fn distinct_opaque_effects_require_evidence_after_a_bounded_runway() {
        let mut ledger = ProgressLedger::new();
        let total = MAX_CONSECUTIVE_UNVERIFIED_EFFECT_TURNS + NO_PROGRESS_TURN_THRESHOLD;

        for attempt in 0..total {
            let id = format!("opaque-{attempt}");
            let telemetry = ledger.record_turn(
                &[call(
                    &id,
                    "bash",
                    json!({"command": format!("sh -c 'printf step-{attempt}'")}),
                )],
                &[result(&id, "ok", false)],
            );
            if attempt < MAX_CONSECUTIVE_UNVERIFIED_EFFECT_TURNS {
                assert!(telemetry.is_none(), "bounded build runway: {attempt}");
            } else {
                let telemetry = telemetry.expect("unverified effect streak must converge");
                let no_progress = attempt - MAX_CONSECUTIVE_UNVERIFIED_EFFECT_TURNS + 1;
                assert_eq!(telemetry.no_progress_turns, no_progress);
                assert_eq!(
                    telemetry.triggered,
                    no_progress == NO_PROGRESS_TURN_THRESHOLD
                );
            }
        }
    }

    #[test]
    fn cosmetic_observation_intents_cannot_recredit_identical_output() {
        let mut ledger = ProgressLedger::new();
        let total = MAX_INTENTS_PER_IDENTICAL_OUTCOME + NO_PROGRESS_TURN_THRESHOLD;
        for attempt in 0..total {
            let id = format!("rg-{attempt}");
            let telemetry = ledger.record_turn(
                &[call(
                    &id,
                    "bash",
                    json!({
                        "command": format!("rg needle . --glob '!__noop_{attempt}__'")
                    }),
                )],
                &[result(&id, "same match", false)],
            );
            if attempt < MAX_INTENTS_PER_IDENTICAL_OUTCOME {
                assert!(telemetry.is_none());
            } else {
                let telemetry = telemetry.expect("identical evidence must converge");
                let no_progress = attempt - MAX_INTENTS_PER_IDENTICAL_OUTCOME + 1;
                assert_eq!(telemetry.no_progress_turns, no_progress);
                assert_eq!(
                    telemetry.triggered,
                    no_progress == NO_PROGRESS_TURN_THRESHOLD
                );
            }
        }
    }

    #[test]
    fn distinct_resources_with_equal_output_are_distinct_evidence() {
        let mut ledger = ProgressLedger::new();
        for index in 0..(MAX_INTENTS_PER_IDENTICAL_OUTCOME + 5) {
            let id = format!("read-equal-{index}");
            assert!(ledger
                .record_turn(
                    &[call(
                        &id,
                        "read",
                        json!({"file_path": format!("file-{index}.txt")}),
                    )],
                    &[result(&id, "same file contents", false)],
                )
                .is_none());
        }
    }

    #[test]
    fn materially_changing_log_and_listing_outcomes_count_within_the_bound() {
        let mut ledger = ProgressLedger::new();
        for (index, output) in ["line one", "line one\nline two", "line two\nline three"]
            .into_iter()
            .enumerate()
        {
            let id = format!("tail-{index}");
            assert!(ledger
                .record_turn(
                    &[call(
                        &id,
                        "bash",
                        json!({"command": "tail -n 20 server.log"}),
                    )],
                    &[result(&id, output, false)],
                )
                .is_none());
        }

        for (index, (command, output)) in [
            ("ls src", "a"),
            ("ls -d src", "src"),
            ("ls -R src", "src:\\na"),
            ("ls -A src", ".hidden\\na"),
        ]
        .into_iter()
        .enumerate()
        {
            let id = format!("ls-{index}");
            assert!(ledger
                .record_turn(
                    &[call(&id, "bash", json!({"command": command}))],
                    &[result(&id, output, false)],
                )
                .is_none());
        }
    }

    #[test]
    fn repeated_effect_plus_new_observation_evidence_is_progress() {
        let mut ledger = ProgressLedger::new();
        let effect = call("effect-1", "bash", json!({"command": "mkdir -p out"}));
        assert!(ledger
            .record_turn(
                std::slice::from_ref(&effect),
                &[result("effect-1", "ok", false)],
            )
            .is_none());

        let repeated_effect = call("effect-2", "bash", json!({"command": "mkdir -p out"}));
        let observation = call("read-1", "read", json!({"file_path": "out/state"}));
        assert!(ledger
            .record_turn(
                &[repeated_effect, observation],
                &[
                    result("effect-2", "ok", false),
                    result("read-1", "new state", false),
                ],
            )
            .is_none());
    }

    #[test]
    fn repeated_positive_delta_for_same_intent_converges_but_distinct_intents_progress() {
        let mut ledger = ProgressLedger::new();
        for (index, path) in ["a", "b", "c"].into_iter().enumerate() {
            let id = format!("distinct-{index}");
            assert!(ledger
                .record_turn(
                    &[call(
                        &id,
                        "bash",
                        json!({"command": format!("date +%s%N > {path}")}),
                    )],
                    &[result_with_changed_key(&id, &format!("target-{path}"))],
                )
                .is_none());
        }

        for (attempt, command) in [
            "printf '%s' \"$RANDOM\" > c",
            "python3 -c 'open(\"c\", \"w\").write(\"new\")'",
            "date +%s%N > c",
        ]
        .into_iter()
        .enumerate()
        {
            let attempt = attempt + 1;
            let id = format!("repeat-{attempt}");
            let telemetry = ledger
                .record_turn(
                    &[call(&id, "bash", json!({"command": command}))],
                    &[result_with_changed_key(&id, "target-c")],
                )
                .expect("repeated physical churn is not fresh semantic progress");
            assert_eq!(telemetry.no_progress_turns, attempt);
            assert_eq!(telemetry.triggered, attempt == NO_PROGRESS_TURN_THRESHOLD);
        }
    }

    #[test]
    fn changing_observation_output_has_a_bounded_novelty_allowance() {
        let mut ledger = ProgressLedger::new();

        for attempt in 0..(MAX_OUTCOME_VARIANTS_PER_INTENT + NO_PROGRESS_TURN_THRESHOLD) {
            let id = format!("bash-{attempt}");
            let calls = vec![call(
                &id,
                "bash",
                json!({ "command": "tail -n 20 changing.log" }),
            )];
            let results = vec![result(&id, &format!("random-outcome-{attempt}"), false)];
            let telemetry = ledger.record_turn(&calls, &results);
            if attempt < MAX_OUTCOME_VARIANTS_PER_INTENT {
                assert!(telemetry.is_none());
            } else {
                let telemetry = telemetry.expect("outcome churn must converge");
                let no_progress = attempt - MAX_OUTCOME_VARIANTS_PER_INTENT + 1;
                assert_eq!(telemetry.no_progress_turns, no_progress);
                assert_eq!(
                    telemetry.triggered,
                    no_progress == NO_PROGRESS_TURN_THRESHOLD
                );
            }
        }
    }

    #[test]
    fn explicit_process_status_change_is_new_evidence() {
        let mut ledger = ProgressLedger::new();
        let calls = vec![call("bash", "bash", json!({ "command": "ps -p 123" }))];
        let running = Content::ToolResult {
            tool_use_id: "bash".to_string(),
            output: json!({ "result": { "status": "running", "process_id": "123" } }),
            is_error: None,
        };
        let finished = Content::ToolResult {
            tool_use_id: "bash".to_string(),
            output: json!({ "result": { "status": "finished", "process_id": "456" } }),
            is_error: None,
        };

        assert!(ledger
            .record_turn(&calls, std::slice::from_ref(&running))
            .is_none());
        assert!(ledger.record_turn(&calls, &[running]).is_some());
        assert!(ledger.record_turn(&calls, &[finished]).is_none());
    }
}
