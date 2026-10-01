//! Repeated tool failure detection.
//!
//! Tracks tool error signatures across iterations and triggers a fail-fast
//! when the same tool keeps failing with the same error pattern, preventing
//! infinite retry loops.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::agent::hooks::shell_policy::{
    classify_bash_command, is_proven_read_only_bash_command, BashFileOperationKind,
};
use crate::agent::loop_kernel::action_intent;
use crate::ai::types::{AiToolCall, Content, ModelMessage, Role};
use crate::tools::registry::{
    agent_call_execution_profile, agent_call_is_research, agent_call_starts_run,
    effective_tool_call, tool_policy_for_call, ToolCategory,
};

/// Default threshold: stop after this many identical failures.
pub const REPEATED_FAILURE_THRESHOLD: usize = 2;
/// Default threshold: stop after this many identical read-only tool sequences.
pub const REPEATED_READ_ONLY_SEQUENCE_THRESHOLD: usize = 3;
/// Default threshold: stop after this many semantically equivalent successful
/// validation-only turns.
pub const REPEATED_VALIDATION_SEQUENCE_THRESHOLD: usize = 3;

/// True when a tool call is pure exploration (native read tools or observational bash).
///
/// `bash` is categorized as write-capable for permissioning, but many agent
/// death-spirals are repeated `git show` / `rg` / `sed` pipelines. Count those
/// as exploration so the loop guard can stop them.
pub(crate) fn is_exploration_probe(call: &AiToolCall) -> bool {
    let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
    if tool_policy_for_call(name, arguments).category == ToolCategory::ReadOnly {
        return true;
    }
    if !matches!(name, "bash" | "shell" | "execute") {
        return false;
    }
    let Some(command) = bash_command_text(arguments) else {
        return false;
    };
    if is_proven_read_only_bash_command(command) {
        return true;
    }
    let class = classify_bash_command(command);
    if class.safety_violation.is_some() {
        return false;
    }
    matches!(
        class.file_operation.as_ref().map(|op| op.kind),
        Some(BashFileOperationKind::Read | BashFileOperationKind::Search)
    )
}

fn bash_command_text(arguments: &serde_json::Value) -> Option<&str> {
    arguments
        .get("command")
        .and_then(|value| value.as_str())
        .or_else(|| arguments.get("cmd").and_then(|value| value.as_str()))
}

/// Check tool results for repeated failures. Returns a diagnostic message
/// if the same tool+error signature has been seen `threshold` or more times.
///
/// On success, only the recovered tool signature is cleared.
pub fn detect_repeated_failures(
    counters: &mut HashMap<String, usize>,
    tool_calls: &[AiToolCall],
    tool_results: &[Content],
) -> Option<String> {
    let mut call_meta = HashMap::new();
    for call in tool_calls {
        let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
        let mut intent = action_intent(call);
        // Observation normalization can discard shell redirections and order.
        // Failed effectful commands need their exact scope so a new target or
        // changed repair cannot be mistaken for an unchanged failed action.
        if matches!(name, "bash" | "shell" | "execute") {
            if let Some(command) = bash_command_text(arguments)
                .filter(|command| !is_proven_read_only_bash_command(command))
            {
                let scope = hash_arguments(&serde_json::Value::String(command.to_string()));
                intent.push_str(&format!(":effect:{scope}"));
            }
        }
        call_meta.insert(call.id.as_str(), (name, intent, arguments));
    }

    let mut recovered_calls = Vec::new();

    for result in tool_results {
        let Content::ToolResult {
            tool_use_id,
            output,
            is_error,
        } = result
        else {
            continue;
        };

        if !is_error.unwrap_or(false) {
            let Some((_, intent, _)) = call_meta.get(tool_use_id.as_str()) else {
                continue;
            };
            recovered_calls.push(format!("{intent}|"));
            continue;
        }

        let Some((tool_name, intent, arguments)) = call_meta.get(tool_use_id.as_str()) else {
            continue;
        };

        let output_str = match output {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let (error_code, error_fingerprint) = extract_error_signature(&output_str);
        // More time can be a real recovery strategy for an actual timeout,
        // but it cannot cure access denial, bad parameters or a failed check.
        // Use Bash's effective timeout (including its default and cap), so
        // spelling an equivalent timeout differently cannot evade convergence.
        let timeout_recovery = if matches!(*tool_name, "bash" | "shell" | "execute")
            && (error_code.contains("timeout")
                || classify_error_code(&error_fingerprint) == "timeout")
        {
            format!(
                "|timeout:{}",
                arguments
                    .get("timeout")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(30_000)
                    .min(600_000)
            )
        } else {
            String::new()
        };
        let signature = format!("{intent}|{error_code}|{error_fingerprint}{timeout_recovery}");
        let count = counters
            .entry(signature)
            .and_modify(|c| *c += 1)
            .or_insert(1);

        if *count >= REPEATED_FAILURE_THRESHOLD {
            return Some(format!(
                "Stopping tool loop: '{}' failed {} times with the same '{}' error. Do not retry the same command unchanged; inspect the previous output, fix the reported issue, or choose a different strategy.",
                tool_name, *count, error_code
            ));
        }
    }

    if !recovered_calls.is_empty() {
        counters.retain(|signature, _| {
            !recovered_calls
                .iter()
                .any(|prefix| signature.starts_with(prefix))
        });
    }

    None
}

/// Detect repeated identical read-only tool sequences across turns.
///
/// This guards against agent loops that keep reissuing the same exploration
/// pattern without taking action on the gathered evidence. Native read tools
/// and observational bash (including git show/log pipelines flagged as
/// file-read/search misuse) all count as exploration probes.
pub fn detect_repeated_read_only_sequence(
    counters: &mut HashMap<String, usize>,
    tool_calls: &[AiToolCall],
    tool_results: &[Content],
) -> Option<String> {
    if tool_calls.is_empty() || !tool_calls.iter().all(is_exploration_probe) {
        counters.clear();
        return None;
    }

    let results = tool_results
        .iter()
        .filter_map(|result| match result {
            Content::ToolResult {
                tool_use_id,
                output,
                is_error,
            } => Some((tool_use_id.as_str(), (output, is_error.unwrap_or(false)))),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    // Intent alone cannot prove a loop: repeated CI/status observations may
    // use the exact same call while returning materially changing evidence.
    // Failed or incomplete batches are handled by the failure guard instead.
    if tool_calls.iter().any(|call| {
        results
            .get(call.id.as_str())
            .is_none_or(|(_, is_error)| *is_error)
    }) {
        counters.clear();
        return None;
    }

    let signature = tool_calls
        .iter()
        .map(|call| {
            let (output, _) = results
                .get(call.id.as_str())
                .expect("completed read-only call has a result");
            format!("{}:{}", action_intent(call), hash_arguments(output))
        })
        .collect::<Vec<_>>()
        .join("|");

    counters.retain(|key, _| key == &signature);
    let count = counters
        .entry(signature)
        .and_modify(|value| *value += 1)
        .or_insert(1);

    if *count >= REPEATED_READ_ONLY_SEQUENCE_THRESHOLD {
        return Some(format!(
            "Stopping exploration loop: the same read-only tool pattern repeated {} times without taking action. Act on the evidence or change strategy.",
            *count
        ));
    }

    None
}

/// Detect successful validation-only turns that repeat the same semantic work.
///
/// Validation commands frequently contain incidental differences (paths,
/// output formatting, or an extra assertion), so hashing the full Bash payload
/// misses the loop we actually care about. This signature records validation
/// families instead: test, lint, build, syntax, and similar intent.
pub fn detect_repeated_validation_sequence(
    counters: &mut HashMap<String, usize>,
    tool_calls: &[AiToolCall],
    tool_results: &[Content],
) -> Option<String> {
    if tool_calls.is_empty() || !tool_calls.iter().all(is_validation_call) {
        counters.clear();
        return None;
    }

    let successful_ids = tool_results
        .iter()
        .filter_map(|result| match result {
            Content::ToolResult {
                tool_use_id,
                is_error,
                ..
            } if !is_error.unwrap_or(false) => Some(tool_use_id.as_str()),
            _ => None,
        })
        .collect::<std::collections::HashSet<_>>();
    if !tool_calls
        .iter()
        .all(|call| successful_ids.contains(call.id.as_str()))
    {
        counters.clear();
        return None;
    }

    let signature = tool_calls
        .iter()
        .map(validation_signature)
        .collect::<Vec<_>>()
        .join("|");
    counters.retain(|key, _| key == &signature);
    let count = counters
        .entry(signature)
        .and_modify(|value| *value += 1)
        .or_insert(1);

    if *count >= REPEATED_VALIDATION_SEQUENCE_THRESHOLD {
        return Some(format!(
            "Validation completed successfully. Agent stopped the same successful validation pattern after {} consecutive runs without new changes; no further validation was necessary.",
            *count,
        ));
    }

    None
}

pub(crate) fn is_validation_call(call: &AiToolCall) -> bool {
    let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
    if name == "agent" {
        return agent_call_execution_profile(arguments) == "verify";
    }
    if !matches!(name, "bash" | "shell" | "execute") {
        return false;
    }

    !validation_families(arguments).is_empty()
}

fn validation_signature(call: &AiToolCall) -> String {
    let (name, arguments) = effective_tool_call(&call.name, &call.arguments);
    if name == "agent" {
        return "agent:verify".to_string();
    }
    validation_families(arguments).join("+")
}

fn validation_families(arguments: &serde_json::Value) -> Vec<&'static str> {
    let command = arguments
        .get("command")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let mut families = Vec::new();
    let groups: &[(&str, &[&str])] = &[
        (
            "test",
            &[
                " test",
                "test ",
                "pytest",
                "go test",
                "mvn test",
                "gradle test",
                "forge test",
            ],
        ),
        ("check", &["cargo check", "git diff --check"]),
        ("lint", &["cargo clippy", " lint", "eslint"]),
        ("typecheck", &[" typecheck", "tsc "]),
        ("build", &[" run build", "cargo build", "npm run build"]),
        (
            "syntax",
            &["node --check", "py_compile", "json.tool", "syntax check"],
        ),
        ("integrity", &["integrity", "scaffold check", "assert "]),
    ];
    for (family, needles) in groups {
        if needles.iter().any(|needle| command.contains(needle)) {
            families.push(*family);
        }
    }
    families
}

/// Stop immediately when top-level exploration produced no usable evidence.
pub fn detect_terminal_explore_failure(
    tool_calls: &[AiToolCall],
    tool_results: &[Content],
) -> Option<String> {
    let explore_calls = tool_calls
        .iter()
        .filter(|call| {
            call.name == "agent"
                && agent_call_starts_run(&call.arguments)
                && agent_call_execution_profile(&call.arguments) == "explore"
        })
        .map(|call| (call.id.as_str(), agent_call_is_research(&call.arguments)))
        .collect::<Vec<_>>();

    if explore_calls.is_empty() {
        return None;
    }

    for result in tool_results {
        let Content::ToolResult {
            tool_use_id,
            output,
            is_error,
        } = result
        else {
            continue;
        };

        let Some((_, requires_file_evidence)) = explore_calls
            .iter()
            .find(|(call_id, _)| *call_id == tool_use_id.as_str())
        else {
            continue;
        };
        // Tool-validation and execution errors have their own retry and
        // repeated-failure policy. Only a successfully returned delegated-run
        // result can prove that exploration completed without usable evidence.
        if is_error.unwrap_or(false) {
            continue;
        }

        let parsed = match output {
            serde_json::Value::String(text) => serde_json::from_str::<serde_json::Value>(text).ok(),
            other => Some(other.clone()),
        }?;

        let summary = parsed
            .get("summary")
            .and_then(|value| value.as_str())
            .unwrap_or("explore returned degraded results");
        let result_payload = parsed.get("result").unwrap_or(&parsed);
        let status = result_payload
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if status == "background_started" {
            continue;
        }
        let outcome = result_payload
            .get("outcome")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        let success = result_payload
            .get("success")
            .and_then(|value| value.as_bool())
            .unwrap_or(true);
        let usable_agents = result_payload
            .get("usable_agents")
            .or_else(|| result_payload.get("successful_agents"))
            .and_then(|value| value.as_u64());
        let paths_examined_count = result_payload
            .get("paths_examined_count")
            .or_else(|| result_payload.get("files_examined_count"))
            .and_then(|value| value.as_u64());

        // New single-agent format: check success bool directly
        // Legacy multi-agent format: check usable_agents count
        let is_terminal = outcome == "failed"
            || !success
            || usable_agents.is_some_and(|count| count == 0)
            || (*requires_file_evidence && paths_examined_count == Some(0));

        if is_terminal {
            return Some(format!(
                "Stopping after degraded exploration: {} Summarize the failed investigation and ask the user to narrow scope before more reads.",
                summary
            ));
        }
    }

    None
}

/// Stop broad manual read-only probing when a prior explore already returned
/// usable delegated coverage and explicitly told the parent to summarize.
pub fn detect_post_explore_manual_fallback(
    conversation: &[ModelMessage],
    tool_calls: &[AiToolCall],
) -> Option<String> {
    if tool_calls.is_empty() {
        return None;
    }

    // Include observational bash probes; permissioning treats bash as writeable
    // but the death-spiral we care about is post-explore manual archaeology.
    if !tool_calls.iter().all(is_exploration_probe) {
        return None;
    }

    let last_explore_result =
        conversation
            .iter()
            .enumerate()
            .rev()
            .find_map(|(message_index, message)| {
                if message.role != Role::User {
                    return None;
                }

                message.content.iter().find_map(|content| {
                    let Content::ToolResult {
                        tool_use_id,
                        output,
                        ..
                    } = content
                    else {
                        return None;
                    };

                    let parsed = match output {
                        serde_json::Value::String(text) => {
                            serde_json::from_str::<serde_json::Value>(text).ok()
                        }
                        other => Some(other.clone()),
                    }?;

                    let result_payload = parsed.get("result").unwrap_or(&parsed);
                    let tool_name = parsed
                        .get("tool")
                        .and_then(|value| value.as_str())
                        .or_else(|| result_payload.get("tool").and_then(|value| value.as_str()));
                    let unified_explore_call =
                        conversation[..message_index].iter().rev().any(|prior| {
                            prior.role == Role::Assistant
                                && prior.content.iter().any(|content| {
                                    matches!(
                                        content,
                                        Content::ToolUse { id, name, input }
                                            if id == tool_use_id
                                                && name == "agent"
                                                && agent_call_starts_run(input)
                                                && agent_call_execution_profile(input) == "explore"
                                                && agent_call_is_research(input)
                                    )
                                })
                        });
                    if tool_name != Some("explore") && !unified_explore_call {
                        return None;
                    }

                    Some(result_payload.clone())
                })
            })?;

    let outcome = last_explore_result
        .get("outcome")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let usable_agents = last_explore_result
        .get("usable_agents")
        .or_else(|| last_explore_result.get("successful_agents"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let next_action_hint = last_explore_result
        .get("next_action_hint")
        .and_then(|value| value.as_str())
        .unwrap_or_default();

    if usable_agents == 0 {
        return None;
    }

    if !matches!(outcome, "partial" | "success") {
        return None;
    }

    if next_action_hint.is_empty() {
        return None;
    }

    Some(
        "Stopping broad manual probing after delegated exploration: usable delegated coverage already exists. Summarize the gathered evidence, call out incomplete targets, and only continue if the user narrows scope.".to_string(),
    )
}

fn hash_arguments(arguments: &serde_json::Value) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    arguments.to_string().hash(&mut hasher);
    hasher.finish()
}

fn extract_error_signature(output_str: &str) -> (String, String) {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(output_str) {
        if let Some(error) = value.get("error") {
            if let Some(error_obj) = error.as_object() {
                let message = error_obj
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let code = error_obj
                    .get("code")
                    .and_then(|v| v.as_str())
                    .map(|c| c.to_ascii_lowercase())
                    .filter(|c| !c.is_empty())
                    .unwrap_or_else(|| classify_error_code(message).to_string());
                return (code, normalize_error_fingerprint(message));
            }

            if let Some(message) = error.as_str() {
                return (
                    classify_error_code(message).to_string(),
                    normalize_error_fingerprint(message),
                );
            }
        }

        if let Some(signature) = extract_history_tool_error_signature(&value) {
            return signature;
        }
    }

    (
        classify_error_code(output_str).to_string(),
        normalize_error_fingerprint(output_str),
    )
}

fn extract_history_tool_error_signature(value: &serde_json::Value) -> Option<(String, String)> {
    let summary = value.get("summary").and_then(|v| v.as_str())?;
    let result = value.get("result");

    let mut message_parts = vec![summary.to_string()];
    if let Some(error) = result
        .and_then(|v| v.get("error"))
        .and_then(|v| v.as_str())
        .filter(|text| !text.trim().is_empty())
    {
        message_parts.push(error.to_string());
    }
    if let Some(output_preview) = result
        .and_then(|v| v.get("output_preview"))
        .and_then(|v| v.as_str())
        .filter(|text| !text.trim().is_empty())
    {
        message_parts.push(output_preview.to_string());
    }

    let message = message_parts.join("\n");
    let code = value
        .get("error_code")
        .and_then(|v| v.as_str())
        .map(|code| code.to_ascii_lowercase())
        .filter(|code| !code.is_empty())
        .unwrap_or_else(|| classify_error_code(&message).to_string());

    Some((code, normalize_error_fingerprint(&message)))
}

fn classify_error_code(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("invalid parameters")
        || lower.contains("missing field")
        || lower.contains("unknown field")
    {
        "invalid_parameters"
    } else if lower.contains("unknown tool") {
        "unknown_tool"
    } else if lower.contains("access denied") || lower.contains("outside workspace") {
        "access_denied"
    } else if lower.contains("timed out") || lower.contains("timeout") {
        "timeout"
    } else if lower.contains("denied") {
        "permission_denied"
    } else {
        "tool_error"
    }
}

fn normalize_error_fingerprint(message: &str) -> String {
    let mut compact = String::new();
    for part in message.split_whitespace() {
        if !compact.is_empty() {
            compact.push(' ');
        }
        compact.push_str(part);
    }

    if compact.is_empty() {
        return "unknown".to_string();
    }

    compact.make_ascii_lowercase();
    compact.chars().take(160).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn successful_results(tool_calls: &[AiToolCall], output: serde_json::Value) -> Vec<Content> {
        tool_calls
            .iter()
            .map(|call| Content::ToolResult {
                tool_use_id: call.id.clone(),
                output: output.clone(),
                is_error: None,
            })
            .collect()
    }

    #[test]
    fn failed_action_cosmetics_converge_without_merging_recovery_or_new_targets() {
        let cases = [
            (
                "bash",
                json!({"command":"rg -n needle src", "timeout":1000, "description":"first"}),
                "bash",
                json!({"command":"cd . && command rg --line-number needle ./src/", "timeout":2000, "description":"again"}),
                json!({"command":"rg needle tests"}),
            ),
            (
                "list",
                json!({"path":"missing", "limit":1}),
                "tool_search",
                json!({"action":"execute", "tool":"list", "arguments":{"path":"missing", "limit":20}}),
                json!({"path":"other", "limit":1}),
            ),
            (
                "bash",
                json!({"command":"echo fixed > first.txt", "timeout":1000, "description":"first repair"}),
                "tool_search",
                json!({"action":"execute", "tool":"bash", "arguments":{"command":"echo fixed > first.txt", "timeout":2000, "description":"same repair"}}),
                json!({"command":"echo fixed > other.txt"}),
            ),
        ];
        for (name, first, retry_name, retry, other) in cases {
            let call = |id: &str, name: &str, arguments| AiToolCall {
                id: id.into(),
                name: name.into(),
                arguments,
            };
            let failure = |id: &str| Content::ToolResult {
                tool_use_id: id.into(),
                output: json!({"error":"access denied"}),
                is_error: Some(true),
            };
            let first = call("first", name, first);
            let retry = call("retry", retry_name, retry);
            let other = call("other", name, other);
            let mut guard = crate::agent::progress::LoopGuard::new();
            assert!(guard
                .evaluate(std::slice::from_ref(&first), &[failure("first")])
                .repeated_failure
                .is_none());
            assert!(
                guard
                    .evaluate(&[other], &[failure("other")])
                    .repeated_failure
                    .is_none(),
                "new target: {name}"
            );
            assert!(
                guard
                    .evaluate(std::slice::from_ref(&retry), &[failure("retry")])
                    .repeated_failure
                    .is_some(),
                "cosmetic retry: {name}"
            );

            // A success through the same normalized action clears its failures,
            // including when the successful call uses the wrapper surface.
            let mut guard = crate::agent::progress::LoopGuard::new();
            guard.evaluate(std::slice::from_ref(&first), &[failure("first")]);
            let success = successful_results(std::slice::from_ref(&retry), json!({"ok":true}));
            guard.evaluate(std::slice::from_ref(&retry), &success);
            assert!(
                guard
                    .evaluate(&[first], &[failure("first")])
                    .repeated_failure
                    .is_none(),
                "recovery: {name}"
            );
        }

        // Timeout changes are meaningful only for timeout failures. Equivalent
        // defaults/capped values still converge, while a genuinely larger
        // window gets its own recovery opportunity.
        for (first_timeout, retry_timeout, should_stop) in [
            (Some(1000), Some(2000), false),
            (None, Some(30_000), true),
            (Some(600_000), Some(700_000), true),
        ] {
            let mut counters = HashMap::new();
            for (index, timeout) in [first_timeout, retry_timeout].into_iter().enumerate() {
                let mut arguments = json!({"command":"cargo test"});
                if let Some(timeout) = timeout {
                    arguments["timeout"] = json!(timeout);
                }
                let calls = [AiToolCall {
                    id: "timeout".into(),
                    name: "bash".into(),
                    arguments,
                }];
                let results = [Content::ToolResult {
                    tool_use_id: "timeout".into(),
                    output: json!({"error":{"code":"timeout","message":"command timed out"}}),
                    is_error: Some(true),
                }];
                assert_eq!(
                    detect_repeated_failures(&mut counters, &calls, &results).is_some(),
                    index == 1 && should_stop,
                    "timeout recovery: {first_timeout:?} -> {retry_timeout:?}"
                );
            }
        }
    }

    #[test]
    fn trips_at_threshold() {
        let call = AiToolCall {
            id: "call_1".to_string(),
            name: "glob".to_string(),
            arguments: json!({"pattern":"**/*"}),
        };
        let result = Content::ToolResult {
            tool_use_id: "call_1".to_string(),
            output: serde_json::Value::String(
                r#"{"error":"Invalid parameters: missing field `pattern`"}"#.to_string(),
            ),
            is_error: Some(true),
        };

        let mut counters = HashMap::new();
        let first = detect_repeated_failures(
            &mut counters,
            std::slice::from_ref(&call),
            std::slice::from_ref(&result),
        );
        assert!(first.is_none());

        let second = detect_repeated_failures(
            &mut counters,
            std::slice::from_ref(&call),
            std::slice::from_ref(&result),
        );
        assert!(second.is_some());
    }

    #[test]
    fn success_clears_counters() {
        let call = AiToolCall {
            id: "call_1".to_string(),
            name: "glob".to_string(),
            arguments: json!({"pattern":"**/*"}),
        };

        let mut counters = HashMap::new();

        // First failure
        let fail_result = Content::ToolResult {
            tool_use_id: "call_1".to_string(),
            output: serde_json::Value::String("error".to_string()),
            is_error: Some(true),
        };
        detect_repeated_failures(
            &mut counters,
            std::slice::from_ref(&call),
            std::slice::from_ref(&fail_result),
        );
        assert!(!counters.is_empty());

        // Success resets
        let ok_result = Content::ToolResult {
            tool_use_id: "call_1".to_string(),
            output: serde_json::Value::String("ok".to_string()),
            is_error: None,
        };
        detect_repeated_failures(
            &mut counters,
            std::slice::from_ref(&call),
            std::slice::from_ref(&ok_result),
        );
        assert!(counters.is_empty());
    }

    #[test]
    fn success_only_clears_matching_signature() {
        let failing_call = AiToolCall {
            id: "call_fail".to_string(),
            name: "glob".to_string(),
            arguments: json!({"pattern":"src/**/*.rs"}),
        };
        let succeeding_call = AiToolCall {
            id: "call_ok".to_string(),
            name: "read".to_string(),
            arguments: json!({"file_path":"src/main.rs"}),
        };

        let fail_result = Content::ToolResult {
            tool_use_id: "call_fail".to_string(),
            output: serde_json::Value::String("timeout".to_string()),
            is_error: Some(true),
        };
        let ok_result = Content::ToolResult {
            tool_use_id: "call_ok".to_string(),
            output: serde_json::Value::String("ok".to_string()),
            is_error: Some(false),
        };

        let mut counters = HashMap::new();
        detect_repeated_failures(
            &mut counters,
            &[failing_call.clone(), succeeding_call.clone()],
            &[fail_result.clone(), ok_result.clone()],
        );
        assert_eq!(counters.len(), 1);

        let second = detect_repeated_failures(
            &mut counters,
            &[failing_call, succeeding_call],
            &[fail_result, ok_result],
        );
        assert!(second.is_some());
    }

    #[test]
    fn repeated_failure_uses_history_error_code_and_output_fingerprint() {
        let call = AiToolCall {
            id: "call_1".to_string(),
            name: "bash".to_string(),
            arguments: json!({"command":"git diff --cached --check"}),
        };
        let raw_output = json!({
            "ok": false,
            "error": {
                "code": "command_failed",
                "message": "Command exited with code 2"
            },
            "data": {
                "output": "crates/grok-auth/README.md:76: trailing whitespace.\n+Just use the default path.  "
            },
            "metadata": {"exit_code": 2, "killed": false}
        })
        .to_string();
        let result = Content::ToolResult {
            tool_use_id: "call_1".to_string(),
            output: crate::agent::history_policy::build_history_tool_result(
                "bash",
                &raw_output,
                true,
            ),
            is_error: Some(true),
        };

        let mut counters = HashMap::new();
        assert!(detect_repeated_failures(
            &mut counters,
            std::slice::from_ref(&call),
            std::slice::from_ref(&result),
        )
        .is_none());
        let diagnostic = detect_repeated_failures(
            &mut counters,
            std::slice::from_ref(&call),
            std::slice::from_ref(&result),
        )
        .expect("second identical command failure should trip loop guard");

        assert!(diagnostic.contains("same 'command_failed' error"));
    }

    #[test]
    fn classify_error_code_matches_categories() {
        assert_eq!(
            classify_error_code("Invalid parameters: missing field `x`"),
            "invalid_parameters"
        );
        assert_eq!(classify_error_code("unknown tool: foo"), "unknown_tool");
        assert_eq!(
            classify_error_code("access denied to /etc/passwd"),
            "access_denied"
        );
        assert_eq!(
            classify_error_code("operation timed out after 30s"),
            "timeout"
        );
        assert_eq!(classify_error_code("some random error"), "tool_error");
    }

    #[test]
    fn normalize_collapses_whitespace() {
        assert_eq!(
            normalize_error_fingerprint("  A   spaced\n error\tmessage  "),
            "a spaced error message"
        );
    }

    #[test]
    fn repeated_read_only_batches_converge_across_tool_surfaces() {
        let call = |name: &str, arguments| AiToolCall {
            id: name.into(),
            name: name.into(),
            arguments,
        };
        let cases = [
            (
                "native",
                vec![
                    call("glob", json!({"pattern":"src/**/*.rs"})),
                    call("grep", json!({"pattern":"TODO"})),
                ],
                json!({"status":"unchanged"}),
            ),
            (
                "shell",
                vec![call(
                    "bash",
                    json!({"command":"git show abc123:apps/mobile/components/chat/ChatBar.tsx | rg -n 'shouldGrow' | head -40"}),
                )],
                json!({"output":"unchanged"}),
            ),
            (
                "mixed",
                vec![
                    call(
                        "bash",
                        json!({"command":"git log --oneline -5 -- apps/mobile"}),
                    ),
                    call(
                        "read",
                        json!({"path":"apps/mobile/components/chat/ChatBar.tsx"}),
                    ),
                ],
                json!({"ok":true}),
            ),
            (
                "wrapped",
                vec![call(
                    "tool_search",
                    json!({"action":"execute", "tool":"list", "arguments":{"path":"/home/example", "depth":1, "limit":100}}),
                )],
                json!({"ok":true}),
            ),
        ];
        for (label, mut calls, output) in cases {
            assert!(calls.iter().all(is_exploration_probe), "{label}");
            let mut counters = HashMap::new();
            for turn in 1..=REPEATED_READ_ONLY_SEQUENCE_THRESHOLD {
                if label == "wrapped" {
                    calls[0].arguments["arguments"]["limit"] = json!(100 * turn);
                }
                let results = successful_results(&calls, output.clone());
                assert_eq!(
                    detect_repeated_read_only_sequence(&mut counters, &calls, &results).is_some(),
                    turn == REPEATED_READ_ONLY_SEQUENCE_THRESHOLD,
                    "{label}, turn {turn}"
                );
                assert!(!counters.is_empty(), "{label}, turn {turn}");
            }
        }
    }

    #[test]
    fn repeated_read_only_intent_allows_changing_status_output() {
        let calls = vec![AiToolCall {
            id: "status-1".to_string(),
            name: "bash".to_string(),
            arguments: json!({"command": "gh run view 123 --json status,conclusion"}),
        }];
        let mut counters = HashMap::new();

        for status in ["queued", "in_progress", "completed", "success"] {
            let results = successful_results(&calls, json!({"status": status}));
            assert!(
                detect_repeated_read_only_sequence(&mut counters, &calls, &results).is_none(),
                "changed status output must remain observable"
            );
        }
    }

    #[test]
    fn repeated_read_only_sequence_resets_on_write() {
        let readonly = vec![AiToolCall {
            id: "call_1".to_string(),
            name: "glob".to_string(),
            arguments: json!({"pattern":"src/**/*.rs"}),
        }];
        let write = vec![AiToolCall {
            id: "call_2".to_string(),
            name: "edit".to_string(),
            arguments: json!({"file_path":"src/main.rs"}),
        }];

        let mut counters = HashMap::new();
        let readonly_results = successful_results(&readonly, json!({"ok": true}));
        detect_repeated_read_only_sequence(&mut counters, &readonly, &readonly_results);
        assert!(!counters.is_empty());

        assert!(detect_repeated_read_only_sequence(&mut counters, &write, &[]).is_none());
        assert!(counters.is_empty());
    }

    #[test]
    fn repeated_semantic_validation_sequence_trips_despite_command_drift() {
        let validation = |id: &str, command: &str| AiToolCall {
            id: id.to_string(),
            name: "bash".to_string(),
            arguments: json!({"command": command}),
        };
        let success = |id: &str| Content::ToolResult {
            tool_use_id: id.to_string(),
            output: json!({"ok": true}),
            is_error: None,
        };
        let mut counters = HashMap::new();

        assert!(detect_repeated_validation_sequence(
            &mut counters,
            &[validation("v1", "npm test && node --check app.js")],
            &[success("v1")],
        )
        .is_none());
        assert!(detect_repeated_validation_sequence(
            &mut counters,
            &[validation(
                "v2",
                "npm test -- --runInBand && node --check src/app.js"
            )],
            &[success("v2")],
        )
        .is_none());
        let diagnostic = detect_repeated_validation_sequence(
            &mut counters,
            &[validation(
                "v3",
                "npm test -- --silent && node --check scripts/deploy.js",
            )],
            &[success("v3")],
        )
        .expect("third equivalent validation turn should stop");

        assert!(diagnostic.starts_with("Validation completed successfully."));
        assert!(diagnostic.contains("3 consecutive runs"));
    }

    #[test]
    fn failed_or_non_validation_turn_resets_validation_sequence() {
        let validation = AiToolCall {
            id: "v1".to_string(),
            name: "bash".to_string(),
            arguments: json!({"command": "cargo test --workspace"}),
        };
        let failed = Content::ToolResult {
            tool_use_id: "v1".to_string(),
            output: json!({"error": "test failed"}),
            is_error: Some(true),
        };
        let mut counters = HashMap::new();

        assert!(detect_repeated_validation_sequence(
            &mut counters,
            std::slice::from_ref(&validation),
            &[failed],
        )
        .is_none());
        assert!(counters.is_empty());

        let edit = AiToolCall {
            id: "e1".to_string(),
            name: "edit".to_string(),
            arguments: json!({"file_path": "src/lib.rs"}),
        };
        assert!(detect_repeated_validation_sequence(&mut counters, &[edit], &[]).is_none());
        assert!(counters.is_empty());
    }

    #[test]
    fn wrapped_write_tools_are_not_classified_as_read_only() {
        let wrapped_write = AiToolCall {
            id: "write-1".to_string(),
            name: "tool_search".to_string(),
            arguments: json!({
                "action": "execute",
                "tool": "write",
                "arguments": {"file_path": "src/main.rs", "content": "fn main() {}"}
            }),
        };

        let mut counters = HashMap::new();
        assert!(detect_repeated_read_only_sequence(&mut counters, &[wrapped_write], &[]).is_none());
        assert!(counters.is_empty());
    }

    #[test]
    fn terminal_explore_failure_stops_after_zero_success_run() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({"agent_type":"explore","prompt":"audit"}),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: json!({
                "summary": "Explore completed: 2 agents (0 succeeded, 2 failed), 0 turns, 0 files examined",
                "result": {
                    "outcome": "failed",
                    "successful_agents": 0,
                    "failed_agents": 2,
                    "files_examined_count": 0
                }
            }),
            is_error: None,
        }];

        let diagnostic = detect_terminal_explore_failure(&tool_calls, &tool_results)
            .expect("failed explore should stop");
        assert!(diagnostic.contains("degraded exploration"));
    }

    #[test]
    fn terminal_explore_failure_stops_after_zero_evidence_partial_run() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({"agent_type":"explore","prompt":"audit"}),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: json!({
                "summary": "Explore completed: 2 agents (1 usable, 1 degraded, 0 failed), 8 turns, 0 files examined",
                "result": {
                    "outcome": "partial",
                    "usable_agents": 1,
                    "failed_agents": 0,
                    "files_examined_count": 0
                }
            }),
            is_error: None,
        }];

        let diagnostic = detect_terminal_explore_failure(&tool_calls, &tool_results)
            .expect("zero-evidence explore should stop");
        assert!(diagnostic.contains("degraded exploration"));
    }

    #[test]
    fn terminal_explore_failure_allows_background_start_to_continue() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({
                "profile": "audit-specialist",
                "capabilities": ["read"],
                "prompt": "audit"
            }),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: json!({
                "summary": "explore agent background_started",
                "result": {
                    "status": "background_started",
                    "delegated_run_id": "run-1"
                }
            }),
            is_error: None,
        }];

        let diagnostic = detect_terminal_explore_failure(&tool_calls, &tool_results);
        assert!(diagnostic.is_none());
    }

    #[test]
    fn terminal_explore_failure_does_not_stop_successful_usable_run() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({"agent_type":"explore","prompt":"audit"}),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: json!({
                "summary": "Explore completed: 2 agents (2 usable, 0 degraded, 0 failed), 10 turns, 24 files examined",
                "result": {
                    "outcome": "success",
                    "usable_agents": 2,
                    "successful_agents": 2,
                    "failed_agents": 0,
                    "files_examined_count": 24
                }
            }),
            is_error: None,
        }];

        let diagnostic = detect_terminal_explore_failure(&tool_calls, &tool_results);
        assert!(diagnostic.is_none());
    }

    #[test]
    fn terminal_explore_guard_reads_the_real_retained_agent_contract() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({
                "action": "spawn",
                "name": "map runtime",
                "instructions": "audit",
                "capabilities": ["read"]
            }),
        }];
        let raw = crate::tools::ToolResult::success_data(json!({
            "delegated_run_id": "run-1",
            "outcome": "success",
            "success": true,
            "usable_agents": 1,
            "files_examined_count": 8,
            "next_action_hint": "Synthesize the evidence."
        }))
        .output;
        let history = crate::agent::history_policy::build_history_tool_result("agent", &raw, false);
        assert_eq!(history["result"]["paths_examined_count"], 8);
        assert_eq!(history["result"]["usable_agents"], 1);
        assert_eq!(
            history["result"]["next_action_hint"],
            "Synthesize the evidence."
        );
        let results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: history,
            is_error: Some(false),
        }];

        assert!(detect_terminal_explore_failure(&tool_calls, &results).is_none());
    }

    #[test]
    fn execute_only_success_does_not_require_examined_files() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({
                "action": "spawn",
                "name": "focused validator",
                "instructions": "run tests",
                "capabilities": ["execute"]
            }),
        }];
        let raw = crate::tools::ToolResult::success_data(json!({
            "delegated_run_id": "run-validate",
            "outcome": "success",
            "success": true,
            "usable_agents": 1,
            "files_examined_count": 0,
            "findings": "Focused tests passed."
        }))
        .output;
        let results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: crate::agent::history_policy::build_history_tool_result("agent", &raw, false),
            is_error: Some(false),
        }];

        assert!(detect_terminal_explore_failure(&tool_calls, &results).is_none());
    }

    #[test]
    fn terminal_explore_failure_ignores_status_control_result() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({
                "action": "status",
                "profile": "explore",
                "delegated_run_id": "run-1"
            }),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: json!({
                "summary": "delegated agent completed",
                "result": {
                    "status": "completed",
                    "delegated_run_id": "run-1"
                }
            }),
            is_error: None,
        }];

        let diagnostic = detect_terminal_explore_failure(&tool_calls, &tool_results);
        assert!(diagnostic.is_none());
    }

    #[test]
    fn terminal_explore_failure_ignores_tool_validation_error() {
        let tool_calls = vec![AiToolCall {
            id: "tool-1".to_string(),
            name: "agent".to_string(),
            arguments: json!({
                "action": "spawn",
                "profile": "explore",
                "capabilities": ["read", "write"]
            }),
        }];
        let tool_results = vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: json!({
                "error_code": "invalid_agent_spec",
                "summary": "Profile 'explore' is read-only and cannot request write or execute"
            }),
            is_error: Some(true),
        }];

        let diagnostic = detect_terminal_explore_failure(&tool_calls, &tool_results);
        assert!(diagnostic.is_none());
    }

    #[test]
    fn post_explore_manual_fallback_stops_read_only_probe_after_partial_usable_run() {
        let conversation = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::ToolResult {
                tool_use_id: "explore-1".to_string(),
                output: serde_json::json!({
                    "tool": "explore",
                    "result": {
                        "outcome": "partial",
                        "usable_agents": 1,
                        "next_action_hint": "Use the evidence already gathered."
                    }
                }),
                is_error: Some(false),
            }],
        }];
        let tool_calls = vec![AiToolCall {
            id: "read-1".to_string(),
            name: "read".to_string(),
            arguments: serde_json::json!({"file_path":"src/main.rs"}),
        }];

        let diagnostic = detect_post_explore_manual_fallback(&conversation, &tool_calls)
            .expect("manual fallback should stop");
        assert!(diagnostic.contains("usable delegated coverage already exists"));
    }

    #[test]
    fn post_explore_manual_fallback_understands_unified_agent_history() {
        let conversation = vec![
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::ToolUse {
                    id: "agent-1".to_string(),
                    name: "agent".to_string(),
                    input: json!({
                        "action": "spawn",
                        "name": "map runtime",
                        "instructions": "Map the runtime boundaries",
                        "capabilities": ["read"]
                    }),
                }],
            },
            ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "agent-1".to_string(),
                    output: json!({
                        "tool": "agent",
                        "result": {
                            "outcome": "success",
                            "usable_agents": 1,
                            "files_examined_count": 8,
                            "next_action_hint": "Synthesize the delegated evidence."
                        }
                    }),
                    is_error: Some(false),
                }],
            },
        ];
        let tool_calls = vec![AiToolCall {
            id: "read-1".to_string(),
            name: "read".to_string(),
            arguments: json!({"file_path":"crates/mitsuro-core/src/agent/orchestrator.rs"}),
        }];

        let diagnostic = detect_post_explore_manual_fallback(&conversation, &tool_calls)
            .expect("unified explore result should prevent broad manual fallback");
        assert!(diagnostic.contains("usable delegated coverage already exists"));
    }

    #[test]
    fn post_explore_guard_uses_real_retained_agent_output() {
        let raw = crate::tools::ToolResult::success_data(json!({
            "delegated_run_id": "run-1",
            "outcome": "success",
            "success": true,
            "usable_agents": 1,
            "files_examined_count": 8,
            "next_action_hint": "Synthesize the delegated evidence."
        }))
        .output;
        let conversation = vec![
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::ToolUse {
                    id: "agent-1".to_string(),
                    name: "agent".to_string(),
                    input: json!({
                        "action": "spawn",
                        "name": "map runtime",
                        "instructions": "Map runtime boundaries",
                        "capabilities": ["read"]
                    }),
                }],
            },
            ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "agent-1".to_string(),
                    output: crate::agent::history_policy::build_history_tool_result(
                        "agent", &raw, false,
                    ),
                    is_error: Some(false),
                }],
            },
        ];
        let tool_calls = vec![AiToolCall {
            id: "read-1".to_string(),
            name: "read".to_string(),
            arguments: json!({"file_path":"src/main.rs"}),
        }];

        assert!(detect_post_explore_manual_fallback(&conversation, &tool_calls).is_some());
    }

    #[test]
    fn execute_only_child_does_not_block_later_manual_research() {
        let raw = crate::tools::ToolResult::success_data(json!({
            "delegated_run_id": "run-validate",
            "outcome": "success",
            "success": true,
            "usable_agents": 1,
            "files_examined_count": 0,
            "next_action_hint": "Review the validation result."
        }))
        .output;
        let conversation = vec![
            ModelMessage {
                role: Role::Assistant,
                content: vec![Content::ToolUse {
                    id: "agent-1".to_string(),
                    name: "agent".to_string(),
                    input: json!({
                        "action": "spawn",
                        "name": "focused validator",
                        "instructions": "Run focused tests",
                        "capabilities": ["execute"]
                    }),
                }],
            },
            ModelMessage {
                role: Role::User,
                content: vec![Content::ToolResult {
                    tool_use_id: "agent-1".to_string(),
                    output: crate::agent::history_policy::build_history_tool_result(
                        "agent", &raw, false,
                    ),
                    is_error: Some(false),
                }],
            },
        ];
        let tool_calls = vec![AiToolCall {
            id: "read-1".to_string(),
            name: "read".to_string(),
            arguments: json!({"file_path":"src/main.rs"}),
        }];

        assert!(detect_post_explore_manual_fallback(&conversation, &tool_calls).is_none());
    }

    #[test]
    fn post_explore_manual_fallback_allows_non_probe_actions() {
        let conversation = vec![ModelMessage {
            role: Role::User,
            content: vec![Content::ToolResult {
                tool_use_id: "explore-1".to_string(),
                output: serde_json::json!({
                    "tool": "explore",
                    "result": {
                        "outcome": "partial",
                        "usable_agents": 1,
                        "next_action_hint": "Use the evidence already gathered."
                    }
                }),
                is_error: Some(false),
            }],
        }];
        let tool_calls = vec![AiToolCall {
            id: "bash-1".to_string(),
            name: "bash".to_string(),
            arguments: serde_json::json!({"command":"cargo test"}),
        }];

        let diagnostic = detect_post_explore_manual_fallback(&conversation, &tool_calls);
        assert!(diagnostic.is_none());
    }
}
