//! Contracts shared by both agent loop kernels.
//!
//! The parent orchestrator and the delegated child loop enforce the same
//! recovery doctrine (one bounded synthesis landing turn, completion shields
//! for dispatched mutations). This module keeps their wording and semantics in
//! one place so the two kernels cannot drift apart.

use serde_json::Value;

use crate::tools::registry::{effective_tool_call, tool_policy_for_call, ToolCategory};

/// Audience for a loop-guard landing turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LandingAudience {
    /// The parent loop addresses the user directly.
    User,
    /// A delegated child reports back to its parent agent.
    ParentAgent,
}

/// Deterministic landing answer when the synthesis turn itself returns no text.
pub(crate) const LANDING_FALLBACK_USER: &str = "I stopped this run after the loop guard detected repeated work without enough new evidence. The evidence gathered so far remains available; a new instruction can steer a different approach.";
pub(crate) const LANDING_FALLBACK_PARENT_AGENT: &str =
    "The delegated loop stopped after repeated work without enough new semantic progress.";

/// Build the one-shot system instruction for a loop-guard landing turn.
pub(crate) fn loop_guard_landing_instruction(
    diagnostic: &str,
    audience: LandingAudience,
) -> String {
    let audience_instruction = match audience {
        LandingAudience::User => {
            "Give the user a concise evidence-based answer, identify any unresolved blocker, and state what new direction would be needed to continue."
        }
        LandingAudience::ParentAgent => {
            "Using only evidence already gathered, return a concise report to the parent with established findings, paths or changes, unresolved gaps, and the materially different direction needed to continue."
        }
    };
    format!(
        "[LOOP GUARD LANDING]\n{diagnostic}\n\nThis is the one bounded synthesis turn. No tools are available. {audience_instruction} Do not request or describe another tool call."
    )
}

/// Once a mutating operation has been dispatched, dropping its future is not
/// proof that its side effects stopped. Signal the exact call's cancellation
/// token and retain ownership until the registry's governed timeout or its
/// producer-owned terminal result. Read-only calls remain immediately
/// cancellable. Bash owns a process-group drop guard and kill-on-drop child,
/// so dropping it is its bounded quiescence mechanism; waiting there would
/// regress an interrupt into the command's (potentially long) timeout.
pub(crate) fn tool_call_requires_completion_shield(tool_name: &str, input: &Value) -> bool {
    let (effective_name, effective_input) = effective_tool_call(tool_name, input);
    matches!(
        tool_policy_for_call(effective_name, effective_input).category,
        ToolCategory::Write
    ) && effective_name != "bash"
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn landing_instruction_matches_audience() {
        let user = loop_guard_landing_instruction("diag", LandingAudience::User);
        assert!(user.contains("[LOOP GUARD LANDING]\ndiag\n"));
        assert!(user.contains("Give the user a concise evidence-based answer"));

        let child = loop_guard_landing_instruction("diag", LandingAudience::ParentAgent);
        assert!(child.contains("return a concise report to the parent"));
    }

    #[test]
    fn landing_fallbacks_are_distinct_by_audience() {
        assert_ne!(LANDING_FALLBACK_USER, LANDING_FALLBACK_PARENT_AGENT);
    }

    #[test]
    fn shield_covers_write_and_wrapper_targets_but_not_bash() {
        assert!(tool_call_requires_completion_shield("edit", &json!({})));
        assert!(tool_call_requires_completion_shield(
            "tool_search",
            &json!({"action": "execute", "tool": "write", "arguments": {}})
        ));
        assert!(!tool_call_requires_completion_shield("read", &json!({})));
        assert!(!tool_call_requires_completion_shield("bash", &json!({})));
    }
}
