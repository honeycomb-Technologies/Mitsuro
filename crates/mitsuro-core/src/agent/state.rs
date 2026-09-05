//! Agent run budgets and configuration

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::constants;

/// Soft pressure for unlimited interactive runs. Not a hard stop — injects a
/// strategy reminder so long poll/research thrash surfaces before the user
/// has to abort. Hard ceilings remain explicit/project/goal budgets only.
pub const INTERACTIVE_SOFT_TURN_WARN: usize = 40;
pub const INTERACTIVE_SOFT_TURN_REPLAN: usize = 80;

/// Optional resource limits for one parent agent run.
///
/// A missing value is deliberately unlimited. Behavioral loop protection is
/// handled by the semantic progress ledger rather than by an arbitrary turn
/// ceiling.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct RunBudget {
    /// Maximum provider turns in this run (`None` = unlimited).
    pub max_turns: Option<usize>,
}

impl RunBudget {
    pub const fn unlimited() -> Self {
        Self { max_turns: None }
    }

    pub const fn with_max_turns(max_turns: usize) -> Self {
        Self {
            max_turns: Some(max_turns),
        }
    }

    pub fn is_exhausted(self, completed_turns: usize) -> bool {
        self.max_turns
            .is_some_and(|max_turns| completed_turns >= max_turns)
    }
}

/// Provenance for the effective run budget recorded in runtime telemetry.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunBudgetSource {
    /// Typed per-run override supplied by the caller.
    ExplicitRun,
    /// Historical trace value retained for backward-compatible replay. New
    /// runs resolve bounded callers through `ExplicitRun`.
    LegacyMaxIterations,
    /// Repository-owned `.mitsuro/settings.json` override.
    ProjectSettings,
    /// Finite default used only while a durable Goal is active.
    GoalAttemptDefault,
    /// No configured ceiling; primary interactive execution is unlimited.
    UnlimitedDefault,
}

/// The canonical effective budget and where it came from.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunBudgetResolution {
    pub budget: RunBudget,
    pub source: RunBudgetSource,
}

impl RunBudgetResolution {
    /// Resolve exactly once at the core execution boundary.
    ///
    /// Per-run overrides are strongest, followed by repository policy and the
    /// unlimited interactive default.
    pub fn resolve(explicit_run: Option<RunBudget>, project: Option<RunBudget>) -> Self {
        if let Some(budget) = explicit_run {
            return Self {
                budget,
                source: RunBudgetSource::ExplicitRun,
            };
        }
        if let Some(budget) = project {
            return Self {
                budget,
                source: RunBudgetSource::ProjectSettings,
            };
        }

        Self {
            budget: RunBudget::unlimited(),
            source: RunBudgetSource::UnlimitedDefault,
        }
    }

    /// Resolve the parent loop for an active Goal. Explicit and project
    /// settings retain precedence. The default parent loop remains unlimited:
    /// bounded Goal attempts are enforced by the workflow manager and may roll
    /// over without terminating the approved plan.
    pub fn resolve_goal_attempt(
        explicit_run: Option<RunBudget>,
        project: Option<RunBudget>,
    ) -> Self {
        Self::resolve(explicit_run, project)
    }
}

/// Configuration for agent behavior
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Maximum turns for primary interactive sessions (None = unlimited).
    pub primary_max_turns: Option<usize>,
    /// Maximum turns for sub-agents (None = unlimited).
    pub subagent_max_turns: Option<usize>,
    /// Maximum turns for ACP sessions (None = unlimited).
    pub acp_max_turns: Option<usize>,
    /// Idle stream timeout in seconds.
    pub stream_idle_timeout_secs: u64,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            primary_max_turns: None,
            subagent_max_turns: None,
            acp_max_turns: None,
            stream_idle_timeout_secs: constants::http::STREAM_TIMEOUT.as_secs(),
        }
    }
}

impl AgentConfig {
    /// Resolve the primary-session turn budget.
    pub fn primary_max_turns(&self) -> Option<usize> {
        self.primary_max_turns
    }

    /// Typed primary-run override for the canonical core resolver.
    ///
    /// `None` means there is no caller override, allowing project settings or
    /// the unlimited default to resolve at the execution boundary.
    pub fn primary_run_budget_override(&self) -> Option<RunBudget> {
        self.primary_max_turns().map(RunBudget::with_max_turns)
    }

    /// Resolve the ACP turn budget.
    pub fn acp_max_turns(&self) -> Option<usize> {
        self.acp_max_turns
    }

    /// Resolve the configured idle stream timeout.
    pub fn stream_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.stream_idle_timeout_secs.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentConfig, RunBudget, RunBudgetResolution, RunBudgetSource};

    #[test]
    fn default_config_leaves_primary_and_acp_unlimited() {
        let config = AgentConfig::default();

        assert_eq!(config.primary_max_turns(), None);
        assert_eq!(config.primary_run_budget_override(), None);
        assert_eq!(config.acp_max_turns(), None);
        assert_eq!(config.subagent_max_turns, None);
        assert!(config.stream_idle_timeout().as_secs() >= 600);
    }

    #[test]
    fn unlimited_primary_budget_allows_progress_beyond_fifty_turns() {
        let resolution = RunBudgetResolution::resolve(None, None);

        assert_eq!(resolution.source, RunBudgetSource::UnlimitedDefault);
        for completed_turns in 0..=100 {
            assert!(!resolution.budget.is_exhausted(completed_turns));
        }
    }

    #[test]
    fn run_budget_resolution_preserves_precedence_and_provenance() {
        let project = RunBudget::with_max_turns(80);
        let project_resolution = RunBudgetResolution::resolve(None, Some(project));
        assert_eq!(project_resolution.budget.max_turns, Some(80));
        assert_eq!(project_resolution.source, RunBudgetSource::ProjectSettings);

        let explicit = RunBudgetResolution::resolve(Some(RunBudget::unlimited()), Some(project));
        assert_eq!(explicit.budget.max_turns, None);
        assert_eq!(explicit.source, RunBudgetSource::ExplicitRun);
    }

    #[test]
    fn active_goal_parent_run_is_unlimited_without_an_explicit_ceiling() {
        let resolution = RunBudgetResolution::resolve_goal_attempt(None, None);
        assert_eq!(resolution.source, RunBudgetSource::UnlimitedDefault);
        assert_eq!(resolution.budget.max_turns, None);
    }
}
