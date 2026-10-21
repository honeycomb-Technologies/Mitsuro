/// Internal classification of labeled runtime instruction sections. Labels are
/// the existing message format; parse them at this boundary so budgeting and
/// cache partitioning cannot silently disagree about new section kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContextKind {
    WorkerGoal,
    WorkerCapability,
    HiveIdentity,
    WorkerPersona,
    Workflow,
    Runtime,
    Delegation,
    Project,
    HiveOperations,
    Discovery,
    Supplemental,
}

impl ContextKind {
    pub(crate) fn from_text(text: &str) -> Self {
        let starts_with =
            |prefixes: &[&str]| prefixes.iter().any(|prefix| text.starts_with(prefix));
        if starts_with(&[
            "[WORKER GOAL CAPABILITY]",
            "[WORKER GOAL WORKSPACE]",
            "[CANONICAL WORKER GOAL ATTEMPT]",
        ]) {
            Self::WorkerGoal
        } else if starts_with(&[
            "[WORKER CONVERSATION CAPABILITY]",
            "[WORKER GROUP RESPONSE CAPABILITY]",
        ]) {
            Self::WorkerCapability
        } else if starts_with(&[
            "[HIVE COORDINATOR]",
            "[HIVE SOUL",
            "[HIVE IDENTITY",
            "[HIVE USER",
            "[HIVE CREW IDENTITY",
            "[HIVE CREW SOUL",
        ]) {
            Self::HiveIdentity
        } else if text.starts_with("[HIVE WORKER") {
            Self::WorkerPersona
        } else if starts_with(&["[DURABLE GOAL -", "[PLAN MODE ACTIVE", "[ACTIVE PLAN"]) {
            Self::Workflow
        } else if starts_with(&[
            "[AUTONOMOUS TASKS]",
            "[WORKSPACE MODE:",
            "[ENVIRONMENT]",
            "[GROUP ROOM",
        ]) {
            Self::Runtime
        } else if text.starts_with("[DELEGATION MODE:") {
            Self::Delegation
        } else if starts_with(&[
            "[PROJECT INSTRUCTIONS",
            "[PROJECT SETTINGS]",
            "[HIVE PROJECT OVERLAY",
        ]) {
            Self::Project
        } else if starts_with(&["[HIVE HEARTBEAT", "[HIVE CHANNELS"]) {
            Self::HiveOperations
        } else if starts_with(&["[AVAILABLE SKILLS]", "[RECENT DELEGATED RUNS]"]) {
            Self::Discovery
        } else {
            Self::Supplemental
        }
    }

    pub(crate) fn budget_priority(self) -> u8 {
        match self {
            Self::WorkerGoal => 220,
            Self::WorkerCapability | Self::HiveIdentity | Self::WorkerPersona => 200,
            // Canonical execution obligations precede optional project/retrieval
            // context. The workflow renderer bounds them independently.
            Self::Workflow => 180,
            Self::Runtime | Self::Delegation => 120,
            Self::Project => 100,
            Self::HiveOperations => 90,
            Self::Discovery => 80,
            Self::Supplemental => 60,
        }
    }

    pub(crate) fn is_stable_identity(self) -> bool {
        // Worker persona has a high allocation priority but remains dynamic;
        // it is not part of the primary Hive's frozen identity cache contract.
        self == Self::HiveIdentity
    }

    pub(crate) fn is_stable_project(self) -> bool {
        matches!(self, Self::Project | Self::Delegation)
    }
}
