use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Plan status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    /// Plan is being worked on.
    InProgress,
    /// All tasks completed.
    Completed,
    /// Plan was abandoned.
    Abandoned,
}

impl std::fmt::Display for PlanStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanStatus::InProgress => write!(f, "in_progress"),
            PlanStatus::Completed => write!(f, "completed"),
            PlanStatus::Abandoned => write!(f, "abandoned"),
        }
    }
}

impl std::str::FromStr for PlanStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "in_progress" | "inprogress" => Ok(PlanStatus::InProgress),
            "completed" | "complete" | "done" => Ok(PlanStatus::Completed),
            "abandoned" | "cancelled" | "canceled" => Ok(PlanStatus::Abandoned),
            _ => Err(format!("Unknown plan status: {}", s)),
        }
    }
}

/// Task status (individual task within a plan).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[default]
    Pending,
    InProgress,
    Completed,
    Blocked,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "pending"),
            TaskStatus::InProgress => write!(f, "in_progress"),
            TaskStatus::Completed => write!(f, "completed"),
            TaskStatus::Blocked => write!(f, "blocked"),
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "pending" => Ok(TaskStatus::Pending),
            "in_progress" | "inprogress" => Ok(TaskStatus::InProgress),
            "completed" | "complete" | "done" => Ok(TaskStatus::Completed),
            "blocked" => Ok(TaskStatus::Blocked),
            _ => Err(format!("Unknown task status: {}", s)),
        }
    }
}

/// A single task within a phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanTask {
    /// Task ID like "1.1", "2.3", or "1.1.1" for subtasks.
    pub id: String,
    /// Parent task ID for subtasks (None = top-level task).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Task description.
    pub description: String,
    /// Implementation details/context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Completion summary (required when completing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// Whether the task is complete (kept for backward compatibility).
    pub completed: bool,
    /// Task status (richer than just completed bool).
    #[serde(default)]
    pub status: TaskStatus,
    /// Task IDs that must complete before this task can start.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_by: Vec<String>,
    /// Task IDs that are waiting on this task.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<String>,
    /// Child task IDs (for hierarchy).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<String>,
    /// Priority (1 = highest).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    /// When the task was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    /// When the task was completed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

impl PlanTask {
    pub fn new(id: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            parent_id: None,
            description: description.into(),
            context: None,
            result: None,
            completed: false,
            status: TaskStatus::Pending,
            blocked_by: Vec::new(),
            blocks: Vec::new(),
            children: Vec::new(),
            priority: None,
            created_at: Some(Utc::now()),
            completed_at: None,
        }
    }

    /// Create a subtask with parent reference.
    pub fn new_subtask(
        id: impl Into<String>,
        parent_id: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            parent_id: Some(parent_id.into()),
            description: description.into(),
            context: None,
            result: None,
            completed: false,
            status: TaskStatus::Pending,
            blocked_by: Vec::new(),
            blocks: Vec::new(),
            children: Vec::new(),
            priority: None,
            created_at: Some(Utc::now()),
            completed_at: None,
        }
    }

    /// Check if this task is a subtask (has parent).
    pub fn is_subtask(&self) -> bool {
        self.parent_id.is_some()
    }

    /// Get the depth level (0 for top-level, 1+ for subtasks).
    pub fn depth(&self) -> usize {
        self.id.matches('.').count().saturating_sub(1)
    }
}

/// A phase containing multiple tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanPhase {
    /// Phase number (1, 2, 3, ...).
    pub number: usize,
    /// Phase name/title.
    pub name: String,
    /// Tasks in this phase.
    pub tasks: Vec<PlanTask>,
}

impl PlanPhase {
    pub fn new(number: usize, name: impl Into<String>) -> Self {
        Self {
            number,
            name: name.into(),
            tasks: Vec::new(),
        }
    }

    /// Add a task to this phase (test helper).
    #[cfg(test)]
    pub fn add_task(&mut self, description: impl Into<String>) -> &PlanTask {
        let task_num = self.tasks.len() + 1;
        let id = format!("{}.{}", self.number, task_num);
        self.tasks.push(PlanTask::new(id, description));
        self.tasks.last().expect("task just pushed")
    }

    /// Count completed tasks.
    pub fn completed_count(&self) -> usize {
        self.tasks
            .iter()
            .filter(|t| t.completed || t.status == TaskStatus::Completed)
            .count()
    }

    /// Check if all tasks are complete.
    pub fn is_complete(&self) -> bool {
        !self.tasks.is_empty()
            && self
                .tasks
                .iter()
                .all(|t| t.completed || t.status == TaskStatus::Completed)
    }
}

/// A complete plan file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanFile {
    /// Plan title.
    pub title: String,
    /// When the plan was created.
    pub created_at: DateTime<Utc>,
    /// Session ID that created this plan.
    pub session_id: Option<String>,
    /// Working directory for this plan.
    pub working_dir: Option<String>,
    /// Current status.
    pub status: PlanStatus,
    /// Plan phases.
    pub phases: Vec<PlanPhase>,
    /// Optional notes section.
    pub notes: Option<String>,
    /// Version number for conflict detection (incremented on each save).
    #[serde(default)]
    pub version: u64,
    /// File path (set when loaded/saved).
    #[serde(skip)]
    pub file_path: Option<PathBuf>,
}

impl PlanFile {
    /// Create a new empty plan.
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            created_at: Utc::now(),
            session_id: None,
            working_dir: None,
            status: PlanStatus::InProgress,
            phases: Vec::new(),
            notes: None,
            version: 0,
            file_path: None,
        }
    }

    /// Add a new phase (test helper).
    #[cfg(test)]
    pub fn add_phase(&mut self, name: impl Into<String>) -> &mut PlanPhase {
        let number = self.phases.len() + 1;
        self.phases.push(PlanPhase::new(number, name));
        self.phases.last_mut().expect("phase just pushed")
    }
}
