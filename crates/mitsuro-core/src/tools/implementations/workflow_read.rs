//! Read the current session's canonical Goal without changing workflow state.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::storage::Database;
use crate::tools::parse_params;
use crate::tools::registry::{Tool, ToolContext, ToolResult};
use crate::workflow::{WorkflowManager, WorkflowSnapshot};

const MAX_PAGE_CHARS: usize = 2_000;

pub struct WorkflowReadTool;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Section {
    Objective,
    Constraints,
    Criteria,
    Steps,
    Plan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    goal_id: String,
    #[serde(default)]
    content_hash: Option<String>,
    section: Section,
    #[serde(default)]
    item_id: Option<String>,
    #[serde(default)]
    offset: usize,
}

#[async_trait]
impl Tool for WorkflowReadTool {
    fn name(&self) -> &str {
        "workflow_read"
    }

    fn description(&self) -> &str {
        "Read the full canonical Goal objective, constraints, criteria, steps, or plan for the current session. Returns exact text in bounded pages; follow next_offset using the returned content_hash. This tool never changes workflow state."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "goal_id": {"type": "string"},
                "content_hash": {"type": "string", "description": "Exact section hash from the first page; required with nonzero offset. Unrelated accounting revisions do not invalidate it."},
                "section": {"type": "string", "enum": ["objective", "constraints", "criteria", "steps", "plan"]},
                "item_id": {"type": "string", "description": "Optional exact criterion or step ID; omit to read all items in that section."},
                "offset": {"type": "integer", "minimum": 0, "description": "Character offset from next_offset; omit for the first page. Read every page needed to recover omitted requirements."}
            },
            "required": ["goal_id", "section"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, params: Value, ctx: &ToolContext) -> ToolResult {
        let params: Params = match parse_params(params) {
            Ok(params) => params,
            Err(error) => return error,
        };
        match read_page(&params, ctx) {
            Ok(page) => ToolResult::success_data(page),
            Err(error) => ToolResult::error(error),
        }
    }
}

fn read_page(params: &Params, ctx: &ToolContext) -> anyhow::Result<Value> {
    let session_id = ctx
        .session_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("workflow_read requires an active session"))?;
    let path = ctx
        .db_path
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("workflow_read requires a durable database"))?;
    let database = Database::open_read_only(path)?;
    // Ownership and all workflow rows must come from one SQLite read snapshot,
    // including when another process advances the Goal during pagination.
    database
        .conn()
        .execute_batch("BEGIN DEFERRED TRANSACTION")?;
    let owner = database
        .conn()
        .query_row(
            "SELECT user_id FROM sessions WHERE id = ?1",
            [session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .ok_or_else(|| anyhow::anyhow!("workflow_read session does not exist"))?;
    anyhow::ensure!(
        owner.as_deref() == ctx.user_id.as_deref(),
        "workflow_read does not match the session owner"
    );
    let manager = WorkflowManager::with_shared_db(Arc::new(Mutex::new(database)));
    let snapshot = manager
        .get_snapshot(session_id)?
        .ok_or_else(|| anyhow::anyhow!("No durable Goal exists for this session"))?;
    anyhow::ensure!(
        snapshot.goal.id == params.goal_id,
        "Goal changed; use Goal {} and restart pagination",
        snapshot.goal.id
    );
    let source = section_text(&snapshot, params)?;
    // Every provider turn updates attempt accounting and can advance the
    // aggregate revision. Pin the exact section, not unrelated counters, so
    // pagination remains usable during an active Goal.
    let content_hash = format!("{:x}", Sha256::digest(source.as_bytes()));
    anyhow::ensure!(
        params.offset == 0 || params.content_hash.is_some(),
        "content_hash from the first page is required when offset is nonzero"
    );
    if let Some(expected) = params.content_hash.as_deref() {
        anyhow::ensure!(
            expected == content_hash,
            "Section content changed; restart pagination at offset 0 without content_hash"
        );
    }
    let total_chars = source.chars().count();
    anyhow::ensure!(
        params.offset <= total_chars,
        "offset exceeds section length {total_chars}"
    );
    let content = source
        .chars()
        .skip(params.offset)
        .take(MAX_PAGE_CHARS)
        .collect::<String>();
    let next_offset = params.offset + content.chars().count();
    Ok(json!({
        "goal_id": snapshot.goal.id,
        "revision": snapshot.aggregate_revision,
        "content_hash": content_hash,
        "offset": params.offset,
        "next_offset": (next_offset < total_chars).then_some(next_offset),
        "total_chars": total_chars,
        "complete": next_offset == total_chars,
        "content": content,
        "format": if matches!(params.section, Section::Objective) { "text" } else { "json" },
        "note": "Pages are exact consecutive excerpts; join content strings without separators to recover this full section."
    }))
}

fn section_text(snapshot: &WorkflowSnapshot, params: &Params) -> anyhow::Result<String> {
    if let Some(id) = params.item_id.as_deref() {
        return match params.section {
            Section::Criteria => snapshot
                .criteria
                .iter()
                .find(|criterion| criterion.id == id)
                .map(serde_json::to_string)
                .transpose()?
                .ok_or_else(|| {
                    anyhow::anyhow!("No criterion with that ID exists in the current Goal")
                }),
            Section::Steps => snapshot
                .steps
                .iter()
                .find(|step| step.id == id)
                .map(serde_json::to_string)
                .transpose()?
                .ok_or_else(|| anyhow::anyhow!("No step with that ID exists in the current Goal")),
            _ => anyhow::bail!("item_id is supported only for criteria and steps"),
        };
    }
    Ok(match params.section {
        Section::Objective => snapshot.goal.objective.clone(),
        Section::Constraints => serde_json::to_string(&snapshot.goal.constraints)?,
        Section::Criteria => serde_json::to_string(&snapshot.criteria)?,
        Section::Steps => serde_json::to_string(&snapshot.steps)?,
        Section::Plan => serde_json::to_string(&snapshot.plan_revision)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SessionManager;
    use crate::tools::registry::{tool_policy, PermissionMode, ToolCategory};
    use crate::workflow::{CreateGoalInput, CriterionInput};

    fn fixture() -> (tempfile::TempDir, ToolContext, WorkflowSnapshot) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("workflow.sqlite");
        let sessions = SessionManager::new(Database::new(&path).unwrap());
        let session_id = sessions
            .create_session("Bounded Goal", None, Some("/tmp"))
            .unwrap();
        let snapshot = WorkflowManager::new(path.clone())
            .unwrap()
            .create_goal(
                &session_id,
                CreateGoalInput {
                    title: "Recover all requirements".into(),
                    objective: "🦀".repeat(3_900),
                    constraints: vec!["Preserve each exact requirement".into()],
                    criteria: vec![CriterionInput {
                        description: "Verify Unicode pagination".into(),
                        required: true,
                    }],
                    token_budget: None,
                },
                "create-read-fixture",
                "user",
            )
            .unwrap()
            .snapshot;
        let ctx = ToolContext {
            working_dir: temp.path().to_path_buf(),
            ..Default::default()
        }
        .with_session_metadata(session_id, path);
        (temp, ctx, snapshot)
    }

    fn params(snapshot: &WorkflowSnapshot) -> Params {
        Params {
            goal_id: snapshot.goal.id.clone(),
            content_hash: None,
            section: Section::Objective,
            item_id: None,
            offset: 0,
        }
    }

    #[test]
    fn workflow_read_reconstructs_long_unicode_objective_without_mutation() {
        let (_temp, ctx, snapshot) = fixture();
        let mut params = params(&snapshot);
        let mut full = String::new();
        loop {
            let page = read_page(&params, &ctx).unwrap();
            assert!(serde_json::to_string(&page).unwrap().len() < 16 * 1024);
            full.push_str(page["content"].as_str().unwrap());
            let Some(offset) = page["next_offset"].as_u64() else {
                break;
            };
            params.offset = offset as usize;
            params.content_hash = page["content_hash"].as_str().map(str::to_owned);
        }
        assert_eq!(full, snapshot.goal.objective);
        let after = WorkflowManager::new(ctx.db_path.clone().unwrap())
            .unwrap()
            .get_snapshot(ctx.session_id.as_deref().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(after).unwrap(),
            serde_json::to_value(snapshot).unwrap()
        );
    }

    #[test]
    fn workflow_read_rejects_other_owner_goal_hash_and_item() {
        let (_temp, mut ctx, snapshot) = fixture();
        let mut params = params(&snapshot);
        ctx.user_id = Some("another-owner".into());
        assert!(read_page(&params, &ctx).is_err());
        ctx.user_id = None;
        params.goal_id = "another-goal".into();
        assert!(read_page(&params, &ctx).is_err());
        params.goal_id = snapshot.goal.id.clone();
        params.content_hash = Some("changed-content".into());
        assert!(read_page(&params, &ctx).is_err());
        params.content_hash = None;
        params.section = Section::Criteria;
        params.item_id = Some("another-criterion".into());
        assert!(read_page(&params, &ctx).is_err());
        params.item_id = Some(snapshot.criteria[0].id.clone());
        let page = read_page(&params, &ctx).unwrap();
        let criterion: Value = serde_json::from_str(page["content"].as_str().unwrap()).unwrap();
        assert_eq!(criterion["id"], snapshot.criteria[0].id);
    }

    #[test]
    fn workflow_read_pages_survive_unrelated_revisions_but_reject_changed_content() {
        let (_temp, ctx, snapshot) = fixture();
        let mut params = params(&snapshot);
        let first = read_page(&params, &ctx).unwrap();
        params.offset = first["next_offset"].as_u64().unwrap() as usize;
        assert!(
            read_page(&params, &ctx).is_err(),
            "pagination must pin its source"
        );
        params.content_hash = first["content_hash"].as_str().map(str::to_owned);
        let manager = WorkflowManager::new(ctx.db_path.clone().unwrap()).unwrap();
        let updated = manager
            .edit_goal(
                ctx.session_id.as_deref().unwrap(),
                &snapshot.goal.id,
                snapshot.aggregate_revision,
                crate::workflow::EditGoalInput {
                    title: Some("Changed title".into()),
                    objective: None,
                    constraints: None,
                    criteria: None,
                    token_budget: None,
                },
                "change-title",
                "user",
            )
            .unwrap()
            .snapshot;
        let next = read_page(&params, &ctx).unwrap();
        assert_eq!(next["revision"], updated.aggregate_revision);
        assert_eq!(
            format!(
                "{}{}",
                first["content"].as_str().unwrap(),
                next["content"].as_str().unwrap()
            ),
            snapshot.goal.objective
        );
        manager
            .edit_goal(
                ctx.session_id.as_deref().unwrap(),
                &snapshot.goal.id,
                updated.aggregate_revision,
                crate::workflow::EditGoalInput {
                    title: None,
                    objective: Some("Changed outcome".into()),
                    constraints: None,
                    criteria: None,
                    token_budget: None,
                },
                "change-outcome",
                "user",
            )
            .unwrap();
        assert!(read_page(&params, &ctx).is_err());
    }

    #[tokio::test]
    async fn workflow_read_is_discoverable_and_read_only_in_plan_mode() {
        let policy = tool_policy("workflow_read");
        assert_eq!(policy.category, ToolCategory::ReadOnly);
        assert!(policy.allowed_in_plan_mode);
        let registry = crate::tools::registry::ToolRegistry::new();
        crate::tools::register_all_tools(&registry).await;
        assert!(registry.get("workflow_read").await.is_some());
        let (_temp, mut ctx, snapshot) = fixture();
        ctx.permission_mode = PermissionMode::Supervised;
        ctx.plan_mode = true;
        let result = WorkflowReadTool
            .execute(
                json!({"goal_id": snapshot.goal.id, "section": "objective"}),
                &ctx,
            )
            .await;
        assert!(!result.is_error, "{}", result.output);
        let invalid = WorkflowReadTool.execute(json!({"goal_id": snapshot.goal.id, "section": "objective", "session_id": "forged"}), &ctx).await;
        assert!(invalid.is_error);
    }
}
