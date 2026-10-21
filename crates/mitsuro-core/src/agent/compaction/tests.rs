use tempfile::TempDir;

use super::{
    cut_point::find_cut_point, microcompact::microcompact_messages, run_compaction_pipeline,
    CompactionManager, CompactionRequest, CompactionTrigger,
};
use crate::ai::types::{Content, ModelMessage, Role};
use crate::plan::PlanManager;
use crate::storage::{
    CompactionStore, Database, MemoryStore, MessageStore, SessionManager,
    COMPACTION_FLUSH_TITLE_PREFIX,
};

fn text_message(role: Role, text: &str) -> ModelMessage {
    ModelMessage {
        role,
        content: vec![Content::text(text.to_string())],
    }
}

fn create_persisted_conversation(
    temp: &TempDir,
    name: &str,
) -> (std::path::PathBuf, String, Vec<ModelMessage>) {
    let db_path = temp.path().join(format!("{name}.db"));
    let db = Database::new(&db_path).expect("db");
    let session_manager = SessionManager::new(db);
    let session_id = session_manager
        .create_session("Compaction test", None, None)
        .expect("session");

    let conversation = vec![
        text_message(Role::User, "start task"),
        text_message(Role::Assistant, "working"),
        text_message(Role::User, "continue"),
        text_message(Role::Assistant, "recent work"),
    ];

    for message in &conversation {
        let role = match message.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            _ => continue,
        };
        let content = serde_json::to_string(&message.content).expect("content json");
        session_manager
            .save_message(&session_id, role, &content)
            .expect("save");
    }

    (db_path, session_id, conversation)
}

#[tokio::test]
async fn run_compaction_pipeline_replaces_history_in_place() {
    let temp = TempDir::new().expect("temp dir");
    let db_path = temp.path().join("compaction.db");
    let db = Database::new(&db_path).expect("db");
    let session_manager = SessionManager::new(db);
    let session_id = session_manager
        .create_session("Compaction test", None, None)
        .expect("session");

    for (role, text) in [
        ("user", "start task"),
        ("assistant", "working"),
        ("user", "continue"),
        ("assistant", "recent work"),
    ] {
        let content = serde_json::json!([{ "type": "text", "text": text }]).to_string();
        session_manager
            .save_message(&session_id, role, &content)
            .expect("save");
    }

    let conversation = vec![
        text_message(Role::User, "start task"),
        text_message(Role::Assistant, "working"),
        text_message(Role::User, "continue"),
        text_message(Role::Assistant, "recent work"),
    ];

    let result = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Manual {
            preservation_hints: None,
            direction: Some("Keep going".to_string()),
        },
        compaction_manager: CompactionManager::for_model(
            crate::ai::providers::ProviderId::MiniMax,
            crate::ai::models::ApiFormat::Anthropic,
            crate::constants::ai::DEFAULT_MODEL,
            200_000,
        ),
        request_budget: None,
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        project_dir: None,
        user_id: None,
        summary_override: Some(crate::agent::SummarizationResult {
            work_summary: "Did important work".to_string(),
            key_decisions: vec!["Use SQLite".to_string()],
            pending_tasks: vec!["Finish compaction".to_string()],
            important_files: vec!["src/main.rs".to_string()],
        }),
    })
    .await
    .expect("compact");

    assert!(result.replaced_messages > 0);
    assert!(result.compacted_conversation.len() < conversation.len() + 2);
    assert!(result
        .compacted_conversation
        .iter()
        .any(|message| message.content.iter().any(|content| {
            matches!(content, Content::Text { text, .. } if text.contains("Conversation Compacted"))
        })));
    let summary_text = result.compacted_conversation[1]
        .content
        .iter()
        .find_map(|content| match content {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .expect("summary text");
    assert!(
        !summary_text.contains("## Latest User Objective"),
        "objective retained verbatim in the tail must not be duplicated in the summary"
    );

    let db = Database::new(&db_path).expect("db");
    let records = MessageStore::new(&db)
        .load_session_message_records(&session_id)
        .expect("records");
    assert_eq!(records.len(), result.compacted_conversation.len());

    let checkpoints = CompactionStore::new(&db)
        .count_checkpoints(&session_id)
        .expect("checkpoint count");
    assert_eq!(checkpoints, 1);
    let compacted_history_json: String = db
        .conn()
        .query_row(
            "SELECT compacted_history_json FROM compaction_checkpoints WHERE session_id = ?1",
            [&session_id],
            |row| row.get(0),
        )
        .expect("checkpoint history");
    assert_eq!(compacted_history_json, "[]");
    let segment_json: String = db
        .conn()
        .query_row(
            "SELECT segment_markdown FROM compaction_segments WHERE session_id = ?1",
            [&session_id],
            |row| row.get(0),
        )
        .expect("segment snapshot");
    let segment: serde_json::Value =
        serde_json::from_str(&segment_json).expect("canonical segment json");
    assert_eq!(segment["schema"], "mitsuro.compaction_segment.v1");
    assert!(segment["messages"]
        .as_array()
        .is_some_and(|items| !items.is_empty()));
}

#[tokio::test]
async fn auto_compaction_uses_caller_trigger_estimate() {
    let temp = TempDir::new().expect("temp dir");
    let (db_path, session_id, conversation) = create_persisted_conversation(&temp, "auto-trigger");

    let result = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Auto,
        compaction_manager: CompactionManager::for_model(
            crate::ai::providers::ProviderId::MiniMax,
            crate::ai::models::ApiFormat::Anthropic,
            crate::constants::ai::DEFAULT_MODEL,
            2_000,
        ),
        request_budget: Some(super::CompactionRequestBudget {
            total_tokens: 200_000,
            fixed_overhead_tokens: 0,
        }),
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        project_dir: None,
        user_id: None,
        summary_override: Some(crate::agent::SummarizationResult {
            work_summary: "Triggered by injected context".to_string(),
            key_decisions: Vec::new(),
            pending_tasks: Vec::new(),
            important_files: Vec::new(),
        }),
    })
    .await
    .expect("auto compaction should use caller estimate");

    assert_eq!(result.estimated_tokens_before, 200_000);
    assert!(result.replaced_messages > 0);
}

#[tokio::test]
async fn compaction_reports_irreducible_fixed_request_overhead() {
    let temp = TempDir::new().expect("temp dir");
    let (db_path, session_id, conversation) =
        create_persisted_conversation(&temp, "irreducible-overhead");

    let result = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Auto,
        compaction_manager: CompactionManager::default(),
        request_budget: Some(super::CompactionRequestBudget {
            total_tokens: 500_000,
            fixed_overhead_tokens: 500_000,
        }),
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        summary_override: None,
        project_dir: None,
        user_id: None,
    })
    .await;
    let error = match result {
        Ok(_) => panic!("fixed overhead must make target irreducible"),
        Err(error) => error,
    };

    assert!(error
        .to_string()
        .contains("irreducible fixed request overhead"));
}

#[tokio::test]
async fn compaction_without_ai_uses_deterministic_summary() {
    let temp = TempDir::new().expect("temp dir");
    let (db_path, session_id, conversation) = create_persisted_conversation(&temp, "fallback");

    let result = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Manual {
            preservation_hints: Some("Preserve the continuation objective".to_string()),
            direction: None,
        },
        compaction_manager: CompactionManager::default(),
        request_budget: None,
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        project_dir: None,
        user_id: None,
        summary_override: None,
    })
    .await
    .expect("manual compaction should have deterministic fallback");

    assert!(result
        .summary
        .work_summary
        .contains("deterministic compaction summary"));
    assert_ne!(result.summary.work_summary, "No summary available.");

    let flush = MemoryStore::new(Database::new(&db_path).expect("memory db"))
        .list(None, None)
        .into_iter()
        .find(|memory| memory.title.starts_with(COMPACTION_FLUSH_TITLE_PREFIX))
        .expect("compaction flush memory");
    assert_eq!(flush.source, crate::storage::MemorySource::Compaction);
    assert!(flush.content.contains("start task"));
    assert_eq!(
        flush.source_session_id.as_deref(),
        Some(session_id.as_str())
    );
}

#[tokio::test]
async fn compaction_carries_prior_semantics_without_raw_nesting_or_plan_duplication() {
    let temp = TempDir::new().expect("temp dir");
    let db_path = temp.path().join("deduplicated-context.db");
    let session_manager = SessionManager::new(Database::new(&db_path).expect("db"));
    let session_id = session_manager
        .create_session("Compaction deduplication", None, None)
        .expect("session");
    let conversation = vec![
        text_message(
            Role::User,
            "# Conversation Compacted\n\n## Work Summary\n\nOLD SUMMARY SENTINEL",
        ),
        text_message(Role::Assistant, "continued old work"),
        text_message(Role::User, "finish the current implementation"),
        text_message(Role::Assistant, "working on the current implementation"),
    ];
    for message in &conversation {
        let role = match message.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            _ => continue,
        };
        session_manager
            .save_message(
                &session_id,
                role,
                &serde_json::to_string(&message.content).expect("message json"),
            )
            .expect("save message");
    }

    let plan_manager = PlanManager::new(db_path.clone()).expect("plan manager");
    let mut plan = plan_manager
        .create_plan("PLAN DUPLICATION SENTINEL", &session_id, None)
        .expect("plan");
    plan.add_phase("Implementation")
        .add_task("Finish the current implementation");
    plan_manager.save_plan(&plan).expect("save plan");

    let result = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Manual {
            preservation_hints: None,
            direction: None,
        },
        compaction_manager: CompactionManager::default(),
        request_budget: None,
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        project_dir: None,
        user_id: None,
        summary_override: Some(crate::agent::SummarizationResult {
            work_summary: "Fresh bounded summary".to_string(),
            key_decisions: Vec::new(),
            pending_tasks: vec!["Finish the current implementation".to_string()],
            important_files: Vec::new(),
        }),
    })
    .await
    .expect("compact");

    let compacted_text = result
        .compacted_conversation
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(compacted_text.contains("OLD SUMMARY SENTINEL"));
    assert_eq!(
        compacted_text.matches("# Conversation Compacted").count(),
        1
    );
    assert!(!compacted_text.contains("PLAN DUPLICATION SENTINEL"));
    assert!(!compacted_text.contains("Active Plan (post-compaction)"));
}

#[tokio::test]
async fn two_compactions_preserve_bounded_structured_prior_semantics() {
    let temp = TempDir::new().expect("temp dir");
    let (db_path, session_id, conversation) =
        create_persisted_conversation(&temp, "two-compactions");
    let first = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Manual {
            preservation_hints: None,
            direction: None,
        },
        compaction_manager: CompactionManager::default(),
        request_budget: None,
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        summary_override: Some(crate::agent::SummarizationResult {
            work_summary: "FIRST WORK SEMANTIC SENTINEL".to_string(),
            key_decisions: vec!["FIRST DECISION SEMANTIC SENTINEL".to_string()],
            pending_tasks: vec!["FIRST PENDING SEMANTIC SENTINEL".to_string()],
            important_files: Vec::new(),
        }),
        project_dir: None,
        user_id: None,
    })
    .await
    .expect("first compaction");

    assert_eq!(
        first.compacted_conversation[1].role,
        Role::Assistant,
        "generated context must not impersonate a user instruction"
    );
    assert!(super::cut_point::is_compaction_summary(
        &first.compacted_conversation[1]
    ));
    let mut legacy = first.compacted_conversation[1].clone();
    legacy.role = Role::User;
    assert!(
        super::cut_point::is_compaction_summary(&legacy),
        "existing checkpoints remain readable"
    );

    let appended = vec![
        text_message(Role::User, "new objective after first compaction"),
        text_message(Role::Assistant, "new work after first compaction"),
    ];
    let session_manager = SessionManager::new(Database::new(&db_path).expect("db"));
    for message in &appended {
        let role = if message.role == Role::User {
            "user"
        } else {
            "assistant"
        };
        session_manager
            .save_message(
                &session_id,
                role,
                &serde_json::to_string(&message.content).expect("message json"),
            )
            .expect("append message");
    }
    let mut second_conversation = first.compacted_conversation;
    second_conversation.extend(appended);

    let second = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &second_conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Manual {
            preservation_hints: None,
            direction: None,
        },
        compaction_manager: CompactionManager::default(),
        request_budget: None,
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        summary_override: Some(crate::agent::SummarizationResult {
            work_summary: "SECOND WORK SEMANTIC SENTINEL".to_string(),
            key_decisions: Vec::new(),
            pending_tasks: Vec::new(),
            important_files: Vec::new(),
        }),
        project_dir: None,
        user_id: None,
    })
    .await
    .expect("second compaction");

    let text = second
        .compacted_conversation
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|content| match content {
            Content::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("FIRST WORK SEMANTIC SENTINEL"));
    assert!(text.contains("FIRST DECISION SEMANTIC SENTINEL"));
    assert!(text.contains("FIRST PENDING SEMANTIC SENTINEL"));
    assert!(text.contains("SECOND WORK SEMANTIC SENTINEL"));
    assert_eq!(text.matches("# Conversation Compacted").count(), 1);
    assert!(text.len() < 20_000, "structured carry must remain bounded");
}

#[test]
fn find_cut_point_keeps_tail_messages() {
    let messages = vec![
        super::cut_point::IndexedMessage {
            id: 1,
            message: text_message(Role::User, "old"),
        },
        super::cut_point::IndexedMessage {
            id: 2,
            message: text_message(Role::Assistant, "old reply"),
        },
        super::cut_point::IndexedMessage {
            id: 3,
            message: text_message(Role::User, "recent"),
        },
        super::cut_point::IndexedMessage {
            id: 4,
            message: text_message(Role::Assistant, "latest"),
        },
    ];

    let cut = find_cut_point(&messages, 0, 1).expect("cut");
    assert_eq!(cut.first_kept_message_id, 4);
    assert_eq!(cut.kept_messages.len(), 1);
}

#[test]
fn microcompact_strips_old_thinking_blocks() {
    let mut conversation = Vec::new();
    for index in 0..8 {
        conversation.push(text_message(Role::User, &format!("question {index}")));
        conversation.push(ModelMessage {
            role: Role::Assistant,
            content: vec![
                Content::Thinking {
                    thinking: format!("reasoning {index}"),
                    signature: String::new(),
                },
                Content::text(format!("answer {index}")),
            ],
        });
    }

    let result = microcompact_messages(&conversation);
    assert!(result.changed);
    let first_assistant = &result.messages[1];
    assert!(!first_assistant
        .content
        .iter()
        .any(|content| matches!(content, Content::Thinking { .. })));
}

#[test]
fn microcompact_truncates_unicode_tool_results_safely() {
    let mut conversation = Vec::new();
    conversation.push(ModelMessage {
        role: Role::User,
        content: vec![Content::ToolResult {
            tool_use_id: "tool-1".to_string(),
            output: serde_json::json!({
                "retention": "summarize_after_turn",
                "summary": "unicode output",
                "result": "🦀".repeat(600),
            }),
            is_error: None,
        }],
    });
    for index in 0..7 {
        conversation.push(text_message(Role::Assistant, &format!("assistant {index}")));
    }

    let result = microcompact_messages(&conversation);
    assert!(result.changed);
    let Content::ToolResult { output, .. } = &result.messages[0].content[0] else {
        panic!("expected tool result");
    };
    let truncated = output
        .get("result")
        .and_then(|value| value.as_str())
        .expect("truncated result");
    assert!(truncated.contains("[microcompact truncated]"));
}

#[tokio::test]
async fn compaction_preserves_citations_on_retained_text_in_durable_storage() {
    let temp = TempDir::new().unwrap();
    let (db_path, session_id, mut conversation) =
        create_persisted_conversation(&temp, "cited-tail");
    let citation = crate::ai::types::Citation {
        url: "https://example.org/evidence".into(),
        title: "Evidence".into(),
        cited_text: "verified finding".into(),
    };
    let cited = ModelMessage {
        role: Role::Assistant,
        content: vec![Content::Text {
            text: "Verified finding with source.".into(),
            citations: vec![citation.clone()],
        }],
    };
    SessionManager::new(Database::new(&db_path).unwrap())
        .save_message(
            &session_id,
            "assistant",
            &serde_json::to_string(&cited.content).unwrap(),
        )
        .unwrap();
    conversation.push(cited);
    let result = run_compaction_pipeline(CompactionRequest {
        db_path: &db_path,
        session_id: &session_id,
        conversation: &conversation,
        working_dir: temp.path(),
        ai_client: None,
        model: None,
        trigger: CompactionTrigger::Manual {
            preservation_hints: None,
            direction: None,
        },
        compaction_manager: CompactionManager::default(),
        request_budget: None,
        last_usage_prompt_tokens: None,
        messages_after_usage: 0,
        project_dir: None,
        user_id: None,
        summary_override: Some(crate::agent::SummarizationResult {
            work_summary: "Prior work".into(),
            key_decisions: Vec::new(),
            pending_tasks: Vec::new(),
            important_files: Vec::new(),
        }),
    })
    .await
    .unwrap();
    let has_source = |messages: &[ModelMessage]| {
        messages.iter().flat_map(|message| &message.content).any(|content| matches!(content,
        Content::Text { text, citations } if text == "Verified finding with source." && citations == &vec![citation.clone()]
    ))
    };
    assert!(has_source(&result.compacted_conversation));
    let db = Database::new(&db_path).unwrap();
    let reloaded = MessageStore::new(&db)
        .load_session_message_records(&session_id)
        .unwrap()
        .into_iter()
        .map(|record| ModelMessage {
            role: if record.role == "assistant" {
                Role::Assistant
            } else {
                Role::User
            },
            content: serde_json::from_str(&record.content_json).unwrap(),
        })
        .collect::<Vec<_>>();
    assert!(has_source(&reloaded));
}
