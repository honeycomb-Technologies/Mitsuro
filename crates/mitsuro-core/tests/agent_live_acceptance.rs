//! Opt-in acceptance of the exact candidate against the selected real provider.
//! Normal workspace tests never contact a provider. Evidence and all mutable
//! state belong to an explicit private directory outside the source tree.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use mitsuro_core::agent::loop_events::LoopStopReason;
use mitsuro_core::agent::{
    LoopEvent, LoopInput, OrchestratorServices, RunBudget, RunProvenance, RunSpecBuilder,
};
use mitsuro_core::ai::client::{AiClient, AiClientConfig, CallOptions};
use mitsuro_core::ai::models::ModelKey;
use mitsuro_core::ai::providers::ProviderId;
use mitsuro_core::ai::types::{Content, ModelMessage, Role};
use mitsuro_core::process::ProcessRegistry;
use mitsuro_core::skills::SkillsManager;
use mitsuro_core::storage::{Database, Preferences, SessionManager, SessionType, WorkMode};
use mitsuro_core::tools::registry::PermissionMode;
use mitsuro_core::tools::{EditTool, ReadTool, ToolRegistry};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("required acceptance setting: {name}"))
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn workspace_snapshot(root: &Path) -> Result<BTreeMap<String, String>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<String, String>) -> Result<()> {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(root, &path, files)?;
            } else {
                files.insert(
                    path.strip_prefix(root)?.to_string_lossy().into_owned(),
                    file_sha256(&path)?,
                );
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

#[tokio::test]
#[ignore = "explicit real-provider spend; requires isolated evidence paths and exact artifact hash"]
async fn selected_provider_agent_acceptance() -> Result<()> {
    ensure!(
        required_env("MITSURO_RUN_AGENT_LIVE_ACCEPTANCE")? == "1",
        "real-provider acceptance was not enabled"
    );
    let evidence = PathBuf::from(required_env("MITSURO_ACCEPTANCE_EVIDENCE_DIR")?);
    let source_db = PathBuf::from(required_env("MITSURO_ACCEPTANCE_SOURCE_DB")?);
    let auth_file = PathBuf::from(required_env("MITSURO_ACCEPTANCE_AUTH_FILE")?);
    let expected_model: ModelKey =
        serde_json::from_str(&required_env("MITSURO_ACCEPTANCE_MODEL_KEY")?)?;
    let executable = std::env::current_exe()?;
    let executable_sha256 = file_sha256(&executable)?;
    ensure!(
        executable_sha256 == required_env("MITSURO_ACCEPTANCE_EXE_SHA256")?,
        "candidate artifact hash changed"
    );
    ensure!(!evidence.exists(), "use a new private evidence directory");
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .canonicalize()?;
    let evidence_parent = evidence
        .parent()
        .context("evidence needs a parent directory")?
        .canonicalize()?;
    ensure!(
        !evidence_parent.starts_with(&repository),
        "evidence must be outside the repository"
    );
    ensure!(
        !mitsuro_core::paths::config_dir().starts_with(source_db.parent().unwrap()),
        "test config must be isolated from production"
    );

    // These explicit readers neither migrate a database nor refresh credentials.
    let db = Database::open_read_only(&source_db)?;
    db.conn().execute_batch("BEGIN DEFERRED TRANSACTION")?;
    let preferences = Preferences::new(db);
    let selected = preferences
        .get_current_model_key()
        .context("no exact persisted model key")?;
    ensure!(
        selected == expected_model,
        "persisted model selection changed; no fallback is allowed"
    );
    ensure!(
        selected.provider == ProviderId::Grok,
        "this bounded auth reader supports the reviewed Grok selection only"
    );
    let metadata = preferences
        .get_cached_models(selected.provider)
        .unwrap_or_default()
        .into_iter()
        .find(|model| model.key() == selected)
        .context("selected model missing from persisted exact catalog")?;
    ensure!(
        metadata.supports_tools,
        "selected model does not advertise tools"
    );
    drop(preferences);
    let auth_sha256 = file_sha256(&auth_file)?;
    let entries: HashMap<String, grok_auth::AuthEntry> =
        serde_json::from_slice(&std::fs::read(&auth_file)?)?;
    ensure!(
        entries.len() == 1,
        "auth selection is ambiguous; do not guess a credential"
    );
    let entry = entries.into_values().next().unwrap();
    ensure!(
        !entry.access_token.is_empty() && !entry.is_expired(chrono::Duration::minutes(10)),
        "credential unavailable or expires too soon; refresh requires separate authorization"
    );
    let client = Arc::new(AiClient::new_with_resolved_model(
        AiClientConfig::for_grok_with_metadata(&metadata),
        entry.access_token,
        metadata.resolve_runtime(),
    )?);
    std::fs::create_dir(&evidence)?;
    std::fs::write(
        evidence.join("candidate.json"),
        serde_json::to_vec_pretty(&json!({
            "executable":executable,"executable_sha256":executable_sha256,"model_key":selected,
            "source_state":required_env("MITSURO_ACCEPTANCE_SOURCE_STATE")?,
            "config_dir":mitsuro_core::paths::config_dir(),"maximum_provider_turns":8,"maximum_output_tokens_per_turn":4096,
            "evidence_level":"real provider via candidate production run_loop; isolated local fixtures; no deployed-runtime claim"
        }))?,
    )?;

    for (name, prompt, max_turns, may_edit) in [
        ("edit", "Read calculator.py and repair add(a, b) so it adds the two numbers. Change only that file using edit, then read it to verify the result. The external acceptance harness will execute the Python check. Finish with a concise account of the change and verification.", 4, true),
        ("audit", "Read calculator.py and validator.py. Perform a read-only correctness audit focused on whether valid_name rejects an empty name. Explain the concrete defect and its location. Do not change any files or delegate.", 3, false),
        ("greeting", "Hello. Briefly say hello; do not inspect or change files.", 1, false),
    ] {
        run_case(&evidence, name, prompt, max_turns, may_edit, Arc::clone(&client)).await?;
    }
    ensure!(
        file_sha256(&auth_file)? == auth_sha256,
        "source auth changed during acceptance; inspect concurrent activity"
    );
    std::fs::write(
        evidence.join("acceptance.json"),
        serde_json::to_vec_pretty(
            &json!({"passed":true,"model_key":selected,"executable_sha256":executable_sha256,"cases":["edit","audit","greeting"]}),
        )?,
    )?;
    println!(
        "real-provider candidate acceptance passed; evidence={}",
        evidence.display()
    );
    Ok(())
}

async fn run_case(
    evidence: &Path,
    name: &str,
    prompt: &str,
    max_turns: usize,
    may_edit: bool,
    client: Arc<AiClient>,
) -> Result<()> {
    let case = evidence.join(name);
    let workspace = case.join("workspace");
    std::fs::create_dir_all(&workspace)?;
    std::fs::write(
        workspace.join("calculator.py"),
        "def add(a, b):\n    return a - b\n",
    )?;
    std::fs::write(
        workspace.join("validator.py"),
        "def valid_name(name):\n    return len(name) >= 0\n",
    )?;
    std::fs::write(workspace.join("AGENTS.md"), "Keep changes inside this fixture. Do not delegate, use network tools, or access any parent directory. Report only evidence you actually inspected.\n")?;
    let before = workspace_snapshot(&workspace)?;
    let db_path = case.join("runtime.db");
    let sessions = SessionManager::new(Database::new(&db_path)?);
    let session_id = sessions.create_session(
        "Isolated live acceptance",
        Some(&client.config().model),
        workspace.to_str(),
    )?;
    let conversation = vec![ModelMessage {
        role: Role::User,
        content: vec![Content::text(prompt.to_owned())],
    }];
    sessions.save_message(
        &session_id,
        "user",
        &serde_json::to_string(&conversation[0].content)?,
    )?;
    let registry = Arc::new(ToolRegistry::new());
    registry.register(Arc::new(ReadTool)).await;
    if may_edit {
        registry.register(Arc::new(EditTool)).await;
    }
    let tools = registry.get_ai_tools_all().await;
    let allowlist: HashSet<String> = tools.iter().map(|tool| tool.name.clone()).collect();
    let spec = RunSpecBuilder::new(
        RunProvenance::Acp,
        session_id.clone(),
        workspace.clone(),
        SessionType::Code,
    )
    .project_dir(Some(workspace.clone()))
    .permission_mode(PermissionMode::Autonomous)
    .execution_tool_allowlist(Some(allowlist))
    .initial_work_mode(WorkMode::Build)
    .run_budget(Some(RunBudget::with_max_turns(max_turns)))
    .stream_idle_timeout(Duration::from_secs(60))
    .generate_title(false)
    .call_options(CallOptions {
        tools: Some(tools),
        max_tokens: Some(4096),
        session_id: Some(session_id.clone()),
        ..Default::default()
    })
    .build(&client)?;
    let services = OrchestratorServices {
        ai_client: client,
        tool_registry: registry,
        process_registry: Arc::new(ProcessRegistry::new()),
        db_path: db_path.clone(),
        skills_manager: Arc::new(RwLock::new(SkillsManager::new(case.join("skills"), None))),
    };
    let (mut events, input) = spec.start(services, conversation);
    let mut log = std::fs::File::create(case.join("events.jsonl"))?;
    let mut answer = String::new();
    let mut tools_executed = Vec::new();
    let mut provider_turns = 0;
    let mut terminal = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    while let Ok(Some(event)) = tokio::time::timeout_at(deadline, events.recv()).await {
        match &event {
            LoopEvent::TextDelta { delta } | LoopEvent::TextDeltaWithCitations { delta, .. } => {
                answer.push_str(delta)
            }
            LoopEvent::ToolExecuting { name, .. } => tools_executed.push(name.clone()),
            LoopEvent::ProviderRequestPrepared { .. } => provider_turns += 1,
            LoopEvent::Finished { stop_reason, .. } => terminal = Some(stop_reason.clone()),
            LoopEvent::ToolApprovalRequired { id, .. } => {
                input.send(LoopInput::ToolApproval {
                    tool_call_id: id.clone(),
                    approved: false,
                })?;
            }
            _ => {}
        }
        // Keep exact public prose/tool evidence, without private reasoning or
        // raw provider payloads. Every readable file belongs to the fixture.
        if !matches!(
            &event,
            LoopEvent::ThinkingDelta { .. } | LoopEvent::ThinkingComplete { .. }
        ) {
            serde_json::to_writer(&mut log, &event)?;
            log.write_all(b"\n")?;
            log.flush()?;
        }
        if terminal.is_some() {
            break;
        }
    }
    if terminal.is_none() {
        let _ = input.send(LoopInput::Cancel);
        let shutdown = tokio::time::Instant::now() + Duration::from_secs(10);
        while let Ok(Some(event)) = tokio::time::timeout_at(shutdown, events.recv()).await {
            if matches!(event, LoopEvent::Finished { .. }) {
                break;
            }
        }
    }
    let after = workspace_snapshot(&workspace)?;
    let persisted = SessionManager::new(Database::open_read_only(&db_path)?)
        .load_session_messages(&session_id)?;
    let result = json!({"case":name,"session_id":session_id,"terminal":terminal,"provider_turns":provider_turns,"tools_executed":tools_executed,"answer":answer,"before":before,"after":after,"durable_assistant_messages":persisted.iter().filter(|(role,_)|role=="assistant").count()});
    std::fs::write(
        case.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    ensure!(
        terminal == Some(LoopStopReason::Completed),
        "{name}: no completed terminal event; review private evidence"
    );
    ensure!(
        provider_turns <= max_turns && !answer.trim().is_empty(),
        "{name}: turn budget or answer check failed"
    );
    ensure!(
        persisted.iter().any(|(role, _)| role == "assistant"),
        "{name}: assistant answer was not durable"
    );
    if may_edit {
        ensure!(
            tools_executed.iter().any(|tool| tool == "edit")
                && tools_executed.iter().any(|tool| tool == "read"),
            "edit: real read/edit execution missing"
        );
        let changed = before
            .keys()
            .chain(after.keys())
            .filter(|path| before.get(*path) != after.get(*path))
            .collect::<HashSet<_>>();
        ensure!(
            changed.len() == 1 && changed.iter().all(|path| path.as_str() == "calculator.py"),
            "edit: changes escaped calculator.py"
        );
        // Only execute the exact permitted arithmetic AST; generated imports,
        // calls, filesystem operations, or extra statements fail before exec.
        let check = r#"import ast, pathlib, sys
tree = ast.parse(pathlib.Path(sys.argv[1]).read_text())
expected = ast.parse('def add(a, b):\n    return a + b\n')
assert ast.dump(tree) == ast.dump(expected)
scope = {'__builtins__': {}}
exec(compile(tree, '<verified fixture>', 'exec'), scope)
add = scope['add']
assert [add(2, 3), add(-4, 6), add(0, 0)] == [5, 2, 0]
"#;
        let status = std::process::Command::new("python3")
            .args(["-I", "-c", check])
            .arg(workspace.join("calculator.py"))
            .status()?;
        ensure!(
            status.success(),
            "edit: independent arithmetic check failed"
        );
    } else {
        ensure!(before == after, "{name}: read-only workspace changed");
        if name == "greeting" {
            ensure!(
                tools_executed.is_empty(),
                "greeting: unnecessary tool execution"
            );
        } else {
            ensure!(
                tools_executed.iter().any(|tool| tool == "read")
                    && answer.contains("validator.py")
                    && answer.to_lowercase().contains("empty"),
                "audit: expected grounded defect missing"
            );
        }
    }
    std::fs::write(
        case.join("checks.json"),
        serde_json::to_vec_pretty(&json!({"passed":true,"case":name}))?,
    )?;
    Ok(())
}
