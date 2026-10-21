use tempfile::TempDir;

use super::{
    CanvasNode, CanvasNodeBinding, CanvasOperation, CanvasPatchInput, CanvasResourceCreateClaim,
    CanvasResourceKind, CreateCanvasInput, ReserveCanvasResourceCreateInput, SpatialCanvasStore,
    SpatialCanvasSurface, WorldRect,
};
use crate::storage::Database;

fn store_in(temp_dir: &TempDir) -> SpatialCanvasStore {
    SpatialCanvasStore::new(
        Database::new(&temp_dir.path().join("canvas.db")).expect("database should initialize"),
    )
}

fn create_input(op: &str, project: &str, name: &str) -> CreateCanvasInput {
    CreateCanvasInput {
        create_op_id: op.to_string(),
        project_dir: project.to_string(),
        surface: SpatialCanvasSurface::Code,
        name: name.to_string(),
        shared_theme_id: "graphite".to_string(),
    }
}

fn changes_node(id: &str) -> CanvasNode {
    CanvasNode {
        id: id.to_string(),
        title: "Agent".to_string(),
        rect: WorldRect {
            x: -320.0,
            y: 48.0,
            width: 640.0,
            height: 480.0,
        },
        z_index: 2,
        binding: CanvasNodeBinding::Changes,
        revision: 0,
    }
}

#[test]
fn migration_80_creates_canvas_snapshot_event_and_resource_intent_tables() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db = Database::new(&temp_dir.path().join("migration.db")).expect("database");
    assert_eq!(db.get_schema_version(), 80);
    for table in [
        "spatial_canvases",
        "spatial_canvas_events",
        "spatial_canvas_resource_creates",
    ] {
        let exists: bool = db
            .conn()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                [table],
                |row| row.get(0),
            )
            .expect("table lookup");
        assert!(exists, "{table} should be created by migration 80");
    }
}

#[test]
fn exact_owner_queries_do_not_treat_local_null_as_wildcard() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    let local = store
        .create_for_exact_owner(None, create_input("local-op", "/tmp/local", "Main"))
        .expect("create local")
        .canvas;
    let alice = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("alice-op", "/tmp/alice", "Main"),
        )
        .expect("create alice")
        .canvas;

    let local_ids = store
        .list_for_exact_owner(None, None)
        .expect("list local")
        .into_iter()
        .map(|canvas| canvas.id)
        .collect::<Vec<_>>();
    assert_eq!(local_ids.as_slice(), std::slice::from_ref(&local.id));
    assert!(store
        .get_for_exact_owner(&alice.id, None)
        .expect("query local")
        .is_none());
    assert!(store
        .get_for_exact_owner(&local.id, Some("alice"))
        .expect("query alice")
        .is_none());
}

#[test]
fn create_and_patch_operations_are_idempotent() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    let first = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("create-once", "/tmp/project", "Main"),
        )
        .expect("first create");
    let replay = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("create-once", "/tmp/project", "Main"),
        )
        .expect("create replay");
    assert!(first.created);
    assert!(!replay.created);
    assert_eq!(first.canvas.id, replay.canvas.id);

    let patch = CanvasPatchInput {
        op_id: "patch-once".to_string(),
        client_id: "client-a".to_string(),
        expected_revision: 0,
        operations: vec![CanvasOperation::UpsertNode {
            node: changes_node("agent-1"),
        }],
    };
    let applied = store
        .patch_for_exact_owner(&first.canvas.id, Some("alice"), patch.clone())
        .expect("apply patch");
    let replayed = store
        .patch_for_exact_owner(&first.canvas.id, Some("alice"), patch)
        .expect("replay patch");
    assert!(applied.applied);
    assert!(!replayed.applied);
    assert_eq!(applied.canvas.revision, 1);
    assert_eq!(replayed.canvas.revision, 1);
    assert_eq!(replayed.canvas.state.nodes.len(), 1);

    let events = store
        .list_events_after_for_exact_owner(&first.canvas.id, Some("alice"), 0, 256)
        .expect("event page")
        .expect("owned canvas");
    assert_eq!(events.events.len(), 1);
    assert_eq!(events.events[0].op_id, "patch-once");

    store
        .patch_for_exact_owner(
            &first.canvas.id,
            Some("alice"),
            CanvasPatchInput {
                op_id: "rename-after-create".to_string(),
                client_id: "client-a".to_string(),
                expected_revision: 1,
                operations: vec![CanvasOperation::RenameCanvas {
                    name: "Renamed".to_string(),
                }],
            },
        )
        .expect("rename after create");
    let late_create_replay = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("create-once", "/tmp/project", "Main"),
        )
        .expect("late create replay");
    assert!(!late_create_replay.created);
    assert_eq!(late_create_replay.canvas.name, "Renamed");
}

#[test]
fn replay_lookups_do_not_depend_on_mutable_resources() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    let created = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("durable-create", "/tmp/project", "Main"),
        )
        .expect("create canvas")
        .canvas;

    let (create_replay, stored_request) = store
        .create_replay_for_exact_owner(Some("alice"), "durable-create")
        .expect("lookup create replay")
        .expect("stored create operation");
    assert!(!create_replay.created);
    assert_eq!(create_replay.canvas.id, created.id);
    assert_eq!(stored_request.project_dir, "/tmp/project");

    let patch = CanvasPatchInput {
        op_id: "durable-patch".to_string(),
        client_id: "client-a".to_string(),
        expected_revision: 0,
        operations: vec![CanvasOperation::UpsertNode {
            node: changes_node("changes"),
        }],
    };
    store
        .patch_for_exact_owner(&created.id, Some("alice"), patch.clone())
        .expect("apply patch");
    let replay = store
        .patch_replay_for_exact_owner(&created.id, Some("alice"), &patch)
        .expect("lookup patch replay")
        .expect("stored patch operation");
    assert!(!replay.applied);
    assert_eq!(replay.canvas.revision, 1);

    let mut conflicting = patch;
    conflicting.client_id = "client-b".to_string();
    assert!(store
        .patch_replay_for_exact_owner(&created.id, Some("alice"), &conflicting)
        .expect_err("conflicting retry must fail")
        .to_string()
        .contains("different input"));
}

#[test]
fn create_operation_ids_are_idempotent_within_exact_owner_only() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    let alice = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("shared-client-op", "/tmp/alice", "Main"),
        )
        .expect("alice create")
        .canvas;
    let conflicting_alice_reuse = store.create_for_exact_owner(
        Some("alice"),
        create_input("shared-client-op", "/tmp/other", "Main"),
    );
    assert!(conflicting_alice_reuse
        .expect_err("same owner cannot reuse an operation id with different input")
        .to_string()
        .contains("different input"));
    let bob = store
        .create_for_exact_owner(
            Some("bob"),
            create_input("shared-client-op", "/tmp/bob", "Main"),
        )
        .expect("bob create")
        .canvas;

    assert_ne!(alice.id, bob.id);
    assert_eq!(alice.owner_user_id.as_deref(), Some("alice"));
    assert_eq!(bob.owner_user_id.as_deref(), Some("bob"));
}

#[test]
fn duplicate_canvas_names_conflict_for_local_and_tenant_owners() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    for owner in [None, Some("alice")] {
        let prefix = owner.unwrap_or("local");
        store
            .create_for_exact_owner(
                owner,
                create_input(&format!("{prefix}-one"), "/tmp/project", "Main"),
            )
            .expect("first named canvas");
        let duplicate = store.create_for_exact_owner(
            owner,
            create_input(&format!("{prefix}-two"), "/tmp/project", "Main"),
        );
        assert!(duplicate
            .expect_err("duplicate canvas name should fail")
            .to_string()
            .contains("already exists"));
    }
}

#[test]
fn stale_revision_is_rejected_without_appending_an_event() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    let canvas = store
        .create_for_exact_owner(None, create_input("create", "/tmp/project", "Main"))
        .expect("create")
        .canvas;
    store
        .patch_for_exact_owner(
            &canvas.id,
            None,
            CanvasPatchInput {
                op_id: "first".to_string(),
                client_id: "client".to_string(),
                expected_revision: 0,
                operations: vec![CanvasOperation::UpsertNode {
                    node: changes_node("agent"),
                }],
            },
        )
        .expect("first patch");
    let stale = store.patch_for_exact_owner(
        &canvas.id,
        None,
        CanvasPatchInput {
            op_id: "stale".to_string(),
            client_id: "client".to_string(),
            expected_revision: 0,
            operations: vec![CanvasOperation::RemoveNode {
                node_id: "agent".to_string(),
            }],
        },
    );
    assert!(stale
        .expect_err("stale patch")
        .to_string()
        .contains("revision conflict"));
    let page = store
        .list_events_after_for_exact_owner(&canvas.id, None, 0, 256)
        .expect("page")
        .expect("owned");
    assert_eq!(page.events.len(), 1);
}

#[test]
fn replay_ledger_is_bounded_and_reports_trimmed_gaps() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("canvas.db");
    let store = SpatialCanvasStore::new(Database::new(&db_path).expect("database"));
    let canvas = store
        .create_for_exact_owner(None, create_input("create", "/tmp/project", "Main"))
        .expect("create")
        .canvas;

    for revision in 0..2_050_u64 {
        store
            .patch_for_exact_owner(
                &canvas.id,
                None,
                CanvasPatchInput {
                    op_id: format!("patch-{revision}"),
                    client_id: "client".to_string(),
                    expected_revision: revision,
                    operations: vec![CanvasOperation::SetSharedTheme {
                        theme_id: "graphite".to_string(),
                    }],
                },
            )
            .expect("patch");
    }

    let page = store
        .list_events_after_for_exact_owner(&canvas.id, None, 0, 256)
        .expect("page")
        .expect("owned");
    assert!(page.gap);
    assert_eq!(page.earliest_sequence, Some(3));
    assert_eq!(page.latest_sequence, 2_050);
    assert_eq!(page.events.first().map(|event| event.sequence), Some(3));

    let future_cursor = store
        .list_events_after_for_exact_owner(&canvas.id, None, 9_999, 256)
        .expect("future cursor page")
        .expect("owned");
    assert!(future_cursor.gap);
    assert!(future_cursor.events.is_empty());

    let event_count: u64 = Database::new(&db_path)
        .expect("database")
        .conn()
        .query_row(
            "SELECT COUNT(*) FROM spatial_canvas_events WHERE canvas_id = ?1",
            [&canvas.id],
            |row| row.get(0),
        )
        .expect("event count");
    assert_eq!(event_count, 2_048);
}

#[test]
fn archived_canvas_can_be_restored_by_exact_owner() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    let canvas = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("archive-create", "/tmp/project", "Main"),
        )
        .expect("create")
        .canvas;
    let archived = store
        .patch_for_exact_owner(
            &canvas.id,
            Some("alice"),
            CanvasPatchInput {
                op_id: "archive".to_string(),
                client_id: "client".to_string(),
                expected_revision: 0,
                operations: vec![CanvasOperation::SetArchived { archived: true }],
            },
        )
        .expect("archive")
        .canvas;
    assert!(archived.archived_at.is_some());
    assert!(store
        .list_for_exact_owner(Some("alice"), None)
        .expect("list active")
        .is_empty());
    assert!(store
        .get_for_exact_owner(&canvas.id, None)
        .expect("other owner")
        .is_none());

    let restored = store
        .patch_for_exact_owner(
            &canvas.id,
            Some("alice"),
            CanvasPatchInput {
                op_id: "restore".to_string(),
                client_id: "client".to_string(),
                expected_revision: 1,
                operations: vec![CanvasOperation::SetArchived { archived: false }],
            },
        )
        .expect("restore")
        .canvas;
    assert!(restored.archived_at.is_none());
    assert_eq!(
        store
            .list_for_exact_owner(Some("alice"), None)
            .expect("list restored")
            .len(),
        1
    );
}

#[test]
fn agent_bindings_require_exact_owner_and_same_project() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("canvas.db");
    let db = Database::new(&db_path).expect("database");
    db.conn()
        .execute(
            "INSERT INTO users (id, email) VALUES ('alice', 'alice@canvas.test')",
            [],
        )
        .expect("seed exact-owner user");
    db.conn()
        .execute(
            "INSERT INTO sessions (
                 id, title, created_at, updated_at, working_dir, project_dir,
                 user_id, session_type
             ) VALUES ('alice-session', 'Agent', ?1, ?1, '/tmp/project',
                       '/tmp/project', 'alice', 'code')",
            [chrono::Utc::now().to_rfc3339()],
        )
        .expect("seed exact-owner session");
    drop(db);
    let store = store_in(&temp_dir);

    let alice = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("alice-canvas", "/tmp/project", "Main"),
        )
        .expect("alice canvas")
        .canvas;
    let agent = CanvasNode {
        binding: CanvasNodeBinding::Agent {
            session_id: "alice-session".to_string(),
        },
        ..changes_node("agent")
    };
    store
        .patch_for_exact_owner(
            &alice.id,
            Some("alice"),
            CanvasPatchInput {
                op_id: "bind-alice".to_string(),
                client_id: "client".to_string(),
                expected_revision: 0,
                operations: vec![CanvasOperation::UpsertNode {
                    node: agent.clone(),
                }],
            },
        )
        .expect("same owner and project binding");

    let bob = store
        .create_for_exact_owner(
            Some("bob"),
            create_input("bob-canvas", "/tmp/project", "Main"),
        )
        .expect("bob canvas")
        .canvas;
    let wrong_owner = store.patch_for_exact_owner(
        &bob.id,
        Some("bob"),
        CanvasPatchInput {
            op_id: "bind-bob".to_string(),
            client_id: "client".to_string(),
            expected_revision: 0,
            operations: vec![CanvasOperation::UpsertNode { node: agent }],
        },
    );
    assert!(
        wrong_owner
            .expect_err("cross-owner binding must fail")
            .to_string()
            .contains("already bound"),
        "already-bound live identities must be unique across canvases"
    );

    let db = Database::new(&db_path).expect("reopen database");
    db.conn()
        .execute(
            "INSERT INTO sessions (
                 id, title, created_at, updated_at, working_dir, project_dir,
                 user_id, session_type
             ) VALUES ('alice-unbound', 'Agent', ?1, ?1, '/tmp/project',
                       '/tmp/project', 'alice', 'code')",
            [chrono::Utc::now().to_rfc3339()],
        )
        .expect("seed unbound exact-owner session");
    drop(db);
    let unbound = CanvasNode {
        binding: CanvasNodeBinding::Agent {
            session_id: "alice-unbound".to_string(),
        },
        ..changes_node("agent-unbound")
    };
    let wrong_owner_unbound = store.patch_for_exact_owner(
        &bob.id,
        Some("bob"),
        CanvasPatchInput {
            op_id: "bind-bob-unbound".to_string(),
            client_id: "client".to_string(),
            expected_revision: 0,
            operations: vec![CanvasOperation::UpsertNode { node: unbound }],
        },
    );
    assert!(
        wrong_owner_unbound
            .expect_err("cross-owner unbound binding must fail")
            .to_string()
            .contains("exact-owner"),
        "unbound Agent sessions still require the exact canvas owner"
    );

    let other_project = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("other-project", "/tmp/other", "Main"),
        )
        .expect("other project canvas")
        .canvas;
    let db = Database::new(&db_path).expect("reopen database for project session");
    db.conn()
        .execute(
            "INSERT INTO sessions (
                 id, title, created_at, updated_at, working_dir, project_dir,
                 user_id, session_type
             ) VALUES ('alice-home-session', 'Agent', ?1, ?1, '/tmp/project',
                       '/tmp/project', 'alice', 'code')",
            [chrono::Utc::now().to_rfc3339()],
        )
        .expect("seed same-owner home-project session");
    drop(db);
    let home_agent = CanvasNode {
        binding: CanvasNodeBinding::Agent {
            session_id: "alice-home-session".to_string(),
        },
        ..changes_node("agent-home")
    };
    let wrong_project = store.patch_for_exact_owner(
        &other_project.id,
        Some("alice"),
        CanvasPatchInput {
            op_id: "bind-other".to_string(),
            client_id: "client".to_string(),
            expected_revision: 0,
            operations: vec![CanvasOperation::UpsertNode { node: home_agent }],
        },
    );
    assert!(wrong_project
        .expect_err("cross-project binding must fail")
        .to_string()
        .contains("same project"));
}

fn terminal_request_json(node_id: &str) -> String {
    format!(
        r#"{{"node":{{"id":"{node_id}","title":"Terminal","rect":{{"x":-240.0,"y":80.0,"width":640.0,"height":440.0}},"z_index":3}},"resource":{{"kind":"terminal","size":{{"cols":80,"rows":24}}}}}}"#
    )
}

#[test]
fn bound_terminal_detach_uses_generation_cas_and_replays_older_generations() {
    let temp_dir = TempDir::new().expect("temp dir");
    let store = store_in(&temp_dir);
    let canvas = store
        .create_for_exact_owner(
            Some("alice"),
            create_input("create-term-canvas", "/tmp/project", "Main"),
        )
        .expect("create canvas")
        .canvas;
    let node_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeee1";
    let request_json = terminal_request_json(node_id);
    let reserved = store
        .reserve_resource_create_for_exact_owner(
            &canvas.id,
            Some("alice"),
            ReserveCanvasResourceCreateInput {
                create_op_id: "create-term-1".to_string(),
                bind_op_id: "bind-term-1".to_string(),
                client_id: "client-a".to_string(),
                node_id: node_id.to_string(),
                resource_kind: CanvasResourceKind::Terminal,
                request_json: request_json.clone(),
            },
        )
        .expect("reserve terminal");
    assert!(reserved.created);
    assert_eq!(reserved.intent.detach_generation, 0);

    let CanvasResourceCreateClaim::Acquired(claimed) = store
        .claim_resource_create_for_exact_owner(
            &canvas.id,
            Some("alice"),
            "create-term-1",
            "executor-a",
            false,
        )
        .expect("claim")
    else {
        panic!("expected an acquired create claim");
    };
    let claim_token = claimed.claim_token.expect("claim token");
    store
        .mark_resource_create_provisioned_for_exact_owner(
            &canvas.id,
            Some("alice"),
            "create-term-1",
            "executor-a",
            &claim_token,
            r#"{"pid":1}"#,
        )
        .expect("provision");
    let bound = store
        .bind_resource_create_for_exact_owner(
            &canvas.id,
            Some("alice"),
            "create-term-1",
            "executor-a",
            &claim_token,
        )
        .expect("bind");
    assert!(bound.state.nodes.iter().any(|node| node.id == node_id));

    let ahead = store.detach_bound_resource_for_exact_owner(
        &canvas.id,
        Some("alice"),
        "create-term-1",
        1,
        &request_json,
    );
    assert!(
        ahead
            .expect_err("future detach generation must fail")
            .to_string()
            .contains("ahead"),
        "CAS must reject a detach generation the durable intent has not reached"
    );

    let (intent, detached, binding_absent, _ack) = store
        .detach_bound_resource_for_exact_owner(
            &canvas.id,
            Some("alice"),
            "create-term-1",
            0,
            &request_json,
        )
        .expect("first detach");
    assert!(binding_absent);
    assert_eq!(intent.detach_generation, 1);
    assert!(detached.state.nodes.iter().all(|node| node.id != node_id));

    let (replay, _, replay_absent, _) = store
        .detach_bound_resource_for_exact_owner(
            &canvas.id,
            Some("alice"),
            "create-term-1",
            0,
            &request_json,
        )
        .expect("older generation replay");
    assert!(replay_absent);
    assert_eq!(replay.detach_generation, 1);

    let mismatched = store.detach_bound_resource_for_exact_owner(
        &canvas.id,
        Some("alice"),
        "create-term-1",
        1,
        &request_json.replace("Terminal", "Other"),
    );
    assert!(
        mismatched
            .expect_err("fingerprint mismatch")
            .to_string()
            .contains("does not match"),
        "detach CAS must reject a different create intent payload"
    );
}
