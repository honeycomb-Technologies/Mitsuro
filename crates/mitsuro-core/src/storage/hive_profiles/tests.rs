use std::fs;

use tempfile::TempDir;

use crate::paths;
use crate::storage::Database;

use super::{
    default_profile_seed, HiveCrewProfileDocumentKind, HiveCrewProfileSeed,
    HiveProfileDocumentKind, HiveProfileOwner, HiveProfileSeed, HiveProfileStore,
    HiveProfileStoreError,
};

fn test_store() -> (TempDir, HiveProfileStore) {
    let temp = TempDir::new().unwrap();
    let db = Database::new(&temp.path().join("mitsuro.db")).unwrap();
    (temp, HiveProfileStore::new(db))
}

#[test]
fn profile_owner_ids_are_deterministic_and_workspace_independent() {
    let alice_one = HiveProfileOwner::user("alice").unwrap();
    let alice_two = HiveProfileOwner::from_user_id(Some("alice")).unwrap();
    let bob = HiveProfileOwner::user("bob").unwrap();

    assert_eq!(HiveProfileOwner::local().profile_id(), "local");
    assert_eq!(alice_one.profile_id(), alice_two.profile_id());
    assert_ne!(alice_one.profile_id(), bob.profile_id());
    assert!(!alice_one.profile_id().contains("alice"));
}

#[test]
fn get_or_create_is_idempotent_and_isolates_users() {
    let (_temp, store) = test_store();
    let alice = HiveProfileOwner::user("alice").unwrap();
    let bob = HiveProfileOwner::user("bob").unwrap();

    let first = store.get_or_create(&alice).unwrap();
    let second = store.get_or_create(&alice).unwrap();
    let bob_snapshot = store.get_or_create(&bob).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.revision, 0);
    assert_eq!(first.user_id.as_deref(), Some("alice"));
    assert_eq!(bob_snapshot.user_id.as_deref(), Some("bob"));
    assert_ne!(first.profile_id, bob_snapshot.profile_id);
}

#[test]
fn load_rejects_a_stored_owner_mismatch() {
    let temp = TempDir::new().unwrap();
    let db = Database::new(&temp.path().join("mitsuro.db")).unwrap();
    db.conn()
        .execute(
            "INSERT INTO hive_profiles (id, user_id) VALUES ('local', 'unexpected-user')",
            [],
        )
        .unwrap();
    let store = HiveProfileStore::new(db);

    let error = store.load(&HiveProfileOwner::local()).unwrap_err();

    assert!(matches!(
        error,
        HiveProfileStoreError::OwnerCollision { profile_id } if profile_id == "local"
    ));
}

#[test]
fn document_updates_use_optimistic_profile_revisions() {
    let (_temp, store) = test_store();
    let owner = HiveProfileOwner::user("alice").unwrap();
    let initial = store.get_or_create(&owner).unwrap();

    let updated = store
        .update_document(
            &owner,
            HiveProfileDocumentKind::Soul,
            "  Stay curious.  ",
            initial.revision,
        )
        .unwrap();
    assert_eq!(updated.revision, 1);
    assert_eq!(updated.soul.unwrap().content, "Stay curious.");

    let error = store
        .update_document(
            &owner,
            HiveProfileDocumentKind::Identity,
            "Name: stale",
            initial.revision,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        HiveProfileStoreError::RevisionConflict {
            expected: 0,
            actual: 1
        }
    ));
}

#[test]
fn crew_updates_advance_both_profile_and_member_revision() {
    let (_temp, store) = test_store();
    let owner = HiveProfileOwner::local();
    let initial = store.get_or_create(&owner).unwrap();

    let first = store
        .update_crew_document(
            &owner,
            "reviewer",
            HiveCrewProfileDocumentKind::Identity,
            "Name: Reef",
            initial.revision,
        )
        .unwrap();
    let reviewer = first.crew_member("reviewer").unwrap();
    assert_eq!(first.revision, 1);
    assert_eq!(reviewer.revision, 1);
    assert_eq!(reviewer.identity.as_ref().unwrap().content, "Name: Reef");

    let second = store
        .update_crew_document(
            &owner,
            "reviewer",
            HiveCrewProfileDocumentKind::Soul,
            "Evidence first.",
            first.revision,
        )
        .unwrap();
    assert_eq!(second.revision, 2);
    assert_eq!(second.crew_member("reviewer").unwrap().revision, 2);
}

#[test]
fn merge_missing_never_overwrites_and_replay_is_revision_stable() {
    let (_temp, store) = test_store();
    let owner = HiveProfileOwner::local();
    let seed = HiveProfileSeed {
        documents: vec![
            (HiveProfileDocumentKind::Soul, "Original soul".to_string()),
            (HiveProfileDocumentKind::User, "Original user".to_string()),
        ],
        crew: vec![HiveCrewProfileSeed {
            slug: "builder".to_string(),
            documents: vec![(
                HiveCrewProfileDocumentKind::Identity,
                "Original builder".to_string(),
            )],
        }],
    };

    let first = store.merge_missing(&owner, &seed).unwrap();
    assert_eq!(first.snapshot.revision, 1);
    assert_eq!(first.inserted_documents.len(), 2);
    assert_eq!(first.inserted_crew_documents.len(), 1);

    let replay = store.merge_missing(&owner, &seed).unwrap();
    assert_eq!(replay.snapshot.revision, 1);
    assert!(replay.inserted_documents.is_empty());
    assert!(replay.inserted_crew_documents.is_empty());

    let edited = store
        .update_document(
            &owner,
            HiveProfileDocumentKind::Soul,
            "User-edited soul",
            replay.snapshot.revision,
        )
        .unwrap();
    let replay_after_edit = store.merge_missing(&owner, &seed).unwrap();
    assert_eq!(replay_after_edit.snapshot.revision, edited.revision);
    assert_eq!(
        replay_after_edit.snapshot.soul.unwrap().content,
        "User-edited soul"
    );
}

#[test]
fn defaults_include_user_and_exclude_all_memory_documents() {
    let (_temp, store) = test_store();
    let owner = HiveProfileOwner::local();

    let result = store.bootstrap_defaults(&owner).unwrap();

    assert!(result.snapshot.soul.is_some());
    assert!(result.snapshot.identity.is_some());
    assert!(result.snapshot.user.is_some());
    assert!(result.snapshot.heartbeat.is_some());
    assert!(result.snapshot.channels.is_some());
    assert_eq!(result.snapshot.crew.len(), 3);
    assert!(result
        .snapshot
        .crew
        .iter()
        .all(|member| member.identity.is_some() && member.soul.is_some()));
}

#[test]
fn default_seed_keeps_personality_human_without_claiming_false_familiarity() {
    let seed = default_profile_seed();
    let soul = seed
        .documents
        .iter()
        .find(|(kind, _)| *kind == HiveProfileDocumentKind::Soul)
        .map(|(_, content)| content.as_str())
        .unwrap();
    let user = seed
        .documents
        .iter()
        .find(|(kind, _)| *kind == HiveProfileDocumentKind::User)
        .map(|(_, content)| content.as_str())
        .unwrap();

    assert!(soul.contains("warm, curious, and candid"));
    assert!(soul.contains("never emotionally flat"));
    assert!(soul.contains("never fake familiarity"));
    assert!(soul.contains("never fake familiarity, manipulate, flatter, or invent memory"));
    assert!(user.contains("user-authored"));
    assert!(user.contains("Do not store secrets"));
    assert!(user.contains("present guesses as facts"));
}

#[test]
fn local_legacy_import_is_idempotent_and_excludes_memory() {
    let (temp, store) = test_store();
    let legacy_home = temp.path().join("legacy-hive");
    fs::create_dir_all(legacy_home.join("crew").join("researcher")).unwrap();
    fs::write(legacy_home.join(paths::HIVE_SOUL_FILE), "Legacy soul").unwrap();
    fs::write(legacy_home.join(paths::HIVE_USER_FILE), "Legacy user").unwrap();
    fs::write(legacy_home.join(paths::HIVE_MEMORY_FILE), "Do not activate").unwrap();
    fs::write(
        legacy_home.join("crew").join("researcher").join("SOUL.md"),
        "Research deeply",
    )
    .unwrap();
    fs::write(
        legacy_home
            .join("crew")
            .join("researcher")
            .join("MEMORY.md"),
        "Do not activate this either",
    )
    .unwrap();
    let owner = HiveProfileOwner::local();

    let first = store
        .import_local_legacy_home(&owner, &legacy_home)
        .unwrap();
    assert!(first.excluded_memory);
    assert_eq!(first.excluded_crew_memory_count, 1);
    assert_eq!(first.merge.snapshot.soul.unwrap().content, "Legacy soul");
    assert_eq!(first.merge.snapshot.user.unwrap().content, "Legacy user");
    assert_eq!(first.merge.snapshot.revision, 1);

    let replay = store
        .import_local_legacy_home(&owner, &legacy_home)
        .unwrap();
    assert_eq!(replay.merge.snapshot.revision, 1);
    assert!(replay.merge.inserted_documents.is_empty());
    assert!(replay.merge.inserted_crew_documents.is_empty());
}

#[test]
fn legacy_import_rejects_authenticated_profile_targets() {
    let (temp, store) = test_store();
    let owner = HiveProfileOwner::user("alice").unwrap();

    let error = store
        .import_local_legacy_home(&owner, temp.path())
        .unwrap_err();

    assert!(matches!(
        error,
        HiveProfileStoreError::LegacyImportRequiresLocalOwner
    ));
}

#[test]
fn profile_documents_are_bounded_before_persistence() {
    let (_temp, store) = test_store();
    let owner = HiveProfileOwner::local();
    let initial = store.get_or_create(&owner).unwrap();
    let oversized = "x".repeat(super::MAX_HIVE_PROFILE_DOCUMENT_BYTES + 1);

    let error = store
        .update_document(
            &owner,
            HiveProfileDocumentKind::Soul,
            &oversized,
            initial.revision,
        )
        .unwrap_err();

    assert!(matches!(error, HiveProfileStoreError::ContentTooLarge));
    assert_eq!(
        store.load(&owner).unwrap().unwrap().revision,
        initial.revision
    );
}
