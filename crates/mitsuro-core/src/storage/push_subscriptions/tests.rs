use super::PushSubscriptionStore;
use crate::storage::database::Database;
use tempfile::TempDir;

fn test_db() -> (TempDir, Database) {
    let temp = TempDir::new().expect("temp dir should exist");
    let db = Database::new(&temp.path().join("test.db")).expect("database should initialize");
    (temp, db)
}

#[test]
fn remove_by_endpoint_for_user_respects_multi_tenant_ownership() {
    let (_temp, db) = test_db();
    let store = PushSubscriptionStore::new(&db);
    let endpoint = "https://push.example.test/subscription";

    store
        .upsert(
            Some("alice"),
            endpoint,
            "BCV4QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3Q",
            "abcdefghijklmnop",
        )
        .expect("subscription should insert");

    let removed = store
        .remove_by_endpoint_for_user(Some("bob"), endpoint)
        .expect("scoped removal should succeed");
    assert!(!removed);
    assert_eq!(store.count_for_user(Some("alice")).unwrap(), 1);

    let removed = store
        .remove_by_endpoint_for_user(Some("alice"), endpoint)
        .expect("owner removal should succeed");
    assert!(removed);
    assert_eq!(store.count_for_user(Some("alice")).unwrap(), 0);
}

#[test]
fn remove_by_endpoint_for_user_scopes_single_tenant_rows_to_null_owner() {
    let (_temp, db) = test_db();
    let store = PushSubscriptionStore::new(&db);
    let endpoint = "https://push.example.test/local";

    store
        .upsert(
            None,
            endpoint,
            "BCV4QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3QxM2V3Q",
            "abcdefghijklmnop",
        )
        .expect("subscription should insert");

    let removed = store
        .remove_by_endpoint_for_user(None, endpoint)
        .expect("single-tenant removal should succeed");
    assert!(removed);
    assert_eq!(store.count_for_user(None).unwrap(), 0);
}
