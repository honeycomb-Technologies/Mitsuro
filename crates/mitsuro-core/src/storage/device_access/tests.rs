use super::*;

fn store(path: &std::path::Path) -> DeviceAccessStore {
    DeviceAccessStore::new(Database::new(path).unwrap())
}

#[test]
fn bootstrap_never_authenticates_and_grants_survive_restart_and_revoke_individually() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("access.db");
    let mut access = store(&path);
    let invite = access
        .create_invitation("https://owner.example", 100)
        .unwrap();
    assert_eq!(access.authenticate(&invite.secret, 101).unwrap(), None);
    assert!(access
        .claim(&invite.secret, "https://wrong.example", "Phone", 101)
        .is_err());
    let claim = access
        .claim(&invite.secret, &invite.origin, "Phone", 101)
        .unwrap();
    assert_eq!(access.authenticate(&claim.credential, 102).unwrap(), None);
    assert_eq!(
        access.status(&claim.credential, 102).unwrap(),
        PairingStatus::Pending
    );
    assert!(access
        .claim(&invite.secret, &invite.origin, "Other phone", 102)
        .is_err());
    let device = access.approve(&invite.id, 103).unwrap();
    assert!(access.approve(&invite.id, 103).is_err());
    let second = access.create_invitation(&invite.origin, 104).unwrap();
    let other_claim = access
        .claim(&second.secret, &second.origin, "Tablet", 105)
        .unwrap();
    access.approve(&second.id, 106).unwrap();
    drop(access);
    let mut access = store(&path);
    // The approval response can be lost: the client retains its own claim
    // credential and can discover the committed grant after either restart.
    assert_eq!(
        access.status(&claim.credential, 107).unwrap(),
        PairingStatus::Approved {
            device_id: device.id.clone()
        }
    );
    access.revoke(&device.id, 108).unwrap();
    assert_eq!(
        access.status(&claim.credential, 109).unwrap(),
        PairingStatus::Unavailable
    );
    assert!(!access.active(&device.id, 109).unwrap());
    assert!(access
        .authenticate(&other_claim.credential, 109)
        .unwrap()
        .is_some());
}

#[test]
fn expiry_and_cancellation_fail_closed_at_the_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let mut access = store(&temp.path().join("access.db"));
    let invitation = access
        .create_invitation("https://owner.example", 100)
        .unwrap();
    assert!(access
        .claim(&invitation.secret, &invitation.origin, "Phone", 220)
        .is_err());
    let invitation = access.create_invitation(&invitation.origin, 300).unwrap();
    let claim = access
        .claim(&invitation.secret, &invitation.origin, "Phone", 301)
        .unwrap();
    assert!(access.approve(&invitation.id, 420).is_err());
    assert_eq!(
        access.status(&claim.credential, 420).unwrap(),
        PairingStatus::Unavailable
    );
    let invitation = access.create_invitation(&invitation.origin, 500).unwrap();
    access
        .claim(&invitation.secret, &invitation.origin, "Phone", 501)
        .unwrap();
    access.cancel(&invitation.id).unwrap();
    assert!(access.approve(&invitation.id, 502).is_err());
    let invitation = access.create_invitation(&invitation.origin, 600).unwrap();
    let claim = access
        .claim(&invitation.secret, &invitation.origin, "Phone", 601)
        .unwrap();
    let device = access.approve(&invitation.id, 602).unwrap();
    assert!(access
        .authenticate(&claim.credential, device.expires_at - 1)
        .unwrap()
        .is_some());
    assert_eq!(
        access
            .authenticate(&claim.credential, device.expires_at)
            .unwrap(),
        None
    );
}

#[test]
fn concurrent_claims_and_approvals_have_one_winner() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("access.db");
    let invitation = store(&path)
        .create_invitation("https://owner.example", 100)
        .unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let barrier = barrier.clone();
            let path = path.clone();
            let secret = invitation.secret.clone();
            std::thread::spawn(move || {
                let mut access = store(&path);
                barrier.wait();
                access
                    .claim(&secret, "https://owner.example", "Phone", 101)
                    .is_ok()
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|w| usize::from(w.join().unwrap()))
            .sum::<usize>(),
        1
    );
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let barrier = barrier.clone();
            let path = path.clone();
            let id = invitation.id.clone();
            std::thread::spawn(move || {
                let mut access = store(&path);
                barrier.wait();
                access.approve(&id, 102).is_ok()
            })
        })
        .collect();
    assert_eq!(
        workers
            .into_iter()
            .map(|w| usize::from(w.join().unwrap()))
            .sum::<usize>(),
        1
    );
    assert_eq!(store(&path).devices(103).unwrap().len(), 1);
}

#[test]
fn database_and_metadata_never_expose_reusable_secrets() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("access.db");
    let mut access = store(&path);
    let invitation = access
        .create_invitation("https://owner.example", 100)
        .unwrap();
    let claim = access
        .claim(&invitation.secret, &invitation.origin, "Phone", 101)
        .unwrap();
    access.approve(&invitation.id, 102).unwrap();
    let stored: (String, String) = access
        .db
        .conn()
        .query_row(
            "SELECT secret_hash, claim_hash FROM device_pairing_invites",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        stored,
        (
            secret_hash(&invitation.secret),
            secret_hash(&claim.credential)
        )
    );
    let metadata = serde_json::to_string(&access.devices(103).unwrap()).unwrap();
    assert!(!metadata.contains(&claim.credential));
    assert!(!metadata.contains("token_hash"));
}

#[test]
fn migration_preserves_existing_owner_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("access.db");
    let db = Database::new(&path).unwrap();
    super::super::Preferences::new(db)
        .set("server_remote_access_token", "existing-owner-credential")
        .unwrap();
    let db = Database::new(&path).unwrap();
    db.conn().execute_batch("DROP TABLE device_pairing_invites; DROP TABLE paired_devices; DELETE FROM schema_version WHERE version=81;").unwrap();
    drop(db);
    let db = Database::new(&path).unwrap();
    assert_eq!(db.get_schema_version(), 81);
    assert_eq!(
        super::super::Preferences::new(db)
            .get("server_remote_access_token")
            .unwrap(),
        "existing-owner-credential"
    );
    assert!(store(&path).devices(100).unwrap().is_empty());
}

#[test]
fn invitation_count_and_device_names_are_bounded() {
    let temp = tempfile::tempdir().unwrap();
    let mut access = store(&temp.path().join("access.db"));
    let invitation = access
        .create_invitation("https://owner.example", 100)
        .unwrap();
    for name in ["", "   ", "Phone\nOther", &"x".repeat(81)] {
        assert!(matches!(
            access.claim(&invitation.secret, &invitation.origin, name, 101),
            Err(DeviceAccessError::InvalidName)
        ));
    }
    for _ in 0..3 {
        access.create_invitation(&invitation.origin, 100).unwrap();
    }
    assert!(matches!(
        access.create_invitation(&invitation.origin, 100),
        Err(DeviceAccessError::Limit)
    ));
    assert!(access.create_invitation(&invitation.origin, 220).is_ok());
}

#[test]
fn stream_tickets_are_single_use_bound_expiring_and_revocable() {
    let temp = tempfile::tempdir().unwrap();
    let mut access = store(&temp.path().join("tickets.db"));
    let invite = access
        .create_invitation("https://owner.example", 100)
        .unwrap();
    let claim = access
        .claim(&invite.secret, &invite.origin, "Phone", 101)
        .unwrap();
    let device = access.approve(&invite.id, 102).unwrap();
    let ticket = access
        .create_stream_ticket(&device.id, "/ws/terminal", 103)
        .unwrap();
    assert_eq!(access.authenticate(&ticket, 104).unwrap(), None);
    assert_eq!(
        access
            .consume_stream_ticket(&ticket, "/ws/terminal", 104)
            .unwrap(),
        Some(device.id.clone())
    );
    assert_eq!(
        access
            .consume_stream_ticket(&ticket, "/ws/terminal", 104)
            .unwrap(),
        None
    );
    let ticket = access
        .create_stream_ticket(&device.id, "/ws/terminal", 103)
        .unwrap();
    assert_eq!(
        access
            .consume_stream_ticket(&ticket, "/api/probe", 104)
            .unwrap(),
        None
    );
    assert_eq!(
        access
            .consume_stream_ticket(&ticket, "/ws/terminal", 104)
            .unwrap(),
        None
    );
    let ticket = access
        .create_stream_ticket(&device.id, "/ws/terminal", 103)
        .unwrap();
    assert_eq!(
        access
            .consume_stream_ticket(&ticket, "/ws/terminal", 133)
            .unwrap(),
        None
    );
    let ticket = access
        .create_stream_ticket(&device.id, "/ws/terminal", 103)
        .unwrap();
    access.revoke(&device.id, 104).unwrap();
    assert_eq!(
        access
            .consume_stream_ticket(&ticket, "/ws/terminal", 105)
            .unwrap(),
        None
    );
    assert_eq!(access.authenticate(&claim.credential, 105).unwrap(), None);
}
