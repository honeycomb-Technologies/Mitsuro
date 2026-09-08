//! Single-owner device enrollment. Bootstrap secrets never authorize normal API
//! requests; approving a specific claim creates a separately revocable grant.

use rand::RngCore;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::Database;

pub const STREAM_TICKET_PREFIX: &str = "mitsuro_stream_";
pub const DEVICE_TOKEN_PREFIX: &str = "mitsuro_device_";
pub const INVITATION_LIFETIME_SECONDS: i64 = 120;
pub const DEVICE_LIFETIME_SECONDS: i64 = 90 * 24 * 60 * 60;

#[derive(Debug, thiserror::Error)]
pub enum DeviceAccessError {
    #[error("Pairing invitation is invalid, expired, or already claimed")]
    InvalidInvitation,
    #[error("Pairing request is unavailable or expired")]
    InvalidClaim,
    #[error("Too many devices or pending invitations; revoke a device or wait for invitations to expire")]
    Limit,
    #[error("Device name must contain between 1 and 80 printable characters")]
    InvalidName,
    #[error("Secure random generation failed")]
    Random,
    #[error("Device access storage failed")]
    Storage(#[from] rusqlite::Error),
}

type Result<T> = std::result::Result<T, DeviceAccessError>;

// Deliberately no Debug: the creation response is the only place the raw
// invitation is exposed. Normal list/status responses contain no secrets.
#[derive(Serialize)]
pub struct PairingInvitation {
    pub id: String,
    pub secret: String,
    pub origin: String,
    pub expires_at: i64,
}

#[derive(Serialize)]
pub struct PairingClaim {
    pub credential: String,
    pub comparison_code: String,
    pub expires_at: i64,
}

#[derive(Debug, Serialize)]
pub struct PendingDevice {
    pub id: String,
    pub name: String,
    pub comparison_code: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairedDevice {
    pub id: String,
    pub name: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub revoked_at: Option<i64>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PairingStatus {
    Pending,
    Approved { device_id: String },
    Unavailable,
}

pub struct DeviceAccessStore {
    db: Database,
    tickets: std::collections::HashMap<String, (String, String, i64)>,
}

impl DeviceAccessStore {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            tickets: Default::default(),
        }
    }

    /// A one-use, route-bound credential for WebSocket clients without headers.
    pub fn create_stream_ticket(
        &mut self,
        device_id: &str,
        path: &str,
        now: i64,
    ) -> Result<String> {
        self.tickets.retain(|_, (_, _, expires)| *expires > now);
        if !self.active(device_id, now)?
            || !path.starts_with('/')
            || path.starts_with("//")
            || path.len() > 2048
            || path.contains(['?', '#', '\\'])
        {
            return Err(DeviceAccessError::InvalidClaim);
        }
        if self.tickets.len() >= 256
            || self
                .tickets
                .values()
                .filter(|(id, _, _)| id == device_id)
                .count()
                >= 16
        {
            return Err(DeviceAccessError::Limit);
        }
        let ticket = random_secret(STREAM_TICKET_PREFIX)?;
        self.tickets.insert(
            secret_hash(&ticket),
            (device_id.to_owned(), path.to_owned(), now + 30),
        );
        Ok(ticket)
    }

    pub fn consume_stream_ticket(
        &mut self,
        ticket: &str,
        path: &str,
        now: i64,
    ) -> Result<Option<String>> {
        if !valid_secret(ticket, STREAM_TICKET_PREFIX) {
            return Ok(None);
        }
        let Some((id, expected, expires)) = self.tickets.remove(&secret_hash(ticket)) else {
            return Ok(None);
        };
        if expected != path || expires <= now || !self.active(&id, now)? {
            return Ok(None);
        }
        Ok(Some(id))
    }

    pub fn create_invitation(&mut self, origin: &str, now: i64) -> Result<PairingInvitation> {
        let tx = self
            .db
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "DELETE FROM device_pairing_invites WHERE expires_at <= ?1 OR state IN ('approved', 'denied')",
            [now],
        )?;
        let pending: i64 =
            tx.query_row("SELECT COUNT(*) FROM device_pairing_invites", [], |r| {
                r.get(0)
            })?;
        if pending >= 4 {
            return Err(DeviceAccessError::Limit);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let secret = random_secret("mitsuro_pair_")?;
        let expires_at = now + INVITATION_LIFETIME_SECONDS;
        tx.execute(
            "INSERT INTO device_pairing_invites(id, secret_hash, origin, expires_at) VALUES (?1, ?2, ?3, ?4)",
            params![id, secret_hash(&secret), origin, expires_at],
        )?;
        tx.commit()?;
        Ok(PairingInvitation {
            id,
            secret,
            origin: origin.to_owned(),
            expires_at,
        })
    }

    /// Claim once. If the response is lost, create a fresh invitation; never
    /// retry a consumed invitation or reveal another claimant's credential.
    pub fn claim(
        &mut self,
        secret: &str,
        origin: &str,
        name: &str,
        now: i64,
    ) -> Result<PairingClaim> {
        let name = name.trim();
        if name.is_empty() || name.chars().count() > 80 || name.chars().any(char::is_control) {
            return Err(DeviceAccessError::InvalidName);
        }
        if !valid_secret(secret, "mitsuro_pair_") {
            return Err(DeviceAccessError::InvalidInvitation);
        }
        let tx = self
            .db
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let invitation: Option<(String, i64)> = tx.query_row(
            "SELECT id, expires_at FROM device_pairing_invites WHERE secret_hash=?1 AND origin=?2 AND expires_at>?3 AND state='available'",
            params![secret_hash(secret), origin, now], |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        let (id, expires_at) = invitation.ok_or(DeviceAccessError::InvalidInvitation)?;
        let credential = random_secret(DEVICE_TOKEN_PREFIX)?;
        let code_bytes = Sha256::digest(credential.as_bytes());
        let comparison_code = format!(
            "{:02X}{:02X}-{:02X}{:02X}",
            code_bytes[0], code_bytes[1], code_bytes[2], code_bytes[3]
        );
        tx.execute(
            "UPDATE device_pairing_invites SET state='pending', claim_hash=?1, device_name=?2, comparison_code=?3 WHERE id=?4",
            params![secret_hash(&credential), name, comparison_code, id],
        )?;
        tx.commit()?;
        Ok(PairingClaim {
            credential,
            comparison_code,
            expires_at,
        })
    }

    /// Approving and granting access are one durable transaction. The phone
    /// already holds its credential, so a lost approval/status response is safe
    /// to recover without keeping a recoverable credential in the database.
    pub fn approve(&mut self, id: &str, now: i64) -> Result<PairedDevice> {
        let tx = self
            .db
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let claim: Option<(String, String)> = tx.query_row(
            "SELECT claim_hash, device_name FROM device_pairing_invites WHERE id=?1 AND state='pending' AND expires_at>?2",
            params![id, now], |r| Ok((r.get(0)?, r.get(1)?)),
        ).optional()?;
        let (hash, name) = claim.ok_or(DeviceAccessError::InvalidClaim)?;
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM paired_devices WHERE revoked_at IS NULL AND expires_at>?1",
            [now],
            |r| r.get(0),
        )?;
        if count >= 64 {
            return Err(DeviceAccessError::Limit);
        }
        // Bound retained metadata as well as active grants.
        tx.execute(
            "DELETE FROM paired_devices WHERE expires_at<=?1 OR revoked_at<=?2",
            params![now, now - 30 * 86400],
        )?;
        let device = PairedDevice {
            id: id.to_owned(),
            name,
            created_at: now,
            expires_at: now + DEVICE_LIFETIME_SECONDS,
            revoked_at: None,
        };
        tx.execute("INSERT INTO paired_devices(id, name, token_hash, created_at, expires_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![device.id, device.name, hash, device.created_at, device.expires_at])?;
        tx.execute(
            "UPDATE device_pairing_invites SET state='approved' WHERE id=?1",
            [id],
        )?;
        tx.commit()?;
        Ok(device)
    }

    pub fn cancel(&self, id: &str) -> Result<()> {
        self.db.conn().execute("UPDATE device_pairing_invites SET state='denied' WHERE id=?1 AND state IN ('available', 'pending')", [id])?;
        Ok(())
    }

    pub fn status(&self, credential: &str, now: i64) -> Result<PairingStatus> {
        if let Some(device_id) = self.authenticate(credential, now)? {
            return Ok(PairingStatus::Approved { device_id });
        }
        if !valid_secret(credential, DEVICE_TOKEN_PREFIX) {
            return Ok(PairingStatus::Unavailable);
        }
        let pending: bool = self.db.conn().query_row(
            "SELECT EXISTS(SELECT 1 FROM device_pairing_invites WHERE claim_hash=?1 AND state='pending' AND expires_at>?2)",
            params![secret_hash(credential), now], |r| r.get(0))?;
        Ok(if pending {
            PairingStatus::Pending
        } else {
            PairingStatus::Unavailable
        })
    }

    pub fn authenticate(&self, credential: &str, now: i64) -> Result<Option<String>> {
        if !valid_secret(credential, DEVICE_TOKEN_PREFIX) {
            return Ok(None);
        }
        Ok(self.db.conn().query_row(
            "SELECT id FROM paired_devices WHERE token_hash=?1 AND revoked_at IS NULL AND expires_at>?2",
            params![secret_hash(credential), now], |r| r.get(0)).optional()?)
    }

    pub fn active(&self, id: &str, now: i64) -> Result<bool> {
        Ok(self.db.conn().query_row("SELECT EXISTS(SELECT 1 FROM paired_devices WHERE id=?1 AND revoked_at IS NULL AND expires_at>?2)", params![id, now], |r| r.get(0))?)
    }

    pub fn revoke(&mut self, id: &str, now: i64) -> Result<()> {
        let tx = self
            .db
            .conn_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE paired_devices SET revoked_at=COALESCE(revoked_at, ?1) WHERE id=?2",
            params![now, id],
        )?;
        tx.execute(
            "UPDATE device_pairing_invites SET state='denied' WHERE id=?1",
            [id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn pending(&self, now: i64) -> Result<Vec<PendingDevice>> {
        let mut stmt = self.db.conn().prepare("SELECT id, device_name, comparison_code, expires_at FROM device_pairing_invites WHERE state='pending' AND expires_at>?1 ORDER BY expires_at")?;
        let rows = stmt
            .query_map([now], |r| {
                Ok(PendingDevice {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    comparison_code: r.get(2)?,
                    expires_at: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    pub fn devices(&self, now: i64) -> Result<Vec<PairedDevice>> {
        let mut stmt = self.db.conn().prepare("SELECT id, name, created_at, expires_at, revoked_at FROM paired_devices WHERE expires_at>?1 AND revoked_at IS NULL ORDER BY created_at DESC LIMIT 64")?;
        let rows = stmt
            .query_map([now], |r| {
                Ok(PairedDevice {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    created_at: r.get(2)?,
                    expires_at: r.get(3)?,
                    revoked_at: r.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }
}

fn random_secret(prefix: &str) -> Result<String> {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|_| DeviceAccessError::Random)?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("{prefix}{hex}"))
}

fn valid_secret(value: &str, prefix: &str) -> bool {
    value
        .strip_prefix(prefix)
        .is_some_and(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn secret_hash(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
mod tests;
