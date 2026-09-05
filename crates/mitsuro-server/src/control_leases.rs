//! Ephemeral, server-authoritative control leases for interactive Code Agents.
//!
//! Leases coordinate same-account native clients. They deliberately remain
//! process-local: durable Agent sessions survive a server restart, while
//! mutation authority does not. Public snapshots expose only controller
//! identity and expiry; the opaque token is returned only by an explicit take.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use uuid::Uuid;

use axum::http::HeaderMap;
use mitsuro_core::storage::SessionType;

use crate::error::AppError;
use crate::types::{SessionControlLease, SessionControlLeaseSummary, SessionControlStateResponse};

pub(crate) const CONTROL_LEASE_HEADER: &str = "x-mitsuro-control-lease";
pub(crate) const CONTROL_LEASE_TTL: Duration = Duration::from_secs(15);
const MAX_CLIENT_ID_BYTES: usize = 128;
const MAX_LEASES: usize = 4_096;

#[derive(Debug, Clone)]
struct LeaseState {
    client_id: String,
    token: String,
    last_seen: Instant,
}

#[derive(Debug)]
pub(crate) struct SessionControlLeaseRegistry {
    leases: HashMap<String, LeaseState>,
    ttl: Duration,
}

impl Default for SessionControlLeaseRegistry {
    fn default() -> Self {
        Self::new(CONTROL_LEASE_TTL)
    }
}

impl SessionControlLeaseRegistry {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            leases: HashMap::new(),
            ttl,
        }
    }

    pub(crate) fn snapshot(&mut self, session_id: &str) -> SessionControlStateResponse {
        self.prune_expired();
        SessionControlStateResponse {
            session_id: session_id.to_owned(),
            controller: self.leases.get(session_id).map(|lease| self.summary(lease)),
            ttl_seconds: self.ttl.as_secs(),
        }
    }

    /// Claim an unowned lease, refresh this client's authority, or explicitly
    /// transfer it when `force` is true. A passive observer can never steal a
    /// live lease through the automatic attach path.
    pub(crate) fn take(
        &mut self,
        session_id: &str,
        client_id: String,
        force: bool,
    ) -> Result<SessionControlLease, AppError> {
        validate_client_id(&client_id)?;
        self.prune_expired();
        if let Some(current) = self.leases.get(session_id) {
            if current.client_id != client_id && !force {
                return Err(AppError::Conflict(format!(
                    "Code Agent is controlled by another client ({})",
                    current.client_id
                )));
            }
        }
        if self.leases.len() >= MAX_LEASES && !self.leases.contains_key(session_id) {
            return Err(AppError::ServiceUnavailable(
                "Code Agent control lease registry is at capacity".to_owned(),
            ));
        }

        let lease = LeaseState {
            client_id,
            token: Uuid::new_v4().to_string(),
            last_seen: Instant::now(),
        };
        let response = self.private_lease(session_id, &lease);
        self.leases.insert(session_id.to_owned(), lease);
        Ok(response)
    }

    pub(crate) fn heartbeat(
        &mut self,
        session_id: &str,
        client_id: &str,
        token: &str,
    ) -> Result<SessionControlStateResponse, AppError> {
        validate_client_id(client_id)?;
        self.prune_expired();
        let ttl = self.ttl;
        let lease = self.leases.get_mut(session_id).ok_or_else(|| {
            AppError::Conflict(
                "Code Agent has no active control lease; take control before mutating it"
                    .to_owned(),
            )
        })?;
        if lease.client_id != client_id || !constant_time_token_eq(&lease.token, token) {
            return Err(AppError::Conflict(
                "Code Agent control lease is held by another client".to_owned(),
            ));
        }
        lease.last_seen = Instant::now();
        let controller = Some(summary_with_ttl(lease, ttl));
        Ok(SessionControlStateResponse {
            session_id: session_id.to_owned(),
            controller,
            ttl_seconds: ttl.as_secs(),
        })
    }

    /// Authorize a Code mutation only under an active matching lease. A
    /// matching token also renews the stale window.
    pub(crate) fn authorize_mutation(
        &mut self,
        session_id: &str,
        token: Option<&str>,
    ) -> Result<(), AppError> {
        self.prune_expired();
        let Some(lease) = self.leases.get_mut(session_id) else {
            return Err(AppError::Conflict(
                "Code Agent control lease is required; take control before mutating it".to_owned(),
            ));
        };
        let token = token.filter(|token| !token.is_empty()).ok_or_else(|| {
            AppError::Conflict(format!(
                "Code Agent is controlled by another client ({})",
                lease.client_id
            ))
        })?;
        if !constant_time_token_eq(&lease.token, token) {
            return Err(AppError::Conflict(format!(
                "Code Agent is controlled by another client ({})",
                lease.client_id
            )));
        }
        lease.last_seen = Instant::now();
        Ok(())
    }

    fn prune_expired(&mut self) {
        let ttl = self.ttl;
        self.leases
            .retain(|_, lease| lease.last_seen.elapsed() < ttl);
    }

    fn summary(&self, lease: &LeaseState) -> SessionControlLeaseSummary {
        summary_with_ttl(lease, self.ttl)
    }

    fn private_lease(&self, session_id: &str, lease: &LeaseState) -> SessionControlLease {
        SessionControlLease {
            resource_id: session_id.to_owned(),
            client_id: lease.client_id.clone(),
            lease_token: lease.token.clone(),
            expires_at: expiry_timestamp(self.ttl),
        }
    }
}

/// Enforce the active native controller only for interactive Code sessions.
/// Chat remains conventional and Hive remains exclusively daemon-controlled.
pub(crate) async fn authorize_code_agent_mutation(
    state: &crate::AppState,
    session_id: &str,
    session_type: SessionType,
    headers: &HeaderMap,
) -> Result<(), AppError> {
    if session_type != SessionType::Code {
        return Ok(());
    }
    let token = control_lease_token(headers)?;
    state
        .session_control_leases
        .write()
        .await
        .authorize_mutation(session_id, token)
}

fn control_lease_token(headers: &HeaderMap) -> Result<Option<&str>, AppError> {
    let Some(value) = headers.get(CONTROL_LEASE_HEADER) else {
        return Ok(None);
    };
    let token = value.to_str().map_err(|_| {
        AppError::BadRequest("Code Agent control lease header is invalid".to_owned())
    })?;
    if token.is_empty() {
        return Err(AppError::BadRequest(
            "Code Agent control lease header cannot be empty".to_owned(),
        ));
    }
    Ok(Some(token))
}

fn validate_client_id(client_id: &str) -> Result<(), AppError> {
    let trimmed = client_id.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_CLIENT_ID_BYTES || trimmed != client_id {
        return Err(AppError::BadRequest(format!(
            "Code Agent controller client_id must contain 1 to {MAX_CLIENT_ID_BYTES} bytes without surrounding whitespace"
        )));
    }
    Ok(())
}

fn summary_with_ttl(lease: &LeaseState, ttl: Duration) -> SessionControlLeaseSummary {
    let remaining = ttl.saturating_sub(lease.last_seen.elapsed());
    SessionControlLeaseSummary {
        client_id: lease.client_id.clone(),
        expires_at: expiry_timestamp(remaining),
    }
}

fn expiry_timestamp(remaining: Duration) -> String {
    let chrono_remaining = chrono::Duration::from_std(remaining).unwrap_or_default();
    (Utc::now() + chrono_remaining).to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn constant_time_token_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut difference = left.len() ^ right.len();
    let compared_len = left.len().max(right.len());
    for index in 0..compared_len {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observer_is_rejected_and_explicit_transfer_invalidates_old_token() {
        let mut registry = SessionControlLeaseRegistry::default();
        let first = registry
            .take("code-1", "canvas-a".to_owned(), false)
            .expect("first controller");
        assert!(registry.authorize_mutation("code-1", None).is_err());
        assert!(registry
            .take("code-1", "canvas-b".to_owned(), false)
            .is_err());

        let second = registry
            .take("code-1", "canvas-b".to_owned(), true)
            .expect("explicit transfer");
        assert!(registry
            .authorize_mutation("code-1", Some(&first.lease_token))
            .is_err());
        registry
            .authorize_mutation("code-1", Some(&second.lease_token))
            .expect("new controller token");
    }

    #[tokio::test]
    async fn mutation_requires_a_lease_before_acquisition_and_after_expiry() {
        let mut registry = SessionControlLeaseRegistry::new(Duration::from_millis(2));
        assert!(registry.authorize_mutation("code-1", None).is_err());
        registry
            .take("code-1", "canvas-a".to_owned(), false)
            .expect("lease");
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(registry.authorize_mutation("code-1", None).is_err());
        assert!(registry.snapshot("code-1").controller.is_none());
    }

    #[test]
    fn another_client_can_take_over_at_the_fifteen_second_stale_boundary() {
        let mut registry = SessionControlLeaseRegistry::default();
        registry
            .take("code-1", "canvas-a".to_owned(), false)
            .expect("initial lease");
        registry
            .leases
            .get_mut("code-1")
            .expect("stored lease")
            .last_seen = Instant::now()
            .checked_sub(CONTROL_LEASE_TTL)
            .expect("representable fake time");

        let replacement = registry
            .take("code-1", "canvas-b".to_owned(), false)
            .expect("lease is stale at the exact policy boundary");
        assert_eq!(replacement.client_id, "canvas-b");
        assert_eq!(registry.snapshot("code-1").ttl_seconds, 15);
    }

    #[test]
    fn public_summary_never_contains_the_private_token() {
        let mut registry = SessionControlLeaseRegistry::default();
        let private = registry
            .take("code-1", "canvas-a".to_owned(), false)
            .expect("lease");
        let public = registry.snapshot("code-1");
        let json = serde_json::to_value(public).expect("serialize summary");
        assert!(json.get("lease_token").is_none());
        assert!(json["controller"].get("lease_token").is_none());
        assert!(!private.lease_token.is_empty());
    }
}
