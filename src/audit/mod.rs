//! Audit log (P1-8).
//!
//! Every security-relevant action produces one immutable record:
//! `{ts, actor, tenant, action, target, outcome, request_id}`.
//!
//! Records are written to the metadata store (queryable for 90 days) **and**
//! emitted as a structured `axiom.audit` log line, so an external log pipeline
//! can retain them independently of the database.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::CasResult;

/// One audit record.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unix seconds.
    pub ts: u64,
    /// Acting user id (`"-"` for unauthenticated attempts).
    pub actor: String,
    pub tenant_id: String,
    /// Dotted action name, e.g. `workspace.create`, `auth.login`.
    pub action: String,
    /// Object acted upon (workspace id, member id, path…).
    pub target: String,
    /// `ok` / `denied` / `error`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl AuditEvent {
    pub fn new(
        actor: &str,
        tenant_id: &str,
        action: &str,
        target: &str,
        outcome: &str,
    ) -> Self {
        Self {
            ts: current_ts(),
            actor: actor.to_owned(),
            tenant_id: tenant_id.to_owned(),
            action: action.to_owned(),
            target: target.to_owned(),
            outcome: outcome.to_owned(),
            request_id: crate::telemetry::current_request_id(),
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

/// Storage for audit records.
pub trait AuditRepo: Send + Sync {
    fn append(&self, event: &AuditEvent) -> CasResult<()>;
    /// Most recent records for a tenant, newest first.
    fn list(&self, tenant_id: &str, limit: usize) -> CasResult<Vec<AuditEvent>>;
    /// Delete records older than `before_ts`; returns how many were removed.
    fn purge_before(&self, before_ts: u64) -> CasResult<usize>;
}

/// Emit to the `axiom.audit` tracing target (retained by the log pipeline).
pub fn emit(event: &AuditEvent) {
    tracing::info!(
        target: "axiom.audit",
        actor = %event.actor,
        tenant_id = %event.tenant_id,
        action = %event.action,
        target_object = %event.target,
        outcome = %event.outcome,
        request_id = event.request_id.as_deref().unwrap_or("-"),
        detail = event.detail.as_deref().unwrap_or("-"),
        "audit"
    );
}

/// Write through to the store and the log pipeline.
pub fn record(repo: Option<&Arc<dyn AuditRepo>>, event: AuditEvent) {
    emit(&event);
    if let Some(repo) = repo {
        if let Err(err) = repo.append(&event) {
            tracing::error!(error = %err, action = %event.action, "failed to persist audit event");
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory repo (local mode / tests)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct MemoryAuditRepo {
    events: parking_lot::Mutex<Vec<AuditEvent>>,
}

impl MemoryAuditRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AuditRepo for MemoryAuditRepo {
    fn append(&self, event: &AuditEvent) -> CasResult<()> {
        self.events.lock().push(event.clone());
        Ok(())
    }

    fn list(&self, tenant_id: &str, limit: usize) -> CasResult<Vec<AuditEvent>> {
        let guard = self.events.lock();
        let mut out: Vec<AuditEvent> = guard
            .iter()
            .filter(|e| e.tenant_id == tenant_id)
            .cloned()
            .collect();
        out.sort_by(|a, b| b.ts.cmp(&a.ts));
        out.truncate(limit);
        Ok(out)
    }

    fn purge_before(&self, before_ts: u64) -> CasResult<usize> {
        let mut guard = self.events.lock();
        let before = guard.len();
        guard.retain(|e| e.ts >= before_ts);
        Ok(before - guard.len())
    }
}

pub fn current_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Retention window (90 days).
pub const RETENTION_SECS: u64 = 90 * 24 * 3600;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_repo_filters_and_purges() {
        let repo = MemoryAuditRepo::new();
        repo.append(&AuditEvent::new("u1", "t1", "workspace.create", "ws1", "ok"))
            .unwrap();
        repo.append(&AuditEvent::new("u2", "t2", "workspace.create", "ws2", "ok"))
            .unwrap();
        repo.append(&AuditEvent::new("u1", "t1", "workspace.delete", "ws1", "ok"))
            .unwrap();

        let listed = repo.list("t1", 10).unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().all(|e| e.tenant_id == "t1"));

        assert_eq!(repo.purge_before(u64::MAX).unwrap(), 3);
        assert!(repo.list("t1", 10).unwrap().is_empty());
    }
}
