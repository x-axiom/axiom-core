//! Storage quota and usage accounting (P1-1).
//!
//! Beta semantics:
//! - Usage is tracked **per tenant** (the billing unit), not per workspace.
//! - A plan maps to a byte ceiling; `AXIOM_QUOTA_*_BYTES` can override each
//!   tier without a redeploy of code.
//! - Uploads are charged for **newly stored** bytes only: deduplicated chunks
//!   are free, because they cost nothing extra to keep.
//!
//! The ledger is a simple counter with `add`/`get`/`set`. A reconcile command
//! (see `reconcile` below) can rebuild it from the workspace metadata when
//! counters drift.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::CasResult;

/// Default ceilings (bytes) per plan tier.
const DEFAULT_FREE: u64 = 5 * 1024 * 1024 * 1024; // 5 GiB
const DEFAULT_PRO: u64 = 100 * 1024 * 1024 * 1024; // 100 GiB
const DEFAULT_ENTERPRISE: u64 = 1024 * 1024 * 1024 * 1024; // 1 TiB

/// Plan → quota mapping.
#[derive(Clone, Debug)]
pub struct QuotaPolicy {
    free_bytes: u64,
    pro_bytes: u64,
    enterprise_bytes: u64,
}

impl Default for QuotaPolicy {
    fn default() -> Self {
        Self {
            free_bytes: DEFAULT_FREE,
            pro_bytes: DEFAULT_PRO,
            enterprise_bytes: DEFAULT_ENTERPRISE,
        }
    }
}

impl QuotaPolicy {
    /// Explicit limits (tests / per-environment configuration).
    pub fn new(free_bytes: u64, pro_bytes: u64, enterprise_bytes: u64) -> Self {
        Self { free_bytes, pro_bytes, enterprise_bytes }
    }

    /// Read overrides from `AXIOM_QUOTA_FREE_BYTES` /
    /// `AXIOM_QUOTA_PRO_BYTES` / `AXIOM_QUOTA_ENTERPRISE_BYTES`.
    pub fn from_env() -> Self {
        let mut policy = Self::default();
        if let Some(v) = env_bytes("AXIOM_QUOTA_FREE_BYTES") {
            policy.free_bytes = v;
        }
        if let Some(v) = env_bytes("AXIOM_QUOTA_PRO_BYTES") {
            policy.pro_bytes = v;
        }
        if let Some(v) = env_bytes("AXIOM_QUOTA_ENTERPRISE_BYTES") {
            policy.enterprise_bytes = v;
        }
        policy
    }

    /// Byte ceiling for a plan claim. Unknown/absent plans are treated as Free
    /// (fail closed: the cheapest tier, never unlimited).
    pub fn limit_for(&self, plan: Option<&str>) -> u64 {
        match plan.map(|p| p.trim().to_ascii_lowercase()).as_deref() {
            Some("pro") => self.pro_bytes,
            Some("enterprise") => self.enterprise_bytes,
            _ => self.free_bytes,
        }
    }

    /// Canonical plan name for responses/metrics.
    pub fn plan_name(plan: Option<&str>) -> &'static str {
        match plan.map(|p| p.trim().to_ascii_lowercase()).as_deref() {
            Some("pro") => "pro",
            Some("enterprise") => "enterprise",
            _ => "free",
        }
    }
}

fn env_bytes(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
}

// ---------------------------------------------------------------------------
// Usage ledger
// ---------------------------------------------------------------------------

/// Tenant-level byte counter.
pub trait UsageRepo: Send + Sync {
    /// Current recorded usage.
    fn get(&self, tenant_id: &str) -> CasResult<u64>;
    /// Add `delta` bytes and return the new total.
    fn add(&self, tenant_id: &str, delta: u64) -> CasResult<u64>;
    /// Overwrite the recorded usage (used by reconciliation).
    fn set(&self, tenant_id: &str, bytes: u64) -> CasResult<()>;
}

/// Process-local ledger (local mode / tests).
#[derive(Default)]
pub struct MemoryUsageRepo {
    inner: RwLock<HashMap<String, u64>>,
}

impl MemoryUsageRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl UsageRepo for MemoryUsageRepo {
    fn get(&self, tenant_id: &str) -> CasResult<u64> {
        Ok(*self.inner.read().get(tenant_id).unwrap_or(&0))
    }

    fn add(&self, tenant_id: &str, delta: u64) -> CasResult<u64> {
        let mut guard = self.inner.write();
        let entry = guard.entry(tenant_id.to_owned()).or_insert(0);
        *entry = entry.saturating_add(delta);
        Ok(*entry)
    }

    fn set(&self, tenant_id: &str, bytes: u64) -> CasResult<()> {
        self.inner.write().insert(tenant_id.to_owned(), bytes);
        Ok(())
    }
}

/// Snapshot returned to clients.
#[derive(Clone, Debug, serde::Serialize)]
pub struct UsageSnapshot {
    pub tenant_id: String,
    pub plan: String,
    pub used_bytes: u64,
    pub quota_bytes: u64,
    pub remaining_bytes: u64,
}

impl UsageSnapshot {
    pub fn new(tenant_id: &str, plan: Option<&str>, used: u64, quota: u64) -> Self {
        Self {
            tenant_id: tenant_id.to_owned(),
            plan: QuotaPolicy::plan_name(plan).to_owned(),
            used_bytes: used,
            quota_bytes: quota,
            remaining_bytes: quota.saturating_sub(used),
        }
    }
}

/// Load a snapshot for a tenant.
pub fn snapshot(
    usage: &dyn UsageRepo,
    policy: &QuotaPolicy,
    tenant_id: &str,
    plan: Option<&str>,
) -> CasResult<UsageSnapshot> {
    let used = usage.get(tenant_id)?;
    Ok(UsageSnapshot::new(
        tenant_id,
        plan,
        used,
        policy.limit_for(plan),
    ))
}

/// Compute the remaining byte budget for an upload, if the tenant is subject
/// to a quota. Returns `None` when usage is unavailable (quota disabled).
pub fn remaining_budget(
    usage: Option<&Arc<dyn UsageRepo>>,
    policy: &QuotaPolicy,
    tenant_id: Option<&str>,
    plan: Option<&str>,
) -> CasResult<Option<u64>> {
    let (Some(usage), Some(tenant)) = (usage, tenant_id) else {
        return Ok(None);
    };
    let used = usage.get(tenant)?;
    Ok(Some(policy.limit_for(plan).saturating_sub(used)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_limits_are_tiered_and_fail_closed() {
        let policy = QuotaPolicy::default();
        assert_eq!(policy.limit_for(None), policy.free_bytes);
        assert_eq!(policy.limit_for(Some("unknown")), policy.free_bytes);
        assert_eq!(policy.limit_for(Some("Pro")), policy.pro_bytes);
        assert_eq!(policy.limit_for(Some("enterprise")), policy.enterprise_bytes);
        assert!(policy.pro_bytes > policy.free_bytes);
    }

    #[test]
    fn usage_accumulates_and_snapshots() {
        let repo = MemoryUsageRepo::new();
        assert_eq!(repo.get("t1").unwrap(), 0);
        assert_eq!(repo.add("t1", 100).unwrap(), 100);
        assert_eq!(repo.add("t1", 50).unwrap(), 150);
        repo.set("t1", 10).unwrap();
        assert_eq!(repo.get("t1").unwrap(), 10);

        let policy = QuotaPolicy::default();
        let snap = snapshot(&repo, &policy, "t1", Some("free")).unwrap();
        assert_eq!(snap.used_bytes, 10);
        assert_eq!(snap.remaining_bytes, policy.free_bytes - 10);
    }
}
