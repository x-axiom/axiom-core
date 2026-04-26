//! Soft-delete and recycle bin for workspaces (E11-S04).
//!
//! Implements a two-phase workspace deletion:
//!
//! 1. **Soft-delete** — marks the workspace `deleted_at` and hides it from
//!    normal API access. Data is retained for a configurable retention window.
//! 2. **Purge** — after the retention window expires, [`RecycleBin::purge_expired`]
//!    permanently removes the workspace (via `delete_workspace`).
//!
//! # Retention policies
//!
//! ```text
//! Free plan  →  7 days
//! Pro plan   →  30 days
//! Custom(n)  →  n days
//! ```
//!
//! # Usage
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use axiom_core::gc::recycle_bin::{RecycleBin, RetentionPolicy};
//! # use axiom_core::store::traits::WorkspaceRepo;
//! # fn example(repo: Arc<dyn WorkspaceRepo>) {
//! let bin = RecycleBin::new(repo);
//! bin.soft_delete("ws-001", RetentionPolicy::Pro).unwrap();
//! let deleted = bin.list().unwrap();
//! bin.restore("ws-001").unwrap();
//! # }
//! ```

use std::sync::Arc;

use crate::error::{CasError, CasResult};
use crate::model::hash::current_timestamp;
use crate::store::traits::{Workspace, WorkspaceRepo};

// ---------------------------------------------------------------------------
// RetentionPolicy
// ---------------------------------------------------------------------------

/// Retention period before a soft-deleted workspace is permanently purged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// 7-day retention (Free tier).
    Free,
    /// 30-day retention (Pro tier).
    Pro,
    /// Custom retention period in days.
    Custom(u64),
}

impl RetentionPolicy {
    /// Returns the retention duration in seconds.
    pub fn retention_secs(&self) -> u64 {
        match self {
            RetentionPolicy::Free => 7 * 86_400,
            RetentionPolicy::Pro => 30 * 86_400,
            RetentionPolicy::Custom(days) => days * 86_400,
        }
    }

    /// Returns the retention duration in days.
    pub fn retention_days(&self) -> u64 {
        match self {
            RetentionPolicy::Free => 7,
            RetentionPolicy::Pro => 30,
            RetentionPolicy::Custom(days) => *days,
        }
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        RetentionPolicy::Pro
    }
}

// ---------------------------------------------------------------------------
// RecycleBin
// ---------------------------------------------------------------------------

/// Metadata key stored in the workspace's JSON `metadata` field to record
/// the retention policy in days.
const RETENTION_DAYS_KEY: &str = "recycle_retention_days";

/// Service layer for soft-delete / recycle bin operations.
pub struct RecycleBin {
    repo: Arc<dyn WorkspaceRepo>,
}

impl RecycleBin {
    /// Create a new `RecycleBin` backed by `repo`.
    pub fn new(repo: Arc<dyn WorkspaceRepo>) -> Self {
        Self { repo }
    }

    /// Soft-delete a workspace.
    ///
    /// Sets `deleted_at` to the current timestamp and stores the retention
    /// duration in the workspace's JSON metadata. The workspace is hidden from
    /// [`WorkspaceRepo::list_workspaces`] but still retrievable via
    /// [`WorkspaceRepo::get_workspace`] and [`RecycleBin::list`].
    ///
    /// Returns `CasError::WorkspaceNotFound` if the workspace does not exist
    /// or is already soft-deleted.
    pub fn soft_delete(&self, id: &str, policy: RetentionPolicy) -> CasResult<()> {
        let mut ws = self
            .repo
            .get_workspace(id)?
            .ok_or_else(|| CasError::WorkspaceNotFound(id.to_owned()))?;

        if ws.deleted_at.is_some() {
            return Err(CasError::InvalidObject(format!(
                "workspace '{id}' is already soft-deleted"
            )));
        }

        let now = current_timestamp();
        self.repo.soft_delete_workspace(id, now)?;

        // Persist retention info in metadata JSON so purge_expired can respect it.
        let retention_days = policy.retention_days();
        let mut meta: serde_json::Value =
            serde_json::from_str(&ws.metadata).unwrap_or(serde_json::Value::Object(Default::default()));
        meta[RETENTION_DAYS_KEY] = serde_json::Value::Number(retention_days.into());
        ws.metadata = serde_json::to_string(&meta)?;
        ws.deleted_at = Some(now);
        self.repo.update_workspace(&ws)?;

        Ok(())
    }

    /// Restore a soft-deleted workspace to active status.
    ///
    /// Returns `CasError::WorkspaceNotFound` if the workspace does not exist.
    /// Returns `CasError::InvalidObject` if the workspace is not soft-deleted.
    pub fn restore(&self, id: &str) -> CasResult<()> {
        let ws = self
            .repo
            .get_workspace(id)?
            .ok_or_else(|| CasError::WorkspaceNotFound(id.to_owned()))?;

        if ws.deleted_at.is_none() {
            return Err(CasError::InvalidObject(format!(
                "workspace '{id}' is not soft-deleted"
            )));
        }

        self.repo.restore_workspace(id)
    }

    /// List all soft-deleted workspaces.
    pub fn list(&self) -> CasResult<Vec<Workspace>> {
        self.repo.list_deleted_workspaces()
    }

    /// Permanently delete workspaces whose retention window has expired.
    ///
    /// `now` is the current Unix timestamp (seconds); pass
    /// [`current_timestamp()`] in production code. Accepts an explicit value
    /// so tests can control time.
    ///
    /// Returns the IDs of workspaces that were permanently deleted.
    pub fn purge_expired(&self, now: u64) -> CasResult<Vec<String>> {
        let deleted = self.repo.list_deleted_workspaces()?;
        let mut purged = Vec::new();

        for ws in deleted {
            let deleted_at = match ws.deleted_at {
                Some(ts) => ts,
                None => continue, // should not happen
            };

            let retention_secs = extract_retention_secs(&ws.metadata);
            if now.saturating_sub(deleted_at) >= retention_secs {
                self.repo.delete_workspace(&ws.id)?;
                purged.push(ws.id);
            }
        }

        Ok(purged)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the `recycle_retention_days` value from a metadata JSON string and
/// convert it to seconds. Falls back to the Pro-tier default (30 days) if the
/// key is missing or the JSON is malformed.
fn extract_retention_secs(metadata: &str) -> u64 {
    let days: u64 = serde_json::from_str::<serde_json::Value>(metadata)
        .ok()
        .and_then(|v| v[RETENTION_DAYS_KEY].as_u64())
        .unwrap_or(RetentionPolicy::default().retention_days());
    days * 86_400
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // ---------------------------------------------------------------------------
    // In-memory WorkspaceRepo for testing (no FDB / SQLite dependency)
    // ---------------------------------------------------------------------------

    #[derive(Default)]
    struct MemWorkspaceRepo {
        data: Mutex<HashMap<String, Workspace>>,
    }

    impl WorkspaceRepo for MemWorkspaceRepo {
        fn create_workspace(&self, ws: &Workspace) -> CasResult<()> {
            let mut d = self.data.lock().unwrap();
            if d.contains_key(&ws.id) {
                return Err(CasError::AlreadyExists);
            }
            d.insert(ws.id.clone(), ws.clone());
            Ok(())
        }

        fn get_workspace(&self, id: &str) -> CasResult<Option<Workspace>> {
            Ok(self.data.lock().unwrap().get(id).cloned())
        }

        fn list_workspaces(&self) -> CasResult<Vec<Workspace>> {
            let d = self.data.lock().unwrap();
            let mut out: Vec<_> = d.values().filter(|w| w.deleted_at.is_none()).cloned().collect();
            out.sort_by_key(|w| w.created_at);
            Ok(out)
        }

        fn delete_workspace(&self, id: &str) -> CasResult<()> {
            self.data.lock().unwrap().remove(id);
            Ok(())
        }

        fn update_workspace(&self, ws: &Workspace) -> CasResult<()> {
            let mut d = self.data.lock().unwrap();
            if let Some(entry) = d.get_mut(&ws.id) {
                *entry = ws.clone();
            }
            Ok(())
        }

        fn soft_delete_workspace(&self, id: &str, deleted_at: u64) -> CasResult<()> {
            let mut d = self.data.lock().unwrap();
            if let Some(ws) = d.get_mut(id) {
                if ws.deleted_at.is_none() {
                    ws.deleted_at = Some(deleted_at);
                }
            }
            Ok(())
        }

        fn restore_workspace(&self, id: &str) -> CasResult<()> {
            let mut d = self.data.lock().unwrap();
            if let Some(ws) = d.get_mut(id) {
                ws.deleted_at = None;
            }
            Ok(())
        }

        fn list_deleted_workspaces(&self) -> CasResult<Vec<Workspace>> {
            let d = self.data.lock().unwrap();
            let mut out: Vec<_> = d.values().filter(|w| w.deleted_at.is_some()).cloned().collect();
            out.sort_by_key(|w| w.deleted_at);
            Ok(out)
        }
    }

    fn make_ws(id: &str) -> Workspace {
        Workspace {
            id: id.to_owned(),
            name: id.to_owned(),
            created_at: 1_000_000,
            metadata: "{}".to_owned(),
            local_path: None,
            current_ref: None,
            current_version: None,
            deleted_at: None,
        }
    }

    fn make_bin() -> (RecycleBin, Arc<MemWorkspaceRepo>) {
        let repo = Arc::new(MemWorkspaceRepo::default());
        repo.create_workspace(&make_ws("ws-1")).unwrap();
        repo.create_workspace(&make_ws("ws-2")).unwrap();
        let bin = RecycleBin::new(repo.clone());
        (bin, repo)
    }

    // --- RetentionPolicy ---

    #[test]
    fn free_policy_is_7_days() {
        assert_eq!(RetentionPolicy::Free.retention_days(), 7);
        assert_eq!(RetentionPolicy::Free.retention_secs(), 7 * 86_400);
    }

    #[test]
    fn pro_policy_is_30_days() {
        assert_eq!(RetentionPolicy::Pro.retention_days(), 30);
        assert_eq!(RetentionPolicy::Pro.retention_secs(), 30 * 86_400);
    }

    #[test]
    fn custom_policy_respected() {
        assert_eq!(RetentionPolicy::Custom(14).retention_days(), 14);
        assert_eq!(RetentionPolicy::Custom(14).retention_secs(), 14 * 86_400);
    }

    // --- soft_delete ---

    #[test]
    fn soft_delete_hides_from_list_workspaces() {
        let (bin, repo) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Free).unwrap();

        let active = repo.list_workspaces().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, "ws-2");
    }

    #[test]
    fn soft_delete_sets_deleted_at() {
        let (bin, repo) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Pro).unwrap();

        let ws = repo.get_workspace("ws-1").unwrap().unwrap();
        assert!(ws.deleted_at.is_some());
    }

    #[test]
    fn soft_delete_stores_retention_in_metadata() {
        let (bin, repo) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Custom(14)).unwrap();

        let ws = repo.get_workspace("ws-1").unwrap().unwrap();
        let meta: serde_json::Value = serde_json::from_str(&ws.metadata).unwrap();
        assert_eq!(meta[RETENTION_DAYS_KEY].as_u64(), Some(14));
    }

    #[test]
    fn soft_delete_nonexistent_returns_error() {
        let (bin, _) = make_bin();
        let err = bin.soft_delete("no-such", RetentionPolicy::Free).unwrap_err();
        assert!(matches!(err, CasError::WorkspaceNotFound(_)));
    }

    #[test]
    fn soft_delete_already_deleted_returns_error() {
        let (bin, _) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Free).unwrap();
        let err = bin.soft_delete("ws-1", RetentionPolicy::Free).unwrap_err();
        assert!(matches!(err, CasError::InvalidObject(_)));
    }

    // --- list ---

    #[test]
    fn list_returns_only_deleted() {
        let (bin, _) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Free).unwrap();

        let deleted = bin.list().unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].id, "ws-1");
    }

    #[test]
    fn list_empty_when_none_deleted() {
        let (bin, _) = make_bin();
        assert!(bin.list().unwrap().is_empty());
    }

    // --- restore ---

    #[test]
    fn restore_returns_workspace_to_active() {
        let (bin, repo) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Free).unwrap();
        bin.restore("ws-1").unwrap();

        let active = repo.list_workspaces().unwrap();
        assert_eq!(active.len(), 2);
        assert!(bin.list().unwrap().is_empty());
    }

    #[test]
    fn restore_nonexistent_returns_error() {
        let (bin, _) = make_bin();
        let err = bin.restore("no-such").unwrap_err();
        assert!(matches!(err, CasError::WorkspaceNotFound(_)));
    }

    #[test]
    fn restore_active_workspace_returns_error() {
        let (bin, _) = make_bin();
        let err = bin.restore("ws-1").unwrap_err();
        assert!(matches!(err, CasError::InvalidObject(_)));
    }

    // --- purge_expired ---

    #[test]
    fn purge_expired_removes_past_retention() {
        let (bin, repo) = make_bin();
        // Soft-delete with a 7-day retention
        bin.soft_delete("ws-1", RetentionPolicy::Free).unwrap();

        let ws = repo.get_workspace("ws-1").unwrap().unwrap();
        let deleted_at = ws.deleted_at.unwrap();

        // Advance time past 7 days
        let now = deleted_at + 7 * 86_400 + 1;
        let purged = bin.purge_expired(now).unwrap();

        assert_eq!(purged, vec!["ws-1".to_owned()]);
        assert!(repo.get_workspace("ws-1").unwrap().is_none());
    }

    #[test]
    fn purge_expired_keeps_within_retention() {
        let (bin, repo) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Free).unwrap();

        let ws = repo.get_workspace("ws-1").unwrap().unwrap();
        let deleted_at = ws.deleted_at.unwrap();

        // Still within retention window (6 days later)
        let now = deleted_at + 6 * 86_400;
        let purged = bin.purge_expired(now).unwrap();

        assert!(purged.is_empty());
        assert!(repo.get_workspace("ws-1").unwrap().is_some());
    }

    #[test]
    fn purge_expired_respects_custom_retention() {
        let (bin, repo) = make_bin();
        bin.soft_delete("ws-1", RetentionPolicy::Custom(1)).unwrap();

        let ws = repo.get_workspace("ws-1").unwrap().unwrap();
        let deleted_at = ws.deleted_at.unwrap();

        // 1 day + 1 second later → should be purged
        let now = deleted_at + 86_400 + 1;
        let purged = bin.purge_expired(now).unwrap();
        assert_eq!(purged.len(), 1);
    }

    #[test]
    fn purge_empty_recycle_bin_is_noop() {
        let (bin, _) = make_bin();
        let purged = bin.purge_expired(u64::MAX).unwrap();
        assert!(purged.is_empty());
    }
}
