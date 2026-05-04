//! Resume/checkpoint helpers for interrupted push/pull (E05-S03).

use std::collections::HashSet;

use crate::error::CasResult;
use crate::model::current_timestamp;
use crate::store::traits::{ObjectManifestRepo, SyncDirection, SyncSession, SyncSessionRepo, SyncSessionStatus};

// ---------------------------------------------------------------------------
// ResumeSet
// ---------------------------------------------------------------------------

/// Set of object hashes known to have been transferred in a previous
/// interrupted session. Used to skip re-uploading or re-downloading objects
/// that were already confirmed sent/received.
pub struct ResumeSet {
    inner: HashSet<String>,
}

impl ResumeSet {
    pub fn empty() -> Self {
        Self { inner: HashSet::new() }
    }

    pub fn from_manifest(entries: Vec<String>) -> Self {
        Self { inner: entries.into_iter().collect() }
    }

    pub fn contains_hex(&self, hex: &str) -> bool {
        self.inner.contains(hex)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

// ---------------------------------------------------------------------------
// find_resumable
// ---------------------------------------------------------------------------

/// Find a `Running` session for the given remote+direction that is within the
/// TTL (`max_age_secs`). Returns the most recent qualifying session, if any.
pub fn find_resumable(
    sessions: &dyn SyncSessionRepo,
    remote_name: &str,
    direction: SyncDirection,
    max_age_secs: u64,
    now_secs: u64,
) -> CasResult<Option<SyncSession>> {
    let all = sessions.list_sync_sessions(Some(remote_name), 50)?;
    let candidate = all.into_iter().find(|s| {
        s.direction == direction
            && s.status == SyncSessionStatus::Running
            && now_secs.saturating_sub(s.started_at) < max_age_secs
    });
    Ok(candidate)
}

// ---------------------------------------------------------------------------
// cleanup_stale_sessions
// ---------------------------------------------------------------------------

/// Mark stale `Running` sessions as `Failed` and, if `manifests` is provided,
/// delete their persisted manifests. Returns the count of expired sessions.
pub fn cleanup_stale_sessions(
    sessions: &dyn SyncSessionRepo,
    manifests: Option<&dyn ObjectManifestRepo>,
    max_age_secs: u64,
    now_secs: u64,
) -> CasResult<usize> {
    let all = sessions.list_sync_sessions(None, 200)?;
    let mut count = 0usize;
    for mut s in all {
        if s.status == SyncSessionStatus::Running
            && now_secs.saturating_sub(s.started_at) >= max_age_secs
        {
            s.status = SyncSessionStatus::Failed;
            s.finished_at = Some(current_timestamp());
            s.error_message = Some("expired by cleanup".to_string());
            sessions.update_session(&s)?;
            if let Some(m) = manifests {
                m.manifest_delete(&s.id)?;
            }
            count += 1;
        }
    }
    Ok(count)
}
