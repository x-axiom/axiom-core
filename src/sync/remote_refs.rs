/// Remote-tracking refs management.
///
/// Re-exports storage types and provides the [`compare_refs`] function for
/// computing ahead/behind counts between a local ref and its remote-tracking
/// counterpart.
pub use crate::store::traits::{AheadBehind, RemoteRef, RemoteTrackingRepo};

use std::collections::{HashSet, VecDeque};

use crate::error::{CasError, CasResult};
use crate::model::VersionId;
use crate::store::traits::{RefRepo, VersionRepo};

/// Compare a local ref with its remote-tracking counterpart.
///
/// Returns `(ahead, behind)` counts:
/// - **ahead**: commits reachable from the local ref's target but not from the
///   remote-tracking ref's target.
/// - **behind**: commits reachable from the remote-tracking ref's target but
///   not from the local ref's target.
///
/// Returns `CasError::NotFound` if either ref does not exist.
pub fn compare_refs(
    remote_name: &str,
    ref_name: &str,
    local_refs: &dyn RefRepo,
    remote_tracking: &dyn RemoteTrackingRepo,
    versions: &dyn VersionRepo,
) -> CasResult<AheadBehind> {
    let local_ref = local_refs
        .get_ref(ref_name)?
        .ok_or_else(|| CasError::NotFound(format!("local ref '{ref_name}' not found")))?;

    let remote_ref = remote_tracking
        .get_remote_ref(remote_name, ref_name)?
        .ok_or_else(|| {
            CasError::NotFound(format!(
                "remote-tracking ref '{remote_name}/{ref_name}' not found"
            ))
        })?;

    if local_ref.target == remote_ref.target {
        return Ok(AheadBehind::default());
    }

    let local_ancestors = collect_ancestors(&local_ref.target, versions)?;
    let remote_ancestors = collect_ancestors(&remote_ref.target, versions)?;

    Ok(AheadBehind {
        ahead: local_ancestors.difference(&remote_ancestors).count(),
        behind: remote_ancestors.difference(&local_ancestors).count(),
    })
}

/// BFS from `start`, collecting all reachable version IDs (including `start`).
fn collect_ancestors(
    start: &VersionId,
    versions: &dyn VersionRepo,
) -> CasResult<HashSet<VersionId>> {
    let mut visited: HashSet<VersionId> = HashSet::new();
    let mut queue: VecDeque<VersionId> = VecDeque::new();
    queue.push_back(start.clone());

    while let Some(id) = queue.pop_front() {
        if !visited.insert(id.clone()) {
            continue;
        }
        if let Some(v) = versions.get_version(&id)? {
            for parent in v.parents {
                if !visited.contains(&parent) {
                    queue.push_back(parent);
                }
            }
        }
    }

    Ok(visited)
}
