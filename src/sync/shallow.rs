//! Shallow clone support (E05-S02).
//!
//! Provides [`ShallowBoundary`] and [`collect_versions_shallow`], which
//! implement depth-limited BFS over the version DAG for the shallow clone
//! protocol.

use std::collections::{HashSet, VecDeque};

use crate::error::CasResult;
use crate::model::VersionId;
use crate::store::traits::VersionRepo;

// ─── ShallowBoundary ──────────────────────────────────────────────────────────

/// The set of version IDs that lie immediately below the shallow clone
/// boundary: these version IDs were *referenced* by a fetched version's
/// `parents` field but were **not** themselves fetched.
///
/// The boundary acts like a graft in Git: local operations that walk history
/// (e.g. push, log) must stop at these version IDs rather than following
/// their parents further.
#[derive(Clone, Debug, Default)]
pub struct ShallowBoundary {
    boundary: HashSet<VersionId>,
}

impl ShallowBoundary {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a single version ID to the boundary.
    pub fn add(&mut self, vid: VersionId) {
        self.boundary.insert(vid);
    }

    /// Returns `true` if `vid` is a known boundary version.
    pub fn contains(&self, vid: &VersionId) -> bool {
        self.boundary.contains(vid)
    }

    /// Returns `true` when the boundary is empty (i.e. this is a full clone).
    pub fn is_empty(&self) -> bool {
        self.boundary.is_empty()
    }

    /// Number of boundary version IDs recorded.
    pub fn len(&self) -> usize {
        self.boundary.len()
    }

    /// Iterate over all boundary version IDs.
    pub fn iter(&self) -> impl Iterator<Item = &VersionId> {
        self.boundary.iter()
    }

    /// Consume the boundary and return the underlying `HashSet`.
    pub fn into_set(self) -> HashSet<VersionId> {
        self.boundary
    }
}

// ─── Depth-limited version BFS ────────────────────────────────────────────────

/// Collect version IDs reachable from `tips` within BFS `depth`.
///
/// Returns:
/// * `versions`  — every version ID at BFS distance `< depth` from any tip
///   (tips themselves are at distance 0 and are always included if `depth ≥ 1`).
/// * `boundary`  — every **parent** version ID at distance `== depth` that
///   was *not* itself included in `versions`.
///
/// `depth = 0` means **unlimited** — all reachable versions are returned and
/// the boundary is empty, matching a full-clone profile.
///
/// Versions reached via multiple DAG paths are deduplicated; they appear
/// only in `versions` (not also in `boundary`).
pub fn collect_versions_shallow(
    tips: &[VersionId],
    depth: u32,
    version_repo: &dyn VersionRepo,
) -> CasResult<(Vec<VersionId>, ShallowBoundary)> {
    let mut visited: HashSet<VersionId> = HashSet::new();
    let mut versions: Vec<VersionId> = Vec::new();
    let mut boundary = ShallowBoundary::new();

    // BFS queue carries (version_id, bfs_level).
    let mut queue: VecDeque<(VersionId, u32)> =
        tips.iter().map(|v| (v.clone(), 0)).collect();

    while let Some((vid, level)) = queue.pop_front() {
        if !visited.insert(vid.clone()) {
            continue;
        }

        // depth == 0 → unlimited; otherwise cut at the boundary level.
        if depth > 0 && level >= depth {
            boundary.add(vid);
            continue;
        }

        versions.push(vid.clone());

        if let Some(v) = version_repo.get_version(&vid)? {
            for parent in v.parents {
                if !visited.contains(&parent) {
                    queue.push_back((parent, level + 1));
                }
            }
        }
    }

    Ok((versions, boundary))
}
