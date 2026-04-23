//! Fast-forward detection (E04-S05).
//!
//! A version `new` is a **fast-forward** of `old` iff `old` is reachable from
//! `new` via the version-DAG `parents` edges (i.e. `old` is an ancestor of
//! `new`, or they are equal).
//!
//! This is the foundational algorithm used by Push (`FinalizeRefs`) and Pull
//! (auto-advance) to decide whether a ref update is safe.
//!
//! ## Special cases
//! - **Empty `old`** (None / empty hex / all-zero hash) means "ref does not
//!   yet exist" → always fast-forward.
//! - **Equal IDs** → trivially fast-forward.
//! - **Missing version** in the store → traversal stops at that node and the
//!   function returns `Ok(false)` if `old` was not yet found.  No error is
//!   raised, so an isolated history fragment safely reports non-fast-forward.

use std::collections::{HashSet, VecDeque};

use crate::error::CasResult;
use crate::model::VersionId;
use crate::store::traits::VersionRepo;

/// Return `true` when advancing a ref from `old_vid` to `new_vid` is a
/// fast-forward (i.e. `old_vid` is an ancestor of, or equal to, `new_vid`).
///
/// See the module docs for the special-case handling of empty IDs.
pub fn is_fast_forward(
    old_vid: &VersionId,
    new_vid: &VersionId,
    versions: &dyn VersionRepo,
) -> CasResult<bool> {
    if is_empty_vid(old_vid) {
        return Ok(true);
    }
    if old_vid == new_vid {
        return Ok(true);
    }

    // BFS from new_vid through parents looking for old_vid.
    let mut queue: VecDeque<VersionId> = VecDeque::new();
    let mut visited: HashSet<VersionId> = HashSet::new();
    queue.push_back(new_vid.clone());

    while let Some(id) = queue.pop_front() {
        if !visited.insert(id.clone()) {
            continue;
        }
        if &id == old_vid {
            return Ok(true);
        }
        if let Some(v) = versions.get_version(&id)? {
            for parent in v.parents {
                queue.push_back(parent);
            }
        }
    }

    Ok(false)
}

/// `true` when `vid` represents "no version" — empty string or an all-zero
/// hex representation of a hash.
fn is_empty_vid(vid: &VersionId) -> bool {
    let s = vid.as_str();
    s.is_empty() || s.chars().all(|c| c == '0')
}
