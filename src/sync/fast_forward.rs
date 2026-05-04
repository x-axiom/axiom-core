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
//! - **Deep history** → `is_fast_forward_with_limit` returns
//!   `CasError::SyncError("history too deep")` when the BFS visits more than
//!   `max_nodes` version entries.  The public `is_fast_forward` wrapper uses a
//!   default limit of 100 000.

use std::collections::{HashSet, VecDeque};

use crate::error::{CasError, CasResult};
use crate::model::VersionId;
use crate::store::traits::VersionRepo;

/// Default BFS node limit for [`is_fast_forward`].
const DEFAULT_MAX_NODES: usize = 100_000;

/// Return `true` when advancing a ref from `old_vid` to `new_vid` is a
/// fast-forward (i.e. `old_vid` is an ancestor of, or equal to, `new_vid`).
///
/// Uses [`is_fast_forward_with_limit`] with a default cap of
/// [`DEFAULT_MAX_NODES`] (100 000 versions).  Returns
/// `Err(CasError::SyncError("history too deep"))` if the cap is exceeded.
///
/// See the module docs for the special-case handling of empty IDs.
pub fn is_fast_forward(
    old_vid: &VersionId,
    new_vid: &VersionId,
    versions: &dyn VersionRepo,
) -> CasResult<bool> {
    is_fast_forward_with_limit(old_vid, new_vid, versions, DEFAULT_MAX_NODES)
}

/// Like [`is_fast_forward`] but with an explicit BFS node cap.
///
/// Returns `Err(CasError::SyncError("history too deep"))` when the number of
/// distinct versions visited during the BFS exceeds `max_nodes`.  This
/// prevents a pathologically long (or adversarially crafted) version history
/// from causing unbounded memory allocation and blocking the event loop.
pub fn is_fast_forward_with_limit(
    old_vid: &VersionId,
    new_vid: &VersionId,
    versions: &dyn VersionRepo,
    max_nodes: usize,
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
    let mut nodes_visited: usize = 0;

    while let Some(id) = queue.pop_front() {
        if !visited.insert(id.clone()) {
            continue;
        }
        nodes_visited += 1;
        if nodes_visited > max_nodes {
            return Err(CasError::SyncError(format!(
                "history too deep: BFS exceeded {max_nodes} version nodes"
            )));
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

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::model::{VersionId, VersionNode, hash_bytes};
    use crate::store::memory::InMemoryVersionRepo;
    use crate::store::traits::VersionRepo as VersionRepoTrait;

    fn vid(s: &str) -> VersionId {
        VersionId(s.to_string())
    }

    fn make_version(id: &str, parents: Vec<&str>, repo: &dyn VersionRepoTrait) {
        let v = VersionNode {
            id: vid(id),
            parents: parents.iter().map(|p| vid(p)).collect(),
            root: hash_bytes(id.as_bytes()),
            message: id.into(),
            timestamp: 0,
            metadata: HashMap::new(),
        };
        repo.put_version(&v).unwrap();
    }

    fn repo() -> Arc<InMemoryVersionRepo> {
        Arc::new(InMemoryVersionRepo::default())
    }

    // ── Basic correctness (pre-existing behaviour) ────────────────────────

    #[test]
    fn linear_history_a_b_c() {
        let r = repo();
        make_version("a", vec![], r.as_ref());
        make_version("b", vec!["a"], r.as_ref());
        make_version("c", vec!["b"], r.as_ref());

        assert!(is_fast_forward(&vid("a"), &vid("c"), r.as_ref()).unwrap());
        assert!(is_fast_forward(&vid("b"), &vid("c"), r.as_ref()).unwrap());
        assert!(!is_fast_forward(&vid("c"), &vid("b"), r.as_ref()).unwrap());
    }

    #[test]
    fn forked_history() {
        let r = repo();
        make_version("a", vec![], r.as_ref());
        make_version("b", vec!["a"], r.as_ref());
        make_version("c", vec!["a"], r.as_ref());

        assert!(!is_fast_forward(&vid("b"), &vid("c"), r.as_ref()).unwrap());
        assert!(!is_fast_forward(&vid("c"), &vid("b"), r.as_ref()).unwrap());
    }

    #[test]
    fn merge_commit() {
        let r = repo();
        make_version("a", vec![], r.as_ref());
        make_version("b", vec!["a"], r.as_ref());
        make_version("c", vec!["a"], r.as_ref());
        make_version("d", vec!["b", "c"], r.as_ref());

        assert!(is_fast_forward(&vid("a"), &vid("d"), r.as_ref()).unwrap());
        assert!(is_fast_forward(&vid("b"), &vid("d"), r.as_ref()).unwrap());
        assert!(is_fast_forward(&vid("c"), &vid("d"), r.as_ref()).unwrap());
    }

    #[test]
    fn empty_old_always_ff() {
        let r = repo();
        make_version("a", vec![], r.as_ref());
        let zeros = VersionId("0".repeat(64));
        assert!(is_fast_forward(&zeros, &vid("a"), r.as_ref()).unwrap());
        let empty = VersionId(String::new());
        assert!(is_fast_forward(&empty, &vid("a"), r.as_ref()).unwrap());
    }

    // ── E05-S10: depth limit ──────────────────────────────────────────────

    /// Build a linear chain of `n` versions: v0 → v1 → … → v(n-1).
    /// Returns the ID of the head (v(n-1)) and the tail (v0).
    fn build_linear_chain(n: usize, repo: &dyn VersionRepoTrait) -> (VersionId, VersionId) {
        let tail = vid(&format!("chain-v0"));
        make_version("chain-v0", vec![], repo);
        for i in 1..n {
            let parent = format!("chain-v{}", i - 1);
            let current = format!("chain-v{i}");
            make_version(&current, vec![&parent], repo);
        }
        let head = vid(&format!("chain-v{}", n - 1));
        (head, tail)
    }

    /// A chain of 101 001 versions with limit=100 000 must return an error.
    #[test]
    fn fast_forward_rejects_pathologically_deep_history() {
        let r = repo();
        let chain_len = 101_001; // one more than the default cap
        let (head, tail) = build_linear_chain(chain_len, r.as_ref());

        let result = is_fast_forward_with_limit(&tail, &head, r.as_ref(), 100_000);
        assert!(
            result.is_err(),
            "expected Err for a chain of {chain_len} versions, got Ok"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("too deep") || msg.contains("exceeded"),
            "unexpected error message: {msg}"
        );
    }

    /// A chain of exactly 100 000 versions (at the limit) must succeed.
    #[test]
    fn fast_forward_allows_history_at_limit() {
        let r = repo();
        let (head, tail) = build_linear_chain(100_000, r.as_ref());
        // The BFS visits up to 100_000 nodes; the limit is inclusive-on-equal
        // because we check `> max_nodes`, so 100_000 visited must be Ok.
        let result = is_fast_forward_with_limit(&tail, &head, r.as_ref(), 100_000);
        assert!(result.is_ok(), "expected Ok at the limit, got {result:?}");
        assert!(result.unwrap());
    }

    /// The public `is_fast_forward` wrapper honours the default 100 000 limit.
    #[test]
    fn public_wrapper_uses_default_limit() {
        let r = repo();
        let (head, tail) = build_linear_chain(100_001, r.as_ref());
        let result = is_fast_forward(&tail, &head, r.as_ref());
        assert!(result.is_err(), "public wrapper must enforce default limit");
    }
}
