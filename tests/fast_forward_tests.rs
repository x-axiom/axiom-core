//! Tests for the canonical fast-forward detection algorithm (E04-S05).

use std::collections::HashMap;

use axiom_core::model::{ChunkHash, VersionId, VersionNode};
use axiom_core::store::memory::InMemoryVersionRepo;
use axiom_core::store::traits::VersionRepo;
use axiom_core::sync::fast_forward::is_fast_forward;

fn vid(name: &str) -> VersionId {
    // Pad to 64 hex chars so empty-vid heuristics never accidentally fire.
    let padded = format!("{name:0>64}");
    VersionId(padded)
}

fn put(repo: &dyn VersionRepo, id: &VersionId, parents: Vec<VersionId>) {
    repo.put_version(&VersionNode {
        id: id.clone(),
        parents,
        root: ChunkHash::from([0u8; 32]),
        message: String::new(),
        timestamp: 0,
        metadata: HashMap::new(),
    })
    .unwrap();
}

#[test]
fn linear_history_a_to_c_is_ff() {
    // A → B → C
    let repo = InMemoryVersionRepo::new();
    let a = vid("a");
    let b = vid("b");
    let c = vid("c");
    put(&repo, &a, vec![]);
    put(&repo, &b, vec![a.clone()]);
    put(&repo, &c, vec![b.clone()]);

    assert!(is_fast_forward(&a, &c, &repo).unwrap());
    assert!(is_fast_forward(&b, &c, &repo).unwrap());
    assert!(is_fast_forward(&a, &b, &repo).unwrap());
}

#[test]
fn forked_history_is_not_ff() {
    // A → B,  A → C   (two siblings)
    let repo = InMemoryVersionRepo::new();
    let a = vid("a");
    let b = vid("b");
    let c = vid("c");
    put(&repo, &a, vec![]);
    put(&repo, &b, vec![a.clone()]);
    put(&repo, &c, vec![a.clone()]);

    // B → C is *not* a fast-forward (B is not an ancestor of C).
    assert!(!is_fast_forward(&b, &c, &repo).unwrap());
    assert!(!is_fast_forward(&c, &b, &repo).unwrap());
}

#[test]
fn merge_history_a_to_d_is_ff() {
    // A → B
    // A → C
    //  B,C → D  (D has two parents)
    let repo = InMemoryVersionRepo::new();
    let a = vid("a");
    let b = vid("b");
    let c = vid("c");
    let d = vid("d");
    put(&repo, &a, vec![]);
    put(&repo, &b, vec![a.clone()]);
    put(&repo, &c, vec![a.clone()]);
    put(&repo, &d, vec![b.clone(), c.clone()]);

    assert!(is_fast_forward(&a, &d, &repo).unwrap());
    assert!(is_fast_forward(&b, &d, &repo).unwrap());
    assert!(is_fast_forward(&c, &d, &repo).unwrap());
}

#[test]
fn equal_ids_is_ff() {
    let repo = InMemoryVersionRepo::new();
    let a = vid("a");
    put(&repo, &a, vec![]);
    assert!(is_fast_forward(&a, &a, &repo).unwrap());
}

#[test]
fn empty_old_is_always_ff() {
    // Creating a brand-new ref: old is empty / all-zeroes.
    let repo = InMemoryVersionRepo::new();
    let new_tip = vid("a");
    put(&repo, &new_tip, vec![]);

    // Empty string.
    assert!(is_fast_forward(&VersionId(String::new()), &new_tip, &repo).unwrap());
    // All-zero 64-char hex.
    assert!(
        is_fast_forward(&VersionId("0".repeat(64)), &new_tip, &repo).unwrap()
    );
}

#[test]
fn missing_version_returns_false_not_error() {
    // `new` exists but its parent is not stored — BFS should terminate
    // gracefully and return false (not propagate an error).
    let repo = InMemoryVersionRepo::new();
    let a = vid("a"); // never inserted — represents the "missing" target
    let new_tip = vid("new");
    put(&repo, &new_tip, vec![vid("missing-parent")]);

    let result = is_fast_forward(&a, &new_tip, &repo).unwrap();
    assert!(!result);
}

#[test]
fn empty_repo_with_any_targets() {
    // No versions at all in the repo: BFS finishes with no matches → false.
    let repo = InMemoryVersionRepo::new();
    let a = vid("a");
    let b = vid("b");
    assert!(!is_fast_forward(&a, &b, &repo).unwrap());
}

#[test]
fn deep_linear_history_terminates() {
    // 200 versions chained linearly — confirms iterative BFS does not blow up.
    let repo = InMemoryVersionRepo::new();
    let mut prev: Option<VersionId> = None;
    let mut all = Vec::new();
    for i in 0..200 {
        let id = vid(&format!("v{i}"));
        put(
            &repo,
            &id,
            prev.clone().map(|p| vec![p]).unwrap_or_default(),
        );
        all.push(id.clone());
        prev = Some(id);
    }
    assert!(is_fast_forward(&all[0], all.last().unwrap(), &repo).unwrap());
}
