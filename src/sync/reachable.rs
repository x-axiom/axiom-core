//! Reachable-object collection for the sync protocol (E03-S03).
//!
//! Starting from a set of **want** `VersionId`s, this module performs an
//! iterative BFS over the version DAG and the associated object graph
//! (NodeEntry → TreeNode → Chunk), collecting every object that needs to be
//! transferred to a peer that already has the **have** versions.
//!
//! The **have** set acts as a cut boundary: when a version in `have` is
//! reached during the BFS, traversal stops at that node (neither it nor any
//! of its ancestors are collected).  This mirrors Git's `want`/`have`
//! negotiation semantics.
//!
//! All traversal is iterative (VecDeque / explicit stack) to avoid stack
//! overflow on large histories (10 000+ versions).

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::{CasError, CasResult};
use crate::model::{
    ChunkHash, NodeKind, TreeNodeKind, VersionId,
};
use crate::store::traits::{NodeStore, ReachableObjects, TreeStore, VersionRepo};

// ─── CancelToken ─────────────────────────────────────────────────────────────

/// A lightweight cooperative cancellation token.
///
/// Pass a clone to `collect_reachable`; call [`CancelToken::cancel`] from
/// any thread to request early termination.  The next iteration of the BFS
/// will return `Err(CasError::SyncError("cancelled"))`.
#[derive(Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal cancellation.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Returns `true` if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

// ─── Public entry point ──────────────────────────────────────────────────────

/// Collect every object reachable from `want` that is **not** reachable from
/// `have`.
///
/// # Arguments
/// * `want`         — version IDs to start BFS from (e.g. remote ref tips).
/// * `have`         — version IDs the peer already has; acts as cut boundary.
/// * `version_repo` — access to the version DAG.
/// * `tree_store`   — access to `TreeNode` records.
/// * `node_store`   — access to `NodeEntry` records.
/// * `cancel`       — cooperative cancellation token; pass `&CancelToken::new()`
///                    for no cancellation.
///
/// # Returns
/// A [`ReachableObjects`] whose four sets contain every version, tree-node,
/// node-entry, and chunk hash that must be transferred.
pub fn collect_reachable(
    want: &[VersionId],
    have: &HashSet<VersionId>,
    version_repo: &dyn VersionRepo,
    tree_store: &dyn TreeStore,
    node_store: &dyn NodeStore,
    cancel: &CancelToken,
) -> CasResult<ReachableObjects> {
    let mut result = ReachableObjects::default();
    let mut visited_versions: HashSet<VersionId> = HashSet::new();
    let mut version_queue: VecDeque<VersionId> = want.iter().cloned().collect();

    while let Some(vid) = version_queue.pop_front() {
        if cancel.is_cancelled() {
            return Err(CasError::SyncError("cancelled".into()));
        }

        // Cut at `have` boundary — peer already has this version and everything
        // reachable from it.
        if have.contains(&vid) {
            continue;
        }

        // Skip already-visited nodes to handle DAG diamonds.
        if !visited_versions.insert(vid.clone()) {
            continue;
        }

        result.versions.insert(vid.clone());

        if let Some(v) = version_repo.get_version(&vid)? {
            // Collect all objects referenced by this version's directory tree.
            collect_node_tree(&v.root, node_store, tree_store, &mut result, cancel)?;

            // Enqueue parent versions for BFS.
            for parent in &v.parents {
                version_queue.push_back(parent.clone());
            }
        }
    }

    Ok(result)
}

// ─── Internal helpers ────────────────────────────────────────────────────────

/// Iteratively walk the `NodeEntry` namespace tree rooted at `root_hash`,
/// adding every node, tree-node, and chunk hash to `result`.
fn collect_node_tree(
    root_hash: &ChunkHash,
    node_store: &dyn NodeStore,
    tree_store: &dyn TreeStore,
    result: &mut ReachableObjects,
    cancel: &CancelToken,
) -> CasResult<()> {
    let mut node_stack: Vec<ChunkHash> = vec![root_hash.clone()];

    while let Some(node_hash) = node_stack.pop() {
        if cancel.is_cancelled() {
            return Err(CasError::SyncError("cancelled".into()));
        }

        // Dedup: skip already-collected node entries.
        if !result.node_hashes.insert(node_hash.clone()) {
            continue;
        }

        if let Some(node) = node_store.get_node(&node_hash)? {
            match node.kind {
                NodeKind::File { root, .. } => {
                    // root is a TreeNode hash.
                    collect_merkle_tree(&root, tree_store, result, cancel)?;
                }
                NodeKind::Directory { children } => {
                    for child_hash in children.into_values() {
                        node_stack.push(child_hash);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Iteratively walk a Merkle tree rooted at `root_hash`, adding every
/// tree-node and leaf chunk hash to `result`.
fn collect_merkle_tree(
    root_hash: &ChunkHash,
    tree_store: &dyn TreeStore,
    result: &mut ReachableObjects,
    cancel: &CancelToken,
) -> CasResult<()> {
    let mut tree_stack: Vec<ChunkHash> = vec![root_hash.clone()];

    while let Some(tree_hash) = tree_stack.pop() {
        if cancel.is_cancelled() {
            return Err(CasError::SyncError("cancelled".into()));
        }

        // Dedup: skip already-collected tree nodes.
        if !result.tree_hashes.insert(tree_hash.clone()) {
            continue;
        }

        if let Some(tree) = tree_store.get_tree_node(&tree_hash)? {
            match tree.kind {
                TreeNodeKind::Leaf { chunk } => {
                    result.chunk_hashes.insert(chunk);
                }
                TreeNodeKind::Internal { children } => {
                    for child_hash in children {
                        tree_stack.push(child_hash);
                    }
                }
            }
        }
    }

    Ok(())
}
