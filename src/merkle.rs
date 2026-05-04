//! Merkle tree builder and rehydrator for file-level chunk trees.
//!
//! Converts an ordered sequence of chunk descriptors into a persistent
//! multi-branch Merkle tree with a fixed fan-out (default 64). Every
//! internal node's hash is the BLAKE3 digest of its concatenated child
//! hashes, ensuring deterministic root computation.

use crate::error::{CasError, CasResult};
use crate::model::chunk::ChunkDescriptor;
use crate::model::hash::{hash_children, ChunkHash};
use crate::model::tree::{FileObject, TreeNode, TreeNodeKind};
use crate::store::traits::TreeStore;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Default fan-out for internal tree nodes.
pub const DEFAULT_FAN_OUT: usize = 64;

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// Build a Merkle tree from ordered chunk descriptors, persist every node
/// through the given [`TreeStore`], and return a [`FileObject`] representing
/// the file.
///
/// If `descriptors` is empty the function returns an error — a file must
/// have at least one chunk (even an empty file produces a zero-length chunk).
pub fn build_tree<S: TreeStore + ?Sized>(
    descriptors: &[ChunkDescriptor],
    fan_out: usize,
    store: &S,
) -> CasResult<FileObject> {
    if descriptors.is_empty() {
        return Err(CasError::InvalidObject(
            "cannot build tree from empty chunk list".into(),
        ));
    }

    let total_size: u64 = descriptors.iter().map(|d| d.length as u64).sum();
    let chunk_count = descriptors.len() as u32;

    // 1. Create leaf nodes (one per chunk).
    let mut current_level: Vec<ChunkHash> = Vec::with_capacity(descriptors.len());
    for desc in descriptors {
        let leaf = TreeNode {
            hash: desc.hash,
            kind: TreeNodeKind::Leaf { chunk: desc.hash },
        };
        store.put_tree_node(&leaf)?;
        current_level.push(desc.hash);
    }

    // 2. Iteratively build internal levels until we have a single root.
    while current_level.len() > 1 {
        let mut next_level: Vec<ChunkHash> = Vec::new();

        for group in current_level.chunks(fan_out) {
            if group.len() == 1 {
                // Promote single child directly — no wrapper node needed.
                next_level.push(group[0]);
            } else {
                let hash = hash_children(group);
                let node = TreeNode {
                    hash,
                    kind: TreeNodeKind::Internal {
                        children: group.to_vec(),
                    },
                };
                store.put_tree_node(&node)?;
                next_level.push(hash);
            }
        }

        current_level = next_level;
    }

    let root = current_level[0];

    Ok(FileObject {
        root,
        size: total_size,
        chunk_count,
    })
}

// ---------------------------------------------------------------------------
// Rehydrate (load ordered chunk hashes from root)
// ---------------------------------------------------------------------------

/// Traverse the Merkle tree rooted at `root` and return the ordered list
/// of leaf chunk hashes. This is enough to reconstruct the original file
/// by fetching each chunk from the CAS in order.
pub fn rehydrate<S: TreeStore + ?Sized>(
    root: &ChunkHash,
    store: &S,
) -> CasResult<Vec<ChunkHash>> {
    let node = store
        .get_tree_node(root)?
        .ok_or_else(|| CasError::NotFound(root.to_hex().to_string()))?;

    match &node.kind {
        TreeNodeKind::Leaf { chunk } => Ok(vec![*chunk]),
        TreeNodeKind::Internal { children } => {
            let mut leaves = Vec::new();
            for child_hash in children {
                let child_leaves = rehydrate(child_hash, store)?;
                leaves.extend(child_leaves);
            }
            Ok(leaves)
        }
    }
}
