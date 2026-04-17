//! Diff engine for comparing version roots at directory, file, and chunk level.
//!
//! The engine walks two directory trees (represented as `NodeEntry` roots),
//! compares children by name, and for modified files descends into the Merkle
//! tree to produce chunk-level statistics. Unchanged subtrees are skipped
//! by hash comparison.

use std::collections::{BTreeSet, HashSet};

use crate::error::{CasError, CasResult};
use crate::model::diff::{DiffEntry, DiffKind, DiffResult};
use crate::model::hash::ChunkHash;
use crate::model::node::{NodeEntry, NodeKind};
use crate::model::tree::TreeNodeKind;
use crate::store::traits::{NodeStore, TreeStore};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compare two version roots and return a full diff result.
///
/// Both `old_root` and `new_root` should be hashes of directory `NodeEntry`
/// nodes. The engine walks the directory trees, emitting `DiffEntry` items
/// for added, removed, and modified paths, and computes chunk-level
/// statistics for modified files.
pub fn diff_versions<N: NodeStore, T: TreeStore>(
    old_root: &ChunkHash,
    new_root: &ChunkHash,
    node_store: &N,
    tree_store: &T,
) -> CasResult<DiffResult> {
    // Fast path: identical roots mean no changes.
    if old_root == new_root {
        return Ok(DiffResult::empty());
    }

    let old_node = node_store
        .get_node(old_root)?
        .ok_or_else(|| CasError::NotFound(format!("node {}", old_root.to_hex())))?;
    let new_node = node_store
        .get_node(new_root)?
        .ok_or_else(|| CasError::NotFound(format!("node {}", new_root.to_hex())))?;

    let mut entries = Vec::new();
    let mut stats = ChunkStats::default();

    walk_diff("", &old_node, &new_node, node_store, tree_store, &mut entries, &mut stats)?;

    let added_files = entries.iter().filter(|e| e.kind == DiffKind::Added).count();
    let removed_files = entries.iter().filter(|e| e.kind == DiffKind::Removed).count();
    let modified_files = entries.iter().filter(|e| e.kind == DiffKind::Modified).count();

    Ok(DiffResult {
        entries,
        added_files,
        removed_files,
        modified_files,
        added_chunks: stats.added_chunks,
        removed_chunks: stats.removed_chunks,
        unchanged_chunks: stats.unchanged_chunks,
    })
}

// ---------------------------------------------------------------------------
// Internal: recursive directory walk
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ChunkStats {
    added_chunks: usize,
    removed_chunks: usize,
    unchanged_chunks: usize,
}

/// Recursively walk two directory nodes and collect diff entries.
fn walk_diff<N: NodeStore, T: TreeStore>(
    prefix: &str,
    old_node: &NodeEntry,
    new_node: &NodeEntry,
    node_store: &N,
    tree_store: &T,
    entries: &mut Vec<DiffEntry>,
    stats: &mut ChunkStats,
) -> CasResult<()> {
    let old_children = dir_children(old_node);
    let new_children = dir_children(new_node);

    // Collect all unique child names.
    let all_names: BTreeSet<&str> = old_children
        .keys()
        .chain(new_children.keys())
        .map(|s| s.as_str())
        .collect();

    for name in all_names {
        let path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };

        match (old_children.get(name), new_children.get(name)) {
            // Added
            (None, Some(new_hash)) => {
                collect_added(&path, new_hash, node_store, tree_store, entries, stats)?;
            }
            // Removed
            (Some(old_hash), None) => {
                collect_removed(&path, old_hash, node_store, tree_store, entries, stats)?;
            }
            // Possibly modified
            (Some(old_hash), Some(new_hash)) => {
                if old_hash == new_hash {
                    // Unchanged subtree — skip entirely.
                    // Count unchanged chunks if this is a file.
                    count_unchanged_if_file(old_hash, node_store, tree_store, stats)?;
                    continue;
                }

                let old_child = node_store.get_node(old_hash)?.ok_or_else(|| {
                    CasError::NotFound(format!("node {}", old_hash.to_hex()))
                })?;
                let new_child = node_store.get_node(new_hash)?.ok_or_else(|| {
                    CasError::NotFound(format!("node {}", new_hash.to_hex()))
                })?;

                match (&old_child.kind, &new_child.kind) {
                    // Both directories: recurse.
                    (NodeKind::Directory { .. }, NodeKind::Directory { .. }) => {
                        walk_diff(&path, &old_child, &new_child, node_store, tree_store, entries, stats)?;
                    }
                    // Both files: report modified + chunk diff.
                    (NodeKind::File { root: old_root, .. }, NodeKind::File { root: new_root, .. }) => {
                        entries.push(DiffEntry {
                            path: path.clone(),
                            kind: DiffKind::Modified,
                            old_hash: Some(*old_hash),
                            new_hash: Some(*new_hash),
                        });
                        diff_file_chunks(old_root, new_root, tree_store, stats)?;
                    }
                    // Type changed (file -> dir or dir -> file): remove old + add new.
                    _ => {
                        collect_removed(&path, old_hash, node_store, tree_store, entries, stats)?;
                        collect_added(&path, new_hash, node_store, tree_store, entries, stats)?;
                    }
                }
            }
            (None, None) => unreachable!(),
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers: extract children map from a NodeEntry
// ---------------------------------------------------------------------------

/// Get children map from a directory node. Returns empty map for files.
fn dir_children(node: &NodeEntry) -> &std::collections::BTreeMap<String, ChunkHash> {
    static EMPTY: std::sync::LazyLock<std::collections::BTreeMap<String, ChunkHash>> =
        std::sync::LazyLock::new(std::collections::BTreeMap::new);
    match &node.kind {
        NodeKind::Directory { children } => children,
        NodeKind::File { .. } => &EMPTY,
    }
}

// ---------------------------------------------------------------------------
// Helpers: collect all paths under an added/removed subtree
// ---------------------------------------------------------------------------

fn collect_added<N: NodeStore, T: TreeStore>(
    path: &str,
    hash: &ChunkHash,
    node_store: &N,
    tree_store: &T,
    entries: &mut Vec<DiffEntry>,
    stats: &mut ChunkStats,
) -> CasResult<()> {
    let node = node_store.get_node(hash)?.ok_or_else(|| {
        CasError::NotFound(format!("node {}", hash.to_hex()))
    })?;

    match &node.kind {
        NodeKind::File { root, .. } => {
            entries.push(DiffEntry {
                path: path.to_string(),
                kind: DiffKind::Added,
                old_hash: None,
                new_hash: Some(*hash),
            });
            stats.added_chunks += count_leaves(root, tree_store)?;
        }
        NodeKind::Directory { children } => {
            for (name, child_hash) in children {
                let child_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}/{name}")
                };
                collect_added(&child_path, child_hash, node_store, tree_store, entries, stats)?;
            }
        }
    }

    Ok(())
}

fn collect_removed<N: NodeStore, T: TreeStore>(
    path: &str,
    hash: &ChunkHash,
    node_store: &N,
    tree_store: &T,
    entries: &mut Vec<DiffEntry>,
    stats: &mut ChunkStats,
) -> CasResult<()> {
    let node = node_store.get_node(hash)?.ok_or_else(|| {
        CasError::NotFound(format!("node {}", hash.to_hex()))
    })?;

    match &node.kind {
        NodeKind::File { root, .. } => {
            entries.push(DiffEntry {
                path: path.to_string(),
                kind: DiffKind::Removed,
                old_hash: Some(*hash),
                new_hash: None,
            });
            stats.removed_chunks += count_leaves(root, tree_store)?;
        }
        NodeKind::Directory { children } => {
            for (name, child_hash) in children {
                let child_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}/{name}")
                };
                collect_removed(&child_path, child_hash, node_store, tree_store, entries, stats)?;
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers: file-level Merkle chunk diff
// ---------------------------------------------------------------------------

/// Compare two Merkle trees at chunk level. Counts added, removed, and
/// unchanged leaf chunks using set-based comparison on chunk hashes.
fn diff_file_chunks<T: TreeStore>(
    old_root: &ChunkHash,
    new_root: &ChunkHash,
    tree_store: &T,
    stats: &mut ChunkStats,
) -> CasResult<()> {
    if old_root == new_root {
        stats.unchanged_chunks += count_leaves(old_root, tree_store)?;
        return Ok(());
    }

    let old_leaves = collect_leaves(old_root, tree_store)?;
    let new_leaves = collect_leaves(new_root, tree_store)?;

    let old_set: HashSet<&ChunkHash> = old_leaves.iter().collect();
    let new_set: HashSet<&ChunkHash> = new_leaves.iter().collect();

    stats.added_chunks += new_set.difference(&old_set).count();
    stats.removed_chunks += old_set.difference(&new_set).count();
    stats.unchanged_chunks += old_set.intersection(&new_set).count();

    Ok(())
}

/// Recursively collect all leaf chunk hashes from a Merkle tree.
fn collect_leaves<T: TreeStore>(
    root: &ChunkHash,
    tree_store: &T,
) -> CasResult<Vec<ChunkHash>> {
    let node = tree_store.get_tree_node(root)?;
    match node {
        Some(n) => match &n.kind {
            TreeNodeKind::Leaf { chunk } => Ok(vec![*chunk]),
            TreeNodeKind::Internal { children } => {
                let mut leaves = Vec::new();
                for child in children {
                    leaves.extend(collect_leaves(child, tree_store)?);
                }
                Ok(leaves)
            }
        },
        // If tree node is not found, this file has no tree structure
        // (e.g. a single-chunk file stored directly). Treat root as leaf.
        None => Ok(vec![*root]),
    }
}

/// Count leaves without collecting them (for unchanged subtrees).
fn count_leaves<T: TreeStore>(
    root: &ChunkHash,
    tree_store: &T,
) -> CasResult<usize> {
    Ok(collect_leaves(root, tree_store)?.len())
}

/// Count unchanged chunks for a file node that didn't change.
fn count_unchanged_if_file<N: NodeStore, T: TreeStore>(
    hash: &ChunkHash,
    node_store: &N,
    tree_store: &T,
    stats: &mut ChunkStats,
) -> CasResult<()> {
    if let Some(node) = node_store.get_node(hash)? {
        match &node.kind {
            NodeKind::File { root, .. } => {
                stats.unchanged_chunks += count_leaves(root, tree_store)?;
            }
            NodeKind::Directory { children } => {
                for child_hash in children.values() {
                    count_unchanged_if_file(child_hash, node_store, tree_store, stats)?;
                }
            }
        }
    }
    Ok(())
}
