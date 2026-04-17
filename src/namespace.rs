//! Directory tree builder, path resolution, and namespace utilities.
//!
//! Converts a flat list of `(path, file_root_hash, file_size)` entries into a
//! content-addressed directory tree. Every directory node's hash is derived
//! deterministically from its sorted children, so unchanged subtrees always
//! produce the same hash and can be reused across versions.

use std::collections::BTreeMap;

use crate::error::{CasError, CasResult};
use crate::model::hash::{ChunkHash, VersionId};
use crate::model::node::{NodeEntry, NodeKind};
use crate::store::traits::{NodeStore, PathIndexRepo};

// ---------------------------------------------------------------------------
// Input type
// ---------------------------------------------------------------------------

/// A file to include in the directory tree.
#[derive(Clone, Debug)]
pub struct FileInput {
    /// Relative path, e.g. `"src/main.rs"`. Must not start with `/`.
    pub path: String,
    /// Merkle tree root hash of the file's content.
    pub root: ChunkHash,
    /// Total file size in bytes.
    pub size: u64,
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// Build a content-addressed directory tree from a list of files, persist
/// every node through `node_store`, index paths through `path_index` for the
/// given `version_id`, and return the root directory's `NodeEntry`.
///
/// # Errors
///
/// Returns an error if the file list is empty, a path is invalid, or
/// persistence fails.
pub fn build_directory_tree<N: NodeStore, P: PathIndexRepo>(
    files: &[FileInput],
    version_id: &VersionId,
    node_store: &N,
    path_index: &P,
) -> CasResult<NodeEntry> {
    if files.is_empty() {
        return Err(CasError::InvalidObject(
            "cannot build directory tree from empty file list".into(),
        ));
    }

    // -----------------------------------------------------------------------
    // 1. Build an in-memory tree of intermediate directory nodes.
    // -----------------------------------------------------------------------
    let mut root_children: BTreeMap<String, DirBuildNode> = BTreeMap::new();

    for file in files {
        let parts: Vec<&str> = file.path.split('/').collect();
        if parts.is_empty() || parts.iter().any(|p| p.is_empty()) {
            return Err(CasError::InvalidObject(format!(
                "invalid path: {:?}",
                file.path
            )));
        }
        insert_path(&mut root_children, &parts, file.root, file.size);
    }

    // -----------------------------------------------------------------------
    // 2. Recursively materialise from leaves to root.
    // -----------------------------------------------------------------------
    let root_entry = materialise_dir(
        &root_children,
        "",           // path prefix for root
        version_id,
        node_store,
        path_index,
    )?;

    // Index the root itself.
    path_index.put_path_entry(version_id, "", &root_entry.hash, true)?;

    Ok(root_entry)
}

// ---------------------------------------------------------------------------
// In-memory build helpers
// ---------------------------------------------------------------------------

enum DirBuildNode {
    File { root: ChunkHash, size: u64 },
    Dir(BTreeMap<String, DirBuildNode>),
}

fn insert_path(
    children: &mut BTreeMap<String, DirBuildNode>,
    parts: &[&str],
    root: ChunkHash,
    size: u64,
) {
    debug_assert!(!parts.is_empty());

    if parts.len() == 1 {
        // Leaf file.
        children.insert(
            parts[0].to_string(),
            DirBuildNode::File { root, size },
        );
    } else {
        // Intermediate directory.
        let dir_name = parts[0].to_string();
        let sub = children
            .entry(dir_name)
            .or_insert_with(|| DirBuildNode::Dir(BTreeMap::new()));

        match sub {
            DirBuildNode::Dir(sub_children) => {
                insert_path(sub_children, &parts[1..], root, size);
            }
            DirBuildNode::File { .. } => {
                // A file already exists at this path component — overwrite
                // with a directory (unusual, but handle gracefully).
                let mut sub_children = BTreeMap::new();
                insert_path(&mut sub_children, &parts[1..], root, size);
                *sub = DirBuildNode::Dir(sub_children);
            }
        }
    }
}

/// Recursively materialise a directory node from its build-tree children.
fn materialise_dir<N: NodeStore, P: PathIndexRepo>(
    children: &BTreeMap<String, DirBuildNode>,
    prefix: &str,
    version_id: &VersionId,
    node_store: &N,
    path_index: &P,
) -> CasResult<NodeEntry> {
    let mut child_hashes: BTreeMap<String, ChunkHash> = BTreeMap::new();

    for (name, child) in children {
        let child_path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };

        let entry = match child {
            DirBuildNode::File { root, size } => {
                let file_entry = make_file_node(*root, *size);
                node_store.put_node(&file_entry)?;
                path_index.put_path_entry(
                    version_id,
                    &child_path,
                    &file_entry.hash,
                    false,
                )?;
                file_entry
            }
            DirBuildNode::Dir(sub_children) => {
                let dir_entry = materialise_dir(
                    sub_children,
                    &child_path,
                    version_id,
                    node_store,
                    path_index,
                )?;
                path_index.put_path_entry(
                    version_id,
                    &child_path,
                    &dir_entry.hash,
                    true,
                )?;
                dir_entry
            }
        };

        child_hashes.insert(name.clone(), entry.hash);
    }

    let dir_entry = make_dir_node(child_hashes);
    node_store.put_node(&dir_entry)?;

    Ok(dir_entry)
}

// ---------------------------------------------------------------------------
// Node constructors with deterministic hashing
// ---------------------------------------------------------------------------

/// Create a file `NodeEntry` whose hash is the file's Merkle tree root.
fn make_file_node(root: ChunkHash, size: u64) -> NodeEntry {
    // For a file node, the identity IS the Merkle tree root hash.
    NodeEntry {
        hash: root,
        kind: NodeKind::File { root, size },
    }
}

/// Create a directory `NodeEntry` with a deterministic hash derived from
/// its sorted child (name, hash) pairs.
fn make_dir_node(children: BTreeMap<String, ChunkHash>) -> NodeEntry {
    let hash = hash_directory(&children);
    NodeEntry {
        hash,
        kind: NodeKind::Directory { children },
    }
}

/// Compute a deterministic BLAKE3 hash for a directory from its sorted
/// children. The hash covers `name || child_hash` for each child in
/// `BTreeMap` order (lexicographic by name).
fn hash_directory(children: &BTreeMap<String, ChunkHash>) -> ChunkHash {
    let mut hasher = blake3::Hasher::new();
    for (name, hash) in children {
        hasher.update(name.as_bytes());
        hasher.update(hash.as_bytes());
    }
    hasher.finalize()
}

// ---------------------------------------------------------------------------
// Path resolution
// ---------------------------------------------------------------------------

/// Resolve a `/`-separated path relative to a directory root, walking
/// through nested directories via the `NodeStore`.
///
/// Returns the `NodeEntry` at the given path, or `None` if not found.
pub fn resolve_path<N: NodeStore>(
    root: &ChunkHash,
    path: &str,
    node_store: &N,
) -> CasResult<Option<NodeEntry>> {
    let root_entry = match node_store.get_node(root)? {
        Some(e) => e,
        None => return Ok(None),
    };

    if path.is_empty() {
        return Ok(Some(root_entry));
    }

    let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    walk(&root_entry, &parts, node_store)
}

fn walk<N: NodeStore>(
    current: &NodeEntry,
    remaining: &[&str],
    node_store: &N,
) -> CasResult<Option<NodeEntry>> {
    if remaining.is_empty() {
        return Ok(Some(current.clone()));
    }

    match &current.kind {
        NodeKind::Directory { children } => {
            let name = remaining[0];
            match children.get(name) {
                Some(child_hash) => {
                    let child = match node_store.get_node(child_hash)? {
                        Some(e) => e,
                        None => return Ok(None),
                    };
                    walk(&child, &remaining[1..], node_store)
                }
                None => Ok(None),
            }
        }
        NodeKind::File { .. } => {
            // Tried to descend into a file — path doesn't exist.
            Ok(None)
        }
    }
}
