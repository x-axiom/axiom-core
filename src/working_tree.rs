//! Working-tree status computation for workspace-bound local folders.
//!
//! Given a `(local_path, head_version)` pair, [`compute_status`] produces a
//! per-file change list by:
//!
//! 1. Walking the local directory with [`walkdir`], skipping ignored paths.
//! 2. Computing each file's Merkle root hash in memory (no store writes).
//! 3. Comparing against the file hashes recorded in the HEAD version tree.
//! 4. Collecting files present in HEAD but absent on disk as `Deleted`.
//!
//! This is the foundation for the Changes UI (E15-S05), stage/commit
//! (E15-S06), and checkout protection (E15-S08).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use walkdir::WalkDir;

use crate::chunker::{chunk_bytes, ChunkPolicy};
use crate::error::{CasError, CasResult};
use crate::merkle::DEFAULT_FAN_OUT;
use crate::model::hash::{hash_bytes, hash_children, ChunkHash, VersionId};
use crate::model::node::NodeKind;
use crate::store::traits::{NodeStore, TreeStore, VersionRepo};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// The kind of change detected for a local path relative to HEAD.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileChange {
    /// Path exists locally but is not tracked in HEAD (new file).
    Untracked,
    /// Path exists in both HEAD and locally, but content has changed.
    Modified,
    /// Path is tracked in HEAD but the local file has been removed.
    Deleted,
    /// Reserved for future use (e.g. file↔directory flip). Not emitted
    /// in the current implementation.
    TypeChanged,
}

/// A single entry in the working-tree status output.
#[derive(Clone, Debug)]
pub struct WorkingTreeEntry {
    /// Workspace-relative path using `/` separators (e.g. `"src/main.rs"`).
    pub path: String,
    /// The kind of change detected.
    pub change: FileChange,
    /// Size of the local file in bytes. `None` for `Deleted` entries.
    pub local_size: Option<u64>,
    /// Size recorded in HEAD. `None` for `Untracked` entries.
    pub head_size: Option<u64>,
}

/// The full status of a workspace relative to its HEAD version.
#[derive(Clone, Debug)]
pub struct WorkingTreeStatus {
    /// Every path that differs from HEAD. Unchanged files are not included.
    pub entries: Vec<WorkingTreeEntry>,
    /// The version that was used as the comparison base. `None` when no
    /// HEAD exists yet (all local files are `Untracked`).
    pub head_version: Option<VersionId>,
}

impl WorkingTreeStatus {
    /// Returns `true` if there are any uncommitted changes.
    pub fn is_dirty(&self) -> bool {
        !self.entries.is_empty()
    }
}

// ---------------------------------------------------------------------------
// IgnoreMatcher — stub replaced by S03
// ---------------------------------------------------------------------------

/// Path ignore matcher. The stub implementation ignores nothing; E15-S03
/// will replace this with a real `.axiomignore`-backed matcher.
pub struct IgnoreMatcher {
    _private: (),
}

impl IgnoreMatcher {
    /// Create a matcher that ignores nothing (all paths are visible).
    pub fn none() -> Self {
        Self { _private: () }
    }

    /// Returns `true` if the workspace-relative `path` should be excluded
    /// from status computation and commits.
    pub fn is_ignored(&self, _path: &str) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Internal: Merkle root computation (no store writes)
// ---------------------------------------------------------------------------

/// Compute the Merkle root hash of `data` using the same algorithm as
/// [`crate::merkle::build_tree`], but without persisting any tree nodes.
///
/// Identical inputs always produce the same root, so this can be compared
/// directly against the `ChunkHash` stored in the HEAD `NodeEntry`.
pub fn compute_file_root(data: &[u8]) -> ChunkHash {
    if data.is_empty() {
        return hash_bytes(data);
    }

    // 1. Split into variable-size chunks.
    let policy = ChunkPolicy::default();
    let raw_chunks = chunk_bytes(data, &policy);

    // `chunk_bytes` returns `(hash, data, offset, len)` tuples; we only need
    // the hashes for the Merkle levels.
    let mut level: Vec<ChunkHash> = raw_chunks.into_iter().map(|(h, _, _, _)| h).collect();

    // 2. Iteratively collapse levels until a single root remains.
    while level.len() > 1 {
        let mut next: Vec<ChunkHash> = Vec::new();
        for group in level.chunks(DEFAULT_FAN_OUT) {
            if group.len() == 1 {
                next.push(group[0]);
            } else {
                next.push(hash_children(group));
            }
        }
        level = next;
    }

    level[0]
}

/// Read a file from disk and compute its Merkle root. Returns an `Io` error
/// if the file cannot be read.
fn compute_file_root_from_path(path: &Path) -> CasResult<(ChunkHash, u64)> {
    let data = std::fs::read(path)?;
    let size = data.len() as u64;
    let root = compute_file_root(&data);
    Ok((root, size))
}

// ---------------------------------------------------------------------------
// Internal: walk HEAD tree into a flat map
// ---------------------------------------------------------------------------

/// Recursively walk the node tree rooted at `root_hash` and collect all
/// file entries as `(workspace-relative-path) → (chunk-root-hash, size)`.
///
/// `prefix` is the path accumulated so far (empty for the workspace root).
fn collect_files_from_tree(
    root_hash: &ChunkHash,
    prefix: &str,
    nodes: &dyn NodeStore,
) -> CasResult<HashMap<String, (ChunkHash, u64)>> {
    let entry = match nodes.get_node(root_hash)? {
        Some(e) => e,
        None => {
            return Err(CasError::NotFound(format!(
                "node {} not found while collecting tree",
                root_hash.to_hex()
            )));
        }
    };

    let mut out = HashMap::new();

    match entry.kind {
        NodeKind::File { root, size } => {
            // Only record the node if it has a non-empty path (i.e., not the
            // workspace root itself, which should always be a directory).
            if !prefix.is_empty() {
                out.insert(prefix.to_string(), (root, size));
            }
        }
        NodeKind::Directory { children } => {
            for (name, child_hash) in children {
                let child_path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };
                let child_entries =
                    collect_files_from_tree(&child_hash, &child_path, nodes)?;
                out.extend(child_entries);
            }
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compute the working-tree status of `local_path` relative to `head_version`.
///
/// # Parameters
///
/// - `local_path` — absolute (or relative) path to the workspace root on
///   disk. Must exist and be a directory.
/// - `head_version` — the `VersionId` to compare against, or `None` when the
///   workspace has never been committed (all local files are `Untracked`).
/// - `versions` — used to resolve the version's root tree hash.
/// - `_trees` — reserved for future `TreeStore`-level operations (e.g. chunk
///   diffing). Not used in the current implementation.
/// - `nodes` — used to walk the HEAD node tree.
/// - `ignore` — controls which local paths to skip.
///
/// # Returns
///
/// A [`WorkingTreeStatus`] with one [`WorkingTreeEntry`] per changed path.
/// Unchanged files are not included. Entries are sorted by path.
pub fn compute_status(
    local_path: &Path,
    head_version: Option<&VersionId>,
    versions: &dyn VersionRepo,
    _trees: &dyn TreeStore,
    nodes: &dyn NodeStore,
    ignore: &IgnoreMatcher,
) -> CasResult<WorkingTreeStatus> {
    // ------------------------------------------------------------------
    // 1. Build the HEAD file map (empty when there is no HEAD version).
    // ------------------------------------------------------------------
    let head_files: HashMap<String, (ChunkHash, u64)> = match head_version {
        None => HashMap::new(),
        Some(vid) => {
            let version = versions
                .get_version(vid)?
                .ok_or_else(|| CasError::NotFound(format!("version {vid} not found")))?;
            collect_files_from_tree(&version.root, "", nodes)?
        }
    };

    // ------------------------------------------------------------------
    // 2. Walk the local directory and classify each file.
    // ------------------------------------------------------------------
    let mut entries: Vec<WorkingTreeEntry> = Vec::new();
    let mut local_paths: HashSet<String> = HashSet::new();

    for entry in WalkDir::new(local_path)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|e| {
            // Skip hidden directories (e.g. `.git`) at any level.
            let name = e.file_name().to_string_lossy();
            !name.starts_with('.') || e.depth() == 0
        })
    {
        let entry = entry.map_err(|e| CasError::Io(e.into()))?;

        // Only process regular files.
        if !entry.file_type().is_file() {
            continue;
        }

        // Compute the workspace-relative path with `/` separators.
        let rel = entry
            .path()
            .strip_prefix(local_path)
            .map_err(|_| CasError::InvalidObject("path strip_prefix failed".into()))?;

        let rel_str = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");

        if ignore.is_ignored(&rel_str) {
            continue;
        }

        local_paths.insert(rel_str.clone());

        let (local_root, local_size) = compute_file_root_from_path(entry.path())?;

        match head_files.get(&rel_str) {
            None => {
                // Not in HEAD → Untracked.
                entries.push(WorkingTreeEntry {
                    path: rel_str,
                    change: FileChange::Untracked,
                    local_size: Some(local_size),
                    head_size: None,
                });
            }
            Some((head_root, head_size)) => {
                if local_root != *head_root {
                    // Same path, different hash → Modified.
                    entries.push(WorkingTreeEntry {
                        path: rel_str,
                        change: FileChange::Modified,
                        local_size: Some(local_size),
                        head_size: Some(*head_size),
                    });
                }
                // Equal hash → unchanged, not included.
            }
        }
    }

    // ------------------------------------------------------------------
    // 3. Files in HEAD that are no longer on disk → Deleted.
    // ------------------------------------------------------------------
    for (path, (_head_root, head_size)) in &head_files {
        if !local_paths.contains(path) {
            entries.push(WorkingTreeEntry {
                path: path.clone(),
                change: FileChange::Deleted,
                local_size: None,
                head_size: Some(*head_size),
            });
        }
    }

    // Sort by path for deterministic output.
    entries.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(WorkingTreeStatus {
        entries,
        head_version: head_version.cloned(),
    })
}
