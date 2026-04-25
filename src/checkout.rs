//! Checkout: materialise a stored version onto a local file-system path.
//!
//! [`checkout_to_path`] is the single entry point.  It walks the version's
//! node tree, reconstructs every file from CAS, writes them to `target`, and
//! optionally deletes local files that are absent from the target version.
//!
//! Two safety levels are supported:
//!
//! * **[`CheckoutMode::Safe`]** — files whose local content differs from the
//!   version being checked out are left untouched and collected in
//!   [`CheckoutReport::skipped_dirty`].  Local files absent from the target
//!   version are also preserved.  Use this when the caller has already
//!   verified the working tree is clean, or wants to let the user decide what
//!   to do with modified files.
//!
//! * **[`CheckoutMode::Force`]** — all target-version files are written
//!   unconditionally, and local files absent from the target version are
//!   deleted.  After a Force checkout, [`crate::working_tree::compute_status`]
//!   will return an empty entry list.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use crate::api::state::AppState;
use crate::error::{CasError, CasResult};
use crate::merkle::rehydrate;
use crate::model::hash::{ChunkHash, VersionId};
use crate::working_tree::{collect_files_from_tree, compute_file_root, IgnoreMatcher};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Controls how conflicting local modifications are handled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckoutMode {
    /// Skip files whose local content differs from the target version.
    /// Local files absent from the target version are preserved.
    Safe,
    /// Overwrite all files unconditionally.
    /// Local files absent from the target version are deleted.
    Force,
}

/// Summary of a completed checkout operation.
#[derive(Clone, Debug)]
pub struct CheckoutReport {
    /// Number of files written (created or overwritten).
    pub written: usize,
    /// Number of local files deleted (Force mode only; always 0 for Safe).
    pub deleted: usize,
    /// Workspace-relative paths skipped because the local content differed
    /// from the target version (Safe mode only).
    pub skipped_dirty: Vec<String>,
}

// ---------------------------------------------------------------------------
// Internal: reconstruct a file from CAS
// ---------------------------------------------------------------------------

/// Reconstruct raw file bytes by rehydrating the Merkle tree and concatenating
/// all leaf chunks in order.
fn reconstruct_file(merkle_root: &ChunkHash, state: &AppState) -> CasResult<Vec<u8>> {
    let chunk_hashes = rehydrate(merkle_root, state.trees.as_ref())?;
    let mut bytes = Vec::new();
    for hash in chunk_hashes {
        let chunk = state
            .chunks
            .get_chunk(&hash)?
            .ok_or_else(|| CasError::NotFound(format!("chunk {} missing", hash.to_hex())))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Internal: build a platform-native absolute path from a `/`-separated
// workspace-relative path.
// ---------------------------------------------------------------------------

fn abs_path(target: &Path, rel: &str) -> PathBuf {
    rel.split('/').fold(target.to_path_buf(), |p, part| p.join(part))
}

// ---------------------------------------------------------------------------
// Internal: walk local dir → set of workspace-relative `/`-separated paths
// ---------------------------------------------------------------------------

fn walk_local_files(local_dir: &Path, ignore: &IgnoreMatcher) -> CasResult<HashSet<String>> {
    let mut out = HashSet::new();
    for entry in WalkDir::new(local_dir)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|e| {
            if e.depth() == 0 {
                return true;
            }
            if e.file_type().is_dir() {
                if let Ok(rel) = e.path().strip_prefix(local_dir) {
                    let rel_str: String = rel
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                        .join("/");
                    return !ignore.is_dir_ignored(&rel_str);
                }
            }
            true
        })
    {
        let entry = entry.map_err(|e| CasError::Io(e.into()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(local_dir)
            .map_err(|_| CasError::InvalidObject("path strip_prefix failed".into()))?;
        let rel_str = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");
        if !ignore.is_ignored(&rel_str) {
            out.insert(rel_str);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Materialise `version_id` onto the file-system path `target`.
///
/// # Parameters
///
/// - `version_id` — the version to check out.
/// - `target` — the local directory to write into (need not be empty).
/// - `mode` — [`CheckoutMode::Safe`] to protect dirty local files;
///   [`CheckoutMode::Force`] to overwrite unconditionally.
/// - `state` — provides all storage backends (chunks, trees, nodes, versions).
///
/// # Returns
///
/// A [`CheckoutReport`] with counts of written / deleted files and a list of
/// paths skipped due to local modifications (Safe mode).
///
/// # Errors
///
/// Returns an error if the version or any referenced object cannot be found
/// in the store, or if any file-system operation fails.
pub fn checkout_to_path(
    version_id: &VersionId,
    target: &Path,
    mode: CheckoutMode,
    state: &AppState,
) -> CasResult<CheckoutReport> {
    // ------------------------------------------------------------------
    // 1. Resolve the version's file tree.
    // ------------------------------------------------------------------
    let version = state
        .versions
        .get_version(version_id)?
        .ok_or_else(|| CasError::NotFound(format!("version {version_id} not found")))?;

    let target_files: HashMap<String, (ChunkHash, u64)> =
        collect_files_from_tree(&version.root, "", state.nodes.as_ref())?;

    // ------------------------------------------------------------------
    // 2. Walk the current local directory (for Safe-mode comparison and
    //    Force-mode deletion).  Apply built-in ignore rules so that
    //    `node_modules/`, `target/`, etc. are never touched.
    // ------------------------------------------------------------------
    let ignore = IgnoreMatcher::from_workspace_root(target);
    let local_paths = walk_local_files(target, &ignore)?;

    // ------------------------------------------------------------------
    // 3. Write files from the target version.
    // ------------------------------------------------------------------
    let mut written: usize = 0;
    let mut skipped_dirty: Vec<String> = Vec::new();

    // Sort for deterministic ordering and readable logs.
    let mut sorted_files: Vec<(&String, &(ChunkHash, u64))> = target_files.iter().collect();
    sorted_files.sort_by_key(|(p, _)| p.as_str());

    for (rel_path, (merkle_root, _size)) in &sorted_files {
        let abs = abs_path(target, rel_path);

        if abs.exists() {
            // Compare local content hash to the target version's hash.
            let local_data = std::fs::read(&abs).map_err(|e| {
                CasError::Io(std::io::Error::new(
                    e.kind(),
                    format!("failed to read '{}': {e}", abs.display()),
                ))
            })?;
            let local_root = compute_file_root(&local_data);

            if local_root == *merkle_root {
                // Already correct — no write needed.
                continue;
            }

            // Content differs.
            match mode {
                CheckoutMode::Safe => {
                    skipped_dirty.push(rel_path.to_string());
                    continue;
                }
                CheckoutMode::Force => { /* fall through to write */ }
            }
        }

        // Reconstruct file content and write to disk.
        let bytes = reconstruct_file(merkle_root, state)?;
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                CasError::Io(std::io::Error::new(
                    e.kind(),
                    format!("failed to create dir '{}': {e}", parent.display()),
                ))
            })?;
        }
        std::fs::write(&abs, &bytes).map_err(|e| {
            CasError::Io(std::io::Error::new(
                e.kind(),
                format!("failed to write '{}': {e}", abs.display()),
            ))
        })?;
        written += 1;
    }

    // ------------------------------------------------------------------
    // 4. Delete local files absent from the target version (Force only).
    //    Only files tracked by the local walk are considered — ignored
    //    paths (e.g. node_modules/) are never touched.
    // ------------------------------------------------------------------
    let mut deleted: usize = 0;

    if matches!(mode, CheckoutMode::Force) {
        let mut to_delete: Vec<&str> = local_paths
            .iter()
            .filter(|p| !target_files.contains_key(p.as_str()))
            .map(|p| p.as_str())
            .collect();
        to_delete.sort(); // deterministic order

        for rel_path in to_delete {
            let abs = abs_path(target, rel_path);
            if abs.is_file() {
                std::fs::remove_file(&abs).map_err(|e| {
                    CasError::Io(std::io::Error::new(
                        e.kind(),
                        format!("failed to delete '{}': {e}", abs.display()),
                    ))
                })?;
                deleted += 1;
            }
        }
    }

    Ok(CheckoutReport {
        written,
        deleted,
        skipped_dirty,
    })
}
