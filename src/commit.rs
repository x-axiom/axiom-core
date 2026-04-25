//! Version commit service and branch/tag ref management.
//!
//! This module provides a high-level `CommitService` that orchestrates
//! creating version records, managing branch and tag references, and
//! querying version history. It sits on top of the repository traits
//! (`VersionRepo`, `RefRepo`) without depending on concrete storage.
//!
//! It also exports [`commit_partial`], a free function for creating a new
//! version from a subset of locally modified files (used by E15-S06 stage /
//! commit flow in the desktop app).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::api::state::AppState;
use crate::chunker::{chunk_bytes, ChunkPolicy};
use crate::error::{CasError, CasResult};
use crate::merkle::{build_tree, DEFAULT_FAN_OUT};
use crate::model::hash::{current_timestamp, ChunkHash, VersionId};
use crate::model::refs::{Ref, RefKind};
use crate::model::version::VersionNode;
use crate::namespace::{build_directory_tree, FileInput};
use crate::store::traits::{RefRepo, VersionRepo};
use crate::working_tree::collect_files_from_tree;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default branch name used when no explicit branch is specified.
pub const DEFAULT_BRANCH: &str = "main";

// ---------------------------------------------------------------------------
// CommitRequest — input DTO for creating a commit
// ---------------------------------------------------------------------------

/// Input for creating a new version commit.
pub struct CommitRequest {
    /// Root hash of the directory tree for this version.
    pub root: ChunkHash,
    /// Parent version IDs. Empty for the initial commit.
    pub parents: Vec<VersionId>,
    /// Commit message.
    pub message: String,
    /// Optional metadata key-value pairs.
    pub metadata: HashMap<String, String>,
    /// Branch to advance. If `None`, uses `DEFAULT_BRANCH`.
    pub branch: Option<String>,
}

// ---------------------------------------------------------------------------
// CommitService
// ---------------------------------------------------------------------------

/// High-level service for version commits and ref management.
///
/// Uses trait objects for storage backends, consistent with `AppState`.
pub struct CommitService {
    versions: Arc<dyn VersionRepo>,
    refs: Arc<dyn RefRepo>,
}

impl CommitService {
    /// Create a new `CommitService` from repository implementations.
    pub fn new(versions: Arc<dyn VersionRepo>, refs: Arc<dyn RefRepo>) -> Self {
        Self { versions, refs }
    }

    // -----------------------------------------------------------------------
    // Commit
    // -----------------------------------------------------------------------

    /// Create a new version commit and advance the target branch.
    ///
    /// The version ID is deterministically derived from parents + root +
    /// timestamp so that identical inputs yield the same version.
    pub fn commit(&self, req: CommitRequest) -> CasResult<VersionNode> {
        let branch_name = req.branch.as_deref().unwrap_or(DEFAULT_BRANCH);
        let ts = current_timestamp();

        // Derive a deterministic version ID.
        let id = make_version_id(&req.parents, &req.root, ts);

        let version = VersionNode {
            id,
            parents: req.parents,
            root: req.root,
            message: req.message,
            timestamp: ts,
            metadata: req.metadata,
        };

        // Persist the version record.
        self.versions.put_version(&version)?;

        // Create or advance the branch ref.
        let branch_ref = Ref {
            name: branch_name.to_string(),
            kind: RefKind::Branch,
            target: version.id.clone(),
        };
        self.refs.put_ref(&branch_ref)?;

        Ok(version)
    }

    // -----------------------------------------------------------------------
    // Branch operations
    // -----------------------------------------------------------------------

    /// Create a new branch pointing to the given version.
    /// Fails if a branch (or tag) with the same name already exists.
    pub fn create_branch(&self, name: &str, target: &VersionId) -> CasResult<Ref> {
        if let Some(existing) = self.refs.get_ref(name)? {
            return Err(CasError::InvalidRef(format!(
                "ref '{}' already exists as {:?}",
                existing.name, existing.kind
            )));
        }
        let r = Ref {
            name: name.to_string(),
            kind: RefKind::Branch,
            target: target.clone(),
        };
        self.refs.put_ref(&r)?;
        Ok(r)
    }

    /// Update an existing branch to point to a new version.
    /// Fails if the ref does not exist or is not a branch.
    pub fn update_branch(&self, name: &str, target: &VersionId) -> CasResult<Ref> {
        let existing = self.refs.get_ref(name)?.ok_or_else(|| {
            CasError::NotFound(format!("branch '{name}' not found"))
        })?;
        if existing.kind != RefKind::Branch {
            return Err(CasError::InvalidRef(format!(
                "'{name}' is a tag, not a branch"
            )));
        }
        let r = Ref {
            name: name.to_string(),
            kind: RefKind::Branch,
            target: target.clone(),
        };
        self.refs.put_ref(&r)?;
        Ok(r)
    }

    /// List all branches.
    pub fn list_branches(&self) -> CasResult<Vec<Ref>> {
        self.refs.list_refs(Some(RefKind::Branch))
    }

    /// Resolve a branch name to its target version.
    pub fn resolve_branch(&self, name: &str) -> CasResult<Option<VersionNode>> {
        match self.refs.get_ref(name)? {
            Some(r) if r.kind == RefKind::Branch => self.versions.get_version(&r.target),
            _ => Ok(None),
        }
    }

    /// Delete a branch. Tags cannot be deleted through this method.
    pub fn delete_branch(&self, name: &str) -> CasResult<()> {
        let existing = self.refs.get_ref(name)?.ok_or_else(|| {
            CasError::NotFound(format!("branch '{name}' not found"))
        })?;
        if existing.kind != RefKind::Branch {
            return Err(CasError::InvalidRef(format!(
                "'{name}' is a tag, not a branch"
            )));
        }
        self.refs.delete_ref(name)
    }

    // -----------------------------------------------------------------------
    // Tag operations
    // -----------------------------------------------------------------------

    /// Create an immutable tag. Fails if a ref with the same name already exists.
    pub fn create_tag(&self, name: &str, target: &VersionId) -> CasResult<Ref> {
        if let Some(existing) = self.refs.get_ref(name)? {
            return Err(CasError::InvalidRef(format!(
                "ref '{}' already exists as {:?}",
                existing.name, existing.kind
            )));
        }
        let r = Ref {
            name: name.to_string(),
            kind: RefKind::Tag,
            target: target.clone(),
        };
        self.refs.put_ref(&r)?;
        Ok(r)
    }

    /// Resolve a tag name to its target version.
    pub fn resolve_tag(&self, name: &str) -> CasResult<Option<VersionNode>> {
        match self.refs.get_ref(name)? {
            Some(r) if r.kind == RefKind::Tag => self.versions.get_version(&r.target),
            _ => Ok(None),
        }
    }

    /// List all tags.
    pub fn list_tags(&self) -> CasResult<Vec<Ref>> {
        self.refs.list_refs(Some(RefKind::Tag))
    }

    // -----------------------------------------------------------------------
    // Ref resolution (unified)
    // -----------------------------------------------------------------------

    /// Resolve any ref (branch or tag) by name to its target version.
    pub fn resolve_ref(&self, name: &str) -> CasResult<Option<VersionNode>> {
        match self.refs.get_ref(name)? {
            Some(r) => self.versions.get_version(&r.target),
            None => Ok(None),
        }
    }

    /// Resolve `HEAD` — shorthand for resolving the default branch.
    pub fn resolve_head(&self) -> CasResult<Option<VersionNode>> {
        self.resolve_branch(DEFAULT_BRANCH)
    }

    // -----------------------------------------------------------------------
    // History
    // -----------------------------------------------------------------------

    /// Walk version history from the given version, following first-parent
    /// links. Returns up to `limit` versions in reverse chronological order.
    pub fn history(&self, from: &VersionId, limit: usize) -> CasResult<Vec<VersionNode>> {
        self.versions.list_history(from, limit)
    }

    /// Walk history from the tip of a branch.
    pub fn branch_history(&self, branch: &str, limit: usize) -> CasResult<Vec<VersionNode>> {
        let tip = self.refs.get_ref(branch)?.ok_or_else(|| {
            CasError::NotFound(format!("branch '{branch}' not found"))
        })?;
        self.versions.list_history(&tip.target, limit)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Derive a deterministic version ID from parents + root + timestamp.
fn make_version_id(parents: &[VersionId], root: &ChunkHash, timestamp: u64) -> VersionId {
    let mut hasher = blake3::Hasher::new();
    for p in parents {
        hasher.update(p.as_str().as_bytes());
    }
    hasher.update(root.as_bytes());
    hasher.update(&timestamp.to_le_bytes());
    let hash = hasher.finalize();
    VersionId(hash.to_hex().to_string())
}

// ---------------------------------------------------------------------------
// Partial commit (E15-S06)
// ---------------------------------------------------------------------------

/// Result of a [`commit_partial`] operation.
#[derive(Debug)]
pub struct PartialCommitResult {
    /// The newly created version.
    pub version: VersionNode,
    /// How many files were read and hashed from disk (staged).
    pub staged_files: usize,
    /// How many files came unchanged from HEAD (zero-copy).
    pub carried_files: usize,
}

/// Create a new version from a local working tree, committing only the files
/// whose workspace-relative paths appear in `staged_paths`.
///
/// Files not in `staged_paths` are carried over from `head_version` unchanged
/// (their tree hashes are reused — no re-reading or re-hashing of disk
/// content).  Files in `staged_paths` that are absent on disk are treated as
/// **deletions** and are not included in the new tree.
///
/// # Parameters
///
/// - `local_path` — absolute path to the bound local folder.
/// - `head_version` — the version to base the new commit on, or `None` for
///   an initial (parentless) commit.
/// - `staged_paths` — workspace-relative paths to include/update from disk.
///   Any path in this set that does not exist on disk is treated as deleted.
/// - `message` — commit message.
/// - `branch` — branch ref to advance.  Defaults to `"main"` when `None`.
/// - `state` — the `AppState` providing all storage backends.
///
/// # Errors
///
/// Returns an error if a staged file cannot be read, if the HEAD tree cannot
/// be walked, or if storage operations fail.
pub fn commit_partial(
    local_path: &Path,
    head_version: Option<&VersionId>,
    staged_paths: &[String],
    message: String,
    branch: Option<&str>,
    state: &AppState,
) -> CasResult<PartialCommitResult> {
    if staged_paths.is_empty() {
        return Err(CasError::InvalidObject(
            "staged_paths must not be empty".into(),
        ));
    }

    let branch_name = branch.unwrap_or(DEFAULT_BRANCH);
    let policy = ChunkPolicy::default();

    // ------------------------------------------------------------------
    // 1. Build the HEAD file map (reused for carry-over files).
    // ------------------------------------------------------------------
    let head_files: HashMap<String, (ChunkHash, u64)> = match head_version {
        None => HashMap::new(),
        Some(vid) => {
            let version = state
                .versions
                .get_version(vid)?
                .ok_or_else(|| CasError::NotFound(format!("version {vid} not found")))?;
            collect_files_from_tree(&version.root, "", state.nodes.as_ref())?
        }
    };

    // ------------------------------------------------------------------
    // 2. Assemble the full file list for the new version.
    //
    //    - Carried files: paths NOT in staged_paths, taken from HEAD as-is.
    //    - Staged files: paths IN staged_paths, read from disk, chunked,
    //      and Merkle-hashed.  Absent-on-disk paths are omitted (deleted).
    // ------------------------------------------------------------------
    let staged_set: std::collections::HashSet<&str> =
        staged_paths.iter().map(String::as_str).collect();

    // Start with all HEAD files that are NOT being staged (carry over).
    let mut file_inputs: Vec<FileInput> = head_files
        .iter()
        .filter(|(path, _)| !staged_set.contains(path.as_str()))
        .map(|(path, (root, size))| FileInput {
            path: path.clone(),
            root: *root,
            size: *size,
        })
        .collect();

    let carried_files = file_inputs.len();

    // Now process each staged path.
    let mut staged_files: usize = 0;
    for rel_path in staged_paths {
        let abs_path = local_path.join(rel_path);
        if !abs_path.exists() {
            // File deleted by user — omit from new tree.
            continue;
        }
        let data = std::fs::read(&abs_path).map_err(|e| {
            CasError::Io(std::io::Error::new(
                e.kind(),
                format!("failed to read '{}': {e}", abs_path.display()),
            ))
        })?;

        // Chunk and persist.
        let raw_chunks = chunk_bytes(&data, &policy);
        for (_, chunk_data, _, _) in &raw_chunks {
            state.chunks.put_chunk(chunk_data.clone())?;
        }
        let descriptors: Vec<_> = raw_chunks
            .into_iter()
            .enumerate()
            .map(|(order, (hash, _, offset, length))| {
                crate::model::chunk::ChunkDescriptor {
                    offset,
                    length,
                    hash,
                    order: order as u32,
                }
            })
            .collect();

        // Build Merkle tree.
        let file_obj = build_tree(&descriptors, DEFAULT_FAN_OUT, state.trees.as_ref())?;

        file_inputs.push(FileInput {
            path: rel_path.clone(),
            root: file_obj.root,
            size: file_obj.size,
        });
        staged_files += 1;
    }

    if file_inputs.is_empty() {
        return Err(CasError::InvalidObject(
            "resulting tree is empty — all staged files were deleted and no HEAD files remain"
                .into(),
        ));
    }

    // Sort to ensure deterministic tree structure.
    file_inputs.sort_by(|a, b| a.path.cmp(&b.path));

    // ------------------------------------------------------------------
    // 3. Build directory tree.
    // ------------------------------------------------------------------
    let tmp_vid = VersionId::from("__pending__");
    let root_entry = build_directory_tree(
        &file_inputs,
        &tmp_vid,
        state.nodes.as_ref(),
        state.path_index.as_ref(),
    )?;

    // ------------------------------------------------------------------
    // 4. Resolve parent version id.
    // ------------------------------------------------------------------
    let parents: Vec<VersionId> = match head_version {
        Some(vid) => vec![vid.clone()],
        None => {
            // No explicit head; fall back to current branch tip if it exists.
            match state.refs.get_ref(branch_name)? {
                Some(r) => vec![r.target],
                None => vec![],
            }
        }
    };

    // ------------------------------------------------------------------
    // 5. Commit.
    // ------------------------------------------------------------------
    let svc = CommitService::new(state.versions.clone(), state.refs.clone());
    let version = svc.commit(CommitRequest {
        root: root_entry.hash,
        parents,
        message,
        metadata: HashMap::new(),
        branch: Some(branch_name.to_string()),
    })?;

    // ------------------------------------------------------------------
    // 6. Re-index paths under the real version id.
    // ------------------------------------------------------------------
    build_directory_tree(
        &file_inputs,
        &version.id,
        state.nodes.as_ref(),
        state.path_index.as_ref(),
    )?;

    Ok(PartialCommitResult {
        version,
        staged_files,
        carried_files,
    })
}
