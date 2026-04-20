//! Version commit service and branch/tag ref management.
//!
//! This module provides a high-level `CommitService` that orchestrates
//! creating version records, managing branch and tag references, and
//! querying version history. It sits on top of the repository traits
//! (`VersionRepo`, `RefRepo`) without depending on concrete storage.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{CasError, CasResult};
use crate::model::hash::{current_timestamp, ChunkHash, VersionId};
use crate::model::refs::{Ref, RefKind};
use crate::model::version::VersionNode;
use crate::store::traits::{RefRepo, VersionRepo};

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
