use crate::error::CasResult;
use crate::model::{
    ChunkHash, NodeEntry, Ref, RefKind, TreeNode, VersionId, VersionNode,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Metadata about a path entry in a specific version.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PathEntry {
    /// The path relative to the version root (e.g. "src/main.rs").
    pub path: String,
    /// Content hash of the node at this path.
    pub node_hash: ChunkHash,
    /// Whether this path is a directory (true) or file (false).
    pub is_directory: bool,
}

/// Content-addressed storage for raw chunks.
pub trait ChunkStore: Send + Sync {
    /// Store a chunk and return its content hash. Idempotent.
    fn put_chunk(&self, data: Vec<u8>) -> CasResult<ChunkHash>;
    /// Retrieve chunk data by hash.
    fn get_chunk(&self, hash: &ChunkHash) -> CasResult<Option<Vec<u8>>>;
    /// Check whether a chunk exists.
    fn has_chunk(&self, hash: &ChunkHash) -> CasResult<bool>;

    /// Store multiple chunks and return their content hashes. Idempotent.
    /// Default implementation calls `put_chunk` for each item.
    fn put_chunks(&self, chunks: Vec<Vec<u8>>) -> CasResult<Vec<ChunkHash>> {
        chunks.into_iter().map(|data| self.put_chunk(data)).collect()
    }

    /// Retrieve multiple chunks by hash.
    /// Default implementation calls `get_chunk` for each hash.
    fn get_chunks(&self, hashes: &[ChunkHash]) -> CasResult<Vec<Option<Vec<u8>>>> {
        hashes.iter().map(|h| self.get_chunk(h)).collect()
    }

    /// Check whether multiple chunks exist.
    /// Default implementation calls `has_chunk` for each hash.
    fn has_chunks(&self, hashes: &[ChunkHash]) -> CasResult<Vec<bool>> {
        hashes.iter().map(|h| self.has_chunk(h)).collect()
    }
}

/// Storage for Merkle Tree nodes (file-level tree structure).
pub trait TreeStore: Send + Sync {
    /// Store a tree node. Idempotent (keyed by hash).
    fn put_tree_node(&self, node: &TreeNode) -> CasResult<()>;
    /// Retrieve a tree node by hash.
    fn get_tree_node(&self, hash: &ChunkHash) -> CasResult<Option<TreeNode>>;
}

/// Storage for directory tree nodes (file/directory namespace).
pub trait NodeStore: Send + Sync {
    /// Store a node entry. Idempotent (keyed by hash).
    fn put_node(&self, entry: &NodeEntry) -> CasResult<()>;
    /// Retrieve a node entry by hash.
    fn get_node(&self, hash: &ChunkHash) -> CasResult<Option<NodeEntry>>;
}

/// Repository for version DAG nodes.
pub trait VersionRepo: Send + Sync {
    /// Insert a new version node.
    fn put_version(&self, version: &VersionNode) -> CasResult<()>;
    /// Get a version by id.
    fn get_version(&self, id: &VersionId) -> CasResult<Option<VersionNode>>;
    /// List version history starting from a given version, walking parents.
    /// Returns up to `limit` versions in reverse chronological order.
    fn list_history(&self, from: &VersionId, limit: usize) -> CasResult<Vec<VersionNode>>;
}

/// Repository for branch and tag refs.
pub trait RefRepo: Send + Sync {
    /// Create or update a ref. For tags, fails if already exists unless force=true.
    fn put_ref(&self, r: &Ref) -> CasResult<()>;
    /// Get a ref by name.
    fn get_ref(&self, name: &str) -> CasResult<Option<Ref>>;
    /// Delete a ref by name.
    fn delete_ref(&self, name: &str) -> CasResult<()>;
    /// List all refs, optionally filtered by kind.
    fn list_refs(&self, kind: Option<RefKind>) -> CasResult<Vec<Ref>>;

    /// Atomically compare-and-swap a ref's target.
    ///
    /// Reads the current ref named `name`:
    /// - If `expected_old` is `None`, requires the ref to **not exist**.
    /// - If `expected_old` is `Some(v)`, requires the current target to equal `v`.
    ///
    /// When the condition holds, writes `new_ref` and returns `Ok(true)`.
    /// When the condition is **not** met, leaves the store unchanged and returns
    /// `Ok(false)`.
    ///
    /// The default implementation is a **non-atomic** read-check-write, which
    /// is safe for single-process use (SQLite/in-memory).  The FoundationDB
    /// backend overrides this with a genuinely atomic FDB transaction, making
    /// it safe under concurrent writers.
    fn compare_and_swap_ref(
        &self,
        name: &str,
        expected_old: Option<&VersionId>,
        new_ref: &Ref,
    ) -> CasResult<bool> {
        let current = self.get_ref(name)?;
        let matches = match (current.as_ref(), expected_old) {
            (None, None) => true,
            (Some(r), Some(expected)) => r.target == *expected,
            _ => false,
        };
        if matches {
            self.put_ref(new_ref)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Repository for path-based metadata indexing per version.
pub trait PathIndexRepo: Send + Sync {
    /// Index a path entry for a given version.
    fn put_path_entry(
        &self,
        version_id: &VersionId,
        path: &str,
        node_hash: &ChunkHash,
        is_directory: bool,
    ) -> CasResult<()>;

    /// Look up a node by version and path.
    fn get_by_path(
        &self,
        version_id: &VersionId,
        path: &str,
    ) -> CasResult<Option<PathEntry>>;

    /// List immediate children under a directory path for a given version.
    fn list_directory(
        &self,
        version_id: &VersionId,
        dir_path: &str,
    ) -> CasResult<Vec<PathEntry>>;
}

/// Set of all objects reachable from a given set of version roots.
/// Used by the sync protocol to determine what needs to be transferred.
#[derive(Clone, Debug, Default)]
pub struct ReachableObjects {
    pub versions: HashSet<VersionId>,
    pub tree_hashes: HashSet<ChunkHash>,
    pub node_hashes: HashSet<ChunkHash>,
    pub chunk_hashes: HashSet<ChunkHash>,
}

/// Store operations needed for the sync protocol.
pub trait SyncStore: Send + Sync {
    /// Walk the object graph starting from every id in `want`, stopping
    /// whenever a version in `have` is reached. The result contains every
    /// reachable object that is **not** transitively covered by `have`.
    ///
    /// Servers use this with a non-empty `have` set (the client's "I already
    /// have these tips" boundary) so the response only contains the diff;
    /// callers that want the full reachable closure should pass an empty
    /// `have` set or use [`collect_reachable_objects`].
    fn collect_reachable_with_have(
        &self,
        want: &[VersionId],
        have: &HashSet<VersionId>,
    ) -> CasResult<ReachableObjects>;

    /// Walk the object graph starting from `roots`, collecting all reachable
    /// version, tree, node, and chunk hashes (no `have` boundary).
    fn collect_reachable_objects(&self, roots: &[VersionId]) -> CasResult<ReachableObjects> {
        self.collect_reachable_with_have(roots, &HashSet::new())
    }

    /// Return every version id known to this store.
    fn list_all_version_ids(&self) -> CasResult<Vec<VersionId>>;
}

/// A workspace record. Workspaces are logical containers used by the SaaS
/// product for multi-tenant isolation; in Phase 0 (single-user local mode)
/// only the seeded `default` workspace is meaningful.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub created_at: u64,
    /// Free-form JSON metadata (e.g. description). Stored as a string so the
    /// store layer does not need to depend on a JSON type.
    pub metadata: String,
    /// Absolute path to the bound local folder (E15-S01). `None` when no
    /// folder has been bound yet.
    pub local_path: Option<String>,
    /// Name of the ref currently checked out in the local folder (e.g.
    /// `"main"`). `None` when no folder is bound or after a detached checkout.
    pub current_ref: Option<String>,
    /// Hex-encoded `VersionId` of the last commit / checkout written to
    /// `local_path`. Used as the comparison base for `compute_status`.
    pub current_version: Option<String>,
    /// Unix timestamp (seconds) when this workspace was soft-deleted.
    /// `None` means the workspace is active.
    pub deleted_at: Option<u64>,
}

/// Repository for workspace records.
pub trait WorkspaceRepo: Send + Sync {
    /// Insert a workspace. Fails if the id already exists.
    fn create_workspace(&self, ws: &Workspace) -> CasResult<()>;
    /// Get a workspace by id (regardless of soft-delete status).
    fn get_workspace(&self, id: &str) -> CasResult<Option<Workspace>>;
    /// List active (non-deleted) workspaces ordered by `created_at` ascending.
    fn list_workspaces(&self) -> CasResult<Vec<Workspace>>;
    /// Permanently delete a workspace by id. No-op if it does not exist.
    fn delete_workspace(&self, id: &str) -> CasResult<()>;
    /// Update mutable fields of an existing workspace (name, metadata,
    /// local_path, current_ref, current_version). No-op if id not found.
    fn update_workspace(&self, ws: &Workspace) -> CasResult<()>;
    /// Mark a workspace as soft-deleted by recording `deleted_at`.
    fn soft_delete_workspace(&self, id: &str, deleted_at: u64) -> CasResult<()>;
    /// Clear `deleted_at`, restoring a soft-deleted workspace to active.
    fn restore_workspace(&self, id: &str) -> CasResult<()>;
    /// List soft-deleted workspaces ordered by `deleted_at` ascending.
    fn list_deleted_workspaces(&self) -> CasResult<Vec<Workspace>>;
}

// ---------------------------------------------------------------------------
// Arc blanket impls — allows sharing a single store across multiple services
// ---------------------------------------------------------------------------

impl<T: VersionRepo + ?Sized> VersionRepo for Arc<T> {
    fn put_version(&self, version: &VersionNode) -> CasResult<()> {
        (**self).put_version(version)
    }
    fn get_version(&self, id: &VersionId) -> CasResult<Option<VersionNode>> {
        (**self).get_version(id)
    }
    fn list_history(&self, from: &VersionId, limit: usize) -> CasResult<Vec<VersionNode>> {
        (**self).list_history(from, limit)
    }
}

impl<T: RefRepo + ?Sized> RefRepo for Arc<T> {
    fn put_ref(&self, r: &Ref) -> CasResult<()> {
        (**self).put_ref(r)
    }
    fn get_ref(&self, name: &str) -> CasResult<Option<Ref>> {
        (**self).get_ref(name)
    }
    fn delete_ref(&self, name: &str) -> CasResult<()> {
        (**self).delete_ref(name)
    }
    fn list_refs(&self, kind: Option<RefKind>) -> CasResult<Vec<Ref>> {
        (**self).list_refs(kind)
    }
    fn compare_and_swap_ref(
        &self,
        name: &str,
        expected_old: Option<&VersionId>,
        new_ref: &Ref,
    ) -> CasResult<bool> {
        (**self).compare_and_swap_ref(name, expected_old, new_ref)
    }
}

impl<T: WorkspaceRepo + ?Sized> WorkspaceRepo for Arc<T> {
    fn create_workspace(&self, ws: &Workspace) -> CasResult<()> {
        (**self).create_workspace(ws)
    }
    fn get_workspace(&self, id: &str) -> CasResult<Option<Workspace>> {
        (**self).get_workspace(id)
    }
    fn list_workspaces(&self) -> CasResult<Vec<Workspace>> {
        (**self).list_workspaces()
    }
    fn delete_workspace(&self, id: &str) -> CasResult<()> {
        (**self).delete_workspace(id)
    }
    fn update_workspace(&self, ws: &Workspace) -> CasResult<()> {
        (**self).update_workspace(ws)
    }
    fn soft_delete_workspace(&self, id: &str, deleted_at: u64) -> CasResult<()> {
        (**self).soft_delete_workspace(id, deleted_at)
    }
    fn restore_workspace(&self, id: &str) -> CasResult<()> {
        (**self).restore_workspace(id)
    }
    fn list_deleted_workspaces(&self) -> CasResult<Vec<Workspace>> {
        (**self).list_deleted_workspaces()
    }
}

impl<T: SyncStore + ?Sized> SyncStore for Arc<T> {
    fn collect_reachable_with_have(
        &self,
        want: &[VersionId],
        have: &HashSet<VersionId>,
    ) -> CasResult<ReachableObjects> {
        (**self).collect_reachable_with_have(want, have)
    }
    fn collect_reachable_objects(&self, roots: &[VersionId]) -> CasResult<ReachableObjects> {
        (**self).collect_reachable_objects(roots)
    }
    fn list_all_version_ids(&self) -> CasResult<Vec<VersionId>> {
        (**self).list_all_version_ids()
    }
}

/// A configured remote endpoint for sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Remote {
    /// Unique short name (e.g. "origin").
    pub name: String,
    /// gRPC or HTTP endpoint URL.
    pub url: String,
    /// Bearer token or empty string when unauthenticated.
    pub auth_token: String,
    /// Optional SaaS tenant identifier.
    pub tenant_id: Option<String>,
    /// Optional remote workspace identifier to sync with.
    pub workspace_id: Option<String>,
    /// Unix timestamp (seconds) when this remote was added.
    pub created_at: u64,
}

/// CRUD repository for remote configurations.
pub trait RemoteRepo: Send + Sync {
    /// Add a remote. Returns `CasError::AlreadyExists` if name is taken.
    fn add_remote(&self, remote: &Remote) -> CasResult<()>;
    /// Update an existing remote in place.
    fn update_remote(&self, remote: &Remote) -> CasResult<()>;
    /// Remove a remote and cascade-delete its remote_refs and sync_sessions.
    fn remove_remote(&self, name: &str) -> CasResult<()>;
    /// Get a remote by name.
    fn get_remote(&self, name: &str) -> CasResult<Option<Remote>>;
    /// List all remotes ordered by created_at ascending.
    fn list_remotes(&self) -> CasResult<Vec<Remote>>;
}

impl<T: RemoteRepo + ?Sized> RemoteRepo for Arc<T> {
    fn add_remote(&self, remote: &Remote) -> CasResult<()> {
        (**self).add_remote(remote)
    }
    fn update_remote(&self, remote: &Remote) -> CasResult<()> {
        (**self).update_remote(remote)
    }
    fn remove_remote(&self, name: &str) -> CasResult<()> {
        (**self).remove_remote(name)
    }
    fn get_remote(&self, name: &str) -> CasResult<Option<Remote>> {
        (**self).get_remote(name)
    }
    fn list_remotes(&self) -> CasResult<Vec<Remote>> {
        (**self).list_remotes()
    }
}

/// Last-known state of a ref on a remote server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteRef {
    /// The remote this tracking ref belongs to (e.g. "origin").
    pub remote_name: String,
    /// The ref name (e.g. "main", "v1.0").
    pub ref_name: String,
    /// Branch or tag.
    pub kind: RefKind,
    /// The version this ref pointed to when last synced.
    pub target: VersionId,
    /// Unix timestamp (seconds) of the last update.
    pub updated_at: u64,
}

/// Ahead/behind counts comparing a local ref to a remote-tracking ref.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AheadBehind {
    /// Commits reachable from local HEAD but not from the remote-tracking ref.
    pub ahead: usize,
    /// Commits reachable from the remote-tracking ref but not from local HEAD.
    pub behind: usize,
}

/// Storage for remote-tracking refs (last-known remote state).
pub trait RemoteTrackingRepo: Send + Sync {
    /// Insert or replace a remote-tracking ref.
    fn update_remote_ref(&self, r: &RemoteRef) -> CasResult<()>;
    /// Get a remote-tracking ref by (remote_name, ref_name).
    fn get_remote_ref(&self, remote_name: &str, ref_name: &str) -> CasResult<Option<RemoteRef>>;
    /// List all remote-tracking refs for a given remote, ordered by ref_name.
    fn list_remote_refs(&self, remote_name: &str) -> CasResult<Vec<RemoteRef>>;
    /// Delete a single remote-tracking ref. No-op if it does not exist.
    fn delete_remote_ref(&self, remote_name: &str, ref_name: &str) -> CasResult<()>;
}

impl<T: RemoteTrackingRepo + ?Sized> RemoteTrackingRepo for Arc<T> {
    fn update_remote_ref(&self, r: &RemoteRef) -> CasResult<()> {
        (**self).update_remote_ref(r)
    }
    fn get_remote_ref(&self, remote_name: &str, ref_name: &str) -> CasResult<Option<RemoteRef>> {
        (**self).get_remote_ref(remote_name, ref_name)
    }
    fn list_remote_refs(&self, remote_name: &str) -> CasResult<Vec<RemoteRef>> {
        (**self).list_remote_refs(remote_name)
    }
    fn delete_remote_ref(&self, remote_name: &str, ref_name: &str) -> CasResult<()> {
        (**self).delete_remote_ref(remote_name, ref_name)
    }
}

// ─── Sync session log (E04-S06) ──────────────────────────────────────────────

/// Direction of a sync operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncDirection {
    Push,
    Pull,
}

impl SyncDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncDirection::Push => "push",
            SyncDirection::Pull => "pull",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "push" => Some(Self::Push),
            "pull" => Some(Self::Pull),
            _ => None,
        }
    }
}

/// Lifecycle status of a sync session.
///
/// Mirrors the `sync_sessions.status` CHECK constraint in the SQLite schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncSessionStatus {
    Running,
    Completed,
    Failed,
}

impl SyncSessionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncSessionStatus::Running => "running",
            SyncSessionStatus::Completed => "completed",
            SyncSessionStatus::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One row in the `sync_sessions` table.
#[derive(Clone, Debug)]
pub struct SyncSession {
    pub id: String,
    pub remote_name: String,
    pub direction: SyncDirection,
    pub status: SyncSessionStatus,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub objects_transferred: u64,
    pub bytes_transferred: u64,
    pub error_message: Option<String>,
}

/// Storage for sync session log entries.
pub trait SyncSessionRepo: Send + Sync {
    /// Insert a new session row (status defaults to `Running`).
    fn create_session(&self, session: &SyncSession) -> CasResult<()>;

    /// Update an existing session: status, finished_at, transferred counts,
    /// and error message.
    fn update_session(&self, session: &SyncSession) -> CasResult<()>;

    /// Look up a session by id.
    fn get_session(&self, id: &str) -> CasResult<Option<SyncSession>>;

    /// List recent sessions, optionally filtered by remote name.
    /// Most-recent first; `limit` caps the result count.
    fn list_sync_sessions(
        &self,
        remote: Option<&str>,
        limit: usize,
    ) -> CasResult<Vec<SyncSession>>;
}

impl<T: SyncSessionRepo + ?Sized> SyncSessionRepo for Arc<T> {
    fn create_session(&self, session: &SyncSession) -> CasResult<()> {
        (**self).create_session(session)
    }
    fn update_session(&self, session: &SyncSession) -> CasResult<()> {
        (**self).update_session(session)
    }
    fn get_session(&self, id: &str) -> CasResult<Option<SyncSession>> {
        (**self).get_session(id)
    }
    fn list_sync_sessions(
        &self,
        remote: Option<&str>,
        limit: usize,
    ) -> CasResult<Vec<SyncSession>> {
        (**self).list_sync_sessions(remote, limit)
    }
}

// ---------------------------------------------------------------------------
// ObjectManifestRepo (E05-S03) — per-session object transfer manifest
// ---------------------------------------------------------------------------

/// Persists which object hashes have been confirmed transferred for a given
/// sync session.
///
/// Used by push/pull clients to skip already-transferred objects when resuming
/// an interrupted session without re-transferring the full pack.
///
/// Hash values are stored as lowercase hex strings (64 chars for BLAKE3).
pub trait ObjectManifestRepo: Send + Sync {
    /// Append confirmed-transferred object hashes to the manifest for `session_id`.
    fn manifest_append(&self, session_id: &str, hash_hexes: &[String]) -> CasResult<()>;

    /// Load all recorded hash hex strings for `session_id`.
    fn manifest_load(&self, session_id: &str) -> CasResult<Vec<String>>;

    /// Delete all manifest entries for `session_id`.
    ///
    /// Called when a session completes or fails so no stale entries remain.
    fn manifest_delete(&self, session_id: &str) -> CasResult<()>;
}

impl<T: ObjectManifestRepo + ?Sized> ObjectManifestRepo for Arc<T> {
    fn manifest_append(&self, session_id: &str, hash_hexes: &[String]) -> CasResult<()> {
        (**self).manifest_append(session_id, hash_hexes)
    }
    fn manifest_load(&self, session_id: &str) -> CasResult<Vec<String>> {
        (**self).manifest_load(session_id)
    }
    fn manifest_delete(&self, session_id: &str) -> CasResult<()> {
        (**self).manifest_delete(session_id)
    }
}

// ---------------------------------------------------------------------------
// WtCacheRepo — working-tree hash cache (mtime + size fast-path)
// ---------------------------------------------------------------------------

/// A single entry in the working-tree hash cache.
#[derive(Clone, Debug)]
pub struct WtCacheEntry {
    /// Last-modified timestamp of the file in nanoseconds since the Unix epoch.
    pub mtime_ns: i64,
    /// File size in bytes at the time the hash was computed.
    pub size: u64,
    /// Merkle root hash of the file (hex string, 64 chars).
    pub hash_hex: String,
}

/// Cache of `(workspace_id, path) → (mtime_ns, size, hash)`.
///
/// `compute_status` uses this to skip re-hashing files whose `(mtime_ns,
/// size)` pair hasn't changed since the last status computation.
pub trait WtCacheRepo: Send + Sync {
    /// Look up a cached entry for the given workspace and workspace-relative
    /// path. Returns `None` if there is no cached entry.
    fn wt_cache_get(&self, workspace_id: &str, path: &str) -> CasResult<Option<WtCacheEntry>>;

    /// Insert or replace the cache entry for `(workspace_id, path)`.
    fn wt_cache_put(
        &self,
        workspace_id: &str,
        path: &str,
        entry: &WtCacheEntry,
    ) -> CasResult<()>;

    /// Delete all cache entries for `workspace_id`. Called when a workspace is
    /// deleted or its `local_path` changes.
    fn wt_cache_clear(&self, workspace_id: &str) -> CasResult<()>;
}

impl<T: WtCacheRepo + ?Sized> WtCacheRepo for Arc<T> {
    fn wt_cache_get(&self, workspace_id: &str, path: &str) -> CasResult<Option<WtCacheEntry>> {
        (**self).wt_cache_get(workspace_id, path)
    }
    fn wt_cache_put(
        &self,
        workspace_id: &str,
        path: &str,
        entry: &WtCacheEntry,
    ) -> CasResult<()> {
        (**self).wt_cache_put(workspace_id, path, entry)
    }
    fn wt_cache_clear(&self, workspace_id: &str) -> CasResult<()> {
        (**self).wt_cache_clear(workspace_id)
    }
}
