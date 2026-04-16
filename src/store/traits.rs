use crate::error::CasResult;
use crate::model::{
    ChunkHash, NodeEntry, Ref, RefKind, TreeNode, VersionId, VersionNode,
};

/// Metadata about a path entry in a specific version.
#[derive(Clone, Debug)]
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
