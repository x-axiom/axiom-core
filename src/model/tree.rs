use serde::{Deserialize, Serialize};

use super::hash::ChunkHash;

/// A node in a Merkle Tree that represents a file's chunk structure.
///
/// Leaf nodes reference raw chunks; internal nodes reference child TreeNodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TreeNode {
    pub hash: ChunkHash,
    pub kind: TreeNodeKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TreeNodeKind {
    /// Leaf node pointing to a raw data chunk.
    Leaf { chunk: ChunkHash },
    /// Internal node with ordered children (other TreeNode hashes).
    Internal { children: Vec<ChunkHash> },
}

/// A complete file object represented as a Merkle Tree root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileObject {
    /// Root hash of the Merkle Tree.
    pub root: ChunkHash,
    /// Total size of the file in bytes.
    pub size: u64,
    /// Number of leaf chunks.
    pub chunk_count: u32,
}
