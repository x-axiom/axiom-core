use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::hash::ChunkHash;

/// The kind of a node in the directory tree.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NodeKind {
    /// A file, pointing to its Merkle Tree root hash.
    File {
        root: ChunkHash,
        size: u64,
    },
    /// A directory, mapping child names to their NodeEntry hashes.
    Directory {
        children: BTreeMap<String, ChunkHash>,
    },
}

/// An entry in the directory tree. Each entry is content-addressed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeEntry {
    pub hash: ChunkHash,
    pub kind: NodeKind,
}

impl NodeEntry {
    pub fn is_file(&self) -> bool {
        matches!(self.kind, NodeKind::File { .. })
    }

    pub fn is_directory(&self) -> bool {
        matches!(self.kind, NodeKind::Directory { .. })
    }
}
