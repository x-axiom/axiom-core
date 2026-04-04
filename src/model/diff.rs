use serde::{Deserialize, Serialize};

use super::hash::ChunkHash;

/// The kind of change detected in a diff.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffKind {
    Added,
    Removed,
    Modified,
}

/// A single entry in a directory-level diff.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiffEntry {
    pub path: String,
    pub kind: DiffKind,
    pub old_hash: Option<ChunkHash>,
    pub new_hash: Option<ChunkHash>,
}

/// The result of comparing two versions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiffResult {
    pub entries: Vec<DiffEntry>,
    pub added_files: usize,
    pub removed_files: usize,
    pub modified_files: usize,
    pub added_chunks: usize,
    pub removed_chunks: usize,
    pub unchanged_chunks: usize,
}

impl DiffResult {
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            added_files: 0,
            removed_files: 0,
            modified_files: 0,
            added_chunks: 0,
            removed_chunks: 0,
            unchanged_chunks: 0,
        }
    }
}
