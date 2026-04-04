use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::hash::{ChunkHash, VersionId};

/// An immutable version snapshot in the DAG.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionNode {
    /// Unique version identifier (BLAKE3 hex of parents + root + timestamp).
    pub id: VersionId,
    /// Parent version(s). Empty for initial commit, multiple for merge.
    pub parents: Vec<VersionId>,
    /// Root hash of the directory tree at this version.
    pub root: ChunkHash,
    /// Human-readable commit message.
    pub message: String,
    /// Unix timestamp in seconds.
    pub timestamp: u64,
    /// Optional key-value metadata.
    pub metadata: HashMap<String, String>,
}
