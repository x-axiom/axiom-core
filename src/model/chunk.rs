use serde::{Deserialize, Serialize};

use super::hash::ChunkHash;

/// A content-addressed chunk of raw bytes.
#[derive(Clone, Debug)]
pub struct ChunkRecord {
    pub hash: ChunkHash,
    pub data: Vec<u8>,
    pub size: u64,
}

/// Descriptor produced by a chunker, before data is persisted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkDescriptor {
    pub offset: u64,
    pub length: u32,
    pub hash: ChunkHash,
}
