use blake3::Hash;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Content-addressed hash used for chunks, tree nodes, and objects.
pub type ChunkHash = Hash;

/// Unique identifier for a version, stored as hex string of BLAKE3 hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VersionId(pub String);

impl VersionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for VersionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for VersionId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Compute BLAKE3 hash of raw bytes.
pub fn hash_bytes(data: &[u8]) -> ChunkHash {
    blake3::hash(data)
}

/// Compute BLAKE3 hash over a sequence of child hashes (for tree nodes).
pub fn hash_children(children: &[ChunkHash]) -> ChunkHash {
    let mut hasher = blake3::Hasher::new();
    for child in children {
        hasher.update(child.as_bytes());
    }
    hasher.finalize()
}

/// Get current unix timestamp in seconds.
pub fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
