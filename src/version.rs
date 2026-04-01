use crate::cas::ChunkHash;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionNode {
    pub id: String,
    pub parents: Vec<String>,
    pub root: ChunkHash,
    pub message: String,
    pub timestamp: u64,
}

pub struct VersionStore {
    versions: RwLock<HashMap<String, VersionNode>>,
}

impl VersionStore {
    pub fn new() -> Self {
        Self {
            versions: RwLock::new(HashMap::new()),
        }
    }

    pub fn commit(&self, parents: Vec<String>, root: ChunkHash, message: String) -> String {
        let timestamp = current_ts();
        // let id = format!(
        //     "{:x?}",
        //     blake3::hash(format!("{:?}{:?}{:?}", &parents, root, timestamp).as_bytes()).to_hex()
        // );
        // id

        // Instead of using `format!` to concatenate a large string, updating data directly in the `Hasher` of `blake3` reduces one memory copy
        let mut hasher = blake3::Hasher::new();
        for parent in &parents {
            hasher.update(parent.as_bytes());
        }
        hasher.update(root.as_bytes());
        hasher.update(&timestamp.to_le_bytes());
        let id = hasher.finalize().to_hex().to_string();
        let node = VersionNode {
            id: id.clone(),
            parents,
            root,
            message,
            timestamp,
        };
        self.versions.write().insert(id.clone(), node);
        id
    }

    pub fn get(&self, id: &str) -> Option<VersionNode> {
        self.versions.read().get(id).cloned()
    }
}

fn current_ts() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_epoch.as_secs()
}
