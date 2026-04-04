use parking_lot::RwLock;
use std::collections::HashMap;

use crate::error::{CasError, CasResult};
use crate::model::{
    ChunkHash, NodeEntry, Ref, RefKind, TreeNode, VersionId, VersionNode,
    hash_bytes, hash_children,
};
use super::traits::{ChunkStore, TreeStore, NodeStore, VersionRepo, RefRepo};

// ---------------------------------------------------------------------------
// InMemoryChunkStore
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryChunkStore {
    chunks: RwLock<HashMap<ChunkHash, Vec<u8>>>,
}

impl InMemoryChunkStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ChunkStore for InMemoryChunkStore {
    fn put_chunk(&self, data: Vec<u8>) -> CasResult<ChunkHash> {
        let hash = hash_bytes(&data);
        self.chunks.write().entry(hash).or_insert(data);
        Ok(hash)
    }

    fn get_chunk(&self, hash: &ChunkHash) -> CasResult<Option<Vec<u8>>> {
        Ok(self.chunks.read().get(hash).cloned())
    }

    fn has_chunk(&self, hash: &ChunkHash) -> CasResult<bool> {
        Ok(self.chunks.read().contains_key(hash))
    }
}

// ---------------------------------------------------------------------------
// InMemoryTreeStore
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryTreeStore {
    nodes: RwLock<HashMap<ChunkHash, TreeNode>>,
}

impl InMemoryTreeStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TreeStore for InMemoryTreeStore {
    fn put_tree_node(&self, node: &TreeNode) -> CasResult<()> {
        self.nodes.write().entry(node.hash).or_insert_with(|| node.clone());
        Ok(())
    }

    fn get_tree_node(&self, hash: &ChunkHash) -> CasResult<Option<TreeNode>> {
        Ok(self.nodes.read().get(hash).cloned())
    }
}

// ---------------------------------------------------------------------------
// InMemoryNodeStore
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryNodeStore {
    entries: RwLock<HashMap<ChunkHash, NodeEntry>>,
}

impl InMemoryNodeStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl NodeStore for InMemoryNodeStore {
    fn put_node(&self, entry: &NodeEntry) -> CasResult<()> {
        self.entries.write().entry(entry.hash).or_insert_with(|| entry.clone());
        Ok(())
    }

    fn get_node(&self, hash: &ChunkHash) -> CasResult<Option<NodeEntry>> {
        Ok(self.entries.read().get(hash).cloned())
    }
}

// ---------------------------------------------------------------------------
// InMemoryVersionRepo
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryVersionRepo {
    versions: RwLock<HashMap<VersionId, VersionNode>>,
}

impl InMemoryVersionRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl VersionRepo for InMemoryVersionRepo {
    fn put_version(&self, version: &VersionNode) -> CasResult<()> {
        self.versions.write().insert(version.id.clone(), version.clone());
        Ok(())
    }

    fn get_version(&self, id: &VersionId) -> CasResult<Option<VersionNode>> {
        Ok(self.versions.read().get(id).cloned())
    }

    fn list_history(&self, from: &VersionId, limit: usize) -> CasResult<Vec<VersionNode>> {
        let versions = self.versions.read();
        let mut result = Vec::new();
        let mut current = Some(from.clone());

        while let Some(id) = current {
            if result.len() >= limit {
                break;
            }
            if let Some(node) = versions.get(&id) {
                result.push(node.clone());
                // Follow first parent for linear history.
                current = node.parents.first().cloned();
            } else {
                break;
            }
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// InMemoryRefRepo
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryRefRepo {
    refs: RwLock<HashMap<String, Ref>>,
}

impl InMemoryRefRepo {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RefRepo for InMemoryRefRepo {
    fn put_ref(&self, r: &Ref) -> CasResult<()> {
        let mut refs = self.refs.write();
        if let Some(existing) = refs.get(&r.name) {
            if existing.kind == RefKind::Tag {
                return Err(CasError::AlreadyExists);
            }
        }
        refs.insert(r.name.clone(), r.clone());
        Ok(())
    }

    fn get_ref(&self, name: &str) -> CasResult<Option<Ref>> {
        Ok(self.refs.read().get(name).cloned())
    }

    fn delete_ref(&self, name: &str) -> CasResult<()> {
        self.refs.write().remove(name);
        Ok(())
    }

    fn list_refs(&self, kind: Option<RefKind>) -> CasResult<Vec<Ref>> {
        let refs = self.refs.read();
        let result: Vec<Ref> = refs
            .values()
            .filter(|r| kind.as_ref().is_none_or(|k| &r.kind == k))
            .cloned()
            .collect();
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible combined store (bridges old CasStore API)
// ---------------------------------------------------------------------------

/// Combined in-memory store that provides chunk + simple object support,
/// compatible with the v0 API surface. Will be deprecated in favor of
/// separate stores.
#[derive(Default)]
pub struct InMemoryCas {
    chunks: RwLock<HashMap<ChunkHash, Vec<u8>>>,
    objects: RwLock<HashMap<ChunkHash, Vec<ChunkHash>>>,
}

impl InMemoryCas {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_chunk(&self, data: Vec<u8>) -> ChunkHash {
        let hash = hash_bytes(&data);
        self.chunks.write().entry(hash).or_insert(data);
        hash
    }

    pub fn get_chunk(&self, hash: &ChunkHash) -> Option<Vec<u8>> {
        self.chunks.read().get(hash).cloned()
    }

    pub fn put_object(&self, chunks: Vec<ChunkHash>) -> ChunkHash {
        let hash = hash_children(&chunks);
        self.objects.write().insert(hash, chunks);
        hash
    }

    pub fn get_object_chunks(&self, hash: &ChunkHash) -> Option<Vec<ChunkHash>> {
        self.objects.read().get(hash).cloned()
    }
}
