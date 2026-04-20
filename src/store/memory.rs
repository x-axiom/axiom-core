use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};

use crate::error::{CasError, CasResult};
use crate::model::{
    ChunkHash, NodeEntry, Ref, RefKind, TreeNode, VersionId, VersionNode,
    hash_bytes, hash_children,
};
use super::traits::{ChunkStore, TreeStore, NodeStore, VersionRepo, RefRepo, PathIndexRepo, PathEntry, SyncStore, ReachableObjects};

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

// ---------------------------------------------------------------------------
// InMemoryPathIndex
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryPathIndex {
    /// (version_id_str, path) -> PathEntry
    entries: RwLock<HashMap<(String, String), PathEntry>>,
}

impl InMemoryPathIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PathIndexRepo for InMemoryPathIndex {
    fn put_path_entry(
        &self,
        version_id: &VersionId,
        path: &str,
        node_hash: &ChunkHash,
        is_directory: bool,
    ) -> CasResult<()> {
        let key = (version_id.as_str().to_string(), path.to_string());
        self.entries.write().insert(
            key,
            PathEntry {
                path: path.to_string(),
                node_hash: node_hash.clone(),
                is_directory,
            },
        );
        Ok(())
    }

    fn get_by_path(
        &self,
        version_id: &VersionId,
        path: &str,
    ) -> CasResult<Option<PathEntry>> {
        let key = (version_id.as_str().to_string(), path.to_string());
        Ok(self.entries.read().get(&key).cloned())
    }

    fn list_directory(
        &self,
        version_id: &VersionId,
        dir_path: &str,
    ) -> CasResult<Vec<PathEntry>> {
        let vid = version_id.as_str().to_string();
        let prefix = if dir_path.is_empty() {
            String::new()
        } else {
            format!("{dir_path}/")
        };
        let guard = self.entries.read();
        let results: Vec<PathEntry> = guard
            .iter()
            .filter(|((v, p), _)| {
                v == &vid
                    && if prefix.is_empty() {
                        !p.contains('/')
                    } else {
                        p.starts_with(&prefix)
                            && !p[prefix.len()..].contains('/')
                    }
            })
            .map(|(_, entry)| entry.clone())
            .collect();
        Ok(results)
    }
}

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

// ---------------------------------------------------------------------------
// InMemorySyncStore
// ---------------------------------------------------------------------------

use std::sync::Arc;
use crate::model::{NodeKind, TreeNodeKind};

/// In-memory implementation of `SyncStore` that walks the object graph
/// through the individual store traits.
pub struct InMemorySyncStore {
    pub versions: Arc<InMemoryVersionRepo>,
    pub trees: Arc<InMemoryTreeStore>,
    pub nodes: Arc<InMemoryNodeStore>,
}

impl InMemorySyncStore {
    pub fn new(
        versions: Arc<InMemoryVersionRepo>,
        trees: Arc<InMemoryTreeStore>,
        nodes: Arc<InMemoryNodeStore>,
    ) -> Self {
        Self { versions, trees, nodes }
    }
}

impl SyncStore for InMemorySyncStore {
    fn collect_reachable_objects(&self, roots: &[VersionId]) -> CasResult<ReachableObjects> {
        let mut result = ReachableObjects::default();

        // BFS over version DAG
        let mut version_queue: VecDeque<VersionId> = roots.iter().cloned().collect();
        while let Some(vid) = version_queue.pop_front() {
            if !result.versions.insert(vid.clone()) {
                continue;
            }
            if let Some(version) = self.versions.get_version(&vid)? {
                // Enqueue parents
                for parent in &version.parents {
                    if !result.versions.contains(parent) {
                        version_queue.push_back(parent.clone());
                    }
                }
                // Walk the directory tree from root node
                let mut node_queue: VecDeque<ChunkHash> = VecDeque::new();
                node_queue.push_back(version.root.clone());

                while let Some(node_hash) = node_queue.pop_front() {
                    if !result.node_hashes.insert(node_hash.clone()) {
                        continue;
                    }
                    if let Some(entry) = self.nodes.get_node(&node_hash)? {
                        match &entry.kind {
                            NodeKind::File { root, .. } => {
                                // Walk merkle tree
                                let mut tree_queue: VecDeque<ChunkHash> = VecDeque::new();
                                tree_queue.push_back(root.clone());
                                while let Some(th) = tree_queue.pop_front() {
                                    if !result.tree_hashes.insert(th.clone()) {
                                        continue;
                                    }
                                    if let Some(tree_node) = self.trees.get_tree_node(&th)? {
                                        match &tree_node.kind {
                                            TreeNodeKind::Leaf { chunk } => {
                                                result.chunk_hashes.insert(chunk.clone());
                                            }
                                            TreeNodeKind::Internal { children } => {
                                                for child in children {
                                                    if !result.tree_hashes.contains(child) {
                                                        tree_queue.push_back(child.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            NodeKind::Directory { children } => {
                                for child_hash in children.values() {
                                    if !result.node_hashes.contains(child_hash) {
                                        node_queue.push_back(child_hash.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    fn list_all_version_ids(&self) -> CasResult<Vec<VersionId>> {
        Ok(self.versions.versions.read().keys().cloned().collect())
    }
}
