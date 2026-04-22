//! Integration tests for LocalSyncStore (E03-S04).
//!
//! Exercises: upload files → create version → collect_reachable_objects →
//! verify integrity.

#![cfg(feature = "local")]

use std::collections::HashMap;
use std::sync::Arc;

use axiom_core::commit::{CommitRequest, CommitService};
use axiom_core::model::hash::{hash_bytes, hash_children};
use axiom_core::model::{ChunkHash, NodeEntry, NodeKind, TreeNode, TreeNodeKind, VersionId};
use axiom_core::store::traits::{ChunkStore, NodeStore, SyncStore, TreeStore, VersionRepo};
use axiom_core::store::{LocalSyncStore, RocksDbCasStore, SqliteMetadataStore};

// ─── Test fixture ────────────────────────────────────────────────────────────

struct Env {
    cas: Arc<RocksDbCasStore>,
    meta: Arc<SqliteMetadataStore>,
    sync: LocalSyncStore,
    _tmp: tempfile::TempDir,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let cas = Arc::new(RocksDbCasStore::open(tmp.path().join("cas")).unwrap());
        let meta = Arc::new(SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap());
        let sync = LocalSyncStore::new(cas.clone(), meta.clone());
        Self { cas, meta, sync, _tmp: tmp }
    }

    /// Store a raw chunk, return its hash.
    fn put_chunk(&self, data: &[u8]) -> ChunkHash {
        self.cas.put_chunk(data.to_vec()).unwrap()
    }

    /// Create a leaf TreeNode for a chunk, return tree hash.
    fn put_leaf_tree(&self, chunk: ChunkHash) -> ChunkHash {
        let node = TreeNode {
            hash: hash_bytes(chunk.as_bytes()),
            kind: TreeNodeKind::Leaf { chunk },
        };
        self.cas.put_tree_node(&node).unwrap();
        node.hash
    }

    /// Create a file NodeEntry, return its hash.
    fn put_file_node(&self, content: &[u8]) -> ChunkHash {
        let chunk = self.put_chunk(content);
        let tree = self.put_leaf_tree(chunk);
        let node = NodeEntry {
            hash: hash_bytes(tree.as_bytes()),
            kind: NodeKind::File { root: tree, size: content.len() as u64 },
        };
        self.cas.put_node(&node).unwrap();
        node.hash
    }

    /// Commit a version with the given root node hash, return VersionId.
    fn commit(&self, root: ChunkHash, parents: Vec<VersionId>, msg: &str) -> VersionId {
        let svc = CommitService::new(self.meta.clone(), self.meta.clone());
        svc.commit(CommitRequest {
            root,
            parents,
            message: msg.to_string(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap()
        .id
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[test]
fn collect_single_version_finds_all_objects() {
    let env = Env::new();

    let file_node = env.put_file_node(b"hello, axiom!");
    let vid = env.commit(file_node, vec![], "v1");

    let result = env.sync.collect_reachable_objects(&[vid.clone()]).unwrap();

    assert!(result.versions.contains(&vid), "version must be collected");
    assert_eq!(result.node_hashes.len(), 1, "one node entry");
    assert_eq!(result.tree_hashes.len(), 1, "one tree node");
    assert_eq!(result.chunk_hashes.len(), 1, "one chunk");
}

#[test]
fn list_all_version_ids_returns_committed_versions() {
    let env = Env::new();

    let root1 = env.put_file_node(b"v1 content");
    let root2 = env.put_file_node(b"v2 content");
    let v1 = env.commit(root1, vec![], "v1");
    let v2 = env.commit(root2, vec![v1.clone()], "v2");

    let all_ids = env.sync.list_all_version_ids().unwrap();
    let id_set: std::collections::HashSet<_> = all_ids.into_iter().collect();

    assert!(id_set.contains(&v1));
    assert!(id_set.contains(&v2));
}

#[test]
fn collect_linear_chain_traverses_all_parents() {
    let env = Env::new();

    let r1 = env.put_file_node(b"rev1");
    let r2 = env.put_file_node(b"rev2");
    let r3 = env.put_file_node(b"rev3");

    let v1 = env.commit(r1, vec![], "v1");
    let v2 = env.commit(r2, vec![v1.clone()], "v2");
    let v3 = env.commit(r3, vec![v2.clone()], "v3");

    let result = env.sync.collect_reachable_objects(&[v3.clone()]).unwrap();

    assert_eq!(result.versions.len(), 3, "all 3 versions");
    assert_eq!(result.chunk_hashes.len(), 3, "3 distinct chunks");
}

#[test]
fn collect_deduplicates_shared_objects() {
    let env = Env::new();

    // Two versions share the same file node / chunk.
    let shared_node = env.put_file_node(b"shared data");
    let v1 = env.commit(shared_node, vec![], "v1");
    let v2 = env.commit(shared_node, vec![v1.clone()], "v2");

    let result = env.sync.collect_reachable_objects(&[v2]).unwrap();

    // Only one unique chunk, tree node, and file node even though both
    // versions reference the same root.
    assert_eq!(result.chunk_hashes.len(), 1);
    assert_eq!(result.tree_hashes.len(), 1);
    assert_eq!(result.node_hashes.len(), 1);
}

#[test]
fn collect_multi_file_root_collects_all_chunks() {
    let env = Env::new();

    let f1 = env.put_file_node(b"file A");
    let f2 = env.put_file_node(b"file B");
    let f3 = env.put_file_node(b"file C");

    // Build a directory NodeEntry containing three files.
    let mut children = std::collections::BTreeMap::new();
    children.insert("a.txt".to_string(), f1);
    children.insert("b.txt".to_string(), f2);
    children.insert("c.txt".to_string(), f3);

    // Deterministic hash for the directory node.
    let mut h = blake3::Hasher::new();
    for (k, v) in &children {
        h.update(k.as_bytes());
        h.update(v.as_bytes());
    }
    let dir_node = NodeEntry { hash: h.finalize(), kind: NodeKind::Directory { children } };
    env.cas.put_node(&dir_node).unwrap();

    let vid = env.commit(dir_node.hash, vec![], "dir-commit");

    let result = env.sync.collect_reachable_objects(&[vid]).unwrap();

    // 1 dir node + 3 file nodes = 4 node entries
    assert_eq!(result.node_hashes.len(), 4);
    // 3 tree nodes (one leaf per file)
    assert_eq!(result.tree_hashes.len(), 3);
    // 3 chunks
    assert_eq!(result.chunk_hashes.len(), 3);
}

#[test]
fn collect_empty_roots_returns_empty() {
    let env = Env::new();
    let result = env.sync.collect_reachable_objects(&[]).unwrap();
    assert!(result.versions.is_empty());
    assert!(result.chunk_hashes.is_empty());
}

#[test]
fn list_all_version_ids_empty_store_returns_empty() {
    let env = Env::new();
    let ids = env.sync.list_all_version_ids().unwrap();
    assert!(ids.is_empty());
}

#[test]
fn collect_internal_tree_node_collects_all_leaves() {
    let env = Env::new();

    // Build a 2-level Merkle tree: internal node → 2 leaf nodes.
    let chunk_a = env.put_chunk(b"chunk-a");
    let chunk_b = env.put_chunk(b"chunk-b");

    let leaf_a = TreeNode { hash: hash_bytes(chunk_a.as_bytes()), kind: TreeNodeKind::Leaf { chunk: chunk_a } };
    let leaf_b = TreeNode { hash: hash_bytes(chunk_b.as_bytes()), kind: TreeNodeKind::Leaf { chunk: chunk_b } };
    env.cas.put_tree_node(&leaf_a).unwrap();
    env.cas.put_tree_node(&leaf_b).unwrap();

    let internal_hash = hash_children(&[leaf_a.hash, leaf_b.hash]);
    let internal = TreeNode {
        hash: internal_hash,
        kind: TreeNodeKind::Internal { children: vec![leaf_a.hash, leaf_b.hash] },
    };
    env.cas.put_tree_node(&internal).unwrap();

    let file_node = NodeEntry {
        hash: hash_bytes(internal_hash.as_bytes()),
        kind: NodeKind::File { root: internal_hash, size: 16 },
    };
    env.cas.put_node(&file_node).unwrap();

    let vid = env.commit(file_node.hash, vec![], "multi-chunk");
    let result = env.sync.collect_reachable_objects(&[vid]).unwrap();

    assert_eq!(result.chunk_hashes.len(), 2, "both leaf chunks");
    assert_eq!(result.tree_hashes.len(), 3, "internal + 2 leaf tree nodes");
}
