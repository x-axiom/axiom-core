//! Integration tests for the reachable-object collection algorithm (E03-S03).

use std::collections::{BTreeMap, HashMap, HashSet};

use axiom_core::model::{
    ChunkHash, NodeEntry, NodeKind, TreeNode, TreeNodeKind, VersionId, VersionNode, hash_bytes,
};
use axiom_core::store::memory::{
    InMemoryChunkStore, InMemoryNodeStore, InMemoryTreeStore, InMemoryVersionRepo,
};
use axiom_core::store::traits::{ChunkStore, NodeStore, TreeStore, VersionRepo};
use axiom_core::sync::reachable::{CancelToken, collect_reachable};

// ─── Fixture helpers ─────────────────────────────────────────────────────────

struct Stores {
    chunks: InMemoryChunkStore,
    trees: InMemoryTreeStore,
    nodes: InMemoryNodeStore,
    versions: InMemoryVersionRepo,
}

impl Default for Stores {
    fn default() -> Self {
        Self {
            chunks: InMemoryChunkStore::new(),
            trees: InMemoryTreeStore::new(),
            nodes: InMemoryNodeStore::new(),
            versions: InMemoryVersionRepo::new(),
        }
    }
}

impl Stores {
    /// Create a chunk with deterministic content, return its hash.
    fn add_chunk(&self, content: &[u8]) -> ChunkHash {
        self.chunks.put_chunk(content.to_vec()).unwrap()
    }

    /// Create a leaf TreeNode for a chunk and return the TreeNode hash.
    fn add_leaf_tree(&self, chunk: ChunkHash) -> ChunkHash {
        let node = TreeNode {
            hash: hash_bytes(chunk.as_bytes()),
            kind: TreeNodeKind::Leaf { chunk },
        };
        self.trees.put_tree_node(&node).unwrap();
        node.hash
    }

    /// Create a file NodeEntry with a single-chunk tree; return the NodeEntry hash.
    fn add_file_node(&self, content: &[u8]) -> ChunkHash {
        let chunk_hash = self.add_chunk(content);
        let tree_hash = self.add_leaf_tree(chunk_hash);
        let node = NodeEntry {
            hash: hash_bytes(tree_hash.as_bytes()),
            kind: NodeKind::File { root: tree_hash, size: content.len() as u64 },
        };
        self.nodes.put_node(&node).unwrap();
        node.hash
    }

    /// Create a directory NodeEntry with the given children (name → NodeEntry hash).
    fn add_dir_node(&self, children: BTreeMap<String, ChunkHash>) -> ChunkHash {
        // stable hash: hash over sorted child hashes
        let mut h = blake3::Hasher::new();
        for (k, v) in &children {
            h.update(k.as_bytes());
            h.update(v.as_bytes());
        }
        let node = NodeEntry {
            hash: h.finalize(),
            kind: NodeKind::Directory { children },
        };
        self.nodes.put_node(&node).unwrap();
        node.hash
    }

    /// Create a version node and return its VersionId.
    fn add_version(
        &self,
        id: &str,
        parents: Vec<&str>,
        root: ChunkHash,
    ) -> VersionId {
        let vid = VersionId::from(id);
        let v = VersionNode {
            id: vid.clone(),
            parents: parents.into_iter().map(VersionId::from).collect(),
            root,
            message: id.to_string(),
            timestamp: 0,
            metadata: HashMap::new(),
        };
        self.versions.put_version(&v).unwrap();
        vid
    }
}

fn no_cancel() -> CancelToken {
    CancelToken::new()
}

fn id(s: &str) -> VersionId {
    VersionId::from(s)
}

fn ids(v: &[&str]) -> HashSet<VersionId> {
    v.iter().map(|s| id(s)).collect()
}

// ─── Empty repository ─────────────────────────────────────────────────────────

#[test]
fn empty_want_returns_empty() {
    let s = Stores::default();
    let result = collect_reachable(
        &[],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();
    assert!(result.versions.is_empty());
    assert!(result.chunk_hashes.is_empty());
}

#[test]
fn want_nonexistent_version_returns_empty_objects() {
    let s = Stores::default();
    let result = collect_reachable(
        &[id("ghost")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();
    // The version ID itself is collected (it's in want), but no objects.
    assert!(result.versions.contains(&id("ghost")));
    assert!(result.chunk_hashes.is_empty());
}

// ─── Linear chain ────────────────────────────────────────────────────────────

/// v1 → v2 → v3 (linear). Collect all when have={}.
#[test]
fn linear_chain_collects_all() {
    let s = Stores::default();
    let root1 = s.add_file_node(b"file-v1");
    let root2 = s.add_file_node(b"file-v2");
    let root3 = s.add_file_node(b"file-v3");

    s.add_version("v1", vec![], root1);
    s.add_version("v2", vec!["v1"], root2);
    s.add_version("v3", vec!["v2"], root3);

    let result = collect_reachable(
        &[id("v3")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    assert_eq!(result.versions, ids(&["v1", "v2", "v3"]));
    // 3 distinct chunks (one per version) + 3 tree nodes + 3 node entries
    assert_eq!(result.chunk_hashes.len(), 3);
    assert_eq!(result.tree_hashes.len(), 3);
    assert_eq!(result.node_hashes.len(), 3);
}

/// v1 → v2 → v3, have={v1}. Only v2 and v3 should be collected.
#[test]
fn linear_chain_with_have_cuts_at_boundary() {
    let s = Stores::default();
    let root1 = s.add_file_node(b"file-v1");
    let root2 = s.add_file_node(b"file-v2");
    let root3 = s.add_file_node(b"file-v3");

    s.add_version("v1", vec![], root1);
    s.add_version("v2", vec!["v1"], root2);
    s.add_version("v3", vec!["v2"], root3);

    let result = collect_reachable(
        &[id("v3")],
        &ids(&["v1"]),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    assert_eq!(result.versions, ids(&["v2", "v3"]));
    assert!(!result.versions.contains(&id("v1")));
    assert_eq!(result.chunk_hashes.len(), 2);
}

// ─── Forked history ───────────────────────────────────────────────────────────

/// v1 → v2a (branch A)
///    → v2b (branch B)
/// want=[v2a, v2b], have={}  → collect all 3
#[test]
fn forked_history_collects_all_branches() {
    let s = Stores::default();
    let root1 = s.add_file_node(b"base");
    let root2a = s.add_file_node(b"branch-a");
    let root2b = s.add_file_node(b"branch-b");

    s.add_version("v1", vec![], root1);
    s.add_version("v2a", vec!["v1"], root2a);
    s.add_version("v2b", vec!["v1"], root2b);

    let result = collect_reachable(
        &[id("v2a"), id("v2b")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    assert_eq!(result.versions, ids(&["v1", "v2a", "v2b"]));
    // v1's chunk is shared; only 3 unique chunks total
    assert_eq!(result.chunk_hashes.len(), 3);
}

/// Shared root is only collected once even if reached from two branches.
#[test]
fn shared_ancestor_deduplicated() {
    let s = Stores::default();
    let shared_chunk = s.add_chunk(b"shared");
    let tree = s.add_leaf_tree(shared_chunk);
    let root_hash = {
        let node = NodeEntry {
            hash: hash_bytes(tree.as_bytes()),
            kind: NodeKind::File { root: tree, size: 6 },
        };
        s.nodes.put_node(&node).unwrap();
        node.hash
    };

    s.add_version("v1", vec![], root_hash);
    s.add_version("v2a", vec!["v1"], root_hash); // same root
    s.add_version("v2b", vec!["v1"], root_hash); // same root

    let result = collect_reachable(
        &[id("v2a"), id("v2b")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    assert_eq!(result.versions, ids(&["v1", "v2a", "v2b"]));
    // All three reference the same single node/tree/chunk — no duplicates.
    assert_eq!(result.chunk_hashes.len(), 1);
    assert_eq!(result.tree_hashes.len(), 1);
    assert_eq!(result.node_hashes.len(), 1);
}

// ─── Merge commit ─────────────────────────────────────────────────────────────

/// v1 → v2a ─┐
///           └── v3 (merge)
/// v1 → v2b ─┘
#[test]
fn merge_commit_collects_all_parents() {
    let s = Stores::default();
    let r1 = s.add_file_node(b"v1");
    let r2a = s.add_file_node(b"v2a");
    let r2b = s.add_file_node(b"v2b");
    let r3 = s.add_file_node(b"v3-merged");

    s.add_version("v1", vec![], r1);
    s.add_version("v2a", vec!["v1"], r2a);
    s.add_version("v2b", vec!["v1"], r2b);
    s.add_version("v3", vec!["v2a", "v2b"], r3);

    let result = collect_reachable(
        &[id("v3")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    assert_eq!(result.versions, ids(&["v1", "v2a", "v2b", "v3"]));
    assert_eq!(result.chunk_hashes.len(), 4);
}

/// Merge commit with have={v2a, v2b}: only v3 itself is needed.
#[test]
fn merge_commit_with_have_both_parents() {
    let s = Stores::default();
    let r1 = s.add_file_node(b"v1");
    let r2a = s.add_file_node(b"v2a");
    let r2b = s.add_file_node(b"v2b");
    let r3 = s.add_file_node(b"v3-merged");

    s.add_version("v1", vec![], r1);
    s.add_version("v2a", vec!["v1"], r2a);
    s.add_version("v2b", vec!["v1"], r2b);
    s.add_version("v3", vec!["v2a", "v2b"], r3);

    let result = collect_reachable(
        &[id("v3")],
        &ids(&["v2a", "v2b"]),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    assert_eq!(result.versions, ids(&["v3"]));
    assert_eq!(result.chunk_hashes.len(), 1);
}

// ─── Directory tree ───────────────────────────────────────────────────────────

/// Version with a directory containing two files.
#[test]
fn directory_node_children_collected() {
    let s = Stores::default();
    let file_a = s.add_file_node(b"file-a");
    let file_b = s.add_file_node(b"file-b");
    let mut children = BTreeMap::new();
    children.insert("a.txt".to_string(), file_a);
    children.insert("b.txt".to_string(), file_b);
    let dir_root = s.add_dir_node(children);

    s.add_version("v1", vec![], dir_root);

    let result = collect_reachable(
        &[id("v1")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    // dir node + 2 file nodes = 3 node hashes
    assert_eq!(result.node_hashes.len(), 3);
    // 2 tree nodes (one per file) + 2 chunks
    assert_eq!(result.chunk_hashes.len(), 2);
    assert_eq!(result.tree_hashes.len(), 2);
}

// ─── Cancellation ────────────────────────────────────────────────────────────

#[test]
fn cancel_returns_error() {
    let s = Stores::default();
    // Build a chain long enough to matter.
    let root = s.add_file_node(b"data");
    s.add_version("v1", vec![], root);
    s.add_version("v2", vec!["v1"], root);
    s.add_version("v3", vec!["v2"], root);

    let cancel = CancelToken::new();
    cancel.cancel(); // Cancel before we even start.

    let err = collect_reachable(
        &[id("v3")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &cancel,
    )
    .unwrap_err();

    assert!(err.to_string().contains("cancelled"));
}

// ─── Large repo (no OOM / no stack overflow) ─────────────────────────────────

/// 10 000-version linear chain. Verifies iterative BFS does not overflow.
#[test]
fn large_linear_chain_no_stack_overflow() {
    let s = Stores::default();
    let root = s.add_file_node(b"shared");

    // Build v0 → v1 → … → v9999
    for i in 0u32..10_000 {
        let parents = if i == 0 { vec![] } else { vec![format!("v{}", i - 1)] };
        s.add_version(
            &format!("v{i}"),
            parents.iter().map(|s| s.as_str()).collect(),
            root,
        );
    }

    let result = collect_reachable(
        &[id("v9999")],
        &HashSet::new(),
        &s.versions,
        &s.trees,
        &s.nodes,
        &no_cancel(),
    )
    .unwrap();

    assert_eq!(result.versions.len(), 10_000);
    // All versions reference the same single node/tree/chunk.
    assert_eq!(result.chunk_hashes.len(), 1);
}
