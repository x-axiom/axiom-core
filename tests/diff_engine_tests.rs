use std::collections::BTreeMap;

use axiom_core::diff_engine::diff_versions;
use axiom_core::model::diff::DiffKind;
use axiom_core::model::hash::hash_bytes;
use axiom_core::model::node::{NodeEntry, NodeKind};
use axiom_core::model::tree::{TreeNode, TreeNodeKind};
use axiom_core::model::ChunkHash;
use axiom_core::store::{InMemoryNodeStore, InMemoryTreeStore};
use axiom_core::store::traits::{NodeStore, TreeStore};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_file_node(ns: &InMemoryNodeStore, ts: &InMemoryTreeStore, tag: &str) -> NodeEntry {
    let chunk_hash = hash_bytes(tag.as_bytes());
    // Create a single-leaf Merkle tree for this "file".
    let leaf = TreeNode {
        hash: chunk_hash,
        kind: TreeNodeKind::Leaf { chunk: chunk_hash },
    };
    ts.put_tree_node(&leaf).unwrap();

    let node = NodeEntry {
        hash: hash_bytes(format!("file:{tag}").as_bytes()),
        kind: NodeKind::File {
            root: chunk_hash,
            size: tag.len() as u64,
        },
    };
    ns.put_node(&node).unwrap();
    node
}

fn make_dir_node(
    ns: &InMemoryNodeStore,
    children: BTreeMap<String, ChunkHash>,
) -> NodeEntry {
    // Deterministic dir hash from children.
    let mut hasher = blake3::Hasher::new();
    for (name, hash) in &children {
        hasher.update(name.as_bytes());
        hasher.update(hash.as_bytes());
    }
    let hash = hasher.finalize();

    let node = NodeEntry {
        hash,
        kind: NodeKind::Directory { children },
    };
    ns.put_node(&node).unwrap();
    node
}

fn stores() -> (InMemoryNodeStore, InMemoryTreeStore) {
    (InMemoryNodeStore::new(), InMemoryTreeStore::new())
}

// ---------------------------------------------------------------------------
// Identical versions → empty diff
// ---------------------------------------------------------------------------

#[test]
fn test_identical_versions_empty_diff() {
    let (ns, ts) = stores();

    let f1 = make_file_node(&ns, &ts, "hello");
    let root = make_dir_node(&ns, BTreeMap::from([("a.txt".into(), f1.hash)]));

    let result = diff_versions(&root.hash, &root.hash, &ns, &ts).unwrap();
    assert_eq!(result.entries.len(), 0);
    assert_eq!(result.added_files, 0);
    assert_eq!(result.removed_files, 0);
    assert_eq!(result.modified_files, 0);
}

// ---------------------------------------------------------------------------
// One file added
// ---------------------------------------------------------------------------

#[test]
fn test_one_file_added() {
    let (ns, ts) = stores();

    let f1 = make_file_node(&ns, &ts, "hello");
    let f2 = make_file_node(&ns, &ts, "world");

    let old_root = make_dir_node(&ns, BTreeMap::from([("a.txt".into(), f1.hash)]));
    let new_root = make_dir_node(
        &ns,
        BTreeMap::from([("a.txt".into(), f1.hash), ("b.txt".into(), f2.hash)]),
    );

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.added_files, 1);
    assert_eq!(result.removed_files, 0);
    assert_eq!(result.modified_files, 0);

    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].path, "b.txt");
    assert_eq!(result.entries[0].kind, DiffKind::Added);
}

// ---------------------------------------------------------------------------
// One file removed
// ---------------------------------------------------------------------------

#[test]
fn test_one_file_removed() {
    let (ns, ts) = stores();

    let f1 = make_file_node(&ns, &ts, "hello");
    let f2 = make_file_node(&ns, &ts, "world");

    let old_root = make_dir_node(
        &ns,
        BTreeMap::from([("a.txt".into(), f1.hash), ("b.txt".into(), f2.hash)]),
    );
    let new_root = make_dir_node(&ns, BTreeMap::from([("a.txt".into(), f1.hash)]));

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.added_files, 0);
    assert_eq!(result.removed_files, 1);
    assert_eq!(result.modified_files, 0);

    assert_eq!(result.entries[0].path, "b.txt");
    assert_eq!(result.entries[0].kind, DiffKind::Removed);
}

// ---------------------------------------------------------------------------
// One file modified, others unchanged
// ---------------------------------------------------------------------------

#[test]
fn test_one_file_modified() {
    let (ns, ts) = stores();

    let f1 = make_file_node(&ns, &ts, "hello");
    let f2_old = make_file_node(&ns, &ts, "world-v1");
    let f2_new = make_file_node(&ns, &ts, "world-v2");

    let old_root = make_dir_node(
        &ns,
        BTreeMap::from([("a.txt".into(), f1.hash), ("b.txt".into(), f2_old.hash)]),
    );
    let new_root = make_dir_node(
        &ns,
        BTreeMap::from([("a.txt".into(), f1.hash), ("b.txt".into(), f2_new.hash)]),
    );

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.added_files, 0);
    assert_eq!(result.removed_files, 0);
    assert_eq!(result.modified_files, 1);

    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].path, "b.txt");
    assert_eq!(result.entries[0].kind, DiffKind::Modified);
}

// ---------------------------------------------------------------------------
// Nested directory: file added inside subdirectory
// ---------------------------------------------------------------------------

#[test]
fn test_nested_directory_add() {
    let (ns, ts) = stores();

    let f1 = make_file_node(&ns, &ts, "main");
    let f2 = make_file_node(&ns, &ts, "helper");

    let old_src = make_dir_node(&ns, BTreeMap::from([("main.rs".into(), f1.hash)]));
    let new_src = make_dir_node(
        &ns,
        BTreeMap::from([("main.rs".into(), f1.hash), ("helper.rs".into(), f2.hash)]),
    );

    let old_root = make_dir_node(&ns, BTreeMap::from([("src".into(), old_src.hash)]));
    let new_root = make_dir_node(&ns, BTreeMap::from([("src".into(), new_src.hash)]));

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.added_files, 1);
    assert_eq!(result.entries[0].path, "src/helper.rs");
    assert_eq!(result.entries[0].kind, DiffKind::Added);
}

// ---------------------------------------------------------------------------
// Unchanged subtree is skipped (shared directory hash)
// ---------------------------------------------------------------------------

#[test]
fn test_unchanged_subtree_skipped() {
    let (ns, ts) = stores();

    let f_lib = make_file_node(&ns, &ts, "lib");
    let f_doc_old = make_file_node(&ns, &ts, "doc-v1");
    let f_doc_new = make_file_node(&ns, &ts, "doc-v2");

    let src_dir = make_dir_node(&ns, BTreeMap::from([("lib.rs".into(), f_lib.hash)]));

    let old_root = make_dir_node(
        &ns,
        BTreeMap::from([
            ("src".into(), src_dir.hash),
            ("README.md".into(), f_doc_old.hash),
        ]),
    );
    let new_root = make_dir_node(
        &ns,
        BTreeMap::from([
            ("src".into(), src_dir.hash), // same hash → skipped
            ("README.md".into(), f_doc_new.hash),
        ]),
    );

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    // Only README.md changed, src/ is skipped.
    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].path, "README.md");
    assert_eq!(result.entries[0].kind, DiffKind::Modified);
}

// ---------------------------------------------------------------------------
// Chunk-level stats: unchanged chunks counted for skipped files
// ---------------------------------------------------------------------------

#[test]
fn test_chunk_stats_unchanged() {
    let (ns, ts) = stores();

    let f_unchanged = make_file_node(&ns, &ts, "stable");
    let f_changed_old = make_file_node(&ns, &ts, "volatile-v1");
    let f_changed_new = make_file_node(&ns, &ts, "volatile-v2");

    let old_root = make_dir_node(
        &ns,
        BTreeMap::from([
            ("stable.txt".into(), f_unchanged.hash),
            ("volatile.txt".into(), f_changed_old.hash),
        ]),
    );
    let new_root = make_dir_node(
        &ns,
        BTreeMap::from([
            ("stable.txt".into(), f_unchanged.hash),
            ("volatile.txt".into(), f_changed_new.hash),
        ]),
    );

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    // stable.txt has 1 unchanged chunk.
    assert_eq!(result.unchanged_chunks, 1);
    // volatile.txt: old chunk removed, new chunk added.
    assert_eq!(result.added_chunks, 1);
    assert_eq!(result.removed_chunks, 1);
}

// ---------------------------------------------------------------------------
// Chunk-level stats with multi-leaf Merkle tree
// ---------------------------------------------------------------------------

#[test]
fn test_chunk_stats_multi_leaf() {
    let (ns, ts) = stores();

    // Build a file with 3 leaf chunks.
    let c1 = hash_bytes(b"chunk1");
    let c2 = hash_bytes(b"chunk2");
    let c3 = hash_bytes(b"chunk3");
    let c4 = hash_bytes(b"chunk4"); // replaces c3 in new version

    for c in [c1, c2, c3, c4] {
        ts.put_tree_node(&TreeNode {
            hash: c,
            kind: TreeNodeKind::Leaf { chunk: c },
        }).unwrap();
    }

    // Old file tree: internal -> [c1, c2, c3]
    let old_internal_hash = hash_bytes(b"old-internal");
    ts.put_tree_node(&TreeNode {
        hash: old_internal_hash,
        kind: TreeNodeKind::Internal {
            children: vec![c1, c2, c3],
        },
    }).unwrap();

    // New file tree: internal -> [c1, c2, c4]
    let new_internal_hash = hash_bytes(b"new-internal");
    ts.put_tree_node(&TreeNode {
        hash: new_internal_hash,
        kind: TreeNodeKind::Internal {
            children: vec![c1, c2, c4],
        },
    }).unwrap();

    let old_file = NodeEntry {
        hash: hash_bytes(b"file-old"),
        kind: NodeKind::File {
            root: old_internal_hash,
            size: 300,
        },
    };
    let new_file = NodeEntry {
        hash: hash_bytes(b"file-new"),
        kind: NodeKind::File {
            root: new_internal_hash,
            size: 300,
        },
    };
    ns.put_node(&old_file).unwrap();
    ns.put_node(&new_file).unwrap();

    let old_root = make_dir_node(&ns, BTreeMap::from([("data.bin".into(), old_file.hash)]));
    let new_root = make_dir_node(&ns, BTreeMap::from([("data.bin".into(), new_file.hash)]));

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.modified_files, 1);
    // c1, c2 unchanged; c3 removed; c4 added.
    assert_eq!(result.unchanged_chunks, 2);
    assert_eq!(result.removed_chunks, 1);
    assert_eq!(result.added_chunks, 1);
}

// ---------------------------------------------------------------------------
// Mixed: add + remove + modify in one diff
// ---------------------------------------------------------------------------

#[test]
fn test_mixed_changes() {
    let (ns, ts) = stores();

    let f_keep = make_file_node(&ns, &ts, "keep");
    let f_remove = make_file_node(&ns, &ts, "gone");
    let f_modify_old = make_file_node(&ns, &ts, "mod-v1");
    let f_modify_new = make_file_node(&ns, &ts, "mod-v2");
    let f_add = make_file_node(&ns, &ts, "new-file");

    let old_root = make_dir_node(
        &ns,
        BTreeMap::from([
            ("keep.txt".into(), f_keep.hash),
            ("gone.txt".into(), f_remove.hash),
            ("mod.txt".into(), f_modify_old.hash),
        ]),
    );
    let new_root = make_dir_node(
        &ns,
        BTreeMap::from([
            ("keep.txt".into(), f_keep.hash),
            ("mod.txt".into(), f_modify_new.hash),
            ("new.txt".into(), f_add.hash),
        ]),
    );

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.added_files, 1);
    assert_eq!(result.removed_files, 1);
    assert_eq!(result.modified_files, 1);
    assert_eq!(result.entries.len(), 3);

    let paths: Vec<(&str, &DiffKind)> = result
        .entries
        .iter()
        .map(|e| (e.path.as_str(), &e.kind))
        .collect();
    assert!(paths.contains(&("gone.txt", &DiffKind::Removed)));
    assert!(paths.contains(&("mod.txt", &DiffKind::Modified)));
    assert!(paths.contains(&("new.txt", &DiffKind::Added)));
}

// ---------------------------------------------------------------------------
// Type change: file → directory
// ---------------------------------------------------------------------------

#[test]
fn test_type_change_file_to_directory() {
    let (ns, ts) = stores();

    let f_old = make_file_node(&ns, &ts, "was-file");
    let f_child = make_file_node(&ns, &ts, "child-file");
    let new_dir = make_dir_node(&ns, BTreeMap::from([("inner.txt".into(), f_child.hash)]));

    let old_root = make_dir_node(&ns, BTreeMap::from([("x".into(), f_old.hash)]));
    let new_root = make_dir_node(&ns, BTreeMap::from([("x".into(), new_dir.hash)]));

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    // Old file "x" removed, new file "x/inner.txt" added.
    assert_eq!(result.removed_files, 1);
    assert_eq!(result.added_files, 1);

    let paths: Vec<(&str, &DiffKind)> = result
        .entries
        .iter()
        .map(|e| (e.path.as_str(), &e.kind))
        .collect();
    assert!(paths.contains(&("x", &DiffKind::Removed)));
    assert!(paths.contains(&("x/inner.txt", &DiffKind::Added)));
}

// ---------------------------------------------------------------------------
// Empty directories
// ---------------------------------------------------------------------------

#[test]
fn test_empty_dir_vs_populated() {
    let (ns, ts) = stores();

    let empty_root = make_dir_node(&ns, BTreeMap::new());

    let f = make_file_node(&ns, &ts, "content");
    let populated_root = make_dir_node(&ns, BTreeMap::from([("a.txt".into(), f.hash)]));

    let result = diff_versions(&empty_root.hash, &populated_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.added_files, 1);
    assert_eq!(result.removed_files, 0);
}

// ---------------------------------------------------------------------------
// Deeply nested modification
// ---------------------------------------------------------------------------

#[test]
fn test_deeply_nested_modification() {
    let (ns, ts) = stores();

    let f_old = make_file_node(&ns, &ts, "deep-v1");
    let f_new = make_file_node(&ns, &ts, "deep-v2");

    let old_c = make_dir_node(&ns, BTreeMap::from([("f.txt".into(), f_old.hash)]));
    let new_c = make_dir_node(&ns, BTreeMap::from([("f.txt".into(), f_new.hash)]));

    let old_b = make_dir_node(&ns, BTreeMap::from([("c".into(), old_c.hash)]));
    let new_b = make_dir_node(&ns, BTreeMap::from([("c".into(), new_c.hash)]));

    let old_root = make_dir_node(&ns, BTreeMap::from([("a".into(), old_b.hash)]));
    let new_root = make_dir_node(&ns, BTreeMap::from([("a".into(), new_b.hash)]));

    let result = diff_versions(&old_root.hash, &new_root.hash, &ns, &ts).unwrap();
    assert_eq!(result.modified_files, 1);
    assert_eq!(result.entries[0].path, "a/c/f.txt");
}
