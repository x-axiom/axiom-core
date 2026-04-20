#![cfg(feature = "local")]

#[cfg(test)]
mod tests {
    use axiom_core::model::*;
    use axiom_core::store::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    // Helper: open a fresh RocksDbCasStore in a temp directory.
    fn open_store() -> (RocksDbCasStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = RocksDbCasStore::open(dir.path()).unwrap();
        (store, dir)
    }

    // Helper: reopen the store at the same path (simulates process restart).
    fn reopen_store(dir: &TempDir) -> RocksDbCasStore {
        RocksDbCasStore::open(dir.path()).unwrap()
    }

    // ── ChunkStore basics ───────────────────────────────────

    #[test]
    fn chunk_put_and_get() {
        let (store, _dir) = open_store();
        let data = b"hello rocksdb".to_vec();
        let hash = store.put_chunk(data.clone()).unwrap();
        let retrieved = store.get_chunk(&hash).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn chunk_idempotent_write() {
        let (store, _dir) = open_store();
        let data = b"same content".to_vec();
        let h1 = store.put_chunk(data.clone()).unwrap();
        let h2 = store.put_chunk(data).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn chunk_has_chunk() {
        let (store, _dir) = open_store();
        let hash = store.put_chunk(b"exists".to_vec()).unwrap();
        assert!(store.has_chunk(&hash).unwrap());

        let missing = hash_bytes(b"missing");
        assert!(!store.has_chunk(&missing).unwrap());
    }

    #[test]
    fn chunk_get_missing_returns_none() {
        let (store, _dir) = open_store();
        let missing = hash_bytes(b"nope");
        assert!(store.get_chunk(&missing).unwrap().is_none());
    }

    // ── ChunkStore persistence across restart ───────────────

    #[test]
    fn chunk_persists_across_reopen() {
        let (store, dir) = open_store();
        let data = b"persist me".to_vec();
        let hash = store.put_chunk(data.clone()).unwrap();
        drop(store);

        let store2 = reopen_store(&dir);
        let retrieved = store2.get_chunk(&hash).unwrap().unwrap();
        assert_eq!(retrieved, data);
        assert!(store2.has_chunk(&hash).unwrap());
    }

    // ── TreeStore basics ────────────────────────────────────

    #[test]
    fn tree_put_and_get_leaf() {
        let (store, _dir) = open_store();
        let leaf = TreeNode {
            hash: hash_bytes(b"chunk-leaf"),
            kind: TreeNodeKind::Leaf {
                chunk: hash_bytes(b"chunk-leaf"),
            },
        };
        store.put_tree_node(&leaf).unwrap();

        let retrieved = store.get_tree_node(&leaf.hash).unwrap().unwrap();
        assert_eq!(retrieved.hash, leaf.hash);
    }

    #[test]
    fn tree_put_and_get_internal() {
        let (store, _dir) = open_store();
        let child_a = hash_bytes(b"a");
        let child_b = hash_bytes(b"b");
        let parent_hash = hash_children(&[child_a, child_b]);

        let node = TreeNode {
            hash: parent_hash,
            kind: TreeNodeKind::Internal {
                children: vec![child_a, child_b],
            },
        };
        store.put_tree_node(&node).unwrap();

        let retrieved = store.get_tree_node(&parent_hash).unwrap().unwrap();
        match retrieved.kind {
            TreeNodeKind::Internal { children } => {
                assert_eq!(children.len(), 2);
                assert_eq!(children[0], child_a);
                assert_eq!(children[1], child_b);
            }
            _ => panic!("expected internal node"),
        }
    }

    #[test]
    fn tree_get_missing_returns_none() {
        let (store, _dir) = open_store();
        let missing = hash_bytes(b"no-tree");
        assert!(store.get_tree_node(&missing).unwrap().is_none());
    }

    #[test]
    fn tree_persists_across_reopen() {
        let (store, dir) = open_store();
        let node = TreeNode {
            hash: hash_bytes(b"persist-tree"),
            kind: TreeNodeKind::Leaf {
                chunk: hash_bytes(b"persist-tree"),
            },
        };
        store.put_tree_node(&node).unwrap();
        drop(store);

        let store2 = reopen_store(&dir);
        let retrieved = store2.get_tree_node(&node.hash).unwrap().unwrap();
        assert_eq!(retrieved.hash, node.hash);
    }

    // ── NodeStore basics ────────────────────────────────────

    #[test]
    fn node_put_and_get_file() {
        let (store, _dir) = open_store();
        let entry = NodeEntry {
            hash: hash_bytes(b"file-entry"),
            kind: NodeKind::File {
                root: hash_bytes(b"file-root"),
                size: 2048,
            },
        };
        store.put_node(&entry).unwrap();

        let retrieved = store.get_node(&entry.hash).unwrap().unwrap();
        assert!(retrieved.is_file());
    }

    #[test]
    fn node_put_and_get_directory() {
        let (store, _dir) = open_store();
        let mut children = BTreeMap::new();
        children.insert("readme.md".to_string(), hash_bytes(b"child"));

        let entry = NodeEntry {
            hash: hash_bytes(b"dir-entry"),
            kind: NodeKind::Directory { children },
        };
        store.put_node(&entry).unwrap();

        let retrieved = store.get_node(&entry.hash).unwrap().unwrap();
        assert!(retrieved.is_directory());
    }

    #[test]
    fn node_get_missing_returns_none() {
        let (store, _dir) = open_store();
        let missing = hash_bytes(b"no-node");
        assert!(store.get_node(&missing).unwrap().is_none());
    }

    #[test]
    fn node_persists_across_reopen() {
        let (store, dir) = open_store();
        let entry = NodeEntry {
            hash: hash_bytes(b"persist-node"),
            kind: NodeKind::File {
                root: hash_bytes(b"persist-root"),
                size: 512,
            },
        };
        store.put_node(&entry).unwrap();
        drop(store);

        let store2 = reopen_store(&dir);
        let retrieved = store2.get_node(&entry.hash).unwrap().unwrap();
        assert!(retrieved.is_file());
    }

    // ── Error path tests ────────────────────────────────────

    #[test]
    fn open_on_empty_directory_succeeds() {
        let dir = TempDir::new().unwrap();
        let result = RocksDbCasStore::open(dir.path());
        assert!(result.is_ok());
    }

    #[test]
    fn reopen_existing_directory_succeeds() {
        let dir = TempDir::new().unwrap();
        {
            let _store = RocksDbCasStore::open(dir.path()).unwrap();
        }
        let result = RocksDbCasStore::open(dir.path());
        assert!(result.is_ok());
    }

    // ── Idempotent tree and node writes ─────────────────────

    #[test]
    fn tree_idempotent_write() {
        let (store, _dir) = open_store();
        let node = TreeNode {
            hash: hash_bytes(b"idem-tree"),
            kind: TreeNodeKind::Leaf {
                chunk: hash_bytes(b"idem-tree"),
            },
        };
        store.put_tree_node(&node).unwrap();
        store.put_tree_node(&node).unwrap(); // second write should not fail

        let retrieved = store.get_tree_node(&node.hash).unwrap().unwrap();
        assert_eq!(retrieved.hash, node.hash);
    }

    #[test]
    fn node_idempotent_write() {
        let (store, _dir) = open_store();
        let entry = NodeEntry {
            hash: hash_bytes(b"idem-node"),
            kind: NodeKind::File {
                root: hash_bytes(b"idem-root"),
                size: 100,
            },
        };
        store.put_node(&entry).unwrap();
        store.put_node(&entry).unwrap(); // second write should not fail

        let retrieved = store.get_node(&entry.hash).unwrap().unwrap();
        assert!(retrieved.is_file());
    }

    // ── Multiple records coexist ────────────────────────────

    #[test]
    fn multiple_chunks_coexist() {
        let (store, _dir) = open_store();
        let h1 = store.put_chunk(b"alpha".to_vec()).unwrap();
        let h2 = store.put_chunk(b"beta".to_vec()).unwrap();
        let h3 = store.put_chunk(b"gamma".to_vec()).unwrap();

        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
        assert_eq!(store.get_chunk(&h1).unwrap().unwrap(), b"alpha");
        assert_eq!(store.get_chunk(&h2).unwrap().unwrap(), b"beta");
        assert_eq!(store.get_chunk(&h3).unwrap().unwrap(), b"gamma");
    }

    #[test]
    fn full_lifecycle_across_restart() {
        let (store, dir) = open_store();

        // Write a chunk, a tree node, and a directory node.
        let chunk_hash = store.put_chunk(b"lifecycle data".to_vec()).unwrap();

        let tree_node = TreeNode {
            hash: hash_bytes(b"lc-tree"),
            kind: TreeNodeKind::Leaf {
                chunk: chunk_hash,
            },
        };
        store.put_tree_node(&tree_node).unwrap();

        let mut children = BTreeMap::new();
        children.insert("data.bin".to_string(), tree_node.hash);
        let dir_node = NodeEntry {
            hash: hash_bytes(b"lc-dir"),
            kind: NodeKind::Directory { children },
        };
        store.put_node(&dir_node).unwrap();

        // Drop and reopen.
        drop(store);
        let store2 = reopen_store(&dir);

        // All records survive.
        assert_eq!(
            store2.get_chunk(&chunk_hash).unwrap().unwrap(),
            b"lifecycle data"
        );
        assert!(store2.get_tree_node(&tree_node.hash).unwrap().is_some());
        assert!(store2.get_node(&dir_node.hash).unwrap().is_some());
    }
}
