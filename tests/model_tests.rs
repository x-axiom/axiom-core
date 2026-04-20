#[cfg(test)]
mod tests {
    use axiom_core::model::*;
    use axiom_core::store::*;
    use std::collections::BTreeMap;

    // ── ChunkStore ──────────────────────────────────────────

    #[test]
    fn chunk_store_put_and_get() {
        let store = InMemoryChunkStore::new();
        let data = b"hello world".to_vec();
        let hash = store.put_chunk(data.clone()).unwrap();

        let retrieved = store.get_chunk(&hash).unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn chunk_store_idempotent() {
        let store = InMemoryChunkStore::new();
        let data = b"same content".to_vec();
        let h1 = store.put_chunk(data.clone()).unwrap();
        let h2 = store.put_chunk(data).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn chunk_store_has_chunk() {
        let store = InMemoryChunkStore::new();
        let hash = store.put_chunk(b"test".to_vec()).unwrap();
        assert!(store.has_chunk(&hash).unwrap());

        let missing = hash_bytes(b"nonexistent");
        assert!(!store.has_chunk(&missing).unwrap());
    }

    #[test]
    fn chunk_store_get_missing_returns_none() {
        let store = InMemoryChunkStore::new();
        let missing = hash_bytes(b"nope");
        assert!(store.get_chunk(&missing).unwrap().is_none());
    }

    // ── ChunkStore batch methods ────────────────────────────

    #[test]
    fn chunk_store_put_chunks() {
        let store = InMemoryChunkStore::new();
        let chunks = vec![b"aaa".to_vec(), b"bbb".to_vec(), b"ccc".to_vec()];
        let hashes = store.put_chunks(chunks.clone()).unwrap();
        assert_eq!(hashes.len(), 3);
        for (data, hash) in chunks.iter().zip(&hashes) {
            let got = store.get_chunk(hash).unwrap().unwrap();
            assert_eq!(&got, data);
        }
    }

    #[test]
    fn chunk_store_get_chunks() {
        let store = InMemoryChunkStore::new();
        let h1 = store.put_chunk(b"x".to_vec()).unwrap();
        let h2 = store.put_chunk(b"y".to_vec()).unwrap();
        let missing = hash_bytes(b"missing");

        let results = store.get_chunks(&[h1, missing, h2]).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_deref(), Some(b"x".as_slice()));
        assert!(results[1].is_none());
        assert_eq!(results[2].as_deref(), Some(b"y".as_slice()));
    }

    #[test]
    fn chunk_store_has_chunks() {
        let store = InMemoryChunkStore::new();
        let h1 = store.put_chunk(b"exists".to_vec()).unwrap();
        let missing = hash_bytes(b"nope");

        let results = store.has_chunks(&[h1, missing]).unwrap();
        assert_eq!(results, vec![true, false]);
    }

    #[test]
    fn chunk_store_put_chunks_empty() {
        let store = InMemoryChunkStore::new();
        let hashes = store.put_chunks(vec![]).unwrap();
        assert!(hashes.is_empty());
    }

    // ── TreeStore ───────────────────────────────────────────

    #[test]
    fn tree_store_put_and_get() {
        let store = InMemoryTreeStore::new();
        let leaf = TreeNode {
            hash: hash_bytes(b"chunk-a"),
            kind: TreeNodeKind::Leaf {
                chunk: hash_bytes(b"chunk-a"),
            },
        };
        store.put_tree_node(&leaf).unwrap();

        let retrieved = store.get_tree_node(&leaf.hash).unwrap().unwrap();
        assert_eq!(retrieved.hash, leaf.hash);
    }

    #[test]
    fn tree_store_internal_node() {
        let store = InMemoryTreeStore::new();
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

    // ── NodeStore ───────────────────────────────────────────

    #[test]
    fn node_store_file_entry() {
        let store = InMemoryNodeStore::new();
        let file_root = hash_bytes(b"file-root");
        let file_hash = hash_bytes(b"file-entry-hash");

        let entry = NodeEntry {
            hash: file_hash,
            kind: NodeKind::File {
                root: file_root,
                size: 1024,
            },
        };
        store.put_node(&entry).unwrap();

        let retrieved = store.get_node(&file_hash).unwrap().unwrap();
        assert!(retrieved.is_file());
        assert!(!retrieved.is_directory());
    }

    #[test]
    fn node_store_directory_entry() {
        let store = InMemoryNodeStore::new();
        let child_hash = hash_bytes(b"child");
        let mut children = BTreeMap::new();
        children.insert("readme.md".to_string(), child_hash);

        let dir_hash = hash_bytes(b"dir-hash");
        let entry = NodeEntry {
            hash: dir_hash,
            kind: NodeKind::Directory { children },
        };
        store.put_node(&entry).unwrap();

        let retrieved = store.get_node(&dir_hash).unwrap().unwrap();
        assert!(retrieved.is_directory());
    }

    // ── VersionRepo ─────────────────────────────────────────

    #[test]
    fn version_repo_put_and_get() {
        let repo = InMemoryVersionRepo::new();
        let v = VersionNode {
            id: VersionId::from("v1"),
            parents: vec![],
            root: hash_bytes(b"root1"),
            message: "init".into(),
            timestamp: 1000,
            metadata: Default::default(),
        };
        repo.put_version(&v).unwrap();

        let retrieved = repo.get_version(&VersionId::from("v1")).unwrap().unwrap();
        assert_eq!(retrieved.message, "init");
    }

    #[test]
    fn version_repo_list_history() {
        let repo = InMemoryVersionRepo::new();

        let v1 = VersionNode {
            id: VersionId::from("v1"),
            parents: vec![],
            root: hash_bytes(b"r1"),
            message: "first".into(),
            timestamp: 1000,
            metadata: Default::default(),
        };
        let v2 = VersionNode {
            id: VersionId::from("v2"),
            parents: vec![VersionId::from("v1")],
            root: hash_bytes(b"r2"),
            message: "second".into(),
            timestamp: 2000,
            metadata: Default::default(),
        };
        let v3 = VersionNode {
            id: VersionId::from("v3"),
            parents: vec![VersionId::from("v2")],
            root: hash_bytes(b"r3"),
            message: "third".into(),
            timestamp: 3000,
            metadata: Default::default(),
        };

        repo.put_version(&v1).unwrap();
        repo.put_version(&v2).unwrap();
        repo.put_version(&v3).unwrap();

        let history = repo.list_history(&VersionId::from("v3"), 10).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].id.as_str(), "v3");
        assert_eq!(history[1].id.as_str(), "v2");
        assert_eq!(history[2].id.as_str(), "v1");
    }

    #[test]
    fn version_repo_list_history_with_limit() {
        let repo = InMemoryVersionRepo::new();

        let v1 = VersionNode {
            id: VersionId::from("v1"),
            parents: vec![],
            root: hash_bytes(b"r1"),
            message: "first".into(),
            timestamp: 1000,
            metadata: Default::default(),
        };
        let v2 = VersionNode {
            id: VersionId::from("v2"),
            parents: vec![VersionId::from("v1")],
            root: hash_bytes(b"r2"),
            message: "second".into(),
            timestamp: 2000,
            metadata: Default::default(),
        };

        repo.put_version(&v1).unwrap();
        repo.put_version(&v2).unwrap();

        let history = repo.list_history(&VersionId::from("v2"), 1).unwrap();
        assert_eq!(history.len(), 1);
    }

    // ── RefRepo ─────────────────────────────────────────────

    #[test]
    fn ref_repo_branch_crud() {
        let repo = InMemoryRefRepo::new();
        let branch = Ref {
            name: "main".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v1"),
        };
        repo.put_ref(&branch).unwrap();

        let retrieved = repo.get_ref("main").unwrap().unwrap();
        assert_eq!(retrieved.target.as_str(), "v1");

        // Branch can be updated
        let updated = Ref {
            name: "main".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v2"),
        };
        repo.put_ref(&updated).unwrap();
        let retrieved = repo.get_ref("main").unwrap().unwrap();
        assert_eq!(retrieved.target.as_str(), "v2");

        // Delete
        repo.delete_ref("main").unwrap();
        assert!(repo.get_ref("main").unwrap().is_none());
    }

    #[test]
    fn ref_repo_tag_immutable() {
        let repo = InMemoryRefRepo::new();
        let tag = Ref {
            name: "v1.0".into(),
            kind: RefKind::Tag,
            target: VersionId::from("v1"),
        };
        repo.put_ref(&tag).unwrap();

        // Tag cannot be overwritten
        let duplicate = Ref {
            name: "v1.0".into(),
            kind: RefKind::Tag,
            target: VersionId::from("v2"),
        };
        assert!(repo.put_ref(&duplicate).is_err());
    }

    #[test]
    fn ref_repo_list_by_kind() {
        let repo = InMemoryRefRepo::new();
        repo.put_ref(&Ref {
            name: "main".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v2"),
        })
        .unwrap();
        repo.put_ref(&Ref {
            name: "v1.0".into(),
            kind: RefKind::Tag,
            target: VersionId::from("v1"),
        })
        .unwrap();

        let branches = repo.list_refs(Some(RefKind::Branch)).unwrap();
        assert_eq!(branches.len(), 1);
        assert_eq!(branches[0].name, "main");

        let tags = repo.list_refs(Some(RefKind::Tag)).unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].name, "v1.0");

        let all = repo.list_refs(None).unwrap();
        assert_eq!(all.len(), 2);
    }

    // ── Model types ─────────────────────────────────────────

    #[test]
    fn hash_children_deterministic() {
        let a = hash_bytes(b"a");
        let b = hash_bytes(b"b");

        let h1 = hash_children(&[a, b]);
        let h2 = hash_children(&[a, b]);
        assert_eq!(h1, h2);

        // Order matters
        let h3 = hash_children(&[b, a]);
        assert_ne!(h1, h3);
    }

    #[test]
    fn diff_result_empty() {
        let diff = DiffResult::empty();
        assert_eq!(diff.entries.len(), 0);
        assert_eq!(diff.added_files, 0);
    }

    #[test]
    fn version_id_display_and_eq() {
        let id1 = VersionId::from("abc123");
        let id2 = VersionId::from("abc123");
        assert_eq!(id1, id2);
        assert_eq!(format!("{}", id1), "abc123");
    }

    #[test]
    fn file_object_serialization() {
        let obj = FileObject {
            root: hash_bytes(b"root"),
            size: 1048576,
            chunk_count: 4,
        };
        let json = serde_json::to_string(&obj).unwrap();
        let deserialized: FileObject = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.root, obj.root);
        assert_eq!(deserialized.size, 1048576);
        assert_eq!(deserialized.chunk_count, 4);
    }

    // ── SyncStore ───────────────────────────────────────────

    #[test]
    fn sync_store_list_all_version_ids_empty() {
        let versions = std::sync::Arc::new(InMemoryVersionRepo::new());
        let trees = std::sync::Arc::new(InMemoryTreeStore::new());
        let nodes = std::sync::Arc::new(InMemoryNodeStore::new());
        let sync = InMemorySyncStore::new(versions, trees, nodes);

        let ids = sync.list_all_version_ids().unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn sync_store_collect_reachable_simple() {
        use axiom_core::store::SyncStore;

        let versions = std::sync::Arc::new(InMemoryVersionRepo::new());
        let trees = std::sync::Arc::new(InMemoryTreeStore::new());
        let nodes = std::sync::Arc::new(InMemoryNodeStore::new());
        let chunks = InMemoryChunkStore::new();

        // Create a chunk
        let chunk_hash = chunks.put_chunk(b"hello".to_vec()).unwrap();

        // Create a tree leaf pointing to the chunk
        let tree_leaf = TreeNode {
            hash: hash_bytes(b"tree-leaf"),
            kind: TreeNodeKind::Leaf { chunk: chunk_hash },
        };
        trees.put_tree_node(&tree_leaf).unwrap();

        // Create a file node pointing to the tree root
        let file_node = NodeEntry {
            hash: hash_bytes(b"file-node"),
            kind: NodeKind::File { root: tree_leaf.hash, size: 5 },
        };
        nodes.put_node(&file_node).unwrap();

        // Create a directory node containing the file
        let mut children = std::collections::BTreeMap::new();
        children.insert("hello.txt".to_string(), file_node.hash);
        let dir_node = NodeEntry {
            hash: hash_bytes(b"dir-node"),
            kind: NodeKind::Directory { children },
        };
        nodes.put_node(&dir_node).unwrap();

        // Create a version pointing to the dir as root
        let version = VersionNode {
            id: VersionId::from("v1"),
            parents: vec![],
            root: dir_node.hash,
            message: "init".to_string(),
            timestamp: 1000,
            metadata: std::collections::HashMap::new(),
        };
        versions.put_version(&version).unwrap();

        let sync = InMemorySyncStore::new(versions, trees, nodes);
        let reachable = sync.collect_reachable_objects(&[VersionId::from("v1")]).unwrap();

        assert!(reachable.versions.contains(&VersionId::from("v1")));
        assert!(reachable.node_hashes.contains(&dir_node.hash));
        assert!(reachable.node_hashes.contains(&file_node.hash));
        assert!(reachable.tree_hashes.contains(&tree_leaf.hash));
        assert!(reachable.chunk_hashes.contains(&chunk_hash));
    }

    #[test]
    fn sync_store_collect_walks_parent_versions() {
        use axiom_core::store::SyncStore;

        let versions = std::sync::Arc::new(InMemoryVersionRepo::new());
        let trees = std::sync::Arc::new(InMemoryTreeStore::new());
        let nodes = std::sync::Arc::new(InMemoryNodeStore::new());

        // Create an empty dir node for both versions
        let dir_node = NodeEntry {
            hash: hash_bytes(b"empty-dir"),
            kind: NodeKind::Directory { children: std::collections::BTreeMap::new() },
        };
        nodes.put_node(&dir_node).unwrap();

        let v1 = VersionNode {
            id: VersionId::from("v1"),
            parents: vec![],
            root: dir_node.hash,
            message: "first".to_string(),
            timestamp: 1000,
            metadata: std::collections::HashMap::new(),
        };
        let v2 = VersionNode {
            id: VersionId::from("v2"),
            parents: vec![VersionId::from("v1")],
            root: dir_node.hash,
            message: "second".to_string(),
            timestamp: 2000,
            metadata: std::collections::HashMap::new(),
        };
        versions.put_version(&v1).unwrap();
        versions.put_version(&v2).unwrap();

        let sync = InMemorySyncStore::new(versions.clone(), trees, nodes);

        // Starting from v2 should also reach v1
        let reachable = sync.collect_reachable_objects(&[VersionId::from("v2")]).unwrap();
        assert!(reachable.versions.contains(&VersionId::from("v1")));
        assert!(reachable.versions.contains(&VersionId::from("v2")));

        // list_all_version_ids should return both
        let all_ids = sync.list_all_version_ids().unwrap();
        assert_eq!(all_ids.len(), 2);
    }
}
