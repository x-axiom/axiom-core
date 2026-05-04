use std::io::Cursor;

use axiom_core::chunker::{chunk_and_persist, ChunkPolicy};
use axiom_core::merkle::{build_tree, rehydrate, DEFAULT_FAN_OUT};
use axiom_core::model::chunk::ChunkDescriptor;
use axiom_core::model::hash::hash_bytes;
#[cfg(feature = "local")]
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::{
    ChunkStore, InMemoryChunkStore, InMemoryTreeStore, TreeStore,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_descriptors(count: usize) -> Vec<ChunkDescriptor> {
    (0..count)
        .map(|i| {
            let data = format!("chunk-{i}");
            ChunkDescriptor {
                offset: (i * 1024) as u64,
                length: 1024,
                hash: hash_bytes(data.as_bytes()),
                order: i as u32,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Deterministic root: same input → same root
// ---------------------------------------------------------------------------

#[test]
fn test_deterministic_root_hash() {
    let store = InMemoryTreeStore::new();
    let descs = make_descriptors(100);

    let fo1 = build_tree(&descs, DEFAULT_FAN_OUT, &store).unwrap();
    let fo2 = build_tree(&descs, DEFAULT_FAN_OUT, &store).unwrap();

    assert_eq!(fo1.root, fo2.root);
    assert_eq!(fo1.size, fo2.size);
    assert_eq!(fo1.chunk_count, fo2.chunk_count);
}

// ---------------------------------------------------------------------------
// Different input → different root
// ---------------------------------------------------------------------------

#[test]
fn test_different_input_different_root() {
    let store = InMemoryTreeStore::new();
    let descs_a = make_descriptors(50);
    let descs_b = make_descriptors(51);

    let fo_a = build_tree(&descs_a, DEFAULT_FAN_OUT, &store).unwrap();
    let fo_b = build_tree(&descs_b, DEFAULT_FAN_OUT, &store).unwrap();

    assert_ne!(fo_a.root, fo_b.root);
}

// ---------------------------------------------------------------------------
// Rehydrate: leaf order matches original
// ---------------------------------------------------------------------------

#[test]
fn test_rehydrate_preserves_chunk_order() {
    let store = InMemoryTreeStore::new();
    let descs = make_descriptors(200);
    let expected_hashes: Vec<_> = descs.iter().map(|d| d.hash).collect();

    let fo = build_tree(&descs, DEFAULT_FAN_OUT, &store).unwrap();
    let leaves = rehydrate(&fo.root, &store).unwrap();

    assert_eq!(leaves, expected_hashes);
}

// ---------------------------------------------------------------------------
// Single chunk → leaf is root
// ---------------------------------------------------------------------------

#[test]
fn test_single_chunk_tree() {
    let store = InMemoryTreeStore::new();
    let descs = make_descriptors(1);

    let fo = build_tree(&descs, DEFAULT_FAN_OUT, &store).unwrap();
    assert_eq!(fo.chunk_count, 1);

    // Root should be the leaf hash itself.
    assert_eq!(fo.root, descs[0].hash);

    let leaves = rehydrate(&fo.root, &store).unwrap();
    assert_eq!(leaves, vec![descs[0].hash]);
}

// ---------------------------------------------------------------------------
// Fan-out respected: multi-level tree
// ---------------------------------------------------------------------------

#[test]
fn test_fan_out_creates_multi_level_tree() {
    let store = InMemoryTreeStore::new();
    // 256 chunks with fan-out 64 → first level: 4 internal nodes → root.
    let descs = make_descriptors(256);

    let fo = build_tree(&descs, 64, &store).unwrap();
    assert_eq!(fo.chunk_count, 256);

    // The root should be an internal node with 4 children.
    let root_node = store.get_tree_node(&fo.root).unwrap().unwrap();
    match &root_node.kind {
        axiom_core::model::tree::TreeNodeKind::Internal { children } => {
            assert_eq!(children.len(), 4);
        }
        _ => panic!("expected internal root node"),
    }

    // Rehydrate should still give all 256 leaves.
    let leaves = rehydrate(&fo.root, &store).unwrap();
    assert_eq!(leaves.len(), 256);
}

// ---------------------------------------------------------------------------
// Three-level tree
// ---------------------------------------------------------------------------

#[test]
fn test_three_level_tree() {
    let store = InMemoryTreeStore::new();
    // fan_out=4, 20 chunks → level-1: 5 internal nodes → level-2: 2 nodes → root (1 internal of 2)
    let descs = make_descriptors(20);

    let fo = build_tree(&descs, 4, &store).unwrap();
    let leaves = rehydrate(&fo.root, &store).unwrap();
    assert_eq!(leaves.len(), 20);

    let expected: Vec<_> = descs.iter().map(|d| d.hash).collect();
    assert_eq!(leaves, expected);
}

// ---------------------------------------------------------------------------
// Empty input → error
// ---------------------------------------------------------------------------

#[test]
fn test_empty_input_error() {
    let store = InMemoryTreeStore::new();
    let result = build_tree(&[], DEFAULT_FAN_OUT, &store);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// FileObject metadata
// ---------------------------------------------------------------------------

#[test]
fn test_file_object_metadata() {
    let store = InMemoryTreeStore::new();
    let descs = make_descriptors(10);

    let fo = build_tree(&descs, DEFAULT_FAN_OUT, &store).unwrap();
    assert_eq!(fo.chunk_count, 10);
    assert_eq!(fo.size, 10 * 1024); // each descriptor has length=1024
}

// ---------------------------------------------------------------------------
// End-to-end with real chunker
// ---------------------------------------------------------------------------

#[test]
fn test_end_to_end_chunk_then_tree() {
    let chunk_store = InMemoryChunkStore::new();
    let tree_store = InMemoryTreeStore::new();

    let mut data = vec![0u8; 512 * 1024];
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((i * 31 + 7) % 256) as u8;
    }

    let policy = ChunkPolicy {
        min_size: 1024,
        avg_size: 4096,
        max_size: 16384,
    };

    let mut cursor = Cursor::new(data.clone());
    let chunking = chunk_and_persist(&mut cursor, &policy, &chunk_store).unwrap();
    assert!(chunking.descriptors.len() > 1);

    let fo = build_tree(&chunking.descriptors, DEFAULT_FAN_OUT, &tree_store).unwrap();
    assert_eq!(fo.size, data.len() as u64);

    // Rehydrate chunk hashes and reconstruct the file.
    let leaf_hashes = rehydrate(&fo.root, &tree_store).unwrap();
    assert_eq!(leaf_hashes.len(), chunking.descriptors.len());

    let mut reconstructed = Vec::new();
    for hash in &leaf_hashes {
        let chunk_data = chunk_store.get_chunk(hash).unwrap().unwrap();
        reconstructed.extend_from_slice(&chunk_data);
    }
    assert_eq!(reconstructed, data);
}

// ---------------------------------------------------------------------------
// Persistence across RocksDB reopen
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "local")]
fn test_persistence_across_rocksdb_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("cas");

    let descs = make_descriptors(100);
    let root_hash;

    // Build & persist.
    {
        let store = RocksDbCasStore::open(&db_path).unwrap();
        let fo = build_tree(&descs, DEFAULT_FAN_OUT, &store).unwrap();
        root_hash = fo.root;
    }

    // Reopen & rehydrate.
    {
        let store = RocksDbCasStore::open(&db_path).unwrap();
        let leaves = rehydrate(&root_hash, &store).unwrap();
        let expected: Vec<_> = descs.iter().map(|d| d.hash).collect();
        assert_eq!(leaves, expected);
    }
}

// ---------------------------------------------------------------------------
// Leaf ordering for known input
// ---------------------------------------------------------------------------

#[test]
fn test_leaf_ordering_known_input() {
    let store = InMemoryTreeStore::new();
    let hashes: Vec<_> = (0..5u8)
        .map(|i| hash_bytes(&[i]))
        .collect();

    let descs: Vec<ChunkDescriptor> = hashes
        .iter()
        .enumerate()
        .map(|(i, h)| ChunkDescriptor {
            offset: (i * 100) as u64,
            length: 100,
            hash: *h,
            order: i as u32,
        })
        .collect();

    let fo = build_tree(&descs, 2, &store).unwrap();
    let leaves = rehydrate(&fo.root, &store).unwrap();

    assert_eq!(leaves, hashes);
}
