use axiom_core::model::{
    VersionId, VersionNode, Ref, RefKind, TreeNode, TreeNodeKind,
    hash_bytes, hash_children, current_timestamp,
};
use axiom_core::store::{
    ChunkStore, TreeStore, VersionRepo, RefRepo,
    InMemoryChunkStore, InMemoryVersionRepo, InMemoryRefRepo,
    RocksDbCasStore,
};

fn main() {
    // ── 1. InMemory model demo ───────────────────────────────
    println!("=== InMemory Demo ===");
    let chunks = InMemoryChunkStore::new();
    let versions = InMemoryVersionRepo::new();
    let refs = InMemoryRefRepo::new();

    // Store chunks
    let data = b"Hello, Axiom v0.1!".to_vec();
    let chunk_hash = chunks.put_chunk(data).unwrap();
    println!("Stored chunk: {}", chunk_hash);

    // Build a simple object (flat list of chunks → root hash)
    let root_hash = hash_children(&[chunk_hash]);
    println!("Object root:  {}", root_hash);

    // Create a version
    let version_id = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(root_hash.as_bytes());
        hasher.update(&current_timestamp().to_le_bytes());
        VersionId(hasher.finalize().to_hex().to_string())
    };

    let v1 = VersionNode {
        id: version_id.clone(),
        parents: vec![],
        root: root_hash,
        message: "initial commit".into(),
        timestamp: current_timestamp(),
        metadata: Default::default(),
    };
    versions.put_version(&v1).unwrap();
    println!("Version:      {}", v1.id);

    // Create a branch ref
    let main_ref = Ref {
        name: "main".into(),
        kind: RefKind::Branch,
        target: version_id.clone(),
    };
    refs.put_ref(&main_ref).unwrap();

    // Resolve branch → version → root
    let resolved = refs.get_ref("main").unwrap().unwrap();
    let resolved_version = versions.get_version(&resolved.target).unwrap().unwrap();
    println!("main → {}  root: {}", resolved_version.id, resolved_version.root);

    // Verify chunk still retrievable
    let retrieved = chunks.get_chunk(&chunk_hash).unwrap().unwrap();
    println!("Data:         {:?}", String::from_utf8_lossy(&retrieved));

    // ── 2. RocksDB persistence demo ──────────────────────────
    println!("\n=== RocksDB Persistence Demo ===");
    let db_path = ".axiom/demo-cas";

    // Write phase
    {
        let store = RocksDbCasStore::open(db_path).unwrap();
        println!("Opened RocksDB at {}", db_path);

        // Store a chunk
        let data = b"Hello from RocksDB!".to_vec();
        let hash = store.put_chunk(data).unwrap();
        println!("Stored chunk:  {}", hash);

        // Idempotent: writing the same content again yields the same hash
        let hash2 = store.put_chunk(b"Hello from RocksDB!".to_vec()).unwrap();
        assert_eq!(hash, hash2);
        println!("Idempotent:    same hash on duplicate write ✓");

        // Store a tree node referencing that chunk
        let leaf = TreeNode {
            hash: hash_bytes(b"leaf-node"),
            kind: TreeNodeKind::Leaf { chunk: hash },
        };
        store.put_tree_node(&leaf).unwrap();
        println!("Stored tree:   {}", leaf.hash);

        println!("Closing store...");
    }

    // Read-back phase (simulates process restart)
    {
        let store = RocksDbCasStore::open(db_path).unwrap();
        println!("Reopened RocksDB at {}", db_path);

        let chunk_hash = hash_bytes(b"Hello from RocksDB!");
        let retrieved = store.get_chunk(&chunk_hash).unwrap().unwrap();
        println!("Retrieved:     {:?}", String::from_utf8_lossy(&retrieved));

        let tree_hash = hash_bytes(b"leaf-node");
        let tree_node = store.get_tree_node(&tree_hash).unwrap().unwrap();
        println!("Tree node:     {} (persisted across reopen ✓)", tree_node.hash);
    }

    println!("\nDone. RocksDB data persisted at {}/", db_path);
}
