use axiom_core::model::{
    VersionId, VersionNode, Ref, RefKind,
    hash_children, current_timestamp,
};
use axiom_core::store::{
    ChunkStore, VersionRepo, RefRepo,
    InMemoryChunkStore, InMemoryVersionRepo, InMemoryRefRepo,
};

fn main() {
    // ── New v0.1 model demo ──────────────────────────────────
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
}
