use blake3::Hash;
use parking_lot::RwLock;
use std::collections::HashMap;

pub type ChunkHash = Hash;

#[derive(Clone)]
pub struct Chunk {
    pub hash: ChunkHash,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct Object {
    pub hash: ChunkHash,
    pub chunks: Vec<ChunkHash>,
}

pub trait CasStore: Send + Sync {
    fn put_chunk(&self, data: Vec<u8>) -> ChunkHash;
    fn get_chunk(&self, hash: &ChunkHash) -> Option<Vec<u8>>;
    fn put_object(&self, chunks: Vec<ChunkHash>) -> ChunkHash;
    fn get_object(&self, hash: &ChunkHash) -> Option<Object>;
}

#[derive(Default)]
pub struct InMemoryCas {
    chunks: RwLock<HashMap<ChunkHash, Vec<u8>>>,
    objects: RwLock<HashMap<ChunkHash, Object>>,
}

impl InMemoryCas {
    pub fn new() -> Self {
        Self::default()
    }

    fn hash(data: &[u8]) -> ChunkHash {
        blake3::hash(data)
    }
}

impl CasStore for InMemoryCas {
    fn put_chunk(&self, data: Vec<u8>) -> ChunkHash {
        let hash = Self::hash(&data);
        self.chunks.write().entry(hash).or_insert(data);
        hash
    }

    fn get_chunk(&self, hash: &ChunkHash) -> Option<Vec<u8>> {
        self.chunks.read().get(hash).cloned()
    }

    fn put_object(&self, chunks: Vec<ChunkHash>) -> ChunkHash {
        let mut hasher = blake3::Hasher::new();
        for chunk in &chunks {
            hasher.update(chunk.as_bytes());
        }
        let hash = hasher.finalize();
        let object = Object { hash, chunks };
        self.objects.write().insert(hash, object);
        hash
    }

    fn get_object(&self, hash: &ChunkHash) -> Option<Object> {
        self.objects.read().get(hash).cloned()
    }
}
