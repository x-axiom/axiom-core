//! Shared application state injected into all handlers.

use std::sync::Arc;

use crate::store::traits::{ChunkStore, TreeStore, NodeStore, VersionRepo, RefRepo, PathIndexRepo};

/// Shared application state available to all route handlers.
///
/// Wraps the storage backends as trait objects in `Arc` so the state can be
/// cheaply cloned into each handler by axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    /// Content-addressed chunk storage.
    pub chunks: Arc<dyn ChunkStore>,
    /// Merkle tree node storage.
    pub trees: Arc<dyn TreeStore>,
    /// Directory tree node storage.
    pub nodes: Arc<dyn NodeStore>,
    /// Version DAG repository.
    pub versions: Arc<dyn VersionRepo>,
    /// Branch and tag ref repository.
    pub refs: Arc<dyn RefRepo>,
    /// Path-based metadata index.
    pub path_index: Arc<dyn PathIndexRepo>,
}

impl AppState {
    /// Build an `AppState` backed by RocksDB (CAS) + SQLite (metadata)
    /// for local single-user operation.
    pub fn local(
        cas: crate::store::RocksDbCasStore,
        meta: crate::store::SqliteMetadataStore,
    ) -> Self {
        let cas = Arc::new(cas);
        let meta = Arc::new(meta);
        Self {
            chunks: cas.clone(),
            trees: cas.clone(),
            nodes: cas,
            versions: meta.clone(),
            refs: meta.clone(),
            path_index: meta,
        }
    }

    /// Build a fully in-memory `AppState` for testing.
    pub fn memory() -> Self {
        Self {
            chunks: Arc::new(crate::store::InMemoryChunkStore::new()),
            trees: Arc::new(crate::store::InMemoryTreeStore::new()),
            nodes: Arc::new(crate::store::InMemoryNodeStore::new()),
            versions: Arc::new(crate::store::InMemoryVersionRepo::new()),
            refs: Arc::new(crate::store::InMemoryRefRepo::new()),
            path_index: Arc::new(crate::store::InMemoryPathIndex::new()),
        }
    }
}
