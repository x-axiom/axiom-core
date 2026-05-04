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
    #[cfg(feature = "local")]
    pub fn local(
        cas: crate::store::RocksDbCasStore,
        meta: crate::store::SqliteMetadataStore,
    ) -> Self {
        Self::local_from_arcs(Arc::new(cas), Arc::new(meta))
    }

    /// Same as [`AppState::local`] but takes already-shared `Arc`s, so the
    /// caller can keep its own handle on the underlying stores (e.g. for
    /// repos that are not exposed through `AppState`).
    #[cfg(feature = "local")]
    pub fn local_from_arcs(
        cas: Arc<crate::store::RocksDbCasStore>,
        meta: Arc<crate::store::SqliteMetadataStore>,
    ) -> Self {
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

    /// Build a cloud `AppState` backed by S3 (chunks) + FoundationDB (metadata),
    /// **without** any cache layer.
    #[cfg(all(feature = "cloud", feature = "fdb"))]
    pub fn cloud_uncached(
        s3: crate::store::s3::S3ChunkStore,
        fdb: crate::store::fdb::FdbMetadataStore,
    ) -> Self {
        let fdb = Arc::new(fdb);
        Self {
            chunks: Arc::new(s3),
            trees: fdb.clone(),
            nodes: fdb.clone(),
            versions: fdb.clone(),
            refs: fdb.clone(),
            path_index: fdb,
        }
    }

    /// Build a cloud `AppState` backed by S3 (chunks) + FoundationDB (metadata),
    /// **with** a DragonflyDB / Redis cache layer wrapping both stores.
    #[cfg(all(feature = "cloud", feature = "fdb"))]
    pub fn cloud_cached<S, M>(
        chunks: crate::store::cache::CachedChunkStore<S>,
        meta: crate::store::cache::CachedMetadataStore<M>,
    ) -> Self
    where
        S: crate::store::ChunkStore + 'static,
        M: crate::store::TreeStore
            + crate::store::NodeStore
            + crate::store::VersionRepo
            + crate::store::RefRepo
            + crate::store::PathIndexRepo
            + Send
            + Sync
            + 'static,
    {
        let meta = Arc::new(meta);
        Self {
            chunks: Arc::new(chunks),
            trees: meta.clone(),
            nodes: meta.clone(),
            versions: meta.clone(),
            refs: meta.clone(),
            path_index: meta,
        }
    }
}
