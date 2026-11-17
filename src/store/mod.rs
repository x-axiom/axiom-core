pub mod traits;
pub mod memory;
#[cfg(feature = "local")]
pub mod rocksdb;
#[cfg(feature = "local")]
pub mod sqlite;
#[cfg(feature = "cloud")]
pub mod s3;
#[cfg(feature = "cloud")]
pub mod cache;
#[cfg(feature = "fdb")]
pub mod fdb;
pub mod factory;

pub use traits::{ChunkStore, TreeStore, NodeStore, VersionRepo, RefRepo, PathIndexRepo, PathEntry, SyncStore, ReachableObjects, Workspace, WorkspaceRepo, ObjectManifestRepo, Remote, RemoteRepo, WtCacheRepo, WtCacheEntry};
pub use memory::{
    InMemoryChunkStore, InMemoryTreeStore, InMemoryNodeStore,
    InMemoryVersionRepo, InMemoryRefRepo, InMemoryPathIndex, InMemoryCas,
    InMemorySyncStore, InMemoryManifestStore, InMemoryWtCache, HashMapWtCache,
};
#[cfg(feature = "local")]
pub use rocksdb::RocksDbCasStore;
#[cfg(feature = "local")]
pub use sqlite::SqliteMetadataStore;
#[cfg(feature = "cloud")]
pub use s3::{S3ChunkStore, S3Config, StaticCredentials};
#[cfg(feature = "cloud")]
pub use cache::{CacheClient, CacheConfig, CacheStats, CachedChunkStore, CachedMetadataStore};
#[cfg(feature = "fdb")]
pub use fdb::{FdbMetadataStore, FdbConfig};
pub use factory::{StoreFactory, StoreConfig, BackendConfig, LocalConfig, CloudConfig};

// ---------------------------------------------------------------------------
// LocalSyncStore — SyncStore backed by RocksDB + SQLite
// ---------------------------------------------------------------------------

#[cfg(feature = "local")]
pub struct LocalSyncStore {
    pub cas: std::sync::Arc<rocksdb::RocksDbCasStore>,
    pub meta: std::sync::Arc<sqlite::SqliteMetadataStore>,
}

#[cfg(feature = "local")]
impl LocalSyncStore {
    pub fn new(
        cas: std::sync::Arc<rocksdb::RocksDbCasStore>,
        meta: std::sync::Arc<sqlite::SqliteMetadataStore>,
    ) -> Self {
        Self { cas, meta }
    }
}

#[cfg(feature = "local")]
impl SyncStore for LocalSyncStore {
    fn collect_reachable_with_have(
        &self,
        want: &[crate::model::VersionId],
        have: &std::collections::HashSet<crate::model::VersionId>,
    ) -> crate::error::CasResult<ReachableObjects> {
        use crate::sync::reachable::{CancelToken, collect_reachable};

        collect_reachable(
            want,
            have,
            self.meta.as_ref(),
            self.cas.as_ref(),
            self.cas.as_ref(),
            &CancelToken::new(),
        )
    }

    fn list_all_version_ids(&self) -> crate::error::CasResult<Vec<crate::model::VersionId>> {
        // Call the inherent SQLite implementation explicitly: `Arc<T>` also
        // satisfies `VersionRepo`, whose default would shadow it.
        self.meta.as_ref().list_all_version_ids()
    }
}

// ---------------------------------------------------------------------------
// CompositeSyncStore — SyncStore over any backend's trait objects
// ---------------------------------------------------------------------------

/// [`SyncStore`] assembled from the trait objects held by `AppState`.
///
/// This is what makes the sync protocol backend-agnostic: the same graph walk
/// works for RocksDB+SQLite (local) and S3+FoundationDB (cloud).
pub struct CompositeSyncStore {
    pub versions: std::sync::Arc<dyn VersionRepo>,
    pub trees: std::sync::Arc<dyn TreeStore>,
    pub nodes: std::sync::Arc<dyn NodeStore>,
    pub chunks: std::sync::Arc<dyn ChunkStore>,
}

impl CompositeSyncStore {
    pub fn new(
        versions: std::sync::Arc<dyn VersionRepo>,
        trees: std::sync::Arc<dyn TreeStore>,
        nodes: std::sync::Arc<dyn NodeStore>,
        chunks: std::sync::Arc<dyn ChunkStore>,
    ) -> Self {
        Self { versions, trees, nodes, chunks }
    }
}

impl SyncStore for CompositeSyncStore {
    fn collect_reachable_with_have(
        &self,
        want: &[crate::model::VersionId],
        have: &std::collections::HashSet<crate::model::VersionId>,
    ) -> crate::error::CasResult<ReachableObjects> {
        use crate::sync::reachable::{CancelToken, collect_reachable};

        collect_reachable(
            want,
            have,
            self.versions.as_ref(),
            self.trees.as_ref(),
            self.nodes.as_ref(),
            &CancelToken::new(),
        )
    }

    fn list_all_version_ids(&self) -> crate::error::CasResult<Vec<crate::model::VersionId>> {
        self.versions.list_all_version_ids()
    }
}
