pub mod traits;
pub mod memory;
#[cfg(feature = "local")]
pub mod rocksdb;
#[cfg(feature = "local")]
pub mod sqlite;

pub use traits::{ChunkStore, TreeStore, NodeStore, VersionRepo, RefRepo, PathIndexRepo, PathEntry, SyncStore, ReachableObjects, Workspace, WorkspaceRepo, ObjectManifestRepo};
pub use memory::{
    InMemoryChunkStore, InMemoryTreeStore, InMemoryNodeStore,
    InMemoryVersionRepo, InMemoryRefRepo, InMemoryPathIndex, InMemoryCas,
    InMemorySyncStore, InMemoryManifestStore,
};
#[cfg(feature = "local")]
pub use rocksdb::RocksDbCasStore;
#[cfg(feature = "local")]
pub use sqlite::SqliteMetadataStore;

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
        self.meta.list_all_version_ids()
    }
}
