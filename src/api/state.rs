//! Shared application state injected into all handlers.

use std::sync::Arc;

use crate::store::sqlite::SqliteMetadataStore;
use crate::store::RocksDbCasStore;

/// Shared application state available to all route handlers.
///
/// Wraps the storage backends in `Arc` so the state can be cheaply cloned
/// into each handler by axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    /// Content-addressed storage (chunks, tree nodes, directory nodes).
    pub cas: Arc<RocksDbCasStore>,
    /// Metadata storage (versions, refs, path index).
    pub meta: Arc<SqliteMetadataStore>,
}

impl AppState {
    /// Create a new `AppState` from pre-opened stores.
    pub fn new(cas: RocksDbCasStore, meta: SqliteMetadataStore) -> Self {
        Self {
            cas: Arc::new(cas),
            meta: Arc::new(meta),
        }
    }
}
