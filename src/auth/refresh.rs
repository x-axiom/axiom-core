//! Refresh-token storage abstraction.
//!
//! Access tokens are stateless (verified by public key); refresh tokens must be
//! revocable, so every refresh token has a server-side record keyed by its
//! `jti`.
//!
//! Two implementations exist:
//!
//! - [`MemoryRefreshTokenStore`] — process-local, used by local/dev mode and by
//!   tests. Records are lost on restart, which is acceptable for single-user
//!   local operation.
//! - `FdbRefreshTokenStore` — durable, shared across replicas, used whenever
//!   the `fdb` feature is enabled and a cluster handle is available.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::error::CasResult;

/// Persisted state for one refresh token.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RefreshRecord {
    pub user_id: String,
    /// Absolute expiry (Unix seconds), re-checked server-side.
    pub exp: u64,
}

/// Durable, revocable storage for refresh tokens.
pub trait RefreshTokenStore: Send + Sync {
    /// Insert or replace the record for `jti`.
    fn put(&self, jti: &str, record: RefreshRecord) -> CasResult<()>;
    /// Atomically read and remove the record (single-use semantics).
    fn take(&self, jti: &str) -> CasResult<Option<RefreshRecord>>;
    /// Remove the record if present. Used for explicit logout / cleanup.
    fn revoke(&self, jti: &str) -> CasResult<()>;
}

// ---------------------------------------------------------------------------
// In-memory store
// ---------------------------------------------------------------------------

/// Process-local refresh-token store.
///
/// Suitable for local single-user mode and tests; **not** suitable for
/// multi-replica deployments, where logout/revocation would only affect the
/// replica that served the request.
#[derive(Default)]
pub struct MemoryRefreshTokenStore {
    inner: Mutex<HashMap<String, RefreshRecord>>,
}

impl MemoryRefreshTokenStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of live records (test/diagnostic helper).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl RefreshTokenStore for MemoryRefreshTokenStore {
    fn put(&self, jti: &str, record: RefreshRecord) -> CasResult<()> {
        self.inner.lock().insert(jti.to_owned(), record);
        Ok(())
    }

    fn take(&self, jti: &str) -> CasResult<Option<RefreshRecord>> {
        Ok(self.inner.lock().remove(jti))
    }

    fn revoke(&self, jti: &str) -> CasResult<()> {
        self.inner.lock().remove(jti);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FoundationDB store
// ---------------------------------------------------------------------------

#[cfg(feature = "fdb")]
pub use fdb_store::FdbRefreshTokenStore;

#[cfg(feature = "fdb")]
mod fdb_store {
    use std::sync::Arc;

    use foundationdb::Database;

    use super::{RefreshRecord, RefreshTokenStore};
    use crate::error::{CasError, CasResult};

    /// FDB key prefix for refresh-token records.
    const RT_PREFIX: &str = "rt\x00";

    fn rt_key(jti: &str) -> Vec<u8> {
        format!("{RT_PREFIX}{jti}").into_bytes()
    }

    /// FoundationDB-backed refresh-token store (durable, multi-replica safe).
    pub struct FdbRefreshTokenStore {
        db: Arc<Database>,
    }

    impl FdbRefreshTokenStore {
        pub fn new(db: Arc<Database>) -> Self {
            Self { db }
        }

        /// Open the cluster (or the platform-default cluster when `None`) and
        /// build a store. Mirrors `TenantRepo::new`.
        pub fn open(cluster_file: Option<&str>) -> CasResult<Self> {
            let db = match cluster_file {
                Some(path) => Database::from_path(path)
                    .map_err(|e| CasError::Store(format!("FDB open '{path}': {e}")))?,
                None => Database::default()
                    .map_err(|e| CasError::Store(format!("FDB open default cluster: {e}")))?,
            };
            Ok(Self::new(Arc::new(db)))
        }

        fn rt() -> CasResult<tokio::runtime::Handle> {
            tokio::runtime::Handle::try_current().map_err(|e| {
                CasError::Store(format!("FdbRefreshTokenStore requires a Tokio runtime: {e}"))
            })
        }
    }

    impl RefreshTokenStore for FdbRefreshTokenStore {
        fn put(&self, jti: &str, record: RefreshRecord) -> CasResult<()> {
            let key = rt_key(jti);
            let bytes = serde_json::to_vec(&record)?;
            let db = Arc::clone(&self.db);
            Self::rt()?
                .block_on(db.run(|trx, _| {
                    let key = key.clone();
                    let bytes = bytes.clone();
                    async move {
                        trx.set(&key, &bytes);
                        Ok(())
                    }
                }))
                .map_err(|e| CasError::Store(format!("FDB refresh store: {e}")))?;
            Ok(())
        }

        fn take(&self, jti: &str) -> CasResult<Option<RefreshRecord>> {
            let key = rt_key(jti);
            let db = Arc::clone(&self.db);
            let raw = Self::rt()?
                .block_on(db.run(|trx, _| {
                    let key = key.clone();
                    async move {
                        let value = trx.get(&key, false).await?;
                        trx.clear(&key);
                        Ok(value.map(|b| b.to_vec()))
                    }
                }))
                .map_err(|e| CasError::Store(format!("FDB refresh consume: {e}")))?;

            match raw {
                Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                None => Ok(None),
            }
        }

        fn revoke(&self, jti: &str) -> CasResult<()> {
            let key = rt_key(jti);
            let db = Arc::clone(&self.db);
            Self::rt()?
                .block_on(db.run(|trx, _| {
                    let key = key.clone();
                    async move {
                        trx.clear(&key);
                        Ok(())
                    }
                }))
                .map_err(|e| CasError::Store(format!("FDB refresh revoke: {e}")))?;
            Ok(())
        }
    }
}
