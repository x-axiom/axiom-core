//! DragonflyDB / Redis cache layer.
//!
//! Provides transparent caching decorators for the core storage traits.
//! Cache failures are **non-fatal**: errors are logged at WARN level and the
//! operation falls through to the underlying store automatically.
//!
//! # TTL tiers
//!
//! | Object type | TTL | Rationale |
//! |-------------|-----|-----------|
//! | Chunks | 24 h | Large blobs; content-addressed immutable |
//! | Trees / nodes / versions / path entries | 1 h | Content-addressed immutable |
//! | Refs (branches / tags) | 30 s | Mutable pointers; short TTL for freshness |
//!
//! # Degradation policy
//!
//! Any error from the cache tier (connection failure, timeout, serialization
//! failure) is logged at WARN level and the operation continues against the
//! underlying store.  Callers never observe a cache error.
//!
//! # Example
//!
//! ```ignore
//! let cache = Arc::new(CacheClient::new(CacheConfig::default())?);
//! let chunks = CachedChunkStore::new(s3_store, Arc::clone(&cache));
//! let meta   = CachedMetadataStore::new(fdb_store, Arc::clone(&cache));
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fred::prelude::*;

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, NodeEntry, Ref, RefKind, TreeNode, VersionId, VersionNode};
use super::traits::{ChunkStore, NodeStore, PathEntry, PathIndexRepo, RefRepo, TreeStore, VersionRepo};

// ---------------------------------------------------------------------------
// TTL constants
// ---------------------------------------------------------------------------

/// Content chunks are immutable — 24 h.
const CHUNK_TTL: i64 = 86_400;
/// Trees, nodes, versions, and path entries are content-addressed — 1 h.
const META_TTL: i64 = 3_600;
/// Refs are mutable pointers — 30 s.
const REF_TTL: i64 = 30;

// ---------------------------------------------------------------------------
// CacheConfig
// ---------------------------------------------------------------------------

/// Configuration for [`CacheClient`].
#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// DragonflyDB / Redis URL, e.g. `"redis://localhost:6379"`.
    pub url: String,
    /// Number of connections in the pool. Must be ≥ 1.
    pub pool_size: usize,
    /// Key prefix prepended to every cache key.  Use a per-deployment value
    /// to avoid collisions when multiple Axiom instances share one cache.
    pub key_prefix: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            pool_size: 4,
            key_prefix: "axiom".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// CacheStats
// ---------------------------------------------------------------------------

/// Snapshot of cache operation counters, suitable for metrics export.
#[derive(Clone, Debug, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    /// Count of errors (connection failures, serialization errors, etc.).
    pub errors: u64,
}

// ---------------------------------------------------------------------------
// CacheClient
// ---------------------------------------------------------------------------

/// Low-level cache client wrapping a `fred` connection pool.
///
/// All operations are synchronous from the caller's perspective; async I/O
/// is driven by the Tokio runtime via `Handle::block_on`.
pub struct CacheClient {
    pool: RedisPool,
    prefix: String,
    hits: AtomicU64,
    misses: AtomicU64,
    errors: AtomicU64,
}

impl CacheClient {
    /// Connect to a DragonflyDB / Redis instance and return a ready client.
    ///
    /// Returns `CasError::Store` if the URL is invalid or the connection
    /// could not be established.  The caller may choose to fall back to an
    /// uncached store when this fails.
    pub fn new(cfg: CacheConfig) -> CasResult<Self> {
        let config = RedisConfig::from_url(&cfg.url)
            .map_err(|e| CasError::Store(format!("cache: invalid URL '{}': {e}", cfg.url)))?;

        let pool = Builder::from_config(config)
            .build_pool(cfg.pool_size.max(1))
            .map_err(|e| CasError::Store(format!("cache: pool build failed: {e}")))?;

        let rt = Self::rt()?;
        rt.block_on(pool.init())
            .map_err(|e| CasError::Store(format!("cache: pool init failed: {e}")))?;

        Ok(Self {
            pool,
            prefix: cfg.key_prefix,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    fn rt() -> CasResult<tokio::runtime::Handle> {
        tokio::runtime::Handle::try_current()
            .map_err(|e| CasError::Store(format!("CacheClient requires a Tokio runtime: {e}")))
    }

    fn key(&self, segment: &str, id: &str) -> String {
        format!("{}:{}:{}", self.prefix, segment, id)
    }

    // -----------------------------------------------------------------------
    // Raw byte operations
    // -----------------------------------------------------------------------

    /// Get raw bytes from cache. Returns `None` on cache miss or any error.
    pub fn get_bytes(&self, segment: &str, id: &str) -> Option<Vec<u8>> {
        let k = self.key(segment, id);
        let rt = Self::rt().ok()?;
        match rt.block_on(self.pool.get::<Option<Vec<u8>>, _>(k.as_str())) {
            Ok(Some(v)) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(v)
            }
            Ok(None) => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(key = %k, error = %e, "cache GET failed, treating as miss");
                None
            }
        }
    }

    /// Write raw bytes to cache with the given TTL (seconds).
    /// Errors are logged at WARN and silently ignored.
    pub fn set_bytes(&self, segment: &str, id: &str, value: &[u8], ttl_secs: i64) {
        let k = self.key(segment, id);
        let data = value.to_vec();
        let Ok(rt) = Self::rt() else { return };
        if let Err(e) = rt.block_on(
            self.pool.set::<(), _, _>(
                k.as_str(),
                data,
                Some(Expiration::EX(ttl_secs)),
                None,
                false,
            ),
        ) {
            self.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(key = %k, error = %e, "cache SET failed");
        }
    }

    /// Delete a key from cache. Errors are logged and ignored.
    pub fn del(&self, segment: &str, id: &str) {
        let k = self.key(segment, id);
        let Ok(rt) = Self::rt() else { return };
        if let Err(e) = rt.block_on(self.pool.del::<(), _>(k.as_str())) {
            self.errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(key = %k, error = %e, "cache DEL failed");
        }
    }

    /// Check whether a key exists without fetching its value.
    ///
    /// Returns `Some(true)` / `Some(false)` on success, `None` on error.
    pub fn key_exists(&self, segment: &str, id: &str) -> Option<bool> {
        let k = self.key(segment, id);
        let rt = Self::rt().ok()?;
        match rt.block_on(self.pool.exists::<i64, _>(k.as_str())) {
            Ok(n) if n > 0 => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(true)
            }
            Ok(_) => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                Some(false)
            }
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(key = %k, error = %e, "cache EXISTS failed, treating as miss");
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Typed JSON helpers
    // -----------------------------------------------------------------------

    fn get_json<T: serde::de::DeserializeOwned>(&self, segment: &str, id: &str) -> Option<T> {
        let bytes = self.get_bytes(segment, id)?;
        match serde_json::from_slice::<T>(&bytes) {
            Ok(v) => Some(v),
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(segment, id, error = %e, "cache: JSON deserialize failed, treating as miss");
                None
            }
        }
    }

    fn set_json<T: serde::Serialize>(&self, segment: &str, id: &str, val: &T, ttl: i64) {
        match serde_json::to_vec(val) {
            Ok(bytes) => self.set_bytes(segment, id, &bytes, ttl),
            Err(e) => {
                tracing::warn!(segment, id, error = %e, "cache: JSON serialize failed, skipping cache write");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Observability
    // -----------------------------------------------------------------------

    /// Return a snapshot of cache operation counters.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// CachedChunkStore
// ---------------------------------------------------------------------------

/// A [`ChunkStore`] decorator that caches chunk data in DragonflyDB.
///
/// - `put_chunk`: writes to the inner store, then writes to cache (CHUNK_TTL).
/// - `get_chunk`: checks cache first; on miss, reads inner and back-fills.
/// - `has_chunk`: checks cache key existence; falls through to inner on miss.
///   Negative results are **not** cached (a chunk could be added at any time).
pub struct CachedChunkStore<S> {
    inner: S,
    cache: Arc<CacheClient>,
}

impl<S: ChunkStore> CachedChunkStore<S> {
    pub fn new(inner: S, cache: Arc<CacheClient>) -> Self {
        Self { inner, cache }
    }

    pub fn cache(&self) -> &CacheClient {
        &self.cache
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn chunk_id(hash: &ChunkHash) -> String {
        hash.to_hex().to_string()
    }
}

impl<S: ChunkStore> ChunkStore for CachedChunkStore<S> {
    fn put_chunk(&self, data: Vec<u8>) -> CasResult<ChunkHash> {
        let hash = self.inner.put_chunk(data.clone())?;
        self.cache.set_bytes("c", &Self::chunk_id(&hash), &data, CHUNK_TTL);
        Ok(hash)
    }

    fn get_chunk(&self, hash: &ChunkHash) -> CasResult<Option<Vec<u8>>> {
        let id = Self::chunk_id(hash);
        if let Some(bytes) = self.cache.get_bytes("c", &id) {
            return Ok(Some(bytes));
        }
        let result = self.inner.get_chunk(hash)?;
        if let Some(ref bytes) = result {
            self.cache.set_bytes("c", &id, bytes, CHUNK_TTL);
        }
        Ok(result)
    }

    fn has_chunk(&self, hash: &ChunkHash) -> CasResult<bool> {
        let id = Self::chunk_id(hash);
        // If cache confirms existence, skip the inner store entirely.
        if let Some(true) = self.cache.key_exists("c", &id) {
            return Ok(true);
        }
        // Negative results are not cached — chunks can be written at any time.
        self.inner.has_chunk(hash)
    }
}

// ---------------------------------------------------------------------------
// CachedMetadataStore
// ---------------------------------------------------------------------------

/// A metadata store decorator that caches trees, nodes, versions, refs, and
/// path entries in DragonflyDB.
///
/// # Cache semantics per object type
///
/// - **Trees / nodes / versions / path entries**: content-addressed and
///   immutable once written → cached with META_TTL (1 h).
/// - **Refs**: mutable branch/tag pointers → cached with REF_TTL (30 s).
///   Cache is kept consistent on `put_ref`, `delete_ref`, and
///   `compare_and_swap_ref`.
/// - **`list_history`**: implemented as a cached `get_version` chain walk so
///   subsequent calls benefit from the warmed version cache.
/// - **`list_refs`** / **`list_directory`**: `list_refs` is not cached
///   (infrequent, invalidation is complex); `list_directory` is cached because
///   versions are immutable — a given (version_id, dir_path) pair always
///   returns the same entries.
pub struct CachedMetadataStore<M> {
    inner: M,
    cache: Arc<CacheClient>,
}

impl<M> CachedMetadataStore<M>
where
    M: TreeStore + NodeStore + VersionRepo + RefRepo + PathIndexRepo + Send + Sync,
{
    pub fn new(inner: M, cache: Arc<CacheClient>) -> Self {
        Self { inner, cache }
    }

    pub fn cache(&self) -> &CacheClient {
        &self.cache
    }

    pub fn inner(&self) -> &M {
        &self.inner
    }
}

// --- TreeStore ---

impl<M> TreeStore for CachedMetadataStore<M>
where
    M: TreeStore + NodeStore + VersionRepo + RefRepo + PathIndexRepo + Send + Sync,
{
    fn put_tree_node(&self, node: &TreeNode) -> CasResult<()> {
        self.inner.put_tree_node(node)?;
        self.cache.set_json("t", &node.hash.to_hex().to_string(), node, META_TTL);
        Ok(())
    }

    fn get_tree_node(&self, hash: &ChunkHash) -> CasResult<Option<TreeNode>> {
        let id = hash.to_hex().to_string();
        if let Some(v) = self.cache.get_json("t", &id) {
            return Ok(Some(v));
        }
        let result = self.inner.get_tree_node(hash)?;
        if let Some(ref node) = result {
            self.cache.set_json("t", &id, node, META_TTL);
        }
        Ok(result)
    }
}

// --- NodeStore ---

impl<M> NodeStore for CachedMetadataStore<M>
where
    M: TreeStore + NodeStore + VersionRepo + RefRepo + PathIndexRepo + Send + Sync,
{
    fn put_node(&self, entry: &NodeEntry) -> CasResult<()> {
        self.inner.put_node(entry)?;
        self.cache.set_json("n", &entry.hash.to_hex().to_string(), entry, META_TTL);
        Ok(())
    }

    fn get_node(&self, hash: &ChunkHash) -> CasResult<Option<NodeEntry>> {
        let id = hash.to_hex().to_string();
        if let Some(v) = self.cache.get_json("n", &id) {
            return Ok(Some(v));
        }
        let result = self.inner.get_node(hash)?;
        if let Some(ref entry) = result {
            self.cache.set_json("n", &id, entry, META_TTL);
        }
        Ok(result)
    }
}

// --- VersionRepo ---

impl<M> VersionRepo for CachedMetadataStore<M>
where
    M: TreeStore + NodeStore + VersionRepo + RefRepo + PathIndexRepo + Send + Sync,
{
    fn put_version(&self, version: &VersionNode) -> CasResult<()> {
        self.inner.put_version(version)?;
        self.cache.set_json("v", version.id.as_str(), version, META_TTL);
        Ok(())
    }

    fn get_version(&self, id: &VersionId) -> CasResult<Option<VersionNode>> {
        if let Some(v) = self.cache.get_json("v", id.as_str()) {
            return Ok(Some(v));
        }
        let result = self.inner.get_version(id)?;
        if let Some(ref ver) = result {
            self.cache.set_json("v", id.as_str(), ver, META_TTL);
        }
        Ok(result)
    }

    /// Walk the parent chain using the cached `get_version` so that warm-cache
    /// runs don't hit the backing store for every ancestor.
    fn list_history(&self, from: &VersionId, limit: usize) -> CasResult<Vec<VersionNode>> {
        let mut result = Vec::new();
        let mut current = Some(from.clone());
        while let Some(id) = current {
            if result.len() >= limit {
                break;
            }
            match self.get_version(&id)? {
                Some(node) => {
                    let next = node.parents.first().cloned();
                    result.push(node);
                    current = next;
                }
                None => break,
            }
        }
        Ok(result)
    }
}

// --- RefRepo ---

impl<M> RefRepo for CachedMetadataStore<M>
where
    M: TreeStore + NodeStore + VersionRepo + RefRepo + PathIndexRepo + Send + Sync,
{
    fn put_ref(&self, r: &Ref) -> CasResult<()> {
        self.inner.put_ref(r)?;
        self.cache.set_json("r", &r.name, r, REF_TTL);
        Ok(())
    }

    fn get_ref(&self, name: &str) -> CasResult<Option<Ref>> {
        if let Some(r) = self.cache.get_json("r", name) {
            return Ok(Some(r));
        }
        let result = self.inner.get_ref(name)?;
        if let Some(ref r) = result {
            self.cache.set_json("r", name, r, REF_TTL);
        }
        Ok(result)
    }

    fn delete_ref(&self, name: &str) -> CasResult<()> {
        self.inner.delete_ref(name)?;
        self.cache.del("r", name);
        Ok(())
    }

    /// Not cached — listing all refs is infrequent and hard to invalidate
    /// correctly when any ref changes.
    fn list_refs(&self, kind: Option<RefKind>) -> CasResult<Vec<Ref>> {
        self.inner.list_refs(kind)
    }

    /// After a successful CAS, update the cache to keep it consistent.
    fn compare_and_swap_ref(
        &self,
        name: &str,
        expected_old: Option<&VersionId>,
        new_ref: &Ref,
    ) -> CasResult<bool> {
        let swapped = self.inner.compare_and_swap_ref(name, expected_old, new_ref)?;
        if swapped {
            self.cache.set_json("r", name, new_ref, REF_TTL);
        }
        Ok(swapped)
    }
}

// --- PathIndexRepo ---

impl<M> PathIndexRepo for CachedMetadataStore<M>
where
    M: TreeStore + NodeStore + VersionRepo + RefRepo + PathIndexRepo + Send + Sync,
{
    fn put_path_entry(
        &self,
        version_id: &VersionId,
        path: &str,
        node_hash: &ChunkHash,
        is_directory: bool,
    ) -> CasResult<()> {
        self.inner.put_path_entry(version_id, path, node_hash, is_directory)?;
        let entry = PathEntry {
            path: path.to_string(),
            node_hash: *node_hash,
            is_directory,
        };
        let entry_id = format!("{}:{}", version_id.as_str(), path);
        self.cache.set_json("p", &entry_id, &entry, META_TTL);
        Ok(())
    }

    fn get_by_path(
        &self,
        version_id: &VersionId,
        path: &str,
    ) -> CasResult<Option<PathEntry>> {
        let entry_id = format!("{}:{}", version_id.as_str(), path);
        if let Some(v) = self.cache.get_json::<PathEntry>("p", &entry_id) {
            return Ok(Some(v));
        }
        let result = self.inner.get_by_path(version_id, path)?;
        if let Some(ref entry) = result {
            self.cache.set_json("p", &entry_id, entry, META_TTL);
        }
        Ok(result)
    }

    /// Cached — versions are immutable so (version_id, dir_path) → entries
    /// is a stable mapping that never needs invalidation.
    fn list_directory(
        &self,
        version_id: &VersionId,
        dir_path: &str,
    ) -> CasResult<Vec<PathEntry>> {
        let dir_id = format!("{}:{}", version_id.as_str(), dir_path);
        if let Some(v) = self.cache.get_json::<Vec<PathEntry>>("pd", &dir_id) {
            return Ok(v);
        }
        let result = self.inner.list_directory(version_id, dir_path)?;
        self.cache.set_json("pd", &dir_id, &result, META_TTL);
        Ok(result)
    }
}
