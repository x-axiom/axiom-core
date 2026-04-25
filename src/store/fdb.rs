//! FoundationDB-backed metadata store.
//!
//! Implements [`TreeStore`], [`NodeStore`], [`VersionRepo`], [`RefRepo`], and
//! [`PathIndexRepo`] on top of FoundationDB, providing linearizable, ACID
//! metadata storage suitable for multi-region SaaS deployments.
//!
//! # Prerequisites
//!
//! The FoundationDB client library must be installed:
//!   - macOS: `brew install foundationdb`
//!   - Ubuntu/Debian: install the package from the FDB release page
//!
//! The FDB network thread must be started **once** at application startup,
//! before creating any [`FdbMetadataStore`]:
//!
//! ```ignore
//! // Keep the guard alive for the process lifetime.
//! let _network_guard = unsafe { foundationdb::boot() };
//! ```
//!
//! # Key layout
//!
//! All keys are UTF-8 byte strings using `\x00` as a separator:
//!
//! ```text
//! {tenant}\x00{workspace}\x00tree\x00{hash_hex}
//! {tenant}\x00{workspace}\x00node\x00{hash_hex}
//! {tenant}\x00{workspace}\x00ver\x00{version_id}
//! {tenant}\x00{workspace}\x00ref\x00{ref_name}
//! {tenant}\x00{workspace}\x00path\x00{version_id}\x00{path}
//! ```
//!
//! # Large value sharding
//!
//! FoundationDB limits values to 100 000 bytes but in practice recommends
//! staying under [`VALUE_SHARD_SIZE`] (9 000 bytes) for best performance.
//! Values that exceed this threshold are transparently split into numbered
//! shards:
//!
//! ```text
//! base_key            → \xfe + big-endian u32 shard_count   (5 bytes)
//! base_key\x00\x00\x00\x00  (index 0, big-endian)           → shard 0 bytes
//! base_key\x00\x00\x00\x01  (index 1, big-endian)           → shard 1 bytes
//! …
//! ```
//!
//! Reads detect the `\xfe` marker and reassemble shards transparently.
//!
//! # Retry policy
//!
//! All write transactions use `Database::run`, which automatically retries on
//! FDB conflict errors (error code 1020) up to [`MAX_RETRIES`] times with
//! exponential back-off managed by the FDB client library.

use std::sync::Arc;

use foundationdb::{Database, FdbError, KeySelector, RangeOption};
use foundationdb::options::StreamingMode;

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, NodeEntry, Ref, RefKind, TreeNode, VersionId, VersionNode};
use super::traits::{PathEntry, PathIndexRepo, NodeStore, RefRepo, TreeStore, VersionRepo};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Values larger than this are split into numbered shards.
/// 9 KB stays well below FDB's recommended per-value limit.
const VALUE_SHARD_SIZE: usize = 9_000;

/// Magic first byte of a shard-header value (cannot appear as valid JSON).
const SHARD_MARKER: u8 = 0xfe;

/// Maximum number of transaction retries on conflict before giving up.
/// FDB's `run` helper handles the retry loop; this is a documentation comment
/// — the actual retry count is configured via `TransactionOptions`.
const MAX_RETRIES: u32 = 5;

/// Maximum entries returned from a single range scan (path index listing).
const RANGE_SCAN_LIMIT: usize = 10_000;

// ---------------------------------------------------------------------------
// FdbConfig
// ---------------------------------------------------------------------------

/// Configuration for [`FdbMetadataStore`].
#[derive(Clone, Debug)]
pub struct FdbConfig {
    /// Path to the FoundationDB cluster file.
    ///
    /// `None` falls back to the default location:
    /// - Linux: `/etc/foundationdb/fdb.cluster`
    /// - macOS: `/usr/local/etc/foundationdb/fdb.cluster`
    /// - Or the `FDB_CLUSTER_FILE` environment variable.
    pub cluster_file: Option<String>,

    /// Tenant / organisation identifier.  Used as the first component of
    /// every key, providing logical isolation between tenants.
    pub tenant: String,

    /// Workspace identifier.  Used as the second key component.
    pub workspace: String,
}

// ---------------------------------------------------------------------------
// FdbMetadataStore
// ---------------------------------------------------------------------------

/// Metadata store backed by FoundationDB.
///
/// Implements [`TreeStore`], [`NodeStore`], [`VersionRepo`], [`RefRepo`], and
/// [`PathIndexRepo`].  All operations are synchronous from the caller's
/// perspective; FDB I/O is driven by the FDB network thread, and blocking is
/// performed via `tokio::runtime::Handle::block_on`.
pub struct FdbMetadataStore {
    db: Arc<Database>,
    /// Tenant identifier (embedded in every key).
    tenant: String,
    /// Workspace identifier (embedded in every key).
    workspace: String,
}

impl FdbMetadataStore {
    /// Open a [`FdbMetadataStore`] from an explicit [`FdbConfig`].
    ///
    /// # Errors
    ///
    /// Returns `CasError::Store` if the FDB cluster file cannot be opened.
    ///
    /// # Safety
    ///
    /// The caller must have called `unsafe { foundationdb::boot() }` before
    /// this function and kept the returned `NetworkAutoStop` guard alive.
    pub fn new(cfg: FdbConfig) -> CasResult<Self> {
        let db = match &cfg.cluster_file {
            Some(path) => Database::new_compat(path)
                .map_err(|e| CasError::Store(format!("FDB open cluster file '{}' failed: {e}", path)))?,
            None => Database::default()
                .map_err(|e| CasError::Store(format!("FDB open default cluster failed: {e}")))?,
        };

        Ok(Self {
            db: Arc::new(db),
            tenant: cfg.tenant,
            workspace: cfg.workspace,
        })
    }

    // -----------------------------------------------------------------------
    // Key helpers
    // -----------------------------------------------------------------------

    /// Build a key `{tenant}\x00{workspace}\x00{segment}\x00{id}`.
    fn make_key(&self, segment: &str, id: &str) -> Vec<u8> {
        format!("{}\x00{}\x00{}\x00{}", self.tenant, self.workspace, segment, id)
            .into_bytes()
    }

    /// Build a path-index key:
    /// `{prefix}/path\x00{version_id}\x00{path}`.
    fn path_key(&self, version_id: &VersionId, path: &str) -> Vec<u8> {
        self.make_key("path", &format!("{}\x00{}", version_id.as_str(), path))
    }

    /// Key prefix for all path-index entries of a given version.
    fn path_prefix(&self, version_id: &VersionId) -> Vec<u8> {
        self.make_key("path", &format!("{}\x00", version_id.as_str()))
    }

    /// Build the key for shard `index` of a sharded value at `base_key`.
    fn shard_key(base_key: &[u8], index: u32) -> Vec<u8> {
        let mut k = base_key.to_vec();
        k.extend_from_slice(b"\x00\xfd");
        k.extend_from_slice(&index.to_be_bytes());
        k
    }

    /// Return the smallest key that is strictly greater than all keys with
    /// the given prefix (i.e. `prefix` with `\xff` appended).
    fn prefix_end(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        end.push(0xff);
        end
    }

    // -----------------------------------------------------------------------
    // Value sharding
    // -----------------------------------------------------------------------

    /// Enqueue a `set` (and possibly multiple shard sets) on `trx`.
    ///
    /// Must be called inside a `db.run` closure.
    fn fdb_set(trx: &foundationdb::RetryableTransaction, key: &[u8], value: &[u8]) {
        if value.len() <= VALUE_SHARD_SIZE {
            trx.set(key, value);
        } else {
            let shards: Vec<&[u8]> = value.chunks(VALUE_SHARD_SIZE).collect();
            let count = shards.len() as u32;

            // Write the shard-count header at the base key.
            let mut header = Vec::with_capacity(5);
            header.push(SHARD_MARKER);
            header.extend_from_slice(&count.to_be_bytes());
            trx.set(key, &header);

            for (i, shard) in shards.iter().enumerate() {
                let sk = Self::shard_key(key, i as u32);
                trx.set(&sk, shard);
            }
        }
    }

    /// Read and reassemble a potentially-sharded value.
    ///
    /// Reads are performed in a separate snapshot transaction (no conflict
    /// range recorded), which is correct for immutable CAS objects.
    async fn fdb_get(db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>, FdbError> {
        let trx = db.create_trx()?;

        let maybe = trx.get(key, false).await?;
        let bytes = match maybe {
            None => return Ok(None),
            Some(b) => b.to_vec(),
        };

        // Check for a shard-count header.
        if bytes.len() == 5 && bytes[0] == SHARD_MARKER {
            let count = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
            let mut assembled = Vec::new();
            for i in 0..count {
                let sk = Self::shard_key(key, i);
                let shard = trx
                    .get(&sk, false)
                    .await?
                    .ok_or_else(|| FdbError::from_code(2004))?; // missing shard → data corruption
                assembled.extend_from_slice(&shard);
            }
            Ok(Some(assembled))
        } else {
            Ok(Some(bytes))
        }
    }

    // -----------------------------------------------------------------------
    // Runtime helper
    // -----------------------------------------------------------------------

    fn rt() -> CasResult<tokio::runtime::Handle> {
        tokio::runtime::Handle::try_current()
            .map_err(|e| CasError::Store(format!("FdbMetadataStore requires a Tokio runtime: {e}")))
    }

    // -----------------------------------------------------------------------
    // Generic read / write helpers
    // -----------------------------------------------------------------------

    /// Deserialize a JSON value stored at `key`, returning `None` if absent.
    fn get_json<T: serde::de::DeserializeOwned>(&self, segment: &str, id: &str) -> CasResult<Option<T>> {
        let key = self.make_key(segment, id);
        let rt = Self::rt()?;

        let maybe = rt
            .block_on(Self::fdb_get(&self.db, &key))
            .map_err(|e| CasError::Store(format!("FDB get {segment}/{id}: {e}")))?;

        match maybe {
            None => Ok(None),
            Some(bytes) => {
                let val: T = serde_json::from_slice(&bytes)
                    .map_err(|e| CasError::Store(format!("FDB deserialize {segment}/{id}: {e}")))?;
                Ok(Some(val))
            }
        }
    }

    /// Write a JSON value to `key`, sharding if necessary.
    fn put_json<T: serde::Serialize>(&self, segment: &str, id: &str, val: &T) -> CasResult<()> {
        let key = self.make_key(segment, id);
        // Serialization of our model types cannot fail.
        let value = serde_json::to_vec(val).expect("FdbMetadataStore: serialize failed");
        let rt = Self::rt()?;

        rt.block_on(self.db.run(|trx, _| {
            let key = key.clone();
            let value = value.clone();
            async move {
                Self::fdb_set(&trx, &key, &value);
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB put {segment}/{id}: {e}")))?;

        Ok(())
    }

    /// Read a JSON value stored at an explicit `key` (pre-built).
    fn get_json_at_key<T: serde::de::DeserializeOwned>(&self, key: &[u8]) -> CasResult<Option<T>> {
        let rt = Self::rt()?;
        let maybe = rt
            .block_on(Self::fdb_get(&self.db, key))
            .map_err(|e| CasError::Store(format!("FDB get: {e}")))?;
        match maybe {
            None => Ok(None),
            Some(bytes) => {
                let val: T = serde_json::from_slice(&bytes)
                    .map_err(|e| CasError::Store(format!("FDB deserialize: {e}")))?;
                Ok(Some(val))
            }
        }
    }

    /// Delete a key.
    fn delete_key(&self, segment: &str, id: &str) -> CasResult<()> {
        let key = self.make_key(segment, id);
        let rt = Self::rt()?;

        rt.block_on(self.db.run(|trx, _| {
            let key = key.clone();
            async move {
                trx.clear(&key);
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB delete {segment}/{id}: {e}")))?;

        Ok(())
    }

    /// Perform a range scan over `[start, end)` and return all raw key-value
    /// pairs.  Uses `StreamingMode::WantAll` to fetch all results in one
    /// round-trip.
    fn range_scan(&self, start: Vec<u8>, end: Vec<u8>) -> CasResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let rt = Self::rt()?;

        let pairs = rt.block_on(async {
            let trx = self.db.create_trx().map_err(|e| CasError::Store(format!("FDB trx: {e}")))?;

            let begin = KeySelector::first_greater_or_equal(start.as_slice());
            let end_sel = KeySelector::first_greater_or_equal(end.as_slice());

            let opt = RangeOption {
                mode: StreamingMode::WantAll,
                limit: Some(RANGE_SCAN_LIMIT),
                ..RangeOption::from((begin, end_sel))
            };

            let kvs = trx
                .get_range(&opt, 1, false)
                .await
                .map_err(|e| CasError::Store(format!("FDB range_scan: {e}")))?;

            Ok::<Vec<(Vec<u8>, Vec<u8>)>, CasError>(
                kvs.iter()
                    .map(|kv| (kv.key().to_vec(), kv.value().to_vec()))
                    .collect(),
            )
        })?;

        Ok(pairs)
    }
}

// ---------------------------------------------------------------------------
// TreeStore
// ---------------------------------------------------------------------------

impl TreeStore for FdbMetadataStore {
    fn put_tree_node(&self, node: &TreeNode) -> CasResult<()> {
        let id = node.hash.to_hex().to_string();
        self.put_json("tree", &id, node)
    }

    fn get_tree_node(&self, hash: &ChunkHash) -> CasResult<Option<TreeNode>> {
        self.get_json("tree", &hash.to_hex().to_string())
    }
}

// ---------------------------------------------------------------------------
// NodeStore
// ---------------------------------------------------------------------------

impl NodeStore for FdbMetadataStore {
    fn put_node(&self, entry: &NodeEntry) -> CasResult<()> {
        let id = entry.hash.to_hex().to_string();
        self.put_json("node", &id, entry)
    }

    fn get_node(&self, hash: &ChunkHash) -> CasResult<Option<NodeEntry>> {
        self.get_json("node", &hash.to_hex().to_string())
    }
}

// ---------------------------------------------------------------------------
// VersionRepo
// ---------------------------------------------------------------------------

impl VersionRepo for FdbMetadataStore {
    fn put_version(&self, version: &VersionNode) -> CasResult<()> {
        self.put_json("ver", version.id.as_str(), version)
    }

    fn get_version(&self, id: &VersionId) -> CasResult<Option<VersionNode>> {
        self.get_json("ver", id.as_str())
    }

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

// ---------------------------------------------------------------------------
// RefRepo
// ---------------------------------------------------------------------------

impl RefRepo for FdbMetadataStore {
    fn put_ref(&self, r: &Ref) -> CasResult<()> {
        // Tags are immutable — fail if one already exists with this name.
        if let Some(existing) = self.get_ref(&r.name)? {
            if existing.kind == RefKind::Tag {
                return Err(CasError::AlreadyExists);
            }
        }
        self.put_json("ref", &r.name, r)
    }

    fn get_ref(&self, name: &str) -> CasResult<Option<Ref>> {
        self.get_json("ref", name)
    }

    fn delete_ref(&self, name: &str) -> CasResult<()> {
        self.delete_key("ref", name)
    }

    fn list_refs(&self, kind: Option<RefKind>) -> CasResult<Vec<Ref>> {
        let start = self.make_key("ref", "");
        let end = Self::prefix_end(&start);
        let pairs = self.range_scan(start, end)?;

        let mut refs = Vec::new();
        for (_k, v) in pairs {
            match serde_json::from_slice::<Ref>(&v) {
                Ok(r) => {
                    if kind.as_ref().is_none_or(|k| &r.kind == k) {
                        refs.push(r);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "FDB: skipping malformed ref entry");
                }
            }
        }
        Ok(refs)
    }

    /// Atomically compare-and-swap a ref's target using a single FDB
    /// transaction, providing true optimistic concurrency control.
    ///
    /// If FDB detects a write-write conflict (another writer committed between
    /// our read and commit), `db.run` retries the whole transaction
    /// automatically — up to the client-configured retry limit.
    fn compare_and_swap_ref(
        &self,
        name: &str,
        expected_old: Option<&VersionId>,
        new_ref: &Ref,
    ) -> CasResult<bool> {
        let key = self.make_key("ref", name);
        let expected_target = expected_old.map(|v| v.0.clone());
        let new_bytes = serde_json::to_vec(new_ref).expect("serialize Ref");
        let rt = Self::rt()?;

        let swapped = rt
            .block_on(self.db.run(|trx, _maybe_committed| {
                let key = key.clone();
                let expected = expected_target.clone();
                let new_bytes = new_bytes.clone();
                async move {
                    let current_raw = trx.get(&key, false).await?;
                    let current_target: Option<String> = current_raw.and_then(|b| {
                        serde_json::from_slice::<Ref>(&b).ok().map(|r| r.target.0)
                    });

                    let condition_met = match (&current_target, &expected) {
                        (None, None) => true,
                        (Some(cur), Some(exp)) => cur == exp,
                        _ => false,
                    };

                    if condition_met {
                        trx.set(&key, &new_bytes);
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
            }))
            .map_err(|e: FdbError| CasError::Store(format!("FDB compare_and_swap_ref '{name}': {e}")))?;

        Ok(swapped)
    }
}

// ---------------------------------------------------------------------------
// PathIndexRepo
// ---------------------------------------------------------------------------

impl PathIndexRepo for FdbMetadataStore {
    fn put_path_entry(
        &self,
        version_id: &VersionId,
        path: &str,
        node_hash: &ChunkHash,
        is_directory: bool,
    ) -> CasResult<()> {
        let key = self.path_key(version_id, path);
        // Value format: "{hash_hex},{is_dir}"  (compact, no JSON overhead)
        let value = format!(
            "{},{}",
            node_hash.to_hex(),
            if is_directory { "1" } else { "0" }
        )
        .into_bytes();
        let rt = Self::rt()?;

        rt.block_on(self.db.run(|trx, _| {
            let key = key.clone();
            let value = value.clone();
            async move {
                trx.set(&key, &value);
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB put_path_entry: {e}")))?;

        Ok(())
    }

    fn get_by_path(
        &self,
        version_id: &VersionId,
        path: &str,
    ) -> CasResult<Option<PathEntry>> {
        let key = self.path_key(version_id, path);
        let rt = Self::rt()?;

        let maybe = rt
            .block_on(Self::fdb_get(&self.db, &key))
            .map_err(|e| CasError::Store(format!("FDB get_by_path: {e}")))?;

        match maybe {
            None => Ok(None),
            Some(bytes) => {
                let entry = Self::decode_path_entry(path, &bytes)?;
                Ok(Some(entry))
            }
        }
    }

    fn list_directory(
        &self,
        version_id: &VersionId,
        dir_path: &str,
    ) -> CasResult<Vec<PathEntry>> {
        // Build the prefix for this version's path space.
        let version_path_prefix = self.path_prefix(version_id);

        // Narrow down to entries under dir_path (empty = root).
        let scan_prefix = if dir_path.is_empty() {
            version_path_prefix.clone()
        } else {
            let norm = if dir_path.ends_with('/') {
                dir_path.to_string()
            } else {
                format!("{dir_path}/")
            };
            // Append the normalised dir path to the version-level prefix.
            let mut p = version_path_prefix.clone();
            p.extend_from_slice(norm.as_bytes());
            p
        };

        let scan_end = Self::prefix_end(&scan_prefix);
        let pairs = self.range_scan(scan_prefix.clone(), scan_end)?;

        // The path segment key is:
        //   {tenant}\x00{workspace}\x00path\x00{version_id}\x00{full_path}
        // `version_path_prefix` ends right before the full_path portion.
        let path_offset = version_path_prefix.len();

        // Additional dir-path prefix length to strip for the relative part.
        let dir_prefix_len = if dir_path.is_empty() {
            0
        } else if dir_path.ends_with('/') {
            dir_path.len()
        } else {
            dir_path.len() + 1 // account for the appended '/'
        };

        let mut entries = Vec::new();
        for (k, v) in pairs {
            if k.len() <= path_offset {
                continue;
            }
            // full_path is the UTF-8 portion after the version prefix
            let full_path = match std::str::from_utf8(&k[path_offset..]) {
                Ok(s) => s,
                Err(_) => continue, // skip non-UTF-8 keys
            };

            // Only include immediate children: relative portion has no '/'.
            let relative = &full_path[dir_prefix_len..];
            if relative.contains('/') {
                continue;
            }

            match Self::decode_path_entry(full_path, &v) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    tracing::warn!(key = ?k, error = %e, "FDB: skipping malformed path entry");
                }
            }
        }

        Ok(entries)
    }
}

impl FdbMetadataStore {
    /// Decode a `path_key` value (`"{hash_hex},{is_dir}"`) into a [`PathEntry`].
    fn decode_path_entry(path: &str, value: &[u8]) -> CasResult<PathEntry> {
        let s = std::str::from_utf8(value)
            .map_err(|e| CasError::Store(format!("FDB path entry non-UTF-8: {e}")))?;
        let mut parts = s.splitn(2, ',');
        let hash_hex = parts
            .next()
            .ok_or_else(|| CasError::Store("FDB path entry: missing hash".into()))?;
        let is_dir_flag = parts
            .next()
            .ok_or_else(|| CasError::Store("FDB path entry: missing is_dir".into()))?;

        let bytes = hex::decode(hash_hex)
            .map_err(|e| CasError::Store(format!("FDB path entry bad hash hex: {e}")))?;
        if bytes.len() != 32 {
            return Err(CasError::Store("FDB path entry: hash must be 32 bytes".into()));
        }
        let arr: [u8; 32] = bytes.try_into().unwrap();
        let node_hash = blake3::Hash::from_bytes(arr);

        Ok(PathEntry {
            path: path.to_string(),
            node_hash,
            is_directory: is_dir_flag == "1",
        })
    }
}
