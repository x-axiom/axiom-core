//! FDB-backed reference counter for content-addressed objects (E11-S01).
//!
//! Tracks how many live versions reference each [`NodeEntry`] hash within a
//! workspace.  When the count drops to zero the object is enqueued in the
//! garbage queue for the sweep phase (E11-S02) to collect.
//!
//! # Key layout
//!
//! ```text
//! rc\x00{tenant_id}\x00{workspace_id}\x00{hash_hex}      → 4-byte LE u32 (refcount)
//! garbage\x00{tenant_id}\x00{workspace_id}\x00{hash_hex} → 8-byte LE u64 (enqueued_at unix secs)
//! ```
//!
//! # Atomicity
//!
//! - **Increment**: uses FDB `atomic_op(MutationType::Add)` — one round-trip,
//!   no conflict keys.  Initialises absent keys to 1 automatically.
//! - **Decrement**: uses a read-modify-write transaction inside `db.run()` so
//!   FDB conflict detection provides serialisable semantics.  Keys that reach
//!   zero are cleared and a garbage marker is written in the same transaction.
//!
//! Both operations are batched: at most [`BATCH_SIZE`] hashes per FDB
//! transaction, keeping each transaction within FDB's limits.
//!
//! # Tree traversal
//!
//! Reference counts are maintained at the **NodeEntry** level (the directory-
//! tree namespace).  A BFS walk starting from the version root visits every
//! reachable NodeEntry hash; the set is deduplicated so shared sub-trees are
//! counted only once per reference.
//!
//! File-level Merkle trees and raw chunks are tracked indirectly: an object
//! is safe to delete from the CAS store only after its NodeEntry refcount
//! reaches zero.  Chunk-level GC is handled separately by E11-S03
//! (S3 lifecycle policies).

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use foundationdb::options::MutationType;
use foundationdb::{Database, FdbError, KeySelector, RangeOption};
use foundationdb::options::StreamingMode;

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, NodeKind};
use crate::store::traits::NodeStore;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Max hashes processed per FDB transaction.
const BATCH_SIZE: usize = 500;

/// Prefix for reference-count keys.
const RC_PREFIX: &str = "rc\x00";

/// Prefix for garbage-queue keys.
const GARBAGE_PREFIX: &str = "garbage\x00";

/// Max garbage entries returned per range scan.
const GC_SCAN_LIMIT: usize = 100_000;

// ---------------------------------------------------------------------------
// RefCountRepo
// ---------------------------------------------------------------------------

/// FDB-backed reference counter for content-addressed objects.
///
/// Thread-safe; wrap in `Arc<RefCountRepo>` to share across tasks.
///
/// The FDB network thread must be started before constructing this struct:
/// ```ignore
/// let _guard = unsafe { foundationdb::boot() };
/// ```
pub struct RefCountRepo {
    db: Arc<Database>,
}

impl RefCountRepo {
    /// Wrap an existing FDB `Database`.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Increment reference counts for all [`NodeEntry`]s reachable from
    /// `root`.
    ///
    /// Call this **after** a new version is durably committed.
    ///
    /// Returns the number of unique objects whose count was incremented.
    pub fn increment_refs(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        root: &ChunkHash,
        nodes: &dyn NodeStore,
    ) -> CasResult<usize> {
        let hashes = collect_node_hashes(root, nodes)?;
        let count = hashes.len();

        for batch in hashes.chunks(BATCH_SIZE) {
            let rc_keys: Vec<Vec<u8>> = batch
                .iter()
                .map(|h| Self::rc_key(tenant_id, workspace_id, h))
                .collect();

            Self::fdb_increment_batch(Arc::clone(&self.db), rc_keys)?;
        }

        Ok(count)
    }

    /// Decrement reference counts for all [`NodeEntry`]s reachable from
    /// `root`.
    ///
    /// Objects whose count drops to zero are cleared from the refcount index
    /// and added to the garbage queue atomically.
    ///
    /// Call this **after** a version is deleted from the version DAG.
    ///
    /// Returns the hashes newly enqueued as garbage.
    pub fn decrement_refs(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        root: &ChunkHash,
        nodes: &dyn NodeStore,
    ) -> CasResult<Vec<ChunkHash>> {
        let hashes = collect_node_hashes(root, nodes)?;
        let now = unix_now();

        let mut newly_garbage: Vec<ChunkHash> = Vec::new();

        for batch in hashes.chunks(BATCH_SIZE) {
            let rc_keys: Vec<Vec<u8>> = batch
                .iter()
                .map(|h| Self::rc_key(tenant_id, workspace_id, h))
                .collect();
            let garbage_keys: Vec<Vec<u8>> = batch
                .iter()
                .map(|h| Self::garbage_key(tenant_id, workspace_id, h))
                .collect();

            let zeroed = Self::fdb_decrement_batch(
                Arc::clone(&self.db),
                rc_keys,
                garbage_keys,
                now,
            )?;

            for (hash, became_zero) in batch.iter().zip(zeroed.iter()) {
                if *became_zero {
                    newly_garbage.push(*hash);
                }
            }
        }

        Ok(newly_garbage)
    }

    /// Return the current reference count for a single object.
    ///
    /// Returns `0` for objects not present in the index (never referenced or
    /// already GC'd).
    pub fn get_refcount(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        hash: &ChunkHash,
    ) -> CasResult<u32> {
        let key = Self::rc_key(tenant_id, workspace_id, hash);
        let rt = Self::rt()?;

        let raw = rt.block_on(async {
            let trx = self
                .db
                .create_trx()
                .map_err(|e| CasError::Store(format!("FDB create_trx: {e}")))?;
            let v = trx
                .get(&key, false)
                .await
                .map_err(|e| CasError::Store(format!("FDB get refcount: {e}")))?;
            Ok::<Option<Vec<u8>>, CasError>(v.map(|b| b.to_vec()))
        })?;

        Ok(raw
            .as_deref()
            .and_then(|b| b.try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(0))
    }

    /// List all garbage-queue entries for a workspace without removing them.
    ///
    /// Returns `(hash, enqueued_at_unix_secs)` pairs.  Use this to inspect
    /// the queue or enforce a grace period before draining.
    pub fn list_garbage(
        &self,
        tenant_id: &str,
        workspace_id: &str,
    ) -> CasResult<Vec<(ChunkHash, u64)>> {
        let prefix = Self::garbage_prefix(tenant_id, workspace_id);
        let end = Self::prefix_end(&prefix);
        let kvs = self.fdb_range_scan(prefix.clone(), end)?;

        let prefix_len = prefix.len();
        let mut result = Vec::with_capacity(kvs.len());

        for (k, v) in &kvs {
            let hash = parse_hash_from_key(k, prefix_len)?;
            let ts = v
                .as_slice()
                .try_into()
                .ok()
                .map(u64::from_le_bytes)
                .unwrap_or(0);
            result.push((hash, ts));
        }

        Ok(result)
    }

    /// Drain all garbage-queue entries for a workspace atomically.
    ///
    /// Clears the garbage markers and returns the enqueued hashes.
    /// Typically called by the GC sweep (E11-S02) after the grace period.
    pub fn drain_garbage(
        &self,
        tenant_id: &str,
        workspace_id: &str,
    ) -> CasResult<Vec<ChunkHash>> {
        let prefix = Self::garbage_prefix(tenant_id, workspace_id);
        let end = Self::prefix_end(&prefix);
        let kvs = self.fdb_range_scan(prefix.clone(), end.clone())?;

        let prefix_len = prefix.len();
        let mut hashes = Vec::with_capacity(kvs.len());

        for (k, _) in &kvs {
            hashes.push(parse_hash_from_key(k, prefix_len)?);
        }

        // Clear the entire garbage range atomically.
        if !kvs.is_empty() {
            let rt = Self::rt()?;
            rt.block_on(self.db.run(|trx, _| {
                let prefix = prefix.clone();
                let end = end.clone();
                async move {
                    trx.clear_range(&prefix, &end);
                    Ok(())
                }
            }))
            .map_err(|e: FdbError| CasError::Store(format!("FDB drain_garbage: {e}")))?;
        }

        Ok(hashes)
    }

    // -----------------------------------------------------------------------
    // Key builders
    // -----------------------------------------------------------------------

    fn rc_key(tenant_id: &str, workspace_id: &str, hash: &ChunkHash) -> Vec<u8> {
        format!("{RC_PREFIX}{tenant_id}\x00{workspace_id}\x00{hash}").into_bytes()
    }

    fn garbage_key(tenant_id: &str, workspace_id: &str, hash: &ChunkHash) -> Vec<u8> {
        format!("{GARBAGE_PREFIX}{tenant_id}\x00{workspace_id}\x00{hash}").into_bytes()
    }

    /// Public-crate wrapper used by the sweep phase to clear garbage entries.
    pub(crate) fn garbage_key_pub(tenant_id: &str, workspace_id: &str, hash: &ChunkHash) -> Vec<u8> {
        Self::garbage_key(tenant_id, workspace_id, hash)
    }

    fn garbage_prefix(tenant_id: &str, workspace_id: &str) -> Vec<u8> {
        format!("{GARBAGE_PREFIX}{tenant_id}\x00{workspace_id}\x00").into_bytes()
    }

    fn prefix_end(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        end.push(0xff);
        end
    }

    // -----------------------------------------------------------------------
    // FDB helpers
    // -----------------------------------------------------------------------

    /// Atomically increment each key in `rc_keys` by 1.
    ///
    /// Uses FDB `MutationType::Add` (little-endian signed add) with value
    /// `[1, 0, 0, 0]`.  Absent keys are initialised to 1 automatically.
    fn fdb_increment_batch(db: Arc<Database>, rc_keys: Vec<Vec<u8>>) -> CasResult<()> {
        let rt = Self::rt()?;
        rt.block_on(db.run(|trx, _| {
            let rc_keys = rc_keys.clone();
            async move {
                for key in &rc_keys {
                    trx.atomic_op(key, &[1u8, 0, 0, 0], MutationType::Add);
                }
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB increment_batch: {e}")))?;
        Ok(())
    }

    /// Decrement each refcount key; zero-count keys are cleared and their
    /// garbage markers written.
    ///
    /// Returns a bool per entry: `true` if that entry reached zero.
    fn fdb_decrement_batch(
        db: Arc<Database>,
        rc_keys: Vec<Vec<u8>>,
        garbage_keys: Vec<Vec<u8>>,
        enqueued_at: u64,
    ) -> CasResult<Vec<bool>> {
        let rt = Self::rt()?;
        let ts_bytes = enqueued_at.to_le_bytes().to_vec();

        rt.block_on(db.run(|trx, _| {
            let rc_keys = rc_keys.clone();
            let garbage_keys = garbage_keys.clone();
            let ts_bytes = ts_bytes.clone();
            async move {
                let mut zeroed = Vec::with_capacity(rc_keys.len());
                for (rc_key, garbage_key) in rc_keys.iter().zip(garbage_keys.iter()) {
                    let val = trx.get(rc_key, false).await?;
                    let count = val
                        .as_deref()
                        .and_then(|b| b.try_into().ok())
                        .map(u32::from_le_bytes)
                        .unwrap_or(0);

                    if count <= 1 {
                        trx.clear(rc_key);
                        trx.set(garbage_key, &ts_bytes);
                        zeroed.push(true);
                    } else {
                        trx.set(rc_key, &(count - 1).to_le_bytes());
                        zeroed.push(false);
                    }
                }
                Ok(zeroed)
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB decrement_batch: {e}")))
    }

    fn fdb_range_scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> CasResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let rt = Self::rt()?;
        rt.block_on(async {
            let trx = self
                .db
                .create_trx()
                .map_err(|e| CasError::Store(format!("FDB create_trx: {e}")))?;
            let begin = KeySelector::first_greater_or_equal(start.as_slice());
            let end_sel = KeySelector::first_greater_or_equal(end.as_slice());
            let opt = RangeOption {
                mode: StreamingMode::WantAll,
                limit: Some(GC_SCAN_LIMIT),
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
        })
    }

    fn rt() -> CasResult<tokio::runtime::Handle> {
        tokio::runtime::Handle::try_current()
            .map_err(|e| CasError::Store(format!("RefCountRepo requires a Tokio runtime: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Tree traversal
// ---------------------------------------------------------------------------

/// BFS walk of the NodeEntry tree rooted at `root`.
///
/// Returns a deduplicated list of every reachable [`ChunkHash`] (each hash
/// identifies a [`NodeEntry`] in the node store).
///
/// Missing nodes (not present in `nodes`) are silently skipped — they may
/// have already been GC'd or the tree may be partially populated.
pub(crate) fn collect_node_hashes(root: &ChunkHash, nodes: &dyn NodeStore) -> CasResult<Vec<ChunkHash>> {
    let mut visited: HashSet<[u8; 32]> = HashSet::new();
    let mut result: Vec<ChunkHash> = Vec::new();
    let mut queue: VecDeque<ChunkHash> = VecDeque::new();

    queue.push_back(*root);

    while let Some(hash) = queue.pop_front() {
        if !visited.insert(*hash.as_bytes()) {
            continue; // already processed (shared sub-tree)
        }
        result.push(hash);

        match nodes.get_node(&hash)? {
            None => {
                // Node not in store — skip gracefully.
                tracing::warn!(hash = %hash, "RefCountRepo: node not found during tree walk");
            }
            Some(entry) => {
                if let NodeKind::Directory { ref children } = entry.kind {
                    for child_hash in children.values() {
                        if !visited.contains(child_hash.as_bytes()) {
                            queue.push_back(*child_hash);
                        }
                    }
                }
                // File nodes: track the NodeEntry hash (already added above).
                // The Merkle tree root is tracked implicitly — no chunk-level GC here.
            }
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs()
}

/// Extract a `ChunkHash` from a garbage-queue FDB key.
///
/// Keys have the form `{prefix}{hash_hex}` where the prefix is `prefix_len`
/// bytes long.
fn parse_hash_from_key(key: &[u8], prefix_len: usize) -> CasResult<ChunkHash> {
    let hex = key.get(prefix_len..).ok_or_else(|| {
        CasError::Store(format!("GC: garbage key too short ({} bytes)", key.len()))
    })?;
    let hex_str = std::str::from_utf8(hex)
        .map_err(|e| CasError::Store(format!("GC: garbage key not UTF-8: {e}")))?;
    blake3::Hash::from_hex(hex_str)
        .map_err(|e| CasError::Store(format!("GC: invalid hash in garbage key '{hex_str}': {e}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crate::model::{ChunkHash, NodeEntry, NodeKind, hash_bytes};
    use crate::store::memory::InMemoryNodeStore;

    use super::collect_node_hashes;

    fn file_node(hash: ChunkHash, content_hash: ChunkHash) -> NodeEntry {
        NodeEntry {
            hash,
            kind: NodeKind::File { root: content_hash, size: 42 },
        }
    }

    fn dir_node(hash: ChunkHash, children: BTreeMap<String, ChunkHash>) -> NodeEntry {
        NodeEntry { hash, kind: NodeKind::Directory { children } }
    }

    fn h(tag: &str) -> ChunkHash {
        hash_bytes(tag.as_bytes())
    }

    /// Single file at root.
    #[test]
    fn test_collect_single_file() {
        let store = InMemoryNodeStore::new();
        let root_hash = h("root");
        let content_hash = h("content");
        store.put_node(&file_node(root_hash, content_hash)).unwrap();

        let hashes = collect_node_hashes(&root_hash, &store).unwrap();
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0], root_hash);
    }

    /// Directory with two files.
    #[test]
    fn test_collect_directory_with_files() {
        let store = InMemoryNodeStore::new();

        let file_a_hash = h("file_a");
        let file_b_hash = h("file_b");
        store.put_node(&file_node(file_a_hash, h("content_a"))).unwrap();
        store.put_node(&file_node(file_b_hash, h("content_b"))).unwrap();

        let mut children = BTreeMap::new();
        children.insert("a.txt".into(), file_a_hash);
        children.insert("b.txt".into(), file_b_hash);

        let dir_hash = h("dir");
        store.put_node(&dir_node(dir_hash, children)).unwrap();

        let hashes = collect_node_hashes(&dir_hash, &store).unwrap();
        // dir + 2 files = 3 NodeEntry hashes
        assert_eq!(hashes.len(), 3);
        let set: std::collections::HashSet<_> = hashes.into_iter().collect();
        assert!(set.contains(&dir_hash));
        assert!(set.contains(&file_a_hash));
        assert!(set.contains(&file_b_hash));
    }

    /// Shared sub-directory referenced from two parents — counted once.
    #[test]
    fn test_collect_deduplicates_shared_subtree() {
        let store = InMemoryNodeStore::new();

        let shared_file = h("shared_file");
        store.put_node(&file_node(shared_file, h("content"))).unwrap();

        let mut children = BTreeMap::new();
        children.insert("shared".into(), shared_file);
        let shared_dir = h("shared_dir");
        store.put_node(&dir_node(shared_dir, children.clone())).unwrap();

        // Two parent directories both reference the same sub-directory.
        let mut root_children = BTreeMap::new();
        root_children.insert("a".into(), shared_dir);
        root_children.insert("b".into(), shared_dir); // same hash again
        let root_hash = h("root");
        store.put_node(&dir_node(root_hash, root_children)).unwrap();

        let hashes = collect_node_hashes(&root_hash, &store).unwrap();
        // root + shared_dir + shared_file = 3 (shared_dir only counted once)
        assert_eq!(hashes.len(), 3);
    }

    /// Missing node is skipped without error.
    #[test]
    fn test_collect_missing_node_skipped() {
        let store = InMemoryNodeStore::new();
        // root is not in the store at all
        let hashes = collect_node_hashes(&h("nonexistent"), &store).unwrap();
        // The root hash itself is still "visited" before get_node returns None.
        assert_eq!(hashes.len(), 1);
    }

    /// Deep nested directory structure.
    #[test]
    fn test_collect_deep_nesting() {
        let store = InMemoryNodeStore::new();

        // leaf file
        let leaf = h("leaf_file");
        store.put_node(&file_node(leaf, h("leaf_content"))).unwrap();

        // d2 → leaf
        let d2 = h("d2");
        let mut c2 = BTreeMap::new();
        c2.insert("leaf".into(), leaf);
        store.put_node(&dir_node(d2, c2)).unwrap();

        // d1 → d2
        let d1 = h("d1");
        let mut c1 = BTreeMap::new();
        c1.insert("d2".into(), d2);
        store.put_node(&dir_node(d1, c1)).unwrap();

        // root → d1
        let root = h("root");
        let mut cr = BTreeMap::new();
        cr.insert("d1".into(), d1);
        store.put_node(&dir_node(root, cr)).unwrap();

        let hashes = collect_node_hashes(&root, &store).unwrap();
        assert_eq!(hashes.len(), 4); // root, d1, d2, leaf
    }
}
