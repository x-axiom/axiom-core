//! Mark-sweep garbage collector (E11-S02).
//!
//! Reclaims [`NodeEntry`] objects whose reference count reached zero (tracked
//! by [`RefCountRepo`]) and whose grace period has expired.
//!
//! # Two-phase confirmation
//!
//! ```text
//! Phase 1 (E11-S01 — already done)
//!   refcount drops to 0  →  object enters garbage queue
//!                            (FDB key: garbage\x00{tid}\x00{wid}\x00{hash})
//!
//! Phase 2 (this module — run_sweep)
//!   for each garbage entry:
//!     - age ≥ grace_period_secs ?  (configurable, default 24 h)
//!     - hash NOT in live set ?      (safety: BFS from all refs)
//!   if both true → move to pending_delete queue, remove from garbage
//!                  (FDB key: pending\x00{tid}\x00{wid}\x00{hash})
//!
//! Phase 3 (caller or E11-S05 scheduler)
//!   drain pending_delete → physical deletion (S3 key removal, node store
//!   is append-only CAS so no FDB record removal needed at this layer)
//! ```
//!
//! # Safety guarantee
//!
//! The live-set check in Phase 2 is a **double-safety net**: normally a hash
//! in the garbage queue cannot be in the live set because decrement only
//! zeros the counter when no live version references it.  However, network
//! partitions or bugs could lead to inconsistency, so the sweep always
//! confirms before enqueuing for deletion.
//!
//! # Non-blocking reads
//!
//! The mark phase uses snapshot reads on the node store and version/ref
//! repos; it does not hold any write locks during traversal.  GC therefore
//! never blocks concurrent reads or writes to the content store.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use foundationdb::{Database, FdbError, KeySelector, RangeOption};
use foundationdb::options::StreamingMode;

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, VersionId};
use crate::store::traits::{NodeStore, RefRepo, VersionRepo};

use super::refcount::{RefCountRepo, collect_node_hashes};

// ---------------------------------------------------------------------------
// GcConfig
// ---------------------------------------------------------------------------

/// Tuning parameters for a GC sweep run.
#[derive(Clone, Debug)]
pub struct GcConfig {
    /// Objects younger than this are not deleted even if refcount = 0.
    ///
    /// Provides a safety window against race conditions where a new version
    /// references an object whose ref-count has not yet been incremented.
    pub grace_period_secs: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: 24 * 3600, // 24 hours
        }
    }
}

// ---------------------------------------------------------------------------
// GcReport
// ---------------------------------------------------------------------------

/// Summary returned by [`GcSweeper::run_sweep`].
#[derive(Clone, Debug, Default)]
pub struct GcReport {
    /// Number of unique object hashes in the live set.
    pub live_objects: usize,
    /// Total garbage-queue entries examined in this run.
    pub garbage_candidates: usize,
    /// Entries skipped because they are younger than the grace period.
    pub grace_period_pending: usize,
    /// Entries skipped because they appeared in the live set (safety check).
    ///
    /// Under normal operation this should always be zero.  A non-zero value
    /// indicates a refcount/live-set inconsistency and is logged as a warning.
    pub unsafe_skipped: usize,
    /// Entries promoted to the `pending_delete` queue this run.
    pub pending_delete_enqueued: usize,
}

// ---------------------------------------------------------------------------
// GcSweeper
// ---------------------------------------------------------------------------

/// Orchestrates mark-sweep GC for a single workspace.
///
/// # Usage
///
/// ```ignore
/// let sweeper = GcSweeper::new(db, rc_repo, refs, versions, nodes, GcConfig::default());
/// let report = sweeper.run_sweep("tenant-1", "ws-1")?;
/// println!("GC: {} objects pending deletion", report.pending_delete_enqueued);
/// ```
pub struct GcSweeper {
    db: Arc<Database>,
    rc: Arc<RefCountRepo>,
    refs: Arc<dyn RefRepo>,
    versions: Arc<dyn VersionRepo>,
    nodes: Arc<dyn NodeStore>,
    config: GcConfig,
}

impl GcSweeper {
    /// Create a new sweeper with the given stores.
    pub fn new(
        db: Arc<Database>,
        rc: Arc<RefCountRepo>,
        refs: Arc<dyn RefRepo>,
        versions: Arc<dyn VersionRepo>,
        nodes: Arc<dyn NodeStore>,
        config: GcConfig,
    ) -> Self {
        Self { db, rc, refs, versions, nodes, config }
    }

    // -----------------------------------------------------------------------
    // Main entry point
    // -----------------------------------------------------------------------

    /// Run a full mark-sweep cycle for one workspace.
    ///
    /// ## Steps
    ///
    /// 1. **Mark** — BFS from every ref tip; collect the live-object set.
    /// 2. **Sweep** — scan the garbage queue; filter by grace period and
    ///    live-set membership.
    /// 3. **Enqueue** — promote confirmed garbage to `pending_delete`.
    ///
    /// Physical deletion (S3 / RocksDB) is left to the caller or the
    /// scheduler (E11-S05) so this method is always non-destructive.
    pub fn run_sweep(&self, tenant_id: &str, workspace_id: &str) -> CasResult<GcReport> {
        let mut report = GcReport::default();
        let now = unix_now();

        // ── Phase 1: Mark ───────────────────────────────────────────────────
        tracing::info!(tenant_id, workspace_id, "GC mark phase started");
        let live_set = self.build_live_set()?;
        report.live_objects = live_set.len();
        tracing::info!(
            tenant_id,
            workspace_id,
            live_objects = report.live_objects,
            "GC mark phase complete"
        );

        // ── Phase 2: Sweep ──────────────────────────────────────────────────
        tracing::info!(tenant_id, workspace_id, "GC sweep phase started");
        let garbage = self.rc.list_garbage(tenant_id, workspace_id)?;
        report.garbage_candidates = garbage.len();

        let mut to_enqueue: Vec<ChunkHash> = Vec::new();
        let enqueue_ts = now;

        for (hash, enqueued_at) in &garbage {
            let age = now.saturating_sub(*enqueued_at);

            // Grace period check.
            if age < self.config.grace_period_secs {
                report.grace_period_pending += 1;
                continue;
            }

            // Safety: double-check against live set.
            if live_set.contains(hash.as_bytes()) {
                tracing::warn!(
                    hash = %hash,
                    tenant_id,
                    workspace_id,
                    "GC: object is in garbage queue but also in live set — skipping (refcount inconsistency)"
                );
                report.unsafe_skipped += 1;
                continue;
            }

            to_enqueue.push(*hash);
        }

        // ── Phase 3: Enqueue pending_delete ─────────────────────────────────
        if !to_enqueue.is_empty() {
            tracing::info!(
                tenant_id,
                workspace_id,
                count = to_enqueue.len(),
                "GC: promoting objects to pending_delete"
            );

            // Remove from garbage queue, write to pending_delete, atomically.
            self.promote_to_pending_delete(
                tenant_id,
                workspace_id,
                &to_enqueue,
                enqueue_ts,
            )?;

            report.pending_delete_enqueued = to_enqueue.len();

            tracing::info!(
                tenant_id,
                workspace_id,
                report = ?report,
                "GC sweep complete"
            );
        } else {
            tracing::info!(
                tenant_id,
                workspace_id,
                report = ?report,
                "GC sweep complete — nothing to enqueue"
            );
        }

        Ok(report)
    }

    // -----------------------------------------------------------------------
    // Pending-delete queue
    // -----------------------------------------------------------------------

    /// Return all objects currently in the `pending_delete` queue.
    ///
    /// Returns `(hash, enqueued_at_unix_secs)` pairs.
    pub fn list_pending_delete(
        &self,
        tenant_id: &str,
        workspace_id: &str,
    ) -> CasResult<Vec<(ChunkHash, u64)>> {
        let prefix = Self::pending_prefix(tenant_id, workspace_id);
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

    /// Remove the given hashes from the `pending_delete` queue.
    ///
    /// Call this after physical deletion (e.g. S3 object removal) to
    /// acknowledge that the objects have been fully cleaned up.
    pub fn drain_pending_delete_entries(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        hashes: &[ChunkHash],
    ) -> CasResult<()> {
        if hashes.is_empty() {
            return Ok(());
        }

        let keys: Vec<Vec<u8>> = hashes
            .iter()
            .map(|h| Self::pending_key(tenant_id, workspace_id, h))
            .collect();

        let rt = Self::rt()?;
        rt.block_on(self.db.run(|trx, _| {
            let keys = keys.clone();
            async move {
                for key in &keys {
                    trx.clear(key);
                }
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB drain_pending_delete: {e}")))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Mark phase
    // -----------------------------------------------------------------------

    /// BFS from every ref tip → collect all live node hashes.
    ///
    /// Walks all branch/tag ref tips, then follows their version DAG parents
    /// to collect every reachable version root, then collects every NodeEntry
    /// hash reachable from those roots.
    fn build_live_set(&self) -> CasResult<HashSet<[u8; 32]>> {
        let mut live: HashSet<[u8; 32]> = HashSet::new();

        // Walk every ref tip's version DAG.
        let all_refs = self.refs.list_refs(None)?;
        let mut visited_versions: HashSet<String> = HashSet::new();
        let mut version_queue: std::collections::VecDeque<VersionId> =
            std::collections::VecDeque::new();

        // Seed from all ref tips.
        for r in &all_refs {
            if visited_versions.insert(r.target.0.clone()) {
                version_queue.push_back(r.target.clone());
            }
        }

        // BFS through the version DAG.
        while let Some(vid) = version_queue.pop_front() {
            let version = match self.versions.get_version(&vid)? {
                Some(v) => v,
                None => {
                    tracing::warn!(version = %vid, "GC: version not found in DAG during mark");
                    continue;
                }
            };

            // Collect all NodeEntry hashes from this version's root tree.
            let node_hashes = collect_node_hashes(&version.root, self.nodes.as_ref())?;
            for h in node_hashes {
                live.insert(*h.as_bytes());
            }

            // Enqueue parent versions.
            for parent in &version.parents {
                if visited_versions.insert(parent.0.clone()) {
                    version_queue.push_back(parent.clone());
                }
            }
        }

        Ok(live)
    }

    // -----------------------------------------------------------------------
    // FDB: promote garbage → pending_delete (atomic)
    // -----------------------------------------------------------------------

    fn promote_to_pending_delete(
        &self,
        tenant_id: &str,
        workspace_id: &str,
        hashes: &[ChunkHash],
        ts: u64,
    ) -> CasResult<()> {
        let ts_bytes = ts.to_le_bytes().to_vec();

        let garbage_keys: Vec<Vec<u8>> = hashes
            .iter()
            .map(|h| RefCountRepo::garbage_key_pub(tenant_id, workspace_id, h))
            .collect();
        let pending_keys: Vec<Vec<u8>> = hashes
            .iter()
            .map(|h| Self::pending_key(tenant_id, workspace_id, h))
            .collect();

        let rt = Self::rt()?;
        rt.block_on(self.db.run(|trx, _| {
            let garbage_keys = garbage_keys.clone();
            let pending_keys = pending_keys.clone();
            let ts_bytes = ts_bytes.clone();
            async move {
                for (gk, pk) in garbage_keys.iter().zip(pending_keys.iter()) {
                    trx.clear(gk);
                    trx.set(pk, &ts_bytes);
                }
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB promote_to_pending_delete: {e}")))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Key builders
    // -----------------------------------------------------------------------

    fn pending_key(tenant_id: &str, workspace_id: &str, hash: &ChunkHash) -> Vec<u8> {
        format!("pending\x00{tenant_id}\x00{workspace_id}\x00{hash}").into_bytes()
    }

    fn pending_prefix(tenant_id: &str, workspace_id: &str) -> Vec<u8> {
        format!("pending\x00{tenant_id}\x00{workspace_id}\x00").into_bytes()
    }

    fn prefix_end(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        end.push(0xff);
        end
    }

    // -----------------------------------------------------------------------
    // FDB helpers
    // -----------------------------------------------------------------------

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
                limit: Some(100_000),
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
            .map_err(|e| CasError::Store(format!("GcSweeper requires a Tokio runtime: {e}")))
    }
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

fn parse_hash_from_key(key: &[u8], prefix_len: usize) -> CasResult<ChunkHash> {
    let hex = key.get(prefix_len..).ok_or_else(|| {
        CasError::Store(format!("GC: pending key too short ({} bytes)", key.len()))
    })?;
    let hex_str = std::str::from_utf8(hex)
        .map_err(|e| CasError::Store(format!("GC: pending key not UTF-8: {e}")))?;
    blake3::Hash::from_hex(hex_str)
        .map_err(|e| CasError::Store(format!("GC: invalid hash in pending key '{hex_str}': {e}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crate::model::{ChunkHash, NodeEntry, NodeKind, Ref, RefKind, VersionId, VersionNode, hash_bytes};
    use crate::store::memory::{InMemoryNodeStore, InMemoryRefRepo, InMemoryVersionRepo};
    use crate::store::traits::{NodeStore, RefRepo, VersionRepo};

    use super::super::refcount::collect_node_hashes;

    fn h(tag: &str) -> ChunkHash {
        hash_bytes(tag.as_bytes())
    }

    fn file_node(hash: ChunkHash) -> NodeEntry {
        NodeEntry { hash, kind: NodeKind::File { root: h("content"), size: 42 } }
    }

    fn dir_node(hash: ChunkHash, children: BTreeMap<String, ChunkHash>) -> NodeEntry {
        NodeEntry { hash, kind: NodeKind::Directory { children } }
    }

    fn version(id: VersionId, root: ChunkHash, parents: Vec<VersionId>) -> VersionNode {
        VersionNode {
            id,
            root,
            parents,
            message: "test".into(),
            timestamp: 0,
            metadata: Default::default(),
        }
    }

    fn branch(name: &str, target: VersionId) -> Ref {
        Ref { name: name.into(), kind: RefKind::Branch, target }
    }

    // ------------------------------------------------------------------
    // collect_node_hashes re-exported tests (mark phase logic)
    // ------------------------------------------------------------------

    #[test]
    fn test_mark_empty_tree() {
        let store = InMemoryNodeStore::new();
        // A tree with one file node at root.
        let root = h("root_file");
        store.put_node(&file_node(root)).unwrap();

        let live: std::collections::HashSet<[u8; 32]> = collect_node_hashes(&root, &store)
            .unwrap()
            .iter()
            .map(|h| *h.as_bytes())
            .collect();

        assert!(live.contains(root.as_bytes()));
        assert_eq!(live.len(), 1);
    }

    #[test]
    fn test_mark_full_tree() {
        let store = InMemoryNodeStore::new();

        let file_a = h("file_a");
        let file_b = h("file_b");
        store.put_node(&file_node(file_a)).unwrap();
        store.put_node(&file_node(file_b)).unwrap();

        let mut children = BTreeMap::new();
        children.insert("a".into(), file_a);
        children.insert("b".into(), file_b);
        let root = h("root_dir");
        store.put_node(&dir_node(root, children)).unwrap();

        let live: std::collections::HashSet<[u8; 32]> = collect_node_hashes(&root, &store)
            .unwrap()
            .iter()
            .map(|h| *h.as_bytes())
            .collect();

        assert_eq!(live.len(), 3);
        assert!(live.contains(root.as_bytes()));
        assert!(live.contains(file_a.as_bytes()));
        assert!(live.contains(file_b.as_bytes()));
    }

    // ------------------------------------------------------------------
    // build_live_set through multiple refs and versions
    // ------------------------------------------------------------------

    fn populate_two_branch_store() -> (
        Arc<InMemoryNodeStore>,
        Arc<InMemoryRefRepo>,
        Arc<InMemoryVersionRepo>,
    ) {
        let nodes = Arc::new(InMemoryNodeStore::new());
        let refs = Arc::new(InMemoryRefRepo::new());
        let versions = Arc::new(InMemoryVersionRepo::new());

        // Two files, each in their own version (separate branches).
        let file_main = h("file_main");
        let file_dev = h("file_dev");
        nodes.put_node(&file_node(file_main)).unwrap();
        nodes.put_node(&file_node(file_dev)).unwrap();

        let v_main = VersionId("v_main".into());
        let v_dev = VersionId("v_dev".into());
        versions.put_version(&version(v_main.clone(), file_main, vec![])).unwrap();
        versions.put_version(&version(v_dev.clone(), file_dev, vec![])).unwrap();

        refs.put_ref(&branch("main", v_main)).unwrap();
        refs.put_ref(&branch("dev", v_dev)).unwrap();

        (nodes, refs, versions)
    }

    #[test]
    fn test_build_live_set_covers_all_branches() {
        let (nodes, refs_store, versions_store) = populate_two_branch_store();

        // Build live set manually (same logic as GcSweeper::build_live_set).
        let mut live: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();

        let all_refs = refs_store.list_refs(None).unwrap();
        let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut queue: std::collections::VecDeque<VersionId> = std::collections::VecDeque::new();

        for r in &all_refs {
            if visited.insert(r.target.0.clone()) {
                queue.push_back(r.target.clone());
            }
        }
        while let Some(vid) = queue.pop_front() {
            let v = versions_store.get_version(&vid).unwrap().unwrap();
            for h in collect_node_hashes(&v.root, nodes.as_ref()).unwrap() {
                live.insert(*h.as_bytes());
            }
            for parent in &v.parents {
                if visited.insert(parent.0.clone()) {
                    queue.push_back(parent.clone());
                }
            }
        }

        assert_eq!(live.len(), 2);
        assert!(live.contains(h("file_main").as_bytes()));
        assert!(live.contains(h("file_dev").as_bytes()));
    }

    #[test]
    fn test_build_live_set_walks_parent_versions() {
        let nodes = Arc::new(InMemoryNodeStore::new());
        let refs_store = Arc::new(InMemoryRefRepo::new());
        let versions_store = Arc::new(InMemoryVersionRepo::new());

        let root_v1 = h("root_v1");
        let root_v2 = h("root_v2");
        nodes.put_node(&file_node(root_v1)).unwrap();
        nodes.put_node(&file_node(root_v2)).unwrap();

        let v1 = VersionId("v1".into());
        let v2 = VersionId("v2".into());
        versions_store.put_version(&version(v1.clone(), root_v1, vec![])).unwrap();
        versions_store
            .put_version(&version(v2.clone(), root_v2, vec![v1.clone()]))
            .unwrap();

        // Only v2 is in a ref; v1 is a parent.
        refs_store.put_ref(&branch("main", v2)).unwrap();

        // Live set must include both roots (v1 reached via parent walk).
        let mut live: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
        let all_refs = refs_store.list_refs(None).unwrap();
        let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut queue: std::collections::VecDeque<VersionId> = std::collections::VecDeque::new();
        for r in &all_refs {
            if visited.insert(r.target.0.clone()) {
                queue.push_back(r.target.clone());
            }
        }
        while let Some(vid) = queue.pop_front() {
            let v = versions_store.get_version(&vid).unwrap().unwrap();
            for h in collect_node_hashes(&v.root, nodes.as_ref()).unwrap() {
                live.insert(*h.as_bytes());
            }
            for parent in &v.parents {
                if visited.insert(parent.0.clone()) {
                    queue.push_back(parent.clone());
                }
            }
        }

        assert!(live.contains(root_v1.as_bytes()), "parent version root must be in live set");
        assert!(live.contains(root_v2.as_bytes()));
    }

    #[test]
    fn test_build_live_set_deduplicates_shared_subtree() {
        let nodes = Arc::new(InMemoryNodeStore::new());
        let refs_store = Arc::new(InMemoryRefRepo::new());
        let versions_store = Arc::new(InMemoryVersionRepo::new());

        let shared_file = h("shared");
        nodes.put_node(&file_node(shared_file)).unwrap();

        // Both versions reference the same root (content-addressed — same root hash).
        let root = h("shared_root");
        let mut children = BTreeMap::new();
        children.insert("file".into(), shared_file);
        nodes.put_node(&NodeEntry { hash: root, kind: NodeKind::Directory { children } }).unwrap();

        let v1 = VersionId("v1".into());
        let v2 = VersionId("v2".into());
        versions_store.put_version(&version(v1.clone(), root, vec![])).unwrap();
        versions_store.put_version(&version(v2.clone(), root, vec![])).unwrap();

        refs_store.put_ref(&branch("main", v1)).unwrap();
        refs_store.put_ref(&branch("dev", v2)).unwrap();

        let mut live: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
        for r in refs_store.list_refs(None).unwrap() {
            let v = versions_store.get_version(&r.target).unwrap().unwrap();
            for h in collect_node_hashes(&v.root, nodes.as_ref()).unwrap() {
                live.insert(*h.as_bytes());
            }
        }

        // root + shared_file = 2 (not 4 from two traversals)
        assert_eq!(live.len(), 2);
    }

    #[test]
    fn test_gc_config_default_grace_period() {
        use super::GcConfig;
        let cfg = GcConfig::default();
        assert_eq!(cfg.grace_period_secs, 24 * 3600);
    }

    #[test]
    fn test_gc_report_default() {
        use super::GcReport;
        let r = GcReport::default();
        assert_eq!(r.live_objects, 0);
        assert_eq!(r.garbage_candidates, 0);
        assert_eq!(r.grace_period_pending, 0);
        assert_eq!(r.unsafe_skipped, 0);
        assert_eq!(r.pending_delete_enqueued, 0);
    }
}
