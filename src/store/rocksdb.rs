use std::path::Path;
use std::sync::Arc;

use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, NodeEntry, TreeNode, hash_bytes};
use super::traits::{ChunkStore, TreeStore, NodeStore};

// ---------------------------------------------------------------------------
// Keyspace / Column Family names
// ---------------------------------------------------------------------------
//
// RocksDB keyspace layout:
//   CF "chunks" — raw chunk bytes, keyed by 32-byte BLAKE3 hash
//   CF "trees"  — JSON-serialized TreeNode, keyed by 32-byte hash
//   CF "nodes"  — JSON-serialized NodeEntry, keyed by 32-byte hash
//
// All keys are the raw 32-byte BLAKE3 hash (not hex-encoded) for compact
// storage and efficient prefix iteration. Values in the chunks CF are raw
// bytes; values in trees and nodes CFs are JSON for debuggability during
// the POC phase. A future optimization may switch to a binary format.
// ---------------------------------------------------------------------------

const CF_CHUNKS: &str = "chunks";
const CF_TREES: &str = "trees";
const CF_NODES: &str = "nodes";

/// All column family names used by the CAS store.
fn cf_names() -> Vec<&'static str> {
    vec![CF_CHUNKS, CF_TREES, CF_NODES]
}

// ---------------------------------------------------------------------------
// RocksDbCasStore
// ---------------------------------------------------------------------------

/// Persistent content-addressed store backed by RocksDB.
///
/// Implements [`ChunkStore`], [`TreeStore`], and [`NodeStore`] using three
/// column families in a single RocksDB instance.
pub struct RocksDbCasStore {
    db: Arc<DB>,
}

impl RocksDbCasStore {
    /// Open or create a RocksDB-backed CAS store at the given directory path.
    ///
    /// The directory and any missing parents will be created automatically.
    /// Column families are created on first open and reused on subsequent opens.
    pub fn open<P: AsRef<Path>>(path: P) -> CasResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names()
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)
            .map_err(|e| CasError::Store(e.to_string()))?;

        Ok(Self { db: Arc::new(db) })
    }
}

// ---------------------------------------------------------------------------
// ChunkStore
// ---------------------------------------------------------------------------

impl ChunkStore for RocksDbCasStore {
    fn put_chunk(&self, data: Vec<u8>) -> CasResult<ChunkHash> {
        let hash = hash_bytes(&data);
        let cf = self
            .db
            .cf_handle(CF_CHUNKS)
            .ok_or_else(|| CasError::Store("missing CF: chunks".into()))?;

        // Idempotent: only write if absent. A blind put is also fine for
        // content-addressed data (same key ⇒ same value), but the existence
        // check avoids an unnecessary write amplification.
        if self.db.get_cf(&cf, hash.as_bytes()).map_err(|e| CasError::Store(e.to_string()))?.is_none() {
            self.db
                .put_cf(&cf, hash.as_bytes(), &data)
                .map_err(|e| CasError::Store(e.to_string()))?;
        }

        Ok(hash)
    }

    fn get_chunk(&self, hash: &ChunkHash) -> CasResult<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(CF_CHUNKS)
            .ok_or_else(|| CasError::Store("missing CF: chunks".into()))?;

        self.db
            .get_cf(&cf, hash.as_bytes())
            .map_err(|e| CasError::Store(e.to_string()))
    }

    fn has_chunk(&self, hash: &ChunkHash) -> CasResult<bool> {
        let cf = self
            .db
            .cf_handle(CF_CHUNKS)
            .ok_or_else(|| CasError::Store("missing CF: chunks".into()))?;

        // Use get_pinned to avoid an allocation when we only care about existence.
        let pinned = self
            .db
            .get_pinned_cf(&cf, hash.as_bytes())
            .map_err(|e| CasError::Store(e.to_string()))?;

        Ok(pinned.is_some())
    }
}

// ---------------------------------------------------------------------------
// TreeStore
// ---------------------------------------------------------------------------

impl TreeStore for RocksDbCasStore {
    fn put_tree_node(&self, node: &TreeNode) -> CasResult<()> {
        let cf = self
            .db
            .cf_handle(CF_TREES)
            .ok_or_else(|| CasError::Store("missing CF: trees".into()))?;

        // Idempotent: skip write if key already present.
        if self.db.get_pinned_cf(&cf, node.hash.as_bytes()).map_err(|e| CasError::Store(e.to_string()))?.is_none() {
            let value = serde_json::to_vec(node)?;
            self.db
                .put_cf(&cf, node.hash.as_bytes(), &value)
                .map_err(|e| CasError::Store(e.to_string()))?;
        }

        Ok(())
    }

    fn get_tree_node(&self, hash: &ChunkHash) -> CasResult<Option<TreeNode>> {
        let cf = self
            .db
            .cf_handle(CF_TREES)
            .ok_or_else(|| CasError::Store("missing CF: trees".into()))?;

        match self.db.get_cf(&cf, hash.as_bytes()).map_err(|e| CasError::Store(e.to_string()))? {
            Some(bytes) => {
                let node: TreeNode = serde_json::from_slice(&bytes)?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// NodeStore
// ---------------------------------------------------------------------------

impl NodeStore for RocksDbCasStore {
    fn put_node(&self, entry: &NodeEntry) -> CasResult<()> {
        let cf = self
            .db
            .cf_handle(CF_NODES)
            .ok_or_else(|| CasError::Store("missing CF: nodes".into()))?;

        if self.db.get_pinned_cf(&cf, entry.hash.as_bytes()).map_err(|e| CasError::Store(e.to_string()))?.is_none() {
            let value = serde_json::to_vec(entry)?;
            self.db
                .put_cf(&cf, entry.hash.as_bytes(), &value)
                .map_err(|e| CasError::Store(e.to_string()))?;
        }

        Ok(())
    }

    fn get_node(&self, hash: &ChunkHash) -> CasResult<Option<NodeEntry>> {
        let cf = self
            .db
            .cf_handle(CF_NODES)
            .ok_or_else(|| CasError::Store("missing CF: nodes".into()))?;

        match self.db.get_cf(&cf, hash.as_bytes()).map_err(|e| CasError::Store(e.to_string()))? {
            Some(bytes) => {
                let entry: NodeEntry = serde_json::from_slice(&bytes)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }
}
