use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{params, Connection};

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, Ref, RefKind, VersionId, VersionNode};
use super::traits::{PathEntry, PathIndexRepo, RefRepo, VersionRepo};

// ---------------------------------------------------------------------------
// Schema migration
// ---------------------------------------------------------------------------

/// Current schema version. Bump this when adding new migrations.
#[allow(dead_code)]
const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Apply all pending migrations up to `CURRENT_SCHEMA_VERSION`.
fn run_migrations(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;

    let current: u32 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |row| row.get(0),
        )
        .map_err(|e| CasError::Store(e.to_string()))?;

    if current < 1 {
        migrate_v1(conn)?;
    }

    Ok(())
}

/// Migration v1: initial schema.
fn migrate_v1(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "
        -- Version DAG nodes
        CREATE TABLE versions (
            id         TEXT PRIMARY KEY,
            root       TEXT NOT NULL,
            message    TEXT NOT NULL,
            timestamp  INTEGER NOT NULL,
            metadata   TEXT NOT NULL DEFAULT '{}'
        );

        -- Multi-parent support for merge commits
        CREATE TABLE version_parents (
            version_id TEXT NOT NULL,
            parent_id  TEXT NOT NULL,
            ordinal    INTEGER NOT NULL,
            PRIMARY KEY (version_id, ordinal),
            FOREIGN KEY (version_id) REFERENCES versions(id),
            FOREIGN KEY (parent_id) REFERENCES versions(id)
        );

        -- Branch and tag refs
        CREATE TABLE refs (
            name   TEXT PRIMARY KEY,
            kind   TEXT NOT NULL CHECK (kind IN ('branch', 'tag')),
            target TEXT NOT NULL,
            FOREIGN KEY (target) REFERENCES versions(id)
        );

        -- Directory tree nodes (content-addressed)
        CREATE TABLE nodes (
            hash TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
            data TEXT NOT NULL
        );

        -- Directory children mapping
        CREATE TABLE directory_entries (
            parent_hash TEXT NOT NULL,
            name        TEXT NOT NULL,
            child_hash  TEXT NOT NULL,
            PRIMARY KEY (parent_hash, name),
            FOREIGN KEY (parent_hash) REFERENCES nodes(hash),
            FOREIGN KEY (child_hash) REFERENCES nodes(hash)
        );

        -- Path index: maps (version_id, path) -> node info
        CREATE TABLE file_versions (
            version_id TEXT NOT NULL,
            path       TEXT NOT NULL,
            node_hash  TEXT NOT NULL,
            node_kind  TEXT NOT NULL CHECK (node_kind IN ('file', 'directory')),
            PRIMARY KEY (version_id, path)
        );

        -- Extended commit info
        CREATE TABLE commits (
            version_id TEXT PRIMARY KEY,
            author     TEXT NOT NULL DEFAULT '',
            committer  TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (version_id) REFERENCES versions(id)
        );

        -- Indexes for common queries
        CREATE INDEX idx_version_parents_parent ON version_parents(parent_id);
        CREATE INDEX idx_file_versions_path ON file_versions(path);
        CREATE INDEX idx_refs_target ON refs(target);

        INSERT INTO schema_version (version) VALUES (1);
        ",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// SqliteMetadataStore — single connection wrapper
// ---------------------------------------------------------------------------

/// SQLite-backed metadata store implementing `VersionRepo`, `RefRepo`, and
/// `PathIndexRepo`. All three share one database file.
pub struct SqliteMetadataStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteMetadataStore {
    /// Open (or create) a SQLite metadata store at the given file path.
    pub fn open<P: AsRef<Path>>(path: P) -> CasResult<Self> {
        let conn =
            Connection::open(path).map_err(|e| CasError::Store(e.to_string()))?;

        // Enable WAL for better concurrent read performance.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| CasError::Store(e.to_string()))?;

        run_migrations(&conn)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open an in-memory SQLite database (useful for tests).
    pub fn open_in_memory() -> CasResult<Self> {
        let conn =
            Connection::open_in_memory().map_err(|e| CasError::Store(e.to_string()))?;

        conn.execute_batch("PRAGMA foreign_keys=ON;")
            .map_err(|e| CasError::Store(e.to_string()))?;

        run_migrations(&conn)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

// ---------------------------------------------------------------------------
// Helper: ChunkHash <-> hex string
// ---------------------------------------------------------------------------

fn hash_to_hex(h: &ChunkHash) -> String {
    h.to_hex().to_string()
}

fn hex_to_hash(s: &str) -> CasResult<ChunkHash> {
    let bytes = hex::decode(s).map_err(|e| CasError::Store(format!("invalid hex hash: {e}")))?;
    if bytes.len() != 32 {
        return Err(CasError::Store("hash must be 32 bytes".into()));
    }
    let arr: [u8; 32] = bytes.try_into().unwrap();
    Ok(blake3::Hash::from_bytes(arr))
}

// ---------------------------------------------------------------------------
// VersionRepo
// ---------------------------------------------------------------------------

impl VersionRepo for SqliteMetadataStore {
    fn put_version(&self, version: &VersionNode) -> CasResult<()> {
        let conn = self.conn.lock();
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| CasError::Store(e.to_string()))?;

        let metadata_json =
            serde_json::to_string(&version.metadata)?;

        tx.execute(
            "INSERT OR REPLACE INTO versions (id, root, message, timestamp, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                version.id.as_str(),
                hash_to_hex(&version.root),
                version.message,
                version.timestamp,
                metadata_json,
            ],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;

        // Remove old parent rows (idempotent upsert).
        tx.execute(
            "DELETE FROM version_parents WHERE version_id = ?1",
            params![version.id.as_str()],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;

        for (i, parent) in version.parents.iter().enumerate() {
            tx.execute(
                "INSERT INTO version_parents (version_id, parent_id, ordinal)
                 VALUES (?1, ?2, ?3)",
                params![version.id.as_str(), parent.as_str(), i as i64],
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        }

        tx.commit()
            .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn get_version(&self, id: &VersionId) -> CasResult<Option<VersionNode>> {
        let conn = self.conn.lock();

        let mut stmt = conn
            .prepare(
                "SELECT id, root, message, timestamp, metadata FROM versions WHERE id = ?1",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;

        let version_opt = stmt
            .query_row(params![id.as_str()], |row| {
                let id_str: String = row.get(0)?;
                let root_hex: String = row.get(1)?;
                let message: String = row.get(2)?;
                let timestamp: u64 = row.get(3)?;
                let metadata_json: String = row.get(4)?;
                Ok((id_str, root_hex, message, timestamp, metadata_json))
            })
            .optional()
            .map_err(|e| CasError::Store(e.to_string()))?;

        let Some((id_str, root_hex, message, timestamp, metadata_json)) = version_opt else {
            return Ok(None);
        };

        let root = hex_to_hash(&root_hex)?;
        let metadata: std::collections::HashMap<String, String> =
            serde_json::from_str(&metadata_json)?;

        // Fetch parents ordered by ordinal.
        let mut parent_stmt = conn
            .prepare(
                "SELECT parent_id FROM version_parents
                 WHERE version_id = ?1 ORDER BY ordinal",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;

        let parents: Vec<VersionId> = parent_stmt
            .query_map(params![id_str], |row| {
                let pid: String = row.get(0)?;
                Ok(VersionId(pid))
            })
            .map_err(|e| CasError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CasError::Store(e.to_string()))?;

        Ok(Some(VersionNode {
            id: VersionId(id_str),
            parents,
            root,
            message,
            timestamp,
            metadata,
        }))
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

impl RefRepo for SqliteMetadataStore {
    fn put_ref(&self, r: &Ref) -> CasResult<()> {
        let conn = self.conn.lock();

        // Check tag immutability.
        let existing: Option<String> = conn
            .query_row(
                "SELECT kind FROM refs WHERE name = ?1",
                params![r.name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| CasError::Store(e.to_string()))?;

        if let Some(kind_str) = existing {
            if kind_str == "tag" {
                return Err(CasError::AlreadyExists);
            }
        }

        let kind_str = match r.kind {
            RefKind::Branch => "branch",
            RefKind::Tag => "tag",
        };

        conn.execute(
            "INSERT OR REPLACE INTO refs (name, kind, target) VALUES (?1, ?2, ?3)",
            params![r.name, kind_str, r.target.as_str()],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;

        Ok(())
    }

    fn get_ref(&self, name: &str) -> CasResult<Option<Ref>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT name, kind, target FROM refs WHERE name = ?1")
            .map_err(|e| CasError::Store(e.to_string()))?;

        let ref_opt = stmt
            .query_row(params![name], |row| {
                let name: String = row.get(0)?;
                let kind_str: String = row.get(1)?;
                let target: String = row.get(2)?;
                Ok((name, kind_str, target))
            })
            .optional()
            .map_err(|e| CasError::Store(e.to_string()))?;

        match ref_opt {
            Some((name, kind_str, target)) => {
                let kind = match kind_str.as_str() {
                    "branch" => RefKind::Branch,
                    "tag" => RefKind::Tag,
                    other => {
                        return Err(CasError::InvalidRef(format!(
                            "unknown ref kind: {other}"
                        )))
                    }
                };
                Ok(Some(Ref {
                    name,
                    kind,
                    target: VersionId(target),
                }))
            }
            None => Ok(None),
        }
    }

    fn delete_ref(&self, name: &str) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM refs WHERE name = ?1", params![name])
            .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn list_refs(&self, kind: Option<RefKind>) -> CasResult<Vec<Ref>> {
        let conn = self.conn.lock();

        let kind_val = match &kind {
            Some(RefKind::Branch) => Some("branch"),
            Some(RefKind::Tag) => Some("tag"),
            None => None,
        };

        let raw_rows: Vec<(String, String, String)> = if let Some(kv) = kind_val {
            let mut stmt = conn
                .prepare("SELECT name, kind, target FROM refs WHERE kind = ?1")
                .map_err(|e| CasError::Store(e.to_string()))?;
            stmt.query_map(params![kv], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|e| CasError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CasError::Store(e.to_string()))?
        } else {
            let mut stmt = conn
                .prepare("SELECT name, kind, target FROM refs")
                .map_err(|e| CasError::Store(e.to_string()))?;
            stmt.query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|e| CasError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CasError::Store(e.to_string()))?
        };

        let mut refs = Vec::new();
        for (name, kind_str, target) in raw_rows {
            let kind = match kind_str.as_str() {
                "branch" => RefKind::Branch,
                "tag" => RefKind::Tag,
                other => {
                    return Err(CasError::InvalidRef(format!(
                        "unknown ref kind: {other}"
                    )))
                }
            };
            refs.push(Ref {
                name,
                kind,
                target: VersionId(target),
            });
        }

        Ok(refs)
    }
}

// ---------------------------------------------------------------------------
// PathIndexRepo
// ---------------------------------------------------------------------------

impl PathIndexRepo for SqliteMetadataStore {
    fn put_path_entry(
        &self,
        version_id: &VersionId,
        path: &str,
        node_hash: &ChunkHash,
        is_directory: bool,
    ) -> CasResult<()> {
        let conn = self.conn.lock();
        let kind_str = if is_directory { "directory" } else { "file" };

        conn.execute(
            "INSERT OR REPLACE INTO file_versions (version_id, path, node_hash, node_kind)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                version_id.as_str(),
                path,
                hash_to_hex(node_hash),
                kind_str,
            ],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;

        Ok(())
    }

    fn get_by_path(
        &self,
        version_id: &VersionId,
        path: &str,
    ) -> CasResult<Option<PathEntry>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT path, node_hash, node_kind FROM file_versions
                 WHERE version_id = ?1 AND path = ?2",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;

        let entry_opt = stmt
            .query_row(params![version_id.as_str(), path], |row| {
                let path: String = row.get(0)?;
                let hash_hex: String = row.get(1)?;
                let kind_str: String = row.get(2)?;
                Ok((path, hash_hex, kind_str))
            })
            .optional()
            .map_err(|e| CasError::Store(e.to_string()))?;

        match entry_opt {
            Some((path, hash_hex, kind_str)) => {
                let node_hash = hex_to_hash(&hash_hex)?;
                let is_directory = kind_str == "directory";
                Ok(Some(PathEntry {
                    path,
                    node_hash,
                    is_directory,
                }))
            }
            None => Ok(None),
        }
    }

    fn list_directory(
        &self,
        version_id: &VersionId,
        dir_path: &str,
    ) -> CasResult<Vec<PathEntry>> {
        let conn = self.conn.lock();

        // Normalize: ensure dir_path ends with "/" for prefix matching,
        // unless it's empty (root directory).
        let prefix = if dir_path.is_empty() {
            String::new()
        } else if dir_path.ends_with('/') {
            dir_path.to_string()
        } else {
            format!("{dir_path}/")
        };

        let mut stmt = conn
            .prepare(
                "SELECT path, node_hash, node_kind FROM file_versions
                 WHERE version_id = ?1 AND path LIKE ?2 AND path != ?3",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;

        let like_pattern = format!("{prefix}%");

        let rows = stmt
            .query_map(
                params![version_id.as_str(), like_pattern, dir_path],
                |row| {
                    let path: String = row.get(0)?;
                    let hash_hex: String = row.get(1)?;
                    let kind_str: String = row.get(2)?;
                    Ok((path, hash_hex, kind_str))
                },
            )
            .map_err(|e| CasError::Store(e.to_string()))?;

        let mut entries = Vec::new();
        for row in rows {
            let (path, hash_hex, kind_str) =
                row.map_err(|e| CasError::Store(e.to_string()))?;

            // Only include immediate children: no additional '/' after the prefix.
            let relative = &path[prefix.len()..];
            if relative.contains('/') {
                continue;
            }

            let node_hash = hex_to_hash(&hash_hex)?;
            let is_directory = kind_str == "directory";
            entries.push(PathEntry {
                path,
                node_hash,
                is_directory,
            });
        }

        Ok(entries)
    }
}

// We need the `optional` extension from rusqlite.
use rusqlite::OptionalExtension;

// We need `hex` for hash conversion. Since we already have blake3,
// let's use a minimal inline hex decoder to avoid adding another dep.
mod hex {
    pub fn decode(s: &str) -> Result<Vec<u8>, String> {
        if s.len() % 2 != 0 {
            return Err("odd-length hex string".into());
        }
        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16)
                    .map_err(|e| format!("invalid hex at offset {i}: {e}"))
            })
            .collect()
    }
}
