use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::{params, Connection};

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, Ref, RefKind, VersionId, VersionNode};
use super::traits::{
    ObjectManifestRepo, PathEntry, PathIndexRepo, RefRepo, Remote, RemoteRef, RemoteRepo,
    RemoteTrackingRepo, SyncDirection, SyncSession, SyncSessionRepo, SyncSessionStatus,
    VersionRepo, WtCacheEntry, WtCacheRepo,
};

// ---------------------------------------------------------------------------
// Schema migration
// ---------------------------------------------------------------------------

/// Current schema version. Bump this when adding new migrations.
const CURRENT_SCHEMA_VERSION: u32 = 8;

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

    if current < CURRENT_SCHEMA_VERSION {
        // Run all pending migrations inside a single transaction so a
        // failure at any step rolls back the entire batch.
        conn.execute_batch("BEGIN;")
            .map_err(|e| CasError::Store(e.to_string()))?;

        let result = (|| {
            if current < 1 {
                migrate_v1(conn)?;
            }
            if current < 2 {
                migrate_v2(conn)?;
            }
            if current < 3 {
                migrate_v3(conn)?;
            }
            if current < 4 {
                migrate_v4(conn)?;
            }
            if current < 5 {
                migrate_v5(conn)?;
            }
            if current < 6 {
                migrate_v6(conn)?;
            }
            if current < 7 {
                migrate_v7(conn)?;
            }
            if current < 8 {
                migrate_v8(conn)?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                conn.execute_batch("COMMIT;")
                    .map_err(|e| CasError::Store(e.to_string()))?;
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(e);
            }
        }
    }

    assert_eq!(
        get_schema_version(conn)?,
        CURRENT_SCHEMA_VERSION,
        "schema_version mismatch after migrations",
    );

    Ok(())
}

/// Read the current schema version from the database.
fn get_schema_version(conn: &Connection) -> CasResult<u32> {
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_version",
        [],
        |row| row.get(0),
    )
    .map_err(|e| CasError::Store(e.to_string()))
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

/// Migration v2: SaaS multi-tenant tables.
///
/// Adds `workspaces`, `remotes`, `remote_refs`, and `sync_sessions` tables.
/// Creates a "default" workspace for existing data.
fn migrate_v2(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "
        -- Workspace isolation
        CREATE TABLE IF NOT EXISTS workspaces (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            created_at  INTEGER NOT NULL,
            metadata    TEXT NOT NULL DEFAULT '{}'
        );

        -- Remote server endpoints for sync
        CREATE TABLE IF NOT EXISTS remotes (
            name        TEXT PRIMARY KEY,
            url         TEXT NOT NULL,
            auth_token  TEXT NOT NULL DEFAULT '',
            created_at  INTEGER NOT NULL
        );

        -- Refs on remote servers (last-known state)
        CREATE TABLE IF NOT EXISTS remote_refs (
            remote_name TEXT NOT NULL,
            ref_name    TEXT NOT NULL,
            kind        TEXT NOT NULL CHECK (kind IN ('branch', 'tag')),
            target      TEXT NOT NULL,
            updated_at  INTEGER NOT NULL,
            PRIMARY KEY (remote_name, ref_name),
            FOREIGN KEY (remote_name) REFERENCES remotes(name)
        );

        -- Sync session log
        CREATE TABLE IF NOT EXISTS sync_sessions (
            id          TEXT PRIMARY KEY,
            remote_name TEXT NOT NULL,
            direction   TEXT NOT NULL CHECK (direction IN ('push', 'pull')),
            status      TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
            started_at  INTEGER NOT NULL,
            finished_at INTEGER,
            objects_transferred INTEGER NOT NULL DEFAULT 0,
            bytes_transferred   INTEGER NOT NULL DEFAULT 0,
            error_message       TEXT,
            FOREIGN KEY (remote_name) REFERENCES remotes(name)
        );

        -- Create default workspace for existing data
        INSERT OR IGNORE INTO workspaces (id, name, created_at, metadata)
        VALUES ('default', 'default', strftime('%s', 'now'), '{}');

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_remote_refs_remote ON remote_refs(remote_name);
        CREATE INDEX IF NOT EXISTS idx_sync_sessions_remote ON sync_sessions(remote_name);

        -- Advance schema version
        UPDATE schema_version SET version = 2;
        ",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;

    Ok(())
}

/// Migration v3: add tenant_id / workspace_id columns to remotes.
fn migrate_v3(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "
        ALTER TABLE remotes ADD COLUMN tenant_id TEXT;
        ALTER TABLE remotes ADD COLUMN workspace_id TEXT;
        UPDATE schema_version SET version = 3;
        ",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;
    Ok(())
}

/// Migration v4: rebuild `remote_refs` and `sync_sessions` so their FK to
/// `remotes(name)` declares `ON DELETE CASCADE`.
///
/// SQLite cannot modify foreign keys in place, so we follow the standard
/// rebuild dance: rename old table → create new with corrected FK → copy
/// data → drop old → recreate indexes. `PRAGMA foreign_keys` must be OFF
/// during the rebuild (the entire migration runs inside the v1+ outer
/// transaction; we wrap with `defer_foreign_keys` instead, which is the
/// transaction-scoped equivalent and does not require leaving the txn).
fn migrate_v4(conn: &Connection) -> CasResult<()> {
    // Pre-clean orphan rows before rebuilding with FK constraints.
    // In a v3 DB that existed before FK constraints were enforced, rows whose
    // `remote_name` no longer matches any `remotes.name` would cause the
    // INSERT into the new tables to violate the FK at COMMIT time (because we
    // use `defer_foreign_keys`).  Clean them up first and log the count so
    // operators know if stale data was removed.
    let orphan_remote_refs = conn
        .execute(
            "DELETE FROM remote_refs WHERE remote_name NOT IN (SELECT name FROM remotes)",
            [],
        )
        .map_err(|e| CasError::Store(format!("migrate_v4 pre-clean remote_refs: {e}")))?;

    let orphan_sync_sessions = conn
        .execute(
            "DELETE FROM sync_sessions WHERE remote_name NOT IN (SELECT name FROM remotes)",
            [],
        )
        .map_err(|e| CasError::Store(format!("migrate_v4 pre-clean sync_sessions: {e}")))?;

    if orphan_remote_refs > 0 || orphan_sync_sessions > 0 {
        tracing::warn!(
            orphan_remote_refs,
            orphan_sync_sessions,
            "migrate_v4: removed orphan rows before FK rebuild"
        );
    }

    conn.execute_batch(
        "
        PRAGMA defer_foreign_keys = ON;

        -- ── remote_refs ────────────────────────────────────────────────
        CREATE TABLE remote_refs_new (
            remote_name TEXT NOT NULL,
            ref_name    TEXT NOT NULL,
            kind        TEXT NOT NULL CHECK (kind IN ('branch', 'tag')),
            target      TEXT NOT NULL,
            updated_at  INTEGER NOT NULL,
            PRIMARY KEY (remote_name, ref_name),
            FOREIGN KEY (remote_name) REFERENCES remotes(name) ON DELETE CASCADE
        );
        INSERT INTO remote_refs_new
            SELECT remote_name, ref_name, kind, target, updated_at FROM remote_refs;
        DROP TABLE remote_refs;
        ALTER TABLE remote_refs_new RENAME TO remote_refs;
        CREATE INDEX IF NOT EXISTS idx_remote_refs_remote ON remote_refs(remote_name);

        -- ── sync_sessions ──────────────────────────────────────────────
        CREATE TABLE sync_sessions_new (
            id          TEXT PRIMARY KEY,
            remote_name TEXT NOT NULL,
            direction   TEXT NOT NULL CHECK (direction IN ('push', 'pull')),
            status      TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
            started_at  INTEGER NOT NULL,
            finished_at INTEGER,
            objects_transferred INTEGER NOT NULL DEFAULT 0,
            bytes_transferred   INTEGER NOT NULL DEFAULT 0,
            error_message       TEXT,
            FOREIGN KEY (remote_name) REFERENCES remotes(name) ON DELETE CASCADE
        );
        INSERT INTO sync_sessions_new
            SELECT id, remote_name, direction, status, started_at, finished_at,
                   objects_transferred, bytes_transferred, error_message
            FROM sync_sessions;
        DROP TABLE sync_sessions;
        ALTER TABLE sync_sessions_new RENAME TO sync_sessions;
        CREATE INDEX IF NOT EXISTS idx_sync_sessions_remote ON sync_sessions(remote_name);

        UPDATE schema_version SET version = 4;
        ",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;
    Ok(())
}

/// Migration v5: add `sync_manifests` table for E05-S03 resume support.
fn migrate_v6(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "ALTER TABLE workspaces ADD COLUMN local_path TEXT;
         ALTER TABLE workspaces ADD COLUMN current_ref TEXT;
         ALTER TABLE workspaces ADD COLUMN current_version TEXT;
         UPDATE schema_version SET version = 6;",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;
    Ok(())
}

fn migrate_v7(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS wt_cache (
            workspace_id  TEXT    NOT NULL,
            path          TEXT    NOT NULL,
            mtime_ns      INTEGER NOT NULL,
            size          INTEGER NOT NULL,
            hash_hex      TEXT    NOT NULL,
            PRIMARY KEY (workspace_id, path)
        );
        CREATE INDEX IF NOT EXISTS idx_wt_cache_workspace
            ON wt_cache(workspace_id);
        UPDATE schema_version SET version = 7;
        ",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;
    Ok(())
}

/// Migration v8: add `deleted_at` column to `workspaces` for soft-delete
/// (E11-S04 recycle bin).
fn migrate_v8(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "ALTER TABLE workspaces ADD COLUMN deleted_at INTEGER;
         CREATE INDEX IF NOT EXISTS idx_workspaces_deleted_at
             ON workspaces(deleted_at);
         UPDATE schema_version SET version = 8;",
    )
    .map_err(|e| CasError::Store(e.to_string()))?;
    Ok(())
}

fn migrate_v5(conn: &Connection) -> CasResult<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sync_manifests (
            session_id  TEXT    NOT NULL,
            hash_hex    TEXT    NOT NULL,
            PRIMARY KEY (session_id, hash_hex)
        );
        CREATE INDEX IF NOT EXISTS idx_sync_manifests_session
            ON sync_manifests(session_id);

        UPDATE schema_version SET version = 5;
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

// ---------------------------------------------------------------------------
// WorkspaceRepo
// ---------------------------------------------------------------------------

impl crate::store::traits::WorkspaceRepo for SqliteMetadataStore {
    fn create_workspace(&self, ws: &crate::store::traits::Workspace) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO workspaces (id, name, created_at, metadata, local_path, current_ref, current_version) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![ws.id, ws.name, ws.created_at, ws.metadata, ws.local_path.as_deref(), ws.current_ref.as_deref(), ws.current_version.as_deref()],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn get_workspace(&self, id: &str) -> CasResult<Option<crate::store::traits::Workspace>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT id, name, created_at, metadata, local_path, current_ref, current_version, deleted_at FROM workspaces WHERE id = ?1")
            .map_err(|e| CasError::Store(e.to_string()))?;
        stmt.query_row(params![id], |row| {
            Ok(crate::store::traits::Workspace {
                id: row.get(0)?,
                name: row.get(1)?,
                created_at: row.get::<_, i64>(2)? as u64,
                metadata: row.get(3)?,
                local_path: row.get(4)?,
                current_ref: row.get(5)?,
                current_version: row.get(6)?,
                deleted_at: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
            })
        })
        .optional()
        .map_err(|e| CasError::Store(e.to_string()))
    }

    fn list_workspaces(&self) -> CasResult<Vec<crate::store::traits::Workspace>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT id, name, created_at, metadata, local_path, current_ref, current_version, deleted_at FROM workspaces WHERE deleted_at IS NULL ORDER BY created_at ASC")
            .map_err(|e| CasError::Store(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::store::traits::Workspace {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    created_at: row.get::<_, i64>(2)? as u64,
                    metadata: row.get(3)?,
                    local_path: row.get(4)?,
                    current_ref: row.get(5)?,
                    current_version: row.get(6)?,
                    deleted_at: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                })
            })
            .map_err(|e| CasError::Store(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| CasError::Store(e.to_string()))?);
        }
        Ok(out)
    }

    fn delete_workspace(&self, id: &str) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM workspaces WHERE id = ?1", params![id])
            .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn update_workspace(&self, ws: &crate::store::traits::Workspace) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE workspaces SET name=?2, metadata=?3, local_path=?4, current_ref=?5, current_version=?6 WHERE id=?1",
            params![ws.id, ws.name, ws.metadata, ws.local_path.as_deref(), ws.current_ref.as_deref(), ws.current_version.as_deref()],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn soft_delete_workspace(&self, id: &str, deleted_at: u64) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE workspaces SET deleted_at=?2 WHERE id=?1 AND deleted_at IS NULL",
            params![id, deleted_at as i64],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn restore_workspace(&self, id: &str) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE workspaces SET deleted_at=NULL WHERE id=?1",
            params![id],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn list_deleted_workspaces(&self) -> CasResult<Vec<crate::store::traits::Workspace>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT id, name, created_at, metadata, local_path, current_ref, current_version, deleted_at FROM workspaces WHERE deleted_at IS NOT NULL ORDER BY deleted_at ASC")
            .map_err(|e| CasError::Store(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(crate::store::traits::Workspace {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    created_at: row.get::<_, i64>(2)? as u64,
                    metadata: row.get(3)?,
                    local_path: row.get(4)?,
                    current_ref: row.get(5)?,
                    current_version: row.get(6)?,
                    deleted_at: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                })
            })
            .map_err(|e| CasError::Store(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| CasError::Store(e.to_string()))?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// RemoteRepo
// ---------------------------------------------------------------------------

impl RemoteRepo for SqliteMetadataStore {
    fn add_remote(&self, remote: &Remote) -> CasResult<()> {
        let conn = self.conn.lock();
        let rows = conn
            .execute(
                "INSERT INTO remotes (name, url, auth_token, tenant_id, workspace_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    remote.name,
                    remote.url,
                    remote.auth_token,
                    remote.tenant_id,
                    remote.workspace_id,
                    remote.created_at as i64,
                ],
            )
            .map_err(|e| {
                // SQLite UNIQUE constraint violation → AlreadyExists
                if e.to_string().contains("UNIQUE") {
                    CasError::AlreadyExists
                } else {
                    CasError::Store(e.to_string())
                }
            })?;
        debug_assert_eq!(rows, 1);
        Ok(())
    }

    fn remove_remote(&self, name: &str) -> CasResult<()> {
        let conn = self.conn.lock();
        // FKs on remote_refs and sync_sessions are declared with
        // ON DELETE CASCADE (schema v4), so a single delete on `remotes`
        // is enough to clean both child tables.
        conn.execute("DELETE FROM remotes WHERE name = ?1", params![name])
            .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn get_remote(&self, name: &str) -> CasResult<Option<Remote>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT name, url, auth_token, tenant_id, workspace_id, created_at
                 FROM remotes WHERE name = ?1",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        stmt.query_row(params![name], |row| {
            Ok(Remote {
                name: row.get(0)?,
                url: row.get(1)?,
                auth_token: row.get(2)?,
                tenant_id: row.get(3)?,
                workspace_id: row.get(4)?,
                created_at: row.get::<_, i64>(5)? as u64,
            })
        })
        .optional()
        .map_err(|e| CasError::Store(e.to_string()))
    }

    fn list_remotes(&self) -> CasResult<Vec<Remote>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT name, url, auth_token, tenant_id, workspace_id, created_at
                 FROM remotes ORDER BY created_at ASC",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(Remote {
                    name: row.get(0)?,
                    url: row.get(1)?,
                    auth_token: row.get(2)?,
                    tenant_id: row.get(3)?,
                    workspace_id: row.get(4)?,
                    created_at: row.get::<_, i64>(5)? as u64,
                })
            })
            .map_err(|e| CasError::Store(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| CasError::Store(e.to_string()))?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// RemoteTrackingRepo
// ---------------------------------------------------------------------------

impl RemoteTrackingRepo for SqliteMetadataStore {
    fn update_remote_ref(&self, r: &RemoteRef) -> CasResult<()> {
        let conn = self.conn.lock();
        let kind_str = match r.kind {
            RefKind::Branch => "branch",
            RefKind::Tag => "tag",
        };
        conn.execute(
            "INSERT OR REPLACE INTO remote_refs
             (remote_name, ref_name, kind, target, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![r.remote_name, r.ref_name, kind_str, r.target.as_str(), r.updated_at as i64],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn get_remote_ref(
        &self,
        remote_name: &str,
        ref_name: &str,
    ) -> CasResult<Option<RemoteRef>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT remote_name, ref_name, kind, target, updated_at
                 FROM remote_refs WHERE remote_name = ?1 AND ref_name = ?2",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        stmt.query_row(params![remote_name, ref_name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .optional()
        .map_err(|e| CasError::Store(e.to_string()))?
        .map(|(rn, rfn, kind_str, target, updated_at)| {
            let kind = if kind_str == "tag" { RefKind::Tag } else { RefKind::Branch };
            Ok(RemoteRef {
                remote_name: rn,
                ref_name: rfn,
                kind,
                target: VersionId(target),
                updated_at: updated_at as u64,
            })
        })
        .transpose()
    }

    fn list_remote_refs(&self, remote_name: &str) -> CasResult<Vec<RemoteRef>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT remote_name, ref_name, kind, target, updated_at
                 FROM remote_refs WHERE remote_name = ?1 ORDER BY ref_name ASC",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        let rows = stmt
            .query_map(params![remote_name], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(|e| CasError::Store(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            let (rn, rfn, kind_str, target, updated_at) =
                r.map_err(|e| CasError::Store(e.to_string()))?;
            let kind = if kind_str == "tag" { RefKind::Tag } else { RefKind::Branch };
            out.push(RemoteRef {
                remote_name: rn,
                ref_name: rfn,
                kind,
                target: VersionId(target),
                updated_at: updated_at as u64,
            });
        }
        Ok(out)
    }

    fn delete_remote_ref(&self, remote_name: &str, ref_name: &str) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM remote_refs WHERE remote_name = ?1 AND ref_name = ?2",
            params![remote_name, ref_name],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SyncSessionRepo (E04-S06)
// ---------------------------------------------------------------------------

fn row_to_sync_session(
    id: String,
    remote_name: String,
    direction: String,
    status: String,
    started_at: i64,
    finished_at: Option<i64>,
    objects_transferred: i64,
    bytes_transferred: i64,
    error_message: Option<String>,
) -> CasResult<SyncSession> {
    let direction = SyncDirection::parse(&direction)
        .ok_or_else(|| CasError::Store(format!("invalid direction: {direction}")))?;
    let status = SyncSessionStatus::parse(&status)
        .ok_or_else(|| CasError::Store(format!("invalid status: {status}")))?;
    Ok(SyncSession {
        id,
        remote_name,
        direction,
        status,
        started_at: started_at as u64,
        finished_at: finished_at.map(|v| v as u64),
        objects_transferred: objects_transferred as u64,
        bytes_transferred: bytes_transferred as u64,
        error_message,
    })
}

impl SyncSessionRepo for SqliteMetadataStore {
    fn create_session(&self, session: &SyncSession) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO sync_sessions
             (id, remote_name, direction, status, started_at, finished_at,
              objects_transferred, bytes_transferred, error_message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                session.id,
                session.remote_name,
                session.direction.as_str(),
                session.status.as_str(),
                session.started_at as i64,
                session.finished_at.map(|v| v as i64),
                session.objects_transferred as i64,
                session.bytes_transferred as i64,
                session.error_message,
            ],
        )
        .map_err(|e| {
            if e.to_string().contains("UNIQUE") {
                CasError::AlreadyExists
            } else {
                CasError::Store(e.to_string())
            }
        })?;
        Ok(())
    }

    fn update_session(&self, session: &SyncSession) -> CasResult<()> {
        let conn = self.conn.lock();
        let n = conn
            .execute(
                "UPDATE sync_sessions SET
                    status = ?2,
                    finished_at = ?3,
                    objects_transferred = ?4,
                    bytes_transferred = ?5,
                    error_message = ?6
                 WHERE id = ?1",
                params![
                    session.id,
                    session.status.as_str(),
                    session.finished_at.map(|v| v as i64),
                    session.objects_transferred as i64,
                    session.bytes_transferred as i64,
                    session.error_message,
                ],
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        if n == 0 {
            return Err(CasError::NotFound(format!("sync session {}", session.id)));
        }
        Ok(())
    }

    fn get_session(&self, id: &str) -> CasResult<Option<SyncSession>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT id, remote_name, direction, status, started_at, finished_at,
                        objects_transferred, bytes_transferred, error_message
                 FROM sync_sessions WHERE id = ?1",
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        stmt.query_row(params![id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })
        .optional()
        .map_err(|e| CasError::Store(e.to_string()))?
        .map(|(id, rn, dir, st, sa, fa, ot, bt, em)| {
            row_to_sync_session(id, rn, dir, st, sa, fa, ot, bt, em)
        })
        .transpose()
    }

    fn list_sync_sessions(
        &self,
        remote: Option<&str>,
        limit: usize,
    ) -> CasResult<Vec<SyncSession>> {
        let conn = self.conn.lock();
        let limit_i64 = limit as i64;
        let rows: Vec<_> = if let Some(r) = remote {
            let mut stmt = conn
                .prepare(
                    "SELECT id, remote_name, direction, status, started_at, finished_at,
                            objects_transferred, bytes_transferred, error_message
                     FROM sync_sessions WHERE remote_name = ?1
                     ORDER BY started_at DESC LIMIT ?2",
                )
                .map_err(|e| CasError::Store(e.to_string()))?;
            stmt.query_map(params![r, limit_i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            })
            .map_err(|e| CasError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CasError::Store(e.to_string()))?
        } else {
            let mut stmt = conn
                .prepare(
                    "SELECT id, remote_name, direction, status, started_at, finished_at,
                            objects_transferred, bytes_transferred, error_message
                     FROM sync_sessions
                     ORDER BY started_at DESC LIMIT ?1",
                )
                .map_err(|e| CasError::Store(e.to_string()))?;
            stmt.query_map(params![limit_i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            })
            .map_err(|e| CasError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CasError::Store(e.to_string()))?
        };

        rows.into_iter()
            .map(|(id, rn, dir, st, sa, fa, ot, bt, em)| {
                row_to_sync_session(id, rn, dir, st, sa, fa, ot, bt, em)
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// ObjectManifestRepo (E05-S03 resume support)
// ---------------------------------------------------------------------------

impl ObjectManifestRepo for SqliteMetadataStore {
    fn manifest_append(&self, session_id: &str, hash_hexes: &[String]) -> CasResult<()> {
        let conn = self.conn.lock();
        for hex in hash_hexes {
            conn.execute(
                "INSERT OR IGNORE INTO sync_manifests (session_id, hash_hex) VALUES (?1, ?2)",
                params![session_id, hex],
            )
            .map_err(|e| CasError::Store(e.to_string()))?;
        }
        Ok(())
    }

    fn manifest_load(&self, session_id: &str) -> CasResult<Vec<String>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT hash_hex FROM sync_manifests WHERE session_id = ?1")
            .map_err(|e| CasError::Store(e.to_string()))?;
        let rows = stmt
            .query_map(params![session_id], |row| row.get::<_, String>(0))
            .map_err(|e| CasError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(rows)
    }

    fn manifest_delete(&self, session_id: &str) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM sync_manifests WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// list_all_version_ids helper (used by LocalSyncStore)
// ---------------------------------------------------------------------------

impl SqliteMetadataStore {
    /// Return every version id stored in this database.
    pub fn list_all_version_ids(&self) -> CasResult<Vec<VersionId>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT id FROM versions")
            .map_err(|e| CasError::Store(e.to_string()))?;
        let ids = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                Ok(VersionId(id))
            })
            .map_err(|e| CasError::Store(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(ids)
    }
}

// ---------------------------------------------------------------------------
// WtCacheRepo implementation
// ---------------------------------------------------------------------------

impl WtCacheRepo for SqliteMetadataStore {
    fn wt_cache_get(&self, workspace_id: &str, path: &str) -> CasResult<Option<WtCacheEntry>> {
        let conn = self.conn.lock();
        let result = conn.query_row(
            "SELECT mtime_ns, size, hash_hex FROM wt_cache
             WHERE workspace_id = ?1 AND path = ?2",
            rusqlite::params![workspace_id, path],
            |row| {
                Ok(WtCacheEntry {
                    mtime_ns: row.get(0)?,
                    size: row.get::<_, i64>(1)? as u64,
                    hash_hex: row.get(2)?,
                })
            },
        );
        match result {
            Ok(e) => Ok(Some(e)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(CasError::Store(e.to_string())),
        }
    }

    fn wt_cache_put(
        &self,
        workspace_id: &str,
        path: &str,
        entry: &WtCacheEntry,
    ) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO wt_cache (workspace_id, path, mtime_ns, size, hash_hex)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                workspace_id,
                path,
                entry.mtime_ns,
                entry.size as i64,
                entry.hash_hex,
            ],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }

    fn wt_cache_clear(&self, workspace_id: &str) -> CasResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM wt_cache WHERE workspace_id = ?1",
            rusqlite::params![workspace_id],
        )
        .map_err(|e| CasError::Store(e.to_string()))?;
        Ok(())
    }
}

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
