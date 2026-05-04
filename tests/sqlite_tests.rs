#![cfg(feature = "local")]

use std::collections::HashMap;

use axiom_core::model::{hash_bytes, Ref, RefKind, VersionId, VersionNode};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::traits::{PathIndexRepo, RefRepo, VersionRepo};
use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_version(id: &str, parents: Vec<&str>, message: &str) -> VersionNode {
    let root = hash_bytes(id.as_bytes());
    VersionNode {
        id: VersionId::from(id),
        parents: parents.into_iter().map(VersionId::from).collect(),
        root,
        message: message.to_string(),
        timestamp: 1_700_000_000,
        metadata: HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Migration tests
// ---------------------------------------------------------------------------

#[test]
fn test_migration_creates_schema_from_empty() {
    let store = SqliteMetadataStore::open_in_memory().expect("open in-memory");
    // If we got here, migration succeeded. Verify by storing a version.
    let v = make_version("v1", vec![], "init");
    store.put_version(&v).expect("put_version after migration");
    let got = store.get_version(&v.id).expect("get_version");
    assert!(got.is_some());
}

#[test]
fn test_migration_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");

    // First open — creates schema.
    {
        let store = SqliteMetadataStore::open(&db_path).expect("first open");
        let v = make_version("v1", vec![], "init");
        store.put_version(&v).unwrap();
    }

    // Second open — schema already exists, migration should be a no-op.
    {
        let store = SqliteMetadataStore::open(&db_path).expect("second open");
        let got = store
            .get_version(&VersionId::from("v1"))
            .expect("get after reopen");
        assert!(got.is_some());
        assert_eq!(got.unwrap().message, "init");
    }
}

// ---------------------------------------------------------------------------
// v1→v2 migration tests
// ---------------------------------------------------------------------------

/// Helper: create a v1-only database by applying the v1 schema directly.
fn create_v1_database(path: &std::path::Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;").unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_version (version INTEGER NOT NULL);
         CREATE TABLE versions (
             id TEXT PRIMARY KEY, root TEXT NOT NULL, message TEXT NOT NULL,
             timestamp INTEGER NOT NULL, metadata TEXT NOT NULL DEFAULT '{}'
         );
         CREATE TABLE version_parents (
             version_id TEXT NOT NULL, parent_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
             PRIMARY KEY (version_id, ordinal)
         );
         CREATE TABLE refs (
             name TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK (kind IN ('branch','tag')),
             target TEXT NOT NULL
         );
         CREATE TABLE nodes (
             hash TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK (kind IN ('file','directory')),
             data TEXT NOT NULL
         );
         CREATE TABLE directory_entries (
             parent_hash TEXT NOT NULL, name TEXT NOT NULL, child_hash TEXT NOT NULL,
             PRIMARY KEY (parent_hash, name)
         );
         CREATE TABLE file_versions (
             version_id TEXT NOT NULL, path TEXT NOT NULL,
             node_hash TEXT NOT NULL, node_kind TEXT NOT NULL,
             PRIMARY KEY (version_id, path)
         );
         CREATE TABLE commits (
             version_id TEXT PRIMARY KEY, author TEXT NOT NULL DEFAULT '',
             committer TEXT NOT NULL DEFAULT ''
         );
         CREATE INDEX idx_version_parents_parent ON version_parents(parent_id);
         CREATE INDEX idx_file_versions_path ON file_versions(path);
         CREATE INDEX idx_refs_target ON refs(target);
         INSERT INTO schema_version (version) VALUES (1);",
    ).unwrap();
}

#[test]
fn test_migration_v0_to_v2_fresh_install() {
    // A fresh open_in_memory should go 0→1→2.
    let store = SqliteMetadataStore::open_in_memory().unwrap();

    // Verify v2 tables exist by storing a version (uses v1 tables)
    let v = make_version("v1", vec![], "fresh");
    store.put_version(&v).unwrap();

    // Verify the v2 tables exist by querying them directly.
    // We can't access the connection directly, so reopen on a file path.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");
    let _store = SqliteMetadataStore::open(&db_path).unwrap();

    let conn = Connection::open(&db_path).unwrap();
    let ver: u32 = conn
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert!(ver >= 2, "schema version should be at least 2, got {ver}");

    // v2 tables exist
    let ws_count: u32 = conn
        .query_row("SELECT COUNT(*) FROM workspaces", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ws_count, 1, "default workspace should exist");

    let ws_name: String = conn
        .query_row("SELECT name FROM workspaces WHERE id = 'default'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ws_name, "default");

    // Other v2 tables should exist (empty)
    let remote_count: u32 = conn
        .query_row("SELECT COUNT(*) FROM remotes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remote_count, 0);

    let rr_count: u32 = conn
        .query_row("SELECT COUNT(*) FROM remote_refs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rr_count, 0);

    let ss_count: u32 = conn
        .query_row("SELECT COUNT(*) FROM sync_sessions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ss_count, 0);
}

#[test]
fn test_migration_v1_to_v2_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");

    // Create a v1 database with some data.
    create_v1_database(&db_path);
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO versions (id, root, message, timestamp) VALUES ('v1', 'aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd', 'hello', 1700000000)",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO refs (name, kind, target) VALUES ('main', 'branch', 'v1')",
            [],
        ).unwrap();
    }

    // Open with SqliteMetadataStore — should auto-migrate v1→v2.
    let store = SqliteMetadataStore::open(&db_path).unwrap();

    // Existing v1 data preserved.
    let v = store.get_version(&VersionId::from("v1")).unwrap();
    assert!(v.is_some());
    assert_eq!(v.unwrap().message, "hello");

    let r = store.get_ref("main").unwrap();
    assert!(r.is_some());
    assert_eq!(r.unwrap().target.as_str(), "v1");

    // Verify schema version is now 2.
    let conn = Connection::open(&db_path).unwrap();
    let ver: u32 = conn
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert!(ver >= 2, "schema version should be at least 2, got {ver}");

    // Default workspace created.
    let ws_name: String = conn
        .query_row("SELECT name FROM workspaces WHERE id = 'default'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ws_name, "default");
}

#[test]
fn test_migration_v2_idempotent_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");

    // First open: fresh v0→v2.
    {
        let _store = SqliteMetadataStore::open(&db_path).unwrap();
    }

    // Second open: v2 already applied, should be no-op.
    {
        let store = SqliteMetadataStore::open(&db_path).unwrap();
        // Can still use v1 tables.
        let v = make_version("v1", vec![], "after reopen");
        store.put_version(&v).unwrap();
    }

    let conn = Connection::open(&db_path).unwrap();
    let ver: u32 = conn
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
        .unwrap();
    assert!(ver >= 2, "schema version should be at least 2, got {ver}");

    // Still only one default workspace (INSERT OR IGNORE).
    let ws_count: u32 = conn
        .query_row("SELECT COUNT(*) FROM workspaces", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ws_count, 1);
}

// ---------------------------------------------------------------------------
// VersionRepo tests
// ---------------------------------------------------------------------------

#[test]
fn test_version_put_and_get() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let v = make_version("abc123", vec![], "first commit");
    store.put_version(&v).unwrap();

    let got = store.get_version(&v.id).unwrap().expect("should exist");
    assert_eq!(got.id, v.id);
    assert_eq!(got.root, v.root);
    assert_eq!(got.message, "first commit");
    assert_eq!(got.timestamp, v.timestamp);
    assert!(got.parents.is_empty());
}

#[test]
fn test_version_with_parents() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();

    let v1 = make_version("v1", vec![], "init");
    let v2 = make_version("v2", vec!["v1"], "second");
    let v3 = make_version("v3", vec!["v1", "v2"], "merge");

    store.put_version(&v1).unwrap();
    store.put_version(&v2).unwrap();
    store.put_version(&v3).unwrap();

    let got = store
        .get_version(&VersionId::from("v3"))
        .unwrap()
        .unwrap();
    assert_eq!(got.parents.len(), 2);
    assert_eq!(got.parents[0].as_str(), "v1");
    assert_eq!(got.parents[1].as_str(), "v2");
}

#[test]
fn test_version_get_missing() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let got = store.get_version(&VersionId::from("nonexistent")).unwrap();
    assert!(got.is_none());
}

#[test]
fn test_version_with_metadata() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let mut v = make_version("v-meta", vec![], "with metadata");
    v.metadata.insert("author".into(), "alice".into());
    v.metadata.insert("tool".into(), "axiom".into());
    store.put_version(&v).unwrap();

    let got = store.get_version(&v.id).unwrap().unwrap();
    assert_eq!(got.metadata.get("author").unwrap(), "alice");
    assert_eq!(got.metadata.get("tool").unwrap(), "axiom");
}

#[test]
fn test_list_history_linear() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();

    let v1 = make_version("v1", vec![], "first");
    let v2 = make_version("v2", vec!["v1"], "second");
    let v3 = make_version("v3", vec!["v2"], "third");

    store.put_version(&v1).unwrap();
    store.put_version(&v2).unwrap();
    store.put_version(&v3).unwrap();

    let history = store
        .list_history(&VersionId::from("v3"), 10)
        .unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].id.as_str(), "v3");
    assert_eq!(history[1].id.as_str(), "v2");
    assert_eq!(history[2].id.as_str(), "v1");
}

#[test]
fn test_list_history_with_limit() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();

    let v1 = make_version("v1", vec![], "first");
    let v2 = make_version("v2", vec!["v1"], "second");
    let v3 = make_version("v3", vec!["v2"], "third");

    store.put_version(&v1).unwrap();
    store.put_version(&v2).unwrap();
    store.put_version(&v3).unwrap();

    let history = store
        .list_history(&VersionId::from("v3"), 2)
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id.as_str(), "v3");
    assert_eq!(history[1].id.as_str(), "v2");
}

// ---------------------------------------------------------------------------
// RefRepo tests
// ---------------------------------------------------------------------------

#[test]
fn test_ref_put_and_get_branch() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let v = make_version("v1", vec![], "init");
    store.put_version(&v).unwrap();

    let r = Ref {
        name: "main".into(),
        kind: RefKind::Branch,
        target: VersionId::from("v1"),
    };
    store.put_ref(&r).unwrap();

    let got = store.get_ref("main").unwrap().unwrap();
    assert_eq!(got.name, "main");
    assert_eq!(got.kind, RefKind::Branch);
    assert_eq!(got.target.as_str(), "v1");
}

#[test]
fn test_ref_branch_update() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let v1 = make_version("v1", vec![], "init");
    let v2 = make_version("v2", vec!["v1"], "second");
    store.put_version(&v1).unwrap();
    store.put_version(&v2).unwrap();

    store
        .put_ref(&Ref {
            name: "main".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v1"),
        })
        .unwrap();

    // Update branch to point to v2.
    store
        .put_ref(&Ref {
            name: "main".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v2"),
        })
        .unwrap();

    let got = store.get_ref("main").unwrap().unwrap();
    assert_eq!(got.target.as_str(), "v2");
}

#[test]
fn test_ref_tag_immutable() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let v1 = make_version("v1", vec![], "init");
    store.put_version(&v1).unwrap();

    store
        .put_ref(&Ref {
            name: "v1.0".into(),
            kind: RefKind::Tag,
            target: VersionId::from("v1"),
        })
        .unwrap();

    // Attempting to overwrite a tag should fail.
    let result = store.put_ref(&Ref {
        name: "v1.0".into(),
        kind: RefKind::Tag,
        target: VersionId::from("v1"),
    });
    assert!(result.is_err());
}

#[test]
fn test_ref_get_missing() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let got = store.get_ref("nonexistent").unwrap();
    assert!(got.is_none());
}

#[test]
fn test_ref_delete() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let v = make_version("v1", vec![], "init");
    store.put_version(&v).unwrap();

    store
        .put_ref(&Ref {
            name: "feature".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v1"),
        })
        .unwrap();

    store.delete_ref("feature").unwrap();
    let got = store.get_ref("feature").unwrap();
    assert!(got.is_none());
}

#[test]
fn test_list_refs_all() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let v = make_version("v1", vec![], "init");
    store.put_version(&v).unwrap();

    store
        .put_ref(&Ref {
            name: "main".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v1"),
        })
        .unwrap();
    store
        .put_ref(&Ref {
            name: "v1.0".into(),
            kind: RefKind::Tag,
            target: VersionId::from("v1"),
        })
        .unwrap();

    let all = store.list_refs(None).unwrap();
    assert_eq!(all.len(), 2);
}

#[test]
fn test_list_refs_by_kind() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let v = make_version("v1", vec![], "init");
    store.put_version(&v).unwrap();

    store
        .put_ref(&Ref {
            name: "main".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v1"),
        })
        .unwrap();
    store
        .put_ref(&Ref {
            name: "dev".into(),
            kind: RefKind::Branch,
            target: VersionId::from("v1"),
        })
        .unwrap();
    store
        .put_ref(&Ref {
            name: "v1.0".into(),
            kind: RefKind::Tag,
            target: VersionId::from("v1"),
        })
        .unwrap();

    let branches = store.list_refs(Some(RefKind::Branch)).unwrap();
    assert_eq!(branches.len(), 2);

    let tags = store.list_refs(Some(RefKind::Tag)).unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].name, "v1.0");
}

// ---------------------------------------------------------------------------
// PathIndexRepo tests
// ---------------------------------------------------------------------------

#[test]
fn test_path_put_and_get() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let node_hash = hash_bytes(b"file content");
    let vid = VersionId::from("v1");

    store
        .put_path_entry(&vid, "src/main.rs", &node_hash, false)
        .unwrap();

    let got = store.get_by_path(&vid, "src/main.rs").unwrap().unwrap();
    assert_eq!(got.path, "src/main.rs");
    assert_eq!(got.node_hash, node_hash);
    assert!(!got.is_directory);
}

#[test]
fn test_path_get_missing() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let vid = VersionId::from("v1");
    let got = store.get_by_path(&vid, "nonexistent").unwrap();
    assert!(got.is_none());
}

#[test]
fn test_path_list_directory() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let vid = VersionId::from("v1");

    let h1 = hash_bytes(b"main.rs");
    let h2 = hash_bytes(b"lib.rs");
    let h3 = hash_bytes(b"nested");
    let h4 = hash_bytes(b"deep_file");

    store.put_path_entry(&vid, "src/main.rs", &h1, false).unwrap();
    store.put_path_entry(&vid, "src/lib.rs", &h2, false).unwrap();
    store.put_path_entry(&vid, "src/nested", &h3, true).unwrap();
    // This is a deeper entry — should NOT appear in list_directory("src").
    store
        .put_path_entry(&vid, "src/nested/deep.rs", &h4, false)
        .unwrap();

    let entries = store.list_directory(&vid, "src").unwrap();
    assert_eq!(entries.len(), 3); // main.rs, lib.rs, nested
    let names: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    assert!(names.contains(&"src/main.rs"));
    assert!(names.contains(&"src/lib.rs"));
    assert!(names.contains(&"src/nested"));
    // deep.rs should NOT be here.
    assert!(!names.contains(&"src/nested/deep.rs"));
}

#[test]
fn test_path_list_root_directory() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();
    let vid = VersionId::from("v1");

    let h1 = hash_bytes(b"readme");
    let h2 = hash_bytes(b"src_dir");
    let h3 = hash_bytes(b"nested_file");

    store.put_path_entry(&vid, "README.md", &h1, false).unwrap();
    store.put_path_entry(&vid, "src", &h2, true).unwrap();
    store
        .put_path_entry(&vid, "src/main.rs", &h3, false)
        .unwrap();

    let entries = store.list_directory(&vid, "").unwrap();
    assert_eq!(entries.len(), 2); // README.md, src
}

#[test]
fn test_path_version_isolation() {
    let store = SqliteMetadataStore::open_in_memory().unwrap();

    let h1 = hash_bytes(b"content_v1");
    let h2 = hash_bytes(b"content_v2");

    let v1 = VersionId::from("v1");
    let v2 = VersionId::from("v2");

    store.put_path_entry(&v1, "file.txt", &h1, false).unwrap();
    store.put_path_entry(&v2, "file.txt", &h2, false).unwrap();

    let got_v1 = store.get_by_path(&v1, "file.txt").unwrap().unwrap();
    let got_v2 = store.get_by_path(&v2, "file.txt").unwrap().unwrap();

    assert_eq!(got_v1.node_hash, h1);
    assert_eq!(got_v2.node_hash, h2);
    assert_ne!(h1, h2);
}

// ---------------------------------------------------------------------------
// Persistence test (file-backed)
// ---------------------------------------------------------------------------

#[test]
fn test_persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("meta.db");

    // Write data.
    {
        let store = SqliteMetadataStore::open(&db_path).unwrap();
        let v = make_version("v1", vec![], "persisted");
        store.put_version(&v).unwrap();
        store
            .put_ref(&Ref {
                name: "main".into(),
                kind: RefKind::Branch,
                target: VersionId::from("v1"),
            })
            .unwrap();
        store
            .put_path_entry(
                &VersionId::from("v1"),
                "hello.txt",
                &hash_bytes(b"hello"),
                false,
            )
            .unwrap();
    }

    // Reopen and verify.
    {
        let store = SqliteMetadataStore::open(&db_path).unwrap();

        let v = store
            .get_version(&VersionId::from("v1"))
            .unwrap()
            .unwrap();
        assert_eq!(v.message, "persisted");

        let r = store.get_ref("main").unwrap().unwrap();
        assert_eq!(r.target.as_str(), "v1");

        let p = store
            .get_by_path(&VersionId::from("v1"), "hello.txt")
            .unwrap()
            .unwrap();
        assert!(!p.is_directory);
    }
}

// ---------------------------------------------------------------------------
// E06-S06: migrate_v4 orphan pre-clean
// ---------------------------------------------------------------------------

/// Manually construct a v3 schema, inject orphan rows into `remote_refs` and
/// `sync_sessions` (rows whose `remote_name` does not exist in `remotes`),
/// then open with `SqliteMetadataStore::open` which runs `migrate_v4`.
///
/// Verifies that:
/// - Migration succeeds (DB reaches schema v4+).
/// - Orphan rows are removed.
/// - Legitimate rows (linked to real remotes) are preserved.
#[test]
fn test_migrate_v4_pre_clean_removes_orphan_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("orphan_test.db");

    // ---- Build a v3 database directly ----
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = OFF;").unwrap();

        // Create schema_version table and set it to v3.
        conn.execute_batch(
            "CREATE TABLE schema_version (version INTEGER NOT NULL);
             INSERT INTO schema_version VALUES (3);",
        )
        .unwrap();

        // v1 tables (versions, refs, etc. — needed for open to succeed).
        conn.execute_batch(
            "CREATE TABLE versions (
                 id TEXT PRIMARY KEY,
                 parent_ids TEXT,
                 root_hash TEXT NOT NULL,
                 message TEXT NOT NULL,
                 timestamp INTEGER NOT NULL,
                 metadata TEXT NOT NULL
             );
             CREATE TABLE refs (
                 name TEXT PRIMARY KEY,
                 kind TEXT NOT NULL,
                 target TEXT NOT NULL
             );
             CREATE TABLE tree_nodes (
                 hash TEXT PRIMARY KEY,
                 data BLOB NOT NULL
             );
             CREATE TABLE node_entries (
                 hash TEXT PRIMARY KEY,
                 data BLOB NOT NULL
             );
             CREATE TABLE path_entries (
                 version_id TEXT NOT NULL,
                 path TEXT NOT NULL,
                 node_hash TEXT NOT NULL,
                 is_directory INTEGER NOT NULL,
                 PRIMARY KEY (version_id, path)
             );",
        )
        .unwrap();

        // v2 tables (workspaces).
        conn.execute_batch(
            "CREATE TABLE workspaces (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 default_ref TEXT
             );",
        )
        .unwrap();

        // v3 tables (remotes, remote_refs, sync_sessions) — without CASCADE.
        conn.execute_batch(
            "CREATE TABLE remotes (
                 name TEXT PRIMARY KEY,
                 url TEXT NOT NULL,
                 auth_token TEXT,
                 tenant_id TEXT,
                 workspace_id TEXT,
                 created_at INTEGER NOT NULL
             );
             CREATE TABLE remote_refs (
                 remote_name TEXT NOT NULL,
                 ref_name    TEXT NOT NULL,
                 kind        TEXT NOT NULL,
                 target      TEXT NOT NULL,
                 updated_at  INTEGER NOT NULL,
                 PRIMARY KEY (remote_name, ref_name)
             );
             CREATE TABLE sync_sessions (
                 id          TEXT PRIMARY KEY,
                 remote_name TEXT NOT NULL,
                 direction   TEXT NOT NULL,
                 status      TEXT NOT NULL,
                 started_at  INTEGER NOT NULL,
                 finished_at INTEGER,
                 objects_transferred INTEGER NOT NULL DEFAULT 0,
                 bytes_transferred   INTEGER NOT NULL DEFAULT 0,
                 error_message TEXT
             );",
        )
        .unwrap();

        // Insert a real remote.
        conn.execute_batch(
            "INSERT INTO remotes VALUES ('origin', 'https://example.com', NULL, NULL, NULL, 0);",
        )
        .unwrap();

        // Insert a legitimate remote_ref (linked to real remote).
        conn.execute_batch(
            "INSERT INTO remote_refs VALUES ('origin', 'main', 'branch', 'abc123', 0);",
        )
        .unwrap();

        // Insert ORPHAN rows — remote_name 'ghost' does not exist in `remotes`.
        conn.execute_batch(
            "INSERT INTO remote_refs VALUES ('ghost', 'main', 'branch', 'deadbeef', 0);",
        )
        .unwrap();
        conn.execute_batch(
            "INSERT INTO sync_sessions
                 VALUES ('sess-1', 'ghost', 'push', 'completed', 0, 1, 0, 0, NULL);",
        )
        .unwrap();
    }

    // ---- Open via SqliteMetadataStore — this runs migrate_v4 ----
    let store = SqliteMetadataStore::open(&db_path)
        .expect("open should succeed even with orphan rows in v3 DB");

    // ---- Verify schema reached latest version ----
    // (Indirectly: the store is usable for normal operations.)
    let conn_guard = {
        // Access the raw connection to inspect the DB state.
        // We can't get the raw connection back; instead verify via the public
        // API that the store is functional, and verify orphan removal by
        // opening a raw connection to the same file.
        drop(store);
        Connection::open(&db_path).unwrap()
    };

    // Orphan remote_ref for 'ghost' must be gone.
    let orphan_count: i64 = conn_guard
        .query_row(
            "SELECT COUNT(*) FROM remote_refs WHERE remote_name = 'ghost'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(orphan_count, 0, "orphan remote_refs rows should be removed");

    // Orphan sync_session for 'ghost' must be gone.
    let orphan_session: i64 = conn_guard
        .query_row(
            "SELECT COUNT(*) FROM sync_sessions WHERE remote_name = 'ghost'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(orphan_session, 0, "orphan sync_sessions rows should be removed");

    // Legitimate remote_ref for 'origin' must still exist.
    let legit_count: i64 = conn_guard
        .query_row(
            "SELECT COUNT(*) FROM remote_refs WHERE remote_name = 'origin'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(legit_count, 1, "legitimate remote_ref must be preserved");

    // Schema must have been upgraded (version >= 4).
    let schema_version: u32 = conn_guard
        .query_row("SELECT version FROM schema_version", [], |row| row.get(0))
        .unwrap();
    assert!(
        schema_version >= 4,
        "schema_version must be >= 4 after successful migration, got {schema_version}"
    );
}
