use std::collections::HashMap;

use axiom_core::model::{hash_bytes, Ref, RefKind, VersionId, VersionNode};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::traits::{PathIndexRepo, RefRepo, VersionRepo};

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
