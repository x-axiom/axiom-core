use axiom_core::model::{Ref, RefKind, VersionId, VersionNode};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::traits::{RefRepo, Remote, RemoteRepo, RemoteTrackingRepo, VersionRepo};
use axiom_core::sync::remote_refs::{AheadBehind, RemoteRef, compare_refs};
use std::collections::HashMap;

fn store() -> SqliteMetadataStore {
    SqliteMetadataStore::open_in_memory().expect("in-memory store")
}

fn make_remote_ref(remote: &str, r#ref: &str, target: &str) -> RemoteRef {
    RemoteRef {
        remote_name: remote.to_string(),
        ref_name: r#ref.to_string(),
        kind: RefKind::Branch,
        target: VersionId(target.to_string()),
        updated_at: 1_000_000,
    }
}

fn put_version(db: &SqliteMetadataStore, id: &str, parents: &[&str]) {
    let v = VersionNode {
        id: VersionId(id.to_string()),
        parents: parents.iter().map(|p| VersionId(p.to_string())).collect(),
        root: blake3::hash(id.as_bytes()),
        message: id.to_string(),
        timestamp: 0,
        metadata: HashMap::new(),
    };
    db.put_version(&v).unwrap();
}

fn put_local_ref(db: &SqliteMetadataStore, name: &str, target: &str) {
    db.put_ref(&Ref {
        name: name.to_string(),
        kind: RefKind::Branch,
        target: VersionId(target.to_string()),
    })
    .unwrap();
}

fn add_remote(db: &SqliteMetadataStore, name: &str) {
    db.add_remote(&Remote {
        name: name.to_string(),
        url: "https://example.com".into(),
        auth_token: String::new(),
        tenant_id: None,
        workspace_id: None,
        created_at: 0,
    })
    .unwrap();
}

// ---------------------------------------------------------------------------
// update_remote_ref / get_remote_ref
// ---------------------------------------------------------------------------

#[test]
fn update_and_get_remote_ref() {
    let db = store();
    add_remote(&db, "origin");
    let r = RemoteRef {
        remote_name: "origin".into(),
        ref_name: "main".into(),
        kind: RefKind::Branch,
        target: VersionId("abc123".into()),
        updated_at: 42,
    };
    db.update_remote_ref(&r).unwrap();
    let got = db.get_remote_ref("origin", "main").unwrap().unwrap();
    assert_eq!(got, r);
}

#[test]
fn get_nonexistent_remote_ref_returns_none() {
    let db = store();
    assert!(db.get_remote_ref("origin", "main").unwrap().is_none());
}

#[test]
fn update_remote_ref_is_idempotent_upsert() {
    let db = store();
    add_remote(&db, "origin");
    let r1 = make_remote_ref("origin", "main", "old-target");
    db.update_remote_ref(&r1).unwrap();
    let r2 = RemoteRef {
        target: VersionId("new-target".into()),
        updated_at: 9_999,
        ..r1
    };
    db.update_remote_ref(&r2).unwrap();
    let got = db.get_remote_ref("origin", "main").unwrap().unwrap();
    assert_eq!(got.target.0, "new-target");
    assert_eq!(got.updated_at, 9_999);
}

#[test]
fn tag_kind_round_trips() {
    let db = store();
    add_remote(&db, "origin");
    let r = RemoteRef {
        remote_name: "origin".into(),
        ref_name: "v1.0".into(),
        kind: RefKind::Tag,
        target: VersionId("deadbeef".into()),
        updated_at: 1,
    };
    db.update_remote_ref(&r).unwrap();
    let got = db.get_remote_ref("origin", "v1.0").unwrap().unwrap();
    assert_eq!(got.kind, RefKind::Tag);
}

// ---------------------------------------------------------------------------
// list_remote_refs
// ---------------------------------------------------------------------------

#[test]
fn list_remote_refs_empty() {
    let db = store();
    assert!(db.list_remote_refs("origin").unwrap().is_empty());
}

#[test]
fn list_remote_refs_filters_by_remote() {
    let db = store();
    add_remote(&db, "origin");
    add_remote(&db, "backup");
    db.update_remote_ref(&make_remote_ref("origin", "main", "h1")).unwrap();
    db.update_remote_ref(&make_remote_ref("origin", "dev", "h2")).unwrap();
    db.update_remote_ref(&make_remote_ref("backup", "main", "h3")).unwrap();

    let origin_refs = db.list_remote_refs("origin").unwrap();
    assert_eq!(origin_refs.len(), 2);
    // ordered by ref_name ASC
    assert_eq!(origin_refs[0].ref_name, "dev");
    assert_eq!(origin_refs[1].ref_name, "main");

    let backup_refs = db.list_remote_refs("backup").unwrap();
    assert_eq!(backup_refs.len(), 1);
}

// ---------------------------------------------------------------------------
// delete_remote_ref
// ---------------------------------------------------------------------------

#[test]
fn delete_remote_ref_removes_it() {
    let db = store();
    add_remote(&db, "origin");
    db.update_remote_ref(&make_remote_ref("origin", "main", "h1")).unwrap();
    db.delete_remote_ref("origin", "main").unwrap();
    assert!(db.get_remote_ref("origin", "main").unwrap().is_none());
}

#[test]
fn delete_nonexistent_remote_ref_is_noop() {
    let db = store();
    db.delete_remote_ref("origin", "main").unwrap();
}

// ---------------------------------------------------------------------------
// compare_refs
// ---------------------------------------------------------------------------

#[test]
fn compare_refs_equal_heads() {
    let db = store();
    add_remote(&db, "origin");
    put_version(&db, "v1", &[]);
    put_local_ref(&db, "main", "v1");
    db.update_remote_ref(&make_remote_ref("origin", "main", "v1")).unwrap();

    let ab = compare_refs("origin", "main", &db, &db, &db).unwrap();
    assert_eq!(ab, AheadBehind { ahead: 0, behind: 0 });
}

#[test]
fn compare_refs_ahead_by_one() {
    // local: v1 → v2 → v3 (HEAD)
    // remote: v1 → v2 (HEAD)
    let db = store();
    add_remote(&db, "origin");
    put_version(&db, "v1", &[]);
    put_version(&db, "v2", &["v1"]);
    put_version(&db, "v3", &["v2"]);
    put_local_ref(&db, "main", "v3");
    db.update_remote_ref(&make_remote_ref("origin", "main", "v2")).unwrap();

    let ab = compare_refs("origin", "main", &db, &db, &db).unwrap();
    assert_eq!(ab, AheadBehind { ahead: 1, behind: 0 });
}

#[test]
fn compare_refs_behind_by_one() {
    // local: v1 (HEAD)
    // remote: v1 → v2 (HEAD)
    let db = store();
    add_remote(&db, "origin");
    put_version(&db, "v1", &[]);
    put_version(&db, "v2", &["v1"]);
    put_local_ref(&db, "main", "v1");
    db.update_remote_ref(&make_remote_ref("origin", "main", "v2")).unwrap();

    let ab = compare_refs("origin", "main", &db, &db, &db).unwrap();
    assert_eq!(ab, AheadBehind { ahead: 0, behind: 1 });
}

#[test]
fn compare_refs_diverged() {
    // common ancestor: v1
    // local:  v1 → v2 → v3
    // remote: v1 → v4
    let db = store();
    add_remote(&db, "origin");
    put_version(&db, "v1", &[]);
    put_version(&db, "v2", &["v1"]);
    put_version(&db, "v3", &["v2"]);
    put_version(&db, "v4", &["v1"]);
    put_local_ref(&db, "main", "v3");
    db.update_remote_ref(&make_remote_ref("origin", "main", "v4")).unwrap();

    let ab = compare_refs("origin", "main", &db, &db, &db).unwrap();
    // local has {v3, v2, v1}, remote has {v4, v1}
    // ahead = {v3, v2}, behind = {v4}
    assert_eq!(ab, AheadBehind { ahead: 2, behind: 1 });
}

#[test]
fn compare_refs_missing_local_ref_returns_not_found() {
    let db = store();
    add_remote(&db, "origin");
    db.update_remote_ref(&make_remote_ref("origin", "main", "v1")).unwrap();
    let err = compare_refs("origin", "main", &db, &db, &db).unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}

#[test]
fn compare_refs_missing_remote_ref_returns_not_found() {
    let db = store();
    put_version(&db, "v1", &[]);
    put_local_ref(&db, "main", "v1");
    let err = compare_refs("origin", "main", &db, &db, &db).unwrap_err();
    assert!(err.to_string().contains("not found"), "got: {err}");
}
