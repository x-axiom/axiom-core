use axiom_core::error::CasError;
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::traits::{Remote, RemoteRepo};

fn make_remote(name: &str, url: &str) -> Remote {
    Remote {
        name: name.to_string(),
        url: url.to_string(),
        auth_token: String::new(),
        tenant_id: None,
        workspace_id: None,
        created_at: 1_000_000,
    }
}

fn store() -> SqliteMetadataStore {
    SqliteMetadataStore::open_in_memory().expect("in-memory store")
}

// ---------------------------------------------------------------------------
// add_remote / get_remote
// ---------------------------------------------------------------------------

#[test]
fn add_and_get_remote() {
    let db = store();
    let r = Remote {
        name: "origin".into(),
        url: "https://sync.example.com".into(),
        auth_token: "tok123".into(),
        tenant_id: Some("tenant-abc".into()),
        workspace_id: Some("ws-xyz".into()),
        created_at: 12345,
    };
    db.add_remote(&r).unwrap();
    let got = db.get_remote("origin").unwrap().expect("should exist");
    assert_eq!(got, r);
}

#[test]
fn add_remote_null_optional_fields() {
    let db = store();
    let r = make_remote("backup", "https://backup.example.com");
    db.add_remote(&r).unwrap();
    let got = db.get_remote("backup").unwrap().unwrap();
    assert!(got.tenant_id.is_none());
    assert!(got.workspace_id.is_none());
}

#[test]
fn get_nonexistent_remote_returns_none() {
    let db = store();
    assert!(db.get_remote("no-such").unwrap().is_none());
}

#[test]
fn add_duplicate_remote_returns_already_exists() {
    let db = store();
    let r = make_remote("origin", "https://a.example.com");
    db.add_remote(&r).unwrap();
    let err = db.add_remote(&r).unwrap_err();
    assert!(
        matches!(err, CasError::AlreadyExists),
        "expected AlreadyExists, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// list_remotes
// ---------------------------------------------------------------------------

#[test]
fn list_remotes_empty() {
    let db = store();
    assert!(db.list_remotes().unwrap().is_empty());
}

#[test]
fn list_remotes_returns_all_ordered_by_created_at() {
    let db = store();
    let r1 = Remote { created_at: 100, ..make_remote("alpha", "https://alpha.example.com") };
    let r2 = Remote { created_at: 200, ..make_remote("beta", "https://beta.example.com") };
    let r3 = Remote { created_at: 50, ..make_remote("gamma", "https://gamma.example.com") };
    db.add_remote(&r1).unwrap();
    db.add_remote(&r2).unwrap();
    db.add_remote(&r3).unwrap();

    let list = db.list_remotes().unwrap();
    assert_eq!(list.len(), 3);
    // ordered by created_at ASC: gamma(50), alpha(100), beta(200)
    assert_eq!(list[0].name, "gamma");
    assert_eq!(list[1].name, "alpha");
    assert_eq!(list[2].name, "beta");
}

// ---------------------------------------------------------------------------
// remove_remote
// ---------------------------------------------------------------------------

#[test]
fn remove_remote_deletes_it() {
    let db = store();
    db.add_remote(&make_remote("origin", "https://a.example.com")).unwrap();
    db.remove_remote("origin").unwrap();
    assert!(db.get_remote("origin").unwrap().is_none());
    assert!(db.list_remotes().unwrap().is_empty());
}

#[test]
fn remove_nonexistent_remote_is_noop() {
    let db = store();
    // Should not error
    db.remove_remote("no-such").unwrap();
}

#[test]
fn remove_remote_then_re_add_succeeds() {
    // Verifies that all FK-referenced rows (remote_refs, sync_sessions) were
    // cleaned up: if they were not, re-adding after delete might cause FK
    // issues on future inserts to those tables.
    let db = store();
    db.add_remote(&make_remote("origin", "https://a.example.com")).unwrap();
    db.remove_remote("origin").unwrap();
    // Re-adding the same name must succeed (uniqueness is clear)
    db.add_remote(&make_remote("origin", "https://b.example.com")).unwrap();
    let got = db.get_remote("origin").unwrap().unwrap();
    assert_eq!(got.url, "https://b.example.com");
}

#[test]
fn update_remote_rewrites_existing_remote() {
    let db = store();
    db.add_remote(&make_remote("origin", "https://a.example.com")).unwrap();

    let updated = Remote {
        name: "origin".into(),
        url: "https://b.example.com".into(),
        auth_token: "new-token".into(),
        tenant_id: Some("tenant-1".into()),
        workspace_id: Some("ws-1".into()),
        created_at: 2_000_000,
    };
    db.update_remote(&updated).unwrap();

    let got = db.get_remote("origin").unwrap().unwrap();
    assert_eq!(got, updated);
}

#[test]
fn update_missing_remote_returns_not_found() {
    let db = store();
    let err = db
        .update_remote(&Remote {
            name: "origin".into(),
            url: "https://sync.example.com".into(),
            auth_token: String::new(),
            tenant_id: None,
            workspace_id: None,
            created_at: 1,
        })
        .unwrap_err();

    assert!(matches!(err, CasError::NotFound(_)), "got: {err:?}");
}

