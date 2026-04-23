//! Tests for the sync session log (E04-S06).

use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::traits::{Remote, RemoteRepo, SyncSessionRepo};
use axiom_core::sync::session::{
    SyncDirection, SyncSessionStatus, begin_session, complete_session, fail_session,
};

fn store_with_remote(name: &str) -> SqliteMetadataStore {
    let db = SqliteMetadataStore::open_in_memory().expect("in-memory store");
    db.add_remote(&Remote {
        name: name.into(),
        url: "https://example".into(),
        auth_token: String::new(),
        tenant_id: None,
        workspace_id: None,
        created_at: 1,
    })
    .unwrap();
    db
}

#[test]
fn begin_creates_running_session() {
    let db = store_with_remote("origin");
    let s = begin_session(&db, "sess-1", "origin", SyncDirection::Push).unwrap();
    assert_eq!(s.status, SyncSessionStatus::Running);
    assert_eq!(s.direction, SyncDirection::Push);
    assert_eq!(s.objects_transferred, 0);
    assert_eq!(s.bytes_transferred, 0);
    assert!(s.finished_at.is_none());
    assert!(s.error_message.is_none());

    let got = db.get_session("sess-1").unwrap().unwrap();
    assert_eq!(got.id, "sess-1");
    assert_eq!(got.status, SyncSessionStatus::Running);
}

#[test]
fn complete_marks_completed_with_stats() {
    let db = store_with_remote("origin");
    begin_session(&db, "sess-2", "origin", SyncDirection::Pull).unwrap();
    let s = complete_session(&db, "sess-2", 42, 9000).unwrap();
    assert_eq!(s.status, SyncSessionStatus::Completed);
    assert_eq!(s.objects_transferred, 42);
    assert_eq!(s.bytes_transferred, 9000);
    assert!(s.finished_at.is_some());
    assert!(s.error_message.is_none());
}

#[test]
fn fail_marks_failed_with_error() {
    let db = store_with_remote("origin");
    begin_session(&db, "sess-3", "origin", SyncDirection::Push).unwrap();
    let s = fail_session(&db, "sess-3", "boom").unwrap();
    assert_eq!(s.status, SyncSessionStatus::Failed);
    assert_eq!(s.error_message.as_deref(), Some("boom"));
    assert!(s.finished_at.is_some());
}

#[test]
fn list_sync_sessions_filters_by_remote() {
    let db = store_with_remote("origin");
    db.add_remote(&Remote {
        name: "backup".into(),
        url: "https://backup".into(),
        auth_token: String::new(),
        tenant_id: None,
        workspace_id: None,
        created_at: 1,
    })
    .unwrap();

    begin_session(&db, "a", "origin", SyncDirection::Push).unwrap();
    begin_session(&db, "b", "origin", SyncDirection::Pull).unwrap();
    begin_session(&db, "c", "backup", SyncDirection::Push).unwrap();

    let origin = db.list_sync_sessions(Some("origin"), 10).unwrap();
    assert_eq!(origin.len(), 2);
    assert!(origin.iter().all(|s| s.remote_name == "origin"));

    let backup = db.list_sync_sessions(Some("backup"), 10).unwrap();
    assert_eq!(backup.len(), 1);
    assert_eq!(backup[0].id, "c");

    let all = db.list_sync_sessions(None, 10).unwrap();
    assert_eq!(all.len(), 3);
}

#[test]
fn list_respects_limit_and_orders_recent_first() {
    let db = store_with_remote("origin");
    // Insert 5 sessions with explicit started_at so ordering is deterministic.
    for i in 0..5u64 {
        let s = axiom_core::store::traits::SyncSession {
            id: format!("s{i}"),
            remote_name: "origin".into(),
            direction: SyncDirection::Push,
            status: SyncSessionStatus::Running,
            started_at: 1000 + i,
            finished_at: None,
            objects_transferred: 0,
            bytes_transferred: 0,
            error_message: None,
        };
        db.create_session(&s).unwrap();
    }
    let got = db.list_sync_sessions(Some("origin"), 3).unwrap();
    assert_eq!(got.len(), 3);
    // Most-recent first.
    assert_eq!(got[0].id, "s4");
    assert_eq!(got[1].id, "s3");
    assert_eq!(got[2].id, "s2");
}

#[test]
fn update_nonexistent_returns_not_found() {
    let db = store_with_remote("origin");
    let err = complete_session(&db, "ghost", 0, 0).unwrap_err();
    matches!(err, axiom_core::error::CasError::NotFound(_));
}

#[test]
fn duplicate_create_fails() {
    let db = store_with_remote("origin");
    begin_session(&db, "dup", "origin", SyncDirection::Push).unwrap();
    let err = begin_session(&db, "dup", "origin", SyncDirection::Push).unwrap_err();
    matches!(err, axiom_core::error::CasError::AlreadyExists);
}
