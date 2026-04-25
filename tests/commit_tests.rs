use std::collections::HashMap;
use std::sync::Arc;

use axiom_core::commit::{CommitRequest, CommitService, DEFAULT_BRANCH};
use axiom_core::error::CasError;
use axiom_core::model::hash::hash_bytes;
use axiom_core::model::refs::RefKind;
use axiom_core::store::{InMemoryRefRepo, InMemoryVersionRepo};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn svc() -> CommitService {
    CommitService::new(Arc::new(InMemoryVersionRepo::new()), Arc::new(InMemoryRefRepo::new()))
}

fn root(tag: &str) -> axiom_core::model::ChunkHash {
    hash_bytes(tag.as_bytes())
}

fn initial_commit(
    svc: &CommitService,
    msg: &str,
) -> axiom_core::model::VersionNode {
    svc.commit(CommitRequest {
        root: root("tree-v1"),
        parents: vec![],
        message: msg.to_string(),
        metadata: HashMap::new(),
        branch: None,
    })
    .unwrap()
}

// ---------------------------------------------------------------------------
// Commit basics
// ---------------------------------------------------------------------------

#[test]
fn test_initial_commit_creates_version() {
    let svc = svc();
    let v = initial_commit(&svc, "initial commit");

    assert!(!v.id.as_str().is_empty());
    assert_eq!(v.parents.len(), 0);
    assert_eq!(v.root, root("tree-v1"));
    assert_eq!(v.message, "initial commit");
    assert!(v.timestamp > 0);
}

#[test]
fn test_commit_advances_default_branch() {
    let svc = svc();
    let v = initial_commit(&svc, "first");

    let head = svc.resolve_head().unwrap().unwrap();
    assert_eq!(head.id, v.id);
}

#[test]
fn test_second_commit_with_parent() {
    let svc = svc();
    let v1 = initial_commit(&svc, "first");

    let v2 = svc
        .commit(CommitRequest {
            root: root("tree-v2"),
            parents: vec![v1.id.clone()],
            message: "second".into(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    assert_eq!(v2.parents, vec![v1.id.clone()]);

    // HEAD should now point to v2.
    let head = svc.resolve_head().unwrap().unwrap();
    assert_eq!(head.id, v2.id);
}

#[test]
fn test_dag_linkage_history() {
    let svc = svc();
    let v1 = initial_commit(&svc, "c1");

    let v2 = svc
        .commit(CommitRequest {
            root: root("tree-v2"),
            parents: vec![v1.id.clone()],
            message: "c2".into(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    let v3 = svc
        .commit(CommitRequest {
            root: root("tree-v3"),
            parents: vec![v2.id.clone()],
            message: "c3".into(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    let history = svc.history(&v3.id, 10).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].id, v3.id);
    assert_eq!(history[1].id, v2.id);
    assert_eq!(history[2].id, v1.id);
}

#[test]
fn test_commit_to_named_branch() {
    let svc = svc();

    let v = svc
        .commit(CommitRequest {
            root: root("feat-tree"),
            parents: vec![],
            message: "feature start".into(),
            metadata: HashMap::new(),
            branch: Some("feature/x".into()),
        })
        .unwrap();

    // Should be on feature/x, not main.
    let resolved = svc.resolve_branch("feature/x").unwrap().unwrap();
    assert_eq!(resolved.id, v.id);

    // main should not exist.
    assert!(svc.resolve_head().unwrap().is_none());
}

#[test]
fn test_commit_with_metadata() {
    let svc = svc();
    let mut meta = HashMap::new();
    meta.insert("author".into(), "alice".into());

    let v = svc
        .commit(CommitRequest {
            root: root("meta-tree"),
            parents: vec![],
            message: "with meta".into(),
            metadata: meta,
            branch: None,
        })
        .unwrap();

    assert_eq!(v.metadata.get("author").unwrap(), "alice");
}

// ---------------------------------------------------------------------------
// Branch operations
// ---------------------------------------------------------------------------

#[test]
fn test_create_branch() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    let br = svc.create_branch("dev", &v.id).unwrap();
    assert_eq!(br.name, "dev");
    assert_eq!(br.kind, RefKind::Branch);
    assert_eq!(br.target, v.id);
}

#[test]
fn test_create_branch_duplicate_fails() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_branch("dev", &v.id).unwrap();
    let err = svc.create_branch("dev", &v.id).unwrap_err();
    assert!(matches!(err, CasError::InvalidRef(_)));
}

#[test]
fn test_update_branch() {
    let svc = svc();
    let v1 = initial_commit(&svc, "c1");

    svc.create_branch("dev", &v1.id).unwrap();

    let v2 = svc
        .commit(CommitRequest {
            root: root("v2"),
            parents: vec![v1.id.clone()],
            message: "c2".into(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    let updated = svc.update_branch("dev", &v2.id).unwrap();
    assert_eq!(updated.target, v2.id);

    let resolved = svc.resolve_branch("dev").unwrap().unwrap();
    assert_eq!(resolved.id, v2.id);
}

#[test]
fn test_update_nonexistent_branch_fails() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    let err = svc.update_branch("ghost", &v.id).unwrap_err();
    assert!(matches!(err, CasError::NotFound(_)));
}

#[test]
fn test_update_tag_as_branch_fails() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_tag("v1.0", &v.id).unwrap();
    let err = svc.update_branch("v1.0", &v.id).unwrap_err();
    assert!(matches!(err, CasError::InvalidRef(_)));
}

#[test]
fn test_list_branches() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_branch("dev", &v.id).unwrap();
    svc.create_branch("staging", &v.id).unwrap();

    let branches = svc.list_branches().unwrap();
    // main + dev + staging = 3
    assert_eq!(branches.len(), 3);
    let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
    assert!(names.contains(&DEFAULT_BRANCH));
    assert!(names.contains(&"dev"));
    assert!(names.contains(&"staging"));
}

#[test]
fn test_delete_branch() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_branch("tmp", &v.id).unwrap();
    svc.delete_branch("tmp").unwrap();

    assert!(svc.resolve_branch("tmp").unwrap().is_none());
}

#[test]
fn test_delete_nonexistent_branch_fails() {
    let svc = svc();
    let err = svc.delete_branch("nope").unwrap_err();
    assert!(matches!(err, CasError::NotFound(_)));
}

#[test]
fn test_delete_tag_via_branch_fails() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_tag("v1.0", &v.id).unwrap();
    let err = svc.delete_branch("v1.0").unwrap_err();
    assert!(matches!(err, CasError::InvalidRef(_)));
}

// ---------------------------------------------------------------------------
// Tag operations
// ---------------------------------------------------------------------------

#[test]
fn test_create_tag() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    let tag = svc.create_tag("v1.0", &v.id).unwrap();
    assert_eq!(tag.name, "v1.0");
    assert_eq!(tag.kind, RefKind::Tag);
    assert_eq!(tag.target, v.id);
}

#[test]
fn test_resolve_tag() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_tag("release", &v.id).unwrap();
    let resolved = svc.resolve_tag("release").unwrap().unwrap();
    assert_eq!(resolved.id, v.id);
}

#[test]
fn test_tag_overwrite_rejected() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_tag("v1.0", &v.id).unwrap();

    // Attempting to create the same tag again should fail.
    let err = svc.create_tag("v1.0", &v.id).unwrap_err();
    assert!(matches!(err, CasError::InvalidRef(_)));
}

#[test]
fn test_list_tags() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    svc.create_tag("v1.0", &v.id).unwrap();
    svc.create_tag("v2.0", &v.id).unwrap();

    let tags = svc.list_tags().unwrap();
    assert_eq!(tags.len(), 2);
}

// ---------------------------------------------------------------------------
// Ref resolution (unified)
// ---------------------------------------------------------------------------

#[test]
fn test_resolve_ref_branch() {
    let svc = svc();
    let v = initial_commit(&svc, "init");

    let resolved = svc.resolve_ref(DEFAULT_BRANCH).unwrap().unwrap();
    assert_eq!(resolved.id, v.id);
}

#[test]
fn test_resolve_ref_tag() {
    let svc = svc();
    let v = initial_commit(&svc, "init");
    svc.create_tag("tagged", &v.id).unwrap();

    let resolved = svc.resolve_ref("tagged").unwrap().unwrap();
    assert_eq!(resolved.id, v.id);
}

#[test]
fn test_resolve_ref_missing() {
    let svc = svc();
    assert!(svc.resolve_ref("nonexistent").unwrap().is_none());
}

// ---------------------------------------------------------------------------
// HEAD / default branch
// ---------------------------------------------------------------------------

#[test]
fn test_head_resolves_default_branch() {
    let svc = svc();
    let v1 = initial_commit(&svc, "c1");

    let v2 = svc
        .commit(CommitRequest {
            root: root("v2"),
            parents: vec![v1.id.clone()],
            message: "c2".into(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    let head = svc.resolve_head().unwrap().unwrap();
    assert_eq!(head.id, v2.id);
}

#[test]
fn test_head_none_when_no_commits() {
    let svc = svc();
    assert!(svc.resolve_head().unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Branch history
// ---------------------------------------------------------------------------

#[test]
fn test_branch_history() {
    let svc = svc();
    let v1 = initial_commit(&svc, "c1");

    let v2 = svc
        .commit(CommitRequest {
            root: root("v2"),
            parents: vec![v1.id.clone()],
            message: "c2".into(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    let history = svc.branch_history(DEFAULT_BRANCH, 10).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].id, v2.id);
    assert_eq!(history[1].id, v1.id);
}

#[test]
fn test_branch_history_nonexistent_fails() {
    let svc = svc();
    let err = svc.branch_history("nope", 10).unwrap_err();
    assert!(matches!(err, CasError::NotFound(_)));
}

// ---------------------------------------------------------------------------
// SQLite backend integration
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "local")]
fn test_commit_with_sqlite_backend() {
    use axiom_core::store::sqlite::SqliteMetadataStore;

    let db = SqliteMetadataStore::open_in_memory().unwrap();
    // SqliteMetadataStore implements both VersionRepo and RefRepo.
    // We need two references, so wrap in Arc.
    use std::sync::Arc;
    let db = Arc::new(db);

    let svc = CommitService::new(db.clone(), db.clone());

    let v1 = svc
        .commit(CommitRequest {
            root: root("sqlite-tree"),
            parents: vec![],
            message: "sqlite commit".into(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    let head = svc.resolve_head().unwrap().unwrap();
    assert_eq!(head.id, v1.id);

    svc.create_tag("v1.0", &v1.id).unwrap();
    let tag = svc.resolve_tag("v1.0").unwrap().unwrap();
    assert_eq!(tag.id, v1.id);
}

// ---------------------------------------------------------------------------
// commit_partial
// ---------------------------------------------------------------------------

/// Write a file at `dir/rel_path` with `content` and return the relative path.
fn write_file(dir: &std::path::Path, rel_path: &str, content: &[u8]) {
    let abs = dir.join(rel_path);
    if let Some(parent) = abs.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(abs, content).unwrap();
}

#[test]
fn test_commit_partial_initial_single_file() {
    let tmp = tempfile::tempdir().unwrap();
    let state = axiom_core::api::state::AppState::memory();

    write_file(tmp.path(), "hello.txt", b"hello world");

    let result = axiom_core::commit::commit_partial(
        tmp.path(),
        None,
        &["hello.txt".to_string()],
        "initial".to_string(),
        None,
        &state,
    )
    .unwrap();

    assert_eq!(result.staged_files, 1);
    assert_eq!(result.carried_files, 0);
    assert_eq!(result.version.parents.len(), 0);
    assert_eq!(result.version.message, "initial");
}

/// Stage one file, commit, then stage a second file — the first file's hash
/// must remain identical in the new version (zero-copy carry-over).
#[test]
fn test_commit_partial_carry_over_hash_unchanged() {
    use axiom_core::commit::commit_partial;

    let tmp = tempfile::tempdir().unwrap();
    let state = axiom_core::api::state::AppState::memory();

    write_file(tmp.path(), "a.txt", b"file-a content");
    write_file(tmp.path(), "b.txt", b"file-b content");

    // First commit: only stage a.txt
    let r1 = commit_partial(
        tmp.path(),
        None,
        &["a.txt".to_string()],
        "add a".to_string(),
        None,
        &state,
    )
    .unwrap();
    let v1 = r1.version;

    // Walk the first version tree to get a.txt's root hash.
    use axiom_core::working_tree::collect_files_from_tree;
    let files_v1 = collect_files_from_tree(&v1.root, "", state.nodes.as_ref()).unwrap();
    let a_hash_v1 = files_v1["a.txt"].0;

    // Second commit: stage b.txt only; a.txt should be carried over.
    let r2 = commit_partial(
        tmp.path(),
        Some(&v1.id),
        &["b.txt".to_string()],
        "add b".to_string(),
        None,
        &state,
    )
    .unwrap();
    let v2 = r2.version;

    assert_eq!(r2.staged_files, 1);
    assert_eq!(r2.carried_files, 1);
    assert_eq!(v2.parents, vec![v1.id.clone()]);

    // a.txt's hash in v2 must equal a.txt's hash in v1 (zero-copy).
    let files_v2 = collect_files_from_tree(&v2.root, "", state.nodes.as_ref()).unwrap();
    let a_hash_v2 = files_v2["a.txt"].0;
    assert_eq!(a_hash_v1, a_hash_v2, "a.txt hash must be identical (zero-copy carry-over)");

    // b.txt should also appear in v2.
    assert!(files_v2.contains_key("b.txt"), "b.txt should be in v2");
}

/// Staging a path that does not exist on disk = deletion; the file must be
/// absent from the resulting tree.
#[test]
fn test_commit_partial_deleted_file_absent_from_tree() {
    use axiom_core::commit::commit_partial;

    let tmp = tempfile::tempdir().unwrap();
    let state = axiom_core::api::state::AppState::memory();

    write_file(tmp.path(), "keep.txt", b"keep");
    write_file(tmp.path(), "gone.txt", b"gone");

    // v1: both files
    let r1 = commit_partial(
        tmp.path(),
        None,
        &["keep.txt".to_string(), "gone.txt".to_string()],
        "v1".to_string(),
        None,
        &state,
    )
    .unwrap();

    // Remove gone.txt from disk to simulate deletion.
    std::fs::remove_file(tmp.path().join("gone.txt")).unwrap();

    // v2: stage gone.txt (deleted) — keep.txt is carry-over.
    let r2 = commit_partial(
        tmp.path(),
        Some(&r1.version.id),
        &["gone.txt".to_string()],
        "delete gone".to_string(),
        None,
        &state,
    )
    .unwrap();

    use axiom_core::working_tree::collect_files_from_tree;
    let files = collect_files_from_tree(&r2.version.root, "", state.nodes.as_ref()).unwrap();
    assert!(files.contains_key("keep.txt"), "keep.txt should still be present");
    assert!(!files.contains_key("gone.txt"), "gone.txt should have been deleted");
}

/// Staging an empty list must return an error.
#[test]
fn test_commit_partial_empty_staged_paths_error() {
    let tmp = tempfile::tempdir().unwrap();
    let state = axiom_core::api::state::AppState::memory();

    let err = axiom_core::commit::commit_partial(
        tmp.path(),
        None,
        &[],
        "should fail".to_string(),
        None,
        &state,
    )
    .unwrap_err();

    assert!(matches!(err, axiom_core::error::CasError::InvalidObject(_)));
}
