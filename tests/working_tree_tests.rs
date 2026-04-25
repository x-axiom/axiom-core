//! Integration tests for `axiom_core::working_tree::compute_status`.
//!
//! These tests exercise the four core change types (Untracked, Modified,
//! Deleted, unchanged) using real filesystem temp dirs and in-memory stores.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use tempfile::TempDir;

use axiom_core::commit::{CommitRequest, CommitService};
use axiom_core::model::hash::hash_bytes;
use axiom_core::model::VersionId;
use axiom_core::namespace::{build_directory_tree, FileInput};
use axiom_core::store::{
    InMemoryNodeStore, InMemoryPathIndex, InMemoryRefRepo, InMemoryTreeStore, InMemoryVersionRepo,
};
use axiom_core::working_tree::{
    compute_file_root, compute_status, FileChange, IgnoreMatcher,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write a set of `(relative_path, content)` files into a fresh TempDir.
fn setup_workspace(files: &[(&str, &[u8])]) -> TempDir {
    let dir = TempDir::new().unwrap();
    for (rel, content) in files {
        let full = dir.path().join(rel);
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&full, content).unwrap();
    }
    dir
}

/// In-memory store bundle needed for the helpers below.
struct Stores {
    versions: Arc<InMemoryVersionRepo>,
    nodes: Arc<InMemoryNodeStore>,
    trees: Arc<InMemoryTreeStore>,
    path_index: Arc<InMemoryPathIndex>,
}

impl Stores {
    fn new() -> Self {
        Self {
            versions: Arc::new(InMemoryVersionRepo::new()),
            nodes: Arc::new(InMemoryNodeStore::new()),
            trees: Arc::new(InMemoryTreeStore::new()),
            path_index: Arc::new(InMemoryPathIndex::new()),
        }
    }
}

/// Build a HEAD version from a list of `(relative_path, content)` pairs.
///
/// Uses `hash_bytes(content)` as the `FileInput.root` — valid because all
/// test files are small (< 16 KB → single FastCDC chunk → Merkle root =
/// `hash_bytes(content)`).
fn build_head(stores: &Stores, files: &[(&str, &[u8])]) -> VersionId {
    let vid = VersionId::from("head-v1");

    let file_inputs: Vec<FileInput> = files
        .iter()
        .map(|(path, content)| FileInput {
            path: path.to_string(),
            root: hash_bytes(content),
            size: content.len() as u64,
        })
        .collect();

    let root_entry =
        build_directory_tree(&file_inputs, &vid, stores.nodes.as_ref(), stores.path_index.as_ref())
            .unwrap();

    let svc = CommitService::new(stores.versions.clone(), Arc::new(InMemoryRefRepo::new()));
    let version = svc
        .commit(CommitRequest {
            root: root_entry.hash,
            parents: vec![],
            message: "head commit".to_string(),
            metadata: HashMap::new(),
            branch: None,
        })
        .unwrap();

    version.id
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// All local files are Untracked when there is no HEAD version.
#[test]
fn test_all_untracked_when_no_head() {
    let dir = setup_workspace(&[("a.txt", b"hello"), ("sub/b.txt", b"world")]);
    let stores = Stores::new();
    let ignore = IgnoreMatcher::none();

    let status = compute_status(
        dir.path(),
        None,
        stores.versions.as_ref(),
        stores.trees.as_ref(),
        stores.nodes.as_ref(),
        &ignore,
    )
    .unwrap();

    assert!(status.is_dirty(), "expected dirty with 2 untracked files");
    assert_eq!(status.head_version, None);
    assert_eq!(status.entries.len(), 2);
    assert!(status
        .entries
        .iter()
        .all(|e| e.change == FileChange::Untracked));
}

/// Files whose content matches HEAD exactly are not emitted.
#[test]
fn test_unchanged_not_emitted() {
    let content = b"unchanged content";
    let dir = setup_workspace(&[("readme.md", content)]);
    let stores = Stores::new();
    let vid = build_head(&stores, &[("readme.md", content)]);
    let ignore = IgnoreMatcher::none();

    let status = compute_status(
        dir.path(),
        Some(&vid),
        stores.versions.as_ref(),
        stores.trees.as_ref(),
        stores.nodes.as_ref(),
        &ignore,
    )
    .unwrap();

    assert!(!status.is_dirty(), "expected clean working tree");
    assert!(status.entries.is_empty());
    assert_eq!(status.head_version, Some(vid));
}

/// A file whose local content differs from HEAD is reported as Modified.
#[test]
fn test_modified_file() {
    let head_content = b"original content";
    let local_content = b"modified content - changed";
    let dir = setup_workspace(&[("doc.txt", local_content)]);
    let stores = Stores::new();
    let vid = build_head(&stores, &[("doc.txt", head_content)]);
    let ignore = IgnoreMatcher::none();

    let status = compute_status(
        dir.path(),
        Some(&vid),
        stores.versions.as_ref(),
        stores.trees.as_ref(),
        stores.nodes.as_ref(),
        &ignore,
    )
    .unwrap();

    assert!(status.is_dirty());
    assert_eq!(status.entries.len(), 1);
    let entry = &status.entries[0];
    assert_eq!(entry.path, "doc.txt");
    assert_eq!(entry.change, FileChange::Modified);
    assert_eq!(entry.local_size, Some(local_content.len() as u64));
    assert_eq!(entry.head_size, Some(head_content.len() as u64));
}

/// A file present in HEAD but missing from disk is reported as Deleted.
#[test]
fn test_deleted_file() {
    let stores = Stores::new();
    // Build HEAD with two files, but only write one of them to disk.
    let vid = build_head(
        &stores,
        &[("keep.txt", b"still here"), ("gone.txt", b"deleted")],
    );
    // Only create keep.txt locally; gone.txt is absent.
    let dir = setup_workspace(&[("keep.txt", b"still here")]);
    let ignore = IgnoreMatcher::none();

    let status = compute_status(
        dir.path(),
        Some(&vid),
        stores.versions.as_ref(),
        stores.trees.as_ref(),
        stores.nodes.as_ref(),
        &ignore,
    )
    .unwrap();

    assert!(status.is_dirty());
    let deleted: Vec<_> = status
        .entries
        .iter()
        .filter(|e| e.change == FileChange::Deleted)
        .collect();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].path, "gone.txt");
    assert!(deleted[0].local_size.is_none());
    assert_eq!(deleted[0].head_size, Some(b"deleted".len() as u64));
}

/// Mixed scenario: unchanged + modified + deleted + untracked all together.
#[test]
fn test_mixed_changes() {
    let stores = Stores::new();

    let vid = build_head(
        &stores,
        &[
            ("unchanged.txt", b"same"),
            ("modified.txt", b"old value"),
            ("deleted.txt", b"will be gone"),
        ],
    );

    let dir = setup_workspace(&[
        ("unchanged.txt", b"same"),
        ("modified.txt", b"new value"),
        // deleted.txt intentionally absent
        ("untracked.txt", b"brand new file"),
    ]);
    let ignore = IgnoreMatcher::none();

    let status = compute_status(
        dir.path(),
        Some(&vid),
        stores.versions.as_ref(),
        stores.trees.as_ref(),
        stores.nodes.as_ref(),
        &ignore,
    )
    .unwrap();

    assert!(status.is_dirty());
    // 3 changes: modified, deleted, untracked (unchanged is not emitted)
    assert_eq!(status.entries.len(), 3, "{:?}", status.entries);

    let by_path: HashMap<_, _> =
        status.entries.iter().map(|e| (e.path.as_str(), e)).collect();

    assert_eq!(by_path["modified.txt"].change, FileChange::Modified);
    assert_eq!(by_path["deleted.txt"].change, FileChange::Deleted);
    assert_eq!(by_path["untracked.txt"].change, FileChange::Untracked);
    assert!(!by_path.contains_key("unchanged.txt"));
}

/// Verify that `compute_file_root` is deterministic and matches
/// `hash_bytes(content)` for small (single-chunk) files.
#[test]
fn test_compute_file_root_single_chunk() {
    let data = b"small file content";
    let root = compute_file_root(data);
    assert_eq!(root, hash_bytes(data));
}

/// Empty file root equals hash of empty bytes.
#[test]
fn test_compute_file_root_empty() {
    let root = compute_file_root(b"");
    assert_eq!(root, hash_bytes(b""));
}
