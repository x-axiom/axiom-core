#![cfg(feature = "local")]

use axiom_core::model::hash::hash_bytes;
use axiom_core::model::node::NodeKind;
use axiom_core::model::VersionId;
use axiom_core::namespace::{build_directory_tree, resolve_path, FileInput};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::InMemoryNodeStore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn file(path: &str) -> FileInput {
    FileInput {
        path: path.to_string(),
        root: hash_bytes(path.as_bytes()),
        size: 1024,
    }
}

fn vid() -> VersionId {
    VersionId::from("v1")
}

// ---------------------------------------------------------------------------
// Multi-level tree construction
// ---------------------------------------------------------------------------

#[test]
fn test_build_multi_level_tree() {
    let node_store = InMemoryNodeStore::new();
    let path_index = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let files = vec![
        file("README.md"),
        file("src/main.rs"),
        file("src/lib.rs"),
        file("src/util/helper.rs"),
        file("docs/guide.md"),
    ];

    let root = build_directory_tree(&files, &v, &node_store, &path_index).unwrap();
    assert!(root.is_directory());

    // Root should have 3 children: README.md, src, docs.
    match &root.kind {
        NodeKind::Directory { children } => {
            assert_eq!(children.len(), 3);
            assert!(children.contains_key("README.md"));
            assert!(children.contains_key("src"));
            assert!(children.contains_key("docs"));
        }
        _ => panic!("expected directory root"),
    }
}

// ---------------------------------------------------------------------------
// Deterministic hashing: same files → same root hash
// ---------------------------------------------------------------------------

#[test]
fn test_deterministic_root_hash() {
    let files = vec![
        file("a.txt"),
        file("b/c.txt"),
        file("b/d.txt"),
    ];
    let v = vid();

    let ns1 = InMemoryNodeStore::new();
    let pi1 = SqliteMetadataStore::open_in_memory().unwrap();
    let root1 = build_directory_tree(&files, &v, &ns1, &pi1).unwrap();

    let ns2 = InMemoryNodeStore::new();
    let pi2 = SqliteMetadataStore::open_in_memory().unwrap();
    let root2 = build_directory_tree(&files, &v, &ns2, &pi2).unwrap();

    assert_eq!(root1.hash, root2.hash);
}

// ---------------------------------------------------------------------------
// Unchanged subtree reuse: same hash when files don't change
// ---------------------------------------------------------------------------

#[test]
fn test_unchanged_subtree_reuse() {
    let v1 = VersionId::from("v1");
    let v2 = VersionId::from("v2");

    let files_v1 = vec![
        file("src/main.rs"),
        file("src/lib.rs"),
        file("docs/guide.md"),
    ];

    // v2: only docs/guide.md changes.
    let files_v2 = vec![
        file("src/main.rs"),
        file("src/lib.rs"),
        FileInput {
            path: "docs/guide.md".into(),
            root: hash_bytes(b"new-content"),
            size: 2048,
        },
    ];

    let ns1 = InMemoryNodeStore::new();
    let pi1 = SqliteMetadataStore::open_in_memory().unwrap();
    let root1 = build_directory_tree(&files_v1, &v1, &ns1, &pi1).unwrap();

    let ns2 = InMemoryNodeStore::new();
    let pi2 = SqliteMetadataStore::open_in_memory().unwrap();
    let root2 = build_directory_tree(&files_v2, &v2, &ns2, &pi2).unwrap();

    // Root hashes should differ (docs changed).
    assert_ne!(root1.hash, root2.hash);

    // But the "src" subtree hash should be identical.
    let src1 = resolve_path(&root1.hash, "src", &ns1).unwrap().unwrap();
    let src2 = resolve_path(&root2.hash, "src", &ns2).unwrap().unwrap();
    assert_eq!(src1.hash, src2.hash);
}

// ---------------------------------------------------------------------------
// Path resolution: nested file
// ---------------------------------------------------------------------------

#[test]
fn test_resolve_nested_file() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let file_input = file("src/util/helper.rs");
    let files = vec![file_input.clone(), file("README.md")];
    let root = build_directory_tree(&files, &v, &ns, &pi).unwrap();

    let resolved = resolve_path(&root.hash, "src/util/helper.rs", &ns)
        .unwrap()
        .unwrap();
    assert!(resolved.is_file());
    match &resolved.kind {
        NodeKind::File { root, size } => {
            assert_eq!(*root, file_input.root);
            assert_eq!(*size, 1024);
        }
        _ => panic!("expected file"),
    }
}

// ---------------------------------------------------------------------------
// Path resolution: nested directory
// ---------------------------------------------------------------------------

#[test]
fn test_resolve_nested_directory() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let files = vec![
        file("src/main.rs"),
        file("src/lib.rs"),
    ];
    let root = build_directory_tree(&files, &v, &ns, &pi).unwrap();

    let src = resolve_path(&root.hash, "src", &ns).unwrap().unwrap();
    assert!(src.is_directory());
    match &src.kind {
        NodeKind::Directory { children } => {
            assert_eq!(children.len(), 2);
            assert!(children.contains_key("main.rs"));
            assert!(children.contains_key("lib.rs"));
        }
        _ => panic!("expected directory"),
    }
}

// ---------------------------------------------------------------------------
// Path resolution: root
// ---------------------------------------------------------------------------

#[test]
fn test_resolve_root_path() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let files = vec![file("a.txt")];
    let root = build_directory_tree(&files, &v, &ns, &pi).unwrap();

    let resolved = resolve_path(&root.hash, "", &ns).unwrap().unwrap();
    assert_eq!(resolved.hash, root.hash);
}

// ---------------------------------------------------------------------------
// Path resolution: missing path
// ---------------------------------------------------------------------------

#[test]
fn test_resolve_missing_path() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let files = vec![file("a.txt")];
    let root = build_directory_tree(&files, &v, &ns, &pi).unwrap();

    let resolved = resolve_path(&root.hash, "nonexistent", &ns).unwrap();
    assert!(resolved.is_none());
}

// ---------------------------------------------------------------------------
// Path index: directory listing via PathIndexRepo
// ---------------------------------------------------------------------------

#[test]
fn test_path_index_directory_listing() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let files = vec![
        file("src/main.rs"),
        file("src/lib.rs"),
        file("src/util/helper.rs"),
        file("README.md"),
    ];
    build_directory_tree(&files, &v, &ns, &pi).unwrap();

    // List root children via path index.
    use axiom_core::store::traits::PathIndexRepo;
    let root_entries = pi.list_directory(&v, "").unwrap();
    assert_eq!(root_entries.len(), 2); // README.md, src

    // List src/ children.
    let src_entries = pi.list_directory(&v, "src").unwrap();
    assert_eq!(src_entries.len(), 3); // main.rs, lib.rs, util

    // Lookup specific path.
    let entry = pi.get_by_path(&v, "src/main.rs").unwrap().unwrap();
    assert!(!entry.is_directory);

    let entry = pi.get_by_path(&v, "src/util").unwrap().unwrap();
    assert!(entry.is_directory);
}

// ---------------------------------------------------------------------------
// Directory children ordering is stable
// ---------------------------------------------------------------------------

#[test]
fn test_directory_children_stable_ordering() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    // Insert in reverse alphabetical order.
    let files = vec![
        file("z.txt"),
        file("m.txt"),
        file("a.txt"),
    ];
    let root = build_directory_tree(&files, &v, &ns, &pi).unwrap();

    match &root.kind {
        NodeKind::Directory { children } => {
            let keys: Vec<&String> = children.keys().collect();
            // BTreeMap ensures sorted order.
            assert_eq!(keys, vec!["a.txt", "m.txt", "z.txt"]);
        }
        _ => panic!("expected directory"),
    }
}

// ---------------------------------------------------------------------------
// Empty file list → error
// ---------------------------------------------------------------------------

#[test]
fn test_empty_file_list_error() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let result = build_directory_tree(&[], &v, &ns, &pi);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Single file at root
// ---------------------------------------------------------------------------

#[test]
fn test_single_file_at_root() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let files = vec![file("only.txt")];
    let root = build_directory_tree(&files, &v, &ns, &pi).unwrap();

    assert!(root.is_directory());
    let resolved = resolve_path(&root.hash, "only.txt", &ns).unwrap().unwrap();
    assert!(resolved.is_file());
}

// ---------------------------------------------------------------------------
// Deeply nested path
// ---------------------------------------------------------------------------

#[test]
fn test_deeply_nested_path() {
    let ns = InMemoryNodeStore::new();
    let pi = SqliteMetadataStore::open_in_memory().unwrap();
    let v = vid();

    let files = vec![file("a/b/c/d/e/f.txt")];
    let root = build_directory_tree(&files, &v, &ns, &pi).unwrap();

    let resolved = resolve_path(&root.hash, "a/b/c/d/e/f.txt", &ns)
        .unwrap()
        .unwrap();
    assert!(resolved.is_file());

    let dir = resolve_path(&root.hash, "a/b/c", &ns).unwrap().unwrap();
    assert!(dir.is_directory());
}
