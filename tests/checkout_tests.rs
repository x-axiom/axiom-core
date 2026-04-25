//! Integration tests for `axiom_core::checkout`.

use axiom_core::api::state::AppState;
use axiom_core::checkout::{checkout_to_path, CheckoutMode};
use axiom_core::commit::commit_partial;
use axiom_core::working_tree::compute_status;
use axiom_core::working_tree::IgnoreMatcher;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write `content` to `dir/rel_path`, creating parent directories as needed.
fn write(dir: &std::path::Path, rel_path: &str, content: &[u8]) {
    let abs = dir.join(rel_path);
    if let Some(parent) = abs.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(abs, content).unwrap();
}

/// Create an initial commit from all files in `dir` and return the version id.
fn initial_commit(
    dir: &std::path::Path,
    paths: &[&str],
    message: &str,
    state: &AppState,
) -> axiom_core::model::hash::VersionId {
    let owned: Vec<String> = paths.iter().map(|p| p.to_string()).collect();
    let result = commit_partial(dir, None, &owned, message.to_string(), None, state).unwrap();
    result.version.id
}

// ---------------------------------------------------------------------------
// Test: empty directory → checkout → all files written correctly
// ---------------------------------------------------------------------------

#[test]
fn test_checkout_empty_dir_writes_all_files() {
    let src = tempfile::tempdir().unwrap();
    let state = AppState::memory();

    write(src.path(), "a.txt", b"hello");
    write(src.path(), "sub/b.txt", b"world");

    let vid = initial_commit(src.path(), &["a.txt", "sub/b.txt"], "v1", &state);

    // Checkout into a fresh empty directory.
    let target = tempfile::tempdir().unwrap();
    let report =
        checkout_to_path(&vid, target.path(), CheckoutMode::Force, &state).unwrap();

    assert_eq!(report.written, 2);
    assert_eq!(report.deleted, 0);
    assert!(report.skipped_dirty.is_empty());

    // Verify file contents.
    let a = std::fs::read(target.path().join("a.txt")).unwrap();
    assert_eq!(a, b"hello");
    let b = std::fs::read(target.path().join("sub/b.txt")).unwrap();
    assert_eq!(b, b"world");
}

// ---------------------------------------------------------------------------
// Test: after Force checkout, compute_status returns empty
// ---------------------------------------------------------------------------

#[test]
fn test_checkout_force_status_clean_after() {
    let src = tempfile::tempdir().unwrap();
    let state = AppState::memory();

    write(src.path(), "x.txt", b"original");
    let vid = initial_commit(src.path(), &["x.txt"], "v1", &state);

    // Mutate x.txt and add y.txt in a new checkout target.
    let target = tempfile::tempdir().unwrap();
    write(target.path(), "x.txt", b"MODIFIED");
    write(target.path(), "y.txt", b"extra");

    // Force checkout should overwrite x.txt and delete y.txt.
    let report =
        checkout_to_path(&vid, target.path(), CheckoutMode::Force, &state).unwrap();

    assert_eq!(report.written, 1); // x.txt overwritten
    assert_eq!(report.deleted, 1); // y.txt deleted
    assert!(report.skipped_dirty.is_empty());

    // Status must be empty (no changes vs HEAD).
    let ignore = IgnoreMatcher::none();
    let status = compute_status(
        target.path(),
        Some(&vid),
        state.versions.as_ref(),
        state.trees.as_ref(),
        state.nodes.as_ref(),
        &ignore,
        None,
    )
    .unwrap();
    assert!(!status.is_dirty(), "expected clean status after Force checkout");
}

// ---------------------------------------------------------------------------
// Test: Safe mode preserves dirty local file
// ---------------------------------------------------------------------------

#[test]
fn test_checkout_safe_skips_dirty_file() {
    let src = tempfile::tempdir().unwrap();
    let state = AppState::memory();

    write(src.path(), "clean.txt", b"unchanged");
    write(src.path(), "dirty.txt", b"original");
    let vid = initial_commit(
        src.path(),
        &["clean.txt", "dirty.txt"],
        "v1",
        &state,
    );

    // Simulate the user having modified dirty.txt before checkout.
    let target = tempfile::tempdir().unwrap();
    write(target.path(), "dirty.txt", b"LOCAL MODIFICATION");
    // clean.txt is absent → will be written by checkout.

    let report =
        checkout_to_path(&vid, target.path(), CheckoutMode::Safe, &state).unwrap();

    // clean.txt was missing → written.
    assert_eq!(report.written, 1);
    // dirty.txt was different → skipped.
    assert_eq!(report.skipped_dirty, vec!["dirty.txt".to_string()]);
    // No deletions in Safe mode.
    assert_eq!(report.deleted, 0);

    // Verify dirty.txt was NOT overwritten.
    let contents = std::fs::read(target.path().join("dirty.txt")).unwrap();
    assert_eq!(contents, b"LOCAL MODIFICATION");
}

// ---------------------------------------------------------------------------
// Test: Force mode deletes local files not in the target version
// ---------------------------------------------------------------------------

#[test]
fn test_checkout_force_deletes_extra_local_files() {
    let src = tempfile::tempdir().unwrap();
    let state = AppState::memory();

    write(src.path(), "keep.txt", b"keep");
    let vid = initial_commit(src.path(), &["keep.txt"], "v1", &state);

    let target = tempfile::tempdir().unwrap();
    write(target.path(), "keep.txt", b"keep"); // same content
    write(target.path(), "extra.txt", b"extra"); // not in version

    let report =
        checkout_to_path(&vid, target.path(), CheckoutMode::Force, &state).unwrap();

    // keep.txt already matches → not written.
    assert_eq!(report.written, 0);
    // extra.txt not in version → deleted.
    assert_eq!(report.deleted, 1);
    assert!(!target.path().join("extra.txt").exists());
}

// ---------------------------------------------------------------------------
// Test: Safe mode does NOT delete extra local files
// ---------------------------------------------------------------------------

#[test]
fn test_checkout_safe_preserves_extra_local_files() {
    let src = tempfile::tempdir().unwrap();
    let state = AppState::memory();

    write(src.path(), "tracked.txt", b"tracked");
    let vid = initial_commit(src.path(), &["tracked.txt"], "v1", &state);

    let target = tempfile::tempdir().unwrap();
    write(target.path(), "tracked.txt", b"tracked"); // already correct
    write(target.path(), "untracked.txt", b"extra"); // extra local

    let report =
        checkout_to_path(&vid, target.path(), CheckoutMode::Safe, &state).unwrap();

    assert_eq!(report.written, 0);
    assert_eq!(report.deleted, 0);
    // untracked.txt must still exist.
    assert!(target.path().join("untracked.txt").exists());
}

// ---------------------------------------------------------------------------
// Test: already-correct local files are not re-written
// ---------------------------------------------------------------------------

#[test]
fn test_checkout_skips_already_correct_files() {
    let src = tempfile::tempdir().unwrap();
    let state = AppState::memory();

    write(src.path(), "file.txt", b"exact content");
    let vid = initial_commit(src.path(), &["file.txt"], "v1", &state);

    let target = tempfile::tempdir().unwrap();
    // Write the exact same bytes.
    write(target.path(), "file.txt", b"exact content");

    let report =
        checkout_to_path(&vid, target.path(), CheckoutMode::Force, &state).unwrap();

    // No write needed: content already matches.
    assert_eq!(report.written, 0);
    assert_eq!(report.deleted, 0);
}
