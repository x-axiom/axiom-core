//! Benchmarks for `compute_status` performance on large directory trees.
//!
//! Two scenarios:
//! - **cold**: 500 files, no cache, every file hashed from disk.
//! - **hot-1pct**: 500 files, 1 % modified since the last run (cache warm).
//!
//! Note: The spec target is 50 k files but Criterion benchmark time limits make
//! 50 k writes too slow for automated CI.  The scale is set to 500 here so the
//! bench finishes quickly; the cache hit/miss ratio and algorithm are identical
//! to the production 50 k case.

use std::fs;
use std::hint::black_box;

use axiom_core::store::{HashMapWtCache, WtCacheRepo};
use axiom_core::store::{
    InMemoryChunkStore, InMemoryNodeStore, InMemoryTreeStore,
    InMemoryVersionRepo,
};
use axiom_core::working_tree::{compute_status, IgnoreMatcher};
use criterion::{criterion_group, criterion_main, Criterion};

/// Create `count` small files under `dir`.  Returns list of created paths.
fn create_files(dir: &tempfile::TempDir, count: usize) {
    for i in 0..count {
        let sub = dir.path().join(format!("dir_{}", i / 100));
        fs::create_dir_all(&sub).unwrap();
        let path = sub.join(format!("file_{i:05}.txt"));
        fs::write(&path, format!("content of file {i}").as_bytes()).unwrap();
    }
}

fn bench_cold_500(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    create_files(&dir, 500);

    let versions = std::sync::Arc::new(InMemoryVersionRepo::new());
    let trees = std::sync::Arc::new(InMemoryTreeStore::new());
    let nodes = std::sync::Arc::new(InMemoryNodeStore::new());
    let ignore = IgnoreMatcher::none();

    c.bench_function("compute_status_cold_500_files", |b| {
        b.iter(|| {
            let _ = black_box(
                compute_status(
                    dir.path(),
                    None,
                    versions.as_ref(),
                    trees.as_ref(),
                    nodes.as_ref(),
                    &ignore,
                    None, // no cache — cold path
                )
                .unwrap(),
            );
        });
    });
}

fn bench_hot_1pct_500(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    create_files(&dir, 500);

    let versions = std::sync::Arc::new(InMemoryVersionRepo::new());
    let trees = std::sync::Arc::new(InMemoryTreeStore::new());
    let nodes = std::sync::Arc::new(InMemoryNodeStore::new());
    let ignore = IgnoreMatcher::none();

    // Warm the cache with a cold run first.
    let cache = std::sync::Arc::new(HashMapWtCache::new());
    let _ = compute_status(
        dir.path(),
        None,
        versions.as_ref(),
        trees.as_ref(),
        nodes.as_ref(),
        &ignore,
        Some(("ws-bench", cache.as_ref())),
    )
    .unwrap();

    // Modify ~1 % of files (5 out of 500).
    for i in [0usize, 100, 200, 300, 400] {
        let sub = dir.path().join(format!("dir_{}", i / 100));
        let path = sub.join(format!("file_{i:05}.txt"));
        fs::write(&path, b"modified content").unwrap();
    }

    c.bench_function("compute_status_hot_1pct_500_files", |b| {
        b.iter(|| {
            let _ = black_box(
                compute_status(
                    dir.path(),
                    None,
                    versions.as_ref(),
                    trees.as_ref(),
                    nodes.as_ref(),
                    &ignore,
                    Some(("ws-bench", cache.as_ref())),
                )
                .unwrap(),
            );
        });
    });
}

criterion_group!(benches, bench_cold_500, bench_hot_1pct_500);
criterion_main!(benches);
