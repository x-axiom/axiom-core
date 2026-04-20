//! Performance benchmarks for axiom-core core operations (AXIOM-113).
//!
//! Covers: chunking, Merkle tree build, commit, diff, and rehydrate (download).
//! Run with: cargo bench --bench poc_benchmark

use std::hint::black_box;
use std::sync::Arc;

use axiom_core::chunker::{chunk_bytes, ChunkPolicy};
use axiom_core::commit::{CommitRequest, CommitService};
use axiom_core::diff_engine::diff_versions;
use axiom_core::merkle::{build_tree, rehydrate, DEFAULT_FAN_OUT};
use axiom_core::model::chunk::ChunkDescriptor;
use axiom_core::model::hash::{ChunkHash, VersionId};
use axiom_core::namespace::{build_directory_tree, FileInput};
use axiom_core::store::memory::{
    InMemoryChunkStore, InMemoryNodeStore, InMemoryPathIndex, InMemoryRefRepo, InMemoryTreeStore,
    InMemoryVersionRepo,
};
use axiom_core::store::traits::{ChunkStore, RefRepo, VersionRepo};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// ---------------------------------------------------------------------------
// Helper: create a FileInput from raw data
// ---------------------------------------------------------------------------

fn make_file_input(
    name: &str,
    data: &[u8],
    chunk_store: &InMemoryChunkStore,
    tree_store: &InMemoryTreeStore,
) -> FileInput {
    let hash = chunk_store.put_chunk(data.to_vec()).unwrap();
    let descs = vec![ChunkDescriptor {
        hash,
        offset: 0,
        length: data.len() as u32,
        order: 0,
    }];
    let fo = build_tree(&descs, DEFAULT_FAN_OUT, tree_store).unwrap();
    FileInput {
        path: name.to_string(),
        root: fo.root,
        size: data.len() as u64,
    }
}

// ---------------------------------------------------------------------------
// 1. FastCDC chunking
// ---------------------------------------------------------------------------

fn bench_chunking(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunking");
    let policy = ChunkPolicy::default();

    for &size in &[64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        let label = if size >= 1024 * 1024 {
            format!("{}MB", size / (1024 * 1024))
        } else {
            format!("{}KB", size / 1024)
        };
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        group.bench_with_input(BenchmarkId::from_parameter(&label), &data, |b, data| {
            b.iter(|| chunk_bytes(black_box(data), &policy));
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Merkle tree build
// ---------------------------------------------------------------------------

fn bench_merkle_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("merkle_build");

    for &num_chunks in &[10, 100, 500] {
        let store = InMemoryTreeStore::new();
        let chunk_store = InMemoryChunkStore::new();
        let descs: Vec<ChunkDescriptor> = (0..num_chunks)
            .map(|i| {
                let data = vec![i as u8; 1024];
                let hash = chunk_store.put_chunk(data).unwrap();
                ChunkDescriptor {
                    hash,
                    offset: (i * 1024) as u64,
                    length: 1024,
                    order: i,
                }
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_chunks}_chunks")),
            &descs,
            |b, descs| {
                b.iter(|| build_tree(black_box(descs), DEFAULT_FAN_OUT, &store));
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Rehydrate (download path)
// ---------------------------------------------------------------------------

fn bench_rehydrate(c: &mut Criterion) {
    let mut group = c.benchmark_group("rehydrate");

    for &num_chunks in &[10, 100, 500] {
        let tree_store = InMemoryTreeStore::new();
        let chunk_store = InMemoryChunkStore::new();
        let descs: Vec<ChunkDescriptor> = (0..num_chunks)
            .map(|i| {
                let data = vec![i as u8; 4096];
                let hash = chunk_store.put_chunk(data).unwrap();
                ChunkDescriptor {
                    hash,
                    offset: (i * 4096) as u64,
                    length: 4096,
                    order: i,
                }
            })
            .collect();
        let fo = build_tree(&descs, DEFAULT_FAN_OUT, &tree_store).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_chunks}_chunks")),
            &fo.root,
            |b, root| {
                b.iter(|| rehydrate(black_box(root), &tree_store));
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Commit (version creation + branch advance)
// ---------------------------------------------------------------------------

fn bench_commit(c: &mut Criterion) {
    c.bench_function("commit_create_version", |b| {
        let versions: Arc<dyn VersionRepo> = Arc::new(InMemoryVersionRepo::new());
        let refs: Arc<dyn RefRepo> = Arc::new(InMemoryRefRepo::new());
        let svc = CommitService::new(versions, refs);
        let root = ChunkHash::from_bytes([42u8; 32]);

        b.iter(|| {
            let _ = svc.commit(black_box(CommitRequest {
                root: root.clone(),
                parents: vec![],
                message: "bench".into(),
                metadata: Default::default(),
                branch: None,
            }));
        });
    });
}

// ---------------------------------------------------------------------------
// 5. Diff engine
// ---------------------------------------------------------------------------

fn bench_diff(c: &mut Criterion) {
    let mut group = c.benchmark_group("diff");

    for &num_files in &[10, 50, 200] {
        let chunk_store = InMemoryChunkStore::new();
        let tree_store = InMemoryTreeStore::new();
        let node_store = InMemoryNodeStore::new();
        let path_index = InMemoryPathIndex::new();

        // Build "old" version.
        let files_old: Vec<FileInput> = (0..num_files)
            .map(|i| {
                make_file_input(
                    &format!("file_{i}.txt"),
                    format!("old-content-{i}").as_bytes(),
                    &chunk_store,
                    &tree_store,
                )
            })
            .collect();
        let vid_old = VersionId::from("bench-old");
        let old_root =
            build_directory_tree(&files_old, &vid_old, &node_store, &path_index).unwrap();

        // Build "new" version: modify half, add 10%.
        let mut files_new: Vec<FileInput> = (0..num_files)
            .map(|i| {
                let content = if i % 2 == 0 {
                    format!("new-content-{i}")
                } else {
                    format!("old-content-{i}")
                };
                make_file_input(
                    &format!("file_{i}.txt"),
                    content.as_bytes(),
                    &chunk_store,
                    &tree_store,
                )
            })
            .collect();
        for i in num_files..num_files + num_files / 10 {
            files_new.push(make_file_input(
                &format!("file_{i}.txt"),
                format!("added-{i}").as_bytes(),
                &chunk_store,
                &tree_store,
            ));
        }
        let vid_new = VersionId::from("bench-new");
        let new_root =
            build_directory_tree(&files_new, &vid_new, &node_store, &path_index).unwrap();

        let old_hash = old_root.hash.clone();
        let new_hash = new_root.hash.clone();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_files}_files")),
            &(&old_hash, &new_hash),
            |b, &(old, new)| {
                b.iter(|| diff_versions(black_box(old), black_box(new), &node_store, &tree_store));
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// 6. Namespace / directory tree build
// ---------------------------------------------------------------------------

fn bench_namespace_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("namespace_build");

    for &num_files in &[10, 50, 200] {
        let chunk_store = InMemoryChunkStore::new();
        let tree_store = InMemoryTreeStore::new();
        let node_store = InMemoryNodeStore::new();
        let path_index = InMemoryPathIndex::new();

        let files: Vec<FileInput> = (0..num_files)
            .map(|i| {
                let dir = format!("dir{}", i % 5);
                make_file_input(
                    &format!("{dir}/file_{i}.txt"),
                    &vec![i as u8; 1024],
                    &chunk_store,
                    &tree_store,
                )
            })
            .collect();

        let vid = VersionId::from("bench-ns");
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_files}_files")),
            &files,
            |b, files| {
                b.iter(|| {
                    build_directory_tree(black_box(files), &vid, &node_store, &path_index)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_chunking,
    bench_merkle_build,
    bench_rehydrate,
    bench_commit,
    bench_diff,
    bench_namespace_build,
);
criterion_main!(benches);
