use std::io::Cursor;

use axiom_core::chunker::{chunk_and_persist, ChunkPolicy};
use axiom_core::store::InMemoryChunkStore;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn make_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((i * 31 + 7) % 256) as u8;
    }
    data
}

fn bench_chunking_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("fastcdc_chunk");
    let policy = ChunkPolicy::default();

    for &size in &[
        64 * 1024,           // 64 KB
        1024 * 1024,         // 1 MB
        10 * 1024 * 1024,    // 10 MB
    ] {
        let label = if size >= 1024 * 1024 {
            format!("{}MB", size / (1024 * 1024))
        } else {
            format!("{}KB", size / 1024)
        };

        let data = make_data(size);

        group.throughput(criterion::Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(&label),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = InMemoryChunkStore::new();
                    let mut cursor = Cursor::new(data.as_slice());
                    chunk_and_persist(&mut cursor, &policy, &store).unwrap()
                });
            },
        );
    }
    group.finish();
}

fn bench_chunk_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("fastcdc_chunk_count");
    let policy = ChunkPolicy::default();

    for &size in &[1024 * 1024, 10 * 1024 * 1024] {
        let label = format!("{}MB", size / (1024 * 1024));
        let data = make_data(size);

        group.bench_with_input(
            BenchmarkId::from_parameter(&label),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = InMemoryChunkStore::new();
                    let mut cursor = Cursor::new(data.as_slice());
                    let result = chunk_and_persist(&mut cursor, &policy, &store).unwrap();
                    result.descriptors.len()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_chunking_throughput, bench_chunk_count);
criterion_main!(benches);
