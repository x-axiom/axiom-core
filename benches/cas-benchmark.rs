use axiom_core::store::{ChunkStore, InMemoryChunkStore, InMemoryCas};
use axiom_core::model::hash_children;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_put_chunk_various_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_chunk");

    for size in [1024, 4096, 1024 * 1024].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}kb", size / 1024)),
            size,
            |b, &size| {
                let store = InMemoryChunkStore::new();
                let data = vec![42u8; size];

                b.iter(|| {
                    let _ = store.put_chunk(black_box(data.clone()));
                });
            },
        );
    }
    group.finish();
}

fn bench_get_chunk(c: &mut Criterion) {
    c.bench_function("get_chunk_4kb", |b| {
        let store = InMemoryChunkStore::new();
        let data = vec![7u8; 4096];
        let hash = store.put_chunk(data).unwrap();

        b.iter(|| {
            let _ = store.get_chunk(black_box(&hash));
        });
    });
}

fn bench_put_object(c: &mut Criterion) {
    c.bench_function("put_object_100_chunks", |b| {
        let store = InMemoryCas::new();
        let chunks: Vec<_> = (0..100)
            .map(|i| store.put_chunk(vec![i as u8; 256]))
            .collect();

        b.iter(|| {
            let _ = hash_children(black_box(&chunks));
        });
    });
}

criterion_group!(
    benches,
    bench_put_chunk_various_sizes,
    bench_get_chunk,
    bench_put_object
);
criterion_main!(benches);
