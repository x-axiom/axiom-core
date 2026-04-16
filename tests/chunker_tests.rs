use std::io::Cursor;

use axiom_core::chunker::{chunk_and_persist, chunk_bytes, reassemble, ChunkPolicy};
use axiom_core::model::hash::hash_bytes;
use axiom_core::store::{ChunkStore, InMemoryChunkStore};

fn default_policy() -> ChunkPolicy {
    ChunkPolicy::default()
}

/// Small policy for tests so we get multiple chunks from smaller inputs.
fn small_policy() -> ChunkPolicy {
    ChunkPolicy {
        min_size: 256,
        avg_size: 1024,
        max_size: 4096,
    }
}

// ---------------------------------------------------------------------------
// Stability: identical content produces identical chunks
// ---------------------------------------------------------------------------

#[test]
fn test_identical_content_produces_stable_chunks() {
    let data = vec![0xABu8; 128 * 1024]; // 128 KB
    let policy = small_policy();

    let run1 = chunk_bytes(&data, &policy);
    let run2 = chunk_bytes(&data, &policy);

    assert!(!run1.is_empty());
    assert_eq!(run1.len(), run2.len());
    for (a, b) in run1.iter().zip(run2.iter()) {
        assert_eq!(a.0, b.0, "hashes must be identical");
        assert_eq!(a.2, b.2, "offsets must be identical");
        assert_eq!(a.3, b.3, "lengths must be identical");
    }
}

// ---------------------------------------------------------------------------
// Localized edit: only nearby chunks change
// ---------------------------------------------------------------------------

#[test]
fn test_localized_edit_bounded_churn() {
    // Use a larger file with the default policy for realistic chunk boundaries.
    let size = 2 * 1024 * 1024; // 2 MB
    let mut original = vec![0u8; size];
    // Use a simple PRNG for reproducible pseudo-random content.
    let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
    for b in original.iter_mut() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *b = (state >> 33) as u8;
    }

    let mut modified = original.clone();
    // Modify a small 128-byte region in the middle.
    let mid = modified.len() / 2;
    for b in &mut modified[mid..mid + 128] {
        *b = 0xFF;
    }

    let policy = ChunkPolicy::default();
    let chunks_orig = chunk_bytes(&original, &policy);
    let chunks_mod = chunk_bytes(&modified, &policy);

    // Both should produce multiple chunks.
    assert!(chunks_orig.len() > 4);
    assert!(chunks_mod.len() > 4);

    // Count how many chunk hashes are shared.
    let orig_hashes: std::collections::HashSet<_> =
        chunks_orig.iter().map(|c| c.0).collect();
    let mod_hashes: std::collections::HashSet<_> =
        chunks_mod.iter().map(|c| c.0).collect();

    let shared = orig_hashes.intersection(&mod_hashes).count();
    // Most chunks should be unchanged — at least half should survive.
    assert!(
        shared > chunks_orig.len() / 2,
        "too many chunks changed: shared={shared}, total={}",
        chunks_orig.len()
    );
}

// ---------------------------------------------------------------------------
// Chunk sizes respect min/max
// ---------------------------------------------------------------------------

#[test]
fn test_chunk_sizes_within_bounds() {
    let mut data = vec![0u8; 512 * 1024]; // 512 KB
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((i * 31 + 5) % 256) as u8;
    }

    let policy = small_policy();
    let chunks = chunk_bytes(&data, &policy);

    assert!(chunks.len() > 1);
    for (i, (_hash, chunk_data, _offset, length)) in chunks.iter().enumerate() {
        // Last chunk may be smaller than min_size.
        if i < chunks.len() - 1 {
            assert!(
                *length >= policy.min_size,
                "chunk {i} size {length} < min {}",
                policy.min_size
            );
        }
        assert!(
            *length <= policy.max_size,
            "chunk {i} size {length} > max {}",
            policy.max_size
        );
        assert_eq!(*length as usize, chunk_data.len());
    }
}

// ---------------------------------------------------------------------------
// Chunk descriptors have correct offset/length/order and reconstruct data
// ---------------------------------------------------------------------------

#[test]
fn test_descriptors_offset_length_order() {
    let mut data = vec![0u8; 128 * 1024];
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((i * 17 + 3) % 256) as u8;
    }

    let store = InMemoryChunkStore::new();
    let mut cursor = Cursor::new(data.clone());
    let result = chunk_and_persist(&mut cursor, &small_policy(), &store).unwrap();

    assert_eq!(result.total_bytes, data.len() as u64);
    assert!(!result.descriptors.is_empty());

    // Verify ordering and contiguity.
    let mut expected_offset: u64 = 0;
    for (i, desc) in result.descriptors.iter().enumerate() {
        assert_eq!(desc.order, i as u32, "order mismatch at {i}");
        assert_eq!(desc.offset, expected_offset, "offset gap at {i}");
        expected_offset += desc.length as u64;
    }
    assert_eq!(expected_offset, data.len() as u64, "offsets don't cover full input");
}

// ---------------------------------------------------------------------------
// Roundtrip: chunk then reassemble
// ---------------------------------------------------------------------------

#[test]
fn test_roundtrip_chunk_and_reassemble() {
    let mut data = vec![0u8; 200 * 1024];
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((i * 41 + 9) % 256) as u8;
    }

    let store = InMemoryChunkStore::new();
    let mut cursor = Cursor::new(data.clone());
    let result = chunk_and_persist(&mut cursor, &small_policy(), &store).unwrap();

    let reconstructed = reassemble(&result.descriptors, &store).unwrap();
    assert_eq!(reconstructed, data);
}

// ---------------------------------------------------------------------------
// CAS persistence: chunks are deduplicated
// ---------------------------------------------------------------------------

#[test]
fn test_cas_deduplication() {
    let data = vec![0xCDu8; 64 * 1024];

    let store = InMemoryChunkStore::new();
    let mut c1 = Cursor::new(data.clone());
    let mut c2 = Cursor::new(data.clone());

    let r1 = chunk_and_persist(&mut c1, &small_policy(), &store).unwrap();
    let r2 = chunk_and_persist(&mut c2, &small_policy(), &store).unwrap();

    assert_eq!(r1.descriptors.len(), r2.descriptors.len());
    for (a, b) in r1.descriptors.iter().zip(r2.descriptors.iter()) {
        assert_eq!(a.hash, b.hash);
        // Both should be retrievable.
        assert!(store.has_chunk(&a.hash).unwrap());
    }
}

// ---------------------------------------------------------------------------
// Large file: 10 MB+
// ---------------------------------------------------------------------------

#[test]
fn test_large_file_chunking() {
    let size = 10 * 1024 * 1024 + 7; // 10 MB + 7 bytes
    let mut data = vec![0u8; size];
    for (i, b) in data.iter_mut().enumerate() {
        *b = ((i * 53 + 11) % 256) as u8;
    }

    let store = InMemoryChunkStore::new();
    let mut cursor = Cursor::new(data.clone());
    let result =
        chunk_and_persist(&mut cursor, &ChunkPolicy::default(), &store).unwrap();

    assert_eq!(result.total_bytes, size as u64);
    assert!(
        result.descriptors.len() > 10,
        "expected many chunks, got {}",
        result.descriptors.len()
    );

    // Verify roundtrip on large file.
    let reconstructed = reassemble(&result.descriptors, &store).unwrap();
    assert_eq!(reconstructed.len(), data.len());
    assert_eq!(reconstructed, data);
}

// ---------------------------------------------------------------------------
// Empty input
// ---------------------------------------------------------------------------

#[test]
fn test_empty_input() {
    let store = InMemoryChunkStore::new();
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let result = chunk_and_persist(&mut cursor, &small_policy(), &store).unwrap();

    assert!(result.descriptors.is_empty());
    assert_eq!(result.total_bytes, 0);
}

// ---------------------------------------------------------------------------
// Small file (smaller than min_size)
// ---------------------------------------------------------------------------

#[test]
fn test_small_file_single_chunk() {
    let data = b"hello world".to_vec();
    let store = InMemoryChunkStore::new();
    let mut cursor = Cursor::new(data.clone());
    let result = chunk_and_persist(&mut cursor, &small_policy(), &store).unwrap();

    assert_eq!(result.descriptors.len(), 1);
    assert_eq!(result.descriptors[0].offset, 0);
    assert_eq!(result.descriptors[0].length, data.len() as u32);
    assert_eq!(result.descriptors[0].order, 0);
    assert_eq!(result.descriptors[0].hash, hash_bytes(&data));

    let reconstructed = reassemble(&result.descriptors, &store).unwrap();
    assert_eq!(reconstructed, data);
}
