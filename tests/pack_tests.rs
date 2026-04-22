//! Integration tests for the AXPK pack format (E03-S02).

#![cfg(feature = "cloud")]

use axiom_core::sync::pack::{ObjectType, PackReader, PackWriter};

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn make_hash(seed: u8) -> [u8; 32] {
    [seed; 32]
}

// ─── Basic API ───────────────────────────────────────────────────────────────

#[test]
fn empty_pack_round_trips() {
    let pack = PackWriter::new().finish();
    let mut reader = PackReader::new(&pack).expect("valid pack");
    assert_eq!(reader.object_count(), 0);
    assert!(reader.next_object().unwrap().is_none());
}

#[test]
fn single_object_round_trip() {
    let mut w = PackWriter::new();
    let hash = make_hash(0x42);
    let data = b"hello, axiom!";
    w.add_object(ObjectType::Chunk, &hash, data).unwrap();
    let pack = w.finish();

    let mut r = PackReader::new(&pack).unwrap();
    assert_eq!(r.object_count(), 1);

    let obj = r.next_object().unwrap().unwrap();
    assert_eq!(obj.obj_type, ObjectType::Chunk);
    assert_eq!(obj.hash, hash);
    assert_eq!(obj.data, data);

    assert!(r.next_object().unwrap().is_none());
}

#[test]
fn multi_object_round_trip_all_types() {
    let mut w = PackWriter::new();

    let entries: Vec<(ObjectType, u8, &[u8])> = vec![
        (ObjectType::Chunk, 0x01, b"chunk payload"),
        (ObjectType::TreeNode, 0x02, b"tree node payload"),
        (ObjectType::NodeEntry, 0x03, b"node entry payload"),
        (ObjectType::Version, 0x04, b"version payload"),
    ];

    for (t, seed, data) in &entries {
        w.add_object(*t, &make_hash(*seed), data).unwrap();
    }
    assert_eq!(w.len(), 4);
    let pack = w.finish();

    let mut r = PackReader::new(&pack).unwrap();
    assert_eq!(r.object_count(), 4);

    for (expected_type, seed, expected_data) in &entries {
        let obj = r.next_object().unwrap().unwrap();
        assert_eq!(obj.obj_type, *expected_type);
        assert_eq!(obj.hash, make_hash(*seed));
        assert_eq!(obj.data, *expected_data);
    }
    assert!(r.next_object().unwrap().is_none());
}

// ─── Checksum ────────────────────────────────────────────────────────────────

#[test]
fn tampered_pack_fails_checksum() {
    let mut pack = PackWriter::new().finish();
    // Flip a byte in the middle of the header.
    pack[4] ^= 0xFF;
    assert!(PackReader::new(&pack).is_err());
}

#[test]
fn truncated_pack_fails() {
    let pack = PackWriter::new().finish();
    assert!(PackReader::new(&pack[..pack.len() - 1]).is_err());
}

// ─── Writer: object count in header ──────────────────────────────────────────

#[test]
fn object_count_patched_correctly() {
    let mut w = PackWriter::new();
    for i in 0..10u8 {
        w.add_object(ObjectType::Chunk, &make_hash(i), &[i; 100]).unwrap();
    }
    let pack = w.finish();
    let r = PackReader::new(&pack).unwrap();
    assert_eq!(r.object_count(), 10);
}

// ─── Compression ─────────────────────────────────────────────────────────────

/// Highly compressible data (all zeros) should produce a pack that is
/// meaningfully smaller than the raw payload.
#[test]
fn zstd_compresses_repetitive_data() {
    let payload = vec![0u8; 64 * 1024]; // 64 KiB of zeros
    let mut w = PackWriter::new();
    w.add_object(ObjectType::Chunk, &make_hash(0), &payload).unwrap();
    let pack = w.finish();
    // Compressed pack must be substantially smaller than raw payload.
    assert!(
        pack.len() < payload.len() / 4,
        "pack size {} should be < {} (1/4 of raw)",
        pack.len(),
        payload.len() / 4
    );
}

/// Incompressible data (random-ish) should still round-trip correctly,
/// even if compression doesn't shrink it much.
#[test]
fn zstd_round_trips_incompressible_data() {
    // Pseudo-random but deterministic payload.
    let payload: Vec<u8> = (0u16..1024).map(|i| (i.wrapping_mul(251) as u8)).collect();
    let mut w = PackWriter::new();
    w.add_object(ObjectType::Chunk, &make_hash(1), &payload).unwrap();
    let pack = w.finish();

    let mut r = PackReader::new(&pack).unwrap();
    let obj = r.next_object().unwrap().unwrap();
    assert_eq!(obj.data, payload);
}

// ─── Large pack ──────────────────────────────────────────────────────────────

/// Stress-test with 200 objects of varying sizes.
#[test]
fn large_pack_round_trip() {
    let mut w = PackWriter::new();
    let types = [
        ObjectType::Chunk,
        ObjectType::TreeNode,
        ObjectType::NodeEntry,
        ObjectType::Version,
    ];
    let payloads: Vec<Vec<u8>> = (0u8..200)
        .map(|i| vec![i; (i as usize + 1) * 16])
        .collect();

    for (i, payload) in payloads.iter().enumerate() {
        let t = types[i % types.len()];
        w.add_object(t, &make_hash(i as u8), payload).unwrap();
    }
    let pack = w.finish();

    let mut r = PackReader::new(&pack).unwrap();
    assert_eq!(r.object_count(), 200);

    for (i, expected) in payloads.iter().enumerate() {
        let obj = r.next_object().unwrap().unwrap();
        assert_eq!(obj.hash, make_hash(i as u8));
        assert_eq!(&obj.data, expected);
    }
    assert!(r.next_object().unwrap().is_none());
}
