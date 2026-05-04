//! Parallel upload helpers for multi-stream push (E05-S04).
//!
//! Provides [`shard_object_lists`] which distributes objects across `n`
//! buckets for concurrent `UploadPack` transmission. Objects are assigned
//! deterministically by `hash[0] % n` so repeated calls with the same input
//! always produce identical shard assignments.

use crate::sync::proto::{ObjectId, ObjectList};

/// Distributes each object in `lists` into one of `n` shards by taking
/// `hash[0] % n`. Within each shard the `ObjectList` grouping by proto type is
/// preserved (empty groups are dropped).
///
/// # Special cases
/// * `n == 0` or `n == 1` → the original list is returned wrapped in a single
///   element `Vec` (no copying).
/// * `lists` is empty     → a single empty shard is returned.
pub fn shard_object_lists(lists: Vec<ObjectList>, n: usize) -> Vec<Vec<ObjectList>> {
    if n <= 1 || lists.is_empty() {
        return vec![lists];
    }

    let mut buckets: Vec<std::collections::HashMap<i32, Vec<ObjectId>>> =
        (0..n).map(|_| std::collections::HashMap::new()).collect();

    for list in lists {
        let proto_type = list.r#type;
        for oid in list.hashes {
            let shard = if oid.hash.is_empty() { 0 } else { (oid.hash[0] as usize) % n };
            buckets[shard].entry(proto_type).or_default().push(oid);
        }
    }

    buckets
        .into_iter()
        .map(|shard| {
            let mut out: Vec<ObjectList> = shard
                .into_iter()
                .filter(|(_, hashes)| !hashes.is_empty())
                .map(|(r#type, hashes)| ObjectList { r#type, hashes })
                .collect();
            // Sort by proto type for deterministic ordering within each shard.
            out.sort_by_key(|l| l.r#type);
            out
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(first_byte: u8) -> ObjectId {
        let mut h = vec![0u8; 32];
        h[0] = first_byte;
        ObjectId { hash: h }
    }

    #[test]
    fn single_shard_returns_original() {
        let lists = vec![ObjectList {
            r#type: 1,
            hashes: vec![oid(0), oid(1)],
        }];
        let shards = shard_object_lists(lists, 1);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0][0].hashes.len(), 2);
    }

    #[test]
    fn zero_shard_count_treated_as_one() {
        let lists = vec![ObjectList { r#type: 1, hashes: vec![oid(42)] }];
        let shards = shard_object_lists(lists, 0);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0][0].hashes.len(), 1);
    }

    #[test]
    fn empty_input_returns_single_empty_shard() {
        let shards = shard_object_lists(vec![], 4);
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_empty());
    }

    #[test]
    fn distributes_8_objects_evenly_into_4_shards() {
        // First bytes 0..7 → shards 0,1,2,3,0,1,2,3 (each shard gets 2).
        let hashes: Vec<ObjectId> = (0u8..8).map(oid).collect();
        let lists = vec![ObjectList { r#type: 1, hashes }];
        let shards = shard_object_lists(lists, 4);
        assert_eq!(shards.len(), 4);
        for shard in &shards {
            let count: usize = shard.iter().map(|l| l.hashes.len()).sum();
            assert_eq!(count, 2);
        }
    }

    #[test]
    fn total_object_count_is_preserved() {
        let total_in: usize = 20;
        let hashes: Vec<ObjectId> = (0u8..20).map(oid).collect();
        let lists = vec![ObjectList { r#type: 1, hashes }];
        let shards = shard_object_lists(lists, 7);
        let total_out: usize = shards
            .iter()
            .flat_map(|s| s.iter())
            .map(|l| l.hashes.len())
            .sum();
        assert_eq!(total_out, total_in);
    }

    #[test]
    fn type_grouping_preserved_within_shard() {
        // Objects with first byte 0 go to shard 0; verify both types present.
        let lists = vec![
            ObjectList { r#type: 1, hashes: vec![oid(0)] },
            ObjectList { r#type: 2, hashes: vec![oid(0)] },
        ];
        let shards = shard_object_lists(lists, 2);
        assert_eq!(shards.len(), 2);
        // Shard 0 should have 2 ObjectLists (types 1 and 2), shard 1 empty.
        let non_empty: Vec<_> = shards.iter().filter(|s| !s.is_empty()).collect();
        assert_eq!(non_empty.len(), 1);
        assert_eq!(non_empty[0].len(), 2); // two type groups
    }
}
