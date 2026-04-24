//! Bloom-filter–based have-set encoding for pull negotiation (E05-S05).
//!
//! ## Purpose
//! When a client already holds many version objects (> [`SWITCH_THRESHOLD`]),
//! sending the complete have-list as individual hash bytes is expensive in
//! bandwidth (~32 bytes × N).  A Bloom filter encodes the same set in roughly
//! `1/27th` of the space at a 1 % false-positive rate, which is acceptable
//! because a false positive means the server skips sending an object the
//! client actually needs — a very rare outcome that resolves on a subsequent
//! pull.
//!
//! ## Wire format
//! The serialised filter is a compact binary envelope:
//!
//! ```text
//! [ m : u32 LE ]   — number of bits in the filter
//! [ k : u8     ]   — number of hash functions
//! [ bits …     ]   — ⌈m/8⌉ raw bytes
//! ```
//!
//! Total overhead: 5 bytes header + ⌈m/8⌉ bit-array bytes.
//!
//! ## Hash functions
//! Items are 32-byte BLAKE3 digests.  We derive `k` independent bit indices
//! using the Kirsch–Mitzenmacher double-hashing technique:
//!
//! ```text
//! h1 = u64::from_le_bytes(item[0..8])
//! h2 = u64::from_le_bytes(item[8..16])
//! index_i = (h1 + i * h2) % m   for i in 0..k
//! ```
//!
//! This avoids re-hashing while providing independent-enough probes for small
//! `k` values (proven effective for k ≤ 30).

/// Object-count threshold above which the client switches from a full hash
/// list to a Bloom filter encoding.  Below this threshold the raw list is
/// smaller than any reasonable filter.
pub const SWITCH_THRESHOLD: usize = 10_000;

// ─── Optimal parameter helpers ────────────────────────────────────────────────

/// Optimal bit-array size for `n` items and a target false-positive rate `p`.
///
/// Formula: m = ⌈ −n · ln p / (ln 2)² ⌉, minimum 64.
fn optimal_m(n: usize, fp_rate: f64) -> usize {
    if n == 0 {
        return 64;
    }
    let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
    let m = (-(n as f64) * fp_rate.ln() / ln2_sq).ceil() as usize;
    m.max(64)
}

/// Optimal number of hash probes for bit-array of `m` bits and `n` items.
///
/// Formula: k = round((m / n) · ln 2), clamped to [1, 30].
fn optimal_k(m: usize, n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    let k = ((m as f64 / n as f64) * std::f64::consts::LN_2).round() as usize;
    k.clamp(1, 30)
}

// ─── BloomFilter ─────────────────────────────────────────────────────────────

/// A compact, serialisable Bloom filter for 32-byte hash items.
///
/// Create via [`BloomFilter::with_capacity`], populate with [`insert`], query
/// with [`contains`], then serialise with [`to_bytes`] / deserialise with
/// [`from_bytes`].
///
/// [`insert`]: BloomFilter::insert
/// [`contains`]: BloomFilter::contains
/// [`to_bytes`]: BloomFilter::to_bytes
/// [`from_bytes`]: BloomFilter::from_bytes
#[derive(Clone, Debug)]
pub struct BloomFilter {
    bits: Vec<u8>, // ⌈m/8⌉ bytes
    m: usize,      // total bit count
    k: usize,      // probe count
}

impl BloomFilter {
    /// Construct a filter optimised for `n` items at `fp_rate` (e.g. `0.01`
    /// for 1 % false-positive probability).
    pub fn with_capacity(n: usize, fp_rate: f64) -> Self {
        let m = optimal_m(n, fp_rate);
        let k = optimal_k(m, n);
        Self {
            bits: vec![0u8; (m + 7) / 8],
            m,
            k,
        }
    }

    /// Record that `hash` is a member of the set.
    ///
    /// `hash` must be exactly 32 bytes (a BLAKE3 digest).
    pub fn insert(&mut self, hash: &[u8; 32]) {
        for i in 0..self.k {
            let idx = self.probe(hash, i);
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }

    /// Return `true` if `hash` is **probably** a member of the set.
    ///
    /// A `true` result may be a false positive (probability ≤ configured
    /// `fp_rate`).  A `false` result is always correct (no false negatives).
    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        for i in 0..self.k {
            let idx = self.probe(hash, i);
            if self.bits[idx / 8] & (1 << (idx % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialise to the wire format (`[m: u32 LE][k: u8][bits…]`).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + self.bits.len());
        out.extend_from_slice(&(self.m as u32).to_le_bytes());
        out.push(self.k as u8);
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialise from the wire format produced by [`to_bytes`].
    ///
    /// Returns `None` if the data is malformed or truncated.
    ///
    /// [`to_bytes`]: BloomFilter::to_bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 5 {
            return None;
        }
        let m = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let k = data[4] as usize;
        if k == 0 || m == 0 {
            return None;
        }
        let expected_bytes = (m + 7) / 8;
        if data.len() < 5 + expected_bytes {
            return None;
        }
        Some(Self {
            bits: data[5..5 + expected_bytes].to_vec(),
            m,
            k,
        })
    }

    /// Estimated serialised size in bytes (header + bit-array).
    pub fn serialised_len(&self) -> usize {
        5 + self.bits.len()
    }

    // ── Private ──────────────────────────────────────────────────────────────

    /// Kirsch–Mitzenmacher double-hashing: derive bit index `i` from `hash`.
    #[inline]
    fn probe(&self, hash: &[u8; 32], i: usize) -> usize {
        let h1 = u64::from_le_bytes(hash[0..8].try_into().unwrap());
        let h2 = u64::from_le_bytes(hash[8..16].try_into().unwrap());
        (h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.m as u64) as usize
    }
}

// ─── Public helpers ───────────────────────────────────────────────────────────

/// Returns `true` when the client should use a Bloom filter instead of a full
/// hash list (i.e., when the have-set is large enough that the filter is
/// smaller than the raw list).
///
/// Threshold: [`SWITCH_THRESHOLD`] objects.
#[inline]
pub fn should_use_bloom(n: usize) -> bool {
    n > SWITCH_THRESHOLD
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hash(seed: u8) -> [u8; 32] {
        let mut h = [0u8; 32];
        h[0] = seed;
        h[1] = seed.wrapping_mul(7);
        h[2] = seed.wrapping_mul(13);
        h
    }

    #[test]
    fn threshold_switch_boundary() {
        assert!(!should_use_bloom(SWITCH_THRESHOLD));
        assert!(should_use_bloom(SWITCH_THRESHOLD + 1));
    }

    #[test]
    fn inserted_item_always_contained() {
        let mut bf = BloomFilter::with_capacity(1_000, 0.01);
        for i in 0u8..=200 {
            let h = make_hash(i);
            bf.insert(&h);
            assert!(bf.contains(&h), "inserted hash {i} must be found");
        }
    }

    #[test]
    fn not_inserted_item_probably_absent() {
        let mut bf = BloomFilter::with_capacity(1_000, 0.01);
        for i in 0u8..100 {
            bf.insert(&make_hash(i));
        }
        // Non-inserted hashes should almost all return false.
        let false_positives = (101u8..=255)
            .filter(|&i| bf.contains(&make_hash(i)))
            .count();
        // Allow a small number of false positives, but not all.
        assert!(false_positives < 50, "too many false positives: {false_positives}");
    }

    #[test]
    fn no_false_negatives() {
        let mut bf = BloomFilter::with_capacity(500, 0.01);
        let items: Vec<[u8; 32]> = (0u8..=255).map(make_hash).collect();
        for h in &items {
            bf.insert(h);
        }
        // Every inserted item must return true.
        for h in &items {
            assert!(bf.contains(h), "false negative detected");
        }
    }

    #[test]
    fn roundtrip_serialisation() {
        let mut bf = BloomFilter::with_capacity(200, 0.01);
        let items: Vec<[u8; 32]> = (0u8..50).map(make_hash).collect();
        for h in &items {
            bf.insert(h);
        }

        let bytes = bf.to_bytes();
        let bf2 = BloomFilter::from_bytes(&bytes).expect("deserialise ok");

        // All inserted items must still be found after roundtrip.
        for h in &items {
            assert!(bf2.contains(h), "item lost after roundtrip");
        }
    }

    #[test]
    fn from_bytes_rejects_truncated_data() {
        let bf = BloomFilter::with_capacity(100, 0.01);
        let bytes = bf.to_bytes();
        assert!(BloomFilter::from_bytes(&bytes[..4]).is_none());
        assert!(BloomFilter::from_bytes(&[]).is_none());
    }

    #[test]
    fn serialised_len_is_compact_for_10k_items() {
        let bf = BloomFilter::with_capacity(10_000, 0.01);
        let bytes = bf.to_bytes();
        let raw_list_size = 10_000 * 32; // 320 KB
        // Filter should be substantially smaller than the raw list.
        assert!(
            bytes.len() < raw_list_size / 10,
            "filter too large: {} bytes (raw list = {} bytes)",
            bytes.len(),
            raw_list_size
        );
    }

    #[test]
    fn optimal_parameters_for_1pct_fp() {
        // At 1% FP rate, optimal k is ~7.
        let bf = BloomFilter::with_capacity(10_000, 0.01);
        assert!(bf.k >= 6 && bf.k <= 8, "expected k≈7, got {}", bf.k);
        // m/n ≈ 9.6 bits per item.
        let bits_per_item = bf.m as f64 / 10_000.0;
        assert!(
            (9.0..=11.0).contains(&bits_per_item),
            "expected ~9.6 bits/item, got {bits_per_item:.2}"
        );
    }
}
