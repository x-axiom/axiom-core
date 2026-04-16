//! FastCDC-based content-defined chunking.
//!
//! Splits a byte stream into variable-size chunks whose boundaries are
//! determined by content, not position. This ensures that localized edits
//! only affect nearby chunk boundaries while the rest of the file produces
//! identical chunks and hashes.

use std::io::Read;

use fastcdc::v2020::FastCDC;

use crate::error::CasResult;
use crate::model::chunk::ChunkDescriptor;
use crate::model::hash::{hash_bytes, ChunkHash};
use crate::store::traits::ChunkStore;

// ---------------------------------------------------------------------------
// Chunk size policy
// ---------------------------------------------------------------------------

/// Configuration for chunk size boundaries.
#[derive(Clone, Debug)]
pub struct ChunkPolicy {
    /// Minimum chunk size in bytes.
    pub min_size: u32,
    /// Target average chunk size in bytes.
    pub avg_size: u32,
    /// Maximum chunk size in bytes.
    pub max_size: u32,
}

impl Default for ChunkPolicy {
    fn default() -> Self {
        Self {
            min_size: 16 * 1024,  // 16 KB
            avg_size: 64 * 1024,  // 64 KB
            max_size: 256 * 1024, // 256 KB
        }
    }
}

// ---------------------------------------------------------------------------
// Chunking result
// ---------------------------------------------------------------------------

/// Result of chunking a single file / byte stream.
#[derive(Clone, Debug)]
pub struct ChunkingResult {
    /// Ordered chunk descriptors (offset, length, hash, order).
    pub descriptors: Vec<ChunkDescriptor>,
    /// Total bytes processed.
    pub total_bytes: u64,
}

// ---------------------------------------------------------------------------
// chunk_reader — streaming chunker
// ---------------------------------------------------------------------------

/// Read an entire stream into memory and chunk it with FastCDC.
///
/// For files larger than available RAM a future version will stream in
/// fixed-size windows; for the POC this is sufficient and keeps the
/// implementation straightforward.
pub fn chunk_bytes(data: &[u8], policy: &ChunkPolicy) -> Vec<(ChunkHash, Vec<u8>, u64, u32)> {
    let chunker = FastCDC::new(
        data,
        policy.min_size,
        policy.avg_size,
        policy.max_size,
    );

    chunker
        .enumerate()
        .map(|(_order, chunk)| {
            let slice = &data[chunk.offset..chunk.offset + chunk.length];
            let hash = hash_bytes(slice);
            (hash, slice.to_vec(), chunk.offset as u64, chunk.length as u32)
        })
        .collect()
}

/// Chunk a `Read` source and persist every chunk to the given `ChunkStore`.
///
/// Returns a [`ChunkingResult`] with ordered descriptors.
pub fn chunk_and_persist<R: Read, S: ChunkStore>(
    reader: &mut R,
    policy: &ChunkPolicy,
    store: &S,
) -> CasResult<ChunkingResult> {
    // Read the full stream. A streaming windowed approach can replace this
    // buffer in a future iteration without changing the public API.
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;

    let raw_chunks = chunk_bytes(&buf, policy);

    let mut descriptors = Vec::with_capacity(raw_chunks.len());
    for (order, (hash, data, offset, length)) in raw_chunks.into_iter().enumerate() {
        // Persist (idempotent — CAS deduplicates by hash).
        store.put_chunk(data)?;

        descriptors.push(ChunkDescriptor {
            offset,
            length,
            hash,
            order: order as u32,
        });
    }

    let total_bytes = buf.len() as u64;

    Ok(ChunkingResult {
        descriptors,
        total_bytes,
    })
}

/// Reconstruct the original byte stream from ordered descriptors + CAS.
pub fn reassemble<S: ChunkStore>(
    descriptors: &[ChunkDescriptor],
    store: &S,
) -> CasResult<Vec<u8>> {
    let total: u64 = descriptors.iter().map(|d| d.length as u64).sum();
    let mut out = Vec::with_capacity(total as usize);

    // Sort by order to be safe.
    let mut sorted: Vec<_> = descriptors.to_vec();
    sorted.sort_by_key(|d| d.order);

    for desc in &sorted {
        let data = store
            .get_chunk(&desc.hash)?
            .ok_or_else(|| crate::error::CasError::NotFound(desc.hash.to_hex().to_string()))?;
        out.extend_from_slice(&data);
    }

    Ok(out)
}
