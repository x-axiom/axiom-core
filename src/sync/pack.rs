//! AXPK — binary pack format encoder / decoder.
//!
//! ## Wire layout
//!
//! ```text
//! Header  (16 bytes)
//!   magic          : [u8; 4]  = b"AXPK"
//!   version        : u32 LE   = 1
//!   object_count   : u64 LE
//!
//! Per-object entry
//!   type           : u8       (0=Chunk, 1=TreeNode, 2=NodeEntry, 3=Version)
//!   hash           : [u8; 32] (BLAKE3)
//!   compressed_len : u64 LE
//!   data           : [u8; compressed_len]  (zstd, level 3)
//!
//! Footer  (32 bytes)
//!   checksum       : [u8; 32] (BLAKE3 over all preceding bytes)
//! ```

use std::io::Cursor;

use crate::error::{CasError, CasResult};

// ─── Constants ───────────────────────────────────────────────────────────────

const MAGIC: &[u8; 4] = b"AXPK";
const FORMAT_VERSION: u32 = 1;

/// Byte offset of the `object_count` field in the header.
const OBJECT_COUNT_OFFSET: usize = 8; // after magic(4) + version(4)
const HEADER_SIZE: usize = 4 + 4 + 8; // magic + version + object_count
const CHECKSUM_SIZE: usize = 32;

/// Per-entry prefix: type(1) + hash(32) + compressed_len(8).
const ENTRY_PREFIX_SIZE: usize = 1 + 32 + 8;

// ─── ObjectType ──────────────────────────────────────────────────────────────

/// Type tag for an object entry in an AXPK pack.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectType {
    Chunk     = 0,
    TreeNode  = 1,
    NodeEntry = 2,
    Version   = 3,
}

impl TryFrom<u8> for ObjectType {
    type Error = CasError;

    fn try_from(v: u8) -> CasResult<Self> {
        match v {
            0 => Ok(Self::Chunk),
            1 => Ok(Self::TreeNode),
            2 => Ok(Self::NodeEntry),
            3 => Ok(Self::Version),
            _ => Err(CasError::SyncError(format!("unknown object type byte: {v}"))),
        }
    }
}

// ─── PackObject ──────────────────────────────────────────────────────────────

/// A single decoded pack entry. `data` is the **decompressed** payload.
#[derive(Debug)]
pub struct PackObject {
    pub obj_type: ObjectType,
    pub hash: [u8; 32],
    /// Decompressed payload bytes.
    pub data: Vec<u8>,
}

// ─── PackWriter ──────────────────────────────────────────────────────────────

/// Streaming pack writer.
///
/// # Example
/// ```
/// # use axiom_core::sync::pack::{PackWriter, ObjectType};
/// let mut w = PackWriter::new();
/// let hash = [0u8; 32];
/// w.add_object(ObjectType::Chunk, &hash, b"hello world").unwrap();
/// let bytes = w.finish();
/// assert!(bytes.starts_with(b"AXPK"));
/// ```
pub struct PackWriter {
    /// Accumulated bytes: header (with placeholder count) + entries.
    body: Vec<u8>,
    object_count: u64,
}

impl PackWriter {
    /// Create a new writer. The header is pre-allocated with a placeholder
    /// object count that is patched to the real value in [`finish`].
    pub fn new() -> Self {
        let mut body = Vec::with_capacity(1 << 20); // 1 MiB initial capacity
        body.extend_from_slice(MAGIC);
        body.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        body.extend_from_slice(&0u64.to_le_bytes()); // placeholder; patched in finish()
        Self { body, object_count: 0 }
    }

    /// Append one object. `data` is the **raw** (uncompressed) payload; the
    /// writer compresses it with zstd level 3 before writing.
    pub fn add_object(
        &mut self,
        obj_type: ObjectType,
        hash: &[u8; 32],
        data: &[u8],
    ) -> CasResult<()> {
        let compressed = zstd::bulk::compress(data, 3)
            .map_err(|e| CasError::SyncError(format!("zstd compress: {e}")))?;

        self.body.push(obj_type as u8);
        self.body.extend_from_slice(hash);
        self.body.extend_from_slice(&(compressed.len() as u64).to_le_bytes());
        self.body.extend_from_slice(&compressed);
        self.object_count += 1;
        Ok(())
    }

    /// Finalise the pack: patch the object count in the header, append the
    /// BLAKE3 checksum over all bytes, and return the complete pack bytes.
    pub fn finish(mut self) -> Vec<u8> {
        // Patch object count at its fixed offset in the header.
        self.body[OBJECT_COUNT_OFFSET..OBJECT_COUNT_OFFSET + 8]
            .copy_from_slice(&self.object_count.to_le_bytes());

        // Append BLAKE3 checksum over everything written so far.
        let checksum = blake3::hash(&self.body);
        self.body.extend_from_slice(checksum.as_bytes());
        self.body
    }

    /// Number of objects added so far.
    pub fn len(&self) -> u64 {
        self.object_count
    }

    /// `true` if no objects have been added yet.
    pub fn is_empty(&self) -> bool {
        self.object_count == 0
    }
}

impl Default for PackWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ─── PackReader ──────────────────────────────────────────────────────────────

/// Streaming pack reader.
///
/// Validates the header magic, version, and BLAKE3 footer checksum at
/// construction time. Objects are then yielded one-by-one via
/// [`next_object`], which decompresses each entry on demand.
///
/// # Example
/// ```
/// # use axiom_core::sync::pack::{PackWriter, PackReader, ObjectType};
/// let mut w = PackWriter::new();
/// w.add_object(ObjectType::Version, &[1u8; 32], b"v1").unwrap();
/// let pack = w.finish();
///
/// let mut r = PackReader::new(&pack).unwrap();
/// let obj = r.next_object().unwrap().unwrap();
/// assert_eq!(obj.obj_type, ObjectType::Version);
/// assert_eq!(obj.data, b"v1");
/// ```
pub struct PackReader<'a> {
    data: &'a [u8],
    /// Current read position (starts at HEADER_SIZE).
    pos: usize,
    object_count: u64,
    objects_read: u64,
}

impl<'a> PackReader<'a> {
    /// Parse and validate header + footer checksum.
    ///
    /// Returns an error if the magic bytes are wrong, the version is
    /// unsupported, or the checksum does not match.
    pub fn new(data: &'a [u8]) -> CasResult<Self> {
        let min_len = HEADER_SIZE + CHECKSUM_SIZE;
        if data.len() < min_len {
            return Err(CasError::SyncError(format!(
                "pack too small: {} bytes (min {min_len})",
                data.len()
            )));
        }

        // Validate magic.
        if &data[..4] != MAGIC {
            return Err(CasError::SyncError("invalid AXPK magic".into()));
        }

        // Validate version.
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(CasError::SyncError(format!(
                "unsupported pack version: {version}"
            )));
        }

        // Validate BLAKE3 checksum (last 32 bytes cover all preceding bytes).
        let body_end = data.len() - CHECKSUM_SIZE;
        let expected = &data[body_end..];
        let actual = blake3::hash(&data[..body_end]);
        if actual.as_bytes() != expected {
            return Err(CasError::SyncError("pack checksum mismatch".into()));
        }

        let object_count = u64::from_le_bytes(
            data[OBJECT_COUNT_OFFSET..OBJECT_COUNT_OFFSET + 8]
                .try_into()
                .unwrap(),
        );

        Ok(Self { data, pos: HEADER_SIZE, object_count, objects_read: 0 })
    }

    /// Total number of objects declared in the pack header.
    pub fn object_count(&self) -> u64 {
        self.object_count
    }

    /// Yield the next object, decompressing its payload on demand.
    ///
    /// Returns `Ok(None)` when all objects have been consumed.
    pub fn next_object(&mut self) -> CasResult<Option<PackObject>> {
        if self.objects_read >= self.object_count {
            return Ok(None);
        }

        // Body (without checksum footer) is the valid data region.
        let body_end = self.data.len() - CHECKSUM_SIZE;

        if self.pos + ENTRY_PREFIX_SIZE > body_end {
            return Err(CasError::SyncError("truncated pack entry prefix".into()));
        }

        let obj_type = ObjectType::try_from(self.data[self.pos])?;
        let hash: [u8; 32] = self.data[self.pos + 1..self.pos + 33]
            .try_into()
            .unwrap();
        let compressed_len = u64::from_le_bytes(
            self.data[self.pos + 33..self.pos + 41].try_into().unwrap(),
        ) as usize;
        self.pos += ENTRY_PREFIX_SIZE;

        if self.pos + compressed_len > body_end {
            return Err(CasError::SyncError("pack entry data truncated".into()));
        }

        let compressed_slice = &self.data[self.pos..self.pos + compressed_len];
        let data = zstd::decode_all(Cursor::new(compressed_slice))
            .map_err(|e| CasError::SyncError(format!("zstd decompress: {e}")))?;

        self.pos += compressed_len;
        self.objects_read += 1;

        Ok(Some(PackObject { obj_type, hash, data }))
    }
}
