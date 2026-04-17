//! Request and response DTOs for the HTTP API.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct ObjectInfoResponse {
    pub hash: String,
    pub size: u64,
    pub exists: bool,
}

// ---------------------------------------------------------------------------
// Versions
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateVersionRequest {
    pub root: String,
    pub parents: Vec<String>,
    pub message: String,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
pub struct VersionResponse {
    pub id: String,
    pub root: String,
    pub parents: Vec<String>,
    pub message: String,
    pub timestamp: u64,
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
pub struct VersionListResponse {
    pub versions: Vec<VersionResponse>,
}

// ---------------------------------------------------------------------------
// Refs
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateRefRequest {
    pub name: String,
    pub kind: String,
    pub target: String,
}

#[derive(Serialize)]
pub struct RefResponse {
    pub name: String,
    pub kind: String,
    pub target: String,
}

#[derive(Serialize)]
pub struct RefListResponse {
    pub refs: Vec<RefResponse>,
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct DiffRequest {
    pub old_version: String,
    pub new_version: String,
}

#[derive(Serialize)]
pub struct DiffEntryResponse {
    pub path: String,
    pub kind: String,
    pub old_hash: Option<String>,
    pub new_hash: Option<String>,
}

#[derive(Serialize)]
pub struct DiffResponse {
    pub entries: Vec<DiffEntryResponse>,
    pub added_files: usize,
    pub removed_files: usize,
    pub modified_files: usize,
    pub added_chunks: usize,
    pub removed_chunks: usize,
    pub unchanged_chunks: usize,
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

/// Query parameters for single-file streaming upload.
#[derive(Deserialize)]
pub struct FileUploadQuery {
    /// Relative path for the file (e.g. "src/main.rs").
    pub path: String,
    /// Branch to commit to. Defaults to "main".
    #[serde(default)]
    pub branch: Option<String>,
    /// Commit message.
    #[serde(default)]
    pub message: Option<String>,
    /// Parent version IDs (comma-separated).
    #[serde(default)]
    pub parents: Option<String>,
}

/// A file entry in a directory upload request.
#[derive(Deserialize)]
pub struct DirectoryFileEntry {
    /// Relative path (e.g. "src/main.rs").
    pub path: String,
    /// File content, base64-encoded.
    pub content_base64: String,
}

/// Request body for directory (multi-file) upload.
#[derive(Deserialize)]
pub struct DirectoryUploadRequest {
    /// Files to include in this version.
    pub files: Vec<DirectoryFileEntry>,
    /// Branch to commit to. Defaults to "main".
    #[serde(default)]
    pub branch: Option<String>,
    /// Commit message.
    #[serde(default)]
    pub message: Option<String>,
    /// Parent version IDs.
    #[serde(default)]
    pub parents: Vec<String>,
}

/// Deduplication and ingestion statistics.
#[derive(Serialize)]
pub struct UploadStats {
    pub total_files: usize,
    pub total_bytes: u64,
    pub total_chunks: usize,
    /// Chunks that were already present in CAS.
    pub dedup_chunks: usize,
    /// Chunks newly written to CAS.
    pub new_chunks: usize,
    /// Bytes saved by deduplication.
    pub dedup_bytes: u64,
}

/// Response for both file and directory upload.
#[derive(Serialize)]
pub struct UploadResponse {
    pub version_id: String,
    pub root: String,
    pub branch: String,
    pub stats: UploadStats,
}

// ---------------------------------------------------------------------------
// Download / Directory listing
// ---------------------------------------------------------------------------

/// A child entry in a directory listing.
#[derive(Serialize)]
pub struct DirChildEntry {
    pub name: String,
    pub is_directory: bool,
    /// Size in bytes (files only, 0 for directories).
    pub size: u64,
    /// Content hash hex.
    pub hash: String,
}

/// Response for a directory listing request.
#[derive(Serialize)]
pub struct DirListingResponse {
    pub version_id: String,
    pub path: String,
    pub entries: Vec<DirChildEntry>,
}
