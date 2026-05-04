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

/// Paginated version history response.
#[derive(Serialize)]
pub struct PaginatedVersionListResponse {
    pub versions: Vec<VersionResponse>,
    pub offset: usize,
    pub limit: usize,
    pub has_more: bool,
}

/// Node metadata for a path within a version.
#[derive(Serialize)]
pub struct NodeMetadataResponse {
    pub version_id: String,
    pub path: String,
    pub hash: String,
    pub is_directory: bool,
    /// File size in bytes (0 for directories).
    pub size: u64,
    /// Child names (directories only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<String>>,
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

// ---------------------------------------------------------------------------
// Workspace contract (E09 Web Console)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateWorkspaceHttpRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Serialize)]
pub struct WorkspaceSummaryResponse {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize)]
pub struct WorkspaceTreeEntryResponse {
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Serialize)]
pub struct WorkspaceVersionResponse {
    pub id: String,
    pub parent_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    pub timestamp: String,
}

#[derive(Serialize)]
pub struct WorkspaceVersionPageResponse {
    pub items: Vec<WorkspaceVersionResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize)]
pub struct WorkspaceDiffEntryResponse {
    pub path: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_size_bytes: Option<u64>,
}
