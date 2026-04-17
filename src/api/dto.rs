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
