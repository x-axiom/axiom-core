//! Streaming upload endpoints.
//!
//! - `POST /file`      — single-file streaming upload (raw body)
//! - `POST /directory`  — multi-file upload (JSON with base64-encoded content)

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine;
use std::collections::HashMap;

use crate::api::dto::{
    DirectoryUploadRequest, FileUploadQuery, UploadResponse, UploadStats,
};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::chunker::{chunk_bytes, ChunkPolicy};
use crate::commit::{CommitRequest, CommitService, DEFAULT_BRANCH};
use crate::merkle::{build_tree, DEFAULT_FAN_OUT};
use crate::model::chunk::ChunkDescriptor;
use crate::model::VersionId;
use crate::namespace::{build_directory_tree, FileInput};
use crate::store::traits::{ChunkStore, RefRepo};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/file", post(upload_file))
        .route("/directory", post(upload_directory))
}

// ---------------------------------------------------------------------------
// Dedup-aware chunk persistence
// ---------------------------------------------------------------------------

/// Chunk and persist data, returning descriptors and dedup stats.
fn chunk_persist_with_stats(
    data: &[u8],
    policy: &ChunkPolicy,
    store: &impl ChunkStore,
) -> Result<(Vec<ChunkDescriptor>, DedupFileStats), ApiError> {
    let raw_chunks = chunk_bytes(data, policy);
    let mut descriptors = Vec::with_capacity(raw_chunks.len());
    let mut new_chunks: usize = 0;
    let mut dedup_chunks: usize = 0;
    let mut dedup_bytes: u64 = 0;

    for (order, (hash, chunk_data, offset, length)) in raw_chunks.into_iter().enumerate() {
        let already_exists = store.has_chunk(&hash).map_err(|e| ApiError(e))?;
        store.put_chunk(chunk_data).map_err(|e| ApiError(e))?;

        if already_exists {
            dedup_chunks += 1;
            dedup_bytes += length as u64;
        } else {
            new_chunks += 1;
        }

        descriptors.push(ChunkDescriptor {
            offset,
            length,
            hash,
            order: order as u32,
        });
    }

    let total_chunks = descriptors.len();

    Ok((
        descriptors,
        DedupFileStats {
            total_bytes: data.len() as u64,
            total_chunks,
            new_chunks,
            dedup_chunks,
            dedup_bytes,
        },
    ))
}

struct DedupFileStats {
    total_bytes: u64,
    total_chunks: usize,
    new_chunks: usize,
    dedup_chunks: usize,
    dedup_bytes: u64,
}

// ---------------------------------------------------------------------------
// POST /file — single-file streaming upload
// ---------------------------------------------------------------------------

async fn upload_file(
    State(state): State<AppState>,
    Query(query): Query<FileUploadQuery>,
    body: Bytes,
) -> ApiResult<Json<UploadResponse>> {
    let policy = ChunkPolicy::default();
    let branch = query.branch.as_deref().unwrap_or(DEFAULT_BRANCH);
    let message = query
        .message
        .unwrap_or_else(|| format!("upload {}", &query.path));
    let mut parents: Vec<VersionId> = query
        .parents
        .map(|p| {
            p.split(',')
                .filter(|s| !s.is_empty())
                .map(|s| VersionId::from(s.trim()))
                .collect()
        })
        .unwrap_or_default();

    // Auto-resolve branch head as parent if none provided.
    if parents.is_empty() {
        if let Some(r) = state.meta.get_ref(branch).map_err(ApiError::from)? {
            parents.push(r.target);
        }
    }

    // 1. Chunk and persist with dedup tracking.
    let (descriptors, file_stats) =
        chunk_persist_with_stats(&body, &policy, state.cas.as_ref())?;

    // 2. Build Merkle tree for this file.
    let file_obj = build_tree(&descriptors, DEFAULT_FAN_OUT, state.cas.as_ref())
        .map_err(ApiError::from)?;

    // 3. Build a single-file directory tree.
    //    CommitService needs a version_id for path indexing, but we don't have
    //    it yet. We'll use a temporary placeholder and re-index after commit.
    let file_input = FileInput {
        path: query.path.clone(),
        root: file_obj.root,
        size: file_obj.size,
    };

    // Create a temporary version id for path indexing — will be replaced by
    // the real one shortly. Use a deterministic placeholder.
    let tmp_vid = VersionId::from("__pending__");
    let root_entry = build_directory_tree(
        &[file_input],
        &tmp_vid,
        state.cas.as_ref(),
        state.meta.as_ref(),
    )
    .map_err(ApiError::from)?;

    // 4. Commit to branch.
    let svc = CommitService::new(state.meta.clone(), state.meta.clone());
    let version = svc
        .commit(CommitRequest {
            root: root_entry.hash,
            parents,
            message,
            metadata: HashMap::new(),
            branch: Some(branch.to_string()),
        })
        .map_err(ApiError::from)?;

    // 5. Re-index paths under the real version id.
    re_index_paths(
        &[FileInput {
            path: query.path,
            root: file_obj.root,
            size: file_obj.size,
        }],
        &version.id,
        state.cas.as_ref(),
        state.meta.as_ref(),
    )?;

    Ok(Json(UploadResponse {
        version_id: version.id.to_string(),
        root: root_entry.hash.to_hex().to_string(),
        branch: branch.to_string(),
        stats: UploadStats {
            total_files: 1,
            total_bytes: file_stats.total_bytes,
            total_chunks: file_stats.total_chunks,
            new_chunks: file_stats.new_chunks,
            dedup_chunks: file_stats.dedup_chunks,
            dedup_bytes: file_stats.dedup_bytes,
        },
    }))
}

// ---------------------------------------------------------------------------
// POST /directory — multi-file upload
// ---------------------------------------------------------------------------

async fn upload_directory(
    State(state): State<AppState>,
    Json(req): Json<DirectoryUploadRequest>,
) -> ApiResult<Json<UploadResponse>> {
    if req.files.is_empty() {
        return Err(ApiError(crate::error::CasError::InvalidObject(
            "file list must not be empty".into(),
        )));
    }

    let policy = ChunkPolicy::default();
    let branch = req.branch.as_deref().unwrap_or(DEFAULT_BRANCH);
    let message = req.message.unwrap_or_else(|| {
        format!("upload {} file(s)", req.files.len())
    });
    let mut parents: Vec<VersionId> = req.parents.into_iter().map(VersionId::from).collect();

    // Auto-resolve branch head as parent if none provided.
    if parents.is_empty() {
        if let Some(r) = state.meta.get_ref(branch).map_err(ApiError::from)? {
            parents.push(r.target);
        }
    }

    let engine = base64::engine::general_purpose::STANDARD;

    let mut file_inputs: Vec<FileInput> = Vec::with_capacity(req.files.len());
    let mut total_bytes: u64 = 0;
    let mut total_chunks: usize = 0;
    let mut new_chunks: usize = 0;
    let mut dedup_chunks: usize = 0;
    let mut dedup_bytes: u64 = 0;

    for entry in &req.files {
        // Decode base64 content.
        let data = engine.decode(&entry.content_base64).map_err(|e| {
            ApiError(crate::error::CasError::InvalidObject(format!(
                "invalid base64 for '{}': {e}",
                entry.path
            )))
        })?;

        // Chunk and persist.
        let (descriptors, stats) =
            chunk_persist_with_stats(&data, &policy, state.cas.as_ref())?;

        // Build Merkle tree.
        let file_obj = build_tree(&descriptors, DEFAULT_FAN_OUT, state.cas.as_ref())
            .map_err(ApiError::from)?;

        file_inputs.push(FileInput {
            path: entry.path.clone(),
            root: file_obj.root,
            size: file_obj.size,
        });

        total_bytes += stats.total_bytes;
        total_chunks += stats.total_chunks;
        new_chunks += stats.new_chunks;
        dedup_chunks += stats.dedup_chunks;
        dedup_bytes += stats.dedup_bytes;
    }

    // Build directory tree with temporary version id.
    let tmp_vid = VersionId::from("__pending__");
    let root_entry = build_directory_tree(
        &file_inputs,
        &tmp_vid,
        state.cas.as_ref(),
        state.meta.as_ref(),
    )
    .map_err(ApiError::from)?;

    // Commit.
    let svc = CommitService::new(state.meta.clone(), state.meta.clone());
    let version = svc
        .commit(CommitRequest {
            root: root_entry.hash,
            parents,
            message,
            metadata: HashMap::new(),
            branch: Some(branch.to_string()),
        })
        .map_err(ApiError::from)?;

    // Re-index paths under real version id.
    re_index_paths(
        &file_inputs,
        &version.id,
        state.cas.as_ref(),
        state.meta.as_ref(),
    )?;

    Ok(Json(UploadResponse {
        version_id: version.id.to_string(),
        root: root_entry.hash.to_hex().to_string(),
        branch: branch.to_string(),
        stats: UploadStats {
            total_files: file_inputs.len(),
            total_bytes,
            total_chunks,
            new_chunks,
            dedup_chunks,
            dedup_bytes,
        },
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Re-index file paths under the real version id (the initial build used a
/// placeholder because version id wasn't known yet).
fn re_index_paths(
    files: &[FileInput],
    version_id: &VersionId,
    node_store: &impl crate::store::traits::NodeStore,
    path_index: &impl crate::store::traits::PathIndexRepo,
) -> ApiResult<()> {
    // Rebuild the directory tree — the node_store writes are idempotent, so
    // the only new work is the path_index entries for the real version id.
    build_directory_tree(files, version_id, node_store, path_index)
        .map_err(ApiError::from)?;
    Ok(())
}
