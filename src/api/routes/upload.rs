//! Streaming upload endpoints.
//!
//! - `POST /file`      — single-file streaming upload (raw body)
//! - `POST /directory`  — multi-file upload (JSON with base64-encoded content)

use axum::extract::{Extension, Query, State};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine;
use std::collections::HashMap;

use crate::api::dto::{
    DirectoryUploadRequest, FileUploadQuery, UploadResponse, UploadStats,
};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::auth::rbac::{AuthContext, PlanContext};
use crate::chunker::{chunk_bytes, ChunkPolicy};
use crate::commit::{CommitRequest, CommitService, DEFAULT_BRANCH};
use crate::merkle::{build_tree, DEFAULT_FAN_OUT};
use crate::model::chunk::ChunkDescriptor;
use crate::model::VersionId;
use crate::namespace::{build_directory_tree, FileInput};

pub fn router() -> Router<AppState> {
    Router::new()
        // Raw streaming body: the 2 MiB extractor default is disabled and the
        // size ceiling is enforced inside the handler (structured 413).
        .route("/file", post(upload_file))
        .layer(axum::extract::DefaultBodyLimit::disable())
        .merge(
            Router::new()
                .route("/directory", post(upload_directory))
                .layer(axum::extract::DefaultBodyLimit::max(32 * 1024 * 1024)),
        )
}

// ---------------------------------------------------------------------------
// Dedup-aware chunk persistence
// ---------------------------------------------------------------------------

/// Chunk and persist data, returning descriptors and dedup stats.
fn chunk_persist_with_stats(
    data: &[u8],
    policy: &ChunkPolicy,
    store: &dyn crate::store::traits::ChunkStore,
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
            new_bytes: (data.len() as u64).saturating_sub(dedup_bytes),
            new_chunks,
            dedup_chunks,
            dedup_bytes,
        },
    ))
}

#[derive(Default)]
struct DedupFileStats {
    total_bytes: u64,
    total_chunks: usize,
    new_bytes: u64,
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
    auth: Option<Extension<AuthContext>>,
    plan: Option<Extension<PlanContext>>,
    request: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let max_bytes = max_upload_bytes();

    let tenant_id = auth.as_ref().map(|Extension(ctx)| ctx.tenant_id.clone());
    let plan_name = plan.as_ref().and_then(|Extension(p)| p.0.clone());
    let plan_ref = plan_name.as_deref();

    // Quota is resolved once per request: remaining budget is enforced while
    // streaming, so an over-quota upload stops early instead of filling S3.
    let (quota_bytes, used_bytes, budget) = match tenant_id.as_deref() {
        Some(tenant) => {
            let quota = state.quota.limit_for(plan_ref);
            let used = match state.usage.as_ref() {
                Some(repo) => repo.get(tenant).unwrap_or(0),
                None => 0,
            };
            (Some(quota), used, Some(quota.saturating_sub(used)))
        }
        None => (None, 0, None),
    };

    let (descriptors, file_stats) = match ingest_stream(
        &state,
        request.into_body(),
        max_bytes,
        budget,
        used_bytes,
        quota_bytes,
    )
    .await
    {
        Ok(value) => value,
        Err(IngestError::TooLarge) => return payload_too_large(max_bytes),
        Err(IngestError::QuotaExceeded { used, quota }) => {
            return quota_exceeded(used, quota, plan_ref);
        }
        Err(IngestError::Api(err)) => return err.into_response(),
    };

    let new_bytes = file_stats.new_bytes;
    let query_path_for_audit = query.path.clone();
    let committed = match finalize_single_upload(&state, query, descriptors, file_stats) {
        Ok(json) => json,
        Err(err) => return err.into_response(),
    };

    // Charge only after the commit succeeded: aborted uploads leave orphan
    // chunks for GC to reclaim, but must never consume quota.
    if let (Some(tenant), Some(repo)) = (tenant_id.as_deref(), state.usage.as_ref()) {
        if new_bytes > 0 {
            if let Err(err) = repo.add(tenant, new_bytes) {
                tracing::error!(error = %err, tenant, "failed to record usage");
            }
        }
    }

    if let Some(Extension(ctx)) = auth.as_ref() {
        state.record_audit(
            &ctx.user_id,
            &ctx.tenant_id,
            "object.upload",
            &format!("{} ({} bytes)", query_path_for_audit, new_bytes),
            "ok",
        );
    }

    committed.into_response()
}

fn quota_exceeded(used: u64, quota: u64, plan: Option<&str>) -> axum::response::Response {
    use axum::response::IntoResponse;

    crate::telemetry::record_quota_rejection(plan);
    (
        axum::http::StatusCode::PAYMENT_REQUIRED,
        Json(serde_json::json!({
            "error": "quota_exceeded",
            "code": "quota_exceeded",
            "message": format!(
                "storage quota exceeded: {used} of {quota} bytes used on the {} plan",
                crate::quota::QuotaPolicy::plan_name(plan)
            ),
            "used_bytes": used,
            "quota_bytes": quota,
            "plan": crate::quota::QuotaPolicy::plan_name(plan),
            "request_id": crate::telemetry::current_request_id(),
            "retryable": false,
        })),
    )
        .into_response()
}

/// Configurable upload ceiling (default 8 GiB).
///
/// Enforced while streaming, so an oversized body is rejected as soon as the
/// limit is crossed instead of being buffered first.
fn max_upload_bytes() -> u64 {
    std::env::var("AXIOM_MAX_UPLOAD_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

fn payload_too_large(max_bytes: u64) -> axum::response::Response {
    use axum::response::IntoResponse;

    (
        axum::http::StatusCode::PAYLOAD_TOO_LARGE,
        Json(serde_json::json!({
            "error": "payload_too_large",
            "code": "payload_too_large",
            "message": format!("request body exceeds the configured limit of {max_bytes} bytes"),
            "max_bytes": max_bytes,
            "request_id": crate::telemetry::current_request_id(),
            "retryable": false,
        })),
    )
        .into_response()
}

enum IngestError {
    TooLarge,
    QuotaExceeded { used: u64, quota: u64 },
    Api(ApiError),
}

/// Stream the request body, chunk it incrementally, and persist chunks.
///
/// Memory use is bounded by `ChunkPolicy::max_size` (256 KiB) plus one
/// transport frame, so upload size no longer determines RSS.
async fn ingest_stream(
    state: &AppState,
    body: axum::body::Body,
    max_bytes: u64,
    quota_budget: Option<u64>,
    used_bytes: u64,
    quota_bytes: Option<u64>,
) -> Result<(Vec<ChunkDescriptor>, DedupFileStats), IngestError> {
    use futures_util::StreamExt;

    let policy = ChunkPolicy::default();
    let flush_at = policy.max_size as usize;

    let mut buffer: Vec<u8> = Vec::with_capacity(flush_at * 2);
    let mut descriptors: Vec<ChunkDescriptor> = Vec::new();
    let mut stats = DedupFileStats::default();
    let mut offset: u64 = 0;
    let mut stream = body.into_data_stream();

    while let Some(frame) = stream.next().await {
        let bytes = frame.map_err(|e| {
            IngestError::Api(ApiError(crate::error::CasError::InvalidObject(format!(
                "failed to read request body: {e}"
            ))))
        })?;

        if stats.total_bytes.saturating_add(bytes.len() as u64) > max_bytes {
            return Err(IngestError::TooLarge);
        }
        if let (Some(budget), Some(quota)) = (quota_budget, quota_bytes) {
            if stats.new_bytes > budget {
                return Err(IngestError::QuotaExceeded { used: used_bytes, quota });
            }
        }
        stats.total_bytes += bytes.len() as u64;
        buffer.extend_from_slice(&bytes);

        // Flush full windows. Content-defined boundaries are computed within
        // each window; because the window equals `max_size`, boundaries match
        // a whole-file chunking pass.
        while buffer.len() >= flush_at {
            let window: Vec<u8> = buffer.drain(..flush_at).collect();
            persist_chunks(
                state,
                &window,
                offset,
                &policy,
                &mut descriptors,
                &mut stats,
                quota_budget,
                used_bytes,
                quota_bytes,
            )
            .await?;
            offset += window.len() as u64;
        }
    }

    if !buffer.is_empty() {
        let tail = std::mem::take(&mut buffer);
        let len = tail.len() as u64;
        persist_chunks(
            state,
            &tail,
            offset,
            &policy,
            &mut descriptors,
            &mut stats,
            quota_budget,
            used_bytes,
            quota_bytes,
        )
        .await?;
        offset += len;
    }
    let _ = offset;

    Ok((descriptors, stats))
}

#[allow(clippy::too_many_arguments)]
async fn persist_chunks(
    state: &AppState,
    data: &[u8],
    base_offset: u64,
    policy: &ChunkPolicy,
    descriptors: &mut Vec<ChunkDescriptor>,
    stats: &mut DedupFileStats,
    quota_budget: Option<u64>,
    used_bytes: u64,
    quota_bytes: Option<u64>,
) -> Result<(), IngestError> {
    let raw = chunk_bytes(data, policy);

    for (_hash, chunk, offset, length) in raw {
        let order = descriptors.len() as u32;
        let hash = _hash;
        let owned = chunk.to_vec();

        // Step 1: dedup probe (blocking).
        let probe = state.chunks.clone();
        let hash_for_probe = hash.clone();
        let already_exists = tokio::task::spawn_blocking(move || probe.has_chunk(&hash_for_probe))
            .await
            .map_err(|e| {
                IngestError::Api(ApiError(crate::error::CasError::Store(format!(
                    "chunk worker failed: {e}"
                ))))
            })?
            .map_err(|e| IngestError::Api(ApiError(e)))?;

        if already_exists {
            stats.dedup_chunks += 1;
            stats.dedup_bytes += length as u64;
        } else {
            // Step 2: quota gate before the bytes touch storage.
            if let (Some(budget), Some(quota)) = (quota_budget, quota_bytes) {
                if stats.new_bytes.saturating_add(length as u64) > budget {
                    return Err(IngestError::QuotaExceeded { used: used_bytes, quota });
                }
            }

            // Step 3: persist (blocking).
            let store = state.chunks.clone();
            let hash_for_store = hash.clone();
            tokio::task::spawn_blocking(move || store.put_chunk(owned))
                .await
                .map_err(|e| {
                    IngestError::Api(ApiError(crate::error::CasError::Store(format!(
                        "chunk worker failed: {e}"
                    ))))
                })?
                .map_err(|e| IngestError::Api(ApiError(e)))?;

            crate::telemetry::record_storage_operation("chunk_store", "put", "ok");
            stats.new_chunks += 1;
            stats.new_bytes += length as u64;
        }

        descriptors.push(ChunkDescriptor {
            offset: base_offset + offset,
            length,
            hash,
            order,
        });
    }

    Ok(())
}

/// Build the Merkle tree, commit the version, and re-index paths.
fn finalize_single_upload(
    state: &AppState,
    query: FileUploadQuery,
    descriptors: Vec<ChunkDescriptor>,
    file_stats: DedupFileStats,
) -> ApiResult<Json<UploadResponse>> {
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
        if let Some(r) = state.refs.get_ref(branch).map_err(ApiError::from)? {
            parents.push(r.target);
        }
    }

    // 1. Build Merkle tree for this file.
    let file_obj = build_tree(&descriptors, DEFAULT_FAN_OUT, state.trees.as_ref())
        .map_err(ApiError::from)?;

    // 2. Build a single-file directory tree under a temporary version id.
    let file_input = FileInput {
        path: query.path.clone(),
        root: file_obj.root,
        size: file_obj.size,
    };

    let tmp_vid = VersionId::from("__pending__");
    let root_entry = build_directory_tree(
        &[file_input],
        &tmp_vid,
        state.nodes.as_ref(),
        state.path_index.as_ref(),
    )
    .map_err(ApiError::from)?;

    // 3. Commit to branch.
    let svc = CommitService::new(state.versions.clone(), state.refs.clone());
    let version = svc
        .commit(CommitRequest {
            root: root_entry.hash,
            parents,
            message,
            metadata: HashMap::new(),
            branch: Some(branch.to_string()),
        })
        .map_err(ApiError::from)?;

    // 4. Re-index paths under the real version id.
    re_index_paths(
        &[FileInput {
            path: query.path,
            root: file_obj.root,
            size: file_obj.size,
        }],
        &version.id,
        state.nodes.as_ref(),
        state.path_index.as_ref(),
    )?;

    let total_chunks = descriptors.len();
    Ok(Json(UploadResponse {
        version_id: version.id.to_string(),
        root: root_entry.hash.to_hex().to_string(),
        branch: branch.to_string(),
        stats: UploadStats {
            total_files: 1,
            total_bytes: file_stats.total_bytes,
            total_chunks,
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
    upload_directory_service(&state, req).map(Json)
}

/// Synchronous service backing `POST /directory`.
///
/// Exposed so non-HTTP callers (Tauri IPC, internal pipelines) can run the
/// same chunk → tree → commit pipeline without going through axum.
pub fn upload_directory_service(
    state: &AppState,
    req: DirectoryUploadRequest,
) -> ApiResult<UploadResponse> {
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
        if let Some(r) = state.refs.get_ref(branch).map_err(ApiError::from)? {
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
            chunk_persist_with_stats(&data, &policy, state.chunks.as_ref())?;

        // Build Merkle tree.
        let file_obj = build_tree(&descriptors, DEFAULT_FAN_OUT, state.trees.as_ref())
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
        state.nodes.as_ref(),
        state.path_index.as_ref(),
    )
    .map_err(ApiError::from)?;

    // Commit.
    let svc = CommitService::new(state.versions.clone(), state.refs.clone());
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
        state.nodes.as_ref(),
        state.path_index.as_ref(),
    )?;

    Ok(UploadResponse {
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
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Re-index file paths under the real version id (the initial build used a
/// placeholder because version id wasn't known yet).
fn re_index_paths(
    files: &[FileInput],
    version_id: &VersionId,
    node_store: &dyn crate::store::traits::NodeStore,
    path_index: &dyn crate::store::traits::PathIndexRepo,
) -> ApiResult<()> {
    // Rebuild the directory tree — the node_store writes are idempotent, so
    // the only new work is the path_index entries for the real version id.
    build_directory_tree(files, version_id, node_store, path_index)
        .map_err(ApiError::from)?;
    Ok(())
}
