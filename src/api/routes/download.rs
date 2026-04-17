//! Download and directory listing endpoints.
//!
//! - `GET /version/:ref/file/*path`  — stream a file's content
//! - `GET /version/:ref/ls/*path`    — list directory entries
//! - `GET /version/:ref/ls`          — list root directory

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::stream;

use crate::api::dto::{DirChildEntry, DirListingResponse};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::error::CasError;
use crate::merkle::rehydrate;
use crate::model::node::NodeKind;
use crate::model::VersionId;
use crate::store::traits::{ChunkStore, NodeStore, PathIndexRepo, RefRepo, VersionRepo};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/version/{ref}/file/{*path}", get(download_file))
        .route("/version/{ref}/ls", get(list_root))
        .route("/version/{ref}/ls/{*path}", get(list_dir))
}

// ---------------------------------------------------------------------------
// Version resolution: id / branch / tag
// ---------------------------------------------------------------------------

/// Resolve a ref string to a (version_id, root_hash) pair.
///
/// Tries in order: exact version id → branch → tag.
fn resolve_version(
    ref_str: &str,
    meta: &(impl VersionRepo + RefRepo),
) -> Result<(VersionId, blake3::Hash), ApiError> {
    // 1. Try as literal version id.
    let vid = VersionId::from(ref_str);
    if let Some(v) = meta.get_version(&vid).map_err(ApiError::from)? {
        return Ok((v.id, v.root));
    }

    // 2. Try as ref (branch or tag).
    if let Some(r) = meta.get_ref(ref_str).map_err(ApiError::from)? {
        let v = meta
            .get_version(&r.target)
            .map_err(ApiError::from)?
            .ok_or_else(|| {
                ApiError(CasError::NotFound(format!(
                    "version '{}' (target of ref '{}') not found",
                    r.target, ref_str
                )))
            })?;
        return Ok((v.id, v.root));
    }

    Err(ApiError(CasError::NotFound(format!(
        "version or ref '{ref_str}' not found"
    ))))
}

// ---------------------------------------------------------------------------
// GET /version/:ref/file/*path — streaming file download
// ---------------------------------------------------------------------------

async fn download_file(
    State(state): State<AppState>,
    Path((ref_str, file_path)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let (version_id, _root) = resolve_version(&ref_str, state.meta.as_ref())?;

    // Look up path in the path index.
    let entry = state
        .meta
        .get_by_path(&version_id, &file_path)
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(CasError::NotFound(format!(
                "path '{}' not found in version '{}'",
                file_path, version_id
            )))
        })?;

    if entry.is_directory {
        return Err(ApiError(CasError::InvalidObject(format!(
            "path '{}' is a directory, not a file",
            file_path
        ))));
    }

    // Get the node to find the Merkle tree root.
    let node = state
        .cas
        .get_node(&entry.node_hash)
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(CasError::NotFound(format!(
                "node '{}' not found",
                entry.node_hash.to_hex()
            )))
        })?;

    let (merkle_root, file_size) = match &node.kind {
        NodeKind::File { root, size } => (*root, *size),
        NodeKind::Directory { .. } => {
            return Err(ApiError(CasError::InvalidObject(
                "expected file node".into(),
            )));
        }
    };

    // Rehydrate the Merkle tree to get ordered chunk hashes.
    let chunk_hashes =
        rehydrate(&merkle_root, state.cas.as_ref()).map_err(ApiError::from)?;

    // Build a streaming response by reading chunks in order.
    let cas = state.cas.clone();
    let chunk_stream = stream::iter(chunk_hashes.into_iter().map(move |hash| {
        cas.get_chunk(&hash)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            .and_then(|opt| {
                opt.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("chunk {} missing", hash.to_hex()),
                    )
                })
            })
    }));

    // Derive a filename from the last path component.
    let filename = file_path
        .rsplit('/')
        .next()
        .unwrap_or(&file_path);

    let body = Body::from_stream(chunk_stream);

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .header(header::CONTENT_LENGTH, file_size)
        .body(body)
        .unwrap())
}

// ---------------------------------------------------------------------------
// GET /version/:ref/ls — list root directory
// ---------------------------------------------------------------------------

async fn list_root(
    State(state): State<AppState>,
    Path(ref_str): Path<String>,
) -> ApiResult<Json<DirListingResponse>> {
    list_dir_impl(&state, &ref_str, "").await
}

// ---------------------------------------------------------------------------
// GET /version/:ref/ls/*path — list subdirectory
// ---------------------------------------------------------------------------

async fn list_dir(
    State(state): State<AppState>,
    Path((ref_str, dir_path)): Path<(String, String)>,
) -> ApiResult<Json<DirListingResponse>> {
    list_dir_impl(&state, &ref_str, &dir_path).await
}

async fn list_dir_impl(
    state: &AppState,
    ref_str: &str,
    dir_path: &str,
) -> ApiResult<Json<DirListingResponse>> {
    let (version_id, _root) = resolve_version(ref_str, state.meta.as_ref())?;

    // Verify the path is actually a directory (or root "").
    if !dir_path.is_empty() {
        let entry = state
            .meta
            .get_by_path(&version_id, dir_path)
            .map_err(ApiError::from)?
            .ok_or_else(|| {
                ApiError(CasError::NotFound(format!(
                    "path '{}' not found in version '{}'",
                    dir_path, version_id
                )))
            })?;

        if !entry.is_directory {
            return Err(ApiError(CasError::InvalidObject(format!(
                "path '{}' is a file, not a directory",
                dir_path
            ))));
        }
    }

    // List children from path index.
    let children = state
        .meta
        .list_directory(&version_id, dir_path)
        .map_err(ApiError::from)?;

    let entries: Vec<DirChildEntry> = children
        .into_iter()
        .map(|pe| {
            // Extract leaf name from full path.
            let name = pe
                .path
                .rsplit('/')
                .next()
                .unwrap_or(&pe.path)
                .to_string();

            let size = if pe.is_directory {
                0
            } else {
                // Try to get file size from node.
                state
                    .cas
                    .get_node(&pe.node_hash)
                    .ok()
                    .flatten()
                    .and_then(|n| match n.kind {
                        NodeKind::File { size, .. } => Some(size),
                        _ => None,
                    })
                    .unwrap_or(0)
            };

            DirChildEntry {
                name,
                is_directory: pe.is_directory,
                size,
                hash: pe.node_hash.to_hex().to_string(),
            }
        })
        .collect();

    Ok(Json(DirListingResponse {
        version_id: version_id.to_string(),
        path: dir_path.to_string(),
        entries,
    }))
}
