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

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/version/{ref}/file/{*path}", get(download_file))
        .route("/version/{ref}/ls", get(list_root))
        .route("/version/{ref}/ls/{*path}", get(list_dir))
}

// ---------------------------------------------------------------------------
// GET /version/:ref/file/*path — streaming file download
// ---------------------------------------------------------------------------

async fn download_file(
    State(state): State<AppState>,
    Path((ref_str, file_path)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    download_file_response(&state, &ref_str, &file_path)
}

pub fn download_file_response(
    state: &AppState,
    ref_str: &str,
    file_path: &str,
) -> Result<Response, ApiError> {
    let v = super::helpers::resolve_version_node(ref_str, state.versions.as_ref(), state.refs.as_ref())?;
    let version_id = v.id;

    // Look up path in the path index.
    let entry = state
        .path_index
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
        .nodes
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
        rehydrate(&merkle_root, state.trees.as_ref()).map_err(ApiError::from)?;

    // Build a streaming response by reading chunks in order.
    let chunks = state.chunks.clone();
    let chunk_stream = stream::iter(chunk_hashes.into_iter().map(move |hash| {
        chunks.get_chunk(&hash)
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
    list_directory_service(state, ref_str, dir_path).map(Json)
}

/// Synchronous service for listing a directory at `(ref, path)`.
///
/// Used by both the HTTP handler and the Tauri `browse_directory` IPC
/// command. Pass an empty `dir_path` to list the version root.
pub fn list_directory_service(
    state: &AppState,
    ref_str: &str,
    dir_path: &str,
) -> ApiResult<DirListingResponse> {
    let (version_id, _root) = {
        let v = super::helpers::resolve_version_node(ref_str, state.versions.as_ref(), state.refs.as_ref())?;
        (v.id, v.root)
    };

    // Verify the path is actually a directory (or root "").
    if !dir_path.is_empty() {
        let entry = state
            .path_index
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
        .path_index
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
                    .nodes
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

    Ok(DirListingResponse {
        version_id: version_id.to_string(),
        path: dir_path.to_string(),
        entries,
    })
}

/// Synchronous service that reassembles a file's full byte content.
///
/// Walks the path index → node → Merkle tree → chunks, returning the
/// concatenated bytes. Used by the Tauri `download_file` IPC command;
/// the HTTP handler streams chunks directly instead of buffering.
pub fn read_file_service(
    state: &AppState,
    ref_str: &str,
    file_path: &str,
) -> ApiResult<Vec<u8>> {
    let v = super::helpers::resolve_version_node(ref_str, state.versions.as_ref(), state.refs.as_ref())?;
    let version_id = v.id;

    let entry = state
        .path_index
        .get_by_path(&version_id, file_path)
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

    let node = state
        .nodes
        .get_node(&entry.node_hash)
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(CasError::NotFound(format!(
                "node '{}' not found",
                entry.node_hash.to_hex()
            )))
        })?;

    let merkle_root = match &node.kind {
        NodeKind::File { root, .. } => *root,
        NodeKind::Directory { .. } => {
            return Err(ApiError(CasError::InvalidObject(
                "expected file node".into(),
            )));
        }
    };

    let chunk_hashes =
        rehydrate(&merkle_root, state.trees.as_ref()).map_err(ApiError::from)?;

    let mut bytes = Vec::new();
    for hash in chunk_hashes {
        let chunk = state
            .chunks
            .get_chunk(&hash)
            .map_err(ApiError::from)?
            .ok_or_else(|| {
                ApiError(CasError::NotFound(format!(
                    "chunk {} missing",
                    hash.to_hex()
                )))
            })?;
        bytes.extend_from_slice(&chunk);
    }

    Ok(bytes)
}
