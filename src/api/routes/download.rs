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
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let range = headers
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok());
    download_file_response_with_range(&state, &ref_str, &file_path, range)
}

pub fn download_file_response(
    state: &AppState,
    ref_str: &str,
    file_path: &str,
) -> Result<Response, ApiError> {
    download_file_response_with_range(state, ref_str, file_path, None)
}

/// Parse a single `Range: bytes=...` header against a known size.
///
/// Returns `Ok(None)` for "serve the whole body", `Err(())` when the range is
/// unsatisfiable or malformed (caller replies 416).
fn parse_range(header: &str, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let spec = header.trim().strip_prefix("bytes=").ok_or(())?;
    if spec.contains(',') {
        // Multi-range is not supported; serving the full body is legal.
        return Ok(None);
    }
    let (start_s, end_s) = spec.split_once('-').ok_or(())?;

    if start_s.is_empty() {
        // Suffix range: last N bytes.
        let n: u64 = end_s.trim().parse().map_err(|_| ())?;
        if n == 0 || size == 0 {
            return Err(());
        }
        let start = size.saturating_sub(n);
        return Ok(Some((start, size - 1)));
    }

    let start: u64 = start_s.trim().parse().map_err(|_| ())?;
    if start >= size {
        return Err(());
    }
    let end = if end_s.trim().is_empty() {
        size - 1
    } else {
        end_s.trim().parse::<u64>().map_err(|_| ())?.min(size - 1)
    };
    if end < start {
        return Err(());
    }
    Ok(Some((start, end)))
}

/// Sanitize a filename for `Content-Disposition` and add an RFC 5987 form for
/// non-ASCII names. Never allows CR/LF or unescaped quotes into the header.
fn content_disposition(filename: &str) -> String {
    let safe: String = filename
        .chars()
        .map(|c| match c {
            '"' | '\\' | '\r' | '\n' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();

    let mut encoded = String::new();
    for byte in safe.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(*byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    format!(
        "attachment; filename=\"{safe}\"; filename*=UTF-8''{encoded}"
    )
}

pub fn download_file_response_with_range(
    state: &AppState,
    ref_str: &str,
    file_path: &str,
    range_header: Option<&str>,
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

    // Resolve the requested byte window (P1-12).
    let range = match range_header {
        Some(value) => match parse_range(value, file_size) {
            Ok(range) => range,
            Err(()) => {
                return Ok(Response::builder()
                    .status(axum::http::StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, format!("bytes */{file_size}"))
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(Body::empty())
                    .map_err(|e| ApiError(CasError::Store(e.to_string())))?);
            }
        },
        None => None,
    };

    let queue: std::sync::Arc<parking_lot::Mutex<std::collections::VecDeque<_>>> =
        std::sync::Arc::new(parking_lot::Mutex::new(chunk_hashes.into_iter().collect()));

    let (start, end) = range.unwrap_or((0, file_size.saturating_sub(1)));
    let requested = if file_size == 0 { 0 } else { end - start + 1 };

    let chunks = state.chunks.clone();
    let stream_start = start;
    let stream_end = end;
    let offset = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Each chunk is fetched on the blocking pool; byte offsets are tracked in
    // the stream state so ranges can be sliced without buffering the file.
    let chunk_stream = stream::unfold(
        (queue, offset, chunks),
        move |(queue, offset, chunks)| async move {
            let hash = { queue.lock().pop_front() }?;

            let store = chunks.clone();
            let fetched = tokio::task::spawn_blocking(move || store.get_chunk(&hash))
                .await
                .ok()?;

            let bytes = match fetched {
                Ok(Some(bytes)) => bytes,
                Ok(None) => {
                    return Some((
                        Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "chunk missing",
                        )),
                        (queue, offset, chunks),
                    ));
                }
                Err(e) => {
                    return Some((
                        Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())),
                        (queue, offset, chunks),
                    ));
                }
            };

            let chunk_start = offset.load(std::sync::atomic::Ordering::Relaxed);
            let chunk_len = bytes.len() as u64;
            offset.store(chunk_start + chunk_len, std::sync::atomic::Ordering::Relaxed);

            let chunk_end = chunk_start + chunk_len; // exclusive
            if chunk_end <= stream_start || chunk_start > stream_end {
                // Outside the window: emit an empty chunk and continue.
                return Some((Ok(Vec::new()), (queue, offset, chunks)));
            }

            let from = stream_start.saturating_sub(chunk_start) as usize;
            let to = (stream_end.min(chunk_end - 1) + 1 - chunk_start) as usize;
            Some((Ok(bytes[from..to].to_vec()), (queue, offset, chunks)))
        },
    );

    let filename = file_path.rsplit('/').next().unwrap_or(file_path);

    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_DISPOSITION, content_disposition(filename))
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, requested);

    if range.is_some() {
        builder = builder
            .status(axum::http::StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{file_size}"));
    }

    builder
        .body(Body::from_stream(chunk_stream))
        .map_err(|e| ApiError(CasError::Store(format!("response build: {e}"))))
}

// ---------------------------------------------------------------------------
// GET /version/:ref/ls — list root directory
// ---------------------------------------------------------------------------

async fn list_root(
    State(state): State<AppState>,
    Path(ref_str): Path<String>,
) -> ApiResult<Json<DirListingResponse>> {
    super::helpers::run_blocking(move || list_directory_service(&state, &ref_str, "").map(Json))
        .await
}

// ---------------------------------------------------------------------------
// GET /version/:ref/ls/*path — list subdirectory
// ---------------------------------------------------------------------------

async fn list_dir(
    State(state): State<AppState>,
    Path((ref_str, dir_path)): Path<(String, String)>,
) -> ApiResult<Json<DirListingResponse>> {
    super::helpers::run_blocking(move || {
        list_directory_service(&state, &ref_str, &dir_path).map(Json)
    })
    .await
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
