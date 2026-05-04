//! Diff endpoints.

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};

use crate::api::dto::{DiffEntryResponse, DiffRequest, DiffResponse};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::diff_engine::diff_versions;

pub fn router() -> Router<AppState> {
    Router::new().route("/", post(compute_diff))
}

async fn compute_diff(
    State(state): State<AppState>,
    Json(req): Json<DiffRequest>,
) -> ApiResult<Json<DiffResponse>> {
    diff_versions_service(&state, &req.old_version, &req.new_version).map(Json)
}

/// Synchronous service that computes the diff between two refs/version ids.
///
/// Resolves each side via the ref index, then walks the Merkle tree-backed
/// directory representation to compute file-level changes and chunk-level
/// dedup statistics. Used by both the HTTP handler and the Tauri
/// `diff_versions` IPC command.
pub fn diff_versions_service(
    state: &AppState,
    old_ref: &str,
    new_ref: &str,
) -> ApiResult<DiffResponse> {
    let old_version =
        super::helpers::resolve_version_node(old_ref, state.versions.as_ref(), state.refs.as_ref())?;
    let new_version =
        super::helpers::resolve_version_node(new_ref, state.versions.as_ref(), state.refs.as_ref())?;

    let result = diff_versions(
        &old_version.root,
        &new_version.root,
        &*state.nodes,
        &*state.trees,
    )
    .map_err(ApiError::from)?;

    Ok(DiffResponse {
        entries: result
            .entries
            .iter()
            .map(|e| DiffEntryResponse {
                path: e.path.clone(),
                kind: format!("{:?}", e.kind).to_lowercase(),
                old_hash: e.old_hash.map(|h| h.to_hex().to_string()),
                new_hash: e.new_hash.map(|h| h.to_hex().to_string()),
            })
            .collect(),
        added_files: result.added_files,
        removed_files: result.removed_files,
        modified_files: result.modified_files,
        added_chunks: result.added_chunks,
        removed_chunks: result.removed_chunks,
        unchanged_chunks: result.unchanged_chunks,
    })
}
