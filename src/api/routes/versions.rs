//! Version endpoints.

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::dto::{
    CreateVersionRequest, NodeMetadataResponse, PaginatedVersionListResponse,
    VersionResponse,
};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::commit::{CommitRequest, CommitService};
use crate::model::node::NodeKind;
use crate::model::VersionId;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(create_version))
        .route("/{id}", get(get_version))
        .route("/{id}/history", get(get_history))
        .route("/{id}/path/{*path}", get(get_node_metadata))
}

async fn create_version(
    State(state): State<AppState>,
    Json(req): Json<CreateVersionRequest>,
) -> ApiResult<Json<VersionResponse>> {
    let root = super::objects::parse_hash(&req.root)?;
    let parents: Vec<VersionId> = req.parents.into_iter().map(VersionId::from).collect();

    let svc = CommitService::new(state.versions.clone(), state.refs.clone());
    let version = svc
        .commit(CommitRequest {
            root,
            parents,
            message: req.message,
            metadata: req.metadata,
            branch: req.branch,
        })
        .map_err(ApiError::from)?;

    Ok(Json(version_to_dto(&version)))
}

async fn get_version(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<VersionResponse>> {
    let v = super::helpers::resolve_version_node(&id, state.versions.as_ref(), state.refs.as_ref())?;
    Ok(Json(version_to_dto(&v)))
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_limit() -> usize {
    50
}

async fn get_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> ApiResult<Json<PaginatedVersionListResponse>> {
    // Resolve id/branch/tag to a starting version.
    let start = super::helpers::resolve_version_node(&id, state.versions.as_ref(), state.refs.as_ref())?;

    // Fetch one more than limit+offset to determine has_more.
    let total_needed = query.offset + query.limit + 1;
    let all = state
        .versions
        .list_history(&start.id, total_needed)
        .map_err(ApiError::from)?;

    let has_more = all.len() > query.offset + query.limit;
    let page = all
        .into_iter()
        .skip(query.offset)
        .take(query.limit)
        .collect::<Vec<_>>();

    Ok(Json(PaginatedVersionListResponse {
        versions: page.iter().map(version_to_dto).collect(),
        offset: query.offset,
        limit: query.limit,
        has_more,
    }))
}

// ---------------------------------------------------------------------------
// GET /{id}/path/{*path} — node metadata by version + path
// ---------------------------------------------------------------------------

async fn get_node_metadata(
    State(state): State<AppState>,
    Path((id, file_path)): Path<(String, String)>,
) -> ApiResult<Json<NodeMetadataResponse>> {
    let v = super::helpers::resolve_version_node(&id, state.versions.as_ref(), state.refs.as_ref())?;

    let entry = state
        .path_index
        .get_by_path(&v.id, &file_path)
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(crate::error::CasError::NotFound(format!(
                "path '{}' not found in version '{}'",
                file_path, v.id
            )))
        })?;

    // Enrich with node data from CAS.
    let node = state
        .nodes
        .get_node(&entry.node_hash)
        .map_err(ApiError::from)?;

    let (is_directory, size, children) = match node.as_ref().map(|n| &n.kind) {
        Some(NodeKind::File { size, .. }) => (false, *size, None),
        Some(NodeKind::Directory { children }) => {
            let names: Vec<String> = children.keys().cloned().collect();
            (true, 0, Some(names))
        }
        None => (entry.is_directory, 0, None),
    };

    Ok(Json(NodeMetadataResponse {
        version_id: v.id.to_string(),
        path: file_path,
        hash: entry.node_hash.to_hex().to_string(),
        is_directory,
        size,
        children,
    }))
}

pub fn version_to_dto(v: &crate::model::VersionNode) -> VersionResponse {
    VersionResponse {
        id: v.id.as_str().to_string(),
        root: v.root.to_hex().to_string(),
        parents: v.parents.iter().map(|p| p.as_str().to_string()).collect(),
        message: v.message.clone(),
        timestamp: v.timestamp,
        metadata: v.metadata.clone(),
    }
}
