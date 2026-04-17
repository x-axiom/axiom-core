//! Version endpoints.

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::dto::{CreateVersionRequest, VersionListResponse, VersionResponse};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::commit::{CommitRequest, CommitService};
use crate::model::VersionId;
use crate::store::traits::VersionRepo;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", post(create_version))
        .route("/{id}", get(get_version))
        .route("/{id}/history", get(get_history))
}

async fn create_version(
    State(state): State<AppState>,
    Json(req): Json<CreateVersionRequest>,
) -> ApiResult<Json<VersionResponse>> {
    let root = super::objects::parse_hash(&req.root)?;
    let parents: Vec<VersionId> = req.parents.into_iter().map(VersionId::from).collect();

    let svc = CommitService::new(state.meta.clone(), state.meta.clone());
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
    let version = state
        .meta
        .get_version(&VersionId::from(id.as_str()))
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(crate::error::CasError::NotFound(format!(
                "version '{id}' not found"
            )))
        })?;

    Ok(Json(version_to_dto(&version)))
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    50
}

async fn get_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> ApiResult<Json<VersionListResponse>> {
    let history = state
        .meta
        .list_history(&VersionId::from(id.as_str()), query.limit)
        .map_err(ApiError::from)?;

    Ok(Json(VersionListResponse {
        versions: history.iter().map(version_to_dto).collect(),
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
