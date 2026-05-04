//! Branch and tag ref endpoints.

use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::dto::{CreateRefRequest, RefListResponse, RefResponse, VersionResponse};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::commit::CommitService;
use crate::model::{RefKind, VersionId};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_refs).post(create_ref))
        .route("/{name}", get(get_ref).put(update_ref).delete(delete_ref))
        .route("/{name}/resolve", get(resolve_ref))
}

#[derive(Deserialize)]
pub struct RefFilterQuery {
    pub kind: Option<String>,
}

async fn list_refs(
    State(state): State<AppState>,
    Query(query): Query<RefFilterQuery>,
) -> ApiResult<Json<RefListResponse>> {
    let kind = match query.kind.as_deref() {
        Some("branch") => Some(RefKind::Branch),
        Some("tag") => Some(RefKind::Tag),
        _ => None,
    };

    list_refs_service(&state, kind).map(Json)
}

/// Synchronous service for listing refs, optionally filtered by kind.
pub fn list_refs_service(
    state: &AppState,
    kind: Option<RefKind>,
) -> ApiResult<RefListResponse> {
    let refs = state.refs.list_refs(kind).map_err(ApiError::from)?;

    Ok(RefListResponse {
        refs: refs.iter().map(ref_to_dto).collect(),
    })
}

async fn create_ref(
    State(state): State<AppState>,
    Json(req): Json<CreateRefRequest>,
) -> ApiResult<Json<RefResponse>> {
    create_ref_service(&state, &req.name, &req.kind, &req.target).map(Json)
}

/// Synchronous service for creating a branch or tag ref.
///
/// `kind` must be `"branch"` or `"tag"`. `target` is interpreted as a
/// version id (no ref-resolution).
pub fn create_ref_service(
    state: &AppState,
    name: &str,
    kind: &str,
    target: &str,
) -> ApiResult<RefResponse> {
    let svc = CommitService::new(state.versions.clone(), state.refs.clone());
    let target = VersionId::from(target);

    let r = match kind {
        "branch" => svc.create_branch(name, &target).map_err(ApiError::from)?,
        "tag" => svc.create_tag(name, &target).map_err(ApiError::from)?,
        other => {
            return Err(ApiError(crate::error::CasError::InvalidRef(format!(
                "unknown ref kind: {other}"
            ))))
        }
    };

    Ok(ref_to_dto(&r))
}

async fn get_ref(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<RefResponse>> {
    let r = state
        .refs
        .get_ref(&name)
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(crate::error::CasError::NotFound(format!(
                "ref '{name}' not found"
            )))
        })?;

    Ok(Json(ref_to_dto(&r)))
}

#[derive(Deserialize)]
pub struct UpdateRefRequest {
    pub target: String,
}

async fn update_ref(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateRefRequest>,
) -> ApiResult<Json<RefResponse>> {
    let svc = CommitService::new(state.versions.clone(), state.refs.clone());
    let target = VersionId::from(req.target.as_str());

    let r = svc.update_branch(&name, &target).map_err(ApiError::from)?;
    Ok(Json(ref_to_dto(&r)))
}

async fn delete_ref(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    delete_ref_service(&state, &name)?;
    Ok(Json(serde_json::json!({ "deleted": name })))
}

/// Synchronous service for deleting a branch or tag ref by name.
///
/// Unlike `CommitService::delete_branch`, this also accepts tags so callers
/// (notably the Tauri `delete_ref` IPC command) have a single uniform
/// entry point.
pub fn delete_ref_service(state: &AppState, name: &str) -> ApiResult<()> {
    let existing = state
        .refs
        .get_ref(name)
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(crate::error::CasError::NotFound(format!(
                "ref '{name}' not found"
            )))
        })?;

    match existing.kind {
        RefKind::Branch => {
            let svc = CommitService::new(state.versions.clone(), state.refs.clone());
            svc.delete_branch(name).map_err(ApiError::from)?;
        }
        RefKind::Tag => {
            state.refs.delete_ref(name).map_err(ApiError::from)?;
        }
    }

    Ok(())
}

async fn resolve_ref(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<VersionResponse>> {
    let svc = CommitService::new(state.versions.clone(), state.refs.clone());
    let version = svc
        .resolve_ref(&name)
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError(crate::error::CasError::NotFound(format!(
                "ref '{name}' not found or target missing"
            )))
        })?;

    Ok(Json(super::versions::version_to_dto(&version)))
}

fn ref_to_dto(r: &crate::model::Ref) -> RefResponse {
    RefResponse {
        name: r.name.clone(),
        kind: match r.kind {
            RefKind::Branch => "branch".into(),
            RefKind::Tag => "tag".into(),
        },
        target: r.target.as_str().to_string(),
    }
}
