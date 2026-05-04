//! SaaS-style workspace HTTP contract used by axiom-web.
//!
//! In Phase 0 refs and versions are still globally scoped. These handlers
//! bridge that gap by serving workspace-shaped endpoints backed by the
//! existing global stores, falling back to the legacy `main` ref for the
//! seeded `default` workspace when no workspace head has been persisted yet.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Deserialize;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::api::dto::{
    CreateWorkspaceHttpRequest, WorkspaceDiffEntryResponse, WorkspaceSummaryResponse,
    WorkspaceTreeEntryResponse, WorkspaceVersionPageResponse, WorkspaceVersionResponse,
};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::error::CasError;
use crate::store::traits::{Workspace, WorkspaceRepo};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_workspaces).post(create_workspace))
        .route("/{workspace_id}", delete(delete_workspace))
        .route("/{workspace_id}/tree", get(list_tree_root))
        .route("/{workspace_id}/tree/{*path}", get(list_tree_at_path))
        .route("/{workspace_id}/download/{*path}", get(download_workspace_file))
        .route("/{workspace_id}/versions", get(list_versions))
        .route("/{workspace_id}/diff", get(diff_versions))
}

#[derive(Deserialize)]
struct VersionsQuery {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct DiffQuery {
    from: String,
    to: String,
}

fn default_limit() -> usize {
    20
}

fn workspace_repo(state: &AppState) -> ApiResult<&dyn WorkspaceRepo> {
    state
        .workspaces
        .as_deref()
        .ok_or_else(|| ApiError(CasError::Store("workspace API unavailable for this backend".into())))
}

fn load_workspace(state: &AppState, workspace_id: &str) -> ApiResult<Workspace> {
    workspace_repo(state)?
        .get_workspace(workspace_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(CasError::WorkspaceNotFound(workspace_id.to_owned())))
}

fn current_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn iso_timestamp(unix_secs: u64) -> String {
    OffsetDateTime::from_unix_timestamp(unix_secs as i64)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn generate_workspace_id(name: &str, created_at: u64) -> String {
    let slug = name
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            '-' | '_' => ch,
            _ => '-',
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned();
    let hash = blake3::hash(format!("{slug}:{created_at}").as_bytes())
        .to_hex()
        .to_string();
    if slug.is_empty() {
        format!("ws-{}", &hash[..12])
    } else {
        format!("{slug}-{}", &hash[..8])
    }
}

fn workspace_head_selector(state: &AppState, workspace: &Workspace) -> ApiResult<Option<String>> {
    if let Some(version_id) = &workspace.current_version {
        return Ok(Some(version_id.clone()));
    }
    if let Some(ref_name) = &workspace.current_ref {
        return Ok(Some(ref_name.clone()));
    }

    // Phase 0 bridge: old single-user data lives on the global `main` ref.
    if workspace.id == "default"
        && state
            .refs
            .get_ref("main")
            .map_err(ApiError::from)?
            .is_some()
    {
        return Ok(Some("main".to_owned()));
    }

    Ok(None)
}

fn workspace_updated_at(state: &AppState, workspace: &Workspace) -> u64 {
    let Some(selector) = workspace_head_selector(state, workspace).ok().flatten() else {
        return workspace.created_at;
    };

    super::helpers::resolve_version_node(&selector, state.versions.as_ref(), state.refs.as_ref())
        .map(|version| version.timestamp)
        .unwrap_or(workspace.created_at)
}

fn workspace_to_summary(state: &AppState, workspace: Workspace) -> WorkspaceSummaryResponse {
    let updated_at = workspace_updated_at(state, &workspace);
    WorkspaceSummaryResponse {
        id: workspace.id,
        name: workspace.name,
        size_bytes: None,
        created_at: iso_timestamp(workspace.created_at),
        updated_at: iso_timestamp(updated_at),
    }
}

fn parse_cursor(cursor: Option<&str>) -> ApiResult<usize> {
    match cursor {
        Some(raw) if !raw.is_empty() => raw.parse::<usize>().map_err(|_| {
            ApiError(CasError::InvalidObject(format!(
                "invalid cursor '{raw}' (expected numeric offset)"
            )))
        }),
        _ => Ok(0),
    }
}

async fn list_workspaces(
    State(state): State<AppState>,
) -> ApiResult<Json<Vec<WorkspaceSummaryResponse>>> {
    let workspaces = workspace_repo(&state)?
        .list_workspaces()
        .map_err(ApiError::from)?;
    Ok(Json(
        workspaces
            .into_iter()
            .map(|workspace| workspace_to_summary(&state, workspace))
            .collect(),
    ))
}

async fn create_workspace(
    State(state): State<AppState>,
    Json(req): Json<CreateWorkspaceHttpRequest>,
) -> ApiResult<Json<WorkspaceSummaryResponse>> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err(ApiError(CasError::InvalidObject(
            "workspace name must not be empty".into(),
        )));
    }

    let repo = workspace_repo(&state)?;
    let existing = repo.list_workspaces().map_err(ApiError::from)?;
    if existing.iter().any(|workspace| workspace.name == name) {
        return Err(ApiError(CasError::AlreadyExists));
    }

    let created_at = current_unix_secs();
    let metadata = match req.description.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        Some(description) => serde_json::json!({ "description": description }).to_string(),
        None => "{}".to_owned(),
    };
    let workspace = Workspace {
        id: generate_workspace_id(name, created_at),
        name: name.to_owned(),
        created_at,
        metadata,
        local_path: None,
        current_ref: None,
        current_version: None,
        deleted_at: None,
    };

    repo.create_workspace(&workspace).map_err(ApiError::from)?;
    Ok(Json(workspace_to_summary(&state, workspace)))
}

async fn delete_workspace(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
) -> ApiResult<StatusCode> {
    if workspace_id == "default" {
        return Err(ApiError(CasError::InvalidObject(
            "cannot delete the default workspace".into(),
        )));
    }

    workspace_repo(&state)?
        .delete_workspace(&workspace_id)
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_tree_root(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
) -> ApiResult<Json<Vec<WorkspaceTreeEntryResponse>>> {
    list_tree_impl(&state, &workspace_id, "").map(Json)
}

async fn list_tree_at_path(
    State(state): State<AppState>,
    Path((workspace_id, path)): Path<(String, String)>,
) -> ApiResult<Json<Vec<WorkspaceTreeEntryResponse>>> {
    list_tree_impl(&state, &workspace_id, &path).map(Json)
}

fn list_tree_impl(
    state: &AppState,
    workspace_id: &str,
    path: &str,
) -> ApiResult<Vec<WorkspaceTreeEntryResponse>> {
    let workspace = load_workspace(state, workspace_id)?;
    let Some(selector) = workspace_head_selector(state, &workspace)? else {
        return Ok(Vec::new());
    };

    let listing = super::download::list_directory_service(state, &selector, path)?;
    Ok(listing
        .entries
        .into_iter()
        .map(|entry| WorkspaceTreeEntryResponse {
            name: entry.name,
            kind: if entry.is_directory { "dir" } else { "file" }.to_owned(),
            size_bytes: (!entry.is_directory).then_some(entry.size),
            updated_at: None,
        })
        .collect())
}

async fn download_workspace_file(
    State(state): State<AppState>,
    Path((workspace_id, file_path)): Path<(String, String)>,
) -> Result<axum::response::Response, ApiError> {
    let workspace = load_workspace(&state, &workspace_id)?;
    let selector = workspace_head_selector(&state, &workspace)?.ok_or_else(|| {
        ApiError(CasError::NotFound(format!(
            "workspace '{}' has no committed files",
            workspace_id
        )))
    })?;
    super::download::download_file_response(&state, &selector, &file_path)
}

async fn list_versions(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<VersionsQuery>,
) -> ApiResult<Json<WorkspaceVersionPageResponse>> {
    let workspace = load_workspace(&state, &workspace_id)?;
    let offset = parse_cursor(query.cursor.as_deref())?;
    let Some(selector) = workspace_head_selector(&state, &workspace)? else {
        return Ok(Json(WorkspaceVersionPageResponse {
            items: Vec::new(),
            next_cursor: None,
        }));
    };

    let page = super::versions::version_history_service(&state, &selector, query.limit, offset)?;
    let next_cursor = page.has_more.then(|| (offset + query.limit).to_string());
    Ok(Json(WorkspaceVersionPageResponse {
        items: page
            .versions
            .into_iter()
            .map(|version| WorkspaceVersionResponse {
                id: version.id,
                parent_ids: version.parents,
                message: (!version.message.is_empty()).then_some(version.message),
                author: version.metadata.get("author").cloned(),
                timestamp: iso_timestamp(version.timestamp),
            })
            .collect(),
        next_cursor,
    }))
}

async fn diff_versions(
    State(state): State<AppState>,
    Path(workspace_id): Path<String>,
    Query(query): Query<DiffQuery>,
) -> ApiResult<Json<Vec<WorkspaceDiffEntryResponse>>> {
    let _workspace = load_workspace(&state, &workspace_id)?;
    let diff = super::diff::diff_versions_service(&state, &query.from, &query.to)?;
    Ok(Json(
        diff.entries
            .into_iter()
            .map(|entry| WorkspaceDiffEntryResponse {
                path: entry.path,
                kind: entry.kind,
                old_size_bytes: None,
                new_size_bytes: None,
            })
            .collect(),
    ))
}