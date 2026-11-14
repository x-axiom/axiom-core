//! SaaS-style workspace HTTP contract used by axiom-web.
//!
//! In Phase 0 refs and versions are still globally scoped. These handlers
//! bridge that gap by serving workspace-shaped endpoints backed by the
//! existing global stores, falling back to the legacy `main` ref for the
//! seeded `default` workspace when no workspace head has been persisted yet.

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{patch, post};
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::api::dto::{
    CreateWorkspaceHttpRequest, InviteWorkspaceMemberRequest, UpdateWorkspaceMemberRoleRequest,
    WorkspaceDiffEntryResponse, WorkspaceMemberResponse, WorkspaceSummaryResponse,
    WorkspaceTreeEntryResponse, WorkspaceVersionPageResponse, WorkspaceVersionResponse,
};
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::auth::rbac::{AuthContext, Permission};
use crate::error::CasError;
use crate::store::traits::{Workspace, WorkspaceRepo};
use crate::tenant::model::Role;
use crate::tenant::model::{
    Membership, OrgId, User, UserId, Workspace as TenantWorkspace, WorkspaceId,
};
use crate::tenant::TenantDirectory;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(list_workspaces).post(create_workspace))
        .route("/{workspace_id}", delete(delete_workspace))
        .route("/deleted", get(list_deleted_workspaces))
        .route("/{workspace_id}/restore", post(restore_workspace))
        .route("/{workspace_id}/purge", post(purge_workspace))
        .route("/{workspace_id}/members", get(list_members))
        .route("/{workspace_id}/members/invite", post(invite_member))
        .route(
            "/{workspace_id}/members/{member_id}",
            patch(update_member_role).delete(remove_member),
        )
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

fn tenant_directory(state: &AppState) -> ApiResult<&dyn TenantDirectory> {
    state
    .tenant_directory
        .as_deref()
        .ok_or_else(|| ApiError(CasError::Store("tenant API unavailable for this backend".into())))
}

/// Organization recorded in a workspace's metadata (set at creation).
fn workspace_org(workspace: &Workspace) -> Option<String> {
    serde_json::from_str::<Value>(&workspace.metadata)
        .ok()
        .and_then(|v| v.get("org_id")?.as_str().map(str::to_owned))
}

/// Load a workspace and enforce organization scoping.
///
/// This is the **only** path allowed to resolve a workspace for reads: it
/// prevents cross-tenant access (IDOR) by rejecting any workspace whose
/// recorded `org_id` differs from the caller's token.
fn load_workspace(
    state: &AppState,
    auth: Option<&AuthContext>,
    workspace_id: &str,
) -> ApiResult<Workspace> {
    let workspace = workspace_repo(state)?
        .get_workspace(workspace_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(CasError::WorkspaceNotFound(workspace_id.to_owned())))?;

    if state.http_auth_mode != crate::api::state::HttpAuthMode::Disabled {
        let auth = require_auth(auth)?;
        match workspace_org(&workspace) {
            Some(org) if org == auth.org_id => {}
            Some(_) => {
                return Err(ApiError(CasError::Forbidden(format!(
                    "workspace '{workspace_id}' is outside current org scope"
                ))));
            }
            None => {
                return Err(ApiError(CasError::Forbidden(format!(
                    "workspace '{workspace_id}' has no organization scope"
                ))));
            }
        }
    }

    Ok(workspace)
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

    // Workspace-scoped ref used by cloud sync (`ws/<id>/main`). Present once a
    // client has pushed into this workspace.
    let scoped = format!("ws/{}/main", workspace.id);
    if state.refs.get_ref(&scoped).map_err(ApiError::from)?.is_some() {
        return Ok(Some(scoped));
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

fn tenant_workspace_to_summary(workspace: TenantWorkspace) -> WorkspaceSummaryResponse {
    WorkspaceSummaryResponse {
        id: workspace.id.0,
        name: workspace.name,
        size_bytes: None,
        created_at: iso_timestamp(workspace.created_at),
        updated_at: iso_timestamp(workspace.created_at),
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

fn auth_context(auth: Option<Extension<AuthContext>>) -> Option<AuthContext> {
    auth.map(|Extension(ctx)| ctx)
}

fn ensure_permission(
    state: &AppState,
    auth: Option<&AuthContext>,
    permission: Permission,
) -> ApiResult<()> {
    if state.http_auth_mode == crate::api::state::HttpAuthMode::Disabled {
        return Ok(());
    }

    let auth = auth.ok_or_else(|| {
        ApiError(CasError::Unauthorized("missing auth context".into()))
    })?;

    if auth.can(permission) {
        Ok(())
    } else {
        Err(ApiError(CasError::Forbidden(format!(
            "role '{:?}' lacks '{:?}' permission",
            auth.role, permission
        ))))
    }
}

fn require_auth(auth: Option<&AuthContext>) -> ApiResult<&AuthContext> {
    auth.ok_or_else(|| ApiError(CasError::Unauthorized("missing auth context".into())))
}

fn ensure_member_management_permission(auth: Option<&AuthContext>) -> ApiResult<&AuthContext> {
    let auth = require_auth(auth)?;
    if matches!(auth.role, Role::Owner | Role::Admin) {
        Ok(auth)
    } else {
        Err(ApiError(CasError::Forbidden(
            "member management requires owner or admin role".into(),
        )))
    }
}

#[cfg_attr(not(feature = "fdb"), allow(dead_code))]
/// Lowercase role name used by the public API.
fn api_role_name(role: &Role) -> String {
    format!("{role:?}").to_ascii_lowercase()
}

fn member_role_from_api(role: &str) -> ApiResult<Role> {
    match role.trim().to_ascii_lowercase().as_str() {
        "owner" => Ok(Role::Owner),
        "admin" => Ok(Role::Admin),
        "member" => Ok(Role::Member),
        "viewer" => Ok(Role::Viewer),
        other => Err(ApiError(CasError::InvalidObject(format!(
            "unsupported member role '{other}'"
        )))),
    }
}

#[cfg_attr(not(feature = "fdb"), allow(dead_code))]
fn member_role_to_api(role: &Role) -> &'static str {
    match role {
        Role::Owner => "owner",
        Role::Admin => "admin",
        Role::Member => "member",
        Role::Viewer => "viewer",
    }
}

#[cfg_attr(not(feature = "fdb"), allow(dead_code))]
fn derive_display_name(email: &str) -> String {
    let local = email.split('@').next().unwrap_or(email).trim();
    if local.is_empty() {
        return email.to_owned();
    }

    local
        .split(['.', '_', '-'])
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut chars = segment.chars();
            match chars.next() {
                Some(first) => {
                    let mut word = first.to_ascii_uppercase().to_string();
                    word.push_str(chars.as_str());
                    word
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn load_tenant_workspace(
    state: &AppState,
    auth: Option<&AuthContext>,
    workspace_id: &str,
) -> ApiResult<TenantWorkspace> {
    let auth = require_auth(auth)?;
    let workspace = tenant_directory(state)?
        .get_workspace(&WorkspaceId::from(workspace_id))
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(CasError::WorkspaceNotFound(workspace_id.to_owned())))?;

    if workspace.org_id.as_str() != auth.org_id {
        return Err(ApiError(CasError::Forbidden(format!(
            "workspace '{}' is outside current org scope",
            workspace_id
        ))));
    }

    Ok(workspace)
}

fn membership_to_response(
    directory: &dyn TenantDirectory,
    membership: Membership,
) -> ApiResult<WorkspaceMemberResponse> {
    let user = directory
        .get_user(&membership.user_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(CasError::NotFound(format!(
            "user '{}' missing for membership",
            membership.user_id
        ))))?;
    Ok(user_to_member_response(user, membership.role))
}

fn user_to_member_response(user: User, role: Role) -> WorkspaceMemberResponse {
    WorkspaceMemberResponse {
        id: user.id.0,
        email: user.email,
        name: user.display_name,
        avatar_url: None,
        role: member_role_to_api(&role).to_owned(),
        joined_at: iso_timestamp(user.created_at),
    }
}

fn ensure_not_last_owner(
    directory: &dyn TenantDirectory,
    org_id: &OrgId,
    membership: &Membership,
    next_role: Option<&Role>,
) -> ApiResult<()> {
    if membership.role != Role::Owner {
        return Ok(());
    }

    if matches!(next_role, Some(Role::Owner)) {
        return Ok(());
    }

    let owner_count = directory
        .list_members_of_org(org_id)
        .map_err(ApiError::from)?
        .into_iter()
        .filter(|entry| entry.role == Role::Owner)
        .count();
    if owner_count <= 1 {
        return Err(ApiError(CasError::Forbidden(
            "cannot remove or demote the last owner".into(),
        )));
    }

    Ok(())
}

async fn list_workspaces(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
) -> ApiResult<Json<Vec<WorkspaceSummaryResponse>>> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Read)?;

    if state.http_auth_mode != crate::api::state::HttpAuthMode::Disabled {
        if let Some(auth) = auth.as_ref() {
            if let Some(directory) = state.tenant_directory.as_deref() {
                let workspaces = directory
                    .list_workspaces_by_org(&OrgId::from(auth.org_id.as_str()))
                    .map_err(ApiError::from)?;
                // Hide soft-deleted workspaces: they live in the recycle bin
                // until restored or purged.
                let repo = state.workspaces.as_deref();
                let visible = workspaces.into_iter().filter(|ws| match repo {
                    Some(repo) => repo
                        .get_workspace(&ws.id.0)
                        .ok()
                        .flatten()
                        .map(|row| row.deleted_at.is_none())
                        .unwrap_or(true),
                    None => true,
                });
                return Ok(Json(
                    visible.map(tenant_workspace_to_summary).collect(),
                ));
            }
        }
    }

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
    auth: Option<Extension<AuthContext>>,
    Json(req): Json<CreateWorkspaceHttpRequest>,
) -> ApiResult<Json<WorkspaceSummaryResponse>> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Write)?;
    let name = req.name.trim();
    if name.is_empty() {
        return Err(ApiError(CasError::InvalidObject(
            "workspace name must not be empty".into(),
        )));
    }

    if state.http_auth_mode != crate::api::state::HttpAuthMode::Disabled {
        if let Some(directory) = state.tenant_directory.as_deref() {
            let auth = require_auth(auth.as_ref())?;
            let org_id = OrgId::from(auth.org_id.as_str());
            let existing = directory
                .list_workspaces_by_org(&org_id)
                .map_err(ApiError::from)?;
            if existing.iter().any(|workspace| workspace.name == name) {
                return Err(ApiError(CasError::AlreadyExists));
            }

            let tenant_workspace = directory
                .create_workspace(&org_id, name, 0)
                .map_err(ApiError::from)?;

            // Mirror into the data-plane repository so browse/versions/diff can
            // resolve the workspace, carrying its org scope for authorization.
            if let Some(repo) = state.workspaces.as_deref() {
                let mirrored = Workspace {
                    id: tenant_workspace.id.0.clone(),
                    name: name.to_owned(),
                    created_at: tenant_workspace.created_at,
                    metadata: serde_json::json!({ "org_id": auth.org_id }).to_string(),
                    local_path: None,
                    current_ref: None,
                    current_version: None,
                    deleted_at: None,
                };
                if let Err(err) = repo.create_workspace(&mirrored) {
                    // Roll back the tenant record so the two stores stay aligned.
                    let _ = directory
                        .delete_workspace(&tenant_workspace.id, &org_id);
                    return Err(ApiError::from(err));
                }
            }

            state.record_audit(
                &auth.user_id,
                &auth.tenant_id,
                "workspace.create",
                &tenant_workspace.id.0,
                "ok",
            );
            return Ok(Json(tenant_workspace_to_summary(tenant_workspace)));
        }
    }

    let repo = workspace_repo(&state)?;
    let existing = repo.list_workspaces().map_err(ApiError::from)?;
    if existing.iter().any(|workspace| workspace.name == name) {
        return Err(ApiError(CasError::AlreadyExists));
    }

    let created_at = current_unix_secs();
    let mut metadata = serde_json::Map::new();
    if let Some(description) = req.description.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        metadata.insert("description".into(), description.into());
    }
    if state.http_auth_mode != crate::api::state::HttpAuthMode::Disabled {
        if let Some(auth) = auth.as_ref() {
            metadata.insert("org_id".into(), auth.org_id.clone().into());
        }
    }
    let metadata = Value::Object(metadata).to_string();
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
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
) -> ApiResult<StatusCode> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Delete)?;
    if workspace_id == "default" {
        return Err(ApiError(CasError::InvalidObject(
            "cannot delete the default workspace".into(),
        )));
    }

    // Soft delete: the workspace disappears from listings but its metadata is
    // kept for the recycle-bin grace period (P1-3).
    let now = current_unix_secs();
    if state.http_auth_mode != crate::api::state::HttpAuthMode::Disabled {
        if state.tenant_directory.is_some() {
            let auth_ctx = require_auth(auth.as_ref())?;
            let workspace = load_tenant_workspace(&state, Some(auth_ctx), &workspace_id)?;
            match state.workspaces.as_deref() {
                Some(repo) => {
                    repo.soft_delete_workspace(&workspace_id, now)
                        .map_err(ApiError::from)?;
                }
                // No data-plane repository on this backend: fall back to
                // removing the tenant record (no recycle bin available).
                None => {
                    tenant_directory(&state)?
                        .delete_workspace(
                            &WorkspaceId::from(workspace_id.as_str()),
                            &workspace.org_id,
                        )
                        .map_err(ApiError::from)?;
                }
            }
            state.record_audit(
                &auth_ctx.user_id,
                &auth_ctx.tenant_id,
                "workspace.delete",
                &workspace_id,
                "ok",
            );
            return Ok(StatusCode::NO_CONTENT);
        }
    }

    workspace_repo(&state)?
        .soft_delete_workspace(&workspace_id, now)
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_members(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
) -> ApiResult<Json<Vec<WorkspaceMemberResponse>>> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Read)?;
    let workspace = load_tenant_workspace(&state, auth.as_ref(), &workspace_id)?;
    let directory = tenant_directory(&state)?;
    let members = directory
        .list_members_of_org(&workspace.org_id)
        .map_err(ApiError::from)?
        .into_iter()
        .map(|membership| membership_to_response(directory, membership))
        .collect::<ApiResult<Vec<_>>>()?;
    Ok(Json(members))
}

/// Result of an invite: either an existing user was added, or a signed
/// invitation link was created for a new email (P2-6).
#[derive(serde::Serialize)]
#[serde(untagged)]
enum InviteOutcome {
    Added(WorkspaceMemberResponse),
    Invited {
        status: &'static str,
        email: String,
        role: String,
        invitation_token: String,
        expires_at: u64,
    },
}

async fn invite_member(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
    Json(req): Json<InviteWorkspaceMemberRequest>,
) -> ApiResult<Json<InviteOutcome>> {
    let auth = auth_context(auth);
    let auth_ctx = ensure_member_management_permission(auth.as_ref())?;
    let workspace = load_tenant_workspace(&state, auth.as_ref(), &workspace_id)?;
    let email = req.email.trim().to_ascii_lowercase();
    if email.is_empty() || !email.contains('@') {
        return Err(ApiError(CasError::InvalidObject(
            "member email must be a valid address".into(),
        )));
    }

    let role = member_role_from_api(&req.role)?;
    let directory = tenant_directory(&state)?;

    // Existing account: add the membership immediately (admin action).
    if let Some(user) = directory.get_user_by_email(&email).map_err(ApiError::from)? {
        if directory
            .get_membership(&user.id, &workspace.org_id)
            .map_err(ApiError::from)?
            .is_some()
        {
            return Err(ApiError(CasError::AlreadyExists));
        }
        directory
            .put_membership(&user.id, &workspace.org_id, role.clone())
            .map_err(ApiError::from)?;
        state.record_audit(
            &auth_ctx.user_id,
            &auth_ctx.tenant_id,
            "member.invite",
            &user.id.0,
            "ok",
        );
        return Ok(Json(InviteOutcome::Added(user_to_member_response(user, role))));
    }

    // New email: never create an account on the inviter's behalf. Issue a
    // signed invitation the invitee must accept while signed in as that email.
    let Some(auth_service) = state.auth.as_ref() else {
        // No auth service (local/dev deployments without identity): fall back
        // to the legacy behaviour so in-process team management still works.
        tracing::warn!(
            email = %email,
            "auth disabled: creating the invited user directly instead of issuing an invitation"
        );
        let user = directory
            .create_user(&email, &derive_display_name(&email))
            .map_err(ApiError::from)?;
        directory
            .put_membership(&user.id, &workspace.org_id, role.clone())
            .map_err(ApiError::from)?;
        state.record_audit(
            &auth_ctx.user_id,
            &auth_ctx.tenant_id,
            "member.invite",
            &user.id.0,
            "ok",
        );
        return Ok(Json(InviteOutcome::Added(user_to_member_response(user, role))));
    };

    let (token, expires_at) = auth_service
        .jwt()
        .issue_invitation(
            workspace.org_id.as_str(),
            &email,
            &api_role_name(&role),
            &auth_ctx.user_id,
        )
        .map_err(ApiError::from)?;

    state.record_audit(
        &auth_ctx.user_id,
        &auth_ctx.tenant_id,
        "member.invite_pending",
        &email,
        "ok",
    );

    Ok(Json(InviteOutcome::Invited {
        status: "invited",
        email,
        role: api_role_name(&role),
        invitation_token: token,
        expires_at,
    }))
}

async fn update_member_role(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((workspace_id, member_id)): Path<(String, String)>,
    Json(req): Json<UpdateWorkspaceMemberRoleRequest>,
) -> ApiResult<Json<WorkspaceMemberResponse>> {
    let auth = auth_context(auth);
    let auth_ctx = ensure_member_management_permission(auth.as_ref())?;
    let workspace = load_tenant_workspace(&state, auth.as_ref(), &workspace_id)?;
    let role = member_role_from_api(&req.role)?;
    let directory = tenant_directory(&state)?;
    let user_id = UserId::from(member_id.as_str());
    let membership = directory
        .get_membership(&user_id, &workspace.org_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(CasError::NotFound(format!(
            "member '{}' not found",
            member_id
        ))))?;
    ensure_not_last_owner(directory, &workspace.org_id, &membership, Some(&role))?;
    let user = directory
        .get_user(&user_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(CasError::NotFound(format!(
            "user '{}' not found",
            member_id
        ))))?;
    directory.put_membership(&user_id, &workspace.org_id, role.clone())
        .map_err(ApiError::from)?;
    state.record_audit(
        &auth_ctx.user_id,
        &auth_ctx.tenant_id,
        "member.role_update",
        &member_id,
        "ok",
    );
    Ok(Json(user_to_member_response(user, role)))
}

async fn remove_member(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((workspace_id, member_id)): Path<(String, String)>,
) -> ApiResult<Json<Value>> {
    let auth = auth_context(auth);
    let auth_ctx = ensure_member_management_permission(auth.as_ref())?;
    let workspace = load_tenant_workspace(&state, auth.as_ref(), &workspace_id)?;
    let directory = tenant_directory(&state)?;
    let user_id = UserId::from(member_id.as_str());
    let membership = directory
        .get_membership(&user_id, &workspace.org_id)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(CasError::NotFound(format!(
            "member '{}' not found",
            member_id
        ))))?;
    ensure_not_last_owner(directory, &workspace.org_id, &membership, None)?;
    directory.delete_membership(&user_id, &workspace.org_id)
        .map_err(ApiError::from)?;
    state.record_audit(
        &auth_ctx.user_id,
        &auth_ctx.tenant_id,
        "member.remove",
        &member_id,
        "ok",
    );
    Ok(Json(serde_json::json!({})))
}

async fn list_tree_root(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
) -> ApiResult<Json<Vec<WorkspaceTreeEntryResponse>>> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Read)?;
    super::helpers::run_blocking(move || {
        list_tree_impl(&state, auth.as_ref(), &workspace_id, "").map(Json)
    })
    .await
}

async fn list_tree_at_path(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((workspace_id, path)): Path<(String, String)>,
) -> ApiResult<Json<Vec<WorkspaceTreeEntryResponse>>> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Read)?;
    super::helpers::run_blocking(move || {
        list_tree_impl(&state, auth.as_ref(), &workspace_id, &path).map(Json)
    })
    .await
}

fn list_tree_impl(
    state: &AppState,
    auth: Option<&AuthContext>,
    workspace_id: &str,
    path: &str,
) -> ApiResult<Vec<WorkspaceTreeEntryResponse>> {
    let workspace = load_workspace(state, auth, workspace_id)?;
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
    auth: Option<Extension<AuthContext>>,
    Path((workspace_id, file_path)): Path<(String, String)>,
) -> Result<axum::response::Response, ApiError> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Read)?;
    let workspace = load_workspace(&state, auth.as_ref(), &workspace_id)?;
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
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
    Query(query): Query<VersionsQuery>,
) -> ApiResult<Json<WorkspaceVersionPageResponse>> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Read)?;
    let workspace = load_workspace(&state, auth.as_ref(), &workspace_id)?;
    let offset = parse_cursor(query.cursor.as_deref())?;
    let Some(selector) = workspace_head_selector(&state, &workspace)? else {
        return Ok(Json(WorkspaceVersionPageResponse {
            items: Vec::new(),
            next_cursor: None,
        }));
    };

    let limit = crate::api::pagination::clamp_limit(query.limit);
    let page = super::versions::version_history_service(&state, &selector, limit, offset)?;
    let next_cursor = page.has_more.then(|| (offset + limit).to_string());
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
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
    Query(query): Query<DiffQuery>,
) -> ApiResult<Json<Vec<WorkspaceDiffEntryResponse>>> {
    let auth = auth_context(auth);
    ensure_permission(&state, auth.as_ref(), Permission::Read)?;
    let _workspace = load_workspace(&state, auth.as_ref(), &workspace_id)?;
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

/// Grace period before a soft-deleted workspace becomes purgeable.
fn workspace_grace_secs() -> u64 {
    std::env::var("AXIOM_WORKSPACE_GRACE_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(7 * 24 * 3600)
}

async fn list_deleted_workspaces(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
) -> ApiResult<Json<Vec<WorkspaceSummaryResponse>>> {
    let auth = auth_context(auth);
    let auth = require_auth(auth.as_ref())?;
    if !matches!(auth.role, Role::Owner | Role::Admin) {
        return Err(ApiError(CasError::Forbidden(
            "recycle bin requires owner or admin role".into(),
        )));
    }

    let repo = workspace_repo(&state)?;
    let deleted = repo.list_deleted_workspaces().map_err(ApiError::from)?;
    let visible = deleted
        .into_iter()
        .filter(|ws| workspace_org(ws).as_deref() == Some(auth.org_id.as_str()))
        .map(|ws| workspace_to_summary(&state, ws));

    Ok(Json(visible.collect()))
}

async fn restore_workspace(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
) -> ApiResult<StatusCode> {
    let auth = auth_context(auth);
    let auth = require_auth(auth.as_ref())?;
    if !matches!(auth.role, Role::Owner | Role::Admin) {
        return Err(ApiError(CasError::Forbidden(
            "restore requires owner or admin role".into(),
        )));
    }

    let workspace = load_workspace(&state, Some(auth), &workspace_id)?;
    if workspace.deleted_at.is_none() {
        return Err(ApiError(CasError::InvalidObject(
            "workspace is not deleted".into(),
        )));
    }

    workspace_repo(&state)?
        .restore_workspace(&workspace_id)
        .map_err(ApiError::from)?;
    state.record_audit(
        &auth.user_id,
        &auth.tenant_id,
        "workspace.restore",
        &workspace_id,
        "ok",
    );
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
struct PurgeQuery {
    #[serde(default)]
    dry_run: bool,
}

#[derive(serde::Serialize)]
struct PurgeResponse {
    workspace_id: String,
    dry_run: bool,
    eligible: bool,
    /// Seconds until the workspace becomes purgeable (0 = now).
    remaining_grace_secs: u64,
    purged: bool,
}

async fn purge_workspace(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(workspace_id): Path<String>,
    Query(query): Query<PurgeQuery>,
) -> ApiResult<Json<PurgeResponse>> {
    let auth = auth_context(auth);
    let auth = require_auth(auth.as_ref())?;
    if auth.role != Role::Owner {
        return Err(ApiError(CasError::Forbidden(
            "purge requires the owner role".into(),
        )));
    }

    let workspace = load_workspace(&state, Some(auth), &workspace_id)?;
    let Some(deleted_at) = workspace.deleted_at else {
        return Err(ApiError(CasError::InvalidObject(
            "workspace is not in the recycle bin".into(),
        )));
    };

    let grace = workspace_grace_secs();
    let now = current_unix_secs();
    let age = now.saturating_sub(deleted_at);
    let eligible = age >= grace;
    let remaining_grace_secs = grace.saturating_sub(age);

    if query.dry_run || !eligible {
        return Ok(Json(PurgeResponse {
            workspace_id,
            dry_run: query.dry_run,
            eligible,
            remaining_grace_secs,
            purged: false,
        }));
    }

    // Purge permanently: metadata row + tenant record. Object chunks are
    // reclaimed by GC separately and never deleted here.
    workspace_repo(&state)?
        .delete_workspace(&workspace_id)
        .map_err(ApiError::from)?;

    if let (Some(directory), Some(org)) = (state.tenant_directory.as_deref(), workspace_org(&workspace)) {
        let _ = directory.delete_workspace(&WorkspaceId::from(workspace_id.as_str()), &OrgId::from(org));
    }

    state.record_audit(
        &auth.user_id,
        &auth.tenant_id,
        "workspace.purge",
        &workspace_id,
        "ok",
    );

    Ok(Json(PurgeResponse {
        workspace_id,
        dry_run: false,
        eligible: true,
        remaining_grace_secs: 0,
        purged: true,
    }))
}

/// Purge every soft-deleted workspace whose grace period has elapsed.
///
/// Called by the background reaper and by admin tooling. Object chunks are
/// **not** touched here: they are reclaimed by the GC sweep once unreachable.
pub fn purge_expired_workspaces(state: &AppState) -> usize {
    let Some(repo) = state.workspaces.as_deref() else {
        return 0;
    };
    let grace = workspace_grace_secs();
    let now = current_unix_secs();

    let Ok(deleted) = repo.list_deleted_workspaces() else {
        return 0;
    };

    let mut purged = 0;
    for workspace in deleted {
        let Some(deleted_at) = workspace.deleted_at else {
            continue;
        };
        if now.saturating_sub(deleted_at) < grace {
            continue;
        }

        if repo.delete_workspace(&workspace.id).is_err() {
            continue;
        }
        if let (Some(directory), Some(org)) =
            (state.tenant_directory.as_deref(), workspace_org(&workspace))
        {
            let _ = directory
                .delete_workspace(&WorkspaceId::from(workspace.id.clone()), &OrgId::from(org));
        }
        purged += 1;
        tracing::info!(workspace = %workspace.id, "recycle bin: purged expired workspace");
    }
    purged
}

#[cfg(test)]
mod tests {
    use super::{derive_display_name, member_role_from_api, member_role_to_api};
    use crate::tenant::model::Role;

    #[test]
    fn derive_display_name_from_email_local_part() {
        assert_eq!(derive_display_name("jane_doe@example.com"), "Jane Doe");
        assert_eq!(derive_display_name("ops-team@example.com"), "Ops Team");
    }

    #[test]
    fn member_role_round_trips_api_shape() {
        let owner = match member_role_from_api("owner") {
            Ok(role) => role,
            Err(_) => panic!("owner should parse"),
        };
        let viewer = match member_role_from_api("viewer") {
            Ok(role) => role,
            Err(_) => panic!("viewer should parse"),
        };
        assert_eq!(owner, Role::Owner);
        assert_eq!(viewer, Role::Viewer);
        assert_eq!(member_role_to_api(&Role::Admin), "admin");
        assert_eq!(member_role_to_api(&Role::Member), "member");
    }
}