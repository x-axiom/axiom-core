//! Audit trail endpoint (P1-8).

use axum::extract::{Extension, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::audit::AuditEvent;
use crate::auth::rbac::AuthContext;
use crate::error::CasError;
use crate::tenant::model::Role;

pub fn router() -> Router<AppState> {
    Router::new().route("/api/v1/audit", get(list_audit))
}

#[derive(Deserialize)]
struct AuditQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    crate::api::pagination::DEFAULT_LIMIT
}

async fn list_audit(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Query(query): Query<AuditQuery>,
) -> ApiResult<Json<Vec<AuditEvent>>> {
    let Some(Extension(auth)) = auth else {
        return Err(ApiError(CasError::Unauthorized("missing auth context".into())));
    };
    if !matches!(auth.role, Role::Owner | Role::Admin) {
        return Err(ApiError(CasError::Forbidden(
            "audit log requires owner or admin role".into(),
        )));
    }

    let repo = state.audit.as_deref().ok_or_else(|| {
        ApiError(CasError::Store("audit log is not available on this backend".into()))
    })?;

    let limit = crate::api::pagination::clamp_limit(query.limit);
    Ok(Json(repo.list(&auth.tenant_id, limit)?))
}
