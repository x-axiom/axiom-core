//! Tenant usage / quota endpoint (P1-1).

use axum::extract::{Extension, State};
use axum::routing::get;
use axum::{Json, Router};

use crate::api::error::ApiResult;
use crate::api::state::AppState;
use crate::auth::rbac::{AuthContext, PlanContext};
use crate::quota::{UsageSnapshot, snapshot};

pub fn router() -> Router<AppState> {
    Router::new().route("/api/v1/usage", get(get_usage))
}

async fn get_usage(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    plan: Option<Extension<PlanContext>>,
) -> ApiResult<Json<UsageSnapshot>> {
    let tenant = auth
        .as_ref()
        .map(|Extension(ctx)| ctx.tenant_id.clone())
        .unwrap_or_else(|| "local".to_owned());
    let plan = plan.as_ref().and_then(|Extension(p)| p.0.clone());

    // Without a ledger (quota disabled) report zero usage against the plan cap
    // so clients can always render the meter.
    let used = match state.usage.as_deref() {
        Some(repo) => repo.get(&tenant)?,
        None => 0,
    };

    Ok(Json(UsageSnapshot::new(
        &tenant,
        plan.as_deref(),
        used,
        state.quota.limit_for(plan.as_deref()),
    )))
}
