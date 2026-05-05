//! Health check endpoint.

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use crate::api::dto::{HealthCheckResponse, HealthResponse};
use crate::api::state::AppState;
use crate::model::hash::hash_bytes;
use crate::tenant::model::OrgId;
use crate::telemetry;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health_check))
        .route("/live", get(live_check))
        .route("/ready", get(ready_check))
        .route("/metrics", get(metrics_endpoint))
}

async fn health_check(State(state): State<AppState>) -> Response {
    readiness_response(&state)
}

async fn live_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        checks: None,
    })
}

async fn ready_check(State(state): State<AppState>) -> Response {
    readiness_response(&state)
}

async fn metrics_endpoint() -> Response {
    match telemetry::render_prometheus() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(HealthResponse {
                status: "error".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                checks: Some(vec![HealthCheckResponse {
                    name: "metrics".into(),
                    status: "error".into(),
                    message: Some(err.to_string()),
                }]),
            }),
        )
            .into_response(),
    }
}

fn readiness_response(state: &AppState) -> Response {
    let checks = readiness_checks(state);
    let ready = checks.iter().all(|check| check.status == "ok");
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(HealthResponse {
            status: if ready { "ok" } else { "degraded" }.into(),
            version: env!("CARGO_PKG_VERSION").into(),
            checks: Some(checks),
        }),
    )
        .into_response()
}

fn readiness_checks(state: &AppState) -> Vec<HealthCheckResponse> {
    let mut checks = Vec::new();

    checks.push(check_result(
        "metadata_refs",
        state.refs.list_refs(None).map(|_| ()),
    ));

    let readiness_hash = hash_bytes(b"axiom-ready-probe");
    checks.push(check_result(
        "chunk_store",
        state.chunks.has_chunk(&readiness_hash).map(|_| ()),
    ));

    if let Some(workspaces) = state.workspaces.as_deref() {
        checks.push(check_result(
            "workspace_repo",
            workspaces.list_workspaces().map(|_| ()),
        ));
    }

    if let Some(directory) = state.tenant_directory.as_deref() {
        checks.push(check_result(
            "tenant_directory",
            directory
                .list_workspaces_by_org(&OrgId::from("axiom-ready-probe"))
                .map(|_| ()),
        ));
    }

    checks
}

fn check_result(name: &str, result: crate::error::CasResult<()>) -> HealthCheckResponse {
    match result {
        Ok(()) => HealthCheckResponse {
            name: name.into(),
            status: "ok".into(),
            message: None,
        },
        Err(err) => HealthCheckResponse {
            name: name.into(),
            status: "error".into(),
            message: Some(err.to_string()),
        },
    }
}
