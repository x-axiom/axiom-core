//! Health check endpoint.

use axum::routing::get;
use axum::{Json, Router};

use crate::api::dto::HealthResponse;
use crate::api::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/health", get(health_check))
}

async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    })
}
