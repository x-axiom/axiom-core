//! Axum HTTP API for axiom-core.
//!
//! This module provides the application bootstrap, shared state, router
//! composition, DTOs, and centralized error mapping for the v0.1 POC.

pub mod state;
pub mod error;
pub mod dto;
pub mod routes;

use axum::Router;
use tower_http::trace::TraceLayer;

use state::AppState;

/// Build the full axum `Router` with all route groups and middleware.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(routes::health::router())
        .nest("/api/v1/objects", routes::objects::router())
        .nest("/api/v1/versions", routes::versions::router())
        .nest("/api/v1/refs", routes::refs::router())
        .nest("/api/v1/diff", routes::diff::router())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
