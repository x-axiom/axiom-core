//! Axum HTTP API for axiom-core.
//!
//! This module provides the application bootstrap, shared state, router
//! composition, DTOs, and centralized error mapping for the v0.1 POC.

pub mod state;
pub mod error;
pub mod dto;
pub mod routes;

use axum::Router;
use axum::response::Html;
use axum::routing::get;
use tower_http::cors::{Any, CorsLayer};

use crate::auth::middleware::TrustedGatewayAuthLayer;
use crate::telemetry;

use state::{AppState, HttpAuthMode};

/// Build the full axum `Router` with all route groups and middleware.
pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let protected_api = Router::new()
        .nest("/api/v1/objects", routes::objects::router())
        .nest("/api/v1/workspaces", routes::workspaces::router())
        .nest("/api/v1/versions", routes::versions::router())
        .nest("/api/v1/refs", routes::refs::router())
        .nest("/api/v1/diff", routes::diff::router())
        .nest("/api/v1/upload", routes::upload::router())
        .nest("/api/v1", routes::download::router())
        .route("/api", get(api_index));

    let protected_api = match state.http_auth_mode {
        HttpAuthMode::Disabled => protected_api,
        HttpAuthMode::TrustedGatewayHeaders => {
            protected_api.layer(TrustedGatewayAuthLayer::new())
        }
    };

    Router::new()
        .merge(routes::health::router())
        .merge(protected_api)
        .layer(cors)
        .layer(axum::middleware::from_fn(telemetry::observe_http_request))
        .with_state(state)
}

async fn api_index() -> Html<&'static str> {
    Html(include_str!("api_index.html"))
}
