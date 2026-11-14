//! Axum HTTP API for axiom-core.
//!
//! This module provides the application bootstrap, shared state, router
//! composition, DTOs, and centralized error mapping for the v0.1 POC.

pub mod state;
pub mod error;
pub mod dto;
pub mod pagination;
pub mod routes;

use std::time::Duration;

use axum::Router;
use axum::http::{HeaderValue, Method, header};
use axum::response::Html;
use axum::routing::get;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::auth::middleware::{JwtAuthLayer, TrustedGatewayAuthLayer};
use crate::telemetry;

use state::{AppState, HttpAuthMode};

/// Machine-readable contract: the checked-in OpenAPI 3.1 document.
///
/// It is compiled into the binary so the spec can never drift from the
/// deployed build without a code change (P2-5).
async fn openapi_spec() -> impl axum::response::IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        include_str!("../../openapi/axiom-v1.json"),
    )
}

/// Minimal human-readable index (no external CDN, CSP-friendly).
async fn api_docs() -> axum::response::Html<&'static str> {
    axum::response::Html(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>Axiom API</title>
<style>body{font-family:system-ui,sans-serif;max-width:760px;margin:48px auto;padding:0 16px;line-height:1.5}
code{background:#f3f3f3;padding:2px 6px;border-radius:4px}h1{font-size:24px}</style></head>
<body><h1>Axiom API v0.5</h1>
<p>The OpenAPI 3.1 contract is served at <a href="/openapi.json"><code>/openapi.json</code></a>.
Import it into Postman, Insomnia, or any OpenAPI tooling.</p>
<ul>
<li>Auth: <code>/auth/login</code>, <code>/auth/refresh</code>, <code>/auth/logout</code>, <code>/auth/jwks</code></li>
<li>Workspaces: <code>/api/v1/workspaces</code></li>
<li>Versions and refs: <code>/api/v1/versions/{id}/history</code>, <code>/api/v1/refs</code></li>
<li>Transfer: <code>/api/v1/upload/file</code>, <code>/api/v1/version/{ref}/file/{path}</code></li>
<li>Quota and audit: <code>/api/v1/usage</code>, <code>/api/v1/audit</code></li>
</ul>
<p>Errors always use <code>{error, code, message, request_id, retryable}</code>.</p>
</body></html>"#,
    )
}

/// Build the CORS layer from an explicit allowlist.
///
/// Only the methods and headers the API actually uses are permitted, and the
/// allowed origins are echoed back instead of `*` so browser credentials and
/// `Vary: Origin` caching behave correctly.
pub fn build_cors_layer(origins: &[String]) -> CorsLayer {
    let parsed: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|origin| match HeaderValue::from_str(origin) {
            Ok(value) => Some(value),
            Err(_) => {
                tracing::warn!(origin, "ignoring invalid CORS origin");
                None
            }
        })
        .collect();

    if parsed.is_empty() {
        tracing::warn!(
            "CORS allowlist is empty: cross-origin browser requests will be rejected"
        );
    }

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(parsed))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::RANGE,
            header::ACCEPT,
        ])
        .expose_headers([
            header::CONTENT_RANGE,
            header::ACCEPT_RANGES,
            header::CONTENT_DISPOSITION,
        ])
        .max_age(Duration::from_secs(600))
}

/// Build the full axum `Router` with all route groups and middleware.
pub fn build_router(state: AppState) -> Router {
    telemetry::init_domain_metrics();

    let cors = build_cors_layer(&state.cors_origins);

    let protected_api = Router::new()
        .nest("/api/v1/objects", routes::objects::router())
        .nest("/api/v1/workspaces", routes::workspaces::router())
        .nest("/api/v1/versions", routes::versions::router())
        .nest("/api/v1/refs", routes::refs::router())
        .nest("/api/v1/diff", routes::diff::router())
        .nest("/api/v1/upload", routes::upload::router())
        .nest("/api/v1", routes::download::router())
        .merge(routes::usage::router())
        .merge(routes::audit::router())
        .route("/api", get(api_index));

    let protected_api = match state.http_auth_mode {
        HttpAuthMode::Disabled => protected_api,
        HttpAuthMode::TrustedGatewayHeaders => {
            let secret = std::env::var("AXIOM_GATEWAY_SECRET").ok();
            if secret.is_none() {
                tracing::warn!(
                    "AXIOM_GATEWAY_SECRET is not set; identity headers are trusted without proof of origin"
                );
            }
            protected_api.layer(TrustedGatewayAuthLayer::with_secret(secret))
        }
        HttpAuthMode::Jwt => {
            let auth = state
                .auth
                .clone()
                .expect("HttpAuthMode::Jwt requires an attached AuthService");
            protected_api.layer(JwtAuthLayer::new(auth))
        }
    };

    Router::new()
        .route("/openapi.json", get(openapi_spec))
        .route("/docs", get(api_docs))
        .merge(routes::health::router())
        // Auth endpoints must stay reachable without a token.
        .merge(crate::auth::routes::router())
        .merge(protected_api)
        .layer(cors)
        .layer(axum::middleware::from_fn(telemetry::observe_http_request))
        .with_state(state)
}

async fn api_index() -> Html<&'static str> {
    Html(include_str!("api_index.html"))
}
