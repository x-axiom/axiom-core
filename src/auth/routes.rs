//! Public authentication endpoints (`/auth/*`).
//!
//! These routes are intentionally mounted **outside** the trusted-gateway
//! middleware: a caller without a token must be able to reach `login`,
//! `refresh`, and `jwks`.
//!
//! Error contract:
//! - `401 invalid_token` / `401 token_expired`
//! - `403 forbidden`
//! - `503 auth_unavailable` (deployment not configured for auth)

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::auth::jwt::JwkSet;
use crate::auth::service::{AuthService, TokenPair};
use crate::error::CasError;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/refresh", post(refresh))
        .route("/auth/logout", post(logout))
        .route("/auth/session", get(session))
        .route("/auth/jwks", get(jwks))
        .route("/.well-known/jwks.json", get(jwks))
        .route("/auth/oauth/github", get(oauth_start))
        .route("/auth/oauth/github/callback", get(oauth_callback))
}

// ---------------------------------------------------------------------------
// Service lookup
// ---------------------------------------------------------------------------

fn service(state: &AppState) -> Result<&Arc<AuthService>, ApiError> {
    state.auth.as_ref().ok_or_else(|| {
        ApiError(CasError::Store(
            "auth_unavailable: this deployment has no auth service configured".into(),
        ))
    })
}

// ---------------------------------------------------------------------------
// Request / response shapes
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct LoginRequest {
    /// `github` (default) or `dev` (local development only).
    #[serde(default = "default_provider")]
    provider: String,
    /// GitHub access token obtained by the web console via NextAuth.
    access_token: Option<String>,
    /// GitHub OAuth authorization code (server-side flow).
    code: Option<String>,
    /// Dev-provider identity.
    email: Option<String>,
    name: Option<String>,
}

fn default_provider() -> String {
    "github".to_owned()
}

#[derive(Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Deserialize)]
pub struct LogoutRequest {
    pub refresh_token: String,
}

#[derive(Deserialize)]
struct OAuthCallbackQuery {
    code: String,
    state: String,
}

#[derive(Serialize)]
struct LogoutResponse {
    revoked: bool,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> ApiResult<Json<TokenPair>> {
    let auth = service(&state)?;

    let pair = match req.provider.as_str() {
        "github" => {
            if let Some(code) = req.code.as_deref() {
                auth.login_github_code(code).await?
            } else if let Some(token) = req.access_token.as_deref() {
                auth.login_github_token(token).await?
            } else {
                return Err(ApiError(CasError::InvalidObject(
                    "login requires either access_token or code for provider=github".into(),
                )));
            }
        }
        "dev" => {
            let email = req.email.as_deref().ok_or_else(|| {
                ApiError(CasError::InvalidObject("dev login requires email".into()))
            })?;
            let name = req.name.as_deref().unwrap_or(email);
            let presented = headers
                .get("x-axiom-dev-secret")
                .and_then(|v| v.to_str().ok());
            auth.dev_login(email, name, presented)?
        }
        other => {
            return Err(ApiError(CasError::InvalidObject(format!(
                "unsupported login provider '{other}'"
            ))));
        }
    };

    Ok(Json(pair))
}

async fn refresh(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> ApiResult<Json<TokenPair>> {
    let auth = service(&state)?;
    Ok(Json(auth.refresh(&req.refresh_token)?))
}

async fn logout(
    State(state): State<AppState>,
    Json(req): Json<LogoutRequest>,
) -> ApiResult<Json<LogoutResponse>> {
    let auth = service(&state)?;
    auth.logout(&req.refresh_token)?;
    Ok(Json(LogoutResponse { revoked: true }))
}

async fn jwks(State(state): State<AppState>) -> ApiResult<Json<JwkSet>> {
    let auth = service(&state)?;
    Ok(Json(auth.jwks()))
}

async fn session(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(auth) = state.auth.as_ref() else {
        return unavailable().into_response();
    };

    let Some(token) = bearer(&headers) else {
        return ApiError(CasError::Unauthorized("missing bearer token".into())).into_response();
    };

    match auth.verify_access(&token) {
        Ok(claims) => Json(serde_json::json!({
            "user_id": claims.sub,
            "tenant_id": claims.tenant_id,
            "org_id": claims.org_id,
            "roles": claims.roles,
            "exp": claims.exp,
        }))
        .into_response(),
        Err(err) => ApiError(err).into_response(),
    }
}

async fn oauth_start(State(state): State<AppState>) -> Response {
    let Some(auth) = state.auth.as_ref() else {
        return unavailable().into_response();
    };

    let state_token = match auth.issue_state() {
        Ok(token) => token,
        Err(err) => return ApiError(err).into_response(),
    };

    match auth.authorize_url(&state_token) {
        Ok(url) => Redirect::to(&url).into_response(),
        Err(err) => ApiError(err).into_response(),
    }
}

async fn oauth_callback(
    State(state): State<AppState>,
    Query(query): Query<OAuthCallbackQuery>,
) -> Response {
    let Some(auth) = state.auth.as_ref() else {
        return unavailable().into_response();
    };

    if let Err(err) = auth.verify_state(&query.state) {
        return ApiError(err).into_response();
    }

    match auth.login_github_code(&query.code).await {
        Ok(pair) => Json(pair).into_response(),
        Err(err) => ApiError(err).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
}

fn unavailable() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": "auth_unavailable",
            "code": "auth_unavailable",
            "message": "this deployment has no auth service configured"
        })),
    )
}
