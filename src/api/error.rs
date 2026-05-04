//! Centralized HTTP error mapping.
//!
//! Converts domain `CasError` variants into consistent JSON error responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::error::CasError;

/// Structured error response body.
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
}

/// Wrapper that implements `IntoResponse` for `CasError`.
pub struct ApiError(pub CasError);

impl From<CasError> for ApiError {
    fn from(err: CasError) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            CasError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            CasError::AlreadyExists => (StatusCode::CONFLICT, "already_exists"),
            CasError::HashMismatch => (StatusCode::BAD_REQUEST, "hash_mismatch"),
            CasError::InvalidObject(_) => (StatusCode::BAD_REQUEST, "invalid_object"),
            CasError::InvalidRef(_) => (StatusCode::BAD_REQUEST, "invalid_ref"),
            CasError::Store(_) => (StatusCode::INTERNAL_SERVER_ERROR, "store_error"),
            CasError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
            CasError::Forbidden(_) => (StatusCode::FORBIDDEN, "forbidden"),
            CasError::TenantNotFound(_) => (StatusCode::NOT_FOUND, "tenant_not_found"),
            CasError::WorkspaceNotFound(_) => (StatusCode::NOT_FOUND, "workspace_not_found"),
            CasError::RateLimitExceeded(_) => (StatusCode::TOO_MANY_REQUESTS, "rate_limit_exceeded"),
            CasError::QuotaExceeded(_) => (StatusCode::PAYMENT_REQUIRED, "quota_exceeded"),
            CasError::SyncError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "sync_error"),
            CasError::NonFastForward(_) => (StatusCode::CONFLICT, "non_fast_forward"),
            CasError::SerdeJson(_) => (StatusCode::BAD_REQUEST, "serialization_error"),
            CasError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "io_error"),
        };

        let body = ErrorResponse {
            error: error_type.to_string(),
            message: self.0.to_string(),
        };

        (status, Json(body)).into_response()
    }
}

/// Convenient result type for handlers.
pub type ApiResult<T> = Result<T, ApiError>;
