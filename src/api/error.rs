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
