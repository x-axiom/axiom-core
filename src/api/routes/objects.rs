//! Object (chunk) endpoints.

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};

use crate::api::dto::ObjectInfoResponse;
use crate::api::error::{ApiError, ApiResult};
use crate::api::state::AppState;
use crate::store::traits::ChunkStore;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/{hash}", get(get_object_info))
}

async fn get_object_info(
    State(state): State<AppState>,
    Path(hash_hex): Path<String>,
) -> ApiResult<Json<ObjectInfoResponse>> {
    let hash = parse_hash(&hash_hex)?;

    let exists = state.cas.has_chunk(&hash).map_err(ApiError::from)?;
    let size = if exists {
        state
            .cas
            .get_chunk(&hash)
            .map_err(ApiError::from)?
            .map(|d| d.len() as u64)
            .unwrap_or(0)
    } else {
        0
    };

    Ok(Json(ObjectInfoResponse {
        hash: hash_hex,
        size,
        exists,
    }))
}

pub fn parse_hash(hex_str: &str) -> ApiResult<blake3::Hash> {
    let bytes = hex::decode(hex_str).map_err(|e| {
        ApiError(crate::error::CasError::InvalidObject(format!(
            "invalid hex hash: {e}"
        )))
    })?;
    if bytes.len() != 32 {
        return Err(ApiError(crate::error::CasError::InvalidObject(
            "hash must be 32 bytes".into(),
        )));
    }
    let arr: [u8; 32] = bytes.try_into().unwrap();
    Ok(blake3::Hash::from_bytes(arr))
}
