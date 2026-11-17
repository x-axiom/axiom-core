//! Shared helpers used across multiple route modules.

use crate::api::error::ApiError;
use crate::error::CasError;
use crate::model::version::VersionNode;
use crate::model::VersionId;
use crate::store::traits::{RefRepo, VersionRepo};

/// Resolve a ref string (version id / branch / tag) to a full `VersionNode`.
///
/// Tries in order: exact version id → branch/tag ref.
pub fn resolve_version_node(
    ref_str: &str,
    versions: &dyn VersionRepo,
    refs: &dyn RefRepo,
) -> Result<VersionNode, ApiError> {
    // 1. Try as literal version id.
    let vid = VersionId::from(ref_str);
    if let Some(v) = versions.get_version(&vid).map_err(ApiError::from)? {
        return Ok(v);
    }

    // 2. Try as ref (branch or tag).
    if let Some(r) = refs.get_ref(ref_str).map_err(ApiError::from)? {
        let v = versions
            .get_version(&r.target)
            .map_err(ApiError::from)?
            .ok_or_else(|| {
                ApiError(CasError::NotFound(format!(
                    "version '{}' (target of ref '{}') not found",
                    r.target, ref_str
                )))
            })?;
        return Ok(v);
    }

    Err(ApiError(CasError::NotFound(format!(
        "version or ref '{ref_str}' not found"
    ))))
}

/// Run a blocking store call on the blocking pool so it never occupies a
/// Tokio worker (P1-7). Store backends (RocksDB, SQLite, S3 SDK calls behind a
/// blocking facade) are synchronous by design.
pub async fn run_blocking<T, F>(f: F) -> crate::api::error::ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> crate::api::error::ApiResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| {
            crate::api::error::ApiError(crate::error::CasError::Store(format!(
                "blocking task failed: {e}"
            )))
        })?
}
