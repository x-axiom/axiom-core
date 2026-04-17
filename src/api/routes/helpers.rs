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
    meta: &(impl VersionRepo + RefRepo),
) -> Result<VersionNode, ApiError> {
    // 1. Try as literal version id.
    let vid = VersionId::from(ref_str);
    if let Some(v) = meta.get_version(&vid).map_err(ApiError::from)? {
        return Ok(v);
    }

    // 2. Try as ref (branch or tag).
    if let Some(r) = meta.get_ref(ref_str).map_err(ApiError::from)? {
        let v = meta
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
