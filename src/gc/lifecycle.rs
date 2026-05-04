//! S3 bucket lifecycle policy management (E11-S03).
//!
//! Applies and retrieves S3 [lifecycle configuration rules] on an Axiom
//! storage bucket.  Three categories of rules are supported:
//!
//! 1. **Deleted-objects expiration** — objects under the `deleted/` prefix are
//!    permanently deleted after a configurable number of days (default 7).
//! 2. **Incomplete multipart upload cancellation** — S3 automatically aborts
//!    any multipart upload that has not been completed within a configurable
//!    number of days (default 1).
//! 3. **Storage-class tiering** *(optional)* — live chunks are transitioned
//!    from `STANDARD` → `STANDARD_IA` after 30 days, then to `GLACIER` after
//!    90 days. Disabled by default; enable by setting
//!    [`TieringConfig::enabled`] to `true`.
//!
//! # Usage
//!
//! ```no_run
//! # async fn example() {
//! use axiom_core::store::s3::{S3Config, StaticCredentials};
//! use axiom_core::gc::lifecycle::{LifecyclePolicyConfig, S3LifecycleManager};
//!
//! let s3_cfg = S3Config::new(
//!     "my-bucket",
//!     "us-east-1",
//!     None,
//!     Some(StaticCredentials { access_key_id: "key".into(), secret_access_key: "secret".into() }),
//!     false,
//! );
//! let manager = S3LifecycleManager::new(s3_cfg);
//! manager.apply_lifecycle_policy("my-bucket", &LifecyclePolicyConfig::default()).await.unwrap();
//! # }
//! ```
//!
//! [lifecycle configuration rules]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html

use std::sync::Arc;

use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
    LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, Transition, TransitionStorageClass,
};
use aws_sdk_s3::Client;

use crate::error::{CasError, CasResult};
use crate::store::s3::{S3Config, StaticCredentials};

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Optional storage-class tiering policy.
///
/// When enabled, live chunks under `chunks/` are transitioned:
/// - `STANDARD` → `STANDARD_IA` after [`TieringConfig::days_to_ia`] days.
/// - `STANDARD_IA` → `GLACIER` after [`TieringConfig::days_to_glacier`] days.
///
/// Both values count from the day the object was **created** (not from the
/// previous transition).  AWS requires `days_to_ia ≥ 30` for `STANDARD_IA`
/// and `days_to_glacier > days_to_ia`.
#[derive(Clone, Debug)]
pub struct TieringConfig {
    /// Whether storage-class tiering is active.  When `false` (the default)
    /// no transition rules are applied and the other fields are ignored.
    pub enabled: bool,
    /// Days after creation to move live chunks to `STANDARD_IA` (min 30).
    pub days_to_ia: i32,
    /// Days after creation to move live chunks to `GLACIER`
    /// (must be greater than `days_to_ia`).
    pub days_to_glacier: i32,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            days_to_ia: 30,
            days_to_glacier: 90,
        }
    }
}

/// Full lifecycle policy configuration for an Axiom bucket.
#[derive(Clone, Debug)]
pub struct LifecyclePolicyConfig {
    /// Number of days before objects in the `deleted/` prefix are permanently
    /// removed by S3.  Provides a recovery window (default 7 days).
    pub deleted_objects_expiration_days: i32,
    /// Number of days after which S3 aborts incomplete multipart uploads
    /// (default 1).
    pub abort_incomplete_multipart_days: i32,
    /// Optional storage-class tiering for live chunk objects.
    pub tiering: TieringConfig,
}

impl Default for LifecyclePolicyConfig {
    fn default() -> Self {
        Self {
            deleted_objects_expiration_days: 7,
            abort_incomplete_multipart_days: 1,
            tiering: TieringConfig::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// S3LifecycleManager
// ---------------------------------------------------------------------------

/// Applies and retrieves S3 lifecycle rules for an Axiom storage bucket.
///
/// Internally wraps an AWS SDK [`Client`].  Construct with
/// [`S3LifecycleManager::new`] and share via `Arc` across tasks.
pub struct S3LifecycleManager {
    client: Arc<Client>,
}

impl S3LifecycleManager {
    /// Create a manager from an [`S3Config`].
    ///
    /// Must be called from within a Tokio runtime (AWS SDK initialises
    /// asynchronously during client construction).
    pub fn new(cfg: S3Config) -> Self {
        let client = build_client(cfg);
        Self {
            client: Arc::new(client),
        }
    }

    /// Create from an already-constructed [`Client`].
    pub fn from_client(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Apply (replace) the lifecycle policy on `bucket`.
    ///
    /// This is an idempotent `PUT` — calling it multiple times with the same
    /// config is safe and only the last call takes effect.
    ///
    /// # Rules applied
    ///
    /// | Rule ID | Prefix | Action |
    /// |---------|--------|--------|
    /// | `axiom-deleted-expiry` | `deleted/` | Expire after N days |
    /// | `axiom-abort-mpu` | *(all)* | Abort incomplete MPU after N days |
    /// | `axiom-tiering-ia` *(if enabled)* | `chunks/` | Transition to IA |
    /// | `axiom-tiering-glacier` *(if enabled)* | `chunks/` | Transition to Glacier |
    pub async fn apply_lifecycle_policy(
        &self,
        bucket: &str,
        config: &LifecyclePolicyConfig,
    ) -> CasResult<()> {
        let rules = build_rules(config)?;

        let lc = BucketLifecycleConfiguration::builder()
            .set_rules(Some(rules))
            .build()
            .map_err(|e| CasError::Store(format!("S3 lifecycle config build error: {e}")))?;

        self.client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(lc)
            .send()
            .await
            .map_err(|e| CasError::Store(format!("S3 put_bucket_lifecycle_configuration: {e}")))?;

        tracing::info!(bucket, "S3 lifecycle policy applied successfully");
        Ok(())
    }

    /// Retrieve the current lifecycle configuration for `bucket`.
    ///
    /// Returns `None` if no lifecycle configuration exists.
    pub async fn get_lifecycle_policy(
        &self,
        bucket: &str,
    ) -> CasResult<Option<Vec<LifecycleRule>>> {
        match self
            .client
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await
        {
            Ok(resp) => Ok(Some(resp.rules().to_vec())),
            Err(e) => {
                let svc_err = e.into_service_error();
                // AWS returns NoSuchLifecycleConfiguration when no rules exist.
                if svc_err.is_no_such_lifecycle_configuration() {
                    Ok(None)
                } else {
                    Err(CasError::Store(format!(
                        "S3 get_bucket_lifecycle_configuration: {svc_err}"
                    )))
                }
            }
        }
    }

    /// Remove all lifecycle rules from `bucket`.
    ///
    /// After this call the bucket has no lifecycle policy.  A no-op if the
    /// bucket already has no lifecycle configuration.
    pub async fn delete_lifecycle_policy(&self, bucket: &str) -> CasResult<()> {
        self.client
            .delete_bucket_lifecycle()
            .bucket(bucket)
            .send()
            .await
            .map_err(|e| CasError::Store(format!("S3 delete_bucket_lifecycle: {e}")))?;

        tracing::info!(bucket, "S3 lifecycle policy deleted");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Rule builders
// ---------------------------------------------------------------------------

fn build_rules(config: &LifecyclePolicyConfig) -> CasResult<Vec<LifecycleRule>> {
    let mut rules: Vec<LifecycleRule> = Vec::new();

    // ── Rule 1: Expire objects under deleted/ after N days ──────────────────
    let deleted_expiry = LifecycleRule::builder()
        .id("axiom-deleted-expiry")
        .filter(
            LifecycleRuleFilter::builder()
                .prefix("deleted/")
                .build(),
        )
        .expiration(
            LifecycleExpiration::builder()
                .days(config.deleted_objects_expiration_days)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .map_err(|e| CasError::Store(format!("build rule axiom-deleted-expiry: {e}")))?;

    rules.push(deleted_expiry);

    // ── Rule 2: Abort incomplete multipart uploads ───────────────────────────
    let abort_mpu = LifecycleRule::builder()
        .id("axiom-abort-mpu")
        .filter(
            LifecycleRuleFilter::builder()
                .prefix("")   // applies to all objects
                .build(),
        )
        .abort_incomplete_multipart_upload(
            AbortIncompleteMultipartUpload::builder()
                .days_after_initiation(config.abort_incomplete_multipart_days)
                .build(),
        )
        .status(ExpirationStatus::Enabled)
        .build()
        .map_err(|e| CasError::Store(format!("build rule axiom-abort-mpu: {e}")))?;

    rules.push(abort_mpu);

    // ── Rules 3 & 4: Optional storage-class tiering for live chunks ──────────
    if config.tiering.enabled {
        validate_tiering_config(&config.tiering)?;

        // 3a. STANDARD → STANDARD_IA
        let tiering_ia = LifecycleRule::builder()
            .id("axiom-tiering-ia")
            .filter(
                LifecycleRuleFilter::builder()
                    .prefix("chunks/")
                    .build(),
            )
            .transitions(
                Transition::builder()
                    .days(config.tiering.days_to_ia)
                    .storage_class(TransitionStorageClass::StandardIa)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .map_err(|e| CasError::Store(format!("build rule axiom-tiering-ia: {e}")))?;

        rules.push(tiering_ia);

        // 3b. STANDARD_IA → GLACIER
        let tiering_glacier = LifecycleRule::builder()
            .id("axiom-tiering-glacier")
            .filter(
                LifecycleRuleFilter::builder()
                    .prefix("chunks/")
                    .build(),
            )
            .transitions(
                Transition::builder()
                    .days(config.tiering.days_to_glacier)
                    .storage_class(TransitionStorageClass::Glacier)
                    .build(),
            )
            .status(ExpirationStatus::Enabled)
            .build()
            .map_err(|e| CasError::Store(format!("build rule axiom-tiering-glacier: {e}")))?;

        rules.push(tiering_glacier);
    }

    Ok(rules)
}

fn validate_tiering_config(t: &TieringConfig) -> CasResult<()> {
    if t.days_to_ia < 30 {
        return Err(CasError::Store(format!(
            "TieringConfig: days_to_ia must be ≥ 30 (got {})",
            t.days_to_ia
        )));
    }
    if t.days_to_glacier <= t.days_to_ia {
        return Err(CasError::Store(format!(
            "TieringConfig: days_to_glacier ({}) must be greater than days_to_ia ({})",
            t.days_to_glacier, t.days_to_ia
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_client(cfg: S3Config) -> Client {
    let mut s3_builder = S3ConfigBuilder::new()
        .region(Region::new(cfg.region.clone()))
        .force_path_style(cfg.force_path_style);

    if let Some(endpoint) = cfg.endpoint_url {
        s3_builder = s3_builder.endpoint_url(endpoint);
    }

    if let Some(creds) = cfg.credentials {
        let provider = SharedCredentialsProvider::new(Credentials::new(
            creds.access_key_id,
            creds.secret_access_key,
            None,
            None,
            "axiom-lifecycle-credentials",
        ));
        s3_builder = s3_builder.credentials_provider(provider);
    }

    Client::from_conf(s3_builder.build())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // LifecyclePolicyConfig defaults
    // ------------------------------------------------------------------

    #[test]
    fn test_default_config() {
        let cfg = LifecyclePolicyConfig::default();
        assert_eq!(cfg.deleted_objects_expiration_days, 7);
        assert_eq!(cfg.abort_incomplete_multipart_days, 1);
        assert!(!cfg.tiering.enabled);
        assert_eq!(cfg.tiering.days_to_ia, 30);
        assert_eq!(cfg.tiering.days_to_glacier, 90);
    }

    // ------------------------------------------------------------------
    // build_rules — without tiering
    // ------------------------------------------------------------------

    #[test]
    fn test_build_rules_no_tiering() {
        let cfg = LifecyclePolicyConfig::default();
        let rules = build_rules(&cfg).unwrap();
        assert_eq!(rules.len(), 2);

        let ids: Vec<_> = rules.iter().map(|r| r.id().unwrap_or("")).collect();
        assert!(ids.contains(&"axiom-deleted-expiry"));
        assert!(ids.contains(&"axiom-abort-mpu"));
    }

    #[test]
    fn test_deleted_expiry_rule_prefix() {
        let cfg = LifecyclePolicyConfig::default();
        let rules = build_rules(&cfg).unwrap();
        let rule = rules.iter().find(|r| r.id() == Some("axiom-deleted-expiry")).unwrap();

        // Should target the deleted/ prefix.
        let filter = rule.filter().unwrap();
        assert_eq!(filter.prefix(), Some("deleted/"));

        // Days should match config.
        let expiry = rule.expiration().unwrap();
        assert_eq!(expiry.days(), Some(7));

        assert_eq!(rule.status(), &ExpirationStatus::Enabled);
    }

    #[test]
    fn test_abort_mpu_rule() {
        let cfg = LifecyclePolicyConfig::default();
        let rules = build_rules(&cfg).unwrap();
        let rule = rules.iter().find(|r| r.id() == Some("axiom-abort-mpu")).unwrap();

        let abort = rule.abort_incomplete_multipart_upload().unwrap();
        assert_eq!(abort.days_after_initiation(), Some(1));
        assert_eq!(rule.status(), &ExpirationStatus::Enabled);
    }

    #[test]
    fn test_custom_expiry_days() {
        let cfg = LifecyclePolicyConfig {
            deleted_objects_expiration_days: 14,
            abort_incomplete_multipart_days: 2,
            tiering: TieringConfig::default(),
        };
        let rules = build_rules(&cfg).unwrap();
        let expiry_rule = rules.iter().find(|r| r.id() == Some("axiom-deleted-expiry")).unwrap();
        assert_eq!(expiry_rule.expiration().unwrap().days(), Some(14));

        let mpu_rule = rules.iter().find(|r| r.id() == Some("axiom-abort-mpu")).unwrap();
        assert_eq!(
            mpu_rule.abort_incomplete_multipart_upload().unwrap().days_after_initiation(),
            Some(2)
        );
    }

    // ------------------------------------------------------------------
    // build_rules — with tiering
    // ------------------------------------------------------------------

    #[test]
    fn test_build_rules_with_tiering() {
        let cfg = LifecyclePolicyConfig {
            tiering: TieringConfig {
                enabled: true,
                days_to_ia: 30,
                days_to_glacier: 90,
            },
            ..LifecyclePolicyConfig::default()
        };
        let rules = build_rules(&cfg).unwrap();
        assert_eq!(rules.len(), 4);

        let ids: Vec<_> = rules.iter().map(|r| r.id().unwrap_or("")).collect();
        assert!(ids.contains(&"axiom-tiering-ia"));
        assert!(ids.contains(&"axiom-tiering-glacier"));
    }

    #[test]
    fn test_tiering_ia_rule() {
        let cfg = LifecyclePolicyConfig {
            tiering: TieringConfig { enabled: true, days_to_ia: 30, days_to_glacier: 90 },
            ..LifecyclePolicyConfig::default()
        };
        let rules = build_rules(&cfg).unwrap();
        let rule = rules.iter().find(|r| r.id() == Some("axiom-tiering-ia")).unwrap();

        let transitions = rule.transitions();
        assert_eq!(transitions.len(), 1);
        assert_eq!(transitions[0].days(), Some(30));
        assert_eq!(transitions[0].storage_class(), Some(&TransitionStorageClass::StandardIa));
        assert_eq!(rule.filter().unwrap().prefix(), Some("chunks/"));
    }

    #[test]
    fn test_tiering_glacier_rule() {
        let cfg = LifecyclePolicyConfig {
            tiering: TieringConfig { enabled: true, days_to_ia: 30, days_to_glacier: 90 },
            ..LifecyclePolicyConfig::default()
        };
        let rules = build_rules(&cfg).unwrap();
        let rule = rules.iter().find(|r| r.id() == Some("axiom-tiering-glacier")).unwrap();

        let transitions = rule.transitions();
        assert_eq!(transitions.len(), 1);
        assert_eq!(transitions[0].days(), Some(90));
        assert_eq!(transitions[0].storage_class(), Some(&TransitionStorageClass::Glacier));
    }

    // ------------------------------------------------------------------
    // validate_tiering_config
    // ------------------------------------------------------------------

    #[test]
    fn test_tiering_validation_ia_too_short() {
        let t = TieringConfig { enabled: true, days_to_ia: 15, days_to_glacier: 90 };
        assert!(validate_tiering_config(&t).is_err());
    }

    #[test]
    fn test_tiering_validation_glacier_not_greater() {
        let t = TieringConfig { enabled: true, days_to_ia: 30, days_to_glacier: 30 };
        assert!(validate_tiering_config(&t).is_err());
    }

    #[test]
    fn test_tiering_validation_valid() {
        let t = TieringConfig { enabled: true, days_to_ia: 30, days_to_glacier: 91 };
        assert!(validate_tiering_config(&t).is_ok());
    }

    #[test]
    fn test_tiering_validation_error_skipped_when_disabled() {
        // Invalid values but tiering is disabled, so build_rules skips validation.
        let cfg = LifecyclePolicyConfig {
            tiering: TieringConfig { enabled: false, days_to_ia: 1, days_to_glacier: 1 },
            ..LifecyclePolicyConfig::default()
        };
        let rules = build_rules(&cfg).unwrap();
        assert_eq!(rules.len(), 2); // no tiering rules
    }
}
