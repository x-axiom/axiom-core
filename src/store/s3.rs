//! S3-backed [`ChunkStore`] implementation.
//!
//! Supports both AWS S3 and S3-compatible endpoints (Cloudflare R2, MinIO, …)
//! via the `endpoint_url` configuration field.
//!
//! # Key layout
//!
//! ```text
//! chunks/{hash[0..2]}/{hash[2..4]}/{hash_hex}
//! ```
//!
//! The two-level directory prefix distributes objects across S3 prefixes,
//! avoiding request-rate hot-spots on a single shard.
//!
//! # Large-object handling
//!
//! Objects whose **uncompressed data** exceed [`MULTIPART_THRESHOLD`] bytes
//! are uploaded using S3 multipart upload in [`MULTIPART_PART_SIZE`]-byte
//! parts.  Small objects use a single `PutObject` call.
//!
//! # Content integrity
//!
//! Every `PutObject` (and every multipart part) includes a Base64-encoded
//! MD5 digest in the `Content-MD5` header so that S3 can reject corrupted
//! payloads at ingest time.

use std::sync::Arc;

use aws_config::SdkConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use base64::Engine;
use bytes::Bytes;
use md5::{Digest, Md5};

use crate::error::{CasError, CasResult};
use crate::model::hash::{hash_bytes, ChunkHash};
use super::traits::ChunkStore;

// ---------------------------------------------------------------------------
// Thresholds
// ---------------------------------------------------------------------------

/// Objects larger than this are uploaded via multipart (100 MiB).
const MULTIPART_THRESHOLD: usize = 100 * 1024 * 1024;

/// Each multipart part is 10 MiB (S3 minimum is 5 MiB, except for the last part).
const MULTIPART_PART_SIZE: usize = 10 * 1024 * 1024;

// ---------------------------------------------------------------------------
// S3Config
// ---------------------------------------------------------------------------

/// Configuration for the [`S3ChunkStore`].
///
/// Build with [`S3Config::builder`] or use [`S3Config::from_env`] to pick up
/// standard `AWS_*` environment variables.
#[derive(Clone, Debug)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region (e.g. `"us-east-1"`).  For R2 / MinIO pass any non-empty
    /// string — the actual routing is driven by `endpoint_url`.
    pub region: String,
    /// Override the S3 endpoint (for Cloudflare R2, MinIO, etc.).
    /// Leave `None` to use the default AWS endpoint for `region`.
    pub endpoint_url: Option<String>,
    /// Static credentials.  When `None` the SDK falls back to the default
    /// credential chain (env vars, ~/.aws/credentials, EC2 instance profile).
    pub credentials: Option<StaticCredentials>,
    /// Force path-style addressing (`/{bucket}/{key}` instead of
    /// `{bucket}.s3.amazonaws.com/{key}`).  Required for MinIO.
    pub force_path_style: bool,
}

/// Plain static AWS credentials.
#[derive(Clone, Debug)]
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl S3Config {
    /// Construct from explicit values.  This is the most portable constructor
    /// and is what integration tests use (pointed at a local MinIO container).
    pub fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint_url: Option<String>,
        credentials: Option<StaticCredentials>,
        force_path_style: bool,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint_url,
            credentials,
            force_path_style,
        }
    }
}

// ---------------------------------------------------------------------------
// S3ChunkStore
// ---------------------------------------------------------------------------

/// Content-addressed chunk store backed by S3 (or any S3-compatible service).
///
/// The internal [`Client`] is wrapped in an `Arc` so `S3ChunkStore` is cheap
/// to clone.  All operations are synchronous from the caller's perspective but
/// internally use Tokio's current-thread executor via `tokio::runtime::Handle`.
///
/// # Thread safety
///
/// `S3ChunkStore` is `Send + Sync`.  The AWS SDK client already manages its
/// own connection pool internally.
pub struct S3ChunkStore {
    client: Arc<Client>,
    bucket: String,
}

impl S3ChunkStore {
    /// Create a new `S3ChunkStore` from an explicit [`S3Config`].
    ///
    /// This must be called from within a Tokio runtime context because the AWS
    /// SDK performs async initialisation during client construction.
    pub fn new(cfg: S3Config) -> Self {
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
                "axiom-static-credentials",
            ));
            s3_builder = s3_builder.credentials_provider(provider);
        }

        let s3_cfg = s3_builder.build();
        let client = Client::from_conf(s3_cfg);

        Self {
            client: Arc::new(client),
            bucket: cfg.bucket,
        }
    }

    /// Create from an already-loaded [`SdkConfig`] (e.g. from `aws_config::load_from_env`).
    pub fn from_sdk_config(sdk_cfg: &SdkConfig, bucket: impl Into<String>) -> Self {
        let client = Client::new(sdk_cfg);
        Self {
            client: Arc::new(client),
            bucket: bucket.into(),
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Compute the S3 object key for a given hash.
    fn object_key(hash: &ChunkHash) -> String {
        let hex = hash.to_hex().to_string();
        // Two-level prefix to spread objects across S3 shards.
        format!("chunks/{}/{}/{}", &hex[..2], &hex[2..4], hex)
    }

    /// Compute Base64-encoded MD5 of `data` for use in `Content-MD5`.
    fn content_md5(data: &[u8]) -> String {
        let digest = Md5::digest(data);
        base64::engine::general_purpose::STANDARD.encode(digest)
    }

    /// Upload `data` to `key` using a single `PutObject` call.
    fn put_single(
        &self,
        key: &str,
        data: &[u8],
        rt: &tokio::runtime::Handle,
    ) -> CasResult<()> {
        let md5 = Self::content_md5(data);
        let body = ByteStream::from(Bytes::copy_from_slice(data));

        rt.block_on(
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .content_type("application/octet-stream")
                .content_md5(md5)
                .body(body)
                .send(),
        )
        .map_err(|e| CasError::Store(format!("S3 PutObject failed: {e}")))?;

        Ok(())
    }

    /// Upload `data` to `key` using S3 multipart upload (for large objects).
    fn put_multipart(
        &self,
        key: &str,
        data: &[u8],
        rt: &tokio::runtime::Handle,
    ) -> CasResult<()> {
        // 1. Initiate
        let create_out: CreateMultipartUploadOutput = rt
            .block_on(
                self.client
                    .create_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .content_type("application/octet-stream")
                    .send(),
            )
            .map_err(|e| CasError::Store(format!("S3 CreateMultipartUpload failed: {e}")))?;

        let upload_id = create_out
            .upload_id()
            .ok_or_else(|| CasError::Store("S3 CreateMultipartUpload returned no upload_id".into()))?
            .to_owned();

        // 2. Upload parts
        let mut completed_parts: Vec<CompletedPart> = Vec::new();
        let mut part_number: i32 = 1;

        for chunk in data.chunks(MULTIPART_PART_SIZE) {
            let md5 = Self::content_md5(chunk);
            let body = ByteStream::from(Bytes::copy_from_slice(chunk));

            let upload_out = rt
                .block_on(
                    self.client
                        .upload_part()
                        .bucket(&self.bucket)
                        .key(key)
                        .upload_id(&upload_id)
                        .part_number(part_number)
                        .content_md5(md5)
                        .body(body)
                        .send(),
                )
                .map_err(|e| {
                    // Abort on part failure — best-effort cleanup.
                    let _ = rt.block_on(
                        self.client
                            .abort_multipart_upload()
                            .bucket(&self.bucket)
                            .key(key)
                            .upload_id(&upload_id)
                            .send(),
                    );
                    CasError::Store(format!("S3 UploadPart {part_number} failed: {e}"))
                })?;

            let etag = upload_out
                .e_tag()
                .ok_or_else(|| CasError::Store(format!("S3 UploadPart {part_number} returned no ETag")))?
                .to_owned();

            completed_parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            );
            part_number += 1;
        }

        // 3. Complete
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        rt.block_on(
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(completed)
                .send(),
        )
        .map_err(|e| CasError::Store(format!("S3 CompleteMultipartUpload failed: {e}")))?;

        Ok(())
    }

    /// Get a Tokio handle, panicking with a clear message if not in a runtime.
    fn handle() -> tokio::runtime::Handle {
        tokio::runtime::Handle::try_current()
            .expect("S3ChunkStore requires a Tokio runtime context")
    }
}

// ---------------------------------------------------------------------------
// ChunkStore impl
// ---------------------------------------------------------------------------

impl ChunkStore for S3ChunkStore {
    fn put_chunk(&self, data: Vec<u8>) -> CasResult<ChunkHash> {
        let hash = hash_bytes(&data);
        let key = Self::object_key(&hash);
        let rt = Self::handle();

        // Idempotent: skip upload if the object already exists.
        let exists: bool = rt
            .block_on(
                self.client
                    .head_object()
                    .bucket(&self.bucket)
                    .key(&key)
                    .send(),
            )
            .map(|_| true)
            .unwrap_or(false);

        if !exists {
            if data.len() > MULTIPART_THRESHOLD {
                self.put_multipart(&key, &data, &rt)?;
            } else {
                self.put_single(&key, &data, &rt)?;
            }
        }

        Ok(hash)
    }

    fn get_chunk(&self, hash: &ChunkHash) -> CasResult<Option<Vec<u8>>> {
        let key = Self::object_key(hash);
        let rt = Self::handle();

        let result = rt.block_on(
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send(),
        );

        match result {
            Ok(out) => {
                let data = rt
                    .block_on(out.body.collect())
                    .map_err(|e| CasError::Store(format!("S3 GetObject body read failed: {e}")))?
                    .into_bytes()
                    .to_vec();
                Ok(Some(data))
            }
            Err(e) => {
                // Distinguish "not found" from other S3 errors.
                let is_not_found = e
                    .as_service_error()
                    .map(|se| se.is_no_such_key())
                    .unwrap_or(false);

                if is_not_found {
                    Ok(None)
                } else {
                    Err(CasError::Store(format!("S3 GetObject failed: {e}")))
                }
            }
        }
    }

    fn has_chunk(&self, hash: &ChunkHash) -> CasResult<bool> {
        let key = Self::object_key(hash);
        let rt = Self::handle();

        let result = rt.block_on(
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send(),
        );

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                let is_not_found = e
                    .as_service_error()
                    .map(|se| se.is_not_found())
                    .unwrap_or(false);
                if is_not_found {
                    Ok(false)
                } else {
                    Err(CasError::Store(format!("S3 HeadObject failed: {e}")))
                }
            }
        }
    }
}
