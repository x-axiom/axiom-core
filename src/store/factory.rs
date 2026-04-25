//! `StoreFactory` — dynamic backend selection for Axiom storage.
//!
//! Selects between the **local** (RocksDB + SQLite) and **cloud**
//! (S3 + FoundationDB + optional DragonflyDB cache) backends based on a
//! [`StoreConfig`] value.
//!
//! # Loading config
//!
//! ```ignore
//! // From a TOML file:
//! let state = StoreFactory::from_file("axiom.toml")?.create()?;
//!
//! // From environment variables:
//! let state = StoreFactory::from_env()?.create()?;
//! ```
//!
//! # TOML format
//!
//! Local backend:
//!
//! ```toml
//! [local]
//! data_dir = ".axiom"
//! ```
//!
//! Cloud backend:
//!
//! ```toml
//! [cloud.s3]
//! bucket            = "my-bucket"
//! region            = "us-east-1"
//! endpoint_url      = "https://..."   # optional — for R2 / MinIO
//! access_key_id     = "..."           # optional — falls back to env chain
//! secret_access_key = "..."           # optional
//! force_path_style  = false
//!
//! [cloud.fdb]
//! cluster_file = "/etc/foundationdb/fdb.cluster"   # optional
//! tenant       = "acme"
//! workspace    = "production"
//!
//! [cloud.cache]          # optional section — omit to disable caching
//! url        = "redis://localhost:6379"
//! pool_size  = 4
//! key_prefix = "axiom"
//! ```
//!
//! # Environment variables
//!
//! | Variable | Description |
//! |----------|-------------|
//! | `AXIOM_BACKEND` | `local` or `cloud` (required) |
//! | `AXIOM_DATA_DIR` | local data directory (default: `.axiom`) |
//! | `AXIOM_S3_BUCKET` | S3 bucket name |
//! | `AXIOM_S3_REGION` | AWS region (default: `us-east-1`) |
//! | `AXIOM_S3_ENDPOINT_URL` | Override endpoint (R2, MinIO) |
//! | `AXIOM_S3_ACCESS_KEY_ID` | Static access key |
//! | `AXIOM_S3_SECRET_ACCESS_KEY` | Static secret key |
//! | `AXIOM_S3_FORCE_PATH_STYLE` | `true`/`false` (default: `false`) |
//! | `AXIOM_FDB_CLUSTER_FILE` | FDB cluster file path |
//! | `AXIOM_FDB_TENANT` | FDB tenant name |
//! | `AXIOM_FDB_WORKSPACE` | FDB workspace name |
//! | `AXIOM_CACHE_URL` | DragonflyDB/Redis URL (omit to disable) |
//! | `AXIOM_CACHE_POOL_SIZE` | Connection pool size (default: `4`) |
//! | `AXIOM_CACHE_KEY_PREFIX` | Key prefix (default: `axiom`) |
//!
//! # Compile-time backend availability
//!
//! The backend variants have feature-gated implementations:
//! - `BackendConfig::Local` requires the `local` Cargo feature.
//! - `BackendConfig::Cloud` requires both `cloud` and `fdb` Cargo features.
//!
//! Attempting to create an unsupported backend at runtime returns
//! `CasError::Store` with a descriptive message instead of compiling that code
//! path away entirely, so that the binary can report a useful error when
//! misconfigured.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::api::state::AppState;
use crate::error::{CasError, CasResult};

// ---------------------------------------------------------------------------
// File-level config structures (TOML-serializable)
// ---------------------------------------------------------------------------

/// Top-level configuration.  Exactly one of `local` or `cloud` must be
/// present (the other must be absent / `None`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreConfig {
    /// Local (RocksDB + SQLite) backend configuration.
    pub local: Option<LocalConfig>,
    /// Cloud (S3 + FoundationDB + optional cache) backend configuration.
    pub cloud: Option<CloudConfig>,
}

impl StoreConfig {
    /// Validate that exactly one backend is configured and return the
    /// equivalent [`BackendConfig`].
    pub fn into_backend(self) -> CasResult<BackendConfig> {
        match (self.local, self.cloud) {
            (Some(l), None) => Ok(BackendConfig::Local(l)),
            (None, Some(c)) => Ok(BackendConfig::Cloud(c)),
            (Some(_), Some(_)) => Err(CasError::Store(
                "StoreConfig: both 'local' and 'cloud' sections are present; specify exactly one".into(),
            )),
            (None, None) => Err(CasError::Store(
                "StoreConfig: neither 'local' nor 'cloud' section found; specify exactly one".into(),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// BackendConfig — the canonical discriminated union
// ---------------------------------------------------------------------------

/// Which storage backend to instantiate.
#[derive(Debug, Clone)]
pub enum BackendConfig {
    /// Local single-process backend: RocksDB for content, SQLite for metadata.
    Local(LocalConfig),
    /// Cloud multi-tenant backend: S3 for content, FoundationDB for metadata,
    /// optional DragonflyDB / Redis cache layer.
    Cloud(CloudConfig),
}

// ---------------------------------------------------------------------------
// LocalConfig
// ---------------------------------------------------------------------------

/// Configuration for the local (RocksDB + SQLite) backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalConfig {
    /// Directory where RocksDB CAS (`cas/`) and `meta.db` are stored.
    /// Defaults to `.axiom` relative to the current working directory.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self { data_dir: default_data_dir() }
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from(".axiom")
}

// ---------------------------------------------------------------------------
// CloudConfig
// ---------------------------------------------------------------------------

/// Configuration for the cloud (S3 + FoundationDB + cache) backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudConfig {
    pub s3: S3FileConfig,
    pub fdb: FdbFileConfig,
    /// Optional cache layer.  Omit the section to disable caching.
    pub cache: Option<CacheFileConfig>,
}

// ---- S3 ----

/// TOML / env representation of S3 settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3FileConfig {
    pub bucket: String,
    /// AWS region. Defaults to `"us-east-1"`.
    #[serde(default = "default_region")]
    pub region: String,
    /// Override endpoint URL (for Cloudflare R2, MinIO, etc.).
    pub endpoint_url: Option<String>,
    /// Static access key ID.  When absent, falls back to the AWS SDK default
    /// credential chain (env vars → ~/.aws/credentials → EC2 profile).
    pub access_key_id: Option<String>,
    /// Static secret access key (required when `access_key_id` is set).
    pub secret_access_key: Option<String>,
    /// Force path-style addressing. Required for MinIO.
    #[serde(default)]
    pub force_path_style: bool,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

// ---- FDB ----

/// TOML / env representation of FoundationDB settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FdbFileConfig {
    /// Path to the FDB cluster file.  Defaults to the FDB platform default.
    pub cluster_file: Option<String>,
    /// Tenant / organisation identifier.
    pub tenant: String,
    /// Workspace identifier.
    pub workspace: String,
}

// ---- Cache ----

/// TOML / env representation of DragonflyDB / Redis cache settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFileConfig {
    /// Redis / DragonflyDB URL (e.g. `"redis://localhost:6379"`).
    #[serde(default = "default_cache_url")]
    pub url: String,
    /// Connection pool size. Defaults to `4`.
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    /// Key prefix to namespace all cache keys. Defaults to `"axiom"`.
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
}

fn default_cache_url() -> String {
    "redis://127.0.0.1:6379".to_string()
}
fn default_pool_size() -> usize {
    4
}
fn default_key_prefix() -> String {
    "axiom".to_string()
}

// ---------------------------------------------------------------------------
// StoreFactory
// ---------------------------------------------------------------------------

/// Builds an [`AppState`] from a [`BackendConfig`].
///
/// Instantiation is fail-fast: any misconfiguration (missing fields,
/// unreachable backend, missing Cargo feature) returns a `CasError::Store`
/// with a descriptive message.
pub struct StoreFactory {
    config: BackendConfig,
}

impl StoreFactory {
    /// Create a factory from an explicit [`BackendConfig`].
    pub fn new(config: BackendConfig) -> Self {
        Self { config }
    }

    /// Load config from a TOML file and return a ready factory.
    ///
    /// # Errors
    ///
    /// Returns `CasError::Store` if the file cannot be read or the TOML is
    /// invalid / ambiguous.
    pub fn from_file(path: impl AsRef<Path>) -> CasResult<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| {
            CasError::Store(format!("StoreFactory: cannot read '{}': {e}", path.display()))
        })?;
        let raw: StoreConfig = toml::from_str(&content).map_err(|e| {
            CasError::Store(format!(
                "StoreFactory: TOML parse error in '{}': {e}",
                path.display()
            ))
        })?;
        let backend = raw.into_backend()?;
        Ok(Self::new(backend))
    }

    /// Build a factory by reading `AXIOM_*` environment variables.
    ///
    /// `AXIOM_BACKEND` must be set to `"local"` or `"cloud"`.  See the module
    /// documentation for the full list of supported variables.
    ///
    /// # Errors
    ///
    /// Returns `CasError::Store` if required variables are missing or contain
    /// invalid values.
    pub fn from_env() -> CasResult<Self> {
        let backend_str = std::env::var("AXIOM_BACKEND").map_err(|_| {
            CasError::Store("StoreFactory: AXIOM_BACKEND is not set (expected 'local' or 'cloud')".into())
        })?;

        let config = match backend_str.to_lowercase().as_str() {
            "local" => BackendConfig::Local(local_config_from_env()?),
            "cloud" => BackendConfig::Cloud(cloud_config_from_env()?),
            other => {
                return Err(CasError::Store(format!(
                    "StoreFactory: AXIOM_BACKEND='{}' is not valid; expected 'local' or 'cloud'",
                    other
                )));
            }
        };

        Ok(Self::new(config))
    }

    /// Instantiate the configured backend and return an [`AppState`].
    ///
    /// # Errors
    ///
    /// Returns `CasError::Store` if the backend cannot be opened, or if the
    /// required Cargo feature is not enabled for the requested backend.
    pub fn create(self) -> CasResult<AppState> {
        match self.config {
            BackendConfig::Local(cfg) => Self::create_local(cfg),
            BackendConfig::Cloud(cfg) => Self::create_cloud(cfg),
        }
    }

    // -----------------------------------------------------------------------
    // Local backend
    // -----------------------------------------------------------------------

    #[cfg(feature = "local")]
    fn create_local(cfg: LocalConfig) -> CasResult<AppState> {
        use crate::store::{RocksDbCasStore, SqliteMetadataStore};

        let cas_path = cfg.data_dir.join("cas");
        let meta_path = cfg.data_dir.join("meta.db");

        std::fs::create_dir_all(&cfg.data_dir).map_err(|e| {
            CasError::Store(format!(
                "StoreFactory: cannot create data dir '{}': {e}",
                cfg.data_dir.display()
            ))
        })?;

        let cas = RocksDbCasStore::open(&cas_path).map_err(|e| {
            CasError::Store(format!("StoreFactory: RocksDB open '{}' failed: {e}", cas_path.display()))
        })?;
        let meta = SqliteMetadataStore::open(&meta_path).map_err(|e| {
            CasError::Store(format!("StoreFactory: SQLite open '{}' failed: {e}", meta_path.display()))
        })?;

        Ok(AppState::local(cas, meta))
    }

    #[cfg(not(feature = "local"))]
    fn create_local(_cfg: LocalConfig) -> CasResult<AppState> {
        Err(CasError::Store(
            "StoreFactory: local backend requires the 'local' Cargo feature; \
             rebuild with --features local"
                .into(),
        ))
    }

    // -----------------------------------------------------------------------
    // Cloud backend
    // -----------------------------------------------------------------------

    #[cfg(all(feature = "cloud", feature = "fdb"))]
    fn create_cloud(cfg: CloudConfig) -> CasResult<AppState> {
        use std::sync::Arc;

        use crate::store::cache::{CacheClient, CacheConfig, CachedChunkStore, CachedMetadataStore};
        use crate::store::fdb::{FdbConfig, FdbMetadataStore};
        use crate::store::s3::{S3ChunkStore, S3Config, StaticCredentials};

        // ---- S3 chunk store ----
        let credentials = match (&cfg.s3.access_key_id, &cfg.s3.secret_access_key) {
            (Some(ak), Some(sk)) => Some(StaticCredentials {
                access_key_id: ak.clone(),
                secret_access_key: sk.clone(),
            }),
            (None, None) => None,
            _ => {
                return Err(CasError::Store(
                    "StoreFactory: S3 config requires both access_key_id and secret_access_key, \
                     or neither (use default credential chain)"
                        .into(),
                ));
            }
        };

        let s3_cfg = S3Config::new(
            cfg.s3.bucket,
            cfg.s3.region,
            cfg.s3.endpoint_url,
            credentials,
            cfg.s3.force_path_style,
        );
        let s3 = S3ChunkStore::new(s3_cfg)?;

        // ---- FDB metadata store ----
        let fdb_cfg = FdbConfig {
            cluster_file: cfg.fdb.cluster_file,
            tenant: cfg.fdb.tenant,
            workspace: cfg.fdb.workspace,
        };
        let fdb = FdbMetadataStore::new(fdb_cfg)?;

        // ---- Optional cache layer ----
        Ok(if let Some(cache_cfg) = cfg.cache {
            let cache_config = CacheConfig {
                url: cache_cfg.url,
                pool_size: cache_cfg.pool_size,
                key_prefix: cache_cfg.key_prefix,
            };
            match CacheClient::new(cache_config) {
                Ok(client) => {
                    let client = Arc::new(client);
                    let cached_s3 = CachedChunkStore::new(s3, Arc::clone(&client));
                    let cached_fdb = CachedMetadataStore::new(fdb, Arc::clone(&client));
                    AppState::cloud_cached(cached_s3, cached_fdb)
                }
                Err(e) => {
                    // Cache is best-effort: log and continue without it.
                    tracing::warn!(error = %e, "StoreFactory: cache init failed, running without cache");
                    AppState::cloud_uncached(s3, fdb)
                }
            }
        } else {
            AppState::cloud_uncached(s3, fdb)
        })
    }

    #[cfg(not(all(feature = "cloud", feature = "fdb")))]
    fn create_cloud(_cfg: CloudConfig) -> CasResult<AppState> {
        Err(CasError::Store(
            "StoreFactory: cloud backend requires the 'cloud' and 'fdb' Cargo features; \
             rebuild with --features cloud,fdb"
                .into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Env-var loading helpers
// ---------------------------------------------------------------------------

fn local_config_from_env() -> CasResult<LocalConfig> {
    let data_dir = match std::env::var("AXIOM_DATA_DIR") {
        Ok(v) => PathBuf::from(v),
        Err(_) => default_data_dir(),
    };
    Ok(LocalConfig { data_dir })
}

fn cloud_config_from_env() -> CasResult<CloudConfig> {
    let bucket = require_env("AXIOM_S3_BUCKET")?;
    let region = std::env::var("AXIOM_S3_REGION")
        .unwrap_or_else(|_| default_region());
    let endpoint_url = std::env::var("AXIOM_S3_ENDPOINT_URL").ok();
    let access_key_id = std::env::var("AXIOM_S3_ACCESS_KEY_ID").ok();
    let secret_access_key = std::env::var("AXIOM_S3_SECRET_ACCESS_KEY").ok();
    let force_path_style = std::env::var("AXIOM_S3_FORCE_PATH_STYLE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    let fdb_cluster = std::env::var("AXIOM_FDB_CLUSTER_FILE").ok();
    let fdb_tenant = require_env("AXIOM_FDB_TENANT")?;
    let fdb_workspace = require_env("AXIOM_FDB_WORKSPACE")?;

    let cache = match std::env::var("AXIOM_CACHE_URL") {
        Ok(url) => {
            let pool_size = std::env::var("AXIOM_CACHE_POOL_SIZE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(default_pool_size());
            let key_prefix = std::env::var("AXIOM_CACHE_KEY_PREFIX")
                .unwrap_or_else(|_| default_key_prefix());
            Some(CacheFileConfig { url, pool_size, key_prefix })
        }
        Err(_) => None,
    };

    Ok(CloudConfig {
        s3: S3FileConfig {
            bucket,
            region,
            endpoint_url,
            access_key_id,
            secret_access_key,
            force_path_style,
        },
        fdb: FdbFileConfig {
            cluster_file: fdb_cluster,
            tenant: fdb_tenant,
            workspace: fdb_workspace,
        },
        cache,
    })
}

/// Read a required environment variable, returning `CasError::Store` if absent.
fn require_env(name: &str) -> CasResult<String> {
    std::env::var(name).map_err(|_| {
        CasError::Store(format!("StoreFactory: required env var '{name}' is not set"))
    })
}
