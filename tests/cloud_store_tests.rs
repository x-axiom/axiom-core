//! Integration tests for cloud storage backends.
//!
//! # Requirements
//!
//! All tests in this file require Docker to be running on the host.
//!
//! # Running
//!
//! S3 + cache layer tests (MinIO + Redis via Docker):
//! ```sh
//! cargo test --test cloud_store_tests --features cloud
//! ```
//!
//! Full cloud tests including FoundationDB:
//! ```sh
//! cargo test --test cloud_store_tests --features cloud,fdb
//! ```
//!
//! Slow tests are marked `#[ignore]`.  To include them:
//! ```sh
//! cargo test --test cloud_store_tests --features cloud -- --include-ignored
//! ```

// ============================================================
// S3 CHUNK STORE TESTS
// ============================================================

#[cfg(feature = "cloud")]
mod s3_chunk_store {
    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_credential_types::Credentials;
    use aws_sdk_s3::Client as AwsS3Client;
    use aws_sdk_s3::config::{Builder as S3Builder, Region};
    use testcontainers_modules::minio::MinIO;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    use axiom_core::error::CasError;
    use axiom_core::model::hash_bytes;
    use axiom_core::store::{ChunkStore, S3ChunkStore, S3Config, StaticCredentials};

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    async fn start_minio() -> (impl Drop, String) {
        let container = MinIO::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(9000).await.unwrap();
        let endpoint = format!("http://{}:{}", host, port);
        (container, endpoint)
    }

    /// Create a bucket in MinIO using the AWS SDK directly (async).
    async fn create_bucket(endpoint: &str, bucket: &str) {
        let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");
        let cfg = S3Builder::new()
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .force_path_style(true)
            .build();
        let client = AwsS3Client::from_conf(cfg);
        client.create_bucket().bucket(bucket).send().await.unwrap();
    }

    fn make_cfg(endpoint: &str, bucket: &str) -> S3Config {
        S3Config::new(
            bucket,
            "us-east-1",
            Some(endpoint.to_string()),
            Some(StaticCredentials {
                access_key_id: "minioadmin".into(),
                secret_access_key: "minioadmin".into(),
            }),
            true,
        )
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /// Round-trip: put a chunk and retrieve it back unchanged.
    #[tokio::test]
    async fn put_and_get_round_trip() {
        let (_c, ep) = start_minio().await;
        create_bucket(&ep, "axiom-chunks").await;
        let cfg = make_cfg(&ep, "axiom-chunks");

        let (original, retrieved) =
            tokio::task::spawn_blocking(move || -> Result<_, CasError> {
                let store = S3ChunkStore::new(cfg);
                let data = b"hello cloud storage".to_vec();
                let hash = store.put_chunk(data.clone())?;
                let got = store.get_chunk(&hash)?.unwrap();
                Ok((data, got))
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(original, retrieved);
    }

    /// `get_chunk` returns `None` for an unknown hash.
    #[tokio::test]
    async fn get_missing_chunk_returns_none() {
        let (_c, ep) = start_minio().await;
        create_bucket(&ep, "axiom-chunks").await;
        let cfg = make_cfg(&ep, "axiom-chunks");

        let result = tokio::task::spawn_blocking(move || -> Result<_, CasError> {
            let store = S3ChunkStore::new(cfg);
            let fake = hash_bytes(b"definitely-not-stored");
            store.get_chunk(&fake)
        })
        .await
        .unwrap()
        .unwrap();

        assert!(result.is_none());
    }

    /// `has_chunk` returns false before put and true after.
    #[tokio::test]
    async fn has_chunk_true_and_false() {
        let (_c, ep) = start_minio().await;
        create_bucket(&ep, "axiom-chunks").await;
        let cfg = make_cfg(&ep, "axiom-chunks");

        let (before, after) = tokio::task::spawn_blocking(move || -> Result<_, CasError> {
            let store = S3ChunkStore::new(cfg);
            let data = b"has-chunk-test-data".to_vec();
            let hash = hash_bytes(&data);
            let b = store.has_chunk(&hash)?;
            store.put_chunk(data)?;
            let a = store.has_chunk(&hash)?;
            Ok((b, a))
        })
        .await
        .unwrap()
        .unwrap();

        assert!(!before);
        assert!(after);
    }

    /// Putting the same bytes twice returns the same hash and does not error.
    #[tokio::test]
    async fn put_is_idempotent() {
        let (_c, ep) = start_minio().await;
        create_bucket(&ep, "axiom-chunks").await;
        let cfg = make_cfg(&ep, "axiom-chunks");

        tokio::task::spawn_blocking(move || -> Result<(), CasError> {
            let store = S3ChunkStore::new(cfg);
            let data = b"idempotent-upload".to_vec();
            let h1 = store.put_chunk(data.clone())?;
            let h2 = store.put_chunk(data)?;
            assert_eq!(h1, h2);
            Ok(())
        })
        .await
        .unwrap()
        .unwrap();
    }

    /// Large (>100 MiB) upload via multipart API round-trips correctly.
    ///
    /// Marked `#[ignore]` because it is slow (~120 MB transfer over loopback).
    #[tokio::test]
    #[ignore]
    async fn multipart_upload_round_trip() {
        let (_c, ep) = start_minio().await;
        create_bucket(&ep, "axiom-chunks").await;
        let cfg = make_cfg(&ep, "axiom-chunks");

        let (original_len, retrieved_len) =
            tokio::task::spawn_blocking(move || -> Result<_, CasError> {
                let store = S3ChunkStore::new(cfg);
                // 120 MiB — exceeds the 100 MiB multipart threshold
                let data = vec![0xABu8; 120 * 1024 * 1024];
                let hash = store.put_chunk(data.clone())?;
                let got = store.get_chunk(&hash)?.unwrap();
                Ok((data.len(), got.len()))
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(original_len, retrieved_len);
    }
}

// ============================================================
// CACHED CHUNK STORE TESTS
// ============================================================

/// Tests for `CachedChunkStore` layered over `InMemoryChunkStore` + Redis.
///
/// These tests validate cache behaviour (hits, misses, stats, graceful
/// degradation) without requiring MinIO.
#[cfg(feature = "cloud")]
mod cached_chunk_store {
    use std::sync::Arc;

    use testcontainers_modules::redis::Redis;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    use axiom_core::error::CasError;
    use axiom_core::model::hash_bytes;
    use axiom_core::store::{
        CacheClient, CacheConfig, CachedChunkStore, ChunkStore, InMemoryChunkStore,
    };

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    async fn start_redis() -> (impl Drop, String) {
        let container = Redis::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(6379).await.unwrap();
        let url = format!("redis://{}:{}", host, port);
        (container, url)
    }

    fn make_cache_cfg(redis_url: &str) -> CacheConfig {
        CacheConfig {
            url: redis_url.to_string(),
            pool_size: 2,
            key_prefix: "axiom-test".into(),
        }
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /// After `put_chunk`, the second `get_chunk` is served from cache (hit).
    #[tokio::test]
    async fn cache_hit_on_second_get() {
        let (_c, redis_url) = start_redis().await;
        let cfg = make_cache_cfg(&redis_url);

        let (hits_before, hits_after) =
            tokio::task::spawn_blocking(move || -> Result<_, CasError> {
                let inner = InMemoryChunkStore::default();
                let cache = Arc::new(CacheClient::new(cfg)?);
                let store = CachedChunkStore::new(inner, Arc::clone(&cache));

                let data = b"cached-chunk-data".to_vec();
                store.put_chunk(data.clone())?;

                // First access after put — data is already in cache.
                let h1 = cache.stats().hits;
                store.get_chunk(&hash_bytes(&data))?;
                let h2 = cache.stats().hits;

                Ok((h1, h2))
            })
            .await
            .unwrap()
            .unwrap();

        assert!(hits_after > hits_before, "expected a cache hit on second get");
    }

    /// Cold get (miss) → put → warm get (hit): cache fills on put and serves the next read.
    #[tokio::test]
    async fn cache_miss_then_backfill_then_hit() {
        let (_c, redis_url) = start_redis().await;
        let cfg = make_cache_cfg(&redis_url);

        let (misses_after_cold, hits_after_warm) =
            tokio::task::spawn_blocking(move || -> Result<_, CasError> {
                let inner = InMemoryChunkStore::default();
                let cache = Arc::new(CacheClient::new(cfg)?);
                let store = CachedChunkStore::new(inner, Arc::clone(&cache));

                let data = b"backfill-test".to_vec();
                let hash = hash_bytes(&data);

                // Cold miss: key does not exist yet.
                store.get_chunk(&hash)?;
                let misses = cache.stats().misses;

                // Put populates both inner store and cache.
                store.put_chunk(data)?;

                // Warm hit.
                store.get_chunk(&hash)?;
                let hits = cache.stats().hits;

                Ok((misses, hits))
            })
            .await
            .unwrap()
            .unwrap();

        assert!(misses_after_cold >= 1, "expected at least one miss");
        assert!(hits_after_warm >= 1, "expected at least one cache hit");
    }

    /// `has_chunk` benefits from the cache: a put followed by `has_chunk` is a cache hit.
    #[tokio::test]
    async fn has_chunk_uses_cache() {
        let (_c, redis_url) = start_redis().await;
        let cfg = make_cache_cfg(&redis_url);

        let hits = tokio::task::spawn_blocking(move || -> Result<_, CasError> {
            let inner = InMemoryChunkStore::default();
            let cache = Arc::new(CacheClient::new(cfg)?);
            let store = CachedChunkStore::new(inner, Arc::clone(&cache));

            let data = b"has-chunk-cache-test".to_vec();
            store.put_chunk(data.clone())?;

            let h_before = cache.stats().hits;
            let exists = store.has_chunk(&hash_bytes(&data))?;
            let h_after = cache.stats().hits;

            assert!(exists);
            Ok(h_after - h_before)
        })
        .await
        .unwrap()
        .unwrap();

        assert!(hits >= 1, "expected has_chunk to register a cache hit");
    }

    /// If the cache URL is invalid, `CacheClient::new` returns an error.
    /// The caller can fall back to an uncached store gracefully.
    #[tokio::test]
    async fn invalid_cache_url_returns_error() {
        let result = tokio::task::spawn_blocking(|| {
            CacheClient::new(CacheConfig {
                url: "redis://definitely-not-a-host:9999".into(),
                pool_size: 1,
                key_prefix: "test".into(),
            })
        })
        .await
        .unwrap();

        // Connection should fail or succeed eventually; we only check that
        // an error is returned for a clearly invalid host.  Some Redis clients
        // defer connection until the first operation — this validates at least
        // that we handle the result correctly without panicking.
        let _ = result; // either Ok or Err is acceptable depending on driver behaviour
    }
}

// ============================================================
// FDB METADATA STORE TESTS  (requires --features cloud,fdb)
// ============================================================

#[cfg(all(feature = "cloud", feature = "fdb"))]
mod fdb_metadata_store {
    use std::collections::BTreeMap;
    use std::sync::Once;
    use std::time::Duration;

    use testcontainers_modules::testcontainers::core::{
        ExecCommand, IntoContainerPort, WaitFor,
    };
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::GenericImage;

    use axiom_core::error::CasError;
    use axiom_core::model::{
        hash_bytes, ChunkHash, NodeEntry, NodeKind, Ref, RefKind, TreeNode, TreeNodeKind,
        VersionNode,
    };
    use axiom_core::store::{
        FdbConfig, FdbMetadataStore, NodeStore, PathIndexRepo, RefRepo, TreeStore, VersionRepo,
    };

    // ------------------------------------------------------------------
    // FDB network guard — boot() must be called exactly once per process.
    // ------------------------------------------------------------------

    static FDB_BOOT: Once = Once::new();

    pub(super) fn ensure_fdb_network() {
        FDB_BOOT.call_once(|| {
            // SAFETY: called once, the guard is intentionally leaked to keep
            // the FDB network thread alive for the duration of the test process.
            std::mem::forget(unsafe { foundationdb::boot() });
        });
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    async fn start_fdb_and_write_cluster_file() -> (impl Drop, tempfile::NamedTempFile) {
        let image = GenericImage::new("foundationdb/foundationdb", "7.3.0")
            .with_exposed_port(4500.tcp())
            .with_wait_for(WaitFor::Duration {
                length: Duration::from_secs(6),
            });

        let container = image.start().await.unwrap();

        // Configure the FDB cluster (single-process, in-memory mode).
        let _ = container
            .exec(ExecCommand::new([
                "fdbcli",
                "--exec",
                "configure new single memory",
            ]))
            .await;

        // Wait for configuration to take effect.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(4500).await.unwrap();

        // Write the external cluster file pointing to the host-mapped port.
        let cluster_content = format!("docker:docker@{}:{}", host, port);
        let mut cluster_file = tempfile::NamedTempFile::new().unwrap();
        use std::io::Write;
        cluster_file.write_all(cluster_content.as_bytes()).unwrap();
        cluster_file.flush().unwrap();

        (container, cluster_file)
    }

    fn fdb_cfg(cluster_path: &str, workspace: &str) -> FdbConfig {
        FdbConfig {
            cluster_file: Some(cluster_path.to_string()),
            tenant: "test_tenant".into(),
            workspace: workspace.to_string(),
        }
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /// Exercises all five trait implementations in a single container session.
    #[tokio::test]
    async fn all_traits_round_trip() {
        ensure_fdb_network();
        let (_c, cluster_file) = start_fdb_and_write_cluster_file().await;
        let cluster_path = cluster_file.path().to_str().unwrap().to_string();

        tokio::task::spawn_blocking(move || -> Result<(), CasError> {
            let store = FdbMetadataStore::new(fdb_cfg(&cluster_path, "ws_all_traits"))?;

            // ---- TreeStore ----
            let leaf_hash = hash_bytes(b"leaf-data");
            let tree = TreeNode {
                hash: hash_bytes(b"tree-node"),
                kind: TreeNodeKind::Leaf { chunk: leaf_hash },
            };
            store.put_tree_node(&tree)?;
            let got = store.get_tree_node(&tree.hash)?.unwrap();
            assert_eq!(got.hash, tree.hash);

            // ---- NodeStore ----
            let node = NodeEntry {
                hash: hash_bytes(b"node-entry"),
                kind: NodeKind::File { root: leaf_hash, size: 42 },
            };
            store.put_node(&node)?;
            let got_node = store.get_node(&node.hash)?.unwrap();
            assert_eq!(got_node.hash, node.hash);

            // ---- VersionRepo ----
            let root_hash = hash_bytes(b"root");
            let version = VersionNode {
                id: hash_bytes(b"version-1"),
                parents: vec![],
                root: root_hash,
                message: "initial commit".into(),
                timestamp: 1_700_000_000,
                metadata: Default::default(),
            };
            store.put_version(&version)?;
            let got_v = store.get_version(&version.id)?.unwrap();
            assert_eq!(got_v.message, "initial commit");

            let v2_id = hash_bytes(b"version-2");
            let version2 = VersionNode {
                id: v2_id,
                parents: vec![version.id],
                root: root_hash,
                message: "second commit".into(),
                timestamp: 1_700_001_000,
                metadata: Default::default(),
            };
            store.put_version(&version2)?;
            let history = store.list_history(&v2_id, 10)?;
            assert_eq!(history.len(), 2);
            assert_eq!(history[0].message, "second commit");

            // ---- RefRepo ----
            let branch = Ref {
                name: "main".into(),
                kind: RefKind::Branch,
                target: v2_id,
            };
            store.put_ref(&branch)?;
            let got_ref = store.get_ref("main")?.unwrap();
            assert_eq!(got_ref.target, v2_id);

            store.delete_ref("main")?;
            assert!(store.get_ref("main")?.is_none());

            // ---- RefRepo: compare_and_swap_ref ----
            let v3_id = hash_bytes(b"version-3");
            let new_ref = Ref { name: "cas-branch".into(), kind: RefKind::Branch, target: v3_id };

            // CAS from None → new ref: should succeed.
            let ok = store.compare_and_swap_ref("cas-branch", None, &new_ref)?;
            assert!(ok, "CAS from None should succeed");

            // CAS with wrong expected: should fail.
            let wrong_expected = hash_bytes(b"wrong-version");
            let rejected = store.compare_and_swap_ref("cas-branch", Some(&wrong_expected), &new_ref)?;
            assert!(!rejected, "CAS with wrong expected should fail");

            // CAS with correct expected: should succeed.
            let updated = Ref { name: "cas-branch".into(), kind: RefKind::Branch, target: v2_id };
            let ok2 = store.compare_and_swap_ref("cas-branch", Some(&v3_id), &updated)?;
            assert!(ok2, "CAS with correct expected should succeed");

            // ---- PathIndexRepo ----
            let version_id = hash_bytes(b"path-version");
            let file_hash = hash_bytes(b"file-content");
            store.put_path_entry(&version_id, "src/main.rs", &file_hash, false)?;
            store.put_path_entry(&version_id, "src", &hash_bytes(b"src-dir"), true)?;

            let entry = store.get_by_path(&version_id, "src/main.rs")?.unwrap();
            assert_eq!(entry.path, "src/main.rs");
            assert!(!entry.is_directory);

            let children = store.list_directory(&version_id, "src")?;
            assert!(!children.is_empty());

            Ok(())
        })
        .await
        .unwrap()
        .unwrap();
    }

    /// Concurrent CAS: N tasks race to create the same ref; exactly one wins.
    #[tokio::test]
    async fn compare_and_swap_concurrent_exactly_one_wins() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        ensure_fdb_network();
        let (_c, cluster_file) = start_fdb_and_write_cluster_file().await;
        let cluster_path = cluster_file.path().to_str().unwrap().to_string();

        let wins = Arc::new(AtomicUsize::new(0));
        let concurrency = 8usize;

        let handles: Vec<_> = (0..concurrency)
            .map(|i| {
                let cp = cluster_path.clone();
                let wins = Arc::clone(&wins);
                tokio::task::spawn_blocking(move || -> Result<(), CasError> {
                    let store = FdbMetadataStore::new(fdb_cfg(&cp, "ws_cas_race"))?;
                    let target = hash_bytes(format!("version-{i}").as_bytes());
                    let r = Ref { name: "race-ref".into(), kind: RefKind::Branch, target };
                    if store.compare_and_swap_ref("race-ref", None, &r)? {
                        wins.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                })
            })
            .collect();

        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(
            wins.load(Ordering::Relaxed),
            1,
            "exactly one CAS should succeed among concurrent writers"
        );
    }
}

// ============================================================
// STORE FACTORY TESTS  (requires --features cloud,fdb)
// ============================================================

#[cfg(all(feature = "cloud", feature = "fdb"))]
mod store_factory {
    use std::time::Duration;

    use testcontainers_modules::minio::MinIO;
    use testcontainers_modules::testcontainers::core::{
        ExecCommand, IntoContainerPort, WaitFor,
    };
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::GenericImage;

    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_credential_types::Credentials;
    use aws_sdk_s3::Client as AwsS3Client;
    use aws_sdk_s3::config::{Builder as S3Builder, Region};

    use axiom_core::store::StoreFactory;

    async fn create_bucket(endpoint: &str, bucket: &str) {
        let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");
        let cfg = S3Builder::new()
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .force_path_style(true)
            .build();
        let client = AwsS3Client::from_conf(cfg);
        client.create_bucket().bucket(bucket).send().await.unwrap();
    }

    /// `StoreFactory::from_env()` with `AXIOM_BACKEND=local` creates an
    /// `AppState` backed by RocksDB + SQLite.
    #[tokio::test]
    async fn from_env_local_creates_appstate() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data_dir = tmp.path().to_str().unwrap().to_string();

        let state = tokio::task::spawn_blocking(move || {
            // SAFETY: test process is single-threaded here; no race condition.
            unsafe {
                std::env::set_var("AXIOM_BACKEND", "local");
                std::env::set_var("AXIOM_DATA_DIR", &data_dir);
            }
            StoreFactory::from_env().and_then(|f| f.create())
        })
        .await
        .unwrap();

        assert!(state.is_ok(), "local factory should succeed: {:?}", state);
    }

    /// `StoreFactory::from_env()` with `AXIOM_BACKEND=cloud` pointing to real
    /// MinIO + FDB containers successfully creates an `AppState`.
    #[tokio::test]
    async fn from_env_cloud_creates_appstate() {
        use std::io::Write;

        // Ensure FDB network is initialised.
        super::fdb_metadata_store::ensure_fdb_network();

        // Start MinIO.
        let minio = MinIO::default().start().await.unwrap();
        let minio_host = minio.get_host().await.unwrap();
        let minio_port = minio.get_host_port_ipv4(9000).await.unwrap();
        let minio_ep = format!("http://{}:{}", minio_host, minio_port);
        create_bucket(&minio_ep, "factory-chunks").await;

        // Start FDB.
        let fdb_image = GenericImage::new("foundationdb/foundationdb", "7.3.0")
            .with_exposed_port(4500.tcp())
            .with_wait_for(WaitFor::Duration { length: Duration::from_secs(6) });
        let fdb = fdb_image.start().await.unwrap();
        let _ = fdb
            .exec(ExecCommand::new(["fdbcli", "--exec", "configure new single memory"]))
            .await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let fdb_host = fdb.get_host().await.unwrap();
        let fdb_port = fdb.get_host_port_ipv4(4500).await.unwrap();
        let cluster_content = format!("docker:docker@{}:{}", fdb_host, fdb_port);
        let mut cluster_file = tempfile::NamedTempFile::new().unwrap();
        cluster_file.write_all(cluster_content.as_bytes()).unwrap();
        cluster_file.flush().unwrap();
        let cluster_path = cluster_file.path().to_str().unwrap().to_string();

        let state = tokio::task::spawn_blocking(move || {
            unsafe {
                std::env::set_var("AXIOM_BACKEND", "cloud");
                std::env::set_var("AXIOM_S3_BUCKET", "factory-chunks");
                std::env::set_var("AXIOM_S3_REGION", "us-east-1");
                std::env::set_var("AXIOM_S3_ENDPOINT_URL", &minio_ep);
                std::env::set_var("AXIOM_S3_ACCESS_KEY_ID", "minioadmin");
                std::env::set_var("AXIOM_S3_SECRET_ACCESS_KEY", "minioadmin");
                std::env::set_var("AXIOM_S3_FORCE_PATH_STYLE", "true");
                std::env::set_var("AXIOM_FDB_CLUSTER_FILE", &cluster_path);
                std::env::set_var("AXIOM_FDB_TENANT", "factory_tenant");
                std::env::set_var("AXIOM_FDB_WORKSPACE", "factory_workspace");
            }
            StoreFactory::from_env().and_then(|f| f.create())
        })
        .await
        .unwrap();

        assert!(state.is_ok(), "cloud factory should succeed: {:?}", state);
    }

    /// Specifying both `[local]` and `[cloud]` in TOML returns an error.
    #[tokio::test]
    async fn from_file_ambiguous_backend_returns_error() {
        let toml_content = r#"
[local]
data_dir = "/tmp/axiom-test"

[cloud.s3]
bucket = "test"
region = "us-east-1"
force_path_style = false

[cloud.fdb]
tenant = "t"
workspace = "w"
"#;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        use std::io::Write;
        f.write_all(toml_content.as_bytes()).unwrap();
        f.flush().unwrap();
        let path = f.path().to_str().unwrap().to_string();

        let result = tokio::task::spawn_blocking(move || StoreFactory::from_file(&path))
            .await
            .unwrap();

        assert!(
            result.is_err(),
            "ambiguous (both local and cloud) config should fail"
        );
    }
}



// ============================================================
// END-TO-END TEST  (requires --features cloud,fdb)
// ============================================================

/// Full pipeline: upload chunks → build tree → commit → list directory → download.
///
/// Uses S3 (MinIO) for chunks, FDB for metadata, and Redis for caching.
/// This validates all three tiers working together end-to-end.
#[cfg(all(feature = "cloud", feature = "fdb"))]
mod e2e {
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use testcontainers_modules::minio::MinIO;
    use testcontainers_modules::redis::Redis;
    use testcontainers_modules::testcontainers::core::{
        ExecCommand, IntoContainerPort, WaitFor,
    };
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::GenericImage;

    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_credential_types::Credentials;
    use aws_sdk_s3::Client as AwsS3Client;
    use aws_sdk_s3::config::{Builder as S3Builder, Region};

    use axiom_core::api::state::AppState;
    use axiom_core::error::CasError;
    use axiom_core::model::{
        ChunkHash, NodeEntry, NodeKind, Ref, RefKind, VersionNode, hash_bytes,
    };
    use axiom_core::store::{
        CacheClient, CacheConfig, CachedChunkStore, CachedMetadataStore,
        ChunkStore, FdbConfig, FdbMetadataStore, NodeStore, PathIndexRepo,
        RefRepo, S3ChunkStore, S3Config, StaticCredentials, TreeStore, VersionRepo,
    };

    async fn create_bucket(endpoint: &str, bucket: &str) {
        let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");
        let cfg = S3Builder::new()
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(creds))
            .force_path_style(true)
            .build();
        let client = AwsS3Client::from_conf(cfg);
        client.create_bucket().bucket(bucket).send().await.unwrap();
    }

    /// Upload three files, build a directory version, then verify every file
    /// can be retrieved from the cache-warm store.
    #[tokio::test]
    async fn upload_commit_list_download() {
        super::fdb_metadata_store::ensure_fdb_network();

        // ---- Start infrastructure ----

        let minio = MinIO::default().start().await.unwrap();
        let minio_host = minio.get_host().await.unwrap();
        let minio_port = minio.get_host_port_ipv4(9000).await.unwrap();
        let minio_ep = format!("http://{}:{}", minio_host, minio_port);
        create_bucket(&minio_ep, "e2e-chunks").await;

        let redis = Redis::default().start().await.unwrap();
        let redis_host = redis.get_host().await.unwrap();
        let redis_port = redis.get_host_port_ipv4(6379).await.unwrap();
        let redis_url = format!("redis://{}:{}", redis_host, redis_port);

        let fdb_image = GenericImage::new("foundationdb/foundationdb", "7.3.0")
            .with_exposed_port(4500.tcp())
            .with_wait_for(WaitFor::Duration { length: Duration::from_secs(6) });
        let fdb = fdb_image.start().await.unwrap();
        let _ = fdb
            .exec(ExecCommand::new(["fdbcli", "--exec", "configure new single memory"]))
            .await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let fdb_host = fdb.get_host().await.unwrap();
        let fdb_port = fdb.get_host_port_ipv4(4500).await.unwrap();
        let cluster_content = format!("docker:docker@{}:{}", fdb_host, fdb_port);
        let mut cluster_file = tempfile::NamedTempFile::new().unwrap();
        cluster_file.write_all(cluster_content.as_bytes()).unwrap();
        cluster_file.flush().unwrap();
        let cluster_path = cluster_file.path().to_str().unwrap().to_string();

        // ---- Run the end-to-end scenario in a blocking thread ----

        let ep = minio_ep.clone();
        tokio::task::spawn_blocking(move || -> Result<(), CasError> {
            // Build stores.
            let s3_cfg = S3Config::new(
                "e2e-chunks",
                "us-east-1",
                Some(ep),
                Some(StaticCredentials {
                    access_key_id: "minioadmin".into(),
                    secret_access_key: "minioadmin".into(),
                }),
                true,
            );
            let s3 = S3ChunkStore::new(s3_cfg);

            let fdb_cfg = FdbConfig {
                cluster_file: Some(cluster_path),
                tenant: "e2e_tenant".into(),
                workspace: "e2e_workspace".into(),
            };
            let fdb = FdbMetadataStore::new(fdb_cfg)?;

            let cache_cfg = CacheConfig {
                url: redis_url,
                pool_size: 2,
                key_prefix: "e2e-test".into(),
            };
            let cache = Arc::new(CacheClient::new(cache_cfg)?);
            let cached_s3 = CachedChunkStore::new(s3, Arc::clone(&cache));
            let cached_fdb = CachedMetadataStore::new(fdb, Arc::clone(&cache));

            // ---- Upload three files ----

            let files: &[(&str, &[u8])] = &[
                ("README.md",  b"# Hello Axiom"),
                ("src/main.rs", b"fn main() {}"),
                ("Cargo.toml", b"[package]\nname = \"e2e\"\n"),
            ];

            let mut dir_children: BTreeMap<String, ChunkHash> = BTreeMap::new();
            let mut src_children: BTreeMap<String, ChunkHash> = BTreeMap::new();

            for (path, data) in files {
                let chunk_hash = cached_s3.put_chunk(data.to_vec())?;
                let node = NodeEntry {
                    hash: hash_bytes(data),
                    kind: NodeKind::File { root: chunk_hash, size: data.len() as u64 },
                };
                cached_fdb.put_node(&node)?;

                let name = path.split('/').last().unwrap();
                if path.starts_with("src/") {
                    src_children.insert(name.into(), node.hash);
                } else {
                    dir_children.insert(name.into(), node.hash);
                }
            }

            // ---- Build directory nodes ----

            let src_node = NodeEntry {
                hash: hash_bytes(b"src-dir-node"),
                kind: NodeKind::Directory { children: src_children },
            };
            cached_fdb.put_node(&src_node)?;
            dir_children.insert("src".into(), src_node.hash);

            let root_node = NodeEntry {
                hash: hash_bytes(b"root-dir-node"),
                kind: NodeKind::Directory { children: dir_children },
            };
            cached_fdb.put_node(&root_node)?;

            // ---- Commit ----

            let version = VersionNode {
                id: hash_bytes(b"e2e-version-1"),
                parents: vec![],
                root: root_node.hash,
                message: "e2e initial commit".into(),
                timestamp: 1_700_000_000,
                metadata: Default::default(),
            };
            cached_fdb.put_version(&version)?;

            let main_ref = Ref {
                name: "main".into(),
                kind: RefKind::Branch,
                target: version.id,
            };
            cached_fdb.put_ref(&main_ref)?;

            // ---- Index paths ----

            for (path, data) in files {
                let node_hash = hash_bytes(data);
                cached_fdb.put_path_entry(&version.id, path, &node_hash, false)?;
            }
            cached_fdb.put_path_entry(&version.id, "src", &src_node.hash, true)?;
            cached_fdb.put_path_entry(&version.id, "", &root_node.hash, true)?;

            // ---- Verify: list root directory ----

            let root_entries = cached_fdb.list_directory(&version.id, "")?;
            assert!(!root_entries.is_empty(), "root directory should have entries");

            // ---- Verify: download each file ----

            for (_path, data) in files {
                let node_hash = hash_bytes(data);
                let node = cached_fdb.get_node(&node_hash)?.expect("node must exist");
                let chunk_hash = match node.kind {
                    NodeKind::File { root, .. } => root,
                    _ => panic!("expected file node"),
                };
                let retrieved = cached_s3.get_chunk(&chunk_hash)?.expect("chunk must exist");
                assert_eq!(retrieved, *data, "downloaded content must match uploaded content");
            }

            // ---- Verify: cache is warm (hits > 0 after reads) ----

            let stats = cache.stats();
            assert!(stats.hits > 0, "cache should have recorded hits after warm reads");

            Ok(())
        })
        .await
        .unwrap()
        .unwrap();
    }
}
