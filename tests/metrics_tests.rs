//! P1-2: domain metrics and readiness gauge are exported.

use std::net::SocketAddr;
use std::sync::Arc;

use axiom_core::api::{build_router, state::AppState, state::HttpAuthMode};
use axiom_core::auth::jwt::JwtService;
use axiom_core::auth::refresh::MemoryRefreshTokenStore;
use axiom_core::auth::service::AuthService;
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::tenant::memory::MemoryTenantDirectory;

async fn start() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();

    let jwt = Arc::new(JwtService::new(
        vec![axiom_core::auth::jwt::JwtKeyPair::generate().unwrap()],
        0,
        Arc::new(MemoryRefreshTokenStore::new()),
    ));
    let auth = Arc::new(AuthService::new(
        jwt,
        Arc::new(MemoryTenantDirectory::new()),
        None,
        None,
    ));

    let state = AppState::local(cas, meta)
        .with_auth(auth)
        .with_http_auth(HttpAuthMode::Jwt);

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), tmp)
}

#[tokio::test]
async fn metrics_expose_readiness_auth_and_quota_counters() {
    let (base, _tmp) = start().await;

    // Touch readiness so the gauge is published.
    let ready = reqwest::get(format!("{base}/ready")).await.unwrap();
    assert!(ready.status().is_success());

    // Trigger an authentication failure.
    let denied = reqwest::get(format!("{base}/api/v1/workspaces")).await.unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::UNAUTHORIZED);

    let metrics = reqwest::get(format!("{base}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    for name in [
        "axiom_ready",
        "axiom_auth_failures_total",
        "axiom_quota_rejections_total",
        "axiom_sync_operations_total",
        "axiom_storage_operations_total",
    ] {
        assert!(
            metrics.contains(name),
            "expected metric '{name}' in /metrics output"
        );
    }

    assert!(
        metrics.contains("axiom_auth_failures_total{reason=\"missing_bearer\"}"),
        "auth failure counter should be labelled by reason:\n{metrics}"
    );
    assert!(
        metrics.contains("axiom_ready 1"),
        "readiness gauge should be 1 after a successful probe"
    );
}
