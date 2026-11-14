//! P1-1: tenant quota enforcement + usage accounting.

use std::net::SocketAddr;
use std::sync::Arc;

use axiom_core::api::{build_router, state::AppState, state::HttpAuthMode};
use axiom_core::auth::jwt::JwtService;
use axiom_core::auth::refresh::MemoryRefreshTokenStore;
use axiom_core::auth::service::AuthService;
use axiom_core::quota::QuotaPolicy;
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::tenant::memory::MemoryTenantDirectory;
use reqwest::StatusCode;
use serde_json::Value;

const DEV_SECRET: &str = "quota-dev-secret";
const FREE_QUOTA: u64 = 1024 * 1024; // 1 MiB

struct Harness {
    base: String,
    _tmp: tempfile::TempDir,
}

async fn start() -> Harness {
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
        Some(DEV_SECRET.to_owned()),
    ));

    let state = AppState::local(cas, meta)
        .with_auth(auth)
        .with_http_auth(HttpAuthMode::Jwt)
        .with_quota_policy(QuotaPolicy::new(FREE_QUOTA, 10 * FREE_QUOTA, 100 * FREE_QUOTA));

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    Harness { base: format!("http://{addr}"), _tmp: tmp }
}

async fn token(h: &Harness) -> String {
    let resp = reqwest::Client::new()
        .post(format!("{}/auth/login", h.base))
        .header("x-axiom-dev-secret", DEV_SECRET)
        .json(&serde_json::json!({
            "provider": "dev",
            "email": "quota@example.com",
            "name": "Quota"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["roles"][0], "Owner");
    body["access_token"].as_str().unwrap().to_owned()
}

fn payload(len: usize, seed: u64) -> Vec<u8> {
    let mut data = vec![0u8; len];
    let mut state = seed;
    for byte in data.iter_mut() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        *byte = (state >> 33) as u8;
    }
    data
}

#[tokio::test]
async fn upload_over_quota_returns_402_and_usage_is_accounted() {
    let h = start().await;
    let token = token(&h).await;

    // 600 KiB — inside the 1 MiB free tier.
    let first = payload(600 * 1024, 1);
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/upload/file?path=first.bin", h.base))
        .bearer_auth(&token)
        .body(first.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "first upload should fit");

    let usage: Value = reqwest::Client::new()
        .get(format!("{}/api/v1/usage", h.base))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(usage["plan"], "free");
    assert_eq!(usage["quota_bytes"], FREE_QUOTA);
    assert_eq!(usage["used_bytes"], first.len() as u64);
    assert_eq!(usage["remaining_bytes"], FREE_QUOTA - first.len() as u64);

    // Another 600 KiB would cross the ceiling.
    let second = payload(600 * 1024, 2);
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/upload/file?path=second.bin", h.base))
        .bearer_auth(&token)
        .body(second)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYMENT_REQUIRED);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "quota_exceeded");
    assert_eq!(body["quota_bytes"], FREE_QUOTA);
    assert_eq!(body["used_bytes"], first.len() as u64);
    assert_eq!(body["plan"], "free");

    // Re-uploading identical content is deduplicated: no new bytes, no charge.
    let resp = reqwest::Client::new()
        .post(format!("{}/api/v1/upload/file?path=first-copy.bin", h.base))
        .bearer_auth(&token)
        .body(first)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "deduplicated content must not consume quota"
    );

    let usage: Value = reqwest::Client::new()
        .get(format!("{}/api/v1/usage", h.base))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(usage["used_bytes"], 600 * 1024);
}

#[tokio::test]
async fn usage_endpoint_requires_identity() {
    let h = start().await;
    let resp = reqwest::get(format!("{}/api/v1/usage", h.base))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
