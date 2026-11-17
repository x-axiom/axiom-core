//! P2-1: CORS is an explicit allowlist, not `*`.

use std::net::SocketAddr;

use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::sqlite::SqliteMetadataStore;
use reqwest::header::{ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD, ORIGIN, VARY};

const ALLOWED: &str = "https://console.example";

async fn start() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();

    let state = AppState::local(cas, meta).with_cors_origins(vec![ALLOWED.to_owned()]);
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), tmp)
}

#[tokio::test]
async fn preflight_allows_only_configured_origins() {
    let (base, _tmp) = start().await;
    let client = reqwest::Client::new();

    let allowed = client
        .request(reqwest::Method::OPTIONS, format!("{base}/api/v1/workspaces"))
        .header(ORIGIN, ALLOWED)
        .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .send()
        .await
        .unwrap();
    assert!(allowed.status().is_success());
    assert_eq!(allowed.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], ALLOWED);

    let denied = client
        .request(reqwest::Method::OPTIONS, format!("{base}/api/v1/workspaces"))
        .header(ORIGIN, "https://evil.example")
        .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .send()
        .await
        .unwrap();
    assert!(
        denied.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).is_none(),
        "disallowed origin must not receive an allow-origin header"
    );
}

#[tokio::test]
async fn actual_requests_echo_the_allowed_origin_and_vary() {
    let (base, _tmp) = start().await;

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/workspaces"))
        .header(ORIGIN, ALLOWED)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "local mode requires no token");
    assert_eq!(resp.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], ALLOWED);
    assert!(
        resp.headers()[VARY]
            .to_str()
            .unwrap()
            .to_ascii_lowercase()
            .contains("origin"),
        "Vary must include Origin so caches do not mix up origins"
    );

    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/workspaces"))
        .header(ORIGIN, "https://evil.example")
        .send()
        .await
        .unwrap();
    assert!(resp.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
}
