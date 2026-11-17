//! P2-3: list endpoints clamp `limit` to a documented maximum.

use std::net::SocketAddr;

use axiom_core::api::pagination::MAX_LIMIT;
use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::sqlite::SqliteMetadataStore;
use serde_json::Value;

async fn start() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();
    let app = build_router(AppState::local(cas, meta));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), tmp)
}

#[tokio::test]
async fn history_limit_is_capped_at_the_documented_maximum() {
    let (base, _tmp) = start().await;
    let client = reqwest::Client::new();

    // 105 commits => one more than the cap.
    for i in 0..(MAX_LIMIT + 5) {
        let resp = client
            .post(format!("{base}/api/v1/upload/file?path=f{i}.bin"))
            .body(format!("payload-{i}").into_bytes())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "upload {i} failed");
    }

    let huge: Value = client
        .get(format!("{base}/api/v1/versions/main/history?limit=100000"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let items = huge["versions"].as_array().unwrap();
    assert_eq!(huge["limit"], 100);
    assert_eq!(
        items.len(),
        MAX_LIMIT,
        "limit must be clamped to {MAX_LIMIT}, got {}",
        items.len()
    );
    assert_eq!(huge["has_more"], true);

    // limit=0 must not produce an empty or unbounded page.
    let tiny: Value = client
        .get(format!("{base}/api/v1/versions/main/history?limit=0"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tiny["versions"].as_array().unwrap().len(), 1);
}
