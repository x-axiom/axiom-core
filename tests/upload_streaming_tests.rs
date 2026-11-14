//! P0-4: uploads must stream (no 2 MiB cap, bounded memory) and reject
//! oversized bodies with a structured 413.

use std::net::SocketAddr;

use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::sqlite::SqliteMetadataStore;
use reqwest::StatusCode;
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

fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut data = vec![0u8; len];
    let mut state = seed;
    for byte in data.iter_mut() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *byte = (state >> 33) as u8;
    }
    data
}

#[tokio::test]
async fn large_upload_streams_and_oversize_returns_structured_413() {
    let (base, _tmp) = start().await;

    // 1. 12 MiB — well past the old 2 MiB extractor limit.
    let big = pseudo_random(12 * 1024 * 1024, 0xA11CE);
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/upload/file?path=big.bin"))
        .body(big.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "12 MiB upload must no longer hit a body limit"
    );

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["stats"]["total_bytes"], big.len() as u64);
    let chunks = body["stats"]["total_chunks"].as_u64().unwrap();
    assert!(
        chunks > 10,
        "12 MiB should be split into many content-defined chunks, got {chunks}"
    );

    // 2. Configure a 64 KiB ceiling and verify the structured rejection.
    // SAFETY: this test is the only one in this binary mutating the
    // environment, and it does so sequentially around the request.
    unsafe { std::env::set_var("AXIOM_MAX_UPLOAD_BYTES", "65536") };

    let too_big = pseudo_random(1024 * 1024, 0xBEEF);
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/upload/file?path=too-big.bin"))
        .body(too_big)
        .send()
        .await
        .unwrap();

    unsafe { std::env::remove_var("AXIOM_MAX_UPLOAD_BYTES") };

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "payload_too_large");
    assert_eq!(body["max_bytes"], 65536);
}
