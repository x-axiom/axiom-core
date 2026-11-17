//! P1-12: HTTP Range support and safe `Content-Disposition`.

use std::net::SocketAddr;

use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::sqlite::SqliteMetadataStore;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_RANGE, RANGE};
use serde_json::Value;

const SIZE: usize = 1024 * 1024;

async fn start() -> (String, Vec<u8>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();
    let app = build_router(AppState::local(cas, meta));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Deterministic content: byte i == (i % 251) so slicing is verifiable.
    let data: Vec<u8> = (0..SIZE).map(|i| (i % 251) as u8).collect();
    let base = format!("http://{addr}");

    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/upload/file?path=range.bin"))
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    (base, data, tmp)
}

#[tokio::test]
async fn range_requests_return_partial_content() {
    let (base, data, _tmp) = start().await;
    let client = reqwest::Client::new();

    let full = client
        .get(format!("{base}/api/v1/version/main/file/range.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.headers()[ACCEPT_RANGES], "bytes");
    assert!(
        full.headers()[CONTENT_DISPOSITION]
            .to_str()
            .unwrap()
            .contains("filename*=UTF-8''range.bin"),
        "RFC 5987 filename must be present"
    );
    assert_eq!(full.bytes().await.unwrap().len(), SIZE);

    // Explicit window.
    let part = client
        .get(format!("{base}/api/v1/version/main/file/range.bin"))
        .header(RANGE, "bytes=100-199")
        .send()
        .await
        .unwrap();
    assert_eq!(part.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(part.headers()[CONTENT_RANGE], format!("bytes 100-199/{SIZE}"));
    assert_eq!(part.bytes().await.unwrap().as_ref(), &data[100..=199]);

    // Suffix window (last 64 bytes).
    let tail = client
        .get(format!("{base}/api/v1/version/main/file/range.bin"))
        .header(RANGE, "bytes=-64")
        .send()
        .await
        .unwrap();
    assert_eq!(tail.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(tail.bytes().await.unwrap().as_ref(), &data[SIZE - 64..]);

    // Open-ended window.
    let open = client
        .get(format!("{base}/api/v1/version/main/file/range.bin"))
        .header(RANGE, format!("bytes={}-", SIZE - 6))
        .send()
        .await
        .unwrap();
    assert_eq!(open.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(open.bytes().await.unwrap().as_ref(), &data[SIZE - 6..]);

    // Unsatisfiable range.
    let bad = client
        .get(format!("{base}/api/v1/version/main/file/range.bin"))
        .header(RANGE, format!("bytes={}-", SIZE + 10))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(bad.headers()[CONTENT_RANGE], format!("bytes */{SIZE}"));
}

#[tokio::test]
async fn hostile_filename_cannot_break_response_headers() {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();
    let app = build_router(AppState::local(cas, meta));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Filename containing a quote and CR/LF metacharacters.
    let resp = client
        .post(format!(
            "{base}/api/v1/upload/file?path=evil%22name%0D%0Abin.txt"
        ))
        .body(b"hello".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!(
            "{base}/api/v1/version/main/file/evil%22name%0D%0Abin.txt"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let disposition = resp.headers()[CONTENT_DISPOSITION].to_str().unwrap().to_owned();
    assert!(
        !disposition.contains('\r') && !disposition.contains('\n'),
        "CR/LF must never reach the header: {disposition}"
    );
    assert_eq!(resp.bytes().await.unwrap().as_ref(), b"hello");

    let _ = &base;
    let _: Option<Value> = None;
}
