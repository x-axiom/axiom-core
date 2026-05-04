#![cfg(feature = "local")]
//! Integration tests for streaming download and directory listing (AXIOM-111).

use std::net::SocketAddr;

use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::RocksDbCasStore;
use base64::Engine;
use reqwest::{Client, StatusCode};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> (String, Client, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();

    let app = build_router(AppState::local(cas, meta));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    (format!("http://{addr}"), Client::new(), tmp)
}

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Upload a directory and return the version_id.
async fn upload_dir(
    base: &str,
    client: &Client,
    files: &[(&str, &[u8])],
    branch: Option<&str>,
) -> Value {
    let entries: Vec<Value> = files
        .iter()
        .map(|(path, data)| {
            serde_json::json!({
                "path": path,
                "content_base64": b64(data),
            })
        })
        .collect();

    let mut body = serde_json::json!({ "files": entries, "message": "test upload" });
    if let Some(b) = branch {
        body["branch"] = serde_json::json!(b);
    }

    client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Download by version id — byte-for-byte equality
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_file_by_version_id() {
    let (base, client, _tmp) = start_server().await;
    let content = b"hello from download test!";

    let uploaded = upload_dir(&base, &client, &[("greeting.txt", content)], None).await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/file/greeting.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/octet-stream"
    );

    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), content);
}

// ---------------------------------------------------------------------------
// Download by branch name
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_file_by_branch() {
    let (base, client, _tmp) = start_server().await;
    let content = b"branch content";

    upload_dir(&base, &client, &[("data.bin", content)], Some("main")).await;

    let resp = client
        .get(format!("{base}/api/v1/version/main/file/data.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content);
}

// ---------------------------------------------------------------------------
// Download by tag name
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_file_by_tag() {
    let (base, client, _tmp) = start_server().await;
    let content = b"tagged release content";

    let uploaded = upload_dir(&base, &client, &[("release.bin", content)], None).await;
    let vid = uploaded["version_id"].as_str().unwrap();

    // Create a tag pointing to this version.
    client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "v1.0",
            "kind": "tag",
            "target": vid,
        }))
        .send()
        .await
        .unwrap();

    // Download via tag.
    let resp = client
        .get(format!("{base}/api/v1/version/v1.0/file/release.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content);
}

// ---------------------------------------------------------------------------
// Download nested file
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_nested_file() {
    let (base, client, _tmp) = start_server().await;
    let content = b"fn main() { println!(\"hello\"); }";

    let uploaded = upload_dir(
        &base,
        &client,
        &[
            ("src/main.rs", content),
            ("src/lib.rs", b"pub mod foo;"),
        ],
        None,
    )
    .await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/file/src/main.rs"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), content);
}

// ---------------------------------------------------------------------------
// Download — missing path returns 404
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_missing_path_404() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(&base, &client, &[("a.txt", b"data")], None).await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/file/nonexistent.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "not_found");
}

// ---------------------------------------------------------------------------
// Download — invalid ref returns 404
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_invalid_ref_404() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .get(format!(
            "{base}/api/v1/version/nonexistent-ref/file/any.txt"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Download directory path returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_download_directory_path_returns_400() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(
        &base,
        &client,
        &[("src/main.rs", b"fn main() {}")],
        None,
    )
    .await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/file/src"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Directory listing — root
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_root_directory() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(
        &base,
        &client,
        &[
            ("src/main.rs", b"fn main() {}"),
            ("README.md", b"# Hello"),
        ],
        None,
    )
    .await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/ls"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["version_id"], vid);
    assert_eq!(body["path"], "");

    let entries = body["entries"].as_array().unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"src"));
    assert!(names.contains(&"README.md"));

    // "src" should be a directory.
    let src = entries.iter().find(|e| e["name"] == "src").unwrap();
    assert_eq!(src["is_directory"], true);

    // "README.md" should be a file with size > 0.
    let readme = entries.iter().find(|e| e["name"] == "README.md").unwrap();
    assert_eq!(readme["is_directory"], false);
    assert!(readme["size"].as_u64().unwrap() > 0);
}

// ---------------------------------------------------------------------------
// Directory listing — subdirectory
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_subdirectory() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(
        &base,
        &client,
        &[
            ("src/main.rs", b"fn main() {}"),
            ("src/lib.rs", b"pub mod foo;"),
        ],
        None,
    )
    .await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/ls/src"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["path"], "src");

    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    let names: Vec<&str> = entries.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"main.rs"));
    assert!(names.contains(&"lib.rs"));
}

// ---------------------------------------------------------------------------
// Directory listing — missing path 404
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_missing_dir_404() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(&base, &client, &[("a.txt", b"data")], None).await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/ls/nonexistent"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Large file round-trip (1 MB)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_large_file_roundtrip() {
    let (base, client, _tmp) = start_server().await;

    // 1 MB pseudo-random data.
    let mut data = vec![0u8; 1024 * 1024];
    let mut state: u64 = 0xCAFE_BABE;
    for byte in data.iter_mut() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *byte = (state >> 33) as u8;
    }

    // Upload via single-file endpoint.
    let resp = client
        .post(format!(
            "{base}/api/v1/upload/file?path=big.bin&message=large"
        ))
        .body(data.clone())
        .send()
        .await
        .unwrap();
    let uploaded: Value = resp.json().await.unwrap();
    let vid = uploaded["version_id"].as_str().unwrap();

    // Download and verify byte equality.
    let dl_resp = client
        .get(format!("{base}/api/v1/version/{vid}/file/big.bin"))
        .send()
        .await
        .unwrap();
    assert_eq!(dl_resp.status(), StatusCode::OK);
    assert_eq!(
        dl_resp.headers().get("content-length").unwrap().to_str().unwrap(),
        data.len().to_string()
    );
    let downloaded = dl_resp.bytes().await.unwrap();
    assert_eq!(downloaded.len(), data.len());
    assert_eq!(downloaded.as_ref(), data.as_slice());
}

// ---------------------------------------------------------------------------
// List on file path returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_list_on_file_path_returns_400() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(&base, &client, &[("doc.txt", b"content")], None).await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/version/{vid}/ls/doc.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
