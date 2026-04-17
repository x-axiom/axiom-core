//! Integration tests for streaming upload API (AXIOM-110).

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

    let app = build_router(AppState::new(cas, meta));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    (format!("http://{addr}"), Client::new(), tmp)
}

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

// ---------------------------------------------------------------------------
// Single-file upload
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_single_file_upload() {
    let (base, client, _tmp) = start_server().await;
    let content = b"hello world from streaming upload test";

    let resp = client
        .post(format!(
            "{base}/api/v1/upload/file?path=greeting.txt&message=first+upload"
        ))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert!(body["version_id"].as_str().unwrap().len() > 0);
    assert!(body["root"].as_str().unwrap().len() == 64); // hex of 32-byte hash
    assert_eq!(body["branch"], "main");
    assert_eq!(body["stats"]["total_files"], 1);
    assert!(body["stats"]["total_bytes"].as_u64().unwrap() > 0);
    assert!(body["stats"]["total_chunks"].as_u64().unwrap() >= 1);
    assert_eq!(body["stats"]["dedup_chunks"], 0); // first upload, nothing deduped
}

// ---------------------------------------------------------------------------
// Single-file upload — custom branch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_single_file_upload_custom_branch() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .post(format!(
            "{base}/api/v1/upload/file?path=a.txt&branch=dev&message=dev+commit"
        ))
        .body(b"dev content".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["branch"], "dev");

    // Verify the branch ref was created.
    let ref_resp = reqwest::get(format!("{base}/api/v1/refs/dev"))
        .await
        .unwrap();
    assert_eq!(ref_resp.status(), StatusCode::OK);
    let ref_body: Value = ref_resp.json().await.unwrap();
    assert_eq!(ref_body["target"], body["version_id"]);
}

// ---------------------------------------------------------------------------
// Directory (multi-file) upload
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_directory_upload() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": [
                { "path": "src/main.rs", "content_base64": b64(b"fn main() {}") },
                { "path": "src/lib.rs", "content_base64": b64(b"pub mod foo;") },
                { "path": "README.md", "content_base64": b64(b"# Hello") }
            ],
            "message": "initial directory upload"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["stats"]["total_files"], 3);
    assert_eq!(body["branch"], "main");
    assert!(body["version_id"].as_str().unwrap().len() > 0);
}

// ---------------------------------------------------------------------------
// Dedup detection — upload same content twice
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_dedup_detection() {
    let (base, client, _tmp) = start_server().await;
    let content = b"identical content for dedup test";

    // First upload.
    let resp1 = client
        .post(format!("{base}/api/v1/upload/file?path=a.txt&message=v1"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    let body1: Value = resp1.json().await.unwrap();
    let new1 = body1["stats"]["new_chunks"].as_u64().unwrap();
    assert!(new1 >= 1);
    assert_eq!(body1["stats"]["dedup_chunks"], 0);

    // Second upload — same content, different path.
    let resp2 = client
        .post(format!("{base}/api/v1/upload/file?path=b.txt&message=v2"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    let body2: Value = resp2.json().await.unwrap();
    // All chunks should be detected as duplicates now.
    assert_eq!(body2["stats"]["dedup_chunks"].as_u64().unwrap(), new1);
    assert_eq!(body2["stats"]["new_chunks"], 0);
    assert!(body2["stats"]["dedup_bytes"].as_u64().unwrap() > 0);
}

// ---------------------------------------------------------------------------
// Large file upload (256 KB — exercises multiple chunks)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_large_file_upload() {
    let (base, client, _tmp) = start_server().await;

    // 1 MB of pseudo-random data to ensure multiple chunks.
    let mut data = vec![0u8; 1024 * 1024];
    // Use a simple PRNG to create varied content that triggers chunk boundaries.
    let mut state: u64 = 0xDEAD_BEEF;
    for byte in data.iter_mut() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *byte = (state >> 33) as u8;
    }

    let resp = client
        .post(format!(
            "{base}/api/v1/upload/file?path=large.bin&message=big+file"
        ))
        .body(data.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["stats"]["total_bytes"].as_u64().unwrap(),
        data.len() as u64
    );
    // With default 64KB avg, 256KB should produce multiple chunks.
    assert!(body["stats"]["total_chunks"].as_u64().unwrap() >= 2);
}

// ---------------------------------------------------------------------------
// Version history chain — upload then verify parent linkage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upload_with_parent_version() {
    let (base, client, _tmp) = start_server().await;

    // First upload.
    let r1: Value = client
        .post(format!("{base}/api/v1/upload/file?path=f.txt&message=v1"))
        .body(b"version1".to_vec())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let v1 = r1["version_id"].as_str().unwrap();

    // Second upload with parent.
    let r2: Value = client
        .post(format!(
            "{base}/api/v1/upload/file?path=f.txt&message=v2&parents={v1}"
        ))
        .body(b"version2".to_vec())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let v2 = r2["version_id"].as_str().unwrap();

    // Get version and check parent.
    let ver: Value = reqwest::get(format!("{base}/api/v1/versions/{v2}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ver["parents"][0], v1);
}

// ---------------------------------------------------------------------------
// Directory upload — empty file list rejected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_directory_upload_empty_files_rejected() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": [],
            "message": "empty"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Directory upload — invalid base64 rejected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_directory_upload_invalid_base64() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": [
                { "path": "bad.txt", "content_base64": "not!valid!base64!!!" }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_object");
}

// ---------------------------------------------------------------------------
// Directory upload — verify version resolves via branch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_directory_upload_branch_resolves() {
    let (base, client, _tmp) = start_server().await;

    let resp: Value = client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": [
                { "path": "a.txt", "content_base64": b64(b"aaa") },
                { "path": "b.txt", "content_base64": b64(b"bbb") }
            ],
            "branch": "release",
            "message": "release build"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let vid = resp["version_id"].as_str().unwrap();

    // Resolve branch → should point to this version.
    let resolved: Value = reqwest::get(format!("{base}/api/v1/refs/release/resolve"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resolved["id"], vid);
}

// ---------------------------------------------------------------------------
// Dedup across directory uploads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_dedup_across_directory_uploads() {
    let (base, client, _tmp) = start_server().await;
    let shared = b64(b"shared content across versions");

    // First directory upload.
    let r1: Value = client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": [
                { "path": "common.txt", "content_base64": shared },
                { "path": "unique1.txt", "content_base64": b64(b"only in v1") }
            ],
            "message": "v1"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r1["stats"]["dedup_chunks"], 0);

    // Second directory upload — common.txt is the same.
    let r2: Value = client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": [
                { "path": "common.txt", "content_base64": shared },
                { "path": "unique2.txt", "content_base64": b64(b"only in v2") }
            ],
            "message": "v2"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    // At least 1 chunk should be deduped (the common.txt content).
    assert!(r2["stats"]["dedup_chunks"].as_u64().unwrap() >= 1);
}
