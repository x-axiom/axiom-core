//! HTTP API smoke tests.
//!
//! These tests spin up an in-process axum server on a random port, then
//! exercise the key routes via reqwest.

use std::net::SocketAddr;

use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::RocksDbCasStore;
use reqwest::StatusCode;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Boot the axum app on a random port and return the base URL.
async fn start_server() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas_path = tmp.path().join("cas");
    let meta_path = tmp.path().join("meta.db");

    let cas = RocksDbCasStore::open(&cas_path).unwrap();
    let meta = SqliteMetadataStore::open(&meta_path).unwrap();

    let state = AppState::new(cas, meta);
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), tmp)
}

// ---------------------------------------------------------------------------
// Health endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_endpoint() {
    let (base, _tmp) = start_server().await;

    let resp = reqwest::get(format!("{base}/health")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert!(body["version"].as_str().unwrap().starts_with("0."));
}

// ---------------------------------------------------------------------------
// Object info — nonexistent hash returns exists: false
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_object_info_not_found() {
    let (base, _tmp) = start_server().await;

    // Valid 32-byte hex hash that doesn't exist.
    let hash = "a".repeat(64);
    let resp = reqwest::get(format!("{base}/api/v1/objects/{hash}"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["exists"], false);
    assert_eq!(body["size"], 0);
}

// ---------------------------------------------------------------------------
// Object info — invalid hash returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_object_info_bad_hash() {
    let (base, _tmp) = start_server().await;

    let resp = reqwest::get(format!("{base}/api/v1/objects/not-a-hash"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_object");
}

// ---------------------------------------------------------------------------
// Version create + get
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_version_create_and_get() {
    let (base, _tmp) = start_server().await;
    let client = reqwest::Client::new();

    let root_hash = blake3::hash(b"test-root").to_hex().to_string();

    // Create version.
    let create_resp = client
        .post(format!("{base}/api/v1/versions"))
        .json(&serde_json::json!({
            "root": root_hash,
            "parents": [],
            "message": "test commit"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::OK);

    let created: Value = create_resp.json().await.unwrap();
    let version_id = created["id"].as_str().unwrap().to_string();
    assert!(!version_id.is_empty());
    assert_eq!(created["message"], "test commit");

    // Get version.
    let get_resp = reqwest::get(format!("{base}/api/v1/versions/{version_id}"))
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);

    let fetched: Value = get_resp.json().await.unwrap();
    assert_eq!(fetched["id"], version_id);
}

// ---------------------------------------------------------------------------
// Version not found
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_version_not_found() {
    let (base, _tmp) = start_server().await;

    let resp = reqwest::get(format!("{base}/api/v1/versions/nonexistent"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Refs: create branch, list, resolve
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_refs_branch_lifecycle() {
    let (base, _tmp) = start_server().await;
    let client = reqwest::Client::new();

    let root_hash = blake3::hash(b"ref-root").to_hex().to_string();

    // Create a version first (commit auto-creates "main" branch).
    let create_resp = client
        .post(format!("{base}/api/v1/versions"))
        .json(&serde_json::json!({
            "root": root_hash,
            "parents": [],
            "message": "init"
        }))
        .send()
        .await
        .unwrap();
    let created: Value = create_resp.json().await.unwrap();
    let version_id = created["id"].as_str().unwrap().to_string();

    // Get the auto-created "main" ref.
    let main_resp = reqwest::get(format!("{base}/api/v1/refs/main"))
        .await
        .unwrap();
    assert_eq!(main_resp.status(), StatusCode::OK);

    let main_ref: Value = main_resp.json().await.unwrap();
    assert_eq!(main_ref["name"], "main");
    assert_eq!(main_ref["kind"], "branch");
    assert_eq!(main_ref["target"], version_id);

    // Resolve ref → version.
    let resolve_resp = reqwest::get(format!("{base}/api/v1/refs/main/resolve"))
        .await
        .unwrap();
    assert_eq!(resolve_resp.status(), StatusCode::OK);

    let resolved: Value = resolve_resp.json().await.unwrap();
    assert_eq!(resolved["id"], version_id);

    // List refs.
    let list_resp = reqwest::get(format!("{base}/api/v1/refs"))
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);

    let list: Value = list_resp.json().await.unwrap();
    assert!(list["refs"].as_array().unwrap().len() >= 1);
}

// ---------------------------------------------------------------------------
// Refs: tag immutability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_tag_create_and_overwrite_rejected() {
    let (base, _tmp) = start_server().await;
    let client = reqwest::Client::new();

    let root_hash = blake3::hash(b"tag-root").to_hex().to_string();

    // Create a version.
    let v: Value = client
        .post(format!("{base}/api/v1/versions"))
        .json(&serde_json::json!({
            "root": root_hash,
            "parents": [],
            "message": "for tag"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let vid = v["id"].as_str().unwrap();

    // Create tag.
    let tag_resp = client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "v1.0",
            "kind": "tag",
            "target": vid
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(tag_resp.status(), StatusCode::OK);

    // Attempt to create same tag again → should fail.
    let dup_resp = client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "v1.0",
            "kind": "tag",
            "target": vid
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(dup_resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Ref not found
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ref_not_found() {
    let (base, _tmp) = start_server().await;

    let resp = reqwest::get(format!("{base}/api/v1/refs/nonexistent"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Error response structure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_error_response_structure() {
    let (base, _tmp) = start_server().await;

    let resp = reqwest::get(format!("{base}/api/v1/versions/does-not-exist"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let body: Value = resp.json().await.unwrap();
    // Should have "error" and "message" fields.
    assert!(body["error"].is_string());
    assert!(body["message"].is_string());
    assert_eq!(body["error"], "not_found");
}

// ---------------------------------------------------------------------------
// Version history
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_version_history() {
    let (base, _tmp) = start_server().await;
    let client = reqwest::Client::new();

    let root = blake3::hash(b"hist-root").to_hex().to_string();

    // First commit.
    let v1: Value = client
        .post(format!("{base}/api/v1/versions"))
        .json(&serde_json::json!({
            "root": root,
            "parents": [],
            "message": "c1"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let v1_id = v1["id"].as_str().unwrap().to_string();

    // Second commit with parent.
    let root2 = blake3::hash(b"hist-root-2").to_hex().to_string();
    let v2: Value = client
        .post(format!("{base}/api/v1/versions"))
        .json(&serde_json::json!({
            "root": root2,
            "parents": [v1_id],
            "message": "c2"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let v2_id = v2["id"].as_str().unwrap().to_string();

    // Query history from v2.
    let history_resp = reqwest::get(format!("{base}/api/v1/versions/{v2_id}/history?limit=10"))
        .await
        .unwrap();
    assert_eq!(history_resp.status(), StatusCode::OK);

    let history: Value = history_resp.json().await.unwrap();
    let versions = history["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0]["id"], v2_id);
    assert_eq!(versions[1]["id"], v1_id);
}
