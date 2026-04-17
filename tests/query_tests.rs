//! Integration tests for version history, refs, diff query APIs (AXIOM-112).

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

/// Upload directory, return full response body.
async fn upload_dir(base: &str, client: &Client, files: &[(&str, &[u8])], msg: &str) -> Value {
    let entries: Vec<Value> = files
        .iter()
        .map(|(p, d)| serde_json::json!({ "path": p, "content_base64": b64(d) }))
        .collect();
    client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({ "files": entries, "message": msg }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Build a chain of N versions, return all version IDs.
async fn build_chain(base: &str, client: &Client, count: usize) -> Vec<String> {
    let mut ids = Vec::new();
    for i in 0..count {
        let content = format!("content-{i}");
        let parents = if let Some(last) = ids.last() {
            format!("&parents={last}")
        } else {
            String::new()
        };
        let resp: Value = client
            .post(format!(
                "{base}/api/v1/upload/file?path=file.txt&message=commit-{i}{parents}"
            ))
            .body(content.into_bytes())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        ids.push(resp["version_id"].as_str().unwrap().to_string());
    }
    ids
}

// ===========================================================================
// Paginated version history
// ===========================================================================

#[tokio::test]
async fn test_history_pagination_basics() {
    let (base, client, _tmp) = start_server().await;
    let ids = build_chain(&base, &client, 5).await;
    let tip = ids.last().unwrap();

    // Default: limit=50, offset=0 → all 5.
    let resp: Value = client
        .get(format!("{base}/api/v1/versions/{tip}/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["versions"].as_array().unwrap().len(), 5);
    assert_eq!(resp["offset"], 0);
    assert_eq!(resp["has_more"], false);
}

#[tokio::test]
async fn test_history_pagination_limit_offset() {
    let (base, client, _tmp) = start_server().await;
    let ids = build_chain(&base, &client, 5).await;
    let tip = ids.last().unwrap();

    // Page 1: limit=2, offset=0.
    let p1: Value = client
        .get(format!(
            "{base}/api/v1/versions/{tip}/history?limit=2&offset=0"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(p1["versions"].as_array().unwrap().len(), 2);
    assert_eq!(p1["has_more"], true);
    assert_eq!(p1["offset"], 0);
    assert_eq!(p1["limit"], 2);

    // Page 2: limit=2, offset=2.
    let p2: Value = client
        .get(format!(
            "{base}/api/v1/versions/{tip}/history?limit=2&offset=2"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(p2["versions"].as_array().unwrap().len(), 2);
    assert_eq!(p2["has_more"], true);

    // Page 3: limit=2, offset=4.
    let p3: Value = client
        .get(format!(
            "{base}/api/v1/versions/{tip}/history?limit=2&offset=4"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(p3["versions"].as_array().unwrap().len(), 1);
    assert_eq!(p3["has_more"], false);
}

#[tokio::test]
async fn test_history_stable_order() {
    let (base, client, _tmp) = start_server().await;
    let ids = build_chain(&base, &client, 3).await;
    let tip = ids.last().unwrap();

    let resp: Value = client
        .get(format!("{base}/api/v1/versions/{tip}/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let versions = resp["versions"].as_array().unwrap();
    // Most recent first.
    assert_eq!(versions[0]["id"], ids[2]);
    assert_eq!(versions[1]["id"], ids[1]);
    assert_eq!(versions[2]["id"], ids[0]);
}

// ===========================================================================
// History by branch name
// ===========================================================================

#[tokio::test]
async fn test_history_by_branch_name() {
    let (base, client, _tmp) = start_server().await;
    let ids = build_chain(&base, &client, 3).await;

    // Query history via branch "main" instead of version id.
    let resp: Value = client
        .get(format!("{base}/api/v1/versions/main/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["versions"].as_array().unwrap().len(), 3);
    assert_eq!(resp["versions"][0]["id"], ids[2]);
}

// ===========================================================================
// Get version by branch / tag
// ===========================================================================

#[tokio::test]
async fn test_get_version_by_branch() {
    let (base, client, _tmp) = start_server().await;
    let ids = build_chain(&base, &client, 1).await;

    let resp: Value = client
        .get(format!("{base}/api/v1/versions/main"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["id"], ids[0]);
}

#[tokio::test]
async fn test_get_version_by_tag() {
    let (base, client, _tmp) = start_server().await;
    let ids = build_chain(&base, &client, 1).await;

    // Create tag.
    client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "v1.0",
            "kind": "tag",
            "target": ids[0],
        }))
        .send()
        .await
        .unwrap();

    let resp: Value = client
        .get(format!("{base}/api/v1/versions/v1.0"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["id"], ids[0]);
}

// ===========================================================================
// Refs listing
// ===========================================================================

#[tokio::test]
async fn test_refs_listing_with_kind_filter() {
    let (base, client, _tmp) = start_server().await;
    let ids = build_chain(&base, &client, 1).await;

    // Create a tag.
    client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "release-1",
            "kind": "tag",
            "target": ids[0],
        }))
        .send()
        .await
        .unwrap();

    // List all refs.
    let all: Value = client
        .get(format!("{base}/api/v1/refs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(all["refs"].as_array().unwrap().len() >= 2); // main + release-1

    // Filter branches only.
    let branches: Value = client
        .get(format!("{base}/api/v1/refs?kind=branch"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(branches["refs"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["kind"] == "branch"));

    // Filter tags only.
    let tags: Value = client
        .get(format!("{base}/api/v1/refs?kind=tag"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(tags["refs"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["kind"] == "tag"));
    assert_eq!(tags["refs"].as_array().unwrap().len(), 1);
}

// ===========================================================================
// Diff by ref names
// ===========================================================================

#[tokio::test]
async fn test_diff_by_version_ids() {
    let (base, client, _tmp) = start_server().await;

    let v1 = upload_dir(&base, &client, &[("a.txt", b"hello")], "v1").await;
    let v2 = upload_dir(
        &base,
        &client,
        &[("a.txt", b"world"), ("b.txt", b"new")],
        "v2",
    )
    .await;

    let resp: Value = client
        .post(format!("{base}/api/v1/diff"))
        .json(&serde_json::json!({
            "old_version": v1["version_id"],
            "new_version": v2["version_id"],
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(resp["entries"].as_array().unwrap().len() > 0);
    assert!(resp["added_files"].as_u64().unwrap() >= 1); // b.txt added
}

#[tokio::test]
async fn test_diff_by_branch_name() {
    let (base, client, _tmp) = start_server().await;

    // First upload → creates v1 on main.
    let v1 = upload_dir(&base, &client, &[("a.txt", b"hello")], "v1").await;
    let v1_id = v1["version_id"].as_str().unwrap();

    // Create a tag for v1.
    client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "baseline",
            "kind": "tag",
            "target": v1_id,
        }))
        .send()
        .await
        .unwrap();

    // Second upload → advances main.
    upload_dir(
        &base,
        &client,
        &[("a.txt", b"changed"), ("new.txt", b"added")],
        "v2",
    )
    .await;

    // Diff using tag vs branch names.
    let resp: Value = client
        .post(format!("{base}/api/v1/diff"))
        .json(&serde_json::json!({
            "old_version": "baseline",
            "new_version": "main",
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(resp["entries"].as_array().unwrap().len() > 0);
}

#[tokio::test]
async fn test_diff_missing_ref_404() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .post(format!("{base}/api/v1/diff"))
        .json(&serde_json::json!({
            "old_version": "nonexistent",
            "new_version": "also-nonexistent",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ===========================================================================
// Node metadata
// ===========================================================================

#[tokio::test]
async fn test_node_metadata_file() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(
        &base,
        &client,
        &[("src/main.rs", b"fn main() {}"), ("README.md", b"# Hi")],
        "init",
    )
    .await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp: Value = client
        .get(format!("{base}/api/v1/versions/{vid}/path/src/main.rs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["version_id"], vid);
    assert_eq!(resp["path"], "src/main.rs");
    assert_eq!(resp["is_directory"], false);
    assert!(resp["size"].as_u64().unwrap() > 0);
    assert!(resp["hash"].as_str().unwrap().len() == 64);
    assert!(resp.get("children").is_none()); // skip_serializing_if None
}

#[tokio::test]
async fn test_node_metadata_directory() {
    let (base, client, _tmp) = start_server().await;

    let uploaded = upload_dir(
        &base,
        &client,
        &[
            ("src/main.rs", b"fn main() {}"),
            ("src/lib.rs", b"pub mod x;"),
        ],
        "init",
    )
    .await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp: Value = client
        .get(format!("{base}/api/v1/versions/{vid}/path/src"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["is_directory"], true);
    assert_eq!(resp["size"], 0);
    let children = resp["children"].as_array().unwrap();
    assert!(children.iter().any(|c| c == "main.rs"));
    assert!(children.iter().any(|c| c == "lib.rs"));
}

#[tokio::test]
async fn test_node_metadata_by_branch() {
    let (base, client, _tmp) = start_server().await;

    upload_dir(&base, &client, &[("hello.txt", b"world")], "init").await;

    let resp: Value = client
        .get(format!("{base}/api/v1/versions/main/path/hello.txt"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["path"], "hello.txt");
    assert_eq!(resp["is_directory"], false);
}

#[tokio::test]
async fn test_node_metadata_missing_path_404() {
    let (base, client, _tmp) = start_server().await;
    let uploaded = upload_dir(&base, &client, &[("a.txt", b"data")], "init").await;
    let vid = uploaded["version_id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/api/v1/versions/{vid}/path/nope.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ===========================================================================
// Error cases
// ===========================================================================

#[tokio::test]
async fn test_history_invalid_ref_404() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .get(format!(
            "{base}/api/v1/versions/nonexistent/history"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_version_missing_404() {
    let (base, client, _tmp) = start_server().await;

    let resp = client
        .get(format!("{base}/api/v1/versions/does-not-exist"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
