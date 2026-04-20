//! End-to-end validation tests for the complete POC workflow (AXIOM-113).
//!
//! Each test exercises a realistic multi-step scenario that spans upload,
//! commit, tagging, diff, download, and query operations — validating that
//! the system works as a coherent whole rather than as isolated components.

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

async fn upload_dir(
    base: &str,
    client: &Client,
    files: &[(&str, &[u8])],
    msg: &str,
    parents: &[&str],
) -> Value {
    let entries: Vec<Value> = files
        .iter()
        .map(|(p, d)| serde_json::json!({ "path": p, "content_base64": b64(d) }))
        .collect();
    client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": entries,
            "message": msg,
            "parents": parents,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

// ===========================================================================
// 1. Full lifecycle: upload → commit → tag → diff → download
// ===========================================================================

#[tokio::test]
async fn test_full_lifecycle_upload_tag_diff_download() {
    let (base, client, _tmp) = start_server().await;

    // --- Step 1: Upload initial version with two files ---
    let v1 = upload_dir(
        &base,
        &client,
        &[
            ("src/main.rs", b"fn main() { println!(\"hello\"); }"),
            ("README.md", b"# Project\nInitial commit."),
        ],
        "initial commit",
        &[],
    )
    .await;
    let v1_id = v1["version_id"].as_str().unwrap();
    assert!(!v1_id.is_empty());
    assert_eq!(v1["branch"], "main");

    // --- Step 2: Tag v1 as "v1.0" ---
    let tag_resp = client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "v1.0",
            "kind": "tag",
            "target": v1_id,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(tag_resp.status(), StatusCode::OK);

    // --- Step 3: Upload second version (modify + add file) ---
    let v2 = upload_dir(
        &base,
        &client,
        &[
            ("src/main.rs", b"fn main() { println!(\"world\"); }"),
            ("README.md", b"# Project\nUpdated."),
            ("src/lib.rs", b"pub mod utils;"),
        ],
        "second commit",
        &[],
    )
    .await;
    let v2_id = v2["version_id"].as_str().unwrap();
    assert_ne!(v1_id, v2_id);

    // --- Step 4: Diff v1.0 (tag) vs main (branch) ---
    let diff: Value = client
        .post(format!("{base}/api/v1/diff"))
        .json(&serde_json::json!({
            "old_version": "v1.0",
            "new_version": "main",
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entries = diff["entries"].as_array().unwrap();
    assert!(!entries.is_empty());
    // src/lib.rs was added
    assert!(entries.iter().any(|e| e["path"] == "src/lib.rs" && e["kind"] == "added"));

    // --- Step 5: Download a file from v2 by branch name ---
    let file_resp = client
        .get(format!("{base}/api/v1/version/main/file/src/main.rs"))
        .send()
        .await
        .unwrap();
    assert_eq!(file_resp.status(), StatusCode::OK);
    let body = file_resp.bytes().await.unwrap();
    assert_eq!(&body[..], b"fn main() { println!(\"world\"); }");

    // --- Step 6: Download a file from v1 by tag name ---
    let file_v1 = client
        .get(format!("{base}/api/v1/version/v1.0/file/src/main.rs"))
        .send()
        .await
        .unwrap();
    assert_eq!(file_v1.status(), StatusCode::OK);
    let body_v1 = file_v1.bytes().await.unwrap();
    assert_eq!(&body_v1[..], b"fn main() { println!(\"hello\"); }");
}

// ===========================================================================
// 2. Version history with pagination and branch resolution
// ===========================================================================

#[tokio::test]
async fn test_version_history_pagination_flow() {
    let (base, client, _tmp) = start_server().await;

    // Create 4 sequential versions.
    let mut ids = Vec::new();
    for i in 0..4 {
        let content = format!("version {i}");
        let parents = if let Some(last) = ids.last() {
            format!("&parents={last}")
        } else {
            String::new()
        };
        let resp: Value = client
            .post(format!(
                "{base}/api/v1/upload/file?path=data.txt&message=v{i}{parents}"
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

    // Query full history via "main" branch.
    let history: Value = client
        .get(format!("{base}/api/v1/versions/main/history"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(history["versions"].as_array().unwrap().len(), 4);
    assert_eq!(history["has_more"], false);

    // Page through: 2 per page.
    let p1: Value = client
        .get(format!(
            "{base}/api/v1/versions/main/history?limit=2&offset=0"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(p1["versions"].as_array().unwrap().len(), 2);
    assert_eq!(p1["has_more"], true);
    // Most recent first.
    assert_eq!(p1["versions"][0]["id"], ids[3]);

    let p2: Value = client
        .get(format!(
            "{base}/api/v1/versions/main/history?limit=2&offset=2"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(p2["versions"].as_array().unwrap().len(), 2);
    assert_eq!(p2["has_more"], false);
    assert_eq!(p2["versions"][0]["id"], ids[1]);
}

// ===========================================================================
// 3. Tag creation, resolution, and immutability
// ===========================================================================

#[tokio::test]
async fn test_tag_creation_resolve_and_immutability() {
    let (base, client, _tmp) = start_server().await;

    // Upload a version.
    let v1 = upload_dir(&base, &client, &[("a.txt", b"alpha")], "v1", &[]).await;
    let v1_id = v1["version_id"].as_str().unwrap();

    // Tag it.
    let tag: Value = client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "release-1.0",
            "kind": "tag",
            "target": v1_id,
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tag["name"], "release-1.0");
    assert_eq!(tag["kind"], "tag");

    // Resolve tag.
    let resolved: Value = client
        .get(format!("{base}/api/v1/refs/release-1.0/resolve"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resolved["id"], v1_id);

    // Upload v2 to advance main — tag should still point to v1.
    upload_dir(&base, &client, &[("a.txt", b"beta")], "v2", &[]).await;

    let still_v1: Value = client
        .get(format!("{base}/api/v1/refs/release-1.0/resolve"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(still_v1["id"], v1_id);

    // Attempting to overwrite tag should fail.
    let overwrite = client
        .put(format!("{base}/api/v1/refs/release-1.0"))
        .json(&serde_json::json!({ "target": "some-other-id" }))
        .send()
        .await
        .unwrap();
    assert!(overwrite.status().is_client_error() || overwrite.status().is_server_error());
}

// ===========================================================================
// 4. Directory listing and node metadata
// ===========================================================================

#[tokio::test]
async fn test_directory_listing_and_node_metadata() {
    let (base, client, _tmp) = start_server().await;

    let v = upload_dir(
        &base,
        &client,
        &[
            ("src/main.rs", b"fn main() {}"),
            ("src/lib.rs", b"pub mod x;"),
            ("docs/guide.md", b"# Guide"),
        ],
        "init",
        &[],
    )
    .await;
    let vid = v["version_id"].as_str().unwrap();

    // List root directory.
    let root_ls: Value = client
        .get(format!("{base}/api/v1/version/{vid}/ls"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let root_entries = root_ls["entries"].as_array().unwrap();
    let names: Vec<&str> = root_entries.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"src"));
    assert!(names.contains(&"docs"));

    // List src/ subdirectory.
    let src_ls: Value = client
        .get(format!("{base}/api/v1/version/{vid}/ls/src"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let src_entries = src_ls["entries"].as_array().unwrap();
    assert_eq!(src_entries.len(), 2);
    let src_names: Vec<&str> = src_entries.iter().map(|e| e["name"].as_str().unwrap()).collect();
    assert!(src_names.contains(&"main.rs"));
    assert!(src_names.contains(&"lib.rs"));

    // Node metadata for a file.
    let file_meta: Value = client
        .get(format!("{base}/api/v1/versions/{vid}/path/src/main.rs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(file_meta["is_directory"], false);
    assert_eq!(file_meta["path"], "src/main.rs");
    assert!(file_meta["size"].as_u64().unwrap() > 0);

    // Node metadata for a directory.
    let dir_meta: Value = client
        .get(format!("{base}/api/v1/versions/{vid}/path/src"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dir_meta["is_directory"], true);
    let children = dir_meta["children"].as_array().unwrap();
    assert!(children.iter().any(|c| c == "main.rs"));
}

// ===========================================================================
// 5. Diff shows correct change categories
// ===========================================================================

#[tokio::test]
async fn test_diff_change_categories() {
    let (base, client, _tmp) = start_server().await;

    let v1 = upload_dir(
        &base,
        &client,
        &[
            ("keep.txt", b"unchanged"),
            ("modify.txt", b"original"),
            ("remove.txt", b"will be removed"),
        ],
        "baseline",
        &[],
    )
    .await;

    let v2 = upload_dir(
        &base,
        &client,
        &[
            ("keep.txt", b"unchanged"),
            ("modify.txt", b"modified content"),
            ("added.txt", b"brand new"),
        ],
        "changes",
        &[],
    )
    .await;

    let diff: Value = client
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

    let entries = diff["entries"].as_array().unwrap();
    assert!(entries.iter().any(|e| e["path"] == "added.txt" && e["kind"] == "added"));
    assert!(entries.iter().any(|e| e["path"] == "remove.txt" && e["kind"] == "removed"));
    assert!(entries.iter().any(|e| e["path"] == "modify.txt" && e["kind"] == "modified"));
    // "keep.txt" should NOT appear in diff (unchanged).
    assert!(entries.iter().all(|e| e["path"] != "keep.txt"));

    assert!(diff["added_files"].as_u64().unwrap() >= 1);
    assert!(diff["removed_files"].as_u64().unwrap() >= 1);
    assert!(diff["modified_files"].as_u64().unwrap() >= 1);
}

// ===========================================================================
// 6. Deduplication across versions
// ===========================================================================

#[tokio::test]
async fn test_deduplication_across_versions() {
    let (base, client, _tmp) = start_server().await;

    // Upload large enough content to produce chunks.
    let big = vec![42u8; 128 * 1024]; // 128KB
    let v1: Value = client
        .post(format!(
            "{base}/api/v1/upload/file?path=data.bin&message=first"
        ))
        .body(big.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let new1 = v1["stats"]["new_chunks"].as_u64().unwrap();
    let dedup1 = v1["stats"]["dedup_chunks"].as_u64().unwrap();

    // Upload same content again — all chunks should be deduped.
    let v2: Value = client
        .post(format!(
            "{base}/api/v1/upload/file?path=data.bin&message=second"
        ))
        .body(big)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let new2 = v2["stats"]["new_chunks"].as_u64().unwrap();
    let dedup2 = v2["stats"]["dedup_chunks"].as_u64().unwrap();

    assert!(new1 > 0);
    assert_eq!(new2, 0);
    assert_eq!(dedup1, 0);
    assert!(dedup2 > 0);
}

// ===========================================================================
// 7. Download verification — content integrity
// ===========================================================================

#[tokio::test]
async fn test_download_content_integrity() {
    let (base, client, _tmp) = start_server().await;

    let original = b"The quick brown fox jumps over the lazy dog.\n".repeat(100);
    let v: Value = client
        .post(format!(
            "{base}/api/v1/upload/file?path=story.txt&message=text"
        ))
        .body(original.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let vid = v["version_id"].as_str().unwrap();

    let downloaded = client
        .get(format!("{base}/api/v1/version/{vid}/file/story.txt"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    assert_eq!(downloaded.as_ref(), original.as_slice());
}

// ===========================================================================
// 8. Branch creation and switching
// ===========================================================================

#[tokio::test]
async fn test_branch_creation_and_divergence() {
    let (base, client, _tmp) = start_server().await;

    // Upload to main.
    let v1 = upload_dir(&base, &client, &[("a.txt", b"shared base")], "base", &[]).await;
    let v1_id = v1["version_id"].as_str().unwrap();

    // Create a feature branch from v1.
    client
        .post(format!("{base}/api/v1/refs"))
        .json(&serde_json::json!({
            "name": "feature",
            "kind": "branch",
            "target": v1_id,
        }))
        .send()
        .await
        .unwrap();

    // Upload v2 to main (advances main but not feature).
    upload_dir(&base, &client, &[("a.txt", b"main advanced")], "v2 on main", &[]).await;

    // Resolve both branches and verify they diverged.
    let main_head: Value = client
        .get(format!("{base}/api/v1/refs/main/resolve"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let feature_head: Value = client
        .get(format!("{base}/api/v1/refs/feature/resolve"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_ne!(main_head["id"], feature_head["id"]);
    assert_eq!(feature_head["id"], v1_id);

    // Download from feature branch — should still have original content.
    let body = client
        .get(format!("{base}/api/v1/version/feature/file/a.txt"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&body[..], b"shared base");
}

// ===========================================================================
// 9. Error handling: 404 on missing resources
// ===========================================================================

#[tokio::test]
async fn test_error_handling_missing_resources() {
    let (base, client, _tmp) = start_server().await;

    // Non-existent version.
    let r = client
        .get(format!("{base}/api/v1/versions/nonexistent"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);

    // Non-existent file path (after creating a version).
    upload_dir(&base, &client, &[("a.txt", b"data")], "init", &[]).await;
    let r = client
        .get(format!("{base}/api/v1/version/main/file/nope.txt"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);

    // Non-existent ref.
    let r = client
        .get(format!("{base}/api/v1/refs/ghost/resolve"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);

    // Diff with invalid ref.
    let r = client
        .post(format!("{base}/api/v1/diff"))
        .json(&serde_json::json!({
            "old_version": "nope",
            "new_version": "also-nope",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}

// ===========================================================================
// 10. Health check works
// ===========================================================================

#[tokio::test]
async fn test_health_check() {
    let (base, client, _tmp) = start_server().await;

    let resp: Value = client
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["status"], "ok");
    assert!(resp["version"].as_str().unwrap().len() > 0);
}
