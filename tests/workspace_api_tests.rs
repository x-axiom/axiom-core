#![cfg(feature = "local")]

use std::net::SocketAddr;

use axiom_core::api::{build_router, state::AppState};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::RocksDbCasStore;
use reqwest::StatusCode;
use serde_json::Value;

async fn start_server() -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas_path = tmp.path().join("cas");
    let meta_path = tmp.path().join("meta.db");

    let cas = RocksDbCasStore::open(&cas_path).unwrap();
    let meta = SqliteMetadataStore::open(&meta_path).unwrap();

    let app = build_router(AppState::local(cas, meta));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), tmp)
}

#[tokio::test]
async fn workspace_http_contract_list_create_delete() {
    let (base, _tmp) = start_server().await;
    let client = reqwest::Client::new();

    let list_resp = client
        .get(format!("{base}/api/v1/workspaces"))
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let listed: Value = list_resp.json().await.unwrap();
    assert!(listed.as_array().unwrap().iter().any(|ws| ws["id"] == "default"));

    let create_resp = client
        .post(format!("{base}/api/v1/workspaces"))
        .json(&serde_json::json!({ "name": "team-alpha" }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::OK);
    let created: Value = create_resp.json().await.unwrap();
    let workspace_id = created["id"].as_str().unwrap().to_string();
    assert_eq!(created["name"], "team-alpha");
    assert!(created["created_at"].as_str().unwrap().contains('T'));
    assert!(created["updated_at"].as_str().unwrap().contains('T'));

    let dup_resp = client
        .post(format!("{base}/api/v1/workspaces"))
        .json(&serde_json::json!({ "name": "team-alpha" }))
        .send()
        .await
        .unwrap();
    assert_eq!(dup_resp.status(), StatusCode::CONFLICT);

    let delete_resp = client
        .delete(format!("{base}/api/v1/workspaces/{workspace_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn workspace_http_contract_tree_versions_and_download() {
    let (base, _tmp) = start_server().await;
    let client = reqwest::Client::new();

    let upload_resp = client
        .post(format!("{base}/api/v1/upload/directory"))
        .json(&serde_json::json!({
            "files": [
                { "path": "src/main.rs", "content_base64": base64::encode("fn main() {}") },
                { "path": "README.md", "content_base64": base64::encode("hello") }
            ],
            "message": "initial"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), StatusCode::OK);

    let tree_resp = client
        .get(format!("{base}/api/v1/workspaces/default/tree"))
        .send()
        .await
        .unwrap();
    assert_eq!(tree_resp.status(), StatusCode::OK);
    let tree: Value = tree_resp.json().await.unwrap();
    let names: Vec<&str> = tree.as_array().unwrap().iter().map(|entry| entry["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"README.md"));
    assert!(names.contains(&"src"));

    let versions_resp = client
        .get(format!("{base}/api/v1/workspaces/default/versions?limit=10"))
        .send()
        .await
        .unwrap();
    assert_eq!(versions_resp.status(), StatusCode::OK);
    let versions: Value = versions_resp.json().await.unwrap();
    assert_eq!(versions["items"].as_array().unwrap().len(), 1);
    assert!(versions["items"][0]["timestamp"].as_str().unwrap().contains('T'));
    assert!(versions.get("next_cursor").is_none() || versions["next_cursor"].is_null());

    let download_resp = client
        .get(format!("{base}/api/v1/workspaces/default/download/src/main.rs"))
        .send()
        .await
        .unwrap();
    assert_eq!(download_resp.status(), StatusCode::OK);
    assert_eq!(download_resp.bytes().await.unwrap(), "fn main() {}".as_bytes());
}

#[tokio::test]
async fn workspace_http_contract_diff_route() {
    let (base, _tmp) = start_server().await;
    let client = reqwest::Client::new();

    let first: Value = client
        .post(format!("{base}/api/v1/upload/file?path=notes.txt&message=v1"))
        .body("hello")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let second: Value = client
        .post(format!("{base}/api/v1/upload/file?path=notes.txt&message=v2"))
        .body("hello world")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let from_id = first["version_id"].as_str().unwrap();
    let to_id = second["version_id"].as_str().unwrap();

    let diff_resp = client
        .get(format!(
            "{base}/api/v1/workspaces/default/diff?from={from_id}&to={to_id}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(diff_resp.status(), StatusCode::OK);
    let diff: Value = diff_resp.json().await.unwrap();
    assert!(diff.as_array().unwrap().iter().any(|entry| {
        entry["path"] == "notes.txt" && entry["kind"] == "modified"
    }));
}