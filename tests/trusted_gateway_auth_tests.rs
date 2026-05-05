#![cfg(feature = "local")]

use std::net::SocketAddr;

use axiom_core::api::{build_router, state::{AppState, HttpAuthMode}};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::store::RocksDbCasStore;
use reqwest::StatusCode;

async fn start_server(auth_mode: HttpAuthMode) -> (String, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let cas_path = tmp.path().join("cas");
    let meta_path = tmp.path().join("meta.db");

    let cas = RocksDbCasStore::open(&cas_path).unwrap();
    let meta = SqliteMetadataStore::open(&meta_path).unwrap();

    let app = build_router(AppState::local(cas, meta).with_http_auth(auth_mode));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://{addr}"), tmp)
}

fn client_for(role: &str) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("x-user-id", "user-1".parse().unwrap());
    headers.insert("x-tenant-id", "tenant-1".parse().unwrap());
    headers.insert("x-org-id", "org-1".parse().unwrap());
    headers.insert("x-roles", role.parse().unwrap());
    reqwest::Client::builder().default_headers(headers).build().unwrap()
}

#[tokio::test]
async fn trusted_gateway_auth_requires_headers_for_protected_routes() {
    let (base, _tmp) = start_server(HttpAuthMode::TrustedGatewayHeaders).await;

    let health = reqwest::get(format!("{base}/health")).await.unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let unauth = reqwest::get(format!("{base}/api/v1/workspaces")).await.unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let authed = client_for("Viewer")
        .get(format!("{base}/api/v1/workspaces"))
        .send()
        .await
        .unwrap();
    assert_eq!(authed.status(), StatusCode::OK);

    let refs = client_for("Viewer")
        .get(format!("{base}/api/v1/refs"))
        .send()
        .await
        .unwrap();
    assert_eq!(refs.status(), StatusCode::OK);
}

#[tokio::test]
async fn trusted_gateway_auth_enforces_workspace_rbac() {
    let (base, _tmp) = start_server(HttpAuthMode::TrustedGatewayHeaders).await;

    let viewer_create = client_for("Viewer")
        .post(format!("{base}/api/v1/workspaces"))
        .json(&serde_json::json!({ "name": "rbac-demo" }))
        .send()
        .await
        .unwrap();
    assert_eq!(viewer_create.status(), StatusCode::FORBIDDEN);

    let member_create = client_for("Member")
        .post(format!("{base}/api/v1/workspaces"))
        .json(&serde_json::json!({ "name": "rbac-demo" }))
        .send()
        .await
        .unwrap();
    assert_eq!(member_create.status(), StatusCode::OK);
    let workspace: serde_json::Value = member_create.json().await.unwrap();
    let workspace_id = workspace["id"].as_str().unwrap();

    let member_delete = client_for("Member")
        .delete(format!("{base}/api/v1/workspaces/{workspace_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(member_delete.status(), StatusCode::FORBIDDEN);

    let admin_delete = client_for("Admin")
        .delete(format!("{base}/api/v1/workspaces/{workspace_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(admin_delete.status(), StatusCode::NO_CONTENT);
}