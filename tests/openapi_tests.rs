//! P2-5: the served OpenAPI document is the API contract.

use std::net::SocketAddr;

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
async fn openapi_spec_is_served_and_covers_the_public_surface() {
    let (base, _tmp) = start().await;

    let resp = reqwest::get(format!("{base}/openapi.json")).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()["content-type"],
        "application/json",
        "spec must be served as JSON"
    );

    let spec: Value = resp.json().await.unwrap();
    assert_eq!(spec["openapi"], "3.1.0");
    assert_eq!(spec["info"]["version"], "0.5.0");

    let paths = spec["paths"].as_object().expect("paths object");
    for required in [
        "/auth/login",
        "/auth/refresh",
        "/auth/logout",
        "/auth/jwks",
        "/api/v1/workspaces",
        "/api/v1/workspaces/{workspace_id}/tree",
        "/api/v1/workspaces/{workspace_id}/versions",
        "/api/v1/workspaces/{workspace_id}/diff",
        "/api/v1/versions/{id}/history",
        "/api/v1/refs",
        "/api/v1/upload/file",
        "/api/v1/version/{ref}/file/{path}",
        "/api/v1/usage",
        "/api/v1/audit",
    ] {
        assert!(paths.contains_key(required), "spec is missing {required}");
    }

    // P2-3: the documented cap must match the implementation.
    let history = &spec["paths"]["/api/v1/versions/{id}/history"]["get"]["parameters"];
    let limit = history
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "limit")
        .expect("history limit parameter");
    assert_eq!(limit["schema"]["maximum"], 100);

    let audit_limit = spec["paths"]["/api/v1/audit"]["get"]["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "limit")
        .expect("audit limit parameter");
    assert_eq!(audit_limit["schema"]["maximum"], 100);

    // The error envelope is part of the contract.
    assert!(spec["components"]["schemas"]["Error"]["properties"]["code"].is_object());
    assert_eq!(
        spec["components"]["securitySchemes"]["bearerAuth"]["scheme"],
        "bearer"
    );
}

#[tokio::test]
async fn docs_page_is_available_without_external_assets() {
    let (base, _tmp) = start().await;
    let resp = reqwest::get(format!("{base}/docs")).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.unwrap();
    assert!(body.contains("/openapi.json"));
    assert!(!body.contains("cdn."), "docs must not depend on a CDN");
}
