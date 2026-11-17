//! P1-3: recycle bin (soft delete → grace period → purge) and the invariant
//! that purging a workspace never deletes content chunks.

use std::net::SocketAddr;
use std::sync::Arc;

use axiom_core::api::{build_router, state::AppState, state::HttpAuthMode};
use axiom_core::auth::jwt::JwtService;
use axiom_core::auth::refresh::MemoryRefreshTokenStore;
use axiom_core::auth::service::AuthService;
use axiom_core::store::RocksDbCasStore;
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::tenant::memory::MemoryTenantDirectory;
use reqwest::StatusCode;
use serde_json::Value;

const DEV_SECRET: &str = "bin-dev-secret";

struct Harness {
    base: String,
    token: String,
    _tmp: tempfile::TempDir,
}

async fn start() -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();

    let jwt = Arc::new(JwtService::new(
        vec![axiom_core::auth::jwt::JwtKeyPair::generate().unwrap()],
        0,
        Arc::new(MemoryRefreshTokenStore::new()),
    ));
    let auth = Arc::new(AuthService::new(
        jwt,
        Arc::new(MemoryTenantDirectory::new()),
        None,
        Some(DEV_SECRET.to_owned()),
    ));

    let state = AppState::local(cas, meta)
        .with_auth(auth)
        .with_http_auth(HttpAuthMode::Jwt);

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let base = format!("http://{addr}");
    let tokens: Value = reqwest::Client::new()
        .post(format!("{base}/auth/login"))
        .header("x-axiom-dev-secret", DEV_SECRET)
        .json(&serde_json::json!({
            "provider": "dev",
            "email": "bin@example.com",
            "name": "Bin"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    Harness {
        base,
        token: tokens["access_token"].as_str().unwrap().to_owned(),
        _tmp: tmp,
    }
}

fn client(h: &Harness) -> reqwest::Client {
    reqwest::Client::new()
}

#[tokio::test]
async fn soft_delete_restore_and_purge_preserve_chunks() {
    let h = start().await;

    // Content that must survive the purge.
    let payload = vec![7u8; 256 * 1024];
    let upload: Value = client(&h)
        .post(format!("{}/api/v1/upload/file?path=keep.bin", h.base))
        .bearer_auth(&h.token)
        .body(payload.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(upload["version_id"].is_string());

    let created: Value = client(&h)
        .post(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(&h.token)
        .json(&serde_json::json!({ "name": "recycle-me" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ws_id = created["id"].as_str().unwrap().to_owned();

    // Delete → hidden from the active list, visible in the recycle bin.
    let deleted = client(&h)
        .delete(format!("{}/api/v1/workspaces/{ws_id}", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

    let active: Vec<Value> = client(&h)
        .get(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !active.iter().any(|w| w["id"] == ws_id.as_str()),
        "deleted workspace must disappear from the active list"
    );

    let bin: Vec<Value> = client(&h)
        .get(format!("{}/api/v1/workspaces/deleted", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(bin.iter().any(|w| w["id"] == ws_id.as_str()));

    // Restore brings it back.
    let restored = client(&h)
        .post(format!("{}/api/v1/workspaces/{ws_id}/restore", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap();
    assert_eq!(restored.status(), StatusCode::NO_CONTENT);

    let active: Vec<Value> = client(&h)
        .get(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(active.iter().any(|w| w["id"] == ws_id.as_str()));

    // Delete again, then dry-run purge inside the grace period.
    client(&h)
        .delete(format!("{}/api/v1/workspaces/{ws_id}", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap();

    let dry: Value = client(&h)
        .post(format!("{}/api/v1/workspaces/{ws_id}/purge?dry_run=true", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dry["purged"], false);
    assert_eq!(dry["eligible"], false);
    assert!(dry["remaining_grace_secs"].as_u64().unwrap() > 0);

    // Grace of 0 seconds makes it purgeable immediately.
    unsafe { std::env::set_var("AXIOM_WORKSPACE_GRACE_SECS", "0") };
    let purge: Value = client(&h)
        .post(format!("{}/api/v1/workspaces/{ws_id}/purge", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    unsafe { std::env::remove_var("AXIOM_WORKSPACE_GRACE_SECS") };

    assert_eq!(purge["purged"], true);
    assert_eq!(purge["eligible"], true);

    let bin: Vec<Value> = client(&h)
        .get(format!("{}/api/v1/workspaces/deleted", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!bin.iter().any(|w| w["id"] == ws_id.as_str()));

    // Invariant: purging workspace metadata must never delete object chunks.
    let downloaded = client(&h)
        .get(format!("{}/api/v1/version/main/file/keep.bin", h.base))
        .bearer_auth(&h.token)
        .send()
        .await
        .unwrap();
    assert_eq!(downloaded.status(), StatusCode::OK);
    assert_eq!(
        downloaded.bytes().await.unwrap().len(),
        payload.len(),
        "content must remain readable after the workspace is purged"
    );
}
