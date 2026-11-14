//! HTTP-level tests for the SaaS auth loop (P0-1).
//!
//! Covers the acceptance criteria that can be verified without a real GitHub
//! app: login → protected API access → refresh → logout → JWKS.

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

const DEV_SECRET: &str = "test-dev-secret";

struct Harness {
    base: String,
    _tmp: tempfile::TempDir,
}

async fn start() -> Harness {
    let tmp = tempfile::tempdir().unwrap();
    let cas = RocksDbCasStore::open(tmp.path().join("cas")).unwrap();
    let meta = SqliteMetadataStore::open(tmp.path().join("meta.db")).unwrap();

    let jwt = Arc::new(
        JwtService::new(
            vec![axiom_core::auth::jwt::JwtKeyPair::generate().unwrap()],
            0,
            Arc::new(MemoryRefreshTokenStore::new()),
        ),
    );
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

    Harness { base: format!("http://{addr}"), _tmp: tmp }
}

async fn dev_login(h: &Harness) -> Value {
    let resp = reqwest::Client::new()
        .post(format!("{}/auth/login", h.base))
        .header("x-axiom-dev-secret", DEV_SECRET)
        .json(&serde_json::json!({
            "provider": "dev",
            "email": "alice@example.com",
            "name": "Alice"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json().await.unwrap()
}

#[tokio::test]
async fn login_unlocks_protected_api_and_jwks_is_public() {
    let h = start().await;
    let tokens = dev_login(&h).await;

    assert_eq!(tokens["token_type"], "Bearer");
    assert!(tokens["access_token"].as_str().unwrap().len() > 20);
    assert_eq!(tokens["roles"][0], "Owner");
    assert!(!tokens["org_id"].as_str().unwrap().is_empty());

    // Protected route accepts the Axiom access token.
    let ok = reqwest::Client::new()
        .get(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(tokens["access_token"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);

    // JWKS is public and Ed25519-shaped.
    let jwks: Value = reqwest::get(format!("{}/.well-known/jwks.json", h.base))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(jwks["keys"][0]["kty"], "OKP");
    assert_eq!(jwks["keys"][0]["crv"], "Ed25519");
    assert_eq!(jwks["keys"][0]["alg"], "EdDSA");
}

#[tokio::test]
async fn protected_route_without_token_is_401() {
    let h = start().await;
    let resp = reqwest::get(format!("{}/api/v1/workspaces", h.base))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "unauthorized");
}

#[tokio::test]
async fn refresh_rotates_tokens_and_old_refresh_cannot_be_reused() {
    let h = start().await;
    let first = dev_login(&h).await;
    let old_refresh = first["refresh_token"].as_str().unwrap().to_owned();

    let refreshed: Value = reqwest::Client::new()
        .post(format!("{}/auth/refresh", h.base))
        .json(&serde_json::json!({ "refresh_token": old_refresh }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_ne!(
        refreshed["refresh_token"].as_str().unwrap(),
        old_refresh,
        "refresh tokens must rotate"
    );

    // Replaying the consumed refresh token must fail.
    let reuse = reqwest::Client::new()
        .post(format!("{}/auth/refresh", h.base))
        .json(&serde_json::json!({ "refresh_token": first["refresh_token"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(reuse.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn logout_revokes_refresh_token() {
    let h = start().await;
    let tokens = dev_login(&h).await;
    let refresh = tokens["refresh_token"].as_str().unwrap().to_owned();

    let logout = reqwest::Client::new()
        .post(format!("{}/auth/logout", h.base))
        .json(&serde_json::json!({ "refresh_token": refresh }))
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::OK);

    let after = reqwest::Client::new()
        .post(format!("{}/auth/refresh", h.base))
        .json(&serde_json::json!({ "refresh_token": tokens["refresh_token"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(after.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn dev_login_requires_the_configured_secret() {
    let h = start().await;

    let missing = reqwest::Client::new()
        .post(format!("{}/auth/login", h.base))
        .json(&serde_json::json!({ "provider": "dev", "email": "bob@example.com" }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let wrong = reqwest::Client::new()
        .post(format!("{}/auth/login", h.base))
        .header("x-axiom-dev-secret", "wrong")
        .json(&serde_json::json!({ "provider": "dev", "email": "bob@example.com" }))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn session_endpoint_echoes_identity() {
    let h = start().await;
    let tokens = dev_login(&h).await;

    let session: Value = reqwest::Client::new()
        .get(format!("{}/auth/session", h.base))
        .bearer_auth(tokens["access_token"].as_str().unwrap())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(session["org_id"], tokens["org_id"]);
    assert_eq!(session["roles"][0], "Owner");
}

// ---------------------------------------------------------------------------
// P0-7: cross-tenant isolation
// ---------------------------------------------------------------------------

async fn dev_login_as(h: &Harness, email: &str, name: &str) -> Value {
    let resp = reqwest::Client::new()
        .post(format!("{}/auth/login", h.base))
        .header("x-axiom-dev-secret", DEV_SECRET)
        .json(&serde_json::json!({ "provider": "dev", "email": email, "name": name }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "login failed for {email}");
    resp.json().await.unwrap()
}

#[tokio::test]
async fn cross_org_workspace_access_is_forbidden() {
    let h = start().await;

    let alice = dev_login_as(&h, "alice-org-a@example.com", "Alice").await;
    let bob = dev_login_as(&h, "bob-org-b@example.com", "Bob").await;
    assert_ne!(alice["org_id"], bob["org_id"], "each account gets its own org");

    let created: Value = reqwest::Client::new()
        .post(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(alice["access_token"].as_str().unwrap())
        .json(&serde_json::json!({ "name": "alice-private" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ws_id = created["id"].as_str().unwrap().to_owned();

    // Owner can read its own workspace.
    let own = reqwest::Client::new()
        .get(format!("{}/api/v1/workspaces/{ws_id}/tree", h.base))
        .bearer_auth(alice["access_token"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(own.status(), StatusCode::OK);

    // A different org must not be able to read tree / versions / diff.
    for path in ["tree", "versions", "diff?from=x&to=y"] {
        let resp = reqwest::Client::new()
            .get(format!("{}/api/v1/workspaces/{ws_id}/{path}", h.base))
            .bearer_auth(bob["access_token"].as_str().unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "cross-org read of '{path}' must be forbidden"
        );
    }
}

#[tokio::test]
async fn workspace_list_is_scoped_to_the_callers_org() {
    let h = start().await;
    let alice = dev_login_as(&h, "alice-list@example.com", "Alice").await;
    let bob = dev_login_as(&h, "bob-list@example.com", "Bob").await;

    reqwest::Client::new()
        .post(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(alice["access_token"].as_str().unwrap())
        .json(&serde_json::json!({ "name": "alice-only" }))
        .send()
        .await
        .unwrap();

    let bob_list: Vec<Value> = reqwest::Client::new()
        .get(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(bob["access_token"].as_str().unwrap())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(
        !bob_list.iter().any(|w| w["name"] == "alice-only"),
        "bob must not see alice's workspace, got {bob_list:?}"
    );
}


// ---------------------------------------------------------------------------
// P1-8: audit trail
// ---------------------------------------------------------------------------

#[tokio::test]
async fn audit_log_records_login_and_workspace_lifecycle() {
    let h = start().await;
    let tokens = dev_login_as(&h, "audit@example.com", "Audit").await;
    let token = tokens["access_token"].as_str().unwrap();

    let created: Value = reqwest::Client::new()
        .post(format!("{}/api/v1/workspaces", h.base))
        .bearer_auth(token)
        .json(&serde_json::json!({ "name": "audited" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ws_id = created["id"].as_str().unwrap().to_owned();

    reqwest::Client::new()
        .delete(format!("{}/api/v1/workspaces/{ws_id}", h.base))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();

    let events: Vec<Value> = reqwest::Client::new()
        .get(format!("{}/api/v1/audit?limit=50", h.base))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let actions: Vec<&str> = events.iter().filter_map(|e| e["action"].as_str()).collect();
    for expected in ["auth.login", "workspace.create", "workspace.delete"] {
        assert!(actions.contains(&expected), "missing audit action {expected}: {actions:?}");
    }

    let first = events.first().unwrap();
    assert!(!first["actor"].as_str().unwrap().is_empty());
    assert!(!first["tenant_id"].as_str().unwrap().is_empty());
    assert!(first["ts"].as_u64().unwrap() > 0);
}
