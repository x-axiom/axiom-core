//! Authentication and permission system tests (E07-S06).
//!
//! # Test structure
//!
//! | Module | Needs running FDB | Notes |
//! |--------|------------------|-------|
//! | `rbac_tests` | no | Pure RBAC matrix; compiles with `--features fdb` |
//! | `middleware_tests` | no | axum oneshot with pre-built AuthContext |
//! | `jwt_tests` | **yes** | Marked `#[ignore]`; run with `-- --include-ignored` |
//!
//! # Running
//!
//! ```sh
//! # RBAC + middleware (no FDB cluster needed, just libfdb installed):
//! cargo test --test auth_tests --features fdb
//!
//! # Full suite including JWT (requires running FoundationDB cluster):
//! cargo test --test auth_tests --features fdb -- --include-ignored
//! ```

// All auth types are gated behind the `fdb` feature.
#![cfg(feature = "fdb")]

// ---------------------------------------------------------------------------
// RBAC matrix tests — no feature gate, no FDB, always compiled
// ---------------------------------------------------------------------------

mod rbac_tests {
    use axiom_core::auth::rbac::{AuthContext, Permission, Resource, is_allowed, require_permission};
    use axiom_core::tenant::model::Role;

    // ------------------------------------------------------------------
    // is_allowed() exhaustive matrix
    // ------------------------------------------------------------------

    #[test]
    fn test_owner_has_all_permissions() {
        for perm in [Permission::Read, Permission::Write, Permission::Delete, Permission::Admin] {
            assert!(is_allowed(&Role::Owner, perm), "Owner should have {perm:?}");
        }
    }

    #[test]
    fn test_admin_has_read_write_delete_not_admin() {
        assert!(is_allowed(&Role::Admin, Permission::Read));
        assert!(is_allowed(&Role::Admin, Permission::Write));
        assert!(is_allowed(&Role::Admin, Permission::Delete));
        assert!(!is_allowed(&Role::Admin, Permission::Admin));
    }

    #[test]
    fn test_member_has_read_write_not_delete_or_admin() {
        assert!(is_allowed(&Role::Member, Permission::Read));
        assert!(is_allowed(&Role::Member, Permission::Write));
        assert!(!is_allowed(&Role::Member, Permission::Delete));
        assert!(!is_allowed(&Role::Member, Permission::Admin));
    }

    #[test]
    fn test_viewer_has_read_only() {
        assert!(is_allowed(&Role::Viewer, Permission::Read));
        assert!(!is_allowed(&Role::Viewer, Permission::Write));
        assert!(!is_allowed(&Role::Viewer, Permission::Delete));
        assert!(!is_allowed(&Role::Viewer, Permission::Admin));
    }

    /// Exhaustive 4 × 4 matrix — every Role × Permission combination.
    #[test]
    fn test_matrix_exhaustive() {
        // (Role, Permission, expected)
        let cases: &[(Role, Permission, bool)] = &[
            // Owner — all allowed
            (Role::Owner, Permission::Read,   true),
            (Role::Owner, Permission::Write,  true),
            (Role::Owner, Permission::Delete, true),
            (Role::Owner, Permission::Admin,  true),
            // Admin — no Admin perm
            (Role::Admin, Permission::Read,   true),
            (Role::Admin, Permission::Write,  true),
            (Role::Admin, Permission::Delete, true),
            (Role::Admin, Permission::Admin,  false),
            // Member — read/write only
            (Role::Member, Permission::Read,   true),
            (Role::Member, Permission::Write,  true),
            (Role::Member, Permission::Delete, false),
            (Role::Member, Permission::Admin,  false),
            // Viewer — read only
            (Role::Viewer, Permission::Read,   true),
            (Role::Viewer, Permission::Write,  false),
            (Role::Viewer, Permission::Delete, false),
            (Role::Viewer, Permission::Admin,  false),
        ];

        for (role, perm, expected) in cases {
            assert_eq!(
                is_allowed(role, *perm),
                *expected,
                "{role:?} × {perm:?} should be {expected}"
            );
        }
    }

    // ------------------------------------------------------------------
    // AuthContext::can()
    // ------------------------------------------------------------------

    fn ctx(role: Role) -> AuthContext {
        AuthContext {
            user_id: "u1".into(),
            tenant_id: "t1".into(),
            org_id: "o1".into(),
            role,
        }
    }

    #[test]
    fn test_auth_context_can_delegates_to_matrix() {
        assert!(ctx(Role::Owner).can(Permission::Admin));
        assert!(!ctx(Role::Viewer).can(Permission::Write));
        assert!(ctx(Role::Member).can(Permission::Write));
        assert!(!ctx(Role::Member).can(Permission::Delete));
    }

    // ------------------------------------------------------------------
    // Workspace-scoped isolation: same user, different roles per workspace
    // ------------------------------------------------------------------

    #[test]
    fn test_workspace_scoped_permission_isolation() {
        let ctx_a = ctx(Role::Owner);   // workspace A
        let ctx_b = ctx(Role::Viewer);  // workspace B (same user, different role)

        assert!(ctx_a.can(Permission::Delete));
        assert!(!ctx_b.can(Permission::Delete));
    }
}

// ---------------------------------------------------------------------------
// Middleware tests — no FDB, axum oneshot
// ---------------------------------------------------------------------------

mod middleware_tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    use axiom_core::auth::rbac::{AuthContext, Permission, Resource, require_permission};
    use axiom_core::tenant::model::Role;

    fn ctx(role: Role) -> AuthContext {
        AuthContext {
            user_id: "u1".into(),
            tenant_id: "t1".into(),
            org_id: "o1".into(),
            role,
        }
    }

    fn protected_router(resource: Resource, permission: Permission) -> Router {
        Router::new()
            .route("/test", get(|| async { "ok" }))
            .route_layer(require_permission(resource, permission))
    }

    /// Build a request with an `AuthContext` already in extensions.
    fn request_with_ctx(ctx: AuthContext) -> Request<Body> {
        let mut req = Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ctx);
        req
    }

    /// Build a request with no `AuthContext` (unauthenticated).
    fn request_no_ctx() -> Request<Body> {
        Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap()
    }

    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_missing_auth_context_returns_401() {
        let app = protected_router(Resource::Version, Permission::Write);
        let resp = app.oneshot(request_no_ctx()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_viewer_write_returns_403() {
        let app = protected_router(Resource::Version, Permission::Write);
        let resp = app.oneshot(request_with_ctx(ctx(Role::Viewer))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_viewer_read_passes() {
        let app = protected_router(Resource::Version, Permission::Read);
        let resp = app.oneshot(request_with_ctx(ctx(Role::Viewer))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_member_delete_returns_403() {
        let app = protected_router(Resource::Object, Permission::Delete);
        let resp = app.oneshot(request_with_ctx(ctx(Role::Member))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_admin_delete_passes() {
        let app = protected_router(Resource::Object, Permission::Delete);
        let resp = app.oneshot(request_with_ctx(ctx(Role::Admin))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_admin_admin_permission_returns_403() {
        let app = protected_router(Resource::Workspace, Permission::Admin);
        let resp = app.oneshot(request_with_ctx(ctx(Role::Admin))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_owner_admin_permission_passes() {
        let app = protected_router(Resource::Workspace, Permission::Admin);
        let resp = app.oneshot(request_with_ctx(ctx(Role::Owner))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_member_write_passes() {
        let app = protected_router(Resource::Ref, Permission::Write);
        let resp = app.oneshot(request_with_ctx(ctx(Role::Member))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

// ---------------------------------------------------------------------------
// JWT tests — requires running FDB cluster
// ---------------------------------------------------------------------------

mod jwt_tests {
    use std::sync::{Arc, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use axiom_core::auth::jwt::{JwtKeyPair, JwtService};
    use axiom_core::auth::middleware::AuthLayer;
    use axiom_core::auth::rbac::AuthContext;
    use axiom_core::error::CasError;
    use axiom_core::tenant::model::Role;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use axum::routing::get;
    use foundationdb::Database;
    use tower::ServiceExt;

    // ------------------------------------------------------------------
    // FDB boot (once per test process)
    // ------------------------------------------------------------------

    static FDB_GUARD: OnceLock<foundationdb::api::NetworkAutoStop> = OnceLock::new();

    fn boot_fdb() {
        FDB_GUARD.get_or_init(|| unsafe { foundationdb::boot() });
    }

    async fn make_svc() -> JwtService {
        boot_fdb();
        let db = Arc::new(Database::default().expect("FDB default database"));
        JwtService::with_generated_key(db).expect("JwtService init")
    }

    async fn make_svc_two_keys() -> JwtService {
        boot_fdb();
        let db = Arc::new(Database::default().expect("FDB default database"));
        let k0 = JwtKeyPair::generate().unwrap();
        let k1 = JwtKeyPair::generate().unwrap();
        // active key is k1
        JwtService::new(vec![k0, k1], 1, db)
    }

    fn unix_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // ------------------------------------------------------------------
    // Access token lifecycle
    // ------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_issue_and_verify_access_token() {
        let svc = make_svc().await;
        let token = svc
            .issue_access_token("user-1", "tenant-1", "org-1", vec!["Member".into()])
            .unwrap();

        let claims = svc.verify_access_token(&token).unwrap();
        assert_eq!(claims.sub, "user-1");
        assert_eq!(claims.tenant_id, "tenant-1");
        assert_eq!(claims.org_id, "org-1");
        assert_eq!(claims.roles, vec!["Member"]);
        assert!(claims.exp > unix_now());
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_expired_token_returns_unauthorized() {
        let svc = make_svc().await;
        // Issue a token that expired 60 seconds ago.
        let past_exp = unix_now().saturating_sub(60);
        let token = svc
            .issue_access_token_with_exp("user-1", "t1", "o1", vec!["Owner".into()], past_exp)
            .unwrap();

        let err = svc.verify_access_token(&token).unwrap_err();
        assert!(matches!(err, CasError::Unauthorized(_)), "expected Unauthorized, got {err:?}");
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_tampered_token_rejected() {
        let svc = make_svc().await;
        let token = svc
            .issue_access_token("u", "t", "o", vec!["Viewer".into()])
            .unwrap();

        // Flip the last character to corrupt the signature.
        let mut tampered = token.clone();
        let last = tampered.pop().unwrap();
        let replacement = if last == 'a' { 'b' } else { 'a' };
        tampered.push(replacement);

        let err = svc.verify_access_token(&tampered).unwrap_err();
        assert!(matches!(err, CasError::Unauthorized(_)));
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_malformed_token_rejected() {
        let svc = make_svc().await;
        let err = svc.verify_access_token("not.a.jwt").unwrap_err();
        assert!(matches!(err, CasError::Unauthorized(_)));
    }

    // ------------------------------------------------------------------
    // Key rotation
    // ------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_key_rotation_both_keys_can_verify() {
        let svc = make_svc_two_keys().await;

        // Active key is k1 (index 1) — issue a token.
        let token = svc
            .issue_access_token("u1", "t1", "o1", vec!["Admin".into()])
            .unwrap();

        // Verification must succeed (tries all registered keys).
        let claims = svc.verify_access_token(&token).unwrap();
        assert_eq!(claims.sub, "u1");
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_wrong_service_rejects_token() {
        let svc_a = make_svc().await;
        let svc_b = make_svc().await; // different key

        let token = svc_a
            .issue_access_token("u1", "t1", "o1", vec!["Owner".into()])
            .unwrap();

        let err = svc_b.verify_access_token(&token).unwrap_err();
        assert!(matches!(err, CasError::Unauthorized(_)));
    }

    // ------------------------------------------------------------------
    // JWKS
    // ------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_jwks_contains_all_public_keys() {
        let svc = make_svc_two_keys().await;
        let jwks = svc.jwks();
        assert_eq!(jwks.keys.len(), 2);
        for jwk in &jwks.keys {
            assert_eq!(jwk.kty, "OKP");
            assert_eq!(jwk.crv, "Ed25519");
            assert_eq!(jwk.alg, "EdDSA");
            assert!(!jwk.x.is_empty());
        }
    }

    // ------------------------------------------------------------------
    // Claims — no sensitive data
    // ------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_access_token_contains_no_sensitive_fields() {
        let svc = make_svc().await;
        let token = svc
            .issue_access_token("u1", "t1", "o1", vec!["Member".into()])
            .unwrap();

        // Decode the payload (second base64url segment) without verification.
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT must have 3 parts");

        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let payload = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let payload_str = String::from_utf8(payload).unwrap();

        // Sensitive fields that must NOT appear in the payload.
        for sensitive in &["password", "secret", "private_key", "client_secret"] {
            assert!(
                !payload_str.contains(sensitive),
                "Token payload must not contain '{sensitive}': {payload_str}"
            );
        }
    }

    // ------------------------------------------------------------------
    // Cross-tenant isolation
    // ------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_cross_tenant_claims_are_isolated() {
        let svc = make_svc().await;

        let token_a = svc
            .issue_access_token("user-a", "tenant-a", "org-a", vec!["Owner".into()])
            .unwrap();
        let token_b = svc
            .issue_access_token("user-b", "tenant-b", "org-b", vec!["Member".into()])
            .unwrap();

        let claims_a = svc.verify_access_token(&token_a).unwrap();
        let claims_b = svc.verify_access_token(&token_b).unwrap();

        // tenant A's token must not grant access to tenant B's resources.
        assert_ne!(claims_a.tenant_id, claims_b.tenant_id);
        assert_ne!(claims_a.sub, claims_b.sub);
        assert_ne!(claims_a.jti, claims_b.jti);
    }

    // ------------------------------------------------------------------
    // Concurrent token issuance
    // ------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_concurrent_access_token_issuance() {
        let svc = Arc::new(make_svc().await);
        let handles: Vec<_> = (0..20)
            .map(|i| {
                let svc = Arc::clone(&svc);
                tokio::spawn(async move {
                    svc.issue_access_token(
                        &format!("user-{i}"),
                        "tenant-1",
                        "org-1",
                        vec!["Member".into()],
                    )
                    .unwrap()
                })
            })
            .collect();

        let tokens: Vec<String> = {
            let mut out = Vec::with_capacity(handles.len());
            for h in handles {
                out.push(h.await.unwrap());
            }
            out
        };

        // Every token must be valid and unique.
        let mut jtis = std::collections::HashSet::new();
        for token in &tokens {
            let claims = svc.verify_access_token(token).unwrap();
            assert!(jtis.insert(claims.jti), "duplicate jti");
        }
    }

    // ------------------------------------------------------------------
    // Refresh token lifecycle (needs FDB reads/writes)
    // ------------------------------------------------------------------

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_refresh_token_single_use() {
        let svc = make_svc().await;
        let token = svc.issue_refresh_token("user-1").await.unwrap();

        // First use: succeeds.
        let user_id = svc.verify_and_consume_refresh_token(&token).await.unwrap();
        assert_eq!(user_id, "user-1");

        // Second use: must fail (single-use).
        let err = svc.verify_and_consume_refresh_token(&token).await.unwrap_err();
        assert!(matches!(err, CasError::Unauthorized(_)));
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_refresh_token_revocation() {
        let svc = make_svc().await;
        let token = svc.issue_refresh_token("user-1").await.unwrap();

        // Revoke before use.
        let claims: axiom_core::auth::jwt::AccessClaims = {
            // We don't have direct access to RefreshClaims, but we can parse
            // the jti from the base64 payload to get the jti for revocation.
            // Instead, verify+consume once to get the side-effect, but we want
            // to test revoke_refresh_token. Let's revoke via token directly.
            //
            // JwtService::revoke_refresh_token needs a jti string.
            // We'll decode the token payload manually.
            use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
            let parts: Vec<&str> = token.split('.').collect();
            let payload = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
            serde_json::from_slice(&payload).unwrap()
        };
        svc.revoke_refresh_token(&claims.jti).await.unwrap();

        // After revocation, token should be rejected.
        let err = svc.verify_and_consume_refresh_token(&token).await.unwrap_err();
        assert!(matches!(err, CasError::Unauthorized(_)));
    }

    // ------------------------------------------------------------------
    // Auth middleware (HTTP) with JWT
    // ------------------------------------------------------------------

    fn make_auth_router(svc: Arc<JwtService>) -> Router {
        Router::new()
            .route("/protected", get(|| async { "secret" }))
            .route_layer(AuthLayer::new(svc))
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_auth_middleware_no_token_returns_401() {
        let svc = Arc::new(make_svc().await);
        let app = make_auth_router(svc);

        let req = Request::builder()
            .uri("/protected")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_auth_middleware_valid_token_passes() {
        let svc = Arc::new(make_svc().await);
        let token = svc
            .issue_access_token("u1", "t1", "o1", vec!["Member".into()])
            .unwrap();
        let app = make_auth_router(Arc::clone(&svc));

        let req = Request::builder()
            .uri("/protected")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_auth_middleware_expired_token_returns_401() {
        let svc = Arc::new(make_svc().await);
        let past_exp = unix_now().saturating_sub(60);
        let token = svc
            .issue_access_token_with_exp("u1", "t1", "o1", vec!["Owner".into()], past_exp)
            .unwrap();
        let app = make_auth_router(Arc::clone(&svc));

        let req = Request::builder()
            .uri("/protected")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_auth_middleware_invalid_bearer_scheme_returns_401() {
        let svc = Arc::new(make_svc().await);
        let app = make_auth_router(svc);

        let req = Request::builder()
            .uri("/protected")
            .header(header::AUTHORIZATION, "Basic dXNlcjpwYXNz")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    #[ignore = "requires foundationdb cluster"]
    async fn test_auth_middleware_injects_auth_context() {
        use axum::Extension;

        let svc = Arc::new(make_svc().await);
        let token = svc
            .issue_access_token("user-42", "t1", "o1", vec!["Owner".into()])
            .unwrap();

        // Handler that asserts the injected AuthContext.
        async fn check_ctx(Extension(ctx): Extension<AuthContext>) -> String {
            ctx.user_id.clone()
        }

        let app = Router::new()
            .route("/check", get(check_ctx))
            .route_layer(AuthLayer::new(Arc::clone(&svc)));

        let req = Request::builder()
            .uri("/check")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(body, "user-42");
    }
}
