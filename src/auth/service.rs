//! Auth service: the single place that mints, refreshes, and revokes Axiom
//! tokens.
//!
//! The service deliberately owns no HTTP concerns — routes in
//! [`crate::auth::routes`] translate requests into calls here, and tests can
//! drive the same methods directly.

use std::sync::Arc;

use serde::Serialize;

use crate::auth::jwt::{AccessClaims, JwkSet, JwtService, REFRESH_TTL_SECS};
use crate::auth::oauth::{GitHubIdentity, GitHubOAuth};
use crate::error::{CasError, CasResult};
use crate::tenant::model::Role;
use crate::tenant::TenantDirectory;

/// Token bundle returned by login / refresh.
#[derive(Clone, Debug, Serialize)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    /// Access-token lifetime in seconds.
    pub expires_in: u64,
    pub token_type: String,
    pub user_id: String,
    pub email: String,
    pub display_name: String,
    pub tenant_id: String,
    pub org_id: String,
    pub workspace_id: String,
    /// PascalCase role names, matching the JWT `roles` claim.
    pub roles: Vec<String>,
}

/// Authentication entry points used by the HTTP layer.
pub struct AuthService {
    jwt: Arc<JwtService>,
    directory: Arc<dyn TenantDirectory>,
    github: Option<GitHubOAuth>,
    /// Enables `POST /auth/login {provider:"dev"}` for local development.
    /// Only honoured when the request also carries the matching secret header
    /// and the process is not running with `AXIOM_ENV=production`.
    dev_secret: Option<String>,
}

impl AuthService {
    pub fn new(
        jwt: Arc<JwtService>,
        directory: Arc<dyn TenantDirectory>,
        github: Option<GitHubOAuth>,
        dev_secret: Option<String>,
    ) -> Self {
        Self { jwt, directory, github, dev_secret }
    }

    /// The directory used for account lookups. Exposed so `AppState` can share
    /// exactly the same directory between `/auth/*` and the workspace APIs.
    pub fn directory(&self) -> Arc<dyn TenantDirectory> {
        Arc::clone(&self.directory)
    }

    pub fn jwks(&self) -> JwkSet {
        self.jwt.jwks()
    }

    pub fn jwt(&self) -> &Arc<JwtService> {
        &self.jwt
    }

    pub fn verify_access(&self, token: &str) -> CasResult<AccessClaims> {
        self.jwt.verify_access_token(token)
    }

    /// Dev-only login path. Requires an explicitly configured secret, a
    /// matching `x-axiom-dev-secret` header, and a non-production environment.
    pub fn dev_login(&self, email: &str, name: &str, presented_secret: Option<&str>) -> CasResult<TokenPair> {
        let configured = self.dev_secret.as_deref().ok_or_else(|| {
            CasError::Forbidden("dev login is not enabled on this deployment".into())
        })?;
        if std::env::var("AXIOM_ENV").as_deref() == Ok("production") {
            return Err(CasError::Forbidden(
                "dev login is disabled in production".into(),
            ));
        }
        let presented = presented_secret.ok_or_else(|| {
            CasError::Unauthorized("missing x-axiom-dev-secret header".into())
        })?;
        if !constant_time_eq(configured.as_bytes(), presented.as_bytes()) {
            return Err(CasError::Unauthorized("invalid dev login secret".into()));
        }
        self.issue_for_identity(email, name)
    }

    /// Exchange a verified GitHub identity for Axiom tokens.
    pub fn login_github_identity(&self, identity: &GitHubIdentity) -> CasResult<TokenPair> {
        self.issue_for_identity(&identity.email, &identity.name)
    }

    /// Verify a GitHub access token, then exchange it for Axiom tokens.
    pub async fn login_github_token(&self, access_token: &str) -> CasResult<TokenPair> {
        let github = self.github.as_ref().ok_or_else(|| {
            CasError::Store("github oauth is not configured (AXIOM_GITHUB_*)".into())
        })?;
        let identity = github.fetch_identity(access_token).await?;
        self.login_github_identity(&identity)
    }

    /// Exchange a GitHub OAuth `code` for Axiom tokens.
    pub async fn login_github_code(&self, code: &str) -> CasResult<TokenPair> {
        let github = self.github.as_ref().ok_or_else(|| {
            CasError::Store("github oauth is not configured (AXIOM_GITHUB_*)".into())
        })?;
        let token = github.exchange_code(code).await?;
        let identity = github.fetch_identity(&token).await?;
        self.login_github_identity(&identity)
    }

    pub fn authorize_url(&self, state: &str) -> CasResult<String> {
        let github = self.github.as_ref().ok_or_else(|| {
            CasError::Store("github oauth is not configured (AXIOM_GITHUB_*)".into())
        })?;
        Ok(github.authorize_url(state))
    }

    pub fn issue_state(&self) -> CasResult<String> {
        self.jwt.issue_state_token(&uuid::Uuid::new_v4().to_string())
    }

    pub fn verify_state(&self, state: &str) -> CasResult<()> {
        self.jwt.verify_state_token(state)
    }

    /// Rotate a refresh token: consume the presented one, mint a new pair.
    pub fn refresh(&self, refresh_token: &str) -> CasResult<TokenPair> {
        let user_id = self.jwt.verify_and_consume_refresh_token(refresh_token)?;
        let user = self
            .directory
            .get_user(&crate::tenant::model::UserId::from(user_id.as_str()))?
            .ok_or_else(|| CasError::Unauthorized("refresh subject no longer exists".into()))?;
        self.issue_for_existing_user(&user)
    }

    /// Revoke a refresh token. Revocation is idempotent.
    pub fn logout(&self, refresh_token: &str) -> CasResult<()> {
        let jti = self.jwt.refresh_token_jti(refresh_token)?;
        self.jwt.revoke_refresh_token(&jti)
    }

    fn issue_for_identity(&self, email: &str, name: &str) -> CasResult<TokenPair> {
        let account = self.directory.ensure_account(email, name)?;
        self.mint(
            &account.user.id.0,
            &account.user.email,
            &account.user.display_name,
            &account.tenant_id.0,
            &account.org_id.0,
            &account.workspace.id.0,
            account.role,
        )
    }

    fn issue_for_existing_user(
        &self,
        user: &crate::tenant::model::User,
    ) -> CasResult<TokenPair> {
        let account = self.directory.ensure_account(&user.email, &user.display_name)?;
        self.mint(
            &account.user.id.0,
            &account.user.email,
            &account.user.display_name,
            &account.tenant_id.0,
            &account.org_id.0,
            &account.workspace.id.0,
            account.role,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn mint(
        &self,
        user_id: &str,
        email: &str,
        display_name: &str,
        tenant_id: &str,
        org_id: &str,
        workspace_id: &str,
        role: Role,
    ) -> CasResult<TokenPair> {
        let roles = vec![role_name(&role).to_owned()];
        let access_token = self
            .jwt
            .issue_access_token(user_id, tenant_id, org_id, roles.clone())?;
        let refresh_token = self.jwt.issue_refresh_token(user_id)?;
        Ok(TokenPair {
            access_token,
            refresh_token,
            expires_in: crate::auth::jwt::ACCESS_TTL_SECS,
            token_type: "Bearer".into(),
            user_id: user_id.to_owned(),
            email: email.to_owned(),
            display_name: display_name.to_owned(),
            tenant_id: tenant_id.to_owned(),
            org_id: org_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            roles,
        })
    }

    /// Refresh-token TTL, exposed for docs/tests.
    pub fn refresh_ttl_secs(&self) -> u64 {
        REFRESH_TTL_SECS
    }
}

/// PascalCase role name as used in JWT claims.
pub fn role_name(role: &Role) -> &'static str {
    match role {
        Role::Owner => "Owner",
        Role::Admin => "Admin",
        Role::Member => "Member",
        Role::Viewer => "Viewer",
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::refresh::MemoryRefreshTokenStore;
    use crate::tenant::memory::MemoryTenantDirectory;

    fn service(dev_secret: Option<&str>) -> AuthService {
        let jwt = Arc::new(
            JwtService::new(
                vec![crate::auth::jwt::JwtKeyPair::generate().unwrap()],
                0,
                Arc::new(MemoryRefreshTokenStore::new()),
            ),
        );
        AuthService::new(
            jwt,
            Arc::new(MemoryTenantDirectory::new()),
            None,
            dev_secret.map(str::to_owned),
        )
    }

    #[test]
    fn first_login_provisions_owner_account() {
        let svc = service(None);
        let pair = svc.dev_login_for_test("owner@example.com", "Owner");
        assert_eq!(pair.roles, vec!["Owner".to_string()]);
        assert!(!pair.access_token.is_empty());
        assert_eq!(pair.expires_in, crate::auth::jwt::ACCESS_TTL_SECS);

        let claims = svc.verify_access(&pair.access_token).unwrap();
        assert_eq!(claims.org_id, pair.org_id);
        assert_eq!(claims.tenant_id, pair.tenant_id);
    }

    #[test]
    fn refresh_rotates_and_revokes_previous_token() {
        let svc = service(None);
        let first = svc.dev_login_for_test("owner@example.com", "Owner");
        let second = svc.refresh(&first.refresh_token).unwrap();
        assert_ne!(second.refresh_token, first.refresh_token);

        // The first refresh token was consumed and must not work again.
        assert!(svc.refresh(&first.refresh_token).is_err());
    }

    #[test]
    fn logout_revokes_refresh_token() {
        let svc = service(None);
        let pair = svc.dev_login_for_test("owner@example.com", "Owner");
        svc.logout(&pair.refresh_token).unwrap();
        assert!(svc.refresh(&pair.refresh_token).is_err());
    }

    #[test]
    fn dev_login_requires_secret_when_configured() {
        let svc = service(Some("s3cret"));
        assert!(svc.dev_login("a@b.c", "A", None).is_err());
        assert!(svc.dev_login("a@b.c", "A", Some("wrong")).is_err());
        assert!(svc.dev_login("a@b.c", "A", Some("s3cret")).is_ok());
    }

    #[test]
    fn state_tokens_round_trip() {
        let svc = service(None);
        let state = svc.issue_state().unwrap();
        assert!(svc.verify_state(&state).is_ok());
        assert!(svc.verify_state("not-a-token").is_err());
    }

    impl AuthService {
        /// Test helper: bypass the dev-secret gate.
        fn dev_login_for_test(&self, email: &str, name: &str) -> TokenPair {
            self.issue_for_identity(email, name).unwrap()
        }
    }
}
