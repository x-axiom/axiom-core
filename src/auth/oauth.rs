//! GitHub OAuth helpers (authorization-code flow + token → identity lookup).
//!
//! Two entry points are supported:
//!
//! 1. **Server-side OAuth** — `GET /auth/oauth/github` redirects to GitHub, and
//!    the callback exchanges `code` for an access token here.
//! 2. **Token bridge** — the web console already runs NextAuth and holds a
//!    GitHub access token. `POST /auth/login` with `provider = "github"` and
//!    that token verifies it against the GitHub API and exchanges it for Axiom
//!    tokens, so the browser never sends a GitHub token to `/api/v1/*`.

use std::time::Duration;

use serde::Deserialize;

use crate::error::{CasError, CasResult};

/// Configuration for the GitHub OAuth application.
#[derive(Clone, Debug)]
pub struct GitHubOAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
    /// GitHub API base URL. Overridable for tests / GH Enterprise.
    pub api_base: String,
    /// OAuth web base URL (`https://github.com`).
    pub web_base: String,
}

impl GitHubOAuthConfig {
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        redirect_uri: impl Into<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            redirect_uri: redirect_uri.into(),
            api_base: "https://api.github.com".into(),
            web_base: "https://github.com".into(),
        }
    }

    /// Build from environment variables. Returns `None` when the app is not
    /// configured (auth endpoints then report `oauth_not_configured`).
    pub fn from_env() -> Option<Self> {
        let client_id = std::env::var("AXIOM_GITHUB_CLIENT_ID").ok()?;
        let client_secret = std::env::var("AXIOM_GITHUB_CLIENT_SECRET").ok()?;
        let redirect_uri = std::env::var("AXIOM_GITHUB_REDIRECT_URI").ok()?;
        Some(Self::new(client_id, client_secret, redirect_uri))
    }
}

/// Normalized identity returned by GitHub.
#[derive(Clone, Debug)]
pub struct GitHubIdentity {
    pub id: u64,
    pub email: String,
    pub name: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Deserialize)]
struct UserResponse {
    id: u64,
    login: String,
    name: Option<String>,
    email: Option<String>,
}

#[derive(Deserialize)]
struct EmailResponse {
    email: String,
    primary: bool,
    verified: bool,
}

/// GitHub OAuth client.
#[derive(Clone)]
pub struct GitHubOAuth {
    config: GitHubOAuthConfig,
    http: reqwest::Client,
}

impl GitHubOAuth {
    pub fn new(config: GitHubOAuthConfig) -> CasResult<Self> {
        let http = reqwest::Client::builder()
            .user_agent("axiom-core/0.5 (+https://axiom.dev)")
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| CasError::Store(format!("github oauth client: {e}")))?;
        Ok(Self { config, http })
    }

    pub fn config(&self) -> &GitHubOAuthConfig {
        &self.config
    }

    /// Build the authorize URL the browser should be redirected to.
    pub fn authorize_url(&self, state: &str) -> String {
        format!(
            "{}/login/oauth/authorize?client_id={}&redirect_uri={}&scope=read:user%20user:email&state={}",
            self.config.web_base,
            urlencode(&self.config.client_id),
            urlencode(&self.config.redirect_uri),
            urlencode(state),
        )
    }

    /// Exchange an authorization `code` for a GitHub access token.
    pub async fn exchange_code(&self, code: &str) -> CasResult<String> {
        let resp = self
            .http
            .post(format!("{}/login/oauth/access_token", self.config.web_base))
            .header("accept", "application/json")
            .form(&[
                ("client_id", self.config.client_id.as_str()),
                ("client_secret", self.config.client_secret.as_str()),
                ("code", code),
                ("redirect_uri", self.config.redirect_uri.as_str()),
            ])
            .send()
            .await
            .map_err(|e| CasError::Store(format!("github token exchange: {e}")))?;

        let status = resp.status();
        let body: TokenResponse = resp
            .json()
            .await
            .map_err(|e| CasError::Store(format!("github token response: {e}")))?;

        if let Some(token) = body.access_token {
            return Ok(token);
        }

        Err(CasError::Unauthorized(format!(
            "github token exchange failed ({status}): {}",
            body.error_description.or(body.error).unwrap_or_else(|| "unknown error".into())
        )))
    }

    /// Resolve a GitHub access token to a normalized identity.
    pub async fn fetch_identity(&self, access_token: &str) -> CasResult<GitHubIdentity> {
        let user: UserResponse = self
            .http
            .get(format!("{}/user", self.config.api_base))
            .bearer_auth(access_token)
            .header("accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| CasError::Store(format!("github user lookup: {e}")))?
            .error_for_status()
            .map_err(|_| CasError::Unauthorized("github token rejected".into()))?
            .json()
            .await
            .map_err(|e| CasError::Store(format!("github user payload: {e}")))?;

        let email = match user.email.filter(|e| !e.is_empty()) {
            Some(email) => email,
            None => self.primary_email(access_token).await?,
        };

        Ok(GitHubIdentity {
            id: user.id,
            email,
            name: user.name.unwrap_or(user.login),
        })
    }

    async fn primary_email(&self, access_token: &str) -> CasResult<String> {
        let emails: Vec<EmailResponse> = self
            .http
            .get(format!("{}/user/emails", self.config.api_base))
            .bearer_auth(access_token)
            .header("accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| CasError::Store(format!("github email lookup: {e}")))?
            .error_for_status()
            .map_err(|_| CasError::Unauthorized("github email scope missing".into()))?
            .json()
            .await
            .map_err(|e| CasError::Store(format!("github email payload: {e}")))?;

        emails
            .into_iter()
            .find(|e| e.primary && e.verified)
            .map(|e| e.email)
            .ok_or_else(|| {
                CasError::Unauthorized(
                    "github account has no verified primary email; grant user:email scope".into(),
                )
            })
    }
}

/// Minimal percent-encoding for query-string values.
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorize_url_encodes_redirect_uri() {
        let cfg = GitHubOAuthConfig::new("cid", "secret", "https://api.axiom.dev/auth/oauth/github/callback");
        let client = GitHubOAuth::new(cfg).unwrap();
        let url = client.authorize_url("state-123");
        assert!(url.starts_with("https://github.com/login/oauth/authorize?"));
        assert!(url.contains("client_id=cid"));
        assert!(url.contains("redirect_uri=https%3A%2F%2Fapi.axiom.dev"));
        assert!(url.contains("state=state-123"));
    }

    #[test]
    fn urlencode_keeps_unreserved_bytes() {
        assert_eq!(urlencode("a-b_c.d~e"), "a-b_c.d~e");
        assert_eq!(urlencode("a b"), "a%20b");
    }
}
