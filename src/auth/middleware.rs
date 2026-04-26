//! Authentication middleware for HTTP (axum) and gRPC (tonic).
//!
//! # HTTP (axum)
//!
//! Use [`AuthLayer`] as a route layer.  It reads the
//! `Authorization: Bearer <token>` header, verifies the JWT with
//! [`JwtService`], builds an [`AuthContext`], and inserts it into request
//! extensions.  Downstream handlers can extract it with
//! `axum::Extension<AuthContext>`.
//!
//! ```ignore
//! Router::new()
//!     .route("/api/v1/versions", get(list_versions))
//!     .route_layer(AuthLayer::new(jwt_service.clone()))
//! ```
//!
//! # gRPC (tonic) — `cloud` feature
//!
//! Use [`tonic_auth_interceptor`] when constructing the tonic server:
//!
//! ```ignore
//! Server::builder()
//!     .add_service(interceptor(svc, tonic_auth_interceptor(jwt_service)))
//!     .serve(addr)
//!     .await?;
//! ```
//!
//! # Error codes
//!
//! | Condition               | HTTP | gRPC                       |
//! |-------------------------|------|----------------------------|
//! | Missing `Authorization` | 401  | `Unauthenticated`          |
//! | Bearer token malformed  | 401  | `Unauthenticated`          |
//! | Token expired           | 401  | `Unauthenticated`          |
//! | Token invalid           | 401  | `Unauthenticated`          |

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tower::{Layer, Service};

use crate::auth::jwt::JwtService;
use crate::auth::rbac::AuthContext;
use crate::tenant::model::Role;

// ---------------------------------------------------------------------------
// Helper: parse a role string from a JWT claim
// ---------------------------------------------------------------------------

/// Convert the string representation of a role (as stored in JWT claims)
/// into a [`Role`] enum value.  Returns `None` for unrecognised strings.
fn parse_role(s: &str) -> Option<Role> {
    match s {
        "Owner" => Some(Role::Owner),
        "Admin" => Some(Role::Admin),
        "Member" => Some(Role::Member),
        "Viewer" => Some(Role::Viewer),
        _ => None,
    }
}

/// Pick the highest-privilege role from a list of role strings.
///
/// Precedence (descending): Owner → Admin → Member → Viewer.
/// Falls back to `Role::Viewer` if the list is empty or contains no
/// recognised strings.
fn effective_role(roles: &[String]) -> Role {
    let mut best: Option<Role> = None;

    for s in roles {
        if let Some(r) = parse_role(s) {
            best = Some(match best {
                None => r,
                Some(ref current) => {
                    if role_level(&r) > role_level(current) { r } else { best.take().unwrap() }
                }
            });
        }
    }

    best.unwrap_or(Role::Viewer)
}

/// Numeric privilege level for ordering roles.
fn role_level(r: &Role) -> u8 {
    match r {
        Role::Owner => 3,
        Role::Admin => 2,
        Role::Member => 1,
        Role::Viewer => 0,
    }
}

// ---------------------------------------------------------------------------
// AuthLayer
// ---------------------------------------------------------------------------

/// Tower [`Layer`] that enforces JWT authentication on every request.
///
/// Wrap individual routes or the entire router:
/// ```ignore
/// Router::new()
///     .route("/protected", get(handler))
///     .route_layer(AuthLayer::new(jwt))
/// ```
#[derive(Clone)]
pub struct AuthLayer {
    jwt: Arc<JwtService>,
}

impl AuthLayer {
    pub fn new(jwt: Arc<JwtService>) -> Self {
        Self { jwt }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthMiddleware {
            inner,
            jwt: Arc::clone(&self.jwt),
        }
    }
}

// ---------------------------------------------------------------------------
// AuthMiddleware service
// ---------------------------------------------------------------------------

/// Tower [`Service`] that verifies the JWT and injects [`AuthContext`].
#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    jwt: Arc<JwtService>,
}

impl<S> Service<Request<Body>> for AuthMiddleware<S>
where
    S: Service<Request<Body>, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<Body>) -> Self::Future {
        // Extract the Bearer token from the Authorization header.
        let token_result = extract_bearer(req.headers());

        match token_result {
            Err(resp) => Box::pin(async move { Ok(resp) }),
            Ok(token) => {
                // Verify the token.  Cloning the Arc is cheap.
                match self.jwt.verify_access_token(&token) {
                    Err(_) => {
                        let resp =
                            (StatusCode::UNAUTHORIZED, "invalid or expired token").into_response();
                        Box::pin(async move { Ok(resp) })
                    }
                    Ok(claims) => {
                        // Build AuthContext and inject into extensions.
                        let role = effective_role(&claims.roles);
                        let ctx = AuthContext {
                            user_id: claims.sub,
                            tenant_id: claims.tenant_id,
                            org_id: claims.org_id,
                            role,
                        };
                        req.extensions_mut().insert(ctx);

                        let fut = self.inner.call(req);
                        Box::pin(async move { fut.await })
                    }
                }
            }
        }
    }
}

/// Extract the raw token string from `Authorization: Bearer <token>`.
///
/// Returns `Err(Response)` with a 401 status on any parsing failure.
fn extract_bearer(headers: &axum::http::HeaderMap) -> Result<String, Response> {
    let header_value = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| {
            (StatusCode::UNAUTHORIZED, "missing Authorization header").into_response()
        })?;

    let value_str = header_value.to_str().map_err(|_| {
        (StatusCode::UNAUTHORIZED, "Authorization header is not valid UTF-8").into_response()
    })?;

    let token = value_str.strip_prefix("Bearer ").ok_or_else(|| {
        (StatusCode::UNAUTHORIZED, "Authorization header must use Bearer scheme").into_response()
    })?;

    if token.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "Bearer token is empty").into_response());
    }

    Ok(token.to_owned())
}

// ---------------------------------------------------------------------------
// gRPC (tonic) interceptor — cloud feature only
// ---------------------------------------------------------------------------

#[cfg(feature = "cloud")]
pub mod grpc {
    //! Tonic interceptor that mirrors the axum JWT middleware for gRPC calls.

    use std::sync::Arc;

    use tonic::metadata::MetadataValue;
    use tonic::{Request, Status};

    use crate::auth::jwt::JwtService;
    use crate::auth::rbac::AuthContext;

    use super::effective_role;

    /// Build a tonic interceptor closure that authenticates gRPC calls.
    ///
    /// The interceptor reads the `authorization` metadata key (lowercase),
    /// expecting the value `Bearer <token>`.  On success it inserts
    /// [`AuthContext`] into the request extensions; on failure it returns
    /// `Unauthenticated`.
    ///
    /// # Usage
    ///
    /// ```ignore
    /// use tonic::transport::Server;
    /// use tonic::service::interceptor;
    ///
    /// Server::builder()
    ///     .add_service(interceptor(my_svc, tonic_auth_interceptor(jwt)))
    ///     .serve(addr)
    ///     .await?;
    /// ```
    pub fn tonic_auth_interceptor(
        jwt: Arc<JwtService>,
    ) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
        move |mut req: Request<()>| {
            let token = extract_bearer_meta(req.metadata())?;

            let claims = jwt
                .verify_access_token(&token)
                .map_err(|_| Status::unauthenticated("invalid or expired token"))?;

            let role = effective_role(&claims.roles);
            let ctx = AuthContext {
                user_id: claims.sub,
                tenant_id: claims.tenant_id,
                org_id: claims.org_id,
                role,
            };
            req.extensions_mut().insert(ctx);

            Ok(req)
        }
    }

    fn extract_bearer_meta(
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<String, Status> {
        let value: &MetadataValue<tonic::metadata::Ascii> = metadata
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?;

        let s = value
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization metadata is not valid ASCII"))?;

        let token = s
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("authorization must use Bearer scheme"))?;

        if token.is_empty() {
            return Err(Status::unauthenticated("Bearer token is empty"));
        }

        Ok(token.to_owned())
    }
}
