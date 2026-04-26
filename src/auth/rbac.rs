//! Role-Based Access Control (RBAC) for Axiom.
//!
//! Defines roles, permissions, and resources, implements the permission
//! matrix, and provides an axum middleware that enforces access checks.
//!
//! # Permission matrix
//!
//! | Role    | Read | Write | Delete | Admin |
//! |---------|------|-------|--------|-------|
//! | Owner   |  ✓   |  ✓    |  ✓     |  ✓    |
//! | Admin   |  ✓   |  ✓    |  ✓     |  —    |
//! | Member  |  ✓   |  ✓    |  —     |  —    |
//! | Viewer  |  ✓   |  —    |  —     |  —    |
//!
//! All checks are workspace-scoped: a user may be `Owner` in workspace A
//! and `Viewer` in workspace B, and each is evaluated independently.
//!
//! # Axum integration
//!
//! Routes that require a specific permission are protected with
//! [`require_permission`], which reads [`AuthContext`] from request
//! extensions (set upstream by the JWT middleware) and returns 403 if the
//! user's role does not grant the requested permission.
//!
//! ```ignore
//! // In router construction:
//! Router::new()
//!     .route("/workspaces/:id/versions", post(create_version))
//!     .route_layer(require_permission(Resource::Version, Permission::Write))
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tower::{Layer, Service};

use crate::tenant::model::Role;

// ---------------------------------------------------------------------------
// Permission + Resource
// ---------------------------------------------------------------------------

/// Actions that can be performed on a resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permission {
    /// Read / list operations.
    Read,
    /// Create / update operations.
    Write,
    /// Delete operations.
    Delete,
    /// Administrative operations (role management, workspace deletion).
    Admin,
}

/// Logical resource categories subject to access control.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resource {
    Workspace,
    Version,
    Ref,
    Object,
}

// ---------------------------------------------------------------------------
// Permission matrix
// ---------------------------------------------------------------------------

/// Return `true` if `role` grants `permission` on any `resource`.
///
/// The matrix is resource-agnostic at this level — all resources share the
/// same role→permission mapping.  Future evolution can add per-resource
/// overrides here.
pub fn is_allowed(role: &Role, permission: Permission) -> bool {
    match role {
        Role::Owner => true,
        Role::Admin => matches!(permission, Permission::Read | Permission::Write | Permission::Delete),
        Role::Member => matches!(permission, Permission::Read | Permission::Write),
        Role::Viewer => matches!(permission, Permission::Read),
    }
}

// ---------------------------------------------------------------------------
// AuthContext — injected by the JWT middleware (E07-S05)
// ---------------------------------------------------------------------------

/// Authentication context extracted from a verified JWT.
///
/// Injected into request extensions by the JWT middleware and consumed by
/// [`require_permission`] and individual route handlers.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub user_id: String,
    pub tenant_id: String,
    pub org_id: String,
    /// The user's effective role in the current workspace.
    ///
    /// Populated from the `roles` JWT claim.  The first role is used for
    /// permission checks; future work can support multi-role evaluation.
    pub role: Role,
}

impl AuthContext {
    /// Return `true` if this context grants `permission`.
    pub fn can(&self, permission: Permission) -> bool {
        is_allowed(&self.role, permission)
    }
}

// ---------------------------------------------------------------------------
// require_permission — axum route layer
// ---------------------------------------------------------------------------

/// Create a [`tower::Layer`] that enforces `permission` on the given
/// `resource`.
///
/// The layer reads [`AuthContext`] from request extensions.  If absent or
/// if the role does not grant `permission`, it returns a 403 JSON response.
///
/// # Example
///
/// ```ignore
/// router.route_layer(require_permission(Resource::Version, Permission::Write))
/// ```
pub fn require_permission(
    resource: Resource,
    permission: Permission,
) -> RbacLayer {
    RbacLayer { resource, permission }
}

// ---------------------------------------------------------------------------
// Tower layer + service
// ---------------------------------------------------------------------------

/// Layer produced by [`require_permission`].
#[derive(Clone)]
pub struct RbacLayer {
    resource: Resource,
    permission: Permission,
}

impl<S> Layer<S> for RbacLayer {
    type Service = RbacMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RbacMiddleware {
            inner,
            resource: self.resource,
            permission: self.permission,
        }
    }
}

/// Middleware service that enforces RBAC.
#[derive(Clone)]
pub struct RbacMiddleware<S> {
    inner: S,
    #[allow(dead_code)]
    resource: Resource,
    permission: Permission,
}

impl<S> Service<Request<Body>> for RbacMiddleware<S>
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

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let permission = self.permission;

        // Extract AuthContext from extensions.
        let ctx = req.extensions().get::<AuthContext>().cloned();

        match ctx {
            None => {
                // No AuthContext means the JWT middleware did not run or
                // rejected the token — return 401 (not 403: identity unknown).
                Box::pin(async move {
                    Ok(forbidden_response(StatusCode::UNAUTHORIZED, "no auth context"))
                })
            }
            Some(ctx) if !ctx.can(permission) => {
                Box::pin(async move {
                    Ok(forbidden_response(StatusCode::FORBIDDEN, "insufficient permission"))
                })
            }
            Some(_) => {
                let fut = self.inner.call(req);
                Box::pin(async move { fut.await })
            }
        }
    }
}

/// Build a plain-text error response with the given status and message.
fn forbidden_response(status: StatusCode, msg: &'static str) -> Response {
    (status, msg).into_response()
}
