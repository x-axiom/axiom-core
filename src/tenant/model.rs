//! Tenant, organization, workspace, user, and membership data models.
//!
//! These types represent the multi-tenant hierarchy:
//!
//! ```text
//! Tenant ──┐
//!          └── Organization ──┐
//!                             ├── Workspace (storage namespaces)
//!                             └── Membership (User ↔ Role)
//! ```

use serde::{Deserialize, Serialize};

use crate::model::hash::current_timestamp;

// ---------------------------------------------------------------------------
// ID newtype macro
// ---------------------------------------------------------------------------

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

id_type!(TenantId);
id_type!(OrgId);
id_type!(WorkspaceId);
id_type!(UserId);

// ---------------------------------------------------------------------------
// ID generation (requires `fdb` feature for uuid)
// ---------------------------------------------------------------------------

/// Generate a new random UUID v4 string for entity IDs.
#[cfg(feature = "fdb")]
pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// Plan
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Plan {
    #[default]
    Free,
    Pro,
    Enterprise,
}

// ---------------------------------------------------------------------------
// Role
// ---------------------------------------------------------------------------

/// Role of a user within an organization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// Full control, including billing and role management.
    Owner,
    /// Read/write/delete access, no billing or role changes.
    Admin,
    /// Read/write access.
    Member,
    /// Read-only access.
    Viewer,
}

// ---------------------------------------------------------------------------
// Entities
// ---------------------------------------------------------------------------

/// Top-level billing and isolation unit.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tenant {
    pub id: TenantId,
    pub name: String,
    pub plan: Plan,
    /// Unix timestamp (seconds) when the tenant was created.
    pub created_at: u64,
}

#[cfg(feature = "fdb")]
impl Tenant {
    pub fn new(name: impl Into<String>, plan: Plan) -> Self {
        Self {
            id: TenantId(new_id()),
            name: name.into(),
            plan,
            created_at: current_timestamp(),
        }
    }
}

/// A named group within a tenant that owns workspaces and memberships.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Organization {
    pub id: OrgId,
    pub tenant_id: TenantId,
    pub name: String,
    /// Unix timestamp (seconds) when the org was created.
    pub created_at: u64,
}

#[cfg(feature = "fdb")]
impl Organization {
    pub fn new(tenant_id: TenantId, name: impl Into<String>) -> Self {
        Self {
            id: OrgId(new_id()),
            tenant_id,
            name: name.into(),
            created_at: current_timestamp(),
        }
    }
}

/// A storage namespace owned by an organization.
///
/// Maps 1-to-1 with a (`tenant`, `workspace`) pair in [`FdbMetadataStore`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workspace {
    pub id: WorkspaceId,
    pub org_id: OrgId,
    pub name: String,
    /// Maximum storage in bytes (0 = unlimited).
    pub storage_quota: u64,
    /// Unix timestamp (seconds) when the workspace was created.
    pub created_at: u64,
}

#[cfg(feature = "fdb")]
impl Workspace {
    pub fn new(org_id: OrgId, name: impl Into<String>, storage_quota: u64) -> Self {
        Self {
            id: WorkspaceId(new_id()),
            org_id,
            name: name.into(),
            storage_quota,
            created_at: current_timestamp(),
        }
    }
}

/// An authenticated principal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User {
    pub id: UserId,
    pub email: String,
    pub display_name: String,
    /// Unix timestamp (seconds) when the user was created.
    pub created_at: u64,
}

#[cfg(feature = "fdb")]
impl User {
    pub fn new(email: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            id: UserId(new_id()),
            email: email.into(),
            display_name: display_name.into(),
            created_at: current_timestamp(),
        }
    }
}

/// A user's role within a specific organization.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Membership {
    pub user_id: UserId,
    pub org_id: OrgId,
    pub role: Role,
}
