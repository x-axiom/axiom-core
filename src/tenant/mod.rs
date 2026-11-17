pub mod memory;
pub mod model;
#[cfg(feature = "fdb")]
pub mod repo;

use crate::error::{CasError, CasResult};
use self::model::{
    Membership, OrgId, Organization, Plan, Role, TenantId, User, UserId, Workspace, WorkspaceId,
};

/// Resolved identity scope for a signed-in user.
///
/// Produced by [`TenantDirectory::ensure_account`] during login and consumed by
/// the auth service when minting access tokens.
#[derive(Clone, Debug)]
pub struct AccountContext {
    pub user: User,
    pub tenant_id: TenantId,
    pub org_id: OrgId,
    pub role: Role,
    /// Default workspace for the account (created on first login).
    pub workspace: Workspace,
    /// Billing plan of the tenant (drives quota).
    pub plan: Plan,
}

pub trait TenantDirectory: Send + Sync {
	fn get_workspace(&self, id: &WorkspaceId) -> CasResult<Option<Workspace>>;
	fn list_workspaces_by_org(&self, org_id: &OrgId) -> CasResult<Vec<Workspace>>;
	fn create_workspace(&self, org_id: &OrgId, name: &str, storage_quota: u64)
		-> CasResult<Workspace>;
	fn delete_workspace(&self, id: &WorkspaceId, org_id: &OrgId) -> CasResult<()>;

	fn get_user(&self, id: &UserId) -> CasResult<Option<User>>;
	fn get_user_by_email(&self, email: &str) -> CasResult<Option<User>>;
	fn create_user(&self, email: &str, display_name: &str) -> CasResult<User>;

	fn get_membership(&self, user_id: &UserId, org_id: &OrgId) -> CasResult<Option<Membership>>;
	fn list_members_of_org(&self, org_id: &OrgId) -> CasResult<Vec<Membership>>;
	fn put_membership(&self, user_id: &UserId, org_id: &OrgId, role: Role)
		-> CasResult<Membership>;
	fn delete_membership(&self, user_id: &UserId, org_id: &OrgId) -> CasResult<()>;

	// -----------------------------------------------------------------------
	// Account bootstrap + org resolution (needed by the auth loop)
	// -----------------------------------------------------------------------

	/// Find-or-create the user, and make sure they belong to an organization
	/// with at least one workspace.
	///
	/// First login creates tenant → org → default workspace → owner
	/// membership in one step. Subsequent logins return the existing context.
	fn ensure_account(&self, _email: &str, _display_name: &str) -> CasResult<AccountContext> {
		Err(CasError::Store(
			"tenant directory does not support account bootstrap".into(),
		))
	}

	/// Organizations the user is a member of, most recently created last.
	fn list_orgs_of_user(&self, _user_id: &UserId) -> CasResult<Vec<OrgId>> {
		Ok(Vec::new())
	}

	/// Fetch an organization by id.
	fn get_org(&self, _id: &OrgId) -> CasResult<Option<Organization>> {
		Ok(None)
	}
}
