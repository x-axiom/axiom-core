pub mod model;
#[cfg(feature = "fdb")]
pub mod repo;

use crate::error::CasResult;
use self::model::{Membership, OrgId, Role, User, UserId, Workspace, WorkspaceId};

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
}
