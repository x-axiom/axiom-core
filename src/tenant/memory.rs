//! In-memory [`TenantDirectory`] used for local single-user mode and tests.
//!
//! Everything is process-local and lost on restart. Local mode is explicitly a
//! single-user developer experience; production deployments must use the
//! FoundationDB-backed directory (`TenantRepo`).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::model::{
    Membership, OrgId, Organization, Plan, Role, Tenant, User, UserId, Workspace, WorkspaceId,
};
use super::{AccountContext, TenantDirectory};
use crate::error::{CasError, CasResult};

#[derive(Default)]
struct Inner {
    users: HashMap<String, User>,
    email_index: HashMap<String, UserId>,
    orgs: HashMap<String, Organization>,
    workspaces: HashMap<String, Workspace>,
    /// (user_id, org_id) → role
    memberships: HashMap<(String, String), Role>,
    /// (org_id, workspace_id) insertion order
    org_workspaces: HashMap<String, Vec<String>>,
    /// user_id → org ids
    user_orgs: HashMap<String, Vec<String>>,
}

/// Thread-safe, process-local tenant directory.
#[derive(Clone, Default)]
pub struct MemoryTenantDirectory {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryTenantDirectory {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TenantDirectory for MemoryTenantDirectory {
    fn get_workspace(&self, id: &WorkspaceId) -> CasResult<Option<Workspace>> {
        Ok(self.inner.read().workspaces.get(id.as_str()).cloned())
    }

    fn list_workspaces_by_org(&self, org_id: &OrgId) -> CasResult<Vec<Workspace>> {
        let inner = self.inner.read();
        let ids = inner
            .org_workspaces
            .get(org_id.as_str())
            .cloned()
            .unwrap_or_default();
        Ok(ids
            .iter()
            .filter_map(|id| inner.workspaces.get(id).cloned())
            .collect())
    }

    fn create_workspace(
        &self,
        org_id: &OrgId,
        name: &str,
        storage_quota: u64,
    ) -> CasResult<Workspace> {
        let mut inner = self.inner.write();
        if !inner.orgs.contains_key(org_id.as_str()) {
            return Err(CasError::TenantNotFound(org_id.to_string()));
        }
        let ws = Workspace::new(org_id.clone(), name, storage_quota);
        inner.workspaces.insert(ws.id.0.clone(), ws.clone());
        inner
            .org_workspaces
            .entry(org_id.0.clone())
            .or_default()
            .push(ws.id.0.clone());
        Ok(ws)
    }

    fn delete_workspace(&self, id: &WorkspaceId, org_id: &OrgId) -> CasResult<()> {
        let mut inner = self.inner.write();
        let belongs = inner
            .org_workspaces
            .get(org_id.as_str())
            .map(|ids| ids.iter().any(|w| w == id.as_str()))
            .unwrap_or(false);
        if !belongs {
            return Err(CasError::WorkspaceNotFound(id.to_string()));
        }
        inner.workspaces.remove(id.as_str());
        if let Some(ids) = inner.org_workspaces.get_mut(org_id.as_str()) {
            ids.retain(|w| w != id.as_str());
        }
        Ok(())
    }

    fn get_user(&self, id: &UserId) -> CasResult<Option<User>> {
        Ok(self.inner.read().users.get(id.as_str()).cloned())
    }

    fn get_user_by_email(&self, email: &str) -> CasResult<Option<User>> {
        let inner = self.inner.read();
        Ok(inner
            .email_index
            .get(&email.to_ascii_lowercase())
            .and_then(|id| inner.users.get(id.as_str()))
            .cloned())
    }

    fn create_user(&self, email: &str, display_name: &str) -> CasResult<User> {
        let mut inner = self.inner.write();
        let key = email.to_ascii_lowercase();
        if inner.email_index.contains_key(&key) {
            return Err(CasError::AlreadyExists);
        }
        let user = User::new(email.to_owned(), display_name.to_owned());
        inner.email_index.insert(key, user.id.clone());
        inner.users.insert(user.id.0.clone(), user.clone());
        Ok(user)
    }

    fn get_membership(&self, user_id: &UserId, org_id: &OrgId) -> CasResult<Option<Membership>> {
        Ok(self
            .inner
            .read()
            .memberships
            .get(&(user_id.0.clone(), org_id.0.clone()))
            .map(|role| Membership {
                user_id: user_id.clone(),
                org_id: org_id.clone(),
                role: role.clone(),
            }))
    }

    fn list_members_of_org(&self, org_id: &OrgId) -> CasResult<Vec<Membership>> {
        Ok(self
            .inner
            .read()
            .memberships
            .iter()
            .filter(|((_, o), _)| o == org_id.as_str())
            .map(|((u, o), role)| Membership {
                user_id: UserId::from(u.as_str()),
                org_id: OrgId::from(o.as_str()),
                role: role.clone(),
            })
            .collect())
    }

    fn put_membership(
        &self,
        user_id: &UserId,
        org_id: &OrgId,
        role: Role,
    ) -> CasResult<Membership> {
        let mut inner = self.inner.write();
        if !inner.orgs.contains_key(org_id.as_str()) {
            return Err(CasError::TenantNotFound(org_id.to_string()));
        }
        inner
            .memberships
            .insert((user_id.0.clone(), org_id.0.clone()), role.clone());
        inner
            .user_orgs
            .entry(user_id.0.clone())
            .or_default()
            .push(org_id.0.clone());
        Ok(Membership {
            user_id: user_id.clone(),
            org_id: org_id.clone(),
            role,
        })
    }

    fn delete_membership(&self, user_id: &UserId, org_id: &OrgId) -> CasResult<()> {
        let mut inner = self.inner.write();
        inner
            .memberships
            .remove(&(user_id.0.clone(), org_id.0.clone()));
        if let Some(orgs) = inner.user_orgs.get_mut(user_id.as_str()) {
            orgs.retain(|o| o != org_id.as_str());
        }
        Ok(())
    }

    fn list_orgs_of_user(&self, user_id: &UserId) -> CasResult<Vec<OrgId>> {
        Ok(self
            .inner
            .read()
            .user_orgs
            .get(user_id.as_str())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(OrgId::from)
            .collect())
    }

    fn get_org(&self, id: &OrgId) -> CasResult<Option<Organization>> {
        Ok(self.inner.read().orgs.get(id.as_str()).cloned())
    }

    fn ensure_account(&self, email: &str, display_name: &str) -> CasResult<AccountContext> {
        let email = email.trim().to_ascii_lowercase();
        if email.is_empty() || !email.contains('@') {
            return Err(CasError::InvalidObject(
                "account email must be a valid address".into(),
            ));
        }

        let user = match self.get_user_by_email(&email)? {
            Some(user) => user,
            None => self.create_user(&email, display_name)?,
        };

        let org_ids = self.list_orgs_of_user(&user.id)?;
        if let Some(org_id) = org_ids.first() {
            let org = self
                .get_org(org_id)?
                .ok_or_else(|| CasError::Store("org missing for membership".into()))?;
            let role = self
                .get_membership(&user.id, org_id)?
                .map(|m| m.role)
                .unwrap_or(Role::Member);
            let workspace = match self.list_workspaces_by_org(org_id)?.into_iter().next() {
                Some(ws) => ws,
                None => self.create_workspace(org_id, "Default", 0)?,
            };
            return Ok(AccountContext {
                user,
                tenant_id: org.tenant_id,
                org_id: org_id.clone(),
                role,
                workspace,
            });
        }

        let mut inner = self.inner.write();
        let tenant = Tenant::new(format!("{email} tenant"), Plan::Free);
        let org = Organization::new(tenant.id.clone(), email.clone());
        inner.orgs.insert(org.id.0.clone(), org.clone());
        inner.user_orgs.insert(user.id.0.clone(), vec![org.id.0.clone()]);
        inner
            .memberships
            .insert((user.id.0.clone(), org.id.0.clone()), Role::Owner);
        let workspace = Workspace::new(org.id.clone(), "Default", 0);
        inner
            .workspaces
            .insert(workspace.id.0.clone(), workspace.clone());
        inner
            .org_workspaces
            .entry(org.id.0.clone())
            .or_default()
            .push(workspace.id.0.clone());
        drop(inner);

        Ok(AccountContext {
            user,
            tenant_id: tenant.id,
            org_id: org.id,
            role: Role::Owner,
            workspace,
        })
    }
}
