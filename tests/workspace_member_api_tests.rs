#![cfg(feature = "local")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axiom_core::api::{
    build_router,
    state::{AppState, HttpAuthMode},
};
use axiom_core::error::{CasError, CasResult};
use axiom_core::tenant::model::{Membership, OrgId, Role, User, UserId, Workspace, WorkspaceId};
use axiom_core::tenant::TenantDirectory;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::Value;
use tower::ServiceExt;

#[derive(Default)]
struct FakeTenantDirectory {
    inner: Mutex<FakeTenantState>,
}

#[derive(Default)]
struct FakeTenantState {
    workspaces: HashMap<String, Workspace>,
    users: HashMap<String, User>,
    email_index: HashMap<String, String>,
    memberships: HashMap<(String, String), Membership>,
    next_user_id: usize,
    next_workspace_id: usize,
}

impl FakeTenantDirectory {
    fn seeded() -> Self {
        let owner = User {
            id: UserId::from("user-owner"),
            email: "owner@example.com".to_owned(),
            display_name: "Owner One".to_owned(),
            created_at: 1,
        };
        let outsider = User {
            id: UserId::from("user-outsider"),
            email: "outsider@example.com".to_owned(),
            display_name: "Outsider Two".to_owned(),
            created_at: 2,
        };
        let workspace_a = Workspace {
            id: WorkspaceId::from("ws-alpha"),
            org_id: OrgId::from("org-1"),
            name: "alpha".to_owned(),
            storage_quota: 0,
            created_at: 10,
        };
        let workspace_b = Workspace {
            id: WorkspaceId::from("ws-beta"),
            org_id: OrgId::from("org-2"),
            name: "beta".to_owned(),
            storage_quota: 0,
            created_at: 20,
        };

        let mut state = FakeTenantState::default();
        state
            .users
            .insert(owner.id.0.clone(), owner.clone());
        state
            .users
            .insert(outsider.id.0.clone(), outsider.clone());
        state
            .email_index
            .insert(owner.email.clone(), owner.id.0.clone());
        state
            .email_index
            .insert(outsider.email.clone(), outsider.id.0.clone());
        state
            .workspaces
            .insert(workspace_a.id.0.clone(), workspace_a.clone());
        state
            .workspaces
            .insert(workspace_b.id.0.clone(), workspace_b.clone());
        state.memberships.insert(
            (workspace_a.org_id.0.clone(), owner.id.0.clone()),
            Membership {
                user_id: owner.id.clone(),
                org_id: workspace_a.org_id.clone(),
                role: Role::Owner,
            },
        );
        state.memberships.insert(
            (workspace_b.org_id.0.clone(), outsider.id.0.clone()),
            Membership {
                user_id: outsider.id.clone(),
                org_id: workspace_b.org_id.clone(),
                role: Role::Owner,
            },
        );
        state.next_user_id = 1;
        state.next_workspace_id = 1;

        Self {
            inner: Mutex::new(state),
        }
    }
}

impl TenantDirectory for FakeTenantDirectory {
    fn get_workspace(&self, id: &WorkspaceId) -> CasResult<Option<Workspace>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .workspaces
            .get(id.as_str())
            .cloned())
    }

    fn list_workspaces_by_org(&self, org_id: &OrgId) -> CasResult<Vec<Workspace>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .workspaces
            .values()
            .filter(|workspace| workspace.org_id == *org_id)
            .cloned()
            .collect())
    }

    fn create_workspace(
        &self,
        org_id: &OrgId,
        name: &str,
        storage_quota: u64,
    ) -> CasResult<Workspace> {
        let mut inner = self.inner.lock().unwrap();
        let workspace = Workspace {
            id: WorkspaceId::from(format!("ws-created-{}", inner.next_workspace_id)),
            org_id: org_id.clone(),
            name: name.to_owned(),
            storage_quota,
            created_at: 100 + inner.next_workspace_id as u64,
        };
        inner.next_workspace_id += 1;
        inner
            .workspaces
            .insert(workspace.id.0.clone(), workspace.clone());
        Ok(workspace)
    }

    fn delete_workspace(&self, id: &WorkspaceId, org_id: &OrgId) -> CasResult<()> {
        let mut inner = self.inner.lock().unwrap();
        match inner.workspaces.get(id.as_str()) {
            Some(workspace) if workspace.org_id == *org_id => {
                inner.workspaces.remove(id.as_str());
                Ok(())
            }
            _ => Err(CasError::WorkspaceNotFound(id.0.clone())),
        }
    }

    fn get_user(&self, id: &UserId) -> CasResult<Option<User>> {
        Ok(self.inner.lock().unwrap().users.get(id.as_str()).cloned())
    }

    fn get_user_by_email(&self, email: &str) -> CasResult<Option<User>> {
        let inner = self.inner.lock().unwrap();
        let Some(user_id) = inner.email_index.get(email) else {
            return Ok(None);
        };
        Ok(inner.users.get(user_id).cloned())
    }

    fn create_user(&self, email: &str, display_name: &str) -> CasResult<User> {
        let mut inner = self.inner.lock().unwrap();
        if inner.email_index.contains_key(email) {
            return Err(CasError::AlreadyExists);
        }

        let user = User {
            id: UserId::from(format!("user-created-{}", inner.next_user_id)),
            email: email.to_owned(),
            display_name: display_name.to_owned(),
            created_at: 200 + inner.next_user_id as u64,
        };
        inner.next_user_id += 1;
        inner
            .email_index
            .insert(user.email.clone(), user.id.0.clone());
        inner.users.insert(user.id.0.clone(), user.clone());
        Ok(user)
    }

    fn get_membership(&self, user_id: &UserId, org_id: &OrgId) -> CasResult<Option<Membership>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .memberships
            .get(&(org_id.0.clone(), user_id.0.clone()))
            .cloned())
    }

    fn list_members_of_org(&self, org_id: &OrgId) -> CasResult<Vec<Membership>> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .memberships
            .values()
            .filter(|membership| membership.org_id == *org_id)
            .cloned()
            .collect())
    }

    fn put_membership(&self, user_id: &UserId, org_id: &OrgId, role: Role) -> CasResult<Membership> {
        let mut inner = self.inner.lock().unwrap();
        let membership = Membership {
            user_id: user_id.clone(),
            org_id: org_id.clone(),
            role,
        };
        inner
            .memberships
            .insert((org_id.0.clone(), user_id.0.clone()), membership.clone());
        Ok(membership)
    }

    fn delete_membership(&self, user_id: &UserId, org_id: &OrgId) -> CasResult<()> {
        self.inner
            .lock()
            .unwrap()
            .memberships
            .remove(&(org_id.0.clone(), user_id.0.clone()));
        Ok(())
    }
}

fn test_app() -> axum::Router {
    build_router(
        AppState::memory()
            .with_http_auth(HttpAuthMode::TrustedGatewayHeaders)
            .with_tenant_directory(Arc::new(FakeTenantDirectory::seeded())),
    )
}

fn authed_request(method: &str, uri: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-user-id", "user-owner")
        .header("x-tenant-id", "tenant-1")
        .header("x-org-id", "org-1")
        .header("x-roles", "Owner");

    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }

    builder
        .body(match body {
            Some(json) => Body::from(json.to_string()),
            None => Body::empty(),
        })
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn workspace_member_routes_manage_members_and_scope_workspaces() {
    let app = test_app();

    let list_resp = app
        .clone()
        .oneshot(authed_request("GET", "/api/v1/workspaces", None))
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let workspaces = json_body(list_resp).await;
    assert_eq!(workspaces.as_array().unwrap().len(), 1);
    assert_eq!(workspaces[0]["id"], "ws-alpha");

    let create_resp = app
        .clone()
        .oneshot(authed_request(
            "POST",
            "/api/v1/workspaces",
            Some(serde_json::json!({ "name": "team-ops" })),
        ))
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::OK);
    let created_workspace = json_body(create_resp).await;
    let created_workspace_id = created_workspace["id"].as_str().unwrap().to_owned();

    let invite_resp = app
        .clone()
        .oneshot(authed_request(
            "POST",
            "/api/v1/workspaces/ws-alpha/members/invite",
            Some(serde_json::json!({
                "email": "new.user@example.com",
                "role": "admin"
            })),
        ))
        .await
        .unwrap();
    assert_eq!(invite_resp.status(), StatusCode::OK);
    let invited_member = json_body(invite_resp).await;
    let invited_member_id = invited_member["id"].as_str().unwrap().to_owned();
    assert_eq!(invited_member["role"], "admin");

    let members_resp = app
        .clone()
        .oneshot(authed_request(
            "GET",
            "/api/v1/workspaces/ws-alpha/members",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(members_resp.status(), StatusCode::OK);
    let members = json_body(members_resp).await;
    assert_eq!(members.as_array().unwrap().len(), 2);

    let patch_resp = app
        .clone()
        .oneshot(authed_request(
            "PATCH",
            &format!("/api/v1/workspaces/ws-alpha/members/{invited_member_id}"),
            Some(serde_json::json!({ "role": "member" })),
        ))
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);
    let updated_member = json_body(patch_resp).await;
    assert_eq!(updated_member["role"], "member");

    let remove_resp = app
        .clone()
        .oneshot(authed_request(
            "DELETE",
            &format!("/api/v1/workspaces/ws-alpha/members/{invited_member_id}"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(remove_resp.status(), StatusCode::OK);

    let delete_resp = app
        .oneshot(authed_request(
            "DELETE",
            &format!("/api/v1/workspaces/{created_workspace_id}"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn workspace_member_routes_reject_last_owner_changes() {
    let app = test_app();

    let delete_resp = app
        .clone()
        .oneshot(authed_request(
            "DELETE",
            "/api/v1/workspaces/ws-alpha/members/user-owner",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(delete_resp.status(), StatusCode::FORBIDDEN);

    let demote_resp = app
        .oneshot(authed_request(
            "PATCH",
            "/api/v1/workspaces/ws-alpha/members/user-owner",
            Some(serde_json::json!({ "role": "member" })),
        ))
        .await
        .unwrap();
    assert_eq!(demote_resp.status(), StatusCode::FORBIDDEN);
}