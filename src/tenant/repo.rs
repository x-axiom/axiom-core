//! FDB-backed [`TenantRepo`]: CRUD for tenants, organizations, workspaces,
//! users, and memberships.
//!
//! # Key layout
//!
//! All keys are UTF-8 byte strings using `\x00` as a separator:
//!
//! ```text
//! tenant\x00{tenant_id}                        → Tenant JSON
//! org\x00{org_id}                              → Organization JSON
//! tenant_org\x00{tenant_id}\x00{org_id}        → b"" (list index)
//! ws\x00{workspace_id}                         → Workspace JSON
//! org_ws\x00{org_id}\x00{workspace_id}         → b"" (list index)
//! user\x00{user_id}                            → User JSON
//! email\x00{email}                             → user_id bytes (lookup)
//! member\x00{org_id}\x00{user_id}              → Membership JSON
//! user_member\x00{user_id}\x00{org_id}         → b"" (reverse index)
//! ```

use std::sync::Arc;

use foundationdb::{Database, FdbError, KeySelector, RangeOption};
use foundationdb::options::StreamingMode;

use crate::error::{CasError, CasResult};
use super::model::{
    Membership, OrgId, Organization, Plan, Role, Tenant, TenantId, User, UserId,
    Workspace, WorkspaceId,
};

/// Maximum entries returned per range scan.
const SCAN_LIMIT: usize = 10_000;

// ---------------------------------------------------------------------------
// TenantRepo
// ---------------------------------------------------------------------------

/// Global FDB-backed registry for multi-tenant metadata.
///
/// Provides CRUD for [`Tenant`], [`Organization`], [`Workspace`], [`User`],
/// and [`Membership`], with cascade deletes for org and tenant removal.
///
/// All operations are synchronous wrappers around async FDB I/O driven by
/// the FDB network thread.  Callers must be running inside a Tokio runtime.
///
/// The FDB network thread must be started **once** before constructing any
/// `TenantRepo`:
///
/// ```ignore
/// let _guard = unsafe { foundationdb::boot() };
/// ```
pub struct TenantRepo {
    db: Arc<Database>,
}

impl TenantRepo {
    /// Connect to the FDB cluster.
    ///
    /// `cluster_file`: path to the cluster file, or `None` for the system
    /// default (`/etc/foundationdb/fdb.cluster` or `FDB_CLUSTER_FILE`).
    pub fn new(cluster_file: Option<&str>) -> CasResult<Self> {
        let db = match cluster_file {
            Some(path) => Database::from_path(path)
                .map_err(|e| CasError::Store(format!("FDB open {path}: {e}")))?,
            None => Database::default()
                .map_err(|e| CasError::Store(format!("FDB open default cluster: {e}")))?,
        };
        Ok(Self { db: Arc::new(db) })
    }

    // -----------------------------------------------------------------------
    // Key builders
    // -----------------------------------------------------------------------

    fn tenant_key(id: &TenantId) -> Vec<u8> {
        format!("tenant\x00{}", id.as_str()).into_bytes()
    }

    fn org_key(id: &OrgId) -> Vec<u8> {
        format!("org\x00{}", id.as_str()).into_bytes()
    }

    fn tenant_org_key(tenant_id: &TenantId, org_id: &OrgId) -> Vec<u8> {
        format!("tenant_org\x00{}\x00{}", tenant_id.as_str(), org_id.as_str()).into_bytes()
    }

    fn tenant_org_prefix(tenant_id: &TenantId) -> Vec<u8> {
        format!("tenant_org\x00{}\x00", tenant_id.as_str()).into_bytes()
    }

    fn workspace_key(id: &WorkspaceId) -> Vec<u8> {
        format!("ws\x00{}", id.as_str()).into_bytes()
    }

    fn org_ws_key(org_id: &OrgId, ws_id: &WorkspaceId) -> Vec<u8> {
        format!("org_ws\x00{}\x00{}", org_id.as_str(), ws_id.as_str()).into_bytes()
    }

    fn org_ws_prefix(org_id: &OrgId) -> Vec<u8> {
        format!("org_ws\x00{}\x00", org_id.as_str()).into_bytes()
    }

    fn user_key(id: &UserId) -> Vec<u8> {
        format!("user\x00{}", id.as_str()).into_bytes()
    }

    fn email_key(email: &str) -> Vec<u8> {
        format!("email\x00{}", email).into_bytes()
    }

    fn member_key(org_id: &OrgId, user_id: &UserId) -> Vec<u8> {
        format!("member\x00{}\x00{}", org_id.as_str(), user_id.as_str()).into_bytes()
    }

    fn org_member_prefix(org_id: &OrgId) -> Vec<u8> {
        format!("member\x00{}\x00", org_id.as_str()).into_bytes()
    }

    fn user_member_key(user_id: &UserId, org_id: &OrgId) -> Vec<u8> {
        format!("user_member\x00{}\x00{}", user_id.as_str(), org_id.as_str()).into_bytes()
    }

    fn user_member_prefix(user_id: &UserId) -> Vec<u8> {
        format!("user_member\x00{}\x00", user_id.as_str()).into_bytes()
    }

    /// The smallest key strictly greater than all keys with the given prefix.
    fn prefix_end(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        end.push(0xff);
        end
    }

    // -----------------------------------------------------------------------
    // Tokio runtime helper
    // -----------------------------------------------------------------------

    fn rt() -> CasResult<tokio::runtime::Handle> {
        tokio::runtime::Handle::try_current()
            .map_err(|e| CasError::Store(format!("TenantRepo requires a Tokio runtime: {e}")))
    }

    // -----------------------------------------------------------------------
    // Low-level FDB helpers
    // -----------------------------------------------------------------------

    /// Read a single key, returning raw bytes or `None`.
    fn fdb_get_raw(&self, key: Vec<u8>) -> CasResult<Option<Vec<u8>>> {
        let rt = Self::rt()?;
        rt.block_on(async {
            let trx = self
                .db
                .create_trx()
                .map_err(|e| CasError::Store(format!("FDB create_trx: {e}")))?;
            let v = trx
                .get(&key, false)
                .await
                .map_err(|e| CasError::Store(format!("FDB get: {e}")))?;
            Ok(v.map(|b| b.to_vec()))
        })
    }

    /// Read a JSON value at `key`, returning `None` if absent.
    fn get_json<T: serde::de::DeserializeOwned>(&self, key: Vec<u8>) -> CasResult<Option<T>> {
        match self.fdb_get_raw(key)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        }
    }

    /// Write multiple key-value pairs atomically.
    fn put_multi(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> CasResult<()> {
        let rt = Self::rt()?;
        rt.block_on(self.db.run(|trx, _| {
            let pairs = pairs.clone();
            async move {
                for (k, v) in &pairs {
                    trx.set(k, v);
                }
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB put_multi: {e}")))?;
        Ok(())
    }

    /// Delete `keys` and clear `ranges` atomically.
    fn delete_multi(
        &self,
        keys: Vec<Vec<u8>>,
        ranges: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> CasResult<()> {
        let rt = Self::rt()?;
        rt.block_on(self.db.run(|trx, _| {
            let keys = keys.clone();
            let ranges = ranges.clone();
            async move {
                for k in &keys {
                    trx.clear(k);
                }
                for (start, end) in &ranges {
                    trx.clear_range(start, end);
                }
                Ok(())
            }
        }))
        .map_err(|e: FdbError| CasError::Store(format!("FDB delete_multi: {e}")))?;
        Ok(())
    }

    /// Range scan over `[start, end)`.
    fn range_scan(&self, start: Vec<u8>, end: Vec<u8>) -> CasResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let rt = Self::rt()?;
        rt.block_on(async {
            let trx = self
                .db
                .create_trx()
                .map_err(|e| CasError::Store(format!("FDB create_trx: {e}")))?;
            let begin = KeySelector::first_greater_or_equal(start.as_slice());
            let end_sel = KeySelector::first_greater_or_equal(end.as_slice());
            let opt = RangeOption {
                mode: StreamingMode::WantAll,
                limit: Some(SCAN_LIMIT),
                ..RangeOption::from((begin, end_sel))
            };
            let kvs = trx
                .get_range(&opt, 1, false)
                .await
                .map_err(|e| CasError::Store(format!("FDB range_scan: {e}")))?;
            Ok::<Vec<(Vec<u8>, Vec<u8>)>, CasError>(
                kvs.iter()
                    .map(|kv| (kv.key().to_vec(), kv.value().to_vec()))
                    .collect(),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Tenant CRUD
    // -----------------------------------------------------------------------

    /// Create and persist a new tenant.
    pub fn create_tenant(&self, name: &str, plan: Plan) -> CasResult<Tenant> {
        let t = Tenant::new(name, plan);
        let bytes = serde_json::to_vec(&t).expect("serialize");
        self.put_multi(vec![(Self::tenant_key(&t.id), bytes)])?;
        Ok(t)
    }

    /// Get a tenant by ID.
    pub fn get_tenant(&self, id: &TenantId) -> CasResult<Option<Tenant>> {
        self.get_json(Self::tenant_key(id))
    }

    /// Delete a tenant and cascade-delete all its organizations.
    pub fn delete_tenant(&self, id: &TenantId) -> CasResult<()> {
        for org in self.list_orgs_by_tenant(id)? {
            self.delete_org(&org.id)?;
        }
        // Remove the tenant record and any stale tenant_org index entries.
        let prefix = Self::tenant_org_prefix(id);
        let end = Self::prefix_end(&prefix);
        self.delete_multi(vec![Self::tenant_key(id)], vec![(prefix, end)])
    }

    // -----------------------------------------------------------------------
    // Organization CRUD
    // -----------------------------------------------------------------------

    /// Create and persist a new organization under a tenant.
    pub fn create_org(&self, tenant_id: &TenantId, name: &str) -> CasResult<Organization> {
        let org = Organization::new(tenant_id.clone(), name);
        let org_bytes = serde_json::to_vec(&org).expect("serialize");
        self.put_multi(vec![
            (Self::org_key(&org.id), org_bytes),
            (Self::tenant_org_key(tenant_id, &org.id), b"".to_vec()),
        ])?;
        Ok(org)
    }

    /// Get an organization by ID.
    pub fn get_org(&self, id: &OrgId) -> CasResult<Option<Organization>> {
        self.get_json(Self::org_key(id))
    }

    /// List all organizations belonging to a tenant.
    pub fn list_orgs_by_tenant(&self, tenant_id: &TenantId) -> CasResult<Vec<Organization>> {
        let prefix = Self::tenant_org_prefix(tenant_id);
        let end = Self::prefix_end(&prefix);
        let kvs = self.range_scan(prefix.clone(), end)?;
        let prefix_len = prefix.len();
        let mut orgs = Vec::with_capacity(kvs.len());
        for (k, _) in kvs {
            let org_id = OrgId(String::from_utf8_lossy(&k[prefix_len..]).into_owned());
            if let Some(org) = self.get_org(&org_id)? {
                orgs.push(org);
            }
        }
        Ok(orgs)
    }

    /// Delete an organization and cascade-delete its workspaces and memberships.
    ///
    /// Cascade sequence (two-phase: reads then single atomic write):
    /// 1. Snapshot-read workspace IDs and member user IDs from their index ranges.
    /// 2. Single FDB transaction: clear workspace records, org_ws index,
    ///    membership records, org_member index, reverse user_member entries,
    ///    tenant_org index entry, and the org record itself.
    pub fn delete_org(&self, id: &OrgId) -> CasResult<()> {
        // Phase 1: collect dependent IDs.
        let ws_prefix = Self::org_ws_prefix(id);
        let ws_end = Self::prefix_end(&ws_prefix);
        let ws_kvs = self.range_scan(ws_prefix.clone(), ws_end.clone())?;
        let ws_prefix_len = ws_prefix.len();
        let ws_ids: Vec<WorkspaceId> = ws_kvs
            .iter()
            .map(|(k, _)| WorkspaceId(String::from_utf8_lossy(&k[ws_prefix_len..]).into_owned()))
            .collect();

        let member_prefix = Self::org_member_prefix(id);
        let member_end = Self::prefix_end(&member_prefix);
        let member_kvs = self.range_scan(member_prefix.clone(), member_end.clone())?;
        let member_prefix_len = member_prefix.len();
        let user_ids: Vec<UserId> = member_kvs
            .iter()
            .map(|(k, _)| UserId(String::from_utf8_lossy(&k[member_prefix_len..]).into_owned()))
            .collect();

        let tenant_id = self.get_org(id)?.map(|o| o.tenant_id);

        // Phase 2: atomic delete.
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        for ws_id in &ws_ids {
            keys.push(Self::workspace_key(ws_id));
        }
        ranges.push((ws_prefix, ws_end));

        ranges.push((member_prefix, member_end));
        for user_id in &user_ids {
            keys.push(Self::user_member_key(user_id, id));
        }

        if let Some(ref tid) = tenant_id {
            keys.push(Self::tenant_org_key(tid, id));
        }
        keys.push(Self::org_key(id));

        self.delete_multi(keys, ranges)
    }

    // -----------------------------------------------------------------------
    // Workspace CRUD
    // -----------------------------------------------------------------------

    /// Create and persist a new workspace under an org.
    pub fn create_workspace(
        &self,
        org_id: &OrgId,
        name: &str,
        storage_quota: u64,
    ) -> CasResult<Workspace> {
        let ws = Workspace::new(org_id.clone(), name, storage_quota);
        let ws_bytes = serde_json::to_vec(&ws).expect("serialize");
        self.put_multi(vec![
            (Self::workspace_key(&ws.id), ws_bytes),
            (Self::org_ws_key(org_id, &ws.id), b"".to_vec()),
        ])?;
        Ok(ws)
    }

    /// Get a workspace by ID.
    pub fn get_workspace(&self, id: &WorkspaceId) -> CasResult<Option<Workspace>> {
        self.get_json(Self::workspace_key(id))
    }

    /// List all workspaces belonging to an org.
    pub fn list_workspaces_by_org(&self, org_id: &OrgId) -> CasResult<Vec<Workspace>> {
        let prefix = Self::org_ws_prefix(org_id);
        let end = Self::prefix_end(&prefix);
        let kvs = self.range_scan(prefix.clone(), end)?;
        let prefix_len = prefix.len();
        let mut workspaces = Vec::with_capacity(kvs.len());
        for (k, _) in kvs {
            let ws_id = WorkspaceId(String::from_utf8_lossy(&k[prefix_len..]).into_owned());
            if let Some(ws) = self.get_workspace(&ws_id)? {
                workspaces.push(ws);
            }
        }
        Ok(workspaces)
    }

    /// Delete a workspace and its `org_ws` index entry.
    pub fn delete_workspace(&self, id: &WorkspaceId, org_id: &OrgId) -> CasResult<()> {
        self.delete_multi(
            vec![Self::workspace_key(id), Self::org_ws_key(org_id, id)],
            vec![],
        )
    }

    // -----------------------------------------------------------------------
    // User CRUD
    // -----------------------------------------------------------------------

    /// Create and persist a new user.
    ///
    /// Returns [`CasError::AlreadyExists`] if a user with the same email
    /// already exists.
    pub fn create_user(&self, email: &str, display_name: &str) -> CasResult<User> {
        if self.fdb_get_raw(Self::email_key(email))?.is_some() {
            return Err(CasError::AlreadyExists);
        }
        let user = User::new(email, display_name);
        let user_bytes = serde_json::to_vec(&user).expect("serialize");
        self.put_multi(vec![
            (Self::user_key(&user.id), user_bytes),
            (Self::email_key(email), user.id.0.as_bytes().to_vec()),
        ])?;
        Ok(user)
    }

    /// Get a user by ID.
    pub fn get_user(&self, id: &UserId) -> CasResult<Option<User>> {
        self.get_json(Self::user_key(id))
    }

    /// Get a user by email address.
    pub fn get_user_by_email(&self, email: &str) -> CasResult<Option<User>> {
        match self.fdb_get_raw(Self::email_key(email))? {
            None => Ok(None),
            Some(id_bytes) => {
                let id = UserId(String::from_utf8_lossy(&id_bytes).into_owned());
                self.get_user(&id)
            }
        }
    }

    /// Delete a user, their email index, and all membership records they own.
    pub fn delete_user(&self, id: &UserId) -> CasResult<()> {
        let user = match self.get_user(id)? {
            None => return Ok(()),
            Some(u) => u,
        };
        let org_ids = self.list_orgs_of_user(id)?;

        let mut keys = vec![Self::user_key(id), Self::email_key(&user.email)];
        for org_id in &org_ids {
            // Remove the forward membership record.
            keys.push(Self::member_key(org_id, id));
        }
        // Clear the reverse index range in one shot.
        let um_prefix = Self::user_member_prefix(id);
        let um_end = Self::prefix_end(&um_prefix);
        self.delete_multi(keys, vec![(um_prefix, um_end)])
    }

    // -----------------------------------------------------------------------
    // Membership
    // -----------------------------------------------------------------------

    /// Set (or update) a user's role in an org.
    pub fn put_membership(
        &self,
        user_id: &UserId,
        org_id: &OrgId,
        role: Role,
    ) -> CasResult<Membership> {
        let m = Membership {
            user_id: user_id.clone(),
            org_id: org_id.clone(),
            role,
        };
        let bytes = serde_json::to_vec(&m).expect("serialize");
        self.put_multi(vec![
            (Self::member_key(org_id, user_id), bytes),
            (Self::user_member_key(user_id, org_id), b"".to_vec()),
        ])?;
        Ok(m)
    }

    /// Get a user's membership in a specific org.
    pub fn get_membership(
        &self,
        user_id: &UserId,
        org_id: &OrgId,
    ) -> CasResult<Option<Membership>> {
        self.get_json(Self::member_key(org_id, user_id))
    }

    /// List all memberships in an org.
    pub fn list_members_of_org(&self, org_id: &OrgId) -> CasResult<Vec<Membership>> {
        let prefix = Self::org_member_prefix(org_id);
        let end = Self::prefix_end(&prefix);
        let kvs = self.range_scan(prefix, end)?;
        kvs.iter()
            .map(|(_, v)| serde_json::from_slice(v).map_err(CasError::from))
            .collect()
    }

    /// List all org IDs that a user belongs to.
    pub fn list_orgs_of_user(&self, user_id: &UserId) -> CasResult<Vec<OrgId>> {
        let prefix = Self::user_member_prefix(user_id);
        let end = Self::prefix_end(&prefix);
        let kvs = self.range_scan(prefix.clone(), end)?;
        let prefix_len = prefix.len();
        Ok(kvs
            .iter()
            .map(|(k, _)| OrgId(String::from_utf8_lossy(&k[prefix_len..]).into_owned()))
            .collect())
    }

    /// Remove a user's membership from an org.
    pub fn delete_membership(&self, user_id: &UserId, org_id: &OrgId) -> CasResult<()> {
        self.delete_multi(
            vec![
                Self::member_key(org_id, user_id),
                Self::user_member_key(user_id, org_id),
            ],
            vec![],
        )
    }
}
