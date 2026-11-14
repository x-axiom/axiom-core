//! Shared application state injected into all handlers.

use std::sync::Arc;

use crate::auth::service::AuthService;
use crate::audit::AuditRepo;
use crate::quota::{QuotaPolicy, UsageRepo};
use crate::store::traits::{
    ChunkStore, NodeStore, PathIndexRepo, RefRepo, TreeStore, VersionRepo,
    WorkspaceRepo,
};
use crate::tenant::TenantDirectory;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpAuthMode {
    /// No authentication (local single-user / desktop embedding).
    Disabled,
    /// Trust identity headers injected by axiom-gateway after JWT verification.
    TrustedGatewayHeaders,
    /// Verify `Authorization: Bearer <access token>` directly with the auth
    /// service. Used when core is reachable without a gateway (dev) or as
    /// defence-in-depth behind one.
    Jwt,
}

/// Shared application state available to all route handlers.
///
/// Wraps the storage backends as trait objects in `Arc` so the state can be
/// cheaply cloned into each handler by axum's `State` extractor.
#[derive(Clone)]
pub struct AppState {
    /// Content-addressed chunk storage.
    pub chunks: Arc<dyn ChunkStore>,
    /// Merkle tree node storage.
    pub trees: Arc<dyn TreeStore>,
    /// Directory tree node storage.
    pub nodes: Arc<dyn NodeStore>,
    /// Version DAG repository.
    pub versions: Arc<dyn VersionRepo>,
    /// Branch and tag ref repository.
    pub refs: Arc<dyn RefRepo>,
    /// Path-based metadata index.
    pub path_index: Arc<dyn PathIndexRepo>,
    /// Workspace metadata repository when backend supports workspace CRUD.
    pub workspaces: Option<Arc<dyn WorkspaceRepo>>,
    /// Tenant/member directory for SaaS workspace and team management.
    pub tenant_directory: Option<Arc<dyn TenantDirectory>>,
    /// HTTP auth integration mode for protected REST routes.
    pub http_auth_mode: HttpAuthMode,
    /// Login / token-issuing service. `None` disables `/auth/*`.
    pub auth: Option<Arc<AuthService>>,
    /// Tenant-level usage ledger. `None` disables quota enforcement.
    pub usage: Option<Arc<dyn UsageRepo>>,
    /// Plan → byte ceiling mapping.
    pub quota: QuotaPolicy,
    /// Security-relevant audit trail. `None` disables persistence (log only).
    pub audit: Option<Arc<dyn AuditRepo>>,
    /// Browser origins allowed to call the API. Empty means "no cross-origin".
    pub cors_origins: Vec<String>,
}

impl AppState {
    /// Build an `AppState` backed by RocksDB (CAS) + SQLite (metadata)
    /// for local single-user operation.
    #[cfg(feature = "local")]
    pub fn local(
        cas: crate::store::RocksDbCasStore,
        meta: crate::store::SqliteMetadataStore,
    ) -> Self {
        Self::local_from_arcs(Arc::new(cas), Arc::new(meta))
    }

    /// Same as [`AppState::local`] but takes already-shared `Arc`s, so the
    /// caller can keep its own handle on the underlying stores (e.g. for
    /// repos that are not exposed through `AppState`).
    #[cfg(feature = "local")]
    pub fn local_from_arcs(
        cas: Arc<crate::store::RocksDbCasStore>,
        meta: Arc<crate::store::SqliteMetadataStore>,
    ) -> Self {
        Self {
            chunks: cas.clone(),
            trees: cas.clone(),
            nodes: cas,
            versions: meta.clone(),
            refs: meta.clone(),
            path_index: meta.clone(),
            workspaces: Some(meta.clone()),
            tenant_directory: None,
            http_auth_mode: HttpAuthMode::Disabled,
            auth: None,
            usage: Some(meta.clone()),
            quota: QuotaPolicy::from_env(),
            audit: Some(meta),
            cors_origins: default_cors_origins(),
        }
    }

    /// Build a fully in-memory `AppState` for testing.
    pub fn memory() -> Self {
        Self {
            chunks: Arc::new(crate::store::InMemoryChunkStore::new()),
            trees: Arc::new(crate::store::InMemoryTreeStore::new()),
            nodes: Arc::new(crate::store::InMemoryNodeStore::new()),
            versions: Arc::new(crate::store::InMemoryVersionRepo::new()),
            refs: Arc::new(crate::store::InMemoryRefRepo::new()),
            path_index: Arc::new(crate::store::InMemoryPathIndex::new()),
            workspaces: None,
            tenant_directory: None,
            http_auth_mode: HttpAuthMode::Disabled,
            auth: None,
            usage: Some(Arc::new(crate::quota::MemoryUsageRepo::new())),
            quota: QuotaPolicy::from_env(),
            audit: Some(Arc::new(crate::audit::MemoryAuditRepo::new())),
            cors_origins: default_cors_origins(),
        }
    }

    /// Build a cloud `AppState` backed by S3 (chunks) + FoundationDB (metadata),
    /// **without** any cache layer.
    #[cfg(all(feature = "cloud", feature = "fdb"))]
    pub fn cloud_uncached(
        s3: crate::store::s3::S3ChunkStore,
        fdb: crate::store::fdb::FdbMetadataStore,
    ) -> Self {
        let fdb = Arc::new(fdb);
        Self {
            chunks: Arc::new(s3),
            trees: fdb.clone(),
            nodes: fdb.clone(),
            versions: fdb.clone(),
            refs: fdb.clone(),
            path_index: fdb.clone(),
            workspaces: Some(fdb.clone()),
            tenant_directory: None,
            http_auth_mode: HttpAuthMode::TrustedGatewayHeaders,
            auth: None,
            usage: Some(fdb.clone()),
            quota: QuotaPolicy::from_env(),
            audit: Some(fdb),
            cors_origins: default_cors_origins(),
        }
    }

    /// Build a cloud `AppState` backed by S3 (chunks) + FoundationDB (metadata),
    /// **with** a DragonflyDB / Redis cache layer wrapping both stores.
    #[cfg(all(feature = "cloud", feature = "fdb"))]
    pub fn cloud_cached<S, M>(
        chunks: crate::store::cache::CachedChunkStore<S>,
        meta: crate::store::cache::CachedMetadataStore<M>,
    ) -> Self
    where
        S: crate::store::ChunkStore + 'static,
        M: crate::store::TreeStore
            + crate::store::NodeStore
            + crate::store::VersionRepo
            + crate::store::RefRepo
            + crate::store::PathIndexRepo
            + Send
            + Sync
            + 'static,
    {
        let meta = Arc::new(meta);
        Self {
            chunks: Arc::new(chunks),
            trees: meta.clone(),
            nodes: meta.clone(),
            versions: meta.clone(),
            refs: meta.clone(),
            path_index: meta,
            workspaces: None,
            tenant_directory: None,
            http_auth_mode: HttpAuthMode::TrustedGatewayHeaders,
            auth: None,
            usage: None,
            quota: QuotaPolicy::from_env(),
            audit: None,
            cors_origins: default_cors_origins(),
        }
    }

    pub fn with_http_auth(mut self, http_auth_mode: HttpAuthMode) -> Self {
        self.http_auth_mode = http_auth_mode;
        self
    }

    pub fn with_tenant_directory(mut self, tenant_directory: Arc<dyn TenantDirectory>) -> Self {
        self.tenant_directory = Some(tenant_directory);
        self
    }

    /// Attach the workspace repository (cloud mode wires the FDB store).
    pub fn with_workspaces(mut self, workspaces: Arc<dyn WorkspaceRepo>) -> Self {
        self.workspaces = Some(workspaces);
        self
    }

    /// Attach the usage ledger (cloud mode wires the FDB store).
    pub fn with_usage(mut self, usage: Arc<dyn UsageRepo>) -> Self {
        self.usage = Some(usage);
        self
    }

    /// Override the quota policy (tests / per-environment tiers).
    pub fn with_quota_policy(mut self, quota: QuotaPolicy) -> Self {
        self.quota = quota;
        self
    }

    /// Attach the audit trail (cloud mode wires the FDB store).
    pub fn with_audit(mut self, audit: Arc<dyn AuditRepo>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Override the CORS allowlist (tests / per-environment policy).
    pub fn with_cors_origins(mut self, origins: Vec<String>) -> Self {
        self.cors_origins = origins;
        self
    }

    /// Append one audit record, tolerating a missing repository.
    pub fn record_audit(
        &self,
        actor: &str,
        tenant_id: &str,
        action: &str,
        target: &str,
        outcome: &str,
    ) {
        crate::audit::record(
            self.audit.as_ref(),
            crate::audit::AuditEvent::new(actor, tenant_id, action, target, outcome),
        );
    }

    /// Attach the auth service and enable `/auth/*` routes.
    ///
    /// The auth service's tenant directory becomes the state's directory when
    /// none is configured, so login and workspace APIs always agree on the
    /// caller's organization.
    pub fn with_auth(mut self, auth: Arc<AuthService>) -> Self {
        if self.tenant_directory.is_none() {
            self.tenant_directory = Some(auth.directory());
        }
        self.auth = Some(auth);
        self
    }
}

/// Browser origins allowed by default.
///
/// Production must configure `AXIOM_CORS_ORIGINS` explicitly; local dev gets
/// the console and Tauri origins so `npm run dev` keeps working.
pub fn default_cors_origins() -> Vec<String> {
    if let Ok(raw) = std::env::var("AXIOM_CORS_ORIGINS") {
        return raw
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(str::to_owned)
            .collect();
    }

    if std::env::var("AXIOM_ENV").as_deref() == Ok("production") {
        // Fail closed: no allowlist means no cross-origin browser access.
        return Vec::new();
    }

    vec![
        "http://localhost:3000".into(),
        "http://localhost:3001".into(),
        "http://localhost:5173".into(),
        "http://127.0.0.1:3000".into(),
        "http://127.0.0.1:3001".into(),
        "http://127.0.0.1:5173".into(),
        "tauri://localhost".into(),
        "http://tauri.localhost".into(),
        "https://tauri.localhost".into(),
    ]
}
