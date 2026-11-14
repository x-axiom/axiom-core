//! gRPC sync server bootstrap (`axiom.sync.v1`).
//!
//! The Push and Pull/Clone protocol implementations live in
//! [`crate::sync::push_server`] and [`crate::sync::pull_server`]; this module
//! wires them into a single tonic service and actually **serves** it. Without
//! this, the protocol only existed as library code and no process listened on
//! the sync port (P0-3).
//!
//! Authentication: when an [`AuthService`] is attached, every RPC requires
//! `authorization: Bearer <access token>` and the verified identity is inserted
//! into request extensions. Local single-user mode can run without auth.

use std::pin::Pin;
use std::sync::Arc;

use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::api::state::AppState;
use crate::auth::service::AuthService;
use crate::error::{CasError, CasResult};
use crate::store::{ChunkStore, CompositeSyncStore, NodeStore, RefRepo, SyncStore, TreeStore, VersionRepo};
use crate::sync::proto::{
    CloneDownloadRequest, CloneDownloadResponse, CloneInitRequest, CloneInitResponse,
    DownloadPackRequest, DownloadPackResponse, FinalizeRefsRequest, FinalizeRefsResponse,
    ListRefsRequest, ListRefsResponse, NegotiatePullRequest, NegotiatePullResponse,
    NegotiatePushRequest, NegotiatePushResponse, UploadPackRequest, UploadPackResponse,
    sync_service_server::{SyncService, SyncServiceServer},
};
use crate::sync::pull_server::{PullServiceHandler, PullServiceState};
use crate::sync::push_server::{PushServiceHandler, PushServiceState};

/// Default push/pull session TTL (1 hour).
const SESSION_TTL_SECS: u64 = 3600;
/// Default session reaper interval (5 minutes).
const CLEANUP_INTERVAL_SECS: u64 = 300;
/// Default cap on decompressed upload bytes per `UploadPack` (4 GiB).
const MAX_DECOMPRESSED_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Routes Push RPCs to [`PushServiceHandler`] and Pull/Clone RPCs to
/// [`PullServiceHandler`], backed by the same stores.
struct CombinedSyncHandler {
    push: PushServiceHandler,
    pull: PullServiceHandler,
}

#[tonic::async_trait]
impl SyncService for CombinedSyncHandler {
    async fn list_refs(
        &self,
        req: Request<ListRefsRequest>,
    ) -> Result<Response<ListRefsResponse>, Status> {
        self.push.list_refs(req).await
    }

    async fn negotiate_push(
        &self,
        req: Request<NegotiatePushRequest>,
    ) -> Result<Response<NegotiatePushResponse>, Status> {
        self.push.negotiate_push(req).await
    }

    async fn upload_pack(
        &self,
        req: Request<tonic::Streaming<UploadPackRequest>>,
    ) -> Result<Response<UploadPackResponse>, Status> {
        self.push.upload_pack(req).await
    }

    async fn finalize_refs(
        &self,
        req: Request<FinalizeRefsRequest>,
    ) -> Result<Response<FinalizeRefsResponse>, Status> {
        self.push.finalize_refs(req).await
    }

    async fn negotiate_pull(
        &self,
        req: Request<NegotiatePullRequest>,
    ) -> Result<Response<NegotiatePullResponse>, Status> {
        self.pull.negotiate_pull(req).await
    }

    type DownloadPackStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<DownloadPackResponse, Status>> + Send>>;

    async fn download_pack(
        &self,
        req: Request<DownloadPackRequest>,
    ) -> Result<Response<Self::DownloadPackStream>, Status> {
        let resp = self.pull.download_pack(req).await?;
        let inner = resp.into_inner();
        Ok(Response::new(Box::pin(inner)))
    }

    async fn clone_init(
        &self,
        req: Request<CloneInitRequest>,
    ) -> Result<Response<CloneInitResponse>, Status> {
        self.pull.clone_init(req).await
    }

    type CloneDownloadStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<CloneDownloadResponse, Status>> + Send>>;

    async fn clone_download(
        &self,
        req: Request<CloneDownloadRequest>,
    ) -> Result<Response<Self::CloneDownloadStream>, Status> {
        let resp = self.pull.clone_download(req).await?;
        let inner = resp.into_inner();
        Ok(Response::new(Box::pin(inner)))
    }
}

fn build_handler(state: &AppState) -> CombinedSyncHandler {
    let sync: Arc<dyn SyncStore> = Arc::new(CompositeSyncStore::new(
        Arc::clone(&state.versions),
        Arc::clone(&state.trees),
        Arc::clone(&state.nodes),
        Arc::clone(&state.chunks),
    ));

    let push = PushServiceHandler::new(PushServiceState {
        chunks: Arc::clone(&state.chunks) as Arc<dyn ChunkStore>,
        trees: Arc::clone(&state.trees) as Arc<dyn TreeStore>,
        nodes: Arc::clone(&state.nodes) as Arc<dyn NodeStore>,
        versions: Arc::clone(&state.versions) as Arc<dyn VersionRepo>,
        refs: Arc::clone(&state.refs) as Arc<dyn RefRepo>,
        sync: Arc::clone(&sync),
        session_ttl_secs: SESSION_TTL_SECS,
        cleanup_interval_secs: CLEANUP_INTERVAL_SECS,
        max_decompressed_bytes: MAX_DECOMPRESSED_BYTES,
    });

    let pull = PullServiceHandler::new(PullServiceState {
        chunks: state.chunks.clone(),
        trees: state.trees.clone(),
        nodes: state.nodes.clone(),
        versions: state.versions.clone(),
        refs: state.refs.clone(),
        sync,
        session_ttl_secs: SESSION_TTL_SECS,
        cleanup_interval_secs: CLEANUP_INTERVAL_SECS,
    });

    CombinedSyncHandler { push, pull }
}

/// Serve the sync protocol until `shutdown` resolves.
pub async fn serve(
    state: AppState,
    listen: &str,
    auth: Option<Arc<AuthService>>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> CasResult<()> {
    let addr = listen
        .parse()
        .map_err(|e| CasError::Store(format!("invalid gRPC listen address '{listen}': {e}")))?;

    let handler = build_handler(&state);
    let mut builder = Server::builder();

    match auth {
        Some(auth) => {
            let jwt = Arc::clone(auth.jwt());
            let svc = SyncServiceServer::with_interceptor(
                handler,
                crate::auth::middleware::grpc::tonic_auth_interceptor(jwt),
            );
            builder
                .add_service(svc)
                .serve_with_shutdown(addr, shutdown)
                .await
                .map_err(|e| CasError::Store(format!("grpc serve: {e}")))
        }
        None => {
            tracing::warn!("gRPC sync is running without authentication (local mode)");
            builder
                .add_service(SyncServiceServer::new(handler))
                .serve_with_shutdown(addr, shutdown)
                .await
                .map_err(|e| CasError::Store(format!("grpc serve: {e}")))
        }
    }
}
