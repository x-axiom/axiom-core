//! Pull server — gRPC server-side handler for the Pull protocol (E04-S04).
//!
//! ## Protocol (server-side)
//! 1. `NegotiatePull`  — compute which objects the client is missing based on
//!                       its `have` set; create a pull session.
//! 2. `DownloadPack`   — server-streaming: header then one entry per object.
//!
//! ## Memory safety for large object sets
//! Object data is **not** loaded into memory upfront.  `DownloadPack` uses
//! [`futures_util::stream::unfold`] to lazily fetch + compress one object at a
//! time from the backing stores, keeping peak RSS bounded regardless of the
//! number of objects (10,000+ covered by the story acceptance criteria).

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use parking_lot::Mutex;
use tonic::{Request, Response, Status};

use crate::error::{CasError, CasResult};
use crate::model::{NodeEntry, TreeNode, VersionId, VersionNode};
use crate::store::traits::{ChunkStore, NodeStore, RefRepo, SyncStore, TreeStore, VersionRepo};
use crate::sync::proto::{
    CloneDownloadRequest, CloneDownloadResponse, CloneInitRequest, CloneInitResponse,
    DownloadPackRequest, DownloadPackResponse, FinalizeRefsRequest, FinalizeRefsResponse,
    ListRefsRequest, ListRefsResponse, NegotiatePullRequest, NegotiatePullResponse,
    NegotiatePushRequest, NegotiatePushResponse, ObjectId, PackEntry, PackHeader,
    UploadPackRequest, UploadPackResponse,
    download_pack_response::Payload,
};
use crate::sync::proto::sync_service_server::SyncService;

// ─── Proto ObjectType constants ──────────────────────────────────────────────
const PROTO_TYPE_CHUNK: i32 = 1;
const PROTO_TYPE_TREE_NODE: i32 = 2;
const PROTO_TYPE_NODE_ENTRY: i32 = 3;
const PROTO_TYPE_VERSION: i32 = 4;

// ─── Pull session ─────────────────────────────────────────────────────────────

/// An ordered list of (proto_type, 32-byte hash) pairs the server will stream
/// to the client during `DownloadPack`.
#[derive(Clone, Debug)]
struct PullSession {
    objects: Vec<(i32, [u8; 32])>,
}

// ─── PullServiceState ─────────────────────────────────────────────────────────

/// Backing stores injected into [`PullServiceHandler`].
///
/// Wrapped in `Arc` so the server handle and the stream closure can share
/// ownership without cloning.
#[derive(Clone)]
pub struct PullServiceState {
    pub chunks: Arc<dyn ChunkStore>,
    pub trees: Arc<dyn TreeStore>,
    pub nodes: Arc<dyn NodeStore>,
    pub versions: Arc<dyn VersionRepo>,
    pub refs: Arc<dyn RefRepo>,
    /// Sync graph walker. Used by `NegotiatePull` to compute the diff between
    /// `want` and the client's `have` set.
    pub sync: Arc<dyn SyncStore>,
}

// ─── PullServiceHandler ──────────────────────────────────────────────────────

/// gRPC service implementation for the Pull-related RPCs.
///
/// `NegotiatePull` and `DownloadPack` are fully implemented.  All Push and
/// Clone RPCs return `UNIMPLEMENTED` until E04-S07 integration wires a
/// combined handler.
pub struct PullServiceHandler {
    /// Wrapped in `Arc` so stream closures can cheaply clone a reference.
    state: Arc<PullServiceState>,
    sessions: Arc<Mutex<HashMap<String, PullSession>>>,
}

impl PullServiceHandler {
    pub fn new(state: PullServiceState) -> Self {
        Self {
            state: Arc::new(state),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Monotonic counter to disambiguate session IDs minted within the same
/// nanosecond. See [`crate::sync::push_server`] for rationale.
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_session_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let counter = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut buf = [0u8; 24];
    buf[..16].copy_from_slice(&nanos.to_le_bytes());
    buf[16..].copy_from_slice(&counter.to_le_bytes());
    hex::encode(blake3::hash(&buf).as_bytes())
}

fn cas_err_to_status(e: CasError) -> Status {
    match e {
        CasError::NotFound(s) => Status::not_found(s),
        CasError::AlreadyExists => Status::already_exists("already exists"),
        CasError::NonFastForward(s) => Status::failed_precondition(format!("non-fast-forward: {s}")),
        CasError::Unauthorized(s) => Status::unauthenticated(s),
        CasError::Forbidden(s) => Status::permission_denied(s),
        other => Status::internal(other.to_string()),
    }
}

/// Fetch the raw bytes of one object (uncompressed) for streaming.
fn fetch_raw(
    proto_type: i32,
    hash: &blake3::Hash,
    chunks: &dyn ChunkStore,
    trees: &dyn TreeStore,
    nodes: &dyn NodeStore,
    versions: &dyn VersionRepo,
) -> CasResult<Vec<u8>> {
    match proto_type {
        PROTO_TYPE_CHUNK => chunks
            .get_chunk(hash)?
            .ok_or_else(|| CasError::NotFound(format!("chunk {hash}"))),

        PROTO_TYPE_TREE_NODE => {
            let node: TreeNode = trees
                .get_tree_node(hash)?
                .ok_or_else(|| CasError::NotFound(format!("tree node {hash}")))?;
            serde_json::to_vec(&node).map_err(CasError::from)
        }

        PROTO_TYPE_NODE_ENTRY => {
            let entry: NodeEntry = nodes
                .get_node(hash)?
                .ok_or_else(|| CasError::NotFound(format!("node {hash}")))?;
            serde_json::to_vec(&entry).map_err(CasError::from)
        }

        PROTO_TYPE_VERSION => {
            let vid = VersionId(hex::encode(hash.as_bytes()));
            let v: VersionNode = versions
                .get_version(&vid)?
                .ok_or_else(|| CasError::NotFound(format!("version {vid}")))?;
            serde_json::to_vec(&v).map_err(CasError::from)
        }

        other => Err(CasError::SyncError(format!("unknown object type {other}"))),
    }
}

// ─── SyncService impl ─────────────────────────────────────────────────────────

#[tonic::async_trait]
impl SyncService for PullServiceHandler {
    // ── NegotiatePull ─────────────────────────────────────────────────────
    async fn negotiate_pull(
        &self,
        request: Request<NegotiatePullRequest>,
    ) -> Result<Response<NegotiatePullResponse>, Status> {
        let req = request.into_inner();

        // Build want / have sets from the WantEntry list.
        let mut want_vids: Vec<VersionId> = Vec::new();
        let mut have_vids: HashSet<VersionId> = HashSet::new();

        for entry in &req.wants {
            if let Some(w) = &entry.want {
                if w.hash.len() == 32 {
                    want_vids.push(VersionId(hex::encode(&w.hash)));
                }
            }
            if let Some(h) = &entry.have {
                if h.hash.len() == 32 && !h.hash.iter().all(|b| *b == 0) {
                    have_vids.insert(VersionId(hex::encode(&h.hash)));
                }
            }
        }

        if want_vids.is_empty() {
            let session_id = new_session_id();
            {
                let mut sessions = self.sessions.lock();
                sessions.insert(session_id.clone(), PullSession { objects: vec![] });
            }
            return Ok(Response::new(NegotiatePullResponse {
                session_id,
                total_objects: 0,
                estimated_bytes: 0,
            }));
        }

        // BFS to collect all objects the client is missing.
        let reachable = self
            .state
            .sync
            .collect_reachable_with_have(&want_vids, &have_vids)
            .map_err(cas_err_to_status)?;

        // Build ordered object list: versions → tree nodes → node entries → chunks.
        // This ordering means the client can store objects in receive-order
        // without forward-reference gaps.
        let mut objects: Vec<(i32, [u8; 32])> = Vec::new();

        for vid in &reachable.versions {
            if let Ok(bytes) = hex::decode(vid.as_str()) {
                if bytes.len() == 32 {
                    let arr: [u8; 32] = bytes.try_into().unwrap();
                    objects.push((PROTO_TYPE_VERSION, arr));
                }
            }
        }
        for h in &reachable.tree_hashes {
            objects.push((PROTO_TYPE_TREE_NODE, *h.as_bytes()));
        }
        for h in &reachable.node_hashes {
            objects.push((PROTO_TYPE_NODE_ENTRY, *h.as_bytes()));
        }
        for h in &reachable.chunk_hashes {
            objects.push((PROTO_TYPE_CHUNK, *h.as_bytes()));
        }

        let total_objects = objects.len() as u64;

        let session_id = new_session_id();
        {
            let mut sessions = self.sessions.lock();
            sessions.insert(session_id.clone(), PullSession { objects });
        }

        Ok(Response::new(NegotiatePullResponse {
            session_id,
            total_objects,
            estimated_bytes: 0, // informational; omit for now
        }))
    }

    // ── DownloadPack ──────────────────────────────────────────────────────
    type DownloadPackStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<DownloadPackResponse, Status>> + Send>>;

    async fn download_pack(
        &self,
        request: Request<DownloadPackRequest>,
    ) -> Result<Response<Self::DownloadPackStream>, Status> {
        let req = request.into_inner();

        let session = {
            let mut sessions = self.sessions.lock();
            sessions.remove(&req.session_id)
        }
        .ok_or_else(|| Status::not_found(format!("session '{}' not found", req.session_id)))?;

        let total = session.objects.len() as u64;
        let state = Arc::clone(&self.state);

        // First message: header.
        let header_msg: Result<DownloadPackResponse, Status> = Ok(DownloadPackResponse {
            payload: Some(Payload::Header(PackHeader {
                object_count: total,
                estimated_bytes: 0,
                session_id: req.session_id.clone(),
            })),
        });

        // Subsequent messages: one per object, fetched lazily.
        // `unfold` keeps (index, objects, Arc<state>) as its state, advancing
        // one step per poll so only one object is in memory at a time.
        let object_stream = futures_util::stream::unfold(
            (0usize, session.objects, state),
            |(idx, objects, state)| async move {
                if idx >= objects.len() {
                    return None;
                }
                let (proto_type, hash_bytes) = objects[idx];
                let hash = blake3::Hash::from_bytes(hash_bytes);

                let result = (|| -> CasResult<DownloadPackResponse> {
                    let raw = fetch_raw(
                        proto_type,
                        &hash,
                        state.chunks.as_ref(),
                        state.trees.as_ref(),
                        state.nodes.as_ref(),
                        state.versions.as_ref(),
                    )?;
                    let compressed = zstd::bulk::compress(&raw, 3)
                        .map_err(|e| CasError::SyncError(format!("zstd compress: {e}")))?;

                    Ok(DownloadPackResponse {
                        payload: Some(Payload::Entry(PackEntry {
                            r#type: proto_type,
                            hash: Some(ObjectId {
                                hash: hash_bytes.to_vec(),
                            }),
                            data: compressed,
                        })),
                    })
                })();

                let item = result.map_err(cas_err_to_status);
                Some((item, (idx + 1, objects, state)))
            },
        );

        let full_stream = futures_util::stream::once(async move { header_msg }).chain(object_stream);

        Ok(Response::new(Box::pin(full_stream)))
    }

    // ── Unimplemented stubs (Push / Clone) ────────────────────────────────
    async fn list_refs(
        &self,
        _request: Request<ListRefsRequest>,
    ) -> Result<Response<ListRefsResponse>, Status> {
        Err(Status::unimplemented("use PushServiceHandler for list_refs"))
    }

    async fn negotiate_push(
        &self,
        _request: Request<NegotiatePushRequest>,
    ) -> Result<Response<NegotiatePushResponse>, Status> {
        Err(Status::unimplemented("push not handled by PullServiceHandler"))
    }

    async fn upload_pack(
        &self,
        _request: tonic::Request<tonic::Streaming<UploadPackRequest>>,
    ) -> Result<Response<UploadPackResponse>, Status> {
        Err(Status::unimplemented("push not handled by PullServiceHandler"))
    }

    async fn finalize_refs(
        &self,
        _request: Request<FinalizeRefsRequest>,
    ) -> Result<Response<FinalizeRefsResponse>, Status> {
        Err(Status::unimplemented("push not handled by PullServiceHandler"))
    }

    async fn clone_init(
        &self,
        _request: Request<CloneInitRequest>,
    ) -> Result<Response<CloneInitResponse>, Status> {
        Err(Status::unimplemented("clone not yet implemented"))
    }

    type CloneDownloadStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<CloneDownloadResponse, Status>> + Send>>;

    async fn clone_download(
        &self,
        _request: Request<CloneDownloadRequest>,
    ) -> Result<Response<Self::CloneDownloadStream>, Status> {
        Err(Status::unimplemented("clone not yet implemented"))
    }
}
