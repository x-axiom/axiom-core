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
use crate::model::{NodeEntry, NodeKind, Ref, TreeNode, TreeNodeKind, VersionId, VersionNode};
use crate::sync::shallow::collect_versions_shallow;
use crate::sync::bloom_negotiate::BloomFilter;
use crate::store::traits::{ChunkStore, NodeStore, RefRepo, SyncStore, TreeStore, VersionRepo};
use crate::sync::proto::{
    CloneDownloadRequest, CloneDownloadResponse, CloneInitRequest, CloneInitResponse,
    DownloadPackRequest, DownloadPackResponse, FinalizeRefsRequest, FinalizeRefsResponse,
    ListRefsRequest, ListRefsResponse, NegotiatePullRequest, NegotiatePullResponse,
    NegotiatePushRequest, NegotiatePushResponse, ObjectId, PackEntry, PackHeader, RefInfo,
    UploadPackRequest, UploadPackResponse,
    clone_download_response::Payload as ClonePayload,
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
    /// Unix timestamp (seconds) when this session was created.
    created_at: u64,
}

/// Clone session — same payload shape as [`PullSession`] but keyed separately
/// so clone and pull sessions cannot collide.
#[derive(Clone, Debug)]
struct CloneSession {
    objects: Vec<(i32, [u8; 32])>,
    /// Unix timestamp (seconds) when this session was created.
    created_at: u64,
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
    /// How long (seconds) to keep an incomplete session alive after creation.
    /// Default: 3600 (1 hour).
    pub session_ttl_secs: u64,
    /// How often (seconds) the reaper task wakes up.
    /// Default: 300 (5 minutes).
    pub cleanup_interval_secs: u64,
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
    clone_sessions: Arc<Mutex<HashMap<String, CloneSession>>>,
    /// Background reaper task — aborted on drop.
    _reaper: tokio::task::AbortHandle,
}

impl PullServiceHandler {
    pub fn new(state: PullServiceState) -> Self {
        let sessions: Arc<Mutex<HashMap<String, PullSession>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let clone_sessions: Arc<Mutex<HashMap<String, CloneSession>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let ttl = state.session_ttl_secs;
        let interval = state.cleanup_interval_secs;
        let sessions_ref = Arc::clone(&sessions);
        let clone_sessions_ref = Arc::clone(&clone_sessions);

        let reaper = tokio::spawn(async move {
            let interval_dur =
                std::time::Duration::from_secs(interval.max(1));
            loop {
                tokio::time::sleep(interval_dur).await;
                let now = now_secs();
                {
                    let mut map = sessions_ref.lock();
                    let before = map.len();
                    map.retain(|_, s| now.saturating_sub(s.created_at) < ttl);
                    let expired = before - map.len();
                    if expired > 0 {
                        tracing::info!(n_expired = expired, "reaped stale pull sessions");
                    }
                }
                {
                    let mut map = clone_sessions_ref.lock();
                    let before = map.len();
                    map.retain(|_, s| now.saturating_sub(s.created_at) < ttl);
                    let expired = before - map.len();
                    if expired > 0 {
                        tracing::info!(n_expired = expired, "reaped stale clone sessions");
                    }
                }
            }
        });

        Self {
            state: Arc::new(state),
            sessions,
            clone_sessions,
            _reaper: reaper.abort_handle(),
        }
    }
}

impl Drop for PullServiceHandler {
    fn drop(&mut self) {
        self._reaper.abort();
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

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

fn build_ordered_object_list(
    want_vids: &[VersionId],
    have_vids: &HashSet<VersionId>,
    versions: &dyn VersionRepo,
    nodes: &dyn NodeStore,
    trees: &dyn TreeStore,
) -> CasResult<Vec<(i32, [u8; 32])>> {
    #[derive(Default)]
    struct OrderedObjectBuilder {
        objects: Vec<(i32, [u8; 32])>,
        seen_versions: HashSet<VersionId>,
        seen_nodes: HashSet<blake3::Hash>,
        seen_trees: HashSet<blake3::Hash>,
        seen_chunks: HashSet<blake3::Hash>,
    }

    impl OrderedObjectBuilder {
        fn push_version(
            &mut self,
            vid: &VersionId,
            have_vids: &HashSet<VersionId>,
            versions: &dyn VersionRepo,
            nodes: &dyn NodeStore,
            trees: &dyn TreeStore,
        ) -> CasResult<()> {
            if have_vids.contains(vid) || !self.seen_versions.insert(vid.clone()) {
                return Ok(());
            }

            let version = versions
                .get_version(vid)?
                .ok_or_else(|| CasError::NotFound(format!("version {}", vid.as_str())))?;

            for parent in &version.parents {
                self.push_version(parent, have_vids, versions, nodes, trees)?;
            }

            self.push_node(&version.root, nodes, trees)?;

            let bytes = hex::decode(vid.as_str())
                .map_err(|e| CasError::SyncError(format!("invalid version id {}: {e}", vid.as_str())))?;
            if bytes.len() != 32 {
                return Err(CasError::SyncError(format!(
                    "version id {} must decode to 32 bytes",
                    vid.as_str()
                )));
            }

            let arr: [u8; 32] = bytes.try_into().unwrap();
            self.objects.push((PROTO_TYPE_VERSION, arr));
            Ok(())
        }

        fn push_node(
            &mut self,
            node_hash: &blake3::Hash,
            nodes: &dyn NodeStore,
            trees: &dyn TreeStore,
        ) -> CasResult<()> {
            if !self.seen_nodes.insert(*node_hash) {
                return Ok(());
            }

            let node = nodes
                .get_node(node_hash)?
                .ok_or_else(|| CasError::NotFound(format!("node {node_hash}")))?;

            match &node.kind {
                NodeKind::File { root, .. } => self.push_tree(root, trees)?,
                NodeKind::Directory { children } => {
                    for child_hash in children.values() {
                        self.push_node(child_hash, nodes, trees)?;
                    }
                }
            }

            self.objects.push((PROTO_TYPE_NODE_ENTRY, *node_hash.as_bytes()));
            Ok(())
        }

        fn push_tree(
            &mut self,
            tree_hash: &blake3::Hash,
            trees: &dyn TreeStore,
        ) -> CasResult<()> {
            if !self.seen_trees.insert(*tree_hash) {
                return Ok(());
            }

            let tree = trees
                .get_tree_node(tree_hash)?
                .ok_or_else(|| CasError::NotFound(format!("tree node {tree_hash}")))?;

            match &tree.kind {
                TreeNodeKind::Leaf { chunk } => {
                    if self.seen_chunks.insert(*chunk) {
                        self.objects.push((PROTO_TYPE_CHUNK, *chunk.as_bytes()));
                    }
                }
                TreeNodeKind::Internal { children } => {
                    for child_hash in children {
                        self.push_tree(child_hash, trees)?;
                    }
                }
            }

            self.objects.push((PROTO_TYPE_TREE_NODE, *tree_hash.as_bytes()));
            Ok(())
        }
    }

    let mut builder = OrderedObjectBuilder::default();
    for vid in want_vids {
        builder.push_version(vid, have_vids, versions, nodes, trees)?;
    }
    Ok(builder.objects)
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
                sessions.insert(session_id.clone(), PullSession { objects: vec![], created_at: now_secs() });
            }
            return Ok(Response::new(NegotiatePullResponse {
                session_id,
                total_objects: 0,
                estimated_bytes: 0,
            }));
        }

        // Build dependency-first object list so every received object can be
        // validated against already-stored children with no forward refs.
        let mut objects = build_ordered_object_list(
            &want_vids,
            &have_vids,
            self.state.versions.as_ref(),
            self.state.nodes.as_ref(),
            self.state.trees.as_ref(),
        )
        .map_err(cas_err_to_status)?;

        // ── E05-S05: Bloom-filter post-filtering ──────────────────────────
        // If the client sent a non-empty `have_filter`, remove any objects
        // the filter claims the client already holds.  A false positive means
        // the client is missing an object (harmless if FP rate < 1 % per
        // spec); a negative is always correct (client definitely lacks it).
        if !req.have_filter.is_empty() {
            if let Some(bf) = BloomFilter::from_bytes(&req.have_filter) {
                objects.retain(|(_, hash)| !bf.contains(hash));
            }
        }

        let total_objects = objects.len() as u64;

        let session_id = new_session_id();
        {
            let mut sessions = self.sessions.lock();
            sessions.insert(session_id.clone(), PullSession { objects, created_at: now_secs() });
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

    // ── CloneInit ─────────────────────────────────────────────────────────
    async fn clone_init(
        &self,
        request: Request<CloneInitRequest>,
    ) -> Result<Response<CloneInitResponse>, Status> {
        let req = request.into_inner();

        // Collect refs, optionally filtered to the requested subset.
        let all_refs: Vec<Ref> = self.state.refs
            .list_refs(None)
            .map_err(cas_err_to_status)?;

        let selected_refs: Vec<Ref> = if req.refs.is_empty() {
            all_refs
        } else {
            all_refs.into_iter().filter(|r| req.refs.contains(&r.name)).collect()
        };

        let tips: Vec<VersionId> =
            selected_refs.iter().map(|r| r.target.clone()).collect();

        // Collect objects based on depth (None or 0 = full clone).
        let (object_roots, have_set) = if tips.is_empty() {
            (Vec::new(), HashSet::new())
        } else {
            let depth = req.depth.unwrap_or(0);
            if depth == 0 {
                (tips.clone(), HashSet::new())
            } else {
                let (version_ids, boundary) =
                    collect_versions_shallow(&tips, depth, self.state.versions.as_ref())
                        .map_err(cas_err_to_status)?;
                (version_ids, boundary.into_set())
            }
        };

        let objects = build_ordered_object_list(
            &object_roots,
            &have_set,
            self.state.versions.as_ref(),
            self.state.nodes.as_ref(),
            self.state.trees.as_ref(),
        )
        .map_err(cas_err_to_status)?;

        let total_objects = objects.len() as u64;
        let session_id = new_session_id();
        {
            let mut sessions = self.clone_sessions.lock();
            sessions.insert(session_id.clone(), CloneSession { objects, created_at: now_secs() });
        }

        // Build proto RefInfo list.
        let proto_refs: Vec<RefInfo> = selected_refs
            .iter()
            .filter_map(|r| {
                hex::decode(r.target.as_str()).ok().and_then(|bytes| {
                    if bytes.len() == 32 {
                        Some(RefInfo {
                            name: r.name.clone(),
                            target: Some(ObjectId { hash: bytes }),
                        })
                    } else {
                        None
                    }
                })
            })
            .collect();

        Ok(Response::new(CloneInitResponse {
            session_id,
            refs: proto_refs,
            total_objects,
            total_bytes: 0,
        }))
    }

    // ── CloneDownload ─────────────────────────────────────────────────────
    type CloneDownloadStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<CloneDownloadResponse, Status>> + Send>>;

    async fn clone_download(
        &self,
        request: Request<CloneDownloadRequest>,
    ) -> Result<Response<Self::CloneDownloadStream>, Status> {
        let req = request.into_inner();

        let session = {
            let mut sessions = self.clone_sessions.lock();
            sessions.remove(&req.session_id)
        }
        .ok_or_else(|| {
            Status::not_found(format!("clone session '{}' not found", req.session_id))
        })?;

        let total = session.objects.len() as u64;
        let state = Arc::clone(&self.state);

        let header_msg: Result<CloneDownloadResponse, Status> =
            Ok(CloneDownloadResponse {
                payload: Some(ClonePayload::Header(PackHeader {
                    object_count: total,
                    estimated_bytes: 0,
                    session_id: req.session_id.clone(),
                })),
            });

        let object_stream = futures_util::stream::unfold(
            (0usize, session.objects, state),
            |(idx, objects, state)| async move {
                if idx >= objects.len() {
                    return None;
                }
                let (proto_type, hash_bytes) = objects[idx];
                let hash = blake3::Hash::from_bytes(hash_bytes);

                let result = (|| -> CasResult<CloneDownloadResponse> {
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

                    Ok(CloneDownloadResponse {
                        payload: Some(ClonePayload::Entry(PackEntry {
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

        let full_stream =
            futures_util::stream::once(async move { header_msg }).chain(object_stream);

        Ok(Response::new(Box::pin(full_stream)))
    }
}
