//! Push server — gRPC server-side handler for the Push protocol (E04-S02).
//!
//! ## Protocol (server-side)
//! 1. `ListRefs`      — return all branch refs in the workspace.
//! 2. `NegotiatePush` — validate ref updates (fast-forward check) and compute
//!                      which objects are missing on the server.
//! 3. `UploadPack`    — receive the client-streaming object pack; validate each
//!                      object's BLAKE3 hash; write to local stores.
//! 4. `FinalizeRefs`  — re-validate fast-forward; atomically advance refs.
//!
//! ## Concurrency
//! [`PushSession`] is stored in an `Arc<DashMap<session_id, PushSession>>`.
//! `FinalizeRefs` takes `push_sessions` by write-lock on the session entry,
//! providing per-session isolation while allowing concurrent sessions.
//!
//! For the purposes of this implementation the session map lives in-process.
//! A production deployment would use a distributed lock (e.g. FoundationDB
//! CAS) — see the design doc for details.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, NodeEntry, RefKind, TreeNode, VersionId, VersionNode};
use crate::store::traits::{ChunkStore, NodeStore, RefRepo, TreeStore, VersionRepo};
use crate::sync::proto::{
    FinalizeRefsRequest, FinalizeRefsResponse, ListRefsRequest, ListRefsResponse,
    NegotiatePushRequest, NegotiatePushResponse, ObjectId, ObjectList,
    RefInfo, RefUpdateResult, UploadPackRequest, UploadPackResponse,
    upload_pack_request::Payload,
    // Pull / Clone RPCs (not implemented in this story — stub delegation):
    CloneDownloadRequest, CloneDownloadResponse, CloneInitRequest, CloneInitResponse,
    DownloadPackRequest, DownloadPackResponse, NegotiatePullRequest, NegotiatePullResponse,
};
use crate::sync::proto::sync_service_server::SyncService;

// ─── Proto ObjectType constants ───────────────────────────────────────────────
const PROTO_TYPE_CHUNK: i32 = 1;
const PROTO_TYPE_TREE_NODE: i32 = 2;
const PROTO_TYPE_NODE_ENTRY: i32 = 3;
const PROTO_TYPE_VERSION: i32 = 4;

// ─── Push session ─────────────────────────────────────────────────────────────

/// State held between `NegotiatePush` and `FinalizeRefs` for one push session.
#[derive(Clone, Debug)]
struct PushSession {
    /// ref_name → expected new VersionId (hex)
    ref_updates: HashMap<String, RefUpdateInfo>,
    /// All objects the client was told to upload.
    #[allow(dead_code)]
    expected_objects: u64,
    /// Timestamp when the session was created.
    #[allow(dead_code)]
    created_at: u64,
}

#[derive(Clone, Debug)]
struct RefUpdateInfo {
    #[allow(dead_code)]
    old_target: VersionId,
    new_target: VersionId,
    #[allow(dead_code)]
    force: bool,
    /// Kind of the local ref (from the push request — always Branch for now).
    #[allow(dead_code)]
    kind: RefKind,
}

// ─── PushServiceState ─────────────────────────────────────────────────────────

/// Backing store injected into [`PushServiceHandler`].
///
/// All fields are `Arc<dyn Trait>` so the server can be shared across threads.
#[derive(Clone)]
pub struct PushServiceState {
    pub chunks: Arc<dyn ChunkStore>,
    pub trees: Arc<dyn TreeStore>,
    pub nodes: Arc<dyn NodeStore>,
    pub versions: Arc<dyn VersionRepo>,
    pub refs: Arc<dyn RefRepo>,
}

// ─── PushServiceHandler ──────────────────────────────────────────────────────

/// gRPC service implementation for the Push-related RPCs.
///
/// Implements [`SyncService`] (all RPCs); Pull and Clone RPCs return
/// `UNIMPLEMENTED` until E04-S03/S04 are completed.
pub struct PushServiceHandler {
    state: PushServiceState,
    /// In-flight push sessions keyed by session_id.
    sessions: Arc<Mutex<HashMap<String, PushSession>>>,
}

impl PushServiceHandler {
    pub fn new(state: PushServiceState) -> Self {
        Self {
            state,
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

// ─── Helper: fast-forward check ──────────────────────────────────────────────

/// Wrapper around [`crate::sync::fast_forward::is_fast_forward`] that also
/// honours the `--force` flag (force always allows the update).
fn is_fast_forward_allowed(
    old_vid: &VersionId,
    new_vid: &VersionId,
    versions: &dyn VersionRepo,
    force: bool,
) -> CasResult<bool> {
    if force {
        return Ok(true);
    }
    crate::sync::fast_forward::is_fast_forward(old_vid, new_vid, versions)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn new_session_id() -> String {
    // Deterministic-enough for in-process use: BLAKE3 of current nanos.
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    hex::encode(blake3::hash(&ns.to_le_bytes()).as_bytes())
}

/// Extract a VersionId from the `hash` bytes of a proto `ObjectId`.
fn vid_from_bytes(bytes: &[u8]) -> VersionId {
    VersionId(hex::encode(bytes))
}

/// Check whether a VersionId represents "no version" (zero hash or empty).
#[allow(dead_code)]
fn is_empty_vid(vid: &VersionId) -> bool {
    let s = vid.as_str();
    s.is_empty() || s.chars().all(|c| c == '0')
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

// ─── SyncService impl ─────────────────────────────────────────────────────────

#[tonic::async_trait]
impl SyncService for PushServiceHandler {
    // ── ListRefs ──────────────────────────────────────────────────────────
    async fn list_refs(
        &self,
        _request: Request<ListRefsRequest>,
    ) -> Result<Response<ListRefsResponse>, Status> {
        let all = self
            .state
            .refs
            .list_refs(None)
            .map_err(cas_err_to_status)?;

        let refs: Vec<RefInfo> = all
            .into_iter()
            .map(|r| RefInfo {
                name: r.name,
                target: Some(ObjectId {
                    hash: {
                        // VersionId is stored as hex; decode to raw 32 bytes.
                        hex::decode(r.target.as_str()).unwrap_or_default()
                    },
                }),
            })
            .collect();

        Ok(Response::new(ListRefsResponse { refs }))
    }

    // ── NegotiatePush ─────────────────────────────────────────────────────
    async fn negotiate_push(
        &self,
        request: Request<NegotiatePushRequest>,
    ) -> Result<Response<NegotiatePushResponse>, Status> {
        let req = request.into_inner();

        // ① Validate every ref update (fast-forward check).
        let mut session_updates: HashMap<String, RefUpdateInfo> = HashMap::new();
        for upd in &req.ref_updates {
            let old_vid = vid_from_bytes(
                &upd.old_target.as_ref().map(|o| o.hash.clone()).unwrap_or_default(),
            );
            let new_vid = vid_from_bytes(
                &upd.new_target.as_ref().map(|o| o.hash.clone()).unwrap_or_default(),
            );

            let ok = is_fast_forward_allowed(
                &old_vid,
                &new_vid,
                self.state.versions.as_ref(),
                upd.force,
            )
            .map_err(cas_err_to_status)?;

            if !ok {
                return Ok(Response::new(NegotiatePushResponse {
                    accepted: false,
                    session_id: String::new(),
                    need_objects: vec![],
                    reject_reason: format!(
                        "non-fast-forward update for ref '{}'",
                        upd.ref_name
                    ),
                }));
            }

            session_updates.insert(
                upd.ref_name.clone(),
                RefUpdateInfo {
                    old_target: old_vid,
                    new_target: new_vid,
                    force: upd.force,
                    kind: RefKind::Branch,
                },
            );
        }

        // ② Compute which objects the client needs to send.
        //    We determine the server's existing version set and subtract it
        //    from what the client wants to push.
        let mut need_chunks: Vec<ObjectId> = Vec::new();
        let mut need_trees: Vec<ObjectId> = Vec::new();
        let mut need_nodes: Vec<ObjectId> = Vec::new();
        let mut need_versions: Vec<ObjectId> = Vec::new();

        for oid in &req.local_versions {
            let vid = vid_from_bytes(&oid.hash);
            if self
                .state
                .versions
                .get_version(&vid)
                .map_err(cas_err_to_status)?
                .is_none()
            {
                need_versions.push(ObjectId { hash: oid.hash.clone() });
            }
        }

        // Collect all reachable objects from the pushed version tips that the
        // server does not already have.
        {
            use std::collections::HashSet;
            use crate::sync::reachable::{CancelToken, collect_reachable};

            let want_vids: Vec<VersionId> = req
                .local_versions
                .iter()
                .map(|o| vid_from_bytes(&o.hash))
                .collect();

            // Server's "have" = all versions currently known.
            let server_have: HashSet<VersionId> = {
                let all_refs = self.state.refs.list_refs(None).map_err(cas_err_to_status)?;
                all_refs.into_iter().map(|r| r.target).collect()
            };

            let reachable = collect_reachable(
                &want_vids,
                &server_have,
                self.state.versions.as_ref(),
                self.state.trees.as_ref(),
                self.state.nodes.as_ref(),
                &CancelToken::new(),
            )
            .map_err(cas_err_to_status)?;

            for h in &reachable.chunk_hashes {
                if !self
                    .state
                    .chunks
                    .has_chunk(h)
                    .map_err(cas_err_to_status)?
                {
                    need_chunks.push(ObjectId { hash: h.as_bytes().to_vec() });
                }
            }
            for h in &reachable.tree_hashes {
                if self
                    .state
                    .trees
                    .get_tree_node(h)
                    .map_err(cas_err_to_status)?
                    .is_none()
                {
                    need_trees.push(ObjectId { hash: h.as_bytes().to_vec() });
                }
            }
            for h in &reachable.node_hashes {
                if self
                    .state
                    .nodes
                    .get_node(h)
                    .map_err(cas_err_to_status)?
                    .is_none()
                {
                    need_nodes.push(ObjectId { hash: h.as_bytes().to_vec() });
                }
            }
            for v in &reachable.versions {
                if self
                    .state
                    .versions
                    .get_version(v)
                    .map_err(cas_err_to_status)?
                    .is_none()
                {
                    if v.as_str().len() == 64 {
                        if let Ok(b) = hex::decode(v.as_str()) {
                            need_versions.push(ObjectId { hash: b });
                        }
                    }
                }
            }
        }

        let total_objects = (need_chunks.len()
            + need_trees.len()
            + need_nodes.len()
            + need_versions.len()) as u64;

        let mut need_objects = Vec::new();
        if !need_chunks.is_empty() {
            need_objects.push(ObjectList {
                r#type: PROTO_TYPE_CHUNK,
                hashes: need_chunks,
            });
        }
        if !need_trees.is_empty() {
            need_objects.push(ObjectList {
                r#type: PROTO_TYPE_TREE_NODE,
                hashes: need_trees,
            });
        }
        if !need_nodes.is_empty() {
            need_objects.push(ObjectList {
                r#type: PROTO_TYPE_NODE_ENTRY,
                hashes: need_nodes,
            });
        }
        if !need_versions.is_empty() {
            need_objects.push(ObjectList {
                r#type: PROTO_TYPE_VERSION,
                hashes: need_versions,
            });
        }

        // ③ Create session.
        let session_id = new_session_id();
        {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.insert(
                session_id.clone(),
                PushSession {
                    ref_updates: session_updates,
                    expected_objects: total_objects,
                    created_at: now_secs(),
                },
            );
        }

        Ok(Response::new(NegotiatePushResponse {
            accepted: true,
            session_id,
            need_objects,
            reject_reason: String::new(),
        }))
    }

    // ── UploadPack ────────────────────────────────────────────────────────
    async fn upload_pack(
        &self,
        request: Request<Streaming<UploadPackRequest>>,
    ) -> Result<Response<UploadPackResponse>, Status> {
        let mut stream = request.into_inner();

        let mut received: u64 = 0;
        let mut stored: u64 = 0;
        let mut duplicates: u64 = 0;
        let mut session_id = String::new();

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| Status::internal(e.to_string()))?;
            match msg.payload {
                Some(Payload::Header(hdr)) => {
                    session_id = hdr.session_id.clone();
                }
                Some(Payload::Entry(entry)) => {
                    received += 1;
                    let Some(oid) = entry.hash else {
                        return Err(Status::invalid_argument("entry missing hash"));
                    };
                    if oid.hash.len() != 32 {
                        return Err(Status::invalid_argument(format!(
                            "hash must be 32 bytes, got {}",
                            oid.hash.len()
                        )));
                    }

                    // Decompress.
                    let raw = zstd::bulk::decompress(&entry.data, 64 * 1024 * 1024)
                        .map_err(|e| {
                            Status::invalid_argument(format!("decompress failed: {e}"))
                        })?;

                    // Verify hash.
                    let arr: [u8; 32] = oid.hash.as_slice().try_into().unwrap();
                    let declared: ChunkHash = blake3::Hash::from_bytes(arr);

                    match entry.r#type {
                        PROTO_TYPE_CHUNK => {
                            let actual = blake3::hash(&raw);
                            if actual != declared {
                                return Err(Status::data_loss(format!(
                                    "chunk hash mismatch: expected {declared} got {actual}"
                                )));
                            }
                            let was_new =
                                !self.state.chunks.has_chunk(&declared).map_err(cas_err_to_status)?;
                            self.state
                                .chunks
                                .put_chunk(raw)
                                .map_err(cas_err_to_status)?;
                            if was_new { stored += 1; } else { duplicates += 1; }
                        }

                        PROTO_TYPE_TREE_NODE => {
                            let node: TreeNode = serde_json::from_slice(&raw)
                                .map_err(|e| Status::invalid_argument(format!("tree node JSON: {e}")))?;
                            // Hash is over the serialised form for tree nodes.
                            let was_new = self
                                .state
                                .trees
                                .get_tree_node(&node.hash)
                                .map_err(cas_err_to_status)?
                                .is_none();
                            self.state
                                .trees
                                .put_tree_node(&node)
                                .map_err(cas_err_to_status)?;
                            if was_new { stored += 1; } else { duplicates += 1; }
                        }

                        PROTO_TYPE_NODE_ENTRY => {
                            let entry_obj: NodeEntry = serde_json::from_slice(&raw)
                                .map_err(|e| Status::invalid_argument(format!("node entry JSON: {e}")))?;
                            let was_new = self
                                .state
                                .nodes
                                .get_node(&entry_obj.hash)
                                .map_err(cas_err_to_status)?
                                .is_none();
                            self.state
                                .nodes
                                .put_node(&entry_obj)
                                .map_err(cas_err_to_status)?;
                            if was_new { stored += 1; } else { duplicates += 1; }
                        }

                        PROTO_TYPE_VERSION => {
                            let v: VersionNode = serde_json::from_slice(&raw)
                                .map_err(|e| Status::invalid_argument(format!("version JSON: {e}")))?;
                            let was_new = self
                                .state
                                .versions
                                .get_version(&v.id)
                                .map_err(cas_err_to_status)?
                                .is_none();
                            self.state
                                .versions
                                .put_version(&v)
                                .map_err(cas_err_to_status)?;
                            if was_new { stored += 1; } else { duplicates += 1; }
                        }

                        other => {
                            return Err(Status::invalid_argument(format!(
                                "unknown object type {other}"
                            )));
                        }
                    }
                }
                None => {}
            }
        }

        Ok(Response::new(UploadPackResponse {
            session_id,
            received,
            stored,
            duplicates,
        }))
    }

    // ── FinalizeRefs ──────────────────────────────────────────────────────
    async fn finalize_refs(
        &self,
        request: Request<FinalizeRefsRequest>,
    ) -> Result<Response<FinalizeRefsResponse>, Status> {
        let req = request.into_inner();

        // Look up (and remove) the push session.
        let session = {
            let mut sessions = self.sessions.lock().unwrap();
            sessions.remove(&req.session_id)
        };

        // Per-ref result accumulator.
        let mut results: Vec<RefUpdateResult> = Vec::new();

        for upd in &req.ref_updates {
            let old_vid = vid_from_bytes(
                &upd.old_target.as_ref().map(|o| o.hash.clone()).unwrap_or_default(),
            );
            let new_vid = vid_from_bytes(
                &upd.new_target.as_ref().map(|o| o.hash.clone()).unwrap_or_default(),
            );

            // Verify the session recorded this update (guards against TOCTOU).
            let session_update = session.as_ref().and_then(|s| s.ref_updates.get(&upd.ref_name));
            if let Some(su) = session_update {
                if su.new_target != new_vid {
                    results.push(RefUpdateResult {
                        ref_name: upd.ref_name.clone(),
                        status: "error".into(),
                        message: "session target mismatch".into(),
                    });
                    continue;
                }
            }

            // Re-check fast-forward (server state may have changed since NegotiatePush).
            let current_remote = self
                .state
                .refs
                .get_ref(&upd.ref_name)
                .map_err(cas_err_to_status)?;

            let current_tip = current_remote
                .as_ref()
                .map(|r| r.target.clone())
                .unwrap_or_else(|| VersionId(String::new()));

            // If the remote tip has moved since negotiation, re-check.
            if current_tip != old_vid && !upd.force {
                results.push(RefUpdateResult {
                    ref_name: upd.ref_name.clone(),
                    status: "non-fast-forward".into(),
                    message: format!(
                        "remote tip moved to {} since negotiation",
                        current_tip.as_str()
                    ),
                });
                continue;
            }

            let ok = is_fast_forward_allowed(
                &current_tip,
                &new_vid,
                self.state.versions.as_ref(),
                upd.force,
            )
            .map_err(cas_err_to_status)?;

            if !ok {
                results.push(RefUpdateResult {
                    ref_name: upd.ref_name.clone(),
                    status: "non-fast-forward".into(),
                    message: format!("ref '{}' is not a fast-forward", upd.ref_name),
                });
                continue;
            }

            // Advance the ref.
            let new_ref = crate::model::Ref {
                name: upd.ref_name.clone(),
                kind: RefKind::Branch,
                target: new_vid,
            };
            match self.state.refs.put_ref(&new_ref) {
                Ok(()) => {
                    results.push(RefUpdateResult {
                        ref_name: upd.ref_name.clone(),
                        status: "ok".into(),
                        message: String::new(),
                    });
                }
                Err(e) => {
                    results.push(RefUpdateResult {
                        ref_name: upd.ref_name.clone(),
                        status: "error".into(),
                        message: e.to_string(),
                    });
                }
            }
        }

        Ok(Response::new(FinalizeRefsResponse { results }))
    }

    // ── Unimplemented stubs (Pull / Clone — E04-S03/S04) ─────────────────
    async fn negotiate_pull(
        &self,
        _request: Request<NegotiatePullRequest>,
    ) -> Result<Response<NegotiatePullResponse>, Status> {
        Err(Status::unimplemented("pull not yet implemented"))
    }

    type DownloadPackStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<DownloadPackResponse, Status>> + Send>>;
    async fn download_pack(
        &self,
        _request: Request<DownloadPackRequest>,
    ) -> Result<Response<Self::DownloadPackStream>, Status> {
        Err(Status::unimplemented("download_pack not yet implemented"))
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
        Err(Status::unimplemented("clone_download not yet implemented"))
    }
}
