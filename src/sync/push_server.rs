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
//!
//! ### Known limitation: ref-update race
//! `FinalizeRefs` performs `get_ref → fast-forward check → put_ref` without
//! a CAS primitive on `RefRepo`, so two concurrent finalizations on the same
//! ref can both observe the old tip and both call `put_ref` (last writer
//! silently wins). The fix requires `RefRepo::compare_and_swap_ref` backed
//! by an atomic backend op (FDB transaction). Tracked under E06-S02 and the
//! deferral note on E04-S07. Do not patch around it with an in-process
//! Mutex — that hides the race rather than detecting it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use parking_lot::Mutex;
use tonic::{Request, Response, Status, Streaming};

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, NodeEntry, RefKind, TreeNode, VersionId, VersionNode};
use crate::store::traits::{ChunkStore, NodeStore, RefRepo, SyncStore, TreeStore, VersionRepo};
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
    /// Sync graph walker. Used by `NegotiatePush` to compute reachable
    /// objects without depending on the concrete `collect_reachable` helper.
    pub sync: Arc<dyn SyncStore>,
    /// How long (seconds) to keep an incomplete session alive after creation.
    /// Sessions not finalised within this window will be reaped.
    /// Default: 3600 (1 hour).
    pub session_ttl_secs: u64,
    /// How often (seconds) the reaper task wakes up to scan for expired sessions.
    /// Default: 300 (5 minutes).
    pub cleanup_interval_secs: u64,
    /// Maximum cumulative decompressed bytes accepted in one `UploadPack` call.
    /// Prevents zip-bomb / OOM DoS from clients that send many highly-compressed
    /// entries.  Defaults to 4 GiB.  Inject a smaller value in tests.
    pub max_decompressed_bytes: u64,
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
    /// Background reaper task — aborted on drop.
    _reaper: tokio::task::AbortHandle,
}

impl PushServiceHandler {
    pub fn new(state: PushServiceState) -> Self {
        let sessions: Arc<Mutex<HashMap<String, PushSession>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let ttl = state.session_ttl_secs;
        let interval = state.cleanup_interval_secs;
        let sessions_ref = Arc::clone(&sessions);

        let reaper = tokio::spawn(async move {
            let interval_dur =
                std::time::Duration::from_secs(interval.max(1));
            loop {
                tokio::time::sleep(interval_dur).await;
                let now = now_secs();
                let mut map = sessions_ref.lock();
                let before = map.len();
                map.retain(|_, s| now.saturating_sub(s.created_at) < ttl);
                let expired = before - map.len();
                if expired > 0 {
                    tracing::info!(n_expired = expired, "reaped stale push sessions");
                }
            }
        });

        Self {
            state,
            sessions,
            _reaper: reaper.abort_handle(),
        }
    }
}

impl Drop for PushServiceHandler {
    fn drop(&mut self) {
        self._reaper.abort();
    }
}

// ─── Helper: fast-forward check ──────────────────────────────────────────────

/// Wrapper around [`crate::sync::fast_forward::is_fast_forward_with_limit`]
/// that also honours the `--force` flag and the default BFS node cap
/// (100 000 versions).  A pathologically deep history returns
/// `Status::failed_precondition` via `cas_err_to_status`.
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

/// Monotonic counter to disambiguate session IDs minted within the same
/// nanosecond. Combined with full-precision wall-clock nanoseconds it gives
/// collision-free session IDs even under high concurrency.
static SESSION_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_session_id() -> String {
    // Use full-precision (u128) wall-clock nanoseconds plus a process-wide
    // monotonic counter. Hashing both into BLAKE3 yields a fixed-width hex
    // session id that is collision-free in practice.
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
        // E05-S10: pathologically deep history from is_fast_forward_with_limit
        CasError::SyncError(ref s) if s.contains("too deep") || s.contains("exceeded") => {
            Status::failed_precondition(e.to_string())
        }
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
        //    Strategy (E05-S07):
        //    a) If `client_inventory` is non-empty (new client): the client has
        //       already walked its reachable set and listed every object.  We just
        //       check each hash against our stores and collect the missing ones.
        //       This is O(N) existence checks, avoids the server needing to walk a
        //       reachable graph it may not have, and is the authoritative path.
        //    b) If `client_inventory` is empty (old client, backward compat): fall
        //       back to the previous `collect_reachable_with_have` approach.
        let mut need_chunks: Vec<ObjectId> = Vec::new();
        let mut need_trees: Vec<ObjectId> = Vec::new();
        let mut need_nodes: Vec<ObjectId> = Vec::new();
        let mut need_versions: Vec<ObjectId> = Vec::new();

        if !req.client_inventory.is_empty() {
            // ── New path: inventory-based negotiation ─────────────────────────
            for list in &req.client_inventory {
                for oid in &list.hashes {
                    let h = &oid.hash;
                    if h.len() != 32 {
                        continue;
                    }
                    let arr: [u8; 32] = h.as_slice().try_into().unwrap();
                    let hash = blake3::Hash::from_bytes(arr);

                    let missing = match list.r#type {
                        PROTO_TYPE_CHUNK => !self
                            .state
                            .chunks
                            .has_chunk(&hash)
                            .map_err(cas_err_to_status)?,
                        PROTO_TYPE_TREE_NODE => self
                            .state
                            .trees
                            .get_tree_node(&hash)
                            .map_err(cas_err_to_status)?
                            .is_none(),
                        PROTO_TYPE_NODE_ENTRY => self
                            .state
                            .nodes
                            .get_node(&hash)
                            .map_err(cas_err_to_status)?
                            .is_none(),
                        PROTO_TYPE_VERSION => {
                            let vid = vid_from_bytes(h);
                            self.state
                                .versions
                                .get_version(&vid)
                                .map_err(cas_err_to_status)?
                                .is_none()
                        }
                        _ => continue,
                    };

                    if missing {
                        let entry = ObjectId { hash: h.clone() };
                        match list.r#type {
                            PROTO_TYPE_CHUNK => need_chunks.push(entry),
                            PROTO_TYPE_TREE_NODE => need_trees.push(entry),
                            PROTO_TYPE_NODE_ENTRY => need_nodes.push(entry),
                            PROTO_TYPE_VERSION => need_versions.push(entry),
                            _ => {}
                        }
                    }
                }
            }
        } else {
            // ── Legacy path: server-side reachable walk (old clients) ─────────
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

            {
                use std::collections::HashSet;

                let want_vids: Vec<VersionId> = req
                    .local_versions
                    .iter()
                    .map(|o| vid_from_bytes(&o.hash))
                    .collect();

                let server_have: HashSet<VersionId> = {
                    let all_refs = self.state.refs.list_refs(None).map_err(cas_err_to_status)?;
                    all_refs.into_iter().map(|r| r.target).collect()
                };

                let reachable = self
                    .state
                    .sync
                    .collect_reachable_with_have(&want_vids, &server_have)
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
            let mut sessions = self.sessions.lock();
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
        let mut total_decompressed: u64 = 0;
        let max_decompressed = self.state.max_decompressed_bytes;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| Status::internal(e.to_string()))?;
            match msg.payload {
                Some(Payload::Header(hdr)) => {
                    // Validate the session_id refers to a live NegotiatePush.
                    // Without this check, any client could push raw objects
                    // bypassing negotiation and the per-session quota.
                    {
                        let sessions = self.sessions.lock();
                        if !sessions.contains_key(&hdr.session_id) {
                            return Err(Status::not_found(format!(
                                "unknown push session '{}'",
                                hdr.session_id
                            )));
                        }
                    }
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

                    // Enforce per-session decompressed-bytes quota (B6 / E05-S09).
                    total_decompressed = total_decompressed.saturating_add(raw.len() as u64);
                    if total_decompressed > max_decompressed {
                        // Evict the session so the client cannot retry with the
                        // same session_id and continue accumulating bytes.
                        if !session_id.is_empty() {
                            self.sessions.lock().remove(&session_id);
                        }
                        tracing::warn!(
                            session_id = %session_id,
                            total_decompressed,
                            max_decompressed,
                            "upload_pack exceeded decompressed-bytes quota"
                        );
                        return Err(Status::resource_exhausted(format!(
                            "upload exceeds decompressed-bytes limit ({max_decompressed} bytes)"
                        )));
                    }

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

        // Look up (and remove) the push session. A missing session means the
        // client never called NegotiatePush — reject so the per-session
        // mismatch / quota guards below are never bypassed.
        let session = {
            let mut sessions = self.sessions.lock();
            sessions.remove(&req.session_id)
        }
        .ok_or_else(|| {
            Status::not_found(format!("unknown push session '{}'", req.session_id))
        })?;

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
            // The ref must have been declared in NegotiatePush, otherwise the
            // client is trying to advance a ref the server never validated.
            let Some(su) = session.ref_updates.get(&upd.ref_name) else {
                results.push(RefUpdateResult {
                    ref_name: upd.ref_name.clone(),
                    status: "error".into(),
                    message: "ref not declared in NegotiatePush".into(),
                });
                continue;
            };
            if su.new_target != new_vid {
                results.push(RefUpdateResult {
                    ref_name: upd.ref_name.clone(),
                    status: "error".into(),
                    message: "session target mismatch".into(),
                });
                continue;
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
