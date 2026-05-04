//! Clone client — first-time full repository clone (E05-S01) and shallow clone
//! (E05-S02).
//!
//! ## Full-clone protocol (E05-S01)
//! 1. `ListRefs`       — discover all refs on the remote.
//! 2. `NegotiatePull`  — send wants with an empty have list.
//! 3. `DownloadPack`   — receive every reachable object.
//!
//! ## Shallow-clone protocol (E05-S02)
//! 1. `CloneInit(depth)` — server prepares a depth-limited pack session.
//! 2. `CloneDownload`    — stream the objects for that session.
//!
//! ## Unshallow (E05-S02)
//! Uses `NegotiatePull` with the shallow boundary version IDs as the `want`
//! set (empty `have`) to fetch only the missing ancestors.

use std::collections::{HashSet, VecDeque};

use futures_util::StreamExt;
use tonic::{transport::Channel, Request};

use crate::error::{CasError, CasResult};
use crate::model::{NodeKind, Ref, RefKind, VersionId};
use crate::store::traits::{
    ChunkStore, NodeStore, PathIndexRepo, RefRepo, SyncDirection, SyncSessionRepo, TreeStore,
    VersionRepo,
};
use crate::sync::client_auth::ClientAuth;
use crate::sync::proto::{
    CloneDownloadRequest, CloneInitRequest, DownloadPackRequest, ListRefsRequest,
    NegotiatePullRequest, WantEntry,
    clone_download_response::Payload as ClonePayload,
    download_pack_response::Payload,
    sync_service_client::SyncServiceClient,
};
use crate::sync::session::{begin_session, complete_session, fail_session};
use crate::sync::shallow::{ShallowBoundary, collect_versions_shallow};

// ─── Proto ObjectType constants ──────────────────────────────────────────────
const PROTO_TYPE_CHUNK: i32 = 1;
const PROTO_TYPE_TREE_NODE: i32 = 2;
const PROTO_TYPE_NODE_ENTRY: i32 = 3;
const PROTO_TYPE_VERSION: i32 = 4;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Stages emitted to the progress callback during a clone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloneStage {
    /// Fetching the remote's current refs.
    ListingRefs,
    /// Running pull negotiation with the server.
    Negotiating,
    /// Downloading objects; counts updated per object received.
    Downloading { objects_done: u64, objects_total: u64 },
    /// Building local refs and path index from received objects.
    Rebuilding,
    /// Clone complete.
    Done,
}

/// Progress snapshot delivered to the caller's callback.
#[derive(Clone, Debug)]
pub struct CloneProgress {
    pub stage: CloneStage,
    /// Cumulative compressed bytes received in the `DownloadPack` stream.
    pub bytes_downloaded: u64,
}

/// Configuration passed to [`CloneClient::clone`].
pub struct CloneConfig {
    /// Short name of the remote (e.g. `"origin"`).
    pub remote_name: String,
    /// SaaS workspace identifier.
    pub workspace_id: String,
    /// SaaS tenant identifier.
    pub tenant_id: String,
}

/// Per-ref result returned by [`CloneClient::clone`].
#[derive(Clone, Debug)]
pub struct CloneResult {
    /// The ref name that was cloned.
    pub ref_name: String,
    /// The version ID the local branch now points at.
    pub tip: VersionId,
    /// Total objects received during this clone.
    pub objects_received: u64,
    /// Total compressed bytes downloaded.
    pub bytes_downloaded: u64,
}

// ─── CloneClient ──────────────────────────────────────────────────────────────

/// Executes the clone protocol against a remote sync server.
pub struct CloneClient {
    inner: SyncServiceClient<Channel>,
    auth: ClientAuth,
}

impl CloneClient {
    /// Connect to a gRPC sync server at `endpoint`.
    pub async fn connect(
        endpoint: impl Into<tonic::transport::Endpoint>,
    ) -> CasResult<Self> {
        Self::connect_authenticated(endpoint, None).await
    }

    /// Connect to a gRPC sync server, optionally attaching a bearer token to
    /// every request made by this client.
    pub async fn connect_authenticated(
        endpoint: impl Into<tonic::transport::Endpoint>,
        auth_token: Option<&str>,
    ) -> CasResult<Self> {
        let channel = endpoint
            .into()
            .connect()
            .await
            .map_err(|e| CasError::SyncError(format!("connect: {e}")))?;
        Ok(Self {
            inner: SyncServiceClient::new(channel),
            auth: ClientAuth::from_token(auth_token)?,
        })
    }

    /// Clone all (or a filtered set of) refs from the remote into local stores.
    ///
    /// # Parameters
    /// * `config`     — workspace/tenant/remote identity.
    /// * `ref_names`  — branch names to clone; pass an empty slice to clone all
    ///                  remote refs.
    /// * `refs`       — local ref repository (write: branches created on success).
    /// * `chunks / trees / nodes / versions` — local object stores (write).
    /// * `path_index` — if `Some`, the path index is rebuilt for every cloned
    ///                  version in the full history.
    /// * `sessions`   — if `Some`, the clone lifecycle is recorded in the
    ///                  sync-sessions table. Bookkeeping errors are swallowed so
    ///                  they cannot mask a real clone error.
    /// * `progress`   — called at each protocol step and after every object.
    #[allow(clippy::too_many_arguments)]
    pub async fn clone(
        &mut self,
        config: &CloneConfig,
        ref_names: &[String],
        refs: &dyn RefRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        path_index: Option<&dyn PathIndexRepo>,
        sessions: Option<&dyn SyncSessionRepo>,
        progress: impl FnMut(CloneProgress),
    ) -> CasResult<Vec<CloneResult>> {
        let session_id = new_session_id("clone");
        if let Some(repo) = sessions {
            let _ = begin_session(repo, &session_id, &config.remote_name, SyncDirection::Pull);
        }

        let outcome = self
            .clone_inner(
                config, ref_names, refs, chunks, trees, nodes, versions, path_index, progress,
            )
            .await;

        match (&outcome, sessions) {
            (Ok(results), Some(repo)) => {
                let total_objects: u64 = results.iter().map(|r| r.objects_received).sum();
                let total_bytes: u64 = results.iter().map(|r| r.bytes_downloaded).sum();
                let _ = complete_session(repo, &session_id, total_objects, total_bytes);
            }
            (Err(e), Some(repo)) => {
                let _ = fail_session(repo, &session_id, e.to_string());
            }
            _ => {}
        }

        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn clone_inner(
        &mut self,
        config: &CloneConfig,
        ref_names: &[String],
        refs: &dyn RefRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        path_index: Option<&dyn PathIndexRepo>,
        mut progress: impl FnMut(CloneProgress),
    ) -> CasResult<Vec<CloneResult>> {
        // ── Step 1: ListRefs ──────────────────────────────────────────────
        progress(CloneProgress {
            stage: CloneStage::ListingRefs,
            bytes_downloaded: 0,
        });

        let list_resp = self
            .inner
            .list_refs(self.auth.apply(Request::new(ListRefsRequest {
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
            })))
            .await
            .map_err(|e| CasError::SyncError(format!("list_refs: {e}")))?
            .into_inner();

        // Filter to requested ref names (or all if none specified).
        let remote_refs: Vec<(String, Vec<u8>)> = list_resp
            .refs
            .into_iter()
            .filter(|ri| ref_names.is_empty() || ref_names.contains(&ri.name))
            .filter_map(|ri| ri.target.map(|t| (ri.name, t.hash)))
            .collect();

        if remote_refs.is_empty() {
            progress(CloneProgress {
                stage: CloneStage::Done,
                bytes_downloaded: 0,
            });
            return Ok(Vec::new());
        }

        // ── Step 2: NegotiatePull with empty have (clone boundary) ────────
        progress(CloneProgress {
            stage: CloneStage::Negotiating,
            bytes_downloaded: 0,
        });

        // `have = None` — we hold nothing locally; server should send everything
        // reachable from the wanted tips.
        let wants: Vec<WantEntry> = remote_refs
            .iter()
            .map(|(name, hash)| WantEntry {
                ref_name: name.clone(),
                have: None,
                want: Some(crate::sync::proto::ObjectId { hash: hash.clone() }),
            })
            .collect();

        let neg_resp = self
            .inner
            .negotiate_pull(self.auth.apply(Request::new(NegotiatePullRequest {
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
                wants,
                have_filter: Vec::new(),
            })))
            .await
            .map_err(|e| CasError::SyncError(format!("negotiate_pull: {e}")))?
            .into_inner();

        let dl_session_id = neg_resp.session_id.clone();
        let total_objects = neg_resp.total_objects;

        // ── Step 3: DownloadPack ──────────────────────────────────────────
        let mut dl_stream = self
            .inner
            .download_pack(self.auth.apply(Request::new(DownloadPackRequest {
                session_id: dl_session_id,
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
            })))
            .await
            .map_err(|e| CasError::SyncError(format!("download_pack: {e}")))?
            .into_inner();

        let mut objects_done: u64 = 0;
        let mut bytes_downloaded: u64 = 0;

        while let Some(msg) = dl_stream.next().await {
            let msg = msg.map_err(|e| CasError::SyncError(format!("stream recv: {e}")))?;
            match msg.payload {
                Some(Payload::Header(_)) => {
                    // Header received — counts already known from NegotiatePull.
                }
                Some(Payload::Entry(entry)) => {
                    let Some(oid) = entry.hash else {
                        return Err(CasError::SyncError("pack entry missing hash".into()));
                    };
                    if oid.hash.len() != 32 {
                        return Err(CasError::SyncError(format!(
                            "hash must be 32 bytes, got {}",
                            oid.hash.len()
                        )));
                    }

                    bytes_downloaded += entry.data.len() as u64;

                    let raw = zstd::bulk::decompress(&entry.data, 64 * 1024 * 1024)
                        .map_err(|e| CasError::SyncError(format!("decompress: {e}")))?;

                    let arr: [u8; 32] = oid.hash.as_slice().try_into().unwrap();
                    let declared = blake3::Hash::from_bytes(arr);

                    store_object(
                        entry.r#type, declared, &raw, chunks, trees, nodes, versions,
                    )?;

                    objects_done += 1;
                    progress(CloneProgress {
                        stage: CloneStage::Downloading {
                            objects_done,
                            objects_total: total_objects,
                        },
                        bytes_downloaded,
                    });
                }
                None => {}
            }
        }

        // ── Step 4: Rebuild local refs and path index ─────────────────────
        progress(CloneProgress {
            stage: CloneStage::Rebuilding,
            bytes_downloaded,
        });

        for (ref_name, remote_hash) in &remote_refs {
            let tip = VersionId(hex::encode(remote_hash));

            // Create local branch ref pointing at the remote tip.
            refs.put_ref(&Ref {
                name: ref_name.clone(),
                kind: RefKind::Branch,
                target: tip.clone(),
            })?;

            // Rebuild path index for the full history reachable from this tip.
            if let Some(pi) = path_index {
                rebuild_path_index_history(&tip, versions, nodes, pi)?;
            }
        }

        progress(CloneProgress {
            stage: CloneStage::Done,
            bytes_downloaded,
        });

        // Build per-ref results. The object/byte counts are shared across all
        // refs (they were all downloaded in one DownloadPack session).
        let results = remote_refs
            .iter()
            .map(|(ref_name, _)| {
                let tip = refs
                    .get_ref(ref_name)
                    .ok()
                    .flatten()
                    .map(|r| r.target)
                    .unwrap_or_else(|| VersionId(String::new()));
                CloneResult {
                    ref_name: ref_name.clone(),
                    tip,
                    objects_received: objects_done,
                    bytes_downloaded,
                }
            })
            .collect();

        Ok(results)
    }

    // ── Shallow clone (E05-S02) ───────────────────────────────────────────

    /// Perform a depth-limited clone using the `CloneInit` / `CloneDownload`
    /// dedicated RPCs.
    ///
    /// # Parameters
    /// * `depth`      — number of version-history levels to fetch (≥ 1).
    ///                  Use [`CloneClient::clone`] for a full clone.
    /// * `ref_names`  — branch names to clone; empty slice clones all refs.
    /// * other params — same as [`CloneClient::clone`].
    ///
    /// # Returns
    /// `(results, boundary)` where `boundary` records the version IDs just
    /// below the fetched history (parents not fetched).
    #[allow(clippy::too_many_arguments)]
    pub async fn clone_shallow(
        &mut self,
        config: &CloneConfig,
        depth: u32,
        ref_names: &[String],
        refs: &dyn RefRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        path_index: Option<&dyn PathIndexRepo>,
        sessions: Option<&dyn SyncSessionRepo>,
        progress: impl FnMut(CloneProgress),
    ) -> CasResult<(Vec<CloneResult>, ShallowBoundary)> {
        let session_id = new_session_id("shallow");
        if let Some(repo) = sessions {
            let _ = begin_session(repo, &session_id, &config.remote_name, SyncDirection::Pull);
        }

        let outcome = self
            .clone_shallow_inner(
                config, depth, ref_names, refs, chunks, trees, nodes, versions,
                path_index, progress,
            )
            .await;

        match (&outcome, sessions) {
            (Ok((results, _)), Some(repo)) => {
                let total_objects: u64 = results.iter().map(|r| r.objects_received).sum();
                let total_bytes: u64 = results.iter().map(|r| r.bytes_downloaded).sum();
                let _ = complete_session(repo, &session_id, total_objects, total_bytes);
            }
            (Err(e), Some(repo)) => {
                let _ = fail_session(repo, &session_id, e.to_string());
            }
            _ => {}
        }

        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn clone_shallow_inner(
        &mut self,
        config: &CloneConfig,
        depth: u32,
        ref_names: &[String],
        refs: &dyn RefRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        path_index: Option<&dyn PathIndexRepo>,
        mut progress: impl FnMut(CloneProgress),
    ) -> CasResult<(Vec<CloneResult>, ShallowBoundary)> {
        // ── Step 1: CloneInit ─────────────────────────────────────────────
        progress(CloneProgress {
            stage: CloneStage::Negotiating,
            bytes_downloaded: 0,
        });

        let init_resp = self
            .inner
            .clone_init(self.auth.apply(Request::new(CloneInitRequest {
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
                depth: Some(depth),
                refs: ref_names.to_vec(),
            })))
            .await
            .map_err(|e| CasError::SyncError(format!("clone_init: {e}")))?
            .into_inner();

        let dl_session_id = init_resp.session_id.clone();
        let total_objects = init_resp.total_objects;

        // Collect remote ref name → tip hash from the response.
        let remote_refs: Vec<(String, Vec<u8>)> = init_resp
            .refs
            .into_iter()
            .filter(|ri| ref_names.is_empty() || ref_names.contains(&ri.name))
            .filter_map(|ri| ri.target.map(|t| (ri.name, t.hash)))
            .collect();

        if remote_refs.is_empty() {
            progress(CloneProgress {
                stage: CloneStage::Done,
                bytes_downloaded: 0,
            });
            return Ok((Vec::new(), ShallowBoundary::new()));
        }

        // ── Step 2: CloneDownload ─────────────────────────────────────────
        let mut dl_stream = self
            .inner
            .clone_download(self.auth.apply(Request::new(CloneDownloadRequest {
                session_id: dl_session_id,
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
            })))
            .await
            .map_err(|e| CasError::SyncError(format!("clone_download: {e}")))?
            .into_inner();

        let mut objects_done: u64 = 0;
        let mut bytes_downloaded: u64 = 0;
        // Track which version IDs were actually received so we can compute
        // the boundary from the client side.
        let mut received_vids: HashSet<VersionId> = HashSet::new();

        while let Some(msg) = dl_stream.next().await {
            let msg = msg.map_err(|e| CasError::SyncError(format!("stream recv: {e}")))?;
            match msg.payload {
                Some(ClonePayload::Header(_)) => {}
                Some(ClonePayload::Entry(entry)) => {
                    let Some(oid) = entry.hash else {
                        return Err(CasError::SyncError("pack entry missing hash".into()));
                    };
                    if oid.hash.len() != 32 {
                        return Err(CasError::SyncError(format!(
                            "hash must be 32 bytes, got {}",
                            oid.hash.len()
                        )));
                    }

                    bytes_downloaded += entry.data.len() as u64;

                    let raw = zstd::bulk::decompress(&entry.data, 64 * 1024 * 1024)
                        .map_err(|e| CasError::SyncError(format!("decompress: {e}")))?;

                    let arr: [u8; 32] = oid.hash.as_slice().try_into().unwrap();
                    let declared = blake3::Hash::from_bytes(arr);

                    // Track received version IDs for boundary computation.
                    if entry.r#type == 4 {
                        // PROTO_TYPE_VERSION = 4
                        received_vids.insert(VersionId(hex::encode(arr)));
                    }

                    store_object(
                        entry.r#type, declared, &raw, chunks, trees, nodes, versions,
                    )?;

                    objects_done += 1;
                    progress(CloneProgress {
                        stage: CloneStage::Downloading {
                            objects_done,
                            objects_total: total_objects,
                        },
                        bytes_downloaded,
                    });
                }
                None => {}
            }
        }

        // ── Step 3: Rebuild local refs and path index ─────────────────────
        progress(CloneProgress {
            stage: CloneStage::Rebuilding,
            bytes_downloaded,
        });

        for (ref_name, remote_hash) in &remote_refs {
            let tip = VersionId(hex::encode(remote_hash));
            refs.put_ref(&Ref {
                name: ref_name.clone(),
                kind: RefKind::Branch,
                target: tip.clone(),
            })?;
            // Rebuild path index only for fetched versions (not full history).
            if let Some(pi) = path_index {
                for vid in &received_vids {
                    let Some(vnode) = versions.get_version(vid)? else { continue };
                    rebuild_path_index_for_version(vid, &vnode.root, nodes, pi)?;
                }
            }
        }

        // ── Step 4: Compute shallow boundary ─────────────────────────────
        // For each received version, if any of its parents are not in the
        // received set, those parents are boundary versions.
        let mut boundary = ShallowBoundary::new();
        for vid in &received_vids {
            if let Some(v) = versions.get_version(vid)? {
                for parent in &v.parents {
                    if !received_vids.contains(parent) {
                        boundary.add(parent.clone());
                    }
                }
            }
        }

        progress(CloneProgress {
            stage: CloneStage::Done,
            bytes_downloaded,
        });

        let results = remote_refs
            .iter()
            .map(|(ref_name, _)| {
                let tip = refs
                    .get_ref(ref_name)
                    .ok()
                    .flatten()
                    .map(|r| r.target)
                    .unwrap_or_else(|| VersionId(String::new()));
                CloneResult {
                    ref_name: ref_name.clone(),
                    tip,
                    objects_received: objects_done,
                    bytes_downloaded,
                }
            })
            .collect();

        Ok((results, boundary))
    }

    // ── Unshallow (E05-S02) ──────────────────────────────────────────────

    /// Convert a shallow clone into a full clone by fetching all missing
    /// ancestors from the remote.
    ///
    /// Uses the versions in `boundary` as `want` tips (with empty `have`),
    /// so only the ancestry below the current shallow boundary is transferred
    /// — no objects the client already has are re-downloaded.
    ///
    /// After this call the `ShallowBoundary` should be discarded (cleared)
    /// by the caller.
    #[allow(clippy::too_many_arguments)]
    pub async fn unshallow(
        &mut self,
        config: &CloneConfig,
        boundary: &ShallowBoundary,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        path_index: Option<&dyn PathIndexRepo>,
        progress: impl FnMut(CloneProgress),
    ) -> CasResult<u64> {
        if boundary.is_empty() {
            return Ok(0);
        }

        // Build one WantEntry per boundary version (no `have` — these are
        // exactly the versions we do not yet have).
        let wants: Vec<WantEntry> = boundary
            .iter()
            .filter_map(|vid| {
                hex::decode(vid.as_str()).ok().and_then(|bytes| {
                    if bytes.len() == 32 {
                        Some(WantEntry {
                            ref_name: String::new(),
                            have: None,
                            want: Some(crate::sync::proto::ObjectId { hash: bytes }),
                        })
                    } else {
                        None
                    }
                })
            })
            .collect();

        self.download_objects(config, wants, versions, chunks, trees, nodes, path_index, progress)
            .await
    }

    /// Internal helper: NegotiatePull(wants) → DownloadPack → store + rebuild.
    #[allow(clippy::too_many_arguments)]
    async fn download_objects(
        &mut self,
        config: &CloneConfig,
        wants: Vec<WantEntry>,
        versions: &dyn VersionRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        path_index: Option<&dyn PathIndexRepo>,
        mut progress: impl FnMut(CloneProgress),
    ) -> CasResult<u64> {
        if wants.is_empty() {
            return Ok(0);
        }

        progress(CloneProgress {
            stage: CloneStage::Negotiating,
            bytes_downloaded: 0,
        });

        let neg_resp = self
            .inner
            .negotiate_pull(self.auth.apply(Request::new(NegotiatePullRequest {
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
                wants,
                have_filter: Vec::new(),
            })))
            .await
            .map_err(|e| CasError::SyncError(format!("negotiate_pull: {e}")))?
            .into_inner();

        let total_objects = neg_resp.total_objects;
        let mut dl_stream = self
            .inner
            .download_pack(self.auth.apply(Request::new(DownloadPackRequest {
                session_id: neg_resp.session_id,
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
            })))
            .await
            .map_err(|e| CasError::SyncError(format!("download_pack: {e}")))?
            .into_inner();

        let mut objects_done: u64 = 0;
        let mut bytes_downloaded: u64 = 0;
        let mut received_vids: HashSet<VersionId> = HashSet::new();

        while let Some(msg) = dl_stream.next().await {
            let msg = msg.map_err(|e| CasError::SyncError(format!("stream recv: {e}")))?;
            match msg.payload {
                Some(Payload::Header(_)) => {}
                Some(Payload::Entry(entry)) => {
                    let Some(oid) = entry.hash else {
                        return Err(CasError::SyncError("pack entry missing hash".into()));
                    };
                    if oid.hash.len() != 32 {
                        return Err(CasError::SyncError(format!(
                            "hash must be 32 bytes, got {}",
                            oid.hash.len()
                        )));
                    }

                    bytes_downloaded += entry.data.len() as u64;

                    let raw = zstd::bulk::decompress(&entry.data, 64 * 1024 * 1024)
                        .map_err(|e| CasError::SyncError(format!("decompress: {e}")))?;

                    let arr: [u8; 32] = oid.hash.as_slice().try_into().unwrap();
                    let declared = blake3::Hash::from_bytes(arr);

                    if entry.r#type == 4 {
                        received_vids.insert(VersionId(hex::encode(arr)));
                    }

                    store_object(
                        entry.r#type, declared, &raw, chunks, trees, nodes, versions,
                    )?;

                    objects_done += 1;
                    progress(CloneProgress {
                        stage: CloneStage::Downloading {
                            objects_done,
                            objects_total: total_objects,
                        },
                        bytes_downloaded,
                    });
                }
                None => {}
            }
        }

        // Rebuild path index for newly fetched versions.
        if let Some(pi) = path_index {
            for vid in &received_vids {
                if let Some(vnode) = versions.get_version(vid)? {
                    rebuild_path_index_for_version(vid, &vnode.root, nodes, pi)?;
                }
            }
        }

        progress(CloneProgress {
            stage: CloneStage::Done,
            bytes_downloaded,
        });

        Ok(objects_done)
    }
}

// ─── Session ID ───────────────────────────────────────────────────────────────

fn new_session_id(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let input = format!("{nanos}{seq}");
    let hash = blake3::hash(input.as_bytes());
    format!("{prefix}-{}", &hex::encode(hash.as_bytes())[..16])
}

// ─── Object storage ───────────────────────────────────────────────────────────

/// Store a single pack entry into the appropriate local store.
///
/// Verifies the BLAKE3 hash for content chunks.
fn store_object(
    proto_type: i32,
    declared: blake3::Hash,
    raw: &[u8],
    chunks: &dyn ChunkStore,
    trees: &dyn TreeStore,
    nodes: &dyn NodeStore,
    versions: &dyn VersionRepo,
) -> CasResult<()> {
    use crate::model::{TreeNode, VersionNode};

    match proto_type {
        PROTO_TYPE_CHUNK => {
            let actual = blake3::hash(raw);
            if actual != declared {
                return Err(CasError::SyncError(format!(
                    "chunk hash mismatch: expected {declared} got {actual}"
                )));
            }
            chunks.put_chunk(raw.to_vec())?;
        }
        PROTO_TYPE_TREE_NODE => {
            let node: TreeNode = serde_json::from_slice(raw)
                .map_err(|e| CasError::SyncError(format!("tree node JSON: {e}")))?;
            trees.put_tree_node(&node)?;
        }
        PROTO_TYPE_NODE_ENTRY => {
            let entry: crate::model::NodeEntry = serde_json::from_slice(raw)
                .map_err(|e| CasError::SyncError(format!("node entry JSON: {e}")))?;
            nodes.put_node(&entry)?;
        }
        PROTO_TYPE_VERSION => {
            let v: VersionNode = serde_json::from_slice(raw)
                .map_err(|e| CasError::SyncError(format!("version JSON: {e}")))?;
            versions.put_version(&v)?;
        }
        other => {
            return Err(CasError::SyncError(format!(
                "unknown object type {other}"
            )));
        }
    }
    Ok(())
}

// ─── Path index rebuild ───────────────────────────────────────────────────────

/// Walk all versions reachable from `tip` (BFS through parent links) and
/// rebuild the path index for each.
fn rebuild_path_index_history(
    tip: &VersionId,
    versions: &dyn VersionRepo,
    nodes: &dyn NodeStore,
    path_index: &dyn PathIndexRepo,
) -> CasResult<()> {
    let mut queue: VecDeque<VersionId> = VecDeque::new();
    let mut visited: std::collections::HashSet<VersionId> = std::collections::HashSet::new();
    queue.push_back(tip.clone());

    while let Some(vid) = queue.pop_front() {
        if !visited.insert(vid.clone()) {
            continue;
        }
        let Some(vnode) = versions.get_version(&vid)? else {
            continue;
        };
        rebuild_path_index_for_version(&vid, &vnode.root, nodes, path_index)?;
        for parent in &vnode.parents {
            if !visited.contains(parent) {
                queue.push_back(parent.clone());
            }
        }
    }

    Ok(())
}

/// Rebuild the path index for a single version by recursively walking its
/// NodeEntry tree.
fn rebuild_path_index_for_version(
    version_id: &VersionId,
    root_hash: &crate::model::ChunkHash,
    nodes: &dyn NodeStore,
    path_index: &dyn PathIndexRepo,
) -> CasResult<()> {
    // Index the root (empty-string path = repo root directory).
    path_index.put_path_entry(version_id, "", root_hash, true)?;

    // Walk the tree.
    walk_node(version_id, root_hash, "", nodes, path_index)
}

/// Recursively walk a NodeEntry subtree, indexing every child.
fn walk_node(
    version_id: &VersionId,
    node_hash: &crate::model::ChunkHash,
    prefix: &str,
    nodes: &dyn NodeStore,
    path_index: &dyn PathIndexRepo,
) -> CasResult<()> {
    let Some(entry) = nodes.get_node(node_hash)? else {
        return Ok(()); // missing node — skip gracefully during rebuild
    };

    match &entry.kind {
        NodeKind::File { .. } => {
            // Leaf file — already indexed by the caller; nothing to recurse into.
        }
        NodeKind::Directory { children } => {
            for (name, child_hash) in children {
                let child_path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };

                let is_dir = nodes
                    .get_node(child_hash)?
                    .map(|e| e.is_directory())
                    .unwrap_or(false);

                path_index.put_path_entry(version_id, &child_path, child_hash, is_dir)?;

                if is_dir {
                    walk_node(version_id, child_hash, &child_path, nodes, path_index)?;
                }
            }
        }
    }

    Ok(())
}
