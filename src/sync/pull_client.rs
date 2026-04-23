//! Pull client — three-step gRPC pull protocol (E04-S03).
//!
//! ## Protocol
//! 1. `ListRefs`       — fetch remote ref tips to know what is available.
//! 2. `NegotiatePull`  — send want/have list; server returns session + object count.
//! 3. `DownloadPack`   — server-streaming: first msg is header, rest are entries.
//!
//! After a successful download:
//! - Remote-tracking refs are updated for every fetched ref.
//! - Local branches are **fast-forwarded** when the current local tip is an
//!   ancestor of the remote tip.  When it is not, only the remote-tracking ref
//!   is updated and the caller receives a `PullResult` with
//!   `fast_forward = false` for that ref (user must merge/rebase manually).

use futures_util::StreamExt;
use tonic::{transport::Channel, Request};

use crate::error::{CasError, CasResult};
use crate::model::{NodeEntry, Ref, RefKind, TreeNode, VersionId, VersionNode, current_timestamp};
use crate::store::traits::{
    ChunkStore, NodeStore, RefRepo, RemoteTrackingRepo, SyncDirection, SyncSessionRepo,
    TreeStore, VersionRepo,
};
use crate::sync::proto::{
    DownloadPackRequest, ListRefsRequest, NegotiatePullRequest, WantEntry,
    download_pack_response::Payload,
    sync_service_client::SyncServiceClient,
};
use crate::sync::remote_refs::RemoteRef;
use crate::sync::session::{begin_session, complete_session, fail_session};

// ─── Proto ObjectType constants ──────────────────────────────────────────────
const PROTO_TYPE_CHUNK: i32 = 1;
const PROTO_TYPE_TREE_NODE: i32 = 2;
const PROTO_TYPE_NODE_ENTRY: i32 = 3;
const PROTO_TYPE_VERSION: i32 = 4;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Stages emitted to the progress callback during a pull.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PullStage {
    /// Fetching the remote's current refs.
    ListingRefs,
    /// Running pull negotiation with the server.
    Negotiating,
    /// Downloading objects; counts updated per object.
    Downloading { objects_done: u64, objects_total: u64 },
    /// Pull complete.
    Done,
}

/// Progress snapshot delivered to the caller's callback.
#[derive(Clone, Debug)]
pub struct PullProgress {
    pub stage: PullStage,
    /// Cumulative compressed bytes received in the `DownloadPack` stream.
    pub bytes_downloaded: u64,
}

/// Configuration passed to [`PullClient::pull`].
pub struct PullConfig {
    /// Short name of the remote (e.g. `"origin"`).
    pub remote_name: String,
    /// SaaS workspace identifier.
    pub workspace_id: String,
    /// SaaS tenant identifier.
    pub tenant_id: String,
}

/// Per-ref result returned by [`PullClient::pull`].
#[derive(Clone, Debug)]
pub struct PullResult {
    /// The ref name that was pulled.
    pub ref_name: String,
    /// The remote tip after the pull.
    pub remote_tip: VersionId,
    /// `true` when the local branch was fast-forwarded to `remote_tip`.
    /// `false` when the local branch is diverged — only the remote-tracking
    /// ref was updated; the user must merge or rebase manually.
    pub fast_forwarded: bool,
}

// ─── PullClient ───────────────────────────────────────────────────────────────

/// Executes the three-step gRPC pull protocol against a remote sync server.
pub struct PullClient {
    inner: SyncServiceClient<Channel>,
}

impl PullClient {
    /// Connect to a gRPC sync server at `endpoint`.
    pub async fn connect(
        endpoint: impl Into<tonic::transport::Endpoint>,
    ) -> CasResult<Self> {
        let channel = endpoint
            .into()
            .connect()
            .await
            .map_err(|e| CasError::SyncError(format!("connect: {e}")))?;
        Ok(Self {
            inner: SyncServiceClient::new(channel),
        })
    }

    /// Execute a full pull for the given `ref_names`.
    ///
    /// # Parameters
    /// * `config`    — workspace/tenant/remote identity.
    /// * `ref_names` — branch names to pull; pass an empty slice to pull all
    ///                 remote refs.
    /// * `refs`      — local ref repository (read + write for fast-forward).
    /// * `tracking`  — remote-tracking ref repository (updated on success).
    /// * `chunks / trees / nodes / versions` — local object stores (write).
    /// * `sessions`  — if `Some`, the pull lifecycle is recorded in the
    ///                 sync-sessions table (begin/complete/fail). Bookkeeping
    ///                 errors are intentionally swallowed so they cannot mask
    ///                 a real pull error.
    /// * `progress`  — called at each protocol step and after every object.
    ///
    /// # Returns
    /// A `Vec<PullResult>` with one entry per fetched ref.
    #[allow(clippy::too_many_arguments)]
    pub async fn pull(
        &mut self,
        config: &PullConfig,
        ref_names: &[String],
        refs: &dyn RefRepo,
        tracking: &dyn RemoteTrackingRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        sessions: Option<&dyn SyncSessionRepo>,
        progress: impl FnMut(PullProgress),
    ) -> CasResult<Vec<PullResult>> {
        let session_id = new_session_id("pull");
        if let Some(repo) = sessions {
            let _ = begin_session(
                repo,
                &session_id,
                &config.remote_name,
                SyncDirection::Pull,
            );
        }

        let outcome = self
            .pull_inner(
                config, ref_names, refs, tracking, chunks, trees, nodes, versions, progress,
            )
            .await;

        match (&outcome, sessions) {
            (Ok((_, objs, bytes)), Some(repo)) => {
                let _ = complete_session(repo, &session_id, *objs, *bytes);
            }
            (Err(e), Some(repo)) => {
                let _ = fail_session(repo, &session_id, e.to_string());
            }
            _ => {}
        }

        outcome.map(|(results, _, _)| results)
    }

    /// Internal pull body. Returns `(per-ref results, total objects downloaded, bytes downloaded)`.
    #[allow(clippy::too_many_arguments)]
    async fn pull_inner(
        &mut self,
        config: &PullConfig,
        ref_names: &[String],
        refs: &dyn RefRepo,
        tracking: &dyn RemoteTrackingRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        mut progress: impl FnMut(PullProgress),
    ) -> CasResult<(Vec<PullResult>, u64, u64)> {
        // ── Step 1: ListRefs ──────────────────────────────────────────────
        progress(PullProgress {
            stage: PullStage::ListingRefs,
            bytes_downloaded: 0,
        });

        let list_resp = self
            .inner
            .list_refs(Request::new(ListRefsRequest {
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
            }))
            .await
            .map_err(|e| CasError::SyncError(format!("list_refs: {e}")))?
            .into_inner();

        // Filter to requested refs (or all refs if none specified).
        let remote_refs: Vec<_> = list_resp
            .refs
            .into_iter()
            .filter(|ri| ref_names.is_empty() || ref_names.contains(&ri.name))
            .filter_map(|ri| ri.target.map(|t| (ri.name, t.hash)))
            .collect();

        if remote_refs.is_empty() {
            progress(PullProgress {
                stage: PullStage::Done,
                bytes_downloaded: 0,
            });
            return Ok((Vec::new(), 0, 0));
        }

        // ── Build want/have list ──────────────────────────────────────────
        let wants: Vec<WantEntry> = remote_refs
            .iter()
            .map(|(name, remote_hash)| {
                // "have" = local tip of the remote-tracking ref for this branch.
                let have_hash: Vec<u8> = tracking
                    .get_remote_ref(&config.remote_name, name)
                    .ok()
                    .flatten()
                    .map(|rr| {
                        hex::decode(rr.target.as_str()).unwrap_or_default()
                    })
                    .unwrap_or_default();

                WantEntry {
                    ref_name: name.clone(),
                    have: Some(crate::sync::proto::ObjectId { hash: have_hash }),
                    want: Some(crate::sync::proto::ObjectId { hash: remote_hash.clone() }),
                }
            })
            .collect();

        // ── Step 2: NegotiatePull ─────────────────────────────────────────
        progress(PullProgress {
            stage: PullStage::Negotiating,
            bytes_downloaded: 0,
        });

        let neg_resp = self
            .inner
            .negotiate_pull(Request::new(NegotiatePullRequest {
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
                wants,
                have_filter: Vec::new(), // Bloom filter optional — not used yet
            }))
            .await
            .map_err(|e| CasError::SyncError(format!("negotiate_pull: {e}")))?
            .into_inner();

        let session_id = neg_resp.session_id.clone();
        let total_objects = neg_resp.total_objects;

        // ── Step 3: DownloadPack ──────────────────────────────────────────
        let mut dl_stream = self
            .inner
            .download_pack(Request::new(DownloadPackRequest {
                session_id: session_id.clone(),
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
            }))
            .await
            .map_err(|e| CasError::SyncError(format!("download_pack: {e}")))?
            .into_inner();

        let mut objects_done: u64 = 0;
        let mut bytes_downloaded: u64 = 0;

        while let Some(msg) = dl_stream.next().await {
            let msg = msg.map_err(|e| CasError::SyncError(format!("stream recv: {e}")))?;
            match msg.payload {
                Some(Payload::Header(_hdr)) => {
                    // Header received — nothing to store, counts already known.
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

                    // Decompress.
                    let raw = zstd::bulk::decompress(&entry.data, 64 * 1024 * 1024)
                        .map_err(|e| CasError::SyncError(format!("decompress: {e}")))?;

                    let arr: [u8; 32] = oid.hash.as_slice().try_into().unwrap();
                    let declared = blake3::Hash::from_bytes(arr);

                    store_object(entry.r#type, declared, &raw, chunks, trees, nodes, versions)?;

                    objects_done += 1;
                    progress(PullProgress {
                        stage: PullStage::Downloading {
                            objects_done,
                            objects_total: total_objects,
                        },
                        bytes_downloaded,
                    });
                }
                None => {}
            }
        }

        // ── Update remote-tracking refs & attempt fast-forward ────────────
        let now = current_timestamp();
        let mut results = Vec::new();

        for (ref_name, remote_hash) in &remote_refs {
            let remote_vid = VersionId(hex::encode(remote_hash));

            // Update remote-tracking ref.
            let _ = tracking.update_remote_ref(&RemoteRef {
                remote_name: config.remote_name.clone(),
                ref_name: ref_name.clone(),
                kind: RefKind::Branch,
                target: remote_vid.clone(),
                updated_at: now,
            });

            // Attempt fast-forward of local branch.
            let ff = try_fast_forward(ref_name, &remote_vid, refs, versions)?;
            results.push(PullResult {
                ref_name: ref_name.clone(),
                remote_tip: remote_vid,
                fast_forwarded: ff,
            });
        }

        progress(PullProgress {
            stage: PullStage::Done,
            bytes_downloaded,
        });

        Ok((results, objects_done, bytes_downloaded))
    }
}

/// Generate a unique session id with the given short prefix.
fn new_session_id(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos:x}")
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

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
            let entry: NodeEntry = serde_json::from_slice(raw)
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

/// Attempt to fast-forward a local branch to `new_tip`.
///
/// Returns `true` if the branch was fast-forwarded, `false` if it is diverged
/// (only the remote-tracking ref should be updated in that case).
///
/// If the local branch does not yet exist, it is created and `true` is returned.
fn try_fast_forward(
    ref_name: &str,
    new_tip: &VersionId,
    refs: &dyn RefRepo,
    versions: &dyn VersionRepo,
) -> CasResult<bool> {
    let local = refs.get_ref(ref_name)?;

    let is_ff = match &local {
        None => true, // creating for the first time
        Some(r) => {
            if r.target == *new_tip {
                return Ok(true); // already up-to-date
            }
            crate::sync::fast_forward::is_fast_forward(&r.target, new_tip, versions)?
        }
    };

    if is_ff {
        refs.put_ref(&Ref {
            name: ref_name.to_owned(),
            kind: RefKind::Branch,
            target: new_tip.clone(),
        })?;
        Ok(true)
    } else {
        Ok(false)
    }
}
