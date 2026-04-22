//! Push client — four-step gRPC push protocol (E04-S01).
//!
//! ## Protocol
//! 1. `ListRefs`       — fetch remote ref tips
//! 2. `NegotiatePush`  — send local refs + want-versions; get needed-objects list
//! 3. `UploadPack`     — client-streaming: header then one entry per object
//! 4. `FinalizeRefs`   — request the server to advance its refs
//!
//! After a successful `FinalizeRefs`, local remote-tracking refs are updated.

use std::collections::HashMap;

use futures_util::stream;
use tonic::{transport::Channel, Request};

use crate::error::{CasError, CasResult};
use crate::model::{ChunkHash, RefKind, VersionId, current_timestamp};
use crate::store::traits::{ChunkStore, NodeStore, RefRepo, RemoteTrackingRepo, TreeStore, VersionRepo};
use crate::sync::proto::{
    FinalizeRefsRequest, ListRefsRequest, NegotiatePushRequest, ObjectId,
    PackEntry, PackHeader, RefUpdate, UploadPackRequest,
    sync_service_client::SyncServiceClient,
    upload_pack_request::Payload,
};
use crate::sync::remote_refs::RemoteRef;

// ─── Proto ObjectType constants ──────────────────────────────────────────────
// Mirrors the proto enum values without importing the prost enum directly.
const PROTO_TYPE_CHUNK: i32 = 1;
const PROTO_TYPE_TREE_NODE: i32 = 2;
const PROTO_TYPE_NODE_ENTRY: i32 = 3;
const PROTO_TYPE_VERSION: i32 = 4;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Stages emitted to the progress callback during a push.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushStage {
    /// Fetching the remote's current refs.
    ListingRefs,
    /// Running push negotiation with the server.
    Negotiating,
    /// Uploading objects; counts are updated per object.
    Uploading { objects_done: u64, objects_total: u64 },
    /// Asking the server to advance its refs.
    Finalizing,
    /// Push complete.
    Done,
}

/// Progress snapshot delivered to the caller's callback.
#[derive(Clone, Debug)]
pub struct SyncProgress {
    pub stage: PushStage,
    /// Cumulative compressed bytes sent in the `UploadPack` stream.
    pub bytes_uploaded: u64,
}

/// Configuration passed to [`PushClient::push`].
pub struct PushConfig {
    /// Short name of the remote (e.g. `"origin"`).
    ///
    /// Used to update local remote-tracking refs after a successful push.
    pub remote_name: String,
    /// SaaS workspace identifier.
    pub workspace_id: String,
    /// SaaS tenant identifier.
    pub tenant_id: String,
    /// If `true`, allow non-fast-forward ref updates (force-push).
    pub force: bool,
}

// ─── PushClient ───────────────────────────────────────────────────────────────

/// Executes the four-step gRPC push protocol against a remote sync server.
pub struct PushClient {
    inner: SyncServiceClient<Channel>,
}

impl PushClient {
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

    /// Execute a full push.
    ///
    /// # Parameters
    /// * `config`     — workspace/tenant/remote identity and force flag.
    /// * `ref_names`  — branch names to push; pass an empty slice for all local
    ///                  branches.
    /// * `refs`       — local ref repository (read-only).
    /// * `chunks / trees / nodes / versions` — object stores for fetching data.
    /// * `tracking`   — if `Some`, remote-tracking refs are updated in-place
    ///                  for every ref that the server reports as `"ok"`.
    /// * `progress`   — called at each protocol step and after every object.
    ///
    /// # Returns
    /// A map of `ref_name → status` string (`"ok"`, `"non-fast-forward"`,
    /// or `"error"`).
    pub async fn push(
        &mut self,
        config: &PushConfig,
        ref_names: &[String],
        refs: &dyn RefRepo,
        chunks: &dyn ChunkStore,
        trees: &dyn TreeStore,
        nodes: &dyn NodeStore,
        versions: &dyn VersionRepo,
        tracking: Option<&dyn RemoteTrackingRepo>,
        mut progress: impl FnMut(SyncProgress),
    ) -> CasResult<HashMap<String, String>> {
        // ── Step 1: ListRefs ──────────────────────────────────────────────
        progress(SyncProgress {
            stage: PushStage::ListingRefs,
            bytes_uploaded: 0,
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

        // Build: remote_name → target_bytes (32 B)
        let remote_tips: HashMap<String, Vec<u8>> = list_resp
            .refs
            .into_iter()
            .filter_map(|ri| ri.target.map(|t| (ri.name, t.hash)))
            .collect();

        // ── Collect local refs to push ────────────────────────────────────
        let all_local = refs.list_refs(Some(RefKind::Branch))?;
        let to_push: Vec<_> = if ref_names.is_empty() {
            all_local
        } else {
            all_local
                .into_iter()
                .filter(|r| ref_names.contains(&r.name))
                .collect()
        };

        if to_push.is_empty() {
            progress(SyncProgress {
                stage: PushStage::Done,
                bytes_uploaded: 0,
            });
            return Ok(HashMap::new());
        }

        let ref_updates: Vec<RefUpdate> = to_push
            .iter()
            .map(|r| RefUpdate {
                ref_name: r.name.clone(),
                old_target: Some(ObjectId {
                    hash: remote_tips
                        .get(&r.name)
                        .cloned()
                        .unwrap_or_default(),
                }),
                new_target: Some(ObjectId {
                    hash: version_id_to_bytes(&r.target),
                }),
                force: config.force,
            })
            .collect();

        // Only the ref tips are sent as "local versions" — the server uses
        // these roots to compute which objects it is missing.
        let local_versions: Vec<ObjectId> = to_push
            .iter()
            .map(|r| ObjectId {
                hash: version_id_to_bytes(&r.target),
            })
            .collect();

        // ── Step 2: NegotiatePush ─────────────────────────────────────────
        progress(SyncProgress {
            stage: PushStage::Negotiating,
            bytes_uploaded: 0,
        });

        let neg_resp = self
            .inner
            .negotiate_push(Request::new(NegotiatePushRequest {
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
                ref_updates: ref_updates.clone(),
                local_versions,
            }))
            .await
            .map_err(|e| CasError::SyncError(format!("negotiate_push: {e}")))?
            .into_inner();

        if !neg_resp.accepted {
            let reason = neg_resp.reject_reason;
            let lower = reason.to_lowercase();
            if lower.contains("non-fast-forward") || lower.contains("fast-forward") {
                return Err(CasError::NonFastForward(reason));
            }
            return Err(CasError::SyncError(format!("push rejected: {reason}")));
        }

        let session_id = neg_resp.session_id.clone();
        let total_objects: u64 = neg_resp
            .need_objects
            .iter()
            .map(|l| l.hashes.len() as u64)
            .sum();

        // ── Step 3: UploadPack ────────────────────────────────────────────
        // Build the full message list up front (header + one message per object),
        // then send as a single client-streaming RPC call.
        let mut messages: Vec<UploadPackRequest> =
            Vec::with_capacity(total_objects as usize + 1);

        // First message: pack header.
        messages.push(UploadPackRequest {
            payload: Some(Payload::Header(PackHeader {
                object_count: total_objects,
                estimated_bytes: 0,
                session_id: session_id.clone(),
            })),
        });

        let mut objects_done: u64 = 0;
        let mut bytes_uploaded: u64 = 0;

        for obj_list in &neg_resp.need_objects {
            let proto_type = obj_list.r#type;
            for oid in &obj_list.hashes {
                let raw = fetch_object(proto_type, &oid.hash, chunks, trees, nodes, versions)?;
                let compressed = zstd::bulk::compress(&raw, 3)
                    .map_err(|e| CasError::SyncError(format!("zstd compress: {e}")))?;

                bytes_uploaded += compressed.len() as u64;
                objects_done += 1;

                messages.push(UploadPackRequest {
                    payload: Some(Payload::Entry(PackEntry {
                        r#type: proto_type,
                        hash: Some(ObjectId { hash: oid.hash.clone() }),
                        data: compressed,
                    })),
                });

                progress(SyncProgress {
                    stage: PushStage::Uploading {
                        objects_done,
                        objects_total: total_objects,
                    },
                    bytes_uploaded,
                });
            }
        }

        self.inner
            .upload_pack(Request::new(stream::iter(messages)))
            .await
            .map_err(|e| CasError::SyncError(format!("upload_pack: {e}")))?;

        // ── Step 4: FinalizeRefs ──────────────────────────────────────────
        progress(SyncProgress {
            stage: PushStage::Finalizing,
            bytes_uploaded,
        });

        let fin_resp = self
            .inner
            .finalize_refs(Request::new(FinalizeRefsRequest {
                session_id,
                workspace_id: config.workspace_id.clone(),
                tenant_id: config.tenant_id.clone(),
                ref_updates,
            }))
            .await
            .map_err(|e| CasError::SyncError(format!("finalize_refs: {e}")))?
            .into_inner();

        // ── Update remote-tracking refs ────────────────────────────────────
        let now = current_timestamp();
        let mut results = HashMap::new();

        for result in &fin_resp.results {
            results.insert(result.ref_name.clone(), result.status.clone());

            if result.status == "ok" {
                if let Some(tracking_repo) = tracking {
                    if let Some(r) = to_push.iter().find(|r| r.name == result.ref_name) {
                        let _ = tracking_repo.update_remote_ref(&RemoteRef {
                            remote_name: config.remote_name.clone(),
                            ref_name: r.name.clone(),
                            kind: r.kind.clone(),
                            target: r.target.clone(),
                            updated_at: now,
                        });
                    }
                }
            }
        }

        progress(SyncProgress {
            stage: PushStage::Done,
            bytes_uploaded,
        });

        Ok(results)
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Convert a [`VersionId`] (hex string or arbitrary short ID) to 32 raw bytes
/// suitable for a proto `ObjectId`.
///
/// If the string is a valid 64-char lowercase hex string (i.e. a real BLAKE3
/// hash), it is decoded directly.  Otherwise the string is hashed with BLAKE3
/// so callers using synthetic IDs in tests still get a valid 32-byte value.
pub fn version_id_to_bytes(vid: &VersionId) -> Vec<u8> {
    if let Ok(bytes) = hex::decode(vid.as_str()) {
        if bytes.len() == 32 {
            return bytes;
        }
    }
    blake3::hash(vid.as_str().as_bytes()).as_bytes().to_vec()
}

/// Convert 32 raw bytes (from a proto `ObjectId`) back to a [`VersionId`] hex
/// string, suitable for [`VersionRepo::get_version`].
pub fn bytes_to_version_id(bytes: &[u8]) -> VersionId {
    VersionId(hex::encode(bytes))
}

/// Fetch an object from the appropriate local store and serialise it to bytes.
///
/// - `CHUNK`      → raw bytes (content-addressed)
/// - `TREE_NODE`  → JSON-encoded [`TreeNode`]
/// - `NODE_ENTRY` → JSON-encoded [`NodeEntry`]
/// - `VERSION`    → JSON-encoded [`VersionNode`]
fn fetch_object(
    proto_type: i32,
    hash_bytes: &[u8],
    chunks: &dyn ChunkStore,
    trees: &dyn TreeStore,
    nodes: &dyn NodeStore,
    versions: &dyn VersionRepo,
) -> CasResult<Vec<u8>> {
    if hash_bytes.len() != 32 {
        return Err(CasError::SyncError(format!(
            "invalid hash length {} (expected 32)",
            hash_bytes.len()
        )));
    }

    let arr: [u8; 32] = hash_bytes.try_into().unwrap();
    let hash: ChunkHash = blake3::Hash::from_bytes(arr);

    match proto_type {
        PROTO_TYPE_CHUNK => chunks
            .get_chunk(&hash)?
            .ok_or_else(|| CasError::NotFound(format!("chunk {hash}"))),

        PROTO_TYPE_TREE_NODE => {
            let node = trees
                .get_tree_node(&hash)?
                .ok_or_else(|| CasError::NotFound(format!("tree node {hash}")))?;
            serde_json::to_vec(&node).map_err(CasError::from)
        }

        PROTO_TYPE_NODE_ENTRY => {
            let entry = nodes
                .get_node(&hash)?
                .ok_or_else(|| CasError::NotFound(format!("node entry {hash}")))?;
            serde_json::to_vec(&entry).map_err(CasError::from)
        }

        PROTO_TYPE_VERSION => {
            // For version objects the hash bytes encode the VersionId hex string.
            let vid = bytes_to_version_id(hash_bytes);
            let v = versions
                .get_version(&vid)?
                .ok_or_else(|| CasError::NotFound(format!("version {vid}")))?;
            serde_json::to_vec(&v).map_err(CasError::from)
        }

        other => Err(CasError::SyncError(format!(
            "unknown proto object type {other}"
        ))),
    }
}
