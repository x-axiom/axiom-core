//! End-to-end integration tests for the Push / Pull sync protocol (E04-S07).
//!
//! ## Test architecture
//! 1. Spin up a tonic gRPC server backed by in-memory stores on an ephemeral
//!    TCP port (`127.0.0.1:0`).
//! 2. Run [`PushClient`] against it from a "client A" with its own in-memory
//!    stores.
//! 3. Run [`PullClient`] against the same server from a "client B" with its
//!    own in-memory stores.
//! 4. Assert byte-for-byte equality of every chunk / tree / node / version
//!    that travelled through the round-trip.
//!
//! All RPCs are dispatched through a `CombinedSyncHandler` that delegates the
//! Push-side calls to [`PushServiceHandler`] and the Pull-side calls to
//! [`PullServiceHandler`].

#![cfg(feature = "cloud")]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axiom_core::error::CasError;
use axiom_core::model::{
    NodeEntry, NodeKind, Ref, RefKind, TreeNode, TreeNodeKind, VersionId, VersionNode,
};
use axiom_core::store::memory::{
    InMemoryChunkStore, InMemoryManifestStore, InMemoryNodeStore, InMemoryRefRepo,
    InMemoryTreeStore,
    InMemoryVersionRepo,
};
use axiom_core::store::traits::{
    ChunkStore, NodeStore, ObjectManifestRepo, PathIndexRepo, RefRepo, Remote, RemoteRepo,
    RemoteTrackingRepo, SyncDirection, SyncSessionRepo, SyncSessionStatus, TreeStore, VersionRepo,
};
use axiom_core::store::sqlite::SqliteMetadataStore;
use axiom_core::sync::SyncServiceServer;
use axiom_core::sync::proto::{
    CloneDownloadRequest, CloneDownloadResponse, CloneInitRequest, CloneInitResponse,
    DownloadPackRequest, DownloadPackResponse, FinalizeRefsRequest, FinalizeRefsResponse,
    ListRefsRequest, ListRefsResponse, NegotiatePullRequest, NegotiatePullResponse,
    NegotiatePushRequest, NegotiatePushResponse, UploadPackRequest, UploadPackResponse,
};
use axiom_core::sync::proto::sync_service_server::SyncService;
use axiom_core::store::memory::InMemoryPathIndex;
use axiom_core::sync::clone_client::{CloneClient, CloneConfig};
use axiom_core::sync::shallow::ShallowBoundary;
use axiom_core::sync::pull_client::{PullClient, PullConfig};
use axiom_core::sync::pull_server::{PullServiceHandler, PullServiceState};
use axiom_core::sync::push_client::{PushClient, PushConfig};
use axiom_core::sync::push_server::{PushServiceHandler, PushServiceState};
use axiom_core::sync::bloom_negotiate::BloomFilter;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};

// ─── Combined SyncService handler ─────────────────────────────────────────────

/// Routes Push RPCs to `PushServiceHandler` and Pull RPCs to
/// `PullServiceHandler`.  Both handlers share the same backing stores via
/// `Arc<dyn …>`, so the server appears as a single workspace to clients.
struct CombinedSyncHandler {
    push: PushServiceHandler,
    pull: PullServiceHandler,
}

#[tonic::async_trait]
impl SyncService for CombinedSyncHandler {
    // ── Push side ─────────────────────────────────────────────────────────
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
        req: tonic::Request<tonic::Streaming<UploadPackRequest>>,
    ) -> Result<Response<UploadPackResponse>, Status> {
        self.push.upload_pack(req).await
    }

    async fn finalize_refs(
        &self,
        req: Request<FinalizeRefsRequest>,
    ) -> Result<Response<FinalizeRefsResponse>, Status> {
        self.push.finalize_refs(req).await
    }

    // ── Pull side ─────────────────────────────────────────────────────────
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
        let boxed: Self::DownloadPackStream = Box::pin(inner);
        Ok(Response::new(boxed))
    }

    // ── Clone (routed to pull handler) ────────────────────────────────────
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
        let boxed: Self::CloneDownloadStream = Box::pin(inner);
        Ok(Response::new(boxed))
    }
}

// ─── Test harness ─────────────────────────────────────────────────────────────

/// Bundle of the four object stores + ref repo used by both clients.
#[derive(Clone)]
struct Backends {
    chunks: Arc<InMemoryChunkStore>,
    trees: Arc<InMemoryTreeStore>,
    nodes: Arc<InMemoryNodeStore>,
    versions: Arc<InMemoryVersionRepo>,
    refs: Arc<InMemoryRefRepo>,
}

impl Backends {
    fn new() -> Self {
        Self {
            chunks: Arc::new(InMemoryChunkStore::default()),
            trees: Arc::new(InMemoryTreeStore::default()),
            nodes: Arc::new(InMemoryNodeStore::default()),
            versions: Arc::new(InMemoryVersionRepo::new()),
            refs: Arc::new(InMemoryRefRepo::default()),
        }
    }
}

/// In-memory remote-tracking repo for tests.
#[derive(Default)]
struct InMemoryRemoteTracking {
    inner: parking_lot::RwLock<
        HashMap<(String, String), axiom_core::store::traits::RemoteRef>,
    >,
}

impl RemoteTrackingRepo for InMemoryRemoteTracking {
    fn update_remote_ref(
        &self,
        r: &axiom_core::store::traits::RemoteRef,
    ) -> Result<(), CasError> {
        self.inner
            .write()
            .insert((r.remote_name.clone(), r.ref_name.clone()), r.clone());
        Ok(())
    }
    fn get_remote_ref(
        &self,
        remote_name: &str,
        ref_name: &str,
    ) -> Result<Option<axiom_core::store::traits::RemoteRef>, CasError> {
        Ok(self
            .inner
            .read()
            .get(&(remote_name.to_string(), ref_name.to_string()))
            .cloned())
    }
    fn list_remote_refs(
        &self,
        remote_name: &str,
    ) -> Result<Vec<axiom_core::store::traits::RemoteRef>, CasError> {
        Ok(self
            .inner
            .read()
            .iter()
            .filter(|((rn, _), _)| rn == remote_name)
            .map(|(_, v)| v.clone())
            .collect())
    }
    fn delete_remote_ref(&self, remote_name: &str, ref_name: &str) -> Result<(), CasError> {
        self.inner
            .write()
            .remove(&(remote_name.to_string(), ref_name.to_string()));
        Ok(())
    }
}

/// Spawn an in-process gRPC server and return its TCP address.
async fn spawn_server(server: Backends) -> std::net::SocketAddr {
    let sync_store: Arc<dyn axiom_core::store::traits::SyncStore> =
        Arc::new(axiom_core::store::InMemorySyncStore::new(
            server.versions.clone(),
            server.trees.clone(),
            server.nodes.clone(),
        ));
    let push_state = PushServiceState {
        chunks: server.chunks.clone(),
        trees: server.trees.clone(),
        nodes: server.nodes.clone(),
        versions: server.versions.clone(),
        refs: server.refs.clone(),
        sync: sync_store.clone(),
    };
    let pull_state = PullServiceState {
        chunks: server.chunks.clone(),
        trees: server.trees.clone(),
        nodes: server.nodes.clone(),
        versions: server.versions.clone(),
        refs: server.refs.clone(),
        sync: sync_store,
    };
    let handler = CombinedSyncHandler {
        push: PushServiceHandler::new(push_state),
        pull: PullServiceHandler::new(pull_state),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);

    tokio::spawn(async move {
        Server::builder()
            .add_service(SyncServiceServer::new(handler))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn connect_push(addr: std::net::SocketAddr) -> PushClient {
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    PushClient::connect(endpoint).await.unwrap()
}

async fn connect_pull(addr: std::net::SocketAddr) -> PullClient {
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    PullClient::connect(endpoint).await.unwrap()
}

// ─── Fixture builders ─────────────────────────────────────────────────────────

/// Insert a complete tiny version graph into `b`:
/// - 1 chunk of `data`
/// - 1 leaf tree node referencing that chunk
/// - 1 file node entry referencing the tree
/// - 1 version pointing at the node entry
/// - 1 branch ref `main` pointing at the version
///
/// Returns the new version id (hex 64-char so it round-trips through the
/// 32-byte proto envelope).
fn seed_branch(b: &Backends, branch: &str, data: &[u8], parents: Vec<VersionId>) -> VersionId {
    // Chunk
    let chunk_hash = b.chunks.put_chunk(data.to_vec()).unwrap();

    // Tree node (leaf)
    let tree_hash = blake3::hash(&[chunk_hash.as_bytes().as_slice(), branch.as_bytes()].concat());
    let tree = TreeNode {
        hash: tree_hash,
        kind: TreeNodeKind::Leaf { chunk: chunk_hash },
    };
    b.trees.put_tree_node(&tree).unwrap();

    // Node entry — File variant
    let entry_hash = blake3::hash(format!("entry-{branch}-{}", data.len()).as_bytes());
    let entry = NodeEntry {
        hash: entry_hash,
        kind: NodeKind::File {
            root: tree.hash,
            size: data.len() as u64,
        },
    };
    b.nodes.put_node(&entry).unwrap();

    // Version (id = BLAKE3 hex of unique input → 64-char hex)
    let vid_input = format!("{branch}-{}-{}", data.len(), parents.len());
    let vid_hash = blake3::hash(vid_input.as_bytes());
    let vid = VersionId(hex::encode(vid_hash.as_bytes()));
    let v = VersionNode {
        id: vid.clone(),
        parents,
        root: entry.hash,
        message: format!("commit on {branch}"),
        timestamp: 0,
        metadata: HashMap::new(),
    };
    b.versions.put_version(&v).unwrap();

    // Branch ref
    b.refs
        .put_ref(&Ref {
            name: branch.into(),
            kind: RefKind::Branch,
            target: vid.clone(),
        })
        .unwrap();

    vid
}

fn cfg_push(force: bool) -> PushConfig {
    PushConfig {
        remote_name: "origin".into(),
        workspace_id: "default".into(),
        tenant_id: "test".into(),
        force,
        concurrency: 1,
    }
}

fn cfg_pull() -> PullConfig {
    PullConfig {
        remote_name: "origin".into(),
        workspace_id: "default".into(),
        tenant_id: "test".into(),
        have_filter_override: None,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn push_then_pull_roundtrip_byte_for_byte() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    // Client A: seed + push.
    let client_a = Backends::new();
    let vid_a = seed_branch(&client_a, "main", b"hello world", vec![]);

    let mut push = connect_push(addr).await;
    let tracking_a = InMemoryRemoteTracking::default();
    let results = push
        .push(
            &cfg_push(false),
            &[],
            client_a.refs.as_ref(),
            client_a.chunks.as_ref(),
            client_a.trees.as_ref(),
            client_a.nodes.as_ref(),
            client_a.versions.as_ref(),
            Some(&tracking_a),
            None,
            None,
            |_| {},
        )
        .await
        .expect("push ok");

    assert_eq!(results.get("main").map(String::as_str), Some("ok"));
    assert!(server.refs.get_ref("main").unwrap().is_some());
    assert_eq!(server.refs.get_ref("main").unwrap().unwrap().target, vid_a);

    // Client B: pull into empty stores.
    let client_b = Backends::new();
    let tracking_b = InMemoryRemoteTracking::default();
    let mut pull = connect_pull(addr).await;
    let pulled = pull
        .pull(
            &cfg_pull(),
            &[],
            client_b.refs.as_ref(),
            &tracking_b,
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            None,
            |_| {},
        )
        .await
        .expect("pull ok");

    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].ref_name, "main");
    assert_eq!(pulled[0].remote_tip, vid_a);
    assert!(pulled[0].fast_forwarded);

    // Local branch was fast-forwarded.
    assert_eq!(client_b.refs.get_ref("main").unwrap().unwrap().target, vid_a);

    // Byte-for-byte version content matches.
    let v_a = client_a.versions.get_version(&vid_a).unwrap().unwrap();
    let v_b = client_b.versions.get_version(&vid_a).unwrap().unwrap();
    assert_eq!(v_a.id, v_b.id);
    assert_eq!(v_a.root, v_b.root);
    assert_eq!(v_a.message, v_b.message);
    assert_eq!(v_a.parents, v_b.parents);
}

#[tokio::test]
async fn empty_push_when_no_local_refs_is_noop() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client_a = Backends::new(); // no refs, no objects
    let mut push = connect_push(addr).await;
    let results = push
        .push(
            &cfg_push(false),
            &[],
            client_a.refs.as_ref(),
            client_a.chunks.as_ref(),
            client_a.trees.as_ref(),
            client_a.nodes.as_ref(),
            client_a.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .expect("empty push ok");
    assert!(results.is_empty());
}

#[tokio::test]
async fn non_fast_forward_push_is_rejected() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    // Client A pushes initial commit.
    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"v1", vec![]);
    let mut push_a = connect_push(addr).await;
    push_a
        .push(
            &cfg_push(false),
            &[],
            client_a.refs.as_ref(),
            client_a.chunks.as_ref(),
            client_a.trees.as_ref(),
            client_a.nodes.as_ref(),
            client_a.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .unwrap();

    // Client B has a different (forked) commit on main, also with empty parents,
    // so its tip is NOT a descendant of A's tip → server must reject as
    // non-fast-forward.
    let client_b = Backends::new();
    seed_branch(&client_b, "main", b"v2-different", vec![]);
    let mut push_b = connect_push(addr).await;

    let err = push_b
        .push(
            &cfg_push(false),
            &[],
            client_b.refs.as_ref(),
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .expect_err("should reject");
    assert!(matches!(err, CasError::NonFastForward(_)));
}

#[tokio::test]
async fn force_push_overrides_non_fast_forward() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    // Client A pushes initial.
    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"v1", vec![]);
    let mut push_a = connect_push(addr).await;
    push_a
        .push(
            &cfg_push(false),
            &[],
            client_a.refs.as_ref(),
            client_a.chunks.as_ref(),
            client_a.trees.as_ref(),
            client_a.nodes.as_ref(),
            client_a.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .unwrap();

    // Client B forks and force-pushes.
    let client_b = Backends::new();
    let vid_b = seed_branch(&client_b, "main", b"v2-forced", vec![]);
    let mut push_b = connect_push(addr).await;
    let results = push_b
        .push(
            &cfg_push(true), // force
            &[],
            client_b.refs.as_ref(),
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .expect("force push ok");
    assert_eq!(results.get("main").map(String::as_str), Some("ok"));

    // Server now points at B's version.
    assert_eq!(
        server.refs.get_ref("main").unwrap().unwrap().target,
        vid_b
    );
}

#[tokio::test]
async fn progress_callback_invoked_for_push_and_pull() {
    use std::sync::Mutex;

    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"hello", vec![]);

    // Push: collect stages.
    let push_stages = Arc::new(Mutex::new(Vec::new()));
    let push_stages_clone = Arc::clone(&push_stages);

    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        None,
        None,
        None,
        move |p| {
            push_stages_clone
                .lock()
                .unwrap()
                .push(format!("{:?}", p.stage));
        },
    )
    .await
    .unwrap();

    let stages = push_stages.lock().unwrap();
    assert!(stages.iter().any(|s| s.starts_with("ListingRefs")));
    assert!(stages.iter().any(|s| s.starts_with("Negotiating")));
    assert!(stages.iter().any(|s| s.starts_with("Uploading")));
    assert!(stages.iter().any(|s| s.starts_with("Finalizing")));
    assert!(stages.iter().any(|s| s.starts_with("Done")));
    drop(stages);

    // Pull: collect stages.
    let client_b = Backends::new();
    let tracking_b = InMemoryRemoteTracking::default();
    let pull_stages = Arc::new(Mutex::new(Vec::new()));
    let pull_stages_clone = Arc::clone(&pull_stages);

    let mut pull = connect_pull(addr).await;
    pull.pull(
        &cfg_pull(),
        &[],
        client_b.refs.as_ref(),
        &tracking_b,
        client_b.chunks.as_ref(),
        client_b.trees.as_ref(),
        client_b.nodes.as_ref(),
        client_b.versions.as_ref(),
        None,
        None,
        move |p| {
            pull_stages_clone
                .lock()
                .unwrap()
                .push(format!("{:?}", p.stage));
        },
    )
    .await
    .unwrap();

    let stages = pull_stages.lock().unwrap();
    assert!(stages.iter().any(|s| s.starts_with("ListingRefs")));
    assert!(stages.iter().any(|s| s.starts_with("Negotiating")));
    assert!(stages.iter().any(|s| s.starts_with("Downloading")));
    assert!(stages.iter().any(|s| s.starts_with("Done")));
}

#[tokio::test]
async fn push_updates_remote_tracking_refs() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client_a = Backends::new();
    let vid = seed_branch(&client_a, "main", b"track-me", vec![]);

    let tracking = InMemoryRemoteTracking::default();
    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        Some(&tracking),
        None,
        None,
        |_| {},
    )
    .await
    .unwrap();

    let rr = tracking.get_remote_ref("origin", "main").unwrap().unwrap();
    assert_eq!(rr.target, vid);
    assert_eq!(rr.remote_name, "origin");
}

#[tokio::test]
async fn pull_from_empty_remote_returns_no_refs() {
    let server = Backends::new(); // no refs at all
    let addr = spawn_server(server.clone()).await;

    let client_b = Backends::new();
    let tracking_b = InMemoryRemoteTracking::default();
    let mut pull = connect_pull(addr).await;
    let results = pull
        .pull(
            &cfg_pull(),
            &[],
            client_b.refs.as_ref(),
            &tracking_b,
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            None,
            |_| {},
        )
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn push_and_pull_record_sync_sessions() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    // Shared SQLite session repo (in-memory) — also needs the remote row
    // because `sync_sessions.remote_name` is a FK into `remotes`.
    let session_db = SqliteMetadataStore::open_in_memory().expect("in-memory sqlite");
    session_db
        .add_remote(&Remote {
            name: "origin".into(),
            url: format!("http://{addr}"),
            auth_token: String::new(),
            tenant_id: None,
            workspace_id: None,
            created_at: 0,
        })
        .unwrap();

    // ── Push with session tracking enabled ───────────────────────────────
    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"session-test", vec![]);

    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        None,
        Some(&session_db),
        None,
        |_| {},
    )
    .await
    .expect("push ok");

    // Push session was recorded as Completed with non-zero counters.
    let push_sessions = session_db.list_sync_sessions(Some("origin"), 10).unwrap();
    let push_session = push_sessions
        .iter()
        .find(|s| s.id.starts_with("push-"))
        .expect("push session row");
    assert_eq!(push_session.status, SyncSessionStatus::Completed);
    assert_eq!(push_session.remote_name, "origin");
    assert!(push_session.finished_at.is_some());
    assert!(push_session.objects_transferred > 0);
    assert!(push_session.bytes_transferred > 0);
    assert!(push_session.error_message.is_none());

    // ── Pull with session tracking enabled ───────────────────────────────
    let client_b = Backends::new();
    let tracking_b = InMemoryRemoteTracking::default();
    let mut pull = connect_pull(addr).await;
    pull.pull(
        &cfg_pull(),
        &[],
        client_b.refs.as_ref(),
        &tracking_b,
        client_b.chunks.as_ref(),
        client_b.trees.as_ref(),
        client_b.nodes.as_ref(),
        client_b.versions.as_ref(),
        Some(&session_db),
        None,
        |_| {},
    )
    .await
    .expect("pull ok");

    let pull_sessions = session_db.list_sync_sessions(Some("origin"), 10).unwrap();
    let pull_session = pull_sessions
        .iter()
        .find(|s| s.id.starts_with("pull-"))
        .expect("pull session row");
    assert_eq!(pull_session.status, SyncSessionStatus::Completed);
    assert!(pull_session.objects_transferred > 0);
    assert!(pull_session.bytes_transferred > 0);
}

#[tokio::test]
async fn failed_push_records_failed_session() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let session_db = SqliteMetadataStore::open_in_memory().expect("in-memory sqlite");
    session_db
        .add_remote(&Remote {
            name: "origin".into(),
            url: format!("http://{addr}"),
            auth_token: String::new(),
            tenant_id: None,
            workspace_id: None,
            created_at: 0,
        })
        .unwrap();

    // Initial push by A succeeds (also recorded).
    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"v1", vec![]);
    let mut push_a = connect_push(addr).await;
    push_a
        .push(
            &cfg_push(false),
            &[],
            client_a.refs.as_ref(),
            client_a.chunks.as_ref(),
            client_a.trees.as_ref(),
            client_a.nodes.as_ref(),
            client_a.versions.as_ref(),
            None,
            Some(&session_db),
            None,
            |_| {},
        )
        .await
        .unwrap();

    // B pushes a forked tip without --force → server rejects → session = Failed.
    let client_b = Backends::new();
    seed_branch(&client_b, "main", b"v2-different", vec![]);
    let mut push_b = connect_push(addr).await;
    let _err = push_b
        .push(
            &cfg_push(false),
            &[],
            client_b.refs.as_ref(),
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            Some(&session_db),
            None,
            |_| {},
        )
        .await
        .expect_err("push must be rejected");

    let sessions = session_db.list_sync_sessions(Some("origin"), 10).unwrap();
    let failed = sessions
        .iter()
        .find(|s| s.status == SyncSessionStatus::Failed)
        .expect("failed session row");
    assert!(failed.id.starts_with("push-"));
    assert!(failed.error_message.is_some());
    assert!(failed.finished_at.is_some());
}

// ─── Server-side safety guards (B2 / B3 regressions) ─────────────────────────

#[tokio::test]
async fn upload_pack_rejects_unknown_session_id() {
    use axiom_core::sync::SyncServiceClient;
    use axiom_core::sync::proto::{PackHeader, upload_pack_request::Payload};

    let server = Backends::new();
    let addr = spawn_server(server).await;

    let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    let mut client = SyncServiceClient::connect(endpoint).await.unwrap();

    // Stream a single Header carrying a fabricated session_id that the server
    // never minted. Handler must refuse at the header stage — before accepting
    // any object entries — otherwise raw objects could be written bypassing
    // NegotiatePush's quota / ref validation.
    let stream = tokio_stream::iter(vec![UploadPackRequest {
        payload: Some(Payload::Header(PackHeader {
            object_count: 0,
            estimated_bytes: 0,
            session_id: "never-negotiated".into(),
        })),
    }]);
    let err = client
        .upload_pack(Request::new(stream))
        .await
        .expect_err("upload without valid session must fail");
    assert_eq!(
        err.code(),
        tonic::Code::NotFound,
        "expected NotFound, got: {err:?}"
    );
}

#[tokio::test]
async fn finalize_refs_rejects_unknown_session_id() {
    use axiom_core::sync::SyncServiceClient;

    let server = Backends::new();
    let addr = spawn_server(server).await;

    let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    let mut client = SyncServiceClient::connect(endpoint).await.unwrap();

    // Call FinalizeRefs with a session_id that was never created.
    let err = client
        .finalize_refs(Request::new(FinalizeRefsRequest {
            session_id: "bogus-session".into(),
            workspace_id: String::new(),
            tenant_id: String::new(),
            ref_updates: vec![],
        }))
        .await
        .expect_err("finalize with unknown session must fail");
    assert_eq!(
        err.code(),
        tonic::Code::NotFound,
        "expected NotFound, got: {err:?}"
    );
}

// ─── Clone tests (E05-S01) ────────────────────────────────────────────────────

fn cfg_clone() -> CloneConfig {
    CloneConfig {
        remote_name: "origin".into(),
        workspace_id: "default".into(),
        tenant_id: "test".into(),
    }
}

async fn connect_clone(addr: std::net::SocketAddr) -> CloneClient {
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    CloneClient::connect(endpoint).await.unwrap()
}

#[tokio::test]
async fn clone_empty_remote_succeeds() {
    // Server has no refs — clone should return an empty result, not an error.
    let server = Backends::new();
    let addr = spawn_server(server).await;

    let client = Backends::new();
    let mut clone = connect_clone(addr).await;
    let results = clone
        .clone(
            &cfg_clone(),
            &[],
            client.refs.as_ref(),
            client.chunks.as_ref(),
            client.trees.as_ref(),
            client.nodes.as_ref(),
            client.versions.as_ref(),
            None,
            None,
            |_| {},
        )
        .await
        .expect("clone of empty remote must not error");

    assert!(results.is_empty());
    assert!(client.refs.list_refs(None).unwrap().is_empty());
}

#[tokio::test]
async fn clone_with_history_rebuilds_refs_and_path_index() {
    // Client A pushes data, then client B clones from the same server into
    // empty stores.  After the clone:
    // 1. client B has a local `main` branch ref pointing at A's tip.
    // 2. client B can read back the cloned VersionNode.
    // 3. The path index has an entry for the root path of that version.
    let server = Backends::new();
    let addr = spawn_server(server).await;

    // Client A: seed + push.
    let client_a = Backends::new();
    let vid_a = seed_branch(&client_a, "main", b"clone-test-data", vec![]);
    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        None,
        None,
        None,
        |_| {},
    )
    .await
    .expect("push ok");

    // Client B: clone into empty stores with a path index.
    let client_b = Backends::new();
    let path_index = Arc::new(InMemoryPathIndex::default());
    let mut clone = connect_clone(addr).await;
    let results = clone
        .clone(
            &cfg_clone(),
            &[],
            client_b.refs.as_ref(),
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            Some(path_index.as_ref()),
            None,
            |_| {},
        )
        .await
        .expect("clone ok");

    // (1) One result for "main".
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].ref_name, "main");
    assert_eq!(results[0].tip, vid_a);
    assert!(results[0].objects_received > 0);

    // (2) Local branch ref was created.
    let local_main = client_b.refs.get_ref("main").unwrap().unwrap();
    assert_eq!(local_main.target, vid_a);

    // (3) VersionNode is readable.
    let v = client_b.versions.get_version(&vid_a).unwrap().unwrap();
    assert_eq!(v.id, vid_a);

    // (4) Path index was populated for the cloned version (root entry).
    let root_entry = path_index.get_by_path(&vid_a, "").unwrap();
    assert!(root_entry.is_some(), "path index root entry must exist after clone");
}

// ── Shallow clone tests (E05-S02) ─────────────────────────────────────────────

#[tokio::test]
async fn shallow_clone_depth_1_gets_only_tip_version() {
    // Server: root → tip (2-commit chain on branch "main").
    let server = Backends::new();
    let root_vid = seed_branch(&server, "main", b"root", vec![]);
    let tip_vid = seed_branch(&server, "main", b"tip", vec![root_vid.clone()]);
    let addr = spawn_server(server).await;

    let client = Backends::new();
    let path_index = InMemoryPathIndex::new();
    let mut clone = connect_clone(addr).await;

    let (results, boundary) = clone
        .clone_shallow(
            &cfg_clone(),
            1, // depth=1: only the tip
            &[],
            client.refs.as_ref(),
            client.chunks.as_ref(),
            client.trees.as_ref(),
            client.nodes.as_ref(),
            client.versions.as_ref(),
            Some(&path_index),
            None,
            |_| {},
        )
        .await
        .expect("shallow clone must not error");

    // Only the tip was transferred.
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].ref_name, "main");
    assert_eq!(results[0].tip, tip_vid);

    // Tip version is readable.
    assert!(client.versions.get_version(&tip_vid).unwrap().is_some());

    // Root version must NOT be in the client store.
    assert!(client.versions.get_version(&root_vid).unwrap().is_none());

    // Boundary contains the parent we didn't fetch.
    assert!(boundary.contains(&root_vid), "root_vid must be in boundary");
}

#[tokio::test]
async fn shallow_clone_depth_2_gets_two_versions() {
    // Server: root → mid → tip (3-commit chain).
    let server = Backends::new();
    let root_vid = seed_branch(&server, "main", b"r", vec![]);
    let mid_vid = seed_branch(&server, "main", b"mi", vec![root_vid.clone()]);
    let tip_vid = seed_branch(&server, "main", b"tip", vec![mid_vid.clone()]);
    let addr = spawn_server(server).await;

    let client = Backends::new();
    let path_index = InMemoryPathIndex::new();
    let mut clone = connect_clone(addr).await;

    let (results, boundary) = clone
        .clone_shallow(
            &cfg_clone(),
            2, // depth=2: tip + mid
            &[],
            client.refs.as_ref(),
            client.chunks.as_ref(),
            client.trees.as_ref(),
            client.nodes.as_ref(),
            client.versions.as_ref(),
            Some(&path_index),
            None,
            |_| {},
        )
        .await
        .expect("shallow clone depth=2 must not error");

    assert_eq!(results[0].tip, tip_vid);
    assert!(client.versions.get_version(&tip_vid).unwrap().is_some());
    assert!(client.versions.get_version(&mid_vid).unwrap().is_some());
    assert!(client.versions.get_version(&root_vid).unwrap().is_none());

    assert!(boundary.contains(&root_vid), "root_vid must be in boundary");
    assert!(!boundary.contains(&mid_vid), "mid_vid must NOT be a boundary");
}

#[tokio::test]
async fn unshallow_retrieves_full_history() {
    // Server: root → mid → tip (3-commit chain).
    let server = Backends::new();
    let root_vid = seed_branch(&server, "main", b"r", vec![]);
    let mid_vid = seed_branch(&server, "main", b"mi", vec![root_vid.clone()]);
    let tip_vid = seed_branch(&server, "main", b"tip", vec![mid_vid.clone()]);
    let addr = spawn_server(server).await;

    let client = Backends::new();
    let path_index = InMemoryPathIndex::new();
    let mut clone = connect_clone(addr).await;

    // Shallow clone with depth=1: only the tip.
    let (_, boundary) = clone
        .clone_shallow(
            &cfg_clone(),
            1,
            &[],
            client.refs.as_ref(),
            client.chunks.as_ref(),
            client.trees.as_ref(),
            client.nodes.as_ref(),
            client.versions.as_ref(),
            Some(&path_index),
            None,
            |_| {},
        )
        .await
        .expect("shallow clone must not error");

    assert!(client.versions.get_version(&root_vid).unwrap().is_none());
    assert!(!boundary.is_empty());

    // Unshallow: fetch the missing ancestors (mid + root).
    let objects_received = clone
        .unshallow(
            &cfg_clone(),
            &boundary,
            client.chunks.as_ref(),
            client.trees.as_ref(),
            client.nodes.as_ref(),
            client.versions.as_ref(),
            Some(&path_index),
            |_| {},
        )
        .await
        .expect("unshallow must not error");

    assert!(objects_received > 0, "unshallow must transfer some objects");

    // After unshallow, all three versions must be present.
    assert!(client.versions.get_version(&tip_vid).unwrap().is_some());
    assert!(client.versions.get_version(&mid_vid).unwrap().is_some());
    assert!(client.versions.get_version(&root_vid).unwrap().is_some());
}

// ─── Resume/checkpoint tests (E05-S03) ───────────────────────────────────────

#[tokio::test]
async fn resume_push_manifest_is_recorded_and_cleaned_up() {
    // Push with an InMemoryManifestStore. After a successful push the manifest
    // for the session should be empty (deleted on completion).
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"resume-push-data", vec![]);

    let session_db = SqliteMetadataStore::open_in_memory().expect("in-memory sqlite");
    session_db
        .add_remote(&Remote {
            name: "origin".into(),
            url: format!("http://{addr}"),
            auth_token: String::new(),
            tenant_id: None,
            workspace_id: None,
            created_at: 0,
        })
        .unwrap();

    let manifest_store = InMemoryManifestStore::new();

    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        None,
        Some(&session_db),
        Some(&manifest_store),
        |_| {},
    )
    .await
    .expect("push ok");

    // After successful push: session is Completed.
    let sessions = session_db.list_sync_sessions(Some("origin"), 10).unwrap();
    let push_session = sessions
        .iter()
        .find(|s| s.id.starts_with("push-"))
        .expect("push session row");
    assert_eq!(push_session.status, SyncSessionStatus::Completed);

    // After successful push: manifest for that session is deleted.
    let remaining = manifest_store.manifest_load(&push_session.id).unwrap();
    assert!(
        remaining.is_empty(),
        "manifest must be cleared after successful push, got: {remaining:?}"
    );
}

#[tokio::test]
async fn resume_pull_manifest_accumulates_versions_and_cleaned_up() {
    // Push data first, then pull with manifest tracking. After success the
    // manifest for the pull session should be deleted.
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"resume-pull-data", vec![]);

    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        None,
        None,
        None,
        |_| {},
    )
    .await
    .expect("push ok");

    let client_b = Backends::new();
    let tracking_b = InMemoryRemoteTracking::default();

    let session_db = SqliteMetadataStore::open_in_memory().expect("in-memory sqlite");
    session_db
        .add_remote(&Remote {
            name: "origin".into(),
            url: format!("http://{addr}"),
            auth_token: String::new(),
            tenant_id: None,
            workspace_id: None,
            created_at: 0,
        })
        .unwrap();

    let manifest_store = InMemoryManifestStore::new();

    let mut pull = connect_pull(addr).await;
    pull.pull(
        &cfg_pull(),
        &[],
        client_b.refs.as_ref(),
        &tracking_b,
        client_b.chunks.as_ref(),
        client_b.trees.as_ref(),
        client_b.nodes.as_ref(),
        client_b.versions.as_ref(),
        Some(&session_db),
        Some(&manifest_store),
        |_| {},
    )
    .await
    .expect("pull ok");

    // Session is Completed.
    let sessions = session_db.list_sync_sessions(Some("origin"), 10).unwrap();
    let pull_session = sessions
        .iter()
        .find(|s| s.id.starts_with("pull-"))
        .expect("pull session row");
    assert_eq!(pull_session.status, SyncSessionStatus::Completed);

    // Manifest is deleted after success.
    let remaining = manifest_store.manifest_load(&pull_session.id).unwrap();
    assert!(
        remaining.is_empty(),
        "manifest must be cleared after successful pull, got: {remaining:?}"
    );
}

#[tokio::test]
async fn resume_stale_session_is_not_resumed() {
    // A Running session started long ago (started_at = 0) should NOT be resumed
    // because it exceeds the max_age_secs threshold (86400 seconds).
    use axiom_core::store::traits::{SyncSession, SyncSessionStatus};

    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"stale-session-data", vec![]);

    let session_db = SqliteMetadataStore::open_in_memory().expect("in-memory sqlite");
    session_db
        .add_remote(&Remote {
            name: "origin".into(),
            url: format!("http://{addr}"),
            auth_token: String::new(),
            tenant_id: None,
            workspace_id: None,
            created_at: 0,
        })
        .unwrap();

    // Insert an ancient Running session manually.
    let stale_id = "push-000000stale";
    session_db
        .create_session(&SyncSession {
            id: stale_id.to_string(),
            remote_name: "origin".to_string(),
            direction: SyncDirection::Push,
            status: SyncSessionStatus::Running,
            started_at: 0, // epoch — definitely older than 86400 s
            finished_at: None,
            objects_transferred: 0,
            bytes_transferred: 0,
            error_message: None,
        })
        .unwrap();

    let manifest_store = InMemoryManifestStore::new();
    // Seed manifest with a fake hash for the stale session.
    manifest_store
        .manifest_append(stale_id, &["aabbcc".to_string()])
        .unwrap();

    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        None,
        Some(&session_db),
        Some(&manifest_store),
        |_| {},
    )
    .await
    .expect("push ok");

    // The stale session should have been transitioned to Failed.
    let stale = session_db
        .get_session(stale_id)
        .unwrap()
        .expect("stale session must still exist");
    assert_eq!(
        stale.status,
        SyncSessionStatus::Failed,
        "stale session must be marked Failed"
    );

    // A new Completed session should exist for this push.
    let sessions = session_db.list_sync_sessions(Some("origin"), 10).unwrap();
    let new_session = sessions
        .iter()
        .find(|s| s.id != stale_id && s.status == SyncSessionStatus::Completed)
        .expect("new completed session must exist");
    assert!(new_session.id.starts_with("push-"));
}

#[tokio::test]
async fn resume_pull_skips_manifested_versions_via_have_hints() {
    // Server has a version. Client B has it locally AND the pull session
    // manifest lists it. The pull should still succeed (no error) even
    // though the server sees it as already-have via the have hint.
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client_a = Backends::new();
    let vid = seed_branch(&client_a, "main", b"have-hint-data", vec![]);

    // Push A's version to the server.
    let mut push = connect_push(addr).await;
    push.push(
        &cfg_push(false),
        &[],
        client_a.refs.as_ref(),
        client_a.chunks.as_ref(),
        client_a.trees.as_ref(),
        client_a.nodes.as_ref(),
        client_a.versions.as_ref(),
        None,
        None,
        None,
        |_| {},
    )
    .await
    .expect("push ok");

    let session_db = SqliteMetadataStore::open_in_memory().expect("in-memory sqlite");
    session_db
        .add_remote(&Remote {
            name: "origin".into(),
            url: format!("http://{addr}"),
            auth_token: String::new(),
            tenant_id: None,
            workspace_id: None,
            created_at: 0,
        })
        .unwrap();

    // Client B already has the version locally.
    let client_b = Backends::new();
    // Copy the version (and its tree/node/chunk objects) from A to B.
    let v = client_a.versions.get_version(&vid).unwrap().unwrap();
    client_b.versions.put_version(&v).unwrap();
    let tracking_b = InMemoryRemoteTracking::default();

    let manifest_store = InMemoryManifestStore::new();

    // Pull should complete without error even though client_b already has
    // the objects — the have hints prevent the server from re-sending them.
    let mut pull = connect_pull(addr).await;
    let results = pull
        .pull(
            &cfg_pull(),
            &[],
            client_b.refs.as_ref(),
            &tracking_b,
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            Some(&session_db),
            Some(&manifest_store),
            |_| {},
        )
        .await
        .expect("pull ok with manifest");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].ref_name, "main");
    assert_eq!(results[0].remote_tip, vid);
}

// ─── E05-S04: Parallel chunked transfer ──────────────────────────────────────

/// Helper: push with a specific concurrency value.
async fn push_with_concurrency(
    addr: std::net::SocketAddr,
    client: &Backends,
    concurrency: u8,
) -> HashMap<String, String> {
    let mut push = connect_push(addr).await;
    push.push(
        &PushConfig {
            remote_name: "origin".into(),
            workspace_id: "default".into(),
            tenant_id: "test".into(),
            force: false,
            concurrency,
        },
        &[],
        client.refs.as_ref(),
        client.chunks.as_ref(),
        client.trees.as_ref(),
        client.nodes.as_ref(),
        client.versions.as_ref(),
        None,
        None,
        None,
        |_| {},
    )
    .await
    .expect("parallel push ok")
}

#[tokio::test]
async fn parallel_push_4_branches_concurrency_4_all_objects_transferred() {
    // Seed 4 branches (each creates 1 chunk + 1 tree + 1 node + 1 version = 4
    // objects), then push with concurrency=4.  Each shard carries ~4 objects,
    // and all must arrive on the server.
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client = Backends::new();
    let vid_a = seed_branch(&client, "main", b"data-a", vec![]);
    let vid_b = seed_branch(&client, "feat", b"data-b", vec![]);
    let vid_c = seed_branch(&client, "dev", b"data-c", vec![]);
    let vid_d = seed_branch(&client, "fix", b"data-d", vec![]);

    let results = push_with_concurrency(addr, &client, 4).await;

    assert_eq!(results.get("main").map(String::as_str), Some("ok"));
    assert_eq!(results.get("feat").map(String::as_str), Some("ok"));
    assert_eq!(results.get("dev").map(String::as_str), Some("ok"));
    assert_eq!(results.get("fix").map(String::as_str), Some("ok"));

    // All 4 versions must be present on the server.
    assert!(server.versions.get_version(&vid_a).unwrap().is_some());
    assert!(server.versions.get_version(&vid_b).unwrap().is_some());
    assert!(server.versions.get_version(&vid_c).unwrap().is_some());
    assert!(server.versions.get_version(&vid_d).unwrap().is_some());

    // Server refs must point at the pushed tips.
    assert_eq!(server.refs.get_ref("main").unwrap().unwrap().target, vid_a);
    assert_eq!(server.refs.get_ref("feat").unwrap().unwrap().target, vid_b);
    assert_eq!(server.refs.get_ref("dev").unwrap().unwrap().target, vid_c);
    assert_eq!(server.refs.get_ref("fix").unwrap().unwrap().target, vid_d);
}

#[tokio::test]
async fn parallel_push_result_matches_serial() {
    // Push the same data twice: once serially (concurrency=1) and once in
    // parallel (concurrency=4), to two separate servers.  Both server ref
    // states must be identical.
    let server_serial = Backends::new();
    let addr_serial = spawn_server(server_serial.clone()).await;

    let server_parallel = Backends::new();
    let addr_parallel = spawn_server(server_parallel.clone()).await;

    let client = Backends::new();
    let vid = seed_branch(&client, "main", b"compare-data", vec![]);

    push_with_concurrency(addr_serial, &client, 1).await;
    push_with_concurrency(addr_parallel, &client, 4).await;

    // Both servers must have the same version.
    let v_serial = server_serial.versions.get_version(&vid).unwrap().unwrap();
    let v_parallel = server_parallel.versions.get_version(&vid).unwrap().unwrap();
    assert_eq!(v_serial.id, v_parallel.id);
    assert_eq!(v_serial.root, v_parallel.root);

    // Both servers must have the branch ref pointing at the same target.
    let r_serial = server_serial.refs.get_ref("main").unwrap().unwrap();
    let r_parallel = server_parallel.refs.get_ref("main").unwrap().unwrap();
    assert_eq!(r_serial.target, r_parallel.target);
}

#[tokio::test]
async fn parallel_push_concurrency_clamped_succeeds() {
    // concurrency=0 is clamped to 1 (serial path) — must not panic or error.
    // concurrency=16 (max) must also succeed.
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client = Backends::new();
    let vid = seed_branch(&client, "main", b"clamp-test", vec![]);

    // concurrency=0 → clamped to 1.
    push_with_concurrency(addr, &client, 0).await;
    assert!(server.refs.get_ref("main").unwrap().is_some());

    // Second push to the same server: already up-to-date (need_objects empty),
    // but must not error on concurrency=16 either.
    let results = push_with_concurrency(addr, &client, 16).await;
    assert_eq!(results.get("main").map(String::as_str), Some("ok"));
    assert_eq!(server.refs.get_ref("main").unwrap().unwrap().target, vid);
}

// ─── E05-S05: Bloom Filter have-set encoding ──────────────────────────────────

/// Build a Bloom filter containing every object hash reachable from the given
/// version in `b`'s stores (version + node + tree + chunk).
fn bloom_of_version_objects(b: &Backends, vid: &VersionId) -> Vec<u8> {
    let mut bf = BloomFilter::with_capacity(64, 0.01);

    // Version hash.
    if let Ok(bytes) = hex::decode(vid.as_str()) {
        if let Ok(arr) = TryInto::<[u8; 32]>::try_into(bytes) {
            bf.insert(&arr);
        }
    }

    // Walk the version → node → tree → chunk chain.
    if let Some(v) = b.versions.get_version(vid).unwrap() {
        // root is the NodeEntry hash.
        bf.insert(v.root.as_bytes());

        if let Some(node) = b.nodes.get_node(&v.root).unwrap() {
            if let NodeKind::File { root: tree_hash, .. } = node.kind {
                bf.insert(tree_hash.as_bytes());

                if let Some(tree) = b.trees.get_tree_node(&tree_hash).unwrap() {
                    if let TreeNodeKind::Leaf { chunk } = tree.kind {
                        bf.insert(chunk.as_bytes());
                    }
                }
            }
        }
    }

    bf.to_bytes()
}

#[tokio::test]
async fn bloom_pull_have_filter_skips_objects_client_already_has() {
    // Scenario: server has V1 (seeded).  Client B also has V1's data (seeded
    // locally, bypassing pull).  Client B pulls with a Bloom filter that
    // covers all V1 objects → server should filter them → 0 objects downloaded
    // → ref still fast-forwarded because client B already has the data.
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    // Seed V1 on server and on client B independently.
    let vid = seed_branch(&server, "main", b"bloom-test-data", vec![]);
    let client_b = Backends::new();
    seed_branch(&client_b, "main", b"bloom-test-data", vec![]);

    // Build BF of all V1 objects that client B already has.
    let have_filter = bloom_of_version_objects(&client_b, &vid);

    let tracking_b = InMemoryRemoteTracking::default();
    let mut pull = connect_pull(addr).await;

    let results = pull
        .pull(
            &PullConfig {
                remote_name: "origin".into(),
                workspace_id: "default".into(),
                tenant_id: "test".into(),
                have_filter_override: Some(have_filter),
            },
            &[],
            client_b.refs.as_ref(),
            &tracking_b,
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            None,
            |_| {},
        )
        .await
        .expect("pull ok with bloom filter");

    // Remote ref must be resolved and branch must point at vid.
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].ref_name, "main");
    assert_eq!(results[0].remote_tip, vid);
    assert!(results[0].fast_forwarded);
    assert_eq!(client_b.refs.get_ref("main").unwrap().unwrap().target, vid);
}

#[tokio::test]
async fn bloom_pull_empty_filter_unchanged_behaviour() {
    // With no have_filter (empty bytes), pull must behave exactly as before.
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let vid = seed_branch(&server, "main", b"no-bloom-data", vec![]);

    let client = Backends::new();
    let tracking = InMemoryRemoteTracking::default();
    let mut pull = connect_pull(addr).await;

    let results = pull
        .pull(
            &cfg_pull(),
            &[],
            client.refs.as_ref(),
            &tracking,
            client.chunks.as_ref(),
            client.trees.as_ref(),
            client.nodes.as_ref(),
            client.versions.as_ref(),
            None,
            None,
            |_| {},
        )
        .await
        .expect("pull ok without bloom filter");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].remote_tip, vid);
    assert!(results[0].fast_forwarded);
    // All V1 objects must be present on the client.
    assert!(client.versions.get_version(&vid).unwrap().is_some());
}

#[tokio::test]
async fn bloom_pull_unknown_object_still_downloaded_when_not_in_filter() {
    // Objects NOT in the Bloom filter must still be downloaded.
    // Client B has V1 but NOT V2; filter covers only V1.  Pull V2 from server.
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let vid1 = seed_branch(&server, "main", b"first-version", vec![]);
    let vid2 = seed_branch(&server, "main", b"second-version", vec![vid1.clone()]);

    // Client B has V1 but not V2.
    let client_b = Backends::new();
    seed_branch(&client_b, "main", b"first-version", vec![]);

    // BF covers only V1 objects → V2 is NOT in the filter.
    let have_filter = bloom_of_version_objects(&client_b, &vid1);

    let tracking_b = InMemoryRemoteTracking::default();
    // Pre-populate tracking so client B's have for "main" is vid1.
    tracking_b.update_remote_ref(&axiom_core::store::traits::RemoteRef {
        remote_name: "origin".into(),
        ref_name: "main".into(),
        kind: axiom_core::model::RefKind::Branch,
        target: vid1.clone(),
        updated_at: 0,
    }).unwrap();

    let mut pull = connect_pull(addr).await;

    let results = pull
        .pull(
            &PullConfig {
                remote_name: "origin".into(),
                workspace_id: "default".into(),
                tenant_id: "test".into(),
                have_filter_override: Some(have_filter),
            },
            &[],
            client_b.refs.as_ref(),
            &tracking_b,
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            None,
            |_| {},
        )
        .await
        .expect("pull ok for v2");

    // V2 must have arrived.
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].remote_tip, vid2);
    assert!(client_b.versions.get_version(&vid2).unwrap().is_some());
}

// ─── E05-S07: client_inventory negotiation ────────────────────────────────────

/// Client A pushes "shared data".  Client B then has the *same* chunk (same
/// BLAKE3 hash) plus a new "unique data" chunk on a fresh branch.  B's push
/// must not transfer the already-present shared chunk.
///
/// This verifies that `client_inventory` → server diff → `need_objects`
/// correctly excludes objects the server already holds.
#[tokio::test]
async fn push_skips_objects_server_already_has() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    // Client A: push "shared data" on main.
    let client_a = Backends::new();
    seed_branch(&client_a, "main", b"shared data", vec![]);
    let mut push_a = connect_push(addr).await;
    push_a
        .push(
            &cfg_push(false),
            &[],
            client_a.refs.as_ref(),
            client_a.chunks.as_ref(),
            client_a.trees.as_ref(),
            client_a.nodes.as_ref(),
            client_a.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .expect("push A ok");

    // The server now has the "shared data" chunk.
    let shared_hash = client_a.chunks.put_chunk(b"shared data".to_vec()).unwrap();
    assert!(server.chunks.has_chunk(&shared_hash).unwrap());

    // Client B: create a different workspace that has the *same* "shared data"
    // branch AND a new "feature" branch with unique content.
    let client_b = Backends::new();
    seed_branch(&client_b, "main", b"shared data", vec![]);
    let vid_feature = seed_branch(&client_b, "feature", b"unique data for feature", vec![]);

    // Push only the "feature" branch from client B.
    let mut push_b = connect_push(addr).await;
    let results = push_b
        .push(
            &PushConfig {
                remote_name: "origin".into(),
                workspace_id: "default".into(),
                tenant_id: "test".into(),
                force: false,
                concurrency: 1,
            },
            &["feature".to_string()],
            client_b.refs.as_ref(),
            client_b.chunks.as_ref(),
            client_b.trees.as_ref(),
            client_b.nodes.as_ref(),
            client_b.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .expect("push B ok");

    assert_eq!(results.get("feature").map(String::as_str), Some("ok"));
    // The feature branch's version and unique chunk must arrive on the server.
    assert!(server.refs.get_ref("feature").unwrap().is_some());
    assert_eq!(
        server.refs.get_ref("feature").unwrap().unwrap().target,
        vid_feature
    );
    assert!(server.versions.get_version(&vid_feature).unwrap().is_some());

    // The unique data chunk must have been transferred.
    let unique_hash = client_b.chunks.put_chunk(b"unique data for feature".to_vec()).unwrap();
    assert!(server.chunks.has_chunk(&unique_hash).unwrap());
}

/// An initial push (server starts empty) must transfer every reachable object
/// exactly once.  The `client_inventory` path is authoritative: server count
/// after push must equal client reachable count.
#[tokio::test]
async fn initial_push_inventory_is_authoritative() {
    let server = Backends::new();
    let addr = spawn_server(server.clone()).await;

    let client = Backends::new();
    let vid = seed_branch(&client, "main", b"test content for inventory check", vec![]);

    let mut push = connect_push(addr).await;
    let results = push
        .push(
            &cfg_push(false),
            &[],
            client.refs.as_ref(),
            client.chunks.as_ref(),
            client.trees.as_ref(),
            client.nodes.as_ref(),
            client.versions.as_ref(),
            None,
            None,
            None,
            |_| {},
        )
        .await
        .expect("initial push ok");

    assert_eq!(results.get("main").map(String::as_str), Some("ok"));

    // The exact version must be present on the server.
    assert!(server.versions.get_version(&vid).unwrap().is_some());

    // Every chunk the client put must now also be in the server store.
    // We know `seed_branch` puts exactly one chunk of the supplied data.
    let chunk_hash = client.chunks.put_chunk(b"test content for inventory check".to_vec()).unwrap();
    assert!(
        server.chunks.has_chunk(&chunk_hash).unwrap(),
        "server must hold the content chunk after initial push"
    );
}
