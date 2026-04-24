/// Reachable-object BFS algorithm (no gRPC deps, always compiled).
pub mod reachable;

/// Fast-forward detection for ref updates (always compiled).
pub mod fast_forward;

/// Sync session log helpers (always compiled — uses SQLite metadata store).
pub mod session;

/// Remote configuration management (CRUD for remote endpoints).
pub mod remote;

/// Remote-tracking refs management.
pub mod remote_refs;

/// Push client — four-step gRPC push protocol.
#[cfg(feature = "cloud")]
pub mod push_client;

/// Push server — gRPC server-side handler for Push RPCs.
#[cfg(feature = "cloud")]
pub mod push_server;

/// Pull client — three-step gRPC pull protocol.
#[cfg(feature = "cloud")]
pub mod pull_client;

/// Pull server — gRPC server-side handler for Pull RPCs.
#[cfg(feature = "cloud")]
pub mod pull_server;

/// AXPK pack format encoder / decoder (requires zstd — cloud only).
#[cfg(feature = "cloud")]
pub mod pack;

/// Clone client — first-time full repository clone (E05-S01).
#[cfg(feature = "cloud")]
pub mod clone_client;

/// Shallow clone boundary tracking (E05-S02).
#[cfg(feature = "cloud")]
pub mod shallow;

/// Resume/checkpoint helpers for interrupted push/pull (E05-S03).
#[cfg(feature = "cloud")]
pub mod resume;

/// Generated gRPC types and service traits for `axiom.sync.v1`.
#[cfg(feature = "cloud")]
pub mod proto {
    tonic::include_proto!("axiom.sync.v1");
}

#[cfg(feature = "cloud")]
pub use proto::sync_service_client::SyncServiceClient;
#[cfg(feature = "cloud")]
pub use proto::sync_service_server::{SyncService, SyncServiceServer};
