/// Reachable-object BFS algorithm (no gRPC deps, always compiled).
pub mod reachable;

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

/// AXPK pack format encoder / decoder (requires zstd — cloud only).
#[cfg(feature = "cloud")]
pub mod pack;

/// Generated gRPC types and service traits for `axiom.sync.v1`.
#[cfg(feature = "cloud")]
pub mod proto {
    tonic::include_proto!("axiom.sync.v1");
}

#[cfg(feature = "cloud")]
pub use proto::sync_service_client::SyncServiceClient;
#[cfg(feature = "cloud")]
pub use proto::sync_service_server::{SyncService, SyncServiceServer};
