/// Reachable-object BFS algorithm (no gRPC deps, always compiled).
pub mod reachable;

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
