/// Generated gRPC types and service traits for `axiom.sync.v1`.
pub mod proto {
    tonic::include_proto!("axiom.sync.v1");
}

pub mod pack;
pub mod reachable;

pub use proto::sync_service_client::SyncServiceClient;
pub use proto::sync_service_server::{SyncService, SyncServiceServer};
