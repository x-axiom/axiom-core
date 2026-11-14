pub mod error;
pub mod model;
pub mod store;
pub mod chunker;
pub mod merkle;
pub mod namespace;
pub mod commit;
pub mod checkout;
pub mod diff_engine;
pub mod working_tree;
pub mod api;
pub mod sync;

/// gRPC sync server bootstrap (cloud mode).
#[cfg(feature = "cloud")]
pub mod grpc;

// Keep legacy modules for backward compatibility during transition.
pub mod cas;
pub mod version;
pub mod tenant;
pub mod audit;
pub mod auth;
pub mod quota;
pub mod gc;
pub mod telemetry;