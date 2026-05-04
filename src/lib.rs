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

// Keep legacy modules for backward compatibility during transition.
pub mod cas;
pub mod version;
#[cfg(feature = "fdb")]
pub mod tenant;
#[cfg(feature = "fdb")]
pub mod auth;
pub mod gc;