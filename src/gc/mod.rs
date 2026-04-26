//! Garbage collection and object lifecycle management.

pub mod recycle_bin;
#[cfg(feature = "fdb")]
pub mod refcount;
#[cfg(feature = "fdb")]
pub mod sweep;
#[cfg(feature = "fdb")]
pub mod scheduler;
#[cfg(feature = "cloud")]
pub mod lifecycle;
