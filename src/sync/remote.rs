/// Remote configuration management.
///
/// Re-exports [`Remote`] and [`RemoteRepo`] from the store traits layer so
/// callers can import everything from `sync::remote` without depending on
/// `store::traits` directly.
pub use crate::store::traits::{Remote, RemoteRepo};
