pub mod hash;
pub mod chunk;
pub mod tree;
pub mod node;
pub mod version;
pub mod refs;
pub mod diff;

// Re-export core types for convenience.
pub use hash::{ChunkHash, VersionId, hash_bytes, hash_children, current_timestamp};
pub use chunk::{ChunkRecord, ChunkDescriptor};
pub use tree::{TreeNode, TreeNodeKind, FileObject};
pub use node::{NodeEntry, NodeKind};
pub use version::VersionNode;
pub use refs::{Ref, RefKind};
pub use diff::{DiffResult, DiffEntry, DiffKind};
