use serde::{Deserialize, Serialize};

use super::hash::VersionId;

/// The kind of a ref.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefKind {
    /// A mutable branch that advances with new commits.
    Branch,
    /// An immutable tag that always points to the same version.
    Tag,
}

/// A named reference pointing to a version.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ref {
    /// Name of the ref, e.g. "main", "v1.0".
    pub name: String,
    /// The kind of ref.
    pub kind: RefKind,
    /// The version this ref points to.
    pub target: VersionId,
}
