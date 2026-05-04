use thiserror::Error;

pub type CasResult<T> = Result<T, CasError>;

#[derive(Error, Debug)]
pub enum CasError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists")]
    AlreadyExists,

    #[error("hash mismatch")]
    HashMismatch,

    #[error("invalid object: {0}")]
    InvalidObject(String),

    #[error("invalid ref: {0}")]
    InvalidRef(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("tenant not found: {0}")]
    TenantNotFound(String),

    #[error("workspace not found: {0}")]
    WorkspaceNotFound(String),

    #[error("rate limit exceeded: {0}")]
    RateLimitExceeded(String),

    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),

    #[error("sync error: {0}")]
    SyncError(String),

    #[error("non-fast-forward: {0}")]
    NonFastForward(String),

    #[error("serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}