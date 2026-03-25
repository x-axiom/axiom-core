use thiserror::Error;

pub type CasResult<T> = Result<T, CasError>;

#[derive(Error, Debug)]
pub enum CasError {
    #[error("object not found")]
    NotFound,

    #[error("already exists")]
    AlreadyExists,

    #[error("hash mismatch")]
    HashMismatch,

    #[error("invalid object: {0}")]
    InvalidObject(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}