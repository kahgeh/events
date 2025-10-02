use thiserror::Error;

#[derive(Debug, Error)]
pub enum EsError {
    #[error("Database error: {0}")]
    Db(#[from] turso::Error),

    #[error("Concurrency conflict: expected version {expected}, but was {actual}")]
    Concurrency {
        expected: i64,
        actual: i64,
        stream_id: String,
    },

    #[error("Payload too large: {size} bytes exceeds maximum {max}")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("UUID error: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("Time error: {0}")]
    Time(#[from] time::error::ComponentRange),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Invalid partition state: {0}")]
    InvalidPartition(String),

    #[error("Cursor error: {0}")]
    Cursor(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Invalid table name: {0}")]
    InvalidTableName(String),
}

pub type Result<T> = std::result::Result<T, EsError>;
