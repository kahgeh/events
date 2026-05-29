use thiserror::Error;

#[derive(Debug, Error)]
pub enum EsError {
    #[error("Database error: {0}")]
    Db(#[from] turso::Error),

    #[error("Incorrect event version: expected {expected}, but was {actual}")]
    IncorrectEventVersion { expected: i64, actual: i64 },

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

    #[error("Invalid event stream version: {0}")]
    InvalidVersion(String),

    #[error("Invalid workflow metadata: {0}")]
    InvalidWorkflowMetadata(String),

    #[error("Invalid {field}: {value}")]
    InvalidSafeName { field: String, value: String },

    #[error("Invalid read limit: {limit}; expected 1..={max}")]
    InvalidReadLimit { limit: usize, max: usize },

    /// Events were durably committed to the partition but the catalog
    /// event-stream catalog update failed after exhausting retries. The store is
    /// in an inconsistent state: the event stream has advanced but the version
    /// index has not.
    ///
    /// **This is not retryable.** Callers must:
    /// 1. Investigate and address the underlying cause (e.g. catalog DB
    ///    connectivity, disk pressure, permissions).
    /// 2. Rebuild the affected event-stream catalog before attempting to append more events.
    ///
    /// Continuing to write without resolving the cause risks duplicate event-stream versions.
    #[error("Catalog drift: events committed to partition but event-stream catalog update failed at version {committed_version}")]
    CatalogDrift {
        committed_version: i64,
        source: Box<EsError>,
    },
}

pub type Result<T> = std::result::Result<T, EsError>;
