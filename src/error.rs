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

    /// Events were durably committed to the partition but the catalog
    /// `stream_heads` update failed after exhausting retries. The store is
    /// in an inconsistent state: the event log has advanced but the version
    /// index has not.
    ///
    /// **This is not retryable.** Callers must:
    /// 1. Investigate and address the underlying cause (e.g. catalog DB
    ///    connectivity, disk pressure, permissions).
    /// 2. Call [`EventStore::reconcile_stream_head`] for the affected stream
    ///    (or [`EventStore::recover_all_stale_heads`] for a full sweep)
    ///    before attempting to append more events.
    ///
    /// Continuing to write without resolving the cause and recovering risks
    /// duplicate stream versions.
    #[error("Catalog drift: events committed to partition but stream_heads update failed for stream {stream_id} at version {committed_version} — resolve underlying cause and call reconcile_stream_head before further writes")]
    CatalogDrift {
        stream_id: String,
        committed_version: i64,
        source: Box<EsError>,
    },
}

pub type Result<T> = std::result::Result<T, EsError>;
