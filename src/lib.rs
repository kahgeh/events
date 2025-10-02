pub mod catalog;
pub mod error;
pub mod eventstore;
pub mod migration;
pub mod pool;
pub mod projector;
pub mod rotation;
pub mod validation;

// Re-exports
pub use catalog::{Catalog, ConsumerOffset, PartitionRef, PartitionedCursor, StreamHead};
pub use error::{EsError, Result};
pub use eventstore::{AppendResult, EventEnvelope, EventStore, ExpectedVersion, NewEvent};
pub use pool::{DatabaseInstanceStats, DatabasePool, PoolStats, PooledConnection};
pub use projector::{
    acquire_lease, bootstrap_cursor, checkpoint, is_lease_valid, release_lease, renew_lease,
    with_projection_tx, IdempotentProcessor, Projector,
};
pub use rotation::{floor_to_window_ms, label_for, RotationPolicy};
