pub mod broadcast;
pub mod catalog;
pub mod completions_store;
pub mod error;
pub mod eventstore;
pub mod migration;
pub mod pool;
pub mod projector;
pub mod rotation;
pub mod runtime;
pub mod validation;

// Re-exports
pub use broadcast::{
    create_broadcast_system, create_broadcast_system_with_capacity, CompletionBroadcastLoop,
    CompletionEvent, CompletionSendError, CompletionSender, CompletionSubscriber,
};
pub use catalog::{Catalog, ConsumerOffset, PartitionRef, PartitionedCursor, StreamHead};
pub use completions_store::{Completion, CompletionStatus, CompletionsStore};
pub use error::{EsError, Result};
pub use eventstore::{AppendResult, EventEnvelope, EventStore, ExpectedVersion, NewEvent};
pub use pool::{DatabaseInstanceStats, DatabasePool, PoolStats, PooledConnection};
pub use projector::{
    acquire_lease, bootstrap_cursor, checkpoint, is_lease_valid, release_lease, renew_lease,
    with_projection_tx, IdempotentProcessor, Projector, ProjectorHandler, ProjectorHandlerError,
};
pub use rotation::{floor_to_window_ms, label_for, RotationPolicy};
pub use runtime::{EventsRuntime, RuntimeConfig, DEFAULT_COMPLETIONS_TTL};
