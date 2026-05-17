pub mod actor;
pub mod broadcast;
pub mod catalog;
pub mod error;
pub mod eventstore;
pub mod migration;
pub mod notifications_store;
pub mod pool;
pub mod projector;
pub mod rotation;
pub mod runtime;
pub mod validation;

// Re-exports
pub use actor::{
    ActorType, ActorTypeParseError, SYSTEM_PROVISIONING_PROJECTOR, SYSTEM_SELF_HEALER,
};
pub use broadcast::{
    create_broadcast_system, create_broadcast_system_with_capacity, EventKind, ItemProgress,
    ItemStatus, StreamEvent, StreamEventBroadcastLoop, StreamEventSendError, StreamEventSender,
    StreamEventSubscriber,
};
pub use catalog::{Catalog, ConsumerOffset, PartitionRef, PartitionedCursor, StreamHead};
pub use error::{EsError, Result};
pub use eventstore::{AppendResult, EventEnvelope, EventStore, ExpectedVersion, NewEvent};
pub use notifications_store::NotificationsStore;
pub use pool::{DatabaseInstanceStats, DatabasePool, PoolStats, PooledConnection};
pub use projector::{
    bootstrap_cursor, checkpoint, get_active_workflow, with_projection_tx, ActiveWorkflow,
    IdempotentProcessor, Projector, ProjectorBatchOutcome, ProjectorHandler, ProjectorHandlerError,
};
pub use rotation::{floor_to_window_ms, label_for, RotationPolicy};
pub use runtime::{EventsRuntime, RuntimeConfig, DEFAULT_EVENTS_STORE_TTL};
