//! Projection scheduling is application-owned.
//!
//! The events crate exposes bounded event-log reads through `EventLog`.
//! Applications should store projection offsets and active workflow state in
//! their own database, in the same transaction as their read-model updates.
