//! Projection scheduling is application-owned.
//!
//! The events crate exposes bounded owner-log reads through `OwnerEventStore`.
//! Applications should store projection offsets and active workflow state in
//! their own database, in the same transaction as their read-model updates.
