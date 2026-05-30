//! Projection scheduling is application-owned.
//!
//! The events crate exposes bounded event-stream reads through `EventStream`.
//! Applications should store handler checkpoints and workflow failure state in
//! their own database, in the same transaction as their read-model updates.
