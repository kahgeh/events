//! Actor types for audit trail tracking.
//!
//! This module defines the actor types that can initiate events in the system.
//! Every event should have an actor_id and actor_type to enable audit queries
//! like "who did what when".

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// The type of actor that initiated an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorType {
    /// End user authenticated via Clerk.
    /// Actor ID format: `user_xxxxx` (Clerk user ID)
    User,

    /// Internal system process (projector, scheduler, self-healing).
    /// Actor ID format: `system:<component-name>` (e.g., `system:provisioning-projector`)
    System,

    /// Admin user accessing via admin portal.
    /// Actor ID format: `admin_xxxxx` (reserved for future use)
    Admin,

    /// External API caller using API key.
    /// Actor ID format: `api_key:<key-prefix>` (reserved for future use)
    ApiKey,
}

impl ActorType {
    /// Returns the string representation of the actor type.
    pub fn as_str(&self) -> &'static str {
        match self {
            ActorType::User => "user",
            ActorType::System => "system",
            ActorType::Admin => "admin",
            ActorType::ApiKey => "api_key",
        }
    }
}

impl fmt::Display for ActorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for ActorType {
    type Err = ActorTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "user" => Ok(ActorType::User),
            "system" => Ok(ActorType::System),
            "admin" => Ok(ActorType::Admin),
            "api_key" => Ok(ActorType::ApiKey),
            _ => Err(ActorTypeParseError(s.to_string())),
        }
    }
}

/// Error returned when parsing an invalid actor type string.
#[derive(Debug, Clone)]
pub struct ActorTypeParseError(String);

impl fmt::Display for ActorTypeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid actor type: {}", self.0)
    }
}

impl std::error::Error for ActorTypeParseError {}
