//! Notifications store for async request tracking
//!
//! This module provides a separate Turso database for storing progress notifications
//! with TTL-based expiration. It enables FOH instances to:
//! 1. Query progress status on SSE reconnection
//! 2. Get the latest notification for a request without polling the main events DB
//!
//! Notifications are transient coordination signals, not domain events.
//! They are separate from EventStream, which is the durable event stream.

use crate::broadcast::{EventKind, ItemProgress, StreamEvent};
use crate::{EsError, Result};
use std::path::Path;
use std::time::Duration;
use turso::Database;

const DEFAULT_TTL_SECS: u64 = 300; // 5 minutes

/// Store for managing progress notifications with TTL
pub struct NotificationsStore {
    db: Database,
    ttl: Duration,
}

impl NotificationsStore {
    /// Create a new notifications store at the specified path
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be created or migrations fail.
    pub async fn new(path: &Path) -> Result<Self> {
        Self::with_ttl(path, Duration::from_secs(DEFAULT_TTL_SECS)).await
    }

    /// Create a new notifications store with a custom TTL
    pub async fn with_ttl(path: &Path, ttl: Duration) -> Result<Self> {
        let db_path = path.join("notifications_store.db");
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| EsError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", db_path)))?;
        let db = turso::Builder::new_local(db_path_str).build().await?;

        let conn = db.connect()?;

        // Run migrations - drop old table if schema changed
        conn.execute("DROP TABLE IF EXISTS completions", ())
            .await
            .ok();

        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS stream_events (
                request_id TEXT PRIMARY KEY,
                stream_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                kind TEXT NOT NULL,
                current_step INTEGER NOT NULL DEFAULT 0,
                total_steps INTEGER NOT NULL DEFAULT 0,
                step_name TEXT NOT NULL DEFAULT '',
                payload TEXT,
                error_message TEXT,
                retriable INTEGER DEFAULT 0,
                items TEXT,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
            "#,
            (),
        )
        .await?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_stream_events_expires ON stream_events(expires_at)",
            (),
        )
        .await?;

        Ok(Self { db, ttl })
    }

    /// Record a stream event for a request
    ///
    /// This uses UPSERT semantics - newer events replace older ones.
    /// This should be called by the event handler after each progress/completion event.
    pub async fn record(&self, event: &StreamEvent) -> Result<()> {
        let conn = self.db.connect()?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let expires_at = now + self.ttl.as_secs() as i64;

        let kind = match event.kind {
            EventKind::Progress => "progress",
            EventKind::Completed => "completed",
            EventKind::Failed => "failed",
        };

        let payload = event.payload.as_ref().map(|p| p.to_string());
        let retriable = event.retriable.map(|r| if r { 1i64 } else { 0i64 });
        let items = if event.items.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&event.items).unwrap_or_default())
        };

        conn.execute(
            r#"
            INSERT INTO stream_events
            (request_id, stream_id, timestamp, kind, current_step, total_steps, step_name,
             payload, error_message, retriable, items, created_at, expires_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(request_id) DO UPDATE SET
                stream_id = excluded.stream_id,
                timestamp = excluded.timestamp,
                kind = excluded.kind,
                current_step = excluded.current_step,
                total_steps = excluded.total_steps,
                step_name = excluded.step_name,
                payload = excluded.payload,
                error_message = excluded.error_message,
                retriable = excluded.retriable,
                items = excluded.items,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at
            "#,
            (
                event.request_id.as_str(),
                event.stream_id.as_str(),
                event.timestamp,
                kind,
                event.current_step as i64,
                event.total_steps as i64,
                event.step_name.as_str(),
                turso::Value::from(payload.as_deref()),
                turso::Value::from(event.error_message.as_deref()),
                turso::Value::from(retriable),
                turso::Value::from(items.as_deref()),
                now,
                expires_at,
            ),
        )
        .await?;

        tracing::debug!(
            request_id = %event.request_id,
            kind = %kind,
            step = %event.current_step,
            expires_at = %expires_at,
            "Recorded stream event"
        );

        Ok(())
    }

    /// Look up the latest event for a request_id
    ///
    /// Returns None if:
    /// - No event exists for this request
    /// - The event has expired (TTL exceeded)
    pub async fn get(&self, request_id: &str) -> Result<Option<StreamEvent>> {
        let conn = self.db.connect()?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        let mut rows = conn
            .query(
                r#"
                SELECT stream_id, timestamp, kind, current_step, total_steps, step_name,
                       payload, error_message, retriable, items
                FROM stream_events
                WHERE request_id = ?1 AND expires_at > ?2
                "#,
                (request_id, now),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        let stream_id = row
            .get_value(0)?
            .as_text()
            .ok_or_else(|| EsError::Cursor("Expected text for stream_id".to_string()))?
            .to_string();

        let timestamp = row
            .get_value(1)?
            .as_integer()
            .copied()
            .ok_or_else(|| EsError::Cursor("Expected integer for timestamp".to_string()))?;

        let kind_value = row.get_value(2)?;
        let kind_str = kind_value
            .as_text()
            .ok_or_else(|| EsError::Cursor("Expected text for kind".to_string()))?;

        let kind = match kind_str.as_str() {
            "progress" => EventKind::Progress,
            "completed" => EventKind::Completed,
            _ => EventKind::Failed,
        };

        let current_step = row.get_value(3)?.as_integer().copied().unwrap_or(0) as u32;

        let total_steps = row.get_value(4)?.as_integer().copied().unwrap_or(0) as u32;

        let step_name = row
            .get_value(5)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .unwrap_or_default();

        let payload = row
            .get_value(6)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .and_then(|p| serde_json::from_str(&p).ok());

        let error_message = row
            .get_value(7)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));

        let retriable = row
            .get_value(8)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .map(|r| r != 0);

        let items: Vec<ItemProgress> = row
            .get_value(9)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()))
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        Ok(Some(StreamEvent {
            request_id: request_id.to_string(),
            stream_id,
            timestamp,
            kind,
            current_step,
            total_steps,
            step_name,
            payload,
            error_message,
            retriable,
            items,
        }))
    }

    /// Clean up expired events
    ///
    /// Returns the number of deleted records.
    /// This should be called periodically (e.g., every 60 seconds).
    pub async fn cleanup(&self) -> Result<u64> {
        let conn = self.db.connect()?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        let deleted = conn
            .execute("DELETE FROM stream_events WHERE expires_at < ?1", (now,))
            .await?;

        if deleted > 0 {
            tracing::debug!(deleted = deleted, "Cleaned up expired stream events");
        }

        Ok(deleted)
    }
}

#[cfg(test)]
#[path = "notifications_store_tests.rs"]
mod notifications_store_tests;
