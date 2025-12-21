//! Notifications store for async request tracking
//!
//! This module provides a separate Turso database for storing progress notifications
//! with TTL-based expiration. It enables FOH instances to:
//! 1. Query progress status on SSE reconnection
//! 2. Get the latest notification for a request without polling the main events DB
//!
//! Notifications are transient coordination signals, not domain events.
//! They are separate from the EventStore which is the permanent event sourcing journal.

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
    /// This should be called by the projector after each progress/completion event.
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

        let current_step = row
            .get_value(3)?
            .as_integer()
            .copied()
            .unwrap_or(0) as u32;

        let total_steps = row
            .get_value(4)?
            .as_integer()
            .copied()
            .unwrap_or(0) as u32;

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
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_record_and_get_progress() {
        let temp_dir = TempDir::new().unwrap();
        let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

        let event = StreamEvent::progress(
            "req-123".to_string(),
            "stream-1".to_string(),
            1,
            3,
            "Creating app".to_string(),
        );

        store.record(&event).await.unwrap();

        let result = store.get("req-123").await.unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        assert_eq!(retrieved.request_id, "req-123");
        assert_eq!(retrieved.stream_id, "stream-1");
        assert_eq!(retrieved.current_step, 1);
        assert_eq!(retrieved.total_steps, 3);
        assert_eq!(retrieved.kind, EventKind::Progress);
    }

    #[tokio::test]
    async fn test_record_and_get_completed() {
        let temp_dir = TempDir::new().unwrap();
        let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

        let event = StreamEvent::completed(
            "req-456".to_string(),
            "stream-2".to_string(),
            3,
            Some(serde_json::json!({"machine_id": "m-789"})),
        );

        store.record(&event).await.unwrap();

        let result = store.get("req-456").await.unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        assert_eq!(retrieved.kind, EventKind::Completed);
        assert!(retrieved.payload.is_some());
        assert_eq!(retrieved.payload.unwrap()["machine_id"], "m-789");
    }

    #[tokio::test]
    async fn test_record_and_get_failed() {
        let temp_dir = TempDir::new().unwrap();
        let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

        let event = StreamEvent::failed(
            "req-789".to_string(),
            "stream-3".to_string(),
            2,
            3,
            "Network error".to_string(),
            true,
        );

        store.record(&event).await.unwrap();

        let result = store.get("req-789").await.unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        assert_eq!(retrieved.kind, EventKind::Failed);
        assert_eq!(retrieved.error_message, Some("Network error".to_string()));
        assert_eq!(retrieved.retriable, Some(true));
    }

    #[tokio::test]
    async fn test_upsert_updates_event() {
        let temp_dir = TempDir::new().unwrap();
        let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

        // Record progress event
        let event1 = StreamEvent::progress(
            "req-upsert".to_string(),
            "stream-1".to_string(),
            1,
            3,
            "Step 1".to_string(),
        );
        store.record(&event1).await.unwrap();

        // Record completion event for same request
        let event2 = StreamEvent::completed(
            "req-upsert".to_string(),
            "stream-1".to_string(),
            3,
            Some(serde_json::json!({"result": "success"})),
        );
        store.record(&event2).await.unwrap();

        // Should get the latest (completion) event
        let result = store.get("req-upsert").await.unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        assert_eq!(retrieved.kind, EventKind::Completed);
        assert_eq!(retrieved.current_step, 3);
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let store = NotificationsStore::new(temp_dir.path()).await.unwrap();

        let result = store.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let temp_dir = TempDir::new().unwrap();
        let store = NotificationsStore::with_ttl(temp_dir.path(), Duration::from_secs(1))
            .await
            .unwrap();

        let event = StreamEvent::progress(
            "req-ttl".to_string(),
            "stream-1".to_string(),
            1,
            1,
            "Testing".to_string(),
        );

        store.record(&event).await.unwrap();

        // Should exist immediately
        assert!(store.get("req-ttl").await.unwrap().is_some());

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Should not exist after TTL
        assert!(store.get("req-ttl").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let store = NotificationsStore::with_ttl(temp_dir.path(), Duration::from_secs(1))
            .await
            .unwrap();

        // Record multiple events
        for i in 0..5 {
            let event = StreamEvent::progress(
                format!("req-{}", i),
                "stream-1".to_string(),
                1,
                1,
                "Testing".to_string(),
            );
            store.record(&event).await.unwrap();
        }

        // Wait for TTL
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Cleanup should delete all expired
        let deleted = store.cleanup().await.unwrap();
        assert!(deleted >= 5, "Expected at least 5 deleted, got {}", deleted);

        // Cleanup again should delete nothing
        let deleted = store.cleanup().await.unwrap();
        assert_eq!(deleted, 0);
    }
}
