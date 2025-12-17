//! Completions store for async request tracking
//!
//! This module provides a separate Turso database for storing completion records
//! with TTL-based expiration. It enables FOH instances to:
//! 1. Query completion status on SSE reconnection
//! 2. Avoid polling the events DB for completion status
//!
//! Completions are transient coordination signals, not domain events.

use crate::{EsError, Result};
use std::path::Path;
use std::time::Duration;
use turso::Database;

const DEFAULT_TTL_SECS: u64 = 300; // 5 minutes

/// Completion status for an async request
#[derive(Debug, Clone)]
pub enum CompletionStatus {
    /// Request completed successfully
    Success {
        /// Domain-specific payload as JSON
        payload: Option<serde_json::Value>,
    },
    /// Request failed
    Failed {
        /// Error message
        error: String,
        /// Whether the operation can be retried
        retriable: bool,
    },
}

/// A completion record for an async request
#[derive(Debug, Clone)]
pub struct Completion {
    /// The request ID that triggered this completion
    pub request_id: String,
    /// The completion status
    pub status: CompletionStatus,
}

/// Store for managing async request completions with TTL
pub struct CompletionsStore {
    db: Database,
    ttl: Duration,
}

impl CompletionsStore {
    /// Create a new completions store at the specified path
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be created or migrations fail.
    pub async fn new(path: &Path) -> Result<Self> {
        Self::with_ttl(path, Duration::from_secs(DEFAULT_TTL_SECS)).await
    }

    /// Create a new completions store with a custom TTL
    pub async fn with_ttl(path: &Path, ttl: Duration) -> Result<Self> {
        let db_path = path.join("completions.db");
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| EsError::InvalidPath(format!("Invalid UTF-8 in path: {:?}", db_path)))?;
        let db = turso::Builder::new_local(db_path_str).build().await?;

        let conn = db.connect()?;

        // Run migrations
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS completions (
                request_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                payload TEXT,
                error_message TEXT,
                retriable INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            )
            "#,
            (),
        )
        .await?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_completions_expires ON completions(expires_at)",
            (),
        )
        .await?;

        Ok(Self { db, ttl })
    }

    /// Record a completion for a request
    ///
    /// This should be called by the projector after processing an event
    /// that has a request_id in its metadata.
    pub async fn record(&self, completion: &Completion) -> Result<()> {
        let conn = self.db.connect()?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let expires_at = now + self.ttl.as_secs() as i64;

        let (status, payload, error_message, retriable) = match &completion.status {
            CompletionStatus::Success { payload } => (
                "success",
                payload.as_ref().map(|p| p.to_string()),
                None::<String>,
                0i64,
            ),
            CompletionStatus::Failed { error, retriable } => (
                "failed",
                None,
                Some(error.clone()),
                if *retriable { 1i64 } else { 0i64 },
            ),
        };

        conn.execute(
            r#"
            INSERT INTO completions
            (request_id, status, payload, error_message, retriable, created_at, expires_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(request_id) DO UPDATE SET
                status = excluded.status,
                payload = excluded.payload,
                error_message = excluded.error_message,
                retriable = excluded.retriable,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at
            "#,
            (
                completion.request_id.as_str(),
                status,
                turso::Value::from(payload.as_deref()),
                turso::Value::from(error_message.as_deref()),
                retriable,
                now,
                expires_at,
            ),
        )
        .await?;

        tracing::debug!(
            request_id = %completion.request_id,
            status = %status,
            expires_at = %expires_at,
            "Recorded completion"
        );

        Ok(())
    }

    /// Look up a completion by request_id
    ///
    /// Returns None if:
    /// - The completion doesn't exist
    /// - The completion has expired (TTL exceeded)
    pub async fn get(&self, request_id: &str) -> Result<Option<Completion>> {
        let conn = self.db.connect()?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        let mut rows = conn
            .query(
                r#"
                SELECT status, payload, error_message, retriable
                FROM completions
                WHERE request_id = ?1 AND expires_at > ?2
                "#,
                (request_id, now),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        let status = row
            .get_value(0)?
            .as_text()
            .ok_or_else(|| EsError::Cursor("Expected text for status column".to_string()))?
            .to_string();

        let payload = row
            .get_value(1)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));

        let error_message = row
            .get_value(2)
            .ok()
            .and_then(|v| v.as_text().map(|s| s.to_string()));

        let retriable = row
            .get_value(3)
            .ok()
            .and_then(|v| v.as_integer().copied())
            .unwrap_or(0);

        let completion_status = if status == "success" {
            CompletionStatus::Success {
                payload: payload.and_then(|p| serde_json::from_str(&p).ok()),
            }
        } else {
            CompletionStatus::Failed {
                error: error_message.unwrap_or_default(),
                retriable: retriable != 0,
            }
        };

        Ok(Some(Completion {
            request_id: request_id.to_string(),
            status: completion_status,
        }))
    }

    /// Clean up expired completions
    ///
    /// Returns the number of deleted records.
    /// This should be called periodically (e.g., every 60 seconds).
    pub async fn cleanup(&self) -> Result<u64> {
        let conn = self.db.connect()?;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        let deleted = conn
            .execute("DELETE FROM completions WHERE expires_at < ?1", (now,))
            .await?;

        if deleted > 0 {
            tracing::debug!(deleted = deleted, "Cleaned up expired completions");
        }

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_record_and_get_success() {
        let temp_dir = TempDir::new().unwrap();
        let store = CompletionsStore::new(temp_dir.path()).await.unwrap();

        let completion = Completion {
            request_id: "req-123".to_string(),
            status: CompletionStatus::Success {
                payload: Some(serde_json::json!({"machine_id": "m-456"})),
            },
        };

        store.record(&completion).await.unwrap();

        let result = store.get("req-123").await.unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        assert_eq!(retrieved.request_id, "req-123");

        if let CompletionStatus::Success { payload } = retrieved.status {
            assert!(payload.is_some());
            let p = payload.unwrap();
            assert_eq!(p["machine_id"], "m-456");
        } else {
            panic!("Expected Success status");
        }
    }

    #[tokio::test]
    async fn test_record_and_get_failed() {
        let temp_dir = TempDir::new().unwrap();
        let store = CompletionsStore::new(temp_dir.path()).await.unwrap();

        let completion = Completion {
            request_id: "req-456".to_string(),
            status: CompletionStatus::Failed {
                error: "Machine creation failed".to_string(),
                retriable: true,
            },
        };

        store.record(&completion).await.unwrap();

        let result = store.get("req-456").await.unwrap();
        assert!(result.is_some());

        let retrieved = result.unwrap();
        if let CompletionStatus::Failed { error, retriable } = retrieved.status {
            assert_eq!(error, "Machine creation failed");
            assert!(retriable);
        } else {
            panic!("Expected Failed status");
        }
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let store = CompletionsStore::new(temp_dir.path()).await.unwrap();

        let result = store.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let temp_dir = TempDir::new().unwrap();
        // Use 1 second TTL for testing
        let store = CompletionsStore::with_ttl(temp_dir.path(), Duration::from_secs(1))
            .await
            .unwrap();

        let completion = Completion {
            request_id: "req-789".to_string(),
            status: CompletionStatus::Success { payload: None },
        };

        store.record(&completion).await.unwrap();

        // Should exist immediately
        assert!(store.get("req-789").await.unwrap().is_some());

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Should not exist after TTL
        assert!(store.get("req-789").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let store = CompletionsStore::with_ttl(temp_dir.path(), Duration::from_secs(1))
            .await
            .unwrap();

        // Record multiple completions
        for i in 0..5 {
            let completion = Completion {
                request_id: format!("req-{}", i),
                status: CompletionStatus::Success { payload: None },
            };
            store.record(&completion).await.unwrap();
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
