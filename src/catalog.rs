use crate::{
    pool::{configure_connection, configure_database, DatabasePool},
    EsError, Result,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use turso::Database;

/// Helper function to safely extract text value from database row
fn get_text_safe(row: &turso::Row, index: usize) -> Result<String> {
    row.get_value(index)?
        .as_text()
        .ok_or_else(|| EsError::Migration(format!("Expected text value at column {}", index)))
        .map(|s| s.to_string())
}

/// Helper function to safely extract integer value from database row
fn get_integer_safe(row: &turso::Row, index: usize) -> Result<i64> {
    row.get_value(index)?
        .as_integer()
        .ok_or_else(|| EsError::Migration(format!("Expected integer value at column {}", index)))
        .copied()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionedCursor {
    pub partition: String,
    pub created_at_ms: i64,
    pub sequence: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRef {
    pub name: String,
    pub path: String,
    pub start_ms: i64,
    pub end_ms: Option<i64>,
    pub sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamHead {
    pub stream_id: String,
    pub version: i64,
    pub last_created_at_ms: i64,
    pub last_event_id: uuid::Uuid,
    pub last_partition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerOffset {
    pub consumer: String,
    pub partition: String,
    pub cursor_created_at: i64,
    pub cursor_sequence: i64,
    pub updated_at: i64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    /// Stream ID of the active workflow (if any)
    pub workflow_stream_id: Option<String>,
    /// Event ID of the workflow start event (e.g., PROVISION_REQUESTED)
    pub workflow_event_id: Option<uuid::Uuid>,
}

pub struct Catalog {
    pub(crate) db: Database,
    pool: Option<Arc<DatabasePool>>,
}

impl Catalog {
    /// Opens a catalog database at the specified path.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database path cannot be converted to UTF-8
    /// - The database cannot be created or opened
    /// - Migration fails
    pub async fn open(path: &Path) -> Result<Self> {
        let db_path = path.join("catalog.db");
        let db_path_str = db_path.to_str().ok_or_else(|| {
            EsError::InvalidPath("Catalog path contains invalid UTF-8".to_string())
        })?;

        let db = turso::Builder::new_local(db_path_str).build().await?;
        configure_database(&db).await?;

        // Run migrations
        let conn = db.connect()?;
        configure_connection(&conn).await?;
        crate::migration::catalog_migrations().run(&conn).await?;

        Ok(Self { db, pool: None })
    }

    /// Opens a catalog database using a shared connection pool.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database path cannot be converted to UTF-8
    /// - The database cannot be created or opened
    /// - Migration fails
    pub async fn open_with_pool(path: &Path, pool: Arc<DatabasePool>) -> Result<Self> {
        let db_path = path.join("catalog.db");
        let db_path_str = db_path.to_str().ok_or_else(|| {
            EsError::InvalidPath("Catalog path contains invalid UTF-8".to_string())
        })?;

        let db = turso::Builder::new_local(db_path_str).build().await?;
        configure_database(&db).await?;

        // Run migrations using a connection from the pool
        {
            let conn = pool.get_catalog_connection().await?;
            crate::migration::catalog_migrations().run(&conn).await?;
        }

        Ok(Self {
            db,
            pool: Some(pool),
        })
    }

    /// Gets a connection for catalog operations, using the pool when available
    pub async fn get_connection(&self) -> Result<crate::pool::PooledConnection> {
        match &self.pool {
            Some(pool) => pool.get_catalog_connection().await,
            None => {
                let conn = self.db.connect()?;
                configure_connection(&conn).await?;
                Ok(crate::pool::PooledConnection::from_direct(conn))
            }
        }
    }

    /// Creates or updates a partition record in the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database operations fail
    /// - Partition data is invalid
    pub async fn create_partition(&self, partition: &PartitionRef) -> Result<()> {
        let sealed = if partition.sealed { 1 } else { 0 };
        let conn = self.get_connection().await?;

        conn.execute(
            r#"
            INSERT INTO partitions (name, path, start_ms, end_ms, sealed)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(name) DO UPDATE SET
                path = excluded.path,
                start_ms = excluded.start_ms,
                end_ms = excluded.end_ms,
                sealed = excluded.sealed
            "#,
            (
                partition.name.clone(),
                partition.path.clone(),
                partition.start_ms,
                partition.end_ms,
                sealed,
            ),
        )
        .await?;

        Ok(())
    }

    pub async fn seal_partition(&self, name: &str, end_ms: i64) -> Result<()> {
        let conn = self.get_connection().await?;
        conn.execute(
            "UPDATE partitions SET sealed = 1, end_ms = ?1 WHERE name = ?2",
            (end_ms, name),
        )
        .await?;
        Ok(())
    }

    pub async fn get_active_partition(&self) -> Result<Option<PartitionRef>> {
        let conn = self.get_connection().await?;
        let mut rows = conn.query(
            "SELECT name, path, start_ms, end_ms, sealed FROM partitions WHERE sealed = 0 ORDER BY name DESC LIMIT 1",
            (),
        ).await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        Ok(Some(self.row_to_partition_ref(&row)?))
    }

    pub async fn get_partitions_by_range(
        &self,
        start_ms: i64,
        end_ms: Option<i64>,
    ) -> Result<Vec<PartitionRef>> {
        let conn = self.get_connection().await?;

        let mut rows = match end_ms {
            Some(end) => {
                conn.query(
                    "SELECT name, path, start_ms, end_ms, sealed FROM partitions WHERE start_ms <= ?1 AND (end_ms IS NULL OR end_ms >= ?2) ORDER BY start_ms, name",
                    (start_ms, end),
                ).await?
            }
            None => {
                conn.query(
                    "SELECT name, path, start_ms, end_ms, sealed FROM partitions WHERE start_ms <= ?1 ORDER BY start_ms, name",
                    (start_ms,),
                ).await?
            }
        };

        self.collect_partitions_from_rows(&mut rows).await
    }

    pub async fn get_next_partition(
        &self,
        current_partition: &str,
    ) -> Result<Option<PartitionRef>> {
        let conn = self.get_connection().await?;

        // Validate the current partition exists — fail fast on stale/corrupt checkpoints
        let mut exists = conn
            .query(
                "SELECT 1 FROM partitions WHERE name = ?1",
                (current_partition,),
            )
            .await?;
        if exists.next().await?.is_none() {
            return Err(EsError::InvalidPartition(format!(
                "Partition not found: {}",
                current_partition
            )));
        }

        let mut rows = conn
            .query(
                "SELECT name, path, start_ms, end_ms, sealed FROM partitions WHERE name > ?1 ORDER BY name LIMIT 1",
                (current_partition,),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        Ok(Some(self.row_to_partition_ref(&row)?))
    }

    /// Updates the stream head in the catalog, only advancing the version forward.
    ///
    /// Returns `true` if the row was inserted or updated, `false` if the existing
    /// version was already >= the provided version (no-op).
    pub async fn update_stream_head(
        &self,
        stream_id: &str,
        version: i64,
        created_at_ms: i64,
        event_id: &uuid::Uuid,
        partition: &str,
    ) -> Result<bool> {
        let conn = self.get_connection().await?;
        let rows_affected = conn
            .execute(
                r#"
            INSERT INTO stream_heads (stream_id, version, last_created_at_ms, last_event_id, last_partition)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(stream_id) DO UPDATE SET
                version = excluded.version,
                last_created_at_ms = excluded.last_created_at_ms,
                last_event_id = excluded.last_event_id,
                last_partition = excluded.last_partition
            WHERE excluded.version > stream_heads.version
            "#,
                (stream_id, version, created_at_ms, event_id.to_string(), partition),
            )
            .await?;
        Ok(rows_affected > 0)
    }

    pub async fn get_stream_head(&self, stream_id: &str) -> Result<Option<StreamHead>> {
        let conn = self.get_connection().await?;
        let mut rows = conn.query(
            "SELECT stream_id, version, last_created_at_ms, last_event_id, last_partition FROM stream_heads WHERE stream_id = ?1",
            (stream_id,),
        ).await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        let event_id_str = get_text_safe(&row, 3)?;
        let event_id = uuid::Uuid::parse_str(&event_id_str)?;

        Ok(Some(StreamHead {
            stream_id: get_text_safe(&row, 0)?,
            version: get_integer_safe(&row, 1)?,
            last_created_at_ms: get_integer_safe(&row, 2)?,
            last_event_id: event_id,
            last_partition: get_text_safe(&row, 4)?,
        }))
    }

    pub async fn update_consumer_offset(
        &self,
        consumer: &str,
        cursor: &PartitionedCursor,
        lease: Option<(&str, i64)>,
    ) -> Result<()> {
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;

        let (lease_owner, lease_expires_at) = if let Some((owner, expires)) = lease {
            (Some(owner.to_string()), Some(expires))
        } else {
            (None, None)
        };

        let conn = self.get_connection().await?;
        conn.execute(
            r#"
            INSERT INTO consumer_offsets (consumer, partition, cursor_created_at, cursor_sequence, updated_at, lease_owner, lease_expires_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(consumer) DO UPDATE SET
                partition = excluded.partition,
                cursor_created_at = excluded.cursor_created_at,
                cursor_sequence = excluded.cursor_sequence,
                updated_at = excluded.updated_at,
                lease_owner = excluded.lease_owner,
                lease_expires_at = excluded.lease_expires_at
            "#,
            (consumer, cursor.partition.clone(), cursor.created_at_ms, cursor.sequence, now, lease_owner, lease_expires_at),
        ).await?;
        Ok(())
    }

    pub async fn get_consumer_offset(&self, consumer: &str) -> Result<Option<ConsumerOffset>> {
        let conn = self.get_connection().await?;

        let mut rows = conn
            .query(
                "SELECT consumer, partition, cursor_created_at, cursor_sequence, updated_at, lease_owner, lease_expires_at, workflow_stream_id, workflow_event_id FROM consumer_offsets WHERE consumer = ?1",
                (consumer,),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };

        // Parse optional workflow_event_id
        let workflow_event_id = match self.get_optional_text(&row, 8)? {
            Some(s) => Some(uuid::Uuid::parse_str(&s)?),
            None => None,
        };

        Ok(Some(ConsumerOffset {
            consumer: get_text_safe(&row, 0)?,
            partition: get_text_safe(&row, 1)?,
            cursor_created_at: get_integer_safe(&row, 2)?,
            cursor_sequence: get_integer_safe(&row, 3)?,
            updated_at: get_integer_safe(&row, 4)?,
            lease_owner: self.get_optional_text(&row, 5)?,
            lease_expires_at: self.get_optional_integer(&row, 6)?,
            workflow_stream_id: self.get_optional_text(&row, 7)?,
            workflow_event_id,
        }))
    }

    /// Acquire a lease for a consumer.
    ///
    /// For an existing row, succeeds when the lease is unowned (NULL),
    /// already held by the same owner, or expired.
    ///
    /// For a cold-start consumer with no row, inserts a new row whose
    /// cursor points at the earliest partition so the offset is always
    /// safe to use directly (no placeholder sentinel).
    ///
    /// Returns `false` when another owner holds an active lease, or when
    /// no partitions exist yet (cannot construct a valid cursor).
    pub async fn acquire_lease(
        &self,
        consumer: &str,
        owner: &str,
        expires_at: i64,
    ) -> Result<bool> {
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        let conn = self.get_connection().await?;

        // Try UPDATE first — handles existing rows (unlocked, same-owner, expired)
        let updated = conn
            .execute(
                r#"
            UPDATE consumer_offsets
            SET lease_owner = ?1, lease_expires_at = ?2, updated_at = ?3
            WHERE consumer = ?4
              AND (lease_owner IS NULL
                   OR lease_owner = ?1
                   OR lease_expires_at IS NULL
                   OR lease_expires_at < ?3)
            "#,
                (owner, expires_at, now, consumer),
            )
            .await?;

        if updated > 0 {
            return Ok(true);
        }

        // Check whether the row exists but the UPDATE was blocked by an active lease
        let mut rows = conn
            .query(
                "SELECT 1 FROM consumer_offsets WHERE consumer = ?1",
                (consumer,),
            )
            .await?;
        if rows.next().await?.is_some() {
            // Row exists — another owner holds an active lease
            return Ok(false);
        }

        // No row at all — cold start. Resolve the earliest partition so the
        // cursor is immediately valid for replay (no empty-string sentinel).
        let mut part_rows = conn
            .query(
                "SELECT name FROM partitions ORDER BY start_ms, name LIMIT 1",
                (),
            )
            .await?;
        let Some(part_row) = part_rows.next().await? else {
            // No partitions exist yet — cannot create a valid offset row
            return Ok(false);
        };
        let earliest_partition = get_text_safe(&part_row, 0)?;

        // INSERT OR IGNORE: if a concurrent caller inserted between our
        // SELECT and this INSERT, IGNORE makes us return false (lost race)
        // rather than erroring — the winner already holds the lease.
        let inserted = conn
            .execute(
                r#"
            INSERT OR IGNORE INTO consumer_offsets
                (consumer, partition, cursor_created_at, cursor_sequence, updated_at, lease_owner, lease_expires_at)
            VALUES (?1, ?2, 0, 0, ?3, ?4, ?5)
            "#,
                (consumer, earliest_partition, now, owner, expires_at),
            )
            .await?;

        Ok(inserted > 0)
    }

    /// Renew a lease that the caller already holds.
    ///
    /// Succeeds only when the caller is the current lease owner.
    ///
    /// Returns `false` when:
    /// - The row does not exist
    /// - The lease is unowned (released) — callers must re-acquire via `acquire_lease`
    /// - Another owner holds the lease (active or expired)
    ///
    /// Expired leases held by a different owner must go through
    /// `acquire_lease`, not `renew_lease`. This ensures that after
    /// `release_lease` NULLs both columns, a stale heartbeat calling
    /// `renew_lease` gets `false` and knows it must stop processing.
    pub async fn renew_lease(&self, consumer: &str, owner: &str, expires_at: i64) -> Result<bool> {
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        let conn = self.get_connection().await?;

        let result = conn
            .execute(
                r#"
            UPDATE consumer_offsets
            SET lease_owner = ?1, lease_expires_at = ?2, updated_at = ?3
            WHERE consumer = ?4
              AND lease_owner = ?1
            "#,
                (owner, expires_at, now, consumer),
            )
            .await?;

        Ok(result > 0)
    }

    pub async fn release_lease(&self, consumer: &str, owner: &str) -> Result<bool> {
        let conn = self.get_connection().await?;
        let result = conn.execute(
            "UPDATE consumer_offsets SET lease_owner = NULL, lease_expires_at = NULL WHERE consumer = ?1 AND lease_owner = ?2",
            (consumer, owner),
        ).await?;

        Ok(result > 0)
    }

    pub async fn get_all_partitions(&self) -> Result<Vec<PartitionRef>> {
        let conn = self.get_connection().await?;
        let mut rows = conn
            .query(
                "SELECT name, path, start_ms, end_ms, sealed FROM partitions ORDER BY start_ms, name",
                (),
            )
            .await?;

        self.collect_partitions_from_rows(&mut rows).await
    }

    /// Helper function to convert a database row to PartitionRef
    fn row_to_partition_ref(&self, row: &turso::Row) -> Result<PartitionRef> {
        Ok(PartitionRef {
            name: get_text_safe(row, 0)?,
            path: get_text_safe(row, 1)?,
            start_ms: get_integer_safe(row, 2)?,
            end_ms: self.get_optional_integer(row, 3)?,
            sealed: get_integer_safe(row, 4)? == 1,
        })
    }

    /// Helper function to safely extract optional integer value using let else pattern
    fn get_optional_integer(&self, row: &turso::Row, index: usize) -> Result<Option<i64>> {
        let value = row.get_value(index)?;
        let Some(int_value) = value.as_integer() else {
            return Ok(None);
        };

        Ok(Some(*int_value))
    }

    /// Helper function to safely extract optional text value using let else pattern
    fn get_optional_text(&self, row: &turso::Row, index: usize) -> Result<Option<String>> {
        let value = row.get_value(index)?;
        let Some(text_value) = value.as_text() else {
            return Ok(None);
        };

        Ok(Some(text_value.to_string()))
    }

    /// Helper function to collect partitions from database rows
    async fn collect_partitions_from_rows(
        &self,
        rows: &mut turso::Rows,
    ) -> Result<Vec<PartitionRef>> {
        let mut result = Vec::new();

        while let Some(row) = rows.next().await? {
            result.push(self.row_to_partition_ref(&row)?);
        }

        Ok(result)
    }
}
