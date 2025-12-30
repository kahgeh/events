use crate::{EsError, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub struct Migration {
    pub name: String,
    pub sql: &'static str,
}

pub struct MigrationRunner {
    migrations: Vec<Migration>,
}

impl Default for MigrationRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationRunner {
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }

    pub fn with_migration(mut self, migration: Migration) -> Self {
        self.migrations.push(migration);
        self
    }

    pub async fn run(&self, conn: &turso::Connection) -> Result<()> {
        self.run_async(conn).await
    }

    async fn run_async(&self, conn: &turso::Connection) -> Result<()> {
        self.ensure_migrations_table(conn).await?;
        let applied = self.load_applied_migrations(conn).await?;
        self.run_pending_migrations(conn, applied).await?;

        Ok(())
    }

    /// Creates the migrations tracking table if it doesn't exist
    async fn ensure_migrations_table(&self, conn: &turso::Connection) -> Result<()> {
        conn.execute(
            r#"
            CREATE TABLE IF NOT EXISTS _migrations (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                checksum TEXT NOT NULL,
                applied_at INTEGER NOT NULL
            )
            "#,
            (),
        )
        .await?;
        Ok(())
    }

    /// Loads all previously applied migrations from the database
    async fn load_applied_migrations(
        &self,
        conn: &turso::Connection,
    ) -> Result<HashMap<String, String>> {
        let mut rows = conn
            .query("SELECT name, checksum FROM _migrations ORDER BY id", ())
            .await?;
        let mut applied: HashMap<String, String> = HashMap::new();

        while let Some(row) = rows.next().await? {
            let (name, checksum) = Self::parse_migration_row(row)?;
            applied.insert(name, checksum);
        }

        Ok(applied)
    }

    /// Parses a single migration row from the database
    fn parse_migration_row(row: turso::Row) -> Result<(String, String)> {
        let name = row
            .get_value(0)?
            .as_text()
            .ok_or_else(|| {
                EsError::Migration("Expected text value for migration name".to_string())
            })?
            .to_string();
        let checksum = row
            .get_value(1)?
            .as_text()
            .ok_or_else(|| {
                EsError::Migration("Expected text value for migration checksum".to_string())
            })?
            .to_string();
        Ok((name, checksum))
    }

    /// Applies a single migration and records it in the migrations table
    async fn apply_migration(
        conn: &turso::Connection,
        migration: &Migration,
        checksum: &str,
    ) -> Result<()> {
        tracing::info!("Applying migration: {}", migration.name);

        // Run migration SQL (use execute_batch to support multiple statements)
        conn.execute_batch(migration.sql).await?;

        // Record the migration
        let now = (time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        conn.execute(
            "INSERT INTO _migrations (name, checksum, applied_at) VALUES (?1, ?2, ?3)",
            (migration.name.as_str(), checksum, now),
        )
        .await?;

        tracing::info!("Applied migration: {}", migration.name);
        Ok(())
    }

    /// Verifies that a migration's checksum matches what was previously applied
    fn verify_migration_checksum(
        migration: &Migration,
        applied_checksum: &str,
        expected_checksum: &str,
    ) -> Result<()> {
        if applied_checksum != expected_checksum {
            return Err(EsError::Migration(format!(
                "Migration {} has changed checksum. Expected: {}, Applied: {}",
                migration.name, expected_checksum, applied_checksum
            )));
        }
        tracing::debug!("Migration {} already applied", migration.name);
        Ok(())
    }

    /// Runs all pending migrations that haven't been applied yet
    async fn run_pending_migrations(
        &self,
        conn: &turso::Connection,
        applied: HashMap<String, String>,
    ) -> Result<()> {
        for migration in &self.migrations {
            let checksum = hex::encode(Sha256::digest(migration.sql.as_bytes()));

            match applied.get(&migration.name) {
                Some(applied_checksum) => {
                    // Migration already applied, verify checksum
                    Self::verify_migration_checksum(migration, applied_checksum, &checksum)?;
                }
                None => {
                    // Migration not yet applied, apply it
                    Self::apply_migration(conn, migration, &checksum).await?;
                }
            }
        }
        Ok(())
    }
}

// Partition DB migrations
pub fn partition_migrations() -> MigrationRunner {
    MigrationRunner::new().with_migration(Migration {
        name: "001_create_events_table".into(),
        sql: r#"
            CREATE TABLE IF NOT EXISTS events (
                id TEXT,
                stream_id TEXT NOT NULL,
                type TEXT NOT NULL,
                payload TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                trace_id TEXT,
                span_id TEXT,
                request_id TEXT,
                actor_id TEXT NOT NULL,
                actor_type TEXT NOT NULL,
                PRIMARY KEY (id),
                UNIQUE (stream_id, version)
            );
            CREATE INDEX IF NOT EXISTS idx_events_global ON events(created_at, id);
            CREATE INDEX IF NOT EXISTS idx_events_actor ON events(actor_id, actor_type);
            CREATE INDEX IF NOT EXISTS idx_events_actor_type ON events(actor_type);
            "#,
    })
}

// Catalog DB migrations
pub fn catalog_migrations() -> MigrationRunner {
    MigrationRunner::new()
        .with_migration(Migration {
            name: "001_create_catalog_schema".into(),
            sql: r#"
            CREATE TABLE IF NOT EXISTS partitions (
                name TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                start_ms INTEGER NOT NULL,
                end_ms INTEGER,
                sealed INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_partitions_range ON partitions(start_ms, end_ms);

            CREATE TABLE IF NOT EXISTS stream_heads (
                stream_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                last_created_at_ms INTEGER NOT NULL,
                last_event_id TEXT NOT NULL,
                last_partition TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS consumer_offsets (
                consumer TEXT PRIMARY KEY,
                partition TEXT NOT NULL,
                cursor_created_at INTEGER NOT NULL,
                cursor_event_id TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                lease_owner TEXT,
                lease_expires_at INTEGER,
                workflow_stream_id TEXT,
                workflow_event_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_consumer_offsets_consumer ON consumer_offsets(consumer);
            CREATE INDEX IF NOT EXISTS idx_consumer_offsets_lease ON consumer_offsets(lease_owner, lease_expires_at);
            "#,
        })
}
